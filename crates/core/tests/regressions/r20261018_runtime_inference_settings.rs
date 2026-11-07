//! Defect: saved inference controls were collapsed or ignored at request time.
//! Root cause: the request contract lacked independent summary, verbosity and search access.
//! Work item: 20261018 runtime inference settings and exact provider requests.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::deps::NoopCompactor;
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::provider::openai::{OpenAIProvider, OpenAIProviderOptions, OpenAiApi};
use klaus_agent_core::provider::retry::RetryOptions;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeString};
use klaus_agent_core::schemas::config::{EngineConfig, WebSearchAccess};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::provider::{
    ModelCapabilities, ModelProvider, ModelRequest, ModelStopReason, ModelStreamEvent,
    OutputVerbosity, ReasoningSummary, WebSearchRequest,
};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

const WAIT: Duration = Duration::from_secs(10);

fn request() -> ModelRequest {
    runtime_codec::decode_text(&RuntimeString::from(
        r#"{"model":"fixture","system":[],"messages":[],"tools":[],"maxOutputTokens":321}"#,
    ))
    .unwrap()
}

fn capabilities() -> ModelCapabilities {
    runtime_codec::decode_text(&RuntimeString::from(r#"{"tools":true,"effort":{"levels":["high"]},"outputVerbosity":"medium","webSearch":true}"#)).unwrap()
}

async fn captured_body(mut request: ModelRequest) -> Value {
    request.model = "fixture".into();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        let body = loop {
            let mut chunk = [0_u8; 8192];
            let count = socket.read(&mut chunk).await.unwrap();
            assert_ne!(count, 0);
            assert!(bytes.len() + count <= 128 * 1024, "bounded request capture");
            bytes.extend_from_slice(&chunk[..count]);
            if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&bytes[..end]);
                let length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                if bytes.len() >= end + 4 + length {
                    break serde_json::from_slice::<Value>(&bytes[end + 4..end + 4 + length])
                        .unwrap();
                }
            }
        };
        let stream = concat!(
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"done\"}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"output\":[]}}\n\n",
        );
        let response = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{stream}", stream.len());
        socket.write_all(response.as_bytes()).await.unwrap();
        socket.shutdown().await.unwrap();
        body
    });
    let provider = OpenAIProvider::new(OpenAIProviderOptions {
        api: Some(OpenAiApi::Responses),
        default_model: Some("fixture".into()),
        api_key: Some("fixture-only".into()),
        base_url: Some(url),
        capabilities: Some(capabilities()),
        retry: Some(RetryOptions {
            max_attempts: Some(1),
            ..Default::default()
        }),
        ..Default::default()
    });
    let events = tokio::time::timeout(
        WAIT,
        provider
            .stream(request, AbortSignal::new())
            .collect::<Vec<_>>(),
    )
    .await
    .unwrap();
    assert!(
        events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::MessageStop { .. })),
        "{events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::Error { .. })),
        "{events:?}"
    );
    tokio::time::timeout(WAIT, server).await.unwrap().unwrap()
}

#[tokio::test]
async fn provider_wire_honors_output_override_summary_and_search_access() {
    for (verbosity, expected, summary, access) in [
        (
            None,
            "medium",
            ReasoningSummary::Auto,
            WebSearchAccess::Live,
        ),
        (
            Some(OutputVerbosity::Low),
            "low",
            ReasoningSummary::Concise,
            WebSearchAccess::Cached,
        ),
        (
            Some(OutputVerbosity::High),
            "high",
            ReasoningSummary::Detailed,
            WebSearchAccess::Indexed,
        ),
        (
            Some(OutputVerbosity::Medium),
            "medium",
            ReasoningSummary::None,
            WebSearchAccess::Cached,
        ),
        (
            None,
            "medium",
            ReasoningSummary::None,
            WebSearchAccess::Disabled,
        ),
    ] {
        let mut request = request();
        request.model_verbosity = verbosity;
        request.reasoning_summary = summary;
        request.web_search = Some(WebSearchRequest {
            access,
            ..Default::default()
        });
        let body = captured_body(request).await;
        assert_eq!(body["text"]["verbosity"], json!(expected));
        if summary == ReasoningSummary::None {
            assert!(body["reasoning"].get("summary").is_none(), "{body}");
        } else {
            assert_eq!(body["reasoning"]["summary"], json!(summary.as_str()));
        }
        if access == WebSearchAccess::Disabled {
            assert!(body.get("tools").is_none(), "{body}");
        } else {
            assert_eq!(
                body["tools"][0]["external_web_access"],
                json!(access != WebSearchAccess::Cached)
            );
            if access == WebSearchAccess::Indexed {
                assert_eq!(body["tools"][0]["indexed_web_access"], json!(true));
            } else {
                assert!(body["tools"][0].get("indexed_web_access").is_none());
            }
        }
        assert_eq!(body["max_output_tokens"], json!(321));
        assert_eq!(body["include"], json!(["reasoning.encrypted_content"]));
    }
}

#[tokio::test]
async fn unsupported_protocol_controls_fail_before_network_access() {
    let provider = OpenAIProvider::new(OpenAIProviderOptions {
        api: Some(OpenAiApi::Chat),
        api_key: Some("fixture-only".into()),
        base_url: Some("http://127.0.0.1:1".into()),
        capabilities: Some(capabilities()),
        ..Default::default()
    });
    for field in 0..3 {
        let mut request = request();
        match field {
            0 => request.model_verbosity = Some(OutputVerbosity::Low),
            1 => request.reasoning_summary = ReasoningSummary::Detailed,
            _ => {
                request.web_search = Some(WebSearchRequest {
                    access: WebSearchAccess::Cached,
                    ..Default::default()
                })
            }
        }
        let events = tokio::time::timeout(
            WAIT,
            provider
                .stream(request, AbortSignal::new())
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
        assert!(
            matches!(&events[..], [ModelStreamEvent::Error { error }] if error.code.as_ref().is_some_and(|code| code == "unsupported_inference_setting")),
            "{events:?}"
        );
    }
}

#[test]
fn config_and_request_codecs_preserve_nullable_default_and_independent_access() {
    for access in ["disabled", "cached", "indexed", "live"] {
        let config: EngineConfig = serde_json::from_value(json!({"modelVerbosity":null,"reasoningSummary":"none","webSearch":{"mode":"client","source":"codex","access":access}})).unwrap();
        assert_eq!(config.model_verbosity, None);
        assert_eq!(config.reasoning_summary, ReasoningSummary::None);
        assert_eq!(config.web_search.access.as_str(), access);
        let wire = runtime_codec::encode_value(&config).unwrap();
        assert_eq!(
            runtime_codec::decode_value::<EngineConfig>(&wire).unwrap(),
            config
        );
        let mut request = request();
        request.model_verbosity = Some(OutputVerbosity::High);
        request.reasoning_summary = ReasoningSummary::Detailed;
        request.web_search = Some(WebSearchRequest {
            access: config.web_search.access,
            ..Default::default()
        });
        let wire = runtime_codec::encode_value(&request).unwrap();
        assert_eq!(
            runtime_codec::decode_value::<ModelRequest>(&wire).unwrap(),
            request
        );
    }
    for (mode, expected) in [
        ("cached", WebSearchAccess::Cached),
        ("indexed", WebSearchAccess::Indexed),
        ("live", WebSearchAccess::Live),
    ] {
        let config: EngineConfig =
            serde_json::from_value(json!({"webSearch":{"mode":mode}})).unwrap();
        assert_eq!(config.web_search.access, expected);
        let config: EngineConfig =
            serde_json::from_value(json!({"webSearch":{"mode":mode,"access":"live"}})).unwrap();
        assert_eq!(config.web_search.access, WebSearchAccess::Live);
    }
    assert!(serde_json::from_value::<EngineConfig>(json!({"modelVerbosity":"short"})).is_err());
    assert!(serde_json::from_value::<EngineConfig>(json!({"reasoningSummary":"hidden"})).is_err());
}

#[tokio::test]
async fn session_updates_publish_controls_to_later_requests_and_preserve_noop_revision() {
    let root =
        std::env::temp_dir().join(format!("klaus-inference-settings-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut options = agent_options(process.clone());
    options.provider = Arc::new(OpenAIProvider::new(OpenAIProviderOptions {
        api: Some(OpenAiApi::Responses),
        capabilities: Some(capabilities()),
        ..Default::default()
    }));
    options.compactor = Some(Arc::new(NoopCompactor));
    let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let captured = requests.clone();
    options.deps.call_model = Some(Box::new(move |request, _| {
        let mut captured = captured.lock().unwrap();
        assert!(captured.len() < 3);
        captured.push(request);
        Box::pin(futures::stream::iter([
            ModelStreamEvent::TextDelta {
                text: "done".into(),
            },
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: klaus_agent_core::schemas::message::Usage {
                    input_tokens: 0,
                    output_tokens: 0,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    total_tokens: Some(0),
                },
            },
        ]))
    }));
    let agent = Agent::new(options);
    let session = agent.start_session(StartSessionOptions {
        config: Some(json!({"model":"fixture","prewarm":false,"webSearch":{"mode":"off","source":"native","access":"disabled"},"autoMemory":false,"agentMemory":false,"memory":{"enabled":false}})),
        cwd: Some(root.to_string_lossy().into_owned().into()),
        user_config_dir: Some(root.to_string_lossy().into_owned().into()), ..Default::default()
    });
    let before = session.model_revision();
    let initial = session.runtime_inference_settings();
    assert_eq!(
        session
            .set_runtime_inference_settings(None, None, None)
            .unwrap(),
        initial
    );
    assert_eq!(session.model_revision(), before);
    for round in 0..3 {
        if round == 1 {
            session
                .set_runtime_inference_settings(
                    Some(Some(OutputVerbosity::High)),
                    Some(ReasoningSummary::Detailed),
                    Some(WebSearchAccess::Cached),
                )
                .unwrap();
        } else if round == 2 {
            session
                .set_runtime_inference_settings(
                    Some(None),
                    Some(ReasoningSummary::None),
                    Some(WebSearchAccess::Disabled),
                )
                .unwrap();
        }
        let events = tokio::time::timeout(
            WAIT,
            session
                .run_streamed(
                    Input::Text(format!("fixture round {round}").into()),
                    TurnOptions::default(),
                )
                .events
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
        assert!(
            events
                .iter()
                .any(|event| matches!(event, EngineEvent::TurnCompleted { .. })),
            "{events:?}"
        );
    }
    let captured = requests.lock().unwrap().clone();
    assert_eq!(captured.len(), 3);
    assert_eq!(captured[0].model_verbosity, None);
    assert!(captured[0].web_search.is_none());
    assert_eq!(captured[1].model_verbosity, Some(OutputVerbosity::High));
    assert_eq!(captured[1].reasoning_summary, ReasoningSummary::Detailed);
    assert_eq!(
        captured[1].web_search.as_ref().unwrap().access,
        WebSearchAccess::Cached
    );
    assert_eq!(captured[2].model_verbosity, None);
    assert_eq!(captured[2].reasoning_summary, ReasoningSummary::None);
    assert!(captured[2].web_search.is_none());
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
