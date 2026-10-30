//! Defect: compatible chat requests could not explicitly enable or disable thinking.
//! Root cause: the request dialect omitted the boolean switch and retained effort
//! when thinking was disabled. Work item 20261015: per-turn thinking control.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{resolve_config, Input, TurnOptions};
use klaus_agent_core::provider::openai::{
    OpenAIProvider, OpenAIProviderOptions, OpenAiApi, OpenAiReasoningDialect,
};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::schemas::effort::{EffortLevel, EffortValue};
use klaus_agent_core::types::provider::{
    EffortCapability, ModelCapabilities, ThinkingCapability, ThinkingConfig,
};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};
use super::rbd0a6abc_empty_tool_choice::{
    capture_provider_request, record_engine_request, single_attempt,
};

fn capabilities() -> ModelCapabilities {
    ModelCapabilities {
        thinking: Some(ThinkingCapability::default()),
        effort: Some(EffortCapability {
            levels: vec![EffortLevel::Low, EffortLevel::High, EffortLevel::Xhigh],
            default: Some(EffortLevel::High),
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn public_chat_provider_sends_boolean_thinking_without_conflicting_effort() {
    let mut request = record_engine_request(false).await;
    request.model = "qwen3.8-max".into();
    request.effort = Some(EffortValue::Level(EffortLevel::Xhigh));
    for dialect in [
        OpenAiReasoningDialect::Qwen,
        OpenAiReasoningDialect::ManagedQwen,
    ] {
        for (thinking, enabled) in [
            (Some(ThinkingConfig::Adaptive), true),
            (
                Some(ThinkingConfig::Enabled {
                    budget_tokens: Some(4096),
                }),
                true,
            ),
            (Some(ThinkingConfig::Disabled), false),
            (None, false),
        ] {
            request.thinking = thinking;
            let body = capture_provider_request(request.clone(), |base_url| {
                Box::new(OpenAIProvider::new(OpenAIProviderOptions {
                    api: Some(OpenAiApi::Chat),
                    api_key: Some("fixture-key".into()),
                    base_url: Some(base_url),
                    retry: Some(single_attempt()),
                    capabilities: Some(capabilities()),
                    reasoning_dialect: Some(dialect),
                    ..Default::default()
                }))
            })
            .await;
            assert_eq!(
                body["enable_thinking"],
                json!(enabled),
                "{dialect:?}: {body}"
            );
            assert_eq!(
                body.get("reasoning_effort"),
                enabled.then(|| json!("xhigh")).as_ref()
            );
            assert!(
                body.get("thinking_budget").is_none(),
                "effort and a fixed budget must not coexist"
            );
            assert!(
                body.get("extra_body").is_none(),
                "the boolean belongs at the HTTP body root"
            );
            assert!(
                body.get("thinking").is_none(),
                "the endpoint uses a boolean switch"
            );
            assert_eq!(body["stream"], json!(true));
            assert_eq!(
                body.get("max_completion_tokens").is_none(),
                enabled && dialect == OpenAiReasoningDialect::ManagedQwen
            );
        }
    }
}

#[tokio::test]
async fn other_dialects_and_undeclared_thinking_do_not_receive_the_switch() {
    let mut request = record_engine_request(false).await;
    request.thinking = Some(ThinkingConfig::Adaptive);
    request.effort = Some(EffortValue::Level(EffortLevel::High));
    for (dialect, declared) in [
        (OpenAiReasoningDialect::OpenAi, true),
        (OpenAiReasoningDialect::DeepSeek, true),
        (OpenAiReasoningDialect::Qwen, false),
        (OpenAiReasoningDialect::ManagedQwen, false),
    ] {
        let mut caps = capabilities();
        if !declared {
            caps.thinking = None;
        }
        let body = capture_provider_request(request.clone(), |base_url| {
            Box::new(OpenAIProvider::new(OpenAIProviderOptions {
                api: Some(OpenAiApi::Chat),
                api_key: Some("fixture-key".into()),
                base_url: Some(base_url),
                retry: Some(single_attempt()),
                capabilities: Some(caps),
                reasoning_dialect: Some(dialect),
                ..Default::default()
            }))
        })
        .await;
        assert!(body.get("enable_thinking").is_none(), "{dialect:?}: {body}");
    }
}

async fn read_body(socket: &mut TcpStream) -> Value {
    let mut bytes = Vec::new();
    loop {
        let mut chunk = [0_u8; 4096];
        let read = socket.read(&mut chunk).await.unwrap();
        assert_ne!(read, 0, "the fixture must receive a complete HTTP request");
        bytes.extend_from_slice(&chunk[..read]);
        assert!(bytes.len() < 1024 * 1024, "fixture request exceeds 1 MiB");
        if let Some(end) = bytes
            .windows(4)
            .position(|part| part == b"\r\n\r\n")
            .map(|i| i + 4)
        {
            let length = std::str::from_utf8(&bytes[..end])
                .unwrap()
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            if bytes.len() >= end + length {
                return serde_json::from_slice(&bytes[end..end + length]).unwrap();
            }
        }
    }
}

#[tokio::test]
async fn public_session_applies_on_off_on_to_consecutive_http_requests() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let observed = requests.clone();
    let server = tokio::spawn(async move {
        for _ in 0..3 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let captured = read_body(&mut socket).await;
            observed.lock().unwrap().push(captured);
            let delta = json!({"id":"fixture", "model":"qwen3.8-max", "choices":[{"index":0,"delta":{"content":"Done."},"finish_reason":null}]});
            let stop = json!({"id":"fixture", "model":"qwen3.8-max", "choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":1,"total_tokens":11}});
            let body = format!("data: {delta}\n\ndata: {stop}\n\ndata: [DONE]\n\n");
            let response = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
            socket.write_all(response.as_bytes()).await.unwrap();
        }
    });
    let provider = Arc::new(OpenAIProvider::new(OpenAIProviderOptions {
        api: Some(OpenAiApi::Chat),
        api_key: Some("fixture-key".into()),
        base_url: Some(format!("http://{address}/v1")),
        default_model: Some("qwen3.8-max".into()),
        capabilities: Some(capabilities()),
        reasoning_dialect: Some(OpenAiReasoningDialect::Qwen),
        retry: Some(single_attempt()),
        ..Default::default()
    }));
    let root =
        std::env::temp_dir().join(format!("klaus-thinking-control-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut options = agent_options(process.clone());
    options.provider = provider.clone();
    options.cwd = Some(root.to_string_lossy().into_owned());
    let mut config = options.config.take().unwrap();
    config.as_object_mut().unwrap().extend(
        json!({
            "model":"qwen3.8-max", "prewarm":false, "fork":false,
            "autoMemory":false, "agentMemory":false, "autoDream":false,
            "speculativeCompaction":false, "loopDetection":false
        })
        .as_object()
        .unwrap()
        .clone(),
    );
    options.config = Some(config.clone());
    let agent = Agent::new(options);
    let session = agent.start_session(StartSessionOptions {
        cwd: Some(root.to_string_lossy().into_owned().into()),
        user_config_dir: Some(root.to_string_lossy().into_owned().into()),
        ..Default::default()
    });
    let mut revision = 0;
    for enabled in [true, false, true] {
        config["thinking"] = json!(enabled);
        // Simulate a retained provider default; explicit off must still win.
        config["effort"] = json!("xhigh");
        revision = session
            .reconfigure_inference(
                provider.clone(),
                resolve_config(config.clone()).unwrap(),
                None,
                None,
                revision,
            )
            .unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            session.run(Input::Text("Reply briefly.".into()), TurnOptions::default()),
        )
        .await
        .unwrap();
        assert!(result.error.is_none(), "{:?}", result.error);
    }
    tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .unwrap()
        .unwrap();
    let requests = requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 3);
    for (body, enabled) in requests.iter().zip([true, false, true]) {
        assert_eq!(body["enable_thinking"], json!(enabled), "{body}");
        assert_eq!(
            body.get("reasoning_effort"),
            enabled.then(|| json!("xhigh")).as_ref()
        );
    }
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
