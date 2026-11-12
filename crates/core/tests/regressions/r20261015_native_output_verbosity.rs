//! Defect: declared native output detail never reached provider requests.
//! Root cause: the capability contract omitted output verbosity and its wire field.
//! Work item: 20261015 supported native output detail without changing token budgets.

use std::collections::HashMap;
use std::time::Duration;

use futures::StreamExt;
use klaus_agent_core::provider::openai::{
    OpenAIProvider, OpenAIProviderOptions, OpenAiApi, OpenAiReasoningDialect,
};
use klaus_agent_core::provider::retry::RetryOptions;
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeString};
use klaus_agent_core::schemas::content::{ContentBlock, TextBlock};
use klaus_agent_core::schemas::effort::{EffortLevel, EffortValue};
use klaus_agent_core::schemas::message::{Message, UserMessage};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    ModelCapabilities, ModelProvider, ModelRequest, ModelStreamEvent,
};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

fn capabilities(output_verbosity: Option<&str>) -> ModelCapabilities {
    let mut wire = json!({
        "effort": { "levels": ["high"] },
        "structuredOutput": true
    });
    if let Some(verbosity) = output_verbosity {
        wire["outputVerbosity"] = json!(verbosity);
    }
    runtime_codec::decode_text(&RuntimeString::from(wire.to_string())).unwrap()
}

fn request(model: &str) -> ModelRequest {
    ModelRequest {
        supports_image_detail_original: false,
        model_verbosity: None,
        reasoning_summary: Default::default(),
        model: model.into(),
        image_limits: None,
        max_media_blocks: None,
        system: Vec::new(),
        messages: vec![Message::User(UserMessage {
            uuid: "fixture-user".into(),
            question_answer: None,
            content: vec![ContentBlock::Text(TextBlock {
                text: "Return the result.".into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_meta: None,
        })],
        tools: Vec::new(),
        native_deferred_tools: None,
        deferred_tools: Vec::new(),
        tool_choice: None,
        max_output_tokens: 321,
        task_budget: None,
        output_schema: Some(
            runtime_json::parse(&RuntimeString::from(
                r#"{"type":"object","properties":{"result":{"type":"string"}},"required":["result"],"additionalProperties":false}"#,
            ))
            .unwrap(),
        ),
        web_search: None,
        thinking: None,
        effort: Some(EffortValue::Level(EffortLevel::High)),
        skip_cache_write: None,
        cache_ttl: None,
        cache_domain: None,
        cache_key: None,
        context_management: None,
        context_hints: None,
        thinking_context: None,
    }
}

async fn capture(
    mut options: OpenAIProviderOptions,
    request: ModelRequest,
    subscription: bool,
) -> Value {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let api = options.api.unwrap_or_default();
    options.base_url = Some(if subscription {
        format!("http://{address}/chatgpt.com/backend-api/codex")
    } else {
        format!("http://{address}")
    });
    options.api_key = Some("fixture-only".into());
    options.retry = Some(RetryOptions {
        max_attempts: Some(1),
        congestion_hold_ms: Some(0),
        ..Default::default()
    });
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        let body: Value = loop {
            let mut chunk = [0_u8; 8192];
            let count = socket.read(&mut chunk).await.unwrap();
            assert_ne!(count, 0);
            bytes.extend_from_slice(&chunk[..count]);
            if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&bytes[..end]);
                let length = headers
                    .lines()
                    .filter_map(|line| line.split_once(':'))
                    .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                    .map(|(_, value)| value.trim().parse::<usize>().unwrap())
                    .unwrap();
                if bytes.len() >= end + 4 + length {
                    break serde_json::from_slice(&bytes[end + 4..end + 4 + length]).unwrap();
                }
            }
        };
        let stream = match api {
            OpenAiApi::Chat => concat!(
                "data: {\"choices\":[{\"delta\":{\"content\":\"done\"},\"finish_reason\":\"stop\"}]}\n\n",
                "data: [DONE]\n\n",
            ),
            OpenAiApi::Responses => concat!(
                "data: {\"type\":\"response.output_text.delta\",\"delta\":\"done\"}\n\n",
                "data: {\"type\":\"response.completed\",\"response\":{\"output\":[]}}\n\n",
            ),
        };
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{stream}",
            stream.len()
        );
        socket.write_all(response.as_bytes()).await.unwrap();
        socket.shutdown().await.unwrap();
        body
    });
    let provider = OpenAIProvider::new(options);
    let events = tokio::time::timeout(
        Duration::from_secs(10),
        provider
            .stream(request, AbortSignal::new())
            .collect::<Vec<_>>(),
    )
    .await
    .unwrap();
    assert!(events
        .iter()
        .any(|event| matches!(event, ModelStreamEvent::MessageStop { .. })));
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::Error { .. })),
        "{events:?}"
    );
    tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn responses_preserve_per_model_output_detail_and_structured_format() {
    for (model, expected) in [
        ("main", Some("low")),
        ("summary", Some("medium")),
        ("plain", None),
        ("gpt-6.1-sol", None),
    ] {
        let body = capture(
            OpenAIProviderOptions {
                api: Some(OpenAiApi::Responses),
                default_model: Some("main".into()),
                capabilities: Some(capabilities(Some("low"))),
                capabilities_by_model: Some(HashMap::from([
                    ("summary".into(), capabilities(Some("medium"))),
                    ("plain".into(), capabilities(None)),
                ])),
                ..Default::default()
            },
            request(model),
            false,
        )
        .await;
        assert_eq!(body["text"]["verbosity"].as_str(), expected, "{body}");
        assert_eq!(body["text"]["format"]["type"], json!("json_schema"));
        assert_eq!(
            body["text"]["format"]["schema"]["required"],
            json!(["result"])
        );
        assert_eq!(body["max_output_tokens"], json!(321));
        assert_eq!(body["reasoning"]["effort"], json!("high"));
    }
}

#[tokio::test]
async fn output_detail_is_omitted_for_other_protocols_and_dialects() {
    for (api, dialect) in [
        (OpenAiApi::Chat, OpenAiReasoningDialect::OpenAi),
        (OpenAiApi::Responses, OpenAiReasoningDialect::DeepSeek),
        (OpenAiApi::Responses, OpenAiReasoningDialect::ManagedQwen),
        (OpenAiApi::Responses, OpenAiReasoningDialect::Qwen),
        (OpenAiApi::Responses, OpenAiReasoningDialect::Qoder),
        (OpenAiApi::Responses, OpenAiReasoningDialect::LlamaCpp),
        (OpenAiApi::Responses, OpenAiReasoningDialect::LegacyTokens),
    ] {
        let body = capture(
            OpenAIProviderOptions {
                api: Some(api),
                reasoning_dialect: Some(dialect),
                default_model: Some("main".into()),
                capabilities: Some(capabilities(Some("high"))),
                ..Default::default()
            },
            request("main"),
            false,
        )
        .await;
        assert!(body.get("verbosity").is_none(), "{body}");
        assert!(body["text"].get("verbosity").is_none(), "{body}");
    }
}

#[tokio::test]
async fn subscription_adaptation_keeps_declared_output_detail() {
    let body = capture(
        OpenAIProviderOptions {
            api: Some(OpenAiApi::Responses),
            default_model: Some("main".into()),
            capabilities: Some(capabilities(Some("low"))),
            ..Default::default()
        },
        request("main"),
        true,
    )
    .await;
    assert_eq!(body["text"]["verbosity"], json!("low"));
    assert!(body.get("max_output_tokens").is_none());
    assert_eq!(body["reasoning"]["effort"], json!("high"));
}

#[test]
fn declared_output_detail_round_trips_and_rejects_unknown_values() {
    for value in ["low", "medium", "high"] {
        let wire = RuntimeString::from(format!(r#"{{"outputVerbosity":"{value}"}}"#));
        let decoded: ModelCapabilities = runtime_codec::decode_text(&wire).unwrap();
        assert_eq!(
            runtime_codec::encode_value(&decoded).unwrap(),
            runtime_json::parse(&wire).unwrap()
        );
    }
    let invalid = RuntimeString::from(r#"{"outputVerbosity":"verbose"}"#);
    assert!(runtime_codec::decode_text::<ModelCapabilities>(&invalid).is_err());
}
