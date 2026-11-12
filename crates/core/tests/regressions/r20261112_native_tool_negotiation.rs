//! Defect: unsupported native tool fields terminated gateway sessions.
//! Root cause: native search declarations had no scoped downgrade boundary.
//! Work item 20261112: negotiate once without changing active tool state.

use futures::StreamExt;
use klaus_agent_core::provider::anthropic::{AnthropicProvider, AnthropicProviderOptions};
use klaus_agent_core::provider::openai::{OpenAIProvider, OpenAIProviderOptions, OpenAiApi};
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeString};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    ModelCapabilities, ModelProvider, ModelRequest, ModelStreamEvent,
};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn read_request(socket: &mut tokio::net::TcpStream) -> Value {
    let mut bytes = Vec::new();
    loop {
        let mut chunk = [0; 4096];
        let count = socket.read(&mut chunk).await.unwrap();
        assert_ne!(count, 0);
        bytes.extend_from_slice(&chunk[..count]);
        assert!(bytes.len() < 2 * 1024 * 1024);
        if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            let length = String::from_utf8_lossy(&bytes[..end])
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            if bytes.len() >= end + 4 + length {
                return serde_json::from_slice(&bytes[end + 4..end + 4 + length]).unwrap();
            }
        }
    }
}

fn request() -> ModelRequest {
    runtime_codec::decode_text(&RuntimeString::from(json!({
        "model":"fixture-model", "system":[], "messages":[], "maxOutputTokens":128,
        "tools":[
            {"name":"tool_search","description":"Find tools","kind":"tool_search","inputSchema":{"type":"object"}},
            {"name":"read","description":"Read active tool","inputSchema":{"type":"object"}}
        ]
    }).to_string())).unwrap()
}

async fn negotiate(anthropic: bool, reject_fallback: bool) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let caps = ModelCapabilities {
        native_deferred_tools: Some(true),
        ..Default::default()
    };
    let provider: Arc<dyn ModelProvider> = if anthropic {
        Arc::new(AnthropicProvider::new(AnthropicProviderOptions {
            base_url: Some(base),
            api_key: Some("fixture".into()),
            default_model: Some("fixture-model".into()),
            capabilities: Some(caps),
            ..Default::default()
        }))
    } else {
        Arc::new(OpenAIProvider::new(OpenAIProviderOptions {
            base_url: Some(base),
            api_key: Some("fixture".into()),
            api: Some(OpenAiApi::Responses),
            default_model: Some("fixture-model".into()),
            capabilities: Some(caps),
            ..Default::default()
        }))
    };
    let server = tokio::spawn(async move {
        let mut requests = Vec::new();
        for index in 0..3 {
            let (mut socket, _) = listener.accept().await.unwrap();
            requests.push(read_request(&mut socket).await);
            let (status, content_type, body) = if index == 0 || reject_fallback {
                (
                    "422 Unprocessable Entity",
                    "application/json",
                    "{\"error\":{\"message\":\"Extra Inputs are not permitted\"}}",
                )
            } else if anthropic {
                ("200 OK", "text/event-stream", "data: {\"type\":\"message_start\",\"message\":{\"model\":\"fixture-model\",\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\ndata: {\"type\":\"message_stop\"}\n\n")
            } else {
                (
                    "200 OK",
                    "text/event-stream",
                    "data: {\"type\":\"response.completed\",\"response\":{\"output\":[]}}\n\n",
                )
            };
            let response = format!("HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
            socket.write_all(response.as_bytes()).await.unwrap();
        }
        requests
    });
    let mut request = request();
    if anthropic {
        // Unloaded inventory is not active; the active read declaration must stay.
        request.deferred_tools = vec![request.tools[1].clone()];
        request.deferred_tools[0].name = "not_yet_loaded".into();
    }
    for _ in 0..2 {
        let events = tokio::time::timeout(
            Duration::from_secs(10),
            provider
                .stream(request.clone(), AbortSignal::new())
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
        assert_eq!(
            events
                .iter()
                .any(|event| matches!(event, ModelStreamEvent::Error { .. })),
            reject_fallback,
            "{events:?}"
        );
    }
    let requests = tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        requests.len(),
        3,
        "one native attempt, one downgrade, one learned request"
    );
    for (index, body) in requests.iter().enumerate() {
        eprintln!("native tool negotiation {anthropic}/{reject_fallback}/{index}: {body}");
        let tools = body["tools"].as_array().unwrap();
        assert!(tools.iter().any(|tool| tool["name"] == "read"));
        let native = tools
            .iter()
            .any(|tool| tool["type"] == "tool_search" || tool["defer_loading"] == true);
        assert_eq!(native, index == 0);
        if anthropic && index > 0 {
            assert!(!tools.iter().any(|tool| tool["name"] == "not_yet_loaded"));
        }
    }
}

#[tokio::test]
async fn responses_gateway_downgrades_once_and_remembers_without_losing_active_tools() {
    negotiate(false, false).await;
}

#[tokio::test]
async fn responses_second_refusal_is_terminal_not_another_native_retry() {
    negotiate(false, true).await;
}

#[tokio::test]
async fn messages_gateway_downgrades_only_unsurfaced_tool_definitions() {
    negotiate(true, false).await;
}

#[tokio::test]
async fn messages_second_refusal_is_terminal() {
    negotiate(true, true).await;
}
