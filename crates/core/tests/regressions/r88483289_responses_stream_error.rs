//! Defect: a Responses stream error nested under `error` reached a turn as an
//! empty, unknown provider failure instead of its actionable message and code.
//! Root cause: the Responses SSE adapter read only top-level error fields.
//! Fixed by: session 88483289 provider-error investigation.

use futures::StreamExt;
use klaus_agent_core::provider::openai::{OpenAIProvider, OpenAIProviderOptions, OpenAiApi};
use klaus_agent_core::schemas::content::{ContentBlock, TextBlock};
use klaus_agent_core::schemas::message::{Message, UserMessage};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{ProviderError, ProviderErrorKind};
use klaus_agent_core::types::provider::{ModelProvider, ModelRequest, ModelStreamEvent};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn stream_error_details(payload: serde_json::Value) -> ProviderError {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let event_type = payload["type"].as_str().unwrap();
    let body = format!("event: {event_type}\ndata: {payload}\n\n");
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut received = Vec::new();
        let mut buffer = [0u8; 4096];
        loop {
            let count = stream.read(&mut buffer).await.unwrap();
            assert!(count > 0, "the request must arrive before the peer closes");
            received.extend_from_slice(&buffer[..count]);
            if let Some(end) = received.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&received[..end]);
                let body_length = headers
                    .lines()
                    .filter_map(|line| line.split_once(':'))
                    .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                    .map(|(_, length)| length.trim().parse::<usize>().unwrap())
                    .unwrap_or(0);
                if received.len() >= end + 4 + body_length {
                    break;
                }
            }
        }
        stream.write_all(response.as_bytes()).await.unwrap();
        stream.shutdown().await.unwrap();
    });
    let provider = OpenAIProvider::new(OpenAIProviderOptions {
        api_key: Some("test-key".into()),
        base_url: Some(base_url),
        api: Some(OpenAiApi::Responses),
        ..Default::default()
    });
    let request = ModelRequest {
        supports_image_detail_original: false,
        model_verbosity: None,
        reasoning_summary: Default::default(),
        model: "test-model".into(),
        image_limits: None,
        max_media_blocks: None,
        system: vec![],
        messages: vec![Message::User(UserMessage {
            question_answer: None,
            uuid: "request".into(),
            content: vec![ContentBlock::Text(TextBlock {
                text: "Hello".into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_meta: None,
        })],
        tools: vec![],
        deferred_tools: Vec::new(),
        tool_choice: None,
        max_output_tokens: 20_000,
        task_budget: None,
        output_schema: None,
        web_search: None,
        thinking: None,
        effort: None,
        skip_cache_write: None,
        cache_ttl: None,
        cache_key: None,
        context_management: None,
        context_hints: None,
        thinking_context: None,
    };
    let error = tokio::time::timeout(Duration::from_secs(5), async {
        let mut events = provider.stream(request, AbortSignal::new());
        while let Some(event) = events.next().await {
            if let ModelStreamEvent::Error { error } = event {
                return error;
            }
        }
        panic!("the stream error must reach the caller");
    })
    .await
    .expect("the stream error must finish promptly");
    server.await.unwrap();
    error
}

async fn stream_error(payload: serde_json::Value) -> (ProviderErrorKind, String) {
    let error = stream_error_details(payload).await;
    (
        error.kind,
        String::from_utf8(error.message.to_node_utf8()).unwrap(),
    )
}

#[tokio::test]
async fn nested_responses_error_preserves_message_and_classification() {
    let (kind, message) = stream_error(serde_json::json!({
        "type": "error",
        "code": "",
        "message": "",
        "error": {
            "type": "invalid_request_error",
            "code": "context_length_exceeded",
            "message": "prompt exceeds context window"
        }
    }))
    .await;
    assert_eq!(kind, ProviderErrorKind::PromptTooLong);
    assert_eq!(message, "prompt exceeds context window");
}

#[tokio::test]
async fn message_less_responses_error_never_becomes_a_blank_turn_failure() {
    let (kind, message) = stream_error(serde_json::json!({ "type": "error" })).await;
    assert_eq!(kind, ProviderErrorKind::Unknown);
    assert_eq!(
        message,
        "Responses stream returned an error without a message"
    );
}

#[tokio::test]
async fn failed_response_with_empty_message_preserves_error_type() {
    let (kind, message) = stream_error(serde_json::json!({
        "type": "response.failed",
        "response": { "error": { "type": "server_error", "message": "" } }
    }))
    .await;
    assert_eq!(kind, ProviderErrorKind::Overloaded);
    assert_eq!(
        message,
        "Responses stream reported a failed response without a message"
    );
}

#[tokio::test]
async fn failed_response_fallback_preserves_structured_error_identity() {
    for message in ["", " \t\r\n", "  provider failure detail  "] {
        let error = stream_error_details(serde_json::json!({
            "type": "response.failed",
            "response": { "error": {
                "type": "server_error", "code": "fixture_capacity", "message": message,
                "request_id": "req_stream_fixture"
            } }
        }))
        .await;
        assert_eq!(error.kind, ProviderErrorKind::Overloaded);
        assert_eq!(error.status, None);
        assert_eq!(error.code, Some("fixture_capacity".into()));
        assert_eq!(error.error_type, Some("server_error".into()));
        assert_eq!(error.request_id, Some("req_stream_fixture".into()));
        let expected = if message.trim().is_empty() {
            "Responses stream reported a failed response without a message"
        } else {
            message
        };
        assert_eq!(
            String::from_utf8(error.message.to_node_utf8()).unwrap(),
            expected
        );
    }
}
