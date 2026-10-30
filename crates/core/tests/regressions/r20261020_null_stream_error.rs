//! Defect: successful provider responses with `error: null` failed before output.
//! Root cause: stream error detection treated a nullable field as an error value.
//! Work item: 20261020 nullable stream error detection.

use std::time::Duration;

use futures::StreamExt;
use klaus_agent_core::provider::openai::{OpenAIProvider, OpenAIProviderOptions, OpenAiApi};
use klaus_agent_core::provider::retry::RetryOptions;
use klaus_agent_core::schemas::content::{ContentBlock, TextBlock};
use klaus_agent_core::schemas::message::{Message, UserMessage};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::ProviderErrorKind;
use klaus_agent_core::types::provider::{ModelProvider, ModelRequest, ModelStreamEvent};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn request() -> ModelRequest {
    ModelRequest {
        supports_image_detail_original: false,
        model: "nullable-error-fixture".into(),
        image_limits: None,
        max_media_blocks: None,
        system: vec![],
        messages: vec![Message::User(UserMessage {
            question_answer: None,
            uuid: "nullable-error-request".into(),
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
        tool_choice: None,
        max_output_tokens: 100,
        task_budget: None,
        output_schema: None,
        web_search: None,
        thinking: None,
        effort: None,
        model_verbosity: None,
        reasoning_summary: Default::default(),
        skip_cache_write: None,
        cache_ttl: None,
        cache_key: None,
        context_management: None,
        context_hints: None,
        thinking_context: None,
    }
}

fn sse(events: &[Value]) -> String {
    events
        .iter()
        .map(|event| format!("data: {event}\n\n"))
        .collect()
}

async fn stream(api: OpenAiApi, content_type: &str, body: String) -> Vec<ModelStreamEvent> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut received = Vec::new();
        let mut buffer = [0; 4096];
        loop {
            let count = socket.read(&mut buffer).await.unwrap();
            assert!(count > 0, "the complete request must arrive");
            received.extend_from_slice(&buffer[..count]);
            let Some(end) = received.windows(4).position(|bytes| bytes == b"\r\n\r\n") else {
                continue;
            };
            let headers = String::from_utf8_lossy(&received[..end]);
            let length = headers
                .lines()
                .filter_map(|line| line.split_once(':'))
                .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                .unwrap()
                .1
                .trim()
                .parse::<usize>()
                .unwrap();
            if received.len() >= end + 4 + length {
                break;
            }
        }
        socket.write_all(response.as_bytes()).await.unwrap();
        socket.shutdown().await.unwrap();
    });
    let provider = OpenAIProvider::new(OpenAIProviderOptions {
        api_key: Some("fixture-key".into()),
        base_url: Some(base_url),
        api: Some(api),
        retry: Some(RetryOptions {
            max_attempts: Some(1),
            congestion_hold_ms: Some(0),
            unbounded_network_retries: Some(false),
            ..Default::default()
        }),
        ..Default::default()
    });
    let events = tokio::time::timeout(Duration::from_secs(5), async {
        provider
            .stream(request(), AbortSignal::new())
            .collect()
            .await
    })
    .await
    .expect("the provider must finish after one local response");
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("the local server must finish after its response")
        .unwrap();
    events
}

fn assert_completed(events: &[ModelStreamEvent]) {
    let mut text = String::new();
    for event in events {
        match event {
            ModelStreamEvent::TextDelta { text: delta } => {
                text.push_str(&String::from_utf8(delta.to_node_utf8()).unwrap());
            }
            ModelStreamEvent::Error { error } => panic!("unexpected provider error: {error:?}"),
            _ => {}
        }
    }
    assert_eq!(text, "Hello");
    assert!(events
        .iter()
        .any(|event| matches!(event, ModelStreamEvent::MessageStart { .. })));
    assert!(events
        .iter()
        .any(|event| matches!(event, ModelStreamEvent::MessageStop { .. })));
}

#[tokio::test]
async fn responses_nullable_error_prelude_reaches_completion() {
    let events = stream(
        OpenAiApi::Responses,
        "text/event-stream",
        sse(&[
            json!({"type":"response.created","response":{"model":"nullable-error-fixture","error":null,"output":[],"usage":null}}),
            json!({"type":"response.in_progress","response":{"error":null,"output":[],"usage":null}}),
            json!({"type":"response.output_text.delta","delta":"Hello"}),
            json!({"type":"response.completed","response":{"error":null,"status":"completed","output":[],"usage":{"input_tokens":10,"output_tokens":1,"total_tokens":11}}}),
        ]),
    )
    .await;
    assert_completed(&events);
}

#[tokio::test]
async fn chat_nullable_error_prelude_deltas_and_usage_reach_completion() {
    let body = sse(&[
        json!({"error":null,"choices":[]}),
        json!({"error":null,"model":"nullable-error-fixture","choices":[{"delta":{"role":"assistant","content":"Hello"},"finish_reason":null}]}),
        json!({"error":null,"choices":[{"delta":{},"finish_reason":"stop"}]}),
        json!({"error":null,"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":1,"total_tokens":11}}),
    ]) + "data: [DONE]\n\n";
    let events = stream(OpenAiApi::Chat, "text/event-stream", body).await;
    assert_completed(&events);
    assert!(events.iter().any(|event| matches!(
        event,
        ModelStreamEvent::UsageSnapshot { usage } if usage.input_tokens == 10 && usage.output_tokens == 1
    )));
}

#[tokio::test]
async fn buffered_chat_nullable_error_reaches_completion() {
    let events = stream(
        OpenAiApi::Chat,
        "application/json",
        json!({"error":null,"model":"nullable-error-fixture","choices":[{"message":{"role":"assistant","content":"Hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":1,"total_tokens":11}}).to_string(),
    )
    .await;
    assert_completed(&events);
}

#[tokio::test]
async fn non_null_error_values_remain_failures() {
    for value in [json!(false), json!({})] {
        let events = stream(
            OpenAiApi::Chat,
            "text/event-stream",
            sse(&[json!({"error":value})]),
        )
        .await;
        assert!(
            matches!(events.as_slice(), [ModelStreamEvent::Error { error }] if error.kind == ProviderErrorKind::Unknown)
        );
    }
}

#[tokio::test]
async fn real_error_preserves_classification_and_details() {
    for api in [OpenAiApi::Chat, OpenAiApi::Responses] {
        let error = json!({"error_code":401,"error":{"type":"authentication_error","code":"invalid_api_key","message":"fixture authentication failed","request_id":"fixture-request"}});
        let events = stream(api, "text/event-stream", sse(&[error])).await;
        let [ModelStreamEvent::Error { error }] = events.as_slice() else {
            panic!("expected the original provider error: {events:?}");
        };
        assert_eq!(error.kind, ProviderErrorKind::Auth);
        assert_eq!(error.status, Some(401));
        assert_eq!(error.message, "fixture authentication failed");
        assert_eq!(error.code, Some("invalid_api_key".into()));
        assert_eq!(error.error_type, Some("authentication_error".into()));
        assert_eq!(error.request_id, Some("fixture-request".into()));
    }
}

#[tokio::test]
async fn explicit_responses_error_with_null_nested_error_remains_a_failure() {
    let events = stream(
        OpenAiApi::Responses,
        "text/event-stream",
        sse(&[json!({"type":"error","error":null,"message":"fixture rate limited","code":"rate_limit_exceeded"})]),
    )
    .await;
    let [ModelStreamEvent::Error { error }] = events.as_slice() else {
        panic!("expected the explicit Responses error: {events:?}");
    };
    assert_eq!(error.kind, ProviderErrorKind::RateLimit);
    assert_eq!(error.message, "fixture rate limited");
}
