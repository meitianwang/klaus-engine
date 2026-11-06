//! Defect: structured context-limit rejections reached the query loop as generic
//! request errors, so reactive compaction never had a chance to recover.
//! Root cause: provider HTTP/SSE adapters discarded the upstream error code.
//! Fixed by: 2026-09-06 context management recovery work item.

use futures::StreamExt;
use klaus_agent_core::provider::anthropic::{AnthropicProvider, AnthropicProviderOptions};
use klaus_agent_core::provider::openai::{OpenAIProvider, OpenAIProviderOptions};
use klaus_agent_core::provider::retry::RetryOptions;
use klaus_agent_core::schemas::content::{ContentBlock, TextBlock};
use klaus_agent_core::schemas::message::{Message, UserMessage};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::ProviderErrorKind;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStreamEvent, ThinkingConfig,
};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(super) fn summary_request() -> ModelRequest {
    ModelRequest {
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
                text: "Summarize the task.".into(),
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
        thinking: Some(ThinkingConfig::Disabled),
        effort: None,
        skip_cache_write: None,
        cache_ttl: None,
        cache_key: None,
        context_management: None,
        context_hints: None,
        thinking_context: None,
    }
}

pub(super) async fn serve_response(
    status: &str,
    content_type: &str,
    body: String,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut received = Vec::new();
        let mut buffer = [0u8; 4096];
        loop {
            let count = stream.read(&mut buffer).await.unwrap();
            assert!(count > 0, "request must arrive before the peer closes");
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
    (base_url, server)
}

async fn assert_provider_error(provider: &dyn ModelProvider, expected: ProviderErrorKind) {
    let error = tokio::time::timeout(Duration::from_secs(5), async {
        let mut stream = provider.stream(summary_request(), AbortSignal::new());
        while let Some(event) = stream.next().await {
            if let ModelStreamEvent::Error { error } = event {
                return error;
            }
        }
        panic!("the rejected request must emit a provider error");
    })
    .await
    .expect("a deterministic request rejection must finish promptly");
    assert_eq!(error.kind, expected, "{:?}", error.message);
}

#[tokio::test]
async fn anthropic_http_and_sse_codes_enable_context_recovery() {
    for streaming in [false, true] {
        for (code, expected) in [
            ("context_length_exceeded", ProviderErrorKind::PromptTooLong),
            ("unrelated_request_error", ProviderErrorKind::InvalidRequest),
        ] {
            let error = serde_json::json!({
                "type": "error",
                "error": {
                    "type": "invalid_request_error",
                    "code": code,
                    "message": "Request rejected",
                },
            });
            let (status, content_type, body) = if streaming {
                (
                    "200 OK",
                    "text/event-stream",
                    format!("event: error\ndata: {error}\n\n"),
                )
            } else {
                ("400 Bad Request", "application/json", error.to_string())
            };
            let (base_url, server) = serve_response(status, content_type, body).await;
            let provider = AnthropicProvider::new(AnthropicProviderOptions {
                api_key: Some("test-key".into()),
                base_url: Some(base_url),
                retry: Some(RetryOptions {
                    max_attempts: Some(1),
                    congestion_hold_ms: Some(0),
                    ..Default::default()
                }),
                ..Default::default()
            });
            assert_provider_error(&provider, expected).await;
            server.await.unwrap();
        }
    }
}

#[tokio::test]
async fn openai_top_level_context_code_enables_context_recovery() {
    let (base_url, server) = serve_response(
        "400 Bad Request",
        "application/json",
        serde_json::json!({
            "code": "context_length_exceeded",
            "message": "Request rejected",
        })
        .to_string(),
    )
    .await;
    let provider = OpenAIProvider::new(OpenAIProviderOptions {
        api_key: Some("test-key".into()),
        base_url: Some(base_url),
        retry: Some(RetryOptions {
            max_attempts: Some(1),
            congestion_hold_ms: Some(0),
            ..Default::default()
        }),
        ..Default::default()
    });
    assert_provider_error(&provider, ProviderErrorKind::PromptTooLong).await;
    server.await.unwrap();
}
