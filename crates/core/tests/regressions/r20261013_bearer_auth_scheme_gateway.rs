//! Defect: a custom Anthropic-wire entry behind a gateway that validates only
//! the `Authorization: Bearer` carrier could never authenticate: the provider
//! spent the stored key as `x-api-key` unconditionally, and such gateways
//! answer that header (and both headers together) with INVALID_API_KEY.
//! Root cause: the Anthropic provider had no way to spend a stored API key on
//! the bearer carrier; the scheme was hardcoded at the two request sites.
//! Work item: 2026-10-13 bearer auth scheme for custom model providers;
//! public provider surface.

use std::time::Duration;

use futures::StreamExt;
use klaus_agent_core::provider::anthropic::{AnthropicProvider, AnthropicProviderOptions};
use klaus_agent_core::provider::auth::ApiKeyScheme;
use klaus_agent_core::provider::retry::RetryOptions;
use klaus_agent_core::schemas::content::{ContentBlock, TextBlock};
use klaus_agent_core::schemas::message::{Message, UserMessage};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{ModelProvider, ModelRequest};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const MODEL: &str = "gateway-bearer-fixture";
const WAIT: Duration = Duration::from_secs(15);

fn text(value: &str) -> TextBlock {
    TextBlock {
        text: value.into(),
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    }
}

fn request() -> ModelRequest {
    ModelRequest {
        supports_image_detail_original: false,
        model_verbosity: None,
        reasoning_summary: Default::default(),
        model: MODEL.into(),
        image_limits: None,
        max_media_blocks: None,
        system: vec![],
        messages: vec![Message::User(UserMessage {
            uuid: "bearer-scheme".into(),
            question_answer: None,
            content: vec![ContentBlock::Text(text("hi"))],
            is_meta: None,
        })],
        tools: vec![],
        deferred_tools: Vec::new(),
        tool_choice: None,
        max_output_tokens: 64,
        task_budget: None,
        output_schema: None,
        web_search: None,
        thinking: None,
        effort: None,
        skip_cache_write: None,
        cache_ttl: None,
        cache_domain: None,
        cache_key: None,
        context_management: None,
        context_hints: None,
        thinking_context: None,
    }
}

/// Serve exactly one request and hand back its raw header block, answering
/// with the smallest SSE payload the provider accepts as a finished turn.
async fn capture_one_request(listener: TcpListener) -> String {
    let (mut socket, _) = listener.accept().await.unwrap();
    let mut received = Vec::new();
    let mut header_end = None;
    let mut content_length = None;
    loop {
        let mut chunk = [0_u8; 4096];
        let read = socket.read(&mut chunk).await.unwrap();
        assert_ne!(read, 0, "the request must complete before EOF");
        received.extend_from_slice(&chunk[..read]);
        if header_end.is_none() {
            header_end = received
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|position| position + 4);
            if let Some(end) = header_end {
                let headers = std::str::from_utf8(&received[..end]).unwrap();
                content_length = headers.lines().find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                });
            }
        }
        if let (Some(end), Some(length)) = (header_end, content_length) {
            if received.len() >= end + length {
                break;
            }
        }
    }
    let event = "data: {\"type\":\"message_stop\"}\n\n";
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{event}",
        event.len()
    );
    socket.write_all(response.as_bytes()).await.unwrap();
    String::from_utf8(received[..header_end.unwrap()].to_vec()).unwrap()
}

fn header_line(headers: &str, name: &str) -> Option<String> {
    headers.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.eq_ignore_ascii_case(name)
            .then(|| value.trim().to_string())
    })
}

async fn one_request_with_scheme(scheme: ApiKeyScheme) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(capture_one_request(listener));
    let provider = AnthropicProvider::new(AnthropicProviderOptions {
        api_key: Some("fixture-key".into()),
        base_url: Some(format!("http://{address}")),
        api_key_scheme: scheme,
        retry: Some(RetryOptions {
            max_attempts: Some(1),
            congestion_hold_ms: Some(0),
            ..Default::default()
        }),
        ..Default::default()
    });
    let events = tokio::time::timeout(
        WAIT,
        provider
            .stream(request(), AbortSignal::new())
            .collect::<Vec<_>>(),
    )
    .await
    .expect("the fixture request settles locally");
    assert!(!events.is_empty(), "the turn produced stream events");
    tokio::time::timeout(WAIT, server)
        .await
        .expect("the request reached the local server")
        .unwrap()
}

/// A bearer-scheme entry spends its stored key on the Authorization carrier
/// and leaves `x-api-key` off the wire entirely: a gateway that validates only
/// the bearer scheme rejects the native header outright.
#[tokio::test]
async fn bearer_scheme_spends_the_key_on_the_authorization_header() {
    let headers = one_request_with_scheme(ApiKeyScheme::Bearer).await;
    assert_eq!(
        header_line(&headers, "authorization").as_deref(),
        Some("Bearer fixture-key"),
        "the stored key must ride the bearer carrier: {headers}"
    );
    assert!(
        header_line(&headers, "x-api-key").is_none(),
        "the native header must not leak next to the bearer one: {headers}"
    );
}

/// The default stays byte-identical to before the knob existed: first-party
/// and every pre-existing entry keep spending the key as `x-api-key`.
#[tokio::test]
async fn the_default_scheme_still_spends_the_key_as_x_api_key() {
    let headers = one_request_with_scheme(ApiKeyScheme::default()).await;
    assert_eq!(
        header_line(&headers, "x-api-key").as_deref(),
        Some("fixture-key"),
        "the default carrier must stay x-api-key: {headers}"
    );
    assert!(
        header_line(&headers, "authorization").is_none(),
        "an unconfigured entry must not start sending Authorization: {headers}"
    );
}
