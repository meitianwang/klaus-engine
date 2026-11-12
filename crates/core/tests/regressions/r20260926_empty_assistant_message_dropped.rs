//! Defect: a session whose model spent its whole output budget on reasoning and
//! returned nothing visible (content `[]`, no tool call) persisted an empty
//! assistant message. On the next request the OpenAI Chat Completions wire
//! rendered it as a bare `{"role":"assistant"}` — neither `content` nor
//! `tool_calls` — and the gateway rejected every later turn with
//! `Assistant message must contain either 'content' or 'tool_calls'!`. Because
//! the rollout is append-only, the bad message poisoned the whole session: it
//! wedged on a 400 within ~40ms on each retry until the context changed.
//! Root cause: `to_chat_messages_with_profile` pushed the assistant object
//! unconditionally, while Anthropic (`!content.is_empty()`), Gemini
//! (`!parts.is_empty()`) and the Responses wire (`flush_text` only on non-empty)
//! all already skip an assistant turn with no renderable payload.
//! Fixed by: dropping an assistant message with neither text nor tool_calls on
//! the Chat Completions wire, matching the other three. The empty message stays
//! in the rollout (its usage accounting is preserved); it is simply never sent.

use futures::StreamExt;
use klaus_agent_core::provider::openai::{OpenAIProvider, OpenAIProviderOptions, OpenAiApi};
use klaus_agent_core::provider::retry::RetryOptions;
use klaus_agent_core::schemas::content::{ContentBlock, TextBlock};
use klaus_agent_core::schemas::message::{AssistantMessage, Message, UserMessage};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{ModelProvider, ModelRequest};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn single_attempt() -> RetryOptions {
    RetryOptions {
        max_attempts: Some(1),
        congestion_hold_ms: Some(0),
        ..Default::default()
    }
}

fn user(text: &str) -> Message {
    Message::User(UserMessage {
        question_answer: None,
        uuid: format!("u-{text}").into(),
        content: vec![ContentBlock::Text(TextBlock {
            text: text.into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })],
        is_meta: None,
    })
}

/// The exact history shape the wedged session carried: a user turn, then an
/// assistant message whose content is an EMPTY array (the reasoning-only
/// response that produced nothing visible), then another user turn.
fn poisoned_history() -> Vec<Message> {
    vec![
        user("keep analysing"),
        Message::Assistant(AssistantMessage {
            uuid: "a-empty".into(),
            content: vec![],
            model: "qwen3.8-27b-abliterated-q4".into(),
            usage: None,
        }),
        user("and now?"),
    ]
}

fn request_with_poisoned_history() -> ModelRequest {
    ModelRequest {
        supports_image_detail_original: false,
        model_verbosity: None,
        reasoning_summary: Default::default(),
        model: "qwen3.8-27b-abliterated-q4".into(),
        image_limits: None,
        max_media_blocks: None,
        system: vec![],
        messages: poisoned_history(),
        tools: vec![],
        native_deferred_tools: None,
        deferred_tools: Vec::new(),
        tool_choice: None,
        max_output_tokens: 16384,
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

/// Capture the provider's real HTTP body, then reject the fixture request.
async fn capture_chat_body(request: ModelRequest) -> Value {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let mut server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut received = Vec::new();
        let mut buffer = [0u8; 4096];
        let body = loop {
            let count = stream.read(&mut buffer).await.unwrap();
            assert!(count > 0, "the complete request must reach the fixture");
            received.extend_from_slice(&buffer[..count]);
            if let Some(end) = received.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&received[..end]);
                let length = headers
                    .lines()
                    .filter_map(|line| line.split_once(':'))
                    .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                    .map(|(_, length)| length.trim().parse::<usize>().unwrap())
                    .expect("provider JSON requests declare their content length");
                if received.len() >= end + 4 + length {
                    break serde_json::from_slice::<Value>(&received[end + 4..end + 4 + length])
                        .unwrap();
                }
            }
        };
        let rejection =
            r#"{"error":{"type":"invalid_request_error","message":"Captured test request"}}"#;
        let response = format!(
            "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{rejection}",
            rejection.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
        stream.shutdown().await.unwrap();
        body
    });
    let provider = OpenAIProvider::new(OpenAIProviderOptions {
        api_key: Some("test-key".into()),
        base_url: Some(base_url),
        api: Some(OpenAiApi::Chat),
        retry: Some(single_attempt()),
        ..Default::default()
    });
    let outcome = tokio::time::timeout(Duration::from_secs(5), async {
        let mut stream = provider.stream(request, AbortSignal::new());
        while stream.next().await.is_some() {}
        (&mut server).await.unwrap()
    })
    .await;
    match outcome {
        Ok(body) => body,
        Err(_) => {
            server.abort();
            panic!("the local provider request must finish within five seconds")
        }
    }
}

#[tokio::test]
async fn empty_assistant_message_never_reaches_the_chat_wire() {
    let body = capture_chat_body(request_with_poisoned_history()).await;
    let messages = body["messages"].as_array().expect("chat messages array");

    // The poisoned assistant turn is gone from the wire; only the two user turns
    // survive. A bare `{"role":"assistant"}` would be the 400 trigger.
    let assistants: Vec<&Value> = messages
        .iter()
        .filter(|message| message["role"] == json!("assistant"))
        .collect();
    assert!(
        assistants.is_empty(),
        "the empty assistant message must be dropped, got: {assistants:?}"
    );
    let users: Vec<&Value> = messages
        .iter()
        .filter(|message| message["role"] == json!("user"))
        .collect();
    assert_eq!(users.len(), 2, "both user turns survive: {messages:?}");

    // No assistant object anywhere on the wire lacks both content and tool_calls.
    for message in messages {
        if message["role"] == json!("assistant") {
            let has_content = message.get("content").is_some_and(|c| !c.is_null());
            let has_calls = message
                .get("tool_calls")
                .and_then(Value::as_array)
                .is_some_and(|calls| !calls.is_empty());
            assert!(
                has_content || has_calls,
                "a Chat Completions assistant message must carry content or tool_calls: {message}"
            );
        }
    }
}
