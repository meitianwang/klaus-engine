//! Defect: a session that received a queued user steer (or an elicitation
//! answer) between an assistant message's tool_calls and the matching tool
//! results projected that user message straight into the Chat Completions wire
//! inside the tool batch. Strict OpenAI-compatible validators reject the shape
//! with `An assistant message with 'tool_calls' must be followed by tool
//! messages responding to each 'tool_call_id'`, wedging every later request of
//! the session on a 400 — exactly what a long-lived session hit after
//! compaction replayed the interleaved record.
//! Root cause: `to_chat_messages_with_profile` emitted messages in record order
//! and only guarded media interleaving, never user-message interleaving, inside
//! an open tool batch.
//! Fixed by: holding user messages back while a batch is open and flushing them
//! right after the batch closes; a batch whose calls never got results (aborted
//! execution) is settled with one synthetic `tool` message per call so the next
//! assistant message starts a clean window.

use futures::StreamExt;
use klaus_agent_core::provider::openai::{OpenAIProvider, OpenAIProviderOptions, OpenAiApi};
use klaus_agent_core::provider::retry::RetryOptions;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeString};
use klaus_agent_core::schemas::content::{ContentBlock, OutputBlock, TextBlock, ToolUseBlock};
use klaus_agent_core::schemas::message::{
    AssistantMessage, Message, ToolResultMessage, UserMessage,
};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{ModelProvider, ModelRequest};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Tool-use inputs are exact-runtime JSON, not Serde values.
macro_rules! rjson {
    ($($value:tt)*) => {
        runtime_json::parse(&RuntimeString::from(json!($($value)*).to_string())).unwrap()
    };
}

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

fn assistant_with_call(text: &str, call_id: &str) -> Message {
    Message::Assistant(AssistantMessage {
        uuid: format!("a-{call_id}").into(),
        content: vec![
            ContentBlock::Text(TextBlock {
                text: text.into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            }),
            ContentBlock::ToolUse(ToolUseBlock {
                id: call_id.into(),
                name: "grep".into(),
                input: rjson!({ "pattern": "advisor" }),
                tool_kind: None,
                thought_signature: None,
            }),
        ],
        model: "qwen3.8-max".into(),
        usage: None,
    })
}

fn assistant_text(text: &str) -> Message {
    Message::Assistant(AssistantMessage {
        uuid: format!("a-{text}").into(),
        content: vec![ContentBlock::Text(TextBlock {
            text: text.into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })],
        model: "qwen3.8-max".into(),
        usage: None,
    })
}

fn tool_result(call_id: &str, text: &str) -> Message {
    Message::ToolResult(ToolResultMessage {
        uuid: format!("t-{call_id}").into(),
        tool_use_id: call_id.into(),
        content: vec![OutputBlock::Text(TextBlock {
            text: text.into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })],
        is_error: false,
        tool_kind: None,
        native_tool_output: None,
        output: None,
        extra_user_blocks: None,
    })
}

fn request_with(messages: Vec<Message>) -> ModelRequest {
    ModelRequest {
        supports_image_detail_original: false,
        model_verbosity: None,
        reasoning_summary: Default::default(),
        model: "qwen3.8-max".into(),
        image_limits: None,
        max_media_blocks: None,
        system: vec![],
        messages,
        tools: vec![],
        tool_choice: None,
        max_output_tokens: 16384,
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
    }
}

/// Capture the provider's real HTTP body, then reject the fixture request.
async fn capture_chat_body(request: ModelRequest) -> Value {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let mut server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut received = Vec::new();
        let mut buffer = [0u8; 8192];
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

/// The strict OpenAI-compatible invariant: every assistant message carrying
/// tool_calls is followed IMMEDIATELY by one `tool` message per call id, in any
/// order, before any other role appears.
fn assert_batch_contiguity(messages: &[Value]) {
    let mut i = 0;
    while i < messages.len() {
        let calls = messages[i]
            .get("tool_calls")
            .and_then(Value::as_array)
            .filter(|_| messages[i]["role"] == json!("assistant"));
        if let Some(calls) = calls {
            let ids: Vec<&str> = calls
                .iter()
                .map(|call| call["id"].as_str().unwrap())
                .collect();
            let mut seen: Vec<&str> = Vec::new();
            let mut j = i + 1;
            while j < messages.len() && seen.len() < ids.len() {
                assert_eq!(
                    messages[j]["role"],
                    json!("tool"),
                    "only tool messages may follow an assistant tool_calls batch, got: {}",
                    messages[j]
                );
                seen.push(messages[j]["tool_call_id"].as_str().unwrap());
                j += 1;
            }
            for id in &ids {
                assert!(
                    seen.contains(id),
                    "call {id} has no tool message in its batch window: {messages:?}"
                );
            }
            i = j;
        } else {
            i += 1;
        }
    }
}

fn wire_texts(messages: &[Value]) -> Vec<String> {
    messages
        .iter()
        .map(|m| {
            let role = m["role"].as_str().unwrap_or("?");
            let label = if role == "tool" {
                m.get("tool_call_id").and_then(Value::as_str)
            } else {
                m.get("content").and_then(Value::as_str)
            };
            format!("{}:{}", role, label.unwrap_or(""))
        })
        .collect()
}

#[tokio::test]
async fn mid_batch_user_steer_moves_after_the_tool_run() {
    let history = vec![
        user("investigate the advisor render path"),
        assistant_with_call("searching", "call_a"),
        user("also check the sidebar while you are at it"),
        tool_result("call_a", "three hits"),
        assistant_text("done"),
    ];
    let body = capture_chat_body(request_with(history)).await;
    let messages = body["messages"].as_array().expect("chat messages array");
    assert_batch_contiguity(messages);

    let texts = wire_texts(messages);
    assert_eq!(
        texts,
        vec![
            "user:investigate the advisor render path",
            "assistant:searching",
            "tool:call_a",
            "user:also check the sidebar while you are at it",
            "assistant:done",
        ],
        "the steer must keep its content but move behind the closed batch: {texts:?}"
    );
}

#[tokio::test]
async fn several_mid_batch_steers_keep_their_order() {
    let history = vec![
        user("go"),
        assistant_with_call("working", "call_c"),
        user("first steer"),
        user("second steer"),
        tool_result("call_c", "ok"),
    ];
    let body = capture_chat_body(request_with(history)).await;
    let messages = body["messages"].as_array().expect("chat messages array");
    assert_batch_contiguity(messages);
    assert_eq!(
        wire_texts(messages),
        vec![
            "user:go",
            "assistant:working",
            "tool:call_c",
            "user:first steer",
            "user:second steer",
        ]
    );
}

#[tokio::test]
async fn unanswered_batch_is_settled_before_the_next_assistant() {
    let history = vec![
        user("go"),
        assistant_with_call("calling", "call_b"),
        assistant_text("the run died before the result landed"),
        user("and now?"),
    ];
    let body = capture_chat_body(request_with(history)).await;
    let messages = body["messages"].as_array().expect("chat messages array");
    assert_batch_contiguity(messages);
    assert_eq!(
        wire_texts(messages),
        vec![
            "user:go",
            "assistant:calling",
            "tool:call_b",
            "assistant:the run died before the result landed",
            "user:and now?",
        ],
        "the orphan call gets a synthetic tool message: {messages:?}"
    );
    let synthetic = &messages[2];
    assert!(
        synthetic["content"]
            .as_str()
            .unwrap_or_default()
            .to_lowercase()
            .contains("no result"),
        "the synthetic result must say no result was recorded: {synthetic}"
    );
}
