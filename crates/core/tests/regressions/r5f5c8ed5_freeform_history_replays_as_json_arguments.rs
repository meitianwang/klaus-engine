//! Defect: a session that ran a freeform tool (apply_patch) under the
//! Responses wire carried the call's raw patch TEXT in history; after the
//! session switched to a chat-completions model (Qwen via the QwenWork/Qoder
//! gateway) every replayed request serialized that input as a quoted string
//! literal in `function.arguments`, and the gateway rejected the whole turn
//! with `<400> InternalError.Algo.InvalidParameter: The "function.arguments"
//! parameter of the code model must be in JSON format.` — the session died on
//! every subsequent message until the model was switched back.
//! Root cause: the chat wire (and the Anthropic/Gemini object-shaped wires)
//! serialized tool inputs verbatim, while only the Responses wire has a
//! dedicated raw-text slot (`custom_tool_call.input`).
//! Fixed by: objectifying non-object tool inputs on object-shaped wires,
//! wrapping the raw text under the single string property the tool's own
//! schema declares (apply_patch: `patch`), so the replayed arguments parse as
//! a JSON object and still validate against the declared schema.

use futures::StreamExt;
use klaus_agent_core::provider::openai::{OpenAIProvider, OpenAIProviderOptions, OpenAiApi};
use klaus_agent_core::provider::retry::RetryOptions;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimeString};
use klaus_agent_core::schemas::content::{ContentBlock, OutputBlock, TextBlock, ToolUseBlock};
use klaus_agent_core::schemas::message::{AssistantMessage, Message, ToolResultMessage};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ProviderToolCallKind, ToolSpec, ToolSpecFormat, ToolSpecKind,
};
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

fn apply_patch_spec() -> ToolSpec {
    ToolSpec {
        name: "apply_patch".into(),
        description: "Apply a patch.".into(),
        input_schema: runtime_json::parse(&RuntimeString::from(
            r#"{"type":"object","properties":{"patch":{"type":"string"}},"required":["patch"],"additionalProperties":false}"#,
        ))
        .expect("schema literal must parse"),
        kind: ToolSpecKind::Freeform,
        format: Some(ToolSpecFormat::Text),
    }
}

/// The exact history shape the dead session carried: an assistant turn whose
/// apply_patch input is raw patch text (the Responses wire's custom format),
/// followed by its tool result.
fn freeform_history() -> Vec<Message> {
    vec![
        Message::Assistant(AssistantMessage {
            uuid: "a-patch".into(),
            content: vec![ContentBlock::ToolUse(ToolUseBlock {
                id: "call_patch".into(),
                name: "apply_patch".into(),
                input: RuntimeJson::String(RuntimeString::from(
                    "*** Begin Patch\n*** Update File: a.ts\n@@\n+x\n*** End Patch\n",
                )),
                tool_kind: Some(ProviderToolCallKind::Custom),
                thought_signature: None,
            })],
            model: "gpt-5".into(),
            usage: None,
        }),
        Message::ToolResult(ToolResultMessage {
            uuid: "r-patch".into(),
            tool_use_id: "call_patch".into(),
            content: vec![OutputBlock::Text(TextBlock {
                text: "applied".into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_error: false,
            tool_kind: Some(ProviderToolCallKind::Custom),
            native_tool_output: None,
            output: None,
            extra_user_blocks: None,
        }),
    ]
}

fn request_with_history() -> ModelRequest {
    ModelRequest {
        supports_image_detail_original: false,
        model_verbosity: None,
        reasoning_summary: Default::default(),
        model: "qwen3.8-max".into(),
        image_limits: None,
        max_media_blocks: None,
        system: vec![],
        messages: freeform_history(),
        tools: vec![apply_patch_spec()],
        deferred_tools: Vec::new(),
        tool_choice: None,
        max_output_tokens: 1000,
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
async fn replayed_freeform_history_sends_json_object_arguments_on_the_chat_wire() {
    let body = capture_chat_body(request_with_history()).await;
    let messages = body["messages"].as_array().expect("chat messages array");
    let assistant = messages
        .iter()
        .find(|message| message["role"] == json!("assistant"))
        .expect("the replayed assistant turn must be on the wire");
    let arguments = assistant["tool_calls"][0]["function"]["arguments"]
        .as_str()
        .expect("chat wire arguments are a string")
        .to_string();
    let parsed: Value = serde_json::from_str(&arguments).unwrap_or_else(|error| {
        panic!("the gateway rejects non-JSON arguments; got {arguments:?}: {error}")
    });
    assert_eq!(
        parsed,
        json!({ "patch": "*** Begin Patch\n*** Update File: a.ts\n@@\n+x\n*** End Patch\n" }),
        "the raw patch must survive under the schema-declared key"
    );
}
