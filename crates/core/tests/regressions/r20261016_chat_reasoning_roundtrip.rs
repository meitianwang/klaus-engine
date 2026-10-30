//! Defect: Chat tool continuations discarded the model's returned reasoning.
//! Root cause: the history adapter emitted text and tools but omitted reasoning_content.
//! Work item 20261016: retain same-model plaintext reasoning on its native channel.

use futures::StreamExt;
use klaus_agent_core::provider::openai::{OpenAIProvider, OpenAIProviderOptions};
use klaus_agent_core::runtime_values::{
    runtime_codec, runtime_json, RuntimeCodecError, RuntimeDecode, RuntimeString,
};
use klaus_agent_core::schemas::content::{ContentBlock, ThinkingBlock};
use klaus_agent_core::schemas::message::{AssistantMessage, Message};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{ModelProvider, ModelRequest, ModelStreamEvent};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn decode<T: RuntimeDecode>(value: Value) -> Result<T, RuntimeCodecError> {
    let exact = runtime_json::parse(&value.to_string().into()).unwrap();
    runtime_codec::decode_value(&exact)
}

fn request() -> ModelRequest {
    decode(json!({
        "model":"fixture-model", "maxOutputTokens":256,
        "system":[
            {"name":"base", "text":"Complete the user's task.", "cacheable":true},
            {"name":"multi-agent-role", "text":"Continue the active task.", "cacheable":false}
        ],
        "messages":[{"type":"user","uuid":"user-1","content":[
            {"type":"text","text":"Fix the input layout and explain the result."}
        ]}],
        "tools":[], "cacheKey":"stable-session"
    }))
    .unwrap()
}

async fn captured(requests: Vec<ModelRequest>) -> Vec<Value> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let count = requests.len();
    let server = tokio::spawn(async move {
        let mut captured = Vec::new();
        for _ in 0..count {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            loop {
                let mut chunk = [0; 4096];
                let size = socket.read(&mut chunk).await.unwrap();
                assert_ne!(size, 0);
                bytes.extend_from_slice(&chunk[..size]);
                if let Some(end) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&bytes[..end]);
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            let (key, value) = line.split_once(':')?;
                            key.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap();
                    if bytes.len() >= end + 4 + length {
                        captured.push(
                            serde_json::from_slice::<Value>(&bytes[end + 4..end + 4 + length])
                                .unwrap(),
                        );
                        break;
                    }
                }
            }
            let body = concat!(
                "data: {\"model\":\"fixture-model\",\"choices\":[{\"delta\":{\"content\":\"Continue\"},\"finish_reason\":\"stop\"}]}\n\n",
                "data: [DONE]\n\n"
            );
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            socket.shutdown().await.unwrap();
        }
        captured
    });
    let provider = OpenAIProvider::new(OpenAIProviderOptions {
        base_url: Some(base_url),
        api_key: Some("fixture-key".into()),
        ..Default::default()
    });
    for request in requests {
        let events: Vec<_> = provider.stream(request, AbortSignal::new()).collect().await;
        assert!(!events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::Error { .. })));
    }
    server.await.unwrap()
}

fn thinking(text: RuntimeString, signature: Option<&str>, encrypted: Option<&str>) -> ContentBlock {
    ContentBlock::Thinking(ThinkingBlock {
        text,
        message_id: None,
        signature: signature.map(Into::into),
        encrypted_content: encrypted.map(Into::into),
    })
}

#[tokio::test]
async fn chat_tool_continuations_keep_reasoning_on_its_own_channel() {
    let mut first = request();
    let mut assistant: AssistantMessage = decode(json!({
        "uuid":"assistant-1", "model":"fixture-model", "content":[
            {"type":"text","text":"I found the relevant layout."},
            {"type":"tool_use","id":"call-1","name":"read","input":{"path":"input.css"}}
        ]
    }))
    .unwrap();
    assistant.content.splice(
        0..0,
        [
            thinking("Keep the layout task active. ".into(), None, None),
            thinking(RuntimeString::from_utf16(vec![0xd83d]), None, None),
            thinking(RuntimeString::from_utf16(vec![0xde80]), None, None),
        ],
    );
    first.messages.push(Message::Assistant(assistant));
    first.messages.push(
        decode(json!({
            "type":"tool_result","uuid":"result-1","toolUseId":"call-1",
            "isError":false,"content":[{"type":"text","text":"The layout still needs a fix."}]
        }))
        .unwrap(),
    );
    let mut second = first.clone();
    second.messages.push(decode(json!({
        "type":"user","uuid":"user-2","content":[{"type":"text","text":"Also check the narrow layout."}]
    })).unwrap());
    let wire = captured(vec![first, second]).await;
    for request in &wire {
        let messages = request["messages"].as_array().unwrap();
        assert_eq!(messages[1]["role"], "user");
        assert_eq!(
            messages[2]["reasoning_content"],
            "Keep the layout task active. 🚀"
        );
        assert_eq!(messages[2]["content"], "I found the relevant layout.");
        assert_eq!(messages[2]["tool_calls"][0]["id"], "call-1");
        assert_eq!(messages[3]["role"], "tool");
        assert_eq!(messages.last().unwrap()["role"], "system");
        assert_eq!(request["prompt_cache_key"], "stable-session");
    }
    assert_eq!(
        wire[0]["messages"].as_array().unwrap()[..4],
        wire[1]["messages"].as_array().unwrap()[..4]
    );
}

#[tokio::test]
async fn chat_replay_never_projects_foreign_or_protected_reasoning() {
    let mut request = request();
    for (index, model, signature, encrypted) in [
        (0, "previous-model", None, None),
        (1, "fixture-model", Some("signed"), None),
        (2, "fixture-model", None, Some("encrypted")),
        (3, "fixture-model", None, None),
    ] {
        let mut assistant: AssistantMessage = decode(json!({
            "uuid":format!("assistant-{index}"),"model":model,
            "content":[{"type":"text","text":"Visible result"}]
        }))
        .unwrap();
        assistant
            .content
            .insert(0, thinking("Private state".into(), signature, encrypted));
        request.messages.push(Message::Assistant(assistant));
    }
    let wire = captured(vec![request]).await;
    let messages = wire[0]["messages"].as_array().unwrap();
    for message in &messages[2..5] {
        assert!(message.get("reasoning_content").is_none());
        assert_eq!(message["content"], "Visible result");
    }
    assert_eq!(messages[5]["reasoning_content"], "Private state");
}
