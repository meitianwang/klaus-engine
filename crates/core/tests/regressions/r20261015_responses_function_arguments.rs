//! Defect: function arguments delivered in completion events became empty tool inputs.
//! Root cause: the Responses adapter read deltas but ignored argument snapshots.
//! Work item 20261015: preserve complete function arguments before tool execution.

use futures::StreamExt;
use klaus_agent_core::provider::openai::{OpenAIProvider, OpenAIProviderOptions, OpenAiApi};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{ModelProvider, ModelRequest, ModelStreamEvent};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn item(index: usize, arguments: &str) -> Value {
    json!({"type":"function_call","id":format!("item-{index}"),
        "call_id":format!("call-{index}"),"name":"read","arguments":arguments})
}

fn added(index: usize, arguments: &str) -> Value {
    json!({"type":"response.output_item.added","item":item(index, arguments)})
}

fn done(index: usize, arguments: &str) -> Value {
    json!({"type":"response.output_item.done","item":item(index, arguments)})
}

fn delta(index: usize, arguments: &str) -> Value {
    json!({"type":"response.function_call_arguments.delta",
        "item_id":format!("item-{index}"),"delta":arguments})
}

fn arguments_done(index: usize, arguments: &str) -> Value {
    json!({"type":"response.function_call_arguments.done",
        "item_id":format!("item-{index}"),"arguments":arguments})
}

async fn stream(mut events: Vec<Value>) -> Vec<ModelStreamEvent> {
    events.push(json!({"type":"response.completed","response":{
        "output":[item(0, "{}")],"usage":{"input_tokens":1,"output_tokens":1}
    }}));
    let body: String = events
        .iter()
        .map(|event| format!("data: {event}\n\n"))
        .collect();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        loop {
            let mut bytes = [0_u8; 4096];
            let count = socket.read(&mut bytes).await.unwrap();
            assert_ne!(count, 0);
            request.extend_from_slice(&bytes[..count]);
            if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&request[..end]);
                let length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                if request.len() >= end + 4 + length {
                    break;
                }
            }
        }
        let response = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
        socket.write_all(response.as_bytes()).await.unwrap();
        socket.shutdown().await.unwrap();
    });
    let provider = OpenAIProvider::new(OpenAIProviderOptions {
        api: Some(OpenAiApi::Responses),
        api_key: Some("fixture-key".into()),
        base_url: Some(base_url),
        ..Default::default()
    });
    let request = ModelRequest {
        supports_image_detail_original: false,
        model_verbosity: None,
        reasoning_summary: Default::default(),
        model: "fixture-model".into(),
        image_limits: None,
        max_media_blocks: None,
        system: vec![],
        messages: vec![],
        tools: vec![],
        deferred_tools: Vec::new(),
        tool_choice: None,
        max_output_tokens: 128,
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
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        provider.stream(request, AbortSignal::new()).collect(),
    )
    .await
    .unwrap();
    server.await.unwrap();
    result
}

fn inputs(events: &[ModelStreamEvent]) -> BTreeMap<String, Value> {
    let mut pending = BTreeMap::<String, String>::new();
    let mut completed = BTreeMap::new();
    for event in events {
        match event {
            ModelStreamEvent::ToolUseStart { id, .. } => {
                assert!(pending
                    .insert(id.try_to_utf8().unwrap(), String::new())
                    .is_none());
            }
            ModelStreamEvent::ToolUseInputDelta { id, partial_json } => {
                pending
                    .get_mut(&id.try_to_utf8().unwrap())
                    .unwrap()
                    .push_str(&partial_json.try_to_utf8().unwrap());
            }
            ModelStreamEvent::ToolUseStop { id } => {
                let text = pending.remove(&id.try_to_utf8().unwrap()).unwrap();
                completed.insert(
                    id.try_to_utf8().unwrap(),
                    serde_json::from_str(&text).unwrap(),
                );
            }
            ModelStreamEvent::Error { error } => panic!("unexpected error: {error:?}"),
            _ => {}
        }
    }
    assert!(pending.is_empty());
    completed
}

#[tokio::test]
async fn public_provider_recovers_function_arguments_from_each_snapshot_source() {
    let arguments = r#"{"path":"/tmp/fixture.txt","limit":7}"#;
    let sequences = [
        vec![added(0, arguments), done(0, arguments)],
        vec![
            added(0, ""),
            arguments_done(0, arguments),
            done(0, arguments),
        ],
        vec![added(0, ""), done(0, arguments)],
        vec![
            added(0, ""),
            delta(0, "{\"path\":"),
            arguments_done(0, arguments),
            done(0, arguments),
        ],
        vec![
            added(0, ""),
            delta(0, arguments),
            arguments_done(0, arguments),
            done(0, arguments),
            done(0, arguments),
        ],
        vec![
            added(0, ""),
            delta(0, "{\"path\":"),
            added(0, ""),
            done(0, arguments),
        ],
    ];
    for sequence in sequences {
        let events = stream(sequence).await;
        assert_eq!(
            inputs(&events),
            BTreeMap::from([(
                "call-0".to_owned(),
                serde_json::from_str::<Value>(arguments).unwrap()
            )])
        );
    }
}

#[tokio::test]
async fn parallel_function_snapshots_keep_arguments_with_their_calls() {
    let mut sequence: Vec<_> = (0..6).map(|index| added(index, "")).collect();
    for index in (0..6).rev() {
        let arguments = json!({"path":format!("/tmp/file-{index}.txt")}).to_string();
        sequence.push(arguments_done(index, &arguments));
        sequence.push(done(index, &arguments));
    }
    let events = stream(sequence).await;
    let expected = (0..6)
        .map(|index| {
            (
                format!("call-{index}"),
                json!({"path":format!("/tmp/file-{index}.txt")}),
            )
        })
        .collect();
    assert_eq!(inputs(&events), expected);
}

#[tokio::test]
async fn conflicting_function_snapshots_do_not_commit_tool_execution() {
    let events = stream(vec![
        added(0, ""),
        added(1, ""),
        done(1, r#"{"path":"/tmp/accepted.txt"}"#),
        delta(0, "{\"path\":\"old"),
        arguments_done(0, r#"{"path":"new"}"#),
        done(0, r#"{"path":"new"}"#),
    ])
    .await;
    assert!(!events.iter().any(|event| matches!(
        event,
        ModelStreamEvent::ToolUseStop { .. } | ModelStreamEvent::MessageStop { .. }
    )));
    assert!(
        matches!(events.last(), Some(ModelStreamEvent::Error { error })
        if error.code.as_ref().is_some_and(|code| code == "response_tool_arguments_mismatch"))
    );
}
