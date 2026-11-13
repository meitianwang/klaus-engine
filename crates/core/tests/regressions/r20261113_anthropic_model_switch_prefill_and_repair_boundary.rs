//! Defect: switching from Opus 5.5 to Sonnet 5.5 after a thinking-only failed turn
//! and continuing produced two errors:
//! 1. `strip_stale_thinking` fabricated a trailing `"[Thinking removed for model switch]"`
//!    assistant message, causing Anthropic to reject the continuation with HTTP 400
//!    `"This model does not support assistant message prefill. The conversation must end with a user message."`
//! 2. `context_hint_state.bind_model` wrote a `persist_history_repair` `Compacted`
//!    record that `thread_history` and `thread_index` projected as a false
//!    `CompactBoundary` (`"Context automatically compacted"`) despite zero compaction.
//!
//! Work item: 2026-11-13 Anthropic model-switch continuation prefill & repair boundary fix.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{resolve_config, Input, TurnOptions};
use klaus_agent_core::provider::anthropic::{AnthropicProvider, AnthropicProviderOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeString};
use klaus_agent_core::schemas::message::Message;
use klaus_agent_core::schemas::rollout::RolloutItem;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::store::thread_history::project_thread_history;
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::provider::ModelCapabilities;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

fn sse_event(data: Value) -> String {
    format!(
        "event: {}\ndata: {data}\n\n",
        data["type"].as_str().unwrap()
    )
}

fn reply_thinking_only_signed(model: &str, signature: &str) -> String {
    sse_event(json!({
        "type": "message_start",
        "message": {"model": model, "usage": {"input_tokens": 100, "output_tokens": 0}}
    })) + &sse_event(json!({
        "type": "content_block_start",
        "index": 0,
        "content_block": {"type": "thinking", "thinking": ""}
    })) + &sse_event(json!({
        "type": "content_block_delta",
        "index": 0,
        "delta": {"type": "signature_delta", "signature": signature}
    })) + &sse_event(json!({"type": "content_block_stop", "index": 0}))
        + &sse_event(json!({
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn"},
            "usage": {"output_tokens": 120}
        }))
        + &sse_event(json!({"type": "message_stop"}))
}

fn reply_empty(model: &str) -> String {
    sse_event(json!({
        "type": "message_start",
        "message": {"model": model, "usage": {"input_tokens": 100, "output_tokens": 0}}
    })) + &sse_event(json!({
        "type": "message_delta",
        "delta": {"stop_reason": "end_turn"},
        "usage": {"output_tokens": 0}
    })) + &sse_event(json!({"type": "message_stop"}))
}

fn reply_text(model: &str, text: &str) -> String {
    sse_event(json!({
        "type": "message_start",
        "message": {"model": model, "usage": {"input_tokens": 150, "output_tokens": 0}}
    })) + &sse_event(json!({
        "type": "content_block_start",
        "index": 0,
        "content_block": {"type": "text", "text": ""}
    })) + &sse_event(json!({
        "type": "content_block_delta",
        "index": 0,
        "delta": {"type": "text_delta", "text": text}
    })) + &sse_event(json!({"type": "content_block_stop", "index": 0}))
        + &sse_event(json!({
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn"},
            "usage": {"output_tokens": 25}
        }))
        + &sse_event(json!({"type": "message_stop"}))
}

async fn read_http_json(socket: &mut TcpStream) -> Value {
    let mut bytes = Vec::new();
    let (header_end, length) = loop {
        let mut buf = [0u8; 4096];
        let count = socket.read(&mut buf).await.unwrap();
        assert!(count > 0);
        bytes.extend_from_slice(&buf[..count]);
        if let Some(pos) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&bytes[..pos]);
            let len = headers
                .lines()
                .find_map(|line| {
                    let (k, v) = line.split_once(':')?;
                    k.eq_ignore_ascii_case("content-length")
                        .then(|| v.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            break (pos + 4, len);
        }
    };
    while bytes.len() < header_end + length {
        let mut buf = [0u8; 4096];
        let count = socket.read(&mut buf).await.unwrap();
        assert!(count > 0);
        bytes.extend_from_slice(&buf[..count]);
    }
    serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap()
}

/// Validate strict modern Claude wire invariants:
/// 1. Final message must be role="user" (no assistant prefill).
/// 2. Roles must strictly alternate between "user" and "assistant".
/// 3. Inside every "user" message, all "tool_result" blocks must come before non-tool_result blocks.
/// 4. No cross-model signature may survive onto `claude-sonnet-5-5`.
fn check_strict_anthropic_wire(body: &Value) -> Result<(), (u16, String)> {
    let model = body["model"].as_str().unwrap_or_default();
    let Some(messages) = body["messages"].as_array() else {
        return Err((400, "messages must be an array".into()));
    };
    if messages.is_empty() {
        return Err((400, "messages must not be empty".into()));
    }
    if messages.last().and_then(|m| m["role"].as_str()) == Some("assistant") {
        return Err((
            400,
            "This model does not support assistant message prefill. The conversation must end with a user message.".into(),
        ));
    }
    let mut prev_role: Option<&str> = None;
    for (idx, msg) in messages.iter().enumerate() {
        let role = msg["role"].as_str().unwrap_or_default();
        if prev_role == Some(role) {
            return Err((
                400,
                format!("messages: roles must alternate between \"user\" and \"assistant\" at index {idx}"),
            ));
        }
        prev_role = Some(role);
        if let Some(blocks) = msg["content"].as_array() {
            if blocks.is_empty() {
                return Err((400, format!("messages.{idx}.content must not be empty")));
            }
            if role == "user" {
                let mut seen_non_tool_result = false;
                for block in blocks {
                    let is_tr = block["type"].as_str() == Some("tool_result");
                    if is_tr && seen_non_tool_result {
                        return Err((
                            400,
                            format!("messages.{idx}: tool_result blocks must precede other content blocks"),
                        ));
                    }
                    if !is_tr {
                        seen_non_tool_result = true;
                    }
                }
            }
            if role == "assistant" && model == "claude-sonnet-5-5" {
                for block in blocks {
                    if block["signature"].as_str() == Some("OPUS_ONLY_SIGNATURE") {
                        return Err((400, "Invalid 'signature' in 'thinking' block".into()));
                    }
                }
            }
        }
    }
    Ok(())
}

async fn write_sse_or_error(socket: &mut TcpStream, body: &Value, sse_body: &str) {
    match check_strict_anthropic_wire(body) {
        Ok(()) => {
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{sse_body}",
                sse_body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        }
        Err((status, detail)) => {
            let err_json = json!({
                "type": "error",
                "error": {
                    "type": "invalid_request_error",
                    "message": detail
                }
            })
            .to_string();
            let response = format!(
                "HTTP/1.1 {status} Bad Request\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{err_json}",
                err_json.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        }
    }
}

fn capabilities() -> ModelCapabilities {
    runtime_codec::decode_text(&RuntimeString::from(
        json!({
            "tools": true,
            "vision": true,
            "contextWindow": 200_000,
            "maxOutputTokens": 8_192,
            "effort": {"levels": ["low", "medium", "high"], "default": "high"},
            "thinking": {"adaptive": true, "interleaved": true},
            "structuredOutput": false
        })
        .to_string(),
    ))
    .unwrap()
}

fn anthropic_provider(base_url: &str, model: &str) -> Arc<AnthropicProvider> {
    Arc::new(AnthropicProvider::new(AnthropicProviderOptions {
        api_key: Some("fixture-key".into()),
        base_url: Some(base_url.into()),
        default_model: Some(model.into()),
        capabilities: Some(capabilities()),
        ..Default::default()
    }))
}

fn session_config(model: &str, selection: &str) -> Value {
    json!({
        "model": model,
        "contextWindow": 200_000,
        "maxOutputTokens": 4_096,
        "contextHints": true,
        "thinking": true,
        "effort": "high",
        "autoCompaction": true,
        "prewarm": false,
        "loopDetection": false,
        "autoMemory": false,
        "autoDream": false,
        "memoryRecall": false,
        "maxRetries": 0,
        "approval_policy": "never",
        "sandbox_mode": "danger-full-access",
        "modelSource": {
            "source": "subscription",
            "modelId": model,
            "selection": selection
        }
    })
}

#[tokio::test]
async fn opus_thinking_only_failure_then_switch_to_sonnet_and_continue_succeeds_without_prefill_or_fake_compaction(
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let captured_requests: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let captured_for_server = captured_requests.clone();

    let server = tokio::spawn(async move {
        // Round 0 (Opus): returns signed empty-text thinking -> triggers empty_closing_retry
        // Round 1 (Opus retry): returns empty -> fails turn with "the model returned no content for this turn"
        // Round 2 (Opus Continue): returns empty -> triggers empty_closing_retry
        // Round 3 (Opus Continue retry): returns signed empty-text thinking -> fails turn again
        // Round 4 (Switched to Sonnet + Continue): must end on user message, no prefill 400!
        let scripted = [
            reply_thinking_only_signed("claude-opus-5-5", "OPUS_ONLY_SIGNATURE"),
            reply_empty("claude-opus-5-5"),
            reply_thinking_only_signed("claude-opus-5-5", "OPUS_ONLY_SIGNATURE"),
            reply_thinking_only_signed("claude-opus-5-5", "OPUS_ONLY_SIGNATURE"),
            reply_text(
                "claude-sonnet-5-5",
                "已切换到 Sonnet 5.5 并继续完成微信群聊视频剪辑。",
            ),
        ];
        for reply in scripted {
            let (mut socket, _) = listener.accept().await.unwrap();
            let body = read_http_json(&mut socket).await;
            captured_for_server.lock().unwrap().push(body.clone());
            write_sse_or_error(&mut socket, &body, &reply).await;
        }
    });

    let dir = tempfile::tempdir().unwrap();
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.path().join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));

    let mut options = agent_options(process.clone());
    options.provider = anthropic_provider(&base_url, "claude-opus-5-5");
    options.config = Some(session_config("claude-opus-5-5", "subscription:opus"));
    options.cwd = Some(dir.path().to_string_lossy().into_owned());
    options.include_default_agents = Some(false);

    let session = Agent::new(options)
        .open_session(
            store.clone(),
            StartSessionOptions {
                user_config_dir: Some(dir.path().to_string_lossy().into_owned().into()),
                base_instructions: Some("Assist the user with video editing.".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // 1. First turn on Opus 5.5 fails with "the model returned no content for this turn".
    let events_1: Vec<_> = session
        .run_streamed(
            Input::Text("1 蹬 2 有一点 Opus5.5 3 fable5.2 也是一条 4 弹验证 5 交流群".into()),
            TurnOptions::default(),
        )
        .events
        .collect()
        .await;
    assert!(events_1.iter().any(|e| matches!(
        e,
        EngineEvent::TurnFailed { error, .. }
            if error.message.contains("the model returned no content for this turn")
    )));

    // 2. Clicking Continue on Opus 5.5 fails a second time leaving a signed empty-text
    //    thinking block at the very end of conversation history.
    let events_2: Vec<_> = session
        .run_streamed(Input::Parts(Vec::new()), TurnOptions::default())
        .events
        .collect()
        .await;
    assert!(events_2.iter().any(|e| matches!(
        e,
        EngineEvent::TurnFailed { error, .. }
            if error.message.contains("the model returned no content for this turn")
    )));

    // 3. Switch from Opus 5.5 to Sonnet 5.5 and click Continue (`Input::Parts(Vec::new())`).
    session
        .reconfigure_inference(
            anthropic_provider(&base_url, "claude-sonnet-5-5"),
            resolve_config(session_config("claude-sonnet-5-5", "subscription:sonnet")).unwrap(),
            None,
            None,
            session.model_revision(),
        )
        .unwrap();

    let events_3: Vec<_> = tokio::time::timeout(
        Duration::from_secs(10),
        session
            .run_streamed(Input::Parts(Vec::new()), TurnOptions::default())
            .events
            .collect(),
    )
    .await
    .unwrap();

    assert!(
        !events_3
            .iter()
            .any(|e| matches!(e, EngineEvent::TurnFailed { .. })),
        "Sonnet 5.5 continuation after Opus thinking-only failure must succeed: {events_3:?}"
    );
    assert!(
        !events_3
            .iter()
            .any(|e| matches!(e, EngineEvent::Compaction { .. })),
        "No compaction should run at ~100 tokens on a 200K window"
    );

    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();

    {
        let requests = captured_requests.lock().unwrap();
        assert_eq!(requests.len(), 5);
        let sonnet_req = &requests[4];
        assert_eq!(sonnet_req["model"], "claude-sonnet-5-5");
        let wire_str = sonnet_req["messages"].to_string();
        assert!(!wire_str.contains("Thinking removed for model switch"));
        assert!(!wire_str.contains("[no content]"));
        assert!(!wire_str.contains("OPUS_ONLY_SIGNATURE"));
        assert_eq!(
            sonnet_req["messages"].as_array().unwrap().last().unwrap()["role"],
            "user"
        );
    }

    // 4. Verify rollout contains the history-repair `Compacted` record from
    //    `context_hint_state.bind_model`, yet NEITHER `project_thread_history` NOR
    //    `paged_thread_history` projects a bogus `CompactBoundary` ("上下文已自动压缩").
    session.flush_rollout().await.unwrap();
    let thread_id = session.id.try_to_utf8().unwrap();
    let persisted = store.read_thread(&thread_id).await.unwrap();
    assert!(
        persisted
            .raw_items
            .iter()
            .any(|item| matches!(item, RolloutItem::Compacted(c) if c.is_history_repair())),
        "expected context_hint_state model switch to persist a history-repair record"
    );
    let projected = project_thread_history(persisted.raw_items);
    assert!(
        !projected
            .history
            .iter()
            .any(|m| matches!(m, Message::CompactBoundary(_))),
        "history-repair records must not project as UI CompactBoundary rows: {:?}",
        projected.history
    );
    if let Some(page) = store
        .paged_thread_history(&thread_id, None, Some(50), None, None)
        .await
        .unwrap()
    {
        assert!(
            !page
                .history
                .history
                .iter()
                .any(|m| matches!(m, Message::CompactBoundary(_))),
            "thread_index must not materialize UI CompactBoundary rows for history repairs: {:?}",
            page.history.history
        );
    }

    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn continuing_after_interrupted_partial_assistant_text_or_tool_result_avoids_prefill_and_role_collision(
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let captured: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let captured_server = captured.clone();

    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let body = read_http_json(&mut socket).await;
        captured_server.lock().unwrap().push(body.clone());
        write_sse_or_error(
            &mut socket,
            &body,
            &reply_text("claude-sonnet-5-5", "接着上半段继续完成渲染。"),
        )
        .await;
    });

    let dir = tempfile::tempdir().unwrap();
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let decode =
        |v: Value| -> Message { runtime_codec::decode_text(&v.to_string().into()).unwrap() };

    // History has:
    // 1. User -> Assistant(tool_use) -> ToolResult -> Assistant(Opus thinking-only) -> User
    //    (tests that dropping the Opus thinking-only assistant merges User cleanly after ToolResult
    //    keeping tool_result first and roles strictly alternating)
    // 2. Trailing partial Assistant text message ("已完成前两步...") followed by empty Continue
    //    (tests that `build_params` appends a user continuation prompt so Sonnet 5.5 never 400s on prefill).
    let initial_history = vec![
        decode(
            json!({"type":"user","uuid":"u1","content":[{"type":"text","text":"开始剪辑视频"}]}),
        ),
        decode(json!({
            "type":"assistant","uuid":"a1","model":"claude-opus-5-5",
            "content":[{"type":"tool_use","id":"call-1","name":"read","input":{"path":"PROGRESS.md"}}]
        })),
        decode(json!({
            "type":"tool_result","uuid":"tr1","toolUseId":"call-1",
            "content":[{"type":"text","text":"进度草稿内容"}],"isError":false
        })),
        decode(json!({
            "type":"assistant","uuid":"a-think-only","model":"claude-opus-5-5",
            "content":[{"type":"thinking","text":"internal opus thought","signature":"OPUS_ONLY_SIGNATURE"}]
        })),
        decode(
            json!({"type":"user","uuid":"u2","content":[{"type":"text","text":"继续生成字幕"}]}),
        ),
        decode(json!({
            "type":"assistant","uuid":"a-partial","model":"claude-opus-5-5",
            "content":[
                {"type":"thinking","text":"more opus thought","signature":"OPUS_ONLY_SIGNATURE"},
                {"type":"text","text":"已完成前两步字幕时间轴对齐，接下来"}
            ]
        })),
    ];

    let mut options = agent_options(process.clone());
    options.provider = anthropic_provider(&base_url, "claude-sonnet-5-5");
    options.config = Some(session_config("claude-sonnet-5-5", "subscription:sonnet"));
    options.cwd = Some(dir.path().to_string_lossy().into_owned());
    options.include_default_agents = Some(false);

    let session = Agent::new(options).start_session(StartSessionOptions {
        initial_history: Some(initial_history),
        user_config_dir: Some(dir.path().to_string_lossy().into_owned().into()),
        base_instructions: Some("Continue task.".into()),
        ..Default::default()
    });

    let events: Vec<_> = session
        .run_streamed(Input::Parts(Vec::new()), TurnOptions::default())
        .events
        .collect()
        .await;

    assert!(
        !events
            .iter()
            .any(|e| matches!(e, EngineEvent::TurnFailed { .. })),
        "Continuation after partial assistant message must succeed on Sonnet 5.5: {events:?}"
    );

    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();

    {
        let requests = captured.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let msgs = requests[0]["messages"].as_array().unwrap();
        assert_eq!(msgs.last().unwrap()["role"], "user");
        let penultimate = &msgs[msgs.len() - 2];
        assert_eq!(penultimate["role"], "assistant");
        assert!(penultimate["content"]
            .to_string()
            .contains("已完成前两步字幕时间轴对齐"));
    }

    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
}
