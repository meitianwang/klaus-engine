//! Public-facade contract for cross-thread inspection. Source-owned fixtures
//! assert what reading must preserve; no other client's implementation is a gate.
use super::*;
use crate::app_server::wire;
use futures::StreamExt;
use klaus_agent_core::schemas::rollout::SessionMeta;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::rollout::parse_rollout_line;
use klaus_agent_core::store::session_store::SessionStore;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Notify;

pub(super) fn fixture_header(id: &str) -> Value {
    json!({"kind":"session_meta","schemaVersion":2,"meta":{"id":id,"createdAt":1700000000000_i64,"cwd":"/tmp","model":"m","title":"Read history"}})
}
pub(super) fn fixture_turn(id: &str, text: &str) -> Vec<Value> {
    vec![
        json!({"kind":"turn_context","ctx":{"cwd":"/tmp","model":"m","permissionMode":"default","turnId":id,"startedAtMs":1700000000100_i64}}),
        json!({"kind":"message","message":{"type":"user","uuid":format!("{id}-user"),"content":[{"type":"text","text":text}]}}),
        json!({"kind":"message","message":{"type":"assistant","uuid":format!("{id}-answer"),"content":[{"type":"text","text":format!("answer {id}")}],"model":"m"}}),
        json!({"kind":"event","event":"turn_lifecycle","turnId":id,"payload":{"status":"completed","finalMessageId":format!("{id}-answer"),"startedAtMs":1700000000100_i64,"finishedAtMs":1700000000200_i64,"durationMs":100}}),
    ]
}
pub(super) fn write_fixture(dir: &std::path::Path, id: &str, rows: &[Value]) -> std::path::PathBuf {
    std::fs::create_dir_all(dir).unwrap();
    for row in rows {
        assert!(
            parse_rollout_line(&row.to_string()).is_some(),
            "invalid source fixture {row}"
        );
    }
    let path = dir.join(format!("1700000000000-{id}.jsonl"));
    std::fs::write(
        &path,
        rows.iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n")
            + "\n",
    )
    .unwrap();
    path
}
pub(super) fn store(dir: &std::path::Path) -> Arc<dyn SessionStore> {
    Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.to_string_lossy().into_owned()),
        archive_dir: None,
    }))
}
pub(super) fn read_only_client(
    store: Arc<dyn SessionStore>,
    activations: Arc<AtomicUsize>,
) -> TestClient {
    let agent = make_agent_for_tests(vec![]);
    let lifecycle = crate::lifecycle::protocol_lifecycle(agent, Some(store));
    let create: CreateSession = Arc::new(move |_| {
        activations.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err("read must not create a session".into()) })
    });
    TestClient::start_with(create, Some(lifecycle))
}
pub(super) async fn read(client: &mut TestClient, id: u64, params: Value) -> Value {
    client
        .send(json!({"id":id,"method":"engine/readThread","params":params}))
        .await;
    client.response_for(id).await.0
}

#[tokio::test]
async fn read_thread_pages_complete_content_across_compaction_and_rollback() {
    let dir = std::env::temp_dir().join(format!("klaus-thread-read-{}", uuid::Uuid::new_v4()));
    let long = "完整文字🦀".repeat(8000);
    let mut rows = vec![fixture_header("history")];
    rows.extend(fixture_turn("one", &long));
    rows.push(json!({"kind":"compacted","summary":"compressed","replacedRange":[0,2],"replacementHistory":[]}));
    rows.extend(fixture_turn("two", "second"));
    rows.extend(fixture_turn("discarded", "must disappear"));
    rows.push(json!({"kind":"rollback","numTurns":1}));
    rows.extend(fixture_turn("three", "new branch"));
    let path = write_fixture(&dir, "history", &rows);
    let before = std::fs::read(&path).unwrap();
    let activations = Arc::new(AtomicUsize::new(0));
    let mut client = read_only_client(store(&dir), activations.clone());
    client.initialize().await;
    let first = read(&mut client, 10, json!({"threadId":"history"})).await;
    assert_eq!(first["result"]["turns"][0]["id"], "three", "{first}");
    assert_eq!(
        first["result"]["page"],
        json!({"order":"newest_first","limit":1,"nextCursor":"three","hasMore":true})
    );
    let older = read(
        &mut client,
        11,
        json!({"threadId":"history","cursor":"three","limit":2}),
    )
    .await;
    assert_eq!(older["result"]["turns"][0]["id"], "two");
    let one = &older["result"]["turns"][1];
    assert_eq!(one["id"], "one");
    assert_eq!(one["items"][0]["content"][0]["text"], long);
    assert_eq!(one["items"][1]["phase"], "final_answer");
    assert_eq!(one["items"][2]["type"], "contextCompaction");
    assert_eq!(older["result"]["page"]["hasMore"], false);
    assert!(older["result"]["page"]["nextCursor"].is_null());
    for (id, params) in [
        (12, json!({"threadId":"history","cursor":"discarded"})),
        (13, json!({"threadId":"history","cursor":"unknown"})),
        (14, json!({"threadId":"history","limit":11})),
        (15, json!({"threadId":"history","limit":0})),
    ] {
        assert_eq!(
            read(&mut client, id, params).await["error"]["code"],
            wire::INVALID_PARAMS
        );
    }
    assert_eq!(activations.load(Ordering::SeqCst), 0);
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert!(!path.with_extension("jsonl.lock").exists());
    client.close().await;
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn read_thread_preserves_native_message_identity_and_phase() {
    let dir =
        std::env::temp_dir().join(format!("klaus-thread-read-phase-{}", uuid::Uuid::new_v4()));
    let mut rows = vec![fixture_header("phases")];
    let mut turn = fixture_turn("native", "inspect message phases");
    turn[2]["message"]["content"] = json!([
        {"type":"text","text":"Checking","messageId":"native-commentary","phase":"commentary"},
        {"type":"text","text":"Answer","messageId":"native-final","phase":"final_answer"}
    ]);
    rows.extend(turn);
    let path = write_fixture(&dir, "phases", &rows);
    let before = std::fs::read(&path).unwrap();
    let activations = Arc::new(AtomicUsize::new(0));
    let mut client = read_only_client(store(&dir), activations.clone());
    client.initialize().await;
    let reply = read(&mut client, 10, json!({"threadId":"phases"})).await;
    let messages: Vec<_> = reply["result"]["turns"][0]["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|item| item["type"] == "agentMessage")
        .collect();
    assert_eq!(messages.len(), 2, "{reply}");
    assert_eq!(messages[0]["id"], "native-commentary");
    assert_eq!(messages[0]["phase"], "commentary");
    assert_eq!(messages[1]["id"], "native-final");
    assert_eq!(messages[1]["phase"], "final_answer");
    assert_eq!(activations.load(Ordering::SeqCst), 0);
    assert_eq!(std::fs::read(&path).unwrap(), before);
    client.close().await;
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn read_thread_preserves_tools_outputs_and_does_not_restore_archived_or_deleted_threads() {
    let dir = std::env::temp_dir().join(format!("klaus-thread-read-{}", uuid::Uuid::new_v4()));
    let mut rows = vec![fixture_header("archived")];
    let mut turn = fixture_turn("tool-turn", "inspect results");
    turn[1]["message"]["content"]
        .as_array_mut()
        .unwrap()
        .push(json!({"type":"image","mimeType":"image/png","base64":"aGVsbG8="}));
    turn[2] = json!({"kind":"message","message":{"type":"assistant","uuid":"answer","model":"m","content":[
        {"type":"thinking","text":"reasoning details"},
        {"type":"tool_use","id":"shell","name":"bash","input":{"command":"printf hello"}},
        {"type":"tool_use","id":"mcp","name":"mcp__docs__fetch","input":{"url":"https://example.test"}},
        {"type":"tool_use","id":"dynamic","name":"klaus_query","input":{"key":"session"}}
    ]}});
    let output = "complete output".repeat(3000);
    for (id, error) in [("shell", false), ("mcp", true), ("dynamic", false)] {
        turn.insert(turn.len()-1,json!({"kind":"message","message":{"type":"tool_result","uuid":format!("result-{id}"),"toolUseId":id,"content":[{"type":"text","text":output}],"isError":error}}));
    }
    rows.extend(turn);
    let path = write_fixture(&dir.join("archived"), "archived", &rows);
    let before = std::fs::read(&path).unwrap();
    let store = store(&dir);
    let activations = Arc::new(AtomicUsize::new(0));
    let mut client = read_only_client(store.clone(), activations.clone());
    client.initialize().await;
    let reply = read(&mut client, 20, json!({"threadId":"archived"})).await;
    let result = &reply["result"];
    assert_eq!(result["thread"]["archived"], true, "{reply}");
    assert_eq!(result["thread"]["status"]["type"], "notLoaded");
    let items = result["turns"][0]["items"].as_array().unwrap();
    assert_eq!(
        items[0]["content"][1],
        json!({"type":"image","url":"data:image/png;base64,aGVsbG8="})
    );
    assert_eq!(
        items
            .iter()
            .find(|item| item["type"] == "commandExecution")
            .unwrap()["aggregatedOutput"],
        output
    );
    assert_eq!(
        items
            .iter()
            .find(|item| item["type"] == "mcpToolCall")
            .unwrap()["status"],
        "failed"
    );
    assert_eq!(
        items
            .iter()
            .filter(|item| item["type"] == "functionCallOutput")
            .count(),
        3
    );
    assert_eq!(
        items
            .iter()
            .find(|item| item["type"] == "reasoning")
            .unwrap()["content"][0],
        "reasoning details"
    );
    assert_eq!(activations.load(Ordering::SeqCst), 0);
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1);
    std::fs::remove_file(&path).unwrap();
    assert_eq!(
        read(&mut client, 21, json!({"threadId":"archived"})).await["error"]["code"],
        wire::UNKNOWN_SESSION
    );
    client.close().await;
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_thread_observes_a_streaming_turn_without_waiting_or_reactivating() {
    assert_streaming_thread_read(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_thread_observes_native_final_phase_before_turn_completion() {
    assert_streaming_thread_read(true).await;
}

async fn assert_streaming_thread_read(native_phase: bool) {
    let dir = std::env::temp_dir().join(format!("klaus-thread-read-live-{}", uuid::Uuid::new_v4()));
    let store = store(&dir);
    let meta: SessionMeta = match parse_rollout_line(&fixture_header("live").to_string()).unwrap() {
        klaus_agent_core::schemas::rollout::RolloutItem::SessionMeta { meta, .. } => meta,
        _ => unreachable!(),
    };
    store.create(meta).await.unwrap().shutdown().await.unwrap();
    let started = Arc::new(Notify::new());
    let entered = started.clone();
    let cancelled = Arc::new(AtomicBool::new(false));
    let cancelled_model = cancelled.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_model = calls.clone();
    let edited_path = dir.join("edited.txt");
    let write_path = edited_path.to_string_lossy().into_owned();
    let written = "written by this turn\n".repeat(2048);
    let write_content = written.clone();
    let agent = make_agent_with_call_model(Box::new(move |_, signal| {
        if calls_model.fetch_add(1, Ordering::SeqCst) == 0 {
            let mut first = vec![
                ev::message_start(None),
                ev::thinking("first reasoning"),
                ev::thinking_block("first reasoning", None),
            ];
            let input = klaus_agent_core::runtime_values::runtime_json::parse(
                &json!({"path":write_path,"content":write_content})
                    .to_string()
                    .into(),
            )
            .unwrap();
            first.extend(ev::tool_use("live-write", "write", &input));
            let shell_input = klaus_agent_core::runtime_values::runtime_json::parse(
                &json!({"command":"printf 'original stdout'; exit 7"})
                    .to_string()
                    .into(),
            )
            .unwrap();
            first.extend(ev::tool_use("live-shell", "bash", &shell_input));
            first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
            return Box::pin(futures::stream::iter(first));
        }
        let started = entered.clone();
        let cancelled = cancelled_model.clone();
        let mut events = vec![ev::message_start(None), ev::thinking("second reasoning")];
        if native_phase {
            events.extend([
                ModelStreamEvent::TextMessage {
                    id: "native-commentary".into(),
                    phase: Some("commentary".into()),
                },
                ev::text("Checking the results"),
                ModelStreamEvent::TextMessage {
                    id: "native-final".into(),
                    phase: Some("final_answer".into()),
                },
            ]);
        }
        events.push(ev::text("partial reply"));
        Box::pin(
            futures::stream::iter(events).chain(futures::stream::once(async move {
                started.notify_one();
                signal.cancelled().await;
                cancelled.store(true, Ordering::SeqCst);
                ev::stop(Some(ModelStopReason::EndTurn), None)
            })),
        )
    }));
    let mut lifecycle = crate::lifecycle::protocol_lifecycle(agent.clone(), Some(store.clone()));
    // Force the disk side of the read to remain at the initial header: all
    // accepted turn content must come from the live snapshot, without a flush.
    let header = store.read_thread("live").await.unwrap();
    lifecycle.read_thread = Some(Arc::new(move |_| {
        let header = header.clone();
        Box::pin(async move { Ok(header) })
    }));
    let create_store = store.clone();
    let activations = Arc::new(AtomicUsize::new(0));
    let count = activations.clone();
    let create: CreateSession = Arc::new(move |ctx| {
        count.fetch_add(1, Ordering::SeqCst);
        let agent = agent.clone();
        let store = create_store.clone();
        Box::pin(async move {
            agent
                .resume_session(
                    store,
                    &ctx.resume.unwrap(),
                    StartSessionOptions {
                        approval_handler: Some(ctx.approval_handler),
                        elicit_handler: Some(ctx.elicit_handler),
                        mcp_elicitation_presenter: Some(ctx.mcp_elicitation_presenter),
                        permission_mode: Some(ExternalPermissionMode::BypassPermissions),
                        ..Default::default()
                    },
                )
                .await
                .map_err(|error| Box::new(error) as BoxError)
        })
    });
    let mut client = TestClient::start_full(
        create,
        Some(lifecycle),
        Some(dir.join("rules.json")),
        Some(dir.join("config.json")),
    );
    client.initialize().await;
    client
        .send(json!({"id":2,"method":"session/start","params":{"init":{},"resume":"live"}}))
        .await;
    let session = client.response_for(2).await.0["result"]["sessionId"].clone();
    let empty = read(&mut client, 3, json!({"threadId":"live"})).await;
    assert_eq!(empty["result"]["turns"], json!([]), "{empty}");
    client.send(json!({"id":4,"method":"turn/start","params":{"sessionId":session,"submissionId":"active-turn","input":[{"type":"text","text":"keep running"}]}})).await;
    client.response_for(4).await;
    tokio::time::timeout(REPLY_TIMEOUT, started.notified())
        .await
        .unwrap();
    let final_message_id = loop {
        let event = client.next_message().await;
        if event["params"]["event"]["event"]["type"] == "text.delta"
            && event["params"]["event"]["event"]["text"] == "partial reply"
        {
            break event["params"]["event"]["event"]["messageId"].clone();
        }
    };
    let reply = tokio::time::timeout(
        Duration::from_secs(2),
        read(&mut client, 5, json!({"threadId":"live"})),
    )
    .await
    .expect("read settles before model end");
    assert_eq!(
        reply["result"]["thread"]["status"]["type"], "active",
        "{reply}"
    );
    assert_eq!(
        reply["result"]["turns"][0]["status"], "inProgress",
        "{reply}"
    );
    assert!(
        reply["result"]["turns"][0]["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["text"] == "partial reply"),
        "{reply}"
    );
    assert!(!cancelled.load(Ordering::SeqCst));
    assert_eq!(activations.load(Ordering::SeqCst), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let items = reply["result"]["turns"][0]["items"].as_array().unwrap();
    let messages: Vec<_> = items
        .iter()
        .filter(|item| item["type"] == "agentMessage")
        .collect();
    assert_eq!(
        messages.len(),
        if native_phase { 2 } else { 1 },
        "live reads must not duplicate delta buffers"
    );
    let last = messages.last().unwrap();
    assert_eq!(last["id"], final_message_id);
    assert_eq!(last["text"], "partial reply");
    if native_phase {
        assert_eq!(messages[0]["text"], "Checking the results");
        assert_eq!(messages[0]["phase"], "commentary");
        assert_eq!(last["phase"], "final_answer");
    } else {
        assert!(last.get("phase").is_none());
    }
    assert!(items
        .iter()
        .any(|item| item["type"] == "userMessage" && item["content"][0]["text"] == "keep running"));
    let reasoning: Vec<&str> = items
        .iter()
        .filter(|item| item["type"] == "reasoning")
        .filter_map(|item| item["content"][0].as_str())
        .collect();
    assert_eq!(reasoning, vec!["first reasoning", "second reasoning"]);
    assert!(items
        .iter()
        .any(|item| item["type"] == "functionCallOutput" && item["name"] == "write"));
    assert!(items.iter().any(|item| item["type"] == "dynamicToolCall"
        && item["tool"] == "write"
        && item["status"] == "completed"));
    assert_eq!(std::fs::read_to_string(&edited_path).unwrap(), written);
    let command = items
        .iter()
        .find(|item| item["type"] == "commandExecution")
        .unwrap();
    assert_eq!(command["exitCode"], 7, "{command}");
    assert_eq!(command["status"], "failed");
    assert!(command["aggregatedOutput"]
        .as_str()
        .unwrap()
        .contains("original stdout"));
    client.close().await;
    let mut cold = read_only_client(store, activations.clone());
    cold.initialize().await;
    let persisted = read(&mut cold, 6, json!({"threadId":"live"})).await;
    assert!(
        persisted["result"]["turns"][0]["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["type"] == "fileChange"
                && item["changes"][0]["kind"] == json!({"type":"add"})
                && item["changes"][0]["diff"] == written),
        "{persisted}"
    );
    let cold_command = persisted["result"]["turns"][0]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["type"] == "commandExecution")
        .unwrap();
    assert_eq!(cold_command["exitCode"], 7, "{cold_command}");
    assert_eq!(cold_command["status"], "failed");
    assert!(cold_command["aggregatedOutput"]
        .as_str()
        .unwrap()
        .contains("original stdout"));
    cold.close().await;
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn read_thread_reads_a_new_unpersisted_session_without_creating_a_turn() {
    let mut client = TestClient::start(make_create_session(&[]));
    client.initialize().await;
    client
        .send(json!({"id":2,"method":"session/start","params":{"init":{}}}))
        .await;
    let session = client.response_for(2).await.0["result"]["sessionId"].clone();
    let reply = read(&mut client, 3, json!({"threadId":session})).await;
    assert_eq!(reply["result"]["turns"], json!([]), "{reply}");
    assert_eq!(reply["result"]["thread"]["status"]["type"], "notLoaded");
    client.close().await;
}

#[tokio::test]
async fn read_thread_store_reads_alongside_the_existing_writer_without_changing_bytes() {
    use klaus_agent_core::schemas::rollout::RolloutItem;
    let dir =
        std::env::temp_dir().join(format!("klaus-thread-store-read-{}", uuid::Uuid::new_v4()));
    let store = store(&dir);
    let RolloutItem::SessionMeta { meta, .. } =
        parse_rollout_line(&fixture_header("writer").to_string()).unwrap()
    else {
        unreachable!()
    };
    let writer = store.create(meta).await.unwrap();
    let path = dir.join("1700000000000-writer.jsonl");
    for (id, expected) in [("one", 1), ("two", 2)] {
        let RolloutItem::Message { message } =
            parse_rollout_line(&fixture_turn(id, "kept in full")[1].to_string()).unwrap()
        else {
            unreachable!()
        };
        writer.append_message(message).await;
        writer.flush().await.unwrap();
        let before = std::fs::read(&path).unwrap();
        let modified = std::fs::metadata(&path).unwrap().modified().unwrap();
        let snapshot = tokio::time::timeout(Duration::from_secs(2), store.read_thread("writer"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            snapshot
                .raw_items
                .iter()
                .filter(|item| matches!(item, RolloutItem::Message { .. }))
                .count(),
            expected
        );
        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert_eq!(
            std::fs::metadata(&path).unwrap().modified().unwrap(),
            modified
        );
        assert!(!snapshot.archived);
    }
    writer.shutdown().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}
