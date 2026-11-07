//! Mobile reliability regression, 2026-10-03.
//! Root cause: follow used model context after compaction, and discarded the
//! partial item state at its stream cut. The public named facade must provide
//! complete original history plus explicit coverage and atomic active items.
use super::*;
use crate::app_server::wire;
use futures::StreamExt;
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::Message;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::rollout::parse_rollout_line;
use klaus_agent_core::store::session_store::SessionStore;
use std::sync::Mutex;
use tokio::sync::Notify;

fn history_fixture(dir: &std::path::Path) -> Arc<dyn SessionStore> {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../core/tests/fixtures/complete_thread_history.json"
    ))
    .unwrap();
    let rows = fixture["items"].as_array().unwrap();
    for row in rows {
        assert!(parse_rollout_line(&row.to_string()).is_some());
    }
    std::fs::write(
        dir.join("1700000000000-history.jsonl"),
        rows.iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n")
            + "\n",
    )
    .unwrap();
    Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.to_string_lossy().into_owned()),
        archive_dir: None,
    }))
}

fn persistent_create(
    agent: Arc<Agent>,
    store: Arc<dyn SessionStore>,
    captured: Arc<Mutex<Option<Arc<Session>>>>,
) -> CreateSession {
    Arc::new(move |ctx| {
        let agent = agent.clone();
        let store = store.clone();
        let captured = captured.clone();
        Box::pin(async move {
            let session = agent
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
                .map_err(|error| Box::new(error) as BoxError)?;
            *captured.lock().unwrap() = Some(session.clone());
            Ok(session)
        })
    })
}

async fn start(client: &mut TestClient) -> Value {
    client
        .send(json!({"id":1,"method":"initialize","params":{
            "clientInfo":{"name":"complete-follow-test"},
            "capabilities":{"revisionedSessionEvents":true,"productEvents":true}
        }}))
        .await;
    client.response_for(1).await;
    client.send(json!({"method":"initialized"})).await;
    client
        .send(json!({"id":2,"method":"session/start","params":{
            "init":{},"resume":"history"
        }}))
        .await;
    client.response_for(2).await.0["result"]["sessionId"].clone()
}

async fn follow(client: &mut TestClient, id: u64, session: &Value, original: bool) -> Value {
    client
        .send(json!({"id":id,"method":"session/follow","params":{
            "sessionId":session,"includeOriginalHistory":original
        }}))
        .await;
    client.response_for(id).await.0
}

#[tokio::test]
async fn original_follow_preserves_compacted_messages_media_tools_and_turn_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let store = history_fixture(dir.path());
    let agent = make_agent_for_tests(vec![]);
    let captured = Arc::new(Mutex::new(None));
    let lifecycle = crate::lifecycle::protocol_lifecycle(agent.clone(), Some(store.clone()));
    let mut client = TestClient::start_full(
        persistent_create(agent, store, captured),
        Some(lifecycle),
        Some(dir.path().join("rules.json")),
        Some(dir.path().join("config.json")),
    );
    let session = start(&mut client).await;
    let context = follow(&mut client, 3, &session, false).await;
    let context_history = context["result"]["snapshot"]["history"].as_array().unwrap();
    assert!(!context_history
        .iter()
        .any(|row| row["uuid"] == "assistant-one"));
    assert!(context["result"]["snapshot"].get("activeItems").is_none());
    let reply = follow(&mut client, 4, &session, true).await;
    let snapshot = &reply["result"]["snapshot"];
    let history = snapshot["history"].as_array().expect("complete history");
    assert!(history.iter().any(|row| row["uuid"] == "assistant-one"));
    assert!(history.iter().any(|row| row["uuid"] == "user-one"));
    assert!(history.iter().any(|row| row["type"] == "compact_boundary"));
    assert!(!history.iter().any(|row| row["uuid"] == "discarded-user"));
    let tool = history
        .iter()
        .find(|row| row["uuid"] == "result-one")
        .unwrap();
    assert_eq!(
        tool["output"],
        json!({"exit_code":130,"stdout":"Stopped","fixture":"structured"})
    );
    assert_eq!(tool["extraUserBlocks"][0]["filename"], "tool.pdf");
    assert_eq!(snapshot["historyTurnIds"]["assistant-one"], "one");
    assert_eq!(snapshot["turnStatuses"]["one"], "interrupted");
    assert_eq!(snapshot["activeItems"], json!([]));
    assert!(snapshot["coveredToolResults"]
        .as_array()
        .unwrap()
        .contains(&json!({"turnId":"one","toolUseId":"shell-one"})));
    assert!(!snapshot["coveredToolResults"]
        .as_array()
        .unwrap()
        .contains(&json!({"turnId":"one","toolUseId":"unfinished-one"})));
    let covered = snapshot["coveredMessageIds"].as_array().unwrap();
    for id in ["assistant-one", "part-one", "assistant-one-0", "shell-one"] {
        assert!(
            covered.contains(&json!(id)),
            "missing authoritative item identity {id}"
        );
    }
    assert_eq!(reply["result"]["mode"], "snapshot");
    assert_eq!(reply["result"]["revision"], context["result"]["revision"]);
    client.close().await;
}

#[tokio::test]
async fn original_follow_cuts_partial_items_and_covers_ahead_completion_during_store_read() {
    let dir = tempfile::tempdir().unwrap();
    let store = history_fixture(dir.path());
    let release_model = Arc::new(Notify::new());
    let release = release_model.clone();
    let agent = make_agent_with_call_model_and_config(
        Box::new(move |_, _| {
            let release = release.clone();
            Box::pin(
                futures::stream::iter(vec![
                    ev::message_start(None),
                    // This assembly row is filtered from persisted content. The
                    // reasoning ID must still join its delta and completion.
                    ev::text(""),
                    ev::thinking("reasoning prefix"),
                    ev::thinking_block("reasoning prefix", Some("signed-reasoning")),
                    ModelStreamEvent::TextMessage {
                        id: "native-follow-item".into(),
                        phase: Some("final_answer".into()),
                    },
                    ev::text("prefix"),
                ])
                .chain(futures::stream::once(async move {
                    release.notified().await;
                    ev::stop(Some(ModelStopReason::EndTurn), None)
                })),
            )
        }),
        Some(
            json!({"agentMemory":false,"autoMemory":false,"autoDream":false,"memoryRecall":false,"memory":{"enabled":false}}),
        ),
    );
    let read_entered = Arc::new(Notify::new());
    let release_read = Arc::new(Notify::new());
    let read_signal = read_entered.clone();
    let release = release_read.clone();
    let mut lifecycle = crate::lifecycle::protocol_lifecycle(agent.clone(), Some(store.clone()));
    let actual_read = lifecycle.read_thread.take().unwrap();
    let first_read = Arc::new(std::sync::atomic::AtomicBool::new(true));
    lifecycle.read_thread = Some(Arc::new(move |id| {
        let actual_read = actual_read.clone();
        let read_signal = read_signal.clone();
        let release = release.clone();
        let first_read = first_read.clone();
        Box::pin(async move {
            if first_read.swap(false, std::sync::atomic::Ordering::SeqCst) {
                read_signal.notify_one();
                release.notified().await;
            }
            actual_read(id).await
        })
    }));
    let captured = Arc::new(Mutex::new(None));
    let mut client = TestClient::start_full(
        persistent_create(agent, store.clone(), captured.clone()),
        Some(lifecycle),
        Some(dir.path().join("rules.json")),
        Some(dir.path().join("config.json")),
    );
    let session = start(&mut client).await;
    client
        .send(json!({"id":3,"method":"turn/start","params":{
            "sessionId":session,"submissionId":"live-follow-turn","input":"new live input"
        }}))
        .await;
    client.response_for(3).await;
    let mut reasoning_id = Value::Null;
    let mut product_turn_id = Value::Null;
    let (prefix_revision, message_id) = loop {
        let event = client.next_message().await;
        if event["method"] == "item/reasoning/delta" {
            product_turn_id = event["params"]["turnId"].clone();
            assert!(
                product_turn_id.is_string(),
                "product items need canonical turn IDs"
            );
        }
        if event["params"]["event"]["event"]["type"] == "thinking.delta" {
            reasoning_id = event["params"]["event"]["event"]["messageId"].clone();
            assert!(
                reasoning_id.is_string(),
                "reasoning identity must be authoritative"
            );
        }
        if event["params"]["event"]["event"]["type"] == "text.delta"
            && event["params"]["event"]["event"]["text"] != ""
        {
            assert_eq!(event["params"]["event"]["event"]["text"], "prefix");
            break (
                event["params"]["streamRevision"].as_u64().unwrap(),
                event["params"]["event"]["event"]["messageId"].clone(),
            );
        }
    };
    client
        .send(json!({"id":4,"method":"session/follow","params":{
            "sessionId":session,"includeOriginalHistory":true
        }}))
        .await;
    tokio::time::timeout(REPLY_TIMEOUT, read_entered.notified())
        .await
        .unwrap();
    // The provider completes while the read is paused and protocol output is
    // held. The complete assistant row exists before its item.completed frame.
    release_model.notify_one();
    let live = captured.lock().unwrap().as_ref().unwrap().clone();
    tokio::time::timeout(REPLY_TIMEOUT, async {
        loop {
            if live.visible_history().iter().any(|row| matches!(row,
                Message::Assistant(message) if message.content.iter().any(|block|
                    matches!(block, ContentBlock::Text(text) if text.message_id.as_ref().is_some_and(|id| id.try_to_utf8().ok().as_deref() == message_id.as_str()))))) {
                break;
            }
            tokio::task::yield_now().await;
        }
    }).await.unwrap();
    release_read.notify_one();
    let (reply, before_reply) = client.response_for(4).await;
    assert!(!before_reply
        .iter()
        .any(|event| event["params"]["event"]["event"]["type"] == "item.completed"));
    let result = &reply["result"];
    let snapshot = &result["snapshot"];
    let snapshot_revision = result["revision"].as_u64().unwrap();
    let active = snapshot["activeItems"].as_array().unwrap();
    assert!(active
        .iter()
        .any(|row| row["turnId"] == product_turn_id && row["item"]["id"] == reasoning_id));
    assert!(
        active
            .iter()
            .any(|row| row["item"]["id"] == message_id && row["item"]["text"] == "prefix"),
        "{reply}"
    );
    assert!(snapshot["coveredMessageIds"]
        .as_array()
        .unwrap()
        .contains(&message_id));
    assert!(snapshot["coveredMessageIds"]
        .as_array()
        .unwrap()
        .contains(&reasoning_id));
    assert!(active.iter().any(|row| row["item"]["id"] == reasoning_id
        && row["item"]["type"] == "reasoning"
        && row["item"]["text"] == "reasoning prefix"));
    assert!(snapshot["history"]
        .as_array()
        .unwrap()
        .iter()
        .any(|row| row["type"] == "user" && row["content"][0]["text"] == "new live input"));
    let completed_revision = loop {
        let event = client.next_message().await;
        if event["params"]["event"]["event"]["type"] == "item.completed"
            && event["params"]["event"]["event"]["item"]["id"] == message_id
        {
            break event["params"]["streamRevision"].as_u64().unwrap();
        }
    };
    assert!(completed_revision > prefix_revision);
    assert!(
        completed_revision > snapshot_revision,
        "completion revision {completed_revision} must follow snapshot cut {snapshot_revision}"
    );
    client
        .send(json!({"id":5,"method":"session/follow","params":{
            "sessionId":session,"includeOriginalHistory":true,
            "streamId":result["streamId"],"afterRevision":prefix_revision
        }}))
        .await;
    let replay = client.response_for(5).await.0;
    assert_eq!(replay["result"]["mode"], "replay");
    assert!(replay["result"]["events"]
        .as_array()
        .unwrap()
        .iter()
        .any(|row| row["revision"] == completed_revision
            && row["event"]["event"]["item"]["id"] == message_id));
    let replay_events = replay["result"]["events"].as_array().unwrap();
    let through_cut = replay_events
        .iter()
        .map(|row| row["revision"].as_u64().unwrap())
        .filter(|revision| *revision <= snapshot_revision)
        .collect::<Vec<_>>();
    let through_cut_count = snapshot_revision
        .checked_sub(prefix_revision)
        .expect("the snapshot cut must not precede its observed prefix");
    assert_eq!(
        u64::try_from(through_cut.len()).unwrap(),
        through_cut_count,
        "retained replay must witness every revision through the exact snapshot cut"
    );
    for (index, revision) in through_cut.iter().enumerate() {
        let expected = prefix_revision
            .checked_add(u64::try_from(index).unwrap())
            .unwrap()
            .checked_add(1)
            .unwrap();
        assert_eq!(*revision, expected);
    }
    let cut_frames = replay_events
        .iter()
        .filter(|row| row["revision"].as_u64().unwrap() <= snapshot_revision)
        .take(16)
        .map(|row| {
            (
                row["revision"].as_u64().unwrap(),
                row["event"]["event"]["type"].as_str(),
                row["event"]
                    .as_object()
                    .unwrap()
                    .keys()
                    .take(12)
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();
    eprintln!(
        "[original-follow-cut] prefix={prefix_revision} cut={snapshot_revision} completed={completed_revision} frames={cut_frames:?}"
    );
    let replay_head = replay["result"]["revision"].as_u64().unwrap();
    let retained_tail = replay_events
        .iter()
        .filter(|row| row["revision"].as_u64().unwrap() > snapshot_revision)
        .cloned()
        .collect::<Vec<_>>();
    client
        .send(json!({"id":6,"method":"session/follow","params":{
            "sessionId":session,"includeOriginalHistory":true,
            "streamId":result["streamId"],"afterRevision":snapshot_revision
        }}))
        .await;
    let replay_from_cut = client.response_for(6).await.0;
    assert_eq!(replay_from_cut["result"]["mode"], "replay");
    assert_eq!(replay_from_cut["result"]["streamId"], result["streamId"]);
    let cut_tail = replay_from_cut["result"]["events"].as_array().unwrap();
    assert!(cut_tail
        .iter()
        .all(|row| row["revision"].as_u64().unwrap() > snapshot_revision));
    let replayed_retained_tail = cut_tail
        .iter()
        .filter(|row| row["revision"].as_u64().unwrap() <= replay_head)
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(replayed_retained_tail, retained_tail);
    client.close().await;
}

#[tokio::test]
async fn original_follow_never_silently_uses_compacted_context_without_store_reader() {
    let mut client = TestClient::start(make_create_session(&[]));
    client.initialize().await;
    client
        .send(json!({"id":2,"method":"session/start","params":{"init":{}}}))
        .await;
    let session = client.response_for(2).await.0["result"]["sessionId"].clone();
    let reply = follow(&mut client, 3, &session, true).await;
    assert_eq!(reply["result"]["mode"], "snapshot");
    assert!(reply["result"]["snapshot"]["history"].is_null());
    assert!(reply["result"]["snapshot"]["error"]
        .as_str()
        .unwrap()
        .contains("read-only session store"));
    client.close().await;
}

#[test]
fn original_follow_optional_field_is_published_and_default_stays_compatible() {
    let old: wire::SessionFollowParams = serde_json::from_value(json!({"sessionId":"s"})).unwrap();
    assert_eq!(old.include_original_history, None);
    let field = schema_bundle()["requests"]["session/follow"]["params"]["properties"]
        ["includeOriginalHistory"]
        .clone();
    assert!(
        field.is_object(),
        "optional original-history contract must be published"
    );
}

#[tokio::test]
async fn original_follow_preserves_pre_cut_live_tool_output_without_marking_results_covered() {
    let dir = tempfile::tempdir().unwrap();
    let store = history_fixture(dir.path());
    let release = dir.path().join("release-tool");
    struct ReleaseTool(std::path::PathBuf);
    impl Drop for ReleaseTool {
        fn drop(&mut self) {
            let _ = std::fs::write(&self.0, "release");
        }
    }
    let release_guard = ReleaseTool(release.clone());
    let command = format!("printf 'pre-cut stdout'; while [ ! -f '{}' ]; do sleep 0.01; done; printf ' post-cut stdout'", release.display());
    let input = klaus_agent_core::runtime_values::runtime_json::parse(
        &json!({"command":command}).to_string().into(),
    )
    .unwrap();
    let mut first = vec![ev::message_start(None)];
    first.extend(ev::tool_use("shell-one", "bash", &input));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let agent = make_agent_for_tests(vec![first, text_turn("tool done")]);
    let lifecycle = crate::lifecycle::protocol_lifecycle(agent.clone(), Some(store.clone()));
    let mut client = TestClient::start_full(
        persistent_create(agent, store, Arc::new(Mutex::new(None))),
        Some(lifecycle),
        Some(dir.path().join("rules.json")),
        Some(dir.path().join("config.json")),
    );
    let session = start(&mut client).await;
    client
        .send(json!({"id":3,"method":"turn/start","params":{
            "sessionId":session,"input":"run test command"
        }}))
        .await;
    client.response_for(3).await;
    loop {
        let event = client.next_message().await;
        let inner = &event["params"]["event"]["event"];
        if inner["type"] == "tool.event"
            && inner["toolUseId"] == "shell-one"
            && inner["event"]["type"] == "stdout"
        {
            assert_eq!(inner["event"]["chunk"], "pre-cut stdout");
            break;
        }
    }
    // A cold mobile reader first consumes this provider-free history. It
    // must not install a fabricated terminal status before the full follow.
    client
        .send(
            json!({"id":30,"method":"engine/readThreadHistory","params":{
                "threadId":session
            }}),
        )
        .await;
    let preview = client.response_for(30).await.0;
    let reply = follow(&mut client, 4, &session, true).await;
    let snapshot = &reply["result"]["snapshot"];
    assert!(snapshot["activeItems"]
        .as_array()
        .unwrap()
        .iter()
        .any(|row| row["item"]["type"] == "tool_progress"
            && row["item"]["toolUseId"] == "shell-one"
            && row["item"]["event"] == json!({"type":"stdout","chunk":"pre-cut stdout"})));
    let active_turn_id = snapshot["activeItems"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["item"]["type"] == "tool_progress")
        .unwrap()["turnId"]
        .clone();
    assert_ne!(active_turn_id, json!("one"));
    let active_id = active_turn_id.as_str().unwrap();
    assert_eq!(preview["result"]["turnStatuses"][active_id], "running");
    assert_eq!(snapshot["turnStatuses"][active_id], "running");
    // The archived fixture has no terminal record; recovery must keep its
    // interrupted status while only the actual lease is reported running.
    assert_eq!(snapshot["turnStatuses"]["one"], "interrupted");
    assert!(snapshot["coveredToolResults"]
        .as_array()
        .unwrap()
        .contains(&json!({"turnId":"one","toolUseId":"shell-one"})));
    assert!(!snapshot["coveredToolResults"]
        .as_array()
        .unwrap()
        .contains(&json!({"turnId":active_turn_id,"toolUseId":"shell-one"})));
    std::fs::write(release, "release").unwrap();
    drop(release_guard);
    loop {
        let event = client.next_message().await;
        if event["method"] == "turn/completed" {
            break;
        }
    }
    let finished = follow(&mut client, 31, &session, true).await;
    assert_eq!(
        finished["result"]["snapshot"]["turnStatuses"][active_id],
        "completed"
    );
    client
        .send(
            json!({"id":32,"method":"engine/readThreadHistory","params":{
                "threadId":session
            }}),
        )
        .await;
    let finished_preview = client.response_for(32).await.0;
    assert_eq!(
        finished_preview["result"]["turnStatuses"][active_id],
        "completed"
    );
    client.close().await;
}

#[derive(Debug)]
struct PendingHistoryRollout {
    inner: Box<dyn klaus_agent_core::store::session_store::LiveRollout>,
    pending: Mutex<Vec<Message>>,
    armed: std::sync::atomic::AtomicBool,
    entered: Notify,
    released: Notify,
}

impl PendingHistoryRollout {
    fn record_pending_message(&self, message: Message) {
        let mut pending = self.pending.lock().unwrap();
        assert!(
            pending.len() < 8,
            "the deterministic test writer is bounded"
        );
        pending.push(message);
    }
}

#[async_trait::async_trait]
impl klaus_agent_core::store::session_store::LiveRollout for PendingHistoryRollout {
    async fn prepare_batch(
        &self,
        maximum_records: usize,
    ) -> std::io::Result<klaus_agent_core::store::session_store::PreparedRolloutBatch<'_>> {
        let admission = self.inner.prepare_batch(maximum_records).await?;
        Ok(
            klaus_agent_core::store::session_store::PreparedRolloutBatch::new(move |items| {
                assert!(items.len() <= maximum_records);
                let mut forwarded = Vec::new();
                for item in items {
                    match item {
                        klaus_agent_core::schemas::rollout::RolloutItem::Message { message } => {
                            self.record_pending_message(message);
                        }
                        other => forwarded.push(other),
                    }
                }
                admission.commit(forwarded);
            }),
        )
    }

    fn inspection_revision(&self) -> Option<u64> {
        let pending = self.pending.lock().unwrap();
        self.inner
            .inspection_revision()
            .map(|revision| revision + pending.len() as u64)
    }
    async fn append_message(&self, message: Message) {
        self.record_pending_message(message);
    }
    async fn append_turn_context(
        &self,
        ctx: klaus_agent_core::schemas::rollout::TurnContextSnapshot,
    ) {
        self.inner.append_turn_context(ctx).await;
    }
    async fn append_compaction(
        &self,
        record: klaus_agent_core::store::session_store::CompactionRecord,
    ) {
        self.inner.append_compaction(record).await;
    }
    async fn append_event(
        &self,
        event: klaus_agent_core::runtime_values::RuntimeString,
        options: Option<klaus_agent_core::store::session_store::AppendEventOptions>,
    ) {
        self.inner.append_event(event, options).await;
    }
    async fn append_rollback(
        &self,
        record: klaus_agent_core::store::session_store::RollbackRecord,
    ) {
        self.inner.append_rollback(record).await;
    }
    async fn append_checkpoint(
        &self,
        record: klaus_agent_core::store::session_store::CheckpointRecord,
    ) {
        self.inner.append_checkpoint(record).await;
    }
    async fn append_usage(&self, record: klaus_agent_core::schemas::rollout::UsageLedgerItem) {
        self.inner.append_usage(record).await;
    }
    async fn append_subagent_lifecycle(
        &self,
        record: klaus_agent_core::schemas::subagent_lifecycle::SubagentLifecycleRecord,
    ) {
        self.inner.append_subagent_lifecycle(record).await;
    }
    async fn append_background_process_lifecycle(
        &self,
        record: klaus_agent_core::schemas::background_process::BackgroundProcessRecord,
    ) {
        self.inner.append_background_process_lifecycle(record).await;
    }
    async fn flush(&self) -> std::io::Result<()> {
        if self.armed.swap(false, std::sync::atomic::Ordering::SeqCst) {
            self.entered.notify_one();
            self.released.notified().await;
        }
        let has_pending = !self.pending.lock().unwrap().is_empty();
        if has_pending {
            let admission = self.inner.prepare_batch(8).await?;
            // Transfer under the same lock used by inspection_revision so the
            // accepted count cannot temporarily decrease during a flush.
            let mut pending = self.pending.lock().unwrap();
            let items = pending
                .drain(..)
                .map(|message| klaus_agent_core::schemas::rollout::RolloutItem::Message { message })
                .collect();
            admission.commit(items);
        }
        self.inner.flush().await
    }
    async fn shutdown(&self) -> std::io::Result<()> {
        self.flush().await?;
        self.inner.shutdown().await
    }
}

#[tokio::test]
async fn original_follow_awaits_pending_writer_before_reading_saved_history() {
    use klaus_agent_core::store::session_store::LiveRollout;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    let dir = tempfile::tempdir().unwrap();
    let store = history_fixture(dir.path());
    let agent = make_agent_for_tests(vec![]);
    let read_calls = Arc::new(AtomicUsize::new(0));
    let calls = read_calls.clone();
    let mut lifecycle = crate::lifecycle::protocol_lifecycle(agent.clone(), Some(store.clone()));
    let reader = lifecycle.read_thread.take().unwrap();
    lifecycle.read_thread = Some(Arc::new(move |id| {
        calls.fetch_add(1, Ordering::SeqCst);
        reader(id)
    }));
    let gate: Arc<Mutex<Option<Arc<PendingHistoryRollout>>>> = Arc::new(Mutex::new(None));
    let captured = gate.clone();
    let resume_store = store.clone();
    let create: CreateSession = Arc::new(move |ctx| {
        let store = resume_store.clone();
        let agent = agent.clone();
        let captured = captured.clone();
        Box::pin(async move {
            let resumed = store.resume_for_write(&ctx.resume.unwrap()).await?;
            let rollout = Arc::new(PendingHistoryRollout {
                inner: resumed.rollout,
                pending: Mutex::new(Vec::new()),
                armed: AtomicBool::new(false),
                entered: Notify::new(),
                released: Notify::new(),
            });
            let session = agent.start_session(StartSessionOptions {
                id: Some("history".into()),
                initial_history: Some(resumed.resume.messages),
                rollout: Some(rollout.clone()),
                store: Some(store),
                approval_handler: Some(ctx.approval_handler),
                elicit_handler: Some(ctx.elicit_handler),
                permission_mode: Some(ExternalPermissionMode::BypassPermissions),
                ..Default::default()
            });
            session.restore_turn_index(resumed.resume.turn_index);
            *captured.lock().unwrap() = Some(rollout);
            Ok(session)
        })
    });
    let mut client = TestClient::start_full(
        create,
        Some(lifecycle),
        Some(dir.path().join("rules.json")),
        Some(dir.path().join("config.json")),
    );
    let session = start(&mut client).await;
    follow(&mut client, 3, &session, false).await;
    let rollout = gate.lock().unwrap().as_ref().unwrap().clone();
    rollout
        .append_message(Message::User(
            klaus_agent_core::schemas::message::UserMessage {
                uuid: "pending-original".into(),
                question_answer: None,
                content: vec![ContentBlock::Text(
                    klaus_agent_core::schemas::content::TextBlock {
                        text: "queued but not written".into(),
                        citations: None,
                        harness_envelope: None,
                        message_id: None,
                        phase: None,
                    },
                )],
                is_meta: None,
            },
        ))
        .await;
    assert!(!store.read_thread("history").await.unwrap().raw_items.iter().any(|item|
        matches!(item, klaus_agent_core::schemas::rollout::RolloutItem::Message { message:Message::User(row) } if row.uuid == "pending-original")));
    rollout.armed.store(true, Ordering::SeqCst);
    client
        .send(json!({"id":4,"method":"session/follow","params":{
            "sessionId":session,"includeOriginalHistory":true
        }}))
        .await;
    tokio::time::timeout(REPLY_TIMEOUT, rollout.entered.notified())
        .await
        .unwrap();
    assert_eq!(
        read_calls.load(Ordering::SeqCst),
        0,
        "the saved reader must not race the pending writer barrier"
    );
    rollout.released.notify_one();
    let reply = client.response_for(4).await.0;
    assert!(reply["result"]["snapshot"]["history"]
        .as_array()
        .unwrap()
        .iter()
        .any(|row| row["uuid"] == "pending-original"));
    assert_eq!(read_calls.load(Ordering::SeqCst), 1);
    client.close().await;
}

#[tokio::test]
async fn original_follow_overlays_live_structured_results_after_the_saved_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let store = history_fixture(dir.path());
    let prefix = store.read_thread("history").await.unwrap();
    let input = klaus_agent_core::runtime_values::runtime_json::parse(
        &json!({"command":"printf 'live structured'; exit 7"})
            .to_string()
            .into(),
    )
    .unwrap();
    let mut first = vec![ev::message_start(None)];
    first.extend(ev::tool_use("live-result-tool", "bash", &input));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let agent = make_agent_for_tests(vec![first, text_turn("final result")]);
    let mut lifecycle = crate::lifecycle::protocol_lifecycle(agent.clone(), Some(store.clone()));
    // A fixed prefix represents the file snapshot taken before a concurrent
    // tool completion. The overlay must preserve its separate typed output.
    lifecycle.read_thread = Some(Arc::new(move |_| {
        let prefix = prefix.clone();
        Box::pin(async move { Ok(prefix) })
    }));
    let mut client = TestClient::start_full(
        persistent_create(agent, store, Arc::new(Mutex::new(None))),
        Some(lifecycle),
        Some(dir.path().join("rules.json")),
        Some(dir.path().join("config.json")),
    );
    let session = start(&mut client).await;
    client
        .send(json!({"id":3,"method":"turn/start","params":{
            "sessionId":session,"input":"run result command"
        }}))
        .await;
    client.response_for(3).await;
    loop {
        if client.next_message().await["method"] == "turn/completed" {
            break;
        }
    }
    let reply = follow(&mut client, 4, &session, true).await;
    let result = reply["result"]["snapshot"]["history"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["type"] == "tool_result" && row["toolUseId"] == "live-result-tool")
        .unwrap();
    assert_eq!(result["output"]["exitCode"], 7);
    assert_eq!(result["output"]["timedOut"], false);
    assert!(result["content"]
        .as_array()
        .unwrap()
        .iter()
        .any(|block| block["text"]
            .as_str()
            .is_some_and(|text| text.contains("live structured"))));
    client.close().await;
}

#[tokio::test]
async fn original_follow_retries_when_compaction_appends_after_flush_and_read() {
    use klaus_agent_core::store::session_store::{CompactionRecord, LiveRollout};
    use std::sync::atomic::{AtomicUsize, Ordering};
    let dir = tempfile::tempdir().unwrap();
    let store = history_fixture(dir.path());
    let agent = make_agent_for_tests(vec![]);
    let captured: Arc<Mutex<Option<Arc<dyn LiveRollout>>>> = Arc::new(Mutex::new(None));
    let target = captured.clone();
    let resume_store = store.clone();
    let create_agent = agent.clone();
    let create: CreateSession = Arc::new(move |ctx| {
        let store = resume_store.clone();
        let agent = create_agent.clone();
        let target = target.clone();
        Box::pin(async move {
            let resumed = store.resume_for_write(&ctx.resume.unwrap()).await?;
            let rollout: Arc<dyn LiveRollout> = Arc::from(resumed.rollout);
            let session = agent.start_session(StartSessionOptions {
                id: Some("history".into()),
                initial_history: Some(resumed.resume.messages),
                rollout: Some(rollout.clone()),
                store: Some(store),
                approval_handler: Some(ctx.approval_handler),
                permission_mode: Some(ExternalPermissionMode::BypassPermissions),
                ..Default::default()
            });
            session.restore_turn_index(resumed.resume.turn_index);
            *target.lock().unwrap() = Some(rollout);
            Ok(session)
        })
    });
    let entered = Arc::new(Notify::new());
    let released = Arc::new(Notify::new());
    let reads = Arc::new(AtomicUsize::new(0));
    let calls = reads.clone();
    let ready = entered.clone();
    let release = released.clone();
    let mut lifecycle = crate::lifecycle::protocol_lifecycle(agent, Some(store.clone()));
    let actual = lifecycle.read_thread.take().unwrap();
    lifecycle.read_thread = Some(Arc::new(move |id| {
        let actual = actual.clone();
        let calls = calls.clone();
        let ready = ready.clone();
        let release = release.clone();
        Box::pin(async move {
            let snapshot = actual(id).await?;
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                ready.notify_one();
                release.notified().await;
            }
            Ok(snapshot)
        })
    }));
    let mut client = TestClient::start_full(
        create,
        Some(lifecycle),
        Some(dir.path().join("rules.json")),
        Some(dir.path().join("config.json")),
    );
    let session = start(&mut client).await;
    client
        .send(json!({"id":3,"method":"session/follow","params":{
            "sessionId":session,"includeOriginalHistory":true
        }}))
        .await;
    tokio::time::timeout(REPLY_TIMEOUT, entered.notified())
        .await
        .unwrap();
    let rollout = captured.lock().unwrap().as_ref().unwrap().clone();
    // This original message has already left the live model context by the
    // time the stale saved read returns. The accepted-write fence must retry.
    rollout
        .append_message(Message::User(
            klaus_agent_core::schemas::message::UserMessage {
                uuid: "concurrent-original".into(),
                question_answer: None,
                is_meta: None,
                content: vec![ContentBlock::Text(
                    klaus_agent_core::schemas::content::TextBlock {
                        text: "original erased from prompt context".into(),
                        citations: None,
                        harness_envelope: None,
                        message_id: None,
                        phase: None,
                    },
                )],
            },
        ))
        .await;
    rollout
        .append_compaction(CompactionRecord {
            approval_parent_history: None,
            source_message_ids: None,
            summary: "concurrent summary".into(),
            replaced_range: (0, 1),
            replacement_history: Some(Vec::new()),
        })
        .await;
    released.notify_one();
    let reply = client.response_for(3).await.0;
    assert_eq!(
        reads.load(Ordering::SeqCst),
        2,
        "a moving rollout cannot be accepted"
    );
    assert!(reply["result"]["snapshot"]["history"]
        .as_array()
        .unwrap()
        .iter()
        .any(|row| row["uuid"] == "concurrent-original"));
    client.close().await;
}

#[tokio::test]
async fn original_follow_persists_unfinished_reasoning_identity_when_tool_is_aborted() {
    let dir = tempfile::tempdir().unwrap();
    let store = history_fixture(dir.path());
    let agent = make_agent_with_call_model(Box::new(move |_, signal| {
        Box::pin(
            futures::stream::iter(vec![
                ev::message_start(None),
                ModelStreamEvent::ToolUseStart {
                    id: "aborted-tool".into(),
                    name: "bash".into(),
                    thought_signature: None,
                    tool_kind: None,
                    input_format: None,
                },
                ev::thinking("unfinished reasoning"),
            ])
            .chain(futures::stream::once(async move {
                signal.cancelled().await;
                ev::stop(Some(ModelStopReason::EndTurn), None)
            })),
        )
    }));
    let lifecycle = crate::lifecycle::protocol_lifecycle(agent.clone(), Some(store.clone()));
    let mut client = TestClient::start_full(
        persistent_create(agent, store, Arc::new(Mutex::new(None))),
        Some(lifecycle),
        Some(dir.path().join("rules.json")),
        Some(dir.path().join("config.json")),
    );
    let session = start(&mut client).await;
    client
        .send(json!({"id":3,"method":"turn/start","params":{
            "sessionId":session,"input":"reason before abort"
        }}))
        .await;
    client.response_for(3).await;
    let reasoning_id = loop {
        let event = client.next_message().await;
        if event["method"] == "item/reasoning/delta" {
            break event["params"]["messageId"].clone();
        }
    };
    client
        .send(json!({"id":4,"method":"turn/interrupt","params":{"sessionId":session}}))
        .await;
    let (_, preceding) = client.response_for(4).await;
    if !preceding
        .iter()
        .any(|event| event["method"] == "turn/completed")
    {
        loop {
            if client.next_message().await["method"] == "turn/completed" {
                break;
            }
        }
    }
    let reply = follow(&mut client, 5, &session, true).await;
    let snapshot = &reply["result"]["snapshot"];
    let thinking = snapshot["history"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|row| row["content"].as_array().into_iter().flatten())
        .find(|block| block["messageId"] == reasoning_id)
        .unwrap();
    assert_eq!(thinking["type"], "thinking");
    assert_eq!(thinking["text"], "unfinished reasoning");
    assert!(snapshot["coveredMessageIds"]
        .as_array()
        .unwrap()
        .contains(&reasoning_id));
    client.close().await;
}
