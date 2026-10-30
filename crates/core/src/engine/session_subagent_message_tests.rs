//! Ordinary worker messages exercise the real query, tool and sidechain runtime.

use super::*;
macro_rules! exact_json {
    ($($json:tt)+) => {{
        crate::runtime_values::runtime_json::parse(&RuntimeString::from(stringify!($($json)+)))
            .expect("test JSON literal must parse")
    }};
}
#[path = "session_subagent_hardening_tests.rs"]
mod hardening;
#[path = "session_subagent_residency_tests.rs"]
mod residency_admission;
#[path = "session_subagent_writer_tests.rs"]
mod writer_persistence;
use crate::types::permission::{PermissionContext, PermissionReason, PermissionResult};
use crate::types::tool::{BackgroundTaskStatus, SubagentMessageRequest};
use tokio::sync::{Notify, Semaphore};

struct MessageGate {
    entered: Arc<Notify>,
    release: Arc<Semaphore>,
    permissions: Arc<Mutex<Vec<PermissionContext>>>,
}

#[async_trait::async_trait]
impl Tool for MessageGate {
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _: &RuntimeJson) -> bool {
        true
    }
    fn persistence_identity(&self) -> Option<RuntimeString> {
        Some("fixture:message-gate:1".into())
    }
    fn static_name(&self) -> &str {
        "message_gate"
    }
    fn static_description(&self) -> &str {
        "Wait at a deterministic read-only runtime boundary."
    }
    fn input_schema(&self) -> RuntimeJson {
        exact_json!({"type":"object","properties":{},"additionalProperties":false})
    }
    fn is_read_only(&self, _: &RuntimeJson) -> bool {
        true
    }
    async fn check_permissions(
        &self,
        _: &RuntimeJson,
        _: &ToolContext,
    ) -> Option<PermissionResult> {
        Some(PermissionResult::Allow {
            updated_input: None,
            reason: PermissionReason::ToolSelfCheck,
        })
    }
    async fn call(
        &self,
        _: RuntimeJson,
        ctx: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, crate::types::errors::ToolError> {
        self.permissions
            .lock()
            .unwrap()
            .push(ctx.permission.clone());
        self.entered.notify_one();
        tokio::select! {
            biased;
            _ = ctx.signal.cancelled() => return Err(crate::types::errors::ToolError::new("gate cancelled")),
            permit = self.release.acquire() => permit.unwrap().forget(),
        }
        Ok(text_result(
            exact_json!({"ok":true}),
            "gate released",
            false,
        ))
    }
}

fn gate() -> Arc<MessageGate> {
    Arc::new(MessageGate {
        entered: Arc::new(Notify::new()),
        release: Arc::new(Semaphore::new(0)),
        permissions: Arc::new(Mutex::new(Vec::new())),
    })
}

fn gate_turn() -> Vec<crate::types::provider::ModelStreamEvent> {
    let mut events = vec![ev::message_start(None)];
    events.extend(exact_tool_use_events(
        "message-gate-call",
        "message_gate",
        &exact_json!({}),
    ));
    events.push(ev::stop(
        Some(ModelStopReason::ToolUse),
        Some(make_usage(None, None)),
    ));
    events
}

fn fixture(
    turns: ScriptedTurns,
    tools: Vec<AnyTool>,
) -> (Arc<Session>, Arc<Mutex<Vec<ModelRequest>>>) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let mut config = default_config();
    config.agent_memory = false;
    config.memory_recall = false;
    config.fork = false;
    let session = mk_session_with_deps(
        unique_subagent_test_deps(recording_deps(requests.clone(), turns)),
        config,
        tools,
    );
    (session, requests)
}

async fn spawn_worker(session: &Arc<Session>, mut options: SpawnSubAgentOptions) -> RuntimeString {
    options.fork_context.get_or_insert(ForkContext::None);
    session.make_spawn_background_agent(
        0,
        None,
        session.permission_context(),
        Arc::new(Vec::new),
        false,
        None,
        None,
    )(options)
    .await
    .expect("worker creation must be accepted")
    .agent_id
}

async fn completed_parent(session: &Arc<Session>, prompt: &str) -> RuntimeString {
    let run = session.make_spawn_sub_agent_with_capture(
        0,
        CancellationToken::new(),
        session.permission_context(),
        Arc::new(Vec::new),
        false,
        None,
        None,
        None,
        Some("fixture-parent-turn".into()),
    )(SpawnSubAgentOptions {
        prompt: prompt.into(),
        fork_context: Some(ForkContext::None),
        ..Default::default()
    })
    .await
    .expect("the real direct parent must accept its initial input");
    let id = run.agent_id.as_str().unwrap().clone();
    let events = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        run.events.collect::<Vec<_>>(),
    )
    .await
    .expect("the real direct parent must complete");
    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::TurnCompleted {
            stop_reason: TerminalReason::Completed,
            ..
        }
    )));
    finished(session, &id).await;
    settle_ordinary_owners(session).await;
    assert_ordinary_creation_bindings(session, &id, 1, Some("fixture-parent-turn"), None);
    id
}

fn message(target: &RuntimeString, text: &str, user_initiated: bool) -> SubagentMessageRequest {
    SubagentMessageRequest {
        trigger_turn: true,
        target: target.clone(),
        message: text.into(),
        summary: "Runtime follow-up".into(),
        sender_agent_id: None,
        user_initiated,
    }
}

async fn finished(session: &Arc<Session>, id: &RuntimeString) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if session
                .background_task_registry()
                .get(id)
                .is_some_and(|task| task.status != BackgroundTaskStatus::Running)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|error| {
        panic!(
            "worker must settle: {error}; id={id:?}; tasks={:?}; queue={:?}; admitted={:?}",
            session.background_agent_summaries(),
            session.conversation.queue().snapshot(),
            session.conversation.queue().admitted_snapshot()
        );
    });
}

async fn entered(gate: &MessageGate) {
    tokio::time::timeout(std::time::Duration::from_secs(5), gate.entered.notified())
        .await
        .expect("the real tool must run");
}

fn assert_ordinary_creation_bindings(
    session: &Arc<Session>,
    id: &RuntimeString,
    depth: u32,
    spawn_turn_id: Option<&str>,
    parent_agent_id: Option<&str>,
) {
    let expected_turn = spawn_turn_id.map(RuntimeString::from);
    {
        let entries = session.subagents.background_agents.lock().unwrap();
        let entry = entries.get(&id.try_to_utf8().unwrap()).unwrap();
        assert_eq!(entry.depth, depth);
        assert_eq!(entry.spawn_turn_id, expected_turn);
    }
    let lifecycle = session.subagent_lifecycle_snapshot();
    let record = lifecycle
        .agents
        .iter()
        .find(|record| &record.agent_id == id)
        .unwrap();
    assert_eq!(record.context.depth, depth);
    assert_eq!(record.context.spawn_turn_id, expected_turn);
    assert_eq!(
        record.context.parent_agent_id,
        parent_agent_id.map(RuntimeString::from)
    );
}

async fn settle_ordinary_owners(session: &Arc<Session>) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let handles = std::mem::take(&mut session.background_tasks.lock().unwrap().handles);
            if handles.is_empty() {
                break;
            }
            for handle in handles {
                handle.await.unwrap();
            }
        }
    })
    .await
    .expect("registered ordinary owners must finish");
}

fn text_positions(request: &ModelRequest, text: &str) -> Vec<usize> {
    request
        .messages
        .iter()
        .enumerate()
        .filter_map(|(index, message)| match message {
            Message::User(user)
                if user.content.iter().any(
                    |block| matches!(block, ContentBlock::Text(block) if block.text == text),
                ) =>
            {
                Some(index)
            }
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn root_accepts_queue_only_mail_but_rejects_followup_work_before_enqueue() {
    let (session, requests) = fixture(vec![text_turn("sender completed")], vec![]);
    let followup = message(
        &session.id,
        "Do not start the root through child follow-up.",
        false,
    );
    let error = session
        .send_subagent_message(followup.clone())
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(
        error,
        RuntimeString::from("Follow-up tasks cannot target the root agent.")
    );
    assert!(session.conversation.queue().snapshot().is_empty());
    assert!(requests.lock().unwrap().is_empty());

    let child = spawn_worker(
        &session,
        SpawnSubAgentOptions {
            prompt: "Provide an existing sender identity.".into(),
            ..Default::default()
        },
    )
    .await;
    finished(&session, &child).await;
    let pending = session.conversation.queue().snapshot().len();
    let inference_count = requests.lock().unwrap().len();
    let mut mail = followup;
    mail.trigger_turn = false;
    mail.sender_agent_id = Some(child);
    let delivered = session.send_subagent_message(mail).await.unwrap().unwrap();
    assert_eq!(delivered.agent_id, session.id);
    assert!(!delivered.resumed);
    assert_eq!(session.conversation.queue().snapshot().len(), pending + 1);
    assert_eq!(requests.lock().unwrap().len(), inference_count);
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn ordinary_running_message_is_consumed_after_the_next_tool_without_restart() {
    let gate = gate();
    let (session, requests) = fixture(vec![gate_turn(), text_turn("done")], vec![gate.clone()]);
    let id = spawn_worker(
        &session,
        SpawnSubAgentOptions {
            prompt: "initial".into(),
            name: Some("reader".into()),
            ..Default::default()
        },
    )
    .await;
    entered(&gate).await;
    let result = session
        .send_subagent_message(message(&"reader".into(), "/literal follow-up", false))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.agent_id, id);
    assert!(!result.resumed);
    gate.release.add_permits(1);
    finished(&session, &id).await;
    {
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(text_positions(&requests[1], "/literal follow-up").len(), 1);
        assert_eq!(text_positions(&requests[1], "initial").len(), 1);
    }
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn ordinary_completed_message_resumes_the_same_history_and_identity() {
    let (session, requests) = fixture(
        vec![text_turn("first answer"), text_turn("second answer")],
        vec![],
    );
    let id = spawn_worker(
        &session,
        SpawnSubAgentOptions {
            prompt: "initial".into(),
            ..Default::default()
        },
    )
    .await;
    finished(&session, &id).await;
    let result = session
        .send_subagent_message(message(&id, "follow-up", false))
        .await
        .unwrap()
        .unwrap();
    assert!(result.resumed);
    assert_eq!(result.agent_id, id);
    finished(&session, &id).await;
    {
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(text_positions(&requests[1], "initial").len(), 1);
        assert_eq!(text_positions(&requests[1], "follow-up").len(), 1);
        assert!(requests[1].messages.iter().any(|message| matches!(message, Message::Assistant(assistant) if assistant.content.iter().any(|block| matches!(block, ContentBlock::Text(text) if text.text == "first answer")))));
    }
    assert_eq!(session.subagent_lifecycle_snapshot().agents.len(), 1);
    assert_eq!(session.subagent_lifecycle_snapshot().agents[0].agent_id, id);
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn ordinary_resumed_failure_receipt_does_not_reuse_the_previous_success() {
    let (session, requests) = fixture(
        vec![
            text_turn("previous successful worker result"),
            provider_error_turn("current resumed worker failure"),
        ],
        vec![],
    );
    let (send, mut receipts) = tokio::sync::mpsc::unbounded_channel();
    session.set_agent_notification_sink(Some(Arc::new(move |receipt| {
        let _ = send.send(receipt.clone());
    })));
    let id = spawn_worker(
        &session,
        SpawnSubAgentOptions {
            prompt: "initial".into(),
            ..Default::default()
        },
    )
    .await;
    let first = tokio::time::timeout(std::time::Duration::from_secs(5), receipts.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.status, "completed");
    let resumed = session
        .send_subagent_message(message(&id, "explicit failing follow-up", false))
        .await
        .unwrap()
        .unwrap();
    assert!(resumed.resumed);
    let failed = tokio::time::timeout(std::time::Duration::from_secs(5), receipts.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failed.status, "failed");
    assert_eq!(failed.agent_id.as_ref(), Some(&id));
    assert_eq!(requests.lock().unwrap().len(), 2);
    let notice = session
        .queued_messages()
        .into_iter()
        .find(|item| item.uuid.as_ref() == Some(&failed.notification_id))
        .expect("the failed receipt identifies its retained result message");
    assert_eq!(notice.mode, "task-notification");
    assert!(!notice.triggers_turn());
    let crate::engine::message_queue::QueuedValue::Text(payload) = &notice.value else {
        panic!("the failure notification must contain text");
    };
    assert!(
        payload.contains("current resumed worker failure"),
        "{payload:?}"
    );
    assert!(
        !payload.contains("previous successful worker result"),
        "the new failure must not masquerade as the previous answer: {payload:?}"
    );
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn ordinary_terminal_boundary_message_is_not_lost() {
    let (session, requests) = fixture(vec![text_turn("first"), text_turn("next")], vec![]);
    let weak = Arc::downgrade(&session);
    let sent = Arc::new(AtomicBool::new(false));
    let (tx, rx) = tokio::sync::oneshot::channel();
    let tx = Arc::new(Mutex::new(Some(tx)));
    session.set_subagent_event_sink(Some(Arc::new(move |id, event| {
        if matches!(
            event,
            crate::engine::session::SubagentStreamEvent::Event(EngineEvent::TurnCompleted { .. })
        ) && !sent.swap(true, Ordering::SeqCst)
        {
            let session = weak.upgrade().unwrap();
            let id = RuntimeString::from(id);
            let tx = tx.lock().unwrap().take().unwrap();
            tokio::spawn(async move {
                let result = session
                    .send_subagent_message(message(&id, "at terminal handoff", false))
                    .await;
                tx.send(result).ok();
            });
        }
    })));
    let id = spawn_worker(
        &session,
        SpawnSubAgentOptions {
            prompt: "initial".into(),
            ..Default::default()
        },
    )
    .await;
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), rx)
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(result.agent_id, id);
    finished(&session, &id).await;
    {
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(text_positions(&requests[1], "at terminal handoff").len(), 1);
    }
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn ordinary_concurrent_messages_have_one_resume_owner_and_keep_queue_order() {
    let gate = gate();
    let (session, requests) = fixture(
        vec![text_turn("first"), gate_turn(), text_turn("next")],
        vec![gate.clone()],
    );
    let id = spawn_worker(
        &session,
        SpawnSubAgentOptions {
            prompt: "initial".into(),
            ..Default::default()
        },
    )
    .await;
    finished(&session, &id).await;
    let (one, two) = tokio::join!(
        session.send_subagent_message(message(&id, "one", false)),
        session.send_subagent_message(message(&id, "two", false))
    );
    assert!(one.unwrap().unwrap().resumed);
    assert!(!two.unwrap().unwrap().resumed);
    entered(&gate).await;
    gate.release.add_permits(1);
    finished(&session, &id).await;
    {
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        let last = requests.last().unwrap();
        assert_eq!(text_positions(last, "one").len(), 1);
        assert_eq!(text_positions(last, "two").len(), 1);
        assert!(text_positions(last, "one")[0] < text_positions(last, "two")[0]);
    }
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn ordinary_model_stop_can_resume_only_after_the_old_run_exits() {
    let gate = gate();
    let (session, requests) = fixture(
        vec![gate_turn(), text_turn("recovered")],
        vec![gate.clone()],
    );
    let id = spawn_worker(
        &session,
        SpawnSubAgentOptions {
            prompt: "initial".into(),
            ..Default::default()
        },
    )
    .await;
    entered(&gate).await;
    assert!(session.background_task_registry().stop(&id));
    let result = session
        .send_subagent_message(message(&id, "recover after model stop", false))
        .await
        .unwrap()
        .unwrap();
    assert!(result.resumed);
    finished(&session, &id).await;
    assert_eq!(requests.lock().unwrap().len(), 2);
    assert_eq!(
        text_positions(&requests.lock().unwrap()[1], "recover after model stop").len(),
        1
    );
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn ordinary_user_stop_rejects_automatic_recovery_and_trusted_input_clears_it() {
    let gate = gate();
    let (session, requests) = fixture(
        vec![
            gate_turn(),
            text_turn("manual recovery"),
            text_turn("automatic allowed"),
        ],
        vec![gate.clone()],
    );
    let id = spawn_worker(
        &session,
        SpawnSubAgentOptions {
            prompt: "initial".into(),
            ..Default::default()
        },
    )
    .await;
    entered(&gate).await;
    assert!(session.background_task_registry().stop_by_user(&id));
    finished(&session, &id).await;
    let error = session
        .send_subagent_message(message(&id, "automatic", false))
        .await
        .unwrap()
        .unwrap_err();
    assert!(error.contains("cancelled by the user"));
    assert_eq!(requests.lock().unwrap().len(), 1);
    let result = session
        .send_subagent_message(message(&id, "manual", true))
        .await
        .unwrap()
        .unwrap();
    assert!(result.resumed);
    finished(&session, &id).await;
    assert_eq!(
        text_positions(&requests.lock().unwrap()[1], "manual").len(),
        1
    );
    assert!(session
        .send_subagent_message(message(&id, "automatic now allowed", false))
        .await
        .unwrap()
        .is_ok());
    finished(&session, &id).await;
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn ordinary_failed_worker_keeps_its_original_tools_for_message_recovery() {
    let gate = gate();
    let (session, requests) = fixture(
        vec![
            provider_error_turn("first run failed"),
            gate_turn(),
            text_turn("recovered"),
        ],
        vec![gate.clone(), create_write_tool()],
    );
    let id = spawn_worker(
        &session,
        SpawnSubAgentOptions {
            prompt: "initial".into(),
            tools: Some(vec![gate.clone()]),
            ..Default::default()
        },
    )
    .await;
    finished(&session, &id).await;
    assert_eq!(
        session.background_task_registry().get(&id).unwrap().status,
        BackgroundTaskStatus::Failed
    );
    session
        .send_subagent_message(message(&id, "recover", false))
        .await
        .unwrap()
        .unwrap();
    entered(&gate).await;
    assert_eq!(gate.permissions.lock().unwrap().len(), 1);
    gate.release.add_permits(1);
    finished(&session, &id).await;
    {
        let requests = requests.lock().unwrap();
        for request in requests.iter() {
            assert!(!request
                .tools
                .iter()
                .any(|tool| tool.name == "write" || tool.name == "Write"));
        }
    }
    session.shutdown_and_wait().await.unwrap();
}

async fn persistent_fixture(
    turns: ScriptedTurns,
    tools: Vec<AnyTool>,
) -> (
    Arc<Session>,
    Arc<JsonlSessionStore>,
    Arc<Mutex<Vec<ModelRequest>>>,
    PathBuf,
) {
    persistent_fixture_with_role(turns, tools, None).await
}

async fn persistent_fixture_with_role(
    turns: ScriptedTurns,
    tools: Vec<AnyTool>,
    role: Option<&str>,
) -> (
    Arc<Session>,
    Arc<JsonlSessionStore>,
    Arc<Mutex<Vec<ModelRequest>>>,
    PathBuf,
) {
    let definitions = role.map(|name| {
        vec![crate::engine::builtin_agents::BUILTIN_AGENTS
            .iter()
            .find(|definition| definition.name == name)
            .unwrap()
            .clone()]
    });
    persistent_fixture_with_definitions(turns, tools, definitions).await
}

async fn persistent_fixture_with_definitions(
    turns: ScriptedTurns,
    tools: Vec<AnyTool>,
    definitions: Option<Vec<AgentDefinition>>,
) -> (
    Arc<Session>,
    Arc<JsonlSessionStore>,
    Arc<Mutex<Vec<ModelRequest>>>,
    PathBuf,
) {
    persistent_fixture_with_provider_and_definitions(
        turns,
        tools,
        definitions,
        Arc::new(fake_provider()),
    )
    .await
}

async fn persistent_fixture_with_provider_and_definitions(
    turns: ScriptedTurns,
    tools: Vec<AnyTool>,
    definitions: Option<Vec<AgentDefinition>>,
    provider: Arc<dyn ModelProvider>,
) -> (
    Arc<Session>,
    Arc<JsonlSessionStore>,
    Arc<Mutex<Vec<ModelRequest>>>,
    PathBuf,
) {
    let dir = temp_store_dir();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.to_str().unwrap().to_owned()),
        archive_dir: None,
    }));
    let rollout: RolloutSink = Arc::from(
        store
            .create(SessionMeta {
                id: "s1".into(),
                created_at: 1,
                cwd: runtime_path_from_host_path(&dir).into_runtime_string(),
                model: "fake-model".into(),
                title: None,
                originator: None,
                cli_version: None,
                source: None,
                role: None,
                parent_thread_id: None,
                forked_from_id: None,
                git: None,
                host_metadata: None,
            })
            .await
            .unwrap(),
    );
    let requests = Arc::new(Mutex::new(Vec::new()));
    let mut config = default_config();
    config.agent_memory = false;
    config.memory_recall = false;
    let session = mk_session_with_provider_memory_store_and_agents(
        provider,
        unique_subagent_test_deps(recording_deps(requests.clone(), turns)),
        config,
        tools,
        None,
        Some(rollout),
        Some(store.clone()),
        None,
        definitions,
    );
    (session, store, requests, dir)
}

#[tokio::test]
async fn ordinary_cold_recovery_rebuilds_the_current_native_tool_inventory() {
    cold_recovery_retains_authority(None).await;
}

#[tokio::test]
async fn explore_cold_followup_reuses_history_and_current_role() {
    cold_recovery_retains_authority(Some("Explore")).await;
}

async fn cold_recovery_retains_authority(role: Option<&str>) {
    let gate = gate();
    let (session, store, initial_requests, dir) = persistent_fixture_with_role(
        vec![text_turn("parent ready"), text_turn("saved")],
        vec![gate.clone()],
        role,
    )
    .await;
    let direct_parent = completed_parent(&session, "Persisted direct parent").await;
    let parent_output_file = session
        .background_task_registry()
        .get(&direct_parent)
        .unwrap()
        .output_file;
    let direct_parent_host = direct_parent.try_to_utf8().unwrap();
    assert_eq!(initial_requests.lock().unwrap().len(), 1);
    assert_eq!(
        text_positions(
            &initial_requests.lock().unwrap()[0],
            "Persisted direct parent"
        )
        .len(),
        1
    );
    let id = session.make_spawn_background_agent(
        1,
        Some("saved-parent-turn".into()),
        session.permission_context(),
        Arc::new(Vec::new),
        false,
        Some(direct_parent_host.clone()),
        None,
    )(SpawnSubAgentOptions {
        prompt: "original".into(),
        fork_context: Some(ForkContext::None),
        name: Some("saved-reader".into()),
        agent_type: role.map(RuntimeString::from),
        ..Default::default()
    })
    .await
    .expect("worker creation must be accepted")
    .agent_id;
    finished(&session, &id).await;
    settle_ordinary_owners(&session).await;
    assert_ordinary_creation_bindings(
        &session,
        &id,
        2,
        Some("saved-parent-turn"),
        Some(&direct_parent_host),
    );
    let loaded = store
        .read_sub_agent("s1", &id.try_to_utf8().unwrap())
        .await
        .unwrap();
    let creations = loaded
        .raw_items
        .iter()
        .filter_map(|item| match item {
            RolloutItem::Event(event) if event.event == "ordinary_subagent_creation" => {
                event.payload.as_ref()
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(creations.len(), 1);
    assert_eq!(
        creations[0].get("depth").and_then(RuntimeJson::as_u64),
        Some(2)
    );
    assert_eq!(
        creations[0]
            .get("spawnTurnId")
            .and_then(RuntimeJson::as_str),
        Some(&RuntimeString::from("saved-parent-turn")),
    );
    assert!(session
        .turn_index
        .lock()
        .unwrap()
        .agent_notifications
        .is_empty());
    if role == Some("Explore") {
        let mut queued = message(&id, "queued search context", false);
        queued.trigger_turn = false;
        assert!(
            !session
                .send_subagent_message(queued)
                .await
                .unwrap()
                .unwrap()
                .resumed
        );
        assert_eq!(initial_requests.lock().unwrap().len(), 2);
        assert_eq!(
            session.background_task_registry().get(&id).unwrap().status,
            BackgroundTaskStatus::Completed
        );
    }
    session.shutdown_and_wait().await.unwrap();
    std::fs::remove_file(runtime_path_to_file_io_host(&parent_output_file)).unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let mut expanded = default_config();
    expanded.agent_memory = false;
    expanded.memory_recall = false;
    let cold = mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        unique_subagent_test_deps(recording_deps(
            requests.clone(),
            vec![gate_turn(), text_turn("continued")],
        )),
        expanded,
        vec![gate.clone(), create_write_tool()],
        None,
        None,
        Some(store.clone()),
        None,
        role.map(|name| {
            vec![crate::engine::builtin_agents::BUILTIN_AGENTS
                .iter()
                .find(|definition| definition.name == name)
                .unwrap()
                .clone()]
        }),
    );
    let cold_parent_turn = ordinary_parent_turn_lease(&cold, "unrelated-cold-parent-turn");
    let result = cold
        .send_subagent_message(message(&"saved-reader".into(), "cold follow-up", false))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.agent_id, id);
    entered(&gate).await;
    assert_eq!(gate.permissions.lock().unwrap().len(), 1);
    gate.release.add_permits(1);
    finished(&cold, &id).await;
    settle_ordinary_owners(&cold).await;
    assert_ordinary_creation_bindings(
        &cold,
        &id,
        2,
        Some("saved-parent-turn"),
        Some(&direct_parent_host),
    );
    assert!(cold
        .turn_index
        .lock()
        .unwrap()
        .agent_notifications
        .is_empty());
    {
        let requests = requests.lock().unwrap();
        let initial = initial_requests.lock().unwrap();
        assert_eq!(initial.len(), 2);
        assert_eq!(text_positions(&initial[1], "original").len(), 1);
        assert_eq!(requests[0].model, initial[1].model);
        if role == Some("Explore") {
            assert_eq!(requests[0].tools, initial[1].tools);
            let prompt = |request: &ModelRequest| {
                request
                    .system
                    .iter()
                    .filter(|part| part.text.contains("read-only exploration sub-agent"))
                    .cloned()
                    .collect::<Vec<_>>()
            };
            assert_eq!(prompt(&initial[1]).len(), 1);
            assert_eq!(prompt(&requests[0]), prompt(&initial[1]));
        }
        assert_eq!(text_positions(&requests[0], "original").len(), 1);
        assert_eq!(text_positions(&requests[0], "cold follow-up").len(), 1);
        assert_eq!(
            requests[0]
                .tools
                .iter()
                .any(|tool| tool.name == "write" || tool.name == "Write"),
            role != Some("Explore")
        );
    }
    drop(cold_parent_turn);
    cold.shutdown_and_wait().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn ordinary_cold_legacy_turn_binding_stays_none_under_an_unrelated_parent_turn() {
    let (session, store, _, dir) = persistent_fixture(vec![text_turn("saved")], vec![]).await;
    let original = session.make_spawn_background_agent(
        0,
        Some("original-stamped-turn".into()),
        session.permission_context(),
        Arc::new(Vec::new),
        false,
        None,
        None,
    )(SpawnSubAgentOptions {
        prompt: "legacy original".into(),
        fork_context: Some(ForkContext::None),
        ..Default::default()
    })
    .await
    .expect("worker creation must be accepted");
    finished(&session, &original.agent_id).await;
    settle_ordinary_owners(&session).await;
    session.shutdown_and_wait().await.unwrap();
    let saved = store
        .read_sub_agent("s1", &original.agent_id.try_to_utf8().unwrap())
        .await
        .unwrap();
    let mut metadata = saved
        .raw_items
        .iter()
        .find_map(|item| match item {
            RolloutItem::Event(event) if event.event == "ordinary_subagent_creation" => {
                event.payload.clone()
            }
            _ => None,
        })
        .unwrap();
    let RuntimeJson::Object(fields) = &mut metadata else {
        panic!("creation metadata must be an object");
    };
    assert!(fields.remove("spawnTurnId").is_some());
    fields.insert("name", RuntimeJson::String("legacy-reader".into()));
    let legacy_key = format!("{}-legacy", original.agent_id.try_to_utf8().unwrap());
    let legacy_id = RuntimeString::from(legacy_key.clone());
    let legacy = store
        .open_sub_agent("s1", &legacy_key, saved.meta.clone().unwrap())
        .await
        .unwrap();
    legacy
        .append_event(
            "ordinary_subagent_creation".into(),
            Some(AppendEventOptions {
                turn_id: None,
                payload: Some(metadata),
            }),
        )
        .await;
    legacy
        .append_event(
            "ordinary_subagent_creation_committed".into(),
            Some(AppendEventOptions {
                turn_id: None,
                payload: Some(RuntimeJson::Bool(true)),
            }),
        )
        .await;
    for item in saved.raw_items {
        if let RolloutItem::Message { message } = item {
            legacy.append_message(message).await;
        }
    }
    legacy.flush().await.unwrap();
    legacy.shutdown().await.unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let mut config = default_config();
    config.agent_memory = false;
    config.memory_recall = false;
    let cold = mk_session_with_provider_memory_and_store(
        Arc::new(fake_provider()),
        unique_subagent_test_deps(recording_deps(
            requests.clone(),
            vec![text_turn("legacy recovered")],
        )),
        config,
        vec![],
        None,
        None,
        Some(store),
        None,
    );
    let parent_turn = ordinary_parent_turn_lease(&cold, "unrelated-live-parent-turn");
    let result = cold
        .send_subagent_message(message(&"legacy-reader".into(), "legacy follow-up", false))
        .await
        .unwrap()
        .unwrap();
    assert!(result.resumed);
    assert_eq!(result.agent_id, legacy_id);
    finished(&cold, &legacy_id).await;
    settle_ordinary_owners(&cold).await;
    assert_ordinary_creation_bindings(&cold, &legacy_id, 1, None, None);
    {
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(text_positions(&requests[0], "legacy original").len(), 1);
        assert_eq!(text_positions(&requests[0], "legacy follow-up").len(), 1);
    }
    let receipts = cold.turn_index.lock().unwrap().agent_notifications.clone();
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].agent_id.as_ref(), Some(&legacy_id));
    assert_eq!(receipts[0].source, "background-task");
    assert_eq!(receipts[0].turn_id, None);
    drop(parent_turn);
    cold.shutdown_and_wait().await.unwrap();
    std::fs::remove_file(runtime_path_to_file_io_host(&original.output_file)).unwrap();
    std::fs::remove_file(std::env::temp_dir().join(format!("klaus-agent-bg-{legacy_key}.txt")))
        .unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn ordinary_id_name_collision_and_duplicate_names_reject_instead_of_guessing() {
    let (session, _) = fixture(
        vec![
            text_turn("one"),
            text_turn("two"),
            text_turn("three"),
            text_turn("ID priority"),
        ],
        vec![],
    );
    let first = spawn_worker(
        &session,
        SpawnSubAgentOptions {
            prompt: "initial".into(),
            name: Some("same-name".into()),
            ..Default::default()
        },
    )
    .await;
    finished(&session, &first).await;
    let second = spawn_worker(
        &session,
        SpawnSubAgentOptions {
            prompt: "initial".into(),
            name: Some("same-name".into()),
            ..Default::default()
        },
    )
    .await;
    finished(&session, &second).await;
    assert!(session
        .send_subagent_message(message(&"same-name".into(), "ambiguous", false))
        .await
        .unwrap()
        .unwrap_err()
        .contains("Ambiguous"));
    let third = spawn_worker(
        &session,
        SpawnSubAgentOptions {
            prompt: "initial".into(),
            name: Some(first.clone()),
            ..Default::default()
        },
    )
    .await;
    finished(&session, &third).await;
    assert_eq!(
        session
            .send_subagent_message(message(&first, "explicit ID priority", false))
            .await
            .unwrap()
            .unwrap()
            .agent_id,
        first
    );
    finished(&session, &first).await;
    assert!(session
        .send_subagent_message(message(
            &"unrelated-team-peer".into(),
            "outside registry",
            false
        ))
        .await
        .is_none());
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn ordinary_cold_user_cancellation_remains_sticky_until_trusted_recovery() {
    let gate = gate();
    let (session, store, _, dir) = persistent_fixture(vec![gate_turn()], vec![gate.clone()]).await;
    let id = spawn_worker(
        &session,
        SpawnSubAgentOptions {
            prompt: "original".into(),
            ..Default::default()
        },
    )
    .await;
    entered(&gate).await;
    assert!(session.background_task_registry().stop_by_user(&id));
    finished(&session, &id).await;
    session.shutdown_and_wait().await.unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let cold = mk_session_with_provider_memory_and_store(
        Arc::new(fake_provider()),
        unique_subagent_test_deps(recording_deps(
            requests.clone(),
            vec![text_turn("recovered")],
        )),
        default_config(),
        vec![gate],
        None,
        None,
        Some(store),
        None,
    );
    assert!(cold
        .send_subagent_message(message(&id, "automatic", false))
        .await
        .unwrap()
        .unwrap_err()
        .contains("cancelled by the user"));
    assert!(requests.lock().unwrap().is_empty());
    assert!(
        cold.send_subagent_message(message(&id, "human recovery", true))
            .await
            .unwrap()
            .unwrap()
            .resumed
    );
    finished(&cold, &id).await;
    assert_eq!(
        text_positions(&requests.lock().unwrap()[0], "human recovery").len(),
        1
    );
    cold.shutdown_and_wait().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn ordinary_cold_missing_authority_metadata_fails_closed_by_id_and_saved_name() {
    let (session, store, requests, dir) =
        persistent_fixture(vec![], vec![create_write_tool()]).await;
    let child = store
        .open_sub_agent(
            "s1",
            "incomplete-child",
            crate::store::session_store::SubAgentMeta {
                agent_type: Some("general-purpose".into()),
                model: "fake-model".into(),
                permission_mode: None,
                effort: None,
                created_at: 1,
            },
        )
        .await
        .unwrap();
    child
        .append_event(
            "ordinary_subagent_creation".into(),
            Some(AppendEventOptions {
                turn_id: None,
                payload: Some(exact_json!({"version":1,"name":"saved-incomplete-name"})),
            }),
        )
        .await;
    child.flush().await.unwrap();
    child.shutdown().await.unwrap();
    for target in ["incomplete-child", "saved-incomplete-name"] {
        assert!(session
            .send_subagent_message(message(&target.into(), "cannot broaden authority", true))
            .await
            .unwrap()
            .is_err());
    }
    assert!(
        requests.lock().unwrap().is_empty(),
        "rejected recovery must not infer"
    );
    session.shutdown_and_wait().await.unwrap();
    assert!(session.background_tasks.lock().unwrap().handles.is_empty());
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn ordinary_plan_is_one_shot_and_cannot_be_message_resumed() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let session = mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        unique_subagent_test_deps(recording_deps(
            requests.clone(),
            vec![text_turn("one shot")],
        )),
        default_config(),
        vec![],
        None,
        None,
        None,
        None,
        Some(vec![crate::engine::builtin_agents::BUILTIN_AGENTS
            .iter()
            .find(|definition| definition.name == "Plan")
            .unwrap()
            .clone()]),
    );
    let id = spawn_worker(
        &session,
        SpawnSubAgentOptions {
            prompt: "initial".into(),
            agent_type: Some("Plan".into()),
            ..Default::default()
        },
    )
    .await;
    finished(&session, &id).await;
    assert_eq!(
        session.background_task_registry().get(&id).unwrap().status,
        BackgroundTaskStatus::Completed
    );
    assert_eq!(requests.lock().unwrap().len(), 1);
    assert!(session
        .send_subagent_message(message(&id, "resume", true))
        .await
        .unwrap()
        .unwrap_err()
        .contains("one-shot"));
    session.shutdown_and_wait().await.unwrap();
}
