use crate::engine::agent::{Agent, AgentOptions, StartSessionOptions};
use crate::engine::deps::EngineDepsOverrides;
use crate::engine::fork::{build_fork_transcript, is_in_fork_child};
use crate::engine::rewind::{
    BoxError, RewindController, RewindOutcome, RewindScope, RewindSession, RollbackResult,
};
use crate::engine::session::{Input, TurnOptions};
use crate::fs::node_path::{project_runtime_path, runtime_path_from_native};
use crate::runtime_process::RuntimeProcessContext;
use crate::runtime_values::{
    runtime_codec, runtime_json, RuntimeJson, RuntimeObject, RuntimePath, RuntimeString,
};
use crate::schemas::content::{ContentBlock, OutputBlock, TextBlock, ToolUseBlock};
use crate::schemas::message::{
    AssistantMessage, CompactBoundaryMessage, Message, ToolResultMessage, Usage, UserMessage,
};
use crate::schemas::rollout::{RolloutItem, SessionMeta, TurnContextSnapshot, UsageLedgerItem};
use crate::schemas::subagent_lifecycle::{
    SubagentContext, SubagentKind, SubagentLifecycleRecord, SubagentStatus,
};
use crate::store::history_rewind::{count_user_turns, drop_last_n_user_turns};
use crate::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use crate::store::projection::{project_checkpoints, project_for_prompt};
use crate::store::session_store::{
    AppendEventOptions, CheckpointRecord, CompactionRecord, RollbackRecord, SessionStore,
    SubAgentEffort, SubAgentMeta, ThreadSummary,
};
use crate::testing::{ev, fake_provider};
use crate::tools::builtins::task::{create_task_tool, TaskToolOptions};
use crate::tools::builtins::tool_search::with_deferred_tools;
use crate::types::permission::ExternalPermissionMode;
use crate::types::provider::{CallModel, ModelRequest, ModelStopReason, ModelStreamEvent};
use async_trait::async_trait;
use serde_json::Value as LegacyConfigValue;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

const SCENARIO: &str = "session-persistence-resume-fork-rewind";

macro_rules! exact_json {
    (null) => {
        RuntimeJson::Null
    };
    ([ $($value:tt),* $(,)? ]) => {
        RuntimeJson::Array(vec![$(exact_json!($value)),*])
    };
    ({ $($key:literal : $value:tt),* $(,)? }) => {{
        let mut object = RuntimeObject::new();
        $(object.insert($key, exact_json!($value));)*
        RuntimeJson::Object(object)
    }};
    ($value:expr) => {
        runtime_codec::encode_value(&$value)
            .expect("exact session persistence trace JSON expression must encode")
    };
}

fn legacy_model_config(model: &str) -> LegacyConfigValue {
    LegacyConfigValue::Object(serde_json::Map::from_iter([
        (
            "model".to_owned(),
            LegacyConfigValue::String(model.to_owned()),
        ),
        ("thinking".to_owned(), LegacyConfigValue::Bool(false)),
    ]))
}

fn exact_tool_use_events(id: &str, name: &str, input: &RuntimeJson) -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::ToolUseStart {
            id: id.into(),
            name: name.into(),
            thought_signature: None,
            tool_kind: None,
            input_format: None,
        },
        ModelStreamEvent::ToolUseInputDelta {
            id: id.into(),
            partial_json: runtime_json::stringify(input),
        },
        ModelStreamEvent::ToolUseStop { id: id.into() },
    ]
}

static PERSISTENCE_CONFIG_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct PersistenceConfigEnvironment {
    #[cfg(test)]
    _config_env_guard: std::sync::MutexGuard<'static, ()>,
    previous: Option<std::ffi::OsString>,
}

impl PersistenceConfigEnvironment {
    fn install(directory: &Path) -> Self {
        #[cfg(test)]
        let config_env_guard = crate::test_support::CONFIG_ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let previous = std::env::var_os("KLAUS_CONFIG_DIR");
        crate::runtime_process::set_process_environment_var("KLAUS_CONFIG_DIR", directory);
        Self {
            #[cfg(test)]
            _config_env_guard: config_env_guard,
            previous,
        }
    }
}

impl Drop for PersistenceConfigEnvironment {
    fn drop(&mut self) {
        match self.previous.take() {
            Some(value) => {
                crate::runtime_process::set_process_environment_var("KLAUS_CONFIG_DIR", value)
            }
            None => crate::runtime_process::remove_process_environment_var("KLAUS_CONFIG_DIR"),
        }
    }
}

pub async fn session_persistence_trace(
    process_context: &RuntimeProcessContext,
) -> Result<RuntimeJson, Box<dyn std::error::Error + Send + Sync>> {
    let _serial = PERSISTENCE_CONFIG_LOCK.lock().await;
    let config = TempDirGuard::new("klaus-persistence-config");
    let _environment = PersistenceConfigEnvironment::install(config.path());
    verify_surfaced_internal_name_resume_wiring(process_context).await?;
    let dir = TempDirGuard::new("klaus-session-trace");
    let result = build_trace(dir.path()).await;
    let base = runtime_path_from_native(dir.path()).into_runtime_string();
    result.map(|trace| normalize_paths(trace, &base))
}

async fn verify_surfaced_internal_name_resume_wiring(
    process_context: &RuntimeProcessContext,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let dir = TempDirGuard::new("klaus-surfaced-tool-resume");
    verify_surfaced_internal_name_resume_wiring_in(process_context, dir.path()).await
}

async fn verify_surfaced_internal_name_resume_wiring_in(
    process_context: &RuntimeProcessContext,
    dir: &Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let runtime_dir = runtime_path_from_native(dir);
    let host_dir = host_path_string(&runtime_dir, "session persistence trace store directory")?;
    let store: Arc<dyn SessionStore> = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(host_dir.clone()),
        archive_dir: None,
    }));
    let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let request_sink = Arc::clone(&requests);
    let call_model: CallModel = Box::new(move |request, _signal| {
        let index = {
            let mut captured = request_sink.lock().unwrap();
            let index = captured.len();
            captured.push(request);
            index
        };
        let events = if index == 0 {
            let mut events = vec![ev::message_start(Some("gpt-5"))];
            events.extend(exact_tool_use_events(
                "surface-task",
                "tool_search",
                &exact_json!({ "query": "select:task" }),
            ));
            events.push(ev::stop(Some(ModelStopReason::ToolUse), None));
            events
        } else {
            vec![
                ev::message_start(Some("gpt-5")),
                ev::text(if index == 1 { "surfaced" } else { "resumed" }),
                ev::stop(None, None),
            ]
        };
        Box::pin(futures::stream::iter(events))
    });
    let tools = with_deferred_tools(vec![create_task_tool(TaskToolOptions::default())], Some(0));
    let agent = Agent::new(AgentOptions {
        process_context: process_context.clone(),
        provider: Arc::new(fake_provider()),
        config: Some(legacy_model_config("gpt-5")),
        tools: Some(tools),
        web_fetch_page: None,
        browser_runtime: None,
        capability_roots: Vec::new(),
        cwd: Some(host_dir),
        deps: EngineDepsOverrides {
            call_model: Some(call_model),
            estimate_tokens: Some(Arc::new(|_| 0)),
            ..EngineDepsOverrides::default()
        },
        compactor: None,
        approval_handler: None,
        approval_reviewer: None,
        elicit_handler: None,
        credential_store: None,
        hooks: Vec::new(),
        observer: Some(None),
        logger: None,
        pricing: None,
        agents: None,
        include_default_agents: None,
        skills: None,
        output_styles: None,
        workflows: None,
        mcp_catalog: None,
        plugin_catalog: None,
        lsp_servers: None,
        agent_memory: None,
        mcp_clients: None,
        mcp_server_configs: Vec::new(),
        mcp_tools: None,
        refresh_tools: None,
        system_prompt_provider: None,
        plugins: Vec::new(),
        plugin_load_report: None,
    });
    let initial = agent
        .open_session(
            Arc::clone(&store),
            StartSessionOptions {
                id: Some("surfaced-tool-resume".into()),
                cwd: Some(runtime_dir),
                ..StartSessionOptions::default()
            },
        )
        .await?;
    let initial_turn = initial
        .run(
            Input::Text("surface the task tool".into()),
            TurnOptions::default(),
        )
        .await;
    assert_eq!(
        initial_turn.stop_reason,
        crate::types::errors::TerminalReason::Completed
    );
    initial.dispose().await;

    let resumed = agent
        .resume_session(
            store,
            "surfaced-tool-resume",
            StartSessionOptions::default(),
        )
        .await?;
    let resumed_turn = resumed
        .run(
            Input::Text("continue after resume".into()),
            TurnOptions::default(),
        )
        .await;
    assert_eq!(
        resumed_turn.stop_reason,
        crate::types::errors::TerminalReason::Completed
    );
    resumed.dispose().await;

    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    let visible_tool_names = |request: &ModelRequest| {
        let mut names = request
            .tools
            .iter()
            .map(|tool| tool.name.clone())
            .collect::<Vec<_>>();
        names.sort();
        names
    };
    assert_eq!(
        visible_tool_names(&requests[0]),
        vec![
            RuntimeString::from("tool_search"),
            RuntimeString::from("update_plan")
        ]
    );
    assert_eq!(
        visible_tool_names(&requests[1]),
        vec![
            RuntimeString::from("spawn_agent"),
            RuntimeString::from("tool_search"),
            RuntimeString::from("update_plan")
        ]
    );
    // `task` is deferred, so the rendered `spawn_agent` can appear only when
    // the first session surfaced and persisted the stable internal name `task`.
    // Resume must restore that internal identity and re-render it; persisting or
    // restoring `spawn_agent` would leave deferred `task` filtered out here.
    assert_eq!(
        visible_tool_names(&requests[2]),
        vec![
            RuntimeString::from("spawn_agent"),
            RuntimeString::from("tool_search"),
            RuntimeString::from("update_plan")
        ]
    );
    assert!(!requests[2].tools.iter().any(|tool| tool.name == "task"));
    Ok(())
}

async fn build_trace(dir: &Path) -> Result<RuntimeJson, Box<dyn std::error::Error + Send + Sync>> {
    let runtime_dir = runtime_path_from_native(dir);
    let host_dir = host_path_string(&runtime_dir, "session persistence trace store directory")?;
    let store = JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(host_dir.clone()),
        archive_dir: None,
    });
    let meta = SessionMeta {
        id: "sess-trace".into(),
        created_at: 1_700_000_123,
        cwd: "/tmp/proj".into(),
        model: "model-a".into(),
        title: Some("Trace Session".into()),
        originator: Some("trace".into()),
        cli_version: Some("1.2.3".into()),
        source: Some("test".into()),
        role: None,
        parent_thread_id: Some("parent-1".into()),
        forked_from_id: Some("fork-source-1".into()),
        git: Some(crate::schemas::rollout::GitSnapshot {
            commit: Some("abc123".into()),
            branch: Some("main".into()),
            origin_url: Some("https://example.invalid/repo.git".into()),
        }),
        host_metadata: None,
    };

    let live = store.create(meta).await?;
    live.append_turn_context(ctx(
        "/tmp/proj",
        "model-a",
        ExternalPermissionMode::Default,
        "turn-1",
    ))
    .await;
    live.append_message(user("u1", "one")).await;
    live.append_usage(UsageLedgerItem {
        inference: None,
        request_id: "req-a1".into(),
        assistant_message_id: "a1".into(),
        model: "model-a".into(),
        usage: Usage {
            input_tokens: 100,
            output_tokens: 25,
            cache_read_input_tokens: 10,
            cache_creation_input_tokens: 5,
            total_tokens: None,
        },
        cost_usd: 0.42,
    })
    .await;
    live.append_subagent_lifecycle(SubagentLifecycleRecord {
        agent_id: "child-lifecycle-1".into(),
        kind: SubagentKind::Foreground,
        status: SubagentStatus::Running,
        context: SubagentContext {
            session_id: "sess-trace".into(),
            parent_agent_id: None,
            parent_thread_id: Some("sess-trace".into()),
            agent_type: Some("research".into()),
            cwd: "/tmp/proj".into(),
            description: "collect evidence".into(),
            depth: 1,
            resume_from: None,
            spawn_turn_id: None,
        },
        started_at_ms: 10,
        updated_at_ms: 20,
        completed_at_ms: None,
        last_result: None,
        last_error: None,
        cancellation_requested: false,
        model: None,
        model_source: None,
    })
    .await;
    live.append_message(assistant_text("a1", "model-a", "one done"))
        .await;
    live.append_checkpoint(CheckpointRecord {
        hash: Some("h1".into()),
    })
    .await;
    live.append_turn_context(ctx(
        "/tmp/proj",
        "model-b",
        ExternalPermissionMode::Plan,
        "turn-2",
    ))
    .await;
    live.append_message(user("u2", "two")).await;
    live.append_message(assistant_text("a2", "model-b", "two done"))
        .await;
    live.append_checkpoint(CheckpointRecord {
        hash: Some("h2".into()),
    })
    .await;
    live.append_compaction(CompactionRecord {
        approval_parent_history: None,
        summary: "summary 1".into(),
        replaced_range: (0, 2),
        source_message_ids: None,
        replacement_history: None,
    })
    .await;
    live.append_message(user("u3", "three")).await;
    live.append_message(assistant_text("a3", "model-b", "three done"))
        .await;
    live.append_checkpoint(CheckpointRecord {
        hash: Some("h3".into()),
    })
    .await;
    live.append_rollback(RollbackRecord { num_turns: 1 }).await;
    live.shutdown().await?;

    let reopened = store.reopen("sess-trace").await?;
    reopened
        .append_turn_context(ctx(
            "/tmp/proj/sub",
            "model-c",
            ExternalPermissionMode::AcceptEdits,
            "turn-4",
        ))
        .await;
    reopened
        .append_event(
            "tools_surfaced".into(),
            Some(AppendEventOptions {
                turn_id: Some("turn-4".into()),
                payload: Some(exact_json!({ "names": ["read", "write", "read", 42] })),
            }),
        )
        .await;
    reopened.append_message(user("u4", "four")).await;
    reopened
        .append_message(assistant_text("a4", "model-c", "four done"))
        .await;
    reopened
        .append_checkpoint(CheckpointRecord { hash: None })
        .await;
    reopened.shutdown().await?;

    let agent_id_host = format!("fork/child{}", char::from_u32(0x1f600).unwrap());
    let agent_id = RuntimeString::from(agent_id_host.clone());
    let sub = store
        .open_sub_agent(
            "sess-trace",
            &agent_id_host,
            SubAgentMeta {
                agent_type: Some("fork".into()),
                model: "model-child".into(),
                permission_mode: Some("bubble".into()),
                effort: Some(SubAgentEffort::Number(0.7)),
                created_at: 1_700_000_456,
            },
        )
        .await?;
    sub.append_event(
        "tools_surfaced".into(),
        Some(AppendEventOptions {
            turn_id: None,
            payload: Some(exact_json!({ "names": ["grep", "read", "grep", false] })),
        }),
    )
    .await;
    sub.append_message(user("child-u", "child prompt")).await;
    sub.append_message(assistant_text("child-a", "model-child", "child answer"))
        .await;
    sub.append_compaction(CompactionRecord {
        approval_parent_history: None,
        summary: "child summary".into(),
        replaced_range: (0, 1),
        source_message_ids: None,
        replacement_history: Some(vec![assistant_text(
            "child-s",
            "model-child",
            "child snapshot",
        )]),
    })
    .await;
    sub.shutdown().await?;

    let file = dir.join("1700000123-sess-trace.jsonl");
    use std::io::Write as _;
    let mut handle = std::fs::OpenOptions::new().append(true).open(&file)?;
    handle.write_all(br#"{"kind":"message","message":{"type":"user""#)?;
    drop(handle);

    let fresh = JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(host_dir),
        archive_dir: None,
    });
    let resumed = fresh.resume("sess-trace").await?;
    // `tool_search` surfaces stable internal names. The rollout/resume boundary
    // must preserve those exact identities rather than provider-rendered names.
    let resumed_surfaced_event = resumed.raw_items.iter().find_map(|item| match item {
        RolloutItem::Event(event) if event.event == "tools_surfaced" => event.payload.as_ref(),
        _ => None,
    });
    let expected_surfaced_event = exact_json!({ "names": ["read", "write", "read", 42] });
    assert_eq!(resumed_surfaced_event, Some(&expected_surfaced_event));
    let sub_transcript = fresh.load_sub_agent("sess-trace", &agent_id_host).await?;
    let listed_sub_agents = fresh.list_sub_agents("sess-trace").await?;
    let page = fresh
        .list(crate::store::session_store::ListOptions {
            cwd: Some("/tmp/proj".into()),
            ..Default::default()
        })
        .await?;
    let cwd_miss = fresh
        .list(crate::store::session_store::ListOptions {
            cwd: Some("/tmp/elsewhere".into()),
            ..Default::default()
        })
        .await?;

    Ok(exact_json!({
        "scenario": SCENARIO,
        "store": {
            "meta": (&resumed.meta),
            "lastTurnContext": (&resumed.last_turn_context),
            "messages": (resumed.messages.iter().map(message_summary).collect::<Vec<_>>()),
            "projectedMessages": (project_for_prompt(&resumed.raw_items).iter().map(message_summary).collect::<Vec<_>>()),
            "checkpoints": (project_checkpoints(&resumed.raw_items)),
            "rawItems": (resumed.raw_items.iter().map(raw_item_summary).collect::<Vec<_>>()),
            "list": {
                "cwdMatch": (page.items.iter().map(thread_summary).collect::<Vec<_>>()),
                "cwdMissCount": (cwd_miss.items.len()),
            },
            "subAgent": {
                "requestedId": agent_id,
                "listedIds": listed_sub_agents,
                "meta": (sub_transcript.meta.as_ref().map(subagent_meta_summary)),
                "messages": (sub_transcript.messages.iter().map(message_summary).collect::<Vec<_>>()),
                "surfacedTools": (sub_transcript.surfaced_tools.unwrap_or_default()),
            },
        },
        "projection": (projection_trace()),
        "fork": (fork_trace()),
        "rewind": (rewind_trace().await),
    }))
}

fn projection_trace() -> RuntimeJson {
    let history = vec![
        compact_boundary("summary-prefix", "kept summary"),
        user("p-u1", "first"),
        assistant_text("p-a1", "model-p", "first done"),
        meta_user("p-meta", "retry with schema"),
        assistant_text("p-a2", "model-p", "retry done"),
        user("p-u2", "second"),
        assistant_text("p-a3", "model-p", "second done"),
    ];
    exact_json!({
        "countUserTurns": (count_user_turns(&history)),
        "dropOne": (drop_last_n_user_turns(&history, 1.0).iter().map(message_summary).collect::<Vec<_>>()),
        "dropMany": (drop_last_n_user_turns(&history, 99.0).iter().map(message_summary).collect::<Vec<_>>()),
        "dropFractional": (drop_last_n_user_turns(&history, 1.9).iter().map(message_summary).collect::<Vec<_>>()),
    })
}

fn fork_trace() -> RuntimeJson {
    let conversation = vec![
        user("f-u1", "parent prompt"),
        Message::Assistant(AssistantMessage {
            uuid: "f-a1".into(),
            model: "model-f".into(),
            content: vec![
                text_block("working"),
                tool_use_block("done-call", "read", exact_json!({ "file": "a.txt" })),
                tool_use_block("dangling-a", "task", exact_json!({ "prompt": "subtask a" })),
                tool_use_block("dangling-b", "bash", exact_json!({ "command": "echo hi" })),
            ],
            usage: None,
        }),
        tool_result("f-r1", "done-call", "already done"),
    ];
    let n = Arc::new(Mutex::new(0usize));
    let transcript = build_fork_transcript(&conversation, "collect evidence", {
        let n = Arc::clone(&n);
        move || {
            let mut guard = n.lock().unwrap();
            *guard += 1;
            format!("fork-id-{}", *guard)
        }
    });
    let placeholder_tool_use_ids: Vec<RuntimeString> = transcript
        .iter()
        .filter_map(|m| match m {
            Message::ToolResult(t) if t.uuid.starts_with("fork-id-") => Some(t.tool_use_id.clone()),
            _ => None,
        })
        .collect();
    let directive = transcript.last();
    let directive_uuid = directive.map(uuid_of).unwrap_or_default();
    let directive_text = directive
        .and_then(|m| match m {
            Message::User(u) => first_text(&u.content),
            _ => None,
        })
        .unwrap_or_default();

    exact_json!({
        "plainDetected": (is_in_fork_child(&conversation)),
        "forkDetected": (is_in_fork_child(&transcript)),
        "transcript": (transcript.iter().map(message_summary).collect::<Vec<_>>()),
        "placeholderToolUseIds": placeholder_tool_use_ids,
        "directive": {
            "uuid": directive_uuid,
            "startsWithTag": (directive_text.starts_with("<fork-worker>")),
            "endsWithDirective": (directive_text.ends_with("Directive: collect evidence")),
            "text": directive_text,
        },
    })
}

async fn rewind_trace() -> RuntimeJson {
    let session = Arc::new(FakeRewindSession::new(
        rewind_history(),
        vec![Some("h2".into()), Some("h3".into())],
        None,
    ));
    let mut controller = RewindController::new(Arc::clone(&session) as Arc<dyn RewindSession>);
    controller.before_turn().await;
    let both = controller.rewind(2.0, RewindScope::Both).await;

    let invalid_session = Arc::new(FakeRewindSession::new(rewind_history(), vec![], None));
    let invalid = RewindController::new(invalid_session as Arc<dyn RewindSession>)
        .rewind(0.0, RewindScope::Both)
        .await;

    let guarded_session = Arc::new(FakeRewindSession::new(
        rewind_history(),
        vec![Some("h1".into()), Some("h2".into()), Some("h3".into())],
        Some("cannot rollback while a turn is in progress".into()),
    ));
    let guarded = RewindController::new(Arc::clone(&guarded_session) as Arc<dyn RewindSession>)
        .rewind(1.0, RewindScope::Both)
        .await;

    exact_json!({
        "seededCheckpointPadding": (session.initial_ledger.clone()),
        "snapshotLabels": (session.snapshot_labels()),
        "recordedCheckpoints": (session.recorded_checkpoints()),
        "rollbackRequests": (session.rollback_requests()),
        "restoredHashes": (session.restored_hashes()),
        "both": (rewind_outcome_summary(&both)),
        "historyAfterBoth": (session.history().iter().map(message_summary).collect::<Vec<_>>()),
        "invalid": (rewind_outcome_summary(&invalid)),
        "guardedBoth": {
            "outcome": (rewind_outcome_summary(&guarded)),
            "restoredHashes": (guarded_session.restored_hashes()),
        },
    })
}

fn rewind_outcome_summary(outcome: &RewindOutcome) -> RuntimeJson {
    let mut object = RuntimeObject::new();
    object.insert("scope", exact_json!(outcome.scope));
    object.insert("droppedTurns", exact_json!(outcome.dropped_turns));
    object.insert("remainingTurns", exact_json!(outcome.remaining_turns));
    object.insert("filesRestored", exact_json!(outcome.files_restored));
    if let Some(error) = &outcome.error {
        object.insert("error", RuntimeJson::String(error.clone()));
    }
    RuntimeJson::Object(object)
}

#[derive(Debug)]
struct FakeRewindState {
    history: Vec<Message>,
    snapshot_labels: Vec<String>,
    recorded_checkpoints: Vec<Option<String>>,
    rollback_requests: Vec<u64>,
    restored_hashes: Vec<String>,
}

#[derive(Debug)]
struct FakeRewindSession {
    state: Mutex<FakeRewindState>,
    checkpoints: Vec<Option<String>>,
    rollback_error: Option<String>,
    initial_ledger: Vec<Option<String>>,
}

impl FakeRewindSession {
    fn new(
        history: Vec<Message>,
        checkpoints: Vec<Option<String>>,
        rollback_error: Option<String>,
    ) -> Self {
        let initial_ledger = seeded_ledger(&history, &checkpoints);
        Self {
            state: Mutex::new(FakeRewindState {
                history,
                snapshot_labels: Vec::new(),
                recorded_checkpoints: Vec::new(),
                rollback_requests: Vec::new(),
                restored_hashes: Vec::new(),
            }),
            checkpoints,
            rollback_error,
            initial_ledger,
        }
    }

    fn snapshot_labels(&self) -> Vec<String> {
        self.state.lock().unwrap().snapshot_labels.clone()
    }

    fn recorded_checkpoints(&self) -> Vec<Option<String>> {
        self.state.lock().unwrap().recorded_checkpoints.clone()
    }

    fn rollback_requests(&self) -> Vec<u64> {
        self.state.lock().unwrap().rollback_requests.clone()
    }

    fn restored_hashes(&self) -> Vec<String> {
        self.state.lock().unwrap().restored_hashes.clone()
    }
}

#[async_trait]
impl RewindSession for FakeRewindSession {
    fn history(&self) -> Vec<Message> {
        self.state.lock().unwrap().history.clone()
    }

    fn resumed_checkpoints(&self) -> Vec<Option<String>> {
        self.checkpoints.clone()
    }

    async fn snapshot(&self, label: &str) -> Result<Option<String>, BoxError> {
        self.state
            .lock()
            .unwrap()
            .snapshot_labels
            .push(label.to_string());
        Ok(Some("h4".into()))
    }

    async fn record_checkpoint(&self, hash: Option<String>) {
        self.state.lock().unwrap().recorded_checkpoints.push(hash);
    }

    async fn rollback(&self, num_turns: f64) -> Result<RollbackResult, BoxError> {
        let mut state = self.state.lock().unwrap();
        state.rollback_requests.push(num_turns as u64);
        if let Some(err) = &self.rollback_error {
            return Err(std::io::Error::other(err.clone()).into());
        }
        let before = count_user_turns(&state.history);
        state.history = drop_last_n_user_turns(&state.history, num_turns);
        let remaining = count_user_turns(&state.history);
        Ok(RollbackResult {
            dropped_turns: before.saturating_sub(remaining),
            remaining_turns: remaining,
            messages: state.history.len(),
        })
    }

    async fn restore_checkpoint(&self, hash: &str) -> Result<(), BoxError> {
        self.state
            .lock()
            .unwrap()
            .restored_hashes
            .push(hash.to_string());
        Ok(())
    }

    // A preview changes nothing, so there is nothing for this trace to record.
    async fn checkpoint_changed_files(&self, _hash: &str) -> Result<Vec<String>, BoxError> {
        Ok(Vec::new())
    }
}

fn seeded_ledger(history: &[Message], persisted: &[Option<String>]) -> Vec<Option<String>> {
    let turns = count_user_turns(history);
    let pad = turns.saturating_sub(persisted.len());
    let start = persisted.len().saturating_sub(turns);
    let mut out = Vec::new();
    for _ in 0..pad {
        out.push(None);
    }
    out.extend_from_slice(&persisted[start..]);
    out
}

fn ctx(
    cwd: &str,
    model: &str,
    permission_mode: ExternalPermissionMode,
    turn_id: &str,
) -> TurnContextSnapshot {
    TurnContextSnapshot {
        cwd: cwd.into(),
        model: model.into(),
        permission_mode,
        turn_id: Some(turn_id.into()),
        started_at_ms: None,
        approval_policy: Some(crate::types::permission::ApprovalPolicy::Never),
        sandbox_mode: Some(crate::types::permission::SandboxMode::DangerFullAccess),
        approvals_reviewer: Some(crate::types::permission::ApprovalsReviewer::User),
        network_allowed: Some(true),
        writable_roots: Some(vec![cwd.into(), "/tmp/shared".into()]),
    }
}

fn user(uuid: &str, text: &str) -> Message {
    Message::User(UserMessage {
        question_answer: None,
        uuid: uuid.into(),
        content: vec![text_block(text)],
        is_meta: None,
    })
}

fn meta_user(uuid: &str, text: &str) -> Message {
    Message::User(UserMessage {
        question_answer: None,
        uuid: uuid.into(),
        content: vec![text_block(text)],
        is_meta: Some(true),
    })
}

fn assistant_text(uuid: &str, model: &str, text: &str) -> Message {
    Message::Assistant(AssistantMessage {
        uuid: uuid.into(),
        model: model.into(),
        content: vec![text_block(text)],
        usage: None,
    })
}

fn tool_result(uuid: &str, tool_use_id: &str, text: &str) -> Message {
    Message::ToolResult(ToolResultMessage {
        uuid: uuid.into(),
        tool_use_id: tool_use_id.into(),
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

fn compact_boundary(uuid: &str, summary: &str) -> Message {
    Message::CompactBoundary(CompactBoundaryMessage {
        native_compaction: None,
        uuid: uuid.into(),
        summary: summary.into(),
    })
}

fn rewind_history() -> Vec<Message> {
    vec![
        compact_boundary("r-summary", "resume summary"),
        user("r-u1", "turn one"),
        assistant_text("r-a1", "model-r", "one done"),
        user("r-u2", "turn two"),
        assistant_text("r-a2", "model-r", "two done"),
        user("r-u3", "turn three"),
        assistant_text("r-a3", "model-r", "three done"),
    ]
}

fn text_block(text: &str) -> ContentBlock {
    ContentBlock::Text(TextBlock {
        text: text.into(),
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    })
}

fn tool_use_block(id: &str, name: &str, input: RuntimeJson) -> ContentBlock {
    ContentBlock::ToolUse(ToolUseBlock {
        id: id.into(),
        name: name.into(),
        input,
        tool_kind: None,
        thought_signature: None,
    })
}

fn output_text(blocks: &[OutputBlock]) -> Option<RuntimeString> {
    blocks.iter().find_map(|block| match block {
        OutputBlock::Text(t) => Some(t.text.clone()),
        _ => None,
    })
}

fn first_text(blocks: &[ContentBlock]) -> Option<RuntimeString> {
    blocks.iter().find_map(|block| match block {
        ContentBlock::Text(t) => Some(t.text.clone()),
        _ => None,
    })
}

fn message_summary(message: &Message) -> RuntimeJson {
    match message {
        Message::User(message) => exact_json!({
            "type": "user",
            "uuid": (&message.uuid),
            "text": (first_text(&message.content)),
            "isMeta": (message.is_meta == Some(true)),
        }),
        Message::Assistant(message) => {
            let tool_use_ids: Vec<RuntimeString> = message
                .content
                .iter()
                .filter_map(|block| match block {
                    ContentBlock::ToolUse(t) => Some(t.id.clone()),
                    _ => None,
                })
                .collect();
            exact_json!({
                "type": "assistant",
                "uuid": (&message.uuid),
                "model": (&message.model),
                "text": (first_text(&message.content)),
                "toolUseIds": tool_use_ids,
            })
        }
        Message::ToolResult(message) => exact_json!({
            "type": "tool_result",
            "uuid": (&message.uuid),
            "toolUseId": (&message.tool_use_id),
            "text": (output_text(&message.content)),
            "isError": (message.is_error),
        }),
        Message::System(message) => exact_json!({
            "type": "system",
            "uuid": (&message.uuid),
            "subtype": (message.subtype),
            "text": (&message.text),
        }),
        Message::CompactBoundary(message) => exact_json!({
            "type": "compact_boundary",
            "uuid": (&message.uuid),
            "summary": (&message.summary),
        }),
    }
}

fn raw_item_summary(item: &RolloutItem) -> RuntimeJson {
    match item {
        RolloutItem::SessionMeta {
            schema_version,
            meta,
        } => exact_json!({
            "kind": "session_meta",
            "schemaVersion": schema_version,
            "id": (&meta.id),
        }),
        RolloutItem::TurnContext { ctx } => exact_json!({
            "kind": "turn_context",
            "cwd": (&ctx.cwd),
            "model": (&ctx.model),
            "approvalPolicy": (ctx.approval_policy),
            "sandboxMode": (ctx.sandbox_mode),
            "approvalsReviewer": (ctx.approvals_reviewer),
            "networkAllowed": (ctx.network_allowed),
            "writableRoots": (&ctx.writable_roots),
            "turnId": (&ctx.turn_id),
        }),
        RolloutItem::Message { message } => exact_json!({
            "kind": "message",
            "message": (message_summary(message)),
        }),
        RolloutItem::Compacted(item) => exact_json!({
            "kind": "compacted",
            "summary": (&item.summary),
            "replacedRange": [(item.replaced_range.0), (item.replaced_range.1)],
            "replacementHistory": (item.replacement_history.as_ref().map(|history| {
                history.iter().map(message_summary).collect::<Vec<_>>()
            })),
        }),
        RolloutItem::Event(item) => exact_json!({
            "kind": "event",
            "event": (&item.event),
            "turnId": (&item.turn_id),
            "payload": (&item.payload),
        }),
        RolloutItem::Rollback(item) => exact_json!({
            "kind": "rollback",
            "numTurns": (item.num_turns),
        }),
        RolloutItem::Checkpoint(item) => exact_json!({
            "kind": "checkpoint",
            "hash": (&item.hash),
        }),
        RolloutItem::UsageLedger(item) => exact_json!({
            "kind": "usage_ledger",
            "requestId": (&item.request_id),
            "assistantMessageId": (&item.assistant_message_id),
            "model": (&item.model),
            "usage": (&item.usage),
            "costUsd": (item.cost_usd),
        }),
        RolloutItem::SubagentLifecycle(item) => exact_json!({
            "kind": "subagent_lifecycle",
            "record": (&item.record),
        }),
        RolloutItem::BackgroundProcessLifecycle(item) => exact_json!({
            "kind": "background_process_lifecycle",
            "record": (&item.record),
        }),
    }
}

fn thread_summary(summary: &ThreadSummary) -> RuntimeJson {
    exact_json!({
        "id": (&summary.id),
        "createdAt": (summary.created_at),
        "cwd": (&summary.cwd),
        "model": (&summary.model),
    })
}

fn subagent_meta_summary(meta: &SubAgentMeta) -> RuntimeJson {
    let mut object = RuntimeObject::new();
    if let Some(agent_type) = &meta.agent_type {
        object.insert(
            "agentType",
            runtime_codec::encode_value(agent_type)
                .expect("sub-agent type must have an exact runtime encoding"),
        );
    }
    object.insert(
        "model",
        runtime_codec::encode_value(&meta.model)
            .expect("sub-agent model must have an exact runtime encoding"),
    );
    if let Some(permission_mode) = &meta.permission_mode {
        object.insert(
            "permissionMode",
            runtime_codec::encode_value(permission_mode)
                .expect("sub-agent permission mode must have an exact runtime encoding"),
        );
    }
    if let Some(effort) = &meta.effort {
        let effort = match effort {
            SubAgentEffort::Text(value) => RuntimeJson::String(value.into()),
            SubAgentEffort::Number(value) => runtime_codec::encode_value(value)
                .expect("sub-agent effort must have an exact runtime encoding"),
        };
        object.insert("effort", effort);
    }
    object.insert(
        "createdAt",
        runtime_codec::encode_value(&meta.created_at)
            .expect("sub-agent creation time must have an exact runtime encoding"),
    );
    RuntimeJson::Object(object)
}

fn uuid_of(message: &Message) -> RuntimeString {
    match message {
        Message::User(x) => x.uuid.clone(),
        Message::Assistant(x) => x.uuid.clone(),
        Message::ToolResult(x) => x.uuid.clone(),
        Message::System(x) => x.uuid.clone(),
        Message::CompactBoundary(x) => x.uuid.clone(),
    }
}

fn normalize_paths(mut value: RuntimeJson, base: &RuntimeString) -> RuntimeJson {
    match &mut value {
        RuntimeJson::String(text) => {
            *text = runtime_replace_all(text, base, &RuntimeString::from("<TMP>"));
        }
        RuntimeJson::Array(items) => {
            for item in items {
                *item = normalize_paths(std::mem::take(item), base);
            }
        }
        RuntimeJson::Object(map) => {
            for item in map.values_mut() {
                *item = normalize_paths(std::mem::take(item), base);
            }
        }
        RuntimeJson::Null | RuntimeJson::Bool(_) | RuntimeJson::Number(_) => {}
    }
    value
}

fn runtime_replace_all(
    value: &RuntimeString,
    from: &RuntimeString,
    to: &RuntimeString,
) -> RuntimeString {
    let value = value.as_utf16();
    let from = from.as_utf16();
    if from.is_empty() {
        return RuntimeString::from_utf16(value.to_vec());
    }
    let mut output = Vec::with_capacity(value.len());
    let mut index = 0;
    while index < value.len() {
        if value[index..].starts_with(from) {
            output.extend_from_slice(to.as_utf16());
            index += from.len();
        } else {
            output.push(value[index]);
            index += 1;
        }
    }
    RuntimeString::from_utf16(output)
}

fn unique_temp_dir(tag: &str) -> PathBuf {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("{tag}-{pid}-{nanos}"))
}

struct TempDirGuard {
    path: PathBuf,
}

impl TempDirGuard {
    fn new(tag: &str) -> Self {
        Self {
            path: unique_temp_dir(tag),
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn host_path_string(
    path: &RuntimePath,
    argument_label: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    project_runtime_path(path, argument_label)
        .map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("runtime path projection failed: {:?}", error.message()),
            )
        })?
        .into_os_string()
        .into_string()
        .map_err(|path| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("{argument_label} cannot cross the legacy UTF-8 path seam: {path:?}"),
            )
            .into()
        })
}

#[cfg(test)]
mod provider_behavior_tests {
    use super::*;
    use crate::provider::behavior::{
        render_tool_specs_for_behavior, resolve_provider_behavior_profile,
    };
    use crate::provider::openai::to_responses_input_with_profile;
    use crate::store::history_rewind::take_last_n_user_turns;
    use crate::tools::pool::to_tool_spec;
    use crate::types::events::{ToolLifecyclePhase, ToolLifecycleRecord};
    use crate::types::json::{Json, JsonSchema};
    use crate::types::provider::{ProviderToolCallKind, ToolSpec, ToolSpecKind};
    use crate::types::tool::{
        Tool, ToolCatalogEntry, ToolContext, ToolExposure, ToolProgress, ToolResult,
    };

    struct VisibilityTool {
        name: &'static str,
        exposure: ToolExposure,
        input_schema: JsonSchema,
    }

    impl VisibilityTool {
        fn new(name: &'static str, exposure: ToolExposure) -> Self {
            Self {
                name,
                exposure,
                input_schema: serde_json::Map::from_iter([(
                    "type".into(),
                    LegacyConfigValue::String("object".into()),
                )]),
            }
        }
    }

    #[async_trait]
    impl Tool for VisibilityTool {
        fn static_name(&self) -> &str {
            self.name
        }

        fn static_description(&self) -> &str {
            "provider visibility fixture tool"
        }

        fn static_input_schema(&self) -> &JsonSchema {
            &self.input_schema
        }

        fn exposure(&self) -> ToolExposure {
            self.exposure
        }

        fn is_enabled(&self) -> bool {
            true
        }

        fn is_concurrency_safe(&self, _input: &Json) -> bool {
            true
        }

        fn is_read_only(&self, _input: &Json) -> bool {
            true
        }

        async fn call(
            &self,
            _input: Json,
            _ctx: &ToolContext,
            _on_progress: Option<ToolProgress>,
        ) -> Result<ToolResult, crate::types::errors::ToolError> {
            panic!("visibility fixture tools must not be invoked")
        }
    }

    async fn capture_model_visible_tools() -> Vec<RuntimeString> {
        let dir = TempDirGuard::new("klaus-direct-model-only-request");
        let runtime_dir = runtime_path_from_native(dir.path());
        let host_dir = host_path_string(
            &runtime_dir,
            "direct-model-only trace session store directory",
        )
        .expect("fixture session directory must cross the legacy store seam");
        let store: Arc<dyn SessionStore> =
            Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
                dir: Some(host_dir.clone()),
                archive_dir: None,
            }));
        let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
        let request_sink = Arc::clone(&requests);
        let call_model: CallModel = Box::new(move |request, _signal| {
            request_sink.lock().unwrap().push(request);
            Box::pin(futures::stream::iter(vec![
                ev::message_start(Some("gpt-5")),
                ev::text("visibility captured"),
                ev::stop(None, None),
            ]))
        });
        let tools: Vec<Arc<dyn Tool>> = vec![
            Arc::new(VisibilityTool::new("read", ToolExposure::Direct)),
            Arc::new(VisibilityTool::new("bridge", ToolExposure::DirectModelOnly)),
            Arc::new(VisibilityTool::new("later", ToolExposure::Deferred)),
        ];
        let agent = Agent::new(AgentOptions {
            process_context: crate::runtime_process::test_runtime_process_context(),
            provider: Arc::new(fake_provider()),
            config: Some(legacy_model_config("gpt-5")),
            tools: Some(tools),
            web_fetch_page: None,
            browser_runtime: None,
            capability_roots: Vec::new(),
            cwd: Some(host_dir),
            deps: EngineDepsOverrides {
                call_model: Some(call_model),
                estimate_tokens: Some(Arc::new(|_| 0)),
                ..EngineDepsOverrides::default()
            },
            compactor: None,
            approval_handler: None,
            approval_reviewer: None,
            elicit_handler: None,
            credential_store: None,
            hooks: Vec::new(),
            observer: Some(None),
            logger: None,
            pricing: None,
            agents: None,
            include_default_agents: None,
            skills: None,
            output_styles: None,
            workflows: None,
            mcp_catalog: None,
            plugin_catalog: None,
            lsp_servers: None,
            agent_memory: None,
            mcp_clients: None,
            mcp_server_configs: Vec::new(),
            mcp_tools: None,
            refresh_tools: None,
            system_prompt_provider: None,
            plugins: Vec::new(),
            plugin_load_report: None,
        });
        let session = agent
            .open_session(
                store,
                StartSessionOptions {
                    id: Some("direct-model-only-request".into()),
                    cwd: Some(runtime_dir),
                    ..StartSessionOptions::default()
                },
            )
            .await
            .unwrap();
        let turn = session
            .run(
                Input::Text("capture model-visible tools".into()),
                TurnOptions::default(),
            )
            .await;
        assert_eq!(
            turn.stop_reason,
            crate::types::errors::TerminalReason::Completed
        );
        session.dispose().await;

        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let mut names = requests[0]
            .tools
            .iter()
            .map(|tool| tool.name.clone())
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    fn assistant_provider_call(
        id: &str,
        name: &str,
        input: RuntimeJson,
        tool_kind: ProviderToolCallKind,
    ) -> Message {
        Message::Assistant(AssistantMessage {
            uuid: format!("assistant-{id}").into(),
            content: vec![ContentBlock::ToolUse(ToolUseBlock {
                id: id.into(),
                name: name.into(),
                input,
                thought_signature: None,
                tool_kind: Some(tool_kind),
            })],
            model: "gpt-5".into(),
            usage: None,
        })
    }

    fn provider_result(
        id: &str,
        text: &str,
        tool_kind: ProviderToolCallKind,
        native_tool_output: Option<RuntimeJson>,
    ) -> Message {
        Message::ToolResult(ToolResultMessage {
            uuid: format!("result-{id}").into(),
            tool_use_id: id.into(),
            content: vec![OutputBlock::Text(TextBlock {
                text: text.into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_error: false,
            output: None,
            extra_user_blocks: None,
            tool_kind: Some(tool_kind),
            native_tool_output,
        })
    }

    fn assistant_function(id: &str, name: &str, input: RuntimeJson) -> Message {
        Message::Assistant(AssistantMessage {
            uuid: format!("assistant-{id}").into(),
            content: vec![ContentBlock::ToolUse(ToolUseBlock {
                id: id.into(),
                name: name.into(),
                input,
                thought_signature: None,
                tool_kind: None,
            })],
            model: "gpt-5".into(),
            usage: None,
        })
    }

    fn function_result(id: &str, text: &str) -> Message {
        Message::ToolResult(ToolResultMessage {
            uuid: format!("result-{id}").into(),
            tool_use_id: id.into(),
            content: vec![OutputBlock::Text(TextBlock {
                text: text.into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_error: false,
            output: None,
            extra_user_blocks: None,
            tool_kind: None,
            native_tool_output: None,
        })
    }

    fn assistant_custom(id: &str, name: &str, input: &str) -> Message {
        assistant_provider_call(
            id,
            name,
            RuntimeJson::String(input.into()),
            ProviderToolCallKind::Custom,
        )
    }

    fn custom_result(id: &str, text: &str) -> Message {
        provider_result(id, text, ProviderToolCallKind::Custom, None)
    }

    fn assistant_tool_search(id: &str, input: RuntimeJson) -> Message {
        assistant_provider_call(id, "tool_search", input, ProviderToolCallKind::ToolSearch)
    }

    fn tool_search_result(id: &str, output: RuntimeJson) -> Message {
        provider_result(
            id,
            "search complete",
            ProviderToolCallKind::ToolSearch,
            Some(output),
        )
    }

    fn native_search_output_fixture() -> RuntimeJson {
        exact_json!({"tools":[
          {"type":"function","name":"read","description":"Read","inputSchema":{"type":"object"}},
          {"type":"namespace","name":"calendar","tools":[
            {"type":"function","name":"events","description":"Events","parameters":{"type":"object"}}
          ]},
          {"type":"custom","kind":"custom","name":"ignored","description":"Ignored","inputSchema":{"type":"object"}},
          {"type":"namespace","name":"empty","tools":[{"type":"custom","name":"ignored"}]}
        ]})
    }

    fn function_spec(name: &str) -> ToolSpec {
        ToolSpec {
            name: name.into(),
            description: "conflicting current function".into(),
            input_schema: exact_json!({ "type": "object" }),
            kind: ToolSpecKind::Function,
            format: None,
        }
    }

    fn fixture_meta() -> SessionMeta {
        SessionMeta {
            id: "provider-behavior-resume".into(),
            created_at: 1_700_000_999,
            cwd: "/tmp/provider-behavior".into(),
            model: "gpt-5".into(),
            title: None,
            originator: Some("provider-behavior-fixture".into()),
            cli_version: None,
            source: Some("test".into()),
            role: None,
            parent_thread_id: None,
            forked_from_id: None,
            git: None,
            host_metadata: None,
        }
    }

    fn call_observation(message: &Message) -> RuntimeJson {
        let Message::Assistant(assistant) = message else {
            panic!("expected assistant call")
        };
        let call = assistant
            .content
            .iter()
            .find_map(|block| match block {
                ContentBlock::ToolUse(call) => Some(call),
                _ => None,
            })
            .expect("assistant call block");
        let wire = runtime_codec::encode_value(message).unwrap();
        exact_json!({
            "callId": (&call.id),
            "kind": (call.tool_kind.unwrap_or_default()),
            "toolKind": (wire.pointer("/content/0/toolKind").cloned()),
            "inputFormat": (wire.pointer("/content/0/inputFormat").cloned()),
        })
    }

    fn result_observation(message: &Message) -> RuntimeJson {
        let Message::ToolResult(result) = message else {
            panic!("expected tool result")
        };
        let wire = runtime_codec::encode_value(message).unwrap();
        exact_json!({
            "toolUseId": (&result.tool_use_id),
            "toolKind": (wire.get("toolKind").cloned()),
            "nativeToolOutput": (wire.get("nativeToolOutput").cloned()),
        })
    }

    fn message_ids(messages: &[Message]) -> Vec<RuntimeString> {
        messages.iter().map(uuid_of).collect()
    }

    fn find_call<'a>(messages: &'a [Message], id: &str) -> &'a Message {
        messages
            .iter()
            .find(|message| match message {
                Message::Assistant(assistant) => assistant
                    .content
                    .iter()
                    .any(|block| matches!(block, ContentBlock::ToolUse(call) if call.id == id)),
                _ => false,
            })
            .unwrap_or_else(|| panic!("missing persisted call {id}"))
    }

    fn find_result<'a>(messages: &'a [Message], id: &str) -> &'a Message {
        messages
            .iter()
            .find(|message| {
                matches!(message, Message::ToolResult(result) if result.tool_use_id == id)
            })
            .unwrap_or_else(|| panic!("missing persisted result {id}"))
    }

    fn raw_call_input(message: &Message) -> RuntimeJson {
        let Message::Assistant(assistant) = message else {
            panic!("expected assistant call")
        };
        match &assistant.content[0] {
            ContentBlock::ToolUse(call) => call.input.clone(),
            _ => panic!("expected tool-use block"),
        }
    }

    #[tokio::test]
    async fn provider_behavior_resume_compaction_fixture() {
        let native_output = native_search_output_fixture();
        let original = vec![
            assistant_function("f1", "read", exact_json!({"path":"notes.md"})),
            function_result("f1", "old function result"),
            assistant_custom("c1", "apply_patch", "*** Begin Patch"),
            custom_result("c1", "done"),
            assistant_tool_search("s1", exact_json!({"query":"calendar"})),
            tool_search_result("s1", native_output.clone()),
        ];

        let encoded = original
            .iter()
            .map(|message| runtime_codec::encode_text(message).unwrap())
            .collect::<Vec<_>>();
        let decoded = encoded
            .iter()
            .map(|line| runtime_codec::decode_text::<Message>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(decoded, original);

        let dir = TempDirGuard::new("klaus-provider-behavior-persistence");
        let runtime_dir = runtime_path_from_native(dir.path());
        let store = JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(
                host_path_string(
                    &runtime_dir,
                    "provider behavior trace persistence directory",
                )
                .expect("fixture session directory must cross the legacy store seam"),
            ),
            archive_dir: None,
        });
        let live = store.create(fixture_meta()).await.unwrap();
        for message in &original {
            live.append_message(message.clone()).await;
        }
        let compacted = vec![
            compact_boundary("compact-provider", "provider state"),
            original[2].clone(),
            original[3].clone(),
            original[4].clone(),
            original[5].clone(),
            assistant_function(
                "identity-call",
                "spawn_agent",
                exact_json!({"task_name":"audit"}),
            ),
            function_result("identity-call", "identity complete"),
        ];
        live.append_compaction(CompactionRecord {
            approval_parent_history: None,
            summary: "provider state".into(),
            replaced_range: (0, original.len() as u64),
            source_message_ids: None,
            replacement_history: Some(compacted.clone()),
        })
        .await;
        live.append_event(
            "tools_surfaced".into(),
            Some(AppendEventOptions {
                turn_id: Some("turn-surface".into()),
                payload: Some(exact_json!({"names":["task"]})),
            }),
        )
        .await;
        live.append_event(
            "tool_lifecycle".into(),
            Some(AppendEventOptions {
                turn_id: Some("turn-identity".into()),
                payload: Some(
                    runtime_codec::encode_value(&ToolLifecycleRecord {
                        turn_id: "turn-identity".into(),
                        tool_use_id: "identity-call".into(),
                        tool_name: "task".into(),
                        phase: ToolLifecyclePhase::Requested,
                        source: None,
                        arguments: Some(
                            exact_json!({"model": "low", "description":"audit","prompt":"audit"}),
                        ),
                        approval_kind: None,
                        event: None,
                        is_error: None,
                        error: None,
                    })
                    .unwrap(),
                ),
            }),
        )
        .await;
        live.shutdown().await.unwrap();

        let resumed = store.resume("provider-behavior-resume").await.unwrap();
        assert_eq!(resumed.messages, compacted);

        let parent_history = vec![
            user("u1", "one"),
            assistant_text("a1", "gpt-5", "one done"),
            user("u2", "two"),
            assistant_text("a2", "gpt-5", "two done"),
            tool_result("r2", "turn-two-call", "two result"),
            meta_user("meta2", "continue the same turn"),
            user("u3", "three"),
            assistant_text("a3", "gpt-5", "three done"),
        ];
        let recent = take_last_n_user_turns(&parent_history, 2);
        let child = store
            .open_sub_agent(
                "provider-behavior-resume",
                "child-recent",
                SubAgentMeta {
                    agent_type: None,
                    model: "gpt-5".into(),
                    permission_mode: Some("default".into()),
                    effort: None,
                    created_at: 1_700_001_000,
                },
            )
            .await
            .unwrap();
        child
            .append_message(user("child-directive", "Directive: inspect recent context"))
            .await;
        child.shutdown().await.unwrap();
        let child_transcript = store
            .load_sub_agent("provider-behavior-resume", "child-recent")
            .await
            .unwrap();

        let profile = resolve_provider_behavior_profile(None, &RuntimeString::from("gpt-5"));
        let replay_after_tool_removal =
            to_responses_input_with_profile(&decoded[2..], &[], &profile);
        let conflicting_current_tools =
            vec![function_spec("apply_patch"), function_spec("tool_search")];
        let replay_after_compaction = to_responses_input_with_profile(
            &resumed.messages,
            &conflicting_current_tools,
            &profile,
        );

        let surfaced_internal_names = resumed
            .raw_items
            .iter()
            .find_map(|item| match item {
                RolloutItem::Event(event) if event.event == "tools_surfaced" => event
                    .payload
                    .as_ref()
                    .and_then(|payload| payload.get("names"))
                    .cloned(),
                _ => None,
            })
            .unwrap();
        let internal_engine_event_name = resumed
            .raw_items
            .iter()
            .find_map(|item| match item {
                RolloutItem::Event(event) if event.event == "tool_lifecycle" => event
                    .payload
                    .as_ref()
                    .and_then(|payload| payload.get("toolName"))
                    .cloned(),
                _ => None,
            })
            .unwrap();
        let visible_history_name = resumed
            .messages
            .iter()
            .find_map(|message| match message {
                Message::Assistant(assistant) => {
                    assistant.content.iter().find_map(|block| match block {
                        ContentBlock::ToolUse(call) if call.id == "identity-call" => {
                            Some(call.name.clone())
                        }
                        _ => None,
                    })
                }
                _ => None,
            })
            .unwrap();

        let task = create_task_tool(TaskToolOptions::default());
        let rendered_task =
            render_tool_specs_for_behavior(&[to_tool_spec(task.as_ref())], &profile)
                .into_iter()
                .next()
                .unwrap();
        let catalog = ToolCatalogEntry {
            name: "task".into(),
            description: rendered_task.description.clone(),
            search_hint: None,
            exposure: ToolExposure::Deferred,
            spec: Some(rendered_task),
        };
        let catalog_spec = runtime_codec::encode_value(catalog.spec.as_ref().unwrap()).unwrap();
        let direct_model_only = ToolExposure::DirectModelOnly;
        let model_request_tools = capture_model_visible_tools().await;
        let model_request_has_bridge = model_request_tools.iter().any(|name| name == "bridge");
        let model_request_hides_later = model_request_tools.iter().all(|name| name != "later");

        let trace = exact_json!({
            "historyKinds": [
                (call_observation(&decoded[0])),
                (call_observation(find_call(&resumed.messages, "c1"))),
                (call_observation(find_call(&resumed.messages, "s1"))),
            ],
            "nativeToolOutputs": [
                (result_observation(&decoded[1])),
                (result_observation(find_result(&resumed.messages, "c1"))),
                (result_observation(find_result(&resumed.messages, "s1"))),
            ],
            "customRawInput": (raw_call_input(find_call(&resumed.messages, "c1"))),
            "replayAfterToolRemoval": replay_after_tool_removal,
            "replayAfterCompaction": replay_after_compaction,
            "surfacedInternalNames": {
                "persisted": surfaced_internal_names,
                "catalog": {
                    "name": (catalog.name),
                    "exposure": (catalog.exposure),
                    "spec": {
                        "name": (catalog_spec.get("name").cloned()),
                        "kind": (catalog_spec.get("kind").cloned()),
                        "format": (catalog_spec.get("format").cloned()),
                        "forkTurns": (catalog_spec.pointer("/inputSchema/properties/fork_turns").cloned()),
                        "reasoningEffort": (catalog_spec.pointer("/inputSchema/properties/reasoning_effort").cloned()),
                        "legacyForkContext": (catalog_spec.pointer("/inputSchema/properties/fork_context").cloned()),
                    }
                }
            },
            "directModelOnlyVisibility": {
                "exposure": (direct_model_only),
                "requestTools": model_request_tools,
                "model": model_request_has_bridge,
                "deferredHidden": model_request_hides_later,
                "codeMode": (direct_model_only.allows_code_mode_bridge()),
                "discoverable": (direct_model_only == ToolExposure::Deferred),
            },
            "recentTurnIds": (message_ids(&recent)),
            "childSidechainIds": (message_ids(&child_transcript.messages)),
            "visibleHistoryName": visible_history_name,
            "internalEngineEventName": internal_engine_event_name,
        });

        assert_eq!(
            trace,
            exact_json!({
                "historyKinds": [
                    {"callId":"f1","kind":"function","toolKind":null,"inputFormat":null},
                    {"callId":"c1","kind":"custom","toolKind":"custom","inputFormat":null},
                    {"callId":"s1","kind":"tool_search","toolKind":"tool_search","inputFormat":null}
                ],
                "nativeToolOutputs": [
                    {"toolUseId":"f1","toolKind":null,"nativeToolOutput":null},
                    {"toolUseId":"c1","toolKind":"custom","nativeToolOutput":null},
                    {"toolUseId":"s1","toolKind":"tool_search","nativeToolOutput":native_output}
                ],
                "customRawInput": "*** Begin Patch",
                "replayAfterToolRemoval": [
                    {"type":"custom_tool_call","call_id":"c1","name":"apply_patch","input":"*** Begin Patch"},
                    {"type":"custom_tool_call_output","call_id":"c1","output":"done"},
                    {"type":"tool_search_call","call_id":"s1","execution":"client","arguments":{"query":"calendar"}},
                    {"type":"tool_search_output","call_id":"s1","status":"completed","execution":"client","tools":[
                        {"type":"function","name":"read","description":"Read","strict":false,"defer_loading":true,"parameters":{"type":"object"}},
                        {"type":"namespace","name":"calendar","description":"Tools in the calendar namespace.","tools":[
                            {"type":"function","name":"events","description":"Events","strict":false,"defer_loading":true,"parameters":{"type":"object"}}
                        ]}
                    ]}
                ],
                "replayAfterCompaction": [
                    {"role":"user","content":"# Earlier Conversation Summary\n\nprovider state"},
                    {"type":"custom_tool_call","call_id":"c1","name":"apply_patch","input":"*** Begin Patch"},
                    {"type":"custom_tool_call_output","call_id":"c1","output":"done"},
                    {"type":"tool_search_call","call_id":"s1","execution":"client","arguments":{"query":"calendar"}},
                    {"type":"tool_search_output","call_id":"s1","status":"completed","execution":"client","tools":[
                        {"type":"function","name":"read","description":"Read","strict":false,"defer_loading":true,"parameters":{"type":"object"}},
                        {"type":"namespace","name":"calendar","description":"Tools in the calendar namespace.","tools":[
                            {"type":"function","name":"events","description":"Events","strict":false,"defer_loading":true,"parameters":{"type":"object"}}
                        ]}
                    ]},
                    {"type":"function_call","call_id":"identity-call","name":"spawn_agent","arguments":"{\"task_name\":\"audit\"}"},
                    {"type":"function_call_output","call_id":"identity-call","output":"identity complete"}
                ],
                "surfacedInternalNames": {
                    "persisted": ["task"],
                    "catalog": {
                        "name": "task",
                        "exposure": "deferred",
                        "spec": {
                            "name": "spawn_agent",
                            "kind": null,
                            "format": null,
                            "forkTurns": {
                                "type": "string",
                                "description": "Conversation history to inherit: all (default) or none. Positive integer strings also inherit the full conversation."
                            },
                            "reasoningEffort": {
                                "type": "string",
                                "description": "Reasoning effort override for the child. Omit to inherit the parent selection."
                            },
                            "legacyForkContext": null
                        }
                    }
                },
                "directModelOnlyVisibility": {
                    "exposure": "direct_model_only",
                    "requestTools": ["bridge", "read", "tool_search", "update_plan"],
                    "model": true,
                    "deferredHidden": true,
                    "codeMode": false,
                    "discoverable": false
                },
                "recentTurnIds": ["u2","a2","r2","meta2","u3","a3"],
                "childSidechainIds": ["child-directive"],
                "visibleHistoryName": "spawn_agent",
                "internalEngineEventName": "task"
            })
        );
    }
}
