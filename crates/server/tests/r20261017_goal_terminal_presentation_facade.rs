//! Defect: terminal presentation was inferred from successor admission alone.
//! Root cause: completed and failed carriers lacked a captured native decision.
//! Work item: 20261017 terminal presentation and actual continuation authority.

use super::*;
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeString};
use klaus_agent_protocol::types::{ProtocolEvent, ProtocolGoalContinuation};
use std::sync::atomic::AtomicBool;
use std::sync::Mutex;

async fn until(
    client: &mut TestClient,
    frames: &mut Vec<Value>,
    predicate: impl Fn(&Value) -> bool,
) -> Value {
    if let Some(frame) = frames.iter().find(|frame| predicate(frame)) {
        return frame.clone();
    }
    tokio::time::timeout(REPLY_TIMEOUT, async {
        loop {
            let frame = client.next_message().await;
            assert!(frames.len() < 256, "bounded terminal presentation fixture");
            let found = predicate(&frame);
            frames.push(frame.clone());
            if found {
                return frame;
            }
        }
    })
    .await
    .expect("the correlated actual terminal must settle")
}

async fn named(
    client: &mut TestClient,
    frames: &mut Vec<Value>,
    id: u64,
    method: &str,
    params: Value,
) -> Value {
    client
        .send(json!({"id":id,"method":method,"params":params}))
        .await;
    let reply = until(client, frames, |frame| {
        frame["id"] == id && frame.get("method").is_none()
    })
    .await;
    assert!(reply.get("error").is_none(), "{method}: {reply}");
    reply["result"].clone()
}

async fn terminal_case(plan: bool, disabled: bool, active: bool, failed: bool) {
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let _config = ConfigGuard::new(root.path());
    let calls = Arc::new(AtomicUsize::new(0));
    let observed_calls = calls.clone();
    let agent = make_agent_with_call_model_and_config(
        Box::new(move |_, _| {
            assert_eq!(
                observed_calls.fetch_add(1, Ordering::SeqCst),
                0,
                "no successor is admitted by these presentation-only cases"
            );
            if failed {
                panic!("bounded ordinary provider factory failure");
            }
            Box::pin(futures::stream::iter(vec![
                ev::message_start(None),
                ev::text("The requested inspection is complete."),
                ev::stop(Some(ModelStopReason::EndTurn), None),
            ]))
        }),
        Some(
            json!({"goal":true,"prewarm":false,"autoMemory":false,"autoDream":false,"maxRetries":0}),
        ),
    );
    let store: Arc<dyn SessionStore> = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.path().join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let factory: CreateSession = {
        let agent = agent.clone();
        let store = store.clone();
        let cwd = root.path().to_string_lossy().into_owned();
        Arc::new(move |ctx: SessionContext| {
            let agent = agent.clone();
            let store = store.clone();
            let cwd = cwd.clone();
            Box::pin(async move {
                agent.open_session(store, StartSessionOptions {
                    id:ctx.init.thread_id, cwd:Some(cwd.clone().into()), user_config_dir:Some(cwd.into()),
                    approval_handler:Some(ctx.approval_handler), elicit_handler:Some(ctx.elicit_handler),
                    mcp_elicitation_presenter:Some(ctx.mcp_elicitation_presenter),
                    config:Some(json!({"goal":true,"prewarm":false,"autoMemory":false,"autoDream":false,"maxRetries":0})),
                    ..Default::default()
                }).await.map_err(|error| Box::new(error) as BoxError)
            })
        })
    };
    let lifecycle = crate::lifecycle::protocol_lifecycle(agent, Some(store));
    let mut client = TestClient::start_full(
        factory,
        Some(lifecycle),
        Some(root.path().join("permissions.json")),
        Some(root.path().join("config.json")),
    );
    let mut frames = Vec::new();
    named(
        &mut client,
        &mut frames,
        1,
        "initialize",
        json!({
            "clientInfo":{"name":"terminal-presentation-test"},"capabilities":{"productEvents":true}
        }),
    )
    .await;
    client.send(json!({"method":"initialized"})).await;
    let started = named(
        &mut client,
        &mut frames,
        2,
        "session/start",
        json!({
            "init":{"threadId":"terminal-presentation-thread"}
        }),
    )
    .await;
    let session = started["sessionId"].clone();
    // Install an Active durable goal only after Plan has closed automatic
    // admission. Disabling tools later must not erase its presentation state.
    named(
        &mut client,
        &mut frames,
        3,
        "session/setCollaborationMode",
        json!({"sessionId":session,"mode":"plan"}),
    )
    .await;
    let created = named(&mut client, &mut frames, 4, "session/setGoal", json!({
        "sessionId":session,"objective":"Keep the saved objective independent of notification presentation",
        "status":if active {"active"} else {"paused"},"origin":"user","expectedGoalId":null
    })).await;
    let generation = created["threadGoal"]["goalId"].clone();
    assert!(generation.is_string(), "{created}");
    if disabled {
        named(
            &mut client,
            &mut frames,
            5,
            "session/updateConfig",
            json!({"sessionId":session,"update":{"goal":false}}),
        )
        .await;
    }
    if !plan {
        named(
            &mut client,
            &mut frames,
            6,
            "session/setCollaborationMode",
            json!({"sessionId":session,"mode":"default"}),
        )
        .await;
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let prior = "presentation-prior";
    let admitted = named(
        &mut client,
        &mut frames,
        7,
        "turn/start",
        json!({
            "sessionId":session,"submissionId":prior,"input":"Inspect the current evidence once."
        }),
    )
    .await;
    assert_eq!(admitted["turnId"], prior);
    let result = until(&mut client, &mut frames, |frame| {
        frame["params"]["event"]["submissionId"] == prior
            && frame["params"]["event"].get("result").is_some()
    })
    .await;
    let expected = if active && !failed {
        "suppress"
    } else {
        "inherit"
    };
    assert_eq!(
        result["params"]["event"]["terminalNotificationDisposition"],
        expected
    );
    assert_eq!(
        result["params"]["event"]["result"]["stopReason"],
        if failed { "error" } else { "completed" }
    );
    let typed = frames
        .iter()
        .find(|frame| {
            if failed {
                frame["method"] == "turn/failed" && frame["params"]["submissionId"] == prior
            } else {
                frame["method"] == "turn/completed" && frame["params"]["turnId"] == prior
            }
        })
        .expect("the actual terminal has a named typed carrier");
    assert_eq!(typed["params"]["terminalNotificationDisposition"], expected);
    if failed {
        assert!(
            typed["params"]["detail"]["error"]["message"]
                .as_str()
                .is_some_and(|message| !message.is_empty()),
            "{typed}"
        );
    }
    let raw_terminal = frames
        .iter()
        .find(|frame| {
            frame["params"]["event"]["submissionId"] == prior
                && matches!(
                    frame["params"]["event"]["event"]["type"].as_str(),
                    Some("turn.completed" | "turn.failed")
                )
        })
        .expect("the raw actual terminal is preserved");
    assert_eq!(
        raw_terminal["params"]["event"]["terminalNotificationDisposition"],
        expected
    );
    let saved = named(
        &mut client,
        &mut frames,
        8,
        "session/goal",
        json!({"sessionId":session}),
    )
    .await;
    assert_eq!(saved["threadGoal"]["goalId"], generation);
    assert_eq!(
        saved["threadGoal"]["status"],
        if active { "active" } else { "paused" }
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(
        !frames
            .iter()
            .any(|frame| frame["method"] == "goal.continuation"),
        "presentation suppression must not invent pending or admitted successors"
    );
    assert!(!frames
        .iter()
        .any(|frame| frame["method"] == "turn/started"
            && frame["params"]["goalContinuation"] == true));
    for started in frames
        .iter()
        .filter(|frame| frame["method"] == "turn/started")
    {
        assert!(
            started["params"]
                .get("terminalNotificationDisposition")
                .is_none(),
            "terminal presentation must not alter actual-start provenance"
        );
    }
    client.close().await;
}

#[tokio::test]
async fn completed_plan_with_active_saved_goal_suppresses_without_a_candidate() {
    terminal_case(true, false, true, false).await;
}
#[tokio::test]
async fn completed_default_with_disabled_tools_and_active_saved_goal_suppresses() {
    terminal_case(false, true, true, false).await;
}
#[tokio::test]
async fn paused_goal_without_candidate_preserves_ordinary_completed_presentation() {
    terminal_case(false, false, false, false).await;
}
#[tokio::test]
async fn failed_plan_with_active_saved_goal_inherits_and_keeps_the_typed_failure() {
    terminal_case(true, false, true, true).await;
}
#[tokio::test]
async fn failed_default_with_disabled_tools_and_active_saved_goal_inherits() {
    terminal_case(false, true, true, true).await;
}
#[tokio::test]
async fn completed_plan_with_paused_saved_goal_inherits_without_a_candidate() {
    terminal_case(true, false, false, false).await;
}
#[tokio::test]
async fn failed_plan_with_paused_saved_goal_inherits_without_a_candidate() {
    terminal_case(true, false, false, true).await;
}

#[derive(Clone, Copy)]
enum TerminalFaultKind {
    RunSettlement,
    PresentationRead,
}

struct TerminalFault {
    kind: TerminalFaultKind,
    database: std::path::PathBuf,
    backup: std::path::PathBuf,
    clock_calls: AtomicUsize,
    installed: AtomicBool,
}

impl TerminalFault {
    fn new(root: &std::path::Path, kind: TerminalFaultKind) -> Arc<Self> {
        Arc::new(Self {
            kind,
            database: root.join("goals.sqlite3"),
            backup: root.join("goals-terminal-fixture-backup.sqlite3"),
            clock_calls: AtomicUsize::new(0),
            installed: AtomicBool::new(false),
        })
    }

    fn observer(
        self: &Arc<Self>,
    ) -> Arc<Mutex<klaus_agent_core::observability::otel::OtelObserver>> {
        use klaus_agent_core::observability::otel::{
            create_otel_observer, CreateOtelObserverOptions,
        };
        let fault = self.clone();
        Arc::new(Mutex::new(create_otel_observer(
            CreateOtelObserverOptions {
                now: Some(Box::new(move || {
                    let index = fault.clock_calls.fetch_add(1, Ordering::SeqCst);
                    // One nonempty provider response has exactly these timer reads:
                    // TurnStarted, first TextDelta, then the actual TurnCompleted.
                    if index == 2 {
                        assert!(
                            fault.database.is_file(),
                            "only obstruct the fixture database"
                        );
                        match fault.kind {
                            TerminalFaultKind::RunSettlement => {
                                // Provider usage was already charged. Fail the
                                // existing run marker's actual settlement write.
                                std::thread::spawn(|| {
                                tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
                                    tokio::time::timeout(REPLY_TIMEOUT,
                                        klaus_agent_core::goal::store::testing_execute(
                                            "CREATE TEMP TABLE terminal_fixture_run_guard (present INTEGER CHECK (present = 1)); INSERT INTO terminal_fixture_run_guard SELECT COUNT(*) FROM goal_runs WHERE thread_id = 'terminal-storage-fault-thread'; DROP TABLE terminal_fixture_run_guard; CREATE TRIGGER terminal_fixture_settlement_failure BEFORE DELETE ON goal_runs WHEN OLD.thread_id = 'terminal-storage-fault-thread' BEGIN SELECT RAISE(ABORT, 'forced goal run settlement write failure'); END;"
                                        )).await.expect("bounded SQL fault installation").unwrap();
                                });
                            }).join().expect("SQL fault installer completes");
                            }
                            TerminalFaultKind::PresentationRead => {
                                assert!(!fault.backup.exists());
                                std::fs::rename(&fault.database, &fault.backup).unwrap();
                                std::fs::create_dir(&fault.database).unwrap();
                            }
                        }
                        fault.installed.store(true, Ordering::SeqCst);
                    }
                    index as f64
                })),
                ..Default::default()
            },
        )))
    }

    fn restore_file(&self) {
        if self.backup.exists() {
            std::fs::remove_dir(&self.database).unwrap();
            std::fs::rename(&self.backup, &self.database).unwrap();
        }
    }
}

impl Drop for TerminalFault {
    fn drop(&mut self) {
        self.restore_file();
    }
}

fn observed_agent(
    call_model: klaus_agent_core::types::provider::CallModel,
    observer: Arc<Mutex<klaus_agent_core::observability::otel::OtelObserver>>,
) -> Arc<Agent> {
    Arc::new(Agent::new(AgentOptions {
        process_context: crate::test_process_context(),
        provider: Arc::new(fake_provider()),
        config: Some(
            json!({"goal":true,"prewarm":false,"autoMemory":false,"autoDream":false,"maxRetries":0}),
        ),
        tools: None,
        web_fetch_page: None,
        browser_runtime: None,
        capability_roots: Vec::new(),
        cwd: Some(".".to_owned()),
        deps: EngineDepsOverrides {
            call_model: Some(call_model),
            estimate_tokens: None,
            compactor: None,
            uuid: None,
            now: None,
        },
        compactor: None,
        approval_handler: None,
        approval_reviewer: None,
        elicit_handler: None,
        credential_store: None,
        hooks: Vec::new(),
        observer: Some(Some(observer)),
        logger: None,
        pricing: None,
        agents: None,
        include_default_agents: Some(false),
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
    }))
}

async fn terminal_storage_fault(kind: TerminalFaultKind) {
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let _config = ConfigGuard::new(root.path());
    let fault = TerminalFault::new(root.path(), kind);
    let calls = Arc::new(AtomicUsize::new(0));
    let provider_calls = calls.clone();
    let agent = observed_agent(
        Box::new(move |_, _| {
            assert_eq!(
                provider_calls.fetch_add(1, Ordering::SeqCst),
                0,
                "the fault fixture has one real provider response"
            );
            Box::pin(futures::stream::iter(vec![
                ev::message_start(None),
                ev::text("Measured progress before terminal storage failure."),
                ev::stop(
                    Some(ModelStopReason::EndTurn),
                    Some(klaus_agent_core::schemas::message::Usage {
                        input_tokens: 8,
                        output_tokens: 2,
                        cache_read_input_tokens: 0,
                        cache_creation_input_tokens: 0,
                        total_tokens: None,
                    }),
                ),
            ]))
        }),
        fault.observer(),
    );
    let store: Arc<dyn SessionStore> = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.path().join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let live = Arc::new(std::sync::OnceLock::<Arc<Session>>::new());
    let factory: CreateSession = {
        let agent = agent.clone();
        let store = store.clone();
        let live = live.clone();
        let cwd = root.path().to_string_lossy().into_owned();
        Arc::new(move |ctx: SessionContext| {
            let agent = agent.clone();
            let store = store.clone();
            let live = live.clone();
            let cwd = cwd.clone();
            Box::pin(async move {
                let session = agent
                    .open_session(
                        store,
                        StartSessionOptions {
                            id: ctx.init.thread_id,
                            cwd: Some(cwd.clone().into()),
                            user_config_dir: Some(cwd.into()),
                            approval_handler: Some(ctx.approval_handler),
                            elicit_handler: Some(ctx.elicit_handler),
                            mcp_elicitation_presenter: Some(ctx.mcp_elicitation_presenter),
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(|error| Box::new(error) as BoxError)?;
                assert!(live.set(session.clone()).is_ok());
                Ok(session)
            })
        })
    };
    let lifecycle = crate::lifecycle::protocol_lifecycle(agent, Some(store));
    let mut client = TestClient::start_full(
        factory,
        Some(lifecycle),
        Some(root.path().join("permissions.json")),
        Some(root.path().join("config.json")),
    );
    let mut frames = Vec::new();
    named(&mut client,&mut frames,1,"initialize",json!({"clientInfo":{"name":"terminal-storage-fault-test"},"capabilities":{"productEvents":true}})).await;
    client.send(json!({"method":"initialized"})).await;
    let session = named(
        &mut client,
        &mut frames,
        2,
        "session/start",
        json!({"init":{"threadId":"terminal-storage-fault-thread"}}),
    )
    .await["sessionId"]
        .clone();
    if matches!(kind, TerminalFaultKind::PresentationRead) {
        named(
            &mut client,
            &mut frames,
            3,
            "session/setCollaborationMode",
            json!({"sessionId":session,"mode":"plan"}),
        )
        .await;
    }
    let mut goal_request = json!({
        "sessionId":session,"objective":"Preserve the actual terminal when storage fails","origin":"user","status":"active","expectedGoalId":null
    });
    if matches!(kind, TerminalFaultKind::RunSettlement) {
        goal_request["submit"] = true.into();
        goal_request["clientRequestId"] = "terminal-storage-fault-create".into();
    }
    let created = named(&mut client, &mut frames, 4, "session/setGoal", goal_request).await;
    assert!(created["threadGoal"]["goalId"].is_string());
    if matches!(kind, TerminalFaultKind::PresentationRead) {
        named(
            &mut client,
            &mut frames,
            5,
            "session/updateConfig",
            json!({"sessionId":session,"update":{"goal":false}}),
        )
        .await;
        named(
            &mut client,
            &mut frames,
            6,
            "session/setCollaborationMode",
            json!({"sessionId":session,"mode":"default"}),
        )
        .await;
        named(&mut client,&mut frames,7,"turn/start",json!({"sessionId":session,"submissionId":"lookup-failure-turn","input":"Inspect the evidence once."})).await;
    }
    let started = until(&mut client, &mut frames, |frame| {
        frame["method"] == "turn/started"
    })
    .await;
    let prior = started["params"]["submissionId"]
        .as_str()
        .unwrap()
        .to_owned();
    let completed = until(&mut client, &mut frames, |frame| {
        frame["params"]["event"]["submissionId"] == prior
            && frame["params"]["event"]["event"]["type"] == "turn.completed"
    })
    .await;
    assert!(fault.installed.load(Ordering::SeqCst));
    assert_eq!(
        fault.clock_calls.load(Ordering::SeqCst),
        3,
        "the fault is at the actual completed timer boundary"
    );
    assert_eq!(
        completed["params"]["event"]["event"]["stopReason"],
        "completed"
    );
    assert_eq!(
        completed["params"]["event"]["terminalNotificationDisposition"],
        "suppress"
    );
    match kind {
        TerminalFaultKind::RunSettlement => {
            let failed = until(&mut client, &mut frames, |frame| {
                frame["method"] == "turn/failed" && frame["params"]["submissionId"] == prior
            })
            .await;
            assert_eq!(
                failed["params"]["terminalNotificationDisposition"],
                "inherit"
            );
            assert!(
                failed["params"]["detail"]["error"]["message"]
                    .as_str()
                    .unwrap()
                    .contains("goal turn settlement failed")
                    && failed["params"]["detail"]["error"]["message"]
                        .as_str()
                        .unwrap()
                        .contains("forced goal run settlement write failure"),
                "{failed}"
            );
            let result = until(&mut client, &mut frames, |frame| {
                frame["params"]["event"]["submissionId"] == prior
                    && frame["params"]["event"].get("result").is_some()
            })
            .await;
            assert_eq!(
                result["params"]["event"]["terminalNotificationDisposition"],
                "inherit"
            );
            assert_eq!(result["params"]["event"]["result"]["stopReason"], "error");
            let completed_position = frames.iter().position(|frame| frame == &completed).unwrap();
            let failed_position = frames.iter().position(|frame| frame == &failed).unwrap();
            let result_position = frames.iter().position(|frame| frame == &result).unwrap();
            assert!(completed_position < failed_position && failed_position < result_position);
            assert_eq!(
                frames
                    .iter()
                    .filter(|frame| frame["method"] == "turn/failed"
                        && frame["params"]["submissionId"] == prior)
                    .count(),
                1
            );
            let persisted =
                klaus_agent_core::goal::store::get_goal("terminal-storage-fault-thread")
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(
                persisted.tokens_used, 10,
                "accepted provider usage commits before the separate run settlement failure"
            );
            assert!(
                !frames.iter().any(|frame| {
                    frame["method"] == "goal.continuation"
                        && frame["params"]["priorSubmissionId"] == prior
                }),
                "a failed settlement cannot reserve another continuation"
            );
            let runtime = live.get().unwrap().goal_observation_runtime().unwrap();
            let cleanup_error = runtime.wait_for_idle_cleanup().await.unwrap_err();
            assert!(cleanup_error.to_string().contains("forced goal run settlement write failure"),
                "the actual failed settlement must still guard automatic admission: {cleanup_error}");
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            runtime.suppress_continuation();
            klaus_agent_core::goal::store::testing_execute(
                "DROP TRIGGER terminal_fixture_settlement_failure;",
            )
            .await
            .unwrap();
        }
        TerminalFaultKind::PresentationRead => {
            let warning = frames.iter().find(|frame|frame["params"]["event"]["event"]["type"]=="notification"
                && frame["params"]["event"]["event"]["message"]=="Goal state could not be read for terminal notification presentation; ordinary completion was suppressed.")
                .expect("the unavailable presentation lookup emits its fixed bounded diagnostic");
            assert_eq!(warning["params"]["event"]["event"]["level"], "warn");
            // The original later eligibility read is deliberately unchanged.
            // Its real failure may report an op error, but cannot erase the
            // actual core completion that was already preserved above.
            until(&mut client, &mut frames, |frame| {
                frame["params"]["event"]["submissionId"] == prior
                    && frame["params"]["event"].get("error").is_some()
            })
            .await;
            assert!(
                !frames.iter().any(|frame| frame["method"] == "turn/failed"
                    && frame["params"]["submissionId"] == prior),
                "presentation lookup must not fabricate a failed turn"
            );
            fault.restore_file();
            let saved = named(
                &mut client,
                &mut frames,
                8,
                "session/goal",
                json!({"sessionId":session}),
            )
            .await;
            assert_eq!(
                saved["threadGoal"]["status"], "active",
                "presentation failure must not invent Goal state"
            );
        }
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    client.close().await;
}

#[tokio::test]
async fn actual_goal_run_settlement_failure_suppresses_superseded_success_and_preserves_failure_aliases(
) {
    terminal_storage_fault(TerminalFaultKind::RunSettlement).await;
}

#[tokio::test]
async fn unavailable_presentation_lookup_preserves_actual_completed_terminal_without_fabricating_failure(
) {
    terminal_storage_fault(TerminalFaultKind::PresentationRead).await;
}

#[test]
fn continuation_presentation_codec_and_schema_require_the_exact_resolved_union() {
    let valid = [
        json!({"pending":true}),
        json!({"pending":false,"continued":true,"priorCompletionNotificationDisposition":"suppress"}),
        json!({"pending":false,"continued":false,"priorCompletionNotificationDisposition":"inherit"}),
        json!({"pending":false,"continued":false,"priorCompletionNotificationDisposition":"suppress"}),
    ];
    let invalid = [
        json!({"pending":false,"continued":false}),
        json!({"pending":true,"priorCompletionNotificationDisposition":"suppress"}),
        json!({"pending":false,"continued":true,"priorCompletionNotificationDisposition":"inherit"}),
        json!({"pending":false,"continued":false,"priorCompletionNotificationDisposition":"notify"}),
        json!({"pending":false,"continued":false,"priorCompletionNotificationDisposition":null}),
        json!({"pending":true,"priorCompletionNotificationDisposition":null}),
        json!({"pending":false,"continued":false,"priorCompletionNotificationDisposition":true}),
    ];
    for (accepted, extra) in valid
        .into_iter()
        .map(|value| (true, value))
        .chain(invalid.into_iter().map(|value| (false, value)))
    {
        let mut value = json!({"decisionId":"decision","priorSubmissionId":"P","goalId":"goal","successorSubmissionId":"S"});
        value
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let exact = runtime_json::parse(&RuntimeString::from(value.to_string())).unwrap();
        let decoded = runtime_codec::decode_value::<ProtocolGoalContinuation>(&exact);
        assert_eq!(decoded.is_ok(), accepted, "{value}");
        if let Ok(decoded) = decoded {
            assert_eq!(runtime_codec::encode_value(&decoded).unwrap(), exact);
        }
        value["sessionId"] = "session".into();
        let typed = serde_json::from_value::<wire::GoalContinuationNotification>(value.clone());
        assert_eq!(typed.is_ok(), accepted, "{value}");
        if let Ok(typed) = typed {
            assert_eq!(serde_json::to_value(typed).unwrap(), value);
        }
    }
    let schema =
        serde_json::to_value(schemars::schema_for!(wire::GoalContinuationNotification)).unwrap();
    assert_eq!(schema["oneOf"].as_array().unwrap().len(), 3);
    assert_eq!(
        schema["definitions"]["TerminalNotificationDisposition"]["enum"],
        json!(["inherit", "suppress"])
    );
    assert_eq!(
        schema["oneOf"][1]["properties"]["priorCompletionNotificationDisposition"]["const"],
        "suppress"
    );
    assert_eq!(
        schema["oneOf"][2]["required"],
        json!(["continued", "priorCompletionNotificationDisposition"])
    );
}

#[test]
fn optional_terminal_presentation_is_omitted_or_an_exact_nonnull_enum_on_both_carriers() {
    for extra in [
        None,
        Some(json!("inherit")),
        Some(json!("suppress")),
        Some(Value::Null),
        Some(json!("notify")),
        Some(json!(false)),
    ] {
        let accepted = extra
            .as_ref()
            .is_none_or(|value| matches!(value.as_str(), Some("inherit" | "suppress")));
        let mut value = json!({"submissionId":"P"});
        if let Some(extra) = &extra {
            value["terminalNotificationDisposition"] = extra.clone();
        }
        let exact = runtime_json::parse(&RuntimeString::from(value.to_string())).unwrap();
        let decoded = runtime_codec::decode_value::<ProtocolEvent>(&exact);
        assert_eq!(decoded.is_ok(), accepted, "{value}");
        if let Ok(decoded) = decoded {
            let encoded = runtime_codec::encode_value(&decoded).unwrap();
            assert!(encoded.semantic_eq_unordered(&exact), "{value}");
        }
        let mut completed = json!({"sessionId":"session","turnId":"P","result":{}});
        let mut failed = json!({"sessionId":"session","submissionId":"S","detail":{"type":"turn.failed","error":{"message":"kept"}}});
        if let Some(extra) = extra {
            completed["terminalNotificationDisposition"] = extra.clone();
            failed["terminalNotificationDisposition"] = extra;
        }
        let completed_decoded =
            serde_json::from_value::<wire::TurnCompletedNotification>(completed.clone());
        assert_eq!(completed_decoded.is_ok(), accepted, "{completed}");
        if let Ok(decoded) = completed_decoded {
            assert_eq!(serde_json::to_value(decoded).unwrap(), completed);
        }
        let failed_decoded =
            serde_json::from_value::<wire::TurnLifecycleNotification>(failed.clone());
        assert_eq!(failed_decoded.is_ok(), accepted, "{failed}");
        if let Ok(decoded) = failed_decoded {
            assert_eq!(serde_json::to_value(decoded).unwrap(), failed);
        }
    }
}
