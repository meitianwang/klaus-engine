//! Interrupt origins preserve goal authority and fence foreground reservation ownership.

use super::*;
use futures::StreamExt;
use klaus_agent_core::goal::{store as goal_store, ThreadGoal, ThreadGoalStatus};
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeString};
use klaus_agent_core::schemas::message::Usage;
use klaus_agent_core::types::abort::AbortSignal;
use std::sync::Mutex;

struct ProviderScript {
    calls: AtomicUsize,
    entered: tokio::sync::Notify,
    cancelled: tokio::sync::Notify,
    releases: [Arc<tokio::sync::Semaphore>; 2],
    signals: Mutex<Vec<AbortSignal>>,
    cancellation_rows: Mutex<Vec<(usize, Option<ThreadGoal>, usize)>>,
}
impl ProviderScript {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            entered: tokio::sync::Notify::new(),
            cancelled: tokio::sync::Notify::new(),
            releases: std::array::from_fn(|_| Arc::new(tokio::sync::Semaphore::new(0))),
            signals: Mutex::new(Vec::new()),
            cancellation_rows: Mutex::new(Vec::new()),
        })
    }
}

struct InterruptFixture {
    client: TestClient,
    store: Arc<ControlledStore>,
    session: Arc<Session>,
    session_id: String,
    automatic_release: Arc<tokio::sync::Semaphore>,
}

pub(super) async fn request(
    client: &mut TestClient,
    id: u64,
    method: &str,
    params: Value,
) -> (Value, Vec<Value>) {
    let diagnostic_context = format!("endpoint={:p} request={id} method={method}", client);
    let diagnostic_send =
        klaus_agent_core::goal::testing_phase::Span::start("rpc.send", &diagnostic_context);
    client
        .send(json!({"id":id,"method":method,"params":params}))
        .await;
    diagnostic_send.finish("ok");
    let diagnostic_reply =
        klaus_agent_core::goal::testing_phase::Span::start("rpc.reply", &diagnostic_context);
    let response = client.response_for(id).await;
    diagnostic_reply.finish(if response.0.get("error").is_some() {
        "error-reply"
    } else {
        "reply"
    });
    response
}

fn instructions(items: &[RolloutItem]) -> usize {
    klaus_agent_core::goal::retained_context::instructions_from_rollout(items).len()
}

async fn assert_rejected_attempt_is_not_authorization(fixture: &InterruptFixture) {
    use klaus_agent_core::goal::retained_context as retained;
    let raw = fixture
        .store
        .read_thread("interrupt-origin-thread")
        .await
        .unwrap()
        .raw_items;
    let attempts: Vec<_> = raw
        .iter()
        .filter_map(|item| match item {
            RolloutItem::Event(event) if event.event == retained::GOAL_CONTROL_ATTEMPT_EVENT => {
                event.payload.as_ref()
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        attempts.len(),
        1,
        "a real Stop attempt must remain diagnostic history"
    );
    let id = attempts[0].get("requestId").unwrap().as_str().unwrap();
    let dispositions: Vec<_> = raw
        .iter()
        .filter_map(|item| match item {
            RolloutItem::Event(event)
                if event.event == retained::GOAL_CONTROL_DISPOSITION_EVENT =>
            {
                event.payload.as_ref()
            }
            _ => None,
        })
        .collect();
    assert_eq!(dispositions.len(), 1);
    assert_eq!(
        dispositions[0].get("accepted").unwrap().as_bool(),
        Some(false)
    );
    let projected = retained::instructions_from_rollout(&raw);
    let restored =
        retained::instructions_from_rollout_with_receipts("interrupt-origin-thread", &raw)
            .await
            .unwrap();
    assert_eq!(projected, restored);
    assert_eq!(
        projected.len(),
        1,
        "only the original goal control remains authorized"
    );
    for messages in [projected, fixture.session.history()] {
        assert!(
            !messages.iter().any(|message| matches!(message,
            klaus_agent_core::schemas::message::Message::User(user) if &user.uuid == id)),
            "a rejected attempt cannot enter live or restored authorization context"
        );
    }
}

async fn wait_interrupt(client: &mut TestClient, initial: Vec<Value>, submission: &str) -> Value {
    let mut initial = std::collections::VecDeque::from(initial);
    tokio::time::timeout(REPLY_TIMEOUT, async {
        loop {
            let frame = match initial.pop_front() {
                Some(frame) => frame,
                None => client.next_message().await,
            };
            let event = &frame["params"]["event"];
            if event["submissionId"] == submission && event.get("turnInterrupt").is_some() {
                return event.clone();
            }
        }
    })
    .await
    .expect("an acknowledged interrupt must emit a settled typed result")
}

async fn fixture(
    root: &std::path::Path,
    script: Arc<ProviderScript>,
    control: Arc<InstructionFlushControl>,
    config: Value,
) -> InterruptFixture {
    let scoped = config.get("taskResourceScope").is_some();
    let store = Arc::new(ControlledStore {
        inner: Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(root.join("sessions").to_string_lossy().into_owned()),
            archive_dir: None,
        })),
        control,
    });
    let calls = script.clone();
    let provider_store = store.clone();
    let agent = make_agent_with_call_model_and_config(
        Box::new(move |_, signal| {
            let index = calls.calls.fetch_add(1, Ordering::SeqCst);
            assert!(
                index < 2,
                "only explicit fixture turns may call the provider"
            );
            calls.signals.lock().unwrap().push(signal.clone());
            calls.entered.notify_one();
            let observed = calls.clone();
            let stream_signal = signal.clone();
            let store = provider_store.clone();
            tokio::spawn(async move {
                signal.cancelled().await;
                let goal = goal_store::get_goal("interrupt-origin-thread")
                    .await
                    .unwrap();
                let count = instructions(
                    &store
                        .read_thread("interrupt-origin-thread")
                        .await
                        .unwrap()
                        .raw_items,
                );
                observed
                    .cancellation_rows
                    .lock()
                    .unwrap()
                    .push((index, goal, count));
                observed.cancelled.notify_one();
            });
            let release = calls.releases[index].clone();
            let text = if index == 0 {
                "Original foreground finished"
            } else {
                "Replacement foreground finished"
            };
            Box::pin(
                futures::stream::iter(vec![ev::message_start(None), ev::text(text)]).chain(
                    futures::stream::once(async move {
                        tokio::select! {
                            permit = release.acquire() => {
                                permit.unwrap().forget();
                                Some(ev::stop(
                                    Some(ModelStopReason::EndTurn),
                                    Some(Usage {
                                        input_tokens: 0,
                                        output_tokens: 0,
                                        cache_read_input_tokens: 0,
                                        cache_creation_input_tokens: 0,
                                        total_tokens: None,
                                    }),
                                ))
                            }
                            _ = stream_signal.cancelled() => None,
                        }
                    })
                    .filter_map(futures::future::ready),
                ),
            )
        }),
        Some(config.clone()),
    );
    let captured = Arc::new(Mutex::new(None::<Arc<Session>>));
    let automatic_release = Arc::new(tokio::sync::Semaphore::new(0));
    let factory: CreateSession = {
        let store = store.clone();
        let agent = agent.clone();
        let captured = captured.clone();
        let automatic_release = automatic_release.clone();
        Arc::new(move |ctx| {
            let store = store.clone();
            let agent = agent.clone();
            let config = config.clone();
            let captured = captured.clone();
            let automatic_release = automatic_release.clone();
            Box::pin(async move {
                let session = agent
                    .open_session(
                        store,
                        StartSessionOptions {
                            id: ctx.init.thread_id,
                            approval_handler: Some(ctx.approval_handler),
                            elicit_handler: Some(ctx.elicit_handler),
                            mcp_elicitation_presenter: Some(ctx.mcp_elicitation_presenter),
                            config: Some(config),
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(|error| Box::new(error) as BoxError)?;
                if let Some(runtime) = session.goal_runtime() {
                    runtime.testing_before_automatic_provider_admission(Some(Arc::new(
                        move || {
                            let release = automatic_release.clone();
                            Box::pin(async move {
                                release.acquire().await.unwrap().forget();
                            })
                        },
                    )));
                }
                *captured.lock().unwrap() = Some(session.clone());
                Ok(session)
            })
        })
    };
    if scoped {
        goal_store::insert_goal(ThreadGoal {
            thread_id: "interrupt-origin-thread".into(),
            goal_id: "legacy-scoped-goal".into(),
            objective: "Previously saved objective".into(),
            status: ThreadGoalStatus::Paused,
            token_budget: Some(800),
            tokens_used: 21,
            time_used_seconds: 3,
            created_at_ms: 10,
            updated_at_ms: 10,
        })
        .await
        .unwrap()
        .unwrap();
    }
    let lifecycle = crate::lifecycle::protocol_lifecycle(agent, Some(store.clone()));
    let mut client = TestClient::start_with(factory, Some(lifecycle));
    client.initialize().await;
    let (started, _) = request(
        &mut client,
        2,
        "session/start",
        json!({"init":{"threadId":"interrupt-origin-thread"}}),
    )
    .await;
    assert!(started.get("error").is_none(), "{started}");
    let session_id = started["result"]["sessionId"].as_str().unwrap().to_owned();
    let params = if scoped {
        json!({"sessionId":session_id,"status":"paused","origin":"user"})
    } else {
        json!({"sessionId":session_id,"objective":"Preserve the actual goal authority","status":"paused","origin":"user"})
    };
    let (goal, _) = request(&mut client, 3, "session/setGoal", params).await;
    assert!(goal.get("error").is_none(), "{goal}");
    let session = captured.lock().unwrap().clone().unwrap();
    InterruptFixture {
        client,
        store,
        session,
        session_id,
        automatic_release,
    }
}

async fn start_foreground(fixture: &mut InterruptFixture, id: u64, submission: &str) {
    let (started, _) = request(&mut fixture.client, id, "turn/start", json!({
        "sessionId":fixture.session_id,"submissionId":submission,"input":"Wait for the explicit fixture control"
    })).await;
    assert_eq!(started["result"]["turnId"], submission, "{started}");
}

#[tokio::test]
async fn typed_interrupt_origins_pause_before_cancel_with_only_user_stop_authorization() {
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    for (origin, feature_off) in [
        (Some("user-stop"), false),
        (Some("system"), false),
        (Some("targeted-turn"), false),
        (None, true),
    ] {
        let root = tempfile::tempdir().unwrap();
        let _config = ConfigGuard::new(root.path());
        let script = ProviderScript::new();
        let mut fixture = fixture(
            root.path(),
            script.clone(),
            Arc::new(InstructionFlushControl::new()),
            json!({"goal":true,"prewarm":false,"autoMemory":false,"autoDream":false}),
        )
        .await;
        start_foreground(&mut fixture, 4, "foreground-origin").await;
        tokio::time::timeout(REPLY_TIMEOUT, script.entered.notified())
            .await
            .unwrap();
        fixture
            .session
            .set_thread_goal(None, Some("active".into()), None)
            .await
            .unwrap();
        if feature_off {
            fixture.session.set_goal_enabled(false).await.unwrap();
        }
        let before = instructions(
            &fixture
                .store
                .read_thread("interrupt-origin-thread")
                .await
                .unwrap()
                .raw_items,
        );
        assert_eq!(before, 1);
        let mut params = json!({"sessionId":fixture.session_id,"submissionId":"stop-origin",
            "expectedTurnId":"foreground-origin"});
        if let Some(origin) = origin {
            params["origin"] = origin.into();
        }
        let (ack, frames) = request(&mut fixture.client, 5, "turn/interrupt", params).await;
        assert_eq!(ack["result"]["submissionId"], "stop-origin", "{ack}");
        let result = wait_interrupt(&mut fixture.client, frames, "stop-origin").await;
        assert_eq!(result["turnInterrupt"]["accepted"], true, "{result}");
        assert_eq!(
            result["turnInterrupt"]["interruptedTurnId"],
            "foreground-origin"
        );
        assert!(
            result["turnInterrupt"].get("goalPauseError").is_none(),
            "{result}"
        );
        tokio::time::timeout(REPLY_TIMEOUT, script.cancelled.notified())
            .await
            .unwrap();
        {
            let rows = script.cancellation_rows.lock().unwrap();
            assert_eq!(rows.len(), 1);
            let (_, goal, count) = &rows[0];
            let expected_status = if origin == Some("targeted-turn") {
                ThreadGoalStatus::Active
            } else {
                ThreadGoalStatus::Paused
            };
            assert_eq!(
                goal.as_ref().unwrap().status,
                expected_status,
                "the authoritative status must precede the cancellation signal"
            );
            let expected_count = if origin == Some("system") || origin == Some("targeted-turn") {
                before
            } else {
                before + 1
            };
            assert_eq!(
                *count, expected_count,
                "only actual user Stop may retain a user pause instruction"
            );
        }
        if origin == Some("targeted-turn") {
            assert!(
                !fixture
                    .session
                    .goal_runtime()
                    .unwrap()
                    .continuation_suppressed(),
                "targeted interruption must not set the user Stop latch"
            );
            fixture
                .session
                .set_thread_goal(None, Some("paused".into()), None)
                .await
                .unwrap();
        }
        fixture.automatic_release.add_permits(1);
        assert_eq!(script.calls.load(Ordering::SeqCst), 1);
        fixture.client.close().await;
    }
}

#[tokio::test]
async fn a_mismatched_fenced_user_stop_does_not_pause_authorize_or_cancel() {
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let _config = ConfigGuard::new(root.path());
    let script = ProviderScript::new();
    let mut fixture = fixture(
        root.path(),
        script.clone(),
        Arc::new(InstructionFlushControl::new()),
        json!({"goal":true,"prewarm":false,"autoMemory":false,"autoDream":false}),
    )
    .await;
    start_foreground(&mut fixture, 4, "actual-foreground").await;
    tokio::time::timeout(REPLY_TIMEOUT, script.entered.notified())
        .await
        .unwrap();
    fixture
        .session
        .set_thread_goal(None, Some("active".into()), None)
        .await
        .unwrap();
    let before = goal_store::get_record("interrupt-origin-thread")
        .await
        .unwrap();
    let (ack, frames) = request(
        &mut fixture.client,
        5,
        "turn/interrupt",
        json!({
            "sessionId":fixture.session_id,"submissionId":"stale-before-pause","origin":"user-stop",
            "expectedTurnId":"unrelated-foreground"
        }),
    )
    .await;
    assert_eq!(ack["result"]["submissionId"], "stale-before-pause");
    let settled = wait_interrupt(&mut fixture.client, frames, "stale-before-pause").await;
    assert_eq!(settled["turnInterrupt"]["accepted"], false);
    assert!(settled["error"].as_str().unwrap().contains("foreground"));
    assert_eq!(
        goal_store::get_record("interrupt-origin-thread")
            .await
            .unwrap(),
        before
    );
    assert!(!script.signals.lock().unwrap()[0].is_cancelled());
    assert!(!fixture
        .session
        .goal_runtime()
        .unwrap()
        .continuation_suppressed());
    assert_eq!(
        instructions(
            &fixture
                .store
                .read_thread("interrupt-origin-thread")
                .await
                .unwrap()
                .raw_items
        ),
        1
    );
    fixture
        .session
        .set_thread_goal(None, Some("paused".into()), None)
        .await
        .unwrap();
    script.releases[0].add_permits(1);
    wait_for_goal_completion(
        &mut fixture.client,
        Vec::new(),
        "Original foreground finished",
    )
    .await;
    fixture.automatic_release.add_permits(1);
    fixture.client.close().await;
}

struct FrozenClock(tokio::task::JoinHandle<()>);
impl FrozenClock {
    fn new() -> Self {
        tokio::time::pause();
        // Keep logical time fixed while real rollout I/O settles. This exercises
        // reservation ownership rather than racing the production pause deadline.
        Self(tokio::spawn(async {
            loop {
                tokio::task::yield_now().await;
            }
        }))
    }
}
impl Drop for FrozenClock {
    fn drop(&mut self) {
        self.0.abort();
        tokio::time::resume();
    }
}

async fn delayed_stop_replacement(expected_fence: bool) {
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let _config = ConfigGuard::new(root.path());
    let script = ProviderScript::new();
    let control = Arc::new(InstructionFlushControl::new());
    let mut fixture = fixture(
        root.path(),
        script.clone(),
        control.clone(),
        json!({"goal":true,"prewarm":false,"autoMemory":false,"autoDream":false}),
    )
    .await;
    start_foreground(&mut fixture, 4, "original-foreground").await;
    tokio::time::timeout(REPLY_TIMEOUT, script.entered.notified())
        .await
        .unwrap();
    let runtime = fixture.session.goal_runtime().unwrap();
    runtime.testing_freeze_elapsed();
    fixture
        .session
        .set_thread_goal(None, Some("active".into()), None)
        .await
        .unwrap();
    let before = goal_store::get_record("interrupt-origin-thread")
        .await
        .unwrap();
    let clock = FrozenClock::new();
    control.block.store(true, Ordering::SeqCst);
    let mut params = json!({"sessionId":fixture.session_id,"submissionId":"delayed-fenced-stop","origin":"user-stop"});
    if expected_fence {
        params["expectedTurnId"] = "original-foreground".into();
    }
    fixture
        .client
        .send(json!({"id":5,"method":"turn/interrupt","params":params}))
        .await;
    control.entered.notified().await;
    assert!(!script.signals.lock().unwrap()[0].is_cancelled());
    // Replacement admission preserves goal content and control intent while
    // persisting one new run marker. The old Stop cannot mutate that snapshot.
    start_foreground(&mut fixture, 6, "replacement-foreground").await;
    script.releases[0].add_permits(1);
    script.entered.notified().await;
    assert_eq!(script.calls.load(Ordering::SeqCst), 2);
    assert!(!script.signals.lock().unwrap()[1].is_cancelled());
    let current = goal_store::get_record("interrupt-origin-thread")
        .await
        .unwrap();
    let mut expected_current = before.clone();
    expected_current.revision = expected_current.revision.checked_add(1).unwrap();
    assert_eq!(
        current, expected_current,
        "actual provider admission adds only its run-marker revision without a goal edit"
    );
    control.release.add_permits(1);
    let (ack, seen) = fixture.client.response_for(5).await;
    assert_eq!(ack["result"]["submissionId"], "delayed-fenced-stop");
    let settled = wait_interrupt(&mut fixture.client, seen, "delayed-fenced-stop").await;
    assert_eq!(settled["turnInterrupt"]["accepted"], false, "{settled}");
    assert!(settled["error"].as_str().unwrap().contains("changed"));
    assert!(settled["turnInterrupt"]["goalPauseError"]
        .as_str()
        .unwrap()
        .contains("goal"));
    assert!(
        settled["turnInterrupt"]["goalPauseError"]
            .as_str()
            .unwrap()
            .contains("lost its foreground reservation"),
        "the exact reservation guard rejects the pause before goal revision CAS: {settled}"
    );
    assert!(
        !script.signals.lock().unwrap()[1].is_cancelled(),
        "a replaced reservation must never receive the old Stop cancellation"
    );
    assert_eq!(
        goal_store::get_record("interrupt-origin-thread")
            .await
            .unwrap(),
        current,
        "the stale pause must not change goal status, counters, timestamps or revision"
    );
    assert!(
        !runtime.continuation_suppressed(),
        "a stale Stop must not latch the replacement runtime"
    );
    assert_rejected_attempt_is_not_authorization(&fixture).await;
    fixture
        .session
        .set_thread_goal(None, Some("paused".into()), None)
        .await
        .unwrap();
    drop(clock);
    script.releases[1].add_permits(1);
    wait_for_goal_completion(
        &mut fixture.client,
        Vec::new(),
        "Replacement foreground finished",
    )
    .await;
    runtime.testing_unfreeze_elapsed();
    fixture.automatic_release.add_permits(1);
    let (queue, _) = request(
        &mut fixture.client,
        7,
        "session/queueSnapshot",
        json!({"sessionId":fixture.session_id}),
    )
    .await;
    assert!(queue["result"]["queueSnapshot"]["items"]
        .as_array()
        .unwrap()
        .is_empty());
    fixture.client.close().await;
}

#[tokio::test]
async fn a_user_stop_delayed_in_evidence_flush_cannot_cancel_a_replacement_reservation() {
    delayed_stop_replacement(true).await;
}

#[tokio::test]
async fn an_unfenced_user_stop_cannot_retarget_a_replacement_reservation() {
    delayed_stop_replacement(false).await;
}

#[test]
fn interrupt_origin_and_foreground_fence_round_trip_with_strict_typed_rejection() {
    for origin in [
        None,
        Some("user-stop"),
        Some("system"),
        Some("targeted-turn"),
    ] {
        let mut params = json!({"sessionId":"session","submissionId":"stop-correlation","expectedTurnId":"foreground-owner"});
        let mut op = json!({"id":"stop-correlation","op":{"type":"interrupt","expectedTurnId":"foreground-owner"}});
        if let Some(origin) = origin {
            params["origin"] = origin.into();
            op["op"]["origin"] = origin.into();
        }
        let decoded: wire::TurnInterruptParams = serde_json::from_value(params.clone()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), params);
        let exact = runtime_json::parse(&RuntimeString::from(op.to_string())).unwrap();
        let parsed = klaus_agent_protocol::types::parse_op(exact).unwrap();
        let output = runtime_json::stringify(&runtime_codec::encode_value(&parsed).unwrap())
            .try_to_utf8()
            .unwrap();
        assert_eq!(serde_json::from_str::<Value>(&output).unwrap(), op);
    }
    for origin in ["automatic", "user", "internal", "descendant-cleanup", ""] {
        assert!(serde_json::from_value::<wire::TurnInterruptParams>(
            json!({"sessionId":"session","origin":origin})
        )
        .is_err());
        let input = json!({"id":"invalid","op":{"type":"interrupt","origin":origin}});
        let exact = runtime_json::parse(&RuntimeString::from(input.to_string())).unwrap();
        assert!(klaus_agent_protocol::types::parse_op(exact).is_err());
    }
    let input = json!({"id":"invalid","op":{"type":"interrupt","expectedTurnId":""}});
    let exact = runtime_json::parse(&RuntimeString::from(input.to_string())).unwrap();
    assert!(klaus_agent_protocol::types::parse_op(exact).is_err());
    let event = klaus_agent_protocol::types::ProtocolEvent {
        submission_id: "stop-correlation".into(),
        turn_interrupt: Some(klaus_agent_protocol::types::ProtocolTurnInterruptResult {
            accepted: false,
            interrupted_turn_id: None,
            goal_pause_error: Some("pause failed".into()),
        }),
        error: Some("stale foreground reservation".into()),
        ..Default::default()
    };
    let encoded = runtime_codec::encode_value(&event).unwrap();
    let decoded: klaus_agent_protocol::types::ProtocolEvent =
        runtime_codec::decode_value(&encoded).unwrap();
    assert_eq!(decoded.turn_interrupt, event.turn_interrupt);
}

#[tokio::test]
async fn system_interrupt_reports_pause_failure_and_still_cancels_without_user_authorization() {
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let _config = ConfigGuard::new(root.path());
    let script = ProviderScript::new();
    let mut fixture = fixture(
        root.path(),
        script.clone(),
        Arc::new(InstructionFlushControl::new()),
        json!({"goal":true,"prewarm":false,"autoMemory":false,"autoDream":false}),
    )
    .await;
    start_foreground(&mut fixture, 4, "system-failure-foreground").await;
    tokio::time::timeout(REPLY_TIMEOUT, script.entered.notified())
        .await
        .unwrap();
    fixture
        .session
        .set_thread_goal(None, Some("active".into()), None)
        .await
        .unwrap();
    goal_store::testing_execute("CREATE TRIGGER reject_system_pause BEFORE INSERT ON goals BEGIN SELECT RAISE(ABORT, 'forced system pause failure'); END;").await.unwrap();
    let (ack, frames) = request(
        &mut fixture.client,
        5,
        "turn/interrupt",
        json!({
            "sessionId":fixture.session_id,"submissionId":"system-failure-stop","origin":"system",
            "expectedTurnId":"system-failure-foreground"
        }),
    )
    .await;
    assert_eq!(ack["result"]["submissionId"], "system-failure-stop");
    let settled = wait_interrupt(&mut fixture.client, frames, "system-failure-stop").await;
    assert_eq!(settled["turnInterrupt"]["accepted"], true, "{settled}");
    assert!(
        settled["turnInterrupt"]["goalPauseError"]
            .as_str()
            .unwrap()
            .contains("forced system pause failure"),
        "{settled}"
    );
    tokio::time::timeout(REPLY_TIMEOUT, script.cancelled.notified())
        .await
        .unwrap();
    assert!(script.signals.lock().unwrap()[0].is_cancelled());
    assert_eq!(
        instructions(
            &fixture
                .store
                .read_thread("interrupt-origin-thread")
                .await
                .unwrap()
                .raw_items
        ),
        1,
        "system cancellation must never invent user Stop authorization"
    );
    goal_store::testing_execute("DROP TRIGGER reject_system_pause;")
        .await
        .unwrap();
    fixture
        .session
        .set_thread_goal(None, Some("paused".into()), None)
        .await
        .unwrap();
    fixture.automatic_release.add_permits(1);
    fixture.client.close().await;
}

#[tokio::test]
async fn lazy_interrupt_and_invalid_fences_settle_without_creating_a_session() {
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let _config = ConfigGuard::new(root.path());
    let creates = Arc::new(AtomicUsize::new(0));
    let counter = creates.clone();
    let never_create: CreateSession = Arc::new(move |_| {
        counter.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { panic!("interrupting an unbound endpoint must not initialize a Session") })
    });
    let mut client = TestClient::start(never_create);
    client.initialize().await;
    let (started, _) = request(
        &mut client,
        2,
        "session/start",
        json!({"init":{"threadId":"lazy-interrupt"}}),
    )
    .await;
    let session = started["result"]["sessionId"].clone();
    for (index, origin) in ["user-stop", "system", "targeted-turn"]
        .into_iter()
        .enumerate()
    {
        let submission = format!("lazy-stop-{index}");
        let (ack, frames) = request(
            &mut client,
            10 + index as u64,
            "turn/interrupt",
            json!({
                "sessionId":session,"submissionId":submission,"origin":origin
            }),
        )
        .await;
        assert_eq!(ack["result"]["submissionId"], submission);
        let settled = wait_interrupt(&mut client, frames, &submission).await;
        assert_eq!(
            settled["turnInterrupt"]["accepted"],
            origin != "targeted-turn",
            "{settled}"
        );
        assert!(settled["turnInterrupt"].get("interruptedTurnId").is_none());
        assert!(settled["turnInterrupt"].get("goalPauseError").is_none());
    }
    let (ack,frames) = request(&mut client,20,"turn/interrupt",json!({
        "sessionId":session,"submissionId":"lazy-missing-target","origin":"user-stop","expectedTurnId":"absent-foreground"
    })).await;
    assert_eq!(ack["result"]["submissionId"], "lazy-missing-target");
    let settled = wait_interrupt(&mut client, frames, "lazy-missing-target").await;
    assert_eq!(settled["turnInterrupt"]["accepted"], false);
    for (id, params) in [
        (21, json!({"sessionId":session,"origin":"invalid"})),
        (22, json!({"sessionId":session,"expectedTurnId":""})),
    ] {
        let (invalid, _) = request(&mut client, id, "turn/interrupt", params).await;
        assert_eq!(invalid["error"]["code"], wire::INVALID_PARAMS, "{invalid}");
    }
    assert_eq!(creates.load(Ordering::SeqCst), 0);
    client.close().await;
}

#[tokio::test]
async fn scoped_goal_writes_fail_before_authorization_but_legacy_pause_and_clear_remain_available()
{
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let _config = ConfigGuard::new(root.path());
    let script = ProviderScript::new();
    let mut fixture = fixture(
        root.path(),
        script.clone(),
        Arc::new(InstructionFlushControl::new()),
        json!({
            "goal":true,"prewarm":false,"autoMemory":false,"autoDream":false,"taskResourceScope":{
                "taskId":"limited-goal-controls","mode":"scoped-auto","readRoots":[],"writeRoots":[]
            }
        }),
    )
    .await;
    let before = goal_store::get_record("interrupt-origin-thread")
        .await
        .unwrap();
    assert_eq!(before.goal.as_ref().unwrap().tokens_used, 21);
    let (read, _) = request(
        &mut fixture.client,
        4,
        "session/goal",
        json!({"sessionId":fixture.session_id}),
    )
    .await;
    assert_eq!(read["result"]["goalPursuitEligible"], false);
    assert_eq!(
        read["result"]["threadGoal"]["objective"],
        "Previously saved objective"
    );
    for (index, patch) in [
        json!({"objective":"Forbidden new objective"}),
        json!({"tokenBudget":900}),
        json!({"status":"active"}),
    ]
    .into_iter()
    .enumerate()
    {
        let mut params = patch;
        params["sessionId"] = fixture.session_id.clone().into();
        params["origin"] = "user".into();
        let (refused, _) = request(
            &mut fixture.client,
            10 + index as u64,
            "session/setGoal",
            params,
        )
        .await;
        assert!(
            refused["error"]["message"]
                .as_str()
                .unwrap()
                .contains("task resource scope"),
            "{refused}"
        );
        assert_eq!(
            goal_store::get_record("interrupt-origin-thread")
                .await
                .unwrap(),
            before
        );
        assert_eq!(
            instructions(
                &fixture
                    .store
                    .read_thread("interrupt-origin-thread")
                    .await
                    .unwrap()
                    .raw_items
            ),
            1,
            "an ineligible write must not append trusted user goal evidence"
        );
    }
    let (cleared,_) = request(&mut fixture.client,20,"session/clearGoal",json!({
        "sessionId":fixture.session_id,"origin":"user","expectedGoalId":"legacy-scoped-goal","expectedGoalRevision":before.revision
    })).await;
    assert_eq!(cleared["result"]["goalCleared"], true, "{cleared}");
    assert!(goal_store::get_goal("interrupt-origin-thread")
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        instructions(
            &fixture
                .store
                .read_thread("interrupt-origin-thread")
                .await
                .unwrap()
                .raw_items
        ),
        2
    );
    assert_eq!(script.calls.load(Ordering::SeqCst), 0);
    fixture.client.close().await;
}

#[tokio::test]
async fn a_critical_stop_deadline_revokes_a_late_pause_writer_before_it_can_commit() {
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let _config = ConfigGuard::new(root.path());
    let script = ProviderScript::new();
    let control = Arc::new(InstructionFlushControl::new());
    let mut fixture = fixture(
        root.path(),
        script.clone(),
        control.clone(),
        json!({"goal":true,"prewarm":false,"autoMemory":false,"autoDream":false}),
    )
    .await;
    start_foreground(&mut fixture, 4, "deadline-foreground").await;
    tokio::time::timeout(REPLY_TIMEOUT, script.entered.notified())
        .await
        .unwrap();
    let runtime = fixture.session.goal_runtime().unwrap();
    runtime.testing_freeze_elapsed();
    fixture
        .session
        .set_thread_goal(None, Some("active".into()), None)
        .await
        .unwrap();
    let before = goal_store::get_record("interrupt-origin-thread")
        .await
        .unwrap();
    let stop_started = std::time::Instant::now();
    control.block.store(true, Ordering::SeqCst);
    fixture
        .client
        .send(json!({"id":5,"method":"turn/interrupt","params":{
            "sessionId":fixture.session_id,"submissionId":"deadline-stop","origin":"user-stop",
            "expectedTurnId":"deadline-foreground"
        }}))
        .await;
    if tokio::time::timeout(REPLY_TIMEOUT, control.entered.notified())
        .await
        .is_err()
    {
        let settled = wait_interrupt(&mut fixture.client, Vec::new(), "deadline-stop").await;
        let current = goal_store::get_record("interrupt-origin-thread").await;
        panic!("the active Stop must reach its evidence flush before expiry: result={settled}; goal={current:?}");
    }
    eprintln!(
        "[goal-stop-deadline] evidenceEnteredMs={}",
        stop_started.elapsed().as_millis()
    );
    assert!(!script.signals.lock().unwrap()[0].is_cancelled());
    let (ack, frames) = fixture.client.response_for(5).await;
    assert_eq!(ack["result"]["submissionId"], "deadline-stop");
    let settled = wait_interrupt(&mut fixture.client, frames, "deadline-stop").await;
    eprintln!(
        "[goal-stop-deadline] settledMs={} result={settled}",
        stop_started.elapsed().as_millis()
    );
    assert_eq!(settled["turnInterrupt"]["accepted"], true, "{settled}");
    assert!(
        runtime.continuation_suppressed(),
        "the Stop must fence automatic work even if its writer times out"
    );
    assert!(
        settled["turnInterrupt"]["goalPauseError"]
            .as_str()
            .unwrap()
            .contains("pending"),
        "{settled}"
    );
    assert!(
        script.signals.lock().unwrap()[0].is_cancelled(),
        "critical cancellation must not wait for the retained evidence writer"
    );
    assert_eq!(
        goal_store::get_record("interrupt-origin-thread")
            .await
            .unwrap(),
        before
    );
    control.release.add_permits(1);
    // Acquiring the same writer authority proves the detached pause attempt has
    // completed after its flush; the assertion cannot race that late transaction.
    let finished = tokio::time::timeout(
        REPLY_TIMEOUT,
        klaus_agent_core::goal::service::thread_mutation_guard("interrupt-origin-thread"),
    )
    .await
    .expect("the released late writer must finish before its final state is inspected");
    drop(finished);
    assert_eq!(
        goal_store::get_record("interrupt-origin-thread")
            .await
            .unwrap(),
        before,
        "a cancelled reservation must lose all authority to commit a late pause"
    );
    assert_eq!(
        instructions(
            &fixture
                .store
                .read_thread("interrupt-origin-thread")
                .await
                .unwrap()
                .raw_items
        ),
        1,
        "a timed-out attempt is history, not accepted pause authorization"
    );
    assert_rejected_attempt_is_not_authorization(&fixture).await;
    runtime.notify_idle();
    let (_, frames) = request(
        &mut fixture.client,
        6,
        "session/queueSnapshot",
        json!({"sessionId":fixture.session_id}),
    )
    .await;
    assert!(!frames
        .iter()
        .any(|frame| frame["method"] == "turn/started"
            && frame["params"]["goalContinuation"] == true));
    assert_eq!(
        script.calls.load(Ordering::SeqCst),
        1,
        "a timed-out pause must not restart goal work"
    );
    fixture
        .session
        .set_thread_goal(None, Some("paused".into()), None)
        .await
        .unwrap();
    runtime.testing_unfreeze_elapsed();
    fixture.automatic_release.add_permits(1);
    fixture.client.close().await;
}

async fn seed_saved_goal(store: &dyn SessionStore, thread: &str) {
    let rollout = store
        .create(klaus_agent_core::schemas::rollout::SessionMeta {
            id: thread.into(),
            created_at: 10,
            cwd: "/tmp".into(),
            model: "fixture".into(),
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
        .unwrap();
    rollout.shutdown().await.unwrap();
    goal_store::insert_goal(ThreadGoal {
        thread_id: thread.into(),
        goal_id: "saved-pause-generation".into(),
        objective: "Saved active work must stop before teardown".into(),
        status: ThreadGoalStatus::Active,
        token_budget: Some(1000),
        tokens_used: 17,
        time_used_seconds: 4,
        created_at_ms: 10,
        updated_at_ms: 10,
    })
    .await
    .unwrap()
    .unwrap();
}

#[tokio::test]
async fn lazy_saved_active_user_and_system_interrupts_pause_without_creating_a_session() {
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    for origin in ["user-stop", "system"] {
        let root = tempfile::tempdir().unwrap();
        let _config = ConfigGuard::new(root.path());
        let control = Arc::new(InstructionFlushControl::new());
        let store = Arc::new(ControlledStore {
            inner: Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
                dir: Some(root.path().join("sessions").to_string_lossy().into_owned()),
                archive_dir: None,
            })),
            control: control.clone(),
        });
        seed_saved_goal(store.as_ref(), "lazy-saved-stop").await;
        let before = goal_store::get_record("lazy-saved-stop").await.unwrap();
        let creates = Arc::new(AtomicUsize::new(0));
        let count = creates.clone();
        let never_create: CreateSession = Arc::new(move |_| {
            count.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { panic!("saved Stop must not construct a Session") })
        });
        let provider_calls = Arc::new(AtomicUsize::new(0));
        let calls = provider_calls.clone();
        let agent = make_agent_with_call_model_and_config(
            Box::new(move |_, _| {
                calls.fetch_add(1, Ordering::SeqCst);
                panic!("saved Stop must not resolve a provider")
            }),
            Some(json!({"goal":true,"prewarm":false,"autoMemory":false,"autoDream":false})),
        );
        let lifecycle = crate::lifecycle::protocol_lifecycle(agent, Some(store.clone()));
        let mut client = TestClient::start_with(never_create, Some(lifecycle));
        client.initialize().await;
        let (started, _) = request(
            &mut client,
            2,
            "session/start",
            json!({"init":{"threadId":"lazy-saved-stop"}}),
        )
        .await;
        let session = started["result"]["sessionId"].clone();
        let (ack, frames) = request(
            &mut client,
            3,
            "turn/interrupt",
            json!({
                "sessionId":session,"submissionId":"lazy-saved-pause","origin":origin
            }),
        )
        .await;
        assert_eq!(ack["result"]["submissionId"], "lazy-saved-pause");
        let settled = wait_interrupt(&mut client, frames, "lazy-saved-pause").await;
        assert_eq!(settled["turnInterrupt"]["accepted"], true, "{settled}");
        assert!(
            settled["turnInterrupt"].get("goalPauseError").is_none(),
            "{settled}"
        );
        assert!(settled["turnInterrupt"].get("interruptedTurnId").is_none());
        let saved = goal_store::get_record("lazy-saved-stop").await.unwrap();
        let goal = saved.goal.unwrap();
        let original = before.goal.unwrap();
        assert_eq!(goal.status, ThreadGoalStatus::Paused);
        assert_eq!(
            (
                goal.goal_id,
                goal.objective,
                goal.tokens_used,
                goal.time_used_seconds
            ),
            (
                original.goal_id,
                original.objective,
                original.tokens_used,
                original.time_used_seconds
            )
        );
        assert_eq!(saved.revision, before.revision + 1);
        assert!(
            !control.writer_owned.load(Ordering::SeqCst),
            "the saved writer must flush and release before the settled result"
        );
        let raw = store
            .read_thread("lazy-saved-stop")
            .await
            .unwrap()
            .raw_items;
        let restored =
            klaus_agent_core::goal::retained_context::instructions_from_rollout_with_receipts(
                "lazy-saved-stop",
                &raw,
            )
            .await
            .unwrap();
        assert_eq!(restored.len(), usize::from(origin == "user-stop"));
        assert_eq!(instructions(&raw), restored.len());
        assert_eq!(creates.load(Ordering::SeqCst), 0);
        assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
        client.close().await;
    }
}

#[tokio::test]
async fn targeted_interrupt_without_a_target_has_no_goal_or_cancellation_effects() {
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let _config = ConfigGuard::new(root.path());
    let script = ProviderScript::new();
    let mut fixture = fixture(
        root.path(),
        script.clone(),
        Arc::new(InstructionFlushControl::new()),
        json!({"goal":true,"prewarm":false,"autoMemory":false,"autoDream":false}),
    )
    .await;
    start_foreground(&mut fixture, 4, "required-target-foreground").await;
    tokio::time::timeout(REPLY_TIMEOUT, script.entered.notified())
        .await
        .unwrap();
    fixture
        .session
        .set_thread_goal(None, Some("active".into()), None)
        .await
        .unwrap();
    let before = goal_store::get_record("interrupt-origin-thread")
        .await
        .unwrap();
    let (_, frames) = request(
        &mut fixture.client,
        5,
        "turn/interrupt",
        json!({
            "sessionId":fixture.session_id,"submissionId":"missing-target","origin":"targeted-turn"
        }),
    )
    .await;
    let settled = wait_interrupt(&mut fixture.client, frames, "missing-target").await;
    assert_eq!(settled["turnInterrupt"]["accepted"], false);
    assert!(settled["error"]
        .as_str()
        .unwrap()
        .contains("requires expectedTurnId"));
    assert_eq!(
        goal_store::get_record("interrupt-origin-thread")
            .await
            .unwrap(),
        before
    );
    assert!(!script.signals.lock().unwrap()[0].is_cancelled());
    assert!(!fixture
        .session
        .goal_runtime()
        .unwrap()
        .continuation_suppressed());
    assert_eq!(
        instructions(
            &fixture
                .store
                .read_thread("interrupt-origin-thread")
                .await
                .unwrap()
                .raw_items
        ),
        1
    );
    fixture
        .session
        .set_thread_goal(None, Some("paused".into()), None)
        .await
        .unwrap();
    script.releases[0].add_permits(1);
    wait_for_goal_completion(
        &mut fixture.client,
        Vec::new(),
        "Original foreground finished",
    )
    .await;
    fixture.automatic_release.add_permits(1);
    fixture.client.close().await;
}

#[tokio::test]
async fn an_idle_user_stop_cannot_pause_or_cancel_a_later_human_reservation() {
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let _config = ConfigGuard::new(root.path());
    let script = ProviderScript::new();
    let control = Arc::new(InstructionFlushControl::new());
    let mut fixture = fixture(
        root.path(),
        script.clone(),
        control.clone(),
        json!({"goal":true,"prewarm":false,"autoMemory":false,"autoDream":false}),
    )
    .await;
    let (mode, _) = request(
        &mut fixture.client,
        3,
        "session/setCollaborationMode",
        json!({"sessionId":fixture.session_id,"mode":"plan"}),
    )
    .await;
    assert!(mode.get("error").is_none(), "{mode}");
    let runtime = fixture.session.goal_runtime().unwrap();
    runtime.testing_freeze_elapsed();
    fixture
        .session
        .set_thread_goal(None, Some("active".into()), None)
        .await
        .unwrap();
    // Plan mode prevents automatic work while this active goal's idle Stop
    // waits for evidence flush. A later ordinary turn still invalidates it.
    let before = goal_store::get_record("interrupt-origin-thread")
        .await
        .unwrap();
    let clock = FrozenClock::new();
    control.block.store(true, Ordering::SeqCst);
    fixture
        .client
        .send(json!({"id":4,"method":"turn/interrupt","params":{
            "sessionId":fixture.session_id,"submissionId":"idle-delayed-stop","origin":"user-stop"
        }}))
        .await;
    control.entered.notified().await;
    start_foreground(&mut fixture, 5, "human-after-idle-stop").await;
    script.entered.notified().await;
    control.release.add_permits(1);
    let (_, frames) = fixture.client.response_for(4).await;
    let settled = wait_interrupt(&mut fixture.client, frames, "idle-delayed-stop").await;
    assert_eq!(settled["turnInterrupt"]["accepted"], false, "{settled}");
    assert_eq!(
        goal_store::get_record("interrupt-origin-thread")
            .await
            .unwrap(),
        before
    );
    assert!(!script.signals.lock().unwrap()[0].is_cancelled());
    assert!(!fixture
        .session
        .goal_runtime()
        .unwrap()
        .continuation_suppressed());
    assert_rejected_attempt_is_not_authorization(&fixture).await;
    drop(clock);
    script.releases[0].add_permits(1);
    wait_for_goal_completion(
        &mut fixture.client,
        Vec::new(),
        "Original foreground finished",
    )
    .await;
    fixture
        .session
        .set_thread_goal(None, Some("paused".into()), None)
        .await
        .unwrap();
    runtime.testing_unfreeze_elapsed();
    fixture.automatic_release.add_permits(1);
    fixture.client.close().await;
}

#[tokio::test]
async fn resumed_authorization_resolves_a_committed_pause_receipt_without_its_disposition() {
    use klaus_agent_core::goal::retained_context as retained;
    use klaus_agent_core::goal::service::HostGoalMutation;
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let _config = ConfigGuard::new(root.path());
    let store: Arc<dyn SessionStore> = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.path().join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    seed_saved_goal(store.as_ref(), "pause-receipt-recovery").await;
    let acquired = store
        .resume_for_write("pause-receipt-recovery")
        .await
        .unwrap();
    let first = retained::instruction_for_set(
        "original-objective".into(),
        Some(&"exact durable objective".into()),
        None,
    )
    .unwrap()
    .unwrap();
    retained::append_instruction(acquired.rollout.as_ref(), &first)
        .await
        .unwrap();
    let snapshot = goal_store::get_record("pause-receipt-recovery")
        .await
        .unwrap();
    let mutation = HostGoalMutation {
        status: Some("paused".into()),
        expected_goal_id: Some(snapshot.goal.unwrap().goal_id),
        expected_revision: Some(snapshot.revision),
        ..Default::default()
    };
    let applied = retained::pause_control_attempt(
        "committed-pause-attempt".into(),
        Some("old-foreground".into()),
        &mutation,
    )
    .unwrap();
    retained::append_control_attempt(acquired.rollout.as_ref(), &applied)
        .await
        .unwrap();
    let runtime = klaus_agent_core::goal::runtime::GoalRuntime::new(
        "pause-receipt-recovery".into(),
        Arc::new(|| "recovery-goal-id".into()),
        Arc::new(|| 20),
        None,
    );
    runtime
        .apply_host_update_once(mutation, "committed-pause-attempt")
        .await
        .unwrap();
    // Simulate a crash after SQL COMMIT, before the accepted disposition append.
    let rejected_mutation = HostGoalMutation {
        status: Some("paused".into()),
        ..Default::default()
    };
    let rejected = retained::pause_control_attempt(
        "uncommitted-pause-attempt".into(),
        None,
        &rejected_mutation,
    )
    .unwrap();
    retained::append_control_attempt(acquired.rollout.as_ref(), &rejected)
        .await
        .unwrap();
    let clear = retained::clear_instruction("later-clear-control".into());
    retained::append_instruction(acquired.rollout.as_ref(), &clear)
        .await
        .unwrap();
    acquired.rollout.shutdown().await.unwrap();
    let raw = store
        .read_thread("pause-receipt-recovery")
        .await
        .unwrap()
        .raw_items;
    let authorized =
        retained::instructions_from_rollout_with_receipts("pause-receipt-recovery", &raw)
            .await
            .unwrap();
    let expected: Vec<_> = [
        "original-objective",
        "committed-pause-attempt",
        "later-clear-control",
    ]
    .into_iter()
    .map(RuntimeString::from)
    .collect();
    let ids = |messages: &[klaus_agent_core::schemas::message::Message]| {
        messages
            .iter()
            .filter_map(|message| match message {
                klaus_agent_core::schemas::message::Message::User(user) => Some(user.uuid.clone()),
                _ => None,
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(ids(&authorized), expected);
    let provider_calls = Arc::new(AtomicUsize::new(0));
    let calls = provider_calls.clone();
    let agent = make_agent_with_call_model_and_config(
        Box::new(move |_, _| {
            calls.fetch_add(1, Ordering::SeqCst);
            panic!("restoring scoped authorization must not infer")
        }),
        Some(json!({"goal":true,"prewarm":false,"autoMemory":false,"autoDream":false})),
    );
    let resumed = agent
        .resume_session(
            store,
            "pause-receipt-recovery",
            StartSessionOptions::default(),
        )
        .await
        .unwrap();
    let restored_ids: Vec<_> = ids(&resumed.history())
        .into_iter()
        .filter(|id| expected.contains(id) || *id == rejected.request_id)
        .collect();
    assert_eq!(
        restored_ids, expected,
        "receipt recovery must retain original attempt order without granting pending attempts"
    );
    assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
    resumed.dispose().await;
}

#[tokio::test]
async fn an_unfenced_stop_cancels_its_owner_and_preserves_a_queued_human_turn() {
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let _config = ConfigGuard::new(root.path());
    let script = ProviderScript::new();
    let control = Arc::new(InstructionFlushControl::new());
    let mut fixture = fixture(
        root.path(),
        script.clone(),
        control.clone(),
        json!({"goal":true,"prewarm":false,"autoMemory":false,"autoDream":false}),
    )
    .await;
    start_foreground(&mut fixture, 4, "unfenced-current-owner").await;
    tokio::time::timeout(REPLY_TIMEOUT, script.entered.notified())
        .await
        .unwrap();
    fixture
        .session
        .set_thread_goal(None, Some("active".into()), None)
        .await
        .unwrap();
    let clock = FrozenClock::new();
    control.block.store(true, Ordering::SeqCst);
    fixture.client.send(json!({"id":5,"method":"turn/interrupt","params":{
        "sessionId":fixture.session_id,"submissionId":"unfenced-stop-preserve-queue","origin":"user-stop"
    }})).await;
    control.entered.notified().await;
    start_foreground(&mut fixture, 6, "queued-human-after-stop").await;
    assert_eq!(script.calls.load(Ordering::SeqCst), 1);
    control.release.add_permits(1);
    let (_, frames) = fixture.client.response_for(5).await;
    let settled = wait_interrupt(&mut fixture.client, frames, "unfenced-stop-preserve-queue").await;
    assert_eq!(settled["turnInterrupt"]["accepted"], true, "{settled}");
    assert_eq!(
        settled["turnInterrupt"]["interruptedTurnId"],
        "unfenced-current-owner"
    );
    script.entered.notified().await;
    assert_eq!(
        script.calls.load(Ordering::SeqCst),
        2,
        "the queued human token must survive Stop of the previous owner"
    );
    assert!(script.signals.lock().unwrap()[0].is_cancelled());
    assert!(!script.signals.lock().unwrap()[1].is_cancelled());
    assert_eq!(
        goal_store::get_goal("interrupt-origin-thread")
            .await
            .unwrap()
            .unwrap()
            .status,
        ThreadGoalStatus::Paused
    );
    drop(clock);
    script.releases[1].add_permits(1);
    wait_for_goal_completion(
        &mut fixture.client,
        Vec::new(),
        "Replacement foreground finished",
    )
    .await;
    fixture.automatic_release.add_permits(1);
    fixture.client.close().await;
}

#[tokio::test]
async fn user_stop_cancels_an_ordinary_turn_without_rewriting_any_stopped_goal() {
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    for status in [
        ThreadGoalStatus::Paused,
        ThreadGoalStatus::Blocked,
        ThreadGoalStatus::UsageLimited,
        ThreadGoalStatus::BudgetLimited,
        ThreadGoalStatus::Complete,
    ] {
        let root = tempfile::tempdir().unwrap();
        let _config = ConfigGuard::new(root.path());
        let script = ProviderScript::new();
        let mut fixture = fixture(
            root.path(),
            script.clone(),
            Arc::new(InstructionFlushControl::new()),
            json!({"goal":true,"prewarm":false,"autoMemory":false,"autoDream":false}),
        )
        .await;
        let (mode, _) = request(
            &mut fixture.client,
            4,
            "session/setCollaborationMode",
            json!({"sessionId":fixture.session_id,"mode":"plan"}),
        )
        .await;
        assert!(mode.get("error").is_none(), "{mode}");
        goal_store::update_goal(
            "interrupt-origin-thread",
            klaus_agent_core::goal::types::GoalUpdate {
                status: Some(status),
                ..Default::default()
            },
            20,
        )
        .await
        .unwrap()
        .unwrap();
        start_foreground(&mut fixture, 5, "ordinary-turn-with-stopped-goal").await;
        tokio::time::timeout(REPLY_TIMEOUT, script.entered.notified())
            .await
            .unwrap();
        let before = goal_store::get_record("interrupt-origin-thread")
            .await
            .unwrap();
        assert_eq!(before.goal.as_ref().unwrap().status, status);
        let before_authorization =
            klaus_agent_core::goal::retained_context::instructions_from_rollout(
                &fixture
                    .store
                    .read_thread("interrupt-origin-thread")
                    .await
                    .unwrap()
                    .raw_items,
            );
        let (_, frames) = request(&mut fixture.client, 6, "turn/interrupt", json!({
            "sessionId":fixture.session_id,"submissionId":"stop-ordinary-stopped-goal","origin":"user-stop",
            "expectedTurnId":"ordinary-turn-with-stopped-goal"
        })).await;
        let settled =
            wait_interrupt(&mut fixture.client, frames, "stop-ordinary-stopped-goal").await;
        assert_eq!(
            settled["turnInterrupt"]["accepted"], true,
            "{status:?}: {settled}"
        );
        assert!(
            settled["turnInterrupt"].get("goalPauseError").is_none(),
            "{status:?}: {settled}"
        );
        tokio::time::timeout(REPLY_TIMEOUT, script.cancelled.notified())
            .await
            .unwrap();
        assert!(script.signals.lock().unwrap()[0].is_cancelled());
        assert_eq!(
            goal_store::get_record("interrupt-origin-thread")
                .await
                .unwrap(),
            before,
            "Stop of an ordinary turn must preserve every stored field for {status:?}"
        );
        let raw = fixture
            .store
            .read_thread("interrupt-origin-thread")
            .await
            .unwrap()
            .raw_items;
        let after_authorization =
            klaus_agent_core::goal::retained_context::instructions_from_rollout_with_receipts(
                "interrupt-origin-thread",
                &raw,
            )
            .await
            .unwrap();
        assert_eq!(
            after_authorization, before_authorization,
            "Stop must add no paused or resume authorization for {status:?}"
        );
        assert!(!raw.iter().any(|item| matches!(item, RolloutItem::Event(event)
            if event.event == "user_goal_control_attempt" || event.event == "user_goal_control_disposition")),
            "a stopped goal must not even begin a new pause control attempt");
        {
            let observed = script.cancellation_rows.lock().unwrap();
            assert_eq!(observed.len(), 1);
            assert_eq!(observed[0].1, before.goal);
            assert_eq!(observed[0].2, before_authorization.len());
        }
        assert!(fixture
            .session
            .goal_observation_runtime()
            .unwrap()
            .continuation_suppressed());
        assert_eq!(script.calls.load(Ordering::SeqCst), 1);
        fixture.automatic_release.add_permits(1);
        fixture.client.close().await;
    }
}

#[tokio::test]
async fn lazy_user_stop_keeps_stopped_goal_records_without_opening_a_writer_or_provider() {
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    for status in [
        ThreadGoalStatus::Paused,
        ThreadGoalStatus::Blocked,
        ThreadGoalStatus::UsageLimited,
        ThreadGoalStatus::BudgetLimited,
        ThreadGoalStatus::Complete,
    ] {
        let root = tempfile::tempdir().unwrap();
        let _config = ConfigGuard::new(root.path());
        let control = Arc::new(InstructionFlushControl::new());
        let store = Arc::new(ControlledStore {
            inner: Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
                dir: Some(root.path().join("sessions").to_string_lossy().into_owned()),
                archive_dir: None,
            })),
            control: control.clone(),
        });
        seed_saved_goal(store.as_ref(), "lazy-already-stopped").await;
        control.closed.notified().await;
        goal_store::update_goal(
            "lazy-already-stopped",
            klaus_agent_core::goal::types::GoalUpdate {
                status: Some(status),
                ..Default::default()
            },
            20,
        )
        .await
        .unwrap()
        .unwrap();
        let before = goal_store::get_record("lazy-already-stopped")
            .await
            .unwrap();
        let raw_before = store
            .read_thread("lazy-already-stopped")
            .await
            .unwrap()
            .raw_items;
        let creates = Arc::new(AtomicUsize::new(0));
        let count = creates.clone();
        let never_create: CreateSession = Arc::new(move |_| {
            count.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { panic!("Stop of a saved stopped goal must remain provider-free") })
        });
        let agent = make_agent_with_call_model_and_config(
            Box::new(|_, _| panic!("saved stopped-goal control must not infer")),
            Some(json!({"goal":true,"prewarm":false,"autoMemory":false,"autoDream":false})),
        );
        let lifecycle = crate::lifecycle::protocol_lifecycle(agent, Some(store.clone()));
        let mut client = TestClient::start_with(never_create, Some(lifecycle.clone()));
        client.initialize().await;
        let (started, _) = request(
            &mut client,
            2,
            "session/start",
            json!({"init":{"threadId":"lazy-already-stopped"}}),
        )
        .await;
        let (_,frames) = request(&mut client,3,"turn/interrupt",json!({
            "sessionId":started["result"]["sessionId"],"submissionId":"lazy-stopped-user-stop","origin":"user-stop"
        })).await;
        let settled = wait_interrupt(&mut client, frames, "lazy-stopped-user-stop").await;
        assert_eq!(
            settled["turnInterrupt"]["accepted"], true,
            "{status:?}: {settled}"
        );
        assert!(
            settled["turnInterrupt"].get("goalPauseError").is_none(),
            "{status:?}: {settled}"
        );
        assert_eq!(
            goal_store::get_record("lazy-already-stopped")
                .await
                .unwrap(),
            before
        );
        assert_eq!(
            store
                .read_thread("lazy-already-stopped")
                .await
                .unwrap()
                .raw_items,
            raw_before
        );
        // Exercise the saved service directly too: lazy preflight must not be
        // the only layer preventing a new user-origin pause writer.
        let handler = lifecycle.saved_goal_request.as_ref().unwrap();
        handler(
            "lazy-already-stopped".into(),
            klaus_agent_core::goal::service::SavedGoalRequest::Pause {
                captured: before.clone(),
                user_origin: true,
                foreground_submission_id: None,
                control_guard: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            goal_store::get_record("lazy-already-stopped")
                .await
                .unwrap(),
            before
        );
        assert_eq!(
            store
                .read_thread("lazy-already-stopped")
                .await
                .unwrap()
                .raw_items,
            raw_before
        );
        assert!(!control.writer_owned.load(Ordering::SeqCst));
        assert!(
            control.closed.notified().now_or_never().is_none(),
            "stopped-goal preflight must not acquire and release any saved writer"
        );
        assert_eq!(creates.load(Ordering::SeqCst), 0);
        client.close().await;
    }
}

#[path = "r20261017_goal_stop_intent_authority_facade.rs"]
mod stop_intent_authority;
