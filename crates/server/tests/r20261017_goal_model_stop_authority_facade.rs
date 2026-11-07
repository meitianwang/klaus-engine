//! Fresh explicit model controls may resume pursuit after an older Stop.

use super::*;

async fn model_stop_client(
    root: &std::path::Path,
    agent: Arc<Agent>,
    trace: Arc<GoalFixtureTrace>,
) -> (TestClient, Arc<dyn SessionStore>, Arc<Session>, String) {
    let store: Arc<dyn SessionStore> = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let captured = Arc::new(Mutex::new(None::<Arc<Session>>));
    let factory: CreateSession = {
        let agent = agent.clone();
        let store = store.clone();
        let captured = captured.clone();
        let trace = trace.clone();
        Arc::new(move |ctx| {
            let agent = agent.clone();
            let store = store.clone();
            let captured = captured.clone();
            let trace = trace.clone();
            Box::pin(async move {
                trace.note("factory.open.begin");
                let session = agent.open_session(store, StartSessionOptions {
                    id:ctx.init.thread_id, approval_handler:Some(ctx.approval_handler), elicit_handler:Some(ctx.elicit_handler),
                    mcp_elicitation_presenter:Some(ctx.mcp_elicitation_presenter),
                    config:Some(json!({"goal":true,"prewarm":false,"autoMemory":false,"autoDream":false})), ..Default::default()
                }).await.map_err(|error|Box::new(error)as BoxError)?;
                trace.note("factory.open.ready");
                if let Some(runtime) = session.goal_runtime() {
                    runtime
                        .testing_set_phase_observer(Some(Arc::new(move |phase| trace.note(phase))));
                }
                *captured.lock().unwrap() = Some(session.clone());
                Ok(session)
            })
        })
    };
    let lifecycle = crate::lifecycle::protocol_lifecycle(agent, Some(store.clone()));
    let mut client = TestClient::start_with(factory, Some(lifecycle));
    trace.note("initialize.begin");
    client.initialize().await;
    trace.note("initialize.ready");
    trace.note("session.start.submit");
    client.send(json!({"id":2,"method":"session/start","params":{"init":{"threadId":"model-stop-authority-thread"}}})).await;
    let (started, _) = client.response_for(2).await;
    trace.note("session.start.ack");
    let id = started["result"]["sessionId"].as_str().unwrap().to_owned();
    // Construct the persistent root without admitting any provider request.
    trace.note("materialize.submit");
    client
        .send(json!({"id":3,"method":"session/queueSnapshot","params":{"sessionId":id}}))
        .await;
    let (materialized, _) = client.response_for(3).await;
    trace.note("materialize.ack");
    assert!(materialized.get("error").is_none(), "{materialized}");
    assert!(
        materialized["result"]["queueSnapshot"]["items"]
            .as_array()
            .unwrap()
            .is_empty(),
        "provider-free materialization must not create queued input"
    );
    let session = captured.lock().unwrap().clone().unwrap();
    assert!(session.goal_runtime().is_some());
    assert!(
        session.goal_enabled(),
        "the factory already enables goal controls"
    );
    (client, store, session, id)
}

async fn interrupt_prior_work(
    client: &mut TestClient,
    session: &str,
    request_id: u64,
    trace: &GoalFixtureTrace,
    expected_turn_id: Option<&str>,
) -> Vec<Value> {
    trace.note("old.stop.submit");
    let mut params = json!({
        "sessionId":session,"submissionId":"old-idle-stop","origin":"user-stop"
    });
    if let Some(expected) = expected_turn_id {
        params["expectedTurnId"] = json!(expected);
    }
    client
        .send(json!({"id":request_id,"method":"turn/interrupt","params":params}))
        .await;
    let (ack, initial) = client.response_for(request_id).await;
    trace.note("idle.stop.ack");
    assert_eq!(ack["result"]["submissionId"], "old-idle-stop");
    let mut observed = initial.clone();
    let mut initial = VecDeque::from(initial);
    trace.note("old.stop.settled.await");
    tokio::time::timeout(REPLY_TIMEOUT, async {
        loop {
            let frame = match initial.pop_front() {
                Some(frame) => frame,
                None => {
                    let frame = client.next_message().await;
                    observed.push(frame.clone());
                    frame
                }
            };
            let event = &frame["params"]["event"];
            if event["submissionId"] == "old-idle-stop" && event.get("turnInterrupt").is_some() {
                trace.note("idle.stop.result.received");
                if event["turnInterrupt"]["accepted"] != true {
                    trace.note("idle.stop.result.refused");
                }
                assert_eq!(event["turnInterrupt"]["accepted"], true, "{event}");
                assert!(
                    event["turnInterrupt"].get("goalPauseError").is_none(),
                    "{event}"
                );
                break;
            }
        }
    })
    .await
    .expect("the real Stop must settle before the new provider request");
    trace.note("old.stop.settled");
    observed
}

async fn fresh_model_create_after_stop(had_completed_goal: bool) {
    let trace = GoalFixtureTrace::new(if had_completed_goal {
        "fresh-model-completed-stop"
    } else {
        "fresh-model-no-goal-stop"
    });
    trace.note("serial.await");
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    trace.note("serial.acquired");
    let root = tempfile::tempdir().unwrap();
    let _config = ConfigGuard::new(root.path());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let seed_entered = Arc::new(tokio::sync::Notify::new());
    let seed_release = Arc::new(tokio::sync::Semaphore::new(0));
    let _seed_release_on_drop = ReleaseGateOnDrop(seed_release.clone());
    let mut steps = Vec::new();
    if had_completed_goal {
        steps.extend([
            create("pre-stop-create", "The previously completed objective"),
            complete("pre-stop-complete"),
            Step::HeldFinal(
                "Previous objective complete",
                seed_entered.clone(),
                seed_release,
            ),
        ]);
    }
    steps.extend([
        create(
            "post-stop-create",
            "A fresh explicitly authorized objective",
        ),
        complete("post-stop-complete"),
        Step::Final("Fresh objective complete"),
    ]);
    let agent = scripted_goal_agent(steps, requests.clone());
    let (mut client, store, session, id) =
        model_stop_client(root.path(), agent, trace.clone()).await;
    assert_eq!(requests.lock().unwrap().len(), 0);
    let seed_frames = if had_completed_goal {
        trace.note("seed.turn.submit");
        let frames = start_named_turn(&mut client, &id, 10, "completed-before-stop").await;
        trace.note("seed.turn.ack");
        tokio::time::timeout(REPLY_TIMEOUT, seed_entered.notified())
            .await
            .expect(
                "the completed goal's final provider response must remain owned by the seed turn",
            );
        trace.note("seed.provider.held");
        Some(frames)
    } else {
        None
    };
    trace.note("seed.ready");
    let before = klaus_agent_core::goal::store::get_record("model-stop-authority-thread")
        .await
        .unwrap();
    if let Some(goal) = before.goal.as_ref() {
        assert_eq!(
            goal.status,
            klaus_agent_core::goal::ThreadGoalStatus::Complete
        );
    }
    let raw_before = store
        .read_thread("model-stop-authority-thread")
        .await
        .unwrap()
        .raw_items;
    let stop_frames = interrupt_prior_work(
        &mut client,
        &id,
        20,
        trace.as_ref(),
        had_completed_goal.then_some("completed-before-stop"),
    )
    .await;
    if let Some(mut frames) = seed_frames {
        frames.extend(stop_frames);
        let stopped_seed = finish_named_turn(&mut client, "completed-before-stop", frames).await;
        assert_eq!(stopped_seed["stopReason"], "aborted", "{stopped_seed}");
    }
    let runtime = session.goal_runtime().unwrap();
    assert!(
        runtime.continuation_suppressed(),
        "the actual old Stop must latch before a fresh provider request"
    );
    let mut expected_stopped = before.clone();
    if had_completed_goal {
        assert_eq!(
            expected_stopped.goal.as_ref().map(|goal| goal.status),
            Some(klaus_agent_core::goal::ThreadGoalStatus::Complete)
        );
        // Aborting the held seed retires its owned run marker after completion.
        expected_stopped.revision = expected_stopped.revision.checked_add(1).unwrap();
    }
    assert_eq!(
        klaus_agent_core::goal::store::get_record("model-stop-authority-thread")
            .await
            .unwrap(),
        expected_stopped
    );
    let raw_stopped = store
        .read_thread("model-stop-authority-thread")
        .await
        .unwrap()
        .raw_items;
    assert_eq!(
        klaus_agent_core::goal::retained_context::instructions_from_rollout(&raw_stopped),
        klaus_agent_core::goal::retained_context::instructions_from_rollout(&raw_before),
        "Stop without active work must invent no pause or resume authorization"
    );
    trace.note("fresh.turn.submit");
    let frames = start_named_turn(&mut client, &id, 30, "fresh-create-after-stop").await;
    trace.note("fresh.turn.ack");
    trace.note("fresh.turn.completed.await");
    assert_eq!(
        finish_named_turn(&mut client, "fresh-create-after-stop", frames).await["finalText"],
        "Fresh objective complete"
    );
    trace.note("fresh.turn.completed");
    let after = klaus_agent_core::goal::store::get_record("model-stop-authority-thread")
        .await
        .unwrap();
    let goal = after.goal.as_ref().unwrap();
    assert_eq!(
        goal.status,
        klaus_agent_core::goal::ThreadGoalStatus::Complete
    );
    assert_eq!(goal.objective, "A fresh explicitly authorized objective");
    if let Some(previous) = before.goal {
        assert_ne!(goal.goal_id, previous.goal_id);
    }
    assert!(
        !runtime.continuation_suppressed(),
        "a genuinely fresh model create owns resume authority"
    );
    let raw = store
        .read_thread("model-stop-authority-thread")
        .await
        .unwrap()
        .raw_items;
    assert!(!result_for(&raw, "post-stop-create").is_error);
    assert!(
        !result_for(&raw, "post-stop-complete").is_error,
        "the subsequent provider request must bind and complete the newly created generation"
    );
    assert_eq!(
        requests.lock().unwrap().len(),
        if had_completed_goal { 6 } else { 3 }
    );
    trace.note("client.close.begin");
    client.close().await;
    trace.note("client.close.end");
}

#[tokio::test]
async fn a_fresh_model_create_after_a_no_goal_stop_clears_the_old_latch() {
    fresh_model_create_after_stop(false).await;
}
#[tokio::test]
async fn a_fresh_model_create_after_a_completed_goal_stop_binds_its_new_generation() {
    fresh_model_create_after_stop(true).await;
}

struct ModelControlRelease {
    runtime: Arc<klaus_agent_core::goal::runtime::GoalRuntime>,
    create: Arc<tokio::sync::Semaphore>,
    stop: Arc<tokio::sync::Semaphore>,
    provider: Arc<tokio::sync::Semaphore>,
}
impl Drop for ModelControlRelease {
    fn drop(&mut self) {
        self.runtime.testing_before_model_create(None);
        self.runtime.testing_before_stop_snapshot(None);
        self.create.add_permits(1);
        self.stop.add_permits(1);
        self.provider.add_permits(1);
    }
}
struct ControlClock(tokio::task::JoinHandle<()>);
impl ControlClock {
    fn new() -> Self {
        tokio::time::pause();
        Self(tokio::spawn(async {
            loop {
                tokio::task::yield_now().await;
            }
        }))
    }
}
impl Drop for ControlClock {
    fn drop(&mut self) {
        self.0.abort();
        tokio::time::resume();
    }
}

#[tokio::test]
async fn an_executing_model_create_cannot_cross_a_newer_stop_epoch_or_clear_its_latch() {
    let trace = GoalFixtureTrace::new("old-model-create-newer-stop");
    trace.note("serial.await");
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    trace.note("serial.acquired");
    let root = tempfile::tempdir().unwrap();
    let _config = ConfigGuard::new(root.path());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let provider_signals = Arc::new(Mutex::new(Vec::new()));
    let second_provider = Arc::new(tokio::sync::Notify::new());
    let provider_release = Arc::new(tokio::sync::Semaphore::new(0));
    let agent = {
        let requests = requests.clone();
        let signals = provider_signals.clone();
        let second = second_provider.clone();
        let release = provider_release.clone();
        make_agent_with_call_model_and_config(
            Box::new(move |request, signal| {
                let index = {
                    let mut requests = requests.lock().unwrap();
                    let index = requests.len();
                    requests.push(request);
                    index
                };
                signals.lock().unwrap().push(signal.clone());
                if index == 0 {
                    let input = runtime_json::parse(&RuntimeString::from(
                        r#"{"objective":"The old request must not create after Stop"}"#,
                    ))
                    .unwrap();
                    let mut events = vec![ev::message_start(None)];
                    events.extend(ev::tool_use("create-across-stop", "create_goal", &input));
                    events.push(ev::stop(
                        Some(ModelStopReason::ToolUse),
                        Some(Usage {
                            input_tokens: 0,
                            output_tokens: 0,
                            cache_read_input_tokens: 0,
                            cache_creation_input_tokens: 0,
                            total_tokens: None,
                        }),
                    ));
                    return Box::pin(futures::stream::iter(events));
                }
                assert_eq!(
                    index, 1,
                    "only the failed tool's ordinary follow-up may run"
                );
                second.notify_one();
                let release = release.clone();
                Box::pin(futures::stream::iter(vec![ev::message_start(None), ev::text("Old create was refused")])
                .chain(futures::stream::once(async move {
                    tokio::select! {
                        permit=release.acquire()=> { permit.unwrap().forget(); Some(ev::stop(Some(ModelStopReason::EndTurn), Some(Usage {
                            input_tokens:0,output_tokens:0,cache_read_input_tokens:0,cache_creation_input_tokens:0,total_tokens:None,
                        }))) },
                        _=signal.cancelled()=>None,
                    }
                }).filter_map(futures::future::ready)))
            }),
            Some(json!({"goal":true,"prewarm":false,"autoMemory":false,"autoDream":false})),
        )
    };
    let (mut client, store, session, id) =
        model_stop_client(root.path(), agent, trace.clone()).await;
    let runtime = session.goal_runtime().unwrap();
    let create_entered = Arc::new(tokio::sync::Notify::new());
    let stop_entered = Arc::new(tokio::sync::Notify::new());
    let create_release = Arc::new(tokio::sync::Semaphore::new(0));
    let stop_release = Arc::new(tokio::sync::Semaphore::new(0));
    runtime.testing_before_model_create(Some({
        let entered = create_entered.clone();
        let release = create_release.clone();
        Arc::new(move || {
            let entered = entered.clone();
            let release = release.clone();
            Box::pin(async move {
                entered.notify_one();
                release.acquire().await.unwrap().forget();
            })
        })
    }));
    runtime.testing_before_stop_snapshot(Some({
        let entered = stop_entered.clone();
        let release = stop_release.clone();
        Arc::new(move || {
            let entered = entered.clone();
            let release = release.clone();
            Box::pin(async move {
                entered.notify_one();
                release.acquire().await.unwrap().forget();
            })
        })
    }));
    let cleanup = ModelControlRelease {
        runtime: runtime.clone(),
        create: create_release.clone(),
        stop: stop_release.clone(),
        provider: provider_release,
    };
    let before = klaus_agent_core::goal::store::get_record("model-stop-authority-thread")
        .await
        .unwrap();
    assert!(before.goal.is_none());
    trace.note("old.turn.submit");
    let mut frames = start_named_turn(&mut client, &id, 10, "provider-before-newer-stop").await;
    trace.note("old.turn.ack");
    trace.note("model.create.hook.await");
    tokio::time::timeout(REPLY_TIMEOUT, create_entered.notified())
        .await
        .expect("the real create_goal handler must hold its captured provider identity");
    trace.note("model.create.hook.observed");
    assert_eq!(requests.lock().unwrap().len(), 1);
    assert!(!provider_signals.lock().unwrap()[0].is_cancelled());
    let clock = ControlClock::new();
    trace.note("clock.frozen");
    trace.note("new.stop.submit");
    client.send(json!({"id":20,"method":"turn/interrupt","params":{
        "sessionId":id,"submissionId":"newer-stop-with-old-create","origin":"user-stop","expectedTurnId":"provider-before-newer-stop"
    }})).await;
    let (ack, observed) = client.response_for(20).await;
    trace.note("new.stop.ack");
    assert_eq!(ack["result"]["submissionId"], "newer-stop-with-old-create");
    frames.extend(observed);
    trace.note("new.stop.hook.await");
    stop_entered.notified().await;
    trace.note("new.stop.hook.observed");
    assert!(
        !provider_signals.lock().unwrap()[0].is_cancelled(),
        "the stale mutation is rejected by pursuit epoch before physical provider cancellation"
    );
    runtime.testing_before_model_create(None);
    trace.note("model.create.release");
    create_release.add_permits(1);
    trace.note("second.provider.await");
    second_provider.notified().await;
    trace.note("second.provider.observed");
    assert!(!provider_signals.lock().unwrap()[1].is_cancelled());
    session.flush_rollout().await.unwrap();
    let saved = store
        .read_thread("model-stop-authority-thread")
        .await
        .unwrap();
    let failed = result_for(&saved.raw_items, "create-across-stop");
    assert!(
        failed.is_error,
        "the executing real tool must return an actual failed result"
    );
    let text = failed
        .content
        .iter()
        .filter_map(|block| match block {
            OutputBlock::Text(text) => Some(text.text.try_to_utf8().unwrap()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(text.contains("intent changed"), "{text}");
    assert_eq!(
        klaus_agent_core::goal::store::get_record("model-stop-authority-thread")
            .await
            .unwrap(),
        before,
        "the rejected old create must persist neither a goal nor a control revision"
    );
    runtime.testing_before_stop_snapshot(None);
    trace.note("new.stop.release");
    stop_release.add_permits(1);
    trace.note("new.stop.settled.await");
    tokio::time::timeout(REPLY_TIMEOUT, async {
        loop {
            let frame = client.next_message().await;
            assert!(frames.len() < 256, "unexpected Stop frame flood");
            let settled = frame["params"]["event"]["submissionId"] == "newer-stop-with-old-create"
                && frame["params"]["event"].get("turnInterrupt").is_some();
            frames.push(frame.clone());
            if settled {
                assert_eq!(
                    frame["params"]["event"]["turnInterrupt"]["accepted"], true,
                    "{frame}"
                );
                break;
            }
        }
    })
    .await
    .expect("the newer Stop must settle after its released snapshot");
    trace.note("new.stop.settled");
    assert!(
        runtime.continuation_suppressed(),
        "the old tool cannot clear the newer Stop latch"
    );
    assert!(provider_signals
        .lock()
        .unwrap()
        .iter()
        .all(|signal| signal.is_cancelled()));
    drop(clock);
    trace.note("old.turn.completed.await");
    let result = finish_named_turn(&mut client, "provider-before-newer-stop", frames).await;
    trace.note("old.turn.completed");
    assert_eq!(result["stopReason"], "aborted", "{result}");
    assert_eq!(
        klaus_agent_core::goal::store::get_record("model-stop-authority-thread")
            .await
            .unwrap(),
        before
    );
    assert_eq!(requests.lock().unwrap().len(), 2);
    drop(cleanup);
    trace.note("client.close.begin");
    client.close().await;
    trace.note("client.close.end");
}
