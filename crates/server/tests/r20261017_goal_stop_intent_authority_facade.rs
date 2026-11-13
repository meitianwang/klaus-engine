//! Physical cancellation of an old turn does not revoke newer goal intent.

use super::*;
use klaus_agent_core::goal::runtime::GoalRuntime;

#[derive(Clone, Copy, Debug)]
enum Window {
    BeforeSnapshot,
    AfterCommit,
}
#[derive(Clone, Copy, Debug)]
enum Intent {
    Active,
    NewGeneration,
}
struct Release {
    runtime: Arc<GoalRuntime>,
    barrier: Arc<tokio::sync::Semaphore>,
    automatic: Arc<tokio::sync::Semaphore>,
}
impl Drop for Release {
    fn drop(&mut self) {
        self.runtime.testing_before_stop_snapshot(None);
        self.runtime.testing_before_stop_cancel(None);
        self.runtime.testing_unfreeze_elapsed();
        self.barrier.add_permits(1);
        self.automatic.add_permits(1);
    }
}

async fn newer_goal_intent_during_stop(window: Window, intent: Intent) {
    let trace = GoalFixtureTrace::new(&format!("f4-{window:?}-{intent:?}"));
    trace.note("serial.await");
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    trace.note("serial.acquired");
    let root = tempfile::tempdir().unwrap();
    let _config = ConfigGuard::new(root.path());
    let script = ProviderScript::new();
    trace.note("fixture.begin");
    let mut fixture = fixture(
        root.path(),
        script.clone(),
        Arc::new(InstructionFlushControl::new()),
        json!({"goal":true,"prewarm":false,"autoMemory":false,"autoDream":false}),
    )
    .await;
    trace.note("fixture.ready");
    trace.note("foreground.submit");
    start_foreground(&mut fixture, 4, "same-foreground").await;
    trace.note("foreground.ack");
    trace.note("provider.enter.await");
    tokio::time::timeout(REPLY_TIMEOUT, script.entered.notified())
        .await
        .unwrap();
    trace.note("provider.entered");
    let runtime = fixture.session.goal_runtime().unwrap();
    runtime.testing_freeze_elapsed();
    trace.note("initial.active.begin");
    fixture
        .session
        .set_thread_goal(None, Some("active".into()), None)
        .await
        .unwrap();
    trace.note("initial.active.ready");
    let original = goal_store::get_record("interrupt-origin-thread")
        .await
        .unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let barrier = Arc::new(tokio::sync::Semaphore::new(0));
    let hook: klaus_agent_core::goal::runtime::GoalAdmissionBarrier = {
        let entered = entered.clone();
        let barrier = barrier.clone();
        let trace = trace.clone();
        Arc::new(move || {
            let entered = entered.clone();
            let barrier = barrier.clone();
            let trace = trace.clone();
            Box::pin(async move {
                trace.note("stop.hook.enter");
                entered.notify_one();
                trace.note("stop.hook.release.await");
                barrier.acquire().await.unwrap().forget();
                trace.note("stop.hook.released");
            })
        })
    };
    match window {
        Window::BeforeSnapshot => runtime.testing_before_stop_snapshot(Some(hook)),
        Window::AfterCommit => runtime.testing_before_stop_cancel(Some(hook)),
    }
    let clock = FrozenClock::new();
    trace.note("clock.frozen");
    let release = Release {
        runtime: runtime.clone(),
        barrier: barrier.clone(),
        automatic: fixture.automatic_release.clone(),
    };
    trace.note("stop.submit");
    fixture.client.send(json!({"id":5,"method":"turn/interrupt","params":{
        "sessionId":fixture.session_id,"submissionId":"older-stop","origin":"user-stop","expectedTurnId":"same-foreground"
    }})).await;
    trace.note("stop.ack");
    trace.note("stop.hook.await");
    entered.notified().await;
    trace.note("stop.hook.observed");
    assert_eq!(script.calls.load(Ordering::SeqCst), 1);
    assert!(
        !script.signals.lock().unwrap()[0].is_cancelled(),
        "the exact same foreground is still held"
    );
    let paused = goal_store::get_record("interrupt-origin-thread")
        .await
        .unwrap();
    assert_eq!(
        paused.goal.as_ref().unwrap().status,
        match window {
            Window::BeforeSnapshot => ThreadGoalStatus::Active,
            Window::AfterCommit => ThreadGoalStatus::Paused,
        }
    );
    match intent {
        Intent::Active => {
            trace.note("new.active.submit");
            let (updated, _) = request(&mut fixture.client, 6, "session/setGoal", json!({
                "sessionId":fixture.session_id,"status":"active","origin":"user","expectedGoalId":original.goal.as_ref().unwrap().goal_id.try_to_utf8().unwrap()
            })).await;
            trace.note("new.active.ack");
            assert!(updated.get("error").is_none(), "{updated}");
        }
        Intent::NewGeneration => {
            trace.note("new.clear.submit");
            let (cleared, _) = request(&mut fixture.client, 6, "session/clearGoal", json!({
                "sessionId":fixture.session_id,"origin":"user","expectedGoalId":original.goal.as_ref().unwrap().goal_id.try_to_utf8().unwrap()
            })).await;
            trace.note("new.clear.ack");
            assert_eq!(cleared["result"]["goalCleared"], true, "{cleared}");
            trace.note("new.create.submit");
            let (created, _) = request(&mut fixture.client, 7, "session/setGoal", json!({
                "sessionId":fixture.session_id,"objective":"The later explicit objective must remain authorized","status":"active","origin":"user","expectedGoalId":null
            })).await;
            trace.note("new.create.ack");
            assert!(created.get("error").is_none(), "{created}");
        }
    }
    trace.note("new.snapshot.begin");
    let newer = goal_store::get_record("interrupt-origin-thread")
        .await
        .unwrap();
    assert_eq!(
        newer.goal.as_ref().unwrap().status,
        ThreadGoalStatus::Active
    );
    assert_eq!(
        newer.goal.as_ref().unwrap().goal_id == original.goal.as_ref().unwrap().goal_id,
        matches!(intent, Intent::Active)
    );
    assert!(!runtime.continuation_suppressed());
    trace.note("new.snapshot.ready");
    trace.note("new.authorization.read");
    let authorized_before = instructions(
        &fixture
            .store
            .read_thread("interrupt-origin-thread")
            .await
            .unwrap()
            .raw_items,
    );
    trace.note("new.authorization.ready");
    runtime.testing_before_stop_snapshot(None);
    runtime.testing_before_stop_cancel(None);
    trace.note("stop.release");
    barrier.add_permits(1);
    trace.note("stop.settled.await");
    let (ack, frames) = fixture.client.response_for(5).await;
    assert_eq!(ack["result"]["submissionId"], "older-stop");
    let settled = wait_interrupt(&mut fixture.client, frames, "older-stop").await;
    trace.note("stop.settled");
    assert_eq!(
        settled["turnInterrupt"]["accepted"], true,
        "the physical captured turn can still be cancelled: {settled}"
    );
    assert!(script.signals.lock().unwrap()[0].is_cancelled());
    assert_eq!(
        goal_store::get_record("interrupt-origin-thread")
            .await
            .unwrap(),
        newer,
        "the older Stop cannot pause or charge the newer intent in {window:?}/{intent:?}"
    );
    assert!(
        !runtime.continuation_suppressed(),
        "the older Stop cannot latch newer pursuit in {window:?}/{intent:?}"
    );
    let raw = fixture
        .store
        .read_thread("interrupt-origin-thread")
        .await
        .unwrap()
        .raw_items;
    assert_eq!(
        instructions(&raw),
        authorized_before,
        "the late rejected pause must not add authority after the new intent"
    );
    let restored =
        klaus_agent_core::goal::retained_context::instructions_from_rollout_with_receipts(
            "interrupt-origin-thread",
            &raw,
        )
        .await
        .unwrap();
    assert_eq!(
        restored,
        klaus_agent_core::goal::retained_context::instructions_from_rollout(&raw)
    );
    if matches!(window, Window::BeforeSnapshot) {
        assert!(
            settled["turnInterrupt"]["goalPauseError"].is_string(),
            "the rejected old control remains explicit: {settled}"
        );
    }
    // Pause only after checking the newer intent. This drains the fixture's
    // independent idle barrier without weakening any Stop ownership assertion.
    trace.note("cleanup.pause.submit");
    let (paused, _) = request(&mut fixture.client, 8, "session/setGoal", json!({
        "sessionId":fixture.session_id,"status":"paused","origin":"user","expectedGoalId":newer.goal.as_ref().unwrap().goal_id.try_to_utf8().unwrap()
    })).await;
    trace.note("cleanup.pause.ack");
    assert!(paused.get("error").is_none(), "{paused}");
    trace.note("cleanup.release");
    drop(release);
    drop(clock);
    trace.note("client.close.begin");
    fixture.client.close().await;
    trace.note("client.close.end");
}

#[tokio::test]
async fn a_new_active_intent_before_the_old_stop_snapshot_survives_cancellation() {
    newer_goal_intent_during_stop(Window::BeforeSnapshot, Intent::Active).await;
}
#[tokio::test]
async fn a_new_generation_before_the_old_stop_snapshot_survives_cancellation() {
    newer_goal_intent_during_stop(Window::BeforeSnapshot, Intent::NewGeneration).await;
}
#[tokio::test]
async fn a_new_active_intent_after_pause_commit_survives_the_old_final_latch() {
    newer_goal_intent_during_stop(Window::AfterCommit, Intent::Active).await;
}
#[tokio::test]
async fn a_new_generation_after_pause_commit_survives_the_old_final_latch() {
    newer_goal_intent_during_stop(Window::AfterCommit, Intent::NewGeneration).await;
}

struct ReleaseSaved(Arc<tokio::sync::Semaphore>);
impl Drop for ReleaseSaved {
    fn drop(&mut self) {
        self.0.add_permits(1);
    }
}

async fn lazy_stop_keeps_its_captured_intent(origin: &'static str, had_goal: bool) {
    use klaus_agent_core::goal::service::SavedGoalRequest;
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let _config = ConfigGuard::new(root.path());
    let store: Arc<dyn SessionStore> = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.path().join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    seed_saved_goal(store.as_ref(), "lazy-captured-stop").await;
    if !had_goal {
        goal_store::clear_goal("lazy-captured-stop").await.unwrap();
    }
    let initial = goal_store::get_record("lazy-captured-stop").await.unwrap();
    let provider_calls = Arc::new(AtomicUsize::new(0));
    let calls = provider_calls.clone();
    let agent = make_agent_with_call_model_and_config(
        Box::new(move |_, _| {
            calls.fetch_add(1, Ordering::SeqCst);
            panic!("cold control and paused goal editing must never resolve a provider")
        }),
        Some(json!({"goal":true,"prewarm":false,"autoMemory":false,"autoDream":false})),
    );
    let base_lifecycle = crate::lifecycle::protocol_lifecycle(agent.clone(), Some(store.clone()));
    let mut lazy_lifecycle = base_lifecycle.clone();
    let handler = lazy_lifecycle.saved_goal_request.take().unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let _release_on_failure = ReleaseSaved(release.clone());
    let snapshots = Arc::new(Mutex::new(Vec::new()));
    lazy_lifecycle.saved_goal_request = Some({
        let entered = entered.clone();
        let release = release.clone();
        let snapshots = snapshots.clone();
        Arc::new(move |thread, request| {
            let handler = handler.clone();
            let entered = entered.clone();
            let release = release.clone();
            let snapshots = snapshots.clone();
            Box::pin(async move {
                if let SavedGoalRequest::Pause { captured, .. } = &request {
                    snapshots.lock().unwrap().push(captured.clone());
                    entered.notify_one();
                    release.acquire().await.unwrap().forget();
                }
                handler(thread, request).await
            })
        })
    });
    let lazy_creates = Arc::new(AtomicUsize::new(0));
    let creates = lazy_creates.clone();
    let never_create: CreateSession = Arc::new(move |_| {
        creates.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { panic!("lazy Stop must not construct a core Session") })
    });
    let mut lazy = TestClient::start_with(never_create, Some(lazy_lifecycle));
    lazy.initialize().await;
    let (started, _) = request(
        &mut lazy,
        2,
        "session/start",
        json!({"init":{"threadId":"lazy-captured-stop"}}),
    )
    .await;
    let session_id = started["result"]["sessionId"].clone();
    let clock = FrozenClock::new();
    lazy.send(json!({"id":3,"method":"turn/interrupt","params":{
        "sessionId":session_id,"submissionId":"captured-lazy-stop","origin":origin
    }}))
    .await;
    entered.notified().await;
    assert_eq!(
        *snapshots.lock().unwrap(),
        vec![initial.clone()],
        "the callback must receive the exact first transaction's capture"
    );
    assert_eq!(lazy_creates.load(Ordering::SeqCst), 0);

    // A separate endpoint performs real named control while the first endpoint
    // still owns no Session or rollout writer. Its new paused generation then
    // releases its writer before the older saved request is allowed to continue.
    let other_creates = Arc::new(AtomicUsize::new(0));
    let factory: CreateSession = {
        let agent = agent.clone();
        let store = store.clone();
        let creates = other_creates.clone();
        Arc::new(move |ctx| {
            let agent = agent.clone();
            let store = store.clone();
            let creates = creates.clone();
            Box::pin(async move {
                creates.fetch_add(1, Ordering::SeqCst);
                agent.resume_session(store, "lazy-captured-stop", StartSessionOptions {
                    id:ctx.init.thread_id, approval_handler:Some(ctx.approval_handler), elicit_handler:Some(ctx.elicit_handler),
                    mcp_elicitation_presenter:Some(ctx.mcp_elicitation_presenter),
                    config:Some(json!({"goal":true,"prewarm":false,"autoMemory":false,"autoDream":false})), ..Default::default()
                }).await.map_err(|error|Box::new(error)as BoxError)
            })
        })
    };
    let mut other = TestClient::start_with(factory, Some(base_lifecycle));
    other.initialize().await;
    let (cleared, _) = request(
        &mut other,
        2,
        "session/clearGoal",
        json!({"threadId":"lazy-captured-stop","origin":"user"}),
    )
    .await;
    assert_eq!(cleared["result"]["goalCleared"], true, "{cleared}");
    let (started, _) = request(
        &mut other,
        3,
        "session/start",
        json!({"init":{"threadId":"lazy-captured-stop"}}),
    )
    .await;
    let (created, _) = request(&mut other, 4, "session/setGoal", json!({
        "sessionId":started["result"]["sessionId"],"objective":"The later endpoint owns this new intent","status":"paused","origin":"user","expectedGoalId":null
    })).await;
    assert!(created.get("error").is_none(), "{created}");
    other.close().await;
    let newer = goal_store::get_record("lazy-captured-stop").await.unwrap();
    assert_eq!(
        newer.goal.as_ref().unwrap().status,
        ThreadGoalStatus::Paused
    );
    assert_ne!(
        newer.goal.as_ref().map(|goal| &goal.goal_id),
        initial.goal.as_ref().map(|goal| &goal.goal_id)
    );
    let raw_before = store
        .read_thread("lazy-captured-stop")
        .await
        .unwrap()
        .raw_items;
    release.add_permits(1);
    let (_, frames) = lazy.response_for(3).await;
    let settled = wait_interrupt(&mut lazy, frames, "captured-lazy-stop").await;
    assert_eq!(
        settled["turnInterrupt"]["accepted"], true,
        "the lazy endpoint remains physically idle: {settled}"
    );
    assert_eq!(
        settled["turnInterrupt"]["goalPauseError"].is_string(),
        had_goal,
        "{settled}"
    );
    assert_eq!(
        goal_store::get_record("lazy-captured-stop").await.unwrap(),
        newer,
        "an older cold control must neither adopt nor rewrite the new generation"
    );
    let raw_after = store
        .read_thread("lazy-captured-stop")
        .await
        .unwrap()
        .raw_items;
    assert_eq!(
        instructions(&raw_after),
        instructions(&raw_before),
        "a rejected or absent capture grants no pause authorization"
    );
    assert_eq!(
        klaus_agent_core::goal::retained_context::instructions_from_rollout_with_receipts(
            "lazy-captured-stop",
            &raw_after
        )
        .await
        .unwrap(),
        klaus_agent_core::goal::retained_context::instructions_from_rollout(&raw_before)
    );
    if !had_goal {
        assert_eq!(
            raw_after, raw_before,
            "a captured absence must not acquire a saved writer or append evidence"
        );
    }
    assert_eq!(lazy_creates.load(Ordering::SeqCst), 0);
    assert_eq!(other_creates.load(Ordering::SeqCst), 1);
    assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
    drop(clock);
    lazy.close().await;
}

#[tokio::test]
async fn cold_user_stop_rejects_a_new_generation_after_its_first_capture() {
    lazy_stop_keeps_its_captured_intent("user-stop", true).await;
}
#[tokio::test]
async fn cold_system_stop_rejects_a_new_generation_after_its_first_capture() {
    lazy_stop_keeps_its_captured_intent("system", true).await;
}
#[tokio::test]
async fn cold_user_stop_with_no_goal_does_not_adopt_a_later_generation() {
    lazy_stop_keeps_its_captured_intent("user-stop", false).await;
}
#[tokio::test]
async fn cold_system_stop_with_no_goal_does_not_adopt_a_later_generation() {
    lazy_stop_keeps_its_captured_intent("system", false).await;
}

struct ReleaseContendedOwner {
    runtime: Arc<GoalRuntime>,
    provider: Arc<tokio::sync::Semaphore>,
    automatic: Arc<tokio::sync::Semaphore>,
}
impl Drop for ReleaseContendedOwner {
    fn drop(&mut self) {
        self.runtime
            .testing_before_automatic_provider_admission(None);
        self.runtime.testing_unfreeze_elapsed();
        self.provider.add_permits(1);
        self.automatic.add_permits(1);
    }
}

async fn cold_stop_refuses_a_live_writer(origin: &'static str) {
    use klaus_agent_core::goal::service::SavedGoalRequest;
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let _config = ConfigGuard::new(root.path());
    let script = ProviderScript::new();
    let control = Arc::new(InstructionFlushControl::new());
    let diagnostic_owner = klaus_agent_core::goal::testing_phase::Span::start(
        "coldStop.ownerFixture",
        &format!("origin={origin}"),
    );
    let mut owner = fixture(
        root.path(),
        script.clone(),
        control.clone(),
        json!({"goal":true,"prewarm":false,"autoMemory":false,"autoDream":false}),
    )
    .await;
    diagnostic_owner.finish("ok");
    let (started, mut owner_frames) = request(&mut owner.client, 4, "turn/start", json!({
        "sessionId":owner.session_id,"submissionId":"actual-live-writer-owner","input":"Hold the actual provider while its writer remains owned"
    })).await;
    assert_eq!(started["result"]["turnId"], "actual-live-writer-owner");
    tokio::time::timeout(REPLY_TIMEOUT, async {
        loop {
            if owner_frames.iter().any(|frame| {
                frame["params"]["event"]["event"]["type"] == "text.delta"
                    && frame["params"]["event"]["event"]["text"] == "Original foreground finished"
            }) {
                break;
            }
            assert!(owner_frames.len() < 128, "unexpected owner frame flood");
            owner_frames.push(owner.client.next_message().await);
        }
    })
    .await
    .expect("the real owner must poll its provider stream before the contention snapshot");
    assert_eq!(script.calls.load(Ordering::SeqCst), 1);
    let runtime = owner.session.goal_runtime().unwrap();
    runtime.testing_freeze_elapsed();
    let cleanup = ReleaseContendedOwner {
        runtime: runtime.clone(),
        provider: script.releases[0].clone(),
        automatic: owner.automatic_release.clone(),
    };
    owner
        .session
        .set_thread_goal(None, Some("active".into()), None)
        .await
        .unwrap();
    owner.session.flush_rollout().await.unwrap();
    let before = goal_store::get_record("interrupt-origin-thread")
        .await
        .unwrap();
    assert_eq!(
        before.goal.as_ref().unwrap().status,
        ThreadGoalStatus::Active
    );
    let raw_before = owner
        .store
        .read_thread("interrupt-origin-thread")
        .await
        .unwrap()
        .raw_items;
    let live_before = owner.session.history();
    assert!(
        control.writer_owned.load(Ordering::SeqCst),
        "the real Jsonl writer lease is still live"
    );
    assert!(!script.signals.lock().unwrap()[0].is_cancelled());

    let lazy_creates = Arc::new(AtomicUsize::new(0));
    let provider_calls = Arc::new(AtomicUsize::new(0));
    let calls = provider_calls.clone();
    let agent = make_agent_with_call_model_and_config(
        Box::new(move |_, _| {
            calls.fetch_add(1, Ordering::SeqCst);
            panic!("a contended saved Stop must never resolve a provider")
        }),
        Some(json!({"goal":true,"prewarm":false,"autoMemory":false,"autoDream":false})),
    );
    let mut lifecycle = crate::lifecycle::protocol_lifecycle(agent, Some(owner.store.clone()));
    let handler = lifecycle.saved_goal_request.take().unwrap();
    let refusals = Arc::new(Mutex::new(Vec::new()));
    let captures = Arc::new(Mutex::new(Vec::new()));
    lifecycle.saved_goal_request = Some({
        let refusals = refusals.clone();
        let captures = captures.clone();
        Arc::new(move |thread, request| {
            let handler = handler.clone();
            let refusals = refusals.clone();
            let captures = captures.clone();
            Box::pin(async move {
                let pause = matches!(&request, SavedGoalRequest::Pause { .. });
                if let SavedGoalRequest::Pause { captured, .. } = &request {
                    captures.lock().unwrap().push(captured.clone());
                }
                let diagnostic_handler = klaus_agent_core::goal::testing_phase::Span::start(
                    "coldStop.savedGoalHandler",
                    &format!("thread={thread:?} pause={pause}"),
                );
                let result = handler(thread, request).await;
                diagnostic_handler.finish(if result.is_ok() { "ok" } else { "error" });
                if pause {
                    let error = result
                        .as_ref()
                        .expect_err("the real owned writer must refuse cold acquisition");
                    let error = error
                        .downcast_ref::<std::io::Error>()
                        .expect("the saved service must retain the writer's I/O refusal");
                    refusals.lock().unwrap().push(error.kind());
                }
                result
            })
        })
    });
    let creates = lazy_creates.clone();
    let never_create: CreateSession = Arc::new(move |_| {
        creates.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { panic!("the cold endpoint must not acquire a core Session") })
    });
    klaus_agent_core::goal::testing_phase::mark(
        "coldStop.writerOwned",
        &format!(
            "origin={origin} writer_owned={} owner_provider_requests={}",
            control.writer_owned.load(Ordering::SeqCst),
            script.calls.load(Ordering::SeqCst)
        ),
    );
    let mut lazy = TestClient::start_with(never_create, Some(lifecycle));
    lazy.initialize().await;
    let (started, _) = request(
        &mut lazy,
        2,
        "session/start",
        json!({"init":{"threadId":"interrupt-origin-thread"}}),
    )
    .await;
    klaus_agent_core::goal::testing_phase::mark(
        "coldStop.dispatch",
        &format!(
            "origin={origin} writer_owned={} lazy_creates={} provider_requests={}",
            control.writer_owned.load(Ordering::SeqCst),
            lazy_creates.load(Ordering::SeqCst),
            provider_calls.load(Ordering::SeqCst)
        ),
    );
    let (ack,frames)=request(&mut lazy,3,"turn/interrupt",json!({
        "sessionId":started["result"]["sessionId"],"submissionId":"contended-cold-stop","origin":origin
    })).await;
    assert_eq!(ack["result"]["submissionId"], "contended-cold-stop");
    let settled = wait_interrupt(&mut lazy, frames, "contended-cold-stop").await;
    // Physical acceptance concerns this idle endpoint. The explicit pause error
    // is required: an ACK or accepted=true must never imply a confirmed pause.
    assert_eq!(settled["turnInterrupt"]["accepted"], true, "{settled}");
    assert!(
        settled["turnInterrupt"]["goalPauseError"]
            .as_str()
            .is_some_and(|message| message.contains("locked by another writer")),
        "the final outcome must expose the actual writer refusal: {settled}"
    );
    assert!(
        settled["turnInterrupt"].get("interruptedTurnId").is_none(),
        "the other endpoint is never the cancellation target: {settled}"
    );
    assert_eq!(
        *refusals.lock().unwrap(),
        vec![std::io::ErrorKind::WouldBlock]
    );
    assert_eq!(*captures.lock().unwrap(), vec![before.clone()]);
    owner.session.flush_rollout().await.unwrap();
    assert_eq!(
        goal_store::get_record("interrupt-origin-thread")
            .await
            .unwrap(),
        before,
        "writer contention must refuse before SQL or accounting changes"
    );
    assert_eq!(
        owner
            .store
            .read_thread("interrupt-origin-thread")
            .await
            .unwrap()
            .raw_items,
        raw_before,
        "a refused writer cannot append an attempted or accepted authorization"
    );
    assert_eq!(owner.session.history(), live_before);
    assert!(control.writer_owned.load(Ordering::SeqCst));
    assert!(
        !script.signals.lock().unwrap()[0].is_cancelled(),
        "the real owner continues running"
    );
    assert!(
        !runtime.continuation_suppressed(),
        "cold refusal cannot latch another endpoint's runtime"
    );
    assert_eq!(script.calls.load(Ordering::SeqCst), 1);
    assert_eq!(lazy_creates.load(Ordering::SeqCst), 0);
    assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
    lazy.close().await;

    owner
        .session
        .set_thread_goal(None, Some("paused".into()), None)
        .await
        .unwrap();
    script.releases[0].add_permits(1);
    wait_for_goal_completion(
        &mut owner.client,
        owner_frames,
        "Original foreground finished",
    )
    .await;
    drop(cleanup);
    owner.client.close().await;
    assert!(
        !control.writer_owned.load(Ordering::SeqCst),
        "the real owner's lease releases during cleanup"
    );
}

#[tokio::test]
async fn cold_user_stop_refuses_a_live_writer_without_touching_its_foreground() {
    cold_stop_refuses_a_live_writer("user-stop").await;
}
#[tokio::test]
async fn cold_system_stop_refuses_a_live_writer_without_touching_its_foreground() {
    cold_stop_refuses_a_live_writer("system").await;
}

#[path = "r20261017_goal_control_lane_cold_facade.rs"]
mod control_lane_cold;
