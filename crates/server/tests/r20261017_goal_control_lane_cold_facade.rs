//! A queued cold Stop must not adopt a newer Session published on its endpoint.

use super::*;
use klaus_agent_core::goal::service::SavedGoalRequest;

struct ReleaseColdControls(Arc<tokio::sync::Semaphore>);
impl Drop for ReleaseColdControls {
    fn drop(&mut self) {
        self.0.add_permits(4);
    }
}

#[tokio::test]
async fn a_queued_cold_stop_cannot_adopt_a_same_endpoint_goal_edit_without_a_turn() {
    let trace = GoalFixtureTrace::new("queued-cold-stop-local-publication");
    trace.note("serial.await");
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let _config = ConfigGuard::new(root.path());
    let store: Arc<dyn SessionStore> = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.path().join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let thread = "queued-cold-local-publication";
    seed_saved_goal(store.as_ref(), thread).await;
    let initial = goal_store::get_record(thread).await.unwrap();
    let provider_calls = Arc::new(AtomicUsize::new(0));
    let calls = provider_calls.clone();
    let agent = make_agent_with_call_model_and_config(
        Box::new(move |_, _| {
            calls.fetch_add(1, Ordering::SeqCst);
            panic!("goal control and cold Stop must not admit a provider")
        }),
        Some(json!({"goal":true,"prewarm":false,"autoMemory":false,"autoDream":false})),
    );
    let mut lifecycle = crate::lifecycle::protocol_lifecycle(agent.clone(), Some(store.clone()));
    let handler = lifecycle.saved_goal_request.take().unwrap();
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let handled = Arc::new(AtomicUsize::new(0));
    let cleanup = ReleaseColdControls(release.clone());
    lifecycle.saved_goal_request = Some({
        let entered = entered.clone();
        let release = release.clone();
        let handled = handled.clone();
        let trace = trace.clone();
        Arc::new(move |thread, request| {
            let handler = handler.clone();
            let entered = entered.clone();
            let release = release.clone();
            let handled = handled.clone();
            let trace = trace.clone();
            Box::pin(async move {
                if let SavedGoalRequest::Pause { .. } = &request {
                    handled.fetch_add(1, Ordering::SeqCst);
                    trace.note("cold.handler.enter");
                    entered.add_permits(1);
                    release.acquire().await.unwrap().forget();
                    trace.note("cold.handler.release");
                }
                handler(thread, request).await
            })
        })
    });
    let creates = Arc::new(AtomicUsize::new(0));
    let captured = Arc::new(Mutex::new(None::<Arc<Session>>));
    let factory: CreateSession = {
        let agent = agent.clone();
        let store = store.clone();
        let creates = creates.clone();
        let captured = captured.clone();
        Arc::new(move |ctx| {
            let agent = agent.clone();
            let store = store.clone();
            let creates = creates.clone();
            let captured = captured.clone();
            Box::pin(async move {
                creates.fetch_add(1, Ordering::SeqCst);
                let session = agent.resume_session(store, thread, StartSessionOptions {
                    id:ctx.init.thread_id, approval_handler:Some(ctx.approval_handler),
                    elicit_handler:Some(ctx.elicit_handler), mcp_elicitation_presenter:Some(ctx.mcp_elicitation_presenter),
                    config:Some(json!({"goal":true,"prewarm":false,"autoMemory":false,"autoDream":false})),
                    ..Default::default()
                }).await.map_err(|error|Box::new(error) as BoxError)?;
                *captured.lock().unwrap() = Some(session.clone());
                Ok(session)
            })
        })
    };
    let mut client = TestClient::start_with(factory, Some(lifecycle));
    client.initialize().await;
    let (started, _) = request(
        &mut client,
        2,
        "session/start",
        json!({"init":{"threadId":thread}}),
    )
    .await;
    let session_id = started["result"]["sessionId"].clone();
    let mut frames = Vec::new();
    trace.note("urgent.capacity.submit");
    for index in 0..4 {
        let submission = format!("held-cold-system-{index}");
        client
            .send(json!({"id":10+index,"method":"turn/interrupt","params":{
                "sessionId":session_id,"submissionId":submission,"origin":"system"
            }}))
            .await;
    }
    tokio::time::timeout(REPLY_TIMEOUT, entered.acquire_many(4))
        .await
        .unwrap()
        .unwrap()
        .forget();
    trace.note("urgent.capacity.held");
    assert_eq!(handled.load(Ordering::SeqCst), 4);
    assert_eq!(creates.load(Ordering::SeqCst), 0);
    assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
    client
        .send(json!({"id":20,"method":"turn/interrupt","params":{
            "sessionId":session_id,"submissionId":"queued-cold-user-stop","origin":"user-stop"
        }}))
        .await;
    trace.note("queued.stop.ack");
    let (edited, observed) = request(&mut client, 21, "session/setGoal", json!({
        "sessionId":session_id,"objective":"The later local goal edit owns this paused intent",
        "status":"paused","origin":"user","expectedGoalId":initial.goal.as_ref().unwrap().goal_id.try_to_utf8().unwrap()
    })).await;
    trace.note("local.goal.ack");
    frames.extend(observed);
    assert!(edited.get("error").is_none(), "{edited}");
    assert_eq!(
        edited["result"]["threadGoal"]["objective"],
        "The later local goal edit owns this paused intent"
    );
    assert_eq!(creates.load(Ordering::SeqCst), 1);
    assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
    let newer = goal_store::get_record(thread).await.unwrap();
    assert_eq!(
        newer.goal.as_ref().unwrap().status,
        ThreadGoalStatus::Paused
    );
    let before_instructions = klaus_agent_core::goal::retained_context::instructions_from_rollout(
        &store.read_thread(thread).await.unwrap().raw_items,
    );
    assert_eq!(
        before_instructions.len(),
        1,
        "only the real later edit is authoritative"
    );
    trace.note("urgent.capacity.release");
    release.add_permits(4);
    let settled = wait_interrupt(&mut client, frames, "queued-cold-user-stop").await;
    trace.note("queued.stop.refused");
    assert_eq!(settled["turnInterrupt"]["accepted"], false, "{settled}");
    assert!(
        settled["turnInterrupt"].get("goalPauseError").is_none(),
        "the refused queued Stop must not enter saved pause work: {settled}"
    );
    assert_eq!(
        handled.load(Ordering::SeqCst),
        4,
        "the queued fifth Stop refuses before its first saved handler/read"
    );
    assert_eq!(goal_store::get_record(thread).await.unwrap(), newer);
    let after_instructions = klaus_agent_core::goal::retained_context::instructions_from_rollout(
        &store.read_thread(thread).await.unwrap().raw_items,
    );
    assert_eq!(after_instructions, before_instructions);
    assert!(!captured
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .goal_runtime()
        .unwrap()
        .continuation_suppressed());
    assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
    drop(cleanup);
    trace.note("client.close.begin");
    client.close().await;
    trace.note("client.close.done");
}
