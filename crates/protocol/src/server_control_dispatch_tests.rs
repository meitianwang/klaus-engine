//! Held Stop dispatch must allow newer goal controls and ordinary work to arrive.
//! Control work has bounded retained payloads and remains owned through EOF.

use super::*;

#[path = "server_pending_resume_liveness_tests.rs"]
mod pending_resume_liveness;

fn admitted_goal(id: &str) -> AdmittedOp {
    AdmittedOp {
        op: Op {
            id: id.into(),
            op: OpKind::GetGoal,
        },
        run_signal: None,
        foreground_guard: None,
        busy_guard: None,
        interrupt: None,
    }
}

fn unstarted_control_server() -> Arc<ProtocolServer> {
    let create: CreateSession =
        Arc::new(|_| Box::pin(async { panic!("a goal-state read must not create a session") }));
    Arc::new(ProtocolServer::new(ProtocolServerOptions {
        create_session: create,
        lifecycle: None,
        plugin_admin: None,
        credential_store: None,
        input: Box::new(tokio::io::empty()),
        output: Box::new(tokio::io::sink()),
    }))
}

#[tokio::test]
async fn control_lanes_charge_active_bytes_and_reap_aborted_tasks() {
    let server = unstarted_control_server();
    let mut goals = ControlDispatches::new(4, 16, MAX_GOAL_CONTROL_BYTES);
    let mut urgent = ControlDispatches::new(4, 16, MAX_URGENT_CONTROL_BYTES);
    assert!(matches!(
        goals.admit(
            &server.inner,
            admitted_goal("full-frame"),
            MAX_PROTOCOL_LINE_BYTES
        ),
        ControlAdmission::Accepted
    ));
    assert_eq!(goals.bytes, MAX_PROTOCOL_LINE_BYTES);
    assert_eq!(goals.active.len(), 1);
    match goals.admit(&server.inner, admitted_goal("one-byte-over"), 1) {
        ControlAdmission::Full(rejected) => assert_eq!(rejected.op.id, "one-byte-over"),
        _ => panic!("an active frame must retain its byte charge"),
    }
    assert!(matches!(
        urgent.admit(&server.inner, admitted_goal("independent-capacity"), 1),
        ControlAdmission::Accepted
    ));
    assert_eq!(server.inner.dispatches.abort_all(), 2);
    timeout(Duration::from_secs(10), goals.wait_completion())
        .await
        .unwrap();
    timeout(Duration::from_secs(10), urgent.wait_completion())
        .await
        .unwrap();
    assert_eq!(goals.bytes, 0);
    assert_eq!(urgent.bytes, 0);
    assert!(goals.active.is_empty());
    assert!(urgent.active.is_empty());
    assert!(!server.inner.dispatches.has_active());
    server.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn control_lane_bounds_queued_count_and_preserves_fifo_on_rejection() {
    let server = unstarted_control_server();
    let mut lane = ControlDispatches::new(1, 2, 10);
    for (id, bytes) in [("active", 1), ("next", 2), ("last", 3)] {
        assert!(matches!(
            lane.admit(&server.inner, admitted_goal(id), bytes),
            ControlAdmission::Accepted
        ));
    }
    assert_eq!(lane.active.len(), 1);
    assert_eq!(lane.pending.len(), 2);
    assert_eq!(lane.bytes, 6);
    match lane.admit(&server.inner, admitted_goal("overflow"), 1) {
        ControlAdmission::Full(rejected) => assert_eq!(rejected.op.id, "overflow"),
        _ => panic!("pending count must reject before retaining another payload"),
    }
    assert_eq!(
        lane.pending
            .iter()
            .map(|(op, _)| op.op.id.clone())
            .collect::<Vec<_>>(),
        vec![RuntimeString::from("next"), RuntimeString::from("last")]
    );
    lane.clear_pending();
    assert_eq!(lane.bytes, 1);
    timeout(Duration::from_secs(10), lane.wait_completion())
        .await
        .unwrap();
    assert_eq!(lane.bytes, 0);
    assert!(matches!(
        lane.admit(&server.inner, admitted_goal("recovered"), 10),
        ControlAdmission::Accepted
    ));
    timeout(Duration::from_secs(10), lane.wait_completion())
        .await
        .unwrap();
    assert_eq!(lane.bytes, 0);
    server.shutdown_and_wait().await.unwrap();
}

struct ControlDirectory(std::path::PathBuf);
impl Drop for ControlDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct ControlFixture {
    server: Arc<ProtocolServer>,
    done: tokio::task::JoinHandle<()>,
    lines: tokio::io::Lines<BufReader<Box<dyn AsyncRead + Unpin + Send>>>,
    writer: Box<dyn AsyncWrite + Unpin + Send>,
    session: Arc<Session>,
    _config: ConfigDirEnvGuard,
    _directory: ControlDirectory,
}
impl ControlFixture {
    async fn send(&mut self, value: Value) {
        let mut bytes = serde_json::to_vec(&value).unwrap();
        bytes.push(b'\n');
        self.writer.write_all(&bytes).await.unwrap();
        self.writer.flush().await.unwrap();
    }
    async fn event(&mut self, id: &str) -> Value {
        timeout(Duration::from_secs(10), async {
            loop {
                let line = self
                    .lines
                    .next_line()
                    .await
                    .unwrap()
                    .expect("correlated control reply before EOF");
                let event: Value = serde_json::from_str(&line).unwrap();
                if event["submissionId"] == id && event.get("event").is_none() {
                    return event;
                }
            }
        })
        .await
        .expect("correlated control reply remains live")
    }
    async fn close(self) {
        let Self {
            server,
            done,
            mut lines,
            mut writer,
            session,
            _config,
            _directory,
        } = self;
        writer.shutdown().await.unwrap();
        drop(writer);
        // Drain output concurrently with shutdown so transport backpressure is real.
        timeout(Duration::from_secs(10), async {
            while lines.next_line().await.unwrap().is_some() {}
            done.await.unwrap();
        })
        .await
        .expect("EOF drains the tracked control tasks");
        assert!(!server.inner.dispatches.has_active());
        drop(session);
        drop(server);
        drop(_config);
        drop(_directory);
    }
}

async fn control_fixture() -> ControlFixture {
    let agent = make_agent(
        ".",
        vec![text_turn("ordinary replacement ran")],
        Some(json!({"goal":true,"prewarm":false,"autoMemory":false,"autoDream":false})),
    );
    control_fixture_with_agent(agent).await
}

async fn control_fixture_with_agent(agent: Arc<Agent>) -> ControlFixture {
    let directory = unique_temp_dir("tracked-control-lane");
    std::fs::create_dir_all(&directory).unwrap();
    let config = ConfigDirEnvGuard::set(&directory);
    let store: Arc<dyn SessionStore> = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(directory.join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let captured = Arc::new(Mutex::new(None::<Arc<Session>>));
    let thread = format!("control-lane-{}", uuid::Uuid::new_v4());
    let create: CreateSession = {
        let captured = captured.clone();
        Arc::new(move |args| {
            let agent = agent.clone();
            let store = store.clone();
            let captured = captured.clone();
            let thread = thread.clone();
            Box::pin(async move {
                let session = agent.open_session(store, StartSessionOptions {
                    id: Some(thread.into()), approval_handler: Some(args.approval_handler),
                    elicit_handler: Some(args.elicit_handler), mcp_elicitation_presenter: Some(args.mcp_elicitation_presenter),
                    config: Some(json!({"goal":true,"prewarm":false,"autoMemory":false,"autoDream":false})), ..Default::default()
                }).await.map_err(|error|Box::new(error) as BoxError)?;
                *captured.lock().unwrap() = Some(session.clone());
                Ok(session)
            })
        })
    };
    let ((server_r, server_w), (client_r, mut client_w)) = duplex_pair();
    let (server, done) = start_server(create, server_r, server_w);
    client_w
        .write_all(b"{\"id\":\"materialize\",\"op\":{\"type\":\"get_queue_snapshot\"}}\n")
        .await
        .unwrap();
    let mut lines = BufReader::new(client_r).lines();
    timeout(Duration::from_secs(10), async {
        loop {
            let line = lines.next_line().await.unwrap().unwrap();
            let event: Value = serde_json::from_str(&line).unwrap();
            if event["submissionId"] == "materialize" {
                assert!(event.get("error").is_none(), "{event}");
                break;
            }
        }
    })
    .await
    .unwrap();
    let session = captured.lock().unwrap().clone().unwrap();
    assert!(session.goal_runtime().is_some());
    ControlFixture {
        server,
        done,
        lines,
        writer: client_w,
        session,
        _config: config,
        _directory: ControlDirectory(directory),
    }
}

struct ReleaseControlStop {
    runtime: Arc<klaus_agent_core::goal::runtime::GoalRuntime>,
    release: Arc<tokio::sync::Semaphore>,
}
impl Drop for ReleaseControlStop {
    fn drop(&mut self) {
        self.runtime.testing_before_stop_cancel(None);
        self.release.add_permits(MAX_ACTIVE_URGENT_CONTROLS);
    }
}

#[tokio::test]
async fn an_idle_held_stop_allows_goal_ack_and_an_ordinary_replacement_turn() {
    let mut fixture = control_fixture().await;
    let runtime = fixture.session.goal_runtime().unwrap();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    runtime.testing_before_stop_cancel(Some({
        let entered = entered.clone();
        let release = release.clone();
        Arc::new(move || {
            let entered = entered.clone();
            let release = release.clone();
            Box::pin(async move {
                entered.notify_one();
                release.acquire().await.unwrap().forget();
            })
        })
    }));
    let cleanup = ReleaseControlStop {
        runtime: runtime.clone(),
        release: release.clone(),
    };
    fixture
        .send(json!({"id":"held-idle-stop","op":{"type":"interrupt","origin":"user-stop"}}))
        .await;
    timeout(Duration::from_secs(10), entered.notified())
        .await
        .unwrap();
    fixture.send(json!({"id":"new-goal","op":{"type":"set_goal","objective":"The newer paused intent","status":"paused","origin":"user"}})).await;
    let goal = fixture.event("new-goal").await;
    assert_eq!(goal["threadGoal"]["status"], "paused", "{goal}");
    assert_eq!(goal["threadGoal"]["objective"], "The newer paused intent");
    fixture.send(json!({"id":"replacement","op":{"type":"run","input":"Run ordinary work while the old Stop is held"}})).await;
    let result = fixture.event("replacement").await;
    assert_eq!(
        result["result"]["finalText"], "ordinary replacement ran",
        "{result}"
    );
    assert_eq!(result["result"]["stopReason"], "completed");
    runtime.testing_before_stop_cancel(None);
    release.add_permits(1);
    let stop = fixture.event("held-idle-stop").await;
    assert_eq!(
        stop["turnInterrupt"]["accepted"], false,
        "the old idle capture cannot adopt a later turn: {stop}"
    );
    assert!(!runtime.continuation_suppressed());
    drop(cleanup);
    fixture.close().await;
}

#[tokio::test]
async fn saturated_goal_controls_preserve_urgent_progress_and_drain_on_eof() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let agent = make_recording_agent(".", requests.clone());
    let mut fixture = control_fixture_with_agent(agent).await;
    let ordinary = fixture
        .server
        .inner
        .queue_operation_gate
        .clone()
        .lock_owned()
        .await;
    fixture
        .send(json!({"id":"held-ordinary","op":{"type":"get_queue_snapshot"}}))
        .await;
    fixture.send(json!({"id":"retired-queued-run","op":{"type":"run","input":"This queued work must never reach a provider"}})).await;
    let thread = fixture.session.id.try_to_utf8().unwrap();
    let held = klaus_agent_core::goal::service::thread_mutation_guard(&thread).await;
    for index in 0..MAX_ACTIVE_GOAL_CONTROLS + MAX_QUEUED_GOAL_CONTROLS {
        fixture.send(json!({"id":format!("goal-{index}"),"op":{"type":"set_goal","objective":"Retained bounded control","status":"paused","origin":"user"}})).await;
    }
    fixture
        .send(json!({"id":"goal-overflow","op":{"type":"get_goal"}}))
        .await;
    fixture
        .send(json!({"id":"urgent-stop","op":{"type":"interrupt","origin":"system"}}))
        .await;
    let mut overflow = false;
    let mut urgent = false;
    timeout(Duration::from_secs(10), async {
        while !overflow || !urgent {
            let line = fixture.lines.next_line().await.unwrap().unwrap();
            let event: Value = serde_json::from_str(&line).unwrap();
            if event["submissionId"] == "goal-overflow" {
                assert!(
                    event["error"]
                        .as_str()
                        .unwrap()
                        .contains("goal control capacity exceeded"),
                    "{event}"
                );
                overflow = true;
            }
            if event["submissionId"] == "urgent-stop" && event.get("turnInterrupt").is_some() {
                assert_eq!(event["turnInterrupt"]["accepted"], true, "{event}");
                urgent = true;
            }
        }
    })
    .await
    .expect("normal control saturation cannot starve urgent Stop");
    assert_eq!(
        fixture
            .server
            .inner
            .run_admission
            .lock()
            .unwrap()
            .foreground
            .len(),
        1,
        "the ordinary queued Run still owns its admitted reservation"
    );
    assert!(requests.lock().unwrap().is_empty());
    fixture.writer.shutdown().await.unwrap();
    timeout(Duration::from_secs(10), async {
        while !fixture.server.inner.dispatches.admission_is_closed() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("EOF closes control admission while earlier tasks remain tracked");
    assert!(fixture.server.inner.dispatches.has_active());
    assert!(!fixture.done.is_finished());
    {
        let admission = fixture.server.inner.run_admission.lock().unwrap();
        assert!(
            admission.foreground.is_empty(),
            "EOF retires queued Run ownership before active work drains"
        );
        assert_eq!(admission.pending_goal_stops, 0);
    }
    assert!(requests.lock().unwrap().is_empty());
    drop(held);
    drop(ordinary);
    fixture.close().await;
    assert!(
        requests.lock().unwrap().is_empty(),
        "retired queued work cannot start during drain"
    );
}

#[tokio::test]
async fn urgent_control_hard_limit_returns_a_correlated_interrupt_refusal() {
    let mut fixture = control_fixture().await;
    let runtime = fixture.session.goal_runtime().unwrap();
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    runtime.testing_before_stop_cancel(Some({
        let entered = entered.clone();
        let release = release.clone();
        Arc::new(move || {
            let entered = entered.clone();
            let release = release.clone();
            Box::pin(async move {
                entered.add_permits(1);
                release.acquire().await.unwrap().forget();
            })
        })
    }));
    let cleanup = ReleaseControlStop {
        runtime: runtime.clone(),
        release: release.clone(),
    };
    for index in 0..MAX_ACTIVE_URGENT_CONTROLS + MAX_QUEUED_URGENT_CONTROLS {
        fixture
            .send(json!({"id":format!("stop-{index}"),"op":{"type":"interrupt","origin":"system"}}))
            .await;
    }
    timeout(
        Duration::from_secs(10),
        entered.acquire_many(MAX_ACTIVE_URGENT_CONTROLS as u32),
    )
    .await
    .unwrap()
    .unwrap()
    .forget();
    let epoch_before_overflow = runtime.request_identity().pursuit_epoch;
    let stops_before_overflow = fixture
        .server
        .inner
        .run_admission
        .lock()
        .unwrap()
        .pending_goal_stops;
    fixture
        .send(json!({"id":"urgent-overflow","op":{"type":"interrupt","origin":"user-stop"}}))
        .await;
    let refused = fixture.event("urgent-overflow").await;
    assert_eq!(refused["turnInterrupt"]["accepted"], false, "{refused}");
    assert_eq!(
        runtime.request_identity().pursuit_epoch,
        epoch_before_overflow
    );
    assert_eq!(
        fixture
            .server
            .inner
            .run_admission
            .lock()
            .unwrap()
            .pending_goal_stops,
        stops_before_overflow
    );
    assert!(!runtime.continuation_suppressed());
    assert!(
        refused["error"]
            .as_str()
            .unwrap()
            .contains("urgent control capacity exceeded"),
        "{refused}"
    );
    fixture
        .send(json!({"id":"independent-goal-read","op":{"type":"get_goal"}}))
        .await;
    let read = fixture.event("independent-goal-read").await;
    assert!(read.get("error").is_none(), "{read}");
    runtime.testing_before_stop_cancel(None);
    release.add_permits(MAX_ACTIVE_URGENT_CONTROLS);
    drop(cleanup);
    fixture.close().await;
}

#[tokio::test]
async fn a_queued_unfenced_stop_cannot_adopt_a_later_foreground_turn() {
    let provider_started = Arc::new(Notify::new());
    let provider_cancelled = Arc::new(Notify::new());
    let agent = make_blocking_agent(".", provider_started.clone(), provider_cancelled.clone());
    let mut fixture = control_fixture_with_agent(agent).await;
    let runtime = fixture.session.goal_runtime().unwrap();
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    runtime.testing_before_stop_cancel(Some({
        let entered = entered.clone();
        let release = release.clone();
        Arc::new(move || {
            let entered = entered.clone();
            let release = release.clone();
            Box::pin(async move {
                entered.add_permits(1);
                release.acquire().await.unwrap().forget();
            })
        })
    }));
    let cleanup = ReleaseControlStop {
        runtime: runtime.clone(),
        release: release.clone(),
    };
    for index in 0..MAX_ACTIVE_URGENT_CONTROLS {
        fixture.send(json!({"id":format!("held-urgent-{index}"),"op":{"type":"interrupt","origin":"system"}})).await;
    }
    timeout(
        Duration::from_secs(10),
        entered.acquire_many(MAX_ACTIVE_URGENT_CONTROLS as u32),
    )
    .await
    .unwrap()
    .unwrap()
    .forget();
    fixture
        .send(json!({"id":"queued-bare-stop","op":{"type":"interrupt","origin":"user-stop"}}))
        .await;
    fixture.send(json!({"id":"later-live-turn","op":{"type":"run","input":"Keep this later foreground alive"}})).await;
    timeout(Duration::from_secs(10), provider_started.notified())
        .await
        .unwrap();
    {
        let admission = fixture.server.inner.run_admission.lock().unwrap();
        assert_eq!(admission.pending_goal_stops, MAX_ACTIVE_URGENT_CONTROLS + 1);
        let current = admission
            .foreground
            .front()
            .expect("the later actual provider owns the foreground");
        assert_eq!(current.submission_id, "later-live-turn");
        assert!(!current.signal.is_cancelled());
    }
    runtime.testing_before_stop_cancel(None);
    release.add_permits(MAX_ACTIVE_URGENT_CONTROLS);
    let refused = fixture.event("queued-bare-stop").await;
    assert_eq!(refused["turnInterrupt"]["accepted"], false, "{refused}");
    assert!(
        refused["error"]
            .as_str()
            .unwrap()
            .contains("before its control task began"),
        "{refused}"
    );
    {
        let admission = fixture.server.inner.run_admission.lock().unwrap();
        let current = admission
            .foreground
            .front()
            .expect("stale Stop leaves the actual provider live");
        assert_eq!(current.submission_id, "later-live-turn");
        assert!(!current.signal.is_cancelled());
    }
    assert!(provider_cancelled.notified().now_or_never().is_none());
    assert!(!runtime.continuation_suppressed());
    assert!(fixture
        .session
        .goal_snapshot()
        .await
        .unwrap()
        .goal
        .is_none());
    fixture.send(json!({"id":"cleanup-target","op":{"type":"interrupt","origin":"targeted-turn","expectedTurnId":"later-live-turn"}})).await;
    let interrupted = fixture.event("cleanup-target").await;
    assert_eq!(
        interrupted["turnInterrupt"]["accepted"], true,
        "{interrupted}"
    );
    timeout(Duration::from_secs(10), provider_cancelled.notified())
        .await
        .unwrap();
    drop(cleanup);
    fixture.close().await;
}

struct ReleaseControlModelCreate {
    runtime: Arc<klaus_agent_core::goal::runtime::GoalRuntime>,
    release: Arc<tokio::sync::Semaphore>,
}
impl Drop for ReleaseControlModelCreate {
    fn drop(&mut self) {
        self.runtime.testing_before_model_create(None);
        self.release.add_permits(1);
    }
}

#[tokio::test]
async fn a_queued_stop_revokes_an_older_model_create_at_arrival() {
    let agent = make_agent(
        ".",
        vec![
            tool_use_turn(
                "queued-old-create",
                "create_goal",
                json!({"objective":"This older request cannot create"}),
            ),
            text_turn("The older create was refused"),
        ],
        Some(json!({"goal":true,"prewarm":false,"autoMemory":false,"autoDream":false})),
    );
    let mut fixture = control_fixture_with_agent(agent).await;
    let runtime = fixture.session.goal_runtime().unwrap();
    let urgent_entered = Arc::new(tokio::sync::Semaphore::new(0));
    let urgent_release = Arc::new(tokio::sync::Semaphore::new(0));
    runtime.testing_before_stop_cancel(Some({
        let entered = urgent_entered.clone();
        let release = urgent_release.clone();
        Arc::new(move || {
            let entered = entered.clone();
            let release = release.clone();
            Box::pin(async move {
                entered.add_permits(1);
                release.acquire().await.unwrap().forget();
            })
        })
    }));
    let stop_cleanup = ReleaseControlStop {
        runtime: runtime.clone(),
        release: urgent_release.clone(),
    };
    for index in 0..MAX_ACTIVE_URGENT_CONTROLS {
        fixture.send(json!({"id":format!("capacity-owner-{index}"),"op":{"type":"interrupt","origin":"system"}})).await;
    }
    timeout(
        Duration::from_secs(10),
        urgent_entered.acquire_many(MAX_ACTIVE_URGENT_CONTROLS as u32),
    )
    .await
    .unwrap()
    .unwrap()
    .forget();
    let create_entered = Arc::new(Notify::new());
    let create_release = Arc::new(tokio::sync::Semaphore::new(0));
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
    let create_cleanup = ReleaseControlModelCreate {
        runtime: runtime.clone(),
        release: create_release.clone(),
    };
    let thread = fixture.session.id.try_to_utf8().unwrap();
    let before = klaus_agent_core::goal::store::get_record(&thread)
        .await
        .unwrap();
    assert!(before.goal.is_none());
    // These four accepted Stops predate this provider request. Only the queued
    // fifth Stop can revoke the real create handler's newer captured epoch.
    fixture
        .send(json!({"id":"old-model-turn","op":{"type":"run","input":"Attempt the goal tool"}}))
        .await;
    timeout(Duration::from_secs(10), create_entered.notified())
        .await
        .unwrap();
    fixture
        .send(json!({"id":"queued-model-stop","op":{"type":"interrupt","origin":"user-stop"}}))
        .await;
    fixture
        .send(json!({"id":"arrival-observed","op":{"type":"get_goal"}}))
        .await;
    let observed = fixture.event("arrival-observed").await;
    assert!(observed.get("error").is_none(), "{observed}");
    assert_eq!(
        fixture
            .server
            .inner
            .run_admission
            .lock()
            .unwrap()
            .pending_goal_stops,
        MAX_ACTIVE_URGENT_CONTROLS + 1
    );
    runtime.testing_before_model_create(None);
    create_release.add_permits(1);
    let result = fixture.event("old-model-turn").await;
    assert_eq!(result["result"]["stopReason"], "completed", "{result}");
    assert_eq!(
        result["result"]["finalText"],
        "The older create was refused"
    );
    timeout(Duration::from_secs(10), async {
        while !fixture
            .server
            .inner
            .run_admission
            .lock()
            .unwrap()
            .foreground
            .is_empty()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the completed old reservation retires while every Stop remains held");
    let history = fixture.session.history();
    let failure = history
        .iter()
        .find_map(|message| match message {
            Message::ToolResult(result) if result.tool_use_id == "queued-old-create" => {
                Some(result)
            }
            _ => None,
        })
        .expect("the actual create_goal tool must produce a result");
    assert!(
        failure.is_error,
        "queued Stop revokes the old model create before mutation"
    );
    assert!(format!("{:?}", failure.content).contains("intent changed"));
    assert_eq!(
        klaus_agent_core::goal::store::get_record(&thread)
            .await
            .unwrap(),
        before
    );
    fixture.send(json!({"id":"new-authorized-intent","op":{"type":"set_goal","objective":"Keep the later explicit goal intact","status":"paused","origin":"user"}})).await;
    let newer = fixture.event("new-authorized-intent").await;
    assert_eq!(
        newer["threadGoal"]["objective"], "Keep the later explicit goal intact",
        "{newer}"
    );
    let saved_newer = klaus_agent_core::goal::store::get_record(&thread)
        .await
        .unwrap();
    runtime.testing_before_stop_cancel(None);
    urgent_release.add_permits(MAX_ACTIVE_URGENT_CONTROLS);
    let stale = fixture.event("queued-model-stop").await;
    assert_eq!(stale["turnInterrupt"]["accepted"], false, "{stale}");
    assert_eq!(
        klaus_agent_core::goal::store::get_record(&thread)
            .await
            .unwrap(),
        saved_newer
    );
    assert!(!runtime.continuation_suppressed());
    drop(create_cleanup);
    drop(stop_cleanup);
    fixture.close().await;
}

#[tokio::test]
async fn malformed_or_mismatched_interrupts_preserve_goal_authority_and_correlation() {
    let mut fixture = control_fixture().await;
    let runtime = fixture.session.goal_runtime().unwrap();
    let before_epoch = runtime.request_identity().pursuit_epoch;
    let thread = fixture.session.id.try_to_utf8().unwrap();
    let before = klaus_agent_core::goal::store::get_record(&thread)
        .await
        .unwrap();
    fixture
        .writer
        .write_all(
            b"{\"id\":\"\\ud800\",\"op\":{\"type\":\"interrupt\",\"origin\":\"user-stop\"}}\n",
        )
        .await
        .unwrap();
    fixture.writer.flush().await.unwrap();
    fixture.send(json!({"id":"mismatched-stop","op":{"type":"interrupt","origin":"user-stop","expectedTurnId":"absent-foreground"}})).await;
    let mut malformed = false;
    let mut mismatched = false;
    timeout(Duration::from_secs(10), async {
        while !malformed || !mismatched {
            let line = fixture.lines.next_line().await.unwrap().unwrap();
            let event = runtime_json::parse(&RuntimeString::from(line)).unwrap();
            let id = event.get("submissionId").and_then(RuntimeJson::as_string);
            if id == Some(&RuntimeString::from_utf16([0xd800])) {
                assert!(event
                    .get("error")
                    .and_then(RuntimeJson::as_str)
                    .unwrap()
                    .starts_with("op id is not scalar UTF-8"));
                assert!(event.get("turnInterrupt").is_none());
                malformed = true;
            }
            if id == Some(&RuntimeString::from("mismatched-stop")) {
                assert_eq!(
                    event
                        .get("turnInterrupt")
                        .and_then(|result| result.get("accepted"))
                        .and_then(RuntimeJson::as_bool),
                    Some(false)
                );
                assert!(event
                    .get("error")
                    .and_then(RuntimeJson::as_str)
                    .unwrap()
                    .contains("no longer the foreground"));
                mismatched = true;
            }
        }
    })
    .await
    .expect("both invalid requests retain their exact error correlation");
    assert_eq!(runtime.request_identity().pursuit_epoch, before_epoch);
    assert_eq!(
        fixture
            .server
            .inner
            .run_admission
            .lock()
            .unwrap()
            .pending_goal_stops,
        0
    );
    assert!(!runtime.continuation_suppressed());
    assert_eq!(
        klaus_agent_core::goal::store::get_record(&thread)
            .await
            .unwrap(),
        before
    );
    fixture.close().await;
}

#[tokio::test]
async fn cancelling_the_reader_closes_admission_before_queued_ownership_drops() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let agent = make_recording_agent(".", requests.clone());
    let mut fixture = control_fixture_with_agent(agent).await;
    let held = fixture
        .server
        .inner
        .queue_operation_gate
        .clone()
        .lock_owned()
        .await;
    fixture
        .send(json!({"id":"held-ordinary","op":{"type":"get_queue_snapshot"}}))
        .await;
    fixture.send(json!({"id":"reader-cancelled-run","op":{"type":"run","input":"Never execute after reader cancellation"}})).await;
    fixture
        .send(json!({"id":"queued-arrival-visible","op":{"type":"get_goal"}}))
        .await;
    let visible = fixture.event("queued-arrival-visible").await;
    assert!(visible.get("error").is_none(), "{visible}");
    assert_eq!(
        fixture
            .server
            .inner
            .run_admission
            .lock()
            .unwrap()
            .foreground
            .len(),
        1
    );
    fixture.done.abort();
    timeout(Duration::from_secs(10), async {
        while !fixture.done.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("reader cancellation reaches its ownership guards");
    assert!(fixture.server.inner.stopped.load(Ordering::SeqCst));
    assert!(fixture.server.inner.dispatches.admission_is_closed());
    {
        let admission = fixture.server.inner.run_admission.lock().unwrap();
        assert!(admission.foreground.is_empty());
        assert_eq!(admission.pending_goal_stops, 0);
    }
    assert!(requests.lock().unwrap().is_empty());
    drop(held);
    let ControlFixture {
        server,
        done,
        mut lines,
        mut writer,
        session,
        _config,
        _directory,
    } = fixture;
    writer.shutdown().await.unwrap();
    drop(writer);
    timeout(Duration::from_secs(10), async {
        while lines.next_line().await.unwrap().is_some() {}
        server.shutdown_and_wait().await.unwrap();
    })
    .await
    .expect("the retained tracker drains after reader cancellation");
    assert!(done.await.unwrap_err().is_cancelled());
    assert!(!server.inner.dispatches.has_active());
    assert!(requests.lock().unwrap().is_empty());
    drop(session);
    drop(server);
    drop(_config);
    drop(_directory);
}
