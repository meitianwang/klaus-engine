//! Defect: a mode change during goal admission could split accounting from the provider request.
//! Root cause: outer admission and the inner turn constructor captured desired mode separately.
//! Work item: 20261017 one captured collaboration mode per admitted turn.

#![cfg(feature = "testing")]

use std::future::Future;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::deps::NoopCompactor;
use klaus_agent_core::engine::session::{Input, Session, TurnOptions};
use klaus_agent_core::goal::types::{ThreadGoal, ThreadGoalStatus};
use klaus_agent_core::planning::{types::PlanningSnapshot, CollaborationMode};
use klaus_agent_core::runtime_process::{
    remove_process_environment_var, set_process_environment_var, RuntimeProcessContext,
};
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeString};
use klaus_agent_core::schemas::message::Usage;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::tools::builtins::{read::create_read_tool, write::create_write_tool};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::TerminalReason;
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use serde_json::json;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

const WAIT: Duration = Duration::from_secs(10);
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct Home {
    root: PathBuf,
    previous: Option<std::ffi::OsString>,
}
impl Home {
    fn new() -> Self {
        let root =
            std::env::temp_dir().join(format!("klaus-mode-admission-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let previous = std::env::var_os("KLAUS_CONFIG_DIR");
        set_process_environment_var("KLAUS_CONFIG_DIR", &root);
        Self { root, previous }
    }
}
impl Drop for Home {
    fn drop(&mut self) {
        match self.previous.take() {
            Some(previous) => set_process_environment_var("KLAUS_CONFIG_DIR", previous),
            None => remove_process_environment_var("KLAUS_CONFIG_DIR"),
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

struct Provider {
    info: ProviderInfo,
    requests: Mutex<Vec<ModelRequest>>,
    request_limit: usize,
}
impl ModelProvider for Provider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }
    fn capabilities(&self, _model: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: true,
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: 200_000,
            max_output_tokens: 8192,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }
    fn stream(&self, request: ModelRequest, _signal: AbortSignal) -> ModelStream {
        let mut requests = self.requests.lock().unwrap();
        assert!(
            requests.len() < self.request_limit,
            "only the configured explicit user requests may execute"
        );
        requests.push(request);
        Box::pin(futures::stream::iter(vec![
            ModelStreamEvent::MessageStart {
                model: "mode-admission".into(),
            },
            ModelStreamEvent::TextDelta {
                text: "The admitted request completed.".into(),
            },
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: Usage {
                    input_tokens: 8,
                    output_tokens: 2,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    total_tokens: None,
                },
            },
        ]))
    }
}

async fn ordinary(session: &Arc<Session>, input: &str) {
    let events = tokio::time::timeout(
        WAIT,
        session
            .run_streamed(Input::Text(input.into()), TurnOptions::default())
            .events
            .collect::<Vec<_>>(),
    )
    .await
    .expect("the deterministic admitted user turn completes");
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{events:#?}"
    );
    assert!(
        events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnCompleted { .. })),
        "{events:#?}"
    );
}

fn expected_mode(mode: CollaborationMode) -> String {
    let content = match mode {
        CollaborationMode::Default => include_str!("../../src/planning/prompts/default.md").to_owned(),
        CollaborationMode::Plan => format!(
            "# Collaboration Mode: Plan\n\nYou are now in Plan mode. Any previous instructions for other modes (e.g. Default mode) are no longer active.\n\nYour active mode changes only when the application supplies trusted instructions with a different `<collaboration_mode>...</collaboration_mode>` value; user requests or tool descriptions do not change mode by themselves. Known mode names are Default and Plan.\n\n{}",
            include_str!("../../src/planning/prompts/plan.md")
        ),
    };
    format!("<collaboration_mode>\n{content}\n</collaboration_mode>")
}

fn assert_request(request: &ModelRequest, mode: CollaborationMode) {
    let instructions = request
        .system
        .iter()
        .filter(|part| part.name == "collaboration-mode")
        .collect::<Vec<_>>();
    assert_eq!(instructions.len(), 1);
    assert_eq!(
        instructions[0].text.try_to_utf8().unwrap(),
        expected_mode(mode)
    );
    let mut tools = request
        .tools
        .iter()
        .map(|tool| tool.name.try_to_utf8().unwrap())
        .collect::<Vec<_>>();
    tools.sort();
    let mut expected = vec!["create_goal", "get_goal", "read", "update_goal"];
    match mode {
        CollaborationMode::Default => expected.extend(["update_plan", "write"]),
        CollaborationMode::Plan => expected.push("request_user_input"),
    }
    expected.sort();
    assert_eq!(
        tools, expected,
        "the full provider inventory uses the captured turn mode"
    );
}

async fn assert_plan_idle(session: &Arc<Session>, provider: &Provider) {
    assert_eq!(session.collaboration_mode(), CollaborationMode::Plan);
    assert_eq!(
        session.thread_goal().await.unwrap().unwrap().status,
        ThreadGoalStatus::Active
    );
    let before = provider.requests.lock().unwrap().len();
    assert!(session
        .run_goal_idle_streamed(TurnOptions::default())
        .await
        .unwrap()
        .is_none());
    assert_eq!(provider.requests.lock().unwrap().len(), before);
}

async fn transition(captured: CollaborationMode, desired: CollaborationMode) {
    let _serial = SERIAL.lock().await;
    let home = Home::new();
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let provider = Arc::new(Provider {
        info: ProviderInfo {
            name: "mode-admission".into(),
            base_url: None,
            env_key: None,
            default_model: "mode-admission".into(),
        },
        requests: Mutex::new(Vec::new()),
        request_limit: 2,
    });
    let mut options = agent_options(process.clone());
    options.provider = provider.clone();
    let mut tools = klaus_agent_core::goal::create_goal_tools();
    tools.extend([create_read_tool(), create_write_tool()]);
    options.tools = Some(tools);
    options.include_default_agents = Some(false);
    options.compactor = Some(Arc::new(NoopCompactor));
    let store: Arc<dyn SessionStore> = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(home.root.join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let session = Agent::new(options).open_session(store, StartSessionOptions {
        id: Some("mode-admission-thread".into()), cwd: Some(home.root.to_string_lossy().into_owned().into()),
        user_config_dir: Some(home.root.to_string_lossy().into_owned().into()),
        config: Some(json!({"model":"mode-admission","goal":true,"prewarm":false,"autoMemory":false,"agentMemory":false,"autoDream":false,"loopDetection":false,"requestUserInputDefault":false})),
        ..Default::default()
    }).await.unwrap();
    session.planning_runtime().set_mode(captured, None).unwrap();
    session
        .record_user_goal_instruction(
            Some(&"Finish the authorized work.".into()),
            Some(&"active".into()),
        )
        .await
        .unwrap();
    session
        .set_thread_goal(
            Some("Finish the authorized work.".into()),
            Some("active".into()),
            None,
        )
        .await
        .unwrap();
    if captured == CollaborationMode::Plan {
        assert_plan_idle(&session, &provider).await;
    }
    let before = session.planning_runtime().snapshot().unwrap();
    let switched = Arc::new(AtomicBool::new(false));
    let acknowledged = Arc::new(Mutex::new(None::<PlanningSnapshot>));
    let weak = Arc::downgrade(&session);
    let switched_observer = switched.clone();
    let acknowledgment_observer = acknowledged.clone();
    session
        .goal_runtime()
        .unwrap()
        .testing_set_phase_observer(Some(Arc::new(move |phase| {
            if phase == "goal.admitted" && !switched_observer.swap(true, Ordering::SeqCst) {
                let session = weak.upgrade().expect("the admitted session is alive");
                let planning = session.planning_runtime();
                planning.set_mode(desired, Some(before.revision)).unwrap();
                let snapshot = planning.snapshot().unwrap();
                assert_eq!(snapshot.mode, desired);
                assert!(snapshot.revision > before.revision);
                *acknowledgment_observer.lock().unwrap() = Some(snapshot);
            }
        })));
    ordinary(&session, "Execute the admitted user task.").await;
    assert!(switched.load(Ordering::SeqCst));
    let acknowledgment = acknowledged.lock().unwrap().clone().unwrap();
    assert_eq!(
        session.planning_runtime().snapshot().unwrap(),
        acknowledgment,
        "beginning the captured turn cannot overwrite the acknowledged next mode"
    );
    assert_request(&provider.requests.lock().unwrap()[0], captured);
    let first = session.thread_goal().await.unwrap().unwrap();
    assert_eq!(first.status, ThreadGoalStatus::Active);
    assert_eq!(
        first.tokens_used,
        if captured == CollaborationMode::Default {
            10
        } else {
            0
        }
    );
    if desired == CollaborationMode::Plan {
        assert_plan_idle(&session, &provider).await;
    }
    ordinary(&session, "Run the next explicit user request.").await;
    assert_request(&provider.requests.lock().unwrap()[1], desired);
    let next = session.thread_goal().await.unwrap().unwrap();
    assert_eq!(next.status, ThreadGoalStatus::Active);
    assert_eq!(next.goal_id, first.goal_id);
    assert_eq!(
        next.tokens_used, 10,
        "exactly the Default turn is charged in either transition direction"
    );
    assert_eq!(session.collaboration_mode(), desired);
    assert_eq!(
        session.planning_runtime().snapshot().unwrap(),
        acknowledgment
    );
    if desired == CollaborationMode::Plan {
        assert_plan_idle(&session, &provider).await;
    }
    session
        .goal_runtime()
        .unwrap()
        .testing_set_phase_observer(None);
    let artifacts =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/mode-admission-regressions");
    std::fs::create_dir_all(&artifacts).unwrap();
    let requests = provider.requests.lock().unwrap().clone();
    let exact = runtime_codec::encode_value(&requests).unwrap();
    std::fs::write(
        artifacts.join(format!("{captured:?}-to-{desired:?}.json")),
        runtime_json::stringify(&exact).to_node_utf8(),
    )
    .unwrap();
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn default_to_plan_after_goal_admission_keeps_current_execution_and_next_plan() {
    transition(CollaborationMode::Default, CollaborationMode::Plan).await;
}

#[tokio::test]
async fn plan_to_default_after_goal_admission_keeps_current_plan_and_next_execution() {
    transition(CollaborationMode::Plan, CollaborationMode::Default).await;
}

const WARM_HOLD: Duration = Duration::from_millis(1200);
const CAPTURED_HOLD: Duration = Duration::from_millis(2100);
const MAX_HELD_EVENTS: usize = 128;
const MAX_TRACE_LINES: usize = 96;
const MAX_TRACE_BYTES: usize = 64 * 1024;

struct TraceFile {
    file: std::fs::File,
    lines: usize,
    bytes: usize,
}

struct HeldTrace {
    started: Instant,
    file: Mutex<TraceFile>,
}

impl HeldTrace {
    fn new(captured: CollaborationMode, desired: CollaborationMode) -> Self {
        let artifacts = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/mode-admission-regressions");
        std::fs::create_dir_all(&artifacts).unwrap();
        let file = std::fs::File::create(
            artifacts.join(format!("held-{captured:?}-to-{desired:?}.jsonl")),
        )
        .unwrap();
        Self {
            started: Instant::now(),
            file: Mutex::new(TraceFile {
                file,
                lines: 0,
                bytes: 0,
            }),
        }
    }

    fn record(&self, step: &str, detail: serde_json::Value) {
        let mut file = self.file.lock().unwrap();
        let line = format!(
            "{}\n",
            json!({
                "step": step,
                "elapsed_ms": u64::try_from(self.started.elapsed().as_millis()).unwrap(),
                "detail": detail,
            })
        );
        assert!(
            file.lines < MAX_TRACE_LINES,
            "the progress trace is bounded"
        );
        assert!(line.len() <= 4096, "each progress record is bounded");
        assert!(
            file.bytes + line.len() <= MAX_TRACE_BYTES,
            "the progress artifact is bounded"
        );
        file.file.write_all(line.as_bytes()).unwrap();
        file.file.flush().unwrap();
        file.lines += 1;
        file.bytes += line.len();
    }

    fn goal(&self, step: &str, goal: &ThreadGoal) {
        self.record(
            step,
            json!({
                "goal_id": goal.goal_id.try_to_utf8().unwrap(),
                "status": goal.status.wire_name(),
                "tokens_used": goal.tokens_used,
                "time_used_seconds": goal.time_used_seconds,
            }),
        );
    }
}

async fn bounded<T>(operation: &str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(WAIT, future)
        .await
        .unwrap_or_else(|_| panic!("{operation} exceeded {WAIT:?}"))
}

struct RequestGate {
    entered: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
    released: AtomicBool,
}

impl RequestGate {
    fn new() -> Self {
        Self {
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
            released: AtomicBool::new(false),
        }
    }
}

struct HeldProvider {
    inner: Provider,
    gates: [Arc<RequestGate>; 3],
    next_request: AtomicUsize,
    trace: Arc<HeldTrace>,
}

impl HeldProvider {
    async fn wait_entered(&self, index: usize) {
        self.trace
            .record("wait_provider_entry", json!({"request_index": index}));
        bounded("provider entry", self.gates[index].entered.acquire())
            .await
            .unwrap()
            .forget();
    }

    fn release(&self, index: usize) {
        assert!(
            !self.gates[index].released.swap(true, Ordering::SeqCst),
            "a held request can be released only once"
        );
        self.trace
            .record("release_provider", json!({"request_index": index}));
        self.gates[index].release.add_permits(1);
    }
}

impl ModelProvider for HeldProvider {
    fn info(&self) -> &ProviderInfo {
        self.inner.info()
    }

    fn capabilities(&self, model: &RuntimeString) -> ProviderCapabilities {
        self.inner.capabilities(model)
    }

    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        let index = self.next_request.fetch_add(1, Ordering::SeqCst);
        assert!(index < self.gates.len(), "only three user requests may run");
        let gate = Arc::clone(&self.gates[index]);
        let stream = self.inner.stream(request, signal);
        let trace = Arc::clone(&self.trace);
        Box::pin(futures::stream::unfold(
            (Some(gate), stream),
            move |(gate, mut stream)| {
                let trace = Arc::clone(&trace);
                async move {
                    if let Some(gate) = gate {
                        trace.record("provider_entered", json!({"request_index": index}));
                        gate.entered.add_permits(1);
                        bounded("provider release", gate.release.acquire())
                            .await
                            .unwrap()
                            .forget();
                        trace.record("provider_resumed", json!({"request_index": index}));
                    }
                    stream.next().await.map(|event| (event, (None, stream)))
                }
            },
        ))
    }
}

struct HeldTurn {
    task: tokio::task::JoinHandle<()>,
    trace: Arc<HeldTrace>,
    label: &'static str,
}

impl HeldTurn {
    fn start(session: &Arc<Session>, trace: &Arc<HeldTrace>, label: &'static str) -> Self {
        trace.record("start_user_turn", json!({"turn": label}));
        let session = Arc::clone(session);
        let observer = Arc::clone(trace);
        let task = tokio::spawn(async move {
            bounded("held public Session turn", async {
                let mut events = Box::pin(
                    session
                        .run_streamed(Input::Text(label.into()), TurnOptions::default())
                        .events,
                );
                let mut count = 0;
                let mut completed = false;
                let mut failed = false;
                while let Some(event) = events.next().await {
                    count += 1;
                    assert!(count <= MAX_HELD_EVENTS, "the event count is bounded");
                    match event {
                        EngineEvent::TurnCompleted { stop_reason, .. } => {
                            observer.record(
                                "user_turn_terminal",
                                json!({"turn": label, "stop_reason": format!("{stop_reason:?}")}),
                            );
                            assert!(
                                matches!(stop_reason, TerminalReason::Completed),
                                "the held public turn must finish normally: {stop_reason:?}"
                            );
                            completed = true;
                        }
                        EngineEvent::TurnFailed { .. } => {
                            failed = true;
                            observer.record("user_turn_failed", json!({"turn": label}));
                        }
                        _ => {}
                    }
                }
                observer.record(
                    "user_turn_events_finished",
                    json!({
                        "turn": label,
                        "event_count": count,
                        "completed": completed,
                        "failed": failed,
                    }),
                );
                assert!(!failed, "the actual public Session turn must not fail");
                assert!(completed, "the actual public Session turn must complete");
            })
            .await;
        });
        Self {
            task,
            trace: Arc::clone(trace),
            label,
        }
    }

    async fn finish(mut self) {
        self.trace
            .record("wait_user_turn_finish", json!({"turn": self.label}));
        bounded("held Session task join", &mut self.task)
            .await
            .unwrap();
    }
}

impl Drop for HeldTurn {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn persisted_goal(session: &Arc<Session>, trace: &HeldTrace, step: &str) -> ThreadGoal {
    trace.record("flush_goal_begin", json!({"checkpoint": step}));
    let runtime = session.goal_runtime().unwrap();
    bounded("flush Goal progress", runtime.flush_progress())
        .await
        .unwrap();
    trace.record("read_goal_begin", json!({"checkpoint": step}));
    let goal = bounded("read persisted Goal", runtime.goal())
        .await
        .unwrap()
        .unwrap();
    trace.goal(step, &goal);
    goal
}

fn assert_history(previous: &ThreadGoal, current: &ThreadGoal) {
    assert_eq!(current.goal_id, previous.goal_id);
    assert_eq!(current.status, ThreadGoalStatus::Active);
    assert!(current.tokens_used >= previous.tokens_used);
    assert!(current.time_used_seconds >= previous.time_used_seconds);
}

async fn hold(trace: &HeldTrace, step: &str, duration: Duration) -> Duration {
    trace.record(
        "hold_begin",
        json!({
            "checkpoint": step,
            "minimum_ms": u64::try_from(duration.as_millis()).unwrap(),
        }),
    );
    let started = Instant::now();
    bounded("bounded provider hold", tokio::time::sleep(duration)).await;
    let measured = started.elapsed();
    trace.record(
        "hold_end",
        json!({
            "checkpoint": step,
            "measured_ms": u64::try_from(measured.as_millis()).unwrap(),
        }),
    );
    assert!(measured >= duration);
    measured
}

fn assert_held_time(
    trace: &HeldTrace,
    mode: CollaborationMode,
    before: &ThreadGoal,
    after: &ThreadGoal,
    measured: Duration,
) {
    assert_history(before, after);
    let delta = after.time_used_seconds - before.time_used_seconds;
    trace.record(
        "held_time_result",
        json!({
            "mode": format!("{mode:?}"),
            "measured_ms": u64::try_from(measured.as_millis()).unwrap(),
            "before_seconds": before.time_used_seconds,
            "after_seconds": after.time_used_seconds,
            "delta_seconds": delta,
        }),
    );
    assert_eq!(after.tokens_used, before.tokens_used);
    match mode {
        CollaborationMode::Default => {
            assert!(
                delta >= 2,
                "the held captured Default interval must accrue time"
            );
        }
        CollaborationMode::Plan => {
            assert_eq!(
                delta, 0,
                "the held captured Plan interval must stay neutral"
            );
        }
    }
}

async fn held_time_transition(captured: CollaborationMode, desired: CollaborationMode) {
    let _serial = bounded("serialize held fixture", SERIAL.lock()).await;
    let home = Home::new();
    let trace = Arc::new(HeldTrace::new(captured, desired));
    trace.record(
        "setup_begin",
        json!({"captured": format!("{captured:?}"), "desired": format!("{desired:?}")}),
    );
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let provider = Arc::new(HeldProvider {
        inner: Provider {
            info: ProviderInfo {
                name: "mode-admission".into(),
                base_url: None,
                env_key: None,
                default_model: "mode-admission".into(),
            },
            requests: Mutex::new(Vec::new()),
            request_limit: 3,
        },
        gates: std::array::from_fn(|_| Arc::new(RequestGate::new())),
        next_request: AtomicUsize::new(0),
        trace: Arc::clone(&trace),
    });
    let mut options = agent_options(process.clone());
    options.provider = provider.clone();
    let mut tools = klaus_agent_core::goal::create_goal_tools();
    tools.extend([create_read_tool(), create_write_tool()]);
    options.tools = Some(tools);
    options.include_default_agents = Some(false);
    options.compactor = Some(Arc::new(NoopCompactor));
    let store: Arc<dyn SessionStore> = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(home.root.join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    trace.record("open_session_begin", json!({}));
    let session = bounded(
        "open held fixture Session",
        Agent::new(options).open_session(
            store,
            StartSessionOptions {
                id: Some("mode-admission-held-thread".into()),
                cwd: Some(home.root.to_string_lossy().into_owned().into()),
                user_config_dir: Some(home.root.to_string_lossy().into_owned().into()),
                config: Some(json!({
                    "model": "mode-admission", "goal": true, "prewarm": false,
                    "autoMemory": false, "agentMemory": false, "autoDream": false,
                    "loopDetection": false, "requestUserInputDefault": false,
                })),
                ..Default::default()
            },
        ),
    )
    .await
    .unwrap();
    trace.record("session_opened", json!({}));
    let runtime = session.goal_runtime().unwrap();
    session
        .planning_runtime()
        .set_mode(CollaborationMode::Default, None)
        .unwrap();
    trace.record("authorize_goal_begin", json!({}));
    bounded(
        "authorize warm Goal",
        session.record_user_goal_instruction(
            Some(&"Finish the authorized work.".into()),
            Some(&"active".into()),
        ),
    )
    .await
    .unwrap();
    trace.record("create_goal_begin", json!({}));
    bounded(
        "create warm Goal",
        session.set_thread_goal(
            Some("Finish the authorized work.".into()),
            Some("active".into()),
            None,
        ),
    )
    .await
    .unwrap();

    let warm_turn = HeldTurn::start(&session, &trace, "Seed the historical Default request.");
    provider.wait_entered(0).await;
    let warm_before = persisted_goal(&session, &trace, "warm_admitted").await;
    hold(&trace, "warm_history", WARM_HOLD).await;
    let warm_held = persisted_goal(&session, &trace, "warm_held").await;
    assert_history(&warm_before, &warm_held);
    assert!(warm_held.time_used_seconds - warm_before.time_used_seconds >= 1);
    provider.release(0);
    warm_turn.finish().await;
    let warm = persisted_goal(&session, &trace, "warm_completed").await;
    assert_history(&warm_held, &warm);
    assert_eq!(warm.tokens_used, 10);
    assert!(warm.time_used_seconds >= 1);
    assert_request(
        &provider.inner.requests.lock().unwrap()[0],
        CollaborationMode::Default,
    );

    let planning = session.planning_runtime();
    let before_capture = planning.snapshot().unwrap();
    planning
        .set_mode(captured, Some(before_capture.revision))
        .unwrap();
    trace.record(
        "select_captured_idle_mode",
        json!({"mode": format!("{captured:?}")}),
    );
    bounded(
        "select captured idle accounting mode",
        runtime.synchronize_execution_mode(captured == CollaborationMode::Default),
    )
    .await
    .unwrap();

    let captured_turn = HeldTurn::start(&session, &trace, "Run the held captured request.");
    provider.wait_entered(1).await;
    assert!(runtime.turn_in_flight());
    // Read after actual admission so earlier idle time and fractional carry remain legitimate history.
    let admitted = persisted_goal(&session, &trace, "captured_admitted").await;
    assert_history(&warm, &admitted);
    assert_eq!(admitted.tokens_used, 10);
    hold(&trace, "before_desired_change", Duration::from_millis(150)).await;
    let before_change = persisted_goal(&session, &trace, "before_desired_change").await;
    assert_history(&admitted, &before_change);
    if captured == CollaborationMode::Plan {
        assert_eq!(before_change.time_used_seconds, admitted.time_used_seconds);
    }

    let before_desired = planning.snapshot().unwrap();
    planning
        .set_mode(desired, Some(before_desired.revision))
        .unwrap();
    // This snapshot acknowledges internal desired state; no facade/protocol ACK is exercised.
    let internal_desired = planning.snapshot().unwrap();
    assert_eq!(internal_desired.mode, desired);
    assert!(internal_desired.revision > before_desired.revision);
    trace.record(
        "desired_mode_saved",
        json!({
            "mode": format!("{desired:?}"),
            "revision": internal_desired.revision,
            "in_flight": runtime.turn_in_flight(),
        }),
    );
    assert!(runtime.turn_in_flight());
    trace.record("in_flight_synchronize_begin", json!({}));
    bounded(
        "synchronize desired mode while provider is held",
        runtime.synchronize_execution_mode(desired == CollaborationMode::Default),
    )
    .await
    .unwrap();
    assert!(runtime.turn_in_flight());
    trace.record("in_flight_synchronize_complete", json!({}));
    let at_switch = persisted_goal(&session, &trace, "after_in_flight_synchronize").await;
    assert_history(&before_change, &at_switch);
    if captured == CollaborationMode::Plan {
        assert_eq!(at_switch.time_used_seconds, before_change.time_used_seconds);
    }
    let measured = hold(&trace, "after_desired_change", CAPTURED_HOLD).await;
    let held = persisted_goal(&session, &trace, "captured_held_after_change").await;
    assert_held_time(&trace, captured, &at_switch, &held, measured);
    provider.release(1);
    captured_turn.finish().await;
    let captured_done = persisted_goal(&session, &trace, "captured_completed").await;
    assert_history(&held, &captured_done);
    assert_eq!(
        captured_done.tokens_used,
        if captured == CollaborationMode::Default {
            20
        } else {
            10
        }
    );
    assert_request(&provider.inner.requests.lock().unwrap()[1], captured);
    assert_eq!(planning.snapshot().unwrap(), internal_desired);
    assert_eq!(session.collaboration_mode(), desired);
    if desired == CollaborationMode::Plan {
        bounded(
            "active Plan idle suppression",
            assert_plan_idle(&session, &provider.inner),
        )
        .await;
    }

    let next_turn = HeldTurn::start(&session, &trace, "Run the held next desired request.");
    provider.wait_entered(2).await;
    assert!(runtime.turn_in_flight());
    let next_before = persisted_goal(&session, &trace, "next_admitted").await;
    assert_history(&captured_done, &next_before);
    let next_measured = hold(&trace, "next_desired_mode", CAPTURED_HOLD).await;
    let next_held = persisted_goal(&session, &trace, "next_held").await;
    assert_held_time(&trace, desired, &next_before, &next_held, next_measured);
    provider.release(2);
    next_turn.finish().await;
    let finished = persisted_goal(&session, &trace, "next_completed").await;
    assert_history(&next_held, &finished);
    assert_history(&warm, &finished);
    assert_eq!(finished.tokens_used, 20);
    assert_eq!(planning.snapshot().unwrap(), internal_desired);
    assert_eq!(session.collaboration_mode(), desired);
    assert_request(&provider.inner.requests.lock().unwrap()[2], desired);
    assert_eq!(provider.inner.requests.lock().unwrap().len(), 3);
    if desired == CollaborationMode::Plan {
        bounded(
            "final active Plan idle suppression",
            assert_plan_idle(&session, &provider.inner),
        )
        .await;
        let idle = persisted_goal(&session, &trace, "final_plan_idle").await;
        assert_history(&finished, &idle);
        assert_eq!(idle.tokens_used, finished.tokens_used);
        assert_eq!(idle.time_used_seconds, finished.time_used_seconds);
    }

    let requests = provider.inner.requests.lock().unwrap().clone();
    let exact = runtime_codec::encode_value(&requests).unwrap();
    let artifacts =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/mode-admission-regressions");
    std::fs::write(
        artifacts.join(format!("held-{captured:?}-to-{desired:?}.json")),
        runtime_json::stringify(&exact).to_node_utf8(),
    )
    .unwrap();
    trace.record("shutdown_begin", json!({"request_count": requests.len()}));
    bounded("held Session shutdown", session.shutdown_and_wait())
        .await
        .unwrap();
    trace.record("process_shutdown_begin", json!({}));
    bounded("held process shutdown", process.shutdown())
        .await
        .unwrap();
    trace.record("complete", json!({}));
}

#[tokio::test]
async fn default_to_plan_while_provider_held_keeps_captured_time_and_history() {
    held_time_transition(CollaborationMode::Default, CollaborationMode::Plan).await;
}

#[tokio::test]
async fn plan_to_default_while_provider_held_does_not_start_captured_time() {
    held_time_transition(CollaborationMode::Plan, CollaborationMode::Default).await;
}
