//! Defect: descendant provider requests could miss the root goal or settle twice.
//! Root cause: goal accounting depended on a live root turn instead of a captured generation.
//! Work item 20261017: root-owned descendant billing, idle time and generation isolation.

use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::deps::NoopCompactor;
use klaus_agent_core::engine::session::{Input, Session, TurnOptions};
use klaus_agent_core::goal::types::{ThreadGoal, ThreadGoalStatus};
use klaus_agent_core::runtime_process::{
    remove_process_environment_var, set_process_environment_var, RuntimeProcessContext,
};
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::agent_notification::AgentNotification;
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::tools::builtins::task::{create_task_tool, TaskToolOptions};
use klaus_agent_core::types::abort::AbortSignal;
#[cfg(feature = "testing")]
use klaus_agent_core::types::errors::TerminalReason;
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
#[cfg(feature = "testing")]
use klaus_agent_core::types::tool::{
    ForkContext, SpawnBackgroundAgent, SpawnSubAgentOptions, SwarmSpawn, Tool, ToolContext,
    ToolProgress, ToolResult,
};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Semaphore};

const CHILD: &str = "BILLING_CHILD_DIRECTIVE";
const GRANDCHILD: &str = "BILLING_GRAND_DIRECTIVE";
#[cfg(feature = "testing")]
const RESIDENCY_ROOT: &str = "Fill the resident child pool.";
#[cfg(feature = "testing")]
const RESIDENCY_CHILD: &str = "RESIDENCY_SEED_DIRECTIVE";
#[cfg(feature = "testing")]
const ARM_ROOT: &str = "Establish the current goal request boundary.";
const ROOT: &str = "Start measured delegation.";

struct Home {
    path: std::path::PathBuf,
    previous: Option<String>,
}
impl Home {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("klaus-goal-descendants-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        let previous = std::env::var("KLAUS_CONFIG_DIR").ok();
        set_process_environment_var("KLAUS_CONFIG_DIR", &*path.to_string_lossy());
        Self { path, previous }
    }
}
impl Drop for Home {
    fn drop(&mut self) {
        match &self.previous {
            Some(value) => set_process_environment_var("KLAUS_CONFIG_DIR", value),
            None => remove_process_environment_var("KLAUS_CONFIG_DIR"),
        }
        if std::thread::panicking() {
            eprintln!(
                "Retained descendant accounting fixture: {}",
                self.path.display()
            );
        } else {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

fn usage(tokens: u64) -> Usage {
    Usage {
        input_tokens: tokens - 1,
        output_tokens: 1,
        cache_read_input_tokens: 19,
        cache_creation_input_tokens: 23,
        total_tokens: Some(999),
    }
}
fn sampled_stop(reason: ModelStopReason, tokens: u64) -> Vec<ModelStreamEvent> {
    let usage = usage(tokens);
    vec![
        ModelStreamEvent::UsageSnapshot {
            usage: usage.clone(),
        },
        ModelStreamEvent::UsageSnapshot {
            usage: usage.clone(),
        },
        ModelStreamEvent::MessageStop {
            stop_reason: reason,
            usage,
        },
    ]
}
fn answer(text: &str, tokens: u64) -> Vec<ModelStreamEvent> {
    let mut events = vec![
        ModelStreamEvent::MessageStart {
            model: "descendant-fixture".into(),
        },
        ModelStreamEvent::TextDelta { text: text.into() },
    ];
    events.extend(sampled_stop(ModelStopReason::EndTurn, tokens));
    events
}
fn task(id: &str, prompt: &str, background: bool, tokens: u64) -> Vec<ModelStreamEvent> {
    let input = json!({"model": "low", "description":"Measure scoped work","prompt":prompt,"fork_turns":"none","run_in_background":background});
    tool_turn("task", id, input, tokens)
}
fn tool_turn(name: &str, id: &str, input: serde_json::Value, tokens: u64) -> Vec<ModelStreamEvent> {
    let mut events = vec![
        ModelStreamEvent::MessageStart {
            model: "descendant-fixture".into(),
        },
        ModelStreamEvent::ToolUseStart {
            id: id.into(),
            name: name.into(),
            thought_signature: None,
            tool_kind: None,
            input_format: None,
        },
        ModelStreamEvent::ToolUseInputDelta {
            id: id.into(),
            partial_json: input.to_string().into(),
        },
        ModelStreamEvent::ToolUseStop { id: id.into() },
    ];
    events.extend(sampled_stop(ModelStopReason::ToolUse, tokens));
    events
}
fn contains(request: &ModelRequest, needle: &str) -> bool {
    request.messages.iter().any(|message| match message {
        Message::User(message) if message.is_meta != Some(true) => message
            .content
            .iter()
            .any(|block| matches!(block, ContentBlock::Text(text) if text.text == needle)),
        _ => false,
    })
}

struct DescendantProvider {
    info: ProviderInfo,
    root_calls: AtomicUsize,
    child_calls: AtomicUsize,
    grandchild_calls: AtomicUsize,
    #[cfg(feature = "testing")]
    residency_root_calls: AtomicUsize,
    #[cfg(feature = "testing")]
    residency_child_calls: AtomicUsize,
    #[cfg(feature = "testing")]
    hold_residency: AtomicUsize,
    #[cfg(feature = "testing")]
    residency_started: Arc<Semaphore>,
    #[cfg(feature = "testing")]
    release_residency: Arc<Semaphore>,
    #[cfg(feature = "testing")]
    background_path: AtomicUsize,
    grandchild_started: Arc<Semaphore>,
    release_grandchild: Arc<Semaphore>,
}
impl DescendantProvider {
    fn new() -> Self {
        Self {
            info: ProviderInfo {
                name: "descendant-fixture".into(),
                default_model: "descendant-fixture".into(),
                base_url: None,
                env_key: None,
            },
            root_calls: AtomicUsize::new(0),
            child_calls: AtomicUsize::new(0),
            grandchild_calls: AtomicUsize::new(0),
            #[cfg(feature = "testing")]
            residency_root_calls: AtomicUsize::new(0),
            #[cfg(feature = "testing")]
            residency_child_calls: AtomicUsize::new(0),
            #[cfg(feature = "testing")]
            hold_residency: AtomicUsize::new(0),
            #[cfg(feature = "testing")]
            residency_started: Arc::new(Semaphore::new(0)),
            #[cfg(feature = "testing")]
            release_residency: Arc::new(Semaphore::new(0)),
            #[cfg(feature = "testing")]
            background_path: AtomicUsize::new(0),
            grandchild_started: Arc::new(Semaphore::new(0)),
            release_grandchild: Arc::new(Semaphore::new(0)),
        }
    }
}
impl ModelProvider for DescendantProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }
    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
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
    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        #[cfg(feature = "testing")]
        {
            if contains(&request, ARM_ROOT) && !contains(&request, ROOT) {
                return Box::pin(futures::stream::iter(answer(
                    "Goal request boundary established.",
                    1,
                )));
            }
            if contains(&request, RESIDENCY_CHILD) {
                self.residency_child_calls.fetch_add(1, Ordering::SeqCst);
                if self.hold_residency.load(Ordering::SeqCst) != 0 {
                    let started = self.residency_started.clone();
                    let release = self.release_residency.clone();
                    return Box::pin(async_stream::stream! {
                        started.add_permits(1);
                        tokio::select! {
                            permit = release.acquire() => { permit.unwrap().forget(); },
                            _ = signal.cancelled() => return,
                        }
                        for event in answer("Held resident completed.", 1) { yield event; }
                    });
                }
                return Box::pin(futures::stream::iter(answer("Resident seed completed.", 1)));
            }
            if contains(&request, RESIDENCY_ROOT) && !contains(&request, ROOT) {
                return match self.residency_root_calls.fetch_add(1, Ordering::SeqCst) {
                    call @ 0..=2 => Box::pin(futures::stream::iter(task(
                        &format!("resident-seed-{call}"),
                        RESIDENCY_CHILD,
                        false,
                        1,
                    ))),
                    3 => Box::pin(futures::stream::iter(tool_turn(
                        "capture_spawn",
                        "capture-spawn",
                        json!({}),
                        1,
                    ))),
                    4 => Box::pin(futures::stream::iter(answer("Resident pool is full.", 1))),
                    call => panic!("unexpected residency root request {call}"),
                };
            }
        }
        if contains(&request, GRANDCHILD) {
            assert_eq!(
                self.grandchild_calls.fetch_add(1, Ordering::SeqCst),
                0,
                "one grandchild provider request"
            );
            let started = self.grandchild_started.clone();
            let release = self.release_grandchild.clone();
            return Box::pin(async_stream::stream! {
                started.add_permits(1);
                yield ModelStreamEvent::MessageStart { model:"descendant-fixture".into() };
                yield ModelStreamEvent::TextDelta { text:"Grandchild is working after the root became idle.".into() };
                tokio::select! {
                    permit = release.acquire() => { permit.unwrap().forget(); }
                    _ = signal.cancelled() => { return; }
                }
                for event in sampled_stop(ModelStopReason::EndTurn,13) { yield event; }
            });
        }
        if contains(&request, CHILD) {
            return match self.child_calls.fetch_add(1, Ordering::SeqCst) {
                0 => {
                    assert!(
                        request.tools.iter().any(|tool| tool.name == "task"),
                        "the child must really delegate"
                    );
                    Box::pin(futures::stream::iter(task(
                        "spawn-grandchild",
                        GRANDCHILD,
                        false,
                        7,
                    )))
                }
                1 => Box::pin(futures::stream::iter(answer(
                    "Child completed its nested work.",
                    17,
                ))),
                call => panic!("unexpected child request {call}"),
            };
        }
        match self.root_calls.fetch_add(1, Ordering::SeqCst) {
            0 => {
                assert!(request.tools.iter().any(|tool| tool.name == "task"));
                #[cfg(feature = "testing")]
                if self.background_path.load(Ordering::SeqCst) != 0 {
                    let path = self.background_path.load(Ordering::SeqCst);
                    let input = json!({"model": "low", "description":"Measure scoped work", "prompt":CHILD,
                        "fork_turns":"none", "run_in_background":true, "peer":"billing-peer",
                        "persistent":path == PreparationPath::Persistent as usize});
                    return Box::pin(futures::stream::iter(tool_turn(
                        "task",
                        "spawn-child",
                        input,
                        3,
                    )));
                }
                Box::pin(futures::stream::iter(task("spawn-child", CHILD, true, 3)))
            }
            1 => Box::pin(futures::stream::iter(answer(
                "Root turn completed while the child continues.",
                5,
            ))),
            call => panic!("unexpected root request {call}"),
        }
    }
}

#[cfg(feature = "testing")]
struct CaptureSpawn(std::sync::Weak<Mutex<Option<SpawnBackgroundAgent>>>);

#[cfg(feature = "testing")]
#[async_trait::async_trait]
impl Tool for CaptureSpawn {
    fn static_name(&self) -> &str {
        "capture_spawn"
    }
    fn static_description(&self) -> &str {
        "Retain the current child creation callback for admission cancellation."
    }
    fn persistence_identity(&self) -> Option<RuntimeString> {
        Some("fixture:goal-capture-spawn:1".into())
    }
    fn input_schema(&self) -> klaus_agent_core::types::json::Json {
        klaus_agent_core::runtime_values::runtime_json::parse(&"{\"type\":\"object\"}".into())
            .unwrap()
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_read_only(&self, _: &klaus_agent_core::types::json::Json) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _: &klaus_agent_core::types::json::Json) -> bool {
        true
    }
    async fn call(
        &self,
        _: klaus_agent_core::types::json::Json,
        context: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, klaus_agent_core::types::errors::ToolError> {
        *self
            .0
            .upgrade()
            .expect("fixture owns the callback slot")
            .lock()
            .unwrap() = Some(
            context
                .spawn_background_agent
                .clone()
                .expect("root exposes child creation"),
        );
        Ok(klaus_agent_core::tools::build_tool::text_result(
            klaus_agent_core::types::json::Json::Null,
            "Child creation callback retained.",
            false,
        ))
    }
}

struct Fixture {
    session: Arc<Session>,
    provider: Arc<DescendantProvider>,
    process: RuntimeProcessContext,
    phase_origin: Instant,
    phases: Arc<Mutex<Vec<(&'static str, u128)>>>,
    receipts: mpsc::Receiver<AgentNotification>,
    #[cfg(feature = "testing")]
    spawn: Arc<Mutex<Option<SpawnBackgroundAgent>>>,
    _agent: Agent,
}
impl Fixture {
    async fn new(home: &Home) -> Self {
        Self::new_with_swarm(home, None).await
    }

    async fn new_with_swarm(
        home: &Home,
        swarm: Option<klaus_agent_core::engine::session::SwarmIdentity>,
    ) -> Self {
        let process = RuntimeProcessContext::initialize(
            super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
        )
        .unwrap();
        let provider = Arc::new(DescendantProvider::new());
        let mut options =
            super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
        options.provider = provider.clone();
        options.tools = Some(vec![create_task_tool(TaskToolOptions::default())]);
        #[cfg(feature = "testing")]
        let spawn = {
            let spawn = Arc::new(Mutex::new(None));
            options
                .tools
                .as_mut()
                .unwrap()
                .push(Arc::new(CaptureSpawn(Arc::downgrade(&spawn))));
            spawn
        };
        options.compactor = Some(Arc::new(NoopCompactor));
        options.cwd = Some(home.path.to_string_lossy().into_owned());
        let agent = Agent::new(options);
        let store: Arc<dyn SessionStore> =
            Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
                dir: Some(home.path.join("sessions").to_string_lossy().into_owned()),
                archive_dir: None,
            }));
        let session = agent.open_session(store,StartSessionOptions { swarm, cwd:Some(home.path.to_string_lossy().to_string().into()),user_config_dir:Some(home.path.to_string_lossy().to_string().into()),config:Some(json!({"model":"descendant-fixture","goal":true,"prewarm":false,"autoMemory":false,"agentMemory":false,"maxRetries":0,"forwardSubagentText":false,"maxConcurrentSubAgents":3})),..Default::default() }).await.unwrap();
        let phase_origin = std::time::Instant::now();
        let phases = Arc::new(Mutex::new(Vec::new()));
        #[cfg(feature = "testing")]
        {
            let phases = phases.clone();
            session
                .goal_runtime()
                .expect("persistent fixture has goal runtime")
                .testing_set_phase_observer(Some(Arc::new(move |phase| {
                    let mut trace = phases.lock().unwrap();
                    if trace.len() == 256 {
                        trace.remove(0);
                    }
                    trace.push((phase, phase_origin.elapsed().as_millis()));
                })));
        }
        let (send, receipts) = mpsc::channel(16);
        session.set_agent_notification_sink(Some(Arc::new(move |receipt| {
            send.try_send(receipt.clone())
                .expect("bounded descendant receipt fixture");
        })));
        Self {
            session,
            provider,
            process,
            phase_origin,
            phases,
            receipts,
            #[cfg(feature = "testing")]
            spawn,
            _agent: agent,
        }
    }
    async fn run_root_to_idle(&self) {
        let mut events = Vec::new();
        let mut stream = self
            .session
            .run_streamed(Input::Text(ROOT.into()), TurnOptions::default())
            .events;
        let finished = tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(event) = stream.next().await {
                assert!(events.len() < 256, "bounded root event fixture");
                events.push(event);
            }
        })
        .await;
        assert!(
            finished.is_ok(),
            "root request stalled: elapsed_ms={}, phases={:?}, root={}, child={}, grandchild={}, lifecycle={:?}, root_events={events:?}",
            self.phase_origin.elapsed().as_millis(),
            self.phases.lock().unwrap(),
            self.provider.root_calls.load(Ordering::SeqCst),
            self.provider.child_calls.load(Ordering::SeqCst),
            self.provider.grandchild_calls.load(Ordering::SeqCst),
            self.session.subagent_lifecycle_snapshot()
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event, EngineEvent::TurnCompleted { .. })),
            "root must finish before descendant usage arrives: {events:?}"
        );
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
            "root delegation failed: {events:?}"
        );
        let entered = tokio::time::timeout(
            Duration::from_secs(5),
            self.provider.grandchild_started.acquire(),
        )
        .await;
        assert!(
            entered.is_ok(),
            "nested request never entered: elapsed_ms={}, phases={:?}, root={}, child={}, grandchild={}, lifecycle={:?}, root_events={events:?}",
            self.phase_origin.elapsed().as_millis(),
            self.phases.lock().unwrap(),
            self.provider.root_calls.load(Ordering::SeqCst),
            self.provider.child_calls.load(Ordering::SeqCst),
            self.provider.grandchild_calls.load(Ordering::SeqCst),
            self.session.subagent_lifecycle_snapshot()
        );
        entered.unwrap().unwrap().forget();
        assert_eq!(self.provider.root_calls.load(Ordering::SeqCst), 2);
        assert_eq!(self.provider.child_calls.load(Ordering::SeqCst), 1);
        assert_eq!(self.provider.grandchild_calls.load(Ordering::SeqCst), 1);
    }
    async fn finish_descendants(&mut self, root_calls: usize) {
        self.provider.release_grandchild.add_permits(1);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let receipt = self
                    .receipts
                    .recv()
                    .await
                    .expect("child completion must reach the root");
                assert_ne!(receipt.status, "failed", "descendant failed: {receipt:?}");
                if receipt.status == "completed" {
                    break;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(
            self.provider.root_calls.load(Ordering::SeqCst),
            root_calls,
            "descendant billing must not invoke an idle root"
        );
        assert_eq!(self.provider.child_calls.load(Ordering::SeqCst), 2);
        assert_eq!(self.provider.grandchild_calls.load(Ordering::SeqCst), 1);
    }
    async fn close(self) {
        #[cfg(feature = "testing")]
        self.spawn.lock().unwrap().take();
        self.session.dispose().await;
        self.process.shutdown().await.unwrap();
    }
}
async fn persisted(thread_id: &RuntimeString) -> ThreadGoal {
    klaus_agent_core::goal::store::get_goal(&thread_id.try_to_utf8().unwrap())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_nested_requests_bill_one_root_goal_after_the_root_is_idle() {
    let home = Home::new();
    let mut fixture = Fixture::new(&home).await;
    let measured = Instant::now();
    let created = fixture
        .session
        .set_thread_goal(
            Some("Complete delegated work and retain exact usage.".into()),
            None,
            Some(Some(1000)),
        )
        .await
        .unwrap();
    fixture.run_root_to_idle().await;
    assert_eq!(
        persisted(&created.goal.thread_id).await.tokens_used,
        15,
        "root requests3+5 and child delegation7 settle before the held grandchild"
    );
    // The root is idle and both descendant lifetimes overlap. Only the root's
    // goal clock may accrue this real elapsed interval.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let idle = fixture.session.thread_goal().await.unwrap().unwrap();
    assert!(
        idle.time_used_seconds >= 1,
        "an active goal clock includes idle-root work"
    );
    assert!(
        idle.time_used_seconds <= i64::try_from(measured.elapsed().as_secs()).unwrap(),
        "overlapping descendant clocks must not be added to the root: {idle:?}"
    );
    assert_eq!(
        idle.tokens_used, 15,
        "held descendants have not reported their terminal usage"
    );
    fixture.finish_descendants(2).await;
    let completed = persisted(&created.goal.thread_id).await;
    assert_eq!(completed.goal_id, created.goal.goal_id);
    assert_eq!(completed.status, ThreadGoalStatus::Active);
    assert_eq!(
        completed.tokens_used, 45,
        "root3+5 +child7+17 +grandchild13, each request exactly once despite duplicate snapshots"
    );
    fixture.close().await;
    assert_eq!(
        persisted(&created.goal.thread_id).await.tokens_used,
        45,
        "descendant cleanup must not debit terminal usage twice"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn old_nested_requests_cannot_charge_a_replaced_root_goal_generation() {
    let home = Home::new();
    let mut fixture = Fixture::new(&home).await;
    let first = fixture
        .session
        .set_thread_goal(Some("Original delegated objective.".into()), None, None)
        .await
        .unwrap();
    fixture.run_root_to_idle().await;
    assert_eq!(persisted(&first.goal.thread_id).await.tokens_used, 15);
    fixture.session.clear_thread_goal().await.unwrap();
    let replacement = fixture
        .session
        .set_thread_goal(
            Some("Independent replacement objective.".into()),
            None,
            None,
        )
        .await
        .unwrap();
    assert_ne!(replacement.goal.goal_id, first.goal.goal_id);
    assert_eq!(replacement.goal.tokens_used, 0);
    fixture.finish_descendants(2).await;
    let saved = persisted(&replacement.goal.thread_id).await;
    assert_eq!(saved.goal_id, replacement.goal.goal_id);
    assert_eq!(saved.status, ThreadGoalStatus::Active);
    assert_eq!(
        saved.tokens_used, 0,
        "late child and grandchild usage belongs only to the captured old generation"
    );
    fixture.close().await;
    assert_eq!(persisted(&replacement.goal.thread_id).await.tokens_used, 0);
}

#[cfg(feature = "testing")]
struct PreparationGate {
    entered: Arc<Semaphore>,
    unloaded: Arc<Semaphore>,
    release: Option<std::sync::mpsc::SyncSender<()>>,
}

#[cfg(feature = "testing")]
impl PreparationGate {
    fn install(fixture: &Fixture) -> Self {
        let entered = Arc::new(Semaphore::new(0));
        let unloaded = Arc::new(Semaphore::new(0));
        let unloaded_signal = unloaded.clone();
        let (release, receive) = std::sync::mpsc::sync_channel(1);
        let receive = Mutex::new(receive);
        let first = AtomicUsize::new(0);
        let phases = fixture.phases.clone();
        let origin = fixture.phase_origin;
        let signal = entered.clone();
        fixture
            .session
            .goal_runtime()
            .expect("persistent fixture has goal runtime")
            .testing_set_phase_observer(Some(Arc::new(move |phase| {
                {
                    let mut trace = phases.lock().unwrap();
                    if trace.len() == 256 {
                        trace.remove(0);
                    }
                    trace.push((phase, origin.elapsed().as_millis()));
                }
                if phase == "subagent.residency.unloaded" {
                    unloaded_signal.add_permits(1);
                }
                if phase == "subagent.residency.flush"
                    && first
                        .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                {
                    signal.add_permits(1);
                    // Pause the real eviction owner before its durability wait.
                    // Other runtime workers continue polling root admission.
                    tokio::task::block_in_place(|| {
                        receive.lock().unwrap().recv().unwrap();
                    });
                }
            })));
        Self {
            entered,
            unloaded,
            release: Some(release),
        }
    }

    fn release(&mut self) {
        if let Some(release) = self.release.take() {
            release.try_send(()).unwrap();
        }
    }
}

#[cfg(feature = "testing")]
impl Drop for PreparationGate {
    fn drop(&mut self) {
        // A failed assertion must also unblock the real worker before runtime
        // teardown waits for it. The channel owns exactly one release token.
        if let Some(release) = self.release.take() {
            let _ = release.try_send(());
        }
    }
}

#[cfg(feature = "testing")]
impl Fixture {
    async fn fill_residency(&self) {
        let mut stream = self
            .session
            .run_streamed(Input::Text(RESIDENCY_ROOT.into()), TurnOptions::default())
            .events;
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut count = 0;
            while let Some(event) = stream.next().await {
                count += 1;
                assert!(count < 256, "bounded residency seed events");
                assert!(
                    !matches!(event, EngineEvent::TurnFailed { .. }),
                    "residency seed failed: {event:?}"
                );
            }
            while self.session.goal_runtime().unwrap().has_descendant_work()
                || self
                    .session
                    .background_agent_summaries()
                    .iter()
                    .any(|agent| {
                        agent.status == klaus_agent_core::types::tool::BackgroundTaskStatus::Running
                    })
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("real seed workers must finish before measured delegation");
        assert_eq!(self.provider.residency_root_calls.load(Ordering::SeqCst), 5);
        assert!(
            self.spawn.lock().unwrap().is_some(),
            "real tool context must expose child creation"
        );
        assert_eq!(
            self.provider.residency_child_calls.load(Ordering::SeqCst),
            3
        );
        let lifecycle = self.session.subagent_lifecycle_snapshot();
        assert_eq!(lifecycle.agents.len(), 3);
        assert!(lifecycle.agents.iter().all(|agent| agent.status
            == klaus_agent_core::engine::subagent_lifecycle::SubagentStatus::Completed));
    }

    async fn run_root_to_preparing_child(
        &self,
        gate: &PreparationGate,
    ) -> tokio::task::JoinHandle<Vec<EngineEvent>> {
        let mut stream = self
            .session
            .run_streamed(Input::Text(ROOT.into()), TurnOptions::default())
            .events;
        let root = tokio::spawn(async move {
            let mut events = Vec::new();
            while let Some(event) = stream.next().await {
                assert!(events.len() < 256, "bounded root event fixture");
                events.push(event);
            }
            events
        });
        let entered = tokio::time::timeout(Duration::from_secs(5), gate.entered.acquire()).await;
        assert!(
            entered.is_ok(),
            "real residency unload never reached persistence: phases={:?}, lifecycle={:?}",
            self.phases.lock().unwrap(),
            self.session.subagent_lifecycle_snapshot()
        );
        entered.unwrap().unwrap().forget();
        assert!(
            !root.is_finished(),
            "root must await residency and initial child input acceptance"
        );
        assert_eq!(self.provider.root_calls.load(Ordering::SeqCst), 1);
        assert_eq!(self.provider.child_calls.load(Ordering::SeqCst), 0);
        assert_eq!(self.provider.grandchild_calls.load(Ordering::SeqCst), 0);
        root
    }

    async fn finish_root(
        &self,
        root: tokio::task::JoinHandle<Vec<EngineEvent>>,
        expected: TerminalReason,
        root_calls: usize,
    ) {
        let events = tokio::time::timeout(Duration::from_secs(5), root)
            .await
            .expect("released admission must settle the root")
            .expect("root event driver must not panic");
        assert!(
            events.iter().any(|event| matches!(
                event,
                EngineEvent::TurnCompleted { stop_reason, .. } if *stop_reason == expected
            )),
            "root must settle with {expected:?}: {events:?}"
        );
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
            "root admission must settle without failure: {events:?}"
        );
        assert_eq!(self.provider.root_calls.load(Ordering::SeqCst), root_calls);
    }

    fn start_child(
        &self,
        path: PreparationPath,
        opts: SpawnSubAgentOptions,
    ) -> tokio::task::JoinHandle<AdmissionResult> {
        let opts = path.options(opts);
        if matches!(path, PreparationPath::Direct | PreparationPath::Isolated) {
            let stream = self.session.testing_run_uncaptured_subagent(
                opts,
                format!("goal-direct-{}", uuid::Uuid::new_v4()),
                matches!(path, PreparationPath::Isolated),
            );
            tokio::spawn(async move { AdmissionResult::Stream(collect_child_stream(stream).await) })
        } else {
            let spawn = self.spawn.lock().unwrap().clone().unwrap();
            tokio::spawn(async move { AdmissionResult::Background(spawn(opts).await) })
        }
    }

    async fn arm_root_request(&self) {
        let events = tokio::time::timeout(
            Duration::from_secs(5),
            self.session
                .run_streamed(Input::Text(ARM_ROOT.into()), TurnOptions::default())
                .events
                .collect::<Vec<_>>(),
        )
        .await
        .expect("arming root request must finish");
        assert!(events.iter().any(|event| matches!(
            event,
            EngineEvent::TurnCompleted {
                stop_reason: TerminalReason::Completed,
                ..
            }
        )));
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
            "arming request failed: {events:?}"
        );
    }

    async fn await_first_root_usage(&self, goal: &ThreadGoal) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                // Root accounting normally flushes after tool dispatch. Its
                // real provider response can settle while residency is blocked.
                self.session
                    .goal_runtime()
                    .unwrap()
                    .flush_progress()
                    .await
                    .unwrap();
                let saved = persisted(&goal.thread_id).await;
                assert_eq!(saved.goal_id, goal.goal_id);
                if saved.tokens_used == 3 {
                    break;
                }
                assert!(
                    saved.tokens_used < 3,
                    "only the first root request can settle"
                );
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the admitted first root request must settle its terminal usage");
    }

    async fn await_descendant_release(&self) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.session.goal_runtime().unwrap().has_descendant_work() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("completed or refused preparation must release every descendant lease");
    }

    async fn assert_preparing_child_blocks_idle(&self, root_calls: usize) {
        assert!(
            self.session.goal_runtime().unwrap().has_descendant_work(),
            "the preparation lifetime must already own a descendant lease"
        );
        assert!(
            self.session
                .run_goal_idle_streamed(TurnOptions::default())
                .await
                .unwrap()
                .is_none(),
            "a real preparing child must refuse idle continuation admission"
        );
        assert_eq!(self.provider.root_calls.load(Ordering::SeqCst), root_calls);
        assert_eq!(self.provider.child_calls.load(Ordering::SeqCst), 0);
        assert_eq!(self.provider.grandchild_calls.load(Ordering::SeqCst), 0);
    }
}

#[cfg(feature = "testing")]
#[derive(Clone, Copy, Debug)]
#[repr(usize)]
enum PreparationPath {
    Ordinary = 0,
    Persistent = 1,
    External = 2,
    Direct = 3,
    Isolated = 4,
}

#[cfg(feature = "testing")]
impl PreparationPath {
    fn all() -> [Self; 5] {
        [
            Self::Ordinary,
            Self::Persistent,
            Self::External,
            Self::Direct,
            Self::Isolated,
        ]
    }

    fn options(self, mut opts: SpawnSubAgentOptions) -> SpawnSubAgentOptions {
        match self {
            Self::Persistent => {
                opts.persistent = Some(true);
                opts.swarm = Some(SwarmSpawn {
                    peer: "billing-peer".into(),
                });
            }
            Self::External => {
                opts.persistent = Some(false);
                opts.swarm = Some(SwarmSpawn {
                    peer: "billing-peer".into(),
                });
            }
            _ => {}
        }
        opts
    }
}

#[cfg(feature = "testing")]
#[derive(Debug)]
enum AdmissionResult {
    Background(Result<klaus_agent_core::types::tool::SpawnBackgroundAgentResult, RuntimeString>),
    Stream(Vec<EngineEvent>),
}

#[cfg(feature = "testing")]
async fn collect_child_stream(
    mut stream: klaus_agent_core::engine::session::EngineEventStream,
) -> Vec<EngineEvent> {
    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        assert!(events.len() < 256, "bounded direct child event fixture");
        events.push(event);
    }
    events
}

#[cfg(feature = "testing")]
impl AdmissionResult {
    fn assert_shutdown_refused(self) {
        let error = match self {
            Self::Background(result) => result.expect_err("shutdown must reject input acceptance"),
            Self::Stream(events) => events
                .into_iter()
                .find_map(|event| match event {
                    EngineEvent::TurnFailed { error, .. } => Some(error.message),
                    _ => None,
                })
                .expect("direct creation must report its shutdown refusal"),
        };
        let message = error.try_to_utf8().unwrap();
        assert!(
            message.contains("shutting down")
                || message == "cannot start inference on a closing session"
                || message == "Sub-agent creation was cancelled."
                || message == "Sub-agent execution was interrupted.",
            "valid configuration must fail at the real shutdown boundary: {message}"
        );
    }
}

#[cfg(feature = "testing")]
enum PreparationGoalChange {
    Clear,
    Replace,
    Create,
}

#[cfg(feature = "testing")]
async fn assert_preparing_child_keeps_captured_authority(
    change: PreparationGoalChange,
    path: PreparationPath,
) {
    let home = Home::new();
    let mut fixture = Fixture::new(&home).await;
    fixture.fill_residency().await;
    fixture
        .provider
        .background_path
        .store(path as usize, Ordering::SeqCst);
    let original = if matches!(change, PreparationGoalChange::Create) {
        None
    } else {
        Some(
            fixture
                .session
                .set_thread_goal(Some("Original delegated objective.".into()), None, None)
                .await
                .unwrap()
                .goal,
        )
    };
    let mut gate = PreparationGate::install(&fixture);
    let root = fixture.run_root_to_preparing_child(&gate).await;
    fixture.assert_preparing_child_blocks_idle(1).await;
    if let Some(original) = &original {
        fixture.await_first_root_usage(original).await;
        assert_eq!(
            persisted(&original.thread_id).await.tokens_used,
            3,
            "only the first root request has settled before residency admission"
        );
        assert!(fixture.session.clear_thread_goal().await.unwrap());
    } else {
        assert!(fixture.session.thread_goal().await.unwrap().is_none());
    }
    assert!(fixture.session.thread_goal().await.unwrap().is_none());
    fixture.assert_preparing_child_blocks_idle(1).await;
    let replacement = if matches!(change, PreparationGoalChange::Clear) {
        None
    } else {
        let replacement = fixture
            .session
            .set_thread_goal(Some("Independent later objective.".into()), None, None)
            .await
            .unwrap()
            .goal;
        if let Some(original) = &original {
            assert_ne!(replacement.goal_id, original.goal_id);
        }
        assert_eq!(replacement.status, ThreadGoalStatus::Active);
        assert_eq!(replacement.tokens_used, 0);
        fixture.assert_preparing_child_blocks_idle(1).await;
        Some(replacement)
    };

    // Release real residency admission and initial input acceptance before
    // collecting the root and the nested provider requests.
    gate.release();
    fixture.finish_root(root, TerminalReason::Aborted, 1).await;
    tokio::time::timeout(
        Duration::from_secs(5),
        fixture.provider.grandchild_started.acquire(),
    )
    .await
    .expect("released preparation must reach the real nested provider")
    .unwrap()
    .forget();
    assert_eq!(fixture.provider.child_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.provider.grandchild_calls.load(Ordering::SeqCst), 1);
    fixture.finish_descendants(1).await;
    fixture.await_descendant_release().await;
    match &replacement {
        Some(replacement) => {
            let saved = persisted(&replacement.thread_id).await;
            assert_eq!(saved.goal_id, replacement.goal_id);
            assert_eq!(saved.status, ThreadGoalStatus::Active);
            assert_eq!(
                saved.tokens_used, 0,
                "delayed child7+17 and grandchild13 must retain the captured authority"
            );
        }
        None => assert!(fixture.session.thread_goal().await.unwrap().is_none()),
    }
    fixture.close().await;
    if let Some(replacement) = &replacement {
        assert_eq!(persisted(&replacement.thread_id).await.tokens_used, 0);
    }
}

#[cfg(feature = "testing")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preparing_task_keeps_descendant_work_after_the_root_goal_is_cleared() {
    assert_preparing_child_keeps_captured_authority(
        PreparationGoalChange::Clear,
        PreparationPath::Ordinary,
    )
    .await;
}

#[cfg(feature = "testing")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preparing_task_cannot_adopt_a_replacement_root_goal() {
    assert_preparing_child_keeps_captured_authority(
        PreparationGoalChange::Replace,
        PreparationPath::Ordinary,
    )
    .await;
}

#[cfg(feature = "testing")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preparing_task_without_a_goal_cannot_adopt_a_later_root_goal() {
    assert_preparing_child_keeps_captured_authority(
        PreparationGoalChange::Create,
        PreparationPath::Ordinary,
    )
    .await;
}

#[cfg(feature = "testing")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn persistent_and_external_preparation_keep_the_initial_goal_generation() {
    for path in [PreparationPath::Persistent, PreparationPath::External] {
        for change in [
            PreparationGoalChange::Replace,
            PreparationGoalChange::Create,
        ] {
            assert_preparing_child_keeps_captured_authority(change, path).await;
        }
    }
}

#[cfg(feature = "testing")]
async fn replace_direct_goal(fixture: &Fixture, original: Option<&ThreadGoal>) -> ThreadGoal {
    if let Some(original) = original {
        assert_eq!(
            persisted(&original.thread_id).await.tokens_used,
            1,
            "the arming root request belongs to the original goal"
        );
        assert!(fixture.session.clear_thread_goal().await.unwrap());
    } else {
        assert!(fixture.session.thread_goal().await.unwrap().is_none());
    }
    let goal = fixture
        .session
        .set_thread_goal(
            Some("Independent direct-wrapper objective.".into()),
            None,
            None,
        )
        .await
        .unwrap()
        .goal;
    if let Some(original) = original {
        assert_ne!(goal.goal_id, original.goal_id);
    }
    assert_eq!(goal.tokens_used, 0);
    goal
}

#[cfg(feature = "testing")]
async fn assert_direct_capture(path: PreparationPath, create: bool, before_first_poll: bool) {
    let home = Home::new();
    let fixture = Fixture::new(&home).await;
    fixture.fill_residency().await;
    let original = if create {
        None
    } else {
        Some(
            fixture
                .session
                .set_thread_goal(
                    Some("Original direct-wrapper objective.".into()),
                    None,
                    None,
                )
                .await
                .unwrap()
                .goal,
        )
    };
    fixture.arm_root_request().await;
    let mut gate = PreparationGate::install(&fixture);
    let stream = fixture.session.testing_run_uncaptured_subagent(
        SpawnSubAgentOptions {
            prompt: CHILD.into(),
            fork_context: Some(ForkContext::None),
            ..Default::default()
        },
        format!("direct-capture-{}", uuid::Uuid::new_v4()),
        matches!(path, PreparationPath::Isolated),
    );
    let early_goal = if before_first_poll {
        assert!(
            fixture
                .session
                .goal_runtime()
                .unwrap()
                .has_descendant_work(),
            "creating the real wrapper stream must already own its lease"
        );
        Some(replace_direct_goal(&fixture, original.as_ref()).await)
    } else {
        None
    };
    let driver = tokio::spawn(collect_child_stream(stream));
    tokio::time::timeout(Duration::from_secs(5), gate.entered.acquire())
        .await
        .expect("direct wrapper must reach real residency durability")
        .unwrap()
        .forget();
    fixture.assert_preparing_child_blocks_idle(0).await;
    let replacement = match early_goal {
        Some(goal) => goal,
        None => replace_direct_goal(&fixture, original.as_ref()).await,
    };
    fixture.assert_preparing_child_blocks_idle(0).await;
    gate.release();
    tokio::time::timeout(
        Duration::from_secs(5),
        fixture.provider.grandchild_started.acquire(),
    )
    .await
    .expect("released direct child must reach its real nested provider")
    .unwrap()
    .forget();
    fixture.provider.release_grandchild.add_permits(1);
    let events = tokio::time::timeout(Duration::from_secs(5), driver)
        .await
        .expect("direct child stream must finish")
        .expect("direct child driver must not panic");
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{path:?}: {events:?}"
    );
    assert!(
        events.iter().any(|event| matches!(
            event,
            EngineEvent::TurnCompleted {
                stop_reason: TerminalReason::Completed,
                ..
            }
        )),
        "{path:?}: {events:?}"
    );
    assert_eq!(fixture.provider.root_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.provider.child_calls.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.provider.grandchild_calls.load(Ordering::SeqCst), 1);
    fixture.await_descendant_release().await;
    let saved = persisted(&replacement.thread_id).await;
    assert_eq!(saved.goal_id, replacement.goal_id);
    assert_eq!(
        saved.tokens_used, 0,
        "{path:?}: child7+17 and grandchild13 must keep their captured generation"
    );
    fixture.close().await;
    assert_eq!(persisted(&replacement.thread_id).await.tokens_used, 0);
}

#[cfg(feature = "testing")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn direct_and_isolated_residency_waits_cannot_adopt_a_later_goal() {
    for path in [PreparationPath::Direct, PreparationPath::Isolated] {
        for create in [false, true] {
            assert_direct_capture(path, create, false).await;
        }
    }
}

#[cfg(feature = "testing")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn isolated_stream_creation_captures_goal_before_setup_is_first_polled() {
    for create in [false, true] {
        assert_direct_capture(PreparationPath::Isolated, create, true).await;
    }
}

#[cfg(feature = "testing")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn persistent_initial_completion_releases_goal_lease_while_the_mailbox_stays_live() {
    let home = Home::new();
    let team: RuntimeString = format!("goal-peer-{}", uuid::Uuid::new_v4()).into();
    klaus_agent_core::tasks::swarm_store::write_team_config(
        &klaus_agent_core::tasks::swarm_store::TeamConfigInput {
            team_name: team.clone(),
            description: None,
            agent_type: None,
            session_id: "billing-root".into(),
            model: "descendant-fixture".into(),
            cwd: home.path.to_string_lossy().to_string().into(),
            created_at: 1,
            joined_at: 1,
        },
    )
    .await
    .unwrap();
    let fixture = Fixture::new_with_swarm(
        &home,
        Some(klaus_agent_core::engine::session::SwarmIdentity {
            team,
            peer: "lead".into(),
        }),
    )
    .await;
    fixture.fill_residency().await;
    fixture
        .provider
        .background_path
        .store(PreparationPath::Persistent as usize, Ordering::SeqCst);
    let original = fixture
        .session
        .set_thread_goal(Some("Initial peer objective.".into()), None, None)
        .await
        .unwrap()
        .goal;
    let mut gate = PreparationGate::install(&fixture);
    let root = fixture.run_root_to_preparing_child(&gate).await;
    fixture.assert_preparing_child_blocks_idle(1).await;
    fixture.await_first_root_usage(&original).await;
    fixture.session.clear_thread_goal().await.unwrap();
    let replacement = fixture
        .session
        .set_thread_goal(Some("Independent mailbox objective.".into()), None, None)
        .await
        .unwrap()
        .goal;
    gate.release();
    fixture.finish_root(root, TerminalReason::Aborted, 1).await;
    tokio::time::timeout(
        Duration::from_secs(5),
        fixture.provider.grandchild_started.acquire(),
    )
    .await
    .expect("persistent peer must reach nested work")
    .unwrap()
    .forget();
    fixture.provider.release_grandchild.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let peer_idle = fixture
                .session
                .background_agent_summaries()
                .iter()
                .any(|agent| {
                    agent.teammate
                        && agent.idle
                        && agent.status
                            == klaus_agent_core::types::tool::BackgroundTaskStatus::Running
                });
            if peer_idle && fixture.provider.child_calls.load(Ordering::SeqCst) == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("accepted peer must remain idle with a live mailbox after its initial input");
    fixture.await_descendant_release().await;
    assert_eq!(fixture.provider.root_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.provider.child_calls.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.provider.grandchild_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        persisted(&replacement.thread_id).await.tokens_used,
        0,
        "initial child7+17 and grandchild13 cannot debit a later mailbox objective"
    );
    fixture.close().await;
    assert_eq!(persisted(&replacement.thread_id).await.tokens_used, 0);
}

#[cfg(feature = "testing")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_residency_admission_releases_its_captured_descendant_lease() {
    for path in PreparationPath::all() {
        let home = Home::new();
        let fixture = Fixture::new(&home).await;
        fixture.fill_residency().await;
        let goal = fixture
            .session
            .set_thread_goal(Some("Cancel a preparing child.".into()), None, None)
            .await
            .unwrap()
            .goal;
        let mut gate = PreparationGate::install(&fixture);
        let pending = fixture.start_child(
            path,
            SpawnSubAgentOptions {
                prompt: CHILD.into(),
                fork_context: Some(ForkContext::None),
                ..Default::default()
            },
        );
        tokio::time::timeout(Duration::from_secs(5), gate.entered.acquire())
            .await
            .expect("real child creation must reach residency persistence")
            .unwrap()
            .forget();
        fixture.assert_preparing_child_blocks_idle(0).await;

        pending.abort();
        assert!(pending.await.unwrap_err().is_cancelled());
        fixture.await_descendant_release().await;
        assert_eq!(fixture.provider.child_calls.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.provider.grandchild_calls.load(Ordering::SeqCst), 0);
        assert_eq!(persisted(&goal.thread_id).await.tokens_used, 0);

        // The cancelled caller releases its lease while the independent eviction
        // owner still holds the real runtime pin. Let that owner finish before teardown.
        gate.release();
        tokio::time::timeout(Duration::from_secs(5), gate.unloaded.acquire())
            .await
            .expect("cancelled admission must let the eviction owner finish")
            .unwrap()
            .forget();
        fixture.close().await;
        assert_eq!(persisted(&goal.thread_id).await.tokens_used, 0);
    }
}

#[cfg(feature = "testing")]
async fn assert_role_boundary_rejection(opts: SpawnSubAgentOptions, expected: &str) {
    for execution_full in [false, true] {
        let home = Home::new();
        let fixture = Fixture::new(&home).await;
        fixture.fill_residency().await;
        if execution_full {
            fixture.provider.hold_residency.store(1, Ordering::SeqCst);
            let spawn = fixture.spawn.lock().unwrap().clone().unwrap();
            for _ in 0..3 {
                spawn(SpawnSubAgentOptions {
                    prompt: RESIDENCY_CHILD.into(),
                    fork_context: Some(ForkContext::None),
                    ..Default::default()
                })
                .await
                .expect("valid held workers must fill execution capacity");
                tokio::time::timeout(
                    Duration::from_secs(5),
                    fixture.provider.residency_started.acquire(),
                )
                .await
                .expect("an admitted worker must reach the real provider")
                .unwrap()
                .forget();
            }
            assert_eq!(
                fixture
                    .session
                    .background_agent_summaries()
                    .iter()
                    .filter(|agent| agent.status
                        == klaus_agent_core::types::tool::BackgroundTaskStatus::Running)
                    .count(),
                3
            );
        }
        let before = fixture.session.subagent_lifecycle_snapshot();
        let seed_calls = fixture
            .provider
            .residency_child_calls
            .load(Ordering::SeqCst);
        let goal = fixture
            .session
            .set_thread_goal(Some("Validate child role admission.".into()), None, None)
            .await
            .unwrap()
            .goal;
        fixture.arm_root_request().await;
        let mut gate = PreparationGate::install(&fixture);
        for path in PreparationPath::all() {
            let pending = fixture.start_child(path, opts.clone());
            let outcome = tokio::time::timeout(Duration::from_secs(5), pending)
                .await
                .expect("invalid child settings must settle admission")
                .expect("admission driver must not panic");
            let error = match outcome {
                AdmissionResult::Background(result) => {
                    result.expect_err("invalid child settings must reject input acceptance")
                }
                AdmissionResult::Stream(events) => {
                    assert!(
                        !events
                            .iter()
                            .any(|event| matches!(event, EngineEvent::TurnStarted { .. })),
                        "preflight rejection cannot start a child turn: {events:?}"
                    );
                    events
                        .into_iter()
                        .find_map(|event| match event {
                            EngineEvent::TurnFailed { error, .. } => Some(error.message),
                            _ => None,
                        })
                        .expect("the real direct wrapper must report its configuration error")
                }
            };
            let message = error.try_to_utf8().unwrap();
            assert!(
                message.contains(expected),
                "actual admission error: {message}"
            );
            if expected != "unknown agent type" {
                assert!(
                    !message.contains("unknown agent type"),
                    "call inference validation must precede role layering: {message}"
                );
            }
            assert_eq!(
                gate.entered.available_permits(),
                0,
                "invalid settings cannot enter eviction durability"
            );
            assert_eq!(
                gate.unloaded.available_permits(),
                0,
                "invalid settings cannot unload a resident"
            );
            assert_eq!(fixture.provider.child_calls.load(Ordering::SeqCst), 0);
            assert_eq!(fixture.provider.grandchild_calls.load(Ordering::SeqCst), 0);
            assert_eq!(
                fixture.session.subagent_lifecycle_snapshot(),
                before,
                "preflight rejection cannot publish or change any existing identity"
            );
            assert_eq!(
                fixture
                    .provider
                    .residency_child_calls
                    .load(Ordering::SeqCst),
                seed_calls
            );
            assert_eq!(
                persisted(&goal.thread_id).await.tokens_used,
                1,
                "only the arming root request may settle"
            );
        }

        gate.release();
        if execution_full {
            fixture.provider.hold_residency.store(0, Ordering::SeqCst);
            fixture.provider.release_residency.add_permits(3);
            tokio::time::timeout(Duration::from_secs(5), async {
                while fixture
                    .session
                    .background_agent_summaries()
                    .iter()
                    .any(|agent| {
                        agent.status == klaus_agent_core::types::tool::BackgroundTaskStatus::Running
                    })
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("all held execution permits must settle");
        }
        fixture.await_descendant_release().await;
        let spawn = fixture.spawn.lock().unwrap().clone().unwrap();
        let next = spawn(SpawnSubAgentOptions {
            prompt: RESIDENCY_CHILD.into(),
            fork_context: Some(ForkContext::None),
            ..Default::default()
        })
        .await
        .expect("valid admission must still succeed after the refusal");
        let next_id = next.agent_id.try_to_utf8().unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if fixture
                    .session
                    .background_agent_summaries()
                    .iter()
                    .any(|agent| {
                        agent.id == next_id
                            && agent.status
                                == klaus_agent_core::types::tool::BackgroundTaskStatus::Completed
                    })
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("valid follow-up admission must complete");
        fixture.await_descendant_release().await;
        assert_eq!(
            fixture
                .provider
                .residency_child_calls
                .load(Ordering::SeqCst),
            seed_calls + 1
        );
        assert_eq!(persisted(&goal.thread_id).await.tokens_used, 2);
        drop(spawn);
        fixture.close().await;
    }
}

#[cfg(feature = "testing")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_public_fork_marker_is_not_an_authorized_role_definition() {
    assert_role_boundary_rejection(
        SpawnSubAgentOptions {
            prompt: CHILD.into(),
            fork_context: Some(ForkContext::None),
            agent_type: Some("fork".into()),
            ..Default::default()
        },
        "unknown agent type",
    )
    .await;
}

#[cfg(feature = "testing")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn call_model_and_effort_validation_precede_unknown_role_rejection() {
    let base = SpawnSubAgentOptions {
        prompt: CHILD.into(),
        fork_context: Some(ForkContext::None),
        agent_type: Some("unregistered-goal-capture-role".into()),
        ..Default::default()
    };
    assert_role_boundary_rejection(
        SpawnSubAgentOptions {
            model: Some("custom:missing-role-admission-model".into()),
            ..base.clone()
        },
        "Source-qualified sub-agent models require an endpoint source resolver.",
    )
    .await;
    assert_role_boundary_rejection(
        SpawnSubAgentOptions {
            effort: Some(klaus_agent_core::schemas::effort::EffortValue::Level(
                klaus_agent_core::schemas::effort::EffortLevel::High,
            )),
            ..base.clone()
        },
        "Reasoning effort `high` is not supported for model `descendant-fixture`.",
    )
    .await;
    assert_role_boundary_rejection(base, "unknown agent type").await;
}

#[cfg(feature = "testing")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejected_child_initialization_releases_its_captured_descendant_lease() {
    for path in PreparationPath::all() {
        let home = Home::new();
        let fixture = Fixture::new(&home).await;
        fixture.fill_residency().await;
        let goal = fixture
            .session
            .set_thread_goal(
                Some("Release a valid preparation refused by shutdown.".into()),
                None,
                None,
            )
            .await
            .unwrap()
            .goal;
        let mut gate = PreparationGate::install(&fixture);
        let pending = fixture.start_child(
            path,
            SpawnSubAgentOptions {
                prompt: CHILD.into(),
                fork_context: Some(ForkContext::None),
                ..Default::default()
            },
        );
        tokio::time::timeout(Duration::from_secs(5), gate.entered.acquire())
            .await
            .expect("real child creation must reach residency persistence")
            .unwrap()
            .forget();
        fixture.assert_preparing_child_blocks_idle(0).await;

        fixture.session.initiate_shutdown();
        assert!(
            fixture.session.is_disposed(),
            "shutdown must install its real admission fence synchronously"
        );
        gate.release();
        let outcome = tokio::time::timeout(Duration::from_secs(5), pending)
            .await
            .expect("shutdown must settle valid creation admission")
            .expect("creation driver must not panic");
        outcome.assert_shutdown_refused();
        fixture.await_descendant_release().await;
        assert_eq!(fixture.provider.root_calls.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.provider.child_calls.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.provider.grandchild_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            fixture.session.subagent_lifecycle_snapshot().agents.len(),
            3,
            "refused initialization must not publish a fourth child identity"
        );
        assert_eq!(persisted(&goal.thread_id).await.tokens_used, 0);
        fixture.close().await;
        assert_eq!(persisted(&goal.thread_id).await.tokens_used, 0);
    }
}
