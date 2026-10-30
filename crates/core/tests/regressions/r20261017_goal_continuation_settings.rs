//! Defect: automatic goal admission could discard request-scoped settings.
//! Root cause: subsequent turns reconstructed options without preserving their ownership.
//! Work item 20261017: continuation settings, real child ownership and sustained progress.

use async_trait::async_trait;
use futures::StreamExt;
use klaus_agent_core::browser::BrowserHostToolMapping;
use klaus_agent_core::capabilities::{RuntimeCapabilityRoot, SelectedRuntimeCapabilityRoot};
use klaus_agent_core::engine::agent::{Agent, AgentOptions, StartSessionOptions};
use klaus_agent_core::engine::deps::NoopCompactor;
use klaus_agent_core::engine::session::{Input, Session, TurnOptions};
use klaus_agent_core::goal::types::ThreadGoalStatus;
use klaus_agent_core::runtime_process::{
    remove_process_environment_var, set_process_environment_var, RuntimeProcessContext,
};
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeString};
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::effort::{EffortLevel, EffortValue};
use klaus_agent_core::schemas::message::{Message, SystemSubtype, Usage};
use klaus_agent_core::schemas::rollout::RolloutItem;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::tools::build_tool::text_result;
use klaus_agent_core::tools::builtins::task::{create_task_tool, TaskToolOptions};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::ToolError;
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::json::{Json, JsonSchema};
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};
use serde_json::json;
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

const WAIT: Duration = Duration::from_secs(5);
const CHILD: &str = "SETTINGS_CHILD_DIRECTIVE";
type Slot = Arc<Mutex<Weak<Session>>>;
type Handler = Arc<dyn Fn(ModelRequest, AbortSignal) -> ModelStream + Send + Sync>;

struct Home {
    path: std::path::PathBuf,
    previous: Option<String>,
}
impl Home {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("klaus-goal-settings-{}", uuid::Uuid::new_v4()));
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
            eprintln!("Retained goal settings fixture: {}", self.path.display());
        } else {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}
struct Provider {
    info: ProviderInfo,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
    handler: Handler,
}
impl ModelProvider for Provider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }
    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: true,
            thinking: false,
            structured_output: true,
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
        {
            let mut captured = self.requests.lock().unwrap();
            assert!(captured.len() < 96, "bounded continuation request fixture");
            captured.push(request.clone());
        }
        (self.handler)(request, signal)
    }
}
type PhaseMeasurements = Arc<Mutex<Vec<(&'static str, u128, u128)>>>;

struct Fixture {
    session: Arc<Session>,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
    provider: Arc<dyn ModelProvider>,
    store: Arc<JsonlSessionStore>,
    process: RuntimeProcessContext,
    phase_origin: std::time::Instant,
    phases: PhaseMeasurements,
    _agent: Agent,
    _home: Home,
}
impl Fixture {
    async fn new(
        slot: &Slot,
        tools: Vec<Arc<dyn Tool>>,
        setup: impl FnOnce(&mut AgentOptions),
        handler: impl Fn(ModelRequest, AbortSignal) -> ModelStream + Send + Sync + 'static,
    ) -> Self {
        let home = Home::new();
        let process = RuntimeProcessContext::initialize(
            super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
        )
        .unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let mut options =
            super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
        let provider: Arc<dyn ModelProvider> = Arc::new(Provider {
            info: ProviderInfo {
                name: "goal-settings-default".into(),
                default_model: "goal-settings-default".into(),
                base_url: None,
                env_key: None,
            },
            requests: requests.clone(),
            handler: Arc::new(handler),
        });
        options.provider = provider.clone();
        options.tools = Some(tools);
        options.compactor = Some(Arc::new(NoopCompactor));
        options.cwd = Some(home.path.to_string_lossy().into_owned());
        setup(&mut options);
        let agent = Agent::new(options);
        let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(home.path.join("sessions").to_string_lossy().into_owned()),
            archive_dir: None,
        }));
        let session=agent.open_session(store.clone(),StartSessionOptions{cwd:Some(home.path.to_string_lossy().to_string().into()),user_config_dir:Some(home.path.to_string_lossy().to_string().into()),config:Some(json!({"model":"goal-settings-default","sandbox_mode":"danger-full-access","approval_policy":"never","goal":true,"prewarm":false,"autoMemory":false,"agentMemory":false,"idleMicrocompact":false,"earlyMicrocompact":false,"speculativeCompaction":false,"loopDetection":false,"maxRetries":0,"taskBudgetTokens":37})),..Default::default()}).await.unwrap();
        let phase_origin = std::time::Instant::now();
        let phases = Arc::new(Mutex::new(Vec::new()));
        #[cfg(feature = "testing")]
        {
            let phases = phases.clone();
            let diagnostic_thread = session.id.clone();
            session
                .goal_runtime()
                .expect("persistent fixture has goal runtime")
                .testing_set_phase_observer(Some(Arc::new(move |phase| {
                    let entered_ms = phase_origin.elapsed().as_millis();
                    klaus_agent_core::goal::testing_phase::mark(
                        phase,
                        &format!("thread={diagnostic_thread:?} observer_entered_ms={entered_ms}"),
                    );
                    let mut trace = phases.lock().unwrap();
                    if trace.len() == 256 {
                        trace.remove(0);
                    }
                    trace.push((phase, entered_ms, phase_origin.elapsed().as_millis()));
                })));
        }
        *slot.lock().unwrap() = Arc::downgrade(&session);
        Self {
            session,
            requests,
            provider,
            store,
            process,
            phase_origin,
            phases,
            _agent: agent,
            _home: home,
        }
    }
    async fn finish(self) {
        self.session.dispose().await;
        self.process.shutdown().await.unwrap();
    }
}
fn session(slot: &Slot) -> Arc<Session> {
    slot.lock()
        .unwrap()
        .upgrade()
        .expect("live fixture session")
}
fn usage(output: u64) -> Usage {
    Usage {
        input_tokens: 3,
        output_tokens: output,
        cache_read_input_tokens: 19,
        cache_creation_input_tokens: 23,
        total_tokens: Some(999),
    }
}
fn stop(reason: ModelStopReason, output: u64) -> ModelStreamEvent {
    ModelStreamEvent::MessageStop {
        stop_reason: reason,
        usage: usage(output),
    }
}
fn answer(text: &str, output: u64) -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::MessageStart {
            model: "goal-turn-override".into(),
        },
        ModelStreamEvent::TextDelta { text: text.into() },
        stop(ModelStopReason::EndTurn, output),
    ]
}
fn call(id: &str, name: &str, input: serde_json::Value) -> Vec<ModelStreamEvent> {
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
            partial_json: input.to_string().into(),
        },
        ModelStreamEvent::ToolUseStop { id: id.into() },
    ]
}
fn observe_events(id: &str) -> Vec<ModelStreamEvent> {
    let mut events = vec![ModelStreamEvent::MessageStart {
        model: "goal-turn-override".into(),
    }];
    events.extend(call(id, "fixture_observe", json!({})));
    events.push(stop(ModelStopReason::ToolUse, 1));
    events
}
fn directive(request: &ModelRequest, needle: &str) -> bool {
    request.messages.iter().any(|message|matches!(message,Message::User(user) if user.is_meta!=Some(true)&&user.content.iter().any(|block|matches!(block,ContentBlock::Text(text) if text.text==needle))))
}
#[derive(Debug)]
struct Observation {
    sandbox: Option<klaus_agent_core::types::permission::SandboxMode>,
    browser_tabs: String,
    depth: Option<u32>,
}
struct ObserveTool {
    schema: JsonSchema,
    observations: Arc<Mutex<Vec<Observation>>>,
}
#[async_trait]
impl Tool for ObserveTool {
    fn static_name(&self) -> &str {
        "fixture_observe"
    }
    fn static_description(&self) -> &str {
        "Record the actual request-scoped tool capabilities."
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_read_only(&self, _: &Json) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _: &Json) -> bool {
        false
    }
    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let mut observations = self.observations.lock().unwrap();
        assert!(observations.len() < 4);
        observations.push(Observation {
            sandbox: ctx.sandbox_mode,
            browser_tabs: ctx
                .browser_tool_mapping
                .as_ref()
                .expect("selected browser capability reaches actual tool context")
                .tabs
                .clone(),
            depth: ctx.depth,
        });
        Ok(text_result(
            input,
            "Captured the scoped tool context.",
            false,
        ))
    }
}
async fn collect(
    fixture: &Fixture,
    turn: klaus_agent_core::engine::session::Turn,
) -> Vec<EngineEvent> {
    let mut stream = turn.events;
    let mut events = Vec::new();
    let finished = tokio::time::timeout(WAIT, async {
        while let Some(event) = stream.next().await {
            assert!(events.len() < 256, "bounded settings event fixture");
            events.push(event);
        }
    })
    .await;
    let index = fixture.session.turn_index_snapshot();
    assert!(finished.is_ok(), "settings turn stalled: elapsed_ms={}, phases={:?}, provider_requests={}, started_turns={}, goal_metadata={}, lifecycle={:?}, events={events:?}",
        fixture.phase_origin.elapsed().as_millis(),
        fixture.phases.lock().unwrap(),
        fixture.requests.lock().unwrap().len(),
        index.turn_started_at_ms.len(),
        index.turn_goal_metadata.len(),
        fixture.session.subagent_lifecycle_snapshot());
    events
}
fn assert_success(events: &[EngineEvent]) {
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{events:?}"
    );
}

#[tokio::test]
async fn actual_automatic_turn_preserves_scoped_settings_and_real_task_child_ownership() {
    let slot: Slot = Arc::new(Mutex::new(Weak::new()));
    let observations = Arc::new(Mutex::new(Vec::new()));
    let root_calls = Arc::new(AtomicUsize::new(0));
    let child_calls = Arc::new(AtomicUsize::new(0));
    let fixture=Fixture::new(&slot,vec![Arc::new(ObserveTool{schema:json!({"type":"object","properties":{}}).as_object().unwrap().clone(),observations:observations.clone()}),create_task_tool(TaskToolOptions::default())],|options|{
        options.config.as_mut().unwrap()["permissionMode"]=json!("default");
        options.capability_roots=vec![RuntimeCapabilityRoot::host("desired").with_browser_tool_mapping(BrowserHostToolMapping{tabs:"desired-tabs-owner".to_string(),..Default::default()}),RuntimeCapabilityRoot::host("fallback").with_browser_tool_mapping(BrowserHostToolMapping{tabs:"fallback-tabs-owner".to_string(),..Default::default()})];
    },{let slot=slot.clone();let root_calls=root_calls.clone();let child_calls=child_calls.clone();move|request,_|{
        if directive(&request,CHILD){return match child_calls.fetch_add(1,Ordering::SeqCst){0=>Box::pin(futures::stream::iter(observe_events("child-observation"))),1=>Box::pin(futures::stream::iter(answer("The continuation child completed.",1))),call=>panic!("unexpected child request {call}")};}
        match root_calls.fetch_add(1,Ordering::SeqCst){
            0=>Box::pin(futures::stream::iter(observe_events("initial-observation"))),
            1=>Box::pin(futures::stream::iter(answer(r#"{"ready":true}"#,1))),
            2=>{let mut events=observe_events("continuation-observation");events.pop();events.extend(call("continuation-child","task",json!({"model": "low", "description":"Check scoped context","prompt":CHILD,"fork_turns":"none","run_in_background":false})));events.push(stop(ModelStopReason::ToolUse,1));Box::pin(futures::stream::iter(events))},
            3=>Box::pin(futures::stream::iter(answer("The continuation has made measured progress.",1))),
            4=>{assert!(request.messages.iter().any(|message|matches!(message,Message::User(user) if user.content.iter().any(|block|matches!(block,ContentBlock::Text(text) if text.text.contains("of the token target for this task"))))),"the original output-token policy must cause this real follow-up request");let root=session(&slot);Box::pin(async_stream::stream!{root.set_thread_goal(None,Some("paused".into()),None).await.unwrap();for event in answer("The automatic goal checkpoint is recorded.",32){yield event;}})},
            call=>panic!("unexpected root request {call}"),
        }
    }}).await;
    assert_eq!(
        fixture.session.collaboration_mode(),
        klaus_agent_core::planning::CollaborationMode::Default
    );
    let created = fixture
        .session
        .set_thread_goal(
            Some("Continue using the selected execution settings.".into()),
            None,
            None,
        )
        .await
        .unwrap();
    let metadata = runtime_json::parse(&RuntimeString::from(
        r#"{"label":"render-only-sentinel","kind":"fixture"}"#,
    ))
    .unwrap();
    let options=TurnOptions{model:Some("goal-turn-override".into()),effort:Some(EffortValue::Level(EffortLevel::High)),selected_capability_roots:Some(vec![SelectedRuntimeCapabilityRoot::host("desired")]),output_schema:Some(json!({"type":"object","properties":{"ready":{"type":"boolean"}},"required":["ready"],"additionalProperties":false}).as_object().unwrap().clone()),display_metadata:Some(metadata.clone()),system_prompt_append:Some("Keep the request-scoped instruction sentinel.".into()),token_budget:Some(24),..Default::default()};
    let mut events = collect(
        &fixture,
        fixture.session.run_streamed(
            Input::Text("Start with a typed response.".into()),
            options.clone(),
        ),
    )
    .await;
    assert_success(&events);
    assert_eq!(
        root_calls.load(Ordering::SeqCst),
        2,
        "initial output schema suppresses the soft token minimum"
    );
    let automatic = fixture
        .session
        .run_goal_idle_streamed(options)
        .await
        .unwrap()
        .expect("active goal admits one actual automatic turn");
    events.extend(collect(&fixture, automatic).await);
    assert_success(&events);
    assert_eq!(root_calls.load(Ordering::SeqCst), 5);
    assert_eq!(child_calls.load(Ordering::SeqCst), 2);
    {
        let requests = fixture.requests.lock().unwrap();
        assert_eq!(requests.len(), 7);
        let roots = requests
            .iter()
            .filter(|request| !directive(request, CHILD))
            .collect::<Vec<_>>();
        assert_eq!(roots.len(), 5);
        for (index, request) in roots.iter().enumerate() {
            assert_eq!(request.model, "goal-turn-override");
            assert_eq!(request.effort, Some(EffortValue::Level(EffortLevel::High)));
            assert_eq!(request.task_budget, Some(37));
            assert!(request
                .system
                .iter()
                .any(|part| part.text.contains("request-scoped instruction sentinel")));
            assert_eq!(request.output_schema.is_some(), index < 2);
            let encoded = runtime_json::stringify(&runtime_codec::encode_value(*request).unwrap());
            assert!(
                !encoded.contains("render-only-sentinel"),
                "display-only data cannot enter any model request lane"
            );
        }
        for request in requests.iter().filter(|request| directive(request, CHILD)) {
            assert_eq!(
                request.model, "goal-turn-override",
                "real task child inherits the admitted parent model"
            );
            assert_eq!(
                request.effort,
                Some(EffortValue::Level(EffortLevel::High)),
                "real task child inherits the admitted parent effort"
            );
        }
    }
    {
        let observed = observations.lock().unwrap();
        assert_eq!(observed.len(), 3);
        assert_eq!(
            observed
                .iter()
                .filter(|observation| observation.depth == Some(0))
                .count(),
            2
        );
        assert_eq!(
            observed
                .iter()
                .filter(|observation| observation.depth == Some(1))
                .count(),
            1
        );
        for observation in observed.iter() {
            assert_eq!(
                observation.sandbox,
                Some(klaus_agent_core::types::permission::SandboxMode::DangerFullAccess)
            );
            assert_eq!(observation.browser_tabs,"desired-tabs-owner","the captured capability owner reaches both continuation and actual child tool contexts");
        }
    }
    let turns = events
        .iter()
        .filter_map(|event| match event {
            EngineEvent::TurnStarted { turn_id } => Some(turn_id.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(turns.len(), 2);
    assert_ne!(turns[0], turns[1]);
    let children = fixture.session.subagent_lifecycle_snapshot().agents;
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].context.spawn_turn_id.as_ref(), Some(&turns[1]));
    assert_eq!(children[0].context.session_id, fixture.session.id);
    assert_eq!(children[0].context.parent_agent_id, None);
    let index = fixture.session.turn_index_snapshot();
    assert_eq!(
        index.turn_goal_metadata[&turns[1]].goal_id.as_ref(),
        Some(&created.goal.goal_id)
    );
    assert!(index.turn_goal_metadata[&turns[1]].goal_continuation);
    assert!(!index.turn_goal_metadata[&turns[0]].goal_continuation);
    fixture.session.flush_rollout().await.unwrap();
    let saved = fixture
        .store
        .read_thread(&fixture.session.id.try_to_utf8().unwrap())
        .await
        .unwrap();
    let contexts = saved
        .raw_items
        .iter()
        .filter_map(|item| match item {
            RolloutItem::TurnContext { ctx } => Some(ctx),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(contexts.len(), 2);
    for (i, ctx) in contexts.iter().enumerate() {
        assert_eq!(ctx.turn_id.as_ref(), Some(&turns[i]));
        assert_eq!(ctx.model, "goal-turn-override");
        assert_eq!(
            ctx.sandbox_mode,
            Some(klaus_agent_core::types::permission::SandboxMode::DangerFullAccess)
        );
    }
    let displays = saved
        .raw_items
        .iter()
        .filter_map(|item| match item {
            RolloutItem::Message {
                message: Message::System(system),
            } if system.subtype == SystemSubtype::UserDisplay => {
                Some(runtime_json::parse(&system.text).unwrap())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        displays.len(),
        1,
        "automatic turn cannot duplicate human display metadata"
    );
    assert_eq!(displays[0].get("localRender"), Some(&metadata));
    assert_eq!(
        fixture.session.thread_goal().await.unwrap().unwrap().status,
        ThreadGoalStatus::Paused
    );
    fixture.finish().await;
}

#[tokio::test]
async fn progressing_automatic_work_survives_twenty_four_separate_idle_admissions() {
    const AUTOMATIC_TURNS: usize = 24;
    let slot: Slot = Arc::new(Mutex::new(Weak::new()));
    let calls = Arc::new(AtomicUsize::new(0));
    let fixture = Fixture::new(&slot, Vec::new(), |_| {}, {
        let calls = calls.clone();
        move |_, _| {
            let call = calls.fetch_add(1, Ordering::SeqCst);
            assert!(call <= AUTOMATIC_TURNS, "bounded progressing goal fixture");
            Box::pin(futures::stream::iter(answer(
                &format!("Measured progress checkpoint {call}."),
                1,
            )))
        }
    })
    .await;
    let goal = fixture
        .session
        .set_thread_goal(
            Some("Continue while measurable work remains.".into()),
            None,
            None,
        )
        .await
        .unwrap()
        .goal;
    let first = collect(
        &fixture,
        fixture.session.run_streamed(
            Input::Text("Begin measured work.".into()),
            TurnOptions::default(),
        ),
    )
    .await;
    assert_success(&first);
    let mut ids = HashSet::new();
    for event in first {
        if let EngineEvent::TurnStarted { turn_id } = event {
            assert!(ids.insert(turn_id));
        }
    }
    for admitted in 1..=AUTOMATIC_TURNS {
        let turn = fixture
            .session
            .run_goal_idle_streamed(TurnOptions::default())
            .await
            .unwrap()
            .unwrap_or_else(|| {
                panic!("progressing work was stopped by an arbitrary turn cap at{admitted}")
            });
        let events = collect(&fixture, turn).await;
        assert_success(&events);
        let started = events
            .iter()
            .filter_map(|event| match event {
                EngineEvent::TurnStarted { turn_id } => Some(turn_id.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(started.len(), 1);
        assert!(
            ids.insert(started[0].clone()),
            "every continuation owns a fresh turn identity"
        );
        let saved = fixture.session.thread_goal().await.unwrap().unwrap();
        assert_eq!(saved.goal_id, goal.goal_id);
        assert_eq!(saved.status, ThreadGoalStatus::Active);
        assert_eq!(
            saved.tokens_used,
            4 * i64::try_from(admitted + 1).unwrap(),
            "actual request usage continues to settle once during sustained progress"
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), AUTOMATIC_TURNS + 1);
    assert_eq!(ids.len(), AUTOMATIC_TURNS + 1);
    fixture
        .session
        .set_thread_goal(None, Some("paused".into()), None)
        .await
        .unwrap();
    assert!(fixture
        .session
        .run_goal_idle_streamed(TurnOptions::default())
        .await
        .unwrap()
        .is_none());
    assert_eq!(calls.load(Ordering::SeqCst), AUTOMATIC_TURNS + 1);
    fixture.finish().await;
}

#[cfg(feature = "testing")]
#[path = "r20261017_permission_preparation_window.rs"]
mod permission_preparation_window;

#[path = "r20261017_child_capability_capture.rs"]
mod child_capability_capture;
