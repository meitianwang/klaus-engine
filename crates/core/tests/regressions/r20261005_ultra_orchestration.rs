//! Ultra runtime regression: local Ultra selection used to be dropped at parse
//! time, workflow=false removed inline orchestration, and children ignored the
//! effective parent-turn effort. The request policy derives collaboration from
//! raw effort, while providers receive a supported inference effort.
//! Work item: 2026-10-05 Klaus Ultra orchestration.

use klaus_agent_core::engine::agent::{Agent, AgentOptions, StartSessionOptions};
use klaus_agent_core::engine::deps::EngineDepsOverrides;
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::effort::{EffortLevel, EffortValue};
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::tools::builtins::task::{create_task_tool, TaskToolOptions};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::ToolError;
use klaus_agent_core::types::json::Json;
use klaus_agent_core::types::permission::PermissionResult;
use klaus_agent_core::types::provider::{
    ModelCapabilities, ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::tool::{
    BackgroundTaskRegistry, PlanningToolCapability, Tool, ToolContext, ToolProgress, ToolResult,
};
use serde_json::json;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

struct CaptureCancellationTaskRegistry {
    task: Arc<dyn Tool>,
    captured: tokio::sync::mpsc::UnboundedSender<Arc<dyn BackgroundTaskRegistry>>,
}

#[async_trait::async_trait]
impl Tool for CaptureCancellationTaskRegistry {
    fn name(&self) -> RuntimeString {
        self.task.name()
    }
    fn description(&self) -> RuntimeString {
        self.task.description()
    }
    fn input_schema(&self) -> Json {
        self.task.input_schema()
    }
    fn persistence_identity(&self) -> Option<RuntimeString> {
        self.task.persistence_identity()
    }
    fn is_enabled(&self) -> bool {
        self.task.is_enabled()
    }
    fn is_concurrency_safe(&self, input: &Json) -> bool {
        self.task.is_concurrency_safe(input)
    }
    fn is_read_only(&self, input: &Json) -> bool {
        self.task.is_read_only(input)
    }
    fn planning_capability(&self, input: &Json) -> PlanningToolCapability {
        self.task.planning_capability(input)
    }
    fn parse_input(&self, input: &Json) -> Result<Json, ToolError> {
        self.task.parse_input(input)
    }
    async fn check_permissions(&self, input: &Json, ctx: &ToolContext) -> Option<PermissionResult> {
        self.task.check_permissions(input, ctx).await
    }
    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        assert!(self
            .captured
            .send(ctx.background_tasks.clone().expect("real task registry"))
            .is_ok());
        self.task.call(input, ctx, progress).await
    }
}

pub(super) struct RecordingProvider {
    info: ProviderInfo,
    pub(super) requests: Mutex<Vec<ModelRequest>>,
    spawn: Option<&'static str>,
    spawn_input: Option<serde_json::Value>,
    spawned: AtomicBool,
    nested_spawned: AtomicBool,
    child_signals: Mutex<Vec<AbortSignal>>,
    metadata: ModelCapabilities,
}

impl ModelProvider for RecordingProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: true,
            thinking: self.metadata.thinking.is_some() || self.metadata.effort.is_some(),
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: 200_000,
            max_output_tokens: 8192,
            native_pdf: true,
            vision: true,
            image_limits: None,
            max_media_blocks: None,
        }
    }

    fn model_metadata(&self, _: &RuntimeString) -> ModelCapabilities {
        self.metadata.clone()
    }

    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        let is_child = request.messages.iter().any(|message| match message {
            Message::User(user) => user.content.iter().any(|block| matches!(block, ContentBlock::Text(text) if text.text.contains("ultra-worker-directive"))),
            _ => false,
        });
        let has_tools = !request.tools.is_empty();
        let grandchild = request.messages.iter().any(|message| match message {
            Message::User(user) => user.content.iter().any(|block| matches!(block, ContentBlock::Text(text) if text.text.contains("full-history-grandchild"))),
            _ => false,
        });
        let spawn_nested = self
            .spawn
            .is_some_and(|scenario| scenario.starts_with("task-inherited-context"))
            && is_child
            && !grandchild
            && !self.nested_spawned.swap(true, Ordering::SeqCst);
        self.requests.lock().unwrap().push(request);
        if is_child && self.spawn == Some("task-cancel") {
            self.child_signals.lock().unwrap().push(signal.clone());
            return Box::pin(futures::stream::once(async move {
                signal.cancelled().await;
                ModelStreamEvent::MessageStop {
                    stop_reason: ModelStopReason::EndTurn,
                    usage: Usage {
                        input_tokens: 10,
                        output_tokens: 1,
                        cache_read_input_tokens: 0,
                        cache_creation_input_tokens: 0,
                        total_tokens: None,
                    },
                }
            }));
        }
        if let Some(scenario) = self.spawn.filter(|_| {
            has_tools && (spawn_nested || (!is_child && !self.spawned.swap(true, Ordering::SeqCst)))
        }) {
            let tool = if scenario.starts_with("task") {
                "task"
            } else if scenario.starts_with("skill-effort") {
                "skill"
            } else {
                "workflow"
            };
            let input = if spawn_nested {
                let mut input = json!({"model": "low", "description":"grandchild", "prompt":"full-history-grandchild", "run_in_background":false});
                if scenario.ends_with("replace") {
                    input["subagent_type"] = json!("replacement");
                }
                input
            } else if let Some(input) = self.spawn_input.clone() {
                input
            } else if scenario.starts_with("task") {
                json!({"model": "low", "description":"independent worker", "prompt":"ultra-worker-directive: inspect your assigned scope", "fork_turns":"none", "run_in_background": scenario == "task-background"})
            } else if scenario == "named-workflow" {
                json!({"name":"ultra-disk-regression", "budget_tokens":10000})
            } else {
                json!({"script":"export const meta = { name: 'ultra-regression', description: 'Validate inheritance' }; const first = await agent('ultra-worker-directive: inherited'); const second = await agent('ultra-worker-directive: native low', {effort:'low'}); const third = await agent('ultra-worker-directive: explicit mode off', {effort:'low',ultraMode:false}); return [first, second, third];", "budget_tokens": 10000})
            };
            return Box::pin(futures::stream::iter(vec![
                ModelStreamEvent::MessageStart {
                    model: "test-model".into(),
                },
                ModelStreamEvent::ToolUseStart {
                    id: "delegate".into(),
                    name: tool.into(),
                    thought_signature: None,
                    tool_kind: None,
                    input_format: None,
                },
                ModelStreamEvent::ToolUseInputDelta {
                    id: "delegate".into(),
                    partial_json: input.to_string().into(),
                },
                ModelStreamEvent::ToolUseStop {
                    id: "delegate".into(),
                },
                ModelStreamEvent::MessageStop {
                    stop_reason: ModelStopReason::ToolUse,
                    usage: Usage {
                        input_tokens: 10,
                        output_tokens: 1,
                        cache_read_input_tokens: 0,
                        cache_creation_input_tokens: 0,
                        total_tokens: None,
                    },
                },
            ]));
        }
        Box::pin(futures::stream::iter(vec![
            ModelStreamEvent::TextDelta {
                text: "Done.".into(),
            },
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: Usage {
                    input_tokens: 10,
                    output_tokens: 1,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    total_tokens: None,
                },
            },
        ]))
    }
}

fn agent_options(
    process_context: RuntimeProcessContext,
    provider: Arc<RecordingProvider>,
    cwd: String,
    session_effort: Option<&str>,
) -> AgentOptions {
    let mut config = json!({"model": "test-model", "workflow":false, "maxTurns":8, "thinking":false, "permissionMode":"bypassPermissions"});
    if let Some(effort) = session_effort {
        config["effort"] = json!(effort);
    }
    AgentOptions {
        process_context,
        provider,
        config: Some(config),
        tools: None,
        web_fetch_page: None,
        browser_runtime: None,
        capability_roots: Vec::new(),
        cwd: Some(cwd),
        deps: EngineDepsOverrides::default(),
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
    }
}

pub(super) struct Harness {
    process: RuntimeProcessContext,
    pub(super) provider: Arc<RecordingProvider>,
    pub(super) session: Arc<klaus_agent_core::engine::session::Session>,
    dir: std::path::PathBuf,
    task_registry: Option<tokio::sync::mpsc::UnboundedReceiver<Arc<dyn BackgroundTaskRegistry>>>,
}

pub(super) async fn harness(session_effort: Option<&str>, spawn: Option<&'static str>) -> Harness {
    harness_with_metadata(session_effort, spawn, ModelCapabilities::default()).await
}

pub(super) async fn harness_with_metadata(
    session_effort: Option<&str>,
    spawn: Option<&'static str>,
    metadata: ModelCapabilities,
) -> Harness {
    harness_with_child_options(session_effort, spawn, metadata, json!({}), None, None).await
}

pub(super) async fn harness_with_child_options(
    session_effort: Option<&str>,
    spawn: Option<&'static str>,
    metadata: ModelCapabilities,
    config_overrides: serde_json::Value,
    agents: Option<Vec<klaus_agent_core::engine::agent_definitions::AgentDefinition>>,
    spawn_input: Option<serde_json::Value>,
) -> Harness {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let dir = std::env::temp_dir().join(format!("klaus-ultra-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let workflow_dir = dir.join(".klaus/workflows");
    std::fs::create_dir_all(&workflow_dir).unwrap();
    std::fs::write(workflow_dir.join("fixture.js"), "export const meta = { name: 'ultra-disk-regression', description: 'Validate inherited and explicit effort' }; const first = await agent('ultra-worker-directive: inherited'); const second = await agent('ultra-worker-directive: native low', {effort:'low'}); const third = await agent('ultra-worker-directive: explicit mode off', {effort:'low',ultraMode:false}); return [first, second, third];").unwrap();
    let provider = Arc::new(RecordingProvider {
        info: ProviderInfo {
            name: "turn-effort-provider".into(),
            base_url: None,
            env_key: None,
            default_model: "test-model".into(),
        },
        requests: Mutex::new(Vec::new()),
        spawn,
        spawn_input,
        spawned: AtomicBool::new(false),
        nested_spawned: AtomicBool::new(false),
        child_signals: Mutex::new(Vec::new()),
        metadata,
    });
    let mut options = agent_options(
        process.clone(),
        provider.clone(),
        dir.to_string_lossy().into_owned(),
        session_effort,
    );
    options
        .config
        .as_mut()
        .unwrap()
        .as_object_mut()
        .unwrap()
        .extend(config_overrides.as_object().unwrap().clone());
    options.agents = agents;
    let task_registry = if spawn == Some("task-cancel") {
        let (captured, registry) = tokio::sync::mpsc::unbounded_channel();
        options.tools = Some(vec![Arc::new(CaptureCancellationTaskRegistry {
            task: create_task_tool(TaskToolOptions::default()),
            captured,
        })]);
        Some(registry)
    } else {
        None
    };
    if spawn.is_some_and(|scenario| scenario.starts_with("skill-effort")) {
        let effort =
            (spawn == Some("skill-effort-low")).then_some(EffortValue::Level(EffortLevel::Low));
        options.skills = Some(Arc::new(move || {
            vec![klaus_agent_core::extensions::skills::loader::Skill {
                name: "step-effort".into(),
                path: "/skills/step-effort/SKILL.md".into(),
                body: "SKILL_SESSION_SENTINEL".into(),
                effort: effort.clone(),
                ..Default::default()
            }]
        }));
    }
    let agent = Agent::new(options);
    let session = agent.start_session(StartSessionOptions {
        cwd: Some(dir.to_string_lossy().into_owned().into()),
        user_config_dir: Some(dir.to_string_lossy().into_owned().into()),
        ..Default::default()
    });
    Harness {
        process,
        provider,
        session,
        dir,
        task_registry,
    }
}

impl Harness {
    fn last_effort(&self) -> Option<EffortValue> {
        let requests = self.provider.requests.lock().unwrap();
        let request = requests
            .last()
            .expect("the turn must have produced a provider request");
        request.effort.clone()
    }

    pub(super) async fn teardown(self) {
        self.session.shutdown_and_wait().await.unwrap();
        self.process.shutdown().await.unwrap();
        std::fs::remove_dir_all(&self.dir).unwrap();
    }
}

fn ultra() -> EffortValue {
    EffortValue::Level(EffortLevel::Ultra)
}
fn system_contains(request: &ModelRequest, text: &str) -> bool {
    request.system.iter().any(|part| part.text.contains(text))
}

#[tokio::test]
async fn ultra_per_turn_mode_switch_has_no_stale_authorization_on_nonthinking_model() {
    let h = harness(Some("low"), None).await;
    for (effort, active) in [
        (Some(ultra()), true),
        (None, false),
        (Some(ultra()), true),
        (Some(EffortValue::Level(EffortLevel::Max)), false),
    ] {
        let result = h
            .session
            .run(
                Input::Text("short request".into()),
                TurnOptions {
                    effort,
                    ..Default::default()
                },
            )
            .await;
        assert!(result.error.is_none(), "{:?}", result.error);
        let requests = h.provider.requests.lock().unwrap();
        let request = requests.last().unwrap();
        assert_eq!(
            system_contains(request, "Proactive multi-agent delegation is active"),
            active
        );
        assert_eq!(
            system_contains(request, "Any earlier instruction enabling proactive multi-agent delegation no longer applies"),
            !active
        );
        assert_eq!(
            request.thinking, None,
            "orchestration survives a nonthinking provider"
        );
        assert!(
            request.tools.iter().any(|tool| tool.name == "workflow"),
            "legacy false cannot remove workflow capability"
        );
    }
    assert_eq!(h.last_effort(), Some(EffortValue::Level(EffortLevel::Max)));
    h.teardown().await;
}

#[tokio::test]
async fn session_ultra_is_overridden_for_one_turn_and_returns_on_the_next() {
    let h = harness(Some("ultra"), None).await;
    for (effort, ultra_mode, active) in [
        (None, None, true),
        (
            Some(EffortValue::Level(EffortLevel::Low)),
            Some(false),
            false,
        ),
        (Some(EffortValue::Level(EffortLevel::Low)), None, false),
        (None, None, true),
    ] {
        let result = h
            .session
            .run(
                Input::Text("short request".into()),
                TurnOptions {
                    effort,
                    ultra_mode,
                    ..Default::default()
                },
            )
            .await;
        assert!(result.error.is_none(), "{:?}", result.error);
        let requests = h.provider.requests.lock().unwrap();
        assert_eq!(
            system_contains(
                requests.last().unwrap(),
                "Proactive multi-agent delegation is active"
            ),
            active
        );
    }
    assert_eq!(
        h.last_effort(),
        Some(EffortValue::Level(EffortLevel::Medium))
    );
    h.teardown().await;
}

#[tokio::test]
async fn fresh_task_child_inherits_ultra_override_without_parent_transcript() {
    let h = harness(Some("low"), Some("task")).await;
    let result = h
        .session
        .run(
            Input::Text("root-only-context delegate".into()),
            TurnOptions {
                effort: Some(ultra()),
                ..Default::default()
            },
        )
        .await;
    assert!(result.error.is_none(), "{:?}", result.error);
    {
        let requests = h.provider.requests.lock().unwrap();
        let child = requests.iter().find(|request| request.messages.iter().any(|message| matches!(message, Message::User(user) if user.content.iter().any(|block| matches!(block, ContentBlock::Text(text) if text.text.contains("ultra-worker-directive")))))).unwrap_or_else(|| panic!("missing real task child request; history={:?}", h.session.history()));
        assert_eq!(child.effort, Some(EffortValue::Level(EffortLevel::Medium)));
        assert!(system_contains(
            child,
            "Proactive multi-agent delegation is active"
        ));
        assert!(!child.messages.iter().any(|message| matches!(message, Message::User(user) if user.content.iter().any(|block| matches!(block, ContentBlock::Text(text) if text.text.contains("root-only-context"))))));
    }
    let after = h
        .session
        .run(Input::Text("after".into()), TurnOptions::default())
        .await;
    assert!(after.error.is_none());
    assert_eq!(h.last_effort(), Some(EffortValue::Level(EffortLevel::Low)));
    h.teardown().await;
}

#[tokio::test]
async fn workflow_agents_inherit_ultra_and_honor_explicit_effort_with_old_false_config() {
    for source in ["workflow", "named-workflow"] {
        let h = harness_with_metadata(
            Some("low"),
            Some(source),
            ModelCapabilities {
                supported_reasoning_efforts: vec!["low".into(), "medium".into()],
                ..Default::default()
            },
        )
        .await;
        let result = h
            .session
            .run(
                Input::Text("root-only-context workflow".into()),
                TurnOptions {
                    effort: Some(ultra()),
                    ..Default::default()
                },
            )
            .await;
        assert!(result.error.is_none(), "{:?}", result.error);
        {
            let requests = h.provider.requests.lock().unwrap();
            let children: Vec<_> = requests.iter().filter(|request| request.messages.iter().any(|message| matches!(message, Message::User(user) if user.content.iter().any(|block| matches!(block, ContentBlock::Text(text) if text.text.contains("ultra-worker-directive")))))).collect();
            assert_eq!(
                children.len(),
                3,
                "the real JS workflow spawned all independent stages; history={:?}",
                h.session.history()
            );
            assert_eq!(
                children[0].effort,
                Some(EffortValue::Level(EffortLevel::Medium))
            );
            assert_eq!(
                children[1].effort,
                Some(EffortValue::Level(EffortLevel::Low))
            );
            assert!(system_contains(
                children[0],
                "Proactive multi-agent delegation is active"
            ));
            assert!(system_contains(
                children[1],
                "Any earlier instruction enabling proactive multi-agent delegation no longer applies"
            ));
            assert_eq!(
                children[2].effort,
                Some(EffortValue::Level(EffortLevel::Low))
            );
            assert!(system_contains(
                children[2],
                "Any earlier instruction enabling proactive multi-agent delegation no longer applies"
            ));
        }
        h.teardown().await;
    }
}

#[tokio::test]
async fn background_task_snapshots_parent_turn_ultra_before_detaching() {
    let h = harness(Some("low"), Some("task-background")).await;
    let result = h
        .session
        .run(
            Input::Text("delegate independently".into()),
            TurnOptions {
                effort: Some(ultra()),
                ..Default::default()
            },
        )
        .await;
    assert!(result.error.is_none(), "{:?}", result.error);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let found = h.provider.requests.lock().unwrap().iter().any(|request| request.messages.iter().any(|message| matches!(message, Message::User(user) if user.content.iter().any(|block| matches!(block, ContentBlock::Text(text) if text.text.contains("ultra-worker-directive"))))));
            if found { break; }
            tokio::task::yield_now().await;
        }
    }).await.expect("background child started");
    {
        let requests = h.provider.requests.lock().unwrap();
        let child = requests.iter().find(|request| request.messages.iter().any(|message| matches!(message, Message::User(user) if user.content.iter().any(|block| matches!(block, ContentBlock::Text(text) if text.text.contains("ultra-worker-directive")))))).unwrap();
        assert_eq!(child.effort, Some(EffortValue::Level(EffortLevel::Medium)));
        assert!(system_contains(
            child,
            "Proactive multi-agent delegation is active"
        ));
    }
    h.teardown().await;
}

#[test]
fn legacy_false_resolves_to_effective_workflow_true() {
    let config = klaus_agent_core::engine::session::resolve_config(
        json!({"workflow":false,"effort":"ultra"}),
    )
    .unwrap();
    assert!(config.workflow);
    assert_eq!(config.effort, Some(EffortValue::Level(EffortLevel::Ultra)));
    assert!(config.ultra_mode);
    let projected = serde_json::to_value(config).unwrap();
    assert_eq!(projected["workflow"], json!(true));
}

#[tokio::test]
async fn cancelling_an_ultra_turn_preserves_its_foreground_worker_until_targeted_stop() {
    let mut h = harness(Some("low"), Some("task-cancel")).await;
    let signal = AbortSignal::new();
    let session = h.session.clone();
    let parent_signal = signal.clone();
    let running = tokio::spawn(async move {
        session
            .run(
                Input::Text("delegate independently".into()),
                TurnOptions {
                    effort: Some(ultra()),
                    signal: Some(parent_signal),
                    ..Default::default()
                },
            )
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if !h.provider.child_signals.lock().unwrap().is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("foreground worker starts before cancellation");
    let registry = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        h.task_registry.as_mut().unwrap().recv(),
    )
    .await
    .expect("the real task must expose its registry")
    .expect("the task registry capture channel stays open");
    let child_signal = h.provider.child_signals.lock().unwrap()[0].clone();
    let children = h.session.subagent_lifecycle_snapshot();
    assert_eq!(children.agents.len(), 1, "one accepted foreground child");
    let child = &children.agents[0];
    assert_eq!(
        child.kind,
        klaus_agent_core::engine::subagent_lifecycle::SubagentKind::Foreground
    );
    let child_id = child.agent_id.clone();
    assert_eq!(
        registry.get(&child_id).unwrap().status,
        klaus_agent_core::types::tool::BackgroundTaskStatus::Running
    );
    signal.cancel();
    let parent = tokio::time::timeout(std::time::Duration::from_secs(5), running)
        .await
        .expect("cancelled parent unwinds")
        .unwrap();
    assert!(parent.error.is_none(), "{:?}", parent.error);
    assert_eq!(
        parent.stop_reason,
        klaus_agent_core::types::errors::TerminalReason::Aborted
    );
    assert!(
        !child_signal.is_cancelled(),
        "parent cancellation leaves the accepted child provider live"
    );
    assert_eq!(
        registry.get(&child_id).unwrap().status,
        klaus_agent_core::types::tool::BackgroundTaskStatus::Running
    );
    assert!(
        registry.stop(&child_id),
        "targeted stop must address the accepted foreground child"
    );
    tokio::time::timeout(std::time::Duration::from_secs(5), child_signal.cancelled())
        .await
        .expect("targeted stop cancels the actual child provider signal");
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if registry.get(&child_id).is_some_and(|child| {
                child.status == klaus_agent_core::types::tool::BackgroundTaskStatus::Interrupted
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the targeted child reaches its interrupted terminal state");
    assert!(
        !registry.stop(&child_id),
        "the child execution owner must settle before teardown"
    );
    drop(registry);
    drop(h.task_registry.take());
    h.teardown().await;
}

#[tokio::test]
async fn reconfiguring_session_ultra_to_no_effort_removes_mode_from_fresh_children() {
    let h = harness(Some("ultra"), Some("task")).await;
    let selected = klaus_agent_core::engine::session::resolve_config(
        json!({"model":"test-model","thinking":false,"permissionMode":"bypassPermissions"}),
    )
    .unwrap();
    h.session
        .reconfigure_inference(
            h.provider.clone(),
            selected,
            None,
            None,
            h.session.model_revision(),
        )
        .unwrap();
    let result = h
        .session
        .run(
            Input::Text("delegate independently".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(result.error.is_none(), "{:?}", result.error);
    {
        let requests = h.provider.requests.lock().unwrap();
        let child = requests.iter().find(|request| request.messages.iter().any(|message| matches!(message, Message::User(user) if user.content.iter().any(|block| matches!(block, ContentBlock::Text(text) if text.text.contains("ultra-worker-directive")))))).expect("real fresh child after reconfiguration");
        assert_eq!(child.effort, None);
        assert!(system_contains(
            child,
            "Any earlier instruction enabling proactive multi-agent delegation no longer applies"
        ));
    }
    h.teardown().await;
}
