//! Defect: explicit collaboration Plan did not block dynamic tool and runtime mutations.
//! Root cause: ordinary permission decisions had no independent admitted Plan effect ceiling.
//! Work item: 20261018 captured planning effects across dispatch, delegation and shell execution.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::agent_definitions::AgentDefinition;
use klaus_agent_core::engine::session::{Input, Session, TurnOptions};
use klaus_agent_core::hooks::types::{
    Hook, HookEvent, HookFn, HookHandler, HookInput, HookOutcome,
};
use klaus_agent_core::planning::{instructions::instruction_part, CollaborationMode};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimeString};
use klaus_agent_core::schemas::config::default_config;
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::Message;
use klaus_agent_core::schemas::rollout::RolloutItem;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::testing::ev;
use klaus_agent_core::tools::build_tool::text_result;
use klaus_agent_core::tools::builtins::bash::create_bash_tool;
use klaus_agent_core::tools::builtins::task::{create_task_tool, TaskToolOptions};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::ToolError;
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::json::{Json, JsonSchema};
use klaus_agent_core::types::permission::{ApprovalHandler, ApprovalRequest, ApprovalResponse};
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::tool::{Isolation, Tool, ToolContext, ToolProgress, ToolResult};
use serde_json::{json, Value};

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

const WAIT: Duration = Duration::from_secs(10);
const READ: &str = "effect_probe_read";
const MUTATE: &str = "effect_probe_mutate";
const NESTED_BASH: &str = "effect_probe_nested_bash";
const CHILD_MARKER: &str = "PLAN_EFFECT_RESEARCH_CHILD";

enum Reply {
    Tool(&'static str, &'static str, Value),
    Text,
}

type OnFirstRoot = Arc<dyn Fn() + Send + Sync>;

struct Provider {
    info: ProviderInfo,
    root_replies: Mutex<VecDeque<Reply>>,
    child_replies: Mutex<VecDeque<Reply>>,
    requests: Mutex<Vec<(bool, ModelRequest)>>,
    on_first_root: Mutex<Option<OnFirstRoot>>,
    worktree_calls: Arc<AtomicUsize>,
}

impl Provider {
    fn new(root: Vec<Reply>, child: Vec<Reply>) -> Arc<Self> {
        Arc::new(Self {
            info: ProviderInfo {
                name: "plan-effect-regression".into(),
                base_url: None,
                env_key: None,
                default_model: "plan-effect-regression".into(),
            },
            root_replies: Mutex::new(root.into()),
            child_replies: Mutex::new(child.into()),
            requests: Mutex::new(Vec::new()),
            on_first_root: Mutex::new(None),
            worktree_calls: Arc::new(AtomicUsize::new(0)),
        })
    }

    fn result_is_error(&self, id: &str) -> bool {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .rev()
            .flat_map(|(_, request)| request.messages.iter())
            .find_map(|message| match message {
                Message::ToolResult(result) if result.tool_use_id == id => Some(result.is_error),
                _ => None,
            })
            .expect("the next actual request contains the correlated tool result")
    }

    fn assert_mode(&self, child: bool, mode: CollaborationMode) {
        let expected = instruction_part(&default_config(), mode);
        for (_, request) in self
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(c, _)| *c == child)
        {
            let parts: Vec<_> = request
                .system
                .iter()
                .filter(|part| part.name == "collaboration-mode")
                .collect();
            assert_eq!(parts, vec![&expected]);
        }
    }
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

    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        let child = request.messages.iter().any(|message| match message {
            Message::User(user) => user.content.iter().any(
                |block| matches!(block, ContentBlock::Text(text) if text.text.contains(CHILD_MARKER)),
            ),
            _ => false,
        });
        {
            let mut requests = self.requests.lock().unwrap();
            assert!(requests.len() < 8, "only bounded scripted requests may run");
            requests.push((child, request));
        }
        if !child {
            let callback = self.on_first_root.lock().unwrap().take();
            if let Some(callback) = callback {
                callback();
            }
        }
        let reply = if child {
            self.child_replies.lock().unwrap().pop_front()
        } else {
            self.root_replies.lock().unwrap().pop_front()
        }
        .expect("each inference must have a scripted response");
        let events = match reply {
            Reply::Tool(id, name, input) => vec![
                ev::message_start(None),
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
                ev::stop(Some(ModelStopReason::ToolUse), None),
            ],
            Reply::Text => vec![
                ev::message_start(None),
                ev::text("The bounded research turn completed."),
                ev::stop(Some(ModelStopReason::EndTurn), None),
            ],
        };
        Box::pin(futures::stream::iter(events))
    }
}

enum Effect {
    Read(PathBuf),
    Mutate(PathBuf),
    NestedBash(String),
}

struct Probe {
    name: &'static str,
    schema: JsonSchema,
    effect: Effect,
    calls: AtomicUsize,
    ordinary_policies: Mutex<Vec<Option<klaus_agent_core::types::permission::SandboxMode>>>,
}

impl Probe {
    fn new(name: &'static str, effect: Effect) -> Arc<Self> {
        Arc::new(Self {
            name,
            schema: json!({"type":"object","properties":{},"additionalProperties":false})
                .as_object()
                .unwrap()
                .clone(),
            effect,
            calls: AtomicUsize::new(0),
            ordinary_policies: Mutex::new(Vec::new()),
        })
    }
}

#[async_trait]
impl Tool for Probe {
    fn static_name(&self) -> &str {
        self.name
    }
    fn static_description(&self) -> &str {
        "Exercise an owned planning effect boundary"
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }
    fn persistence_identity(&self) -> Option<RuntimeString> {
        Some(format!("plan-effect-probe-v1:{}", self.name).into())
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _: &Json) -> bool {
        false
    }
    fn is_read_only(&self, _: &Json) -> bool {
        !matches!(self.effect, Effect::Mutate(_))
    }
    async fn call(
        &self,
        _: Json,
        ctx: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        assert!(self.calls.fetch_add(1, Ordering::SeqCst) < 2);
        self.ordinary_policies
            .lock()
            .unwrap()
            .push(ctx.sandbox_mode);
        let text = match &self.effect {
            Effect::Read(path) => std::fs::read_to_string(path).unwrap(),
            Effect::Mutate(path) => {
                std::fs::write(path, "changed").unwrap();
                "changed".to_string()
            }
            Effect::NestedBash(command) => {
                let run = ctx
                    .run_bash
                    .as_ref()
                    .expect("a real Session supplies run_bash");
                run(command.clone(), None).await?
            }
        };
        Ok(text_result(
            RuntimeJson::String(text.clone().into()),
            text,
            false,
        ))
    }
}

struct Approvals {
    calls: AtomicUsize,
    rewrite: Option<(RuntimeJson, RuntimeJson)>,
}

#[async_trait]
impl ApprovalHandler for Approvals {
    async fn approve(&self, request: ApprovalRequest, signal: AbortSignal) -> ApprovalResponse {
        let previous = self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some((original, rewritten)) = &self.rewrite {
            assert_eq!(previous, 0);
            assert!(!signal.is_cancelled());
            assert_eq!(request.tool_name, "bash");
            assert_eq!(&request.input, original);
            ApprovalResponse::Allow {
                updated_input: Some(rewritten.clone()),
                updated_permissions: None,
            }
        } else {
            ApprovalResponse::Deny {
                message: Some("No approval is authorized by this fixture.".into()),
            }
        }
    }
}

struct WorktreeProbe {
    calls: Arc<AtomicUsize>,
    marker: PathBuf,
}

#[async_trait]
impl HookFn for WorktreeProbe {
    async fn run(&self, _: HookInput) -> HookOutcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        std::fs::create_dir_all(&self.marker).unwrap();
        HookOutcome::default()
    }
}

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("klaus-plan-effects-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Rig {
    process: RuntimeProcessContext,
    session: Arc<Session>,
    store: Arc<JsonlSessionStore>,
    thread: String,
    sandbox: klaus_agent_core::types::permission::SandboxMode,
}

impl Rig {
    async fn new(
        root: &Path,
        provider: Arc<Provider>,
        tools: Vec<Arc<dyn Tool>>,
        sandbox: klaus_agent_core::types::permission::SandboxMode,
        rules: (Vec<&str>, Vec<&str>),
        approvals: Arc<Approvals>,
        agents: Vec<AgentDefinition>,
    ) -> Self {
        let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
        let mut options = agent_options(process.clone());
        options.provider = provider.clone();
        options.hooks = vec![Hook {
            event: HookEvent::WorktreeCreate,
            matcher: None,
            handler: HookHandler::Fn(Arc::new(WorktreeProbe {
                calls: provider.worktree_calls.clone(),
                marker: root.join("worktree-create-hook"),
            })),
            is_async: false,
        }];
        options.tools = Some(tools);
        options.cwd = Some(root.to_string_lossy().into_owned());
        options.approval_handler = Some(approvals);
        options.agents = Some(agents);
        options.include_default_agents = Some(false);
        options.config = Some(json!({
            "sandbox_mode":sandbox, "approval_policy":"on-request",
            "sandboxNetworkAllowed":true, "requireRealSandbox":false, "sandboxRetryOnDenial":false,
            "prewarm":false, "goal":false, "autoMemory":false, "autoDream":false,
            "agentMemory":false, "memoryRecall":false, "memory":{"enabled":false},
            "loopDetection":false, "allow":rules.0, "ask":rules.1
        }));
        let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(root.join("sessions").to_string_lossy().into_owned()),
            ..Default::default()
        }));
        let thread = format!("plan-effect-{}", uuid::Uuid::new_v4());
        let agent = Agent::new(options);
        let session = agent
            .open_session(
                store.clone(),
                StartSessionOptions {
                    id: Some(thread.clone().into()),
                    cwd: Some(root.to_string_lossy().into_owned().into()),
                    user_config_dir: Some(
                        root.join("config").to_string_lossy().into_owned().into(),
                    ),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        session
            .planning_runtime()
            .set_mode(CollaborationMode::Plan, None)
            .unwrap();
        Self {
            process,
            session,
            store,
            thread,
            sandbox,
        }
    }

    async fn run(&self) {
        let events = tokio::time::timeout(
            WAIT,
            self.session
                .run_streamed(
                    Input::Text("Run the bounded planning effect probe.".into()),
                    TurnOptions::default(),
                )
                .events
                .collect::<Vec<_>>(),
        )
        .await
        .expect("the scripted turn finishes within the existing regression deadline");
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
            "{events:?}"
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event, EngineEvent::TurnCompleted { .. })),
            "{events:?}"
        );
    }

    async fn finish(&self) {
        self.session.shutdown_and_wait().await.unwrap();
        let saved = self.store.read_thread(&self.thread).await.unwrap();
        let saved_modes: Vec<_> = saved
            .raw_items
            .iter()
            .filter_map(|item| match item {
                RolloutItem::TurnContext { ctx } => ctx.sandbox_mode,
                _ => None,
            })
            .collect();
        assert!(
            !saved_modes.is_empty(),
            "the actual admitted turns were persisted"
        );
        assert!(
            saved_modes.iter().all(|mode| *mode == self.sandbox),
            "Plan is not an ordinary permission preset: {saved_modes:?}"
        );
        self.process.shutdown().await.unwrap();
    }
}

fn no_approvals() -> Arc<Approvals> {
    Arc::new(Approvals {
        calls: AtomicUsize::new(0),
        rewrite: None,
    })
}

fn write_command(path: &Path) -> String {
    format!(
        "printf changed > '{}'",
        path.to_string_lossy().replace('\'', "'\\''")
    )
}

#[tokio::test]
async fn plan_refuses_dynamic_mutators_before_approval_across_filesystem_policies_and_rules() {
    for (sandbox, explicit_allow) in [
        (
            klaus_agent_core::types::permission::SandboxMode::ReadOnly,
            false,
        ),
        (
            klaus_agent_core::types::permission::SandboxMode::DangerFullAccess,
            false,
        ),
        (
            klaus_agent_core::types::permission::SandboxMode::ReadOnly,
            true,
        ),
    ] {
        let root = Directory::new();
        let seed = root.0.join("read.txt");
        let marker = root.0.join("must-not-change");
        std::fs::write(&seed, "readable").unwrap();
        let read = Probe::new(READ, Effect::Read(seed));
        let mutate = Probe::new(MUTATE, Effect::Mutate(marker.clone()));
        let provider = Provider::new(
            vec![
                Reply::Tool("read", READ, json!({})),
                Reply::Tool("mutate", MUTATE, json!({})),
                Reply::Text,
            ],
            vec![],
        );
        let approvals = no_approvals();
        let rig = Rig::new(
            &root.0,
            provider.clone(),
            vec![read.clone(), mutate.clone()],
            sandbox,
            (if explicit_allow { vec![MUTATE] } else { vec![] }, vec![]),
            approvals.clone(),
            vec![],
        )
        .await;
        let before = rig.session.runtime_policy_state();
        rig.run().await;
        assert_eq!(approvals.calls.load(Ordering::SeqCst), 0);
        assert_eq!(read.calls.load(Ordering::SeqCst), 1);
        assert_eq!(mutate.calls.load(Ordering::SeqCst), 0);
        assert!(!marker.exists());
        assert!(!provider.result_is_error("read"));
        assert!(provider.result_is_error("mutate"));
        assert_eq!(
            read.ordinary_policies.lock().unwrap().as_slice(),
            &[Some(rig.sandbox)]
        );
        assert_eq!(rig.session.runtime_policy_state(), before);
        provider.assert_mode(false, CollaborationMode::Plan);
        assert_eq!(provider.requests.lock().unwrap().len(), 3);
        rig.finish().await;
    }
}

#[tokio::test]
async fn captured_plan_effect_limit_survives_mode_change_until_the_next_turn() {
    let root = Directory::new();
    let marker = root.0.join("next-default-turn-only");
    let mutate = Probe::new(MUTATE, Effect::Mutate(marker.clone()));
    let provider = Provider::new(
        vec![
            Reply::Tool("captured-plan", MUTATE, json!({})),
            Reply::Text,
            Reply::Tool("next-default", MUTATE, json!({})),
            Reply::Text,
        ],
        vec![],
    );
    let approvals = no_approvals();
    let rig = Rig::new(
        &root.0,
        provider.clone(),
        vec![mutate.clone()],
        klaus_agent_core::types::permission::SandboxMode::DangerFullAccess,
        (vec![], vec![]),
        approvals.clone(),
        vec![],
    )
    .await;
    let ordinary = rig.session.runtime_policy_state();
    let weak = Arc::downgrade(&rig.session);
    *provider.on_first_root.lock().unwrap() = Some(Arc::new(move || {
        weak.upgrade()
            .unwrap()
            .planning_runtime()
            .set_mode(CollaborationMode::Default, None)
            .unwrap();
    }));
    rig.run().await;
    assert_eq!(rig.session.collaboration_mode(), CollaborationMode::Default);
    assert!(provider.result_is_error("captured-plan"));
    assert_eq!(mutate.calls.load(Ordering::SeqCst), 0);
    assert!(!marker.exists());
    provider.assert_mode(false, CollaborationMode::Plan);
    rig.run().await;
    assert!(!provider.result_is_error("next-default"));
    assert_eq!(mutate.calls.load(Ordering::SeqCst), 1);
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), "changed");
    {
        let requests = provider.requests.lock().unwrap();
        assert_eq!(requests.len(), 4);
        let expected = instruction_part(&default_config(), CollaborationMode::Default);
        for (_, request) in &requests[2..] {
            assert_eq!(
                request
                    .system
                    .iter()
                    .filter(|part| part.name == "collaboration-mode")
                    .collect::<Vec<_>>(),
                vec![&expected]
            );
        }
    }
    assert_eq!(approvals.calls.load(Ordering::SeqCst), 0);
    assert_eq!(rig.session.runtime_policy_state(), ordinary);
    rig.finish().await;
}

#[tokio::test]
async fn fresh_plan_research_child_reads_without_gaining_mutation_from_its_ordinary_preset() {
    let root = Directory::new();
    let seed = root.0.join("research.txt");
    let marker = root.0.join("child-must-not-change");
    std::fs::write(&seed, "research data").unwrap();
    let read = Probe::new(READ, Effect::Read(seed));
    let mutate = Probe::new(MUTATE, Effect::Mutate(marker.clone()));
    let roles = vec![
        AgentDefinition {
            name: "plan-effect-research".into(),
            description: "Read owned fixture data".into(),
            runtime_policy: Some(klaus_agent_core::engine::session::RuntimePolicyUpdate {
                approval_policy: Some(klaus_agent_core::types::permission::ApprovalPolicy::Never),
                sandbox_mode: Some(
                    klaus_agent_core::types::permission::SandboxMode::DangerFullAccess,
                ),
                approvals_reviewer: None,
                sandbox_workspace_write: None,
                network_allowed: Some(true),
            }),
            ..Default::default()
        },
        AgentDefinition {
            name: "plan-effect-worktree".into(),
            description: "A role requiring a new worktree".into(),
            isolation: Some(Isolation::Worktree),
            runtime_policy: Some(klaus_agent_core::engine::session::RuntimePolicyUpdate {
                approval_policy: Some(klaus_agent_core::types::permission::ApprovalPolicy::Never),
                sandbox_mode: Some(
                    klaus_agent_core::types::permission::SandboxMode::DangerFullAccess,
                ),
                approvals_reviewer: None,
                sandbox_workspace_write: None,
                network_allowed: Some(true),
            }),
            ..Default::default()
        },
    ];
    let provider = Provider::new(
        vec![
            Reply::Tool(
                "research-task",
                "task",
                json!({"model": "low",
                    "description":"Bounded Plan research", "prompt":CHILD_MARKER,
                    "subagent_type":"plan-effect-research", "run_in_background":false
                }),
            ),
            Reply::Tool(
                "worktree-task",
                "task",
                json!({"model": "low",
                    "description":"Forbidden Plan worktree", "prompt":"No isolated child may start.",
                    "subagent_type":"plan-effect-worktree", "run_in_background":false
                }),
            ),
            Reply::Text,
        ],
        vec![
            Reply::Tool("child-read", READ, json!({})),
            Reply::Tool("child-mutate", MUTATE, json!({})),
            Reply::Text,
        ],
    );
    let approvals = no_approvals();
    let rig = Rig::new(
        &root.0,
        provider.clone(),
        vec![
            create_task_tool(TaskToolOptions {
                model_catalog: false,
                model_options: Vec::new(),
                agents: roles
                    .iter()
                    .map(
                        |role| klaus_agent_core::tools::builtins::task::AgentDefinition {
                            name: role.name.clone(),
                            description: role.description.clone(),
                            isolation: role.isolation,
                            background: role.background,
                        },
                    )
                    .collect(),
            }),
            read.clone(),
            mutate.clone(),
        ],
        klaus_agent_core::types::permission::SandboxMode::ReadOnly,
        (vec!["task"], vec![]),
        approvals.clone(),
        roles,
    )
    .await;
    let ordinary = rig.session.runtime_policy_state();
    rig.run().await;
    assert_eq!(approvals.calls.load(Ordering::SeqCst), 0);
    assert_eq!(read.calls.load(Ordering::SeqCst), 1);
    assert_eq!(mutate.calls.load(Ordering::SeqCst), 0);
    assert!(!marker.exists());
    assert!(!provider.result_is_error("research-task"));
    assert!(!provider.result_is_error("child-read"));
    assert!(provider.result_is_error("child-mutate"));
    assert!(provider.result_is_error("worktree-task"));
    assert_eq!(provider.worktree_calls.load(Ordering::SeqCst), 0);
    assert!(!root.0.join("worktree-create-hook").exists());
    provider.assert_mode(false, CollaborationMode::Plan);
    provider.assert_mode(true, CollaborationMode::Plan);
    assert_eq!(provider.requests.lock().unwrap().len(), 6);
    assert_eq!(rig.session.runtime_policy_state(), ordinary);
    let snapshot = rig.session.subagent_lifecycle_snapshot();
    assert_eq!(snapshot.agents.len(), 1);
    let child_id = snapshot.agents[0].agent_id.try_to_utf8().unwrap();
    rig.finish().await;
    let child = rig
        .store
        .read_sub_agent(&rig.thread, &child_id)
        .await
        .unwrap();
    assert_eq!(child.meta.unwrap().permission_mode, None);
    let creations: Vec<_> = child
        .raw_items
        .iter()
        .filter_map(|item| match item {
            RolloutItem::Event(event) if event.event == "ordinary_subagent_creation" => {
                event.payload.as_ref()
            }
            _ => None,
        })
        .collect();
    assert_eq!(creations.len(), 1);
    assert_eq!(creations[0].get("permissionMode"), None);
    assert_eq!(
        creations[0].get("collaborationOrigin"),
        Some(&runtime_json::parse(&r#"{"version":1,"mode":"plan"}"#.into()).unwrap())
    );
    assert!(!marker.exists());
}

#[tokio::test]
async fn plan_shell_reads_run_but_direct_and_nested_runtime_writes_do_not() {
    let root = Directory::new();
    let direct_marker = root.0.join("direct-shell-write");
    let nested_marker = root.0.join("nested-shell-write");
    let nested = Probe::new(
        NESTED_BASH,
        Effect::NestedBash(write_command(&nested_marker)),
    );
    let provider = Provider::new(
        vec![
            Reply::Tool("shell-read", "bash", json!({"command":"pwd"})),
            Reply::Tool(
                "shell-write",
                "bash",
                json!({"command":write_command(&direct_marker)}),
            ),
            Reply::Tool("nested-shell-write", NESTED_BASH, json!({})),
            Reply::Text,
        ],
        vec![],
    );
    let approvals = no_approvals();
    let rig = Rig::new(
        &root.0,
        provider.clone(),
        vec![create_bash_tool(), nested.clone()],
        klaus_agent_core::types::permission::SandboxMode::DangerFullAccess,
        (vec![], vec![]),
        approvals.clone(),
        vec![],
    )
    .await;
    let ordinary = rig.session.runtime_policy_state();
    rig.run().await;
    assert!(!provider.result_is_error("shell-read"));
    assert!(provider.result_is_error("shell-write"));
    assert!(provider.result_is_error("nested-shell-write"));
    assert_eq!(
        nested.calls.load(Ordering::SeqCst),
        1,
        "the read-declared adapter actually reached its runtime callback"
    );
    assert!(!direct_marker.exists());
    assert!(!nested_marker.exists());
    assert_eq!(approvals.calls.load(Ordering::SeqCst), 0);
    assert_eq!(rig.session.runtime_policy_state(), ordinary);
    assert_eq!(provider.requests.lock().unwrap().len(), 4);
    rig.finish().await;
}

#[tokio::test]
async fn a_read_only_shell_approval_cannot_rewrite_the_call_into_a_plan_write() {
    let root = Directory::new();
    let marker = root.0.join("approval-rewrite-write");
    let bash = create_bash_tool();
    let original = runtime_json::parse(&json!({"command":"pwd"}).to_string().into()).unwrap();
    let rewritten =
        runtime_json::parse(&json!({"command":write_command(&marker)}).to_string().into()).unwrap();
    let approvals = Arc::new(Approvals {
        calls: AtomicUsize::new(0),
        rewrite: Some((bash.parse_input(&original).unwrap(), rewritten)),
    });
    let provider = Provider::new(
        vec![
            Reply::Tool("rewritten-read", "bash", json!({"command":"pwd"})),
            Reply::Text,
        ],
        vec![],
    );
    let rig = Rig::new(
        &root.0,
        provider.clone(),
        vec![bash],
        klaus_agent_core::types::permission::SandboxMode::ReadOnly,
        (vec![], vec!["bash"]),
        approvals.clone(),
        vec![],
    )
    .await;
    let ordinary = rig.session.runtime_policy_state();
    assert_eq!(
        ordinary.approval_policy,
        Some(klaus_agent_core::types::permission::ApprovalPolicy::OnRequest),
        "the rewrite fixture must reach the ordinary approval handler"
    );
    rig.run().await;
    assert_eq!(
        approvals.calls.load(Ordering::SeqCst),
        1,
        "only the originally read-only command is presented for ordinary approval"
    );
    assert!(provider.result_is_error("rewritten-read"));
    assert!(!marker.exists());
    assert_eq!(rig.session.runtime_policy_state(), ordinary);
    assert_eq!(provider.requests.lock().unwrap().len(), 2);
    rig.finish().await;
}
