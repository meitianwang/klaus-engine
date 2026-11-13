//! The Session: owns message history, resolved config, rollout sink, the turn
//! loop, background agents, compaction wiring, rewind, hooks, reminders, the MCP
//! hub, and tools. Plus the re-exported `resolve_config`, the crate-wide
//! config resolver (see `engine::session_config`).
//!
//! The engine mutates a single instance freely from async methods, closures
//! and detached background tasks; the Rust realization is an
//! `Arc<Session>` whose mutable state lives behind interior mutability
//! (`Mutex`/`RwLock`/atomics); every seam that captured `this` in the source
//! captures a cloned `Arc<Session>` (or a cloned shared handle) here.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64};
use std::sync::{Arc, Mutex, RwLock, Weak};

use crate::sync_ext::MutexExt;

use futures::stream::Stream;

use crate::capabilities::{RuntimeCapabilityRegistry, SelectedRuntimeCapabilityRoot};
use crate::checkpoint::checkpoint_service::CheckpointService;
use crate::extensions::commands::{Command, LocalRunOutcome};
use crate::extensions::runtime_snapshot::ExtensionRuntimeSnapshot;
use crate::extensions::skills::conditional_skills::ConditionalSkillGate;
use crate::extensions::skills::loader::Skill;
use crate::hooks::bus::HookBus;
use crate::hooks::setup::SetupLifecycleSnapshot;
use crate::lsp::lsp_manager::LspManager;
use crate::mcp::client::{ManagedClient, McpAuthorizeHandler};
use crate::mcp::connection_manager::McpConnectionManager;
use crate::mcp::credential_store::CredentialStore;
use crate::mcp::elicitation::McpElicitationRouter;
use crate::mcp::types::McpServerConfig;
use crate::memory::agent_memory::{AgentMemoryHost, AgentMemoryLayout};
use crate::memory::maintenance::{MemoryMaintenanceQueryError, MemoryMaintenanceSnapshot};
use crate::memory::pinned_root::PinnedMemoryRoot;
use crate::memory::snapshot_decision::{
    AgentMemorySnapshotDecisionAudit, AgentMemorySnapshotDecisionPolicy,
    AgentMemorySnapshotDecisionRegistry,
};
use crate::observability::cost_tracker::CostTracker;
use crate::observability::logger::Logger;
use crate::observability::otel::OtelObserver;
use crate::permission::engine::PermissionEngine;
use crate::permission::permission_update::SessionPermissionState;
use crate::runtime_process::RuntimeEnv;
use crate::runtime_process::RuntimeProcessContext;
use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeJson, RuntimeObject,
    RuntimePath, RuntimeString,
};
use crate::schemas::config::{ResolvedConfig, SandboxWorkspaceWrite};
use crate::schemas::content::ContentBlock;
use crate::schemas::message::Message;
use crate::schemas::rollout::SessionMeta;
use crate::store::session_store::{LiveRollout, SessionStore};
use crate::tools::file_state::InMemoryFileStateCache;
use crate::types::abort::AbortSignal;
use crate::types::events::EngineEvent;
use crate::types::events::FileChange;
use crate::types::events::FileChangeKind;
use crate::types::events::ToolDomainEvent;
use crate::types::permission::{ApprovalHandler, ApprovalReviewer};
use crate::types::provider::{CallModel, ModelRequest, ModelStream, SystemPromptPart};
use crate::types::reminder::ReminderProvider;
use crate::types::sandbox::SandboxBackend;
/// Re-exported so the Agent factory can name the session's swarm identity seed.
pub use crate::types::tool::SwarmIdentity;
use crate::types::tool::{ElicitHandler, McpResourceClient, Tool, WorkflowJournalEntry};

#[path = "session_error_details.rs"]
mod error_details;
pub use error_details::{sanitize_turn_error_details, turn_error_details_from_lifecycle};

/// The rollout persistence sink.
pub type RolloutSink = Arc<dyn LiveRollout>;
use crate::worktree::worktree::WorktreeSession;

use crate::engine::agent_definitions::{AgentDefinition, AgentRegistry};
use crate::engine::deps::EngineDeps;
use crate::engine::message_queue::MessageQueue;
use crate::engine::resource_governor::ResourceGovernor;
use crate::engine::subagent_lifecycle::SubagentLifecycleRegistry;

use tokio_util::sync::CancellationToken;

fn decode_provider_unit_enum<T: Copy>(
    value: &RuntimeJson,
    variants: &[RuntimeEnumVariant<'_>],
    values: &[T],
) -> Result<T, RuntimeCodecError> {
    runtime_codec::decode_external(value, variants, |variant| match variant {
        RuntimeExternalVariant::Unit { variant } => values
            .get(variant)
            .copied()
            .ok_or_else(|| RuntimeCodecError::new(RuntimeCodecErrorKind::UnknownEnumVariant)),
        RuntimeExternalVariant::Payload { .. } => Err(RuntimeCodecError::new(
            RuntimeCodecErrorKind::InvalidEnumShape,
        )),
    })
}

/// A boxed engine-event stream (a `Turn`'s events).
pub type EngineEventStream = std::pin::Pin<Box<dyn Stream<Item = EngineEvent> + Send>>;
/// A resolvable system-prompt provider: `() -> Future<Vec<SystemPromptPart>>`.
pub type SystemPromptProvider = Arc<
    dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Vec<SystemPromptPart>> + Send>>
        + Send
        + Sync,
>;

fn append_developer_instructions(
    provider: SystemPromptProvider,
    developer_instructions: Option<RuntimeString>,
) -> SystemPromptProvider {
    let Some(developer_instructions) = developer_instructions else {
        return provider;
    };
    Arc::new(move || {
        let provider = provider.clone();
        let developer_instructions = developer_instructions.clone();
        Box::pin(async move {
            let mut parts = provider().await;
            parts.push(SystemPromptPart {
                name: "developer".into(),
                text: developer_instructions,
                cacheable: None,
                cache_scope: None,
            });
            parts
        })
    })
}
/// Tool objects, erased and shared.
pub type AnyTool = Arc<dyn Tool>;

/// A shareable model-call handle. The engine's [`CallModel`] seam is a `Box`, but
/// the session must call the model from many places across turns, so it is held as
/// a cheaply-cloned `Arc` and re-boxed on demand for each `query` / recall call.
type SharedCallModel = Arc<dyn Fn(ModelRequest, AbortSignal) -> ModelStream + Send + Sync>;

/// The session's clone of [`EngineDeps`], with a shareable model call. Every field
/// is `Arc`-backed so a fresh `EngineDeps` can be minted for each `query` call
/// (the source shares one deps object across every turn).
#[derive(Clone)]
pub(crate) struct SharedDeps {
    provider_id: RuntimeString,
    call_model: SharedCallModel,
    estimate_tokens: crate::engine::deps::EstimateTokensFn,
    compactor: Arc<dyn crate::engine::deps::Compactor>,
    uuid: crate::engine::deps::UuidFn,
    now: crate::engine::deps::NowFn,
}

/// Stable non-provider dependencies retained across inference revisions.
struct SessionIdentityDeps {
    uuid: crate::engine::deps::UuidFn,
    now: crate::engine::deps::NowFn,
}

impl SharedDeps {
    fn from_engine(d: EngineDeps) -> Self {
        SharedDeps {
            provider_id: d.provider_id,
            call_model: Arc::from(d.call_model),
            estimate_tokens: d.estimate_tokens,
            compactor: d.compactor,
            uuid: d.uuid,
            now: d.now,
        }
    }

    /// A fresh owned `CallModel` box delegating to the shared handle.
    pub(crate) fn call_model_owned(&self) -> CallModel {
        let cm = self.call_model.clone();
        Box::new(move |req, signal| cm(req, signal))
    }

    /// A fresh `EngineDeps` for a `query` call (the source passes `this.deps`).
    pub(crate) fn to_engine_deps(&self) -> EngineDeps {
        EngineDeps {
            provider_id: self.provider_id.clone(),
            call_model: self.call_model_owned(),
            estimate_tokens: self.estimate_tokens.clone(),
            compactor: self.compactor.clone(),
            uuid: self.uuid.clone(),
            now: self.now.clone(),
        }
    }
}

/// Adapts a host-connected [`ManagedClient`] to the resource/prompt-tool view
/// (name + list/read resources + list/get prompts + instructions).
struct ManagedResourceClient(Arc<ManagedClient>, &'static str);

#[async_trait::async_trait]
impl McpResourceClient for ManagedResourceClient {
    fn native_mcp_client(&self) -> Option<ManagedClient> {
        Some(self.0.as_ref().clone())
    }
    fn instructions_group(&self) -> &'static str {
        self.1
    }
    fn name(&self) -> &RuntimeString {
        &self.0.name
    }
    fn instructions(&self) -> Option<&RuntimeString> {
        self.0.instructions.as_ref()
    }
    async fn list_resources(&self) -> Vec<crate::types::tool::McpResourceDescriptor> {
        self.0.resources.clone()
    }
    async fn read_resource(
        &self,
        uri: &RuntimeString,
    ) -> Vec<crate::types::tool::McpResourceContent> {
        self.0.read_resource(uri).await
    }
    async fn list_prompts(&self) -> Option<Vec<crate::types::tool::McpPromptDescriptor>> {
        Some(self.0.prompts.clone())
    }
    async fn get_prompt(
        &self,
        name: &RuntimeString,
        args: Option<RuntimeObject>,
    ) -> Option<RuntimeString> {
        Some(self.0.get_prompt(name, args).await)
    }
}

/// The default per-agent memory host. First-use initialization is an explicit
/// policy; update requests have no handler and therefore fail closed without a
/// synced marker.
struct DefaultAgentMemoryHost {
    layout: crate::memory::agent_memory::DefaultAgentMemoryLayout,
}

#[async_trait::async_trait]
impl AgentMemoryHost for DefaultAgentMemoryHost {
    fn layout(&self) -> &dyn AgentMemoryLayout {
        &self.layout
    }
}

// The remaining regions of this module live in sibling files; every item keeps
// its `engine::session::` path through the re-exports below. The `pub use`
// lists are the module's EXPLICIT public surface; the `pub(crate)` globs only
// keep in-crate callers working and export nothing outside the crate.
#[path = "session_subagent_mcp.rs"]
mod subagent_mcp;
pub(crate) use subagent_mcp::{SubagentMcpCapture, SubagentMcpRuntime};
#[path = "session_types.rs"]
mod session_types_part;
#[allow(unused_imports)]
pub(crate) use session_types_part::*;
pub use session_types_part::{
    BackgroundAgentInfo, InferencePublication, Input, PreparedReviewRequirements, SessionInit,
    SessionInitError, SwarmInit, Turn, TurnOptions, TurnResult, UserContentPart, ValidatedInput,
};
#[path = "session_lifecycle_state.rs"]
mod session_lifecycle_state_part;
pub use session_lifecycle_state_part::SessionShutdownOutcome;
#[allow(unused_imports)]
pub(crate) use session_lifecycle_state_part::*;
#[path = "session_advisor.rs"]
mod session_advisor_part;
#[path = "session_approval_async.rs"]
mod session_approval_async_part;
#[path = "session_approval_review.rs"]
mod session_approval_review_part;
#[path = "session_core_state.rs"]
mod session_core_state_part;
#[path = "session_orchestrator.rs"]
mod session_orchestrator_part;
#[path = "session_usage.rs"]
mod session_usage_part;

#[path = "session_inference.rs"]
mod session_inference_part;
#[path = "session_subagent_targets.rs"]
mod session_subagent_targets_part;
#[path = "session_targets.rs"]
mod session_targets_part;
#[allow(unused_imports)]
pub(crate) use session_core_state_part::*;
use session_inference_part::InferenceAdmission;
pub(crate) use session_inference_part::{
    InferenceActivityGuard, InferenceBundle, InferenceSlot, InputModelRevision,
};
pub use session_subagent_targets_part::{SubagentTargetResolver, MODEL_TARGET_SOURCES};
pub(crate) use session_targets_part::TargetSnapshot;
pub use session_targets_part::{
    FeatureSlot, FeatureTargets, InferenceTarget, InferenceTargetState, InferenceTargets,
};
#[path = "session_core_control.rs"]
mod session_core_control_part;
#[path = "session_questions.rs"]
mod session_questions_part;
#[allow(unused_imports)]
pub(crate) use session_core_control_part::*;
#[path = "session_core_policy.rs"]
mod session_core_policy_part;
#[allow(unused_imports)]
pub(crate) use session_core_policy_part::*;
#[path = "session_outcomes.rs"]
mod session_outcomes_part;
#[allow(unused_imports)]
pub(crate) use session_outcomes_part::*;
pub use session_outcomes_part::{
    BudgetState, CompactOutcome, ContextWindowState, DreamNowCompletion, DreamNowOutcome,
    DreamNowReason, DreamNowTask, EnqueueOptions, GenerateTitleResult, PredictionContext,
    RollbackOutcome, SessionContextSnapshot, SideQuestionResult,
};
// The crate-wide config resolver moved to its own module; the re-export keeps
// every `engine::session::resolve_config*` call path unchanged.
pub use crate::engine::session_config::{
    config_layer_from_value, resolve_config, resolve_config_default, resolve_config_layers,
};
#[path = "session_context_restoration.rs"]
mod session_context_restoration_part;
#[path = "session_controllers.rs"]
mod session_controllers_part;
#[allow(unused_imports)]
pub(crate) use session_controllers_part::*;
#[path = "session_isolation.rs"]
mod session_isolation_part;
#[allow(unused_imports)]
pub(crate) use session_isolation_part::*;
#[path = "session_spawn.rs"]
mod session_spawn_part;
#[allow(unused_imports)]
pub(crate) use session_spawn_part::*;
#[path = "session_run.rs"]
mod session_run_part;
#[allow(unused_imports)]
pub(crate) use session_run_part::*;
#[path = "session_cron.rs"]
mod session_cron_part;
#[path = "session_goal.rs"]
mod session_goal_part;
#[path = "session_prewarm.rs"]
mod session_prewarm_part;
#[path = "session_retained_goal.rs"]
mod session_retained_goal_part;
#[allow(unused_imports)]
pub(crate) use session_cron_part::*;
#[allow(unused_imports)]
pub(crate) use session_goal_part::*;
#[allow(unused_imports)]
pub(crate) use session_prewarm_part::*;
#[path = "session_approval_budget.rs"]
mod session_approval_budget_part;
pub(crate) use session_approval_budget_part::*;
#[path = "session_approval_query.rs"]
mod session_approval_query_part;
pub(crate) use session_approval_query_part::*;
#[path = "session_review.rs"]
mod session_review_part;
#[allow(unused_imports)]
pub(crate) use session_review_part::*;
pub use session_review_part::{
    ReviewCodeLocation, ReviewFinding, ReviewLineRange, ReviewOutput, ReviewRequest,
    ReviewSessionResult, ReviewTarget,
};

// ---------------------------------------------------------------------------
// The Session
// ---------------------------------------------------------------------------

/// Ordered sink receiving every Agent Memory snapshot-decision audit.
pub(crate) type SnapshotDecisionOrderedSink =
    Arc<dyn Fn(AgentMemorySnapshotDecisionAudit) + Send + Sync + 'static>;

/// The session's Agent Memory snapshot-decision runtime cluster: the decision
/// policy and registry, the async durable-persistence gate and durability
/// chains, the fatal-inconsistency watch, the late-bound recovery `Weak<Self>`,
/// and the ordered/broadcast audit sinks.
pub(crate) struct SessionSnapshotState {
    policy: AgentMemorySnapshotDecisionPolicy,
    decisions: AgentMemorySnapshotDecisionRegistry,
    /// Agent Memory snapshot persistence gate. The async `event_gate`
    /// linearizes registry mutation, rollout append, and live event
    /// publication; each staged registry/filesystem proposal crosses `serial`
    /// through its exact append+flush and in-memory CAS (filesystem stages
    /// retain their private OS transaction lock while that guard is held).
    persistence: AsyncPersistenceGate,
    /// Session-owned durability chains and constructor recovery receipts.
    durability: Mutex<SnapshotDurabilityRegistry>,
    /// Wakes already-waiting query/recovery callers when persistence becomes
    /// fatally inconsistent; the retained watch value also covers late calls.
    fatal_events: tokio::sync::watch::Sender<Option<RuntimeString>>,
    /// Late-bound owner for lazy recovery from public `&self` entrypoints.
    self_ref: std::sync::OnceLock<Weak<Session>>,
    ordered_sink: Mutex<Option<SnapshotDecisionOrderedSink>>,
    events: tokio::sync::broadcast::Sender<AgentMemorySnapshotDecisionAudit>,
}

/// Pending model selection and live session permission rules.
pub(crate) struct SessionModeControl {
    pending_model_override: Mutex<Option<RuntimeString>>,
    /// A historical plan-document binding restored from older transcripts.
    plan_name: Mutex<Option<RuntimeString>>,
    permission: Arc<Mutex<SessionPermissionState>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimePolicyUpdate {
    pub approval_policy: Option<crate::types::permission::ApprovalPolicy>,
    pub sandbox_mode: Option<crate::types::permission::SandboxMode>,
    pub approvals_reviewer: Option<crate::types::permission::ApprovalsReviewer>,
    pub sandbox_workspace_write: Option<SandboxWorkspaceWrite>,
    pub network_allowed: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimePolicyState {
    pub approval_policy: Option<crate::types::permission::ApprovalPolicy>,
    pub sandbox_mode: Option<crate::types::permission::SandboxMode>,
    pub approvals_reviewer: Option<crate::types::permission::ApprovalsReviewer>,
    pub generation: u64,
    pub sandbox_workspace_write: SandboxWorkspaceWrite,
    pub network_allowed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimePolicyCommit {
    pub previous: RuntimePolicyState,
    pub effective: RuntimePolicyState,
}

pub(crate) fn runtime_network_allowed(
    mode: Option<crate::types::permission::SandboxMode>,
    explicit: Option<bool>,
    workspace: &SandboxWorkspaceWrite,
) -> bool {
    use crate::types::permission::SandboxMode;
    match mode.unwrap_or(SandboxMode::ReadOnly) {
        SandboxMode::DangerFullAccess => true,
        SandboxMode::ReadOnly => explicit.unwrap_or(false),
        SandboxMode::WorkspaceWrite => explicit.unwrap_or(workspace.network_access),
    }
}

pub(crate) fn runtime_sandbox_retry_allowed(
    policy: Option<crate::types::permission::ApprovalPolicy>,
) -> bool {
    use crate::types::permission::ApprovalPolicy;
    match policy.unwrap_or_default() {
        ApprovalPolicy::UnlessTrusted => true,
        ApprovalPolicy::Granular(settings) => settings.sandbox_approval,
        ApprovalPolicy::OnRequest | ApprovalPolicy::Never => false,
    }
}

pub(crate) struct RuntimePolicySnapshot {
    pub state: RuntimePolicyState,
    pub sandbox_backend: Arc<dyn SandboxBackend>,
}

impl RuntimePolicySnapshot {
    pub(crate) fn state(&self) -> RuntimePolicyState {
        self.state.clone()
    }

    pub(crate) fn apply_to_context(&self, context: &mut crate::types::tool::ToolContext) {
        context.approval_policy = self.state.approval_policy;
        context.sandbox_mode = self.state.sandbox_mode;
        context.approvals_reviewer = self.state.approvals_reviewer;
        context.network_allowed = Some(self.state.network_allowed);
        context.sandbox_retry_on_denial = Some(
            runtime_sandbox_retry_allowed(self.state.approval_policy)
                && context.sandbox.task_resource_scope().is_none(),
        );
        if let Some(mode) = self.state.sandbox_mode {
            let mut roots = vec![context.cwd.as_runtime_string().clone()];
            roots.extend(
                context
                    .permission
                    .additional_writable_roots
                    .iter()
                    .map(|root| root.as_runtime_string().clone()),
            );
            context.sandbox = Arc::new(crate::sandbox::runtime_policy::RuntimePolicySandbox::new(
                context.sandbox.clone(),
                mode,
                roots,
                self.state.network_allowed,
                self.state.sandbox_workspace_write.exclude_tmpdir_env_var,
                self.state.sandbox_workspace_write.exclude_slash_tmp,
            ));
        }
    }
}

/// The session's memory-root & nested-memory loading cluster: the main-memory
/// directory content, its construction-time pinned identity and pin error, the
/// resolved default project-memory root eligible for the restricted maintenance
/// sensitive-path exemption, and the nested-memory trigger/loaded-path tracking.
pub(crate) struct SessionMemoryRoots {
    main: Option<String>,
    /// Construction-time identity for the main memory directory. Maintenance
    /// never re-pins this pathname after the Session becomes observable.
    pinned_root: Option<Arc<PinnedMemoryRoot>>,
    pin_error: Option<RuntimeString>,
    /// The exact default project-memory root resolved by core. This is the only
    /// root eligible for the restricted maintenance sensitive-path exemption.
    internal_root: Option<RuntimePath>,
    nested_triggers: Mutex<HashSet<RuntimePath>>,
    nested_loaded_paths: Arc<Mutex<HashSet<RuntimePath>>>,
    invoked_skill_content: Mutex<
        HashMap<Option<RuntimeString>, Vec<crate::context::skill_receipts::InvokedSkillContent>>,
    >,
}

/// The session's MCP runtime cluster: host resource clients and their
/// instructions, plugin-declared server configs, the lazily-initialized
/// connection manager with its live clients, and the runtime/shutdown gate.
pub(crate) struct SessionMcpRuntime {
    resource_clients: Vec<Arc<dyn McpResourceClient>>,
    instructions: Vec<McpInstruction>,
    plugin_servers: RwLock<Vec<McpServerConfig>>,
    manual_servers: RwLock<Vec<McpServerConfig>>,
    manager_initialized: tokio::sync::OnceCell<()>,
    registration_revision: AtomicU64,
    /// The lifecycle-transition sink handed to the connection manager at
    /// construction and filled in later by the host, so the observer does not
    /// have to become a `SessionInit` field.
    status_observer: Arc<crate::mcp::types::McpStatusObserverSlot>,
    manager: tokio::sync::Mutex<McpConnectionManager>,
    runtime_clients: RwLock<Vec<Arc<ManagedClient>>>,
    gate: Arc<tokio::sync::RwLock<()>>,
}

/// One complete extension projection, staged by the endpoint and committed only
/// at a turn-admission boundary. Running turns retain their captured tools and
/// hook bus until their lease ends.
pub(crate) struct SessionPluginUpdate {
    pub agents: Vec<AgentDefinition>,
    pub skills: Option<Arc<dyn Fn() -> Vec<Skill> + Send + Sync>>,
    pub commands: Vec<Command>,
    pub hooks: Vec<crate::hooks::types::Hook>,
    pub plugin_mcp_servers: Vec<McpServerConfig>,
    pub extension_runtime: ExtensionRuntimeSnapshot,
    pub output_styles: Vec<crate::extensions::output_styles::OutputStyle>,
    pub lsp_servers: Vec<crate::lsp::types::LspServerConfig>,
    pub runtime_capabilities: RuntimeCapabilityRegistry,
    pub browser_tool_overrides: HashSet<RuntimeString>,
    pub host_supplied_tools: bool,
}

#[derive(Clone)]
pub(crate) struct SessionSkillSnapshot {
    pub skills: Option<Arc<dyn Fn() -> Vec<Skill> + Send + Sync>>,
    pub gate: Option<Arc<ConditionalSkillGate>>,
}

impl SessionSkillSnapshot {
    pub(crate) fn all(&self) -> Vec<Skill> {
        self.skills
            .as_ref()
            .map(|skills| skills())
            .unwrap_or_default()
    }

    pub(crate) fn visible(&self) -> Vec<Skill> {
        let all = self.all();
        self.gate
            .as_ref()
            .map_or_else(|| all.clone(), |gate| gate.filter(&all))
    }
}

pub(crate) struct SessionPluginProjection {
    pub skill_gate: Option<Arc<ConditionalSkillGate>>,
    pub agents: AgentRegistry,
    pub skills: Option<Arc<dyn Fn() -> Vec<Skill> + Send + Sync>>,
    pub commands: Vec<Command>,
    pub hook_bus: Option<Arc<HookBus>>,
    pub tools: Vec<Arc<dyn Tool>>,
    pub extension_runtime: ExtensionRuntimeSnapshot,
    pub plugin_mcp_servers: Vec<McpServerConfig>,
    pub active_output_style: Option<ActiveOutputStyle>,
    pub lsp_manager: Option<Arc<LspManager>>,
    pub lsp_configs: Vec<crate::lsp::types::LspServerConfig>,
    pub runtime_capabilities: RuntimeCapabilityRegistry,
}

pub(crate) struct SessionPluginPublication {
    pub projection: Arc<SessionPluginProjection>,
}

#[derive(Default)]
pub(crate) struct SessionPluginRuntime {
    pending: Option<SessionPluginPublication>,
    current: Option<Arc<SessionPluginProjection>>,
    step_mcp_applied: Option<Arc<SessionPluginProjection>>,
    mcp_dirty: bool,
}

/// The session's sub-agent runtime cluster: the background-agent registry, the
/// authoritative lifecycle registry, resumed transcripts and their surfaced
/// tool sets (keyed by sub-agent identity), and cached per-agent memory-recall
/// reminder providers.
/// Ordered observer used by the app-server to stream child conversations.
/// Every visible child is addressed by its own agent ID. Transcript and
/// notification ownership remain with that child's direct parent.
pub enum SubagentStreamEvent<'a> {
    Event(&'a EngineEvent),
    Prompt(&'a Message),
    Notification(&'a crate::schemas::agent_notification::AgentNotification),
}

pub type SubagentEventSink = Arc<dyn Fn(&str, SubagentStreamEvent<'_>) + Send + Sync>;

pub type AgentNotificationSink =
    Arc<dyn Fn(&crate::schemas::agent_notification::AgentNotification) + Send + Sync>;

pub(crate) struct SessionSubagentState {
    model_target_resolver: Mutex<Option<SubagentTargetResolver>>,
    notification_sink: Mutex<Option<AgentNotificationSink>>,
    notification_ids_by_child: Mutex<HashMap<String, HashSet<RuntimeString>>>,
    event_sink: Mutex<Option<SubagentEventSink>>,
    background_agents: Arc<Mutex<HashMap<String, BackgroundAgentEntry>>>,
    lifecycle: Mutex<SubagentLifecycleRegistry>,
    /// Each running producer leases the exact sidechain it may append to.
    parent_rollouts: Arc<crate::engine::subagent_rollouts::SubagentRollouts>,
    transcripts: Mutex<HashMap<SubAgentMapKey, Vec<Message>>>,
    surfaced: Mutex<HashMap<SubAgentMapKey, Arc<Mutex<HashSet<RuntimeString>>>>>,
    memory_recall_providers: Mutex<HashMap<String, Arc<dyn ReminderProvider>>>,
}

/// The session's memory-maintenance runtime cluster: the authoritative
/// snapshot, the sticky first-wins query-fatal fence, the durable-persistence
/// gate, and the live/isolated maintenance event sinks.
pub(crate) struct SessionMaintenanceState {
    snapshot: Mutex<MemoryMaintenanceSnapshot>,
    /// Sticky first-wins failure for an admitted lifecycle transition whose
    /// durable state can no longer be proven. Serving the last visible
    /// snapshot after this boundary would falsely label it authoritative.
    query_fatal: std::sync::OnceLock<MemoryMaintenanceQueryError>,
    /// Auto Memory / Auto Dream persistence gate: a lifecycle event cannot
    /// become query/sink-visible before its exact rollout prefix has crossed
    /// the `serial` barrier.
    persistence: SyncPersistenceGate,
    event_sink: Mutex<Option<MaintenanceEventSinkRegistration>>,
    isolated_sink: Mutex<IsolatedMaintenanceSinkState>,
}

/// The session's Setup init/maintenance runtime cluster: the execution serial,
/// the completed-init context-admission serial, the lifecycle snapshot, the
/// durable-persistence gate, and the terminal durability registry.
pub(crate) struct SessionSetupState {
    /// Serializes Setup init/maintenance execution.  A real run also crosses
    /// this gate before SessionStart so a pre-admitted init cannot overlap the
    /// first prompt.
    serial: Arc<tokio::sync::Mutex<()>>,
    /// Exactly one independent turn may own the completed init context from its
    /// durable Started claim through a durable Released/Consumed/Unknown
    /// terminal. The retained owner, never the caller future, holds this guard.
    context_admission_serial: Arc<tokio::sync::Mutex<()>>,
    lifecycle: Mutex<SetupLifecycleSnapshot>,
    /// Setup persistence gate. `serial` also guarantees a caller-cancellation
    /// terminal cannot overtake the still-running Started barrier, and a
    /// non-cooperative in-process hook that outlives a bounded shutdown can
    /// never append or start a Setup flush after `finalized` becomes visible
    /// under `event_gate`.
    persistence: SyncPersistenceGate,
    /// Session-owned terminal durability receipts. A caller dropping its Setup
    /// future cannot cancel the flush or let an idempotent replay pass it.
    durability: Mutex<SetupDurabilityRegistry>,
}

/// The session's auto-memory extraction + Dream scheduling runtime cluster: the
/// last-processed memory cursor, the extraction in-progress/pending flags, and
/// the Dream scan timestamp / in-progress flag / owner-generation tokens.
pub(crate) struct SessionMemoryScheduler {
    last_memory_message_uuid: Mutex<Option<RuntimeString>>,
    extraction_in_progress: AtomicBool,
    extraction_pending: AtomicBool,
    last_dream_scan_at: AtomicI64,
    dream_in_progress: AtomicBool,
    next_dream_owner: AtomicU64,
    active_dream_owner: AtomicU64,
    session_memory_config: Mutex<crate::memory::session_memory::SessionMemoryConfig>,
    session_memory_compact_config: Mutex<crate::memory::session_memory::SessionMemoryCompactConfig>,
    session_memory_initialized: AtomicBool,
    tokens_at_last_session_memory: AtomicU64,
    last_session_memory_cursor_uuid: Mutex<Option<RuntimeString>>,
    last_summarized_message_id: Mutex<Option<RuntimeString>>,
    session_memory_started_at: AtomicI64,
    session_memory_in_progress: AtomicBool,
    session_memory_settled: Arc<tokio::sync::Notify>,
    /// Cancellation handle for the in-flight session-notes fork, so shutdown
    /// can abort it like every other maintenance task.
    session_memory_abort: Mutex<Option<CancellationToken>>,
}

/// The session's live turn-loop runtime cluster: the ordered live-turn stack
/// with its generation counter and turn-active flag, the innermost turn's
/// abort token, and the once-per-session thread-started / cache-policy-warning
/// latches.
pub(crate) struct SessionTurnState {
    /// Every live turn, ordered by creation. A generation-specific lease owns
    /// each entry so dropping an unpolled or overlapping stream cannot clear a
    /// newer turn's state.
    active_stack: Mutex<Vec<ActiveTurnState>>,
    next_generation: AtomicU64,
    /// The source watermark survives lease retirement, empty turns and rollback.
    latest_prediction_source: Mutex<Option<(u64, Option<RuntimeString>)>>,
    active: AtomicBool,
    current_abort: Mutex<Option<CancellationToken>>,
    thread_started_emitted: AtomicBool,
    cache_policy_warned: AtomicBool,
    /// Epoch millis of the most recent provider response, or 0 before the first
    /// one. Survives between turns so the idle micro-compaction gap can measure
    /// the pause while the user was away, which is the only pause long enough to
    /// cool a provider prompt cache.
    last_response_at_ms: AtomicI64,
    /// Latched once the session has been idle past the longest prompt-cache
    /// lifetime. Never clears: the flag decides how prior-turn thinking blocks are
    /// declared on every later request, and that declaration is cache-key
    /// material, so flipping back would invalidate the prefix a second time.
    thinking_context_cold: AtomicBool,
    context_window_usage: Arc<Mutex<crate::context::window_usage::WindowUsageState>>,
    context_window_update_gate: Arc<tokio::sync::Mutex<()>>,
    /// Tokens retired by this session's compactions. Spent and never refunded, so
    /// the padded remaining-context countdown keeps descending instead of rewinding
    /// each time history is summarized away.
    padded_retired_tokens: AtomicU64,
    /// The highest long-horizon total this session has ever reported, so the padded
    /// countdown cannot go back up when live occupancy shrinks.
    padded_high_water_tokens: AtomicU64,
    /// The turn a USER interrupt landed on, retained until that turn's terminal
    /// record is written.
    ///
    /// An `Aborted` stop reason alone cannot justify telling the model "the user
    /// interrupted the previous turn on purpose": the same reason also comes from
    /// session shutdown and from a poisoned message queue. Only the Host-facing
    /// interrupt entry point sets this, and it records WHICH turn it hit so an
    /// overlapping or already-settled turn cannot inherit the marker. The turn's
    /// terminal writer takes it, so the guidance is persisted exactly once.
    user_interrupted_turn: Mutex<Option<RuntimeString>>,
}

/// The session's conversation-history cluster: the live message transcript,
/// the hidden-context message ids excluded from visible history, the resumed
/// checkpoint ids carried over from rollout, and the queued-message runtime.
pub(crate) struct SessionConversationState {
    history: Mutex<Vec<Message>>,
    retained_goal_instructions: Mutex<Vec<Message>>,
    goal_submission_ids: Mutex<HashSet<RuntimeString>>,
    goal_completions: Mutex<Vec<crate::goal::retained_context::GoalCompletion>>,
    hidden_message_ids: HashSet<RuntimeString>,
    resumed_checkpoints: Vec<Option<String>>,
    queue: Arc<MessageQueue>,
}

/// The session's working-tree cluster: the LIVE working directory (switches
/// when a worktree is entered), the cwd before the active worktree was
/// entered, and the active worktree session.
pub(crate) struct SessionWorkspaceState {
    cwd: RwLock<RuntimePath>,
    original_cwd: RwLock<RuntimePath>,
    current_worktree: Mutex<Option<WorktreeSession>>,
}

/// The session's teardown-lifecycle cluster: the dispose flag shared with
/// spawned tasks, the abandoned-resume marker, and the late-bound watch
/// receivers publishing the abandoned rollout close and the single shared
/// shutdown outcome.
pub(crate) struct SessionLifecycleState {
    disposed: Arc<AtomicBool>,
    /// Marks a resume-only Session whose caller disappeared before handoff.
    /// This permits only the final, already-producer-fenced rollout close to
    /// receive a fresh bounded poll budget after coarse timer advancement.
    abandoned_resume_cleanup: AtomicBool,
    abandoned_rollout_shutdown:
        std::sync::OnceLock<tokio::sync::watch::Receiver<Option<Result<(), RuntimeString>>>>,
    /// All concurrent shutdown callers observe the same completed cleanup
    /// result instead of returning early while the first caller is still
    /// draining maintenance, rollout, LSP, or MCP state.
    shutdown_completion:
        std::sync::OnceLock<tokio::sync::watch::Receiver<Option<SessionShutdownOutcome>>>,
}

/// The session's durability cluster: the live rollout sink, the session
/// store, and the lazily installed checkpoint service.
pub(crate) struct SessionPersistenceState {
    rollout: Option<Arc<dyn LiveRollout>>,
    ordered_rollout: Option<Arc<crate::store::session_rollout::SessionRollout>>,
    usage_receipts: Option<Arc<crate::store::receipt_broker::ReceiptBroker>>,
    tool_tasks: Arc<crate::engine::query::RetainedToolTasks>,
    store: Option<Arc<dyn SessionStore>>,
    checkpoint_svc: Mutex<Option<Arc<CheckpointService>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnGoalMetadata {
    pub goal_continuation: bool,
    pub input_origin: RuntimeString,
    pub goal_id: Option<RuntimeString>,
}
impl crate::runtime_values::RuntimeEncode for TurnGoalMetadata {
    fn encode_runtime(
        &self,
        out: &mut crate::runtime_values::RuntimeEncoder,
    ) -> Result<(), RuntimeCodecError> {
        let mut object = crate::runtime_values::RuntimeObjectEncoder::new();
        object.field("goalContinuation", &self.goal_continuation)?;
        object.field("inputOrigin", &self.input_origin)?;
        object.field("goalId", &self.goal_id)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}
impl crate::runtime_values::RuntimeDecode for TurnGoalMetadata {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        let object = value.as_object().ok_or_else(|| {
            RuntimeCodecError::new(crate::runtime_values::RuntimeCodecErrorKind::Custom(
                "goal turn metadata must be an object".into(),
            ))
        })?;
        let continuation = object
            .get("goalContinuation")
            .and_then(RuntimeJson::as_bool)
            .ok_or_else(|| {
                RuntimeCodecError::new(crate::runtime_values::RuntimeCodecErrorKind::Custom(
                    "goalContinuation must be a boolean".into(),
                ))
            })?;
        let origin = object
            .get("inputOrigin")
            .and_then(RuntimeJson::as_str)
            .filter(|origin| **origin == "user" || **origin == "automatic")
            .ok_or_else(|| {
                RuntimeCodecError::new(crate::runtime_values::RuntimeCodecErrorKind::Custom(
                    "invalid inputOrigin".into(),
                ))
            })?
            .clone();
        let goal_id = object.get("goalId").and_then(RuntimeJson::as_str).cloned();
        Ok(Self {
            goal_continuation: continuation,
            input_origin: origin,
            goal_id,
        })
    }
}
/// Renderer-facing turn index: which turn each persisted message belongs to,
/// and each turn's terminal status. Kept incrementally live during runs and
/// rebuilt from the rollout on resume, then served alongside `get_history` so
/// the renderer can fold by turn instead of guessing boundaries.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TurnIndex {
    pub turn_goal_metadata: HashMap<RuntimeString, TurnGoalMetadata>,
    /// Durable delivery receipts, separate from all model-visible history.
    pub agent_notifications: Vec<crate::schemas::agent_notification::AgentNotification>,
    /// message uuid -> turn id.
    pub message_turn: HashMap<RuntimeString, RuntimeString>,
    /// turn id -> the assistant message whose text is the completed Turn's final
    /// reply. Tools may run after this message, so position alone is not enough.
    pub turn_final_message: HashMap<RuntimeString, RuntimeString>,
    /// turn id -> observed lifecycle status. Live inspection adds `running`
    /// from active leases; persisted entries are terminal statuses only.
    pub turn_status: HashMap<RuntimeString, RuntimeString>,
    /// Turn start time in milliseconds from the engine clock. Kept separately
    /// from terminal status so a live renderer can calculate the exact duration.
    pub turn_started_at_ms: HashMap<RuntimeString, i64>,
    /// turn id -> absolute terminal time in milliseconds from the engine clock.
    /// Served with history so the renderer can keep the completion time after a
    /// session is reopened instead of replacing it with the reload time.
    pub turn_finished_at_ms: HashMap<RuntimeString, i64>,
    /// turn id -> elapsed execution time in milliseconds, persisted when the
    /// Turn reaches any terminal state and replayed on session resume.
    pub turn_duration_ms: HashMap<RuntimeString, u64>,
    /// turn id -> failure message, present only for `failed` turns. Served with
    /// `get_history` so a resumed transcript can show WHY the turn failed; the
    /// live error card is ephemeral and the message itself is not a transcript
    /// message (it must never re-enter the model context).
    pub turn_errors: HashMap<RuntimeString, RuntimeString>,
    /// turn id -> normalized provider error kind (`network` / `rate_limit` /
    /// `overloaded` / …), present only for `failed` turns whose failure
    /// originated at the provider. Lets a resumed transcript localize the
    /// failure card instead of parsing the raw message.
    pub turn_error_kinds: HashMap<RuntimeString, RuntimeString>,
    /// Turn id -> complete EngineError diagnostic JSON, separate from model context.
    /// Older histories omit this optional map.
    pub turn_error_details: HashMap<RuntimeString, RuntimeJson>,
    /// turn id -> the tool_use ids whose calls the turn's ABORT killed, as opposed
    /// to tools that failed on their own merits. Both carry `is_error` for the
    /// model, so without this a reopened thread paints the user's stop as a red
    /// tool failure. Filled live from the tool lifecycle and replayed from the
    /// rollout's `cancelled` phase records.
    pub turn_cancelled_tools: HashMap<RuntimeString, Vec<RuntimeString>>,
}

impl crate::runtime_values::RuntimeEncode for TurnIndex {
    fn encode_runtime(
        &self,
        out: &mut crate::runtime_values::RuntimeEncoder,
    ) -> Result<(), RuntimeCodecError> {
        let mut object = crate::runtime_values::RuntimeObjectEncoder::new();
        object.field(
            "turnGoalMetadata",
            &self
                .turn_goal_metadata
                .iter()
                .map(|(key, value)| (key.clone(), value))
                .collect::<std::collections::BTreeMap<_, _>>(),
        )?;
        if !self.agent_notifications.is_empty() {
            object.field("agentNotifications", &self.agent_notifications)?;
        }
        object.field(
            "historyTurnIds",
            &self
                .message_turn
                .iter()
                .map(|(key, value)| (key.clone(), value))
                .collect::<std::collections::BTreeMap<_, _>>(),
        )?;
        object.field(
            "turnFinalMessageIds",
            &self
                .turn_final_message
                .iter()
                .map(|(key, value)| (key.clone(), value))
                .collect::<std::collections::BTreeMap<_, _>>(),
        )?;
        object.field(
            "turnStatuses",
            &self
                .turn_status
                .iter()
                .map(|(key, value)| (key.clone(), value))
                .collect::<std::collections::BTreeMap<_, _>>(),
        )?;
        object.field(
            "turnStartedAtMs",
            &self
                .turn_started_at_ms
                .iter()
                .map(|(key, value)| (key.clone(), value))
                .collect::<std::collections::BTreeMap<_, _>>(),
        )?;
        object.field(
            "turnFinishedAtMs",
            &self
                .turn_finished_at_ms
                .iter()
                .map(|(key, value)| (key.clone(), value))
                .collect::<std::collections::BTreeMap<_, _>>(),
        )?;
        object.field(
            "turnDurations",
            &self
                .turn_duration_ms
                .iter()
                .map(|(key, value)| (key.clone(), value))
                .collect::<std::collections::BTreeMap<_, _>>(),
        )?;
        object.field(
            "turnErrors",
            &self
                .turn_errors
                .iter()
                .map(|(key, value)| (key.clone(), value))
                .collect::<std::collections::BTreeMap<_, _>>(),
        )?;
        object.field(
            "turnErrorKinds",
            &self
                .turn_error_kinds
                .iter()
                .map(|(key, value)| (key.clone(), value))
                .collect::<std::collections::BTreeMap<_, _>>(),
        )?;
        let details = self.error_details_snapshot();
        if !details.is_empty() {
            object.field("turnErrorDetails", &details)?;
        }
        object.field(
            "turnCancelledTools",
            &self
                .turn_cancelled_tools
                .iter()
                .map(|(key, value)| (key.clone(), value))
                .collect::<std::collections::BTreeMap<_, _>>(),
        )?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl crate::runtime_values::RuntimeDecode for TurnIndex {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                turn_goal_metadata: object
                    .defaulted::<std::collections::BTreeMap<RuntimeString, TurnGoalMetadata>>(
                        "turnGoalMetadata",
                        &[],
                        Default::default,
                    )?
                    .into_iter()
                    .collect(),
                agent_notifications: object.defaulted("agentNotifications", &[], Vec::new)?,
                message_turn: object
                    .defaulted::<std::collections::BTreeMap<RuntimeString, RuntimeString>>(
                        "historyTurnIds",
                        &[],
                        Default::default,
                    )?
                    .into_iter()
                    .collect(),
                turn_final_message: object
                    .defaulted::<std::collections::BTreeMap<RuntimeString, RuntimeString>>(
                        "turnFinalMessageIds",
                        &[],
                        Default::default,
                    )?
                    .into_iter()
                    .collect(),
                turn_status: object
                    .defaulted::<std::collections::BTreeMap<RuntimeString, RuntimeString>>(
                        "turnStatuses",
                        &[],
                        Default::default,
                    )?
                    .into_iter()
                    .collect(),
                turn_started_at_ms: object
                    .defaulted::<std::collections::BTreeMap<RuntimeString, i64>>(
                        "turnStartedAtMs",
                        &[],
                        Default::default,
                    )?
                    .into_iter()
                    .collect(),
                turn_finished_at_ms: object
                    .defaulted::<std::collections::BTreeMap<RuntimeString, i64>>(
                        "turnFinishedAtMs",
                        &[],
                        Default::default,
                    )?
                    .into_iter()
                    .collect(),
                turn_duration_ms: object
                    .defaulted::<std::collections::BTreeMap<RuntimeString, u64>>(
                        "turnDurations",
                        &[],
                        Default::default,
                    )?
                    .into_iter()
                    .collect(),
                turn_errors: object
                    .defaulted::<std::collections::BTreeMap<RuntimeString, RuntimeString>>(
                        "turnErrors",
                        &[],
                        Default::default,
                    )?
                    .into_iter()
                    .collect(),
                turn_error_kinds: object
                    .defaulted::<std::collections::BTreeMap<RuntimeString, RuntimeString>>(
                        "turnErrorKinds",
                        &[],
                        Default::default,
                    )?
                    .into_iter()
                    .collect(),
                turn_error_details: object
                    .optional::<std::collections::BTreeMap<RuntimeString, RuntimeJson>>(
                        "turnErrorDetails",
                        &[],
                    )?
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|(id, value)| {
                        sanitize_turn_error_details(&value).map(|value| (id, value))
                    })
                    .collect(),
                turn_cancelled_tools: object
                    .defaulted::<std::collections::BTreeMap<RuntimeString, Vec<RuntimeString>>>(
                        "turnCancelledTools",
                        &[],
                        Default::default,
                    )?
                    .into_iter()
                    .collect(),
            })
        })
    }
}

/// What each turn of this session changed on disk.
///
/// This answers "what did this turn produce", which is NOT the question a
/// repository diff answers: the working tree also holds edits from before the
/// session and loses the ones already committed during it, and a session in a
/// plain directory has no repository to ask.
///
/// Grouped by turn because that is the scope a reader can act on: a turn's own
/// volume stops being meaningful once it is summed with the next turn's, and a
/// file edited in three turns has three separate answers, not one running total.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SessionFileChanges {
    /// Oldest turn first, so the list reads as the order the session worked in.
    pub turns: Vec<TurnFileChanges>,
}

/// One turn's files, first-touched first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnFileChanges {
    /// `None` only for records replayed from a rollout written before file
    /// changes carried their turn id, which cannot be attributed after the fact.
    pub turn_id: Option<RuntimeString>,
    pub files: Vec<SessionFileChange>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionFileChange {
    pub path: RuntimeString,
    pub kind: FileChangeKind,
    /// Lines this TURN added / removed in this file, measured once against what
    /// the turn first found there — not the sum of its edits, which double-counts
    /// a line each later edit touches again and can exceed the file's own length.
    ///
    /// `None` means the volume is unknown (no readable baseline, or no exact
    /// count was affordable). A host must render that as unknown, never as zero.
    pub added: Option<u64>,
    pub removed: Option<u64>,
    /// Newest exact baseline-to-content diff for this file within the turn.
    pub turn_diff: Option<RuntimeString>,
}

impl SessionFileChanges {
    /// Record what ONE reported edit leaves the file at, inside its turn.
    ///
    /// The counts REPLACE whatever the same turn recorded for this file before:
    /// each report already measures the whole turn's effect on it.
    ///
    /// The kind is the turn's verdict on the file, not the last tool call's:
    /// a file the turn created and then edited was still created by it, so
    /// `create` survives later `modify`s. A `delete` always wins because the file
    /// is gone; a later `create` at the same path takes it back.
    pub fn record(
        &mut self,
        turn_id: Option<&RuntimeString>,
        path: &RuntimeString,
        kind: FileChangeKind,
        added: Option<u64>,
        removed: Option<u64>,
    ) {
        let turn = match self
            .turns
            .iter_mut()
            .rev()
            .find(|turn| turn.turn_id.as_ref() == turn_id)
        {
            Some(turn) => turn,
            None => {
                self.turns.push(TurnFileChanges {
                    turn_id: turn_id.cloned(),
                    files: Vec::new(),
                });
                self.turns.last_mut().expect("just pushed")
            }
        };
        if let Some(existing) = turn.files.iter_mut().find(|file| &file.path == path) {
            existing.kind = match (existing.kind, kind) {
                (_, FileChangeKind::Delete) => FileChangeKind::Delete,
                (FileChangeKind::Create, FileChangeKind::Modify) => FileChangeKind::Create,
                (_, next) => next,
            };
            existing.added = added;
            existing.removed = removed;
            existing.turn_diff = None;
            return;
        }
        turn.files.push(SessionFileChange {
            path: path.clone(),
            kind,
            added,
            removed,
            turn_diff: None,
        });
    }

    /// Fold a `file_change` event's payload, where a tool that could not count
    /// lines contributes the edit itself with an unknown volume.
    pub fn record_change(&mut self, turn_id: Option<&RuntimeString>, change: &FileChange) {
        self.record(
            turn_id,
            &change.path,
            change.kind,
            change.added,
            change.removed,
        );
        let file = self
            .turns
            .iter_mut()
            .rev()
            .find(|turn| turn.turn_id.as_ref() == turn_id)
            .and_then(|turn| turn.files.iter_mut().find(|file| file.path == change.path))
            .expect("record inserted the file");
        file.turn_diff = change.turn_diff.clone();
    }
}

/// Every file edit carried by ONE engine event, including the ones a sub-agent
/// performed: a child's stream is forwarded on the spawning tool's progress
/// events, so a session that only reads its own top-level `file_change`s would
/// omit whatever it delegated.
pub(crate) fn file_changes_in_event(event: &EngineEvent) -> Vec<&FileChange> {
    let mut out = Vec::new();
    collect_file_changes(event, &mut out);
    out
}

fn collect_file_changes<'a>(event: &'a EngineEvent, out: &mut Vec<&'a FileChange>) {
    if let EngineEvent::ToolEvent { event, .. } = event {
        match event {
            ToolDomainEvent::FileChange { change } => out.push(change),
            ToolDomainEvent::AgentProgress { event, .. } => collect_file_changes(event, out),
            _ => {}
        }
    }
}

/// A captured live fork contains history, never a second live runtime or its
/// credentials. Only Session can construct it at the turn-admission boundary.
#[derive(Clone)]
pub struct LiveForkSnapshot {
    pub(crate) approval_context: crate::engine::approval_capture::ReviewParentCheckpoint,
    pub(crate) planning: crate::planning::types::PlanningSnapshot,
    pub(crate) retained_planning_turns: std::collections::HashSet<RuntimeString>,
    pub(crate) inherited_goal: Option<crate::goal::store::GoalForkSnapshot>,
    pub(crate) source_thread_id: RuntimeString,
    pub(crate) cwd: RuntimePath,
    pub(crate) history: Vec<Message>,
}

/// The latest completed parent turn captured for an ephemeral side task.
/// Only Session constructs this; hosts never submit inherited history.
#[derive(Clone)]
pub struct LiveSideTaskSnapshot {
    pub(crate) approval_context: crate::engine::approval_capture::ReviewParentCheckpoint,
    pub(crate) source_thread_id: RuntimeString,
    pub(crate) cwd: RuntimePath,
    pub(crate) history: Vec<Message>,
}

/// Owns the whole conversation + turn loop. Shared as `Arc<Session>`.
pub struct Session {
    pub id: RuntimeString,
    pub meta: SessionMeta,
    process_context: RuntimeProcessContext,
    background_processes: Arc<crate::command_runtime::background::BackgroundCommandRegistry>,
    inference: RwLock<Arc<InferenceBundle>>,
    inference_admission: Arc<Mutex<InferenceAdmission>>,
    inference_changed: tokio::sync::Notify,
    deps: SessionIdentityDeps,
    /// Session-scoped background compaction precompute state. Lives as long as
    /// the session; dropping it cancels every in-flight background summary.
    precompute: Arc<crate::engine::query::PrecomputeRegistry>,
    // Structural session configuration. Read model-dependent fields from the
    // current inference bundle; this initial model only identifies the cached
    // construction-time environment prompt that may need rebuilding.
    config: ResolvedConfig,
    tools: Vec<AnyTool>,
    host_refresh_tools: Option<Arc<dyn Fn() -> Vec<AnyTool> + Send + Sync>>,
    /// The session's working-tree cluster (see [`SessionWorkspaceState`]).
    workspace: SessionWorkspaceState,
    workflow_journal_log: Mutex<Vec<WorkflowJournalEntry>>,
    has_workflow_tool: bool,
    swarm_identity: Mutex<Option<SwarmIdentity>>,
    swarm_delivery_state: Arc<crate::tasks::swarm_store::MailboxDeliveryState>,
    /// A team successfully created by this Session and registered for the same
    /// session-end cleanup lifecycle as TeamCreate.
    created_swarm_team: Mutex<Option<RuntimeString>>,
    /// Linearizes task-list identity changes with query capture and change
    /// delivery so an old-team callback cannot overtake a new-team snapshot.
    task_list_identity_gate: Mutex<()>,
    task_list_identity_generation: AtomicU64,
    lsp_manager: Option<Arc<LspManager>>,
    lsp_configs: Vec<crate::lsp::types::LspServerConfig>,
    agent_memory_host: Option<Arc<dyn AgentMemoryHost>>,
    /// The session's Agent Memory snapshot-decision runtime cluster (see
    /// [`SessionSnapshotState`]).
    snapshot: SessionSnapshotState,
    runtime_capabilities: RuntimeCapabilityRegistry,
    selected_capability_roots: Option<Vec<SelectedRuntimeCapabilityRoot>>,
    /// The session's memory-root & nested-memory loading cluster (see
    /// [`SessionMemoryRoots`]).
    memory_roots: SessionMemoryRoots,
    /// Per-session override for the user's config home (`~/.klaus`). Used by
    /// tests to keep the session from reading the developer's real
    /// `AGENTS.md`.
    user_config_dir: Option<RuntimePath>,
    runtime_policy: RwLock<Arc<RuntimePolicySnapshot>>,
    required_network_sandbox: Option<Arc<dyn SandboxBackend>>,
    #[cfg(windows)]
    windows_network: Option<Arc<crate::network_proxy::SessionWindowsNetwork>>,
    file_state: Arc<InMemoryFileStateCache>,
    context_countdown_anchor:
        Arc<Mutex<crate::context::total_tokens_reminder::TaskCountdownAnchor>>,
    governor: ResourceGovernor,
    permission_engine: Arc<dyn PermissionEngine>,
    durable_permissions: Arc<crate::permission::durable::DurablePermissionLayer>,
    agent_registry: AgentRegistry,
    root_agent_definition: Option<AgentDefinition>,
    approval_handler: Option<Arc<dyn ApprovalHandler>>,
    approval_reviewer: Option<Arc<dyn ApprovalReviewer>>,
    approval_review: Arc<session_approval_review_part::ApprovalReviewRuntime>,
    approval_async: Arc<session_approval_async_part::ApprovalAsyncRuntime>,
    elicit_handler: Option<Arc<dyn ElicitHandler>>,
    async_questions: Arc<crate::engine::questions::AsyncQuestionRegistry>,
    mcp_elicitation_router: McpElicitationRouter,
    mcp_authorize_handler: Option<Arc<dyn McpAuthorizeHandler>>,
    mcp_oauth_redirect_uri: Option<String>,
    credential_store: Option<Arc<dyn CredentialStore>>,
    /// The session's permission-mode control cluster (see
    /// [`SessionModeControl`]).
    mode: SessionModeControl,
    pub(crate) planning: Arc<crate::planning::runtime::PlanningRuntime>,
    /// The session's live turn-loop runtime cluster (see [`SessionTurnState`]).
    turn: SessionTurnState,
    hook_bus: Option<Arc<HookBus>>,
    /// The session's Setup init/maintenance runtime cluster (see
    /// [`SessionSetupState`]).
    setup: SessionSetupState,
    /// Retained cross-process TodoV2 watchers. Unsubscribe closes publication;
    /// shutdown joins every worker before reporting completion.
    task_list_subscriptions: Mutex<TaskListSubscriptionRegistry>,
    /// The session's teardown-lifecycle cluster (see
    /// [`SessionLifecycleState`]).
    lifecycle: SessionLifecycleState,
    /// The session's durability cluster (see [`SessionPersistenceState`]).
    persistence: SessionPersistenceState,
    /// The session's MCP runtime cluster (see [`SessionMcpRuntime`]).
    mcp: SessionMcpRuntime,
    slash_commands: Vec<Command>,
    /// Late-bound: the per-turn reminder providers, whose closures capture the
    /// `Arc<Session>` so they read live state. Installed once, right after the
    /// `Arc<Session>` exists.
    reminder_providers: std::sync::OnceLock<Vec<Arc<dyn ReminderProvider>>>,
    skills_thunk: Option<Arc<dyn Fn() -> Vec<Skill> + Send + Sync>>,
    skill_gate: Option<Arc<ConditionalSkillGate>>,
    /// Late-bound (see [`Session::reminder_providers`]): the edited-files reminder
    /// bound to the canonical `file_state`.
    edited_files_reminder: std::sync::OnceLock<Option<Arc<dyn ReminderProvider>>>,
    system_prompt_provider: RwLock<SystemPromptProvider>,
    system_prompt_is_default: bool,
    workspace_instruction_cache_refresh: RwLock<Option<Arc<dyn Fn() + Send + Sync>>>,
    developer_instructions: Option<RuntimeString>,
    active_output_style: Option<ActiveOutputStyle>,
    extension_runtime: ExtensionRuntimeSnapshot,
    plugin_publication: Mutex<SessionPluginRuntime>,
    extension_failure_sink: Mutex<Option<crate::extensions::failure::ExtensionFailureSink>>,
    pub cost: Arc<Mutex<CostTracker>>,
    shell_env: Arc<tokio::sync::OnceCell<Option<RuntimeEnv>>>,
    observer: Option<Arc<Mutex<OtelObserver>>>,
    logger: Option<Arc<Logger>>,
    /// The session's conversation-history cluster (see
    /// [`SessionConversationState`]).
    conversation: SessionConversationState,
    /// Turn grouping served with `get_history` (see [`TurnIndex`]).
    pub(crate) turn_index: Mutex<TurnIndex>,
    /// Per-message tool-result budget replacement state (request view).
    /// Reconstructed from the rollout's records on resume; frozen ids keep a
    /// later pass from rewriting a prefix the provider already cached.
    pub(crate) tool_result_budget_state:
        Arc<Mutex<crate::context::tool_result_budget::ReplacementState>>,
    pub(crate) context_hint_state: Arc<crate::context::context_hints::ContextHintState>,
    /// What API-level context editing has cleared for this session so far (see
    /// [`crate::context::context_editing::ContextEditState`]). Fed from the
    /// provider's applied-edits reports; the running tool-use total anchors the
    /// next count-based clearing trigger so clears arrive in batches.
    pub(crate) context_edit_state: Arc<crate::context::context_editing::ContextEditState>,
    /// What this session changed on disk, served with `get_file_changes` (see
    /// [`SessionFileChanges`]). Folded live during runs and rebuilt from the
    /// rollout on resume, so reopening a thread still reports what it produced.
    pub(crate) file_changes: Mutex<SessionFileChanges>,
    /// The session's sub-agent runtime cluster (see [`SessionSubagentState`]).
    subagents: SessionSubagentState,
    /// The session's memory-maintenance runtime cluster (see
    /// [`SessionMaintenanceState`]).
    maintenance: SessionMaintenanceState,
    surfaced_tools: Arc<Mutex<HashSet<RuntimeString>>>,
    /// The session's auto-memory extraction + Dream scheduling runtime cluster
    /// (see [`SessionMemoryScheduler`]).
    memory_scheduler: SessionMemoryScheduler,
    /// Deterministic interleaving point for the two-Session stale-gate test.
    /// Production builds have no hook and therefore no extra synchronization.
    #[cfg(test)]
    dream_before_lock_hook: Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>,
    /// Detached background-agent run tasks; awaited (after abort) on dispose so
    /// each agent's transcript flush completes.
    background_tasks: Mutex<BackgroundRunRegistry>,
    /// The session-level cron scheduler state (scheduled prompts).
    cron: CronRuntime,
    /// Goal mode: the thread's goal runtime, present only when `config.goal` is
    /// on. `None` is what makes the goal tools refuse and the turn loop skip
    /// the continuation check entirely.
    goal: Option<Arc<crate::goal::runtime::GoalRuntime>>,
    /// Where goal changes are delivered out of band. A goal changes from inside
    /// a turn (the model) and from outside one (the budget, a failed turn), so a
    /// host that only saw op replies would show a stale goal for a whole run.
    goal_observer: Arc<GoalObserverSlot>,
    /// Startup transport prewarm: provider endpoint handshake pooled ahead of
    /// the first request, kept warm for the session's lifetime.
    transport_prewarm: Mutex<Option<(u64, crate::provider::prewarm::TransportPrewarm)>>,
    /// Set once the first turn has resolved the startup prewarm.
    transport_prewarm_consumed: AtomicBool,
    /// Atomically gates registration against shutdown and owns cancellation +
    /// join handles for automatic extraction and automatic/manual Dream forks.
    maintenance_tasks: Mutex<MaintenanceRegistry>,
    /// Covers the short pre-registration window (including manual Dream's OS
    /// lock acquisition) so shutdown cannot snapshot an empty task registry and
    /// ACK before the pending admission either registers or rolls back.
    maintenance_admission: Arc<MaintenanceAdmissionTracker>,
}

impl TurnIndex {
    pub fn error_details_snapshot(&self) -> std::collections::BTreeMap<RuntimeString, RuntimeJson> {
        self.turn_error_details
            .iter()
            .filter(|(id, _)| {
                self.turn_status
                    .get(*id)
                    .is_none_or(|status| status == "failed")
            })
            .filter_map(|(id, value)| {
                sanitize_turn_error_details(value).map(|value| (id.clone(), value))
            })
            .collect()
    }

    /// Overlay authoritative live metadata on a provider-free saved projection.
    /// Recovery infers interruption from missing terminal records; a live lease
    /// can override that inference without restarting a session or provider.
    pub fn merge_live(&mut self, live: Self) {
        self.turn_goal_metadata.extend(live.turn_goal_metadata);
        for receipt in live.agent_notifications {
            if let Some(existing) = self
                .agent_notifications
                .iter_mut()
                .find(|saved| saved.notification_id == receipt.notification_id)
            {
                *existing = receipt;
            } else {
                self.agent_notifications.push(receipt);
            }
        }
        self.message_turn.extend(live.message_turn);
        self.turn_final_message.extend(live.turn_final_message);
        self.turn_status.extend(live.turn_status);
        self.turn_started_at_ms.extend(live.turn_started_at_ms);
        self.turn_finished_at_ms.extend(live.turn_finished_at_ms);
        self.turn_duration_ms.extend(live.turn_duration_ms);
        self.turn_errors.extend(live.turn_errors);
        self.turn_error_kinds.extend(live.turn_error_kinds);
        self.turn_error_details.extend(live.turn_error_details);
        self.turn_cancelled_tools.extend(live.turn_cancelled_tools);
    }
}

impl Session {
    /// The session-scoped background compaction precompute registry.
    pub(crate) fn precompute_registry(&self) -> Arc<crate::engine::query::PrecomputeRegistry> {
        self.precompute.clone()
    }

    /// Snapshot of the turn index for `get_history` replies.
    pub fn turn_index_snapshot(&self) -> TurnIndex {
        // A rollout without a terminal lifecycle record is interrupted only
        // on recovery. An already-owned live lease is authoritative evidence
        // that this same turn is running during a read-only inspection.
        let active_ids = self
            .turn
            .active_stack
            .lock_recover()
            .iter()
            .filter_map(|turn| turn.turn_id.clone())
            .collect::<Vec<_>>();
        let mut index = self.turn_index.lock_recover().clone();
        index.turn_error_details = index.error_details_snapshot().into_iter().collect();
        for id in active_ids {
            // Terminal metadata may run ahead of notification delivery while
            // its lease is still unwinding. Preserve that explicit terminal.
            index
                .turn_status
                .entry(id)
                .or_insert_with(|| "running".into());
        }
        index
    }

    /// Snapshot of the session's file changes for `get_file_changes` replies.
    pub fn file_changes_snapshot(&self) -> SessionFileChanges {
        self.file_changes.lock_recover().clone()
    }

    /// Install the set rebuilt from a resumed rollout.
    ///
    /// Resume reconstruction happens in the store (it owns the item stream), and
    /// the session is handed to the caller only after this runs, so there is no
    /// window where a host could read an empty set for a thread that has history.
    pub fn restore_file_changes(&self, changes: SessionFileChanges) {
        *self.file_changes.lock_recover() = changes;
    }

    /// Reinstall the turn index a rollout replay rebuilt.
    ///
    /// Without this a reopened thread reports NO turn statuses, so every turn
    /// reads as a normal completed one: a turn the user stopped loses its stopped
    /// label and its elapsed time, becomes collapsible, and stops offering to
    /// continue. The rollout carries all of it (`turn_lifecycle` records status and
    /// durationMs); the only thing missing was handing it back to the session.
    pub fn restore_turn_index(&self, mut index: TurnIndex) {
        index.turn_error_details = index.error_details_snapshot().into_iter().collect();
        *self.turn_index.lock_recover() = index;
    }

    /// The in-memory mirror of a child whose sidechain never opened.
    ///
    /// A sub-agent whose per-agent rollout failed to open still ran and still
    /// produced messages; with no file to hold them this map is their only
    /// home. The transcript read path consults it before answering "empty"
    /// for a live child, so a missing disk transcript falls back to the
    /// in-memory mirror.
    pub fn mirrored_subagent_transcript(&self, agent_id: &str) -> Option<Vec<Message>> {
        let key = SubAgentMapKey::String(RuntimeString::from(agent_id));
        self.subagents.transcripts.lock_recover().get(&key).cloned()
    }

    /// Fold every file edit this event reports (sub-agent edits included) into
    /// the turn that produced them.
    pub(crate) fn record_event_file_changes(
        &self,
        turn_id: Option<&RuntimeString>,
        event: &EngineEvent,
    ) {
        let changes = file_changes_in_event(event);
        if changes.is_empty() {
            return;
        }
        let mut state = self.file_changes.lock_recover();
        for change in changes {
            state.record_change(turn_id, change);
        }
    }
}

#[derive(Clone)]
struct McpInstruction {
    name: RuntimeString,
    instructions: RuntimeString,
}

#[derive(Clone)]
pub(crate) struct ActiveOutputStyle {
    name: RuntimeString,
    body: RuntimeString,
}

pub(crate) enum SlashCommandOutcome {
    Prompt(Vec<ContentBlock>),
    Local(LocalRunOutcome),
}

pub(crate) struct SessionMemoryCompactAdapter {
    pub(crate) session: Weak<Session>,
}

#[async_trait::async_trait]
impl crate::context::compact::full_compact::SessionMemoryCompactProvider
    for SessionMemoryCompactAdapter
{
    async fn try_compact_with_session_memory(
        &self,
        messages: &[Message],
        config: &ResolvedConfig,
        instructions: Option<&RuntimeString>,
    ) -> Option<crate::context::compact::full_compact::CompactionResult> {
        let session = self.session.upgrade()?;
        session
            .try_session_memory_compaction(messages, config, instructions)
            .await
    }
}

// ---------------------------------------------------------------------------
// Tests: the turn loop (run / run_streamed / instrument) end to end.
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "session_run_tests.rs"]
mod turn_tests;
