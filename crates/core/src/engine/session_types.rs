// ---------------------------------------------------------------------------
// Public input / result types
// ---------------------------------------------------------------------------

use super::AnyTool;
use super::EngineEventStream;
use super::SystemPromptProvider;
use crate::browser::BrowserRuntime;
use crate::capabilities::RuntimeCapabilityRegistry;
use crate::capabilities::SelectedRuntimeCapabilityRoot;
use crate::engine::agent_definitions::AgentDefinition;
use crate::engine::deps::EngineDeps;
use crate::engine::subagent_lifecycle::SubagentLifecycleRecord;
use crate::extensions::commands::Command;
use crate::extensions::output_styles::OutputStyle;
use crate::extensions::runtime_snapshot::ExtensionRuntimeSnapshot;
use crate::extensions::skills::conditional_skills::ConditionalSkillGate;
use crate::extensions::skills::loader::Skill;
use crate::hooks::bus::HookBus;
use crate::hooks::setup::SetupLifecycleSnapshot;
use crate::lsp::types::LspServerConfig;
use crate::mcp::client::ManagedClient;
use crate::mcp::client::McpAuthorizeHandler;
use crate::mcp::credential_store::CredentialStore;
use crate::mcp::elicitation::McpElicitationPresenter;
use crate::mcp::types::McpServerConfig;
use crate::memory::agent_memory::AgentMemoryHost;
use crate::memory::maintenance::MemoryMaintenanceSnapshot;
use crate::memory::snapshot_decision::AgentMemorySnapshotDecisionAudit;
use crate::memory::snapshot_decision::AgentMemorySnapshotDecisionPolicy;
use crate::network_proxy::TrustedWindowsSandboxContext;
use crate::observability::cost_tracker::PricingTable;
use crate::observability::logger::Logger;
use crate::observability::otel::OtelObserver;
use crate::runtime_process::RuntimeEnv;
use crate::runtime_process::RuntimeProcessContext;
use crate::runtime_values::RuntimeJson;
use crate::runtime_values::RuntimePath;
use crate::runtime_values::RuntimeString;
use crate::schemas::config::ResolvedConfig;
use crate::schemas::effort::EffortValue;
use crate::schemas::message::Message;
use crate::schemas::message::Usage;
use crate::schemas::rollout::SessionRole;
use crate::schemas::rollout::UsageLedgerItem;
use crate::store::session_store::LiveRollout;
use crate::store::session_store::SessionStore;
/// A structured piece of a user turn: text or inline base64 media.
use crate::types::abort::AbortSignal;
use crate::types::errors::EngineError;
use crate::types::errors::TerminalReason;
use crate::types::events::ThreadItem;
use crate::types::json::Json;
use crate::types::json::JsonSchema;
use crate::types::permission::ApprovalHandler;
use crate::types::permission::ApprovalReviewer;
use crate::types::permission::ExternalPermissionMode;
use crate::types::provider::ModelProvider;
use crate::types::tool::BackgroundTaskStatus;
use crate::types::tool::ElicitHandler;
use crate::types::tool::WorkflowJournalEntry;
use crate::worktree::worktree::WorktreeSession;
use std::sync::Arc;
use std::sync::Mutex;
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserContentPart {
    Text {
        text: RuntimeString,
    },
    Image {
        base64: RuntimeString,
        mime_type: RuntimeString,
        /// Local provenance for display-only history projections; never sent
        /// to a provider. Mirrors the image block's `sourcePath`.
        source_path: Option<RuntimeString>,
    },
    Document {
        base64: RuntimeString,
        mime_type: RuntimeString,
        filename: Option<RuntimeString>,
    },
    Media {
        base64: RuntimeString,
        mime_type: RuntimeString,
        filename: Option<RuntimeString>,
    },
}

/// A user turn: a plain string or structured content parts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Input {
    Text(RuntimeString),
    Parts(Vec<UserContentPart>),
}

/// An input which has passed all structural, payload, and model checks. The
/// private fields prevent callers from bypassing validation before persistence.
#[derive(Debug)]
pub struct ValidatedInput {
    pub(crate) input: Input,
    pub(crate) pdf_bytes: usize,
    pub(crate) pdf_documents: usize,
    /// The input carries at least one image part (drives the vision gate).
    pub(crate) has_images: bool,
    pub(crate) model: RuntimeString,
    pub(crate) provider_revision: super::InputModelRevision,
}

pub(crate) const MAX_DIRECT_PDF_INPUT_BYTES: usize = crate::fs::pdf::PDF_TARGET_RAW_SIZE as usize;
pub(crate) const MAX_DIRECT_PDF_DOCUMENTS: usize = 32;
pub(crate) const MAX_ENCODED_PDF_INPUT_BYTES: usize =
    MAX_DIRECT_PDF_INPUT_BYTES.div_ceil(3) * 4 + (MAX_DIRECT_PDF_DOCUMENTS - 1) * 4;
pub(crate) const MAX_DOCUMENT_FILENAME_CHARS: usize = 255;
pub(crate) const MAX_DOCUMENT_FILENAME_BYTES: usize = 255;

/// Per-turn options threaded into `run` / `run_streamed`.
#[derive(Default, Clone)]
pub struct TurnOptions {
    /// Host abort signal for this turn; linked into the turn's own controller.
    pub signal: Option<AbortSignal>,
    pub model: Option<RuntimeString>,
    /// Per-turn reasoning effort. Like `model`, this overrides the session's own
    /// inference config for THIS turn only and leaves the stored config alone —
    /// a host delivering work into another conversation must not silently
    /// re-tune that conversation's reasoning level for its later turns.
    pub effort: Option<EffortValue>,
    /// Per-turn local orchestration override, with the same turn-only scope as
    /// `effort`. `None` reads the session config's `ultra_mode`.
    pub ultra_mode: Option<bool>,
    pub output_schema: Option<JsonSchema>,
    /// Retired input, rejected at turn admission. Use the session runtime policy.
    pub permission_mode_override: Option<ExternalPermissionMode>,
    pub approval_handler: Option<Arc<dyn ApprovalHandler>>,
    pub approval_reviewer: Option<Arc<dyn ApprovalReviewer>>,
    pub system_prompt_append: Option<RuntimeString>,
    /// Soft OUTPUT-token target for this run (a minimum to fill, not a cap).
    /// Ignored when `output_schema` is set.
    pub token_budget: Option<u64>,
    /// Per-turn capability-root selection. When set, this overrides
    /// the session's default selected roots for host/plugin runtimes.
    pub selected_capability_roots: Option<Vec<SelectedRuntimeCapabilityRoot>>,
    /// Host-owned rendering metadata persisted with this turn's user message.
    /// It is never included in provider requests.
    pub display_metadata: Option<RuntimeJson>,
    /// Mark this turn's input as META, so it is not rendered as something the
    /// user typed. Engine-initiated turns (a goal continuation) set this: the
    /// text is an instruction the engine wrote to itself.
    pub meta_input: bool,
    /// Internal admission owner: queued foreground work outranks automatic goals.
    pub goal_pending_input: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
    /// Internal receipt invoked only after the provider factory accepts this request.
    pub on_provider_request_admitted: Option<crate::engine::query::OnProviderRequestAdmitted>,
}

/// A live turn: an async stream of engine events.
pub struct Turn {
    pub events: EngineEventStream,
}

/// The aggregated outcome of a fully-consumed turn.
pub struct TurnResult {
    pub items: Vec<ThreadItem>,
    pub final_text: RuntimeString,
    pub usage: Option<Usage>,
    pub stop_reason: TerminalReason,
    pub error: Option<EngineError>,
    /// The parsed structured result — present only when the turn ran with
    /// `output_schema` and the final text was valid JSON.
    pub structured_output: Option<Json>,
}

/// A read-only view of one background sub-agent (a `run_in_background` task or a
/// persistent swarm teammate), for a host progress panel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackgroundAgentInfo {
    pub id: String,
    pub description: String,
    pub status: BackgroundTaskStatus,
    pub teammate: bool,
    pub idle: bool,
    pub transcript: String,
    pub output_file: String,
}

/// How often a parked persistent teammate polls its mailbox for the next message.
pub(crate) const TEAMMATE_IDLE_POLL_MS: u64 = 200;

/// The restricted tool set the auto-memory forks (extraction + consolidation) may use.
pub(crate) fn memory_fork_tool_names() -> &'static [&'static str] {
    &["read", "grep", "glob", "write", "edit"]
}

/// Which of the restricted fork tools MUTATE the memory directory. A failed
/// mutation and a failed probe (read/grep/glob) mean different things to a
/// fork's verdict; see `run_restricted_memory_fork`.
pub(crate) fn memory_fork_tool_mutates(name: &str) -> bool {
    matches!(name, "write" | "edit")
}

/// Background memory maintenance can legitimately wait on a slow provider much
/// longer than an interactive stream without being abandoned.
pub(crate) const MEMORY_FORK_STREAM_IDLE_TIMEOUT_MS: u64 = 10 * 60 * 1000;

/// The file-stat gate is re-evaluated at most once per this interval.
pub(crate) const DREAM_SCAN_INTERVAL_MS: i64 = 10 * 60 * 1000;
pub(crate) const MAINTENANCE_SHUTDOWN_ERROR: &str =
    "memory maintenance shutdown exceeded the 60000ms grace period";
pub(crate) const MAINTENANCE_FORCE_ABORT_JOIN_MS: u64 = 5_000;
pub(crate) const MAINTENANCE_FORCE_ABORT_ERROR: &str =
    "memory maintenance force-abort did not join within 5000ms";
pub(crate) const MANUAL_DREAM_SHUTDOWN_ERROR: &str = "memory consolidation stopped during shutdown";
pub(crate) const SESSION_SHUTDOWN_GRACE_MS: u64 = 60_000;
pub(crate) const SESSION_SHUTDOWN_FORCE_MS: u64 =
    SESSION_SHUTDOWN_GRACE_MS + MAINTENANCE_FORCE_ABORT_JOIN_MS;
pub(crate) const SESSION_SHUTDOWN_CLEANUP_MS: u64 = 70_000;
pub(crate) const SESSION_SHUTDOWN_FLUSH_MS: u64 = 72_000;
pub(crate) const SESSION_SHUTDOWN_PERSISTENCE_MS: u64 = 75_000;

// ---------------------------------------------------------------------------
// SessionInit
// ---------------------------------------------------------------------------

/// Provider-bound requirements admitted before synchronous inference publication.
/// Private fields prevent a caller from attaching one account's policy to another transport.
#[derive(Clone)]
pub struct PreparedReviewRequirements {
    pub(crate) provider: Arc<dyn ModelProvider>,
    pub(crate) cloud:
        Option<Arc<crate::engine::approval_requirements_cloud::CloudManagedRequirementsProvider>>,
    pub(crate) requirements: Arc<crate::engine::approval_requirements::ManagedReviewRequirements>,
}

/// Publication ownership and prepared targets must reach the commit check
/// together. This in-process input is never an outward diagnostic snapshot.
///
/// Tier targets use the shared [`InferenceTargets`] shape: a publication swaps
/// the main provider and every auxiliary target in one atomic step, so an
/// in-flight request can never observe a main/tier pairing that was never
/// published together.
pub struct InferencePublication {
    pub expected_revision: u64,
    pub change_id: Option<String>,
    pub targets: Option<super::InferenceTargets>,
    pub advisor_target: Option<super::InferenceTargetState>,
    pub orchestrator_target: Option<super::InferenceTargetState>,
    pub orchestration_mode: bool,
    /// The complete desired per-feature target state; a default value turns
    /// every feature slot back onto the session's main model.
    pub feature_targets: super::FeatureTargets,
    pub source: Option<(String, String)>,
    pub rates: Option<PricingTable>,
    pub publication_key: Option<String>,
    pub review_requirements: Option<PreparedReviewRequirements>,
}

/// Everything needed to construct a [`Session`]. Optional fields default to
/// "feature off" / host-not-wired.
pub struct SessionInit {
    pub process_context: RuntimeProcessContext,
    pub id: Option<RuntimeString>,
    /// Override for the user's config home. `None` uses `~/.klaus`.
    pub user_config_dir: Option<RuntimePath>,
    pub provider: Arc<dyn ModelProvider>,
    pub deps: EngineDeps,
    pub config: ResolvedConfig,
    pub network_allowed: Option<bool>,
    /// Engine-owned persistent rules, independent of config and session grants.
    pub durable_permissions: Option<Arc<crate::permission::durable::DurablePermissionLayer>>,
    /// Host-authenticated Windows broker route. This never comes from public
    /// engine configuration or the command environment.
    pub trusted_windows_sandbox_context: Option<TrustedWindowsSandboxContext>,
    /// Host's explicit context-window pin (overrides per-model capabilities).
    pub context_window_override: Option<u64>,
    /// Host-injected cost table for this session's CostTracker.
    pub pricing: Option<PricingTable>,
    pub tools: Vec<AnyTool>,
    /// Host callback to recompute the live tool set between turns.
    pub refresh_tools: Option<Arc<dyn Fn() -> Vec<AnyTool> + Send + Sync>>,
    pub cwd: RuntimePath,
    pub approval_handler: Option<Arc<dyn ApprovalHandler>>,
    pub approval_reviewer: Option<Arc<dyn ApprovalReviewer>>,
    pub elicit_handler: Option<Arc<dyn ElicitHandler>>,
    pub mcp_elicitation_presenter: Option<Arc<dyn McpElicitationPresenter>>,
    pub mcp_authorize_handler: Option<Arc<dyn McpAuthorizeHandler>>,
    pub mcp_oauth_redirect_uri: Option<String>,
    pub credential_store: Option<Arc<dyn CredentialStore>>,
    pub hook_bus: Option<Arc<HookBus>>,
    pub rollout: Option<Arc<dyn LiveRollout>>,
    pub store: Option<Arc<dyn SessionStore>>,
    /// Host-connected MCP clients exposed to `list_mcp_resources`/`read_mcp_resource`.
    pub mcp_clients: Vec<Arc<ManagedClient>>,
    /// MCP servers declared by loaded plugins; the session connects them lazily
    /// before the first model turn that needs tool specs.
    pub plugin_mcp_servers: Vec<McpServerConfig>,
    /// Slash commands already loaded by the host/plugin layer.
    pub slash_commands: Vec<Command>,
    pub initial_history: Option<Vec<Message>>,
    /// Original bounded review evidence, independent of the compacted model window.
    pub initial_approval_context: Option<crate::engine::approval_capture::ReviewParentCheckpoint>,
    /// Latest lifecycle state for surviving review records on resume.
    pub initial_approval_reviews: Option<Vec<crate::types::approval_review::ApprovalReviewEvent>>,
    /// Persisted per-message tool-result budget replacement records (tool_use_id
    /// → the exact body the model saw). Replayed on resume so the request-view
    /// budget re-applies the same bodies byte-identically instead of re-deciding.
    pub initial_tool_result_budget_records: Option<Vec<(RuntimeString, RuntimeString)>>,
    pub hidden_context_message_ids: Option<Vec<RuntimeString>>,
    pub initial_usage_ledger: Option<Vec<UsageLedgerItem>>,
    pub initial_subagent_lifecycle: Option<Vec<SubagentLifecycleRecord>>,
    pub initial_background_processes:
        Option<Vec<crate::schemas::background_process::BackgroundProcessRecord>>,
    /// Resume-reconstructed automatic memory-maintenance audit state.
    pub initial_memory_maintenance: Option<MemoryMaintenanceSnapshot>,
    /// Resume-reconstructed Setup init/maintenance lifecycle state.
    pub initial_setup_lifecycle: Option<SetupLifecycleSnapshot>,
    /// Resume-reconstructed Agent Memory snapshot decision audit state.
    pub initial_agent_memory_snapshot_decisions: Option<Vec<AgentMemorySnapshotDecisionAudit>>,
    pub session_role: Option<SessionRole>,
    pub parent_thread_id: Option<RuntimeString>,
    /// Thread id this session was forked from, persisted in the session header.
    pub forked_from_id: Option<RuntimeString>,
    pub initial_checkpoints: Option<Vec<Option<String>>>,
    pub system_prompt_provider: Option<SystemPromptProvider>,
    pub base_instructions: Option<RuntimeString>,
    pub developer_instructions: Option<RuntimeString>,
    /// `None` ⇒ auto-wire a fresh (no-op) observer; `Some(None)` ⇒ disable.
    pub observer: Option<Option<Arc<Mutex<OtelObserver>>>>,
    pub logger: Option<Arc<Logger>>,
    pub agents: Option<Vec<AgentDefinition>>,
    pub root_agent_definition: Option<AgentDefinition>,
    pub skills: Option<Arc<dyn Fn() -> Vec<Skill> + Send + Sync>>,
    pub output_styles: Option<Arc<dyn Fn() -> Vec<OutputStyle> + Send + Sync>>,
    pub skill_gate: Option<Arc<ConditionalSkillGate>>,
    pub current_worktree: Option<WorktreeSession>,
    pub prior_workflow_journal: Option<Vec<WorkflowJournalEntry>>,
    pub initial_surfaced_tools: Option<Vec<RuntimeString>>,
    pub swarm: Option<SwarmInit>,
    pub lsp_servers: Option<Arc<dyn Fn() -> Vec<LspServerConfig> + Send + Sync>>,
    pub shell_env: Option<RuntimeEnv>,
    pub agent_memory: Option<Arc<dyn AgentMemoryHost>>,
    /// Optional protocol/Host override for first-use and unavailable-handler
    /// snapshot decisions. When absent, the AgentMemoryHost policy is used.
    pub agent_memory_snapshot_policy: Option<AgentMemorySnapshotDecisionPolicy>,
    pub browser_runtime: Option<Arc<dyn BrowserRuntime>>,
    pub runtime_capabilities: RuntimeCapabilityRegistry,
    pub selected_capability_roots: Option<Vec<SelectedRuntimeCapabilityRoot>>,
    /// Core-owned effective extension graph for diagnostics consumers.
    pub extension_runtime: ExtensionRuntimeSnapshot,
}

#[derive(Debug, thiserror::Error)]
pub enum SessionInitError {
    #[error("session persistence initialization failed: {source}")]
    Persistence { source: std::io::Error },
    #[error("administrator approval requirements failed: {message}")]
    ApprovalRequirements { message: String },
    #[error("planning state failed: {message}")]
    PlanningState { message: String },
    #[error("page extraction source selection failed: {message}")]
    PageExtraction { message: String },
    #[error("root agent selection failed: {message}")]
    AgentDefinition { message: String },
    #[error("sandbox selection failed: {message}")]
    SandboxSelection { message: String },
    #[error("trusted Windows broker context rejected: {message}")]
    TrustedWindowsContext { message: String },
    #[error("managed network proxy initialization failed: {source}")]
    NetworkProxy {
        #[source]
        source: std::io::Error,
    },
    #[error("managed network sandbox initialization failed: {source}")]
    ManagedNetworkSandbox {
        #[source]
        source: std::io::Error,
    },
}

impl SessionInitError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Persistence { .. } => "session_persistence_initialization_failed",
            Self::ApprovalRequirements { .. } => "approval_requirements_failed",
            Self::PlanningState { .. } => "planning_state_failed",
            Self::PageExtraction { .. } => "page_extraction_source_rejected",
            Self::AgentDefinition { .. } => "agent_definition_rejected",
            Self::SandboxSelection { .. } => "sandbox_selection_failed",
            Self::TrustedWindowsContext { .. } => "windows_broker_context_rejected",
            Self::NetworkProxy { .. } => "network_proxy_initialization_failed",
            Self::ManagedNetworkSandbox { .. } => "managed_network_sandbox_failed",
        }
    }

    pub fn into_io_error(self) -> std::io::Error {
        let kind = match &self {
            Self::Persistence { source } => source.kind(),
            Self::ApprovalRequirements { .. } => std::io::ErrorKind::PermissionDenied,
            Self::PlanningState { .. } => std::io::ErrorKind::InvalidData,
            Self::PageExtraction { .. } => std::io::ErrorKind::InvalidInput,
            Self::AgentDefinition { .. } => std::io::ErrorKind::InvalidInput,
            Self::SandboxSelection { .. } => std::io::ErrorKind::Unsupported,
            Self::TrustedWindowsContext { .. } => std::io::ErrorKind::InvalidInput,
            Self::NetworkProxy { .. } | Self::ManagedNetworkSandbox { .. } => {
                std::io::ErrorKind::Other
            }
        };
        std::io::Error::new(kind, self)
    }
}

/// This session's swarm identity seed (team + optional peer name).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwarmInit {
    pub team: RuntimeString,
    pub peer: Option<RuntimeString>,
}
