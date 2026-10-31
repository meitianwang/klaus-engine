//! The server-side adapter: reads newline-delimited [`Op`]s and drives a core
//! [`Session`], projecting every engine event back out as a newline-delimited
//! [`ProtocolEvent`].
//!
//! Approval flow: when the engine calls our approval handler, the engine
//! separately emits an `approval.requested` event on the stream; here we park a
//! resolver keyed by the request id. The matching `approve` op resolves it,
//! unblocking the engine. ask_user has no event of its own — it surfaces as an
//! `elicitation` message answered by a `resolve_elicitation` op.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex, Weak};

use async_trait::async_trait;
use futures::{FutureExt, StreamExt};
#[path = "server_turn_submission.rs"]
mod turn_submission;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot, Mutex as AsyncMutex, Notify, OnceCell};
use turn_submission::{BusySubmissionGuard, NonSteerableReservation};
pub use turn_submission::{
    TurnSubmission, TurnSubmissionKind, TurnSubmissionMode, TurnSubmissionOutcome,
    TurnSubmissionRejection,
};

use klaus_agent_core::browser::BrowserHostToolMapping;
use klaus_agent_core::capabilities::{RuntimeCapabilityRoot, SelectedRuntimeCapabilityRoot};
use klaus_agent_core::credential_store::{
    is_valid_model_provider_id, model_credential_key, CredentialStore, NamespacedCredentialStore,
};
use klaus_agent_core::engine::agent_definitions::AgentDefinition;
use klaus_agent_core::engine::rewind::{
    BoxError, RewindController, RewindOutcome, RewindPreview, RewindScope, RewindSession,
    RollbackResult,
};
use klaus_agent_core::engine::session::{
    resolve_config, DreamNowCompletion, EnqueueOptions, Input, Session, SessionShutdownOutcome,
    TurnOptions,
};
use klaus_agent_core::extensions::output_styles::OutputStyle;
use klaus_agent_core::extensions::plugins::installed::{InstalledPluginRecord, InstalledPlugins};
use klaus_agent_core::extensions::plugins::runtime::PluginRuntimeSnapshot;
use klaus_agent_core::extensions::skills::loader::Skill;
use klaus_agent_core::hooks::types::{Hook, HookHandler};
use klaus_agent_core::mcp::client::{
    McpAuthorizeHandler, McpAuthorizeRequest, McpAuthorizeResponse,
};
use klaus_agent_core::mcp::elicitation::{
    McpElicitationAction, McpElicitationEvent, McpElicitationPresenter,
};
use klaus_agent_core::mcp::types::{McpOAuthOption, McpServerConfig, McpServerOptions};
use klaus_agent_core::observability::cost_tracker::PricingTable;
use klaus_agent_core::permission::durable::DurablePermissionLayer;
use klaus_agent_core::permission::rule_store::DurableRules;
use klaus_agent_core::provider::anthropic::{AnthropicProvider, AnthropicProviderOptions};
use klaus_agent_core::provider::auth::{
    validate_provider_extra_headers, ApiKeyScheme, AuthSource, StaticBearerAuth,
};
use klaus_agent_core::provider::codex_subscription::codex_oauth_defaults;
use klaus_agent_core::provider::gemini::{
    CodeAssistOptions, GeminiProvider, GeminiProviderOptions, GeminiThinkingDialect,
};
use klaus_agent_core::provider::gemini_subscription::gemini_oauth_defaults;
use klaus_agent_core::provider::oauth::{OAuthAuth, OAuthAuthOptions, OAuthConfig, OAuthTokens};
use klaus_agent_core::provider::openai::{
    OpenAICompatibilityOptions, OpenAIInstructionRole, OpenAIProvider, OpenAIProviderOptions,
    OpenAIRequestIdentity, OpenAiApi, OpenAiReasoningDialect,
};
use klaus_agent_core::runtime_values::{
    runtime_codec, runtime_json, try_to_scalar_json, RuntimeJson, RuntimeObject, RuntimeString,
};
use klaus_agent_core::schemas::content::{ContentBlock, ImageBlock, OutputBlock, TextBlock};
use klaus_agent_core::schemas::effort::EffortLevel;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::schemas::subagent_message::{message_preview, validate_text_message};
#[cfg(test)]
use klaus_agent_core::store::session_store::ResumedLiveRollout;
use klaus_agent_core::store::session_store::{
    ListOptions, SubAgentReadSnapshot, SubAgentTranscript, ThreadPage, ThreadReadSnapshot,
    ThreadSummary,
};
use klaus_agent_core::store::session_store::{UnarchiveSessionResult, UnarchiveSessionStatus};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{
    EngineError, EngineErrorCode, InputValidationError, InputValidationErrorCode, TerminalReason,
    ToolError,
};
use klaus_agent_core::types::events::{CompactionMode, EngineEvent, NotificationLevel, ThreadItem};
use klaus_agent_core::types::json::Json;
use klaus_agent_core::types::permission::{
    ApprovalHandler, ApprovalRequest, ApprovalResponse, ExternalPermissionMode,
};
use klaus_agent_core::types::provider::ModelProvider;
use klaus_agent_core::types::provider::{EffortCapability, ModelCapabilities, ThinkingCapability};
use klaus_agent_core::types::tool::{
    ElicitHandler, ElicitRequest, ElicitResponse, SubagentMessageRequest, Tool, ToolContext,
    ToolExposure, ToolProgress, ToolResult, ToolResultSerializable,
};

use crate::activity::{AbortSlot, ActivityGuard, ActivityTracker};

use crate::new_uuid;
use crate::types::{
    AgentMemorySnapshotDecisionQueryResult, AgentMemorySnapshotUnavailableReason,
    ArchiveSessionResult, BackgroundAgentSummary, BoundSession, CompactProgress,
    CompactProgressPhase, CompactSessionResult, CompactTrigger, DeleteSessionResult, Elicitation,
    GenerateTitleOpResult, InstalledPluginMutationResult, McpAuthorizationRequestWire,
    McpAuthorizationResolvedWire, McpAuthorizationResponseWire, McpAuthorizationRevokedWire,
    McpAuthorizationStatusWire, McpElicitationResolvedWire, McpPromptResultWire,
    McpPromptsResultWire, McpResourceReadResultWire, McpRuntimeResult, McpRuntimeScope,
    McpRuntimeSnapshot, McpServerPromptsWire, McpToolCallResultWire, ModelCapabilitiesInit, Op,
    OpKind, OpenAiCompatibilityInit, PendingResumeResult, PluginActivationScope,
    ProtocolCommandHook, ProtocolDynamicToolCall, ProtocolDynamicToolCallOutputContentItem,
    ProtocolDynamicToolFunctionSpec, ProtocolDynamicToolNamespaceTool, ProtocolDynamicToolResolved,
    ProtocolDynamicToolResponse, ProtocolDynamicToolSpec, ProtocolErrorDetail, ProtocolEvent,
    ProtocolHostCapabilitiesInit, ProtocolMcpOAuthOption, ProtocolMcpServerInit,
    ProtocolPluginCatalog, ProtocolPluginCatalogRoot, ProtocolRuntimeCapabilityRootInit,
    ProtocolSelectedCapabilityRootInit, ProtocolSessionConfigUpdate,
    ProtocolSessionConfigUpdateResult, ProtocolSessionFileChange, ProtocolSessionInit,
    ProtocolSessionTurnFileChanges, ProtocolThreadGoal, ProviderAuthInit, ProviderInit,
    ProviderOAuthInit, RenameThreadResult, RunOpOptions, RunResult, ShutdownCompletion,
    SteerResult, UpdateThreadHostMetadataResult, UpdateTitleResult,
};

/// A boxed future produced by a chained op.
type ChainFuture = Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send>>;

/// Convert a `RuntimeString` error (the MCP subsystem's error type) into a
/// `BoxError` that `report_op_error` expects.
fn mcp_err(error: RuntimeString) -> BoxError {
    let lossy = error
        .try_to_utf8()
        .unwrap_or_else(|_| "MCP error (non-UTF-8)".to_string());
    lossy.into()
}
/// A boxed future produced by `create_session`.
pub type CreateSessionFuture = Pin<Box<dyn Future<Output = Result<Arc<Session>, BoxError>> + Send>>;
pub type ListSessionsFuture = Pin<Box<dyn Future<Output = Result<ThreadPage, BoxError>> + Send>>;
pub type ArchiveSessionFuture =
    Pin<Box<dyn Future<Output = Result<ArchiveSessionResult, BoxError>> + Send>>;
pub type UnarchiveSessionFuture = Pin<Box<dyn Future<Output = UnarchiveSessionResult> + Send>>;
pub type DeleteSessionFuture =
    Pin<Box<dyn Future<Output = Result<DeleteSessionResult, BoxError>> + Send>>;
/// Facade persistence runs after reader admission and before final cancellation.
pub type InterruptCommit = Box<dyn FnOnce() -> Result<(), String> + Send>;
pub type InterruptPrepareFuture =
    Pin<Box<dyn Future<Output = Result<InterruptCommit, String>> + Send>>;
pub type InterruptPreparation = Box<dyn FnOnce() -> InterruptPrepareFuture + Send>;
pub type UpdateTitleFuture =
    Pin<Box<dyn Future<Output = Result<UpdateTitleResult, BoxError>> + Send>>;
pub type RenameThreadFuture =
    Pin<Box<dyn Future<Output = Result<RenameThreadResult, BoxError>> + Send>>;
pub type UpdateThreadHostMetadataFuture =
    Pin<Box<dyn Future<Output = Result<UpdateThreadHostMetadataResult, BoxError>> + Send>>;
pub type LoadSubagentTranscriptFuture =
    Pin<Box<dyn Future<Output = Result<SubAgentTranscript, BoxError>> + Send>>;

const MAX_PROTOCOL_LINE_BYTES: usize = 32 * 1024 * 1024;
// Five 10 MiB attachments expand to about 67 MiB in standard base64.
// Only side_question may use this envelope; ordinary ops keep their old cap.
const MAX_SIDE_QUESTION_LINE_BYTES: usize = 72 * 1024 * 1024;
const MAX_PENDING_PROTOCOL_LINES: usize = 64;
const MAX_PENDING_PROTOCOL_BYTES: usize = 4 * 1024 * 1024;
const PROTOCOL_SHUTDOWN_GRACE_MS: u64 = 75_000;
const PROTOCOL_SHUTDOWN_FORCE_MS: u64 = 80_000;
const PROTOCOL_SHUTDOWN_TRANSPORT_MS: u64 = 85_000;
const PROTOCOL_SHUTDOWN_CLOSE_MS: u64 = 90_000;

#[derive(Clone, Copy)]
struct ProtocolShutdownTimings {
    grace: std::time::Duration,
    force: std::time::Duration,
    transport: std::time::Duration,
    close: std::time::Duration,
}

impl Default for ProtocolShutdownTimings {
    fn default() -> Self {
        Self {
            grace: std::time::Duration::from_millis(PROTOCOL_SHUTDOWN_GRACE_MS),
            force: std::time::Duration::from_millis(PROTOCOL_SHUTDOWN_FORCE_MS),
            transport: std::time::Duration::from_millis(PROTOCOL_SHUTDOWN_TRANSPORT_MS),
            close: std::time::Duration::from_millis(PROTOCOL_SHUTDOWN_CLOSE_MS),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProtocolShutdownFailure {
    message: String,
    timed_out: bool,
}

impl ProtocolShutdownFailure {
    fn error(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            timed_out: false,
        }
    }

    pub(crate) fn timeout(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            timed_out: true,
        }
    }
}

#[derive(Debug, Default)]
struct ProtocolShutdownFailures {
    errors: Vec<String>,
    timed_out: bool,
}

impl ProtocolShutdownFailures {
    fn push_failure(&mut self, failure: ProtocolShutdownFailure) {
        self.timed_out |= failure.timed_out;
        self.errors.push(failure.message);
    }

    fn push_error(&mut self, error: impl Into<String>) {
        self.errors.push(error.into());
    }

    fn push_timeout(&mut self, error: impl Into<String>) {
        self.timed_out = true;
        self.errors.push(error.into());
    }

    fn extend_core(&mut self, outcome: SessionShutdownOutcome) {
        self.timed_out |= outcome.timed_out;
        self.errors.extend(outcome.errors);
    }
}

#[derive(Debug, PartialEq, Eq)]
enum ProtocolLine {
    Line(String),
    TooLarge,
    Eof,
}

struct AdmittedOp {
    op: Op,
    run_signal: Option<AbortSignal>,
    foreground_guard: Option<CurrentSubmissionGuard>,
    busy_guard: Option<BusySubmissionGuard>,
    interrupt: Option<Result<InterruptAdmission, String>>,
}

struct InterruptAdmission {
    foreground: Option<(uuid::Uuid, String, AbortSignal)>,
    activation_epoch: u64,
    stop_lease: Option<PendingGoalStop>,
    session: Option<Arc<Session>>,
    goal_stop: Option<(
        Arc<klaus_agent_core::goal::runtime::GoalRuntime>,
        klaus_agent_core::goal::runtime::GoalStopCapture,
    )>,
}

#[derive(Default)]
struct PendingDispatches {
    ops: VecDeque<(AdmittedOp, usize)>,
    bytes: usize,
}

impl PendingDispatches {
    fn push(&mut self, op: AdmittedOp, bytes: usize) -> Option<AdmittedOp> {
        if self.ops.len() >= MAX_PENDING_PROTOCOL_LINES
            || bytes > MAX_PENDING_PROTOCOL_BYTES.saturating_sub(self.bytes)
        {
            return Some(op);
        }
        self.bytes += bytes;
        self.ops.push_back((op, bytes));
        None
    }

    fn pop(&mut self) -> Option<AdmittedOp> {
        let (op, bytes) = self.ops.pop_front()?;
        self.bytes -= bytes;
        Some(op)
    }
}

// Goal controls retain the existing single-frame allowance. Urgent controls
// have separate capacity so goal payloads cannot prevent Stop or authorization.
const MAX_ACTIVE_GOAL_CONTROLS: usize = 4;
const MAX_QUEUED_GOAL_CONTROLS: usize = 16;
const MAX_GOAL_CONTROL_BYTES: usize = MAX_PROTOCOL_LINE_BYTES;
const MAX_ACTIVE_URGENT_CONTROLS: usize = 4;
const MAX_QUEUED_URGENT_CONTROLS: usize = 16;
const MAX_URGENT_CONTROL_BYTES: usize = 256 * 1024;

#[derive(Clone, Copy)]
enum ControlLaneKind {
    Goal,
    Urgent,
}

fn control_lane(op: &OpKind) -> Option<ControlLaneKind> {
    match op {
        OpKind::GetGoal | OpKind::SetGoal { .. } | OpKind::ClearGoal { .. } => {
            Some(ControlLaneKind::Goal)
        }
        OpKind::ResolveMcpAuthorization { .. } | OpKind::Interrupt { .. } | OpKind::Shutdown => {
            Some(ControlLaneKind::Urgent)
        }
        _ => None,
    }
}

struct ControlDispatches {
    active: futures::stream::FuturesUnordered<Pin<Box<dyn Future<Output = usize> + Send>>>,
    pending: VecDeque<(AdmittedOp, usize)>,
    bytes: usize,
    max_active: usize,
    max_pending: usize,
    max_bytes: usize,
}

enum ControlAdmission {
    Accepted,
    Full(Box<AdmittedOp>),
    Closed,
}

impl ControlDispatches {
    fn new(max_active: usize, max_pending: usize, max_bytes: usize) -> Self {
        Self {
            active: futures::stream::FuturesUnordered::new(),
            pending: VecDeque::new(),
            bytes: 0,
            max_active,
            max_pending,
            max_bytes,
        }
    }

    fn reap_finished(&mut self) {
        while let Some(Some(bytes)) = self.active.next().now_or_never() {
            self.bytes -= bytes;
        }
    }

    async fn wait_completion(&mut self) {
        if self.active.is_empty() {
            std::future::pending::<()>().await;
        } else if let Some(bytes) = self.active.next().await {
            self.bytes -= bytes;
        }
    }

    fn track(&mut self, task: tokio::task::JoinHandle<()>, bytes: usize) {
        self.active.push(Box::pin(async move {
            let _ = task.await;
            bytes
        }));
    }

    fn start_queued(&mut self, inner: &Arc<ServerInner>) -> bool {
        while self.active.len() < self.max_active {
            let Some((op, bytes)) = self.pending.pop_front() else {
                break;
            };
            let Some(task) = inner.spawn_dispatch(op) else {
                self.bytes -= bytes;
                return false;
            };
            self.track(task, bytes);
        }
        true
    }

    fn has_capacity(&self, bytes: usize) -> bool {
        bytes <= self.max_bytes.saturating_sub(self.bytes)
            && (self.active.len() < self.max_active || self.pending.len() < self.max_pending)
    }

    fn admit(
        &mut self,
        inner: &Arc<ServerInner>,
        op: AdmittedOp,
        bytes: usize,
    ) -> ControlAdmission {
        if !self.has_capacity(bytes) {
            return ControlAdmission::Full(Box::new(op));
        }
        if self.pending.is_empty() && self.active.len() < self.max_active {
            let Some(task) = inner.spawn_dispatch(op) else {
                return ControlAdmission::Closed;
            };
            self.track(task, bytes);
        } else {
            if self.pending.len() >= self.max_pending {
                return ControlAdmission::Full(Box::new(op));
            }
            self.pending.push_back((op, bytes));
        }
        // Active work keeps its payload charge until its join future is reaped.
        self.bytes += bytes;
        ControlAdmission::Accepted
    }

    fn clear_pending(&mut self) {
        for (_, bytes) in self.pending.drain(..) {
            self.bytes -= bytes;
        }
    }
}

async fn read_protocol_line_with_limit<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    limit: usize,
) -> std::io::Result<ProtocolLine> {
    let mut line = Vec::with_capacity(limit.min(8 * 1024));
    let mut too_large = false;
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            if too_large {
                return Ok(ProtocolLine::TooLarge);
            }
            if line.is_empty() {
                return Ok(ProtocolLine::Eof);
            }
            return String::from_utf8(line)
                .map(ProtocolLine::Line)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error));
        }

        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |index| index + 1);
        let chunk_len = newline.unwrap_or(available.len());
        if !too_large {
            if line
                .len()
                .checked_add(chunk_len)
                .is_none_or(|length| length > limit)
            {
                too_large = true;
                line.clear();
            } else {
                line.extend_from_slice(&available[..chunk_len]);
            }
        }
        reader.consume(consumed);

        if newline.is_some() {
            if too_large {
                return Ok(ProtocolLine::TooLarge);
            }
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            return String::from_utf8(line)
                .map(ProtocolLine::Line)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error));
        }
    }
}

/// The handlers a host must forward to the session it mints. `approval_handler`
/// routes the engine's approval requests through the protocol's pending-map;
/// `elicit_handler` does the same for ask_user.
pub struct CreateSessionArgs {
    pub durable_permissions: Arc<DurablePermissionLayer>,
    pub approval_handler: Arc<dyn ApprovalHandler>,
    pub elicit_handler: Arc<dyn ElicitHandler>,
    pub mcp_elicitation_presenter: Arc<dyn McpElicitationPresenter>,
    pub mcp_authorize_handler: Arc<dyn McpAuthorizeHandler>,
    pub init: ProtocolSessionInit,
    pub provider: Option<Arc<dyn ModelProvider>>,
    pub mcp_server_configs: Vec<McpServerConfig>,
    pub capability_roots: Vec<RuntimeCapabilityRoot>,
    pub selected_capability_roots: Option<Vec<SelectedRuntimeCapabilityRoot>>,
    pub dynamic_tools: Vec<Arc<dyn Tool>>,
    pub agents: Vec<AgentDefinition>,
    pub skills: Vec<Skill>,
    pub output_styles: Vec<OutputStyle>,
    pub hooks: Vec<Hook>,
    pub pricing: Option<PricingTable>,
}

/// Mints the session, called ONCE on the first `run` op and reused for every
/// later op — the session is PERSISTENT so conversation history accumulates
/// across turns and `rewind` has something to rewind.
pub type CreateSession = Arc<dyn Fn(CreateSessionArgs) -> CreateSessionFuture + Send + Sync>;
/// Classifies resolved root ownership without constructing a Session or provider.
pub type ClassifyGoalPursuit =
    Arc<dyn Fn(ProtocolSessionInit) -> Result<bool, String> + Send + Sync>;
pub type SavedGoalRequestHandler = Arc<
    dyn Fn(
            String,
            klaus_agent_core::goal::service::SavedGoalRequest,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<klaus_agent_core::goal::runtime::GoalSnapshot, BoxError>>
                    + Send,
            >,
        > + Send
        + Sync,
>;
pub type ReadThread = Arc<
    dyn Fn(String) -> Pin<Box<dyn Future<Output = Result<ThreadReadSnapshot, BoxError>> + Send>>
        + Send
        + Sync,
>;
pub type ReadSessionSummary = Arc<
    dyn Fn(String) -> Pin<Box<dyn Future<Output = Result<Option<ThreadSummary>, BoxError>> + Send>>
        + Send
        + Sync,
>;
pub type ReadProfileActivity = Arc<
    dyn Fn() -> Pin<
            Box<
                dyn Future<
                        Output = Result<
                            klaus_agent_core::store::profile_activity::ProfileActivity,
                            BoxError,
                        >,
                    > + Send,
            >,
        > + Send
        + Sync,
>;
pub type ReadSubagentHistory = Arc<
    dyn Fn(
            String,
            String,
        ) -> Pin<Box<dyn Future<Output = Result<SubAgentReadSnapshot, BoxError>> + Send>>
        + Send
        + Sync,
>;
pub type ImportExternalSession = Arc<
    dyn Fn(
            klaus_agent_core::store::external_import::ExternalSessionImport,
        ) -> Pin<
            Box<
                dyn Future<
                        Output = Result<
                            klaus_agent_core::store::external_import::ExternalSessionImportResult,
                            BoxError,
                        >,
                    > + Send,
            >,
        > + Send
        + Sync,
>;
pub type ListSessions = Arc<dyn Fn(ListOptions) -> ListSessionsFuture + Send + Sync>;
pub type BindSession = Arc<
    dyn Fn(CreateSessionArgs, String, Option<String>, Option<String>) -> CreateSessionFuture
        + Send
        + Sync,
>;
pub type ArchiveSession = Arc<dyn Fn(String) -> ArchiveSessionFuture + Send + Sync>;
pub type UnarchiveSession = Arc<dyn Fn(String) -> UnarchiveSessionFuture + Send + Sync>;
pub type DeleteSession = Arc<dyn Fn(String) -> DeleteSessionFuture + Send + Sync>;
pub type UpdateTitle = Arc<dyn Fn(String, String) -> UpdateTitleFuture + Send + Sync>;
pub type RenameThread = Arc<dyn Fn(String, String) -> RenameThreadFuture + Send + Sync>;
pub type UpdateThreadHostMetadata =
    Arc<dyn Fn(String, Option<RuntimeJson>) -> UpdateThreadHostMetadataFuture + Send + Sync>;
pub type LoadSubagentTranscript =
    Arc<dyn Fn(String, String) -> LoadSubagentTranscriptFuture + Send + Sync>;

/// One atomic ledger mutation: the affected record and resulting persisted state.
pub struct InstalledPluginLedgerMutation {
    pub plugin: InstalledPluginRecord,
    pub installed_plugins: InstalledPlugins,
    pub published_to_loaded_sessions: bool,
}

/// Endpoint-owned installed-plugin administration.
#[async_trait]
pub trait ProtocolPluginAdmin: Send + Sync {
    async fn list_installed_plugins(&self) -> Result<InstalledPlugins, BoxError>;
    fn runtime_snapshot(&self) -> PluginRuntimeSnapshot;
    async fn set_installed_plugin_enabled(
        &self,
        name: &str,
        enabled: bool,
    ) -> Result<Option<InstalledPluginLedgerMutation>, BoxError>;
    async fn uninstall_plugin(
        &self,
        name: &str,
        remove_files: bool,
    ) -> Result<Option<InstalledPluginLedgerMutation>, BoxError>;
    /// Materialize an already-resolved plugin source directory into the store and
    /// record it in the ledger, preserving existing enablement and data. New
    /// installs start enabled. A supplied expected version requires an existing
    /// matching installation; an empty string matches an unversioned record.
    /// `None` when the source has no loadable manifest.
    async fn install_plugin(
        &self,
        marketplace: &str,
        source_dir: &str,
        version: Option<&str>,
        expected_installed_version: Option<&str>,
    ) -> Result<Option<InstalledPluginLedgerMutation>, BoxError>;
    /// Retire the legacy bundled Diagram record after the official package is available.
    async fn migrate_bundled_diagram(&self, source_dir: &str) -> Result<bool, BoxError>;
    /// Read the packages under each supplied marketplace root and join them with
    /// the ledger's install/enable state.
    async fn list_plugin_catalog(
        &self,
        roots: &[ProtocolPluginCatalogRoot],
    ) -> Result<ProtocolPluginCatalog, BoxError>;
}

/// Publishes host skills into one loaded engine session.
pub type SetSkills = Arc<dyn Fn(Arc<Session>, Vec<Skill>) -> Result<(), String> + Send + Sync>;

/// One paged saved-transcript read. When `turn_id` is set, cursor and limit
/// select messages in that turn before the store decodes their content.
#[derive(Debug, Clone, Default)]
pub struct ThreadPageRequest {
    pub cursor: Option<String>,
    pub limit: Option<usize>,
    pub items_view: Option<String>,
    pub sort_direction: Option<String>,
    pub turn_id: Option<String>,
}

pub type ReadThreadPageFuture = Pin<
    Box<
        dyn Future<
                Output = Result<
                    Option<klaus_agent_core::store::thread_index::ThreadPageSnapshot>,
                    BoxError,
                >,
            > + Send,
    >,
>;

/// Serves one page of a saved rollout from the store's durable turn index.
/// `None` means the store does not implement indexed paging. Errors are final
/// and must never trigger a whole-file retry.
pub type ReadThreadPage =
    Arc<dyn Fn(String, ThreadPageRequest) -> ReadThreadPageFuture + Send + Sync>;

/// Store maintenance stays inside the named facade and never creates a session.
pub type CompressRollouts = Arc<
    dyn Fn(Arc<AtomicBool>) -> Pin<Box<dyn Future<Output = Result<RuntimeJson, BoxError>> + Send>>
        + Send
        + Sync,
>;
pub type MigrateRollouts = Arc<
    dyn Fn(
            klaus_agent_core::store::session_store::RolloutMigrationRequest,
            Arc<AtomicBool>,
        ) -> Pin<Box<dyn Future<Output = Result<RuntimeJson, BoxError>> + Send>>
        + Send
        + Sync,
>;

/// Shared by every facade connection belonging to one endpoint.
#[derive(Default)]
pub struct RolloutMaintenance {
    pub compress: Option<CompressRollouts>,
    pub migrate: Option<MigrateRollouts>,
    pub operation_gate: Arc<tokio::sync::Mutex<()>>,
    pub startup_claimed: AtomicBool,
    pub compression_pending: AtomicBool,
}

#[derive(Default, Clone)]
pub struct ProtocolLifecycle {
    pub rollout_maintenance: Option<Arc<RolloutMaintenance>>,
    /// Endpoint-owned publication into the loaded session extension generation.
    pub set_skills: Option<SetSkills>,
    /// Saved goal access uses the store directly and does not resolve a model.
    pub saved_goal_request: Option<SavedGoalRequestHandler>,
    pub classify_goal_pursuit: Option<ClassifyGoalPursuit>,
    pub read_thread: Option<ReadThread>,
    /// Paged saved-transcript reads backed by the store's durable turn index.
    /// `None` keeps every paged read on the whole-file projection.
    pub read_thread_page: Option<ReadThreadPage>,
    pub read_session_summary: Option<ReadSessionSummary>,
    pub read_profile_activity: Option<ReadProfileActivity>,
    pub read_subagent_history: Option<ReadSubagentHistory>,
    /// Direct app-server store operation; it never instantiates a protocol session.
    pub import_external_session: Option<ImportExternalSession>,
    pub list_sessions: Option<ListSessions>,
    pub resume_session: Option<BindSession>,
    pub fork_session: Option<BindSession>,
    pub create_side_task: Option<BindSession>,
    pub archive_session: Option<ArchiveSession>,
    pub unarchive_session: Option<UnarchiveSession>,
    pub delete_session: Option<DeleteSession>,
    pub update_title: Option<UpdateTitle>,
    pub rename_thread: Option<RenameThread>,
    pub update_thread_host_metadata: Option<UpdateThreadHostMetadata>,
    pub load_subagent_transcript: Option<LoadSubagentTranscript>,
}

/// Endpoint-internal provider decoration after credentials have been resolved.
/// It is not serialized and cannot introduce a second client integration surface.
pub type ProviderDecorator = Arc<
    dyn Fn(Arc<dyn ModelProvider>, &ProtocolSessionInit) -> Arc<dyn ModelProvider> + Send + Sync,
>;

/// Options for constructing a [`ProtocolServer`].
pub struct ProtocolServerOptions {
    pub create_session: CreateSession,
    pub lifecycle: Option<ProtocolLifecycle>,
    /// Endpoint-level plugin ledger manager. Absent for hermetic endpoints.
    pub plugin_admin: Option<Arc<dyn ProtocolPluginAdmin>>,
    /// Endpoint-owned secret store shared by provider and MCP authentication.
    pub credential_store: Option<Arc<dyn CredentialStore>>,
    /// Newline-delimited JSON ops arrive here.
    pub input: Box<dyn AsyncRead + Unpin + Send>,
    /// One JSON protocol event (or notification) is written per line here.
    pub output: Box<dyn AsyncWrite + Unpin + Send>,
}

/// A unit of the serial op-chain: a submission id (for error correlation) plus a
/// deferred async job.
struct ChainJob {
    submission_id: String,
    run: Box<dyn FnOnce() -> ChainFuture + Send>,
}

struct ReadonlyRequest {
    signal: AbortSignal,
}

enum PluginAdminOp {
    List,
    Runtime,
    SetEnabled {
        name: String,
        enabled: bool,
    },
    Uninstall {
        name: String,
        remove_files: bool,
    },
    Install {
        marketplace: String,
        source_dir: String,
        version: Option<String>,
        expected_installed_version: Option<String>,
    },
    Catalog {
        roots: Vec<ProtocolPluginCatalogRoot>,
    },
}

/// One lock orders foreground admission, steering, interruption and termination.
struct ForegroundReservation {
    token: uuid::Uuid,
    submission_id: String,
    early_steer: bool,
    signal: AbortSignal,
}

struct RunAdmission {
    signal: AbortSignal,
    /// Every admitted foreground operation owns a place in the serial queue.
    /// A starting resume still owns the front even while it is not steerable.
    foreground: VecDeque<ForegroundReservation>,
    /// Manual compact and review cannot accept main-thread steering.
    non_steerable: HashMap<uuid::Uuid, NonSteerableReservation>,
    steer_turn_id: Option<String>,
    cancelled_realtime: HashMap<String, std::time::Instant>,
    pending_goal_stops: usize,
    activation_epoch: u64,
    last_active_token: Option<uuid::Uuid>,
}

impl RunAdmission {
    fn new() -> Self {
        Self {
            signal: AbortSignal::new(),
            foreground: VecDeque::new(),
            non_steerable: HashMap::new(),
            steer_turn_id: None,
            cancelled_realtime: HashMap::new(),
            pending_goal_stops: 0,
            activation_epoch: 0,
            last_active_token: None,
        }
    }

    fn activate_front(&mut self) {
        if let Some(owner) = self.foreground.front() {
            self.activation_epoch = self
                .activation_epoch
                .checked_add(1)
                .expect("foreground activation epoch exhausted");
            self.last_active_token = Some(owner.token);
        }
    }

    fn child_token(&self) -> AbortSignal {
        self.signal.child_token()
    }

    fn refresh_early_steer(&mut self, stopped: bool) {
        self.steer_turn_id = self.foreground.front().and_then(|owner| {
            (owner.early_steer && !owner.signal.is_cancelled() && !stopped)
                .then(|| owner.submission_id.clone())
        });
    }

    fn mark_steerable(&mut self, token: uuid::Uuid, stopped: bool) {
        if let Some(owner) = self.foreground.front() {
            if owner.token == token && !owner.signal.is_cancelled() && !stopped {
                self.steer_turn_id = Some(owner.submission_id.clone());
            }
        }
    }

    fn finish_steer(&mut self, token: uuid::Uuid) {
        if self
            .foreground
            .front()
            .is_some_and(|owner| owner.token == token)
        {
            self.steer_turn_id = None;
        }
    }

    fn steer_rejection(&self, expected: Option<&str>, stopped: bool) -> Option<String> {
        let expected = expected?;
        if self.signal.is_cancelled() || stopped {
            return Some("steer turn is no longer active".to_string());
        }
        match self.steer_turn_id.as_deref() {
            None => Some("steer turn is no longer active".to_string()),
            Some(active) if active != expected => Some(format!(
                "steer turn mismatch: expected {expected}, active {active}"
            )),
            Some(_) => None,
        }
    }
}

/// One reservation owns one decision. Drop resolves an unadmitted successor.
struct GoalContinuationDecision {
    inner: Weak<ServerInner>,
    pending: crate::types::ProtocolGoalContinuation,
    settled: AtomicBool,
    prior_presentation: crate::types::ProtocolTerminalNotificationDisposition,
}
impl GoalContinuationDecision {
    fn settle(&self, continued: bool) -> Option<tokio::task::JoinHandle<()>> {
        self.settle_with_presentation(continued, None)
    }
    fn settle_with_presentation(
        &self,
        continued: bool,
        presentation: Option<crate::types::ProtocolTerminalNotificationDisposition>,
    ) -> Option<tokio::task::JoinHandle<()>> {
        if self.settled.swap(true, Ordering::SeqCst) {
            return None;
        }
        let inner = self.inner.upgrade()?;
        let mut decision = self.pending.clone();
        decision.pending = false;
        decision.continued = Some(continued);
        decision.prior_completion_notification_disposition = Some(if continued {
            crate::types::ProtocolTerminalNotificationDisposition::Suppress
        } else {
            presentation.unwrap_or(self.prior_presentation)
        });
        let event = ProtocolEvent {
            submission_id: RuntimeString::default(),
            goal_continuation_decision: Some(decision),
            ..Default::default()
        };
        // The writer retains the settled decision if its caller is cancelled
        // while awaiting the receipt. Provider admission is never replayed.
        let runtime = tokio::runtime::Handle::try_current().ok()?;
        Some(runtime.spawn(async move {
            inner.write(event).await;
        }))
    }
}

struct GoalReservation {
    signal: AbortSignal,
    guard: CurrentSubmissionGuard,
    submission_id: String,
}

struct CurrentSubmissionGuard {
    inner: Weak<ServerInner>,
    submission_id: String,
    reservation_token: uuid::Uuid,
    goal_reschedule: bool,
    goal_decision: Option<Arc<GoalContinuationDecision>>,
}

impl CurrentSubmissionGuard {
    async fn settle_goal_decision(&self, continued: bool) {
        if let Some(decision) = &self.goal_decision {
            if let Some(write) = decision.settle(continued) {
                let _ = write.await;
            }
        }
    }
    async fn settle_failed_goal_decision(&self) {
        if let Some(decision) = &self.goal_decision {
            if let Some(write) = decision.settle_with_presentation(
                false,
                Some(crate::types::ProtocolTerminalNotificationDisposition::Suppress),
            ) {
                let _ = write.await;
            }
        }
    }
}

impl Drop for CurrentSubmissionGuard {
    fn drop(&mut self) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        let mut admission = inner.run_admission.lock().unwrap();
        let was_owner = admission
            .foreground
            .front()
            .is_some_and(|owner| owner.token == self.reservation_token);
        if let Some(position) = admission
            .foreground
            .iter()
            .position(|owner| owner.token == self.reservation_token)
        {
            admission.foreground.remove(position);
        }
        if was_owner {
            admission.activate_front();
            admission.refresh_early_steer(inner.stopped.load(Ordering::SeqCst));
            let mut current = inner.current_submission_id.lock().unwrap();
            if *current == self.submission_id {
                current.clear();
            }
        }
        let schedule = was_owner && self.goal_reschedule && admission.foreground.is_empty();
        drop(admission);
        inner.foreground_retired.notify_waiters();
        if let Some(decision) = &self.goal_decision {
            decision.settle(false);
        }
        if schedule {
            inner.schedule_goal_idle();
        }
    }
}

/// Holds automatic admission while a genuine endpoint Stop is being settled.
struct PendingGoalStop {
    inner: Weak<ServerInner>,
    armed: bool,
}

impl PendingGoalStop {
    fn release_locked(&mut self, admission: &mut RunAdmission) {
        if self.armed {
            admission.pending_goal_stops -= 1;
            self.armed = false;
        }
    }
}

impl Drop for PendingGoalStop {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Some(inner) = self.inner.upgrade() {
            let mut admission = inner.run_admission.lock().unwrap();
            self.release_locked(&mut admission);
            drop(admission);
            inner.schedule_goal_idle();
        }
    }
}

#[derive(Clone, Default)]
struct SessionInitState {
    raw: ProtocolSessionInit,
    provider: Option<Arc<dyn ModelProvider>>,
    mcp_server_configs: Vec<McpServerConfig>,
    capability_roots: Vec<RuntimeCapabilityRoot>,
    selected_capability_roots: Option<Vec<SelectedRuntimeCapabilityRoot>>,
    dynamic_tools: Vec<Arc<dyn Tool>>,
    agents: Vec<AgentDefinition>,
    skills: Vec<Skill>,
    output_styles: Vec<OutputStyle>,
    hooks: Vec<Hook>,
    pricing: Option<PricingTable>,
    targets: Option<klaus_agent_core::engine::session::InferenceTargets>,
    advisor_target: Option<klaus_agent_core::engine::session::InferenceTargetState>,
    feature_targets: klaus_agent_core::engine::session::FeatureTargets,
}

#[derive(Default)]
struct QueuePublicationBarrierState {
    active: bool,
    buffered: Vec<UnsolicitedWrite>,
    // Core publication sequences are process-global and monotonic. One floor
    // per Session therefore supersedes both the queried list and an old team
    // identity whose callback reaches protocol late, while remaining bounded
    // across repeated TeamCreate/TeamDelete identity changes.
    task_publication_floors: HashMap<RuntimeString, u64>,
}

/// Owns one queue-mutation publication epoch. Dropping an in-flight protocol
/// dispatch must never leave the endpoint permanently buffering queue events.
struct QueuePublicationBarrierGuard {
    inner: Arc<ServerInner>,
    drop_action: Option<PublicationBarrierDropAction>,
}

#[derive(Clone, Copy)]
enum PublicationBarrierDropAction {
    Release,
    Discard,
}

impl QueuePublicationBarrierGuard {
    fn release(mut self) {
        self.inner.release_queue_publication_barrier();
        self.drop_action = None;
    }

    fn discard(mut self) {
        self.inner.discard_queue_publication_barrier();
        self.drop_action = None;
    }
}

impl Drop for QueuePublicationBarrierGuard {
    fn drop(&mut self) {
        match self.drop_action.take() {
            Some(PublicationBarrierDropAction::Release) => {
                self.inner.release_queue_publication_barrier();
            }
            Some(PublicationBarrierDropAction::Discard) => {
                self.inner.discard_queue_publication_barrier();
            }
            None => {}
        }
    }
}

enum SessionPublicationRequest {
    Ensure,
    Bind {
        submission_id: String,
        thread_id: String,
        new_thread_id: Option<String>,
        before_message_uuid: Option<String>,
        bind: BindSession,
    },
}

struct SessionPublicationInflight {
    session: Arc<Session>,
    awaiting_bound_write: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SessionPublicationAdmissionState {
    Pending,
    Published,
    Closed,
}

/// Shared admission boundary for every Session-originated callback installed
/// before publication. A callback holds this mutex through its FIFO enqueue,
/// so failed-publication cleanup can close admission without a cloned old sink
/// escaping after the initial-event barrier has been discarded.
struct SessionPublicationAdmission {
    state: StdMutex<SessionPublicationAdmissionState>,
}

impl SessionPublicationAdmission {
    fn new() -> Self {
        Self {
            state: StdMutex::new(SessionPublicationAdmissionState::Pending),
        }
    }

    fn enqueue(&self, enqueue: impl FnOnce()) {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if *state == SessionPublicationAdmissionState::Closed {
            return;
        }
        enqueue();
    }

    fn close(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *state = SessionPublicationAdmissionState::Closed;
    }

    #[cfg(test)]
    fn state(&self) -> SessionPublicationAdmissionState {
        *self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

struct SessionBindingAdapters {
    queue_unsubscribe: Option<Box<dyn Fn() + Send + Sync>>,
    task_list_unsubscribe: Option<Box<dyn Fn() + Send + Sync>>,
    background_process_unsubscribe: Option<Box<dyn Fn() + Send + Sync>>,
}

impl Drop for SessionBindingAdapters {
    fn drop(&mut self) {
        for unsubscribe in [
            self.queue_unsubscribe.take(),
            self.task_list_unsubscribe.take(),
            self.background_process_unsubscribe.take(),
        ]
        .into_iter()
        .flatten()
        {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(unsubscribe));
        }
    }
}

struct SessionPublicationResources {
    barrier: Option<QueuePublicationBarrierGuard>,
    adapters: Option<SessionBindingAdapters>,
    queue_operation_guard: Option<tokio::sync::OwnedMutexGuard<()>>,
    publication_guard: Option<tokio::sync::OwnedMutexGuard<()>>,
    inflight_owner: Option<(Weak<ServerInner>, Weak<Session>)>,
}

impl SessionPublicationResources {
    fn empty() -> Self {
        Self {
            barrier: None,
            adapters: None,
            queue_operation_guard: None,
            publication_guard: None,
            inflight_owner: None,
        }
    }

    fn new(
        inner: &Arc<ServerInner>,
        session: &Arc<Session>,
        publication_guard: tokio::sync::OwnedMutexGuard<()>,
    ) -> Self {
        Self {
            barrier: None,
            adapters: None,
            queue_operation_guard: None,
            publication_guard: Some(publication_guard),
            inflight_owner: Some((Arc::downgrade(inner), Arc::downgrade(session))),
        }
    }

    fn clear_inflight(&mut self) {
        let Some((inner, session)) = self.inflight_owner.take() else {
            return;
        };
        let (Some(inner), Some(session)) = (inner.upgrade(), session.upgrade()) else {
            return;
        };
        let mut inflight = inner
            .session_publication_inflight
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if inflight
            .as_ref()
            .is_some_and(|publication| Arc::ptr_eq(&publication.session, &session))
        {
            inflight.take();
        }
    }

    fn discard(&mut self) {
        self.clear_inflight();
        // Stop producers before discarding their buffered output, then release
        // the operation epoch only after its barrier is inactive.
        drop(self.adapters.take());
        if let Some(barrier) = self.barrier.take() {
            barrier.discard();
        }
        self.queue_operation_guard.take();
        self.publication_guard.take();
    }
}

impl Drop for SessionPublicationResources {
    fn drop(&mut self) {
        self.discard();
    }
}

struct UnpublishedSessionCleanupOwner {
    resources: Option<SessionPublicationResources>,
    task_guard: Option<ActivityGuard>,
}

impl Drop for UnpublishedSessionCleanupOwner {
    fn drop(&mut self) {
        // Release every protocol-visible owner before tracker idle can become
        // observable, including when Tokio force-aborts this coroutine.
        drop(self.resources.take());
        drop(self.task_guard.take());
    }
}

/// Owns a minted Session until it is atomically published. If any await,
/// panic, caller cancellation, or shutdown abort wins first, Drop transfers
/// both the Session and publication gate to retained cleanup before the outer
/// dispatch guard can disappear.
struct UnpublishedSessionGuard {
    inner: std::sync::Weak<ServerInner>,
    session: Option<Arc<Session>>,
    resources: SessionPublicationResources,
    admission: Arc<SessionPublicationAdmission>,
}

impl UnpublishedSessionGuard {
    fn new(
        inner: &Arc<ServerInner>,
        session: Arc<Session>,
        publication_guard: tokio::sync::OwnedMutexGuard<()>,
    ) -> Self {
        let resources = SessionPublicationResources::new(inner, &session, publication_guard);
        Self {
            inner: Arc::downgrade(inner),
            session: Some(session),
            resources,
            admission: Arc::new(SessionPublicationAdmission::new()),
        }
    }

    fn admission(&self) -> Arc<SessionPublicationAdmission> {
        self.admission.clone()
    }

    fn arm_barrier(&mut self, barrier: QueuePublicationBarrierGuard) {
        self.resources.barrier = Some(barrier);
    }

    fn arm_queue_operation_guard(&mut self, guard: tokio::sync::OwnedMutexGuard<()>) {
        self.resources.queue_operation_guard = Some(guard);
    }

    fn arm_adapters(&mut self, adapters: SessionBindingAdapters) {
        self.resources.adapters = Some(adapters);
    }

    fn disarm_inflight_fallback(&mut self) {
        self.resources.inflight_owner.take();
    }

    /// Finish a publication whose fallible resources were validated and whose
    /// observable state was committed while the inflight mutex was held. This
    /// path intentionally contains only Option moves and Tokio guard releases.
    fn finish_commit(mut self) {
        self.resources.barrier.take();
        self.resources.adapters.take();
        self.resources.queue_operation_guard.take();
        self.session.take();
        self.resources.publication_guard.take();
    }
}

impl Drop for UnpublishedSessionGuard {
    fn drop(&mut self) {
        let Some(session) = self.session.take() else {
            return;
        };
        // Close callback admission synchronously. Cleanup may not receive its
        // first runtime poll before a captured barrier/adapters owner is
        // dropped, and an old cloned closure must never observe Pending after
        // that discard boundary.
        self.admission.close();
        // Core teardown is Session-owned from this synchronous point. The
        // protocol adapter cleanup below is abortable so endpoint force-close
        // can release its locks, but aborting it cannot abandon Session cleanup.
        session.initiate_shutdown();
        if let Some(inner) = self.inner.upgrade() {
            inner.start_unpublished_session_cleanup(
                session,
                std::mem::replace(&mut self.resources, SessionPublicationResources::empty()),
                self.admission.clone(),
            );
        }
    }
}

#[derive(Default)]
struct PendingPromptState {
    closed: bool,
    next_token: u64,
    approvals: HashMap<RuntimeString, (u64, oneshot::Sender<ApprovalResponse>)>,
    elicitations: HashMap<String, (u64, oneshot::Sender<ElicitResponse>)>,
    mcp_authorizations: HashMap<String, (u64, oneshot::Sender<McpAuthorizeResponse>)>,
    dynamic_calls: HashMap<
        RuntimeString,
        (
            u64,
            oneshot::Sender<Result<ProtocolDynamicToolResponse, RuntimeString>>,
        ),
    >,
}

impl PendingPromptState {
    fn next_token(&mut self) -> Option<u64> {
        let token = self.next_token;
        self.next_token = self.next_token.checked_add(1)?;
        Some(token)
    }
}

/// Shared, interior-mutable server state.
struct ServerInner {
    provider_decorator: StdMutex<Option<ProviderDecorator>>,
    subagent_target_resolver:
        StdMutex<Option<klaus_agent_core::engine::session::SubagentTargetResolver>>,
    durable_permissions: Arc<DurablePermissionLayer>,
    create_session: CreateSession,
    lifecycle: ProtocolLifecycle,
    plugin_admin: Option<Arc<dyn ProtocolPluginAdmin>>,
    credential_store: Option<Arc<dyn CredentialStore>>,
    output: AsyncMutex<Option<Box<dyn AsyncWrite + Unpin + Send>>>,
    /// Serializes the complete synchronous stop admission-close boundary. The
    /// `stopped` flag is published at the start of the owner transaction, but
    /// a concurrent caller cannot return through this mutex until every
    /// top-level admission gate and parked Host interaction has been closed.
    stop_gate: StdMutex<()>,
    stopped: AtomicBool,
    /// Unified admission/drain state for every parked Host interaction.
    pending_prompts: StdMutex<PendingPromptState>,
    /// Side-question/title submission id -> independent cancellation signal.
    pending_readonly: StdMutex<HashMap<String, Arc<ReadonlyRequest>>>,
    cancelled_predictions: StdMutex<HashMap<String, std::time::Instant>>,
    /// Independent probes are dispatch work, not inference or readonly turns.
    /// Closing cancels them before waiting for the dispatch publication barrier.
    /// Host-supplied session init envelope, applied before the session is bound.
    session_init: StdMutex<SessionInitState>,
    /// The submission id of the run currently streaming — used to tag elicitations.
    current_submission_id: StdMutex<String>,
    /// Cancellation is captured at dispatch, before work enters the serial chain.
    /// Interrupt replaces this token so already-ACKed work cannot restart later.
    run_admission: StdMutex<RunAdmission>,
    foreground_retired: Notify,
    interrupt_commits: StdMutex<HashMap<String, InterruptPreparation>>,
    last_run_options: StdMutex<Option<RunOpOptions>>,
    goal_publication_buffer: StdMutex<Option<Vec<ProtocolEvent>>>,
    last_completed_submission: StdMutex<Option<String>>,
    /// Single-flight session creation (persistent across `run` ops).
    session: OnceCell<Arc<Session>>,
    /// Minted but not yet Host-visible Session. It remains retained until a
    /// flushed BoundSession is atomically published or cleanup releases its
    /// writer lease.
    session_publication_inflight: StdMutex<Option<SessionPublicationInflight>>,
    /// Set before an active session is archived/deleted so queued turns cannot start.
    session_closing: AtomicBool,
    session_closing_notify: Notify,
    /// Shared rewind orchestration for the persistent session.
    rewind_ctl: AsyncMutex<Option<SessionRewindBinding>>,
    /// Serializes `run` + `rewind` (+ `set_*`) ops so turns never overlap.
    chain_tx: StdMutex<Option<mpsc::UnboundedSender<ChainJob>>>,
    /// Becomes true only after the serial op-chain runner has stopped.  A
    /// separate flag is required because `Notify` does not retain permits for
    /// future waiters.
    chain_finished: AtomicBool,
    chain_finished_notify: Notify,
    chain_runner_task: StdMutex<AbortSlot>,
    active_chain_task: StdMutex<AbortSlot>,
    /// Ordered bridge for Session-originated events that are not tied to an op.
    unsolicited_tx: StdMutex<Option<mpsc::UnboundedSender<UnsolicitedWrite>>>,
    /// Serializes lazy create/resume/fork through adapter installation and
    /// publication. The owned guard is transferred to unpublished-session
    /// cleanup on cancellation, so shutdown cannot snapshot between mint and
    /// either publication or writer-lease release.
    session_publication_gate: Arc<AsyncMutex<()>>,
    /// Serializes Host queue reads/mutations that publish correlated responses.
    /// Core turn-owned reserve/drain transitions remain concurrent and are
    /// ordered by `queue_publication_barrier` instead.
    queue_operation_gate: Arc<AsyncMutex<()>>,
    /// While a Host mutation awaits its durability receipt, queue snapshots and
    /// queue-correlated acknowledgements are retained here. The mutation result
    /// enters `unsolicited_tx` first, then this buffer is released onto the same
    /// FIFO, so a result at revision N cannot be followed by a previously
    /// published response that exposed revision >N.
    queue_publication_barrier: StdMutex<QueuePublicationBarrierState>,
    /// First transport failure observed while projecting the ordered
    /// unsolicited stream. It is folded into the final shutdown completion.
    output_write_error: StdMutex<Option<String>>,
    unsolicited_finished: AtomicBool,
    unsolicited_finished_notify: Notify,
    unsolicited_task: StdMutex<AbortSlot>,
    #[cfg(test)]
    snapshot_write_test_gate: StdMutex<Option<Arc<SnapshotWriteTestGate>>>,
    #[cfg(test)]
    todo_query_test_gate: StdMutex<Option<Arc<TodoQueryTestGate>>>,
    #[cfg(test)]
    todo_callback_test_gate: StdMutex<Option<Arc<TodoCallbackTestGate>>>,
    #[cfg(test)]
    session_publication_callback_test_gate:
        StdMutex<Option<Arc<SessionPublicationCallbackTestGate>>>,
    #[cfg(test)]
    session_publication_commit_test_gate: StdMutex<Option<Arc<SessionPublicationCommitTestGate>>>,
    #[cfg(test)]
    session_publication_ensure_commit_test_gate:
        StdMutex<Option<Arc<SessionPublicationCommitTestGate>>>,
    #[cfg(test)]
    pending_prompt_registration_test_gate: StdMutex<Option<Arc<PendingPromptRegistrationTestGate>>>,
    #[cfg(test)]
    stop_admission_test_gate: StdMutex<Option<Arc<StopAdmissionTestGate>>>,
    #[cfg(test)]
    session_publication_commit_panic_for_test: AtomicBool,
    #[cfg(test)]
    session_publication_sinks_for_test: StdMutex<SessionPublicationTestSinks>,
    #[cfg(test)]
    session_publication_admission_for_test: StdMutex<Option<Weak<SessionPublicationAdmission>>>,
    /// Retain core subscription handles for the lifetime of this protocol
    /// binding. Dropping either handle immediately unsubscribes, which would
    /// otherwise make reconnect/Host invalidation streams silently disappear.
    queue_unsubscribe: StdMutex<Option<Box<dyn Fn() + Send + Sync>>>,
    task_list_unsubscribe: StdMutex<Option<Box<dyn Fn() + Send + Sync>>>,
    background_process_unsubscribe: StdMutex<Option<Box<dyn Fn() + Send + Sync>>>,
    /// The explicit wire shutdown request, if shutdown was initiated by an op.
    /// Only the first id is authoritative; high-level clients deduplicate calls.
    shutdown_submission_id: StdMutex<Option<RuntimeString>>,
    /// The input task can execute immediate (non-chain) operations.  Teardown
    /// closes this admission gate and waits for the currently dispatched line.
    dispatches: Arc<ActivityTracker>,
    /// Side-question and title tasks are detached from the input reader but must
    /// observe cancellation and finish before the Session is disposed.
    readonly_tasks: Arc<ActivityTracker>,
    /// Detached completion/cleanup tasks include dream completion and disposal
    /// of a Session rejected before publication.  They settle after the normal
    /// Session shutdown boundary (or independently when no Session was stored).
    completion_tasks: Arc<ActivityTracker>,
    /// Filesystem lifecycle transactions with a durable intent outlive the
    /// request/chain future that launched them. They are waited during
    /// shutdown, but never force-aborted mid-transaction.
    durable_lifecycle_tasks: Arc<ActivityTracker>,
    shutdown: Notify,
}

/// Marks the output transport untrustworthy if a task is cancelled after it
/// has begun writing a frame but before the frame and its flush complete.  The
/// task that owns an ordinary event can be force-aborted during shutdown; an
/// error-return-only path cannot observe that future being dropped.
struct OutputFrameGuard<'a> {
    inner: &'a ServerInner,
    frame_len: usize,
    written: usize,
    flushed: bool,
}

#[derive(Default)]
struct ProtocolWriteProgress {
    wrote_bytes: bool,
    flushed: bool,
}

/// Marks a caller-owned success frame untrustworthy if cancellation occurs
/// after bytes were accepted but before the authoritative flush boundary.
struct TransactionalWriteGuard<'a> {
    inner: &'a ServerInner,
    label: &'static str,
    progress: ProtocolWriteProgress,
}

impl<'a> TransactionalWriteGuard<'a> {
    fn new(inner: &'a ServerInner, label: &'static str) -> Self {
        Self {
            inner,
            label,
            progress: ProtocolWriteProgress::default(),
        }
    }
}

impl Drop for TransactionalWriteGuard<'_> {
    fn drop(&mut self) {
        if self.progress.wrote_bytes && !self.progress.flushed {
            self.inner.record_output_write_error(format!(
                "protocol {} write was interrupted before flush",
                self.label
            ));
        }
    }
}

impl<'a> OutputFrameGuard<'a> {
    fn new(inner: &'a ServerInner, frame_len: usize) -> Self {
        Self {
            inner,
            frame_len,
            written: 0,
            flushed: false,
        }
    }

    fn advance(&mut self, written: usize) {
        self.written = self
            .written
            .checked_add(written)
            .expect("protocol frame write offset overflow");
        debug_assert!(self.written <= self.frame_len);
    }

    fn complete(&mut self) {
        self.flushed = true;
    }
}

impl Drop for OutputFrameGuard<'_> {
    fn drop(&mut self) {
        // Zero bytes is cancellation-safe: the next writer can emit a complete
        // frame. A fully written but not-yet-flushed frame is also recoverable
        // by the shutdown path's explicit flush. Only a true prefix makes the
        // newline-delimited stream permanently ambiguous.
        if self.written == 0 || self.written == self.frame_len || self.flushed {
            return;
        }
        self.inner.record_output_write_error(
            "protocol output frame write was interrupted before it was fully flushed".to_string(),
        );
    }
}

/// Completes the retained chain state on every task exit, including panic or
/// cancellation while the runner future is being dropped.
struct ChainFinishGuard {
    inner: Arc<ServerInner>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StartState {
    NotStarted,
    Running,
    ClosingBeforeStart,
    Finished,
}

struct StartOwnerGuard {
    server: Option<Arc<ProtocolServer>>,
}

impl StartOwnerGuard {
    fn disarm(&mut self) {
        self.server = None;
    }
}

impl Drop for StartOwnerGuard {
    fn drop(&mut self) {
        let Some(server) = self.server.take() else {
            return;
        };
        server.stop();
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn(async move {
                    let _ = server.shutdown_and_wait().await;
                });
            }
            Err(_) => server.mark_start_finished(),
        }
    }
}

impl Drop for ChainFinishGuard {
    fn drop(&mut self) {
        self.inner.mark_chain_finished();
    }
}

struct UnsolicitedFinishGuard {
    inner: Arc<ServerInner>,
}

impl Drop for UnsolicitedFinishGuard {
    fn drop(&mut self) {
        self.inner.mark_unsolicited_finished();
    }
}

enum UnsolicitedWrite {
    Event(ProtocolEvent),
    EventWithReceipt {
        event: ProtocolEvent,
        receipt: UnsolicitedWriteReceipt,
    },
    TaskListChange {
        change: klaus_agent_core::tasks::task_store::TaskListChange,
        publication_sequence: u64,
    },
    MemoryMaintenanceSnapshot {
        submission_id: RuntimeString,
        snapshot: crate::types::MemoryMaintenanceSnapshot,
    },
}

struct UnsolicitedWriteReceipt {
    written: Option<oneshot::Sender<Result<(), String>>>,
    progress: ProtocolWriteProgress,
    /// Present only for BoundSession. The writer owns the complete transaction
    /// across write+flush and synchronously commits it before waking the
    /// request-side waiter. Dropping any queued/in-progress write drops this
    /// owner into retained unpublished cleanup first.
    publication: Option<BoundSessionPublication>,
}

impl Drop for UnsolicitedWriteReceipt {
    fn drop(&mut self) {
        if self.progress.wrote_bytes && !self.progress.flushed {
            if let Some(publication) = self.publication.as_ref() {
                publication.inner.record_output_write_error(
                    "protocol BoundSession write was interrupted before flush".to_string(),
                );
            }
        }
        // Register retained unpublished cleanup before closing the result
        // sender can wake/drop the request-side publication tracker guard.
        drop(self.publication.take());
    }
}

struct BoundSessionPublication {
    inner: Arc<ServerInner>,
    session: Arc<Session>,
    unpublished: Option<UnpublishedSessionGuard>,
    parent_guard: Option<ActivityGuard>,
}

impl BoundSessionPublication {
    fn commit(mut self) -> Result<(), String> {
        let unpublished = self
            .unpublished
            .take()
            .expect("BoundSession publication owner is armed");
        self.inner
            .commit_session_publication(&self.session, unpublished, true)?;
        if self.inner.session_closing.load(Ordering::SeqCst) {
            self.session.initiate_shutdown();
        }
        self.parent_guard.take();
        Ok(())
    }
}

impl Drop for BoundSessionPublication {
    fn drop(&mut self) {
        // The unpublished guard registers retained cleanup while its parent is
        // still active; only then may tracker idle/settled observation advance.
        drop(self.unpublished.take());
        drop(self.parent_guard.take());
    }
}

#[cfg(test)]
#[path = "server_test_gates.rs"]
mod test_gates;
#[cfg(test)]
use test_gates::*;

impl UnsolicitedWrite {
    #[cfg(test)]
    fn into_event(self) -> ProtocolEvent {
        self.into_event_and_receipt().0
    }

    fn into_event_and_receipt(self) -> (ProtocolEvent, Option<UnsolicitedWriteReceipt>) {
        match self {
            Self::Event(event) => (event, None),
            Self::EventWithReceipt { event, receipt } => (event, Some(receipt)),
            Self::TaskListChange { change, .. } => (
                ProtocolEvent {
                    submission_id: RuntimeString::default(),
                    task_list_change: Some(change),
                    ..Default::default()
                },
                None,
            ),
            Self::MemoryMaintenanceSnapshot {
                submission_id,
                snapshot,
            } => (
                ProtocolEvent {
                    submission_id,
                    memory_maintenance: Some(snapshot),
                    ..Default::default()
                },
                None,
            ),
        }
    }
}

fn task_write_is_superseded(
    write: &UnsolicitedWrite,
    floors: &HashMap<RuntimeString, u64>,
) -> bool {
    let UnsolicitedWrite::TaskListChange {
        change,
        publication_sequence,
    } = write
    else {
        return false;
    };
    floors
        .get(&change.session_id)
        .is_some_and(|floor| publication_sequence <= floor)
}

/// Reads newline-delimited ops and drives a core session.
pub struct ProtocolServer {
    inner: Arc<ServerInner>,
    input: StdMutex<Option<Box<dyn AsyncRead + Unpin + Send>>>,
    chain_rx: StdMutex<Option<mpsc::UnboundedReceiver<ChainJob>>>,
    unsolicited_rx: StdMutex<Option<mpsc::UnboundedReceiver<UnsolicitedWrite>>>,
    /// Shares one teardown result across `start()` and every explicit close
    /// caller.  In particular, a second close must not return while the first
    /// caller is still awaiting Session/MCP/LSP/rollout teardown.
    shutdown_result: std::sync::OnceLock<tokio::sync::watch::Receiver<Option<Result<(), String>>>>,
    start_state: StdMutex<StartState>,
    start_finished_notify: Notify,
    shutdown_timings: ProtocolShutdownTimings,
}

impl ProtocolServer {
    /// Internal facade support. The same layer is passed into the lazy Session
    /// factory, so refreshing a registered session never creates one.
    pub fn replace_durable_permission_rules(&self, rules: &DurableRules) -> Result<(), String> {
        let _stop = self
            .inner
            .stop_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.inner.stopped.load(Ordering::SeqCst)
            || self.inner.session_closing.load(Ordering::SeqCst)
            || self
                .inner
                .session
                .get()
                .is_some_and(|session| session.is_disposed())
        {
            return Err("session is closing".to_string());
        }
        self.inner.durable_permissions.replace_stored(rules);
        Ok(())
    }

    /// The caller holds the endpoint rule-store lock across save and receipt.
    pub fn confirm_durable_permission_grant(
        &self,
        update: &klaus_agent_core::types::permission::PermissionUpdate,
        rules: &[klaus_agent_core::types::permission::PermissionRule],
    ) {
        self.inner.durable_permissions.confirm_saved(update, rules);
    }

    /// Borrow an already-published session without initializing, resuming, or
    /// changing its lifecycle. Used only by endpoint read snapshots.
    pub fn notify_goal_idle(&self) {
        self.inner.schedule_goal_idle();
    }
    pub fn existing_session(&self) -> Option<Arc<Session>> {
        self.inner
            .session
            .get()
            .filter(|session| !session.is_disposed())
            .cloned()
    }

    /// Prepare the facade-owned session without serializing its transcript.
    /// Publication is retained by the protocol completion owner, so dropping a
    /// request waiter does not cancel recovery or release the rollout writer.
    pub async fn prepare_session(&self) -> Result<Arc<Session>, String> {
        if self.is_closing() {
            return Err("the session is closing".to_string());
        }
        self.inner
            .ensure_session()
            .await
            .map_err(|error| error.to_string())
    }

    /// Initialize the bound session for the facade-owned realtime transcript sink.
    pub async fn realtime_session(&self) -> Result<Arc<Session>, String> {
        self.prepare_session().await
    }

    /// Cancel only a facade-owned submission, including one still in the serial queue.
    /// Keep ownership and the active interruption under the same admission gate.
    pub fn interrupt_realtime_submission(&self, submission_id: &str) -> bool {
        let mut admission = self.inner.run_admission.lock().unwrap();
        let position = admission
            .foreground
            .iter()
            .position(|owner| owner.submission_id == submission_id);
        if let Some(position) = position {
            admission.foreground[position].signal.cancel();
            if position == 0 {
                if let Some(session) = self.inner.session.get() {
                    session.interrupt();
                }
                self.inner.fail_pending("interrupted");
            }
        } else {
            // Facade admission precedes the protocol reader. Remember a cancellation
            // across that narrow gap so a queued task cannot start after Stop.
            admission
                .cancelled_realtime
                .retain(|_, time| time.elapsed() < std::time::Duration::from_secs(60));
            if admission.cancelled_realtime.len() >= MAX_PENDING_PROTOCOL_LINES {
                return false;
            }
            admission
                .cancelled_realtime
                .insert(submission_id.into(), std::time::Instant::now());
        }
        true
    }

    /// Read this session's current steering target without changing its ownership.
    pub fn realtime_steer_target(&self) -> Option<String> {
        if self.is_closing() {
            return None;
        }
        self.inner
            .run_admission
            .lock()
            .unwrap()
            .steer_turn_id
            .clone()
    }

    /// Retire only queued voice work; a turn that already owns execution continues.
    pub fn retire_queued_realtime_submission(&self, submission_id: &str) -> bool {
        let mut admission = self.inner.run_admission.lock().unwrap();
        let position = admission
            .foreground
            .iter()
            .position(|owner| owner.submission_id == submission_id);
        if position == Some(0) && *self.inner.current_submission_id.lock().unwrap() == submission_id
        {
            return false;
        }
        if let Some(position) = position {
            admission.foreground[position].signal.cancel();
        } else {
            admission
                .cancelled_realtime
                .retain(|_, time| time.elapsed() < std::time::Duration::from_secs(60));
            if admission.cancelled_realtime.len() >= MAX_PENDING_PROTOCOL_LINES {
                return false;
            }
            admission
                .cancelled_realtime
                .insert(submission_id.into(), std::time::Instant::now());
        }
        true
    }

    /// Whether this endpoint has stopped admitting work. The facade uses this
    /// before queueing a turn because its pipe writer may outlive input close.
    pub fn is_closing(&self) -> bool {
        self.inner.stopped.load(Ordering::SeqCst)
            || self.inner.session_closing.load(Ordering::SeqCst)
    }

    /// Cancellation edge for bounded facade preparation that does not own a
    /// turn. Closing wakes every waiter without taking the ordinary op queue.
    pub async fn wait_until_closing(&self) {
        loop {
            let closing = self.inner.session_closing_notify.notified();
            if self.inner.session_closing.load(Ordering::SeqCst) {
                return;
            }
            closing.await;
        }
    }

    /// Establish a capture fence directly, without entering the serial turn queue.
    pub fn begin_model_update(
        &self,
        change_id: String,
        expected_revision: Option<u64>,
    ) -> Result<u64, String> {
        if self.is_closing() {
            return Err("cannot begin a model update on a closing protocol session".to_string());
        }
        self.existing_session()
            .ok_or_else(|| "model update requires an existing live session".to_string())?
            .begin_model_update(change_id, expected_revision)
    }

    /// Internal named-facade support for callers without a change fence.
    pub async fn reconfigure_model(
        &self,
        init: ProtocolSessionInit,
        expected_revision: u64,
    ) -> Result<u64, String> {
        self.reconfigure_model_with_change_id(init, expected_revision, None)
            .await
    }

    /// Preparation never takes the turn queue or the pending-prompt lock. Core
    /// owns capture/publication safety and the matching change's failure state.
    pub async fn reconfigure_model_with_change_id(
        &self,
        init: ProtocolSessionInit,
        expected_revision: u64,
        change_id: Option<String>,
    ) -> Result<u64, String> {
        let session = self
            .existing_session()
            .ok_or_else(|| "model reconfiguration requires an existing live session".to_string())?;
        let prepare = async {
            validate_serializable_session_init(&init)?;
            let mut targets = targets_from_init(&init, self.inner.credential_store.clone()).await?;
            if let Some(decorate) = self.inner.provider_decorator.lock().unwrap().as_ref() {
                decorate_model_targets(&mut targets, decorate);
            }
            // A tier-only publication carries no main provider: it swaps the
            // auxiliary targets under their own CAS and must not be mistaken for
            // an incomplete main-model change. It still runs inside this prepare
            // block so a failure reports against the change fence.
            if init.provider.is_none() {
                session
                    .prepare_inference_targets(&mut targets, &mut None)
                    .await?;
                return session.reconfigure_tier_targets(
                    targets.ok_or("tier-only model reconfiguration requires modelTierTargets")?,
                    expected_revision,
                );
            }
            let mut advisor_target =
                advisor_target_from_init(&init, self.inner.credential_store.clone()).await?;
            if let (Some(target), Some(decorate)) = (
                advisor_target.as_mut(),
                self.inner.provider_decorator.lock().unwrap().as_ref(),
            ) {
                decorate_model_target(target, decorate);
            }
            let mut feature_targets =
                feature_targets_from_init(&init, self.inner.credential_store.clone()).await?;
            if let Some(decorate) = self.inner.provider_decorator.lock().unwrap().as_ref() {
                decorate_feature_targets(&mut feature_targets, decorate);
            }
            let config_value = inference_config_value(&init)?;
            let context_window_override = config_value
                .get("contextWindow")
                .and_then(serde_json::Value::as_u64);
            let explicit_model = config_value
                .get("model")
                .is_some_and(|value| !value.is_null());
            let mut config = resolve_config(config_value)?;
            let provider = provider_from_init(&init, self.inner.credential_store.clone())
                .await?
                .ok_or_else(|| "model reconfiguration requires an explicit provider".to_string())?;
            let provider = self
                .inner
                .provider_decorator
                .lock()
                .unwrap()
                .as_ref()
                .map(|decorate| decorate(provider.clone(), &init))
                .unwrap_or(provider);
            let catalog = klaus_agent_core::provider::model_catalog::ModelCatalog::load(None);
            let provider =
                klaus_agent_core::provider::model_catalog::with_model_catalog(provider, catalog);
            if !explicit_model {
                config.model = provider.info().default_model.clone();
            }
            let pricing = init
                .pricing
                .as_ref()
                .map(|entries| {
                    entries
                        .iter()
                        .map(|entry| {
                            scalar_runtime_string(entry.model.clone(), "pricing model")
                                .map(|model| (model, entry.pricing))
                        })
                        .collect::<Result<Vec<_>, String>>()
                })
                .transpose()?;
            let publication_key = canonical_inference_key(&init)?;
            let review_requirements = session
                .prepare_review_requirements(provider.clone())
                .await?;
            session
                .prepare_inference_targets(&mut targets, &mut advisor_target)
                .await?;
            session
                .prepare_feature_targets(&mut feature_targets)
                .await?;
            if self.is_closing() {
                return Err("cannot reconfigure a closing protocol session".to_string());
            }
            session.reconfigure_inference_with_targets(
                provider,
                config,
                context_window_override,
                pricing,
                klaus_agent_core::engine::session::InferencePublication {
                    expected_revision,
                    change_id: change_id.clone(),
                    targets,
                    advisor_target,
                    feature_targets,
                    source: model_source_from_init(&init)?,
                    rates: None,
                    publication_key: Some(publication_key),
                    review_requirements: Some(review_requirements),
                },
            )
        };
        let result = tokio::time::timeout(std::time::Duration::from_secs(75), async {
            tokio::select! {
                biased;
                _ = self.wait_until_closing() => Err("model preparation cancelled because the session is closing".to_string()),
                result = prepare => result,
            }
        })
        .await
        .unwrap_or_else(|_| Err("model provider preparation timed out".to_string()));
        if let (Some(change_id), Err(error)) = (change_id.as_deref(), &result) {
            session.fail_model_update(change_id, error.clone());
        }
        result
    }

    /// Install an endpoint-owned provider decorator before starting this server.
    pub fn install_provider_decorator(&self, decorator: ProviderDecorator) {
        *self.inner.provider_decorator.lock().unwrap() = Some(decorator);
    }

    /// Install source resolution for new child admissions before starting this server.
    pub fn install_subagent_target_resolver(
        &self,
        resolver: klaus_agent_core::engine::session::SubagentTargetResolver,
    ) {
        *self.inner.subagent_target_resolver.lock().unwrap() = Some(resolver);
    }

    pub fn new(opts: ProtocolServerOptions) -> Self {
        let (chain_tx, chain_rx) = mpsc::unbounded_channel();
        let (unsolicited_tx, unsolicited_rx) = mpsc::unbounded_channel();
        let inner = Arc::new(ServerInner {
            provider_decorator: StdMutex::new(None),
            subagent_target_resolver: StdMutex::new(None),
            durable_permissions: Arc::new(DurablePermissionLayer::default()),
            create_session: opts.create_session,
            lifecycle: opts.lifecycle.unwrap_or_default(),
            plugin_admin: opts.plugin_admin,
            credential_store: opts.credential_store,
            output: AsyncMutex::new(Some(opts.output)),
            stop_gate: StdMutex::new(()),
            stopped: AtomicBool::new(false),
            pending_prompts: StdMutex::new(PendingPromptState::default()),
            pending_readonly: StdMutex::new(HashMap::new()),
            cancelled_predictions: StdMutex::new(HashMap::new()),
            session_init: StdMutex::new(SessionInitState::default()),
            current_submission_id: StdMutex::new(String::new()),
            run_admission: StdMutex::new(RunAdmission::new()),
            foreground_retired: Notify::new(),
            interrupt_commits: StdMutex::new(HashMap::new()),
            last_run_options: StdMutex::new(None),
            goal_publication_buffer: StdMutex::new(None),
            last_completed_submission: StdMutex::new(None),
            session: OnceCell::new(),
            session_publication_inflight: StdMutex::new(None),
            session_closing: AtomicBool::new(false),
            session_closing_notify: Notify::new(),
            rewind_ctl: AsyncMutex::new(None),
            chain_tx: StdMutex::new(Some(chain_tx)),
            chain_finished: AtomicBool::new(false),
            chain_finished_notify: Notify::new(),
            chain_runner_task: StdMutex::new(AbortSlot::default()),
            active_chain_task: StdMutex::new(AbortSlot::default()),
            unsolicited_tx: StdMutex::new(Some(unsolicited_tx)),
            session_publication_gate: Arc::new(AsyncMutex::new(())),
            queue_operation_gate: Arc::new(AsyncMutex::new(())),
            queue_publication_barrier: StdMutex::new(QueuePublicationBarrierState::default()),
            output_write_error: StdMutex::new(None),
            unsolicited_finished: AtomicBool::new(false),
            unsolicited_finished_notify: Notify::new(),
            unsolicited_task: StdMutex::new(AbortSlot::default()),
            #[cfg(test)]
            snapshot_write_test_gate: StdMutex::new(None),
            #[cfg(test)]
            todo_query_test_gate: StdMutex::new(None),
            #[cfg(test)]
            todo_callback_test_gate: StdMutex::new(None),
            #[cfg(test)]
            session_publication_callback_test_gate: StdMutex::new(None),
            #[cfg(test)]
            session_publication_commit_test_gate: StdMutex::new(None),
            #[cfg(test)]
            session_publication_ensure_commit_test_gate: StdMutex::new(None),
            #[cfg(test)]
            pending_prompt_registration_test_gate: StdMutex::new(None),
            #[cfg(test)]
            stop_admission_test_gate: StdMutex::new(None),
            #[cfg(test)]
            session_publication_commit_panic_for_test: AtomicBool::new(false),
            #[cfg(test)]
            session_publication_sinks_for_test: StdMutex::new(
                SessionPublicationTestSinks::default(),
            ),
            #[cfg(test)]
            session_publication_admission_for_test: StdMutex::new(None),
            queue_unsubscribe: StdMutex::new(None),
            task_list_unsubscribe: StdMutex::new(None),
            background_process_unsubscribe: StdMutex::new(None),
            shutdown_submission_id: StdMutex::new(None),
            dispatches: Arc::new(ActivityTracker::default()),
            readonly_tasks: Arc::new(ActivityTracker::default()),
            completion_tasks: Arc::new(ActivityTracker::default()),
            durable_lifecycle_tasks: Arc::new(ActivityTracker::default()),
            shutdown: Notify::new(),
        });
        ProtocolServer {
            inner,
            input: StdMutex::new(Some(opts.input)),
            chain_rx: StdMutex::new(Some(chain_rx)),
            unsolicited_rx: StdMutex::new(Some(unsolicited_rx)),
            shutdown_result: std::sync::OnceLock::new(),
            start_state: StdMutex::new(StartState::NotStarted),
            start_finished_notify: Notify::new(),
            shutdown_timings: ProtocolShutdownTimings::default(),
        }
    }

    /// Begin consuming ops from the input stream. Resolves when the stream ends
    /// or [`stop`](Self::stop) is called.
    pub async fn start(self: Arc<Self>) {
        let owner = {
            let mut state = self.start_state.lock().unwrap();
            match *state {
                StartState::NotStarted => {
                    *state = StartState::Running;
                    true
                }
                StartState::Running | StartState::ClosingBeforeStart | StartState::Finished => {
                    false
                }
            }
        };
        if !owner {
            // A close that won before the first start may have had its initiating
            // future cancelled.  Participating here retries that shared teardown;
            // a concurrent real owner will publish the same retained completion.
            if *self.start_state.lock().unwrap() == StartState::ClosingBeforeStart {
                let _ = self.shutdown_and_wait().await;
            }
            self.wait_for_start_finish().await;
            return;
        }
        let mut owner_guard = StartOwnerGuard {
            server: Some(self.clone()),
        };
        let inner = self.inner.clone();

        if let Some(mut rx) = self.unsolicited_rx.lock().unwrap().take() {
            let event_inner = inner.clone();
            let task = tokio::spawn(async move {
                let _finish_guard = UnsolicitedFinishGuard {
                    inner: event_inner.clone(),
                };
                let writer = async {
                    while let Some(write) = rx.recv().await {
                        #[cfg(test)]
                        {
                            let gate = event_inner.snapshot_write_test_gate.lock().unwrap().clone();
                            if let Some(gate) = gate {
                                let audit = match &write {
                                    UnsolicitedWrite::Event(event) => {
                                        event.agent_memory_snapshot_decision.clone()
                                    }
                                    _ => None,
                                };
                                gate.pause_requested(audit).await;
                            }
                        }
                        let (event, mut receipt) = write.into_event_and_receipt();
                        let result = event_inner
                            .write_unsolicited_event(
                                event,
                                receipt.as_mut().map(|receipt| &mut receipt.progress),
                            )
                            .await;
                        #[cfg(test)]
                        if result.is_ok()
                            && receipt
                                .as_ref()
                                .is_some_and(|receipt| receipt.publication.is_some())
                        {
                            let gate = event_inner
                                .session_publication_commit_test_gate
                                .lock()
                                .unwrap()
                                .clone();
                            if let Some(gate) = gate {
                                gate.pause().await;
                            }
                        }
                        if let Err(error) = &result {
                            event_inner.record_output_write_error(error.clone());
                        }
                        let write_failed = result.is_err();
                        if let Some(receipt) = receipt {
                            event_inner.complete_unsolicited_write_receipt(receipt, result);
                        }
                        if write_failed {
                            break;
                        }
                    }
                };
                if std::panic::AssertUnwindSafe(writer)
                    .catch_unwind()
                    .await
                    .is_err()
                {
                    // The loop future (including any in-progress receipt and
                    // writer-owned publication transaction) has already been
                    // dropped by catch_unwind before transport failure is
                    // recorded and finish becomes observable.
                    event_inner.record_output_write_error(
                        "protocol unsolicited writer panicked".to_string(),
                    );
                    event_inner.stop();
                }
            });
            inner
                .unsolicited_task
                .lock()
                .unwrap()
                .publish(task.abort_handle());
        }

        // The serial op-chain runner: runs one chained op at a time; a failure is
        // surfaced to the triggering op (so a client awaiting a reply rejects)
        // and swallowed so a later op still runs.
        if let Some(mut rx) = self.chain_rx.lock().unwrap().take() {
            let ci = inner.clone();
            let task = tokio::spawn(async move {
                let _finish_guard = ChainFinishGuard { inner: ci.clone() };
                while let Some(job) = rx.recv().await {
                    let ChainJob { submission_id, run } = job;
                    // Keep deep turn/lifecycle futures off the long-lived runner's stack while
                    // awaiting each task here still preserves strict one-at-a-time ordering.
                    let task = tokio::spawn(run());
                    ci.active_chain_task
                        .lock()
                        .unwrap()
                        .publish(task.abort_handle());
                    let result = match task.await {
                        Ok(result) => result,
                        Err(error) => Err(Box::new(error) as BoxError),
                    };
                    ci.active_chain_task.lock().unwrap().clear();
                    if let Err(err) = result {
                        let msg = err_message(&err);
                        ci.write(ProtocolEvent {
                            submission_id: (submission_id.clone()).into(),
                            error: Some(msg.clone().into()),
                            ..Default::default()
                        })
                        .await;
                        ci.notify(
                            NotificationLevel::Warn,
                            format!("protocol: op failed: {msg}"),
                        )
                        .await;
                    }
                }
            });
            inner
                .chain_runner_task
                .lock()
                .unwrap()
                .publish(task.abort_handle());
        }

        let input = self.input.lock().unwrap().take();
        let Some(input) = input else {
            let _ = self.shutdown_and_wait().await;
            owner_guard.disarm();
            return;
        };
        let mut reader = BufReader::new(input);
        let mut pending = PendingDispatches::default();
        let mut active: Option<tokio::task::JoinHandle<()>> = None;
        let mut goal_controls = ControlDispatches::new(
            MAX_ACTIVE_GOAL_CONTROLS,
            MAX_QUEUED_GOAL_CONTROLS,
            MAX_GOAL_CONTROL_BYTES,
        );
        let mut urgent_controls = ControlDispatches::new(
            MAX_ACTIVE_URGENT_CONTROLS,
            MAX_QUEUED_URGENT_CONTROLS,
            MAX_URGENT_CONTROL_BYTES,
        );
        // Move the sole shutdown owner after reader queues. On cancellation it
        // closes admission before queued foreground and Stop leases can notify.
        let mut owner_guard = owner_guard;
        'input: loop {
            let line = {
                // Retain a partially consumed frame when a dispatch completes:
                // restarting the read future would discard its buffered prefix.
                let reading =
                    read_protocol_line_with_limit(&mut reader, MAX_SIDE_QUESTION_LINE_BYTES);
                tokio::pin!(reading);
                loop {
                    urgent_controls.reap_finished();
                    goal_controls.reap_finished();
                    if !urgent_controls.start_queued(&inner) || !goal_controls.start_queued(&inner)
                    {
                        break 'input;
                    }
                    if active.is_none() {
                        if let Some(op) = pending.pop() {
                            active = inner.spawn_dispatch(op);
                            if active.is_none() {
                                break 'input;
                            }
                        }
                    }
                    tokio::select! {
                        biased;
                        _ = inner.shutdown.notified() => break 'input,
                        _ = async {
                            match active.as_mut() {
                                Some(task) => { let _ = task.await; }
                                None => std::future::pending::<()>().await,
                            }
                        } => {
                            active = None;
                            if inner.stopped.load(Ordering::SeqCst) { break 'input; }
                        }
                        _ = urgent_controls.wait_completion() => {},
                        _ = goal_controls.wait_completion() => {},
                        result = &mut reading => break result,
                    }
                }
            };
            match line {
                Ok(ProtocolLine::Line(line)) => {
                    let bytes = line.len();
                    let parser = inner.clone();
                    let Some(parsed) =
                        inner.spawn_dispatch_work(async move { parser.parse_line(&line).await })
                    else {
                        break;
                    };
                    let Ok(Some(op)) = parsed.await else {
                        continue;
                    };
                    if bytes > MAX_PROTOCOL_LINE_BYTES
                        && !matches!(&op.op, OpKind::SideQuestion { .. })
                    {
                        let mut error = InputValidationError::new(
                            InputValidationErrorCode::RequestTooLarge,
                            format!(
                                "protocol frame exceeds the {MAX_PROTOCOL_LINE_BYTES}-byte limit"
                            ),
                        );
                        error.detail.limit_bytes = Some(MAX_PROTOCOL_LINE_BYTES as u64);
                        inner.report_input_error("", error).await;
                        continue;
                    }
                    let kind = control_lane(&op.op);
                    if let Some(kind) = kind {
                        let lane = match kind {
                            ControlLaneKind::Goal => &mut goal_controls,
                            ControlLaneKind::Urgent => &mut urgent_controls,
                        };
                        lane.reap_finished();
                        if !lane.start_queued(&inner) {
                            break;
                        }
                        // Reject before capturing a Stop or retaining its lease.
                        // No capacity can be consumed concurrently by this reader.
                        if !lane.has_capacity(bytes) {
                            inner.report_control_overload(op, kind).await;
                            continue;
                        }
                    }
                    let op = match inner.admit_op(op) {
                        Ok(admitted) => admitted,
                        Err(rejected) => {
                            inner.report_foreground_admission_rejection(*rejected).await;
                            continue;
                        }
                    };
                    if let Some(kind) = kind {
                        let lane = match kind {
                            ControlLaneKind::Goal => &mut goal_controls,
                            ControlLaneKind::Urgent => &mut urgent_controls,
                        };
                        match lane.admit(&inner, op, bytes) {
                            ControlAdmission::Accepted => {}
                            ControlAdmission::Closed => break,
                            ControlAdmission::Full(rejected) => {
                                let AdmittedOp { op, interrupt, .. } = *rejected;
                                drop(interrupt);
                                inner.report_control_overload(op, kind).await;
                            }
                        }
                    } else if active.is_none() {
                        active = inner.spawn_dispatch(op);
                        if active.is_none() {
                            break;
                        }
                    } else if let Some(rejected_op) = pending.push(op, bytes) {
                        let AdmittedOp {
                            op,
                            foreground_guard,
                            ..
                        } = rejected_op;
                        // Release rejected foreground ownership before publishing
                        // an error, even if output is blocked by a slow client.
                        drop(foreground_guard);
                        let reporter = inner.clone();
                        let Some(rejected) = inner.spawn_dispatch_work(async move {
                            reporter.write(ProtocolEvent {
                                submission_id: op.id,
                                error: Some("protocol request queue is full; retry after the current request completes".into()),
                                ..Default::default()
                            }).await;
                        }) else { break; };
                        let _ = rejected.await;
                    }
                }
                Ok(ProtocolLine::TooLarge) => {
                    let Some(dispatch_guard) = inner.dispatches.begin() else {
                        break;
                    };
                    let task_id = dispatch_guard.id();
                    let dispatch_inner = inner.clone();
                    let task = tokio::spawn(async move {
                        let _dispatch_guard = dispatch_guard;
                        let mut error = InputValidationError::new(
                            InputValidationErrorCode::RequestTooLarge,
                            format!(
                                "protocol frame exceeds the {MAX_SIDE_QUESTION_LINE_BYTES}-byte limit"
                            ),
                        );
                        error.detail.limit_bytes = Some(MAX_SIDE_QUESTION_LINE_BYTES as u64);
                        dispatch_inner.report_input_error("", error).await;
                    });
                    inner.dispatches.attach(task_id, task.abort_handle());
                    let _ = task.await;
                }
                // A clean close or a dying pipe/socket ends this endpoint.
                Ok(ProtocolLine::Eof) | Err(_) => break,
            }
        }
        // Close all endpoint admission before retiring queued ownership. Guard
        // drops cannot schedule a fresh successor after this EOF boundary.
        self.stop();
        drop(pending);
        goal_controls.clear_pending();
        urgent_controls.clear_pending();
        // Active handles remain ActivityTracker-owned through abort and drain.
        // EOF does not resolve while persistent Session resources remain alive.
        let _ = self.shutdown_and_wait().await;
        goal_controls.reap_finished();
        urgent_controls.reap_finished();
        owner_guard.disarm();
    }

    /// Tear down: abort any running turn, fail parked approvals, stop the reader.
    pub fn stop(&self) {
        self.inner.stop();
    }

    /// Stop accepting work and wait until the serialized op-chain and the
    /// persistent Session (including MCP, LSP, background tasks, hooks, and
    /// rollout storage) have fully shut down.
    pub async fn shutdown_and_wait(self: &Arc<Self>) -> Result<(), String> {
        // Signal synchronously even when another caller already owns the async
        // completion cell.  This also makes a close before `start()` safe.
        self.stop();
        {
            let mut state = self.start_state.lock().unwrap();
            if *state == StartState::NotStarted {
                *state = StartState::ClosingBeforeStart;
            }
        }
        let mut completion = self
            .shutdown_result
            .get_or_init(|| {
                let (send, receive) = tokio::sync::watch::channel(None);
                let server = self.clone();
                // Teardown is owned by the endpoint. A cancelled first caller
                // cannot drop the FIFO drain, final ACK, or output close midway.
                tokio::spawn(async move {
                    let result = std::panic::AssertUnwindSafe(server.finish_shutdown())
                        .catch_unwind()
                        .await
                        .unwrap_or_else(|_| Err("protocol shutdown task panicked".into()));
                    let _ = send.send(Some(result));
                });
                receive
            })
            .clone();
        loop {
            if let Some(result) = completion.borrow().clone() {
                return result;
            }
            if completion.changed().await.is_err() {
                return Err("protocol shutdown task ended without a completion".into());
            }
        }
    }

    async fn finish_shutdown(&self) -> Result<(), String> {
        let started_at = tokio::time::Instant::now();
        let grace_deadline = started_at + self.shutdown_timings.grace;
        let force_deadline = started_at + self.shutdown_timings.force;
        let transport_deadline = started_at + self.shutdown_timings.transport;
        let close_deadline = started_at + self.shutdown_timings.close;
        let mut failures = ProtocolShutdownFailures::default();
        // Cleanup timeouts do not by themselves corrupt the byte stream.  A
        // timed-out provider/Host callback must still receive a correlated
        // `timedOut` completion when no output frame was interrupted.  The
        // frame guard below is the precise transport-corruption boundary.
        let mut transport_trustworthy = true;

        // If `start()` never took the receiver, there can be no runner to emit
        // the completion signal.  Drop the receiver and complete it here.  If
        // `start()` won the mutex race, its runner owns the receiver and will
        // signal after the final queued/active job settles.
        if self.chain_rx.lock().unwrap().take().is_some() {
            self.inner.mark_chain_finished();
        }
        // No Session can be missed after this point: every input-dispatched
        // immediate op admitted before stop must finish (and publish or reject
        // its session) before the retained teardown snapshot proceeds.
        if let Err(error) = self
            .inner
            .dispatches
            .shutdown_wait(grace_deadline, force_deadline, "protocol dispatch")
            .await
        {
            failures.push_failure(error);
        }
        // The unsolicited writer owns the complete BoundSession transaction.
        // Give an in-progress write the grace window; on expiry, aborting that
        // writer synchronously drops its transaction into retained cleanup
        // before the request-side receipt closes. All waits remain bounded.
        enum PendingPublicationSnapshot {
            AwaitingBoundWrite,
            Ready(Option<Arc<Session>>),
        }

        let mut forced_pending_bound_writer = false;
        let cleanup_session = loop {
            let publication = {
                let mut inflight = self
                    .inner
                    .session_publication_inflight
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if inflight
                    .as_ref()
                    .is_some_and(|publication| publication.awaiting_bound_write)
                {
                    PendingPublicationSnapshot::AwaitingBoundWrite
                } else {
                    let pending = inflight.take().map(|publication| publication.session);
                    let published = self.inner.session.get().cloned();
                    debug_assert!(published.is_none() || pending.is_none());
                    PendingPublicationSnapshot::Ready(published.or(pending))
                }
            };
            match publication {
                PendingPublicationSnapshot::AwaitingBoundWrite => {
                    let deadline = if forced_pending_bound_writer {
                        force_deadline
                    } else {
                        grace_deadline
                    };
                    match tokio::time::timeout_at(
                        deadline,
                        self.inner.session_publication_gate.lock(),
                    )
                    .await
                    {
                        Ok(publication) => {
                            drop(publication);
                            continue;
                        }
                        Err(_) if !forced_pending_bound_writer => {
                            forced_pending_bound_writer = true;
                            self.inner.unsolicited_task.lock().unwrap().force_abort();
                            continue;
                        }
                        Err(_) => {
                            failures.push_timeout(
                                "Pending BoundSession writer did not release publication ownership after force-abort",
                            );
                            let mut inflight = self
                                .inner
                                .session_publication_inflight
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner());
                            let pending = inflight.take().map(|publication| publication.session);
                            let published = self.inner.session.get().cloned();
                            debug_assert!(published.is_none() || pending.is_none());
                            break published.or(pending);
                        }
                    }
                }
                PendingPublicationSnapshot::Ready(session) => break session,
            }
        };
        // Start core cleanup in parallel with chain/readonly draining so the
        // core's 60/65/70/72/75s phases fit inside the endpoint force boundary.
        let mut core_cleanup = cleanup_session
            .map(|session| tokio::spawn(async move { session.shutdown_and_wait_detailed().await }));
        if tokio::time::timeout_at(grace_deadline, self.inner.wait_for_chain_finish())
            .await
            .is_err()
        {
            failures.push_timeout("protocol operation chain timed out during shutdown");
            self.inner.active_chain_task.lock().unwrap().force_abort();
            self.inner.chain_runner_task.lock().unwrap().force_abort();
            if tokio::time::timeout_at(force_deadline, self.inner.wait_for_chain_finish())
                .await
                .is_err()
            {
                failures.push_timeout("protocol operation chain did not stop after force-abort");
            }
        }
        self.inner.active_chain_task.lock().unwrap().clear();
        self.inner.chain_runner_task.lock().unwrap().clear();

        // A cancelled chain waiter must not cancel a durable archive restore.
        // The owned transaction keeps its tracker guard until it has either
        // committed main-last or durably recovered the archived namespace.
        if let Err(error) = self
            .inner
            .durable_lifecycle_tasks
            .shutdown_wait_unabortable(force_deadline, "protocol durable lifecycle work")
            .await
        {
            failures.push_failure(error);
        }

        if let Err(error) = self
            .inner
            .readonly_tasks
            .shutdown_wait(grace_deadline, force_deadline, "protocol readonly work")
            .await
        {
            failures.push_failure(error);
        }
        if let Some(cleanup) = core_cleanup.as_mut() {
            match tokio::time::timeout_at(force_deadline, &mut *cleanup).await {
                Ok(Ok(outcome)) => failures.extend_core(outcome),
                Ok(Err(error)) => {
                    failures.push_error(format!("core session cleanup task failed: {error}"));
                }
                Err(_) => {
                    cleanup.abort();
                    failures
                        .push_timeout("core session cleanup timed out during protocol shutdown");
                }
            }
        }
        // Core has completed its pause-before-cancellation phase, or reported
        // the bounded shutdown failure. Only now cancel the protocol parent
        // token inherited by the active model and every foreground reservation.
        self.inner.run_admission.lock().unwrap().signal.cancel();
        if let Err(error) = self
            .inner
            .completion_tasks
            .shutdown_wait(grace_deadline, force_deadline, "protocol completion work")
            .await
        {
            failures.push_failure(error);
        }

        // Maintenance and snapshot transitions enter the protocol FIFO under
        // their core event gates. Core cleanup has settled every pending
        // request, so clearing both adapters is the final publication barrier
        // before the FIFO is closed.
        if let Some(session) = self.inner.session.get() {
            session.set_ordered_memory_maintenance_event_sink(None);
            session.set_subagent_event_sink(None);
            session.set_agent_notification_sink(None);
            if tokio::time::timeout_at(
                force_deadline,
                session.set_ordered_agent_memory_snapshot_decision_sink(None),
            )
            .await
            .is_err()
            {
                failures
                    .push_timeout("snapshot event publication barrier timed out during shutdown");
            }
        }
        for (name, unsubscribe) in [
            (
                "queue",
                self.inner
                    .queue_unsubscribe
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .take(),
            ),
            (
                "task-list",
                self.inner
                    .task_list_unsubscribe
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .take(),
            ),
            (
                "background-process",
                self.inner
                    .background_process_unsubscribe
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .take(),
            ),
        ] {
            if let Some(unsubscribe) = unsubscribe {
                if std::panic::catch_unwind(std::panic::AssertUnwindSafe(unsubscribe)).is_err() {
                    failures.push_error(format!(
                        "protocol {name} subscription cleanup panicked during shutdown"
                    ));
                }
            }
        }

        // Session shutdown may synchronously publish terminal maintenance
        // lifecycle records. Keep the sink channel alive until that cleanup has
        // settled, then close and drain its single FIFO writer before ACKing the
        // shutdown submission.
        let _ = self.inner.unsolicited_tx.lock().unwrap().take();
        let local_unsolicited_rx = { self.unsolicited_rx.lock().unwrap().take() };
        if let Some(mut rx) = local_unsolicited_rx {
            let drain = async {
                // Retain completion even if the caller that initialized the
                // shared shutdown OnceCell is cancelled while draining.
                let _finish_guard = UnsolicitedFinishGuard {
                    inner: self.inner.clone(),
                };
                let writer = async {
                    while let Some(write) = rx.recv().await {
                        let (event, mut receipt) = write.into_event_and_receipt();
                        let result = self
                            .inner
                            .write_unsolicited_event(
                                event,
                                receipt.as_mut().map(|receipt| &mut receipt.progress),
                            )
                            .await;
                        #[cfg(test)]
                        if result.is_ok()
                            && receipt
                                .as_ref()
                                .is_some_and(|receipt| receipt.publication.is_some())
                        {
                            let gate = self
                                .inner
                                .session_publication_commit_test_gate
                                .lock()
                                .unwrap()
                                .clone();
                            if let Some(gate) = gate {
                                gate.pause().await;
                            }
                        }
                        if let Err(error) = &result {
                            self.inner.record_output_write_error(error.clone());
                        }
                        let write_failed = result.is_err();
                        if let Some(receipt) = receipt {
                            self.inner
                                .complete_unsolicited_write_receipt(receipt, result);
                        }
                        if write_failed {
                            break;
                        }
                    }
                };
                if std::panic::AssertUnwindSafe(writer)
                    .catch_unwind()
                    .await
                    .is_err()
                {
                    self.inner.record_output_write_error(
                        "protocol unsolicited writer panicked".to_string(),
                    );
                    self.inner.stop();
                }
            };
            if tokio::time::timeout_at(grace_deadline, drain)
                .await
                .is_err()
            {
                failures.push_timeout("protocol unsolicited event drain timed out during shutdown");
            }
        } else if tokio::time::timeout_at(grace_deadline, self.inner.wait_for_unsolicited_finish())
            .await
            .is_err()
        {
            failures.push_timeout("protocol unsolicited writer timed out during shutdown");
            self.inner.unsolicited_task.lock().unwrap().force_abort();
            if tokio::time::timeout_at(force_deadline, self.inner.wait_for_unsolicited_finish())
                .await
                .is_err()
            {
                failures.push_timeout("protocol unsolicited writer did not stop after force-abort");
            }
        }
        self.inner.unsolicited_task.lock().unwrap().clear();
        if tokio::time::timeout_at(force_deadline, async {
            *self.inner.rewind_ctl.lock().await = None;
        })
        .await
        .is_err()
        {
            failures.push_timeout("protocol rewind cleanup timed out during shutdown");
        }

        let cleanup_completed = failures.errors.is_empty();
        let output_write_error = self
            .inner
            .output_write_error
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if output_write_error.is_some() {
            // Never flush a known partial frame, or a fully written but
            // unflushed BoundSession whose retained publication transaction was
            // rolled back. Either case would make invalid bytes newly visible.
            transport_trustworthy = false;
        }
        let pre_ack_flush = if transport_trustworthy {
            match tokio::time::timeout_at(transport_deadline, self.inner.flush_output()).await {
                Ok(result) => result,
                Err(_) => {
                    let error = "protocol output flush timed out during shutdown".to_string();
                    failures.push_timeout(error.clone());
                    Err(error)
                }
            }
        } else {
            Err("protocol output is not trustworthy after an interrupted writer".into())
        };
        if pre_ack_flush.is_err() {
            transport_trustworthy = false;
        }
        let flush_completed = output_write_error.is_none() && pre_ack_flush.is_ok();
        if let Some(error) = output_write_error {
            failures.push_error(error);
        }
        if let Err(error) = &pre_ack_flush {
            if !failures.errors.iter().any(|existing| existing == error) {
                failures.push_error(error.clone());
            }
        }
        let completion = ShutdownCompletion {
            ok: cleanup_completed && flush_completed,
            flush_completed,
            cleanup_completed,
            timed_out: failures.timed_out,
            error: bounded_shutdown_error(&failures.errors),
        };

        let shutdown_submission_id = self.inner.shutdown_submission_id.lock().unwrap().clone();
        let ack_result = match (shutdown_submission_id, transport_trustworthy) {
            (Some(submission_id), true) => {
                match tokio::time::timeout_at(transport_deadline, async {
                    let mut ack_write =
                        TransactionalWriteGuard::new(self.inner.as_ref(), "Shutdown ACK");
                    self.inner
                        .write_unsolicited_event(
                            ProtocolEvent {
                                submission_id,
                                shutdown_completed: Some(Box::new(completion)),
                                ..Default::default()
                            },
                            Some(&mut ack_write.progress),
                        )
                        .await
                })
                .await
                {
                    Ok(result) => result.map_err(ProtocolShutdownFailure::error),
                    Err(_) => Err(ProtocolShutdownFailure::timeout(
                        "protocol shutdown ACK timed out",
                    )),
                }
            }
            (Some(_), false) => Err(ProtocolShutdownFailure::error(
                "protocol shutdown ACK skipped because the output stream may contain a partial frame",
            )),
            (None, _) => Ok(()),
        };
        if ack_result.is_err() {
            transport_trustworthy = false;
        }
        // Teardown signals end-of-stream for both started and never-started
        // endpoints.  This lives inside the shared completion so every caller
        // observes the same fully closed output boundary.
        let close_result = match tokio::time::timeout_at(close_deadline, async {
            self.inner.output.lock().await.take()
        })
        .await
        {
            Ok(Some(mut output)) if transport_trustworthy => {
                let result = match tokio::time::timeout_at(close_deadline, output.shutdown()).await
                {
                    Ok(result) => result.map_err(|error| {
                        ProtocolShutdownFailure::error(format!(
                            "failed to close protocol output: {error}"
                        ))
                    }),
                    Err(_) => Err(ProtocolShutdownFailure::timeout(
                        "protocol output close timed out during shutdown",
                    )),
                };
                // Dropping the owned writer is the hard EOF boundary even when
                // AsyncWrite::shutdown is broken, pending, or returns an error.
                drop(output);
                result
            }
            Ok(Some(output)) => {
                // Do not call a graceful shutdown/flush on a stream with a
                // partial or rolled-back transaction frame. Dropping the writer
                // is the only safe hard-close boundary.
                drop(output);
                Ok(())
            }
            Ok(None) => Ok(()),
            Err(_) => Err(ProtocolShutdownFailure::timeout(
                "protocol output ownership timed out during shutdown",
            )),
        };
        self.mark_start_finished();

        if let Err(error) = ack_result {
            failures.push_failure(error);
        }
        if let Err(error) = close_result {
            failures.push_failure(error);
        }
        if failures.errors.is_empty() {
            Ok(())
        } else {
            Err(failures.errors.join("; "))
        }
    }

    fn mark_start_finished(&self) {
        let mut state = self.start_state.lock().unwrap();
        if *state != StartState::Finished {
            *state = StartState::Finished;
            self.start_finished_notify.notify_waiters();
        }
    }

    async fn wait_for_start_finish(&self) {
        loop {
            let notified = self.start_finished_notify.notified();
            if *self.start_state.lock().unwrap() == StartState::Finished {
                return;
            }
            notified.await;
        }
    }
}

impl Drop for ProtocolServer {
    fn drop(&mut self) {
        // If a sole start owner is aborted, the detached chain runner otherwise
        // retains ServerInner, which itself retains the channel sender it awaits.
        self.inner.stop();
    }
}

impl ServerInner {
    fn stop(&self) {
        let _stop = self
            .stop_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.stopped.swap(true, Ordering::SeqCst) {
            return;
        }
        self.interrupt_commits.lock().unwrap().clear();
        #[cfg(test)]
        {
            let gate = self.stop_admission_test_gate.lock().unwrap().clone();
            if let Some(gate) = gate {
                gate.pause();
            }
        }
        {
            let mut admission = self.run_admission.lock().unwrap();
            admission.steer_turn_id = None;
            // The front owner may already be pursuing a goal. Core shutdown
            // pauses and flushes it before cancelling its signal. Queued owners
            // can be retired immediately without aborting that active model.
            for owner in admission.foreground.iter().skip(1) {
                owner.signal.cancel();
            }
        }
        // Serialize close admission with Pending -> Published. Either stop
        // observes the published Session or the retained inflight Session;
        // there is no set/clear gap in which shutdown can miss both.
        let (published_session, inflight_session) = {
            let inflight = self
                .session_publication_inflight
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.session_closing.store(true, Ordering::SeqCst);
            self.session_closing_notify.notify_waiters();
            (
                self.session.get().cloned(),
                inflight
                    .as_ref()
                    // The writer owns commit or rollback once it is flushing the bind.
                    // Its bounded shutdown path tears down either outcome.
                    .filter(|publication| !publication.awaiting_bound_write)
                    .map(|publication| publication.session.clone()),
            )
        };
        self.dispatches.close_admission();
        self.readonly_tasks.close_admission();
        self.completion_tasks.close_admission();
        self.durable_lifecycle_tasks.close_admission();
        self.shutdown.notify_one();
        // Close the op-chain so its runner drains and exits.
        let _ = self.chain_tx.lock().unwrap().take();
        if let Some(s) = published_session {
            // Do not clear the Session sink here: a reconnect may already have
            // replaced this server's weak sink with its own. This server keeps
            // its channel open through Session cleanup so terminal maintenance
            // lifecycle records can be drained before the shutdown ACK.
            s.initiate_shutdown();
        }
        if let Some(session) = inflight_session {
            session.initiate_shutdown();
        }
        self.cancel_readonly_requests();
        // Unblock every engine-to-Host interaction so the turn/session settles.
        self.close_pending("server stopped");
    }

    fn mark_chain_finished(&self) {
        if !self.chain_finished.swap(true, Ordering::SeqCst) {
            self.chain_finished_notify.notify_waiters();
        }
    }

    async fn wait_for_chain_finish(&self) {
        loop {
            // Register the waiter before checking the retained state so a
            // completion between the check and `.await` cannot be lost.
            let notified = self.chain_finished_notify.notified();
            if self.chain_finished.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }

    fn mark_unsolicited_finished(&self) {
        if !self.unsolicited_finished.swap(true, Ordering::SeqCst) {
            self.unsolicited_finished_notify.notify_waiters();
        }
    }

    fn record_output_write_error(&self, error: String) {
        let newly_poisoned = {
            let mut first = self
                .output_write_error
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if first.is_some() {
                false
            } else {
                *first = Some(error);
                true
            }
        };
        if newly_poisoned {
            // The byte stream has no safe continuation after a partial frame or
            // failed transactional flush. Stop is synchronous and never awaits
            // or acquires the output writer, so this is safe even when invoked
            // by OutputFrameGuard while the writer mutex is still held.
            self.stop();
        }
    }

    async fn wait_for_unsolicited_finish(&self) {
        loop {
            let notified = self.unsolicited_finished_notify.notified();
            if self.unsolicited_finished.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }

    #[cfg(test)]
    async fn pause_pending_prompt_registration_for_test(&self) {
        let gate = self
            .pending_prompt_registration_test_gate
            .lock()
            .unwrap()
            .clone();
        if let Some(gate) = gate {
            gate.pause().await;
        }
    }

    fn register_pending_approval(
        &self,
        request_id: RuntimeString,
        tx: oneshot::Sender<ApprovalResponse>,
    ) -> Result<u64, ()> {
        let mut pending = self
            .pending_prompts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if pending.closed || pending.approvals.contains_key(&request_id) {
            return Err(());
        }
        let Some(token) = pending.next_token() else {
            return Err(());
        };
        pending.approvals.insert(request_id, (token, tx));
        Ok(token)
    }

    fn register_pending_elicitation(
        &self,
        request_id: String,
        tx: oneshot::Sender<ElicitResponse>,
    ) -> Result<u64, ()> {
        let mut pending = self
            .pending_prompts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if pending.closed || pending.elicitations.contains_key(&request_id) {
            return Err(());
        }
        let Some(token) = pending.next_token() else {
            return Err(());
        };
        pending.elicitations.insert(request_id, (token, tx));
        Ok(token)
    }

    fn register_pending_mcp_authorization(
        &self,
        request_id: String,
        tx: oneshot::Sender<McpAuthorizeResponse>,
    ) -> Result<u64, ()> {
        let mut pending = self
            .pending_prompts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if pending.closed || pending.mcp_authorizations.contains_key(&request_id) {
            return Err(());
        }
        let Some(token) = pending.next_token() else {
            return Err(());
        };
        pending.mcp_authorizations.insert(request_id, (token, tx));
        Ok(token)
    }

    fn register_pending_dynamic_call(
        &self,
        call_id: RuntimeString,
        tx: oneshot::Sender<Result<ProtocolDynamicToolResponse, RuntimeString>>,
    ) -> Result<u64, RuntimeString> {
        let mut pending = self
            .pending_prompts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if pending.closed {
            return Err("dynamic tool host is stopping".into());
        }
        if pending.dynamic_calls.contains_key(&call_id) {
            return Err(
                RuntimeString::from("duplicate pending dynamic tool call id: ").concat(&call_id),
            );
        }
        let Some(token) = pending.next_token() else {
            return Err("dynamic tool pending token space is exhausted".into());
        };
        pending.dynamic_calls.insert(call_id, (token, tx));
        Ok(token)
    }

    fn cancel_pending_approval(&self, request_id: &RuntimeString, token: u64) {
        let mut pending = self
            .pending_prompts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if pending
            .approvals
            .get(request_id)
            .is_some_and(|(current, _)| *current == token)
        {
            pending.approvals.remove(request_id);
        }
    }

    fn cancel_pending_elicitation(&self, request_id: &str, token: u64) {
        let mut pending = self
            .pending_prompts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if pending
            .elicitations
            .get(request_id)
            .is_some_and(|(current, _)| *current == token)
        {
            pending.elicitations.remove(request_id);
        }
    }

    fn cancel_pending_mcp_authorization(&self, request_id: &str, token: u64) {
        let mut pending = self
            .pending_prompts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if pending
            .mcp_authorizations
            .get(request_id)
            .is_some_and(|(current, _)| *current == token)
        {
            pending.mcp_authorizations.remove(request_id);
        }
    }

    fn cancel_pending_dynamic_call(&self, call_id: &RuntimeString, token: u64) {
        let mut pending = self
            .pending_prompts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if pending
            .dynamic_calls
            .get(call_id)
            .is_some_and(|(current, _)| *current == token)
        {
            pending.dynamic_calls.remove(call_id);
        }
    }

    fn resolve_pending_approval(
        &self,
        request_id: &RuntimeString,
    ) -> Option<oneshot::Sender<ApprovalResponse>> {
        self.pending_prompts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .approvals
            .remove(request_id)
            .map(|(_, tx)| tx)
    }

    fn resolve_pending_elicitation(
        &self,
        request_id: &str,
    ) -> Option<oneshot::Sender<ElicitResponse>> {
        self.pending_prompts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .elicitations
            .remove(request_id)
            .map(|(_, tx)| tx)
    }

    fn resolve_pending_mcp_authorization(
        &self,
        request_id: &str,
    ) -> Option<oneshot::Sender<McpAuthorizeResponse>> {
        self.pending_prompts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .mcp_authorizations
            .remove(request_id)
            .map(|(_, tx)| tx)
    }

    fn resolve_pending_dynamic_call(
        &self,
        call_id: &RuntimeString,
    ) -> Option<oneshot::Sender<Result<ProtocolDynamicToolResponse, RuntimeString>>> {
        self.pending_prompts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .dynamic_calls
            .remove(call_id)
            .map(|(_, tx)| tx)
    }

    /// Deny/decline + clear every parked host interaction. Interrupt drains the
    /// current epoch but keeps admission open; endpoint stop closes it under
    /// the same mutex so a late adapter cannot insert behind the drain.
    fn fail_pending(&self, message: &str) {
        self.fail_pending_inner(message, false);
    }

    fn close_pending(&self, message: &str) {
        self.fail_pending_inner(message, true);
    }

    fn fail_pending_inner(&self, message: &str, close_prompts: bool) {
        let (approvals, elicits, authorizations, dynamic_calls) = {
            let mut pending = self
                .pending_prompts
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if close_prompts {
                pending.closed = true;
            }
            (
                pending.approvals.drain().collect::<Vec<_>>(),
                pending.elicitations.drain().collect::<Vec<_>>(),
                pending.mcp_authorizations.drain().collect::<Vec<_>>(),
                pending.dynamic_calls.drain().collect::<Vec<_>>(),
            )
        };
        for (_, (_, tx)) in approvals {
            let _ = tx.send(ApprovalResponse::Deny {
                message: Some(message.into()),
            });
        }
        for (_, (_, tx)) in elicits {
            let _ = tx.send(ElicitResponse::Decline {
                reason: Some(message.into()),
            });
        }
        for (_, (_, tx)) in authorizations {
            let _ = tx.send(McpAuthorizeResponse::Decline {
                reason: Some(message.to_string()),
            });
        }
        for (_, (_, tx)) in dynamic_calls {
            let _ = tx.send(Err(message.into()));
        }
    }

    fn begin_readonly_request(&self, request_id: &str) -> Result<Arc<ReadonlyRequest>, BoxError> {
        let mut pending = self.pending_readonly.lock().unwrap();
        if pending.contains_key(request_id) {
            return Err(format!("request {request_id} is already in progress").into());
        }
        let request = Arc::new(ReadonlyRequest {
            signal: AbortSignal::new(),
        });
        pending.insert(request_id.to_string(), request.clone());
        Ok(request)
    }

    fn begin_prediction_request(&self, request_id: &str) -> Result<Arc<ReadonlyRequest>, BoxError> {
        let mut cancelled = self.cancelled_predictions.lock().unwrap();
        cancelled.retain(|_, instant| instant.elapsed() < std::time::Duration::from_secs(300));
        if cancelled.remove(request_id).is_some() {
            return Err("prediction cancelled".into());
        }
        self.begin_readonly_request(request_id)
    }

    fn cancel_prediction(&self, request_id: &str) {
        let mut cancelled = self.cancelled_predictions.lock().unwrap();
        let pending = self.pending_readonly.lock().unwrap();
        if let Some(request) = pending.get(request_id) {
            request.signal.cancel();
            return;
        }
        cancelled.retain(|_, instant| instant.elapsed() < std::time::Duration::from_secs(300));
        if cancelled.len() >= 256 {
            if let Some(oldest) = cancelled
                .iter()
                .min_by_key(|(_, instant)| **instant)
                .map(|(id, _)| id.clone())
            {
                cancelled.remove(&oldest);
            }
        }
        cancelled.insert(request_id.to_owned(), std::time::Instant::now());
    }

    fn finish_readonly_request(&self, request_id: &str, request: &Arc<ReadonlyRequest>) {
        let mut pending = self.pending_readonly.lock().unwrap();
        if pending
            .get(request_id)
            .is_some_and(|current| Arc::ptr_eq(current, request))
        {
            pending.remove(request_id);
        }
    }

    fn cancel_readonly_request(&self, request_id: &str) {
        if let Some(request) = self
            .pending_readonly
            .lock()
            .unwrap()
            .get(request_id)
            .cloned()
        {
            request.signal.cancel();
        }
    }

    fn cancel_readonly_requests(&self) {
        self.cancelled_predictions.lock().unwrap().clear();
        let signals: Vec<_> = self
            .pending_readonly
            .lock()
            .unwrap()
            .drain()
            .map(|(_, request)| request.signal.clone())
            .collect();
        for signal in signals {
            signal.cancel();
        }
    }

    async fn parse_line(self: &Arc<Self>, line: &str) -> Option<Op> {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return None;
        }
        let value: RuntimeJson = match runtime_json::parse(&RuntimeString::from_utf8(trimmed)) {
            Ok(v) => v,
            Err(_) => {
                self.notify(
                    NotificationLevel::Warn,
                    "protocol: dropped non-JSON line".to_string(),
                )
                .await;
                return None;
            }
        };
        let submission_id = value.get("id").and_then(RuntimeJson::as_string).cloned();
        let op = match crate::types::parse_op(value) {
            Ok(op) => op,
            Err(e) => {
                if let Some(submission_id) = submission_id {
                    self.write(ProtocolEvent {
                        submission_id,
                        error: Some(format!("invalid protocol op: {e}").into()),
                        ..Default::default()
                    })
                    .await;
                } else {
                    self.notify(
                        NotificationLevel::Warn,
                        format!("protocol: invalid op: {e}"),
                    )
                    .await;
                }
                return None;
            }
        };
        Some(op)
    }

    fn admit_op(self: &Arc<Self>, op: Op) -> Result<AdmittedOp, Box<Op>> {
        let busy_guard = if matches!(
            op.op,
            OpKind::CompactSession { .. } | OpKind::ReviewSession { .. }
        ) {
            let mut admission = self.run_admission.lock().unwrap();
            if admission.non_steerable.len() >= MAX_PENDING_PROTOCOL_LINES {
                return Err(Box::new(op));
            }
            let token = uuid::Uuid::new_v4();
            admission.non_steerable.insert(
                token,
                NonSteerableReservation {
                    submission_id: op.id.try_to_utf8().unwrap_or_default(),
                    kind: if matches!(op.op, OpKind::ReviewSession { .. }) {
                        TurnSubmissionKind::Review
                    } else {
                        TurnSubmissionKind::Compact
                    },
                    active: false,
                },
            );
            Some(BusySubmissionGuard {
                inner: Arc::downgrade(self),
                token,
            })
        } else {
            None
        };
        let foreground = matches!(
            op.op,
            OpKind::Run { .. } | OpKind::ResumePending { .. } | OpKind::ResumeQueued
        );
        let (run_signal, foreground_guard) = if foreground {
            let mut admission = self.run_admission.lock().unwrap();
            // The dispatch pipe can drain into the serial chain while one model
            // turn remains active. Bound that foreground roster independently.
            if admission.foreground.len() >= MAX_PENDING_PROTOCOL_LINES {
                return Err(Box::new(op));
            }
            let signal = admission.child_token();
            if op
                .id
                .try_to_utf8()
                .ok()
                .is_some_and(|id| admission.cancelled_realtime.remove(&id).is_some())
            {
                signal.cancel();
            }
            let guard = op.id.try_to_utf8().ok().map(|submission_id| {
                let reservation_token = uuid::Uuid::new_v4();
                let was_empty = admission.foreground.is_empty();
                admission.foreground.push_back(ForegroundReservation {
                    token: reservation_token,
                    submission_id: submission_id.clone(),
                    early_steer: matches!(op.op, OpKind::Run { .. }),
                    signal: signal.clone(),
                });
                if was_empty {
                    admission.activate_front();
                    // Cover turn/start ACK-before-dispatch without letting a
                    // later queued run replace a starting resume's ownership.
                    admission.refresh_early_steer(self.stopped.load(Ordering::SeqCst));
                }
                CurrentSubmissionGuard {
                    inner: Arc::downgrade(self),
                    submission_id,
                    reservation_token,
                    goal_reschedule: true,
                    goal_decision: None,
                }
            });
            (Some(signal), guard)
        } else {
            (None, None)
        };
        if foreground {
            if let Some(goal) = self
                .session
                .get()
                .and_then(|session| session.goal_runtime())
            {
                // The idle observer can reserve work under the same admission lock.
                goal.notify_idle();
            }
        }
        let interrupt = match &op.op {
            OpKind::Interrupt {
                origin,
                expected_turn_id,
            } => {
                // Dispatch preserves native string ids in its correlated error.
                // Reject a malformed scalar id before a Stop can revoke authority.
                Some(match scalar_runtime_string(op.id.clone(), "op id") {
                    Ok(_) => self
                        .capture_interrupt(origin.unwrap_or_default(), expected_turn_id.as_ref()),
                    Err(error) => Err(error),
                })
            }
            _ => None,
        };
        Ok(AdmittedOp {
            op,
            run_signal,
            foreground_guard,
            busy_guard,
            interrupt,
        })
    }

    async fn report_control_overload(&self, op: Op, kind: ControlLaneKind) {
        let interrupt = matches!(&op.op, OpKind::Interrupt { .. });
        let id = op.id.clone();
        if interrupt {
            if let Ok(id) = id.try_to_utf8() {
                self.interrupt_commits.lock().unwrap().remove(&id);
            }
        }
        drop(op);
        let label = match kind {
            ControlLaneKind::Goal => "goal",
            ControlLaneKind::Urgent => "urgent",
        };
        self.write(ProtocolEvent {
            submission_id: id,
            turn_interrupt: interrupt.then_some(crate::types::ProtocolTurnInterruptResult {
                accepted: false,
                interrupted_turn_id: None,
                goal_pause_error: None,
            }),
            error: Some(format!("protocol {label} control capacity exceeded; retry after pending controls settle").into()),
            ..Default::default()
        }).await;
    }

    async fn report_foreground_admission_rejection(&self, op: Op) {
        self.write(ProtocolEvent {
            submission_id: op.id,
            error: Some(
                "foreground request queue is full; retry after the current turn completes".into(),
            ),
            ..Default::default()
        })
        .await;
    }

    fn spawn_dispatch(self: &Arc<Self>, op: AdmittedOp) -> Option<tokio::task::JoinHandle<()>> {
        let inner = self.clone();
        self.spawn_dispatch_work(async move { inner.dispatch_admitted(op).await })
    }

    #[cfg(test)]
    async fn dispatch(self: Arc<Self>, op: Op) {
        match self.admit_op(op) {
            Ok(admitted) => self.dispatch_admitted(admitted).await,
            Err(rejected) => self.report_foreground_admission_rejection(*rejected).await,
        }
    }

    fn spawn_dispatch_work<T: Send + 'static>(
        self: &Arc<Self>,
        work: impl Future<Output = T> + Send + 'static,
    ) -> Option<tokio::task::JoinHandle<T>> {
        let dispatch_guard = self.dispatches.begin()?;
        let task_id = dispatch_guard.id();
        let task = tokio::spawn(async move {
            let _dispatch_guard = dispatch_guard;
            work.await
        });
        self.dispatches.attach(task_id, task.abort_handle());
        Some(task)
    }

    async fn dispatch_admitted(self: Arc<Self>, admitted: AdmittedOp) {
        let AdmittedOp {
            op,
            run_signal,
            foreground_guard,
            busy_guard,
            interrupt,
        } = admitted;
        let Op { id, op } = op;
        let submission_id = match scalar_runtime_string(id.clone(), "op id") {
            Ok(id) => id,
            Err(error) => {
                self.write(ProtocolEvent {
                    submission_id: id,
                    error: Some(error.into()),
                    ..Default::default()
                })
                .await;
                return;
            }
        };
        // Initial publication has its own bounded future. Keeping it outside
        // the operation match avoids nesting both dispatch poll stack frames.
        if let OpKind::InitSession { init } = op {
            self.dispatch_init_session(submission_id, init).await;
            return;
        }
        // Which of the two dispatchers below owns this op. The split is
        // mechanical — one match was too large for a single future — so this list
        // has no meaning beyond "the handler lives in `dispatch_secondary`", and
        // it has to agree with that function arm for arm. The MCP request ops
        // (`CallMcpTool`, `ReadMcpResource`, `ListMcpPrompts`, `GetMcpPrompt`)
        // are deliberately absent: they are handled in `dispatch_primary`
        // alongside the rest of the MCP family.
        let is_secondary = matches!(
            &op,
            OpKind::ListInstalledPlugins
                | OpKind::GetPluginRuntime
                | OpKind::SetInstalledPluginEnabled { .. }
                | OpKind::UninstallPlugin { .. }
                | OpKind::InstallPlugin { .. }
                | OpKind::ListPluginCatalog { .. }
                | OpKind::GetHistory
                | OpKind::ListApprovalReviewDenials
                | OpKind::ApproveApprovalReviewDenial { .. }
                | OpKind::GetOriginalHistory
                | OpKind::GetFileChanges
                | OpKind::RewindPreview { .. }
                | OpKind::GetCost
                | OpKind::ContextSnapshot
                | OpKind::GetSubagents
                | OpKind::GetSubagentLifecycle
                | OpKind::SendSubagentMessage { .. }
                | OpKind::GetBackgroundProcesses
                | OpKind::ReadBackgroundProcessOutput { .. }
                | OpKind::StopBackgroundProcess { .. }
                | OpKind::GetMemoryMaintenance
                | OpKind::GetPlanning
                | OpKind::SetCollaborationMode { .. }
                | OpKind::ReadPlanDocument { .. }
                | OpKind::SavePlanDocument { .. }
                | OpKind::AcceptPlan { .. }
                | OpKind::PlanFeedback { .. }
                | OpKind::CancelPlanImplementation { .. }
                | OpKind::SettlePlanImplementation { .. }
                | OpKind::AnswerUserInput { .. }
                | OpKind::GetGoal
                | OpKind::SetGoal { .. }
                | OpKind::ClearGoal { .. }
                | OpKind::LoadSubagentTranscript { .. }
                | OpKind::GetQueued
                | OpKind::TakePending
                | OpKind::GetQueueSnapshot
                | OpKind::EditQueued { .. }
                | OpKind::RemoveQueued { .. }
                | OpKind::ReorderQueued { .. }
                | OpKind::RunSetup { .. }
                | OpKind::GetSetupLifecycle
                | OpKind::GetTaskList { .. }
                | OpKind::ResolveAgentMemorySnapshotDecision { .. }
                | OpKind::SettleAgentMemorySnapshotUnavailable { .. }
                | OpKind::GetAgentMemorySnapshotDecision { .. }
                | OpKind::GetAgentMemorySnapshotDecisions
                | OpKind::CompactSession { .. }
                | OpKind::SideQuestion { .. }
                | OpKind::ReviewSession { .. }
                | OpKind::DreamNow
                | OpKind::GenerateTitle { .. }
                | OpKind::PredictMessage { .. }
                | OpKind::PredictionContext
                | OpKind::ListSessions { .. }
                | OpKind::ResumeSession { .. }
                | OpKind::ForkSession { .. }
                | OpKind::ArchiveSession { .. }
                | OpKind::UnarchiveSession { .. }
                | OpKind::DeleteSession { .. }
                | OpKind::UpdateTitle { .. }
                | OpKind::RenameThread { .. }
                | OpKind::UpdateThreadHostMetadata { .. }
                | OpKind::CreateSideTask { .. }
                | OpKind::Shutdown
        ) || matches!(&op, OpKind::UpdateSessionConfig { update }
            if update.ultra_mode.is_some()
                || update.goal.is_some()
                || update.maximum_goal_token_budget.is_some()
                || update.model_verbosity.is_some()
                || update.reasoning_summary.is_some()
                || update.web_search_access.is_some()
                || update.approval_review.is_some());
        if is_secondary {
            Box::pin(self.dispatch_secondary(submission_id, op, busy_guard)).await;
        } else {
            Box::pin(self.dispatch_primary(
                submission_id,
                op,
                run_signal,
                foreground_guard,
                interrupt,
            ))
            .await;
        }
    }

    async fn dispatch_primary(
        self: &Arc<Self>,
        submission_id: String,
        op: OpKind,
        run_signal: Option<AbortSignal>,
        foreground_guard: Option<CurrentSubmissionGuard>,
        interrupt: Option<Result<InterruptAdmission, String>>,
    ) {
        macro_rules! scalar {
            ($value:expr, $name:literal) => {
                match scalar_runtime_string($value, $name) {
                    Ok(value) => value,
                    Err(error) => {
                        self.report_op_error(&submission_id, error.into()).await;
                        return;
                    }
                }
            };
        }
        macro_rules! optional_scalar {
            ($value:expr, $name:literal) => {
                match optional_scalar_runtime_string($value, $name) {
                    Ok(value) => value,
                    Err(error) => {
                        self.report_op_error(&submission_id, error.into()).await;
                        return;
                    }
                }
            };
        }
        // The guard moves into the serial job so validation, failed chain
        // admission and every early return release only this foreground identity.
        match op {
            // Serialize on the op chain: a second run waits for the first turn to
            // fully settle before touching the shared session.
            OpKind::Run { input, options } => {
                let inner = self.clone();
                let signal = run_signal.expect("foreground operation has an admission token");
                let sid = submission_id.clone();
                self.enqueue(submission_id, move || -> ChainFuture {
                    Box::pin(async move {
                        inner
                            .run_turn(
                                sid,
                                Some(input),
                                options,
                                signal,
                                foreground_guard.expect("foreground operation owns admission"),
                                false,
                            )
                            .await
                    })
                });
            }
            OpKind::ResumePending {
                after_submission_id,
            } => {
                let after = scalar!(after_submission_id, "afterSubmissionId");
                let inner = self.clone();
                let signal = run_signal.expect("foreground operation has an admission token");
                let sid = submission_id.clone();
                // Completion eligibility is published before the terminal event.
                // Reject stale probes without waiting behind an unrelated live turn.
                // Eligible requests still recheck after serial admission below.
                let reason = if signal.is_cancelled() {
                    Some("interrupted")
                } else if self.last_completed_submission.lock().unwrap().as_deref()
                    != Some(after.as_str())
                {
                    Some("not_completed")
                } else {
                    None
                };
                if let Some(reason) = reason {
                    self.finish_steer_turn(
                        foreground_guard
                            .as_ref()
                            .expect("foreground operation owns admission")
                            .reservation_token,
                    );
                    self.write_pending_resume(&sid, false, Some(reason)).await;
                    return;
                }
                self.enqueue(submission_id, move || -> ChainFuture {
                    Box::pin(async move {
                        let reason = if signal.is_cancelled() {
                            Some("interrupted")
                        } else if inner.last_completed_submission.lock().unwrap().as_deref()
                            != Some(after.as_str())
                        {
                            Some("not_completed")
                        } else {
                            None
                        };
                        if let Some(reason) = reason {
                            inner.finish_steer_turn(
                                foreground_guard
                                    .as_ref()
                                    .expect("foreground operation owns admission")
                                    .reservation_token,
                            );
                            inner.write_pending_resume(&sid, false, Some(reason)).await;
                            return Ok(());
                        }
                        inner
                            .run_turn(
                                sid,
                                None,
                                None,
                                signal,
                                foreground_guard.expect("foreground operation owns admission"),
                                false,
                            )
                            .await
                    })
                });
            }
            OpKind::ResumeQueued => {
                let inner = self.clone();
                let signal = run_signal.expect("foreground operation has an admission token");
                let sid = submission_id.clone();
                self.enqueue(submission_id, move || -> ChainFuture {
                    Box::pin(async move {
                        inner
                            .run_turn(
                                sid,
                                None,
                                None,
                                signal,
                                foreground_guard.expect("foreground operation owns admission"),
                                false,
                            )
                            .await
                    })
                });
            }
            OpKind::Interrupt { origin, .. } => {
                let commit = self
                    .interrupt_commits
                    .lock()
                    .unwrap()
                    .remove(&submission_id);
                self.dispatch_interrupt(
                    submission_id,
                    origin.unwrap_or_default(),
                    interrupt.expect("interrupt captures its arrival authority"),
                    commit,
                )
                .await;
            }
            OpKind::SetSkills { skills } => {
                let validation = validate_unique_extension_names(
                    "skill",
                    skills.iter().map(|skill| &skill.name),
                );
                if let Err(error) = validation {
                    self.report_op_error(&submission_id, error.into()).await;
                    return;
                }
                let Some(publish) = self.lifecycle.set_skills.as_ref() else {
                    self.report_op_error(
                        &submission_id,
                        "host skill publication is unavailable".into(),
                    )
                    .await;
                    return;
                };
                match self.ensure_session().await {
                    Ok(session) => match publish(session, skills.clone()) {
                        Ok(()) => {
                            {
                                let mut init = self
                                    .session_init
                                    .lock()
                                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                                init.raw.skills = skills.clone();
                                init.skills = skills;
                            }
                            self.write(ProtocolEvent {
                                submission_id: submission_id.into(),
                                skills_updated: Some(true),
                                ..Default::default()
                            })
                            .await;
                        }
                        Err(error) => self.report_op_error(&submission_id, error.into()).await,
                    },
                    Err(error) => self.report_op_error(&submission_id, error).await,
                }
            }
            OpKind::SetRuntimePolicy {
                permission_mode,
                pre_plan_permission_mode,
                mut approval_policy,
                mut sandbox_mode,
                mut approvals_reviewer,
                mut sandbox_workspace_write,
                network_allowed,
            } => {
                if pre_plan_permission_mode == Some(ExternalPermissionMode::Plan) {
                    self.report_op_error(
                        &submission_id,
                        "pre-Plan permission mode cannot itself be Plan".into(),
                    )
                    .await;
                    return;
                }
                let legacy_plan = permission_mode == Some(ExternalPermissionMode::Plan);
                if pre_plan_permission_mode.is_some() && !legacy_plan {
                    self.report_op_error(
                        &submission_id,
                        "prePlanPermissionMode is valid only for a legacy planning input".into(),
                    )
                    .await;
                    return;
                }
                if approval_policy.is_none()
                    && sandbox_mode.is_none()
                    && approvals_reviewer.is_none()
                {
                    if let Some((approval, sandbox, reviewer)) = (if legacy_plan {
                        pre_plan_permission_mode
                    } else {
                        permission_mode
                    })
                    .and_then(klaus_agent_core::schemas::config::legacy_permission_policy)
                    {
                        approval_policy = Some(approval);
                        sandbox_mode = Some(sandbox);
                        approvals_reviewer = Some(reviewer);
                    }
                }
                if let Some(table) = sandbox_workspace_write.as_mut() {
                    if let Err(message) = table.normalize_writable_roots(None) {
                        self.report_op_error(&submission_id, message.into()).await;
                        return;
                    }
                }
                if let Some(session) = self.session.get() {
                    use klaus_agent_core::engine::session::RuntimePolicyUpdate;
                    let update = RuntimePolicyUpdate {
                        approval_policy,
                        sandbox_mode,
                        approvals_reviewer,
                        sandbox_workspace_write,
                        network_allowed,
                    };
                    match session.set_runtime_policy(update) {
                        Ok(_) => {
                            if legacy_plan {
                                if let Err(message) =
                                    session.set_permission_mode(ExternalPermissionMode::Plan)
                                {
                                    self.report_op_error(&submission_id, message.into()).await;
                                    return;
                                }
                            }
                        }
                        Err(message) => {
                            self.write(ProtocolEvent {
                                submission_id: submission_id.clone().into(),
                                error: Some(message.into()),
                                ..Default::default()
                            })
                            .await;
                            return;
                        }
                    }
                } else {
                    let mut init = self
                        .session_init
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    init.raw.permission_mode = legacy_plan.then_some(ExternalPermissionMode::Plan);
                    init.raw.pre_plan_permission_mode = None;
                    if let Some(value) = network_allowed {
                        init.raw.network_allowed = Some(value);
                    }
                    let config = init
                        .raw
                        .config
                        .get_or_insert_with(|| RuntimeJson::Object(RuntimeObject::new()));
                    let fields = config
                        .as_object_mut()
                        .expect("validated session config is an object");
                    if let Some(value) = sandbox_workspace_write {
                        fields.insert(
                            "sandbox_workspace_write",
                            runtime_codec::encode_value(&value)
                                .expect("sandbox workspace encoding"),
                        );
                    }
                    if let Some(value) = approval_policy {
                        fields.insert(
                            "approval_policy",
                            runtime_codec::encode_value(&value).expect("approval policy encoding"),
                        );
                    }
                    if let Some(value) = sandbox_mode {
                        fields.insert(
                            "sandbox_mode",
                            runtime_codec::encode_value(&value).expect("sandbox mode encoding"),
                        );
                    }
                    if let Some(value) = approvals_reviewer {
                        fields.insert(
                            "approvals_reviewer",
                            runtime_codec::encode_value(&value)
                                .expect("approval reviewer encoding"),
                        );
                    }
                }
                self.write(ProtocolEvent {
                    submission_id: submission_id.into(),
                    runtime_policy_updated: Some(true),
                    ..Default::default()
                })
                .await;
            }
            OpKind::SetRuntimeModel { model } => {
                if let Some(session) = self.session.get() {
                    if let Err(error) = session.set_active_turn_model(model) {
                        self.report_op_error(&submission_id, error.into()).await;
                    }
                }
            }
            OpKind::CancelPrediction { request_id } => {
                let request_id = scalar!(request_id, "requestId");
                self.cancel_prediction(&request_id);
            }
            OpKind::CancelRequest { request_id } => {
                // requestId cancellation is scoped to detached readonly work;
                // resume/fork publication submission ids are not cancellable
                // through this operation.
                let request_id = scalar!(request_id, "requestId");
                self.cancel_readonly_request(&request_id);
            }
            OpKind::Approve {
                request_id,
                response,
            } => {
                let tx = self.resolve_pending_approval(&request_id);
                if let Some(tx) = tx {
                    let _ = tx.send(response);
                } else {
                    self.notify(
                        NotificationLevel::Warn,
                        "protocol: no pending approval for requestId".to_string(),
                    )
                    .await;
                }
            }
            OpKind::ResolveElicitation {
                request_id,
                response,
            } => {
                let request_id = scalar!(request_id, "requestId");
                let scoped = self
                    .session
                    .get()
                    .and_then(|session| session.planning_runtime().snapshot().ok())
                    .is_some_and(|snapshot| {
                        snapshot.questions.iter().any(|question| {
                            question
                                .get("requestId")
                                .and_then(RuntimeJson::as_str)
                                .is_some_and(|id| {
                                    klaus_agent_core::planning::runtime::host(id) == request_id
                                })
                                && question
                                    .get("status")
                                    .and_then(RuntimeJson::as_str)
                                    .is_some_and(|status| status == "pending")
                        })
                    });
                if scoped {
                    self.report_op_error(
                        &submission_id,
                        "Structured planning input requires its scoped answer method".into(),
                    )
                    .await;
                    return;
                }
                let tx = self.resolve_pending_elicitation(&request_id);
                if let Some(tx) = tx {
                    let response = match ElicitResponse::try_from(response) {
                        Ok(response) => response,
                        Err(error) => {
                            self.report_op_error(&submission_id, error.into()).await;
                            return;
                        }
                    };
                    let _ = tx.send(response);
                } else {
                    self.notify(
                        NotificationLevel::Warn,
                        format!("protocol: no pending elicitation for {request_id}"),
                    )
                    .await;
                }
            }
            OpKind::ResolveMcpElicitation {
                request_id,
                response,
            } => match self.ensure_session().await {
                Ok(session) => {
                    let request_id_utf8 = scalar!(request_id.clone(), "requestId");
                    let action = response.action;
                    match session.resolve_mcp_elicitation(&request_id_utf8, response) {
                        Ok(()) => {
                            self.write(ProtocolEvent {
                                submission_id: submission_id.clone().into(),
                                mcp_elicitation_resolved: Some(McpElicitationResolvedWire {
                                    request_id,
                                    action,
                                }),
                                ..Default::default()
                            })
                            .await;
                        }
                        Err(error) => self.report_op_error(&submission_id, error.into()).await,
                    }
                }
                Err(error) => self.report_op_error(&submission_id, error).await,
            },
            OpKind::ResolveMcpAuthorization {
                request_id,
                response,
            } => {
                let request_id_utf8 = scalar!(request_id.clone(), "requestId");
                let tx = self.resolve_pending_mcp_authorization(&request_id_utf8);
                let Some(tx) = tx else {
                    self.report_op_error(
                        &submission_id,
                        format!("no pending MCP authorization for {request_id_utf8}").into(),
                    )
                    .await;
                    return;
                };
                let accepted = matches!(response, McpAuthorizationResponseWire::Code { .. });
                let response = match response {
                    McpAuthorizationResponseWire::Code { code, state } => {
                        McpAuthorizeResponse::Code {
                            code: scalar!(code, "code"),
                            state: optional_scalar!(state, "state"),
                        }
                    }
                    McpAuthorizationResponseWire::Decline { reason } => {
                        McpAuthorizeResponse::Decline {
                            reason: optional_scalar!(reason, "reason"),
                        }
                    }
                };
                let _ = tx.send(response);
                self.write(ProtocolEvent {
                    submission_id: submission_id.clone().into(),
                    mcp_authorization_resolved: Some(McpAuthorizationResolvedWire {
                        request_id,
                        accepted,
                    }),
                    ..Default::default()
                })
                .await;
            }
            OpKind::GetMcpAuthorizationStatus { server_name } => {
                let server_name_utf8 = scalar!(server_name.clone(), "serverName");
                match self.ensure_session().await {
                    Ok(session) => {
                        match session.mcp_authorization_status(&server_name_utf8).await {
                            Ok(auth_status) => {
                                self.write(ProtocolEvent {
                                    submission_id: submission_id.clone().into(),
                                    mcp_authorization_status: Some(McpAuthorizationStatusWire {
                                        server_name,
                                        auth_status,
                                    }),
                                    ..Default::default()
                                })
                                .await;
                            }
                            Err(error) => self.report_op_error(&submission_id, error.into()).await,
                        }
                    }
                    Err(error) => self.report_op_error(&submission_id, error).await,
                }
            }
            OpKind::RevokeMcpAuthorization { server_name } => {
                let server_name_utf8 = scalar!(server_name.clone(), "serverName");
                match self.ensure_session().await {
                    Ok(session) => {
                        match session.revoke_mcp_authorization(&server_name_utf8).await {
                            Ok(auth_status) => {
                                self.write(ProtocolEvent {
                                    submission_id: submission_id.clone().into(),
                                    mcp_authorization_revoked: Some(McpAuthorizationRevokedWire {
                                        server_name,
                                        auth_status,
                                        revoked: true,
                                    }),
                                    ..Default::default()
                                })
                                .await;
                            }
                            Err(error) => self.report_op_error(&submission_id, error.into()).await,
                        }
                    }
                    Err(error) => self.report_op_error(&submission_id, error).await,
                }
            }
            OpKind::GetMcpRuntime { .. } => match self.ensure_session().await {
                Ok(session) => {
                    let runtime = session.mcp_runtime_snapshot().await;
                    self.report_mcp_runtime(&submission_id, Ok(runtime)).await;
                }
                Err(error) => self.report_op_error(&submission_id, error).await,
            },
            OpKind::ReconnectMcpServer { server_name } => {
                let server_name = scalar!(server_name, "serverName");
                match self.ensure_session().await {
                    Ok(session) => {
                        self.report_mcp_runtime(
                            &submission_id,
                            session.reconnect_mcp_server(&server_name).await,
                        )
                        .await;
                    }
                    Err(error) => self.report_op_error(&submission_id, error).await,
                }
            }
            OpKind::UpsertMcpServer { server } => {
                let config = match mcp_server_config_from_init(&server) {
                    Ok(config) => config,
                    Err(error) => {
                        self.report_op_error(&submission_id, error.into()).await;
                        return;
                    }
                };
                match self.ensure_session().await {
                    Ok(session) => {
                        self.report_mcp_runtime(
                            &submission_id,
                            session.upsert_mcp_server(config).await,
                        )
                        .await;
                    }
                    Err(error) => self.report_op_error(&submission_id, error).await,
                }
            }
            OpKind::RemoveMcpServer { server_name } => {
                let server_name = scalar!(server_name, "serverName");
                match self.ensure_session().await {
                    Ok(session) => {
                        self.report_mcp_runtime(
                            &submission_id,
                            session.remove_mcp_server(&server_name).await,
                        )
                        .await;
                    }
                    Err(error) => self.report_op_error(&submission_id, error).await,
                }
            }
            OpKind::ReloadMcpServers { servers } => {
                let configs = match servers
                    .iter()
                    .map(mcp_server_config_from_init)
                    .collect::<Result<Vec<_>, _>>()
                {
                    Ok(configs) => configs,
                    Err(error) => {
                        self.report_op_error(&submission_id, error.into()).await;
                        return;
                    }
                };
                match self.ensure_session().await {
                    Ok(session) => {
                        self.report_mcp_runtime(
                            &submission_id,
                            session.reload_mcp_servers(configs).await,
                        )
                        .await;
                    }
                    Err(error) => self.report_op_error(&submission_id, error).await,
                }
            }
            OpKind::CallMcpTool {
                server_name,
                tool,
                arguments,
            } => {
                let server_name = scalar!(server_name, "serverName");
                let tool = scalar!(tool, "tool");
                let tool_rs: RuntimeString = tool.as_str().into();
                let args = arguments.unwrap_or(RuntimeJson::Object(Default::default()));
                match self.ensure_session().await {
                    Ok(session) => {
                        match session
                            .host_call_mcp_tool(&server_name, &tool_rs, args)
                            .await
                        {
                            Ok(result) => {
                                // Content blocks travel as opaque JSON so the wire
                                // stays 1:1 with the server's own shape.
                                let content = runtime_codec::encode_value(&result.content)
                                    .unwrap_or(RuntimeJson::Array(Vec::new()));
                                self.write(ProtocolEvent {
                                    submission_id: submission_id.clone().into(),
                                    mcp_tool_call: Some(McpToolCallResultWire {
                                        server_name: server_name.as_str().into(),
                                        content,
                                        structured_content: result.structured_content,
                                        is_error: result.is_error,
                                    }),
                                    ..Default::default()
                                })
                                .await;
                            }
                            Err(error) => {
                                self.report_op_error(&submission_id, mcp_err(error)).await
                            }
                        }
                    }
                    Err(error) => self.report_op_error(&submission_id, error).await,
                }
            }
            OpKind::ReadMcpResource { server_name, uri } => {
                let server_name = scalar!(server_name, "serverName");
                let uri = scalar!(uri, "uri");
                let uri_rs: RuntimeString = uri.as_str().into();
                match self.ensure_session().await {
                    Ok(session) => {
                        let contents = session.host_read_mcp_resource(&server_name, &uri_rs).await;
                        self.write(ProtocolEvent {
                            submission_id: submission_id.clone().into(),
                            mcp_resource_read: Some(McpResourceReadResultWire {
                                server_name: server_name.as_str().into(),
                                contents,
                            }),
                            ..Default::default()
                        })
                        .await;
                    }
                    Err(error) => self.report_op_error(&submission_id, error).await,
                }
            }
            OpKind::ListMcpPrompts => match self.ensure_session().await {
                Ok(session) => {
                    // Sourced from the same runtime snapshot the status page reads,
                    // so the prompt inventory can never disagree with it. Servers
                    // with no prompts are dropped: an empty row builds no command.
                    let snapshot = session.mcp_runtime_snapshot().await;
                    let servers = snapshot
                        .servers
                        .into_iter()
                        .filter(|server| !server.prompts.is_empty())
                        .map(|server| McpServerPromptsWire {
                            server_name: server.name,
                            prompts: server.prompts,
                        })
                        .collect();
                    self.write(ProtocolEvent {
                        submission_id: submission_id.clone().into(),
                        mcp_prompts: Some(McpPromptsResultWire { servers }),
                        ..Default::default()
                    })
                    .await;
                }
                Err(error) => self.report_op_error(&submission_id, error).await,
            },
            OpKind::GetMcpPrompt {
                server_name,
                name,
                arguments,
            } => {
                let server_name = scalar!(server_name, "serverName");
                let name = scalar!(name, "name");
                let name_rs: RuntimeString = name.as_str().into();
                // Prompt arguments are a flat string map on the wire; a non-object
                // is treated as "no arguments" rather than rejected.
                let args = arguments.and_then(|value| value.into_object().ok());
                match self.ensure_session().await {
                    Ok(session) => {
                        match session
                            .host_get_mcp_prompt(&server_name, &name_rs, args)
                            .await
                        {
                            Ok(content) => {
                                self.write(ProtocolEvent {
                                    submission_id: submission_id.clone().into(),
                                    mcp_prompt: Some(McpPromptResultWire {
                                        server_name: server_name.as_str().into(),
                                        name: name_rs,
                                        content,
                                    }),
                                    ..Default::default()
                                })
                                .await;
                            }
                            Err(error) => {
                                self.report_op_error(&submission_id, mcp_err(error)).await
                            }
                        }
                    }
                    Err(error) => self.report_op_error(&submission_id, error).await,
                }
            }
            OpKind::GetRuntimeCapabilities => match self.ensure_session().await {
                Ok(session) => {
                    self.write(ProtocolEvent {
                        submission_id: submission_id.clone().into(),
                        runtime_capabilities: Some(
                            session.runtime_capability_snapshot_for_turn(&TurnOptions::default()),
                        ),
                        ..Default::default()
                    })
                    .await;
                }
                Err(error) => self.report_op_error(&submission_id, error).await,
            },
            OpKind::GetExtensionRuntime => match self.ensure_session().await {
                Ok(session) => {
                    self.write(ProtocolEvent {
                        submission_id: submission_id.clone().into(),
                        extension_runtime: Some(Box::new(session.extension_runtime_snapshot())),
                        ..Default::default()
                    })
                    .await;
                }
                Err(error) => self.report_op_error(&submission_id, error).await,
            },
            OpKind::ResolveDynamicToolCall { call_id, response } => {
                let tx = self.resolve_pending_dynamic_call(&call_id);
                if let Some(tx) = tx {
                    let _ = tx.send(Ok(response));
                    self.write(ProtocolEvent {
                        submission_id: submission_id.clone().into(),
                        dynamic_tool_resolved: Some(ProtocolDynamicToolResolved { call_id }),
                        ..Default::default()
                    })
                    .await;
                } else {
                    let error =
                        RuntimeString::from("no pending dynamic tool call for ").concat(&call_id);
                    self.write(ProtocolEvent {
                        submission_id: submission_id.clone().into(),
                        error: Some(error),
                        ..Default::default()
                    })
                    .await;
                    self.notify(
                        NotificationLevel::Warn,
                        "protocol: no pending dynamic tool call for callId".to_string(),
                    )
                    .await;
                }
            }
            OpKind::Rewind { num_turns, scope } => {
                let inner = self.clone();
                let sid = submission_id.clone();
                self.enqueue(submission_id, move || -> ChainFuture {
                    Box::pin(async move {
                        inner.handle_rewind(sid, num_turns, scope).await;
                        Ok(())
                    })
                });
            }
            // Steering is IMMEDIATE (not chained): it must inject into the turn
            // that's currently streaming.
            OpKind::Steer {
                input,
                priority,
                delivery,
                display_metadata,
                expected_turn_id,
                queue_message_id,
            } => {
                let expected_turn_id = optional_scalar!(expected_turn_id, "expectedTurnId");
                let queue_message_id = optional_scalar!(queue_message_id, "queueMessageId");
                if expected_turn_id.as_ref().is_some_and(String::is_empty) {
                    self.report_op_error(&submission_id, "expectedTurnId must not be empty".into())
                        .await;
                    return;
                }
                if queue_message_id.as_ref().is_some_and(|id| {
                    !uuid::Uuid::parse_str(id).is_ok_and(|uuid| uuid.to_string() == *id)
                }) {
                    self.report_op_error(
                        &submission_id,
                        "queueMessageId must be a canonical UUID".into(),
                    )
                    .await;
                    return;
                }
                let rejection = self.run_admission.lock().unwrap().steer_rejection(
                    expected_turn_id.as_deref(),
                    self.stopped.load(Ordering::SeqCst),
                );
                if let Some(error) = rejection {
                    self.report_op_error(&submission_id, error.into()).await;
                    return;
                }
                match self.ensure_session().await {
                    Ok(session) => {
                        let result = {
                            // Synchronous validation/enqueue is inside the same gate
                            // as foreground admission and terminal publication. No
                            // checked steer can slip into an already ended turn.
                            let admission = self.run_admission.lock().unwrap();
                            let rejection = admission.steer_rejection(
                                expected_turn_id.as_deref(),
                                self.stopped.load(Ordering::SeqCst),
                            );
                            match rejection {
                                Some(error) => Err(Err(error)),
                                None => session
                                    .enqueue(
                                        input,
                                        EnqueueOptions {
                                            priority: priority.map(Into::into),
                                            correlation_id: Some(submission_id.clone().into()),
                                            queue_message_id: queue_message_id.map(Into::into),
                                            delivery,
                                            display_metadata,
                                            ..Default::default()
                                        },
                                    )
                                    .map_err(Ok),
                            }
                        };
                        match result {
                            Ok(()) => {}
                            Err(Ok(error)) => self.report_input_error(&submission_id, error).await,
                            Err(Err(error)) => {
                                self.report_op_error(&submission_id, error.into()).await
                            }
                        }
                    }
                    Err(err) => self.report_op_error(&submission_id, err).await,
                }
            }
            // set_model/set_permission_mode ARE chained, so "set then run"
            // deterministically applies to the next turn.
            OpKind::AppendDisplayMessage {
                message_id,
                input,
                display_metadata,
            } => {
                let inner = self.clone();
                self.enqueue(submission_id, move || -> ChainFuture {
                    Box::pin(async move {
                        inner
                            .ensure_session()
                            .await?
                            .append_display_message(message_id, input, display_metadata)
                            .await
                            .map_err(|error| -> BoxError {
                                error.try_to_utf8().unwrap_or_default().into()
                            })?;
                        Ok(())
                    })
                });
            }
            OpKind::SetModel { model } => {
                let model = scalar!(model, "model");
                let inner = self.clone();
                self.enqueue(submission_id, move || -> ChainFuture {
                    Box::pin(async move {
                        inner.ensure_session().await?.set_model(model)?;
                        Ok(())
                    })
                });
            }
            OpKind::SetPermissionMode { mode } => {
                let inner = self.clone();
                self.enqueue(submission_id, move || -> ChainFuture {
                    Box::pin(async move {
                        inner.ensure_session().await?.set_permission_mode(mode)?;
                        Ok(())
                    })
                });
            }
            OpKind::UpdateSessionConfig { update } => {
                let inner = self.clone();
                let sid = submission_id.clone();
                self.enqueue(submission_id, move || -> ChainFuture {
                    Box::pin(async move { inner.apply_session_config(sid, update).await })
                });
            }
            // A misroute is a bug in the classifier above, but it must not be a
            // HANG: the caller is waiting for a correlated reply, and panicking
            // this task would leave it waiting for one that can never arrive.
            // Loud in debug (so the suite catches it), answerable in release.
            other => {
                debug_assert!(
                    false,
                    "secondary protocol op routed to primary dispatcher: {other:?}"
                );
                self.report_op_error(
                    &submission_id,
                    "internal error: op routed to the wrong dispatcher".into(),
                )
                .await;
            }
        }
    }

    async fn dispatch_secondary(
        self: &Arc<Self>,
        submission_id: String,
        op: OpKind,
        busy_guard: Option<BusySubmissionGuard>,
    ) {
        macro_rules! scalar {
            ($value:expr, $name:literal) => {
                match scalar_runtime_string($value, $name) {
                    Ok(value) => value,
                    Err(error) => {
                        self.report_op_error(&submission_id, error.into()).await;
                        return;
                    }
                }
            };
        }
        match op {
            OpKind::UpdateSessionConfig { update } => {
                if let Err(error) = self
                    .apply_session_config(submission_id.clone(), update)
                    .await
                {
                    self.report_op_error(&submission_id, error).await;
                }
            }
            OpKind::ListApprovalReviewDenials => match self.ensure_session().await {
                Ok(session) => {
                    self.write(ProtocolEvent {
                        submission_id: submission_id.into(),
                        approval_review_denials: Some(session.list_approval_review_denials()),
                        ..Default::default()
                    })
                    .await
                }
                Err(error) => self.report_op_error(&submission_id, error).await,
            },
            OpKind::ApproveApprovalReviewDenial { event } => match self.ensure_session().await {
                Ok(session) => match session.approve_approval_review_denial(event).await {
                    Ok(()) => {
                        self.write(ProtocolEvent {
                            submission_id: submission_id.into(),
                            approval_review_denial_approved: Some(true),
                            ..Default::default()
                        })
                        .await
                    }
                    Err(error) => {
                        self.write(ProtocolEvent {
                            submission_id: submission_id.into(),
                            error: Some(error),
                            ..Default::default()
                        })
                        .await
                    }
                },
                Err(error) => self.report_op_error(&submission_id, error).await,
            },
            OpKind::ListInstalledPlugins => {
                self.dispatch_plugin_admin(submission_id, PluginAdminOp::List)
                    .await;
            }
            OpKind::GetPluginRuntime => {
                self.dispatch_plugin_admin(submission_id, PluginAdminOp::Runtime)
                    .await;
            }
            OpKind::SetInstalledPluginEnabled { name, enabled } => {
                let name = scalar!(name, "name");
                self.dispatch_plugin_admin(
                    submission_id,
                    PluginAdminOp::SetEnabled { name, enabled },
                )
                .await;
            }
            OpKind::UninstallPlugin { name, remove_files } => {
                let name = scalar!(name, "name");
                self.dispatch_plugin_admin(
                    submission_id,
                    PluginAdminOp::Uninstall { name, remove_files },
                )
                .await;
            }
            OpKind::InstallPlugin {
                marketplace,
                source_dir,
                version,
                expected_installed_version,
            } => {
                let marketplace = scalar!(marketplace, "marketplace");
                let source_dir = scalar!(source_dir, "sourceDir");
                let version = match version {
                    Some(v) => Some(scalar!(v, "version")),
                    None => None,
                };
                let expected_installed_version = match expected_installed_version {
                    Some(value) => Some(scalar!(value, "expectedInstalledVersion")),
                    None => None,
                };
                self.dispatch_plugin_admin(
                    submission_id,
                    PluginAdminOp::Install {
                        marketplace,
                        source_dir,
                        version,
                        expected_installed_version,
                    },
                )
                .await;
            }
            OpKind::ListPluginCatalog { roots } => {
                self.dispatch_plugin_admin(submission_id, PluginAdminOp::Catalog { roots })
                    .await;
            }
            OpKind::GetHistory => {
                Box::pin(self.dispatch_get_history(submission_id, false)).await;
            }
            OpKind::GetOriginalHistory => {
                Box::pin(self.dispatch_get_history(submission_id, true)).await;
            }
            OpKind::GetFileChanges => {
                Box::pin(self.dispatch_get_file_changes(submission_id)).await;
            }
            OpKind::RewindPreview { num_turns } => {
                Box::pin(self.dispatch_rewind_preview(submission_id, num_turns)).await;
            }
            OpKind::GetCost => {
                Box::pin(self.dispatch_get_cost(submission_id)).await;
            }
            OpKind::ContextSnapshot => {
                Box::pin(self.dispatch_context_snapshot(submission_id)).await;
            }
            OpKind::GetSubagents => {
                Box::pin(self.dispatch_get_subagents(submission_id)).await;
            }
            OpKind::GetSubagentLifecycle => {
                Box::pin(self.dispatch_get_subagent_lifecycle(submission_id)).await;
            }
            OpKind::SendSubagentMessage {
                agent_id,
                message,
                summary,
                user_initiated,
            } => {
                Box::pin(self.dispatch_send_subagent_message(
                    submission_id,
                    agent_id,
                    message,
                    summary,
                    user_initiated,
                ))
                .await;
            }
            OpKind::GetBackgroundProcesses => {
                Box::pin(self.dispatch_get_background_processes(submission_id)).await;
            }
            OpKind::ReadBackgroundProcessOutput {
                shell_id,
                after_offset,
                max_bytes,
            } => {
                Box::pin(self.dispatch_read_background_process_output(
                    submission_id,
                    shell_id,
                    after_offset,
                    max_bytes,
                ))
                .await;
            }
            OpKind::StopBackgroundProcess { shell_id, signal } => {
                Box::pin(self.dispatch_stop_background_process(submission_id, shell_id, signal))
                    .await;
            }
            OpKind::GetMemoryMaintenance => {
                Box::pin(self.dispatch_get_memory_maintenance(submission_id)).await;
            }
            op @ (OpKind::GetPlanning
            | OpKind::SetCollaborationMode { .. }
            | OpKind::ReadPlanDocument { .. }
            | OpKind::SavePlanDocument { .. }
            | OpKind::AcceptPlan { .. }
            | OpKind::PlanFeedback { .. }
            | OpKind::CancelPlanImplementation { .. }
            | OpKind::SettlePlanImplementation { .. }
            | OpKind::AnswerUserInput { .. }) => {
                Box::pin(self.dispatch_planning(submission_id, op)).await;
            }
            OpKind::GetGoal => {
                Box::pin(self.dispatch_get_goal(submission_id)).await;
            }
            OpKind::SetGoal {
                owned_resources,
                objective,
                status,
                token_budget,
                expected_goal_id,
                expected_goal_revision,
                origin,
                submit,
                display_text,
                client_request_id,
            } => {
                Box::pin(self.dispatch_set_goal(
                    submission_id,
                    owned_resources,
                    objective,
                    status,
                    token_budget,
                    expected_goal_id,
                    expected_goal_revision,
                    origin,
                    submit,
                    display_text,
                    client_request_id,
                ))
                .await;
            }
            OpKind::ClearGoal {
                expected_goal_id,
                expected_goal_revision,
                origin,
            } => {
                Box::pin(self.dispatch_clear_goal(
                    submission_id,
                    expected_goal_id,
                    expected_goal_revision,
                    origin,
                ))
                .await;
            }
            OpKind::LoadSubagentTranscript {
                thread_id,
                agent_id,
            } => {
                Box::pin(self.dispatch_load_subagent_transcript(
                    submission_id,
                    thread_id,
                    agent_id,
                ))
                .await;
            }
            OpKind::GetQueued => {
                Box::pin(self.dispatch_get_queued(submission_id)).await;
            }
            OpKind::TakePending => {
                Box::pin(self.dispatch_take_pending(submission_id)).await;
            }
            OpKind::GetQueueSnapshot => {
                Box::pin(self.dispatch_get_queue_snapshot(submission_id)).await;
            }
            OpKind::EditQueued {
                mutation_id,
                uuid,
                expected_revision,
                edit,
            } => {
                Box::pin(self.dispatch_edit_queued(
                    submission_id,
                    mutation_id,
                    uuid,
                    expected_revision,
                    edit,
                ))
                .await;
            }
            OpKind::RemoveQueued {
                mutation_id,
                uuid,
                expected_revision,
            } => {
                Box::pin(self.dispatch_remove_queued(
                    submission_id,
                    mutation_id,
                    uuid,
                    expected_revision,
                ))
                .await;
            }
            OpKind::ReorderQueued {
                mutation_id,
                uuid,
                before_uuid,
                expected_revision,
            } => {
                Box::pin(self.dispatch_reorder_queued(
                    submission_id,
                    mutation_id,
                    uuid,
                    before_uuid,
                    expected_revision,
                ))
                .await;
            }
            OpKind::RunSetup { request } => {
                Box::pin(self.dispatch_run_setup(submission_id, request)).await;
            }
            OpKind::GetSetupLifecycle => {
                Box::pin(self.dispatch_get_setup_lifecycle(submission_id)).await;
            }
            OpKind::GetTaskList { options } => {
                Box::pin(self.dispatch_get_task_list(submission_id, options)).await;
            }
            OpKind::ResolveAgentMemorySnapshotDecision {
                request_id,
                decision,
            } => {
                Box::pin(self.dispatch_resolve_agent_memory_snapshot_decision(
                    submission_id,
                    request_id,
                    decision,
                ))
                .await;
            }
            OpKind::SettleAgentMemorySnapshotUnavailable {
                request_id,
                reason,
                error,
            } => {
                Box::pin(self.dispatch_settle_agent_memory_snapshot_unavailable(
                    submission_id,
                    request_id,
                    reason,
                    error,
                ))
                .await;
            }
            OpKind::GetAgentMemorySnapshotDecision { request_id } => {
                Box::pin(
                    self.dispatch_get_agent_memory_snapshot_decision(submission_id, request_id),
                )
                .await;
            }
            OpKind::GetAgentMemorySnapshotDecisions => {
                Box::pin(self.dispatch_get_agent_memory_snapshot_decisions(submission_id)).await;
            }
            OpKind::CompactSession {
                instructions,
                selection,
            } => {
                Box::pin(self.dispatch_compact_session(
                    submission_id,
                    instructions,
                    selection,
                    busy_guard,
                ))
                .await;
            }
            OpKind::SideQuestion { question } => {
                Box::pin(self.dispatch_side_question(submission_id, question)).await;
            }
            OpKind::ReviewSession {
                target,
                user_facing_hint,
            } => {
                Box::pin(self.dispatch_review_session(
                    submission_id,
                    target,
                    user_facing_hint,
                    busy_guard,
                ))
                .await;
            }
            OpKind::DreamNow => {
                Box::pin(self.dispatch_dream_now(submission_id)).await;
            }
            OpKind::PredictionContext => match self.ensure_session().await {
                Ok(session) => match session.prediction_context().await {
                    Ok(context) => {
                        self.write(ProtocolEvent {
                            submission_id: submission_id.into(),
                            prediction_context: Some(context),
                            ..Default::default()
                        })
                        .await
                    }
                    Err(error) => self.report_op_error(&submission_id, error.into()).await,
                },
                Err(error) => self.report_op_error(&submission_id, error).await,
            },
            OpKind::PredictMessage {
                prompt,
                source_turn_id,
                effort,
            } => {
                Box::pin(self.dispatch_predict_message(
                    submission_id,
                    prompt,
                    source_turn_id,
                    effort,
                ))
                .await;
            }
            OpKind::GenerateTitle {
                instructions,
                prompt,
                persist,
            } => {
                Box::pin(self.dispatch_generate_title(
                    submission_id,
                    instructions,
                    prompt,
                    persist,
                ))
                .await;
            }
            OpKind::ListSessions { options } => {
                Box::pin(self.dispatch_list_sessions(submission_id, options)).await;
            }
            OpKind::CreateSideTask {
                thread_id,
                new_thread_id,
            } => {
                Box::pin(self.dispatch_create_side_task(submission_id, thread_id, new_thread_id))
                    .await;
            }
            OpKind::ResumeSession { thread_id } => {
                Box::pin(self.dispatch_resume_session(submission_id, thread_id)).await;
            }
            OpKind::ForkSession {
                thread_id,
                new_thread_id,
                before_message_uuid,
                inherit_goal,
            } => {
                Box::pin(self.dispatch_fork_session(
                    submission_id,
                    thread_id,
                    new_thread_id,
                    before_message_uuid,
                    inherit_goal,
                ))
                .await;
            }
            OpKind::ArchiveSession { thread_id } => {
                Box::pin(self.dispatch_archive_session(submission_id, thread_id)).await;
            }
            OpKind::UnarchiveSession { thread_id } => {
                Box::pin(self.dispatch_unarchive_session(submission_id, thread_id)).await;
            }
            OpKind::DeleteSession { thread_id } => {
                Box::pin(self.dispatch_delete_session(submission_id, thread_id)).await;
            }
            OpKind::UpdateTitle { thread_id, title } => {
                Box::pin(self.dispatch_update_title(submission_id, thread_id, title)).await;
            }
            OpKind::RenameThread {
                thread_id,
                new_thread_id,
            } => {
                Box::pin(self.dispatch_rename_thread(submission_id, thread_id, new_thread_id))
                    .await;
            }
            OpKind::UpdateThreadHostMetadata {
                thread_id,
                host_metadata,
            } => {
                Box::pin(self.dispatch_update_thread_host_metadata(
                    submission_id,
                    thread_id,
                    host_metadata,
                ))
                .await;
            }
            OpKind::Shutdown => {
                Box::pin(self.dispatch_shutdown(submission_id)).await;
            }
            // See the matching arm in `dispatch_primary`.
            other => {
                debug_assert!(
                    false,
                    "primary protocol op routed to secondary dispatcher: {other:?}"
                );
                self.report_op_error(
                    &submission_id,
                    "internal error: op routed to the wrong dispatcher".into(),
                )
                .await;
            }
        }
    }

    /// Publish history at one ordered output boundary. The original-history
    /// variant flushes the rollout before reading it and keeps the output lease
    /// until the terminal reply is written. Live events generated during the
    /// read therefore follow the snapshot's exact stream revision.
    async fn dispatch_get_history(
        self: &Arc<Self>,
        submission_id: String,
        include_original_history: bool,
    ) {
        let session = match self.ensure_session().await {
            Ok(session) => session,
            Err(error) => {
                self.report_op_error(&submission_id, error).await;
                return;
            }
        };
        let mut output = self.output.lock().await;
        let snapshot = async {
            if include_original_history && self.lifecycle.read_thread.is_none() {
                return Err(
                    "complete original history requires a read-only session store".to_string(),
                );
            }
            // Output ordering bounds notifications; the accepted-write fence
            // also bounds the independent query producer and compaction.
            for _ in 0..8 {
                let revision = if include_original_history {
                    Some(session.rollout_inspection_revision().ok_or(
                        "complete original history requires a rollout inspection revision",
                    )?)
                } else {
                    None
                };
                let mut original = if include_original_history {
                    let read =
                        self.lifecycle.read_thread.as_ref().ok_or(
                            "complete original history requires a read-only session store",
                        )?;
                    session.flush_rollout().await?;
                    let thread_id = session
                        .id
                        .try_to_utf8()
                        .map_err(|error| error.to_string())?;
                    let snapshot = read(thread_id.clone())
                        .await
                        .map_err(|error| error.to_string())?;
                    if snapshot.meta.id != thread_id {
                        return Err("complete original history identity mismatch".to_string());
                    }
                    Some(
                        klaus_agent_core::store::thread_history::project_thread_history(
                            snapshot.raw_items,
                        ),
                    )
                } else {
                    None
                };
                let index = session.turn_index_snapshot();
                let visible = session.visible_history();
                if include_original_history && session.rollout_inspection_revision() != revision {
                    continue;
                }
                let history = if let Some(original) = &mut original {
                    let mut seen: HashSet<RuntimeString> = original
                        .history
                        .iter()
                        .map(message_uuid_for_history)
                        .collect();
                    for message in visible {
                        let id = message_uuid_for_history(&message);
                        // Structured inspection output is separate from Message's
                        // persistence codec, including newly appended live rows.
                        if let Message::ToolResult(result) = &message {
                            if let Some(output) = &result.output {
                                original.tool_outputs.insert(id.clone(), output.clone());
                            }
                        }
                        if seen.contains(&id) {
                            continue;
                        }
                        // Compaction summaries are provider prompt context. Only
                        // real turn-owned rows supplement the original transcript.
                        if matches!(message, Message::CompactBoundary(_))
                            || !index.message_turn.contains_key(&id)
                        {
                            continue;
                        }
                        seen.insert(id);
                        original.history.push(message);
                    }
                    std::mem::take(&mut original.history)
                } else {
                    visible
                };
                let history_outputs = original
                    .as_mut()
                    .map(|snapshot| std::mem::take(&mut snapshot.tool_outputs))
                    .unwrap_or_default();
                let mut merged = original
                    .map(|snapshot| snapshot.turn_index)
                    .unwrap_or_default();
                merged.merge_live(index);
                let error_details = merged.error_details_snapshot();
                return Ok::<_, String>((
                    ProtocolEvent {
                        submission_id: submission_id.clone().into(),
                        history: Some(history),
                        approval_reviews: Some(session.approval_review_history()),
                        goal_completions: Some(session.goal_completions()),
                        turn_goal_metadata: Some(merged.turn_goal_metadata.into_iter().collect()),
                        agent_notifications: Some(merged.agent_notifications),
                        history_turn_ids: Some(merged.message_turn.into_iter().collect()),
                        turn_final_message_ids: Some(
                            merged.turn_final_message.into_iter().collect(),
                        ),
                        turn_started_at_ms: Some(merged.turn_started_at_ms.into_iter().collect()),
                        turn_finished_at_ms: Some(merged.turn_finished_at_ms.into_iter().collect()),
                        turn_statuses: Some(merged.turn_status.into_iter().collect()),
                        turn_durations: Some(merged.turn_duration_ms.into_iter().collect()),
                        turn_errors: Some(merged.turn_errors.into_iter().collect()),
                        turn_error_kinds: Some(merged.turn_error_kinds.into_iter().collect()),
                        turn_error_details: (!error_details.is_empty()).then_some(error_details),
                        turn_cancelled_tools: Some(
                            merged.turn_cancelled_tools.into_iter().collect(),
                        ),
                        ..Default::default()
                    },
                    history_outputs,
                ));
            }
            Err("complete original history remained busy during capture; retry follow".to_string())
        }
        .await;
        let (event, history_outputs) = snapshot.unwrap_or_else(|error| {
            (
                ProtocolEvent {
                    submission_id: submission_id.into(),
                    error: Some(error.into()),
                    ..Default::default()
                },
                HashMap::new(),
            )
        });
        if self.stopped.load(Ordering::SeqCst) {
            return;
        }
        if let Err(error) = self
            .write_locked_history_event(&mut output, event, &history_outputs)
            .await
        {
            self.record_output_write_error(error);
        }
    }

    async fn apply_session_config(
        self: &Arc<Self>,
        submission_id: String,
        update: ProtocolSessionConfigUpdate,
    ) -> Result<(), BoxError> {
        validate_session_config_update(&update)?;
        let model = optional_scalar_runtime_string(update.model.clone(), "session model")?;
        let session = self.ensure_session().await?;
        if let Some(enabled) = update.ultra_mode {
            session.set_ultra_mode(enabled)?;
        }
        if let Some(value) = &update.approval_review {
            session.set_runtime_approval_review(value.clone())?;
        }
        let runtime_settings = session.set_runtime_inference_settings(
            update.model_verbosity,
            update.reasoning_summary,
            update.web_search_access,
        )?;
        if let Some(maximum) = update.maximum_goal_token_budget {
            session
                .set_maximum_goal_token_budget(maximum)
                .await
                .map_err(|error| error.try_to_utf8().unwrap_or_default())?;
        }
        if let Some(enabled) = update.goal {
            session
                .set_goal_enabled(enabled)
                .await
                .map_err(|error| error.try_to_utf8().unwrap_or_default())?;
        }
        if let Some(model) = model {
            session.set_model(model)?;
        }
        if let Some(mode) = update.permission_mode {
            session.set_permission_mode(mode)?;
        }
        let subagent_model_settings = session.set_subagent_model_settings(
            update.subagent_model_catalog,
            update.subagent_model_options.clone(),
        )?;
        self.write(ProtocolEvent {
            submission_id: submission_id.into(),
            session_config: Some(ProtocolSessionConfigUpdateResult {
                subagent_model_catalog: update
                    .subagent_model_catalog
                    .map(|_| subagent_model_settings.0),
                subagent_model_options: update
                    .subagent_model_options
                    .map(|_| subagent_model_settings.1),
                ultra_mode: update.ultra_mode.map(|_| session.ultra_mode()),
                goal: update.goal.map(|_| session.goal_enabled()),
                maximum_goal_token_budget: update
                    .maximum_goal_token_budget
                    .map(|_| session.maximum_goal_token_budget()),
                model: update.model,
                permission_mode: None,
                model_verbosity: update
                    .model_verbosity
                    .map(|_| runtime_settings.model_verbosity),
                reasoning_summary: update
                    .reasoning_summary
                    .map(|_| runtime_settings.reasoning_summary),
                web_search_access: update
                    .web_search_access
                    .map(|_| runtime_settings.web_search_access),
                approval_review: update
                    .approval_review
                    .map(|_| session.runtime_approval_review()),
            }),
            ..Default::default()
        })
        .await;
        if update.goal == Some(true) {
            self.schedule_goal_idle();
        }
        Ok(())
    }

    /// `GetFileChanges`: publish what each of this session's turns changed on disk.
    async fn dispatch_get_file_changes(self: &Arc<Self>, submission_id: String) {
        match self.ensure_session().await {
            Ok(session) => {
                let changes = session.file_changes_snapshot();
                self.write(ProtocolEvent {
                    submission_id: submission_id.clone().into(),
                    file_changes: Some(
                        changes
                            .turns
                            .into_iter()
                            .map(|turn| ProtocolSessionTurnFileChanges {
                                turn_id: turn.turn_id,
                                files: turn
                                    .files
                                    .into_iter()
                                    .map(|file| ProtocolSessionFileChange {
                                        turn_diff: file.turn_diff,
                                        path: file.path,
                                        kind: file.kind,
                                        added: file.added,
                                        removed: file.removed,
                                    })
                                    .collect(),
                            })
                            .collect(),
                    ),
                    ..Default::default()
                })
                .await
            }
            Err(err) => self.report_op_error(&submission_id, err).await,
        }
    }

    /// `RewindPreview`: publish what a rewind WOULD do, having done nothing.
    ///
    /// No bound controller answers `canRewind: false` with a reason rather than an
    /// op error: to a host the two cases mean the same thing — do not offer the
    /// undo — and one shape is easier to render than two.
    async fn dispatch_rewind_preview(self: &Arc<Self>, submission_id: String, num_turns: u32) {
        let guard = self.rewind_ctl.lock().await;
        let preview = match guard.as_ref() {
            None => RewindPreview {
                can_rewind: false,
                files: Vec::new(),
                dropped_turns: 0,
                error: Some("no session to rewind".into()),
            },
            Some(binding) => binding.controller.preview(num_turns as f64).await,
        };
        self.write(ProtocolEvent {
            submission_id: submission_id.clone().into(),
            rewind_preview: Some(preview),
            ..Default::default()
        })
        .await;
    }

    /// `GetCost`: publish the cost tracker snapshot.
    async fn dispatch_get_cost(self: &Arc<Self>, submission_id: String) {
        match self.ensure_session().await {
            Ok(session) => {
                self.write(ProtocolEvent {
                    submission_id: submission_id.clone().into(),
                    cost: Some(session.cost_snapshot()),
                    ..Default::default()
                })
                .await
            }
            Err(err) => self.report_op_error(&submission_id, err).await,
        }
    }

    /// `ContextSnapshot`: publish the context window usage snapshot.
    async fn dispatch_context_snapshot(self: &Arc<Self>, submission_id: String) {
        match self.ensure_session().await {
            Ok(session) => {
                self.write(ProtocolEvent {
                    submission_id: submission_id.clone().into(),
                    context: Some(session.context_snapshot()),
                    ..Default::default()
                })
                .await
            }
            Err(err) => self.report_op_error(&submission_id, err).await,
        }
    }

    /// `GetSubagents`: publish the background agent summaries.
    async fn dispatch_get_subagents(self: &Arc<Self>, submission_id: String) {
        match self.ensure_session().await {
            Ok(session) => {
                let subs: Vec<BackgroundAgentSummary> = session
                    .background_agent_summaries()
                    .into_iter()
                    .map(Into::into)
                    .collect();
                self.write(ProtocolEvent {
                    submission_id: submission_id.clone().into(),
                    subagents: Some(subs),
                    ..Default::default()
                })
                .await
            }
            Err(err) => self.report_op_error(&submission_id, err).await,
        }
    }

    /// `GetSubagentLifecycle`: publish the subagent lifecycle snapshot.
    async fn dispatch_get_subagent_lifecycle(self: &Arc<Self>, submission_id: String) {
        match self.ensure_session().await {
            Ok(session) => {
                self.write(ProtocolEvent {
                    submission_id: submission_id.clone().into(),
                    subagent_lifecycle: Some(session.subagent_lifecycle_snapshot()),
                    ..Default::default()
                })
                .await
            }
            Err(err) => self.report_op_error(&submission_id, err).await,
        }
    }

    async fn dispatch_send_subagent_message(
        self: &Arc<Self>,
        submission_id: String,
        agent_id: RuntimeString,
        message: RuntimeString,
        summary: Option<RuntimeString>,
        user_initiated: bool,
    ) {
        let session = match self.ensure_session().await {
            Ok(session) => session,
            Err(error) => {
                self.report_subagent_message_error(&submission_id, err_message(&error))
                    .await;
                return;
            }
        };
        let summary = summary.unwrap_or_else(|| message_preview(&message));
        if let Err(error) = validate_text_message(&agent_id, &message, &summary) {
            self.report_subagent_message_error(
                &submission_id,
                String::from_utf8(error.to_node_utf8())
                    .expect("message validation diagnostics are valid UTF-8"),
            )
            .await;
            return;
        }
        let request = SubagentMessageRequest {
            trigger_turn: true,
            target: agent_id,
            message,
            summary,
            sender_agent_id: None,
            user_initiated,
        };
        match session.send_subagent_message(request).await {
            Some(Ok(delivery)) => {
                let mut reply = RuntimeObject::new();
                reply.insert("ok", RuntimeJson::Bool(true));
                reply.insert("agentId", RuntimeJson::String(delivery.agent_id));
                reply.insert("resumed", RuntimeJson::Bool(delivery.resumed));
                self.write(ProtocolEvent {
                    submission_id: submission_id.into(),
                    subagent_message: Some(RuntimeJson::Object(reply)),
                    ..Default::default()
                })
                .await;
            }
            Some(Err(error)) => {
                self.report_subagent_message_error(
                    &submission_id,
                    String::from_utf8(error.to_node_utf8())
                        .expect("subagent diagnostics are valid UTF-8"),
                )
                .await;
            }
            None => {
                self.report_subagent_message_error(
                    &submission_id,
                    "No subagent matches the requested recipient.".to_owned(),
                )
                .await
            }
        }
    }

    async fn report_subagent_message_error(&self, submission_id: &str, message: String) {
        let mut detail = RuntimeObject::new();
        detail.insert("code", RuntimeJson::from("subagent_message_rejected"));
        detail.insert("feature", RuntimeJson::from("subagent_message"));
        self.write(ProtocolEvent {
            submission_id: submission_id.to_owned().into(),
            error: Some(message.clone().into()),
            error_detail: Some(ProtocolErrorDetail::Unknown(RuntimeJson::Object(detail))),
            ..Default::default()
        })
        .await;
        self.notify(
            NotificationLevel::Warn,
            format!("protocol: child input rejected: {message}"),
        )
        .await;
    }

    async fn dispatch_get_background_processes(self: &Arc<Self>, submission_id: String) {
        match self.ensure_session().await {
            Ok(session) => {
                self.write(ProtocolEvent {
                    submission_id: submission_id.into(),
                    background_processes: Some(session.background_process_snapshot()),
                    ..Default::default()
                })
                .await;
            }
            Err(error) => self.report_op_error(&submission_id, error).await,
        }
    }

    async fn dispatch_read_background_process_output(
        self: &Arc<Self>,
        submission_id: String,
        shell_id: RuntimeString,
        after_offset: Option<u64>,
        max_bytes: Option<usize>,
    ) {
        match self.ensure_session().await {
            Ok(session) => {
                self.write(ProtocolEvent {
                    submission_id: submission_id.into(),
                    background_process_output: Some(session.read_background_process_output(
                        &shell_id,
                        after_offset,
                        max_bytes,
                    )),
                    ..Default::default()
                })
                .await;
            }
            Err(error) => self.report_op_error(&submission_id, error).await,
        }
    }

    async fn dispatch_stop_background_process(
        self: &Arc<Self>,
        submission_id: String,
        shell_id: RuntimeString,
        signal: Option<RuntimeString>,
    ) {
        let signal = match signal.as_ref().map(RuntimeString::as_utf16) {
            None => klaus_agent_core::tools::shell::Signal::Term,
            Some(value) if value == RuntimeString::from("SIGTERM").as_utf16() => {
                klaus_agent_core::tools::shell::Signal::Term
            }
            Some(value) if value == RuntimeString::from("SIGKILL").as_utf16() => {
                klaus_agent_core::tools::shell::Signal::Kill
            }
            _ => {
                self.report_op_error(&submission_id, "signal must be SIGTERM or SIGKILL".into())
                    .await;
                return;
            }
        };
        match self.ensure_session().await {
            Ok(session) => match session
                .stop_background_process_and_wait(&shell_id, signal)
                .await
            {
                Ok(result) => {
                    self.write(ProtocolEvent {
                        submission_id: submission_id.into(),
                        background_process_stop: Some(result),
                        ..Default::default()
                    })
                    .await;
                }
                Err(error) => self.report_op_error(&submission_id, mcp_err(error)).await,
            },
            Err(error) => self.report_op_error(&submission_id, error).await,
        }
    }

    /// `RunSetup`: chain a setup run onto the serial op chain.
    async fn dispatch_run_setup(
        self: &Arc<Self>,
        submission_id: String,
        mut request: crate::types::SetupRunRequest,
    ) {
        macro_rules! scalar {
            ($value:expr, $name:literal) => {
                match scalar_runtime_string($value, $name) {
                    Ok(value) => value,
                    Err(error) => {
                        self.report_op_error(&submission_id, error.into()).await;
                        return;
                    }
                }
            };
        }
        let request_id = scalar!(request.request_id, "requestId");
        request.request_id = request_id.into();
        let inner = self.clone();
        let sid = submission_id.clone();
        self.enqueue(submission_id, move || -> ChainFuture {
            Box::pin(async move { inner.handle_run_setup(sid, request).await })
        });
    }

    /// `GetSetupLifecycle`: publish the setup lifecycle snapshot.
    async fn dispatch_get_setup_lifecycle(self: &Arc<Self>, submission_id: String) {
        match self.ensure_session().await {
            Ok(session) => {
                self.write(ProtocolEvent {
                    submission_id: submission_id.into(),
                    setup_lifecycle: Some(session.setup_lifecycle_snapshot()),
                    ..Default::default()
                })
                .await;
            }
            Err(err) => self.report_op_error(&submission_id, err).await,
        }
    }

    async fn dispatch_planning(self: &Arc<Self>, submission_id: String, op: OpKind) {
        let session = match self.ensure_session().await {
            Ok(session) => session,
            Err(error) => {
                self.report_op_error(&submission_id, error).await;
                return;
            }
        };
        let mutating = !matches!(&op, OpKind::GetPlanning | OpKind::ReadPlanDocument { .. });
        let runtime = session.planning_runtime();
        let mut reply = ProtocolEvent {
            submission_id: submission_id.clone().into(),
            ..Default::default()
        };
        let result = (|| -> Result<(), RuntimeString> {
            match op {
                OpKind::GetPlanning => {}
                OpKind::SetCollaborationMode {
                    mode,
                    expected_revision,
                } => runtime.set_mode(
                    klaus_agent_core::planning::CollaborationMode::parse(
                        &klaus_agent_core::planning::runtime::host(&mode),
                    )
                    .map_err(RuntimeString::from)?,
                    expected_revision,
                )?,
                OpKind::ReadPlanDocument { plan_id, revision } => {
                    reply.document = Some(runtime.read_document(&plan_id, revision)?)
                }
                OpKind::SavePlanDocument {
                    plan_id,
                    revision,
                    content,
                    expected_content,
                } => {
                    reply.document = Some(runtime.save_document_expected(
                        &plan_id,
                        revision,
                        &content,
                        expected_content.as_ref(),
                    )?)
                }
                OpKind::AcceptPlan {
                    plan_id,
                    revision,
                    client_request_id,
                    content,
                } => {
                    let (accepted, input) =
                        runtime.accept(&plan_id, revision, client_request_id, content)?;
                    reply.accepted = Some(accepted);
                    reply.implementation_input = Some(input);
                }
                OpKind::PlanFeedback {
                    plan_id,
                    revision,
                    client_request_id,
                    feedback,
                } => {
                    let (accepted, input) =
                        runtime.feedback(&plan_id, revision, client_request_id, feedback)?;
                    reply.accepted = Some(accepted);
                    reply.feedback_input = Some(input);
                }
                OpKind::CancelPlanImplementation { plan_id, revision } => {
                    runtime.cancel(&plan_id, revision)?
                }
                OpKind::SettlePlanImplementation {
                    plan_id,
                    revision,
                    client_request_id,
                    outcome,
                } => runtime.settle(&plan_id, revision, &client_request_id, &outcome)?,
                OpKind::AnswerUserInput {
                    request_id,
                    turn_id,
                    tool_call_id,
                    answers,
                    status,
                } => {
                    if !["answered", "skipped", "cancelled"]
                        .iter()
                        .any(|s| status == *s)
                    {
                        return Err("Invalid structured input response status".into());
                    }
                    let snapshot = runtime.snapshot()?;
                    let request = snapshot
                        .questions
                        .iter()
                        .find(|q| {
                            q.get("requestId").and_then(RuntimeJson::as_str) == Some(&request_id)
                                && q.get("turnId").and_then(RuntimeJson::as_str) == Some(&turn_id)
                                && q.get("toolCallId").and_then(RuntimeJson::as_str)
                                    == Some(&tool_call_id)
                                && q.get("status")
                                    .and_then(RuntimeJson::as_str)
                                    .is_some_and(|s| s == "pending")
                        })
                        .ok_or_else(|| {
                            RuntimeString::from(
                                "Structured input request expired or belongs to another turn",
                            )
                        })?;
                    if status == "answered" && answers.is_empty() {
                        return Err("An answered request must contain answers".into());
                    }
                    if status != "answered" && !answers.is_empty() {
                        return Err("An unanswered request cannot contain answers".into());
                    }
                    for (_, values) in answers.entries() {
                        if values.is_empty()
                            || values.iter().any(|v| {
                                klaus_agent_core::planning::runtime::host(v)
                                    .trim()
                                    .is_empty()
                            })
                        {
                            return Err("Question answers must be nonempty".into());
                        }
                    }
                    if status == "answered" {
                        let questions = request
                            .get("request")
                            .and_then(|q| q.get("questions"))
                            .and_then(RuntimeJson::as_array)
                            .ok_or_else(|| {
                                RuntimeString::from("Structured input request has no questions")
                            })?;
                        for (id, _) in answers.entries() {
                            if !questions
                                .iter()
                                .any(|q| q.get("id").and_then(RuntimeJson::as_str) == Some(&id))
                            {
                                return Err("Answer refers to an unknown question identity".into());
                            }
                        }
                    }
                    let mut answer_objects = klaus_agent_core::runtime_values::RuntimeObject::new();
                    for (id, values) in answers.entries() {
                        let mut answer = klaus_agent_core::runtime_values::RuntimeObject::new();
                        answer.insert(
                            "answers",
                            runtime_codec::encode_value(values).expect("answer arrays codec"),
                        );
                        answer_objects.insert(id.clone(), RuntimeJson::Object(answer));
                    }
                    let tx = {
                        let mut pending = self
                            .pending_prompts
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        let key = klaus_agent_core::planning::runtime::host(&request_id);
                        if !pending.elicitations.contains_key(&key) {
                            return Err("Structured input is no longer pending".into());
                        }
                        runtime.resolve_question(
                            &request_id,
                            status.clone(),
                            Some(RuntimeJson::Object(answer_objects)),
                        )?;
                        pending
                            .elicitations
                            .remove(&key)
                            .expect("request exists while prompt lock is held")
                            .1
                    };
                    tx.send(ElicitResponse::UserInput { answers, status })
                        .map_err(|_| {
                            RuntimeString::from(
                                "Structured input request expired before answer delivery",
                            )
                        })?;
                    reply.accepted = Some(true);
                }
                _ => unreachable!("planning dispatcher owns named planning operations"),
            }
            let snapshot = runtime.snapshot()?;
            reply.implementation_id = snapshot
                .implementation
                .as_ref()
                .map(|r| r.implementation_id.clone());
            reply.planning = Some(snapshot);
            Ok(())
        })();
        match result {
            Ok(()) => {
                if mutating {
                    if let Err(error) = session.synchronize_goal_mode().await {
                        self.report_op_error(
                            &submission_id,
                            error.try_to_utf8().unwrap_or_default().into(),
                        )
                        .await;
                        return;
                    }
                }
                self.write(reply).await;
                if mutating
                    && session.collaboration_mode()
                        == klaus_agent_core::planning::CollaborationMode::Default
                {
                    self.schedule_goal_idle();
                }
            }
            Err(error) => {
                self.report_op_error(
                    &submission_id,
                    klaus_agent_core::planning::runtime::host(&error).into(),
                )
                .await
            }
        }
    }

    async fn write_interrupt_result(
        &self,
        submission_id: String,
        accepted: bool,
        interrupted_turn_id: Option<String>,
        goal_pause_error: Option<RuntimeString>,
        error: Option<String>,
    ) {
        self.write(ProtocolEvent {
            submission_id: submission_id.into(),
            turn_interrupt: Some(crate::types::ProtocolTurnInterruptResult {
                accepted,
                interrupted_turn_id: interrupted_turn_id.map(Into::into),
                goal_pause_error,
            }),
            error: error.map(Into::into),
            ..Default::default()
        })
        .await;
    }

    fn capture_interrupt(
        self: &Arc<Self>,
        origin: crate::types::ProtocolInterruptOrigin,
        expected_turn_id: Option<&RuntimeString>,
    ) -> Result<InterruptAdmission, String> {
        use crate::types::ProtocolInterruptOrigin;
        let expected = match expected_turn_id {
            Some(expected) => match expected.try_to_utf8() {
                Ok(expected) if !expected.is_empty() => Some(expected),
                _ => {
                    return Err(
                        "expectedTurnId must identify a foreground facade submission".into(),
                    );
                }
            },
            None => None,
        };
        if origin == ProtocolInterruptOrigin::TargetedTurn && expected.is_none() {
            return Err("targeted-turn interruption requires expectedTurnId".into());
        }
        // This capture runs in reader admission, before a control can queue.
        // It never adopts a later foreground owner when the task starts.
        {
            let mut admission = self.run_admission.lock().unwrap();
            if self.stopped.load(Ordering::SeqCst) || self.session_closing.load(Ordering::SeqCst) {
                return Err("interrupt endpoint is closing".into());
            }
            let captured = admission.foreground.front().map(|owner| {
                (
                    owner.token,
                    owner.submission_id.clone(),
                    owner.signal.clone(),
                )
            });
            if expected.as_ref().is_some_and(|expected| {
                captured
                    .as_ref()
                    .is_none_or(|(_, active, _)| active != expected)
            }) {
                Err("interrupt target is no longer the foreground turn".into())
            } else if origin != ProtocolInterruptOrigin::TargetedTurn
                && admission.pending_goal_stops >= MAX_PENDING_PROTOCOL_LINES
            {
                Err("too many pending goal stops".into())
            } else {
                let lease = (origin != ProtocolInterruptOrigin::TargetedTurn).then(|| {
                    admission.pending_goal_stops += 1;
                    PendingGoalStop {
                        inner: Arc::downgrade(self),
                        armed: true,
                    }
                });
                let session = self.session.get().cloned();
                let goal_stop = (origin != ProtocolInterruptOrigin::TargetedTurn)
                    .then(|| {
                        session
                            .as_ref()
                            .and_then(|session| session.goal_observation_runtime())
                    })
                    .flatten()
                    .map(|runtime| {
                        let capture = runtime.begin_stop();
                        (runtime, capture)
                    });
                Ok(InterruptAdmission {
                    foreground: captured,
                    activation_epoch: admission.activation_epoch,
                    stop_lease: lease,
                    session,
                    goal_stop,
                })
            }
        }
    }

    async fn dispatch_interrupt(
        self: &Arc<Self>,
        submission_id: String,
        origin: crate::types::ProtocolInterruptOrigin,
        capture: Result<InterruptAdmission, String>,
        before_cancel: Option<InterruptPreparation>,
    ) {
        use crate::types::ProtocolInterruptOrigin;
        let InterruptAdmission {
            foreground: captured,
            activation_epoch: captured_epoch,
            mut stop_lease,
            session: pause_session,
            goal_stop,
        } = match capture {
            Ok(capture) => capture,
            Err(error) => {
                self.write_interrupt_result(submission_id, false, None, None, Some(error))
                    .await;
                return;
            }
        };
        // A newly published Session is a later local intent boundary. A cold
        // queued Stop cannot adopt that Session's goal; genuinely cold endpoints
        // keep their first durable record as the saved-goal linearization point.
        let captured_without_session =
            origin != ProtocolInterruptOrigin::TargetedTurn && pause_session.is_none();
        // Accepted arrival already revoked older model-request epochs. A queued
        // control cannot acquire a later foreground owner when it starts.
        // The later SQL and cancellation boundaries recheck the same capture.
        let current = {
            let admission = self.run_admission.lock().unwrap();
            let same_owner = match (captured.as_ref(), admission.foreground.front()) {
                (Some((token, _, _)), Some(owner)) => owner.token == *token,
                (None, None) => admission.activation_epoch == captured_epoch,
                _ => false,
            };
            same_owner
                && !self.stopped.load(Ordering::SeqCst)
                && (!captured_without_session || self.session.get().is_none())
        };
        if !current {
            drop(stop_lease);
            self.write_interrupt_result(
                submission_id,
                false,
                None,
                None,
                Some("interrupt target changed before its control task began".into()),
            )
            .await;
            return;
        }
        let pause_signal = captured
            .as_ref()
            .map(|(_, _, signal)| signal.clone())
            .unwrap_or_else(AbortSignal::new);
        let pause_owner = captured.as_ref().map(|(token, _, _)| *token);
        let foreground_id = captured
            .as_ref()
            .map(|(_, id, _)| RuntimeString::from(id.clone()));
        let weak_owner = Arc::downgrade(self);
        let commit_signal = pause_signal.clone();
        let commit_goal_stop = goal_stop.clone();
        let pause_guard = klaus_agent_core::goal::store::GoalControlGuard::new(move |operation| {
            let owner = weak_owner.upgrade().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "goal pause lost its endpoint",
                )
            })?;
            let admission = owner.run_admission.lock().unwrap();
            let same_owner = match (pause_owner, admission.foreground.front()) {
                (Some(expected), Some(current)) => current.token == expected,
                (None, None) => admission.activation_epoch == captured_epoch,
                _ => false,
            };
            if !same_owner
                || commit_signal.is_cancelled()
                || owner.stopped.load(Ordering::SeqCst)
                || (captured_without_session && owner.session.get().is_some())
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "goal pause lost its foreground reservation",
                ));
            }
            // SQLite obtained its transaction before entering this lock. The
            // operation includes COMMIT, so ownership stays fixed through SQL.
            let result = if let Some((runtime, capture)) = commit_goal_stop.as_ref() {
                runtime.guard_stop_control(*capture, &commit_signal, operation)
            } else {
                operation()
            };
            drop(admission);
            result
        });
        let session = self.session.get().cloned();
        let mut pause_error = None;
        let mut deadline_cancelled = false;
        if origin != ProtocolInterruptOrigin::TargetedTurn {
            let inner = self.clone();
            let pause = async move {
                if let Some(session) = pause_session {
                    if origin == ProtocolInterruptOrigin::System {
                        session
                            .pause_goal_for_shutdown_guarded(Some(pause_guard))
                            .await
                    } else if session.goal_observation_runtime().is_some() {
                        session
                            .pause_goal_by_user_guarded(Some(pause_guard), foreground_id)
                            .await
                    } else {
                        Ok(())
                    }
                } else {
                    inner
                        .pause_lazy_goal(
                            origin == ProtocolInterruptOrigin::UserStop,
                            foreground_id,
                            pause_guard,
                        )
                        .await
                }
            };
            if origin == ProtocolInterruptOrigin::System {
                if let Err(error) = pause.await {
                    pause_error = Some(error);
                }
            } else {
                // The task owns its rollout writer after timeout; the signal
                // revokes its later SQL authority before cancellation can race.
                match tokio::time::timeout(
                    std::time::Duration::from_millis(500),
                    tokio::spawn(pause),
                )
                .await
                {
                    Ok(Ok(Ok(()))) => {}
                    Ok(Ok(Err(error))) => pause_error = Some(error),
                    Ok(Err(error)) => {
                        pause_error = Some(format!("goal pause task failed: {error}").into())
                    }
                    Err(_) => {
                        deadline_cancelled = true;
                        pause_signal.cancel();
                        pause_error =
                            Some("goal pause is still pending; interrupting the turn".into());
                    }
                }
            }
        }
        if let Some((runtime, _)) = goal_stop.as_ref() {
            runtime.testing_wait_stop_cancel().await;
        }
        // Queue locks are acquired here, after reader admission and without
        // holding the foreground gate. A pending queue start can finish its
        // admission and release its own lock before this Stop rechecks target.
        let before_cancel = match before_cancel {
            Some(prepare) => match prepare().await {
                Ok(commit) => Some(commit),
                Err(error) => {
                    drop(stop_lease);
                    self.write_interrupt_result(
                        submission_id,
                        false,
                        None,
                        pause_error,
                        Some(error),
                    )
                    .await;
                    return;
                }
            },
            None => None,
        };
        // A timed-out Stop can own the idle successor of its own retired run,
        // provided no later foreground reservation has ever become active.
        let (accepted, interrupted, persistence_error, execution_started) = {
            let mut admission = self.run_admission.lock().unwrap();
            let mut current = (!captured_without_session || self.session.get().is_none())
                && match (captured.as_ref(), admission.foreground.front()) {
                    (Some((token, _, _)), Some(owner)) => owner.token == *token,
                    (Some((token, _, _)), None) => {
                        deadline_cancelled
                            && admission.last_active_token == Some(*token)
                            && admission.activation_epoch == captured_epoch
                    }
                    (None, None) => admission.activation_epoch == captured_epoch,
                    _ => false,
                };
            let persistence_error = if current {
                before_cancel.and_then(|commit| commit().err())
            } else {
                drop(before_cancel);
                None
            };
            current &= persistence_error.is_none();
            let interrupted = current
                .then(|| captured.as_ref().map(|(_, id, _)| id.clone()))
                .flatten();
            let execution_started = current
                && captured
                    .as_ref()
                    .is_some_and(|(_, id, _)| *self.current_submission_id.lock().unwrap() == *id);
            if current {
                if origin != ProtocolInterruptOrigin::TargetedTurn {
                    if let Some(session) = session.as_ref() {
                        if let Some((runtime, capture)) = goal_stop.as_ref() {
                            runtime.suppress_continuation_for(*capture);
                        }
                        if origin == ProtocolInterruptOrigin::UserStop {
                            session.interrupt_by_user_scoped();
                        } else {
                            session.interrupt();
                        }
                    }
                }
                if let Some((_, _, signal)) = captured.as_ref() {
                    signal.cancel();
                } else {
                    admission.signal.cancel();
                    admission.signal = AbortSignal::new();
                }
                admission.steer_turn_id = None;
                *self.last_completed_submission.lock().unwrap() = None;
                if origin != ProtocolInterruptOrigin::TargetedTurn {
                    self.fail_pending("interrupted");
                }
            }
            if let Some(lease) = &mut stop_lease {
                lease.release_locked(&mut admission);
            }
            (current, interrupted, persistence_error, execution_started)
        };
        self.schedule_goal_idle();
        if let Some(error) = pause_error.as_ref() {
            self.notify(
                NotificationLevel::Warn,
                format!(
                    "goal pause failed before interrupt: {}",
                    error.try_to_utf8().unwrap_or_default()
                ),
            )
            .await;
        }
        if accepted && execution_started {
            if let Some((token, _, _)) = captured.as_ref() {
                // Cancellation acceptance is also a handoff boundary. A client
                // can submit its next turn as soon as it receives this outcome,
                // so an executing foreground reservation must already be gone.
                // Reader-admitted work can still be parked behind publication
                // or an ordinary control. Its cancelled roster owner must stay
                // until that original dispatch retires it, without delaying ACK.
                loop {
                    let retired = self.foreground_retired.notified();
                    tokio::pin!(retired);
                    retired.as_mut().enable();
                    if !self
                        .run_admission
                        .lock()
                        .unwrap()
                        .foreground
                        .iter()
                        .any(|owner| owner.token == *token)
                    {
                        break;
                    }
                    retired.await;
                }
            }
        }
        self.write_interrupt_result(
            submission_id,
            accepted,
            interrupted,
            pause_error,
            persistence_error.or_else(|| {
                (!accepted)
                    .then(|| "interrupt target changed while its goal pause was pending".into())
            }),
        )
        .await;
    }

    async fn pause_lazy_goal(
        &self,
        user_origin: bool,
        foreground_submission_id: Option<RuntimeString>,
        control_guard: klaus_agent_core::goal::store::GoalControlGuard,
    ) -> Result<(), RuntimeString> {
        let thread = self
            .session_init
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .raw
            .thread_id
            .clone();
        let Some(thread) = thread else {
            return Ok(());
        };
        let thread = thread
            .try_to_utf8()
            .map_err(|error| RuntimeString::from(error.to_string()))?;
        // A no-goal lazy endpoint needs neither a writer nor invented evidence.
        // This first provider-free transaction is the cold Stop's goal-intent
        // linearization point. Never reread and adopt a later control's goal.
        let captured = klaus_agent_core::goal::store::get_record(&thread)
            .await
            .map_err(|error| RuntimeString::from(error.to_string()))?;
        let handler = match self.lifecycle.saved_goal_request.as_ref() {
            Some(handler) => handler,
            None if captured
                .goal
                .as_ref()
                .is_none_or(|goal| !goal.status.is_active()) =>
            {
                return Ok(());
            }
            None => return Err("saved goal pause is unavailable for this endpoint".into()),
        };
        handler(
            thread,
            klaus_agent_core::goal::service::SavedGoalRequest::Pause {
                captured,
                user_origin,
                foreground_submission_id,
                control_guard: Some(control_guard),
            },
        )
        .await
        .map(|_| ())
        .map_err(|error| RuntimeString::from(error.to_string()))
    }

    /// Goal reads resolve lazy ownership without constructing inference state.
    async fn dispatch_get_goal(self: &Arc<Self>, submission_id: String) {
        let _publication = self.session_publication_gate.lock().await;
        let result = if let Some(session) = self.session.get() {
            session
                .goal_snapshot()
                .await
                .map(|snapshot| ProtocolEvent {
                    submission_id: submission_id.clone().into(),
                    thread_goal: snapshot.goal.as_ref().map(ProtocolThreadGoal::from),
                    goal_revision: Some(snapshot.revision),
                    goal_pursuit_eligible: Some(session.goal_pursuit_eligible()),
                    ..Default::default()
                })
                .map_err(|error| error.to_string())
        } else {
            let init = self
                .session_init
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .raw
                .clone();
            self.lazy_goal_snapshot(submission_id.clone(), init).await
        };
        drop(_publication);
        match result {
            Ok(reply) => {
                if let Err(error) = runtime_codec::encode_value(&reply) {
                    self.report_op_error(&submission_id, error.to_string().into())
                        .await;
                    return;
                }
                self.write(reply).await;
            }
            Err(error) => self.report_op_error(&submission_id, error.into()).await,
        }
    }

    async fn lazy_goal_snapshot(
        &self,
        submission_id: String,
        init: ProtocolSessionInit,
    ) -> Result<ProtocolEvent, String> {
        let mut eligibility = self
            .lifecycle
            .classify_goal_pursuit
            .as_ref()
            .map(|classify| classify(init.clone()))
            .transpose()?;
        let record = if let Some(thread_id) = init.thread_id {
            let thread_id = thread_id.try_to_utf8().map_err(|error| error.to_string())?;
            if eligibility == Some(true) {
                if let Some(read_thread) = &self.lifecycle.read_thread {
                    match read_thread(thread_id.clone()).await {
                        Ok(snapshot)
                            if snapshot.meta.parent_thread_id.is_some()
                                || !matches!(
                                    snapshot.meta.role,
                                    None | Some(
                                        klaus_agent_core::schemas::rollout::SessionRole::Main
                                    )
                                ) =>
                        {
                            eligibility = Some(false);
                        }
                        Ok(_) => {}
                        Err(error)
                            if error.downcast_ref::<std::io::Error>().is_some_and(|error| {
                                error.kind() == std::io::ErrorKind::NotFound
                            }) => {}
                        Err(error) => {
                            return Err(format!("goal ownership could not be read: {error}"));
                        }
                    }
                }
            }
            if let Some(eligible) = eligibility {
                klaus_agent_core::goal::store::set_thread_goal_eligibility(&thread_id, eligible)
                    .await
                    .map_err(|error| error.to_string())?;
            }
            klaus_agent_core::goal::store::get_record(&thread_id)
                .await
                .map_err(|error| error.to_string())?
        } else {
            Default::default()
        };
        Ok(ProtocolEvent {
            submission_id: submission_id.into(),
            thread_goal: record.goal.as_ref().map(ProtocolThreadGoal::from),
            goal_revision: Some(record.revision),
            goal_pursuit_eligible: Some(eligibility.unwrap_or(false)),
            goal_pursuit_eligibility_notice: eligibility
                .is_none()
                .then(|| "Open this thread to determine whether goal pursuit is available.".into()),
            ..Default::default()
        })
    }

    fn finish_goal_publication(&self) {
        let buffered = self
            .goal_publication_buffer
            .lock()
            .unwrap()
            .take()
            .unwrap_or_default();
        for event in buffered {
            let _ = self.enqueue_queue_ordered_write(UnsolicitedWrite::Event(event));
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn dispatch_set_goal(
        self: &Arc<Self>,
        submission_id: String,
        owned_resources: Option<klaus_agent_core::goal::owned_resources::GoalOwnedResourcesInput>,
        objective: Option<RuntimeString>,
        status: Option<RuntimeString>,
        token_budget: Option<Option<i64>>,
        expected_goal_id: Option<Option<RuntimeString>>,
        expected_goal_revision: Option<i64>,
        origin: Option<RuntimeString>,
        submit: Option<bool>,
        display_text: Option<RuntimeString>,
        client_request_id: Option<RuntimeString>,
    ) {
        let session = match self.ensure_session().await {
            Ok(session) => session,
            Err(error) => {
                self.report_op_error(&submission_id, error).await;
                return;
            }
        };
        let thread = match session.id.try_to_utf8() {
            Ok(thread) => thread,
            Err(error) => {
                self.report_op_error(&submission_id, error.to_string().into())
                    .await;
                return;
            }
        };
        let _mutation = klaus_agent_core::goal::service::thread_mutation_guard(&thread).await;
        let result = async {
            if origin
                .as_ref()
                .is_some_and(|origin| *origin != "user" && *origin != "automatic")
            {
                return Err(RuntimeString::from("origin must be user or automatic"));
            }
            if owned_resources.is_some()
                && (origin.as_ref().is_none_or(|origin| *origin != "user")
                    || objective.as_ref().is_none_or(|objective| {
                        objective
                            .try_to_utf8()
                            .map(|text| text.trim().is_empty())
                            .unwrap_or(true)
                    }))
            {
                return Err(RuntimeString::from(
                    "ownedResources requires an explicit user objective",
                ));
            }
            if submit == Some(true)
                && (origin.as_ref().is_none_or(|origin| *origin != "user")
                    || objective.is_none()
                    || client_request_id
                        .as_ref()
                        .is_none_or(RuntimeString::is_empty))
            {
                return Err(RuntimeString::from(
                    "submit requires an explicit user objective and clientRequestId",
                ));
            }
            let mutation = klaus_agent_core::goal::service::HostGoalMutation {
                owned_resources,
                objective: objective.clone(),
                status,
                token_budget,
                expected_absent: expected_goal_id == Some(None),
                expected_goal_id: expected_goal_id.flatten(),
                expected_revision: expected_goal_revision,
                require_safe_json: true,
                control_guard: None,
            };
            let replay = if submit == Some(true) {
                let request_id = client_request_id
                    .as_ref()
                    .expect("submit id checked")
                    .try_to_utf8()
                    .map_err(|error| RuntimeString::from(error.to_string()))?;
                klaus_agent_core::goal::store::get_goal_request_receipt(&thread, &request_id)
                    .await
                    .map_err(|error| RuntimeString::from(error.to_string()))?
            } else {
                None
            };
            if let Some(receipt) = replay {
                let fingerprint = klaus_agent_core::goal::service::request_fingerprint(&mutation)
                    .map_err(|error| RuntimeString::from(error.to_string()))?;
                if receipt.fingerprint != fingerprint {
                    return Err(
                        "clientRequestId was already used for a different goal request".into(),
                    );
                }
                runtime_codec::encode_value(&receipt.goal)
                    .map_err(|error| RuntimeString::from(error.to_string()))?;
                if !(0..=9_007_199_254_740_991).contains(&receipt.revision) {
                    return Err(
                        "goalRevision cannot be represented as an exact JSON integer".into(),
                    );
                }
            } else {
                let snapshot = session
                    .goal_snapshot()
                    .await
                    .map_err(|error| RuntimeString::from(error.to_string()))?;
                if !(0..9_007_199_254_740_991).contains(&snapshot.revision) {
                    return Err(
                        "the next goalRevision cannot be represented as an exact JSON integer"
                            .into(),
                    );
                }
                session.validate_goal_wire_mutation(&mutation).await?;
            }
            if origin.as_ref().is_some_and(|origin| *origin == "user") {
                if let (Some(true), Some(request_id)) = (submit, client_request_id.as_ref()) {
                    session
                        .record_user_goal_submission_instruction(
                            request_id,
                            objective.as_ref(),
                            mutation.status.as_ref(),
                        )
                        .await?;
                } else {
                    session
                        .record_user_goal_instruction(objective.as_ref(), mutation.status.as_ref())
                        .await?;
                }
            }
            *self.goal_publication_buffer.lock().unwrap() = Some(Vec::new());
            let (update, replayed) = if submit == Some(true) {
                session
                    .set_thread_goal_once(
                        mutation,
                        client_request_id.clone().expect("submit id checked"),
                    )
                    .await?
            } else {
                (session.set_thread_goal_checked(mutation).await?, false)
            };
            runtime_codec::encode_value(&ProtocolThreadGoal::from(&update.goal))
                .map_err(|error| RuntimeString::from(error.to_string()))?;
            if !(0..=9_007_199_254_740_991).contains(&update.revision) {
                return Err("goalRevision cannot be represented as an exact JSON integer".into());
            }
            if submit == Some(true) {
                let presentation = session
                    .record_submitted_goal_objective(
                        client_request_id.expect("submit id checked"),
                        objective.expect("submit objective checked"),
                        display_text,
                    )
                    .await?;
                if !replayed {
                    session.mark_goal_submission_pending(&update.goal.goal_id);
                }
                self.write(ProtocolEvent {
                    submission_id: RuntimeString::default(),
                    goal_submission: Some(crate::types::ProtocolGoalSubmission::from(
                        &presentation,
                    )),
                    ..Default::default()
                })
                .await;
            }
            let mut admission = "deferred";
            if !replayed {
                if let Some(steer) = &update.steer {
                    let steered = {
                        let owner = self.run_admission.lock().unwrap();
                        if owner.steer_turn_id.is_some() {
                            session.try_steer_goal_instruction(steer.clone())?
                        } else {
                            false
                        }
                    };
                    admission = if steered { "steered" } else { "scheduled" };
                }
            }
            self.write(ProtocolEvent {
                submission_id: submission_id.clone().into(),
                thread_goal: Some(ProtocolThreadGoal::from(&update.goal)),
                goal_revision: Some(update.revision),
                goal_admission: Some(admission.into()),
                ..Default::default()
            })
            .await;
            Ok::<_, RuntimeString>(admission == "scheduled")
        }
        .await;
        if let Err(error) = &result {
            self.report_op_error(
                &submission_id,
                error.try_to_utf8().unwrap_or_default().into(),
            )
            .await;
        }
        self.finish_goal_publication();
        if matches!(result, Ok(true)) {
            self.schedule_goal_idle();
        }
    }

    async fn dispatch_clear_goal(
        self: &Arc<Self>,
        submission_id: String,
        expected_goal_id: Option<RuntimeString>,
        expected_goal_revision: Option<i64>,
        origin: Option<RuntimeString>,
    ) {
        let session = match self.ensure_session().await {
            Ok(session) => session,
            Err(error) => {
                self.report_op_error(&submission_id, error).await;
                return;
            }
        };
        let thread = match session.id.try_to_utf8() {
            Ok(thread) => thread,
            Err(error) => {
                self.report_op_error(&submission_id, error.to_string().into())
                    .await;
                return;
            }
        };
        let prepared = async {
            if origin.as_ref().is_none_or(|origin| *origin != "automatic") {
                return Ok::<_, RuntimeString>(None);
            }
            let _mutation = klaus_agent_core::goal::service::thread_mutation_guard(&thread).await;
            let snapshot = session
                .goal_snapshot()
                .await
                .map_err(|error| RuntimeString::from(error.to_string()))?;
            let goal = snapshot
                .goal
                .as_ref()
                .filter(|goal| {
                    goal.status == klaus_agent_core::goal::ThreadGoalStatus::Complete
                        && expected_goal_id.as_ref() == Some(&goal.goal_id)
                })
                .ok_or_else(|| {
                    RuntimeString::from(
                        "automatic clear requires the current completed goal identity",
                    )
                })?;
            if expected_goal_revision.is_some_and(|revision| revision != snapshot.revision) {
                return Err(RuntimeString::from(
                    "GOAL_CONFLICT: the goal changed before clear",
                ));
            }
            session
                .record_goal_completion(goal, snapshot.revision)
                .await?;
            Ok(Some(snapshot.revision))
        }
        .await;
        let automatic_revision = match prepared {
            Ok(revision) => revision,
            Err(error) => {
                self.report_op_error(
                    &submission_id,
                    error.try_to_utf8().unwrap_or_default().into(),
                )
                .await;
                return;
            }
        };
        if automatic_revision.is_some() {
            // Completion cleanup must not revoke the authority of its final report.
            // Release the thread guard so human clear and replacement remain immediate.
            if let Err(error) = session
                .goal_observation_runtime()
                .expect("completed goal has a runtime")
                .wait_for_goal_turn_settlement(
                    expected_goal_id
                        .as_ref()
                        .expect("automatic identity checked"),
                )
                .await
            {
                self.report_op_error(&submission_id, error.to_string().into())
                    .await;
                return;
            }
        }
        let _mutation = klaus_agent_core::goal::service::thread_mutation_guard(&thread).await;
        let result = async {
            match origin.as_ref() {
                Some(origin) if *origin == "automatic" => {
                    let snapshot = session
                        .goal_snapshot()
                        .await
                        .map_err(|error| RuntimeString::from(error.to_string()))?;
                    if Some(snapshot.revision) != automatic_revision
                        || snapshot.goal.as_ref().is_none_or(|goal| {
                            goal.status != klaus_agent_core::goal::ThreadGoalStatus::Complete
                                || expected_goal_id.as_ref() != Some(&goal.goal_id)
                        })
                    {
                        return Err(RuntimeString::from(
                            "GOAL_CONFLICT: the goal changed before clear",
                        ));
                    }
                }
                Some(origin) if *origin == "user" => session.record_user_goal_clear().await?,
                Some(_) => return Err(RuntimeString::from("origin must be user or automatic")),
                None => {}
            }
            *self.goal_publication_buffer.lock().unwrap() = Some(Vec::new());
            session
                .clear_thread_goal_checked(
                    expected_goal_id,
                    expected_goal_revision.or(automatic_revision),
                )
                .await?;
            let snapshot = session
                .goal_snapshot()
                .await
                .map_err(|error| RuntimeString::from(error.to_string()))?;
            self.write(ProtocolEvent {
                submission_id: submission_id.clone().into(),
                thread_goal: snapshot.goal.as_ref().map(ProtocolThreadGoal::from),
                goal_cleared: snapshot.goal.is_none().then_some(true),
                goal_revision: Some(snapshot.revision),
                ..Default::default()
            })
            .await;
            Ok::<_, RuntimeString>(())
        }
        .await;
        if let Err(error) = result {
            self.report_op_error(
                &submission_id,
                error.try_to_utf8().unwrap_or_default().into(),
            )
            .await;
        }
        self.finish_goal_publication();
    }

    /// `CompactSession`: chain a manual compaction onto the serial op chain.
    async fn dispatch_compact_session(
        self: &Arc<Self>,
        submission_id: String,
        instructions: Option<RuntimeString>,
        selection: Option<crate::types::CompactSelection>,
        busy_guard: Option<BusySubmissionGuard>,
    ) {
        let inner = self.clone();
        let sid = submission_id.clone();
        self.enqueue(submission_id, move || -> ChainFuture {
            Box::pin(async move {
                if let Some(guard) = busy_guard.as_ref() {
                    guard.activate();
                }
                let _busy_guard = busy_guard;
                inner.handle_compact(sid, instructions, selection).await
            })
        });
    }

    /// `Shutdown`: record the correlated submission id and stop the server.
    async fn dispatch_shutdown(self: &Arc<Self>, submission_id: String) {
        let mut shutdown_id = self.shutdown_submission_id.lock().unwrap();
        if shutdown_id.is_none() {
            *shutdown_id = Some(submission_id.into());
        }
        drop(shutdown_id);
        self.stop();
    }

    /// `ResolveAgentMemorySnapshotDecision`: apply the host's merge choice to
    /// one pending agent-memory snapshot decision.
    async fn dispatch_resolve_agent_memory_snapshot_decision(
        self: &Arc<Self>,
        submission_id: String,
        request_id: RuntimeString,
        decision: crate::types::AgentMemoryMergeChoice,
    ) {
        let Some(request_id) = self
            .agent_memory_snapshot_request_id_or_report(request_id, &submission_id)
            .await
        else {
            return;
        };
        match self.ensure_session().await {
            Ok(session) => {
                let reply_inner = self.clone();
                let reply_submission_id = submission_id.clone();
                match session
                    .resolve_agent_memory_snapshot_decision_ordered(
                        &request_id,
                        decision,
                        move |audit| {
                            reply_inner.enqueue_queue_ordered_write(UnsolicitedWrite::Event(
                                ProtocolEvent {
                                    submission_id: reply_submission_id.into(),
                                    agent_memory_snapshot_decision: Some(audit),
                                    ..Default::default()
                                },
                            ))
                        },
                    )
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => self.stop(),
                    Err(error) => {
                        self.report_agent_memory_snapshot_op_error_ordered(
                            Some(session.as_ref()),
                            &submission_id,
                            Box::new(error),
                        )
                        .await;
                    }
                }
            }
            Err(error) => {
                self.report_agent_memory_snapshot_op_error_ordered(
                    self.session.get().map(Arc::as_ref),
                    &submission_id,
                    error,
                )
                .await;
            }
        }
    }

    /// `SettleAgentMemorySnapshotUnavailable`: record why the host could not
    /// deliver a snapshot decision (timeout / disconnect / no handler).
    async fn dispatch_settle_agent_memory_snapshot_unavailable(
        self: &Arc<Self>,
        submission_id: String,
        request_id: RuntimeString,
        reason: AgentMemorySnapshotUnavailableReason,
        error: Option<RuntimeString>,
    ) {
        let Some(request_id) = self
            .agent_memory_snapshot_request_id_or_report(request_id, &submission_id)
            .await
        else {
            return;
        };
        match self.ensure_session().await {
            Ok(session) => {
                let response_inner = self.clone();
                let response_submission_id = submission_id.clone();
                let enqueue = move |audit| {
                    response_inner.enqueue_queue_ordered_write(UnsolicitedWrite::Event(
                        ProtocolEvent {
                            submission_id: response_submission_id.into(),
                            agent_memory_snapshot_decision: Some(audit),
                            ..Default::default()
                        },
                    ))
                };
                let result = match reason {
                    AgentMemorySnapshotUnavailableReason::TimedOut => {
                        session
                            .mark_agent_memory_snapshot_timed_out_ordered(
                                &request_id,
                                error,
                                enqueue,
                            )
                            .await
                    }
                    AgentMemorySnapshotUnavailableReason::Disconnected => {
                        session
                            .mark_agent_memory_snapshot_host_disconnected_ordered(
                                &request_id,
                                error,
                                enqueue,
                            )
                            .await
                    }
                    AgentMemorySnapshotUnavailableReason::NoHandler => {
                        session
                            .mark_agent_memory_snapshot_no_handler_ordered(
                                &request_id,
                                error,
                                enqueue,
                            )
                            .await
                    }
                };
                match result {
                    Ok(true) => {}
                    Ok(false) => self.stop(),
                    Err(error) => {
                        self.report_agent_memory_snapshot_op_error_ordered(
                            Some(session.as_ref()),
                            &submission_id,
                            Box::new(error),
                        )
                        .await;
                    }
                }
            }
            Err(error) => {
                self.report_agent_memory_snapshot_op_error_ordered(
                    self.session.get().map(Arc::as_ref),
                    &submission_id,
                    error,
                )
                .await;
            }
        }
    }

    /// `GetAgentMemorySnapshotDecision`: publish one decision audit.
    async fn dispatch_get_agent_memory_snapshot_decision(
        self: &Arc<Self>,
        submission_id: String,
        request_id: RuntimeString,
    ) {
        let Some(request_id) = self
            .agent_memory_snapshot_request_id_or_report(request_id, &submission_id)
            .await
        else {
            return;
        };
        match self.ensure_session().await {
            Ok(session) => {
                let response_inner = self.clone();
                let response_request_id = request_id.clone();
                let response_submission_id = submission_id.clone();
                let result = session
                    .with_agent_memory_snapshot_decision(&request_id, move |audit| {
                        response_inner.enqueue_queue_ordered_write(UnsolicitedWrite::Event(
                            ProtocolEvent {
                                submission_id: response_submission_id.into(),
                                agent_memory_snapshot_decision_query: Some(
                                    AgentMemorySnapshotDecisionQueryResult {
                                        request_id: response_request_id,
                                        audit,
                                    },
                                ),
                                ..Default::default()
                            },
                        ))
                    })
                    .await;
                match result {
                    Ok(true) => {}
                    Ok(false) => self.stop(),
                    Err(error) => {
                        self.report_agent_memory_snapshot_op_error_ordered(
                            Some(session.as_ref()),
                            &submission_id,
                            Box::new(error),
                        )
                        .await;
                    }
                }
            }
            Err(error) => {
                self.report_agent_memory_snapshot_op_error_ordered(
                    self.session.get().map(Arc::as_ref),
                    &submission_id,
                    error,
                )
                .await;
            }
        }
    }

    /// `GetAgentMemorySnapshotDecisions`: publish every decision audit.
    async fn dispatch_get_agent_memory_snapshot_decisions(self: &Arc<Self>, submission_id: String) {
        match self.ensure_session().await {
            Ok(session) => {
                let response_inner = self.clone();
                let response_submission_id = submission_id.clone();
                let result = session
                    .with_agent_memory_snapshot_decisions(move |audits| {
                        response_inner.enqueue_queue_ordered_write(UnsolicitedWrite::Event(
                            ProtocolEvent {
                                submission_id: response_submission_id.into(),
                                agent_memory_snapshot_decisions: Some(audits),
                                ..Default::default()
                            },
                        ))
                    })
                    .await;
                match result {
                    Ok(true) => {}
                    Ok(false) => self.stop(),
                    Err(error) => {
                        self.report_agent_memory_snapshot_op_error_ordered(
                            Some(session.as_ref()),
                            &submission_id,
                            Box::new(error),
                        )
                        .await;
                    }
                }
            }
            Err(error) => {
                self.report_agent_memory_snapshot_op_error_ordered(
                    self.session.get().map(Arc::as_ref),
                    &submission_id,
                    error,
                )
                .await;
            }
        }
    }

    /// `GetQueued`: publish the queued user messages behind the queue barrier.
    async fn dispatch_get_queued(self: &Arc<Self>, submission_id: String) {
        match self.ensure_session().await {
            Ok(session) => {
                let _queue_operation = self.queue_operation_gate.lock().await;
                let barrier = self.begin_queue_publication_barrier();
                match session.await_queue_durability().await {
                    Ok(()) => {
                        let queued = session
                            .queued_messages()
                            .into_iter()
                            .map(|message| (*message).clone())
                            .collect();
                        if !self.enqueue_queue_barrier_head_write(UnsolicitedWrite::Event(
                            ProtocolEvent {
                                submission_id: submission_id.clone().into(),
                                queued: Some(queued),
                                ..Default::default()
                            },
                        )) {
                            self.report_op_error(
                                &submission_id,
                                "protocol server is stopping".into(),
                            )
                            .await;
                        }
                    }
                    Err(error) => {
                        self.report_op_error(
                            &submission_id,
                            runtime_string_error_fragment(&error).into(),
                        )
                        .await;
                    }
                }
                barrier.release();
            }
            Err(err) => self.report_op_error(&submission_id, err).await,
        }
    }

    /// `TakePending`: drain and publish the pending queue entries.
    async fn dispatch_take_pending(self: &Arc<Self>, submission_id: String) {
        match self.ensure_session().await {
            Ok(session) => {
                let _queue_operation = self.queue_operation_gate.lock().await;
                let barrier = self.begin_queue_publication_barrier();
                match session.take_pending_main_messages().await {
                    Ok(pending) => {
                        let pending = pending
                            .into_iter()
                            .map(|message| (*message).clone())
                            .collect();
                        if !self.enqueue_queue_barrier_head_write(UnsolicitedWrite::Event(
                            ProtocolEvent {
                                submission_id: submission_id.clone().into(),
                                pending: Some(pending),
                                ..Default::default()
                            },
                        )) {
                            self.report_op_error(
                                &submission_id,
                                "protocol server is stopping".into(),
                            )
                            .await;
                        }
                    }
                    Err(error) => {
                        self.report_op_error(
                            &submission_id,
                            runtime_string_error_fragment(&error).into(),
                        )
                        .await;
                    }
                }
                barrier.release();
            }
            Err(err) => self.report_op_error(&submission_id, err).await,
        }
    }

    /// `GetQueueSnapshot`: publish a revisioned queue snapshot.
    async fn dispatch_get_queue_snapshot(self: &Arc<Self>, submission_id: String) {
        match self.ensure_session().await {
            Ok(session) => {
                let _queue_operation = self.queue_operation_gate.lock().await;
                let barrier = self.begin_queue_publication_barrier();
                // Even after a failed receipt, Hosts must be able to
                // inspect the one explicit degraded snapshot. It keeps
                // the last proven state and the first persistence error.
                let _ = session.await_queue_durability().await;
                // The barrier prevents a concurrent core revision from
                // entering the FIFO between this authoritative capture
                // and its correlated response. Buffered revisions are
                // released only after the response is enqueued.
                let snapshot = session.queue_snapshot();
                if !self.enqueue_queue_barrier_head_write(UnsolicitedWrite::Event(ProtocolEvent {
                    submission_id: submission_id.clone().into(),
                    queue_snapshot: Some(snapshot),
                    ..Default::default()
                })) {
                    self.report_op_error(&submission_id, "protocol server is stopping".into())
                        .await;
                }
                barrier.release();
            }
            Err(err) => self.report_op_error(&submission_id, err).await,
        }
    }

    /// `EditQueued`: apply one revision-checked edit to a queued message.
    async fn dispatch_edit_queued(
        self: &Arc<Self>,
        submission_id: String,
        mutation_id: RuntimeString,
        uuid: RuntimeString,
        expected_revision: u64,
        edit: crate::types::QueueEdit,
    ) {
        macro_rules! scalar {
            ($value:expr, $name:literal) => {
                match scalar_runtime_string($value, $name) {
                    Ok(value) => value,
                    Err(error) => {
                        self.report_op_error(&submission_id, error.into()).await;
                        return;
                    }
                }
            };
        }
        match self.ensure_session().await {
            Ok(session) => {
                let _queue_operation = self.queue_operation_gate.lock().await;
                let barrier = self.begin_queue_publication_barrier();
                let mutation_id = scalar!(mutation_id, "mutationId");
                let uuid = scalar!(uuid, "uuid");
                let result = session
                    .edit_queued_message(mutation_id.into(), uuid.into(), expected_revision, edit)
                    .await;
                self.enqueue_queue_mutation_result(&submission_id, result)
                    .await;
                barrier.release();
            }
            Err(err) => self.report_op_error(&submission_id, err).await,
        }
    }

    /// `RemoveQueued`: remove one queued message under its expected revision.
    async fn dispatch_remove_queued(
        self: &Arc<Self>,
        submission_id: String,
        mutation_id: RuntimeString,
        uuid: RuntimeString,
        expected_revision: u64,
    ) {
        macro_rules! scalar {
            ($value:expr, $name:literal) => {
                match scalar_runtime_string($value, $name) {
                    Ok(value) => value,
                    Err(error) => {
                        self.report_op_error(&submission_id, error.into()).await;
                        return;
                    }
                }
            };
        }
        match self.ensure_session().await {
            Ok(session) => {
                let _queue_operation = self.queue_operation_gate.lock().await;
                let barrier = self.begin_queue_publication_barrier();
                let mutation_id = scalar!(mutation_id, "mutationId");
                let uuid = scalar!(uuid, "uuid");
                let result = session
                    .remove_queued_message(mutation_id.into(), uuid.into(), expected_revision)
                    .await;
                self.enqueue_queue_mutation_result(&submission_id, result)
                    .await;
                barrier.release();
            }
            Err(err) => self.report_op_error(&submission_id, err).await,
        }
    }

    /// `ReorderQueued`: reorder one queued message under its expected revision.
    async fn dispatch_reorder_queued(
        self: &Arc<Self>,
        submission_id: String,
        mutation_id: RuntimeString,
        uuid: RuntimeString,
        before_uuid: Option<RuntimeString>,
        expected_revision: u64,
    ) {
        macro_rules! scalar {
            ($value:expr, $name:literal) => {
                match scalar_runtime_string($value, $name) {
                    Ok(value) => value,
                    Err(error) => {
                        self.report_op_error(&submission_id, error.into()).await;
                        return;
                    }
                }
            };
        }
        match self.ensure_session().await {
            Ok(session) => {
                let _queue_operation = self.queue_operation_gate.lock().await;
                let barrier = self.begin_queue_publication_barrier();
                let mutation_id = scalar!(mutation_id, "mutationId");
                let uuid = scalar!(uuid, "uuid");
                let before_uuid = match before_uuid {
                    Some(value) => Some(scalar!(value, "beforeUuid").into()),
                    None => None,
                };
                let result = session
                    .reorder_queued_message(
                        mutation_id.into(),
                        uuid.into(),
                        before_uuid,
                        expected_revision,
                    )
                    .await;
                self.enqueue_queue_mutation_result(&submission_id, result)
                    .await;
                barrier.release();
            }
            Err(err) => self.report_op_error(&submission_id, err).await,
        }
    }

    /// `GetMemoryMaintenance`: freeze and publish the memory-maintenance snapshot.
    async fn dispatch_get_memory_maintenance(self: &Arc<Self>, submission_id: String) {
        match self.ensure_session().await {
            Ok(session) => {
                // Freeze the snapshot and enqueue it while holding core's
                // maintenance event gate. The writer does no lazy read, so
                // backpressure cannot reorder a query across a lifecycle
                // transition or revise its authoritative contents later.
                let sender = self.unsolicited_tx.lock().unwrap().as_ref().cloned();
                if let Some(sender) = sender {
                    match session.with_ordered_memory_maintenance_snapshot(|snapshot| {
                        sender.send(UnsolicitedWrite::MemoryMaintenanceSnapshot {
                            submission_id: submission_id.clone().into(),
                            snapshot,
                        })
                    }) {
                        Ok(Ok(())) => {}
                        Ok(Err(_)) => {
                            self.report_op_error(
                                &submission_id,
                                "protocol server is stopping".into(),
                            )
                            .await;
                        }
                        Err(error) => {
                            self.report_memory_maintenance_error(&submission_id, error)
                                .await;
                        }
                    }
                } else {
                    self.report_op_error(&submission_id, "protocol server is stopping".into())
                        .await;
                }
            }
            Err(err) => self.report_op_error(&submission_id, err).await,
        }
    }

    /// `LoadSubagentTranscript`: load one subagent transcript through the host lifecycle hook.
    async fn dispatch_load_subagent_transcript(
        self: &Arc<Self>,
        submission_id: String,
        thread_id: RuntimeString,
        agent_id: RuntimeString,
    ) {
        macro_rules! scalar {
            ($value:expr, $name:literal) => {
                match scalar_runtime_string($value, $name) {
                    Ok(value) => value,
                    Err(error) => {
                        self.report_op_error(&submission_id, error.into()).await;
                        return;
                    }
                }
            };
        }
        match &self.lifecycle.load_subagent_transcript {
            Some(load) => {
                let thread_id = scalar!(thread_id, "threadId");
                let agent_id = scalar!(agent_id, "agentId");
                match load(thread_id, agent_id.clone()).await {
                    Ok(transcript) => {
                        // A live child whose sidechain never opened has no
                        // file to read, and an empty answer here would erase
                        // the run the user just watched. The session's
                        // in-memory mirror holds exactly that transcript
                        // (written when the sidechain open failed); prefer it
                        // over an empty disk read.
                        let transcript = if transcript.messages.is_empty() {
                            match self.ensure_session().await {
                                Ok(session) => {
                                    match session.mirrored_subagent_transcript(&agent_id) {
                                        Some(messages) if !messages.is_empty() => {
                                            SubAgentTranscript {
                                                messages,
                                                ..transcript
                                            }
                                        }
                                        _ => transcript,
                                    }
                                }
                                Err(_) => transcript,
                            }
                        } else {
                            transcript
                        };
                        self.write(ProtocolEvent {
                            submission_id: submission_id.clone().into(),
                            subagent_transcript: Some(transcript),
                            ..Default::default()
                        })
                        .await
                    }
                    Err(err) => self.report_op_error(&submission_id, err).await,
                }
            }
            None => {
                self.report_op_error(
                    &submission_id,
                    "load_subagent_transcript requires a session store".into(),
                )
                .await
            }
        }
    }

    /// `GetTaskList`: publish the task list snapshot for the bound task-list identity.
    async fn dispatch_get_task_list(
        self: &Arc<Self>,
        submission_id: String,
        options: Option<crate::types::TaskListQueryOptions>,
    ) {
        match self.ensure_session().await {
            Ok(session) => {
                let _task_operation = self.queue_operation_gate.lock().await;
                let barrier = self.begin_queue_publication_barrier();
                #[cfg(test)]
                {
                    let gate = self.todo_query_test_gate.lock().unwrap().clone();
                    if let Some(gate) = gate {
                        gate.pause().await;
                    }
                }
                match session
                    .get_task_list_with_publication_cutoff(options.unwrap_or_default())
                    .await
                {
                    Ok(result) => {
                        // The correlated authoritative snapshot is the FIFO
                        // head for this epoch. Any N+1 watcher publication
                        // that races the disk read remains buffered behind it.
                        self.advance_task_publication_floor(
                            &result.snapshot.session_id,
                            result.publication_cutoff,
                        );
                        if !self.enqueue_queue_barrier_head_write(UnsolicitedWrite::Event(
                            ProtocolEvent {
                                submission_id: submission_id.clone().into(),
                                task_list: Some(result.snapshot),
                                ..Default::default()
                            },
                        )) {
                            self.report_op_error(
                                &submission_id,
                                "protocol server is stopping".into(),
                            )
                            .await;
                        }
                    }
                    Err(error) => {
                        self.report_op_error(&submission_id, Box::new(error)).await;
                    }
                }
                barrier.release();
            }
            Err(err) => self.report_op_error(&submission_id, err).await,
        }
    }

    /// `CreateSideTask`: bind a new side-task session through the host lifecycle hook.
    async fn dispatch_create_side_task(
        self: &Arc<Self>,
        submission_id: String,
        thread_id: RuntimeString,
        new_thread_id: Option<RuntimeString>,
    ) {
        macro_rules! scalar {
            ($value:expr, $name:literal) => {
                match scalar_runtime_string($value, $name) {
                    Ok(value) => value,
                    Err(error) => {
                        self.report_op_error(&submission_id, error.into()).await;
                        return;
                    }
                }
            };
        }
        let thread_id = scalar!(thread_id, "threadId");
        let new_thread_id = match optional_scalar_runtime_string(new_thread_id, "newThreadId") {
            Ok(value) => value,
            Err(error) => {
                self.report_op_error(&submission_id, error.into()).await;
                return;
            }
        };
        self.bind_persisted_session(
            submission_id,
            thread_id,
            new_thread_id,
            None,
            self.lifecycle.create_side_task.clone(),
            "create_side_task requires a session store",
        )
        .await;
    }

    fn dispatch_init_session(
        self: &Arc<Self>,
        submission_id: String,
        init: ProtocolSessionInit,
    ) -> futures::future::BoxFuture<'_, ()> {
        Box::pin(self.dispatch_init_session_inner(submission_id, init))
    }

    async fn dispatch_init_session_inner(
        self: &Arc<Self>,
        submission_id: String,
        mut init: ProtocolSessionInit,
    ) {
        // Startup diagnostics: a session/start that stalls inside init used to
        // surface only as the host's silent 60s deadline with nothing in the
        // engine log. One stderr line per stage lets a stall name itself.
        let init_stage = std::time::Instant::now();
        eprintln!("[session-start] {submission_id} stage=init-begin elapsedMs=0");
        // Cheap read-only rejection avoids provider/auth/Host construction for
        // an already-bound or closing endpoint. The gate-protected recheck
        // below remains the authoritative race linearization.
        if self.session_closing.load(Ordering::SeqCst) {
            self.report_op_error(&submission_id, "protocol session has shut down".into())
                .await;
            return;
        }
        // Session startup depends on the requested provider's credentials. A
        // product account is optional, including for guests using their own
        // subscription or API key.
        if self.session.get().is_some() {
            // A prepared publication sets OnceCell before releasing its
            // admission/barrier and gate, while retaining the inflight mutex
            // across that entire no-await tail. Taking the mutex here keeps
            // this fast rejection from writing ahead of buffered initial
            // Session events in that narrow internal window.
            let publication = self
                .session_publication_inflight
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            drop(publication);
            self.report_op_error(
                &submission_id,
                "init_session must be sent before the protocol session is bound".into(),
            )
            .await;
            return;
        }
        if let Some(config) = &init.config {
            let mut config = match scalar_json_value(config, "session config") {
                Ok(config) => config,
                Err(err) => {
                    self.report_op_error(&submission_id, err.into()).await;
                    return;
                }
            };
            if let Some(fields) = config.as_object_mut() {
                if let Err(err) =
                    klaus_agent_core::schemas::config::normalize_sandbox_config(fields)
                {
                    self.report_op_error(&submission_id, err.into()).await;
                    return;
                }
                let base_dir = init.cwd.as_ref().map(session_config_base_dir);
                if let Err(err) =
                    klaus_agent_core::schemas::config::normalize_sandbox_workspace_roots(
                        fields,
                        base_dir.as_deref(),
                    )
                {
                    self.report_op_error(&submission_id, err.into()).await;
                    return;
                }
            }
            if let Err(err) = resolve_config(config.clone()) {
                self.report_op_error(&submission_id, err.into()).await;
                return;
            }
            init.config = Some(
                runtime_codec::decode_text(&RuntimeString::from(config.to_string()))
                    .expect("validated scalar session config encodes"),
            );
        }
        if let Err(err) = validate_serializable_session_init(&init) {
            self.report_op_error(&submission_id, err.into()).await;
            return;
        }
        if init.model_source.is_some() {
            let config = inference_config_value(&init).and_then(|value| {
                runtime_codec::decode_text(&RuntimeString::from(value.to_string()))
                    .map_err(|error| format!("invalid model config: {error}"))
            });
            match config {
                Ok(config) => init.config = Some(config),
                Err(error) => {
                    self.report_op_error(&submission_id, error.into()).await;
                    return;
                }
            }
        }
        let provider = match provider_from_init(&init, self.credential_store.clone()).await {
            Ok(provider) => provider,
            Err(err) => {
                self.report_op_error(&submission_id, err.into()).await;
                return;
            }
        };
        eprintln!(
            "[session-start] {submission_id} stage=init-provider elapsedMs={}",
            init_stage.elapsed().as_millis()
        );
        // Capability answers pass through the model catalog overlay; a
        // configured remote source is revalidated in the background so a
        // long-lived endpoint picks up capability changes without a restart.
        let provider = provider.map(|provider| {
            let provider = self
                .provider_decorator
                .lock()
                .unwrap()
                .as_ref()
                .map(|decorate| decorate(provider.clone(), &init))
                .unwrap_or(provider);
            let catalog = klaus_agent_core::provider::model_catalog::ModelCatalog::load(None);
            if klaus_agent_core::provider::model_catalog::ModelCatalog::configured_source()
                .is_some()
            {
                let catalog = Arc::clone(&catalog);
                tokio::spawn(async move {
                    let _ = catalog.refresh_from_env_default().await;
                });
            }
            klaus_agent_core::provider::model_catalog::with_model_catalog(provider, catalog)
        });
        let host = match host_capabilities_from_init(init.host_capabilities.as_ref()) {
            Ok(host) => host,
            Err(err) => {
                self.report_op_error(&submission_id, err.into()).await;
                return;
            }
        };
        let dynamic_tools = match dynamic_tools_from_init(&init.dynamic_tools, Arc::downgrade(self))
        {
            Ok(tools) => tools,
            Err(err) => {
                self.report_op_error(&submission_id, err.into()).await;
                return;
            }
        };
        let hooks = match command_hooks_from_init(&init.command_hooks) {
            Ok(hooks) => hooks,
            Err(error) => {
                self.report_op_error(&submission_id, error.into()).await;
                return;
            }
        };
        let pricing = match init.pricing.as_ref() {
            Some(entries) => {
                let entries = entries
                    .iter()
                    .map(|entry| {
                        scalar_runtime_string(entry.model.clone(), "pricing model")
                            .map(|model| (model, entry.pricing))
                    })
                    .collect::<Result<Vec<_>, _>>();
                match entries {
                    Ok(entries) => Some(entries),
                    Err(error) => {
                        self.report_op_error(&submission_id, error.into()).await;
                        return;
                    }
                }
            }
            None => None,
        };
        eprintln!(
            "[session-start] {submission_id} stage=init-host-capabilities elapsedMs={}",
            init_stage.elapsed().as_millis()
        );
        let mut targets = match targets_from_init(&init, self.credential_store.clone()).await {
            Ok(targets) => targets,
            Err(error) => {
                self.report_op_error(&submission_id, error.into()).await;
                return;
            }
        };
        if let Some(decorate) = self.provider_decorator.lock().unwrap().as_ref() {
            decorate_model_targets(&mut targets, decorate);
        }
        let mut advisor_target =
            match advisor_target_from_init(&init, self.credential_store.clone()).await {
                Ok(target) => target,
                Err(error) => {
                    self.report_op_error(&submission_id, error.into()).await;
                    return;
                }
            };
        if let (Some(target), Some(decorate)) = (
            advisor_target.as_mut(),
            self.provider_decorator.lock().unwrap().as_ref(),
        ) {
            decorate_model_target(target, decorate);
        }
        let mut feature_targets =
            match feature_targets_from_init(&init, self.credential_store.clone()).await {
                Ok(targets) => targets,
                Err(error) => {
                    self.report_op_error(&submission_id, error.into()).await;
                    return;
                }
            };
        if let Some(decorate) = self.provider_decorator.lock().unwrap().as_ref() {
            decorate_feature_targets(&mut feature_targets, decorate);
        }
        let prospective = SessionInitState {
            agents: init.agents.clone(),
            skills: init.skills.clone(),
            output_styles: init.output_styles.clone(),
            hooks,
            pricing,
            targets,
            advisor_target,
            feature_targets,
            raw: init,
            provider,
            mcp_server_configs: host.mcp_server_configs,
            capability_roots: host.capability_roots,
            selected_capability_roots: host.selected_capability_roots,
            dynamic_tools,
        };

        // Init and lazy create/resume/fork share one publication gate. Build
        // and validate the prospective state first, then linearize the commit:
        // either init's flushed ACK precedes a publisher reading this state, or
        // the publisher wins and init is rejected without overwriting it.
        let gate_wait = std::time::Instant::now();
        let _publication = self.session_publication_gate.lock().await;
        eprintln!(
            "[session-start] {submission_id} stage=init-gate-acquired waitMs={} elapsedMs={}",
            gate_wait.elapsed().as_millis(),
            init_stage.elapsed().as_millis()
        );
        let inflight = self
            .session_publication_inflight
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_some();
        debug_assert!(
            !inflight,
            "the publication gate cannot be acquired while an inflight Session remains"
        );
        if self.session_closing.load(Ordering::SeqCst) {
            self.report_op_error(&submission_id, "protocol session has shut down".into())
                .await;
            return;
        }
        if self.session.get().is_some() {
            self.report_op_error(
                &submission_id,
                "init_session must be sent before the protocol session is bound".into(),
            )
            .await;
            return;
        }
        let initialized = ProtocolEvent {
            submission_id: submission_id.clone().into(),
            initialized: Some(crate::types::InitSessionResult { accepted: true }),
            ..Default::default()
        };
        // The prospective state is not authoritative until its success frame
        // has been fully written and flushed. Cancellation or transport failure
        // therefore leaves the prior state intact for a later publisher.
        let mut ack_write = TransactionalWriteGuard::new(self.as_ref(), "InitSession ACK");
        if let Err(error) = self
            .write_unsolicited_event(initialized, Some(&mut ack_write.progress))
            .await
        {
            self.record_output_write_error(error);
            return;
        }
        *self
            .session_init
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = prospective;
        eprintln!(
            "[session-start] {submission_id} stage=init-ack-written elapsedMs={}",
            init_stage.elapsed().as_millis()
        );
    }

    async fn dispatch_side_question(self: &Arc<Self>, submission_id: String, question: Input) {
        let request = match self.begin_readonly_request(&submission_id) {
            Ok(request) => request,
            Err(error) => {
                self.report_op_error(&submission_id, error).await;
                return;
            }
        };
        let Some(task_guard) = self.readonly_tasks.begin() else {
            request.signal.cancel();
            self.finish_readonly_request(&submission_id, &request);
            return;
        };
        let task_id = task_guard.id();
        let inner = self.clone();
        let task = tokio::spawn(async move {
            let _task_guard = task_guard;
            let signal = request.signal.clone();
            let result: Result<_, String> = async {
                let session = tokio::select! {
                    biased;
                    _ = signal.cancelled() => {
                        return Err("side question cancelled".to_string());
                    }
                    result = inner.ensure_session() => {
                        result.map_err(|error| err_message(&error))?
                    }
                };
                session
                    .ask_side_question_input_with_signal(question, signal.clone())
                    .await
            }
            .await;
            let result = if signal.is_cancelled() {
                Err("side question cancelled".to_string())
            } else {
                result
            };
            let request_id = submission_id.clone();
            match result {
                Ok(result) => {
                    inner
                        .write(ProtocolEvent {
                            submission_id: submission_id.clone().into(),
                            side_question: Some(result),
                            ..Default::default()
                        })
                        .await;
                }
                Err(error) => inner.report_op_error(&submission_id, error.into()).await,
            }
            inner.finish_readonly_request(&request_id, &request);
        });
        self.readonly_tasks.attach(task_id, task.abort_handle());
    }

    async fn dispatch_review_session(
        self: &Arc<Self>,
        submission_id: String,
        target: klaus_agent_core::engine::session::ReviewTarget,
        user_facing_hint: Option<RuntimeString>,
        busy_guard: Option<BusySubmissionGuard>,
    ) {
        let request = match self.begin_readonly_request(&submission_id) {
            Ok(request) => request,
            Err(error) => {
                self.report_op_error(&submission_id, error).await;
                return;
            }
        };
        let Some(task_guard) = self.readonly_tasks.begin() else {
            request.signal.cancel();
            self.finish_readonly_request(&submission_id, &request);
            return;
        };
        let task_id = task_guard.id();
        let inner = self.clone();
        let task = tokio::spawn(async move {
            if let Some(guard) = busy_guard.as_ref() {
                guard.activate();
            }
            let _busy_guard = busy_guard;
            let _task_guard = task_guard;
            let signal = request.signal.clone();
            let result: Result<_, String> = async {
                let session = tokio::select! {
                    biased;
                    _ = signal.cancelled() => {
                        return Err("review cancelled".to_string());
                    }
                    result = inner.ensure_session() => {
                        result.map_err(|error| err_message(&error))?
                    }
                };
                session
                    .run_review_with_signal(
                        klaus_agent_core::engine::session::ReviewRequest {
                            target,
                            user_facing_hint,
                        },
                        signal.clone(),
                    )
                    .await
            }
            .await;
            let result = if signal.is_cancelled() {
                Err("review cancelled".to_string())
            } else {
                result
            };
            let request_id = submission_id.clone();
            match result {
                Ok(result) => {
                    inner
                        .write(ProtocolEvent {
                            submission_id: submission_id.clone().into(),
                            review: Some(result),
                            ..Default::default()
                        })
                        .await;
                }
                Err(error) => inner.report_op_error(&submission_id, error.into()).await,
            }
            inner.finish_readonly_request(&request_id, &request);
        });
        self.readonly_tasks.attach(task_id, task.abort_handle());
    }

    async fn dispatch_dream_now(self: &Arc<Self>, submission_id: String) {
        match self.ensure_session().await {
            Ok(session) => match session.dream_now().await {
                Ok(mut task) => {
                    self.write(ProtocolEvent {
                        submission_id: submission_id.clone().into(),
                        dream: Some(task.outcome()),
                        ..Default::default()
                    })
                    .await;
                    if let Some(done) = task.done.take() {
                        if let Some(task_guard) = self.completion_tasks.begin() {
                            let task_id = task_guard.id();
                            let inner = self.clone();
                            let task = tokio::spawn(async move {
                                let _task_guard = task_guard;
                                let completion = match done.await {
                                    Ok(completion) => completion,
                                    Err(_) => DreamNowCompletion {
                                        ok: false,
                                        reason: Some(
                                            "memory consolidation task ended without a result"
                                                .into(),
                                        ),
                                        files_touched: None,
                                        turn_count: None,
                                    },
                                };
                                inner
                                    .write(ProtocolEvent {
                                        submission_id: submission_id.into(),
                                        dream_completed: Some(completion),
                                        ..Default::default()
                                    })
                                    .await;
                            });
                            self.completion_tasks.attach(task_id, task.abort_handle());
                        }
                    }
                }
                Err(err) => self.report_op_error(&submission_id, err.into()).await,
            },
            Err(err) => self.report_op_error(&submission_id, err).await,
        }
    }

    async fn dispatch_predict_message(
        self: &Arc<Self>,
        submission_id: String,
        prompt: RuntimeString,
        source_turn_id: RuntimeString,
        effort: Option<klaus_agent_core::schemas::effort::EffortValue>,
    ) {
        let prompt = match scalar_runtime_string(prompt, "prompt") {
            Ok(value) => value,
            Err(error) => {
                self.report_op_error(&submission_id, error.into()).await;
                return;
            }
        };
        let source_turn_id = match scalar_runtime_string(source_turn_id, "sourceTurnId") {
            Ok(value) => value,
            Err(error) => {
                self.report_op_error(&submission_id, error.into()).await;
                return;
            }
        };
        let request = match self.begin_prediction_request(&submission_id) {
            Ok(request) => request,
            Err(error) => {
                self.report_op_error(&submission_id, error).await;
                return;
            }
        };
        let Some(task_guard) = self.readonly_tasks.begin() else {
            request.signal.cancel();
            self.finish_readonly_request(&submission_id, &request);
            return;
        };
        let task_id = task_guard.id();
        let inner = self.clone();
        let task = tokio::spawn(async move {
            let _task_guard = task_guard;
            let signal = request.signal.clone();
            let result: Result<_, String> = async {
                let session = tokio::select! {
                    biased;
                    _ = signal.cancelled() => return Err("prediction cancelled".to_string()),
                    result = inner.ensure_session() => result.map_err(|error| err_message(&error))?,
                };
                session
                    .predict_message_with_signal(prompt, source_turn_id, effort, signal.clone())
                    .await
            }
            .await;
            match result {
                Ok(suggestion) if !signal.is_cancelled() => {
                    inner
                        .write(ProtocolEvent {
                            submission_id: submission_id.clone().into(),
                            prediction: Some(crate::types::PredictionResult { suggestion }),
                            ..Default::default()
                        })
                        .await
                }
                Ok(_) => {
                    inner
                        .report_op_error(&submission_id, "prediction cancelled".into())
                        .await
                }
                Err(error) => inner.report_op_error(&submission_id, error.into()).await,
            }
            inner.finish_readonly_request(&submission_id, &request);
        });
        self.readonly_tasks.attach(task_id, task.abort_handle());
    }

    async fn dispatch_generate_title(
        self: &Arc<Self>,
        submission_id: String,
        instructions: Option<RuntimeString>,
        prompt: Option<RuntimeString>,
        persist: Option<bool>,
    ) {
        let instructions = match optional_scalar_runtime_string(instructions, "instructions") {
            Ok(instructions) => instructions,
            Err(error) => {
                self.report_op_error(&submission_id, error.into()).await;
                return;
            }
        };
        let prompt = match optional_scalar_runtime_string(prompt, "prompt") {
            Ok(prompt) => prompt,
            Err(error) => {
                self.report_op_error(&submission_id, error.into()).await;
                return;
            }
        };
        let request = match self.begin_readonly_request(&submission_id) {
            Ok(request) => request,
            Err(error) => {
                self.report_op_error(&submission_id, error).await;
                return;
            }
        };
        let Some(task_guard) = self.readonly_tasks.begin() else {
            request.signal.cancel();
            self.finish_readonly_request(&submission_id, &request);
            return;
        };
        let task_id = task_guard.id();
        let inner = self.clone();
        let task = tokio::spawn(async move {
            let _task_guard = task_guard;
            let signal = request.signal.clone();
            let result: Result<_, String> = async {
                let session = tokio::select! {
                    biased;
                    _ = signal.cancelled() => {
                        return Err("title generation cancelled".to_string());
                    }
                    result = inner.ensure_session() => {
                        result.map_err(|error| err_message(&error))?
                    }
                };
                let thread_id = session.id.clone();
                let title = session
                    .generate_title_with_signal(
                        instructions,
                        prompt,
                        persist.unwrap_or(false),
                        signal.clone(),
                    )
                    .await?;
                Ok((thread_id, title))
            }
            .await;
            let result = if signal.is_cancelled() {
                Err("title generation cancelled".to_string())
            } else {
                result
            };
            let request_id = submission_id.clone();
            match result {
                Ok((thread_id, title)) => {
                    inner
                        .write(ProtocolEvent {
                            submission_id: submission_id.clone().into(),
                            generated_title: Some(GenerateTitleOpResult { thread_id, title }),
                            ..Default::default()
                        })
                        .await;
                }
                Err(error) => inner.report_op_error(&submission_id, error.into()).await,
            }
            inner.finish_readonly_request(&request_id, &request);
        });
        self.readonly_tasks.attach(task_id, task.abort_handle());
    }

    async fn dispatch_archive_session(
        self: &Arc<Self>,
        submission_id: String,
        thread_id: RuntimeString,
    ) {
        let thread_id = match scalar_runtime_string(thread_id, "threadId") {
            Ok(thread_id) => thread_id,
            Err(error) => {
                self.report_op_error(&submission_id, error.into()).await;
                return;
            }
        };
        if self.lifecycle.archive_session.is_none() {
            self.report_op_error(
                &submission_id,
                "archive_session requires a session store".into(),
            )
            .await;
            return;
        }
        self.begin_destructive_lifecycle(&thread_id, "session archived");
        let inner = self.clone();
        let sid = submission_id.clone();
        self.enqueue(submission_id, move || -> ChainFuture {
            Box::pin(async move { inner.handle_archive(sid, thread_id).await })
        });
    }

    /// `ListSessions`: list persisted session threads through the host lifecycle hook.
    async fn dispatch_list_sessions(
        self: &Arc<Self>,
        submission_id: String,
        options: Option<ListOptions>,
    ) {
        match &self.lifecycle.list_sessions {
            Some(list) => match list(options.unwrap_or_default()).await {
                Ok(page) => {
                    self.write(ProtocolEvent {
                        submission_id: submission_id.clone().into(),
                        sessions: Some(page),
                        ..Default::default()
                    })
                    .await
                }
                Err(err) => self.report_op_error(&submission_id, err).await,
            },
            None => {
                self.report_op_error(
                    &submission_id,
                    "list_sessions requires a session store".into(),
                )
                .await
            }
        }
    }

    /// `ResumeSession`: bind a persisted session through the host resume hook.
    async fn dispatch_resume_session(
        self: &Arc<Self>,
        submission_id: String,
        thread_id: RuntimeString,
    ) {
        macro_rules! scalar {
            ($value:expr, $name:literal) => {
                match scalar_runtime_string($value, $name) {
                    Ok(value) => value,
                    Err(error) => {
                        self.report_op_error(&submission_id, error.into()).await;
                        return;
                    }
                }
            };
        }
        let thread_id = scalar!(thread_id, "threadId");
        self.bind_persisted_session(
            submission_id,
            thread_id,
            None,
            None,
            self.lifecycle.resume_session.clone(),
            "resume_session requires a session store",
        )
        .await;
    }

    /// `ForkSession`: bind a forked persisted session through the host fork hook.
    async fn dispatch_fork_session(
        self: &Arc<Self>,
        submission_id: String,
        thread_id: RuntimeString,
        new_thread_id: Option<RuntimeString>,
        before_message_uuid: Option<RuntimeString>,
        inherit_goal: Option<bool>,
    ) {
        macro_rules! scalar {
            ($value:expr, $name:literal) => {
                match scalar_runtime_string($value, $name) {
                    Ok(value) => value,
                    Err(error) => {
                        self.report_op_error(&submission_id, error.into()).await;
                        return;
                    }
                }
            };
        }
        let thread_id = scalar!(thread_id, "threadId");
        let new_thread_id = match optional_scalar_runtime_string(new_thread_id, "newThreadId") {
            Ok(value) => value,
            Err(error) => {
                self.report_op_error(&submission_id, error.into()).await;
                return;
            }
        };
        let before_message_uuid =
            match optional_scalar_runtime_string(before_message_uuid, "beforeMessageUuid") {
                Ok(value) => value,
                Err(error) => {
                    self.report_op_error(&submission_id, error.into()).await;
                    return;
                }
            };
        let bind = if inherit_goal == Some(true) {
            self.lifecycle.fork_session.clone().map(|fork| {
                Arc::new(
                    move |handlers,
                          source: String,
                          new_thread_id,
                          before_message_uuid|
                          -> CreateSessionFuture {
                        let fork = fork.clone();
                        Box::pin(async move {
                            let inherited_goal =
                                klaus_agent_core::goal::store::capture_goal_fork_snapshot(&source)
                                    .await?;
                            let session =
                                fork(handlers, source.clone(), new_thread_id, before_message_uuid)
                                    .await?;
                            if session.meta.role
                                == Some(klaus_agent_core::schemas::rollout::SessionRole::SideTask)
                            {
                                session.initiate_shutdown();
                                return Err(
                                    "ephemeral sessions cannot inherit a deferred goal".into()
                                );
                            }
                            let target = session
                                .id
                                .try_to_utf8()
                                .map_err(|error| -> BoxError { error.to_string().into() })?;
                            if let Err(error) =
                                klaus_agent_core::goal::store::copy_goal_snapshot_deferred(
                                    &target,
                                    &inherited_goal,
                                )
                                .await
                            {
                                session.initiate_shutdown();
                                return Err(Box::new(error) as BoxError);
                            }
                            Ok(session)
                        })
                    },
                ) as BindSession
            })
        } else {
            self.lifecycle.fork_session.clone()
        };
        self.bind_persisted_session(
            submission_id,
            thread_id,
            new_thread_id,
            before_message_uuid,
            bind,
            "fork_session requires a session store",
        )
        .await;
    }

    /// `UnarchiveSession`: chain a session unarchive through the host lifecycle hook.
    async fn dispatch_unarchive_session(
        self: &Arc<Self>,
        submission_id: String,
        thread_id: RuntimeString,
    ) {
        macro_rules! scalar {
            ($value:expr, $name:literal) => {
                match scalar_runtime_string($value, $name) {
                    Ok(value) => value,
                    Err(error) => {
                        self.report_op_error(&submission_id, error.into()).await;
                        return;
                    }
                }
            };
        }
        let thread_id = scalar!(thread_id, "threadId");
        if self.lifecycle.unarchive_session.is_none() {
            self.report_op_error(
                &submission_id,
                "unarchive_session requires a session store".into(),
            )
            .await;
            return;
        }
        let inner = self.clone();
        let sid = submission_id.clone();
        self.enqueue(submission_id, move || -> ChainFuture {
            Box::pin(async move { inner.handle_unarchive(sid, thread_id).await })
        });
    }

    /// `DeleteSession`: chain a destructive session delete through the host lifecycle hook.
    async fn dispatch_delete_session(
        self: &Arc<Self>,
        submission_id: String,
        thread_id: RuntimeString,
    ) {
        macro_rules! scalar {
            ($value:expr, $name:literal) => {
                match scalar_runtime_string($value, $name) {
                    Ok(value) => value,
                    Err(error) => {
                        self.report_op_error(&submission_id, error.into()).await;
                        return;
                    }
                }
            };
        }
        let thread_id = scalar!(thread_id, "threadId");
        if self.lifecycle.delete_session.is_none() {
            self.report_op_error(
                &submission_id,
                "delete_session requires a session store".into(),
            )
            .await;
            return;
        }
        self.begin_destructive_lifecycle(&thread_id, "session deleted");
        let inner = self.clone();
        let sid = submission_id.clone();
        self.enqueue(submission_id, move || -> ChainFuture {
            Box::pin(async move { inner.handle_delete(sid, thread_id).await })
        });
    }

    /// `UpdateTitle`: chain a persisted-session title update through the host lifecycle hook.
    async fn dispatch_update_title(
        self: &Arc<Self>,
        submission_id: String,
        thread_id: RuntimeString,
        title: RuntimeString,
    ) {
        macro_rules! scalar {
            ($value:expr, $name:literal) => {
                match scalar_runtime_string($value, $name) {
                    Ok(value) => value,
                    Err(error) => {
                        self.report_op_error(&submission_id, error.into()).await;
                        return;
                    }
                }
            };
        }
        let thread_id = scalar!(thread_id, "threadId");
        let title = scalar!(title, "title");
        if self.lifecycle.update_title.is_none() {
            self.report_op_error(
                &submission_id,
                "update_title requires a session store".into(),
            )
            .await;
            return;
        }
        let inner = self.clone();
        let sid = submission_id.clone();
        self.enqueue(submission_id, move || -> ChainFuture {
            Box::pin(async move { inner.handle_update_title(sid, thread_id, title).await })
        });
    }

    /// `RenameThread`: re-key a persisted thread through the host storage hook.
    /// The thread must not be live on this endpoint; the store's lifecycle
    /// locking rejects renames raced by another endpoint's writer.
    async fn dispatch_rename_thread(
        self: &Arc<Self>,
        submission_id: String,
        thread_id: RuntimeString,
        new_thread_id: RuntimeString,
    ) {
        macro_rules! scalar {
            ($value:expr, $name:literal) => {
                match scalar_runtime_string($value, $name) {
                    Ok(value) => value,
                    Err(error) => {
                        self.report_op_error(&submission_id, error.into()).await;
                        return;
                    }
                }
            };
        }
        let thread_id = scalar!(thread_id, "threadId");
        let new_thread_id = scalar!(new_thread_id, "newThreadId");
        if self.lifecycle.rename_thread.is_none() {
            self.report_op_error(
                &submission_id,
                "rename_thread requires a session store".into(),
            )
            .await;
            return;
        }
        if self.current_session_for_thread(&thread_id).is_some() {
            self.report_op_error(
                &submission_id,
                "rename_thread requires the thread to be inactive on this endpoint".into(),
            )
            .await;
            return;
        }
        let inner = self.clone();
        let sid = submission_id.clone();
        self.enqueue(submission_id, move || -> ChainFuture {
            Box::pin(async move {
                inner
                    .handle_rename_thread(sid, thread_id, new_thread_id)
                    .await
            })
        });
    }

    /// `UpdateThreadHostMetadata`: replace the thread's opaque host attachment
    /// through the thread's writer. A live writer serializes metadata with
    /// transcript writes without waiting for the model turn to finish.
    async fn dispatch_update_thread_host_metadata(
        self: &Arc<Self>,
        submission_id: String,
        thread_id: RuntimeString,
        host_metadata: Option<RuntimeJson>,
    ) {
        let thread_id = match scalar_runtime_string(thread_id, "threadId") {
            Ok(value) => value,
            Err(error) => {
                self.report_op_error(&submission_id, error.into()).await;
                return;
            }
        };
        if self.lifecycle.update_thread_host_metadata.is_none() {
            self.report_op_error(
                &submission_id,
                "update_thread_host_metadata requires a session store".into(),
            )
            .await;
            return;
        }
        if self.current_session_for_thread(&thread_id).is_some() {
            if let Err(error) = self
                .handle_update_thread_host_metadata(submission_id.clone(), thread_id, host_metadata)
                .await
            {
                self.report_op_error(&submission_id, error).await;
            }
            return;
        }
        let inner = self.clone();
        let sid = submission_id.clone();
        self.enqueue(submission_id, move || -> ChainFuture {
            Box::pin(async move {
                inner
                    .handle_update_thread_host_metadata(sid, thread_id, host_metadata)
                    .await
            })
        });
    }

    /// Append a run/rewind/set op to the serial chain.
    fn enqueue<F>(&self, submission_id: String, f: F)
    where
        F: FnOnce() -> ChainFuture + Send + 'static,
    {
        let job = ChainJob {
            submission_id,
            run: Box::new(f),
        };
        let rejected = {
            let chain = self.chain_tx.lock().unwrap();
            match chain.as_ref() {
                Some(tx) => tx.send(job).err().map(|error| error.0),
                None => Some(job),
            }
        };
        // A dropped reservation can schedule goal work and reacquire chain_tx.
        // Release the queue lock before retiring any rejected job's leases.
        drop(rejected);
    }

    /// Report an IMMEDIATE op failure (correlated `error` reply + a log line) so a
    /// client awaiting the reply rejects deterministically instead of hanging.
    async fn report_op_error(&self, submission_id: &str, err: BoxError) {
        let msg = err_message(&err);
        let conflict = msg.strip_prefix(klaus_agent_core::goal::service::GOAL_CONFLICT_PREFIX);
        let detail = conflict.map(|_| {
            let mut detail = RuntimeObject::new();
            detail.insert("code", RuntimeJson::from("GOAL_CONFLICT"));
            detail.insert("feature", RuntimeJson::from("goal"));
            ProtocolErrorDetail::Unknown(RuntimeJson::Object(detail))
        });
        let msg = conflict.unwrap_or(&msg).to_owned();
        self.write(ProtocolEvent {
            submission_id: (submission_id.to_string()).into(),
            error: Some(msg.clone().into()),
            error_detail: detail,
            ..Default::default()
        })
        .await;
        self.notify(
            NotificationLevel::Warn,
            format!("protocol: op failed: {msg}"),
        )
        .await;
    }

    /// Snapshot lifecycle events and every correlated result share one FIFO.
    /// An immediate direct write here could otherwise overtake a Requested
    /// event already dequeued by a slow unsolicited writer. Keep failures and
    /// their diagnostic notification behind the same Session order gate as
    /// successful decisions and authoritative queries.
    async fn report_agent_memory_snapshot_op_error_ordered(
        self: &Arc<Self>,
        session: Option<&Session>,
        submission_id: &str,
        err: BoxError,
    ) {
        let msg = err_message(&err);
        let error_event = ProtocolEvent {
            submission_id: submission_id.to_string().into(),
            error: Some(msg.clone().into()),
            ..Default::default()
        };
        let notification = ProtocolEvent {
            submission_id: RuntimeString::default(),
            event: Some(EngineEvent::Notification {
                level: NotificationLevel::Warn,
                message: format!("protocol: op failed: {msg}").into(),
            }),
            ..Default::default()
        };
        let inner = self.clone();
        let enqueue = move || {
            let error_sent =
                inner.enqueue_queue_ordered_write(UnsolicitedWrite::Event(error_event));
            if error_sent {
                let _ = inner.enqueue_queue_ordered_write(UnsolicitedWrite::Event(notification));
            }
            error_sent
        };
        let admitted = if let Some(session) = session {
            session.with_agent_memory_snapshot_order(enqueue).await
        } else {
            enqueue()
        };
        if !admitted {
            self.stop();
        }
    }

    async fn agent_memory_snapshot_request_id_or_report(
        self: &Arc<Self>,
        value: RuntimeString,
        submission_id: &str,
    ) -> Option<RuntimeString> {
        match scalar_runtime_string(value, "requestId") {
            Ok(value) => Some(value.into()),
            Err(error) => {
                self.report_agent_memory_snapshot_op_error_ordered(
                    self.session.get().map(Arc::as_ref),
                    submission_id,
                    error.into(),
                )
                .await;
                None
            }
        }
    }

    async fn report_input_error(&self, submission_id: &str, error: InputValidationError) {
        let message = error.message;
        self.write(ProtocolEvent {
            submission_id: (submission_id.to_string()).into(),
            error: Some(message.clone().into()),
            error_detail: Some(error.detail.into()),
            ..Default::default()
        })
        .await;
        self.notify(
            NotificationLevel::Warn,
            format!("protocol: input rejected: {message}"),
        )
        .await;
    }

    async fn report_memory_maintenance_error(
        &self,
        submission_id: &str,
        error: crate::types::MemoryMaintenanceQueryError,
    ) {
        let log_message = format!("{error}");
        let message = error.message;
        self.write(ProtocolEvent {
            submission_id: submission_id.to_string().into(),
            error: Some(message),
            error_detail: Some(error.detail.into()),
            ..Default::default()
        })
        .await;
        self.notify(
            NotificationLevel::Warn,
            format!("protocol: memory maintenance query failed: {log_message}"),
        )
        .await;
    }

    async fn report_mcp_runtime(
        &self,
        submission_id: &str,
        result: Result<McpRuntimeSnapshot, String>,
    ) {
        match result {
            Ok(runtime) => {
                self.write(ProtocolEvent {
                    submission_id: (submission_id.to_string()).into(),
                    mcp_runtime: Some(McpRuntimeResult {
                        runtime,
                        activation_scope: McpRuntimeScope::ActiveSession,
                    }),
                    ..Default::default()
                })
                .await;
            }
            Err(error) => self.report_op_error(submission_id, error.into()).await,
        }
    }

    async fn report_plugin_mutation(
        &self,
        submission_id: &str,
        admin: &Arc<dyn ProtocolPluginAdmin>,
        mutation: InstalledPluginLedgerMutation,
    ) {
        self.write(ProtocolEvent {
            submission_id: (submission_id.to_string()).into(),
            plugin_mutation: Some(Box::new(InstalledPluginMutationResult {
                persisted: true,
                applied_to_current_session: false,
                activation_scope: if mutation.published_to_loaded_sessions {
                    PluginActivationScope::NextTurn
                } else {
                    PluginActivationScope::EndpointRestart
                },
                restart_required: !mutation.published_to_loaded_sessions,
                plugin: mutation.plugin,
                installed_plugins: mutation.installed_plugins,
                runtime: admin.runtime_snapshot(),
            })),
            ..Default::default()
        })
        .await;
    }

    fn dispatch_plugin_admin(
        self: &Arc<Self>,
        submission_id: String,
        op: PluginAdminOp,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let this = self.clone();
        Box::pin(async move {
            let Some(admin) = this.plugin_admin.clone() else {
                this.report_op_error(
                    &submission_id,
                    "installed plugin administration is unavailable".into(),
                )
                .await;
                return;
            };
            match op {
                PluginAdminOp::List => match admin.list_installed_plugins().await {
                    Ok(installed_plugins) => {
                        this.write(ProtocolEvent {
                            submission_id: submission_id.clone().into(),
                            installed_plugins: Some(Box::new(installed_plugins)),
                            ..Default::default()
                        })
                        .await;
                    }
                    Err(error) => this.report_op_error(&submission_id, error).await,
                },
                PluginAdminOp::Runtime => {
                    this.write(ProtocolEvent {
                        submission_id: submission_id.clone().into(),
                        plugin_runtime: Some(Box::new(admin.runtime_snapshot())),
                        ..Default::default()
                    })
                    .await;
                }
                PluginAdminOp::SetEnabled { name, enabled } => {
                    if name.trim().is_empty() {
                        this.report_op_error(&submission_id, "plugin name is required".into())
                            .await;
                        return;
                    }
                    match admin.set_installed_plugin_enabled(&name, enabled).await {
                        Ok(Some(mutation)) => {
                            this.report_plugin_mutation(&submission_id, &admin, mutation)
                                .await;
                        }
                        Ok(None) => {
                            this.report_op_error(
                                &submission_id,
                                format!("installed plugin not found: {name}").into(),
                            )
                            .await;
                        }
                        Err(error) => this.report_op_error(&submission_id, error).await,
                    }
                }
                PluginAdminOp::Uninstall { name, remove_files } => {
                    if name.trim().is_empty() {
                        this.report_op_error(&submission_id, "plugin name is required".into())
                            .await;
                        return;
                    }
                    match admin.uninstall_plugin(&name, remove_files).await {
                        Ok(Some(mutation)) => {
                            this.report_plugin_mutation(&submission_id, &admin, mutation)
                                .await;
                        }
                        Ok(None) => {
                            this.report_op_error(
                                &submission_id,
                                format!("installed plugin not found: {name}").into(),
                            )
                            .await;
                        }
                        Err(error) => this.report_op_error(&submission_id, error).await,
                    }
                }
                PluginAdminOp::Install {
                    marketplace,
                    source_dir,
                    version,
                    expected_installed_version,
                } => {
                    if source_dir.trim().is_empty() {
                        this.report_op_error(
                            &submission_id,
                            "plugin source directory is required".into(),
                        )
                        .await;
                        return;
                    }
                    match admin
                        .install_plugin(
                            &marketplace,
                            &source_dir,
                            version.as_deref(),
                            expected_installed_version.as_deref(),
                        )
                        .await
                    {
                        Ok(Some(mutation)) => {
                            this.report_plugin_mutation(&submission_id, &admin, mutation)
                                .await;
                        }
                        Ok(None) => {
                            this.report_op_error(
                                &submission_id,
                                format!("plugin source has no loadable manifest: {source_dir}")
                                    .into(),
                            )
                            .await;
                        }
                        Err(error) => this.report_op_error(&submission_id, error).await,
                    }
                }
                PluginAdminOp::Catalog { roots } => match admin.list_plugin_catalog(&roots).await {
                    Ok(catalog) => {
                        this.write(ProtocolEvent {
                            submission_id: submission_id.clone().into(),
                            plugin_catalog: Some(Box::new(catalog)),
                            ..Default::default()
                        })
                        .await;
                    }
                    Err(error) => this.report_op_error(&submission_id, error).await,
                },
            }
        })
    }

    /// Single-flight session creation. A failed mint (e.g. resume of a bad thread
    /// id) is NOT cached, so a later op can retry.
    fn ensure_session(
        self: &Arc<Self>,
    ) -> futures::future::BoxFuture<'_, Result<Arc<Session>, BoxError>> {
        Box::pin(self.ensure_session_inner())
    }

    async fn ensure_session_inner(self: &Arc<Self>) -> Result<Arc<Session>, BoxError> {
        if self.session_closing.load(Ordering::SeqCst) {
            return Err("protocol session has shut down".into());
        }
        if self.session.get().is_some() {
            // A publisher sets OnceCell and releases its initial-event barrier
            // in one no-await commit while holding this gate. Waiting here
            // prevents another runtime thread from observing set-before-release.
            let _publication = self.session_publication_gate.lock().await;
            if self.session_closing.load(Ordering::SeqCst) {
                return Err("protocol session has shut down".into());
            }
            if let Some(session) = self.session.get().cloned() {
                return if session.is_disposed() {
                    Err("protocol session has shut down".into())
                } else {
                    Ok(session)
                };
            }
        }
        let result = self
            .start_session_publication(SessionPublicationRequest::Ensure)
            .await
            .map_err(|_| -> BoxError {
                if self.session_closing.load(Ordering::SeqCst) {
                    "protocol session has shut down".into()
                } else {
                    "session publication owner ended without a result".into()
                }
            })?;
        let session = result.map_err(|error| -> BoxError { error.into() })?;
        if self.session_closing.load(Ordering::SeqCst) {
            // This Session is already visible to the retained protocol teardown
            // (or to the destructive lifecycle job that set `session_closing`).
            // Do not start a competing, caller-owned shutdown: cancellation of
            // that caller after Session::disposed flips would let a later waiter
            // return before the original cleanup finished.
            session.initiate_shutdown();
            Err("protocol session has shut down".into())
        } else if session.is_disposed() {
            Err("protocol session has shut down".into())
        } else {
            Ok(session)
        }
    }

    /// Commit a fully prepared Session without a set-before-cleanup gap. Every
    /// lock, slot, channel, and barrier that can reject publication is acquired
    /// and validated before `OnceCell::set`; after it succeeds the remainder is
    /// limited to infallible state moves while the inflight mutex stays held.
    fn commit_session_publication(
        self: &Arc<Self>,
        session: &Arc<Session>,
        unpublished: UnpublishedSessionGuard,
        awaiting_bound_write: bool,
    ) -> Result<(), String> {
        let mut inflight = self
            .session_publication_inflight
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !inflight.as_ref().is_some_and(|publication| {
            publication.awaiting_bound_write == awaiting_bound_write
                && Arc::ptr_eq(&publication.session, session)
        }) {
            return Err("protocol session has shut down".to_string());
        }
        // A lazy Ensure has no Host-visible transport commit and therefore
        // loses when stop closes publication while holding this same inflight
        // mutex. A BoundSession reaches this function only after its frame was
        // flushed; that Host-visible transaction must still commit and then be
        // torn down by `BoundSessionPublication::commit` if stop won meanwhile.
        if !awaiting_bound_write && self.session_closing.load(Ordering::SeqCst) {
            drop(inflight);
            return Err("protocol session has shut down".to_string());
        }
        // Remove the fallback slot while retaining the mutex. The publication
        // owner below is deliberately declared afterwards, so unwind registers
        // retained Session cleanup before the mutex becomes observable again.
        let _slot = inflight.take();
        let mut publication_owner = Some(unpublished);
        let unpublished = publication_owner
            .as_mut()
            .expect("session publication owner is armed");
        unpublished.disarm_inflight_fallback();

        let adapters = unpublished
            .resources
            .adapters
            .as_ref()
            .ok_or_else(|| "session publication adapters are not armed".to_string())?;
        if adapters.queue_unsubscribe.is_none()
            || adapters.task_list_unsubscribe.is_none()
            || adapters.background_process_unsubscribe.is_none()
        {
            return Err("session publication adapters are incomplete".to_string());
        }
        let barrier = unpublished
            .resources
            .barrier
            .as_mut()
            .ok_or_else(|| "session publication barrier is not armed".to_string())?;
        if !matches!(
            barrier.drop_action,
            Some(PublicationBarrierDropAction::Discard)
        ) {
            return Err("session publication barrier is not discard-owned".to_string());
        }

        let mut queue_slot = self
            .queue_unsubscribe
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut task_slot = self
            .task_list_unsubscribe
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut background_process_slot = self
            .background_process_unsubscribe
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if queue_slot.is_some() || task_slot.is_some() || background_process_slot.is_some() {
            return Err("protocol session adapters were installed concurrently".to_string());
        }
        let admission = unpublished.admission.clone();
        let mut admission_state = admission
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if *admission_state != SessionPublicationAdmissionState::Pending {
            return Err("session publication admission is no longer pending".to_string());
        }
        let barrier_inner = barrier.inner.clone();
        let mut barrier_state = barrier_inner
            .queue_publication_barrier
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !barrier_state.active {
            return Err("session publication barrier is no longer active".to_string());
        }
        let sender = self
            .unsolicited_tx
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .cloned()
            .ok_or_else(|| "protocol unsolicited writer is closed".to_string())?;
        if self.session.get().is_some() {
            return Err("protocol session was bound concurrently".to_string());
        }
        if session.is_disposed() {
            return Err("protocol session has shut down".to_string());
        }
        #[cfg(test)]
        if self
            .session_publication_commit_panic_for_test
            .swap(false, Ordering::SeqCst)
        {
            panic!("injected pre-set session publication panic");
        }

        // Move the unsubscribe closures into a rollback owner before set. If
        // `OnceCell::set` unexpectedly unwinds or rejects, their Drop performs
        // the same identity-safe unsubscription as unpublished cleanup.
        let mut prepared_adapters = {
            let adapters = unpublished
                .resources
                .adapters
                .as_mut()
                .expect("validated session publication adapters remain armed");
            SessionBindingAdapters {
                queue_unsubscribe: adapters.queue_unsubscribe.take(),
                task_list_unsubscribe: adapters.task_list_unsubscribe.take(),
                background_process_unsubscribe: adapters.background_process_unsubscribe.take(),
            }
        };
        if self.session.set(session.clone()).is_err() {
            if let Some(adapters) = unpublished.resources.adapters.as_mut() {
                adapters.queue_unsubscribe = prepared_adapters.queue_unsubscribe.take();
                adapters.task_list_unsubscribe = prepared_adapters.task_list_unsubscribe.take();
                adapters.background_process_unsubscribe =
                    prepared_adapters.background_process_unsubscribe.take();
            }
            return Err("protocol session was bound concurrently".to_string());
        }

        // From this point onward the Host-visible BoundSession and the retained
        // OnceCell agree. No failure below can return an error or roll back the
        // publication; transport failure instead stops the published endpoint
        // after every commit lock and guard has been released.
        *queue_slot = prepared_adapters.queue_unsubscribe.take();
        *task_slot = prepared_adapters.task_list_unsubscribe.take();
        *background_process_slot = prepared_adapters.background_process_unsubscribe.take();
        *admission_state = SessionPublicationAdmissionState::Published;
        barrier_state.active = false;
        let buffered = std::mem::take(&mut barrier_state.buffered);
        barrier.drop_action = None;
        let mut fifo_closed = false;
        for write in buffered {
            if task_write_is_superseded(&write, &barrier_state.task_publication_floors) {
                continue;
            }
            if sender.send(write).is_err() {
                fifo_closed = true;
                break;
            }
        }
        drop(barrier_state);
        drop(admission_state);
        drop(task_slot);
        drop(background_process_slot);
        drop(queue_slot);
        drop(inflight);
        if let Some(unpublished) = publication_owner.take() {
            unpublished.finish_commit();
        }
        if fifo_closed {
            self.record_output_write_error(
                "protocol unsolicited FIFO closed during session publication".to_string(),
            );
            self.stop();
        }
        Ok(())
    }

    fn start_session_publication(
        self: &Arc<Self>,
        request: SessionPublicationRequest,
    ) -> oneshot::Receiver<Result<Arc<Session>, String>> {
        let (result_send, result) = oneshot::channel();
        let task_guard = self.completion_tasks.retain_child();
        let task_id = task_guard.id();
        let inner = self.clone();
        let task = tokio::spawn(async move {
            let result = std::panic::AssertUnwindSafe(
                inner.clone().run_session_publication(request, task_guard),
            )
            .catch_unwind()
            .await
            .unwrap_or_else(|_| Err("session publication owner panicked".to_string()));
            let _ = result_send.send(result);
        });
        self.completion_tasks.attach(task_id, task.abort_handle());
        result
    }

    async fn run_session_publication(
        self: Arc<Self>,
        request: SessionPublicationRequest,
        parent_guard: ActivityGuard,
    ) -> Result<Arc<Session>, String> {
        let task_id = parent_guard.id();
        let mut parent_guard = Some(parent_guard);
        let publication_guard = self.session_publication_gate.clone().lock_owned().await;
        match &request {
            SessionPublicationRequest::Ensure => {
                if let Some(session) = self.session.get().cloned() {
                    return Ok(session);
                }
            }
            SessionPublicationRequest::Bind { .. } => {
                if self.session.get().is_some() {
                    return Err(
                        "protocol session is already bound; open a new endpoint to resume or fork another session"
                            .to_string(),
                    );
                }
            }
        }
        if self.session_closing.load(Ordering::SeqCst) {
            return Err("protocol session has shut down".to_string());
        }

        let (session, bound_response) = match request {
            SessionPublicationRequest::Ensure => (
                (self.create_session)(self.create_session_args())
                    .await
                    .map_err(|error| error.to_string())?,
                None,
            ),
            SessionPublicationRequest::Bind {
                submission_id,
                thread_id,
                new_thread_id,
                before_message_uuid,
                bind,
            } => (
                bind(
                    self.create_session_args(),
                    thread_id,
                    new_thread_id,
                    before_message_uuid,
                )
                .await
                .map_err(|error| error.to_string())?,
                Some(submission_id),
            ),
        };
        if session.is_disposed() {
            session.initiate_shutdown();
            return Err("protocol session has shut down".to_string());
        }
        let mut unpublished =
            UnpublishedSessionGuard::new(&self, session.clone(), publication_guard);
        if let Some(resolver) = self.subagent_target_resolver.lock().unwrap().clone() {
            session.install_subagent_target_resolver(resolver);
        }
        let mut prepared = self
            .session_init
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        session
            .prepare_inference_targets(&mut prepared.targets, &mut prepared.advisor_target)
            .await?;
        session
            .prepare_feature_targets(&mut prepared.feature_targets)
            .await?;
        session.install_inference_targets(
            prepared.targets,
            model_source_from_init(&prepared.raw)?,
            prepared.pricing,
        )?;
        session.install_advisor_target(prepared.advisor_target)?;
        session.install_feature_targets(prepared.feature_targets)?;
        {
            let mut inflight = self
                .session_publication_inflight
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if inflight.is_some() {
                return Err("protocol session publication slot is already occupied".to_string());
            }
            *inflight = Some(SessionPublicationInflight {
                session: session.clone(),
                awaiting_bound_write: false,
            });
        }

        let queue_operation = self.queue_operation_gate.clone().lock_owned().await;
        unpublished.arm_queue_operation_guard(queue_operation);
        let barrier = self.begin_session_publication_barrier();
        unpublished.arm_barrier(barrier);
        let publication_admission = unpublished.admission();
        #[cfg(test)]
        {
            *self.session_publication_admission_for_test.lock().unwrap() =
                Some(Arc::downgrade(&publication_admission));
        }
        self.install_memory_maintenance_sink(&session, publication_admission.clone());
        self.install_subagent_event_sink(&session, publication_admission.clone());
        self.install_agent_notification_sink(&session, publication_admission.clone());
        self.install_extension_failure_sink(&session, publication_admission.clone());
        self.install_mcp_status_sink(&session, publication_admission.clone());
        let planning = session.planning_runtime();
        let planning_snapshot = planning
            .snapshot()
            .map_err(|error| klaus_agent_core::planning::runtime::host(&error))?;
        if planning_snapshot.revision == 0 {
            if let Some(mode) = self.create_session_args().init.collaboration_mode {
                let mode = klaus_agent_core::planning::CollaborationMode::parse(
                    &klaus_agent_core::planning::runtime::host(&mode),
                )?;
                planning
                    .set_mode(mode, None)
                    .map_err(|error| klaus_agent_core::planning::runtime::host(&error))?;
            }
        }
        let inner = Arc::downgrade(&self);
        let planning_admission = publication_admission.clone();
        planning.set_observer(Some(Arc::new(move |snapshot| {
            let Some(inner) = inner.upgrade() else {
                return;
            };
            planning_admission.enqueue(move || {
                let _ = inner.enqueue_queue_ordered_write(UnsolicitedWrite::Event(ProtocolEvent {
                    submission_id: RuntimeString::default(),
                    planning: Some(snapshot),
                    ..Default::default()
                }));
            });
        })));
        self.install_goal_sink(&session, publication_admission.clone());
        self.install_agent_memory_snapshot_sink(&session, publication_admission.clone())
            .await;
        unpublished.arm_adapters(SessionBindingAdapters {
            queue_unsubscribe: Some(
                self.install_queue_sink(&session, publication_admission.clone()),
            ),
            task_list_unsubscribe: Some(
                self.install_task_list_sink(&session, publication_admission.clone()),
            ),
            background_process_unsubscribe: Some(
                self.install_background_process_sink(&session, publication_admission),
            ),
        });
        self.install_rewind_controller(session.clone()).await;
        if self.session_closing.load(Ordering::SeqCst) {
            return Err("protocol session has shut down".to_string());
        }

        if let Some(submission_id) = bound_response {
            if !self.completion_tasks.detach_abort_handle(task_id) {
                // Register cleanup before the parent guard can make the
                // completion tracker appear idle.
                drop(unpublished);
                drop(parent_guard.take());
                return Err("protocol session has shut down".to_string());
            }
            {
                let mut inflight = self
                    .session_publication_inflight
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let Some(publication) = inflight.as_mut() else {
                    return Err("protocol session has shut down".to_string());
                };
                if !Arc::ptr_eq(&publication.session, &session) {
                    return Err("protocol session has shut down".to_string());
                }
                publication.awaiting_bound_write = true;
            }
            let publication = BoundSessionPublication {
                inner: self.clone(),
                session: session.clone(),
                unpublished: Some(unpublished),
                parent_guard: parent_guard.take(),
            };
            let receipt = self
                .enqueue_bound_session_write(
                    ProtocolEvent {
                        submission_id: submission_id.into(),
                        session: Some(BoundSession {
                            thread_id: session.id.clone(),
                            forked_from_id: session.meta.forked_from_id.clone(),
                        }),
                        ..Default::default()
                    },
                    publication,
                )
                .ok_or_else(|| "protocol server is stopping".to_string())?;
            receipt
                .await
                .map_err(|_| "BoundSession writer ended before its flush receipt".to_string())??;
            return Ok(session);
        }

        // Lazy Ensure has no transport transaction. It uses the same prepared,
        // no-await commit as bind and linearizes with stop on the inflight
        // mutex; the publication gate remains inside `unpublished` throughout.
        if self.session_closing.load(Ordering::SeqCst) {
            return Err("protocol session has shut down".to_string());
        }
        #[cfg(test)]
        {
            let gate = self
                .session_publication_ensure_commit_test_gate
                .lock()
                .unwrap()
                .clone();
            if let Some(gate) = gate {
                gate.pause().await;
            }
        }
        self.commit_session_publication(&session, unpublished, false)?;
        Ok(session)
    }

    fn start_unpublished_session_cleanup(
        self: &Arc<Self>,
        session: Arc<Session>,
        mut resources: SessionPublicationResources,
        admission: Arc<SessionPublicationAdmission>,
    ) {
        let task_guard = self.completion_tasks.retain_child();
        let task_id = task_guard.id();
        let inner = self.clone();
        // No protocol owner may depend on Tokio polling an aborted cleanup
        // future. Admission was closed synchronously by the unpublished guard;
        // release the inflight slot, subscriptions, publication barrier, and
        // operation/publication leases before the first cleanup await.
        admission.close();
        #[cfg(test)]
        {
            *inner.session_publication_sinks_for_test.lock().unwrap() =
                SessionPublicationTestSinks::default();
        }
        session.set_ordered_memory_maintenance_event_sink(None);
        session.set_subagent_event_sink(None);
        session.set_agent_notification_sink(None);
        resources.discard();
        let task = tokio::spawn(async move {
            let owner = UnpublishedSessionCleanupOwner {
                resources: Some(resources),
                task_guard: Some(task_guard),
            };
            // Do not unconditionally clear the Session's snapshot sink here.
            // A later publication may already have installed a new binding;
            // this rejected Session's old closure carries Closed admission,
            // only a Weak server reference, and dies with the Session.
            let mut rewind = inner.rewind_ctl.lock().await;
            let session_owner = Arc::downgrade(&session);
            if rewind
                .as_ref()
                .is_some_and(|binding| Weak::ptr_eq(&binding.session, &session_owner))
            {
                rewind.take();
            }
            // New init/publication may proceed once all protocol adapters and
            // the inflight slot are gone. Any delayed rewind cleanup is
            // identity-safe, and the Session-owned shutdown task was already
            // initiated synchronously by guard Drop.
            let _ = session.shutdown_and_wait().await;
            drop(owner);
        });
        self.completion_tasks.attach(task_id, task.abort_handle());
    }

    fn current_session_for_thread(&self, thread_id: &str) -> Option<Arc<Session>> {
        self.session
            .get()
            .filter(|session| session.id == thread_id)
            .cloned()
    }

    fn begin_destructive_lifecycle(&self, thread_id: &str, reason: &str) {
        let _stop = self
            .stop_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(session) = self.current_session_for_thread(thread_id) {
            self.session_closing.store(true, Ordering::SeqCst);
            self.session_closing_notify.notify_waiters();
            self.cancel_readonly_requests();
            session.initiate_shutdown();
            self.close_pending(reason);
        }
    }

    async fn shutdown_current_session(&self, thread_id: &str) -> Result<(), BoxError> {
        if let Some(session) = self.current_session_for_thread(thread_id) {
            session
                .shutdown_and_wait()
                .await
                .map_err(|err| -> BoxError { err.into() })?;
            *self.rewind_ctl.lock().await = None;
        }
        Ok(())
    }

    async fn handle_archive(
        self: &Arc<Self>,
        submission_id: String,
        thread_id: String,
    ) -> Result<(), BoxError> {
        let archive = self
            .lifecycle
            .archive_session
            .as_ref()
            .ok_or_else(|| -> BoxError { "archive_session requires a session store".into() })?
            .clone();
        self.shutdown_current_session(&thread_id).await?;
        let result = archive(thread_id).await?;
        self.write(ProtocolEvent {
            submission_id: submission_id.clone().into(),
            archived: Some(result),
            ..Default::default()
        })
        .await;
        Ok(())
    }

    async fn handle_unarchive(
        self: &Arc<Self>,
        submission_id: String,
        thread_id: String,
    ) -> Result<(), BoxError> {
        let result = if self.session_closing.load(Ordering::SeqCst)
            || self.stopped.load(Ordering::SeqCst)
        {
            UnarchiveSessionResult {
                thread_id: thread_id.clone().into(),
                status: UnarchiveSessionStatus::Shutdown,
                thread: None,
                error: Some("protocol session is shutting down".into()),
            }
        } else {
            let unarchive = self
                .lifecycle
                .unarchive_session
                .as_ref()
                .ok_or_else(|| -> BoxError { "unarchive_session requires a session store".into() })?
                .clone();
            // The chain task is a waiter, not the owner of a transaction that
            // may already have published its durable intent. Dropping this
            // JoinHandle detaches the owned work; its retained tracker guard
            // makes shutdown wait without ever force-aborting recovery.
            let retained = self.durable_lifecycle_tasks.retain_child();
            let operation_thread_id = thread_id.clone();
            let operation = tokio::spawn(async move {
                let _retained = retained;
                unarchive(operation_thread_id).await
            });
            match operation.await {
                Ok(result) => result,
                Err(error) => UnarchiveSessionResult {
                    thread_id: thread_id.clone().into(),
                    status: UnarchiveSessionStatus::PersistenceFailed,
                    thread: None,
                    error: Some(format!("owned unarchive task failed: {error}").into()),
                },
            }
        };
        self.write(ProtocolEvent {
            submission_id: submission_id.into(),
            unarchived: Some(result),
            ..Default::default()
        })
        .await;
        Ok(())
    }

    async fn handle_run_setup(
        self: &Arc<Self>,
        submission_id: String,
        request: crate::types::SetupRunRequest,
    ) -> Result<(), BoxError> {
        let session = self.ensure_session().await?;
        let result = session.run_setup_hooks(request).await;
        self.write(ProtocolEvent {
            submission_id: submission_id.into(),
            setup: Some(result),
            ..Default::default()
        })
        .await;
        Ok(())
    }

    async fn handle_delete(
        self: &Arc<Self>,
        submission_id: String,
        thread_id: String,
    ) -> Result<(), BoxError> {
        let delete = self
            .lifecycle
            .delete_session
            .as_ref()
            .ok_or_else(|| -> BoxError { "delete_session requires a session store".into() })?
            .clone();
        self.shutdown_current_session(&thread_id).await?;
        let result = delete(thread_id).await?;
        self.write(ProtocolEvent {
            submission_id: submission_id.clone().into(),
            deleted: Some(result),
            ..Default::default()
        })
        .await;
        Ok(())
    }

    async fn handle_update_title(
        self: &Arc<Self>,
        submission_id: String,
        thread_id: String,
        title: String,
    ) -> Result<(), BoxError> {
        let update = self
            .lifecycle
            .update_title
            .as_ref()
            .ok_or_else(|| -> BoxError { "update_title requires a session store".into() })?
            .clone();
        if let Some(session) = self.current_session_for_thread(&thread_id) {
            session.wait_for_active_turns().await;
            session
                .persist_title(&RuntimeString::from(title.clone()))
                .await
                .map_err(|err| -> BoxError { err.into() })?;
            self.write(ProtocolEvent {
                submission_id: submission_id.clone().into(),
                title: Some(UpdateTitleResult {
                    thread_id: thread_id.into(),
                    title: title.into(),
                }),
                ..Default::default()
            })
            .await;
            return Ok(());
        }
        let result = update(thread_id, title).await?;
        self.write(ProtocolEvent {
            submission_id: submission_id.clone().into(),
            title: Some(result),
            ..Default::default()
        })
        .await;
        Ok(())
    }

    async fn handle_rename_thread(
        self: &Arc<Self>,
        submission_id: String,
        thread_id: String,
        new_thread_id: String,
    ) -> Result<(), BoxError> {
        let rename = self
            .lifecycle
            .rename_thread
            .as_ref()
            .ok_or_else(|| -> BoxError { "rename_thread requires a session store".into() })?
            .clone();
        let result = rename(thread_id, new_thread_id).await?;
        self.write(ProtocolEvent {
            submission_id: submission_id.clone().into(),
            renamed: Some(result),
            ..Default::default()
        })
        .await;
        Ok(())
    }

    async fn handle_update_thread_host_metadata(
        self: &Arc<Self>,
        submission_id: String,
        thread_id: String,
        host_metadata: Option<RuntimeJson>,
    ) -> Result<(), BoxError> {
        let update = self
            .lifecycle
            .update_thread_host_metadata
            .as_ref()
            .ok_or_else(|| -> BoxError {
                "update_thread_host_metadata requires a session store".into()
            })?
            .clone();
        if let Some(session) = self.current_session_for_thread(&thread_id) {
            session
                .persist_host_metadata(host_metadata)
                .await
                .map_err(|err| -> BoxError { err.into() })?;
            self.write(ProtocolEvent {
                submission_id: submission_id.clone().into(),
                host_metadata_updated: Some(UpdateThreadHostMetadataResult {
                    thread_id: thread_id.into(),
                    updated: true,
                }),
                ..Default::default()
            })
            .await;
            return Ok(());
        }
        let result = update(thread_id, host_metadata).await?;
        self.write(ProtocolEvent {
            submission_id: submission_id.clone().into(),
            host_metadata_updated: Some(result),
            ..Default::default()
        })
        .await;
        Ok(())
    }

    fn create_session_args(self: &Arc<Self>) -> CreateSessionArgs {
        let approval: Arc<dyn ApprovalHandler> = Arc::new(ApprovalAdapter {
            inner: self.clone(),
        });
        let elicit: Arc<dyn ElicitHandler> = Arc::new(ElicitAdapter {
            inner: self.clone(),
        });
        let mcp_elicitation: Arc<dyn McpElicitationPresenter> = Arc::new(McpElicitationAdapter {
            inner: self.clone(),
        });
        let mcp_authorize: Arc<dyn McpAuthorizeHandler> = Arc::new(McpAuthorizeAdapter {
            inner: self.clone(),
        });
        let init = self
            .session_init
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        CreateSessionArgs {
            durable_permissions: self.durable_permissions.clone(),
            approval_handler: approval,
            elicit_handler: elicit,
            mcp_elicitation_presenter: mcp_elicitation,
            mcp_authorize_handler: mcp_authorize,
            init: init.raw,
            provider: init.provider,
            mcp_server_configs: init.mcp_server_configs,
            capability_roots: init.capability_roots,
            selected_capability_roots: init.selected_capability_roots,
            dynamic_tools: init.dynamic_tools,
            agents: init.agents,
            skills: init.skills,
            output_styles: init.output_styles,
            hooks: init.hooks,
            pricing: init.pricing,
        }
    }

    async fn install_rewind_controller(&self, session: Arc<Session>) {
        let session_owner = Arc::downgrade(&session);
        let adapter: Arc<dyn RewindSession> = Arc::new(SessionRewindAdapter(session));
        *self.rewind_ctl.lock().await = Some(SessionRewindBinding {
            session: session_owner,
            controller: RewindController::new(adapter),
        });
    }

    fn install_agent_notification_sink(
        self: &Arc<Self>,
        session: &Arc<Session>,
        admission: Arc<SessionPublicationAdmission>,
    ) {
        let inner = Arc::downgrade(self);
        session.set_agent_notification_sink(Some(Arc::new(move |notification| {
            let Some(inner) = inner.upgrade() else {
                return;
            };
            admission.enqueue(|| {
                let _ = inner.enqueue_queue_ordered_write(UnsolicitedWrite::Event(ProtocolEvent {
                    submission_id: RuntimeString::default(),
                    agent_notification: Some(notification.clone()),
                    ..Default::default()
                }));
            });
        })));
    }

    fn install_subagent_event_sink(
        self: &Arc<Self>,
        session: &Arc<Session>,
        admission: Arc<SessionPublicationAdmission>,
    ) {
        let inner = Arc::downgrade(self);
        let bound_session = Arc::downgrade(session);
        session.set_subagent_event_sink(Some(Arc::new(move |agent_id, event| {
            let Some(inner) = inner.upgrade() else {
                return;
            };
            let encoded = match event {
                klaus_agent_core::engine::session::SubagentStreamEvent::Event(event) => {
                    runtime_codec::encode_value(event)
                }
                klaus_agent_core::engine::session::SubagentStreamEvent::Notification(receipt) => {
                    runtime_codec::encode_value(receipt).map(|notification| {
                        let mut event = RuntimeObject::new();
                        event.insert("type", RuntimeJson::String("agent.notification".into()));
                        event.insert("notification", notification);
                        RuntimeJson::Object(event)
                    })
                }
                klaus_agent_core::engine::session::SubagentStreamEvent::Prompt(message) => {
                    runtime_codec::encode_value(message).map(|message| {
                        let mut event = RuntimeObject::new();
                        event.insert("type", RuntimeJson::String("user.message".into()));
                        event.insert("message", message);
                        RuntimeJson::Object(event)
                    })
                }
            };
            let Ok(event) = encoded else {
                return;
            };
            let context = bound_session
                .upgrade()
                .and_then(|session| session.subagent_context(agent_id));
            let payload = Self::subagent_stream_payload(agent_id, event, context.as_ref());
            admission.enqueue(|| {
                let _ = inner.enqueue_queue_ordered_write(UnsolicitedWrite::Event(ProtocolEvent {
                    submission_id: RuntimeString::default(),
                    subagent_event: Some(payload),
                    ..Default::default()
                }));
            });
        })));
    }

    fn subagent_stream_payload(
        agent_id: &str,
        event: RuntimeJson,
        context: Option<&klaus_agent_core::engine::subagent_lifecycle::SubagentContext>,
    ) -> RuntimeJson {
        let mut payload = RuntimeObject::new();
        payload.insert("agentId", RuntimeJson::String(agent_id.into()));
        payload.insert("event", event);
        if let Some(context) = context {
            if let Some(parent) = &context.parent_agent_id {
                payload.insert("parentAgentId", RuntimeJson::String(parent.clone()));
            }
            if let Some(turn_id) = &context.spawn_turn_id {
                payload.insert("spawnTurnId", RuntimeJson::String(turn_id.clone()));
            }
            payload.insert(
                "depth",
                runtime_codec::encode_value(&context.depth).expect("subagent depth codec"),
            );
        }
        RuntimeJson::Object(payload)
    }

    fn install_memory_maintenance_sink(
        self: &Arc<Self>,
        session: &Arc<Session>,
        admission: Arc<SessionPublicationAdmission>,
    ) {
        let inner = Arc::downgrade(self);
        let sink: klaus_agent_core::memory::maintenance::MemoryMaintenanceEventSink =
            Arc::new(move |event| {
                let Some(inner) = inner.upgrade() else {
                    return;
                };
                #[cfg(test)]
                {
                    let gate = inner
                        .session_publication_callback_test_gate
                        .lock()
                        .unwrap()
                        .clone();
                    if let Some(gate) = gate {
                        gate.pause();
                    }
                }
                admission.enqueue(|| {
                    let _ =
                        inner.enqueue_queue_ordered_write(UnsolicitedWrite::Event(ProtocolEvent {
                            submission_id: RuntimeString::default(),
                            memory_maintenance_event: Some(event),
                            ..Default::default()
                        }));
                });
            });
        #[cfg(test)]
        {
            self.session_publication_sinks_for_test
                .lock()
                .unwrap()
                .memory = Some(sink.clone());
        }
        session.set_ordered_memory_maintenance_event_sink(Some(sink));
    }

    /// Publish MCP connection lifecycle transitions as unsolicited events.
    ///
    /// The engine reports these from the transitions themselves, so they arrive
    /// while a dial is in progress rather than only after an op completes. They
    /// ride the same admission gate and ordered write path as the other
    /// unsolicited events: a transition reported during shutdown is dropped with
    /// them instead of racing the FIFO close.
    ///
    /// `submission_id` is deliberately empty. A dial usually happens inside a
    /// turn, but this answers no op, and tagging it with the in-flight submission
    /// would let a host mistake it for that turn's reply.
    fn install_mcp_status_sink(
        self: &Arc<Self>,
        session: &Arc<Session>,
        admission: Arc<SessionPublicationAdmission>,
    ) {
        let inner = Arc::downgrade(self);
        let sink: klaus_agent_core::mcp::types::McpStatusSink = Arc::new(move |update| {
            let Some(inner) = inner.upgrade() else {
                return;
            };
            admission.enqueue(|| {
                let _ = inner.enqueue_queue_ordered_write(UnsolicitedWrite::Event(ProtocolEvent {
                    submission_id: RuntimeString::default(),
                    mcp_server_status: Some(update),
                    ..Default::default()
                }));
            });
        });
        session.set_mcp_status_sink(Some(sink));
    }

    /// Publish every goal change as an unsolicited `threadGoal` frame.
    ///
    /// A goal changes from three directions the host cannot see: the model
    /// creating or finishing one, usage reaching the budget mid-turn, and a
    /// failed turn blocking it. Answering only op replies would leave a UI
    /// showing `active` through an hour of work that had already stopped.
    fn install_goal_sink(
        self: &Arc<Self>,
        session: &Arc<Session>,
        admission: Arc<SessionPublicationAdmission>,
    ) {
        let inner = Arc::downgrade(self);
        let sink: klaus_agent_core::goal::runtime::GoalObserver = Arc::new(move |snapshot| {
            let Some(inner) = inner.upgrade() else {
                return;
            };
            let mut event = ProtocolEvent {
                submission_id: RuntimeString::default(),
                thread_goal: snapshot.goal.as_ref().map(ProtocolThreadGoal::from),
                goal_cleared: snapshot.goal.is_none().then_some(true),
                goal_revision: Some(snapshot.revision),
                ..Default::default()
            };
            if let Err(error) = runtime_codec::encode_value(&event) {
                event = ProtocolEvent {
                    submission_id: RuntimeString::default(),
                    error: Some(
                        format!("Goal state is unavailable on the JSON surface: {error}").into(),
                    ),
                    ..Default::default()
                };
            }
            admission.enqueue(move || {
                let mut buffer = inner.goal_publication_buffer.lock().unwrap();
                if let Some(buffer) = buffer.as_mut() {
                    if buffer.len() == 64 {
                        buffer.remove(0);
                    }
                    buffer.push(event);
                } else {
                    let _ = inner.enqueue_queue_ordered_write(UnsolicitedWrite::Event(event));
                }
            });
        });
        session.set_goal_observer(Some(sink));
        if let Some(goal) = session.goal_observation_runtime() {
            let owner = Arc::downgrade(self);
            goal.set_idle_observer(Some(Arc::new(move || {
                if let Some(owner) = owner.upgrade() {
                    owner.schedule_goal_idle();
                }
            })));
        }
    }

    /// Publish a failed extension point (hook / plugin / skill) as an unsolicited
    /// event.
    ///
    /// These degrade silently by design so a broken hook cannot break a turn,
    /// which is why nothing else in the stream mentions them. Publication rides
    /// the same admission gate and ordered write path as the other unsolicited
    /// events, so a failure reported during shutdown is dropped with them rather
    /// than racing the FIFO close.
    fn install_extension_failure_sink(
        self: &Arc<Self>,
        session: &Arc<Session>,
        admission: Arc<SessionPublicationAdmission>,
    ) {
        let inner = Arc::downgrade(self);
        let sink: klaus_agent_core::extensions::failure::ExtensionFailureSink =
            Arc::new(move |failure| {
                let Some(inner) = inner.upgrade() else {
                    return;
                };
                admission.enqueue(|| {
                    let _ =
                        inner.enqueue_queue_ordered_write(UnsolicitedWrite::Event(ProtocolEvent {
                            submission_id: RuntimeString::default(),
                            extension_failure: Some(failure),
                            ..Default::default()
                        }));
                });
            });
        session.set_extension_failure_sink(Some(sink));
    }

    async fn install_agent_memory_snapshot_sink(
        self: &Arc<Self>,
        session: &Arc<Session>,
        admission: Arc<SessionPublicationAdmission>,
    ) {
        let inner = Arc::downgrade(self);
        let sink: Arc<
            dyn Fn(klaus_agent_core::memory::snapshot_decision::AgentMemorySnapshotDecisionAudit)
                + Send
                + Sync,
        > = Arc::new(move |audit| {
            let Some(inner) = inner.upgrade() else {
                return;
            };
            admission.enqueue(|| {
                let _ = inner.enqueue_queue_ordered_write(UnsolicitedWrite::Event(ProtocolEvent {
                    submission_id: RuntimeString::default(),
                    agent_memory_snapshot_decision: Some(audit),
                    ..Default::default()
                }));
            });
        });
        #[cfg(test)]
        {
            self.session_publication_sinks_for_test
                .lock()
                .unwrap()
                .snapshot = Some(sink.clone());
        }
        session
            .set_ordered_agent_memory_snapshot_decision_sink(Some(sink))
            .await;
    }

    fn begin_queue_publication_barrier(self: &Arc<Self>) -> QueuePublicationBarrierGuard {
        self.begin_publication_barrier(PublicationBarrierDropAction::Release)
    }

    fn begin_session_publication_barrier(self: &Arc<Self>) -> QueuePublicationBarrierGuard {
        self.begin_publication_barrier(PublicationBarrierDropAction::Discard)
    }

    fn begin_publication_barrier(
        self: &Arc<Self>,
        drop_action: PublicationBarrierDropAction,
    ) -> QueuePublicationBarrierGuard {
        let mut state = self
            .queue_publication_barrier
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(
            !state.active,
            "queue publication barriers must be serialized by the operation gate"
        );
        assert!(
            state.buffered.is_empty(),
            "a released queue publication barrier must not retain writes"
        );
        state.active = true;
        QueuePublicationBarrierGuard {
            inner: self.clone(),
            drop_action: Some(drop_action),
        }
    }

    fn enqueue_queue_ordered_write(&self, write: UnsolicitedWrite) -> bool {
        let mut state = self
            .queue_publication_barrier
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if task_write_is_superseded(&write, &state.task_publication_floors) {
            return true;
        }
        if state.active {
            state.buffered.push(write);
            return true;
        }
        let sender = self.unsolicited_tx.lock().unwrap().as_ref().cloned();
        sender.is_some_and(|sender| sender.send(write).is_ok())
    }

    /// Insert a correlated queue response ahead of every queue publication
    /// buffered by the currently active operation epoch. Holding the barrier
    /// mutex through the channel send closes the callback race between the
    /// authoritative capture/result and the release of later revisions.
    fn enqueue_queue_barrier_head_write(&self, write: UnsolicitedWrite) -> bool {
        let state = self
            .queue_publication_barrier
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(
            state.active,
            "a queue barrier-head write requires an active publication epoch"
        );
        let sender = self.unsolicited_tx.lock().unwrap().as_ref().cloned();
        sender.is_some_and(|sender| sender.send(write).is_ok())
    }

    fn enqueue_bound_session_write(
        &self,
        event: ProtocolEvent,
        publication: BoundSessionPublication,
    ) -> Option<oneshot::Receiver<Result<(), String>>> {
        {
            let state = self
                .queue_publication_barrier
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            assert!(
                state.active,
                "a BoundSession write requires an active publication epoch"
            );
        }
        // Never drop the publication transaction while holding the barrier
        // mutex: callback lock order is admission -> barrier, while transaction
        // Drop must close admission before retained cleanup discards the barrier.
        let sender = self
            .unsolicited_tx
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .cloned()?;
        let (written, receipt) = oneshot::channel();
        sender
            .send(UnsolicitedWrite::EventWithReceipt {
                event,
                receipt: UnsolicitedWriteReceipt {
                    written: Some(written),
                    progress: ProtocolWriteProgress::default(),
                    publication: Some(publication),
                },
            })
            .ok()?;
        Some(receipt)
    }

    fn complete_unsolicited_write_receipt(
        self: &Arc<Self>,
        mut receipt: UnsolicitedWriteReceipt,
        write_result: Result<(), String>,
    ) {
        let result = match write_result {
            Ok(()) => match receipt.publication.take() {
                Some(publication) => publication.commit(),
                None => Ok(()),
            },
            Err(error) => {
                // Drop the complete transaction before reporting failure. Its
                // synchronous guard Drop closes callback admission and starts
                // Session-owned shutdown before retained adapter cleanup.
                drop(receipt.publication.take());
                Err(error)
            }
        };
        if receipt.progress.flushed {
            if let Err(error) = &result {
                self.record_output_write_error(format!(
                    "protocol BoundSession publication failed after flush: {error}"
                ));
                self.stop();
            }
        }
        if let Some(written) = receipt.written.take() {
            let _ = written.send(result);
        }
    }

    fn advance_task_publication_floor(&self, session_id: &RuntimeString, publication_cutoff: u64) {
        let mut state = self
            .queue_publication_barrier
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(
            state.active,
            "a TodoV2 publication floor requires an active publication epoch"
        );
        state
            .task_publication_floors
            .entry(session_id.clone())
            .and_modify(|floor| *floor = (*floor).max(publication_cutoff))
            .or_insert(publication_cutoff);
    }

    fn release_queue_publication_barrier(&self) {
        let mut state = self
            .queue_publication_barrier
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !state.active {
            return;
        }
        let sender = self.unsolicited_tx.lock().unwrap().as_ref().cloned();
        let buffered = std::mem::take(&mut state.buffered);
        if let Some(sender) = sender {
            for write in buffered {
                if task_write_is_superseded(&write, &state.task_publication_floors) {
                    continue;
                }
                if sender.send(write).is_err() {
                    break;
                }
            }
        }
        // Keep the barrier mutex held until every buffered write has entered the
        // FIFO. A concurrent callback can only send after this point.
        state.active = false;
    }

    fn discard_queue_publication_barrier(&self) {
        let mut state = self
            .queue_publication_barrier
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !state.active {
            return;
        }
        state.buffered.clear();
        state.active = false;
    }

    fn install_queue_sink(
        self: &Arc<Self>,
        session: &Arc<Session>,
        admission: Arc<SessionPublicationAdmission>,
    ) -> Box<dyn Fn() + Send + Sync> {
        let inner = Arc::downgrade(self);
        let sink: klaus_agent_core::engine::message_queue::QueueStateSubscriber =
            Arc::new(move |snapshot| {
                let Some(inner) = inner.upgrade() else {
                    return;
                };
                admission.enqueue(|| {
                    let _ =
                        inner.enqueue_queue_ordered_write(UnsolicitedWrite::Event(ProtocolEvent {
                            submission_id: RuntimeString::default(),
                            queue_snapshot: Some(snapshot),
                            ..Default::default()
                        }));
                });
            });
        #[cfg(test)]
        {
            self.session_publication_sinks_for_test
                .lock()
                .unwrap()
                .queue = Some(sink.clone());
        }
        session.subscribe_queue_changes(sink)
    }

    fn install_task_list_sink(
        self: &Arc<Self>,
        session: &Arc<Session>,
        admission: Arc<SessionPublicationAdmission>,
    ) -> Box<dyn Fn() + Send + Sync> {
        let inner = Arc::downgrade(self);
        let sink: klaus_agent_core::tasks::task_store::TaskListChangeEnvelopeSubscriber =
            Arc::new(move |envelope| {
                let Some(inner) = inner.upgrade() else {
                    return;
                };
                #[cfg(test)]
                {
                    let gate = inner.todo_callback_test_gate.lock().unwrap().clone();
                    if let Some(gate) = gate {
                        gate.pause();
                    }
                }
                admission.enqueue(|| {
                    let _ = inner.enqueue_queue_ordered_write(UnsolicitedWrite::TaskListChange {
                        change: envelope.change,
                        publication_sequence: envelope.publication_sequence,
                    });
                });
            });
        #[cfg(test)]
        {
            self.session_publication_sinks_for_test.lock().unwrap().task = Some(sink.clone());
        }
        session.subscribe_task_list_change_envelopes(sink)
    }

    fn install_background_process_sink(
        self: &Arc<Self>,
        session: &Arc<Session>,
        admission: Arc<SessionPublicationAdmission>,
    ) -> Box<dyn Fn() + Send + Sync> {
        let inner = Arc::downgrade(self);
        let sink: klaus_agent_core::command_runtime::background::BackgroundProcessEventSink =
            Arc::new(move |event| {
                let Some(inner) = inner.upgrade() else {
                    return;
                };
                admission.enqueue(|| {
                    let _ =
                        inner.enqueue_queue_ordered_write(UnsolicitedWrite::Event(ProtocolEvent {
                            submission_id: RuntimeString::default(),
                            background_process_event: Some(event),
                            ..Default::default()
                        }));
                });
            });
        session.subscribe_background_process_changes(sink)
    }

    async fn enqueue_queue_mutation_result(
        &self,
        submission_id: &str,
        result: crate::types::QueueMutationResult,
    ) {
        if self.enqueue_queue_barrier_head_write(UnsolicitedWrite::Event(ProtocolEvent {
            submission_id: submission_id.into(),
            queue_mutation: Some(result),
            ..Default::default()
        })) {
            return;
        }
        self.report_op_error(submission_id, "protocol server is stopping".into())
            .await;
    }

    async fn bind_persisted_session(
        self: &Arc<Self>,
        submission_id: String,
        thread_id: String,
        new_thread_id: Option<String>,
        before_message_uuid: Option<String>,
        bind: Option<BindSession>,
        missing_message: &'static str,
    ) {
        let Some(bind) = bind else {
            self.report_op_error(&submission_id, missing_message.into())
                .await;
            return;
        };
        let result = self
            .start_session_publication(SessionPublicationRequest::Bind {
                submission_id: submission_id.clone(),
                thread_id,
                new_thread_id,
                before_message_uuid,
                bind,
            })
            .await;
        match result {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => self.report_op_error(&submission_id, error.into()).await,
            Err(_) => {
                self.report_op_error(
                    &submission_id,
                    "session publication owner ended without a result".into(),
                )
                .await
            }
        }
    }

    async fn write_pending_resume(&self, submission_id: &str, started: bool, reason: Option<&str>) {
        self.write(ProtocolEvent {
            submission_id: submission_id.into(),
            pending_resume: Some(PendingResumeResult {
                started,
                reason: reason.map(Into::into),
            }),
            ..Default::default()
        })
        .await;
    }

    fn finish_steer_turn(&self, reservation_token: uuid::Uuid) {
        self.run_admission
            .lock()
            .unwrap()
            .finish_steer(reservation_token);
    }

    fn reserve_goal_locked(
        self: &Arc<Self>,
        admission: &mut RunAdmission,
        prior: Option<(
            String,
            RuntimeString,
            crate::types::ProtocolTerminalNotificationDisposition,
        )>,
    ) -> GoalReservation {
        let signal = admission.child_token();
        let submission_id = format!("goal-{}", uuid::Uuid::new_v4());
        let token = uuid::Uuid::new_v4();
        let goal_decision = prior.map(|(prior_submission_id, goal_id, prior_presentation)| {
            Arc::new(GoalContinuationDecision {
                inner: Arc::downgrade(self),
                settled: AtomicBool::new(false),
                prior_presentation,
                pending: crate::types::ProtocolGoalContinuation {
                    decision_id: uuid::Uuid::new_v4().to_string().into(),
                    prior_submission_id: prior_submission_id.into(),
                    goal_id: Some(goal_id),
                    successor_submission_id: Some(submission_id.clone().into()),
                    pending: true,
                    continued: None,
                    prior_completion_notification_disposition: None,
                },
            })
        });
        admission.foreground.push_back(ForegroundReservation {
            token,
            submission_id: submission_id.clone(),
            early_steer: false,
            signal: signal.clone(),
        });
        admission.activate_front();
        GoalReservation {
            signal,
            submission_id: submission_id.clone(),
            guard: CurrentSubmissionGuard {
                inner: Arc::downgrade(self),
                submission_id,
                reservation_token: token,
                goal_reschedule: true,
                goal_decision,
            },
        }
    }

    fn enqueue_goal(self: &Arc<Self>, reservation: GoalReservation) {
        let GoalReservation {
            signal,
            guard,
            submission_id,
        } = reservation;
        let mut options = self
            .last_run_options
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_default();
        options.output_schema = None;
        options.display_metadata = None;
        options.meta_input = Some(true);
        let inner = self.clone();
        self.enqueue(submission_id.clone(), move || -> ChainFuture {
            Box::pin(async move {
                inner
                    .run_turn(submission_id, None, Some(options), signal, guard, true)
                    .await
            })
        });
    }

    fn schedule_goal_idle(self: &Arc<Self>) {
        let Some(session) = self.session.get() else {
            return;
        };
        if session
            .goal_runtime()
            .is_none_or(|runtime| runtime.continuation_suppressed())
            || self.stopped.load(Ordering::SeqCst)
            || self.session_closing.load(Ordering::SeqCst)
            || self.goal_publication_buffer.lock().unwrap().is_some()
        {
            return;
        }
        let reservation = {
            let mut admission = self.run_admission.lock().unwrap();
            if !admission.foreground.is_empty()
                || !admission.non_steerable.is_empty()
                || admission.signal.is_cancelled()
                || admission.pending_goal_stops > 0
            {
                return;
            }
            self.reserve_goal_locked(&mut admission, None)
        };
        self.enqueue_goal(reservation);
    }

    async fn reserve_goal_successor(
        self: &Arc<Self>,
        session: &Arc<Session>,
        prior: &str,
        signal: &AbortSignal,
        owner: &mut CurrentSubmissionGuard,
        prior_presentation: crate::types::ProtocolTerminalNotificationDisposition,
    ) -> Option<GoalReservation> {
        if signal.is_cancelled()
            || self.stopped.load(Ordering::SeqCst)
            || self.session_closing.load(Ordering::SeqCst)
        {
            return None;
        }
        let runtime = session.goal_runtime()?;
        let candidate = match runtime.continuation_if_idle(false).await {
            Ok(klaus_agent_core::goal::runtime::GoalContinuation::Continue { goal, .. }) => goal,
            _ => return None,
        };
        if self.goal_publication_buffer.lock().unwrap().is_some() {
            return None;
        }
        // The authoritative store read is outside admission. A reservation is
        // still provisional: the core provider boundary rechecks the full state.
        let mut admission = self.run_admission.lock().unwrap();
        if admission.foreground.len() != 1
            || !admission.non_steerable.is_empty()
            || admission
                .foreground
                .front()
                .is_none_or(|front| front.token != owner.reservation_token)
            || admission.pending_goal_stops > 0
            || admission.signal.is_cancelled()
            || signal.is_cancelled()
            || self.stopped.load(Ordering::SeqCst)
            || self.session_closing.load(Ordering::SeqCst)
            || !runtime.enabled()
            || runtime.continuation_suppressed()
            || session.collaboration_mode() == klaus_agent_core::planning::CollaborationMode::Plan
        {
            return None;
        }
        admission.foreground.pop_front();
        admission.steer_turn_id = None;
        let mut current = self.current_submission_id.lock().unwrap();
        if *current == prior {
            current.clear();
        }
        drop(current);
        owner.goal_reschedule = false;
        Some(self.reserve_goal_locked(
            &mut admission,
            Some((prior.to_owned(), candidate.goal_id, prior_presentation)),
        ))
    }

    async fn run_turn(
        self: &Arc<Self>,
        submission_id: String,
        input: Option<Input>,
        options: Option<RunOpOptions>,
        signal: AbortSignal,
        mut submission_guard: CurrentSubmissionGuard,
        goal_idle: bool,
    ) -> Result<(), BoxError> {
        let cancelled = {
            let mut admission = self.run_admission.lock().unwrap();
            let cancelled = self.stopped.load(Ordering::SeqCst)
                || self.session_closing.load(Ordering::SeqCst)
                || signal.is_cancelled();
            if !cancelled {
                // Pending continuation is not steerable until it owns a nonempty
                // batch; an empty/refused continuation never becomes a live turn.
                if input.is_some() {
                    admission.mark_steerable(
                        submission_guard.reservation_token,
                        self.stopped.load(Ordering::SeqCst),
                    );
                }
                *self.current_submission_id.lock().unwrap() = submission_id.clone();
            }
            cancelled
        };
        if cancelled {
            submission_guard.settle_goal_decision(false).await;
            if input.is_none() {
                self.write_pending_resume(&submission_id, false, Some("interrupted"))
                    .await;
            } else {
                self.report_op_error(&submission_id, "turn interrupted before admission".into())
                    .await;
            }
            return Ok(());
        }
        if input.is_some() {
            *self.last_completed_submission.lock().unwrap() = None;
        }
        let session = tokio::select! {
            biased;
            _ = signal.cancelled() => {
                // Session publication has its own retained owner. Cancel this
                // turn's wait without allowing a blocked lazy factory to pin
                // the foreground reservation or execute this input later.
                submission_guard.settle_goal_decision(false).await;
                if input.is_none() {
                    self.write_pending_resume(&submission_id, false, Some("interrupted")).await;
                } else {
                    self.report_op_error(&submission_id, "turn interrupted before admission".into()).await;
                }
                return Ok(());
            }
            session = self.ensure_session() => session?,
        };
        if !goal_idle {
            *self.last_run_options.lock().unwrap() = options.clone();
        }
        let planning_input = input.clone();
        let goal_before = session.thread_goal().await?;

        let pending_owner = Arc::downgrade(self);
        let owner_token = submission_guard.reservation_token;
        let pending_input: Arc<dyn Fn() -> bool + Send + Sync> = Arc::new(move || {
            pending_owner.upgrade().is_none_or(|inner| {
                inner.goal_publication_buffer.lock().unwrap().is_some()
                    || inner
                        .run_admission
                        .lock()
                        .unwrap()
                        .foreground
                        .iter()
                        .any(|reservation| reservation.token != owner_token)
            })
        });
        let mut turn_options = TurnOptions {
            signal: Some(signal.clone()),
            goal_pending_input: Some(pending_input),
            ..Default::default()
        };
        if let Some(opts) = &options {
            if let Some(m) = &opts.model {
                if !m.is_empty() {
                    turn_options.model = Some(m.clone());
                }
            }
            if let Some(effort) = &opts.effort {
                turn_options.effort = Some(effort.clone());
            }
            turn_options.ultra_mode = opts.ultra_mode;
            if opts.permission_mode.is_some() {
                return Err(
                    "Per-turn permissionMode is retired; use session/setRuntimePolicy instead."
                        .into(),
                );
            }
            if let Some(schema) = &opts.output_schema {
                if runtime_json_truthy(schema) {
                    let schema = scalar_json_value(schema, "run outputSchema")
                        .map_err(|error| -> BoxError { error.into() })?;
                    if let Some(obj) = schema.as_object() {
                        turn_options.output_schema = Some(obj.clone());
                    }
                }
            }
            if let Some(sp) = &opts.system_prompt_append {
                if !sp.is_empty() {
                    turn_options.system_prompt_append = Some(sp.clone());
                }
            }
            turn_options.display_metadata = opts.display_metadata.clone();
            turn_options.meta_input = opts.meta_input.unwrap_or(false);
        }

        if let Some(decision) = submission_guard.goal_decision.clone() {
            turn_options.on_provider_request_admitted = Some(Arc::new(move || {
                let write = decision.settle(true);
                Box::pin(async move {
                    if let Some(write) = write {
                        write.await.map_err(|error| {
                            RuntimeString::from(format!(
                                "continuation disposition writer failed: {error}"
                            ))
                        })?;
                    }
                    Ok(())
                })
            }));
        }

        let input = if let Some(input) = input {
            Some(
                match session.validate_input_for_turn(input, &turn_options) {
                    Ok(input) => input,
                    Err(error) => {
                        self.finish_steer_turn(submission_guard.reservation_token);
                        let message = error.message.clone();
                        submission_guard.settle_failed_goal_decision().await;
                        self.report_input_error(&submission_id, error.clone()).await;
                        self.write(ProtocolEvent {
                            submission_id: submission_id.clone().into(),
                            terminal_notification_disposition: Some(
                                crate::types::ProtocolTerminalNotificationDisposition::Inherit,
                            ),
                            event: Some(EngineEvent::TurnFailed {
                                inference: None,
                                error: EngineError {
                                    code: EngineErrorCode::InvalidInput,
                                    message: message.into(),
                                    cause: Some(Box::new(error)),
                                    request_id: None,
                                    provider_kind: None,
                                    retry_after_ms: None,
                                },
                            }),
                            ..Default::default()
                        })
                        .await;
                        return Ok(());
                    }
                },
            )
        } else {
            None
        };

        if signal.is_cancelled() {
            self.report_op_error(
                &submission_id,
                "turn interrupted before plan admission".into(),
            )
            .await;
            return Ok(());
        }
        let planning_claimed = match session
            .planning_runtime()
            .claim_submission(&submission_id.clone().into(), planning_input.as_ref())
        {
            Ok(claimed) => claimed,
            Err(error) => {
                self.finish_steer_turn(submission_guard.reservation_token);
                self.report_op_error(
                    &submission_id,
                    klaus_agent_core::planning::runtime::host(&error).into(),
                )
                .await;
                return Ok(());
            }
        };
        let mut planning_provider_started = false;
        // Capture the pre-turn working-tree checkpoint only after input
        // validation. A rejected document must not create rewind state.
        if input.is_some() {
            if let Some(binding) = self.rewind_ctl.lock().await.as_mut() {
                binding.controller.before_turn().await;
            }
        }

        // Mirror core's run(): finalText = last agent_message text; usage/stop
        // reason from turn.completed — so we can emit a `result` after streaming.
        let mut final_text = RuntimeString::default();
        let mut usage: Option<Usage> = None;
        let mut stop_reason = TerminalReason::Completed;
        let mut completed_turn_id: Option<RuntimeString> = None;

        let turn = if goal_idle {
            let expected_goal = submission_guard
                .goal_decision
                .as_ref()
                .and_then(|decision| decision.pending.goal_id.clone());
            match session
                .run_goal_idle_streamed_for(expected_goal.as_ref(), turn_options)
                .await
            {
                Ok(Some(turn)) => turn,
                Ok(None) => {
                    submission_guard.goal_reschedule = false;
                    if let Some(expected) = expected_goal.as_ref() {
                        if let Some(current) = session.thread_goal().await? {
                            // The rejected candidate cannot own another generation.
                            // Release it once and let a fresh reservation read that intent.
                            submission_guard.goal_reschedule =
                                current.status.is_active() && &current.goal_id != expected;
                        }
                    }
                    submission_guard.settle_goal_decision(false).await;
                    return Ok(());
                }
                Err(error) => {
                    submission_guard.goal_reschedule = false;
                    submission_guard.settle_goal_decision(false).await;
                    return Err(error.try_to_utf8().unwrap_or_default().into());
                }
            }
        } else if let Some(input) = input {
            session.run_streamed_validated(input, turn_options)
        } else {
            match session.resume_pending_streamed(turn_options).await {
                Ok(Some(turn)) => {
                    {
                        let mut admission = self.run_admission.lock().unwrap();
                        if !signal.is_cancelled() {
                            admission.mark_steerable(
                                submission_guard.reservation_token,
                                self.stopped.load(Ordering::SeqCst),
                            );
                        }
                    }
                    *self.last_completed_submission.lock().unwrap() = None;
                    if let Some(binding) = self.rewind_ctl.lock().await.as_mut() {
                        binding.controller.before_turn().await;
                    }
                    self.write_pending_resume(&submission_id, true, None).await;
                    turn
                }
                Ok(None) => {
                    self.finish_steer_turn(submission_guard.reservation_token);
                    self.write_pending_resume(&submission_id, false, Some("empty"))
                        .await;
                    return Ok(());
                }
                Err(error) => {
                    *self.last_completed_submission.lock().unwrap() = None;
                    return Err(scalar_runtime_string(error, "pending continuation error")?.into());
                }
            }
        };
        let mut events = turn.events;
        // One actual turn can have a terminal event followed by a settlement
        // failure. Bound that pair while its core accounting and leases unwind.
        let mut terminal_events = Vec::with_capacity(2);
        while let Some(event) = events.next().await {
            match event {
                EngineEvent::InputRejected {
                    correlation_id: Some(correlation_id),
                    message,
                    detail,
                } => {
                    self.write(ProtocolEvent {
                        submission_id: (correlation_id),
                        error: Some(message),
                        error_detail: Some(detail.into()),
                        ..Default::default()
                    })
                    .await;
                    continue;
                }
                EngineEvent::InputApplied {
                    correlation_id: Some(correlation_id),
                } => {
                    self.write(ProtocolEvent {
                        submission_id: (correlation_id),
                        steered: Some(SteerResult { applied: true }),
                        ..Default::default()
                    })
                    .await;
                    continue;
                }
                event => {
                    let turn_goal_metadata = match &event {
                        EngineEvent::TurnStarted { turn_id } => session
                            .turn_index_snapshot()
                            .turn_goal_metadata
                            .get(turn_id)
                            .cloned(),
                        _ => None,
                    };
                    match &event {
                        EngineEvent::StreamRequestStart { .. } => {
                            planning_provider_started = true;
                        }
                        EngineEvent::TurnStarted { .. } => {
                            // Goal continuations start another core turn under the
                            // same foreground owner after its terminal steer gap.
                            self.run_admission.lock().unwrap().mark_steerable(
                                submission_guard.reservation_token,
                                self.stopped.load(Ordering::SeqCst),
                            );
                        }
                        EngineEvent::ItemCompleted {
                            item: ThreadItem::ProposedPlan { plan, .. },
                            ..
                        } => {
                            if plan.status == "proposed" {
                                final_text = plan.content.clone();
                            }
                        }
                        EngineEvent::ItemCompleted {
                            item: ThreadItem::AgentMessage { text, phase, .. },
                            ..
                        } => {
                            if phase.as_ref().is_none_or(|phase| phase != "commentary") {
                                final_text = text.clone();
                            }
                        }
                        EngineEvent::TurnCompleted {
                            turn_id,
                            usage: u,
                            stop_reason: sr,
                            ..
                        } => {
                            self.finish_steer_turn(submission_guard.reservation_token);
                            completed_turn_id = Some(turn_id.clone());
                            usage = u.clone();
                            stop_reason = *sr;
                        }
                        EngineEvent::TurnFailed { .. } => {
                            self.finish_steer_turn(submission_guard.reservation_token);
                            stop_reason = TerminalReason::Error;
                        }
                        _ => {}
                    }
                    let terminal = matches!(
                        &event,
                        EngineEvent::TurnCompleted { .. } | EngineEvent::TurnFailed { .. }
                    );
                    let frame = ProtocolEvent {
                        submission_id: (submission_id.clone()).into(),
                        goal_continuation: turn_goal_metadata
                            .as_ref()
                            .map(|metadata| metadata.goal_continuation),
                        input_origin: turn_goal_metadata
                            .as_ref()
                            .map(|metadata| metadata.input_origin.clone()),
                        goal_id: turn_goal_metadata
                            .as_ref()
                            .and_then(|metadata| metadata.goal_id.clone()),
                        event: Some(event),
                        ..Default::default()
                    };
                    if terminal {
                        if terminal_events.len() == 2 {
                            return Err("one turn emitted too many terminal events".into());
                        }
                        terminal_events.push(frame);
                    } else {
                        self.write(frame).await;
                    }
                }
            }
        }

        // Capture presentation after actual settlement and before any terminal
        // publication. Durable Active presentation is independent of mode,
        // feature enablement and the current accounting scope.
        let terminal_presentation = if stop_reason == TerminalReason::Completed {
            match session.thread_goal().await {
                Ok(goal) if goal.as_ref().is_some_and(|goal| goal.status.is_active()) => {
                    crate::types::ProtocolTerminalNotificationDisposition::Suppress
                }
                Ok(_) => crate::types::ProtocolTerminalNotificationDisposition::Inherit,
                Err(error) => {
                    let reason: String = error.to_string().chars().take(160).collect();
                    eprintln!("[goal-terminal-presentation] lookup failed: {reason}");
                    self.notify(NotificationLevel::Warn,
                        "Goal state could not be read for terminal notification presentation; ordinary completion was suppressed.".to_owned()).await;
                    crate::types::ProtocolTerminalNotificationDisposition::Suppress
                }
            }
        } else {
            crate::types::ProtocolTerminalNotificationDisposition::Inherit
        };
        // Classify an unadmitted candidate before resolving false: its typed
        // failure remains independent while its predecessor stays suppressed.
        let actual_failure = terminal_events
            .iter()
            .any(|frame| matches!(&frame.event, Some(EngineEvent::TurnFailed { .. })));
        if actual_failure {
            submission_guard.settle_failed_goal_decision().await;
        } else {
            submission_guard.settle_goal_decision(false).await;
        }
        if !terminal_events.is_empty() {
            if let Some(next) = self
                .reserve_goal_successor(
                    &session,
                    &submission_id,
                    &signal,
                    &mut submission_guard,
                    terminal_presentation,
                )
                .await
            {
                let decision = next
                    .guard
                    .goal_decision
                    .as_ref()
                    .expect("a successor owns its disposition");
                self.write(ProtocolEvent {
                    submission_id: RuntimeString::default(),
                    goal_continuation_decision: Some(decision.pending.clone()),
                    ..Default::default()
                })
                .await;
                self.enqueue_goal(next);
            }
        }
        if stop_reason == TerminalReason::Aborted {
            // The core turn has stopped. Retire its reservation before publishing
            // interruption so newly accepted input can join the serial chain
            // while this run finishes its remaining protocol publication.
            drop(submission_guard);
        }
        for mut terminal in terminal_events {
            // A successful core terminal superseded by real settlement failure
            // cannot release Done before the failed outcome and its aliases.
            let superseded_success = actual_failure
                && matches!(
                    &terminal.event,
                    Some(EngineEvent::TurnCompleted {
                        stop_reason: TerminalReason::Completed,
                        ..
                    })
                );
            terminal.terminal_notification_disposition = Some(if superseded_success {
                crate::types::ProtocolTerminalNotificationDisposition::Suppress
            } else {
                terminal_presentation
            });
            self.write(terminal).await;
        }

        if planning_claimed && !planning_provider_started {
            let _ = session
                .planning_runtime()
                .fail_unstarted_submission(&submission_id.clone().into());
        }
        let mut structured_output: Option<RuntimeJson> = None;
        if let Some(opts) = &options {
            let gated = opts
                .output_schema
                .as_ref()
                .map(runtime_json_truthy)
                .unwrap_or(false);
            if gated {
                structured_output = runtime_json::parse(&final_text).ok();
            }
        }
        let final_message_id = completed_turn_id
            .as_ref()
            .and_then(|turn_id| {
                session
                    .turn_index_snapshot()
                    .turn_final_message
                    .get(turn_id)
                    .cloned()
            })
            .map(|assistant_id| {
                // A Responses request may contain commentary and final message items.
                // The Turn result names the displayed final item; the history index
                // continues to address the enclosing persisted assistant record.
                session
                    .history()
                    .into_iter()
                    .find_map(|message| {
                        let Message::Assistant(assistant) = message else {
                            return None;
                        };
                        if assistant.uuid != assistant_id {
                            return None;
                        }
                        assistant
                            .content
                            .into_iter()
                            .filter_map(|block| match block {
                                ContentBlock::Text(text)
                                    if text
                                        .phase
                                        .as_ref()
                                        .is_none_or(|phase| phase != "commentary") =>
                                {
                                    Some(text)
                                }
                                _ => None,
                            })
                            .max_by_key(|text| {
                                text.phase
                                    .as_ref()
                                    .is_some_and(|phase| phase == "final_answer")
                            })
                            .and_then(|text| text.message_id)
                    })
                    .unwrap_or(assistant_id)
            });
        // Enabling goal tools is ordinary session configuration. Only a turn
        // that started with an active goal or changed its goal belongs to that
        // driver. A later user turn after a completed goal is ordinary again.
        let goal_after = session.thread_goal().await?;
        let goal_owned = goal_before
            .as_ref()
            .is_some_and(|goal| goal.status.is_active())
            || goal_before != goal_after;
        // Publish eligibility before the terminal result can trigger a host's
        // follow-up request. Interrupt clears it under the same admission lock.
        {
            let _admission = self.run_admission.lock().unwrap();
            if stop_reason == TerminalReason::Completed
                && !goal_owned
                && !signal.is_cancelled()
                && options
                    .as_ref()
                    .is_none_or(|options| options.meta_input != Some(true))
            {
                *self.last_completed_submission.lock().unwrap() = Some(submission_id.clone());
            }
        }
        self.write(ProtocolEvent {
            submission_id: submission_id.clone().into(),
            terminal_notification_disposition: Some(terminal_presentation),
            result: Some(RunResult {
                stop_reason,
                usage,
                final_text,
                final_message_id,
                structured_output,
            }),
            ..Default::default()
        })
        .await;
        Ok(())
    }

    async fn handle_rewind(
        self: &Arc<Self>,
        submission_id: String,
        num_turns: u32,
        scope_arg: Option<RewindScope>,
    ) {
        let scope = scope_arg.unwrap_or(RewindScope::Both);
        let mut guard = self.rewind_ctl.lock().await;
        match guard.as_mut() {
            None => {
                self.write(ProtocolEvent {
                    submission_id: submission_id.clone().into(),
                    rewound: Some(RewindOutcome {
                        scope,
                        dropped_turns: 0,
                        remaining_turns: 0,
                        files_restored: false,
                        error: Some("no session to rewind".into()),
                    }),
                    ..Default::default()
                })
                .await;
            }
            Some(binding) => {
                let outcome = binding.controller.rewind(num_turns as f64, scope).await;
                self.write(ProtocolEvent {
                    submission_id: submission_id.clone().into(),
                    rewound: Some(outcome),
                    ..Default::default()
                })
                .await;
            }
        }
    }

    async fn handle_compact(
        self: &Arc<Self>,
        submission_id: String,
        instructions: Option<RuntimeString>,
        selection: Option<crate::types::CompactSelection>,
    ) -> Result<(), BoxError> {
        let progress_id = new_uuid();
        self.write(ProtocolEvent {
            submission_id: (submission_id.clone()).into(),
            compact_progress: Some(CompactProgress {
                id: progress_id.clone().into(),
                phase: CompactProgressPhase::Started,
                trigger: CompactTrigger::Manual,
                mode: CompactionMode::Full,
                result: None,
                error: None,
            }),
            ..Default::default()
        })
        .await;
        let outcome = match async {
            let session = self.ensure_session().await?;
            let outcome = match selection {
                Some(selection) => {
                    let direction = match selection.direction {
                        crate::types::CompactSelectionDirection::From => klaus_agent_core::context::compact::full_compact::PartialCompactionDirection::From,
                        crate::types::CompactSelectionDirection::UpTo => klaus_agent_core::context::compact::full_compact::PartialCompactionDirection::UpTo,
                    };
                    session.compact_selected(instructions, selection.message_id, direction).await
                }
                None => session.compact(instructions).await,
            };
            outcome.map_err(|error| -> BoxError { error.into() })
        }
        .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                let message = err_message(&error);
                self.write(ProtocolEvent {
                    submission_id: submission_id.clone().into(),
                    compact_progress: Some(CompactProgress {
                        id: progress_id.into(),
                        phase: CompactProgressPhase::Failed,
                        trigger: CompactTrigger::Manual,
                        mode: CompactionMode::Full,
                        result: None,
                        error: Some(message.into()),
                    }),
                    ..Default::default()
                })
                .await;
                return Err(error);
            }
        };
        let result = CompactSessionResult::from(outcome);
        self.write(ProtocolEvent {
            submission_id: (submission_id.clone()).into(),
            compact_progress: Some(CompactProgress::manual_result(
                progress_id.into(),
                result.clone(),
            )),
            ..Default::default()
        })
        .await;
        self.write(ProtocolEvent {
            submission_id: submission_id.clone().into(),
            compact: Some(result),
            ..Default::default()
        })
        .await;
        Ok(())
    }

    async fn notify(&self, level: NotificationLevel, message: String) {
        // Notifications aren't tied to a submission; tag with an empty id.
        self.write(ProtocolEvent {
            submission_id: (String::new()).into(),
            event: Some(EngineEvent::Notification {
                level,
                message: message.into(),
            }),
            ..Default::default()
        })
        .await;
    }

    fn write(&self, evt: ProtocolEvent) -> futures::future::BoxFuture<'_, ()> {
        // Event serialization owns a large payload. Keep its asynchronous state
        // out of the dispatcher so unrelated operations use a bounded stack.
        Box::pin(async move {
            if self.stopped.load(Ordering::SeqCst) {
                return;
            }
            if let Err(error) = self.write_during_shutdown(evt).await {
                self.record_output_write_error(error);
            }
        })
    }

    /// Write one event even after admission has stopped. Only the ordered
    /// unsolicited drain and the final shutdown ACK use this path.
    async fn write_during_shutdown(&self, evt: ProtocolEvent) -> Result<(), String> {
        self.write_unsolicited_event(evt, None).await
    }

    /// Write one ordered unsolicited event while retaining exact BoundSession
    /// progress in its receipt. A fully written but unflushed ordinary frame can
    /// be recovered by shutdown's final flush; an unpublished BoundSession
    /// cannot, because that would expose a frame after its Session transaction
    /// was rolled back.
    async fn write_unsolicited_event(
        &self,
        evt: ProtocolEvent,
        progress: Option<&mut ProtocolWriteProgress>,
    ) -> Result<(), String> {
        let Ok(mut line) = runtime_codec::encode_bytes(&evt) else {
            return Err("failed to encode protocol event during shutdown".to_string());
        };
        line.push(b'\n');
        let mut output = self.output.lock().await;
        self.write_locked_line(&mut output, line, progress).await
    }

    async fn write_locked_history_event(
        &self,
        output: &mut Option<Box<dyn AsyncWrite + Unpin + Send>>,
        evt: ProtocolEvent,
        history_outputs: &HashMap<RuntimeString, RuntimeJson>,
    ) -> Result<(), String> {
        let mut encoded = runtime_codec::encode_value(&evt)
            .map_err(|_| "failed to encode protocol history event".to_string())?;
        // Message's persistence codec deliberately excludes structured output.
        // Inspection replies carry it explicitly, just as ThreadHistory does.
        if let RuntimeJson::Object(fields) = &mut encoded {
            if let Some(RuntimeJson::Array(history)) = fields.get_mut("history") {
                for message in history {
                    if let RuntimeJson::Object(row) = message {
                        let output = row
                            .get("uuid")
                            .and_then(RuntimeJson::as_str)
                            .and_then(|uuid| history_outputs.get(uuid))
                            .cloned();
                        if let Some(output) = output {
                            row.insert("output", output);
                        }
                    }
                }
            }
        }
        let mut line = runtime_codec::encode_bytes(&encoded)
            .map_err(|_| "failed to encode protocol history event".to_string())?;
        line.push(b'\n');
        self.write_locked_line(output, line, None).await
    }

    async fn write_locked_line(
        &self,
        output: &mut Option<Box<dyn AsyncWrite + Unpin + Send>>,
        line: Vec<u8>,
        mut progress: Option<&mut ProtocolWriteProgress>,
    ) -> Result<(), String> {
        let existing_error = self
            .output_write_error
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if let Some(error) = existing_error {
            return Err(format!("protocol output is poisoned: {error}"));
        }
        let out = output
            .as_mut()
            .ok_or_else(|| "protocol output is closed".to_string())?;
        let mut frame = OutputFrameGuard::new(self, line.len());
        while frame.written < line.len() {
            let written = out
                .write(&line[frame.written..])
                .await
                .map_err(|error| format!("failed to write protocol output: {error}"))?;
            if written == 0 {
                return Err("failed to write protocol output: writer returned zero bytes".into());
            }
            frame.advance(written);
            if let Some(progress) = progress.as_deref_mut() {
                progress.wrote_bytes = true;
            }
        }
        out.flush()
            .await
            .map_err(|error| format!("failed to flush protocol output: {error}"))?;
        frame.complete();
        if let Some(progress) = progress {
            progress.flushed = true;
        }
        Ok(())
    }

    async fn flush_output(&self) -> Result<(), String> {
        let mut output = self.output.lock().await;
        let out = output
            .as_mut()
            .ok_or_else(|| "protocol output is closed".to_string())?;
        out.flush()
            .await
            .map_err(|error| format!("failed to flush protocol output: {error}"))
    }
}

fn message_uuid_for_history(message: &Message) -> RuntimeString {
    match message {
        Message::User(row) => row.uuid.clone(),
        Message::Assistant(row) => row.uuid.clone(),
        Message::ToolResult(row) => row.uuid.clone(),
        Message::System(row) => row.uuid.clone(),
        Message::CompactBoundary(row) => row.uuid.clone(),
    }
}

fn bounded_shutdown_error(errors: &[String]) -> Option<RuntimeString> {
    if errors.is_empty() {
        return None;
    }
    const MAX_ERROR_CHARS: usize = 4096;
    let joined = errors.join("; ");
    let bounded = if joined.chars().count() > MAX_ERROR_CHARS {
        joined.chars().take(MAX_ERROR_CHARS).collect::<String>()
    } else {
        joined
    };
    Some(bounded.into())
}

struct DynamicToolAdapter {
    model_name: RuntimeString,
    namespace: Option<RuntimeString>,
    tool_name: RuntimeString,
    description: RuntimeString,
    input_schema: RuntimeJson,
    exposure: ToolExposure,
    read_only: bool,
    inner: Weak<ServerInner>,
}

impl DynamicToolAdapter {
    async fn call_host(
        &self,
        call_id: RuntimeString,
        input: Json,
        ctx: &ToolContext,
    ) -> Result<ToolResult, ToolError> {
        let inner = self
            .inner
            .upgrade()
            .ok_or_else(|| ToolError::new("dynamic tool host is no longer available"))?;
        let session = inner
            .session
            .get()
            .ok_or_else(|| ToolError::new("dynamic tool call has no bound session"))?;
        let arguments = input.clone();
        let turn_id = inner.current_submission_id.lock().unwrap().clone();
        let (tx, rx) = oneshot::channel();
        #[cfg(test)]
        inner.pause_pending_prompt_registration_for_test().await;
        let token = inner
            .register_pending_dynamic_call(call_id.clone(), tx)
            .map_err(ToolError::new)?;
        inner
            .write(ProtocolEvent {
                submission_id: (turn_id.clone()).into(),
                dynamic_tool_call: Some(ProtocolDynamicToolCall {
                    call_id: call_id.clone(),
                    thread_id: session.id.clone(),
                    turn_id: turn_id.into(),
                    namespace: self.namespace.clone(),
                    tool: self.tool_name.clone(),
                    arguments,
                }),
                ..Default::default()
            })
            .await;

        let response = tokio::select! {
            biased;
            _ = ctx.signal.cancelled() => {
                inner.cancel_pending_dynamic_call(&call_id, token);
                return Err(ToolError::new("dynamic tool call was cancelled before receiving a response"));
            }
            response = rx => response
                .map_err(|_| ToolError::new("dynamic tool response channel closed"))?
                .map_err(ToolError::new)?,
        };
        Ok(dynamic_tool_result(response))
    }
}

#[async_trait]
impl Tool for DynamicToolAdapter {
    fn name(&self) -> RuntimeString {
        self.model_name.clone()
    }

    fn description(&self) -> RuntimeString {
        self.description.clone()
    }

    fn input_schema(&self) -> RuntimeJson {
        self.input_schema.clone()
    }

    fn exposure(&self) -> ToolExposure {
        self.exposure
    }

    /// Host tools a scoped friend task may call. Results, questions and
    /// directory requests are bound by the host to the task's own requester or
    /// owner; starting a collaboration with another friend goes through the
    /// task's permission mode like any other tool. None of them widens the
    /// task's resource scope.
    fn task_scope_host_capability(&self) -> bool {
        [
            "list_friends",
            "send_message_to_friend",
            "web_search",
            "reply_to_friend",
            "ask_friend_for_information",
            "request_directory_access",
        ]
        .iter()
        .any(|name| self.model_name == *name)
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        false
    }

    /// Whatever the host declared. A host tool that only observes state must be
    /// able to say so: otherwise every read prompts (default mode falls through
    /// to "otherwise ask") and plan mode denies it outright, which is wrong for a
    /// tool the model uses to answer questions.
    fn is_read_only(&self, _input: &Json) -> bool {
        self.read_only
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.call_host(new_uuid().into(), input, ctx).await
    }

    async fn call_with_id(
        &self,
        call_id: &RuntimeString,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.call_host(call_id.clone(), input, ctx).await
    }
}

fn dynamic_tool_result(response: ProtocolDynamicToolResponse) -> ToolResult {
    let output = runtime_codec::encode_value(&response).unwrap_or(RuntimeJson::Null);
    let mut model_content = Vec::with_capacity(response.content_items.len());
    for item in response.content_items {
        match item {
            ProtocolDynamicToolCallOutputContentItem::InputText { text } => {
                model_content.push(OutputBlock::Text(TextBlock {
                    text,
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                }));
            }
            ProtocolDynamicToolCallOutputContentItem::InputImage { image_url } => {
                let Some((mime_type, base64)) = parse_data_image_url(&image_url) else {
                    return dynamic_tool_projection_error(
                        "dynamic tool images must be base64 data URLs".into(),
                    );
                };
                model_content.push(OutputBlock::Image(ImageBlock {
                    mime_type,
                    base64,
                    detail: None,
                    source_path: None,
                }));
            }
        }
    }
    ToolResult {
        serializable: ToolResultSerializable {
            output,
            model_content,
            is_error: Some(!response.success),
            mcp_meta: None,
            extra_user_blocks: None,
        },
        side_effects: None,
    }
}

fn dynamic_tool_projection_error(error: RuntimeString) -> ToolResult {
    let mut output = RuntimeObject::new();
    output.insert("success", RuntimeJson::Bool(false));
    output.insert("error", RuntimeJson::String(error.clone()));
    ToolResult {
        serializable: ToolResultSerializable {
            output: RuntimeJson::Object(output),
            model_content: vec![OutputBlock::Text(TextBlock {
                text: error,
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_error: Some(true),
            mcp_meta: None,
            extra_user_blocks: None,
        },
        side_effects: None,
    }
}

fn parse_data_image_url(value: &RuntimeString) -> Option<(RuntimeString, RuntimeString)> {
    const DATA_PREFIX: &[u16] = &[0x0064, 0x0061, 0x0074, 0x0061, 0x003a];
    const BASE64_SUFFIX: &[u16] = &[0x003b, 0x0062, 0x0061, 0x0073, 0x0065, 0x0036, 0x0034];
    let units = value.as_utf16();
    if !units.starts_with(DATA_PREFIX) {
        return None;
    }
    let comma = units.iter().position(|unit| *unit == u16::from(b','))?;
    let metadata = &units[DATA_PREFIX.len()..comma];
    let mime_type = metadata.strip_suffix(BASE64_SUFFIX)?;
    let base64 = &units[comma + 1..];
    if !mime_type.starts_with(&[0x0069, 0x006d, 0x0061, 0x0067, 0x0065, 0x002f])
        || base64.is_empty()
    {
        return None;
    }
    Some((
        RuntimeString::from_utf16(mime_type.to_vec()),
        RuntimeString::from_utf16(base64.to_vec()),
    ))
}

/// Routes the engine's approval requests through the protocol's pending-map.
struct ApprovalAdapter {
    inner: Arc<ServerInner>,
}

#[async_trait]
impl ApprovalHandler for ApprovalAdapter {
    async fn approve(&self, req: ApprovalRequest, signal: AbortSignal) -> ApprovalResponse {
        // The engine separately emits `approval.requested` on the stream; here we
        // just park the resolver until an `approve` op lands.
        if signal.is_cancelled() {
            return ApprovalResponse::Deny { message: None };
        }
        let (tx, rx) = oneshot::channel();
        #[cfg(test)]
        self.inner
            .pause_pending_prompt_registration_for_test()
            .await;
        let request_id = req.id.clone();
        let Ok(token) = self.inner.register_pending_approval(request_id.clone(), tx) else {
            return ApprovalResponse::Deny {
                message: Some("protocol server is stopping".into()),
            };
        };
        tokio::select! {
            biased;
            _ = signal.cancelled() => {
                self.inner.cancel_pending_approval(&request_id, token);
                ApprovalResponse::Deny { message: None }
            }
            response = rx => {
                response.unwrap_or(ApprovalResponse::Deny { message: None })
            }
        }
    }
}

/// Surfaces ask_user as an `elicitation` message keyed by a freshly minted id.
struct ElicitAdapter {
    inner: Arc<ServerInner>,
}

#[async_trait]
impl ElicitHandler for ElicitAdapter {
    async fn elicit(&self, req: ElicitRequest, signal: AbortSignal) -> ElicitResponse {
        if signal.is_cancelled() {
            return ElicitResponse::Decline { reason: None };
        }
        let request_id = new_uuid();
        let (tx, mut rx) = oneshot::channel();
        #[cfg(test)]
        self.inner
            .pause_pending_prompt_registration_for_test()
            .await;
        let Ok(token) = self
            .inner
            .register_pending_elicitation(request_id.clone(), tx)
        else {
            return ElicitResponse::Decline {
                reason: Some("protocol server is stopping".into()),
            };
        };
        let submission_id = self.inner.current_submission_id.lock().unwrap().clone();
        let structured = req
            .metadata
            .as_ref()
            .and_then(|m| m.get("kind"))
            .and_then(RuntimeJson::as_str)
            .is_some_and(|s| s == "request_user_input");
        let planning = if structured {
            self.inner
                .ensure_session()
                .await
                .ok()
                .map(|s| s.planning_runtime())
        } else {
            None
        };
        if let Some(runtime) = &planning {
            let mut pending = klaus_agent_core::runtime_values::RuntimeObject::new();
            pending.insert("requestId", RuntimeJson::String(request_id.clone().into()));
            pending.insert("status", RuntimeJson::String("pending".into()));
            for key in ["turnId", "toolCallId"] {
                pending.insert(
                    key,
                    req.metadata
                        .as_ref()
                        .and_then(|m| m.get(key))
                        .cloned()
                        .unwrap_or(RuntimeJson::String(RuntimeString::default())),
                );
            }
            pending.insert(
                "request",
                runtime_codec::encode_value(&crate::types::ElicitRequestWire::from(req.clone()))
                    .expect("structured input request codec"),
            );
            if let Err(error) = runtime.register_question(RuntimeJson::Object(pending)) {
                self.inner.cancel_pending_elicitation(&request_id, token);
                return ElicitResponse::Decline {
                    reason: Some(error),
                };
            }
        }
        let write = self.inner.write(ProtocolEvent {
            submission_id: submission_id.clone().into(),
            elicitation: Some(Elicitation {
                request_id: request_id.clone().into(),
                request: req.into(),
            }),
            ..Default::default()
        });
        tokio::pin!(write);
        tokio::select! {
            biased;
            _ = signal.cancelled() => {
                self.inner.cancel_pending_elicitation(&request_id, token);
                return ElicitResponse::Decline { reason: None };
            }
            response = &mut rx => {
                return response.unwrap_or(ElicitResponse::Decline { reason: None });
            }
            _ = &mut write => {}
        }
        tokio::select! {
            biased;
            _ = signal.cancelled() => {
                self.inner.cancel_pending_elicitation(&request_id, token);
                ElicitResponse::Decline { reason: None }
            }
            response = rx => {
                response.unwrap_or(ElicitResponse::Decline { reason: None })
            }
        }
    }
}

struct McpElicitationAdapter {
    inner: Arc<ServerInner>,
}

#[async_trait]
impl McpElicitationPresenter for McpElicitationAdapter {
    async fn present(&self, event: McpElicitationEvent) {
        let submission_id = self.inner.current_submission_id.lock().unwrap().clone();
        self.inner
            .write(ProtocolEvent {
                submission_id: submission_id.clone().into(),
                mcp_elicitation: Some(event),
                ..Default::default()
            })
            .await;
    }

    async fn cancelled(&self, request_id: RuntimeString) {
        // The card the host presented for this request will never receive a
        // user answer (turn ended or the connection dropped), so settle it as
        // cancelled instead of leaving the host's card clickable forever.
        let submission_id = self.inner.current_submission_id.lock().unwrap().clone();
        self.inner
            .write(ProtocolEvent {
                submission_id: submission_id.into(),
                mcp_elicitation_resolved: Some(McpElicitationResolvedWire {
                    request_id,
                    action: McpElicitationAction::Cancel,
                }),
                ..Default::default()
            })
            .await;
    }
}

/// Surfaces MCP OAuth as a typed request and parks until the Host resolves it.
struct McpAuthorizeAdapter {
    inner: Arc<ServerInner>,
}

#[async_trait]
impl McpAuthorizeHandler for McpAuthorizeAdapter {
    async fn authorize(
        &self,
        request: McpAuthorizeRequest,
        signal: AbortSignal,
    ) -> McpAuthorizeResponse {
        if signal.is_cancelled() {
            return McpAuthorizeResponse::Decline {
                reason: Some("aborted".to_string()),
            };
        }
        let request_id = new_uuid();
        let (tx, rx) = oneshot::channel();
        #[cfg(test)]
        self.inner
            .pause_pending_prompt_registration_for_test()
            .await;
        let Ok(token) = self
            .inner
            .register_pending_mcp_authorization(request_id.clone(), tx)
        else {
            return McpAuthorizeResponse::Decline {
                reason: Some("protocol server is stopping".to_string()),
            };
        };
        let submission_id = self.inner.current_submission_id.lock().unwrap().clone();
        self.inner
            .write(ProtocolEvent {
                submission_id: submission_id.clone().into(),
                mcp_authorization: Some(McpAuthorizationRequestWire {
                    request_id: request_id.clone().into(),
                    server_name: request.server_name.into(),
                    authorization_url: request.authorization_url.into(),
                    redirect_uri: request.redirect_uri.into(),
                }),
                ..Default::default()
            })
            .await;

        tokio::select! {
            biased;
            _ = signal.cancelled() => {
                self.inner
                    .cancel_pending_mcp_authorization(&request_id, token);
                McpAuthorizeResponse::Decline {
                    reason: Some("aborted".to_string()),
                }
            }
            response = rx => response.unwrap_or(McpAuthorizeResponse::Decline {
                reason: Some("authorization host disconnected".to_string()),
            }),
        }
    }
}

/// Adapts a core [`Session`] to the [`RewindSession`] mechanisms the shared
/// [`RewindController`] composes.
struct SessionRewindBinding {
    session: Weak<Session>,
    controller: RewindController,
}

struct SessionRewindAdapter(Arc<Session>);

#[async_trait]
impl RewindSession for SessionRewindAdapter {
    fn history(&self) -> Vec<Message> {
        self.0.history()
    }
    fn resumed_checkpoints(&self) -> Vec<Option<String>> {
        self.0.resumed_checkpoints().to_vec()
    }
    async fn snapshot(&self, label: &str) -> Result<Option<String>, BoxError> {
        Ok(self.0.snapshot(Some(label.to_string())).await)
    }
    async fn record_checkpoint(&self, hash: Option<String>) {
        self.0.record_checkpoint(hash).await;
    }
    async fn rollback(&self, num_turns: f64) -> Result<RollbackResult, BoxError> {
        match self.0.rollback(num_turns as u32).await {
            Ok(o) => Ok(RollbackResult {
                dropped_turns: o.dropped_turns,
                remaining_turns: o.remaining_turns,
                messages: o.messages,
            }),
            Err(e) => Err(e.into()),
        }
    }
    async fn restore_checkpoint(&self, hash: &str) -> Result<(), BoxError> {
        self.0.restore_checkpoint(hash).await.map_err(|e| e.into())
    }
    async fn checkpoint_changed_files(&self, hash: &str) -> Result<Vec<String>, BoxError> {
        self.0
            .checkpoint_changed_files(hash)
            .await
            .map_err(|e| e.into())
    }
}

fn runtime_json_truthy(value: &RuntimeJson) -> bool {
    match value {
        RuntimeJson::Null => false,
        RuntimeJson::Bool(value) => *value,
        RuntimeJson::Number(value) => {
            let value = value.as_f64();
            value != 0.0 && !value.is_nan()
        }
        RuntimeJson::String(value) => !value.is_empty(),
        RuntimeJson::Array(_) | RuntimeJson::Object(_) => true,
    }
}

/// Canonical digest of the model-bearing part of a session init.
///
/// The digest is what makes a publication idempotent: a host that loses the
/// ACK for a change it already published can replay the same `changeId` and get
/// its revision back instead of republishing, while the SAME `changeId` reused
/// with a different configuration is rejected rather than silently applied.
/// Object key order is normalized first, so two equivalent inits digest equal.
fn canonical_inference_key(init: &ProtocolSessionInit) -> Result<String, String> {
    let inference = ProtocolSessionInit {
        config: init.config.clone(),
        provider: init.provider.clone(),
        auth: init.auth.clone(),
        model_source: init.model_source.clone(),
        model_tier_targets: init.model_tier_targets.clone(),
        advisor_target: init.advisor_target.clone(),
        feature_targets: init.feature_targets.clone(),
        pricing: init.pricing.clone(),
        capabilities: init.capabilities.clone(),
        capabilities_by_model: init.capabilities_by_model.clone(),
        ..Default::default()
    };
    let value = runtime_codec::encode_value(&inference)
        .map_err(|_| "cannot fingerprint model configuration".to_string())?;
    let value = scalar_json_value(&value, "model configuration fingerprint")?;
    Ok(canonical_value(value).to_string())
}

fn canonical_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(object) => {
            let sorted: BTreeMap<_, _> = object.into_iter().collect();
            serde_json::Value::Object(
                sorted
                    .into_iter()
                    .map(|(key, value)| (key, canonical_value(value)))
                    .collect(),
            )
        }
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonical_value).collect())
        }
        other => other,
    }
}

fn scalar_json_value(value: &RuntimeJson, name: &str) -> Result<serde_json::Value, String> {
    let scalar = try_to_scalar_json(value).map_err(|error| format!("invalid {name}: {error}"))?;
    let bytes = runtime_codec::encode_bytes(&scalar)
        .map_err(|error| format!("failed to encode {name}: {error}"))?;
    serde_json::from_slice(&bytes).map_err(|error| format!("failed to project {name}: {error}"))
}

#[cfg(test)]
fn runtime_json_from_scalar_value(value: &serde_json::Value) -> Result<RuntimeJson, String> {
    let scalar =
        klaus_agent_core::runtime_values::ScalarJson::validate_static_schema(value.clone())
            .map_err(|error| format!("invalid scalar test fixture: {error}"))?;
    runtime_codec::encode_value(&scalar)
        .map_err(|error| format!("failed to encode scalar test fixture: {error}"))
}

fn inference_config_value(init: &ProtocolSessionInit) -> Result<serde_json::Value, String> {
    let mut config = init
        .config
        .as_ref()
        .map(|config| scalar_json_value(config, "model config"))
        .transpose()?
        .unwrap_or_else(|| serde_json::json!({}));
    if let Some(source) = &init.model_source {
        config
            .as_object_mut()
            .ok_or("model config must be an object")?
            .insert(
                "modelSource".to_string(),
                scalar_json_value(source, "model source")?,
            );
    }
    Ok(config)
}

fn validate_serializable_session_init(init: &ProtocolSessionInit) -> Result<(), String> {
    if let Some(policy) = &init.agent_memory_snapshot_policy {
        policy.validate().map_err(str::to_string)?;
    }
    validate_unique_extension_names("agent", init.agents.iter().map(|agent| &agent.name))?;
    validate_unique_extension_names("skill", init.skills.iter().map(|skill| &skill.name))?;
    validate_unique_extension_names(
        "output style",
        init.output_styles.iter().map(|style| &style.name),
    )?;
    if let Some(selected) = &init.selected_output_style {
        runtime_exact_nonempty(selected, "selected output style")?;
    }
    let mut pricing_models = HashSet::new();
    for entry in init.pricing.iter().flatten() {
        let model = scalar_runtime_string(entry.model.clone(), "pricing model")?;
        exact_nonempty(&model, "pricing model")?;
        let canonical = model.to_ascii_lowercase();
        if !pricing_models.insert(canonical) {
            return Err(format!("duplicate pricing model '{model}'"));
        }
        for (field, rate) in [
            ("inputPerMTok", entry.pricing.input_per_m_tok),
            ("outputPerMTok", entry.pricing.output_per_m_tok),
            ("cacheReadPerMTok", entry.pricing.cache_read_per_m_tok),
            ("cacheWritePerMTok", entry.pricing.cache_write_per_m_tok),
        ] {
            if !rate.is_finite() || rate < 0.0 {
                return Err(format!(
                    "pricing model '{}' has invalid {field}: {rate}",
                    model
                ));
            }
        }
    }
    for hook in &init.command_hooks {
        let command = scalar_runtime_string(hook.command.clone(), "command hook command")?;
        nonempty(&command, "command hook command")?;
        if hook.timeout_ms == Some(0) {
            return Err("command hook timeoutMs must be greater than zero".to_string());
        }
        if let Some(matcher) = &hook.matcher {
            let matcher = scalar_runtime_string(matcher.clone(), "command hook matcher")?;
            exact_nonempty(&matcher, "command hook matcher")?;
        }
    }
    for server in &init.lsp_servers {
        if server.language.is_empty() {
            return Err("LSP language must not be empty".to_string());
        }
        if server.command.is_empty() {
            return Err("LSP command must not be empty".to_string());
        }
        if server.extensions.is_empty() {
            return Err(format!(
                "LSP server {:?} must declare at least one file extension",
                server.language
            ));
        }
        for extension in &server.extensions {
            if extension.is_empty() {
                return Err("LSP file extension must not be empty".to_string());
            }
        }
    }
    if let Some(env) = &init.shell_env {
        for (name, value) in env.entries() {
            let name = scalar_runtime_string(name, "shell environment variable name")?;
            let value = scalar_runtime_string(value.clone(), "shell environment variable value")?;
            if name.is_empty() || name.contains('=') || name.contains('\0') {
                return Err(format!("invalid shell environment variable name: {name:?}"));
            }
            if value.contains('\0') {
                return Err(format!(
                    "shell environment variable {name} contains a NUL byte"
                ));
            }
        }
    }
    Ok(())
}

fn validate_unique_extension_names<'a>(
    kind: &str,
    names: impl IntoIterator<Item = &'a RuntimeString>,
) -> Result<(), String> {
    let mut seen = HashSet::new();
    for name in names {
        runtime_exact_nonempty(name, kind)?;
        if !seen.insert(name.clone()) {
            return Err(format!(
                "duplicate {kind} '{}'",
                runtime_string_error_fragment(name)
            ));
        }
    }
    Ok(())
}

fn runtime_exact_nonempty(value: &RuntimeString, name: &str) -> Result<(), String> {
    let trimmed = runtime_trim(value);
    if trimmed.is_empty() {
        return Err(format!("{name} must not be empty"));
    }
    if trimmed.as_utf16() != value.as_utf16() {
        return Err(format!("{name} must not contain surrounding whitespace"));
    }
    Ok(())
}

fn runtime_string_error_fragment(value: &RuntimeString) -> String {
    match value.try_to_utf8() {
        Ok(value) => value,
        Err(_) => runtime_codec::encode_text(value)
            .ok()
            .and_then(|encoded| encoded.try_to_utf8().ok())
            .unwrap_or_else(|| "<invalid runtime string>".to_string()),
    }
}

fn exact_nonempty<'a>(value: &'a str, name: &str) -> Result<&'a str, String> {
    let trimmed = nonempty(value, name)?;
    if trimmed != value {
        Err(format!("{name} must not contain surrounding whitespace"))
    } else {
        Ok(value)
    }
}

fn command_hooks_from_init(hooks: &[ProtocolCommandHook]) -> Result<Vec<Hook>, String> {
    hooks
        .iter()
        .map(|hook| {
            Ok(Hook {
                event: hook.event,
                matcher: optional_scalar_runtime_string(
                    hook.matcher.clone(),
                    "command hook matcher",
                )?,
                handler: HookHandler::Command {
                    command: scalar_runtime_string(hook.command.clone(), "command hook command")?,
                    timeout_ms: hook.timeout_ms,
                },
                is_async: hook.is_async,
            })
        })
        .collect()
}

fn validate_session_config_update(update: &ProtocolSessionConfigUpdate) -> Result<(), String> {
    if update.ultra_mode.is_none()
        && update.goal.is_none()
        && update.maximum_goal_token_budget.is_none()
        && update.model.is_none()
        && update.permission_mode.is_none()
        && update.model_verbosity.is_none()
        && update.reasoning_summary.is_none()
        && update.web_search_access.is_none()
        && update.approval_review.is_none()
        && update.subagent_model_catalog.is_none()
        && update.subagent_model_options.is_none()
    {
        return Err(
            "update_session_config requires at least one supported runtime setting".to_string(),
        );
    }
    if update
        .maximum_goal_token_budget
        .flatten()
        .is_some_and(|maximum| !(1..=9_007_199_254_740_991).contains(&maximum))
    {
        return Err("maximumGoalTokenBudget must be a positive safe integer or null".into());
    }
    if let Some(model) = &update.model {
        let model = scalar_runtime_string(model.clone(), "session model")?;
        nonempty(&model, "session model")?;
    }
    if let Some(options) = &update.subagent_model_options {
        klaus_agent_core::schemas::config::validate_subagent_model_options(options)?;
    }
    Ok(())
}

fn dynamic_tools_from_init(
    specs: &[ProtocolDynamicToolSpec],
    inner: Weak<ServerInner>,
) -> Result<Vec<Arc<dyn Tool>>, String> {
    validate_dynamic_tools(specs)?;
    let mut tools: Vec<Arc<dyn Tool>> = Vec::new();
    for spec in specs {
        match spec {
            ProtocolDynamicToolSpec::Function(tool) => {
                tools.push(dynamic_tool_adapter(None, None, tool, inner.clone())?);
            }
            ProtocolDynamicToolSpec::Namespace(namespace) => {
                for tool in &namespace.tools {
                    let ProtocolDynamicToolNamespaceTool::Function(tool) = tool;
                    tools.push(dynamic_tool_adapter(
                        Some(&namespace.name),
                        Some(&namespace.description),
                        tool,
                        inner.clone(),
                    )?);
                }
            }
        }
    }
    Ok(tools)
}

fn runtime_trim_code_unit(unit: u16) -> bool {
    matches!(
        unit,
        0x0009..=0x000d
            | 0x0020
            | 0x00a0
            | 0x1680
            | 0x2000..=0x200a
            | 0x2028
            | 0x2029
            | 0x202f
            | 0x205f
            | 0x3000
            | 0xfeff
    )
}

fn runtime_trim(value: &RuntimeString) -> RuntimeString {
    let units = value.as_utf16();
    let start = units
        .iter()
        .position(|unit| !runtime_trim_code_unit(*unit))
        .unwrap_or(units.len());
    let end = units
        .iter()
        .rposition(|unit| !runtime_trim_code_unit(*unit))
        .map_or(start, |index| index + 1);
    RuntimeString::from_utf16(units[start..end].to_vec())
}

fn runtime_trim_is_empty(value: &RuntimeString) -> bool {
    value
        .as_utf16()
        .iter()
        .all(|unit| runtime_trim_code_unit(*unit))
}

fn dynamic_tool_adapter(
    namespace: Option<&RuntimeString>,
    namespace_description: Option<&RuntimeString>,
    tool: &ProtocolDynamicToolFunctionSpec,
    inner: Weak<ServerInner>,
) -> Result<Arc<dyn Tool>, String> {
    let tool_name = tool.name.clone();
    let tool_description = tool.description.clone();
    let namespace = namespace.cloned();
    let namespace_description = namespace_description.cloned();
    let model_name = namespace
        .as_ref()
        .map(|namespace| {
            namespace
                .concat(&RuntimeString::from("__"))
                .concat(&tool_name)
        })
        .unwrap_or_else(|| tool_name.clone());
    let mut description = tool_description.clone();
    if let (Some(namespace), Some(namespace_description)) =
        (namespace.as_ref(), namespace_description.as_ref())
    {
        if !runtime_trim_is_empty(namespace_description) {
            description = RuntimeString::from("Namespace ")
                .concat(namespace)
                .concat(&RuntimeString::from(": "))
                .concat(namespace_description)
                .concat(&RuntimeString::from("\n\n"))
                .concat(&tool_description);
        }
    }
    let input_schema = RuntimeJson::Object(
        klaus_agent_core::tools::tool_schema::to_tool_json_schema(&tool.input_schema),
    );
    Ok(Arc::new(DynamicToolAdapter {
        model_name,
        namespace,
        tool_name,
        description,
        input_schema,
        exposure: if tool.defer_loading {
            ToolExposure::Deferred
        } else {
            ToolExposure::Direct
        },
        read_only: tool.read_only,
        inner,
    }))
}

fn validate_dynamic_tools(specs: &[ProtocolDynamicToolSpec]) -> Result<(), String> {
    const TOOL_NAME_MAX_LEN: usize = 128;
    const NAMESPACE_MAX_LEN: usize = 64;
    const NAMESPACE_DESCRIPTION_MAX_LEN: usize = 1024;
    const RESERVED_NAMESPACES: &[&str] = &[
        "api_tool",
        "browser",
        "computer",
        "container",
        "file_search",
        "functions",
        "image_gen",
        "multi_tool_use",
        "python",
        "python_user_visible",
        "submodel_delegator",
        "terminal",
        "tool_search",
        "web",
    ];

    fn validate_identifier(value: &str, label: &str, max_len: usize) -> Result<(), String> {
        if value.is_empty()
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        {
            return Err(format!("{label} must match ^[a-zA-Z0-9_-]+$: {value:?}"));
        }
        if value.chars().count() > max_len {
            return Err(format!(
                "{label} must be at most {max_len} characters: {value:?}"
            ));
        }
        Ok(())
    }

    fn validate_tool(
        tool: &ProtocolDynamicToolFunctionSpec,
        namespace: Option<&str>,
        seen: &mut HashSet<String>,
    ) -> Result<(), String> {
        let name = scalar_runtime_string(tool.name.clone(), "dynamic tool name")?;
        if name.trim() != name {
            return Err(format!(
                "dynamic tool name has leading/trailing whitespace: {:?}",
                name
            ));
        }
        validate_identifier(&name, "dynamic tool name", TOOL_NAME_MAX_LEN)?;
        if name == "mcp" || name.starts_with("mcp__") {
            return Err(format!("dynamic tool name is reserved: {name}"));
        }
        if !seen.insert(name.clone()) {
            return Err(match namespace {
                Some(namespace) => {
                    format!("duplicate dynamic tool name in namespace {namespace}: {name}")
                }
                None => format!("duplicate dynamic tool name: {name}"),
            });
        }
        if tool.defer_loading && namespace.is_none() {
            return Err(format!(
                "deferred dynamic tool must include a namespace: {name}"
            ));
        }
        if !matches!(tool.input_schema, RuntimeJson::Object(_)) {
            return Err(format!(
                "dynamic tool input schema must be a JSON object: {name}"
            ));
        }
        Ok(())
    }

    let mut seen_plain_tools = HashSet::new();
    let mut seen_namespaces = HashSet::new();
    let mut seen_model_names = HashSet::new();
    for spec in specs {
        match spec {
            ProtocolDynamicToolSpec::Function(tool) => {
                validate_tool(tool, None, &mut seen_plain_tools)?;
                let tool_name = scalar_runtime_string(tool.name.clone(), "dynamic tool name")?;
                if !seen_model_names.insert(tool_name.clone()) {
                    return Err(format!("duplicate dynamic tool model name: {tool_name}"));
                }
            }
            ProtocolDynamicToolSpec::Namespace(namespace) => {
                let namespace_name =
                    scalar_runtime_string(namespace.name.clone(), "dynamic tool namespace")?;
                if namespace_name.trim() != namespace_name {
                    return Err(format!(
                        "dynamic tool namespace has leading/trailing whitespace: {:?}",
                        namespace_name
                    ));
                }
                validate_identifier(&namespace_name, "dynamic tool namespace", NAMESPACE_MAX_LEN)?;
                if namespace_name == "mcp" || namespace_name.starts_with("mcp__") {
                    return Err(format!(
                        "dynamic tool namespace is reserved: {namespace_name}"
                    ));
                }
                if RESERVED_NAMESPACES.contains(&namespace_name.as_str()) {
                    return Err(format!(
                        "dynamic tool namespace collides with a reserved Responses API namespace: {namespace_name}"
                    ));
                }
                if namespace.description.len_utf16() > NAMESPACE_DESCRIPTION_MAX_LEN {
                    return Err(format!(
                        "dynamic tool namespace description must be at most {NAMESPACE_DESCRIPTION_MAX_LEN} characters"
                    ));
                }
                if !seen_namespaces.insert(namespace_name.clone()) {
                    return Err(format!(
                        "duplicate dynamic tool namespace: {namespace_name}"
                    ));
                }
                if namespace.tools.is_empty() {
                    return Err(format!(
                        "dynamic tool namespace must contain at least one tool: {namespace_name}"
                    ));
                }
                let mut seen_namespace_tools = HashSet::new();
                for tool in &namespace.tools {
                    let ProtocolDynamicToolNamespaceTool::Function(tool) = tool;
                    validate_tool(tool, Some(&namespace_name), &mut seen_namespace_tools)?;
                    let tool_name = scalar_runtime_string(tool.name.clone(), "dynamic tool name")?;
                    let model_name = format!("{namespace_name}__{tool_name}");
                    if !seen_model_names.insert(model_name.clone()) {
                        return Err(format!(
                            "dynamic tool names collide after provider encoding: {model_name}"
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

#[derive(Default)]
struct ResolvedHostCapabilities {
    mcp_server_configs: Vec<McpServerConfig>,
    capability_roots: Vec<RuntimeCapabilityRoot>,
    selected_capability_roots: Option<Vec<SelectedRuntimeCapabilityRoot>>,
}

fn host_capabilities_from_init(
    init: Option<&ProtocolHostCapabilitiesInit>,
) -> Result<ResolvedHostCapabilities, String> {
    let Some(init) = init else {
        return Ok(ResolvedHostCapabilities::default());
    };
    let mut seen_mcp_names = HashSet::new();
    let mut mcp_server_configs = Vec::with_capacity(init.mcp_servers.len());
    for server in &init.mcp_servers {
        let name = protocol_mcp_server_name(server)?;
        if !seen_mcp_names.insert(name.clone()) {
            return Err(format!("duplicate hostCapabilities MCP server '{name}'"));
        }
        mcp_server_configs.push(mcp_server_config_from_init(server)?);
    }

    let mut capability_roots = Vec::with_capacity(init.runtime_capability_roots.len());
    for root in &init.runtime_capability_roots {
        capability_roots.push(runtime_capability_root_from_init(root)?);
    }

    let selected_capability_roots = init
        .selected_capability_roots
        .as_ref()
        .map(|items| {
            items
                .iter()
                .map(selected_capability_root_from_init)
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?;

    Ok(ResolvedHostCapabilities {
        mcp_server_configs,
        capability_roots,
        selected_capability_roots,
    })
}

fn protocol_mcp_server_name(server: &ProtocolMcpServerInit) -> Result<String, String> {
    match server {
        ProtocolMcpServerInit::Stdio { name, .. }
        | ProtocolMcpServerInit::Http { name, .. }
        | ProtocolMcpServerInit::Sse { name, .. } => {
            let name = scalar_runtime_string(name.clone(), "MCP server name")?;
            nonempty(&name, "MCP server name")?;
            Ok(name)
        }
    }
}

fn mcp_server_config_from_init(server: &ProtocolMcpServerInit) -> Result<McpServerConfig, String> {
    // The per-tool deny list travels with the server, so a host that turned one
    // tool off gets it dropped after `tools/list` instead of having to choose
    // between the whole server and nothing.
    let deny =
        |disabled_tools: &Option<Vec<RuntimeString>>| -> Result<Option<Vec<String>>, String> {
            disabled_tools
                .as_ref()
                .map(|values| runtime_string_vec(values, "MCP disabled tool"))
                .transpose()
        };
    match server {
        ProtocolMcpServerInit::Stdio {
            name,
            command,
            args,
            env,
            always_load,
            disabled_tools,
        } => Ok(McpServerConfig::Stdio {
            name: required_runtime_string(name, "MCP server name")?,
            command: required_runtime_string(command, "MCP stdio command")?,
            args: args
                .as_ref()
                .map(|values| runtime_string_vec(values, "MCP stdio argument"))
                .transpose()?,
            env: env.clone(),
            options: McpServerOptions {
                always_load: *always_load,
                disabled_tools: deny(disabled_tools)?,
                ..Default::default()
            },
        }),
        ProtocolMcpServerInit::Http {
            name,
            url,
            headers,
            oauth,
            always_load,
            disabled_tools,
        } => Ok(McpServerConfig::Http {
            name: required_runtime_string(name, "MCP server name")?,
            url: required_runtime_string(url, "MCP http url")?,
            headers: headers.as_ref().map(btree_to_hash_map).transpose()?,
            oauth: oauth
                .as_ref()
                .map(mcp_oauth_from_init)
                .transpose()?
                .flatten(),
            options: McpServerOptions {
                always_load: *always_load,
                disabled_tools: deny(disabled_tools)?,
                ..Default::default()
            },
        }),
        ProtocolMcpServerInit::Sse {
            name,
            url,
            headers,
            oauth,
            always_load,
            disabled_tools,
        } => Ok(McpServerConfig::Sse {
            name: required_runtime_string(name, "MCP server name")?,
            url: required_runtime_string(url, "MCP sse url")?,
            headers: headers.as_ref().map(btree_to_hash_map).transpose()?,
            oauth: oauth
                .as_ref()
                .map(mcp_oauth_from_init)
                .transpose()?
                .flatten(),
            options: McpServerOptions {
                always_load: *always_load,
                disabled_tools: deny(disabled_tools)?,
                ..Default::default()
            },
        }),
    }
}

fn mcp_oauth_from_init(init: &ProtocolMcpOAuthOption) -> Result<Option<McpOAuthOption>, String> {
    Ok(match init {
        ProtocolMcpOAuthOption::Advertised(true) => Some(McpOAuthOption::Advertised),
        ProtocolMcpOAuthOption::Advertised(false) => None,
        ProtocolMcpOAuthOption::Object { scopes } => Some(McpOAuthOption::Object {
            scopes: scopes
                .as_ref()
                .map(|values| runtime_string_vec(values, "MCP OAuth scope"))
                .transpose()?,
        }),
    })
}

fn runtime_capability_root_from_init(
    root: &ProtocolRuntimeCapabilityRootInit,
) -> Result<RuntimeCapabilityRoot, String> {
    let id = required_runtime_string(&root.id, "runtime capability root id")?;
    let mut out = RuntimeCapabilityRoot::host(id);
    if let Some(browser) = &root.browser {
        if browser.host_reference.is_some() {
            return Err(
                "cannot transfer live host capability references over stdio; expose the capability through an MCP server and reference it with mcpServer"
                    .to_string(),
            );
        }
        let server = browser
            .mcp_server
            .as_ref()
            .ok_or_else(|| "browser capability requires mcpServer".to_string())
            .and_then(|value| required_runtime_string(value, "browser capability mcpServer"))?;
        out = out.with_browser_tool_mapping(BrowserHostToolMapping::for_mcp_server(server));
    }
    if root.enabled == Some(false) {
        out = out.disabled();
    }
    Ok(out)
}

fn selected_capability_root_from_init(
    selected: &ProtocolSelectedCapabilityRootInit,
) -> Result<SelectedRuntimeCapabilityRoot, String> {
    match selected {
        ProtocolSelectedCapabilityRootInit::Host { id } => Ok(SelectedRuntimeCapabilityRoot::host(
            required_runtime_string(id, "selected host capability root id")?,
        )),
        ProtocolSelectedCapabilityRootInit::Plugin { id } => {
            Ok(SelectedRuntimeCapabilityRoot::plugin(
                required_runtime_string(id, "selected plugin capability root id")?,
            ))
        }
        ProtocolSelectedCapabilityRootInit::Environment {
            environment_id,
            root,
        } => Ok(SelectedRuntimeCapabilityRoot::environment(
            required_runtime_string(environment_id, "selected environment id")?,
            required_runtime_string(root, "selected environment capability root")?,
        )),
    }
}

fn required_runtime_string(value: &RuntimeString, name: &str) -> Result<String, String> {
    let value = scalar_runtime_string(value.clone(), name)?;
    nonempty(&value, name)?;
    Ok(value)
}

fn runtime_string_vec(values: &[RuntimeString], name: &str) -> Result<Vec<String>, String> {
    values
        .iter()
        .cloned()
        .map(|value| scalar_runtime_string(value, name))
        .collect()
}

fn btree_to_hash_map(
    map: &BTreeMap<RuntimeString, RuntimeString>,
) -> Result<HashMap<String, String>, String> {
    map.iter()
        .map(|(key, value)| {
            Ok((
                scalar_runtime_string(key.clone(), "map key")?,
                scalar_runtime_string(value.clone(), "map value")?,
            ))
        })
        .collect()
}

enum ResolvedProviderAuth {
    None,
    ApiKey(String),
    Source(Arc<dyn AuthSource>),
}

/// Convert the host-declared wire capability set into the engine's
/// [`ModelCapabilities`]. An absent set is the minimal capability — the engine
/// never infers capability from the model id.
fn model_capabilities_from_init(init: Option<&ModelCapabilitiesInit>) -> ModelCapabilities {
    let Some(init) = init else {
        return ModelCapabilities::default();
    };
    ModelCapabilities {
        tools: init.tools,
        output_verbosity: init.output_verbosity,
        auto_review_model_override: init.auto_review_model_override.clone(),
        requires_sandboxed_review: init.requires_sandboxed_review,
        node_repl_auto_review_required: init.node_repl_auto_review_required,
        guardian: init.guardian.clone(),
        guardian_v2: init.guardian_v2.clone(),
        model_messages: init.model_messages.clone(),
        comp_hash: init.comp_hash.clone(),
        supports_reasoning_summary_parameter: init.supports_reasoning_summary_parameter,
        default_reasoning_summary: init.default_reasoning_summary,
        supported_reasoning_efforts: init.supported_reasoning_efforts.clone(),
        default_reasoning_effort: init.default_reasoning_effort.clone(),
        multi_agent_reasoning_effort: init.multi_agent_reasoning_effort.clone(),
        effective_context_window_percent: init.effective_context_window_percent,
        auto_compact_token_limit: init.auto_compact_token_limit,
        used_fallback_model_metadata: init.used_fallback_model_metadata,
        model_specialty: init.model_specialty.clone(),
        supports_image_detail_original: init.supports_image_detail_original,
        effort: init.effort.as_ref().map(|effort| EffortCapability {
            levels: effort
                .levels
                .iter()
                .filter_map(|level| level.try_to_utf8().ok())
                .filter_map(|level| EffortLevel::from_str(&level))
                .collect(),
            default: effort
                .default
                .as_ref()
                .and_then(|d| d.try_to_utf8().ok())
                .and_then(|d| EffortLevel::from_str(&d)),
        }),
        thinking: init.thinking.as_ref().map(|thinking| ThinkingCapability {
            adaptive: thinking.adaptive,
            interleaved: thinking.interleaved,
            can_disable_thinking: thinking.can_disable_thinking,
        }),
        structured_output: init.structured_output,
        web_search: init.web_search,
        prompt_cache_control: init.prompt_cache_control,
        native_pdf: init.native_pdf,
        vision: init.vision,
        one_million_context: init.one_million_context,
        task_budgets: init.task_budgets,
        fast_mode: init.fast_mode,
        context_window: init.context_window,
        max_output_tokens: init.max_output_tokens,
        image_limits: init.image_limits,
        max_media_blocks: init.max_media_blocks,
        media_input_mime_types: init.media_input_mime_types.clone(),
    }
}

/// Per-model capability overrides from the host (`capabilitiesByModel`), keyed
/// by exact model id, ready for the provider options.
fn capabilities_by_model_from_init(
    init: &ProtocolSessionInit,
) -> Option<std::collections::HashMap<String, ModelCapabilities>> {
    let entries = init.capabilities_by_model.as_ref()?;
    let map: std::collections::HashMap<String, ModelCapabilities> = entries
        .iter()
        .filter_map(|entry| {
            let id = entry.model.try_to_utf8().ok()?;
            Some((id, model_capabilities_from_init(Some(&entry.capabilities))))
        })
        .collect();
    (!map.is_empty()).then_some(map)
}

#[cfg(test)]
mod output_verbosity_mapping_tests {
    use super::*;
    use klaus_agent_core::types::provider::OutputVerbosity;

    #[test]
    fn session_init_preserves_each_declared_output_detail() {
        let init: ProtocolSessionInit = runtime_codec::decode_text(&RuntimeString::from(
            r#"{"capabilities":{"outputVerbosity":"low"},"capabilitiesByModel":[
                {"model":"summary","capabilities":{"outputVerbosity":"high"}},
                {"model":"plain","capabilities":{}}
            ]}"#,
        ))
        .unwrap();
        let main = model_capabilities_from_init(init.capabilities.as_ref());
        assert_eq!(main.output_verbosity, Some(OutputVerbosity::Low));
        let tiers = capabilities_by_model_from_init(&init).unwrap();
        assert_eq!(
            tiers["summary"].output_verbosity,
            Some(OutputVerbosity::High)
        );
        assert_eq!(tiers["plain"].output_verbosity, None);
        assert_eq!(model_capabilities_from_init(None).output_verbosity, None);
    }

    #[test]
    fn session_init_rejects_an_unknown_output_detail_value() {
        let invalid = RuntimeString::from(r#"{"capabilities":{"outputVerbosity":"verbose"}}"#);
        assert!(runtime_codec::decode_text::<ProtocolSessionInit>(&invalid).is_err());
    }
}

#[cfg(test)]
mod media_capability_mapping_tests {
    use super::*;

    #[test]
    fn session_init_maps_independent_media_declarations_for_main_and_tier_models() {
        let init: ProtocolSessionInit = runtime_codec::decode_text(&RuntimeString::from(
            r#"{"capabilities":{"imageLimits":{"maxWidth":2000,"maxBase64Size":5242880},"maxMediaBlocks":600},"capabilitiesByModel":[
                {"model":"summary","capabilities":{"imageLimits":{"maxHeight":1200,"maxBase64Size":10485760},"maxMediaBlocks":100}},
                {"model":"undeclared","capabilities":{}}
            ]}"#,
        ))
        .unwrap();
        let main = model_capabilities_from_init(init.capabilities.as_ref());
        assert_eq!(main.image_limits.unwrap().max_width.unwrap().get(), 2_000);
        assert_eq!(main.image_limits.unwrap().target_raw_size, None);
        assert_eq!(main.max_media_blocks.unwrap().get(), 600);
        let tiers = capabilities_by_model_from_init(&init).unwrap();
        let summary = &tiers["summary"];
        assert_eq!(
            summary.image_limits.unwrap().max_height.unwrap().get(),
            1_200
        );
        assert_eq!(summary.image_limits.unwrap().max_width, None);
        assert_eq!(
            summary.image_limits.unwrap().max_base64_size.unwrap().get(),
            10_485_760
        );
        assert_eq!(summary.max_media_blocks.unwrap().get(), 100);
        assert_eq!(tiers["undeclared"].image_limits, None);
        assert_eq!(tiers["undeclared"].max_media_blocks, None);
        assert_eq!(model_capabilities_from_init(None).image_limits, None);
    }

    #[tokio::test]
    async fn gemini_initialization_passes_per_model_metadata_into_the_actual_provider() {
        let init: ProtocolSessionInit = runtime_codec::decode_text(&RuntimeString::from(
            r#"{"provider":{"type":"gemini","model":"main"},"auth":{"type":"api_key","key":"fixture-only"},
                "capabilities":{"imageLimits":{"maxWidth":2000},"maxMediaBlocks":600},
                "capabilitiesByModel":[{"model":"summary","capabilities":{
                    "imageLimits":{"maxHeight":1200,"targetRawSize":512000},"maxMediaBlocks":42
                }}]}"#,
        )).unwrap();
        let provider = provider_from_init(&init, None).await.unwrap().unwrap();
        assert_eq!(
            provider
                .capabilities(&"main".into())
                .max_media_blocks
                .unwrap()
                .get(),
            600
        );
        let summary = provider.capabilities(&"summary".into());
        assert_eq!(summary.image_limits.unwrap().max_width, None);
        assert_eq!(
            summary.image_limits.unwrap().max_height.unwrap().get(),
            1_200
        );
        assert_eq!(
            summary.image_limits.unwrap().target_raw_size.unwrap().get(),
            512_000
        );
        assert_eq!(summary.max_media_blocks.unwrap().get(), 42);
        assert_eq!(
            provider.capabilities(&"undeclared".into()).image_limits,
            None
        );
        assert_eq!(
            provider.capabilities(&"undeclared".into()).max_media_blocks,
            None
        );
    }
}

/// The OpenAI-compatible reasoning wire dialect the host declared; defaults to
/// standard OpenAI when unset.
fn openai_reasoning_dialect_from_init(
    init: Option<&ModelCapabilitiesInit>,
) -> OpenAiReasoningDialect {
    match init
        .and_then(|c| c.reasoning_dialect.as_ref())
        .and_then(|d| d.try_to_utf8().ok())
        .as_deref()
    {
        Some("deepseek") => OpenAiReasoningDialect::DeepSeek,
        Some("qwen") => OpenAiReasoningDialect::Qwen,
        Some("managedQwen") => OpenAiReasoningDialect::ManagedQwen,
        Some("qoder") => OpenAiReasoningDialect::Qoder,
        Some("llamaCpp") => OpenAiReasoningDialect::LlamaCpp,
        Some("legacyTokens") => OpenAiReasoningDialect::LegacyTokens,
        _ => OpenAiReasoningDialect::OpenAi,
    }
}

/// The Gemini thinking wire dialect the host declared; defaults to the level
/// form (Gemini 3+) when unset.
fn gemini_thinking_dialect_from_init(
    init: Option<&ModelCapabilitiesInit>,
) -> GeminiThinkingDialect {
    match init.and_then(|c| c.gemini_thinking.as_ref()) {
        Some(g) if g.mode.try_to_utf8().ok().as_deref() == Some("budget") => {
            GeminiThinkingDialect::Budget {
                min: g.min.unwrap_or(0),
                max: g.max.unwrap_or(u64::MAX),
                can_disable: g.can_disable,
            }
        }
        _ => GeminiThinkingDialect::Level,
    }
}

/// Read the session-config retry knobs (`unboundedNetworkRetries`) so provider
/// construction honors the same flag the turn loop reads. Absent config keeps
/// the engine default (on).
fn retry_options_from_init(
    init: &ProtocolSessionInit,
) -> klaus_agent_core::provider::retry::RetryOptions {
    let mut options = klaus_agent_core::provider::retry::RetryOptions::default();
    if let Some(config) = init
        .config
        .as_ref()
        .and_then(|config| scalar_json_value(config, "session config").ok())
    {
        if let Some(flag) = config
            .get("unboundedNetworkRetries")
            .and_then(|value| value.as_bool())
        {
            options.unbounded_network_retries = Some(flag);
        }
    }
    options
}

fn model_source_from_init(init: &ProtocolSessionInit) -> Result<Option<(String, String)>, String> {
    let Some(value) = init.model_source.as_ref() else {
        return Ok(None);
    };
    let value = scalar_json_value(value, "model source")?;
    let source = value
        .get("source")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or("modelSource requires a nonempty source")?;
    let model_id = value
        .get("modelId")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or("modelSource requires a nonempty modelId")?;
    Ok(Some((source.to_string(), model_id.to_string())))
}

fn decorate_model_target(
    target: &mut klaus_agent_core::engine::session::InferenceTargetState,
    decorate: &ProviderDecorator,
) {
    use klaus_agent_core::engine::session::InferenceTargetState;
    if let InferenceTargetState::Ready(target) = target {
        let mut model_source = klaus_agent_core::runtime_values::RuntimeObject::new();
        model_source.insert("source", RuntimeJson::String(target.source.clone().into()));
        model_source.insert(
            "modelId",
            RuntimeJson::String(target.model_id.clone().into()),
        );
        let init = ProtocolSessionInit {
            model_source: Some(RuntimeJson::Object(model_source)),
            ..Default::default()
        };
        target.provider = decorate(target.provider.clone(), &init);
    }
}

fn decorate_model_targets(
    targets: &mut Option<klaus_agent_core::engine::session::InferenceTargets>,
    decorate: &ProviderDecorator,
) {
    if let Some(targets) = targets {
        for target in [&mut targets.high, &mut targets.medium, &mut targets.low]
            .into_iter()
            .flatten()
        {
            decorate_model_target(target, decorate);
        }
    }
}

async fn targets_from_init(
    init: &ProtocolSessionInit,
    store: Option<Arc<dyn CredentialStore>>,
) -> Result<Option<klaus_agent_core::engine::session::InferenceTargets>, String> {
    use klaus_agent_core::engine::session::InferenceTargets;
    let Some(value) = init.model_tier_targets.as_ref() else {
        return Ok(None);
    };
    let value = scalar_json_value(value, "model tier targets")?;
    let values = value
        .as_object()
        .ok_or("modelTierTargets must be an object")?;
    if values
        .keys()
        .any(|key| !["high", "medium", "low"].contains(&key.as_str()))
    {
        return Err("modelTierTargets contains an unknown tier".into());
    }
    let mut targets = InferenceTargets::default();
    for (name, slot) in [
        ("high", &mut targets.high),
        ("medium", &mut targets.medium),
        ("low", &mut targets.low),
    ] {
        let Some(value) = values.get(name).filter(|value| !value.is_null()) else {
            continue;
        };
        *slot = Some(target_from_value(value, store.clone()).await?);
    }
    Ok(Some(targets))
}

async fn advisor_target_from_init(
    init: &ProtocolSessionInit,
    store: Option<Arc<dyn CredentialStore>>,
) -> Result<Option<klaus_agent_core::engine::session::InferenceTargetState>, String> {
    let Some(value) = init.advisor_target.as_ref() else {
        return Ok(None);
    };
    let value = scalar_json_value(value, "advisor target")?;
    if value.is_null() {
        return Ok(None);
    }
    let target = target_from_value(&value, store)
        .await
        .unwrap_or_else(|message| {
            klaus_agent_core::engine::session::InferenceTargetState::Unavailable {
                source: value["source"].as_str().unwrap_or_default().to_string(),
                model_id: value["modelId"].as_str().unwrap_or_default().to_string(),
                code: "MODEL_TARGET_UNAVAILABLE".into(),
                message,
            }
        });
    Ok(Some(target))
}

fn decorate_feature_targets(
    features: &mut klaus_agent_core::engine::session::FeatureTargets,
    decorate: &ProviderDecorator,
) {
    for target in features.slots_mut().into_iter().flatten() {
        decorate_model_target(target, decorate);
    }
}

/// Parse the per-feature auxiliary targets. A missing or null slot stays
/// unset (the feature follows the session's main model); a slot that fails
/// resolution degrades to `Unavailable` instead of failing the session, and
/// consumers fall back to the main model.
async fn feature_targets_from_init(
    init: &ProtocolSessionInit,
    store: Option<Arc<dyn CredentialStore>>,
) -> Result<klaus_agent_core::engine::session::FeatureTargets, String> {
    use klaus_agent_core::engine::session::{FeatureSlot, FeatureTargets, InferenceTargetState};
    let mut features = FeatureTargets::default();
    let Some(value) = init.feature_targets.as_ref() else {
        return Ok(features);
    };
    let value = scalar_json_value(value, "feature targets")?;
    if value.is_null() {
        return Ok(features);
    }
    let values = value
        .as_object()
        .ok_or("featureTargets must be an object")?;
    if values
        .keys()
        .any(|key| !["title", "prediction", "memory"].contains(&key.as_str()))
    {
        return Err("featureTargets contains an unknown feature".into());
    }
    for slot in [
        FeatureSlot::Title,
        FeatureSlot::Prediction,
        FeatureSlot::Memory,
    ] {
        let Some(value) = values.get(slot.name()).filter(|value| !value.is_null()) else {
            continue;
        };
        let state = target_from_value(value, store.clone())
            .await
            .unwrap_or_else(|message| InferenceTargetState::Unavailable {
                source: value["source"].as_str().unwrap_or_default().to_string(),
                model_id: value["modelId"].as_str().unwrap_or_default().to_string(),
                code: "MODEL_TARGET_UNAVAILABLE".into(),
                message,
            });
        match slot {
            FeatureSlot::Title => features.title = Some(state),
            FeatureSlot::Prediction => features.prediction = Some(state),
            FeatureSlot::Memory => features.memory = Some(state),
        }
    }
    Ok(features)
}

/// Build an endpoint-resolved target with the same credentials and decoration as session targets.
pub async fn resolved_model_target(
    value: &serde_json::Value,
    store: Option<Arc<dyn CredentialStore>>,
    decorate: &ProviderDecorator,
) -> Result<klaus_agent_core::engine::session::InferenceTargetState, String> {
    let mut target = target_from_value(value, store).await?;
    decorate_model_target(&mut target, decorate);
    Ok(target)
}

async fn target_from_value(
    value: &serde_json::Value,
    store: Option<Arc<dyn CredentialStore>>,
) -> Result<klaus_agent_core::engine::session::InferenceTargetState, String> {
    use klaus_agent_core::engine::session::{InferenceTarget, InferenceTargetState};
    let source = value
        .get("source")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or("a model target requires a nonempty source")?
        .to_string();
    let model_id = value
        .get("modelId")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or("a model target requires a nonempty modelId")?
        .to_string();
    if value.get("type").and_then(serde_json::Value::as_str) == Some("unavailable") {
        let code = value
            .get("code")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or("an unavailable target requires a code")?
            .to_string();
        let message = value
            .get("message")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or("an unavailable target requires a message")?
            .to_string();
        return Ok(InferenceTargetState::Unavailable {
            source,
            model_id,
            code,
            message,
        });
    }
    if value.get("type").and_then(serde_json::Value::as_str) != Some("resolved") {
        return Err(
            "the named facade must resolve target source references before protocol initialization"
                .into(),
        );
    }
    let prepare = async {
        if value
            .pointer("/provider/model")
            .and_then(serde_json::Value::as_str)
            .is_none_or(|model| model.trim().is_empty())
        {
            return Err("A resolved model target requires an explicit provider model.".to_string());
        }
        let mut target_init = serde_json::Map::new();
        for key in ["provider", "auth", "capabilities", "pricing"] {
            if let Some(value) = value.get(key) {
                target_init.insert(key.into(), value.clone());
            }
        }
        let nested: ProtocolSessionInit = runtime_codec::decode_text(&RuntimeString::from(
            serde_json::Value::Object(target_init).to_string(),
        ))
        .map_err(|error| format!("invalid model target: {error}"))?;
        if matches!(source.as_str(), "subscription" | "codex")
            && matches!(
                &nested.auth,
                Some(ProviderAuthInit::OAuthToken {
                    credential_id: None,
                    ..
                })
            )
        {
            return Err(
                "Subscription model targets require a persistent credentialId.".to_string(),
            );
        }
        validate_serializable_session_init(&nested)?;
        let provider = provider_from_init(&nested, store.clone())
            .await?
            .ok_or("a resolved model target requires a provider")?;
        let model = provider.info().default_model.clone();
        let capabilities = provider.capabilities(&model);
        if capabilities.context_window == 0 || capabilities.max_output_tokens == 0 {
            return Err(
                "a resolved model target must have positive context and output limits".to_string(),
            );
        }
        let pricing = nested
            .pricing
            .as_ref()
            .map(|entries| {
                entries
                    .iter()
                    .map(|entry| {
                        scalar_runtime_string(entry.model.clone(), "target pricing model")
                            .map(|model| (model, entry.pricing))
                    })
                    .collect::<Result<PricingTable, String>>()
            })
            .transpose()?
            .unwrap_or_else(|| {
                if matches!(source.as_str(), "subscription" | "codex") {
                    klaus_agent_core::observability::cost_tracker::DEFAULT_PRICING.clone()
                } else {
                    Vec::new()
                }
            });
        Ok::<_, String>(InferenceTarget {
            source: source.clone(),
            model_id: model_id.clone(),
            model,
            provider,
            pricing,
            review_requirements: None,
        })
    }
    .await;
    Ok(match prepare {
        Ok(target) => InferenceTargetState::Ready(target),
        Err(message) => InferenceTargetState::Unavailable {
            source,
            model_id,
            code: "MODEL_TARGET_UNAVAILABLE".into(),
            message,
        },
    })
}

async fn provider_from_init(
    init: &ProtocolSessionInit,
    credential_store: Option<Arc<dyn CredentialStore>>,
) -> Result<Option<Arc<dyn ModelProvider>>, String> {
    let Some(provider) = &init.provider else {
        if init.auth.is_some() {
            return Err("provider auth init requires a provider selection".to_string());
        }
        return Ok(None);
    };

    match provider {
        ProviderInit::Anthropic {
            model,
            base_url,
            beta_header,
            http_headers,
            env_http_headers,
            api_key_scheme,
            metadata_user_id,
        } => {
            let model = optional_scalar_runtime_string(model.clone(), "anthropic model")?;
            let base_url = optional_scalar_runtime_string(base_url.clone(), "anthropic base URL")?
                .or_else(|| env_nonempty("ANTHROPIC_BASE_URL"));
            let beta_header =
                optional_scalar_runtime_string(beta_header.clone(), "anthropic beta header")?;
            let http_headers = http_headers.as_ref().map(btree_to_hash_map).transpose()?;
            if let Some(headers) = &http_headers {
                validate_provider_extra_headers(headers)?;
            }
            let env_http_headers = env_http_headers
                .as_ref()
                .map(btree_to_hash_map)
                .transpose()?;
            // The stored-key carrier a BYO entry asked for. Absent (or any
            // unknown value) keeps the Anthropic-native `x-api-key` so every
            // existing session is byte-identical to before the knob existed.
            let api_key_scheme =
                optional_scalar_runtime_string(api_key_scheme.clone(), "anthropic api key scheme")?
                    .as_ref()
                    .map(|scheme| {
                        if *scheme == "bearer" {
                            ApiKeyScheme::Bearer
                        } else {
                            ApiKeyScheme::XApiKey
                        }
                    })
                    .unwrap_or_default();
            let auth = resolve_provider_auth(
                "anthropic",
                "ANTHROPIC_API_KEY",
                &init.auth,
                base_url.clone(),
                beta_header.map(|beta| HashMap::from([("anthropic-beta".to_string(), beta)])),
                credential_store.clone(),
            )
            .await?;
            let (api_key, auth_source) = auth_parts(auth);
            let metadata_user_id = optional_scalar_runtime_string(
                metadata_user_id.clone(),
                "anthropic metadata user id",
            )?;
            Ok(Some(Arc::new(AnthropicProvider::new(
                AnthropicProviderOptions {
                    api_key,
                    base_url,
                    default_model: model,
                    auth_source,
                    api_key_scheme,
                    http_headers,
                    env_http_headers,
                    metadata_user_id,
                    fast_mode: model_source_from_init(init)?
                        .is_some_and(|(source, _)| source == "subscription")
                        && inference_config_value(init)?
                            .get("fastMode")
                            .and_then(serde_json::Value::as_bool)
                            .unwrap_or(false),
                    capabilities: Some(model_capabilities_from_init(init.capabilities.as_ref())),
                    capabilities_by_model: capabilities_by_model_from_init(init),
                    retry: Some(retry_options_from_init(init)),
                    ..Default::default()
                },
            ))))
        }
        ProviderInit::Openai {
            model,
            api,
            base_url,
            compatibility,
        } => {
            let model = optional_scalar_runtime_string(model.clone(), "openai model")?;
            let api = optional_scalar_runtime_string(api.clone(), "openai api")?;
            let api = parse_openai_api(api.as_deref())?;
            let base_url = optional_scalar_runtime_string(base_url.clone(), "openai base URL")?
                .or_else(|| env_nonempty("OPENAI_BASE_URL"));
            let compatibility = openai_compatibility_from_init(compatibility.as_ref(), api)?;
            let source = model_source_from_init(init)?;
            let bridge_fast_mode = klaus_agent_core::provider::openai::local_bridge_fast_mode(
                source.as_ref().map(|(source, _)| source.as_str()),
                api,
                openai_reasoning_dialect_from_init(init.capabilities.as_ref()),
                base_url.as_deref(),
                inference_config_value(init)?
                    .get("fastMode")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false),
            );
            let auth = resolve_provider_auth(
                "openai",
                "OPENAI_API_KEY",
                &init.auth,
                base_url.clone(),
                None,
                credential_store.clone(),
            )
            .await?;
            let (api_key, auth_source) = auth_parts(auth);
            Ok(Some(Arc::new(OpenAIProvider::new(OpenAIProviderOptions {
                remote_compaction: if model_source_from_init(init)?
                    .is_some_and(|(source, _)| source == "codex")
                    && api == OpenAiApi::Responses
                {
                    klaus_agent_core::types::provider::RemoteCompactionSupport::ResponsesV2
                } else {
                    klaus_agent_core::types::provider::RemoteCompactionSupport::Unsupported
                },
                api_key,
                base_url,
                default_model: model,
                api: Some(api),
                auth_source,
                bridge_fast_mode,
                compatibility,
                capabilities: Some(model_capabilities_from_init(init.capabilities.as_ref())),
                capabilities_by_model: capabilities_by_model_from_init(init),
                reasoning_dialect: Some(openai_reasoning_dialect_from_init(
                    init.capabilities.as_ref(),
                )),
                retry: Some(retry_options_from_init(init)),
                fast_mode: model_source_from_init(init)?
                    .is_some_and(|(source, _)| source == "codex")
                    && inference_config_value(init)?
                        .get("fastMode")
                        .and_then(serde_json::Value::as_bool)
                        .unwrap_or(false),
                ..Default::default()
            }))))
        }
        ProviderInit::Gemini {
            model,
            base_url,
            code_assist,
        } => {
            let model = optional_scalar_runtime_string(model.clone(), "gemini model")?;
            let base_url = optional_scalar_runtime_string(base_url.clone(), "gemini base URL")?
                .or_else(|| env_nonempty("GEMINI_BASE_URL"));
            let auth = resolve_provider_auth(
                "gemini",
                "GEMINI_API_KEY",
                &init.auth,
                base_url.clone(),
                None,
                credential_store,
            )
            .await?;
            let (api_key, auth_source) = auth_parts(auth);
            Ok(Some(Arc::new(GeminiProvider::new(GeminiProviderOptions {
                api_key,
                base_url,
                default_model: model,
                auth_source,
                code_assist: code_assist
                    .as_ref()
                    .map(|ca| {
                        Ok::<CodeAssistOptions, String>(CodeAssistOptions {
                            project: optional_scalar_runtime_string(
                                ca.project.clone(),
                                "gemini code assist project",
                            )?,
                            endpoint: optional_scalar_runtime_string(
                                ca.endpoint.clone(),
                                "gemini code assist endpoint",
                            )?,
                            session_id: optional_scalar_runtime_string(
                                ca.session_id.clone(),
                                "gemini code assist sessionId",
                            )?,
                        })
                    })
                    .transpose()?,
                capabilities: Some(model_capabilities_from_init(init.capabilities.as_ref())),
                capabilities_by_model: capabilities_by_model_from_init(init),
                thinking_dialect: Some(gemini_thinking_dialect_from_init(
                    init.capabilities.as_ref(),
                )),
                retry: Some(retry_options_from_init(init)),
                ..Default::default()
            }))))
        }
    }
}

async fn resolve_provider_auth(
    provider_id: &str,
    env_key: &str,
    auth: &Option<ProviderAuthInit>,
    provider_base_url: Option<String>,
    extra_headers: Option<HashMap<String, String>>,
    credential_store: Option<Arc<dyn CredentialStore>>,
) -> Result<ResolvedProviderAuth, String> {
    if let Some(headers) = &extra_headers {
        validate_provider_extra_headers(headers)?;
    }
    let Some(auth) = auth else {
        return Ok(ResolvedProviderAuth::None);
    };
    match auth {
        ProviderAuthInit::Env => {
            let key = env_nonempty(env_key)
                .ok_or_else(|| format!("{env_key} is required for provider auth env"))?;
            Ok(ResolvedProviderAuth::ApiKey(key))
        }
        ProviderAuthInit::ApiKey { key } => {
            let key = required_runtime_string(key, "provider api key")?;
            Ok(ResolvedProviderAuth::ApiKey(key))
        }
        ProviderAuthInit::ApiKeyStored { credential_id } => {
            let credential_id =
                required_runtime_string(credential_id, "provider stored api key credential id")?;
            if !is_valid_model_provider_id(&credential_id) {
                return Err(
                    "provider stored api key credential id must contain only ASCII letters, digits, '.', '_', or '-' and be at most 128 bytes"
                        .to_string(),
                );
            }
            let store = credential_store.ok_or_else(|| {
                "provider stored api key requires an endpoint credential store".to_string()
            })?;
            let key = store
                .get(&model_credential_key(&credential_id))
                .await
                .map_err(|err| format!("failed to read provider api key credential: {err}"))?
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    format!("stored provider api key credential not found: {credential_id}")
                })?;
            Ok(ResolvedProviderAuth::ApiKey(key))
        }
        ProviderAuthInit::StaticBearer { token } => {
            let token = required_runtime_string(token, "provider bearer token")?;
            Ok(ResolvedProviderAuth::Source(Arc::new(
                StaticBearerAuth::new(token, provider_base_url, extra_headers.unwrap_or_default()),
            )))
        }
        ProviderAuthInit::OAuthToken {
            access_token,
            refresh_token,
            expires_at,
            id_token,
            oauth,
            base_url,
            extra_headers: token_headers,
            credential_id,
            replace_stored,
            rotation,
        } => {
            if runtime_trim_is_empty(access_token) {
                return Err("provider oauth access token must not be empty".to_string());
            }
            let access_token = access_token.clone();
            let refresh = refresh_token
                .as_ref()
                .map(|value| {
                    let value = runtime_trim(value);
                    if value.is_empty() {
                        Err("provider oauth refresh token must not be empty".to_string())
                    } else {
                        Ok(value)
                    }
                })
                .transpose()?;
            let id_token = id_token.clone();
            let base_url =
                optional_scalar_runtime_string(base_url.clone(), "provider oauth base URL")?;
            let credential_id = optional_scalar_runtime_string(
                credential_id.clone(),
                "provider oauth credential id",
            )?;
            let config = oauth_config(
                provider_id,
                oauth.as_ref(),
                refresh.is_some() || credential_id.is_some(),
            )?;
            let mut headers = extra_headers.unwrap_or_default();
            if let Some(token_headers) = token_headers {
                headers.extend(btree_to_hash_map(token_headers)?);
            }
            validate_provider_extra_headers(&headers)?;
            let root_store = credential_store.clone();
            let store =
                provider_credential_store(provider_id, credential_id.as_deref(), credential_store)?;
            // A queued account set turns this login into a rotating credential
            // source: the head token above seeds the first slot, the queue and
            // its persistence path travel with it.
            if let Some(rotation) = rotation {
                let root_store = root_store.ok_or_else(|| {
                    "credential rotation requires an endpoint credential store".to_string()
                })?;
                let accounts = rotation
                    .accounts
                    .iter()
                    .map(|account| {
                        Ok(klaus_agent_core::provider::rotation::RotationAccountSpec {
                            id: account
                                .id
                                .try_to_utf8()
                                .map_err(|_| "invalid rotation account id".to_string())?,
                            credential_id: account
                                .credential_id
                                .try_to_utf8()
                                .map_err(|_| "invalid rotation credential id".to_string())?,
                            tokens: OAuthTokens {
                                access_token: account.access_token.clone(),
                                refresh_token: account.refresh_token.clone(),
                                expires_at: account.expires_at,
                                id_token: None,
                            },
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?;
                let source = klaus_agent_core::provider::rotation::MultiAccountAuthSource::new(
                    klaus_agent_core::provider::rotation::RotationInit {
                        file_path: rotation
                            .file_path
                            .try_to_utf8()
                            .map_err(|_| "invalid rotation file path".to_string())?,
                        config,
                        base_url: base_url.or(provider_base_url),
                        extra_headers: headers,
                        store: root_store,
                        accounts,
                    },
                )
                .map_err(|error| error.to_string())?;
                return Ok(ResolvedProviderAuth::Source(Arc::new(source)));
            }
            let submitted_tokens = OAuthTokens {
                access_token,
                refresh_token: refresh,
                expires_at: *expires_at,
                id_token,
            };
            let prepared_credential = match store.as_ref() {
                Some(store) if *replace_stored != Some(true) => {
                    OAuthAuth::capture_stored(store, &config)
                        .await
                        .map_err(|_| {
                            "failed to capture stored provider oauth credential".to_string()
                        })?
                }
                _ => None,
            };
            let persist_submitted = store.is_some() && prepared_credential.is_none();
            let initial_tokens = Some(
                prepared_credential
                    .as_ref()
                    .map_or(submitted_tokens, |credential| credential.tokens()),
            );
            let auth = OAuthAuth::new(OAuthAuthOptions {
                config,
                store,
                tokens: initial_tokens,
                prepared_credential,
                base_url: base_url.or(provider_base_url),
                extra_headers: Some(headers),
                refresh_lock_key: credential_id.map(|id| format!("provider-{provider_id}-{id}")),
                ..Default::default()
            });
            if persist_submitted {
                let saved = if *replace_stored == Some(true) {
                    auth.save().await
                } else {
                    auth.save_if_absent().await
                };
                saved
                    .map_err(|err| format!("failed to persist provider oauth credential: {err}"))?;
            }
            Ok(ResolvedProviderAuth::Source(Arc::new(auth)))
        }
        ProviderAuthInit::OAuthStored {
            credential_id,
            oauth,
            base_url,
            extra_headers: token_headers,
        } => {
            let credential_id =
                required_runtime_string(credential_id, "provider oauth credential id")?;
            let base_url =
                optional_scalar_runtime_string(base_url.clone(), "provider oauth base URL")?;
            let config = oauth_config(provider_id, oauth.as_ref(), true)?;
            let store =
                provider_credential_store(provider_id, Some(&credential_id), credential_store)?
                    .expect("credential id requires a store");
            let prepared_credential = OAuthAuth::capture_stored(&store, &config)
                .await
                .map_err(|_| "failed to capture stored provider oauth credential".to_string())?
                .ok_or_else(|| {
                    format!("stored provider oauth credential not found: {credential_id}")
                })?;
            let mut headers = extra_headers.unwrap_or_default();
            if let Some(token_headers) = token_headers {
                headers.extend(btree_to_hash_map(token_headers)?);
            }
            validate_provider_extra_headers(&headers)?;
            Ok(ResolvedProviderAuth::Source(Arc::new(OAuthAuth::new(
                OAuthAuthOptions {
                    config,
                    store: Some(store),
                    tokens: Some(prepared_credential.tokens()),
                    prepared_credential: Some(prepared_credential),
                    base_url: base_url.or(provider_base_url),
                    extra_headers: Some(headers),
                    refresh_lock_key: Some(format!("provider-{provider_id}-{credential_id}")),
                    ..Default::default()
                },
            ))))
        }
    }
}

fn provider_credential_store(
    provider_id: &str,
    credential_id: Option<&str>,
    store: Option<Arc<dyn CredentialStore>>,
) -> Result<Option<Arc<dyn CredentialStore>>, String> {
    let Some(credential_id) = credential_id else {
        return Ok(None);
    };
    let credential_id = nonempty(credential_id, "provider oauth credential id")?;
    if credential_id.len() > 128
        || !credential_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    {
        return Err(
            "provider oauth credential id must contain only ASCII letters, digits, '.', '_', or '-' and be at most 128 bytes"
                .to_string(),
        );
    }
    let store = store.ok_or_else(|| {
        "provider oauth credential persistence requires an endpoint credential store".to_string()
    })?;
    Ok(Some(Arc::new(NamespacedCredentialStore::new(
        store,
        format!("provider/{provider_id}/{credential_id}"),
    ))))
}

fn auth_parts(auth: ResolvedProviderAuth) -> (Option<String>, Option<Arc<dyn AuthSource>>) {
    match auth {
        ResolvedProviderAuth::None => (None, None),
        ResolvedProviderAuth::ApiKey(key) => (Some(key), None),
        ResolvedProviderAuth::Source(source) => (None, Some(source)),
    }
}

fn oauth_config(
    provider_id: &str,
    init: Option<&ProviderOAuthInit>,
    refreshable: bool,
) -> Result<OAuthConfig, String> {
    let Some(init) = init else {
        if refreshable {
            return Err(
                "provider oauth refresh token requires oauth client configuration".to_string(),
            );
        }
        return Ok(OAuthConfig::default());
    };
    let client_id = optional_scalar_runtime_string(init.client_id.clone(), "oauth clientId")?
        .unwrap_or_default();
    let authorize_url =
        optional_scalar_runtime_string(init.authorize_url.clone(), "oauth authorizeUrl")?
            .unwrap_or_default();
    let token_url = optional_scalar_runtime_string(init.token_url.clone(), "oauth tokenUrl")?
        .unwrap_or_default();
    let redirect_uri =
        optional_scalar_runtime_string(init.redirect_uri.clone(), "oauth redirectUri")?
            .unwrap_or_default();
    let client_id = client_id.trim();
    let authorize_url = authorize_url.trim();
    let token_url = token_url.trim();
    let redirect_uri = redirect_uri.trim();
    if !client_id.is_empty()
        && authorize_url.is_empty()
        && token_url.is_empty()
        && redirect_uri.is_empty()
    {
        return match provider_id {
            "openai" => Ok(codex_oauth_defaults(client_id, None)),
            "gemini" => Ok(gemini_oauth_defaults(client_id, None)),
            _ => Err(
                "anthropic oauth init requires authorizeUrl, tokenUrl, and redirectUri".to_string(),
            ),
        };
    }
    if client_id.is_empty()
        || authorize_url.is_empty()
        || token_url.is_empty()
        || redirect_uri.is_empty()
    {
        return Err(
            "provider oauth init requires clientId, authorizeUrl, tokenUrl, and redirectUri"
                .to_string(),
        );
    }
    Ok(OAuthConfig {
        client_id: client_id.to_string(),
        authorize_url: authorize_url.to_string(),
        token_url: token_url.to_string(),
        redirect_uri: redirect_uri.to_string(),
        scopes: init
            .scopes
            .as_ref()
            .map(|values| runtime_string_vec(values, "oauth scope"))
            .transpose()?,
        extra_authorize_params: init
            .extra_authorize_params
            .as_ref()
            .map(|values| {
                values
                    .iter()
                    .map(|(key, value)| {
                        Ok((
                            scalar_runtime_string(key.clone(), "oauth parameter name")?,
                            scalar_runtime_string(value.clone(), "oauth parameter value")?,
                        ))
                    })
                    .collect::<Result<Vec<_>, String>>()
            })
            .transpose()?,
    })
}

fn parse_openai_api(api: Option<&str>) -> Result<OpenAiApi, String> {
    match api.unwrap_or("responses").to_ascii_lowercase().as_str() {
        "chat" | "chat_completions" | "chat-completions" => Ok(OpenAiApi::Chat),
        "responses" | "response" => Ok(OpenAiApi::Responses),
        other => Err(format!("unsupported openai api '{other}'")),
    }
}

fn openai_compatibility_from_init(
    init: Option<&OpenAiCompatibilityInit>,
    api: OpenAiApi,
) -> Result<Option<OpenAICompatibilityOptions>, String> {
    let Some(init) = init else {
        return Ok(None);
    };
    let extra_headers = init
        .extra_headers
        .as_ref()
        .map(btree_to_hash_map)
        .transpose()?
        .unwrap_or_default();
    let request_identity = init
        .request_identity
        .as_ref()
        .map(|identity| {
            OpenAIRequestIdentity::try_new(required_runtime_string(
                &identity.session_id,
                "openai request identity sessionId",
            )?)
        })
        .transpose()?;
    if api == OpenAiApi::Responses && request_identity.is_some() {
        return Err("openai compatibility requestIdentity requires api 'chat'".to_string());
    }
    if api != OpenAiApi::Responses && init.responses_function_tools.is_some() {
        return Err(
            "openai compatibility responsesFunctionTools requires api 'responses'".to_string(),
        );
    }
    let instruction_role = match optional_scalar_runtime_string(
        init.instruction_role.clone(),
        "openai compatibility instructionRole",
    )?
    .as_deref()
    {
        None => None,
        Some("system") => Some(OpenAIInstructionRole::System),
        Some("developer") => Some(OpenAIInstructionRole::Developer),
        Some(_) => {
            return Err("openai compatibility instructionRole must be system or developer".into());
        }
    };
    OpenAICompatibilityOptions::try_new(extra_headers, request_identity)
        .map(|options| {
            options
                .with_responses_function_tools(init.responses_function_tools.unwrap_or(false))
                .with_instruction_role(instruction_role)
                .with_model_default_effort(init.use_model_default_effort.unwrap_or(false))
        })
        .map(Some)
}

fn nonempty<'a>(value: &'a str, name: &str) -> Result<&'a str, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        Err(format!("{name} must not be empty"))
    } else {
        Ok(trimmed)
    }
}

/// Project only the explicitly supplied config base; never read process cwd.
fn session_config_base_dir(cwd: &RuntimeString) -> std::path::PathBuf {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt;
        std::path::PathBuf::from(std::ffi::OsString::from_vec(cwd.to_node_utf8()))
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStringExt;
        std::path::PathBuf::from(std::ffi::OsString::from_wide(cwd.as_utf16()))
    }
}

fn scalar_runtime_string(value: RuntimeString, name: &str) -> Result<String, String> {
    value
        .try_to_utf8()
        .map_err(|error| format!("{name} is not scalar UTF-8: {error}"))
}

fn optional_scalar_runtime_string(
    value: Option<RuntimeString>,
    name: &str,
) -> Result<Option<String>, String> {
    value
        .map(|value| scalar_runtime_string(value, name))
        .transpose()
}

fn env_nonempty(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn err_message(err: &BoxError) -> String {
    err.to_string()
}

// The end-to-end op/event regression suite lives in a sibling file; every
// test keeps its `server::tests::` path through this exact module mount.
#[cfg(test)]
#[path = "server_tests.rs"]
mod tests;

#[cfg(test)]
mod review_catalog_decorator_tests {
    use super::*;
    use klaus_agent_core::engine::session::{
        InferenceTarget, InferenceTargetState, InferenceTargets,
    };
    use klaus_agent_core::provider::openai::{OpenAIProvider, OpenAIProviderOptions};
    use klaus_agent_core::types::provider::ModelCapabilities;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn auxiliary_target_decoration_keeps_source_identity_and_attaches_full_catalog() {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let decorate: ProviderDecorator = Arc::new(move |provider, init| {
            assert_eq!(
                init.model_source
                    .as_ref()
                    .unwrap()
                    .get("source")
                    .unwrap()
                    .as_string()
                    .unwrap(),
                "codex"
            );
            observed.fetch_add(1, Ordering::SeqCst);
            klaus_agent_core::provider::review_target::with_review_model_catalog(
                provider,
                Arc::new(|| vec![("review-private".into(), ModelCapabilities::default())]),
            )
        });
        let provider: Arc<dyn ModelProvider> =
            Arc::new(OpenAIProvider::new(OpenAIProviderOptions::default()));
        let target = || {
            InferenceTargetState::Ready(InferenceTarget {
                source: "codex".into(),
                model_id: "auxiliary".into(),
                model: "auxiliary".into(),
                provider: provider.clone(),
                pricing: Vec::new(),
                review_requirements: None,
            })
        };
        let mut targets = Some(InferenceTargets {
            high: Some(target()),
            medium: None,
            low: Some(target()),
        });
        decorate_model_targets(&mut targets, &decorate);
        let mut advisor = target();
        decorate_model_target(&mut advisor, &decorate);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        if let InferenceTargetState::Ready(target) = advisor {
            assert_eq!(
                target.provider.offline_model_catalog()[0].0,
                "review-private"
            );
            assert_eq!(target.model, "auxiliary");
        } else {
            panic!("the ready target must remain ready");
        }
    }
}

#[cfg(test)]
mod approval_review_metadata_wire_tests {
    use super::*;

    #[tokio::test]
    async fn native_media_mime_metadata_survives_session_init_and_provider_construction() {
        let init: ProtocolSessionInit = runtime_codec::decode_text(&RuntimeString::from(r#"{
            "provider":{"type":"gemini","model":"native-media-primary","baseUrl":"http://127.0.0.1:1"},
            "auth":{"type":"static_bearer","token":"fixture-token"},
            "capabilities":{"mediaInputMimeTypes":["audio/wav","video/mp4"],"nativePdf":true,"vision":true},
            "capabilitiesByModel":[
                {"model":"native-media-audio","capabilities":{"mediaInputMimeTypes":["audio/webm;codecs=opus"]}},
                {"model":"native-media-text","capabilities":{"tools":false}}
            ]
        }"#)).unwrap();
        let roundtrip: ProtocolSessionInit =
            runtime_codec::decode_value(&runtime_codec::encode_value(&init).unwrap()).unwrap();
        assert_eq!(roundtrip.capabilities, init.capabilities);
        let provider = provider_from_init(&roundtrip, None).await.unwrap().unwrap();
        let primary = provider.model_metadata(&RuntimeString::from("native-media-primary"));
        assert_eq!(
            primary.media_input_mime_types,
            ["audio/wav", "video/mp4"].map(RuntimeString::from)
        );
        assert!(primary.native_pdf);
        assert!(primary.vision);
        let audio = provider.model_metadata(&RuntimeString::from("native-media-audio"));
        assert_eq!(
            audio.media_input_mime_types,
            vec![RuntimeString::from("audio/webm;codecs=opus")]
        );
        assert!(!audio.native_pdf);
        let text = provider.model_metadata(&RuntimeString::from("native-media-text"));
        assert!(text.media_input_mime_types.is_empty());
        assert_eq!(text.tools, Some(false));
    }

    fn model_init() -> ProtocolSessionInit {
        runtime_codec::decode_text(&RuntimeString::from(r#"{
            "provider":{"type":"openai","model":"primary-fixture","api":"responses","baseUrl":"http://127.0.0.1:1"},
            "auth":{"type":"static_bearer","token":"fixture-token"},
            "capabilities":{"autoReviewModelOverride":"review-private","requiresSandboxedReview":true,"nodeReplAutoReviewRequired":true,"guardian":{"shell":"adaptive"},"guardianV2":{"review_threshold_basis_points":5000},"modelMessages":{"guardian_v2":{"risk_score_template":"\ud800"}},"compHash":"revision-3","supportsReasoningSummaryParameter":false,"defaultReasoningSummary":"none","supportedReasoningEfforts":["none","minimal","persistent","custom-effort"],"defaultReasoningEffort":"persistent","effectiveContextWindowPercent":75,"autoCompactTokenLimit":96000,"usedFallbackModelMetadata":true,"modelSpecialty":"cyber","supportsImageDetailOriginal":true},
            "capabilitiesByModel":[{"model":"review-private","capabilities":{"effort":{"levels":["low","high"],"default":"high"},"supportsReasoningSummaryParameter":true,"defaultReasoningSummary":"concise"}}]
        }"#)).unwrap()
    }

    #[test]
    fn strict_model_metadata_init_preserves_review_fields_and_exact_nested_content() {
        let mut init = model_init();
        init.capabilities
            .as_mut()
            .unwrap()
            .multi_agent_reasoning_effort = Some("xhigh".into());
        let metadata = model_capabilities_from_init(init.capabilities.as_ref());
        assert_eq!(
            metadata.multi_agent_reasoning_effort.as_ref().unwrap(),
            "xhigh"
        );
        assert_eq!(
            metadata.auto_review_model_override.as_ref().unwrap(),
            "review-private"
        );
        assert!(metadata.requires_sandboxed_review);
        assert!(metadata.node_repl_auto_review_required);
        assert_eq!(
            metadata.guardian,
            init.capabilities.as_ref().unwrap().guardian
        );
        assert_eq!(
            metadata.guardian_v2,
            init.capabilities.as_ref().unwrap().guardian_v2
        );
        assert_eq!(
            metadata.model_messages,
            init.capabilities.as_ref().unwrap().model_messages
        );
        assert_eq!(metadata.comp_hash.as_ref().unwrap(), "revision-3");
        assert_eq!(metadata.auto_compact_token_limit, Some(96_000));
        assert_eq!(metadata.supports_reasoning_summary_parameter, Some(false));
        assert_eq!(
            metadata.default_reasoning_summary,
            Some(klaus_agent_core::types::provider::ReasoningSummary::None)
        );
        let roundtrip: ProtocolSessionInit =
            runtime_codec::decode_value(&runtime_codec::encode_value(&init).unwrap()).unwrap();
        assert_eq!(
            model_capabilities_from_init(roundtrip.capabilities.as_ref()),
            metadata
        );
        let bad = RuntimeString::from(r#"{"capabilities":{"unexpectedMetadata":true}}"#);
        assert!(runtime_codec::decode_text::<ProtocolSessionInit>(&bad).is_err());
    }

    #[tokio::test]
    async fn actual_provider_construction_receives_the_override_and_review_target_metadata() {
        let init = model_init();
        let provider = provider_from_init(&init, None).await.unwrap().unwrap();
        let primary = provider.model_metadata(&RuntimeString::from("primary-fixture"));
        assert_eq!(
            primary.auto_review_model_override.as_ref().unwrap(),
            "review-private"
        );
        assert!(primary.requires_sandboxed_review);
        assert_eq!(
            primary.supported_reasoning_efforts,
            ["none", "minimal", "persistent", "custom-effort"]
                .map(RuntimeString::from)
                .to_vec()
        );
        assert_eq!(
            primary.default_reasoning_effort.as_ref().unwrap(),
            "persistent"
        );
        assert_eq!(primary.effective_context_window_percent, Some(75));
        assert!(primary.used_fallback_model_metadata);
        assert_eq!(primary.model_specialty.as_ref().unwrap(), "cyber");
        assert!(primary.supports_image_detail_original);
        let review = provider.model_metadata(&RuntimeString::from("review-private"));
        assert_eq!(review.supports_reasoning_summary_parameter, Some(true));
        assert_eq!(
            review.default_reasoning_summary,
            Some(klaus_agent_core::types::provider::ReasoningSummary::Concise)
        );
        let main_model = RuntimeString::from("primary-fixture");
        let review_model = RuntimeString::from("review-private");
        let captured = provider.capture_capabilities(&[main_model.clone(), review_model.clone()]);
        assert_eq!(
            captured.get(&main_model),
            Some(&provider.capabilities(&main_model))
        );
        assert_eq!(
            captured.get(&review_model),
            Some(&provider.capabilities(&review_model))
        );
        assert!(captured[&main_model].supports_image_detail_original);
        assert!(!captured[&review_model].supports_image_detail_original);
    }
}
