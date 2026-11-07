//! Wire protocol message types.
//!
//! A host drives a [`crate::server::ProtocolServer`] by sending newline-delimited
//! JSON [`Op`]s and reading newline-delimited JSON [`ProtocolEvent`]s back.
//! Everything here is plain-JSON serializable (serde, camelCase).

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use klaus_agent_core::runtime_process::RuntimeEnv;
use klaus_agent_core::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use klaus_agent_core::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeMap, RuntimeObject, RuntimeObjectEncoder, RuntimeString,
};
use klaus_agent_core::schemas::content::ContentBlock;

pub use klaus_agent_core::capabilities::{
    ResolvedRuntimeCapabilityRoot, RuntimeCapabilityDescriptor, RuntimeCapabilityName,
    RuntimeCapabilityResolution, RuntimeCapabilitySource, RuntimeCapabilitySourceKind,
    SelectedRuntimeCapabilityRoot,
};
pub use klaus_agent_core::context::compact::{
    CompactionDiagnostic, CompactionDiagnosticKind, CompactionDiagnosticStage, CompactionNoopReason,
};
pub use klaus_agent_core::engine::agent_definitions::AgentDefinition;
pub use klaus_agent_core::engine::message_queue::{
    QueueEdit, QueueItemState, QueueMutationResult, QueueMutationStatus, QueuePriority,
    QueueSnapshot, QueueSnapshotItem, QueueSnapshotStatus, QueueTerminalState, QueuedMessage,
    QueuedValue,
};
use klaus_agent_core::engine::session::{
    BackgroundAgentInfo, CompactOutcome, GenerateTitleResult, SessionContextSnapshot,
    SideQuestionResult,
};
pub use klaus_agent_core::engine::session::{DreamNowCompletion, DreamNowOutcome, DreamNowReason};
pub use klaus_agent_core::engine::session::{Input, UserContentPart};
pub use klaus_agent_core::engine::session::{
    ReviewCodeLocation, ReviewFinding, ReviewLineRange, ReviewOutput, ReviewRequest,
    ReviewSessionResult, ReviewTarget,
};
pub use klaus_agent_core::engine::subagent_lifecycle::{
    SubagentContext, SubagentKind, SubagentLifecycleRecord, SubagentSnapshot, SubagentStatus,
};
pub use klaus_agent_core::extensions::failure::{
    ExtensionFailure, ExtensionFailureKind, ExtensionFailureSink,
};
pub use klaus_agent_core::extensions::output_styles::OutputStyle;
pub use klaus_agent_core::extensions::plugins::installed::{
    InstalledPluginRecord, InstalledPlugins,
};
pub use klaus_agent_core::extensions::plugins::runtime::PluginRuntimeSnapshot;
pub use klaus_agent_core::extensions::runtime_snapshot::{
    EffectiveExtension, EffectiveHook, ExtensionKind, ExtensionRuntimeSnapshot, ExtensionSource,
    ExtensionSourceKind, HookHandlerKind, PricingRuntimeSnapshot, SuppressedExtension,
};
pub use klaus_agent_core::extensions::skills::loader::Skill;
pub use klaus_agent_core::hooks::setup::{
    SetupLifecycleEvent, SetupLifecycleSnapshot, SetupRunRecord, SetupRunRequest, SetupRunResult,
    SetupRunStatus, SetupTrigger,
};
pub use klaus_agent_core::hooks::types::HookEvent;
pub use klaus_agent_core::lsp::types::LspServerConfig;
pub use klaus_agent_core::mcp::elicitation::*;
pub use klaus_agent_core::mcp::oauth_provider::McpAuthStatus;
pub use klaus_agent_core::mcp::runtime_snapshot::{
    McpPromptRuntimeSnapshot, McpResourceRuntimeSnapshot, McpRuntimeSnapshot,
    McpServerRuntimeSnapshot, McpToolRuntimeSnapshot,
};
pub use klaus_agent_core::mcp::types::{
    McpResourceTemplateDescriptor, McpServerInfo, McpServerStatusUpdate, McpStartupFailureReason,
};
pub use klaus_agent_core::memory::agent_memory::{AgentMemoryMergeChoice, AgentMemoryScope};
pub use klaus_agent_core::memory::maintenance::{
    AutoDreamRun, AutoDreamRunReason, AutoMemoryRun, AutoMemoryRunReason, MemoryMaintenanceEvent,
    MemoryMaintenanceQueryError, MemoryMaintenanceQueryErrorCode,
    MemoryMaintenanceQueryErrorDetail, MemoryMaintenanceRunKind, MemoryMaintenanceSnapshot,
    MemoryMaintenanceStatus, MemoryMaintenanceTrigger, MemoryMaintenanceTurn,
};
pub use klaus_agent_core::memory::snapshot_decision::{
    AgentMemorySnapshotDecisionAudit, AgentMemorySnapshotDecisionPolicy,
    AgentMemorySnapshotDecisionRequest, AgentMemorySnapshotDecisionSource,
    AgentMemorySnapshotDecisionStatus, AgentMemorySnapshotDiff, AgentMemorySnapshotDiffEntry,
    AgentMemorySnapshotDiffKind, AgentMemorySnapshotPolicyAction,
};
use klaus_agent_core::observability::cost_tracker::{CostSnapshot, ModelPricing};
pub use klaus_agent_core::schemas::background_process::{
    BackgroundProcessEvent, BackgroundProcessOutput, BackgroundProcessSnapshot,
    BackgroundProcessStopResult,
};
pub use klaus_agent_core::schemas::config::SandboxWorkspaceWrite;
use klaus_agent_core::schemas::effort::EffortValue;
pub use klaus_agent_core::schemas::message::CompactBoundaryMessage;
use klaus_agent_core::schemas::message::{Message, Usage};
pub use klaus_agent_core::store::session_store::{
    ListOptions, SubAgentTranscript, ThreadPage, ThreadSummary, UnarchiveSessionResult,
    UnarchiveSessionStatus,
};
pub use klaus_agent_core::tasks::task_store::{
    Task, TaskListChange, TaskListCursorStatus, TaskListQueryOptions, TaskListSnapshot, TaskStatus,
};
use klaus_agent_core::types::errors::TerminalReason;
pub use klaus_agent_core::types::errors::{InputValidationErrorCode, InputValidationErrorDetail};
pub use klaus_agent_core::types::events::CompactionMode;
use klaus_agent_core::types::events::EngineEvent;
pub use klaus_agent_core::types::events::FileChangeKind;
use klaus_agent_core::types::permission::{ApprovalResponse, ExternalPermissionMode};
use klaus_agent_core::types::tool::{
    BackgroundTaskStatus, ElicitRequest, ElicitResponse, McpResourceContent,
};

/// The rewind types are owned by core (the rewind controller defines them) and
/// shared by the protocol server + clients; re-export so consumers still see them
/// here.
pub use klaus_agent_core::engine::rewind::{RewindOutcome, RewindPreview, RewindScope};

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

/// Priority for a steered message (mirrors core's queue priority).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SteerPriority {
    Now,
    Next,
    Later,
}

impl RuntimeEncode for SteerPriority {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Now => "now",
            Self::Next => "next",
            Self::Later => "later",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for SteerPriority {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("now", &[]),
                RuntimeEnumVariant::new("next", &[]),
                RuntimeEnumVariant::new("later", &[]),
            ],
            &[Self::Now, Self::Next, Self::Later],
        )
    }
}

impl From<SteerPriority> for QueuePriority {
    fn from(p: SteerPriority) -> Self {
        match p {
            SteerPriority::Now => QueuePriority::Now,
            SteerPriority::Next => QueuePriority::Next,
            SteerPriority::Later => QueuePriority::Later,
        }
    }
}

/// Options carried on a `run` op (a JSON-safe subset of core's turn options).
#[derive(Debug, Clone, Default)]
pub struct RunOpOptions {
    pub model: Option<RuntimeString>,
    /// Per-turn reasoning effort override; falls back to the session's own
    /// effort when absent. Mirrors core's `TurnOptions::effort`.
    pub effort: Option<EffortValue>,
    pub ultra_mode: Option<bool>,
    pub permission_mode: Option<ExternalPermissionMode>,
    /// Constrain the turn's output to this JSON Schema; the parsed value comes
    /// back on the `result` event.
    pub output_schema: Option<RuntimeJson>,
    /// Extra text appended to the system prompt for this turn only.
    pub system_prompt_append: Option<RuntimeString>,
    /// Host-owned rendering metadata persisted beside the user turn. The engine
    /// never sends it to a model or interprets its shape.
    pub display_metadata: Option<RuntimeJson>,
    /// Engine-authored input must remain model-visible without becoming a user turn.
    pub meta_input: Option<bool>,
}

/// A submission: a host-generated `id` plus a discriminated `op` payload. The
/// `id` is echoed back on every resulting event as [`ProtocolEvent::submission_id`].
#[derive(Debug)]
pub struct Op {
    pub id: RuntimeString,
    pub op: OpKind,
}

/// Why the facade is stopping one foreground reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProtocolInterruptOrigin {
    #[default]
    UserStop,
    System,
    TargetedTurn,
}

/// Settled interruption outcome; transport acknowledgement is separate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolTurnInterruptResult {
    pub accepted: bool,
    pub interrupted_turn_id: Option<RuntimeString>,
    pub goal_pause_error: Option<RuntimeString>,
}

/// An additional terminal presentation gate; inheritance never forces delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolTerminalNotificationDisposition {
    Inherit,
    Suppress,
}

/// A reserved successor's authoritative admission disposition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolGoalContinuation {
    pub decision_id: RuntimeString,
    pub prior_submission_id: RuntimeString,
    pub goal_id: Option<RuntimeString>,
    pub successor_submission_id: Option<RuntimeString>,
    pub pending: bool,
    pub continued: Option<bool>,
    pub prior_completion_notification_disposition: Option<ProtocolTerminalNotificationDisposition>,
}

/// The discriminated op payload.
#[derive(Debug)]
// Keep the public `InitSession { init: ProtocolSessionInit }` constructor and
// pattern source-compatible while preserving its established serde wire shape.
#[allow(clippy::large_enum_variant)]
pub enum OpKind {
    InitSession {
        init: ProtocolSessionInit,
    },
    Run {
        input: Input,
        options: Option<RunOpOptions>,
    },
    Interrupt {
        origin: Option<ProtocolInterruptOrigin>,
        expected_turn_id: Option<RuntimeString>,
    },
    /// Cancel one in-flight read-only helper by the target op's submission id.
    CancelRequest {
        request_id: RuntimeString,
    },
    Approve {
        request_id: RuntimeString,
        response: ApprovalResponse,
    },
    ListApprovalReviewDenials,
    ApproveApprovalReviewDenial {
        event: klaus_agent_core::types::approval_review::ApprovalReviewEvent,
    },
    ResolveElicitation {
        request_id: RuntimeString,
        response: ElicitResponseWire,
    },
    ResolveMcpElicitation {
        request_id: RuntimeString,
        response: McpElicitationResponse,
    },
    ResolveMcpAuthorization {
        request_id: RuntimeString,
        response: McpAuthorizationResponseWire,
    },
    GetMcpAuthorizationStatus {
        server_name: RuntimeString,
    },
    RevokeMcpAuthorization {
        server_name: RuntimeString,
    },
    GetMcpRuntime {
        /// `Full` (default) returns the complete snapshot including all tools,
        /// resources, and prompts. `ToolsAndAuthOnly` returns only tool names
        /// and auth status (cheaper for status-only consumers).
        detail: Option<RuntimeString>,
        /// Opaque pagination cursor for future use.
        cursor: Option<RuntimeString>,
    },
    ReconnectMcpServer {
        server_name: RuntimeString,
    },
    UpsertMcpServer {
        server: ProtocolMcpServerInit,
    },
    RemoveMcpServer {
        server_name: RuntimeString,
    },
    ReloadMcpServers {
        servers: Vec<ProtocolMcpServerInit>,
    },
    /// Host-driven tool call on a connected MCP server (a UI invoking a tool
    /// directly, not the model). Answered with `mcp_tool_call`.
    CallMcpTool {
        server_name: RuntimeString,
        tool: RuntimeString,
        arguments: Option<RuntimeJson>,
    },
    /// Host-driven resource read. Answered with `mcp_resource_read`.
    ReadMcpResource {
        server_name: RuntimeString,
        uri: RuntimeString,
    },
    /// List every connected server's prompts, so the host can build slash
    /// commands without pulling the full runtime snapshot. Answered with
    /// `mcp_prompts`.
    ListMcpPrompts,
    /// Render a named prompt for insertion as a slash command. Answered with
    /// `mcp_prompt`.
    GetMcpPrompt {
        server_name: RuntimeString,
        name: RuntimeString,
        arguments: Option<RuntimeJson>,
    },
    GetRuntimeCapabilities,
    GetExtensionRuntime,
    ListInstalledPlugins,
    GetPluginRuntime,
    SetInstalledPluginEnabled {
        name: RuntimeString,
        enabled: bool,
    },
    UninstallPlugin {
        name: RuntimeString,
        remove_files: bool,
    },
    /// Materialize an already-resolved plugin source directory into the plugin
    /// store and record it in the installed-plugin ledger. The host resolves and
    /// fetches the source (local copy / git / GCS); the engine owns the store
    /// write and the ledger record, keeping the ledger a single-writer surface.
    InstallPlugin {
        marketplace: RuntimeString,
        source_dir: RuntimeString,
        version: Option<RuntimeString>,
        expected_installed_version: Option<RuntimeString>,
    },
    /// Read the plugin packages under the supplied marketplace roots and join
    /// them with the installed-plugin ledger. The host owns WHICH marketplaces
    /// exist and where their content sits on disk (it fetches them); the engine
    /// owns manifest parsing and the install/enable state, so a plugin page never
    /// has to interpret a manifest or the ledger itself.
    ListPluginCatalog {
        roots: Vec<ProtocolPluginCatalogRoot>,
    },
    /// Read the thread's goal (goal mode). Answers with `thread_goal` absent
    /// when no goal is set.
    GetGoal,
    /// Set, edit, pause or resume the thread's goal.
    ///
    /// An `objective` with no existing goal CREATES one; with an existing goal
    /// it edits it. `status` is the host's side of the split: it may pause and
    /// resume, which `update_goal` deliberately cannot.
    SetGoal {
        owned_resources: Option<klaus_agent_core::goal::owned_resources::GoalOwnedResourcesInput>,
        client_request_id: Option<RuntimeString>,
        submit: Option<bool>,
        display_text: Option<RuntimeString>,
        objective: Option<RuntimeString>,
        status: Option<RuntimeString>,
        /// `Some(None)` removes the budget; absent leaves it alone.
        token_budget: Option<Option<i64>>,
        expected_goal_id: Option<Option<RuntimeString>>,
        expected_goal_revision: Option<i64>,
        origin: Option<RuntimeString>,
    },
    /// Drop the thread's goal entirely, guarded against replacing another generation.
    ClearGoal {
        expected_goal_id: Option<RuntimeString>,
        expected_goal_revision: Option<i64>,
        origin: Option<RuntimeString>,
    },
    /// Resolve a host-provided dynamic tool call. This op is handled immediately
    /// so it can unblock the active turn that emitted the request.
    ResolveDynamicToolCall {
        call_id: RuntimeString,
        response: ProtocolDynamicToolResponse,
    },
    /// Rewind the session by `num_turns` user turns. `scope` picks which halves to
    /// undo — conversation, files, or both (default).
    Rewind {
        // Enforce the guard (numTurns >= 1, integer) at parse time — a
        // 0/negative/fractional count is rejected as an invalid op rather than
        // reaching the handler.
        num_turns: u32,
        scope: Option<RewindScope>,
    },
    /// What a `rewind` of `num_turns` WOULD do to the working tree. Changes
    /// nothing. The file half of a rewind is destructive and irreversible, so a
    /// host that confirms with the user needs the real file list first.
    RewindPreview {
        num_turns: u32,
    },
    /// Mid-turn steering: inject a message into the running turn.
    Steer {
        input: Input,
        priority: Option<SteerPriority>,
        /// How the drain must persist this steer once applied; see
        /// [`klaus_agent_core::engine::message_queue::SteerDelivery`]. Absent
        /// means the legacy tool-result shape.
        delivery: Option<klaus_agent_core::engine::message_queue::SteerDelivery>,
        /// Host rendering metadata persisted beside an applied human steer.
        display_metadata: Option<RuntimeJson>,
        /// Optional compatibility field; a supplied id fences this steer to one live turn.
        expected_turn_id: Option<RuntimeString>,
        /// Stable Host queue identity, independent of the protocol submission id.
        queue_message_id: Option<RuntimeString>,
    },
    /// Persist one host-rendered message as a completed turn without running a
    /// model. Conversations that park inbound messages for the owner's decision
    /// still need their content in the ordinary history.
    AppendDisplayMessage {
        message_id: RuntimeString,
        input: Input,
        display_metadata: RuntimeJson,
    },
    SetModel {
        model: RuntimeString,
    },
    GetPlanning,
    SetCollaborationMode {
        mode: RuntimeString,
        expected_revision: Option<u64>,
    },
    ReadPlanDocument {
        plan_id: RuntimeString,
        revision: u64,
    },
    SavePlanDocument {
        plan_id: RuntimeString,
        revision: u64,
        content: RuntimeString,
        expected_content: Option<RuntimeString>,
    },
    AcceptPlan {
        plan_id: RuntimeString,
        revision: u64,
        client_request_id: RuntimeString,
        content: Option<RuntimeString>,
    },
    PlanFeedback {
        plan_id: RuntimeString,
        revision: u64,
        client_request_id: RuntimeString,
        feedback: RuntimeString,
    },
    CancelPlanImplementation {
        plan_id: RuntimeString,
        revision: u64,
    },
    SettlePlanImplementation {
        plan_id: RuntimeString,
        revision: u64,
        client_request_id: RuntimeString,
        outcome: RuntimeString,
    },
    AnswerUserInput {
        request_id: RuntimeString,
        turn_id: RuntimeString,
        tool_call_id: RuntimeString,
        answers: RuntimeMap<Vec<RuntimeString>>,
        status: RuntimeString,
    },
    SetPermissionMode {
        mode: ExternalPermissionMode,
    },
    /// The complete mutable surface after a session is bound. All other init
    /// fields are session-invariant and require a new protocol endpoint.
    UpdateSessionConfig {
        update: ProtocolSessionConfigUpdate,
    },
    GetHistory,
    /// Full saved original history at the ordered follow snapshot boundary.
    GetOriginalHistory,
    /// What this session changed on disk, folded per file. Separate from any
    /// repository diff a host may also show: this one is scoped to the session
    /// and answerable without version control.
    GetFileChanges,
    GetCost,
    ContextSnapshot,
    GetSubagents,
    GetSubagentLifecycle,
    SendSubagentMessage {
        agent_id: RuntimeString,
        message: RuntimeString,
        summary: Option<RuntimeString>,
        user_initiated: bool,
    },
    GetBackgroundProcesses,
    ReadBackgroundProcessOutput {
        shell_id: RuntimeString,
        after_offset: Option<u64>,
        max_bytes: Option<usize>,
    },
    StopBackgroundProcess {
        shell_id: RuntimeString,
        signal: Option<RuntimeString>,
    },
    LoadSubagentTranscript {
        thread_id: RuntimeString,
        agent_id: RuntimeString,
    },
    ResumePending {
        after_submission_id: RuntimeString,
    },
    /// Explicit foreground continuation of preserved queue input after stop or failure.
    ResumeQueued,
    GetQueued,
    TakePending,
    CompactSession {
        instructions: Option<RuntimeString>,
        selection: Option<CompactSelection>,
    },
    SideQuestion {
        question: Input,
    },
    DreamNow,
    PredictionContext,
    CancelPrediction {
        request_id: RuntimeString,
    },
    PredictMessage {
        prompt: RuntimeString,
        source_turn_id: RuntimeString,
        effort: Option<klaus_agent_core::schemas::effort::EffortValue>,
    },
    GenerateTitle {
        instructions: Option<RuntimeString>,
        /// Host-owned title prompt. When present it replaces the
        /// conversation-derived input so the caller controls exactly what the
        /// title model sees.
        prompt: Option<RuntimeString>,
        persist: Option<bool>,
    },
    ListSessions {
        options: Option<ListOptions>,
    },
    ResumeSession {
        thread_id: RuntimeString,
    },
    ForkSession {
        thread_id: RuntimeString,
        /// Thread id the forked session must adopt. When absent the engine
        /// mints one; hosts that key sessions by thread id supply their own so
        /// the identity stays single-id end to end.
        new_thread_id: Option<RuntimeString>,
        /// Cut point: the branch inherits only history strictly BEFORE the
        /// message with this uuid. Absent = full-transcript fork. The source
        /// thread is never modified either way.
        before_message_uuid: Option<RuntimeString>,
        inherit_goal: Option<bool>,
    },
    ArchiveSession {
        thread_id: RuntimeString,
    },
    DeleteSession {
        thread_id: RuntimeString,
    },
    UpdateTitle {
        thread_id: RuntimeString,
        title: RuntimeString,
    },
    GetMemoryMaintenance,
    GetQueueSnapshot,
    EditQueued {
        mutation_id: RuntimeString,
        uuid: RuntimeString,
        expected_revision: u64,
        edit: QueueEdit,
    },
    RemoveQueued {
        mutation_id: RuntimeString,
        uuid: RuntimeString,
        expected_revision: u64,
    },
    ReorderQueued {
        mutation_id: RuntimeString,
        uuid: RuntimeString,
        before_uuid: Option<RuntimeString>,
        expected_revision: u64,
    },
    UnarchiveSession {
        thread_id: RuntimeString,
    },
    RunSetup {
        request: SetupRunRequest,
    },
    GetSetupLifecycle,
    GetTaskList {
        options: Option<TaskListQueryOptions>,
    },
    ResolveAgentMemorySnapshotDecision {
        request_id: RuntimeString,
        decision: AgentMemoryMergeChoice,
    },
    SettleAgentMemorySnapshotUnavailable {
        request_id: RuntimeString,
        reason: AgentMemorySnapshotUnavailableReason,
        error: Option<RuntimeString>,
    },
    GetAgentMemorySnapshotDecision {
        request_id: RuntimeString,
    },
    GetAgentMemorySnapshotDecisions,
    CreateSideTask {
        thread_id: RuntimeString,
        /// Thread id the side-task thread must adopt. Same single-id contract
        /// as `fork_session.newThreadId`.
        new_thread_id: Option<RuntimeString>,
    },
    Shutdown,
    /// Run an isolated review task against the bound session. The review turn
    /// never touches the main conversation history; the correlated `review`
    /// event carries the structured verdict.
    ReviewSession {
        target: ReviewTarget,
        user_facing_hint: Option<RuntimeString>,
    },
    /// Rename a persisted thread in the session store. This is a pure storage
    /// re-key (rollout + sidechains + index); the thread must not be live.
    /// Exists so hosts migrating to single-id identity can converge legacy
    /// engine-minted thread ids onto their session keys.
    RenameThread {
        thread_id: RuntimeString,
        new_thread_id: RuntimeString,
    },
    /// Replace a persisted thread's opaque host-owned metadata attachment
    /// (`None` removes it). The engine stores the value verbatim and never
    /// interprets it: hosts keep their session bookkeeping on the thread
    /// itself instead of maintaining a parallel store.
    UpdateThreadHostMetadata {
        thread_id: RuntimeString,
        host_metadata: Option<RuntimeJson>,
    },
    /// Atomically update the session's runtime policy without waiting for the
    /// current turn to finish. Takes effect on the next tool invocation within
    /// the running turn. Answered immediately outside the op-chain queue.
    SetRuntimePolicy {
        /// Deprecated input, translated once before the canonical update.
        permission_mode: Option<ExternalPermissionMode>,
        pre_plan_permission_mode: Option<ExternalPermissionMode>,
        approval_policy: Option<klaus_agent_core::types::permission::ApprovalPolicy>,
        sandbox_mode: Option<klaus_agent_core::types::permission::SandboxMode>,
        approvals_reviewer: Option<klaus_agent_core::types::permission::ApprovalsReviewer>,
        sandbox_workspace_write: Option<SandboxWorkspaceWrite>,
        network_allowed: Option<bool>,
    },
    /// Switch the active turn's model without waiting for the current turn to
    /// finish. Takes effect on the next LLM call within the running ReAct loop.
    /// Answered immediately outside the op-chain queue. Also stages the model
    /// for subsequent turns.
    SetRuntimeModel {
        model: RuntimeString,
    },
    /// Replace host-discovered skills for future turns without replacing the session.
    SetSkills {
        skills: Vec<Skill>,
    },
}

/// Explicit Host-unavailable edge for one pending Agent Memory snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentMemorySnapshotUnavailableReason {
    TimedOut,
    Disconnected,
    NoHandler,
}

impl RuntimeEncode for AgentMemorySnapshotUnavailableReason {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(
            out,
            match self {
                Self::TimedOut => "timed-out",
                Self::Disconnected => "disconnected",
                Self::NoHandler => "no-handler",
            },
        )
    }
}

impl RuntimeDecode for AgentMemorySnapshotUnavailableReason {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("timed-out", &[]),
                RuntimeEnumVariant::new("disconnected", &[]),
                RuntimeEnumVariant::new("no-handler", &[]),
            ],
            &[Self::TimedOut, Self::Disconnected, Self::NoHandler],
        )
    }
}

/// Correlated single-request query, including an explicit not-found result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMemorySnapshotDecisionQueryResult {
    pub request_id: RuntimeString,
    pub audit: Option<AgentMemorySnapshotDecisionAudit>,
}

/// A wire elicitation request (an ask_user surfaced to the host).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElicitRequestWire {
    pub prompt: RuntimeString,
    pub questions: Option<Vec<RuntimeObject>>,
    pub attachments_supported: Option<bool>,
    pub metadata: Option<RuntimeObject>,
    pub options: Option<Vec<RuntimeString>>,
    pub multi_select: Option<bool>,
}

impl From<ElicitRequest> for ElicitRequestWire {
    fn from(r: ElicitRequest) -> Self {
        ElicitRequestWire {
            prompt: r.prompt,
            attachments_supported: r.attachments_supported,
            questions: r.questions,
            metadata: r.metadata,
            options: r.options,
            multi_select: r.multi_select,
        }
    }
}

/// The `elicitation` protocol payload: an ask_user request keyed by a minted id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Elicitation {
    pub request_id: RuntimeString,
    pub request: ElicitRequestWire,
}

/// The host's answer to an elicitation: accept (values) or decline (reason).
#[derive(Debug, Clone, PartialEq)]
pub enum ElicitResponseWire {
    Answers {
        answers: RuntimeMap<RuntimeString>,
        attachments: Option<Box<RuntimeMap<Vec<ContentBlock>>>>,
    },
    Accept {
        values: Vec<RuntimeString>,
    },
    Decline {
        reason: Option<RuntimeString>,
    },
}

impl TryFrom<ElicitResponseWire> for ElicitResponse {
    type Error = RuntimeCodecError;

    fn try_from(r: ElicitResponseWire) -> Result<Self, Self::Error> {
        Ok(match r {
            ElicitResponseWire::Answers {
                answers,
                attachments,
            } => ElicitResponse::Answers {
                answers,
                attachments,
            },
            ElicitResponseWire::Accept { values } => ElicitResponse::Accept { values },
            ElicitResponseWire::Decline { reason } => ElicitResponse::Decline { reason },
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpElicitationResolvedWire {
    pub request_id: RuntimeString,
    pub action: McpElicitationAction,
}

/// One interactive MCP OAuth request surfaced by the endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpAuthorizationRequestWire {
    pub request_id: RuntimeString,
    pub server_name: RuntimeString,
    pub authorization_url: RuntimeString,
    pub redirect_uri: RuntimeString,
}

/// The host's response to an MCP authorization request. The code is ingress-only
/// and is never included in an outbound acknowledgement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpAuthorizationResponseWire {
    Code {
        code: RuntimeString,
        state: Option<RuntimeString>,
    },
    Decline {
        reason: Option<RuntimeString>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpAuthorizationResolvedWire {
    pub request_id: RuntimeString,
    pub accepted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpAuthorizationStatusWire {
    pub server_name: RuntimeString,
    pub auth_status: McpAuthStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpAuthorizationRevokedWire {
    pub server_name: RuntimeString,
    pub auth_status: McpAuthStatus,
    pub revoked: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum McpRuntimeScope {
    ActiveSession,
}

impl RuntimeEncode for McpRuntimeScope {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::ActiveSession => "activeSession",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for McpRuntimeScope {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[RuntimeEnumVariant::new("activeSession", &[])],
            &[Self::ActiveSession],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpRuntimeResult {
    pub runtime: McpRuntimeSnapshot,
    pub activation_scope: McpRuntimeScope,
}

/// Reply to `call_mcp_tool`: the normalized MCP tool result. Content blocks are
/// carried as opaque JSON so the wire stays 1:1 with what the server returned.
#[derive(Debug, Clone, PartialEq)]
pub struct McpToolCallResultWire {
    pub server_name: RuntimeString,
    pub content: RuntimeJson,
    pub structured_content: Option<RuntimeJson>,
    pub is_error: Option<bool>,
}

/// Reply to `read_mcp_resource`: the contents a `resources/read` returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpResourceReadResultWire {
    pub server_name: RuntimeString,
    pub contents: Vec<McpResourceContent>,
}

/// Reply to `list_mcp_prompts`: one server's prompts, tagged with the server so
/// the host can namespace the slash command it builds from each.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpServerPromptsWire {
    pub server_name: RuntimeString,
    pub prompts: Vec<McpPromptRuntimeSnapshot>,
}

/// Reply to `list_mcp_prompts`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpPromptsResultWire {
    pub servers: Vec<McpServerPromptsWire>,
}

/// Reply to `get_mcp_prompt`: the rendered prompt text to insert.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpPromptResultWire {
    pub server_name: RuntimeString,
    pub name: RuntimeString,
    pub content: RuntimeString,
}

/// A read-only view of one background sub-agent, for a host progress panel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackgroundAgentSummary {
    pub id: RuntimeString,
    pub description: RuntimeString,
    pub status: BackgroundTaskStatus,
    pub teammate: bool,
    pub idle: bool,
    pub transcript: RuntimeString,
    pub output_file: RuntimeString,
}

impl From<BackgroundAgentInfo> for BackgroundAgentSummary {
    fn from(i: BackgroundAgentInfo) -> Self {
        BackgroundAgentSummary {
            id: i.id.into(),
            description: i.description.into(),
            status: i.status,
            teammate: i.teammate,
            idle: i.idle,
            transcript: i.transcript.into(),
            output_file: i.output_file.into(),
        }
    }
}

/// Final outcome of a `run` (a JSON-safe subset of core's turn result).
#[derive(Debug, Clone, PartialEq)]
pub struct RunResult {
    pub stop_reason: TerminalReason,
    /// Present-but-null when the run produced no `turn.completed` (e.g. a failure).
    pub usage: Option<Usage>,
    pub final_text: RuntimeString,
    /// The persisted assistant message that owns `final_text`. Absent when the
    /// Turn ended after tool-preface narration without a real final reply.
    pub final_message_id: Option<RuntimeString>,
    /// The parsed structured result — present only when the run carried an
    /// `output_schema` and the final text was valid JSON.
    pub structured_output: Option<RuntimeJson>,
}

/// Admission decision for an atomic pending-input continuation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingResumeResult {
    pub started: bool,
    pub reason: Option<RuntimeString>,
}

/// Completion of a checked steer. This is emitted only after the queued input
/// has been attached to a real model continuation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SteerResult {
    pub applied: bool,
}

impl RuntimeEncode for SteerResult {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self { applied } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("applied", applied)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for SteerResult {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                applied: object.required("applied", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundSession {
    pub thread_id: RuntimeString,
    pub forked_from_id: Option<RuntimeString>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveSessionResult {
    pub thread_id: RuntimeString,
    pub archived: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteSessionResult {
    pub thread_id: RuntimeString,
    pub deleted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateTitleResult {
    pub thread_id: RuntimeString,
    pub title: RuntimeString,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenameThreadResult {
    /// The thread id after the rename (the requested new id).
    pub thread_id: RuntimeString,
    pub renamed: bool,
}

/// Wire projection of the thread's goal (goal mode).
///
/// `status` is the core status name (`active` / `paused` / `blocked` /
/// `usage_limited` / `budget_limited` / `complete`), carried as a string so a
/// host that does not know a future status can still display the goal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolThreadGoal {
    pub thread_id: RuntimeString,
    pub goal_id: RuntimeString,
    pub objective: RuntimeString,
    pub status: RuntimeString,
    pub token_budget: Option<i64>,
    pub tokens_used: i64,
    pub time_used_seconds: i64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

impl From<&klaus_agent_core::goal::types::ThreadGoal> for ProtocolThreadGoal {
    fn from(goal: &klaus_agent_core::goal::types::ThreadGoal) -> Self {
        Self {
            thread_id: goal.thread_id.clone(),
            goal_id: goal.goal_id.clone(),
            objective: goal.objective.clone(),
            status: RuntimeString::from(goal.status.wire_name()),
            token_budget: goal.token_budget,
            tokens_used: goal.tokens_used,
            time_used_seconds: goal.time_used_seconds,
            created_at_ms: goal.created_at_ms,
            updated_at_ms: goal.updated_at_ms,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateThreadHostMetadataResult {
    pub thread_id: RuntimeString,
    pub updated: bool,
}

/// A stable boundary and the side of the conversation to summarize.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactSelection {
    pub message_id: RuntimeString,
    pub direction: CompactSelectionDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CompactSelectionDirection {
    From,
    UpTo,
}

impl RuntimeEncode for CompactSelectionDirection {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(
            out,
            match self {
                Self::From => "from",
                Self::UpTo => "upTo",
            },
        )
    }
}

impl RuntimeDecode for CompactSelectionDirection {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("from", &[]),
                RuntimeEnumVariant::new("upTo", &[]),
            ],
            &[Self::From, Self::UpTo],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactSessionResult {
    pub compacted: bool,
    pub freed_tokens: u64,
    pub reason: Option<CompactionNoopReason>,
    pub diagnostics: Vec<CompactionDiagnostic>,
    pub before_tokens: u64,
    pub after_tokens: u64,
    pub before_messages: usize,
    pub after_messages: usize,
    pub boundary: Option<CompactBoundaryMessage>,
}

impl From<CompactOutcome> for CompactSessionResult {
    fn from(outcome: CompactOutcome) -> Self {
        Self {
            compacted: outcome.compacted,
            freed_tokens: outcome.freed_tokens,
            reason: outcome.reason,
            diagnostics: outcome.diagnostics,
            before_tokens: outcome.before_tokens,
            after_tokens: outcome.after_tokens,
            before_messages: outcome.before_messages,
            after_messages: outcome.after_messages,
            boundary: outcome.boundary,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactProgressPhase {
    Started,
    Completed,
    Failed,
}

impl RuntimeEncode for CompactProgressPhase {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Started => "started",
            Self::Completed => "completed",
            Self::Failed => "failed",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for CompactProgressPhase {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("started", &[]),
                RuntimeEnumVariant::new("completed", &[]),
                RuntimeEnumVariant::new("failed", &[]),
            ],
            &[Self::Started, Self::Completed, Self::Failed],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactTrigger {
    Manual,
    Auto,
}

impl RuntimeEncode for CompactTrigger {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Manual => "manual",
            Self::Auto => "auto",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for CompactTrigger {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("manual", &[]),
                RuntimeEnumVariant::new("auto", &[]),
            ],
            &[Self::Manual, Self::Auto],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactProgress {
    pub id: RuntimeString,
    pub phase: CompactProgressPhase,
    pub trigger: CompactTrigger,
    pub mode: CompactionMode,
    pub result: Option<CompactSessionResult>,
    pub error: Option<RuntimeString>,
}

impl CompactProgress {
    pub(crate) fn manual_result(id: RuntimeString, result: CompactSessionResult) -> Self {
        let error = if result.compacted {
            None
        } else {
            result
                .diagnostics
                .iter()
                .find(|diagnostic| diagnostic.stage == CompactionDiagnosticStage::Summary)
                .map(|diagnostic| {
                    diagnostic.message.clone().unwrap_or_else(|| {
                        "Context compaction could not produce a complete summary.".into()
                    })
                })
        };
        Self {
            id,
            phase: if error.is_some() {
                CompactProgressPhase::Failed
            } else {
                CompactProgressPhase::Completed
            },
            trigger: CompactTrigger::Manual,
            mode: CompactionMode::Full,
            result: Some(result),
            error,
        }
    }
}

/// Private Host-authenticated route data for Windows managed networking.
///
/// The stdio endpoint accepts this only on its init channel. It is not part of
/// engine config, sandbox policy, command arguments, or the child environment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolTrustedWindowsSandboxContext {
    pub route_sid: RuntimeString,
    pub http_addr: RuntimeString,
    pub socks_addr: Option<RuntimeString>,
    pub broker_generation: RuntimeString,
    pub policy_digest: RuntimeString,
}

#[derive(Debug, Clone, Default)]
pub struct ProtocolSessionInit {
    pub collaboration_mode: Option<RuntimeString>,
    /// Thread id this session must own. When supplied the engine adopts it as
    /// the persistent thread identity instead of minting one, so a host can
    /// keep a single id across its own routing layer and the rollout store.
    pub thread_id: Option<RuntimeString>,
    pub cwd: Option<RuntimeString>,
    pub config: Option<RuntimeJson>,
    pub permission_mode: Option<ExternalPermissionMode>,
    /// Permission mode to restore when a host starts the session in Plan mode.
    pub pre_plan_permission_mode: Option<ExternalPermissionMode>,
    /// Runtime network selection; omission derives it from the sandbox policy.
    pub network_allowed: Option<bool>,
    pub provider: Option<ProviderInit>,
    pub auth: Option<ProviderAuthInit>,
    pub trusted_windows_sandbox_context: Option<ProtocolTrustedWindowsSandboxContext>,
    /// Redirect URI registered during MCP OAuth dynamic client registration.
    pub mcp_oauth_redirect_uri: Option<RuntimeString>,
    pub host_capabilities: Option<ProtocolHostCapabilitiesInit>,
    /// Host-owned timeout/fallback policy for interactive Agent Memory snapshot
    /// decisions. Supplying this also makes the stdio endpoint the interactive
    /// Host bridge for requested/audit events.
    pub agent_memory_snapshot_policy: Option<AgentMemorySnapshotDecisionPolicy>,
    /// Replaces the engine's generated base prompt for this session.
    pub base_instructions: Option<RuntimeString>,
    /// Appended after the base prompt as a distinct, stable prompt part.
    pub developer_instructions: Option<RuntimeString>,
    /// Host-implemented tools using the engine's canonical function/namespace shape.
    pub dynamic_tools: Vec<ProtocolDynamicToolSpec>,
    /// Serializable language-server process definitions.
    pub lsp_servers: Vec<LspServerConfig>,
    /// Explicit environment merged into shell tool subprocesses. When present,
    /// this takes precedence over login-shell environment capture.
    pub shell_env: Option<RuntimeEnv>,
    /// Static session sub-agent definitions. Session definitions override
    /// endpoint/plugin/builtin definitions with the same name.
    pub agents: Vec<AgentDefinition>,
    /// Run this session's root with the named effective agent definition.
    /// An unknown name fails startup; omission preserves the ordinary root.
    pub agent_definition_name: Option<RuntimeString>,
    /// Static session skills. These augment endpoint/plugin skills and override
    /// name collisions.
    pub skills: Vec<Skill>,
    /// Static output styles available to this session.
    pub output_styles: Vec<OutputStyle>,
    /// Name selected from the effective output-style registry.
    pub selected_output_style: Option<RuntimeString>,
    /// Serializable command hooks only. In-process callback hooks are not a wire
    /// capability and cannot be represented by this type.
    pub command_hooks: Vec<ProtocolCommandHook>,
    /// Ordered model-substring pricing table; first match wins.
    /// `None` selects core defaults; an explicit empty table intentionally
    /// prices every unmatched model at the zero fallback.
    pub pricing: Option<Vec<ProtocolModelPricing>>,
    /// The active model's capability set, declared by the host. The engine never
    /// infers capability from the model id; a provider reads exactly what the
    /// host declared here, and an absent set means the minimal capability.
    pub capabilities: Option<ModelCapabilitiesInit>,
    /// Per-model capability overrides for the session's tier-mapped models.
    /// Auxiliary work (title/compaction/memory) and tier-pinned sub-agents run
    /// on models other than the main one; each such model declares its own set
    /// here. A model absent from the list reads `capabilities`.
    pub capabilities_by_model: Option<Vec<ProtocolModelCapabilitiesEntry>>,
    /// Source-qualified targets resolved by the named facade before initialization.
    pub model_tier_targets: Option<RuntimeJson>,
    /// Independent advisor target; omitted/null disables advisor consultation.
    pub advisor_target: Option<RuntimeJson>,
    /// Independent per-feature auxiliary targets (title, prediction, memory
    /// maintenance); a missing/null slot follows the session's main model.
    pub feature_targets: Option<RuntimeJson>,
    /// Stable source and catalog identity of the main target.
    pub model_source: Option<RuntimeJson>,
}

/// One model's declared capability set inside [`ProtocolSessionInit::capabilities_by_model`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolModelCapabilitiesEntry {
    /// The exact model id this set applies to.
    pub model: RuntimeString,
    pub capabilities: ModelCapabilitiesInit,
}

/// Wire form of the per-model capability set (the host-declared source of truth
/// the engine's providers read instead of a model-id table).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ModelCapabilitiesInit {
    /// Tool/function calling. Absent keeps the provider's backward-compatible
    /// default; false explicitly keeps a plain-chat model tool-free.
    pub tools: Option<bool>,
    /// Explicit compatibility of thinking with specified tool choice.
    pub forced_tool_choice_with_thinking: Option<bool>,
    /// Reasoning effort ladder. Absent = no effort knob.
    pub effort: Option<EffortCapabilityInit>,
    /// Supported native output detail; absent leaves the parameter unset.
    pub output_verbosity: Option<klaus_agent_core::types::provider::OutputVerbosity>,
    pub auto_review_model_override: Option<RuntimeString>,
    pub requires_sandboxed_review: bool,
    pub node_repl_auto_review_required: bool,
    pub guardian: Option<RuntimeJson>,
    pub guardian_v2: Option<RuntimeJson>,
    pub model_messages: Option<RuntimeJson>,
    pub comp_hash: Option<RuntimeString>,
    pub supports_reasoning_summary_parameter: Option<bool>,
    pub default_reasoning_summary: Option<klaus_agent_core::types::provider::ReasoningSummary>,
    /// Review model effort labels retain the provider's complete vocabulary.
    pub supported_reasoning_efforts: Vec<RuntimeString>,
    pub default_reasoning_effort: Option<RuntimeString>,
    /// Preferred native effort while local collaboration is active.
    pub multi_agent_reasoning_effort: Option<RuntimeString>,
    pub effective_context_window_percent: Option<i64>,
    pub auto_compact_token_limit: Option<u64>,
    pub used_fallback_model_metadata: bool,
    pub model_specialty: Option<RuntimeString>,
    pub supports_image_detail_original: bool,
    /// Extended thinking. Absent = the model does not think.
    pub thinking: Option<ThinkingCapabilityInit>,
    pub structured_output: bool,
    pub web_search: bool,
    pub prompt_cache_control: bool,
    pub native_pdf: bool,
    pub vision: bool,
    pub one_million_context: bool,
    pub task_budgets: bool,
    pub fast_mode: bool,
    pub context_window: Option<u64>,
    pub max_output_tokens: Option<u64>,
    pub image_limits: Option<klaus_agent_core::types::provider::ImageLimits>,
    pub max_media_blocks: Option<std::num::NonZeroU32>,
    /// Exact source-declared native audio/video input MIME types.
    pub media_input_mime_types: Vec<RuntimeString>,
    /// OpenAI-compatible reasoning wire dialect: `openai` (default), `deepseek`,
    /// `qwen` (Chat Completions thinking switch), `managedQwen`, `qoder`
    /// (the local bridge), `llamaCpp`, or `legacyTokens`
    /// (a gateway that reads the pre-2024 `max_tokens` output cap and ignores
    /// `max_completion_tokens`). Selects effort spelling, thinking-off signaling,
    /// the output-cap field name, and managed-budget behavior. An unrecognized
    /// value falls back to `openai`.
    pub reasoning_dialect: Option<RuntimeString>,
    /// Gemini thinking wire dialect (level vs budget).
    pub gemini_thinking: Option<GeminiThinkingInit>,
}

/// Declared reasoning-effort ladder: which levels the model honors.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EffortCapabilityInit {
    /// Supported levels (`low`..`max`). Empty = no effort knob.
    pub levels: Vec<RuntimeString>,
    /// Preferred level when the request pins none.
    pub default: Option<RuntimeString>,
}

/// Declared extended-thinking support.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ThinkingCapabilityInit {
    pub adaptive: bool,
    pub interleaved: bool,
    /// Whether the model accepts an explicit "thinking disabled" request.
    pub can_disable_thinking: bool,
}

/// Gemini thinking wire dialect. `mode` is `level` (Gemini 3+ thinkingLevel) or
/// `budget` (Gemini 2.5 numeric thinkingBudget within `[min,max]`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GeminiThinkingInit {
    pub mode: RuntimeString,
    pub min: Option<u64>,
    pub max: Option<u64>,
    pub can_disable: bool,
}

impl PartialEq for ProtocolSessionInit {
    fn eq(&self, other: &Self) -> bool {
        match (
            runtime_codec::encode_value(self),
            runtime_codec::encode_value(other),
        ) {
            (Ok(left), Ok(right)) => left == right,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolCommandHook {
    pub event: HookEvent,
    pub matcher: Option<RuntimeString>,
    pub command: RuntimeString,
    pub timeout_ms: Option<u64>,
    pub is_async: bool,
}

/// One marketplace root the host has already materialized on disk. `dir` holds
/// the plugin packages (each a direct child directory carrying a manifest).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolPluginCatalogRoot {
    pub marketplace: RuntimeString,
    pub dir: RuntimeString,
}

/// The names one package contributes, read from its manifest. This is what lets a
/// plugin page answer "what will I get?" BEFORE installing — the runtime graph
/// only describes plugins that are already installed and loaded.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProtocolPluginContributions {
    pub skills: Vec<RuntimeString>,
    pub agents: Vec<RuntimeString>,
    pub commands: Vec<RuntimeString>,
    pub output_styles: Vec<RuntimeString>,
    /// `"<event> (<count>)"` per hook event, which is what tells a user WHEN a
    /// plugin's hooks fire.
    pub hooks: Vec<RuntimeString>,
    pub mcp_servers: Vec<RuntimeString>,
    pub lsp_servers: Vec<RuntimeString>,
}

/// One catalog entry: the package as the engine read it, joined with the ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolPluginCatalogEntry {
    /// `<name>@<marketplace>`, the ledger key.
    pub id: RuntimeString,
    pub name: RuntimeString,
    pub marketplace: RuntimeString,
    pub version: Option<RuntimeString>,
    pub description: Option<RuntimeString>,
    pub installed: bool,
    /// Absent from the ledger reads as false; an installed record with no
    /// explicit flag reads as true (the engine's `default_enabled`).
    pub enabled: bool,
    pub contributions: ProtocolPluginContributions,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProtocolPluginCatalog {
    pub entries: Vec<ProtocolPluginCatalogEntry>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProtocolModelPricing {
    pub model: RuntimeString,
    pub pricing: ModelPricing,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProtocolDynamicToolSpec {
    Function(ProtocolDynamicToolFunctionSpec),
    Namespace(ProtocolDynamicToolNamespaceSpec),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProtocolDynamicToolFunctionSpec {
    pub name: RuntimeString,
    pub description: RuntimeString,
    pub input_schema: RuntimeJson,
    pub defer_loading: bool,
    /// The host declares this tool observes without mutating. It feeds the
    /// permission engine's read-only baseline exactly like a builtin's own
    /// predicate, so a host tool that only reads state stops prompting on every
    /// call and stays usable in plan mode. Defaults to false: a host that says
    /// nothing keeps the conservative mutating classification.
    pub read_only: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProtocolDynamicToolNamespaceSpec {
    pub name: RuntimeString,
    pub description: RuntimeString,
    pub tools: Vec<ProtocolDynamicToolNamespaceTool>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProtocolDynamicToolNamespaceTool {
    Function(ProtocolDynamicToolFunctionSpec),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProtocolDynamicToolCall {
    pub call_id: RuntimeString,
    pub thread_id: RuntimeString,
    pub turn_id: RuntimeString,
    pub namespace: Option<RuntimeString>,
    pub tool: RuntimeString,
    pub arguments: RuntimeJson,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProtocolDynamicToolResponse {
    pub content_items: Vec<ProtocolDynamicToolCallOutputContentItem>,
    pub success: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProtocolDynamicToolCallOutputContentItem {
    InputText { text: RuntimeString },
    InputImage { image_url: RuntimeString },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolDynamicToolResolved {
    pub call_id: RuntimeString,
}

/// Canonical durable presentation of a user-submitted goal before turn binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolGoalSubmission {
    pub message_uuid: RuntimeString,
    pub display_uuid: RuntimeString,
    pub text: RuntimeString,
}

impl From<&klaus_agent_core::goal::retained_context::GoalSubmission> for ProtocolGoalSubmission {
    fn from(value: &klaus_agent_core::goal::retained_context::GoalSubmission) -> Self {
        Self {
            message_uuid: value.message_uuid.clone(),
            display_uuid: value.display_uuid.clone(),
            text: value.text.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ProtocolSessionConfigUpdate {
    pub subagent_model_catalog: Option<bool>,
    pub subagent_model_options: Option<Vec<klaus_agent_core::schemas::config::SubagentModelOption>>,
    pub ultra_mode: Option<bool>,
    pub goal: Option<bool>,
    pub maximum_goal_token_budget: Option<Option<i64>>,
    pub model: Option<RuntimeString>,
    pub permission_mode: Option<ExternalPermissionMode>,
    pub model_verbosity: Option<Option<klaus_agent_core::types::provider::OutputVerbosity>>,
    pub reasoning_summary: Option<klaus_agent_core::types::provider::ReasoningSummary>,
    pub web_search_access: Option<klaus_agent_core::schemas::config::WebSearchAccess>,
    pub approval_review:
        Option<Option<klaus_agent_core::engine::approval_async::AsyncFeatureConfig>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProtocolSessionConfigUpdateResult {
    pub subagent_model_catalog: Option<bool>,
    pub subagent_model_options: Option<Vec<klaus_agent_core::schemas::config::SubagentModelOption>>,
    pub ultra_mode: Option<bool>,
    pub goal: Option<bool>,
    pub maximum_goal_token_budget: Option<Option<i64>>,
    pub model: Option<RuntimeString>,
    pub permission_mode: Option<ExternalPermissionMode>,
    pub model_verbosity: Option<Option<klaus_agent_core::types::provider::OutputVerbosity>>,
    pub reasoning_summary: Option<klaus_agent_core::types::provider::ReasoningSummary>,
    pub web_search_access: Option<klaus_agent_core::schemas::config::WebSearchAccess>,
    pub approval_review:
        Option<Option<klaus_agent_core::engine::approval_async::AsyncFeatureConfig>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProtocolHostCapabilitiesInit {
    pub mcp_servers: Vec<ProtocolMcpServerInit>,
    pub runtime_capability_roots: Vec<ProtocolRuntimeCapabilityRootInit>,
    pub selected_capability_roots: Option<Vec<ProtocolSelectedCapabilityRootInit>>,
}

/// One MCP server a cross-process host declares for a session. `disabledTools`
/// is the per-TOOL off switch: the names the user turned off on this server, so a
/// host does not have to choose between the whole server and nothing. Only the
/// deny list is on the wire — an allow list has no product surface, and an unused
/// wire field is a field nobody keeps honest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolMcpServerInit {
    Stdio {
        name: RuntimeString,
        command: RuntimeString,
        args: Option<Vec<RuntimeString>>,
        env: Option<RuntimeEnv>,
        always_load: Option<bool>,
        disabled_tools: Option<Vec<RuntimeString>>,
    },
    Http {
        name: RuntimeString,
        url: RuntimeString,
        headers: Option<BTreeMap<RuntimeString, RuntimeString>>,
        oauth: Option<ProtocolMcpOAuthOption>,
        always_load: Option<bool>,
        disabled_tools: Option<Vec<RuntimeString>>,
    },
    Sse {
        name: RuntimeString,
        url: RuntimeString,
        headers: Option<BTreeMap<RuntimeString, RuntimeString>>,
        oauth: Option<ProtocolMcpOAuthOption>,
        always_load: Option<bool>,
        disabled_tools: Option<Vec<RuntimeString>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolRuntimeCapabilityRootInit {
    pub id: RuntimeString,
    pub enabled: Option<bool>,
    pub browser: Option<ProtocolBrowserCapabilityInit>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolBrowserCapabilityInit {
    pub mcp_server: Option<RuntimeString>,
    pub host_reference: Option<RuntimeString>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolSelectedCapabilityRootInit {
    Host {
        id: RuntimeString,
    },
    Plugin {
        id: RuntimeString,
    },
    Environment {
        environment_id: RuntimeString,
        root: RuntimeString,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolMcpOAuthOption {
    Advertised(bool),
    Object { scopes: Option<Vec<RuntimeString>> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderInit {
    Anthropic {
        model: Option<RuntimeString>,
        base_url: Option<RuntimeString>,
        beta_header: Option<RuntimeString>,
        http_headers: Option<BTreeMap<RuntimeString, RuntimeString>>,
        env_http_headers: Option<BTreeMap<RuntimeString, RuntimeString>>,
        /// How a stored API key is spent on the wire: the native `x-api-key`
        /// header when absent, or an `Authorization: Bearer` carrier for
        /// gateways that validate only the bearer scheme.
        api_key_scheme: Option<RuntimeString>,
        /// Request-body `metadata.user_id` for claude.ai OAuth subscription
        /// abuse / rate-limit bucketing (the host forwards a stable opaque id
        /// for the OAuth subscription session).
        metadata_user_id: Option<RuntimeString>,
    },
    Openai {
        model: Option<RuntimeString>,
        api: Option<RuntimeString>,
        base_url: Option<RuntimeString>,
        compatibility: Option<OpenAiCompatibilityInit>,
    },
    Gemini {
        model: Option<RuntimeString>,
        base_url: Option<RuntimeString>,
        code_assist: Option<GeminiCodeAssistInit>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeminiCodeAssistInit {
    pub project: Option<RuntimeString>,
    pub endpoint: Option<RuntimeString>,
    pub session_id: Option<RuntimeString>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct OpenAiCompatibilityInit {
    pub instruction_role: Option<RuntimeString>,
    pub use_model_default_effort: Option<bool>,
    pub extra_headers: Option<BTreeMap<RuntimeString, RuntimeString>>,
    pub request_identity: Option<OpenAiRequestIdentityInit>,
    pub responses_function_tools: Option<bool>,
}

impl fmt::Debug for OpenAiCompatibilityInit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let header_names = self
            .extra_headers
            .as_ref()
            .map(|headers| headers.keys().collect::<Vec<_>>())
            .unwrap_or_default();
        f.debug_struct("OpenAiCompatibilityInit")
            .field("instruction_role", &self.instruction_role)
            .field("use_model_default_effort", &self.use_model_default_effort)
            .field("extra_header_names", &header_names)
            .field("responses_function_tools", &self.responses_function_tools)
            .field(
                "request_identity",
                &self.request_identity.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct OpenAiRequestIdentityInit {
    pub session_id: RuntimeString,
}

impl fmt::Debug for OpenAiRequestIdentityInit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenAiRequestIdentityInit")
            .field("session_id", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderAuthInit {
    Env,
    ApiKey {
        key: RuntimeString,
    },
    /// Select an API key already persisted in the endpoint credential store,
    /// keyed by a stable host-owned id. The key value stays in the store; the
    /// host never re-sends it. This is the BYO model-provider counterpart of
    /// [`ProviderAuthInit::OAuthStored`].
    ApiKeyStored {
        credential_id: RuntimeString,
    },
    StaticBearer {
        token: RuntimeString,
    },
    OAuthToken {
        access_token: RuntimeString,
        refresh_token: Option<RuntimeString>,
        expires_at: Option<i64>,
        id_token: Option<RuntimeString>,
        oauth: Option<ProviderOAuthInit>,
        base_url: Option<RuntimeString>,
        extra_headers: Option<BTreeMap<RuntimeString, RuntimeString>>,
        /// Stable host-owned identity for persisting and selecting this token
        /// set. Token values remain in the endpoint credential store.
        credential_id: Option<RuntimeString>,
        /// Explicit re-login semantics. By default an existing valid stored
        /// token set wins so a stale host copy cannot undo refresh rotation.
        replace_stored: Option<bool>,
        /// A queued set of same-source accounts behind the head token. The
        /// provider rotates to the next account when the upstream refuses a
        /// request for quota reasons, and persists the rotated queue order.
        rotation: Option<ProviderCredentialRotationInit>,
    },
    /// Select a provider OAuth token set already persisted by a prior endpoint.
    OAuthStored {
        credential_id: RuntimeString,
        oauth: Option<ProviderOAuthInit>,
        base_url: Option<RuntimeString>,
        extra_headers: Option<BTreeMap<RuntimeString, RuntimeString>>,
    },
}

/// One queued account of a multi-account subscription credential set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderRotationAccountInit {
    /// Stable account id; the credential namespace is keyed on it.
    pub id: RuntimeString,
    /// Persistent credential id for this account's token set.
    pub credential_id: RuntimeString,
    pub access_token: RuntimeString,
    pub refresh_token: Option<RuntimeString>,
    pub expires_at: Option<i64>,
}

/// The account queue behind a subscription login: where the queue order is
/// persisted and every account's tokens, head first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderCredentialRotationInit {
    /// Absolute path of the host-owned credential file whose account array
    /// order is the rotation queue.
    pub file_path: RuntimeString,
    pub accounts: Vec<ProviderRotationAccountInit>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderOAuthInit {
    pub client_id: Option<RuntimeString>,
    pub authorize_url: Option<RuntimeString>,
    pub token_url: Option<RuntimeString>,
    pub redirect_uri: Option<RuntimeString>,
    pub scopes: Option<Vec<RuntimeString>>,
    pub extra_authorize_params: Option<Vec<(RuntimeString, RuntimeString)>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InitSessionResult {
    pub accepted: bool,
}

impl RuntimeEncode for InitSessionResult {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self { accepted } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("accepted", accepted)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for InitSessionResult {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                accepted: object.required("accepted", &[])?,
            })
        })
    }
}

/// When a persisted plugin mutation becomes effective in the engine runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginActivationScope {
    EndpointRestart,
    FutureSessions,
    NextTurn,
}

impl RuntimeEncode for PluginActivationScope {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::EndpointRestart => "endpoint_restart",
            Self::FutureSessions => "future_sessions",
            Self::NextTurn => "next_turn",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for PluginActivationScope {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("endpoint_restart", &[]),
                RuntimeEnumVariant::new("future_sessions", &[]),
                RuntimeEnumVariant::new("next_turn", &[]),
            ],
            &[Self::EndpointRestart, Self::FutureSessions, Self::NextTurn],
        )
    }
}

/// Successful persisted plugin mutation plus the effective endpoint runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstalledPluginMutationResult {
    pub persisted: bool,
    pub applied_to_current_session: bool,
    pub activation_scope: PluginActivationScope,
    pub restart_required: bool,
    pub plugin: InstalledPluginRecord,
    pub installed_plugins: InstalledPlugins,
    pub runtime: PluginRuntimeSnapshot,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GenerateTitleOpResult {
    pub thread_id: RuntimeString,
    pub title: GenerateTitleResult,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PredictionResult {
    pub suggestion: Option<RuntimeString>,
}

/// One turn's file changes, as reported by `get_file_changes`.
///
/// Grouped by turn because that is the scope the counts mean something in: a
/// turn's volume stops being readable once it is summed with the next turn's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolSessionTurnFileChanges {
    /// `None` only for records replayed from a rollout written before file
    /// changes carried their turn id.
    pub turn_id: Option<RuntimeString>,
    pub files: Vec<ProtocolSessionFileChange>,
}

/// One file a turn changed, as reported by `get_file_changes`.
///
/// `added`/`removed` measure this turn's effect on the file ONCE, against the
/// content the turn first found there — not the sum of its edits, which
/// double-counts every line a later edit touches again. They are not a net diff
/// against a commit either: a host showing "what this turn produced" must stay
/// answerable in a directory with no repository, and must not lose an edit just
/// because it was committed mid-session.
///
/// `None` means the volume is unknown (no readable baseline, or no exact count
/// was affordable). A host must render that as unknown, never as zero.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolSessionFileChange {
    pub path: RuntimeString,
    pub kind: FileChangeKind,
    pub added: Option<u64>,
    pub removed: Option<u64>,
    /// Exact cumulative turn diff, absent for historical or unknown baselines.
    pub turn_diff: Option<RuntimeString>,
}

/// Authoritative completion of an explicit `shutdown` submission.
///
/// The server emits this only after core cleanup has settled and every queued
/// unsolicited lifecycle event has been written. The event carrying this value
/// is the final protocol message before the output transport is flushed and
/// closed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShutdownCompletion {
    /// True only when both core cleanup and the pre-ACK output flush succeeded.
    pub ok: bool,
    /// Whether all output produced before the shutdown ACK was flushed.
    pub flush_completed: bool,
    /// Whether the authoritative core shutdown completed without error.
    pub cleanup_completed: bool,
    /// Stable classification for a cleanup deadline failure.
    pub timed_out: bool,
    /// Bounded human-readable cleanup or transport failure detail.
    pub error: Option<RuntimeString>,
}

/// Structured non-secret metadata for an op error.
///
/// Known variants remain flat on the wire: the original input-validation
/// object is encoded byte-for-byte as before, while maintenance query failures
/// use their own stable `code`. Unknown future detail objects are retained so an
/// older client can still decode and forward the enclosing protocol event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolErrorDetail {
    InputValidation(InputValidationErrorDetail),
    MemoryMaintenance(MemoryMaintenanceQueryErrorDetail),
    Unknown(RuntimeJson),
}

impl From<InputValidationErrorDetail> for ProtocolErrorDetail {
    fn from(value: InputValidationErrorDetail) -> Self {
        Self::InputValidation(value)
    }
}

impl From<MemoryMaintenanceQueryErrorDetail> for ProtocolErrorDetail {
    fn from(value: MemoryMaintenanceQueryErrorDetail) -> Self {
        Self::MemoryMaintenance(value)
    }
}

impl RuntimeEncode for ProtocolErrorDetail {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        match self {
            Self::InputValidation(detail) => detail.encode_runtime(out),
            Self::MemoryMaintenance(detail) => detail.encode_runtime(out),
            Self::Unknown(value) => value.encode_runtime(out),
        }
    }
}

impl RuntimeDecode for ProtocolErrorDetail {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        let Some(code) = value.as_object().and_then(|object| object.get("code")) else {
            return Ok(Self::Unknown(value.clone()));
        };
        if InputValidationErrorCode::decode_runtime(code).is_ok() {
            return InputValidationErrorDetail::decode_runtime(value).map(Self::InputValidation);
        }
        if MemoryMaintenanceQueryErrorCode::decode_runtime(code).is_ok() {
            return match MemoryMaintenanceQueryErrorDetail::decode_runtime(value) {
                Ok(detail) => Ok(Self::MemoryMaintenance(detail)),
                // `runKind` is an optional extensible dimension. A newer
                // endpoint may add a lifecycle kind without adding a new error
                // code; retain the complete object instead of rejecting the
                // whole protocol frame and stranding its correlated waiter.
                Err(error)
                    if matches!(error.kind(), RuntimeCodecErrorKind::UnknownEnumVariant)
                        && value
                            .as_object()
                            .and_then(|object| object.get("runKind"))
                            .is_some_and(|run_kind| matches!(run_kind, RuntimeJson::String(_))) =>
                {
                    Ok(Self::Unknown(value.clone()))
                }
                Err(error) => Err(error),
            };
        }
        Ok(Self::Unknown(value.clone()))
    }
}

/// An outbound message, always tagged with the `submission_id` of the op it
/// answers (or `""` for unsolicited notifications). Exactly one payload field is
/// present per message; discriminate by which one.
#[derive(Debug, Default)]
pub struct ProtocolEvent {
    pub planning: Option<klaus_agent_core::planning::types::PlanningSnapshot>,
    pub document: Option<RuntimeJson>,
    pub implementation_input: Option<RuntimeString>,
    pub feedback_input: Option<RuntimeString>,
    pub implementation_id: Option<RuntimeString>,
    pub accepted: Option<bool>,
    pub turn_interrupt: Option<ProtocolTurnInterruptResult>,
    pub goal_continuation_decision: Option<ProtocolGoalContinuation>,
    /// Captured once before the actual turn's first terminal frame is published.
    pub terminal_notification_disposition: Option<ProtocolTerminalNotificationDisposition>,
    pub submission_id: RuntimeString,
    /// Reply to `init_session`.
    pub initialized: Option<InitSessionResult>,
    /// A 1:1 projection of a core engine event (the main streamed output).
    pub event: Option<EngineEvent>,
    /// Captured actual-turn provenance; present on turn.started envelopes.
    pub goal_continuation: Option<bool>,
    pub input_origin: Option<RuntimeString>,
    pub goal_id: Option<RuntimeString>,
    /// Ownership eligibility, independent of whether the feature is enabled.
    pub goal_pursuit_eligible: Option<bool>,
    pub goal_pursuit_eligibility_notice: Option<RuntimeString>,
    /// Unsolicited durable visible objective; it is bound to a real turn later.
    pub goal_submission: Option<ProtocolGoalSubmission>,
    /// Actual core turn id -> durable goal and input provenance.
    pub turn_goal_metadata:
        Option<BTreeMap<RuntimeString, klaus_agent_core::engine::session::TurnGoalMetadata>>,
    /// An ask_user request the host answers with a `resolve_elicitation` op.
    pub elicitation: Option<Elicitation>,
    /// A structured MCP server request answered with `resolve_mcp_elicitation`.
    pub mcp_elicitation: Option<McpElicitationEvent>,
    /// Ack for a successful MCP elicitation resolution.
    pub mcp_elicitation_resolved: Option<McpElicitationResolvedWire>,
    /// An MCP OAuth URL the host answers with `resolve_mcp_authorization`.
    pub mcp_authorization: Option<McpAuthorizationRequestWire>,
    /// Ack for `resolve_mcp_authorization`; never contains the submitted code.
    pub mcp_authorization_resolved: Option<McpAuthorizationResolvedWire>,
    /// Reply to `get_mcp_authorization_status`.
    pub mcp_authorization_status: Option<McpAuthorizationStatusWire>,
    /// Reply to `revoke_mcp_authorization`.
    pub mcp_authorization_revoked: Option<McpAuthorizationRevokedWire>,
    /// Authoritative post-operation MCP runtime state.
    pub mcp_runtime: Option<McpRuntimeResult>,
    /// One MCP server's lifecycle transition, pushed as it happens. Unsolicited:
    /// it answers no op, so `submission_id` is whatever turn was in flight (or
    /// empty). The host uses it to refresh a status view instead of re-polling the
    /// whole connection set.
    pub mcp_server_status: Option<McpServerStatusUpdate>,
    /// Reply to `call_mcp_tool`.
    pub mcp_tool_call: Option<McpToolCallResultWire>,
    /// Reply to `read_mcp_resource`.
    pub mcp_resource_read: Option<McpResourceReadResultWire>,
    /// Reply to `list_mcp_prompts`.
    pub mcp_prompts: Option<McpPromptsResultWire>,
    /// Reply to `get_mcp_prompt`.
    pub mcp_prompt: Option<McpPromptResultWire>,
    /// Effective capability resolution owned by the active Session.
    pub runtime_capabilities: Option<RuntimeCapabilityResolution>,
    /// Effective extension graph owned by the active Session.
    pub extension_runtime: Option<Box<ExtensionRuntimeSnapshot>>,
    /// Persisted installed-plugin ledger, independent of the live runtime graph.
    pub installed_plugins: Option<Box<InstalledPlugins>>,
    /// Effective plugin graph and load diagnostics captured by this endpoint.
    pub plugin_runtime: Option<Box<PluginRuntimeSnapshot>>,
    /// Persisted plugin mutation with explicit endpoint-restart activation scope.
    pub plugin_mutation: Option<Box<InstalledPluginMutationResult>>,
    /// Reply to `list_plugin_catalog`: the packages readable under the supplied
    /// marketplace roots, each already joined with the ledger's install/enable
    /// state.
    pub plugin_catalog: Option<Box<ProtocolPluginCatalog>>,
    /// A host-provided dynamic tool invocation. The host must answer it with
    /// `resolve_dynamic_tool_call` using the supplied call id.
    pub dynamic_tool_call: Option<ProtocolDynamicToolCall>,
    /// Reply to `resolve_dynamic_tool_call` after the active call was released.
    pub dynamic_tool_resolved: Option<ProtocolDynamicToolResolved>,
    /// Reply to `update_session_config`.
    pub session_config: Option<ProtocolSessionConfigUpdateResult>,
    pub runtime_policy_updated: Option<bool>,
    /// Confirmation that a replacement skill generation has been staged.
    pub skills_updated: Option<bool>,
    /// Confirmation of a `rewind` op (emitted AFTER the engine rewound).
    pub rewound: Option<RewindOutcome>,
    /// Reply to `rewind_preview`: what a rewind would do, having done nothing.
    pub rewind_preview: Option<RewindPreview>,
    /// Final outcome of a `run`.
    pub result: Option<RunResult>,
    /// Reply to a checked `steer`, emitted after actual continuation delivery.
    pub steered: Option<SteerResult>,
    pub pending_resume: Option<PendingResumeResult>,
    /// Reply to `get_history`.
    pub history: Option<Vec<Message>>,
    /// Latest lifecycle state of each automatic approval review.
    pub approval_reviews:
        Option<Vec<klaus_agent_core::types::approval_review::ApprovalReviewEvent>>,
    /// Most recent denied approval reviews, newest first.
    pub approval_review_denials:
        Option<Vec<klaus_agent_core::types::approval_review::ApprovalReviewEvent>>,
    pub approval_review_denial_approved: Option<bool>,
    /// Reply to `get_history`: message uuid -> turn id, so the renderer can
    /// group messages into turns (the turn id matches `turn/started`).
    pub history_turn_ids: Option<BTreeMap<RuntimeString, RuntimeString>>,
    /// Reply to `get_history`: turn id -> final assistant message uuid.
    pub turn_final_message_ids: Option<BTreeMap<RuntimeString, RuntimeString>>,
    /// Reply to `get_history`: turn id -> absolute start time in epoch millis.
    /// Older persisted threads omit this map.
    pub turn_started_at_ms: Option<BTreeMap<RuntimeString, i64>>,
    /// Reply to `get_history`: turn id -> absolute terminal time in epoch millis.
    /// Older persisted threads omit this map.
    pub turn_finished_at_ms: Option<BTreeMap<RuntimeString, i64>>,
    /// Reply to `get_history`: turn id -> terminal status
    /// (`completed` | `interrupted` | `failed`), for the renderer's fold
    /// decision.
    pub turn_statuses: Option<BTreeMap<RuntimeString, RuntimeString>>,
    /// Reply to `get_history`: turn id -> terminal elapsed time in milliseconds.
    /// Older persisted threads omit this map; consumers must tolerate absence.
    pub turn_durations: Option<BTreeMap<RuntimeString, u64>>,
    /// Reply to `get_history`: turn id -> failure message for `failed` turns,
    /// so a resumed transcript can render the reason, not just the status.
    pub turn_errors: Option<BTreeMap<RuntimeString, RuntimeString>>,
    /// Reply to `get_history`: turn id -> normalized provider error kind for
    /// `failed` turns whose failure originated at the provider, so a resumed
    /// transcript can localize the failure card. Absent for older engines.
    pub turn_error_kinds: Option<BTreeMap<RuntimeString, RuntimeString>>,
    /// Optional complete EngineError diagnostics for failed history turns.
    /// Request-local provider media origins are never carried across this boundary.
    pub turn_error_details: Option<BTreeMap<RuntimeString, RuntimeJson>>,
    /// Reply to `get_history`: turn id -> the tool_use ids whose calls the turn's
    /// abort killed. Both a stopped call and a genuine failure carry `is_error`,
    /// so this is what keeps a reopened transcript from painting the user's stop
    /// as a red tool failure. Absent for older engines.
    pub turn_cancelled_tools: Option<BTreeMap<RuntimeString, Vec<RuntimeString>>>,
    /// Reply to `get_file_changes`: one group per turn, oldest first, each
    /// listing the files that turn changed in first-touched order.
    pub file_changes: Option<Vec<ProtocolSessionTurnFileChanges>>,
    /// Reply to `get_cost`.
    pub cost: Option<CostSnapshot>,
    /// Reply to `context_snapshot`.
    pub context: Option<SessionContextSnapshot>,
    /// Reply to `get_subagents`.
    pub subagents: Option<Vec<BackgroundAgentSummary>>,
    /// Reply to `get_subagent_lifecycle` with the core-owned complete snapshot.
    pub subagent_lifecycle: Option<SubagentSnapshot>,
    /// Acknowledges targeted child input without waiting for its run to finish.
    pub subagent_message: Option<RuntimeJson>,
    /// A child conversation event independent of the parent tool or turn lifetime.
    pub subagent_event: Option<RuntimeJson>,
    /// An actual notice received by this session, separate from child progress.
    pub agent_notification:
        Option<klaus_agent_core::schemas::agent_notification::AgentNotification>,
    /// Durable notification receipts returned with session/history.
    pub agent_notifications:
        Option<Vec<klaus_agent_core::schemas::agent_notification::AgentNotification>>,
    /// Reply to `get_background_processes` with the authoritative session snapshot.
    pub background_processes: Option<BackgroundProcessSnapshot>,
    /// Reply to `read_background_process_output`; host reads never advance the model cursor.
    pub background_process_output: Option<BackgroundProcessOutput>,
    /// Reply to `stop_background_process` with the authoritative post-request record.
    pub background_process_stop: Option<BackgroundProcessStopResult>,
    /// Unsolicited cc-compatible background-process lifecycle/output event.
    pub background_process_event: Option<BackgroundProcessEvent>,
    /// Unsolicited automatic Memory/Dream lifecycle transition (`submissionId == ""`).
    pub memory_maintenance_event: Option<MemoryMaintenanceEvent>,
    /// Unsolicited report that an extension point did not run: a hook command
    /// failed, a plugin failed to load, a skill was skipped. Unsolicited because
    /// these degrade silently by design — the turn survives, so nothing else in
    /// the stream would ever mention them.
    pub extension_failure: Option<ExtensionFailure>,
    /// Reply to `get_memory_maintenance` with the reconnect-safe audit snapshot.
    pub memory_maintenance: Option<MemoryMaintenanceSnapshot>,
    /// Reply to `load_subagent_transcript`.
    pub subagent_transcript: Option<SubAgentTranscript>,
    /// Reply to `get_queued`.
    pub queued: Option<Vec<QueuedMessage>>,
    /// Reply to `take_pending`.
    pub pending: Option<Vec<QueuedMessage>>,
    /// Authoritative queue snapshot. Unsolicited revisions use an empty
    /// `submissionId`; correlated replies answer `get_queue_snapshot`.
    pub queue_snapshot: Option<QueueSnapshot>,
    /// Correlated result for edit/remove/reorder, including the authoritative
    /// post-operation snapshot and terminal tombstone when applicable.
    pub queue_mutation: Option<QueueMutationResult>,
    /// Correlated result of an init or maintenance Setup invocation.
    pub setup: Option<SetupRunResult>,
    /// Reconnect-safe persisted Setup lifecycle state.
    pub setup_lifecycle: Option<SetupLifecycleSnapshot>,
    /// Correlated authoritative TodoV2 page.
    pub task_list: Option<TaskListSnapshot>,
    /// Ordered unsolicited full-state TodoV2 invalidation.
    pub task_list_change: Option<TaskListChange>,
    /// Unsolicited transition or correlated resolve/terminal result for one
    /// Agent Memory snapshot request.
    pub agent_memory_snapshot_decision: Option<AgentMemorySnapshotDecisionAudit>,
    /// Reconnect-safe query of every Agent Memory snapshot decision audit.
    pub agent_memory_snapshot_decisions: Option<Vec<AgentMemorySnapshotDecisionAudit>>,
    /// Correlated query result for one request, including missing state.
    pub agent_memory_snapshot_decision_query: Option<AgentMemorySnapshotDecisionQueryResult>,
    /// Reply to `compact_session`.
    pub compact: Option<CompactSessionResult>,
    /// Lifecycle progress for a manual compaction, correlated to the op.
    pub compact_progress: Option<CompactProgress>,
    /// Reply to `side_question`.
    pub side_question: Option<SideQuestionResult>,
    /// Reply to `review_session`.
    pub review: Option<ReviewSessionResult>,
    /// Reply to `dream_now`.
    pub dream: Option<DreamNowOutcome>,
    /// Terminal status for a successfully-started `dream_now` task.
    pub dream_completed: Option<DreamNowCompletion>,
    /// Reply to `generate_title`.
    pub generated_title: Option<GenerateTitleOpResult>,
    /// Ephemeral composer result, never a main conversation event.
    pub prediction: Option<PredictionResult>,
    pub prediction_context: Option<klaus_agent_core::engine::session::PredictionContext>,
    /// Reply to `list_sessions`.
    pub sessions: Option<ThreadPage>,
    /// Reply to `resume_session` / `fork_session`.
    pub session: Option<BoundSession>,
    /// Reply to `archive_session`.
    pub archived: Option<ArchiveSessionResult>,
    /// Reply to `unarchive_session` with a typed, idempotent lifecycle result.
    pub unarchived: Option<UnarchiveSessionResult>,
    /// Reply to `delete_session`.
    pub deleted: Option<DeleteSessionResult>,
    /// Reply to `rename_thread`.
    pub renamed: Option<RenameThreadResult>,
    /// Reply to `update_thread_host_metadata`.
    pub host_metadata_updated: Option<UpdateThreadHostMetadataResult>,
    /// Reply to `update_title`.
    pub title: Option<UpdateTitleResult>,
    /// Final reply to an explicit `shutdown` submission. This is emitted after
    /// cleanup and unsolicited-event draining, immediately before transport EOF.
    pub shutdown_completed: Option<Box<ShutdownCompletion>>,
    /// The thread's goal. Correlated for `get_goal` / `set_goal` / `clear_goal`,
    /// and pushed unsolicited (`submissionId == ""`) whenever the engine itself
    /// changed it: the model creating or completing one, usage reaching the
    /// budget, or a failed turn blocking it. A host that only read replies
    /// would show a stale goal for the whole of a long run.
    pub thread_goal: Option<ProtocolThreadGoal>,
    pub goal_completions: Option<Vec<klaus_agent_core::goal::retained_context::GoalCompletion>>,
    /// `clear_goal` acknowledgement: whether a goal was actually removed.
    pub goal_cleared: Option<bool>,
    /// Monotonically increasing durable goal revision, including clear tombstones.
    pub goal_revision: Option<i64>,
    /// Engine admission disposition; callers never submit continuation prompts.
    pub goal_admission: Option<RuntimeString>,
    /// The op named by `submission_id` FAILED. Carries the message.
    pub error: Option<RuntimeString>,
    /// Stable machine-readable metadata for errors that have a typed contract.
    pub error_detail: Option<ProtocolErrorDetail>,
}

/// Validate a raw runtime JSON value as an [`Op`] (tolerant parse for incoming
/// submissions). Mirrors the wire schema's discriminated-union validation:
/// unknown op types, bad permission modes, and non-positive-integer `numTurns`
/// are all rejected here.
pub fn parse_op(value: RuntimeJson) -> Result<Op, RuntimeCodecError> {
    Op::decode_runtime(&value)
}

/// Exact runtime adapter for a strictly-positive integer (`numTurns >= 1`).
fn de_positive_int(value: &RuntimeJson) -> Result<u32, RuntimeCodecError> {
    let number = value
        .as_number()
        .ok_or_else(|| RuntimeCodecError::new(RuntimeCodecErrorKind::ExpectedNumber))?;
    let number = number.as_f64();
    if !number.is_finite() || number.fract() != 0.0 || !(1.0..=u32::MAX as f64).contains(&number) {
        return Err(RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(
            RuntimeString::from("numTurns must be a positive integer"),
        )));
    }
    Ok(number as u32)
}

/// The exact JSON form of a `run` or `steer` op's `input`.
///
/// Public because the app-server facade carries this field raw — `turn/start`
/// and `session/steer` hand it straight through to the op — so a host building
/// those params needs the encoder that the op itself uses. `Input` is
/// `string | UserContentPart[]` rather than a struct with a derived codec, so
/// without this every host would write the shape out again and be free to drift
/// from what the engine decodes.
pub fn encode_input(input: &Input) -> Result<RuntimeJson, RuntimeCodecError> {
    wire_input::serialize(input)
}

/// Exact runtime adapter for core's `Input` (`string | UserContentPart[]`).
mod wire_input {
    use super::{
        Input, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeJson, RuntimeObjectEncoder,
        RuntimeString, UserContentPart,
    };
    #[derive(Debug)]
    enum WirePart {
        Text {
            text: RuntimeString,
        },
        Image {
            base64: RuntimeString,
            mime_type: RuntimeString,
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

    #[derive(Debug)]
    enum WireInput {
        Text(RuntimeString),
        Parts(Vec<WirePart>),
    }

    pub(super) fn serialize(input: &Input) -> Result<RuntimeJson, RuntimeCodecError> {
        let wire = match input {
            Input::Text(text) => WireInput::Text(text.clone()),
            Input::Parts(parts) => WireInput::Parts(
                parts
                    .iter()
                    .map(|p| match p {
                        UserContentPart::Text { text } => WirePart::Text { text: text.clone() },
                        UserContentPart::Image {
                            base64,
                            mime_type,
                            source_path,
                        } => WirePart::Image {
                            base64: base64.clone(),
                            mime_type: mime_type.clone(),
                            source_path: source_path.clone(),
                        },
                        UserContentPart::Document {
                            base64,
                            mime_type,
                            filename,
                        } => WirePart::Document {
                            base64: base64.clone(),
                            mime_type: mime_type.clone(),
                            filename: filename.clone(),
                        },
                        UserContentPart::Media {
                            base64,
                            mime_type,
                            filename,
                        } => WirePart::Media {
                            base64: base64.clone(),
                            mime_type: mime_type.clone(),
                            filename: filename.clone(),
                        },
                    })
                    .collect(),
            ),
        };
        wire.encode()
    }

    pub(super) fn deserialize(value: &RuntimeJson) -> Result<Input, RuntimeCodecError> {
        let wire = WireInput::decode(value)?;
        Ok(match wire {
            WireInput::Text(text) => Input::Text(text),
            WireInput::Parts(parts) => Input::Parts(
                parts
                    .into_iter()
                    .map(|p| match p {
                        WirePart::Text { text } => UserContentPart::Text { text },
                        WirePart::Image {
                            base64,
                            mime_type,
                            source_path,
                        } => UserContentPart::Image {
                            base64,
                            mime_type,
                            source_path,
                        },
                        WirePart::Document {
                            base64,
                            mime_type,
                            filename,
                        } => UserContentPart::Document {
                            base64,
                            mime_type,
                            filename,
                        },
                        WirePart::Media {
                            base64,
                            mime_type,
                            filename,
                        } => UserContentPart::Media {
                            base64,
                            mime_type,
                            filename,
                        },
                    })
                    .collect(),
            ),
        })
    }

    impl WireInput {
        fn encode(&self) -> Result<RuntimeJson, RuntimeCodecError> {
            match self {
                Self::Text(text) => Ok(RuntimeJson::String(text.clone())),
                Self::Parts(parts) => Ok(RuntimeJson::Array(
                    parts
                        .iter()
                        .enumerate()
                        .map(|(index, part)| {
                            part.encode()
                                .map_err(|error| error.prepend(index.to_string()))
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                )),
            }
        }

        fn decode(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
            match value {
                RuntimeJson::String(text) => Ok(Self::Text(text.clone())),
                RuntimeJson::Array(parts) => Ok(Self::Parts(
                    parts
                        .iter()
                        .enumerate()
                        .map(|(index, value)| {
                            WirePart::decode(value)
                                .map_err(|error| error.prepend(index.to_string()))
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                )),
                _ => Err(RuntimeCodecError::new(
                    RuntimeCodecErrorKind::NoMatchingUntaggedVariant,
                )),
            }
        }
    }

    impl WirePart {
        fn encode(&self) -> Result<RuntimeJson, RuntimeCodecError> {
            let mut object = RuntimeObjectEncoder::new();
            match self {
                Self::Text { text } => {
                    object.field("type", &RuntimeString::from("text"))?;
                    object.field("text", text)?;
                }
                Self::Image {
                    base64,
                    mime_type,
                    source_path,
                } => {
                    object.field("type", &RuntimeString::from("image"))?;
                    object.field("base64", base64)?;
                    object.field("mimeType", mime_type)?;
                    if let Some(source_path) = source_path {
                        object.field("sourcePath", source_path)?;
                    }
                }
                Self::Document {
                    base64,
                    mime_type,
                    filename,
                } => {
                    object.field("type", &RuntimeString::from("document"))?;
                    object.field("base64", base64)?;
                    object.field("mimeType", mime_type)?;
                    if let Some(filename) = filename {
                        object.field("filename", filename)?;
                    }
                }
                Self::Media {
                    base64,
                    mime_type,
                    filename,
                } => {
                    object.field("type", &RuntimeString::from("media"))?;
                    object.field("base64", base64)?;
                    object.field("mimeType", mime_type)?;
                    if let Some(filename) = filename {
                        object.field("filename", filename)?;
                    }
                }
            }
            Ok(RuntimeJson::Object(object.finish()))
        }

        fn decode(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
            klaus_agent_core::runtime_values::runtime_codec::decode_internal(
                value,
                false,
                "type",
                &[],
                &[
                    klaus_agent_core::runtime_values::runtime_codec::RuntimeEnumVariant::new(
                        "text",
                        &[],
                    ),
                    klaus_agent_core::runtime_values::runtime_codec::RuntimeEnumVariant::new(
                        "image",
                        &[],
                    ),
                    klaus_agent_core::runtime_values::runtime_codec::RuntimeEnumVariant::new(
                        "document",
                        &[],
                    ),
                    klaus_agent_core::runtime_values::runtime_codec::RuntimeEnumVariant::new(
                        "media",
                        &[],
                    ),
                ],
                |variant, object| match variant {
                    0 => Ok(Self::Text {
                        text: object.required("text", &[])?,
                    }),
                    1 => Ok(Self::Image {
                        base64: object.required("base64", &[])?,
                        mime_type: object.required("mimeType", &[])?,
                        source_path: object.optional("sourcePath", &[])?,
                    }),
                    2 => Ok(Self::Document {
                        base64: object.required("base64", &[])?,
                        mime_type: object.required("mimeType", &[])?,
                        filename: object.optional("filename", &[])?,
                    }),
                    3 => Ok(Self::Media {
                        base64: object.required("base64", &[])?,
                        mime_type: object.required("mimeType", &[])?,
                        filename: object.optional("filename", &[])?,
                    }),
                    _ => unreachable!(),
                },
            )
        }
    }
}

mod exact_runtime_codec;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn runtime_value(value: serde_json::Value) -> RuntimeJson {
        let text = serde_json::to_string(&value).unwrap();
        klaus_agent_core::runtime_values::runtime_json::parse(&RuntimeString::from_utf8(&text))
            .unwrap()
    }

    fn runtime_encoded_value<T: RuntimeEncode + ?Sized>(
        value: &T,
    ) -> Result<serde_json::Value, String> {
        let text = runtime_codec::encode_text(value).map_err(|error| error.to_string())?;
        let text = text.try_to_utf8().map_err(|error| error.to_string())?;
        serde_json::from_str(&text).map_err(|error| error.to_string())
    }

    fn runtime_decoded_value<T: RuntimeDecode>(value: serde_json::Value) -> Result<T, String> {
        T::decode_runtime(&runtime_value(value)).map_err(|error| error.to_string())
    }

    fn parse_test_op(value: serde_json::Value) -> Result<Op, RuntimeCodecError> {
        parse_op(runtime_value(value))
    }

    #[test]
    fn selected_compaction_codec_preserves_boundary_identity_and_rejects_bad_directions() {
        for direction in ["from", "upTo"] {
            let raw = RuntimeString::from(format!(
                r#"{{"id":"selected","op":{{"type":"compact_session","selection":{{"messageId":"id\ud800","direction":"{direction}"}}}}}}"#
            ));
            let value = klaus_agent_core::runtime_values::runtime_json::parse(&raw).unwrap();
            let op = parse_op(value.clone()).unwrap();
            let OpKind::CompactSession {
                selection: Some(selection),
                ..
            } = &op.op
            else {
                panic!("selected boundary must decode");
            };
            assert_eq!(selection.message_id.as_utf16(), &[105, 100, 0xd800]);
            assert_eq!(runtime_codec::encode_value(&op).unwrap(), value);
        }
        for selection in [
            json!({"messageId":"boundary", "direction":"up_to"}),
            json!({"messageId":"boundary", "direction":"from", "index":2}),
            json!({"direction":"from"}),
        ] {
            assert!(parse_test_op(json!({"id":"bad", "op":{
                "type":"compact_session", "selection":selection
            }}))
            .is_err());
        }
    }

    #[test]
    fn readonly_side_question_uses_the_exact_input_codec() {
        for question in [
            json!("legacy question"),
            json!([
                {"type":"text","text":"parsed document text"},
                {"type":"image","mimeType":"image/png","base64":"iVBORw0KGgo="},
                {"type":"document","mimeType":"application/pdf","base64":"JVBERi0xLjQ=","filename":"report.pdf"}
            ]),
        ] {
            let value = json!({"id":"side","op":{"type":"side_question","question":question}});
            let op = parse_test_op(value.clone()).unwrap();
            assert_eq!(runtime_encoded_value(&op).unwrap(), value);
        }
        let raw = RuntimeString::from(
            r#"{"id":"side","op":{"type":"side_question","question":[{"type":"text","text":"\ud800"}]}}"#,
        );
        let op =
            parse_op(klaus_agent_core::runtime_values::runtime_json::parse(&raw).unwrap()).unwrap();
        let OpKind::SideQuestion {
            question: Input::Parts(parts),
        } = &op.op
        else {
            panic!("parts");
        };
        assert_eq!(
            parts,
            &vec![UserContentPart::Text {
                text: RuntimeString::from_utf16([0xd800])
            }]
        );
        let encoded = runtime_codec::encode_text(&op).unwrap();
        assert!(encoded.contains("\\ud800"));
        for question in [
            json!(null),
            json!(17),
            json!({ "path": "/tmp/report.pdf" }),
            json!([{ "type":"file", "path":"/tmp/report.pdf" }]),
            json!([{ "type":"image", "path":"/tmp/image.png", "mimeType":"image/png" }]),
            json!([{ "type":"document", "base64": false, "mimeType":"application/pdf" }]),
            json!([{ "type":"text", "text":{} }]),
        ] {
            assert!(parse_test_op(
                json!({"id":"bad","op":{"type":"side_question","question":question}})
            )
            .is_err());
        }
    }

    #[test]
    fn root_agent_definition_selector_round_trips_and_rejects_wrong_types() {
        let expected = json!({"agentDefinitionName": "reviewer"});
        let init: ProtocolSessionInit = runtime_decoded_value(expected.clone()).unwrap();
        assert_eq!(init.agent_definition_name, Some("reviewer".into()));
        assert_eq!(runtime_encoded_value(&init).unwrap(), expected);
        for invalid in [json!(7), json!(true), json!([]), json!({})] {
            assert!(runtime_decoded_value::<ProtocolSessionInit>(
                json!({"agentDefinitionName": invalid})
            )
            .is_err());
        }
        let ordinary: ProtocolSessionInit = runtime_decoded_value(json!({})).unwrap();
        assert!(ordinary.agent_definition_name.is_none());
        assert_eq!(runtime_encoded_value(&ordinary).unwrap(), json!({}));
    }

    #[test]
    fn advisor_target_runtime_codec_is_independent_of_tier_targets() {
        for target in [
            json!({"type":"source","source":"custom","modelId":"advisor-entry"}),
            json!({"type":"resolved","source":"local","modelId":"advisor-file",
                "provider":{"type":"openai","model":"advisor-model"},
                "auth":{"type":"api_key","key":"fixture-advisor-key"},
                "capabilities":{"contextWindow":8192,"maxOutputTokens":1024}}),
            json!({"type":"unavailable","source":"custom","modelId":"deleted",
                "code":"MODEL_REMOVED","message":"The selected model was deleted."}),
        ] {
            let expected = json!({"advisorTarget":target,"modelTierTargets":{"high":null}});
            let init: ProtocolSessionInit = runtime_decoded_value(expected.clone()).unwrap();
            assert!(init.advisor_target.is_some());
            assert_eq!(runtime_encoded_value(&init).unwrap(), expected);
        }
        for expected in [json!({}), json!({"advisorTarget":null})] {
            let init: ProtocolSessionInit = runtime_decoded_value(expected).unwrap();
            assert!(init.advisor_target.is_none());
            assert_eq!(runtime_encoded_value(&init).unwrap(), json!({}));
        }
    }

    #[test]
    fn init_session_variant_keeps_owned_init_and_wire_shape() {
        let op = Op {
            id: "init-owned".into(),
            op: OpKind::InitSession {
                init: ProtocolSessionInit {
                    cwd: Some("/workspace".into()),
                    base_instructions: Some("base prompt".into()),
                    ..Default::default()
                },
            },
        };
        let expected = json!({
            "id": "init-owned",
            "op": {
                "type": "init_session",
                "init": {
                    "cwd": "/workspace",
                    "baseInstructions": "base prompt"
                }
            }
        });

        let encoded = runtime_encoded_value(&op).unwrap();
        assert_eq!(encoded, expected);
        let decoded: Op = runtime_decoded_value(encoded).unwrap();
        let OpKind::InitSession { init } = decoded.op else {
            panic!("expected init_session");
        };
        let init: ProtocolSessionInit = init;
        assert!(init.cwd.as_ref().is_some_and(|cwd| cwd == "/workspace"));

        let reconstructed = Op {
            id: decoded.id,
            op: OpKind::InitSession { init },
        };
        assert_eq!(runtime_encoded_value(&reconstructed).unwrap(), expected);
    }

    #[test]
    fn init_session_dream_thresholds_round_trip_inside_generic_config() {
        let expected = json!({
            "id": "init-dream",
            "op": {
                "type": "init_session",
                "init": {
                    "config": {
                        "agentMemory": true,
                        "autoDream": true,
                        "dream": { "minHours": 0.5, "minSessions": 9 }
                    }
                }
            }
        });

        let op = parse_test_op(expected.clone()).expect("generic session config should decode");
        assert_eq!(runtime_encoded_value(&op).unwrap(), expected);
        let OpKind::InitSession { init } = op.op else {
            panic!("expected init_session");
        };
        assert_eq!(
            runtime_encoded_value(init.config.as_ref().unwrap()).unwrap(),
            json!({
                "agentMemory": true,
                "autoDream": true,
                "dream": { "minHours": 0.5, "minSessions": 9 }
            })
        );
    }

    #[test]
    fn memory_maintenance_query_and_event_fields_use_exact_wire_names() {
        let op_json = json!({
            "id": "maintenance-query",
            "op": { "type": "get_memory_maintenance" }
        });
        let op = parse_test_op(op_json.clone()).expect("maintenance query op");
        assert!(matches!(op.op, OpKind::GetMemoryMaintenance));
        assert_eq!(runtime_encoded_value(&op).unwrap(), op_json);

        let dream = AutoDreamRun {
            dream_run_id: "dream-1".into(),
            session_id: "session-1".into(),
            trigger: MemoryMaintenanceTrigger::Auto,
            status: MemoryMaintenanceStatus::Completed,
            started_at: 30,
            ended_at: Some(30),
            reason: None,
            error: None,
            last_consolidated_at: Some(20),
            sessions_reviewing: 5,
            files_touched: vec!["/memory/MEMORY.md".into()],
            turns: vec![MemoryMaintenanceTurn {
                text: "finished\n<dream-summary>the dream sharpened: durable constraints emerged</dream-summary>".into(),
                tool_use_count: 1,
            }],
            turn_count: 2,
            summary: Some("the dream sharpened: durable constraints emerged".into()),
        };
        let live_event = ProtocolEvent {
            submission_id: RuntimeString::default(),
            memory_maintenance_event: Some(MemoryMaintenanceEvent::AutoDream {
                run: dream.clone(),
            }),
            ..Default::default()
        };
        let expected_live = json!({
            "submissionId": "",
            "memoryMaintenanceEvent": {
                "kind": "auto_dream",
                "run": {
                    "dreamRunId": "dream-1",
                    "sessionId": "session-1",
                    "trigger": "auto",
                    "status": "completed",
                    "startedAt": 30,
                    "endedAt": 30,
                    "lastConsolidatedAt": 20,
                    "sessionsReviewing": 5,
                    "filesTouched": ["/memory/MEMORY.md"],
                    "turns": [{
                        "text": "finished\n<dream-summary>the dream sharpened: durable constraints emerged</dream-summary>",
                        "toolUseCount": 1
                    }],
                    "turnCount": 2,
                    "summary": "the dream sharpened: durable constraints emerged"
                }
            }
        });
        assert_eq!(runtime_encoded_value(&live_event).unwrap(), expected_live);
        assert_eq!(
            runtime_decoded_value::<ProtocolEvent>(expected_live)
                .unwrap()
                .memory_maintenance_event,
            live_event.memory_maintenance_event
        );

        let memory = AutoMemoryRun {
            run_id: "memory-1".into(),
            session_id: "session-1".into(),
            trigger: MemoryMaintenanceTrigger::Auto,
            scope: AgentMemoryScope::Project,
            target_dir: "/memory".into(),
            status: MemoryMaintenanceStatus::Completed,
            started_at: 10,
            ended_at: Some(20),
            reason: None,
            error: None,
            message_count: 2,
            cursor_message_uuid: Some("cursor-1".into()),
            files_touched: vec!["/memory/MEMORY.md".into()],
            turn_count: Some(2),
        };
        let snapshot = MemoryMaintenanceSnapshot {
            auto_memory_runs: vec![memory],
            auto_dream_runs: vec![dream],
            auto_memory_cursor_message_uuid: Some("cursor-1".into()),
        };
        let event = ProtocolEvent {
            submission_id: "maintenance-query".into(),
            memory_maintenance: Some(snapshot.clone()),
            ..Default::default()
        };
        let expected = json!({
            "submissionId": "maintenance-query",
            "memoryMaintenance": {
                "autoMemoryRuns": [{
                    "runId": "memory-1",
                    "sessionId": "session-1",
                    "trigger": "auto",
                    "scope": "project",
                    "targetDir": "/memory",
                    "status": "completed",
                    "startedAt": 10,
                    "endedAt": 20,
                    "messageCount": 2,
                    "cursorMessageUuid": "cursor-1",
                    "filesTouched": ["/memory/MEMORY.md"],
                    "turnCount": 2
                }],
                "autoDreamRuns": [{
                    "dreamRunId": "dream-1",
                    "sessionId": "session-1",
                    "trigger": "auto",
                    "status": "completed",
                    "startedAt": 30,
                    "endedAt": 30,
                    "lastConsolidatedAt": 20,
                    "sessionsReviewing": 5,
                    "filesTouched": ["/memory/MEMORY.md"],
                    "turns": [{
                        "text": "finished\n<dream-summary>the dream sharpened: durable constraints emerged</dream-summary>",
                        "toolUseCount": 1
                    }],
                    "turnCount": 2,
                    "summary": "the dream sharpened: durable constraints emerged"
                }],
                "autoMemoryCursorMessageUuid": "cursor-1"
            }
        });
        assert_eq!(runtime_encoded_value(&event).unwrap(), expected);
        let decoded: ProtocolEvent = runtime_decoded_value(expected).unwrap();
        assert_eq!(decoded.memory_maintenance, Some(snapshot));
    }

    #[test]
    fn run_op_input_round_trips_string_and_parts() {
        let v = json!({ "id": "a", "op": { "type": "run", "input": "hello" } });
        let op = parse_test_op(v.clone()).unwrap();
        match &op.op {
            OpKind::Run { input, options } => {
                assert_eq!(*input, Input::Text("hello".into()));
                assert!(options.is_none());
            }
            _ => panic!("expected run"),
        }
        assert_eq!(runtime_encoded_value(&op).unwrap(), v);

        let v2 = json!({
            "id": "b",
            "op": {
                "type": "run",
                "input": [
                    { "type": "text", "text": "hi" },
                    { "type": "image", "base64": "AAAA", "mimeType": "image/png" }
                ]
            }
        });
        let op2 = parse_test_op(v2.clone()).unwrap();
        match &op2.op {
            OpKind::Run { input, .. } => assert_eq!(
                *input,
                Input::Parts(vec![
                    UserContentPart::Text { text: "hi".into() },
                    UserContentPart::Image {
                        base64: "AAAA".into(),
                        mime_type: "image/png".into(),
                        source_path: None,
                    },
                ])
            ),
            _ => panic!("expected run"),
        }
        assert_eq!(runtime_encoded_value(&op2).unwrap(), v2);
    }

    #[test]
    fn run_op_input_round_trips_lone_surrogates_without_scalar_projection() {
        let source = RuntimeString::from(
            r#"{"id":"surrogate","op":{"type":"run","input":[{"type":"text","text":"\ud800"},{"type":"image","base64":"AAAA","mimeType":"image/\udc00"}]}}"#,
        );
        let value = klaus_agent_core::runtime_values::runtime_json::parse(&source).unwrap();
        let op = parse_op(value.clone()).unwrap();

        let OpKind::Run { input, .. } = &op.op else {
            panic!("expected run");
        };
        assert_eq!(
            input,
            &Input::Parts(vec![
                UserContentPart::Text {
                    text: RuntimeString::from_utf16([0xd800]),
                },
                UserContentPart::Image {
                    base64: "AAAA".into(),
                    mime_type: RuntimeString::from_utf16([
                        b'i' as u16,
                        b'm' as u16,
                        b'a' as u16,
                        b'g' as u16,
                        b'e' as u16,
                        b'/' as u16,
                        0xdc00,
                    ]),
                    source_path: None,
                },
            ])
        );
        assert_eq!(runtime_codec::encode_value(&op).unwrap(), value);
    }

    #[test]
    fn run_op_pdf_input_round_trips() {
        let value = json!({
            "id": "pdf-run",
            "op": {
                "type": "run",
                "input": [{
                    "type": "document",
                    "base64": "JVBERi0xLjcK",
                    "mimeType": "application/pdf",
                    "filename": "report.pdf"
                }]
            }
        });

        let op = parse_test_op(value.clone()).unwrap();
        match &op.op {
            OpKind::Run { input, .. } => assert_eq!(
                input,
                &Input::Parts(vec![UserContentPart::Document {
                    base64: "JVBERi0xLjcK".into(),
                    mime_type: "application/pdf".into(),
                    filename: Some("report.pdf".into()),
                }])
            ),
            _ => panic!("expected run"),
        }
        assert_eq!(runtime_encoded_value(&op).unwrap(), value);
    }

    #[test]
    fn run_op_native_media_input_round_trips_exact_mime_and_optional_filename() {
        let value = json!({
            "id": "native-media-run",
            "op": { "type": "run", "input": [
                { "type": "media", "base64": "UklGRg==", "mimeType": "audio/webm;codecs=opus" },
                { "type": "media", "base64": "AAAAHGZ0eXA=", "mimeType": "video/mp4", "filename": "clip.mp4" }
            ]}
        });
        let op = parse_test_op(value.clone()).unwrap();
        let OpKind::Run {
            input: Input::Parts(parts),
            ..
        } = &op.op
        else {
            panic!("expected media input");
        };
        assert!(
            matches!(&parts[0], UserContentPart::Media { mime_type, filename: None, .. } if mime_type == "audio/webm;codecs=opus")
        );
        assert!(
            matches!(&parts[1], UserContentPart::Media { filename: Some(name), .. } if name == "clip.mp4")
        );
        assert_eq!(runtime_encoded_value(&op).unwrap(), value);
    }

    #[test]
    fn protocol_input_error_detail_round_trips_without_document_payload() {
        let event = ProtocolEvent {
            submission_id: "pdf-run".into(),
            error: Some("model text-only-model does not support native PDF input".into()),
            error_detail: Some(
                InputValidationErrorDetail {
                    code: InputValidationErrorCode::UnsupportedInput,
                    feature: Some("native_pdf".into()),
                    provider: Some("openai".into()),
                    model: Some("text-only-model".into()),
                    limit_bytes: None,
                    limit_count: None,
                }
                .into(),
            ),
            ..Default::default()
        };

        let value = runtime_encoded_value(&event).unwrap();
        assert_eq!(
            value,
            json!({
                "submissionId": "pdf-run",
                "error": "model text-only-model does not support native PDF input",
                "errorDetail": {
                    "code": "unsupported_input",
                    "feature": "native_pdf",
                    "provider": "openai",
                    "model": "text-only-model"
                }
            })
        );
        assert!(!value.to_string().contains("JVBER"));
        assert!(matches!(
            runtime_decoded_value::<ProtocolEvent>(value)
                .unwrap()
                .error_detail,
            Some(ProtocolErrorDetail::InputValidation(
                InputValidationErrorDetail {
                    code: InputValidationErrorCode::UnsupportedInput,
                    ..
                }
            ))
        ));
    }

    #[test]
    fn protocol_memory_maintenance_error_detail_is_flat_and_excludes_success_payload() {
        let event = ProtocolEvent {
            submission_id: "maintenance-query".into(),
            error: Some("memory maintenance persistence is inconsistent".into()),
            error_detail: Some(
                MemoryMaintenanceQueryErrorDetail {
                    code: MemoryMaintenanceQueryErrorCode::PersistenceInconsistent,
                    run_kind: Some(MemoryMaintenanceRunKind::AutoDream),
                    run_id: Some("dream-terminal-failure".into()),
                }
                .into(),
            ),
            ..Default::default()
        };

        let value = runtime_encoded_value(&event).unwrap();
        assert_eq!(
            value,
            json!({
                "submissionId": "maintenance-query",
                "error": "memory maintenance persistence is inconsistent",
                "errorDetail": {
                    "code": "memory_maintenance_persistence_inconsistent",
                    "runKind": "auto_dream",
                    "runId": "dream-terminal-failure"
                }
            })
        );
        assert!(value.get("memoryMaintenance").is_none());
        assert!(matches!(
            runtime_decoded_value::<ProtocolEvent>(value)
                .unwrap()
                .error_detail,
            Some(ProtocolErrorDetail::MemoryMaintenance(
                MemoryMaintenanceQueryErrorDetail {
                    code: MemoryMaintenanceQueryErrorCode::PersistenceInconsistent,
                    run_kind: Some(MemoryMaintenanceRunKind::AutoDream),
                    ..
                }
            ))
        ));
    }

    #[test]
    fn protocol_known_error_codes_reject_malformed_typed_detail() {
        assert!(runtime_decoded_value::<ProtocolEvent>(json!({
            "submissionId": "malformed-input-error",
            "error": "malformed typed input error",
            "errorDetail": {
                "code": "unsupported_input",
                "limitBytes": "not-a-number"
            }
        }))
        .is_err());

        assert!(runtime_decoded_value::<ProtocolEvent>(json!({
            "submissionId": "malformed-maintenance-error",
            "error": "malformed typed maintenance error",
            "errorDetail": {
                "code": "memory_maintenance_persistence_inconsistent",
                "runKind": 42,
                "runId": "dream-terminal-failure"
            }
        }))
        .is_err());

        assert!(runtime_decoded_value::<ProtocolEvent>(json!({
            "submissionId": "malformed-maintenance-enum-shape",
            "error": "malformed typed maintenance error",
            "errorDetail": {
                "code": "memory_maintenance_persistence_inconsistent",
                "runKind": { "future_kind": null },
                "runId": "dream-terminal-failure"
            }
        }))
        .is_err());
    }

    #[test]
    fn protocol_unknown_error_detail_decodes_and_round_trips_for_forward_compatibility() {
        let value = json!({
            "submissionId": "future-error",
            "error": "a newer server reported a typed failure",
            "errorDetail": {
                "code": "future_error_code",
                "futureField": { "nested": true }
            }
        });

        let event: ProtocolEvent = runtime_decoded_value(value.clone()).unwrap();
        assert!(matches!(
            &event.error_detail,
            Some(ProtocolErrorDetail::Unknown(_))
        ));
        assert_eq!(runtime_encoded_value(&event).unwrap(), value);
    }

    #[test]
    fn protocol_future_maintenance_run_kind_preserves_the_correlated_error_frame() {
        let value = json!({
            "submissionId": "future-maintenance-kind",
            "error": "a newer maintenance lifecycle became inconsistent",
            "errorDetail": {
                "code": "memory_maintenance_persistence_inconsistent",
                "runKind": "auto_memory_reindex",
                "runId": "future-run",
                "futureField": { "nested": true }
            }
        });
        let expected_detail = runtime_value(value["errorDetail"].clone());

        let event: ProtocolEvent = runtime_decoded_value(value.clone()).unwrap();
        assert!(matches!(
            &event.error_detail,
            Some(ProtocolErrorDetail::Unknown(detail)) if detail == &expected_detail
        ));
        assert_eq!(runtime_encoded_value(&event).unwrap(), value);
    }

    #[test]
    fn provider_oauth_token_credential_id_round_trips() {
        let value = json!({
            "type": "oauth_token",
            "accessToken": "access-secret",
            "refreshToken": "refresh-secret",
            "credentialId": "desktop-account-a",
            "replaceStored": true,
            "oauth": {
                "clientId": "client-a",
                "authorizeUrl": "https://auth.example/authorize",
                "tokenUrl": "https://auth.example/token",
                "redirectUri": "http://127.0.0.1/callback"
            }
        });

        let parsed: ProviderAuthInit = runtime_decoded_value(value.clone()).unwrap();
        assert_eq!(runtime_encoded_value(&parsed).unwrap(), value);
    }

    #[test]
    fn legacy_o_auth_token_alias_decodes_to_canonical_wire_name() {
        let parsed: ProviderAuthInit = runtime_decoded_value(json!({
            "type": "o_auth_token",
            "accessToken": "legacy-access"
        }))
        .unwrap();

        assert_eq!(
            runtime_encoded_value(&parsed).unwrap(),
            json!({
                "type": "oauth_token",
                "accessToken": "legacy-access"
            })
        );
    }

    #[test]
    fn provider_oauth_stored_wire_shape_contains_no_tokens() {
        let value = json!({
            "type": "oauth_stored",
            "credentialId": "desktop-account-a",
            "oauth": {
                "clientId": "client-a",
                "authorizeUrl": "https://auth.example/authorize",
                "tokenUrl": "https://auth.example/token",
                "redirectUri": "http://127.0.0.1/callback"
            }
        });

        let parsed: ProviderAuthInit = runtime_decoded_value(value.clone()).unwrap();
        let encoded = runtime_encoded_value(&parsed).unwrap();
        assert_eq!(encoded, value);
        assert!(encoded.get("accessToken").is_none());
        assert!(encoded.get("refreshToken").is_none());
    }

    #[test]
    fn op_variant_tags_and_field_names() {
        // Approve carries a camelCase requestId + a discriminated response.
        let v = json!({
            "id": "1",
            "op": { "type": "approve", "requestId": "r1", "response": { "decision": "deny" } }
        });
        assert!(parse_test_op(v).is_ok());

        // steer priority is lowercase.
        let v = json!({ "id": "1", "op": { "type": "steer", "input": "x", "priority": "now" } });
        assert!(parse_test_op(v).is_ok());

        for t in [
            "interrupt",
            "get_history",
            "get_cost",
            "get_subagents",
            "get_subagent_lifecycle",
            "get_queued",
            "take_pending",
            "shutdown",
        ] {
            assert!(
                parse_test_op(json!({ "id": "1", "op": { "type": t } })).is_ok(),
                "{t}"
            );
        }
        assert!(parse_test_op(json!({ "id": "1", "op": { "type": "bogus" } })).is_err());
    }

    #[test]
    fn installed_plugin_admin_ops_use_typed_wire_shapes() {
        for value in [
            json!({
                "id": "plugins-list",
                "op": { "type": "list_installed_plugins" }
            }),
            json!({
                "id": "plugins-runtime",
                "op": { "type": "get_plugin_runtime" }
            }),
            json!({
                "id": "plugins-disable",
                "op": {
                    "type": "set_installed_plugin_enabled",
                    "name": "docs@local",
                    "enabled": false
                }
            }),
            json!({
                "id": "plugins-uninstall",
                "op": {
                    "type": "uninstall_plugin",
                    "name": "docs@local",
                    "removeFiles": true
                }
            }),
        ] {
            assert!(parse_test_op(value.clone()).is_ok(), "{value}");
        }
    }

    #[test]
    fn plugin_install_version_guard_preserves_absent_and_empty_versions() {
        for expected in [None, Some("1.0.0"), Some("")] {
            let mut value = json!({
                "id": "plugins-install",
                "op": {
                    "type": "install_plugin",
                    "marketplace": "local",
                    "sourceDir": "/tmp/plugin-package",
                    "version": "2.0.0"
                }
            });
            if let Some(expected) = expected {
                value["op"]["expectedInstalledVersion"] = json!(expected);
            }
            let parsed = parse_test_op(value.clone()).unwrap();
            match &parsed.op {
                OpKind::InstallPlugin {
                    expected_installed_version,
                    ..
                } => assert_eq!(
                    expected_installed_version,
                    &expected.map(RuntimeString::from)
                ),
                _ => panic!("expected plugin install operation"),
            }
            assert_eq!(runtime_encoded_value(&parsed).unwrap(), value);
        }
        assert!(parse_test_op(json!({
            "id": "plugins-invalid-update",
            "op": {
                "type": "install_plugin",
                "marketplace": "local",
                "sourceDir": "/tmp/plugin-package",
                "expectedInstalledVersion": false
            }
        }))
        .is_err());
    }

    #[test]
    fn mcp_oauth_wire_shapes_are_typed_and_never_echo_credentials() {
        assert!(parse_test_op(json!({
            "id": "resolve-1",
            "op": {
                "type": "resolve_mcp_authorization",
                "requestId": "auth-1",
                "response": {
                    "kind": "code",
                    "code": "authorization-code-secret",
                    "state": "csrf-state"
                }
            }
        }))
        .is_ok());
        assert!(parse_test_op(json!({
            "id": "status-1",
            "op": {
                "type": "get_mcp_authorization_status",
                "serverName": "docs"
            }
        }))
        .is_ok());
        assert!(parse_test_op(json!({
            "id": "revoke-1",
            "op": {
                "type": "revoke_mcp_authorization",
                "serverName": "docs"
            }
        }))
        .is_ok());

        let event = ProtocolEvent {
            mcp_authorization: Some(McpAuthorizationRequestWire {
                request_id: "auth-1".into(),
                server_name: "docs".into(),
                authorization_url: "https://auth.example/authorize?state=csrf-state".into(),
                redirect_uri: "http://127.0.0.1:33418/callback".into(),
            }),
            ..Default::default()
        };
        let encoded = runtime_encoded_value(&event).unwrap();
        assert_eq!(
            encoded,
            json!({
                "submissionId": "",
                "mcpAuthorization": {
                    "requestId": "auth-1",
                    "serverName": "docs",
                    "authorizationUrl": "https://auth.example/authorize?state=csrf-state",
                    "redirectUri": "http://127.0.0.1:33418/callback"
                }
            })
        );
        let serialized = encoded.to_string();
        for secret in [
            "authorization-code-secret",
            "oauth-access-secret",
            "oauth-refresh-secret",
        ] {
            assert!(!serialized.contains(secret));
        }
    }

    #[test]
    fn rejects_unknown_permission_mode() {
        let ok = |v: serde_json::Value| parse_test_op(v).is_ok();
        assert!(ok(
            json!({ "id": "1", "op": { "type": "set_permission_mode", "mode": "plan" } })
        ));
        assert!(!ok(
            json!({ "id": "1", "op": { "type": "set_permission_mode", "mode": "bogus" } })
        ));
        assert!(ok(
            json!({ "id": "2", "op": { "type": "run", "input": "hi", "options": { "permissionMode": "acceptEdits" } } })
        ));
        assert!(!ok(
            json!({ "id": "2", "op": { "type": "run", "input": "hi", "options": { "permissionMode": "bogus" } } })
        ));
    }

    #[test]
    fn run_options_are_tolerant_of_extra_keys() {
        let v = json!({
            "id": "3",
            "op": { "type": "run", "input": "hi", "options": { "model": "m", "extra": true } }
        });
        assert!(parse_test_op(v).is_ok());
    }

    #[test]
    fn rewind_requires_positive_integer_num_turns() {
        let mk = |n: serde_json::Value| {
            parse_test_op(json!({ "id": "1", "op": { "type": "rewind", "numTurns": n } }))
        };
        assert!(mk(json!(1)).is_ok());
        assert!(mk(json!(3)).is_ok());
        assert!(mk(json!(0)).is_err());
        assert!(mk(json!(-1)).is_err());
        assert!(mk(json!(1.5)).is_err());
        assert!(mk(json!("2")).is_err());

        let with_scope =
            json!({ "id": "1", "op": { "type": "rewind", "numTurns": 2, "scope": "files" } });
        assert!(parse_test_op(with_scope).is_ok());
    }

    #[test]
    fn fork_session_accepts_an_optional_before_message_uuid_cut_point() {
        let full = parse_test_op(
            json!({ "id": "fork", "op": { "type": "fork_session", "threadId": "thread-1" } }),
        )
        .unwrap();
        match full.op {
            OpKind::ForkSession {
                before_message_uuid,
                ..
            } => assert!(before_message_uuid.is_none()),
            other => panic!("expected fork_session, got {other:?}"),
        }

        let cut = parse_test_op(json!({ "id": "fork", "op": {
            "type": "fork_session",
            "threadId": "thread-1",
            "newThreadId": "branch-1",
            "beforeMessageUuid": "msg-7",
        } }))
        .unwrap();
        match cut.op {
            OpKind::ForkSession {
                thread_id,
                new_thread_id,
                before_message_uuid,
                inherit_goal,
            } => {
                assert!(inherit_goal.is_none());
                assert_eq!(thread_id, "thread-1");
                assert_eq!(new_thread_id.as_ref().unwrap(), "branch-1");
                assert_eq!(before_message_uuid.as_ref().unwrap(), "msg-7");
            }
            other => panic!("expected fork_session, got {other:?}"),
        }
    }

    #[test]
    fn session_management_ops_parse_from_stable_snake_case_wire_names() {
        for value in [
            json!({ "id": "init", "op": { "type": "init_session", "init": { "cwd": "/repo", "permissionMode": "plan", "config": { "contextWindow": 2048 }, "provider": { "type": "openai", "model": "gpt-5", "api": "responses" }, "auth": { "type": "env" } } } }),
            json!({ "id": "list", "op": { "type": "list_sessions", "options": { "limit": 10, "includeArchived": true } } }),
            json!({ "id": "resume", "op": { "type": "resume_session", "threadId": "thread-1" } }),
            json!({ "id": "fork", "op": { "type": "fork_session", "threadId": "thread-1" } }),
            json!({ "id": "archive", "op": { "type": "archive_session", "threadId": "thread-1" } }),
            json!({ "id": "delete", "op": { "type": "delete_session", "threadId": "thread-1" } }),
            json!({ "id": "transcript", "op": { "type": "load_subagent_transcript", "threadId": "thread-1", "agentId": "agent-1" } }),
            json!({ "id": "context", "op": { "type": "context_snapshot" } }),
            json!({ "id": "compact", "op": { "type": "compact_session", "instructions": "preserve decisions" } }),
            json!({ "id": "side", "op": { "type": "side_question", "question": "what happened?" } }),
            json!({ "id": "cancel", "op": { "type": "cancel_request", "requestId": "side" } }),
            json!({ "id": "dream", "op": { "type": "dream_now" } }),
            json!({ "id": "generate-title", "op": { "type": "generate_title", "persist": true } }),
            json!({ "id": "title", "op": { "type": "update_title", "threadId": "thread-1", "title": "New title" } }),
        ] {
            assert!(parse_test_op(value).is_ok());
        }
    }

    // The host entry for review mode: every target shape parses from its
    // stable wire spelling, and malformed targets are rejected.
    #[test]
    fn review_session_op_parses_every_target_shape() {
        for value in [
            json!({ "id": "r1", "op": { "type": "review_session", "target": { "type": "uncommitted_changes" } } }),
            json!({ "id": "r2", "op": { "type": "review_session", "target": { "type": "base_branch", "branch": "main" } } }),
            json!({ "id": "r3", "op": { "type": "review_session", "target": { "type": "commit", "sha": "abc1234", "title": "Fix parser" } } }),
            json!({ "id": "r4", "op": { "type": "review_session", "target": { "type": "files", "paths": ["src/a.rs"] } } }),
            json!({ "id": "r5", "op": { "type": "review_session", "target": { "type": "custom", "instructions": "focus on error handling" }, "userFacingHint": "error handling" } }),
        ] {
            assert!(parse_test_op(value.clone()).is_ok(), "{value}");
        }
        // target is required, and unknown target types are rejected.
        assert!(parse_test_op(json!({ "id": "r6", "op": { "type": "review_session" } })).is_err());
        assert!(parse_test_op(
            json!({ "id": "r7", "op": { "type": "review_session", "target": { "type": "bogus" } } })
        )
        .is_err());
        assert!(parse_test_op(
            json!({ "id": "r8", "op": { "type": "review_session", "target": { "type": "base_branch" } } })
        )
        .is_err());
    }

    #[test]
    fn openai_provider_compatibility_init_accepts_typed_gateway_configuration() {
        let value = json!({
            "id": "init-qwen",
            "op": {
                "type": "init_session",
                "init": {
                    "provider": {
                        "type": "openai",
                        "model": "qwen3.7-max",
                        "api": "chat",
                        "baseUrl": "https://gateway.example/v1",
                        "compatibility": {
                            "requestIdentity": { "sessionId": "klaus-session-1" },
                            "extraHeaders": {
                                "X-Endpoint": "klaus-desktop",
                                "X-Caller-App": "klaus-desktop",
                                "X-User-Id": "user-1",
                                "X-Device-Id": "device-1"
                            }
                        }
                    },
                    "auth": { "type": "api_key", "key": "token" }
                }
            }
        });

        let op = parse_test_op(value.clone()).expect("gateway configuration should decode");
        assert_eq!(runtime_encoded_value(&op).unwrap(), value);
    }

    #[test]
    fn anthropic_provider_init_custom_headers_round_trip() {
        let value = json!({
            "id": "init-gateway",
            "op": {
                "type": "init_session",
                "init": {
                    "provider": {
                        "type": "anthropic",
                        "model": "claude-sonnet-4-6",
                        "baseUrl": "https://gateway.example/v1",
                        "httpHeaders": {
                            "x-caller-app": "desktop",
                            "x-route-id": "42"
                        },
                        "envHttpHeaders": {
                            "x-team-token": "TEAM_TOKEN"
                        }
                    },
                    "auth": { "type": "api_key", "key": "token" }
                }
            }
        });

        let op = parse_test_op(value.clone()).expect("anthropic custom headers should decode");
        assert_eq!(runtime_encoded_value(&op).unwrap(), value);
    }

    #[test]
    fn dream_completion_uses_the_submission_correlated_wire_shape() {
        let event = ProtocolEvent {
            submission_id: "dream-1".into(),
            dream_completed: Some(DreamNowCompletion {
                ok: true,
                reason: None,
                files_touched: Some(vec!["/memory/consolidated.md".into()]),
                turn_count: Some(2),
            }),
            ..Default::default()
        };

        assert_eq!(
            runtime_encoded_value(&event).unwrap(),
            json!({
                "submissionId": "dream-1",
                "dreamCompleted": {
                    "ok": true,
                    "filesTouched": ["/memory/consolidated.md"],
                    "turnCount": 2
                }
            })
        );
    }

    #[test]
    fn dream_start_reason_uses_the_target_kebab_case_wire_value() {
        let event = ProtocolEvent {
            submission_id: "dream-closed".into(),
            dream: Some(DreamNowOutcome {
                ok: false,
                reason: Some(klaus_agent_core::engine::session::DreamNowReason::GateClosed),
            }),
            ..Default::default()
        };

        assert_eq!(
            runtime_encoded_value(&event).unwrap(),
            json!({
                "submissionId": "dream-closed",
                "dream": { "ok": false, "reason": "gate-closed" }
            })
        );
    }

    #[test]
    fn manual_compaction_progress_and_result_expose_full_boundary_details() {
        let result = CompactSessionResult {
            compacted: true,
            freed_tokens: 800,
            reason: None,
            diagnostics: Vec::new(),
            before_tokens: 900,
            after_tokens: 42,
            before_messages: 8,
            after_messages: 2,
            boundary: Some(CompactBoundaryMessage {
                native_compaction: None,
                uuid: "boundary-1".into(),
                summary: "preserved decisions".into(),
            }),
        };
        let started = ProtocolEvent {
            submission_id: "compact-op".into(),
            compact_progress: Some(CompactProgress {
                id: "compact-item".into(),
                phase: CompactProgressPhase::Started,
                trigger: CompactTrigger::Manual,
                mode: CompactionMode::Full,
                result: None,
                error: None,
            }),
            ..Default::default()
        };
        let completed = ProtocolEvent {
            submission_id: "compact-op".into(),
            compact_progress: Some(CompactProgress {
                id: "compact-item".into(),
                phase: CompactProgressPhase::Completed,
                trigger: CompactTrigger::Manual,
                mode: CompactionMode::Full,
                result: Some(result.clone()),
                error: None,
            }),
            ..Default::default()
        };
        let reply = ProtocolEvent {
            submission_id: "compact-op".into(),
            compact: Some(result),
            ..Default::default()
        };

        assert_eq!(
            runtime_encoded_value(&started).unwrap(),
            json!({
                "submissionId": "compact-op",
                "compactProgress": {
                    "id": "compact-item",
                    "phase": "started",
                    "trigger": "manual",
                    "mode": "full"
                }
            })
        );
        assert_eq!(
            runtime_encoded_value(&completed).unwrap(),
            json!({
                "submissionId": "compact-op",
                "compactProgress": {
                    "id": "compact-item",
                    "phase": "completed",
                    "trigger": "manual",
                    "mode": "full",
                    "result": {
                        "compacted": true,
                        "freedTokens": 800,
                        "beforeTokens": 900,
                        "afterTokens": 42,
                        "beforeMessages": 8,
                        "afterMessages": 2,
                        "boundary": {
                            "uuid": "boundary-1",
                            "summary": "preserved decisions"
                        }
                    }
                }
            })
        );
        assert_eq!(
            runtime_encoded_value(&reply).unwrap(),
            json!({
                "submissionId": "compact-op",
                "compact": {
                    "compacted": true,
                    "freedTokens": 800,
                    "beforeTokens": 900,
                    "afterTokens": 42,
                    "beforeMessages": 8,
                    "afterMessages": 2,
                    "boundary": {
                        "uuid": "boundary-1",
                        "summary": "preserved decisions"
                    }
                }
            })
        );
    }

    #[test]
    fn manual_compaction_noop_reason_and_diagnostics_match_progress_and_final_result() {
        let result = CompactSessionResult {
            compacted: false,
            freed_tokens: 0,
            reason: Some(CompactionNoopReason::SummaryInflated),
            diagnostics: vec![CompactionDiagnostic {
                provider_error: None,
                stage: CompactionDiagnosticStage::Summary,
                kind: CompactionDiagnosticKind::ProviderError,
                message: Some("provider stream failed".into()),
            }],
            before_tokens: 900,
            after_tokens: 900,
            before_messages: 8,
            after_messages: 8,
            boundary: None,
        };
        let completed = ProtocolEvent {
            submission_id: "compact-op".into(),
            compact_progress: Some(CompactProgress::manual_result(
                "compact-item".into(),
                result.clone(),
            )),
            ..Default::default()
        };
        let reply = ProtocolEvent {
            submission_id: "compact-op".into(),
            compact: Some(result),
            ..Default::default()
        };

        let completed = runtime_encoded_value(&completed).unwrap();
        let reply = runtime_encoded_value(&reply).unwrap();
        assert_eq!(completed["compactProgress"]["phase"], "failed");
        assert_eq!(
            completed["compactProgress"]["error"],
            "provider stream failed"
        );
        let progress_result = completed["compactProgress"]["result"].clone();
        let final_result = reply["compact"].clone();

        assert_eq!(progress_result, final_result);
        assert_eq!(final_result["reason"], "summary-inflated");
        assert_eq!(
            final_result["diagnostics"],
            json!([{
                "stage": "summary",
                "kind": "provider-error",
                "message": "provider stream failed"
            }])
        );
    }

    #[test]
    fn session_init_carries_full_serializable_engine_envelope() {
        let op = parse_test_op(json!({
            "id": "init",
            "op": {
                "type": "init_session",
                "init": {
                    "baseInstructions": "desktop base",
                    "developerInstructions": "desktop developer",
                    "dynamicTools": [
                        {
                            "type": "function",
                            "name": "lookup",
                            "description": "Look up a value",
                            "inputSchema": { "type": "object" },
                            "deferLoading": true
                        },
                        {
                            "type": "namespace",
                            "name": "desktop",
                            "description": "Desktop services",
                            "tools": [{
                                "type": "function",
                                "name": "open",
                                "description": "Open an item",
                                "inputSchema": { "type": "object" }
                            }]
                        }
                    ],
                    "lspServers": [{
                        "language": "rust",
                        "extensions": ["rs"],
                        "command": "rust-analyzer"
                    }],
                    "shellEnv": { "KLAUS_WINDOW": "window-1" },
                    "agents": [{
                        "name": "desktop-agent",
                        "description": "Desktop specialist"
                    }],
                    "skills": [{
                        "name": "desktop-skill",
                        "description": "Desktop workflow",
                        "body": "Run it.",
                        "path": "/desktop/skills/desktop-skill/SKILL.md"
                    }],
                    "outputStyles": [{
                        "name": "formal",
                        "body": "Use formal prose."
                    }],
                    "selectedOutputStyle": "formal",
                    "commandHooks": [{
                        "event": "PreToolUse",
                        "matcher": "read",
                        "command": "validate-read",
                        "timeoutMs": 5000
                    }],
                    "pricing": [{
                        "model": "fake-model",
                        "inputPerMTok": 7.5,
                        "outputPerMTok": 0.25,
                        "cacheReadPerMTok": 0.1,
                        "cacheWritePerMTok": 0.2
                    }]
                }
            }
        }))
        .expect("full init envelope should parse");

        let OpKind::InitSession { init } = op.op else {
            panic!("expected init_session");
        };
        assert!(init
            .base_instructions
            .as_ref()
            .is_some_and(|value| value == "desktop base"));
        assert!(init
            .developer_instructions
            .as_ref()
            .is_some_and(|value| value == "desktop developer"));
        assert_eq!(init.dynamic_tools.len(), 2);
        assert_eq!(init.lsp_servers[0].language, "rust");
        assert_eq!(init.agents[0].name, "desktop-agent");
        assert_eq!(init.skills[0].name, "desktop-skill");
        assert_eq!(init.output_styles[0].name, "formal");
        assert!(init
            .selected_output_style
            .as_ref()
            .is_some_and(|value| value == "formal"));
        assert_eq!(init.command_hooks[0].event, HookEvent::PreToolUse);
        assert_eq!(
            init.pricing.as_ref().unwrap()[0].pricing.input_per_m_tok,
            7.5
        );
        assert_eq!(
            init.shell_env
                .as_ref()
                .and_then(|env| env.get(&RuntimeString::from("KLAUS_WINDOW"))),
            Some(&RuntimeString::from("window-1"))
        );
    }

    /// `readOnly` is the host's declaration that a dynamic tool only observes.
    /// It has to survive the wire: dropped here, the permission engine classifies
    /// the client's own read tool as mutating, which asks before every question it
    /// answers and refuses it outright in plan mode. Absent means false, so a host
    /// that says nothing keeps the conservative classification.
    #[test]
    fn dynamic_tools_carry_the_hosts_read_only_declaration() {
        let op = parse_test_op(json!({
            "id": "init",
            "op": {
                "type": "init_session",
                "init": {
                    "dynamicTools": [
                        {
                            "type": "function",
                            "name": "klaus_query",
                            "description": "Read the client",
                            "inputSchema": { "type": "object" },
                            "readOnly": true
                        },
                        {
                            "type": "function",
                            "name": "klaus_action",
                            "description": "Operate the client",
                            "inputSchema": { "type": "object" }
                        },
                        {
                            "type": "namespace",
                            "name": "desktop",
                            "description": "Desktop services",
                            "tools": [{
                                "type": "function",
                                "name": "peek",
                                "description": "Look at one thing",
                                "inputSchema": { "type": "object" },
                                "readOnly": true
                            }]
                        }
                    ]
                }
            }
        }))
        .expect("init carrying readOnly dynamic tools should parse");

        let OpKind::InitSession { init } = op.op else {
            panic!("expected init_session");
        };
        let declared: Vec<bool> = init
            .dynamic_tools
            .iter()
            .flat_map(|spec| match spec {
                ProtocolDynamicToolSpec::Function(tool) => vec![tool.read_only],
                ProtocolDynamicToolSpec::Namespace(namespace) => namespace
                    .tools
                    .iter()
                    .map(|ProtocolDynamicToolNamespaceTool::Function(tool)| tool.read_only)
                    .collect(),
            })
            .collect();
        assert_eq!(declared, vec![true, false, true]);
    }

    #[test]
    fn session_init_rejects_unsupported_live_host_objects_instead_of_ignoring_them() {
        assert!(parse_test_op(json!({
            "id": "init",
            "op": {
                "type": "init_session",
                "init": {
                    "mcpClients": [{ "hostReference": "live-client-1" }]
                }
            }
        }))
        .is_err());
        assert!(parse_test_op(json!({
            "id": "init-agent",
            "op": {
                "type": "init_session",
                "init": {
                    "agents": [{
                        "name": "desktop-agent",
                        "description": "Desktop specialist",
                        "mcpServers": [{ "hostReference": "live-agent-mcp" }]
                    }]
                }
            }
        }))
        .is_err());
    }

    #[test]
    fn dynamic_tool_call_and_response_use_codex_compatible_wire_shapes() {
        let event = ProtocolEvent {
            submission_id: "turn-1".into(),
            dynamic_tool_call: Some(ProtocolDynamicToolCall {
                call_id: "call-1".into(),
                thread_id: "thread-1".into(),
                turn_id: "turn-1".into(),
                namespace: Some("desktop".into()),
                tool: "lookup".into(),
                arguments: runtime_value(json!({ "key": "answer" })),
            }),
            ..Default::default()
        };
        assert_eq!(
            runtime_encoded_value(&event).unwrap(),
            json!({
                "submissionId": "turn-1",
                "dynamicToolCall": {
                    "callId": "call-1",
                    "threadId": "thread-1",
                    "turnId": "turn-1",
                    "namespace": "desktop",
                    "tool": "lookup",
                    "arguments": { "key": "answer" }
                }
            })
        );

        let response = parse_test_op(json!({
            "id": "response-1",
            "op": {
                "type": "resolve_dynamic_tool_call",
                "callId": "call-1",
                "response": {
                    "contentItems": [
                        { "type": "inputText", "text": "42" },
                        { "type": "inputImage", "imageUrl": "data:image/png;base64,QQ==" }
                    ],
                    "success": true
                }
            }
        }))
        .expect("dynamic tool response should parse");
        assert!(matches!(
            response.op,
            OpKind::ResolveDynamicToolCall { call_id, .. } if call_id == "call-1"
        ));
    }

    #[test]
    fn session_config_update_has_an_explicit_mutable_surface() {
        assert!(parse_test_op(json!({
            "id": "update",
            "op": {
                "type": "update_session_config",
                "update": { "model": "model-b", "permissionMode": "plan" }
            }
        }))
        .is_ok());
        assert!(parse_test_op(json!({
            "id": "update",
            "op": {
                "type": "update_session_config",
                "update": { "contextWindow": 4096 }
            }
        }))
        .is_err());
    }

    #[test]
    fn runtime_policy_legacy_fields_are_decode_only() {
        let value = json!({
            "id": "runtime-plan",
            "op": {
                "type": "set_runtime_policy",
                "permissionMode": "plan",
                "prePlanPermissionMode": "bypassPermissions",
                "networkAllowed": true,
                "sandboxMode": "read-only"
            }
        });
        let op = parse_test_op(value.clone()).expect("legacy runtime policy should decode");
        let mut expected = value;
        expected["op"]
            .as_object_mut()
            .unwrap()
            .remove("permissionMode");
        expected["op"]
            .as_object_mut()
            .unwrap()
            .remove("prePlanPermissionMode");
        assert_eq!(runtime_encoded_value(&op).unwrap(), expected);
    }

    #[test]
    fn runtime_policy_optional_workspace_table_and_start_network_use_exact_keys() {
        let minimal = json!({"id":"policy","op":{
            "type":"set_runtime_policy"
        }});
        let decoded = parse_test_op(minimal.clone()).unwrap();
        assert_eq!(runtime_encoded_value(&decoded).unwrap(), minimal);
        let exact = json!({"id":"policy","op":{
            "type":"set_runtime_policy",
            "sandboxMode":"workspace-write","networkAllowed":true,
            "sandboxWorkspaceWrite":{
                "writable_roots":[std::env::temp_dir().join("klaus-workspace")],"network_access":false,
                "exclude_tmpdir_env_var":true,"exclude_slash_tmp":true
            }
        }});
        let decoded = parse_test_op(exact.clone()).unwrap();
        assert_eq!(runtime_encoded_value(&decoded).unwrap(), exact);
        for (key, value) in [
            ("sandbox", json!("auto")),
            ("requireRealSandbox", json!(true)),
            ("sandboxRetryOnDenial", json!(false)),
        ] {
            let mut retired = minimal.clone();
            retired["op"][key] = value;
            assert!(parse_test_op(retired).is_err(), "retired field {key}");
        }
        let mut extended_table = exact.clone();
        extended_table["op"]["sandboxWorkspaceWrite"]["future_option"] = json!(true);
        let decoded = parse_test_op(extended_table).unwrap();
        assert_eq!(runtime_encoded_value(&decoded).unwrap(), exact);
        for allowed in [false, true] {
            let value = json!({"networkAllowed":allowed,"config":{
                "sandbox_mode":"read-only"
            }});
            let init: ProtocolSessionInit = runtime_decoded_value(value.clone()).unwrap();
            assert_eq!(init.network_allowed, Some(allowed));
            assert_eq!(runtime_encoded_value(&init).unwrap(), value);
        }
        assert!(parse_test_op(json!({"id":"retired","op":{
            "type":"sandbox_diagnostics","operationId":"retired"
        }}))
        .is_err());
    }

    #[test]
    fn runtime_settings_updates_preserve_null_and_independent_policy_fields() {
        for verbosity in [
            serde_json::Value::Null,
            json!("low"),
            json!("medium"),
            json!("high"),
        ] {
            let value = json!({"id":"settings","op":{"type":"update_session_config","update":{
                "modelVerbosity":verbosity,"reasoningSummary":"none","webSearchAccess":"indexed"
            }}});
            let op = parse_test_op(value.clone()).unwrap();
            assert_eq!(runtime_encoded_value(&op).unwrap(), value);
        }
        let value = json!({"id":"policy","op":{"type":"set_runtime_policy",
            "networkAllowed":false,
            "approvalPolicy":"on-request","sandboxMode":"read-only","approvalsReviewer":"auto_review"
        }});
        let op = parse_test_op(value.clone()).unwrap();
        assert_eq!(runtime_encoded_value(&op).unwrap(), value);
        for update in [
            json!({"modelVerbosity":"verbose"}),
            json!({"reasoningSummary":"raw"}),
            json!({"webSearchAccess":"other"}),
        ] {
            assert!(parse_test_op(
                json!({"id":"invalid","op":{"type":"update_session_config","update":update}})
            )
            .is_err());
        }
    }

    #[test]
    fn protocol_event_result_shape() {
        let e = ProtocolEvent {
            submission_id: "s".into(),
            result: Some(RunResult {
                stop_reason: TerminalReason::Completed,
                usage: None,
                final_text: "hi".into(),
                final_message_id: Some("a-final".into()),
                structured_output: None,
            }),
            ..Default::default()
        };
        assert_eq!(
            runtime_encoded_value(&e).unwrap(),
            json!({
                "submissionId": "s",
                "result": { "stopReason": "completed", "usage": null, "finalText": "hi", "finalMessageId": "a-final" }
            })
        );
    }

    #[test]
    fn archive_result_confirms_success_without_exposing_store_path() {
        let event = ProtocolEvent {
            submission_id: "archive-1".into(),
            archived: Some(ArchiveSessionResult {
                thread_id: "thread-1".into(),
                archived: true,
            }),
            ..Default::default()
        };

        assert_eq!(
            runtime_encoded_value(&event).unwrap(),
            json!({
                "submissionId": "archive-1",
                "archived": {
                    "threadId": "thread-1",
                    "archived": true
                }
            })
        );
    }

    #[test]
    fn session_list_does_not_expose_store_paths() {
        let sessions: ThreadPage = runtime_decoded_value(json!({
            "items": [{
                "id": "thread-1",
                "createdAt": 1_700_000_000,
                "cwd": "/workspace/project",
                "model": "model-a",
                "path": "/Users/alice/.klaus/sessions/1700000000-thread-1.jsonl",
                "archived": false
            }],
            "nextCursor": null
        }))
        .unwrap();
        let event = ProtocolEvent {
            submission_id: "list-1".into(),
            sessions: Some(sessions),
            ..Default::default()
        };

        assert_eq!(
            runtime_encoded_value(&event).unwrap(),
            json!({
                "submissionId": "list-1",
                "sessions": {
                    "items": [{
                        "id": "thread-1",
                        "createdAt": 1_700_000_000,
                        "updatedAt": 1_700_000_000,
                        "cwd": "/workspace/project",
                        "model": "model-a",
                        "role": "main",
                        "archived": false
                    }],
                    "nextCursor": null
                }
            })
        );
    }

    #[test]
    fn elicitation_wire_shape() {
        let e = ProtocolEvent {
            submission_id: "s".into(),
            elicitation: Some(Elicitation {
                request_id: "r".into(),
                request: ElicitRequestWire {
                    questions: None,
                    attachments_supported: None,
                    metadata: None,
                    prompt: "pick".into(),
                    options: Some(vec!["a".into(), "b".into()]),
                    multi_select: None,
                },
            }),
            ..Default::default()
        };
        assert_eq!(
            runtime_encoded_value(&e).unwrap(),
            json!({
                "submissionId": "s",
                "elicitation": {
                    "requestId": "r",
                    "request": { "prompt": "pick", "options": ["a", "b"] }
                }
            })
        );
    }

    #[test]
    fn structured_elicit_answers_round_trip_without_changing_text() {
        let source = json!({"kind":"answers","answers":{"Question, with comma?":"  custom, text  ","Features?":"A, B, C"}});
        let decoded: ElicitResponseWire = runtime_decoded_value(source.clone()).unwrap();
        assert!(matches!(
            &decoded,
            ElicitResponseWire::Answers {
                attachments: None,
                ..
            }
        ));
        assert_eq!(runtime_encoded_value(&decoded).unwrap(), source);
        assert!(runtime_decoded_value::<ElicitResponseWire>(
            json!({"kind":"answers","answers":{"Question?":["A"]}})
        )
        .is_err());
    }

    #[test]
    fn structured_elicit_images_round_trip_outside_the_text_answers() {
        let source = json!({
            "kind": "answers",
            "answers": {"Which layout?": "  Spacious, custom  "},
            "attachments": {
                "Which layout?": [
                    {"type": "image", "mimeType": "image/png", "base64": "AQID"},
                    {"type": "image", "mimeType": "image/webp", "base64": "BAUG"}
                ]
            }
        });
        let decoded: ElicitResponseWire = runtime_decoded_value(source.clone()).unwrap();
        assert_eq!(runtime_encoded_value(&decoded).unwrap(), source);
        let ElicitResponseWire::Answers {
            answers,
            attachments,
        } = decoded.clone()
        else {
            panic!("expected structured answers");
        };
        assert_eq!(
            ElicitResponse::try_from(decoded).unwrap(),
            ElicitResponse::Answers {
                answers,
                attachments,
            }
        );
    }

    #[test]
    fn elicit_attachment_capability_is_explicit_and_optional() {
        for source in [
            json!({"prompt": "Pick", "options": ["A", "B"]}),
            json!({"prompt": "Pick", "questions": [{"question": "Pick"}]}),
            json!({"prompt": "Pick", "attachmentsSupported": false}),
        ] {
            let request: ElicitRequestWire = runtime_decoded_value(source.clone()).unwrap();
            assert_eq!(runtime_encoded_value(&request).unwrap(), source);
        }
        let legacy = ElicitRequest {
            prompt: "Pick".into(),
            questions: None,
            attachments_supported: None,
            metadata: None,
            options: None,
            multi_select: None,
        };
        assert_eq!(
            runtime_encoded_value(&ElicitRequestWire::from(legacy.clone())).unwrap(),
            json!({"prompt": "Pick"})
        );
        let question = runtime_value(json!({"question": "Pick"}))
            .as_object()
            .unwrap()
            .clone();
        let structured = ElicitRequest {
            questions: Some(vec![question]),
            ..legacy
        };
        for prompt in [
            "Pick",
            "How should this request proceed?",
            "Keep retrying, or stop?",
        ] {
            let unsupported = ElicitRequestWire::from(ElicitRequest {
                prompt: prompt.into(),
                ..structured.clone()
            });
            assert!(unsupported.questions.is_some());
            assert_eq!(unsupported.attachments_supported, None);
            let source = runtime_encoded_value(&unsupported).unwrap();
            assert!(source.get("attachmentsSupported").is_none());
            assert_eq!(
                runtime_decoded_value::<ElicitRequestWire>(source).unwrap(),
                unsupported
            );
        }
        for supported in [false, true] {
            let request = ElicitRequestWire::from(ElicitRequest {
                attachments_supported: Some(supported),
                ..structured.clone()
            });
            assert_eq!(request.attachments_supported, Some(supported));
            let source = runtime_encoded_value(&request).unwrap();
            assert_eq!(source["attachmentsSupported"], json!(supported));
            assert_eq!(
                runtime_decoded_value::<ElicitRequestWire>(source).unwrap(),
                request
            );
        }
    }

    #[test]
    fn elicit_response_wire_round_trip() {
        let a: ElicitResponseWire =
            runtime_decoded_value(json!({ "kind": "accept", "values": ["x"] })).unwrap();
        assert_eq!(
            a,
            ElicitResponseWire::Accept {
                values: vec!["x".into()]
            }
        );
        let d: ElicitResponseWire = runtime_decoded_value(json!({ "kind": "decline" })).unwrap();
        assert_eq!(d, ElicitResponseWire::Decline { reason: None });
    }

    #[test]
    fn mcp_elicitation_wire_shape_is_distinct_and_structured() {
        let event_value = json!({
            "submissionId": "s",
            "mcpElicitation": {
                "requestId": "mcp-elicit-public-1",
                "serverName": "computer-use",
                "request": {
                    "mode": "form",
                    "_meta": { "persist": true },
                    "message": "Allow access?",
                    "requestedSchema": {
                        "type": "object",
                        "properties": {
                            "account": { "type": "string", "format": "email" },
                            "remember": { "type": "boolean" }
                        },
                        "required": ["account"]
                    }
                }
            }
        });
        let event: ProtocolEvent = runtime_decoded_value(event_value.clone()).unwrap();
        assert_eq!(runtime_encoded_value(&event).unwrap(), event_value);

        let op = parse_test_op(json!({
            "id": "resolve-1",
            "op": {
                "type": "resolve_mcp_elicitation",
                "requestId": "mcp-elicit-public-1",
                "response": {
                    "action": "accept",
                    "content": { "account": "person@example.com", "remember": true },
                    "_meta": { "persist": true }
                }
            }
        }))
        .unwrap();
        assert!(matches!(
            op.op,
            OpKind::ResolveMcpElicitation {
                response: McpElicitationResponse {
                    action: McpElicitationAction::Accept,
                    content: Some(_),
                    meta: Some(_),
                },
                ..
            }
        ));
    }

    #[test]
    fn rewound_wire_shape() {
        let e = ProtocolEvent {
            submission_id: "s".into(),
            rewound: Some(RewindOutcome {
                scope: RewindScope::Both,
                dropped_turns: 0,
                remaining_turns: 0,
                files_restored: false,
                error: Some("no session to rewind".into()),
            }),
            ..Default::default()
        };
        assert_eq!(
            runtime_encoded_value(&e).unwrap(),
            json!({
                "submissionId": "s",
                "rewound": {
                    "scope": "both",
                    "droppedTurns": 0,
                    "remainingTurns": 0,
                    "filesRestored": false,
                    "error": "no session to rewind"
                }
            })
        );
    }

    #[test]
    fn shutdown_completion_wire_shape_is_exact_and_round_trips() {
        let expected = json!({
            "submissionId": "shutdown-1",
            "shutdownCompleted": {
                "ok": false,
                "flushCompleted": true,
                "cleanupCompleted": false,
                "timedOut": true,
                "error": "memory maintenance shutdown timed out"
            }
        });
        let event: ProtocolEvent = runtime_decoded_value(expected.clone()).unwrap();
        assert_eq!(
            event.shutdown_completed,
            Some(Box::new(ShutdownCompletion {
                ok: false,
                flush_completed: true,
                cleanup_completed: false,
                timed_out: true,
                error: Some("memory maintenance shutdown timed out".into()),
            }))
        );
        assert_eq!(runtime_encoded_value(&event).unwrap(), expected);
    }
    #[test]
    fn subagent_event_wire_retains_child_identity_and_live_payload() {
        let expected = json!({
            "submissionId": "",
            "subagentEvent": {
                "agentId": "child-a",
                "event": { "type": "text.delta", "text": "Live", "messageId": "message-a", "phase": "commentary" }
            }
        });
        let event: ProtocolEvent = runtime_decoded_value(expected.clone()).unwrap();
        assert!(
            event.event.is_none(),
            "child activity must never impersonate parent activity"
        );
        assert!(event.subagent_event.is_some());
        assert_eq!(runtime_encoded_value(&event).unwrap(), expected);
    }
    #[test]
    fn agent_notification_live_and_history_wire_keep_receipt_identity() {
        let receipt = json!({ "notificationId":"stable", "source":"team-mailbox", "status":"updated", "agentName":"reviewer", "timestamp":1000, "turnId":"turn" });
        for expected in [
            json!({ "submissionId":"", "agentNotification":receipt }),
            json!({ "submissionId":"history", "agentNotifications":[receipt] }),
        ] {
            let event: ProtocolEvent = runtime_decoded_value(expected.clone()).unwrap();
            assert!(
                event.event.is_none(),
                "receipt cannot masquerade as parent turn activity"
            );
            assert_eq!(runtime_encoded_value(&event).unwrap(), expected);
        }
    }

    #[test]
    fn subagent_history_wire_retains_all_turn_metadata() {
        let expected = json!({ "submissionId": "history", "subagentTranscript": {
            "messages": [], "turnIndex": {
                "historyTurnIds": { "message": "turn" }, "turnFinalMessageIds": {},
                "turnStatuses": { "turn": "failed" }, "turnStartedAtMs": { "turn": 1000 },
                "turnFinishedAtMs": { "turn": 1500 }, "turnDurations": { "turn": 500 },
                "turnErrors": { "turn": "offline" }, "turnErrorKinds": { "turn": "network" },
                "turnCancelledTools": { "turn": ["tool"] }, "turnGoalMetadata": {}
            }
        } });
        let event: ProtocolEvent = runtime_decoded_value(expected.clone()).unwrap();
        assert_eq!(runtime_encoded_value(&event).unwrap(), expected);
    }
}
