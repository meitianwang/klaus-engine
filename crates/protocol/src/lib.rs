//! Wire protocol for the agent engine: newline-delimited JSON messages over a
//! core session, a client that speaks the protocol, and a server-side adapter
//! that bridges the protocol to a core session.

mod activity;
pub mod client;
pub mod server;
pub mod testing;
pub mod types;

pub use klaus_agent_core::types::abort::AbortSignal;

pub use types::{
    parse_op, AgentMemoryMergeChoice, AgentMemoryScope, AgentMemorySnapshotDecisionAudit,
    AgentMemorySnapshotDecisionPolicy, AgentMemorySnapshotDecisionQueryResult,
    AgentMemorySnapshotDecisionRequest, AgentMemorySnapshotDecisionSource,
    AgentMemorySnapshotDecisionStatus, AgentMemorySnapshotDiff, AgentMemorySnapshotDiffEntry,
    AgentMemorySnapshotDiffKind, AgentMemorySnapshotPolicyAction,
    AgentMemorySnapshotUnavailableReason, ArchiveSessionResult, AutoDreamRun, AutoDreamRunReason,
    AutoMemoryRun, AutoMemoryRunReason, BackgroundAgentSummary, BoundSession,
    CompactBoundaryMessage, CompactProgress, CompactProgressPhase, CompactSelection,
    CompactSelectionDirection, CompactSessionResult, CompactTrigger, CompactionDiagnostic,
    CompactionDiagnosticKind, CompactionDiagnosticStage, CompactionMode, CompactionNoopReason,
    DeleteSessionResult, DreamNowCompletion, DreamNowOutcome, DreamNowReason, EffectiveExtension,
    EffectiveHook, ElicitRequestWire, ElicitResponseWire, Elicitation, ExtensionKind,
    ExtensionRuntimeSnapshot, ExtensionSource, ExtensionSourceKind, GenerateTitleOpResult,
    HookHandlerKind, InitSessionResult, InstalledPluginMutationResult, InstalledPluginRecord,
    InstalledPlugins, ListOptions, LspServerConfig, McpElicitationAction, McpElicitationEvent,
    McpElicitationRequest, McpElicitationResolvedWire, McpElicitationResponse,
    McpElicitationSchema, MemoryMaintenanceEvent, MemoryMaintenanceQueryError,
    MemoryMaintenanceQueryErrorCode, MemoryMaintenanceQueryErrorDetail, MemoryMaintenanceRunKind,
    MemoryMaintenanceSnapshot, MemoryMaintenanceStatus, MemoryMaintenanceTrigger,
    MemoryMaintenanceTurn, Op, OpKind, PluginActivationScope, PluginRuntimeSnapshot,
    PricingRuntimeSnapshot, ProtocolBrowserCapabilityInit, ProtocolCommandHook,
    ProtocolDynamicToolCall, ProtocolDynamicToolCallOutputContentItem,
    ProtocolDynamicToolFunctionSpec, ProtocolDynamicToolNamespaceSpec,
    ProtocolDynamicToolNamespaceTool, ProtocolDynamicToolResolved, ProtocolDynamicToolResponse,
    ProtocolDynamicToolSpec, ProtocolErrorDetail, ProtocolEvent, ProtocolHostCapabilitiesInit,
    ProtocolMcpOAuthOption, ProtocolMcpServerInit, ProtocolModelPricing, ProtocolPluginCatalog,
    ProtocolPluginCatalogEntry, ProtocolPluginCatalogRoot, ProtocolPluginContributions,
    ProtocolRuntimeCapabilityRootInit, ProtocolSelectedCapabilityRootInit,
    ProtocolSessionConfigUpdate, ProtocolSessionConfigUpdateResult, ProtocolSessionFileChange,
    ProtocolSessionInit, ProtocolSessionTurnFileChanges, ProtocolTrustedWindowsSandboxContext,
    ProviderAuthInit, ProviderInit, ProviderOAuthInit, QueueEdit, QueueItemState,
    QueueMutationResult, QueueMutationStatus, QueuePriority, QueueSnapshot, QueueSnapshotItem,
    QueueSnapshotStatus, QueueTerminalState, QueuedMessage, QueuedValue, RenameThreadResult,
    ResolvedRuntimeCapabilityRoot, ReviewCodeLocation, ReviewFinding, ReviewLineRange,
    ReviewOutput, ReviewRequest, ReviewSessionResult, ReviewTarget, RewindOutcome, RewindPreview,
    RewindScope, RunOpOptions, RunResult, RuntimeCapabilityDescriptor, RuntimeCapabilityName,
    RuntimeCapabilityResolution, RuntimeCapabilitySource, RuntimeCapabilitySourceKind,
    SelectedRuntimeCapabilityRoot, SetupLifecycleEvent, SetupLifecycleSnapshot, SetupRunRecord,
    SetupRunRequest, SetupRunResult, SetupRunStatus, SetupTrigger, ShutdownCompletion,
    SteerPriority, SubAgentTranscript, SubagentContext, SubagentKind, SubagentLifecycleRecord,
    SubagentSnapshot, SubagentStatus, SuppressedExtension, Task, TaskListChange,
    TaskListCursorStatus, TaskListQueryOptions, TaskListSnapshot, TaskStatus, ThreadPage,
    ThreadSummary, UnarchiveSessionResult, UnarchiveSessionStatus, UpdateThreadHostMetadataResult,
    UpdateTitleResult,
};

pub use server::{
    ArchiveSession, BindSession, CreateSession, CreateSessionArgs, CreateSessionFuture,
    DeleteSession, InstalledPluginLedgerMutation, ListSessions, LoadSubagentTranscript,
    ProtocolLifecycle, ProtocolPluginAdmin, ProtocolServer, ProtocolServerOptions,
    UnarchiveSession, UnarchiveSessionFuture, UpdateTitle,
};

pub use client::{
    ClientError, EventStream, ProtocolClient, ProtocolClientOptions, ProtocolDreamNowTask,
};

/// A freshly minted submission/request id (v4 UUID).
pub(crate) fn new_uuid() -> String {
    uuid::Uuid::new_v4().to_string()
}
