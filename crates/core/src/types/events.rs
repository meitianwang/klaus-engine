//! The outward event stream + item projection.

use crate::browser::BrowserRuntimeEvent;
use crate::provider::observability::ProviderDecisionEvent;
use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};
use crate::schemas::message::Usage;
use crate::types::errors::{EngineError, InputValidationErrorDetail, TerminalReason};
use crate::types::permission::{
    ApprovalKind, ApprovalRequest, PermissionDestination, PermissionUpdate,
};
use serde::{Deserialize, Serialize};

fn encode_event_variant(
    out: &mut RuntimeEncoder,
    tag: &str,
    fields: RuntimeObjectEncoder,
) -> Result<(), RuntimeCodecError> {
    runtime_codec::encode_internal(out, "type", &[], tag, &fields.finish())
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ItemStatus {
    InProgress,
    Completed,
    Failed,
}

impl RuntimeEncode for ItemStatus {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::InProgress => "in_progress",
            Self::Completed => "completed",
            Self::Failed => "failed",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for ItemStatus {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("in_progress", &[]),
                RuntimeEnumVariant::new("completed", &[]),
                RuntimeEnumVariant::new("failed", &[]),
            ],
            &[Self::InProgress, Self::Completed, Self::Failed],
        )
    }
}

/// Non-secret provenance supplied by the host, never provider configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InferenceSource {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tier: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selection: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InferenceIdentity {
    pub revision: u64,
    pub change_id: Option<RuntimeString>,
    pub model: RuntimeString,
    pub source: Option<RuntimeString>,
    pub tier: Option<RuntimeString>,
    pub selection: Option<RuntimeString>,
}

impl RuntimeEncode for InferenceIdentity {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("revision", &self.revision)?;
        object.field("model", &self.model)?;
        if let Some(value) = &self.change_id {
            object.field("changeId", value)?;
        }
        if let Some(value) = &self.source {
            object.field("source", value)?;
        }
        if let Some(value) = &self.tier {
            object.field("tier", value)?;
        }
        if let Some(value) = &self.selection {
            object.field("selection", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}
impl RuntimeDecode for InferenceIdentity {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                revision: object.required("revision", &[])?,
                model: object.required("model", &[])?,
                change_id: object.optional("changeId", &[])?,
                source: object.optional("source", &[])?,
                tier: object.optional("tier", &[])?,
                selection: object.optional("selection", &[])?,
            })
        })
    }
}

/// The subset of statuses a `file_change` item may carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileChangeStatus {
    Completed,
    Failed,
}

impl RuntimeEncode for FileChangeStatus {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for FileChangeStatus {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("completed", &[]),
                RuntimeEnumVariant::new("failed", &[]),
            ],
            &[Self::Completed, Self::Failed],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileChangeKind {
    Create,
    Modify,
    Delete,
}

impl RuntimeEncode for FileChangeKind {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Create => "create",
            Self::Modify => "modify",
            Self::Delete => "delete",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for FileChangeKind {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("create", &[]),
                RuntimeEnumVariant::new("modify", &[]),
                RuntimeEnumVariant::new("delete", &[]),
            ],
            &[Self::Create, Self::Modify, Self::Delete],
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FileChange {
    pub path: RuntimeString,
    pub kind: FileChangeKind,
    /// Unified-diff-ish preview, optional.
    pub diff: Option<RuntimeString>,
    /// Exact unified diff from this turn's held baseline to the newest content.
    /// Missing on historical events or when the baseline could not be held.
    /// An empty diff means the turn restored the original content and existence.
    pub turn_diff: Option<RuntimeString>,
    /// Lines this TURN has added / removed in this file so far, measured against
    /// the content the turn FIRST found there. A host keeps the newest report per
    /// file instead of adding them up: each one already covers every edit the turn
    /// has made to that file, so summing them charges the same line once per edit
    /// and can exceed the file's own length.
    ///
    /// This answers what the turn produced, which is a different question from
    /// what a repository holds — so it must not be answered with a VCS diff, and a
    /// session in a plain directory answers it just as well.
    ///
    /// `None` means the volume could not be measured (no readable baseline, or no
    /// exact count was affordable), which a host must render as "unknown", never
    /// as zero.
    pub added: Option<u64>,
    pub removed: Option<u64>,
}

impl RuntimeEncode for FileChange {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("path", &self.path)?;
        object.field("kind", &self.kind)?;
        if let Some(diff) = &self.diff {
            object.field("diff", diff)?;
        }
        if let Some(turn_diff) = &self.turn_diff {
            object.field("turnDiff", turn_diff)?;
        }
        if let Some(added) = self.added {
            object.field("added", &added)?;
        }
        if let Some(removed) = self.removed {
            object.field("removed", &removed)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for FileChange {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                path: object.required("path", &[])?,
                kind: object.required("kind", &[])?,
                diff: object.optional("diff", &[])?,
                turn_diff: object.optional("turnDiff", &[])?,
                added: object.optional("added", &[])?,
                removed: object.optional("removed", &[])?,
            })
        })
    }
}

/// The error payload on a `tool_call` item.
#[derive(Debug, Clone, PartialEq)]
pub struct ItemError {
    pub message: RuntimeString,
}

impl RuntimeEncode for ItemError {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("message", &self.message)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ItemError {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                message: object.required("message", &[])?,
            })
        })
    }
}

/// Outward "item" model. The reduced, host-facing
/// projection of what happened — distinct from the internal `Message` log.
#[derive(Debug, Clone, PartialEq)]
pub enum ThreadItem {
    ProposedPlan {
        id: RuntimeString,
        plan: crate::planning::types::ProposedPlan,
    },
    AgentMessage {
        id: RuntimeString,
        text: RuntimeString,
        phase: Option<RuntimeString>,
    },
    Reasoning {
        id: RuntimeString,
        text: RuntimeString,
    },
    CommandExecution {
        id: RuntimeString,
        command: RuntimeString,
        aggregated_output: RuntimeString,
        exit_code: Option<i32>,
        status: ItemStatus,
    },
    FileChange {
        id: RuntimeString,
        changes: Vec<FileChange>,
        status: FileChangeStatus,
    },
    ToolCall {
        id: RuntimeString,
        tool_name: RuntimeString,
        arguments: RuntimeJson,
        result: Option<RuntimeJson>,
        error: Option<ItemError>,
        status: ItemStatus,
    },
    Error {
        id: RuntimeString,
        message: RuntimeString,
    },
}

impl RuntimeEncode for ThreadItem {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        let tag = match self {
            Self::ProposedPlan { id, plan } => {
                object.field("id", id)?;
                object.field("plan", plan)?;
                "proposed_plan"
            }
            Self::AgentMessage { id, text, phase } => {
                object.field("id", id)?;
                object.field("text", text)?;
                if let Some(phase) = phase {
                    object.field("phase", phase)?;
                }
                "agent_message"
            }
            Self::Reasoning { id, text } => {
                object.field("id", id)?;
                object.field("text", text)?;
                "reasoning"
            }
            Self::CommandExecution {
                id,
                command,
                aggregated_output,
                exit_code,
                status,
            } => {
                object.field("id", id)?;
                object.field("command", command)?;
                object.field("aggregatedOutput", aggregated_output)?;
                if let Some(exit_code) = exit_code {
                    object.field("exitCode", exit_code)?;
                }
                object.field("status", status)?;
                "command_execution"
            }
            Self::FileChange {
                id,
                changes,
                status,
            } => {
                object.field("id", id)?;
                object.field("changes", changes)?;
                object.field("status", status)?;
                "file_change"
            }
            Self::ToolCall {
                id,
                tool_name,
                arguments,
                result,
                error,
                status,
            } => {
                object.field("id", id)?;
                object.field("toolName", tool_name)?;
                object.field("arguments", arguments)?;
                if let Some(result) = result {
                    object.field("result", result)?;
                }
                if let Some(error) = error {
                    object.field("error", error)?;
                }
                object.field("status", status)?;
                "tool_call"
            }
            Self::Error { id, message } => {
                object.field("id", id)?;
                object.field("message", message)?;
                "error"
            }
        };
        encode_event_variant(out, tag, object)
    }
}

impl RuntimeDecode for ThreadItem {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        const VARIANTS: &[RuntimeEnumVariant<'_>] = &[
            RuntimeEnumVariant::new("agent_message", &[]),
            RuntimeEnumVariant::new("reasoning", &[]),
            RuntimeEnumVariant::new("command_execution", &[]),
            RuntimeEnumVariant::new("file_change", &[]),
            RuntimeEnumVariant::new("tool_call", &[]),
            RuntimeEnumVariant::new("error", &[]),
            RuntimeEnumVariant::new("proposed_plan", &[]),
        ];
        runtime_codec::decode_internal(value, false, "type", &[], VARIANTS, |variant, object| {
            Ok(match variant {
                0 => Self::AgentMessage {
                    id: object.required("id", &[])?,
                    text: object.required("text", &[])?,
                    phase: object.optional("phase", &[])?,
                },
                1 => Self::Reasoning {
                    id: object.required("id", &[])?,
                    text: object.required("text", &[])?,
                },
                2 => Self::CommandExecution {
                    id: object.required("id", &[])?,
                    command: object.required("command", &[])?,
                    aggregated_output: object.required("aggregatedOutput", &[])?,
                    exit_code: object.optional("exitCode", &[])?,
                    status: object.required("status", &[])?,
                },
                3 => Self::FileChange {
                    id: object.required("id", &[])?,
                    changes: object.required("changes", &[])?,
                    status: object.required("status", &[])?,
                },
                4 => Self::ToolCall {
                    id: object.required("id", &[])?,
                    tool_name: object.required("toolName", &[])?,
                    arguments: object.required("arguments", &[])?,
                    result: object.optional("result", &[])?,
                    error: object.optional("error", &[])?,
                    status: object.required("status", &[])?,
                },
                5 => Self::Error {
                    id: object.required("id", &[])?,
                    message: object.required("message", &[])?,
                },
                6 => Self::ProposedPlan {
                    id: object.required("id", &[])?,
                    plan: object.required("plan", &[])?,
                },
                _ => unreachable!("closed ThreadItem variant table"),
            })
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolLifecyclePhase {
    Requested,
    Approved,
    Started,
    Progress,
    Completed,
    Failed,
    Cancelled,
}

impl RuntimeEncode for ToolLifecyclePhase {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Requested => "requested",
            Self::Approved => "approved",
            Self::Started => "started",
            Self::Progress => "progress",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for ToolLifecyclePhase {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("requested", &[]),
                RuntimeEnumVariant::new("approved", &[]),
                RuntimeEnumVariant::new("started", &[]),
                RuntimeEnumVariant::new("progress", &[]),
                RuntimeEnumVariant::new("completed", &[]),
                RuntimeEnumVariant::new("failed", &[]),
                RuntimeEnumVariant::new("cancelled", &[]),
            ],
            &[
                Self::Requested,
                Self::Approved,
                Self::Started,
                Self::Progress,
                Self::Completed,
                Self::Failed,
                Self::Cancelled,
            ],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolLifecycleSourceKind {
    Builtin,
    Mcp,
    Workflow,
    Subagent,
    CodeMode,
    Shell,
    Browser,
    Host,
}

impl RuntimeEncode for ToolLifecycleSourceKind {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Builtin => "builtin",
            Self::Mcp => "mcp",
            Self::Workflow => "workflow",
            Self::Subagent => "subagent",
            Self::CodeMode => "code_mode",
            Self::Shell => "shell",
            Self::Browser => "browser",
            Self::Host => "host",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for ToolLifecycleSourceKind {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("builtin", &[]),
                RuntimeEnumVariant::new("mcp", &[]),
                RuntimeEnumVariant::new("workflow", &[]),
                RuntimeEnumVariant::new("subagent", &[]),
                RuntimeEnumVariant::new("code_mode", &[]),
                RuntimeEnumVariant::new("shell", &[]),
                RuntimeEnumVariant::new("browser", &[]),
                RuntimeEnumVariant::new("host", &[]),
            ],
            &[
                Self::Builtin,
                Self::Mcp,
                Self::Workflow,
                Self::Subagent,
                Self::CodeMode,
                Self::Shell,
                Self::Browser,
                Self::Host,
            ],
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ToolLifecycleSource {
    pub kind: ToolLifecycleSourceKind,
    pub name: Option<RuntimeString>,
    pub exposure: Option<RuntimeString>,
}

impl RuntimeEncode for ToolLifecycleSource {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("kind", &self.kind)?;
        if let Some(name) = &self.name {
            object.field("name", name)?;
        }
        if let Some(exposure) = &self.exposure {
            object.field("exposure", exposure)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ToolLifecycleSource {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                kind: object.required("kind", &[])?,
                name: object.optional("name", &[])?,
                exposure: object.optional("exposure", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ToolLifecycleRecord {
    pub turn_id: RuntimeString,
    pub tool_use_id: RuntimeString,
    pub tool_name: RuntimeString,
    pub phase: ToolLifecyclePhase,
    pub source: Option<ToolLifecycleSource>,
    pub arguments: Option<RuntimeJson>,
    pub approval_kind: Option<ApprovalKind>,
    pub event: Option<RuntimeJson>,
    pub is_error: Option<bool>,
    pub error: Option<RuntimeString>,
}

impl RuntimeEncode for ToolLifecycleRecord {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("turnId", &self.turn_id)?;
        object.field("toolUseId", &self.tool_use_id)?;
        object.field("toolName", &self.tool_name)?;
        object.field("phase", &self.phase)?;
        if let Some(source) = &self.source {
            object.field("source", source)?;
        }
        if let Some(arguments) = &self.arguments {
            object.field("arguments", arguments)?;
        }
        if let Some(approval_kind) = &self.approval_kind {
            object.field("approvalKind", approval_kind)?;
        }
        if let Some(event) = &self.event {
            object.field("event", event)?;
        }
        if let Some(is_error) = &self.is_error {
            object.field("isError", is_error)?;
        }
        if let Some(error) = &self.error {
            object.field("error", error)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ToolLifecycleRecord {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                turn_id: object.required("turnId", &[])?,
                tool_use_id: object.required("toolUseId", &[])?,
                tool_name: object.required("toolName", &[])?,
                phase: object.required("phase", &[])?,
                source: object.optional("source", &[])?,
                arguments: object.optional("arguments", &[])?,
                approval_kind: object.optional("approvalKind", &[])?,
                event: object.optional("event", &[])?,
                is_error: object.optional("isError", &[])?,
                error: object.optional("error", &[])?,
            })
        })
    }
}

/// Structured, render-agnostic progress a tool emits while running.
#[derive(Debug)]
// Keep the established public event API and serde shape. Boxing Browser here
// would force every event producer and consumer through a breaking API change.
#[allow(clippy::large_enum_variant)]
pub enum ToolDomainEvent {
    PlanUpdated {
        progress: RuntimeJson,
    },
    Stdout {
        chunk: RuntimeString,
    },
    Stderr {
        chunk: RuntimeString,
    },
    Status {
        message: RuntimeString,
    },
    FileChange {
        change: FileChange,
    },
    /// A sub-agent's nested activity, surfaced on the spawning tool's progress
    /// stream in real time. `agent_id` attributes it to
    /// the child; `event` carries the forwarded child EngineEvent. Recursive
    /// with EngineEvent so nested agents compose.
    AgentProgress {
        agent_id: RuntimeJson,
        event: Box<EngineEvent>,
    },
    /// Browser runtime progress/log/artifact events emitted by browser_* tools.
    Browser {
        event: BrowserRuntimeEvent,
    },
    /// Web-search tool progress, matching provider-style search phases without
    /// binding renderers to a specific UI.
    WebSearch {
        phase: RuntimeString,
        query: RuntimeString,
        result_count: Option<u64>,
    },
    /// The effective main-session Plan mode changed after a plan tool completed
    /// its mode transition. Hosts use this as the authoritative Spec-mode edge;
    /// inferring from a tool call would report failed or denied calls as active.
    PlanModeChanged {
        enabled: bool,
    },
    /// Orchestration mode automatically switched execution to the chat's main
    /// model because the configured execution model was unavailable or out of quota.
    OrchestratorFallback {
        failed_model: RuntimeString,
        main_model: RuntimeString,
        reason: RuntimeString,
    },
}

impl RuntimeEncode for ToolDomainEvent {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        let tag = match self {
            Self::PlanUpdated { progress } => {
                object.field("progress", progress)?;
                "plan_updated"
            }
            Self::Stdout { chunk } => {
                object.field("chunk", chunk)?;
                "stdout"
            }
            Self::Stderr { chunk } => {
                object.field("chunk", chunk)?;
                "stderr"
            }
            Self::Status { message } => {
                object.field("message", message)?;
                "status"
            }
            Self::FileChange { change } => {
                object.field("change", change)?;
                "file_change"
            }
            Self::AgentProgress { agent_id, event } => {
                object.field("agentId", agent_id)?;
                object.field("event", event)?;
                "agent_progress"
            }
            Self::Browser { event } => {
                object.field("event", event)?;
                "browser"
            }
            Self::WebSearch {
                phase,
                query,
                result_count,
            } => {
                object.field("phase", phase)?;
                object.field("query", query)?;
                if let Some(result_count) = result_count {
                    object.field("resultCount", result_count)?;
                }
                "web_search"
            }
            Self::PlanModeChanged { enabled } => {
                object.field("enabled", enabled)?;
                "plan_mode_changed"
            }
            Self::OrchestratorFallback {
                failed_model,
                main_model,
                reason,
            } => {
                object.field("failedModel", failed_model)?;
                object.field("mainModel", main_model)?;
                object.field("reason", reason)?;
                "orchestrator_fallback"
            }
        };
        encode_event_variant(out, tag, object)
    }
}

impl RuntimeDecode for ToolDomainEvent {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        const VARIANTS: &[RuntimeEnumVariant<'_>] = &[
            RuntimeEnumVariant::new("stdout", &[]),
            RuntimeEnumVariant::new("stderr", &[]),
            RuntimeEnumVariant::new("status", &[]),
            RuntimeEnumVariant::new("file_change", &[]),
            RuntimeEnumVariant::new("agent_progress", &[]),
            RuntimeEnumVariant::new("browser", &[]),
            RuntimeEnumVariant::new("web_search", &[]),
            RuntimeEnumVariant::new("plan_mode_changed", &[]),
            RuntimeEnumVariant::new("plan_updated", &[]),
            RuntimeEnumVariant::new("orchestrator_fallback", &[]),
        ];
        runtime_codec::decode_internal(value, false, "type", &[], VARIANTS, |variant, object| {
            Ok(match variant {
                0 => Self::Stdout {
                    chunk: object.required("chunk", &[])?,
                },
                1 => Self::Stderr {
                    chunk: object.required("chunk", &[])?,
                },
                2 => Self::Status {
                    message: object.required("message", &[])?,
                },
                3 => Self::FileChange {
                    change: object.required("change", &[])?,
                },
                4 => Self::AgentProgress {
                    agent_id: object.required("agentId", &[])?,
                    event: object.required("event", &[])?,
                },
                5 => Self::Browser {
                    event: object.required("event", &[])?,
                },
                6 => Self::WebSearch {
                    phase: object.required("phase", &[])?,
                    query: object.required("query", &[])?,
                    result_count: object.optional("resultCount", &[])?,
                },
                7 => Self::PlanModeChanged {
                    enabled: object.required("enabled", &[])?,
                },
                8 => Self::PlanUpdated {
                    progress: object.required("progress", &[])?,
                },
                9 => Self::OrchestratorFallback {
                    failed_model: object.required("failedModel", &[])?,
                    main_model: object.required("mainModel", &[])?,
                    reason: object.required("reason", &[])?,
                },
                _ => unreachable!("closed ToolDomainEvent variant table"),
            })
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionMode {
    Micro,
    Full,
}

impl RuntimeEncode for CompactionMode {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Micro => "micro",
            Self::Full => "full",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for CompactionMode {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("micro", &[]),
                RuntimeEnumVariant::new("full", &[]),
            ],
            &[Self::Micro, Self::Full],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NotificationLevel {
    Info,
    Warn,
}

impl RuntimeEncode for NotificationLevel {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Info => "info",
            Self::Warn => "warn",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for NotificationLevel {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("info", &[]),
                RuntimeEnumVariant::new("warn", &[]),
            ],
            &[Self::Info, Self::Warn],
        )
    }
}

/// The single outward event stream. Results travel as events
/// (`turn.completed` / `item.completed`) — never as the generator's return
/// value, which `for await` discards (B1).
#[derive(Debug)]
pub enum EngineEvent {
    PlanUpdated {
        progress: RuntimeJson,
    },
    PlanDelta {
        plan_id: RuntimeString,
        turn_id: RuntimeString,
        revision: u64,
        delta: RuntimeString,
    },
    ThreadStarted {
        thread_id: RuntimeString,
    },
    TurnStarted {
        turn_id: RuntimeString,
    },
    TurnCompleted {
        turn_id: RuntimeString,
        usage: Option<Usage>,
        stop_reason: TerminalReason,
    },
    TurnFailed {
        error: EngineError,
        inference: Option<InferenceIdentity>,
    },
    StreamRequestStart {
        inference: Option<InferenceIdentity>,
    },
    ProviderDecision {
        event: ProviderDecisionEvent,
        inference: Option<InferenceIdentity>,
    },
    TextDelta {
        text: RuntimeString,
        message_id: Option<RuntimeString>,
        phase: Option<RuntimeString>,
        inference: Option<InferenceIdentity>,
    },
    ThinkingDelta {
        text: RuntimeString,
        /// Stable reasoning item identity shared with its completed block.
        message_id: Option<RuntimeString>,
        inference: Option<InferenceIdentity>,
    },
    ItemStarted {
        item: ThreadItem,
        inference: Option<InferenceIdentity>,
    },
    ItemUpdated {
        item: ThreadItem,
        inference: Option<InferenceIdentity>,
    },
    ItemCompleted {
        item: ThreadItem,
        inference: Option<InferenceIdentity>,
    },
    ToolEvent {
        turn_id: RuntimeString,
        tool_use_id: RuntimeString,
        event: ToolDomainEvent,
    },
    QuestionRequested {
        question: crate::engine::questions::AsyncQuestion,
    },
    QuestionResolved {
        question_resolved: crate::engine::questions::QuestionResolution,
    },
    ApprovalRequested {
        request: ApprovalRequest,
    },
    ApprovalReview {
        review: crate::types::approval_review::ApprovalReviewEvent,
    },
    ApprovalReviewStrictRequired {
        turn_id: RuntimeString,
        started_at_ms: i64,
    },
    ApprovalReviewWarning {
        message: RuntimeString,
        kind: Option<RuntimeString>,
    },
    /// A user-approved permission update bound for a persisted (non-`session`)
    /// destination. The engine has already applied it to the live session
    /// state; this signals the host to write it to the named settings file.
    PermissionUpdate {
        destination: PermissionDestination,
        update: PermissionUpdate,
    },
    /// Progress of automatic compaction, including a no-op completion.
    CompactionProgress {
        phase: RuntimeString,
    },
    Compaction {
        mode: CompactionMode,
        freed_tokens: u64,
    },
    Tombstone {
        uuid: RuntimeString,
    },
    Notification {
        level: NotificationLevel,
        message: RuntimeString,
    },
    /// A queued user input was rejected against the actual turn model or shared
    /// per-turn media budget before it reached a provider request.
    InputRejected {
        correlation_id: Option<RuntimeString>,
        message: RuntimeString,
        detail: InputValidationErrorDetail,
    },
    /// A queued input was attached to a real tool continuation.
    InputApplied {
        correlation_id: Option<RuntimeString>,
    },
}

impl EngineEvent {
    pub fn inference(&self) -> Option<&InferenceIdentity> {
        match self {
            Self::TurnFailed { inference, .. }
            | Self::StreamRequestStart { inference }
            | Self::ProviderDecision { inference, .. }
            | Self::TextDelta { inference, .. }
            | Self::ThinkingDelta { inference, .. }
            | Self::ItemStarted { inference, .. }
            | Self::ItemUpdated { inference, .. }
            | Self::ItemCompleted { inference, .. } => inference.as_ref(),
            _ => None,
        }
    }

    pub(crate) fn attribute_inference(&mut self, identity: Option<InferenceIdentity>) {
        let inference = match self {
            Self::TurnFailed { inference, .. }
            | Self::StreamRequestStart { inference }
            | Self::ProviderDecision { inference, .. }
            | Self::TextDelta { inference, .. }
            | Self::ThinkingDelta { inference, .. } => inference,
            Self::ItemStarted { item, inference }
            | Self::ItemUpdated { item, inference }
            | Self::ItemCompleted { item, inference }
                if matches!(
                    item,
                    ThreadItem::AgentMessage { .. }
                        | ThreadItem::Reasoning { .. }
                        | ThreadItem::ToolCall { .. }
                ) =>
            {
                inference
            }
            _ => return,
        };
        if inference.is_none() {
            *inference = identity;
        }
    }
}

impl RuntimeEncode for EngineEvent {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        if let Some(inference) = self.inference() {
            object.field("inference", inference)?;
        }
        let tag = match self {
            Self::PlanUpdated { progress } => {
                object.field("progress", progress)?;
                "plan.updated"
            }
            Self::PlanDelta {
                plan_id,
                turn_id,
                revision,
                delta,
            } => {
                object.field("planId", plan_id)?;
                object.field("turnId", turn_id)?;
                object.field("revision", revision)?;
                object.field("delta", delta)?;
                "plan.delta"
            }
            Self::ThreadStarted { thread_id } => {
                object.field("threadId", thread_id)?;
                "thread.started"
            }
            Self::TurnStarted { turn_id } => {
                object.field("turnId", turn_id)?;
                "turn.started"
            }
            Self::TurnCompleted {
                turn_id,
                usage,
                stop_reason,
            } => {
                object.field("turnId", turn_id)?;
                object.field("usage", usage)?;
                object.field("stopReason", stop_reason)?;
                "turn.completed"
            }
            Self::TurnFailed { error, .. } => {
                object.field("error", error)?;
                "turn.failed"
            }
            Self::StreamRequestStart { .. } => "stream.request_start",
            Self::ProviderDecision { event, .. } => {
                object.field("event", event)?;
                "provider.decision"
            }
            Self::TextDelta {
                text,
                message_id,
                phase,
                ..
            } => {
                object.field("text", text)?;
                if let Some(id) = message_id {
                    object.field("messageId", id)?;
                }
                if let Some(phase) = phase {
                    object.field("phase", phase)?;
                }
                "text.delta"
            }
            Self::ThinkingDelta {
                text, message_id, ..
            } => {
                object.field("text", text)?;
                if let Some(id) = message_id {
                    object.field("messageId", id)?;
                }
                "thinking.delta"
            }
            Self::ItemStarted { item, .. } => {
                object.field("item", item)?;
                "item.started"
            }
            Self::ItemUpdated { item, .. } => {
                object.field("item", item)?;
                "item.updated"
            }
            Self::ItemCompleted { item, .. } => {
                object.field("item", item)?;
                "item.completed"
            }
            Self::ToolEvent {
                turn_id,
                tool_use_id,
                event,
            } => {
                object.field("turnId", turn_id)?;
                object.field("toolUseId", tool_use_id)?;
                object.field("event", event)?;
                "tool.event"
            }
            Self::QuestionRequested { question } => {
                object.field("question", question)?;
                "question.requested"
            }
            Self::QuestionResolved { question_resolved } => {
                object.field("questionResolved", question_resolved)?;
                "question.resolved"
            }
            Self::ApprovalReviewStrictRequired {
                turn_id,
                started_at_ms,
            } => {
                object.field("turnId", turn_id)?;
                object.field("startedAtMs", started_at_ms)?;
                "approval_review_strict_required"
            }
            Self::ApprovalReviewWarning { message, kind } => {
                object.field("message", message)?;
                if let Some(kind) = kind {
                    object.field("kind", kind)?;
                }
                "approval_review_warning"
            }
            Self::ApprovalReview { review } => {
                object.field("review", review)?;
                "approval_review"
            }
            Self::ApprovalRequested { request } => {
                object.field("request", request)?;
                "approval.requested"
            }
            Self::PermissionUpdate {
                destination,
                update,
            } => {
                object.field("destination", destination)?;
                object.field("update", update)?;
                "permission_update"
            }
            Self::CompactionProgress { phase } => {
                object.field("phase", phase)?;
                "compaction.progress"
            }
            Self::Compaction { mode, freed_tokens } => {
                object.field("mode", mode)?;
                object.field("freedTokens", freed_tokens)?;
                "compaction"
            }
            Self::Tombstone { uuid } => {
                object.field("uuid", uuid)?;
                "tombstone"
            }
            Self::Notification { level, message } => {
                object.field("level", level)?;
                object.field("message", message)?;
                "notification"
            }
            Self::InputRejected {
                correlation_id,
                message,
                detail,
            } => {
                if let Some(correlation_id) = correlation_id {
                    object.field("correlationId", correlation_id)?;
                }
                object.field("message", message)?;
                object.field("detail", detail)?;
                "input.rejected"
            }
            Self::InputApplied { correlation_id } => {
                if let Some(correlation_id) = correlation_id {
                    object.field("correlationId", correlation_id)?;
                }
                "input.applied"
            }
        };
        encode_event_variant(out, tag, object)
    }
}

impl RuntimeDecode for EngineEvent {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        const VARIANTS: &[RuntimeEnumVariant<'_>] = &[
            RuntimeEnumVariant::new("thread.started", &[]),
            RuntimeEnumVariant::new("turn.started", &[]),
            RuntimeEnumVariant::new("turn.completed", &[]),
            RuntimeEnumVariant::new("turn.failed", &[]),
            RuntimeEnumVariant::new("stream.request_start", &[]),
            RuntimeEnumVariant::new("provider.decision", &[]),
            RuntimeEnumVariant::new("text.delta", &[]),
            RuntimeEnumVariant::new("thinking.delta", &[]),
            RuntimeEnumVariant::new("item.started", &[]),
            RuntimeEnumVariant::new("item.updated", &[]),
            RuntimeEnumVariant::new("item.completed", &[]),
            RuntimeEnumVariant::new("tool.event", &[]),
            RuntimeEnumVariant::new("approval.requested", &[]),
            RuntimeEnumVariant::new("permission_update", &[]),
            RuntimeEnumVariant::new("compaction", &[]),
            RuntimeEnumVariant::new("tombstone", &[]),
            RuntimeEnumVariant::new("notification", &[]),
            RuntimeEnumVariant::new("input.rejected", &[]),
            RuntimeEnumVariant::new("input.applied", &[]),
            RuntimeEnumVariant::new("compaction.progress", &[]),
            RuntimeEnumVariant::new("question.requested", &[]),
            RuntimeEnumVariant::new("question.resolved", &[]),
            RuntimeEnumVariant::new("plan.updated", &[]),
            RuntimeEnumVariant::new("plan.delta", &[]),
            RuntimeEnumVariant::new("approval_review", &[]),
            RuntimeEnumVariant::new("approval_review_strict_required", &[]),
            RuntimeEnumVariant::new("approval_review_warning", &[]),
        ];
        runtime_codec::decode_internal(value, false, "type", &[], VARIANTS, |variant, object| {
            Ok(match variant {
                0 => Self::ThreadStarted {
                    thread_id: object.required("threadId", &[])?,
                },
                1 => Self::TurnStarted {
                    turn_id: object.required("turnId", &[])?,
                },
                2 => Self::TurnCompleted {
                    // Tolerate pre-turn-identity traces: older baselines encode
                    // `turn.completed` without a turnId.
                    turn_id: object.defaulted("turnId", &[], || RuntimeString::from("legacy"))?,
                    usage: object.required("usage", &[])?,
                    stop_reason: object.required("stopReason", &[])?,
                },
                3 => Self::TurnFailed {
                    error: object.required("error", &[])?,
                    inference: object.optional("inference", &[])?,
                },
                4 => Self::StreamRequestStart {
                    inference: object.optional("inference", &[])?,
                },
                5 => Self::ProviderDecision {
                    inference: object.optional("inference", &[])?,
                    event: object.required("event", &[])?,
                },
                6 => Self::TextDelta {
                    inference: object.optional("inference", &[])?,
                    text: object.required("text", &[])?,
                    message_id: object.optional("messageId", &[])?,
                    phase: object.optional("phase", &[])?,
                },
                7 => Self::ThinkingDelta {
                    inference: object.optional("inference", &[])?,
                    text: object.required("text", &[])?,
                    message_id: object.optional("messageId", &[])?,
                },
                8 => Self::ItemStarted {
                    inference: object.optional("inference", &[])?,
                    item: object.required("item", &[])?,
                },
                9 => Self::ItemUpdated {
                    inference: object.optional("inference", &[])?,
                    item: object.required("item", &[])?,
                },
                10 => Self::ItemCompleted {
                    inference: object.optional("inference", &[])?,
                    item: object.required("item", &[])?,
                },
                11 => Self::ToolEvent {
                    turn_id: object.defaulted("turnId", &[], RuntimeString::default)?,
                    tool_use_id: object.required("toolUseId", &[])?,
                    event: object.required("event", &[])?,
                },
                12 => Self::ApprovalRequested {
                    request: object.required("request", &[])?,
                },
                13 => Self::PermissionUpdate {
                    destination: object.required("destination", &[])?,
                    update: object.required("update", &[])?,
                },
                14 => Self::Compaction {
                    mode: object.required("mode", &[])?,
                    freed_tokens: object.required("freedTokens", &[])?,
                },
                15 => Self::Tombstone {
                    uuid: object.required("uuid", &[])?,
                },
                16 => Self::Notification {
                    level: object.required("level", &[])?,
                    message: object.required("message", &[])?,
                },
                17 => Self::InputRejected {
                    correlation_id: object.optional("correlationId", &[])?,
                    message: object.required("message", &[])?,
                    detail: object.required("detail", &[])?,
                },
                18 => Self::InputApplied {
                    correlation_id: object.optional("correlationId", &[])?,
                },
                19 => Self::CompactionProgress {
                    phase: object.required("phase", &[])?,
                },
                20 => Self::QuestionRequested {
                    question: object.required("question", &[])?,
                },
                21 => Self::QuestionResolved {
                    question_resolved: object.required("questionResolved", &[])?,
                },
                22 => Self::PlanUpdated {
                    progress: object.required("progress", &[])?,
                },
                23 => Self::PlanDelta {
                    plan_id: object.required("planId", &[])?,
                    turn_id: object.required("turnId", &[])?,
                    revision: object.required("revision", &[])?,
                    delta: object.required("delta", &[])?,
                },
                24 => Self::ApprovalReview {
                    review: object.required("review", &[])?,
                },
                25 => Self::ApprovalReviewStrictRequired {
                    turn_id: object.required("turnId", &[])?,
                    started_at_ms: object.required("startedAtMs", &[])?,
                },
                26 => Self::ApprovalReviewWarning {
                    message: object.required("message", &[])?,
                    kind: object.optional("kind", &[])?,
                },
                _ => unreachable!("closed EngineEvent variant table"),
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::permission::ApprovalKind;

    fn exact_json(source: &str) -> RuntimeJson {
        crate::runtime_values::runtime_json::parse(&RuntimeString::from(source)).unwrap()
    }

    #[test]
    fn engine_event_dotted_tags() {
        let ev = EngineEvent::TurnCompleted {
            turn_id: "turn-1".into(),
            usage: None,
            stop_reason: TerminalReason::Completed,
        };
        assert_eq!(
            runtime_codec::encode_value(&ev).unwrap(),
            exact_json(
                r#"{"type":"turn.completed","turnId":"turn-1","usage":null,"stopReason":"completed"}"#
            )
        );

        let ev = EngineEvent::ToolEvent {
            turn_id: "turn-1".into(),
            tool_use_id: "tu_1".into(),
            event: ToolDomainEvent::Stdout { chunk: "hi".into() },
        };
        assert_eq!(
            runtime_codec::encode_value(&ev).unwrap(),
            exact_json(
                r#"{"type":"tool.event","turnId":"turn-1","toolUseId":"tu_1","event":{"type":"stdout","chunk":"hi"}}"#
            )
        );

        let ev = EngineEvent::ToolEvent {
            turn_id: "turn-1".into(),
            tool_use_id: "tu_plan".into(),
            event: ToolDomainEvent::PlanModeChanged { enabled: true },
        };
        assert_eq!(
            runtime_codec::encode_value(&ev).unwrap(),
            exact_json(
                r#"{"type":"tool.event","turnId":"turn-1","toolUseId":"tu_plan","event":{"type":"plan_mode_changed","enabled":true}}"#
            )
        );
    }

    #[test]
    fn tool_event_deserializes_legacy_events_without_turn_id() {
        let ev: EngineEvent = runtime_codec::decode_value(&exact_json(
            r#"{"type":"tool.event","toolUseId":"tu_1","event":{"type":"stdout","chunk":"hi"}}"#,
        ))
        .unwrap();
        match ev {
            EngineEvent::ToolEvent {
                turn_id,
                tool_use_id,
                event: ToolDomainEvent::Stdout { chunk },
            } => {
                assert_eq!(turn_id, "");
                assert_eq!(tool_use_id, "tu_1");
                assert_eq!(chunk, "hi");
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn thread_item_tool_call_wire_shape() {
        let item = ThreadItem::ToolCall {
            id: "i1".into(),
            tool_name: "read".into(),
            arguments: exact_json(r#"{"path":"a.txt"}"#),
            result: None,
            error: None,
            status: ItemStatus::InProgress,
        };
        assert_eq!(
            runtime_codec::encode_value(&item).unwrap(),
            exact_json(
                r#"{"type":"tool_call","id":"i1","toolName":"read","arguments":{"path":"a.txt"},"status":"in_progress"}"#
            )
        );
    }

    #[test]
    fn tool_lifecycle_record_wire_shape() {
        let record = ToolLifecycleRecord {
            turn_id: "turn-1".into(),
            tool_use_id: "t1".into(),
            tool_name: "read".into(),
            phase: ToolLifecyclePhase::Started,
            source: None,
            arguments: Some(exact_json(r#"{"path":"a.txt"}"#)),
            approval_kind: None,
            event: None,
            is_error: None,
            error: None,
        };

        assert_eq!(
            runtime_codec::encode_value(&record).unwrap(),
            exact_json(
                r#"{"turnId":"turn-1","toolUseId":"t1","toolName":"read","phase":"started","arguments":{"path":"a.txt"}}"#
            )
        );

        let approved = ToolLifecycleRecord {
            turn_id: "turn-1".into(),
            tool_use_id: "t1".into(),
            tool_name: "bash".into(),
            phase: ToolLifecyclePhase::Approved,
            source: Some(ToolLifecycleSource {
                kind: ToolLifecycleSourceKind::Shell,
                name: None,
                exposure: Some("direct".into()),
            }),
            arguments: Some(exact_json(r#"{"command":"make test"}"#)),
            approval_kind: Some(ApprovalKind::Tool),
            event: None,
            is_error: Some(false),
            error: None,
        };

        assert_eq!(
            runtime_codec::encode_value(&approved).unwrap(),
            exact_json(
                r#"{"turnId":"turn-1","toolUseId":"t1","toolName":"bash","phase":"approved","source":{"kind":"shell","exposure":"direct"},"arguments":{"command":"make test"},"approvalKind":"tool","isError":false}"#
            )
        );

        let progress = ToolLifecycleRecord {
            turn_id: "turn-1".into(),
            tool_use_id: "t1".into(),
            tool_name: "bash".into(),
            phase: ToolLifecyclePhase::Progress,
            source: None,
            arguments: None,
            approval_kind: None,
            event: Some(exact_json(r#"{"type":"status","message":"running"}"#)),
            is_error: None,
            error: None,
        };

        assert_eq!(
            runtime_codec::encode_value(&progress).unwrap(),
            exact_json(
                r#"{"turnId":"turn-1","toolUseId":"t1","toolName":"bash","phase":"progress","event":{"type":"status","message":"running"}}"#
            )
        );
    }
}

#[cfg(test)]
mod reasoning_identity_tests {
    use super::*;

    #[test]
    fn optional_reasoning_identity_round_trips_without_changing_legacy_wire() {
        let old = crate::runtime_values::runtime_json::parse(
            &r#"{"type":"thinking.delta","text":"partial"}"#.into(),
        )
        .unwrap();
        let event: EngineEvent = runtime_codec::decode_value(&old).unwrap();
        assert_eq!(runtime_codec::encode_value(&event).unwrap(), old);
        let identified = crate::runtime_values::runtime_json::parse(
            &r#"{"type":"thinking.delta","text":"partial","messageId":"assistant-0"}"#.into(),
        )
        .unwrap();
        let event: EngineEvent = runtime_codec::decode_value(&identified).unwrap();
        assert!(
            matches!(&event, EngineEvent::ThinkingDelta { message_id:Some(id), .. } if id == "assistant-0")
        );
        assert_eq!(runtime_codec::encode_value(&event).unwrap(), identified);
        for raw in [
            r#"{"text":"reasoned","signature":"signed"}"#,
            r#"{"text":"reasoned","messageId":"assistant-reasoning-0","signature":"signed"}"#,
        ] {
            let value = crate::runtime_values::runtime_json::parse(&raw.into()).unwrap();
            let block: crate::schemas::content::ThinkingBlock =
                runtime_codec::decode_value(&value).unwrap();
            assert_eq!(runtime_codec::encode_value(&block).unwrap(), value);
        }
    }
}
