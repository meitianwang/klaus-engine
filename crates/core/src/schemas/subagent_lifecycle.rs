//! Sub-agent lifecycle record schema: the persisted rollout line payloads and
//! the host-facing snapshot view. The engine-side registry that maintains these
//! records lives in `crate::engine::subagent_lifecycle`.

use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};
use serde::{Deserialize, Serialize};

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

pub const RECOVERY_INTERRUPTED_REASON: &str =
    "sub-agent execution was interrupted because the parent session restarted";

/// One sub-agent lifecycle record, suitable for runtime snapshots and recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubagentLifecycleRecord {
    pub agent_id: RuntimeString,
    pub kind: SubagentKind,
    pub status: SubagentStatus,
    pub context: SubagentContext,
    pub started_at_ms: i64,
    pub updated_at_ms: i64,
    pub completed_at_ms: Option<i64>,
    pub last_result: Option<RuntimeString>,
    pub last_error: Option<RuntimeString>,
    pub cancellation_requested: bool,
    /// The concrete model this child actually runs on, resolved at spawn time
    /// (tier tokens and definition defaults already applied). Absent on rows
    /// written before the field existed and on spawns whose inference bundle
    /// could not be resolved.
    pub model: Option<RuntimeString>,
    /// The model source (the product's model mode) the resolved model belongs
    /// to, taken from the child bundle's inference attribution. Absent when
    /// the bundle carries no attribution.
    pub model_source: Option<RuntimeString>,
}

impl SubagentLifecycleRecord {
    pub fn new(
        agent_id: impl Into<RuntimeString>,
        kind: SubagentKind,
        context: SubagentContext,
        now_ms: i64,
    ) -> Self {
        Self {
            agent_id: agent_id.into(),
            kind,
            status: SubagentStatus::Spawned,
            context,
            started_at_ms: now_ms,
            updated_at_ms: now_ms,
            completed_at_ms: None,
            last_result: None,
            last_error: None,
            cancellation_requested: false,
            model: None,
            model_source: None,
        }
    }
}

impl RuntimeEncode for SubagentLifecycleRecord {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("agentId", &self.agent_id)?;
        object.field("kind", &self.kind)?;
        object.field("status", &self.status)?;
        object.field("context", &self.context)?;
        object.field("startedAtMs", &self.started_at_ms)?;
        object.field("updatedAtMs", &self.updated_at_ms)?;
        let completed_at_ms = self.completed_at_ms;
        object.field("completedAtMs", &completed_at_ms)?;
        object.field("lastResult", &self.last_result)?;
        object.field("lastError", &self.last_error)?;
        object.field("cancellationRequested", &self.cancellation_requested)?;
        object.field("model", &self.model)?;
        object.field("modelSource", &self.model_source)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for SubagentLifecycleRecord {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                agent_id: object.required("agentId", &[])?,
                kind: object.required("kind", &[])?,
                status: object.required("status", &[])?,
                context: object.required("context", &[])?,
                started_at_ms: object.required("startedAtMs", &[])?,
                updated_at_ms: object.required("updatedAtMs", &[])?,
                completed_at_ms: object.optional("completedAtMs", &[])?,
                last_result: object.optional("lastResult", &[])?,
                last_error: object.optional("lastError", &[])?,
                cancellation_requested: object.required("cancellationRequested", &[])?,
                model: object.optional("model", &[])?,
                model_source: object.optional("modelSource", &[])?,
            })
        })
    }
}

/// Runtime family for one sub-agent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SubagentKind {
    Foreground,
    Background,
    Persistent,
    Workflow,
}

impl RuntimeEncode for SubagentKind {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Foreground => "foreground",
            Self::Background => "background",
            Self::Persistent => "persistent",
            Self::Workflow => "workflow",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for SubagentKind {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("foreground", &[]),
                RuntimeEnumVariant::new("background", &[]),
                RuntimeEnumVariant::new("persistent", &[]),
                RuntimeEnumVariant::new("workflow", &[]),
            ],
            &[
                Self::Foreground,
                Self::Background,
                Self::Persistent,
                Self::Workflow,
            ],
        )
    }
}

/// Engine-visible lifecycle status for one sub-agent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SubagentStatus {
    Spawned,
    Running,
    Idle,
    /// The owning process/session ended while the agent was still live.
    Interrupted,
    Completed,
    /// The child stopped on a configured budget (turn or spend ceiling) with
    /// usable partial work. Not a failure: the stop was planned, and the
    /// parent keeps the child's final text.
    BudgetLimited,
    Failed,
    Cancelled,
}

impl RuntimeEncode for SubagentStatus {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Spawned => "spawned",
            Self::Running => "running",
            Self::Idle => "idle",
            Self::Interrupted => "interrupted",
            Self::Completed => "completed",
            Self::BudgetLimited => "budget_limited",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for SubagentStatus {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("spawned", &[]),
                RuntimeEnumVariant::new("running", &[]),
                RuntimeEnumVariant::new("idle", &[]),
                RuntimeEnumVariant::new("interrupted", &[]),
                RuntimeEnumVariant::new("completed", &[]),
                RuntimeEnumVariant::new("budget_limited", &[]),
                RuntimeEnumVariant::new("failed", &[]),
                RuntimeEnumVariant::new("cancelled", &[]),
            ],
            &[
                Self::Spawned,
                Self::Running,
                Self::Idle,
                Self::Interrupted,
                Self::Completed,
                Self::BudgetLimited,
                Self::Failed,
                Self::Cancelled,
            ],
        )
    }
}

impl SubagentStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            SubagentStatus::Interrupted
                | SubagentStatus::Completed
                | SubagentStatus::BudgetLimited
                | SubagentStatus::Failed
                | SubagentStatus::Cancelled
        )
    }
}

/// Spawn-time context inherited from the parent session/agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubagentContext {
    pub session_id: RuntimeString,
    pub parent_agent_id: Option<RuntimeString>,
    pub parent_thread_id: Option<RuntimeString>,
    pub agent_type: Option<RuntimeString>,
    pub cwd: RuntimeString,
    pub description: RuntimeString,
    pub depth: u32,
    pub resume_from: Option<RuntimeString>,
    /// The turn of the receiving session that spawned this child, stamped at
    /// spawn time. Lifecycle rows replay into that turn even when the child
    /// settles after the turn closed; a record without a resolvable owner is
    /// dropped on replay instead of being stranded at the transcript tail.
    pub spawn_turn_id: Option<RuntimeString>,
}

impl RuntimeEncode for SubagentContext {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("sessionId", &self.session_id)?;
        object.field("parentAgentId", &self.parent_agent_id)?;
        object.field("parentThreadId", &self.parent_thread_id)?;
        object.field("agentType", &self.agent_type)?;
        object.field("cwd", &self.cwd)?;
        object.field("description", &self.description)?;
        object.field("depth", &self.depth)?;
        object.field("resumeFrom", &self.resume_from)?;
        object.field("spawnTurnId", &self.spawn_turn_id)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for SubagentContext {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                session_id: object.required("sessionId", &[])?,
                parent_agent_id: object.optional("parentAgentId", &[])?,
                parent_thread_id: object.optional("parentThreadId", &[])?,
                agent_type: object.optional("agentType", &[])?,
                cwd: object.required("cwd", &[])?,
                description: object.required("description", &[])?,
                depth: object.required("depth", &[])?,
                resume_from: object.optional("resumeFrom", &[])?,
                spawn_turn_id: object.optional("spawnTurnId", &[])?,
            })
        })
    }
}

/// Stable snapshot view returned to hosts/tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubagentSnapshot {
    pub agents: Vec<SubagentLifecycleRecord>,
}

impl RuntimeEncode for SubagentSnapshot {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("agents", &self.agents)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for SubagentSnapshot {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                agents: object.required("agents", &[])?,
            })
        })
    }
}
