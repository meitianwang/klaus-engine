//! Authoritative lifecycle records for automatic memory maintenance.
//!
//! Every transition is both retained by the live Session and persisted as a
//! rollout event. Protocol hosts may therefore subscribe for live updates and
//! query the same current state after event loss or session resume.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use crate::memory::agent_memory::AgentMemoryScope;
use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};

fn decode_unit_enum<T: Copy>(
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryMaintenanceStatus {
    Started,
    Completed,
    Failed,
    Skipped,
}

impl MemoryMaintenanceStatus {
    fn is_terminal(self) -> bool {
        self != Self::Started
    }
}

fn invalid_maintenance_state(message: &'static str) -> RuntimeCodecError {
    RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(RuntimeString::from(message)))
}

impl RuntimeEncode for MemoryMaintenanceStatus {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(
            out,
            match self {
                Self::Started => "started",
                Self::Completed => "completed",
                Self::Failed => "failed",
                Self::Skipped => "skipped",
            },
        )
    }
}

impl RuntimeDecode for MemoryMaintenanceStatus {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("started", &[]),
                RuntimeEnumVariant::new("completed", &[]),
                RuntimeEnumVariant::new("failed", &[]),
                RuntimeEnumVariant::new("skipped", &[]),
            ],
            &[Self::Started, Self::Completed, Self::Failed, Self::Skipped],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryMaintenanceTrigger {
    Auto,
}

impl RuntimeEncode for MemoryMaintenanceTrigger {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(out, "auto")
    }
}

impl RuntimeDecode for MemoryMaintenanceTrigger {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_unit_enum(
            value,
            &[RuntimeEnumVariant::new("auto", &[])],
            &[Self::Auto],
        )
    }
}

/// Stable classification for a reconnect-safe maintenance query failure.
///
/// A persistence inconsistency is terminal for the live Session: returning the
/// last visible snapshot as though it were authoritative would hide an admitted
/// maintenance effect whose terminal transition could not be made durable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MemoryMaintenanceQueryErrorCode {
    PersistenceInconsistent,
}

impl RuntimeEncode for MemoryMaintenanceQueryErrorCode {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(
            out,
            match self {
                Self::PersistenceInconsistent => "memory_maintenance_persistence_inconsistent",
            },
        )
    }
}

impl RuntimeDecode for MemoryMaintenanceQueryErrorCode {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_unit_enum(
            value,
            &[RuntimeEnumVariant::new(
                "memory_maintenance_persistence_inconsistent",
                &[],
            )],
            &[Self::PersistenceInconsistent],
        )
    }
}

/// The automatic maintenance lifecycle whose durable state became uncertain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MemoryMaintenanceRunKind {
    AutoMemory,
    AutoDream,
}

impl RuntimeEncode for MemoryMaintenanceRunKind {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(
            out,
            match self {
                Self::AutoMemory => "auto_memory",
                Self::AutoDream => "auto_dream",
            },
        )
    }
}

impl RuntimeDecode for MemoryMaintenanceRunKind {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("auto_memory", &[]),
                RuntimeEnumVariant::new("auto_dream", &[]),
            ],
            &[Self::AutoMemory, Self::AutoDream],
        )
    }
}

/// Non-secret machine-readable metadata carried by a failed maintenance query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryMaintenanceQueryErrorDetail {
    pub code: MemoryMaintenanceQueryErrorCode,
    pub run_kind: Option<MemoryMaintenanceRunKind>,
    pub run_id: Option<RuntimeString>,
}

impl RuntimeEncode for MemoryMaintenanceQueryErrorDetail {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("code", &self.code)?;
        if let Some(value) = self.run_kind {
            object.field("runKind", &value)?;
        }
        if let Some(value) = &self.run_id {
            object.field("runId", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for MemoryMaintenanceQueryErrorDetail {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                code: object.required("code", &[])?,
                run_kind: object.optional("runKind", &[])?,
                run_id: object.optional("runId", &[])?,
            })
        })
    }
}

/// A fail-closed authoritative-state query failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryMaintenanceQueryError {
    pub message: RuntimeString,
    pub detail: MemoryMaintenanceQueryErrorDetail,
}

impl MemoryMaintenanceQueryError {
    pub fn persistence_inconsistent(
        message: impl Into<RuntimeString>,
        run_kind: Option<MemoryMaintenanceRunKind>,
        run_id: Option<RuntimeString>,
    ) -> Self {
        Self {
            message: message.into(),
            detail: MemoryMaintenanceQueryErrorDetail {
                code: MemoryMaintenanceQueryErrorCode::PersistenceInconsistent,
                run_kind,
                run_id,
            },
        }
    }
}

impl RuntimeEncode for MemoryMaintenanceQueryError {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("message", &self.message)?;
        object.field("detail", &self.detail)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for MemoryMaintenanceQueryError {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                message: object.required("message", &[])?,
                detail: object.required("detail", &[])?,
            })
        })
    }
}

impl fmt::Display for MemoryMaintenanceQueryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = self.message.to_node_utf8();
        formatter.write_str(
            std::str::from_utf8(&bytes).expect("Node UTF-8 projection is always valid UTF-8"),
        )
    }
}

impl std::error::Error for MemoryMaintenanceQueryError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoMemoryRunReason {
    Disabled,
    NoMemoryDirectory,
    EmptyInput,
    DirectMemoryWrite,
    AlreadyRunning,
    Shutdown,
    Interrupted,
    ToolFailure,
    ForkFailure,
}

impl RuntimeEncode for AutoMemoryRunReason {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(
            out,
            match self {
                Self::Disabled => "disabled",
                Self::NoMemoryDirectory => "no-memory-directory",
                Self::EmptyInput => "empty-input",
                Self::DirectMemoryWrite => "direct-memory-write",
                Self::AlreadyRunning => "already-running",
                Self::Shutdown => "shutdown",
                Self::Interrupted => "interrupted",
                Self::ToolFailure => "tool-failure",
                Self::ForkFailure => "fork-failure",
            },
        )
    }
}

impl RuntimeDecode for AutoMemoryRunReason {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("disabled", &[]),
                RuntimeEnumVariant::new("no-memory-directory", &[]),
                RuntimeEnumVariant::new("empty-input", &[]),
                RuntimeEnumVariant::new("direct-memory-write", &[]),
                RuntimeEnumVariant::new("already-running", &[]),
                RuntimeEnumVariant::new("shutdown", &[]),
                RuntimeEnumVariant::new("interrupted", &[]),
                RuntimeEnumVariant::new("tool-failure", &[]),
                RuntimeEnumVariant::new("fork-failure", &[]),
            ],
            &[
                Self::Disabled,
                Self::NoMemoryDirectory,
                Self::EmptyInput,
                Self::DirectMemoryWrite,
                Self::AlreadyRunning,
                Self::Shutdown,
                Self::Interrupted,
                Self::ToolFailure,
                Self::ForkFailure,
            ],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoDreamRunReason {
    Disabled,
    NoMemoryDirectory,
    NoSessionStore,
    AlreadyRunning,
    ScanThrottle,
    TimeGate,
    SessionGate,
    EmptyInput,
    LockHeld,
    LockError,
    StoreError,
    Shutdown,
    Interrupted,
    ToolFailure,
    ForkFailure,
}

impl RuntimeEncode for AutoDreamRunReason {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(
            out,
            match self {
                Self::Disabled => "disabled",
                Self::NoMemoryDirectory => "no-memory-directory",
                Self::NoSessionStore => "no-session-store",
                Self::AlreadyRunning => "already-running",
                Self::ScanThrottle => "scan-throttle",
                Self::TimeGate => "time-gate",
                Self::SessionGate => "session-gate",
                Self::EmptyInput => "empty-input",
                Self::LockHeld => "lock-held",
                Self::LockError => "lock-error",
                Self::StoreError => "store-error",
                Self::Shutdown => "shutdown",
                Self::Interrupted => "interrupted",
                Self::ToolFailure => "tool-failure",
                Self::ForkFailure => "fork-failure",
            },
        )
    }
}

impl RuntimeDecode for AutoDreamRunReason {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("disabled", &[]),
                RuntimeEnumVariant::new("no-memory-directory", &[]),
                RuntimeEnumVariant::new("no-session-store", &[]),
                RuntimeEnumVariant::new("already-running", &[]),
                RuntimeEnumVariant::new("scan-throttle", &[]),
                RuntimeEnumVariant::new("time-gate", &[]),
                RuntimeEnumVariant::new("session-gate", &[]),
                RuntimeEnumVariant::new("empty-input", &[]),
                RuntimeEnumVariant::new("lock-held", &[]),
                RuntimeEnumVariant::new("lock-error", &[]),
                RuntimeEnumVariant::new("store-error", &[]),
                RuntimeEnumVariant::new("shutdown", &[]),
                RuntimeEnumVariant::new("interrupted", &[]),
                RuntimeEnumVariant::new("tool-failure", &[]),
                RuntimeEnumVariant::new("fork-failure", &[]),
            ],
            &[
                Self::Disabled,
                Self::NoMemoryDirectory,
                Self::NoSessionStore,
                Self::AlreadyRunning,
                Self::ScanThrottle,
                Self::TimeGate,
                Self::SessionGate,
                Self::EmptyInput,
                Self::LockHeld,
                Self::LockError,
                Self::StoreError,
                Self::Shutdown,
                Self::Interrupted,
                Self::ToolFailure,
                Self::ForkFailure,
            ],
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AutoMemoryRun {
    pub run_id: RuntimeString,
    pub session_id: RuntimeString,
    pub trigger: MemoryMaintenanceTrigger,
    pub scope: AgentMemoryScope,
    pub target_dir: RuntimeString,
    pub status: MemoryMaintenanceStatus,
    pub started_at: i64,
    pub ended_at: Option<i64>,
    pub reason: Option<AutoMemoryRunReason>,
    pub error: Option<RuntimeString>,
    pub message_count: u64,
    pub cursor_message_uuid: Option<RuntimeString>,
    pub files_touched: Vec<RuntimeString>,
    pub turn_count: Option<u64>,
}

impl AutoMemoryRun {
    /// Lifecycle timestamps come from the wall clock, which may move backwards
    /// while a background run is active. Keep the persisted lifecycle ordered
    /// without rejecting an otherwise authoritative terminal transition.
    pub(crate) fn terminal_timestamp(&self, candidate: i64) -> i64 {
        candidate.max(self.started_at)
    }

    fn validate_lifecycle_shape(&self) -> Result<(), RuntimeCodecError> {
        if self
            .ended_at
            .is_some_and(|ended_at| ended_at < self.started_at)
        {
            return Err(invalid_maintenance_state(
                "Auto Memory endedAt cannot precede startedAt",
            ));
        }
        match self.status {
            MemoryMaintenanceStatus::Started => {
                if self.ended_at.is_some() || self.reason.is_some() || self.error.is_some() {
                    return Err(invalid_maintenance_state(
                        "Auto Memory started run cannot contain terminal fields",
                    ));
                }
            }
            MemoryMaintenanceStatus::Completed => {
                if self.ended_at.is_none() || self.reason.is_some() || self.error.is_some() {
                    return Err(invalid_maintenance_state(
                        "Auto Memory completed run must contain endedAt without failure fields",
                    ));
                }
            }
            MemoryMaintenanceStatus::Failed => {
                if self.ended_at.is_none() || self.reason.is_none() || self.error.is_none() {
                    return Err(invalid_maintenance_state(
                        "Auto Memory failed run must contain endedAt, reason, and error",
                    ));
                }
            }
            MemoryMaintenanceStatus::Skipped => {
                if self.ended_at.is_none() || self.reason.is_none() || self.error.is_some() {
                    return Err(invalid_maintenance_state(
                        "Auto Memory skipped run must contain endedAt and reason only",
                    ));
                }
            }
        }
        Ok(())
    }

    fn same_identity(&self, next: &Self) -> bool {
        self.run_id == next.run_id
            && self.session_id == next.session_id
            && self.trigger == next.trigger
            && self.scope == next.scope
            && self.target_dir == next.target_dir
            && self.started_at == next.started_at
            && self.message_count == next.message_count
    }
}

impl RuntimeEncode for AutoMemoryRun {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("runId", &self.run_id)?;
        object.field("sessionId", &self.session_id)?;
        object.field("trigger", &self.trigger)?;
        object.field("scope", &self.scope)?;
        object.field("targetDir", &self.target_dir)?;
        object.field("status", &self.status)?;
        object.field("startedAt", &self.started_at)?;
        if let Some(value) = self.ended_at {
            object.field("endedAt", &value)?;
        }
        if let Some(value) = self.reason {
            object.field("reason", &value)?;
        }
        if let Some(value) = &self.error {
            object.field("error", value)?;
        }
        object.field("messageCount", &self.message_count)?;
        if let Some(value) = &self.cursor_message_uuid {
            object.field("cursorMessageUuid", value)?;
        }
        object.field("filesTouched", &self.files_touched)?;
        if let Some(value) = self.turn_count {
            object.field("turnCount", &value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for AutoMemoryRun {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            let run = Self {
                run_id: object.required("runId", &[])?,
                session_id: object.required("sessionId", &[])?,
                trigger: object.required("trigger", &[])?,
                scope: object.required("scope", &[])?,
                target_dir: object.required("targetDir", &[])?,
                status: object.required("status", &[])?,
                started_at: object.required("startedAt", &[])?,
                ended_at: object.optional("endedAt", &[])?,
                reason: object.optional("reason", &[])?,
                error: object.optional("error", &[])?,
                message_count: object.required("messageCount", &[])?,
                cursor_message_uuid: object.optional("cursorMessageUuid", &[])?,
                files_touched: object.defaulted("filesTouched", &[], Vec::new)?,
                turn_count: object.optional("turnCount", &[])?,
            };
            run.validate_lifecycle_shape()?;
            Ok(run)
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemoryMaintenanceTurn {
    pub text: RuntimeString,
    pub tool_use_count: u64,
}

impl RuntimeEncode for MemoryMaintenanceTurn {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("text", &self.text)?;
        object.field("toolUseCount", &self.tool_use_count)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for MemoryMaintenanceTurn {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                text: object.required("text", &[])?,
                tool_use_count: object.required("toolUseCount", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AutoDreamRun {
    pub dream_run_id: RuntimeString,
    pub session_id: RuntimeString,
    pub trigger: MemoryMaintenanceTrigger,
    pub status: MemoryMaintenanceStatus,
    pub started_at: i64,
    pub ended_at: Option<i64>,
    pub reason: Option<AutoDreamRunReason>,
    pub error: Option<RuntimeString>,
    pub last_consolidated_at: Option<i64>,
    pub sessions_reviewing: u64,
    pub files_touched: Vec<RuntimeString>,
    /// Most recent bounded assistant turns, matching the Desktop Dream card.
    pub turns: Vec<MemoryMaintenanceTurn>,
    /// Total assistant turn count even when `turns` has been bounded.
    pub turn_count: u64,
    /// Parsed `<dream-summary>` marker from the newest assistant turn that
    /// emitted one. Absent for failed/skipped runs or non-conforming output.
    pub summary: Option<RuntimeString>,
}

impl AutoDreamRun {
    /// See [`AutoMemoryRun::terminal_timestamp`].
    pub(crate) fn terminal_timestamp(&self, candidate: i64) -> i64 {
        candidate.max(self.started_at)
    }

    fn validate_lifecycle_shape(&self) -> Result<(), RuntimeCodecError> {
        if self
            .ended_at
            .is_some_and(|ended_at| ended_at < self.started_at)
        {
            return Err(invalid_maintenance_state(
                "Auto Dream endedAt cannot precede startedAt",
            ));
        }
        match self.status {
            MemoryMaintenanceStatus::Started => {
                if self.ended_at.is_some()
                    || self.reason.is_some()
                    || self.error.is_some()
                    || self.summary.is_some()
                {
                    return Err(invalid_maintenance_state(
                        "Auto Dream started run cannot contain terminal fields",
                    ));
                }
            }
            MemoryMaintenanceStatus::Completed => {
                if self.ended_at.is_none() || self.reason.is_some() || self.error.is_some() {
                    return Err(invalid_maintenance_state(
                        "Auto Dream completed run must contain endedAt without failure fields",
                    ));
                }
            }
            MemoryMaintenanceStatus::Failed => {
                if self.ended_at.is_none()
                    || self.reason.is_none()
                    || self.error.is_none()
                    || self.summary.is_some()
                {
                    return Err(invalid_maintenance_state(
                        "Auto Dream failed run must contain endedAt, reason, and error",
                    ));
                }
            }
            MemoryMaintenanceStatus::Skipped => {
                if self.ended_at.is_none()
                    || self.reason.is_none()
                    || self.error.is_some()
                    || self.summary.is_some()
                {
                    return Err(invalid_maintenance_state(
                        "Auto Dream skipped run must contain endedAt and reason only",
                    ));
                }
            }
        }
        Ok(())
    }

    fn same_identity(&self, next: &Self) -> bool {
        self.dream_run_id == next.dream_run_id
            && self.session_id == next.session_id
            && self.trigger == next.trigger
            && self.started_at == next.started_at
    }
}

impl RuntimeEncode for AutoDreamRun {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("dreamRunId", &self.dream_run_id)?;
        object.field("sessionId", &self.session_id)?;
        object.field("trigger", &self.trigger)?;
        object.field("status", &self.status)?;
        object.field("startedAt", &self.started_at)?;
        if let Some(value) = self.ended_at {
            object.field("endedAt", &value)?;
        }
        if let Some(value) = self.reason {
            object.field("reason", &value)?;
        }
        if let Some(value) = &self.error {
            object.field("error", value)?;
        }
        if let Some(value) = self.last_consolidated_at {
            object.field("lastConsolidatedAt", &value)?;
        }
        object.field("sessionsReviewing", &self.sessions_reviewing)?;
        object.field("filesTouched", &self.files_touched)?;
        object.field("turns", &self.turns)?;
        object.field("turnCount", &self.turn_count)?;
        if let Some(value) = &self.summary {
            object.field("summary", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for AutoDreamRun {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            let run = Self {
                dream_run_id: object.required("dreamRunId", &[])?,
                session_id: object.required("sessionId", &[])?,
                trigger: object.required("trigger", &[])?,
                status: object.required("status", &[])?,
                started_at: object.required("startedAt", &[])?,
                ended_at: object.optional("endedAt", &[])?,
                reason: object.optional("reason", &[])?,
                error: object.optional("error", &[])?,
                last_consolidated_at: object.optional("lastConsolidatedAt", &[])?,
                sessions_reviewing: object.required("sessionsReviewing", &[])?,
                files_touched: object.defaulted("filesTouched", &[], Vec::new)?,
                turns: object.defaulted("turns", &[], Vec::new)?,
                turn_count: object.required("turnCount", &[])?,
                summary: object.optional("summary", &[])?,
            };
            run.validate_lifecycle_shape()?;
            Ok(run)
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum MemoryMaintenanceEvent {
    AutoMemory { run: AutoMemoryRun },
    AutoDream { run: AutoDreamRun },
}

pub type MemoryMaintenanceEventSink = Arc<dyn Fn(MemoryMaintenanceEvent) + Send + Sync + 'static>;

pub const MEMORY_MAINTENANCE_ROLLOUT_EVENT: &str = "memory_maintenance";

impl RuntimeEncode for MemoryMaintenanceEvent {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        let kind = match self {
            Self::AutoMemory { run } => {
                object.field("run", run)?;
                "auto_memory"
            }
            Self::AutoDream { run } => {
                object.field("run", run)?;
                "auto_dream"
            }
        };
        runtime_codec::encode_internal(out, "kind", &[], kind, &object.finish())
    }
}

impl RuntimeDecode for MemoryMaintenanceEvent {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            false,
            "kind",
            &[],
            &[
                RuntimeEnumVariant::new("auto_memory", &[]),
                RuntimeEnumVariant::new("auto_dream", &[]),
            ],
            |variant, object| match variant {
                0 => Ok(Self::AutoMemory {
                    run: object.required("run", &[])?,
                }),
                1 => Ok(Self::AutoDream {
                    run: object.required("run", &[])?,
                }),
                _ => unreachable!("variant index is bounded"),
            },
        )
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct MemoryMaintenanceSnapshot {
    pub auto_memory_runs: Vec<AutoMemoryRun>,
    pub auto_dream_runs: Vec<AutoDreamRun>,
    /// Cursor after the latest Auto Memory transition in event order. This is
    /// independent of run-list presentation order, where an earlier Started run
    /// may complete after a later AlreadyRunning skip.
    pub auto_memory_cursor_message_uuid: Option<RuntimeString>,
}

/// Keep a useful reconnect/audit window without allowing a long-running Session
/// to retain an unbounded maintenance history.
const MAX_RUNS_PER_KIND: usize = 64;

impl MemoryMaintenanceSnapshot {
    /// Apply one authoritative transition. Repeated transitions for a run replace
    /// that run in-place so query ordering remains the order attempts began.
    pub fn apply(&mut self, event: &MemoryMaintenanceEvent) -> bool {
        match event {
            MemoryMaintenanceEvent::AutoMemory { run } => {
                let existing = self
                    .auto_memory_runs
                    .iter()
                    .find(|existing| existing.run_id == run.run_id);
                if !auto_memory_cursor_transition_is_valid(
                    self.auto_memory_cursor_message_uuid.as_ref(),
                    existing,
                    run,
                ) {
                    return false;
                }
                let accepted = upsert_run(
                    &mut self.auto_memory_runs,
                    &run.run_id,
                    run.clone(),
                    |run| &run.run_id,
                    |run| run.status,
                    AutoMemoryRun::validate_lifecycle_shape,
                    AutoMemoryRun::same_identity,
                );
                if accepted {
                    match (run.status, run.reason) {
                        (MemoryMaintenanceStatus::Completed, _)
                        | (
                            MemoryMaintenanceStatus::Skipped,
                            Some(AutoMemoryRunReason::DirectMemoryWrite),
                        ) => {
                            self.auto_memory_cursor_message_uuid = run.cursor_message_uuid.clone();
                        }
                        _ => {}
                    }
                }
                accepted
            }
            MemoryMaintenanceEvent::AutoDream { run } => upsert_run(
                &mut self.auto_dream_runs,
                &run.dream_run_id,
                run.clone(),
                |run| &run.dream_run_id,
                |run| run.status,
                AutoDreamRun::validate_lifecycle_shape,
                AutoDreamRun::same_identity,
            ),
        }
    }

    /// Cold-resume reconciliation for process-local work that cannot still be
    /// running. Return the synthesized terminal transitions so the caller can
    /// append them to the rollout before accepting new maintenance attempts.
    pub fn interrupt_nonterminal_for_recovery(&mut self, now: i64) -> Vec<MemoryMaintenanceEvent> {
        let mut transitions = Vec::new();
        for run in &mut self.auto_memory_runs {
            if run.status == MemoryMaintenanceStatus::Started {
                run.status = MemoryMaintenanceStatus::Failed;
                run.ended_at = Some(run.terminal_timestamp(now));
                run.reason = Some(AutoMemoryRunReason::Interrupted);
                run.error = Some(
                    "automatic memory extraction was interrupted because the session restarted"
                        .into(),
                );
                transitions.push(MemoryMaintenanceEvent::AutoMemory { run: run.clone() });
            }
        }
        for run in &mut self.auto_dream_runs {
            if run.status == MemoryMaintenanceStatus::Started {
                run.status = MemoryMaintenanceStatus::Failed;
                run.ended_at = Some(run.terminal_timestamp(now));
                run.reason = Some(AutoDreamRunReason::Interrupted);
                run.error = Some(
                    "automatic Dream consolidation was interrupted because the session restarted"
                        .into(),
                );
                transitions.push(MemoryMaintenanceEvent::AutoDream { run: run.clone() });
            }
        }
        transitions
    }
}

/// Validate the cursor portion of the Auto Memory state transition separately
/// from immutable run identity. A failed/interrupted attempt must retain the
/// cursor captured by Started, while only a completed extraction (or the
/// intentional direct-write skip) may advance it.
fn auto_memory_cursor_transition_is_valid(
    authoritative_cursor: Option<&RuntimeString>,
    existing: Option<&AutoMemoryRun>,
    next: &AutoMemoryRun,
) -> bool {
    if next.validate_lifecycle_shape().is_err() {
        return false;
    }
    let next_cursor = next.cursor_message_uuid.as_ref();
    let Some(existing) = existing else {
        return match (next.status, next.reason) {
            (MemoryMaintenanceStatus::Started, _) => next_cursor == authoritative_cursor,
            (MemoryMaintenanceStatus::Skipped, Some(AutoMemoryRunReason::DirectMemoryWrite)) => {
                next_cursor.is_some()
            }
            (MemoryMaintenanceStatus::Skipped, _) => next_cursor == authoritative_cursor,
            (MemoryMaintenanceStatus::Completed | MemoryMaintenanceStatus::Failed, _) => false,
        };
    };

    if existing.cursor_message_uuid.as_ref() != authoritative_cursor {
        return false;
    }
    match next.status {
        MemoryMaintenanceStatus::Completed => next_cursor.is_some(),
        MemoryMaintenanceStatus::Started | MemoryMaintenanceStatus::Failed => {
            next.cursor_message_uuid == existing.cursor_message_uuid
        }
        // Auto Memory only admits work after Started. Its worker can complete
        // or fail; every legitimate Skip is a pre-admission standalone event.
        MemoryMaintenanceStatus::Skipped => false,
    }
}

fn upsert_run<T>(
    runs: &mut Vec<T>,
    run_id: &RuntimeString,
    next: T,
    id: impl Fn(&T) -> &RuntimeString,
    status: impl Fn(&T) -> MemoryMaintenanceStatus,
    validate: impl Fn(&T) -> Result<(), RuntimeCodecError>,
    same_identity: impl Fn(&T, &T) -> bool,
) -> bool
where
    T: PartialEq,
{
    if validate(&next).is_err() {
        return false;
    }
    if let Some(existing) = runs.iter_mut().find(|run| id(run) == run_id) {
        if existing == &next {
            return false;
        }
        if !same_identity(existing, &next)
            || status(existing) != MemoryMaintenanceStatus::Started
            || !status(&next).is_terminal()
        {
            return false;
        }
        *existing = next;
        return true;
    }
    // A completed/failed run is never a standalone attempt. Accepting one
    // without its Started transition would let a truncated, forged, or
    // out-of-order replay advance query state (and Auto Memory's cursor)
    // without proving that core admitted the work. Gate-related Skipped runs
    // are intentionally standalone.
    if !matches!(
        status(&next),
        MemoryMaintenanceStatus::Started | MemoryMaintenanceStatus::Skipped
    ) {
        return false;
    }
    if runs.len() >= MAX_RUNS_PER_KIND {
        // A reconnect query must never lose the authoritative in-flight run
        // just because many later gate checks emitted terminal skips. Evict the
        // oldest retained terminal audit entry before accepting the new run.
        // If all 64 entries are Started, rejecting preserves the bounded query
        // contract and avoids reporting an event that was immediately dropped.
        if let Some(index) = runs
            .iter()
            .position(|run| status(run) != MemoryMaintenanceStatus::Started)
        {
            runs.remove(index);
        } else {
            return false;
        }
    }
    runs.push(next);
    true
}

impl RuntimeEncode for MemoryMaintenanceSnapshot {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("autoMemoryRuns", &self.auto_memory_runs)?;
        object.field("autoDreamRuns", &self.auto_dream_runs)?;
        if let Some(value) = &self.auto_memory_cursor_message_uuid {
            object.field("autoMemoryCursorMessageUuid", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for MemoryMaintenanceSnapshot {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            let snapshot = Self {
                auto_memory_runs: object.defaulted("autoMemoryRuns", &[], Vec::new)?,
                auto_dream_runs: object.defaulted("autoDreamRuns", &[], Vec::new)?,
                auto_memory_cursor_message_uuid: object
                    .optional("autoMemoryCursorMessageUuid", &[])?,
            };
            if snapshot.auto_memory_runs.len() > MAX_RUNS_PER_KIND
                || snapshot.auto_dream_runs.len() > MAX_RUNS_PER_KIND
            {
                return Err(invalid_maintenance_state(
                    "memory maintenance snapshot exceeds the retained run limit",
                ));
            }
            let mut memory_ids = BTreeSet::new();
            for run in &snapshot.auto_memory_runs {
                run.validate_lifecycle_shape()?;
                if !memory_ids.insert(run.run_id.clone()) {
                    return Err(invalid_maintenance_state(
                        "Auto Memory snapshot runId values must be unique",
                    ));
                }
            }
            if let Some(cursor) = &snapshot.auto_memory_cursor_message_uuid {
                if !snapshot
                    .auto_memory_runs
                    .iter()
                    .any(|run| run.cursor_message_uuid.as_ref() == Some(cursor))
                {
                    return Err(invalid_maintenance_state(
                        "Auto Memory snapshot cursor must come from a retained authoritative run",
                    ));
                }
            }
            let mut dream_ids = BTreeSet::new();
            for run in &snapshot.auto_dream_runs {
                run.validate_lifecycle_shape()?;
                if !dream_ids.insert(run.dream_run_id.clone()) {
                    return Err(invalid_maintenance_state(
                        "Auto Dream snapshot dreamRunId values must be unique",
                    ));
                }
            }
            Ok(snapshot)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seed_auto_memory_cursor(
        snapshot: &mut MemoryMaintenanceSnapshot,
        cursor: &str,
    ) -> AutoMemoryRun {
        let run = AutoMemoryRun {
            run_id: format!("cursor-seed-{cursor}").into(),
            session_id: "session-1".into(),
            trigger: MemoryMaintenanceTrigger::Auto,
            scope: AgentMemoryScope::Project,
            target_dir: "/memory".into(),
            status: MemoryMaintenanceStatus::Skipped,
            started_at: 0,
            ended_at: Some(0),
            reason: Some(AutoMemoryRunReason::DirectMemoryWrite),
            error: None,
            message_count: 1,
            cursor_message_uuid: Some(cursor.into()),
            files_touched: Vec::new(),
            turn_count: None,
        };
        assert!(snapshot.apply(&MemoryMaintenanceEvent::AutoMemory { run: run.clone() }));
        run
    }

    #[test]
    fn maintenance_query_error_round_trips_with_stable_typed_detail() {
        let error = MemoryMaintenanceQueryError::persistence_inconsistent(
            "automatic memory extraction terminal state could not be persisted",
            Some(MemoryMaintenanceRunKind::AutoMemory),
            Some("run-failed-terminal".into()),
        );

        let encoded = runtime_codec::encode_text(&error).unwrap();
        assert_eq!(
            encoded.try_to_utf8().unwrap(),
            r#"{"message":"automatic memory extraction terminal state could not be persisted","detail":{"code":"memory_maintenance_persistence_inconsistent","runKind":"auto_memory","runId":"run-failed-terminal"}}"#
        );
        assert_eq!(
            runtime_codec::decode_text::<MemoryMaintenanceQueryError>(&encoded).unwrap(),
            error
        );
        assert_eq!(
            error.to_string(),
            "automatic memory extraction terminal state could not be persisted"
        );
    }

    #[test]
    fn maintenance_events_round_trip_with_stable_names() {
        let event = MemoryMaintenanceEvent::AutoMemory {
            run: AutoMemoryRun {
                run_id: "run-1".into(),
                session_id: "session-1".into(),
                trigger: MemoryMaintenanceTrigger::Auto,
                scope: AgentMemoryScope::Project,
                target_dir: "/memory".into(),
                status: MemoryMaintenanceStatus::Failed,
                started_at: 10,
                ended_at: Some(20),
                reason: Some(AutoMemoryRunReason::ToolFailure),
                error: Some("write failed".into()),
                message_count: 3,
                cursor_message_uuid: None,
                files_touched: vec!["/memory/MEMORY.md".into()],
                turn_count: Some(2),
            },
        };
        let encoded = runtime_codec::encode_text(&event).unwrap();
        assert!(encoded.try_to_utf8().unwrap().contains("tool-failure"));
        assert_eq!(
            runtime_codec::decode_text::<MemoryMaintenanceEvent>(&encoded).unwrap(),
            event
        );
    }

    #[test]
    fn snapshot_replaces_transitions_and_bounds_audit_history() {
        let mut snapshot = MemoryMaintenanceSnapshot::default();
        let in_flight = AutoDreamRun {
            dream_run_id: "in-flight".into(),
            session_id: "session-1".into(),
            trigger: MemoryMaintenanceTrigger::Auto,
            status: MemoryMaintenanceStatus::Started,
            started_at: 0,
            ended_at: None,
            reason: None,
            error: None,
            last_consolidated_at: None,
            sessions_reviewing: 0,
            files_touched: Vec::new(),
            turns: Vec::new(),
            turn_count: 0,
            summary: None,
        };
        snapshot.apply(&MemoryMaintenanceEvent::AutoDream {
            run: in_flight.clone(),
        });
        for idx in 0..MAX_RUNS_PER_KIND {
            let event = MemoryMaintenanceEvent::AutoDream {
                run: AutoDreamRun {
                    dream_run_id: format!("run-{idx}").into(),
                    session_id: "session-1".into(),
                    trigger: MemoryMaintenanceTrigger::Auto,
                    status: MemoryMaintenanceStatus::Skipped,
                    started_at: idx as i64,
                    ended_at: Some(idx as i64),
                    reason: Some(AutoDreamRunReason::AlreadyRunning),
                    error: None,
                    last_consolidated_at: None,
                    sessions_reviewing: 0,
                    files_touched: Vec::new(),
                    turns: Vec::new(),
                    turn_count: 0,
                    summary: None,
                },
            };
            snapshot.apply(&event);
        }
        assert_eq!(snapshot.auto_dream_runs.len(), MAX_RUNS_PER_KIND);
        assert_eq!(snapshot.auto_dream_runs[0], in_flight);
        assert_eq!(snapshot.auto_dream_runs[1].dream_run_id, "run-1");

        let mut terminal = snapshot.auto_dream_runs[0].clone();
        terminal.status = MemoryMaintenanceStatus::Completed;
        terminal.ended_at = Some(100);
        snapshot.apply(&MemoryMaintenanceEvent::AutoDream { run: terminal });
        assert_eq!(snapshot.auto_dream_runs.len(), MAX_RUNS_PER_KIND);
        assert_eq!(
            snapshot.auto_dream_runs[0].status,
            MemoryMaintenanceStatus::Completed
        );
    }

    #[test]
    fn saturated_in_flight_window_rejects_unretainable_new_runs() {
        let mut snapshot = MemoryMaintenanceSnapshot::default();
        for index in 0..MAX_RUNS_PER_KIND {
            let run = AutoDreamRun {
                dream_run_id: format!("in-flight-{index}").into(),
                session_id: "session-1".into(),
                trigger: MemoryMaintenanceTrigger::Auto,
                status: MemoryMaintenanceStatus::Started,
                started_at: index as i64,
                ended_at: None,
                reason: None,
                error: None,
                last_consolidated_at: None,
                sessions_reviewing: 0,
                files_touched: Vec::new(),
                turns: Vec::new(),
                turn_count: 0,
                summary: None,
            };
            assert!(snapshot.apply(&MemoryMaintenanceEvent::AutoDream { run }));
        }

        let skipped = AutoDreamRun {
            dream_run_id: "unretainable-skip".into(),
            session_id: "session-1".into(),
            trigger: MemoryMaintenanceTrigger::Auto,
            status: MemoryMaintenanceStatus::Skipped,
            started_at: 100,
            ended_at: Some(100),
            reason: Some(AutoDreamRunReason::AlreadyRunning),
            error: None,
            last_consolidated_at: None,
            sessions_reviewing: 0,
            files_touched: Vec::new(),
            turns: Vec::new(),
            turn_count: 0,
            summary: None,
        };
        assert!(!snapshot.apply(&MemoryMaintenanceEvent::AutoDream { run: skipped }));

        let started = AutoDreamRun {
            dream_run_id: "unretainable-started".into(),
            session_id: "session-1".into(),
            trigger: MemoryMaintenanceTrigger::Auto,
            status: MemoryMaintenanceStatus::Started,
            started_at: 101,
            ended_at: None,
            reason: None,
            error: None,
            last_consolidated_at: None,
            sessions_reviewing: 0,
            files_touched: Vec::new(),
            turns: Vec::new(),
            turn_count: 0,
            summary: None,
        };
        assert!(!snapshot.apply(&MemoryMaintenanceEvent::AutoDream { run: started }));
        assert_eq!(snapshot.auto_dream_runs.len(), MAX_RUNS_PER_KIND);
        assert!(snapshot
            .auto_dream_runs
            .iter()
            .all(|run| run.status == MemoryMaintenanceStatus::Started));
    }

    #[test]
    fn authoritative_cursor_follows_transition_order_not_run_start_order() {
        let mut snapshot = MemoryMaintenanceSnapshot::default();
        seed_auto_memory_cursor(&mut snapshot, "cursor-0");
        let started = AutoMemoryRun {
            run_id: "active".into(),
            session_id: "session-1".into(),
            trigger: MemoryMaintenanceTrigger::Auto,
            scope: AgentMemoryScope::Project,
            target_dir: "/memory".into(),
            status: MemoryMaintenanceStatus::Started,
            started_at: 1,
            ended_at: None,
            reason: None,
            error: None,
            message_count: 1,
            cursor_message_uuid: Some("cursor-0".into()),
            files_touched: Vec::new(),
            turn_count: None,
        };
        snapshot.apply(&MemoryMaintenanceEvent::AutoMemory {
            run: started.clone(),
        });
        let mut skipped = started.clone();
        skipped.run_id = "later-skip".into();
        skipped.status = MemoryMaintenanceStatus::Skipped;
        skipped.ended_at = Some(2);
        skipped.reason = Some(AutoMemoryRunReason::AlreadyRunning);
        snapshot.apply(&MemoryMaintenanceEvent::AutoMemory { run: skipped });
        let mut completed = started;
        completed.status = MemoryMaintenanceStatus::Completed;
        completed.ended_at = Some(3);
        completed.cursor_message_uuid = Some("cursor-1".into());
        snapshot.apply(&MemoryMaintenanceEvent::AutoMemory { run: completed });

        assert_eq!(snapshot.auto_memory_runs[2].run_id, "later-skip");
        assert_eq!(
            snapshot.auto_memory_cursor_message_uuid,
            Some("cursor-1".into())
        );
    }

    #[test]
    fn auto_memory_cursor_moves_only_for_an_accepted_successful_handling() {
        let mut snapshot = MemoryMaintenanceSnapshot::default();
        seed_auto_memory_cursor(&mut snapshot, "cursor-before");
        let started = AutoMemoryRun {
            run_id: "cursor-run".into(),
            session_id: "session-1".into(),
            trigger: MemoryMaintenanceTrigger::Auto,
            scope: AgentMemoryScope::Project,
            target_dir: "/memory".into(),
            status: MemoryMaintenanceStatus::Started,
            started_at: 10,
            ended_at: None,
            reason: None,
            error: None,
            message_count: 1,
            cursor_message_uuid: Some("cursor-before".into()),
            files_touched: Vec::new(),
            turn_count: None,
        };
        assert!(snapshot.apply(&MemoryMaintenanceEvent::AutoMemory {
            run: started.clone(),
        }));
        assert_eq!(
            snapshot.auto_memory_cursor_message_uuid,
            Some("cursor-before".into()),
            "Started must retain the pre-existing authoritative cursor"
        );

        let mut forged_skip = started.clone();
        forged_skip.status = MemoryMaintenanceStatus::Skipped;
        forged_skip.ended_at = Some(15);
        forged_skip.reason = Some(AutoMemoryRunReason::DirectMemoryWrite);
        assert!(!snapshot.apply(&MemoryMaintenanceEvent::AutoMemory { run: forged_skip }));

        let mut forged_failure = started.clone();
        forged_failure.status = MemoryMaintenanceStatus::Failed;
        forged_failure.ended_at = Some(20);
        forged_failure.reason = Some(AutoMemoryRunReason::ToolFailure);
        forged_failure.error = Some("write failed".into());
        forged_failure.cursor_message_uuid = Some("cursor-after-failure".into());
        assert!(!snapshot.apply(&MemoryMaintenanceEvent::AutoMemory {
            run: forged_failure,
        }));
        assert_eq!(
            snapshot.auto_memory_runs[1].status,
            MemoryMaintenanceStatus::Started
        );
        assert_eq!(
            snapshot.auto_memory_cursor_message_uuid,
            Some("cursor-before".into())
        );

        let mut completed = started;
        completed.status = MemoryMaintenanceStatus::Completed;
        completed.ended_at = Some(20);
        completed.cursor_message_uuid = Some("cursor-after-success".into());
        assert!(snapshot.apply(&MemoryMaintenanceEvent::AutoMemory { run: completed }));
        assert_eq!(
            snapshot.auto_memory_cursor_message_uuid,
            Some("cursor-after-success".into())
        );

        let mut ordinary_skip = snapshot.auto_memory_runs[0].clone();
        ordinary_skip.run_id = "ordinary-skip".into();
        ordinary_skip.status = MemoryMaintenanceStatus::Skipped;
        ordinary_skip.started_at = 30;
        ordinary_skip.ended_at = Some(30);
        ordinary_skip.reason = Some(AutoMemoryRunReason::AlreadyRunning);
        ordinary_skip.cursor_message_uuid = Some("forged-skip-cursor".into());
        ordinary_skip.files_touched.clear();
        ordinary_skip.turn_count = None;
        assert!(!snapshot.apply(&MemoryMaintenanceEvent::AutoMemory { run: ordinary_skip }));

        let direct_write = AutoMemoryRun {
            run_id: "direct-write".into(),
            session_id: "session-1".into(),
            trigger: MemoryMaintenanceTrigger::Auto,
            scope: AgentMemoryScope::Project,
            target_dir: "/memory".into(),
            status: MemoryMaintenanceStatus::Skipped,
            started_at: 40,
            ended_at: Some(40),
            reason: Some(AutoMemoryRunReason::DirectMemoryWrite),
            error: None,
            message_count: 1,
            cursor_message_uuid: Some("cursor-after-direct-write".into()),
            files_touched: Vec::new(),
            turn_count: None,
        };
        assert!(snapshot.apply(&MemoryMaintenanceEvent::AutoMemory { run: direct_write }));
        assert_eq!(
            snapshot.auto_memory_cursor_message_uuid,
            Some("cursor-after-direct-write".into())
        );
    }

    #[test]
    fn terminal_runs_reject_stale_or_identity_conflicting_replay() {
        let mut snapshot = MemoryMaintenanceSnapshot::default();
        let cursor_seed = seed_auto_memory_cursor(&mut snapshot, "cursor-before");
        let started_memory = AutoMemoryRun {
            run_id: "memory-run".into(),
            session_id: "session-1".into(),
            trigger: MemoryMaintenanceTrigger::Auto,
            scope: AgentMemoryScope::Project,
            target_dir: "/memory".into(),
            status: MemoryMaintenanceStatus::Started,
            started_at: 10,
            ended_at: None,
            reason: None,
            error: None,
            message_count: 2,
            cursor_message_uuid: Some("cursor-before".into()),
            files_touched: Vec::new(),
            turn_count: None,
        };
        snapshot.apply(&MemoryMaintenanceEvent::AutoMemory {
            run: started_memory.clone(),
        });
        let mut completed_memory = started_memory.clone();
        completed_memory.status = MemoryMaintenanceStatus::Completed;
        completed_memory.ended_at = Some(20);
        completed_memory.cursor_message_uuid = Some("cursor-after".into());
        completed_memory.files_touched = vec!["/memory/MEMORY.md".into()];
        completed_memory.turn_count = Some(1);
        snapshot.apply(&MemoryMaintenanceEvent::AutoMemory {
            run: completed_memory.clone(),
        });

        snapshot.apply(&MemoryMaintenanceEvent::AutoMemory {
            run: started_memory,
        });
        let mut forged_memory = completed_memory.clone();
        forged_memory.session_id = "forged-session".into();
        forged_memory.status = MemoryMaintenanceStatus::Failed;
        forged_memory.reason = Some(AutoMemoryRunReason::ToolFailure);
        forged_memory.error = Some("forged failure".into());
        snapshot.apply(&MemoryMaintenanceEvent::AutoMemory { run: forged_memory });

        assert_eq!(
            snapshot.auto_memory_runs,
            vec![cursor_seed, completed_memory]
        );
        assert_eq!(
            snapshot.auto_memory_cursor_message_uuid,
            Some("cursor-after".into()),
            "a rejected stale transition must not roll back the authoritative cursor"
        );

        let started_dream = AutoDreamRun {
            dream_run_id: "dream-run".into(),
            session_id: "session-1".into(),
            trigger: MemoryMaintenanceTrigger::Auto,
            status: MemoryMaintenanceStatus::Started,
            started_at: 30,
            ended_at: None,
            reason: None,
            error: None,
            last_consolidated_at: None,
            sessions_reviewing: 0,
            files_touched: Vec::new(),
            turns: Vec::new(),
            turn_count: 0,
            summary: None,
        };
        snapshot.apply(&MemoryMaintenanceEvent::AutoDream {
            run: started_dream.clone(),
        });
        let mut completed_dream = started_dream.clone();
        completed_dream.status = MemoryMaintenanceStatus::Completed;
        completed_dream.ended_at = Some(40);
        completed_dream.last_consolidated_at = Some(35);
        completed_dream.sessions_reviewing = 5;
        snapshot.apply(&MemoryMaintenanceEvent::AutoDream {
            run: completed_dream.clone(),
        });
        snapshot.apply(&MemoryMaintenanceEvent::AutoDream { run: started_dream });
        let mut forged_dream = completed_dream.clone();
        forged_dream.started_at = 31;
        forged_dream.status = MemoryMaintenanceStatus::Failed;
        forged_dream.reason = Some(AutoDreamRunReason::ForkFailure);
        forged_dream.error = Some("forged failure".into());
        snapshot.apply(&MemoryMaintenanceEvent::AutoDream { run: forged_dream });

        assert_eq!(snapshot.auto_dream_runs, vec![completed_dream]);
        assert!(snapshot.interrupt_nonterminal_for_recovery(50).is_empty());
    }

    #[test]
    fn replay_rejects_terminal_transition_without_admitted_start() {
        let mut snapshot = MemoryMaintenanceSnapshot::default();
        let completed = AutoMemoryRun {
            run_id: "orphan-terminal".into(),
            session_id: "session-1".into(),
            trigger: MemoryMaintenanceTrigger::Auto,
            scope: AgentMemoryScope::Project,
            target_dir: "/memory".into(),
            status: MemoryMaintenanceStatus::Completed,
            started_at: 10,
            ended_at: Some(20),
            reason: None,
            error: None,
            message_count: 1,
            cursor_message_uuid: Some("forged-cursor".into()),
            files_touched: vec!["/memory/MEMORY.md".into()],
            turn_count: Some(1),
        };
        assert!(!snapshot.apply(&MemoryMaintenanceEvent::AutoMemory { run: completed }));
        assert!(snapshot.auto_memory_runs.is_empty());
        assert_eq!(snapshot.auto_memory_cursor_message_uuid, None);

        let failed = AutoDreamRun {
            dream_run_id: "orphan-dream-terminal".into(),
            session_id: "session-1".into(),
            trigger: MemoryMaintenanceTrigger::Auto,
            status: MemoryMaintenanceStatus::Failed,
            started_at: 10,
            ended_at: Some(20),
            reason: Some(AutoDreamRunReason::ForkFailure),
            error: Some("forged failure".into()),
            last_consolidated_at: None,
            sessions_reviewing: 0,
            files_touched: Vec::new(),
            turns: Vec::new(),
            turn_count: 0,
            summary: None,
        };
        assert!(!snapshot.apply(&MemoryMaintenanceEvent::AutoDream { run: failed }));
        assert!(snapshot.auto_dream_runs.is_empty());
    }

    #[test]
    fn recovery_clamps_terminal_time_when_wall_clock_moves_backwards() {
        let mut snapshot = MemoryMaintenanceSnapshot::default();
        seed_auto_memory_cursor(&mut snapshot, "cursor-before");
        let started = AutoMemoryRun {
            run_id: "clock-run".into(),
            session_id: "session-1".into(),
            trigger: MemoryMaintenanceTrigger::Auto,
            scope: AgentMemoryScope::Project,
            target_dir: "/memory".into(),
            status: MemoryMaintenanceStatus::Started,
            started_at: 100,
            ended_at: None,
            reason: None,
            error: None,
            message_count: 1,
            cursor_message_uuid: Some("cursor-before".into()),
            files_touched: Vec::new(),
            turn_count: None,
        };
        assert!(snapshot.apply(&MemoryMaintenanceEvent::AutoMemory { run: started }));

        let recovered = snapshot.interrupt_nonterminal_for_recovery(50);
        assert_eq!(recovered.len(), 1);
        assert_eq!(snapshot.auto_memory_runs[1].ended_at, Some(100));
        let encoded = runtime_codec::encode_text(&snapshot).unwrap();
        assert_eq!(
            runtime_codec::decode_text::<MemoryMaintenanceSnapshot>(&encoded).unwrap(),
            snapshot
        );
    }

    #[test]
    fn snapshot_codec_rejects_invalid_run_shapes_and_duplicate_ids() {
        let invalid_terminal = AutoMemoryRun {
            run_id: "memory-run".into(),
            session_id: "session-1".into(),
            trigger: MemoryMaintenanceTrigger::Auto,
            scope: AgentMemoryScope::Project,
            target_dir: "/memory".into(),
            status: MemoryMaintenanceStatus::Completed,
            started_at: 10,
            ended_at: None,
            reason: None,
            error: None,
            message_count: 1,
            cursor_message_uuid: Some("cursor".into()),
            files_touched: Vec::new(),
            turn_count: Some(1),
        };
        let invalid = MemoryMaintenanceSnapshot {
            auto_memory_runs: vec![invalid_terminal.clone()],
            ..MemoryMaintenanceSnapshot::default()
        };
        let encoded = runtime_codec::encode_text(&invalid).unwrap();
        assert!(runtime_codec::decode_text::<MemoryMaintenanceSnapshot>(&encoded).is_err());

        let mut completed = AutoMemoryRun {
            run_id: "cursor-source".into(),
            session_id: "session-1".into(),
            trigger: MemoryMaintenanceTrigger::Auto,
            scope: AgentMemoryScope::Project,
            target_dir: "/memory".into(),
            status: MemoryMaintenanceStatus::Completed,
            started_at: 10,
            ended_at: Some(20),
            reason: None,
            error: None,
            message_count: 1,
            cursor_message_uuid: Some("authoritative-cursor".into()),
            files_touched: Vec::new(),
            turn_count: Some(1),
        };
        let invalid = MemoryMaintenanceSnapshot {
            auto_memory_runs: vec![completed.clone()],
            auto_memory_cursor_message_uuid: Some("forged-cursor".into()),
            ..MemoryMaintenanceSnapshot::default()
        };
        let encoded = runtime_codec::encode_text(&invalid).unwrap();
        assert!(runtime_codec::decode_text::<MemoryMaintenanceSnapshot>(&encoded).is_err());

        completed.status = MemoryMaintenanceStatus::Started;
        completed.ended_at = Some(20);
        let invalid = MemoryMaintenanceSnapshot {
            auto_memory_runs: vec![completed],
            ..MemoryMaintenanceSnapshot::default()
        };
        let encoded = runtime_codec::encode_text(&invalid).unwrap();
        assert!(runtime_codec::decode_text::<MemoryMaintenanceSnapshot>(&encoded).is_err());

        let mut duplicate = AutoDreamRun {
            dream_run_id: "duplicate".into(),
            session_id: "session-1".into(),
            trigger: MemoryMaintenanceTrigger::Auto,
            status: MemoryMaintenanceStatus::Skipped,
            started_at: 10,
            ended_at: Some(10),
            reason: Some(AutoDreamRunReason::AlreadyRunning),
            error: None,
            last_consolidated_at: None,
            sessions_reviewing: 0,
            files_touched: Vec::new(),
            turns: Vec::new(),
            turn_count: 0,
            summary: None,
        };
        let first = duplicate.clone();
        duplicate.reason = Some(AutoDreamRunReason::Shutdown);
        let invalid = MemoryMaintenanceSnapshot {
            auto_dream_runs: vec![first, duplicate],
            ..MemoryMaintenanceSnapshot::default()
        };
        let encoded = runtime_codec::encode_text(&invalid).unwrap();
        assert!(runtime_codec::decode_text::<MemoryMaintenanceSnapshot>(&encoded).is_err());
    }
}
