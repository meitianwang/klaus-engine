//! Authoritative Agent Memory snapshot-decision substrate.
//!
//! The snapshot consumer reconciles one flat directory of memory
//! files. This module keeps that filesystem boundary, but adds the identity,
//! correlation, exactly-once, audit, and crash-safe marker contract required by
//! a remote Host. It deliberately does not choose `keep` when no Host exists:
//! callers must either record an explicit Host decision or apply an explicitly
//! configured policy.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use fs2::FileExt;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::memory::agent_memory::{AgentMemoryLayout, AgentMemoryMergeChoice, AgentMemoryScope};
use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, runtime_json, NodeBufferUtf8, RuntimeCodecError, RuntimeCodecErrorKind,
    RuntimeDecode, RuntimeEncode, RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};

const SNAPSHOT_META: &str = "snapshot.json";
const SYNCED_META: &str = ".snapshot-synced.json";
const APPLY_JOURNAL: &str = ".snapshot-apply.json";
const APPLY_LOCK: &str = ".snapshot-apply.lock";
const PENDING_APPLY_MESSAGE: &str =
    "agent memory snapshot Replace is pending recovery; no new request may be prepared";
const MARKER_SCHEMA_VERSION: u64 = 2;
const APPLY_JOURNAL_SCHEMA_VERSION: u64 = 1;
/// Rollout event name carrying an authoritative decision audit transition.
pub const AGENT_MEMORY_SNAPSHOT_DECISION_ROLLOUT_EVENT: &str = "agent-memory-snapshot-decision";
/// Keep event/query payloads bounded even when a memory directory is large.
pub const MAX_SNAPSHOT_DIFF_PATHS: usize = 64;
/// Bound collision retries for same-directory temporary files. A hostile or
/// crashed directory may already contain candidate names; reconciliation must
/// fail instead of spinning forever.
const MAX_TEMP_FILE_ATTEMPTS: usize = 64;
static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static REGISTRY_INSTANCE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

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
pub enum AgentMemorySnapshotDiffKind {
    Added,
    Modified,
    Removed,
}

impl RuntimeEncode for AgentMemorySnapshotDiffKind {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(
            out,
            match self {
                Self::Added => "added",
                Self::Modified => "modified",
                Self::Removed => "removed",
            },
        )
    }
}

impl RuntimeDecode for AgentMemorySnapshotDiffKind {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("added", &[]),
                RuntimeEnumVariant::new("modified", &[]),
                RuntimeEnumVariant::new("removed", &[]),
            ],
            &[Self::Added, Self::Modified, Self::Removed],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMemorySnapshotDiffEntry {
    pub path: RuntimeString,
    pub kind: AgentMemorySnapshotDiffKind,
}

impl RuntimeEncode for AgentMemorySnapshotDiffEntry {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("path", &self.path)?;
        object.field("kind", &self.kind)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for AgentMemorySnapshotDiffEntry {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                path: object.required("path", &[])?,
                kind: object.required("kind", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMemorySnapshotDiff {
    pub added_count: u64,
    pub modified_count: u64,
    pub removed_count: u64,
    /// Lexicographically sorted and bounded to [`MAX_SNAPSHOT_DIFF_PATHS`].
    pub entries: Vec<AgentMemorySnapshotDiffEntry>,
    pub truncated: bool,
}

impl RuntimeEncode for AgentMemorySnapshotDiff {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("addedCount", &self.added_count)?;
        object.field("modifiedCount", &self.modified_count)?;
        object.field("removedCount", &self.removed_count)?;
        object.field("entries", &self.entries)?;
        object.field("truncated", &self.truncated)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for AgentMemorySnapshotDiff {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                added_count: object.required("addedCount", &[])?,
                modified_count: object.required("modifiedCount", &[])?,
                removed_count: object.required("removedCount", &[])?,
                entries: object.defaulted("entries", &[], Vec::new)?,
                truncated: object.defaulted("truncated", &[], || false)?,
            })
        })
    }
}

/// Correlated, immutable identity of one Host decision request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMemorySnapshotDecisionRequest {
    pub request_id: RuntimeString,
    pub agent_type: RuntimeString,
    pub scope: AgentMemoryScope,
    pub snapshot_timestamp: RuntimeString,
    pub snapshot_id: RuntimeString,
    pub local_state_id: RuntimeString,
    pub diff: AgentMemorySnapshotDiff,
    pub first_use: bool,
    pub requested_at: i64,
    pub deadline_at: i64,
}

impl RuntimeEncode for AgentMemorySnapshotDecisionRequest {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("requestId", &self.request_id)?;
        object.field("agentType", &self.agent_type)?;
        object.field("scope", &self.scope)?;
        object.field("snapshotTimestamp", &self.snapshot_timestamp)?;
        object.field("snapshotId", &self.snapshot_id)?;
        object.field("localStateId", &self.local_state_id)?;
        object.field("diff", &self.diff)?;
        object.field("firstUse", &self.first_use)?;
        object.field("requestedAt", &self.requested_at)?;
        object.field("deadlineAt", &self.deadline_at)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for AgentMemorySnapshotDecisionRequest {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                request_id: object.required("requestId", &[])?,
                agent_type: object.required("agentType", &[])?,
                scope: object.required("scope", &[])?,
                snapshot_timestamp: object.required("snapshotTimestamp", &[])?,
                snapshot_id: object.required("snapshotId", &[])?,
                local_state_id: object.required("localStateId", &[])?,
                diff: object.required("diff", &[])?,
                first_use: object.defaulted("firstUse", &[], || false)?,
                requested_at: object.required("requestedAt", &[])?,
                deadline_at: object.required("deadlineAt", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentMemorySnapshotDecisionStatus {
    Requested,
    DecisionRecorded,
    Applied,
    Failed,
    TimedOut,
    Disconnected,
    NoHandler,
    Stale,
}

impl AgentMemorySnapshotDecisionStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Applied
                | Self::Failed
                | Self::TimedOut
                | Self::Disconnected
                | Self::NoHandler
                | Self::Stale
        )
    }
}

impl RuntimeEncode for AgentMemorySnapshotDecisionStatus {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(
            out,
            match self {
                Self::Requested => "requested",
                Self::DecisionRecorded => "decision-recorded",
                Self::Applied => "applied",
                Self::Failed => "failed",
                Self::TimedOut => "timed-out",
                Self::Disconnected => "disconnected",
                Self::NoHandler => "no-handler",
                Self::Stale => "stale",
            },
        )
    }
}

impl RuntimeDecode for AgentMemorySnapshotDecisionStatus {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("requested", &[]),
                RuntimeEnumVariant::new("decision-recorded", &[]),
                RuntimeEnumVariant::new("applied", &[]),
                RuntimeEnumVariant::new("failed", &[]),
                RuntimeEnumVariant::new("timed-out", &[]),
                RuntimeEnumVariant::new("disconnected", &[]),
                RuntimeEnumVariant::new("no-handler", &[]),
                RuntimeEnumVariant::new("stale", &[]),
            ],
            &[
                Self::Requested,
                Self::DecisionRecorded,
                Self::Applied,
                Self::Failed,
                Self::TimedOut,
                Self::Disconnected,
                Self::NoHandler,
                Self::Stale,
            ],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentMemorySnapshotDecisionSource {
    Host,
    FirstUsePolicy,
    TimeoutPolicy,
    DisconnectPolicy,
    NoHandlerPolicy,
}

impl RuntimeEncode for AgentMemorySnapshotDecisionSource {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(
            out,
            match self {
                Self::Host => "host",
                Self::FirstUsePolicy => "first-use-policy",
                Self::TimeoutPolicy => "timeout-policy",
                Self::DisconnectPolicy => "disconnect-policy",
                Self::NoHandlerPolicy => "no-handler-policy",
            },
        )
    }
}

impl RuntimeDecode for AgentMemorySnapshotDecisionSource {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("host", &[]),
                RuntimeEnumVariant::new("first-use-policy", &[]),
                RuntimeEnumVariant::new("timeout-policy", &[]),
                RuntimeEnumVariant::new("disconnect-policy", &[]),
                RuntimeEnumVariant::new("no-handler-policy", &[]),
            ],
            &[
                Self::Host,
                Self::FirstUsePolicy,
                Self::TimeoutPolicy,
                Self::DisconnectPolicy,
                Self::NoHandlerPolicy,
            ],
        )
    }
}

/// An explicit action configured for a Host-unavailable edge. `RequireHost`
/// terminates in the corresponding state without acknowledging the snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentMemorySnapshotPolicyAction {
    RequireHost,
    Initialize,
    Merge,
    Keep,
    Replace,
}

impl AgentMemorySnapshotPolicyAction {
    pub fn decision(self) -> Option<AgentMemoryMergeChoice> {
        match self {
            Self::RequireHost => None,
            Self::Initialize | Self::Replace => Some(AgentMemoryMergeChoice::Replace),
            Self::Merge => Some(AgentMemoryMergeChoice::Merge),
            Self::Keep => Some(AgentMemoryMergeChoice::Keep),
        }
    }
}

impl RuntimeEncode for AgentMemorySnapshotPolicyAction {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(
            out,
            match self {
                Self::RequireHost => "require-host",
                Self::Initialize => "initialize",
                Self::Merge => "merge",
                Self::Keep => "keep",
                Self::Replace => "replace",
            },
        )
    }
}

impl RuntimeDecode for AgentMemorySnapshotPolicyAction {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("require-host", &[]),
                RuntimeEnumVariant::new("initialize", &[]),
                RuntimeEnumVariant::new("merge", &[]),
                RuntimeEnumVariant::new("keep", &[]),
                RuntimeEnumVariant::new("replace", &[]),
            ],
            &[
                Self::RequireHost,
                Self::Initialize,
                Self::Merge,
                Self::Keep,
                Self::Replace,
            ],
        )
    }
}

/// Host-configured behavior. The CC-compatible first-use seed is explicit;
/// timeout/disconnect/no-handler fail closed unless the Host opts into another
/// terminal decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMemorySnapshotDecisionPolicy {
    pub timeout_ms: u64,
    pub first_use: AgentMemorySnapshotPolicyAction,
    pub timeout: AgentMemorySnapshotPolicyAction,
    pub disconnect: AgentMemorySnapshotPolicyAction,
    pub no_handler: AgentMemorySnapshotPolicyAction,
}

impl Default for AgentMemorySnapshotDecisionPolicy {
    fn default() -> Self {
        Self {
            timeout_ms: 60_000,
            first_use: AgentMemorySnapshotPolicyAction::Initialize,
            timeout: AgentMemorySnapshotPolicyAction::RequireHost,
            disconnect: AgentMemorySnapshotPolicyAction::RequireHost,
            no_handler: AgentMemorySnapshotPolicyAction::RequireHost,
        }
    }
}

impl AgentMemorySnapshotDecisionPolicy {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.timeout_ms == 0 || self.timeout_ms > 3_600_000 {
            return Err("snapshot decision timeout must be in 1..=3600000 milliseconds");
        }
        if self.first_use == AgentMemorySnapshotPolicyAction::RequireHost {
            // Explicitly allowed: some Hosts want first-use confirmation.
        }
        if [self.timeout, self.disconnect, self.no_handler]
            .contains(&AgentMemorySnapshotPolicyAction::Initialize)
        {
            return Err("initialize is only valid for the first-use policy");
        }
        Ok(())
    }

    pub fn fallback(
        &self,
        status: AgentMemorySnapshotDecisionStatus,
        first_use: bool,
    ) -> Option<(AgentMemoryMergeChoice, AgentMemorySnapshotDecisionSource)> {
        let (action, source) = if first_use {
            (
                self.first_use,
                AgentMemorySnapshotDecisionSource::FirstUsePolicy,
            )
        } else {
            match status {
                AgentMemorySnapshotDecisionStatus::TimedOut => (
                    self.timeout,
                    AgentMemorySnapshotDecisionSource::TimeoutPolicy,
                ),
                AgentMemorySnapshotDecisionStatus::Disconnected => (
                    self.disconnect,
                    AgentMemorySnapshotDecisionSource::DisconnectPolicy,
                ),
                AgentMemorySnapshotDecisionStatus::NoHandler => (
                    self.no_handler,
                    AgentMemorySnapshotDecisionSource::NoHandlerPolicy,
                ),
                _ => return None,
            }
        };
        action.decision().map(|decision| (decision, source))
    }
}

impl RuntimeEncode for AgentMemorySnapshotDecisionPolicy {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("timeoutMs", &self.timeout_ms)?;
        object.field("firstUse", &self.first_use)?;
        object.field("timeout", &self.timeout)?;
        object.field("disconnect", &self.disconnect)?;
        object.field("noHandler", &self.no_handler)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for AgentMemorySnapshotDecisionPolicy {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                timeout_ms: object.defaulted("timeoutMs", &[], || 60_000)?,
                first_use: object.defaulted("firstUse", &[], || {
                    AgentMemorySnapshotPolicyAction::Initialize
                })?,
                timeout: object.defaulted("timeout", &[], || {
                    AgentMemorySnapshotPolicyAction::RequireHost
                })?,
                disconnect: object.defaulted("disconnect", &[], || {
                    AgentMemorySnapshotPolicyAction::RequireHost
                })?,
                no_handler: object.defaulted("noHandler", &[], || {
                    AgentMemorySnapshotPolicyAction::RequireHost
                })?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMemorySnapshotDecisionAudit {
    pub request: AgentMemorySnapshotDecisionRequest,
    pub status: AgentMemorySnapshotDecisionStatus,
    pub decision: Option<AgentMemoryMergeChoice>,
    pub source: Option<AgentMemorySnapshotDecisionSource>,
    pub decided_at: Option<i64>,
    pub ended_at: Option<i64>,
    pub error: Option<RuntimeString>,
}

impl RuntimeEncode for AgentMemorySnapshotDecisionAudit {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("request", &self.request)?;
        object.field("status", &self.status)?;
        if let Some(value) = self.decision {
            object.field("decision", &value)?;
        }
        if let Some(value) = self.source {
            object.field("source", &value)?;
        }
        if let Some(value) = self.decided_at {
            object.field("decidedAt", &value)?;
        }
        if let Some(value) = self.ended_at {
            object.field("endedAt", &value)?;
        }
        if let Some(value) = &self.error {
            object.field("error", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for AgentMemorySnapshotDecisionAudit {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                request: object.required("request", &[])?,
                status: object.required("status", &[])?,
                decision: object.optional("decision", &[])?,
                source: object.optional("source", &[])?,
                decided_at: object.optional("decidedAt", &[])?,
                ended_at: object.optional("endedAt", &[])?,
                error: object.optional("error", &[])?,
            })
        })
    }
}

/// Schema-v2 marker. Schema-v1 `{ "syncedFrom": ... }` markers remain
/// readable for upgrade compatibility, but every new acknowledgment uses this
/// correlated record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMemorySnapshotSyncMarker {
    pub schema_version: u64,
    pub synced_from: RuntimeString,
    pub snapshot_id: RuntimeString,
    pub request_id: RuntimeString,
    pub decision: AgentMemoryMergeChoice,
    pub source: AgentMemorySnapshotDecisionSource,
    pub applied_at: i64,
}

impl RuntimeEncode for AgentMemorySnapshotSyncMarker {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("schemaVersion", &self.schema_version)?;
        object.field("syncedFrom", &self.synced_from)?;
        object.field("snapshotId", &self.snapshot_id)?;
        object.field("requestId", &self.request_id)?;
        object.field("decision", &self.decision)?;
        object.field("source", &self.source)?;
        object.field("appliedAt", &self.applied_at)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for AgentMemorySnapshotSyncMarker {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                schema_version: object.required("schemaVersion", &[])?,
                synced_from: object.required("syncedFrom", &[])?,
                snapshot_id: object.required("snapshotId", &[])?,
                request_id: object.required("requestId", &[])?,
                decision: object.required("decision", &[])?,
                source: object.required("source", &[])?,
                applied_at: object.required("appliedAt", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentMemorySnapshotMarkerRead {
    Legacy { synced_from: RuntimeString },
    Current(AgentMemorySnapshotSyncMarker),
}

/// Durable intent for a Replace application. The journal carries the exact
/// captured snapshot bytes rather than only a path/version reference: the
/// snapshot producer may advance its flat directory while this process is
/// down, but recovery must still finish the exact Host-approved replacement.
#[derive(Debug, Clone, PartialEq, Eq)]
struct AgentMemorySnapshotApplyJournal {
    schema_version: u64,
    request_id: RuntimeString,
    snapshot_id: RuntimeString,
    local_state_id: RuntimeString,
    snapshot_timestamp: RuntimeString,
    decision: AgentMemoryMergeChoice,
    source: AgentMemorySnapshotDecisionSource,
    applied_at: i64,
    files: Vec<AgentMemorySnapshotApplyJournalFile>,
}

impl RuntimeEncode for AgentMemorySnapshotApplyJournal {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("schemaVersion", &self.schema_version)?;
        object.field("requestId", &self.request_id)?;
        object.field("snapshotId", &self.snapshot_id)?;
        object.field("localStateId", &self.local_state_id)?;
        object.field("snapshotTimestamp", &self.snapshot_timestamp)?;
        object.field("decision", &self.decision)?;
        object.field("source", &self.source)?;
        object.field("appliedAt", &self.applied_at)?;
        object.field("files", &self.files)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for AgentMemorySnapshotApplyJournal {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                schema_version: object.required("schemaVersion", &[])?,
                request_id: object.required("requestId", &[])?,
                snapshot_id: object.required("snapshotId", &[])?,
                local_state_id: object.required("localStateId", &[])?,
                snapshot_timestamp: object.required("snapshotTimestamp", &[])?,
                decision: object.required("decision", &[])?,
                source: object.required("source", &[])?,
                applied_at: object.required("appliedAt", &[])?,
                files: object.required("files", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AgentMemorySnapshotApplyJournalFile {
    path: RuntimeString,
    contents_base64: RuntimeString,
}

impl RuntimeEncode for AgentMemorySnapshotApplyJournalFile {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("path", &self.path)?;
        object.field("contentsBase64", &self.contents_base64)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for AgentMemorySnapshotApplyJournalFile {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                path: object.required("path", &[])?,
                contents_base64: object.required("contentsBase64", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMemorySnapshotRegisterResult {
    pub audit: AgentMemorySnapshotDecisionAudit,
    pub created: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMemorySnapshotTransitionResult {
    pub audit: AgentMemorySnapshotDecisionAudit,
    pub idempotent: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentMemorySnapshotRegistryError {
    InvalidRequest(&'static str),
    UnknownRequest,
    RequestIdConflict,
    DecisionConflict,
    StateConflict {
        expected: AgentMemorySnapshotDecisionStatus,
        actual: AgentMemorySnapshotDecisionStatus,
    },
    ApplyInProgress,
    StagedRegistryMismatch,
    StagedCommitConflict,
    StagedApplyingState,
}

impl fmt::Display for AgentMemorySnapshotRegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest(message) => {
                write!(formatter, "invalid snapshot request: {message}")
            }
            Self::UnknownRequest => formatter.write_str("unknown snapshot decision request"),
            Self::RequestIdConflict => formatter.write_str("snapshot request id conflict"),
            Self::DecisionConflict => formatter.write_str("snapshot decision conflict"),
            Self::StateConflict { expected, actual } => write!(
                formatter,
                "snapshot decision state conflict: expected {expected:?}, found {actual:?}"
            ),
            Self::ApplyInProgress => formatter.write_str("snapshot decision apply is in progress"),
            Self::StagedRegistryMismatch => {
                formatter.write_str("snapshot staged transition belongs to another registry")
            }
            Self::StagedCommitConflict => formatter
                .write_str("snapshot registry changed before the staged transition committed"),
            Self::StagedApplyingState => formatter
                .write_str("snapshot staged transition contains an in-progress apply claim"),
        }
    }
}

impl std::error::Error for AgentMemorySnapshotRegistryError {}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DecisionKey {
    agent_type: RuntimeString,
    snapshot_id: RuntimeString,
    local_state_id: RuntimeString,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecisionEntry {
    audit: AgentMemorySnapshotDecisionAudit,
    applying: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct RegistryState {
    by_request: HashMap<RuntimeString, DecisionEntry>,
    by_identity: HashMap<DecisionKey, RuntimeString>,
}

fn validate_publishable_staged_state(
    state: &RegistryState,
) -> Result<(), AgentMemorySnapshotRegistryError> {
    if state.by_request.values().any(|entry| entry.applying) {
        return Err(AgentMemorySnapshotRegistryError::StagedApplyingState);
    }
    Ok(())
}

/// In-memory CAS/exactly-once registry. Session rollout events persist each
/// returned audit record; [`restore`](Self::restore) rebuilds the same queryable
/// state after reconnect/resume.
pub struct AgentMemorySnapshotDecisionRegistry {
    instance_id: u64,
    inner: Mutex<RegistryState>,
}

impl Default for AgentMemorySnapshotDecisionRegistry {
    fn default() -> Self {
        Self {
            instance_id: REGISTRY_INSTANCE_SEQUENCE.fetch_add(1, Ordering::Relaxed),
            inner: Mutex::new(RegistryState::default()),
        }
    }
}

/// Opaque proposed registry mutation. The value can be inspected while the
/// real registry remains unchanged; consuming it through `commit_staged`
/// performs one instance-bound whole-state CAS.
#[must_use = "staged snapshot state is invisible until it is durably persisted and committed"]
pub struct AgentMemorySnapshotStaged<T> {
    value: T,
    token: AgentMemorySnapshotRegistryCommitToken,
}

impl<T> AgentMemorySnapshotStaged<T> {
    pub fn value(&self) -> &T {
        &self.value
    }
}

struct AgentMemorySnapshotRegistryCommitToken {
    registry_instance_id: u64,
    before: RegistryState,
    after: RegistryState,
}

impl AgentMemorySnapshotDecisionRegistry {
    fn from_state(state: RegistryState) -> Self {
        Self {
            instance_id: REGISTRY_INSTANCE_SEQUENCE.fetch_add(1, Ordering::Relaxed),
            inner: Mutex::new(state),
        }
    }

    fn snapshot_state(&self) -> RegistryState {
        self.inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    fn stage_with<T, F>(
        &self,
        mutation: F,
    ) -> Result<AgentMemorySnapshotStaged<T>, AgentMemorySnapshotRegistryError>
    where
        F: FnOnce(
            &AgentMemorySnapshotDecisionRegistry,
        ) -> Result<T, AgentMemorySnapshotRegistryError>,
    {
        let before = self.snapshot_state();
        let staged_registry = Self::from_state(before.clone());
        let value = mutation(&staged_registry)?;
        let after = staged_registry.snapshot_state();
        validate_publishable_staged_state(&after)?;
        Ok(AgentMemorySnapshotStaged {
            value,
            token: AgentMemorySnapshotRegistryCommitToken {
                registry_instance_id: self.instance_id,
                before,
                after,
            },
        })
    }

    /// Propose registration without changing the queryable real registry.
    pub fn stage_register(
        &self,
        request: AgentMemorySnapshotDecisionRequest,
    ) -> Result<
        AgentMemorySnapshotStaged<AgentMemorySnapshotRegisterResult>,
        AgentMemorySnapshotRegistryError,
    > {
        self.stage_with(move |registry| registry.register(request))
    }

    /// Propose the requested -> decision-recorded CAS without publishing it.
    pub fn stage_record_decision(
        &self,
        request_id: &RuntimeString,
        expected: AgentMemorySnapshotDecisionStatus,
        decision: AgentMemoryMergeChoice,
        source: AgentMemorySnapshotDecisionSource,
        decided_at: i64,
    ) -> Result<
        AgentMemorySnapshotStaged<AgentMemorySnapshotTransitionResult>,
        AgentMemorySnapshotRegistryError,
    > {
        let request_id = request_id.clone();
        self.stage_with(move |registry| {
            registry.record_decision(&request_id, expected, decision, source, decided_at)
        })
    }

    /// Propose a non-applied terminal transition without publishing it.
    pub fn stage_record_terminal(
        &self,
        request_id: &RuntimeString,
        expected: AgentMemorySnapshotDecisionStatus,
        status: AgentMemorySnapshotDecisionStatus,
        ended_at: i64,
        error: Option<RuntimeString>,
    ) -> Result<
        AgentMemorySnapshotStaged<AgentMemorySnapshotTransitionResult>,
        AgentMemorySnapshotRegistryError,
    > {
        let request_id = request_id.clone();
        self.stage_with(move |registry| {
            registry.record_terminal(&request_id, expected, status, ended_at, error)
        })
    }

    /// Commit exactly the state that was staged against this registry.
    /// Consuming the opaque value is the public single-use guarantee; the CAS
    /// additionally rejects manually duplicated, stale, or cross-registry
    /// tokens before changing any state.
    pub fn commit_staged<T>(
        &self,
        staged: AgentMemorySnapshotStaged<T>,
    ) -> Result<T, AgentMemorySnapshotRegistryError> {
        let AgentMemorySnapshotStaged { value, token } = staged;
        self.commit_token(token)?;
        Ok(value)
    }

    fn commit_token(
        &self,
        token: AgentMemorySnapshotRegistryCommitToken,
    ) -> Result<(), AgentMemorySnapshotRegistryError> {
        if token.registry_instance_id != self.instance_id {
            return Err(AgentMemorySnapshotRegistryError::StagedRegistryMismatch);
        }
        validate_publishable_staged_state(&token.after)?;
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if *state != token.before {
            return Err(AgentMemorySnapshotRegistryError::StagedCommitConflict);
        }
        *state = token.after;
        Ok(())
    }

    pub fn register(
        &self,
        request: AgentMemorySnapshotDecisionRequest,
    ) -> Result<AgentMemorySnapshotRegisterResult, AgentMemorySnapshotRegistryError> {
        validate_request(&request)?;
        let key = decision_key(&request);
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if let Some(existing_id) = state.by_identity.get(&key) {
            let audit = state
                .by_request
                .get(existing_id)
                .expect("identity index must reference a decision")
                .audit
                .clone();
            return Ok(AgentMemorySnapshotRegisterResult {
                audit,
                created: false,
            });
        }
        if state.by_request.contains_key(&request.request_id) {
            return Err(AgentMemorySnapshotRegistryError::RequestIdConflict);
        }
        let request_id = request.request_id.clone();
        let audit = AgentMemorySnapshotDecisionAudit {
            request,
            status: AgentMemorySnapshotDecisionStatus::Requested,
            decision: None,
            source: None,
            decided_at: None,
            ended_at: None,
            error: None,
        };
        state.by_identity.insert(key, request_id.clone());
        state.by_request.insert(
            request_id,
            DecisionEntry {
                audit: audit.clone(),
                applying: false,
            },
        );
        Ok(AgentMemorySnapshotRegisterResult {
            audit,
            created: true,
        })
    }

    /// Restore an authoritative record from rollout replay. Replaying the same
    /// request is idempotent; a mismatched deterministic request id is rejected.
    pub fn restore(
        &self,
        audit: AgentMemorySnapshotDecisionAudit,
    ) -> Result<(), AgentMemorySnapshotRegistryError> {
        validate_request(&audit.request)?;
        validate_audit(&audit)?;
        let key = decision_key(&audit.request);
        let request_id = audit.request.request_id.clone();
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if let Some(existing_id) = state.by_identity.get(&key) {
            if existing_id != &request_id {
                return Err(AgentMemorySnapshotRegistryError::RequestIdConflict);
            }
        }
        if let Some(existing) = state.by_request.get(&request_id) {
            if decision_key(&existing.audit.request) != key {
                return Err(AgentMemorySnapshotRegistryError::RequestIdConflict);
            }
        }
        state.by_identity.insert(key, request_id.clone());
        state.by_request.insert(
            request_id,
            DecisionEntry {
                audit,
                applying: false,
            },
        );
        Ok(())
    }

    pub fn query(&self, request_id: &RuntimeString) -> Option<AgentMemorySnapshotDecisionAudit> {
        self.inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .by_request
            .get(request_id)
            .map(|entry| entry.audit.clone())
    }

    pub fn all(&self) -> Vec<AgentMemorySnapshotDecisionAudit> {
        let mut audits = self
            .inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .by_request
            .values()
            .map(|entry| entry.audit.clone())
            .collect::<Vec<_>>();
        audits.sort_by(|left, right| {
            left.request
                .requested_at
                .cmp(&right.request.requested_at)
                .then_with(|| left.request.request_id.cmp(&right.request.request_id))
        });
        audits
    }

    /// CAS `requested -> decision-recorded`. Repeating the same decision is
    /// idempotent even after apply; a different decision is a stable conflict.
    pub fn record_decision(
        &self,
        request_id: &RuntimeString,
        expected: AgentMemorySnapshotDecisionStatus,
        decision: AgentMemoryMergeChoice,
        source: AgentMemorySnapshotDecisionSource,
        decided_at: i64,
    ) -> Result<AgentMemorySnapshotTransitionResult, AgentMemorySnapshotRegistryError> {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let entry = state
            .by_request
            .get_mut(request_id)
            .ok_or(AgentMemorySnapshotRegistryError::UnknownRequest)?;
        if let Some(existing) = entry.audit.decision {
            if existing != decision {
                return Err(AgentMemorySnapshotRegistryError::DecisionConflict);
            }
            return Ok(AgentMemorySnapshotTransitionResult {
                audit: entry.audit.clone(),
                idempotent: true,
            });
        }
        if entry.audit.status != expected
            || expected != AgentMemorySnapshotDecisionStatus::Requested
        {
            return Err(AgentMemorySnapshotRegistryError::StateConflict {
                expected,
                actual: entry.audit.status,
            });
        }
        entry.audit.status = AgentMemorySnapshotDecisionStatus::DecisionRecorded;
        entry.audit.decision = Some(decision);
        entry.audit.source = Some(source);
        entry.audit.decided_at = Some(decided_at);
        Ok(AgentMemorySnapshotTransitionResult {
            audit: entry.audit.clone(),
            idempotent: false,
        })
    }

    /// CAS a still-requested decision into an explicit terminal edge. No
    /// decision and no marker are synthesized by this transition.
    pub fn record_terminal(
        &self,
        request_id: &RuntimeString,
        expected: AgentMemorySnapshotDecisionStatus,
        status: AgentMemorySnapshotDecisionStatus,
        ended_at: i64,
        error: Option<RuntimeString>,
    ) -> Result<AgentMemorySnapshotTransitionResult, AgentMemorySnapshotRegistryError> {
        if !matches!(
            status,
            AgentMemorySnapshotDecisionStatus::Failed
                | AgentMemorySnapshotDecisionStatus::TimedOut
                | AgentMemorySnapshotDecisionStatus::Disconnected
                | AgentMemorySnapshotDecisionStatus::NoHandler
                | AgentMemorySnapshotDecisionStatus::Stale
        ) {
            return Err(AgentMemorySnapshotRegistryError::InvalidRequest(
                "record_terminal requires a non-applied terminal status",
            ));
        }
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let entry = state
            .by_request
            .get_mut(request_id)
            .ok_or(AgentMemorySnapshotRegistryError::UnknownRequest)?;
        if entry.audit.status == status && entry.audit.error == error {
            return Ok(AgentMemorySnapshotTransitionResult {
                audit: entry.audit.clone(),
                idempotent: true,
            });
        }
        if entry.audit.status != expected {
            return Err(AgentMemorySnapshotRegistryError::StateConflict {
                expected,
                actual: entry.audit.status,
            });
        }
        entry.applying = false;
        entry.audit.status = status;
        entry.audit.ended_at = Some(ended_at);
        entry.audit.error = error;
        Ok(AgentMemorySnapshotTransitionResult {
            audit: entry.audit.clone(),
            idempotent: false,
        })
    }

    fn claim_apply(
        &self,
        request_id: &RuntimeString,
    ) -> Result<ApplyClaim, AgentMemorySnapshotRegistryError> {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let entry = state
            .by_request
            .get_mut(request_id)
            .ok_or(AgentMemorySnapshotRegistryError::UnknownRequest)?;
        if entry.audit.status == AgentMemorySnapshotDecisionStatus::Applied {
            return Ok(ApplyClaim::AlreadyApplied(entry.audit.clone()));
        }
        if entry.applying {
            return Err(AgentMemorySnapshotRegistryError::ApplyInProgress);
        }
        if entry.audit.status != AgentMemorySnapshotDecisionStatus::DecisionRecorded {
            return Err(AgentMemorySnapshotRegistryError::StateConflict {
                expected: AgentMemorySnapshotDecisionStatus::DecisionRecorded,
                actual: entry.audit.status,
            });
        }
        entry.applying = true;
        Ok(ApplyClaim::Claimed(entry.audit.clone()))
    }

    fn finish_apply(
        &self,
        request_id: &RuntimeString,
        status: AgentMemorySnapshotDecisionStatus,
        ended_at: i64,
        error: Option<RuntimeString>,
    ) -> Result<AgentMemorySnapshotDecisionAudit, AgentMemorySnapshotRegistryError> {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let entry = state
            .by_request
            .get_mut(request_id)
            .ok_or(AgentMemorySnapshotRegistryError::UnknownRequest)?;
        if !entry.applying {
            return Err(AgentMemorySnapshotRegistryError::StateConflict {
                expected: AgentMemorySnapshotDecisionStatus::DecisionRecorded,
                actual: entry.audit.status,
            });
        }
        entry.applying = false;
        entry.audit.status = status;
        entry.audit.ended_at = Some(ended_at);
        entry.audit.error = error;
        Ok(entry.audit.clone())
    }

    fn release_apply_claim(&self, request_id: &RuntimeString) {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if let Some(entry) = state.by_request.get_mut(request_id) {
            if entry.audit.status == AgentMemorySnapshotDecisionStatus::DecisionRecorded {
                entry.applying = false;
            }
        }
    }
}

enum ApplyClaim {
    Claimed(AgentMemorySnapshotDecisionAudit),
    AlreadyApplied(AgentMemorySnapshotDecisionAudit),
}

/// Cancellation-safe in-memory admission. Dropping an apply future before a
/// terminal transition reopens the same recorded decision for recovery.
struct ApplyLease<'a> {
    registry: &'a AgentMemorySnapshotDecisionRegistry,
    request_id: RuntimeString,
    finished: bool,
}

impl<'a> ApplyLease<'a> {
    fn new(registry: &'a AgentMemorySnapshotDecisionRegistry, request_id: &RuntimeString) -> Self {
        Self {
            registry,
            request_id: request_id.clone(),
            finished: false,
        }
    }

    fn finish(
        &mut self,
        status: AgentMemorySnapshotDecisionStatus,
        ended_at: i64,
        error: Option<RuntimeString>,
    ) -> Result<AgentMemorySnapshotDecisionAudit, AgentMemorySnapshotRegistryError> {
        let audit = self
            .registry
            .finish_apply(&self.request_id, status, ended_at, error)?;
        self.finished = true;
        Ok(audit)
    }
}

impl Drop for ApplyLease<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.registry.release_apply_claim(&self.request_id);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMemorySnapshotApplyOutcome {
    pub audit: AgentMemorySnapshotDecisionAudit,
    pub merge_prompt: Option<RuntimeString>,
    pub already_applied: bool,
}

fn marker_matches_recorded_decision(
    marker: &AgentMemorySnapshotSyncMarker,
    audit: &AgentMemorySnapshotDecisionAudit,
) -> bool {
    marker_matches_request_identity(marker, audit)
        && Some(marker.decision) == audit.decision
        && Some(marker.source) == audit.source
}

fn marker_matches_request_identity(
    marker: &AgentMemorySnapshotSyncMarker,
    audit: &AgentMemorySnapshotDecisionAudit,
) -> bool {
    marker.schema_version == MARKER_SCHEMA_VERSION
        && marker.request_id == audit.request.request_id
        && marker.snapshot_id == audit.request.snapshot_id
        && marker.synced_from == audit.request.snapshot_timestamp
}

const CONFLICTING_DURABLE_MARKER_ERROR: &str =
    "snapshot decision was superseded by a conflicting durable marker";

/// Recover only the durable half of an already-admitted decision application.
///
/// Merge decisions deliberately remain `decision-recorded` until the provider
/// adapter accepts the request carrying their merge prompt. If the process dies
/// after that admission writes the marker but before the matching `applied`
/// rollout transition is published, cold recovery may use this function to
/// finish the audit. An unrelated marker is a normal no-op, while a marker for
/// the same request identity with a different decision/source is the durable
/// cross-process winner and terminates this loser as `stale`. This path must
/// never create or replace a marker or otherwise acknowledge an undelivered
/// prompt.
pub async fn recover_marked_agent_memory_snapshot_decision(
    layout: &dyn AgentMemoryLayout,
    registry: &AgentMemorySnapshotDecisionRegistry,
    request_id: &RuntimeString,
) -> Result<Option<AgentMemorySnapshotDecisionAudit>, AgentMemorySnapshotApplyError> {
    let mut retained_lock = None;
    recover_marked_agent_memory_snapshot_decision_retaining_lock(
        layout,
        registry,
        request_id,
        &mut retained_lock,
    )
    .await
}

async fn recover_marked_agent_memory_snapshot_decision_retaining_lock(
    layout: &dyn AgentMemoryLayout,
    registry: &AgentMemorySnapshotDecisionRegistry,
    request_id: &RuntimeString,
    retained_lock: &mut Option<SnapshotApplyLock>,
) -> Result<Option<AgentMemorySnapshotDecisionAudit>, AgentMemorySnapshotApplyError> {
    let audit = match registry.claim_apply(request_id)? {
        ApplyClaim::AlreadyApplied(audit) => return Ok(Some(audit)),
        ApplyClaim::Claimed(audit) => audit,
    };
    let mut lease = ApplyLease::new(registry, request_id);
    if audit.decision != Some(AgentMemoryMergeChoice::Merge) {
        return Ok(None);
    }
    let Ok(agent_type) = audit.request.agent_type.try_to_utf8() else {
        return Ok(None);
    };
    let local_dir = layout.dir_for(&agent_type, AgentMemoryScope::User);
    let local_boundary = match DirectoryBoundary::open_existing(Path::new(&local_dir)).await {
        Ok(boundary) => boundary,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(AgentMemorySnapshotApplyError::Io(error)),
    };
    let transaction_lock = local_boundary
        .try_acquire_snapshot_apply_lock()
        .await
        .map_err(AgentMemorySnapshotApplyError::Io)?;
    *retained_lock = Some(transaction_lock);
    let marker = match read_marker_from_boundary(&local_boundary)
        .await
        .map_err(AgentMemorySnapshotApplyError::Io)?
    {
        Some(AgentMemorySnapshotMarkerRead::Current(marker))
            if marker_matches_recorded_decision(&marker, &audit) =>
        {
            marker
        }
        Some(AgentMemorySnapshotMarkerRead::Current(marker))
            if marker_matches_request_identity(&marker, &audit) =>
        {
            // The loser becomes stale once both its decision and the winner's
            // marker exist. Clamp to its own lifecycle timestamps so cold
            // recovery cannot synthesize a terminal audit that travels
            // backwards even when the two processes use skewed clocks.
            let ended_at = marker
                .applied_at
                .max(audit.request.requested_at)
                .max(audit.decided_at.unwrap_or(marker.applied_at));
            return Ok(Some(lease.finish(
                AgentMemorySnapshotDecisionStatus::Stale,
                ended_at,
                Some(RuntimeString::from(CONFLICTING_DURABLE_MARKER_ERROR)),
            )?));
        }
        _ => return Ok(None),
    };
    Ok(Some(lease.finish(
        AgentMemorySnapshotDecisionStatus::Applied,
        marker.applied_at,
        None,
    )?))
}

#[derive(Debug)]
pub enum AgentMemorySnapshotApplyError {
    Registry(AgentMemorySnapshotRegistryError),
    Io(io::Error),
    /// A durable rollout append/flush succeeded, but the in-memory CAS or a
    /// checked query/resolve could not prove the same authoritative state.
    /// Callers must treat this as fatal and stop further side effects.
    PersistenceInconsistent(RuntimeString),
}

impl fmt::Display for AgentMemorySnapshotApplyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Registry(error) => error.fmt(formatter),
            Self::Io(error) => write!(formatter, "snapshot decision I/O failed: {error}"),
            Self::PersistenceInconsistent(detail) => write!(
                formatter,
                "snapshot decision persistence is inconsistent: {}",
                String::from_utf16_lossy(detail.as_utf16())
            ),
        }
    }
}

impl std::error::Error for AgentMemorySnapshotApplyError {}

impl From<AgentMemorySnapshotRegistryError> for AgentMemorySnapshotApplyError {
    fn from(value: AgentMemorySnapshotRegistryError) -> Self {
        Self::Registry(value)
    }
}

#[derive(Debug)]
struct CapturedState {
    snapshot_timestamp: RuntimeString,
    snapshot_files: BTreeMap<String, Vec<u8>>,
    local_files: BTreeMap<String, Vec<u8>>,
    snapshot_id: RuntimeString,
    local_state_id: RuntimeString,
}

impl AgentMemorySnapshotApplyJournal {
    fn from_captured(
        audit: &AgentMemorySnapshotDecisionAudit,
        captured: &CapturedState,
        applied_at: i64,
    ) -> io::Result<Self> {
        let decision = audit.decision.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot Replace journal requires a recorded decision",
            )
        })?;
        if decision != AgentMemoryMergeChoice::Replace {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot apply journal is only valid for Replace",
            ));
        }
        let source = audit.source.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot Replace journal requires a recorded decision source",
            )
        })?;
        let mut files = Vec::with_capacity(captured.snapshot_files.len());
        for (path, contents) in &captured.snapshot_files {
            validate_apply_leaf_name(path)?;
            files.push(AgentMemorySnapshotApplyJournalFile {
                path: RuntimeString::from(path.clone()),
                contents_base64: RuntimeString::from(BASE64_STANDARD.encode(contents)),
            });
        }
        Ok(Self {
            schema_version: APPLY_JOURNAL_SCHEMA_VERSION,
            request_id: audit.request.request_id.clone(),
            snapshot_id: audit.request.snapshot_id.clone(),
            local_state_id: audit.request.local_state_id.clone(),
            snapshot_timestamp: audit.request.snapshot_timestamp.clone(),
            decision,
            source,
            applied_at,
            files,
        })
    }

    fn matches_audit(&self, audit: &AgentMemorySnapshotDecisionAudit) -> bool {
        self.schema_version == APPLY_JOURNAL_SCHEMA_VERSION
            && self.request_id == audit.request.request_id
            && self.snapshot_id == audit.request.snapshot_id
            && self.local_state_id == audit.request.local_state_id
            && self.snapshot_timestamp == audit.request.snapshot_timestamp
            && Some(self.decision) == audit.decision
            && Some(self.source) == audit.source
            && self.decision == AgentMemoryMergeChoice::Replace
    }

    fn matches_marker(&self, marker: &AgentMemorySnapshotSyncMarker) -> bool {
        marker.schema_version == MARKER_SCHEMA_VERSION
            && marker.request_id == self.request_id
            && marker.snapshot_id == self.snapshot_id
            && marker.synced_from == self.snapshot_timestamp
            && marker.decision == self.decision
            && marker.source == self.source
            && marker.applied_at == self.applied_at
    }

    fn decode_snapshot_files(&self) -> io::Result<BTreeMap<String, Vec<u8>>> {
        if self.schema_version != APPLY_JOURNAL_SCHEMA_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported snapshot apply journal schema version",
            ));
        }
        let mut files = BTreeMap::new();
        for file in &self.files {
            let path = file.path.try_to_utf8().map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("snapshot apply journal path is not scalar: {error}"),
                )
            })?;
            validate_apply_leaf_name(&path)?;
            let encoded = file.contents_base64.try_to_utf8().map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("snapshot apply journal contents are not scalar: {error}"),
                )
            })?;
            let contents = BASE64_STANDARD
                .decode(encoded.as_bytes())
                .map_err(|error| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("snapshot apply journal contains invalid base64: {error}"),
                    )
                })?;
            if files.insert(path, contents).is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "snapshot apply journal contains a duplicate path",
                ));
            }
        }
        if state_id(
            b"klaus-agent-memory-snapshot-v1",
            Some(&self.snapshot_timestamp),
            &files,
        ) != self.snapshot_id
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot apply journal payload does not match snapshotId",
            ));
        }
        Ok(files)
    }

    fn marker(&self) -> AgentMemorySnapshotSyncMarker {
        AgentMemorySnapshotSyncMarker {
            schema_version: MARKER_SCHEMA_VERSION,
            synced_from: self.snapshot_timestamp.clone(),
            snapshot_id: self.snapshot_id.clone(),
            request_id: self.request_id.clone(),
            decision: self.decision,
            source: self.source,
            applied_at: self.applied_at,
        }
    }
}

/// Inspect the CC-compatible flat snapshot/local directories and create one
/// deterministic request if reconciliation is needed.
pub async fn prepare_agent_memory_snapshot_request(
    layout: &dyn AgentMemoryLayout,
    agent_type: &str,
    requested_at: i64,
    deadline_at: i64,
) -> io::Result<Option<AgentMemorySnapshotDecisionRequest>> {
    if deadline_at < requested_at {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "snapshot deadline precedes request time",
        ));
    }
    let local_dir = layout.dir_for(agent_type, AgentMemoryScope::User);
    let local_boundary = DirectoryBoundary::create(Path::new(&local_dir)).await?;
    let _transaction_lock = local_boundary.try_acquire_snapshot_apply_lock().await?;
    reject_pending_apply_journal(&local_boundary).await?;
    let Some(captured) =
        capture_state_with_local_boundary(layout, agent_type, &local_boundary).await?
    else {
        return Ok(None);
    };
    let first_use = !captured
        .local_files
        .keys()
        .any(|path| path.ends_with(".md"));
    let marker = read_marker_from_boundary(&local_boundary).await?;
    let needs_reconciliation = if first_use {
        true
    } else {
        match marker {
            Some(AgentMemorySnapshotMarkerRead::Current(marker)) => {
                marker.snapshot_id != captured.snapshot_id
            }
            Some(AgentMemorySnapshotMarkerRead::Legacy { synced_from }) => {
                timestamp_is_newer(&captured.snapshot_timestamp, &synced_from)
            }
            None => true,
        }
    };
    if !needs_reconciliation {
        return Ok(None);
    }
    let diff = build_diff(&captured.snapshot_files, &captured.local_files);
    let agent_type = RuntimeString::from(agent_type);
    let request_id =
        deterministic_request_id(&agent_type, &captured.snapshot_id, &captured.local_state_id);
    // Keep this check next to publication even though every new implementation
    // holds APPLY_LOCK. It fails closed if an older/uncooperative process races
    // a journal into the directory while the snapshot was being captured.
    reject_pending_apply_journal(&local_boundary).await?;
    Ok(Some(AgentMemorySnapshotDecisionRequest {
        request_id,
        agent_type,
        scope: AgentMemoryScope::User,
        snapshot_timestamp: captured.snapshot_timestamp,
        snapshot_id: captured.snapshot_id,
        local_state_id: captured.local_state_id,
        diff,
        first_use,
        requested_at,
        deadline_at,
    }))
}

/// Revalidate immutable identities, apply the recorded choice once, then write
/// the rich marker atomically. Replace publishes a durable, self-contained
/// intent before its first local mutation, so a process crash can idempotently
/// finish a partial replacement without treating its own writes as stale Host
/// edits.
pub async fn apply_recorded_agent_memory_snapshot_decision(
    layout: &dyn AgentMemoryLayout,
    registry: &AgentMemorySnapshotDecisionRegistry,
    request_id: &RuntimeString,
    ended_at: i64,
) -> Result<AgentMemorySnapshotApplyOutcome, AgentMemorySnapshotApplyError> {
    let mut retained_lock = None;
    apply_recorded_agent_memory_snapshot_decision_retaining_lock(
        layout,
        registry,
        request_id,
        ended_at,
        &mut retained_lock,
    )
    .await
}

async fn apply_recorded_agent_memory_snapshot_decision_retaining_lock(
    layout: &dyn AgentMemoryLayout,
    registry: &AgentMemorySnapshotDecisionRegistry,
    request_id: &RuntimeString,
    ended_at: i64,
    retained_lock: &mut Option<SnapshotApplyLock>,
) -> Result<AgentMemorySnapshotApplyOutcome, AgentMemorySnapshotApplyError> {
    let audit = match registry.claim_apply(request_id)? {
        ApplyClaim::AlreadyApplied(audit) => {
            let merge_prompt = (audit.decision == Some(AgentMemoryMergeChoice::Merge)).then(|| {
                RuntimeString::from(crate::memory::agent_memory::build_merge_prompt_prefix())
            });
            return Ok(AgentMemorySnapshotApplyOutcome {
                audit,
                merge_prompt,
                already_applied: true,
            });
        }
        ApplyClaim::Claimed(audit) => audit,
    };

    let mut lease = ApplyLease::new(registry, request_id);
    let agent_type = match audit.request.agent_type.try_to_utf8() {
        Ok(agent_type) => agent_type,
        Err(error) => {
            let error = io::Error::new(io::ErrorKind::InvalidData, error);
            let _ = lease.finish(
                AgentMemorySnapshotDecisionStatus::Failed,
                ended_at,
                Some(RuntimeString::from(error.to_string())),
            );
            return Err(AgentMemorySnapshotApplyError::Io(error));
        }
    };

    // Pin the local root before any marker or identity read and retain that
    // exact directory handle through the final Replace/marker mutation. A
    // content-equivalent ordinary directory substituted after localStateId
    // validation must never become the mutation target merely because it now
    // occupies the same pathname.
    let local_dir = layout.dir_for(&agent_type, AgentMemoryScope::User);
    let local_boundary = match DirectoryBoundary::create(Path::new(&local_dir)).await {
        Ok(boundary) => boundary,
        Err(error) => {
            let _ = lease.finish(
                AgentMemorySnapshotDecisionStatus::Failed,
                ended_at,
                Some(RuntimeString::from(error.to_string())),
            );
            return Err(AgentMemorySnapshotApplyError::Io(error));
        }
    };
    // Serialize journal ownership and every recovery/mutation through one
    // permanent inode. A live peer yields WouldBlock without consuming the
    // durable DecisionRecorded transition; process death releases the OS lock
    // automatically so the same audit can resume from its journal.
    let transaction_lock = match local_boundary.try_acquire_snapshot_apply_lock().await {
        Ok(lock) => lock,
        Err(error) => return Err(AgentMemorySnapshotApplyError::Io(error)),
    };
    *retained_lock = Some(transaction_lock);
    let decision = audit
        .decision
        .expect("decision-recorded audit must retain its decision");
    let source = audit
        .source
        .expect("decision-recorded audit must retain its source");

    // Recover the narrow cancellation window after the marker rename but before
    // the authoritative Applied transition was recorded.
    match read_marker_from_boundary(&local_boundary).await {
        Ok(Some(AgentMemorySnapshotMarkerRead::Current(marker)))
            if marker_matches_request_identity(&marker, &audit) =>
        {
            if !marker_matches_recorded_decision(&marker, &audit) {
                let audit = lease.finish(
                    AgentMemorySnapshotDecisionStatus::Stale,
                    ended_at,
                    Some(RuntimeString::from(CONFLICTING_DURABLE_MARKER_ERROR)),
                )?;
                return Ok(AgentMemorySnapshotApplyOutcome {
                    audit,
                    merge_prompt: None,
                    already_applied: false,
                });
            }
            let applied_at = marker.applied_at;
            let decision = marker.decision;
            if decision == AgentMemoryMergeChoice::Replace {
                // The marker is the filesystem commit record. A crash may have
                // left the matching journal behind after that commit; cleanup
                // must itself be durable before publishing Applied.
                match read_apply_journal_from_boundary(&local_boundary).await {
                    Ok(Some(journal))
                        if journal.matches_audit(&audit) && journal.matches_marker(&marker) =>
                    {
                        // Validate the embedded target too. A same-identity
                        // marker must not authorize deleting a corrupted or
                        // attacker-replaced journal without surfacing it.
                        if let Err(error) = journal.decode_snapshot_files() {
                            return Err(AgentMemorySnapshotApplyError::Io(error));
                        }
                        if let Err(error) =
                            remove_matching_apply_journal_temp_siblings(&local_boundary).await
                        {
                            return Err(AgentMemorySnapshotApplyError::Io(error));
                        }
                        if let Err(error) = remove_apply_journal_durable(&local_boundary).await {
                            return Err(AgentMemorySnapshotApplyError::Io(error));
                        }
                    }
                    Ok(Some(_)) => {
                        return Err(AgentMemorySnapshotApplyError::Io(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "snapshot apply journal does not match the committed marker",
                        )));
                    }
                    // Even an absent journal name needs a directory durability
                    // fence: this can be a retry after unlink succeeded but its
                    // sync failed. Publish Applied only after that deletion is
                    // known durable. It also safely fences legacy markers that
                    // predate journals.
                    Ok(None) => {
                        if let Err(error) = local_boundary.sync_directory().await {
                            return Err(AgentMemorySnapshotApplyError::Io(error));
                        }
                    }
                    Err(error) => return Err(AgentMemorySnapshotApplyError::Io(error)),
                }
            }
            let audit =
                lease.finish(AgentMemorySnapshotDecisionStatus::Applied, applied_at, None)?;
            let merge_prompt = (decision == AgentMemoryMergeChoice::Merge).then(|| {
                RuntimeString::from(crate::memory::agent_memory::build_merge_prompt_prefix())
            });
            return Ok(AgentMemorySnapshotApplyOutcome {
                audit,
                merge_prompt,
                already_applied: true,
            });
        }
        Ok(_) => {}
        Err(error) => {
            if decision == AgentMemoryMergeChoice::Replace {
                // A Replace may already own a durable journal. Do not convert
                // a recoverable transaction into a terminal Failed state.
                return Err(AgentMemorySnapshotApplyError::Io(error));
            }
            let _ = lease.finish(
                AgentMemorySnapshotDecisionStatus::Failed,
                ended_at,
                Some(RuntimeString::from(error.to_string())),
            );
            return Err(AgentMemorySnapshotApplyError::Io(error));
        }
    }

    let pending_journal = match read_apply_journal_from_boundary(&local_boundary).await {
        Ok(journal) => journal,
        Err(error) => return Err(AgentMemorySnapshotApplyError::Io(error)),
    };
    if let Some(journal) = pending_journal {
        if !journal.matches_audit(&audit) {
            return Err(AgentMemorySnapshotApplyError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot apply journal does not match the recorded decision",
            )));
        }
        if let Err(error) = complete_replace_from_journal(&local_boundary, &journal).await {
            return Err(AgentMemorySnapshotApplyError::Io(error));
        }
        let audit = lease.finish(
            AgentMemorySnapshotDecisionStatus::Applied,
            journal.applied_at,
            None,
        )?;
        return Ok(AgentMemorySnapshotApplyOutcome {
            audit,
            merge_prompt: None,
            already_applied: true,
        });
    }

    let captured =
        match capture_state_with_local_boundary(layout, &agent_type, &local_boundary).await {
            Ok(Some(captured)) => captured,
            Ok(None) => {
                let audit = lease.finish(
                    AgentMemorySnapshotDecisionStatus::Stale,
                    ended_at,
                    Some(RuntimeString::from(
                        "snapshot disappeared before decision apply",
                    )),
                )?;
                return Ok(AgentMemorySnapshotApplyOutcome {
                    audit,
                    merge_prompt: None,
                    already_applied: false,
                });
            }
            Err(error) => {
                let message = RuntimeString::from(error.to_string());
                let _ = lease.finish(
                    AgentMemorySnapshotDecisionStatus::Failed,
                    ended_at,
                    Some(message),
                );
                return Err(AgentMemorySnapshotApplyError::Io(error));
            }
        };
    if captured.snapshot_id != audit.request.snapshot_id
        || captured.local_state_id != audit.request.local_state_id
    {
        let audit = lease.finish(
            AgentMemorySnapshotDecisionStatus::Stale,
            ended_at,
            Some(RuntimeString::from(
                "snapshot or local memory changed before decision apply",
            )),
        )?;
        return Ok(AgentMemorySnapshotApplyOutcome {
            audit,
            merge_prompt: None,
            already_applied: false,
        });
    }

    if decision == AgentMemoryMergeChoice::Replace {
        if let Err(error) =
            preflight_replace_targets(&local_boundary, &captured.snapshot_files).await
        {
            let _ = lease.finish(
                AgentMemorySnapshotDecisionStatus::Failed,
                ended_at,
                Some(RuntimeString::from(error.to_string())),
            );
            return Err(AgentMemorySnapshotApplyError::Io(error));
        }
        let journal =
            match AgentMemorySnapshotApplyJournal::from_captured(&audit, &captured, ended_at) {
                Ok(journal) => journal,
                Err(error) => {
                    let _ = lease.finish(
                        AgentMemorySnapshotDecisionStatus::Failed,
                        ended_at,
                        Some(RuntimeString::from(error.to_string())),
                    );
                    return Err(AgentMemorySnapshotApplyError::Io(error));
                }
            };
        if let Err(error) = write_apply_journal_atomic_to_boundary(&local_boundary, &journal).await
        {
            // The intent rename may have happened even when its directory sync
            // failed. Leave DecisionRecorded so a retry can inspect the exact
            // journal instead of publishing a false terminal state.
            return Err(AgentMemorySnapshotApplyError::Io(error));
        }
        if let Err(error) = complete_replace_from_journal(&local_boundary, &journal).await {
            return Err(AgentMemorySnapshotApplyError::Io(error));
        }
        let audit = lease.finish(
            AgentMemorySnapshotDecisionStatus::Applied,
            journal.applied_at,
            None,
        )?;
        return Ok(AgentMemorySnapshotApplyOutcome {
            audit,
            merge_prompt: None,
            already_applied: false,
        });
    }

    let apply_result = async {
        let marker = AgentMemorySnapshotSyncMarker {
            schema_version: MARKER_SCHEMA_VERSION,
            synced_from: audit.request.snapshot_timestamp.clone(),
            snapshot_id: audit.request.snapshot_id.clone(),
            request_id: audit.request.request_id.clone(),
            decision,
            source,
            applied_at: ended_at,
        };
        write_agent_memory_snapshot_marker_atomic_to_boundary(&local_boundary, &marker).await
    }
    .await;

    if let Err(error) = apply_result {
        let message = RuntimeString::from(error.to_string());
        let _ = lease.finish(
            AgentMemorySnapshotDecisionStatus::Failed,
            ended_at,
            Some(message),
        );
        return Err(AgentMemorySnapshotApplyError::Io(error));
    }
    let audit = lease.finish(AgentMemorySnapshotDecisionStatus::Applied, ended_at, None)?;
    let merge_prompt = (decision == AgentMemoryMergeChoice::Merge)
        .then(|| RuntimeString::from(crate::memory::agent_memory::build_merge_prompt_prefix()));
    Ok(AgentMemorySnapshotApplyOutcome {
        audit,
        merge_prompt,
        already_applied: false,
    })
}

fn build_staged_filesystem_result<T>(
    registry: &AgentMemorySnapshotDecisionRegistry,
    before: RegistryState,
    staged_registry: &AgentMemorySnapshotDecisionRegistry,
    request_id: &RuntimeString,
    result: Result<T, AgentMemorySnapshotApplyError>,
    transaction_lock: Option<SnapshotApplyLock>,
) -> AgentMemorySnapshotStagedFilesystemResult<T> {
    let after = staged_registry.snapshot_state();
    if let Err(error) = validate_publishable_staged_state(&after) {
        drop(transaction_lock);
        return AgentMemorySnapshotStagedFilesystemResult {
            result: Err(AgentMemorySnapshotApplyError::Registry(error)),
            proposed_audit: None,
            token: None,
            transaction_lock: None,
        };
    }
    if after == before {
        drop(transaction_lock);
        return AgentMemorySnapshotStagedFilesystemResult {
            result,
            proposed_audit: None,
            token: None,
            transaction_lock: None,
        };
    }
    let proposed_audit = after
        .by_request
        .get(request_id)
        .expect("a staged snapshot transition must retain its request")
        .audit
        .clone();
    AgentMemorySnapshotStagedFilesystemResult {
        result,
        proposed_audit: Some(proposed_audit),
        token: Some(AgentMemorySnapshotRegistryCommitToken {
            registry_instance_id: registry.instance_id,
            before,
            after,
        }),
        transaction_lock,
    }
}

/// Execute Replace/Keep/Merge filesystem work against a cloned registry. The
/// returned audit proposal is not queryable from `registry` until its caller
/// durably persists that audit and consumes the value through
/// `commit_staged_filesystem_checked`.
pub async fn stage_apply_recorded_agent_memory_snapshot_decision(
    layout: &dyn AgentMemoryLayout,
    registry: &AgentMemorySnapshotDecisionRegistry,
    request_id: &RuntimeString,
    ended_at: i64,
) -> AgentMemorySnapshotStagedFilesystemResult<AgentMemorySnapshotApplyOutcome> {
    let before = registry.snapshot_state();
    let staged_registry = AgentMemorySnapshotDecisionRegistry::from_state(before.clone());
    let mut transaction_lock = None;
    let result = apply_recorded_agent_memory_snapshot_decision_retaining_lock(
        layout,
        &staged_registry,
        request_id,
        ended_at,
        &mut transaction_lock,
    )
    .await;
    build_staged_filesystem_result(
        registry,
        before,
        &staged_registry,
        request_id,
        result,
        transaction_lock,
    )
}

/// Stage only the durable-marker half of Merge recovery. Matching Applied is
/// proposed but remains invisible until the persisted audit is committed.
pub async fn stage_recover_marked_agent_memory_snapshot_decision(
    layout: &dyn AgentMemoryLayout,
    registry: &AgentMemorySnapshotDecisionRegistry,
    request_id: &RuntimeString,
) -> AgentMemorySnapshotStagedFilesystemResult<Option<AgentMemorySnapshotDecisionAudit>> {
    let before = registry.snapshot_state();
    let staged_registry = AgentMemorySnapshotDecisionRegistry::from_state(before.clone());
    let mut transaction_lock = None;
    let result = recover_marked_agent_memory_snapshot_decision_retaining_lock(
        layout,
        &staged_registry,
        request_id,
        &mut transaction_lock,
    )
    .await;
    build_staged_filesystem_result(
        registry,
        before,
        &staged_registry,
        request_id,
        result,
        transaction_lock,
    )
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// The crash is injected immediately AFTER the named replace step completes.
enum ReplaceCrashPoint {
    FirstRemove,
    FirstWrite,
    ReplaceDurable,
    MarkerDurable,
}

#[cfg(test)]
#[derive(Default)]
struct SnapshotApplyTestFaults {
    replace_crashes: HashMap<PathBuf, ReplaceCrashPoint>,
    directory_sync_failures: HashMap<PathBuf, usize>,
    replace_mutation_pauses: HashMap<PathBuf, ReplaceMutationPause>,
}

#[cfg(test)]
#[derive(Debug, Clone)]
struct ReplaceMutationPause {
    reached: std::sync::Arc<tokio::sync::Notify>,
    resume: std::sync::Arc<tokio::sync::Notify>,
}

#[cfg(test)]
fn snapshot_apply_test_faults() -> &'static Mutex<SnapshotApplyTestFaults> {
    static FAULTS: std::sync::OnceLock<Mutex<SnapshotApplyTestFaults>> = std::sync::OnceLock::new();
    FAULTS.get_or_init(|| Mutex::new(SnapshotApplyTestFaults::default()))
}

#[cfg(test)]
fn set_replace_crash_for_test(path: &Path, point: ReplaceCrashPoint) {
    snapshot_apply_test_faults()
        .lock()
        .unwrap()
        .replace_crashes
        .insert(path.to_path_buf(), point);
}

#[cfg(test)]
fn maybe_crash_replace_for_test(path: &Path, point: ReplaceCrashPoint) -> io::Result<()> {
    let mut faults = snapshot_apply_test_faults().lock().unwrap();
    if faults.replace_crashes.get(path).copied() == Some(point) {
        faults.replace_crashes.remove(path);
        return Err(io::Error::new(
            io::ErrorKind::Interrupted,
            format!("injected snapshot Replace crash at {point:?}"),
        ));
    }
    Ok(())
}

#[cfg(test)]
fn pause_before_replace_mutation_for_test(
    path: &Path,
) -> (
    std::sync::Arc<tokio::sync::Notify>,
    std::sync::Arc<tokio::sync::Notify>,
) {
    let pause = ReplaceMutationPause {
        reached: std::sync::Arc::new(tokio::sync::Notify::new()),
        resume: std::sync::Arc::new(tokio::sync::Notify::new()),
    };
    let result = (pause.reached.clone(), pause.resume.clone());
    snapshot_apply_test_faults()
        .lock()
        .unwrap()
        .replace_mutation_pauses
        .insert(path.to_path_buf(), pause);
    result
}

#[cfg(test)]
async fn maybe_pause_before_replace_mutation_for_test(path: &Path) {
    let pause = snapshot_apply_test_faults()
        .lock()
        .unwrap()
        .replace_mutation_pauses
        .remove(path);
    if let Some(pause) = pause {
        pause.reached.notify_one();
        pause.resume.notified().await;
    }
}

#[cfg(test)]
fn fail_directory_sync_for_test(path: &Path, invocation: usize) {
    assert!(invocation > 0);
    snapshot_apply_test_faults()
        .lock()
        .unwrap()
        .directory_sync_failures
        .insert(path.to_path_buf(), invocation);
}

#[cfg(test)]
fn maybe_fail_directory_sync_for_test(path: &Path) -> io::Result<()> {
    let mut faults = snapshot_apply_test_faults().lock().unwrap();
    let should_fail = match faults.directory_sync_failures.get_mut(path) {
        Some(remaining) if *remaining == 1 => true,
        Some(remaining) => {
            *remaining -= 1;
            false
        }
        None => false,
    };
    if should_fail {
        faults.directory_sync_failures.remove(path);
        return Err(io::Error::other("injected snapshot directory sync failure"));
    }
    Ok(())
}

/// Pins the final memory-directory component and rejects a root symlink or
/// reparse point. On Unix, child mutations use `*at` syscalls relative to this
/// handle, so replacing the directory pathname after validation cannot redirect
/// a write. On Windows, the directory handle intentionally omits delete sharing,
/// preventing rename/replacement while the boundary is live.
struct SnapshotApplyLock {
    file: std::fs::File,
}

impl Drop for SnapshotApplyLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

/// A filesystem-backed registry proposal. `proposed_audit` is the exact audit
/// a Session must append and flush before calling
/// `commit_staged_filesystem_checked`.
/// When a transition exists, the private OS lock remains owned by this value
/// across that persistence gap and is released only on commit or Drop.
#[must_use = "persist proposed_audit before checked commit or deliberately drop this staged result"]
pub struct AgentMemorySnapshotStagedFilesystemResult<T> {
    result: Result<T, AgentMemorySnapshotApplyError>,
    proposed_audit: Option<AgentMemorySnapshotDecisionAudit>,
    token: Option<AgentMemorySnapshotRegistryCommitToken>,
    transaction_lock: Option<SnapshotApplyLock>,
}

impl<T> AgentMemorySnapshotStagedFilesystemResult<T> {
    pub fn result(&self) -> Result<&T, &AgentMemorySnapshotApplyError> {
        self.result.as_ref()
    }

    pub fn proposed_audit(&self) -> Option<&AgentMemorySnapshotDecisionAudit> {
        self.proposed_audit.as_ref()
    }

    pub fn has_registry_transition(&self) -> bool {
        self.token.is_some()
    }
}

impl AgentMemorySnapshotDecisionRegistry {
    /// Publish a previously persisted filesystem-backed proposal without
    /// flattening registry-CAS failure into the staged filesystem result. The
    /// outer `Result` proves whether the staged registry state was committed;
    /// the inner `Result` preserves the filesystem operation's business result,
    /// which can be an error even after a durable Failed audit was committed.
    /// The lock is deliberately still live while the whole-state CAS executes
    /// and is released on both outer- and inner-error paths.
    pub fn commit_staged_filesystem_checked<T>(
        &self,
        staged: AgentMemorySnapshotStagedFilesystemResult<T>,
    ) -> Result<Result<T, AgentMemorySnapshotApplyError>, AgentMemorySnapshotRegistryError> {
        let AgentMemorySnapshotStagedFilesystemResult {
            result,
            token,
            transaction_lock,
            ..
        } = staged;
        let committed = token.map_or(Ok(()), |token| self.commit_token(token));
        drop(transaction_lock);
        committed.map(|()| result)
    }

    /// Compatibility wrapper for callers that do not need to distinguish a
    /// registry-CAS failure from the staged filesystem operation's result. A
    /// durability owner that persisted `proposed_audit` must use the checked
    /// API so an outer CAS failure cannot be mistaken for a committed business
    /// error.
    pub fn commit_staged_filesystem<T>(
        &self,
        staged: AgentMemorySnapshotStagedFilesystemResult<T>,
    ) -> Result<T, AgentMemorySnapshotApplyError> {
        self.commit_staged_filesystem_checked(staged)
            .map_err(AgentMemorySnapshotApplyError::Registry)?
    }
}

struct DirectoryBoundary {
    path: PathBuf,
    handle: tokio::fs::File,
}

impl DirectoryBoundary {
    async fn open_existing(path: &Path) -> io::Result<Self> {
        let handle = open_directory_handle_no_follow(path).await?;
        let boundary = Self {
            path: path.to_path_buf(),
            handle,
        };
        boundary.revalidate().await?;
        Ok(boundary)
    }

    async fn create(path: &Path) -> io::Result<Self> {
        // If the final component already is a symlink, create_dir_all performs
        // no child mutation; the no-follow open below rejects it. Once opened,
        // all child mutations are pinned to the returned directory handle.
        tokio::fs::create_dir_all(path).await?;
        let handle = open_directory_handle_no_follow_for_mutation(path).await?;
        let boundary = Self {
            path: path.to_path_buf(),
            handle,
        };
        boundary.revalidate().await?;
        Ok(boundary)
    }

    async fn revalidate(&self) -> io::Result<()> {
        let current = match open_directory_handle_no_follow(&self.path).await {
            Ok(current) => current,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(directory_replaced_error(&self.path));
            }
            Err(error) => return Err(error),
        };
        if !same_directory_handle(&self.handle, &current).await? {
            return Err(directory_replaced_error(&self.path));
        }
        Ok(())
    }

    /// Persist child-name mutations after their file contents are synced. The
    /// same pinned, no-follow directory descriptor is the durability boundary;
    /// Windows opens it with write access because `FlushFileBuffers` requires
    /// that access in addition to `FILE_FLAG_BACKUP_SEMANTICS`.
    async fn sync_directory(&self) -> io::Result<()> {
        #[cfg(test)]
        maybe_fail_directory_sync_for_test(&self.path)?;
        self.handle.sync_all().await?;
        self.revalidate().await
    }

    /// Acquire the permanent per-memory-directory transaction inode. The leaf
    /// is never removed: unlink/recreate would allow an old holder and a new
    /// holder to lock different inodes concurrently. Opening is relative to the
    /// pinned directory on Unix and no-follow/no-delete-share on Windows.
    async fn try_acquire_snapshot_apply_lock(&self) -> io::Result<SnapshotApplyLock> {
        let path = self.leaf_path(APPLY_LOCK)?;
        #[cfg(unix)]
        let file = {
            use std::os::fd::{AsRawFd, FromRawFd};
            use std::os::unix::ffi::OsStrExt;

            let name = std::ffi::CString::new(Path::new(APPLY_LOCK).as_os_str().as_bytes())
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "snapshot apply lock filename contains NUL",
                    )
                })?;
            loop {
                let fd = unsafe {
                    libc::openat(
                        self.handle.as_raw_fd(),
                        name.as_ptr(),
                        libc::O_RDWR
                            | libc::O_CREAT
                            | libc::O_CLOEXEC
                            | libc::O_NOFOLLOW
                            | libc::O_NONBLOCK,
                        0o666,
                    )
                };
                if fd >= 0 {
                    break unsafe { std::fs::File::from_raw_fd(fd) };
                }
                let error = io::Error::last_os_error();
                if error.raw_os_error() == Some(libc::EINTR) {
                    continue;
                }
                if error.raw_os_error() == Some(libc::ELOOP) {
                    return Err(symlink_file_error(&path));
                }
                return Err(error);
            }
        };
        #[cfg(windows)]
        let file = {
            use std::os::windows::fs::OpenOptionsExt;
            let mut options = std::fs::OpenOptions::new();
            options
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .custom_flags(0x0020_0000) // FILE_FLAG_OPEN_REPARSE_POINT
                .share_mode(0x0000_0001 | 0x0000_0002); // READ | WRITE, no DELETE
            options.open(&path)?
        };
        #[cfg(not(any(unix, windows)))]
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        validate_snapshot_apply_lock_file(&file, &path)?;
        match crate::fs::exclusive_lock::try_lock_exclusive(&file)? {
            crate::fs::exclusive_lock::LockAttempt::Acquired => {}
            crate::fs::exclusive_lock::LockAttempt::Contended => {
                return Err(crate::fs::exclusive_lock::contended_error(
                    "agent memory snapshot transaction is already active",
                ));
            }
        }
        validate_snapshot_apply_lock_file(&file, &path)?;
        self.revalidate().await?;
        Ok(SnapshotApplyLock { file })
    }

    fn leaf_path(&self, name: &str) -> io::Result<PathBuf> {
        let leaf = Path::new(name);
        if name.is_empty() || leaf.file_name().is_none() || leaf.components().count() != 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "agent memory operation requires one flat filename",
            ));
        }
        Ok(self.path.join(leaf))
    }

    #[cfg(unix)]
    fn open_leaf_at(
        &self,
        name: &str,
        flags: libc::c_int,
        mode: libc::mode_t,
    ) -> io::Result<tokio::fs::File> {
        use std::os::fd::{AsRawFd, FromRawFd};
        use std::os::unix::ffi::OsStrExt;

        let path = self.leaf_path(name)?;
        let name =
            std::ffi::CString::new(Path::new(name).as_os_str().as_bytes()).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "agent memory filename contains NUL",
                )
            })?;
        loop {
            let fd = unsafe {
                libc::openat(
                    self.handle.as_raw_fd(),
                    name.as_ptr(),
                    flags | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                    mode as libc::c_uint,
                )
            };
            if fd >= 0 {
                let file = unsafe { std::fs::File::from_raw_fd(fd) };
                return Ok(tokio::fs::File::from_std(file));
            }
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            if error.raw_os_error() == Some(libc::ELOOP) {
                return Err(symlink_file_error(&path));
            }
            return Err(error);
        }
    }

    async fn read_leaf(&self, name: &str) -> io::Result<Vec<u8>> {
        let path = self.leaf_path(name)?;
        #[cfg(unix)]
        let mut file = self.open_leaf_at(name, libc::O_RDONLY | libc::O_NONBLOCK, 0)?;
        #[cfg(not(unix))]
        let mut file = {
            reject_symlink_file(&path).await?;
            let mut options = tokio::fs::OpenOptions::new();
            options.read(true);
            configure_no_follow(&mut options);
            options.open(&path).await?
        };
        let metadata = file.metadata().await?;
        if metadata.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::IsADirectory,
                format!("agent memory path is a directory: {}", path.display()),
            ));
        }
        if !metadata.is_file() {
            return Err(not_regular_file_error(&path));
        }
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).await?;
        Ok(bytes)
    }

    async fn write_leaf(&self, name: &str, content: &[u8]) -> io::Result<()> {
        let path = self.leaf_path(name)?;
        self.validate_regular_leaf_if_present(name).await?;

        // Never truncate the existing inode in place. Besides symlinks, a
        // same-name regular file may be a hard link to a file outside the
        // memory directory. A new same-directory inode plus rename replaces
        // only this directory entry and cannot modify the linked target.
        let (temp, mut file) = self.create_bounded_temp_leaf("replace").await?;
        let result = async {
            file.write_all(content).await?;
            file.sync_all().await?;
            drop(file);
            self.rename_leaf(&temp, name).await
        }
        .await;
        if result.is_err() {
            let _ = self.remove_leaf(&temp).await;
        }
        result.map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "failed to replace agent memory file {}: {error}",
                    path.display()
                ),
            )
        })
    }

    async fn create_bounded_temp_leaf(
        &self,
        purpose: &str,
    ) -> io::Result<(String, tokio::fs::File)> {
        for _ in 0..MAX_TEMP_FILE_ATTEMPTS {
            let sequence = TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let name = format!(
                ".klaus-agent-memory-{purpose}-{}-{sequence}.tmp",
                std::process::id()
            );
            match self.create_new_leaf(&name).await {
                Ok(file) => return Ok((name, file)),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        }
        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "agent memory temporary-file collision limit exceeded",
        ))
    }

    #[cfg(unix)]
    async fn validate_regular_leaf_if_present(&self, name: &str) -> io::Result<()> {
        use std::os::fd::AsRawFd;
        use std::os::unix::ffi::OsStrExt;

        let path = self.leaf_path(name)?;
        let name =
            std::ffi::CString::new(Path::new(name).as_os_str().as_bytes()).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "agent memory filename contains NUL",
                )
            })?;
        loop {
            let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
            let result = unsafe {
                libc::fstatat(
                    self.handle.as_raw_fd(),
                    name.as_ptr(),
                    stat.as_mut_ptr(),
                    libc::AT_SYMLINK_NOFOLLOW,
                )
            };
            if result == 0 {
                let stat = unsafe { stat.assume_init() };
                let kind = stat.st_mode & libc::S_IFMT;
                if kind == libc::S_IFLNK {
                    return Err(symlink_file_error(&path));
                }
                if kind != libc::S_IFREG {
                    return Err(not_regular_file_error(&path));
                }
                return Ok(());
            }
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            if error.kind() == io::ErrorKind::NotFound {
                return Ok(());
            }
            return Err(error);
        }
    }

    #[cfg(not(unix))]
    async fn validate_regular_leaf_if_present(&self, name: &str) -> io::Result<()> {
        reject_symlink_file_if_present(&self.leaf_path(name)?).await
    }

    async fn create_new_leaf(&self, name: &str) -> io::Result<tokio::fs::File> {
        let path = self.leaf_path(name)?;
        #[cfg(unix)]
        let file = self.open_leaf_at(name, libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL, 0o666)?;
        #[cfg(not(unix))]
        let file = {
            let mut options = tokio::fs::OpenOptions::new();
            options.create_new(true).write(true);
            configure_no_follow(&mut options);
            options.open(&path).await?
        };
        if !file.metadata().await?.is_file() {
            return Err(not_regular_file_error(&path));
        }
        Ok(file)
    }

    async fn remove_leaf(&self, name: &str) -> io::Result<()> {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            use std::os::unix::ffi::OsStrExt;

            self.leaf_path(name)?;
            let name =
                std::ffi::CString::new(Path::new(name).as_os_str().as_bytes()).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "agent memory filename contains NUL",
                    )
                })?;
            loop {
                let result = unsafe { libc::unlinkat(self.handle.as_raw_fd(), name.as_ptr(), 0) };
                if result == 0 {
                    return Ok(());
                }
                let error = io::Error::last_os_error();
                if error.raw_os_error() != Some(libc::EINTR) {
                    return Err(error);
                }
            }
        }
        #[cfg(not(unix))]
        {
            tokio::fs::remove_file(self.leaf_path(name)?).await
        }
    }

    /// Publish `from` as `to` only if no owner already published `to`.
    /// Unix uses an fd-relative hard-link CAS followed by unlinking the temp;
    /// Windows uses a write-through move without REPLACE_EXISTING.
    async fn publish_leaf_no_replace(&self, from: &str, to: &str) -> io::Result<()> {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            use std::os::unix::ffi::OsStrExt;

            self.leaf_path(from)?;
            self.leaf_path(to)?;
            let from_name = from.to_owned();
            let from_c = std::ffi::CString::new(Path::new(&from_name).as_os_str().as_bytes())
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "agent memory filename contains NUL",
                    )
                })?;
            let to =
                std::ffi::CString::new(Path::new(to).as_os_str().as_bytes()).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "agent memory filename contains NUL",
                    )
                })?;
            loop {
                let result = unsafe {
                    libc::linkat(
                        self.handle.as_raw_fd(),
                        from_c.as_ptr(),
                        self.handle.as_raw_fd(),
                        to.as_ptr(),
                        0,
                    )
                };
                if result == 0 {
                    self.remove_leaf(&from_name).await?;
                    return Ok(());
                }
                let error = io::Error::last_os_error();
                if error.raw_os_error() != Some(libc::EINTR) {
                    return Err(error);
                }
            }
        }
        #[cfg(windows)]
        {
            use std::os::windows::ffi::OsStrExt;
            use windows_sys::Win32::Storage::FileSystem::{MoveFileExW, MOVEFILE_WRITE_THROUGH};

            let from = self
                .leaf_path(from)?
                .as_os_str()
                .encode_wide()
                .chain(std::iter::once(0))
                .collect::<Vec<_>>();
            let to = self
                .leaf_path(to)?
                .as_os_str()
                .encode_wide()
                .chain(std::iter::once(0))
                .collect::<Vec<_>>();
            let result = unsafe { MoveFileExW(from.as_ptr(), to.as_ptr(), MOVEFILE_WRITE_THROUGH) };
            if result == 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        }
        #[cfg(not(any(unix, windows)))]
        {
            tokio::fs::hard_link(self.leaf_path(from)?, self.leaf_path(to)?).await?;
            self.remove_leaf(from).await
        }
    }

    async fn rename_leaf(&self, from: &str, to: &str) -> io::Result<()> {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            use std::os::unix::ffi::OsStrExt;

            self.leaf_path(from)?;
            self.leaf_path(to)?;
            let from =
                std::ffi::CString::new(Path::new(from).as_os_str().as_bytes()).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "agent memory filename contains NUL",
                    )
                })?;
            let to =
                std::ffi::CString::new(Path::new(to).as_os_str().as_bytes()).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "agent memory filename contains NUL",
                    )
                })?;
            loop {
                let result = unsafe {
                    libc::renameat(
                        self.handle.as_raw_fd(),
                        from.as_ptr(),
                        self.handle.as_raw_fd(),
                        to.as_ptr(),
                    )
                };
                if result == 0 {
                    return Ok(());
                }
                let error = io::Error::last_os_error();
                if error.raw_os_error() != Some(libc::EINTR) {
                    return Err(error);
                }
            }
        }
        #[cfg(windows)]
        {
            use std::os::windows::ffi::OsStrExt;
            use windows_sys::Win32::Storage::FileSystem::{
                MoveFileExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
            };

            let from = self
                .leaf_path(from)?
                .as_os_str()
                .encode_wide()
                .chain(std::iter::once(0))
                .collect::<Vec<_>>();
            let to = self
                .leaf_path(to)?
                .as_os_str()
                .encode_wide()
                .chain(std::iter::once(0))
                .collect::<Vec<_>>();
            let result = unsafe {
                MoveFileExW(
                    from.as_ptr(),
                    to.as_ptr(),
                    MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
                )
            };
            if result == 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        }
        #[cfg(not(any(unix, windows)))]
        {
            tokio::fs::rename(self.leaf_path(from)?, self.leaf_path(to)?).await
        }
    }
}

fn validate_snapshot_apply_lock_file(file: &std::fs::File, path: &Path) -> io::Result<()> {
    let metadata = file.metadata()?;
    if metadata_is_link_like(&metadata) {
        return Err(symlink_file_error(path));
    }
    if !metadata.is_file() {
        return Err(not_regular_file_error(path));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot apply lock must be a single-link regular file",
            ));
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{
            GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
        };

        let mut information = BY_HANDLE_FILE_INFORMATION::default();
        if unsafe { GetFileInformationByHandle(file.as_raw_handle() as _, &mut information) } == 0 {
            return Err(io::Error::last_os_error());
        }
        if information.nNumberOfLinks != 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot apply lock must be a single-link regular file",
            ));
        }
    }
    Ok(())
}

async fn open_directory_handle_no_follow(path: &Path) -> io::Result<tokio::fs::File> {
    open_directory_handle_no_follow_with_mode(path, false).await
}

async fn open_directory_handle_no_follow_for_mutation(path: &Path) -> io::Result<tokio::fs::File> {
    open_directory_handle_no_follow_with_mode(path, true).await
}

async fn open_directory_handle_no_follow_with_mode(
    path: &Path,
    writable: bool,
) -> io::Result<tokio::fs::File> {
    let metadata = match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => metadata,
        Err(error) => return Err(error),
    };
    if metadata_is_link_like(&metadata) {
        return Err(directory_symlink_error(path));
    }
    if !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("agent memory root is not a directory: {}", path.display()),
        ));
    }

    let mut options = tokio::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW);
    #[cfg(windows)]
    {
        options.write(writable);
        options.custom_flags(0x0220_0000); // BACKUP_SEMANTICS | OPEN_REPARSE_POINT
                                           // READ | WRITE | DELETE. Withholding DELETE made the memory directory
                                           // undeletable and unrenamable machine-wide for as long as any boundary was open —
                                           // the engine's own teardown included — while the unix side pins the same directory
                                           // with `O_DIRECTORY|O_NOFOLLOW` and never blocked rmdir. The guarantee here is the
                                           // identity revalidation, not the OS refusing other processes.
        options.share_mode(0x0000_0001 | 0x0000_0002 | 0x0000_0004);
    }
    #[cfg(not(windows))]
    let _ = writable;
    let handle = options.open(path).await?;
    let metadata = handle.metadata().await?;
    if metadata_is_link_like(&metadata) {
        return Err(directory_symlink_error(path));
    }
    if !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("agent memory root is not a directory: {}", path.display()),
        ));
    }
    Ok(handle)
}

fn metadata_is_link_like(metadata: &std::fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;

        // `FileType::is_symlink` does not cover every name-surrogate reparse
        // point (notably directory junctions). Treat every reparse point as an
        // untrusted redirect at this filesystem boundary.
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
        metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
    }
    #[cfg(not(windows))]
    {
        metadata.file_type().is_symlink()
    }
}

async fn same_directory_handle(
    left: &tokio::fs::File,
    right: &tokio::fs::File,
) -> io::Result<bool> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        let left = left.metadata().await?;
        let right = right.metadata().await?;
        Ok(left.dev() == right.dev() && left.ino() == right.ino())
    }
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{
            GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
        };

        fn identity(file: &tokio::fs::File) -> io::Result<(u32, u32, u32)> {
            let mut info = std::mem::MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::zeroed();
            let result =
                unsafe { GetFileInformationByHandle(file.as_raw_handle() as _, info.as_mut_ptr()) };
            if result == 0 {
                return Err(io::Error::last_os_error());
            }
            let info = unsafe { info.assume_init() };
            Ok((
                info.dwVolumeSerialNumber,
                info.nFileIndexHigh,
                info.nFileIndexLow,
            ))
        }

        Ok(identity(left)? == identity(right)?)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (left, right);
        Ok(true)
    }
}

fn directory_symlink_error(path: &Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "agent memory root must not be a symbolic link: {}",
            path.display()
        ),
    )
}

fn directory_replaced_error(path: &Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "agent memory root changed during reconciliation: {}",
            path.display()
        ),
    )
}

fn not_regular_file_error(path: &Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "agent memory path is not a regular file: {}",
            path.display()
        ),
    )
}

pub async fn read_agent_memory_snapshot_marker(
    layout: &dyn AgentMemoryLayout,
    agent_type: &str,
) -> io::Result<Option<AgentMemorySnapshotMarkerRead>> {
    let local_dir = layout.dir_for(agent_type, AgentMemoryScope::User);
    let boundary = match DirectoryBoundary::open_existing(Path::new(&local_dir)).await {
        Ok(boundary) => boundary,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    read_marker_from_boundary(&boundary).await
}

async fn read_marker_from_boundary(
    boundary: &DirectoryBoundary,
) -> io::Result<Option<AgentMemorySnapshotMarkerRead>> {
    boundary.revalidate().await?;
    let bytes = match boundary.read_leaf(SYNCED_META).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            boundary.revalidate().await?;
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    boundary.revalidate().await?;
    let raw = NodeBufferUtf8::decode(&bytes);
    if let Ok(marker) = runtime_codec::decode_text::<AgentMemorySnapshotSyncMarker>(&raw) {
        if marker.schema_version == MARKER_SCHEMA_VERSION
            && !marker.synced_from.is_empty()
            && !marker.snapshot_id.is_empty()
            && !marker.request_id.is_empty()
        {
            return Ok(Some(AgentMemorySnapshotMarkerRead::Current(marker)));
        }
    }
    let value = match runtime_json::parse(&raw) {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };
    // A malformed/future rich marker must not silently downgrade to the legacy
    // timestamp-only contract. Only an object with no schemaVersion is legacy.
    let has_schema_version = runtime_codec::decode_object(&value, false, |object| {
        object.optional::<u64>("schemaVersion", &[])
    })
    .ok()
    .flatten()
    .is_some();
    if has_schema_version {
        return Ok(None);
    }
    let synced_from = match runtime_codec::decode_object(&value, false, |object| {
        object.required::<RuntimeString>("syncedFrom", &[])
    }) {
        Ok(value) if !value.is_empty() => value,
        _ => return Ok(None),
    };
    Ok(Some(AgentMemorySnapshotMarkerRead::Legacy { synced_from }))
}

/// Atomic same-directory temp-file + rename replacement. A failed write or
/// rename leaves no partially written schema-v2 marker.
pub async fn write_agent_memory_snapshot_marker_atomic(
    local_dir: &str,
    marker: &AgentMemorySnapshotSyncMarker,
) -> io::Result<()> {
    let boundary = DirectoryBoundary::create(Path::new(local_dir)).await?;
    write_agent_memory_snapshot_marker_atomic_to_boundary(&boundary, marker).await
}

async fn write_agent_memory_snapshot_marker_atomic_to_boundary(
    boundary: &DirectoryBoundary,
    marker: &AgentMemorySnapshotSyncMarker,
) -> io::Result<()> {
    if marker.schema_version != MARKER_SCHEMA_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unsupported snapshot marker schema version",
        ));
    }
    let mut bytes = runtime_codec::encode_bytes(marker).map_err(io::Error::other)?;
    bytes.push(b'\n');
    let (temp, mut file) = boundary.create_bounded_temp_leaf("marker").await?;
    let result = async {
        file.write_all(&bytes).await?;
        file.sync_all().await?;
        drop(file);
        boundary.rename_leaf(&temp, SYNCED_META).await?;
        boundary.sync_directory().await
    }
    .await;
    if result.is_err() {
        let _ = boundary.remove_leaf(&temp).await;
    }
    result
}

async fn read_apply_journal_from_boundary(
    boundary: &DirectoryBoundary,
) -> io::Result<Option<AgentMemorySnapshotApplyJournal>> {
    boundary.revalidate().await?;
    let bytes = match boundary.read_leaf(APPLY_JOURNAL).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            boundary.revalidate().await?;
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    boundary.revalidate().await?;
    runtime_codec::decode_text::<AgentMemorySnapshotApplyJournal>(&NodeBufferUtf8::decode(&bytes))
        .map(Some)
        .map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid snapshot apply journal: {error}"),
            )
        })
}

async fn reject_pending_apply_journal(boundary: &DirectoryBoundary) -> io::Result<()> {
    let Some(journal) = read_apply_journal_from_boundary(boundary).await? else {
        return Ok(());
    };
    // A caller preparing a request has no durable DecisionRecorded identity
    // with which to recover or clean this transaction. Validate it so corrupt
    // intent is observable, then fail closed without touching journal/marker.
    let _ = journal.decode_snapshot_files()?;
    Err(io::Error::new(
        io::ErrorKind::WouldBlock,
        PENDING_APPLY_MESSAGE,
    ))
}

/// Unix publishes the no-replace journal with `linkat(temp, journal)` and then
/// unlinks the temp name. A process death between those two syscalls leaves two
/// names for the exact same inode. Cold recovery may remove only such a known
/// internal journal-temp sibling; a same-shaped name pointing anywhere else is
/// never touched.
#[cfg(unix)]
async fn remove_matching_apply_journal_temp_siblings(
    boundary: &DirectoryBoundary,
) -> io::Result<()> {
    use std::os::unix::fs::MetadataExt;

    let journal = boundary.open_leaf_at(APPLY_JOURNAL, libc::O_RDONLY | libc::O_NONBLOCK, 0)?;
    let journal_metadata = journal.metadata().await?;
    if !journal_metadata.is_file() {
        return Err(not_regular_file_error(&boundary.leaf_path(APPLY_JOURNAL)?));
    }

    boundary.revalidate().await?;
    let mut reader = tokio::fs::read_dir(&boundary.path).await?;
    let mut candidates = Vec::new();
    while let Some(entry) = reader.next_entry().await? {
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };
        if is_internal_journal_temp_leaf(&name) {
            candidates.push(name);
        }
    }
    candidates.sort();
    boundary.revalidate().await?;

    for name in candidates {
        let candidate = match boundary.open_leaf_at(&name, libc::O_RDONLY | libc::O_NONBLOCK, 0) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        let metadata = candidate.metadata().await?;
        if metadata.is_file()
            && metadata.dev() == journal_metadata.dev()
            && metadata.ino() == journal_metadata.ino()
        {
            // APPLY_LOCK excludes every compliant peer while recovery holds
            // this boundary, so the verified name cannot be repointed by
            // another engine between this inode check and unlinkat.
            boundary.remove_leaf(&name).await?;
        }
    }
    boundary.revalidate().await
}

#[cfg(not(unix))]
async fn remove_matching_apply_journal_temp_siblings(
    _boundary: &DirectoryBoundary,
) -> io::Result<()> {
    Ok(())
}

async fn write_apply_journal_atomic_to_boundary(
    boundary: &DirectoryBoundary,
    journal: &AgentMemorySnapshotApplyJournal,
) -> io::Result<()> {
    if journal.schema_version != APPLY_JOURNAL_SCHEMA_VERSION
        || journal.decision != AgentMemoryMergeChoice::Replace
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid snapshot Replace apply journal",
        ));
    }
    // Validate the embedded payload before publishing the durable intent. A
    // malformed journal must never authorize mutation during cold recovery.
    let _ = journal.decode_snapshot_files()?;
    let mut bytes = runtime_codec::encode_bytes(journal).map_err(io::Error::other)?;
    bytes.push(b'\n');
    let (temp, mut file) = boundary.create_bounded_temp_leaf("journal").await?;
    let write_result = async {
        file.write_all(&bytes).await?;
        file.sync_all().await?;
        drop(file);
        boundary.publish_leaf_no_replace(&temp, APPLY_JOURNAL).await
    }
    .await;
    match write_result {
        Ok(()) => boundary.sync_directory().await,
        Err(publish_error) => {
            let _ = boundary.remove_leaf(&temp).await;
            // A peer or a retry may have won the no-replace CAS. Only the exact
            // same self-contained intent is idempotent; never overwrite or
            // clean a journal belonging to another recorded decision.
            match read_apply_journal_from_boundary(boundary).await {
                Ok(Some(existing)) if existing == *journal => boundary.sync_directory().await,
                Ok(Some(_)) => Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "another snapshot Replace apply journal owns this memory directory",
                )),
                Ok(None) => Err(publish_error),
                Err(read_error) => Err(read_error),
            }
        }
    }
}

async fn remove_apply_journal_durable(boundary: &DirectoryBoundary) -> io::Result<()> {
    match boundary.remove_leaf(APPLY_JOURNAL).await {
        Ok(()) => boundary.sync_directory().await,
        Err(error) if error.kind() == io::ErrorKind::NotFound => boundary.sync_directory().await,
        Err(error) => Err(error),
    }
}

async fn preflight_replace_targets(
    boundary: &DirectoryBoundary,
    snapshot_files: &BTreeMap<String, Vec<u8>>,
) -> io::Result<()> {
    boundary.revalidate().await?;
    for name in snapshot_files.keys() {
        validate_apply_leaf_name(name)?;
        boundary.validate_regular_leaf_if_present(name).await?;
    }
    boundary.revalidate().await
}

async fn complete_replace_from_journal(
    boundary: &DirectoryBoundary,
    journal: &AgentMemorySnapshotApplyJournal,
) -> io::Result<()> {
    let snapshot_files = journal.decode_snapshot_files()?;
    remove_matching_apply_journal_temp_siblings(boundary).await?;
    #[cfg(test)]
    maybe_pause_before_replace_mutation_for_test(&boundary.path).await;
    replace_local_from_captured_snapshot(boundary, &snapshot_files).await?;
    boundary.sync_directory().await?;
    #[cfg(test)]
    maybe_crash_replace_for_test(&boundary.path, ReplaceCrashPoint::ReplaceDurable)?;

    write_agent_memory_snapshot_marker_atomic_to_boundary(boundary, &journal.marker()).await?;
    #[cfg(test)]
    maybe_crash_replace_for_test(&boundary.path, ReplaceCrashPoint::MarkerDurable)?;

    remove_apply_journal_durable(boundary).await
}

/// Apply-time capture retaining the caller's pinned local directory boundary.
/// The snapshot is read-only and copied into memory, while every local identity
/// read and later mutation remains bound to the same directory object.
async fn capture_state_with_local_boundary(
    layout: &dyn AgentMemoryLayout,
    agent_type: &str,
    local_boundary: &DirectoryBoundary,
) -> io::Result<Option<CapturedState>> {
    let snapshot_dir = layout.snapshot_dir_for(agent_type);
    let snapshot_boundary = match DirectoryBoundary::open_existing(Path::new(&snapshot_dir)).await {
        Ok(boundary) => boundary,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let snapshot_timestamp = match read_snapshot_timestamp(&snapshot_boundary).await? {
        Some(timestamp) => timestamp,
        None => return Ok(None),
    };
    let snapshot_files = read_flat_files_from_boundary(
        &snapshot_boundary,
        &[SNAPSHOT_META, APPLY_JOURNAL, APPLY_LOCK],
    )
    .await?;
    let local_files =
        read_flat_files_from_boundary(local_boundary, &[SYNCED_META, APPLY_JOURNAL, APPLY_LOCK])
            .await?;
    let snapshot_id = state_id(
        b"klaus-agent-memory-snapshot-v1",
        Some(&snapshot_timestamp),
        &snapshot_files,
    );
    let local_state_id = state_id(b"klaus-agent-memory-local-v1", None, &local_files);
    Ok(Some(CapturedState {
        snapshot_timestamp,
        snapshot_files,
        local_files,
        snapshot_id,
        local_state_id,
    }))
}

async fn read_snapshot_timestamp(
    boundary: &DirectoryBoundary,
) -> io::Result<Option<RuntimeString>> {
    let bytes = match boundary.read_leaf(SNAPSHOT_META).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let raw = NodeBufferUtf8::decode(&bytes);
    let value = match runtime_json::parse(&raw) {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };
    let updated_at = match runtime_codec::decode_object(&value, false, |object| {
        object.required::<RuntimeString>("updatedAt", &[])
    }) {
        Ok(value) if !value.is_empty() => value,
        _ => return Ok(None),
    };
    Ok(Some(updated_at))
}

async fn read_flat_files_from_boundary(
    boundary: &DirectoryBoundary,
    excluded: &[&str],
) -> io::Result<BTreeMap<String, Vec<u8>>> {
    boundary.revalidate().await?;
    let mut reader = tokio::fs::read_dir(&boundary.path).await?;
    let mut files = BTreeMap::new();
    while let Some(entry) = reader.next_entry().await? {
        let metadata = tokio::fs::symlink_metadata(entry.path()).await?;
        if metadata_is_link_like(&metadata) {
            return Err(symlink_file_error(&entry.path()));
        }
        if !metadata.is_file() {
            continue;
        }
        let name = entry.file_name().into_string().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "agent memory contains a non-UTF-8 filename",
            )
        })?;
        if excluded.contains(&name.as_str()) {
            continue;
        }
        if is_internal_temp_leaf(&name) {
            // A process crash can leave an uncommitted marker temp file. It is
            // not user memory and must not perturb localStateId/diff identity.
            continue;
        }
        files.insert(name.clone(), boundary.read_leaf(&name).await?);
    }
    boundary.revalidate().await?;
    Ok(files)
}

fn is_internal_temp_leaf(name: &str) -> bool {
    (name.starts_with(&format!(".{SYNCED_META}.")) && name.ends_with(".tmp"))
        || ((name.starts_with(".klaus-agent-memory-replace-")
            || name.starts_with(".klaus-agent-memory-marker-")
            || is_internal_journal_temp_leaf(name))
            && name.ends_with(".tmp"))
}

fn is_internal_journal_temp_leaf(name: &str) -> bool {
    name.starts_with(".klaus-agent-memory-journal-") && name.ends_with(".tmp")
}

fn validate_apply_leaf_name(name: &str) -> io::Result<()> {
    let leaf = Path::new(name);
    if name.is_empty() || leaf.file_name().is_none() || leaf.components().count() != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot Replace target requires one flat filename",
        ));
    }
    if name == SNAPSHOT_META
        || name == SYNCED_META
        || name == APPLY_JOURNAL
        || name == APPLY_LOCK
        || is_internal_temp_leaf(name)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("snapshot Replace target uses reserved filename {name}"),
        ));
    }
    Ok(())
}

fn symlink_file_error(path: &Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "agent memory file must not be a symbolic link: {}",
            path.display()
        ),
    )
}

#[cfg(not(unix))]
fn configure_no_follow(options: &mut tokio::fs::OpenOptions) {
    #[cfg(windows)]
    options.custom_flags(0x0020_0000); // FILE_FLAG_OPEN_REPARSE_POINT
    #[cfg(not(windows))]
    let _ = options;
}

#[cfg(not(unix))]
async fn reject_symlink_file(path: &Path) -> io::Result<()> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) if metadata_is_link_like(&metadata) => {
            return Err(symlink_file_error(path));
        }
        // A directory is reported as such rather than as a generic non-file: callers branch
        // on `IsADirectory`, and unix reaches that kind by opening the directory and failing
        // the read, which Windows cannot do (it refuses the open outright).
        Ok(metadata) if metadata.is_dir() => {
            return Err(io::Error::new(
                io::ErrorKind::IsADirectory,
                format!("agent memory path is a directory: {}", path.display()),
            ));
        }
        Ok(metadata) if !metadata.is_file() => {
            return Err(not_regular_file_error(path));
        }
        Ok(_) => {}
        Err(error) => return Err(error),
    }
    Ok(())
}

#[cfg(not(unix))]
async fn reject_symlink_file_if_present(path: &Path) -> io::Result<()> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) if metadata_is_link_like(&metadata) => {
            return Err(symlink_file_error(path));
        }
        Ok(metadata) if !metadata.is_file() => {
            return Err(not_regular_file_error(path));
        }
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    Ok(())
}

fn state_id(
    domain: &[u8],
    timestamp: Option<&RuntimeString>,
    files: &BTreeMap<String, Vec<u8>>,
) -> RuntimeString {
    let mut hash = Sha256::new();
    hash.update(domain);
    if let Some(timestamp) = timestamp {
        hash_runtime_string(&mut hash, timestamp);
    }
    hash.update((files.len() as u64).to_be_bytes());
    for (path, content) in files {
        hash.update((path.len() as u64).to_be_bytes());
        hash.update(path.as_bytes());
        hash.update((content.len() as u64).to_be_bytes());
        hash.update(Sha256::digest(content));
    }
    RuntimeString::from(format!("sha256:{}", hex_digest(hash.finalize().as_slice())))
}

fn deterministic_request_id(
    agent_type: &RuntimeString,
    snapshot_id: &RuntimeString,
    local_state_id: &RuntimeString,
) -> RuntimeString {
    let mut hash = Sha256::new();
    hash.update(b"klaus-agent-memory-request-v1");
    hash_runtime_string(&mut hash, agent_type);
    hash.update(b"user");
    hash_runtime_string(&mut hash, snapshot_id);
    hash_runtime_string(&mut hash, local_state_id);
    RuntimeString::from(format!("amsr_{}", hex_digest(hash.finalize().as_slice())))
}

fn hash_runtime_string(hash: &mut Sha256, value: &RuntimeString) {
    hash.update((value.len_utf16() as u64).to_be_bytes());
    for unit in value.as_utf16() {
        hash.update(unit.to_le_bytes());
    }
}

fn hex_digest(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn build_diff(
    snapshot: &BTreeMap<String, Vec<u8>>,
    local: &BTreeMap<String, Vec<u8>>,
) -> AgentMemorySnapshotDiff {
    let mut entries = Vec::new();
    let mut added_count = 0_u64;
    let mut modified_count = 0_u64;
    let mut removed_count = 0_u64;
    for (path, snapshot_content) in snapshot {
        let kind = match local.get(path) {
            None => {
                added_count += 1;
                Some(AgentMemorySnapshotDiffKind::Added)
            }
            Some(local_content) if local_content != snapshot_content => {
                modified_count += 1;
                Some(AgentMemorySnapshotDiffKind::Modified)
            }
            Some(_) => None,
        };
        if let Some(kind) = kind {
            entries.push(AgentMemorySnapshotDiffEntry {
                path: RuntimeString::from(path.as_str()),
                kind,
            });
        }
    }
    for path in local.keys() {
        if !snapshot.contains_key(path) {
            removed_count += 1;
            entries.push(AgentMemorySnapshotDiffEntry {
                path: RuntimeString::from(path.as_str()),
                kind: AgentMemorySnapshotDiffKind::Removed,
            });
        }
    }
    entries.sort_by(|left, right| left.path.cmp(&right.path));
    let truncated = entries.len() > MAX_SNAPSHOT_DIFF_PATHS;
    entries.truncate(MAX_SNAPSHOT_DIFF_PATHS);
    AgentMemorySnapshotDiff {
        added_count,
        modified_count,
        removed_count,
        entries,
        truncated,
    }
}

async fn replace_local_from_captured_snapshot(
    boundary: &DirectoryBoundary,
    snapshot_files: &BTreeMap<String, Vec<u8>>,
) -> io::Result<()> {
    let mut reader = tokio::fs::read_dir(&boundary.path).await?;
    let mut markdown = Vec::new();
    while let Some(entry) = reader.next_entry().await? {
        let metadata = tokio::fs::symlink_metadata(entry.path()).await?;
        if metadata_is_link_like(&metadata) {
            return Err(symlink_file_error(&entry.path()));
        }
        let name = entry.file_name().into_string().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "agent memory contains a non-UTF-8 filename",
            )
        })?;
        // Exact CC boundary: replace removes existing markdown files only.
        if metadata.is_file() && name.ends_with(".md") {
            markdown.push(name);
        }
    }
    // The path-based enumeration is read-only. Validate it still names the
    // pinned directory before mutating; every mutation below is relative to the
    // handle, so a later root rename cannot redirect it.
    boundary.revalidate().await?;
    markdown.sort();
    for (index, name) in markdown.into_iter().enumerate() {
        let _ = index;
        boundary.remove_leaf(&name).await?;
        #[cfg(test)]
        if index == 0 {
            maybe_crash_replace_for_test(&boundary.path, ReplaceCrashPoint::FirstRemove)?;
        }
    }
    for (index, (name, content)) in snapshot_files.iter().enumerate() {
        let _ = index;
        boundary.write_leaf(name, content).await?;
        #[cfg(test)]
        if index == 0 {
            maybe_crash_replace_for_test(&boundary.path, ReplaceCrashPoint::FirstWrite)?;
        }
    }
    boundary.revalidate().await?;
    Ok(())
}

fn timestamp_is_newer(left: &RuntimeString, right: &RuntimeString) -> bool {
    use chrono::{DateTime, NaiveDate, TimeZone, Utc};

    fn parse(value: &RuntimeString) -> Option<i64> {
        let value = value.try_to_utf8().ok()?;
        if let Ok(date) = DateTime::parse_from_rfc3339(&value) {
            return Some(date.timestamp_millis());
        }
        let date = NaiveDate::parse_from_str(&value, "%Y-%m-%d").ok()?;
        Some(
            Utc.from_utc_datetime(&date.and_hms_opt(0, 0, 0)?)
                .timestamp_millis(),
        )
    }

    matches!((parse(left), parse(right)), (Some(left), Some(right)) if left > right)
}

fn decision_key(request: &AgentMemorySnapshotDecisionRequest) -> DecisionKey {
    DecisionKey {
        agent_type: request.agent_type.clone(),
        snapshot_id: request.snapshot_id.clone(),
        local_state_id: request.local_state_id.clone(),
    }
}

fn validate_request(
    request: &AgentMemorySnapshotDecisionRequest,
) -> Result<(), AgentMemorySnapshotRegistryError> {
    if request.scope != AgentMemoryScope::User {
        return Err(AgentMemorySnapshotRegistryError::InvalidRequest(
            "snapshot decisions are user-scoped",
        ));
    }
    if request.request_id.is_empty()
        || request.agent_type.is_empty()
        || request.snapshot_id.is_empty()
        || request.local_state_id.is_empty()
        || request.snapshot_timestamp.is_empty()
    {
        return Err(AgentMemorySnapshotRegistryError::InvalidRequest(
            "snapshot request identities must be non-empty",
        ));
    }
    if request.agent_type.try_to_utf8().is_err() {
        return Err(AgentMemorySnapshotRegistryError::InvalidRequest(
            "snapshot agent type must be a Unicode scalar string",
        ));
    }
    if request.deadline_at < request.requested_at {
        return Err(AgentMemorySnapshotRegistryError::InvalidRequest(
            "snapshot deadline precedes request time",
        ));
    }
    if deterministic_request_id(
        &request.agent_type,
        &request.snapshot_id,
        &request.local_state_id,
    ) != request.request_id
    {
        return Err(AgentMemorySnapshotRegistryError::InvalidRequest(
            "snapshot request id does not match its identity",
        ));
    }
    Ok(())
}

fn validate_audit(
    audit: &AgentMemorySnapshotDecisionAudit,
) -> Result<(), AgentMemorySnapshotRegistryError> {
    match audit.status {
        AgentMemorySnapshotDecisionStatus::Requested => {
            if audit.decision.is_some() || audit.source.is_some() || audit.ended_at.is_some() {
                return Err(AgentMemorySnapshotRegistryError::InvalidRequest(
                    "requested audit contains decision or terminal fields",
                ));
            }
        }
        AgentMemorySnapshotDecisionStatus::DecisionRecorded => {
            if audit.decision.is_none() || audit.source.is_none() || audit.decided_at.is_none() {
                return Err(AgentMemorySnapshotRegistryError::InvalidRequest(
                    "decision-recorded audit lacks decision metadata",
                ));
            }
        }
        AgentMemorySnapshotDecisionStatus::Applied => {
            if audit.decision.is_none()
                || audit.source.is_none()
                || audit.decided_at.is_none()
                || audit.ended_at.is_none()
            {
                return Err(AgentMemorySnapshotRegistryError::InvalidRequest(
                    "applied audit lacks decision or terminal metadata",
                ));
            }
        }
        _ if audit.ended_at.is_none() => {
            return Err(AgentMemorySnapshotRegistryError::InvalidRequest(
                "terminal audit lacks endedAt",
            ));
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::agent_memory::default_agent_memory_layout;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir() -> PathBuf {
        static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "klaus-snapshot-decision-{}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn seed(layout: &dyn AgentMemoryLayout, agent: &str, timestamp: &str, files: &[(&str, &str)]) {
        let dir = layout.snapshot_dir_for(agent);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            Path::new(&dir).join(SNAPSHOT_META),
            format!(r#"{{"updatedAt":"{timestamp}"}}"#),
        )
        .unwrap();
        for (path, contents) in files {
            std::fs::write(Path::new(&dir).join(path), contents).unwrap();
        }
    }

    fn duplicate_staged<T: Clone>(
        staged: &AgentMemorySnapshotStaged<T>,
    ) -> AgentMemorySnapshotStaged<T> {
        AgentMemorySnapshotStaged {
            value: staged.value.clone(),
            token: AgentMemorySnapshotRegistryCommitToken {
                registry_instance_id: staged.token.registry_instance_id,
                before: staged.token.before.clone(),
                after: staged.token.after.clone(),
            },
        }
    }

    #[test]
    fn persistence_inconsistent_apply_error_has_a_stable_type_and_display() {
        let error = AgentMemorySnapshotApplyError::PersistenceInconsistent(
            "rollout flush succeeded but registry CAS failed".into(),
        );
        assert!(matches!(
            &error,
            AgentMemorySnapshotApplyError::PersistenceInconsistent(detail)
                if detail == &RuntimeString::from(
                    "rollout flush succeeded but registry CAS failed"
                )
        ));
        assert_eq!(
            error.to_string(),
            "snapshot decision persistence is inconsistent: rollout flush succeeded but registry CAS failed"
        );
    }

    #[tokio::test]
    async fn identities_and_bounded_diff_are_sorted_and_deterministic() {
        let root = temp_dir();
        let layout = default_agent_memory_layout(Some(root.to_string_lossy().as_ref()), None);
        seed(
            &layout,
            "writer",
            "2026-07-18T00:00:00Z",
            &[("z.md", "z-new"), ("a.md", "same"), ("m.md", "new")],
        );
        let local = layout.dir_for("writer", AgentMemoryScope::User);
        std::fs::create_dir_all(&local).unwrap();
        std::fs::write(Path::new(&local).join("a.md"), "same").unwrap();
        std::fs::write(Path::new(&local).join("z.md"), "z-old").unwrap();
        std::fs::write(Path::new(&local).join("old.md"), "old").unwrap();

        let first = prepare_agent_memory_snapshot_request(&layout, "writer", 10, 20)
            .await
            .unwrap()
            .unwrap();
        let second = prepare_agent_memory_snapshot_request(&layout, "writer", 11, 21)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.request_id, second.request_id);
        assert_eq!(first.snapshot_id, second.snapshot_id);
        assert_eq!(first.local_state_id, second.local_state_id);
        assert_eq!(first.diff.added_count, 1);
        assert_eq!(first.diff.modified_count, 1);
        assert_eq!(first.diff.removed_count, 1);
        assert_eq!(
            first
                .diff
                .entries
                .iter()
                .map(|entry| entry.path.try_to_utf8().unwrap())
                .collect::<Vec<_>>(),
            ["m.md", "old.md", "z.md"]
        );

        // Directory enumeration order does not affect identity.
        std::fs::remove_file(Path::new(&local).join("a.md")).unwrap();
        std::fs::write(Path::new(&local).join("a.md"), "same").unwrap();
        let third = prepare_agent_memory_snapshot_request(&layout, "writer", 12, 22)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.local_state_id, third.local_state_id);
        assert_eq!(first.request_id, third.request_id);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn staged_registry_mutations_are_invisible_and_tokens_are_exactly_bound() {
        let root = temp_dir();
        let layout = default_agent_memory_layout(Some(root.to_string_lossy().as_ref()), None);
        seed(
            &layout,
            "writer",
            "2026-07-18T00:00:00Z",
            &[("MEMORY.md", "shared")],
        );
        let request = prepare_agent_memory_snapshot_request(&layout, "writer", 10, 20)
            .await
            .unwrap()
            .unwrap();

        let registry = AgentMemorySnapshotDecisionRegistry::default();
        let staged_register = registry.stage_register(request.clone()).unwrap();
        let duplicate_register = duplicate_staged(&staged_register);
        let cross_registry_register = duplicate_staged(&staged_register);
        assert!(registry.query(&request.request_id).is_none());
        assert_eq!(
            staged_register.value().audit.status,
            AgentMemorySnapshotDecisionStatus::Requested
        );

        let other_registry = AgentMemorySnapshotDecisionRegistry::default();
        assert_eq!(
            other_registry
                .commit_staged(cross_registry_register)
                .unwrap_err(),
            AgentMemorySnapshotRegistryError::StagedRegistryMismatch
        );
        let registered = registry.commit_staged(staged_register).unwrap();
        assert!(registered.created);
        assert_eq!(
            registry.query(&request.request_id).unwrap().status,
            AgentMemorySnapshotDecisionStatus::Requested
        );
        assert_eq!(
            registry.commit_staged(duplicate_register).unwrap_err(),
            AgentMemorySnapshotRegistryError::StagedCommitConflict
        );

        let staged_decision = registry
            .stage_record_decision(
                &request.request_id,
                AgentMemorySnapshotDecisionStatus::Requested,
                AgentMemoryMergeChoice::Keep,
                AgentMemorySnapshotDecisionSource::Host,
                11,
            )
            .unwrap();
        assert_eq!(
            registry.query(&request.request_id).unwrap().status,
            AgentMemorySnapshotDecisionStatus::Requested,
            "a staged audit must remain invisible before rollout flush"
        );
        assert_eq!(
            staged_decision.value().audit.status,
            AgentMemorySnapshotDecisionStatus::DecisionRecorded
        );
        let mut invalid_applying_stage = duplicate_staged(&staged_decision);
        invalid_applying_stage
            .token
            .after
            .by_request
            .get_mut(&request.request_id)
            .unwrap()
            .applying = true;
        assert_eq!(
            registry.commit_staged(invalid_applying_stage).unwrap_err(),
            AgentMemorySnapshotRegistryError::StagedApplyingState
        );
        registry.commit_staged(staged_decision).unwrap();
        assert_eq!(
            registry.query(&request.request_id).unwrap().status,
            AgentMemorySnapshotDecisionStatus::DecisionRecorded
        );

        let terminal_registry = AgentMemorySnapshotDecisionRegistry::default();
        terminal_registry.register(request.clone()).unwrap();
        let stale_terminal = terminal_registry
            .stage_record_terminal(
                &request.request_id,
                AgentMemorySnapshotDecisionStatus::Requested,
                AgentMemorySnapshotDecisionStatus::TimedOut,
                12,
                None,
            )
            .unwrap();
        terminal_registry
            .record_terminal(
                &request.request_id,
                AgentMemorySnapshotDecisionStatus::Requested,
                AgentMemorySnapshotDecisionStatus::NoHandler,
                12,
                None,
            )
            .unwrap();
        assert_eq!(
            terminal_registry.commit_staged(stale_terminal).unwrap_err(),
            AgentMemorySnapshotRegistryError::StagedCommitConflict
        );

        let exact_terminal_registry = AgentMemorySnapshotDecisionRegistry::default();
        exact_terminal_registry.register(request.clone()).unwrap();
        let staged_terminal = exact_terminal_registry
            .stage_record_terminal(
                &request.request_id,
                AgentMemorySnapshotDecisionStatus::Requested,
                AgentMemorySnapshotDecisionStatus::TimedOut,
                12,
                Some("host timeout".into()),
            )
            .unwrap();
        assert_eq!(
            exact_terminal_registry
                .query(&request.request_id)
                .unwrap()
                .status,
            AgentMemorySnapshotDecisionStatus::Requested
        );
        exact_terminal_registry
            .commit_staged(staged_terminal)
            .unwrap();
        assert_eq!(
            exact_terminal_registry
                .query(&request.request_id)
                .unwrap()
                .status,
            AgentMemorySnapshotDecisionStatus::TimedOut
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn diff_paths_are_bounded_without_losing_exact_counts() {
        let root = temp_dir();
        let layout = default_agent_memory_layout(Some(root.to_string_lossy().as_ref()), None);
        let snapshot = layout.snapshot_dir_for("many");
        std::fs::create_dir_all(&snapshot).unwrap();
        std::fs::write(
            Path::new(&snapshot).join(SNAPSHOT_META),
            r#"{"updatedAt":"2026-07-18T00:00:00Z"}"#,
        )
        .unwrap();
        for index in 0..(MAX_SNAPSHOT_DIFF_PATHS + 17) {
            std::fs::write(
                Path::new(&snapshot).join(format!("{index:03}.md")),
                index.to_string(),
            )
            .unwrap();
        }
        let request = prepare_agent_memory_snapshot_request(&layout, "many", 1, 2)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            request.diff.added_count as usize,
            MAX_SNAPSHOT_DIFF_PATHS + 17
        );
        assert_eq!(request.diff.entries.len(), MAX_SNAPSHOT_DIFF_PATHS);
        assert!(request.diff.truncated);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn registry_is_exactly_once_and_rejects_conflicting_decisions() {
        let root = temp_dir();
        let layout = default_agent_memory_layout(Some(root.to_string_lossy().as_ref()), None);
        seed(
            &layout,
            "writer",
            "2026-07-18T00:00:00Z",
            &[("MEMORY.md", "shared")],
        );
        let request = prepare_agent_memory_snapshot_request(&layout, "writer", 10, 20)
            .await
            .unwrap()
            .unwrap();
        let registry = AgentMemorySnapshotDecisionRegistry::default();
        assert!(registry.register(request.clone()).unwrap().created);
        assert!(!registry.register(request.clone()).unwrap().created);
        let first = registry
            .record_decision(
                &request.request_id,
                AgentMemorySnapshotDecisionStatus::Requested,
                AgentMemoryMergeChoice::Keep,
                AgentMemorySnapshotDecisionSource::Host,
                12,
            )
            .unwrap();
        assert!(!first.idempotent);
        let duplicate = registry
            .record_decision(
                &request.request_id,
                AgentMemorySnapshotDecisionStatus::Requested,
                AgentMemoryMergeChoice::Keep,
                AgentMemorySnapshotDecisionSource::Host,
                13,
            )
            .unwrap();
        assert!(duplicate.idempotent);
        assert_eq!(duplicate.audit.decided_at, Some(12));
        assert_eq!(
            registry.record_decision(
                &request.request_id,
                AgentMemorySnapshotDecisionStatus::Requested,
                AgentMemoryMergeChoice::Replace,
                AgentMemorySnapshotDecisionSource::Host,
                13,
            ),
            Err(AgentMemorySnapshotRegistryError::DecisionConflict)
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn concurrent_registration_and_decision_cas_have_one_winner() {
        let root = temp_dir();
        let layout = default_agent_memory_layout(Some(root.to_string_lossy().as_ref()), None);
        seed(
            &layout,
            "writer",
            "2026-07-18T00:00:00Z",
            &[("MEMORY.md", "shared")],
        );
        let request = prepare_agent_memory_snapshot_request(&layout, "writer", 10, 20)
            .await
            .unwrap()
            .unwrap();
        let registry = std::sync::Arc::new(AgentMemorySnapshotDecisionRegistry::default());
        let created = (0..16)
            .map(|_| {
                let registry = registry.clone();
                let request = request.clone();
                std::thread::spawn(move || registry.register(request).unwrap().created)
            })
            .map(|thread| thread.join().unwrap())
            .filter(|created| *created)
            .count();
        assert_eq!(created, 1);

        let transitioned = (0..16)
            .map(|index| {
                let registry = registry.clone();
                let request_id = request.request_id.clone();
                std::thread::spawn(move || {
                    registry
                        .record_decision(
                            &request_id,
                            AgentMemorySnapshotDecisionStatus::Requested,
                            AgentMemoryMergeChoice::Merge,
                            AgentMemorySnapshotDecisionSource::Host,
                            100 + index,
                        )
                        .unwrap()
                        .idempotent
                })
            })
            .map(|thread| thread.join().unwrap())
            .filter(|idempotent| !*idempotent)
            .count();
        assert_eq!(transitioned, 1);
        assert_eq!(
            registry.query(&request.request_id).unwrap().status,
            AgentMemorySnapshotDecisionStatus::DecisionRecorded
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn cancelled_apply_lease_reopens_the_same_recorded_decision() {
        let root = temp_dir();
        let layout = default_agent_memory_layout(Some(root.to_string_lossy().as_ref()), None);
        seed(
            &layout,
            "writer",
            "2026-07-18T00:00:00Z",
            &[("MEMORY.md", "shared")],
        );
        let request = prepare_agent_memory_snapshot_request(&layout, "writer", 10, 20)
            .await
            .unwrap()
            .unwrap();
        let registry = AgentMemorySnapshotDecisionRegistry::default();
        registry.register(request.clone()).unwrap();
        registry
            .record_decision(
                &request.request_id,
                AgentMemorySnapshotDecisionStatus::Requested,
                AgentMemoryMergeChoice::Keep,
                AgentMemorySnapshotDecisionSource::Host,
                11,
            )
            .unwrap();

        let first_audit = match registry.claim_apply(&request.request_id).unwrap() {
            ApplyClaim::Claimed(audit) => audit,
            ApplyClaim::AlreadyApplied(_) => panic!("decision was not applied"),
        };
        let lease = ApplyLease::new(&registry, &request.request_id);
        drop(lease); // models cancellation/drop of the apply future
        let second_audit = match registry.claim_apply(&request.request_id).unwrap() {
            ApplyClaim::Claimed(audit) => audit,
            ApplyClaim::AlreadyApplied(_) => panic!("decision was not applied"),
        };
        assert_eq!(first_audit, second_audit);
        registry.release_apply_claim(&request.request_id);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn matching_committed_marker_recovers_applied_after_state_publish_cancellation() {
        let root = temp_dir();
        let layout = default_agent_memory_layout(Some(root.to_string_lossy().as_ref()), None);
        seed(
            &layout,
            "writer",
            "2026-07-18T00:00:00Z",
            &[("MEMORY.md", "shared")],
        );
        let local = layout.dir_for("writer", AgentMemoryScope::User);
        std::fs::create_dir_all(&local).unwrap();
        std::fs::write(Path::new(&local).join("MEMORY.md"), "local").unwrap();
        let request = prepare_agent_memory_snapshot_request(&layout, "writer", 10, 20)
            .await
            .unwrap()
            .unwrap();
        let registry = AgentMemorySnapshotDecisionRegistry::default();
        registry.register(request.clone()).unwrap();
        registry
            .record_decision(
                &request.request_id,
                AgentMemorySnapshotDecisionStatus::Requested,
                AgentMemoryMergeChoice::Keep,
                AgentMemorySnapshotDecisionSource::Host,
                11,
            )
            .unwrap();
        write_agent_memory_snapshot_marker_atomic(
            &local,
            &AgentMemorySnapshotSyncMarker {
                schema_version: 2,
                synced_from: request.snapshot_timestamp.clone(),
                snapshot_id: request.snapshot_id.clone(),
                request_id: request.request_id.clone(),
                decision: AgentMemoryMergeChoice::Keep,
                source: AgentMemorySnapshotDecisionSource::Host,
                applied_at: 12,
            },
        )
        .await
        .unwrap();
        // A later local edit would ordinarily make the original CAS stale. The
        // matching committed marker proves the decision already crossed the
        // atomic filesystem boundary before cancellation.
        std::fs::write(Path::new(&local).join("MEMORY.md"), "later edit").unwrap();
        let outcome = apply_recorded_agent_memory_snapshot_decision(
            &layout,
            &registry,
            &request.request_id,
            99,
        )
        .await
        .unwrap();
        assert!(outcome.already_applied);
        assert_eq!(
            outcome.audit.status,
            AgentMemorySnapshotDecisionStatus::Applied
        );
        assert_eq!(outcome.audit.ended_at, Some(12));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn first_durable_marker_wins_across_registries_without_conflicting_replace() {
        for winner_choice in [AgentMemoryMergeChoice::Keep, AgentMemoryMergeChoice::Merge] {
            let root = temp_dir();
            let layout = default_agent_memory_layout(Some(root.to_string_lossy().as_ref()), None);
            seed(
                &layout,
                "writer",
                "2026-07-18T00:00:00Z",
                &[("MEMORY.md", "snapshot"), ("new.md", "new")],
            );
            let local = layout.dir_for("writer", AgentMemoryScope::User);
            std::fs::create_dir_all(&local).unwrap();
            std::fs::write(Path::new(&local).join("MEMORY.md"), "local").unwrap();
            std::fs::write(Path::new(&local).join("orphan.md"), "orphan").unwrap();
            let request = prepare_agent_memory_snapshot_request(&layout, "writer", 10, 20)
                .await
                .unwrap()
                .unwrap();

            let winner = AgentMemorySnapshotDecisionRegistry::default();
            winner.register(request.clone()).unwrap();
            winner
                .record_decision(
                    &request.request_id,
                    AgentMemorySnapshotDecisionStatus::Requested,
                    winner_choice,
                    AgentMemorySnapshotDecisionSource::Host,
                    11,
                )
                .unwrap();
            let winner_outcome = apply_recorded_agent_memory_snapshot_decision(
                &layout,
                &winner,
                &request.request_id,
                12,
            )
            .await
            .unwrap();
            assert_eq!(
                winner_outcome.audit.status,
                AgentMemorySnapshotDecisionStatus::Applied
            );
            let winner_marker = read_agent_memory_snapshot_marker(&layout, "writer")
                .await
                .unwrap()
                .unwrap();

            let loser = AgentMemorySnapshotDecisionRegistry::default();
            loser.register(request.clone()).unwrap();
            loser
                .record_decision(
                    &request.request_id,
                    AgentMemorySnapshotDecisionStatus::Requested,
                    AgentMemoryMergeChoice::Replace,
                    AgentMemorySnapshotDecisionSource::Host,
                    13,
                )
                .unwrap();
            let loser_outcome = apply_recorded_agent_memory_snapshot_decision(
                &layout,
                &loser,
                &request.request_id,
                14,
            )
            .await
            .unwrap();

            assert_eq!(
                loser_outcome.audit.status,
                AgentMemorySnapshotDecisionStatus::Stale
            );
            assert_eq!(
                loser_outcome.audit.error,
                Some(RuntimeString::from(CONFLICTING_DURABLE_MARKER_ERROR))
            );
            assert_eq!(
                std::fs::read_to_string(Path::new(&local).join("MEMORY.md")).unwrap(),
                "local"
            );
            assert_eq!(
                std::fs::read_to_string(Path::new(&local).join("orphan.md")).unwrap(),
                "orphan"
            );
            assert!(!Path::new(&local).join("new.md").exists());
            assert!(!Path::new(&local).join(APPLY_JOURNAL).exists());
            assert_eq!(
                read_agent_memory_snapshot_marker(&layout, "writer")
                    .await
                    .unwrap(),
                Some(winner_marker)
            );
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[tokio::test]
    async fn conflicting_durable_marker_stales_merge_cold_recovery_before_delivery() {
        let root = temp_dir();
        let layout = default_agent_memory_layout(Some(root.to_string_lossy().as_ref()), None);
        seed(
            &layout,
            "writer",
            "2026-07-18T00:00:00Z",
            &[("MEMORY.md", "snapshot")],
        );
        let local = layout.dir_for("writer", AgentMemoryScope::User);
        std::fs::create_dir_all(&local).unwrap();
        std::fs::write(Path::new(&local).join("MEMORY.md"), "local").unwrap();
        let request = prepare_agent_memory_snapshot_request(&layout, "writer", 10, 20)
            .await
            .unwrap()
            .unwrap();

        let winner = AgentMemorySnapshotDecisionRegistry::default();
        winner.register(request.clone()).unwrap();
        winner
            .record_decision(
                &request.request_id,
                AgentMemorySnapshotDecisionStatus::Requested,
                AgentMemoryMergeChoice::Keep,
                AgentMemorySnapshotDecisionSource::Host,
                11,
            )
            .unwrap();
        apply_recorded_agent_memory_snapshot_decision(&layout, &winner, &request.request_id, 12)
            .await
            .unwrap();
        let winner_marker = read_agent_memory_snapshot_marker(&layout, "writer")
            .await
            .unwrap()
            .unwrap();

        let loser = AgentMemorySnapshotDecisionRegistry::default();
        loser.register(request.clone()).unwrap();
        loser
            .record_decision(
                &request.request_id,
                AgentMemorySnapshotDecisionStatus::Requested,
                AgentMemoryMergeChoice::Merge,
                AgentMemorySnapshotDecisionSource::Host,
                13,
            )
            .unwrap();
        let recovered =
            recover_marked_agent_memory_snapshot_decision(&layout, &loser, &request.request_id)
                .await
                .unwrap()
                .expect("the conflicting durable marker must terminalize the merge loser");

        assert_eq!(recovered.status, AgentMemorySnapshotDecisionStatus::Stale);
        assert_eq!(recovered.ended_at, Some(13));
        assert_eq!(
            recovered.error,
            Some(RuntimeString::from(CONFLICTING_DURABLE_MARKER_ERROR))
        );
        assert_eq!(loser.query(&request.request_id), Some(recovered));
        assert_eq!(
            std::fs::read_to_string(Path::new(&local).join("MEMORY.md")).unwrap(),
            "local"
        );
        assert_eq!(
            read_agent_memory_snapshot_marker(&layout, "writer")
                .await
                .unwrap(),
            Some(winner_marker)
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn merge_recovery_requires_existing_matching_provider_admission_marker() {
        let root = temp_dir();
        let layout = default_agent_memory_layout(Some(root.to_string_lossy().as_ref()), None);
        seed(
            &layout,
            "writer",
            "2026-07-18T00:00:00Z",
            &[("MEMORY.md", "shared")],
        );
        let local = layout.dir_for("writer", AgentMemoryScope::User);
        std::fs::create_dir_all(&local).unwrap();
        std::fs::write(Path::new(&local).join("MEMORY.md"), "local").unwrap();
        let request = prepare_agent_memory_snapshot_request(&layout, "writer", 10, 20)
            .await
            .unwrap()
            .unwrap();
        let registry = AgentMemorySnapshotDecisionRegistry::default();
        registry.register(request.clone()).unwrap();
        registry
            .record_decision(
                &request.request_id,
                AgentMemorySnapshotDecisionStatus::Requested,
                AgentMemoryMergeChoice::Merge,
                AgentMemorySnapshotDecisionSource::Host,
                11,
            )
            .unwrap();

        assert_eq!(
            recover_marked_agent_memory_snapshot_decision(&layout, &registry, &request.request_id,)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            registry.query(&request.request_id).unwrap().status,
            AgentMemorySnapshotDecisionStatus::DecisionRecorded
        );
        assert!(read_agent_memory_snapshot_marker(&layout, "writer")
            .await
            .unwrap()
            .is_none());

        write_agent_memory_snapshot_marker_atomic(
            &local,
            &AgentMemorySnapshotSyncMarker {
                schema_version: MARKER_SCHEMA_VERSION,
                synced_from: request.snapshot_timestamp.clone(),
                snapshot_id: request.snapshot_id.clone(),
                request_id: request.request_id.clone(),
                decision: AgentMemoryMergeChoice::Merge,
                source: AgentMemorySnapshotDecisionSource::Host,
                applied_at: 12,
            },
        )
        .await
        .unwrap();
        let recovered =
            recover_marked_agent_memory_snapshot_decision(&layout, &registry, &request.request_id)
                .await
                .unwrap()
                .expect("matching provider-admission marker completes the audit");
        assert_eq!(recovered.status, AgentMemorySnapshotDecisionStatus::Applied);
        assert_eq!(recovered.ended_at, Some(12));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn staged_merge_recovery_is_invisible_and_retains_lock_until_commit() {
        let root = temp_dir();
        let layout = default_agent_memory_layout(Some(root.to_string_lossy().as_ref()), None);
        seed(
            &layout,
            "writer",
            "2026-07-18T00:00:00Z",
            &[("MEMORY.md", "shared")],
        );
        let local = layout.dir_for("writer", AgentMemoryScope::User);
        std::fs::create_dir_all(&local).unwrap();
        std::fs::write(Path::new(&local).join("MEMORY.md"), "local").unwrap();
        let request = prepare_agent_memory_snapshot_request(&layout, "writer", 10, 20)
            .await
            .unwrap()
            .unwrap();
        let registry = AgentMemorySnapshotDecisionRegistry::default();
        registry.register(request.clone()).unwrap();
        let recorded = registry
            .record_decision(
                &request.request_id,
                AgentMemorySnapshotDecisionStatus::Requested,
                AgentMemoryMergeChoice::Merge,
                AgentMemorySnapshotDecisionSource::Host,
                11,
            )
            .unwrap()
            .audit;
        write_agent_memory_snapshot_marker_atomic(
            &local,
            &AgentMemorySnapshotSyncMarker {
                schema_version: MARKER_SCHEMA_VERSION,
                synced_from: request.snapshot_timestamp.clone(),
                snapshot_id: request.snapshot_id.clone(),
                request_id: request.request_id.clone(),
                decision: AgentMemoryMergeChoice::Merge,
                source: AgentMemorySnapshotDecisionSource::Host,
                applied_at: 12,
            },
        )
        .await
        .unwrap();

        let staged = stage_recover_marked_agent_memory_snapshot_decision(
            &layout,
            &registry,
            &request.request_id,
        )
        .await;
        assert_eq!(
            staged.proposed_audit().unwrap().status,
            AgentMemorySnapshotDecisionStatus::Applied
        );
        assert_eq!(
            registry.query(&request.request_id).unwrap().status,
            AgentMemorySnapshotDecisionStatus::DecisionRecorded
        );
        let contender_registry = AgentMemorySnapshotDecisionRegistry::default();
        contender_registry.restore(recorded).unwrap();
        assert!(matches!(
            recover_marked_agent_memory_snapshot_decision(
                &layout,
                &contender_registry,
                &request.request_id,
            )
            .await
            .unwrap_err(),
            AgentMemorySnapshotApplyError::Io(ref error)
                if error.kind() == io::ErrorKind::WouldBlock
        ));

        let recovered = registry
            .commit_staged_filesystem(staged)
            .unwrap()
            .expect("matching marker proposes Applied");
        assert_eq!(recovered.status, AgentMemorySnapshotDecisionStatus::Applied);
        assert_eq!(
            registry.query(&request.request_id).unwrap().status,
            AgentMemorySnapshotDecisionStatus::Applied
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn stale_identity_is_terminal_and_does_not_write_marker() {
        let root = temp_dir();
        let layout = default_agent_memory_layout(Some(root.to_string_lossy().as_ref()), None);
        seed(
            &layout,
            "writer",
            "2026-07-18T00:00:00Z",
            &[("MEMORY.md", "shared")],
        );
        let local = layout.dir_for("writer", AgentMemoryScope::User);
        std::fs::create_dir_all(&local).unwrap();
        std::fs::write(Path::new(&local).join("MEMORY.md"), "local").unwrap();
        let request = prepare_agent_memory_snapshot_request(&layout, "writer", 10, 20)
            .await
            .unwrap()
            .unwrap();
        let registry = AgentMemorySnapshotDecisionRegistry::default();
        registry.register(request.clone()).unwrap();
        registry
            .record_decision(
                &request.request_id,
                AgentMemorySnapshotDecisionStatus::Requested,
                AgentMemoryMergeChoice::Keep,
                AgentMemorySnapshotDecisionSource::Host,
                11,
            )
            .unwrap();
        std::fs::write(Path::new(&local).join("MEMORY.md"), "changed").unwrap();
        let outcome = apply_recorded_agent_memory_snapshot_decision(
            &layout,
            &registry,
            &request.request_id,
            12,
        )
        .await
        .unwrap();
        assert_eq!(
            outcome.audit.status,
            AgentMemorySnapshotDecisionStatus::Stale
        );
        assert!(read_agent_memory_snapshot_marker(&layout, "writer")
            .await
            .unwrap()
            .is_none());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn replace_removes_only_markdown_and_writes_rich_marker_atomically() {
        let root = temp_dir();
        let layout = default_agent_memory_layout(Some(root.to_string_lossy().as_ref()), None);
        seed(
            &layout,
            "writer",
            "2026-07-18T00:00:00Z",
            &[("MEMORY.md", "shared"), ("snapshot.dat", "new")],
        );
        let local = layout.dir_for("writer", AgentMemoryScope::User);
        std::fs::create_dir_all(&local).unwrap();
        std::fs::write(Path::new(&local).join("MEMORY.md"), "local").unwrap();
        std::fs::write(Path::new(&local).join("orphan.md"), "orphan").unwrap();
        std::fs::write(Path::new(&local).join("keep.json"), "keep").unwrap();
        let request = prepare_agent_memory_snapshot_request(&layout, "writer", 10, 20)
            .await
            .unwrap()
            .unwrap();
        let registry = AgentMemorySnapshotDecisionRegistry::default();
        registry.register(request.clone()).unwrap();
        registry
            .record_decision(
                &request.request_id,
                AgentMemorySnapshotDecisionStatus::Requested,
                AgentMemoryMergeChoice::Replace,
                AgentMemorySnapshotDecisionSource::Host,
                11,
            )
            .unwrap();
        let outcome = apply_recorded_agent_memory_snapshot_decision(
            &layout,
            &registry,
            &request.request_id,
            12,
        )
        .await
        .unwrap();
        assert_eq!(
            outcome.audit.status,
            AgentMemorySnapshotDecisionStatus::Applied
        );
        assert_eq!(
            std::fs::read_to_string(Path::new(&local).join("MEMORY.md")).unwrap(),
            "shared"
        );
        assert!(!Path::new(&local).join("orphan.md").exists());
        assert_eq!(
            std::fs::read_to_string(Path::new(&local).join("keep.json")).unwrap(),
            "keep"
        );
        assert_eq!(
            std::fs::read_to_string(Path::new(&local).join("snapshot.dat")).unwrap(),
            "new"
        );
        let marker = read_agent_memory_snapshot_marker(&layout, "writer")
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            marker,
            AgentMemorySnapshotMarkerRead::Current(AgentMemorySnapshotSyncMarker {
                schema_version: 2,
                decision: AgentMemoryMergeChoice::Replace,
                source: AgentMemorySnapshotDecisionSource::Host,
                applied_at: 12,
                ..
            })
        ));
        assert!(
            prepare_agent_memory_snapshot_request(&layout, "writer", 13, 20)
                .await
                .unwrap()
                .is_none()
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn staged_apply_keeps_real_registry_invisible_and_lock_owned_until_commit() {
        let root = temp_dir();
        let layout = default_agent_memory_layout(Some(root.to_string_lossy().as_ref()), None);
        seed(
            &layout,
            "writer",
            "2026-07-18T00:00:00Z",
            &[("MEMORY.md", "snapshot")],
        );
        let local = PathBuf::from(layout.dir_for("writer", AgentMemoryScope::User));
        std::fs::create_dir_all(&local).unwrap();
        std::fs::write(local.join("MEMORY.md"), "local").unwrap();
        let request = prepare_agent_memory_snapshot_request(&layout, "writer", 10, 20)
            .await
            .unwrap()
            .unwrap();
        let registry = AgentMemorySnapshotDecisionRegistry::default();
        registry.register(request.clone()).unwrap();
        let recorded = registry
            .record_decision(
                &request.request_id,
                AgentMemorySnapshotDecisionStatus::Requested,
                AgentMemoryMergeChoice::Replace,
                AgentMemorySnapshotDecisionSource::Host,
                11,
            )
            .unwrap()
            .audit;

        let staged = stage_apply_recorded_agent_memory_snapshot_decision(
            &layout,
            &registry,
            &request.request_id,
            12,
        )
        .await;
        assert_eq!(
            staged.result().unwrap().audit.status,
            AgentMemorySnapshotDecisionStatus::Applied
        );
        assert_eq!(
            staged.proposed_audit().unwrap().status,
            AgentMemorySnapshotDecisionStatus::Applied
        );
        assert!(staged.has_registry_transition());
        assert_eq!(
            registry.query(&request.request_id).unwrap().status,
            AgentMemorySnapshotDecisionStatus::DecisionRecorded,
            "the real registry must not publish before its rollout flush"
        );

        let contender_registry = AgentMemorySnapshotDecisionRegistry::default();
        contender_registry.restore(recorded).unwrap();
        let contender_error = apply_recorded_agent_memory_snapshot_decision(
            &layout,
            &contender_registry,
            &request.request_id,
            13,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            contender_error,
            AgentMemorySnapshotApplyError::Io(ref error)
                if error.kind() == io::ErrorKind::WouldBlock
                    && error.to_string()
                        == "agent memory snapshot transaction is already active"
        ));

        let committed = registry.commit_staged_filesystem(staged).unwrap();
        assert_eq!(
            committed.audit.status,
            AgentMemorySnapshotDecisionStatus::Applied
        );
        assert_eq!(
            registry.query(&request.request_id).unwrap().status,
            AgentMemorySnapshotDecisionStatus::Applied
        );
        let contender_recovery = apply_recorded_agent_memory_snapshot_decision(
            &layout,
            &contender_registry,
            &request.request_id,
            99,
        )
        .await
        .unwrap();
        assert!(contender_recovery.already_applied);
        assert_eq!(contender_recovery.audit.ended_at, Some(12));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn staged_apply_checked_cas_conflict_is_distinct_when_current_matches_proposal_and_releases_lock(
    ) {
        let root = temp_dir();
        let layout = default_agent_memory_layout(Some(root.to_string_lossy().as_ref()), None);
        seed(
            &layout,
            "writer",
            "2026-07-18T00:00:00Z",
            &[("MEMORY.md", "snapshot")],
        );
        let local = PathBuf::from(layout.dir_for("writer", AgentMemoryScope::User));
        std::fs::create_dir_all(&local).unwrap();
        std::fs::write(local.join("MEMORY.md"), "local").unwrap();
        let request = prepare_agent_memory_snapshot_request(&layout, "writer", 10, 20)
            .await
            .unwrap()
            .unwrap();
        let registry = AgentMemorySnapshotDecisionRegistry::default();
        registry.register(request.clone()).unwrap();
        let recorded = registry
            .record_decision(
                &request.request_id,
                AgentMemorySnapshotDecisionStatus::Requested,
                AgentMemoryMergeChoice::Replace,
                AgentMemorySnapshotDecisionSource::Host,
                11,
            )
            .unwrap()
            .audit;
        let staged = stage_apply_recorded_agent_memory_snapshot_decision(
            &layout,
            &registry,
            &request.request_id,
            12,
        )
        .await;
        let proposed = staged.proposed_audit().unwrap().clone();
        registry.restore(proposed.clone()).unwrap();
        assert_eq!(registry.query(&request.request_id), Some(proposed));
        assert!(matches!(
            registry
                .commit_staged_filesystem_checked(staged)
                .unwrap_err(),
            AgentMemorySnapshotRegistryError::StagedCommitConflict
        ));

        // The failed CAS consumed the staged value, so its owned file lock must
        // already be released. An independent replay can immediately recover
        // the exact committed marker.
        let contender_registry = AgentMemorySnapshotDecisionRegistry::default();
        contender_registry.restore(recorded).unwrap();
        let recovered = apply_recorded_agent_memory_snapshot_decision(
            &layout,
            &contender_registry,
            &request.request_id,
            99,
        )
        .await
        .unwrap();
        assert!(recovered.already_applied);
        assert_eq!(recovered.audit.ended_at, Some(12));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn dropping_uncommitted_staged_apply_releases_lock_without_publishing_registry() {
        let root = temp_dir();
        let layout = default_agent_memory_layout(Some(root.to_string_lossy().as_ref()), None);
        seed(
            &layout,
            "writer",
            "2026-07-18T00:00:00Z",
            &[("MEMORY.md", "snapshot")],
        );
        let local = PathBuf::from(layout.dir_for("writer", AgentMemoryScope::User));
        std::fs::create_dir_all(&local).unwrap();
        std::fs::write(local.join("MEMORY.md"), "local").unwrap();
        let request = prepare_agent_memory_snapshot_request(&layout, "writer", 10, 20)
            .await
            .unwrap()
            .unwrap();
        let registry = AgentMemorySnapshotDecisionRegistry::default();
        registry.register(request.clone()).unwrap();
        let recorded = registry
            .record_decision(
                &request.request_id,
                AgentMemorySnapshotDecisionStatus::Requested,
                AgentMemoryMergeChoice::Replace,
                AgentMemorySnapshotDecisionSource::Host,
                11,
            )
            .unwrap()
            .audit;
        let staged = stage_apply_recorded_agent_memory_snapshot_decision(
            &layout,
            &registry,
            &request.request_id,
            12,
        )
        .await;

        let contender_registry = AgentMemorySnapshotDecisionRegistry::default();
        contender_registry.restore(recorded).unwrap();
        assert!(matches!(
            apply_recorded_agent_memory_snapshot_decision(
                &layout,
                &contender_registry,
                &request.request_id,
                13,
            )
            .await
            .unwrap_err(),
            AgentMemorySnapshotApplyError::Io(ref error)
                if error.kind() == io::ErrorKind::WouldBlock
        ));
        drop(staged); // models rollout append/flush failure
        assert_eq!(
            registry.query(&request.request_id).unwrap().status,
            AgentMemorySnapshotDecisionStatus::DecisionRecorded
        );

        let recovered = apply_recorded_agent_memory_snapshot_decision(
            &layout,
            &registry,
            &request.request_id,
            99,
        )
        .await
        .unwrap();
        assert!(recovered.already_applied);
        assert_eq!(recovered.audit.ended_at, Some(12));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn staged_apply_error_can_propose_failed_before_committing_original_error() {
        let root = temp_dir();
        let layout = default_agent_memory_layout(Some(root.to_string_lossy().as_ref()), None);
        seed(
            &layout,
            "writer",
            "2026-07-18T00:00:00Z",
            &[("MEMORY.md", "snapshot")],
        );
        let request = prepare_agent_memory_snapshot_request(&layout, "writer", 10, 20)
            .await
            .unwrap()
            .unwrap();
        let local = PathBuf::from(layout.dir_for("writer", AgentMemoryScope::User));
        // A directory is omitted from localStateId just as before, but cannot
        // be replaced as a flat regular-file target. This reaches the explicit
        // Failed transition after the transaction lock is owned.
        std::fs::create_dir(local.join("MEMORY.md")).unwrap();
        let registry = AgentMemorySnapshotDecisionRegistry::default();
        registry.register(request.clone()).unwrap();
        registry
            .record_decision(
                &request.request_id,
                AgentMemorySnapshotDecisionStatus::Requested,
                AgentMemoryMergeChoice::Replace,
                AgentMemorySnapshotDecisionSource::Host,
                11,
            )
            .unwrap();

        let staged = stage_apply_recorded_agent_memory_snapshot_decision(
            &layout,
            &registry,
            &request.request_id,
            12,
        )
        .await;
        assert!(matches!(
            staged.result(),
            Err(AgentMemorySnapshotApplyError::Io(error))
                if error.kind() == io::ErrorKind::InvalidData
        ));
        assert_eq!(
            staged.proposed_audit().unwrap().status,
            AgentMemorySnapshotDecisionStatus::Failed
        );
        assert_eq!(
            registry.query(&request.request_id).unwrap().status,
            AgentMemorySnapshotDecisionStatus::DecisionRecorded
        );
        let error = registry
            .commit_staged_filesystem_checked(staged)
            .expect("the Failed audit registry CAS must commit")
            .unwrap_err();
        assert!(matches!(
            error,
            AgentMemorySnapshotApplyError::Io(ref error)
                if error.kind() == io::ErrorKind::InvalidData
        ));
        assert_eq!(
            registry.query(&request.request_id).unwrap().status,
            AgentMemorySnapshotDecisionStatus::Failed
        );
        let boundary = DirectoryBoundary::open_existing(&local).await.unwrap();
        drop(boundary.try_acquire_snapshot_apply_lock().await.unwrap());

        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn recoverable_staged_apply_error_has_no_token_and_keeps_its_journal() {
        let root = temp_dir();
        let layout = default_agent_memory_layout(Some(root.to_string_lossy().as_ref()), None);
        seed(
            &layout,
            "writer",
            "2026-07-18T00:00:00Z",
            &[("MEMORY.md", "snapshot")],
        );
        let local = PathBuf::from(layout.dir_for("writer", AgentMemoryScope::User));
        std::fs::create_dir_all(&local).unwrap();
        std::fs::write(local.join("MEMORY.md"), "local").unwrap();
        let request = prepare_agent_memory_snapshot_request(&layout, "writer", 10, 20)
            .await
            .unwrap()
            .unwrap();
        let registry = AgentMemorySnapshotDecisionRegistry::default();
        registry.register(request.clone()).unwrap();
        registry
            .record_decision(
                &request.request_id,
                AgentMemorySnapshotDecisionStatus::Requested,
                AgentMemoryMergeChoice::Replace,
                AgentMemorySnapshotDecisionSource::Host,
                11,
            )
            .unwrap();

        set_replace_crash_for_test(&local, ReplaceCrashPoint::FirstRemove);
        let staged = stage_apply_recorded_agent_memory_snapshot_decision(
            &layout,
            &registry,
            &request.request_id,
            12,
        )
        .await;
        assert!(matches!(
            staged.result(),
            Err(AgentMemorySnapshotApplyError::Io(error))
                if error.kind() == io::ErrorKind::Interrupted
        ));
        assert!(!staged.has_registry_transition());
        assert!(staged.proposed_audit().is_none());
        assert_eq!(
            registry.query(&request.request_id).unwrap().status,
            AgentMemorySnapshotDecisionStatus::DecisionRecorded
        );
        assert!(local.join(APPLY_JOURNAL).is_file());
        assert!(matches!(
            registry.commit_staged_filesystem(staged).unwrap_err(),
            AgentMemorySnapshotApplyError::Io(ref error)
                if error.kind() == io::ErrorKind::Interrupted
        ));

        let recovered = apply_recorded_agent_memory_snapshot_decision(
            &layout,
            &registry,
            &request.request_id,
            99,
        )
        .await
        .unwrap();
        assert_eq!(
            recovered.audit.status,
            AgentMemorySnapshotDecisionStatus::Applied
        );
        assert_eq!(recovered.audit.ended_at, Some(12));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn replace_journal_recovers_every_partial_apply_crash_window() {
        for crash_point in [
            ReplaceCrashPoint::FirstRemove,
            ReplaceCrashPoint::FirstWrite,
            ReplaceCrashPoint::ReplaceDurable,
            ReplaceCrashPoint::MarkerDurable,
        ] {
            let root = temp_dir();
            let layout = default_agent_memory_layout(Some(root.to_string_lossy().as_ref()), None);
            seed(
                &layout,
                "writer",
                "2026-07-18T00:00:00Z",
                &[
                    ("MEMORY.md", "snapshot-memory"),
                    ("second.md", "snapshot-second"),
                ],
            );
            let local = PathBuf::from(layout.dir_for("writer", AgentMemoryScope::User));
            std::fs::create_dir_all(&local).unwrap();
            std::fs::write(local.join("MEMORY.md"), "local-memory").unwrap();
            std::fs::write(local.join("orphan.md"), "local-orphan").unwrap();
            std::fs::write(local.join("keep.json"), "keep").unwrap();

            let request = prepare_agent_memory_snapshot_request(&layout, "writer", 10, 20)
                .await
                .unwrap()
                .unwrap();
            let registry = AgentMemorySnapshotDecisionRegistry::default();
            registry.register(request.clone()).unwrap();
            let recorded = registry
                .record_decision(
                    &request.request_id,
                    AgentMemorySnapshotDecisionStatus::Requested,
                    AgentMemoryMergeChoice::Replace,
                    AgentMemorySnapshotDecisionSource::Host,
                    11,
                )
                .unwrap()
                .audit;

            set_replace_crash_for_test(&local, crash_point);
            let error = apply_recorded_agent_memory_snapshot_decision(
                &layout,
                &registry,
                &request.request_id,
                12,
            )
            .await
            .unwrap_err();
            assert!(matches!(
                error,
                AgentMemorySnapshotApplyError::Io(ref error)
                    if error.kind() == io::ErrorKind::Interrupted
            ));
            assert_eq!(
                registry.query(&request.request_id).unwrap().status,
                AgentMemorySnapshotDecisionStatus::DecisionRecorded,
                "{crash_point:?} must remain recoverable"
            );
            assert!(local.join(APPLY_JOURNAL).is_file());

            // Model a cold process: only the already-durable DecisionRecorded
            // audit and filesystem transaction artifacts survive.
            let recovered_registry = AgentMemorySnapshotDecisionRegistry::default();
            recovered_registry.restore(recorded).unwrap();
            let recovered = apply_recorded_agent_memory_snapshot_decision(
                &layout,
                &recovered_registry,
                &request.request_id,
                99,
            )
            .await
            .unwrap();
            assert_eq!(
                recovered.audit.status,
                AgentMemorySnapshotDecisionStatus::Applied
            );
            assert_eq!(
                recovered.audit.ended_at,
                Some(12),
                "the durable journal/marker owns the original apply timestamp"
            );
            assert_eq!(
                std::fs::read_to_string(local.join("MEMORY.md")).unwrap(),
                "snapshot-memory"
            );
            assert_eq!(
                std::fs::read_to_string(local.join("second.md")).unwrap(),
                "snapshot-second"
            );
            assert!(!local.join("orphan.md").exists());
            assert_eq!(
                std::fs::read_to_string(local.join("keep.json")).unwrap(),
                "keep"
            );
            assert!(!local.join(APPLY_JOURNAL).exists());
            assert!(matches!(
                read_agent_memory_snapshot_marker(&layout, "writer")
                    .await
                    .unwrap(),
                Some(AgentMemorySnapshotMarkerRead::Current(marker))
                    if marker.request_id == request.request_id
                        && marker.decision == AgentMemoryMergeChoice::Replace
                        && marker.applied_at == 12
            ));
            assert!(!std::fs::read_dir(&local)
                .unwrap()
                .filter_map(Result::ok)
                .any(|entry| is_internal_temp_leaf(&entry.file_name().to_string_lossy())));

            std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[tokio::test]
    async fn pending_replace_journal_blocks_concurrent_prepare_until_cold_recovery() {
        let root = temp_dir();
        let layout = default_agent_memory_layout(Some(root.to_string_lossy().as_ref()), None);
        seed(
            &layout,
            "writer",
            "2026-07-18T00:00:00Z",
            &[("MEMORY.md", "snapshot")],
        );
        let local = PathBuf::from(layout.dir_for("writer", AgentMemoryScope::User));
        std::fs::create_dir_all(&local).unwrap();
        std::fs::write(local.join("MEMORY.md"), "local").unwrap();
        let request = prepare_agent_memory_snapshot_request(&layout, "writer", 10, 20)
            .await
            .unwrap()
            .unwrap();
        let registry = AgentMemorySnapshotDecisionRegistry::default();
        registry.register(request.clone()).unwrap();
        let recorded = registry
            .record_decision(
                &request.request_id,
                AgentMemorySnapshotDecisionStatus::Requested,
                AgentMemoryMergeChoice::Replace,
                AgentMemorySnapshotDecisionSource::Host,
                11,
            )
            .unwrap()
            .audit;

        set_replace_crash_for_test(&local, ReplaceCrashPoint::FirstRemove);
        apply_recorded_agent_memory_snapshot_decision(&layout, &registry, &request.request_id, 12)
            .await
            .unwrap_err();
        assert!(local.join(APPLY_JOURNAL).is_file());

        let boundary = DirectoryBoundary::create(&local).await.unwrap();
        let owner_journal = read_apply_journal_from_boundary(&boundary)
            .await
            .unwrap()
            .unwrap();
        let mut foreign_journal = owner_journal.clone();
        foreign_journal.request_id = "foreign-request".into();
        let publish_error = write_apply_journal_atomic_to_boundary(&boundary, &foreign_journal)
            .await
            .unwrap_err();
        assert_eq!(publish_error.kind(), io::ErrorKind::WouldBlock);
        assert_eq!(
            publish_error.to_string(),
            "another snapshot Replace apply journal owns this memory directory"
        );
        assert_eq!(
            read_apply_journal_from_boundary(&boundary)
                .await
                .unwrap()
                .unwrap(),
            owner_journal,
            "no-replace publication must preserve the first durable intent"
        );
        drop(boundary);

        let error = prepare_agent_memory_snapshot_request(&layout, "writer", 13, 20)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        assert_eq!(error.to_string(), PENDING_APPLY_MESSAGE);
        assert_eq!(
            registry.query(&request.request_id).unwrap().status,
            AgentMemorySnapshotDecisionStatus::DecisionRecorded
        );
        assert!(read_agent_memory_snapshot_marker(&layout, "writer")
            .await
            .unwrap()
            .is_none());

        let recovered_registry = AgentMemorySnapshotDecisionRegistry::default();
        recovered_registry.restore(recorded).unwrap();
        let recovered = apply_recorded_agent_memory_snapshot_decision(
            &layout,
            &recovered_registry,
            &request.request_id,
            99,
        )
        .await
        .unwrap();
        assert_eq!(
            recovered.audit.status,
            AgentMemorySnapshotDecisionStatus::Applied
        );
        assert_eq!(recovered.audit.ended_at, Some(12));
        assert!(!local.join(APPLY_JOURNAL).exists());

        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cold_recovery_removes_only_same_inode_journal_temp_siblings() {
        use std::os::unix::fs::MetadataExt;

        let root = temp_dir();
        let layout = default_agent_memory_layout(Some(root.to_string_lossy().as_ref()), None);
        seed(
            &layout,
            "writer",
            "2026-07-18T00:00:00Z",
            &[("MEMORY.md", "snapshot")],
        );
        let local = PathBuf::from(layout.dir_for("writer", AgentMemoryScope::User));
        std::fs::create_dir_all(&local).unwrap();
        std::fs::write(local.join("MEMORY.md"), "local").unwrap();
        let request = prepare_agent_memory_snapshot_request(&layout, "writer", 10, 20)
            .await
            .unwrap()
            .unwrap();
        let registry = AgentMemorySnapshotDecisionRegistry::default();
        registry.register(request.clone()).unwrap();
        let recorded = registry
            .record_decision(
                &request.request_id,
                AgentMemorySnapshotDecisionStatus::Requested,
                AgentMemoryMergeChoice::Replace,
                AgentMemorySnapshotDecisionSource::Host,
                11,
            )
            .unwrap()
            .audit;
        set_replace_crash_for_test(&local, ReplaceCrashPoint::FirstRemove);
        apply_recorded_agent_memory_snapshot_decision(&layout, &registry, &request.request_id, 12)
            .await
            .unwrap_err();

        // Recreate the exact on-disk state of a process death after
        // linkat(temp, APPLY_JOURNAL) but before unlinkat(temp): two names for
        // one inode. A same-shaped unrelated temp proves cleanup is identity-
        // based rather than a filename glob.
        let matching_temp = local.join(".klaus-agent-memory-journal-crashed-1.tmp");
        let unrelated_temp = local.join(".klaus-agent-memory-journal-unrelated-2.tmp");
        std::fs::hard_link(local.join(APPLY_JOURNAL), &matching_temp).unwrap();
        std::fs::write(&unrelated_temp, "do-not-delete").unwrap();
        let journal_metadata = std::fs::metadata(local.join(APPLY_JOURNAL)).unwrap();
        assert_eq!(
            std::fs::metadata(&matching_temp).unwrap().ino(),
            journal_metadata.ino()
        );
        assert_ne!(
            std::fs::metadata(&unrelated_temp).unwrap().ino(),
            journal_metadata.ino()
        );

        let recovered_registry = AgentMemorySnapshotDecisionRegistry::default();
        recovered_registry.restore(recorded).unwrap();
        let recovered = apply_recorded_agent_memory_snapshot_decision(
            &layout,
            &recovered_registry,
            &request.request_id,
            99,
        )
        .await
        .unwrap();
        assert_eq!(
            recovered.audit.status,
            AgentMemorySnapshotDecisionStatus::Applied
        );
        assert!(!matching_temp.exists());
        assert_eq!(
            std::fs::read_to_string(&unrelated_temp).unwrap(),
            "do-not-delete"
        );
        assert!(!local.join(APPLY_JOURNAL).exists());

        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn active_replace_owner_excludes_independent_contender_and_loser_retries_idempotently() {
        let root = temp_dir();
        let layout = default_agent_memory_layout(Some(root.to_string_lossy().as_ref()), None);
        seed(
            &layout,
            "writer",
            "2026-07-18T00:00:00Z",
            &[("MEMORY.md", "snapshot")],
        );
        let local = PathBuf::from(layout.dir_for("writer", AgentMemoryScope::User));
        std::fs::create_dir_all(&local).unwrap();
        std::fs::write(local.join("MEMORY.md"), "local").unwrap();
        std::fs::write(local.join("orphan.md"), "orphan").unwrap();
        let request = prepare_agent_memory_snapshot_request(&layout, "writer", 10, 20)
            .await
            .unwrap()
            .unwrap();
        let owner_registry = std::sync::Arc::new(AgentMemorySnapshotDecisionRegistry::default());
        owner_registry.register(request.clone()).unwrap();
        let recorded = owner_registry
            .record_decision(
                &request.request_id,
                AgentMemorySnapshotDecisionStatus::Requested,
                AgentMemoryMergeChoice::Replace,
                AgentMemorySnapshotDecisionSource::Host,
                11,
            )
            .unwrap()
            .audit;
        let contender_registry = AgentMemorySnapshotDecisionRegistry::default();
        contender_registry.restore(recorded).unwrap();

        let (reached, resume) = pause_before_replace_mutation_for_test(&local);
        let owner_layout = layout.clone();
        let owner_registry_task = owner_registry.clone();
        let owner_request_id = request.request_id.clone();
        let owner = tokio::spawn(async move {
            apply_recorded_agent_memory_snapshot_decision(
                &owner_layout,
                owner_registry_task.as_ref(),
                &owner_request_id,
                12,
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), reached.notified())
            .await
            .expect("owner must reach the first-mutation gate");

        assert!(local.join(APPLY_JOURNAL).is_file());
        assert_eq!(
            std::fs::read_to_string(local.join("MEMORY.md")).unwrap(),
            "local"
        );
        assert!(local.join("orphan.md").is_file());
        assert!(read_agent_memory_snapshot_marker(&layout, "writer")
            .await
            .unwrap()
            .is_none());

        let error = apply_recorded_agent_memory_snapshot_decision(
            &layout,
            &contender_registry,
            &request.request_id,
            13,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            AgentMemorySnapshotApplyError::Io(ref error)
                if error.kind() == io::ErrorKind::WouldBlock
                    && error.to_string()
                        == "agent memory snapshot transaction is already active"
        ));
        assert_eq!(
            contender_registry
                .query(&request.request_id)
                .unwrap()
                .status,
            AgentMemorySnapshotDecisionStatus::DecisionRecorded
        );
        assert_eq!(
            std::fs::read_to_string(local.join("MEMORY.md")).unwrap(),
            "local"
        );
        assert!(local.join("orphan.md").is_file());
        assert!(read_agent_memory_snapshot_marker(&layout, "writer")
            .await
            .unwrap()
            .is_none());

        resume.notify_one();
        let owner_outcome = owner.await.unwrap().unwrap();
        assert_eq!(
            owner_outcome.audit.status,
            AgentMemorySnapshotDecisionStatus::Applied
        );
        let loser_retry = apply_recorded_agent_memory_snapshot_decision(
            &layout,
            &contender_registry,
            &request.request_id,
            99,
        )
        .await
        .unwrap();
        assert!(loser_retry.already_applied);
        assert_eq!(loser_retry.audit.ended_at, Some(12));
        assert_eq!(
            std::fs::read_to_string(local.join("MEMORY.md")).unwrap(),
            "snapshot"
        );
        assert!(!local.join("orphan.md").exists());
        assert!(local.join(APPLY_LOCK).is_file());

        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn replace_directory_sync_failures_never_publish_applied() {
        // 1: intent rename, 2: replaced directory, 3: marker rename,
        // 4: journal cleanup. Every boundary must fail closed and remain
        // recoverable from the same durable DecisionRecorded audit.
        for failed_sync in 1..=4 {
            let root = temp_dir();
            let layout = default_agent_memory_layout(Some(root.to_string_lossy().as_ref()), None);
            seed(
                &layout,
                "writer",
                "2026-07-18T00:00:00Z",
                &[("MEMORY.md", "snapshot")],
            );
            let local = PathBuf::from(layout.dir_for("writer", AgentMemoryScope::User));
            std::fs::create_dir_all(&local).unwrap();
            std::fs::write(local.join("MEMORY.md"), "local").unwrap();
            let request = prepare_agent_memory_snapshot_request(&layout, "writer", 10, 20)
                .await
                .unwrap()
                .unwrap();
            let registry = AgentMemorySnapshotDecisionRegistry::default();
            registry.register(request.clone()).unwrap();
            let recorded = registry
                .record_decision(
                    &request.request_id,
                    AgentMemorySnapshotDecisionStatus::Requested,
                    AgentMemoryMergeChoice::Replace,
                    AgentMemorySnapshotDecisionSource::Host,
                    11,
                )
                .unwrap()
                .audit;

            fail_directory_sync_for_test(&local, failed_sync);
            let error = apply_recorded_agent_memory_snapshot_decision(
                &layout,
                &registry,
                &request.request_id,
                12,
            )
            .await
            .unwrap_err();
            assert!(matches!(
                error,
                AgentMemorySnapshotApplyError::Io(ref error)
                    if error.to_string() == "injected snapshot directory sync failure"
            ));
            assert_eq!(
                registry.query(&request.request_id).unwrap().status,
                AgentMemorySnapshotDecisionStatus::DecisionRecorded,
                "directory sync {failed_sync} must not publish Applied"
            );

            let recovered_registry = AgentMemorySnapshotDecisionRegistry::default();
            recovered_registry.restore(recorded).unwrap();
            let recovered = apply_recorded_agent_memory_snapshot_decision(
                &layout,
                &recovered_registry,
                &request.request_id,
                99,
            )
            .await
            .unwrap();
            assert_eq!(
                recovered.audit.status,
                AgentMemorySnapshotDecisionStatus::Applied
            );
            assert_eq!(recovered.audit.ended_at, Some(12));
            assert!(!local.join(APPLY_JOURNAL).exists());

            std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[tokio::test]
    async fn matching_marker_never_cleans_a_mismatched_replace_journal() {
        let root = temp_dir();
        let layout = default_agent_memory_layout(Some(root.to_string_lossy().as_ref()), None);
        seed(
            &layout,
            "writer",
            "2026-07-18T00:00:00Z",
            &[("MEMORY.md", "snapshot")],
        );
        let local = PathBuf::from(layout.dir_for("writer", AgentMemoryScope::User));
        std::fs::create_dir_all(&local).unwrap();
        std::fs::write(local.join("MEMORY.md"), "local").unwrap();
        let request = prepare_agent_memory_snapshot_request(&layout, "writer", 10, 20)
            .await
            .unwrap()
            .unwrap();
        let registry = AgentMemorySnapshotDecisionRegistry::default();
        registry.register(request.clone()).unwrap();
        let recorded = registry
            .record_decision(
                &request.request_id,
                AgentMemorySnapshotDecisionStatus::Requested,
                AgentMemoryMergeChoice::Replace,
                AgentMemorySnapshotDecisionSource::Host,
                11,
            )
            .unwrap()
            .audit;

        set_replace_crash_for_test(&local, ReplaceCrashPoint::MarkerDurable);
        apply_recorded_agent_memory_snapshot_decision(&layout, &registry, &request.request_id, 12)
            .await
            .unwrap_err();

        let boundary = DirectoryBoundary::create(&local).await.unwrap();
        let mut journal = read_apply_journal_from_boundary(&boundary)
            .await
            .unwrap()
            .unwrap();
        journal.request_id = "different-request".into();
        let mut bytes = runtime_codec::encode_bytes(&journal).unwrap();
        bytes.push(b'\n');
        // Model external corruption/replacement. The production publisher is
        // intentionally no-replace and must not offer an API that overwrites
        // an existing owner's intent.
        std::fs::write(local.join(APPLY_JOURNAL), bytes).unwrap();
        drop(boundary);

        let recovered_registry = AgentMemorySnapshotDecisionRegistry::default();
        recovered_registry.restore(recorded).unwrap();
        let error = apply_recorded_agent_memory_snapshot_decision(
            &layout,
            &recovered_registry,
            &request.request_id,
            99,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            AgentMemorySnapshotApplyError::Io(ref error)
                if error.kind() == io::ErrorKind::InvalidData
                    && error.to_string().contains("does not match the committed marker")
        ));
        assert!(local.join(APPLY_JOURNAL).is_file());
        assert_eq!(
            recovered_registry
                .query(&request.request_id)
                .unwrap()
                .status,
            AgentMemorySnapshotDecisionStatus::DecisionRecorded
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn replace_rejects_same_name_symlink_without_touching_external_target() {
        use std::os::unix::fs::symlink;

        let root = temp_dir();
        let outside_root = temp_dir();
        let outside = outside_root.join("sentinel.md");
        std::fs::write(&outside, "outside-sentinel").unwrap();
        let layout = default_agent_memory_layout(Some(root.to_string_lossy().as_ref()), None);
        seed(
            &layout,
            "writer",
            "2026-07-18T00:00:00Z",
            &[("MEMORY.md", "snapshot")],
        );

        // Capture an empty local state, then replace the snapshot's destination
        // name with a symlink. A symlink-blind identity check still sees the
        // original empty state and a normal write would follow it.
        let request = prepare_agent_memory_snapshot_request(&layout, "writer", 10, 20)
            .await
            .unwrap()
            .unwrap();
        let local = layout.dir_for("writer", AgentMemoryScope::User);
        std::fs::create_dir_all(&local).unwrap();
        symlink(&outside, Path::new(&local).join("MEMORY.md")).unwrap();

        let registry = AgentMemorySnapshotDecisionRegistry::default();
        registry.register(request.clone()).unwrap();
        registry
            .record_decision(
                &request.request_id,
                AgentMemorySnapshotDecisionStatus::Requested,
                AgentMemoryMergeChoice::Replace,
                AgentMemorySnapshotDecisionSource::Host,
                11,
            )
            .unwrap();

        let error = apply_recorded_agent_memory_snapshot_decision(
            &layout,
            &registry,
            &request.request_id,
            12,
        )
        .await
        .unwrap_err();
        let AgentMemorySnapshotApplyError::Io(error) = error else {
            panic!("symlink revalidation must fail as I/O");
        };
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("symbolic link"));
        assert_eq!(
            registry.query(&request.request_id).unwrap().status,
            AgentMemorySnapshotDecisionStatus::Failed
        );
        assert_eq!(
            std::fs::read_to_string(&outside).unwrap(),
            "outside-sentinel"
        );
        assert!(
            std::fs::symlink_metadata(Path::new(&local).join("MEMORY.md"))
                .unwrap()
                .file_type()
                .is_symlink()
        );
        let direct_error = DirectoryBoundary::open_existing(Path::new(&local))
            .await
            .unwrap()
            .write_leaf("MEMORY.md", b"must-not-follow")
            .await
            .unwrap_err();
        assert_eq!(direct_error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            std::fs::read_to_string(&outside).unwrap(),
            "outside-sentinel"
        );
        assert!(read_agent_memory_snapshot_marker(&layout, "writer")
            .await
            .unwrap()
            .is_none());

        std::fs::remove_dir_all(root).unwrap();
        std::fs::remove_dir_all(outside_root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn prepare_rejects_symlinked_snapshot_root() {
        use std::os::unix::fs::symlink;

        let root = temp_dir();
        let outside_root = temp_dir();
        std::fs::write(
            outside_root.join(SNAPSHOT_META),
            r#"{"updatedAt":"2026-07-18T00:00:00Z"}"#,
        )
        .unwrap();
        let sentinel = outside_root.join("MEMORY.md");
        std::fs::write(&sentinel, "outside-snapshot").unwrap();
        let layout = default_agent_memory_layout(Some(root.to_string_lossy().as_ref()), None);
        let snapshot = PathBuf::from(layout.snapshot_dir_for("writer"));
        std::fs::create_dir_all(snapshot.parent().unwrap()).unwrap();
        symlink(&outside_root, &snapshot).unwrap();

        let error = prepare_agent_memory_snapshot_request(&layout, "writer", 10, 20)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("symbolic link"));
        assert_eq!(
            std::fs::read_to_string(&sentinel).unwrap(),
            "outside-snapshot"
        );

        std::fs::remove_dir_all(root).unwrap();
        std::fs::remove_dir_all(outside_root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn replace_and_marker_reject_identity_equivalent_symlinked_local_root() {
        use std::os::unix::fs::symlink;

        let root = temp_dir();
        let outside_root = temp_dir();
        let layout = default_agent_memory_layout(Some(root.to_string_lossy().as_ref()), None);
        seed(
            &layout,
            "writer",
            "2026-07-18T00:00:00Z",
            &[("MEMORY.md", "snapshot")],
        );
        let local = PathBuf::from(layout.dir_for("writer", AgentMemoryScope::User));
        std::fs::create_dir_all(&local).unwrap();
        std::fs::write(local.join("MEMORY.md"), "outside-sentinel").unwrap();
        let request = prepare_agent_memory_snapshot_request(&layout, "writer", 10, 20)
            .await
            .unwrap()
            .unwrap();

        // The replacement directory has byte-identical content, so content-only
        // localStateId revalidation cannot distinguish it from the captured root.
        std::fs::write(outside_root.join("MEMORY.md"), "outside-sentinel").unwrap();
        std::fs::remove_dir_all(&local).unwrap();
        symlink(&outside_root, &local).unwrap();

        let registry = AgentMemorySnapshotDecisionRegistry::default();
        registry.register(request.clone()).unwrap();
        registry
            .record_decision(
                &request.request_id,
                AgentMemorySnapshotDecisionStatus::Requested,
                AgentMemoryMergeChoice::Replace,
                AgentMemorySnapshotDecisionSource::Host,
                11,
            )
            .unwrap();
        let error = apply_recorded_agent_memory_snapshot_decision(
            &layout,
            &registry,
            &request.request_id,
            12,
        )
        .await
        .unwrap_err();
        let AgentMemorySnapshotApplyError::Io(error) = error else {
            panic!("symlinked local root must fail as I/O");
        };
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("symbolic link"));
        assert_eq!(
            registry.query(&request.request_id).unwrap().status,
            AgentMemorySnapshotDecisionStatus::Failed
        );
        assert_eq!(
            std::fs::read_to_string(outside_root.join("MEMORY.md")).unwrap(),
            "outside-sentinel"
        );
        assert!(!outside_root.join(SYNCED_META).exists());

        let marker = AgentMemorySnapshotSyncMarker {
            schema_version: MARKER_SCHEMA_VERSION,
            synced_from: request.snapshot_timestamp,
            snapshot_id: request.snapshot_id,
            request_id: request.request_id,
            decision: AgentMemoryMergeChoice::Replace,
            source: AgentMemorySnapshotDecisionSource::Host,
            applied_at: 12,
        };
        let marker_error =
            write_agent_memory_snapshot_marker_atomic(local.to_string_lossy().as_ref(), &marker)
                .await
                .unwrap_err();
        assert_eq!(marker_error.kind(), io::ErrorKind::InvalidData);
        assert!(marker_error.to_string().contains("symbolic link"));
        assert!(!outside_root.join(SYNCED_META).exists());

        std::fs::remove_dir_all(root).unwrap();
        std::fs::remove_dir_all(outside_root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pinned_directory_boundary_cannot_be_redirected_after_validation() {
        use std::os::unix::fs::symlink;

        let root = temp_dir();
        let outside_root = temp_dir();
        let local = root.join("local");
        let moved_local = root.join("moved-local");
        std::fs::create_dir_all(&local).unwrap();
        std::fs::write(local.join("MEMORY.md"), "captured-local").unwrap();
        std::fs::write(outside_root.join("MEMORY.md"), "outside-sentinel").unwrap();
        let boundary = DirectoryBoundary::open_existing(&local).await.unwrap();

        // Replace the validated path after the directory handle was acquired.
        // Child writes must stay relative to the pinned handle, never the new
        // symlink target, and final revalidation must report the substitution.
        std::fs::rename(&local, &moved_local).unwrap();
        symlink(&outside_root, &local).unwrap();
        boundary
            .write_leaf("MEMORY.md", b"pinned-local")
            .await
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(moved_local.join("MEMORY.md")).unwrap(),
            "pinned-local"
        );
        assert_eq!(
            std::fs::read_to_string(outside_root.join("MEMORY.md")).unwrap(),
            "outside-sentinel"
        );
        let error = boundary.revalidate().await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("symbolic link"));

        std::fs::remove_dir_all(root).unwrap();
        std::fs::remove_dir_all(outside_root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn complete_apply_pins_local_root_before_identity_revalidation() {
        use std::sync::atomic::{AtomicBool, Ordering};

        struct SwapOnSnapshotLookupLayout {
            snapshot: PathBuf,
            local: PathBuf,
            moved_local: PathBuf,
            decoy: PathBuf,
            armed: AtomicBool,
        }

        impl AgentMemoryLayout for SwapOnSnapshotLookupLayout {
            fn dir_for(&self, _agent_type: &str, _scope: AgentMemoryScope) -> String {
                self.local.to_string_lossy().into_owned()
            }

            fn snapshot_dir_for(&self, _agent_type: &str) -> String {
                if self.armed.swap(false, Ordering::SeqCst) {
                    std::fs::rename(&self.local, &self.moved_local).unwrap();
                    std::fs::rename(&self.decoy, &self.local).unwrap();
                }
                self.snapshot.to_string_lossy().into_owned()
            }
        }

        let root = temp_dir();
        let snapshot = root.join("snapshot");
        let local = root.join("local");
        let moved_local = root.join("moved-local");
        let decoy = root.join("decoy");
        std::fs::create_dir_all(&snapshot).unwrap();
        std::fs::write(
            snapshot.join(SNAPSHOT_META),
            r#"{"updatedAt":"2026-07-18T00:00:00Z"}"#,
        )
        .unwrap();
        std::fs::write(snapshot.join("MEMORY.md"), "snapshot-new").unwrap();
        std::fs::create_dir_all(&local).unwrap();
        std::fs::write(local.join("MEMORY.md"), "identity-equivalent-old").unwrap();
        std::fs::create_dir_all(&decoy).unwrap();
        std::fs::write(decoy.join("MEMORY.md"), "identity-equivalent-old").unwrap();
        let layout = SwapOnSnapshotLookupLayout {
            snapshot,
            local: local.clone(),
            moved_local: moved_local.clone(),
            decoy,
            armed: AtomicBool::new(false),
        };

        let request = prepare_agent_memory_snapshot_request(&layout, "writer", 10, 20)
            .await
            .unwrap()
            .unwrap();
        let registry = AgentMemorySnapshotDecisionRegistry::default();
        registry.register(request.clone()).unwrap();
        registry
            .record_decision(
                &request.request_id,
                AgentMemorySnapshotDecisionStatus::Requested,
                AgentMemoryMergeChoice::Replace,
                AgentMemorySnapshotDecisionSource::Host,
                11,
            )
            .unwrap();
        layout.armed.store(true, Ordering::SeqCst);

        let error = apply_recorded_agent_memory_snapshot_decision(
            &layout,
            &registry,
            &request.request_id,
            12,
        )
        .await
        .unwrap_err();
        let AgentMemorySnapshotApplyError::Io(error) = error else {
            panic!("root substitution must fail as I/O");
        };
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("changed during reconciliation"));
        assert_eq!(
            registry.query(&request.request_id).unwrap().status,
            AgentMemorySnapshotDecisionStatus::Failed
        );
        assert_eq!(
            std::fs::read_to_string(moved_local.join("MEMORY.md")).unwrap(),
            "identity-equivalent-old"
        );
        assert_eq!(
            std::fs::read_to_string(local.join("MEMORY.md")).unwrap(),
            "identity-equivalent-old"
        );
        assert!(!moved_local.join(SYNCED_META).exists());
        assert!(!local.join(SYNCED_META).exists());

        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn replace_does_not_truncate_same_name_external_hard_link() {
        use std::os::unix::fs::MetadataExt;

        let root = temp_dir();
        let outside_root = temp_dir();
        let outside = outside_root.join("sentinel.dat");
        std::fs::write(&outside, "outside-sentinel").unwrap();
        let layout = default_agent_memory_layout(Some(root.to_string_lossy().as_ref()), None);
        seed(
            &layout,
            "writer",
            "2026-07-18T00:00:00Z",
            &[("snapshot.dat", "snapshot-new")],
        );
        let local = layout.dir_for("writer", AgentMemoryScope::User);
        std::fs::create_dir_all(&local).unwrap();
        std::fs::hard_link(&outside, Path::new(&local).join("snapshot.dat")).unwrap();

        let request = prepare_agent_memory_snapshot_request(&layout, "writer", 10, 20)
            .await
            .unwrap()
            .unwrap();
        let registry = AgentMemorySnapshotDecisionRegistry::default();
        registry.register(request.clone()).unwrap();
        registry
            .record_decision(
                &request.request_id,
                AgentMemorySnapshotDecisionStatus::Requested,
                AgentMemoryMergeChoice::Replace,
                AgentMemorySnapshotDecisionSource::Host,
                11,
            )
            .unwrap();
        let outcome = apply_recorded_agent_memory_snapshot_decision(
            &layout,
            &registry,
            &request.request_id,
            12,
        )
        .await
        .unwrap();

        assert_eq!(
            outcome.audit.status,
            AgentMemorySnapshotDecisionStatus::Applied
        );
        assert_eq!(
            std::fs::read_to_string(&outside).unwrap(),
            "outside-sentinel"
        );
        assert_eq!(
            std::fs::read_to_string(Path::new(&local).join("snapshot.dat")).unwrap(),
            "snapshot-new"
        );
        assert_eq!(std::fs::metadata(&outside).unwrap().nlink(), 1);

        std::fs::remove_dir_all(root).unwrap();
        std::fs::remove_dir_all(outside_root).unwrap();
    }

    #[tokio::test]
    async fn replace_rejects_same_name_non_regular_leaf_without_marker() {
        let root = temp_dir();
        let layout = default_agent_memory_layout(Some(root.to_string_lossy().as_ref()), None);
        seed(
            &layout,
            "writer",
            "2026-07-18T00:00:00Z",
            &[("snapshot.dat", "snapshot-new")],
        );
        let local = layout.dir_for("writer", AgentMemoryScope::User);
        std::fs::create_dir_all(Path::new(&local).join("snapshot.dat")).unwrap();
        std::fs::write(
            Path::new(&local).join("snapshot.dat/sentinel"),
            "directory-sentinel",
        )
        .unwrap();

        let request = prepare_agent_memory_snapshot_request(&layout, "writer", 10, 20)
            .await
            .unwrap()
            .unwrap();
        let registry = AgentMemorySnapshotDecisionRegistry::default();
        registry.register(request.clone()).unwrap();
        registry
            .record_decision(
                &request.request_id,
                AgentMemorySnapshotDecisionStatus::Requested,
                AgentMemoryMergeChoice::Replace,
                AgentMemorySnapshotDecisionSource::Host,
                11,
            )
            .unwrap();
        let error = apply_recorded_agent_memory_snapshot_decision(
            &layout,
            &registry,
            &request.request_id,
            12,
        )
        .await
        .unwrap_err();
        let AgentMemorySnapshotApplyError::Io(error) = error else {
            panic!("non-regular destination must fail as I/O");
        };

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("not a regular file"));
        assert_eq!(
            registry.query(&request.request_id).unwrap().status,
            AgentMemorySnapshotDecisionStatus::Failed
        );
        assert_eq!(
            std::fs::read_to_string(Path::new(&local).join("snapshot.dat/sentinel")).unwrap(),
            "directory-sentinel"
        );
        assert!(!Path::new(&local).join(SYNCED_META).exists());

        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn marker_atomic_update_replaces_existing_regular_file() {
        let root = temp_dir();
        let layout = default_agent_memory_layout(Some(root.to_string_lossy().as_ref()), None);
        let local = layout.dir_for("writer", AgentMemoryScope::User);
        let first = AgentMemorySnapshotSyncMarker {
            schema_version: MARKER_SCHEMA_VERSION,
            synced_from: "2026-07-17T00:00:00Z".into(),
            snapshot_id: "sha256:first".into(),
            request_id: "amsr_first".into(),
            decision: AgentMemoryMergeChoice::Keep,
            source: AgentMemorySnapshotDecisionSource::Host,
            applied_at: 10,
        };
        let second = AgentMemorySnapshotSyncMarker {
            schema_version: MARKER_SCHEMA_VERSION,
            synced_from: "2026-07-18T00:00:00Z".into(),
            snapshot_id: "sha256:second".into(),
            request_id: "amsr_second".into(),
            decision: AgentMemoryMergeChoice::Replace,
            source: AgentMemorySnapshotDecisionSource::Host,
            applied_at: 20,
        };
        write_agent_memory_snapshot_marker_atomic(&local, &first)
            .await
            .unwrap();
        write_agent_memory_snapshot_marker_atomic(&local, &second)
            .await
            .unwrap();
        assert_eq!(
            read_agent_memory_snapshot_marker(&layout, "writer")
                .await
                .unwrap(),
            Some(AgentMemorySnapshotMarkerRead::Current(second))
        );
        assert!(!std::fs::read_dir(&local)
            .unwrap()
            .filter_map(Result::ok)
            .any(|entry| entry.file_name().to_string_lossy().ends_with(".tmp")));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn reads_legacy_marker_but_new_failed_write_never_publishes_partial_marker() {
        let root = temp_dir();
        let layout = default_agent_memory_layout(Some(root.to_string_lossy().as_ref()), None);
        seed(
            &layout,
            "writer",
            "2026-07-18T00:00:00Z",
            &[("MEMORY.md", "shared")],
        );
        let local = layout.dir_for("writer", AgentMemoryScope::User);
        std::fs::create_dir_all(&local).unwrap();
        std::fs::write(Path::new(&local).join("MEMORY.md"), "local").unwrap();
        std::fs::write(
            Path::new(&local).join(SYNCED_META),
            r#"{"syncedFrom":"2026-07-01T00:00:00Z"}"#,
        )
        .unwrap();
        assert!(matches!(
            read_agent_memory_snapshot_marker(&layout, "writer")
                .await
                .unwrap(),
            Some(AgentMemorySnapshotMarkerRead::Legacy { .. })
        ));
        let request = prepare_agent_memory_snapshot_request(&layout, "writer", 10, 20)
            .await
            .unwrap()
            .unwrap();
        let registry = AgentMemorySnapshotDecisionRegistry::default();
        registry.register(request.clone()).unwrap();
        registry
            .record_decision(
                &request.request_id,
                AgentMemorySnapshotDecisionStatus::Requested,
                AgentMemoryMergeChoice::Keep,
                AgentMemorySnapshotDecisionSource::Host,
                11,
            )
            .unwrap();
        std::fs::remove_file(Path::new(&local).join(SYNCED_META)).unwrap();
        std::fs::create_dir(Path::new(&local).join(SYNCED_META)).unwrap();
        let error = apply_recorded_agent_memory_snapshot_decision(
            &layout,
            &registry,
            &request.request_id,
            12,
        )
        .await
        .unwrap_err();
        assert!(matches!(error, AgentMemorySnapshotApplyError::Io(_)));
        assert_eq!(
            registry.query(&request.request_id).unwrap().status,
            AgentMemorySnapshotDecisionStatus::Failed
        );
        assert!(matches!(
            read_agent_memory_snapshot_marker(&layout, "writer").await,
            Err(error) if error.kind() == io::ErrorKind::IsADirectory
        ));
        assert!(!std::fs::read_dir(&local)
            .unwrap()
            .filter_map(Result::ok)
            .any(|entry| entry.file_name().to_string_lossy().ends_with(".tmp")));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn default_policy_never_silently_keeps_without_a_host() {
        let policy = AgentMemorySnapshotDecisionPolicy::default();
        assert_eq!(
            policy.fallback(AgentMemorySnapshotDecisionStatus::NoHandler, false),
            None
        );
        assert_eq!(
            policy.fallback(AgentMemorySnapshotDecisionStatus::TimedOut, false),
            None
        );
        assert_eq!(
            policy.fallback(AgentMemorySnapshotDecisionStatus::Disconnected, false),
            None
        );
        assert_eq!(
            policy.fallback(AgentMemorySnapshotDecisionStatus::Requested, true),
            Some((
                AgentMemoryMergeChoice::Replace,
                AgentMemorySnapshotDecisionSource::FirstUsePolicy
            ))
        );
    }

    #[test]
    fn request_audit_policy_and_marker_runtime_codecs_round_trip_exact_fields() {
        let request = AgentMemorySnapshotDecisionRequest {
            request_id: "amsr_1".into(),
            agent_type: "writer".into(),
            scope: AgentMemoryScope::User,
            snapshot_timestamp: "2026-07-18T00:00:00Z".into(),
            snapshot_id: "sha256:snapshot".into(),
            local_state_id: "sha256:local".into(),
            diff: AgentMemorySnapshotDiff {
                added_count: 1,
                modified_count: 2,
                removed_count: 3,
                entries: vec![AgentMemorySnapshotDiffEntry {
                    path: "a.md".into(),
                    kind: AgentMemorySnapshotDiffKind::Modified,
                }],
                truncated: true,
            },
            first_use: false,
            requested_at: 10,
            deadline_at: 20,
        };
        let audit = AgentMemorySnapshotDecisionAudit {
            request,
            status: AgentMemorySnapshotDecisionStatus::Applied,
            decision: Some(AgentMemoryMergeChoice::Merge),
            source: Some(AgentMemorySnapshotDecisionSource::Host),
            decided_at: Some(11),
            ended_at: Some(12),
            error: None,
        };
        let encoded = runtime_codec::encode_text(&audit).unwrap();
        let decoded: AgentMemorySnapshotDecisionAudit =
            runtime_codec::decode_text(&encoded).unwrap();
        assert_eq!(decoded, audit);
        let text = encoded.try_to_utf8().unwrap();
        assert!(text.contains(r#""status":"applied""#));
        assert!(text.contains(r#""source":"host""#));

        let policy = AgentMemorySnapshotDecisionPolicy {
            timeout_ms: 1234,
            first_use: AgentMemorySnapshotPolicyAction::Initialize,
            timeout: AgentMemorySnapshotPolicyAction::Keep,
            disconnect: AgentMemorySnapshotPolicyAction::Merge,
            no_handler: AgentMemorySnapshotPolicyAction::Replace,
        };
        let encoded = runtime_codec::encode_text(&policy).unwrap();
        assert_eq!(
            runtime_codec::decode_text::<AgentMemorySnapshotDecisionPolicy>(&encoded).unwrap(),
            policy
        );

        let marker = AgentMemorySnapshotSyncMarker {
            schema_version: 2,
            synced_from: "2026-07-18T00:00:00Z".into(),
            snapshot_id: "sha256:snapshot".into(),
            request_id: "amsr_1".into(),
            decision: AgentMemoryMergeChoice::Replace,
            source: AgentMemorySnapshotDecisionSource::FirstUsePolicy,
            applied_at: 12,
        };
        let encoded = runtime_codec::encode_text(&marker).unwrap();
        assert_eq!(
            runtime_codec::decode_text::<AgentMemorySnapshotSyncMarker>(&encoded).unwrap(),
            marker
        );
    }
}
