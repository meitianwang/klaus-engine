//! Wire types for the steering queue: the Host-facing snapshot/mutation
//! surface and the persisted checkpoint records, together with their
//! hand-written runtime codecs. Queue behavior lives in the parent module.

use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};
use crate::schemas::content::ContentBlock;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

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

/// Dequeue order: `now` (0) > `next` (1) > `later` (2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QueuePriority {
    Now,
    Next,
    Later,
}

impl RuntimeEncode for QueuePriority {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Now => "now",
            Self::Next => "next",
            Self::Later => "later",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for QueuePriority {
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

impl QueuePriority {
    pub(super) fn rank(self) -> u8 {
        match self {
            QueuePriority::Now => 0,
            QueuePriority::Next => 1,
            QueuePriority::Later => 2,
        }
    }
}

/// A queued steering message's payload: plain text or structured content.
#[derive(Debug, Clone, PartialEq)]
pub enum QueuedValue {
    Text(RuntimeString),
    Blocks(Vec<ContentBlock>),
}

/// How a drained steer message must land in the thread history.
///
/// A steer is one wire verb (`session/steer`) serving two very different senders,
/// and they need different history shapes:
///
/// - `User`: a human follow-up typed into a running turn. It must persist as its
///   OWN user message, exactly like the same text sent while idle, so the
///   transcript keeps a durable user bubble for it after the turn settles and
///   folds. Gluing it into a tool result (the `Delegation` shape) made the bubble
///   vanish on history takeover, because a tool result is process evidence and
///   folds away.
/// - `Delegation`: a cross-conversation delivery whose envelope rides the LAST
///   tool result's text. The renderer lifts that envelope back out into its own
///   bubble, and the envelope carries the sender id the recipient needs to reply
///   — a plain user message would lose both.
///
/// `None` decodes as `Delegation`: rollouts and queue snapshots written before
/// this field existed all held delegations or engine nudges, and neither may
/// change shape on replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SteerDelivery {
    User,
    Delegation,
}

impl RuntimeEncode for SteerDelivery {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::User => "user",
            Self::Delegation => "delegation",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for SteerDelivery {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("user", &[]),
                RuntimeEnumVariant::new("delegation", &[]),
            ],
            &[Self::User, Self::Delegation],
        )
    }
}

/// A queued steering message — the engine-relevant subset of a host command
/// (REPL/bridge-only fields are intentionally dropped).
#[derive(Debug, Clone, PartialEq)]
pub struct QueuedMessage {
    /// The message content: plain text or structured content blocks.
    pub value: QueuedValue,
    /// Free-form mode tag (e.g. `prompt`, `task-notification`) — drives the drain
    /// filter.
    pub mode: RuntimeString,
    /// Defaults to `next` on enqueue, `later` on `enqueue_pending_notification`.
    pub priority: Option<QueuePriority>,
    pub uuid: Option<RuntimeString>,
    /// Owning session/thread. Exposed so a reconnecting Host cannot accidentally
    /// apply a UUID-scoped mutation to a queue snapshot from another session.
    pub session_id: Option<RuntimeString>,
    /// When true the resulting message is hidden in the transcript but visible to
    /// the model.
    pub is_meta: Option<bool>,
    /// Provenance (`None` = human).
    pub origin: Option<RuntimeString>,
    /// Target agent; `None` = the main thread. The drain filters by this for agent
    /// isolation.
    pub agent_id: Option<RuntimeString>,
    /// False retains mailbox input without admitting another inference turn.
    /// Omitted values preserve explicit steering and older queue checkpoints.
    pub trigger_turn: Option<bool>,
    /// Host correlation for deferred validation/application. Runtime-only: queue
    /// snapshots must not expose protocol submission identifiers.
    pub correlation_id: Option<RuntimeString>,
    /// Decoded PDF bytes represented by this immutable queued value.
    pub pdf_bytes: usize,
    /// Number of PDF document parts represented by this queued value.
    pub pdf_documents: usize,
    /// How the drain must persist this steer once applied; see [`SteerDelivery`].
    /// `None` means the legacy tool-result shape (`SteerDelivery::Delegation`).
    pub delivery: Option<SteerDelivery>,
    /// Host rendering metadata survives queue checkpointing but stays outside model input.
    pub display_metadata: Option<RuntimeJson>,
}

impl QueuedMessage {
    pub fn triggers_turn(&self) -> bool {
        self.trigger_turn.unwrap_or_else(|| {
            // Older completion checkpoints did not carry an admission flag.
            !(self.mode == "task-notification"
                && self
                    .origin
                    .as_ref()
                    .is_some_and(|origin| origin == "background-task"))
        })
    }
}

/// Whether a queued item is still Host-mutable or has been atomically reserved
/// by the active model continuation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueItemState {
    Pending,
    Reserved,
}

impl RuntimeEncode for QueueItemState {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(
            out,
            match self {
                Self::Pending => "pending",
                Self::Reserved => "reserved",
            },
        )
    }
}

impl RuntimeDecode for QueueItemState {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("pending", &[]),
                RuntimeEnumVariant::new("reserved", &[]),
            ],
            &[Self::Pending, Self::Reserved],
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueueSnapshotItem {
    pub message: QueuedMessage,
    pub state: QueueItemState,
    pub mutable: bool,
}

impl RuntimeEncode for QueueSnapshotItem {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("message", &self.message)?;
        object.field("state", &self.state)?;
        object.field("mutable", &self.mutable)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for QueueSnapshotItem {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                message: object.required("message", &[])?,
                state: object.required("state", &[])?,
                mutable: object.required("mutable", &[])?,
            })
        })
    }
}

/// Authoritative per-session queue state. `revision` advances on enqueue,
/// reservation, mutation, drain/take, rejection, and clear.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueSnapshotStatus {
    Authoritative,
    /// The queue can no longer prove whether the first failed checkpoint was
    /// committed. The snapshot body remains the last state that was proven
    /// authoritative; `error` identifies the first durability failure.
    PersistenceFailed,
}

impl RuntimeEncode for QueueSnapshotStatus {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(
            out,
            match self {
                Self::Authoritative => "authoritative",
                Self::PersistenceFailed => "persistence-failed",
            },
        )
    }
}

impl RuntimeDecode for QueueSnapshotStatus {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("authoritative", &[]),
                RuntimeEnumVariant::new("persistence-failed", &[]),
            ],
            &[Self::Authoritative, Self::PersistenceFailed],
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueueSnapshot {
    pub revision: u64,
    pub items: Vec<QueueSnapshotItem>,
    pub status: QueueSnapshotStatus,
    pub error: Option<RuntimeString>,
}

impl RuntimeEncode for QueueSnapshot {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("revision", &self.revision)?;
        object.field("items", &self.items)?;
        object.field("status", &self.status)?;
        if let Some(error) = &self.error {
            object.field("error", error)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for QueueSnapshot {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, true, |object| {
            let snapshot = Self {
                revision: object.required("revision", &[])?,
                items: object.required("items", &[])?,
                status: object.required("status", &[])?,
                error: object.optional("error", &[])?,
            };
            match (&snapshot.status, &snapshot.error) {
                (QueueSnapshotStatus::Authoritative, None)
                | (QueueSnapshotStatus::PersistenceFailed, Some(_)) => Ok(snapshot),
                (QueueSnapshotStatus::Authoritative, Some(_)) => {
                    Err(RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(
                        "an authoritative queue snapshot cannot carry a persistence error".into(),
                    )))
                }
                (QueueSnapshotStatus::PersistenceFailed, None) => {
                    Err(RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(
                        "a persistence-failed queue snapshot requires an error".into(),
                    )))
                }
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct QueueEdit {
    pub value: Option<QueuedValue>,
    pub mode: Option<RuntimeString>,
    pub priority: Option<QueuePriority>,
}

impl RuntimeEncode for QueueEdit {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        if let Some(value) = &self.value {
            object.field("value", value)?;
        }
        if let Some(mode) = &self.mode {
            object.field("mode", mode)?;
        }
        if let Some(priority) = &self.priority {
            object.field("priority", priority)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for QueueEdit {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                value: object.optional("value", &[])?,
                mode: object.optional("mode", &[])?,
                priority: object.optional("priority", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueTerminalState {
    Removed,
    Drained,
    Taken,
    Rejected,
    Cleared,
    /// The active continuation that owned this item ended without durably
    /// consuming it. Shutdown and crash recovery use this terminal state so a
    /// reconnecting Host never observes a ghost `reserved`/running item.
    Interrupted,
}

impl RuntimeEncode for QueueTerminalState {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(
            out,
            match self {
                Self::Removed => "removed",
                Self::Drained => "drained",
                Self::Taken => "taken",
                Self::Rejected => "rejected",
                Self::Cleared => "cleared",
                Self::Interrupted => "interrupted",
            },
        )
    }
}

impl RuntimeDecode for QueueTerminalState {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("removed", &[]),
                RuntimeEnumVariant::new("drained", &[]),
                RuntimeEnumVariant::new("taken", &[]),
                RuntimeEnumVariant::new("rejected", &[]),
                RuntimeEnumVariant::new("cleared", &[]),
                RuntimeEnumVariant::new("interrupted", &[]),
            ],
            &[
                Self::Removed,
                Self::Drained,
                Self::Taken,
                Self::Rejected,
                Self::Cleared,
                Self::Interrupted,
            ],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueMutationStatus {
    Applied,
    /// The in-process CAS committed, but the authoritative queue checkpoint
    /// could not be proven durable. Replaying the same mutation id in this
    /// process returns this terminal result without executing the mutation a
    /// second time.
    OutcomeUnknown,
    /// The queue was already durability-poisoned before this mutation's CAS
    /// body could run. No new revision was executed.
    PersistenceFailed,
    /// Session teardown closed queue admission before this mutation's CAS body
    /// could run. The caller may retry with a new/live session endpoint.
    NotAdmitted,
    StaleRevision,
    Missing,
    Busy,
    Invalid,
    MutationConflict,
}

impl RuntimeEncode for QueueMutationStatus {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(
            out,
            match self {
                Self::Applied => "applied",
                Self::OutcomeUnknown => "outcome-unknown",
                Self::PersistenceFailed => "persistence-failed",
                Self::NotAdmitted => "not-admitted",
                Self::StaleRevision => "stale-revision",
                Self::Missing => "missing",
                Self::Busy => "busy",
                Self::Invalid => "invalid",
                Self::MutationConflict => "mutation-conflict",
            },
        )
    }
}

impl RuntimeDecode for QueueMutationStatus {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("applied", &[]),
                RuntimeEnumVariant::new("outcome-unknown", &[]),
                RuntimeEnumVariant::new("persistence-failed", &[]),
                RuntimeEnumVariant::new("not-admitted", &[]),
                RuntimeEnumVariant::new("stale-revision", &[]),
                RuntimeEnumVariant::new("missing", &[]),
                RuntimeEnumVariant::new("busy", &[]),
                RuntimeEnumVariant::new("invalid", &[]),
                RuntimeEnumVariant::new("mutation-conflict", &[]),
            ],
            &[
                Self::Applied,
                Self::OutcomeUnknown,
                Self::PersistenceFailed,
                Self::NotAdmitted,
                Self::StaleRevision,
                Self::Missing,
                Self::Busy,
                Self::Invalid,
                Self::MutationConflict,
            ],
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueueMutationResult {
    pub mutation_id: RuntimeString,
    pub status: QueueMutationStatus,
    pub snapshot: QueueSnapshot,
    pub terminal_state: Option<QueueTerminalState>,
    pub error: Option<RuntimeString>,
}

impl RuntimeEncode for QueueMutationResult {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("mutationId", &self.mutation_id)?;
        object.field("status", &self.status)?;
        object.field("snapshot", &self.snapshot)?;
        if let Some(value) = &self.terminal_state {
            object.field("terminalState", value)?;
        }
        if let Some(value) = &self.error {
            object.field("error", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for QueueMutationResult {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                mutation_id: object.required("mutationId", &[])?,
                status: object.required("status", &[])?,
                snapshot: object.required("snapshot", &[])?,
                terminal_state: object.optional("terminalState", &[])?,
                error: object.optional("error", &[])?,
            })
        })
    }
}

impl RuntimeEncode for QueuedValue {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        match self {
            Self::Text(text) => text.encode_runtime(out),
            Self::Blocks(blocks) => blocks.encode_runtime(out),
        }
    }
}

impl RuntimeDecode for QueuedValue {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        match value {
            RuntimeJson::String(text) => Ok(Self::Text(text.clone())),
            RuntimeJson::Array(_) => Ok(Self::Blocks(Vec::<ContentBlock>::decode_runtime(value)?)),
            _ => Err(RuntimeCodecError::new(
                RuntimeCodecErrorKind::NoMatchingUntaggedVariant,
            )),
        }
    }
}

impl RuntimeEncode for QueuedMessage {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("value", &self.value)?;
        object.field("mode", &self.mode)?;
        if let Some(value) = &self.priority {
            object.field("priority", value)?;
        }
        if let Some(value) = &self.uuid {
            object.field("uuid", value)?;
        }
        if let Some(value) = &self.session_id {
            object.field("sessionId", value)?;
        }
        if let Some(value) = &self.is_meta {
            object.field("isMeta", value)?;
        }
        if let Some(value) = &self.origin {
            object.field("origin", value)?;
        }
        if let Some(value) = &self.agent_id {
            object.field("agentId", value)?;
        }
        if let Some(value) = &self.trigger_turn {
            object.field("triggerTurn", value)?;
        }
        if let Some(value) = &self.delivery {
            object.field("delivery", value)?;
        }
        if let Some(value) = &self.display_metadata {
            object.field("displayMetadata", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for QueuedMessage {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                value: object.required("value", &[])?,
                mode: object.required("mode", &[])?,
                priority: object.optional("priority", &[])?,
                uuid: object.optional("uuid", &[])?,
                session_id: object.optional("sessionId", &[])?,
                is_meta: object.optional("isMeta", &[])?,
                origin: object.optional("origin", &[])?,
                agent_id: object.optional("agentId", &[])?,
                trigger_turn: object.optional("triggerTurn", &[])?,
                correlation_id: None,
                pdf_bytes: 0,
                pdf_documents: 0,
                delivery: object.optional("delivery", &[])?,
                display_metadata: object.optional("displayMetadata", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum QueueMutationRequest {
    Edit {
        uuid: RuntimeString,
        expected_revision: u64,
        edit: QueueEdit,
    },
    Remove {
        uuid: RuntimeString,
        expected_revision: u64,
    },
    Reorder {
        uuid: RuntimeString,
        before_uuid: Option<RuntimeString>,
        expected_revision: u64,
    },
}

impl QueueMutationRequest {
    pub(super) fn touches_any(&self, uuids: &HashSet<RuntimeString>) -> bool {
        match self {
            Self::Edit { uuid, .. } | Self::Remove { uuid, .. } => uuids.contains(uuid),
            Self::Reorder {
                uuid, before_uuid, ..
            } => {
                uuids.contains(uuid)
                    || before_uuid
                        .as_ref()
                        .is_some_and(|before_uuid| uuids.contains(before_uuid))
            }
        }
    }
}

impl RuntimeEncode for QueueMutationRequest {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut fields = RuntimeObjectEncoder::new();
        let tag = match self {
            Self::Edit {
                uuid,
                expected_revision,
                edit,
            } => {
                fields.field("uuid", uuid)?;
                fields.field("expectedRevision", expected_revision)?;
                fields.field("edit", edit)?;
                "edit"
            }
            Self::Remove {
                uuid,
                expected_revision,
            } => {
                fields.field("uuid", uuid)?;
                fields.field("expectedRevision", expected_revision)?;
                "remove"
            }
            Self::Reorder {
                uuid,
                before_uuid,
                expected_revision,
            } => {
                fields.field("uuid", uuid)?;
                if let Some(before_uuid) = before_uuid {
                    fields.field("beforeUuid", before_uuid)?;
                }
                fields.field("expectedRevision", expected_revision)?;
                "reorder"
            }
        };
        runtime_codec::encode_internal(out, "type", &[], tag, &fields.finish())
    }
}

impl RuntimeDecode for QueueMutationRequest {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            true,
            "type",
            &[],
            &[
                RuntimeEnumVariant::new("edit", &[]),
                RuntimeEnumVariant::new("remove", &[]),
                RuntimeEnumVariant::new("reorder", &[]),
            ],
            |variant, object| match variant {
                0 => Ok(Self::Edit {
                    uuid: object.required("uuid", &[])?,
                    expected_revision: object.required("expectedRevision", &[])?,
                    edit: object.required("edit", &[])?,
                }),
                1 => Ok(Self::Remove {
                    uuid: object.required("uuid", &[])?,
                    expected_revision: object.required("expectedRevision", &[])?,
                }),
                2 => Ok(Self::Reorder {
                    uuid: object.required("uuid", &[])?,
                    before_uuid: object.optional("beforeUuid", &[])?,
                    expected_revision: object.required("expectedRevision", &[])?,
                }),
                _ => Err(RuntimeCodecError::new(
                    RuntimeCodecErrorKind::UnknownEnumVariant,
                )),
            },
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct PersistedQueueItem {
    pub(super) message: QueuedMessage,
    pub(super) pdf_bytes: usize,
    pub(super) pdf_documents: usize,
}

impl RuntimeEncode for PersistedQueueItem {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("message", &self.message)?;
        object.field("pdfBytes", &self.pdf_bytes)?;
        object.field("pdfDocuments", &self.pdf_documents)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for PersistedQueueItem {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, true, |object| {
            Ok(Self {
                message: object.required("message", &[])?,
                pdf_bytes: object.required("pdfBytes", &[])?,
                pdf_documents: object.required("pdfDocuments", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct PersistedQueueTombstone {
    pub(super) uuid: RuntimeString,
    pub(super) state: QueueTerminalState,
}

impl RuntimeEncode for PersistedQueueTombstone {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("uuid", &self.uuid)?;
        object.field("state", &self.state)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for PersistedQueueTombstone {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, true, |object| {
            Ok(Self {
                uuid: object.required("uuid", &[])?,
                state: object.required("state", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct PersistedQueueMutation {
    pub(super) mutation_id: RuntimeString,
    pub(super) request: QueueMutationRequest,
    pub(super) result: QueueMutationResult,
}

impl RuntimeEncode for PersistedQueueMutation {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("mutationId", &self.mutation_id)?;
        object.field("request", &self.request)?;
        object.field("result", &self.result)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for PersistedQueueMutation {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, true, |object| {
            Ok(Self {
                mutation_id: object.required("mutationId", &[])?,
                request: object.required("request", &[])?,
                result: object.required("result", &[])?,
            })
        })
    }
}

/// Complete core-owned queue checkpoint written to the session rollout. This is
/// deliberately not part of Host initialization: only `Agent::resume_session`
/// may feed a decoded checkpoint back into a new in-process queue.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct QueuePersistenceState {
    pub(super) schema_version: u64,
    pub(super) session_id: RuntimeString,
    pub(super) revision: u64,
    pub(super) items: Vec<PersistedQueueItem>,
    pub(super) reserved: Vec<RuntimeString>,
    pub(super) resume_reserved: Vec<RuntimeString>,
    pub(super) tombstones: Vec<PersistedQueueTombstone>,
    pub(super) mutations: Vec<PersistedQueueMutation>,
}

impl RuntimeEncode for QueuePersistenceState {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("schemaVersion", &self.schema_version)?;
        object.field("sessionId", &self.session_id)?;
        object.field("revision", &self.revision)?;
        object.field("items", &self.items)?;
        object.field("reserved", &self.reserved)?;
        if !self.resume_reserved.is_empty() {
            object.field("resumeReserved", &self.resume_reserved)?;
        }
        object.field("tombstones", &self.tombstones)?;
        object.field("mutations", &self.mutations)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for QueuePersistenceState {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, true, |object| {
            Ok(Self {
                schema_version: object.required("schemaVersion", &[])?,
                session_id: object.required("sessionId", &[])?,
                revision: object.required("revision", &[])?,
                items: object.required("items", &[])?,
                reserved: object.required("reserved", &[])?,
                resume_reserved: object.optional("resumeReserved", &[])?.unwrap_or_default(),
                tombstones: object.required("tombstones", &[])?,
                mutations: object.required("mutations", &[])?,
            })
        })
    }
}
