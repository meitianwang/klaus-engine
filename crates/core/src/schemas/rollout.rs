//! Append-only, replayable rollout records.

use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObject, RuntimeObjectDecoder, RuntimeObjectEncoder,
    RuntimeString,
};
use crate::schemas::background_process::BackgroundProcessRecord;
use crate::schemas::message::{Message, Usage};
use crate::schemas::subagent_lifecycle::SubagentLifecycleRecord;
use crate::types::permission::ExternalPermissionMode;

/// Rollout file schema version.
///
/// v1: `session_meta | turn_context | message | compacted{summary,replacedRange}`.
/// v2: adds optional versioning to `session_meta`, an optional `turnId`/policy
///     snapshot to `turn_context`, an optional `replacementHistory` snapshot to
///     `compacted`, a new tolerant `event` record kind, and an append-only
///     `rollback` marker for conversation rewind.
///
/// v3 records canonical runtime policy fields and stops emitting the retired
/// permission preset. Historical v1/v2 records remain readable.
pub const ROLLOUT_SCHEMA_VERSION: u32 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionRole {
    Main,
    SideTask,
}

impl RuntimeEncode for SessionRole {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(
            out,
            match self {
                Self::Main => "main",
                Self::SideTask => "side_task",
            },
        )
    }
}

impl RuntimeDecode for SessionRole {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        let variants = [
            RuntimeEnumVariant::new("main", &[]),
            RuntimeEnumVariant::new("side_task", &[]),
        ];
        runtime_codec::decode_external(value, &variants, |variant| match variant {
            RuntimeExternalVariant::Unit { variant: 0 } => Ok(Self::Main),
            RuntimeExternalVariant::Unit { variant: 1 } => Ok(Self::SideTask),
            RuntimeExternalVariant::Unit { .. } => Err(RuntimeCodecError::new(
                RuntimeCodecErrorKind::UnknownEnumVariant,
            )),
            RuntimeExternalVariant::Payload { .. } => Err(RuntimeCodecError::new(
                RuntimeCodecErrorKind::InvalidEnumShape,
            )),
        })
    }
}

/// Snapshot of the originating git checkout, when known.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitSnapshot {
    pub commit: Option<RuntimeString>,
    pub branch: Option<RuntimeString>,
    pub origin_url: Option<RuntimeString>,
}

/// Header metadata for a session. The required fields (id/createdAt/cwd/model)
/// are the v1 shape; the rest are v2 versioning/provenance fields, all optional
/// so older headers remain valid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionMeta {
    pub id: RuntimeString,
    pub created_at: i64,
    pub cwd: RuntimeString,
    pub model: RuntimeString,
    pub title: Option<RuntimeString>,
    /// Tool that originated the session (e.g. "cli", "vscode").
    pub originator: Option<RuntimeString>,
    /// Engine/CLI version that wrote the file, for forward-compat triage.
    pub cli_version: Option<RuntimeString>,
    /// Logical source bucket: interactive cli, an editor, a sub-agent, etc.
    pub source: Option<RuntimeString>,
    /// Session product role. Missing means `main` for pre-role rollouts.
    pub role: Option<SessionRole>,
    /// Parent thread id when this session was spawned by another (sub-agents).
    pub parent_thread_id: Option<RuntimeString>,
    /// Thread id this session was forked from, if any.
    pub forked_from_id: Option<RuntimeString>,
    pub git: Option<GitSnapshot>,
    /// Opaque host-owned attachment. The engine stores and returns it verbatim
    /// and never interprets its shape: hosts keep their session bookkeeping on
    /// the thread itself instead of growing a parallel store, and can evolve
    /// the payload without an engine schema change. Updates replace the whole
    /// value (no merging).
    pub host_metadata: Option<RuntimeJson>,
}

/// Per-turn execution context snapshot. cwd/model/permissionMode are the v1
/// shape; turnId and policy fields are optional v2 additions that let resume
/// recover the precise context of the latest turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnContextSnapshot {
    pub cwd: RuntimeString,
    pub model: RuntimeString,
    /// Historical input only. New turn records use canonical policy fields.
    pub permission_mode: ExternalPermissionMode,
    /// Identifier of the turn this snapshot belongs to, when tracked.
    pub turn_id: Option<RuntimeString>,
    /// Absolute turn admission time in epoch milliseconds. Optional so older
    /// rollouts still decode, and persisted at the boundary so a user message
    /// keeps its sent time even if the process dies before a terminal event.
    pub started_at_ms: Option<i64>,
    /// Canonical approval routing captured for this turn.
    pub approval_policy: Option<crate::types::permission::ApprovalPolicy>,
    pub sandbox_mode: Option<crate::types::permission::SandboxMode>,
    pub approvals_reviewer: Option<crate::types::permission::ApprovalsReviewer>,
    pub network_allowed: Option<bool>,
    /// Effective writable roots materialized for the turn.
    pub writable_roots: Option<Vec<RuntimeString>>,
}

/// A compaction record. The model-visible projection drops the messages in
/// `replaced_range` (a half-open `[start, end)` index range over the message
/// stream seen so far) and substitutes `summary` in their place.
///
/// `replacement_history`, when present, is a full snapshot of the
/// post-compaction message list; it takes precedence over the drop-and-substitute
/// path.
/// `source_message_ids` records what the snapshot summarized. Typed question
/// answers outside that source survive replacement, including answers enqueued
/// while the summary was running. Absent metadata retains legacy exact semantics.
#[derive(Debug, Clone, PartialEq)]
pub struct CompactedItem {
    pub summary: RuntimeString,
    pub replaced_range: (u64, u64),
    pub replacement_history: Option<Vec<Message>>,
    pub source_message_ids: Option<Vec<RuntimeString>>,
    /// Original review evidence captured at the same persistence boundary.
    pub approval_parent_history: Option<RuntimeJson>,
}

/// A tolerant event/response record: a
/// structured trace marker persisted for analysis/replay but NOT part of the
/// model-visible message projection. The payload is deliberately open so new
/// event shapes from a newer writer never break an older reader.
#[derive(Debug, Clone, PartialEq)]
pub struct EventItem {
    /// Event discriminator, e.g. "turn_started", "token_count", "tool_call_end".
    pub event: RuntimeString,
    /// Optional turn this event belongs to.
    pub turn_id: Option<RuntimeString>,
    /// Structured payload; opaque to the store.
    pub payload: Option<RuntimeJson>,
}

/// A conversation-rollback marker. It
/// is append-only — the rewind is recorded as a requested user-turn count and
/// applied by the projection on resume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollbackItem {
    /// Number of user turns the rollback requested (may exceed those available).
    /// Constrained to a non-negative integer (a `u32`).
    pub num_turns: u32,
}

impl RuntimeEncode for RollbackItem {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self { num_turns } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("numTurns", num_turns)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for RollbackItem {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                num_turns: object.required("numTurns", &[])?,
            })
        })
    }
}

/// A per-turn working-tree checkpoint ledger entry (the FILESYSTEM half of
/// rewind). Recorded before a turn runs, one per turn, so a resumed session can
/// restore files to a historical turn — the checkpoint git objects live in the
/// shadow repo, but the turn→hash mapping would otherwise be lost on restart.
/// `hash` is null for a turn whose snapshot could not be taken (a git error),
/// preserving the 1:1 turn alignment. A `rollback` drops the last N of these in
/// lockstep with its N user turns. A reader that doesn't know this kind
/// tolerantly skips it (file rewind of old turns degrades to unavailable —
/// exactly the pre-persistence behavior — rather than crashing).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointItem {
    /// Checkpoint hash, serialized as an explicit `null` when the turn's
    /// snapshot could not be taken. The field is REQUIRED on the wire (nullable,
    /// not optional): a record missing it entirely does not parse.
    pub hash: Option<RuntimeString>,
}

/// Immutable billing record for one completed provider request. The USD amount
/// is computed when the request completes so later pricing changes cannot alter
/// historical session cost.
#[derive(Debug, Clone, PartialEq)]
pub struct UsageLedgerItem {
    pub request_id: RuntimeString,
    pub assistant_message_id: RuntimeString,
    pub model: RuntimeString,
    pub usage: Usage,
    pub cost_usd: f64,
    pub inference: Option<crate::types::events::InferenceIdentity>,
}

/// Rollout envelope for the authoritative core lifecycle record. The nested
/// shape avoids colliding with `RolloutItem`'s top-level `kind` discriminator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubagentLifecycleItem {
    pub record: SubagentLifecycleRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackgroundProcessLifecycleItem {
    pub record: BackgroundProcessRecord,
}

/// Append-only, replayable rollout record. The header carries schemaVersion.
#[derive(Debug, Clone, PartialEq)]
pub enum RolloutItem {
    SessionMeta {
        schema_version: u32,
        meta: SessionMeta,
    },
    TurnContext {
        ctx: TurnContextSnapshot,
    },
    Message {
        message: Message,
    },
    Compacted(CompactedItem),
    Event(EventItem),
    Rollback(RollbackItem),
    Checkpoint(CheckpointItem),
    UsageLedger(UsageLedgerItem),
    SubagentLifecycle(SubagentLifecycleItem),
    BackgroundProcessLifecycle(BackgroundProcessLifecycleItem),
}

macro_rules! impl_runtime_object_codec {
    ($type:ty) => {
        impl RuntimeEncode for $type {
            fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
                out.emit(RuntimeJson::Object(self.encode_fields()?))
            }
        }

        impl RuntimeDecode for $type {
            fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
                runtime_codec::decode_object(value, false, Self::decode_fields)
            }
        }
    };
}

impl GitSnapshot {
    fn encode_fields(&self) -> Result<RuntimeObject, RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        if let Some(value) = &self.commit {
            object.field("commit", value)?;
        }
        if let Some(value) = &self.branch {
            object.field("branch", value)?;
        }
        if let Some(value) = &self.origin_url {
            object.field("originUrl", value)?;
        }
        Ok(object.finish())
    }

    fn decode_fields(object: &mut RuntimeObjectDecoder<'_>) -> Result<Self, RuntimeCodecError> {
        Ok(Self {
            commit: object.optional("commit", &[])?,
            branch: object.optional("branch", &[])?,
            origin_url: object.optional("originUrl", &[])?,
        })
    }
}

impl_runtime_object_codec!(GitSnapshot);

impl SessionMeta {
    fn encode_fields(&self) -> Result<RuntimeObject, RuntimeCodecError> {
        let Self {
            id,
            created_at,
            cwd,
            model,
            title,
            originator,
            cli_version,
            source,
            role,
            parent_thread_id,
            forked_from_id,
            git,
            host_metadata,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("id", id)?;
        object.field("createdAt", created_at)?;
        object.field("cwd", cwd)?;
        object.field("model", model)?;
        if let Some(value) = title {
            object.field("title", value)?;
        }
        if let Some(value) = originator {
            object.field("originator", value)?;
        }
        if let Some(value) = cli_version {
            object.field("cliVersion", value)?;
        }
        if let Some(value) = source {
            object.field("source", value)?;
        }
        if let Some(value) = role {
            object.field("role", value)?;
        }
        if let Some(value) = parent_thread_id {
            object.field("parentThreadId", value)?;
        }
        if let Some(value) = forked_from_id {
            object.field("forkedFromId", value)?;
        }
        if let Some(value) = git {
            object.field("git", value)?;
        }
        if let Some(value) = host_metadata {
            object.field("hostMetadata", value)?;
        }
        Ok(object.finish())
    }

    fn decode_fields(object: &mut RuntimeObjectDecoder<'_>) -> Result<Self, RuntimeCodecError> {
        Ok(Self {
            id: object.required("id", &[])?,
            created_at: object.required("createdAt", &[])?,
            cwd: object.required("cwd", &[])?,
            model: object.required("model", &[])?,
            title: object.optional("title", &[])?,
            originator: object.optional("originator", &[])?,
            cli_version: object.optional("cliVersion", &[])?,
            source: object.optional("source", &[])?,
            role: object.optional("role", &[])?,
            parent_thread_id: object.optional("parentThreadId", &[])?,
            forked_from_id: object.optional("forkedFromId", &[])?,
            git: object.optional("git", &[])?,
            host_metadata: object.optional("hostMetadata", &[])?,
        })
    }
}

impl_runtime_object_codec!(SessionMeta);

impl TurnContextSnapshot {
    fn encode_fields(&self) -> Result<RuntimeObject, RuntimeCodecError> {
        let Self {
            cwd,
            model,
            permission_mode: _,
            turn_id,
            started_at_ms,
            approval_policy,
            sandbox_mode,
            approvals_reviewer,
            network_allowed,
            writable_roots,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("cwd", cwd)?;
        object.field("model", model)?;
        if let Some(value) = turn_id {
            object.field("turnId", value)?;
        }
        if let Some(value) = started_at_ms {
            object.field("startedAtMs", value)?;
        }
        if let Some(value) = approval_policy {
            object.field("approvalPolicy", value)?;
        }
        if let Some(value) = sandbox_mode {
            object.field("sandboxMode", value)?;
        }
        if let Some(value) = approvals_reviewer {
            object.field("approvalsReviewer", value)?;
        }
        if let Some(value) = network_allowed {
            object.field("networkAllowed", value)?;
        }
        if let Some(value) = writable_roots {
            object.field("writableRoots", value)?;
        }
        Ok(object.finish())
    }

    fn decode_fields(object: &mut RuntimeObjectDecoder<'_>) -> Result<Self, RuntimeCodecError> {
        Ok(Self {
            cwd: object.required("cwd", &[])?,
            model: object.required("model", &[])?,
            permission_mode: object
                .optional("permissionMode", &[])?
                .unwrap_or(ExternalPermissionMode::Default),
            turn_id: object.optional("turnId", &[])?,
            started_at_ms: object.optional("startedAtMs", &[])?,
            approval_policy: object.optional("approvalPolicy", &[])?,
            sandbox_mode: object.optional("sandboxMode", &[])?,
            approvals_reviewer: object.optional("approvalsReviewer", &[])?,
            network_allowed: object.optional("networkAllowed", &[])?,
            writable_roots: object.optional("writableRoots", &[])?,
        })
    }
}

impl_runtime_object_codec!(TurnContextSnapshot);

impl CompactedItem {
    /// True when this record is an in-place history repair (usage invalidation,
    /// instruction projection cleanup, or media repair) rather than a context
    /// compaction that introduced a summary boundary.
    pub fn is_history_repair(&self) -> bool {
        let Some(replacement) = &self.replacement_history else {
            return false;
        };
        if !self.summary.is_empty() {
            return false;
        }
        let prior_ids: std::collections::HashSet<_> = self
            .source_message_ids
            .as_deref()
            .unwrap_or_default()
            .iter()
            .collect();
        !replacement.iter().any(|message| {
            matches!(
                message,
                Message::CompactBoundary(boundary) if !prior_ids.contains(&boundary.uuid)
            )
        })
    }

    fn encode_fields(&self) -> Result<RuntimeObject, RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("summary", &self.summary)?;
        object.field(
            "replacedRange",
            &vec![self.replaced_range.0, self.replaced_range.1],
        )?;
        if let Some(value) = &self.replacement_history {
            object.field("replacementHistory", value)?;
        }
        if let Some(value) = &self.source_message_ids {
            object.field("sourceMessageIds", value)?;
        }
        if let Some(value) = &self.approval_parent_history {
            object.field("approvalParentHistory", value)?;
        }
        Ok(object.finish())
    }

    fn decode_fields(object: &mut RuntimeObjectDecoder<'_>) -> Result<Self, RuntimeCodecError> {
        let summary = object.required("summary", &[])?;
        let replaced_range: Vec<u64> = object.required("replacedRange", &[])?;
        let [start, end] = replaced_range.as_slice() else {
            return Err(RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(
                RuntimeString::from("replacedRange must contain exactly two integers"),
            ))
            .prepend("replacedRange"));
        };
        Ok(Self {
            summary,
            replaced_range: (*start, *end),
            replacement_history: object.optional("replacementHistory", &[])?,
            source_message_ids: object.optional("sourceMessageIds", &[])?,
            approval_parent_history: object.optional("approvalParentHistory", &[])?,
        })
    }
}

impl_runtime_object_codec!(CompactedItem);

impl EventItem {
    fn encode_fields(&self) -> Result<RuntimeObject, RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("event", &self.event)?;
        if let Some(value) = &self.turn_id {
            object.field("turnId", value)?;
        }
        if let Some(value) = &self.payload {
            object.field("payload", value)?;
        }
        Ok(object.finish())
    }

    fn decode_fields(object: &mut RuntimeObjectDecoder<'_>) -> Result<Self, RuntimeCodecError> {
        Ok(Self {
            event: object.required("event", &[])?,
            turn_id: object.optional("turnId", &[])?,
            payload: object.optional("payload", &[])?,
        })
    }
}

impl_runtime_object_codec!(EventItem);

impl CheckpointItem {
    fn encode_fields(&self) -> Result<RuntimeObject, RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("hash", &self.hash)?;
        Ok(object.finish())
    }

    fn decode_fields(object: &mut RuntimeObjectDecoder<'_>) -> Result<Self, RuntimeCodecError> {
        Ok(Self {
            hash: object.required("hash", &[])?,
        })
    }
}

impl_runtime_object_codec!(CheckpointItem);

impl UsageLedgerItem {
    fn encode_fields(&self) -> Result<RuntimeObject, RuntimeCodecError> {
        let Self {
            request_id,
            assistant_message_id,
            model,
            usage,
            cost_usd,
            inference,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("requestId", request_id)?;
        object.field("assistantMessageId", assistant_message_id)?;
        object.field("model", model)?;
        object.field("usage", usage)?;
        object.field("costUsd", cost_usd)?;
        if let Some(inference) = inference {
            object.field("inference", inference)?;
        }
        Ok(object.finish())
    }

    fn decode_fields(object: &mut RuntimeObjectDecoder<'_>) -> Result<Self, RuntimeCodecError> {
        Ok(Self {
            request_id: object.required("requestId", &[])?,
            assistant_message_id: object.required("assistantMessageId", &[])?,
            model: object.required("model", &[])?,
            usage: object.required("usage", &[])?,
            cost_usd: object.required("costUsd", &[])?,
            inference: object.optional("inference", &[])?,
        })
    }
}

impl_runtime_object_codec!(UsageLedgerItem);

impl SubagentLifecycleItem {
    fn encode_fields(&self) -> Result<RuntimeObject, RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("record", &self.record)?;
        Ok(object.finish())
    }

    fn decode_fields(object: &mut RuntimeObjectDecoder<'_>) -> Result<Self, RuntimeCodecError> {
        Ok(Self {
            record: object.required("record", &[])?,
        })
    }
}

impl_runtime_object_codec!(SubagentLifecycleItem);

impl BackgroundProcessLifecycleItem {
    fn encode_fields(&self) -> Result<RuntimeObject, RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("record", &self.record)?;
        Ok(object.finish())
    }

    fn decode_fields(object: &mut RuntimeObjectDecoder<'_>) -> Result<Self, RuntimeCodecError> {
        Ok(Self {
            record: object.required("record", &[])?,
        })
    }
}

impl_runtime_object_codec!(BackgroundProcessLifecycleItem);

impl RuntimeEncode for RolloutItem {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let (tag, fields) = match self {
            Self::SessionMeta {
                schema_version,
                meta,
            } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("schemaVersion", schema_version)?;
                fields.field("meta", meta)?;
                ("session_meta", fields.finish())
            }
            Self::TurnContext { ctx } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("ctx", ctx)?;
                ("turn_context", fields.finish())
            }
            Self::Message { message } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("message", message)?;
                ("message", fields.finish())
            }
            Self::Compacted(value) => ("compacted", value.encode_fields()?),
            Self::Event(value) => ("event", value.encode_fields()?),
            Self::Rollback(value) => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("numTurns", &value.num_turns)?;
                ("rollback", fields.finish())
            }
            Self::Checkpoint(value) => ("checkpoint", value.encode_fields()?),
            Self::UsageLedger(value) => ("usage_ledger", value.encode_fields()?),
            Self::SubagentLifecycle(value) => ("subagent_lifecycle", value.encode_fields()?),
            Self::BackgroundProcessLifecycle(value) => {
                ("background_process_lifecycle", value.encode_fields()?)
            }
        };
        runtime_codec::encode_internal(out, "kind", &[], tag, &fields)
    }
}

impl RuntimeDecode for RolloutItem {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            false,
            "kind",
            &[],
            &[
                runtime_codec::RuntimeEnumVariant::new("session_meta", &[]),
                runtime_codec::RuntimeEnumVariant::new("turn_context", &[]),
                runtime_codec::RuntimeEnumVariant::new("message", &[]),
                runtime_codec::RuntimeEnumVariant::new("compacted", &[]),
                runtime_codec::RuntimeEnumVariant::new("event", &[]),
                runtime_codec::RuntimeEnumVariant::new("rollback", &[]),
                runtime_codec::RuntimeEnumVariant::new("checkpoint", &[]),
                runtime_codec::RuntimeEnumVariant::new("usage_ledger", &[]),
                runtime_codec::RuntimeEnumVariant::new("subagent_lifecycle", &[]),
                runtime_codec::RuntimeEnumVariant::new("background_process_lifecycle", &[]),
            ],
            |variant, object| match variant {
                0 => Ok(Self::SessionMeta {
                    schema_version: object.required("schemaVersion", &[])?,
                    meta: object.required("meta", &[])?,
                }),
                1 => Ok(Self::TurnContext {
                    ctx: object.required("ctx", &[])?,
                }),
                2 => Ok(Self::Message {
                    message: object.required("message", &[])?,
                }),
                3 => CompactedItem::decode_fields(object).map(Self::Compacted),
                4 => EventItem::decode_fields(object).map(Self::Event),
                5 => Ok(Self::Rollback(RollbackItem {
                    num_turns: object.required("numTurns", &[])?,
                })),
                6 => CheckpointItem::decode_fields(object).map(Self::Checkpoint),
                7 => UsageLedgerItem::decode_fields(object).map(Self::UsageLedger),
                8 => SubagentLifecycleItem::decode_fields(object).map(Self::SubagentLifecycle),
                9 => BackgroundProcessLifecycleItem::decode_fields(object)
                    .map(Self::BackgroundProcessLifecycle),
                _ => Err(RuntimeCodecError::new(
                    RuntimeCodecErrorKind::UnknownEnumVariant,
                )),
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(source: &str) -> RuntimeJson {
        crate::runtime_values::runtime_json::parse(&RuntimeString::from(source)).unwrap()
    }

    fn assert_round_trip(source: &str) -> RolloutItem {
        let wire = parse(source);
        let item = runtime_codec::decode_value::<RolloutItem>(&wire).unwrap();
        assert_eq!(runtime_codec::encode_value(&item).unwrap(), wire);
        item
    }

    #[test]
    fn session_meta_item_wire_shape() {
        let item = RolloutItem::SessionMeta {
            schema_version: ROLLOUT_SCHEMA_VERSION,
            meta: SessionMeta {
                id: "s1".into(),
                created_at: 1000,
                cwd: "/repo".into(),
                model: "m".into(),
                title: None,
                originator: None,
                cli_version: None,
                source: None,
                role: None,
                parent_thread_id: None,
                forked_from_id: None,
                git: None,
                host_metadata: None,
            },
        };
        let v = runtime_codec::encode_value(&item).unwrap();
        assert_eq!(
            v,
            parse(
                r#"{"kind":"session_meta","schemaVersion":3,"meta":{"id":"s1","createdAt":1000,"cwd":"/repo","model":"m"}}"#
            )
        );
        let round = runtime_codec::decode_value::<RolloutItem>(&v).unwrap();
        assert_eq!(round, item);
    }

    #[test]
    fn compacted_and_rollback_wire_shape() {
        let c = RolloutItem::Compacted(CompactedItem {
            approval_parent_history: None,
            summary: "sum".into(),
            replaced_range: (0, 4),
            source_message_ids: None,
            replacement_history: None,
        });
        assert_eq!(
            runtime_codec::encode_value(&c).unwrap(),
            parse(r#"{"kind":"compacted","summary":"sum","replacedRange":[0,4]}"#)
        );

        let r = RolloutItem::Rollback(RollbackItem { num_turns: 2 });
        assert_eq!(
            runtime_codec::encode_value(&r).unwrap(),
            parse(r#"{"kind":"rollback","numTurns":2}"#)
        );
    }

    #[test]
    fn checkpoint_wire_shape_hash_is_required_but_nullable() {
        // A null hash serializes as an explicit `null` (the field is never omitted).
        let c = RolloutItem::Checkpoint(CheckpointItem { hash: None });
        assert_eq!(
            runtime_codec::encode_value(&c).unwrap(),
            parse(r#"{"kind":"checkpoint","hash":null}"#)
        );
        let with_hash = runtime_codec::decode_value::<RolloutItem>(&parse(
            r#"{"kind":"checkpoint","hash":"abc"}"#,
        ))
        .unwrap();
        assert_eq!(
            with_hash,
            RolloutItem::Checkpoint(CheckpointItem {
                hash: Some("abc".into())
            })
        );
        // Missing the field entirely does not parse (nullable ≠ optional).
        let error = runtime_codec::decode_value::<RolloutItem>(&parse(r#"{"kind":"checkpoint"}"#))
            .unwrap_err();
        assert_eq!(error.kind(), &RuntimeCodecErrorKind::MissingField);
        assert_eq!(error.pointer(), RuntimeString::from("/hash"));
    }

    #[test]
    fn usage_ledger_wire_shape_preserves_computed_cost() {
        let item = RolloutItem::UsageLedger(UsageLedgerItem {
            inference: None,
            request_id: "req-1".into(),
            assistant_message_id: "assistant-1".into(),
            model: "model-a".into(),
            usage: Usage {
                input_tokens: 10,
                output_tokens: 3,
                cache_read_input_tokens: 4,
                cache_creation_input_tokens: 5,
                total_tokens: Some(22),
            },
            cost_usd: 0.125,
        });
        let value = runtime_codec::encode_value(&item).unwrap();
        assert_eq!(
            value,
            parse(
                r#"{"kind":"usage_ledger","requestId":"req-1","assistantMessageId":"assistant-1","model":"model-a","usage":{"inputTokens":10,"outputTokens":3,"cacheReadInputTokens":4,"cacheCreationInputTokens":5,"totalTokens":22},"costUsd":0.125}"#
            )
        );
        assert_eq!(
            runtime_codec::decode_value::<RolloutItem>(&value).unwrap(),
            item
        );
    }

    #[test]
    fn subagent_lifecycle_wire_shape_uses_the_core_record() {
        use crate::schemas::subagent_lifecycle::{SubagentContext, SubagentKind, SubagentStatus};

        let item = RolloutItem::SubagentLifecycle(SubagentLifecycleItem {
            record: SubagentLifecycleRecord {
                agent_id: "child-1".into(),
                kind: SubagentKind::Workflow,
                status: SubagentStatus::Failed,
                context: SubagentContext {
                    session_id: "session-1".into(),
                    parent_agent_id: Some("parent-1".into()),
                    parent_thread_id: Some("thread-1".into()),
                    agent_type: Some("research".into()),
                    cwd: "/workspace".into(),
                    description: "collect evidence".into(),
                    depth: 2,
                    resume_from: None,
                    spawn_turn_id: None,
                },
                started_at_ms: 10,
                updated_at_ms: 20,
                completed_at_ms: Some(20),
                last_result: None,
                last_error: Some("failed".into()),
                cancellation_requested: false,
                model: None,
                model_source: None,
            },
        });
        let value = runtime_codec::encode_value(&item).unwrap();
        assert_eq!(
            value,
            parse(
                r#"{"kind":"subagent_lifecycle","record":{"agentId":"child-1","kind":"workflow","status":"failed","context":{"sessionId":"session-1","parentAgentId":"parent-1","parentThreadId":"thread-1","agentType":"research","cwd":"/workspace","description":"collect evidence","depth":2,"resumeFrom":null,"spawnTurnId":null},"startedAtMs":10,"updatedAtMs":20,"completedAtMs":20,"lastResult":null,"lastError":"failed","cancellationRequested":false,"model":null,"modelSource":null}}"#
            )
        );
        assert_eq!(
            runtime_codec::decode_value::<RolloutItem>(&value).unwrap(),
            item
        );
    }

    #[test]
    fn background_process_lifecycle_wire_shape_uses_exact_status_and_offsets() {
        let item = RolloutItem::BackgroundProcessLifecycle(BackgroundProcessLifecycleItem {
            record: BackgroundProcessRecord {
                shell_id: "shell-1".into(),
                command: "printf hello".into(),
                cwd: "/workspace".into(),
                status: crate::schemas::background_process::BackgroundProcessStatus::Failed,
                started_at_ms: 10,
                updated_at_ms: 20,
                completed_at_ms: Some(20),
                exit_code: Some(7),
                termination_signal: None,
                failure: Some("background process exited unsuccessfully".into()),
                stop_requested: false,
                output_start_offset: 4,
                output_next_offset: 9,
                dropped_bytes: 4,
                output_available: true,
            },
        });
        let value = runtime_codec::encode_value(&item).unwrap();
        assert_eq!(
            value,
            parse(
                r#"{"kind":"background_process_lifecycle","record":{"shellId":"shell-1","command":"printf hello","cwd":"/workspace","status":"failed","startedAtMs":10,"updatedAtMs":20,"completedAtMs":20,"exitCode":7,"terminationSignal":null,"failure":"background process exited unsuccessfully","stopRequested":false,"outputStartOffset":4,"outputNextOffset":9,"droppedBytes":4,"outputAvailable":true}}"#
            )
        );
        assert_eq!(
            runtime_codec::decode_value::<RolloutItem>(&value).unwrap(),
            item
        );
    }

    #[test]
    fn historical_presets_are_read_but_new_turn_records_use_canonical_policy() {
        let legacy = parse(
            r#"{"kind":"turn_context","ctx":{"cwd":"/repo","model":"m","permissionMode":"bypassPermissions"}}"#,
        );
        let decoded = runtime_codec::decode_value::<RolloutItem>(&legacy).unwrap();
        let encoded = runtime_codec::encode_value(&decoded).unwrap();
        assert!(encoded.get("ctx").unwrap().get("permissionMode").is_none());
        assert_round_trip(
            r#"{"kind":"turn_context","ctx":{"cwd":"/repo","model":"m","approvalPolicy":"never","sandboxMode":"danger-full-access","approvalsReviewer":"user","networkAllowed":true}}"#,
        );
    }

    #[test]
    fn v1_shapes_and_unknown_v2_fields_remain_tolerant() {
        for source in [
            r#"{"kind":"message","message":{"type":"user","uuid":"u1","content":[]}}"#,
            r#"{"kind":"event","event":"old_event"}"#,
            r#"{"kind":"session_meta","schemaVersion":2,"meta":{"id":"s2","createdAt":2,"cwd":"/repo","model":"m","title":"title","originator":"cli","cliVersion":"1.0","source":"interactive","parentThreadId":"parent","forkedFromId":"fork","git":{"commit":"abc","branch":"main","originUrl":"ssh://repo"}}}"#,
        ] {
            assert_round_trip(source);
        }

        let with_future_field = parse(
            r#"{"kind":"session_meta","schemaVersion":1,"meta":{"id":"s1","createdAt":1,"cwd":"/repo","model":"m","future":true},"futureTop":true}"#,
        );
        let decoded = runtime_codec::decode_value::<RolloutItem>(&with_future_field).unwrap();
        assert_eq!(
            runtime_codec::encode_value(&decoded).unwrap(),
            parse(
                r#"{"kind":"session_meta","schemaVersion":1,"meta":{"id":"s1","createdAt":1,"cwd":"/repo","model":"m"}}"#
            )
        );

        let explicit_null = parse(
            r#"{"kind":"event","event":"old_event","turnId":null,"payload":null,"future":true}"#,
        );
        let explicit_null = runtime_codec::decode_value::<RolloutItem>(&explicit_null).unwrap();
        assert_eq!(
            runtime_codec::encode_value(&explicit_null).unwrap(),
            parse(r#"{"kind":"event","event":"old_event"}"#)
        );
    }

    #[test]
    fn exact_codec_preserves_non_scalar_utf16_in_rollout_carriers() {
        let lone_high = RuntimeString::from_utf16(vec![0xd800]);
        let lone_low = RuntimeString::from_utf16(vec![0xdc00]);
        let mut payload = RuntimeObject::new();
        payload.insert(lone_high.clone(), RuntimeJson::String(lone_low.clone()));
        let item = RolloutItem::Event(EventItem {
            event: lone_high,
            turn_id: Some(lone_low),
            payload: Some(RuntimeJson::Object(payload)),
        });
        let encoded = runtime_codec::encode_value(&item).unwrap();
        assert_eq!(
            runtime_codec::decode_value::<RolloutItem>(&encoded).unwrap(),
            item
        );

        let summary = RuntimeString::from_utf16(vec![0xd800, 0x0061, 0xdc00]);
        let compacted = RolloutItem::Compacted(CompactedItem {
            approval_parent_history: None,
            summary: summary.clone(),
            replaced_range: (0, 1),
            source_message_ids: None,
            replacement_history: None,
        });
        let encoded = runtime_codec::encode_value(&compacted).unwrap();
        assert_eq!(
            runtime_codec::decode_value::<RolloutItem>(&encoded).unwrap(),
            compacted
        );
        let RuntimeJson::Object(encoded) = &encoded else {
            panic!("compacted rollout item must encode as an object");
        };
        assert_eq!(
            encoded
                .get("summary")
                .and_then(RuntimeJson::as_string)
                .expect("encoded compact summary")
                .as_utf16(),
            summary.as_utf16()
        );
    }
}
