//! Persistence interfaces for agent sessions: the writable rollout handle, the
//! resume/list result shapes, and the pluggable [`SessionStore`] backend.

use crate::runtime_process::RuntimeProcessContext;
use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimePath, RuntimeString,
};
use crate::schemas::background_process::BackgroundProcessRecord;
use crate::schemas::message::Message;
use crate::schemas::rollout::{
    RolloutItem, SessionMeta, SessionRole, TurnContextSnapshot, UsageLedgerItem,
};
use crate::schemas::subagent_lifecycle::SubagentLifecycleRecord;
use crate::store::search::{SessionSearchOptions, SessionSearchPage};
use crate::types::abort::AbortSignal;
use crate::types::json::Json;
use async_trait::async_trait;

/// A compaction record to append. The model-visible projection drops the
/// `replaced_range` messages and substitutes `summary` (or replaces history
/// wholesale when `replacement_history` is supplied). A source watermark keeps
/// typed question answers admitted outside the summarized source. See `project_for_prompt`.
#[derive(Debug, Clone, PartialEq)]
pub struct CompactionRecord {
    pub summary: RuntimeString,
    pub replaced_range: (u64, u64),
    pub replacement_history: Option<Vec<Message>>,
    pub source_message_ids: Option<Vec<RuntimeString>>,
    /// Review state published atomically with the replacement model history.
    pub approval_parent_history: Option<RuntimeJson>,
}

/// Optional metadata for an appended trace/event record.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AppendEventOptions {
    pub turn_id: Option<RuntimeString>,
    pub payload: Option<Json>,
}

/// A conversation-rollback marker to append (a `ThreadRolledBack` marker).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RollbackRecord {
    pub num_turns: u32,
}

/// A per-turn working-tree checkpoint to append (the rewind file-ledger, one
/// per turn). `hash` is `None` when the turn's snapshot couldn't be taken,
/// preserving turn alignment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointRecord {
    pub hash: Option<String>,
}

/// Capacity acquired before a caller changes state protected by synchronous
/// locks. Committing consumes that capacity without yielding; dropping an unused
/// admission makes no mutation and releases the ordered admission boundary.
pub struct PreparedRolloutAppend<'a> {
    commit: Box<dyn FnOnce(RolloutItem) + Send + 'a>,
}

/// A fixed-capacity reservation for one synchronous state transition that emits
/// several records. All records commit in order without allowing a flush or
/// another mutation to enter between them.
pub struct PreparedRolloutBatch<'a> {
    commit: Box<dyn FnOnce(Vec<RolloutItem>) + Send + 'a>,
}

impl<'a> PreparedRolloutBatch<'a> {
    /// Construct a synchronous commit after the backend has acquired capacity.
    /// An immediate backend may borrow its protected state for this lifetime.
    pub fn new(commit: impl FnOnce(Vec<RolloutItem>) + Send + 'a) -> Self {
        Self {
            commit: Box::new(commit),
        }
    }

    pub fn commit(self, items: Vec<RolloutItem>) {
        (self.commit)(items);
    }
}

impl<'a> PreparedRolloutAppend<'a> {
    pub fn new(commit: impl FnOnce(RolloutItem) + Send + 'a) -> Self {
        Self {
            commit: Box::new(commit),
        }
    }

    pub fn commit(self, item: RolloutItem) {
        (self.commit)(item);
    }
}

/// A writable, append-only handle to a single session's rollout. Structurally
/// satisfies the engine's rollout sink (append_message + flush) while adding
/// the turn-context and lifecycle methods the store needs.
#[async_trait]
pub trait LiveRollout: Send + Sync + std::fmt::Debug {
    /// Acquire capacity before changing synchronous state. The returned commit
    /// cannot suspend; a closed backend fails before the mutation is published.
    async fn prepare_append(&self) -> std::io::Result<PreparedRolloutAppend<'_>> {
        let batch = self.prepare_batch(1).await?;
        Ok(PreparedRolloutAppend::new(move |item| {
            batch.commit(vec![item])
        }))
    }
    /// Reserve the complete upper bound of records emitted by one mutation.
    /// Immediate backends must still supply a real synchronous commit; there is
    /// no fallback that may suspend after the caller has changed its state.
    async fn prepare_batch(
        &self,
        maximum_records: usize,
    ) -> std::io::Result<PreparedRolloutBatch<'_>>;
    async fn append_message(&self, message: Message);
    async fn append_turn_context(&self, ctx: TurnContextSnapshot);
    /// Append a compaction record. The model-visible projection drops the
    /// `replaced_range` messages and substitutes `summary` (or replaces history
    /// wholesale when `replacement_history` is supplied). Source-aware snapshots
    /// preserve typed answers not included in their source watermark.
    async fn append_compaction(&self, record: CompactionRecord);
    /// Append a trace/event record. Persisted for analysis, not model-visible.
    async fn append_event(&self, event: RuntimeString, opts: Option<AppendEventOptions>);
    /// Append a conversation-rollback marker (a `ThreadRolledBack` record). On
    /// resume the projection drops the last `num_turns` user turns at this
    /// point, so the reconstructed history matches the live rollback.
    /// Append-only — never truncates.
    async fn append_rollback(&self, record: RollbackRecord);
    /// Append a per-turn working-tree checkpoint (the rewind file-ledger, one
    /// per turn). On resume `project_checkpoints` replays these into the ledger
    /// so file rewind can target a historical turn. `hash` is `None` when the
    /// turn's snapshot couldn't be taken, preserving turn alignment.
    async fn append_checkpoint(&self, record: CheckpointRecord);
    /// Append one immutable provider billing record. This record is never part
    /// of the model-visible message projection.
    async fn append_usage(&self, record: UsageLedgerItem);
    /// Append the authoritative state after one sub-agent lifecycle transition.
    /// Lifecycle records are never part of the model-visible projection.
    async fn append_subagent_lifecycle(&self, record: SubagentLifecycleRecord);
    /// Append the authoritative state after one background-process transition.
    async fn append_background_process_lifecycle(&self, _record: BackgroundProcessRecord) {}
    /// Persist the title in the thread metadata authority at this position in
    /// the live writer chain. Unsupported backends use the store lifecycle path.
    async fn update_title(&self, _title: &str) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "live rollout does not support title updates",
        ))
    }
    /// Replace the opaque host-owned attachment through the live writer when
    /// this handle owns the main-session writer lock.
    async fn update_host_metadata(
        &self,
        _host_metadata: Option<RuntimeJson>,
    ) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "live rollout does not support host metadata updates",
        ))
    }
    /// Monotonic count of accepted rollout mutations. Inspectors compare this
    /// before flush/read and after live capture to reject a moving transcript.
    /// Backends without this fence cannot provide complete original follow.
    fn inspection_revision(&self) -> Option<u64> {
        None
    }
    async fn flush(&self) -> std::io::Result<()>;
    async fn shutdown(&self) -> std::io::Result<()>;
}

/// Result of resuming a persisted session.
///
/// `messages` is the COMPACTED, model-visible projection (compaction records
/// are applied, not replayed raw). `raw_items` is the full untrimmed item
/// stream for callers that need the complete trace. `last_turn_context` is the
/// most recent turn-context snapshot, used to restore execution context on
/// resume.
#[derive(Debug, Clone, PartialEq)]
pub struct ResumeResult {
    pub meta: SessionMeta,
    pub messages: Vec<Message>,
    pub raw_items: Vec<RolloutItem>,
    pub last_turn_context: Option<TurnContextSnapshot>,
    /// Rebuilt turn grouping served with `get_history`.
    pub turn_index: crate::engine::session::TurnIndex,
    /// Rebuilt set of files this thread changed, served with `get_file_changes`.
    pub file_changes: crate::engine::session::SessionFileChanges,
}

/// Runtime continuation data. `recovery_items` is an explicitly sparse state
/// projection, never an original-history or fork source. Metadata and billing
/// are reconstructed over the complete rollout independently of the model window.
#[derive(Debug, Clone, PartialEq)]
pub struct RuntimeResumeResult {
    pub meta: SessionMeta,
    pub messages: Vec<Message>,
    pub recovery_items: Vec<RolloutItem>,
    pub last_turn_context: Option<TurnContextSnapshot>,
    pub turn_index: crate::engine::session::TurnIndex,
    pub file_changes: crate::engine::session::SessionFileChanges,
    pub persisted_user_uuids: std::collections::HashSet<RuntimeString>,
    pub hidden_context_message_ids: Vec<RuntimeString>,
    pub usage: Vec<crate::store::runtime_resume::RuntimeUsage>,
}

impl From<ResumeResult> for RuntimeResumeResult {
    fn from(resume: ResumeResult) -> Self {
        let (persisted_user_uuids, usage) =
            crate::store::runtime_resume::history_metadata(&resume.meta, &resume.raw_items);
        let hidden_context_message_ids =
            crate::store::thread_history::side_task_hidden_message_ids(&resume.raw_items);
        Self {
            meta: resume.meta,
            messages: resume.messages,
            recovery_items: resume.raw_items,
            last_turn_context: resume.last_turn_context,
            turn_index: resume.turn_index,
            file_changes: resume.file_changes,
            persisted_user_uuids,
            hidden_context_message_ids,
            usage,
        }
    }
}

#[derive(Debug)]
pub struct ResumedRuntimeRollout {
    pub resume: RuntimeResumeResult,
    pub rollout: Box<dyn LiveRollout>,
}

/// A read-only point-in-time rollout snapshot. Unlike resume, this preserves the
/// full item stream, applies no crash-recovery state, and owns no writer lease.
#[derive(Debug, Clone, PartialEq)]
pub struct ThreadReadSnapshot {
    pub meta: SessionMeta,
    pub raw_items: Vec<RolloutItem>,
    pub updated_at: i64,
    pub archived: bool,
}

/// A sub-agent snapshot that neither resumes its parent nor opens a lifecycle writer.
#[derive(Debug, Clone, PartialEq)]
pub struct SubAgentReadSnapshot {
    pub raw_items: Vec<RolloutItem>,
    pub meta: Option<SubAgentMeta>,
    pub surfaced_tools: Vec<RuntimeString>,
}

/// Atomic read-for-continuation result from [`SessionStore::resume_for_write`].
///
/// `resume` and `rollout` are one storage transaction: the backend must acquire
/// the long-lived writer lease before reading the authoritative history and
/// transfer that exact lease to `rollout`. This prevents a second writer from
/// appending between history reconstruction and writer attachment.
#[derive(Debug)]
pub struct ResumedLiveRollout {
    pub resume: ResumeResult,
    pub rollout: Box<dyn LiveRollout>,
}

/// Lightweight descriptor of a persisted session with authoritative metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct ThreadSummary {
    pub id: RuntimeString,
    pub created_at: i64,
    pub updated_at: i64,
    pub cwd: RuntimeString,
    pub model: RuntimeString,
    pub title: Option<RuntimeString>,
    pub role: SessionRole,
    pub parent_thread_id: Option<RuntimeString>,
    /// Thread id this session was forked from, if any.
    pub forked_from_id: Option<RuntimeString>,
    /// Whether the session lives in the archived subdir.
    pub archived: Option<bool>,
    /// Opaque host-owned attachment, returned verbatim from metadata storage.
    pub host_metadata: Option<RuntimeJson>,
}

impl RuntimeEncode for ThreadSummary {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("id", &self.id)?;
        object.field("createdAt", &self.created_at)?;
        object.field("updatedAt", &self.updated_at)?;
        object.field("cwd", &self.cwd)?;
        object.field("model", &self.model)?;
        if let Some(value) = &self.title {
            object.field("title", value)?;
        }
        object.field("role", &self.role)?;
        if let Some(value) = &self.parent_thread_id {
            object.field("parentThreadId", value)?;
        }
        if let Some(value) = &self.forked_from_id {
            object.field("forkedFromId", value)?;
        }
        if let Some(value) = &self.archived {
            object.field("archived", value)?;
        }
        if let Some(value) = &self.host_metadata {
            object.field("hostMetadata", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ThreadSummary {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            let created_at = object.required("createdAt", &[])?;
            Ok(Self {
                id: object.required("id", &[])?,
                created_at,
                updated_at: object.optional("updatedAt", &[])?.unwrap_or(created_at),
                cwd: object.required("cwd", &[])?,
                model: object.required("model", &[])?,
                title: object.optional("title", &[])?,
                role: object.optional("role", &[])?.unwrap_or(SessionRole::Main),
                parent_thread_id: object.optional("parentThreadId", &[])?,
                forked_from_id: object.optional("forkedFromId", &[])?,
                archived: object.optional("archived", &[])?,
                host_metadata: object.optional("hostMetadata", &[])?,
            })
        })
    }
}

/// Options for a paginated `list()` call.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ListOptions {
    /// Opaque cursor returned by a prior page; resume listing after it.
    pub cursor: Option<RuntimeString>,
    /// Max items to return in this page.
    pub limit: Option<usize>,
    /// Include archived sessions in the results (default: false).
    pub include_archived: Option<bool>,
    /// Only return sessions whose recorded `cwd` matches this directory
    /// (path-normalized) — the filter for picking resume
    /// candidates that belong to the current working directory.
    pub cwd: Option<RuntimePath>,
    /// Product role to list. Missing means normal main sessions.
    pub role: Option<SessionRole>,
}

impl RuntimeEncode for ListOptions {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("cursor", &self.cursor)?;
        object.field("limit", &self.limit)?;
        object.field("includeArchived", &self.include_archived)?;
        object.field("cwd", &self.cwd)?;
        object.field("role", &self.role)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ListOptions {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                cursor: object.optional("cursor", &[])?,
                limit: object.optional("limit", &[])?,
                include_archived: object.optional("includeArchived", &[])?,
                cwd: object.optional("cwd", &[])?,
                role: object.optional("role", &[])?,
            })
        })
    }
}

/// One page of `list()` results plus the cursor to fetch the next page.
#[derive(Debug, Clone, PartialEq)]
pub struct ThreadPage {
    pub items: Vec<ThreadSummary>,
    /// Cursor to pass back for the next page, or `None` at end of list.
    pub next_cursor: Option<RuntimeString>,
    /// Stable hash of eligible IDs in this snapshot, independent of content and
    /// ordering. Clients must reconcile membership before pruning cached rows.
    pub membership_fingerprint: Option<RuntimeString>,
    /// Complete eligible header metadata hash, excluding body and stat recency.
    /// Detects external metadata edits that deliberately preserve updatedAt.
    pub metadata_fingerprint: Option<RuntimeString>,
}

/// Authoritative outcome of moving an archived session back into the active
/// namespace.
///
/// This is deliberately richer than an `io::Result<()>`: callers need to
/// distinguish an idempotent repeat from an absent rollout, an active writer,
/// a competing lifecycle operation, and a durable storage failure without
/// guessing from error strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnarchiveSessionStatus {
    Restored,
    AlreadyActive,
    Missing,
    Running,
    Concurrent,
    Shutdown,
    PersistenceFailed,
}

impl RuntimeEncode for UnarchiveSessionStatus {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(
            out,
            match self {
                Self::Restored => "restored",
                Self::AlreadyActive => "already-active",
                Self::Missing => "missing",
                Self::Running => "running",
                Self::Concurrent => "concurrent",
                Self::Shutdown => "shutdown",
                Self::PersistenceFailed => "persistence-failed",
            },
        )
    }
}

impl RuntimeDecode for UnarchiveSessionStatus {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        let variants = [
            RuntimeEnumVariant::new("restored", &[]),
            RuntimeEnumVariant::new("already-active", &[]),
            RuntimeEnumVariant::new("missing", &[]),
            RuntimeEnumVariant::new("running", &[]),
            RuntimeEnumVariant::new("concurrent", &[]),
            RuntimeEnumVariant::new("shutdown", &[]),
            RuntimeEnumVariant::new("persistence-failed", &[]),
        ];
        let values = [
            Self::Restored,
            Self::AlreadyActive,
            Self::Missing,
            Self::Running,
            Self::Concurrent,
            Self::Shutdown,
            Self::PersistenceFailed,
        ];
        runtime_codec::decode_external(value, &variants, |variant| match variant {
            RuntimeExternalVariant::Unit { variant } => values
                .get(variant)
                .copied()
                .ok_or_else(|| RuntimeCodecError::new(RuntimeCodecErrorKind::UnknownEnumVariant)),
            RuntimeExternalVariant::Payload { .. } => Err(RuntimeCodecError::new(
                RuntimeCodecErrorKind::InvalidEnumShape,
            )),
        })
    }
}

/// Result of [`SessionStore::unarchive`].
///
/// `thread` is the authoritative header projection whenever a rollout could be
/// identified. A persistence failure may therefore still include a thread when
/// the namespace move completed but a final durability/cleanup step failed.
#[derive(Debug, Clone, PartialEq)]
pub struct UnarchiveSessionResult {
    pub thread_id: RuntimeString,
    pub status: UnarchiveSessionStatus,
    pub thread: Option<ThreadSummary>,
    pub error: Option<RuntimeString>,
}

impl RuntimeEncode for UnarchiveSessionResult {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("threadId", &self.thread_id)?;
        object.field("status", &self.status)?;
        if let Some(value) = &self.thread {
            object.field("thread", value)?;
        }
        if let Some(value) = &self.error {
            object.field("error", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for UnarchiveSessionResult {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                thread_id: object.required("threadId", &[])?,
                status: object.required("status", &[])?,
                thread: object.optional("thread", &[])?,
                error: object.optional("error", &[])?,
            })
        })
    }
}

impl RuntimeEncode for ThreadPage {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("items", &self.items)?;
        if let Some(value) = &self.next_cursor {
            object.field("nextCursor", value)?;
        }
        if let Some(value) = &self.membership_fingerprint {
            object.field("membershipFingerprint", value)?;
        }
        if let Some(value) = &self.metadata_fingerprint {
            object.field("metadataFingerprint", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ThreadPage {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                items: object.required("items", &[])?,
                next_cursor: object.optional("nextCursor", &[])?,
                membership_fingerprint: object.optional("membershipFingerprint", &[])?,
                metadata_fingerprint: object.optional("metadataFingerprint", &[])?,
            })
        })
    }
}

/// The effort a sub-agent ran under: either a named level or a numeric value.
#[derive(Debug, Clone, PartialEq)]
pub enum SubAgentEffort {
    Text(RuntimeString),
    Number(f64),
}

impl RuntimeEncode for SubAgentEffort {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        match self {
            Self::Text(value) => value.encode_runtime(out),
            Self::Number(value) => value.encode_runtime(out),
        }
    }
}

impl RuntimeDecode for SubAgentEffort {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        match value {
            RuntimeJson::String(value) => Ok(Self::Text(value.clone())),
            RuntimeJson::Number(_) => Ok(Self::Number(f64::decode_runtime(value)?)),
            _ => Err(RuntimeCodecError::new(
                RuntimeCodecErrorKind::NoMatchingUntaggedVariant,
            )),
        }
    }
}

/// Metadata sidecar for a persisted sub-agent transcript (the
/// `agent-<id>.meta.json` file). Lets a cross-process resume recover the agent's
/// role (to re-apply restrictions) without replaying the whole transcript.
#[derive(Debug, Clone, PartialEq)]
pub struct SubAgentMeta {
    /// The agent role/type this sub-agent ran as, if any.
    pub agent_type: Option<RuntimeString>,
    /// The model the sub-agent ran on.
    pub model: RuntimeString,
    /// Provenance: the effective permission mode the sub-agent ran under.
    pub permission_mode: Option<RuntimeString>,
    /// Provenance: the effective effort level the sub-agent ran under, if any.
    pub effort: Option<SubAgentEffort>,
    pub created_at: i64,
}

impl RuntimeEncode for SubAgentMeta {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        if let Some(value) = &self.agent_type {
            object.field("agentType", value)?;
        }
        object.field("model", &self.model)?;
        if let Some(value) = &self.permission_mode {
            object.field("permissionMode", value)?;
        }
        if let Some(value) = &self.effort {
            object.field("effort", value)?;
        }
        object.field("createdAt", &self.created_at)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for SubAgentMeta {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                agent_type: object.optional("agentType", &[])?,
                model: object.required("model", &[])?,
                permission_mode: object.optional("permissionMode", &[])?,
                effort: object.optional("effort", &[])?,
                created_at: object.required("createdAt", &[])?,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unarchive_result_codec_preserves_typed_status_and_authoritative_thread() {
        let result = UnarchiveSessionResult {
            thread_id: "thread-1".into(),
            status: UnarchiveSessionStatus::AlreadyActive,
            thread: Some(ThreadSummary {
                id: "thread-1".into(),
                created_at: 7,
                updated_at: 9,
                cwd: "/repo".into(),
                model: "m".into(),
                title: Some("title".into()),
                role: SessionRole::Main,
                parent_thread_id: None,
                forked_from_id: None,
                archived: Some(false),
                host_metadata: None,
            }),
            error: None,
        };
        let encoded = runtime_codec::encode_value(&result).unwrap();
        let decoded: UnarchiveSessionResult = runtime_codec::decode_value(&encoded).unwrap();
        assert_eq!(decoded, result);
        assert_eq!(
            runtime_codec::encode_text(&result.status)
                .unwrap()
                .try_to_utf8()
                .unwrap(),
            r#""already-active""#
        );
    }

    #[test]
    fn sub_agent_meta_codec_preserves_order_omission_and_non_scalar_utf16() {
        let meta = SubAgentMeta {
            agent_type: Some(RuntimeString::from_utf16(vec![0xd800])),
            model: RuntimeString::from_utf16(vec![u16::from(b'm'), 0xdc00]),
            permission_mode: None,
            effort: Some(SubAgentEffort::Text(RuntimeString::from_utf16(vec![
                0xd800, 0xdc00,
            ]))),
            created_at: 7,
        };
        let encoded = runtime_codec::encode_value(&meta).unwrap();
        let RuntimeJson::Object(object) = &encoded else {
            panic!("sub-agent metadata encodes as an object");
        };
        assert_eq!(
            object.keys().collect::<Vec<_>>(),
            vec![
                RuntimeString::from("agentType"),
                RuntimeString::from("model"),
                RuntimeString::from("effort"),
                RuntimeString::from("createdAt"),
            ]
        );
        let decoded: SubAgentMeta = runtime_codec::decode_value(&encoded).unwrap();
        assert_eq!(decoded, meta);
    }
}

/// Result of reading back a sub-agent's persisted transcript.
#[derive(Debug, Clone, PartialEq)]
pub struct SubAgentTranscript {
    /// The compacted, model-visible projection of the sub-agent's messages.
    pub messages: Vec<Message>,
    /// The sidecar metadata, if present.
    pub meta: Option<SubAgentMeta>,
    /// Tools this sub-agent surfaced via `tool_search`, recovered from its
    /// `tools_surfaced` rollout events, so a cross-process resume keeps them
    /// callable without re-searching.
    pub surfaced_tools: Option<Vec<RuntimeString>>,
    /// The same persisted turn attribution and lifecycle used by main history.
    pub turn_index: Option<crate::engine::session::TurnIndex>,
}

impl RuntimeEncode for SubAgentTranscript {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("messages", &self.messages)?;
        object.field("meta", &self.meta)?;
        object.field("surfacedTools", &self.surfaced_tools)?;
        if let Some(index) = &self.turn_index {
            object.field("turnIndex", index)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for SubAgentTranscript {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                messages: object.required("messages", &[])?,
                meta: object.optional("meta", &[])?,
                surfaced_tools: object.optional("surfacedTools", &[])?,
                turn_index: object.optional("turnIndex", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RolloutMigrationMode {
    #[default]
    DryRun,
    Apply,
}
#[derive(Debug, Clone, Default)]
pub struct RolloutMigrationRequest {
    pub mode: RolloutMigrationMode,
    pub expected_source_revisions: Option<std::collections::BTreeMap<RuntimeString, RuntimeJson>>,
}
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RolloutMigrationReport {
    pub busy: bool,
    pub files: Vec<RuntimeJson>,
}
impl RuntimeEncode for RolloutMigrationReport {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("busy", &self.busy)?;
        object.field("files", &self.files)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

/// Persistence backend for agent sessions.
#[async_trait]
pub trait SessionStore: Send + Sync {
    async fn migrate_rollouts(
        &self,
        _request: RolloutMigrationRequest,
        _cancelled: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> std::io::Result<RolloutMigrationReport> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "session store does not support rollout migration",
        ))
    }

    async fn compress_cold_rollouts_with_cancellation(
        &self,
        _cancelled: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> std::io::Result<crate::store::compression::CompressionReport> {
        self.compress_cold_rollouts().await
    }
    /// Run the same cold-storage maintenance pass used by automatic startup work.
    /// Manual requests retain the age threshold and all writer/lifecycle fences.
    async fn compress_cold_rollouts(
        &self,
    ) -> std::io::Result<crate::store::compression::CompressionReport> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "session store does not support cold rollout compression",
        ))
    }

    /// Engine-owned durable output directory for this thread. Backends without a
    /// filesystem sidecar seam return `None`; recovered records then report
    /// `outputAvailable: false` rather than claiming bytes survived.
    fn background_process_output_dir(&self, _thread_id: &str) -> Option<RuntimePath> {
        None
    }
    /// Create a new thread identity, rejecting an existing active or archived
    /// identity atomically with writer creation. Publish neither a replacement
    /// nor an index row before its header write has succeeded.
    async fn create(&self, meta: SessionMeta) -> std::io::Result<Box<dyn LiveRollout>>;
    /// Publish a complete external history under the same thread lifecycle
    /// exclusion as create/archive/delete. No partial prefix may become visible.
    /// Existing identities are accepted only for an identical committed import.
    async fn import_external_session(
        &self,
        _import: crate::store::external_import::ExternalSessionImport,
    ) -> std::io::Result<crate::store::external_import::ExternalSessionImportResult> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "session store does not support atomic external history import",
        ))
    }
    /// Inspect durable history without resuming a model, changing archive state,
    /// acquiring a writer, or recovering lifecycle journals.
    async fn read_thread(&self, _thread_id: &str) -> std::io::Result<ThreadReadSnapshot> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "session store does not support read-only thread inspection",
        ))
    }
    /// Read original top-level messages by UUID, preserving request order and
    /// the first persisted occurrence. Unknown IDs are omitted so the authority
    /// consumer can report its own missing-source error.
    async fn read_messages_by_ids(
        &self,
        thread_id: &str,
        ids: &[RuntimeString],
    ) -> std::io::Result<Vec<Message>> {
        let snapshot = self.read_thread(thread_id).await?;
        Ok(crate::store::runtime_resume::select_messages(
            &snapshot.raw_items,
            ids,
        ))
    }
    /// Serve one turn window of a saved rollout from the backend's durable
    /// turn index, projecting only the served slice. `None` keeps the caller
    /// on the whole-file projection.
    async fn paged_thread_history(
        &self,
        _thread_id: &str,
        _cursor: Option<&str>,
        _limit: Option<usize>,
        _items_view: Option<&str>,
        _sort_direction: Option<&str>,
    ) -> std::io::Result<Option<crate::store::thread_index::ThreadPageSnapshot>> {
        Ok(None)
    }
    /// Serve a saved turn's message window without decoding unrequested rows.
    async fn paged_turn_slice(
        &self,
        _thread_id: &str,
        _turn_id: &str,
        _cursor: Option<&str>,
        _limit: Option<usize>,
        _sort_direction: Option<&str>,
    ) -> std::io::Result<Option<crate::store::thread_index::ThreadPageSnapshot>> {
        Ok(None)
    }
    /// Read items older than a stable item identity independently of storage cursors.
    async fn paged_turn_slice_before(
        &self,
        _thread_id: &str,
        _turn_id: &str,
        _before_item_id: &str,
        _limit: Option<usize>,
    ) -> std::io::Result<Option<crate::store::thread_index::ThreadPageSnapshot>> {
        Ok(None)
    }
    /// Complete saved change metadata, independent of transcript page membership.
    async fn saved_file_changes(
        &self,
        _thread_id: &str,
    ) -> std::io::Result<Option<crate::engine::session::SessionFileChanges>> {
        Ok(None)
    }
    /// Inspect a saved child without archive recovery, writer locks, or model startup.
    async fn read_sub_agent(
        &self,
        _thread_id: &str,
        _agent_id: &str,
    ) -> std::io::Result<SubAgentReadSnapshot> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "session store does not support read-only sub-agent history",
        ))
    }
    async fn resume(&self, thread_id: &str) -> std::io::Result<ResumeResult>;
    /// Acquire a continuation writer and load its authoritative history as one
    /// indivisible store operation. Implementations must not emulate this with
    /// separate [`SessionStore::resume`] and [`SessionStore::reopen`] calls: that
    /// creates a TOCTOU window in which accepted records can be omitted from the
    /// resumed in-memory state.
    ///
    /// This method is intentionally required rather than given a fallback so a
    /// custom backend cannot silently opt into the unsafe split operation.
    async fn resume_for_write(&self, thread_id: &str) -> std::io::Result<ResumedLiveRollout>;
    /// Atomic continuation projection. The default keeps existing backends safe
    /// by adapting their leased full-history read; file stores can avoid decoding
    /// obsolete snapshots without weakening `ResumeResult::raw_items`.
    async fn resume_runtime_for_write(
        &self,
        thread_id: &str,
    ) -> std::io::Result<ResumedRuntimeRollout> {
        let resumed = self.resume_for_write(thread_id).await?;
        Ok(ResumedRuntimeRollout {
            resume: resumed.resume.into(),
            rollout: resumed.rollout,
        })
    }
    /// Re-attach a writer to an existing thread so a resumed session persists.
    async fn reopen(&self, thread_id: &str) -> std::io::Result<Box<dyn LiveRollout>>;
    /// List sessions with cursor/limit pagination, newest first.
    async fn list(&self, options: ListOptions) -> std::io::Result<ThreadPage>;
    /// Read one active or archived catalog row without reconstructing history,
    /// recovering lifecycle journals, acquiring a writer, or starting a model.
    async fn read_session_summary(
        &self,
        _thread_id: &str,
    ) -> std::io::Result<Option<ThreadSummary>> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "session store does not support metadata-only session inspection",
        ))
    }
    /// Read lifetime activity without session activation or lifecycle recovery.
    async fn profile_activity(
        &self,
        _now_ms: i64,
    ) -> std::io::Result<crate::store::profile_activity::ProfileActivity> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "session store does not support read-only profile activity",
        ))
    }
    /// Full-text search over persisted conversation content, newest session
    /// first. Only human-visible text participates (user/assistant text,
    /// tool-result text, compaction summaries). `accel` lets file-backed
    /// stores select candidates through the bundled ripgrep; `None` only
    /// affects speed, never the result set. Backends without a native
    /// implementation report `Unsupported`.
    async fn search(
        &self,
        _options: &SessionSearchOptions,
        _accel: Option<&RuntimeProcessContext>,
        _signal: &AbortSignal,
    ) -> std::io::Result<SessionSearchPage> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "session store does not support full-text search",
        ))
    }
    /// Move a session's file into the archived subdir. Returns its new path.
    async fn archive(&self, thread_id: &str) -> std::io::Result<String>;
    /// Idempotently move an archived rollout and its sidechain tree back into
    /// the active namespace. Every durable/storage failure is materialized in
    /// the typed result so protocol adapters never infer lifecycle state from
    /// platform-dependent `io::Error` strings.
    async fn unarchive(&self, thread_id: &str) -> UnarchiveSessionResult;
    /// Delete the main session rollout plus its per-session sidechain directory.
    async fn delete(&self, thread_id: &str) -> std::io::Result<()>;
    /// Re-key a persisted thread: move its rollout, sidechain tree, and header
    /// id onto `new_thread_id`. The thread must have no live writer and the
    /// target id must be unclaimed in every namespace. Exists so hosts
    /// converging on single-id identity can migrate legacy engine-minted
    /// thread ids onto their own session keys; stores without re-key support
    /// keep this default `Unsupported` error.
    async fn rename(&self, thread_id: &str, new_thread_id: &str) -> std::io::Result<()> {
        let _ = new_thread_id;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            format!("session store does not support renaming thread {thread_id}"),
        ))
    }
    /// Update the authoritative session title without rewriting saved history.
    async fn update_title(&self, thread_id: &str, title: &str) -> std::io::Result<()>;

    /// Replace the thread's opaque host-owned attachment in metadata storage
    /// (`None` removes it). The engine never interprets the value; whole-value
    /// replacement keeps the write semantics trivial for every backend.
    async fn update_host_metadata(
        &self,
        thread_id: &str,
        host_metadata: Option<RuntimeJson>,
    ) -> std::io::Result<()> {
        let _ = host_metadata;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            format!("session store does not support host metadata for thread {thread_id}"),
        ))
    }

    /// Open (create-or-append) the per-agent sidechain rollout for a sub-agent,
    /// at `<session>/subagents/agent-<agentId>.jsonl` (+ meta
    /// sidecar). Each sub-agent gets its OWN append-only file so its transcript
    /// survives the spawning turn AND a process restart; a later
    /// [`SessionStore::load_sub_agent`] reads it back so `resume` can continue
    /// it cross-process.
    async fn open_sub_agent(
        &self,
        thread_id: &str,
        agent_id: &str,
        meta: SubAgentMeta,
    ) -> std::io::Result<Box<dyn LiveRollout>>;
    /// Read a sub-agent's persisted transcript (compacted projection) + its
    /// meta. Returns empty messages when no file exists yet.
    async fn load_sub_agent(
        &self,
        thread_id: &str,
        agent_id: &str,
    ) -> std::io::Result<SubAgentTranscript>;
    /// List the agentIds that have a persisted transcript under a session (glob).
    async fn list_sub_agents(&self, thread_id: &str) -> std::io::Result<Vec<String>>;

    /// Count session files whose contents were touched (by file mtime) at or
    /// after `since_ms`. Backs the auto-memory consolidation gate:
    /// "have at least N distinct sessions been worked on since the
    /// last consolidation?". The header index sorts on createdAt only, so this
    /// scans mtimes directly. `exclude_id` drops the calling session from the
    /// tally; archived sessions and non-session files are ignored.
    async fn count_sessions_touched_since(
        &self,
        since_ms: f64,
        exclude_id: Option<&str>,
    ) -> std::io::Result<usize>;
}
