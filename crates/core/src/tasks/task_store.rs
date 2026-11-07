//! Durable tasks and explicit current-plan membership share one list-wide lock.
//! Model TaskCreate allocates max-present numeric ID + 1, with private task
//! incarnations fencing stale references after deletion and numeric reuse.
//! Ordinary TaskWrite steps use opaque IDs and do not consume task numbers.
//! The low-level create_task adapter retains its monotonic high-water allocator.
//! Task dependencies and archived plans remain available across replacements.

use crate::fs::lockfile::{with_lock, LockOptions};
use crate::fs::paths::{sanitize_path_component, tasks_dir};
use crate::fs::watcher::{DirectoryWatcher, DirectoryWatcherOptions};
use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, runtime_json, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode,
    RuntimeEncode, RuntimeEncoder, RuntimeJson, RuntimeObject, RuntimeObjectEncoder, RuntimeString,
};
use crate::schemas::message::Message;
use crate::types::reminder::{ReminderContext, ReminderProvider, RequestReminder};
use async_trait::async_trait;
use futures::FutureExt;
use sha2::{Digest, Sha256};
use std::cmp::Ordering as CmpOrdering;
use std::collections::HashMap;
use std::fmt;
use std::io;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant, SystemTime};
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

mod plan;
pub use plan::{
    list_plan_tasks, list_plan_view, write_task_plan, write_task_plan_approved, TaskPlanApprover,
    TaskPlanBinding, TaskPlanChange, TaskPlanView, TaskPlanWriteEntry,
};

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

pub const TASK_STATUSES: [TaskStatus; 3] = [
    TaskStatus::Pending,
    TaskStatus::InProgress,
    TaskStatus::Completed,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskStatus {
    Pending,
    InProgress,
    Completed,
}

impl RuntimeEncode for TaskStatus {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Pending => "pending",
            Self::InProgress => "in_progress",
            Self::Completed => "completed",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for TaskStatus {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("pending", &[]),
                RuntimeEnumVariant::new("in_progress", &[]),
                RuntimeEnumVariant::new("completed", &[]),
            ],
            &[Self::Pending, Self::InProgress, Self::Completed],
        )
    }
}

impl TaskStatus {
    /// The wire/string form (`"pending" | "in_progress" | "completed"`).
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskStatus::Pending => "pending",
            TaskStatus::InProgress => "in_progress",
            TaskStatus::Completed => "completed",
        }
    }
}

impl fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Task {
    pub id: RuntimeString,
    pub subject: RuntimeString,
    pub description: RuntimeString,
    pub active_form: Option<RuntimeString>,
    pub owner: Option<RuntimeString>,
    pub status: TaskStatus,
    pub blocks: Vec<RuntimeString>,
    pub blocked_by: Vec<RuntimeString>,
    pub metadata: Option<RuntimeObject>,
}

/// A task's durable instance and semantic content, independent of other tasks.
#[derive(Debug, Clone, PartialEq)]
pub struct TaskIdentity {
    pub task: Task,
    pub generation: RuntimeString,
    pub fingerprint: RuntimeString,
}

impl RuntimeEncode for Task {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("id", &self.id)?;
        object.field("subject", &self.subject)?;
        object.field("description", &self.description)?;
        if let Some(value) = &self.active_form {
            object.field("activeForm", value)?;
        }
        if let Some(value) = &self.owner {
            object.field("owner", value)?;
        }
        object.field("status", &self.status)?;
        object.field("blocks", &self.blocks)?;
        object.field("blockedBy", &self.blocked_by)?;
        if let Some(value) = &self.metadata {
            object.field("metadata", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for Task {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                id: object.required("id", &[])?,
                subject: object.required("subject", &[])?,
                description: object.required("description", &[])?,
                active_form: object.optional("activeForm", &[])?,
                owner: object.optional("owner", &[])?,
                status: object.required("status", &[])?,
                blocks: object.defaulted("blocks", &[], Vec::new)?,
                blocked_by: object.defaulted("blockedBy", &[], Vec::new)?,
                metadata: object.optional("metadata", &[])?,
            })
        })
    }
}

const DEFAULT_TASK_LIST_ID: &str = "tasklist";
const HIGH_WATER_MARK_FILE: &str = ".highwatermark";
const SNAPSHOT_LEDGER_FILE: &str = ".snapshot-ledger.json";
const SNAPSHOT_LEDGER_SCHEMA_VERSION: u32 = 2;
const DEFAULT_PAGE_LIMIT: u32 = 100;
const MAX_PAGE_LIMIT: u32 = 1_000;
const MAX_TASK_LIST_WATERMARKS: usize = 1_024;
const LOCK_OPTS: LockOptions = LockOptions {
    retries: Some(30),
    min_timeout_ms: Some(5),
    max_timeout_ms: Some(100),
    stale_ms: None,
};

/// Resolve which task list to read/write. The engine's explicit override
/// wins, followed by the `CLAUDE_CODE_TASK_LIST_ID` override, then the shared
/// team-name projection, session id, and default. Supporting both env names is
/// required for peers to share data and proper-lockfile boundaries; a legacy
/// `team:` prefix is intentionally not retained.
pub fn get_task_list_id(
    session_id: Option<&RuntimeString>,
    team_name: Option<&RuntimeString>,
) -> RuntimeString {
    if let Ok(env) = std::env::var("KLAUS_TASK_LIST_ID") {
        if !env.is_empty() {
            return env.into();
        }
    }
    if let Ok(env) = std::env::var("CLAUDE_CODE_TASK_LIST_ID") {
        if !env.is_empty() {
            return env.into();
        }
    }
    if let Some(team) = team_name {
        if !team.is_empty() {
            return team_task_list_id(team);
        }
    }
    match session_id {
        Some(session_id) if !session_id.is_empty() => session_id.clone(),
        _ => DEFAULT_TASK_LIST_ID.into(),
    }
}

/// TeamCreate projects the raw UI/roster team name through
/// JavaScript `/[^a-zA-Z0-9]/g` and ASCII lowercase before using it as the task
/// list id. Operate on UTF-16 code units so astral and lone-surrogate behavior
/// is identical to JavaScript without the `u` regex flag.
pub(crate) fn team_task_list_id(team: &RuntimeString) -> RuntimeString {
    RuntimeString::from_utf16(
        team.as_utf16()
            .iter()
            .map(|unit| match *unit {
                value if (u16::from(b'A')..=u16::from(b'Z')).contains(&value) => {
                    value + u16::from(b'a' - b'A')
                }
                value
                    if (u16::from(b'a')..=u16::from(b'z')).contains(&value)
                        || (u16::from(b'0')..=u16::from(b'9')).contains(&value) =>
                {
                    value
                }
                _ => u16::from(b'-'),
            })
            .collect::<Vec<u16>>(),
    )
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct CreateTaskInput {
    pub subject: RuntimeString,
    pub description: RuntimeString,
    pub active_form: Option<RuntimeString>,
    /// The peer that owns this task (swarm auto-owner); unset outside a team.
    pub owner: Option<RuntimeString>,
    pub metadata: Option<RuntimeObject>,
}

impl RuntimeEncode for CreateTaskInput {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("subject", &self.subject)?;
        object.field("description", &self.description)?;
        if let Some(value) = &self.active_form {
            object.field("activeForm", value)?;
        }
        if let Some(value) = &self.owner {
            object.field("owner", value)?;
        }
        if let Some(value) = &self.metadata {
            object.field("metadata", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for CreateTaskInput {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                subject: object.required("subject", &[])?,
                description: object.required("description", &[])?,
                active_form: object.optional("activeForm", &[])?,
                owner: object.optional("owner", &[])?,
                metadata: object.optional("metadata", &[])?,
            })
        })
    }
}

/// A partial update merged shallowly onto an existing task (`None` = keep the
/// current value; the task's id is never changed).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TaskUpdate {
    pub subject: Option<RuntimeString>,
    pub description: Option<RuntimeString>,
    pub active_form: Option<RuntimeString>,
    pub owner: Option<RuntimeString>,
    pub status: Option<TaskStatus>,
    pub blocks: Option<Vec<RuntimeString>>,
    pub blocked_by: Option<Vec<RuntimeString>>,
    pub metadata: Option<RuntimeObject>,
}

impl RuntimeEncode for TaskUpdate {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        if let Some(value) = &self.subject {
            object.field("subject", value)?;
        }
        if let Some(value) = &self.description {
            object.field("description", value)?;
        }
        if let Some(value) = &self.active_form {
            object.field("activeForm", value)?;
        }
        if let Some(value) = &self.owner {
            object.field("owner", value)?;
        }
        if let Some(value) = &self.status {
            object.field("status", value)?;
        }
        if let Some(value) = &self.blocks {
            object.field("blocks", value)?;
        }
        if let Some(value) = &self.blocked_by {
            object.field("blockedBy", value)?;
        }
        if let Some(value) = &self.metadata {
            object.field("metadata", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for TaskUpdate {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                subject: object.optional("subject", &[])?,
                description: object.optional("description", &[])?,
                active_form: object.optional("activeForm", &[])?,
                owner: object.optional("owner", &[])?,
                status: object.optional("status", &[])?,
                blocks: object.optional("blocks", &[])?,
                blocked_by: object.optional("blockedBy", &[])?,
                metadata: object.optional("metadata", &[])?,
            })
        })
    }
}

/// Cursor disposition for a task-list query. A stale or malformed cursor
/// restarts at the first page while making that reset explicit to the Host.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskListCursorStatus {
    Current,
    Stale,
    Invalid,
}

impl RuntimeEncode for TaskListCursorStatus {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(
            out,
            match self {
                Self::Current => "current",
                Self::Stale => "stale",
                Self::Invalid => "invalid",
            },
        )
    }
}

impl RuntimeDecode for TaskListCursorStatus {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("current", &[]),
                RuntimeEnumVariant::new("stale", &[]),
                RuntimeEnumVariant::new("invalid", &[]),
            ],
            &[Self::Current, Self::Stale, Self::Invalid],
        )
    }
}

/// Pagination controls for one authoritative TodoV2 snapshot query.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TaskListQueryOptions {
    pub cursor: Option<RuntimeString>,
    pub limit: Option<u32>,
}

impl RuntimeEncode for TaskListQueryOptions {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        if let Some(value) = &self.cursor {
            object.field("cursor", value)?;
        }
        if let Some(value) = &self.limit {
            object.field("limit", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for TaskListQueryOptions {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                cursor: object.optional("cursor", &[])?,
                limit: object.optional("limit", &[])?,
            })
        })
    }
}

/// One page from the authoritative, persisted TodoV2 list owned by a session.
#[derive(Debug, Clone, PartialEq)]
pub struct TaskListSnapshot {
    pub session_id: RuntimeString,
    pub task_list_id: RuntimeString,
    pub plan_id: Option<RuntimeString>,
    pub plan_kind: RuntimeString,
    /// Opaque durable list incarnation. Consumers compare this token only for
    /// equality/recreation; `revision` is monotonic within one generation, and
    /// cross-generation order comes from the event FIFO/query fence rather
    /// than lexicographic UUID ordering.
    pub generation: RuntimeString,
    pub revision: u64,
    pub fingerprint: RuntimeString,
    pub tasks: Vec<Task>,
    pub next_cursor: Option<RuntimeString>,
    pub total: u64,
    pub cursor_status: TaskListCursorStatus,
}

impl RuntimeEncode for TaskListSnapshot {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("sessionId", &self.session_id)?;
        object.field("taskListId", &self.task_list_id)?;
        if let Some(plan_id) = &self.plan_id {
            object.field("planId", plan_id)?;
        }
        object.field("planKind", &self.plan_kind)?;
        object.field("generation", &self.generation)?;
        object.field("revision", &self.revision)?;
        object.field("fingerprint", &self.fingerprint)?;
        object.field("tasks", &self.tasks)?;
        if let Some(value) = &self.next_cursor {
            object.field("nextCursor", value)?;
        }
        object.field("total", &self.total)?;
        object.field("cursorStatus", &self.cursor_status)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for TaskListSnapshot {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                session_id: object.required("sessionId", &[])?,
                task_list_id: object.required("taskListId", &[])?,
                plan_id: object.optional("planId", &[])?,
                plan_kind: object.defaulted("planKind", &[], || "tasks".into())?,
                generation: object.required("generation", &[])?,
                revision: object.required("revision", &[])?,
                fingerprint: object.required("fingerprint", &[])?,
                tasks: object.required("tasks", &[])?,
                next_cursor: object.optional("nextCursor", &[])?,
                total: object.required("total", &[])?,
                cursor_status: object.required("cursorStatus", &[])?,
            })
        })
    }
}

/// Ordered full-state invalidation emitted after a visible task mutation is
/// durable. The full state lets reconnecting Hosts recover without guessing a
/// directory or projecting sub-agent state.
#[derive(Debug, Clone, PartialEq)]
pub struct TaskListChange {
    pub session_id: RuntimeString,
    pub task_list_id: RuntimeString,
    pub plan_id: Option<RuntimeString>,
    pub plan_kind: RuntimeString,
    pub generation: RuntimeString,
    pub revision: u64,
    pub fingerprint: RuntimeString,
    pub tasks: Vec<Task>,
}

impl RuntimeEncode for TaskListChange {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("sessionId", &self.session_id)?;
        object.field("taskListId", &self.task_list_id)?;
        if let Some(plan_id) = &self.plan_id {
            object.field("planId", plan_id)?;
        }
        object.field("planKind", &self.plan_kind)?;
        object.field("generation", &self.generation)?;
        object.field("revision", &self.revision)?;
        object.field("fingerprint", &self.fingerprint)?;
        object.field("tasks", &self.tasks)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for TaskListChange {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                session_id: object.required("sessionId", &[])?,
                task_list_id: object.required("taskListId", &[])?,
                plan_id: object.optional("planId", &[])?,
                plan_kind: object.defaulted("planKind", &[], || "tasks".into())?,
                generation: object.required("generation", &[])?,
                revision: object.required("revision", &[])?,
                fingerprint: object.required("fingerprint", &[])?,
                tasks: object.required("tasks", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TaskListState {
    pub(crate) task_list_id: RuntimeString,
    pub(crate) plan_id: Option<RuntimeString>,
    pub(crate) plan_kind: RuntimeString,
    /// Durable incarnation of the on-disk list directory. A recreated
    /// directory starts a new generation even when its revision is lower than
    /// the previous incarnation.
    pub(crate) storage_generation: RuntimeString,
    pub(crate) revision: u64,
    pub(crate) fingerprint: RuntimeString,
    pub(crate) tasks: Vec<Task>,
}

pub type TaskListChangeSubscriber = Arc<dyn Fn(TaskListChange) + Send + Sync>;
pub(crate) type TaskListStateSubscriber = Arc<dyn Fn(TaskListState, u64, u64) + Send + Sync>;

/// Internal linearization metadata for a correlated authoritative query. The
/// cutoff is process-local and deliberately excluded from Runtime/wire codecs.
#[doc(hidden)]
pub struct TaskListQueryResult {
    pub snapshot: TaskListSnapshot,
    pub publication_cutoff: u64,
}

/// Internal ordered delivery metadata used by protocol adapters. The public
/// Host contract remains [`TaskListChange`] generation/revision/fingerprint.
#[doc(hidden)]
pub struct TaskListChangeEnvelope {
    pub change: TaskListChange,
    pub publication_sequence: u64,
}

#[doc(hidden)]
pub type TaskListChangeEnvelopeSubscriber = Arc<dyn Fn(TaskListChangeEnvelope) + Send + Sync>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TaskListBinding {
    pub(crate) session_id: RuntimeString,
    pub(crate) list_id: RuntimeString,
    pub(crate) generation: u64,
}

pub(crate) type TaskListIdSource = Arc<dyn Fn() -> Option<TaskListBinding> + Send + Sync>;

#[derive(Default)]
struct TaskChangeHub {
    next_id: u64,
    subscribers: Vec<TaskStateSubscriberEntry>,
    next_watermark_sequence: u64,
}

struct TaskStateSubscriberEntry {
    id: u64,
    list_id_source: TaskListIdSource,
    active: Arc<AtomicBool>,
    delivery_tx: std::sync::mpsc::Sender<(TaskListState, u64, u64)>,
    delivery_cutoff: Arc<std::sync::atomic::AtomicU64>,
    last_published: HashMap<TaskListPublishKey, TaskListWatermark>,
}

enum TaskWatchWorker {
    Tokio(tokio::task::JoinHandle<()>),
    Thread(std::thread::JoinHandle<()>),
}

/// Session-retained watcher ownership. `close` stops admission immediately;
/// core shutdown additionally awaits `close_and_wait`, so no scan or publish
/// can run after the Session's completion boundary.
pub(crate) struct TaskListSubscription {
    id: u64,
    cancel: CancellationToken,
    wake: Arc<tokio::sync::Notify>,
    closed: AtomicBool,
    active: Arc<AtomicBool>,
    minimum_binding_generation: Arc<std::sync::atomic::AtomicU64>,
    worker: Mutex<Option<TaskWatchWorker>>,
    dispatcher: Mutex<Option<std::thread::JoinHandle<()>>>,
    completion: tokio::sync::watch::Receiver<Option<Result<(), String>>>,
    dispatcher_completion: tokio::sync::watch::Receiver<Option<Result<(), String>>>,
}

impl TaskListSubscription {
    pub(crate) fn close(&self) {
        if self.closed.swap(true, AtomicOrdering::SeqCst) {
            return;
        }
        self.cancel.cancel();
        self.active.store(false, AtomicOrdering::SeqCst);
        let mut hub = task_change_hub().lock().unwrap();
        hub.subscribers.retain(|entry| entry.id != self.id);
    }

    /// Wake the worker immediately after a Session changes its authoritative
    /// task-list identity (for example TeamCreate), matching CC's synchronous
    /// `notifyTasksUpdated` rewatch behavior.
    pub(crate) fn identity_changed(&self, generation: u64) {
        self.minimum_binding_generation
            .store(generation, AtomicOrdering::SeqCst);
        self.wake.notify_one();
    }

    pub(crate) async fn close_and_wait(&self) -> io::Result<()> {
        self.close();
        let mut completion = self.completion.clone();
        let watcher_result = loop {
            if let Some(result) = completion.borrow().clone() {
                break result;
            }
            if completion.changed().await.is_err() {
                break Err("TodoV2 watcher ended without a completion verdict".to_string());
            }
        };
        let mut errors = Vec::new();
        if let Err(error) = watcher_result {
            errors.push(error);
        }
        let worker = self.worker.lock().unwrap().take();
        match worker {
            Some(TaskWatchWorker::Tokio(worker)) => {
                if let Err(error) = worker.await {
                    errors.push(format!("TodoV2 watcher task failed: {error}"));
                }
            }
            Some(TaskWatchWorker::Thread(worker)) => {
                // The completion watch above is published at the end of this
                // OS thread, so this join cannot block. Avoid leaving a Tokio
                // blocking-pool waiter behind if an outer shutdown deadline
                // drops close_and_wait before an arbitrary callback exits.
                if worker.join().is_err() {
                    errors.push("TodoV2 watcher thread panicked".to_string());
                }
            }
            None => {}
        }
        let mut dispatcher_completion = self.dispatcher_completion.clone();
        let dispatcher_result = loop {
            if let Some(result) = dispatcher_completion.borrow().clone() {
                break result;
            }
            if dispatcher_completion.changed().await.is_err() {
                break Err(
                    "TodoV2 callback dispatcher ended without a completion verdict".to_string(),
                );
            }
        };
        if let Err(error) = dispatcher_result {
            errors.push(error);
        }
        // Never park Tokio's blocking pool on arbitrary Host code. Once the
        // completion verdict is visible, this OS-thread join is immediate. If
        // an outer deadline drops us first, the subscription retains (and on
        // process teardown detaches) the ordinary std::thread handle.
        let dispatcher = self.dispatcher.lock().unwrap().take();
        if let Some(dispatcher) = dispatcher {
            if dispatcher.join().is_err() {
                errors.push("TodoV2 callback dispatcher panicked".to_string());
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(io::Error::other(errors.join("; ")))
        }
    }

    /// Gated with its only user, the unix-only reaper-timeout case in
    /// `engine::session_run_tests`.
    #[cfg(all(test, unix))]
    pub(crate) fn is_finished(&self) -> bool {
        self.completion.borrow().is_some() && self.dispatcher_completion.borrow().is_some()
    }
}

impl Drop for TaskListSubscription {
    fn drop(&mut self) {
        self.close();
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TaskListPublishKey {
    task_list_id: RuntimeString,
    storage_dir: PathBuf,
    binding_generation: u64,
    storage_generation: RuntimeString,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TaskListWatermark {
    revision: u64,
    fingerprint: RuntimeString,
    last_touched: u64,
}

static TASK_CHANGE_HUB: OnceLock<Arc<Mutex<TaskChangeHub>>> = OnceLock::new();

/// Serializes capture plus publication for one concrete task directory across
/// the proper-lockfile release verdict. The registry keeps only weak entries
/// and prunes them on every lookup, so dormant list identities do not
/// accumulate while concurrent operations on the same sanitized directory
/// always share one gate.
static TASK_PUBLICATION_GATES: OnceLock<Mutex<HashMap<PathBuf, Weak<tokio::sync::Mutex<()>>>>> =
    OnceLock::new();

fn task_publication_gate(dir: &Path) -> Arc<tokio::sync::Mutex<()>> {
    let gates = TASK_PUBLICATION_GATES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut gates = gates.lock().unwrap();
    gates.retain(|_, gate| gate.strong_count() > 0);
    if let Some(gate) = gates.get(dir).and_then(Weak::upgrade) {
        return gate;
    }
    let gate = Arc::new(tokio::sync::Mutex::new(()));
    gates.insert(dir.to_path_buf(), Arc::downgrade(&gate));
    gate
}

#[cfg(test)]
struct TaskPublishSendGate {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<(Mutex<bool>, std::sync::Condvar)>,
}

#[cfg(test)]
static TASK_PUBLISH_SEND_GATE: OnceLock<Mutex<Option<TaskPublishSendGate>>> = OnceLock::new();

#[cfg(test)]
fn install_task_publish_send_gate() -> (
    Arc<tokio::sync::Notify>,
    Arc<(Mutex<bool>, std::sync::Condvar)>,
) {
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
    *TASK_PUBLISH_SEND_GATE
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap() = Some(TaskPublishSendGate {
        entered: entered.clone(),
        release: release.clone(),
    });
    (entered, release)
}

#[cfg(test)]
fn wait_at_task_publish_send_gate() {
    let gate = TASK_PUBLISH_SEND_GATE
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap()
        .take();
    let Some(gate) = gate else {
        return;
    };
    gate.entered.notify_one();
    let (released, ready) = &*gate.release;
    let mut released = released.lock().unwrap();
    while !*released {
        released = ready.wait(released).unwrap();
    }
}

#[cfg(not(test))]
fn wait_at_task_publish_send_gate() {}

fn task_change_hub() -> &'static Arc<Mutex<TaskChangeHub>> {
    TASK_CHANGE_HUB.get_or_init(|| Arc::new(Mutex::new(TaskChangeHub::default())))
}

/// Subscribe to all authoritative list-state changes. Session wraps this
/// internal stream and filters it by its core-resolved current list identity.
pub(crate) fn subscribe_task_states(
    list_id_source: TaskListIdSource,
    subscriber: TaskListStateSubscriber,
) -> Arc<TaskListSubscription> {
    let active = Arc::new(AtomicBool::new(true));
    let initial_generation = list_id_source()
        .map(|binding| binding.generation)
        .unwrap_or_default();
    let minimum_binding_generation =
        Arc::new(std::sync::atomic::AtomicU64::new(initial_generation));
    let delivery_cutoff = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let (delivery_tx, delivery_rx) = std::sync::mpsc::channel();
    let (id, cancel) = {
        let mut hub = task_change_hub().lock().unwrap();
        let id = hub.next_id;
        hub.next_id = hub
            .next_id
            .checked_add(1)
            .expect("task subscriber id overflow");
        hub.subscribers.push(TaskStateSubscriberEntry {
            id,
            list_id_source: list_id_source.clone(),
            active: active.clone(),
            delivery_tx,
            delivery_cutoff: delivery_cutoff.clone(),
            last_published: HashMap::new(),
        });
        (id, CancellationToken::new())
    };
    let wake = Arc::new(tokio::sync::Notify::new());
    let dispatcher_active = active.clone();
    let dispatcher_minimum_generation = minimum_binding_generation.clone();
    let dispatcher_delivery_cutoff = delivery_cutoff;
    let (dispatcher_completion_send, dispatcher_completion) = tokio::sync::watch::channel(None);
    let dispatcher = std::thread::Builder::new()
        .name(format!("todo-v2-callback-{id}"))
        .spawn(move || {
            let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
                while let Ok((state, generation, publication_sequence)) = delivery_rx.recv() {
                    if dispatcher_active.load(AtomicOrdering::SeqCst)
                        && generation >= dispatcher_minimum_generation.load(AtomicOrdering::SeqCst)
                    {
                        // Arbitrary Host callbacks are isolated from Tokio. A
                        // blocking callback can hit the typed shutdown deadline
                        // but cannot stop async cancellation or timer progress.
                        let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
                            // This SeqCst load is the delivery-admission
                            // linearization point against a query's SeqCst
                            // fetch_max. A load before that fetch admits the
                            // publication before the query; a load after it
                            // suppresses a publication through the query's
                            // authoritative cutoff. Keep it immediately next
                            // to the arbitrary subscriber invocation.
                            if publication_sequence
                                <= dispatcher_delivery_cutoff.load(AtomicOrdering::SeqCst)
                            {
                                return;
                            }
                            subscriber(state, generation, publication_sequence)
                        }));
                    }
                }
            }))
            .map(|_| ())
            .map_err(|_| "TodoV2 callback dispatcher panicked".to_string());
            let _ = dispatcher_completion_send.send(Some(result));
        })
        .expect("TodoV2 callback dispatcher thread construction failed");
    let worker = watch_task_state_changes(id, list_id_source, cancel.clone(), wake.clone());
    let (completion_send, completion) = tokio::sync::watch::channel(None);
    let worker = match tokio::runtime::Handle::try_current() {
        Ok(runtime) => TaskWatchWorker::Tokio(runtime.spawn(async move {
            let result = AssertUnwindSafe(worker)
                .catch_unwind()
                .await
                .map(|_| ())
                .map_err(|_| "TodoV2 watcher task panicked".to_string());
            let _ = completion_send.send(Some(result));
        })),
        Err(_) => {
            // Session subscriptions are normally installed by an async Host,
            // but the core API is synchronous. Preserve the same cross-process
            // contract for embedders that subscribe outside a Tokio runtime.
            TaskWatchWorker::Thread(std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("TodoV2 watcher runtime construction failed");
                let result = runtime
                    .block_on(AssertUnwindSafe(worker).catch_unwind())
                    .map(|_| ())
                    .map_err(|_| "TodoV2 watcher thread panicked".to_string());
                let _ = completion_send.send(Some(result));
            }))
        }
    };
    Arc::new(TaskListSubscription {
        id,
        cancel,
        wake,
        closed: AtomicBool::new(false),
        active,
        minimum_binding_generation,
        worker: Mutex::new(Some(worker)),
        dispatcher: Mutex::new(Some(dispatcher)),
        completion,
        dispatcher_completion,
    })
}

#[cfg(test)]
fn task_watch_fallback_interval() -> Duration {
    let override_ms = TASK_WATCH_FALLBACK_OVERRIDE_MS.load(AtomicOrdering::SeqCst);
    Duration::from_millis(if override_ms == 0 { 25 } else { override_ms })
}

#[cfg(test)]
static TASK_WATCH_FALLBACK_OVERRIDE_MS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
#[cfg(test)]
static TASK_WATCH_RECONCILE_COUNT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
#[cfg(test)]
type TaskWatchScanGate = Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>;
#[cfg(test)]
static TASK_WATCH_INITIAL_SCAN_GATE: OnceLock<TaskWatchScanGate> = OnceLock::new();

#[cfg(test)]
fn install_task_watch_initial_scan_gate() -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *TASK_WATCH_INITIAL_SCAN_GATE
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap() = Some((entered.clone(), release.clone()));
    (entered, release)
}

#[cfg(test)]
async fn wait_for_task_watch_initial_scan_gate() {
    let gate = TASK_WATCH_INITIAL_SCAN_GATE
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap()
        .take();
    if let Some((entered, release)) = gate {
        entered.notify_one();
        release.notified().await;
    }
}

#[cfg(not(test))]
async fn wait_for_task_watch_initial_scan_gate() {}

#[cfg(not(test))]
fn task_watch_fallback_interval() -> Duration {
    // Bounded fallback reconciliation interval. Native watcher events are
    // immediate; this bounded reconciliation covers missed/coalesced events.
    Duration::from_secs(5)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TaskDirectoryIdentity {
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    created: Option<SystemTime>,
}
impl TaskDirectoryIdentity {
    async fn read(dir: &Path) -> io::Result<Self> {
        let metadata = tokio::fs::metadata(dir).await?;
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;
        Ok(Self {
            #[cfg(unix)]
            device: metadata.dev(),
            #[cfg(unix)]
            inode: metadata.ino(),
            created: metadata.created().ok(),
        })
    }
}
#[derive(Debug, PartialEq, Eq)]
struct TaskLeafStamp {
    name: RuntimeString,
    length: u64,
    modified: Option<SystemTime>,
    created: Option<SystemTime>,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    changed: (i64, i64),
}
#[derive(Debug, PartialEq, Eq)]
struct TaskDirectoryStamp {
    identity: TaskDirectoryIdentity,
    leaves: Vec<TaskLeafStamp>,
}
impl TaskDirectoryStamp {
    async fn read(dir: &Path) -> io::Result<Self> {
        let identity = TaskDirectoryIdentity::read(dir).await?;
        let mut names = read_dir_names(dir).await?;
        names.sort();
        let mut leaves = Vec::new();
        for name in names {
            let text = runtime_naming_terminal(&name);
            if task_watch_ignore(&text, false) && text != SNAPSHOT_LEDGER_FILE {
                continue;
            }
            // Authoritative reads follow symlinks, so fallback must also observe
            // changes to their targets when no native event reaches this list.
            let metadata = tokio::fs::metadata(dir.join(&text)).await?;
            #[cfg(unix)]
            use std::os::unix::fs::MetadataExt;
            leaves.push(TaskLeafStamp {
                name,
                length: metadata.len(),
                modified: metadata.modified().ok(),
                created: metadata.created().ok(),
                #[cfg(unix)]
                inode: metadata.ino(),
                #[cfg(unix)]
                changed: (metadata.ctime(), metadata.ctime_nsec()),
            });
        }
        Ok(Self { identity, leaves })
    }
    fn same_payload(&self, other: &Self) -> bool {
        self.identity == other.identity
            && self
                .leaves
                .iter()
                .filter(|leaf| leaf.name != SNAPSHOT_LEDGER_FILE)
                .eq(other
                    .leaves
                    .iter()
                    .filter(|leaf| leaf.name != SNAPSHOT_LEDGER_FILE))
    }
}
#[derive(Default)]
struct TaskReconciliation {
    stamp: Option<TaskDirectoryStamp>,
    checked: Option<Instant>,
}
type SharedTaskReconciliation = Arc<tokio::sync::Mutex<TaskReconciliation>>;
type TaskReconciliationEntries =
    Mutex<HashMap<(PathBuf, RuntimeString), Weak<tokio::sync::Mutex<TaskReconciliation>>>>;
fn task_reconciliation(dir: &Path, list_id: &RuntimeString) -> SharedTaskReconciliation {
    static ENTRIES: OnceLock<TaskReconciliationEntries> = OnceLock::new();
    let mut entries = ENTRIES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap();
    entries.retain(|_, entry| entry.strong_count() != 0);
    let key = (dir.to_path_buf(), list_id.clone());
    if let Some(entry) = entries.get(&key).and_then(Weak::upgrade) {
        return entry;
    }
    let entry = Arc::new(tokio::sync::Mutex::new(TaskReconciliation::default()));
    entries.insert(key, Arc::downgrade(&entry));
    entry
}
async fn remember_task_reconciliation(
    entry: &mut TaskReconciliation,
    dir: &Path,
    before: TaskDirectoryStamp,
    state: &TaskListState,
) -> io::Result<()> {
    let after = TaskDirectoryStamp::read(dir).await?;
    let ledger = read_snapshot_ledger(dir).await?;
    // The refresh can rewrite its own ledger. Admit that metadata change only
    // when the payload stayed stable and the ledger still names this capture.
    entry.stamp = (before.same_payload(&after)
        && ledger.is_some_and(|ledger| {
            ledger.generation.as_ref() == Some(&state.storage_generation)
                && ledger.revision == state.revision
                && ledger.fingerprint == state.fingerprint
        }))
    .then_some(after);
    entry.checked = Some(Instant::now());
    Ok(())
}
async fn reconcile_observed_tasks(
    list_id: &RuntimeString,
    shared: &SharedTaskReconciliation,
    fallback: bool,
) -> io::Result<()> {
    let mut entry = shared.lock().await;
    if fallback
        && entry
            .checked
            .is_some_and(|checked| checked.elapsed() < task_watch_fallback_interval() / 2)
    {
        return Ok(());
    }
    let dir = dir_of(list_id);
    let before = TaskDirectoryStamp::read(&dir).await;
    entry.checked = Some(Instant::now());
    // Unix ctime changes even when a writer restores mtime. Platforms without
    // that stamp retain content reconciliation, including missed-event repair.
    if cfg!(unix)
        && before
            .as_ref()
            .is_ok_and(|before| entry.stamp.as_ref() == Some(before))
    {
        return Ok(());
    }
    // Failed captures leave the prior stamp intact, so the next event or
    // fallback retries. Only authoritative transactions publish new state.
    let state = refresh_and_publish_task_state(list_id).await?;
    match before {
        Ok(before) => remember_task_reconciliation(&mut entry, &dir, before, &state).await,
        // Metadata is an optimization, never a prerequisite for authoritative
        // recovery, such as replacing a ledger whose symlink target vanished.
        Err(_) => {
            entry.stamp = None;
            Ok(())
        }
    }
}

#[cfg(test)]
static TASK_WATCH_START_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
#[cfg(test)]
static TASK_WATCH_SUPPRESS_EVENTS: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static TASK_WATCH_FAIL_NEXT_START: AtomicBool = AtomicBool::new(false);

async fn start_task_watcher(
    dir: &Path,
    changed: Arc<tokio::sync::Notify>,
) -> notify::Result<DirectoryWatcher> {
    #[cfg(test)]
    TASK_WATCH_START_COUNT.fetch_add(1, AtomicOrdering::SeqCst);
    #[cfg(test)]
    if TASK_WATCH_FAIL_NEXT_START.swap(false, AtomicOrdering::SeqCst) {
        return Err(notify::Error::generic(
            "injected task watcher registration failure",
        ));
    }
    DirectoryWatcher::start(DirectoryWatcherOptions {
        root: dir.to_string_lossy().into_owned(),
        ignore: Some(Arc::new(task_watch_ignore)),
        debounce_ms: Some(50),
        max_watchers: Some(1),
        mode: None,
        on_batch: Box::new(move |_| {
            #[cfg(test)]
            if TASK_WATCH_SUPPRESS_EVENTS.load(AtomicOrdering::SeqCst) {
                return;
            }
            changed.notify_one();
        }),
    })
    .await
}

/// Keep healthy registrations stable. A shared metadata check covers missed
/// events before any lock, JSON decoding or durable snapshot reconciliation.
async fn watch_task_state_changes(
    subscription_id: u64,
    list_id_source: TaskListIdSource,
    cancel: CancellationToken,
    wake: Arc<tokio::sync::Notify>,
) {
    let changed = Arc::new(tokio::sync::Notify::new());
    let mut watched_binding: Option<TaskListBinding> = None;
    let mut watcher: Option<DirectoryWatcher> = None;
    let mut watched_directory = None;
    let mut reconciliation = None;
    loop {
        let Some(binding) = list_id_source() else {
            break;
        };
        let list_id = binding.list_id.clone();
        let dir = dir_of(&list_id);
        if watched_binding.as_ref() != Some(&binding) && ensure_dir(&dir).await.is_ok() {
            if let Ok(replacement) = start_task_watcher(&dir, changed.clone()).await {
                if let Some(prior) = watcher.replace(replacement) {
                    prior.close();
                }
            } else if let Some(prior) = watcher.take() {
                // An old binding's healthy handle cannot stand in for a failed
                // replacement. Leave no handle so fallback retries registration.
                prior.close();
            }
            watched_directory = TaskDirectoryIdentity::read(&dir).await.ok();
            watched_binding = Some(binding.clone());
            let shared = task_reconciliation(&dir, &list_id);
            wait_for_task_watch_initial_scan_gate().await;
            let mut entry = shared.lock().await;
            let before = TaskDirectoryStamp::read(&dir).await;
            // Each new binding retains its ordered initial publication, even
            // when another session already observes the same concrete list.
            if let Ok(state) =
                refresh_and_publish_task_state_to(subscription_id, &list_id, binding.generation)
                    .await
            {
                if let Ok(before) = before {
                    let _ = remember_task_reconciliation(&mut entry, &dir, before, &state).await;
                }
            }
            drop(entry);
            reconciliation = Some(shared);
        }
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            _ = wake.notified() => {},
            _ = changed.notified() => {
                if let Some(shared) = &reconciliation { let _ = reconcile_observed_tasks(&list_id, shared, false).await; }
            }
            _ = tokio::time::sleep(task_watch_fallback_interval()) => {
                if ensure_dir(&dir).await.is_ok() {
                    let identity = TaskDirectoryIdentity::read(&dir).await.ok();
                    if watcher.as_ref().is_none_or(|watcher| !watcher.is_healthy()) || identity != watched_directory {
                        if let Ok(replacement) = start_task_watcher(&dir, changed.clone()).await {
                            if let Some(prior) = watcher.replace(replacement) { prior.close(); }
                            watched_directory = identity;
                        }
                    }
                    if let Some(shared) = &reconciliation { let _ = reconcile_observed_tasks(&list_id, shared, true).await; }
                }
            }
        }
    }
    if let Some(watcher) = watcher {
        watcher.close();
    }
}

fn task_watch_ignore(relative_path: &str, _is_dir: bool) -> bool {
    let Some(name) = relative_path.rsplit('/').next() else {
        return true;
    };
    name != ".task-plans.json"
        && name != ".task-plan-transaction.json"
        && task_file_id(&RuntimeString::from(name)).is_none()
}

fn update_subscriber_watermark(
    entry: &mut TaskStateSubscriberEntry,
    state: &TaskListState,
    binding_generation: u64,
    sequence: u64,
) -> bool {
    let key = TaskListPublishKey {
        task_list_id: state.task_list_id.clone(),
        storage_dir: dir_of(&state.task_list_id),
        binding_generation,
        storage_generation: state.storage_generation.clone(),
    };
    let repeated = entry.last_published.get(&key).is_some_and(|prior| {
        prior.revision == state.revision && prior.fingerprint == state.fingerprint
    });
    if repeated {
        entry
            .last_published
            .get_mut(&key)
            .expect("repeated TodoV2 watermark remains present")
            .last_touched = sequence;
        return false;
    }
    if entry
        .last_published
        .get(&key)
        .is_some_and(|prior| prior.revision > state.revision)
    {
        return false;
    }
    if !entry.last_published.contains_key(&key)
        && entry.last_published.len() >= MAX_TASK_LIST_WATERMARKS
    {
        let oldest = entry
            .last_published
            .iter()
            .min_by_key(|(_, watermark)| watermark.last_touched)
            .map(|(key, _)| key.clone())
            .expect("a full TodoV2 watermark cache has an oldest entry");
        entry.last_published.remove(&oldest);
    }
    entry.last_published.insert(
        key,
        TaskListWatermark {
            revision: state.revision,
            fingerprint: state.fingerprint.clone(),
            last_touched: sequence,
        },
    );
    true
}

fn next_watermark_sequence(hub: &mut TaskChangeHub) -> u64 {
    hub.next_watermark_sequence = hub
        .next_watermark_sequence
        .checked_add(1)
        .expect("TodoV2 task-list watermark sequence overflow");
    hub.next_watermark_sequence
}

#[cfg(test)]
fn seed_task_state(subscription_id: u64, state: &TaskListState, binding_generation: u64) {
    let mut hub = task_change_hub().lock().unwrap();
    let sequence = next_watermark_sequence(&mut hub);
    if let Some(entry) = hub
        .subscribers
        .iter_mut()
        .find(|entry| entry.id == subscription_id)
    {
        let _ = update_subscriber_watermark(entry, state, binding_generation, sequence);
        entry
            .delivery_cutoff
            .fetch_max(sequence, AtomicOrdering::SeqCst);
    }
}

fn seed_matching_task_state(session_id: &RuntimeString, state: &TaskListState) -> u64 {
    let candidates = {
        let hub = task_change_hub().lock().unwrap();
        hub.subscribers
            .iter()
            .map(|entry| (entry.id, entry.list_id_source.clone()))
            .collect::<Vec<_>>()
    };
    let matching = candidates
        .into_iter()
        .filter_map(|(id, source)| {
            source()
                .filter(|binding| {
                    binding.session_id == *session_id && binding.list_id == state.task_list_id
                })
                .map(|binding| (id, binding.generation))
        })
        .collect::<Vec<_>>();
    let mut hub = task_change_hub().lock().unwrap();
    let sequence = next_watermark_sequence(&mut hub);
    for (id, generation) in matching {
        if let Some(entry) = hub.subscribers.iter_mut().find(|entry| entry.id == id) {
            let _ = update_subscriber_watermark(entry, state, generation, sequence);
            entry
                .delivery_cutoff
                .fetch_max(sequence, AtomicOrdering::SeqCst);
        }
    }
    sequence
}

fn publish_task_state_to(
    subscription_id: u64,
    state: &TaskListState,
    binding_generation: u64,
) -> bool {
    let mut hub = task_change_hub().lock().unwrap();
    let sequence = next_watermark_sequence(&mut hub);
    let Some(entry) = hub
        .subscribers
        .iter_mut()
        .find(|entry| entry.id == subscription_id)
    else {
        return false;
    };
    if !update_subscriber_watermark(entry, state, binding_generation, sequence)
        || !entry.active.load(AtomicOrdering::SeqCst)
    {
        return false;
    }
    // Selection, watermark advance, and the nonblocking channel send share
    // one ordering lock. External callbacks still run on their isolated serial
    // dispatcher, so they can never re-enter this critical section.
    wait_at_task_publish_send_gate();
    entry
        .delivery_tx
        .send((state.clone(), binding_generation, sequence))
        .is_ok()
}

fn publish_task_state(state: &TaskListState) -> bool {
    // Resolve each subscriber's current binding before touching its watermark.
    // Publishing list T while a Session is bound to X can therefore never
    // consume T's transition snapshot when that Session later switches to T.
    let candidates = {
        let hub = task_change_hub().lock().unwrap();
        hub.subscribers
            .iter()
            .map(|entry| (entry.id, entry.list_id_source.clone()))
            .collect::<Vec<_>>()
    };
    let matching = candidates
        .into_iter()
        .filter_map(|(id, source)| {
            source()
                .filter(|binding| binding.list_id == state.task_list_id)
                .map(|binding| (id, binding.generation))
        })
        .collect::<Vec<_>>();
    let mut hub = task_change_hub().lock().unwrap();
    let sequence = next_watermark_sequence(&mut hub);
    let mut published = false;
    for (id, binding_generation) in matching {
        let Some(entry) = hub.subscribers.iter_mut().find(|entry| entry.id == id) else {
            continue;
        };
        if update_subscriber_watermark(entry, state, binding_generation, sequence)
            && entry.active.load(AtomicOrdering::SeqCst)
        {
            wait_at_task_publish_send_gate();
            if entry
                .delivery_tx
                .send((state.clone(), binding_generation, sequence))
                .is_ok()
            {
                published = true;
            }
        }
    }
    published
}

fn runtime_naming_terminal(value: &RuntimeString) -> String {
    String::from_utf8(value.to_node_utf8())
        .expect("Node UTF-8 naming projection always produces valid UTF-8")
}

fn dir_of(list_id: &RuntimeString) -> PathBuf {
    PathBuf::from(tasks_dir(&runtime_naming_terminal(list_id)))
}

fn task_path(dir: &Path, id: &RuntimeString) -> PathBuf {
    dir.join(format!(
        "{}.json",
        sanitize_path_component(&runtime_naming_terminal(id))
    ))
}

fn high_water_path(dir: &Path) -> PathBuf {
    dir.join(HIGH_WATER_MARK_FILE)
}

fn snapshot_ledger_path(dir: &Path) -> PathBuf {
    dir.join(SNAPSHOT_LEDGER_FILE)
}

/// Use the list-wide `proper-lockfile` target (`<list>/.lock`),
/// whose exclusion directory is `<list>/.lock.lock`. This serializes Rust list
/// transactions with peer-client operations that take that lock (create/reset/claim);
/// stable scans still defend reads against peer operations that only take a
/// per-task lock or delete without the list lock.
fn task_list_lock_path(dir: &Path) -> PathBuf {
    dir.join(".lock")
}

#[derive(Debug, Clone, PartialEq)]
struct TaskSnapshotLedger {
    schema_version: u32,
    generation: Option<RuntimeString>,
    revision: u64,
    fingerprint: RuntimeString,
}

impl RuntimeEncode for TaskSnapshotLedger {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("schemaVersion", &self.schema_version)?;
        object.field("generation", &self.generation)?;
        object.field("revision", &self.revision)?;
        object.field("fingerprint", &self.fingerprint)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for TaskSnapshotLedger {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                schema_version: object.required("schemaVersion", &[])?,
                generation: object.optional("generation", &[])?,
                revision: object.required("revision", &[])?,
                fingerprint: object.required("fingerprint", &[])?,
            })
        })
    }
}

/// Flush one directory-entry update to stable storage. Atomic leaf replacement
/// is not a crash boundary until the containing directory is durable too.
async fn sync_directory(path: &Path) -> io::Result<()> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        #[cfg(windows)]
        let file = {
            use std::os::windows::fs::OpenOptionsExt;
            use windows_sys::Win32::Storage::FileSystem::{
                FILE_FLAG_BACKUP_SEMANTICS, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
            };
            let mut options = std::fs::OpenOptions::new();
            options
                .read(true)
                .write(true)
                .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
                .custom_flags(FILE_FLAG_BACKUP_SEMANTICS);
            options.open(&path)?
        };
        #[cfg(not(windows))]
        let file = std::fs::File::open(&path)?;
        file.sync_all()
    })
    .await
    .map_err(|error| io::Error::other(format!("TodoV2 directory sync task failed: {error}")))?
}

#[cfg(test)]
static ATOMIC_LEAF_FAILURES: OnceLock<Mutex<HashMap<PathBuf, usize>>> = OnceLock::new();

#[cfg(test)]
fn fail_atomic_leaf_before_replace(target: &Path, times: usize) {
    ATOMIC_LEAF_FAILURES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap()
        .insert(target.to_path_buf(), times);
}

/// Gated with its only user, the unix-only team-create reset case in
/// `engine::session_run_tests`.
#[cfg(all(test, unix))]
pub(crate) fn fail_next_task_list_ledger_write(list_id: &RuntimeString) {
    fail_atomic_leaf_before_replace(&snapshot_ledger_path(&dir_of(list_id)), 1);
}

#[cfg(test)]
fn maybe_fail_atomic_leaf_before_replace(target: &Path) -> io::Result<()> {
    let mut failures = ATOMIC_LEAF_FAILURES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap();
    let Some(remaining) = failures.get_mut(target) else {
        return Ok(());
    };
    *remaining = remaining.saturating_sub(1);
    if *remaining == 0 {
        failures.remove(target);
    }
    Err(io::Error::other(format!(
        "injected TodoV2 atomic leaf failure before replacing {}",
        target.display()
    )))
}

#[cfg(not(test))]
fn maybe_fail_atomic_leaf_before_replace(_target: &Path) -> io::Result<()> {
    Ok(())
}

async fn replace_leaf(temp: &Path, target: &Path) -> io::Result<()> {
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Storage::FileSystem::{
            MoveFileExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
        };

        let temp = temp
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect::<Vec<_>>();
        let target = target
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect::<Vec<_>>();
        let result = unsafe {
            MoveFileExW(
                temp.as_ptr(),
                target.as_ptr(),
                MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
            )
        };
        if result == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
    #[cfg(not(windows))]
    {
        tokio::fs::rename(temp, target).await
    }
}

/// Crash-safe replacement for every authoritative TodoV2 leaf. The temporary
/// file is unique and create-exclusive, fully synced, atomically installed in
/// the same directory, and followed by a parent-directory durability barrier.
async fn write_leaf_atomic(target: &Path, bytes: &[u8]) -> io::Result<()> {
    let parent = target.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("TodoV2 leaf has no parent directory: {}", target.display()),
        )
    })?;
    let target_name = target.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("TodoV2 leaf has no filename: {}", target.display()),
        )
    })?;

    let (temp, mut file) = loop {
        let temp = parent.join(format!(
            ".{}.{}.{}.tmp",
            target_name.to_string_lossy(),
            std::process::id(),
            Uuid::new_v4()
        ));
        let mut options = tokio::fs::OpenOptions::new();
        options.create_new(true).write(true);
        match options.open(&temp).await {
            Ok(file) => break (temp, file),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    };

    let result = async {
        file.write_all(bytes).await?;
        file.sync_all().await?;
        drop(file);
        maybe_fail_atomic_leaf_before_replace(target)?;
        replace_leaf(&temp, target).await?;
        sync_directory(parent).await
    }
    .await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&temp).await;
    }
    result
}

fn parse_json_bytes(bytes: &[u8], description: &str, path: &Path) -> io::Result<RuntimeJson> {
    let text = std::str::from_utf8(bytes).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid UTF-8 in {description} {}: {error}", path.display()),
        )
    })?;
    runtime_json::parse(&RuntimeString::from(text)).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid {description} JSON {}: {error}", path.display()),
        )
    })
}

async fn read_snapshot_ledger(dir: &Path) -> io::Result<Option<TaskSnapshotLedger>> {
    let path = snapshot_ledger_path(dir);
    let bytes = match tokio::fs::read(&path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let value = parse_json_bytes(&bytes, "TodoV2 snapshot ledger", &path)?;
    let ledger = runtime_codec::decode_value::<TaskSnapshotLedger>(&value).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid TodoV2 snapshot ledger: {error}"),
        )
    })?;
    if !matches!(ledger.schema_version, 1 | SNAPSHOT_LEDGER_SCHEMA_VERSION) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "unsupported TodoV2 snapshot ledger schema version {}",
                ledger.schema_version
            ),
        ));
    }
    if ledger.schema_version == SNAPSHOT_LEDGER_SCHEMA_VERSION
        && ledger
            .generation
            .as_ref()
            .is_none_or(|value| value.is_empty())
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "TodoV2 snapshot ledger v2 is missing a non-empty generation",
        ));
    }
    Ok(Some(ledger))
}

async fn write_snapshot_ledger_atomic(dir: &Path, ledger: &TaskSnapshotLedger) -> io::Result<()> {
    let target = snapshot_ledger_path(dir);
    let mut bytes = runtime_codec::encode_bytes(ledger).map_err(io::Error::other)?;
    bytes.push(b'\n');
    write_leaf_atomic(&target, &bytes).await
}

async fn ensure_dir(dir: &Path) -> io::Result<()> {
    tokio::fs::create_dir_all(dir).await
}

/// Parse a leading base-10 integer (optional sign, then digits), ignoring any
/// trailing garbage; `None` when no digits are present.
fn parse_int_prefix(value: &RuntimeString) -> Option<i64> {
    let units = value.as_utf16();
    let mut index = units
        .iter()
        .position(|unit| !is_ecmascript_whitespace(*unit))
        .unwrap_or(units.len());
    let negative = units.get(index) == Some(&u16::from(b'-'));
    if negative || units.get(index) == Some(&u16::from(b'+')) {
        index += 1;
    }
    let start = index;
    while units
        .get(index)
        .is_some_and(|unit| (u16::from(b'0')..=u16::from(b'9')).contains(unit))
    {
        index += 1;
    }
    if index == start {
        return None;
    }
    let mut number = 0_i64;
    for unit in &units[start..index] {
        number = number
            .checked_mul(10)?
            .checked_add(i64::from(*unit - u16::from(b'0')))?;
    }
    Some(if negative { -number } else { number })
}

fn is_ecmascript_whitespace(unit: u16) -> bool {
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

/// Legacy status values some older task files used → current ones.
fn migrate_status(mut data: RuntimeJson) -> RuntimeJson {
    if let Some(object) = data.as_object_mut() {
        let migrated = match object.get("status").and_then(RuntimeJson::as_str) {
            Some(status) if status == "open" => Some("pending"),
            Some(status) if status == "resolved" => Some("completed"),
            Some(status)
                if ["planning", "implementing", "reviewing", "verifying"]
                    .into_iter()
                    .any(|legacy| status == legacy) =>
            {
                Some("in_progress")
            }
            _ => None,
        };
        if let Some(status) = migrated {
            object.insert("status", RuntimeJson::from(status));
        }
    }
    data
}

async fn read_task_file(dir: &Path, id: &RuntimeString) -> io::Result<Option<Task>> {
    let path = task_path(dir, id);
    let bytes = match tokio::fs::read(&path).await {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };
    let data = parse_json_bytes(&bytes, "TodoV2 task", &path)?;
    let task = runtime_codec::decode_value::<Task>(&migrate_status(data)).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid TodoV2 task {}: {error}", path.display()),
        )
    })?;
    if &task.id != id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "TodoV2 task id {} does not match authoritative filename id {}",
                runtime_naming_terminal(&task.id),
                runtime_naming_terminal(id)
            ),
        ));
    }
    Ok(Some(task))
}

async fn write_task_file(dir: &Path, task: &Task) -> io::Result<()> {
    let value = runtime_codec::encode_value(task).map_err(io::Error::other)?;
    let text = runtime_json::stringify_pretty(&value).concat(&RuntimeString::from("\n"));
    write_leaf_atomic(&task_path(dir, &task.id), &text.to_node_utf8()).await
}

async fn read_high_water(dir: &Path) -> io::Result<i64> {
    let path = high_water_path(dir);
    let bytes = match tokio::fs::read(&path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error),
    };
    let text = std::str::from_utf8(&bytes).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "invalid UTF-8 in TodoV2 high-water mark {}: {error}",
                path.display()
            ),
        )
    })?;
    let value = text.trim().parse::<i64>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid TodoV2 high-water mark {}: {error}", path.display()),
        )
    })?;
    if value < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "TodoV2 high-water mark must be non-negative: {}",
                path.display()
            ),
        ));
    }
    Ok(value)
}

async fn bump_high_water(dir: &Path, at_least: i64) -> io::Result<()> {
    let current = read_high_water(dir).await?;
    if at_least > current {
        write_leaf_atomic(&high_water_path(dir), format!("{at_least}\n").as_bytes()).await?;
    }
    Ok(())
}

#[cfg(test)]
static READ_DIR_FAILURES: OnceLock<Mutex<HashMap<PathBuf, usize>>> = OnceLock::new();

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
struct NextEntryFailure {
    successful_entries_before_failure: usize,
    kind: io::ErrorKind,
}

#[cfg(test)]
static NEXT_ENTRY_FAILURES: OnceLock<Mutex<HashMap<PathBuf, NextEntryFailure>>> = OnceLock::new();

#[cfg(test)]
static DISAPPEARING_LISTED_TASKS: OnceLock<Mutex<HashMap<PathBuf, usize>>> = OnceLock::new();

#[cfg(test)]
fn fail_read_dir(dir: &Path, times: usize) {
    READ_DIR_FAILURES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap()
        .insert(dir.to_path_buf(), times);
}

#[cfg(test)]
fn fail_next_entry_after(
    dir: &Path,
    successful_entries_before_failure: usize,
    kind: io::ErrorKind,
) {
    NEXT_ENTRY_FAILURES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap()
        .insert(
            dir.to_path_buf(),
            NextEntryFailure {
                successful_entries_before_failure,
                kind,
            },
        );
}

#[cfg(test)]
fn disappear_listed_task_once(path: &Path) {
    DISAPPEARING_LISTED_TASKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap()
        .insert(path.to_path_buf(), 1);
}

#[cfg(test)]
fn maybe_disappear_listed_task(path: &Path) {
    let mut tasks = DISAPPEARING_LISTED_TASKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap();
    if tasks.remove(path).is_some() {
        std::fs::remove_file(path).unwrap();
    }
}

#[cfg(not(test))]
fn maybe_disappear_listed_task(_path: &Path) {}

#[cfg(test)]
fn maybe_fail_read_dir(dir: &Path) -> io::Result<()> {
    let mut failures = READ_DIR_FAILURES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap();
    let Some(remaining) = failures.get_mut(dir) else {
        return Ok(());
    };
    *remaining = remaining.saturating_sub(1);
    if *remaining == 0 {
        failures.remove(dir);
    }
    Err(io::Error::other(format!(
        "injected TodoV2 read_dir failure for {}",
        dir.display()
    )))
}

#[cfg(test)]
fn maybe_fail_next_entry(dir: &Path) -> io::Result<()> {
    let mut failures = NEXT_ENTRY_FAILURES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap();
    let Some(failure) = failures.get_mut(dir) else {
        return Ok(());
    };
    if failure.successful_entries_before_failure > 0 {
        failure.successful_entries_before_failure -= 1;
        return Ok(());
    }
    let kind = failure.kind;
    failures.remove(dir);
    Err(io::Error::new(
        kind,
        format!("injected TodoV2 next_entry failure for {}", dir.display()),
    ))
}

#[cfg(not(test))]
fn maybe_fail_read_dir(_dir: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(not(test))]
fn maybe_fail_next_entry(_dir: &Path) -> io::Result<()> {
    Ok(())
}

/// Read one directory exactly. A genuinely missing list is empty; every other
/// read/iteration failure is authoritative I/O failure, never a synthetic list.
async fn read_dir_names(dir: &Path) -> io::Result<Vec<RuntimeString>> {
    maybe_fail_read_dir(dir)?;
    let mut rd = match tokio::fs::read_dir(dir).await {
        Ok(rd) => rd,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut names = Vec::new();
    loop {
        maybe_fail_next_entry(dir)?;
        match rd.next_entry().await {
            Ok(Some(entry)) => names.push(crate::fs::node_path::runtime_string_from_native(
                entry.file_name().as_os_str(),
            )),
            Ok(None) => break,
            Err(error) => return Err(error),
        }
    }
    Ok(names)
}

/// Recognize only task leaves, never private plan or snapshot ledgers.
fn task_file_id(name: &RuntimeString) -> Option<RuntimeString> {
    if !name.ends_with(".json") {
        return None;
    }
    let stem = name.slice_utf16(..name.len_utf16().saturating_sub(5));
    let numeric = !stem.is_empty()
        && stem
            .as_utf16()
            .iter()
            .all(|unit| (u16::from(b'0')..=u16::from(b'9')).contains(unit));
    let text = String::from_utf8_lossy(&stem.to_node_utf8()).into_owned();
    let plan_step = text
        .strip_prefix("plan-step-")
        .is_some_and(|uuid| Uuid::parse_str(uuid).is_ok_and(|parsed| parsed.to_string() == uuid));
    (numeric || plan_step).then_some(stem)
}

async fn scan_max_id(dir: &Path) -> io::Result<i64> {
    let names = read_dir_names(dir).await?;
    let mut max = 0i64;
    for name in names {
        if let Some(id) = task_file_id(&name) {
            if let Some(number) = parse_int_prefix(&id) {
                max = std::cmp::max(max, number);
            }
        }
    }
    Ok(max)
}

/// All task ids never reused: max of on-disk files and the high-water mark.
async fn highest_id(dir: &Path) -> io::Result<i64> {
    Ok(std::cmp::max(
        scan_max_id(dir).await?,
        read_high_water(dir).await?,
    ))
}

pub async fn get_task(list_id: &RuntimeString, id: &RuntimeString) -> io::Result<Option<Task>> {
    get_task_bound(list_id, id, None).await
}

pub async fn get_task_bound(
    list_id: &RuntimeString,
    id: &RuntimeString,
    binding: Option<&TaskPlanBinding>,
) -> io::Result<Option<Task>> {
    let dir = dir_of(list_id);
    ensure_dir(&dir).await?;
    let publication_gate = task_publication_gate(&dir);
    let _publication = publication_gate.lock().await;
    let lock = task_list_lock_path(&dir);
    with_lock(
        lock.to_str().expect("UTF-8 task lock"),
        || async {
            plan::recover_transaction(&dir).await?;
            if read_task_file(&dir, id).await?.is_some() {
                plan::check_task_binding(&dir, list_id, id, binding, true).await?;
            }
            read_task_file(&dir, id).await
        },
        LOCK_OPTS,
    )
    .await
}

/// Read one host-visible task, including archived plan entries, under the same
/// publication and file locks used by TaskGet. No plan-wide revision is exposed.
pub async fn get_task_identity_bound(
    list_id: &RuntimeString,
    id: &RuntimeString,
    binding: Option<&TaskPlanBinding>,
) -> io::Result<Option<TaskIdentity>> {
    if task_file_id(&id.concat(&RuntimeString::from(".json"))).as_ref() != Some(id) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Invalid task ID",
        ));
    }
    let dir = dir_of(list_id);
    ensure_dir(&dir).await?;
    let publication_gate = task_publication_gate(&dir);
    let _publication = publication_gate.lock().await;
    let lock = task_list_lock_path(&dir);
    with_lock(
        lock.to_str().expect("UTF-8 task lock"),
        || async {
            plan::recover_transaction(&dir).await?;
            let Some(task) = read_task_file(&dir, id).await? else {
                return Ok(None);
            };
            let token = plan::task_identity_token(&dir, list_id, id, binding, true).await?;
            if !task_is_host_visible(&task) {
                return Ok(None);
            }
            let storage_generation = match read_snapshot_ledger(&dir).await? {
                Some(ledger) if ledger.generation.is_some() => ledger.generation.unwrap(),
                _ => {
                    refresh_task_state_locked(list_id, &dir)
                        .await?
                        .0
                        .storage_generation
                }
            };
            let mut hash = Sha256::new();
            hash.update(b"klaus-task-instance-v1\0");
            for part in [list_id, &storage_generation, &token] {
                let bytes = runtime_codec::encode_bytes(part).map_err(io::Error::other)?;
                hash.update((bytes.len() as u64).to_be_bytes());
                hash.update(bytes);
            }
            let generation =
                RuntimeString::from(format!("sha256:{}", hex_digest(hash.finalize().as_slice())));
            let mut hash = Sha256::new();
            hash.update(b"klaus-task-semantics-v1\0");
            for part in [&task.subject, &task.description] {
                let bytes = runtime_codec::encode_bytes(part).map_err(io::Error::other)?;
                hash.update((bytes.len() as u64).to_be_bytes());
                hash.update(bytes);
            }
            Ok(Some(TaskIdentity {
                task,
                generation,
                fingerprint: RuntimeString::from(format!(
                    "sha256:{}",
                    hex_digest(hash.finalize().as_slice())
                )),
            }))
        },
        LOCK_OPTS,
    )
    .await
}

fn compare_task_ids_numeric(left: &RuntimeString, right: &RuntimeString) -> CmpOrdering {
    fn normalized_digits(value: &RuntimeString) -> &[u16] {
        let digits = value.as_utf16();
        let first_nonzero = digits
            .iter()
            .position(|digit| *digit != u16::from(b'0'))
            .unwrap_or(digits.len().saturating_sub(1));
        &digits[first_nonzero..]
    }

    let left_digits = normalized_digits(left);
    let right_digits = normalized_digits(right);
    left_digits
        .len()
        .cmp(&right_digits.len())
        .then_with(|| left_digits.cmp(right_digits))
        // Numerically equal ids (for example 2 and 02) retain a stable total
        // order independent of directory enumeration order.
        .then_with(|| left.as_utf16().cmp(right.as_utf16()))
}

fn sort_tasks_numeric(tasks: &mut [Task]) {
    tasks.sort_by(|left, right| compare_task_ids_numeric(&left.id, &right.id));
}

async fn scan_tasks_once(dir: &Path) -> io::Result<Vec<Task>> {
    let names = read_dir_names(dir).await?;
    let mut tasks = Vec::new();
    for name in names {
        let Some(id) = task_file_id(&name) else {
            continue;
        };
        maybe_disappear_listed_task(&task_path(dir, &id));
        let task = read_task_file(dir, &id).await?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Interrupted,
                format!(
                    "TodoV2 task {} disappeared during authoritative directory scan",
                    runtime_naming_terminal(&id)
                ),
            )
        })?;
        tasks.push(task);
    }
    sort_tasks_numeric(&mut tasks);
    Ok(tasks)
}

async fn list_tasks_in_dir(dir: &Path) -> io::Result<Vec<Task>> {
    // Non-Rust/legacy writers may not participate in the shared list lock.
    // Require two identical full scans before treating a fingerprint as
    // authoritative; a disappearing leaf or same-name replacement becomes an
    // explicit retryable error and can never advance the durable ledger.
    let first = scan_tasks_once(dir).await?;
    let second = scan_tasks_once(dir).await?;
    if first != second {
        return Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "TodoV2 task directory changed during authoritative snapshot scan",
        ));
    }
    Ok(second)
}

pub async fn list_tasks(list_id: &RuntimeString) -> io::Result<Vec<Task>> {
    let dir = dir_of(list_id);
    ensure_dir(&dir).await?;
    let publication_gate = task_publication_gate(&dir);
    let _publication = publication_gate.lock().await;
    let lock = task_list_lock_path(&dir);
    with_lock(
        lock.to_str().expect("UTF-8 task lock"),
        || async {
            plan::recover_transaction(&dir).await?;
            list_tasks_in_dir(&dir).await
        },
        LOCK_OPTS,
    )
    .await
}

fn task_is_host_visible(task: &Task) -> bool {
    !task
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("_internal"))
        .is_some_and(is_truthy)
}

fn hex_digest(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to a String cannot fail");
    }
    output
}

fn task_fingerprint(tasks: &[Task]) -> io::Result<RuntimeString> {
    let mut hash = Sha256::new();
    hash.update(b"klaus-todo-v2-snapshot-v1\0");
    for task in tasks {
        let encoded = runtime_codec::encode_bytes(task).map_err(io::Error::other)?;
        hash.update((encoded.len() as u64).to_be_bytes());
        hash.update(encoded);
    }
    Ok(RuntimeString::from(format!(
        "sha256:{}",
        hex_digest(hash.finalize().as_slice())
    )))
}

fn task_list_identity_fingerprint(list_id: &RuntimeString) -> String {
    hex_digest(Sha256::digest(list_id.to_node_utf8()).as_slice())
}

async fn refresh_task_state_locked(
    list_id: &RuntimeString,
    dir: &Path,
) -> io::Result<(TaskListState, bool)> {
    plan::recover_transaction(dir).await?;
    refresh_task_state_without_recovery_locked(list_id, dir).await
}

async fn refresh_task_state_without_recovery_locked(
    list_id: &RuntimeString,
    dir: &Path,
) -> io::Result<(TaskListState, bool)> {
    let tasks = list_tasks_in_dir(dir).await?;
    let (plan_id, plan_kind, tasks) = plan::project_current_plan(dir, tasks).await?;
    let mut fingerprint = task_fingerprint(&tasks)?;
    if let Some(plan_id) = &plan_id {
        let mut hash = Sha256::new();
        hash.update(fingerprint.to_node_utf8());
        hash.update(plan_id.to_node_utf8());
        hash.update(plan_kind.to_node_utf8());
        fingerprint = format!("sha256:{}", hex_digest(hash.finalize().as_slice())).into();
    }
    let prior = read_snapshot_ledger(dir).await?;
    let (revision, changed, storage_generation, ledger_needs_write) = match prior {
        Some(ledger) => {
            let changed = ledger.fingerprint != fingerprint;
            let revision = if changed {
                ledger
                    .revision
                    .checked_add(1)
                    .ok_or_else(|| io::Error::other("TodoV2 task-list revision overflow"))?
            } else {
                ledger.revision
            };
            let migrated = ledger.generation.is_none();
            let generation = ledger
                .generation
                .unwrap_or_else(|| RuntimeString::from(Uuid::new_v4().to_string()));
            (revision, changed, generation, changed || migrated)
        }
        None => (
            if tasks.is_empty() { 0 } else { 1 },
            !tasks.is_empty(),
            RuntimeString::from(Uuid::new_v4().to_string()),
            true,
        ),
    };
    if ledger_needs_write {
        write_snapshot_ledger_atomic(
            dir,
            &TaskSnapshotLedger {
                schema_version: SNAPSHOT_LEDGER_SCHEMA_VERSION,
                generation: Some(storage_generation.clone()),
                revision,
                fingerprint: fingerprint.clone(),
            },
        )
        .await?;
    }
    Ok((
        TaskListState {
            task_list_id: list_id.clone(),
            plan_id,
            plan_kind,
            storage_generation,
            revision,
            fingerprint,
            tasks,
        },
        changed,
    ))
}

async fn refresh_and_publish_task_state(list_id: &RuntimeString) -> io::Result<TaskListState> {
    #[cfg(test)]
    TASK_WATCH_RECONCILE_COUNT.fetch_add(1, AtomicOrdering::SeqCst);
    let dir = dir_of(list_id);
    ensure_dir(&dir).await?;
    let publication_gate = task_publication_gate(&dir);
    let _publication = publication_gate.lock().await;
    let lock_path = task_list_lock_path(&dir);
    let state = with_lock(
        lock_path
            .to_str()
            .expect("task-list lock path is a UTF-8 host path"),
        || async {
            let (state, _changed) = refresh_task_state_locked(list_id, &dir).await?;
            Ok(state)
        },
        LOCK_OPTS,
    )
    .await?;
    // Publish only after proper-lockfile release confirms that ownership was
    // never compromised. The process-local publication gate remains held, so
    // an older capture cannot obtain a sequence after a newer query/state.
    publish_task_state(&state);
    Ok(state)
}

async fn refresh_and_publish_task_state_to(
    subscription_id: u64,
    list_id: &RuntimeString,
    binding_generation: u64,
) -> io::Result<TaskListState> {
    let dir = dir_of(list_id);
    ensure_dir(&dir).await?;
    let publication_gate = task_publication_gate(&dir);
    let _publication = publication_gate.lock().await;
    let lock_path = task_list_lock_path(&dir);
    let state = with_lock(
        lock_path
            .to_str()
            .expect("task-list lock path is a UTF-8 host path"),
        || async {
            let (state, _changed) = refresh_task_state_locked(list_id, &dir).await?;
            Ok(state)
        },
        LOCK_OPTS,
    )
    .await?;
    publish_task_state_to(subscription_id, &state, binding_generation);
    // A new subscriber's capture can discover a revision that established
    // subscribers have not seen. Publish it before advancing shared metadata;
    // their pending native events may then safely coalesce with this capture.
    publish_task_state(&state);
    Ok(state)
}

fn encode_task_cursor(state: &TaskListState, offset: usize) -> RuntimeString {
    let fingerprint = runtime_naming_terminal(&state.fingerprint);
    let fingerprint = fingerprint.strip_prefix("sha256:").unwrap_or(&fingerprint);
    RuntimeString::from(format!(
        "todo-v2-v2.{}.{}.{}.{}.{}",
        state.revision,
        offset,
        fingerprint,
        task_list_identity_fingerprint(&state.task_list_id),
        task_list_identity_fingerprint(&state.storage_generation)
    ))
}

fn decode_task_cursor(
    cursor: &RuntimeString,
    state: &TaskListState,
) -> Result<usize, TaskListCursorStatus> {
    let Ok(value) = cursor.try_to_utf8() else {
        return Err(TaskListCursorStatus::Invalid);
    };
    let parts = value.split('.').collect::<Vec<_>>();
    if parts.len() == 5 && parts[0] == "todo-v2-v1" {
        return if parts[4] == task_list_identity_fingerprint(&state.task_list_id) {
            Err(TaskListCursorStatus::Stale)
        } else {
            Err(TaskListCursorStatus::Invalid)
        };
    }
    if parts.len() != 6 || parts[0] != "todo-v2-v2" {
        return Err(TaskListCursorStatus::Invalid);
    }
    let Ok(revision) = parts[1].parse::<u64>() else {
        return Err(TaskListCursorStatus::Invalid);
    };
    let Ok(offset) = parts[2].parse::<usize>() else {
        return Err(TaskListCursorStatus::Invalid);
    };
    let expected_fingerprint = runtime_naming_terminal(&state.fingerprint);
    let expected_fingerprint = expected_fingerprint
        .strip_prefix("sha256:")
        .unwrap_or(&expected_fingerprint);
    if parts[4] != task_list_identity_fingerprint(&state.task_list_id) {
        return Err(TaskListCursorStatus::Invalid);
    }
    if parts[5] != task_list_identity_fingerprint(&state.storage_generation)
        || revision != state.revision
        || parts[3] != expected_fingerprint
    {
        return Err(TaskListCursorStatus::Stale);
    }
    if offset > state.tasks.len() {
        return Err(TaskListCursorStatus::Invalid);
    }
    Ok(offset)
}

/// Read one authoritative page and reconcile direct external file changes into
/// the durable ledger. This correlated snapshot is the query result; change
/// subscribers are driven by the already-armed native watcher/bounded poll, so
/// a query never races an unsolicited baseline ahead of its own response.
pub async fn query_task_list(
    session_id: &RuntimeString,
    list_id: &RuntimeString,
    options: TaskListQueryOptions,
) -> io::Result<TaskListSnapshot> {
    query_task_list_ordered(session_id, list_id, options)
        .await
        .map(|result| result.snapshot)
}

/// Internal query form retaining the process-local publication cutoff used by
/// Session/protocol adapters to supersede not-yet-delivered invalidations.
#[doc(hidden)]
pub async fn query_task_list_ordered(
    session_id: &RuntimeString,
    list_id: &RuntimeString,
    options: TaskListQueryOptions,
) -> io::Result<TaskListQueryResult> {
    let dir = dir_of(list_id);
    ensure_dir(&dir).await?;
    let publication_gate = task_publication_gate(&dir);
    let _publication = publication_gate.lock().await;
    let lock_path = task_list_lock_path(&dir);
    let state = with_lock(
        lock_path
            .to_str()
            .expect("task-list lock path is a UTF-8 host path"),
        || async {
            let (state, _changed) = refresh_task_state_locked(list_id, &dir).await?;
            Ok(state)
        },
        LOCK_OPTS,
    )
    .await?;
    // Establish this correlated response as every matching subscriber's
    // baseline only after lock release succeeds, while the publication gate
    // still prevents an older capture from overtaking it.
    let publication_cutoff = seed_matching_task_state(session_id, &state);
    let (offset, cursor_status) = match options.cursor.as_ref() {
        None => (0, TaskListCursorStatus::Current),
        Some(cursor) => match decode_task_cursor(cursor, &state) {
            Ok(offset) => (offset, TaskListCursorStatus::Current),
            Err(status) => (0, status),
        },
    };
    let limit = options
        .limit
        .unwrap_or(DEFAULT_PAGE_LIMIT)
        .clamp(1, MAX_PAGE_LIMIT) as usize;
    let end = offset.saturating_add(limit).min(state.tasks.len());
    let next_cursor = (end < state.tasks.len()).then(|| encode_task_cursor(&state, end));
    Ok(TaskListQueryResult {
        snapshot: TaskListSnapshot {
            session_id: session_id.clone(),
            task_list_id: list_id.clone(),
            plan_id: state.plan_id.clone(),
            plan_kind: state.plan_kind.clone(),
            generation: state.storage_generation.clone(),
            revision: state.revision,
            fingerprint: state.fingerprint,
            tasks: state.tasks[offset..end].to_vec(),
            next_cursor,
            total: state.tasks.len() as u64,
            cursor_status,
        },
        publication_cutoff,
    })
}

pub async fn create_task(list_id: &RuntimeString, input: CreateTaskInput) -> io::Result<Task> {
    create_task_impl(list_id, input, None, false)
        .await
        .map(|(task, _)| task)
}

pub async fn create_task_bound(
    list_id: &RuntimeString,
    input: CreateTaskInput,
    binding: Option<&TaskPlanBinding>,
) -> io::Result<Task> {
    create_task_impl(list_id, input, binding, true)
        .await
        .map(|(task, _)| task)
}

/// Return the identity receipt captured under the creation lock for hook rollback.
pub async fn create_task_bound_receipt(
    list_id: &RuntimeString,
    input: CreateTaskInput,
    binding: Option<&TaskPlanBinding>,
) -> io::Result<(Task, TaskPlanBinding)> {
    create_task_impl(list_id, input, binding, true).await
}

async fn create_task_impl(
    list_id: &RuntimeString,
    input: CreateTaskInput,
    binding: Option<&TaskPlanBinding>,
    reuse_deleted_max: bool,
) -> io::Result<(Task, TaskPlanBinding)> {
    let dir = dir_of(list_id);
    ensure_dir(&dir).await?;
    let publication_gate = task_publication_gate(&dir);
    let _publication = publication_gate.lock().await;
    let lock_path = task_list_lock_path(&dir);
    let (task, receipt, state) = with_lock(
        lock_path
            .to_str()
            .expect("task-list lock path is a UTF-8 host path"),
        || async {
            let prior_plan = plan::check_append_binding(&dir, list_id, binding).await?;
            let highest = if reuse_deleted_max {
                scan_max_id(&dir).await?
            } else {
                highest_id(&dir).await?
            };
            let next_id = highest
                .checked_add(1)
                .ok_or_else(|| io::Error::other("TodoV2 task id high-water mark overflow"))?;
            let id = RuntimeString::from(next_id.to_string());
            let task = Task {
                id: id.clone(),
                subject: input.subject.clone(),
                description: input.description.clone(),
                active_form: input.active_form.clone(),
                owner: input.owner.clone(),
                // Always pending at creation.
                status: TaskStatus::Pending,
                blocks: Vec::new(),
                blocked_by: Vec::new(),
                metadata: input.metadata.clone(),
            };
            bump_high_water(&dir, parse_int_prefix(&id).unwrap_or(0)).await?;
            plan::append_membership(&dir, list_id, &task, binding, prior_plan).await?;
            let (state, _changed) = refresh_task_state_locked(list_id, &dir).await?;
            let receipt = TaskPlanBinding::capture(list_id);
            Ok((task, receipt, Some(state)))
        },
        LOCK_OPTS,
    )
    .await?;
    if let Some(state) = state {
        publish_task_state(&state);
    }
    Ok((task, receipt))
}

/// Reset a newly-created team's list using the shared list-wide proper-lockfile
/// boundary. Numeric ids already observed remain reserved via
/// the high-water mark, while every visible task leaf is removed and one
/// authoritative empty-state revision is published.
pub(crate) async fn reset_task_list(list_id: &RuntimeString) -> io::Result<()> {
    let dir = dir_of(list_id);
    ensure_dir(&dir).await?;
    let publication_gate = task_publication_gate(&dir);
    let _publication = publication_gate.lock().await;
    let lock_path = task_list_lock_path(&dir);
    let state = with_lock(
        lock_path
            .to_str()
            .expect("task-list lock path is a UTF-8 host path"),
        || async {
            plan::recover_transaction(&dir).await?;
            let highest = scan_max_id(&dir).await?;
            if highest > 0 {
                bump_high_water(&dir, highest).await?;
            }
            let names = read_dir_names(&dir).await?;
            for name in names {
                let Some(id) = task_file_id(&name) else {
                    continue;
                };
                match tokio::fs::remove_file(task_path(&dir, &id)).await {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error),
                }
            }
            sync_directory(&dir).await?;
            let (state, changed) = refresh_task_state_locked(list_id, &dir).await?;
            Ok(changed.then_some(state))
        },
        LOCK_OPTS,
    )
    .await?;
    if let Some(state) = state {
        publish_task_state(&state);
    }
    Ok(())
}

/// Merge `updates` onto a task under its file lock. Returns `None` if it's gone.
pub async fn update_task(
    list_id: &RuntimeString,
    id: &RuntimeString,
    updates: &TaskUpdate,
) -> io::Result<Option<Task>> {
    update_task_bound(list_id, id, updates, None).await
}

pub async fn update_task_bound(
    list_id: &RuntimeString,
    id: &RuntimeString,
    updates: &TaskUpdate,
    binding: Option<&TaskPlanBinding>,
) -> io::Result<Option<Task>> {
    let dir = dir_of(list_id);
    ensure_dir(&dir).await?;
    let publication_gate = task_publication_gate(&dir);
    let _publication = publication_gate.lock().await;
    let list_lock_path = task_list_lock_path(&dir);
    let task_lock_path = task_path(&dir, id);
    let (updated, state) = with_lock(
        list_lock_path
            .to_str()
            .expect("task-list lock path is a UTF-8 host path"),
        || async {
            plan::recover_transaction(&dir).await?;
            plan::check_task_binding(&dir, list_id, id, binding, false).await?;
            // Rust mutations take the list lock first and then the affected
            // task-file lock. The second boundary is the one
            // ordinary updateTask/claimTask writers use; the fixed list ->
            // task order keeps Rust multi-leaf operations deadlock-free.
            let updated = with_lock(
                task_lock_path
                    .to_str()
                    .expect("task lock path is a UTF-8 host path"),
                || async { update_task_unsafe(&dir, id, updates).await },
                LOCK_OPTS,
            )
            .await?;
            if updated.is_some() {
                let (state, changed) = refresh_task_state_locked(list_id, &dir).await?;
                return Ok((updated, changed.then_some(state)));
            }
            Ok((updated, None))
        },
        LOCK_OPTS,
    )
    .await?;
    if let Some(state) = state {
        publish_task_state(&state);
    }
    Ok(updated)
}

/// Lock-free merge — for callers already holding the relevant lock.
async fn update_task_unsafe(
    dir: &Path,
    id: &RuntimeString,
    updates: &TaskUpdate,
) -> io::Result<Option<Task>> {
    let Some(current) = read_task_file(dir, id).await? else {
        return Ok(None);
    };
    let merged = Task {
        id: current.id,
        subject: updates.subject.clone().unwrap_or(current.subject),
        description: updates.description.clone().unwrap_or(current.description),
        active_form: updates.active_form.clone().or(current.active_form),
        owner: updates.owner.clone().or(current.owner),
        status: updates.status.unwrap_or(current.status),
        blocks: updates.blocks.clone().unwrap_or(current.blocks),
        blocked_by: updates.blocked_by.clone().unwrap_or(current.blocked_by),
        metadata: updates.metadata.clone().or(current.metadata),
    };
    write_task_file(dir, &merged).await?;
    Ok(Some(merged))
}

pub async fn delete_task(list_id: &RuntimeString, id: &RuntimeString) -> io::Result<bool> {
    delete_task_bound(list_id, id, None).await
}

pub async fn delete_task_bound(
    list_id: &RuntimeString,
    id: &RuntimeString,
    binding: Option<&TaskPlanBinding>,
) -> io::Result<bool> {
    let dir = dir_of(list_id);
    ensure_dir(&dir).await?;
    let publication_gate = task_publication_gate(&dir);
    let _publication = publication_gate.lock().await;
    let lock_path = task_list_lock_path(&dir);
    let (deleted, state) = with_lock(
        lock_path
            .to_str()
            .expect("task-list lock path is a UTF-8 host path"),
        || async {
            plan::recover_transaction(&dir).await?;
            plan::check_task_binding(&dir, list_id, id, binding, false).await?;
            // Preserve the monotonic allocator used by low-level adapters.
            // Model TaskCreate uses max-present IDs with incarnation fencing.
            if let Some(n) = parse_int_prefix(id) {
                bump_high_water(&dir, n).await?;
            }
            if !plan::delete_task_transaction(&dir, list_id, id).await? {
                return Ok((false, None));
            }
            let (state, _) = refresh_task_state_locked(list_id, &dir).await?;
            Ok((true, Some(state)))
        },
        LOCK_OPTS,
    )
    .await?;
    if let Some(state) = state {
        publish_task_state(&state);
    }
    Ok(deleted)
}

/// Add a bidirectional dependency edge: `from` blocks `to` (idempotent).
pub async fn block_task(
    list_id: &RuntimeString,
    from_id: &RuntimeString,
    to_id: &RuntimeString,
) -> io::Result<()> {
    block_task_bound(list_id, from_id, to_id, None).await
}

pub async fn block_task_bound(
    list_id: &RuntimeString,
    from_id: &RuntimeString,
    to_id: &RuntimeString,
    binding: Option<&TaskPlanBinding>,
) -> io::Result<()> {
    let dir = dir_of(list_id);
    ensure_dir(&dir).await?;
    let publication_gate = task_publication_gate(&dir);
    let _publication = publication_gate.lock().await;
    let lock_path = task_list_lock_path(&dir);
    let state = with_lock(
        lock_path
            .to_str()
            .expect("task-list lock path is a UTF-8 host path"),
        || async {
            plan::recover_transaction(&dir).await?;
            plan::check_task_binding(&dir, list_id, from_id, binding, false).await?;
            plan::check_task_binding(&dir, list_id, to_id, binding, false).await?;
            let from = read_task_file(&dir, from_id).await?;
            let to = read_task_file(&dir, to_id).await?;
            let (Some(from), Some(to)) = (from, to) else {
                return Ok(None);
            };
            if !from.blocks.iter().any(|x| x == to_id) {
                let mut blocks = from.blocks.clone();
                blocks.push(to_id.clone());
                let from_lock_path = task_path(&dir, from_id);
                with_lock(
                    from_lock_path
                        .to_str()
                        .expect("task lock path is a UTF-8 host path"),
                    || async {
                        update_task_unsafe(
                            &dir,
                            from_id,
                            &TaskUpdate {
                                blocks: Some(blocks),
                                ..TaskUpdate::default()
                            },
                        )
                        .await
                    },
                    LOCK_OPTS,
                )
                .await?;
            }
            if !to.blocked_by.iter().any(|x| x == from_id) {
                let mut blocked_by = to.blocked_by.clone();
                blocked_by.push(from_id.clone());
                let to_lock_path = task_path(&dir, to_id);
                with_lock(
                    to_lock_path
                        .to_str()
                        .expect("task lock path is a UTF-8 host path"),
                    || async {
                        update_task_unsafe(
                            &dir,
                            to_id,
                            &TaskUpdate {
                                blocked_by: Some(blocked_by),
                                ..TaskUpdate::default()
                            },
                        )
                        .await
                    },
                    LOCK_OPTS,
                )
                .await?;
            }
            let (state, changed) = refresh_task_state_locked(list_id, &dir).await?;
            Ok(changed.then_some(state))
        },
        LOCK_OPTS,
    )
    .await?;
    if let Some(state) = state {
        publish_task_state(&state);
    }
    Ok(())
}

/// After the first tool round, remind unplanned work to assess whether a plan
/// is needed. Existing plans get a live progress snapshot on longer runs.
/// The query loop supplies a compaction-safe count scoped to the current run.
const REMINDER_TURN_THRESHOLD: usize = 10;

/// Loose truthiness for a metadata flag (`false`, `0`, `""`, `null` are falsy).
fn is_truthy(v: &RuntimeJson) -> bool {
    match v {
        RuntimeJson::Null => false,
        RuntimeJson::Bool(b) => *b,
        RuntimeJson::Number(n) => n.as_f64() != 0.0,
        RuntimeJson::String(s) => !s.is_empty(),
        RuntimeJson::Array(_) | RuntimeJson::Object(_) => true,
    }
}

pub struct TaskReminderProvider;

impl TaskReminderProvider {
    async fn provide(
        &self,
        ctx: &ReminderContext,
        completed_tool_rounds: usize,
    ) -> io::Result<Option<RuntimeString>> {
        if completed_tool_rounds == 0 {
            return Ok(None);
        }
        let list_id = match ctx.task_list_id.as_ref() {
            Some(id) => id.clone(),
            None => get_task_list_id(None, None),
        };
        let visible: Vec<Task> = list_plan_tasks(&list_id, ctx.task_plan.as_ref())
            .await?
            .into_iter()
            .filter(|t| {
                !t.metadata
                    .as_ref()
                    .and_then(|m| m.get("_internal"))
                    .is_some_and(is_truthy)
            })
            .collect();
        if visible.is_empty() {
            return Ok(Some(RuntimeString::from(
                "No progress checklist is currently tracked for this work. Use TaskWrite if a checklist would help carry out the current request; simple answers and straightforward actions do not require one.",
            )));
        }
        if completed_tool_rounds < REMINDER_TURN_THRESHOLD {
            return Ok(None);
        }
        let open: Vec<Task> = visible
            .into_iter()
            .filter(|task| task.status != TaskStatus::Completed)
            .collect();
        if open.is_empty() {
            return Ok(None);
        }
        let lines: Vec<RuntimeString> = open
            .iter()
            .map(|t| {
                RuntimeString::from("#")
                    .concat(&t.id)
                    .concat(&RuntimeString::from(" "))
                    .concat(&RuntimeString::from(t.status.as_str()))
                    .concat(&RuntimeString::from(": "))
                    .concat(&t.subject)
            })
            .collect();
        Ok(Some(
            RuntimeString::from(format!(
                "{} task(s) still open in your task list:\n",
                open.len()
            ))
            .concat(&join_runtime_strings(&lines, "\n"))
            .concat(&RuntimeString::from(
                "\nUpdate them with TaskUpdate as you go.",
            )),
        ))
    }
}

#[async_trait]
impl ReminderProvider for TaskReminderProvider {
    async fn reminder(&self, ctx: &ReminderContext) -> Option<RuntimeString> {
        let assistant_turns = ctx
            .messages
            .iter()
            .filter(|message| matches!(message, Message::Assistant(_)))
            .count();
        self.provide(ctx, assistant_turns).await.unwrap_or(None)
    }

    fn request_refresh(&self) -> Option<RequestReminder> {
        Some(RequestReminder {
            name: "task-plan-reminder",
            required_tool: "TaskWrite",
        })
    }

    async fn reminder_for_request(
        &self,
        ctx: &ReminderContext,
        completed_tool_rounds: u32,
    ) -> Option<RuntimeString> {
        self.provide(ctx, completed_tool_rounds as usize)
            .await
            .unwrap_or(None)
    }
}

fn join_runtime_strings(values: &[RuntimeString], separator: &str) -> RuntimeString {
    let Some(mut output) = values.first().cloned() else {
        return RuntimeString::default();
    };
    let separator = RuntimeString::from(separator);
    for value in &values[1..] {
        output = output.concat(&separator).concat(value);
    }
    output
}

/// Build a task reminder provider; progress is supplied by its originating run.
pub fn create_task_reminder_provider() -> TaskReminderProvider {
    TaskReminderProvider
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::message::AssistantMessage;
    use std::collections::HashSet;
    use std::process::{Command, Stdio};
    use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
    use std::sync::MutexGuard;
    use std::time::Duration;

    fn rs(value: &str) -> RuntimeString {
        RuntimeString::from(value)
    }

    fn list() -> RuntimeString {
        rs("tl")
    }

    // Config-home overrides are process-global; serialize the tests that use them.
    use crate::test_support::CONFIG_ENV_LOCK as ENV_LOCK;

    /// Points the config home at a fresh temp dir for the duration of one test.
    struct TestHome {
        dir: PathBuf,
        _guard: MutexGuard<'static, ()>,
    }

    impl TestHome {
        fn new() -> Self {
            let guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            let mut dir = std::env::temp_dir();
            dir.push(format!(
                "ae-tasks-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir_all(&dir).unwrap();
            crate::runtime_process::set_process_environment_var("KLAUS_CONFIG_DIR", &dir);
            TestHome { dir, _guard: guard }
        }
    }

    impl Drop for TestHome {
        fn drop(&mut self) {
            crate::runtime_process::remove_process_environment_var("KLAUS_CONFIG_DIR");
            std::fs::remove_dir_all(&self.dir).ok();
        }
    }

    struct WatchFallbackOverrideGuard;

    impl WatchFallbackOverrideGuard {
        fn new(milliseconds: u64) -> Self {
            TASK_WATCH_FALLBACK_OVERRIDE_MS.store(milliseconds, AtomicOrdering::SeqCst);
            Self
        }
    }

    impl Drop for WatchFallbackOverrideGuard {
        fn drop(&mut self) {
            TASK_WATCH_FALLBACK_OVERRIDE_MS.store(0, AtomicOrdering::SeqCst);
        }
    }

    fn input(subject: &str, description: &str) -> CreateTaskInput {
        CreateTaskInput {
            subject: rs(subject),
            description: rs(description),
            ..CreateTaskInput::default()
        }
    }

    /// The Node interop oracle, or `None` on a checkout whose npm dependencies were never
    /// installed.
    ///
    /// This case compares the Rust lock against `proper-lockfile` running in a real Node
    /// process, so it needs BOTH the module and a Node interpreter. Panicking instead
    /// failed the test with a spawn or missing-module message that says nothing about the
    /// lock; the reason is printed so an oracle that disappears from a machine that should
    /// have one stays visible.
    fn proper_lockfile_module() -> Option<PathBuf> {
        if !crate::testing::node_interpreter_present() {
            println!(
                "skipping the Node lock-interop check: no `node` on PATH (set KLAUS_NODE, \
                 or install Node.js)"
            );
            return None;
        }
        if let Some(path) = std::env::var_os("KLAUS_PROPER_LOCKFILE") {
            return Some(PathBuf::from(path));
        }
        let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let workspace = manifest
            .ancestors()
            .nth(3)
            .expect("core crate must be nested below the workspace directory");
        [
            // Submodule layout: the engine is mounted at <app>/engine/<engine>, so
            // `workspace` here IS the mount directory and the app's node_modules
            // sits one level above it. The older two-levels-up guess resolved
            // outside the app repo entirely, and the candidate before it pointed
            // into a client tree that no longer exists — between them this oracle
            // could not find the module in a normal checkout.
            workspace.join("../node_modules/proper-lockfile"),
            workspace.join("../../node_modules/proper-lockfile"),
            manifest.join("node_modules/proper-lockfile"),
        ]
        .into_iter()
        .find(|path| path.join("package.json").is_file())
        .or_else(|| {
            println!(
                "skipping the Node lock-interop check: proper-lockfile is not installed here \
                 (set KLAUS_PROPER_LOCKFILE, or run the app's npm install); workspace {}",
                workspace.display()
            );
            None
        })
    }

    fn task(id: &str, subject: &str) -> Task {
        Task {
            id: rs(id),
            subject: rs(subject),
            description: rs("description"),
            active_form: None,
            owner: None,
            status: TaskStatus::Pending,
            blocks: Vec::new(),
            blocked_by: Vec::new(),
            metadata: None,
        }
    }

    /// Unit tests that exercise the process-local publication contract do not
    /// need a directory worker. A `None` identity lets the worker terminate
    /// immediately while the subscription remains registered until explicitly
    /// closed, keeping baseline watcher snapshots out of mutation assertions.
    fn subscribe_test_events(
        list_id: RuntimeString,
        subscriber: TaskListStateSubscriber,
    ) -> Arc<TaskListSubscription> {
        subscribe_task_states(
            Arc::new(move || {
                Some(TaskListBinding {
                    session_id: rs("session"),
                    list_id: list_id.clone(),
                    generation: 0,
                })
            }),
            subscriber,
        )
    }

    async fn wait_for_state_count(states: &Arc<Mutex<Vec<TaskListState>>>, count: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while states.lock().unwrap().len() < count {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("timed out waiting for TodoV2 state delivery");
    }

    async fn wait_for_revision_count(revisions: &Arc<Mutex<Vec<u64>>>, count: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while revisions.lock().unwrap().len() < count {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("timed out waiting for TodoV2 revision delivery");
    }

    #[tokio::test]
    async fn creates_always_pending_gets_and_lists_tasks() {
        let _home = TestHome::new();
        let list = list();
        let t = create_task(&list, input("do a thing", "details"))
            .await
            .unwrap();
        assert_eq!(t.id, "1");
        assert_eq!(t.status, TaskStatus::Pending);
        assert_eq!(t.owner, None);
        assert_eq!(
            get_task(&list, &rs("1")).await.unwrap().unwrap().subject,
            "do a thing"
        );
        create_task(&list, input("second", "d2")).await.unwrap();
        let mut ids: Vec<RuntimeString> = list_tasks(&list)
            .await
            .unwrap()
            .into_iter()
            .map(|x| x.id)
            .collect();
        ids.sort();
        assert_eq!(ids, vec![rs("1"), rs("2")]);
    }

    #[tokio::test]
    async fn never_reuses_an_id_across_delete_monotonic_high_water_mark() {
        let _home = TestHome::new();
        let list = list();
        create_task(&list, input("a", "d")).await.unwrap(); // #1
        create_task(&list, input("b", "d")).await.unwrap(); // #2
        assert!(delete_task(&list, &rs("1")).await.unwrap());
        assert!(delete_task(&list, &rs("2")).await.unwrap());
        let next = create_task(&list, input("c", "d")).await.unwrap();
        assert_eq!(next.id, "3"); // not 1
    }

    #[tokio::test]
    async fn cascades_a_delete_strips_the_id_from_other_tasks_dependency_edges() {
        let _home = TestHome::new();
        let list = list();
        create_task(&list, input("a", "d")).await.unwrap(); // #1
        create_task(&list, input("b", "d")).await.unwrap(); // #2
        block_task(&list, &rs("1"), &rs("2")).await.unwrap(); // #1 blocks #2
        assert_eq!(
            get_task(&list, &rs("2")).await.unwrap().unwrap().blocked_by,
            vec![rs("1")]
        );
        assert!(delete_task(&list, &rs("1")).await.unwrap());
        assert_eq!(
            get_task(&list, &rs("2")).await.unwrap().unwrap().blocked_by,
            Vec::<RuntimeString>::new()
        );
    }

    #[tokio::test]
    async fn delete_reports_false_when_a_pre_read_task_disappears_before_unlink() {
        let _home = TestHome::new();
        let list_id = rs("delete-pre-read-race");
        let created = create_task(&list_id, input("before", "d")).await.unwrap();
        let pre_read = get_task(&list_id, &created.id).await.unwrap();
        assert!(pre_read.is_some(), "TaskUpdate pre-read observed the task");
        let dir = dir_of(&list_id);
        let ledger_before = read_snapshot_ledger(&dir).await.unwrap().unwrap();

        // A CC/other-process delete wins after the tool pre-read but before
        // Rust's list-locked unlink.
        std::fs::remove_file(task_path(&dir, &created.id)).unwrap();
        assert!(!delete_task(&list_id, &created.id).await.unwrap());
        assert_eq!(
            read_snapshot_ledger(&dir).await.unwrap().unwrap(),
            ledger_before,
            "a false delete must not publish/reconcile a synthetic success"
        );
        assert_eq!(read_high_water(&dir).await.unwrap(), 1);
        let next = create_task(&list_id, input("after", "d")).await.unwrap();
        assert_eq!(next.id, "2", "CC high-water-before-unlink remains intact");
    }

    #[tokio::test]
    async fn block_task_adds_a_bidirectional_idempotent_edge() {
        let _home = TestHome::new();
        let list = list();
        create_task(&list, input("a", "d")).await.unwrap();
        create_task(&list, input("b", "d")).await.unwrap();
        block_task(&list, &rs("1"), &rs("2")).await.unwrap();
        block_task(&list, &rs("1"), &rs("2")).await.unwrap(); // idempotent
        assert_eq!(
            get_task(&list, &rs("1")).await.unwrap().unwrap().blocks,
            vec![rs("2")]
        );
        assert_eq!(
            get_task(&list, &rs("2")).await.unwrap().unwrap().blocked_by,
            vec![rs("1")]
        );
    }

    #[tokio::test]
    async fn update_task_merges_fields_and_returns_none_for_a_missing_task() {
        let _home = TestHome::new();
        let list = list();
        create_task(&list, input("a", "d")).await.unwrap();
        let u = update_task(
            &list,
            &rs("1"),
            &TaskUpdate {
                status: Some(TaskStatus::InProgress),
                owner: Some(rs("alice")),
                ..TaskUpdate::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(u.as_ref().unwrap().status, TaskStatus::InProgress);
        assert_eq!(u.as_ref().unwrap().owner.as_ref(), Some(&rs("alice")));
        assert!(update_task(
            &list,
            &rs("999"),
            &TaskUpdate {
                status: Some(TaskStatus::Completed),
                ..TaskUpdate::default()
            },
        )
        .await
        .unwrap()
        .is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rust_update_serializes_with_a_real_claude_code_task_file_lock() {
        const NODE_CC_UPDATE: &str = r#"
const fs = require('fs');
const lockfile = require(process.argv[1]);
const taskPath = process.argv[2];
const readyPath = process.argv[3];
const proceedPath = process.argv[4];
(async () => {
  const release = await lockfile.lock(taskPath, {
    stale: 10000,
    update: 5000,
    retries: { retries: 200, factor: 1, minTimeout: 10, maxTimeout: 10 },
  });
  const task = JSON.parse(fs.readFileSync(taskPath, 'utf8'));
  fs.writeFileSync(readyPath, 'LOCKED');
  while (!fs.existsSync(proceedPath)) {
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  task.description = 'written by Claude Code';
  fs.writeFileSync(taskPath, JSON.stringify(task, null, 2));
  await release();
})().catch((error) => {
  console.error(error && (error.stack || error));
  process.exitCode = 1;
});
"#;

        let home = TestHome::new();
        let list_id = get_task_list_id(Some(&rs("standalone-session")), Some(&rs("cc-team")));
        assert_eq!(list_id, "cc-team");
        assert_eq!(dir_of(&list_id), PathBuf::from(tasks_dir("cc-team")));
        let task = create_task(&list_id, input("original subject", "original description"))
            .await
            .unwrap();
        let path = task_path(&dir_of(&list_id), &task.id);
        let ready = home.dir.join("cc-update-ready");
        let proceed = home.dir.join("cc-update-proceed");
        let Some(module) = proper_lockfile_module() else {
            return;
        };
        let node = std::env::var_os("KLAUS_NODE").unwrap_or_else(|| "node".into());
        let child = Command::new(node)
            .arg("-e")
            .arg(NODE_CC_UPDATE)
            .arg(module)
            .arg(&path)
            .arg(&ready)
            .arg(&proceed)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("launch real proper-lockfile Claude Code update contender");

        tokio::time::timeout(Duration::from_secs(5), async {
            while !ready.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("Claude Code contender did not acquire the task lock");

        let rust_list = list_id.clone();
        let rust_id = task.id.clone();
        let rust_update = tokio::spawn(async move {
            update_task(
                &rust_list,
                &rust_id,
                &TaskUpdate {
                    subject: Some(rs("written by Rust")),
                    ..TaskUpdate::default()
                },
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(150)).await;
        let rust_completed_while_cc_held_lock = rust_update.is_finished();

        std::fs::write(&proceed, b"GO").unwrap();
        let output = tokio::task::spawn_blocking(move || child.wait_with_output())
            .await
            .unwrap()
            .unwrap();
        assert!(
            output.status.success(),
            "Claude Code contender failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let rust_result = rust_update.await.unwrap().unwrap().unwrap();

        assert!(
            !rust_completed_while_cc_held_lock,
            "Rust update completed while Claude Code still owned <task>.json.lock"
        );
        assert_eq!(rust_result.subject, "written by Rust");
        assert_eq!(rust_result.description, "written by Claude Code");
        let persisted = get_task(&list_id, &task.id).await.unwrap().unwrap();
        assert_eq!(persisted.subject, "written by Rust");
        assert_eq!(persisted.description, "written by Claude Code");
    }

    #[tokio::test]
    async fn migrates_legacy_status_values_on_read() {
        let _home = TestHome::new();
        let list = list();
        let dir = PathBuf::from(tasks_dir("tl"));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("7.json"),
            br#"{"id":"7","subject":"s","description":"d","status":"open","blocks":[],"blockedBy":[]}"#,
        )
        .unwrap();
        assert_eq!(
            get_task(&list, &rs("7")).await.unwrap().unwrap().status,
            TaskStatus::Pending
        );
    }

    #[tokio::test]
    async fn preserves_lone_surrogates_and_ecmascript_metadata_property_order() {
        let _home = TestHome::new();
        let list = list();
        let subject = RuntimeString::from_utf16(vec![u16::from(b's'), 0xd800, u16::from(b'x')]);
        let mut metadata = RuntimeObject::new();
        metadata.insert("z", RuntimeJson::from(1));
        metadata.insert("2", RuntimeJson::from(2));
        metadata.insert("a", RuntimeJson::from(3));
        metadata.insert("1", RuntimeJson::from(4));

        let created = create_task(
            &list,
            CreateTaskInput {
                subject: subject.clone(),
                description: rs("exact runtime persistence"),
                metadata: Some(metadata),
                ..CreateTaskInput::default()
            },
        )
        .await
        .unwrap();
        let loaded = get_task(&list, &created.id).await.unwrap().unwrap();

        assert_eq!(loaded.subject.as_utf16(), subject.as_utf16());
        assert_eq!(
            loaded.metadata.unwrap().keys().collect::<Vec<_>>(),
            vec![rs("1"), rs("2"), rs("z"), rs("a")]
        );
    }

    #[tokio::test]
    async fn serializes_concurrent_creates_distinct_ids_none_lost() {
        let _home = TestHome::new();
        let list = list();
        let (a, b, c) = tokio::join!(
            create_task(&list, input("1", "d")),
            create_task(&list, input("2", "d")),
            create_task(&list, input("3", "d")),
        );
        let ids: HashSet<RuntimeString> = [a.unwrap().id, b.unwrap().id, c.unwrap().id]
            .into_iter()
            .collect();
        assert_eq!(ids.len(), 3);
        assert_eq!(list_tasks(&list).await.unwrap().len(), 3);
    }

    #[tokio::test]
    async fn compromised_list_lock_release_never_publishes_the_captured_state() {
        let _home = TestHome::new();
        let list_id = rs("compromised-release-no-publication");
        let baseline = query_task_list(&rs("session"), &list_id, Default::default())
            .await
            .unwrap();
        assert_eq!(baseline.revision, 0);

        // Hold the watcher's first scan so the only possible revision-one
        // publication is the mutation path under test.
        let (scan_entered, scan_release) = install_task_watch_initial_scan_gate();
        let observed = Arc::new(Mutex::new(Vec::<TaskListState>::new()));
        let observed_for_callback = observed.clone();
        let subscription = subscribe_test_events(
            list_id.clone(),
            Arc::new(move |state, _, _| observed_for_callback.lock().unwrap().push(state)),
        );
        scan_entered.notified().await;

        let dir = dir_of(&list_id);
        let lock_directory = PathBuf::from(format!(
            "{}.lock",
            task_list_lock_path(&dir).to_string_lossy()
        ));
        let (release_reached, release_resume) =
            crate::fs::lockfile::install_release_validation_pause(
                lock_directory.to_string_lossy().into_owned(),
            );
        let reached = release_reached.notified();
        let create_list = list_id.clone();
        let create = tokio::spawn(async move {
            create_task(&create_list, input("captured but compromised", "d")).await
        });
        reached.await;
        std::fs::remove_dir(&lock_directory).unwrap();
        std::fs::create_dir(&lock_directory).unwrap();
        release_resume.notify_one();

        let error = create
            .await
            .unwrap()
            .expect_err("a replaced list lock must fail the business operation");
        assert!(error.to_string().contains("ownership was compromised"));
        crate::fs::lockfile::clear_release_validation_pause();
        assert!(
            observed.lock().unwrap().is_empty(),
            "a captured state must not be delivered before lock release succeeds"
        );
        {
            let hub = task_change_hub().lock().unwrap();
            let entry = hub
                .subscribers
                .iter()
                .find(|entry| entry.id == subscription.id)
                .unwrap();
            assert!(
                entry.last_published.is_empty(),
                "the compromised mutation must not advance the subscriber watermark"
            );
        }

        subscription.close();
        scan_release.notify_one();
        subscription.close_and_wait().await.unwrap();
    }

    #[test]
    fn task_list_identity_uses_cc_team_name_then_session_then_default_with_env_override() {
        let _home = TestHome::new();
        crate::runtime_process::set_process_environment_var("KLAUS_TASK_LIST_ID", "forced");
        crate::runtime_process::set_process_environment_var(
            "CLAUDE_CODE_TASK_LIST_ID",
            "cc-forced",
        );
        assert_eq!(
            get_task_list_id(Some(&rs("sess")), Some(&rs("alpha"))),
            "forced"
        );
        crate::runtime_process::remove_process_environment_var("KLAUS_TASK_LIST_ID");
        assert_eq!(
            get_task_list_id(Some(&rs("sess")), Some(&rs("alpha"))),
            "cc-forced"
        );
        crate::runtime_process::remove_process_environment_var("CLAUDE_CODE_TASK_LIST_ID");
        assert_eq!(
            get_task_list_id(Some(&rs("sess")), Some(&rs("alpha"))),
            "alpha"
        );
        assert_eq!(
            dir_of(&get_task_list_id(
                Some(&rs("sess")),
                Some(&rs("alpha/beta"))
            )),
            PathBuf::from(tasks_dir("alpha-beta")),
            "CC/Klaus sanitize the raw team name directly; no legacy team: prefix is retained"
        );
        assert_eq!(team_task_list_id(&rs("My_Team Name")), "my-team-name");
        assert_eq!(team_task_list_id(&rs("A🚀B")), "a--b");
        assert_eq!(
            team_task_list_id(&RuntimeString::from_utf16(vec![
                u16::from(b'A'),
                0xd800,
                u16::from(b'Z'),
            ])),
            "a-z"
        );
        assert_eq!(get_task_list_id(Some(&rs("sess")), None), "sess");
        assert_eq!(get_task_list_id(None, None), "tasklist");
    }

    #[tokio::test]
    async fn authoritative_query_cold_starts_empty_and_keeps_session_identity_separate() {
        let _home = TestHome::new();
        let first = query_task_list(&rs("session-a"), &rs("session-a"), Default::default())
            .await
            .unwrap();
        assert_eq!(first.session_id, "session-a");
        assert_eq!(first.task_list_id, "session-a");
        assert_eq!(first.revision, 0);
        assert_eq!(first.total, 0);
        assert!(first.tasks.is_empty());
        assert_eq!(first.next_cursor, None);
        assert_eq!(first.cursor_status, TaskListCursorStatus::Current);

        let second = query_task_list(&rs("session-b"), &rs("session-b"), Default::default())
            .await
            .unwrap();
        assert_eq!(second.session_id, "session-b");
        assert_eq!(second.task_list_id, "session-b");
        assert_eq!(second.revision, 0);
        assert_eq!(second.fingerprint, first.fingerprint);
        assert_ne!(second.generation, first.generation);
        assert!(snapshot_ledger_path(&dir_of(&rs("session-a"))).exists());
        assert!(snapshot_ledger_path(&dir_of(&rs("session-b"))).exists());
    }

    #[tokio::test]
    async fn process_local_publish_watermarks_are_bounded_per_subscriber() {
        let _home = TestHome::new();
        assert!(task_change_hub().lock().unwrap().subscribers.is_empty());
        let subscription = subscribe_test_events(rs("bounded-0"), Arc::new(|_, _, _| {}));
        for index in 0..=MAX_TASK_LIST_WATERMARKS {
            seed_task_state(
                subscription.id,
                &TaskListState {
                    plan_id: None,
                    plan_kind: "tasks".into(),
                    task_list_id: rs("bounded-0"),
                    storage_generation: RuntimeString::from(format!("storage-{index}")),
                    revision: 1,
                    fingerprint: RuntimeString::from(format!("fingerprint-{index}")),
                    tasks: Vec::new(),
                },
                index as u64,
            );
        }
        // Publishing a different list must not consume this subscriber's cache.
        publish_task_state(&TaskListState {
            plan_id: None,
            plan_kind: "tasks".into(),
            task_list_id: rs("unrelated"),
            storage_generation: rs("unrelated-storage"),
            revision: 99,
            fingerprint: rs("unrelated-fingerprint"),
            tasks: Vec::new(),
        });
        {
            let hub = task_change_hub().lock().unwrap();
            let entry = hub
                .subscribers
                .iter()
                .find(|entry| entry.id == subscription.id)
                .unwrap();
            assert_eq!(entry.last_published.len(), MAX_TASK_LIST_WATERMARKS);
        }
        subscription.close_and_wait().await.unwrap();
        assert!(task_change_hub().lock().unwrap().subscribers.is_empty());
    }

    #[tokio::test]
    async fn snapshot_sorts_numeric_ids_and_pages_with_a_revision_bound_cursor() {
        let _home = TestHome::new();
        let list_id = rs("numeric-page");
        let dir = dir_of(&list_id);
        ensure_dir(&dir).await.unwrap();
        // Write out of order to prove readdir order is not part of the contract.
        write_task_file(&dir, &task("10", "ten")).await.unwrap();
        write_task_file(&dir, &task("2", "two")).await.unwrap();
        write_task_file(&dir, &task("1", "one")).await.unwrap();

        let first = query_task_list(
            &rs("session"),
            &list_id,
            TaskListQueryOptions {
                cursor: None,
                limit: Some(2),
            },
        )
        .await
        .unwrap();
        assert_eq!(first.revision, 1);
        assert_eq!(first.total, 3);
        assert_eq!(
            first
                .tasks
                .iter()
                .map(|task| task.id.clone())
                .collect::<Vec<_>>(),
            vec![rs("1"), rs("2")]
        );

        let second = query_task_list(
            &rs("session"),
            &list_id,
            TaskListQueryOptions {
                cursor: first.next_cursor,
                limit: Some(2),
            },
        )
        .await
        .unwrap();
        assert_eq!(second.revision, 1);
        assert_eq!(second.cursor_status, TaskListCursorStatus::Current);
        assert_eq!(second.tasks, vec![task("10", "ten")]);
        assert_eq!(second.next_cursor, None);
    }

    #[tokio::test]
    async fn external_update_advances_persistent_revision_and_stales_old_cursor() {
        let _home = TestHome::new();
        let list_id = rs("external");
        create_task(&list_id, input("one", "d")).await.unwrap();
        create_task(&list_id, input("two", "d")).await.unwrap();
        let first = query_task_list(
            &rs("session"),
            &list_id,
            TaskListQueryOptions {
                cursor: None,
                limit: Some(1),
            },
        )
        .await
        .unwrap();
        assert_eq!(first.revision, 2);
        let cursor = first.next_cursor.unwrap();

        // Direct disk write simulates another process (no in-process notifier).
        let mut externally_changed = get_task(&list_id, &rs("1")).await.unwrap().unwrap();
        externally_changed.subject = rs("changed outside this process");
        write_task_file(&dir_of(&list_id), &externally_changed)
            .await
            .unwrap();

        let refreshed = query_task_list(
            &rs("session"),
            &list_id,
            TaskListQueryOptions {
                cursor: Some(cursor),
                limit: Some(1),
            },
        )
        .await
        .unwrap();
        assert_eq!(refreshed.revision, 3);
        assert_eq!(refreshed.cursor_status, TaskListCursorStatus::Stale);
        assert_eq!(refreshed.tasks[0].subject, "changed outside this process");
        // A cold reader reconstructs from the durable ledger without bumping.
        let cold = query_task_list(&rs("session"), &list_id, Default::default())
            .await
            .unwrap();
        assert_eq!(cold.revision, 3);
        assert_eq!(cold.fingerprint, refreshed.fingerprint);
        assert!(snapshot_ledger_path(&dir_of(&list_id)).exists());
    }

    #[tokio::test]
    async fn canonical_update_from_another_process_is_invalidated_once_by_owned_watcher() {
        let _home = TestHome::new();
        let list_id = rs("cross-process-ledger");
        create_task(&list_id, input("before", "d")).await.unwrap();
        let baseline = query_task_list(&rs("session"), &list_id, Default::default())
            .await
            .unwrap();
        assert_eq!(baseline.revision, 1);

        let observed = Arc::new(Mutex::new(Vec::<TaskListState>::new()));
        let observed_for_callback = observed.clone();
        let expected_list = list_id.clone();
        let subscription = subscribe_test_events(
            list_id.clone(),
            Arc::new(move |state, _, _| {
                if state.task_list_id == expected_list {
                    observed_for_callback.lock().unwrap().push(state);
                }
            }),
        );
        wait_for_state_count(&observed, 1).await;
        assert_eq!(observed.lock().unwrap()[0].revision, 1);
        observed.lock().unwrap().clear();

        // Simulate a different process completing the canonical file + ledger
        // transaction. Its process-local hub is deliberately not invoked here.
        let dir = dir_of(&list_id);
        let mut external = get_task(&list_id, &rs("1")).await.unwrap().unwrap();
        external.subject = rs("after");
        write_task_file(&dir, &external).await.unwrap();
        let (external_state, changed) = refresh_task_state_locked(&list_id, &dir).await.unwrap();
        assert!(changed);
        assert_eq!(external_state.revision, 2);
        wait_for_state_count(&observed, 1).await;

        let first = query_task_list(&rs("session"), &list_id, Default::default())
            .await
            .unwrap();
        assert_eq!(first.revision, 2);
        assert_eq!(first.tasks[0].subject, "after");
        let second = query_task_list(&rs("session"), &list_id, Default::default())
            .await
            .unwrap();
        assert_eq!(second.revision, 2);
        subscription.close_and_wait().await.unwrap();

        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].revision, 2);
        assert_eq!(observed[0].tasks[0].subject, "after");
    }

    #[tokio::test]
    async fn first_scan_delivers_a_mutation_completed_after_subscribe_returned() {
        let _home = TestHome::new();
        let list_id = rs("first-scan-after-subscribe");
        create_task(&list_id, input("before", "d")).await.unwrap();
        let (entered, release) = install_task_watch_initial_scan_gate();
        let observed = Arc::new(Mutex::new(Vec::<TaskListState>::new()));
        let observed_for_callback = observed.clone();
        let subscription = subscribe_test_events(
            list_id.clone(),
            Arc::new(move |state, _, _| observed_for_callback.lock().unwrap().push(state)),
        );
        entered.notified().await;

        let dir = dir_of(&list_id);
        let mut external = get_task(&list_id, &rs("1")).await.unwrap().unwrap();
        external.subject = rs("after subscribe returned");
        write_task_file(&dir, &external).await.unwrap();
        let (external, changed) = refresh_task_state_locked(&list_id, &dir).await.unwrap();
        assert!(changed);
        assert_eq!(external.revision, 2);
        release.notify_one();

        wait_for_state_count(&observed, 1).await;
        subscription.close_and_wait().await.unwrap();
        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].revision, 2);
        assert_eq!(observed[0].tasks[0].subject, "after subscribe returned");
    }

    #[tokio::test]
    async fn query_seed_does_not_swallow_a_later_revision_on_first_scan() {
        let _home = TestHome::new();
        let list_id = rs("query-before-first-scan");
        for index in 0..5 {
            create_task(&list_id, input(&format!("task-{index}"), "d"))
                .await
                .unwrap();
        }
        let (entered, release) = install_task_watch_initial_scan_gate();
        let observed = Arc::new(Mutex::new(Vec::<TaskListState>::new()));
        let observed_for_callback = observed.clone();
        let subscription = subscribe_test_events(
            list_id.clone(),
            Arc::new(move |state, _, _| observed_for_callback.lock().unwrap().push(state)),
        );
        entered.notified().await;
        let baseline = query_task_list(&rs("session"), &list_id, Default::default())
            .await
            .unwrap();
        assert_eq!(baseline.revision, 5);
        assert!(observed.lock().unwrap().is_empty());

        let dir = dir_of(&list_id);
        let mut external = get_task(&list_id, &rs("1")).await.unwrap().unwrap();
        external.subject = rs("revision six");
        write_task_file(&dir, &external).await.unwrap();
        let (external, changed) = refresh_task_state_locked(&list_id, &dir).await.unwrap();
        assert!(changed);
        assert_eq!(external.revision, 6);
        release.notify_one();

        wait_for_state_count(&observed, 1).await;
        subscription.close_and_wait().await.unwrap();
        assert_eq!(observed.lock().unwrap()[0].revision, 6);
    }

    #[tokio::test]
    async fn query_cutoff_drops_already_queued_state_but_keeps_the_next_publication() {
        let _home = TestHome::new();
        let list_id = rs("query-publication-cutoff");
        let callback_gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let entered_tx = Arc::new(Mutex::new(Some(entered_tx)));
        let revisions = Arc::new(Mutex::new(Vec::<u64>::new()));
        let subscription = subscribe_test_events(list_id.clone(), {
            let callback_gate = callback_gate.clone();
            let entered_tx = entered_tx.clone();
            let revisions = revisions.clone();
            Arc::new(move |state, _, _| {
                revisions.lock().unwrap().push(state.revision);
                if state.revision == 0 {
                    if let Some(entered) = entered_tx.lock().unwrap().take() {
                        let _ = entered.send(());
                    }
                    let (released, ready) = &*callback_gate;
                    let mut released = released.lock().unwrap();
                    while !*released {
                        released = ready.wait(released).unwrap();
                    }
                }
            })
        });
        entered_rx.await.expect("baseline callback entered");

        create_task(&list_id, input("superseded", "d"))
            .await
            .unwrap();
        let snapshot = query_task_list(&rs("session"), &list_id, Default::default())
            .await
            .unwrap();
        assert_eq!(snapshot.revision, 1);
        {
            let (released, ready) = &*callback_gate;
            *released.lock().unwrap() = true;
            ready.notify_all();
        }
        create_task(&list_id, input("newer", "d")).await.unwrap();

        wait_for_revision_count(&revisions, 2).await;
        subscription.close_and_wait().await.unwrap();
        assert_eq!(
            *revisions.lock().unwrap(),
            vec![0, 2],
            "the query supersedes queued <=cutoff invalidations but not N+1"
        );
    }

    #[tokio::test]
    async fn one_sessions_query_seed_cannot_consume_another_sessions_baseline() {
        let _home = TestHome::new();
        let list_id = rs("shared-list-distinct-sessions");
        create_task(&list_id, input("shared", "d")).await.unwrap();
        let (entered, release) = install_task_watch_initial_scan_gate();
        let observed = Arc::new(Mutex::new(Vec::<TaskListState>::new()));
        let observed_for_callback = observed.clone();
        let source_list = list_id.clone();
        let subscription = subscribe_task_states(
            Arc::new(move || {
                Some(TaskListBinding {
                    session_id: rs("session-b"),
                    list_id: source_list.clone(),
                    generation: 0,
                })
            }),
            Arc::new(move |state, _, _| observed_for_callback.lock().unwrap().push(state)),
        );
        entered.notified().await;

        let queried = query_task_list(&rs("session-a"), &list_id, Default::default())
            .await
            .unwrap();
        assert_eq!(queried.revision, 1);
        assert!(observed.lock().unwrap().is_empty());
        release.notify_one();

        wait_for_state_count(&observed, 1).await;
        subscription.close_and_wait().await.unwrap();
        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].revision, 1);
        assert_eq!(observed[0].tasks[0].subject, "shared");
    }

    #[tokio::test]
    async fn a_late_subscribers_capture_also_delivers_changes_to_existing_subscribers() {
        let _home = TestHome::new();
        let _fallback = WatchFallbackOverrideGuard::new(10_000);
        struct RestoreEvents;
        impl Drop for RestoreEvents {
            fn drop(&mut self) {
                TASK_WATCH_SUPPRESS_EVENTS.store(false, AtomicOrdering::SeqCst);
            }
        }
        TASK_WATCH_SUPPRESS_EVENTS.store(true, AtomicOrdering::SeqCst);
        let _events = RestoreEvents;
        let list_id = rs("late-shared-observer");
        create_task(&list_id, input("first revision", "d"))
            .await
            .unwrap();
        let first_states = Arc::new(Mutex::new(Vec::new()));
        let first = subscribe_test_events(list_id.clone(), {
            let states = first_states.clone();
            Arc::new(move |state, _, _| states.lock().unwrap().push(state))
        });
        wait_for_state_count(&first_states, 1).await;
        let mut external = get_task(&list_id, &rs("1")).await.unwrap().unwrap();
        external.subject = rs("second revision");
        write_task_file(&dir_of(&list_id), &external).await.unwrap();
        let second_states = Arc::new(Mutex::new(Vec::new()));
        let second = subscribe_test_events(list_id, {
            let states = second_states.clone();
            Arc::new(move |state, _, _| states.lock().unwrap().push(state))
        });
        wait_for_state_count(&second_states, 1).await;
        wait_for_state_count(&first_states, 2).await;
        first.close_and_wait().await.unwrap();
        second.close_and_wait().await.unwrap();
        assert_eq!(first_states.lock().unwrap()[1].revision, 2);
        assert_eq!(
            first_states.lock().unwrap()[1].tasks[0].subject,
            "second revision"
        );
        assert_eq!(second_states.lock().unwrap().len(), 1);
        assert_eq!(second_states.lock().unwrap()[0].revision, 2);
    }

    #[tokio::test]
    async fn failed_rebind_retries_registration_instead_of_retaining_the_old_watch() {
        let _home = TestHome::new();
        let _fallback = WatchFallbackOverrideGuard::new(100);
        let binding = Arc::new(Mutex::new(TaskListBinding {
            session_id: rs("rebinding-session"),
            list_id: rs("first-binding"),
            generation: 0,
        }));
        let observed = Arc::new(Mutex::new(Vec::new()));
        let subscription = subscribe_task_states(
            {
                let binding = binding.clone();
                Arc::new(move || Some(binding.lock().unwrap().clone()))
            },
            {
                let observed = observed.clone();
                Arc::new(move |state, _, _| observed.lock().unwrap().push(state))
            },
        );
        wait_for_state_count(&observed, 1).await;
        TASK_WATCH_START_COUNT.store(0, AtomicOrdering::SeqCst);
        TASK_WATCH_FAIL_NEXT_START.store(true, AtomicOrdering::SeqCst);
        {
            let mut binding = binding.lock().unwrap();
            binding.list_id = rs("replacement-binding");
            binding.generation = 1;
        }
        subscription.identity_changed(1);
        wait_for_state_count(&observed, 2).await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while TASK_WATCH_START_COUNT.load(AtomicOrdering::SeqCst) < 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("failed rebind must retry the new registration on fallback");
        subscription.close_and_wait().await.unwrap();
        assert_eq!(TASK_WATCH_START_COUNT.load(AtomicOrdering::SeqCst), 2);
        assert_eq!(
            observed.lock().unwrap()[1].task_list_id,
            "replacement-binding"
        );
    }

    #[tokio::test]
    async fn idle_subscribers_keep_healthy_watchers_and_skip_full_reconciliation() {
        let _home = TestHome::new();
        let list_id = rs("idle-shared-observation");
        create_task(&list_id, input("stable", "d")).await.unwrap();
        let observed = Arc::new(Mutex::new(Vec::new()));
        let observed_second = Arc::new(Mutex::new(Vec::new()));
        let first = subscribe_test_events(list_id.clone(), {
            let observed = observed.clone();
            Arc::new(move |state, _, _| observed.lock().unwrap().push(state))
        });
        let second = subscribe_test_events(list_id, {
            let observed = observed_second.clone();
            Arc::new(move |state, _, _| observed.lock().unwrap().push(state))
        });
        wait_for_state_count(&observed, 1).await;
        wait_for_state_count(&observed_second, 1).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        TASK_WATCH_START_COUNT.store(0, AtomicOrdering::SeqCst);
        TASK_WATCH_RECONCILE_COUNT.store(0, AtomicOrdering::SeqCst);
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(TASK_WATCH_START_COUNT.load(AtomicOrdering::SeqCst), 0);
        #[cfg(unix)]
        assert_eq!(TASK_WATCH_RECONCILE_COUNT.load(AtomicOrdering::SeqCst), 0);
        first.close_and_wait().await.unwrap();
        second.close_and_wait().await.unwrap();
    }

    #[tokio::test]
    async fn fallback_metadata_detects_a_file_update_when_native_events_are_missing() {
        let _home = TestHome::new();
        let _fallback = WatchFallbackOverrideGuard::new(200);
        struct RestoreEvents;
        impl Drop for RestoreEvents {
            fn drop(&mut self) {
                TASK_WATCH_SUPPRESS_EVENTS.store(false, AtomicOrdering::SeqCst);
            }
        }
        TASK_WATCH_SUPPRESS_EVENTS.store(true, AtomicOrdering::SeqCst);
        let _events = RestoreEvents;
        let list_id = rs("missed-native-event");
        create_task(&list_id, input("before", "d")).await.unwrap();
        let observed = Arc::new(Mutex::new(Vec::new()));
        let subscription = subscribe_test_events(list_id.clone(), {
            let observed = observed.clone();
            Arc::new(move |state, _, _| observed.lock().unwrap().push(state))
        });
        wait_for_state_count(&observed, 1).await;
        observed.lock().unwrap().clear();
        let mut external = task("1", "after a missing event");
        external.description = rs("changed directly on disk");
        write_task_file(&dir_of(&list_id), &external).await.unwrap();
        wait_for_state_count(&observed, 1).await;
        assert_eq!(
            observed.lock().unwrap()[0].tasks[0].subject,
            "after a missing event"
        );
        subscription.close_and_wait().await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn fallback_detects_a_symlink_target_update_without_native_events() {
        let home = TestHome::new();
        let _fallback = WatchFallbackOverrideGuard::new(200);
        struct RestoreEvents;
        impl Drop for RestoreEvents {
            fn drop(&mut self) {
                TASK_WATCH_SUPPRESS_EVENTS.store(false, AtomicOrdering::SeqCst);
            }
        }
        TASK_WATCH_SUPPRESS_EVENTS.store(true, AtomicOrdering::SeqCst);
        let _events = RestoreEvents;
        let list_id = rs("linked-missed-native-event");
        create_task(&list_id, input("before", "d")).await.unwrap();
        let linked_path = task_path(&dir_of(&list_id), &rs("1"));
        let target = home.dir.join("external-task.json");
        std::fs::rename(&linked_path, &target).unwrap();
        std::os::unix::fs::symlink(&target, &linked_path).unwrap();
        let observed = Arc::new(Mutex::new(Vec::new()));
        let subscription = subscribe_test_events(list_id, {
            let observed = observed.clone();
            Arc::new(move |state, _, _| observed.lock().unwrap().push(state))
        });
        wait_for_state_count(&observed, 1).await;
        observed.lock().unwrap().clear();
        let original_metadata = std::fs::metadata(&target).unwrap();
        let original = std::fs::read_to_string(&target).unwrap();
        let replacement = original.replace("before", "after!");
        assert_eq!(original.len(), replacement.len());
        std::fs::write(&target, replacement).unwrap();
        filetime::set_file_mtime(
            &target,
            filetime::FileTime::from_last_modification_time(&original_metadata),
        )
        .unwrap();
        wait_for_state_count(&observed, 1).await;
        assert_eq!(observed.lock().unwrap()[0].tasks[0].subject, "after!");
        subscription.close_and_wait().await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn fallback_recovers_a_ledger_whose_symlink_target_disappeared() {
        let home = TestHome::new();
        let _fallback = WatchFallbackOverrideGuard::new(200);
        struct RestoreEvents;
        impl Drop for RestoreEvents {
            fn drop(&mut self) {
                TASK_WATCH_SUPPRESS_EVENTS.store(false, AtomicOrdering::SeqCst);
            }
        }
        TASK_WATCH_SUPPRESS_EVENTS.store(true, AtomicOrdering::SeqCst);
        let _events = RestoreEvents;
        let list_id = rs("missing-ledger-target");
        create_task(&list_id, input("stable", "d")).await.unwrap();
        let observed = Arc::new(Mutex::new(Vec::new()));
        let subscription = subscribe_test_events(list_id.clone(), {
            let observed = observed.clone();
            Arc::new(move |state, _, _| observed.lock().unwrap().push(state))
        });
        wait_for_state_count(&observed, 1).await;
        let ledger = dir_of(&list_id).join(SNAPSHOT_LEDGER_FILE);
        std::fs::remove_file(&ledger).unwrap();
        std::os::unix::fs::symlink(home.dir.join("missing-ledger.json"), &ledger).unwrap();
        wait_for_state_count(&observed, 2).await;
        subscription.close_and_wait().await.unwrap();
        assert!(std::fs::symlink_metadata(ledger).unwrap().is_file());
        assert_eq!(observed.lock().unwrap()[1].tasks[0].subject, "stable");
    }

    #[tokio::test]
    async fn watcher_ignores_its_own_lock_and_ledger_artifacts() {
        let _home = TestHome::new();
        let _fallback = WatchFallbackOverrideGuard::new(500);
        let list_id = rs("watcher-no-self-loop");
        let observed = Arc::new(Mutex::new(Vec::<TaskListState>::new()));
        let observed_for_callback = observed.clone();
        let subscription = subscribe_test_events(
            list_id.clone(),
            Arc::new(move |state, _, _| observed_for_callback.lock().unwrap().push(state)),
        );
        wait_for_state_count(&observed, 1).await;
        observed.lock().unwrap().clear();
        TASK_WATCH_RECONCILE_COUNT.store(0, AtomicOrdering::SeqCst);
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(
            TASK_WATCH_RECONCILE_COUNT.load(AtomicOrdering::SeqCst),
            0,
            "lock/ledger/temp artifacts must not self-trigger native reconciliation"
        );

        let dir = dir_of(&list_id);
        write_task_file(&dir, &task("1", "real external task"))
            .await
            .unwrap();
        wait_for_state_count(&observed, 1).await;
        assert!(TASK_WATCH_RECONCILE_COUNT.load(AtomicOrdering::SeqCst) >= 1);
        subscription.close_and_wait().await.unwrap();
        assert_eq!(
            observed.lock().unwrap()[0].tasks[0].subject,
            "real external task"
        );
    }

    #[tokio::test]
    async fn directory_recreation_publishes_a_new_generation_even_when_empty() {
        let _home = TestHome::new();
        let list_id = rs("directory-generation");
        create_task(&list_id, input("old", "d")).await.unwrap();
        let observed = Arc::new(Mutex::new(Vec::<TaskListState>::new()));
        let observed_for_callback = observed.clone();
        let subscription = subscribe_test_events(
            list_id.clone(),
            Arc::new(move |state, _, _| observed_for_callback.lock().unwrap().push(state)),
        );
        wait_for_state_count(&observed, 1).await;
        let first_generation = observed.lock().unwrap()[0].storage_generation.clone();
        observed.lock().unwrap().clear();

        let dir = dir_of(&list_id);
        std::fs::remove_dir_all(&dir).unwrap();
        std::fs::create_dir_all(&dir).unwrap();
        wait_for_state_count(&observed, 1).await;
        let second_generation = observed.lock().unwrap()[0].storage_generation.clone();
        assert_ne!(second_generation, first_generation);
        assert_eq!(observed.lock().unwrap()[0].revision, 0);
        assert!(observed.lock().unwrap()[0].tasks.is_empty());
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(observed.lock().unwrap().len(), 1);
        observed.lock().unwrap().clear();

        std::fs::remove_dir_all(&dir).unwrap();
        std::fs::create_dir_all(&dir).unwrap();
        wait_for_state_count(&observed, 1).await;
        subscription.close_and_wait().await.unwrap();
        assert_ne!(
            observed.lock().unwrap()[0].storage_generation,
            second_generation
        );
        assert_eq!(observed.lock().unwrap()[0].revision, 0);
        assert!(observed.lock().unwrap()[0].tasks.is_empty());
    }

    #[tokio::test]
    async fn callback_can_close_its_own_subscription_without_deadlock() {
        let _home = TestHome::new();
        let list_id = rs("self-unsubscribe");
        let deliveries = Arc::new(AtomicU64::new(0));
        let holder = Arc::new(Mutex::new(None::<std::sync::Weak<TaskListSubscription>>));
        let callback_returned = Arc::new(tokio::sync::Notify::new());
        let deliveries_for_callback = deliveries.clone();
        let holder_for_callback = holder.clone();
        let returned_for_callback = callback_returned.clone();
        let subscription = subscribe_test_events(
            list_id.clone(),
            Arc::new(move |_, _, _| {
                let count = deliveries_for_callback.fetch_add(1, Ordering::SeqCst) + 1;
                if count == 2 {
                    holder_for_callback
                        .lock()
                        .unwrap()
                        .as_ref()
                        .and_then(std::sync::Weak::upgrade)
                        .expect("subscription installed before the second delivery")
                        .close();
                    returned_for_callback.notify_one();
                }
            }),
        );
        *holder.lock().unwrap() = Some(Arc::downgrade(&subscription));
        while deliveries.load(Ordering::SeqCst) < 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        create_task(&list_id, input("one", "d")).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), callback_returned.notified())
            .await
            .expect("self-unsubscribe callback deadlocked");
        subscription.close_and_wait().await.unwrap();
        create_task(&list_id, input("two", "d")).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(deliveries.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn real_child_process_mutation_reaches_active_watcher_without_query() {
        let home = TestHome::new();
        let list_id = rs("real-child-process");
        create_task(&list_id, input("before", "d")).await.unwrap();
        let observed = Arc::new(Mutex::new(Vec::<TaskListState>::new()));
        let observed_for_callback = observed.clone();
        let subscription = subscribe_test_events(
            list_id.clone(),
            Arc::new(move |state, _, _| observed_for_callback.lock().unwrap().push(state)),
        );
        wait_for_state_count(&observed, 1).await;
        observed.lock().unwrap().clear();

        let executable = std::env::current_exe().unwrap();
        let config = home.dir.clone();
        let output = tokio::task::spawn_blocking(move || {
            Command::new(executable)
                .arg("--exact")
                .arg("tasks::task_store::tests::task_external_writer_process_helper")
                .arg("--ignored")
                .arg("--test-threads=1")
                .env("KLAUS_CONFIG_DIR", config)
                .env("KLAUS_TASK_EXTERNAL_WRITER_LIST", "real-child-process")
                .output()
                .unwrap()
        })
        .await
        .unwrap();
        assert!(
            output.status.success(),
            "child writer failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        wait_for_state_count(&observed, 1).await;
        subscription.close_and_wait().await.unwrap();
        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].revision, 2);
        assert_eq!(observed[0].tasks[0].subject, "written by child process");
    }

    #[tokio::test]
    async fn real_child_process_plan_updates_and_replacements_reach_native_watcher_without_query() {
        let home = TestHome::new();
        // A missing native event cannot be rescued by the fast test-only fallback.
        let _fallback = WatchFallbackOverrideGuard::new(30_000);
        let list_id = rs("real-child-plan");
        let before = write_task_plan(
            &list_id,
            vec![
                plan_entry("first", "before"),
                plan_entry("second", "second"),
            ],
            false,
            None,
        )
        .await
        .unwrap();
        let observed = Arc::new(Mutex::new(Vec::<TaskListState>::new()));
        let target = observed.clone();
        let subscription = subscribe_test_events(
            list_id.clone(),
            Arc::new(move |state, _, _| target.lock().unwrap().push(state)),
        );
        wait_for_state_count(&observed, 1).await;
        let plan_id = observed.lock().unwrap()[0].plan_id.clone();
        observed.lock().unwrap().clear();
        for replace in [false, true] {
            let executable = std::env::current_exe().unwrap();
            let config = home.dir.clone();
            let task_id = runtime_naming_terminal(&before[0].id);
            let output = tokio::task::spawn_blocking(move || {
                Command::new(executable)
                    .arg("--exact")
                    .arg("tasks::task_store::tests::task_external_writer_process_helper")
                    .arg("--ignored")
                    .arg("--test-threads=1")
                    .env("KLAUS_CONFIG_DIR", config)
                    .env("KLAUS_TASK_EXTERNAL_WRITER_LIST", "real-child-plan")
                    .env("KLAUS_TASK_EXTERNAL_WRITER_TASK", task_id)
                    .env(
                        "KLAUS_TASK_EXTERNAL_WRITER_REPLACE",
                        if replace { "1" } else { "0" },
                    )
                    .output()
                    .unwrap()
            })
            .await
            .unwrap();
            assert!(
                output.status.success(),
                "child writer failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            wait_for_state_count(&observed, 1).await;
            let current = observed.lock().unwrap().clone();
            assert_eq!(
                current.len(),
                1,
                "one complete snapshot per external transaction"
            );
            assert_eq!(current[0].plan_kind, "plan");
            assert_eq!(current[0].tasks.len(), 2);
            if replace {
                assert_ne!(current[0].plan_id, plan_id);
                assert_eq!(current[0].tasks[0].subject, "replacement B");
                assert_eq!(current[0].tasks[1].subject, "replacement A");
            } else {
                assert_eq!(current[0].plan_id, plan_id);
                assert_eq!(current[0].tasks[0].id, before[0].id);
                assert_eq!(current[0].tasks[0].subject, "written by child process");
                assert_eq!(current[0].tasks[1].subject, "second");
            }
            observed.lock().unwrap().clear();
        }
        subscription.close_and_wait().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "subprocess helper invoked by real_child_process_mutation_reaches_active_watcher_without_query"]
    async fn task_external_writer_process_helper() {
        let list_id = RuntimeString::from(
            std::env::var("KLAUS_TASK_EXTERNAL_WRITER_LIST").expect("writer list env"),
        );
        if std::env::var("KLAUS_TASK_EXTERNAL_WRITER_REPLACE").as_deref() == Ok("1") {
            write_task_plan(
                &list_id,
                vec![
                    plan_entry("replacement-b", "replacement B"),
                    plan_entry("replacement-a", "replacement A"),
                ],
                false,
                None,
            )
            .await
            .unwrap();
            return;
        }
        let task_id =
            std::env::var("KLAUS_TASK_EXTERNAL_WRITER_TASK").unwrap_or_else(|_| "1".to_string());
        let updated = update_task(
            &list_id,
            &rs(&task_id),
            &TaskUpdate {
                subject: Some(rs("written by child process")),
                ..TaskUpdate::default()
            },
        )
        .await
        .unwrap();
        assert!(updated.is_some());
    }

    #[tokio::test]
    async fn malformed_task_fails_query_without_advancing_or_publishing_the_ledger() {
        let _home = TestHome::new();
        let list_id = rs("corrupt-task");
        create_task(&list_id, input("intact", "d")).await.unwrap();
        let dir = dir_of(&list_id);
        let before = read_snapshot_ledger(&dir).await.unwrap().unwrap();

        let observed = Arc::new(Mutex::new(Vec::<TaskListState>::new()));
        let observed_for_callback = observed.clone();
        let expected_list = list_id.clone();
        let subscription = subscribe_test_events(
            list_id.clone(),
            Arc::new(move |state, _, _| {
                if state.task_list_id == expected_list {
                    observed_for_callback.lock().unwrap().push(state);
                }
            }),
        );
        wait_for_state_count(&observed, 1).await;
        assert_eq!(observed.lock().unwrap()[0].revision, 1);
        observed.lock().unwrap().clear();
        std::fs::write(task_path(&dir, &rs("1")), br#"{"subject":"torn""#).unwrap();

        let error = query_task_list(&rs("session"), &list_id, Default::default())
            .await
            .expect_err("partial authoritative task must be an explicit persistence failure");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(read_snapshot_ledger(&dir).await.unwrap().unwrap(), before);
        subscription.close_and_wait().await.unwrap();
        assert!(observed.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn read_dir_error_fails_query_without_publishing_a_synthetic_empty_list() {
        let _home = TestHome::new();
        let list_id = rs("read-dir-error");
        create_task(&list_id, input("intact", "d")).await.unwrap();
        let dir = dir_of(&list_id);
        let before = read_snapshot_ledger(&dir).await.unwrap().unwrap();

        let observed = Arc::new(Mutex::new(Vec::<TaskListState>::new()));
        let observed_for_callback = observed.clone();
        let expected_list = list_id.clone();
        let subscription = subscribe_test_events(
            list_id.clone(),
            Arc::new(move |state, _, _| {
                if state.task_list_id == expected_list {
                    observed_for_callback.lock().unwrap().push(state);
                }
            }),
        );
        wait_for_state_count(&observed, 1).await;
        assert_eq!(observed.lock().unwrap()[0].revision, 1);
        observed.lock().unwrap().clear();
        fail_read_dir(&dir, 1);
        let error = query_task_list(&rs("session"), &list_id, Default::default())
            .await
            .expect_err("directory I/O failure must not masquerade as an empty list");
        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(read_snapshot_ledger(&dir).await.unwrap().unwrap(), before);
        subscription.close_and_wait().await.unwrap();
        assert!(observed.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn next_entry_not_found_is_an_iteration_error_not_an_empty_list() {
        let _home = TestHome::new();
        let list_id = rs("next-entry-not-found");
        create_task(&list_id, input("intact", "d")).await.unwrap();
        let dir = dir_of(&list_id);
        let before = read_snapshot_ledger(&dir).await.unwrap().unwrap();

        fail_next_entry_after(&dir, 0, io::ErrorKind::NotFound);
        let error = query_task_list(&rs("session"), &list_id, Default::default())
            .await
            .expect_err("mid-iteration not-found must remain an authoritative I/O failure");
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
        assert_eq!(read_snapshot_ledger(&dir).await.unwrap().unwrap(), before);
    }

    #[tokio::test]
    async fn a_listed_leaf_disappearing_aborts_the_stable_snapshot() {
        let _home = TestHome::new();
        let list_id = rs("listed-leaf-disappears");
        create_task(&list_id, input("intact", "d")).await.unwrap();
        let dir = dir_of(&list_id);
        let before = read_snapshot_ledger(&dir).await.unwrap().unwrap();
        disappear_listed_task_once(&task_path(&dir, &rs("1")));

        let error = query_task_list(&rs("session"), &list_id, Default::default())
            .await
            .expect_err("a listed leaf replacement/deletion must force an authoritative retry");
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        assert_eq!(read_snapshot_ledger(&dir).await.unwrap().unwrap(), before);
    }

    #[tokio::test]
    async fn failed_atomic_task_replace_preserves_the_old_leaf_and_cleans_the_temp() {
        let _home = TestHome::new();
        let list_id = rs("atomic-task-failure");
        create_task(&list_id, input("old", "d")).await.unwrap();
        let dir = dir_of(&list_id);
        let target = task_path(&dir, &rs("1"));
        let old_bytes = std::fs::read(&target).unwrap();
        fail_atomic_leaf_before_replace(&target, 1);

        let error = update_task(
            &list_id,
            &rs("1"),
            &TaskUpdate {
                subject: Some(rs("new")),
                ..TaskUpdate::default()
            },
        )
        .await
        .expect_err("injected pre-replace failure must reach the mutation caller");
        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(std::fs::read(&target).unwrap(), old_bytes);
        assert_eq!(
            get_task(&list_id, &rs("1")).await.unwrap().unwrap().subject,
            "old"
        );
        let temp_prefix = format!(".{}.", target.file_name().unwrap().to_string_lossy());
        assert!(std::fs::read_dir(&dir).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(&temp_prefix)
        }));
    }

    #[tokio::test]
    async fn malformed_high_water_mark_is_not_treated_as_zero() {
        let _home = TestHome::new();
        let list_id = rs("corrupt-high-water");
        create_task(&list_id, input("one", "d")).await.unwrap();
        let dir = dir_of(&list_id);
        std::fs::write(high_water_path(&dir), b"1 trailing-garbage\n").unwrap();

        let error = create_task(&list_id, input("two", "d"))
            .await
            .expect_err("malformed high-water state must fail closed");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(list_tasks(&list_id).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn malformed_snapshot_ledger_fails_query_without_repairing_over_it() {
        let _home = TestHome::new();
        let list_id = rs("corrupt-ledger");
        create_task(&list_id, input("one", "d")).await.unwrap();
        let path = snapshot_ledger_path(&dir_of(&list_id));
        let corrupt = b"{\"schemaVersion\":1,\"revision\":\"not-a-number\"}\n";
        std::fs::write(&path, corrupt).unwrap();

        let error = query_task_list(&rs("session"), &list_id, Default::default())
            .await
            .expect_err("malformed ledger must be an explicit persistence failure");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(std::fs::read(path).unwrap(), corrupt);
    }

    #[tokio::test]
    async fn visible_mutations_emit_one_ordered_full_state_revision_each() {
        let _home = TestHome::new();
        let list_id = rs("ordered-events");
        let observed = Arc::new(Mutex::new(Vec::<TaskListState>::new()));
        let observed_for_callback = observed.clone();
        let expected_list = list_id.clone();
        let subscription = subscribe_test_events(
            list_id.clone(),
            Arc::new(move |state, _, _| {
                if state.task_list_id == expected_list {
                    observed_for_callback.lock().unwrap().push(state);
                }
            }),
        );
        wait_for_state_count(&observed, 1).await;
        assert_eq!(observed.lock().unwrap()[0].revision, 0);
        observed.lock().unwrap().clear();

        create_task(&list_id, input("first", "d")).await.unwrap();
        update_task(
            &list_id,
            &rs("1"),
            &TaskUpdate {
                status: Some(TaskStatus::InProgress),
                ..TaskUpdate::default()
            },
        )
        .await
        .unwrap();
        create_task(&list_id, input("second", "d")).await.unwrap();
        block_task(&list_id, &rs("1"), &rs("2")).await.unwrap();
        assert!(delete_task(&list_id, &rs("1")).await.unwrap());
        wait_for_state_count(&observed, 5).await;
        subscription.close_and_wait().await.unwrap();

        let observed = observed.lock().unwrap();
        assert_eq!(
            observed
                .iter()
                .map(|state| state.revision)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
        assert_eq!(observed[0].tasks[0].subject, "first");
        assert_eq!(observed[1].tasks[0].status, TaskStatus::InProgress);
        assert_eq!(observed[3].tasks[1].blocked_by, vec![rs("1")]);
        assert_eq!(observed[4].tasks.len(), 1);
        assert!(observed[4].tasks[0].blocked_by.is_empty());
    }

    #[tokio::test]
    async fn authoritative_host_snapshot_excludes_internal_tasks_without_revision_churn() {
        let _home = TestHome::new();
        let list_id = rs("host-visible");
        let revisions = Arc::new(Mutex::new(Vec::<u64>::new()));
        let revisions_for_callback = revisions.clone();
        let expected_list = list_id.clone();
        let subscription = subscribe_test_events(
            list_id.clone(),
            Arc::new(move |state, _, _| {
                if state.task_list_id == expected_list {
                    revisions_for_callback.lock().unwrap().push(state.revision);
                }
            }),
        );
        wait_for_revision_count(&revisions, 1).await;
        assert_eq!(*revisions.lock().unwrap(), vec![0]);
        revisions.lock().unwrap().clear();

        let mut internal_metadata = RuntimeObject::new();
        internal_metadata.insert("_internal", RuntimeJson::Bool(true));
        create_task(
            &list_id,
            CreateTaskInput {
                subject: rs("engine bookkeeping"),
                description: rs("hidden"),
                metadata: Some(internal_metadata),
                ..CreateTaskInput::default()
            },
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(revisions.lock().unwrap().is_empty());

        create_task(&list_id, input("visible", "shown"))
            .await
            .unwrap();
        wait_for_revision_count(&revisions, 1).await;
        subscription.close_and_wait().await.unwrap();
        assert_eq!(*revisions.lock().unwrap(), vec![1]);
        let snapshot = query_task_list(&rs("session"), &list_id, Default::default())
            .await
            .unwrap();
        assert_eq!(snapshot.revision, 1);
        assert_eq!(snapshot.tasks.len(), 1);
        assert_eq!(snapshot.tasks[0].id, "2");
        assert_eq!(snapshot.tasks[0].subject, "visible");
    }

    #[tokio::test]
    async fn concurrent_mutations_are_serialized_with_monotonic_event_revisions() {
        let _home = TestHome::new();
        let list_id = rs("concurrent-revisions");
        let revisions = Arc::new(Mutex::new(Vec::<u64>::new()));
        let revisions_for_callback = revisions.clone();
        let expected_list = list_id.clone();
        let subscription = subscribe_test_events(
            list_id.clone(),
            Arc::new(move |state, _, _| {
                if state.task_list_id == expected_list {
                    revisions_for_callback.lock().unwrap().push(state.revision);
                }
            }),
        );
        wait_for_revision_count(&revisions, 1).await;
        assert_eq!(*revisions.lock().unwrap(), vec![0]);
        revisions.lock().unwrap().clear();

        let (a, b, c) = tokio::join!(
            create_task(&list_id, input("a", "d")),
            create_task(&list_id, input("b", "d")),
            create_task(&list_id, input("c", "d")),
        );
        a.unwrap();
        b.unwrap();
        c.unwrap();
        wait_for_revision_count(&revisions, 3).await;
        subscription.close_and_wait().await.unwrap();

        assert_eq!(*revisions.lock().unwrap(), vec![1, 2, 3]);
        let snapshot = query_task_list(&rs("session"), &list_id, Default::default())
            .await
            .unwrap();
        assert_eq!(snapshot.revision, 3);
        assert_eq!(
            snapshot
                .tasks
                .iter()
                .map(|task| task.id.clone())
                .collect::<Vec<_>>(),
            vec![rs("1"), rs("2"), rs("3")]
        );
    }

    // 发布门控让第一个发布者持有全局 hub 互斥锁并阻塞在条件变量上，等待本
    // 测试 future 放行；同一运行时上的订阅 watcher 周期扫描会同步争用该互斥
    // 锁。单线程运行时唯一的 worker 一旦被 watcher 阻塞，测试 future 便无法
    // 恢复去放行门控，形成三方死锁。多 worker 运行时保证门控期间测试 future
    // 始终可被调度，选择顺序与 channel 发送顺序的断言不受影响。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_publishers_cannot_reverse_selection_and_channel_send_order() {
        let _home = TestHome::new();
        let list_id = rs("publish-send-order");
        let states = Arc::new(Mutex::new(Vec::<TaskListState>::new()));
        let states_for_callback = states.clone();
        let subscription = subscribe_test_events(
            list_id.clone(),
            Arc::new(move |state, _, _| states_for_callback.lock().unwrap().push(state)),
        );
        wait_for_state_count(&states, 1).await;
        let baseline = states.lock().unwrap()[0].clone();
        states.lock().unwrap().clear();

        let mut first = baseline.clone();
        first.revision = 1;
        first.fingerprint = rs("sha256:publisher-one");
        let mut second = baseline;
        second.revision = 2;
        second.fingerprint = rs("sha256:publisher-two");
        let (entered, release) = install_task_publish_send_gate();
        let first_thread = std::thread::spawn(move || publish_task_state(&first));
        entered.notified().await;
        let second_thread = std::thread::spawn(move || publish_task_state(&second));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(states.lock().unwrap().is_empty());
        {
            let (released, ready) = &*release;
            *released.lock().unwrap() = true;
            ready.notify_all();
        }
        assert!(first_thread.join().unwrap());
        assert!(second_thread.join().unwrap());
        wait_for_state_count(&states, 2).await;
        subscription.close_and_wait().await.unwrap();
        assert_eq!(
            states
                .lock()
                .unwrap()
                .iter()
                .map(|state| state.revision)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    fn assistant_turns(n: usize) -> Vec<Message> {
        (0..n)
            .map(|i| {
                Message::Assistant(AssistantMessage {
                    uuid: format!("a{i}").into(),
                    content: vec![],
                    model: "m".into(),
                    usage: None,
                })
            })
            .collect()
    }

    fn reminder_ctx(n: usize) -> ReminderContext {
        ReminderContext {
            messages: assistant_turns(n),
            cwd: "/x".into(),
            task_list_id: Some(rs("tl")),
            task_plan: None,
            signal: None,
            plan_file_path: None,
            plan_file_exists: None,
            has_task_tool: false,
        }
    }

    #[tokio::test]
    async fn refreshes_open_tasks_after_the_work_threshold() {
        let _home = TestHome::new();
        let list = list();
        create_task(&list, input("open task", "d")).await.unwrap();
        let p = create_task_reminder_provider();
        assert_eq!(p.reminder(&reminder_ctx(5)).await, None);
        let r = p.reminder(&reminder_ctx(10)).await.unwrap();
        assert!(r.contains("open task"));
        assert!(p
            .reminder(&reminder_ctx(15))
            .await
            .unwrap()
            .contains("open task"));
    }

    #[tokio::test]
    async fn missing_checklist_guidance_is_conditional_and_excludes_simple_work() {
        let _home = TestHome::new();
        let p = create_task_reminder_provider();
        assert!(p.reminder(&reminder_ctx(0)).await.is_none());
        let reminder = p.reminder(&reminder_ctx(1)).await.unwrap();
        assert!(reminder.contains("if a checklist would help"));
        assert!(reminder.contains("simple answers and straightforward actions do not require one"));
        assert!(p.reminder(&reminder_ctx(20)).await.is_some());
    }

    #[tokio::test]
    async fn with_lock_serializes_concurrent_critical_sections() {
        let home = TestHome::new();
        let target = home
            .dir
            .join("res")
            .to_str()
            .expect("test path is UTF-8")
            .to_owned();
        let active = AtomicI64::new(0);
        let max_active = AtomicI64::new(0);
        let results = futures::future::join_all((0..5).map(|_| {
            with_lock(
                &target,
                || async {
                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    max_active.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok::<(), io::Error>(())
                },
                LockOptions::default(),
            )
        }))
        .await;
        for r in results {
            r.unwrap();
        }
        assert_eq!(max_active.load(Ordering::SeqCst), 1); // never two holders at once
    }
    fn plan_entry(id: &str, subject: &str) -> TaskPlanWriteEntry {
        TaskPlanWriteEntry {
            entry_id: Some(rs(id)),
            subject: Some(rs(subject)),
            description: None,
            active_form: None,
            status: None,
        }
    }

    #[tokio::test]
    async fn task_identity_survives_other_work_and_archival_but_tracks_its_semantics() {
        let _home = TestHome::new();
        let list = rs("task-identity-semantics");
        let a = create_task(&list, input("Inspect A", "Await its external result"))
            .await
            .unwrap();
        let b = create_task(&list, input("Inspect B", "Independent work"))
            .await
            .unwrap();
        let original = get_task_identity_bound(&list, &a.id, None)
            .await
            .unwrap()
            .unwrap();
        update_task(
            &list,
            &b.id,
            &TaskUpdate {
                subject: Some(rs("B changed")),
                status: Some(TaskStatus::Completed),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(
            get_task_identity_bound(&list, &a.id, None).await.unwrap(),
            Some(original.clone())
        );
        update_task(
            &list,
            &a.id,
            &TaskUpdate {
                status: Some(TaskStatus::Completed),
                owner: Some(rs("another-owner")),
                active_form: Some(rs("Finishing A")),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let completed = get_task_identity_bound(&list, &a.id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(completed.task.status, TaskStatus::Completed);
        assert_eq!(completed.fingerprint, original.fingerprint);
        assert_eq!(completed.generation, original.generation);
        update_task(
            &list,
            &a.id,
            &TaskUpdate {
                description: Some(rs("A has a new requirement")),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let changed = get_task_identity_bound(&list, &a.id, None)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(changed.fingerprint, original.fingerprint);
        assert_eq!(changed.generation, original.generation);
        write_task_plan(
            &list,
            vec![plan_entry("new-plan", "New independent task")],
            false,
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            get_task_identity_bound(&list, &a.id, None).await.unwrap(),
            Some(changed)
        );
    }

    #[tokio::test]
    async fn task_identity_changes_on_numeric_reuse_and_directory_recreation() {
        let _home = TestHome::new();
        let list = rs("task-identity-reuse");
        let first = create_task_bound(&list, input("Same work", "Same details"), None)
            .await
            .unwrap();
        let stale = TaskPlanBinding::capture(&list);
        let original = get_task_identity_bound(&list, &first.id, Some(&stale))
            .await
            .unwrap()
            .unwrap();
        delete_task_bound(&list, &first.id, Some(&stale))
            .await
            .unwrap();
        assert_eq!(
            get_task_identity_bound(&list, &first.id, None)
                .await
                .unwrap(),
            None
        );
        let replacement = create_task_bound(&list, input("Same work", "Same details"), None)
            .await
            .unwrap();
        assert_eq!(replacement.id, first.id);
        assert!(get_task_identity_bound(&list, &first.id, Some(&stale))
            .await
            .is_err());
        let replaced = get_task_identity_bound(&list, &first.id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replaced.fingerprint, original.fingerprint);
        assert_ne!(replaced.generation, original.generation);
        tokio::fs::remove_dir_all(dir_of(&list)).await.unwrap();
        let recreated = create_task_bound(&list, input("Same work", "Same details"), None)
            .await
            .unwrap();
        let recreated = get_task_identity_bound(&list, &recreated.id, None)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(recreated.generation, replaced.generation);
        assert!(get_task_identity_bound(&list, &rs("../1"), None)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn task_plan_batch_failure_rolls_back_all_leaves_membership_and_revision() {
        let _home = TestHome::new();
        let list_id = rs("batch-rollback");
        write_task_plan(
            &list_id,
            vec![plan_entry("a", "A"), plan_entry("b", "B")],
            false,
            None,
        )
        .await
        .unwrap();
        let before = query_task_list(&rs("session"), &list_id, Default::default())
            .await
            .unwrap();
        let dir = dir_of(&list_id);
        fail_atomic_leaf_before_replace(&task_path(&dir, &before.tasks[1].id), 1);
        assert!(write_task_plan(
            &list_id,
            vec![plan_entry("a", "Changed A"), plan_entry("b", "Changed B")],
            true,
            None
        )
        .await
        .is_err());
        assert_eq!(
            query_task_list(&rs("session"), &list_id, Default::default())
                .await
                .unwrap(),
            before
        );
        assert_eq!(
            get_task(&list_id, &before.tasks[0].id)
                .await
                .unwrap()
                .unwrap()
                .subject,
            "A"
        );
        assert!(!dir.join(".task-plan-transaction.json").exists());
        // The authoritative snapshot ledger is in the same transaction.
        fail_atomic_leaf_before_replace(&snapshot_ledger_path(&dir), 1);
        assert!(
            write_task_plan(&list_id, vec![plan_entry("c", "Replacement")], false, None)
                .await
                .is_err()
        );
        assert_eq!(
            query_task_list(&rs("session"), &list_id, Default::default())
                .await
                .unwrap(),
            before
        );
        assert_eq!(list_tasks(&list_id).await.unwrap().len(), 2);
        assert_eq!(
            create_task(&list_id, input("append after failure", "d"))
                .await
                .unwrap()
                .id,
            "1"
        );
    }
    #[tokio::test]
    async fn cold_legacy_task_tokens_and_interrupted_plan_journal_restore_safely() {
        let _home = TestHome::new();
        let list = rs("legacy-token-recovery");
        create_task(&list, input("legacy", "details"))
            .await
            .unwrap();
        let dir = dir_of(&list);
        std::fs::remove_file(dir.join(".task-plans.json")).unwrap();
        let stale = TaskPlanBinding::capture(&list);
        get_task_bound(&list, &rs("1"), Some(&stale)).await.unwrap();
        delete_task_bound(&list, &rs("1"), Some(&stale))
            .await
            .unwrap();
        let fresh = TaskPlanBinding::capture(&list);
        create_task_bound(&list, input("replacement", "new identity"), Some(&fresh))
            .await
            .unwrap();
        assert!(get_task_bound(&list, &rs("1"), Some(&stale)).await.is_err());
        let before = query_task_list(&rs("session"), &list, Default::default())
            .await
            .unwrap();
        let before_tasks = list_tasks(&list).await.unwrap();
        let ledger = runtime_json::parse(&RuntimeString::from(
            std::fs::read_to_string(dir.join(".task-plans.json")).unwrap(),
        ))
        .unwrap();
        let snapshot =
            runtime_codec::encode_value(&read_snapshot_ledger(&dir).await.unwrap()).unwrap();
        let created = create_task_bound(
            &list,
            input("interrupted append", "uncommitted"),
            Some(&fresh),
        )
        .await
        .unwrap();
        // Reconstruct the undo journal present if a process dies before the batch commits.
        let mut journal = crate::runtime_values::RuntimeObject::new();
        journal.insert("ledger", ledger);
        journal.insert("tasks", runtime_codec::encode_value(&before_tasks).unwrap());
        journal.insert(
            "created",
            runtime_codec::encode_value(&vec![created.id.clone()]).unwrap(),
        );
        journal.insert("snapshot", snapshot);
        std::fs::write(
            dir.join(".task-plan-transaction.json"),
            runtime_json::stringify(&RuntimeJson::Object(journal)).to_node_utf8(),
        )
        .unwrap();
        let restored = TaskPlanBinding::capture(&list);
        assert_eq!(
            query_task_list(&rs("session"), &list, Default::default())
                .await
                .unwrap(),
            before
        );
        assert!(get_task(&list, &created.id).await.unwrap().is_none());
        assert_eq!(
            list_plan_tasks(&list, Some(&restored)).await.unwrap(),
            before.tasks
        );
        // The rolled-back ID was not observed by the restored run and may be learned anew.
        let child = restored.fork();
        let recreated = create_task_bound(&list, input("later append", "committed"), Some(&child))
            .await
            .unwrap();
        assert_eq!(recreated.id, created.id);
        get_task_bound(&list, &recreated.id, Some(&restored))
            .await
            .unwrap();
        update_task_bound(
            &list,
            &recreated.id,
            &TaskUpdate {
                status: Some(TaskStatus::Completed),
                ..Default::default()
            },
            Some(&restored),
        )
        .await
        .unwrap();
    }

    #[test]
    fn task_leaf_recognition_excludes_private_ledgers_and_requires_exact_opaque_ids() {
        assert_eq!(task_file_id(&rs("01.json")), Some(rs("01")));
        assert!(task_file_id(&rs("plan-step-63cb5ac1-3dde-40a4-ab4e-23d78ef1d62b.json")).is_some());
        for invalid in [
            ".task-plans.json",
            ".task-plan-transaction.json",
            "plan-step-invalid.json",
            "plan-step-63cb5ac13dde40a4ab4e23d78ef1d62b.json",
            "other.json",
        ] {
            assert!(task_file_id(&rs(invalid)).is_none(), "{invalid}");
        }
    }
}
