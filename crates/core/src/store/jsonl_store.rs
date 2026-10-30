//! Append-only JSONL session store.

use crate::fs::node_path::runtime_string_from_native;
use crate::fs::paths::sanitize_path_component;
use crate::runtime_values::{
    runtime_codec, runtime_json, NodeBufferUtf8, RuntimeCodecError, RuntimeCodecErrorKind,
    RuntimeDecode, RuntimeEncode, RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};
use crate::schemas::message::Message;
use crate::schemas::rollout::{
    CheckpointItem, CompactedItem, EventItem, RollbackItem, RolloutItem, SessionMeta,
    TurnContextSnapshot, ROLLOUT_SCHEMA_VERSION,
};
use crate::store::compression;
use crate::store::external_import::{ExternalSessionImport, ExternalSessionImportResult};
use crate::store::header_index::{
    entry_to_summary, file_modified_at, CatalogIoStats, HeaderEntry, HeaderIndex, ScanOptions,
};
use crate::store::projection::project_for_prompt;
use crate::store::rollout::{parse_rollout_line, serialize_rollout_item};
use crate::store::search::{
    file_name_of, ripgrep_candidate_names, scan_rollout_file, SessionSearchHit,
    SessionSearchOptions, SessionSearchPage, DEFAULT_SEARCH_LIMIT,
};
use crate::store::session_store::{
    AppendEventOptions, CheckpointRecord, CompactionRecord, ListOptions, LiveRollout, ResumeResult,
    ResumedLiveRollout, RollbackRecord, SessionStore, SubAgentMeta, SubAgentTranscript, ThreadPage,
    ThreadReadSnapshot, ThreadSummary, UnarchiveSessionResult, UnarchiveSessionStatus,
};
use async_trait::async_trait;
use fs2::FileExt;
use sha2::{Digest, Sha256};
use std::collections::{HashSet, VecDeque};
use std::ffi::OsStr;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot, Mutex};

#[derive(Debug, Clone, Default)]
pub struct JsonlSessionStoreOptions {
    pub dir: Option<String>,
    /// Subdirectory name for archived sessions (default: "archived").
    pub archive_dir: Option<String>,
}

const DEFAULT_ARCHIVE_SUBDIR: &str = "archived";
const DEFAULT_PAGE_LIMIT: usize = 50;
const CATALOG_SNAPSHOT_LIMIT: usize = 16;
const CATALOG_SNAPSHOT_TTL: Duration = Duration::from_secs(5 * 60);
const UNARCHIVE_JOURNAL_SUBDIR: &str = ".unarchive-journal";
const UNARCHIVE_JOURNAL_VERSION: u32 = 1;
const UNARCHIVE_INTENT_FILE: &str = "intent.json";
const UNARCHIVE_DATA_MOVED_MARKER: &str = "data_moved";
const UNARCHIVE_MAIN_MOVED_MARKER: &str = "main_moved";
/// How long `require_sub_agent_dir` waits for the session lifecycle lock
/// before refusing. A same-batch sibling spawn holds it for the duration of
/// its own dir resolution (milliseconds); a lifecycle op (rename/archive/
/// unarchive) can hold it longer, but every holder releases, so a bounded
/// wait converts the old instant refusal into a queue without letting a
/// stuck contender hang a spawn forever.
const SUBAGENT_DIR_LOCK_WAIT: Duration = Duration::from_secs(5);
const SUBAGENT_DIR_LOCK_RETRY_START: Duration = Duration::from_millis(2);
const SUBAGENT_DIR_LOCK_RETRY_MAX: Duration = Duration::from_millis(100);

/// Immutable durable intent for one main-last unarchive transaction. Phase
/// advancement is represented by separately fsynced marker files so a crash
/// can never tear the only copy of the intent while replacing it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct UnarchiveIntent {
    version: u32,
    operation: String,
    thread_id: String,
    file_name: String,
    data_expected: bool,
}

impl RuntimeEncode for UnarchiveIntent {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("version", &self.version)?;
        object.field("operation", &self.operation)?;
        object.field("threadId", &self.thread_id)?;
        object.field("fileName", &self.file_name)?;
        object.field("dataExpected", &self.data_expected)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for UnarchiveIntent {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, true, |object| {
            Ok(Self {
                version: object.required("version", &[])?,
                operation: decode_unarchive_intent_scalar(
                    object.required("operation", &[])?,
                    "operation",
                )?,
                thread_id: decode_unarchive_intent_scalar(
                    object.required("threadId", &[])?,
                    "threadId",
                )?,
                file_name: decode_unarchive_intent_scalar(
                    object.required("fileName", &[])?,
                    "fileName",
                )?,
                data_expected: object.required("dataExpected", &[])?,
            })
        })
    }
}

fn decode_unarchive_intent_scalar(
    value: RuntimeString,
    field: &'static str,
) -> Result<String, RuntimeCodecError> {
    value.try_to_utf8().map_err(|_| {
        RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(RuntimeString::from(
            "unarchive intent filesystem fields must be scalar UTF-8 strings",
        )))
        .prepend(field)
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UnarchiveRecoveryState {
    Archived,
    Active,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UnarchiveTestPoint {
    IntentDurable,
    DataRenamed,
    DataPhaseDurable,
    MainRenamed,
    MainPhaseDurable,
}

#[cfg(test)]
type CreationTestGate = (
    std::sync::Arc<tokio::sync::Notify>,
    std::sync::Arc<tokio::sync::Notify>,
);

#[cfg(test)]
type ImportTestGate = (
    std::sync::Arc<tokio::sync::Notify>,
    std::sync::Arc<(StdMutex<bool>, std::sync::Condvar)>,
);

/// Append-only JSONL session store. Each session is one file
/// `<dir>/<createdAt>-<id>.jsonl`, guarded by a create-exclusive `.lock`
/// sidecar so only one writer may hold a session at a time. Resume is
/// crash-tolerant: a truncated final line (e.g. from `kill -9` mid-append) is
/// skipped, not fatal.
///
/// Resume reconstructs the COMPACTED, model-visible projection (see
/// `project_for_prompt`) rather than the raw transcript, so a resumed session
/// sees exactly the history the model last saw. Listing is backed by a durable,
/// incrementally validated metadata catalog and immutable pagination snapshots.
pub struct JsonlSessionStore {
    dir: String,
    archive_root: String,
    dir_ensured: AtomicBool,
    index: Mutex<HeaderIndex>,
    catalog_snapshots: StdMutex<VecDeque<CatalogSnapshot>>,
    /// Same-process lifecycle admission. The on-disk advisory lock below
    /// covers other store instances/processes; this set also gives a precise
    /// `concurrent` result without relying on platform lock error text.
    lifecycle_ops: StdMutex<HashSet<String>>,
    /// Process-local profile activity cache for this store's roots.
    profile_memory: Mutex<crate::store::profile_activity::MemoryCache>,
    /// Deterministic cancellation at the asynchronous creation boundary. The
    /// production path owns the same writer/identity-lock pair without a gate.
    #[cfg(test)]
    create_before_publish: StdMutex<Option<CreationTestGate>>,
    #[cfg(test)]
    import_before_publish: StdMutex<Option<ImportTestGate>>,
}

impl JsonlSessionStore {
    async fn overlay_meta(&self, meta: &mut SessionMeta) -> io::Result<()> {
        let path = join_path(&self.dir, "thread-metadata.sqlite3");
        let mut value = meta.clone();
        *meta = tokio::task::spawn_blocking(move || {
            let db = crate::store::thread_metadata::ThreadMetadataDb::open(Path::new(&path))?;
            db.overlay_meta(&mut value)?;
            Ok::<_, io::Error>(value)
        })
        .await
        .map_err(io::Error::other)??;
        Ok(())
    }

    async fn overlay_summary(&self, summary: &mut ThreadSummary) -> io::Result<()> {
        let path = join_path(&self.dir, "thread-metadata.sqlite3");
        if !tokio::fs::try_exists(&path).await? {
            return Ok(());
        }
        let mut value = summary.clone();
        *summary = tokio::task::spawn_blocking(move || {
            let db = crate::store::thread_metadata::ThreadMetadataDb::open(Path::new(&path))?;
            db.overlay_summary(&mut value)?;
            Ok::<_, io::Error>(value)
        })
        .await
        .map_err(io::Error::other)??;
        Ok(())
    }

    async fn read_authority_meta(&self, meta: &mut SessionMeta) -> io::Result<()> {
        let path = join_path(&self.dir, "thread-metadata.sqlite3");
        if !tokio::fs::try_exists(&path).await? {
            return Ok(());
        }
        let id = meta.id.clone();
        let current = tokio::task::spawn_blocking(move || {
            crate::store::thread_metadata::ThreadMetadataDb::open(Path::new(&path))?.get(&id)
        })
        .await
        .map_err(io::Error::other)??;
        if let Some(current) = current {
            *meta = current;
        }
        Ok(())
    }

    fn writer_projection(&self, id: &RuntimeString) -> WriterProjection {
        WriterProjection {
            root: self.dir.clone(),
            thread: runtime_path_component_to_store_host(id),
        }
    }

    async fn delete_saved_authority(&self, thread: &str) -> io::Result<()> {
        let root = self.dir.clone();
        let thread = thread.to_string();
        tokio::task::spawn_blocking(move || {
            crate::store::thread_metadata::ThreadMetadataDb::open(Path::new(&join_path(
                &root,
                "thread-metadata.sqlite3",
            )))?
            .delete(&thread.clone().into())?;
            crate::store::thread_index::TurnIndexDb::open(&join_path(
                &root,
                "threads-index.sqlite3",
            ))?
            .delete_thread(&thread)
        })
        .await
        .map_err(io::Error::other)?
    }
    pub fn new(opts: JsonlSessionStoreOptions) -> Self {
        let dir = opts.dir.unwrap_or_else(default_sessions_dir);
        let archive_root = join_path(
            &dir,
            opts.archive_dir
                .as_deref()
                .unwrap_or(DEFAULT_ARCHIVE_SUBDIR),
        );
        JsonlSessionStore {
            dir,
            archive_root,
            dir_ensured: AtomicBool::new(false),
            index: Mutex::new(HeaderIndex::new()),
            catalog_snapshots: StdMutex::new(VecDeque::new()),
            lifecycle_ops: StdMutex::new(HashSet::new()),
            profile_memory: Mutex::new(crate::store::profile_activity::MemoryCache::default()),
            #[cfg(test)]
            create_before_publish: StdMutex::new(None),
            #[cfg(test)]
            import_before_publish: StdMutex::new(None),
        }
    }

    /// Observe metadata I/O without exposing any stored account or message data.
    pub async fn catalog_io_stats(&self) -> CatalogIoStats {
        self.index.lock().await.io_stats()
    }

    fn begin_lifecycle(&self, thread_id: &str) -> Option<LifecycleOperationGuard<'_>> {
        let mut active = self.lifecycle_ops.lock().expect("lifecycle operations");
        if !active.insert(thread_id.to_string()) {
            return None;
        }
        Some(LifecycleOperationGuard {
            active: &self.lifecycle_ops,
            thread_id: thread_id.to_string(),
        })
    }

    /// Try to serialize lifecycle mutations across independent store instances
    /// and processes. The stable SHA-256 name avoids path traversal, Unicode
    /// projection collisions, and filesystem component-length surprises.
    async fn try_lifecycle_file_lock(
        &self,
        thread_id: &str,
    ) -> io::Result<Option<LifecycleFileLock>> {
        let lock_root = join_path(&self.dir, ".lifecycle-locks");
        tokio::fs::create_dir_all(&lock_root).await?;
        let lock_path = join_path(
            &lock_root,
            &format!("{}.lock", sha256_hex(thread_id.as_bytes())),
        );
        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)
            .await?
            .into_std()
            .await;
        match crate::fs::exclusive_lock::try_lock_exclusive(&file)? {
            crate::fs::exclusive_lock::LockAttempt::Acquired => {
                Ok(Some(LifecycleFileLock { file }))
            }
            crate::fs::exclusive_lock::LockAttempt::Contended => Ok(None),
        }
    }

    /// One turn window served from the committed history projection. A saved
    /// legacy rollout is explicitly imported before its first page is published.
    pub async fn paged_thread_history(
        &self,
        thread_id: &str,
        cursor: Option<&str>,
        limit: Option<usize>,
        items_view: Option<&str>,
        sort_direction: Option<&str>,
    ) -> io::Result<Option<crate::store::thread_index::ThreadPageSnapshot>> {
        let path = match self.locate(thread_id).await? {
            Some(path) => path,
            None => return Ok(None),
        };
        let metadata = tokio::fs::metadata(&path).await?;
        let stamp = file_stamp(&metadata);
        let len = metadata.len();
        let db_path = join_path(&self.dir, "threads-index.sqlite3");
        let thread = thread_id.to_string();
        let cursor = cursor.map(str::to_string);
        let items_view = items_view.map(str::to_string);
        let sort = sort_direction.map(str::to_string);
        let snapshot = tokio::task::spawn_blocking(move || {
            if !matches!(sort.as_deref(), None | Some("asc" | "desc")) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "unknown sortDirection",
                ));
            }
            let mut db = crate::store::thread_index::TurnIndexDb::open(&db_path)?;
            db.window(
                &thread,
                &path,
                len,
                stamp,
                cursor.as_deref(),
                limit.unwrap_or(25).clamp(1, 100),
                sort.as_deref() != Some("asc"),
                items_view.as_deref().unwrap_or("full"),
            )
        })
        .await
        .map_err(|error| io::Error::other(error.to_string()))?;
        Ok(Some(snapshot?))
    }

    /// One message window of a saved turn, selected before decoding content.
    pub async fn paged_turn_slice(
        &self,
        thread_id: &str,
        turn_id: &str,
        cursor: Option<&str>,
        limit: Option<usize>,
        sort_direction: Option<&str>,
    ) -> io::Result<Option<crate::store::thread_index::ThreadPageSnapshot>> {
        let path = match self.locate(thread_id).await? {
            Some(path) => path,
            None => return Ok(None),
        };
        let metadata = tokio::fs::metadata(&path).await?;
        let stamp = file_stamp(&metadata);
        let len = metadata.len();
        let db_path = join_path(&self.dir, "threads-index.sqlite3");
        let thread = thread_id.to_string();
        let turn = turn_id.to_string();
        let cursor = cursor.map(str::to_string);
        let desc = sort_direction != Some("asc");
        if !matches!(sort_direction, None | Some("asc" | "desc")) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unknown sortDirection",
            ));
        }
        let snapshot = tokio::task::spawn_blocking(move || {
            let mut db = crate::store::thread_index::TurnIndexDb::open(&db_path)?;
            db.turn_slice(
                &thread,
                &path,
                len,
                stamp,
                &turn,
                crate::store::thread_index::MessageWindow {
                    cursor: cursor.as_deref(),
                    limit: limit.unwrap_or(100).clamp(1, 100),
                    desc,
                },
            )
        })
        .await
        .map_err(|error| io::Error::other(error.to_string()))?;
        snapshot
    }

    async fn locate_namespaces(
        &self,
        thread_id: &str,
    ) -> io::Result<(Option<HeaderEntry>, Option<HeaderEntry>)> {
        Ok((
            find_thread_header(&self.dir, thread_id, false).await?,
            find_thread_header(&self.archive_root, thread_id, true).await?,
        ))
    }

    fn unarchive_journal_root(&self) -> String {
        join_path(&self.dir, UNARCHIVE_JOURNAL_SUBDIR)
    }

    fn unarchive_journal_dir(&self, thread_id: &str) -> String {
        join_path(
            &self.unarchive_journal_root(),
            &sha256_hex(thread_id.as_bytes()),
        )
    }

    async fn write_unarchive_intent(&self, intent: &UnarchiveIntent) -> io::Result<String> {
        let journal_root = self.unarchive_journal_root();
        tokio::fs::create_dir_all(&journal_root).await?;
        // Persist creation of the journal namespace before publishing a child
        // transaction directory into it.
        sync_directory(&self.dir).await?;

        let journal_dir = self.unarchive_journal_dir(&intent.thread_id);
        tokio::fs::create_dir_all(&journal_dir).await?;
        sync_directory(&journal_root).await?;

        let intent_path = join_path(&journal_dir, UNARCHIVE_INTENT_FILE);
        let temp_path = join_path(&journal_dir, "intent.json.tmp");
        match tokio::fs::remove_file(&temp_path).await {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        let bytes = runtime_codec::encode_bytes(intent).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to encode unarchive intent: {error}"),
            )
        })?;
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
            .await?;
        file.write_all(&bytes).await?;
        file.write_all(b"\n").await?;
        file.sync_all().await?;
        drop(file);
        tokio::fs::rename(&temp_path, &intent_path).await?;
        sync_directory(&journal_dir).await?;
        Ok(journal_dir)
    }

    async fn mark_unarchive_phase(&self, journal_dir: &str, marker: &str) -> io::Result<()> {
        let marker_path = join_path(journal_dir, marker);
        match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&marker_path)
            .await
        {
            Ok(mut file) => {
                file.write_all(b"durable\n").await?;
                file.sync_all().await?;
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                // Recovery is idempotent. An existing marker was itself synced
                // before publication, so it already represents this phase.
            }
            Err(error) => return Err(error),
        }
        sync_directory(journal_dir).await
    }

    async fn clear_unarchive_journal(&self, journal_dir: &str) -> io::Result<()> {
        match tokio::fs::remove_dir_all(journal_dir).await {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        }
        sync_directory(&self.unarchive_journal_root()).await
    }

    async fn read_unarchive_intent(&self, journal_dir: &str) -> io::Result<UnarchiveIntent> {
        let bytes = tokio::fs::read(join_path(journal_dir, UNARCHIVE_INTENT_FILE)).await?;
        let raw = NodeBufferUtf8::decode(&bytes);
        let intent: UnarchiveIntent = runtime_codec::decode_text(&raw).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid unarchive intent in {journal_dir}: {error}"),
            )
        })?;
        if intent.version != UNARCHIVE_JOURNAL_VERSION || intent.operation != "unarchive" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported unarchive intent version/operation in {journal_dir}"),
            ));
        }
        if base_name(journal_dir) != sha256_hex(intent.thread_id.as_bytes()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unarchive intent identity does not match {journal_dir}"),
            ));
        }
        let file_name = Path::new(&intent.file_name);
        if !compression::is_rollout_name(&intent.file_name)
            || file_name.components().count() != 1
            || file_name.file_name() != Some(OsStr::new(&intent.file_name))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "invalid rollout file name in unarchive intent: {}",
                    intent.file_name
                ),
            ));
        }
        Ok(intent)
    }

    /// Reconcile a durable unarchive journal while the per-thread lifecycle
    /// lock is held. The main rollout is the commit record: archived main rolls
    /// sidechains back; active main rolls them forward. This remains correct for
    /// crashes both before and after either phase marker is published.
    async fn recover_unarchive_if_needed_locked(
        &self,
        thread_id: &str,
    ) -> io::Result<Option<UnarchiveRecoveryState>> {
        let journal_dir = self.unarchive_journal_dir(thread_id);
        let intent = match self.read_unarchive_intent(&journal_dir).await {
            Ok(intent) => intent,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        if intent.thread_id != thread_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unarchive journal thread identity mismatch",
            ));
        }

        let archived_main = join_path(&self.archive_root, &intent.file_name);
        let active_main = join_path(&self.dir, &intent.file_name);
        let archived_exists = path_exists(&archived_main).await?;
        let active_exists = path_exists(&active_main).await?;
        let state = match (archived_exists, active_exists) {
            (true, false) => UnarchiveRecoveryState::Archived,
            (false, true) => UnarchiveRecoveryState::Active,
            (true, true) => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!(
                        "unarchive recovery found both active and archived rollouts for {thread_id}"
                    ),
                ))
            }
            (false, false) => {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                    "unarchive recovery found neither active nor archived rollout for {thread_id}"
                ),
                ))
            }
        };

        let archived_data = session_data_dir(&archived_main);
        let active_data = session_data_dir(&active_main);
        let archived_data_exists = path_exists(&archived_data).await?;
        let active_data_exists = path_exists(&active_data).await?;
        if archived_data_exists && active_data_exists {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "unarchive recovery found both active and archived sidechain trees for {thread_id}"
                ),
            ));
        }
        if intent.data_expected && !archived_data_exists && !active_data_exists {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("unarchive recovery lost the sidechain tree for {thread_id}"),
            ));
        }

        match state {
            UnarchiveRecoveryState::Archived if active_data_exists => {
                tokio::fs::rename(&active_data, &archived_data).await?;
                sync_unarchive_rename_parents(&active_data, &archived_data).await?;
            }
            UnarchiveRecoveryState::Active if archived_data_exists => {
                tokio::fs::rename(&archived_data, &active_data).await?;
                sync_unarchive_rename_parents(&archived_data, &active_data).await?;
            }
            _ => {}
        }

        // Never infer durability merely from the namespace currently visible
        // after a crash. The previous process may have died after raw rename
        // but before syncing either parent. Re-sync both main parents and, when
        // present, both sidechain parents before clearing intent or reporting a
        // committed state. This also closes a second-crash window after a
        // recovery rename but before its first parent sync.
        sync_unarchive_rename_parents(&archived_main, &active_main).await?;
        if intent.data_expected {
            sync_unarchive_rename_parents(&archived_data, &active_data).await?;
        }

        match state {
            UnarchiveRecoveryState::Active => {
                self.mark_unarchive_phase(&journal_dir, UNARCHIVE_DATA_MOVED_MARKER)
                    .await?;
                self.mark_unarchive_phase(&journal_dir, UNARCHIVE_MAIN_MOVED_MARKER)
                    .await?;
                let header = find_thread_header(&self.dir, thread_id, false)
                    .await?
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("recovered active rollout has no header for {thread_id}"),
                        )
                    })?;
                if header.path != active_main {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("recovered active rollout path mismatch for {thread_id}"),
                    ));
                }
            }
            UnarchiveRecoveryState::Archived => {
                let header = find_thread_header(&self.archive_root, thread_id, true)
                    .await?
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("recovered archived rollout has no header for {thread_id}"),
                        )
                    })?;
                if header.path != archived_main {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("recovered archived rollout path mismatch for {thread_id}"),
                    ));
                }
            }
        }
        self.clear_unarchive_journal(&journal_dir).await?;
        self.index.lock().await.delete(thread_id);
        Ok(Some(state))
    }

    /// Recover every durable transaction visible to list/cold-start readers.
    /// A currently locked transaction is skipped: main-last visibility keeps
    /// list pagination safe, while thread-specific open paths wait for the same
    /// lifecycle lock before touching sidechains.
    async fn recover_pending_unarchives(&self) -> io::Result<()> {
        let journal_root = self.unarchive_journal_root();
        let Some(names) = read_dir_names(&journal_root).await? else {
            return Ok(());
        };
        for name in names {
            let journal_dir = join_path(&journal_root, &name);
            let intent = match self.read_unarchive_intent(&journal_dir).await {
                Ok(intent) => intent,
                // A crash before intent publication cannot have crossed a
                // rename. Leave the harmless orphan for a later maintenance
                // pass rather than racing a live writer still publishing it.
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            let Some(_operation) = self.begin_lifecycle(&intent.thread_id) else {
                continue;
            };
            let Some(_file_lock) = self.try_lifecycle_file_lock(&intent.thread_id).await? else {
                continue;
            };
            self.recover_unarchive_if_needed_locked(&intent.thread_id)
                .await?;
        }
        Ok(())
    }

    /// Build a [`LiveRollout`] appending to `file`. The main session uses a
    /// create-exclusive writer lease. Sidechain files retain the same lease
    /// for their whole append lifetime. Dead process leases use the shared
    /// stale-owner recovery, and the write queue preserves per-file ordering.
    async fn build_live(
        &self,
        file: &str,
        meta: Option<&SessionMeta>,
        opts: BuildLiveOptions,
    ) -> io::Result<Box<dyn LiveRollout>> {
        let lock_path = compression::writer_lock_path(file);
        let lock_handle = if opts.lock {
            let lease = TemporarySessionLock::new(
                acquire_session_lock(&lock_path, file).await?,
                lock_path.clone(),
            );
            let (plain, lease) = materialize_with_lease(file, lease).await?;
            let (handle, lock_path) = lease.into_live_parts();
            return Ok(Self::build_live_with_lock_handle(
                &plain,
                meta,
                opts,
                lock_path,
                Some(handle),
                meta.map(|meta| self.writer_projection(&meta.id)),
            )
            .await);
        } else {
            None
        };
        Ok(Self::build_live_with_lock_handle(
            file,
            meta,
            opts,
            lock_path,
            lock_handle,
            meta.map(|meta| self.writer_projection(&meta.id)),
        )
        .await)
    }

    /// Construct a live rollout from an already-owned writer sidecar. The
    /// atomic resume path uses this to transfer the exact lease acquired before
    /// reading history; it must never release and reacquire between those steps.
    async fn build_live_with_lock_handle(
        file: &str,
        meta: Option<&SessionMeta>,
        opts: BuildLiveOptions,
        lock_path: String,
        lock_handle: Option<tokio::fs::File>,
        projection: Option<WriterProjection>,
    ) -> Box<dyn LiveRollout> {
        let use_lock = opts.lock;
        // Funnel all writes through a single ordered queue so append order is
        // preserved.
        let (tx, finished) = spawn_writer(file.to_string(), projection);
        if opts.header {
            if let Some(meta) = meta {
                let header = RolloutItem::SessionMeta {
                    schema_version: ROLLOUT_SCHEMA_VERSION,
                    meta: meta.clone(),
                };
                let _ = tx
                    .send(WriteCmd::Line(format!(
                        "{}\n",
                        serialize_rollout_item(&header)
                    )))
                    .await;
            }
        }

        let live = JsonlLiveRollout {
            writer: Arc::new(StdMutex::new(WriterState {
                inspection_revision: 0,
                tx: Some(tx),
                shutdown: None,
                finished: Some(finished),
            })),
            admission: crate::store::rollout_admission::OrderedRolloutAdmission::default(),
            use_lock,
            lock_path,
            lock_handle: StdMutex::new(lock_handle),
        };

        // Mark the reattach in the rollout so the transcript records the resume.
        if opts.resumed {
            live.append_event("resumed".into(), None).await;
        }
        Box::new(live)
    }

    /// Per-session `subagents/` dir for `thread_id`, derived from its file path.
    ///
    /// The lifecycle serialization this takes is what keeps a rename or an
    /// unarchive from moving the session file (and with it the data dir this
    /// resolves) out from under a caller. Concurrent sub-agent spawns share
    /// that session, so a try-once acquire would refuse one of them and — with
    /// the spawn path treating a refused sidechain as "run without one" —
    /// silently strand that child's whole transcript. Contenders are queued
    /// rather than rejected: this waits for the lock with a bounded
    /// backoff; only a contender that outlives the whole window is refused,
    /// and that refusal now surfaces instead of vanishing.
    async fn require_sub_agent_dir(&self, thread_id: &str) -> io::Result<String> {
        let deadline = tokio::time::Instant::now() + SUBAGENT_DIR_LOCK_WAIT;
        let mut delay = SUBAGENT_DIR_LOCK_RETRY_START;
        let (_operation, _file_lock) = loop {
            if let Some(operation) = self.begin_lifecycle(thread_id) {
                match self.try_lifecycle_file_lock(thread_id).await {
                    Ok(Some(file_lock)) => break (operation, file_lock),
                    Ok(None) => {}
                    Err(error) => return Err(error),
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!(
                        "session {thread_id} lifecycle lock stayed contended for {}ms",
                        SUBAGENT_DIR_LOCK_WAIT.as_millis()
                    ),
                ));
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(SUBAGENT_DIR_LOCK_RETRY_MAX);
        };
        self.recover_unarchive_if_needed_locked(thread_id).await?;
        let file = self
            .locate(thread_id)
            .await?
            .ok_or_else(|| io::Error::other(format!("session {thread_id} not found")))?;
        Ok(sub_agent_dir(&file))
    }

    /// Resolve a thread id to its file path. Consults the in-memory index
    /// first, then scans the live and archived roots on a miss.
    async fn locate(&self, thread_id: &str) -> io::Result<Option<String>> {
        let path = {
            let mut index = self.index.lock().await;
            let cached = index.get(thread_id).map(|entry| entry.path.clone());
            if let Some(path) = cached {
                match compression::resolve(Path::new(&path)) {
                    Ok(path) => Some(path.to_string_lossy().into_owned()),
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {
                        index.delete(thread_id);
                        None
                    }
                    Err(error) => return Err(error),
                }
            } else {
                None
            }
        };
        let path = if path.is_some() {
            path
        } else {
            let mut index = self.index.lock().await;
            index
                .scan_roots_read_only(&[
                    (
                        Path::new(&self.dir),
                        ScanOptions {
                            archived: false,
                            force: true,
                        },
                    ),
                    (
                        Path::new(&self.archive_root),
                        ScanOptions {
                            archived: true,
                            force: true,
                        },
                    ),
                ])
                .await?;
            index.get(thread_id).map(|entry| entry.path.clone())
        };
        if let Some(path) = &path {
            self.recover_pending_path(path).await?;
        }
        Ok(path)
    }
    async fn recover_pending_path(&self, path: &str) -> io::Result<()> {
        let plain = compression::plain_path(Path::new(path));
        if !PathBuf::from(format!("{}.migration.pending", plain.display())).exists() {
            return Ok(());
        }
        let root = PathBuf::from(&self.dir);
        let archive = PathBuf::from(&self.archive_root);
        tokio::task::spawn_blocking(move || recover_one_pending(&root, &archive, &plain))
            .await
            .map_err(io::Error::other)??;
        *self.index.lock().await = HeaderIndex::new();
        *self.profile_memory.lock().await = crate::store::profile_activity::MemoryCache::default();
        self.catalog_snapshots
            .lock()
            .expect("catalog snapshots")
            .clear();
        Ok(())
    }
    async fn recover_pending_migrations(&self) -> io::Result<()> {
        let root = PathBuf::from(&self.dir);
        let archive = PathBuf::from(&self.archive_root);
        let paths =
            tokio::task::spawn_blocking(move || compression::list_rollouts(&root, &archive))
                .await
                .map_err(io::Error::other)??;
        for path in paths {
            self.recover_pending_path(&path.to_string_lossy()).await?;
        }
        Ok(())
    }

    async fn ensure_dir(&self) -> io::Result<()> {
        if self.dir_ensured.load(Ordering::SeqCst) {
            return Ok(());
        }
        tokio::fs::create_dir_all(&self.dir).await?;
        self.dir_ensured.store(true, Ordering::SeqCst);
        Ok(())
    }
}

struct LifecycleOperationGuard<'a> {
    active: &'a StdMutex<HashSet<String>>,
    thread_id: String,
}

impl Drop for LifecycleOperationGuard<'_> {
    fn drop(&mut self) {
        self.active
            .lock()
            .expect("lifecycle operations")
            .remove(&self.thread_id);
    }
}

struct LifecycleFileLock {
    file: std::fs::File,
}

/// Creation may be cancelled while its queued header is being written. Keep
/// the identity lock until that exact writer finishes, and release its PID
/// sidecar even when the caller no longer waits for the creation result.
struct PendingThreadCreate {
    rollout: Option<Box<dyn LiveRollout>>,
    lifecycle_lock: Option<LifecycleFileLock>,
}

impl Drop for PendingThreadCreate {
    fn drop(&mut self) {
        let Some(rollout) = self.rollout.take() else {
            return;
        };
        let lifecycle_lock = self.lifecycle_lock.take();
        tokio::spawn(async move {
            let _ = rollout.shutdown().await;
            drop(lifecycle_lock);
        });
    }
}

struct TemporarySessionLock {
    handle: Option<tokio::fs::File>,
    path: Option<String>,
}

impl TemporarySessionLock {
    fn new(handle: tokio::fs::File, path: String) -> Self {
        Self {
            handle: Some(handle),
            path: Some(path),
        }
    }

    async fn release(mut self) -> io::Result<()> {
        drop(self.handle.take());
        let Some(path) = self.path.take() else {
            return Ok(());
        };
        match tokio::fs::remove_file(path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// Transfer this exact lease into a [`JsonlLiveRollout`]. Both fields are
    /// taken before `Drop`, so the temporary guard cannot unlink a sidecar now
    /// owned by the long-lived writer.
    fn into_live_parts(mut self) -> (tokio::fs::File, String) {
        let handle = self.handle.take().expect("temporary writer lock handle");
        let path = self.path.take().expect("temporary writer lock path");
        (handle, path)
    }
}

impl Drop for TemporarySessionLock {
    fn drop(&mut self) {
        drop(self.handle.take());
        if let Some(path) = self.path.take() {
            // Async cleanup is attempted explicitly on every successful path;
            // this synchronous fallback prevents an early-return/cancellation
            // from leaving a same-process PID lock that would look live.
            let _ = std::fs::remove_file(path);
        }
    }
}

impl Drop for LifecycleFileLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

struct ImportTemporaryFile(PathBuf);

impl Drop for ImportTemporaryFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn import_external_locked(
    directory: &str,
    existing: Option<&str>,
    import: ExternalSessionImport,
    #[cfg(test)] gate: Option<ImportTestGate>,
) -> io::Result<ExternalSessionImportResult> {
    use std::io::{BufRead, Read, Write};
    let prefix = import.prefix();
    if let Some(existing) = existing {
        let mut reader = std::io::BufReader::new(compression::open_seekable(Path::new(existing))?);
        let mut line = Vec::new();
        for (position, expected) in std::iter::once(None)
            .chain(prefix.iter().map(Some))
            .enumerate()
        {
            line.clear();
            // One imported text is at most 1 MiB; JSON escaping can expand it.
            reader
                .by_ref()
                .take(8 * 1024 * 1024 + 1)
                .read_until(b'\n', &mut line)?;
            if line.len() > 8 * 1024 * 1024 || line.last() != Some(&b'\n') {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "existing thread has no identical complete import",
                ));
            }
            let parsed = std::str::from_utf8(&line).ok().and_then(parse_rollout_line);
            let matches = if position == 0 {
                matches!(parsed, Some(RolloutItem::SessionMeta { ref meta, .. }) if meta.id == import.meta.id)
            } else {
                parsed.as_ref() == expected
            };
            if !matches {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "thread identity belongs to different content or provenance",
                ));
            }
        }
        return Ok(ExternalSessionImportResult {
            already_imported: true,
            message_count: import.messages.len(),
        });
    }
    if !import.allow_create {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "thread identity is already registered by a session",
        ));
    }
    let target = Path::new(directory).join(format!(
        "{}-{}.jsonl",
        import.meta.created_at,
        runtime_path_component_to_store_host(&import.meta.id)
    ));
    let temporary = ImportTemporaryFile(
        Path::new(directory).join(format!(".external-import-{}.tmp", uuid::Uuid::new_v4())),
    );
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut output = options.open(&temporary.0)?;
    let header = RolloutItem::SessionMeta {
        schema_version: ROLLOUT_SCHEMA_VERSION,
        meta: import.meta,
    };
    for item in std::iter::once(&header).chain(prefix.iter()) {
        output.write_all(serialize_rollout_item(item).as_bytes())?;
        output.write_all(b"\n")?;
    }
    output.sync_all()?;
    drop(output);
    #[cfg(test)]
    if let Some((entered, release)) = gate {
        entered.notify_one();
        let (released, wake) = &*release;
        let mut released = released.lock().unwrap();
        while !*released {
            released = wake.wait(released).unwrap();
        }
    }
    // Atomic create-without-replacement publishes the complete file. All store
    // readers ignore the .tmp candidate, and a competing filename is untouched.
    std::fs::hard_link(&temporary.0, &target)?;
    // A post-publication barrier error remains an error. An exact retry can
    // verify the complete prefix; never remove a published user thread here.
    sync_directory_blocking(Path::new(directory))?;
    Ok(ExternalSessionImportResult {
        already_imported: false,
        message_count: import.messages.len(),
    })
}

fn unarchive_result(
    thread_id: &str,
    status: UnarchiveSessionStatus,
    thread: Option<ThreadSummary>,
    error: Option<String>,
) -> UnarchiveSessionResult {
    UnarchiveSessionResult {
        thread_id: thread_id.into(),
        status,
        thread,
        error: error.map(RuntimeString::from),
    }
}

fn persistence_failure(
    thread_id: &str,
    phase: &str,
    error: impl std::fmt::Display,
    thread: Option<ThreadSummary>,
) -> UnarchiveSessionResult {
    unarchive_result(
        thread_id,
        UnarchiveSessionStatus::PersistenceFailed,
        thread,
        Some(format!("{phase}: {error}")),
    )
}

#[async_trait]
impl SessionStore for JsonlSessionStore {
    async fn compress_cold_rollouts(&self) -> io::Result<compression::CompressionReport> {
        self.compress_cold_rollouts_with_cancellation(Arc::new(AtomicBool::new(false)))
            .await
    }
    async fn compress_cold_rollouts_with_cancellation(
        &self,
        cancelled: Arc<AtomicBool>,
    ) -> io::Result<compression::CompressionReport> {
        let root = PathBuf::from(&self.dir);
        let archive = PathBuf::from(&self.archive_root);
        let mut cancellation = compression::MaintenanceCancellation::new(cancelled.clone());
        let result =
            tokio::task::spawn_blocking(move || compression::run(&root, &archive, &cancelled))
                .await;
        cancellation.disarm();
        result.map_err(io::Error::other)?
    }
    async fn migrate_rollouts(
        &self,
        request: crate::store::session_store::RolloutMigrationRequest,
        cancelled: Arc<AtomicBool>,
    ) -> io::Result<crate::store::session_store::RolloutMigrationReport> {
        let root = PathBuf::from(&self.dir);
        let archive = PathBuf::from(&self.archive_root);
        let mut cancellation = compression::MaintenanceCancellation::new(cancelled.clone());
        let result = tokio::task::spawn_blocking(move || {
            migrate_store_rollouts(&root, &archive, request, &cancelled)
        })
        .await;
        cancellation.disarm();
        let report = result.map_err(io::Error::other)??;
        if !report.busy {
            *self.index.lock().await = HeaderIndex::new();
            *self.profile_memory.lock().await =
                crate::store::profile_activity::MemoryCache::default();
            self.catalog_snapshots
                .lock()
                .expect("catalog snapshots")
                .clear();
        }
        Ok(report)
    }

    async fn import_external_session(
        &self,
        import: ExternalSessionImport,
    ) -> io::Result<ExternalSessionImportResult> {
        self.ensure_dir().await?;
        let thread_id = runtime_path_component_to_store_host(&import.meta.id);
        let _operation = self.begin_lifecycle(&thread_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                "thread has a concurrent lifecycle operation",
            )
        })?;
        let file_lock = self
            .try_lifecycle_file_lock(&thread_id)
            .await?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "thread has a concurrent lifecycle operation",
                )
            })?;
        // Use the existing recovery and both namespaces while holding the same
        // identity lock as create/resume/archive/delete, including other processes.
        self.recover_unarchive_if_needed_locked(&thread_id).await?;
        let (active, archived) = self.locate_namespaces(&thread_id).await?;
        if active.is_some() && archived.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "thread exists in both namespaces",
            ));
        }
        let existing = active.or(archived).map(|entry| entry.path);
        // A corrupt/oversized header is not permission to reuse the same ID.
        // UUID imports can identify a reserved filename without interpreting
        // its contents; other lifecycle operations keep their existing policy.
        let suffix = format!("-{thread_id}.jsonl");
        for root in [&self.dir, &self.archive_root] {
            let mut entries = match tokio::fs::read_dir(root).await {
                Ok(entries) => entries,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            while let Some(entry) = entries.next_entry().await? {
                if entry
                    .file_name()
                    .to_string_lossy()
                    .strip_suffix(".zst")
                    .unwrap_or(&entry.file_name().to_string_lossy())
                    .ends_with(&suffix)
                    && existing
                        .as_deref()
                        .is_none_or(|known| Path::new(known) != entry.path())
                {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "thread identity has an unrecognized existing rollout",
                    ));
                }
            }
        }
        let directory = self.dir.clone();
        let import_meta = import.meta.clone();
        let imported_path = existing.clone().unwrap_or_else(|| {
            join_path(
                &directory,
                &format!("{}-{thread_id}.jsonl", import_meta.created_at),
            )
        });
        #[cfg(test)]
        let gate = self.import_before_publish.lock().unwrap().clone();
        // The blocking owner retains the advisory lock and temporary-file guard
        // even if the caller disconnects. A retry observes either no file or the
        // complete import; cancelling a future cannot interrupt publication.
        tokio::task::spawn_blocking(move || {
            let _file_lock = file_lock;
            let result = import_external_locked(
                &directory,
                existing.as_deref(),
                import,
                #[cfg(test)]
                gate,
            )?;
            let metadata_path = join_path(&directory, "thread-metadata.sqlite3");
            let db =
                crate::store::thread_metadata::ThreadMetadataDb::open(Path::new(&metadata_path))?;
            if !result.already_imported {
                db.delete(&import_meta.id)?;
            }
            db.seed_if_absent(&import_meta)?;
            let projection = (|| {
                let mut db = crate::store::thread_index::TurnIndexDb::open(&join_path(
                    &directory,
                    "threads-index.sqlite3",
                ))?;
                if !result.already_imported {
                    db.delete_thread(&runtime_path_component_to_store_host(&import_meta.id))?;
                }
                db.materialize_rollout(
                    &runtime_path_component_to_store_host(&import_meta.id),
                    &imported_path,
                )
            })();
            if let Err(error) = projection {
                tracing::warn!(%error,"failed to project imported rollout");
            }
            Ok::<_, io::Error>(result)
        })
        .await
        .map_err(|error| io::Error::other(format!("external import task failed: {error}")))?
    }

    async fn create(&self, meta: SessionMeta) -> io::Result<Box<dyn LiveRollout>> {
        self.ensure_dir().await?;
        let thread_id = runtime_path_component_to_store_host(&meta.id);
        let _operation = self.begin_lifecycle(&thread_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("session {thread_id} has a concurrent lifecycle operation"),
            )
        })?;
        let file_lock = self
            .try_lifecycle_file_lock(&thread_id)
            .await?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!("session {thread_id} has a concurrent lifecycle operation"),
                )
            })?;
        let (active, archived) = self.locate_namespaces(&thread_id).await?;
        if active.is_some() || archived.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("session {thread_id} already exists"),
            ));
        }
        self.delete_saved_authority(&thread_id).await?;
        let file = join_path(&self.dir, &format!("{}-{thread_id}.jsonl", meta.created_at));
        let live = self
            .build_live(
                &file,
                Some(&meta),
                BuildLiveOptions {
                    header: true,
                    resumed: false,
                    lock: true,
                },
            )
            .await?;
        let mut pending = PendingThreadCreate {
            rollout: Some(live),
            lifecycle_lock: Some(file_lock),
        };
        #[cfg(test)]
        {
            let gate = self.create_before_publish.lock().unwrap().clone();
            if let Some((entered, release)) = gate {
                entered.notify_one();
                release.notified().await;
            }
        }
        let live = pending.rollout.as_ref().expect("new rollout remains owned");
        // The stable lifecycle lock protects the identity check through actual
        // header publication, not just enqueueing its asynchronous write.
        if let Err(primary) = live.flush().await {
            let outcome = match live.shutdown().await {
                Ok(()) => Err(primary),
                Err(cleanup) => Err(io::Error::new(
                    primary.kind(),
                    format!("{primary}; failed to close new rollout: {cleanup}"),
                )),
            };
            pending.rollout.take();
            return outcome;
        }
        let mut authority_meta = meta.clone();
        self.overlay_meta(&mut authority_meta).await?;
        self.index.lock().await.set(HeaderEntry {
            meta: meta.clone(),
            path: file.clone(),
            updated_at: meta.created_at,
            archived: false,
        });
        Ok(pending.rollout.take().expect("new rollout remains owned"))
    }

    /// Re-attach a writer to an EXISTING thread so a resumed session keeps
    /// persisting. Appends to the same jsonl (no new header) under the
    /// single-writer lock, recording a `resumed` marker. Pairs with
    /// [`SessionStore::resume`] (which reads the history); call both to resume
    /// read + write.
    async fn reopen(&self, thread_id: &str) -> io::Result<Box<dyn LiveRollout>> {
        let _operation = self.begin_lifecycle(thread_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("session {thread_id} has a concurrent lifecycle operation"),
            )
        })?;
        let _file_lock = self
            .try_lifecycle_file_lock(thread_id)
            .await?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!("session {thread_id} has a concurrent lifecycle operation"),
                )
            })?;
        self.recover_unarchive_if_needed_locked(thread_id).await?;
        let file = self
            .locate(thread_id)
            .await?
            .ok_or_else(|| io::Error::other(format!("session {thread_id} not found")))?;
        let meta = read_file_meta(&file).await?;
        self.build_live(
            &file,
            Some(&meta),
            BuildLiveOptions {
                header: false,
                resumed: true,
                lock: true,
            },
        )
        .await
    }

    async fn paged_thread_history(
        &self,
        thread_id: &str,
        cursor: Option<&str>,
        limit: Option<usize>,
        items_view: Option<&str>,
        sort_direction: Option<&str>,
    ) -> io::Result<Option<crate::store::thread_index::ThreadPageSnapshot>> {
        JsonlSessionStore::paged_thread_history(
            self,
            thread_id,
            cursor,
            limit,
            items_view,
            sort_direction,
        )
        .await
    }

    async fn paged_turn_slice(
        &self,
        thread_id: &str,
        turn_id: &str,
        cursor: Option<&str>,
        limit: Option<usize>,
        sort_direction: Option<&str>,
    ) -> io::Result<Option<crate::store::thread_index::ThreadPageSnapshot>> {
        JsonlSessionStore::paged_turn_slice(self, thread_id, turn_id, cursor, limit, sort_direction)
            .await
    }

    async fn read_thread(&self, thread_id: &str) -> io::Result<ThreadReadSnapshot> {
        // The opened file is the snapshot boundary. Fix its byte length before
        // reading so concurrent appends cannot make this request chase a live turn.
        // A lifecycle move racing open is retried through the canonical index;
        // no recovery, archive move, or writer acquisition belongs in this path.
        let mut last_error = None;
        for _ in 0..2 {
            let path = self.locate(thread_id).await?.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("session {thread_id} not found"),
                )
            })?;
            let file = match compression::open_async(&path).await {
                Ok(file) => file,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    last_error = Some(error);
                    continue;
                }
                Err(error) => return Err(error),
            };
            let metadata = file.metadata().await?;
            let updated_at = metadata
                .modified()
                .ok()
                .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|time| time.as_millis().min(i64::MAX as u128) as i64)
                .unwrap_or_default();
            let mut bytes = Vec::new();
            file.take(metadata.len()).read_to_end(&mut bytes).await?;
            let raw_items = parse_items(&bytes);
            let mut meta = raw_items
                .iter()
                .find_map(|item| match item {
                    RolloutItem::SessionMeta { meta, .. } => Some(meta.clone()),
                    _ => None,
                })
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("session {thread_id} has no session_meta header"),
                    )
                })?;
            if meta.id != thread_id {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "thread header id mismatch",
                ));
            }
            self.read_authority_meta(&mut meta).await?;
            return Ok(ThreadReadSnapshot {
                meta,
                raw_items,
                updated_at,
                archived: Path::new(&path).starts_with(&self.archive_root),
            });
        }
        Err(last_error
            .unwrap_or_else(|| io::Error::new(io::ErrorKind::NotFound, "thread disappeared")))
    }

    async fn read_sub_agent(
        &self,
        thread_id: &str,
        agent_id: &str,
    ) -> io::Result<crate::store::session_store::SubAgentReadSnapshot> {
        // Resolve from the saved parent path without the lifecycle lock/recovery
        // used by continuation reads. A rename race gets the same bounded retry
        // as read_thread; neither archived parents nor child files are moved.
        for _ in 0..2 {
            let path = self.locate(thread_id).await?.ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "parent thread not found")
            })?;
            let header = match compression::read_prefix(&path, 64 * 1024).await {
                Ok(bytes) => bytes
                    .split(|byte| *byte == b'\n')
                    .next()
                    .unwrap_or_default()
                    .to_vec(),
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            let line = NodeBufferUtf8::decode(&header)
                .try_to_utf8()
                .expect("Node UTF-8 decode yields scalar header text");
            if !matches!(parse_rollout_line(line.trim()), Some(RolloutItem::SessionMeta { meta, .. }) if meta.id == thread_id)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "parent thread header id mismatch",
                ));
            }
            let dir = sub_agent_dir(&path);
            let child = agent_file_path(&dir, agent_id);
            self.recover_pending_path(&child).await?;
            let raw_items = match compression::open_async(&child).await {
                Ok(file) => {
                    let length = file.metadata().await?.len();
                    let mut bytes = Vec::new();
                    file.take(length).read_to_end(&mut bytes).await?;
                    parse_items(&bytes)
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    // A missing child is a valid empty transcript only while
                    // its parent still exists at this location.
                    if !tokio::fs::try_exists(&path).await? {
                        continue;
                    }
                    Vec::new()
                }
                Err(error) => return Err(error),
            };
            let meta = read_sub_agent_meta(&agent_meta_path(&dir, agent_id)).await;
            if !tokio::fs::try_exists(&path).await? {
                continue;
            }
            let surfaced_tools = surfaced_tools_from_items(&raw_items);
            return Ok(crate::store::session_store::SubAgentReadSnapshot {
                raw_items,
                meta,
                surfaced_tools,
            });
        }
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            "parent thread moved during history read",
        ))
    }

    async fn read_messages_by_ids(
        &self,
        thread_id: &str,
        ids: &[RuntimeString],
    ) -> io::Result<Vec<Message>> {
        let file = self.locate(thread_id).await?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("session {thread_id} not found"),
            )
        })?;
        let thread_id = thread_id.to_string();
        let ids = ids.to_vec();
        let cancellation = crate::store::runtime_resume::ReadCancellation::new();
        let cancelled = cancellation.0.clone();
        tokio::task::spawn_blocking(move || {
            crate::store::runtime_resume::read_messages(&file, &thread_id, &ids, &cancelled)
        })
        .await
        .map_err(io::Error::other)?
    }

    async fn resume(&self, thread_id: &str) -> io::Result<ResumeResult> {
        let _operation = self.begin_lifecycle(thread_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("session {thread_id} has a concurrent lifecycle operation"),
            )
        })?;
        let _file_lock = self
            .try_lifecycle_file_lock(thread_id)
            .await?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!("session {thread_id} has a concurrent lifecycle operation"),
                )
            })?;
        self.recover_unarchive_if_needed_locked(thread_id).await?;
        let file = self
            .locate(thread_id)
            .await?
            .ok_or_else(|| io::Error::other(format!("session {thread_id} not found")))?;

        let items = load_items(&file).await?;

        let mut resume = resume_result_from_items(thread_id, items)?;
        self.read_authority_meta(&mut resume.meta).await?;
        Ok(resume)
    }

    async fn resume_for_write(&self, thread_id: &str) -> io::Result<ResumedLiveRollout> {
        let _operation = self.begin_lifecycle(thread_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("session {thread_id} has a concurrent lifecycle operation"),
            )
        })?;
        let _file_lock = self
            .try_lifecycle_file_lock(thread_id)
            .await?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!("session {thread_id} has a concurrent lifecycle operation"),
                )
            })?;
        self.recover_unarchive_if_needed_locked(thread_id).await?;
        let file = self
            .locate(thread_id)
            .await?
            .ok_or_else(|| io::Error::other(format!("session {thread_id} not found")))?;

        // The writer lease is the read linearization point. Once acquired, no
        // other continuation can append between this authoritative load and the
        // returned live writer. Temporary ownership guarantees every `?` and
        // cancellation path below removes the sidecar.
        let lock_path = compression::writer_lock_path(&file);
        let writer_lock =
            TemporarySessionLock::new(acquire_session_lock(&lock_path, &file).await?, lock_path);
        resume_for_write_test_pause(&file).await;
        let items = load_items(&file).await?;
        let mut resume = resume_result_from_items(thread_id, items)?;
        self.read_authority_meta(&mut resume.meta).await?;
        let (file, writer_lock) = materialize_with_lease(&file, writer_lock).await?;

        // No await is permitted after transfer: the returned rollout becomes
        // owner in this same poll, closing cancellation between lease handoff
        // and result publication.
        let (lock_handle, lock_path) = writer_lock.into_live_parts();
        let rollout = Self::build_live_with_lock_handle(
            &file,
            None,
            BuildLiveOptions {
                header: false,
                resumed: true,
                lock: true,
            },
            lock_path,
            Some(lock_handle),
            Some(self.writer_projection(&resume.meta.id)),
        )
        .await;
        Ok(ResumedLiveRollout { resume, rollout })
    }

    async fn resume_runtime_for_write(
        &self,
        thread_id: &str,
    ) -> io::Result<crate::store::session_store::ResumedRuntimeRollout> {
        let _operation = self.begin_lifecycle(thread_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("session {thread_id} has a concurrent lifecycle operation"),
            )
        })?;
        let _file_lock = self
            .try_lifecycle_file_lock(thread_id)
            .await?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!("session {thread_id} has a concurrent lifecycle operation"),
                )
            })?;
        self.recover_unarchive_if_needed_locked(thread_id).await?;
        let file = self
            .locate(thread_id)
            .await?
            .ok_or_else(|| io::Error::other(format!("session {thread_id} not found")))?;

        // The writer lease is the read linearization point. Once acquired, no
        // other continuation can append between this authoritative load and the
        // returned live writer. Temporary ownership guarantees every `?` and
        // cancellation path below removes the sidecar.
        let lock_path = compression::writer_lock_path(&file);
        let writer_lock =
            TemporarySessionLock::new(acquire_session_lock(&lock_path, &file).await?, lock_path);
        resume_for_write_test_pause(&file).await;
        let cancellation = crate::store::runtime_resume::ReadCancellation::new();
        let cancelled = cancellation.0.clone();
        let scan_file = file.clone();
        let scan_thread = thread_id.to_string();
        let (mut resume, writer_lock, file) = tokio::task::spawn_blocking(move || {
            // The blocking owner retains the lease even if its caller is
            // cancelled; the scanner observes cancellation before releasing it.
            let result = crate::store::runtime_resume::load(&scan_file, &scan_thread, &cancelled);
            let resume = result?;
            let plain = compression::materialize(Path::new(&scan_file), &cancelled)?;
            Ok::<_, io::Error>((resume, writer_lock, plain.to_string_lossy().into_owned()))
        })
        .await
        .map_err(io::Error::other)??;
        self.read_authority_meta(&mut resume.meta).await?;

        // No await is permitted after transfer: the returned rollout becomes
        // owner in this same poll, closing cancellation between lease handoff
        // and result publication.
        let (lock_handle, lock_path) = writer_lock.into_live_parts();
        let rollout = Self::build_live_with_lock_handle(
            &file,
            None,
            BuildLiveOptions {
                header: false,
                resumed: true,
                lock: true,
            },
            lock_path,
            Some(lock_handle),
            Some(self.writer_projection(&resume.meta.id)),
        )
        .await;
        Ok(crate::store::session_store::ResumedRuntimeRollout { resume, rollout })
    }

    async fn list(&self, options: ListOptions) -> io::Result<ThreadPage> {
        self.recover_pending_migrations().await?;
        let limit = options
            .limit
            .filter(|limit| *limit > 0)
            .unwrap_or(DEFAULT_PAGE_LIMIT);
        let filter = catalog_filter(&options);
        let cursor = options
            .cursor
            .as_ref()
            .map(decode_catalog_cursor)
            .transpose()?;
        if let Some(CatalogCursor::Snapshot { id, offset }) = &cursor {
            let mut snapshots = self.catalog_snapshots.lock().expect("catalog snapshots");
            snapshots.retain(|snapshot| snapshot.created.elapsed() < CATALOG_SNAPSHOT_TTL);
            let snapshot = snapshots
                .iter()
                .find(|snapshot| &snapshot.id == id)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "session catalog cursor expired; restart without cursor",
                    )
                })?;
            if snapshot.filter != filter || *offset > snapshot.rows.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "session catalog cursor does not match query",
                ));
            }
            return Ok(snapshot.page(*offset, limit));
        }
        self.recover_pending_unarchives().await?;
        let index = &mut *self.index.lock().await;
        let mut roots = vec![(
            Path::new(&self.dir),
            ScanOptions {
                archived: false,
                force: true,
            },
        )];
        if options.include_archived == Some(true) {
            roots.push((
                Path::new(&self.archive_root),
                ScanOptions {
                    archived: true,
                    force: true,
                },
            ));
        }
        index.scan_roots(&roots).await?;

        let mut entries = index.entries().into_iter().cloned().collect::<Vec<_>>();
        let metadata_path = join_path(&self.dir, "thread-metadata.sqlite3");
        entries = tokio::task::spawn_blocking(move || {
            let db =
                crate::store::thread_metadata::ThreadMetadataDb::open(Path::new(&metadata_path))?;
            for entry in &mut entries {
                // Catalog observation must not make an externally replaced
                // legacy header authoritative. Writer ownership seeds metadata.
                if let Some(authority) = db.get(&entry.meta.id)? {
                    entry.meta = authority;
                }
            }
            Ok::<_, io::Error>(entries)
        })
        .await
        .map_err(io::Error::other)??;
        if options.include_archived != Some(true) {
            entries.retain(|e| !e.archived);
        }

        let requested_role = options
            .role
            .unwrap_or(crate::schemas::rollout::SessionRole::Main);
        let mut summaries = Vec::new();
        for entry in entries {
            let summary = entry_to_summary(&entry);
            if summary.role == requested_role {
                summaries.push(summary);
            }
        }

        // Filter resume candidates by cwd: keep only
        // sessions whose recorded cwd matches the requested one, path-normalized.
        if let Some(cwd) = &options.cwd {
            let target = resolve_path(&runtime_path_component_to_store_host(
                cwd.as_runtime_string(),
            ));
            summaries
                .retain(|s| resolve_path(&runtime_path_component_to_store_host(&s.cwd)) == target);
        }

        // Legacy keyset cursors are accepted explicitly, then upgraded to a
        // coherent snapshot. New cursors never rescan or silently skip rows if
        // another process updates, archives, or deletes files between pages.
        let start = match &cursor {
            Some(CatalogCursor::Legacy(cursor)) => index_after_cursor(&summaries, cursor),
            _ => 0,
        };
        let snapshot = CatalogSnapshot {
            id: uuid::Uuid::new_v4().to_string(),
            membership: catalog_membership(&summaries),
            metadata: index.metadata_fingerprint(&summaries),
            rows: Arc::new(summaries),
            filter,
            created: Instant::now(),
        };
        let page = snapshot.page(start, limit);
        if page.next_cursor.is_some() {
            let mut snapshots = self.catalog_snapshots.lock().expect("catalog snapshots");
            snapshots.retain(|snapshot| snapshot.created.elapsed() < CATALOG_SNAPSHOT_TTL);
            while snapshots.len() >= CATALOG_SNAPSHOT_LIMIT {
                snapshots.pop_front();
            }
            snapshots.push_back(snapshot);
        }
        Ok(page)
    }

    async fn read_session_summary(&self, thread_id: &str) -> io::Result<Option<ThreadSummary>> {
        let index = &mut *self.index.lock().await;
        let roots = [
            (Path::new(&self.dir), false),
            (Path::new(&self.archive_root), true),
        ];
        if let Some(entry) = index.read_known(thread_id, &roots).await? {
            let mut summary = entry_to_summary(&entry);
            self.overlay_summary(&mut summary).await?;
            return Ok(Some(summary));
        }
        index
            .scan_roots_read_only(&roots.map(|(path, archived)| {
                (
                    path,
                    ScanOptions {
                        archived,
                        force: true,
                    },
                )
            }))
            .await?;
        let mut summary = index.get(thread_id).map(entry_to_summary);
        if let Some(value) = summary.as_mut() {
            self.overlay_summary(value).await?;
        }
        Ok(summary)
    }

    async fn profile_activity(
        &self,
        now_ms: i64,
    ) -> io::Result<crate::store::profile_activity::ProfileActivity> {
        self.recover_pending_migrations().await?;
        let mut memory = self.profile_memory.lock().await;
        crate::store::profile_activity::read_profile_activity(
            &mut memory,
            &[Path::new(&self.dir), Path::new(&self.archive_root)],
            now_ms,
        )
        .await
    }

    async fn search(
        &self,
        options: &SessionSearchOptions,
        accel: Option<&crate::runtime_process::RuntimeProcessContext>,
        signal: &crate::types::abort::AbortSignal,
    ) -> io::Result<SessionSearchPage> {
        let query = options.query.try_to_utf8().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "session search query must be Unicode text",
            )
        })?;
        if query.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "session search query must not be empty",
            ));
        }
        let limit = options.limit.unwrap_or(DEFAULT_SEARCH_LIMIT);
        if limit == 0 {
            return Ok(SessionSearchPage::default());
        }
        let include_archived = options.include_archived == Some(true);

        // Snapshot the header index with the same visibility refresh as `list`,
        // then release the lock before any file scanning.
        let mut entries: Vec<(ThreadSummary, String)> = {
            let index = &mut *self.index.lock().await;
            index
                .scan(
                    Path::new(&self.dir),
                    ScanOptions {
                        archived: false,
                        force: true,
                    },
                )
                .await?;
            if include_archived {
                index
                    .scan(
                        Path::new(&self.archive_root),
                        ScanOptions {
                            archived: true,
                            force: true,
                        },
                    )
                    .await?;
            }
            index
                .entries()
                .into_iter()
                .filter(|e| include_archived || !e.archived)
                .map(|e| (entry_to_summary(e), e.path.clone()))
                .collect()
        };
        entries.sort_by(|a, b| b.0.created_at.cmp(&a.0.created_at));

        // Optional ripgrep candidate pass, per scanned root. A partially
        // unavailable accelerator degrades the WHOLE selection to a full scan
        // so archived and active sessions stay consistently covered.
        let mut candidates: Option<HashSet<String>> = None;
        if let Some(ctx) = accel {
            let mut names = ripgrep_candidate_names(ctx, &self.dir, &query, signal).await?;
            if include_archived {
                match (
                    &mut names,
                    ripgrep_candidate_names(ctx, &self.archive_root, &query, signal).await?,
                ) {
                    (Some(all), Some(archived)) => all.extend(archived),
                    _ => names = None,
                }
            }
            candidates = names;
        }

        let mut items = Vec::new();
        for (thread, path) in entries {
            if items.len() >= limit {
                break;
            }
            if signal.is_cancelled() {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "session search aborted",
                ));
            }
            if let Some(names) = &candidates {
                if !path.ends_with(".jsonl.zst") && !names.contains(file_name_of(&path)) {
                    continue;
                }
            }
            match scan_rollout_file(&path, &query).await {
                Ok(Some(found)) => items.push(SessionSearchHit {
                    thread,
                    role: found.role.into(),
                    snippet: found.snippet.into(),
                    matches: found.matches,
                }),
                Ok(None) => {}
                // A session deleted or archived mid-search is skipped, not fatal.
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        Ok(SessionSearchPage { items })
    }

    async fn archive(&self, thread_id: &str) -> io::Result<String> {
        self.recover_pending_migrations().await?;
        let _operation = self.begin_lifecycle(thread_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("session {thread_id} has a concurrent lifecycle operation"),
            )
        })?;
        let _lifecycle_file_lock =
            self.try_lifecycle_file_lock(thread_id)
                .await?
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::WouldBlock,
                        format!("session {thread_id} has a concurrent lifecycle operation"),
                    )
                })?;
        self.recover_unarchive_if_needed_locked(thread_id).await?;

        // Keep list/cache readers behind the complete sidechain-first move.
        let mut index = self.index.lock().await;
        let (active, archived) = self.locate_namespaces(thread_id).await?;
        let (mut current_entry, already_archived) = match (active, archived) {
            (Some(active), None) => (active, false),
            (None, Some(archived)) => (archived, true),
            (None, None) => {
                index.delete(thread_id);
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("session {thread_id} not found"),
                ));
            }
            (Some(_), Some(_)) => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("session {thread_id} has both active and archived rollouts"),
                ))
            }
        };
        self.overlay_meta(&mut current_entry.meta).await?;
        let current = current_entry.path.clone();
        let source_lock_path = compression::writer_lock_path(&current);
        let writer_lock = TemporarySessionLock::new(
            acquire_session_lock(&source_lock_path, &current).await?,
            source_lock_path,
        );
        let (current, writer_lock) = materialize_with_lease(&current, writer_lock).await?;
        ensure_inactive_children(&current).await?;
        current_entry.path = current.clone();

        tokio::fs::create_dir_all(&self.archive_root).await?;
        let file_name = base_name(&current).to_string();
        if already_archived {
            // Repair archives created by older builds that moved only the JSONL.
            let old_live_file = join_path(&self.dir, &file_name);
            let old_live_data = session_data_dir(&old_live_file);
            let archived_data = session_data_dir(&current);
            let live_data_exists = path_exists(&old_live_data).await?;
            let archived_data_exists = path_exists(&archived_data).await?;
            if live_data_exists && archived_data_exists {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("session {thread_id} has both active and archived sidechain trees"),
                ));
            }
            if live_data_exists {
                tokio::fs::rename(&old_live_data, &archived_data).await?;
            }
            index.set(current_entry);
            writer_lock.release().await?;
            return Ok(current);
        }

        let dest = join_path(&self.archive_root, &file_name);
        if path_exists(&dest).await? {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("archive rollout destination {dest} already exists"),
            ));
        }
        let source_data = session_data_dir(&current);
        let dest_data = session_data_dir(&dest);
        let source_data_exists = path_exists(&source_data).await?;
        let dest_data_exists = path_exists(&dest_data).await?;
        if source_data_exists && dest_data_exists {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("session {thread_id} has both active and archived sidechain trees"),
            ));
        }
        let moved_data = if source_data_exists {
            tokio::fs::rename(&source_data, &dest_data).await?;
            true
        } else {
            false
        };
        if let Err(move_file_error) = tokio::fs::rename(&current, &dest).await {
            if moved_data {
                if let Err(rollback_error) = tokio::fs::rename(&dest_data, &source_data).await {
                    return Err(io::Error::other(format!(
                        "failed to archive session rollout: {move_file_error}; failed to roll back session data tree: {rollback_error}"
                    )));
                }
            }
            return Err(move_file_error);
        }

        // Re-key the index before releasing list readers.
        index.set(HeaderEntry {
            meta: current_entry.meta,
            path: dest.clone(),
            updated_at: current_entry.updated_at,
            archived: true,
        });
        writer_lock.release().await?;
        Ok(dest)
    }

    async fn unarchive(&self, thread_id: &str) -> UnarchiveSessionResult {
        if let Err(error) = self.recover_pending_migrations().await {
            return persistence_failure(
                thread_id,
                "failed to recover rollout migration indexes",
                error,
                None,
            );
        }
        let Some(_operation) = self.begin_lifecycle(thread_id) else {
            return unarchive_result(thread_id, UnarchiveSessionStatus::Concurrent, None, None);
        };
        let _lifecycle_file_lock = match self.try_lifecycle_file_lock(thread_id).await {
            Ok(Some(lock)) => lock,
            Ok(None) => {
                let archived = self
                    .locate_namespaces(thread_id)
                    .await
                    .ok()
                    .and_then(|(_, archived)| archived)
                    .as_ref()
                    .map(entry_to_summary);
                return unarchive_result(
                    thread_id,
                    UnarchiveSessionStatus::Concurrent,
                    archived,
                    None,
                );
            }
            Err(error) => {
                return persistence_failure(
                    thread_id,
                    "failed to acquire session lifecycle lock",
                    error,
                    None,
                )
            }
        };

        if let Err(error) = self.recover_unarchive_if_needed_locked(thread_id).await {
            return persistence_failure(
                thread_id,
                "failed to recover an interrupted unarchive transaction",
                error,
                None,
            );
        }

        // Re-check disk after taking the cross-process lock because another
        // store may have restored the rollout between admission and lock
        // acquisition.
        let (mut active, mut archived) = match self.locate_namespaces(thread_id).await {
            Ok(namespaces) => namespaces,
            Err(error) => {
                return persistence_failure(
                    thread_id,
                    "failed to revalidate session namespaces",
                    error,
                    None,
                )
            }
        };
        for entry in [&mut active, &mut archived].into_iter().flatten() {
            if let Err(error) = self.overlay_meta(&mut entry.meta).await {
                return persistence_failure(
                    thread_id,
                    "failed to read thread metadata",
                    error,
                    None,
                );
            }
        }
        let mut archived_entry = match (active, archived) {
            (Some(active), None) => {
                self.index.lock().await.set(active.clone());
                return unarchive_result(
                    thread_id,
                    UnarchiveSessionStatus::AlreadyActive,
                    Some(entry_to_summary(&active)),
                    None,
                );
            }
            (None, None) => {
                self.index.lock().await.delete(thread_id);
                return unarchive_result(thread_id, UnarchiveSessionStatus::Missing, None, None);
            }
            (Some(active), Some(_)) => {
                return persistence_failure(
                    thread_id,
                    "session namespace collision",
                    "both active and archived rollouts exist",
                    Some(entry_to_summary(&active)),
                )
            }
            (None, Some(archived)) => archived,
        };
        let archived_summary = entry_to_summary(&archived_entry);
        let source = archived_entry.path.clone();
        let source_lock_path = compression::writer_lock_path(&source);
        let writer_lock = match acquire_session_lock(&source_lock_path, &source).await {
            Ok(handle) => TemporarySessionLock::new(handle, source_lock_path),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                self.index.lock().await.set(archived_entry);
                return unarchive_result(
                    thread_id,
                    UnarchiveSessionStatus::Running,
                    Some(archived_summary),
                    None,
                );
            }
            Err(error) => {
                self.index.lock().await.set(archived_entry);
                return persistence_failure(
                    thread_id,
                    "failed to acquire archived rollout writer lock",
                    error,
                    Some(archived_summary),
                );
            }
        };

        let (source, writer_lock) = match materialize_with_lease(&source, writer_lock).await {
            Ok(value) => value,
            Err(error) => {
                return persistence_failure(
                    thread_id,
                    "failed to restore archived rollout representation",
                    error,
                    Some(archived_summary),
                )
            }
        };
        archived_entry.path = source.clone();
        if let Err(error) = ensure_inactive_children(&source).await {
            return if error.kind() == io::ErrorKind::WouldBlock {
                unarchive_result(
                    thread_id,
                    UnarchiveSessionStatus::Running,
                    Some(archived_summary),
                    None,
                )
            } else {
                persistence_failure(
                    thread_id,
                    "failed to inspect child writer leases",
                    error,
                    Some(archived_summary),
                )
            };
        }

        if let Err(error) = tokio::fs::create_dir_all(&self.dir).await {
            self.index.lock().await.set(archived_entry);
            return persistence_failure(
                thread_id,
                "failed to create active session root",
                error,
                Some(archived_summary),
            );
        }
        self.dir_ensured.store(true, Ordering::SeqCst);

        let file_name = base_name(&source).to_string();
        let dest = join_path(&self.dir, &file_name);
        match path_exists(&dest).await {
            Ok(true) => {
                self.index.lock().await.set(archived_entry);
                return persistence_failure(
                    thread_id,
                    "active rollout destination collision",
                    format!("{dest} already exists"),
                    Some(archived_summary),
                );
            }
            Ok(false) => {}
            Err(error) => {
                self.index.lock().await.set(archived_entry);
                return persistence_failure(
                    thread_id,
                    "failed to inspect active rollout destination",
                    error,
                    Some(archived_summary),
                );
            }
        }

        let source_data = session_data_dir(&source);
        let dest_data = session_data_dir(&dest);
        let source_data_exists = match path_exists(&source_data).await {
            Ok(exists) => exists,
            Err(error) => {
                self.index.lock().await.set(archived_entry);
                return persistence_failure(
                    thread_id,
                    "failed to inspect archived session data tree",
                    error,
                    Some(archived_summary),
                );
            }
        };
        let dest_data_exists = match path_exists(&dest_data).await {
            Ok(exists) => exists,
            Err(error) => {
                self.index.lock().await.set(archived_entry);
                return persistence_failure(
                    thread_id,
                    "failed to inspect active session data tree",
                    error,
                    Some(archived_summary),
                );
            }
        };
        if source_data_exists && dest_data_exists {
            self.index.lock().await.set(archived_entry);
            return persistence_failure(
                thread_id,
                "session data namespace collision",
                "both active and archived sidechain trees exist",
                Some(archived_summary),
            );
        }

        let intent = UnarchiveIntent {
            version: UNARCHIVE_JOURNAL_VERSION,
            operation: "unarchive".to_string(),
            thread_id: thread_id.to_string(),
            file_name: file_name.clone(),
            data_expected: source_data_exists || dest_data_exists,
        };
        let journal_dir = match self.write_unarchive_intent(&intent).await {
            Ok(journal_dir) => journal_dir,
            Err(error) => {
                self.index.lock().await.set(archived_entry);
                return persistence_failure(
                    thread_id,
                    "failed to persist unarchive intent before namespace mutation",
                    error,
                    Some(archived_summary),
                );
            }
        };
        unarchive_test_pause(thread_id, UnarchiveTestPoint::IntentDurable).await;

        // Every error after durable intent is reconciled from disk, never by a
        // Drop-only rollback. Main-last visibility decides rollback vs commit.
        macro_rules! settle_unarchive_error {
            ($phase:literal, $error:expr) => {{
                let primary = $error.to_string();
                match self.recover_unarchive_if_needed_locked(thread_id).await {
                    Ok(Some(UnarchiveRecoveryState::Active)) => {
                        let active_entry = HeaderEntry {
                            meta: archived_entry.meta.clone(),
                            path: dest.clone(),
                            updated_at: archived_entry.updated_at,
                            archived: false,
                        };
                        let summary = entry_to_summary(&active_entry);
                        self.index.lock().await.set(active_entry);
                        return unarchive_result(
                            thread_id,
                            UnarchiveSessionStatus::Restored,
                            Some(summary),
                            None,
                        );
                    }
                    Ok(_) => {
                        self.index.lock().await.set(archived_entry.clone());
                        return persistence_failure(
                            thread_id,
                            $phase,
                            primary,
                            Some(archived_summary.clone()),
                        );
                    }
                    Err(recovery_error) => {
                        self.index.lock().await.delete(thread_id);
                        return persistence_failure(
                            thread_id,
                            $phase,
                            format!("{primary}; durable recovery also failed: {recovery_error}"),
                            None,
                        );
                    }
                }
            }};
        }

        // Move sidechains first. The active main rollout is the list/resume
        // visibility point, so readers can never see an active thread whose
        // sidechain history still lives only in the archive namespace.
        if source_data_exists {
            if let Err(error) = tokio::fs::rename(&source_data, &dest_data).await {
                settle_unarchive_error!("failed to restore session data tree", error);
            }
            // This boundary intentionally precedes parent fsync: cancellation
            // here models a process crash after the directory entry changed but
            // before the rename became durably ordered.
            unarchive_test_pause(thread_id, UnarchiveTestPoint::DataRenamed).await;
            if let Err(error) = sync_unarchive_rename_parents(&source_data, &dest_data).await {
                settle_unarchive_error!("failed to durably restore session data tree", error);
            }
        } else {
            unarchive_test_pause(thread_id, UnarchiveTestPoint::DataRenamed).await;
        }
        if let Err(error) = self
            .mark_unarchive_phase(&journal_dir, UNARCHIVE_DATA_MOVED_MARKER)
            .await
        {
            settle_unarchive_error!("failed to persist unarchive data phase", error);
        }
        unarchive_test_pause(thread_id, UnarchiveTestPoint::DataPhaseDurable).await;

        if let Err(move_file_error) = move_unarchive_main(&source, &dest).await {
            settle_unarchive_error!("unarchive transaction failed", move_file_error);
        }
        // Main is the commit record, so the crash point must be before its
        // parent directories are synced rather than after the durability proof.
        unarchive_test_pause(thread_id, UnarchiveTestPoint::MainRenamed).await;
        if let Err(error) = sync_unarchive_rename_parents(&source, &dest).await {
            settle_unarchive_error!("failed to durably restore session rollout", error);
        }
        if let Err(error) = self
            .mark_unarchive_phase(&journal_dir, UNARCHIVE_MAIN_MOVED_MARKER)
            .await
        {
            settle_unarchive_error!("failed to persist unarchive main phase", error);
        }
        unarchive_test_pause(thread_id, UnarchiveTestPoint::MainPhaseDurable).await;
        if let Err(error) = self.clear_unarchive_journal(&journal_dir).await {
            settle_unarchive_error!("failed to clear completed unarchive journal", error);
        }

        let active_entry = HeaderEntry {
            meta: archived_entry.meta,
            path: dest,
            updated_at: archived_entry.updated_at,
            archived: false,
        };
        let active_summary = entry_to_summary(&active_entry);
        self.index.lock().await.set(active_entry);

        if let Err(error) = writer_lock.release().await {
            return persistence_failure(
                thread_id,
                "restored rollout but failed to remove temporary writer lock",
                error,
                Some(active_summary),
            );
        }

        unarchive_result(
            thread_id,
            UnarchiveSessionStatus::Restored,
            Some(active_summary),
            None,
        )
    }

    async fn delete(&self, thread_id: &str) -> io::Result<()> {
        self.recover_pending_migrations().await?;
        let _operation = self.begin_lifecycle(thread_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("session {thread_id} has a concurrent lifecycle operation"),
            )
        })?;
        let _file_lock = self
            .try_lifecycle_file_lock(thread_id)
            .await?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!("session {thread_id} has a concurrent lifecycle operation"),
                )
            })?;
        self.recover_unarchive_if_needed_locked(thread_id).await?;
        let current = self
            .locate(thread_id)
            .await?
            .ok_or_else(|| io::Error::other(format!("session {thread_id} not found")))?;
        let writer_lock_path = compression::writer_lock_path(&current);
        let writer_lock = TemporarySessionLock::new(
            acquire_session_lock(&writer_lock_path, &current).await?,
            writer_lock_path,
        );
        let (current, writer_lock) = materialize_with_lease(&current, writer_lock).await?;
        ensure_inactive_children(&current).await?;
        let sidechain_dir = current
            .strip_suffix(".jsonl")
            .unwrap_or(&current)
            .to_string();
        match tokio::fs::remove_file(&current).await {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
        match tokio::fs::remove_dir_all(sidechain_dir).await {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
        match tokio::fs::remove_dir_all(background_process_output_dir(&self.dir, thread_id)).await {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
        self.index.lock().await.delete(thread_id);
        self.delete_saved_authority(thread_id).await?;
        writer_lock.release().await
    }

    async fn rename(&self, thread_id: &str, new_thread_id: &str) -> io::Result<()> {
        self.recover_pending_migrations().await?;
        let new_id = new_thread_id.trim();
        if new_id.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "new thread id must be non-empty",
            ));
        }
        if new_id == thread_id {
            return Ok(());
        }
        // Serialize against BOTH ids: the source must not be mid-lifecycle and
        // the destination id must not be claimed by a concurrent operation.
        let _source_op = self.begin_lifecycle(thread_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("session {thread_id} has a concurrent lifecycle operation"),
            )
        })?;
        let _dest_op = self.begin_lifecycle(new_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("session {new_id} has a concurrent lifecycle operation"),
            )
        })?;
        let _source_file_lock =
            self.try_lifecycle_file_lock(thread_id)
                .await?
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::WouldBlock,
                        format!("session {thread_id} has a concurrent lifecycle operation"),
                    )
                })?;
        let _dest_file_lock = self.try_lifecycle_file_lock(new_id).await?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("session {new_id} has a concurrent lifecycle operation"),
            )
        })?;
        self.recover_unarchive_if_needed_locked(thread_id).await?;

        // Keep list/cache readers behind the complete re-key.
        let mut index = self.index.lock().await;
        let (active, archived) = self.locate_namespaces(thread_id).await?;
        let entry = match (active, archived) {
            (Some(active), None) => active,
            (None, Some(archived)) => archived,
            (None, None) => {
                index.delete(thread_id);
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("session {thread_id} not found"),
                ));
            }
            (Some(_), Some(_)) => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("session {thread_id} has both active and archived rollouts"),
                ))
            }
        };
        let (new_active, new_archived) = self.locate_namespaces(new_id).await?;
        if new_active.is_some() || new_archived.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("rename destination thread id {new_id} already exists"),
            ));
        }
        let current = entry.path.clone();
        let writer_lock_path = compression::writer_lock_path(&current);
        let writer_lock = TemporarySessionLock::new(
            acquire_session_lock(&writer_lock_path, &current).await?,
            writer_lock_path,
        );
        let (current, writer_lock) = materialize_with_lease(&current, writer_lock).await?;
        ensure_inactive_children(&current).await?;

        // Rewrite ONLY the header line with the new id; the transcript bytes
        // are copied verbatim. The rewritten copy lands under the destination
        // name first, so a crash between steps leaves both ids readable and
        // never a half-written source.
        let bytes = compression::read(&current).await?;
        let (first_bytes, rest_bytes) = match bytes.iter().position(|byte| *byte == b'\n') {
            Some(position) => (&bytes[..position], &bytes[position + 1..]),
            None => (&bytes[..], &[][..]),
        };
        let first_line = NodeBufferUtf8::decode(first_bytes)
            .try_to_utf8()
            .expect("Node UTF-8 decode yields Unicode scalar header text");
        let Some(RolloutItem::SessionMeta {
            schema_version,
            mut meta,
        }) = parse_rollout_line(&first_line)
        else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("session {thread_id} rollout is missing its header line"),
            ));
        };
        self.overlay_meta(&mut meta).await?;
        let old_meta_id = meta.id.clone();
        meta.id = new_id.into();
        let header = serialize_rollout_item(&RolloutItem::SessionMeta {
            schema_version,
            meta: meta.clone(),
        });
        let root = if entry.archived {
            &self.archive_root
        } else {
            &self.dir
        };
        let dest = join_path(root, &format!("{}-{new_id}.jsonl", meta.created_at));
        if path_exists(&dest).await? {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("rename rollout destination {dest} already exists"),
            ));
        }
        let temp = format!("{dest}.tmp");
        let mut rewritten = Vec::with_capacity(header.len() + 1 + rest_bytes.len());
        rewritten.extend_from_slice(header.as_bytes());
        rewritten.push(b'\n');
        rewritten.extend_from_slice(rest_bytes);
        tokio::fs::write(&temp, &rewritten).await?;
        {
            let flushed = tokio::fs::OpenOptions::new()
                .write(true)
                .open(&temp)
                .await?;
            flushed.sync_all().await?;
        }
        let source_data = session_data_dir(&current);
        let dest_data = session_data_dir(&dest);
        let moved_data = if path_exists(&source_data).await? {
            tokio::fs::rename(&source_data, &dest_data).await?;
            true
        } else {
            false
        };
        let source_processes = background_process_output_dir(&self.dir, thread_id);
        let dest_processes = background_process_output_dir(&self.dir, new_id);
        if path_exists(&dest_processes).await? {
            if moved_data {
                let _ = tokio::fs::rename(&dest_data, &source_data).await;
            }
            let _ = tokio::fs::remove_file(&temp).await;
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("rename background-process destination {dest_processes} already exists"),
            ));
        }
        let moved_processes = if path_exists(&source_processes).await? {
            if let Some(parent) = Path::new(&dest_processes).parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            tokio::fs::rename(&source_processes, &dest_processes).await?;
            true
        } else {
            false
        };
        if let Err(publish_error) = tokio::fs::rename(&temp, &dest).await {
            if moved_processes {
                if let Err(rollback_error) =
                    tokio::fs::rename(&dest_processes, &source_processes).await
                {
                    return Err(io::Error::other(format!(
                        "failed to publish renamed rollout: {publish_error}; failed to roll back background-process outputs: {rollback_error}"
                    )));
                }
            }
            if moved_data {
                if let Err(rollback_error) = tokio::fs::rename(&dest_data, &source_data).await {
                    return Err(io::Error::other(format!(
                        "failed to publish renamed rollout: {publish_error}; failed to roll back session data tree: {rollback_error}"
                    )));
                }
            }
            return Err(publish_error);
        }
        match tokio::fs::remove_file(&current).await {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
        let metadata_path = join_path(&self.dir, "thread-metadata.sqlite3");
        let projection_path = join_path(&self.dir, "threads-index.sqlite3");
        let new_meta_id = meta.id.clone();
        let old_thread = thread_id.to_string();
        tokio::task::spawn_blocking(move || {
            let mut authority =
                crate::store::thread_metadata::ThreadMetadataDb::open(Path::new(&metadata_path))?;
            authority.rename(&old_meta_id, &new_meta_id)?;
            crate::store::thread_index::TurnIndexDb::open(&projection_path)?
                .delete_thread(&old_thread)
        })
        .await
        .map_err(io::Error::other)??;
        index.delete(thread_id);
        let updated_at = file_modified_at(Path::new(&dest))
            .await
            .unwrap_or(meta.created_at);
        index.set(HeaderEntry {
            meta,
            path: dest,
            updated_at,
            archived: entry.archived,
        });
        writer_lock.release().await
    }

    async fn update_title(&self, thread_id: &str, title: &str) -> io::Result<()> {
        let trimmed = title.trim();
        if trimmed.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "title must be non-empty",
            ));
        }
        let _operation = self.begin_lifecycle(thread_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("session {thread_id} has a concurrent lifecycle operation"),
            )
        })?;
        let _file_lock = self
            .try_lifecycle_file_lock(thread_id)
            .await?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!("session {thread_id} has a concurrent lifecycle operation"),
                )
            })?;
        self.recover_unarchive_if_needed_locked(thread_id).await?;
        let file = self
            .locate(thread_id)
            .await?
            .ok_or_else(|| io::Error::other(format!("session {thread_id} not found")))?;
        let writer_lock_path = compression::writer_lock_path(&file);
        let writer_lock = TemporarySessionLock::new(
            acquire_session_lock(&writer_lock_path, &file).await?,
            writer_lock_path,
        );
        let meta = update_metadata(
            &file,
            &self.writer_projection(&thread_id.into()),
            Some(trimmed.to_string()),
            None,
        )
        .await?;

        let archived = file_is_under(&file, &self.archive_root);
        let updated_at = file_modified_at(Path::new(&file))
            .await
            .unwrap_or(meta.created_at);
        self.index.lock().await.set(HeaderEntry {
            meta,
            path: file,
            updated_at,
            archived,
        });
        writer_lock.release().await
    }

    async fn update_host_metadata(
        &self,
        thread_id: &str,
        host_metadata: Option<RuntimeJson>,
    ) -> io::Result<()> {
        let _operation = self.begin_lifecycle(thread_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("session {thread_id} has a concurrent lifecycle operation"),
            )
        })?;
        let _file_lock = self
            .try_lifecycle_file_lock(thread_id)
            .await?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!("session {thread_id} has a concurrent lifecycle operation"),
                )
            })?;
        self.recover_unarchive_if_needed_locked(thread_id).await?;
        let file = self
            .locate(thread_id)
            .await?
            .ok_or_else(|| io::Error::other(format!("session {thread_id} not found")))?;
        let writer_lock_path = compression::writer_lock_path(&file);
        let writer_lock = TemporarySessionLock::new(
            acquire_session_lock(&writer_lock_path, &file).await?,
            writer_lock_path,
        );
        let previous_updated_at = file_modified_at(Path::new(&file)).await.unwrap_or_default();
        let meta = update_metadata(
            &file,
            &self.writer_projection(&thread_id.into()),
            None,
            Some(host_metadata),
        )
        .await?;
        let archived = file_is_under(&file, &self.archive_root);
        self.index.lock().await.set(HeaderEntry {
            meta,
            path: file,
            updated_at: previous_updated_at,
            archived,
        });
        writer_lock.release().await
    }

    /// Open (create-or-append) the per-agent sidechain rollout for a sub-agent —
    /// stored at `<session>/subagents/agent-<agentId>.jsonl` with a meta
    /// sidecar. Reuses [`Self::build_live`] so the file is the SAME rollout
    /// format as the main session, just one file per agent. Appends (no session
    /// header); a brand-new agent creates the file, a resume continuation
    /// appends to the same one.
    async fn open_sub_agent(
        &self,
        thread_id: &str,
        agent_id: &str,
        meta: SubAgentMeta,
    ) -> io::Result<Box<dyn LiveRollout>> {
        self.require_sub_agent_dir(thread_id).await?;
        // Retain parent namespace admission through child lease publication.
        let started = Instant::now();
        let _parent_lifecycle = loop {
            if let Some(lock) = self.try_lifecycle_file_lock(thread_id).await? {
                break lock;
            }
            if started.elapsed() >= SUBAGENT_DIR_LOCK_WAIT {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "parent lifecycle operation is active",
                ));
            }
            tokio::time::sleep(SUBAGENT_DIR_LOCK_RETRY_START).await;
        };
        let parent = self
            .locate(thread_id)
            .await?
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "parent thread not found"))?;
        let dir = sub_agent_dir(&parent);
        self.recover_pending_path(&agent_file_path(&dir, agent_id))
            .await?;
        tokio::fs::create_dir_all(&dir).await?;
        // Write the meta sidecar ONCE, at agent creation (create-exclusive): a
        // resume re-opens the same agentId and must NOT clobber the original
        // createdAt/agentType.
        let mut meta_line = runtime_codec::encode_text(&meta)
            .expect("SubAgentMeta has an exact runtime encoding")
            .to_node_utf8();
        meta_line.push(b'\n');
        match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(agent_meta_path(&dir, agent_id))
            .await
        {
            Ok(mut handle) => {
                handle.write_all(&meta_line).await?;
                handle.flush().await?;
            }
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {} // keep the original
            Err(err) => return Err(err),
        }
        // Each sidechain writer owns its own representation-stable lease.
        self.build_live(
            &agent_file_path(&dir, agent_id),
            None,
            BuildLiveOptions {
                header: false,
                resumed: false,
                lock: true,
            },
        )
        .await
    }

    /// Read a sub-agent's persisted transcript (compacted projection) + its meta.
    async fn load_sub_agent(
        &self,
        thread_id: &str,
        agent_id: &str,
    ) -> io::Result<SubAgentTranscript> {
        let dir = self.require_sub_agent_dir(thread_id).await?;
        let child = agent_file_path(&dir, agent_id);
        self.recover_pending_path(&child).await?;
        let items = load_items_if_exists(&child).await?;
        let messages = match &items {
            Some(items) => project_for_prompt(items),
            None => Vec::new(),
        };
        let meta = read_sub_agent_meta(&agent_meta_path(&dir, agent_id)).await;
        let surfaced_tools = match &items {
            Some(items) => surfaced_tools_from_items(items),
            None => Vec::new(),
        };
        let turn_index = items.as_deref().map(|items| rollout_metadata(items).2);
        Ok(SubAgentTranscript {
            messages,
            turn_index,
            meta,
            surfaced_tools: if surfaced_tools.is_empty() {
                None
            } else {
                Some(surfaced_tools)
            },
        })
    }

    /// List the agentIds that have a persisted transcript under a session.
    async fn list_sub_agents(&self, thread_id: &str) -> io::Result<Vec<String>> {
        let dir = self.require_sub_agent_dir(thread_id).await?;
        let Some(names) = read_dir_names(&dir).await? else {
            return Ok(Vec::new()); // no subagents/ dir yet
        };
        Ok(names
            .into_iter()
            .filter(|n| n.starts_with("agent-") && compression::is_rollout_name(n))
            .filter_map(|name| {
                name.strip_suffix(".zst")
                    .unwrap_or(&name)
                    .strip_prefix("agent-")
                    .and_then(|id| id.strip_suffix(".jsonl"))
                    .map(str::to_owned)
            })
            .collect::<HashSet<_>>()
            .into_iter()
            .collect())
    }

    async fn count_sessions_touched_since(
        &self,
        since_ms: f64,
        exclude_id: Option<&str>,
    ) -> io::Result<usize> {
        let Some(names) = read_dir_names(&self.dir).await? else {
            return Ok(0); // sessions dir not created yet
        };
        let mut count = 0usize;
        for name in &names {
            if !compression::is_rollout_name(name) {
                continue; // skip `.lock` sidecars, the archived/ dir, etc.
            }
            // Match the id EXACTLY (not a suffix): a host-supplied id may itself
            // contain '-', so a suffix test like `-<id>.jsonl` could wrongly drop
            // a different session whose id ends with this one (e.g. exclude `abc`
            // must not also drop `…-x-abc.jsonl`).
            if let Some(exclude_id) = exclude_id {
                if session_id_from_file_name(name) == exclude_id {
                    continue;
                }
            }
            // A raced delete/rename is benign; other metadata/clock errors are
            // authoritative store failures and must reach the Dream lifecycle.
            let metadata = match tokio::fs::metadata(join_path(&self.dir, name)).await {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            if mtime_ms(metadata.modified()?) >= since_ms {
                count += 1;
            }
        }
        Ok(count)
    }
}

fn background_process_output_dir(root: &str, thread_id: &str) -> String {
    let id = runtime_path_component_to_store_host(&RuntimeString::from(thread_id));
    join_path(root, &format!("background-processes/{id}"))
}

/// The default sessions root: `~/.klaus/sessions`.
fn default_sessions_dir() -> String {
    #[allow(deprecated)]
    let home = std::env::home_dir().unwrap_or_else(|| PathBuf::from("."));
    native_text_to_store_host(home.join(".klaus").join("sessions").as_os_str())
}

struct BuildLiveOptions {
    header: bool,
    resumed: bool,
    lock: bool,
}

/// A command on a live rollout's ordered write queue.
enum WriteCmd {
    /// Append one serialized line to the rollout file.
    Line(String),
    /// Persist the authoritative title at this position in the ordered writer
    /// chain, preserving append-vs-metadata happens-before semantics.
    UpdateTitle {
        title: String,
        ack: oneshot::Sender<Result<(), String>>,
    },
    /// Persist the opaque host attachment without acquiring the writer lock a
    /// second time or changing the rollout's activity timestamp.
    UpdateHostMetadata {
        host_metadata: Option<RuntimeJson>,
        ack: oneshot::Sender<Result<(), String>>,
    },
    /// Report the chain's state once everything enqueued before this point is
    /// durable (or has failed).
    Flush(oneshot::Sender<Result<(), String>>),
    /// Flush everything before this command, publish the retained writer
    /// result, and end admission permanently.
    Shutdown(oneshot::Sender<Result<(), String>>),
}

const WRITE_BATCH_ITEMS: usize = 256;
const WRITE_BATCH_BYTES: usize = 1024 * 1024;

/// Spawn the single writer task that drains a rollout's write queue in order.
/// The first append failure poisons the chain: later appends are skipped and
/// every subsequent flush/shutdown reports the original error.
fn spawn_writer(
    file: String,
    projection: Option<WriterProjection>,
) -> (mpsc::Sender<WriteCmd>, oneshot::Receiver<()>) {
    let (tx, mut rx) = mpsc::channel::<WriteCmd>(WRITE_BATCH_ITEMS);
    let (finished_send, finished_receive) = oneshot::channel();
    tokio::spawn(async move {
        let mut failure: Option<String> = None;
        let mut handle = None;
        let mut pending = None;
        loop {
            let cmd = match pending.take() {
                Some(cmd) => cmd,
                None => match rx.recv().await {
                    Some(cmd) => cmd,
                    None => break,
                },
            };
            match cmd {
                WriteCmd::Line(mut line) => {
                    if failure.is_some() {
                        continue; // a poisoned chain never runs later appends
                    }
                    // Coalesce only adjacent appends. Metadata and durability
                    // commands remain ordered barriers, including when a batch
                    // reaches its item or byte budget.
                    let mut count = 1;
                    while count < WRITE_BATCH_ITEMS && line.len() < WRITE_BATCH_BYTES {
                        match rx.try_recv() {
                            Ok(WriteCmd::Line(next)) => {
                                if line.len().saturating_add(next.len()) > WRITE_BATCH_BYTES {
                                    pending = Some(WriteCmd::Line(next));
                                    break;
                                }
                                line.push_str(&next);
                                count += 1;
                            }
                            Ok(barrier) => {
                                pending = Some(barrier);
                                break;
                            }
                            Err(_) => break,
                        }
                    }
                    if let Err(err) = append_to_file(&file, &mut handle, &line).await {
                        failure = Some(err.to_string());
                    } else {
                        project_durable(&file, &projection).await;
                    }
                }
                WriteCmd::UpdateTitle { title, ack } => {
                    let result = match &failure {
                        Some(message) => Err(message.clone()),
                        None => match projection.as_ref() {
                            Some(projection) => {
                                update_metadata(&file, projection, Some(title), None)
                                    .await
                                    .map(|_| ())
                                    .map_err(|err| err.to_string())
                            }
                            None => Err("rollout has no thread metadata authority".into()),
                        },
                    };
                    let _ = ack.send(result);
                }
                WriteCmd::UpdateHostMetadata { host_metadata, ack } => {
                    let result = match &failure {
                        Some(message) => Err(message.clone()),
                        None => match projection.as_ref() {
                            Some(projection) => {
                                update_metadata(&file, projection, None, Some(host_metadata))
                                    .await
                                    .map(|_| ())
                                    .map_err(|err| err.to_string())
                            }
                            None => Err("rollout has no thread metadata authority".into()),
                        },
                    };
                    let _ = ack.send(result);
                }
                WriteCmd::Flush(ack) => {
                    if failure.is_none() {
                        project_durable(&file, &projection).await;
                    }
                    let _ = ack.send(match &failure {
                        Some(message) => Err(message.clone()),
                        None => Ok(()),
                    });
                }
                WriteCmd::Shutdown(ack) => {
                    if failure.is_none() {
                        project_durable(&file, &projection).await;
                    }
                    let _ = ack.send(match &failure {
                        Some(message) => Err(message.clone()),
                        None => Ok(()),
                    });
                    break;
                }
            }
        }
        // Completion retires the writer's file ownership before its caller
        // releases the single-writer lock or reopens the rollout.
        drop(handle);
        let _ = finished_send.send(());
    });
    (tx, finished_receive)
}

async fn append_to_file(
    file: &str,
    handle: &mut Option<tokio::fs::File>,
    data: &str,
) -> io::Result<()> {
    if handle.is_none() {
        *handle = Some(
            tokio::fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(file)
                .await?,
        );
    }
    let handle = handle.as_mut().expect("opened rollout append handle");
    handle.write_all(data.as_bytes()).await?;
    handle.flush().await?;
    handle.sync_data().await
}

async fn read_file_meta(file: &str) -> io::Result<SessionMeta> {
    let file = compression::open_async(file).await?;
    let mut reader = tokio::io::BufReader::new(file);
    let mut line = String::new();
    tokio::io::AsyncBufReadExt::read_line(&mut reader, &mut line).await?;
    match parse_rollout_line(&line) {
        Some(RolloutItem::SessionMeta { meta, .. }) => Ok(meta),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "session has no session_meta header",
        )),
    }
}

#[derive(Debug, Clone)]
struct WriterProjection {
    root: String,
    thread: String,
}

async fn update_metadata(
    file: &str,
    projection: &WriterProjection,
    title: Option<String>,
    host_metadata: Option<Option<RuntimeJson>>,
) -> io::Result<SessionMeta> {
    let meta = read_file_meta(file).await?;
    if meta.id != projection.thread.as_str() {
        return Err(io::Error::other("thread metadata identity mismatch"));
    }
    let path = join_path(&projection.root, "thread-metadata.sqlite3");
    tokio::task::spawn_blocking(move || {
        let mut db = crate::store::thread_metadata::ThreadMetadataDb::open(Path::new(&path))?;
        let meta = db.seed_if_absent(&meta)?;
        if let Some(title) = title {
            let title = title.trim();
            if title.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "title must be non-empty",
                ));
            }
            db.update_title(&meta.id, Some(title.into()))?;
        }
        if let Some(value) = host_metadata {
            db.update_host_metadata(&meta.id, value)?;
        }
        db.get(&meta.id)?
            .ok_or_else(|| io::Error::other("thread metadata disappeared"))
    })
    .await
    .map_err(io::Error::other)?
}

async fn project_durable(file: &str, projection: &Option<WriterProjection>) {
    let Some(projection) = projection.clone() else {
        return;
    };
    let file = file.to_string();
    let result = tokio::task::spawn_blocking(move || {
        let path = join_path(&projection.root, "threads-index.sqlite3");
        let mut db = crate::store::thread_index::TurnIndexDb::open(&path)?;
        match db.materialize_rollout(&projection.thread, &file) {
            Ok(()) => Ok(()),
            Err(error) => {
                if let Err(record) =
                    db.record_projection_failure(&projection.thread, &error.to_string())
                {
                    tracing::warn!(%record, "failed to persist history projection failure");
                }
                Err(error)
            }
        }
    })
    .await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::warn!(%error,"failed to project durable rollout"),
        Err(error) => tracing::warn!(%error,"durable rollout projection task failed"),
    }
}

/// The live writing handle over one rollout file: an ordered write queue plus
/// the (optional) single-writer lock sidecar.
#[derive(Debug)]
struct JsonlLiveRollout {
    writer: Arc<StdMutex<WriterState>>,
    admission: crate::store::rollout_admission::OrderedRolloutAdmission,
    use_lock: bool,
    lock_path: String,
    lock_handle: StdMutex<Option<tokio::fs::File>>,
}

#[derive(Debug)]
struct WriterState {
    inspection_revision: u64,
    tx: Option<mpsc::Sender<WriteCmd>>,
    shutdown: Option<tokio::sync::watch::Receiver<Option<Result<(), String>>>>,
    finished: Option<oneshot::Receiver<()>>,
}

async fn await_writer_shutdown(
    mut completion: tokio::sync::watch::Receiver<Option<Result<(), String>>>,
) -> io::Result<()> {
    loop {
        if let Some(result) = completion.borrow().clone() {
            return result.map_err(io::Error::other);
        }
        if completion.changed().await.is_err() {
            return Err(io::Error::other(
                "rollout shutdown task ended without a completion",
            ));
        }
    }
}

impl JsonlLiveRollout {
    async fn enqueue(&self, item: RolloutItem) {
        if let Ok(admission) = self.prepare_append().await {
            admission.commit(item);
        }
    }

    /// Await the current tail of the write chain; everything enqueued before
    /// this point is then durable (or the chain's poisoning error is returned).
    async fn wait_chain(&self) -> io::Result<()> {
        enum Wait {
            Flush(oneshot::Receiver<Result<(), String>>),
            Shutdown(tokio::sync::watch::Receiver<Option<Result<(), String>>>),
        }
        let wait = {
            let _admission = self.admission.lock().await;
            let (tx, completion) = {
                let writer = self.writer.lock().expect("writer state");
                (writer.tx.clone(), writer.shutdown.clone())
            };
            if let Some(tx) = tx {
                let (ack_tx, ack_rx) = oneshot::channel();
                tx.send(WriteCmd::Flush(ack_tx))
                    .await
                    .map_err(|_| io::Error::other("rollout writer terminated"))?;
                Wait::Flush(ack_rx)
            } else if let Some(completion) = completion {
                Wait::Shutdown(completion)
            } else {
                return Err(io::Error::other(
                    "rollout writer is closed without a shutdown result",
                ));
            }
        };
        match wait {
            Wait::Flush(ack_rx) => match ack_rx.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(message)) => Err(io::Error::other(message)),
                Err(_) => Err(io::Error::other("rollout writer terminated")),
            },
            Wait::Shutdown(completion) => await_writer_shutdown(completion).await,
        }
    }
}

#[async_trait]
impl LiveRollout for JsonlLiveRollout {
    async fn prepare_batch(
        &self,
        maximum_records: usize,
    ) -> io::Result<crate::store::session_store::PreparedRolloutBatch<'_>> {
        if maximum_records == 0 || maximum_records > WRITE_BATCH_ITEMS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "rollout admission must reserve between one and 256 records",
            ));
        }
        let permit = self
            .admission
            .reserve_many(maximum_records, || {
                self.writer.lock().expect("writer state").tx.clone()
            })
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "rollout writer is closed"))?;
        let state = self.writer.clone();
        Ok(crate::store::session_store::PreparedRolloutBatch::new(
            move |items| {
                assert!(items.len() <= maximum_records);
                let count = items.len() as u64;
                let commands = items
                    .iter()
                    .map(|item| WriteCmd::Line(format!("{}\n", serialize_rollout_item(item))))
                    .collect();
                let mut writer = state.lock().expect("writer state");
                permit.send_many(commands);
                writer.inspection_revision = writer
                    .inspection_revision
                    .checked_add(count)
                    .expect("rollout inspection revision exhausted");
                drop(writer);
            },
        ))
    }
    fn inspection_revision(&self) -> Option<u64> {
        Some(
            self.writer
                .lock()
                .expect("writer state")
                .inspection_revision,
        )
    }

    async fn append_message(&self, message: crate::schemas::message::Message) {
        self.enqueue(RolloutItem::Message { message }).await;
    }

    async fn append_turn_context(&self, ctx: TurnContextSnapshot) {
        self.enqueue(RolloutItem::TurnContext { ctx }).await;
    }

    async fn append_compaction(&self, record: CompactionRecord) {
        self.enqueue(RolloutItem::Compacted(CompactedItem {
            summary: record.summary,
            replaced_range: record.replaced_range,
            replacement_history: record.replacement_history,
            source_message_ids: record.source_message_ids,
            approval_parent_history: record.approval_parent_history,
        }))
        .await;
    }

    async fn append_event(&self, event: RuntimeString, opts: Option<AppendEventOptions>) {
        let opts = opts.unwrap_or_default();
        self.enqueue(RolloutItem::Event(EventItem {
            event,
            turn_id: opts.turn_id,
            payload: opts.payload,
        }))
        .await;
    }

    async fn append_rollback(&self, record: RollbackRecord) {
        self.enqueue(RolloutItem::Rollback(RollbackItem {
            num_turns: record.num_turns,
        }))
        .await;
    }

    async fn append_checkpoint(&self, record: CheckpointRecord) {
        self.enqueue(RolloutItem::Checkpoint(CheckpointItem {
            hash: record.hash.map(RuntimeString::from),
        }))
        .await;
    }

    async fn append_usage(&self, record: crate::schemas::rollout::UsageLedgerItem) {
        self.enqueue(RolloutItem::UsageLedger(record)).await;
    }

    async fn append_subagent_lifecycle(
        &self,
        record: crate::schemas::subagent_lifecycle::SubagentLifecycleRecord,
    ) {
        self.enqueue(RolloutItem::SubagentLifecycle(
            crate::schemas::rollout::SubagentLifecycleItem { record },
        ))
        .await;
    }

    async fn append_background_process_lifecycle(
        &self,
        record: crate::schemas::background_process::BackgroundProcessRecord,
    ) {
        self.enqueue(RolloutItem::BackgroundProcessLifecycle(
            crate::schemas::rollout::BackgroundProcessLifecycleItem { record },
        ))
        .await;
    }

    async fn flush(&self) -> io::Result<()> {
        // Await the current tail; all enqueued appends are then durable.
        self.wait_chain().await
    }

    async fn update_title(&self, title: &str) -> io::Result<()> {
        let ack_rx = {
            let _admission = self.admission.lock().await;
            let tx = self
                .writer
                .lock()
                .expect("writer state")
                .tx
                .clone()
                .ok_or_else(|| {
                    io::Error::other("rollout writer is closed without accepting title updates")
                })?;
            let (ack_tx, ack_rx) = oneshot::channel();
            tx.send(WriteCmd::UpdateTitle {
                title: title.to_string(),
                ack: ack_tx,
            })
            .await
            .map_err(|_| io::Error::other("rollout writer terminated"))?;
            ack_rx
        };
        match ack_rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(message)) => Err(io::Error::other(message)),
            Err(_) => Err(io::Error::other("rollout writer terminated")),
        }
    }

    async fn update_host_metadata(&self, host_metadata: Option<RuntimeJson>) -> io::Result<()> {
        let ack_rx = {
            let _admission = self.admission.lock().await;
            let tx = self
                .writer
                .lock()
                .expect("writer state")
                .tx
                .clone()
                .ok_or_else(|| {
                    io::Error::other(
                        "rollout writer is closed without accepting host metadata updates",
                    )
                })?;
            let (ack_tx, ack_rx) = oneshot::channel();
            tx.send(WriteCmd::UpdateHostMetadata {
                host_metadata,
                ack: ack_tx,
            })
            .await
            .map_err(|_| io::Error::other("rollout writer terminated"))?;
            ack_rx
        };
        match ack_rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(message)) => Err(io::Error::other(message)),
            Err(_) => Err(io::Error::other("rollout writer terminated")),
        }
    }

    async fn shutdown(&self) -> io::Result<()> {
        let admission = self.admission.lock().await;
        let completion = {
            let mut writer = self.writer.lock().expect("writer state");
            if let Some(completion) = writer.shutdown.as_ref() {
                completion.clone()
            } else {
                let tx = writer.tx.take();
                let finished = writer.finished.take();
                let lock_handle = self.lock_handle.lock().expect("lock state").take();
                let use_lock = self.use_lock;
                let lock_path = self.lock_path.clone();
                let (send, receive) = tokio::sync::watch::channel(None);
                writer.shutdown = Some(receive.clone());
                // The rollout owns this task. Cancelling the Session phase that
                // initiated shutdown cannot reopen admission or abandon a
                // half-drained writer after the lock sidecar was removed.
                tokio::spawn(async move {
                    let mut errors = Vec::new();
                    match tx {
                        Some(tx) => {
                            let (ack_send, ack_receive) = oneshot::channel();
                            if tx.send(WriteCmd::Shutdown(ack_send)).await.is_err() {
                                errors.push("rollout writer terminated".to_string());
                            } else {
                                match ack_receive.await {
                                    Ok(Ok(())) => {}
                                    Ok(Err(error)) => errors.push(error),
                                    Err(_) => errors.push("rollout writer terminated".to_string()),
                                }
                            }
                        }
                        None => errors.push("rollout writer sender was missing".to_string()),
                    }
                    match finished {
                        Some(finished) => {
                            if finished.await.is_err() {
                                errors.push(
                                    "rollout writer ended without a termination signal".to_string(),
                                );
                            }
                        }
                        None => errors
                            .push("rollout writer termination receiver was missing".to_string()),
                    }
                    drop(lock_handle);
                    if use_lock {
                        if let Err(error) = tokio::fs::remove_file(&lock_path).await {
                            if error.kind() != io::ErrorKind::NotFound {
                                errors.push(format!(
                                    "failed to remove rollout lock sidecar: {error}"
                                ));
                            }
                        }
                    }
                    let result = if errors.is_empty() {
                        Ok(())
                    } else {
                        Err(errors.join("; "))
                    };
                    let _ = send.send(Some(result));
                });
                receive
            }
        };
        drop(admission);
        await_writer_shutdown(completion).await
    }
}

/// Strictly locate one exact session header in a namespace. Unlike the tolerant
/// listing cache, lifecycle mutations must surface unreadable directories/files
/// and duplicate identities as persistence failures rather than misreporting
/// them as `missing`.
async fn find_thread_header(
    root: &str,
    thread_id: &str,
    archived: bool,
) -> io::Result<Option<HeaderEntry>> {
    let Some(names) = read_dir_names(root).await? else {
        return Ok(None);
    };
    let mut found: Option<HeaderEntry> = None;
    for name in names {
        if !compression::is_rollout_name(&name) {
            continue;
        }
        let path = join_path(root, &name);
        if name.ends_with(".jsonl.zst")
            && tokio::fs::try_exists(compression::plain_path(Path::new(&path))).await?
        {
            continue;
        }
        let bytes = match compression::read_prefix(&path, 64 * 1024).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        let first_line = bytes
            .split(|byte| *byte == b'\n')
            .next()
            .unwrap_or_default();
        if first_line.is_empty() {
            continue;
        }
        let line = NodeBufferUtf8::decode(first_line)
            .try_to_utf8()
            .expect("Node UTF-8 decode yields Unicode scalar header text");
        let Some(RolloutItem::SessionMeta { meta, .. }) = parse_rollout_line(&line) else {
            continue;
        };
        if meta.id != thread_id {
            continue;
        }
        if found.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("multiple {archived:?} namespace rollouts have session id {thread_id}"),
            ));
        }
        let updated_at = file_modified_at(Path::new(&path))
            .await
            .unwrap_or(meta.created_at);
        found = Some(HeaderEntry {
            meta,
            path,
            updated_at,
            archived,
        });
    }
    Ok(found)
}

/// Parse a whole rollout file into items, tolerating a corrupt/truncated tail.
async fn load_items(file: &str) -> io::Result<Vec<RolloutItem>> {
    let bytes = compression::read(file).await?;
    Ok(parse_items(&bytes))
}

pub(crate) fn file_stamp(metadata: &std::fs::Metadata) -> i64 {
    metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|time| time.as_nanos().min(i64::MAX as u128) as i64)
        .unwrap_or_default()
}

pub(crate) fn parse_items(bytes: &[u8]) -> Vec<RolloutItem> {
    let content = NodeBufferUtf8::decode(bytes)
        .try_to_utf8()
        .expect("Node UTF-8 decode yields Unicode scalar JSONL text");
    let mut items: Vec<RolloutItem> = Vec::new();
    for line in content.split('\n') {
        if line.is_empty() {
            continue;
        }
        let Some(item) = parse_rollout_line(line) else {
            continue; // tolerant: skip corrupt/truncated lines
        };
        items.push(item);
    }
    items
}

pub(crate) fn rollout_metadata(
    items: &[RolloutItem],
) -> (
    Option<SessionMeta>,
    Option<TurnContextSnapshot>,
    crate::engine::session::TurnIndex,
    crate::engine::session::SessionFileChanges,
) {
    let mut state = RolloutMetadataAccumulator::default();
    for item in items {
        state.push(item);
    }
    state.finish()
}

#[derive(Default)]
pub(crate) struct RolloutMetadataAccumulator {
    meta: Option<SessionMeta>,
    last_turn_context: Option<TurnContextSnapshot>,
    turn_index: crate::engine::session::TurnIndex,
    file_changes: crate::engine::session::SessionFileChanges,
    current_turn: Option<RuntimeString>,
    context_turns: HashSet<RuntimeString>,
}

impl RolloutMetadataAccumulator {
    pub(crate) fn push(&mut self, item: &RolloutItem) {
        let Self {
            meta,
            last_turn_context,
            turn_index,
            file_changes,
            current_turn,
            context_turns,
        } = self;
        match item {
            RolloutItem::SessionMeta { meta: header, .. } => {
                if meta.is_none() {
                    *meta = Some(header.clone());
                }
            }
            RolloutItem::TurnContext { ctx } => {
                // Keep the newest snapshot so resume restores the latest context.
                *last_turn_context = Some(ctx.clone());
                *current_turn = ctx.turn_id.clone();
                if let Some(turn_id) = &ctx.turn_id {
                    context_turns.insert(turn_id.clone());
                }
                if let (Some(turn_id), Some(started_at_ms)) = (&ctx.turn_id, ctx.started_at_ms) {
                    if started_at_ms > 0 {
                        turn_index
                            .turn_started_at_ms
                            .insert(turn_id.clone(), started_at_ms);
                    }
                }
            }
            RolloutItem::Message { message } => {
                if let (Some(turn_id), Some(uuid)) = (&current_turn, rollout_message_uuid(message))
                {
                    turn_index
                        .message_turn
                        .insert(uuid.clone(), turn_id.clone());
                }
            }
            RolloutItem::Event(event) => {
                if event.event == "agent_notification" {
                    if let Some(notification) = event.payload.as_ref().and_then(|payload| {
                        runtime_codec::decode_value::<
                            crate::schemas::agent_notification::AgentNotification,
                        >(payload)
                        .ok()
                    }) {
                        if !turn_index
                            .agent_notifications
                            .iter()
                            .any(|old| old.notification_id == notification.notification_id)
                        {
                            turn_index.agent_notifications.push(notification);
                        }
                    }
                }
                if event.event == FILE_CHANGE_EVENT {
                    if let Some(change) = event
                        .payload
                        .as_ref()
                        .and_then(|payload| runtime_codec::decode_value(payload).ok())
                    {
                        let turn_id = event.turn_id.as_ref().or(current_turn.as_ref());
                        file_changes.record_change(turn_id, &change);
                    }
                }
                // Which tool calls the turn's ABORT killed. The phase is the only
                // thing that separates them from tools that failed on their own
                // merits, so a reopened thread needs it to avoid painting the
                // user's stop as a red tool failure.
                if event.event == "tool_lifecycle" {
                    let payload = event.payload.as_ref().and_then(|p| p.as_object());
                    let phase = payload
                        .and_then(|o| o.get("phase"))
                        .and_then(|p| p.as_str());
                    let tool_use_id = payload
                        .and_then(|o| o.get("toolUseId"))
                        .and_then(|t| t.as_str());
                    let turn_id = payload
                        .and_then(|o| o.get("turnId"))
                        .and_then(|t| t.as_str())
                        .map(RuntimeString::from)
                        .or_else(|| event.turn_id.clone());
                    if let (Some(phase), Some(tool_use_id), Some(turn_id)) =
                        (phase, tool_use_id, turn_id)
                    {
                        if *phase == "cancelled" {
                            let cancelled =
                                turn_index.turn_cancelled_tools.entry(turn_id).or_default();
                            if !cancelled.contains(tool_use_id) {
                                cancelled.push(tool_use_id.clone());
                            }
                        }
                    }
                }
                if matches!(
                    event.event.try_to_utf8().ok().as_deref(),
                    Some("turn_goal_metadata" | "turn_lifecycle")
                ) {
                    if let (Some(turn_id), Some(payload)) = (&event.turn_id, &event.payload) {
                        if let Ok(metadata) = crate::runtime_values::runtime_codec::decode_value::<
                            crate::engine::session::TurnGoalMetadata,
                        >(payload)
                        {
                            turn_index
                                .turn_goal_metadata
                                .insert(turn_id.clone(), metadata);
                        }
                    }
                }
                if event.event == "goal_submission_bound" {
                    if let (Some(turn_id), Some(payload)) = (
                        &event.turn_id,
                        event.payload.as_ref().and_then(RuntimeJson::as_object),
                    ) {
                        for key in ["messageUuid", "displayUuid"] {
                            if let Some(id) = payload.get(key).and_then(RuntimeJson::as_str) {
                                turn_index.message_turn.insert(id.clone(), turn_id.clone());
                            }
                        }
                    }
                }
                if event.event == TURN_LIFECYCLE_EVENT && event.turn_id.is_some() {
                    let status = event
                        .payload
                        .as_ref()
                        .and_then(|p| p.as_object())
                        .and_then(|o| o.get("status"))
                        .and_then(|s| s.as_str());
                    if let (Some(turn_id), Some(status)) = (event.turn_id.as_ref(), status) {
                        turn_index
                            .turn_status
                            .insert(turn_id.clone(), status.into());
                        if status == "completed" {
                            if let Some(message_id) = event
                                .payload
                                .as_ref()
                                .and_then(|p| p.as_object())
                                .and_then(|o| o.get("finalMessageId"))
                                .and_then(|id| id.as_str())
                                .filter(|id| !id.is_empty())
                            {
                                turn_index
                                    .turn_final_message
                                    .insert(turn_id.clone(), message_id.clone());
                            }
                        }
                        if let Some(started_at_ms) = event
                            .payload
                            .as_ref()
                            .and_then(|p| p.as_object())
                            .and_then(|o| o.get("startedAtMs"))
                            .and_then(|value| value.as_f64())
                            .filter(|value| value.is_finite() && *value > 0.0)
                        {
                            turn_index
                                .turn_started_at_ms
                                .insert(turn_id.clone(), started_at_ms as i64);
                        }
                        if let Some(finished_at_ms) = event
                            .payload
                            .as_ref()
                            .and_then(|p| p.as_object())
                            .and_then(|o| o.get("finishedAtMs"))
                            .and_then(|value| value.as_f64())
                            .filter(|value| value.is_finite() && *value > 0.0)
                        {
                            turn_index
                                .turn_finished_at_ms
                                .insert(turn_id.clone(), finished_at_ms as i64);
                        }
                        if let Some(duration_ms) = event
                            .payload
                            .as_ref()
                            .and_then(|p| p.as_object())
                            .and_then(|o| o.get("durationMs"))
                            .and_then(|duration| duration.as_f64())
                            .filter(|duration| duration.is_finite() && *duration >= 0.0)
                        {
                            turn_index
                                .turn_duration_ms
                                .insert(turn_id.clone(), duration_ms as u64);
                        }
                        // A failed turn's payload also carries WHY (see the
                        // writer in instrument); restore it so the resumed
                        // history can render the failure, not just the status.
                        if status == "failed" {
                            let details = event.payload.as_ref().and_then(
                                crate::engine::session::turn_error_details_from_lifecycle,
                            );
                            if let Some(error) = event
                                .payload
                                .as_ref()
                                .and_then(|p| p.as_object())
                                .and_then(|o| o.get("error"))
                                .and_then(|e| e.as_str())
                                .filter(|e| !e.is_empty())
                                .or_else(|| {
                                    details
                                        .as_ref()?
                                        .get("message")?
                                        .as_str()
                                        .filter(|message| !message.is_empty())
                                })
                            {
                                turn_index.turn_errors.insert(turn_id.clone(), error.into());
                            }
                            // Older rollouts predate the `kind` field; absence
                            // simply leaves the renderer its raw-text fallback.
                            if let Some(kind) = event
                                .payload
                                .as_ref()
                                .and_then(|p| p.as_object())
                                .and_then(|o| o.get("kind"))
                                .and_then(|k| k.as_str())
                                .filter(|k| !k.is_empty())
                                .or_else(|| {
                                    details
                                        .as_ref()?
                                        .get("kind")?
                                        .as_str()
                                        .filter(|kind| !kind.is_empty())
                                })
                            {
                                turn_index
                                    .turn_error_kinds
                                    .insert(turn_id.clone(), kind.into());
                            }
                            if let Some(details) = details {
                                turn_index
                                    .turn_error_details
                                    .insert(turn_id.clone(), details);
                            } else {
                                turn_index.turn_error_details.remove(turn_id);
                            }
                        } else {
                            turn_index.turn_error_details.remove(turn_id);
                        }
                    }
                }
            }
            _ => {}
        }
    }

    pub(crate) fn finish(
        self,
    ) -> (
        Option<SessionMeta>,
        Option<TurnContextSnapshot>,
        crate::engine::session::TurnIndex,
        crate::engine::session::SessionFileChanges,
    ) {
        let Self {
            meta,
            last_turn_context,
            mut turn_index,
            file_changes,
            context_turns,
            ..
        } = self;
        // 没有 turn_lifecycle 终态记录的回合：进程被 kill 或系统故障导致
        // turn_lifecycle 没落盘，恢复后按「被打断」处理，渲染层不再把它当正常完结
        // 回合折叠。
        for turn_id in turn_index.message_turn.values() {
            if !turn_index.turn_status.contains_key(turn_id) {
                turn_index
                    .turn_status
                    .insert(turn_id.clone(), "interrupted".into());
            }
        }
        // The same inference for a turn that died before ANY of its messages became
        // durable. `turn_context` is written eagerly when the turn opens while the
        // first message is only appended on the stream's first poll, so a kill in
        // that window — an app relaunch for a permission grant SIGKILLs the engine
        // child — leaves a turn the loop above cannot see, because it iterates the
        // message index. Without a status the renderer treats it as an ordinary
        // turn, and the host offers no way to continue the thread it cut off.
        //
        // Only `turn_context` is evidence here, and only main-session turns write it
        // into this rollout (a sub-agent writes to its own), so this cannot invent a
        // turn. It also cannot resurrect a rewound one: a rewind appends a rollback
        // record and never deletes message lines, so a turn that ran far enough to be
        // rewound is already in `message_turn` and already carries its recorded
        // terminal status.
        for turn_id in &context_turns {
            if !turn_index.turn_status.contains_key(turn_id) {
                turn_index
                    .turn_status
                    .insert(turn_id.clone(), "interrupted".into());
            }
        }

        (meta, last_turn_context, turn_index, file_changes)
    }
}

fn resume_result_from_items(thread_id: &str, items: Vec<RolloutItem>) -> io::Result<ResumeResult> {
    let (meta, last_turn_context, turn_index, file_changes) = rollout_metadata(&items);
    let meta = meta.ok_or_else(|| {
        io::Error::other(format!("session {thread_id} has no session_meta header"))
    })?;
    // The header is the file's own claim about which session it records. A
    // mismatch means the locate/index path resolved this thread id to the
    // WRONG rollout file; loading it would silently rebuild another session's
    // history, turn index and file-change ledger under this id. Fail loudly
    // instead of serving borrowed state.
    if meta.id != *thread_id {
        return Err(io::Error::other(format!(
            "session {thread_id} rollout header names a different session: {}",
            String::from_utf16_lossy(meta.id.as_utf16())
        )));
    }
    let messages = project_for_prompt(&items);
    Ok(ResumeResult {
        meta,
        messages,
        raw_items: items,
        last_turn_context,
        turn_index,
        file_changes,
    })
}

/// The uuid every rollout message carries.
fn rollout_message_uuid(message: &Message) -> Option<&RuntimeString> {
    match message {
        Message::User(m) => Some(&m.uuid),
        Message::Assistant(m) => Some(&m.uuid),
        Message::ToolResult(m) => Some(&m.uuid),
        Message::System(m) => Some(&m.uuid),
        Message::CompactBoundary(m) => Some(&m.uuid),
    }
}

/// Discriminator matching the engine's `turn_lifecycle` rollout event (kept
/// as a local literal to avoid an engine module import cycle).
const TURN_LIFECYCLE_EVENT: &str = "turn_lifecycle";

/// Discriminator matching the engine's per-edit `file_change` rollout event
/// (same reason for the local literal).
const FILE_CHANGE_EVENT: &str = "file_change";

/// Like [`load_items`] but returns `None` when the file does not exist.
async fn load_items_if_exists(file: &str) -> io::Result<Option<Vec<RolloutItem>>> {
    match load_items(file).await {
        Ok(items) => Ok(Some(items)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

/// Union the tool names from every `tools_surfaced` event in a rollout (the
/// discovered tools). Tolerant of a malformed payload — a bad record is
/// skipped, never fatal.
fn surfaced_tools_from_items(items: &[RolloutItem]) -> Vec<RuntimeString> {
    let mut out: Vec<RuntimeString> = Vec::new();
    for item in items {
        let RolloutItem::Event(event) = item else {
            continue;
        };
        if event.event != "tools_surfaced" {
            continue;
        }
        let Some(payload) = event.payload.as_ref().and_then(|p| p.as_object()) else {
            continue;
        };
        let Some(names) = payload.get("names").and_then(|n| n.as_array()) else {
            continue;
        };
        for name in names {
            if let Some(name) = name.as_str() {
                if !out.iter().any(|existing| existing == name) {
                    out.push(name.clone());
                }
            }
        }
    }
    out
}

/// The session id encoded in a `<createdAt>-<id>.jsonl` filename. `createdAt`
/// is a numeric timestamp (no `-`), so the id is everything after the FIRST
/// `-` — which preserves an id that itself contains `-`. Returns the whole base
/// name if there is no `-` (defensive).
fn session_id_from_file_name(name: &str) -> &str {
    let name = name.strip_suffix(".zst").unwrap_or(name);
    let base = name.strip_suffix(".jsonl").unwrap_or(name);
    match base.find('-') {
        Some(dash) => &base[dash + 1..],
        None => base,
    }
}

/// The per-session `subagents/` directory, derived from the session file path.
fn sub_agent_dir(session_file: &str) -> String {
    // `<dir>/<createdAt>-<id>.jsonl` → `<dir>/<createdAt>-<id>/subagents`,
    // producing the per-session `<sessionId>/subagents/` layout.
    join_path(&session_data_dir(session_file), "subagents")
}

fn session_data_dir(session_file: &str) -> String {
    session_file
        .strip_suffix(".zst")
        .unwrap_or(session_file)
        .strip_suffix(".jsonl")
        .unwrap_or(session_file)
        .to_string()
}

async fn path_exists(path: &str) -> io::Result<bool> {
    if compression::is_rollout_name(path) {
        return match compression::resolve(Path::new(path)) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        };
    }
    match tokio::fs::metadata(path).await {
        Ok(_) => Ok(true),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}

/// Flush a directory entry update to the backing filesystem. This is a
/// fail-closed durability barrier: platforms/filesystems that cannot sync the
/// directory report an error instead of acknowledging a crash-safe rename.
async fn sync_directory(path: &str) -> io::Result<()> {
    let path = PathBuf::from(path);
    tokio::task::spawn_blocking(move || sync_directory_blocking(&path))
        .await
        .map_err(|error| io::Error::other(format!("directory sync task failed: {error}")))?
}

fn sync_directory_blocking(path: &Path) -> io::Result<()> {
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
        options.open(path)?
    };
    #[cfg(not(windows))]
    let file = std::fs::File::open(path)?;
    file.sync_all()
}

async fn sync_rename_parents(source: &str, dest: &str) -> io::Result<()> {
    let source_parent = Path::new(source).parent().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "rename source has no parent")
    })?;
    let dest_parent = Path::new(dest).parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "rename destination has no parent",
        )
    })?;
    let source_parent = source_parent.to_string_lossy().into_owned();
    let dest_parent = dest_parent.to_string_lossy().into_owned();
    sync_directory(&source_parent).await?;
    if source_parent != dest_parent {
        sync_directory(&dest_parent).await?;
    }
    Ok(())
}

/// Unarchive-specific wrapper so tests can deterministically fail the exact
/// durability barrier without weakening or bypassing the real directory sync.
async fn sync_unarchive_rename_parents(source: &str, dest: &str) -> io::Result<()> {
    #[cfg(test)]
    {
        let failure = UNARCHIVE_PARENT_SYNC_FAILURE.get_or_init(|| StdMutex::new(None));
        let mut configured = failure.lock().expect("unarchive parent sync failure");
        let should_fail = configured.as_mut().is_some_and(|failure| {
            let matches = (failure.source == source && failure.dest == dest)
                || (failure.source == dest && failure.dest == source);
            if matches {
                failure.remaining = failure.remaining.saturating_sub(1);
            }
            matches
        });
        if configured
            .as_ref()
            .is_some_and(|failure| failure.remaining == 0)
        {
            configured.take();
        }
        drop(configured);
        if should_fail {
            return Err(io::Error::other(format!(
                "injected unarchive parent sync failure for {source} -> {dest}"
            )));
        }
    }
    sync_rename_parents(source, dest).await
}

#[cfg(test)]
struct UnarchiveParentSyncFailure {
    source: String,
    dest: String,
    remaining: usize,
}

#[cfg(test)]
static UNARCHIVE_PARENT_SYNC_FAILURE: std::sync::OnceLock<
    StdMutex<Option<UnarchiveParentSyncFailure>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
fn fail_unarchive_parent_sync_times(source: &str, dest: &str, times: usize) {
    assert!(times > 0);
    UNARCHIVE_PARENT_SYNC_FAILURE
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .expect("unarchive parent sync failure")
        .replace(UnarchiveParentSyncFailure {
            source: source.to_string(),
            dest: dest.to_string(),
            remaining: times,
        });
}

#[cfg(test)]
fn clear_unarchive_parent_sync_failure() {
    UNARCHIVE_PARENT_SYNC_FAILURE
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .expect("unarchive parent sync failure")
        .take();
}

#[cfg(not(test))]
async fn resume_for_write_test_pause(_file: &str) {}

#[cfg(test)]
#[derive(Clone)]
struct ResumeForWritePauseGate {
    entered: std::sync::Arc<tokio::sync::Notify>,
    release: std::sync::Arc<tokio::sync::Notify>,
}

#[cfg(test)]
static RESUME_FOR_WRITE_PAUSE_GATE: std::sync::OnceLock<
    StdMutex<std::collections::HashMap<String, ResumeForWritePauseGate>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
async fn resume_for_write_test_pause(file: &str) {
    let gate = RESUME_FOR_WRITE_PAUSE_GATE
        .get_or_init(|| StdMutex::new(std::collections::HashMap::new()))
        .lock()
        .expect("resume-for-write pause gate")
        .get(file)
        .cloned();
    if let Some(gate) = gate {
        gate.entered.notify_one();
        gate.release.notified().await;
    }
}

#[cfg(test)]
fn pause_resume_for_write_at(
    file: &str,
) -> (
    std::sync::Arc<tokio::sync::Notify>,
    std::sync::Arc<tokio::sync::Notify>,
) {
    let entered = std::sync::Arc::new(tokio::sync::Notify::new());
    let release = std::sync::Arc::new(tokio::sync::Notify::new());
    RESUME_FOR_WRITE_PAUSE_GATE
        .get_or_init(|| StdMutex::new(std::collections::HashMap::new()))
        .lock()
        .expect("resume-for-write pause gate")
        .insert(
            file.to_string(),
            ResumeForWritePauseGate {
                entered: entered.clone(),
                release: release.clone(),
            },
        );
    (entered, release)
}

#[cfg(test)]
fn clear_resume_for_write_pause(file: &str) {
    RESUME_FOR_WRITE_PAUSE_GATE
        .get_or_init(|| StdMutex::new(std::collections::HashMap::new()))
        .lock()
        .expect("resume-for-write pause gate")
        .remove(file);
}

#[cfg(not(test))]
async fn unarchive_test_pause(_thread_id: &str, _point: UnarchiveTestPoint) {}

#[cfg(test)]
#[derive(Clone)]
struct UnarchivePauseGate {
    thread_id: String,
    point: UnarchiveTestPoint,
    entered: std::sync::Arc<tokio::sync::Notify>,
    release: std::sync::Arc<tokio::sync::Notify>,
}

#[cfg(test)]
async fn unarchive_test_pause(thread_id: &str, point: UnarchiveTestPoint) {
    let gate = UNARCHIVE_PAUSE_GATE
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .expect("unarchive pause gate")
        .as_ref()
        .filter(|gate| gate.thread_id == thread_id && gate.point == point)
        .cloned();
    if let Some(gate) = gate {
        gate.entered.notify_one();
        gate.release.notified().await;
    }
}

#[cfg(test)]
fn pause_unarchive_at(
    thread_id: &str,
    point: UnarchiveTestPoint,
) -> (
    std::sync::Arc<tokio::sync::Notify>,
    std::sync::Arc<tokio::sync::Notify>,
) {
    let entered = std::sync::Arc::new(tokio::sync::Notify::new());
    let release = std::sync::Arc::new(tokio::sync::Notify::new());
    UNARCHIVE_PAUSE_GATE
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .expect("unarchive pause gate")
        .replace(UnarchivePauseGate {
            thread_id: thread_id.to_string(),
            point,
            entered: entered.clone(),
            release: release.clone(),
        });
    (entered, release)
}

#[cfg(test)]
fn clear_unarchive_pause() {
    UNARCHIVE_PAUSE_GATE
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .expect("unarchive pause gate")
        .take();
}

#[cfg(test)]
static UNARCHIVE_PAUSE_GATE: std::sync::OnceLock<StdMutex<Option<UnarchivePauseGate>>> =
    std::sync::OnceLock::new();

async fn move_unarchive_main(source: &str, dest: &str) -> io::Result<()> {
    #[cfg(test)]
    {
        let fail_source = UNARCHIVE_FAIL_SOURCE.get_or_init(|| StdMutex::new(None));
        let mut configured = fail_source.lock().expect("unarchive failpoint");
        if configured.as_deref() == Some(source) {
            configured.take();
            return Err(io::Error::other("injected unarchive main move failure"));
        }
    }
    tokio::fs::rename(source, dest).await
}

#[cfg(test)]
fn fail_next_unarchive_main_move(source: &str) {
    UNARCHIVE_FAIL_SOURCE
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .expect("unarchive failpoint")
        .replace(source.to_string());
}

#[cfg(test)]
static UNARCHIVE_FAIL_SOURCE: std::sync::OnceLock<StdMutex<Option<String>>> =
    std::sync::OnceLock::new();

fn file_is_under(file: &str, root: &str) -> bool {
    let file = Path::new(file);
    let root = Path::new(root);
    file.parent().map(|parent| parent == root).unwrap_or(false)
}

// The agentId reaches these builders from a model-controlled `resume` value
// (the `task` tool's free-form string), so it MUST be sanitized to a single
// safe path component — otherwise `resume: '../../etc/x'` would escape the
// subagents dir. A legitimate uuid agentId sanitizes to itself, so resume
// round-trips.
fn agent_file_path(dir: &str, agent_id: &str) -> String {
    join_path(
        dir,
        &format!("agent-{}.jsonl", sanitize_path_component(agent_id)),
    )
}

fn agent_meta_path(dir: &str, agent_id: &str) -> String {
    join_path(
        dir,
        &format!("agent-{}.meta.json", sanitize_path_component(agent_id)),
    )
}

/// Read + parse a sub-agent meta sidecar; `None` when missing/unparseable.
async fn read_sub_agent_meta(file: &str) -> Option<SubAgentMeta> {
    let raw = match tokio::fs::read(file).await {
        Ok(bytes) => NodeBufferUtf8::decode(&bytes),
        Err(_) => return None,
    };
    // Malformed sidecar → treat as absent.
    runtime_codec::decode_text(&raw).ok()
}

struct CatalogSnapshot {
    id: String,
    filter: ListOptions,
    rows: Arc<Vec<ThreadSummary>>,
    membership: RuntimeString,
    metadata: RuntimeString,
    created: Instant,
}

impl CatalogSnapshot {
    fn page(&self, start: usize, limit: usize) -> ThreadPage {
        let end = start.saturating_add(limit).min(self.rows.len());
        ThreadPage {
            items: self.rows[start..end].to_vec(),
            next_cursor: (end < self.rows.len()).then(|| {
                RuntimeString::from(base64url_encode(
                    serde_json::json!({
                        "version": 2, "snapshot": self.id, "offset": end,
                    })
                    .to_string()
                    .as_bytes(),
                ))
            }),
            membership_fingerprint: Some(self.membership.clone()),
            metadata_fingerprint: Some(self.metadata.clone()),
        }
    }
}

fn catalog_filter(options: &ListOptions) -> ListOptions {
    ListOptions {
        include_archived: Some(options.include_archived == Some(true)),
        role: Some(
            options
                .role
                .unwrap_or(crate::schemas::rollout::SessionRole::Main),
        ),
        cwd: options.cwd.clone(),
        ..ListOptions::default()
    }
}

fn catalog_membership(rows: &[ThreadSummary]) -> RuntimeString {
    let mut ids: Vec<_> = rows.iter().map(|row| row.id.clone()).collect();
    ids.sort();
    let encoded = runtime_codec::encode_text(&ids).expect("catalog IDs encode exactly");
    RuntimeString::from(format!("{:x}", Sha256::digest(encoded.to_node_utf8())))
}

enum CatalogCursor {
    Snapshot { id: String, offset: usize },
    Legacy(Cursor),
}

fn decode_catalog_cursor(token: &RuntimeString) -> io::Result<CatalogCursor> {
    let invalid = || {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid session catalog cursor",
        )
    };
    let raw = token.try_to_utf8().map_err(|_| invalid())?;
    let bytes = base64url_decode(&raw).ok_or_else(invalid)?;
    let text = String::from_utf8(bytes).map_err(|_| invalid())?;
    let parsed = runtime_json::parse(&RuntimeString::from(text)).map_err(|_| invalid())?;
    if let Some(version) = parsed.get("version") {
        if version.as_f64() != Some(2.0) {
            return Err(invalid());
        }
        let id = parsed
            .get("snapshot")
            .and_then(RuntimeJson::as_str)
            .and_then(|id| id.try_to_utf8().ok())
            .filter(|id| !id.is_empty())
            .ok_or_else(invalid)?;
        let offset = parsed
            .get("offset")
            .and_then(RuntimeJson::as_f64)
            .filter(|offset| {
                offset.is_finite()
                    && *offset >= 0.0
                    && offset.fract() == 0.0
                    && *offset <= usize::MAX as f64
            })
            .ok_or_else(invalid)? as usize;
        return Ok(CatalogCursor::Snapshot { id, offset });
    }
    decode_cursor(token)
        .map(CatalogCursor::Legacy)
        .ok_or_else(invalid)
}

/// Legacy opaque keyset cursor, accepted for clients from before catalog snapshots.
struct Cursor {
    updated_at: f64,
    id: RuntimeString,
}

fn decode_cursor(token: &RuntimeString) -> Option<Cursor> {
    let token = token.try_to_utf8().ok()?;
    let bytes = base64url_decode(&token)?;
    let raw = String::from_utf8(bytes).ok()?;
    let parsed = runtime_json::parse(&RuntimeString::from(raw)).ok()?;
    Some(Cursor {
        updated_at: parsed
            .get("updatedAt")
            .or_else(|| parsed.get("createdAt"))?
            .as_f64()?,
        id: parsed.get("id")?.as_str()?.clone(),
    })
}

/// Find the index of the first summary strictly after the cursor in a
/// newest-first list. Stable even if items were added/removed since the cursor
/// was issued: we skip everything at or newer than the cursor key.
fn index_after_cursor(summaries: &[ThreadSummary], cursor: &Cursor) -> usize {
    for (i, s) in summaries.iter().enumerate() {
        let updated_at = s.updated_at as f64;
        let newer_or_equal = updated_at > cursor.updated_at
            || (updated_at == cursor.updated_at && s.id >= cursor.id);
        if !newer_or_equal {
            return i;
        }
    }
    summaries.len()
}

fn base_name(path: &str) -> &str {
    match path.rfind(['/', '\\']) {
        Some(idx) => &path[idx + 1..],
        None => path,
    }
}

/// Node projects JavaScript path text to UTF-8 only when addressing the host
/// filesystem. Keep the rollout/header carrier exact everywhere before this
/// terminal boundary.
fn runtime_path_component_to_store_host(value: &RuntimeString) -> String {
    runtime_string_to_store_host(value)
}

fn join_path(base: &str, name: &str) -> String {
    native_text_to_store_host(Path::new(base).join(name).as_os_str())
}

/// Resolve a path to an absolute, lexically-normalized form (`.`/`..` segments
/// applied, trailing separators dropped) without touching the filesystem —
/// relative inputs resolve against the process working directory.
fn resolve_path(path: &str) -> PathBuf {
    let path = Path::new(path);
    let absolute: PathBuf = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().unwrap_or_default().join(path)
    };
    let mut out = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                // `..` above the root clamps at the root.
                out.pop();
            }
            other => out.push(other),
        }
    }
    out
}

fn mtime_ms(modified: std::time::SystemTime) -> f64 {
    match modified.duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => duration.as_secs_f64() * 1000.0,
        Err(err) => -(err.duration().as_secs_f64() * 1000.0),
    }
}

/// List a directory's entry names. A genuinely absent directory is an empty
/// namespace; permission, wrong-type, and iteration failures remain errors.
async fn read_dir_names(dir: &str) -> io::Result<Option<Vec<String>>> {
    let mut read_dir = match tokio::fs::read_dir(dir).await {
        Ok(read_dir) => read_dir,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut names: Vec<String> = Vec::new();
    loop {
        match read_dir.next_entry().await {
            Ok(Some(entry)) => names.push(native_text_to_store_host(&entry.file_name())),
            Ok(None) => return Ok(Some(names)),
            Err(error) => return Err(error),
        }
    }
}

/// Acquire the session's create-exclusive single-writer lock, reclaiming a
/// STALE lock left behind by a crashed writer. The lock file records the
/// holder's PID; when it already exists we read it and, if that process is gone
/// (or a legacy record is missing/garbage), unlink and retry once. So a
/// `kill -9` no longer wedges a later
/// resume, while a lock held by a LIVE writer is still refused with the
/// original error. Same-host only: the signal-0 probe sees local processes,
/// which is exactly the shared-worktree / one-host concurrency this lock
/// guards.
async fn acquire_session_lock(lock_path: &str, file: &str) -> io::Result<tokio::fs::File> {
    for attempt in 0..2 {
        match publish_session_lock(lock_path) {
            Ok(handle) => return Ok(handle),
            Err(err) => {
                if err.kind() != io::ErrorKind::AlreadyExists {
                    return Err(err);
                }
                if attempt == 0 && reclaim_stale_lock(lock_path).await? {
                    continue;
                }
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!("session {file} is locked by another writer"),
                ));
            }
        }
    }
    // Unreachable: the loop returns a handle or errors on both attempts.
    Err(io::Error::new(
        io::ErrorKind::WouldBlock,
        format!("session {file} is locked by another writer"),
    ))
}

static SESSION_LOCK_CANDIDATE_COUNTER: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

struct SessionLockCandidate {
    path: String,
}

impl Drop for SessionLockCandidate {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Atomically publish a fully-stamped writer sidecar. Writing directly after a
/// create-exclusive open exposes an empty-file interval in which another
/// process can misclassify the live owner as a crashed pre-stamp writer. A hard
/// link publishes the prepared inode under the canonical name in one
/// create-if-absent operation, so contenders observe either no lock or the full
/// PID record, never an intermediate state.
fn publish_session_lock(lock_path: &str) -> io::Result<tokio::fs::File> {
    use std::io::Write as _;

    let candidate_path = format!(
        "{lock_path}.candidate-{}-{}",
        std::process::id(),
        SESSION_LOCK_CANDIDATE_COUNTER.fetch_add(1, Ordering::SeqCst)
    );
    let candidate = SessionLockCandidate {
        path: candidate_path.clone(),
    };
    let mut handle = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&candidate_path)?;
    handle.write_all(format!("{}\n", std::process::id()).as_bytes())?;
    handle.sync_all()?;
    drop(handle);

    std::fs::hard_link(&candidate_path, lock_path)?;
    let handle = match std::fs::OpenOptions::new().write(true).open(lock_path) {
        Ok(handle) => handle,
        Err(error) => {
            let _ = std::fs::remove_file(lock_path);
            return Err(error);
        }
    };
    drop(candidate); // remove only the non-canonical candidate link
    Ok(tokio::fs::File::from_std(handle))
}

/// Reclaim the lock IFF its recorded holder is no longer alive (or the record
/// is unusable). Returns true only to the ONE caller that atomically wins the
/// reclaim, so the create-exclusive retry that follows can't double-acquire. A
/// plain unlink+recreate would race: two resumers could both read the dead PID,
/// both unlink, then both create-exclusive after the other's unlink → two
/// writers on one session. `rename` is atomic, so of N racing reclaimers
/// exactly one moves the stale file aside and proceeds; the rest are refused
/// (they can retry the resume).
async fn reclaim_stale_lock(lock_path: &str) -> io::Result<bool> {
    let mut holder_pid: Option<i32> = None;
    // Legacy malformed records are reclaimable. New writers atomically publish
    // a fully-stamped inode, so they never expose an empty pre-stamp interval.
    let bytes = match tokio::fs::read(lock_path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error), // An unreadable live lock is not safe to steal.
    };
    let raw = NodeBufferUtf8::decode(&bytes)
        .try_to_utf8()
        .expect("Node UTF-8 decode yields Unicode scalar lock text");
    if let Some(parsed) = parse_int_prefix(raw.trim()) {
        if parsed > 0 {
            // A recorded value too large to be a real PID behaves like a
            // dead holder (the probe cannot find it), so it stays reclaimable.
            holder_pid = i32::try_from(parsed).ok();
        }
    }
    if let Some(pid) = holder_pid {
        // A running/sleeping holder is a real conflict. A STOPPED holder is not
        // actively writing and, if orphaned, never will be — reclaim it so a
        // restart can take over instead of wedging on a dead-but-existing PID.
        if is_lock_holder_alive(pid, lock_path) && !is_process_stopped(pid) {
            return Ok(false); // live holder → real conflict
        }
    }
    // Atomically claim the stale lock: only the winner of the rename removes it
    // and returns true.
    let claimed = format!("{lock_path}.stale-{}", std::process::id());
    if let Err(error) = tokio::fs::rename(lock_path, &claimed).await {
        if error.kind() == io::ErrorKind::NotFound {
            return Ok(false); // another reclaimer already moved it
        }
        return Err(io::Error::new(
            error.kind(),
            format!("cannot reclaim stale session lock {lock_path}: {error}"),
        ));
    }
    let _ = tokio::fs::remove_file(&claimed).await;
    Ok(true)
}

fn runtime_string_to_store_host(value: &RuntimeString) -> String {
    String::from_utf8(value.to_node_utf8()).expect("Node UTF-8 projection yields valid UTF-8")
}

fn native_text_to_store_host(value: &OsStr) -> String {
    runtime_string_to_store_host(&runtime_string_from_native(value))
}

/// Parse the leading base-10 integer of a string (optional sign, then digits;
/// trailing garbage ignored). `None` when there are no digits or the value
/// overflows.
fn parse_int_prefix(s: &str) -> Option<i64> {
    let t = s.trim_start();
    let (negative, digits) = match t.as_bytes().first() {
        Some(b'-') => (true, &t[1..]),
        Some(b'+') => (false, &t[1..]),
        _ => (false, t),
    };
    let end = digits.bytes().take_while(|b| b.is_ascii_digit()).count();
    if end == 0 {
        return None;
    }
    let value = digits[..end].parse::<i64>().ok()?;
    Some(if negative { -value } else { value })
}

/// True if a local process with this PID currently exists (signal 0 =
/// existence probe). ESRCH → no such process (dead). EPERM → exists but not
/// ours to signal (alive).
#[cfg(unix)]
fn is_process_alive(pid: i32) -> bool {
    let rc = unsafe { libc::kill(pid as libc::pid_t, 0) };
    if rc == 0 {
        return true;
    }
    io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

/// True if a local process with this PID currently exists on Windows.
#[cfg(windows)]
fn is_process_alive(pid: i32) -> bool {
    use winapi::shared::winerror::{ERROR_ACCESS_DENIED, ERROR_INVALID_PARAMETER};
    use winapi::um::handleapi::CloseHandle;
    use winapi::um::minwinbase::STILL_ACTIVE;
    use winapi::um::processthreadsapi::{GetExitCodeProcess, OpenProcess};
    use winapi::um::winnt::PROCESS_QUERY_LIMITED_INFORMATION;

    let Ok(pid) = u32::try_from(pid) else {
        return false;
    };
    if pid == 0 {
        return false;
    }

    let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if handle.is_null() {
        let error = io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or_default() as u32;
        return match error {
            ERROR_INVALID_PARAMETER => false,
            ERROR_ACCESS_DENIED => true,
            _ => true,
        };
    }

    let mut exit_code = 0;
    let queried = unsafe { GetExitCodeProcess(handle, &mut exit_code) };
    unsafe { CloseHandle(handle) };
    queried == 0 || exit_code == STILL_ACTIVE
}

#[cfg(not(windows))]
fn is_lock_holder_alive(pid: i32, _lock_path: &str) -> bool {
    is_process_alive(pid)
}

#[cfg(windows)]
fn is_lock_holder_alive(pid: i32, lock_path: &str) -> bool {
    use std::os::windows::fs::MetadataExt;

    if !is_process_alive(pid) {
        return false;
    }
    let created = std::fs::metadata(lock_path)
        .ok()
        .map(|metadata| metadata.creation_time());
    let started = windows_process_creation_time(pid);
    match (started, created) {
        (Some(started), Some(created)) => !pid_reused_after_lock(started, created),
        _ => true, // An uncertain identity must not create a second writer.
    }
}

#[cfg(windows)]
fn windows_process_creation_time(pid: i32) -> Option<u64> {
    use winapi::shared::minwindef::FILETIME;
    use winapi::um::handleapi::CloseHandle;
    use winapi::um::processthreadsapi::{GetProcessTimes, OpenProcess};
    use winapi::um::winnt::PROCESS_QUERY_LIMITED_INFORMATION;

    let handle = unsafe {
        OpenProcess(
            PROCESS_QUERY_LIMITED_INFORMATION,
            0,
            u32::try_from(pid).ok()?,
        )
    };
    if handle.is_null() {
        return None;
    }
    let mut creation = FILETIME {
        dwLowDateTime: 0,
        dwHighDateTime: 0,
    };
    let mut exit = creation;
    let mut kernel = creation;
    let mut user = creation;
    let queried =
        unsafe { GetProcessTimes(handle, &mut creation, &mut exit, &mut kernel, &mut user) };
    unsafe { CloseHandle(handle) };
    (queried != 0)
        .then_some((u64::from(creation.dwHighDateTime) << 32) | u64::from(creation.dwLowDateTime))
}

#[cfg(any(windows, test))]
fn pid_reused_after_lock(process_created: u64, lock_created: u64) -> bool {
    // Both values are Windows FILETIME ticks (100 ns); allow one second for
    // filesystem timestamp rounding before deciding the PID belongs to a new process.
    process_created > lock_created.saturating_add(10_000_000)
}

/// True if the process is STOPPED (job-control `SIGSTOP`/`T`, or ptrace-stopped).
///
/// A stopped process still answers `kill(pid, 0)` as alive, so the plain
/// liveness probe alone treats it as a real conflict and never reclaims its
/// writer lock. But a stopped holder is NOT actively writing, and an orphaned
/// stopped engine left by a previous run (PPID reparented to init after its
/// shell was killed) will never resume to release the lock — so the next engine
/// resuming that session would wedge forever (`session <id> is locked by another
/// writer`) and the renderer paints a blank transcript. Treating a stopped
/// holder as reclaimable lets a fresh engine take over. Best-effort: any probe
/// failure returns false, falling back to the plain liveness check.
#[cfg(unix)]
fn is_process_stopped(pid: i32) -> bool {
    // `ps -o stat=` prints the process state code; a leading `T` (macOS & Linux)
    // is job-control stopped, `t` (Linux) is trace-stopped. Reclaim only runs on
    // lock contention, so a one-shot `ps` is cheap and avoids depending on
    // libc's (unexposed) `kinfo_proc` layout. Any failure → false, which falls
    // back to the plain liveness check.
    let Ok(output) = std::process::Command::new("ps")
        .args(["-o", "stat=", "-p", &pid.to_string()])
        .output()
    else {
        return false;
    };
    matches!(
        output.stdout.iter().find(|b| !b.is_ascii_whitespace()),
        Some(b'T') | Some(b't')
    )
}

#[cfg(not(unix))]
fn is_process_stopped(_pid: i32) -> bool {
    false
}

const B64URL_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// Encode bytes as unpadded url-safe base64.
fn base64url_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = u32::from(chunk[0]);
        let b1 = u32::from(chunk.get(1).copied().unwrap_or(0));
        let b2 = u32::from(chunk.get(2).copied().unwrap_or(0));
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(B64URL_ALPHABET[(n >> 18) as usize & 63] as char);
        out.push(B64URL_ALPHABET[(n >> 12) as usize & 63] as char);
        if chunk.len() > 1 {
            out.push(B64URL_ALPHABET[(n >> 6) as usize & 63] as char);
        }
        if chunk.len() > 2 {
            out.push(B64URL_ALPHABET[n as usize & 63] as char);
        }
    }
    out
}

/// Decode url-safe base64 (padding tolerated); `None` on any invalid input.
fn base64url_decode(token: &str) -> Option<Vec<u8>> {
    fn value(c: u8) -> Option<u32> {
        match c {
            b'A'..=b'Z' => Some(u32::from(c - b'A')),
            b'a'..=b'z' => Some(u32::from(c - b'a') + 26),
            b'0'..=b'9' => Some(u32::from(c - b'0') + 52),
            b'-' => Some(62),
            b'_' => Some(63),
            _ => None,
        }
    }
    let bytes: Vec<u8> = token.bytes().filter(|&b| b != b'=').collect();
    if bytes.len() % 4 == 1 {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
    for chunk in bytes.chunks(4) {
        let mut n: u32 = 0;
        for (i, &b) in chunk.iter().enumerate() {
            n |= value(b)? << (18 - 6 * i);
        }
        out.push((n >> 16) as u8);
        if chunk.len() > 2 {
            out.push((n >> 8) as u8);
        }
        if chunk.len() > 3 {
            out.push(n as u8);
        }
    }
    Some(out)
}

#[cfg(test)]
#[path = "jsonl_store_external_import_tests.rs"]
mod external_import_tests;

fn invalidate_migration_indexes(root: &Path, archive: &Path) -> io::Result<()> {
    let index_path = root.join("threads-index.sqlite3");
    if index_path.exists() {
        crate::store::thread_index::TurnIndexDb::open(&index_path.to_string_lossy())?
            .invalidate_all()?;
    }
    for path in [
        root.join(".session-header-index-v1.json"),
        archive.join(".session-header-index-v1.json"),
        root.join(".profile-activity-cache.json"),
    ] {
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}
fn recover_one_pending(root: &Path, archive: &Path, path: &Path) -> io::Result<()> {
    let Some(_maintenance) = compression::maintenance_lock(root)? else {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "rollout maintenance is active",
        ));
    };
    let Some(_writer) = compression::lease(PathBuf::from(compression::writer_lock_path(
        &path.to_string_lossy(),
    )))?
    else {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "rollout writer is active",
        ));
    };
    let state = crate::store::rollout_migration::recover(path, &AtomicBool::new(false))?;
    if state == crate::store::rollout_migration::RecoveryState::NeedsIndexRefresh {
        invalidate_migration_indexes(root, archive)?;
        crate::store::rollout_migration::acknowledge(path)?;
    }
    Ok(())
}
fn migration_value(
    report: &crate::store::rollout_migration::MigrationReport,
    path: &Path,
) -> io::Result<RuntimeJson> {
    let text = serde_json::to_string(report).map_err(io::Error::other)?;
    let mut value = runtime_json::parse(&RuntimeString::from(text)).map_err(io::Error::other)?;
    if let RuntimeJson::Object(object) = &mut value {
        object.insert(
            "path",
            RuntimeJson::String(path.to_string_lossy().into_owned().into()),
        );
    }
    Ok(value)
}
fn migrate_store_rollouts(
    root: &Path,
    archive: &Path,
    request: crate::store::session_store::RolloutMigrationRequest,
    cancelled: &AtomicBool,
) -> io::Result<crate::store::session_store::RolloutMigrationReport> {
    use crate::store::rollout_migration::{self as migration, MigrationKind, MigrationStatus};
    use crate::store::session_store::{RolloutMigrationMode, RolloutMigrationReport};
    compression::check(cancelled)?;
    let Some(_maintenance) = compression::maintenance_lock(root)? else {
        return Ok(RolloutMigrationReport {
            busy: true,
            files: Vec::new(),
        });
    };
    let mut report = RolloutMigrationReport::default();
    for original in compression::list_rollouts(root, archive)? {
        compression::check(cancelled)?;
        let result = (|| -> io::Result<RuntimeJson> {
            let (parent_id, parent_main) = compression::parent_identity(root, &original);
            let lock_root = root.join(".lifecycle-locks");
            std::fs::create_dir_all(&lock_root)?;
            let lifecycle = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(lock_root.join(format!("{:x}.lock", Sha256::digest(parent_id.as_bytes()))))?;
            FileExt::try_lock_exclusive(&lifecycle)?;
            if parent_main != compression::plain_path(&original)
                && Path::new(&compression::writer_lock_path(
                    &parent_main.to_string_lossy(),
                ))
                .exists()
            {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "parent rollout writer is active",
                ));
            }
            let Some(_writer) = compression::lease(PathBuf::from(compression::writer_lock_path(
                &original.to_string_lossy(),
            )))?
            else {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "rollout writer is active",
                ));
            };
            let kind = if original
                .parent()
                .and_then(Path::file_name)
                .is_some_and(|name| name == "subagents")
            {
                MigrationKind::Subagent
            } else {
                MigrationKind::Main
            };
            if request.mode == RolloutMigrationMode::Apply && migration::pending(&original)? {
                let state = migration::recover(&original, cancelled)?;
                if state == migration::RecoveryState::NeedsIndexRefresh {
                    invalidate_migration_indexes(root, archive)?;
                    migration::acknowledge(&original)?;
                }
            }
            let plan = migration::preview(&original, kind, cancelled)?;
            if request.mode == RolloutMigrationMode::DryRun {
                return migration_value(&plan.report, &original);
            }
            if let Some(expected) = &request.expected_source_revisions {
                let key = RuntimeString::from(original.to_string_lossy().into_owned());
                let value = expected.get(&key).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "migration source was not included in the preview revision set",
                    )
                })?;
                let actual =
                    serde_json::to_value(&plan.report.source_revision).map_err(io::Error::other)?;
                let supplied: serde_json::Value = serde_json::from_str(
                    &runtime_json::stringify(value)
                        .try_to_utf8()
                        .map_err(io::Error::other)?,
                )
                .map_err(io::Error::other)?;
                if actual != supplied {
                    return Err(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "migration source revision changed since preview",
                    ));
                }
            }
            if plan.report.status == MigrationStatus::Unchanged {
                return migration_value(&plan.report, &original);
            }
            let plain = compression::materialize(&original, cancelled)?;
            let plan = if plain != original {
                migration::preview(&plain, kind, cancelled)?
            } else {
                plan
            };
            let mut file_report = migration::apply_under_lease(&plain, &plan, cancelled)?;
            if file_report.needs_index_refresh {
                invalidate_migration_indexes(root, archive)?;
                migration::acknowledge(&plain)?;
                file_report.needs_index_refresh = false;
            }
            migration_value(&file_report, &plain)
        })();
        match result {
            Ok(value) => report.files.push(value),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => return Err(error),
            Err(error) => {
                let mut object = crate::runtime_values::RuntimeObject::new();
                object.insert(
                    "path",
                    RuntimeJson::String(original.to_string_lossy().into_owned().into()),
                );
                object.insert(
                    "status",
                    RuntimeJson::String(
                        if error.kind() == io::ErrorKind::WouldBlock {
                            "conflict"
                        } else {
                            "failed"
                        }
                        .into(),
                    ),
                );
                object.insert("error", RuntimeJson::String(error.to_string().into()));
                report.files.push(RuntimeJson::Object(object));
            }
        }
    }
    Ok(report)
}

async fn ensure_inactive_children(parent: &str) -> io::Result<()> {
    let dir = sub_agent_dir(parent);
    for name in read_dir_names(&dir).await?.unwrap_or_default() {
        if !name.ends_with(".jsonl.lock") {
            continue;
        }
        let lock = join_path(&dir, &name);
        let child = lock.strip_suffix(".lock").unwrap();
        let lease =
            TemporarySessionLock::new(acquire_session_lock(&lock, child).await?, lock.clone());
        lease.release().await?;
    }
    Ok(())
}

async fn materialize_with_lease(
    path: &str,
    lease: TemporarySessionLock,
) -> io::Result<(String, TemporarySessionLock)> {
    let path = PathBuf::from(path);
    if PathBuf::from(format!(
        "{}.migration.pending",
        compression::plain_path(&path).display()
    ))
    .exists()
    {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "rollout migration requires index recovery before writer attachment",
        ));
    }

    let cancellation = crate::store::runtime_resume::ReadCancellation::new();
    let cancelled = cancellation.0.clone();
    tokio::task::spawn_blocking(move || {
        let plain = compression::materialize(&path, &cancelled)?;
        Ok((plain.to_string_lossy().into_owned(), lease))
    })
    .await
    .map_err(io::Error::other)?
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::RuntimeObject;
    use crate::schemas::content::{ContentBlock, TextBlock};
    use crate::schemas::message::{AssistantMessage, Message, UserMessage};
    use crate::types::permission::ExternalPermissionMode;
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    // A stopped holder (e.g. an orphaned engine left by a previous run) still
    // answers kill(pid,0) as alive; the writer-lock reclaim must recognise it as
    // reclaimable, or resuming that session wedges forever and the transcript
    // renders blank. A genuinely running holder stays a real conflict.
    #[cfg(unix)]
    #[tokio::test]
    async fn reclaim_steals_a_stopped_holders_lock_but_not_a_running_one() {
        use std::io::Write;
        let dir = temp_dir();
        std::fs::create_dir_all(&dir).unwrap();

        // RUNNING holder → real conflict → lock left intact.
        let mut running = std::process::Command::new("sleep")
            .arg("5")
            .spawn()
            .unwrap();
        let running_pid = running.id() as i32;
        let running_lock = dir.join("running.jsonl.lock");
        {
            let mut f = std::fs::File::create(&running_lock).unwrap();
            writeln!(f, "{running_pid}").unwrap();
        }
        assert!(is_process_alive(running_pid));
        assert!(!is_process_stopped(running_pid));
        assert!(
            !reclaim_stale_lock(running_lock.to_str().unwrap())
                .await
                .unwrap(),
            "a live running holder is a real conflict, not reclaimable"
        );
        assert!(
            running_lock.exists(),
            "the running holder's lock stays intact"
        );
        let _ = running.kill();
        let _ = running.wait();

        // STOPPED holder → reclaimable (an orphaned stopped engine never releases).
        let mut stopped = std::process::Command::new("sleep")
            .arg("5")
            .spawn()
            .unwrap();
        let stopped_pid = stopped.id() as i32;
        unsafe {
            libc::kill(stopped_pid as libc::pid_t, libc::SIGSTOP);
        }
        let mut reached_stopped = false;
        for _ in 0..100 {
            if is_process_stopped(stopped_pid) {
                reached_stopped = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(reached_stopped, "the child reached the stopped state");
        assert!(
            is_process_alive(stopped_pid),
            "a stopped process still answers kill(pid,0) as alive"
        );
        let stopped_lock = dir.join("stopped.jsonl.lock");
        {
            let mut f = std::fs::File::create(&stopped_lock).unwrap();
            writeln!(f, "{stopped_pid}").unwrap();
        }
        assert!(
            reclaim_stale_lock(stopped_lock.to_str().unwrap())
                .await
                .unwrap(),
            "a stopped holder's lock is reclaimable"
        );
        assert!(
            !stopped_lock.exists(),
            "the stopped holder's lock is removed on reclaim"
        );
        unsafe {
            libc::kill(stopped_pid as libc::pid_t, libc::SIGCONT);
        }
        let _ = stopped.kill();
        let _ = stopped.wait();

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn unarchive_intent_uses_the_exact_runtime_codec() {
        let intent = UnarchiveIntent {
            version: UNARCHIVE_JOURNAL_VERSION,
            operation: "unarchive".to_string(),
            thread_id: "thread-1".to_string(),
            file_name: "1700000000-thread-1.jsonl".to_string(),
            data_expected: true,
        };
        let encoded = runtime_codec::encode_text(&intent).expect("intent encodes");
        assert_eq!(
            encoded,
            RuntimeString::from(
                r#"{"version":1,"operation":"unarchive","threadId":"thread-1","fileName":"1700000000-thread-1.jsonl","dataExpected":true}"#,
            )
        );
        assert_eq!(
            runtime_codec::decode_text::<UnarchiveIntent>(&encoded).expect("intent decodes"),
            intent
        );

        let with_unknown = RuntimeString::from(
            r#"{"version":1,"operation":"unarchive","threadId":"thread-1","fileName":"1700000000-thread-1.jsonl","dataExpected":true,"unexpected":1}"#,
        );
        let error = runtime_codec::decode_text::<UnarchiveIntent>(&with_unknown)
            .expect_err("journal intent rejects unknown fields");
        assert_eq!(error.kind(), &RuntimeCodecErrorKind::UnknownField);
    }

    fn temp_dir() -> PathBuf {
        let mut dir = std::env::temp_dir();
        let unique = format!(
            "ae-store-{}-{}-{}",
            std::process::id(),
            TEMP_COUNTER.fetch_add(1, Ordering::SeqCst),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        dir.push(unique);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn store_at(dir: &Path) -> JsonlSessionStore {
        JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(
                dir.to_str()
                    .expect("test temp directory is Unicode scalar text")
                    .to_owned(),
            ),
            archive_dir: None,
        })
    }

    fn search_options(query: &str) -> SessionSearchOptions {
        SessionSearchOptions {
            query: query.into(),
            ..Default::default()
        }
    }

    fn search_signal() -> crate::types::abort::AbortSignal {
        crate::types::abort::AbortSignal::new()
    }

    #[tokio::test]
    async fn persists_background_process_lifecycle_records() {
        let dir = temp_dir();
        let store = store_at(&dir);
        let live = store
            .create(meta_with("background-process-session", 1_700_000_000, "/x"))
            .await
            .unwrap();
        let record = crate::schemas::background_process::BackgroundProcessRecord {
            shell_id: "shell-1".into(),
            command: "exit 7".into(),
            cwd: "/x".into(),
            status: crate::schemas::background_process::BackgroundProcessStatus::Failed,
            started_at_ms: 1,
            updated_at_ms: 2,
            completed_at_ms: Some(2),
            exit_code: Some(7),
            termination_signal: None,
            failure: Some("failed".into()),
            stop_requested: false,
            output_start_offset: 0,
            output_next_offset: 6,
            dropped_bytes: 0,
            output_available: true,
        };
        live.append_background_process_lifecycle(record.clone())
            .await;
        live.shutdown().await.unwrap();

        let resumed = store.resume("background-process-session").await.unwrap();
        assert!(resumed.raw_items.iter().any(|item| matches!(
            item,
            RolloutItem::BackgroundProcessLifecycle(item) if item.record == record
        )));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn search_matches_visible_text_and_orders_newest_first() {
        let dir = temp_dir();
        let store = store_at(&dir);
        let old = store
            .create(meta_with("sess-old", 1_700_000_100, "/x"))
            .await
            .unwrap();
        old.append_message(msg("u1", "we discussed the deadlock in the store"))
            .await;
        old.shutdown().await.unwrap();
        let newer = store
            .create(meta_with("sess-new", 1_700_000_200, "/x"))
            .await
            .unwrap();
        newer
            .append_message(msg("u1", "DEADLOCK reproduced again"))
            .await;
        newer
            .append_message(Message::Assistant(AssistantMessage {
                uuid: "a1".into(),
                content: vec![text("fixed the deadlock by holding one lock")],
                model: "m".into(),
                usage: None,
            }))
            .await;
        newer.shutdown().await.unwrap();
        let other = store
            .create(meta_with("sess-none", 1_700_000_300, "/x"))
            .await
            .unwrap();
        other.append_message(msg("u1", "unrelated chatter")).await;
        other.shutdown().await.unwrap();

        // Case-insensitive, newest session first, one hit per session.
        let page = store
            .search(&search_options("deadlock"), None, &search_signal())
            .await
            .unwrap();
        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].thread.id, "sess-new");
        assert_eq!(page.items[0].matches, 2);
        assert_eq!(page.items[0].role, "user");
        assert!(page.items[0].snippet.contains("DEADLOCK reproduced"));
        assert_eq!(page.items[1].thread.id, "sess-old");
        assert_eq!(page.items[1].matches, 1);

        // The limit caps hits, keeping the newest.
        let mut limited = search_options("deadlock");
        limited.limit = Some(1);
        let page = store
            .search(&limited, None, &search_signal())
            .await
            .unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].thread.id, "sess-new");
    }

    #[tokio::test]
    async fn search_ignores_non_visible_content_and_respects_archive_flag() {
        let dir = temp_dir();
        let store = store_at(&dir);
        let live = store
            .create(meta_with("sess-1", 1_700_000_100, "/only-in-header"))
            .await
            .unwrap();
        live.append_message(msg("u1", "searchable body")).await;
        live.shutdown().await.unwrap();

        // Header metadata (cwd/model/ids) is not conversation text.
        let page = store
            .search(&search_options("/only-in-header"), None, &search_signal())
            .await
            .unwrap();
        assert!(page.items.is_empty());

        // Archived sessions are excluded by default and opted back in.
        store.archive("sess-1").await.unwrap();
        let page = store
            .search(&search_options("searchable body"), None, &search_signal())
            .await
            .unwrap();
        assert!(page.items.is_empty());
        let mut with_archived = search_options("searchable body");
        with_archived.include_archived = Some(true);
        let page = store
            .search(&with_archived, None, &search_signal())
            .await
            .unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].thread.id, "sess-1");
        assert_eq!(page.items[0].thread.archived, Some(true));

        // An empty query is a caller error, not an empty result.
        let error = store
            .search(&search_options(""), None, &search_signal())
            .await
            .expect_err("empty query is rejected");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[tokio::test]
    async fn search_with_ripgrep_acceleration_matches_the_fallback() {
        let _guard = crate::test_support::external_process_lock().await;
        let dir = temp_dir();
        let store = store_at(&dir);
        let hit = store
            .create(meta_with("sess-hit", 1_700_000_100, "/x"))
            .await
            .unwrap();
        hit.append_message(msg("u1", "the elusive needle sits here"))
            .await;
        hit.shutdown().await.unwrap();
        let miss = store
            .create(meta_with("sess-miss", 1_700_000_200, "/x"))
            .await
            .unwrap();
        miss.append_message(msg("u1", "nothing to see")).await;
        miss.shutdown().await.unwrap();

        let process = crate::runtime_process::test_runtime_process_context();
        let accelerated = store
            .search(&search_options("needle"), Some(&process), &search_signal())
            .await
            .unwrap();
        let fallback = store
            .search(&search_options("needle"), None, &search_signal())
            .await
            .unwrap();
        // The accelerator narrows candidates; it never changes the results.
        assert_eq!(accelerated, fallback);
        assert_eq!(accelerated.items.len(), 1);
        assert_eq!(accelerated.items[0].thread.id, "sess-hit");
    }

    fn exact_json(source: &str) -> RuntimeJson {
        runtime_json::parse(&RuntimeString::from(source)).expect("valid exact JSON fixture")
    }

    fn meta_with(id: &str, created_at: i64, cwd: &str) -> SessionMeta {
        SessionMeta {
            id: id.into(),
            created_at,
            cwd: cwd.into(),
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
        }
    }

    fn meta() -> SessionMeta {
        meta_with("sess-1", 1_700_000_000, "/x")
    }

    fn text(text: &str) -> ContentBlock {
        ContentBlock::Text(TextBlock {
            text: text.into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })
    }

    fn msg(uuid: &str, text_content: &str) -> Message {
        Message::User(UserMessage {
            question_answer: None,
            uuid: uuid.into(),
            content: vec![text(text_content)],
            is_meta: None,
        })
    }

    fn user() -> Message {
        msg("u1", "hi")
    }

    fn assistant() -> Message {
        Message::Assistant(AssistantMessage {
            uuid: "a1".into(),
            content: vec![text("hello")],
            model: "m".into(),
            usage: None,
        })
    }

    fn type_of(m: &Message) -> &'static str {
        match m {
            Message::User(_) => "user",
            Message::Assistant(_) => "assistant",
            Message::ToolResult(_) => "tool_result",
            Message::System(_) => "system",
            Message::CompactBoundary(_) => "compact_boundary",
        }
    }

    fn uuid_of(m: &Message) -> &RuntimeString {
        match m {
            Message::User(u) => &u.uuid,
            Message::Assistant(a) => &a.uuid,
            Message::ToolResult(t) => &t.uuid,
            Message::System(s) => &s.uuid,
            Message::CompactBoundary(c) => &c.uuid,
        }
    }

    /// Label a projected message: its type for boundaries, otherwise its uuid.
    fn label(m: &Message) -> RuntimeString {
        match m {
            Message::CompactBoundary(_) => "compact_boundary".into(),
            other => uuid_of(other).clone(),
        }
    }

    fn ctx(
        cwd: &str,
        model: &str,
        mode: ExternalPermissionMode,
        turn_id: Option<&str>,
    ) -> TurnContextSnapshot {
        TurnContextSnapshot {
            cwd: cwd.into(),
            model: model.into(),
            permission_mode: mode,
            turn_id: turn_id.map(Into::into),
            started_at_ms: None,
            approval_policy: None,
            sandbox_mode: None,
            approvals_reviewer: None,
            network_allowed: None,
            writable_roots: None,
        }
    }

    fn sub_meta(agent_type: Option<&str>, created_at: i64) -> SubAgentMeta {
        SubAgentMeta {
            agent_type: agent_type.map(Into::into),
            model: "m".into(),
            permission_mode: None,
            effort: None,
            created_at,
        }
    }

    fn lock_path(dir: &Path) -> PathBuf {
        dir.join("1700000000-sess-1.jsonl.lock")
    }

    #[tokio::test]
    async fn agent_notification_receipts_replay_without_model_messages() {
        use crate::schemas::agent_notification::AgentNotification;
        let dir = temp_dir();
        let store = store_at(&dir);
        let live = store.create(meta()).await.unwrap();
        let receipt = AgentNotification {
            notification_id: "stable".into(),
            source: "team-mailbox".into(),
            status: "updated".into(),
            agent_name: Some("reviewer".into()),
            agent_id: None,
            task_id: None,
            timestamp: 1000,
            turn_id: Some("receiving-turn".into()),
        };
        for _ in 0..2 {
            live.append_event(
                "agent_notification".into(),
                Some(crate::store::session_store::AppendEventOptions {
                    turn_id: receipt.turn_id.clone(),
                    payload: Some(runtime_codec::encode_value(&receipt).unwrap()),
                }),
            )
            .await;
        }
        live.flush().await.unwrap();
        live.shutdown().await.unwrap();
        let resumed = store.resume("sess-1").await.unwrap();
        assert!(resumed.messages.is_empty());
        assert!(
            resumed.turn_index.turn_status.is_empty(),
            "notifications cannot invent a turn"
        );
        assert_eq!(resumed.turn_index.agent_notifications, vec![receipt]);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn persists_messages_and_resumes_history() {
        let dir = temp_dir();
        let store = store_at(&dir);
        let live = store.create(meta()).await.unwrap();
        live.append_turn_context(ctx("/x", "m", ExternalPermissionMode::Default, None))
            .await;
        live.append_message(user()).await;
        live.append_message(assistant()).await;
        live.flush().await.unwrap();
        live.shutdown().await.unwrap();

        let resumed = store.resume("sess-1").await.unwrap();
        assert_eq!(resumed.meta.id, "sess-1");
        assert_eq!(
            resumed.messages.iter().map(type_of).collect::<Vec<_>>(),
            vec!["user", "assistant"]
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn resume_marks_turn_without_lifecycle_as_interrupted() {
        // Turn 1 has a turn_lifecycle → "completed". Turn 2 has messages
        // but no turn_lifecycle (process killed mid-turn) → "interrupted".
        let t1: RuntimeString = "t1".into();
        let t2: RuntimeString = "t2".into();
        let items = vec![
            RolloutItem::SessionMeta {
                schema_version: ROLLOUT_SCHEMA_VERSION,
                meta: meta(),
            },
            RolloutItem::TurnContext {
                ctx: TurnContextSnapshot {
                    cwd: "/x".into(),
                    model: "m".into(),
                    permission_mode: ExternalPermissionMode::Default,
                    turn_id: Some(t1.clone()),
                    started_at_ms: Some(1_700_000_000_000),
                    approval_policy: None,
                    sandbox_mode: None,
                    approvals_reviewer: None,
                    network_allowed: None,
                    writable_roots: None,
                },
            },
            RolloutItem::Message {
                message: msg("u1", "hi"),
            },
            RolloutItem::Message {
                message: assistant(),
            },
            RolloutItem::Event(EventItem {
                event: "turn_lifecycle".into(),
                turn_id: Some(t1.clone()),
                payload: Some(RuntimeJson::Object({
                    let mut o = RuntimeObject::new();
                    o.insert(
                        RuntimeString::from("status"),
                        RuntimeJson::String("completed".into()),
                    );
                    o.insert(
                        RuntimeString::from("finishedAtMs"),
                        RuntimeJson::from(1_700_000_005_000i64),
                    );
                    o.insert(
                        RuntimeString::from("finalMessageId"),
                        RuntimeJson::String("a1".into()),
                    );
                    o
                })),
            }),
            RolloutItem::TurnContext {
                ctx: TurnContextSnapshot {
                    cwd: "/x".into(),
                    model: "m".into(),
                    permission_mode: ExternalPermissionMode::Default,
                    turn_id: Some(t2.clone()),
                    started_at_ms: Some(1_700_000_010_000),
                    approval_policy: None,
                    sandbox_mode: None,
                    approvals_reviewer: None,
                    network_allowed: None,
                    writable_roots: None,
                },
            },
            RolloutItem::Message {
                message: msg("u2", "q2"),
            },
            RolloutItem::Message {
                message: Message::Assistant(AssistantMessage {
                    uuid: "a2".into(),
                    content: vec![text("reply")],
                    model: "m".into(),
                    usage: None,
                }),
            },
            // No turn_lifecycle for t2 — process killed mid-turn.
        ];
        let result = resume_result_from_items("sess-1", items).unwrap();
        assert_eq!(
            result
                .turn_index
                .turn_status
                .get(&t1)
                .map(|s| s == "completed"),
            Some(true)
        );
        assert_eq!(
            result
                .turn_index
                .turn_status
                .get(&t2)
                .map(|s| s == "interrupted"),
            Some(true)
        );
        assert_eq!(
            result.turn_index.turn_started_at_ms.get(&t1),
            Some(&1_700_000_000_000)
        );
        assert_eq!(
            result.turn_index.turn_finished_at_ms.get(&t1),
            Some(&1_700_000_005_000)
        );
        assert_eq!(
            result.turn_index.turn_started_at_ms.get(&t2),
            Some(&1_700_000_010_000)
        );
        assert_eq!(result.turn_index.turn_finished_at_ms.get(&t2), None);
        assert_eq!(
            result.turn_index.turn_final_message.get(&t1),
            Some(&RuntimeString::from("a1")),
        );
        // message_turn still maps both turns' messages correctly.
        assert_eq!(
            result
                .turn_index
                .message_turn
                .get(&RuntimeString::from("u1"))
                .map(|s| s == "t1"),
            Some(true)
        );
        assert_eq!(
            result
                .turn_index
                .message_turn
                .get(&RuntimeString::from("u2"))
                .map(|s| s == "t2"),
            Some(true)
        );
    }

    #[test]
    fn resume_marks_a_turn_killed_before_its_first_message_as_interrupted() {
        // `turn_context` is written eagerly when the turn opens; the first message
        // only lands on the stream's first poll. An app relaunch for a permission
        // grant SIGKILLs the engine child, and a kill inside that window leaves a
        // turn with a context record and NO messages — invisible to the
        // message-index backfill, so it used to resume with no status at all. The
        // host then offers no way to continue the thread it just cut off.
        let t1: RuntimeString = "t1".into();
        let t2: RuntimeString = "t2".into();
        let items = vec![
            RolloutItem::SessionMeta {
                schema_version: ROLLOUT_SCHEMA_VERSION,
                meta: meta(),
            },
            RolloutItem::TurnContext {
                ctx: TurnContextSnapshot {
                    cwd: "/x".into(),
                    model: "m".into(),
                    permission_mode: ExternalPermissionMode::Default,
                    turn_id: Some(t1.clone()),
                    started_at_ms: Some(1_700_000_000_000),
                    approval_policy: None,
                    sandbox_mode: None,
                    approvals_reviewer: None,
                    network_allowed: None,
                    writable_roots: None,
                },
            },
            RolloutItem::Message {
                message: msg("u1", "hi"),
            },
            RolloutItem::Event(EventItem {
                event: "turn_lifecycle".into(),
                turn_id: Some(t1.clone()),
                payload: Some(RuntimeJson::Object({
                    let mut o = RuntimeObject::new();
                    o.insert(
                        RuntimeString::from("status"),
                        RuntimeJson::String("completed".into()),
                    );
                    o.insert(
                        RuntimeString::from("finishedAtMs"),
                        RuntimeJson::from(1_700_000_005_000i64),
                    );
                    o
                })),
            }),
            // t2 opened and died before any message became durable.
            RolloutItem::TurnContext {
                ctx: TurnContextSnapshot {
                    cwd: "/x".into(),
                    model: "m".into(),
                    permission_mode: ExternalPermissionMode::Default,
                    turn_id: Some(t2.clone()),
                    started_at_ms: Some(1_700_000_010_000),
                    approval_policy: None,
                    sandbox_mode: None,
                    approvals_reviewer: None,
                    network_allowed: None,
                    writable_roots: None,
                },
            },
        ];
        let result = resume_result_from_items("sess-1", items).unwrap();
        assert_eq!(
            result
                .turn_index
                .turn_status
                .get(&t1)
                .map(|s| s == "completed"),
            Some(true),
            "a recorded terminal status is never overwritten by inference"
        );
        assert_eq!(
            result
                .turn_index
                .turn_status
                .get(&t2)
                .map(|s| s == "interrupted"),
            Some(true),
            "a turn killed before its first message still resumes as interrupted"
        );
        // Inference writes a status only: the killed turn has no finish time, and
        // that absence is what lets the host tell it from a turn the user rewound
        // past (whose recorded terminal carries one).
        assert_eq!(result.turn_index.turn_finished_at_ms.get(&t2), None);
        assert_eq!(result.turn_index.turn_duration_ms.get(&t2), None);
        // Its start time DOES survive, from the eager `turn_context` record. That is
        // the only ordering evidence left for a turn with no messages, and what the
        // host uses to identify it as the newest turn — without it the killed turn
        // is invisible next to the completed one before it.
        assert_eq!(
            result.turn_index.turn_started_at_ms.get(&t2),
            Some(&1_700_000_010_000)
        );
        assert!(
            !result.turn_index.message_turn.values().any(|id| *id == t2),
            "the killed turn owns no messages"
        );
    }

    #[test]
    fn resume_does_not_invent_a_turn_from_a_lifecycle_event_alone() {
        // A `turn_lifecycle` event names a turn, but the turn was never opened in
        // this rollout. Inference must not create a status for it: only
        // `turn_context` and message attribution are evidence that the turn ran
        // here.
        let stray: RuntimeString = "stray".into();
        let items = vec![
            RolloutItem::SessionMeta {
                schema_version: ROLLOUT_SCHEMA_VERSION,
                meta: meta(),
            },
            RolloutItem::Event(EventItem {
                event: "tool_lifecycle".into(),
                turn_id: Some(stray.clone()),
                payload: None,
            }),
        ];
        let result = resume_result_from_items("sess-1", items).unwrap();
        assert_eq!(result.turn_index.turn_status.get(&stray), None);
        assert!(result.turn_index.turn_status.is_empty());
    }

    #[tokio::test]
    async fn resume_replays_the_file_changes_each_turn_made() {
        // Reopening a thread must report what it produced. The records are per
        // edit, so the fold has to run again on resume: within one turn the same
        // path is one row whose counts the newest record REPLACES (each record
        // already measures that turn's whole effect on the file), the create
        // verdict survives the later edit, and a different turn gets its own group.
        let change = |path: &str, kind: &str, added: u64, removed: u64, turn: &str| {
            RolloutItem::Event(EventItem {
                event: "file_change".into(),
                turn_id: Some(RuntimeString::from(turn)),
                payload: Some(RuntimeJson::Object({
                    let mut o = RuntimeObject::new();
                    o.insert(
                        RuntimeString::from("path"),
                        RuntimeJson::String(path.into()),
                    );
                    o.insert(
                        RuntimeString::from("kind"),
                        RuntimeJson::String(kind.into()),
                    );
                    o.insert(
                        "turnDiff",
                        RuntimeJson::from(format!("turn-diff:{turn}:{path}:{added}")),
                    );
                    o.insert(RuntimeString::from("added"), RuntimeJson::from(added));
                    o.insert(RuntimeString::from("removed"), RuntimeJson::from(removed));
                    o
                })),
            })
        };
        let items = vec![
            RolloutItem::SessionMeta {
                schema_version: ROLLOUT_SCHEMA_VERSION,
                meta: meta(),
            },
            change("src/new.rs", "create", 10, 0, "t1"),
            change("src/new.rs", "modify", 2, 1, "t1"),
            change("docs/notes.md", "modify", 4, 3, "t1"),
            change("src/new.rs", "modify", 7, 7, "t2"),
        ];
        let result = resume_result_from_items("sess-1", items).unwrap();
        assert_eq!(
            result.file_changes.turns[0].files[0].turn_diff,
            Some("turn-diff:t1:src/new.rs:2".into())
        );
        assert_eq!(
            result.file_changes.turns[1].files[0].turn_diff,
            Some("turn-diff:t2:src/new.rs:7".into())
        );
        // One turn's id and the rows asserted for it.
        type TurnRows = (
            Option<RuntimeString>,
            Vec<(RuntimeString, Option<u64>, Option<u64>)>,
        );
        let groups: Vec<TurnRows> = result
            .file_changes
            .turns
            .iter()
            .map(|turn| {
                (
                    turn.turn_id.clone(),
                    turn.files
                        .iter()
                        .map(|file| (file.path.clone(), file.added, file.removed))
                        .collect(),
                )
            })
            .collect();
        assert_eq!(
            groups,
            vec![
                (
                    Some(RuntimeString::from("t1")),
                    vec![
                        (RuntimeString::from("src/new.rs"), Some(2), Some(1)),
                        (RuntimeString::from("docs/notes.md"), Some(4), Some(3)),
                    ]
                ),
                (
                    Some(RuntimeString::from("t2")),
                    vec![(RuntimeString::from("src/new.rs"), Some(7), Some(7))]
                ),
            ]
        );
        assert_eq!(
            result.file_changes.turns[0].files[0].kind,
            crate::types::events::FileChangeKind::Create
        );
    }

    /// A rollout written before file changes carried a turn id still groups: the
    /// turn_context boundary that was open when the record was written says which
    /// turn it belonged to.
    #[tokio::test]
    async fn resume_attributes_a_turnless_file_change_to_the_open_turn_context() {
        let items = vec![
            RolloutItem::SessionMeta {
                schema_version: ROLLOUT_SCHEMA_VERSION,
                meta: meta(),
            },
            RolloutItem::TurnContext {
                ctx: TurnContextSnapshot {
                    cwd: "/x".into(),
                    model: "m".into(),
                    permission_mode: ExternalPermissionMode::Default,
                    turn_id: Some("older-turn".into()),
                    started_at_ms: None,
                    approval_policy: None,
                    sandbox_mode: None,
                    approvals_reviewer: None,
                    network_allowed: None,
                    writable_roots: None,
                },
            },
            RolloutItem::Event(EventItem {
                event: "file_change".into(),
                turn_id: None,
                payload: Some(RuntimeJson::Object({
                    let mut o = RuntimeObject::new();
                    o.insert(
                        RuntimeString::from("path"),
                        RuntimeJson::String("legacy.rs".into()),
                    );
                    o.insert(
                        RuntimeString::from("kind"),
                        RuntimeJson::String("modify".into()),
                    );
                    o.insert(RuntimeString::from("added"), RuntimeJson::from(3u64));
                    o.insert(RuntimeString::from("removed"), RuntimeJson::from(1u64));
                    o
                })),
            }),
        ];
        let result = resume_result_from_items("sess-1", items).unwrap();
        assert_eq!(
            result
                .file_changes
                .turns
                .iter()
                .map(|turn| turn.turn_id.clone())
                .collect::<Vec<_>>(),
            vec![Some(RuntimeString::from("older-turn"))],
        );
    }

    #[tokio::test]
    async fn resume_rejects_a_rollout_whose_header_names_another_session() {
        // The header is the file's own claim about which session it records.
        // Loading a file whose header names a different id would rebuild that
        // session's history, turn index and ledger under the requested id, so
        // the load must fail loudly instead of serving borrowed state.
        let items = vec![
            RolloutItem::SessionMeta {
                schema_version: ROLLOUT_SCHEMA_VERSION,
                meta: meta_with("other-session", 1_700_000_000, "/x"),
            },
            RolloutItem::Message {
                message: msg("u1", "hi"),
            },
        ];
        let error = resume_result_from_items("sess-1", items).unwrap_err();
        let message = error.to_string();
        assert!(message.contains("other-session"), "{message}");
        assert!(message.contains("sess-1"), "{message}");
    }

    #[tokio::test]
    async fn resume_restores_a_failed_turns_error_message() {
        // A failed turn persists status AND the failure message (the live error
        // card is ephemeral); resume must restore both so history can render
        // the reason.
        let t1: RuntimeString = "t1".into();
        let items = vec![
            RolloutItem::SessionMeta {
                schema_version: ROLLOUT_SCHEMA_VERSION,
                meta: meta(),
            },
            RolloutItem::TurnContext {
                ctx: TurnContextSnapshot {
                    cwd: "/x".into(),
                    model: "m".into(),
                    permission_mode: ExternalPermissionMode::Default,
                    turn_id: Some(t1.clone()),
                    started_at_ms: None,
                    approval_policy: None,
                    sandbox_mode: None,
                    approvals_reviewer: None,
                    network_allowed: None,
                    writable_roots: None,
                },
            },
            RolloutItem::Message {
                message: msg("u1", "hi"),
            },
            RolloutItem::Event(EventItem {
                event: "turn_lifecycle".into(),
                turn_id: Some(t1.clone()),
                payload: Some(RuntimeJson::Object({
                    let mut o = RuntimeObject::new();
                    o.insert(
                        RuntimeString::from("status"),
                        RuntimeJson::String("failed".into()),
                    );
                    o.insert(
                        RuntimeString::from("error"),
                        RuntimeJson::String("404 url.not_found".into()),
                    );
                    o
                })),
            }),
        ];
        let result = resume_result_from_items("sess-1", items).unwrap();
        assert_eq!(
            result
                .turn_index
                .turn_status
                .get(&t1)
                .map(|s| s == "failed"),
            Some(true)
        );
        assert_eq!(
            result.turn_index.turn_errors.get(&t1),
            Some(&RuntimeString::from("404 url.not_found"))
        );
    }

    #[tokio::test]
    async fn shutdown_closes_writer_admission_and_retains_completion() {
        let dir = temp_dir();
        let store = store_at(&dir);
        let live = store.create(meta()).await.unwrap();
        live.append_message(msg("before-shutdown", "durable")).await;
        live.shutdown().await.unwrap();

        // Every clone/caller observes the retained terminal result, while all
        // append surfaces become inert after the shutdown linearization point.
        live.append_message(msg("after-shutdown", "must not be written"))
            .await;
        live.append_event("after_shutdown_event".into(), None).await;
        live.flush().await.unwrap();
        live.shutdown().await.unwrap();

        let resumed = store.resume("sess-1").await.unwrap();
        assert!(resumed.raw_items.iter().any(|item| matches!(
            item,
            RolloutItem::Message { message: Message::User(message) }
                if message.uuid == "before-shutdown"
        )));
        assert!(!resumed.raw_items.iter().any(|item| match item {
            RolloutItem::Message {
                message: Message::User(message),
            } => message.uuid == "after-shutdown",
            RolloutItem::Event(event) => event.event == "after_shutdown_event",
            _ => false,
        }));
        assert!(!lock_path(&dir).exists());
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn reopen_reattaches_a_writer_so_a_resumed_session_keeps_persisting() {
        let dir = temp_dir();
        let store = store_at(&dir);
        let live = store.create(meta()).await.unwrap();
        live.append_message(user()).await;
        live.shutdown().await.unwrap(); // release the lock

        // Re-attach and append a continuation message.
        let re = store.reopen("sess-1").await.unwrap();
        re.append_message(assistant()).await;
        re.shutdown().await.unwrap();

        let resumed = store.resume("sess-1").await.unwrap();
        // Both the original and the continuation are present, in order.
        assert_eq!(
            resumed.messages.iter().map(type_of).collect::<Vec<_>>(),
            vec!["user", "assistant"]
        );
        // A `resumed` marker was recorded.
        assert!(resumed
            .raw_items
            .iter()
            .any(|i| matches!(i, RolloutItem::Event(e) if e.event == "resumed")));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn reopen_honors_the_single_writer_lock() {
        let dir = temp_dir();
        let store = store_at(&dir);
        let live = store.create(meta()).await.unwrap();
        // Lock is held by `live`; reopening must fail until it's released.
        let err = store.reopen("sess-1").await.unwrap_err();
        assert!(err.to_string().contains("locked"));
        live.shutdown().await.unwrap();
        let re = store.reopen("sess-1").await.unwrap(); // now succeeds
        re.shutdown().await.unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn atomic_resume_never_returns_history_older_than_a_competing_append() {
        let dir = temp_dir();
        let seed_store = store_at(&dir);
        let seed = seed_store.create(meta()).await.unwrap();
        seed.append_message(msg("t1", "first")).await;
        seed.shutdown().await.unwrap();

        let writer_store = store_at(&dir);
        let writer = writer_store.resume_for_write("sess-1").await.unwrap();
        writer.rollout.append_message(msg("t2", "second")).await;
        writer.rollout.flush().await.unwrap();

        // A reader attempting the old read-then-reopen window while B still
        // owns the writer must fail explicitly, never return T1-only history.
        let contender_store = store_at(&dir);
        let error = contender_store
            .resume_for_write("sess-1")
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);

        writer.rollout.shutdown().await.unwrap();
        let contender = contender_store.resume_for_write("sess-1").await.unwrap();
        let uuids = contender
            .resume
            .messages
            .iter()
            .map(uuid_of)
            .cloned()
            .collect::<Vec<_>>();
        assert!(uuids.contains(&RuntimeString::from("t1")));
        assert!(uuids.contains(&RuntimeString::from("t2")));
        contender.rollout.shutdown().await.unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn atomic_resume_two_store_resumers_have_exactly_one_read_writer_owner() {
        let dir = temp_dir();
        let seed_store = store_at(&dir);
        let seed = seed_store.create(meta()).await.unwrap();
        seed.append_message(msg("t1", "first")).await;
        seed.shutdown().await.unwrap();

        let file = dir.join("1700000000-sess-1.jsonl");
        let file_text = file.to_string_lossy().into_owned();
        let (entered, release) = pause_resume_for_write_at(&file_text);
        let first_store = Arc::new(store_at(&dir));
        let first = {
            let first_store = first_store.clone();
            tokio::spawn(async move { first_store.resume_for_write("sess-1").await })
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
            .await
            .expect("first resumer acquired its writer lease");
        assert!(lock_path(&dir).exists());

        let second_store = store_at(&dir);
        let second_error = second_store.resume_for_write("sess-1").await.unwrap_err();
        assert_eq!(second_error.kind(), io::ErrorKind::WouldBlock);

        clear_resume_for_write_pause(&file_text);
        release.notify_one();
        let first = first.await.unwrap().unwrap();
        first.rollout.shutdown().await.unwrap();
        assert!(!lock_path(&dir).exists());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn atomic_resume_cancellation_removes_writer_lock_and_allows_retry() {
        let dir = temp_dir();
        let seed_store = store_at(&dir);
        let seed = seed_store.create(meta()).await.unwrap();
        seed.append_message(msg("t1", "first")).await;
        seed.shutdown().await.unwrap();

        let file = dir.join("1700000000-sess-1.jsonl");
        let file_text = file.to_string_lossy().into_owned();
        let (entered, _release) = pause_resume_for_write_at(&file_text);
        let store = Arc::new(store_at(&dir));
        let pending = {
            let store = store.clone();
            tokio::spawn(async move { store.resume_for_write("sess-1").await })
        };
        // Generous: the assertion is that the temporary lease is taken at all,
        // and a saturated host can delay the spawned task well past a few seconds.
        tokio::time::timeout(std::time::Duration::from_secs(60), entered.notified())
            .await
            .expect("atomic resume acquired its temporary writer lease");
        assert!(lock_path(&dir).exists());

        pending.abort();
        let _ = pending.await;
        clear_resume_for_write_pause(&file_text);
        assert!(!lock_path(&dir).exists());

        let retried = store_at(&dir).resume_for_write("sess-1").await.unwrap();
        assert!(retried
            .resume
            .messages
            .iter()
            .any(|message| uuid_of(message) == "t1"));
        retried.rollout.shutdown().await.unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn atomic_resume_parse_error_removes_writer_lock_and_allows_retry() {
        let dir = temp_dir();
        let store = store_at(&dir);
        let seed = store.create(meta()).await.unwrap();
        seed.append_message(msg("t1", "first")).await;
        seed.shutdown().await.unwrap();

        let file = dir.join("1700000000-sess-1.jsonl");
        let valid = std::fs::read(&file).unwrap();
        std::fs::write(&file, b"{\"not\":\"a rollout record\"}\n").unwrap();
        let error = store.resume_for_write("sess-1").await.unwrap_err();
        assert!(error.to_string().contains("no session_meta header"));
        assert!(!lock_path(&dir).exists());

        std::fs::write(&file, valid).unwrap();
        let retried = store.resume_for_write("sess-1").await.unwrap();
        retried.rollout.shutdown().await.unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn atomic_resume_lease_blocks_delete_and_title_update() {
        let dir = temp_dir();
        let owner_store = store_at(&dir);
        let live = owner_store.create(meta()).await.unwrap();
        live.append_message(msg("t1", "first")).await;
        live.flush().await.unwrap();

        let mutator_store = store_at(&dir);
        let title_error = mutator_store
            .update_title("sess-1", "new title")
            .await
            .unwrap_err();
        assert_eq!(title_error.kind(), io::ErrorKind::WouldBlock);
        let delete_error = mutator_store.delete("sess-1").await.unwrap_err();
        assert_eq!(delete_error.kind(), io::ErrorKind::WouldBlock);

        live.shutdown().await.unwrap();
        mutator_store
            .update_title("sess-1", "new title")
            .await
            .unwrap();
        assert_eq!(
            mutator_store.resume("sess-1").await.unwrap().meta.title,
            Some(RuntimeString::from("new title"))
        );
        mutator_store.delete("sess-1").await.unwrap();
        assert!(!dir.join("1700000000-sess-1.jsonl").exists());
        assert!(!lock_path(&dir).exists());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn tolerates_a_corrupt_truncated_tail_line_on_resume() {
        let dir = temp_dir();
        let store = store_at(&dir);
        let live = store.create(meta()).await.unwrap();
        live.append_message(user()).await;
        live.append_message(assistant()).await;
        live.flush().await.unwrap();
        live.shutdown().await.unwrap();

        // Simulate a half-written final record from a crash mid-append.
        let file = dir.join("1700000000-sess-1.jsonl");
        use std::io::Write as _;
        let mut handle = std::fs::OpenOptions::new()
            .append(true)
            .open(&file)
            .unwrap();
        handle
            .write_all(br#"{"kind":"message","message":{"type":"user""#)
            .unwrap();
        drop(handle);

        let resumed = store_at(&dir).resume("sess-1").await.unwrap();
        assert_eq!(resumed.messages.len(), 2);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn refuses_a_second_concurrent_writer() {
        let dir = temp_dir();
        let store = store_at(&dir);
        let live = store.create(meta()).await.unwrap();
        let err = store.create(meta()).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        live.shutdown().await.unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn cancelled_thread_create_drains_its_writer_before_releasing_the_identity_lock() {
        let dir = temp_dir();
        let store = Arc::new(store_at(&dir));
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        *store.create_before_publish.lock().unwrap() = Some((entered.clone(), release));
        let creating = tokio::spawn({
            let store = store.clone();
            async move { store.create(meta()).await }
        });
        entered.notified().await;
        creating.abort();
        assert!(creating.await.unwrap_err().is_cancelled());
        assert!(store.index.lock().await.get("sess-1").is_none());
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while lock_path(&dir).exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled creation must release its live PID lock");
        // Cancellation preserves an already-written header; its writer must be
        // released so the same durable identity remains resumable.
        let fresh = store_at(&dir);
        let writer = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                match fresh.reopen("sess-1").await {
                    Ok(writer) => break writer,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        tokio::task::yield_now().await;
                    }
                    Err(error) => panic!("cancelled creation left unusable storage: {error}"),
                }
            }
        })
        .await
        .expect("identity lock must finish releasing after writer cleanup");
        writer.append_message(user()).await;
        writer.shutdown().await.unwrap();
        assert_eq!(fresh.resume("sess-1").await.unwrap().messages.len(), 1);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_reused_windows_pid_does_not_keep_an_older_lock_alive() {
        let lock_created = 100_000_000_000;
        assert!(!pid_reused_after_lock(lock_created - 1, lock_created));
        assert!(!pid_reused_after_lock(
            lock_created + 5_000_000,
            lock_created
        ));
        assert!(pid_reused_after_lock(
            lock_created + 20_000_000,
            lock_created
        ));
    }

    #[tokio::test]
    async fn reclaims_a_stale_lock_left_by_a_crashed_writer_dead_pid() {
        let dir = temp_dir();
        let store = store_at(&dir);
        let live = store.create(meta()).await.unwrap();
        live.append_message(user()).await;
        live.flush().await.unwrap();
        live.shutdown().await.unwrap();
        // Simulate a `kill -9`: a lock naming a PID that is guaranteed not to be
        // a live process.
        std::fs::write(lock_path(&dir), "2147483647\n").unwrap();
        let re = store.reopen("sess-1").await.unwrap(); // must reclaim, not fail "locked"
        re.append_message(assistant()).await;
        re.shutdown().await.unwrap();
        let resumed = store.resume("sess-1").await.unwrap();
        assert_eq!(
            resumed.messages.iter().map(type_of).collect::<Vec<_>>(),
            vec!["user", "assistant"]
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn reports_a_stale_lock_that_cannot_be_moved_instead_of_claiming_a_live_writer() {
        let dir = temp_dir();
        let store = store_at(&dir);
        let live = store.create(meta()).await.unwrap();
        live.shutdown().await.unwrap();
        std::fs::write(lock_path(&dir), "2147483647\n").unwrap();
        std::fs::create_dir(format!(
            "{}.stale-{}",
            lock_path(&dir).display(),
            std::process::id()
        ))
        .unwrap();
        let err = store.reopen("sess-1").await.unwrap_err();
        assert!(
            err.to_string()
                .contains("cannot reclaim stale session lock"),
            "{err}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn unreadable_lock_is_not_stolen_as_if_its_holder_were_dead() {
        let dir = temp_dir();
        let store = store_at(&dir);
        let live = store.create(meta()).await.unwrap();
        live.shutdown().await.unwrap();
        std::fs::create_dir(lock_path(&dir)).unwrap();
        let err = store.reopen("sess-1").await.unwrap_err();
        assert!(
            !err.to_string().contains("locked by another writer"),
            "{err}"
        );
        assert!(lock_path(&dir).is_dir());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn reclaims_a_lock_with_no_garbage_pid() {
        let dir = temp_dir();
        let store = store_at(&dir);
        let live = store.create(meta()).await.unwrap();
        live.flush().await.unwrap();
        live.shutdown().await.unwrap();
        std::fs::write(lock_path(&dir), "").unwrap(); // never stamped
        let re = store.reopen("sess-1").await.unwrap(); // reclaimed as unusable
        re.shutdown().await.unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn refuses_a_lock_whose_recorded_holder_is_still_alive() {
        let dir = temp_dir();
        let store = store_at(&dir);
        let live = store.create(meta()).await.unwrap();
        live.flush().await.unwrap();
        live.shutdown().await.unwrap();
        // Plant a lock naming a LIVE pid (our own) — reopen must NOT reclaim a
        // live holder's lock.
        std::fs::write(lock_path(&dir), format!("{}\n", std::process::id())).unwrap();
        let err = store.reopen("sess-1").await.unwrap_err();
        assert!(err.to_string().contains("locked"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn lists_persisted_sessions() {
        let dir = temp_dir();
        let store = store_at(&dir);
        let live = store.create(meta()).await.unwrap();
        live.append_message(user()).await;
        live.shutdown().await.unwrap();
        let page = store.list(ListOptions::default()).await.unwrap();
        assert!(page.items.iter().any(|t| t.id == "sess-1"));
        std::fs::remove_dir_all(&dir).ok();
    }

    // ---- countSessionsTouchedSince ----

    async fn seed(store: &JsonlSessionStore, id: &str) {
        let live = store
            .create(meta_with(id, 1_700_000_000, "/x"))
            .await
            .unwrap();
        live.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn counts_session_files_and_can_exclude_the_current_one() {
        let dir = temp_dir();
        let store = store_at(&dir);
        for id in ["a", "b", "c"] {
            seed(&store, id).await;
        }
        assert_eq!(
            store.count_sessions_touched_since(0.0, None).await.unwrap(),
            3
        );
        // Current excluded.
        assert_eq!(
            store
                .count_sessions_touched_since(0.0, Some("b"))
                .await
                .unwrap(),
            2
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn excludes_by_exact_id_not_a_suffix_match() {
        let dir = temp_dir();
        let store = store_at(&dir);
        // `x-abc`'s file ends with `-abc.jsonl`; excluding `abc` must NOT drop it.
        for id in ["abc", "x-abc"] {
            seed(&store, id).await;
        }
        // Only `abc` dropped.
        assert_eq!(
            store
                .count_sessions_touched_since(0.0, Some("abc"))
                .await
                .unwrap(),
            1
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn honors_the_since_cutoff_and_ignores_non_session_files() {
        let dir = temp_dir();
        let store = store_at(&dir);
        seed(&store, "old").await;
        // A future cutoff excludes everything; a `.lock`/non-jsonl file is never
        // counted.
        let future = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64()
            * 1000.0
            + 60_000.0;
        assert_eq!(
            store
                .count_sessions_touched_since(future, None)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            store.count_sessions_touched_since(0.0, None).await.unwrap(),
            1
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn returns_0_when_the_sessions_dir_does_not_exist_yet() {
        let dir = temp_dir();
        let store = store_at(&dir.join("nope"));
        assert_eq!(
            store.count_sessions_touched_since(0.0, None).await.unwrap(),
            0
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn session_count_propagates_a_non_directory_store_root() {
        let dir = temp_dir();
        let file = dir.join("sessions-is-a-file");
        std::fs::write(&file, "not a directory").unwrap();
        let store = store_at(&file);

        assert!(store.count_sessions_touched_since(0.0, None).await.is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    // ---- resume reconstructs the compacted view + last turn context ----

    #[tokio::test]
    async fn reconstructs_the_compacted_projection_not_the_raw_history() {
        let dir = temp_dir();
        let store = store_at(&dir);
        let live = store.create(meta()).await.unwrap();
        live.append_message(msg("u1", "one")).await;
        live.append_message(msg("u2", "two")).await;
        live.append_compaction(CompactionRecord {
            approval_parent_history: None,
            summary: "SUMMARY".into(),
            replaced_range: (0, 2),
            source_message_ids: None,
            replacement_history: None,
        })
        .await;
        live.append_message(msg("u3", "three")).await;
        live.shutdown().await.unwrap();

        let resumed = store.resume("sess-1").await.unwrap();
        // Projected (compacted) view: summary + u3.
        assert_eq!(
            resumed.messages.iter().map(label).collect::<Vec<_>>(),
            vec!["compact_boundary", "u3"]
        );
        // Raw items retain the full untrimmed transcript.
        assert_eq!(
            resumed
                .raw_items
                .iter()
                .filter(|i| matches!(i, RolloutItem::Message { .. }))
                .count(),
            3
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn round_trips_a_replacement_history_snapshot_through_the_file() {
        let dir = temp_dir();
        let store = store_at(&dir);
        let live = store.create(meta()).await.unwrap();
        live.append_message(msg("u1", "one")).await;
        live.append_message(msg("u2", "two")).await;
        let mut restored = msg("restore", "Fresh contents of recently-touched files");
        if let Message::User(user) = &mut restored {
            user.is_meta = Some(true);
            if let ContentBlock::Text(block) = &mut user.content[0] {
                block.phase = Some("compact_file_restore".into());
            }
        }
        // Full snapshot: the projection is REPLACED by this list
        // wholesale, and a post-compaction message appended afterward lands on
        // top of it.
        live.append_compaction(CompactionRecord {
            approval_parent_history: None,
            summary: "IGNORED WHEN SNAPSHOT PRESENT".into(),
            replaced_range: (0, 2),
            source_message_ids: None,
            replacement_history: Some(vec![msg("s1", "snapshot only"), restored.clone()]),
        })
        .await;
        live.append_message(msg("u3", "after")).await;
        live.shutdown().await.unwrap();

        let resumed = store.resume("sess-1").await.unwrap();
        // u1/u2 are gone; the snapshot + the post-compaction message remain.
        assert_eq!(
            resumed.messages.iter().map(label).collect::<Vec<_>>(),
            vec!["s1", "restore", "u3"]
        );
        assert_eq!(resumed.messages[1], restored);
        // The on-disk compacted record kept its replacementHistory across
        // serialize/parse.
        let compacted = resumed
            .raw_items
            .iter()
            .find_map(|i| match i {
                RolloutItem::Compacted(c) => Some(c),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            compacted
                .replacement_history
                .as_ref()
                .unwrap()
                .iter()
                .map(|m| uuid_of(m).clone())
                .collect::<Vec<_>>(),
            vec!["s1", "restore"]
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn returns_the_most_recent_turn_context_snapshot() {
        let dir = temp_dir();
        let store = store_at(&dir);
        let live = store.create(meta()).await.unwrap();
        live.append_turn_context(ctx("/x", "m", ExternalPermissionMode::Default, None))
            .await;
        live.append_turn_context(ctx("/y", "m2", ExternalPermissionMode::Plan, Some("t2")))
            .await;
        live.shutdown().await.unwrap();

        let resumed = store.resume("sess-1").await.unwrap();
        let last = resumed.last_turn_context.unwrap();
        assert_eq!(last.cwd, "/y");
        assert_eq!(last.model, "m2");
        assert_eq!(last.turn_id, Some(RuntimeString::from("t2")));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn persists_event_records_but_excludes_them_from_the_projection() {
        let dir = temp_dir();
        let store = store_at(&dir);
        let live = store.create(meta()).await.unwrap();
        live.append_event(
            "turn_started".into(),
            Some(AppendEventOptions {
                turn_id: Some("t1".into()),
                payload: Some(exact_json(r#"{"n":1}"#)),
            }),
        )
        .await;
        live.append_message(msg("u1", "hi")).await;
        live.shutdown().await.unwrap();

        let resumed = store.resume("sess-1").await.unwrap();
        assert_eq!(
            resumed.messages.iter().map(uuid_of).collect::<Vec<_>>(),
            vec!["u1"]
        );
        assert!(resumed
            .raw_items
            .iter()
            .any(|i| matches!(i, RolloutItem::Event(_))));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn event_jsonl_round_trip_distinguishes_lone_surrogates_and_replacement() {
        let dir = temp_dir();
        let store = store_at(&dir);
        let live = store.create(meta()).await.unwrap();
        let event = exact_json(r#""H\ud800L\udc00R\ufffd""#)
            .into_string()
            .unwrap();
        let payload = exact_json(r#"{"high":"\ud800","low":"\udc00","replacement":"\ufffd"}"#);

        live.append_event(
            event.clone(),
            Some(AppendEventOptions {
                turn_id: None,
                payload: Some(payload.clone()),
            }),
        )
        .await;
        live.shutdown().await.unwrap();

        let resumed = store.resume("sess-1").await.unwrap();
        let persisted = resumed
            .raw_items
            .iter()
            .find_map(|item| match item {
                RolloutItem::Event(event) => Some(event),
                _ => None,
            })
            .unwrap();
        assert_eq!(persisted.event, event);
        assert_eq!(persisted.payload.as_ref(), Some(&payload));

        let bytes = std::fs::read(dir.join("1700000000-sess-1.jsonl")).unwrap();
        assert!(bytes.windows(6).any(|window| window == br"\ud800"));
        assert!(bytes.windows(6).any(|window| window == br"\udc00"));
        assert!(bytes.windows(3).any(|window| window == [0xef, 0xbf, 0xbd]));
        std::fs::remove_dir_all(&dir).ok();
    }

    // ---- back-compat: resume a v1 file ----

    #[tokio::test]
    async fn resumes_a_v1_rollout_file() {
        let dir = temp_dir();
        // Hand-write a legacy v1 file: bare session_meta header (schemaVersion 1),
        // a v1 turn_context, two messages, and a v1 compacted{summary,replacedRange}.
        let file = dir.join("1700000000-old.jsonl");
        let lines = [
            r#"{"kind":"session_meta","schemaVersion":1,"meta":{"id":"old","createdAt":1700000000,"cwd":"/x","model":"m"}}"#,
            r#"{"kind":"turn_context","ctx":{"cwd":"/x","model":"m","permissionMode":"default"}}"#,
            r#"{"kind":"message","message":{"type":"user","uuid":"u1","content":[{"type":"text","text":"one"}]}}"#,
            r#"{"kind":"message","message":{"type":"user","uuid":"u2","content":[{"type":"text","text":"two"}]}}"#,
            r#"{"kind":"compacted","summary":"OLD","replacedRange":[0,1]}"#,
        ];
        std::fs::write(&file, format!("{}\n", lines.join("\n"))).unwrap();

        let resumed = store_at(&dir).resume("old").await.unwrap();
        assert_eq!(resumed.meta.id, "old");
        // v1 compaction still projects: drop u1, keep summary + u2.
        assert_eq!(
            resumed.messages.iter().map(label).collect::<Vec<_>>(),
            vec!["compact_boundary", "u2"]
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    // ---- pagination ----

    async fn write_session(store: &JsonlSessionStore, id: &str, created_at: i64) {
        let live = store.create(meta_with(id, created_at, "/x")).await.unwrap();
        live.append_message(msg("u1", "x")).await;
        live.shutdown().await.unwrap();
        let path = Path::new(&store.dir).join(format!("{created_at}-{id}.jsonl"));
        filetime::set_file_mtime(path, filetime::FileTime::from_unix_time(created_at, 0)).unwrap();
    }

    #[tokio::test]
    async fn list_orders_sessions_by_rollout_update_time() {
        let dir = temp_dir();
        let store = store_at(&dir);
        write_session(&store, "created-first", 1_700_000_000).await;
        write_session(&store, "created-last", 1_700_000_100).await;

        let first_path = dir.join("1700000000-created-first.jsonl");
        filetime::set_file_mtime(
            &first_path,
            filetime::FileTime::from_unix_time(1_700_000_200, 0),
        )
        .unwrap();

        let page = store_at(&dir).list(ListOptions::default()).await.unwrap();
        assert_eq!(
            page.items
                .iter()
                .map(|item| item.id.clone())
                .collect::<Vec<_>>(),
            vec![
                RuntimeString::from("created-first"),
                RuntimeString::from("created-last"),
            ]
        );
        assert_eq!(page.items[0].updated_at, 1_700_000_200_000);
        assert_eq!(page.items[1].updated_at, 1_700_000_100_000);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn pages_through_sessions_with_cursor_limit_newest_first_no_overlap() {
        let dir = temp_dir();
        let store = store_at(&dir);
        // createdAt ascending; list() returns newest (highest createdAt) first.
        for i in 0..5 {
            write_session(&store, &format!("s{i}"), 1_700_000_000 + i).await;
        }

        let fresh = store_at(&dir);
        let mut seen: Vec<RuntimeString> = Vec::new();
        let mut cursor: Option<RuntimeString> = None;
        let mut guard = 0;
        loop {
            let page = fresh
                .list(ListOptions {
                    limit: Some(2),
                    cursor: cursor.clone(),
                    ..Default::default()
                })
                .await
                .unwrap();
            seen.extend(page.items.iter().map(|t| t.id.clone()));
            cursor = page.next_cursor;
            guard += 1;
            if cursor.is_none() || guard >= 10 {
                break;
            }
        }

        assert_eq!(seen, vec!["s4", "s3", "s2", "s1", "s0"]);
        // No id appears twice across pages.
        let unique: std::collections::HashSet<&RuntimeString> = seen.iter().collect();
        assert_eq!(unique.len(), seen.len());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn pagination_uses_id_tiebreak_for_equal_created_at_without_loss() {
        let dir = temp_dir();
        let store = store_at(&dir);
        for id in ["a", "b", "c"] {
            write_session(&store, id, 1_700_000_000).await;
        }

        let lister = store_at(&dir);
        let mut seen = Vec::new();
        let mut cursor = None;
        loop {
            let page = lister
                .list(ListOptions {
                    cursor,
                    limit: Some(1),
                    ..Default::default()
                })
                .await
                .unwrap();
            seen.extend(page.items.iter().map(|item| item.id.clone()));
            let Some(next) = page.next_cursor else {
                break;
            };
            cursor = Some(next);
            assert!(seen.len() <= 3, "pagination cursor must make progress");
        }

        assert_eq!(seen, vec!["c", "b", "a"]);
        assert_eq!(
            seen.iter().collect::<std::collections::HashSet<_>>().len(),
            seen.len()
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn defaults_to_a_single_page_when_no_limit_is_given() {
        let dir = temp_dir();
        let store = store_at(&dir);
        write_session(&store, "s0", 1_700_000_000).await;
        let page = store_at(&dir).list(ListOptions::default()).await.unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.next_cursor, None);
        std::fs::remove_dir_all(&dir).ok();
    }

    // ---- archive ----

    #[tokio::test]
    async fn moves_a_session_into_the_archived_subdir_and_excludes_it_from_default_list() {
        let dir = temp_dir();
        let store = store_at(&dir);
        let live = store.create(meta()).await.unwrap();
        live.append_message(msg("u1", "hi")).await;
        live.shutdown().await.unwrap();

        let dest = store.archive("sess-1").await.unwrap();
        assert!(dest.contains("archived"));

        // The original file no longer sits in the live dir.
        let live_files: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| native_text_to_store_host(&e.file_name()))
            .filter(|n| n.ends_with(".jsonl"))
            .collect();
        assert!(live_files.is_empty());

        // Default list excludes archived; includeArchived surfaces it.
        let fresh = store_at(&dir);
        let default_page = fresh.list(ListOptions::default()).await.unwrap();
        assert!(!default_page.items.iter().any(|t| t.id == "sess-1"));
        let with_archived = fresh
            .list(ListOptions {
                include_archived: Some(true),
                ..Default::default()
            })
            .await
            .unwrap();
        let found = with_archived
            .items
            .iter()
            .find(|t| t.id == "sess-1")
            .unwrap();
        assert_eq!(found.archived, Some(true));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn archive_refuses_a_running_writer_without_moving_any_history() {
        let dir = temp_dir();
        let store = store_at(&dir);
        let live = store.create(meta()).await.unwrap();
        live.append_message(msg("u-live", "still running")).await;
        live.flush().await.unwrap();

        let error = store.archive("sess-1").await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        assert!(dir.join("1700000000-sess-1.jsonl").is_file());
        assert!(!dir.join("archived/1700000000-sess-1.jsonl").exists());

        live.shutdown().await.unwrap();
        store.archive("sess-1").await.unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn resumes_a_session_after_it_has_been_archived() {
        let dir = temp_dir();
        let store = store_at(&dir);
        let live = store.create(meta()).await.unwrap();
        live.append_message(msg("u1", "hi")).await;
        live.shutdown().await.unwrap();
        store.archive("sess-1").await.unwrap();

        // Even a fresh store (cold index) locates the archived file on resume.
        let resumed = store_at(&dir).resume("sess-1").await.unwrap();
        assert_eq!(
            resumed.messages.iter().map(uuid_of).collect::<Vec<_>>(),
            vec!["u1"]
        );

        // The archived file is intact and parseable.
        let archived_dir = dir.join("archived");
        let files: Vec<PathBuf> = std::fs::read_dir(&archived_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .collect();
        let content = std::fs::read_to_string(&files[0]).unwrap();
        assert!(content.contains("session_meta"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn archive_moves_the_complete_subagent_tree_and_cold_store_can_load_it() {
        let dir = temp_dir();
        let store = store_at(&dir);
        let live = store.create(meta()).await.unwrap();
        live.append_message(msg("u1", "hi")).await;
        live.shutdown().await.unwrap();
        let sub = store
            .open_sub_agent("sess-1", "agent-A", sub_meta(Some("Explore"), 1))
            .await
            .unwrap();
        sub.append_message(child_user()).await;
        sub.append_message(child_assistant()).await;
        sub.shutdown().await.unwrap();

        let dest = store.archive("sess-1").await.unwrap();
        let archived_session_dir = Path::new(&dest).with_extension("");
        assert!(!dir.join("1700000000-sess-1").exists());
        assert!(archived_session_dir.join("subagents").is_dir());

        let fresh = store_at(&dir);
        assert_eq!(
            fresh.list_sub_agents("sess-1").await.unwrap(),
            vec!["agent-A"]
        );
        let loaded = fresh.load_sub_agent("sess-1", "agent-A").await.unwrap();
        assert_eq!(
            loaded.messages.iter().map(type_of).collect::<Vec<_>>(),
            vec!["user", "assistant"]
        );
        assert_eq!(
            loaded.meta.unwrap().agent_type.as_ref(),
            Some(&RuntimeString::from("Explore"))
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn archive_repairs_a_legacy_main_only_archive() {
        let dir = temp_dir();
        let store = store_at(&dir);
        store
            .create(meta())
            .await
            .unwrap()
            .shutdown()
            .await
            .unwrap();
        let sub = store
            .open_sub_agent("sess-1", "agent-A", sub_meta(Some("Explore"), 1))
            .await
            .unwrap();
        sub.append_message(child_user()).await;
        sub.shutdown().await.unwrap();

        let archive_dir = dir.join("archived");
        std::fs::create_dir_all(&archive_dir).unwrap();
        std::fs::rename(
            dir.join("1700000000-sess-1.jsonl"),
            archive_dir.join("1700000000-sess-1.jsonl"),
        )
        .unwrap();
        assert!(dir.join("1700000000-sess-1/subagents").is_dir());

        let fresh = store_at(&dir);
        fresh.archive("sess-1").await.unwrap();
        assert!(!dir.join("1700000000-sess-1").exists());
        assert!(archive_dir.join("1700000000-sess-1/subagents").is_dir());
        assert_eq!(
            fresh.list_sub_agents("sess-1").await.unwrap(),
            vec!["agent-A"]
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn archive_rolls_back_the_session_data_tree_when_main_move_fails() {
        let dir = temp_dir();
        let store = store_at(&dir);
        store
            .create(meta())
            .await
            .unwrap()
            .shutdown()
            .await
            .unwrap();
        store
            .open_sub_agent("sess-1", "agent-A", sub_meta(None, 1))
            .await
            .unwrap()
            .shutdown()
            .await
            .unwrap();

        let archive_dir = dir.join("archived");
        std::fs::create_dir_all(archive_dir.join("1700000000-sess-1.jsonl")).unwrap();
        let err = store.archive("sess-1").await.unwrap_err();
        assert!(!err.to_string().is_empty());
        assert!(dir.join("1700000000-sess-1/subagents").is_dir());
        assert!(!archive_dir.join("1700000000-sess-1").exists());
        assert!(dir.join("1700000000-sess-1.jsonl").is_file());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn unarchive_is_idempotent_and_preserves_main_and_sidechain_history() {
        let dir = temp_dir();
        let store = store_at(&dir);
        let live = store.create(meta()).await.unwrap();
        live.append_message(msg("u-original", "main history")).await;
        live.shutdown().await.unwrap();
        let sub = store
            .open_sub_agent("sess-1", "agent-A", sub_meta(Some("Explore"), 11))
            .await
            .unwrap();
        sub.append_message(msg("child-original", "sidechain history"))
            .await;
        sub.shutdown().await.unwrap();
        store.archive("sess-1").await.unwrap();

        let restored = store.unarchive("sess-1").await;
        assert_eq!(restored.status, UnarchiveSessionStatus::Restored);
        let summary = restored.thread.unwrap();
        assert_eq!(summary.id, "sess-1");
        assert_eq!(summary.archived, Some(false));
        assert_eq!(restored.error, None);

        let resumed = store.resume("sess-1").await.unwrap();
        assert_eq!(resumed.meta.id, "sess-1");
        assert_eq!(
            resumed.messages.iter().map(uuid_of).collect::<Vec<_>>(),
            vec!["u-original"]
        );
        let child = store.load_sub_agent("sess-1", "agent-A").await.unwrap();
        assert_eq!(
            child.messages.iter().map(uuid_of).collect::<Vec<_>>(),
            vec!["child-original"]
        );
        assert_eq!(child.meta.unwrap().agent_type, Some("Explore".into()));

        let repeated = store_at(&dir).unarchive("sess-1").await;
        assert_eq!(repeated.status, UnarchiveSessionStatus::AlreadyActive);
        assert_eq!(repeated.thread.unwrap().id, "sess-1");
        let listed = store_at(&dir)
            .list(ListOptions {
                include_archived: Some(true),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            listed
                .items
                .iter()
                .filter(|item| item.id == "sess-1")
                .count(),
            1
        );
        assert_eq!(listed.items[0].archived, Some(false));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn unarchive_distinguishes_missing_and_running_archived_rollouts() {
        let dir = temp_dir();
        let store = store_at(&dir);
        assert_eq!(
            store.unarchive("missing").await.status,
            UnarchiveSessionStatus::Missing
        );

        store
            .create(meta())
            .await
            .unwrap()
            .shutdown()
            .await
            .unwrap();
        store.archive("sess-1").await.unwrap();
        let running = store.reopen("sess-1").await.unwrap();
        let refused = store.unarchive("sess-1").await;
        assert_eq!(refused.status, UnarchiveSessionStatus::Running);
        assert_eq!(refused.thread.unwrap().archived, Some(true));
        running.shutdown().await.unwrap();

        assert_eq!(
            store.unarchive("sess-1").await.status,
            UnarchiveSessionStatus::Restored
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn unarchive_reports_cross_store_lifecycle_contention_as_concurrent() {
        let dir = temp_dir();
        let store = store_at(&dir);
        store
            .create(meta())
            .await
            .unwrap()
            .shutdown()
            .await
            .unwrap();
        store.archive("sess-1").await.unwrap();

        let held = store
            .try_lifecycle_file_lock("sess-1")
            .await
            .unwrap()
            .unwrap();
        let competing_store = store_at(&dir);
        let resume_error = competing_store.resume("sess-1").await.unwrap_err();
        assert_eq!(resume_error.kind(), io::ErrorKind::WouldBlock);
        let competing = competing_store.unarchive("sess-1").await;
        assert_eq!(competing.status, UnarchiveSessionStatus::Concurrent);
        assert_eq!(competing.thread.unwrap().archived, Some(true));
        drop(held);

        assert_eq!(
            store_at(&dir).unarchive("sess-1").await.status,
            UnarchiveSessionStatus::Restored
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn unarchive_rolls_back_sidechains_when_main_visibility_move_fails() {
        let dir = temp_dir();
        let store = store_at(&dir);
        store
            .create(meta())
            .await
            .unwrap()
            .shutdown()
            .await
            .unwrap();
        let agent = store
            .open_sub_agent("sess-1", "agent-A", sub_meta(None, 1))
            .await
            .unwrap();
        agent.append_message(msg("child", "history")).await;
        agent.shutdown().await.unwrap();
        let archived = store.archive("sess-1").await.unwrap();
        let archived_data = Path::new(&archived).with_extension("");
        let active_data = dir.join("1700000000-sess-1");
        fail_next_unarchive_main_move(&archived);

        let failed = store.unarchive("sess-1").await;
        assert_eq!(failed.status, UnarchiveSessionStatus::PersistenceFailed);
        assert!(failed
            .error
            .unwrap()
            .try_to_utf8()
            .unwrap()
            .contains("unarchive transaction failed"));
        assert!(Path::new(&archived).is_file());
        assert!(archived_data.join("subagents").is_dir());
        assert!(!active_data.exists());
        assert!(store
            .list(ListOptions::default())
            .await
            .unwrap()
            .items
            .is_empty());

        assert_eq!(
            store.unarchive("sess-1").await.status,
            UnarchiveSessionStatus::Restored
        );
        assert_eq!(
            store.list_sub_agents("sess-1").await.unwrap(),
            vec!["agent-A"]
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn interrupted_unarchive_recovers_every_durable_phase_on_cold_reopen() {
        let cases = [
            (UnarchiveTestPoint::IntentDurable, false),
            (UnarchiveTestPoint::DataRenamed, false),
            (UnarchiveTestPoint::DataPhaseDurable, false),
            (UnarchiveTestPoint::MainRenamed, true),
            (UnarchiveTestPoint::MainPhaseDurable, true),
        ];

        for (case_index, (point, committed)) in cases.into_iter().enumerate() {
            let dir = temp_dir();
            let thread_id = format!("recover-{case_index}");
            let created_at = 1_700_001_000 + case_index as i64;
            let file_name = format!("{created_at}-{thread_id}.jsonl");
            let active_main = dir.join(&file_name);
            let active_data = active_main.with_extension("");
            let archived_main = dir.join(DEFAULT_ARCHIVE_SUBDIR).join(&file_name);
            let archived_data = archived_main.with_extension("");

            let store = Arc::new(store_at(&dir));
            write_session(&store, "page-old", created_at - 10).await;
            write_session(&store, "page-new", created_at + 10).await;

            let live = store
                .create(meta_with(&thread_id, created_at, "/x"))
                .await
                .unwrap();
            live.append_message(msg("main-original", "main history"))
                .await;
            live.shutdown().await.unwrap();
            let child = store
                .open_sub_agent(
                    &thread_id,
                    "agent-A",
                    sub_meta(Some("Explore"), created_at + 1),
                )
                .await
                .unwrap();
            child
                .append_message(msg("child-original", "sidechain history"))
                .await;
            child.shutdown().await.unwrap();
            store.archive(&thread_id).await.unwrap();

            let (entered, _release) = pause_unarchive_at(&thread_id, point);
            let operation = tokio::spawn({
                let store = store.clone();
                let thread_id = thread_id.clone();
                async move { store.unarchive(&thread_id).await }
            });
            tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
                .await
                .expect("unarchive reached deterministic crash boundary");
            operation.abort();
            assert!(operation
                .await
                .expect_err("cancelled unarchive task must not complete")
                .is_cancelled());
            clear_unarchive_pause();

            // A new store instance is the process-restart boundary. `reopen`
            // must reconcile the durable intent before it exposes either the
            // main rollout or its sidechain namespace.
            let fresh = store_at(&dir);
            let reopened = fresh.reopen(&thread_id).await.unwrap();
            reopened.shutdown().await.unwrap();

            assert_eq!(active_main.is_file(), committed, "active main at {point:?}");
            assert_eq!(active_data.is_dir(), committed, "active data at {point:?}");
            assert_eq!(
                archived_main.is_file(),
                !committed,
                "archive main at {point:?}"
            );
            assert_eq!(
                archived_data.is_dir(),
                !committed,
                "archive data at {point:?}"
            );
            assert!(
                !Path::new(&fresh.unarchive_journal_dir(&thread_id)).exists(),
                "recovery clears the durable journal at {point:?}"
            );

            let resumed = fresh.resume(&thread_id).await.unwrap();
            assert_eq!(
                resumed.messages.iter().map(uuid_of).collect::<Vec<_>>(),
                vec!["main-original"],
                "main history at {point:?}"
            );
            let child = fresh.load_sub_agent(&thread_id, "agent-A").await.unwrap();
            assert_eq!(
                child.messages.iter().map(uuid_of).collect::<Vec<_>>(),
                vec!["child-original"],
                "sidechain history at {point:?}"
            );
            assert_eq!(child.meta.unwrap().agent_type, Some("Explore".into()));
            assert_eq!(
                fresh.list_sub_agents(&thread_id).await.unwrap(),
                vec!["agent-A"]
            );

            // Cursor pagination must expose one canonical copy after recovery,
            // regardless of which namespace the main-last commit selected.
            let mut cursor = None;
            let mut seen = Vec::new();
            loop {
                let page = fresh
                    .list(ListOptions {
                        include_archived: Some(true),
                        limit: Some(1),
                        cursor,
                        ..Default::default()
                    })
                    .await
                    .unwrap();
                seen.extend(page.items.iter().map(|item| item.id.clone()));
                cursor = page.next_cursor;
                if cursor.is_none() {
                    break;
                }
            }
            assert_eq!(seen.len(), 3, "pagination cardinality at {point:?}");
            assert_eq!(
                seen.iter().collect::<std::collections::HashSet<_>>().len(),
                seen.len(),
                "pagination has no duplicate namespace entry at {point:?}"
            );
            let listed = fresh
                .list(ListOptions {
                    include_archived: Some(true),
                    ..Default::default()
                })
                .await
                .unwrap();
            let target = listed
                .items
                .iter()
                .find(|item| item.id == thread_id)
                .expect("recovered thread remains listed");
            assert_eq!(target.archived, Some(!committed), "status at {point:?}");

            std::fs::remove_dir_all(&dir).ok();
        }
    }

    #[tokio::test]
    async fn parent_sync_failure_keeps_unarchive_journal_until_cold_recovery() {
        for fail_main_sync in [false, true] {
            let dir = temp_dir();
            let thread_id = if fail_main_sync {
                "main-sync-failure"
            } else {
                "data-sync-failure"
            };
            let created_at = if fail_main_sync {
                1_700_002_001
            } else {
                1_700_002_000
            };
            let file_name = format!("{created_at}-{thread_id}.jsonl");
            let active_main = dir.join(&file_name);
            let active_data = active_main.with_extension("");
            let archived_main = dir.join(DEFAULT_ARCHIVE_SUBDIR).join(&file_name);
            let archived_data = archived_main.with_extension("");

            let store = store_at(&dir);
            let live = store
                .create(meta_with(thread_id, created_at, "/x"))
                .await
                .unwrap();
            live.append_message(msg("main-original", "main history"))
                .await;
            live.shutdown().await.unwrap();
            let child = store
                .open_sub_agent(
                    thread_id,
                    "agent-A",
                    sub_meta(Some("Explore"), created_at + 1),
                )
                .await
                .unwrap();
            child
                .append_message(msg("child-original", "sidechain history"))
                .await;
            child.shutdown().await.unwrap();
            store.archive(thread_id).await.unwrap();

            let (failed_source, failed_dest) = if fail_main_sync {
                (&archived_main, &active_main)
            } else {
                (&archived_data, &active_data)
            };
            fail_unarchive_parent_sync_times(
                &failed_source.to_string_lossy(),
                &failed_dest.to_string_lossy(),
                2,
            );
            let failed = store.unarchive(thread_id).await;
            assert_eq!(
                failed.status,
                UnarchiveSessionStatus::PersistenceFailed,
                "an unproven rename cannot be acknowledged"
            );
            assert!(
                Path::new(&store.unarchive_journal_dir(thread_id)).is_dir(),
                "durable intent remains after parent sync and recovery both fail"
            );
            clear_unarchive_parent_sync_failure();

            // The main namespace still selects rollback vs roll-forward, but a
            // new store must first prove both parent-directory barriers before
            // it can clear intent and expose the session.
            let fresh = store_at(&dir);
            let reopened = fresh.reopen(thread_id).await.unwrap();
            reopened.shutdown().await.unwrap();
            assert!(
                !Path::new(&fresh.unarchive_journal_dir(thread_id)).exists(),
                "cold recovery clears intent only after successful re-sync"
            );
            assert_eq!(active_main.is_file(), fail_main_sync);
            assert_eq!(active_data.is_dir(), fail_main_sync);
            assert_eq!(archived_main.is_file(), !fail_main_sync);
            assert_eq!(archived_data.is_dir(), !fail_main_sync);

            let main = fresh.resume(thread_id).await.unwrap();
            assert_eq!(
                main.messages.iter().map(uuid_of).collect::<Vec<_>>(),
                vec!["main-original"]
            );
            let child = fresh.load_sub_agent(thread_id, "agent-A").await.unwrap();
            assert_eq!(
                child.messages.iter().map(uuid_of).collect::<Vec<_>>(),
                vec!["child-original"]
            );
            let listed = fresh
                .list(ListOptions {
                    include_archived: Some(true),
                    ..Default::default()
                })
                .await
                .unwrap();
            assert_eq!(listed.items.len(), 1);
            assert_eq!(listed.items[0].archived, Some(!fail_main_sync));

            std::fs::remove_dir_all(&dir).ok();
        }
    }

    #[tokio::test]
    async fn unarchive_between_pages_preserves_cursor_order_without_duplication() {
        let dir = temp_dir();
        let store = store_at(&dir);
        for i in 0..5 {
            write_session(&store, &format!("s{i}"), 1_700_000_000 + i).await;
        }
        store.archive("s2").await.unwrap();

        let first = store
            .list(ListOptions {
                limit: Some(2),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            first
                .items
                .iter()
                .map(|item| item.id.clone())
                .collect::<Vec<_>>(),
            vec!["s4", "s3"]
        );
        assert_eq!(
            store.unarchive("s2").await.status,
            UnarchiveSessionStatus::Restored
        );

        let mut seen = first
            .items
            .iter()
            .map(|item| item.id.clone())
            .collect::<Vec<_>>();
        let mut cursor = first.next_cursor;
        while let Some(current) = cursor {
            let page = store
                .list(ListOptions {
                    cursor: Some(current),
                    limit: Some(2),
                    ..Default::default()
                })
                .await
                .unwrap();
            seen.extend(page.items.iter().map(|item| item.id.clone()));
            cursor = page.next_cursor;
        }
        // Continuation pages retain first-page membership; a fresh head sees
        // the restored session without injecting it into an older snapshot.
        assert_eq!(seen, vec!["s4", "s3", "s1", "s0"]);
        assert!(store
            .list(ListOptions::default())
            .await
            .unwrap()
            .items
            .iter()
            .any(|row| row.id == "s2"));
        let unique = seen.iter().collect::<std::collections::HashSet<_>>();
        assert_eq!(unique.len(), seen.len());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn unarchive_from_another_store_refreshes_cached_lists_and_pagination() {
        let dir = temp_dir();
        let mutator = store_at(&dir);
        for i in 0..5 {
            write_session(&mutator, &format!("s{i}"), 1_700_000_000 + i).await;
        }
        mutator.archive("s2").await.unwrap();

        // Prime an independent store's active and archive indexes before the
        // on-disk namespace move. This is the long-lived Desktop process shape:
        // another endpoint/process can mutate the shared sessions directory.
        let lister = store_at(&dir);
        let primed = lister
            .list(ListOptions {
                include_archived: Some(true),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            primed
                .items
                .iter()
                .find(|item| item.id == "s2")
                .and_then(|item| item.archived),
            Some(true)
        );

        let first = lister
            .list(ListOptions {
                limit: Some(2),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            first
                .items
                .iter()
                .map(|item| item.id.clone())
                .collect::<Vec<_>>(),
            vec!["s4", "s3"]
        );

        assert_eq!(
            mutator.unarchive("s2").await.status,
            UnarchiveSessionStatus::Restored
        );

        let mut seen = first
            .items
            .iter()
            .map(|item| item.id.clone())
            .collect::<Vec<_>>();
        let mut cursor = first.next_cursor;
        while let Some(current) = cursor {
            let page = lister
                .list(ListOptions {
                    cursor: Some(current),
                    limit: Some(2),
                    ..Default::default()
                })
                .await
                .unwrap();
            seen.extend(page.items.iter().map(|item| item.id.clone()));
            cursor = page.next_cursor;
        }
        assert_eq!(seen, vec!["s4", "s3", "s1", "s0"]);
        assert_eq!(
            seen.iter().collect::<std::collections::HashSet<_>>().len(),
            seen.len()
        );

        let refreshed = lister
            .list(ListOptions {
                include_archived: Some(true),
                ..Default::default()
            })
            .await
            .unwrap();
        let restored = refreshed
            .items
            .iter()
            .filter(|item| item.id == "s2")
            .collect::<Vec<_>>();
        assert_eq!(restored.len(), 1);
        assert_eq!(restored[0].archived, Some(false));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn delete_removes_the_main_rollout_and_sidechain_directory() {
        let dir = temp_dir();
        let store = store_at(&dir);
        let live = store.create(meta()).await.unwrap();
        live.append_message(msg("u1", "hi")).await;
        live.shutdown().await.unwrap();
        let agent = store
            .open_sub_agent("sess-1", "agent-A", sub_meta(Some("Explore"), 1))
            .await
            .unwrap();
        agent.append_message(msg("a1", "child")).await;
        agent.shutdown().await.unwrap();
        let process_dir = background_process_output_dir(&store.dir, "sess-1");
        std::fs::create_dir_all(&process_dir).unwrap();
        std::fs::write(Path::new(&process_dir).join("shell-1.log"), b"output").unwrap();

        store.delete("sess-1").await.unwrap();

        assert!(store.resume("sess-1").await.is_err());
        assert!(store
            .list(ListOptions {
                include_archived: Some(true),
                ..Default::default()
            })
            .await
            .unwrap()
            .items
            .is_empty());
        assert!(std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .all(|e| native_text_to_store_host(&e.file_name()) != "1700000000-sess-1"));
        assert!(!Path::new(&process_dir).exists());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn host_metadata_roundtrips_replaces_wholesale_and_leaves_the_transcript_intact() {
        let dir = temp_dir();
        let store = store_at(&dir);
        let live = store.create(meta()).await.unwrap();
        live.append_message(msg("u1", "hi")).await;
        live.shutdown().await.unwrap();
        let rollout = dir.join("1700000000-sess-1.jsonl");
        let content_updated_at = filetime::FileTime::from_unix_time(1_700_000_500, 0);
        filetime::set_file_mtime(&rollout, content_updated_at).unwrap();

        // Attach: the value comes back verbatim from the listing.
        let attachment = exact_json(r#"{"customTitle":"Renamed","workspaceKind":"project"}"#);
        store
            .update_host_metadata("sess-1", Some(attachment.clone()))
            .await
            .unwrap();
        let listed = store.list(ListOptions::default()).await.unwrap();
        assert_eq!(listed.items[0].host_metadata, Some(attachment));
        assert_eq!(listed.items[0].updated_at, 1_700_000_500_000);

        // Whole-value replacement, not a merge: the old keys are gone.
        let replaced = exact_json(r#"{"forkedFrom":{"sessionId":"src"}}"#);
        store
            .update_host_metadata("sess-1", Some(replaced.clone()))
            .await
            .unwrap();
        let listed = store.list(ListOptions::default()).await.unwrap();
        assert_eq!(listed.items[0].host_metadata, Some(replaced.clone()));
        assert_eq!(listed.items[0].updated_at, 1_700_000_500_000);

        // The header-only rewrite must leave the transcript bytes untouched.
        let resumed = store.resume("sess-1").await.unwrap();
        assert_eq!(resumed.messages.len(), 1);
        assert_eq!(resumed.meta.host_metadata, Some(replaced));

        // None removes the attachment.
        store.update_host_metadata("sess-1", None).await.unwrap();
        let listed = store.list(ListOptions::default()).await.unwrap();
        assert_eq!(listed.items[0].host_metadata, None);

        // An unknown thread is an error, not a silent no-op.
        assert!(store.update_host_metadata("missing", None).await.is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn rename_rekeys_the_rollout_header_sidechain_tree_and_listing() {
        let dir = temp_dir();
        let store = store_at(&dir);
        let live = store.create(meta()).await.unwrap();
        live.append_message(msg("u1", "hi")).await;
        live.shutdown().await.unwrap();
        let agent = store
            .open_sub_agent("sess-1", "agent-A", sub_meta(Some("Explore"), 1))
            .await
            .unwrap();
        agent.append_message(msg("a1", "child")).await;
        agent.shutdown().await.unwrap();
        let old_process_dir = background_process_output_dir(&store.dir, "sess-1");
        std::fs::create_dir_all(&old_process_dir).unwrap();
        std::fs::write(
            Path::new(&old_process_dir).join("process-1.log"),
            b"persisted output",
        )
        .unwrap();

        store.rename("sess-1", "host-key-1").await.unwrap();

        // The old id is gone from every read path; the new id carries the
        // full transcript, header identity, and sub-agent tree.
        assert!(store.resume("sess-1").await.is_err());
        let resumed = store.resume("host-key-1").await.unwrap();
        assert_eq!(resumed.meta.id, "host-key-1");
        assert_eq!(resumed.messages.len(), 1);
        assert_eq!(
            store.list_sub_agents("host-key-1").await.unwrap(),
            vec!["agent-A".to_string()]
        );
        let fresh = store_at(&dir);
        let page = fresh.list(ListOptions::default()).await.unwrap();
        assert!(page.items.iter().any(|t| t.id == "host-key-1"));
        assert!(page.items.iter().all(|t| t.id != "sess-1"));
        let new_process_dir = background_process_output_dir(&store.dir, "host-key-1");
        assert!(!Path::new(&old_process_dir).exists());
        assert_eq!(
            std::fs::read(Path::new(&new_process_dir).join("process-1.log")).unwrap(),
            b"persisted output"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn rename_rejects_a_claimed_destination_and_a_missing_source() {
        let dir = temp_dir();
        let store = store_at(&dir);
        let live = store.create(meta()).await.unwrap();
        live.append_message(msg("u1", "hi")).await;
        live.shutdown().await.unwrap();
        let mut second = meta();
        second.id = "sess-2".into();
        let other = store.create(second).await.unwrap();
        other.shutdown().await.unwrap();

        let claimed = store.rename("sess-1", "sess-2").await.unwrap_err();
        assert_eq!(claimed.kind(), io::ErrorKind::AlreadyExists);
        let missing = store.rename("sess-9", "host-key-9").await.unwrap_err();
        assert_eq!(missing.kind(), io::ErrorKind::NotFound);
        // No-op identity rename succeeds without touching the store.
        store.rename("sess-1", "sess-1").await.unwrap();
        assert!(store.resume("sess-1").await.is_ok());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn update_title_rewrites_rust_owned_metadata_visible_to_a_fresh_lister() {
        let dir = temp_dir();
        let store = store_at(&dir);
        let live = store.create(meta()).await.unwrap();
        live.append_message(msg("u1", "hi")).await;
        live.shutdown().await.unwrap();

        store
            .update_title("sess-1", "  Updated title  ")
            .await
            .unwrap();

        let fresh = store_at(&dir);
        let page = fresh.list(ListOptions::default()).await.unwrap();
        let summary = page.items.iter().find(|t| t.id == "sess-1").unwrap();
        assert_eq!(summary.title, Some(RuntimeString::from("Updated title")));
        let resumed = fresh.resume("sess-1").await.unwrap();
        assert_eq!(
            resumed.meta.title,
            Some(RuntimeString::from("Updated title"))
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn live_rollout_update_title_rewrites_metadata_while_writer_is_open() {
        let dir = temp_dir();
        let store = store_at(&dir);
        let live = store.create(meta()).await.unwrap();
        live.append_message(msg("u1", "hi")).await;
        live.flush().await.unwrap();

        live.update_title("  Live title  ").await.unwrap();
        live.flush().await.unwrap();

        let page = store.list(ListOptions::default()).await.unwrap();
        let summary = page.items.iter().find(|t| t.id == "sess-1").unwrap();
        assert_eq!(summary.title, Some(RuntimeString::from("Live title")));
        live.shutdown().await.unwrap();
        let resumed = store.resume("sess-1").await.unwrap();
        assert_eq!(resumed.meta.title, Some(RuntimeString::from("Live title")));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn live_rollout_updates_host_metadata_without_relocking_or_reordering() {
        let dir = temp_dir();
        let store = store_at(&dir);
        let live = store.create(meta()).await.unwrap();
        live.append_message(msg("u1", "before")).await;
        live.flush().await.unwrap();
        let updated_at = store.list(ListOptions::default()).await.unwrap().items[0].updated_at;
        let attachment = exact_json(r#"{"sidebarPinnedAt":1700000000000}"#);

        live.update_host_metadata(Some(attachment.clone()))
            .await
            .unwrap();
        let listed = store.list(ListOptions::default()).await.unwrap();
        assert_eq!(listed.items[0].host_metadata, Some(attachment.clone()));
        assert_eq!(listed.items[0].updated_at, updated_at);

        live.append_message(msg("u2", "after")).await;
        live.shutdown().await.unwrap();
        let resumed = store.resume("sess-1").await.unwrap();
        assert_eq!(resumed.meta.host_metadata, Some(attachment));
        assert_eq!(resumed.messages.len(), 2);
        std::fs::remove_dir_all(&dir).ok();
    }

    // ---- list() cwd filter (resume candidate selection) ----

    #[tokio::test]
    async fn returns_only_sessions_whose_recorded_cwd_matches_path_normalized() {
        let dir = temp_dir();
        let store = store_at(&dir);
        for (id, cwd, created_at) in [
            ("a", "/work/foo", 1),
            ("b", "/work/bar", 2),
            ("c", "/work/foo", 3),
        ] {
            let live = store.create(meta_with(id, created_at, cwd)).await.unwrap();
            live.shutdown().await.unwrap();
        }

        let ids = |page: ThreadPage| -> Vec<RuntimeString> {
            let mut ids: Vec<RuntimeString> = page.items.into_iter().map(|i| i.id).collect();
            ids.sort();
            ids
        };
        let foo = store
            .list(ListOptions {
                cwd: Some("/work/foo".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(ids(foo), vec!["a", "c"]);
        let bar = store
            .list(ListOptions {
                cwd: Some("/work/bar".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            bar.items.iter().map(|i| i.id.clone()).collect::<Vec<_>>(),
            vec!["b"]
        );
        // No filter → every session.
        assert_eq!(
            store
                .list(ListOptions::default())
                .await
                .unwrap()
                .items
                .len(),
            3
        );
        // Path normalization: a `.`-suffixed path still matches.
        let dotted = store
            .list(ListOptions {
                cwd: Some("/work/foo/.".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(ids(dotted), vec!["a", "c"]);
        std::fs::remove_dir_all(&dir).ok();
    }

    // ---- sub-agent persistence ----

    fn child_user() -> Message {
        msg("cu1", "sub prompt")
    }

    fn child_assistant() -> Message {
        Message::Assistant(AssistantMessage {
            uuid: "ca1".into(),
            content: vec![text("sub answer")],
            model: "m".into(),
            usage: None,
        })
    }

    #[tokio::test]
    /// Concurrent sub-agent spawns share one session, and the sidechain dir
    /// resolution serializes behind that session's lifecycle lock. A try-once
    /// acquire used to refuse the later spawn outright, which the spawn path
    /// then swallowed — leaving the child with no sidechain at all. The wait
    /// must let both through.
    async fn concurrent_open_sub_agent_calls_both_resolve_the_sidechain_dir() {
        let dir = temp_dir();
        let store = Arc::new(store_at(&dir));
        let live = store.create(meta()).await.unwrap();
        live.shutdown().await.unwrap(); // materialize the session file so the dir exists

        let first = store.clone();
        let second = store.clone();
        let (a, b) = tokio::join!(
            first.open_sub_agent("sess-1", "agent-A", sub_meta(Some("Explore"), 1)),
            second.open_sub_agent("sess-1", "agent-B", sub_meta(Some("Explore"), 2)),
        );
        let a = a.expect("first concurrent sidechain open resolves");
        let b = b.expect("second concurrent sidechain open resolves");
        a.append_message(child_user()).await;
        b.append_message(child_assistant()).await;
        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();

        for (agent, expected) in [("agent-A", "user"), ("agent-B", "assistant")] {
            let loaded = store.load_sub_agent("sess-1", agent).await.unwrap();
            assert_eq!(
                loaded.messages.iter().map(type_of).collect::<Vec<_>>(),
                vec![expected],
                "{agent} sidechain must hold its own transcript"
            );
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn open_sub_agent_writes_a_per_agent_jsonl_and_meta_sidecar() {
        let dir = temp_dir();
        let store = store_at(&dir);
        let live = store.create(meta()).await.unwrap();
        live.shutdown().await.unwrap(); // materialize the session file so the dir exists

        let sub = store
            .open_sub_agent("sess-1", "agent-A", sub_meta(Some("Explore"), 1))
            .await
            .unwrap();
        sub.append_message(child_user()).await;
        sub.append_message(child_assistant()).await;
        sub.shutdown().await.unwrap();

        let loaded = store.load_sub_agent("sess-1", "agent-A").await.unwrap();
        assert_eq!(
            loaded.messages.iter().map(type_of).collect::<Vec<_>>(),
            vec!["user", "assistant"]
        );
        let loaded_meta = loaded.meta.unwrap();
        assert_eq!(
            loaded_meta.agent_type.as_ref(),
            Some(&RuntimeString::from("Explore"))
        );
        assert_eq!(loaded_meta.model, "m");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn recovers_a_sub_agents_surfaced_tools_from_its_rollout_events() {
        let dir = temp_dir();
        let store = store_at(&dir);
        store
            .create(meta())
            .await
            .unwrap()
            .shutdown()
            .await
            .unwrap();

        let sub = store
            .open_sub_agent("sess-1", "agent-S", sub_meta(None, 1))
            .await
            .unwrap();
        sub.append_message(child_user()).await;
        // Two surfacing events (append-only delta); restore is their union, deduped.
        sub.append_event(
            "tools_surfaced".into(),
            Some(AppendEventOptions {
                turn_id: None,
                payload: Some(exact_json(r#"{"names":["alpha","beta"]}"#)),
            }),
        )
        .await;
        sub.append_event(
            "tools_surfaced".into(),
            Some(AppendEventOptions {
                turn_id: None,
                payload: Some(exact_json(r#"{"names":["beta","gamma"]}"#)),
            }),
        )
        .await;
        sub.shutdown().await.unwrap();

        let loaded = store.load_sub_agent("sess-1", "agent-S").await.unwrap();
        let mut surfaced = loaded.surfaced_tools.unwrap();
        surfaced.sort();
        assert_eq!(
            surfaced,
            vec![
                RuntimeString::from("alpha"),
                RuntimeString::from("beta"),
                RuntimeString::from("gamma"),
            ]
        );
        // Events stay out of the model-visible projection.
        assert_eq!(
            loaded.messages.iter().map(type_of).collect::<Vec<_>>(),
            vec!["user"]
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn sanitizes_a_path_traversal_agent_id() {
        let dir = temp_dir();
        let store = store_at(&dir);
        store
            .create(meta())
            .await
            .unwrap()
            .shutdown()
            .await
            .unwrap();

        // A model-controlled `resume` like '../../../../tmp/pwn' must not write
        // outside.
        let evil = "../../../../tmp/pwn";
        let sub = store
            .open_sub_agent("sess-1", evil, sub_meta(None, 1))
            .await
            .unwrap();
        sub.append_message(child_user()).await;
        sub.shutdown().await.unwrap();

        // Nothing was created at an escaped location.
        assert!(std::fs::read(std::env::temp_dir().join("pwn.jsonl")).is_err());
        assert!(std::fs::read(dir.join("tmp").join("pwn.jsonl")).is_err());
        // The transcript landed inside the session's subagents dir under a flat
        // name.
        let sub_dir = dir.join("1700000000-sess-1").join("subagents");
        let names: Vec<String> = std::fs::read_dir(&sub_dir)
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .map(|e| native_text_to_store_host(&e.file_name()))
                    .collect()
            })
            .unwrap_or_default();
        assert!(names
            .iter()
            .any(|n| n.starts_with("agent-") && n.ends_with(".jsonl")));
        assert!(names.iter().all(|n| !n.contains("..") && !n.contains('/')));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn load_sub_agent_returns_empty_for_unknown_agent_and_list_globs_the_dir() {
        let dir = temp_dir();
        let store = store_at(&dir);
        store
            .create(meta())
            .await
            .unwrap()
            .shutdown()
            .await
            .unwrap();

        assert!(store
            .load_sub_agent("sess-1", "nope")
            .await
            .unwrap()
            .messages
            .is_empty());
        assert!(store.list_sub_agents("sess-1").await.unwrap().is_empty());

        for id in ["agent-A", "agent-B"] {
            let sub = store
                .open_sub_agent("sess-1", id, sub_meta(None, 1))
                .await
                .unwrap();
            sub.append_message(child_user()).await;
            sub.shutdown().await.unwrap();
        }
        let mut agents = store.list_sub_agents("sess-1").await.unwrap();
        agents.sort();
        assert_eq!(agents, vec!["agent-A", "agent-B"]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_fresh_store_instance_reads_the_sub_agent_transcript_back() {
        let dir = temp_dir();
        let store1 = store_at(&dir);
        store1
            .create(meta())
            .await
            .unwrap()
            .shutdown()
            .await
            .unwrap();
        let sub = store1
            .open_sub_agent("sess-1", "agent-A", sub_meta(None, 1))
            .await
            .unwrap();
        sub.append_message(child_user()).await;
        sub.append_message(child_assistant()).await;
        sub.shutdown().await.unwrap();

        // A brand-new store over the same dir == a fresh process: it reads from
        // disk.
        let store2 = store_at(&dir);
        let loaded = store2.load_sub_agent("sess-1", "agent-A").await.unwrap();
        assert_eq!(
            loaded.messages.iter().map(type_of).collect::<Vec<_>>(),
            vec!["user", "assistant"]
        );
        assert_eq!(
            store2.list_sub_agents("sess-1").await.unwrap(),
            vec!["agent-A"]
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn resume_continuation_appends_to_the_same_agent_file() {
        let dir = temp_dir();
        let store = store_at(&dir);
        store
            .create(meta())
            .await
            .unwrap()
            .shutdown()
            .await
            .unwrap();

        let first = store
            .open_sub_agent("sess-1", "agent-A", sub_meta(None, 1))
            .await
            .unwrap();
        first.append_message(child_user()).await;
        first.append_message(child_assistant()).await;
        first.shutdown().await.unwrap();

        // Re-open the SAME agentId and append a continuation (mirrors a resume).
        let second = store
            .open_sub_agent("sess-1", "agent-A", sub_meta(None, 2))
            .await
            .unwrap();
        second.append_message(msg("cu2", "more")).await;
        second.shutdown().await.unwrap();

        let loaded = store.load_sub_agent("sess-1", "agent-A").await.unwrap();
        assert_eq!(
            loaded.messages.iter().map(type_of).collect::<Vec<_>>(),
            vec!["user", "assistant", "user"]
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn child_writer_lease_blocks_reattachment_until_shutdown() {
        let dir = temp_dir();
        let store = store_at(&dir);
        store
            .create(meta())
            .await
            .unwrap()
            .shutdown()
            .await
            .unwrap();

        // An unclosed live handle remains a writer; a later continuation must
        // wait for shutdown instead of pretending that handle has crashed.
        let stale = store
            .open_sub_agent("sess-1", "agent-A", sub_meta(None, 1))
            .await
            .unwrap();
        stale.append_message(child_user()).await;
        stale.flush().await.unwrap();

        assert_eq!(
            store
                .open_sub_agent("sess-1", "agent-A", sub_meta(None, 1))
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        stale.shutdown().await.unwrap();
        let reopened = store
            .open_sub_agent("sess-1", "agent-A", sub_meta(None, 1))
            .await
            .unwrap();
        reopened.append_message(child_assistant()).await;
        reopened.shutdown().await.unwrap();

        assert!(!store
            .load_sub_agent("sess-1", "agent-A")
            .await
            .unwrap()
            .messages
            .is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn writes_the_meta_sidecar_once_a_resume_preserves_the_original_created_at() {
        let dir = temp_dir();
        let store = store_at(&dir);
        store
            .create(meta())
            .await
            .unwrap()
            .shutdown()
            .await
            .unwrap();

        let first = store
            .open_sub_agent("sess-1", "agent-A", sub_meta(Some("Explore"), 111))
            .await
            .unwrap();
        first.append_message(child_user()).await;
        first.shutdown().await.unwrap();

        // A resume re-opens with a DIFFERENT createdAt — the original must
        // survive.
        let second = store
            .open_sub_agent("sess-1", "agent-A", sub_meta(Some("Explore"), 999))
            .await
            .unwrap();
        second.shutdown().await.unwrap();

        let loaded = store.load_sub_agent("sess-1", "agent-A").await.unwrap();
        assert_eq!(loaded.meta.unwrap().created_at, 111);
        std::fs::remove_dir_all(&dir).ok();
    }
}
