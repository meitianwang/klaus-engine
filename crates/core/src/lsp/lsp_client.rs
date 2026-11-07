//! One language-server connection.
//!
//! Speaks LSP as JSON-RPC over the server's stdio, using `Content-Length`
//! framed messages. Document synchronization follows the server's negotiated
//! `textDocumentSync` capability; a per-uri monotonic version and the last
//! synchronized text live here. Diagnostics use the
//! pull model (`textDocument/diagnostic`) when the server advertises it, else
//! stored server-pushed `publishDiagnostics`. Server/transport failures remain
//! fail-soft; stable document eligibility failures are returned as typed query
//! errors so tools do not misreport them as empty results.

use crate::fs::node_path::{
    is_absolute_runtime, project_runtime_path, resolve_runtime, runtime_path_from_native,
};
use crate::fs::text_file::decode_text_buffer;
use crate::lsp::types::{
    LspCall, LspCallHierarchyItem, LspDiagnostic, LspHoverResult, LspLocation, LspPosition,
    LspQueryError, LspQueryResult, LspServerConfig, LspSeverity, LspSymbol, LspWorkspaceSymbol,
};
use crate::runtime_process::{
    RuntimeChildStdin, RuntimeChildStdout, RuntimeProcessContext, RuntimeSignal,
    SpawnedRuntimeProcess,
};
use crate::runtime_values::{
    runtime_codec, runtime_json, RuntimeJson, RuntimeNumber, RuntimeObject, RuntimePath,
    RuntimeString,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, Weak};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{oneshot, Mutex as TokioMutex, OwnedRwLockReadGuard, RwLock as TokioRwLock};
use tokio::task::JoinHandle;

/// Default per-request timeout; a hung server returns empty rather than stalling a turn.
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;
/// Default push-diagnostics settle window: how long `notify_edited` waits for republish.
const DEFAULT_DIAGNOSTICS_SETTLE_MS: u64 = 600;
/// Cap items surfaced from a server response (a misbehaving server can't flood context).
const MAX_RESULT_ITEMS: usize = 2000;
/// File-size ceiling (10 MB) for LSP document analysis.
const MAX_LSP_FILE_SIZE_BYTES: u64 = 10_000_000;
/// A path can be replaced independently of the handle we opened. Re-open and
/// re-read a bounded number of times rather than ever synchronizing bytes from
/// a handle that is no longer the file named by the path.
const MAX_DOCUMENT_READ_ATTEMPTS: usize = 3;

/// LSP "ContentModified": the document changed while a request was in flight, so
/// the server abandoned it. The request is RETRIED (the only transient code worth
/// retrying) up to [`MAX_CONTENT_MODIFIED_RETRIES`] times with an exponential
/// backoff of [`RETRY_BASE_DELAY_MS`]·2^attempt (500 / 1000 / 2000 ms). Any other
/// error code falls straight through to the fail-soft `None`.
const LSP_ERROR_CONTENT_MODIFIED: i64 = -32801;
const MAX_CONTENT_MODIFIED_RETRIES: u32 = 3;
const RETRY_BASE_DELAY_MS: u64 = 500;
/// JSON-RPC "method not found" — the benign reply to an unhandled server→client request.
const METHOD_NOT_FOUND: i64 = -32601;

macro_rules! exact_json {
    (null) => {
        RuntimeJson::Null
    };
    ([ $($value:tt),* $(,)? ]) => {
        RuntimeJson::Array(vec![$(exact_json!($value)),*])
    };
    ({ $($key:literal : $value:tt),* $(,)? }) => {{
        #[allow(unused_mut)]
        let mut object = RuntimeObject::new();
        $(object.insert($key, exact_json!($value));)*
        RuntimeJson::Object(object)
    }};
    ($value:expr) => {
        runtime_codec::encode_value(&$value).expect("exact LSP JSON expression must encode")
    };
}

/// Tunables for one client. Absent fields fall back to the module defaults.
#[derive(Debug, Clone, Default)]
pub struct LspClientOptions {
    /// Per-request timeout in milliseconds.
    pub request_timeout_ms: Option<u64>,
    /// Push-diagnostics settle window in milliseconds.
    pub diagnostics_settle_ms: Option<u64>,
}

/// A JSON-RPC error reply — only the numeric code drives retry decisions.
#[derive(Debug)]
struct ResponseError {
    code: i64,
}

/// Outcome of one request attempt.
enum Outcome {
    Success(RuntimeJson),
    ResponseError(ResponseError),
    Timeout,
    /// The connection died while the request was in flight.
    Closed,
}

/// State shared between the public client handle and the background read loop.
struct Shared {
    /// The child's stdin; `None` before start / after dispose.
    writer: TokioMutex<Option<RuntimeChildStdin>>,
    /// In-flight requests keyed by id, awaiting their response.
    pending: StdMutex<HashMap<i64, oneshot::Sender<Result<RuntimeJson, ResponseError>>>>,
    next_id: AtomicI64,
    /// Globally unique ordering tokens; only epochs on the same URI are
    /// compared. Queries remain concurrent and serialize only their brief
    /// document-state commit.
    next_document_query_attempt: AtomicU64,
    dead: AtomicBool,
    /// True once the server advertised `diagnosticProvider` (pull model).
    pull_diagnostics: AtomicBool,
    /// Latest server-pushed raw diagnostics per uri (push model).
    pushed: StdMutex<HashMap<RuntimeString, Vec<RuntimeJson>>>,
    /// Resolvers waiting for the next publishDiagnostics on a uri (push settle).
    waiters: StdMutex<HashMap<RuntimeString, HashMap<u64, oneshot::Sender<()>>>>,
    next_waiter_id: AtomicU64,
    /// Negotiated text-document synchronization contract.
    document_sync: StdMutex<DocumentSyncOptions>,
    /// Open/current document state plus per-URI invalidation generations. The
    /// async mutex serializes didOpen, didChange, and didClose; the generation
    /// check prevents a disk read started before a delete from reopening the
    /// deleted URI after that delete has completed.
    documents: TokioMutex<DocumentLifecycleState>,
    /// Weak entries preserve per-URI request/change exclusion without retaining
    /// one allocation for every path ever queried in a long session.
    document_gates: StdMutex<HashMap<RuntimeString, Weak<TokioRwLock<()>>>>,
    /// Deterministic interleaving probes for the unit tests. Grouping them in
    /// one test-only member keeps the production state list readable.
    #[cfg(test)]
    test_probes: SharedTestProbes,
}

/// Test-only interleaving barriers and observation buffers for [`Shared`].
#[cfg(test)]
#[derive(Default)]
struct SharedTestProbes {
    after_document_read: StdMutex<Option<DocumentReadBarrier>>,
    after_document_handle_read: StdMutex<Option<DocumentReadBarrier>>,
    after_document_handle_read_sequence: StdMutex<Vec<DocumentReadBarrier>>,
    before_document_query_epoch: StdMutex<Option<DocumentReadBarrier>>,
    after_request_sent: StdMutex<Option<DocumentReadBarrier>>,
    before_frame_write_complete: StdMutex<Option<DocumentReadBarrier>>,
    completed_frame_writes: AtomicU64,
    before_lifecycle_state_commit: StdMutex<Option<DocumentReadBarrier>>,
    between_lifecycle_frames: StdMutex<Option<DocumentReadBarrier>>,
    sent_messages: StdMutex<Vec<RuntimeJson>>,
}

#[cfg(test)]
struct DocumentReadBarrier {
    reached: oneshot::Sender<()>,
    resume: oneshot::Receiver<()>,
}

struct CurrentDocument {
    uri: RuntimeString,
    /// A Query holds a shared lease through its actual request. A content
    /// change/close needs the URI's exclusive lease, so the URI cannot become
    /// non-current between ensure and request. Authoritative callers return no
    /// lease because they do not issue a query request.
    _request_guard: Option<OwnedRwLockReadGuard<()>>,
}

/// Balances the lifecycle's in-flight counter even when an async query is
/// cancelled while reading, waiting on a URI gate, synchronizing, or issuing
/// its request. Normal completion performs the decrement inline; cancellation
/// schedules the same cleanup on the current runtime.
struct QueryRegistration {
    shared: Arc<Shared>,
    uri: RuntimeString,
    active: bool,
}

impl QueryRegistration {
    fn finish(&mut self, documents: &mut DocumentLifecycleState) {
        if self.active {
            self.active = false;
            documents.finish_query(&self.uri);
        }
    }
}

impl Drop for QueryRegistration {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        self.active = false;
        let shared = self.shared.clone();
        let uri = self.uri.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                shared.documents.lock().await.finish_query(&uri);
            });
        }
    }
}

/// Owns one pending JSON-RPC id. Response dispatch may remove the sender first;
/// timeout, retry, or future cancellation still removes the id synchronously.
struct PendingRequestRegistration {
    shared: Arc<Shared>,
    id: i64,
}

impl Drop for PendingRequestRegistration {
    fn drop(&mut self) {
        self.shared.pending.lock().unwrap().remove(&self.id);
    }
}

/// Owns one push-diagnostics waiter so a timeout or cancelled caller cannot
/// leave dead senders accumulating under a long-lived URI.
struct DiagnosticsWaiterRegistration {
    shared: Arc<Shared>,
    uri: RuntimeString,
    id: u64,
}

impl Drop for DiagnosticsWaiterRegistration {
    fn drop(&mut self) {
        let mut waiters = self.shared.waiters.lock().unwrap();
        let remove_uri = waiters.get_mut(&self.uri).is_some_and(|per_uri| {
            per_uri.remove(&self.id);
            per_uri.is_empty()
        });
        if remove_uri {
            waiters.remove(&self.uri);
        }
    }
}

/// A document-sync notification and its local lifecycle mutation form one
/// logical transaction. If the caller is cancelled while the notification is
/// being completed by the retained frame owner, poison the connection: the
/// next routed operation will start a fresh server instead of continuing with
/// possibly divergent client/server document state.
struct LifecycleCommitGuard {
    shared: Arc<Shared>,
    armed: bool,
    committed: bool,
}

impl LifecycleCommitGuard {
    fn new(shared: Arc<Shared>) -> Self {
        Self {
            shared,
            armed: false,
            committed: false,
        }
    }

    fn arm(&mut self) {
        self.armed = true;
    }

    fn is_armed(&self) -> bool {
        self.armed
    }

    fn commit(&mut self) {
        self.committed = true;
    }
}

impl Drop for LifecycleCommitGuard {
    fn drop(&mut self) {
        if self.armed && !self.committed {
            self.shared.mark_dead();
        }
    }
}

struct PreparedCallHierarchy {
    items: Vec<RuntimeJson>,
    /// Keep the document current across both prepareCallHierarchy and the
    /// subsequent incoming/outgoing request.
    _document: CurrentDocument,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DocumentChangeSyncKind {
    None,
    Full,
    Incremental,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DocumentSyncIntent {
    Query,
    AuthoritativeEdit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DocumentSyncOptions {
    open_close: bool,
    change: DocumentChangeSyncKind,
}

impl Default for DocumentSyncOptions {
    fn default() -> Self {
        Self {
            open_close: true,
            change: DocumentChangeSyncKind::Full,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OpenDocument {
    version: i64,
    content: RuntimeString,
    opened: bool,
}

#[derive(Default)]
struct QueryLifecycleState {
    latest_commit_epoch: u64,
    in_flight: u64,
}

#[derive(Default)]
struct DocumentLifecycleState {
    open: HashMap<RuntimeString, OpenDocument>,
    /// Kept even when no document is open so in-flight pre-delete reads can
    /// detect that their snapshot lost the lifecycle race.
    invalidation_generations: HashMap<RuntimeString, u64>,
    /// Query/query ordering is independent from authoritative mutation
    /// generations: missing and eligibility-error commits need a tombstone so
    /// an older successful disk read cannot reopen stale content.
    queries: HashMap<RuntimeString, QueryLifecycleState>,
}

impl DocumentLifecycleState {
    fn invalidation_generation(&self, uri: &RuntimeString) -> u64 {
        self.invalidation_generations
            .get(uri)
            .copied()
            .unwrap_or_default()
    }

    fn invalidate(&mut self, uri: &RuntimeString) {
        let generation = self
            .invalidation_generations
            .entry(uri.clone())
            .or_default();
        *generation = generation.wrapping_add(1);
    }

    fn begin_query(&mut self, uri: &RuntimeString) {
        let query = self.queries.entry(uri.clone()).or_default();
        query.in_flight = query.in_flight.saturating_add(1);
    }

    fn latest_query_commit(&self, uri: &RuntimeString) -> u64 {
        self.queries
            .get(uri)
            .map(|query| query.latest_commit_epoch)
            .unwrap_or_default()
    }

    fn commit_query(&mut self, uri: &RuntimeString, epoch: u64) {
        let query = self.queries.entry(uri.clone()).or_default();
        query.latest_commit_epoch = query.latest_commit_epoch.max(epoch);
    }

    fn reset_query_commit(&mut self, uri: &RuntimeString) {
        if let Some(query) = self.queries.get_mut(uri) {
            query.latest_commit_epoch = 0;
        }
    }

    fn finish_query(&mut self, uri: &RuntimeString) {
        if let Some(query) = self.queries.get_mut(uri) {
            debug_assert!(query.in_flight > 0);
            query.in_flight = query.in_flight.saturating_sub(1);
        }
        self.cleanup_quiescent_uri(uri);
    }

    fn cleanup_quiescent_uri(&mut self, uri: &RuntimeString) {
        let query_active = self
            .queries
            .get(uri)
            .is_some_and(|query| query.in_flight > 0);
        if !self.open.contains_key(uri) && !query_active {
            self.queries.remove(uri);
            self.invalidation_generations.remove(uri);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DocumentHandleSnapshot {
    identity: DocumentFileIdentity,
    size_bytes: u64,
    modified: DocumentModifiedIdentity,
}

#[cfg(unix)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct DocumentFileIdentity {
    device: u64,
    inode: u64,
}

#[cfg(unix)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct DocumentModifiedIdentity {
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
}

#[cfg(windows)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct DocumentFileIdentity {
    volume_serial_number: u32,
    file_index: u64,
}

#[cfg(windows)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct DocumentModifiedIdentity {
    creation_time: u64,
    last_write_time: u64,
}

#[cfg(not(any(unix, windows)))]
#[derive(Debug, Clone, PartialEq, Eq)]
struct DocumentFileIdentity {
    modified: Option<std::time::SystemTime>,
}

#[cfg(not(any(unix, windows)))]
#[derive(Debug, Clone, PartialEq, Eq)]
struct DocumentModifiedIdentity {
    modified: Option<std::time::SystemTime>,
}

struct OpenLspDocument {
    file: tokio::fs::File,
    snapshot: DocumentHandleSnapshot,
}

enum DocumentOpenObservation {
    Open(OpenLspDocument),
    Missing,
    Unsupported,
    TransientUnavailable,
}

enum ValidatedLspDocument {
    Eligible,
    Missing,
    EligibilityError(LspQueryError),
    TransientUnavailable,
}

enum QueryDocumentRead {
    Content { content: RuntimeString, epoch: u64 },
    Missing { epoch: u64 },
    EligibilityError { error: LspQueryError, epoch: u64 },
    TransientUnavailable,
}

enum QueryReadCandidate {
    Content {
        content: RuntimeString,
        snapshot: DocumentHandleSnapshot,
    },
    TooLarge {
        size_bytes: u64,
        snapshot: DocumentHandleSnapshot,
    },
    Missing,
    Unsupported,
}

enum OpenDocumentRead {
    Content(RuntimeString),
    TooLarge { size_bytes: u64 },
    Unstable,
}

#[cfg(test)]
#[derive(Default)]
struct DocumentReadHooks {
    after_handle_read: Option<DocumentReadBarrier>,
    after_handle_read_sequence: Vec<DocumentReadBarrier>,
    before_query_epoch: Option<DocumentReadBarrier>,
}

#[cfg(not(test))]
#[derive(Default)]
struct DocumentReadHooks;

#[cfg(test)]
async fn pause_after_document_handle_read(hooks: &mut DocumentReadHooks) {
    let barrier = hooks.after_handle_read.take().or_else(|| {
        (!hooks.after_handle_read_sequence.is_empty())
            .then(|| hooks.after_handle_read_sequence.remove(0))
    });
    if let Some(barrier) = barrier {
        let _ = barrier.reached.send(());
        let _ = barrier.resume.await;
    }
}

#[cfg(not(test))]
async fn pause_after_document_handle_read(_hooks: &mut DocumentReadHooks) {}

#[cfg(test)]
async fn pause_before_document_query_epoch(hooks: &mut DocumentReadHooks) {
    if let Some(barrier) = hooks.before_query_epoch.take() {
        let _ = barrier.reached.send(());
        let _ = barrier.resume.await;
    }
}

#[cfg(not(test))]
async fn pause_before_document_query_epoch(_hooks: &mut DocumentReadHooks) {}

#[cfg(test)]
async fn pause_before_lifecycle_state_commit(shared: &Arc<Shared>) {
    let barrier = shared
        .test_probes
        .before_lifecycle_state_commit
        .lock()
        .unwrap()
        .take();
    if let Some(barrier) = barrier {
        let _ = barrier.reached.send(());
        let _ = barrier.resume.await;
    }
}

#[cfg(not(test))]
async fn pause_before_lifecycle_state_commit(_shared: &Arc<Shared>) {}

#[cfg(test)]
async fn pause_between_lifecycle_frames(shared: &Arc<Shared>) {
    let barrier = shared
        .test_probes
        .between_lifecycle_frames
        .lock()
        .unwrap()
        .take();
    if let Some(barrier) = barrier {
        let _ = barrier.reached.send(());
        let _ = barrier.resume.await;
    }
}

#[cfg(not(test))]
async fn pause_between_lifecycle_frames(_shared: &Arc<Shared>) {}

#[cfg(test)]
fn take_document_read_hooks(shared: &Arc<Shared>) -> DocumentReadHooks {
    DocumentReadHooks {
        after_handle_read: shared
            .test_probes
            .after_document_handle_read
            .lock()
            .unwrap()
            .take(),
        after_handle_read_sequence: std::mem::take(
            &mut *shared
                .test_probes
                .after_document_handle_read_sequence
                .lock()
                .unwrap(),
        ),
        before_query_epoch: shared
            .test_probes
            .before_document_query_epoch
            .lock()
            .unwrap()
            .take(),
    }
}

#[cfg(not(test))]
fn take_document_read_hooks(_shared: &Arc<Shared>) -> DocumentReadHooks {
    DocumentReadHooks
}

#[cfg(unix)]
fn document_handle_snapshot(
    _file: &tokio::fs::File,
    metadata: &std::fs::Metadata,
) -> Option<DocumentHandleSnapshot> {
    use std::os::unix::fs::MetadataExt;

    Some(DocumentHandleSnapshot {
        identity: DocumentFileIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        },
        size_bytes: metadata.len(),
        modified: DocumentModifiedIdentity {
            modified_seconds: metadata.mtime(),
            modified_nanoseconds: metadata.mtime_nsec(),
            changed_seconds: metadata.ctime(),
            changed_nanoseconds: metadata.ctime_nsec(),
        },
    })
}

#[cfg(windows)]
fn document_handle_snapshot(
    file: &tokio::fs::File,
    _metadata: &std::fs::Metadata,
) -> Option<DocumentHandleSnapshot> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
    };

    let mut information = BY_HANDLE_FILE_INFORMATION::default();
    if unsafe { GetFileInformationByHandle(file.as_raw_handle() as _, &mut information) } == 0 {
        return None;
    }
    let file_index = ((information.nFileIndexHigh as u64) << 32) | information.nFileIndexLow as u64;
    let size_bytes = ((information.nFileSizeHigh as u64) << 32) | information.nFileSizeLow as u64;
    let filetime = |value: windows_sys::Win32::Foundation::FILETIME| {
        ((value.dwHighDateTime as u64) << 32) | value.dwLowDateTime as u64
    };
    Some(DocumentHandleSnapshot {
        identity: DocumentFileIdentity {
            volume_serial_number: information.dwVolumeSerialNumber,
            file_index,
        },
        size_bytes,
        modified: DocumentModifiedIdentity {
            creation_time: filetime(information.ftCreationTime),
            last_write_time: filetime(information.ftLastWriteTime),
        },
    })
}

#[cfg(not(any(unix, windows)))]
fn document_handle_snapshot(
    _file: &tokio::fs::File,
    metadata: &std::fs::Metadata,
) -> Option<DocumentHandleSnapshot> {
    let modified = metadata.modified().ok();
    Some(DocumentHandleSnapshot {
        identity: DocumentFileIdentity { modified },
        size_bytes: metadata.len(),
        modified: DocumentModifiedIdentity { modified },
    })
}

/// Open exactly one handle for the eligibility check and any subsequent read.
/// On Unix, O_NONBLOCK prevents a FIFO from stalling the runtime before fstat
/// can reject it; regular files (including symlinks to regular targets) retain
/// ordinary read semantics. Size is evaluated only after current-path identity
/// revalidation, so an atomically replaced old handle cannot publish a stale
/// eligibility result either.
async fn open_lsp_document(path: &std::path::Path) -> DocumentOpenObservation {
    match tokio::fs::metadata(path).await {
        Ok(metadata) if !metadata.is_file() => {
            return DocumentOpenObservation::Unsupported;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return DocumentOpenObservation::Missing;
        }
        _ => {}
    }
    let mut options = tokio::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NONBLOCK);

    let file = match options.open(path).await {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return DocumentOpenObservation::Missing;
        }
        Err(_) => {
            // Some platforms refuse to open directories/sockets as files. A
            // metadata fallback distinguishes that stable eligibility failure
            // from a missing/inaccessible path without ever reading it.
            return match tokio::fs::metadata(path).await {
                Ok(metadata) if !metadata.is_file() => DocumentOpenObservation::Unsupported,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    DocumentOpenObservation::Missing
                }
                _ => DocumentOpenObservation::TransientUnavailable,
            };
        }
    };
    let metadata = match file.metadata().await {
        Ok(metadata) => metadata,
        Err(_) => return DocumentOpenObservation::TransientUnavailable,
    };
    if !metadata.is_file() {
        return DocumentOpenObservation::Unsupported;
    }
    let Some(snapshot) = document_handle_snapshot(&file, &metadata) else {
        return DocumentOpenObservation::TransientUnavailable;
    };
    DocumentOpenObservation::Open(OpenLspDocument { file, snapshot })
}

async fn validate_lsp_document(
    path: &std::path::Path,
    hooks: &mut DocumentReadHooks,
) -> ValidatedLspDocument {
    let mut observed = open_lsp_document(path).await;
    for _ in 0..MAX_DOCUMENT_READ_ATTEMPTS {
        observed = match observed {
            DocumentOpenObservation::Open(opened) => {
                let snapshot = opened.snapshot;
                pause_after_document_handle_read(hooks).await;
                match open_lsp_document(path).await {
                    DocumentOpenObservation::Open(current) if snapshot == current.snapshot => {
                        if snapshot.size_bytes > MAX_LSP_FILE_SIZE_BYTES {
                            return ValidatedLspDocument::EligibilityError(
                                LspQueryError::FileTooLarge {
                                    size_bytes: snapshot.size_bytes,
                                    max_bytes: MAX_LSP_FILE_SIZE_BYTES,
                                },
                            );
                        }
                        return ValidatedLspDocument::Eligible;
                    }
                    current => current,
                }
            }
            DocumentOpenObservation::Missing => match open_lsp_document(path).await {
                DocumentOpenObservation::Missing => return ValidatedLspDocument::Missing,
                current => current,
            },
            DocumentOpenObservation::Unsupported => match open_lsp_document(path).await {
                DocumentOpenObservation::Unsupported => {
                    return ValidatedLspDocument::EligibilityError(
                        LspQueryError::UnsupportedFileType,
                    );
                }
                current => current,
            },
            DocumentOpenObservation::TransientUnavailable => open_lsp_document(path).await,
        };
    }
    ValidatedLspDocument::TransientUnavailable
}

async fn read_lsp_document(
    path: &std::path::Path,
    epochs: &AtomicU64,
    mut hooks: DocumentReadHooks,
) -> QueryDocumentRead {
    let mut observed = open_lsp_document(path).await;
    for _ in 0..MAX_DOCUMENT_READ_ATTEMPTS {
        let candidate = match observed {
            DocumentOpenObservation::Open(opened) => {
                let snapshot = opened.snapshot.clone();
                let outcome = read_open_lsp_document_attempt(opened, &mut hooks).await;
                let current = open_lsp_document(path).await;
                if matches!(&outcome, OpenDocumentRead::Unstable) {
                    observed = current;
                    continue;
                }
                match current {
                    DocumentOpenObservation::Open(current) if snapshot == current.snapshot => {
                        match outcome {
                            OpenDocumentRead::Content(content) => {
                                QueryReadCandidate::Content { content, snapshot }
                            }
                            OpenDocumentRead::TooLarge { size_bytes } => {
                                QueryReadCandidate::TooLarge {
                                    size_bytes,
                                    snapshot,
                                }
                            }
                            OpenDocumentRead::Unstable => unreachable!(),
                        }
                    }
                    current => {
                        observed = current;
                        continue;
                    }
                }
            }
            DocumentOpenObservation::Missing => QueryReadCandidate::Missing,
            DocumentOpenObservation::Unsupported => QueryReadCandidate::Unsupported,
            DocumentOpenObservation::TransientUnavailable => {
                observed = open_lsp_document(path).await;
                continue;
            }
        };

        // The epoch's linearization point is bracketed by equivalent path
        // observations. If replacement wins either side, this candidate is
        // discarded and the replacement is re-read under a later epoch.
        pause_before_document_query_epoch(&mut hooks).await;
        let epoch = epochs.fetch_add(1, Ordering::SeqCst).wrapping_add(1);
        let after_epoch = open_lsp_document(path).await;
        let confirmed = match (&candidate, &after_epoch) {
            (
                QueryReadCandidate::Content { snapshot, .. }
                | QueryReadCandidate::TooLarge { snapshot, .. },
                DocumentOpenObservation::Open(current),
            ) => snapshot == &current.snapshot,
            (QueryReadCandidate::Missing, DocumentOpenObservation::Missing) => true,
            (QueryReadCandidate::Unsupported, DocumentOpenObservation::Unsupported) => true,
            _ => false,
        };
        if confirmed {
            return match candidate {
                QueryReadCandidate::Content { content, .. } => {
                    QueryDocumentRead::Content { content, epoch }
                }
                QueryReadCandidate::TooLarge { size_bytes, .. } => {
                    QueryDocumentRead::EligibilityError {
                        error: LspQueryError::FileTooLarge {
                            size_bytes,
                            max_bytes: MAX_LSP_FILE_SIZE_BYTES,
                        },
                        epoch,
                    }
                }
                QueryReadCandidate::Missing => QueryDocumentRead::Missing { epoch },
                QueryReadCandidate::Unsupported => QueryDocumentRead::EligibilityError {
                    error: LspQueryError::UnsupportedFileType,
                    epoch,
                },
            };
        }
        observed = after_epoch;
    }
    QueryDocumentRead::TransientUnavailable
}

async fn read_open_lsp_document_attempt(
    mut opened: OpenLspDocument,
    hooks: &mut DocumentReadHooks,
) -> OpenDocumentRead {
    if opened.snapshot.size_bytes > MAX_LSP_FILE_SIZE_BYTES {
        return OpenDocumentRead::TooLarge {
            size_bytes: opened.snapshot.size_bytes,
        };
    }
    let mut bytes = Vec::with_capacity(opened.snapshot.size_bytes as usize);
    let mut bounded = (&mut opened.file).take(MAX_LSP_FILE_SIZE_BYTES + 1);
    if bounded.read_to_end(&mut bytes).await.is_err() {
        return OpenDocumentRead::Unstable;
    }
    pause_after_document_handle_read(hooks).await;
    let Some(after_read) = opened
        .file
        .metadata()
        .await
        .ok()
        .and_then(|metadata| document_handle_snapshot(&opened.file, &metadata))
    else {
        return OpenDocumentRead::Unstable;
    };
    if opened.snapshot != after_read {
        return OpenDocumentRead::Unstable;
    }
    if bytes.len() as u64 > MAX_LSP_FILE_SIZE_BYTES {
        return OpenDocumentRead::TooLarge {
            size_bytes: (bytes.len() as u64).max(after_read.size_bytes),
        };
    }
    OpenDocumentRead::Content(decode_text_buffer(&bytes).content)
}

/// Gated with its only user, the unix-only symlinked-document case below.
#[cfg(all(test, unix))]
async fn read_open_lsp_document(opened: OpenLspDocument) -> LspQueryResult<Option<RuntimeString>> {
    let mut hooks = DocumentReadHooks::default();
    match read_open_lsp_document_attempt(opened, &mut hooks).await {
        OpenDocumentRead::Content(content) => Ok(Some(content)),
        OpenDocumentRead::TooLarge { size_bytes } => Err(LspQueryError::FileTooLarge {
            size_bytes,
            max_bytes: MAX_LSP_FILE_SIZE_BYTES,
        }),
        OpenDocumentRead::Unstable => Ok(None),
    }
}

impl Shared {
    fn document_gate(&self, uri: &RuntimeString) -> Arc<TokioRwLock<()>> {
        let mut gates = self.document_gates.lock().unwrap();
        if let Some(gate) = gates.get(uri).and_then(Weak::upgrade) {
            return gate;
        }
        // Opportunistic pruning bounds dead weak keys without a global
        // lifecycle sweep; live guards always keep their canonical Arc up.
        gates.retain(|_, gate| gate.strong_count() > 0);
        let gate = Arc::new(TokioRwLock::new(()));
        gates.insert(uri.clone(), Arc::downgrade(&gate));
        gate
    }

    /// Mark the connection dead and fail every in-flight request so callers
    /// unblock fast (dropping the senders resolves each waiting receiver).
    fn mark_dead(self: &Arc<Self>) {
        self.dead.store(true, Ordering::SeqCst);
        self.pending.lock().unwrap().clear();
        self.waiters.lock().unwrap().clear();
        // Closing stdin makes a poisoned-but-still-running language server
        // terminate. The retained frame owner, if one already holds the lock,
        // completes or fails its one frame first; no later frame is admitted.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let shared = self.clone();
            runtime.spawn(async move {
                *shared.writer.lock().await = None;
            });
        }
    }

    /// Frame + write a JSON-RPC message through a retained owner task. Dropping
    /// the caller cannot cancel `write_all` between header/body bytes; any I/O
    /// uncertainty poisons the connection before another frame is admitted.
    async fn write_message(self: &Arc<Self>, msg: &RuntimeJson) -> bool {
        if self.dead.load(Ordering::SeqCst) {
            return false;
        }
        let mut payload = Vec::new();
        if runtime_json::write_to(msg, &mut payload).is_err() {
            self.mark_dead();
            return false;
        }
        let frame = encode_frame(&payload);
        let message = msg.clone();
        let shared = self.clone();
        let owner = tokio::spawn(async move {
            if shared.dead.load(Ordering::SeqCst) {
                return false;
            }
            let mut guard = shared.writer.lock().await;
            if shared.dead.load(Ordering::SeqCst) {
                return false;
            }
            #[cfg(test)]
            shared
                .test_probes
                .sent_messages
                .lock()
                .unwrap()
                .push(message);
            #[cfg(not(test))]
            let _ = message;

            #[cfg(test)]
            let before_complete = {
                shared
                    .test_probes
                    .before_frame_write_complete
                    .lock()
                    .unwrap()
                    .take()
            };
            #[cfg(test)]
            if let Some(barrier) = before_complete {
                let _ = barrier.reached.send(());
                let _ = barrier.resume.await;
            }

            let succeeded = match guard.as_mut() {
                Some(writer) => {
                    writer.write_all(&frame).await.is_ok() && writer.flush().await.is_ok()
                }
                #[cfg(test)]
                None => true,
                #[cfg(not(test))]
                None => false,
            };
            drop(guard);
            if !succeeded {
                shared.mark_dead();
            } else {
                #[cfg(test)]
                shared
                    .test_probes
                    .completed_frame_writes
                    .fetch_add(1, Ordering::SeqCst);
            }
            succeeded
        });
        match owner.await {
            Ok(succeeded) => succeeded,
            Err(_) => {
                self.mark_dead();
                false
            }
        }
    }
}

/// A live language-server connection.
pub struct LspClient {
    process_context: RuntimeProcessContext,
    config: LspServerConfig,
    root_dir: RuntimePath,
    request_timeout_ms: u64,
    diagnostics_settle_ms: u64,
    shared: Arc<Shared>,
    child: StdMutex<Option<SpawnedRuntimeProcess>>,
    read_task: StdMutex<Option<JoinHandle<()>>>,
}

impl LspClient {
    pub fn new(
        process_context: RuntimeProcessContext,
        config: LspServerConfig,
        root_dir: RuntimePath,
        opts: LspClientOptions,
    ) -> Self {
        Self {
            process_context,
            config,
            root_dir,
            request_timeout_ms: opts
                .request_timeout_ms
                .unwrap_or(DEFAULT_REQUEST_TIMEOUT_MS),
            diagnostics_settle_ms: opts
                .diagnostics_settle_ms
                .unwrap_or(DEFAULT_DIAGNOSTICS_SETTLE_MS),
            shared: Arc::new(Shared {
                writer: TokioMutex::new(None),
                pending: StdMutex::new(HashMap::new()),
                next_id: AtomicI64::new(0),
                next_document_query_attempt: AtomicU64::new(0),
                dead: AtomicBool::new(false),
                pull_diagnostics: AtomicBool::new(false),
                pushed: StdMutex::new(HashMap::new()),
                waiters: StdMutex::new(HashMap::new()),
                next_waiter_id: AtomicU64::new(0),
                document_sync: StdMutex::new(DocumentSyncOptions::default()),
                documents: TokioMutex::new(DocumentLifecycleState::default()),
                document_gates: StdMutex::new(HashMap::new()),
                #[cfg(test)]
                test_probes: SharedTestProbes::default(),
            }),
            child: StdMutex::new(None),
            read_task: StdMutex::new(None),
        }
    }

    pub fn is_dead(&self) -> bool {
        self.shared.dead.load(Ordering::SeqCst)
    }

    fn mark_dead(&self) {
        self.shared.mark_dead();
    }

    /// Spawn + handshake. Resolves once `initialized` is sent; marks dead on failure.
    pub async fn start(&self) {
        let mut child = match self
            .process_context
            .spawn_lsp(
                self.config.command.clone(),
                self.config.args.clone().unwrap_or_default(),
                self.root_dir.clone(),
            )
            .await
        {
            Ok(c) => c,
            Err(_) => {
                self.mark_dead();
                return;
            }
        };
        let stdin = match child.take_stdin() {
            Some(s) => s,
            None => {
                self.mark_dead();
                return;
            }
        };
        let stdout = match child.take_stdout() {
            Some(s) => s,
            None => {
                self.mark_dead();
                return;
            }
        };
        let stderr = child.take_stderr();
        *self.shared.writer.lock().await = Some(stdin);

        // Read + dispatch loop. On stdout EOF (a server crash-on-start) it marks
        // dead and drops the pending senders, so the initialize request below
        // fails FAST instead of waiting out the request timeout.
        let shared = self.shared.clone();
        let read_task = tokio::spawn(read_loop(stdout, shared));
        *self.read_task.lock().unwrap() = Some(read_task);

        // Drain stderr so a chatty server can't block on a full pipe.
        if let Some(mut stderr) = stderr {
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                loop {
                    match stderr.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                }
            });
        }
        *self.child.lock().unwrap() = Some(child);

        let root_uri = runtime_path_to_file_url(&self.root_dir);
        let init = self
            .request(
                "initialize",
                Some(exact_json!({
                    "processId": (std::process::id()),
                    "rootUri": (root_uri.clone()),
                    "workspaceFolders": [{ "uri": (root_uri.clone()), "name": "root" }],
                    "capabilities": (client_capabilities()),
                })),
            )
            .await;
        let init = match init {
            Some(v) => v,
            None => {
                self.mark_dead();
                return;
            }
        };
        self.shared
            .pull_diagnostics
            .store(detects_pull_diagnostics(&init), Ordering::SeqCst);
        *self.shared.document_sync.lock().unwrap() = document_sync_options(&init);
        self.send_notification("initialized", Some(exact_json!({})))
            .await;
    }

    /// Synchronize an edit using the same open/current path used by every
    /// file-backed query. The post-write text avoids a disk re-read.
    pub async fn notify_edited(&self, path: &RuntimePath, text: Option<&RuntimeString>) {
        let _ = self
            .ensure_document_open_current(path, text, true, DocumentSyncIntent::AuthoritativeEdit)
            .await;
    }

    /// Close an open document exactly once and clear all URI-scoped client
    /// state so recreating the same path starts a fresh didOpen lifecycle.
    pub async fn notify_deleted(&self, path: &RuntimePath) {
        if self.is_dead() {
            return;
        }
        let uri = self.document_uri(path);
        let _document_change = self.shared.document_gate(&uri).write_owned().await;
        let sync = *self.shared.document_sync.lock().unwrap();
        let mut documents = self.shared.documents.lock().await;
        let should_close = sync.open_close
            && documents
                .open
                .get(&uri)
                .is_some_and(|document| document.opened);
        let mut lifecycle_commit = LifecycleCommitGuard::new(self.shared.clone());
        if should_close {
            lifecycle_commit.arm();
        }
        documents.invalidate(&uri);
        documents.reset_query_commit(&uri);
        if should_close {
            if !self
                .send_notification(
                    "textDocument/didClose",
                    Some(exact_json!({ "textDocument": { "uri": (uri.clone()) } })),
                )
                .await
            {
                return;
            }
            pause_before_lifecycle_state_commit(&self.shared).await;
        }
        documents.open.remove(&uri);
        documents.cleanup_quiescent_uri(&uri);
        drop(documents);
        self.shared.pushed.lock().unwrap().remove(&uri);
        self.shared.waiters.lock().unwrap().remove(&uri);
        lifecycle_commit.commit();
    }

    pub async fn diagnostics(&self, path: &RuntimePath) -> LspQueryResult<Vec<LspDiagnostic>> {
        let document = match self
            .ensure_document_open_current(path, None, true, DocumentSyncIntent::Query)
            .await?
        {
            Some(document) => document,
            None => return Ok(Vec::new()),
        };
        let uri = document.uri.clone();
        if self.shared.pull_diagnostics.load(Ordering::SeqCst) {
            let report = self
                .request(
                    "textDocument/diagnostic",
                    Some(exact_json!({
                        "textDocument": { "uri": (uri.clone()) },
                    })),
                )
                .await;
            let items: Vec<RuntimeJson> = report
                .as_ref()
                .and_then(|r| r.get("items"))
                .and_then(|i| i.as_array())
                .cloned()
                .unwrap_or_default();
            return Ok(items
                .into_iter()
                .take(MAX_RESULT_ITEMS)
                .map(|d| to_diagnostic(&d))
                .collect());
        }
        Ok(self
            .shared
            .pushed
            .lock()
            .unwrap()
            .get(&uri)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .take(MAX_RESULT_ITEMS)
            .map(|diagnostic| to_diagnostic(&diagnostic))
            .collect())
    }

    pub async fn hover(
        &self,
        path: &RuntimePath,
        line: u32,
        character: u32,
    ) -> LspQueryResult<Option<LspHoverResult>> {
        let Some(document) = self
            .ensure_document_open_current(path, None, false, DocumentSyncIntent::Query)
            .await?
        else {
            return Ok(None);
        };
        let uri = document.uri.clone();
        let Some(res) = self
            .request(
                "textDocument/hover",
                Some(self.pos_params(&uri, line, character)),
            )
            .await
        else {
            return Ok(None);
        };
        Ok(Some(LspHoverResult {
            contents: hover_text(&res),
        }))
    }

    pub async fn definition(
        &self,
        path: &RuntimePath,
        line: u32,
        character: u32,
    ) -> LspQueryResult<Vec<LspLocation>> {
        let Some(document) = self
            .ensure_document_open_current(path, None, false, DocumentSyncIntent::Query)
            .await?
        else {
            return Ok(Vec::new());
        };
        let uri = document.uri.clone();
        let res = self
            .request(
                "textDocument/definition",
                Some(self.pos_params(&uri, line, character)),
            )
            .await;
        Ok(to_locations(res))
    }

    pub async fn references(
        &self,
        path: &RuntimePath,
        line: u32,
        character: u32,
    ) -> LspQueryResult<Vec<LspLocation>> {
        let Some(document) = self
            .ensure_document_open_current(path, None, false, DocumentSyncIntent::Query)
            .await?
        else {
            return Ok(Vec::new());
        };
        let uri = document.uri.clone();
        let mut params = self.pos_params(&uri, line, character);
        if let RuntimeJson::Object(object) = &mut params {
            object.insert("context", exact_json!({ "includeDeclaration": true }));
        }
        let res = self.request("textDocument/references", Some(params)).await;
        Ok(to_locations(res))
    }

    /// textDocument/implementation — same result shape as definition.
    pub async fn implementation(
        &self,
        path: &RuntimePath,
        line: u32,
        character: u32,
    ) -> LspQueryResult<Vec<LspLocation>> {
        let Some(document) = self
            .ensure_document_open_current(path, None, false, DocumentSyncIntent::Query)
            .await?
        else {
            return Ok(Vec::new());
        };
        let uri = document.uri.clone();
        let res = self
            .request(
                "textDocument/implementation",
                Some(self.pos_params(&uri, line, character)),
            )
            .await;
        Ok(to_locations(res))
    }

    pub async fn document_symbols(&self, path: &RuntimePath) -> LspQueryResult<Vec<LspSymbol>> {
        let Some(document) = self
            .ensure_document_open_current(path, None, false, DocumentSyncIntent::Query)
            .await?
        else {
            return Ok(Vec::new());
        };
        let uri = document.uri.clone();
        let res = self
            .request(
                "textDocument/documentSymbol",
                Some(exact_json!({
                    "textDocument": { "uri": (uri) },
                })),
            )
            .await;
        let arr = res
            .as_ref()
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        Ok(to_symbols(&arr))
    }

    /// workspace/symbol — query by name (empty = all); positions are irrelevant here.
    pub async fn workspace_symbol(&self, query: &RuntimeString) -> Vec<LspWorkspaceSymbol> {
        let res = self
            .request(
                "workspace/symbol",
                Some(exact_json!({ "query": (query.clone()) })),
            )
            .await;
        let arr = res
            .as_ref()
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        to_workspace_symbols(&arr)
    }

    /// textDocument/prepareCallHierarchy — resolve the call-hierarchy node(s) at a position.
    pub async fn prepare_call_hierarchy(
        &self,
        path: &RuntimePath,
        line: u32,
        character: u32,
    ) -> LspQueryResult<Vec<LspCallHierarchyItem>> {
        let Some(prepared) = self
            .prepare_call_hierarchy_raw(path, line, character)
            .await?
        else {
            return Ok(Vec::new());
        };
        Ok(prepared
            .items
            .into_iter()
            .take(MAX_RESULT_ITEMS)
            .map(|i| to_call_hierarchy_item(&i))
            .collect())
    }

    pub async fn incoming_calls(
        &self,
        path: &RuntimePath,
        line: u32,
        character: u32,
    ) -> LspQueryResult<Vec<LspCall>> {
        self.calls("callHierarchy/incomingCalls", path, line, character)
            .await
    }

    pub async fn outgoing_calls(
        &self,
        path: &RuntimePath,
        line: u32,
        character: u32,
    ) -> LspQueryResult<Vec<LspCall>> {
        self.calls("callHierarchy/outgoingCalls", path, line, character)
            .await
    }

    /// Raw prepareCallHierarchy items — kept un-simplified so step 2 can echo the FULL `{item}`.
    async fn prepare_call_hierarchy_raw(
        &self,
        path: &RuntimePath,
        line: u32,
        character: u32,
    ) -> LspQueryResult<Option<PreparedCallHierarchy>> {
        let Some(document) = self
            .ensure_document_open_current(path, None, false, DocumentSyncIntent::Query)
            .await?
        else {
            return Ok(None);
        };
        let uri = document.uri.clone();
        let res = self
            .request(
                "textDocument/prepareCallHierarchy",
                Some(self.pos_params(&uri, line, character)),
            )
            .await;
        Ok(Some(PreparedCallHierarchy {
            items: res
                .as_ref()
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default(),
            _document: document,
        }))
    }

    /// Shared two-step incoming/outgoing: prepare → (items[0]) → calls; empty prepare short-circuits.
    async fn calls(
        &self,
        method: &str,
        path: &RuntimePath,
        line: u32,
        character: u32,
    ) -> LspQueryResult<Vec<LspCall>> {
        let Some(prepared) = self
            .prepare_call_hierarchy_raw(path, line, character)
            .await?
        else {
            return Ok(Vec::new());
        };
        if prepared.items.is_empty() {
            return Ok(Vec::new());
        }
        // Pass ONLY the first prepared item as the {item} param.
        let res = self
            .request(
                method,
                Some(exact_json!({ "item": (prepared.items[0].clone()) })),
            )
            .await;
        let arr = res
            .as_ref()
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let mut out: Vec<LspCall> = Vec::new();
        for c in &arr {
            // Incoming edges carry `from`, outgoing edges carry `to`.
            let node = if c.get("from").is_some() {
                c.get("from")
            } else {
                c.get("to")
            };
            let node = match node {
                Some(n) => n,
                None => continue,
            };
            // Skip an edge with no resolvable end.
            if node.get("uri").is_none_or(|u| u.is_null()) {
                continue;
            }
            let from_ranges = c
                .get("fromRanges")
                .and_then(|f| f.as_array())
                .cloned()
                .unwrap_or_default();
            let positions: Vec<LspPosition> = from_ranges
                .iter()
                .filter_map(|r| {
                    let start = r.get("start")?;
                    Some(LspPosition {
                        line: num_at(Some(start), "line"),
                        character: num_at(Some(start), "character"),
                    })
                })
                .collect();
            out.push(LspCall {
                item: to_call_hierarchy_item(node),
                from_ranges: positions,
            });
        }
        Ok(out.into_iter().take(MAX_RESULT_ITEMS).collect())
    }

    /// Graceful shutdown then force-kill the process tree.
    pub async fn dispose(&self) {
        let child = self.child.lock().unwrap().take();
        let mut child = match child {
            Some(c) => c,
            None => return,
        };
        if !self.is_dead() {
            let _ = self.request("shutdown", None).await;
            // Flush the exit notification before the kill destroys the stream.
            self.send_notification("exit", None).await;
        }
        // The connection is done — closing stdin releases the writer.
        *self.shared.writer.lock().await = None;
        child.controller().signal(RuntimeSignal::Kill);
        let _ = child.wait().await;
        if let Some(h) = self.read_task.lock().unwrap().take() {
            h.abort();
        }
        self.mark_dead();
    }

    // --- internals ---

    fn canonical_document_path(&self, path: &RuntimePath) -> RuntimePath {
        resolve_runtime(&self.root_dir, std::slice::from_ref(path)).unwrap_or_else(|_| path.clone())
    }

    fn document_uri(&self, path: &RuntimePath) -> RuntimeString {
        runtime_path_to_file_url(&self.canonical_document_path(path))
    }

    fn pos_params(&self, uri: &RuntimeString, line: u32, character: u32) -> RuntimeJson {
        exact_json!({
            "textDocument": { "uri": (uri.clone()) },
            "position": { "line": (line), "character": (character) },
        })
    }

    /// Ensure a file-backed query observes one authoritative document state.
    ///
    /// The lifecycle mutex deliberately remains held while notifications are
    /// written: a concurrent query/delete therefore cannot reorder didOpen,
    /// didChange, or didClose on the same language-server connection.
    async fn ensure_document_open_current(
        &self,
        path: &RuntimePath,
        text: Option<&RuntimeString>,
        settle_push_diagnostics: bool,
        intent: DocumentSyncIntent,
    ) -> LspQueryResult<Option<CurrentDocument>> {
        if self.is_dead() {
            return Ok(None);
        }

        let canonical_path = self.canonical_document_path(path);
        let uri = runtime_path_to_file_url(&canonical_path);
        let document_gate = self.shared.document_gate(&uri);
        // Real mutation paths also use the manager's canonical write lease;
        // this client-local URI lease makes direct lifecycle calls safe and
        // prevents a query request from observing didClose/didChange midway.
        let authoritative_change = if intent == DocumentSyncIntent::AuthoritativeEdit {
            Some(document_gate.clone().write_owned().await)
        } else {
            None
        };
        let observed_generation = {
            let mut documents = self.shared.documents.lock().await;
            if intent == DocumentSyncIntent::AuthoritativeEdit {
                // A post-write synchronization is authoritative over any
                // query that read the pre-write or pre-create disk state.
                documents.invalidate(&uri);
                documents.reset_query_commit(&uri);
            }
            if intent == DocumentSyncIntent::Query {
                documents.begin_query(&uri);
            }
            documents.invalidation_generation(&uri)
        };
        let mut query_registration =
            (intent == DocumentSyncIntent::Query).then(|| QueryRegistration {
                shared: self.shared.clone(),
                uri: uri.clone(),
                active: true,
            });
        let mut query_attempt = None;
        let mut transient_unavailable = false;
        let content: LspQueryResult<Option<RuntimeString>> =
            match project_runtime_path(&canonical_path, "path") {
                Err(_) => {
                    transient_unavailable = true;
                    Ok(None)
                }
                Ok(native) => match text {
                    Some(text) => {
                        // All current provided-text callers are authoritative
                        // edit notifications. A future Query caller must use a
                        // bracketed epoch just like the disk-read path.
                        if intent == DocumentSyncIntent::Query {
                            transient_unavailable = true;
                            Ok(None)
                        } else {
                            let mut hooks = take_document_read_hooks(&self.shared);
                            match validate_lsp_document(&native, &mut hooks).await {
                                ValidatedLspDocument::Eligible => Ok(Some(text.clone())),
                                ValidatedLspDocument::Missing => Ok(None),
                                ValidatedLspDocument::EligibilityError(error) => Err(error),
                                ValidatedLspDocument::TransientUnavailable => {
                                    transient_unavailable = true;
                                    Ok(None)
                                }
                            }
                        }
                    }
                    None => {
                        let hooks = take_document_read_hooks(&self.shared);
                        match read_lsp_document(
                            &native,
                            &self.shared.next_document_query_attempt,
                            hooks,
                        )
                        .await
                        {
                            QueryDocumentRead::Content { content, epoch } => {
                                if intent == DocumentSyncIntent::Query {
                                    query_attempt = Some(epoch);
                                }
                                Ok(Some(content))
                            }
                            QueryDocumentRead::Missing { epoch } => {
                                if intent == DocumentSyncIntent::Query {
                                    query_attempt = Some(epoch);
                                }
                                Ok(None)
                            }
                            QueryDocumentRead::EligibilityError { error, epoch } => {
                                if intent == DocumentSyncIntent::Query {
                                    query_attempt = Some(epoch);
                                }
                                Err(error)
                            }
                            QueryDocumentRead::TransientUnavailable => {
                                transient_unavailable = true;
                                Ok(None)
                            }
                        }
                    }
                },
            };

        if transient_unavailable {
            if let Some(query_registration) = &mut query_registration {
                let mut documents = self.shared.documents.lock().await;
                query_registration.finish(&mut documents);
            }
            return Ok(None);
        }

        #[cfg(test)]
        let after_document_read = {
            self.shared
                .test_probes
                .after_document_read
                .lock()
                .unwrap()
                .take()
        };
        #[cfg(test)]
        if let Some(barrier) = after_document_read {
            let _ = barrier.reached.send(());
            let _ = barrier.resume.await;
        }

        let sync = *self.shared.document_sync.lock().unwrap();
        let should_open = sync.open_close;
        let mut sent_sync_notification = false;

        // Fast path: a current identical document (or a newer committed Query)
        // only needs a shared request lease. This preserves true same-URI,
        // same-content query concurrency; only a state-changing commit takes
        // the URI's exclusive lease.
        if let Some(attempt) = query_attempt {
            let request_guard = document_gate.clone().read_owned().await;
            let mut documents = self.shared.documents.lock().await;
            if documents.invalidation_generation(&uri) != observed_generation {
                query_registration
                    .as_mut()
                    .expect("Query intent must register its lifecycle")
                    .finish(&mut documents);
                return Ok(None);
            }
            if documents.latest_query_commit(&uri) > attempt {
                let current = documents.open.contains_key(&uri);
                query_registration
                    .as_mut()
                    .expect("Query intent must register its lifecycle")
                    .finish(&mut documents);
                drop(documents);
                return Ok(current.then_some(CurrentDocument {
                    uri,
                    _request_guard: Some(request_guard),
                }));
            }
            let same_current = matches!(
                (&content, documents.open.get(&uri)),
                (Ok(Some(content)), Some(document)) if document.content == *content
            );
            if same_current {
                documents.commit_query(&uri, attempt);
                query_registration
                    .as_mut()
                    .expect("Query intent must register its lifecycle")
                    .finish(&mut documents);
                drop(documents);
                return Ok(Some(CurrentDocument {
                    uri,
                    _request_guard: Some(request_guard),
                }));
            }
            drop(documents);
            drop(request_guard);
        }

        let query_change = if query_attempt.is_some() {
            Some(document_gate.write_owned().await)
        } else {
            None
        };
        let mut documents = self.shared.documents.lock().await;
        if documents.invalidation_generation(&uri) != observed_generation {
            if let Some(query_registration) = &mut query_registration {
                query_registration.finish(&mut documents);
            }
            return Ok(None);
        }

        if let Some(attempt) = query_attempt {
            if documents.latest_query_commit(&uri) > attempt {
                // A newer Query already committed. An older identical read can
                // safely share it; an older different/missing/error read must
                // not overwrite or close it. In both cases, only an existing
                // current document is eligible for the old request.
                let current = documents.open.contains_key(&uri);
                query_registration
                    .as_mut()
                    .expect("Query intent must register its lifecycle")
                    .finish(&mut documents);
                drop(documents);
                let request_guard = query_change.map(|guard| guard.downgrade());
                return Ok(current.then_some(CurrentDocument {
                    uri,
                    _request_guard: request_guard,
                }));
            }
            documents.commit_query(&uri, attempt);
        } else {
            // Close the window opened by the first authoritative invalidation:
            // Queries that began while this edit was synchronizing must not
            // overwrite its post-write state after it commits.
            documents.invalidate(&uri);
            documents.reset_query_commit(&uri);
        }

        let content = match content {
            Ok(Some(content)) => content,
            Ok(None) => {
                let should_close = sync.open_close
                    && documents
                        .open
                        .get(&uri)
                        .is_some_and(|document| document.opened);
                let mut lifecycle_commit = LifecycleCommitGuard::new(self.shared.clone());
                if should_close {
                    lifecycle_commit.arm();
                    if !self
                        .send_notification(
                            "textDocument/didClose",
                            Some(exact_json!({ "textDocument": { "uri": (uri.clone()) } })),
                        )
                        .await
                    {
                        return Ok(None);
                    }
                    pause_before_lifecycle_state_commit(&self.shared).await;
                }
                documents.open.remove(&uri);
                if let Some(query_registration) = &mut query_registration {
                    query_registration.finish(&mut documents);
                } else {
                    documents.cleanup_quiescent_uri(&uri);
                }
                drop(documents);
                self.shared.pushed.lock().unwrap().remove(&uri);
                self.shared.waiters.lock().unwrap().remove(&uri);
                lifecycle_commit.commit();
                return Ok(None);
            }
            Err(error) => {
                let should_close = sync.open_close
                    && documents
                        .open
                        .get(&uri)
                        .is_some_and(|document| document.opened);
                let mut lifecycle_commit = LifecycleCommitGuard::new(self.shared.clone());
                if should_close {
                    lifecycle_commit.arm();
                    if !self
                        .send_notification(
                            "textDocument/didClose",
                            Some(exact_json!({ "textDocument": { "uri": (uri.clone()) } })),
                        )
                        .await
                    {
                        return Err(error);
                    }
                    pause_before_lifecycle_state_commit(&self.shared).await;
                }
                documents.open.remove(&uri);
                if let Some(query_registration) = &mut query_registration {
                    query_registration.finish(&mut documents);
                } else {
                    documents.cleanup_quiescent_uri(&uri);
                }
                drop(documents);
                self.shared.pushed.lock().unwrap().remove(&uri);
                self.shared.waiters.lock().unwrap().remove(&uri);
                lifecycle_commit.commit();
                return Err(error);
            }
        };
        let mut lifecycle_commit = LifecycleCommitGuard::new(self.shared.clone());
        match documents.open.get_mut(&uri) {
            None => {
                if should_open {
                    lifecycle_commit.arm();
                    if !self
                        .send_notification(
                            "textDocument/didOpen",
                            Some(exact_json!({
                                "textDocument": {
                                    "uri": (uri.clone()),
                                    "languageId": (self.config.language.clone()),
                                    "version": 1_i64,
                                    "text": (content.clone()),
                                }
                            })),
                        )
                        .await
                    {
                        return Ok(None);
                    }
                    sent_sync_notification = true;
                    pause_before_lifecycle_state_commit(&self.shared).await;
                }
                documents.open.insert(
                    uri.clone(),
                    OpenDocument {
                        version: 1,
                        content,
                        opened: should_open,
                    },
                );
            }
            Some(document) if document.content != content => {
                let next_version = document.version.saturating_add(1);
                let opened_now = !document.opened && should_open;
                if opened_now {
                    lifecycle_commit.arm();
                    if !self
                        .send_notification(
                            "textDocument/didOpen",
                            Some(exact_json!({
                                "textDocument": {
                                    "uri": (uri.clone()),
                                    "languageId": (self.config.language.clone()),
                                    "version": (next_version),
                                    "text": (content.clone()),
                                }
                            })),
                        )
                        .await
                    {
                        return Ok(None);
                    }
                    sent_sync_notification = true;
                }
                if !opened_now {
                    match sync.change {
                        DocumentChangeSyncKind::None if sync.open_close => {
                            lifecycle_commit.arm();
                            if !self
                                .send_notification(
                                    "textDocument/didClose",
                                    Some(exact_json!({
                                        "textDocument": { "uri": (uri.clone()) }
                                    })),
                                )
                                .await
                            {
                                return Ok(None);
                            }
                            pause_between_lifecycle_frames(&self.shared).await;
                            if !self
                                .send_notification(
                                    "textDocument/didOpen",
                                    Some(exact_json!({
                                        "textDocument": {
                                            "uri": (uri.clone()),
                                            "languageId": (self.config.language.clone()),
                                            "version": (next_version),
                                            "text": (content.clone()),
                                        }
                                    })),
                                )
                                .await
                            {
                                return Ok(None);
                            }
                            sent_sync_notification = true;
                        }
                        DocumentChangeSyncKind::Full => {
                            lifecycle_commit.arm();
                            if !self
                                .send_notification(
                                    "textDocument/didChange",
                                    Some(exact_json!({
                                        "textDocument": {
                                            "uri": (uri.clone()),
                                            "version": (next_version)
                                        },
                                        "contentChanges": [{ "text": (content.clone()) }],
                                    })),
                                )
                                .await
                            {
                                return Ok(None);
                            }
                            sent_sync_notification = true;
                        }
                        DocumentChangeSyncKind::Incremental => {
                            let (end_line, end_character) = document_end(&document.content);
                            lifecycle_commit.arm();
                            if !self
                                .send_notification(
                                    "textDocument/didChange",
                                    Some(exact_json!({
                                        "textDocument": {
                                            "uri": (uri.clone()),
                                            "version": (next_version)
                                        },
                                        "contentChanges": [{
                                            "range": {
                                                "start": { "line": 0_u32, "character": 0_u32 },
                                                "end": {
                                                    "line": (end_line),
                                                    "character": (end_character)
                                                }
                                            },
                                            "text": (content.clone())
                                        }],
                                    })),
                                )
                                .await
                            {
                                return Ok(None);
                            }
                            sent_sync_notification = true;
                        }
                        DocumentChangeSyncKind::None => {}
                    }
                }
                if lifecycle_commit.is_armed() {
                    pause_before_lifecycle_state_commit(&self.shared).await;
                }
                if opened_now {
                    document.opened = true;
                }
                document.version = next_version;
                document.content = content;
            }
            Some(_) => {}
        }
        lifecycle_commit.commit();

        if settle_push_diagnostics
            && !self.shared.pull_diagnostics.load(Ordering::SeqCst)
            && sent_sync_notification
        {
            self.await_push(&uri).await;
        }
        if let Some(query_registration) = &mut query_registration {
            query_registration.finish(&mut documents);
        }
        drop(documents);
        let request_guard = query_change.map(|guard| guard.downgrade());
        drop(authoritative_change);
        Ok(Some(CurrentDocument {
            uri,
            _request_guard: request_guard,
        }))
    }

    /// Send a request with a per-attempt timeout; returns `None` on timeout /
    /// error / dead server. A -32801 ContentModified rejection is retried
    /// (exponential backoff) up to the cap; a resolved JSON `null` is treated as
    /// no result, matching the callers' nullish checks.
    async fn request(&self, method: &str, params: Option<RuntimeJson>) -> Option<RuntimeJson> {
        if self.is_dead() {
            return None;
        }
        let timeout = Duration::from_millis(self.request_timeout_ms);
        let mut attempt: u32 = 0;
        loop {
            let id = self.shared.next_id.fetch_add(1, Ordering::SeqCst);
            let (tx, rx) = oneshot::channel::<Result<RuntimeJson, ResponseError>>();
            self.shared.pending.lock().unwrap().insert(id, tx);
            let pending_registration = PendingRequestRegistration {
                shared: self.shared.clone(),
                id,
            };

            let mut obj = RuntimeObject::new();
            obj.insert("jsonrpc", RuntimeJson::String("2.0".into()));
            obj.insert("id", RuntimeJson::Number(RuntimeNumber::from(id)));
            obj.insert("method", RuntimeJson::String(method.into()));
            if let Some(p) = &params {
                obj.insert("params", p.clone());
            }
            if !self.shared.write_message(&RuntimeJson::Object(obj)).await {
                return None;
            }

            #[cfg(test)]
            let after_request_sent = {
                self.shared
                    .test_probes
                    .after_request_sent
                    .lock()
                    .unwrap()
                    .take()
            };
            #[cfg(test)]
            if let Some(barrier) = after_request_sent {
                let _ = barrier.reached.send(());
                let _ = barrier.resume.await;
            }

            // If the server died between the insert and here, the read loop may
            // never drain this id again — bail immediately.
            if self.is_dead() {
                return None;
            }

            let outcome = tokio::select! {
                r = rx => match r {
                    Ok(Ok(v)) => Outcome::Success(v),
                    Ok(Err(e)) => Outcome::ResponseError(e),
                    Err(_) => Outcome::Closed,
                },
                _ = tokio::time::sleep(timeout) => Outcome::Timeout,
            };
            drop(pending_registration);

            match outcome {
                Outcome::Timeout | Outcome::Closed => return None,
                Outcome::ResponseError(e) => {
                    if e.code == LSP_ERROR_CONTENT_MODIFIED
                        && attempt < MAX_CONTENT_MODIFIED_RETRIES
                    {
                        tokio::time::sleep(Duration::from_millis(
                            RETRY_BASE_DELAY_MS * (1u64 << attempt),
                        ))
                        .await;
                        attempt += 1;
                        continue;
                    }
                    return None;
                }
                Outcome::Success(v) => {
                    if v.is_null() {
                        return None;
                    }
                    return Some(v);
                }
            }
        }
    }

    async fn send_notification(&self, method: &str, params: Option<RuntimeJson>) -> bool {
        let mut obj = RuntimeObject::new();
        obj.insert("jsonrpc", RuntimeJson::String("2.0".into()));
        obj.insert("method", RuntimeJson::String(method.into()));
        if let Some(p) = params {
            obj.insert("params", p);
        }
        self.shared.write_message(&RuntimeJson::Object(obj)).await
    }

    /// Wait (capped) for the next publishDiagnostics for `uri`, or the settle timeout.
    async fn await_push(&self, uri: &RuntimeString) {
        let id = self.shared.next_waiter_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel::<()>();
        self.shared
            .waiters
            .lock()
            .unwrap()
            .entry(uri.clone())
            .or_default()
            .insert(id, tx);
        let _registration = DiagnosticsWaiterRegistration {
            shared: self.shared.clone(),
            uri: uri.clone(),
            id,
        };
        let _ = tokio::time::timeout(Duration::from_millis(self.diagnostics_settle_ms), rx).await;
    }
}

// --- read loop + dispatch ---

async fn read_loop(mut stdout: RuntimeChildStdout, shared: Arc<Shared>) {
    let mut dec = FrameDecoder::new();
    let mut chunk = vec![0u8; 65536];
    loop {
        match stdout.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                dec.push(&chunk[..n]);
                while let Some(payload) = dec.next_message() {
                    handle_message(&shared, payload).await;
                }
            }
        }
    }
    shared.mark_dead();
}

async fn handle_message(shared: &Arc<Shared>, payload: Vec<u8>) {
    let source = match std::str::from_utf8(&payload) {
        Ok(value) => RuntimeString::from(value),
        Err(_) => return,
    };
    let msg = match runtime_json::parse(&source) {
        Ok(v) => v,
        Err(_) => return,
    };
    if let Some(method) = msg.get("method").and_then(RuntimeJson::as_str) {
        if let Some(id) = msg.get("id") {
            // Server→client request.
            handle_server_request(shared, method, id.clone(), msg.get("params")).await;
        } else {
            // Notification.
            handle_notification(shared, method, msg.get("params"));
        }
    } else if let Some(id) = msg.get("id").and_then(|i| i.as_i64()) {
        // Response to one of our requests.
        let tx = shared.pending.lock().unwrap().remove(&id);
        if let Some(tx) = tx {
            if let Some(err) = msg.get("error") {
                let code = err.get("code").and_then(|c| c.as_i64()).unwrap_or(0);
                let _ = tx.send(Err(ResponseError { code }));
            } else {
                let result = msg.get("result").cloned().unwrap_or(RuntimeJson::Null);
                let _ = tx.send(Ok(result));
            }
        }
    }
}

/// Answer the common server→client requests benignly so the server doesn't stall;
/// any other request gets a MethodNotFound reply.
async fn handle_server_request(
    shared: &Arc<Shared>,
    method: &RuntimeString,
    id: RuntimeJson,
    params: Option<&RuntimeJson>,
) {
    let response = match server_request_result(method, params) {
        Ok(value) => exact_json!({
            "jsonrpc": "2.0",
            "id": (id),
            "result": (value),
        }),
        Err(()) => exact_json!({
            "jsonrpc": "2.0",
            "id": (id),
            "error": { "code": (METHOD_NOT_FOUND), "message": "Method not found" },
        }),
    };
    shared.write_message(&response).await;
}

fn server_request_result(
    method: &RuntimeString,
    params: Option<&RuntimeJson>,
) -> Result<RuntimeJson, ()> {
    if method == "workspace/configuration" {
        let n = params
            .and_then(|p| p.get("items"))
            .and_then(|i| i.as_array())
            .map(|a| a.len())
            .unwrap_or(0);
        Ok(RuntimeJson::Array(
            std::iter::repeat_with(|| RuntimeJson::Null)
                .take(n)
                .collect(),
        ))
    } else if method == "client/registerCapability"
        || method == "client/unregisterCapability"
        || method == "window/workDoneProgress/create"
    {
        Ok(RuntimeJson::Null)
    } else {
        Err(())
    }
}

fn handle_notification(shared: &Arc<Shared>, method: &RuntimeString, params: Option<&RuntimeJson>) {
    if method != "textDocument/publishDiagnostics" {
        return;
    }
    let params = match params {
        Some(p) => p,
        None => return,
    };
    let uri = match params.get("uri").and_then(RuntimeJson::as_str) {
        Some(uri) => uri.clone(),
        None => return,
    };
    let diags = params
        .get("diagnostics")
        .and_then(|d| d.as_array())
        .cloned()
        .unwrap_or_default();
    shared.pushed.lock().unwrap().insert(uri.clone(), diags);
    let waiters = shared.waiters.lock().unwrap().remove(&uri);
    if let Some(ws) = waiters {
        for w in ws.into_values() {
            let _ = w.send(());
        }
    }
}

fn detects_pull_diagnostics(init: &RuntimeJson) -> bool {
    init.get("capabilities")
        .and_then(|c| c.get("diagnosticProvider"))
        .map(|v| !v.is_null())
        .unwrap_or(false)
}

/// Normalize the two server capability shapes from the LSP specification.
/// Numeric `textDocumentSync` is the legacy shorthand used by tsserver-style
/// implementations; a non-zero shorthand implies the open/change lifecycle
/// expected by those servers. The structured form is followed field-for-field.
fn document_sync_options(init: &RuntimeJson) -> DocumentSyncOptions {
    let Some(value) = init
        .get("capabilities")
        .and_then(|capabilities| capabilities.get("textDocumentSync"))
    else {
        // Preserve the long-standing full-sync behavior for servers that omit
        // the optional capability entirely.
        return DocumentSyncOptions::default();
    };

    if let Some(kind) = value.as_u64() {
        return match kind {
            0 => DocumentSyncOptions {
                open_close: false,
                change: DocumentChangeSyncKind::None,
            },
            2 => DocumentSyncOptions {
                open_close: true,
                change: DocumentChangeSyncKind::Incremental,
            },
            _ => DocumentSyncOptions::default(),
        };
    }

    let Some(object) = value.as_object() else {
        return DocumentSyncOptions::default();
    };
    let open_close = object
        .get("openClose")
        .and_then(RuntimeJson::as_bool)
        .unwrap_or(false);
    let change = match object.get("change").and_then(RuntimeJson::as_u64) {
        Some(1) => DocumentChangeSyncKind::Full,
        Some(2) => DocumentChangeSyncKind::Incremental,
        _ => DocumentChangeSyncKind::None,
    };
    DocumentSyncOptions { open_close, change }
}

/// LSP positions count UTF-16 code units, which is already RuntimeString's
/// native representation. This produces the exclusive end position needed to
/// replace a whole previous document under incremental synchronization.
fn document_end(content: &RuntimeString) -> (u32, u32) {
    let mut line = 0_u32;
    let mut character = 0_u32;
    for unit in content.as_utf16() {
        if *unit == u16::from(b'\n') {
            line = line.saturating_add(1);
            character = 0;
        } else {
            character = character.saturating_add(1);
        }
    }
    (line, character)
}

// --- message framing ---

/// Prepend the `Content-Length` header + blank line to a JSON payload.
fn encode_frame(payload: &[u8]) -> Vec<u8> {
    let header = format!("Content-Length: {}\r\n\r\n", payload.len());
    let mut out = Vec::with_capacity(header.len() + payload.len());
    out.extend_from_slice(header.as_bytes());
    out.extend_from_slice(payload);
    out
}

/// Incremental `Content-Length` frame reader: bytes in, complete payloads out.
struct FrameDecoder {
    buf: Vec<u8>,
}

impl FrameDecoder {
    fn new() -> Self {
        Self { buf: Vec::new() }
    }

    fn push(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    /// Extract the next complete message payload, or `None` if not yet whole.
    fn next_message(&mut self) -> Option<Vec<u8>> {
        loop {
            let header_end = find_subslice(&self.buf, b"\r\n\r\n")?;
            match parse_content_length(&self.buf[..header_end]) {
                Some(len) => {
                    let body_start = header_end + 4;
                    if self.buf.len() < body_start + len {
                        return None;
                    }
                    let payload = self.buf[body_start..body_start + len].to_vec();
                    self.buf.drain(..body_start + len);
                    return Some(payload);
                }
                // A header block with no Content-Length: drop it so a malformed
                // frame can't wedge the reader, then look for the next block.
                None => {
                    self.buf.drain(..header_end + 4);
                }
            }
        }
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn parse_content_length(header: &[u8]) -> Option<usize> {
    let text = std::str::from_utf8(header).ok()?;
    for line in text.split("\r\n") {
        let mut parts = line.splitn(2, ':');
        let key = parts.next()?.trim();
        if key.eq_ignore_ascii_case("content-length") {
            let val = parts.next()?.trim();
            return val.parse::<usize>().ok();
        }
    }
    None
}

// --- client capabilities ---

/// Client capabilities advertised at initialize — the engine-relevant subset.
fn client_capabilities() -> RuntimeJson {
    exact_json!({
        "textDocument": {
            "synchronization": { "didSave": false, "dynamicRegistration": false },
            "hover": { "contentFormat": ["plaintext", "markdown"] },
            "definition": { "linkSupport": true },
            "references": (RuntimeJson::Object(RuntimeObject::new())),
            "implementation": { "linkSupport": true },
            "documentSymbol": { "hierarchicalDocumentSymbolSupport": true },
            "callHierarchy": { "dynamicRegistration": false },
            "publishDiagnostics": (RuntimeJson::Object(RuntimeObject::new())),
            "diagnostic": { "dynamicRegistration": false }
        },
        "workspace": {
            "configuration": true,
            "workspaceFolders": true,
            "symbol": (RuntimeJson::Object(RuntimeObject::new())),
        }
    })
}

// --- response → engine-shape conversions ---

fn num_at(obj: Option<&RuntimeJson>, key: &str) -> u32 {
    obj.and_then(|o| o.get(key))
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as u32
}

/// First non-null value among `keys` (the `??` chain the raw LSP shapes use).
fn coalesce<'a>(v: &'a RuntimeJson, keys: &[&str]) -> Option<&'a RuntimeJson> {
    for k in keys {
        if let Some(x) = v.get(*k) {
            if !x.is_null() {
                return Some(x);
            }
        }
    }
    None
}

fn severity_from(n: u64) -> LspSeverity {
    match n {
        1 => LspSeverity::Error,
        2 => LspSeverity::Warning,
        3 => LspSeverity::Information,
        4 => LspSeverity::Hint,
        _ => LspSeverity::Error,
    }
}

fn to_diagnostic(d: &RuntimeJson) -> LspDiagnostic {
    // `message` may be a plain string or a MarkupContent `{ value }`.
    let message = match d.get("message") {
        Some(RuntimeJson::String(s)) => s.clone(),
        Some(m) => m
            .get("value")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .unwrap_or_default(),
        None => RuntimeString::default(),
    };
    let range = d.get("range");
    let start = range.and_then(|r| r.get("start"));
    let end = range.and_then(|r| r.get("end"));
    let severity = severity_from(d.get("severity").and_then(|v| v.as_u64()).unwrap_or(1));
    let source = d.get("source").and_then(RuntimeJson::as_str).cloned();
    let code = match d.get("code") {
        None | Some(RuntimeJson::Null) => None,
        Some(RuntimeJson::String(value)) => Some(value.clone()),
        Some(RuntimeJson::Number(value)) => Some(RuntimeString::from(value.to_json_token())),
        Some(other) => Some(runtime_json::stringify(other)),
    };
    LspDiagnostic {
        line: num_at(start, "line"),
        character: num_at(start, "character"),
        end_line: Some(num_at(end, "line")),
        end_character: Some(num_at(end, "character")),
        severity,
        message,
        source,
        code,
    }
}

fn hover_text(h: &RuntimeJson) -> RuntimeString {
    match h.get("contents") {
        Some(RuntimeJson::String(value)) => value.clone(),
        Some(RuntimeJson::Array(values)) => join_runtime_strings(
            &values
                .iter()
                .map(|value| match value {
                    RuntimeJson::String(value) => value.clone(),
                    _ => value
                        .get("value")
                        .and_then(RuntimeJson::as_str)
                        .cloned()
                        .unwrap_or_default(),
                })
                .collect::<Vec<_>>(),
            "\n",
        ),
        Some(value @ RuntimeJson::Object(_)) => value
            .get("value")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .unwrap_or_default(),
        _ => RuntimeString::default(),
    }
}

fn to_locations(res: Option<RuntimeJson>) -> Vec<LspLocation> {
    let res = match res {
        Some(v) => v,
        None => return Vec::new(),
    };
    let arr: Vec<RuntimeJson> = match res.into_array() {
        Ok(array) => array,
        Err(other) => vec![other],
    };
    let mut out: Vec<LspLocation> = Vec::new();
    for item in &arr {
        let uri = coalesce(item, &["uri", "targetUri"]);
        let range = coalesce(item, &["range", "targetSelectionRange", "targetRange"]);
        let (uri, range) = match (uri, range) {
            (Some(u), Some(r)) => (u, r),
            _ => continue,
        };
        let uri = match uri.as_str() {
            Some(uri) => uri,
            None => continue,
        };
        let start = range.get("start");
        let end = range.get("end");
        out.push(LspLocation {
            path: file_url_to_path_safe(uri),
            line: num_at(start, "line"),
            character: num_at(start, "character"),
            end_line: Some(num_at(end, "line")),
            end_character: Some(num_at(end, "character")),
        });
    }
    out.into_iter().take(MAX_RESULT_ITEMS).collect()
}

fn to_symbols(syms: &[RuntimeJson]) -> Vec<LspSymbol> {
    let mut out: Vec<LspSymbol> = Vec::new();
    for s in syms {
        walk_symbol(s, &mut out);
    }
    out.into_iter().take(MAX_RESULT_ITEMS).collect()
}

fn walk_symbol(s: &RuntimeJson, out: &mut Vec<LspSymbol>) {
    // DocumentSymbol carries selectionRange; SymbolInformation carries location.range.
    let range = if s.get("selectionRange").is_some() {
        s.get("selectionRange")
    } else {
        s.get("location").and_then(|l| l.get("range"))
    };
    let start = range.and_then(|r| r.get("start"));
    let detail = match s.get("detail").and_then(RuntimeJson::as_str) {
        Some(detail) if !detail.is_empty() => Some(detail.clone()),
        _ => None,
    };
    out.push(LspSymbol {
        name: s
            .get("name")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .unwrap_or_default(),
        kind: symbol_kind_str(s.get("kind")),
        line: num_at(start, "line"),
        character: num_at(start, "character"),
        detail,
    });
    if let Some(children) = s.get("children").and_then(|c| c.as_array()) {
        for c in children {
            walk_symbol(c, out);
        }
    }
}

fn to_workspace_symbols(syms: &[RuntimeJson]) -> Vec<LspWorkspaceSymbol> {
    let mut out: Vec<LspWorkspaceSymbol> = Vec::new();
    for s in syms {
        let loc = s.get("location");
        // workspace/symbol carries its own location (unlike documentSymbol).
        let uri = match loc.and_then(|l| l.get("uri")) {
            Some(u) if !u.is_null() => u,
            _ => continue,
        };
        let uri = match uri.as_str() {
            Some(uri) => uri,
            None => continue,
        };
        let start = loc
            .and_then(|l| l.get("range"))
            .and_then(|r| r.get("start"));
        let container = match s.get("containerName").and_then(RuntimeJson::as_str) {
            Some(container) if !container.is_empty() => Some(container.clone()),
            _ => None,
        };
        out.push(LspWorkspaceSymbol {
            name: s
                .get("name")
                .and_then(RuntimeJson::as_str)
                .cloned()
                .unwrap_or_default(),
            kind: symbol_kind_str(s.get("kind")),
            path: file_url_to_path_safe(uri),
            line: num_at(start, "line"),
            character: num_at(start, "character"),
            container_name: container,
        });
    }
    out.into_iter().take(MAX_RESULT_ITEMS).collect()
}

fn to_call_hierarchy_item(item: &RuntimeJson) -> LspCallHierarchyItem {
    let start = item.get("range").and_then(|r| r.get("start"));
    let detail = match item.get("detail").and_then(RuntimeJson::as_str) {
        Some(detail) if !detail.is_empty() => Some(detail.clone()),
        _ => None,
    };
    LspCallHierarchyItem {
        name: item
            .get("name")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .unwrap_or_default(),
        kind: symbol_kind_str(item.get("kind")),
        path: item
            .get("uri")
            .and_then(RuntimeJson::as_str)
            .map(file_url_to_path_safe)
            .unwrap_or_default(),
        line: num_at(start, "line"),
        character: num_at(start, "character"),
        detail,
    }
}

fn symbol_kind_str(kind: Option<&RuntimeJson>) -> RuntimeString {
    match kind.and_then(|v| v.as_u64()) {
        Some(1) => "file".into(),
        Some(2) => "module".into(),
        Some(3) => "namespace".into(),
        Some(4) => "package".into(),
        Some(5) => "class".into(),
        Some(6) => "method".into(),
        Some(7) => "property".into(),
        Some(8) => "field".into(),
        Some(9) => "constructor".into(),
        Some(10) => "enum".into(),
        Some(11) => "interface".into(),
        Some(12) => "function".into(),
        Some(13) => "variable".into(),
        Some(14) => "constant".into(),
        Some(15) => "string".into(),
        Some(16) => "number".into(),
        Some(17) => "boolean".into(),
        Some(18) => "array".into(),
        Some(19) => "object".into(),
        Some(20) => "key".into(),
        Some(21) => "null".into(),
        Some(22) => "enumMember".into(),
        Some(23) => "struct".into(),
        Some(24) => "event".into(),
        Some(25) => "operator".into(),
        Some(26) => "typeParameter".into(),
        Some(other) => RuntimeString::from(other.to_string()),
        None => "undefined".into(),
    }
}

// --- file URL <-> path ---

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

fn runtime_path_to_file_url(path: &RuntimePath) -> RuntimeString {
    path_to_file_url(path)
}

fn file_url_to_path_safe(uri: &RuntimeString) -> RuntimeString {
    if uri.starts_with("file:") {
        file_url_to_path(uri).unwrap_or_else(|| uri.clone())
    } else {
        uri.clone()
    }
}

/// Map a filesystem path to a `file://` URL. Absolute paths are used verbatim; a
/// relative path is joined onto the current directory. Bytes outside the safe
/// set are percent-encoded (so plain ASCII paths pass through unchanged).
fn path_to_file_url(path: &RuntimePath) -> RuntimeString {
    let path = if is_absolute_runtime(path) {
        path.clone()
    } else {
        std::env::current_dir()
            .ok()
            .map(|cwd| runtime_path_from_native(&cwd))
            .and_then(|cwd| resolve_runtime(&cwd, std::slice::from_ref(path)).ok())
            .unwrap_or_else(|| path.clone())
    };
    #[cfg(windows)]
    {
        let normalized = path.as_runtime_string().replace("\\", "/");
        let mut out = RuntimeString::from("file:///");
        out = out.concat(&encode_url_path(&normalized));
        out
    }
    #[cfg(not(windows))]
    {
        RuntimeString::from("file://").concat(&encode_url_path(path.as_runtime_string()))
    }
}

fn file_url_to_path(uri: &RuntimeString) -> Option<RuntimeString> {
    const PREFIX: &str = "file://";
    if !uri.starts_with(PREFIX) {
        return None;
    }
    let rest = uri.slice_utf16(PREFIX.encode_utf16().count()..);
    // Everything from the first '/' is the path (a leading empty authority is the
    // common local case; a non-empty host is dropped).
    let slash = rest
        .as_utf16()
        .iter()
        .position(|unit| *unit == u16::from(b'/'));
    let path_part = match slash {
        Some(index) => rest.slice_utf16(index..),
        None => rest,
    };
    let decoded = percent_decode(&path_part);
    #[cfg(windows)]
    {
        let path = if decoded.starts_with("/") {
            decoded.slice_utf16(1..)
        } else {
            decoded
        };
        Some(path.replace("/", "\\"))
    }
    #[cfg(not(windows))]
    {
        Some(decoded)
    }
}

fn encode_url_path(path: &RuntimeString) -> RuntimeString {
    let mut output = Vec::new();
    for point in path.code_points() {
        if point <= 0x7f && is_path_safe_byte(point as u8) {
            output.push(point as u16);
            continue;
        }
        let bytes = encode_wtf8_point(point);
        for byte in bytes {
            output.push(u16::from(b'%'));
            output.push(u16::from(hex_upper(byte >> 4) as u8));
            output.push(u16::from(hex_upper(byte & 0x0f) as u8));
        }
    }
    RuntimeString::from_utf16(output)
}

fn percent_decode(value: &RuntimeString) -> RuntimeString {
    let units = value.as_utf16();
    let mut output = Vec::new();
    let mut index = 0;
    while index < units.len() {
        if units[index] != u16::from(b'%') {
            output.push(units[index]);
            index += 1;
            continue;
        }
        let mut bytes = Vec::new();
        while index + 2 < units.len() && units[index] == u16::from(b'%') {
            let Some(high) = hex_value(units[index + 1]) else {
                return value.clone();
            };
            let Some(low) = hex_value(units[index + 2]) else {
                return value.clone();
            };
            bytes.push((high << 4) | low);
            index += 3;
        }
        let Some(decoded) = decode_wtf8(&bytes) else {
            return value.clone();
        };
        output.extend(decoded);
    }
    RuntimeString::from_utf16(output)
}

fn encode_wtf8_point(point: u32) -> Vec<u8> {
    if point <= 0x7f {
        vec![point as u8]
    } else if point <= 0x7ff {
        vec![0xc0 | (point >> 6) as u8, 0x80 | (point & 0x3f) as u8]
    } else if point <= 0xffff {
        vec![
            0xe0 | (point >> 12) as u8,
            0x80 | ((point >> 6) & 0x3f) as u8,
            0x80 | (point & 0x3f) as u8,
        ]
    } else {
        vec![
            0xf0 | (point >> 18) as u8,
            0x80 | ((point >> 12) & 0x3f) as u8,
            0x80 | ((point >> 6) & 0x3f) as u8,
            0x80 | (point & 0x3f) as u8,
        ]
    }
}

fn decode_wtf8(bytes: &[u8]) -> Option<Vec<u16>> {
    let mut output = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        let first = bytes[index];
        let (point, width) = if first <= 0x7f {
            (u32::from(first), 1)
        } else if (0xc2..=0xdf).contains(&first) {
            let second = continuation(bytes, index + 1)?;
            ((u32::from(first & 0x1f) << 6) | u32::from(second), 2)
        } else if (0xe0..=0xef).contains(&first) {
            let second_byte = *bytes.get(index + 1)?;
            let second = continuation(bytes, index + 1)?;
            let third = continuation(bytes, index + 2)?;
            if (first == 0xe0 && second_byte < 0xa0) || (first == 0xed && second_byte > 0xbf) {
                return None;
            }
            (
                (u32::from(first & 0x0f) << 12) | (u32::from(second) << 6) | u32::from(third),
                3,
            )
        } else if (0xf0..=0xf4).contains(&first) {
            let second_byte = *bytes.get(index + 1)?;
            let second = continuation(bytes, index + 1)?;
            let third = continuation(bytes, index + 2)?;
            let fourth = continuation(bytes, index + 3)?;
            if (first == 0xf0 && second_byte < 0x90) || (first == 0xf4 && second_byte > 0x8f) {
                return None;
            }
            (
                (u32::from(first & 0x07) << 18)
                    | (u32::from(second) << 12)
                    | (u32::from(third) << 6)
                    | u32::from(fourth),
                4,
            )
        } else {
            return None;
        };
        if point <= 0xffff {
            output.push(point as u16);
        } else {
            let adjusted = point - 0x1_0000;
            output.push(0xd800 | (adjusted >> 10) as u16);
            output.push(0xdc00 | (adjusted & 0x3ff) as u16);
        }
        index += width;
    }
    Some(output)
}

fn continuation(bytes: &[u8], index: usize) -> Option<u8> {
    let byte = *bytes.get(index)?;
    (byte & 0xc0 == 0x80).then_some(byte & 0x3f)
}

fn hex_value(unit: u16) -> Option<u8> {
    match unit {
        unit if (u16::from(b'0')..=u16::from(b'9')).contains(&unit) => {
            Some((unit - u16::from(b'0')) as u8)
        }
        unit if (u16::from(b'a')..=u16::from(b'f')).contains(&unit) => {
            Some((unit - u16::from(b'a') + 10) as u8)
        }
        unit if (u16::from(b'A')..=u16::from(b'F')).contains(&unit) => {
            Some((unit - u16::from(b'A') + 10) as u8)
        }
        _ => None,
    }
}

fn is_path_safe_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric()
        || matches!(
            b,
            b'/' | b'-'
                | b'.'
                | b'_'
                | b'~'
                | b':'
                | b'@'
                | b'!'
                | b'$'
                | b'&'
                | b'\''
                | b'('
                | b')'
                | b'*'
                | b'+'
                | b','
                | b';'
                | b'='
        )
}

fn hex_upper(nibble: u8) -> char {
    let n = nibble & 0x0f;
    if n < 10 {
        (b'0' + n) as char
    } else {
        (b'A' + (n - 10)) as char
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::node_path::runtime_path_from_native;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicU64;

    static TEST_PATH_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn lifecycle_temp_dir() -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "klaus-lsp-lifecycle-{}-{}",
            std::process::id(),
            TEST_PATH_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn lifecycle_client(root: &std::path::Path) -> LspClient {
        LspClient::new(
            crate::runtime_process::test_runtime_process_context(),
            LspServerConfig {
                language: RuntimeString::from("typescript"),
                extensions: vec![RuntimeString::from(".ts")],
                command: RuntimeString::from("unused"),
                args: None,
                root_dir: None,
            },
            runtime_path_from_native(root),
            LspClientOptions {
                request_timeout_ms: Some(1),
                diagnostics_settle_ms: Some(1),
            },
        )
    }

    fn sent_methods(client: &LspClient) -> Vec<RuntimeString> {
        client
            .shared
            .test_probes
            .sent_messages
            .lock()
            .unwrap()
            .iter()
            .filter_map(|message| {
                message
                    .get("method")
                    .and_then(RuntimeJson::as_string)
                    .cloned()
            })
            .collect()
    }

    fn empty_shared() -> Arc<Shared> {
        Arc::new(Shared {
            writer: TokioMutex::new(None),
            pending: StdMutex::new(HashMap::new()),
            next_id: AtomicI64::new(0),
            next_document_query_attempt: AtomicU64::new(0),
            dead: AtomicBool::new(false),
            pull_diagnostics: AtomicBool::new(false),
            pushed: StdMutex::new(HashMap::new()),
            waiters: StdMutex::new(HashMap::new()),
            next_waiter_id: AtomicU64::new(0),
            document_sync: StdMutex::new(DocumentSyncOptions::default()),
            documents: TokioMutex::new(DocumentLifecycleState::default()),
            document_gates: StdMutex::new(HashMap::new()),
            test_probes: SharedTestProbes::default(),
        })
    }

    // ---- framing ----

    #[test]
    fn encode_frame_has_header_and_body() {
        assert_eq!(encode_frame(b"{}"), b"Content-Length: 2\r\n\r\n{}".to_vec());
    }

    #[test]
    fn decoder_single_message() {
        let mut d = FrameDecoder::new();
        d.push(b"Content-Length: 2\r\n\r\n{}");
        assert_eq!(d.next_message().unwrap(), b"{}".to_vec());
        assert!(d.next_message().is_none());
    }

    #[test]
    fn decoder_split_across_pushes() {
        let mut d = FrameDecoder::new();
        d.push(b"Content-Length: 13\r\n\r\n{\"a\"");
        assert!(d.next_message().is_none());
        d.push(b":\"hello\"}");
        assert_eq!(d.next_message().unwrap(), br#"{"a":"hello"}"#.to_vec());
    }

    #[test]
    fn decoder_two_messages_in_one_chunk() {
        let mut d = FrameDecoder::new();
        let mut buf = Vec::new();
        buf.extend_from_slice(&encode_frame(br#"{"n":1}"#));
        buf.extend_from_slice(&encode_frame(br#"{"n":2}"#));
        d.push(&buf);
        assert_eq!(d.next_message().unwrap(), br#"{"n":1}"#.to_vec());
        assert_eq!(d.next_message().unwrap(), br#"{"n":2}"#.to_vec());
        assert!(d.next_message().is_none());
    }

    #[test]
    fn parse_content_length_ignores_other_headers_case_insensitive() {
        let hdr = b"Content-Type: application/vscode-jsonrpc; charset=utf-8\r\ncontent-length: 42";
        assert_eq!(parse_content_length(hdr), Some(42));
    }

    #[test]
    fn decoder_recovers_from_missing_content_length() {
        let mut d = FrameDecoder::new();
        d.push(b"X-Bogus: 1\r\n\r\n");
        d.push(&encode_frame(br#"{"ok":1}"#));
        assert_eq!(d.next_message().unwrap(), br#"{"ok":1}"#.to_vec());
    }

    // ---- dispatch ----

    #[tokio::test]
    async fn dispatches_response_to_pending() {
        let shared = empty_shared();
        let (tx, rx) = oneshot::channel();
        shared.pending.lock().unwrap().insert(7, tx);
        handle_message(
            &shared,
            br#"{"jsonrpc":"2.0","id":7,"result":{"ok":true}}"#.to_vec(),
        )
        .await;
        assert_eq!(rx.await.unwrap().unwrap(), exact_json!({"ok": true}));
    }

    #[tokio::test]
    async fn dispatches_error_response() {
        let shared = empty_shared();
        let (tx, rx) = oneshot::channel();
        shared.pending.lock().unwrap().insert(3, tx);
        handle_message(
            &shared,
            br#"{"jsonrpc":"2.0","id":3,"error":{"code":-32801,"message":"content modified"}}"#
                .to_vec(),
        )
        .await;
        assert_eq!(rx.await.unwrap().unwrap_err().code, -32801);
    }

    #[tokio::test]
    async fn cancelled_request_drops_pending_id_while_retained_owner_completes_frame() {
        let root = lifecycle_temp_dir();
        let client = Arc::new(lifecycle_client(&root));
        let (reached_tx, reached_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = oneshot::channel();
        *client
            .shared
            .test_probes
            .before_frame_write_complete
            .lock()
            .unwrap() = Some(DocumentReadBarrier {
            reached: reached_tx,
            resume: resume_rx,
        });

        let request = {
            let client = client.clone();
            tokio::spawn(async move { client.request("test/cancel", None).await })
        };
        reached_rx.await.unwrap();
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        assert!(client.shared.pending.lock().unwrap().is_empty());

        resume_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while client
                .shared
                .test_probes
                .completed_frame_writes
                .load(Ordering::SeqCst)
                == 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the detached writer owner must finish the complete frame");
        assert!(!client.is_dead());
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn pending_request_timeouts_do_not_accumulate_ids() {
        let root = lifecycle_temp_dir();
        let client = lifecycle_client(&root);
        for _ in 0..64 {
            assert!(client.request("test/timeout", None).await.is_none());
            assert!(client.shared.pending.lock().unwrap().is_empty());
        }
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn diagnostics_waiter_timeouts_and_cancellation_do_not_accumulate() {
        let root = lifecycle_temp_dir();
        let mut client = lifecycle_client(&root);
        let uri = RuntimeString::from("file:///waiter.ts");

        for _ in 0..64 {
            client.await_push(&uri).await;
            assert!(client.shared.waiters.lock().unwrap().is_empty());
        }

        client.diagnostics_settle_ms = 60_000;
        let client = Arc::new(client);
        for _ in 0..64 {
            let waiter = {
                let client = client.clone();
                let uri = uri.clone();
                tokio::spawn(async move { client.await_push(&uri).await })
            };
            tokio::time::timeout(Duration::from_secs(1), async {
                while client.shared.waiters.lock().unwrap().is_empty() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("waiter must register before cancellation");
            waiter.abort();
            assert!(waiter.await.unwrap_err().is_cancelled());
            assert!(client.shared.waiters.lock().unwrap().is_empty());
        }
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn publish_diagnostics_stores_and_wakes_waiter() {
        let shared = empty_shared();
        let (tx, rx) = oneshot::channel();
        shared
            .waiters
            .lock()
            .unwrap()
            .entry("file:///x.ts".into())
            .or_default()
            .insert(7, tx);
        handle_message(
            &shared,
            br#"{"jsonrpc":"2.0","method":"textDocument/publishDiagnostics","params":{"uri":"file:///x.ts","diagnostics":[{"range":{"start":{"line":1,"character":2},"end":{"line":1,"character":5}},"severity":1,"message":"boom"}]}}"#
                .to_vec(),
        )
        .await;
        assert!(rx.await.is_ok());
        assert_eq!(
            shared
                .pushed
                .lock()
                .unwrap()
                .get(&RuntimeString::from("file:///x.ts"))
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn workspace_configuration_returns_nulls_per_item() {
        let params =
            exact_json!({ "items": [{"section": "a"}, {"section": "b"}, {"section": "c"}] });
        assert_eq!(
            server_request_result(
                &RuntimeString::from("workspace/configuration"),
                Some(&params)
            )
            .unwrap(),
            exact_json!([null, null, null])
        );
    }

    #[test]
    fn benign_requests_return_null_unknown_is_error() {
        assert_eq!(
            server_request_result(&RuntimeString::from("client/registerCapability"), None).unwrap(),
            RuntimeJson::Null
        );
        assert_eq!(
            server_request_result(&RuntimeString::from("window/workDoneProgress/create"), None)
                .unwrap(),
            RuntimeJson::Null
        );
        assert!(
            server_request_result(&RuntimeString::from("window/showMessageRequest"), None).is_err()
        );
    }

    // ---- capability handling ----

    #[test]
    fn pull_diagnostics_detected_from_capabilities() {
        assert!(detects_pull_diagnostics(
            &exact_json!({"capabilities": {"diagnosticProvider": {"interFileDependencies": true}}})
        ));
        assert!(!detects_pull_diagnostics(
            &exact_json!({"capabilities": {"diagnosticProvider": null}})
        ));
        assert!(!detects_pull_diagnostics(
            &exact_json!({"capabilities": {}})
        ));
        assert!(!detects_pull_diagnostics(&exact_json!({})));
    }

    #[test]
    fn text_document_sync_capability_is_normalized() {
        assert_eq!(
            document_sync_options(&exact_json!({
                "capabilities": { "textDocumentSync": 2_u64 }
            })),
            DocumentSyncOptions {
                open_close: true,
                change: DocumentChangeSyncKind::Incremental,
            }
        );
        assert_eq!(
            document_sync_options(&exact_json!({
                "capabilities": {
                    "textDocumentSync": { "openClose": true, "change": 1_u64 }
                }
            })),
            DocumentSyncOptions {
                open_close: true,
                change: DocumentChangeSyncKind::Full,
            }
        );
        assert_eq!(
            document_sync_options(&exact_json!({
                "capabilities": { "textDocumentSync": 0_u64 }
            })),
            DocumentSyncOptions {
                open_close: false,
                change: DocumentChangeSyncKind::None,
            }
        );
    }

    #[test]
    fn document_end_counts_utf16_code_units() {
        assert_eq!(document_end(&RuntimeString::from("a\n\u{1f600}")), (1, 2));
        assert_eq!(document_end(&RuntimeString::from("")), (0, 0));
    }

    #[tokio::test]
    async fn first_file_query_opens_before_request_and_reuses_current_document() {
        let root = lifecycle_temp_dir();
        let native = root.join("first.ts");
        std::fs::write(&native, "const value = 1;\n").unwrap();
        let path = runtime_path_from_native(&native);
        let client = lifecycle_client(&root);

        assert!(client.hover(&path, 0, 0).await.unwrap().is_none());
        assert!(client.hover(&path, 0, 0).await.unwrap().is_none());

        assert_eq!(
            sent_methods(&client),
            vec![
                RuntimeString::from("textDocument/didOpen"),
                RuntimeString::from("textDocument/hover"),
                RuntimeString::from("textDocument/hover"),
            ]
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn query_synchronizes_changed_disk_content_before_request() {
        let root = lifecycle_temp_dir();
        let native = root.join("change.ts");
        std::fs::write(&native, "old\n").unwrap();
        let path = runtime_path_from_native(&native);
        let client = lifecycle_client(&root);

        let _ = client.hover(&path, 0, 0).await;
        std::fs::write(&native, "new\n").unwrap();
        let _ = client.hover(&path, 0, 0).await;

        assert_eq!(
            sent_methods(&client),
            vec![
                RuntimeString::from("textDocument/didOpen"),
                RuntimeString::from("textDocument/hover"),
                RuntimeString::from("textDocument/didChange"),
                RuntimeString::from("textDocument/hover"),
            ]
        );
        let messages = client.shared.test_probes.sent_messages.lock().unwrap();
        assert_eq!(
            messages[2]
                .pointer("/params/contentChanges/0/text")
                .and_then(RuntimeJson::as_str),
            Some(&RuntimeString::from("new\n"))
        );
        drop(messages);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn cancelled_document_lifecycle_write_poisons_before_local_state_can_diverge() {
        let root = lifecycle_temp_dir();
        let native = root.join("cancelled-lifecycle.ts");
        std::fs::write(&native, "disk\n").unwrap();
        let path = runtime_path_from_native(&native);
        let client = Arc::new(lifecycle_client(&root));
        let (reached_tx, reached_rx) = oneshot::channel();
        let (_resume_tx, resume_rx) = oneshot::channel();
        *client
            .shared
            .test_probes
            .before_lifecycle_state_commit
            .lock()
            .unwrap() = Some(DocumentReadBarrier {
            reached: reached_tx,
            resume: resume_rx,
        });

        let edit = {
            let client = client.clone();
            let path = path.clone();
            tokio::spawn(async move {
                client
                    .notify_edited(&path, Some(&RuntimeString::from("authoritative\n")))
                    .await
            })
        };
        reached_rx.await.unwrap();
        assert_eq!(
            client
                .shared
                .test_probes
                .completed_frame_writes
                .load(Ordering::SeqCst),
            1,
            "the barrier must be after the complete frame and before local state"
        );
        edit.abort();
        assert!(edit.await.unwrap_err().is_cancelled());
        assert!(client.is_dead());
        assert!(client.shared.documents.lock().await.open.is_empty());

        assert_eq!(
            sent_methods(&client),
            vec![RuntimeString::from("textDocument/didOpen")]
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn cancelled_close_open_sequence_poisons_after_first_complete_frame() {
        let root = lifecycle_temp_dir();
        let native = root.join("cancelled-close-open.ts");
        std::fs::write(&native, "old\n").unwrap();
        let path = runtime_path_from_native(&native);
        let client = Arc::new(lifecycle_client(&root));
        assert!(client.hover(&path, 0, 0).await.unwrap().is_none());
        *client.shared.document_sync.lock().unwrap() = DocumentSyncOptions {
            open_close: true,
            change: DocumentChangeSyncKind::None,
        };
        let completed_before = client
            .shared
            .test_probes
            .completed_frame_writes
            .load(Ordering::SeqCst);
        let (reached_tx, reached_rx) = oneshot::channel();
        let (_resume_tx, resume_rx) = oneshot::channel();
        *client
            .shared
            .test_probes
            .between_lifecycle_frames
            .lock()
            .unwrap() = Some(DocumentReadBarrier {
            reached: reached_tx,
            resume: resume_rx,
        });

        let edit = {
            let client = client.clone();
            let path = path.clone();
            tokio::spawn(async move {
                client
                    .notify_edited(&path, Some(&RuntimeString::from("new\n")))
                    .await
            })
        };
        reached_rx.await.unwrap();
        assert_eq!(
            client
                .shared
                .test_probes
                .completed_frame_writes
                .load(Ordering::SeqCst),
            completed_before + 1,
            "didClose must be complete while the one transaction guard remains armed"
        );
        edit.abort();
        assert!(edit.await.unwrap_err().is_cancelled());
        assert!(client.is_dead());
        assert_eq!(
            client
                .shared
                .documents
                .lock()
                .await
                .open
                .get(&client.document_uri(&path))
                .map(|document| document.content.clone()),
            Some(RuntimeString::from("old\n"))
        );
        assert_eq!(
            sent_methods(&client),
            vec![
                RuntimeString::from("textDocument/didOpen"),
                RuntimeString::from("textDocument/hover"),
                RuntimeString::from("textDocument/didClose"),
            ]
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn cancelled_notify_deleted_poisons_after_close_before_local_removal() {
        let root = lifecycle_temp_dir();
        let native = root.join("cancelled-delete.ts");
        std::fs::write(&native, "open\n").unwrap();
        let path = runtime_path_from_native(&native);
        let client = Arc::new(lifecycle_client(&root));
        assert!(client.hover(&path, 0, 0).await.unwrap().is_none());
        let completed_before = client
            .shared
            .test_probes
            .completed_frame_writes
            .load(Ordering::SeqCst);
        let (reached_tx, reached_rx) = oneshot::channel();
        let (_resume_tx, resume_rx) = oneshot::channel();
        *client
            .shared
            .test_probes
            .before_lifecycle_state_commit
            .lock()
            .unwrap() = Some(DocumentReadBarrier {
            reached: reached_tx,
            resume: resume_rx,
        });

        let deletion = {
            let client = client.clone();
            let path = path.clone();
            tokio::spawn(async move { client.notify_deleted(&path).await })
        };
        reached_rx.await.unwrap();
        assert_eq!(
            client
                .shared
                .test_probes
                .completed_frame_writes
                .load(Ordering::SeqCst),
            completed_before + 1
        );
        deletion.abort();
        assert!(deletion.await.unwrap_err().is_cancelled());
        assert!(client.is_dead());
        assert!(client
            .shared
            .documents
            .lock()
            .await
            .open
            .contains_key(&client.document_uri(&path)));
        assert_eq!(
            sent_methods(&client),
            vec![
                RuntimeString::from("textDocument/didOpen"),
                RuntimeString::from("textDocument/hover"),
                RuntimeString::from("textDocument/didClose"),
            ]
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn atomic_path_replacement_after_handle_read_retries_current_file_before_epoch() {
        let root = lifecycle_temp_dir();
        let native = root.join("atomic-replace.ts");
        let replacement = root.join("atomic-replacement.tmp");
        std::fs::write(&native, "old handle\n").unwrap();
        std::fs::write(&replacement, "current path\n").unwrap();
        let path = runtime_path_from_native(&native);
        let client = Arc::new(lifecycle_client(&root));
        let (reached_tx, reached_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = oneshot::channel();
        *client
            .shared
            .test_probes
            .after_document_handle_read
            .lock()
            .unwrap() = Some(DocumentReadBarrier {
            reached: reached_tx,
            resume: resume_rx,
        });

        let query = {
            let client = client.clone();
            let path = path.clone();
            tokio::spawn(async move { client.hover(&path, 0, 0).await })
        };
        reached_rx.await.unwrap();
        std::fs::rename(&replacement, &native).unwrap();
        resume_tx.send(()).unwrap();
        assert!(query.await.unwrap().unwrap().is_none());

        assert_eq!(
            sent_methods(&client),
            vec![
                RuntimeString::from("textDocument/didOpen"),
                RuntimeString::from("textDocument/hover"),
            ]
        );
        let messages = client.shared.test_probes.sent_messages.lock().unwrap();
        assert_eq!(
            messages[0]
                .pointer("/params/textDocument/text")
                .and_then(RuntimeJson::as_str),
            Some(&RuntimeString::from("current path\n")),
            "the old inode's completed read must not receive a commit epoch"
        );
        drop(messages);
        std::fs::remove_dir_all(root).ok();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn replacement_between_validation_and_epoch_cannot_rollback_a_newer_query() {
        let root = lifecycle_temp_dir();
        let native = root.join("epoch-linearization.ts");
        let replacement = root.join("epoch-linearization-replacement.tmp");
        std::fs::write(&native, "A\n").unwrap();
        std::fs::write(&replacement, "B\n").unwrap();
        let path = runtime_path_from_native(&native);
        let client = Arc::new(lifecycle_client(&root));
        let (reached_tx, reached_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = oneshot::channel();
        *client
            .shared
            .test_probes
            .before_document_query_epoch
            .lock()
            .unwrap() = Some(DocumentReadBarrier {
            reached: reached_tx,
            resume: resume_rx,
        });

        let first = {
            let client = client.clone();
            let path = path.clone();
            tokio::spawn(async move { client.hover(&path, 0, 0).await })
        };
        reached_rx.await.unwrap();
        std::fs::rename(&replacement, &native).unwrap();
        assert!(client.hover(&path, 0, 0).await.unwrap().is_none());
        resume_tx.send(()).unwrap();
        assert!(first.await.unwrap().unwrap().is_none());

        assert_eq!(
            sent_methods(&client),
            vec![
                RuntimeString::from("textDocument/didOpen"),
                RuntimeString::from("textDocument/hover"),
                RuntimeString::from("textDocument/hover"),
            ]
        );
        let messages = client.shared.test_probes.sent_messages.lock().unwrap();
        assert_eq!(
            messages[0]
                .pointer("/params/textDocument/text")
                .and_then(RuntimeJson::as_str),
            Some(&RuntimeString::from("B\n")),
            "the candidate observed before its epoch must be revalidated after the epoch"
        );
        assert!(messages.iter().all(|message| {
            message
                .pointer("/params/textDocument/text")
                .and_then(RuntimeJson::as_str)
                != Some(&RuntimeString::from("A\n"))
        }));
        drop(messages);
        std::fs::remove_dir_all(root).ok();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn epoch_revalidation_covers_missing_oversize_and_unsupported_observations() {
        // Missing -> regular creation after the candidate observation.
        let missing_root = lifecycle_temp_dir();
        let missing_native = missing_root.join("missing-linearization.ts");
        let missing_path = runtime_path_from_native(&missing_native);
        let missing_client = Arc::new(lifecycle_client(&missing_root));
        let (reached_tx, reached_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = oneshot::channel();
        *missing_client
            .shared
            .test_probes
            .before_document_query_epoch
            .lock()
            .unwrap() = Some(DocumentReadBarrier {
            reached: reached_tx,
            resume: resume_rx,
        });
        let missing_query = {
            let client = missing_client.clone();
            let path = missing_path.clone();
            tokio::spawn(async move { client.hover(&path, 0, 0).await })
        };
        reached_rx.await.unwrap();
        std::fs::write(&missing_native, "created\n").unwrap();
        resume_tx.send(()).unwrap();
        assert!(missing_query.await.unwrap().unwrap().is_none());
        assert_eq!(
            missing_client
                .shared
                .test_probes
                .sent_messages
                .lock()
                .unwrap()[0]
                .pointer("/params/textDocument/text")
                .and_then(RuntimeJson::as_str),
            Some(&RuntimeString::from("created\n"))
        );
        std::fs::remove_dir_all(missing_root).ok();

        // Oversize -> small atomic replacement after the candidate observation.
        let oversize_root = lifecycle_temp_dir();
        let oversize_native = oversize_root.join("oversize-linearization.ts");
        let oversize_replacement = oversize_root.join("oversize-replacement.tmp");
        std::fs::File::create(&oversize_native)
            .unwrap()
            .set_len(MAX_LSP_FILE_SIZE_BYTES + 1)
            .unwrap();
        std::fs::write(&oversize_replacement, "small\n").unwrap();
        let oversize_path = runtime_path_from_native(&oversize_native);
        let oversize_client = Arc::new(lifecycle_client(&oversize_root));
        let (reached_tx, reached_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = oneshot::channel();
        *oversize_client
            .shared
            .test_probes
            .before_document_query_epoch
            .lock()
            .unwrap() = Some(DocumentReadBarrier {
            reached: reached_tx,
            resume: resume_rx,
        });
        let oversize_query = {
            let client = oversize_client.clone();
            let path = oversize_path.clone();
            tokio::spawn(async move { client.hover(&path, 0, 0).await })
        };
        reached_rx.await.unwrap();
        std::fs::rename(&oversize_replacement, &oversize_native).unwrap();
        resume_tx.send(()).unwrap();
        assert!(oversize_query.await.unwrap().unwrap().is_none());
        assert_eq!(
            oversize_client
                .shared
                .test_probes
                .sent_messages
                .lock()
                .unwrap()[0]
                .pointer("/params/textDocument/text")
                .and_then(RuntimeJson::as_str),
            Some(&RuntimeString::from("small\n"))
        );
        std::fs::remove_dir_all(oversize_root).ok();

        // Unsupported directory -> regular replacement after the candidate observation.
        let unsupported_root = lifecycle_temp_dir();
        let unsupported_native = unsupported_root.join("unsupported-linearization.ts");
        let unsupported_replacement = unsupported_root.join("unsupported-replacement.tmp");
        std::fs::create_dir(&unsupported_native).unwrap();
        std::fs::write(&unsupported_replacement, "regular\n").unwrap();
        let unsupported_path = runtime_path_from_native(&unsupported_native);
        let unsupported_client = Arc::new(lifecycle_client(&unsupported_root));
        let (reached_tx, reached_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = oneshot::channel();
        *unsupported_client
            .shared
            .test_probes
            .before_document_query_epoch
            .lock()
            .unwrap() = Some(DocumentReadBarrier {
            reached: reached_tx,
            resume: resume_rx,
        });
        let unsupported_query = {
            let client = unsupported_client.clone();
            let path = unsupported_path.clone();
            tokio::spawn(async move { client.hover(&path, 0, 0).await })
        };
        reached_rx.await.unwrap();
        std::fs::remove_dir(&unsupported_native).unwrap();
        std::fs::rename(&unsupported_replacement, &unsupported_native).unwrap();
        resume_tx.send(()).unwrap();
        assert!(unsupported_query.await.unwrap().unwrap().is_none());
        assert_eq!(
            unsupported_client
                .shared
                .test_probes
                .sent_messages
                .lock()
                .unwrap()[0]
                .pointer("/params/textDocument/text")
                .and_then(RuntimeJson::as_str),
            Some(&RuntimeString::from("regular\n"))
        );
        std::fs::remove_dir_all(unsupported_root).ok();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn exhausted_unstable_reads_preserve_current_document_without_false_close() {
        let root = lifecycle_temp_dir();
        let native = root.join("unstable-exhaustion.ts");
        std::fs::write(&native, "initial\n").unwrap();
        let path = runtime_path_from_native(&native);
        let client = Arc::new(lifecycle_client(&root));
        assert!(client.hover(&path, 0, 0).await.unwrap().is_none());

        let mut barriers = Vec::new();
        let mut reached = Vec::new();
        let mut resumes = Vec::new();
        let mut replacements = Vec::new();
        for index in 0..MAX_DOCUMENT_READ_ATTEMPTS {
            let replacement = root.join(format!("unstable-replacement-{index}.tmp"));
            std::fs::write(&replacement, format!("replacement-{index}\n")).unwrap();
            replacements.push(replacement);
            let (reached_tx, reached_rx) = oneshot::channel();
            let (resume_tx, resume_rx) = oneshot::channel();
            barriers.push(DocumentReadBarrier {
                reached: reached_tx,
                resume: resume_rx,
            });
            reached.push(reached_rx);
            resumes.push(resume_tx);
        }
        *client
            .shared
            .test_probes
            .after_document_handle_read_sequence
            .lock()
            .unwrap() = barriers;

        let unstable = {
            let client = client.clone();
            let path = path.clone();
            tokio::spawn(async move { client.hover(&path, 0, 0).await })
        };
        for ((reached, resume), replacement) in reached.into_iter().zip(resumes).zip(replacements) {
            reached.await.unwrap();
            std::fs::rename(&replacement, &native).unwrap();
            resume.send(()).unwrap();
        }
        assert!(unstable.await.unwrap().unwrap().is_none());

        assert_eq!(
            sent_methods(&client),
            vec![
                RuntimeString::from("textDocument/didOpen"),
                RuntimeString::from("textDocument/hover"),
            ],
            "transient exhaustion must not emit didClose, didChange, or a request"
        );
        assert_eq!(
            client
                .shared
                .documents
                .lock()
                .await
                .open
                .get(&client.document_uri(&path))
                .map(|document| document.content.clone()),
            Some(RuntimeString::from("initial\n"))
        );
        assert!(client
            .shared
            .documents
            .lock()
            .await
            .queries
            .values()
            .all(|query| query.in_flight == 0));
        std::fs::remove_dir_all(root).ok();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn exhausted_unstable_provided_text_validation_preserves_current_document() {
        let root = lifecycle_temp_dir();
        let native = root.join("unstable-authoritative-validation.ts");
        std::fs::write(&native, "initial\n").unwrap();
        let path = runtime_path_from_native(&native);
        let client = Arc::new(lifecycle_client(&root));
        assert!(client.hover(&path, 0, 0).await.unwrap().is_none());

        let mut barriers = Vec::new();
        let mut reached = Vec::new();
        let mut resumes = Vec::new();
        let mut replacements = Vec::new();
        for index in 0..MAX_DOCUMENT_READ_ATTEMPTS {
            let replacement = root.join(format!("authoritative-replacement-{index}.tmp"));
            std::fs::write(&replacement, format!("authoritative-disk-{index}\n")).unwrap();
            replacements.push(replacement);
            let (reached_tx, reached_rx) = oneshot::channel();
            let (resume_tx, resume_rx) = oneshot::channel();
            barriers.push(DocumentReadBarrier {
                reached: reached_tx,
                resume: resume_rx,
            });
            reached.push(reached_rx);
            resumes.push(resume_tx);
        }
        *client
            .shared
            .test_probes
            .after_document_handle_read_sequence
            .lock()
            .unwrap() = barriers;

        let validation = {
            let client = client.clone();
            let path = path.clone();
            tokio::spawn(async move {
                client
                    .notify_edited(&path, Some(&RuntimeString::from("authoritative\n")))
                    .await
            })
        };
        for ((reached, resume), replacement) in reached.into_iter().zip(resumes).zip(replacements) {
            reached.await.unwrap();
            std::fs::rename(&replacement, &native).unwrap();
            resume.send(()).unwrap();
        }
        validation.await.unwrap();

        assert_eq!(
            sent_methods(&client),
            vec![
                RuntimeString::from("textDocument/didOpen"),
                RuntimeString::from("textDocument/hover"),
            ]
        );
        assert_eq!(
            client
                .shared
                .documents
                .lock()
                .await
                .open
                .get(&client.document_uri(&path))
                .map(|document| document.content.clone()),
            Some(RuntimeString::from("initial\n"))
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn same_handle_metadata_change_during_read_forces_a_bounded_reread() {
        let root = lifecycle_temp_dir();
        let native = root.join("mutated-during-read.ts");
        std::fs::write(&native, "before\n").unwrap();
        let path = runtime_path_from_native(&native);
        let client = Arc::new(lifecycle_client(&root));
        let (reached_tx, reached_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = oneshot::channel();
        *client
            .shared
            .test_probes
            .after_document_handle_read
            .lock()
            .unwrap() = Some(DocumentReadBarrier {
            reached: reached_tx,
            resume: resume_rx,
        });

        let query = {
            let client = client.clone();
            let path = path.clone();
            tokio::spawn(async move { client.hover(&path, 0, 0).await })
        };
        reached_rx.await.unwrap();
        std::fs::write(&native, "after!\n").unwrap();
        resume_tx.send(()).unwrap();
        assert!(query.await.unwrap().unwrap().is_none());

        let messages = client.shared.test_probes.sent_messages.lock().unwrap();
        assert_eq!(
            messages[0]
                .pointer("/params/textDocument/text")
                .and_then(RuntimeJson::as_str),
            Some(&RuntimeString::from("after!\n"))
        );
        drop(messages);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn newer_query_commit_prevents_older_disk_snapshot_overwrite() {
        let root = lifecycle_temp_dir();
        let native = root.join("query-query-race.ts");
        std::fs::write(&native, "A\n").unwrap();
        let path = runtime_path_from_native(&native);
        let client = Arc::new(lifecycle_client(&root));
        let (reached_tx, reached_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = oneshot::channel();
        *client
            .shared
            .test_probes
            .after_document_read
            .lock()
            .unwrap() = Some(DocumentReadBarrier {
            reached: reached_tx,
            resume: resume_rx,
        });

        let older = {
            let client = client.clone();
            let path = path.clone();
            tokio::spawn(async move { client.hover(&path, 0, 0).await })
        };
        reached_rx.await.unwrap();
        std::fs::write(&native, "B\n").unwrap();
        assert!(client.hover(&path, 0, 0).await.unwrap().is_none());
        resume_tx.send(()).unwrap();
        assert!(older.await.unwrap().unwrap().is_none());

        assert_eq!(
            sent_methods(&client),
            vec![
                RuntimeString::from("textDocument/didOpen"),
                RuntimeString::from("textDocument/hover"),
                RuntimeString::from("textDocument/hover"),
            ],
            "the older A snapshot must query the newer B state without a stale didChange"
        );
        {
            let messages = client.shared.test_probes.sent_messages.lock().unwrap();
            assert_eq!(
                messages[0]
                    .pointer("/params/textDocument/text")
                    .and_then(RuntimeJson::as_str),
                Some(&RuntimeString::from("B\n"))
            );
        }
        assert_eq!(
            client
                .shared
                .documents
                .lock()
                .await
                .open
                .get(&client.document_uri(&path))
                .map(|document| document.content.clone()),
            Some(RuntimeString::from("B\n"))
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn identical_concurrent_query_snapshots_both_execute_without_resync() {
        let root = lifecycle_temp_dir();
        let native = root.join("query-query-same.ts");
        std::fs::write(&native, "same\n").unwrap();
        let path = runtime_path_from_native(&native);
        let client = Arc::new(lifecycle_client(&root));
        let (reached_tx, reached_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = oneshot::channel();
        *client
            .shared
            .test_probes
            .after_document_read
            .lock()
            .unwrap() = Some(DocumentReadBarrier {
            reached: reached_tx,
            resume: resume_rx,
        });

        let older = {
            let client = client.clone();
            let path = path.clone();
            tokio::spawn(async move { client.hover(&path, 0, 0).await })
        };
        reached_rx.await.unwrap();
        assert!(client.hover(&path, 0, 0).await.unwrap().is_none());
        resume_tx.send(()).unwrap();
        assert!(older.await.unwrap().unwrap().is_none());

        assert_eq!(
            sent_methods(&client),
            vec![
                RuntimeString::from("textDocument/didOpen"),
                RuntimeString::from("textDocument/hover"),
                RuntimeString::from("textDocument/hover"),
            ],
            "same-content concurrency must retain both requests without serializing or resyncing"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn newer_missing_query_commit_prevents_older_snapshot_reopen() {
        let root = lifecycle_temp_dir();
        let native = root.join("query-query-missing.ts");
        std::fs::write(&native, "stale\n").unwrap();
        let path = runtime_path_from_native(&native);
        let client = Arc::new(lifecycle_client(&root));
        let (reached_tx, reached_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = oneshot::channel();
        *client
            .shared
            .test_probes
            .after_document_read
            .lock()
            .unwrap() = Some(DocumentReadBarrier {
            reached: reached_tx,
            resume: resume_rx,
        });

        let older = {
            let client = client.clone();
            let path = path.clone();
            tokio::spawn(async move { client.hover(&path, 0, 0).await })
        };
        reached_rx.await.unwrap();
        std::fs::remove_file(&native).unwrap();
        assert!(client.hover(&path, 0, 0).await.unwrap().is_none());
        resume_tx.send(()).unwrap();
        assert!(older.await.unwrap().unwrap().is_none());

        assert!(
            sent_methods(&client).is_empty(),
            "the newer missing tombstone must suppress stale didOpen and request"
        );
        assert!(client.shared.documents.lock().await.queries.is_empty());
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn newer_oversize_query_error_prevents_older_snapshot_reopen() {
        let root = lifecycle_temp_dir();
        let native = root.join("query-query-oversize.ts");
        std::fs::write(&native, "stale\n").unwrap();
        let path = runtime_path_from_native(&native);
        let client = Arc::new(lifecycle_client(&root));
        let (reached_tx, reached_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = oneshot::channel();
        *client
            .shared
            .test_probes
            .after_document_read
            .lock()
            .unwrap() = Some(DocumentReadBarrier {
            reached: reached_tx,
            resume: resume_rx,
        });

        let older = {
            let client = client.clone();
            let path = path.clone();
            tokio::spawn(async move { client.hover(&path, 0, 0).await })
        };
        reached_rx.await.unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&native)
            .unwrap()
            .set_len(MAX_LSP_FILE_SIZE_BYTES + 1)
            .unwrap();
        assert_eq!(
            client.hover(&path, 0, 0).await,
            Err(LspQueryError::FileTooLarge {
                size_bytes: MAX_LSP_FILE_SIZE_BYTES + 1,
                max_bytes: MAX_LSP_FILE_SIZE_BYTES,
            })
        );
        resume_tx.send(()).unwrap();
        assert!(older.await.unwrap().unwrap().is_none());

        assert!(sent_methods(&client).is_empty());
        assert!(client.shared.documents.lock().await.queries.is_empty());
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn cancelled_query_releases_lifecycle_registration() {
        let root = lifecycle_temp_dir();
        let native = root.join("cancelled-query.ts");
        std::fs::write(&native, "cancelled\n").unwrap();
        let path = runtime_path_from_native(&native);
        let client = Arc::new(lifecycle_client(&root));
        let (reached_tx, reached_rx) = oneshot::channel();
        let (_resume_tx, resume_rx) = oneshot::channel();
        *client
            .shared
            .test_probes
            .after_document_read
            .lock()
            .unwrap() = Some(DocumentReadBarrier {
            reached: reached_tx,
            resume: resume_rx,
        });

        let query = {
            let client = client.clone();
            tokio::spawn(async move { client.hover(&path, 0, 0).await })
        };
        reached_rx.await.unwrap();
        query.abort();
        assert!(query.await.unwrap_err().is_cancelled());
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if client.shared.documents.lock().await.queries.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled query registration must be cleaned asynchronously");

        assert!(sent_methods(&client).is_empty());
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn query_request_lease_blocks_a_concurrent_missing_commit_until_request_finishes() {
        let root = lifecycle_temp_dir();
        let native = root.join("request-current.ts");
        std::fs::write(&native, "current\n").unwrap();
        let path = runtime_path_from_native(&native);
        let client = Arc::new(lifecycle_client(&root));
        let (reached_tx, reached_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = oneshot::channel();
        *client.shared.test_probes.after_request_sent.lock().unwrap() = Some(DocumentReadBarrier {
            reached: reached_tx,
            resume: resume_rx,
        });

        let request = {
            let client = client.clone();
            let path = path.clone();
            tokio::spawn(async move { client.hover(&path, 0, 0).await })
        };
        reached_rx.await.unwrap();
        std::fs::remove_file(&native).unwrap();
        let mut missing = {
            let client = client.clone();
            let path = path.clone();
            tokio::spawn(async move { client.hover(&path, 0, 0).await })
        };

        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut missing)
                .await
                .is_err(),
            "a missing commit must wait rather than close the URI under an active request"
        );
        assert_eq!(
            sent_methods(&client),
            vec![
                RuntimeString::from("textDocument/didOpen"),
                RuntimeString::from("textDocument/hover"),
            ]
        );

        resume_tx.send(()).unwrap();
        assert!(request.await.unwrap().unwrap().is_none());
        assert!(missing.await.unwrap().unwrap().is_none());
        assert_eq!(
            sent_methods(&client),
            vec![
                RuntimeString::from("textDocument/didOpen"),
                RuntimeString::from("textDocument/hover"),
                RuntimeString::from("textDocument/didClose"),
            ]
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn lsp_file_size_limit_allows_exact_boundary_and_rejects_next_byte() {
        let root = lifecycle_temp_dir();
        let allowed_native = root.join("allowed.ts");
        let rejected_native = root.join("rejected.ts");
        std::fs::File::create(&allowed_native)
            .unwrap()
            .set_len(MAX_LSP_FILE_SIZE_BYTES)
            .unwrap();
        std::fs::File::create(&rejected_native)
            .unwrap()
            .set_len(MAX_LSP_FILE_SIZE_BYTES + 1)
            .unwrap();

        let allowed = lifecycle_client(&root);
        assert!(allowed
            .hover(&runtime_path_from_native(&allowed_native), 0, 0)
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            sent_methods(&allowed),
            vec![
                RuntimeString::from("textDocument/didOpen"),
                RuntimeString::from("textDocument/hover"),
            ]
        );

        let rejected = lifecycle_client(&root);
        assert_eq!(
            rejected
                .hover(&runtime_path_from_native(&rejected_native), 0, 0)
                .await,
            Err(LspQueryError::FileTooLarge {
                size_bytes: MAX_LSP_FILE_SIZE_BYTES + 1,
                max_bytes: MAX_LSP_FILE_SIZE_BYTES,
            })
        );
        assert!(sent_methods(&rejected).is_empty());
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn non_regular_document_is_rejected_without_did_open_or_request() {
        let root = lifecycle_temp_dir();
        let directory = root.join("directory.ts");
        std::fs::create_dir(&directory).unwrap();
        let client = lifecycle_client(&root);

        assert_eq!(
            client
                .hover(&runtime_path_from_native(&directory), 0, 0)
                .await,
            Err(LspQueryError::UnsupportedFileType)
        );
        assert!(sent_methods(&client).is_empty());
        std::fs::remove_dir_all(root).ok();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlink_to_regular_document_is_allowed_and_open_handle_pins_read_target() {
        use std::os::unix::fs::symlink;

        let root = lifecycle_temp_dir();
        let first = root.join("first-target.ts");
        let second = root.join("second-target.ts");
        let link = root.join("linked.ts");
        std::fs::write(&first, "first\n").unwrap();
        std::fs::write(&second, "second\n").unwrap();
        symlink(&first, &link).unwrap();

        let DocumentOpenObservation::Open(opened) = open_lsp_document(&link).await else {
            panic!("symlink to a regular file must open as a document");
        };
        std::fs::remove_file(&link).unwrap();
        symlink(&second, &link).unwrap();
        assert_eq!(
            read_open_lsp_document(opened).await.unwrap(),
            Some(RuntimeString::from("first\n")),
            "the read must stay on the handle opened before the symlink swap"
        );

        let client = lifecycle_client(&root);
        assert!(client
            .hover(&runtime_path_from_native(&link), 0, 0)
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            sent_methods(&client),
            vec![
                RuntimeString::from("textDocument/didOpen"),
                RuntimeString::from("textDocument/hover"),
            ]
        );
        let messages = client.shared.test_probes.sent_messages.lock().unwrap();
        assert_eq!(
            messages[0]
                .pointer("/params/textDocument/text")
                .and_then(RuntimeJson::as_str),
            Some(&RuntimeString::from("second\n"))
        );
        drop(messages);
        std::fs::remove_dir_all(root).ok();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn fifo_document_is_rejected_without_blocking_or_sending_lsp_messages() {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let root = lifecycle_temp_dir();
        let fifo = root.join("pipe.ts");
        let fifo_c = CString::new(fifo.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(fifo_c.as_ptr(), 0o600) }, 0);
        let client = lifecycle_client(&root);

        assert_eq!(
            tokio::time::timeout(
                Duration::from_secs(1),
                client.hover(&runtime_path_from_native(&fifo), 0, 0),
            )
            .await
            .expect("FIFO eligibility must not block"),
            Err(LspQueryError::UnsupportedFileType)
        );
        assert!(sent_methods(&client).is_empty());
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn delete_wins_after_query_disk_read_without_reopening_deleted_uri() {
        let root = lifecycle_temp_dir();
        let native = root.join("delete-race.ts");
        std::fs::write(&native, "stale\n").unwrap();
        let path = runtime_path_from_native(&native);
        let client = Arc::new(lifecycle_client(&root));
        assert!(client.hover(&path, 0, 0).await.unwrap().is_none());
        let (reached_tx, reached_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = oneshot::channel();
        *client
            .shared
            .test_probes
            .after_document_read
            .lock()
            .unwrap() = Some(DocumentReadBarrier {
            reached: reached_tx,
            resume: resume_rx,
        });

        let query = {
            let client = client.clone();
            let path = path.clone();
            tokio::spawn(async move { client.hover(&path, 0, 0).await })
        };
        reached_rx.await.unwrap();
        std::fs::remove_file(&native).unwrap();
        client.notify_deleted(&path).await;
        resume_tx.send(()).unwrap();
        assert!(query.await.unwrap().unwrap().is_none());

        assert_eq!(
            sent_methods(&client),
            vec![
                RuntimeString::from("textDocument/didOpen"),
                RuntimeString::from("textDocument/hover"),
                RuntimeString::from("textDocument/didClose"),
            ]
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn rename_wins_after_old_query_disk_read_without_reopening_old_uri() {
        let root = lifecycle_temp_dir();
        let old_native = root.join("rename-race-old.ts");
        let new_native = root.join("rename-race-new.ts");
        std::fs::write(&old_native, "moved\n").unwrap();
        let old_path = runtime_path_from_native(&old_native);
        let new_path = runtime_path_from_native(&new_native);
        let client = Arc::new(lifecycle_client(&root));
        assert!(client.hover(&old_path, 0, 0).await.unwrap().is_none());
        let (reached_tx, reached_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = oneshot::channel();
        *client
            .shared
            .test_probes
            .after_document_read
            .lock()
            .unwrap() = Some(DocumentReadBarrier {
            reached: reached_tx,
            resume: resume_rx,
        });

        let query = {
            let client = client.clone();
            let old_path = old_path.clone();
            tokio::spawn(async move { client.hover(&old_path, 0, 0).await })
        };
        reached_rx.await.unwrap();
        std::fs::rename(&old_native, &new_native).unwrap();
        client.notify_deleted(&old_path).await;
        client.notify_edited(&new_path, None).await;
        resume_tx.send(()).unwrap();
        assert!(query.await.unwrap().unwrap().is_none());

        let opened_uris = client
            .shared
            .test_probes
            .sent_messages
            .lock()
            .unwrap()
            .iter()
            .filter(|message| {
                message.get("method").and_then(RuntimeJson::as_str)
                    == Some(&RuntimeString::from("textDocument/didOpen"))
            })
            .filter_map(|message| {
                message
                    .pointer("/params/textDocument/uri")
                    .and_then(RuntimeJson::as_string)
                    .cloned()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            opened_uris,
            vec![
                client.document_uri(&old_path),
                client.document_uri(&new_path),
            ]
        );
        assert_eq!(
            sent_methods(&client),
            vec![
                RuntimeString::from("textDocument/didOpen"),
                RuntimeString::from("textDocument/hover"),
                RuntimeString::from("textDocument/didClose"),
                RuntimeString::from("textDocument/didOpen"),
            ]
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn rename_destination_sync_wins_over_pre_rename_missing_query() {
        let root = lifecycle_temp_dir();
        let old_native = root.join("rename-create-old.ts");
        let new_native = root.join("rename-create-new.ts");
        std::fs::write(&old_native, "created by rename\n").unwrap();
        let old_path = runtime_path_from_native(&old_native);
        let new_path = runtime_path_from_native(&new_native);
        let client = Arc::new(lifecycle_client(&root));
        let (reached_tx, reached_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = oneshot::channel();
        *client
            .shared
            .test_probes
            .after_document_read
            .lock()
            .unwrap() = Some(DocumentReadBarrier {
            reached: reached_tx,
            resume: resume_rx,
        });

        let query = {
            let client = client.clone();
            let new_path = new_path.clone();
            tokio::spawn(async move { client.hover(&new_path, 0, 0).await })
        };
        reached_rx.await.unwrap();
        std::fs::rename(&old_native, &new_native).unwrap();
        client.notify_deleted(&old_path).await;
        client.notify_edited(&new_path, None).await;
        resume_tx.send(()).unwrap();
        assert!(query.await.unwrap().unwrap().is_none());

        assert_eq!(
            sent_methods(&client),
            vec![RuntimeString::from("textDocument/didOpen")]
        );
        let new_uri = client.document_uri(&new_path);
        let messages = client.shared.test_probes.sent_messages.lock().unwrap();
        assert_eq!(
            messages[0]
                .pointer("/params/textDocument/uri")
                .and_then(RuntimeJson::as_str),
            Some(&new_uri)
        );
        drop(messages);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn delete_and_rename_close_old_uri_exactly_once_before_opening_new_uri() {
        let root = lifecycle_temp_dir();
        let old_native = root.join("old.ts");
        let new_native = root.join("new.ts");
        std::fs::write(&old_native, "old\n").unwrap();
        let old_path = runtime_path_from_native(&old_native);
        let new_path = runtime_path_from_native(&new_native);
        let client = lifecycle_client(&root);

        let _ = client.hover(&old_path, 0, 0).await;
        std::fs::rename(&old_native, &new_native).unwrap();
        client.notify_deleted(&old_path).await;
        client.notify_deleted(&old_path).await;
        let _ = client.hover(&new_path, 0, 0).await;

        assert_eq!(
            sent_methods(&client),
            vec![
                RuntimeString::from("textDocument/didOpen"),
                RuntimeString::from("textDocument/hover"),
                RuntimeString::from("textDocument/didClose"),
                RuntimeString::from("textDocument/didOpen"),
                RuntimeString::from("textDocument/hover"),
            ]
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn incremental_sync_replaces_the_exact_previous_document_range() {
        let root = lifecycle_temp_dir();
        let native = root.join("incremental.ts");
        std::fs::write(&native, "a\n\u{1f600}").unwrap();
        let path = runtime_path_from_native(&native);
        let client = lifecycle_client(&root);
        *client.shared.document_sync.lock().unwrap() = DocumentSyncOptions {
            open_close: true,
            change: DocumentChangeSyncKind::Incremental,
        };

        let _ = client.hover(&path, 0, 0).await;
        std::fs::write(&native, "replacement").unwrap();
        let _ = client.hover(&path, 0, 0).await;

        let messages = client.shared.test_probes.sent_messages.lock().unwrap();
        assert_eq!(
            messages[2].pointer("/params/contentChanges/0/range/end"),
            Some(&exact_json!({ "line": 1_u32, "character": 2_u32 }))
        );
        drop(messages);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn structured_sync_honors_open_close_independently_from_change_kind() {
        let root = lifecycle_temp_dir();
        let native = root.join("structured.ts");
        std::fs::write(&native, "first").unwrap();
        let path = runtime_path_from_native(&native);
        let client = lifecycle_client(&root);
        *client.shared.document_sync.lock().unwrap() = DocumentSyncOptions {
            open_close: false,
            change: DocumentChangeSyncKind::Full,
        };

        let _ = client.hover(&path, 0, 0).await;
        std::fs::write(&native, "second").unwrap();
        let _ = client.hover(&path, 0, 0).await;
        client.notify_deleted(&path).await;

        assert_eq!(
            sent_methods(&client),
            vec![
                RuntimeString::from("textDocument/hover"),
                RuntimeString::from("textDocument/didChange"),
                RuntimeString::from("textDocument/hover"),
            ]
        );
        std::fs::remove_dir_all(root).ok();
    }

    // ---- conversions ----

    #[test]
    fn to_diagnostic_maps_fields() {
        let d = exact_json!({
            "range": {"start": {"line": 3, "character": 4}, "end": {"line": 3, "character": 10}},
            "severity": 2, "message": "oops", "source": "tsserver", "code": 2304
        });
        let out = to_diagnostic(&d);
        assert_eq!(out.line, 3);
        assert_eq!(out.character, 4);
        assert_eq!(out.end_line, Some(3));
        assert_eq!(out.end_character, Some(10));
        assert_eq!(out.severity, LspSeverity::Warning);
        assert_eq!(out.message, "oops");
        assert_eq!(out.source.as_ref(), Some(&RuntimeString::from("tsserver")));
        assert_eq!(out.code.as_ref(), Some(&RuntimeString::from("2304")));
    }

    #[test]
    fn to_diagnostic_defaults_severity_and_markup_message() {
        let d = exact_json!({
            "range": {"start": {"line": 0, "character": 0}, "end": {"line": 0, "character": 1}},
            "message": {"kind": "markdown", "value": "m"}
        });
        let out = to_diagnostic(&d);
        assert_eq!(out.severity, LspSeverity::Error);
        assert_eq!(out.message, "m");
        assert!(out.source.is_none());
        assert!(out.code.is_none());
    }

    #[test]
    fn hover_text_variants() {
        assert_eq!(hover_text(&exact_json!({"contents": "hi"})), "hi");
        assert_eq!(
            hover_text(&exact_json!({"contents": ["a", {"value": "b"}]})),
            "a\nb"
        );
        assert_eq!(
            hover_text(&exact_json!({"contents": {"kind": "markdown", "value": "x"}})),
            "x"
        );
        assert_eq!(hover_text(&exact_json!({"contents": 42})), "");
    }

    #[test]
    fn to_symbols_recurses_and_maps_kind() {
        let syms = vec![exact_json!({
            "name": "Foo", "kind": 5, "detail": "class Foo",
            "selectionRange": {"start": {"line": 0, "character": 6}, "end": {"line": 0, "character": 9}},
            "children": [{
                "name": "bar", "kind": 6,
                "selectionRange": {"start": {"line": 1, "character": 2}, "end": {"line": 1, "character": 5}}
            }]
        })];
        let out = to_symbols(&syms);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].name, "Foo");
        assert_eq!(out[0].kind, "class");
        assert_eq!(
            out[0].detail.as_ref(),
            Some(&RuntimeString::from("class Foo"))
        );
        assert_eq!(out[0].line, 0);
        assert_eq!(out[1].name, "bar");
        assert_eq!(out[1].kind, "method");
        assert_eq!(out[1].line, 1);
    }

    #[test]
    fn to_symbols_symbolinformation_and_unknown_kind() {
        let syms = vec![exact_json!({
            "name": "g", "kind": 99,
            "location": {"uri": "file:///x", "range": {"start": {"line": 4, "character": 0}, "end": {"line": 4, "character": 1}}}
        })];
        let out = to_symbols(&syms);
        assert_eq!(out[0].kind, "99");
        assert_eq!(out[0].line, 4);
        assert!(out[0].detail.is_none());
    }

    #[test]
    fn severity_from_maps_and_defaults() {
        assert_eq!(severity_from(1), LspSeverity::Error);
        assert_eq!(severity_from(2), LspSeverity::Warning);
        assert_eq!(severity_from(3), LspSeverity::Information);
        assert_eq!(severity_from(4), LspSeverity::Hint);
        assert_eq!(severity_from(9), LspSeverity::Error);
    }

    #[test]
    fn symbol_kind_maps_known_and_unknown() {
        assert_eq!(symbol_kind_str(Some(&exact_json!(5))), "class");
        assert_eq!(symbol_kind_str(Some(&exact_json!(26))), "typeParameter");
        assert_eq!(symbol_kind_str(Some(&exact_json!(100))), "100");
        assert_eq!(symbol_kind_str(None), "undefined");
    }

    // ---- file URL (unix path shapes) ----

    #[cfg(unix)]
    #[test]
    fn path_file_url_round_trip() {
        assert_eq!(
            path_to_file_url(&RuntimePath::from("/Users/x/foo bar.ts")),
            "file:///Users/x/foo%20bar.ts"
        );
        assert_eq!(
            file_url_to_path(&RuntimeString::from("file:///Users/x/foo%20bar.ts")).unwrap(),
            "/Users/x/foo bar.ts"
        );
    }

    #[cfg(unix)]
    #[test]
    fn exact_utf16_file_uri_and_jsonrpc_frame_round_trip() {
        let cases = [
            RuntimeString::from_utf16(vec![0xd800]),
            RuntimeString::from_utf16(vec![0xdc00]),
            RuntimeString::from_utf16(vec![0xfffd]),
        ];
        let mut uris = Vec::new();
        let mut payloads = Vec::new();

        for marker in cases {
            let path = RuntimePath::from(
                RuntimeString::from("/tmp/")
                    .concat(&marker)
                    .concat(&RuntimeString::from(".ts")),
            );
            let uri = path_to_file_url(&path);
            assert_eq!(
                file_url_to_path(&uri),
                Some(path.as_runtime_string().clone())
            );

            let message = exact_json!({
                "jsonrpc": "2.0",
                "method": "test/exact",
                "params": { "text": (marker.clone()), "uri": (uri.clone()) },
            });
            let mut payload = Vec::new();
            runtime_json::write_to(&message, &mut payload).unwrap();
            let frame = encode_frame(&payload);
            let mut decoder = FrameDecoder::new();
            decoder.push(&frame);
            let decoded = decoder.next_message().unwrap();
            let decoded_text = RuntimeString::from(std::str::from_utf8(&decoded).unwrap());
            let parsed = runtime_json::parse(&decoded_text).unwrap();
            assert_eq!(
                parsed
                    .pointer("/params/text")
                    .and_then(RuntimeJson::as_string),
                Some(&marker)
            );
            assert_eq!(
                parsed
                    .pointer("/params/uri")
                    .and_then(RuntimeJson::as_string),
                Some(&uri)
            );
            uris.push(uri);
            payloads.push(payload);
        }

        assert_ne!(uris[0], uris[1]);
        assert_ne!(uris[0], uris[2]);
        assert_ne!(uris[1], uris[2]);
        assert_ne!(payloads[0], payloads[1]);
        assert_ne!(payloads[0], payloads[2]);
        assert_ne!(payloads[1], payloads[2]);
    }

    #[cfg(unix)]
    #[test]
    fn file_url_to_path_safe_passes_non_file() {
        assert_eq!(
            file_url_to_path_safe(&RuntimeString::from("untitled:foo")),
            "untitled:foo"
        );
        assert_eq!(
            file_url_to_path_safe(&RuntimeString::from("file:///a.ts")),
            "/a.ts"
        );
    }

    #[cfg(unix)]
    #[test]
    fn to_locations_handles_location_and_link() {
        let loc = exact_json!({"uri": "file:///a.ts", "range": {"start": {"line": 1, "character": 2}, "end": {"line": 1, "character": 3}}});
        let out = to_locations(Some(loc));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].path, "/a.ts");
        assert_eq!(out[0].line, 1);
        assert_eq!(out[0].end_character, Some(3));

        let link = exact_json!([{"targetUri": "file:///b.ts", "targetSelectionRange": {"start": {"line": 5, "character": 0}, "end": {"line": 5, "character": 4}}}]);
        let out2 = to_locations(Some(link));
        assert_eq!(out2[0].path, "/b.ts");
        assert_eq!(out2[0].line, 5);

        assert!(to_locations(None).is_empty());
        assert!(to_locations(Some(RuntimeJson::Null)).is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn to_workspace_symbols_keeps_path_and_container() {
        let syms = vec![
            exact_json!({"name": "F", "kind": 5, "containerName": "mod", "location": {"uri": "file:///a.ts", "range": {"start": {"line": 2, "character": 1}, "end": {"line": 2, "character": 2}}}}),
            exact_json!({"name": "NoLoc", "kind": 12}),
        ];
        let out = to_workspace_symbols(&syms);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].name, "F");
        assert_eq!(out[0].path, "/a.ts");
        assert_eq!(
            out[0].container_name.as_ref(),
            Some(&RuntimeString::from("mod"))
        );
        assert_eq!(out[0].line, 2);
    }

    #[cfg(unix)]
    #[test]
    fn to_call_hierarchy_item_maps() {
        let item = exact_json!({"name": "fn", "kind": 12, "detail": "fn()", "uri": "file:///c.ts", "range": {"start": {"line": 7, "character": 3}, "end": {"line": 7, "character": 9}}});
        let out = to_call_hierarchy_item(&item);
        assert_eq!(out.name, "fn");
        assert_eq!(out.kind, "function");
        assert_eq!(out.path, "/c.ts");
        assert_eq!(out.line, 7);
        assert_eq!(out.detail.as_ref(), Some(&RuntimeString::from("fn()")));
    }
}
