use crate::network_proxy::ManagedNetworkSandboxContext;
use crate::runtime_process::{
    ParentEnvSnapshot, RuntimeChildPtyReader, RuntimeChildPtyWriter, RuntimeChildStderr,
    RuntimeChildStdin, RuntimeChildStdout, RuntimeEnv, RuntimeProcessContext,
    RuntimeProcessController, RuntimeProcessSurface, RuntimeSignal, RuntimeSpawnRequest,
    SpawnedRuntimeProcess,
};
use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, NodeChunkUtf8, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode,
    RuntimeEncode, RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimePath, RuntimeString,
};
use crate::tools::shell::Signal;
use crate::types::abort::AbortSignal;
use crate::types::sandbox::{ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec};
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{broadcast, watch, Mutex as AsyncMutex};

const DEFAULT_RETAINED_OUTPUT_BYTES: usize = 256 * 1024;
const DEFAULT_EVENT_CAPACITY: usize = 1024;
const ACCEPTED_WRITE_ID_CAPACITY: usize = 1024;
/// Hard bound on how many process entries one registry holds. Reaching the cap
/// makes a new start evict the least-recently-used unprotected session instead
/// of letting abandoned sessions accumulate without limit.
pub const MAX_RETAINED_PROCESSES: usize = 64;
/// The most recently used entries that pruning never evicts. An in-flight
/// interaction always sits inside this window because every read and write
/// refreshes recency.
const PRUNE_PROTECT_RECENT: usize = 8;
#[cfg(test)]
const DEFAULT_EXITED_PROCESS_RETENTION: Duration = Duration::from_millis(25);
#[cfg(not(test))]
const DEFAULT_EXITED_PROCESS_RETENTION: Duration = Duration::from_secs(30);

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

type ProcessResult<T> = Result<T, RuntimeString>;

pub const PROCESS_START_METHOD: &str = "process/start";
pub const PROCESS_READ_METHOD: &str = "process/read";
pub const PROCESS_WRITE_METHOD: &str = "process/write";
pub const PROCESS_SIGNAL_METHOD: &str = "process/signal";
pub const PROCESS_TERMINATE_METHOD: &str = "process/terminate";
pub const PROCESS_OUTPUT_METHOD: &str = "process/output";
pub const PROCESS_EXITED_METHOD: &str = "process/exited";
pub const PROCESS_CLOSED_METHOD: &str = "process/closed";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExecOutputStream {
    Stdout,
    Stderr,
    Pty,
}

impl RuntimeEncode for ExecOutputStream {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
            Self::Pty => "pty",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for ExecOutputStream {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("stdout", &[]),
                RuntimeEnumVariant::new("stderr", &[]),
                RuntimeEnumVariant::new("pty", &[]),
            ],
            &[Self::Stdout, Self::Stderr, Self::Pty],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ProcessSignal {
    Interrupt,
}

impl RuntimeEncode for ProcessSignal {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Interrupt => "interrupt",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for ProcessSignal {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[RuntimeEnumVariant::new("interrupt", &[])],
            &[Self::Interrupt],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalSize {
    pub rows: u16,
    pub cols: u16,
}

impl RuntimeEncode for TerminalSize {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self { rows, cols } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("rows", rows)?;
        object.field("cols", cols)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for TerminalSize {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                rows: object.required("rows", &[])?,
                cols: object.required("cols", &[])?,
            })
        })
    }
}

impl Default for TerminalSize {
    fn default() -> Self {
        Self { rows: 24, cols: 80 }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessOutputChunk {
    pub seq: u64,
    pub stream: ExecOutputStream,
    pub chunk: Vec<u8>,
}

impl RuntimeEncode for ProcessOutputChunk {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("seq", &self.seq)?;
        object.field("stream", &self.stream)?;
        object.field(
            "chunk",
            &RuntimeString::from(base64::engine::general_purpose::STANDARD.encode(&self.chunk)),
        )?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ProcessOutputChunk {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            let encoded: RuntimeString = object.required("chunk", &[])?;
            let encoded = encoded.try_to_utf8().map_err(|_| {
                RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(RuntimeString::from(
                    "process output chunk base64 must be Unicode scalar text",
                )))
            })?;
            let chunk = base64::engine::general_purpose::STANDARD
                .decode(encoded.as_bytes())
                .map_err(|error| {
                    RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(RuntimeString::from(
                        error.to_string(),
                    )))
                })?;
            Ok(Self {
                seq: object.required("seq", &[])?,
                stream: object.required("stream", &[])?,
                chunk,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessEvent {
    Output(ProcessOutputChunk),
    Exited { seq: u64, exit_code: Option<i32> },
    Closed { seq: u64 },
    Failed(RuntimeString),
}

impl ProcessEvent {
    fn retained_len(&self) -> usize {
        match self {
            ProcessEvent::Output(chunk) => chunk.chunk.len(),
            ProcessEvent::Failed(message) => message.to_node_utf8().len(),
            ProcessEvent::Exited { .. } | ProcessEvent::Closed { .. } => 0,
        }
    }
}

#[derive(Clone)]
pub struct ProcessEventLog {
    inner: Arc<ProcessEventLogInner>,
}

struct ProcessEventLogInner {
    history: Mutex<ProcessEventHistory>,
    live_tx: broadcast::Sender<ProcessEvent>,
    event_capacity: usize,
    byte_capacity: usize,
}

#[derive(Default)]
struct ProcessEventHistory {
    events: VecDeque<ProcessEvent>,
    retained_bytes: usize,
}

impl ProcessEventLog {
    fn new(event_capacity: usize, byte_capacity: usize) -> Self {
        let (live_tx, _live_rx) = broadcast::channel(event_capacity);
        Self {
            inner: Arc::new(ProcessEventLogInner {
                history: Mutex::new(ProcessEventHistory::default()),
                live_tx,
                event_capacity,
                byte_capacity,
            }),
        }
    }

    fn publish(&self, event: ProcessEvent) {
        let mut history = self.history();
        history.retained_bytes += event.retained_len();
        history.events.push_back(event.clone());
        while history.events.len() > self.inner.event_capacity
            || history.retained_bytes > self.inner.byte_capacity
        {
            let Some(evicted) = history.events.pop_front() else {
                break;
            };
            history.retained_bytes = history
                .retained_bytes
                .saturating_sub(evicted.retained_len());
        }
        let _ = self.inner.live_tx.send(event);
    }

    pub fn subscribe(&self) -> ProcessEventReceiver {
        let history = self.history();
        let replay = history.events.iter().cloned().collect();
        ProcessEventReceiver {
            replay,
            live_rx: self.inner.live_tx.subscribe(),
        }
    }

    fn history(&self) -> std::sync::MutexGuard<'_, ProcessEventHistory> {
        self.inner
            .history
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

pub struct ProcessEventReceiver {
    replay: VecDeque<ProcessEvent>,
    live_rx: broadcast::Receiver<ProcessEvent>,
}

impl ProcessEventReceiver {
    pub async fn recv(&mut self) -> Result<ProcessEvent, broadcast::error::RecvError> {
        if let Some(event) = self.replay.pop_front() {
            return Ok(event);
        }
        self.live_rx.recv().await
    }
}

pub struct LocalProcessStartParams {
    pub process_id: Option<RuntimeString>,
    pub argv: Vec<RuntimeString>,
    pub cwd: RuntimePath,
    pub sandbox: Arc<dyn SandboxBackend>,
    pub env: Option<RuntimeEnv>,
    pub network_allowed: Option<bool>,
    pub managed_network: Option<ManagedNetworkSandboxContext>,
    pub writable_roots: Option<Vec<RuntimeString>>,
    pub tty: bool,
    pub pipe_stdin: bool,
    pub cols: Option<u16>,
    pub rows: Option<u16>,
    pub yield_ms: Option<i64>,
    pub max_output_bytes: Option<usize>,
    pub signal: Option<AbortSignal>,
}

impl Default for LocalProcessStartParams {
    fn default() -> Self {
        Self {
            process_id: None,
            argv: Vec::new(),
            cwd: RuntimePath::default(),
            sandbox: Arc::new(PassthroughSandbox),
            env: None,
            network_allowed: None,
            managed_network: None,
            writable_roots: None,
            tty: false,
            pipe_stdin: false,
            cols: None,
            rows: None,
            yield_ms: None,
            max_output_bytes: None,
            signal: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalProcessStartResult {
    pub process_id: RuntimeString,
}

#[derive(Debug, Clone)]
pub struct LocalProcessWriteParams {
    pub process_id: RuntimeString,
    pub write_id: RuntimeString,
    pub chunk: Vec<u8>,
    pub yield_ms: Option<i64>,
    pub signal: Option<AbortSignal>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalProcessWriteResponse {
    pub status: WriteStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum WriteStatus {
    Accepted,
    UnknownProcess,
    Starting,
    StdinClosed,
}

impl RuntimeEncode for WriteStatus {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Accepted => "accepted",
            Self::UnknownProcess => "unknownProcess",
            Self::Starting => "starting",
            Self::StdinClosed => "stdinClosed",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for WriteStatus {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("accepted", &[]),
                RuntimeEnumVariant::new("unknownProcess", &[]),
                RuntimeEnumVariant::new("starting", &[]),
                RuntimeEnumVariant::new("stdinClosed", &[]),
            ],
            &[
                Self::Accepted,
                Self::UnknownProcess,
                Self::Starting,
                Self::StdinClosed,
            ],
        )
    }
}

#[derive(Debug, Clone)]
pub struct LocalProcessReadParams {
    pub process_id: RuntimeString,
    pub after_seq: Option<u64>,
    /// Cumulative output byte position the caller has already consumed. When
    /// present, the response reports bytes evicted between that position and
    /// the first returned chunk.
    pub after_offset: Option<u64>,
    pub max_bytes: Option<usize>,
    pub wait_ms: Option<u64>,
    pub signal: Option<AbortSignal>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalProcessReadResponse {
    pub chunks: Vec<ProcessOutputChunk>,
    pub next_seq: u64,
    /// Bytes evicted from the retained middle between the requested position
    /// and the first returned chunk. Always zero unless the caller tracks a
    /// byte offset.
    pub dropped_bytes: u64,
    pub exited: bool,
    pub exit_code: Option<i32>,
    pub closed: bool,
    pub failure: Option<RuntimeString>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalProcessTerminateResponse {
    pub running: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalProcessInfo {
    pub process_id: RuntimeString,
    pub cwd: RuntimePath,
    pub argv: Vec<RuntimeString>,
    pub exited: bool,
    pub exit_code: Option<i32>,
    pub closed: bool,
    pub terminal: bool,
    pub terminal_size: Option<TerminalSize>,
}

#[derive(Default)]
struct PassthroughSandbox;

impl SandboxBackend for PassthroughSandbox {
    fn kind(&self) -> SandboxKind {
        SandboxKind::Process
    }

    fn is_available(&self) -> bool {
        true
    }

    fn transform(&self, req: &ExecRequest, _policy: &SandboxPolicy) -> SpawnSpec {
        let file = req.argv.first().cloned().unwrap_or_default();
        let args = req.argv.iter().skip(1).cloned().collect();
        let mut env = ParentEnvSnapshot::capture();
        if let Some(extra) = &req.env {
            env.overlay(extra);
        }
        crate::fs::ripgrep::ensure_ripgrep_on_path(&mut env);
        SpawnSpec {
            file,
            args,
            cwd: req.cwd.clone(),
            env,
        }
    }
}

#[derive(Default)]
struct AcceptedWriteIds {
    order: VecDeque<RuntimeString>,
    set: HashSet<RuntimeString>,
}

impl AcceptedWriteIds {
    fn contains(&self, write_id: &RuntimeString) -> bool {
        self.set.contains(write_id)
    }

    fn remember(&mut self, write_id: RuntimeString) {
        if self.set.insert(write_id.clone()) {
            self.order.push_back(write_id);
        }
        while self.order.len() > ACCEPTED_WRITE_ID_CAPACITY {
            let Some(evicted) = self.order.pop_front() else {
                break;
            };
            self.set.remove(&evicted);
        }
    }
}

enum ProcessInput {
    Pipe(RuntimeChildStdin),
    Pty(RuntimeChildPtyWriter),
}

impl ProcessInput {
    async fn write_all(&mut self, chunk: &[u8]) -> std::io::Result<()> {
        match self {
            Self::Pipe(stdin) => stdin.write_all(chunk).await,
            Self::Pty(writer) => writer.write_all(chunk).await,
        }
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Pipe(stdin) => stdin.flush().await,
            Self::Pty(writer) => writer.flush().await,
        }
    }

    fn resize(&self, size: TerminalSize) -> std::io::Result<bool> {
        match self {
            Self::Pipe(_) => Ok(false),
            Self::Pty(writer) => {
                writer.resize(size.rows, size.cols, 0, 0)?;
                Ok(true)
            }
        }
    }
}

enum ProcessOutputTransport {
    Pipes(RuntimeChildStdout, RuntimeChildStderr),
    Pty(RuntimeChildPtyReader),
}

/// A retained output chunk plus the cumulative byte position where it starts.
/// The offset lets a read report the exact size of an evicted middle span when
/// a lagging cursor crosses it, so callers can render the loss instead of
/// guessing from sequence jumps.
struct RetainedChunk {
    chunk: ProcessOutputChunk,
    start_offset: u64,
}

struct ProcessEntry {
    argv: Vec<RuntimeString>,
    cwd: RuntimePath,
    input: Option<Arc<AsyncMutex<ProcessInput>>>,
    controller: RuntimeProcessController,
    terminal_size: Option<TerminalSize>,
    pipe_stdin: bool,
    /// Stable oldest prefix of the output, never evicted, so a reader starting
    /// from the beginning still sees how the stream began after the middle was
    /// dropped. Filled to half of the retention budget (chunk-granular: the
    /// boundary chunk may overhang by less than one chunk).
    head_chunks: Vec<RetainedChunk>,
    /// Most recent suffix of the output; the oldest tail chunks are evicted
    /// once the tail budget is exceeded.
    tail_chunks: VecDeque<RetainedChunk>,
    head_bytes: usize,
    tail_bytes: usize,
    head_budget: usize,
    tail_budget: usize,
    /// Cumulative bytes ever produced by the process.
    produced_bytes: u64,
    /// Cumulative bytes evicted from the middle between the head and tail spans.
    evicted_bytes: u64,
    next_seq: u64,
    exit_code: Option<i32>,
    exited: bool,
    closed: bool,
    failure: Option<RuntimeString>,
    accepted_write_ids: AcceptedWriteIds,
    events: ProcessEventLog,
    wake_tx: watch::Sender<u64>,
    /// Held for the duration of a registry read/write/resize call. Pruning only
    /// evicts entries whose lock is free, so an in-flight interaction never has
    /// its process killed underneath it.
    interaction: Arc<AsyncMutex<()>>,
    /// Logical recency stamp (registry-wide monotonic tick) driving eviction
    /// order and the most-recently-used protection window.
    last_used: u64,
}

impl ProcessEntry {
    #[allow(clippy::too_many_arguments)]
    fn new(
        argv: Vec<RuntimeString>,
        cwd: RuntimePath,
        input: Option<Arc<AsyncMutex<ProcessInput>>>,
        controller: RuntimeProcessController,
        terminal_size: Option<TerminalSize>,
        pipe_stdin: bool,
        retained_byte_limit: usize,
        last_used: u64,
    ) -> Self {
        let (wake_tx, _wake_rx) = watch::channel(0);
        let head_budget = retained_byte_limit / 2;
        Self {
            argv,
            cwd,
            input,
            controller,
            terminal_size,
            pipe_stdin,
            head_chunks: Vec::new(),
            tail_chunks: VecDeque::new(),
            head_bytes: 0,
            tail_bytes: 0,
            head_budget,
            tail_budget: retained_byte_limit.saturating_sub(head_budget),
            produced_bytes: 0,
            evicted_bytes: 0,
            next_seq: 1,
            exit_code: None,
            exited: false,
            closed: false,
            failure: None,
            accepted_write_ids: AcceptedWriteIds::default(),
            events: ProcessEventLog::new(DEFAULT_EVENT_CAPACITY, retained_byte_limit),
            wake_tx,
            interaction: Arc::new(AsyncMutex::new(())),
            last_used,
        }
    }

    fn push_output(&mut self, stream: ExecOutputStream, bytes: Vec<u8>) {
        if bytes.is_empty() {
            return;
        }
        let start_offset = self.produced_bytes;
        self.produced_bytes = self.produced_bytes.saturating_add(bytes.len() as u64);
        let seq = self.next_seq;
        let chunk = ProcessOutputChunk {
            seq,
            stream,
            chunk: bytes,
        };
        self.next_seq = self.next_seq.saturating_add(1);
        let _ = self.wake_tx.send(seq);
        self.events.publish(ProcessEvent::Output(chunk.clone()));
        let retained = RetainedChunk {
            chunk,
            start_offset,
        };
        let len = retained.chunk.chunk.len();
        if self.head_bytes < self.head_budget {
            self.head_bytes += len;
            self.head_chunks.push(retained);
        } else {
            self.tail_bytes += len;
            self.tail_chunks.push_back(retained);
            while self.tail_bytes > self.tail_budget {
                let Some(evicted) = self.tail_chunks.pop_front() else {
                    break;
                };
                let evicted_len = evicted.chunk.chunk.len();
                self.tail_bytes = self.tail_bytes.saturating_sub(evicted_len);
                self.evicted_bytes = self.evicted_bytes.saturating_add(evicted_len as u64);
            }
        }
    }

    fn mark_failed(&mut self, message: RuntimeString) {
        if self.failure.is_some() {
            return;
        }
        self.failure = Some(message.clone());
        let seq = self.next_seq;
        self.next_seq = self.next_seq.saturating_add(1);
        let _ = self.wake_tx.send(seq);
        self.events.publish(ProcessEvent::Failed(message));
    }

    fn mark_exited(&mut self, exit_code: Option<i32>) {
        if self.exited {
            return;
        }
        self.exited = true;
        self.exit_code = exit_code;
        let seq = self.next_seq;
        self.next_seq = self.next_seq.saturating_add(1);
        let _ = self.wake_tx.send(seq);
        self.events.publish(ProcessEvent::Exited { seq, exit_code });
    }

    fn mark_closed(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        let seq = self.next_seq;
        self.next_seq = self.next_seq.saturating_add(1);
        let _ = self.wake_tx.send(seq);
        self.events.publish(ProcessEvent::Closed { seq });
    }

    fn read_from(
        &self,
        after_seq: u64,
        after_offset: Option<u64>,
        max_bytes: Option<usize>,
    ) -> LocalProcessReadResponse {
        let mut chunks = Vec::new();
        let mut total_bytes = 0usize;
        let mut next_seq = self.next_seq;
        let mut dropped_bytes = 0u64;
        // Track the cumulative end offset of what the caller consumed so a
        // returned span that crosses the evicted middle can report its exact
        // size. Callers that do not track a byte offset (the process wire
        // surface) see zero and rely on sequence jumps as before.
        let mut consumed_end = after_offset.unwrap_or(0);
        for retained in self.head_chunks.iter().chain(self.tail_chunks.iter()) {
            let chunk = &retained.chunk;
            if chunk.seq <= after_seq {
                continue;
            }
            let chunk_len = chunk.chunk.len();
            if max_bytes.is_some_and(|max| !chunks.is_empty() && total_bytes + chunk_len > max) {
                break;
            }
            if after_offset.is_some() {
                dropped_bytes = dropped_bytes
                    .saturating_add(retained.start_offset.saturating_sub(consumed_end));
                consumed_end = retained.start_offset.saturating_add(chunk_len as u64);
            }
            total_bytes += chunk_len;
            chunks.push(chunk.clone());
            if max_bytes.is_some() {
                next_seq = chunk.seq.saturating_add(1);
            }
            if max_bytes.is_some_and(|max| total_bytes >= max) {
                break;
            }
        }
        LocalProcessReadResponse {
            chunks,
            next_seq,
            dropped_bytes,
            exited: self.exited,
            exit_code: self.exit_code,
            closed: self.closed,
            failure: self.failure.clone(),
        }
    }
}

pub struct LocalExecProcessRegistry {
    processes: Arc<Mutex<HashMap<RuntimeString, ProcessEntry>>>,
    next_process_id: AtomicU64,
    last_used_clock: AtomicU64,
    exited_process_retention: Duration,
}

impl Default for LocalExecProcessRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalExecProcessRegistry {
    pub fn new() -> Self {
        Self::with_exited_process_retention(DEFAULT_EXITED_PROCESS_RETENTION)
    }

    pub fn with_exited_process_retention(exited_process_retention: Duration) -> Self {
        Self {
            processes: Arc::new(Mutex::new(HashMap::new())),
            next_process_id: AtomicU64::new(1),
            last_used_clock: AtomicU64::new(0),
            exited_process_retention,
        }
    }

    pub async fn start(
        &self,
        process_context: RuntimeProcessContext,
        params: LocalProcessStartParams,
    ) -> ProcessResult<LocalProcessStartResult> {
        let process_id = params.process_id.unwrap_or_else(|| {
            RuntimeString::from_utf8(&format!(
                "process-{}",
                self.next_process_id.fetch_add(1, Ordering::Relaxed)
            ))
        });
        if params.argv.is_empty() {
            return Err(RuntimeString::from("argv must not be empty"));
        }
        if params
            .signal
            .as_ref()
            .is_some_and(AbortSignal::is_cancelled)
        {
            return Err(RuntimeString::from("process start aborted"));
        }
        {
            let processes = self.processes();
            if processes.contains_key(&process_id) {
                return Err(RuntimeString::from("process ")
                    .concat(&process_id)
                    .concat(&RuntimeString::from(" already exists")));
            }
        }

        let terminal_size = params.tty.then_some(TerminalSize {
            cols: params.cols.unwrap_or(80).clamp(1, 10_000),
            rows: params.rows.unwrap_or(24).clamp(1, 10_000),
        });
        let policy = SandboxPolicy {
            writable_roots: params
                .writable_roots
                .clone()
                .unwrap_or_else(|| vec![params.cwd.as_runtime_string().clone()]),
            network_allowed: if params.managed_network.is_some() {
                false
            } else {
                params.network_allowed.unwrap_or(false)
            },
            allow_loopback_ports: params
                .managed_network
                .as_ref()
                .map(|network| network.loopback_ports.clone()),
            allow_unix_sockets: params.managed_network.as_ref().map(|network| {
                network
                    .allow_unix_sockets
                    .iter()
                    .map(RuntimeString::from)
                    .collect()
            }),
            allow_all_unix_sockets: params
                .managed_network
                .as_ref()
                .map(|network| network.allow_all_unix_sockets),
            windows_network_proxy_restricting_sid: params
                .managed_network
                .as_ref()
                .and_then(|network| network.network_proxy_restricting_sid.clone()),
            windows_network_allow_local_binding: params
                .managed_network
                .as_ref()
                .map(|network| network.allow_local_binding),
            ..Default::default()
        };
        let request = ExecRequest {
            argv: params.argv.clone(),
            cwd: params.cwd,
            env: params.env,
            timeout_ms: None,
        };
        let spec = params.sandbox.transform(&request, &policy);
        let surface = RuntimeProcessSurface::DirectExec {
            tty: terminal_size.is_some(),
            pipe_stdin: params.pipe_stdin,
            rows: terminal_size.map_or(24, |size| size.rows),
            columns: terminal_size.map_or(80, |size| size.cols),
        };
        let spawner = process_context.spawner();
        let mut child = spawner
            .spawn(RuntimeSpawnRequest::from_spawn_spec(spec, surface))
            .await
            .map_err(|error| error.message().clone())?;
        let controller = child.controller();
        let (input, transport) = if terminal_size.is_some() {
            let pty = child
                .take_pty()
                .ok_or_else(|| RuntimeString::from("runtime PTY endpoint missing"))?;
            let (reader, writer) = pty
                .split()
                .map_err(|error| RuntimeString::from_utf8(&error.to_string()))?;
            (
                Some(Arc::new(AsyncMutex::new(ProcessInput::Pty(writer)))),
                ProcessOutputTransport::Pty(reader),
            )
        } else {
            let stdout = child
                .take_stdout()
                .ok_or_else(|| RuntimeString::from("runtime stdout endpoint missing"))?;
            let stderr = child
                .take_stderr()
                .ok_or_else(|| RuntimeString::from("runtime stderr endpoint missing"))?;
            let input = child
                .take_stdin()
                .map(ProcessInput::Pipe)
                .map(|input| Arc::new(AsyncMutex::new(input)));
            (input, ProcessOutputTransport::Pipes(stdout, stderr))
        };

        let entry = ProcessEntry::new(
            params.argv,
            request.cwd,
            input,
            controller,
            terminal_size,
            params.pipe_stdin,
            params
                .max_output_bytes
                .unwrap_or(DEFAULT_RETAINED_OUTPUT_BYTES),
            self.next_last_used(),
        );
        let pruned = {
            let mut processes = self.processes();
            let pruned = Self::prune_one_if_needed(&mut processes);
            processes.insert(process_id.clone(), entry);
            pruned
        };
        if let Some((_pruned_id, pruned_controller)) = pruned {
            // An evicted session is torn down, not leaked: its subscribers
            // already saw the terminal transition under the table lock, so
            // only the child process itself remains to stop.
            pruned_controller.signal(RuntimeSignal::Kill);
        }
        let mut wake = self.subscribe_wake(&process_id)?;
        let processes = Arc::clone(&self.processes);
        let task_process_id = process_id.clone();
        let retention = self.exited_process_retention;
        tokio::spawn(async move {
            match transport {
                ProcessOutputTransport::Pipes(stdout, stderr) => {
                    drain_pipe_process(
                        child,
                        stdout,
                        stderr,
                        processes,
                        task_process_id,
                        retention,
                    )
                    .await;
                }
                ProcessOutputTransport::Pty(reader) => {
                    drain_pty_process(child, reader, processes, task_process_id, retention).await;
                }
            }
        });

        let yield_ms = params.yield_ms.unwrap_or(250).clamp(0, 30_000) as u64;
        if yield_ms > 0 {
            let deadline = tokio::time::Instant::now() + Duration::from_millis(yield_ms);
            loop {
                if self
                    .info(&process_id)
                    .is_none_or(|info| info.exited || info.closed)
                {
                    break;
                }
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    break;
                }
                if let Some(signal) = params.signal.as_ref() {
                    tokio::select! {
                        _ = wake.changed() => {}
                        _ = tokio::time::sleep(remaining) => break,
                        _ = signal.cancelled() => break,
                    }
                } else {
                    tokio::select! {
                        _ = wake.changed() => {}
                        _ = tokio::time::sleep(remaining) => break,
                    }
                }
            }
        }
        Ok(LocalProcessStartResult { process_id })
    }

    pub async fn read(
        &self,
        params: LocalProcessReadParams,
    ) -> ProcessResult<LocalProcessReadResponse> {
        // Serialize reads and writes against one process, and keep pruning away
        // from a session with an interaction in progress.
        let interaction = {
            let processes = self.processes();
            let Some(entry) = processes.get(&params.process_id) else {
                return Err(RuntimeString::from("unknown process id ").concat(&params.process_id));
            };
            Arc::clone(&entry.interaction)
        };
        let _interaction_guard = interaction.lock().await;
        self.touch_last_used(&params.process_id);
        let deadline =
            tokio::time::Instant::now() + Duration::from_millis(params.wait_ms.unwrap_or_default());
        loop {
            let (response, mut wake) = {
                let processes = self.processes();
                let Some(entry) = processes.get(&params.process_id) else {
                    return Err(
                        RuntimeString::from("unknown process id ").concat(&params.process_id)
                    );
                };
                (
                    entry.read_from(
                        params.after_seq.unwrap_or_default(),
                        params.after_offset,
                        params.max_bytes,
                    ),
                    entry.wake_tx.subscribe(),
                )
            };
            if !response.chunks.is_empty()
                || response.exited
                || response.closed
                || tokio::time::Instant::now() >= deadline
            {
                return Ok(response);
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if let Some(signal) = params.signal.as_ref() {
                tokio::select! {
                    _ = wake.changed() => {}
                    _ = tokio::time::sleep(remaining) => {}
                    _ = signal.cancelled() => return Err(RuntimeString::from("process read aborted")),
                }
            } else {
                tokio::select! {
                    _ = wake.changed() => {}
                    _ = tokio::time::sleep(remaining) => {}
                }
            }
        }
    }

    pub async fn write(
        &self,
        params: LocalProcessWriteParams,
    ) -> ProcessResult<LocalProcessWriteResponse> {
        if params.write_id.is_empty() {
            return Err(RuntimeString::from("writeId must not be empty"));
        }
        // Serialize with reads and with pruning on the same process.
        let interaction = {
            let processes = self.processes();
            processes
                .get(&params.process_id)
                .map(|entry| Arc::clone(&entry.interaction))
        };
        let Some(interaction) = interaction else {
            return Ok(LocalProcessWriteResponse {
                status: WriteStatus::UnknownProcess,
            });
        };
        let _interaction_guard = interaction.lock().await;
        self.touch_last_used(&params.process_id);
        let (input, mut wake) = {
            let mut processes = self.processes();
            let Some(entry) = processes.get_mut(&params.process_id) else {
                return Ok(LocalProcessWriteResponse {
                    status: WriteStatus::UnknownProcess,
                });
            };
            if entry.exited || entry.closed {
                return Ok(LocalProcessWriteResponse {
                    status: WriteStatus::StdinClosed,
                });
            }
            if entry.terminal_size.is_none() && !entry.pipe_stdin {
                return Ok(LocalProcessWriteResponse {
                    status: WriteStatus::StdinClosed,
                });
            }
            if entry.accepted_write_ids.contains(&params.write_id) {
                return Ok(LocalProcessWriteResponse {
                    status: WriteStatus::Accepted,
                });
            }
            entry.accepted_write_ids.remember(params.write_id.clone());
            let Some(input) = entry.input.clone() else {
                return Ok(LocalProcessWriteResponse {
                    status: WriteStatus::StdinClosed,
                });
            };
            (input, entry.wake_tx.subscribe())
        };

        {
            let mut input = input.lock().await;
            if let Err(error) = input.write_all(&params.chunk).await {
                publish_process_failure(
                    &self.processes,
                    &params.process_id,
                    RuntimeString::from_utf8(&format!("failed to write process stdin: {error}")),
                );
                return Ok(LocalProcessWriteResponse {
                    status: WriteStatus::StdinClosed,
                });
            }
            if let Err(error) = input.flush().await {
                publish_process_failure(
                    &self.processes,
                    &params.process_id,
                    RuntimeString::from_utf8(&format!("failed to flush process stdin: {error}")),
                );
                return Ok(LocalProcessWriteResponse {
                    status: WriteStatus::StdinClosed,
                });
            }
        }
        let yield_ms = params.yield_ms.unwrap_or_default().clamp(0, 60_000) as u64;
        if yield_ms > 0 {
            if let Some(signal) = params.signal.as_ref() {
                tokio::select! {
                    _ = wake.changed() => {}
                    _ = tokio::time::sleep(Duration::from_millis(yield_ms)) => {}
                    _ = signal.cancelled() => return Err(RuntimeString::from("process write aborted")),
                }
            } else {
                tokio::select! {
                    _ = wake.changed() => {}
                    _ = tokio::time::sleep(Duration::from_millis(yield_ms)) => {}
                }
            }
        }
        Ok(LocalProcessWriteResponse {
            status: WriteStatus::Accepted,
        })
    }

    /// Closes the stdin side of a piped session so the child observes EOF. Over
    /// a pipe the EOT control byte is just data, so an interactive shell that
    /// promises EOF must drop the writer instead of sending the byte; terminal
    /// sessions take the byte itself because their line discipline already
    /// turns it into EOF. Returns false when the process is unknown, already
    /// finished, or has no live input handle.
    pub fn close_stdin(&self, process_id: &RuntimeString) -> bool {
        let mut processes = self.processes();
        let Some(entry) = processes.get_mut(process_id) else {
            return false;
        };
        if entry.exited || entry.closed {
            return false;
        }
        entry.input.take().is_some()
    }

    pub async fn signal(
        &self,
        process_id: &RuntimeString,
        signal: ProcessSignal,
    ) -> ProcessResult<bool> {
        let controller = self.live_controller(process_id)?;
        let Some(controller) = controller else {
            return Ok(false);
        };
        match signal {
            ProcessSignal::Interrupt => controller.signal(RuntimeSignal::Interrupt),
        };
        Ok(true)
    }

    pub async fn resize(
        &self,
        process_id: &RuntimeString,
        size: TerminalSize,
    ) -> ProcessResult<bool> {
        // Serialize with reads and writes on the same process, and keep pruning
        // away from a session with an interaction in progress.
        let interaction = {
            let processes = self.processes();
            let Some(entry) = processes.get(process_id) else {
                return Err(RuntimeString::from("unknown process id ").concat(process_id));
            };
            Arc::clone(&entry.interaction)
        };
        let _interaction_guard = interaction.lock().await;
        self.touch_last_used(process_id);
        let input = {
            let processes = self.processes();
            let Some(entry) = processes.get(process_id) else {
                return Err(RuntimeString::from("unknown process id ").concat(process_id));
            };
            if entry.exited || entry.closed {
                return Ok(false);
            }
            entry.input.clone()
        };
        let Some(input) = input else { return Ok(false) };
        let resized = input
            .lock()
            .await
            .resize(size)
            .map_err(|error| RuntimeString::from_utf8(&error.to_string()))?;
        if resized {
            if let Some(entry) = self.processes().get_mut(process_id) {
                entry.terminal_size = Some(size);
            }
        }
        Ok(resized)
    }

    pub async fn terminate(
        &self,
        process_id: &RuntimeString,
    ) -> ProcessResult<LocalProcessTerminateResponse> {
        Ok(LocalProcessTerminateResponse {
            running: self.kill_with_signal(process_id, Signal::Kill),
        })
    }

    pub fn kill_with_signal(&self, process_id: &RuntimeString, signal: Signal) -> bool {
        let controller = {
            let processes = self.processes();
            let Some(entry) = processes.get(process_id) else {
                return false;
            };
            if entry.exited || entry.closed {
                return false;
            }
            entry.controller.clone()
        };
        controller.signal(runtime_signal(signal));
        true
    }

    pub fn subscribe_events(
        &self,
        process_id: &RuntimeString,
    ) -> ProcessResult<ProcessEventReceiver> {
        let processes = self.processes();
        let Some(entry) = processes.get(process_id) else {
            return Err(RuntimeString::from("unknown process id ").concat(process_id));
        };
        Ok(entry.events.subscribe())
    }

    pub fn subscribe_wake(
        &self,
        process_id: &RuntimeString,
    ) -> ProcessResult<watch::Receiver<u64>> {
        let processes = self.processes();
        let Some(entry) = processes.get(process_id) else {
            return Err(RuntimeString::from("unknown process id ").concat(process_id));
        };
        Ok(entry.wake_tx.subscribe())
    }

    pub fn info(&self, process_id: &RuntimeString) -> Option<LocalProcessInfo> {
        let processes = self.processes();
        processes
            .get(process_id)
            .map(|entry| Self::info_from_entry(process_id, entry))
    }

    pub fn list(&self) -> Vec<LocalProcessInfo> {
        let processes = self.processes();
        processes
            .iter()
            .map(|(process_id, entry)| Self::info_from_entry(process_id, entry))
            .collect()
    }

    pub fn tail(&self, process_id: &RuntimeString, bytes: usize) -> Option<RuntimeString> {
        let processes = self.processes();
        let entry = processes.get(process_id)?;
        let mut out = VecDeque::new();
        let mut retained = 0usize;
        // Newest contiguous suffix first; the stable head prefix joins only when
        // nothing was evicted, because head bytes are no longer adjacent to tail
        // bytes once the middle span was dropped.
        for rc in entry.tail_chunks.iter().rev() {
            if retained >= bytes {
                break;
            }
            let needed = bytes.saturating_sub(retained);
            let slice = if rc.chunk.chunk.len() > needed {
                &rc.chunk.chunk[rc.chunk.chunk.len() - needed..]
            } else {
                &rc.chunk.chunk[..]
            };
            retained += slice.len();
            out.push_front(slice.to_vec());
        }
        if retained < bytes && entry.evicted_bytes == 0 {
            for rc in entry.head_chunks.iter().rev() {
                if retained >= bytes {
                    break;
                }
                let needed = bytes.saturating_sub(retained);
                let slice = if rc.chunk.chunk.len() > needed {
                    &rc.chunk.chunk[rc.chunk.chunk.len() - needed..]
                } else {
                    &rc.chunk.chunk[..]
                };
                retained += slice.len();
                out.push_front(slice.to_vec());
            }
        }
        let bytes = out.into_iter().flatten().collect::<Vec<_>>();
        Some(NodeChunkUtf8::decode_chunk(&bytes))
    }

    pub fn dispose_all(&self) {
        let ids = self
            .list()
            .into_iter()
            .filter(|info| !info.exited && !info.closed)
            .map(|info| info.process_id)
            .collect::<Vec<_>>();
        for id in ids {
            self.kill_with_signal(&id, Signal::Kill);
        }
    }

    fn info_from_entry(process_id: &RuntimeString, entry: &ProcessEntry) -> LocalProcessInfo {
        LocalProcessInfo {
            process_id: process_id.clone(),
            cwd: entry.cwd.clone(),
            argv: entry.argv.clone(),
            exited: entry.exited,
            exit_code: entry.exit_code,
            closed: entry.closed,
            terminal: entry.terminal_size.is_some(),
            terminal_size: entry.terminal_size,
        }
    }

    fn live_controller(
        &self,
        process_id: &RuntimeString,
    ) -> ProcessResult<Option<RuntimeProcessController>> {
        let processes = self.processes();
        let Some(entry) = processes.get(process_id) else {
            return Err(RuntimeString::from("unknown process id ").concat(process_id));
        };
        if entry.exited || entry.closed {
            return Ok(None);
        }
        Ok(Some(entry.controller.clone()))
    }

    fn next_last_used(&self) -> u64 {
        self.last_used_clock.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn touch_last_used(&self, process_id: &RuntimeString) {
        let tick = self.next_last_used();
        if let Some(entry) = self.processes().get_mut(process_id) {
            entry.last_used = tick;
        }
    }

    /// Evicts one session when the table reached its cap: the least recently
    /// used finished session first, then any least recently used live session,
    /// never inside the most-recently-used protection window and never while an
    /// interaction holds the candidate's lock. Returns the evicted entry's id
    /// and controller so the caller stops the child outside the table lock.
    fn prune_one_if_needed(
        processes: &mut HashMap<RuntimeString, ProcessEntry>,
    ) -> Option<(RuntimeString, RuntimeProcessController)> {
        if processes.len() < MAX_RETAINED_PROCESSES {
            return None;
        }
        let mut meta: Vec<(RuntimeString, u64, bool)> = processes
            .iter()
            .map(|(id, entry)| (id.clone(), entry.last_used, entry.exited || entry.closed))
            .collect();
        let mut found_locked_exited_process = false;
        while let Some(candidate_id) = Self::prune_candidate(&meta) {
            let (candidate_exited, interaction, controller) = match processes.get(&candidate_id) {
                Some(entry) => (
                    entry.exited || entry.closed,
                    Arc::clone(&entry.interaction),
                    entry.controller.clone(),
                ),
                None => {
                    meta.retain(|(id, _, _)| *id != candidate_id);
                    continue;
                }
            };
            if found_locked_exited_process && !candidate_exited {
                // A finished session may briefly hold its interaction lock
                // while publishing terminal events. The table tolerates the
                // soft cap instead of evicting a live session for it.
                return None;
            }
            if let Ok(_interaction_guard) = interaction.try_lock() {
                if let Some(entry) = processes.get_mut(&candidate_id) {
                    // Subscribers observe the normal terminal transition
                    // before the entry disappears.
                    entry.mark_exited(None);
                    entry.mark_closed();
                }
                processes.remove(&candidate_id);
                return Some((candidate_id, controller));
            }
            found_locked_exited_process |= candidate_exited;
            meta.retain(|(id, _, _)| *id != candidate_id);
        }
        None
    }

    /// Centralized eviction order: the least recently used finished session
    /// outside the protected window first, else the least recently used session
    /// outside that window.
    fn prune_candidate(meta: &[(RuntimeString, u64, bool)]) -> Option<RuntimeString> {
        if meta.is_empty() {
            return None;
        }
        let mut by_recency: Vec<&(RuntimeString, u64, bool)> = meta.iter().collect();
        by_recency.sort_by_key(|(_, last_used, _)| Reverse(*last_used));
        let protected: HashSet<RuntimeString> = by_recency
            .iter()
            .take(PRUNE_PROTECT_RECENT)
            .map(|(id, _, _)| (*id).clone())
            .collect();
        let mut lru: Vec<&(RuntimeString, u64, bool)> = meta.iter().collect();
        lru.sort_by_key(|(_, last_used, _)| *last_used);
        if let Some((id, _, _)) = lru
            .iter()
            .find(|(id, _, exited)| !protected.contains(id) && *exited)
        {
            return Some(id.clone());
        }
        lru.into_iter()
            .find(|(id, _, _)| !protected.contains(id))
            .map(|(id, _, _)| id.clone())
    }

    fn processes(&self) -> std::sync::MutexGuard<'_, HashMap<RuntimeString, ProcessEntry>> {
        self.processes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn runtime_signal(signal: Signal) -> RuntimeSignal {
    match signal {
        Signal::Int => RuntimeSignal::Interrupt,
        Signal::Kill => RuntimeSignal::Kill,
        Signal::Hup | Signal::Quit | Signal::Usr1 | Signal::Usr2 | Signal::Term => {
            RuntimeSignal::Terminate
        }
    }
}

fn process_table(
    processes: &Arc<Mutex<HashMap<RuntimeString, ProcessEntry>>>,
) -> std::sync::MutexGuard<'_, HashMap<RuntimeString, ProcessEntry>> {
    processes
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn publish_process_output(
    processes: &Arc<Mutex<HashMap<RuntimeString, ProcessEntry>>>,
    process_id: &RuntimeString,
    stream: ExecOutputStream,
    bytes: &[u8],
) {
    if let Some(entry) = process_table(processes).get_mut(process_id) {
        entry.push_output(stream, bytes.to_vec());
    }
}

fn publish_process_failure(
    processes: &Arc<Mutex<HashMap<RuntimeString, ProcessEntry>>>,
    process_id: &RuntimeString,
    message: RuntimeString,
) {
    if let Some(entry) = process_table(processes).get_mut(process_id) {
        entry.mark_failed(message);
    }
}

fn finish_process(
    processes: Arc<Mutex<HashMap<RuntimeString, ProcessEntry>>>,
    process_id: RuntimeString,
    exit: Result<Option<i32>, RuntimeString>,
    retention: Duration,
) {
    {
        let mut table = process_table(&processes);
        let Some(entry) = table.get_mut(&process_id) else {
            return;
        };
        entry.input = None;
        match exit {
            Ok(code) => entry.mark_exited(code),
            Err(message) => {
                entry.mark_failed(message);
                entry.mark_exited(None);
            }
        }
        entry.mark_closed();
    }
    tokio::spawn(async move {
        tokio::time::sleep(retention).await;
        let mut table = process_table(&processes);
        if table.get(&process_id).is_some_and(|entry| entry.closed) {
            table.remove(&process_id);
        }
    });
}

async fn drain_pipe_process(
    mut child: SpawnedRuntimeProcess,
    mut stdout: RuntimeChildStdout,
    mut stderr: RuntimeChildStderr,
    processes: Arc<Mutex<HashMap<RuntimeString, ProcessEntry>>>,
    process_id: RuntimeString,
    retention: Duration,
) {
    let mut stdout_done = false;
    let mut stderr_done = false;
    let mut stdout_chunk = vec![0_u8; 64 * 1024];
    let mut stderr_chunk = vec![0_u8; 64 * 1024];
    while !(stdout_done && stderr_done) {
        tokio::select! {
            read = stdout.read(&mut stdout_chunk), if !stdout_done => {
                match read {
                    Ok(0) => stdout_done = true,
                    Ok(read) => publish_process_output(
                        &processes,
                        &process_id,
                        ExecOutputStream::Stdout,
                        &stdout_chunk[..read],
                    ),
                    Err(error) => {
                        stdout_done = true;
                        publish_process_failure(
                            &processes,
                            &process_id,
                            RuntimeString::from_utf8(&error.to_string()),
                        );
                    }
                }
            }
            read = stderr.read(&mut stderr_chunk), if !stderr_done => {
                match read {
                    Ok(0) => stderr_done = true,
                    Ok(read) => publish_process_output(
                        &processes,
                        &process_id,
                        ExecOutputStream::Stderr,
                        &stderr_chunk[..read],
                    ),
                    Err(error) => {
                        stderr_done = true;
                        publish_process_failure(
                            &processes,
                            &process_id,
                            RuntimeString::from_utf8(&error.to_string()),
                        );
                    }
                }
            }
        }
    }
    let exit = child
        .wait_terminal()
        .await
        .map(|status| status.code())
        .map_err(|error| error.message().clone());
    child.disarm();
    finish_process(processes, process_id, exit, retention);
}

async fn drain_pty_process(
    mut child: SpawnedRuntimeProcess,
    mut reader: RuntimeChildPtyReader,
    processes: Arc<Mutex<HashMap<RuntimeString, ProcessEntry>>>,
    process_id: RuntimeString,
    retention: Duration,
) {
    let mut chunk = vec![0_u8; 64 * 1024];
    loop {
        match reader.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(read) => publish_process_output(
                &processes,
                &process_id,
                ExecOutputStream::Pty,
                &chunk[..read],
            ),
        }
    }
    let exit = child
        .wait_terminal()
        .await
        .map(|status| status.code())
        .map_err(|error| error.message().clone());
    child.disarm();
    finish_process(processes, process_id, exit, retention);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::sandbox::{
        ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec,
    };
    use std::sync::Arc;

    /// Detached or restricted executors cannot allocate ptys, and so cannot a host
    /// whose system-wide ptmx ceiling is already taken; tty-spawn tests skip there
    /// instead of failing red. The probe lives beside the spawner so every pty test
    /// in the crate asks the same question.
    ///
    /// Unix-only along with its callers, whose subject (a shell trapping a
    /// process-group signal) has no Windows counterpart. There is nothing to probe on
    /// Windows anyway: ConPTY needs no allocation up front and ships from Windows 10
    /// 1809 on, so a refusal there would surface as the spawn failing.
    #[cfg(unix)]
    use crate::runtime_process::pty_allocatable;

    struct IdSandbox;

    impl SandboxBackend for IdSandbox {
        fn kind(&self) -> SandboxKind {
            SandboxKind::Process
        }

        fn is_available(&self) -> bool {
            true
        }

        fn transform(&self, req: &ExecRequest, _policy: &SandboxPolicy) -> SpawnSpec {
            let file = req.argv.first().cloned().unwrap_or_default();
            let args = req.argv.iter().skip(1).cloned().collect();
            let mut env = ParentEnvSnapshot::capture();
            if let Some(extra) = &req.env {
                env.overlay(extra);
            }
            SpawnSpec {
                file,
                args,
                cwd: req.cwd.clone(),
                env,
            }
        }
    }

    fn runtime(value: &str) -> RuntimeString {
        RuntimeString::from(value)
    }

    fn cwd() -> RuntimePath {
        RuntimePath::from(
            std::env::current_dir()
                .unwrap()
                .to_string_lossy()
                .into_owned(),
        )
    }

    fn sandbox() -> Arc<dyn SandboxBackend> {
        Arc::new(IdSandbox)
    }

    #[tokio::test]
    async fn direct_spawn_spec_supplements_path_with_the_resolved_ripgrep_directory() {
        let _g = crate::test_support::ripgrep_global_guard().await;
        let missing = "/nonexistent-klaus-rg-dir";
        let mut env = RuntimeEnv::new();
        env.insert(RuntimeString::from("PATH"), RuntimeString::from(missing));
        let request = ExecRequest {
            argv: vec![RuntimeString::from("rg"), RuntimeString::from("--version")],
            cwd: cwd(),
            env: Some(env),
            timeout_ms: None,
        };

        let spec = PassthroughSandbox.transform(&request, &SandboxPolicy::default());

        let supplemented = spec
            .env
            .get(&RuntimeString::from("PATH"))
            .expect("PATH survives into the spawn spec");
        let supplemented = String::from_utf8(supplemented.to_node_utf8()).expect("PATH is UTF-8");
        let separator = if cfg!(windows) { ';' } else { ':' };
        let mut entries = supplemented.split(separator);
        let first = entries.next().expect("supplemented PATH is non-empty");
        assert_ne!(first, missing);
        let candidate = if cfg!(windows) { "rg.exe" } else { "rg" };
        assert!(
            std::path::Path::new(first).join(candidate).is_file(),
            "supplemented PATH head carries ripgrep: {supplemented}"
        );
        assert_eq!(entries.next(), Some(missing));
    }

    #[test]
    fn direct_spawn_spec_preserves_lone_surrogates_and_environment_order() {
        let lone_surrogate = RuntimeString::from_utf16(vec![b'a' as u16, 0xd800, b'b' as u16]);
        let first_key = RuntimeString::from("KLAUS_EXACT_ORDER_Z_71D4");
        let second_key = RuntimeString::from("KLAUS_EXACT_ORDER_A_71D4");
        let mut env = RuntimeEnv::new();
        env.insert(first_key.clone(), RuntimeString::from("first"));
        env.insert(second_key.clone(), RuntimeString::from("second"));
        let request = ExecRequest {
            argv: vec![RuntimeString::from("printf"), lone_surrogate.clone()],
            cwd: cwd(),
            env: Some(env),
            timeout_ms: None,
        };

        let spec = PassthroughSandbox.transform(&request, &SandboxPolicy::default());

        assert_eq!(spec.args, vec![lone_surrogate]);
        let ordered = spec
            .env
            .entries()
            .filter(|(key, _)| key == &first_key || key == &second_key)
            .map(|(key, _)| key)
            .collect::<Vec<_>>();
        assert_eq!(ordered, vec![first_key, second_key]);
    }

    #[tokio::test]
    async fn retained_reads_are_ordered_by_sequence() {
        let registry = LocalExecProcessRegistry::new();
        let started = registry
            .start(
                crate::runtime_process::test_runtime_process_context(),
                LocalProcessStartParams {
                    process_id: Some(runtime("seq-proc")),
                    argv: vec![runtime("cat")],
                    cwd: cwd(),
                    sandbox: sandbox(),
                    tty: false,
                    pipe_stdin: true,
                    max_output_bytes: Some(1024),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(started.process_id, "seq-proc");
        assert_eq!(registry.info(&started.process_id).unwrap().cwd, cwd());
        registry
            .write(LocalProcessWriteParams {
                process_id: runtime("seq-proc"),
                write_id: runtime("w1"),
                chunk: b"one\n".to_vec(),
                yield_ms: Some(1000),
                signal: None,
            })
            .await
            .unwrap();
        let first = registry
            .read(LocalProcessReadParams {
                process_id: runtime("seq-proc"),
                after_seq: Some(0),
                after_offset: None,
                max_bytes: None,
                wait_ms: Some(100),
                signal: None,
            })
            .await
            .unwrap();
        assert_eq!(first.chunks.len(), 1);
        assert_eq!(first.chunks[0].seq, 1);
        assert_eq!(first.chunks[0].chunk, b"one\n");

        registry
            .write(LocalProcessWriteParams {
                process_id: runtime("seq-proc"),
                write_id: runtime("w2"),
                chunk: b"two\n".to_vec(),
                yield_ms: Some(1000),
                signal: None,
            })
            .await
            .unwrap();
        let second = registry
            .read(LocalProcessReadParams {
                process_id: runtime("seq-proc"),
                after_seq: Some(first.next_seq - 1),
                after_offset: None,
                max_bytes: None,
                wait_ms: Some(100),
                signal: None,
            })
            .await
            .unwrap();
        assert_eq!(second.chunks.len(), 1);
        assert_eq!(second.chunks[0].seq, 2);
        assert_eq!(second.chunks[0].chunk, b"two\n");

        registry.terminate(&runtime("seq-proc")).await.unwrap();
    }

    #[tokio::test]
    async fn write_ids_are_idempotent() {
        let registry = LocalExecProcessRegistry::new();
        registry
            .start(
                crate::runtime_process::test_runtime_process_context(),
                LocalProcessStartParams {
                    process_id: Some(runtime("dedupe-proc")),
                    argv: vec![runtime("cat")],
                    cwd: cwd(),
                    sandbox: sandbox(),
                    tty: false,
                    pipe_stdin: true,
                    max_output_bytes: Some(1024),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let write = LocalProcessWriteParams {
            process_id: runtime("dedupe-proc"),
            write_id: runtime("same-write"),
            chunk: b"only-once\n".to_vec(),
            yield_ms: Some(1000),
            signal: None,
        };
        assert_eq!(
            registry.write(write.clone()).await.unwrap().status,
            WriteStatus::Accepted
        );
        assert_eq!(
            registry.write(write).await.unwrap().status,
            WriteStatus::Accepted
        );

        let read = registry
            .read(LocalProcessReadParams {
                process_id: runtime("dedupe-proc"),
                after_seq: Some(0),
                after_offset: None,
                max_bytes: None,
                wait_ms: Some(100),
                signal: None,
            })
            .await
            .unwrap();
        let text = read
            .chunks
            .iter()
            .map(|chunk| String::from_utf8_lossy(&chunk.chunk))
            .collect::<String>();
        assert_eq!(text.matches("only-once").count(), 1);

        registry.terminate(&runtime("dedupe-proc")).await.unwrap();
    }

    #[tokio::test]
    async fn non_tty_process_keeps_stdin_closed_unless_pipe_stdin_is_requested() {
        let registry = LocalExecProcessRegistry::new();
        registry
            .start(
                crate::runtime_process::test_runtime_process_context(),
                LocalProcessStartParams {
                    process_id: Some(runtime("closed-stdin-proc")),
                    argv: vec![runtime("sleep"), runtime("30")],
                    cwd: cwd(),
                    sandbox: sandbox(),
                    tty: false,
                    pipe_stdin: false,
                    max_output_bytes: Some(1024),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let write = registry
            .write(LocalProcessWriteParams {
                process_id: runtime("closed-stdin-proc"),
                write_id: runtime("closed-stdin-write"),
                chunk: b"ignored\n".to_vec(),
                yield_ms: Some(100),
                signal: None,
            })
            .await
            .unwrap();

        assert_eq!(write.status, WriteStatus::StdinClosed);
        registry
            .terminate(&runtime("closed-stdin-proc"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn process_wake_subscribers_are_notified_on_output() {
        let registry = LocalExecProcessRegistry::new();
        registry
            .start(
                crate::runtime_process::test_runtime_process_context(),
                LocalProcessStartParams {
                    process_id: Some(runtime("wake-proc")),
                    argv: vec![runtime("cat")],
                    cwd: cwd(),
                    sandbox: sandbox(),
                    tty: false,
                    pipe_stdin: true,
                    max_output_bytes: Some(1024),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let mut wake = registry.subscribe_wake(&runtime("wake-proc")).unwrap();
        let before = *wake.borrow();

        registry
            .write(LocalProcessWriteParams {
                process_id: runtime("wake-proc"),
                write_id: runtime("wake-write"),
                chunk: b"wake-output\n".to_vec(),
                yield_ms: Some(1000),
                signal: None,
            })
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(1), wake.changed())
            .await
            .expect("wake change should arrive")
            .expect("wake channel should stay open");
        assert!(*wake.borrow() > before);
        registry.terminate(&runtime("wake-proc")).await.unwrap();
    }

    #[tokio::test]
    async fn closed_processes_are_removed_after_retention_window() {
        let registry =
            LocalExecProcessRegistry::with_exited_process_retention(Duration::from_millis(20));
        registry
            .start(
                crate::runtime_process::test_runtime_process_context(),
                LocalProcessStartParams {
                    process_id: Some(runtime("retained-closed-proc")),
                    argv: vec![runtime("true")],
                    cwd: cwd(),
                    sandbox: sandbox(),
                    tty: false,
                    pipe_stdin: false,
                    max_output_bytes: Some(1024),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert!(registry.info(&runtime("retained-closed-proc")).is_some());
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(registry.info(&runtime("retained-closed-proc")).is_none());
    }

    #[test]
    fn process_output_chunks_use_base64_wire_bytes() {
        let encoded = runtime_codec::encode_value(&ProcessOutputChunk {
            seq: 7,
            stream: ExecOutputStream::Stdout,
            chunk: b"wire-bytes".to_vec(),
        })
        .unwrap();

        assert_eq!(
            encoded.get("chunk"),
            Some(&RuntimeJson::from("d2lyZS1ieXRlcw=="))
        );
        let decoded: ProcessOutputChunk = runtime_codec::decode_value(&encoded).unwrap();
        assert_eq!(decoded.chunk, b"wire-bytes");
    }

    /// Interrupt reaches the target as a signal it can trap, and the process stays
    /// registered afterwards.
    ///
    /// Unix-only, because the behaviour asserted here does not exist on Windows: there
    /// is no per-process-group signal a shell can trap, so interrupt there is
    /// termination (see `klaus_sandbox_win::process::interrupt_tree`). A `trap ... INT`
    /// handler never runs, and asserting that it does would be asserting that Windows
    /// is Unix.
    #[cfg(unix)]
    #[tokio::test]
    async fn typed_interrupt_signals_the_process_without_forgetting_it() {
        if !pty_allocatable() {
            eprintln!("skipping pty test: this environment cannot allocate a pty");
            return;
        }
        let registry = LocalExecProcessRegistry::new();
        registry
            .start(
                crate::runtime_process::test_runtime_process_context(),
                LocalProcessStartParams {
                    process_id: Some(runtime("interrupt-proc")),
                    argv: vec![
                        runtime("bash"),
                        runtime("-c"),
                        runtime(
                            "trap 'echo interrupted; exit 0' INT; while true; do sleep 1; done",
                        ),
                    ],
                    cwd: cwd(),
                    sandbox: sandbox(),
                    tty: true,
                    max_output_bytes: Some(4096),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        registry
            .signal(&runtime("interrupt-proc"), ProcessSignal::Interrupt)
            .await
            .unwrap();
        let read = registry
            .read(LocalProcessReadParams {
                process_id: runtime("interrupt-proc"),
                after_seq: Some(0),
                after_offset: None,
                max_bytes: None,
                wait_ms: Some(5000),
                signal: None,
            })
            .await
            .unwrap();
        let text = read
            .chunks
            .iter()
            .map(|chunk| String::from_utf8_lossy(&chunk.chunk))
            .collect::<String>();

        assert!(text.contains("interrupted"), "output was {text:?}");
        let exited = if read.exited {
            true
        } else {
            registry
                .read(LocalProcessReadParams {
                    process_id: runtime("interrupt-proc"),
                    after_seq: Some(read.next_seq),
                    after_offset: None,
                    max_bytes: None,
                    wait_ms: Some(5000),
                    signal: None,
                })
                .await
                .unwrap()
                .exited
        };
        assert!(exited);
    }

    /// A resize reaches the target, which sees the new geometry.
    ///
    /// Unix-only because the probe is `stty size` under a POSIX shell. On Windows the
    /// pty is a ConPTY and there is no equivalent one-liner that reports its geometry
    /// to the child, so resize on that platform is **not** covered here — stated rather
    /// than papered over, because a reader would otherwise assume it is.
    #[cfg(unix)]
    #[tokio::test]
    async fn pty_process_can_resize_through_process_protocol() {
        if !pty_allocatable() {
            eprintln!("skipping pty test: this environment cannot allocate a pty");
            return;
        }
        let registry = LocalExecProcessRegistry::new();
        registry
            .start(
                crate::runtime_process::test_runtime_process_context(),
                LocalProcessStartParams {
                    process_id: Some(runtime("resize-proc")),
                    argv: vec![
                        runtime("bash"),
                        runtime("-c"),
                        runtime("stty size; read line; stty size; exit"),
                    ],
                    cwd: cwd(),
                    sandbox: sandbox(),
                    tty: true,
                    cols: Some(80),
                    rows: Some(24),
                    max_output_bytes: Some(4096),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let initial = registry
            .read(LocalProcessReadParams {
                process_id: runtime("resize-proc"),
                after_seq: Some(0),
                after_offset: None,
                max_bytes: None,
                wait_ms: Some(5000),
                signal: None,
            })
            .await
            .unwrap();
        let initial_text = initial
            .chunks
            .iter()
            .map(|chunk| String::from_utf8_lossy(&chunk.chunk))
            .collect::<String>();
        assert!(
            initial_text.contains("24 80"),
            "output was {initial_text:?}"
        );

        assert!(registry
            .resize(
                &runtime("resize-proc"),
                TerminalSize {
                    cols: 100,
                    rows: 40,
                },
            )
            .await
            .unwrap());
        registry
            .write(LocalProcessWriteParams {
                process_id: runtime("resize-proc"),
                write_id: runtime("after-resize"),
                chunk: b"go\n".to_vec(),
                yield_ms: Some(5000),
                signal: None,
            })
            .await
            .unwrap();
        let read = registry
            .read(LocalProcessReadParams {
                process_id: runtime("resize-proc"),
                after_seq: Some(initial.next_seq),
                after_offset: None,
                max_bytes: None,
                wait_ms: Some(5000),
                signal: None,
            })
            .await
            .unwrap();
        let text = read
            .chunks
            .iter()
            .map(|chunk| String::from_utf8_lossy(&chunk.chunk))
            .collect::<String>();

        assert!(text.contains("40 100"), "output was {text:?}");
        let exited = if read.exited {
            true
        } else {
            registry
                .read(LocalProcessReadParams {
                    process_id: runtime("resize-proc"),
                    after_seq: Some(read.next_seq),
                    after_offset: None,
                    max_bytes: None,
                    wait_ms: Some(5000),
                    signal: None,
                })
                .await
                .unwrap()
                .exited
        };
        assert!(exited);
    }

    /// The retention window keeps the stream's beginning and its newest end,
    /// and a reader starting from byte zero learns the exact size of the
    /// evicted middle.
    #[cfg(unix)]
    #[tokio::test]
    async fn retention_keeps_head_and_tail_and_reports_the_evicted_middle() {
        let registry = LocalExecProcessRegistry::new();
        registry
            .start(
                crate::runtime_process::test_runtime_process_context(),
                LocalProcessStartParams {
                    process_id: Some(runtime("retention-proc")),
                    argv: vec![
                        runtime("bash"),
                        runtime("-c"),
                        runtime(
                            "i=0; while [ $i -lt 40 ]; do printf 'line-%02d-0123456789abcdef\\n' $i; i=$((i+1)); sleep 0.02; done",
                        ),
                    ],
                    cwd: cwd(),
                    sandbox: sandbox(),
                    tty: false,
                    pipe_stdin: false,
                    yield_ms: Some(0),
                    max_output_bytes: Some(512),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let mut finished = None;
        for _ in 0..300 {
            let read = registry
                .read(LocalProcessReadParams {
                    process_id: runtime("retention-proc"),
                    after_seq: Some(0),
                    after_offset: Some(0),
                    max_bytes: None,
                    wait_ms: Some(100),
                    signal: None,
                })
                .await
                .unwrap();
            if read.exited {
                finished = Some(read);
                break;
            }
            // A read with chunks available returns immediately, so pace the
            // guard loop to span the producer's ~0.8 s lifetime.
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let read = finished.expect("the producer exits within the guard window");
        let retained: usize = read.chunks.iter().map(|chunk| chunk.chunk.len()).sum();
        // 40 lines x 25 bytes = 1000 produced against a 512-byte budget, so
        // the accounting must be exact: retained + dropped == produced.
        assert!(retained > 0);
        assert!(retained < 1000, "the middle must be evicted: {retained}");
        assert_eq!(read.dropped_bytes, 1000 - retained as u64);
        let first = String::from_utf8_lossy(&read.chunks.first().unwrap().chunk);
        let last = String::from_utf8_lossy(&read.chunks.last().unwrap().chunk);
        assert!(first.contains("line-00"), "head kept: {first:?}");
        assert!(last.contains("line-39"), "tail kept: {last:?}");
        registry
            .terminate(&runtime("retention-proc"))
            .await
            .unwrap();
    }

    /// Closing the stdin side of a piped session delivers EOF to the child,
    /// which is what the documented EOT input promises over a pipe.
    #[cfg(unix)]
    #[tokio::test]
    async fn close_stdin_delivers_eof_to_a_piped_child() {
        let registry = LocalExecProcessRegistry::new();
        registry
            .start(
                crate::runtime_process::test_runtime_process_context(),
                LocalProcessStartParams {
                    process_id: Some(runtime("eof-proc")),
                    argv: vec![runtime("cat")],
                    cwd: cwd(),
                    sandbox: sandbox(),
                    tty: false,
                    pipe_stdin: true,
                    yield_ms: Some(0),
                    max_output_bytes: Some(1024),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert!(registry.close_stdin(&runtime("eof-proc")));
        // A second close finds no live input handle.
        assert!(!registry.close_stdin(&runtime("eof-proc")));

        let mut exited = false;
        for _ in 0..50 {
            match registry
                .read(LocalProcessReadParams {
                    process_id: runtime("eof-proc"),
                    after_seq: Some(0),
                    after_offset: None,
                    max_bytes: None,
                    wait_ms: Some(100),
                    signal: None,
                })
                .await
            {
                Ok(read) if read.exited => {
                    exited = true;
                    break;
                }
                Ok(_) => {}
                // The short test retention window may remove the finished
                // entry outright; a gone entry is a finished child.
                Err(_) => {
                    exited = true;
                    break;
                }
            }
        }
        assert!(exited, "cat must exit after its stdin closes");
    }

    /// The process table is capped: a new start at the cap evicts the least
    /// recently used unprotected session, and a recent interaction moves a
    /// session back inside the protected window.
    #[cfg(unix)]
    #[tokio::test]
    async fn the_process_table_prunes_the_least_recently_used_at_the_cap() {
        let registry = LocalExecProcessRegistry::new();
        for index in 0..MAX_RETAINED_PROCESSES {
            registry
                .start(
                    crate::runtime_process::test_runtime_process_context(),
                    LocalProcessStartParams {
                        process_id: Some(runtime(&format!("cap-proc-{index}"))),
                        argv: vec![runtime("sleep"), runtime("30")],
                        cwd: cwd(),
                        sandbox: sandbox(),
                        tty: false,
                        pipe_stdin: false,
                        yield_ms: Some(0),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
        }
        assert_eq!(registry.list().len(), MAX_RETAINED_PROCESSES);

        // Refresh one old session so it re-enters the protected window.
        registry
            .read(LocalProcessReadParams {
                process_id: runtime("cap-proc-0"),
                after_seq: Some(0),
                after_offset: None,
                max_bytes: None,
                wait_ms: Some(0),
                signal: None,
            })
            .await
            .unwrap();

        registry
            .start(
                crate::runtime_process::test_runtime_process_context(),
                LocalProcessStartParams {
                    process_id: Some(runtime("cap-overflow")),
                    argv: vec![runtime("sleep"), runtime("30")],
                    cwd: cwd(),
                    sandbox: sandbox(),
                    tty: false,
                    pipe_stdin: false,
                    yield_ms: Some(0),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert!(
            registry.info(&runtime("cap-proc-0")).is_some(),
            "a recently used session stays protected"
        );
        assert!(
            registry.info(&runtime("cap-proc-1")).is_none(),
            "the least recently used unprotected session is evicted"
        );
        assert!(registry.info(&runtime("cap-overflow")).is_some());
        assert_eq!(registry.list().len(), MAX_RETAINED_PROCESSES);
        registry.dispose_all();
    }
}
