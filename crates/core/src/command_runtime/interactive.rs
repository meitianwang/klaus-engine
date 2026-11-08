//! Persistent, interactive command sessions. The default transport is still a
//! pipe-backed child because that keeps ordinary command output easy to parse;
//! callers can explicitly request a PTY-backed session for full-terminal
//! programs like `vim`, `top`, `ssh`, `sudo`, or REPLs that test `isatty`.
//!
//! Model:
//!  - [`InteractiveCommandRegistry::exec`] spawns a fresh child for the command. If it
//!    FINISHES within the yield window it's a one-shot (output + exit code,
//!    nothing stored). If it's still ALIVE at the deadline (a REPL, a server, an
//!    interactive prompt, or a long-lived `bash`/`python` the model launches on
//!    purpose), the live child is stored as a SESSION keyed by id and returned.
//!  - [`InteractiveCommandRegistry::write_stdin`] feeds bytes to a session's stdin (the
//!    model includes its own newline / control bytes like `\x03`/`\x04`) and
//!    polls more output. State (cwd, env, shell vars) persists naturally for the
//!    life of that one child — exactly how a persistent shell works.
//!
//! Completion is OS process-exit (no in-band sentinels). Sessions survive across
//! turns; a bounded store with LRU eviction caps live children.

use crate::sync_ext::MutexExt;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex as AsyncMutex, Notify};

use super::{CommandOutputBuffer, CommandSpawnRequest};
use crate::network_proxy::ManagedNetworkSandboxContext;
use crate::runtime_process::{
    RuntimeChildPtyReader, RuntimeChildPtyWriter, RuntimeChildStderr, RuntimeChildStdin,
    RuntimeChildStdout, RuntimeEnv, RuntimeProcessContext, RuntimeProcessController,
    RuntimeProcessSurface, RuntimeSignal, RuntimeSpawnError, RuntimeSpawnRequest,
    SpawnedRuntimeProcess,
};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeDecode, RuntimeEncode, RuntimeEncoder, RuntimeJson,
    RuntimeObjectEncoder, RuntimePath, RuntimeString,
};
use crate::tools::shell::Signal;
use crate::types::abort::AbortSignal;
use crate::types::sandbox::{SandboxBackend, SpawnSpec};

const DEFAULT_MAX_OUTPUT: usize = 256 * 1024;
const MAX_SESSIONS: usize = 64;
/// Trailing-output drain after the child exits, so last bytes aren't lost.
const POST_EXIT_DRAIN_MS: u64 = 50;
/// After output goes quiet for this long, return early from a yield wait.
const SETTLE_MS: f64 = 150.0;
/// The EOT control byte (`Ctrl-D`). Over a pipe this is just a byte, so it is
/// treated as an explicit request to close stdin rather than passed through.
const EOT: &str = "\u{4}";

/// Env forced on session children so REPL/pager output over a pipe stays clean
/// and parseable. Overrides inherited values.
const SESSION_ENV: [(&str, &str); 4] = [
    ("TERM", "dumb"),
    ("NO_COLOR", "1"),
    ("PAGER", "cat"),
    ("GIT_PAGER", "cat"),
];

const PTY_ENV: [(&str, &str); 2] = [("TERM", "xterm-256color"), ("COLORTERM", "truecolor")];
const DEFAULT_TERMINAL_COLS: u16 = 80;
const DEFAULT_TERMINAL_ROWS: u16 = 24;
const MAX_TERMINAL_DIMENSION: u16 = 10_000;
const KILL_FALLBACK_DELAY_SECS: u64 = 5;

fn overlay_session_environment(environment: &mut RuntimeEnv, terminal: bool) {
    let overlay = if terminal {
        &PTY_ENV[..]
    } else {
        &SESSION_ENV[..]
    };
    for (key, value) in overlay {
        environment.insert(RuntimeString::from(*key), RuntimeString::from(*value));
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

/// Clock injected into the manager (milliseconds since the epoch).
type NowFn = Arc<dyn Fn() -> f64 + Send + Sync>;
/// Session-id generator injected into the manager.
type UuidFn = Arc<dyn Fn() -> String + Send + Sync>;

fn clamp(v: i64, lo: i64, hi: i64) -> i64 {
    v.min(hi).max(lo)
}

async fn delay(ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

fn default_now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as f64)
        .unwrap_or(0.0)
}

/// A random v4 UUID string (`xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx`).
fn random_uuid() -> String {
    use rand::RngCore;
    let mut b = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut b);
    b[6] = (b[6] & 0x0f) | 0x40; // version 4
    b[8] = (b[8] & 0x3f) | 0x80; // variant 1
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13],
        b[14], b[15]
    )
}

struct SessionTerminator {
    controller: Option<RuntimeProcessController>,
}

impl SessionTerminator {
    fn new(controller: Option<RuntimeProcessController>) -> Self {
        Self { controller }
    }

    fn signal(&mut self, signal: Signal) {
        if let Some(controller) = &self.controller {
            controller.signal(runtime_signal(signal));
        }
    }
}

type SharedTerminator = Arc<Mutex<SessionTerminator>>;

fn signal_session(terminator: &SharedTerminator, signal: Signal) {
    if let Ok(mut terminator) = terminator.lock() {
        terminator.signal(signal);
    }
}

fn signal_session_with_escalation(terminator: SharedTerminator, signal: Signal) {
    signal_session(&terminator, signal);
    if signal != Signal::Kill {
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(KILL_FALLBACK_DELAY_SECS)).await;
            signal_session(&terminator, Signal::Kill);
        });
    }
}

/// Mutable per-session state shared with the background reader task.
struct InnerState {
    buffer: CommandOutputBuffer,
    cursor: usize,
    exited: bool,
    exit_code: Option<i32>,
    /// Timestamp of the last output byte (0 = none yet) — drives settle.
    last_data_at: f64,
    last_used: f64,
}

/// The lock-protected state plus the notifier that fires on output or exit.
struct SharedState {
    inner: Mutex<InnerState>,
    /// Fired on output or exit, to re-evaluate a pending wait.
    notify: Notify,
}

struct Session {
    id: RuntimeString,
    command: RuntimeString,
    shared: Arc<SharedState>,
    /// The child's writable input transport, or `None` once closed / on a child
    /// that never launched.
    input: AsyncMutex<Option<SessionInput>>,
    terminator: SharedTerminator,
    terminal_size: Mutex<Option<TerminalOptions>>,
    /// ConPTY input mapping state, one per session because the carriage-return
    /// collapse must carry across writes.
    #[cfg(windows)]
    conpty_normalizer: Mutex<WindowsTtyInputNormalizer>,
}

enum SessionInput {
    Pipe(RuntimeChildStdin),
    Pty(RuntimeChildPtyWriter),
}

impl SessionInput {
    async fn write_all(&mut self, input: &[u8]) -> io::Result<()> {
        match self {
            SessionInput::Pipe(stdin) => stdin.write_all(input).await,
            SessionInput::Pty(writer) => writer.write_all(input).await,
        }
    }

    fn resize(&mut self, size: TerminalOptions) -> io::Result<bool> {
        match self {
            SessionInput::Pipe(_) => Ok(false),
            SessionInput::Pty(writer) => {
                let size = size.normalized();
                writer.resize(size.rows, size.cols, 0, 0)?;
                Ok(true)
            }
        }
    }
}

/// Stateful normalizer for bytes written to a Windows pseudoconsole.
///
/// ConPTY input normalization: Enter is carriage-return, existing CRLF is
/// collapsed across writes, backspace becomes DEL, and all other bytes pass
/// through unchanged. Applied in `write_stdin` when the session's writer is a
/// direct ConPTY writer; the sandbox pty bridge needs no mapping because its
/// inner side applies unix line discipline.
#[cfg(any(windows, test))]
#[derive(Default)]
struct WindowsTtyInputNormalizer {
    previous_was_cr: bool,
}

#[cfg(any(windows, test))]
impl WindowsTtyInputNormalizer {
    fn normalize(&mut self, bytes: &[u8]) -> Vec<u8> {
        let mut normalized = Vec::with_capacity(bytes.len());
        for &byte in bytes {
            match byte {
                b'\x08' => normalized.push(b'\x7f'),
                b'\n' => {
                    if !self.previous_was_cr {
                        normalized.push(b'\r');
                    }
                }
                _ => normalized.push(byte),
            }
            self.previous_was_cr = byte == b'\r';
        }
        normalized
    }
}

struct SpawnedSession {
    input: Option<SessionInput>,
    terminator: SessionTerminator,
}

/// Append output, stamp the arrival time, and wake any pending wait.
fn push_output(shared: &SharedState, chunk: &[u8], at: f64) {
    {
        let mut inner = shared.inner.lock_recover();
        inner.buffer.push(chunk);
        inner.last_data_at = at;
    }
    shared.notify.notify_waiters();
}

/// Record the child's exit (idempotent) and wake any pending wait.
fn mark_exited(shared: &SharedState, code: Option<i32>) {
    {
        let mut inner = shared.inner.lock_recover();
        if inner.exited {
            return;
        }
        inner.exited = true;
        inner.exit_code = code;
    }
    shared.notify.notify_waiters();
}

/// Drain both pipes and wait for the generation-bound runtime child. Completion
/// is published only after both endpoints close, then ownership is disarmed.
async fn read_and_reap(
    mut child: SpawnedRuntimeProcess,
    mut out: RuntimeChildStdout,
    mut err: RuntimeChildStderr,
    shared: Arc<SharedState>,
    now: NowFn,
) {
    let mut out_done = false;
    let mut err_done = false;
    let mut out_chunk = vec![0u8; 65536];
    let mut err_chunk = vec![0u8; 65536];
    while !(out_done && err_done) {
        tokio::select! {
            n = out.read(&mut out_chunk[..]), if !out_done => {
                match n {
                    Ok(0) | Err(_) => out_done = true,
                    Ok(n) => push_output(&shared, &out_chunk[..n], (now)()),
                }
            }
            n = err.read(&mut err_chunk[..]), if !err_done => {
                match n {
                    Ok(0) | Err(_) => err_done = true,
                    Ok(n) => push_output(&shared, &err_chunk[..n], (now)()),
                }
            }
        }
    }
    let code = match child.wait_terminal().await {
        Ok(status) => status.code(),
        Err(_) => None,
    };
    mark_exited(&shared, code);
    child.disarm();
}

async fn read_pty_and_reap(
    mut child: SpawnedRuntimeProcess,
    mut reader: RuntimeChildPtyReader,
    shared: Arc<SharedState>,
    now: NowFn,
) {
    let mut chunk = vec![0u8; 65536];
    loop {
        match reader.read(&mut chunk[..]).await {
            Ok(0) => break,
            Ok(n) => push_output(&shared, &chunk[..n], (now)()),
            Err(_) => break,
        }
    }
    let code = match child.wait_terminal().await {
        Ok(status) => status.code(),
        Err(_) => None,
    };
    mark_exited(&shared, code);
    child.disarm();
}

/// Parameters for [`InteractiveCommandRegistry::exec`].
pub struct ExecCommandParams {
    pub command: RuntimeString,
    pub cwd: RuntimePath,
    pub sandbox: Arc<dyn SandboxBackend>,
    pub env: Option<RuntimeEnv>,
    pub network_allowed: Option<bool>,
    pub managed_network: Option<ManagedNetworkSandboxContext>,
    pub writable_roots: Option<Vec<RuntimeString>>,
    /// Waive the sandbox's write confinement; see
    /// [`CommandSpawnRequest::unconfined_writes`].
    pub unconfined_writes: bool,
    /// How long to wait for the command to finish before handing back a session.
    pub yield_ms: Option<i64>,
    pub max_output_bytes: Option<usize>,
    pub signal: Option<AbortSignal>,
    /// Request a real terminal transport instead of the default pipe transport.
    pub terminal: Option<TerminalOptions>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecCommandResult {
    pub output: RuntimeString,
    pub exited: bool,
    pub exit_code: Option<i32>,
    /// Non-null when the child is still alive and stored as a session.
    pub session_id: Option<RuntimeString>,
    /// Identifier of this response's output chunk; empty on legacy paths that
    /// predate chunked collection.
    pub chunk_id: RuntimeString,
    /// Milliseconds the collection window actually observed the process.
    pub wall_time_ms: u64,
    /// Approximate tokens over every byte the window observed, including bytes
    /// dropped by the collection cap.
    pub original_token_count: u64,
    /// Bytes dropped by the collection cap before model-facing truncation.
    pub output_omitted_bytes: Option<u64>,
}

impl RuntimeEncode for ExecCommandResult {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("output", &self.output)?;
        object.field("exited", &self.exited)?;
        object.field("exitCode", &self.exit_code)?;
        object.field("sessionId", &self.session_id)?;
        object.field("chunkId", &self.chunk_id)?;
        object.field("wallTimeMs", &self.wall_time_ms)?;
        object.field("originalTokenCount", &self.original_token_count)?;
        object.field("outputOmittedBytes", &self.output_omitted_bytes)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ExecCommandResult {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                output: object.required("output", &[])?,
                exited: object.required("exited", &[])?,
                exit_code: object.optional("exitCode", &[])?,
                session_id: object.optional("sessionId", &[])?,
                chunk_id: object.optional("chunkId", &[])?.unwrap_or_default(),
                wall_time_ms: object.optional("wallTimeMs", &[])?.unwrap_or(0),
                original_token_count: object.optional("originalTokenCount", &[])?.unwrap_or(0),
                output_omitted_bytes: object.optional("outputOmittedBytes", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TerminalOptions {
    pub cols: u16,
    pub rows: u16,
}

impl RuntimeEncode for TerminalOptions {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self { cols, rows } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("cols", cols)?;
        object.field("rows", rows)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for TerminalOptions {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                cols: object.required("cols", &[])?,
                rows: object.required("rows", &[])?,
            })
        })
    }
}

impl Default for TerminalOptions {
    fn default() -> Self {
        Self {
            cols: DEFAULT_TERMINAL_COLS,
            rows: DEFAULT_TERMINAL_ROWS,
        }
    }
}

impl TerminalOptions {
    fn normalized(self) -> Self {
        Self {
            cols: self.cols.clamp(1, MAX_TERMINAL_DIMENSION),
            rows: self.rows.clamp(1, MAX_TERMINAL_DIMENSION),
        }
    }
}

/// Parameters for [`InteractiveCommandRegistry::write_stdin`].
pub struct WriteStdinParams {
    pub session_id: RuntimeString,
    /// Raw bytes to send (include your own newline / control chars). Empty = poll.
    pub input: Option<RuntimeString>,
    pub yield_ms: Option<i64>,
    pub signal: Option<AbortSignal>,
    /// Resize a PTY-backed session before polling/writing input.
    pub resize: Option<TerminalOptions>,
    /// Include a non-consuming snapshot of the latest retained output bytes.
    pub tail_bytes: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteStdinResult {
    pub found: bool,
    pub output: RuntimeString,
    pub exited: bool,
    pub exit_code: Option<i32>,
    pub session_id: Option<RuntimeString>,
    pub terminal: bool,
    pub terminal_size: Option<TerminalOptions>,
    pub resized: bool,
    pub tail: Option<RuntimeString>,
    /// Identifier of this response's output chunk; empty on legacy paths that
    /// predate chunked collection.
    pub chunk_id: RuntimeString,
    /// Milliseconds the collection window actually observed the process.
    pub wall_time_ms: u64,
    /// Approximate tokens over every byte the window observed, including bytes
    /// dropped by the collection cap.
    pub original_token_count: u64,
    /// Bytes dropped by the collection cap before model-facing truncation.
    pub output_omitted_bytes: Option<u64>,
}

impl RuntimeEncode for WriteStdinResult {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("found", &self.found)?;
        object.field("output", &self.output)?;
        object.field("exited", &self.exited)?;
        object.field("exitCode", &self.exit_code)?;
        object.field("sessionId", &self.session_id)?;
        object.field("terminal", &self.terminal)?;
        object.field("terminalSize", &self.terminal_size)?;
        object.field("resized", &self.resized)?;
        object.field("tail", &self.tail)?;
        object.field("chunkId", &self.chunk_id)?;
        object.field("wallTimeMs", &self.wall_time_ms)?;
        object.field("originalTokenCount", &self.original_token_count)?;
        object.field("outputOmittedBytes", &self.output_omitted_bytes)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for WriteStdinResult {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                found: object.required("found", &[])?,
                output: object.required("output", &[])?,
                exited: object.required("exited", &[])?,
                exit_code: object.optional("exitCode", &[])?,
                session_id: object.optional("sessionId", &[])?,
                terminal: object.required("terminal", &[])?,
                terminal_size: object.optional("terminalSize", &[])?,
                resized: object.required("resized", &[])?,
                tail: object.optional("tail", &[])?,
                chunk_id: object.optional("chunkId", &[])?.unwrap_or_default(),
                wall_time_ms: object.optional("wallTimeMs", &[])?.unwrap_or(0),
                original_token_count: object.optional("originalTokenCount", &[])?.unwrap_or(0),
                output_omitted_bytes: object.optional("outputOmittedBytes", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionListEntry {
    pub id: RuntimeString,
    pub command: RuntimeString,
    pub exited: bool,
    pub exit_code: Option<i32>,
    pub terminal: bool,
    pub terminal_size: Option<TerminalOptions>,
}

impl RuntimeEncode for SessionListEntry {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("id", &self.id)?;
        object.field("command", &self.command)?;
        object.field("exited", &self.exited)?;
        object.field("exitCode", &self.exit_code)?;
        object.field("terminal", &self.terminal)?;
        object.field("terminalSize", &self.terminal_size)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for SessionListEntry {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                id: object.required("id", &[])?,
                command: object.required("command", &[])?,
                exited: object.required("exited", &[])?,
                exit_code: object.optional("exitCode", &[])?,
                terminal: object.required("terminal", &[])?,
                terminal_size: object.optional("terminalSize", &[])?,
            })
        })
    }
}

/// Registry of live interactive shell sessions.
pub struct InteractiveCommandRegistry {
    sessions: Mutex<Vec<Arc<Session>>>,
    max_sessions: usize,
    now: NowFn,
    uuid: UuidFn,
    exit_hook_installed: AtomicBool,
}

impl Default for InteractiveCommandRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl InteractiveCommandRegistry {
    pub fn new() -> Self {
        Self::with_config(MAX_SESSIONS, Arc::new(default_now), Arc::new(random_uuid))
    }

    /// Construct with an explicit session cap and injected clock / id generator.
    pub fn with_config(max_sessions: usize, now: NowFn, uuid: UuidFn) -> Self {
        InteractiveCommandRegistry {
            sessions: Mutex::new(Vec::new()),
            max_sessions,
            now,
            uuid,
            exit_hook_installed: AtomicBool::new(false),
        }
    }

    /// Run a command; return its result if it finished, else a live session id.
    pub async fn exec(
        &self,
        process_context: RuntimeProcessContext,
        params: ExecCommandParams,
    ) -> ExecCommandResult {
        self.install_exit_hook();
        self.prune_if_needed();

        let request = CommandSpawnRequest {
            command: params.command.clone(),
            cwd: params.cwd.clone(),
            env: params.env.clone(),
            writable_roots: params
                .writable_roots
                .clone()
                .unwrap_or_else(|| vec![params.cwd.as_runtime_string().clone()]),
            network_allowed: params.network_allowed.unwrap_or(false),
            managed_network: params.managed_network.clone(),
            unconfined_writes: params.unconfined_writes,
        };
        let mut spec = request.to_spawn_spec(params.sandbox.as_ref());

        let max_output = params.max_output_bytes.unwrap_or(DEFAULT_MAX_OUTPUT);
        let shared = Arc::new(SharedState {
            inner: Mutex::new(InnerState {
                buffer: CommandOutputBuffer::new(max_output),
                cursor: 0,
                exited: false,
                exit_code: None,
                last_data_at: 0.0,
                last_used: (self.now)(),
            }),
            notify: Notify::new(),
        });
        let id = RuntimeString::from((self.uuid)());

        let terminal = params.terminal.map(TerminalOptions::normalized);
        overlay_session_environment(&mut spec.env, terminal.is_some());
        let spawned = match terminal {
            Some(term) => {
                self.spawn_pty_session(&process_context, spec, term, shared.clone())
                    .await
            }
            None => {
                self.spawn_pipe_session(&process_context, spec, shared.clone())
                    .await
            }
        };

        let (input_handle, terminator) = match spawned {
            Ok(spawned) => (spawned.input, spawned.terminator),
            Err(e) => {
                // The OS failed to launch the child: surface the message and
                // treat it as an immediately-exited one-shot.
                let msg = RuntimeString::from("\n[spawn error] ")
                    .concat(e.message())
                    .concat(&RuntimeString::from("\n"));
                push_output(&shared, &msg.to_node_utf8(), (self.now)());
                mark_exited(&shared, None);
                (None, SessionTerminator::new(None))
            }
        };

        let session = Arc::new(Session {
            id: id.clone(),
            command: params.command.clone(),
            shared: shared.clone(),
            input: AsyncMutex::new(input_handle),
            terminator: Arc::new(Mutex::new(terminator)),
            terminal_size: Mutex::new(terminal),
            #[cfg(windows)]
            conpty_normalizer: Mutex::new(WindowsTtyInputNormalizer::default()),
        });

        let yield_ms = clamp(params.yield_ms.unwrap_or(10_000), 250, 30_000);
        let deadline = (self.now)() + yield_ms as f64;
        // exec waits the full window (no settle): a command that pauses mid-run
        // must not be prematurely turned into a session — only exit/deadline
        // short-circuit.
        self.wait_for_output_settle(&shared, deadline, f64::INFINITY, params.signal.as_ref())
            .await;

        if self.snapshot_exited(&shared) {
            delay(POST_EXIT_DRAIN_MS).await;
        }
        let text = self.drain(&shared);
        let (exited, exit_code) = self.snapshot_exit(&shared);
        if exited {
            // One-shot: do not store.
            return ExecCommandResult {
                output: text,
                exited: true,
                exit_code,
                session_id: None,
                chunk_id: RuntimeString::default(),
                wall_time_ms: 0,
                original_token_count: 0,
                output_omitted_bytes: None,
            };
        }
        self.sessions.lock_recover().push(session);
        ExecCommandResult {
            output: text,
            exited: false,
            exit_code: None,
            session_id: Some(id),
            chunk_id: RuntimeString::default(),
            wall_time_ms: 0,
            original_token_count: 0,
            output_omitted_bytes: None,
        }
    }

    async fn spawn_pipe_session(
        &self,
        process_context: &RuntimeProcessContext,
        spec: SpawnSpec,
        shared: Arc<SharedState>,
    ) -> Result<SpawnedSession, RuntimeSpawnError> {
        let spawner = process_context.spawner();
        let request =
            RuntimeSpawnRequest::from_spawn_spec(spec, RuntimeProcessSurface::ShellSession);
        let mut child = spawner.spawn(request).await?;
        let controller = child.controller();
        let stdout = child.take_stdout().ok_or_else(|| {
            RuntimeSpawnError::stdio_shape(std::io::Error::other(
                "interactive pipe session was spawned without a stdout pipe",
            ))
        })?;
        let stderr = child.take_stderr().ok_or_else(|| {
            RuntimeSpawnError::stdio_shape(std::io::Error::other(
                "interactive pipe session was spawned without a stderr pipe",
            ))
        })?;
        let stdin = child.take_stdin().map(SessionInput::Pipe);
        let shared_task = shared.clone();
        let now_task = self.now.clone();
        tokio::spawn(read_and_reap(child, stdout, stderr, shared_task, now_task));

        Ok(SpawnedSession {
            input: stdin,
            terminator: SessionTerminator::new(Some(controller)),
        })
    }

    async fn spawn_pty_session(
        &self,
        process_context: &RuntimeProcessContext,
        spec: SpawnSpec,
        terminal: TerminalOptions,
        shared: Arc<SharedState>,
    ) -> Result<SpawnedSession, RuntimeSpawnError> {
        let spawner = process_context.spawner();
        let request = RuntimeSpawnRequest::from_spawn_spec(
            spec,
            RuntimeProcessSurface::InteractivePty {
                rows: terminal.rows,
                columns: terminal.cols,
            },
        );
        let mut child = spawner.spawn(request).await?;
        let controller = child.controller();
        let master = child.take_pty().ok_or_else(|| {
            RuntimeSpawnError::stdio_shape(std::io::Error::other(
                "interactive PTY session was spawned without a PTY master",
            ))
        })?;
        let (reader, writer) = master.split().map_err(RuntimeSpawnError::stdio_shape)?;
        let now = self.now.clone();
        tokio::spawn(read_pty_and_reap(child, reader, shared, now));
        Ok(SpawnedSession {
            input: Some(SessionInput::Pty(writer)),
            terminator: SessionTerminator::new(Some(controller)),
        })
    }

    /// Send input to a live session (or just poll with empty input).
    pub async fn write_stdin(&self, params: WriteStdinParams) -> WriteStdinResult {
        let session = {
            let sessions = self.sessions.lock_recover();
            sessions.iter().find(|s| s.id == params.session_id).cloned()
        };
        let Some(session) = session else {
            return WriteStdinResult {
                found: false,
                output: RuntimeString::default(),
                exited: true,
                exit_code: None,
                session_id: None,
                terminal: false,
                terminal_size: None,
                resized: false,
                tail: None,
                chunk_id: RuntimeString::default(),
                wall_time_ms: 0,
                original_token_count: 0,
                output_omitted_bytes: None,
            };
        };
        {
            session.shared.inner.lock_recover().last_used = (self.now)();
        }

        let mut resized = false;
        if let Some(size) = params.resize.map(TerminalOptions::normalized) {
            if !self.snapshot_exited(&session.shared) {
                if let Some(input) = session.input.lock().await.as_mut() {
                    resized = input.resize(size).unwrap_or(false);
                    if resized {
                        *session.terminal_size.lock_recover() = Some(size);
                    }
                }
            }
        }
        let is_terminal = session.terminal_size.lock_recover().is_some();
        let input = params.input.clone().unwrap_or_default();
        if !input.is_empty() && !self.snapshot_exited(&session.shared) {
            if input == EOT && !is_terminal {
                // Over a pipe `\x04` is just a byte, not EOF (only a PTY treats
                // it as EOF). Honor the intent by closing stdin so e.g. `cat`
                // terminates.
                let taken = session.input.lock().await.take();
                drop(taken);
            } else if let Some(s) = session.input.lock().await.as_mut() {
                let input = input.to_node_utf8();
                #[cfg(windows)]
                let input = match s {
                    SessionInput::Pty(writer) if writer.is_conpty() => {
                        session.conpty_normalizer.lock_recover().normalize(&input)
                    }
                    _ => input,
                };
                let _ = s.write_all(&input).await;
            }
            delay(100).await; // let the child react before polling
        }

        let default_yield = if !input.is_empty() { 10_000 } else { 5_000 };
        let yield_ms = clamp(params.yield_ms.unwrap_or(default_yield), 250, 60_000);
        let deadline = (self.now)() + yield_ms as f64;
        self.wait_for_output_settle(&session.shared, deadline, SETTLE_MS, params.signal.as_ref())
            .await;

        if self.snapshot_exited(&session.shared) {
            delay(POST_EXIT_DRAIN_MS).await;
        }
        let tail = params
            .tail_bytes
            .filter(|bytes| *bytes > 0)
            .map(|bytes| self.tail(&session.shared, bytes));
        let text = self.drain(&session.shared);
        let (exited, exit_code) = self.snapshot_exit(&session.shared);
        let terminal_size = *session.terminal_size.lock_recover();
        if exited {
            let mut sessions = self.sessions.lock_recover();
            if let Some(i) = sessions.iter().position(|s| s.id == session.id) {
                sessions.remove(i);
            }
            return WriteStdinResult {
                found: true,
                output: text,
                exited: true,
                exit_code,
                session_id: None,
                terminal: terminal_size.is_some(),
                terminal_size,
                resized,
                tail,
                chunk_id: RuntimeString::default(),
                wall_time_ms: 0,
                original_token_count: 0,
                output_omitted_bytes: None,
            };
        }
        WriteStdinResult {
            found: true,
            output: text,
            exited: false,
            exit_code: None,
            session_id: Some(session.id.clone()),
            terminal: terminal_size.is_some(),
            terminal_size,
            resized,
            tail,
            chunk_id: RuntimeString::default(),
            wall_time_ms: 0,
            original_token_count: 0,
            output_omitted_bytes: None,
        }
    }

    /// Kill a session and its process tree. Returns false if unknown.
    pub fn kill(&self, id: &RuntimeString) -> bool {
        self.kill_with_signal(id, Signal::Kill)
    }

    /// Signal a session without removing it from the live registry.
    pub fn signal(&self, id: &RuntimeString, signal: Signal) -> bool {
        let sessions = self.sessions.lock_recover();
        let Some(session) = sessions.iter().find(|s| s.id.eq(id)) else {
            return false;
        };
        let terminator = Arc::clone(&session.terminator);
        drop(sessions);
        signal_session(&terminator, signal);
        true
    }

    /// Signal a session and remove it from the live registry. Non-`SIGKILL`
    /// signals get a delayed hard-kill fallback through the PTY host
    /// control path.
    pub fn kill_with_signal(&self, id: &RuntimeString, signal: Signal) -> bool {
        let mut sessions = self.sessions.lock_recover();
        let Some(idx) = sessions.iter().position(|s| s.id.eq(id)) else {
            return false;
        };
        let terminator = Arc::clone(&sessions[idx].terminator);
        sessions.remove(idx);
        signal_session_with_escalation(terminator, signal);
        true
    }

    /// True when a session with this id exists.
    pub fn has(&self, id: &RuntimeString) -> bool {
        self.sessions.lock_recover().iter().any(|s| s.id.eq(id))
    }

    pub fn list(&self) -> Vec<SessionListEntry> {
        self.sessions
            .lock_recover()
            .iter()
            .map(|s| {
                let inner = s.shared.inner.lock_recover();
                let terminal_size = *s.terminal_size.lock_recover();
                SessionListEntry {
                    id: s.id.clone(),
                    command: s.command.clone(),
                    exited: inner.exited,
                    exit_code: inner.exit_code,
                    terminal: terminal_size.is_some(),
                    terminal_size,
                }
            })
            .collect()
    }

    /// Kill every live session. Idempotent.
    pub fn dispose_all(&self) {
        let mut sessions = self.sessions.lock_recover();
        for s in sessions.iter() {
            signal_session(&s.terminator, Signal::Kill);
        }
        sessions.clear();
    }

    fn snapshot_exited(&self, shared: &SharedState) -> bool {
        shared.inner.lock_recover().exited
    }

    fn snapshot_exit(&self, shared: &SharedState) -> (bool, Option<i32>) {
        let inner = shared.inner.lock_recover();
        (inner.exited, inner.exit_code)
    }

    /// Read everything appended since the session's cursor.
    fn drain(&self, shared: &SharedState) -> RuntimeString {
        let mut inner = shared.inner.lock_recover();
        let r = inner.buffer.read_from(inner.cursor);
        inner.cursor = r.next_cursor;
        r.text
    }

    /// Read a retained tail snapshot without advancing the session cursor.
    fn tail(&self, shared: &SharedState, max_bytes: usize) -> RuntimeString {
        let inner = shared.inner.lock_recover();
        let end = inner.buffer.end();
        let start = end.saturating_sub(max_bytes);
        inner.buffer.read_from(start).text
    }

    /// Wait until the child exits, the deadline passes, the signal aborts, OR the
    /// output has arrived and then gone quiet for `settle_ms` — so an interactive
    /// command returns shortly after it responds instead of always blocking the
    /// full window. A command that produces no output yet waits the full deadline
    /// (so a slow-but-finishing command isn't prematurely turned into a session).
    async fn wait_for_output_settle(
        &self,
        shared: &SharedState,
        deadline: f64,
        settle_ms: f64,
        signal: Option<&AbortSignal>,
    ) {
        loop {
            // Register interest in a wake BEFORE reading state, so a notification
            // that fires between our read and our await is not lost.
            let notified = shared.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let (exited, last_data_at) = {
                let inner = shared.inner.lock_recover();
                (inner.exited, inner.last_data_at)
            };
            if exited {
                return;
            }
            if let Some(s) = signal {
                if s.is_cancelled() {
                    return;
                }
            }

            let now = (self.now)();
            if now >= deadline {
                return;
            }
            // Output arrived and then settled → return promptly.
            if last_data_at > 0.0 && now - last_data_at >= settle_ms {
                return;
            }
            let settle_point = if last_data_at > 0.0 {
                last_data_at + settle_ms
            } else {
                deadline
            };
            let wait_ms = f64::max(1.0, f64::min(deadline, settle_point) - now);

            let abort = async {
                match signal {
                    Some(s) => s.cancelled().await,
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(wait_ms as u64)) => {}
                _ = notified.as_mut() => {}
                _ = abort => {}
            }
        }
    }

    /// Evict exited (then oldest) sessions once the live cap is exceeded.
    fn prune_if_needed(&self) {
        let mut sessions = self.sessions.lock_recover();
        while sessions.len() >= self.max_sessions {
            let victim_idx = {
                let exited = sessions
                    .iter()
                    .position(|s| s.shared.inner.lock_recover().exited);
                match exited {
                    Some(i) => i,
                    None => {
                        let mut best = 0usize;
                        let mut best_used = sessions[0].shared.inner.lock_recover().last_used;
                        for i in 1..sessions.len() {
                            let used = sessions[i].shared.inner.lock_recover().last_used;
                            if used < best_used {
                                best = i;
                                best_used = used;
                            }
                        }
                        best
                    }
                }
            };
            let victim = sessions.remove(victim_idx);
            signal_session(&victim.terminator, Signal::Kill);
        }
    }

    fn install_exit_hook(&self) {
        self.exit_hook_installed.swap(true, Ordering::SeqCst);
        // Best-effort process-exit cleanup. Spawned children run in their own
        // detached process groups, so the OS reaps them when this process
        // exits; callers can also invoke `dispose_all` explicitly. There is no
        // portable in-process exit hook that can reach `&self` here.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_process::ParentEnvSnapshot;
    use crate::types::sandbox::{ExecRequest, SandboxKind, SandboxPolicy, SpawnSpec};

    /// Whether the pty cases below can run here, printing the reason when they
    /// cannot. A host whose system-wide ptmx ceiling is already taken fails these
    /// on a missing exit code, which reads as a defect in the command runtime
    /// rather than as an exhausted host.
    #[cfg(unix)]
    fn skip_without_a_pty() -> bool {
        if crate::runtime_process::pty_allocatable() {
            return true;
        }
        eprintln!("skipping pty test: this environment cannot allocate a pty");
        false
    }

    /// A minimal env-inheriting sandbox for tests: `argv[0]` is the file, the
    /// rest are args, and the child inherits this process's environment (plus any
    /// caller overrides) so `bash`/`cat` resolve on PATH.
    struct TestSandbox;

    impl SandboxBackend for TestSandbox {
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
            if let Some(overlay) = &req.env {
                env.overlay(overlay);
            }
            SpawnSpec {
                file,
                args,
                cwd: req.cwd.clone(),
                env,
            }
        }
    }

    struct CaptureSandbox {
        captured: Arc<Mutex<Option<(ExecRequest, SandboxPolicy)>>>,
    }

    impl SandboxBackend for CaptureSandbox {
        fn kind(&self) -> SandboxKind {
            SandboxKind::Process
        }

        fn is_available(&self) -> bool {
            true
        }

        fn transform(&self, req: &ExecRequest, policy: &SandboxPolicy) -> SpawnSpec {
            *self.captured.lock().unwrap() = Some((req.clone(), policy.clone()));
            SpawnSpec {
                file: req.argv[0].clone(),
                args: req.argv[1..].to_vec(),
                cwd: req.cwd.clone(),
                env: req.env.clone().unwrap_or_default(),
            }
        }
    }

    fn cwd() -> RuntimePath {
        RuntimePath::from(
            std::env::current_dir()
                .unwrap()
                .to_string_lossy()
                .into_owned(),
        )
    }

    /// Project an exact runtime string for assertion-failure messages only.
    #[cfg(unix)]
    fn ui_string(value: &RuntimeString) -> String {
        String::from_utf8(value.to_node_utf8()).expect("Node UTF-8 UI projection is valid UTF-8")
    }

    async fn exec(
        m: &InteractiveCommandRegistry,
        command: &str,
        yield_ms: i64,
    ) -> ExecCommandResult {
        m.exec(
            crate::runtime_process::test_runtime_process_context(),
            ExecCommandParams {
                command: RuntimeString::from(command),
                cwd: cwd(),
                sandbox: Arc::new(TestSandbox),
                env: None,
                network_allowed: None,
                managed_network: None,
                writable_roots: None,
                unconfined_writes: false,
                yield_ms: Some(yield_ms),
                max_output_bytes: None,
                signal: None,
                terminal: None,
            },
        )
        .await
    }

    #[tokio::test]
    async fn runs_a_one_shot_command_output_plus_exit_code_no_session_stored() {
        let m = InteractiveCommandRegistry::new();
        let r = exec(&m, "echo hello", 2000).await;
        assert!(r.exited);
        assert_eq!(r.exit_code, Some(0));
        assert!(r.output.contains("hello"));
        assert_eq!(r.session_id, None);
        assert_eq!(m.list().len(), 0);
        m.dispose_all();
    }

    #[tokio::test]
    async fn propagates_a_non_zero_exit_code() {
        let m = InteractiveCommandRegistry::new();
        let r = exec(&m, "exit 3", 2000).await;
        assert!(r.exited);
        assert_eq!(r.exit_code, Some(3));
        m.dispose_all();
    }

    #[tokio::test]
    async fn keeps_a_live_command_as_a_session_and_interacts_via_write_stdin() {
        let m = InteractiveCommandRegistry::new();
        let r = exec(&m, "cat", 250).await; // cat stays alive reading stdin
        assert!(!r.exited);
        assert!(r.session_id.is_some());
        let id = r.session_id.clone().unwrap();
        assert!(m.has(&id));

        let echoed = m
            .write_stdin(WriteStdinParams {
                session_id: id.clone(),
                input: Some(RuntimeString::from("ping\n")),
                yield_ms: Some(4000),
                signal: None,
                resize: None,
                tail_bytes: None,
            })
            .await;
        assert!(echoed.found);
        assert!(echoed.output.contains("ping")); // cat echoed it back
        assert!(!echoed.exited);
        m.dispose_all();
    }

    #[tokio::test]
    async fn persists_shell_state_cwd_across_inputs_in_one_session() {
        let m = InteractiveCommandRegistry::new();
        let r = exec(&m, "bash", 250).await; // a long-lived shell reading stdin
        let id = r.session_id.clone().unwrap();
        assert!(!id.is_empty());
        m.write_stdin(WriteStdinParams {
            session_id: id.clone(),
            input: Some(RuntimeString::from("cd /tmp\n")),
            yield_ms: Some(2000),
            signal: None,
            resize: None,
            tail_bytes: None,
        })
        .await;
        let pwd = m
            .write_stdin(WriteStdinParams {
                session_id: id.clone(),
                input: Some(RuntimeString::from("pwd\n")),
                yield_ms: Some(4000),
                signal: None,
                resize: None,
                tail_bytes: None,
            })
            .await;
        assert!(pwd.output.contains("tmp")); // cd persisted within the session
        m.dispose_all();
    }

    #[tokio::test]
    async fn exits_a_session_when_sent_eof_clearing_it_from_the_store() {
        let m = InteractiveCommandRegistry::new();
        let r = exec(&m, "cat", 250).await;
        let id = r.session_id.clone().unwrap();
        let done = m
            .write_stdin(WriteStdinParams {
                session_id: id.clone(),
                input: Some(RuntimeString::from("\u{4}")),
                yield_ms: Some(4000),
                signal: None,
                resize: None,
                tail_bytes: None,
            })
            .await; // EOF
        assert!(done.exited);
        assert_eq!(done.session_id, None);
        assert!(!m.has(&id));
        m.dispose_all();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pty_exec_presents_stdout_as_a_terminal() {
        if !skip_without_a_pty() {
            return;
        }
        let m = InteractiveCommandRegistry::new();
        let r = m
            .exec(
                crate::runtime_process::test_runtime_process_context(),
                ExecCommandParams {
                    command: RuntimeString::from("test -t 1 && echo tty-ready"),
                    cwd: cwd(),
                    sandbox: Arc::new(TestSandbox),
                    env: None,
                    network_allowed: None,
                    managed_network: None,
                    writable_roots: None,
                    unconfined_writes: false,
                    yield_ms: Some(2000),
                    max_output_bytes: Some(1024),
                    signal: None,
                    terminal: Some(TerminalOptions { cols: 80, rows: 24 }),
                },
            )
            .await;

        assert!(r.exited, "result: {r:?}");
        assert_eq!(r.exit_code, Some(0));
        assert!(
            r.output.contains("tty-ready"),
            "output: {}",
            ui_string(&r.output)
        );
        assert_eq!(r.session_id, None);
        m.dispose_all();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pty_sessions_accept_terminal_input() {
        if !skip_without_a_pty() {
            return;
        }
        let m = InteractiveCommandRegistry::new();
        let r = m
            .exec(
                crate::runtime_process::test_runtime_process_context(),
                ExecCommandParams {
                    command: RuntimeString::from("cat"),
                    cwd: cwd(),
                    sandbox: Arc::new(TestSandbox),
                    env: None,
                    network_allowed: None,
                    managed_network: None,
                    writable_roots: None,
                    unconfined_writes: false,
                    yield_ms: Some(250),
                    max_output_bytes: Some(1024),
                    signal: None,
                    terminal: Some(TerminalOptions { cols: 80, rows: 24 }),
                },
            )
            .await;

        assert!(!r.exited, "result: {r:?}");
        let id = r.session_id.clone().unwrap();
        let echoed = m
            .write_stdin(WriteStdinParams {
                session_id: id.clone(),
                input: Some(RuntimeString::from("pty-ping\n")),
                yield_ms: Some(4000),
                signal: None,
                resize: None,
                tail_bytes: None,
            })
            .await;

        assert!(echoed.found);
        assert!(
            echoed.output.contains("pty-ping"),
            "output: {}",
            ui_string(&echoed.output)
        );
        assert!(!echoed.exited);
        assert!(m.kill(&id));
        m.dispose_all();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pty_sessions_can_be_resized_after_spawn() {
        if !skip_without_a_pty() {
            return;
        }
        let m = InteractiveCommandRegistry::new();
        let r = m
            .exec(
                crate::runtime_process::test_runtime_process_context(),
                ExecCommandParams {
                    command: RuntimeString::from("bash --noprofile --norc"),
                    cwd: cwd(),
                    sandbox: Arc::new(TestSandbox),
                    env: None,
                    network_allowed: None,
                    managed_network: None,
                    writable_roots: None,
                    unconfined_writes: false,
                    yield_ms: Some(250),
                    max_output_bytes: Some(4096),
                    signal: None,
                    terminal: Some(TerminalOptions { cols: 80, rows: 24 }),
                },
            )
            .await;

        assert!(!r.exited, "result: {r:?}");
        let id = r.session_id.clone().unwrap();
        let resized = m
            .write_stdin(WriteStdinParams {
                session_id: id.clone(),
                input: None,
                yield_ms: Some(250),
                signal: None,
                resize: Some(TerminalOptions {
                    cols: 100,
                    rows: 40,
                }),
                tail_bytes: None,
            })
            .await;

        assert!(resized.found);
        assert!(resized.resized);
        assert_eq!(
            resized.terminal_size,
            Some(TerminalOptions {
                cols: 100,
                rows: 40
            })
        );

        let size = m
            .write_stdin(WriteStdinParams {
                session_id: id.clone(),
                input: Some(RuntimeString::from("stty size\n")),
                yield_ms: Some(4000),
                signal: None,
                resize: None,
                tail_bytes: None,
            })
            .await;
        assert!(
            size.output.contains("40 100"),
            "output: {}",
            ui_string(&size.output)
        );
        assert!(m.kill(&id));
        m.dispose_all();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pty_sessions_return_a_tail_snapshot_for_reattach() {
        if !skip_without_a_pty() {
            return;
        }
        let m = InteractiveCommandRegistry::new();
        let r = m
            .exec(
                crate::runtime_process::test_runtime_process_context(),
                ExecCommandParams {
                    command: RuntimeString::from("cat"),
                    cwd: cwd(),
                    sandbox: Arc::new(TestSandbox),
                    env: None,
                    network_allowed: None,
                    managed_network: None,
                    writable_roots: None,
                    unconfined_writes: false,
                    yield_ms: Some(250),
                    max_output_bytes: Some(4096),
                    signal: None,
                    terminal: Some(TerminalOptions { cols: 80, rows: 24 }),
                },
            )
            .await;

        let id = r.session_id.clone().unwrap();
        let echoed = m
            .write_stdin(WriteStdinParams {
                session_id: id.clone(),
                input: Some(RuntimeString::from("reattach-line\n")),
                yield_ms: Some(4000),
                signal: None,
                resize: None,
                tail_bytes: Some(1024),
            })
            .await;

        assert!(echoed.output.contains("reattach-line"));
        assert!(
            echoed
                .tail
                .as_ref()
                .is_some_and(|tail| tail.contains("reattach-line")),
            "tail: {:?}",
            echoed.tail
        );

        let snapshot = m
            .write_stdin(WriteStdinParams {
                session_id: id.clone(),
                input: None,
                yield_ms: Some(250),
                signal: None,
                resize: None,
                tail_bytes: Some(1024),
            })
            .await;

        assert!(
            snapshot.output.is_empty(),
            "output: {}",
            ui_string(&snapshot.output)
        );
        assert!(
            snapshot
                .tail
                .as_ref()
                .is_some_and(|tail| tail.contains("reattach-line")),
            "tail: {:?}",
            snapshot.tail
        );
        assert!(m.kill(&id));
        m.dispose_all();
    }

    #[test]
    fn terminal_options_match_claude_pty_dimension_bounds() {
        assert_eq!(
            TerminalOptions {
                cols: 0,
                rows: 60_000
            }
            .normalized(),
            TerminalOptions {
                cols: 1,
                rows: 10_000
            }
        );
    }

    // `session_terminator_invokes_attached_child_killer` used to sit here. It injected a
    // `Box<dyn SessionKiller>` through `SessionTerminator::attach_killer` and asserted the
    // forwarding order. Commit 2773475 removed both the trait and that setter: a
    // `SessionTerminator` now holds an `Option<RuntimeProcessController>` and `signal`
    // delegates straight to it, and `RuntimeProcessController` is a concrete type whose
    // `signal` reaches `libc::kill`, so there is no seam left to observe the forwarding
    // without spawning a real child. The test was dropped rather than rewritten into a
    // weaker assertion that would look like coverage without being any.
    //
    // `signal_session_with_escalation` (TERM, then KILL after the grace period) is covered
    // by `kill_with_signal_escalates_to_kill_when_the_child_ignores_term` below, which is a
    // process-level test because that is the only way to observe it: the seam the deleted
    // test used is gone.

    /// `kill_with_signal` sends the requested signal at once and, for anything other than
    /// SIGKILL, schedules an unignorable SIGKILL `KILL_FALLBACK_DELAY_SECS` later. A child
    /// that traps TERM is the only way to tell the two apart, and the child's liveness has
    /// to be observed from outside: `RuntimeProcessController` exposes no pid, and the
    /// session is dropped from the registry by this call.
    ///
    /// The child appends to a file every 100ms, so "still running" is the file still
    /// growing and "killed" is the file size holding steady across two reads. Windows has
    /// no `trap`, and the whole point is a signal the child refuses.
    ///
    /// Every wait here is `tokio::time::sleep().await`, never `std::thread::sleep`: the
    /// hard-kill fallback runs in a `tokio::spawn`ed task, and `#[tokio::test]` gives a
    /// current-thread runtime, so blocking this thread starves that task and the SIGKILL
    /// never fires — which looks exactly like the production bug this test is meant to
    /// catch.
    #[cfg(unix)]
    #[tokio::test]
    async fn kill_with_signal_escalates_to_kill_when_the_child_ignores_term() {
        let marker = std::env::temp_dir().join(format!(
            "klaus-escalation-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let marker_path = marker.to_string_lossy().into_owned();
        let registry = InteractiveCommandRegistry::new();
        let started = exec(
            &registry,
            &format!("trap '' TERM; while :; do printf . >> '{marker_path}'; sleep 0.1; done"),
            300,
        )
        .await;
        let session_id = started
            .session_id
            .clone()
            .expect("a looping command stays alive as a session");

        let size = || std::fs::metadata(&marker).map(|m| m.len()).unwrap_or(0);
        async fn grew_within(size: &dyn Fn() -> u64, deadline: Duration) -> bool {
            let start = size();
            let began = std::time::Instant::now();
            while began.elapsed() < deadline {
                if size() > start {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            false
        }

        assert!(
            grew_within(&size, Duration::from_secs(3)).await,
            "the child never started appending, so the rest proves nothing"
        );

        assert!(
            registry.kill_with_signal(&session_id, Signal::Term),
            "the session must be known to the registry"
        );
        // TERM is trapped, so the child has to survive it. Without this the test would also
        // pass against an implementation that simply killed on the first signal, which is
        // the opposite of what the escalation exists for.
        assert!(
            grew_within(&size, Duration::from_secs(2)).await,
            "the child stopped on SIGTERM, but it traps TERM"
        );

        // Past KILL_FALLBACK_DELAY_SECS the scheduled SIGKILL lands, and SIGKILL cannot be
        // trapped. Allow generous slack: this machine can be heavily loaded.
        let deadline = std::time::Instant::now()
            + Duration::from_secs(KILL_FALLBACK_DELAY_SECS)
            + Duration::from_secs(10);
        let mut stopped = false;
        while std::time::Instant::now() < deadline {
            let before = size();
            tokio::time::sleep(Duration::from_millis(400)).await;
            if size() == before && before > 0 {
                stopped = true;
                break;
            }
        }
        assert!(
            stopped,
            "the child kept appending past the kill fallback, so no SIGKILL arrived"
        );

        registry.dispose_all();
        let _ = std::fs::remove_file(&marker);
    }

    #[tokio::test]
    async fn reports_a_missing_session() {
        let m = InteractiveCommandRegistry::new();
        let r = m
            .write_stdin(WriteStdinParams {
                session_id: RuntimeString::from("nope"),
                input: Some(RuntimeString::from("x\n")),
                yield_ms: None,
                signal: None,
                resize: None,
                tail_bytes: None,
            })
            .await;
        assert!(!r.found);
        m.dispose_all();
    }

    #[tokio::test]
    async fn kills_a_live_session_and_removes_it() {
        let m = InteractiveCommandRegistry::new();
        let r = exec(&m, "cat", 250).await;
        let id = r.session_id.clone().unwrap();
        assert!(m.kill(&id));
        assert!(!m.has(&id));
        assert!(!m.kill(&id)); // already gone
        m.dispose_all();
    }

    #[tokio::test]
    async fn spawn_request_uses_command_runtime_contract() {
        let captured = Arc::new(Mutex::new(None));
        let sandbox = CaptureSandbox {
            captured: Arc::clone(&captured),
        };
        let mut env = RuntimeEnv::new();
        env.insert(RuntimeString::from("K"), RuntimeString::from("V"));

        let m = InteractiveCommandRegistry::new();
        let r = m
            .exec(
                crate::runtime_process::test_runtime_process_context(),
                ExecCommandParams {
                    command: RuntimeString::from("echo hello"),
                    cwd: RuntimePath::from("/tmp"),
                    sandbox: Arc::new(sandbox),
                    env: Some(env.clone()),
                    network_allowed: Some(true),
                    managed_network: None,
                    writable_roots: Some(vec![RuntimeString::from("/tmp/project")]),
                    unconfined_writes: false,
                    yield_ms: Some(250),
                    max_output_bytes: Some(1024),
                    signal: None,
                    terminal: None,
                },
            )
            .await;

        assert!(r.exited);
        let (request, policy) = captured.lock().unwrap().clone().unwrap();
        assert_eq!(request.cwd, RuntimePath::from("/tmp"));
        assert_eq!(request.env, Some(env));
        assert_eq!(
            policy.writable_roots,
            vec![RuntimeString::from("/tmp/project")]
        );
        assert!(policy.network_allowed);
        m.dispose_all();
    }
}

#[cfg(test)]
mod exact_tests {
    use super::*;
    use crate::runtime_process::ParentEnvSnapshot;
    use crate::types::sandbox::{ExecRequest, SandboxKind, SandboxPolicy, SpawnSpec};

    #[test]
    fn windows_tty_input_normalizer_matches_conpty_key_encoding() {
        let mut normalizer = WindowsTtyInputNormalizer::default();

        assert_eq!(normalizer.normalize(b"hello\n"), b"hello\r");
        assert_eq!(normalizer.normalize(b"a\x08b"), b"a\x7fb");
        assert_eq!(normalizer.normalize(b"first\r"), b"first\r");
        assert_eq!(normalizer.normalize(b"\nsecond\r\n"), b"second\r");
        assert_eq!(
            normalizer.normalize("utf8 cafe\u{e9} \u{6f22}\n".as_bytes()),
            "utf8 cafe\u{e9} \u{6f22}\r".as_bytes()
        );
    }

    struct TestSandbox;

    impl SandboxBackend for TestSandbox {
        fn kind(&self) -> SandboxKind {
            SandboxKind::Process
        }

        fn is_available(&self) -> bool {
            true
        }

        fn transform(&self, request: &ExecRequest, _policy: &SandboxPolicy) -> SpawnSpec {
            let mut env = ParentEnvSnapshot::capture();
            if let Some(overlay) = &request.env {
                env.overlay(overlay);
            }
            SpawnSpec {
                file: request.argv.first().cloned().unwrap_or_default(),
                args: request.argv.iter().skip(1).cloned().collect(),
                cwd: request.cwd.clone(),
                env,
            }
        }
    }

    fn cwd() -> RuntimePath {
        RuntimePath::from(
            std::env::current_dir()
                .expect("cwd")
                .to_string_lossy()
                .into_owned(),
        )
    }

    #[tokio::test]
    async fn exact_command_and_session_input_reach_runtime_backend() {
        let registry = InteractiveCommandRegistry::new();
        let started = registry
            .exec(
                crate::runtime_process::test_runtime_process_context(),
                ExecCommandParams {
                    command: RuntimeString::from("cat"),
                    cwd: cwd(),
                    sandbox: Arc::new(TestSandbox),
                    env: None,
                    network_allowed: None,
                    managed_network: None,
                    writable_roots: None,
                    unconfined_writes: false,
                    yield_ms: Some(250),
                    max_output_bytes: Some(1024),
                    signal: None,
                    terminal: None,
                },
            )
            .await;
        let session_id = started.session_id.expect("cat stays alive");
        let result = registry
            .write_stdin(WriteStdinParams {
                session_id: session_id.clone(),
                input: Some(RuntimeString::from("exact-input\n")),
                yield_ms: Some(2_000),
                signal: None,
                resize: None,
                tail_bytes: None,
            })
            .await;
        assert!(result.output.contains("exact-input"));
        assert!(registry.kill(&session_id));
    }
}
