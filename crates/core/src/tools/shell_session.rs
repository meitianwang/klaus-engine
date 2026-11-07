//! Compatibility adapter for interactive shell sessions.
//!
//! The tool-facing API is intentionally still the historical
//! `exec_command`/`write_stdin` shape, but the lifecycle is backed by the
//! process registry so callers also get exec-server-style sequence reads,
//! idempotent writes, typed events, and one shared process table.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use tokio::sync::Mutex as AsyncMutex;

use crate::command_runtime::build_runtime_shell_invocation;
use crate::command_runtime::process::{
    ExecOutputStream, LocalExecProcessRegistry, LocalProcessInfo, LocalProcessReadParams,
    LocalProcessStartParams, LocalProcessWriteParams, ProcessEventReceiver, ProcessOutputChunk,
    TerminalSize as ProcessTerminalSize, WriteStatus,
};
use crate::runtime_process::RuntimeProcessContext;
use crate::runtime_values::{NodeChunkUtf8, RuntimeString};
use crate::tools::shell::Signal;

pub use crate::command_runtime::interactive::{
    ExecCommandParams, ExecCommandResult, SessionListEntry, TerminalOptions, WriteStdinParams,
    WriteStdinResult,
};

/// Process-backed compatibility manager for the exec_command / write_stdin tools.
pub struct ShellSessionManager {
    registry: LocalExecProcessRegistry,
    cursors: Mutex<HashMap<RuntimeString, u64>>,
    failure_states: Mutex<HashMap<RuntimeString, WriteStdinFailureState>>,
    session_gates: Mutex<HashMap<RuntimeString, Weak<AsyncMutex<()>>>>,
    next_write_id: AtomicU64,
}

const FAILURE_TAIL_BYTES: usize = 4 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteStdinErrorClass {
    Terminal,
    Transient,
}

impl WriteStdinErrorClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Terminal => "terminal",
            Self::Transient => "transient",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteStdinErrorCode {
    SessionNotFound,
    SessionExited,
    SessionClosed,
    SessionStarting,
    StdinClosed,
    ResizeFailed,
    WriteFailed,
    ReadFailed,
    ProcessFailed,
}

impl WriteStdinErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SessionNotFound => "session_not_found",
            Self::SessionExited => "session_exited",
            Self::SessionClosed => "session_closed",
            Self::SessionStarting => "session_starting",
            Self::StdinClosed => "stdin_closed",
            Self::ResizeFailed => "resize_failed",
            Self::WriteFailed => "write_failed",
            Self::ReadFailed => "read_failed",
            Self::ProcessFailed => "process_failed",
        }
    }

    pub fn class(self) -> WriteStdinErrorClass {
        match self {
            Self::SessionNotFound
            | Self::SessionExited
            | Self::SessionClosed
            | Self::StdinClosed
            | Self::ProcessFailed => WriteStdinErrorClass::Terminal,
            Self::SessionStarting | Self::ResizeFailed | Self::WriteFailed | Self::ReadFailed => {
                WriteStdinErrorClass::Transient
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteStdinFailure {
    pub code: WriteStdinErrorCode,
    pub detail: RuntimeString,
    pub found: bool,
    pub exited: bool,
    pub exit_code: Option<i32>,
    pub session_id: Option<RuntimeString>,
    pub terminal: bool,
    pub terminal_size: Option<TerminalOptions>,
    pub resized: bool,
    pub output: RuntimeString,
    pub tail: Option<RuntimeString>,
    pub retry_allowed: bool,
    pub retries_remaining: u8,
    pub session_blocked: bool,
    pub attempt_prevented: bool,
}

#[derive(Debug, Clone)]
struct WriteStdinFailureState {
    transient_failures: u8,
    blocked: bool,
    failure: WriteStdinFailure,
}

impl Default for ShellSessionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ShellSessionManager {
    pub fn new() -> Self {
        Self {
            registry: LocalExecProcessRegistry::new(),
            cursors: Mutex::new(HashMap::new()),
            failure_states: Mutex::new(HashMap::new()),
            session_gates: Mutex::new(HashMap::new()),
            next_write_id: AtomicU64::new(1),
        }
    }

    pub async fn exec(
        &self,
        process_context: RuntimeProcessContext,
        params: ExecCommandParams,
    ) -> ExecCommandResult {
        let terminal = params.terminal.map(process_terminal_size);
        let process_id = match self
            .registry
            .start(
                process_context,
                LocalProcessStartParams {
                    process_id: None,
                    argv: build_runtime_shell_invocation(&params.command),
                    cwd: params.cwd,
                    sandbox: params.sandbox,
                    env: params.env,
                    network_allowed: params.network_allowed,
                    managed_network: None,
                    writable_roots: params.writable_roots,
                    tty: terminal.is_some(),
                    pipe_stdin: true,
                    cols: terminal.map(|size| size.cols),
                    rows: terminal.map(|size| size.rows),
                    yield_ms: params.yield_ms,
                    max_output_bytes: params.max_output_bytes,
                    signal: params.signal.clone(),
                },
            )
            .await
        {
            Ok(started) => started.process_id,
            Err(error) => {
                return ExecCommandResult {
                    output: error,
                    exited: true,
                    exit_code: Some(1),
                    session_id: None,
                };
            }
        };
        self.clear_failure_state(&process_id);

        match self
            .registry
            .read(LocalProcessReadParams {
                process_id: process_id.clone(),
                after_seq: Some(0),
                max_bytes: params.max_output_bytes,
                wait_ms: Some(0),
                signal: params.signal,
            })
            .await
        {
            Ok(read) => {
                let output = chunks_to_string(&read.chunks);
                if read.exited || read.closed {
                    self.remove_cursor(&process_id);
                    ExecCommandResult {
                        output,
                        exited: true,
                        exit_code: read.exit_code,
                        session_id: None,
                    }
                } else {
                    self.set_cursor(&process_id, output_cursor(&read.chunks));
                    ExecCommandResult {
                        output,
                        exited: false,
                        exit_code: None,
                        session_id: Some(process_id),
                    }
                }
            }
            Err(error) => ExecCommandResult {
                output: error,
                exited: true,
                exit_code: Some(1),
                session_id: None,
            },
        }
    }

    /// Compatibility API retained for embedders that consume the historical
    /// `WriteStdinResult` shape. The model-facing tool uses
    /// [`Self::write_stdin_classified`] so it can expose typed recovery metadata.
    pub async fn write_stdin(&self, params: WriteStdinParams) -> WriteStdinResult {
        match self.write_stdin_classified(params).await {
            Ok(result) => result,
            Err(failure) => legacy_result_from_failure(failure),
        }
    }

    pub async fn write_stdin_classified(
        &self,
        params: WriteStdinParams,
    ) -> Result<WriteStdinResult, WriteStdinFailure> {
        let session_id = params.session_id.clone();
        let gate = self.session_gate(&session_id);
        let _guard = gate.lock().await;
        self.prune_failure_states();
        if let Some(failure) = self.blocked_failure(&session_id) {
            return Err(failure);
        }
        let result = self.write_stdin_unchecked(params).await;
        match result {
            Ok(result) => {
                if result.exited {
                    self.clear_failure_state(&session_id);
                }
                Ok(result)
            }
            Err(failure) => Err(self.record_failure(&session_id, failure)),
        }
    }

    async fn write_stdin_unchecked(
        &self,
        params: WriteStdinParams,
    ) -> Result<WriteStdinResult, WriteStdinFailure> {
        let runtime_session_id = params.session_id.clone();
        let input = params.input.clone().unwrap_or_default();
        let wrote_input = !input.is_empty();
        let Some(initial_info) = self.registry.info(&runtime_session_id) else {
            return Err(write_stdin_failure(
                WriteStdinErrorCode::SessionNotFound,
                RuntimeString::from("the session does not exist or has been evicted"),
                &runtime_session_id,
                None,
                false,
                false,
            ));
        };
        if initial_info.exited && (wrote_input || !self.has_cursor(&runtime_session_id)) {
            self.remove_cursor(&runtime_session_id);
            return Err(write_stdin_failure(
                WriteStdinErrorCode::SessionExited,
                RuntimeString::from("the process has already exited"),
                &runtime_session_id,
                Some(&initial_info),
                false,
                false,
            ));
        }
        if initial_info.closed && (wrote_input || !self.has_cursor(&runtime_session_id)) {
            self.remove_cursor(&runtime_session_id);
            return Err(write_stdin_failure(
                WriteStdinErrorCode::SessionClosed,
                RuntimeString::from("the session has already closed"),
                &runtime_session_id,
                Some(&initial_info),
                false,
                false,
            ));
        }

        let mut resized = false;
        if let Some(size) = params.resize.map(process_terminal_size) {
            match self.registry.resize(&runtime_session_id, size).await {
                Ok(result) => resized = result,
                Err(error) => {
                    let latest_info = self.registry.info(&runtime_session_id);
                    let code = classify_registry_error(
                        latest_info.as_ref(),
                        WriteStdinErrorCode::ResizeFailed,
                    );
                    return Err(write_stdin_failure(
                        code,
                        error,
                        &runtime_session_id,
                        latest_info.as_ref(),
                        latest_info.is_some(),
                        false,
                    ));
                }
            }
        }

        if wrote_input {
            let write_id = format!(
                "shell-write-{}",
                self.next_write_id.fetch_add(1, Ordering::Relaxed)
            );
            match self
                .registry
                .write(LocalProcessWriteParams {
                    process_id: runtime_session_id.clone(),
                    write_id: RuntimeString::from(write_id),
                    chunk: input.to_node_utf8(),
                    yield_ms: params.yield_ms,
                    signal: params.signal.clone(),
                })
                .await
            {
                Ok(response) if response.status == WriteStatus::Accepted => {}
                Ok(response) if response.status == WriteStatus::StdinClosed => {
                    let after_seq = self.cursor(&runtime_session_id);
                    self.remove_cursor(&runtime_session_id);
                    let latest_info = self.registry.info(&runtime_session_id);
                    let mut code = match latest_info.as_ref() {
                        Some(info) if info.exited => WriteStdinErrorCode::SessionExited,
                        Some(info) if info.closed => WriteStdinErrorCode::SessionClosed,
                        _ => WriteStdinErrorCode::StdinClosed,
                    };
                    let mut detail = match code {
                        WriteStdinErrorCode::SessionExited => {
                            RuntimeString::from("the process exited before input was written")
                        }
                        WriteStdinErrorCode::SessionClosed => {
                            RuntimeString::from("the session closed before input was written")
                        }
                        _ => {
                            RuntimeString::from("the process is no longer accepting standard input")
                        }
                    };
                    if let Ok(read) = self
                        .registry
                        .read(LocalProcessReadParams {
                            process_id: runtime_session_id.clone(),
                            after_seq: Some(after_seq),
                            max_bytes: None,
                            wait_ms: Some(0),
                            signal: None,
                        })
                        .await
                    {
                        if let Some(failure) = read.failure {
                            code = WriteStdinErrorCode::WriteFailed;
                            detail = failure;
                        }
                    }
                    return Err(write_stdin_failure(
                        code,
                        detail,
                        &runtime_session_id,
                        latest_info.as_ref(),
                        latest_info
                            .as_ref()
                            .is_some_and(|info| !info.exited && !info.closed),
                        resized,
                    ));
                }
                Ok(response) if response.status == WriteStatus::UnknownProcess => {
                    self.remove_cursor(&runtime_session_id);
                    return Err(write_stdin_failure(
                        WriteStdinErrorCode::SessionNotFound,
                        RuntimeString::from("the process disappeared before input was written"),
                        &runtime_session_id,
                        None,
                        false,
                        resized,
                    ));
                }
                Ok(response) if response.status == WriteStatus::Starting => {
                    return Err(write_stdin_failure(
                        WriteStdinErrorCode::SessionStarting,
                        RuntimeString::from(
                            "the process is still starting and cannot accept input yet",
                        ),
                        &runtime_session_id,
                        self.registry.info(&runtime_session_id).as_ref(),
                        true,
                        resized,
                    ));
                }
                Ok(_) => {
                    return Err(write_stdin_failure(
                        WriteStdinErrorCode::WriteFailed,
                        RuntimeString::from("the process rejected the input"),
                        &runtime_session_id,
                        self.registry.info(&runtime_session_id).as_ref(),
                        true,
                        resized,
                    ));
                }
                Err(error) => {
                    let latest_info = self.registry.info(&runtime_session_id);
                    let code = classify_registry_error(
                        latest_info.as_ref(),
                        WriteStdinErrorCode::WriteFailed,
                    );
                    return Err(write_stdin_failure(
                        code,
                        error,
                        &runtime_session_id,
                        latest_info.as_ref(),
                        latest_info.is_some(),
                        resized,
                    ));
                }
            }
        }

        let after_seq = self.cursor(&runtime_session_id);
        let wait_ms = if wrote_input {
            0
        } else {
            normalized_wait_ms(params.yield_ms, 5_000)
        };
        let read = match self
            .registry
            .read(LocalProcessReadParams {
                process_id: runtime_session_id.clone(),
                after_seq: Some(after_seq),
                max_bytes: None,
                wait_ms: Some(wait_ms),
                signal: params.signal,
            })
            .await
        {
            Ok(read) => read,
            Err(error) => {
                let latest_info = self.registry.info(&runtime_session_id);
                let code =
                    classify_registry_error(latest_info.as_ref(), WriteStdinErrorCode::ReadFailed);
                return Err(write_stdin_failure(
                    code,
                    error,
                    &runtime_session_id,
                    latest_info.as_ref(),
                    latest_info.is_some(),
                    resized,
                ));
            }
        };

        if let Some(failure) = read.failure.clone() {
            self.remove_cursor(&runtime_session_id);
            let latest_info = self.registry.info(&runtime_session_id);
            let mut classified = write_stdin_failure(
                WriteStdinErrorCode::ProcessFailed,
                failure,
                &runtime_session_id,
                latest_info.as_ref(),
                false,
                resized,
            );
            classified.output = chunks_to_string(&read.chunks);
            classified.tail = params
                .tail_bytes
                .filter(|bytes| *bytes > 0)
                .and_then(|bytes| self.registry.tail(&runtime_session_id, bytes));
            return Err(classified);
        }

        let output = chunks_to_string(&read.chunks);
        let tail = params
            .tail_bytes
            .filter(|bytes| *bytes > 0)
            .and_then(|bytes| self.registry.tail(&runtime_session_id, bytes));
        let latest_info = self.registry.info(&runtime_session_id);
        let (terminal, terminal_size) = latest_info
            .as_ref()
            .map(|info| {
                (
                    info.terminal,
                    info.terminal_size.map(interactive_terminal_options),
                )
            })
            .unwrap_or((false, None));

        if read.exited || read.closed {
            self.remove_cursor(&runtime_session_id);
            Ok(WriteStdinResult {
                found: true,
                output,
                exited: true,
                exit_code: read.exit_code,
                session_id: None,
                terminal,
                terminal_size,
                resized,
                tail,
            })
        } else {
            self.set_cursor(
                &runtime_session_id,
                output_cursor(&read.chunks).max(after_seq),
            );
            Ok(WriteStdinResult {
                found: true,
                output,
                exited: false,
                exit_code: None,
                session_id: Some(params.session_id),
                terminal,
                terminal_size,
                resized,
                tail,
            })
        }
    }

    pub fn kill(&self, id: &RuntimeString) -> bool {
        self.kill_with_signal(id, Signal::Kill)
    }

    pub fn kill_with_signal(&self, id: &RuntimeString, signal: Signal) -> bool {
        self.remove_cursor(id);
        self.clear_failure_state(id);
        self.registry.kill_with_signal(id, signal)
    }

    pub fn has(&self, id: &RuntimeString) -> bool {
        self.registry
            .info(id)
            .is_some_and(|info| !info.exited && !info.closed)
    }

    /// Immutable launch evidence for approval of input to an existing terminal.
    pub fn approval_context(
        &self,
        id: &RuntimeString,
    ) -> Option<crate::types::permission::ApprovalTerminalMetadata> {
        self.registry
            .info(id)
            .map(|info| crate::types::permission::ApprovalTerminalMetadata {
                process_id: info.process_id,
                cwd: info.cwd.into_runtime_string(),
                tty: info.terminal,
            })
    }

    pub fn list(&self) -> Vec<SessionListEntry> {
        self.registry
            .list()
            .into_iter()
            .filter(|info| !info.exited && !info.closed)
            .map(|info| SessionListEntry {
                id: info.process_id,
                command: info.argv.last().cloned().unwrap_or_default(),
                exited: info.exited,
                exit_code: info.exit_code,
                terminal: info.terminal,
                terminal_size: info.terminal_size.map(interactive_terminal_options),
            })
            .collect()
    }

    pub fn subscribe_events(
        &self,
        id: &RuntimeString,
    ) -> Result<ProcessEventReceiver, RuntimeString> {
        self.registry.subscribe_events(id)
    }

    pub fn dispose_all(&self) {
        self.registry.dispose_all();
        self.cursors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        self.failure_states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        self.session_gates
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }

    fn cursor(&self, id: &RuntimeString) -> u64 {
        *self
            .cursors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(id)
            .unwrap_or(&0)
    }

    fn has_cursor(&self, id: &RuntimeString) -> bool {
        self.cursors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(id)
    }

    fn set_cursor(&self, id: &RuntimeString, cursor: u64) {
        self.cursors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id.clone(), cursor);
    }

    fn remove_cursor(&self, id: &RuntimeString) {
        self.cursors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(id);
    }

    fn blocked_failure(&self, id: &RuntimeString) -> Option<WriteStdinFailure> {
        let states = self
            .failure_states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let state = states.get(id)?;
        if !state.blocked {
            return None;
        }
        let mut failure = state.failure.clone();
        failure.retry_allowed = false;
        failure.retries_remaining = 0;
        failure.session_blocked = true;
        failure.attempt_prevented = true;
        Some(failure)
    }

    fn record_failure(
        &self,
        session_id: &RuntimeString,
        mut failure: WriteStdinFailure,
    ) -> WriteStdinFailure {
        if failure.tail.is_none() {
            failure.tail = self.registry.tail(session_id, FAILURE_TAIL_BYTES);
        }
        if failure.output.is_empty() {
            failure.output = failure.tail.clone().unwrap_or_default();
        }
        let live_session = self
            .registry
            .info(session_id)
            .is_some_and(|info| !info.exited && !info.closed);
        if failure.code.class() == WriteStdinErrorClass::Terminal && !live_session {
            failure.retry_allowed = false;
            failure.retries_remaining = 0;
            failure.session_blocked = true;
            failure.attempt_prevented = true;
            return failure;
        }
        let mut states = self
            .failure_states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let previous_transient_failures = states
            .get(session_id)
            .map_or(0, |state| state.transient_failures);
        let terminal = failure.code.class() == WriteStdinErrorClass::Terminal;
        let exhausted = terminal || previous_transient_failures >= 1;
        failure.retry_allowed = !exhausted;
        failure.retries_remaining = u8::from(!exhausted);
        failure.session_blocked = exhausted;
        failure.attempt_prevented = false;
        states.insert(
            session_id.clone(),
            WriteStdinFailureState {
                transient_failures: previous_transient_failures.saturating_add(u8::from(!terminal)),
                blocked: exhausted,
                failure: failure.clone(),
            },
        );
        failure
    }

    fn clear_failure_state(&self, id: &RuntimeString) {
        self.failure_states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(id);
    }

    fn session_gate(&self, id: &RuntimeString) -> Arc<AsyncMutex<()>> {
        let mut gates = self
            .session_gates
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        gates.retain(|_, gate| gate.strong_count() > 0);
        if let Some(gate) = gates.get(id).and_then(Weak::upgrade) {
            return gate;
        }
        let gate = Arc::new(AsyncMutex::new(()));
        gates.insert(id.clone(), Arc::downgrade(&gate));
        gate
    }

    fn prune_failure_states(&self) {
        let ids = self
            .failure_states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let stale = ids
            .into_iter()
            .filter(|id| {
                self.registry
                    .info(id)
                    .is_none_or(|info| info.exited || info.closed)
            })
            .collect::<Vec<_>>();
        if stale.is_empty() {
            return;
        }
        let mut states = self
            .failure_states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for id in stale {
            states.remove(&id);
        }
    }
}

fn process_terminal_size(size: TerminalOptions) -> ProcessTerminalSize {
    ProcessTerminalSize {
        cols: size.cols,
        rows: size.rows,
    }
}

fn interactive_terminal_options(size: ProcessTerminalSize) -> TerminalOptions {
    TerminalOptions {
        cols: size.cols,
        rows: size.rows,
    }
}

fn output_cursor(chunks: &[ProcessOutputChunk]) -> u64 {
    chunks.last().map(|chunk| chunk.seq).unwrap_or(0)
}

fn chunks_to_string(chunks: &[ProcessOutputChunk]) -> RuntimeString {
    let mut bytes = Vec::new();
    for chunk in chunks {
        match chunk.stream {
            ExecOutputStream::Stdout | ExecOutputStream::Stderr | ExecOutputStream::Pty => {
                bytes.extend_from_slice(&chunk.chunk);
            }
        }
    }
    NodeChunkUtf8::decode_chunk(&bytes)
}

fn normalized_wait_ms(value: Option<i64>, default: i64) -> u64 {
    value.unwrap_or(default).clamp(0, 60_000) as u64
}

fn classify_registry_error(
    info: Option<&LocalProcessInfo>,
    live_code: WriteStdinErrorCode,
) -> WriteStdinErrorCode {
    match info {
        Some(info) if info.exited => WriteStdinErrorCode::SessionExited,
        Some(info) if info.closed => WriteStdinErrorCode::SessionClosed,
        Some(_) => live_code,
        None => WriteStdinErrorCode::SessionNotFound,
    }
}

fn write_stdin_failure(
    code: WriteStdinErrorCode,
    detail: RuntimeString,
    requested_session_id: &RuntimeString,
    info: Option<&LocalProcessInfo>,
    keep_session: bool,
    resized: bool,
) -> WriteStdinFailure {
    let exited = info.is_none_or(|info| info.exited || info.closed);
    WriteStdinFailure {
        code,
        detail,
        found: info.is_some(),
        exited,
        exit_code: info.and_then(|info| info.exit_code),
        session_id: (keep_session && !exited).then(|| requested_session_id.clone()),
        terminal: info.is_some_and(|info| info.terminal),
        terminal_size: info
            .and_then(|info| info.terminal_size)
            .map(interactive_terminal_options),
        resized,
        output: RuntimeString::default(),
        tail: None,
        retry_allowed: false,
        retries_remaining: 0,
        session_blocked: false,
        attempt_prevented: false,
    }
}

fn legacy_result_from_failure(failure: WriteStdinFailure) -> WriteStdinResult {
    WriteStdinResult {
        found: failure.found,
        output: failure.output,
        exited: failure.exited,
        exit_code: failure.exit_code,
        session_id: failure.session_id,
        terminal: failure.terminal,
        terminal_size: failure.terminal_size,
        resized: failure.resized,
        tail: failure.tail,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command_runtime::process::ProcessEvent;
    use crate::runtime_process::ParentEnvSnapshot;
    use crate::runtime_values::RuntimePath;
    use crate::types::sandbox::{
        ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec,
    };
    use std::sync::Arc;
    use std::time::Duration;

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

    fn cwd() -> RuntimePath {
        RuntimePath::from(
            std::env::current_dir()
                .unwrap()
                .to_string_lossy()
                .into_owned(),
        )
    }

    #[tokio::test]
    async fn shell_session_adapter_preserves_exec_and_write_api() {
        let registry = ShellSessionManager::new();
        let result = registry
            .exec(
                crate::runtime_process::test_runtime_process_context(),
                ExecCommandParams {
                    command: RuntimeString::from("cat"),
                    cwd: cwd(),
                    sandbox: Arc::new(IdSandbox),
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

        assert!(!result.exited);
        let session_id = result.session_id.clone().unwrap();
        let echoed = registry
            .write_stdin(WriteStdinParams {
                session_id: session_id.clone(),
                input: Some(RuntimeString::from("adapter-output\n")),
                yield_ms: Some(4000),
                signal: None,
                resize: None,
                tail_bytes: None,
            })
            .await;

        assert!(echoed.found);
        assert!(echoed.output.contains("adapter-output"));
        let _ = registry
            .write_stdin_classified(WriteStdinParams {
                session_id: session_id.clone(),
                input: Some(RuntimeString::from("\u{4}")),
                yield_ms: Some(1000),
                signal: None,
                resize: None,
                tail_bytes: None,
            })
            .await;
        registry.dispose_all();
    }

    #[tokio::test]
    async fn shell_session_adapter_replays_process_events() {
        let registry = ShellSessionManager::new();
        let result = registry
            .exec(
                crate::runtime_process::test_runtime_process_context(),
                ExecCommandParams {
                    command: RuntimeString::from("cat"),
                    cwd: cwd(),
                    sandbox: Arc::new(IdSandbox),
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

        let session_id = result.session_id.clone().unwrap();
        let echoed = registry
            .write_stdin_classified(WriteStdinParams {
                session_id: session_id.clone(),
                input: Some(RuntimeString::from("event-output\n")),
                yield_ms: Some(4000),
                signal: None,
                resize: None,
                tail_bytes: None,
            })
            .await
            .expect("live session must accept input");
        assert!(echoed.output.contains("event-output"));

        let mut events = registry.subscribe_events(&session_id).unwrap();
        let mut saw_echo = false;
        while let Ok(Ok(event)) =
            tokio::time::timeout(Duration::from_millis(50), events.recv()).await
        {
            if let ProcessEvent::Output(chunk) = event {
                let text = String::from_utf8_lossy(&chunk.chunk);
                if text.contains("event-output") {
                    saw_echo = true;
                    break;
                }
            }
        }

        assert!(saw_echo);
        let _ = registry
            .write_stdin(WriteStdinParams {
                session_id: session_id.clone(),
                input: Some(RuntimeString::from("\u{4}")),
                yield_ms: Some(1000),
                signal: None,
                resize: None,
                tail_bytes: None,
            })
            .await;
        registry.dispose_all();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn exited_session_delivers_final_output_once_then_reports_its_terminal_state() {
        let registry = ShellSessionManager::new();
        let result = registry
            .exec(
                crate::runtime_process::test_runtime_process_context(),
                ExecCommandParams {
                    command: RuntimeString::from("sleep 0.05; printf final-output"),
                    cwd: cwd(),
                    sandbox: Arc::new(IdSandbox),
                    env: None,
                    network_allowed: None,
                    managed_network: None,
                    writable_roots: None,
                    unconfined_writes: false,
                    yield_ms: Some(1),
                    max_output_bytes: Some(1024),
                    signal: None,
                    terminal: None,
                },
            )
            .await;
        let session_id = result
            .session_id
            .expect("command must remain live initially");
        let mut output = RuntimeString::default();
        loop {
            let poll = registry
                .write_stdin_classified(WriteStdinParams {
                    session_id: session_id.clone(),
                    input: None,
                    yield_ms: Some(1000),
                    signal: None,
                    resize: None,
                    tail_bytes: None,
                })
                .await
                .expect("polls before terminal delivery must succeed");
            output = output.concat(&poll.output);
            if poll.exited {
                break;
            }
        }
        assert!(output.contains("final-output"));

        let repeated_poll = registry
            .write_stdin_classified(WriteStdinParams {
                session_id,
                input: None,
                yield_ms: Some(1000),
                signal: None,
                resize: None,
                tail_bytes: None,
            })
            .await
            .expect_err("a delivered terminal session must not be polled forever");
        assert_eq!(repeated_poll.code, WriteStdinErrorCode::SessionExited);
        registry.dispose_all();
    }

    #[tokio::test]
    async fn transient_failure_allows_one_recovery_then_blocks_the_session() {
        let registry = ShellSessionManager::new();
        let session = registry
            .exec(
                crate::runtime_process::test_runtime_process_context(),
                ExecCommandParams {
                    command: RuntimeString::from("cat"),
                    cwd: cwd(),
                    sandbox: Arc::new(IdSandbox),
                    env: None,
                    network_allowed: None,
                    managed_network: None,
                    writable_roots: None,
                    unconfined_writes: false,
                    yield_ms: Some(100),
                    max_output_bytes: Some(1024),
                    signal: None,
                    terminal: None,
                },
            )
            .await;
        let session_id = session.session_id.expect("cat must remain live");
        let info = registry
            .registry
            .info(&session_id)
            .expect("live session info");
        let first = registry.record_failure(
            &session_id,
            write_stdin_failure(
                WriteStdinErrorCode::ReadFailed,
                RuntimeString::from("temporary read failure"),
                &session_id,
                Some(&info),
                true,
                false,
            ),
        );
        assert!(first.retry_allowed);
        assert_eq!(first.retries_remaining, 1);
        assert!(!first.session_blocked);

        let second = registry.record_failure(
            &session_id,
            write_stdin_failure(
                WriteStdinErrorCode::ReadFailed,
                RuntimeString::from("temporary read failure again"),
                &session_id,
                Some(&info),
                true,
                false,
            ),
        );
        assert!(!second.retry_allowed);
        assert_eq!(second.retries_remaining, 0);
        assert!(second.session_blocked);

        let prevented = registry
            .blocked_failure(&session_id)
            .expect("exhausted transient recovery must block the session");
        assert!(prevented.attempt_prevented);
        assert!(prevented.session_blocked);
        registry.dispose_all();
    }

    #[tokio::test]
    async fn missing_sessions_do_not_allocate_failure_state() {
        let registry = ShellSessionManager::new();
        for index in 0..256 {
            let failure = registry
                .write_stdin_classified(WriteStdinParams {
                    session_id: RuntimeString::from(format!("missing-{index}")),
                    input: Some(RuntimeString::from("yes\n")),
                    yield_ms: None,
                    signal: None,
                    resize: None,
                    tail_bytes: None,
                })
                .await
                .expect_err("missing sessions must fail before process I/O");
            assert_eq!(failure.code, WriteStdinErrorCode::SessionNotFound);
            assert!(failure.session_blocked);
            assert!(failure.attempt_prevented);
        }
        assert!(registry
            .failure_states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty());
    }

    #[tokio::test]
    async fn successful_recovery_does_not_refund_transient_budget() {
        let registry = ShellSessionManager::new();
        let session = registry
            .exec(
                crate::runtime_process::test_runtime_process_context(),
                ExecCommandParams {
                    command: RuntimeString::from("cat"),
                    cwd: cwd(),
                    sandbox: Arc::new(IdSandbox),
                    env: None,
                    network_allowed: None,
                    managed_network: None,
                    writable_roots: None,
                    unconfined_writes: false,
                    yield_ms: Some(100),
                    max_output_bytes: Some(1024),
                    signal: None,
                    terminal: None,
                },
            )
            .await;
        let session_id = session.session_id.expect("cat must remain live");
        let info = registry
            .registry
            .info(&session_id)
            .expect("live session info");
        let first = registry.record_failure(
            &session_id,
            write_stdin_failure(
                WriteStdinErrorCode::ReadFailed,
                RuntimeString::from("temporary read failure"),
                &session_id,
                Some(&info),
                true,
                false,
            ),
        );
        assert!(first.retry_allowed);

        let recovered = registry
            .write_stdin_classified(WriteStdinParams {
                session_id: session_id.clone(),
                input: Some(RuntimeString::from("recovered\n")),
                yield_ms: Some(1000),
                signal: None,
                resize: None,
                tail_bytes: None,
            })
            .await
            .expect("the one recovery call must be allowed");
        assert!(recovered.output.contains("recovered"));
        assert!(registry.blocked_failure(&session_id).is_none());

        let info = registry
            .registry
            .info(&session_id)
            .expect("live session info");
        let second = registry.record_failure(
            &session_id,
            write_stdin_failure(
                WriteStdinErrorCode::WriteFailed,
                RuntimeString::from("write failed after recovery"),
                &session_id,
                Some(&info),
                true,
                false,
            ),
        );
        assert!(!second.retry_allowed);
        assert_eq!(second.retries_remaining, 0);
        assert!(second.session_blocked);
        let prevented = registry
            .write_stdin_classified(WriteStdinParams {
                session_id: session_id.clone(),
                input: Some(RuntimeString::from("must-not-reach-cat\n")),
                yield_ms: Some(1000),
                signal: None,
                resize: None,
                tail_bytes: None,
            })
            .await
            .expect_err("the engine must block calls after recovery is exhausted");
        assert!(prevented.attempt_prevented);
        registry.dispose_all();
    }
}

#[cfg(test)]
mod exact_tests {
    use super::*;
    use crate::runtime_process::ParentEnvSnapshot;
    use crate::runtime_values::RuntimePath;
    use crate::types::sandbox::{
        ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec,
    };
    use std::sync::Arc;

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

    #[tokio::test]
    async fn shell_adapter_explicitly_builds_shell_argv() {
        let manager = ShellSessionManager::new();
        let result = manager
            .exec(
                crate::runtime_process::test_runtime_process_context(),
                ExecCommandParams {
                    command: RuntimeString::from("printf shell-adapter"),
                    cwd: RuntimePath::from(
                        std::env::current_dir()
                            .expect("cwd")
                            .to_string_lossy()
                            .into_owned(),
                    ),
                    sandbox: Arc::new(TestSandbox),
                    env: None,
                    network_allowed: None,
                    managed_network: None,
                    writable_roots: None,
                    unconfined_writes: false,
                    yield_ms: Some(2_000),
                    max_output_bytes: Some(1024),
                    signal: None,
                    terminal: None,
                },
            )
            .await;
        assert!(result.exited);
        assert!(result.output.contains("shell-adapter"));
    }
}
