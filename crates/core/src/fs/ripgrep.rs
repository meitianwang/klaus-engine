//! Ripgrep invocation layer for the Glob and Grep tools. The Glob/Grep tools ARE
//! ripgrep (a vendored binary); to keep the search logic and functionality exact —
//! the Rust-regex dialect, gitignore semantics, `--type` definitions, `--sort`,
//! `--hidden` — we invoke ripgrep itself rather than reimplement a search
//! engine. This layer implements the ripgrep runner
//! contract (binary resolution, exit-code handling, EAGAIN single-thread retry,
//! timeout-vs-no-match distinction). The messages are worded freely;
//! the rg flags are facts.

use crate::fs::bundled_ripgrep::bundled_ripgrep_command;
use crate::fs::node_path::{
    is_absolute_runtime, project_runtime_path, relative_posix_runtime, resolve_runtime,
    runtime_path_from_native,
};
use crate::runtime_process::{RuntimeEnv, RuntimeProcessContext, RuntimeSignal};
use crate::runtime_values::{NodeBufferUtf8, RuntimePath, RuntimeString};
use crate::types::abort::AbortSignal;
use crate::types::errors::ToolError;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use unicode_normalization::UnicodeNormalization;

/// 20MB — large monorepos emit 200k+ paths.
const MAX_BUFFER: usize = 20_000_000;
/// Default search timeout; bumped to 60s on WSL2 (file reads 3-5x slower there).
const DEFAULT_TIMEOUT_MS: u64 = 20_000;
const WSL_TIMEOUT_MS: u64 = 60_000;

/// Detect WSL — a Linux kernel reporting "microsoft"/"wsl" in
/// /proc/version. Cached — the kernel doesn't change mid-process.
static IS_WSL: LazyLock<bool> = LazyLock::new(|| {
    if cfg!(target_os = "linux") {
        match std::fs::read_to_string("/proc/version") {
            Ok(v) => {
                let v = v.to_lowercase();
                v.contains("microsoft") || v.contains("wsl")
            }
            Err(_) => false,
        }
    } else {
        false
    }
});

fn is_wsl() -> bool {
    *IS_WSL
}

/// Base-10 integer prefix parse (leading whitespace and an optional sign
/// allowed, digits taken until the first non-digit); anything unparsable is 0.
fn parse_int_prefix(s: &str) -> i64 {
    let t = s.trim_start_matches(is_ecmascript_whitespace);
    let (neg, rest) = match t.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, t.strip_prefix('+').unwrap_or(t)),
    };
    let end = rest
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(rest.len());
    let digits = &rest[..end];
    if digits.is_empty() {
        return 0;
    }
    // Absurdly long digit strings clamp high; they only need to test as > 0.
    let v = digits.parse::<i64>().unwrap_or(i64::MAX);
    if neg {
        -v
    } else {
        v
    }
}

/// The per-call ripgrep timeout: an explicit
/// KLAUS_GLOB_TIMEOUT_SECONDS override wins; otherwise 60s on WSL, 20s elsewhere.
fn effective_timeout_ms() -> u64 {
    let parsed = std::env::var("KLAUS_GLOB_TIMEOUT_SECONDS")
        .ok()
        .map(|v| parse_int_prefix(&v))
        .unwrap_or(0);
    if parsed > 0 {
        return (parsed as u64).saturating_mul(1000);
    }
    if is_wsl() {
        WSL_TIMEOUT_MS
    } else {
        DEFAULT_TIMEOUT_MS
    }
}

/// A failed ripgrep run, classified so the caller can react correctly.
#[derive(Debug, thiserror::Error)]
pub enum RipgrepError {
    /// Distinguishes "search timed out" (or was aborted) from "no matches", so
    /// the caller doesn't report a clean zero.
    #[error("{message}")]
    Timeout {
        message: String,
        /// Complete lines received before the kill (the torn last line dropped).
        partial_results: Vec<RuntimeString>,
    },
    /// The ripgrep binary cannot be found, so the caller can advise the host to
    /// install it.
    #[error("{0}")]
    Unavailable(String),
    /// The binary exists but could not be launched (EACCES/EPERM) — a broken
    /// rg, not zero matches. Surfaced, not swallowed.
    #[error(transparent)]
    Spawn(#[from] std::io::Error),
}

#[derive(Clone)]
enum RipgrepCommandMode {
    Override(OsString),
    Bundled,
}

static RESOLVED_COMMAND: Mutex<Option<RipgrepCommandMode>> = Mutex::new(None);

/// Resolve the ripgrep binary, in precedence order:
/// explicit env override → the verified `@vscode/ripgrep@1.18.0` target asset.
/// The selected mode is cached so a later environment mutation cannot change
/// binary identity. Bundled verification/materialization errors are not
/// cached: every call revalidates the pinned executable and may repair a
/// transiently missing or corrupt cache entry.
pub fn resolve_ripgrep_command() -> Result<OsString, String> {
    let mode = {
        let mut cached = RESOLVED_COMMAND
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cached
            .get_or_insert_with(|| {
                std::env::var("KLAUS_RIPGREP_PATH")
                    .ok()
                    .map(|value| value.trim_matches(is_ecmascript_whitespace).to_string())
                    .filter(|value| !value.is_empty())
                    .map_or(RipgrepCommandMode::Bundled, |path| {
                        RipgrepCommandMode::Override(OsString::from(path))
                    })
            })
            .clone()
    };

    match mode {
        RipgrepCommandMode::Override(path) => Ok(path),
        RipgrepCommandMode::Bundled => bundled_ripgrep_command()
            .and_then(|path| {
                path.ok_or_else(|| {
                    format!(
                        "@vscode/ripgrep@1.18.0 has no bundled executable for {}/{}; set KLAUS_RIPGREP_PATH explicitly",
                        std::env::consts::OS,
                        std::env::consts::ARCH,
                    )
                })
            })
            .map(|path| path.as_os_str().to_owned())
            .map_err(|error| format!("failed to prepare bundled ripgrep: {error}")),
    }
}

/// Test seam: reset the memoized binary path (used by env-override tests).
pub fn reset_ripgrep_command_for_test() {
    *RESOLVED_COMMAND
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
}

static RESOLVED_TOOL_EXECUTABLE: Mutex<Option<Option<PathBuf>>> = Mutex::new(None);

/// Memoized executable path behind [`resolve_ripgrep_command`], for callers that
/// need the binary's directory on every request (shell PATH supplementation).
/// `resolve_ripgrep_command` revalidates the cached binary per call, which is
/// right for search runs but too costly per shell spawn; this memo keeps the
/// first successful resolution and re-resolves only when the pinned file
/// disappears.
fn resolved_ripgrep_executable() -> Option<PathBuf> {
    let mut guard = RESOLVED_TOOL_EXECUTABLE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    match guard.clone() {
        Some(Some(path)) if path.is_file() => return Some(path),
        Some(None) => return None,
        _ => {}
    }
    let resolved = resolve_ripgrep_command()
        .ok()
        .map(PathBuf::from)
        .filter(|path| path.is_file());
    *guard = Some(resolved.clone());
    resolved
}

/// Test seam: drop the memoized executable path so override changes apply.
pub fn reset_ripgrep_tool_executable_for_test() {
    *RESOLVED_TOOL_EXECUTABLE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
}

fn ripgrep_path_candidate_names() -> &'static [&'static str] {
    if cfg!(windows) {
        &["rg.exe", "rg.cmd", "rg.bat"]
    } else {
        &["rg"]
    }
}

fn is_executable_file(path: &Path) -> bool {
    let Ok(metadata) = std::fs::metadata(path) else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

/// The PATH property a child shell reads: exact `PATH`, or on Windows any
/// case variant the parent environment carries.
fn path_environment_key(env: &RuntimeEnv) -> Option<RuntimeString> {
    let exact = RuntimeString::from("PATH");
    if env.contains_key(&exact) {
        return Some(exact);
    }
    if cfg!(windows) {
        return env
            .entries()
            .map(|(key, _)| key)
            .find(|key| String::from_utf8_lossy(&key.to_node_utf8()).eq_ignore_ascii_case("path"));
    }
    None
}

/// Keep the bundled ripgrep usable from shell commands: when `rg` does not
/// resolve on the environment's PATH, prepend the directory of the resolved
/// ripgrep executable so `rg ...` works on machines without a system install.
/// An environment that already resolves `rg` keeps its own binary — the
/// supplement fills the gap, it never shadows an existing install.
pub fn ensure_ripgrep_on_path(env: &mut RuntimeEnv) {
    let key = path_environment_key(env);
    let current = key.as_ref().and_then(|key| env.get(key)).cloned();
    let separator = if cfg!(windows) { ';' } else { ':' };
    if let Some(value) = current.as_ref() {
        let path_bytes = value.to_node_utf8();
        let path_value = String::from_utf8_lossy(&path_bytes);
        let already_resolvable = path_value.split(separator).any(|entry| {
            !entry.is_empty()
                && ripgrep_path_candidate_names()
                    .iter()
                    .any(|name| is_executable_file(&Path::new(entry).join(name)))
        });
        if already_resolvable {
            return;
        }
    }
    let Some(executable) = resolved_ripgrep_executable() else {
        return;
    };
    let Some(directory) = executable.parent() else {
        return;
    };
    let directory = directory.to_string_lossy();
    let supplemented = match current.as_ref() {
        Some(value) if !value.is_empty() => format!(
            "{directory}{separator}{}",
            String::from_utf8_lossy(&value.to_node_utf8())
        ),
        _ => directory.into_owned(),
    };
    let key = key.unwrap_or_else(|| RuntimeString::from("PATH"));
    env.insert(key, RuntimeString::from_utf8(&supplemented));
}

/// Stdout lines: trimmed, a single trailing CR stripped per line, blanks dropped.
fn parse_lines(stdout: &RuntimeString) -> Vec<RuntimeString> {
    let trimmed = trim_ecmascript(stdout);
    let units = trimmed.as_utf16();
    let mut lines = Vec::new();
    let mut start = 0;
    for index in 0..=units.len() {
        if index != units.len() && units[index] != u16::from(b'\n') {
            continue;
        }
        let mut end = index;
        if end > start && units[end - 1] == u16::from(b'\r') {
            end -= 1;
        }
        if start < end {
            lines.push(RuntimeString::from_utf16(units[start..end].to_vec()));
        }
        start = index.saturating_add(1);
    }
    lines
}

/// EAGAIN ("resource temporarily unavailable") — thread-spawn pressure under Docker/CI.
fn is_eagain(stderr: &RuntimeString) -> bool {
    stderr.contains("os error 11") || stderr.contains("Resource temporarily unavailable")
}

/// What one ripgrep invocation produced, before classification.
#[derive(Debug, Default, Clone)]
struct Attempt {
    /// The exit code, when the process exited on its own (None when signaled).
    exit_code: Option<i32>,
    /// Died from SIGTERM/SIGKILL — ours or anyone's.
    term_or_kill: bool,
    /// Our deadline fired and we killed it.
    timed_out: bool,
    /// The abort signal fired and we killed it.
    aborted: bool,
    /// A stream crossed the 20MB cap (truncated to exactly the cap) and we killed it.
    overflow: bool,
    stdout: RuntimeString,
    stderr: RuntimeString,
}

enum Outcome {
    /// The embedded binary could not be verified or materialized.
    ResolutionError(String),
    /// The process could not be spawned at all.
    SpawnError {
        error: std::io::Error,
        command: String,
    },
    Finished(Attempt),
}

#[derive(Debug)]
enum Handled {
    Done(Result<Vec<RuntimeString>, RipgrepError>),
    /// Transient thread-spawn pressure — run once more, single-threaded.
    Retry,
}

/// Append a chunk under the 20MB cap, truncating to exactly the cap when it
/// would cross. Returns true when the cap was crossed.
fn append_capped(buf: &mut Vec<u8>, chunk: &[u8]) -> bool {
    if buf.len() + chunk.len() > MAX_BUFFER {
        let take = MAX_BUFFER - buf.len();
        buf.extend_from_slice(&chunk[..take]);
        true
    } else {
        buf.extend_from_slice(chunk);
        false
    }
}

/// Run one ripgrep invocation: pipe stdout/stderr under the 20MB cap, enforce
/// the deadline and the abort signal, and kill with SIGKILL when either fires
/// (SIGTERM may not kill rg blocked in uninterruptible FS I/O; SIGKILL is
/// reliable), then keep draining both pipes to EOF so output the process had
/// already produced is preserved. No extra base args beyond the caller's are
/// passed, so user rg config is honored.
async fn spawn_rg(
    process_context: &RuntimeProcessContext,
    args: &[RuntimeString],
    targets: &[RuntimePath],
    signal: &AbortSignal,
    single_thread: bool,
    sandbox: Option<(&dyn crate::types::sandbox::SandboxBackend, &RuntimePath)>,
) -> Outcome {
    let command = match if sandbox.is_some() {
        bundled_ripgrep_command()
            .and_then(|value| value.ok_or_else(|| "bundled ripgrep is unavailable".to_owned()))
            .map(|path| path.as_os_str().to_os_string())
    } else {
        resolve_ripgrep_command()
    } {
        Ok(command) => command,
        Err(error) => return Outcome::ResolutionError(error),
    };
    let mut projected_targets = Vec::with_capacity(targets.len());
    for target in targets {
        match project_runtime_path(target, "path") {
            Ok(path) => projected_targets.push(path),
            Err(error) => {
                return Outcome::ResolutionError(
                    String::from_utf8(error.message().to_node_utf8())
                        .expect("Node filesystem path errors have valid UTF-8 projection"),
                )
            }
        }
    }
    let command_display = command.to_string_lossy().into_owned();
    let spawned = if let Some((sandbox, cwd)) = sandbox {
        let program = runtime_path_from_native(std::path::Path::new(&command));
        let Some(scoped) = sandbox.task_runtime_program(&program) else {
            return Outcome::ResolutionError("Task search sandbox is unavailable".into());
        };
        let mut argv = vec![
            program.into_runtime_string(),
            "--no-config".into(),
            "--no-follow".into(),
        ];
        if single_thread {
            argv.extend([RuntimeString::from("-j"), RuntimeString::from("1")]);
        }
        argv.extend(args.iter().cloned());
        argv.extend(targets.iter().map(|path| path.as_runtime_string().clone()));
        let request = crate::types::sandbox::ExecRequest {
            argv,
            cwd: cwd.clone(),
            env: None,
            timeout_ms: None,
        };
        process_context
            .spawn_scoped_ripgrep(
                scoped.transform(&request, &crate::types::sandbox::SandboxPolicy::default()),
            )
            .await
    } else {
        process_context
            .spawn_ripgrep(command, args.to_vec(), projected_targets, single_thread)
            .await
    };
    let mut child = match spawned {
        Ok(c) => c,
        Err(error) => {
            return Outcome::SpawnError {
                error: error.into_io_error(),
                command: command_display,
            }
        }
    };
    let controller = child.controller();
    let mut out_pipe = child.take_stdout().expect("stdout piped");
    let mut err_pipe = child.take_stderr().expect("stderr piped");

    let mut stdout_buf: Vec<u8> = Vec::new();
    let mut stderr_buf: Vec<u8> = Vec::new();
    let mut out_done = false;
    let mut err_done = false;
    let mut overflow = false;
    let mut timed_out = false;
    let mut aborted = false;
    let mut killed = false;

    let timeout = tokio::time::sleep(Duration::from_millis(effective_timeout_ms()));
    tokio::pin!(timeout);

    let mut out_chunk = vec![0u8; 65536];
    let mut err_chunk = vec![0u8; 65536];

    // Drain both pipes until EOF. On overflow, kill and stop reading — the
    // capped buffers already hold everything they may. On deadline/abort, kill
    // but keep draining: the deadline is polled ahead of the reads, so a
    // starved task can observe it only after the process already finished with
    // its complete output sitting in the pipes; discarding that output would
    // misreport a finished search as zero results. SIGKILL guarantees prompt
    // EOF and the 20MB cap bounds the drain. The deadline and the abort signal
    // are checked ahead of the reads so a pre-aborted call is deterministically
    // classified as aborted.
    while !(out_done && err_done) {
        tokio::select! {
            biased;
            _ = &mut timeout, if !killed => {
                timed_out = true;
                killed = true;
                controller.signal(RuntimeSignal::Kill);
            }
            _ = signal.cancelled(), if !killed => {
                aborted = true;
                killed = true;
                controller.signal(RuntimeSignal::Kill);
            }
            n = out_pipe.read(&mut out_chunk[..]), if !out_done => {
                match n {
                    Ok(0) | Err(_) => out_done = true,
                    Ok(n) => {
                        if append_capped(&mut stdout_buf, &out_chunk[..n]) {
                            overflow = true;
                            killed = true;
                            out_done = true;
                            err_done = true;
                            controller.signal(RuntimeSignal::Kill);
                        }
                    }
                }
            }
            n = err_pipe.read(&mut err_chunk[..]), if !err_done => {
                match n {
                    Ok(0) | Err(_) => err_done = true,
                    Ok(n) => {
                        if append_capped(&mut stderr_buf, &err_chunk[..n]) {
                            overflow = true;
                            killed = true;
                            out_done = true;
                            err_done = true;
                            controller.signal(RuntimeSignal::Kill);
                        }
                    }
                }
            }
        }
    }

    // Reap. The deadline and the abort signal stay live while waiting, in case
    // the process lingers after closing its pipes.
    let status = loop {
        if killed {
            break child.wait().await;
        }
        tokio::select! {
            biased;
            s = child.wait() => break s,
            _ = &mut timeout => {
                timed_out = true;
                killed = true;
                controller.signal(RuntimeSignal::Kill);
            }
            _ = signal.cancelled() => {
                aborted = true;
                killed = true;
                controller.signal(RuntimeSignal::Kill);
            }
        }
    };

    let (exit_code, term_or_kill) = match status {
        Ok(st) => {
            #[cfg(unix)]
            let term_or_kill =
                matches!(st.signal(), Some(sig) if sig == libc::SIGTERM || sig == libc::SIGKILL);
            #[cfg(not(unix))]
            let term_or_kill = false;
            (st.code(), term_or_kill)
        }
        // A failed reap is exotic; classify as a code-less, signal-less exit.
        Err(_) => (None, false),
    };

    Outcome::Finished(Attempt {
        exit_code,
        term_or_kill,
        timed_out,
        aborted,
        overflow,
        stdout: NodeBufferUtf8::decode(&stdout_buf),
        stderr: NodeBufferUtf8::decode(&stderr_buf),
    })
}

/// Spawn errors that mean ripgrep is broken, not "no matches" — surfaced, not
/// swallowed. Any other spawn failure produced no exit code, no output, and no
/// kill, so it falls through the classification to an empty result.
fn node_errno(error: &std::io::Error) -> Option<(&'static str, &'static str)> {
    #[cfg(unix)]
    match error.raw_os_error() {
        Some(libc::EACCES) => return Some(("EACCES", "permission denied")),
        Some(libc::EPERM) => return Some(("EPERM", "operation not permitted")),
        Some(libc::ENOTDIR) => return Some(("ENOTDIR", "not a directory")),
        Some(libc::ELOOP) => return Some(("ELOOP", "too many symbolic links encountered")),
        _ => {}
    }
    match error.kind() {
        std::io::ErrorKind::PermissionDenied => Some(("EACCES", "permission denied")),
        std::io::ErrorKind::NotADirectory => Some(("ENOTDIR", "not a directory")),
        _ => None,
    }
}

pub(crate) fn node_stat_error_message(error: &std::io::Error, path: &RuntimePath) -> RuntimeString {
    node_errno(error).map_or_else(
        || RuntimeString::from(error.to_string()),
        |(code, description)| {
            RuntimeString::from(format!("{code}: {description}, stat '"))
                .concat(path.as_runtime_string())
                .concat(&RuntimeString::from("'"))
        },
    )
}

fn node_spawn_error_message(error: &std::io::Error, command: &str) -> Option<String> {
    node_errno(error).map(|(code, _)| match code {
        "EACCES" => format!("spawn {command} {code}"),
        _ => format!("spawn {code}"),
    })
}

fn classify_spawn_error(
    error: std::io::Error,
    command: &str,
) -> Result<Vec<RuntimeString>, RipgrepError> {
    match error.kind() {
        std::io::ErrorKind::NotFound => Err(RipgrepError::Unavailable(
            "ripgrep (rg) was not found. Install ripgrep, set KLAUS_RIPGREP_PATH to its path, or add the @vscode/ripgrep package."
                .to_string(),
        )),
        _ => match node_spawn_error_message(&error, command) {
            Some(message) => Err(RipgrepError::Spawn(std::io::Error::new(error.kind(), message))),
            None => Ok(Vec::new()),
        },
    }
}

/// Classify one finished invocation, branch by
/// branch: clean exit → lines; exit 1 → no matches; EAGAIN → single retry;
/// timeout/abort/overflow → trailing-line drop, with only a timeout-with-zero-
/// results becoming an error.
fn handle(a: &Attempt, is_retry: bool) -> Handled {
    // A clean zero exit (no kill, no overflow) is plain success. This also
    // covers a deadline that fired only after the process had already exited
    // on its own (the drain task was scheduled late): the drained output is
    // complete, so it resolves as a full result, not as a timeout.
    if a.exit_code == Some(0) && !a.overflow && !a.aborted {
        return Handled::Done(Ok(parse_lines(&a.stdout)));
    }
    // Exit code 1 = "no matches", a normal result. (On Windows a killed child
    // also reports code 1, so a kill we initiated must not land here.)
    if a.exit_code == Some(1) && !a.overflow && !a.aborted && !a.timed_out {
        return Handled::Done(Ok(Vec::new()));
    }
    // Transient thread-spawn pressure: retry once, single-threaded.
    if !is_retry && is_eagain(&a.stderr) {
        return Handled::Retry;
    }
    // Timeout / abort / buffer-overflow: keep any partial output, but drop the
    // possibly-torn last line. A cap overflow cuts stdout mid-line just like a
    // kill does (both get the trailing-line drop).
    let is_timeout = !a.overflow && (a.timed_out || a.aborted || a.term_or_kill);
    let is_buffer_overflow = a.overflow;
    let mut lines = if !trim_ecmascript(&a.stdout).is_empty() {
        parse_lines(&a.stdout)
    } else {
        Vec::new()
    };
    if !lines.is_empty() && (is_timeout || is_buffer_overflow) {
        lines.pop();
    }
    // Only a TIMEOUT with zero results becomes an error (so "didn't finish"
    // isn't read as "no matches"); an overflow always has output and resolves
    // with the trimmed partial.
    if is_timeout && lines.is_empty() {
        return Handled::Done(Err(RipgrepError::Timeout {
            message: format!(
                "Search timed out after {}s. It may have matched files but did not finish — try a more specific path or pattern.",
                effective_timeout_ms() / 1000
            ),
            partial_results: lines,
        }));
    }
    Handled::Done(Ok(lines))
}

async fn attempt_once(
    process_context: &RuntimeProcessContext,
    args: &[RuntimeString],
    targets: &[RuntimePath],
    signal: &AbortSignal,
    is_retry: bool,
    sandbox: Option<(&dyn crate::types::sandbox::SandboxBackend, &RuntimePath)>,
) -> Handled {
    match spawn_rg(process_context, args, targets, signal, is_retry, sandbox).await {
        Outcome::ResolutionError(message) => {
            Handled::Done(Err(RipgrepError::Spawn(std::io::Error::other(message))))
        }
        Outcome::SpawnError { error, command } => {
            if sandbox.is_some() {
                return Handled::Done(Err(RipgrepError::Spawn(std::io::Error::other(format!(
                    "Task search could not launch {command}: {error}"
                )))));
            }
            Handled::Done(classify_spawn_error(error, &command))
        }
        Outcome::Finished(a) => {
            if sandbox.is_some()
                && !matches!(a.exit_code, Some(0 | 1))
                && !a.timed_out
                && !a.aborted
                && !a.overflow
                && !is_eagain(&a.stderr)
            {
                return Handled::Done(Err(RipgrepError::Spawn(std::io::Error::other(format!(
                    "Task search process failed (exit {:?}): {:?}",
                    a.exit_code, a.stderr
                )))));
            }
            handle(&a, is_retry)
        }
    }
}

/// Run ripgrep and return its stdout lines (trimmed, CR-stripped, blanks
/// dropped). Exit 0/1 are both success (1 = no matches → `[]`);
/// ENOENT/EACCES/EPERM fail (broken rg, not zero matches); a single EAGAIN
/// triggers a single-threaded retry; a timeout/abort with zero results fails
/// with [`RipgrepError::Timeout`] so "didn't finish" isn't reported as "no
/// matches".
///
/// `targets` is one-or-more search roots (a multi-folder workspace passes all
/// of its roots); ripgrep searches them in a single invocation.
pub async fn run_ripgrep(
    process_context: &RuntimeProcessContext,
    args: &[RuntimeString],
    targets: &[RuntimePath],
    signal: &AbortSignal,
) -> Result<Vec<RuntimeString>, RipgrepError> {
    run_ripgrep_impl(process_context, args, targets, signal, None).await
}

pub async fn run_task_ripgrep(
    ctx: &crate::types::tool::ToolContext,
    args: &[RuntimeString],
    targets: &[RuntimePath],
) -> Result<Vec<RuntimeString>, RipgrepError> {
    if let Some(scope) = ctx.sandbox.task_resource_scope() {
        for target in targets {
            scope
                .check_path(target, &ctx.cwd, false)
                .map_err(|error| RipgrepError::Spawn(std::io::Error::other(error)))?;
        }
        run_ripgrep_impl(
            &ctx.process_context,
            args,
            targets,
            &ctx.signal,
            Some((ctx.sandbox.as_ref(), &ctx.cwd)),
        )
        .await
    } else {
        run_ripgrep(&ctx.process_context, args, targets, &ctx.signal).await
    }
}

async fn run_ripgrep_impl(
    process_context: &RuntimeProcessContext,
    args: &[RuntimeString],
    targets: &[RuntimePath],
    signal: &AbortSignal,
    sandbox: Option<(&dyn crate::types::sandbox::SandboxBackend, &RuntimePath)>,
) -> Result<Vec<RuntimeString>, RipgrepError> {
    match attempt_once(process_context, args, targets, signal, false, sandbox).await {
        Handled::Done(result) => result,
        Handled::Retry => {
            match attempt_once(process_context, args, targets, signal, true, sandbox).await {
                Handled::Done(result) => result,
                // `handle` never asks for a second retry (guarded by `is_retry`).
                Handled::Retry => unreachable!("ripgrep retry is single-shot"),
            }
        }
    }
}

/// Default search roots when the caller gives NO explicit `path`: the session
/// cwd plus any additional writable roots (a multi-folder workspace's non-cwd
/// folders), deduped with cwd first. A non-workspace session has no extras, so
/// this collapses to `[cwd]` and search behaviour is unchanged.
pub fn default_search_roots(
    cwd: &RuntimePath,
    additional_writable_roots: &[RuntimePath],
) -> Vec<RuntimePath> {
    let mut roots = vec![cwd.clone()];
    for root in additional_writable_roots {
        if !roots
            .iter()
            .any(|existing| existing.as_runtime_string() == root.as_runtime_string())
        {
            roots.push(root.clone());
        }
    }
    roots
}

// ---- shared path / env helpers for the search tools ----

/// ECMAScript WhiteSpace + LineTerminator characters used by `String.trim`
/// and regular-expression `\s`. Deliberately excludes U+0085 (NEL) and includes
/// U+FEFF (BOM), unlike Rust's Unicode `char::is_whitespace` classification.
pub(crate) fn is_ecmascript_whitespace(value: char) -> bool {
    matches!(
        value,
        '\u{0009}'..='\u{000d}'
            | '\u{0020}'
            | '\u{00a0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200a}'
            | '\u{2028}'
            | '\u{2029}'
            | '\u{202f}'
            | '\u{205f}'
            | '\u{3000}'
            | '\u{feff}'
    )
}

fn is_ecmascript_whitespace_unit(value: u16) -> bool {
    matches!(
        value,
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

fn trim_ecmascript(value: &RuntimeString) -> RuntimeString {
    let units = value.as_utf16();
    let start = units
        .iter()
        .position(|unit| !is_ecmascript_whitespace_unit(*unit))
        .unwrap_or(units.len());
    let end = units
        .iter()
        .rposition(|unit| !is_ecmascript_whitespace_unit(*unit))
        .map_or(start, |index| index + 1);
    RuntimeString::from_utf16(units[start..end].to_vec())
}

/// Case-insensitive truthy env test: one of 1/true/yes/on.
pub fn is_env_truthy(value: Option<&str>) -> bool {
    match value {
        None => false,
        Some(v) => matches!(
            v.trim_matches(is_ecmascript_whitespace)
                .to_lowercase()
                .as_str(),
            "1" | "true" | "yes" | "on"
        ),
    }
}

/// Node's Windows home fallback order on raw OS strings: %USERPROFILE%, then
/// %HOMEDRIVE%%HOMEPATH%, then %HOME%; empty values count as absent. Called in
/// production by [`platform_home_dir`] on Windows and pinned by tests on every
/// host.
#[cfg(any(windows, test))]
fn select_windows_home(
    user_profile: Option<std::ffi::OsString>,
    home_drive: Option<std::ffi::OsString>,
    home_path: Option<std::ffi::OsString>,
    home: Option<std::ffi::OsString>,
) -> Option<std::ffi::OsString> {
    user_profile
        .filter(|value| !value.is_empty())
        .or_else(|| {
            let drive = home_drive.filter(|value| !value.is_empty())?;
            let path = home_path.filter(|value| !value.is_empty())?;
            let mut value = drive;
            value.push(path);
            Some(value)
        })
        .or_else(|| home.filter(|value| !value.is_empty()))
}

#[cfg(unix)]
fn unix_passwd_home() -> Option<std::path::PathBuf> {
    use std::ffi::CStr;
    use std::mem::MaybeUninit;
    use std::ptr;

    let suggested = unsafe { libc::sysconf(libc::_SC_GETPW_R_SIZE_MAX) };
    let mut buffer_len = if suggested > 0 {
        suggested as usize
    } else {
        16 * 1024
    };
    loop {
        let mut passwd = MaybeUninit::<libc::passwd>::uninit();
        let mut result = ptr::null_mut();
        let mut buffer = vec![0_u8; buffer_len];
        let status = unsafe {
            libc::getpwuid_r(
                libc::geteuid(),
                passwd.as_mut_ptr(),
                buffer.as_mut_ptr().cast(),
                buffer.len(),
                &mut result,
            )
        };
        if status == libc::ERANGE && buffer_len < 1024 * 1024 {
            buffer_len *= 2;
            continue;
        }
        if status != 0 || result.is_null() {
            return None;
        }
        let directory = unsafe { (*result).pw_dir };
        if directory.is_null() {
            return None;
        }
        use std::os::unix::ffi::OsStringExt;
        return Some(std::path::PathBuf::from(OsString::from_vec(
            unsafe { CStr::from_ptr(directory) }.to_bytes().to_vec(),
        )));
    }
}

/// Node-compatible platform home selection. This is visible to sibling
/// filesystem modules so deterministic caches and `~` expansion share one
/// source of truth.
pub(super) fn platform_home_dir() -> Option<std::path::PathBuf> {
    #[cfg(unix)]
    {
        std::env::var_os("HOME")
            .map(std::path::PathBuf::from)
            .or_else(unix_passwd_home)
    }
    #[cfg(windows)]
    {
        select_windows_home(
            std::env::var_os("USERPROFILE"),
            std::env::var_os("HOMEDRIVE"),
            std::env::var_os("HOMEPATH"),
            std::env::var_os("HOME"),
        )
        .map(std::path::PathBuf::from)
        .or_else(|| {
            #[allow(deprecated)]
            std::env::home_dir()
        })
    }
    #[cfg(not(any(unix, windows)))]
    {
        std::env::var_os("HOME").map(std::path::PathBuf::from)
    }
}

fn platform_home_path() -> Option<RuntimePath> {
    platform_home_dir().map(|path| runtime_path_from_native(&path))
}

/// Expand a leading `~`, then resolve to an absolute (NFC-normalized) path
/// against `cwd`. Already-absolute inputs are returned as-is
/// (trimmed and NFC'd), without collapsing `.`/`..`.
pub fn expand_path(p: &RuntimeString, cwd: &RuntimePath) -> Result<RuntimePath, ToolError> {
    if p.as_utf16().contains(&0) {
        return Err(ToolError::new("Path contains null bytes"));
    }
    let mut s = trim_ecmascript(p);
    if s == "~" {
        s = platform_home_path()
            .unwrap_or_default()
            .into_runtime_string();
    } else if s.starts_with("~/") || s.starts_with("~\\") {
        // Join under the home directory; an absolute home makes resolve a pure
        // normalize (matching a path join's `..` collapsing).
        let home = platform_home_path().unwrap_or_default();
        let joined = RuntimePath::from(
            home.as_runtime_string()
                .concat(&RuntimeString::from("/"))
                .concat(&s.slice_utf16(2..)),
        );
        s = if is_absolute_runtime(&joined) {
            resolve_runtime(cwd, &[joined])
                .map_err(|error| ToolError::new(runtime_path_error_host(error.message())))?
                .into_runtime_string()
        } else {
            joined.into_runtime_string()
        };
    }
    let path = RuntimePath::from(normalize_nfc(&s));
    if is_absolute_runtime(&path) {
        Ok(path)
    } else {
        resolve_runtime(cwd, &[cwd.clone(), path])
            .map_err(|error| ToolError::new(runtime_path_error_host(error.message())))
    }
}

/// Relativize `abs` under `cwd` to save tokens; falls back to the absolute
/// path when outside cwd.
///
/// POSIX-rendered: the result goes straight into a tool result the model reads, and the
/// engine tells the model to use forward slashes on Windows.
pub fn to_relative_path(abs: &RuntimePath, cwd: &RuntimePath) -> RuntimePath {
    let rel = relative_posix_runtime(cwd, cwd, abs)
        .expect("absolute search paths compute an exact relative path");
    if rel.as_runtime_string().is_empty() {
        return RuntimePath::from(".");
    }
    if !rel.as_runtime_string().starts_with("..") && !is_absolute_runtime(&rel) {
        return rel;
    }
    abs.clone()
}

fn runtime_path_error_host(value: &RuntimeString) -> String {
    String::from_utf8(value.to_node_utf8()).expect("Node UTF-8 path errors are valid UTF-8")
}

fn normalize_nfc(value: &RuntimeString) -> RuntimeString {
    let mut output = Vec::new();
    let mut scalar_run = String::new();
    let flush = |run: &mut String, output: &mut Vec<u16>| {
        for scalar in run.nfc() {
            let mut encoded = [0; 2];
            output.extend_from_slice(scalar.encode_utf16(&mut encoded));
        }
        run.clear();
    };
    for point in value.code_points() {
        if let Some(scalar) = char::from_u32(point) {
            scalar_run.push(scalar);
        } else {
            flush(&mut scalar_run, &mut output);
            output.push(point as u16);
        }
    }
    flush(&mut scalar_run, &mut output);
    RuntimeString::from_utf16(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    fn r(value: &str) -> RuntimeString {
        RuntimeString::from(value)
    }

    fn parse_lines(value: &str) -> Vec<String> {
        super::parse_lines(&r(value))
            .into_iter()
            .map(|line| {
                String::from_utf8(line.to_node_utf8()).expect("Node UTF-8 test line is valid")
            })
            .collect()
    }

    fn is_eagain(value: &str) -> bool {
        super::is_eagain(&r(value))
    }

    fn expand_path(value: &str, cwd: &str) -> Result<String, ToolError> {
        super::expand_path(&r(value), &RuntimePath::from(cwd)).map(|path| {
            String::from_utf8(path.as_runtime_string().to_node_utf8())
                .expect("Node UTF-8 test path is valid")
        })
    }

    /// A POSIX-shaped expectation rendered with the host separator.
    ///
    /// `expand_path` produces a path for real filesystem access, so it follows the host
    /// dialect; only the separator differs from platform to platform.
    fn shaped(expected: &str) -> String {
        if cfg!(windows) {
            expected.replace('/', "\\")
        } else {
            expected.to_string()
        }
    }

    fn to_relative_path(abs: &str, cwd: &str) -> String {
        String::from_utf8(
            super::to_relative_path(&RuntimePath::from(abs), &RuntimePath::from(cwd))
                .as_runtime_string()
                .to_node_utf8(),
        )
        .expect("Node UTF-8 test path is valid")
    }

    /// Gated like its only caller: the errno cases it formats are unix ones, so on
    /// Windows this helper has no user and shows up as dead code.
    #[cfg(unix)]
    fn node_stat_error_message(error: &std::io::Error, path: &str) -> String {
        String::from_utf8(
            super::node_stat_error_message(error, &RuntimePath::from(path)).to_node_utf8(),
        )
        .expect("Node UTF-8 test error is valid")
    }

    fn home_dir() -> String {
        platform_home_dir()
            .map(|path| path.to_string_lossy().into_owned())
            .unwrap_or_default()
    }

    async fn run_ripgrep(
        args: &[String],
        target: &str,
        signal: &AbortSignal,
    ) -> Result<Vec<String>, RipgrepError> {
        let args = args.iter().map(RuntimeString::from).collect::<Vec<_>>();
        super::run_ripgrep(
            &crate::runtime_process::test_runtime_process_context(),
            &args,
            &[RuntimePath::from(target)],
            signal,
        )
        .await
        .map(|lines| {
            lines
                .into_iter()
                .map(|line| {
                    String::from_utf8(line.to_node_utf8()).expect("Node UTF-8 test result is valid")
                })
                .collect()
        })
    }

    /// Serializes tests that touch process-global state (env vars, the
    /// memoized binary path) or invoke rg (which reads that state). This is the
    /// CRATE-level lock, not a module-private one: `tools::builtins::grep`
    /// installs a fake rg through the same globals, and a per-module lock would
    /// not serialize against it.
    async fn env_guard() -> (
        tokio::sync::MutexGuard<'static, ()>,
        tokio::sync::MutexGuard<'static, ()>,
    ) {
        crate::test_support::ripgrep_global_guard().await
    }

    fn temp_dir() -> PathBuf {
        let mut dir = std::env::temp_dir();
        let unique = format!(
            "ae-ripgrep-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        dir.push(unique);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn rg_available() -> bool {
        std::process::Command::new("rg")
            .arg("--version")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }

    fn base_attempt(exit_code: Option<i32>, stdout: &str) -> Attempt {
        Attempt {
            exit_code,
            stdout: r(stdout),
            ..Default::default()
        }
    }

    fn expect_ok(h: Handled) -> Vec<String> {
        match h {
            Handled::Done(Ok(v)) => v
                .into_iter()
                .map(|line| {
                    String::from_utf8(line.to_node_utf8()).expect("Node UTF-8 test line is valid")
                })
                .collect(),
            other => panic!("expected Ok, got {other:?}"),
        }
    }

    // ---- pure logic ----

    #[test]
    fn parse_lines_trims_strips_cr_drops_blanks() {
        assert_eq!(parse_lines("a\nb\n"), vec!["a", "b"]);
        assert_eq!(parse_lines("a\r\nb\r"), vec!["a", "b"]);
        assert_eq!(parse_lines("a\n\n\nb"), vec!["a", "b"]);
        assert_eq!(parse_lines(""), Vec::<String>::new());
        assert_eq!(parse_lines("  \n"), Vec::<String>::new());
        assert_eq!(parse_lines("\r\n"), Vec::<String>::new());
    }

    #[test]
    fn parse_lines_uses_ecmascript_trim_whitespace() {
        assert_eq!(
            (
                parse_lines("\u{0085}a\u{0085}"),
                parse_lines("\u{feff}a\u{feff}"),
                parse_lines("\u{feff}"),
            ),
            (
                vec!["\u{0085}a\u{0085}".to_string()],
                vec!["a".to_string()],
                Vec::<String>::new(),
            )
        );
    }

    #[test]
    fn is_eagain_detection() {
        assert!(is_eagain("rg: some/path: Resource temporarily unavailable"));
        assert!(is_eagain("thread spawn failed (os error 11)"));
        assert!(!is_eagain("regex parse error"));
        assert!(!is_eagain(""));
    }

    #[test]
    fn parses_integer_prefix() {
        assert_eq!(parse_int_prefix("15"), 15);
        assert_eq!(parse_int_prefix("  15 "), 15);
        assert_eq!(parse_int_prefix("15abc"), 15);
        assert_eq!(parse_int_prefix("7.9"), 7);
        assert_eq!(parse_int_prefix("-3"), -3);
        assert_eq!(parse_int_prefix("+4"), 4);
        assert_eq!(parse_int_prefix("abc"), 0);
        assert_eq!(parse_int_prefix(""), 0);
    }

    #[test]
    fn integer_prefix_uses_ecmascript_leading_whitespace() {
        assert_eq!(
            (
                parse_int_prefix("\u{0085}15"),
                parse_int_prefix("\u{feff}15"),
            ),
            (0, 15)
        );
    }

    #[tokio::test]
    async fn timeout_env_override() {
        let _g = env_guard().await;
        let default_ms = if is_wsl() {
            WSL_TIMEOUT_MS
        } else {
            DEFAULT_TIMEOUT_MS
        };
        crate::runtime_process::remove_process_environment_var("KLAUS_GLOB_TIMEOUT_SECONDS");
        assert_eq!(effective_timeout_ms(), default_ms);
        crate::runtime_process::set_process_environment_var("KLAUS_GLOB_TIMEOUT_SECONDS", "15");
        assert_eq!(effective_timeout_ms(), 15_000);
        crate::runtime_process::set_process_environment_var("KLAUS_GLOB_TIMEOUT_SECONDS", "7.9"); // integer prefix
        assert_eq!(effective_timeout_ms(), 7_000);
        crate::runtime_process::set_process_environment_var("KLAUS_GLOB_TIMEOUT_SECONDS", "0");
        assert_eq!(effective_timeout_ms(), default_ms);
        crate::runtime_process::set_process_environment_var("KLAUS_GLOB_TIMEOUT_SECONDS", "-3");
        assert_eq!(effective_timeout_ms(), default_ms);
        crate::runtime_process::set_process_environment_var("KLAUS_GLOB_TIMEOUT_SECONDS", "abc");
        assert_eq!(effective_timeout_ms(), default_ms);
        crate::runtime_process::remove_process_environment_var("KLAUS_GLOB_TIMEOUT_SECONDS");
    }

    #[tokio::test]
    async fn resolves_env_override_then_bundled_ripgrep() {
        let _g = env_guard().await;
        crate::runtime_process::remove_process_environment_var("KLAUS_RIPGREP_PATH");
        reset_ripgrep_command_for_test();
        let default_command = resolve_ripgrep_command().unwrap();
        // Memoized: a later override is ignored until reset.
        crate::runtime_process::set_process_environment_var("KLAUS_RIPGREP_PATH", "/custom/rg");
        assert_eq!(resolve_ripgrep_command().unwrap(), default_command);
        reset_ripgrep_command_for_test();
        assert_eq!(
            resolve_ripgrep_command().unwrap(),
            OsString::from("/custom/rg")
        );
        // The override is trimmed.
        crate::runtime_process::set_process_environment_var("KLAUS_RIPGREP_PATH", "  /custom/rg  ");
        reset_ripgrep_command_for_test();
        assert_eq!(
            resolve_ripgrep_command().unwrap(),
            OsString::from("/custom/rg")
        );
        // A whitespace-only override falls through to the fixed-lock default.
        crate::runtime_process::set_process_environment_var("KLAUS_RIPGREP_PATH", "   ");
        reset_ripgrep_command_for_test();
        assert_eq!(resolve_ripgrep_command().unwrap(), default_command);
        crate::runtime_process::remove_process_environment_var("KLAUS_RIPGREP_PATH");
        reset_ripgrep_command_for_test();
    }

    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    #[tokio::test]
    async fn resolves_bundled_ripgrep_before_the_system_path() {
        let _g = env_guard().await;
        crate::runtime_process::remove_process_environment_var("KLAUS_RIPGREP_PATH");
        reset_ripgrep_command_for_test();

        let command = resolve_ripgrep_command().unwrap();
        assert_ne!(command, OsString::from("rg"));
        assert!(std::path::Path::new(&command).is_file());
        let output = std::process::Command::new(&command)
            .arg("--version")
            .output()
            .unwrap();
        assert!(output.status.success());
        assert!(String::from_utf8_lossy(&output.stdout).starts_with("ripgrep 15.0.0"));

        reset_ripgrep_command_for_test();
    }

    fn executable_rg_fixture(directory: &Path) -> PathBuf {
        std::fs::create_dir_all(directory).unwrap();
        let name = if cfg!(windows) { "rg.exe" } else { "rg" };
        let path = directory.join(name);
        std::fs::write(&path, b"#!/bin/sh\nexit 0\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        path
    }

    fn path_value(env: &RuntimeEnv) -> String {
        String::from_utf8(
            env.get(&RuntimeString::from("PATH"))
                .expect("PATH present")
                .to_node_utf8(),
        )
        .expect("PATH is UTF-8")
    }

    fn use_override_rg(override_rg: &Path) {
        crate::runtime_process::set_process_environment_var(
            "KLAUS_RIPGREP_PATH",
            override_rg.to_str().unwrap(),
        );
        reset_ripgrep_command_for_test();
        reset_ripgrep_tool_executable_for_test();
    }

    fn clear_override_rg() {
        crate::runtime_process::remove_process_environment_var("KLAUS_RIPGREP_PATH");
        reset_ripgrep_command_for_test();
        reset_ripgrep_tool_executable_for_test();
    }

    #[tokio::test]
    async fn ensure_ripgrep_on_path_leaves_a_path_that_already_resolves_rg() {
        let _g = env_guard().await;
        let root = temp_dir();
        let on_path_rg = executable_rg_fixture(&root.join("on-path"));
        let override_rg = executable_rg_fixture(&root.join("override"));
        use_override_rg(&override_rg);

        let mut env = RuntimeEnv::new();
        let on_path_value = on_path_rg.parent().unwrap().to_str().unwrap().to_string();
        env.insert(
            RuntimeString::from("PATH"),
            RuntimeString::from(on_path_value.as_str()),
        );
        ensure_ripgrep_on_path(&mut env);
        assert_eq!(path_value(&env), on_path_value);

        clear_override_rg();
        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn ensure_ripgrep_on_path_prepends_the_resolved_directory_when_rg_is_missing() {
        let _g = env_guard().await;
        let root = temp_dir();
        let empty_dir = root.join("empty");
        std::fs::create_dir_all(&empty_dir).unwrap();
        let override_rg = executable_rg_fixture(&root.join("override"));
        use_override_rg(&override_rg);

        let mut env = RuntimeEnv::new();
        let original = empty_dir.to_str().unwrap().to_string();
        env.insert(
            RuntimeString::from("PATH"),
            RuntimeString::from(original.as_str()),
        );
        ensure_ripgrep_on_path(&mut env);
        let separator = if cfg!(windows) { ';' } else { ':' };
        let expected = format!(
            "{}{separator}{original}",
            override_rg.parent().unwrap().to_str().unwrap()
        );
        assert_eq!(path_value(&env), expected);

        clear_override_rg();
        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn ensure_ripgrep_on_path_creates_path_when_the_environment_has_none() {
        let _g = env_guard().await;
        let root = temp_dir();
        let override_rg = executable_rg_fixture(&root.join("override"));
        use_override_rg(&override_rg);

        let mut env = RuntimeEnv::new();
        ensure_ripgrep_on_path(&mut env);
        assert_eq!(
            path_value(&env),
            override_rg.parent().unwrap().to_str().unwrap()
        );

        clear_override_rg();
        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn env_override_uses_ecmascript_trim_whitespace() {
        let _g = env_guard().await;

        crate::runtime_process::set_process_environment_var(
            "KLAUS_RIPGREP_PATH",
            "\u{0085}/custom/rg\u{0085}",
        );
        reset_ripgrep_command_for_test();
        let nel_wrapped = resolve_ripgrep_command().unwrap();

        crate::runtime_process::set_process_environment_var(
            "KLAUS_RIPGREP_PATH",
            "\u{feff}/custom/rg\u{feff}",
        );
        reset_ripgrep_command_for_test();
        let bom_wrapped = resolve_ripgrep_command().unwrap();

        crate::runtime_process::set_process_environment_var("KLAUS_RIPGREP_PATH", "\u{0085}");
        reset_ripgrep_command_for_test();
        let nel_only = resolve_ripgrep_command().unwrap();

        crate::runtime_process::set_process_environment_var("KLAUS_RIPGREP_PATH", "\u{feff}");
        reset_ripgrep_command_for_test();
        let bom_only = resolve_ripgrep_command().unwrap();

        crate::runtime_process::remove_process_environment_var("KLAUS_RIPGREP_PATH");
        reset_ripgrep_command_for_test();

        assert_eq!(
            (nel_wrapped, bom_wrapped, nel_only, bom_only),
            (
                OsString::from("\u{0085}/custom/rg\u{0085}"),
                OsString::from("/custom/rg"),
                OsString::from("\u{0085}"),
                bundled_ripgrep_command()
                    .unwrap()
                    .expect("the test host must have a bundled ripgrep asset")
                    .as_os_str()
                    .to_owned(),
            )
        );
    }

    #[test]
    fn append_capped_truncates_at_cap() {
        let mut buf = vec![0u8; MAX_BUFFER - 2];
        assert!(!append_capped(&mut buf, b"a")); // fits
        assert!(append_capped(&mut buf, b"bc")); // crosses — truncated to the cap
        assert_eq!(buf.len(), MAX_BUFFER);
        assert_eq!(&buf[MAX_BUFFER - 2..], b"ab");
    }

    // ---- classification (handle) ----

    #[test]
    fn exit_zero_parses_lines() {
        let a = base_attempt(Some(0), "a\r\nb\n");
        assert_eq!(expect_ok(handle(&a, false)), vec!["a", "b"]);
    }

    #[test]
    fn exit_one_is_no_matches() {
        let a = base_attempt(Some(1), "");
        assert_eq!(expect_ok(handle(&a, false)), Vec::<String>::new());
    }

    #[test]
    fn exit_two_resolves_with_whatever_stdout_has() {
        // A hard rg error (e.g. bad regex) resolves with the (usually empty)
        // stdout, no trailing-line drop.
        let mut a = base_attempt(Some(2), "x\ny");
        a.stderr = r("regex parse error");
        assert_eq!(expect_ok(handle(&a, false)), vec!["x", "y"]);
        let b = base_attempt(Some(2), "");
        assert_eq!(expect_ok(handle(&b, false)), Vec::<String>::new());
    }

    #[tokio::test]
    async fn timeout_with_zero_results_is_error_with_message() {
        let _g = env_guard().await;
        crate::runtime_process::remove_process_environment_var("KLAUS_GLOB_TIMEOUT_SECONDS");
        let secs = effective_timeout_ms() / 1000;
        let mut a = base_attempt(None, "");
        a.timed_out = true;
        match handle(&a, false) {
            Handled::Done(Err(RipgrepError::Timeout {
                message,
                partial_results,
            })) => {
                assert_eq!(
                    message,
                    format!("Search timed out after {secs}s. It may have matched files but did not finish — try a more specific path or pattern.")
                );
                assert!(partial_results.is_empty());
            }
            other => panic!("expected timeout error, got {other:?}"),
        }
    }

    #[test]
    fn timeout_with_partial_output_drops_torn_last_line() {
        let mut a = base_attempt(None, "a\nb\nc");
        a.timed_out = true;
        assert_eq!(expect_ok(handle(&a, false)), vec!["a", "b"]);
    }

    #[test]
    fn partial_output_check_uses_ecmascript_trim_whitespace() {
        let mut nel = base_attempt(None, "\u{0085}kept\npartial");
        nel.timed_out = true;
        let nel_lines = expect_ok(handle(&nel, false));

        let mut bom = base_attempt(None, "\u{feff}kept\npartial");
        bom.timed_out = true;
        let bom_lines = expect_ok(handle(&bom, false));

        assert_eq!(
            (nel_lines, bom_lines),
            (vec!["\u{0085}kept".to_string()], vec!["kept".to_string()],)
        );
    }

    #[test]
    fn late_deadline_after_clean_exit_resolves_with_complete_output() {
        // A starved drain task can observe the deadline after the process
        // already exited cleanly; the drained output is complete, so no
        // torn-line drop applies.
        let mut a = base_attempt(Some(0), "a\nb\n");
        a.timed_out = true;
        assert_eq!(expect_ok(handle(&a, false)), vec!["a", "b"]);
    }

    #[test]
    fn timeout_with_single_line_still_errors() {
        // One partial line: dropping the torn line leaves zero results → error.
        let mut a = base_attempt(None, "only");
        a.timed_out = true;
        match handle(&a, false) {
            Handled::Done(Err(RipgrepError::Timeout {
                partial_results, ..
            })) => assert!(partial_results.is_empty()),
            other => panic!("expected timeout error, got {other:?}"),
        }
    }

    #[test]
    fn abort_classifies_like_timeout() {
        let mut a = base_attempt(None, "");
        a.aborted = true;
        assert!(matches!(
            handle(&a, false),
            Handled::Done(Err(RipgrepError::Timeout { .. }))
        ));
    }

    #[test]
    fn external_kill_signal_classifies_like_timeout() {
        let mut a = base_attempt(None, "x\ny\nz");
        a.term_or_kill = true;
        assert_eq!(expect_ok(handle(&a, false)), vec!["x", "y"]);
        let mut b = base_attempt(None, "");
        b.term_or_kill = true;
        assert!(matches!(
            handle(&b, false),
            Handled::Done(Err(RipgrepError::Timeout { .. }))
        ));
    }

    #[test]
    fn overflow_always_resolves_with_trailing_line_dropped() {
        let mut a = base_attempt(None, "a\nb");
        a.overflow = true;
        assert_eq!(expect_ok(handle(&a, false)), vec!["a"]);
        // Even with zero lines, an overflow resolves — it never becomes a
        // timeout error.
        let mut b = base_attempt(None, "");
        b.overflow = true;
        assert_eq!(expect_ok(handle(&b, false)), Vec::<String>::new());
    }

    #[test]
    fn eagain_requests_single_retry() {
        let mut a = base_attempt(Some(2), "");
        a.stderr = r("rg: Resource temporarily unavailable");
        assert!(matches!(handle(&a, false), Handled::Retry));
        // On the retry itself, EAGAIN falls through to normal classification.
        assert_eq!(expect_ok(handle(&a, true)), Vec::<String>::new());
    }

    #[test]
    fn eagain_retry_precedes_timeout_classification() {
        // A timed-out run whose stderr shows EAGAIN still retries.
        let mut a = base_attempt(None, "");
        a.timed_out = true;
        a.stderr = r("thread spawn failed (os error 11)");
        assert!(matches!(handle(&a, false), Handled::Retry));
    }

    // ---- shared helpers ----

    #[test]
    fn is_env_truthy_cases() {
        assert!(is_env_truthy(Some("1")));
        assert!(is_env_truthy(Some("TRUE")));
        assert!(is_env_truthy(Some(" yes ")));
        assert!(is_env_truthy(Some("On")));
        assert!(!is_env_truthy(None));
        assert!(!is_env_truthy(Some("")));
        assert!(!is_env_truthy(Some("0")));
        assert!(!is_env_truthy(Some("off")));
    }

    #[test]
    fn is_env_truthy_uses_ecmascript_trim_whitespace() {
        assert_eq!(
            (
                is_env_truthy(Some("\u{0085}true\u{0085}")),
                is_env_truthy(Some("\u{feff}true\u{feff}")),
            ),
            (false, true)
        );
    }

    #[test]
    fn expand_path_resolves_relative_and_keeps_absolute() {
        assert_eq!(
            expand_path("src/main.rs", "/repo").unwrap(),
            shaped("/repo/src/main.rs")
        );
        // Relative paths are normalized against cwd…
        assert_eq!(expand_path("a/../b", "/repo").unwrap(), shaped("/repo/b"));
        // …but absolute paths are returned as-is, without collapsing.
        assert_eq!(expand_path("/a/../b", "/repo").unwrap(), "/a/../b");
        // Input is trimmed.
        assert_eq!(expand_path("  /x  ", "/repo").unwrap(), "/x");
    }

    #[test]
    fn expand_path_uses_ecmascript_string_trim_boundaries() {
        assert_eq!(
            expand_path("\u{0085}.\u{0085}", "/repo").unwrap(),
            shaped("/repo/\u{0085}.\u{0085}")
        );
        assert_eq!(
            expand_path("\u{feff}.\u{feff}", "/repo").unwrap(),
            shaped("/repo")
        );
    }

    #[test]
    fn expand_path_rejects_null_bytes() {
        let err = expand_path("a\0b", "/repo").unwrap_err();
        assert_eq!(err.message, "Path contains null bytes");
    }

    #[test]
    fn windows_home_candidates_follow_node_compatible_fallback_order() {
        let candidate = |value: &str| Some(std::ffi::OsString::from(value));
        assert_eq!(
            select_windows_home(
                candidate(r"C:\Users\alice"),
                candidate("D:"),
                candidate(r"\Profiles\alice"),
                candidate(r"E:\home\alice"),
            ),
            candidate(r"C:\Users\alice")
        );
        assert_eq!(
            select_windows_home(
                None,
                candidate("D:"),
                candidate(r"\Profiles\alice"),
                candidate(r"E:\home\alice"),
            ),
            candidate(r"D:\Profiles\alice")
        );
        assert_eq!(
            select_windows_home(None, None, None, candidate(r"E:\home\alice")),
            candidate(r"E:\home\alice")
        );
        assert_eq!(select_windows_home(candidate(""), None, None, None), None);
    }

    #[cfg(unix)]
    #[test]
    fn node_stat_errno_messages_match_fs_stat() {
        let path = "/repo/target";
        let cases = [
            (
                libc::EACCES,
                "EACCES: permission denied, stat '/repo/target'",
            ),
            (
                libc::EPERM,
                "EPERM: operation not permitted, stat '/repo/target'",
            ),
            (
                libc::ENOTDIR,
                "ENOTDIR: not a directory, stat '/repo/target'",
            ),
            (
                libc::ELOOP,
                "ELOOP: too many symbolic links encountered, stat '/repo/target'",
            ),
        ];
        for (raw, expected) in cases {
            let error = std::io::Error::from_raw_os_error(raw);
            assert_eq!(node_stat_error_message(&error, path), expected);
        }
    }

    #[cfg(unix)]
    #[test]
    fn node_spawn_errno_messages_match_child_process() {
        let cases = [
            (libc::EACCES, "spawn /opt/rg EACCES"),
            (libc::EPERM, "spawn EPERM"),
            (libc::ENOTDIR, "spawn ENOTDIR"),
            (libc::ELOOP, "spawn ELOOP"),
        ];
        for (raw, expected) in cases {
            let error = std::io::Error::from_raw_os_error(raw);
            let result = classify_spawn_error(error, "/opt/rg");
            let RipgrepError::Spawn(error) = result.unwrap_err() else {
                panic!("errno {raw} was not surfaced as a spawn error");
            };
            assert_eq!(error.to_string(), expected);
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn tilde_uses_passwd_home_when_home_is_unset() {
        let _g = env_guard().await;
        let Some(expected) = unix_passwd_home() else {
            return;
        };
        let previous = std::env::var_os("HOME");
        crate::runtime_process::remove_process_environment_var("HOME");
        let selected = platform_home_dir();
        let expanded = expand_path("~", "/tmp/fallback-cwd").unwrap();
        if let Some(previous) = previous {
            crate::runtime_process::set_process_environment_var("HOME", previous);
        } else {
            crate::runtime_process::remove_process_environment_var("HOME");
        }
        assert_eq!(selected, Some(expected.clone()));
        assert_eq!(expanded, expected.to_string_lossy());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn empty_home_is_preserved_like_node_os_homedir() {
        let _g = env_guard().await;
        let previous = std::env::var_os("HOME");
        crate::runtime_process::set_process_environment_var("HOME", "");
        let selected = platform_home_dir();
        if let Some(previous) = previous {
            crate::runtime_process::set_process_environment_var("HOME", previous);
        } else {
            crate::runtime_process::remove_process_environment_var("HOME");
        }
        assert_eq!(selected, Some(PathBuf::new()));
    }

    #[test]
    fn expand_path_normalizes_to_nfc() {
        // "e" + combining acute → precomposed "é".
        assert_eq!(
            expand_path("cafe\u{301}", "/repo").unwrap(),
            shaped("/repo/caf\u{e9}")
        );
    }

    #[test]
    fn expand_path_expands_tilde() {
        let home = home_dir();
        if home.is_empty() {
            return; // no home directory in this environment — nothing to assert
        }
        assert_eq!(expand_path("~", "/repo").unwrap(), home);
        let expanded = expand_path("~/sub/file.txt", "/repo").unwrap();
        assert!(expanded.starts_with(&home), "{expanded}");
        assert!(expanded.ends_with(&shaped("sub/file.txt")), "{expanded}");
        assert!(!expanded.contains('~'));
    }

    #[test]
    fn to_relative_path_inside_equal_and_outside() {
        assert_eq!(to_relative_path("/repo/src/a.rs", "/repo"), "src/a.rs");
        assert_eq!(to_relative_path("/repo", "/repo"), ".");
        assert_eq!(to_relative_path("/etc/passwd", "/repo"), "/etc/passwd");
        assert_eq!(to_relative_path("/repo2/x", "/repo"), "/repo2/x");
    }

    // ---- integration (skips when rg is absent) ----

    #[tokio::test]
    async fn lists_files_and_finds_content_matches() {
        let _g = env_guard().await;
        crate::runtime_process::remove_process_environment_var("KLAUS_RIPGREP_PATH");
        reset_ripgrep_command_for_test();
        if !rg_available() {
            return;
        }
        let dir = temp_dir();
        std::fs::write(dir.join("a.txt"), "hello world\n").unwrap();
        std::fs::write(dir.join("b.rs"), "fn main() {}\n").unwrap();
        let d = dir.to_string_lossy().into_owned();
        let signal = AbortSignal::new();

        let files = run_ripgrep(&["--files".to_string()], &d, &signal)
            .await
            .unwrap();
        assert_eq!(files.len(), 2);
        assert!(files.iter().any(|f| f.ends_with("a.txt")));
        assert!(files.iter().any(|f| f.ends_with("b.rs")));

        let hits = run_ripgrep(&["-l".to_string(), "hello".to_string()], &d, &signal)
            .await
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert!(hits[0].ends_with("a.txt"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn default_search_roots_dedups_and_keeps_cwd_first() {
        let cwd = RuntimePath::from("/repo/main");
        // No extras -> unchanged single-root behaviour.
        assert_eq!(super::default_search_roots(&cwd, &[]), vec![cwd.clone()]);
        // Extra roots append after cwd; a duplicate of cwd is dropped.
        let extras = [
            RuntimePath::from("/repo/shared"),
            RuntimePath::from("/repo/main"),
            RuntimePath::from("/repo/lib"),
        ];
        assert_eq!(
            super::default_search_roots(&cwd, &extras),
            vec![
                RuntimePath::from("/repo/main"),
                RuntimePath::from("/repo/shared"),
                RuntimePath::from("/repo/lib"),
            ]
        );
    }

    #[tokio::test]
    async fn searches_multiple_targets_in_one_pass() {
        let _g = env_guard().await;
        crate::runtime_process::remove_process_environment_var("KLAUS_RIPGREP_PATH");
        reset_ripgrep_command_for_test();
        if !rg_available() {
            return;
        }
        // Two distinct workspace roots, one matching file in each.
        let root_a = temp_dir();
        let root_b = temp_dir();
        std::fs::write(root_a.join("a.txt"), "needle\n").unwrap();
        std::fs::write(root_b.join("b.txt"), "needle\n").unwrap();
        let signal = AbortSignal::new();
        let args: Vec<RuntimeString> = ["-l", "needle"]
            .iter()
            .map(|s| RuntimeString::from(*s))
            .collect();
        let targets = [
            RuntimePath::from(root_a.to_string_lossy().into_owned().as_str()),
            RuntimePath::from(root_b.to_string_lossy().into_owned().as_str()),
        ];
        let hits: Vec<String> = super::run_ripgrep(
            &crate::runtime_process::test_runtime_process_context(),
            &args,
            &targets,
            &signal,
        )
        .await
        .unwrap()
        .into_iter()
        .map(|line| String::from_utf8(line.to_node_utf8()).expect("valid UTF-8"))
        .collect();
        // A single ripgrep invocation returns matches from BOTH roots.
        assert!(
            hits.iter().any(|h| h.ends_with("a.txt")),
            "root A miss: {hits:?}"
        );
        assert!(
            hits.iter().any(|h| h.ends_with("b.txt")),
            "root B miss: {hits:?}"
        );
        let _ = std::fs::remove_dir_all(&root_a);
        let _ = std::fs::remove_dir_all(&root_b);
    }

    #[tokio::test]
    async fn no_matches_resolves_empty() {
        let _g = env_guard().await;
        crate::runtime_process::remove_process_environment_var("KLAUS_RIPGREP_PATH");
        reset_ripgrep_command_for_test();
        if !rg_available() {
            return;
        }
        let dir = temp_dir();
        std::fs::write(dir.join("a.txt"), "hello\n").unwrap();
        let d = dir.to_string_lossy().into_owned();
        let signal = AbortSignal::new();
        let out = run_ripgrep(
            &["-l".to_string(), "zzz_absent_needle_zzz".to_string()],
            &d,
            &signal,
        )
        .await
        .unwrap();
        assert!(out.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn invalid_regex_resolves_empty() {
        let _g = env_guard().await;
        crate::runtime_process::remove_process_environment_var("KLAUS_RIPGREP_PATH");
        reset_ripgrep_command_for_test();
        if !rg_available() {
            return;
        }
        let dir = temp_dir();
        std::fs::write(dir.join("a.txt"), "hello\n").unwrap();
        let d = dir.to_string_lossy().into_owned();
        let signal = AbortSignal::new();
        // rg exits 2 on a bad pattern; that resolves with (empty) stdout.
        let out = run_ripgrep(&["-l".to_string(), "(".to_string()], &d, &signal)
            .await
            .unwrap();
        assert!(out.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn missing_binary_is_unavailable_error() {
        let _g = env_guard().await;
        let dir = temp_dir();
        let missing = dir
            .join("definitely-missing-rg")
            .to_string_lossy()
            .into_owned();
        crate::runtime_process::set_process_environment_var("KLAUS_RIPGREP_PATH", &missing);
        reset_ripgrep_command_for_test();
        let d = dir.to_string_lossy().into_owned();
        let signal = AbortSignal::new();
        let err = run_ripgrep(&["--files".to_string()], &d, &signal)
            .await
            .unwrap_err();
        let message = match err {
            RipgrepError::Unavailable(msg) => msg,
            other => panic!("expected Unavailable, got {other:?}"),
        };
        crate::runtime_process::remove_process_environment_var("KLAUS_RIPGREP_PATH");
        reset_ripgrep_command_for_test();
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(
            message,
            "ripgrep (rg) was not found. Install ripgrep, set KLAUS_RIPGREP_PATH to its path, or add the @vscode/ripgrep package."
        );
    }

    /// A run that finished before the deadline was observed must resolve with its
    /// COMPLETE output, not as a zero-result timeout: the drain loop polls the
    /// deadline ahead of the reads, so a starved task can reach the timeout branch
    /// with the process already exited and its full output sitting in the pipes.
    ///
    /// Both preconditions are established as facts instead of inferred from
    /// wall-clock magnitudes. The fake binary touches `started` before it works and
    /// `done` after its last write, so:
    ///  - "the process exited on its own" is gated on `done` appearing. The kill can
    ///    only be issued when the future is polled again, so a child that already
    ///    finished can no longer be killed however long the host starved it.
    ///  - "the deadline elapsed while unpolled" is gated on an absolute wake-up
    ///    computed from the observed spawn, and a sleep may only overshoot it.
    ///
    /// Pinning the stages to fixed durations instead (0.5s fake exit, 30s deadline,
    /// 31s resume) made this a coin flip: a saturated host starved the child past
    /// the resume point, the timeout branch then killed a still-live process, and
    /// the torn-line drop cut output that was supposed to be complete.
    #[cfg(unix)]
    #[tokio::test]
    async fn starved_deadline_still_returns_output_of_a_finished_run() {
        use std::future::Future;
        use std::task::Poll;

        // The deadline only has to outlast the phase that still polls (spawn until
        // `started`); everything after that runs unpolled, where no timer can fire.
        const DEADLINE: Duration = Duration::from_secs(10);
        // Long enough that the spawn-detection loop cannot accidentally drain the
        // run to EOF and miss the scenario entirely.
        const FAKE_WORK: Duration = Duration::from_secs(3);

        let _g = env_guard().await;
        let dir = temp_dir();
        let fake_rg = dir.join("fake-rg.sh");
        let started_marker = dir.join("started");
        let done_marker = dir.join("done");
        std::fs::write(
            &fake_rg,
            format!(
                "#!/bin/sh\ntouch '{started}'\nsleep {work}\necho one\necho two\ntouch '{done}'\nexit 0\n",
                started = started_marker.display(),
                work = FAKE_WORK.as_secs(),
                done = done_marker.display(),
            ),
        )
        .unwrap();
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&fake_rg, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        crate::runtime_process::set_process_environment_var(
            "KLAUS_RIPGREP_PATH",
            fake_rg.to_str().unwrap(),
        );
        reset_ripgrep_command_for_test();
        crate::runtime_process::set_process_environment_var(
            "KLAUS_GLOB_TIMEOUT_SECONDS",
            DEADLINE.as_secs().to_string(),
        );

        let context = crate::runtime_process::test_runtime_process_context();
        let args = vec![r("--files")];
        let target_string = dir.to_string_lossy().into_owned();
        let target = RuntimePath::from(target_string.as_str());
        let signal = AbortSignal::new();
        let mut fut = std::pin::pin!(super::run_ripgrep(
            &context,
            &args,
            std::slice::from_ref(&target),
            &signal
        ));

        // Poll only until the child is provably running (which also means the
        // deadline is armed), then stop polling for the rest of the run.
        let spawn_seen = loop {
            if std::future::poll_fn(|cx| Poll::Ready(fut.as_mut().poll(cx)))
                .await
                .is_ready()
            {
                panic!("the run resolved before it could be starved — scenario not exercised");
            }
            if started_marker.exists() {
                break std::time::Instant::now();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };

        // Unpolled: wait for the child's own completion marker. This is what makes
        // "finished before the deadline was observed" a fact rather than a bet.
        let waiting_since = std::time::Instant::now();
        while !done_marker.exists() {
            assert!(
                waiting_since.elapsed() < Duration::from_secs(60),
                "the fake ripgrep never finished writing — the host is broken, not the code"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Still unpolled: cross the armed deadline, so the resumed poll observes it
        // as already expired and takes the timeout branch against a finished child.
        let resume_at = spawn_seen + DEADLINE + Duration::from_millis(500);
        tokio::time::sleep_until(tokio::time::Instant::from_std(resume_at)).await;
        let result = fut.await;

        crate::runtime_process::remove_process_environment_var("KLAUS_GLOB_TIMEOUT_SECONDS");
        crate::runtime_process::remove_process_environment_var("KLAUS_RIPGREP_PATH");
        reset_ripgrep_command_for_test();
        let _ = std::fs::remove_dir_all(&dir);

        let lines = result
            .unwrap()
            .into_iter()
            .map(|line| {
                String::from_utf8(line.to_node_utf8()).expect("Node UTF-8 test line is valid")
            })
            .collect::<Vec<_>>();
        assert_eq!(lines, vec!["one", "two"]);
    }

    #[tokio::test]
    async fn pre_aborted_run_reports_timeout_error() {
        let _g = env_guard().await;
        crate::runtime_process::remove_process_environment_var("KLAUS_RIPGREP_PATH");
        reset_ripgrep_command_for_test();
        if !rg_available() {
            return;
        }
        let dir = temp_dir();
        std::fs::write(dir.join("a.txt"), "hello\n").unwrap();
        let d = dir.to_string_lossy().into_owned();
        let signal = AbortSignal::new();
        signal.cancel();
        let err = run_ripgrep(&["--files".to_string()], &d, &signal)
            .await
            .unwrap_err();
        match err {
            RipgrepError::Timeout {
                message,
                partial_results,
            } => {
                assert!(message.contains("Search timed out after"));
                assert!(partial_results.is_empty());
            }
            other => panic!("expected Timeout, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
