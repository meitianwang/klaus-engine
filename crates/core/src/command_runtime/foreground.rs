//! Run a model-supplied shell command under the sandbox.
//!
//! The whole descendant tree is spawned into its own detached process group so
//! it can be reaped on abort/timeout (grandchildren included), output is capped
//! by a bounded head/tail aggregator so a runaway command cannot exhaust memory,
//! and both a deadline and a cancellation signal terminate the group with an
//! escalating SIGTERM-then-SIGKILL sequence.

use super::CommandSpawnRequest;
use crate::network_proxy::ManagedNetworkSandboxContext;
use crate::runtime_process::{
    RuntimeEnv, RuntimeProcessContext, RuntimeProcessSurface, RuntimeSignal, RuntimeSpawnRequest,
};
use crate::runtime_values::{NodeChunkUtf8, RuntimePath, RuntimeString};
use crate::types::abort::AbortSignal;
use crate::types::sandbox::SandboxBackend;
use std::collections::VecDeque;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::time::Instant;

/// Bounded output aggregator: keep the head and a rolling tail, collapsing the
/// middle. This caps memory regardless of how much a command prints.
struct BoundedOutput {
    head: Vec<Vec<u8>>,
    head_bytes: usize,
    tail: VecDeque<Vec<u8>>,
    tail_bytes: usize,
    elided: usize,
    head_limit: usize,
    tail_limit: usize,
    /// Newlines seen across the whole stream, including elided bytes.
    newlines_seen: usize,
    /// Whether the last pushed byte was a newline (no trailing partial line).
    ends_with_newline: bool,
    pushed_any: bool,
}

impl BoundedOutput {
    fn new(cap: usize) -> Self {
        let head_limit = ((cap as f64) * 0.6).floor() as usize;
        let tail_limit = cap - head_limit;
        BoundedOutput {
            head: Vec::new(),
            head_bytes: 0,
            tail: VecDeque::new(),
            tail_bytes: 0,
            elided: 0,
            head_limit,
            tail_limit,
            newlines_seen: 0,
            ends_with_newline: false,
            pushed_any: false,
        }
    }

    fn push(&mut self, input: &[u8]) {
        if !input.is_empty() {
            self.newlines_seen += input.iter().filter(|byte| **byte == b'\n').count();
            self.ends_with_newline = *input.last().expect("checked non-empty") == b'\n';
            self.pushed_any = true;
        }
        let mut buf: &[u8] = input;
        if self.head_bytes < self.head_limit {
            let room = self.head_limit - self.head_bytes;
            if buf.len() <= room {
                self.head.push(buf.to_vec());
                self.head_bytes += buf.len();
                return;
            }
            self.head.push(buf[..room].to_vec());
            self.head_bytes += room;
            buf = &buf[room..];
        }
        self.tail.push_back(buf.to_vec());
        self.tail_bytes += buf.len();
        while self.tail_bytes > self.tail_limit && !self.tail.is_empty() {
            // The deque is non-empty here, so a front always exists.
            let front_len = self.tail.front().map(Vec::len).unwrap_or(0);
            if self.tail_bytes >= front_len && self.tail.len() > 1 {
                self.tail.pop_front();
                self.tail_bytes -= front_len;
                self.elided += front_len;
            } else {
                // Trim within the single front buffer.
                let overflow = self.tail_bytes - self.tail_limit;
                if let Some(front) = self.tail.front_mut() {
                    *front = front[overflow..].to_vec();
                }
                self.tail_bytes -= overflow;
                self.elided += overflow;
            }
        }
    }

    fn elided_bytes(&self) -> usize {
        self.elided
    }

    /// Lines the process produced across the whole stream, with `str::lines`
    /// semantics (a trailing partial line counts as one). Stays correct when
    /// the middle was elided, unlike counting the rendered text's lines.
    fn total_lines_seen(&self) -> usize {
        if !self.pushed_any {
            return 0;
        }
        self.newlines_seen + usize::from(!self.ends_with_newline)
    }

    /// The aggregated text: head + tail verbatim, with an elision marker spliced
    /// in when the middle was dropped.
    fn render(&self) -> RuntimeString {
        let head: Vec<u8> = self.head.concat();
        let mut tail: Vec<u8> = Vec::new();
        for chunk in &self.tail {
            tail.extend_from_slice(chunk);
        }
        let head_s = NodeChunkUtf8::decode_chunk(&head);
        let tail_s = NodeChunkUtf8::decode_chunk(&tail);
        if self.elided == 0 {
            head_s.concat(&tail_s)
        } else {
            head_s
                .concat(&RuntimeString::from_utf8(&format!(
                    "\n… [{} bytes elided] …\n",
                    self.elided
                )))
                .concat(&tail_s)
        }
    }
}

/// Which pipe a streamed chunk arrived on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputStream {
    Stdout,
    Stderr,
}

/// One shell run request.
pub struct RunShellOptions<'a> {
    pub process_context: RuntimeProcessContext,
    pub command: RuntimeString,
    pub cwd: RuntimePath,
    pub sandbox: &'a dyn SandboxBackend,
    pub signal: AbortSignal,
    pub timeout_ms: u64,
    pub max_output_bytes: usize,
    pub env: Option<RuntimeEnv>,
    pub network_allowed: Option<bool>,
    pub managed_network: Option<ManagedNetworkSandboxContext>,
    pub writable_roots: Option<Vec<RuntimeString>>,
    /// Waive the sandbox's write confinement; see
    /// [`CommandSpawnRequest::unconfined_writes`].
    pub unconfined_writes: bool,
    /// Streamed progress callback, invoked per stdout/stderr chunk as it arrives.
    #[allow(clippy::type_complexity)]
    pub on_chunk: Option<
        Box<
            dyn Fn(OutputStream, &RuntimeString) -> crate::types::tool::BoxFuture<'static, ()>
                + Send
                + 'a,
        >,
    >,
}

/// The outcome of a shell run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunShellResult {
    pub output: RuntimeString,
    /// The process exit code, or `None` when it was terminated by a signal.
    pub exit_code: Option<i32>,
    /// The terminating signal name (e.g. `"SIGKILL"`), or `None` on a normal exit.
    pub signal: Option<RuntimeString>,
    pub timed_out: bool,
    pub aborted: bool,
    pub elided_bytes: usize,
    /// Lines counted across the whole stream, including bytes the capture
    /// buffer elided; `output` alone undercounts once elision happened.
    pub total_lines: usize,
}

/// The signal name the close event reports for a signal number (POSIX names).
#[cfg(unix)]
fn signal_name(sig: i32) -> RuntimeString {
    // An if-chain (rather than a match) so numerically-aliased signals on a given
    // platform never trip an unreachable-pattern error at compile time.
    if sig == libc::SIGHUP {
        return RuntimeString::from("SIGHUP");
    }
    if sig == libc::SIGINT {
        return RuntimeString::from("SIGINT");
    }
    if sig == libc::SIGQUIT {
        return RuntimeString::from("SIGQUIT");
    }
    if sig == libc::SIGILL {
        return RuntimeString::from("SIGILL");
    }
    if sig == libc::SIGTRAP {
        return RuntimeString::from("SIGTRAP");
    }
    if sig == libc::SIGABRT {
        return RuntimeString::from("SIGABRT");
    }
    if sig == libc::SIGBUS {
        return RuntimeString::from("SIGBUS");
    }
    if sig == libc::SIGFPE {
        return RuntimeString::from("SIGFPE");
    }
    if sig == libc::SIGKILL {
        return RuntimeString::from("SIGKILL");
    }
    if sig == libc::SIGUSR1 {
        return RuntimeString::from("SIGUSR1");
    }
    if sig == libc::SIGSEGV {
        return RuntimeString::from("SIGSEGV");
    }
    if sig == libc::SIGUSR2 {
        return RuntimeString::from("SIGUSR2");
    }
    if sig == libc::SIGPIPE {
        return RuntimeString::from("SIGPIPE");
    }
    if sig == libc::SIGALRM {
        return RuntimeString::from("SIGALRM");
    }
    if sig == libc::SIGTERM {
        return RuntimeString::from("SIGTERM");
    }
    if sig == libc::SIGCHLD {
        return RuntimeString::from("SIGCHLD");
    }
    if sig == libc::SIGCONT {
        return RuntimeString::from("SIGCONT");
    }
    if sig == libc::SIGSTOP {
        return RuntimeString::from("SIGSTOP");
    }
    if sig == libc::SIGTSTP {
        return RuntimeString::from("SIGTSTP");
    }
    if sig == libc::SIGTTIN {
        return RuntimeString::from("SIGTTIN");
    }
    if sig == libc::SIGTTOU {
        return RuntimeString::from("SIGTTOU");
    }
    if sig == libc::SIGURG {
        return RuntimeString::from("SIGURG");
    }
    if sig == libc::SIGXCPU {
        return RuntimeString::from("SIGXCPU");
    }
    if sig == libc::SIGXFSZ {
        return RuntimeString::from("SIGXFSZ");
    }
    if sig == libc::SIGVTALRM {
        return RuntimeString::from("SIGVTALRM");
    }
    if sig == libc::SIGPROF {
        return RuntimeString::from("SIGPROF");
    }
    if sig == libc::SIGWINCH {
        return RuntimeString::from("SIGWINCH");
    }
    if sig == libc::SIGSYS {
        return RuntimeString::from("SIGSYS");
    }
    RuntimeString::from_utf8(&format!("SIG{sig}"))
}

/// Run a shell command under the sandbox. Uses a detached process group so the
/// whole tree (including grandchildren) can be killed on abort/timeout.
pub async fn run_shell(opts: RunShellOptions<'_>) -> RunShellResult {
    let RunShellOptions {
        process_context,
        command,
        cwd,
        sandbox,
        signal,
        timeout_ms,
        max_output_bytes,
        env,
        network_allowed,
        managed_network,
        writable_roots,
        unconfined_writes,
        on_chunk,
    } = opts;

    let request = CommandSpawnRequest {
        command,
        cwd: cwd.clone(),
        env,
        writable_roots: writable_roots.unwrap_or_else(|| vec![cwd.as_runtime_string().clone()]),
        network_allowed: network_allowed.unwrap_or(false),
        managed_network,
        unconfined_writes,
    };
    let spec = request.to_spawn_spec(sandbox);

    let mut agg = BoundedOutput::new(max_output_bytes);
    let mut timed_out = false;
    let mut aborted = false;

    let spawner = process_context.spawner();
    let request =
        RuntimeSpawnRequest::from_spawn_spec(spec, RuntimeProcessSurface::ForegroundShell);
    let mut child = match spawner.spawn(request).await {
        Ok(child) => child,
        Err(e) => {
            if signal.is_cancelled() {
                aborted = true;
            }
            let message = RuntimeString::from("\n[spawn error] ")
                .concat(e.message())
                .concat(&RuntimeString::from("\n"));
            agg.push(&message.to_node_utf8());
            return RunShellResult {
                output: agg.render(),
                exit_code: None,
                signal: None,
                timed_out,
                aborted,
                elided_bytes: agg.elided_bytes(),
                total_lines: agg.total_lines_seen(),
            };
        }
    };
    let controller = child.controller();
    let mut out_pipe = child.take_stdout().expect("foreground stdout piped");
    let mut err_pipe = child.take_stderr().expect("foreground stderr piped");

    // Keep exactly one progress record in this operation owner. A full writer
    // pauses pipe reads while cancellation, escalation and reaping stay live.
    let mut progress: Option<crate::types::tool::BoxFuture<'static, ()>> = None;
    let mut out_done = false;
    let mut err_done = false;
    let mut exited = false;
    let mut final_status = None;

    let mut out_chunk = vec![0u8; 65536];
    let mut err_chunk = vec![0u8; 65536];

    let timeout_fut = tokio::time::sleep(Duration::from_millis(timeout_ms));
    tokio::pin!(timeout_fut);
    // The delayed hard-kill, armed 2s after the first SIGTERM; disabled until then.
    let sigkill_fut = tokio::time::sleep(Duration::from_secs(2));
    tokio::pin!(sigkill_fut);
    let mut sigkill_armed = false;
    let mut sigkill_scheduled = false;

    // Drain both pipes and reap the child. On the deadline or the abort signal,
    // SIGTERM the group and schedule a SIGKILL 2s later, but keep reading until the
    // process actually closes so buffered output (and a graceful exit) is captured.
    while !(out_done && err_done && exited && progress.is_none()) {
        tokio::select! {
            biased;
            _ = &mut timeout_fut, if !timed_out => {
                timed_out = true;
                controller.signal(RuntimeSignal::Terminate);
                if !sigkill_scheduled {
                    sigkill_fut.as_mut().reset(Instant::now() + Duration::from_secs(2));
                    sigkill_armed = true;
                    sigkill_scheduled = true;
                }
            }
            _ = signal.cancelled(), if !aborted => {
                aborted = true;
                controller.signal(RuntimeSignal::Terminate);
                if !sigkill_scheduled {
                    sigkill_fut.as_mut().reset(Instant::now() + Duration::from_secs(2));
                    sigkill_armed = true;
                    sigkill_scheduled = true;
                }
            }
            _ = &mut sigkill_fut, if sigkill_armed => {
                sigkill_armed = false;
                controller.signal(RuntimeSignal::Kill);
            }
            _ = async { progress.as_mut().expect("progress branch is enabled").await }, if progress.is_some() => {
                progress = None;
            }
            r = out_pipe.read(&mut out_chunk[..]), if !out_done && progress.is_none() => {
                match r {
                    Ok(0) | Err(_) => out_done = true,
                    Ok(n) => {
                        agg.push(&out_chunk[..n]);
                        if let Some(cb) = &on_chunk {
                            let text = NodeChunkUtf8::decode_chunk(&out_chunk[..n]);
                            progress = Some(cb(OutputStream::Stdout, &text));
                        }
                    }
                }
            }
            r = err_pipe.read(&mut err_chunk[..]), if !err_done && progress.is_none() => {
                match r {
                    Ok(0) | Err(_) => err_done = true,
                    Ok(n) => {
                        agg.push(&err_chunk[..n]);
                        if let Some(cb) = &on_chunk {
                            let text = NodeChunkUtf8::decode_chunk(&err_chunk[..n]);
                            progress = Some(cb(OutputStream::Stderr, &text));
                        }
                    }
                }
            }
            s = child.wait_terminal(), if !exited => {
                exited = true;
                final_status = s.ok();
            }
        }
    }

    let (exit_code, sig) = match final_status {
        Some(st) => {
            let signal = st.signal();
            #[cfg(unix)]
            let signal = signal.map(signal_name);
            #[cfg(not(unix))]
            let signal = signal.map(|sig| RuntimeString::from(format!("SIG{sig}")));
            (st.code(), signal)
        }
        // A failed reap is exotic; report a code-less, signal-less exit.
        None => (None, None),
    };

    child.disarm();
    RunShellResult {
        output: agg.render(),
        exit_code,
        signal: sig,
        timed_out,
        aborted,
        elided_bytes: agg.elided_bytes(),
        total_lines: agg.total_lines_seen(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // Everything below the pure `BoundedOutput` cases drives a real POSIX shell, so its
    // helpers and their imports are gated with it; on Windows they would be types and
    // functions with no user.
    #[cfg(unix)]
    use crate::runtime_process::ParentEnvSnapshot;
    #[cfg(unix)]
    use crate::types::sandbox::{ExecRequest, SandboxKind, SandboxPolicy, SpawnSpec};
    #[cfg(unix)]
    use std::path::PathBuf;
    #[cfg(unix)]
    use std::sync::{Arc, Mutex};

    #[cfg(unix)]
    type CapturedExecRequest = Arc<Mutex<Option<(ExecRequest, SandboxPolicy)>>>;

    // ---- BoundedOutput (pure) ----

    #[test]
    fn bounded_output_concatenates_when_within_cap() {
        let mut b = BoundedOutput::new(1000);
        b.push(b"hello");
        b.push(b" world");
        assert_eq!(b.render(), "hello world");
        assert_eq!(b.elided_bytes(), 0);
    }

    #[test]
    fn bounded_output_head_only_fill() {
        // head_limit = floor(10 * 0.6) = 6; both chunks fit in the head untouched.
        let mut b = BoundedOutput::new(10);
        b.push(b"abc");
        b.push(b"de");
        assert_eq!(b.render(), "abcde");
        assert_eq!(b.elided_bytes(), 0);
    }

    #[test]
    fn bounded_output_elides_middle_keeping_head_and_tail() {
        // cap 10 → head_limit 6, tail_limit 4.
        let mut b = BoundedOutput::new(10);
        b.push(b"0123456789ABCDE"); // 15 bytes
        assert_eq!(b.render(), "012345\n… [5 bytes elided] …\nBCDE");
        assert_eq!(b.elided_bytes(), 5);
    }

    #[test]
    fn bounded_output_accumulates_elision_across_pushes() {
        let mut b = BoundedOutput::new(10);
        b.push(b"012345678"); // fills head (6) + tail ("678")
        b.push(b"9ABCDEF"); // overflows the tail, dropping the older tail bytes
        assert_eq!(b.render(), "012345\n… [6 bytes elided] …\nCDEF");
        assert_eq!(b.elided_bytes(), 6);
    }

    #[test]
    fn bounded_output_counts_stream_lines_across_elision() {
        // cap 10 → head_limit 6, tail_limit 4.
        let mut b = BoundedOutput::new(10);
        assert_eq!(b.total_lines_seen(), 0);
        b.push(b"a\nb\n");
        assert_eq!(b.total_lines_seen(), 2);
        b.push(b"c");
        // A trailing partial line counts as one, matching `str::lines`.
        assert_eq!(b.total_lines_seen(), 3);
        b.push(b"ddddd");
        b.push(b"\n");
        assert!(b.elided_bytes() > 0);
        // "cddddd\n" completes the third line: the count spans the whole
        // stream even though the middle was elided from the retained bytes.
        assert_eq!(b.total_lines_seen(), 3);
    }

    // ---- test sandbox backends ----

    /// Passes the invocation through unchanged, recording the request + policy so
    /// tests can assert what `run_shell` handed the backend. Fills the environment
    /// from the request (or the current process env, so `bash` resolves on PATH).
    #[cfg(unix)]
    struct PassthroughSandbox {
        captured: CapturedExecRequest,
    }

    #[cfg(unix)]
    impl SandboxBackend for PassthroughSandbox {
        fn kind(&self) -> SandboxKind {
            SandboxKind::Process
        }
        fn is_available(&self) -> bool {
            true
        }
        fn transform(&self, req: &ExecRequest, policy: &SandboxPolicy) -> SpawnSpec {
            *self.captured.lock().unwrap() = Some((req.clone(), policy.clone()));
            let file = req.argv[0].clone();
            let args = req.argv[1..].to_vec();
            let env = req.env.clone().unwrap_or_else(ParentEnvSnapshot::capture);
            SpawnSpec {
                file,
                args,
                cwd: req.cwd.clone(),
                env,
            }
        }
    }

    /// Produces a spawn spec pointing at a nonexistent binary, to exercise the
    /// spawn-error path.
    #[cfg(unix)]
    struct FailingSandbox;

    #[cfg(unix)]
    impl SandboxBackend for FailingSandbox {
        fn kind(&self) -> SandboxKind {
            SandboxKind::Process
        }
        fn is_available(&self) -> bool {
            true
        }
        fn transform(&self, req: &ExecRequest, _policy: &SandboxPolicy) -> SpawnSpec {
            SpawnSpec {
                file: "/nonexistent/definitely-not-a-real-binary-xyz".into(),
                args: Vec::new(),
                cwd: req.cwd.clone(),
                env: ParentEnvSnapshot::capture(),
            }
        }
    }

    #[cfg(unix)]
    fn temp_dir() -> PathBuf {
        static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let mut dir = std::env::temp_dir();
        let unique = format!(
            "ae-exec-{}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        dir.push(unique);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[cfg(unix)]
    fn passthrough() -> (CapturedExecRequest, PassthroughSandbox) {
        let captured = Arc::new(Mutex::new(None));
        let sb = PassthroughSandbox {
            captured: captured.clone(),
        };
        (captured, sb)
    }

    #[cfg(unix)]
    fn opts<'a>(
        command: &str,
        cwd: &str,
        sandbox: &'a dyn SandboxBackend,
        signal: AbortSignal,
    ) -> RunShellOptions<'a> {
        RunShellOptions {
            process_context: crate::runtime_process::test_runtime_process_context(),
            command: command.into(),
            cwd: cwd.into(),
            sandbox,
            signal,
            timeout_ms: 30_000,
            max_output_bytes: 1_000_000,
            env: None,
            network_allowed: None,
            managed_network: None,
            writable_roots: None,
            unconfined_writes: false,
            on_chunk: None,
        }
    }

    // ---- signal name mapping ----

    #[cfg(unix)]
    #[test]
    fn signal_name_maps_common_signals() {
        assert_eq!(signal_name(libc::SIGTERM), "SIGTERM");
        assert_eq!(signal_name(libc::SIGKILL), "SIGKILL");
        assert_eq!(signal_name(libc::SIGSYS), "SIGSYS");
        // An unmapped number falls through to a generic name.
        assert_eq!(signal_name(9999), "SIG9999");
    }

    // ---- run_shell (Unix, needs bash) ----

    #[cfg(unix)]
    #[tokio::test]
    async fn runs_a_command_and_captures_stdout() {
        let dir = temp_dir();
        let d = dir.to_string_lossy().into_owned();
        let (_cap, sb) = passthrough();
        let res = run_shell(opts("echo hello", &d, &sb, AbortSignal::new())).await;
        assert_eq!(res.output, "hello\n");
        assert_eq!(res.exit_code, Some(0));
        assert_eq!(res.signal, None);
        assert!(!res.timed_out);
        assert!(!res.aborted);
        assert_eq!(res.elided_bytes, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn propagates_a_nonzero_exit_code() {
        let dir = temp_dir();
        let d = dir.to_string_lossy().into_owned();
        let (_cap, sb) = passthrough();
        let res = run_shell(opts("exit 3", &d, &sb, AbortSignal::new())).await;
        assert_eq!(res.exit_code, Some(3));
        assert!(!res.timed_out);
        assert!(!res.aborted);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn caps_output_and_reports_elided_bytes() {
        let dir = temp_dir();
        let d = dir.to_string_lossy().into_owned();
        let (_cap, sb) = passthrough();
        let mut o = opts("printf 'a%.0s' $(seq 1 5000)", &d, &sb, AbortSignal::new());
        o.max_output_bytes = 100; // head_limit 60, tail_limit 40
        let res = run_shell(o).await;
        assert_eq!(res.exit_code, Some(0));
        assert_eq!(res.elided_bytes, 4900);
        assert!(res.output.contains("[4900 bytes elided]"));
        // Head + tail retained, middle collapsed: 60 + 40 = 100 'a's total.
        assert_eq!(
            res.output
                .as_utf16()
                .iter()
                .filter(|&&unit| unit == u16::from(b'a'))
                .count(),
            100
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn times_out_a_long_running_command() {
        let dir = temp_dir();
        let d = dir.to_string_lossy().into_owned();
        let (_cap, sb) = passthrough();
        let mut o = opts("sleep 5", &d, &sb, AbortSignal::new());
        o.timeout_ms = 150;
        let res = run_shell(o).await;
        assert!(res.timed_out);
        assert!(!res.aborted);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn aborts_when_the_signal_is_already_cancelled() {
        let dir = temp_dir();
        let d = dir.to_string_lossy().into_owned();
        let (_cap, sb) = passthrough();
        let signal = AbortSignal::new();
        signal.cancel();
        let res = run_shell(opts("sleep 5", &d, &sb, signal)).await;
        assert!(res.aborted);
        assert!(!res.timed_out);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn streams_chunks_through_the_callback() {
        let dir = temp_dir();
        let d = dir.to_string_lossy().into_owned();
        let (_cap, sb) = passthrough();
        let seen: Arc<Mutex<RuntimeString>> = Arc::new(Mutex::new(RuntimeString::default()));
        let sink = seen.clone();
        let mut o = opts("echo streamed", &d, &sb, AbortSignal::new());
        o.on_chunk = Some(Box::new(
            move |stream: OutputStream, chunk: &RuntimeString| {
                if stream == OutputStream::Stdout {
                    let mut seen = sink.lock().unwrap();
                    *seen = seen.concat(chunk);
                }
                Box::pin(async {})
            },
        ));
        let res = run_shell(o).await;
        assert_eq!(res.output, "streamed\n");
        assert!(seen.lock().unwrap().contains("streamed"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn replaces_the_environment_from_the_options() {
        let dir = temp_dir();
        let d = dir.to_string_lossy().into_owned();
        let (_cap, sb) = passthrough();
        let mut env = RuntimeEnv::new();
        // PATH is required for `bash` to resolve once the parent env is scrubbed.
        env.insert(
            "PATH".into(),
            std::env::var("PATH").unwrap_or_default().into(),
        );
        env.insert("FOO".into(), "bar".into());
        let mut o = opts("printf '%s' \"$FOO\"", &d, &sb, AbortSignal::new());
        o.env = Some(env);
        let res = run_shell(o).await;
        assert_eq!(res.output, "bar");
        assert_eq!(res.exit_code, Some(0));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn reports_a_spawn_error() {
        let dir = temp_dir();
        let d = dir.to_string_lossy().into_owned();
        let sb = FailingSandbox;
        let res = run_shell(opts("whatever", &d, &sb, AbortSignal::new())).await;
        assert!(res.output.contains("[spawn error]"));
        assert_eq!(res.exit_code, None);
        assert_eq!(res.signal, None);
        assert!(!res.timed_out);
        assert!(!res.aborted);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn defaults_the_policy_writable_root_to_cwd_and_no_network() {
        let dir = temp_dir();
        let d = dir.to_string_lossy().into_owned();
        let (captured, sb) = passthrough();
        let _ = run_shell(opts("true", &d, &sb, AbortSignal::new())).await;
        let (req, policy) = captured.lock().unwrap().clone().unwrap();
        assert_eq!(policy.writable_roots, vec![RuntimeString::from(d.clone())]);
        assert!(!policy.network_allowed);
        // The invocation is the platform shell wrapping the command.
        assert_eq!(
            req.argv,
            vec![
                RuntimeString::from("bash"),
                RuntimeString::from("-c"),
                RuntimeString::from("true"),
            ]
        );
        assert_eq!(req.cwd, RuntimePath::from(d));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn forwards_provided_writable_roots_and_network_flag() {
        let dir = temp_dir();
        let d = dir.to_string_lossy().into_owned();
        let (captured, sb) = passthrough();
        let mut o = opts("true", &d, &sb, AbortSignal::new());
        o.writable_roots = Some(vec!["/x".into(), "/y".into()]);
        o.network_allowed = Some(true);
        let _ = run_shell(o).await;
        let (_req, policy) = captured.lock().unwrap().clone().unwrap();
        assert_eq!(
            policy.writable_roots,
            vec![RuntimeString::from("/x"), RuntimeString::from("/y")]
        );
        assert!(policy.network_allowed);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
