//! Capture the user's INTERACTIVE login-shell environment. A bare `bash -c
//! <command>` (how the bash tool runs) sources NO rc file, so it misses the
//! `PATH` and exported variables the user's `.zshrc` / `.bashrc` set up (a
//! version manager's shims, Homebrew paths, proxy vars). This runs the user's
//! shell in login mode with the rc sourced, dumps the resulting environment,
//! and returns it so the bash tool can inject it.
//!
//! Faithful capture: login shell, the interactive rc explicitly sourced (login
//! mode alone sources the PROFILE, not the rc), `PWD`/`OLDPWD` and other
//! per-invocation noise excluded, a 10s timeout, and graceful degradation — any
//! failure (missing/hanging shell, rc error, timeout, Windows) returns `None`
//! and the caller falls back to the bare environment.
//!
//! SECURITY: the engine does NOT inject the user's WHOLE interactive env.
//! Capturing everything would surface rc-only secrets whose NAME does not match
//! a name-only secret heuristic (a `DATABASE_URL` with an inline password, a
//! `REDIS_URL`, a `*_DSN`) — readable by every model-run command, a worse
//! surface than the bare environment the bash tool already exposes. Instead it
//! injects only PATH-style / directory variables — which are structurally
//! directory lists, never credentials — plus a few safe locale scalars, and
//! refuses the dynamic-loader / rc-injection vars (`LD_*` / `DYLD_*` /
//! `BASH_ENV`) outright. The process scrubber's sensitive-name decision runs on
//! top as defense-in-depth, without converting the ordered exact environment
//! through a scalar map.

use crate::runtime_process::{
    RuntimeCompletedProcessRequest, RuntimeCompletedProcessSurface, RuntimeEnv,
    RuntimeProcessContext,
};
use crate::runtime_values::{
    NodeBufferUtf8, RuntimeDecode, RuntimeJson, RuntimeObject, RuntimePath, RuntimeString,
};
use regex::Regex;
use std::sync::LazyLock;
use std::time::Duration;

const CAPTURE_TIMEOUT_MS: u64 = 10_000;
const MAX_OUTPUT_BYTES: usize = 4 * 1024 * 1024;

/// A syntactically valid POSIX shell variable name (the capture filter).
static VALID_NAME: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*$").unwrap());

/// Names ending in a path/dir-style suffix — directory lists, never credentials.
static PATH_LIKE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(PATH|HOME|ROOT|DIR|PREFIX|INSTALL|BIN)$").unwrap());

/// The name-only secret heuristic used by the process sandbox. Capture keeps
/// the same defense-in-depth decision without converting the ordered exact
/// environment through a `HashMap` compatibility layer.
static SENSITIVE_NAME: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(KEY|TOKEN|SECRET|PASSWORD|PASSWD|CREDENTIAL|AUTH|SESSION|COOKIE)")
        .expect("sensitive env-var regex is valid")
});

/// Safe non-path scalars worth recovering from the rc (the interactive locale).
const SAFE_SCALARS: [&str; 5] = ["LANG", "LC_ALL", "LC_CTYPE", "LANGUAGE", "TZ"];

/// Dynamic-loader / rc-injection vars — refused even when path-shaped
/// (code-exec vectors).
const REFUSED_NAMES: [&str; 5] = ["BASH_ENV", "ENV", "IFS", "PROMPT_COMMAND", "GLOBIGNORE"];

/// Whether a captured variable is safe to inject. Only PATH-style directory
/// variables (and a few locale scalars) cross over — never arbitrary rc exports
/// (which could be credentials) and never the dynamic-loader / rc-hook vars
/// (which are code-execution vectors). The process scrubber's sensitive-name
/// rule is still applied afterwards as a secondary guard.
fn is_injectable_key(key: &str) -> bool {
    if REFUSED_NAMES.contains(&key) || key.starts_with("LD_") || key.starts_with("DYLD_") {
        return false;
    }
    SAFE_SCALARS.contains(&key) || PATH_LIKE.is_match(key)
}

fn survives_process_scrub(key: &str) -> bool {
    // These are the injectable members of the sandbox scrubber's explicit
    // allowlist. Everything else follows its sensitive-name heuristic.
    matches!(key, "PATH" | "HOME" | "LANG" | "LC_ALL" | "LC_CTYPE" | "TZ")
        || !SENSITIVE_NAME.is_match(key)
}

#[derive(Debug, Clone)]
pub struct CaptureShellEnvOptions {
    /// The shell to run; defaults to `$SHELL` (or `/bin/sh`).
    pub shell: Option<String>,
    /// Working directory for the capture (a per-directory rc reads this).
    pub cwd: RuntimePath,
    /// Capture timeout; defaults to 10s.
    pub timeout_ms: Option<u64>,
}

/// Build the per-shell capture script: source the interactive rc (tolerating a
/// missing or erroring rc — `2>/dev/null` and `;` so `env` still runs), then
/// dump the environment. Login mode sources the profile, not the rc, so the rc
/// is sourced explicitly here.
fn build_capture_script(shell: &str) -> String {
    // Dump NUL-delimited (`env -0`) so a variable whose VALUE contains a newline
    // cannot be misread, by a line-based parse, as a separate spoofed variable
    // (e.g. a crafted value injecting `\nLD_PRELOAD=...`). Fall back to newline
    // `env` where -0 is unsupported.
    let dump = "env -0 2>/dev/null || env";
    if shell.contains("zsh") {
        return format!(
            "[ -r \"${{ZDOTDIR:-$HOME}}/.zshrc\" ] && . \"${{ZDOTDIR:-$HOME}}/.zshrc\" 2>/dev/null; {dump}"
        );
    }
    if shell.contains("bash") {
        return format!("[ -r \"$HOME/.bashrc\" ] && . \"$HOME/.bashrc\" 2>/dev/null; {dump}");
    }
    // POSIX sh and unknown shells: `$ENV` is the interactive-rc hook.
    format!("[ -n \"$ENV\" ] && [ -r \"$ENV\" ] && . \"$ENV\" 2>/dev/null; {dump}")
}

/// Run the capture shell and return its stdout, or `None` on any failure
/// (spawn error, timeout, non-zero exit, or output over the buffer cap). The rc
/// errors themselves are non-fatal (`; env` still ran) so a clean stdout still
/// resolves.
async fn run_capture(
    process_context: &RuntimeProcessContext,
    shell: &str,
    script: &str,
    cwd: &RuntimePath,
    timeout_ms: u64,
) -> Option<Vec<u8>> {
    let request = RuntimeCompletedProcessRequest::new(
        RuntimeCompletedProcessSurface::ShellEnvSnapshot,
        RuntimeString::from(shell),
    )
    .with_args(vec![
        RuntimeString::from("-lc"),
        RuntimeString::from(script),
    ])
    .with_cwd(cwd.clone())
    .with_timeout(Duration::from_millis(timeout_ms))
    .with_max_stdout_bytes(MAX_OUTPUT_BYTES);
    let output = process_context.exec_completed(request).await.ok()?;
    (output.status.success() && output.stdout.len() <= MAX_OUTPUT_BYTES).then_some(output.stdout)
}

/// Parse the env dump into an injectable map. Records are NUL-delimited when the
/// shell's `env -0` is available (so a value's embedded newline cannot spoof a
/// second variable), falling back to newline-split. Only PATH-style / locale
/// variables survive ([`is_injectable_key`]); the process scrubber's matching
/// name-only rule then strips any secret-named straggler.
fn parse_env(raw: &[u8]) -> RuntimeEnv {
    let mut parsed = RuntimeObject::new();
    // env -0 separates records with NUL; if none is present we got newline `env`.
    let delimiter = if raw.contains(&0) { 0 } else { b'\n' };
    for rec in raw.split(|byte| *byte == delimiter) {
        if rec.is_empty() {
            continue;
        }
        // Skip a record with no `=` or one that starts with `=` (empty name).
        let eq = match rec.iter().position(|byte| *byte == b'=') {
            Some(0) | None => continue,
            Some(i) => i,
        };
        // POSIX variable names admitted by VALID_NAME are ASCII, so a failed
        // scalar decode is necessarily an invalid name rather than a value
        // projection decision.
        let Ok(key) = std::str::from_utf8(&rec[..eq]) else {
            continue;
        };
        if !VALID_NAME.is_match(key) || !is_injectable_key(key) || !survives_process_scrub(key) {
            continue;
        }
        parsed.insert(
            key,
            RuntimeJson::String(NodeBufferUtf8::decode(&rec[eq + 1..])),
        );
    }
    RuntimeEnv::decode_runtime(&RuntimeJson::Object(parsed))
        .expect("capture parser emits a string-only exact environment")
}

/// Capture the interactive login-shell environment, or `None` on any failure /
/// on Windows. The result is already secret-scrubbed and noise-free, ready to
/// inject as the bash tool's extra environment.
pub async fn capture_login_shell_env(
    process_context: &RuntimeProcessContext,
    opts: &CaptureShellEnvOptions,
) -> Option<RuntimeEnv> {
    if cfg!(windows) {
        return None;
    }
    let shell = opts
        .shell
        .clone()
        .unwrap_or_else(|| std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_string()));
    let raw = run_capture(
        process_context,
        &shell,
        &build_capture_script(&shell),
        &opts.cwd,
        opts.timeout_ms.unwrap_or(CAPTURE_TIMEOUT_MS),
    )
    .await?;
    let env = parse_env(&raw);
    // An empty capture (no usable vars) is treated as "nothing to inject".
    if env.is_empty() {
        None
    } else {
        Some(env)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::sync::Mutex;

    // Serializes the tests that must set the process `HOME` (the capture shell
    // inherits it) so parallel tests do not clobber each other's value.
    static ENV_LOCK: Mutex<()> = Mutex::const_new(());

    fn temp_dir() -> PathBuf {
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let mut dir = std::env::temp_dir();
        let unique = format!(
            "ae-shellenv-{}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        dir.push(unique);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn has_bash() -> bool {
        !cfg!(windows) && Path::new("/bin/bash").exists()
    }

    /// Run a capture with `HOME` pointed at `dir` for the duration of the child,
    /// restoring it (and releasing the lock) before the caller asserts.
    async fn capture_with_home(dir: &Path, opts: CaptureShellEnvOptions) -> Option<RuntimeEnv> {
        let _guard = ENV_LOCK.lock().await;
        let prev = std::env::var_os("HOME");
        crate::runtime_process::set_process_environment_var("HOME", dir);
        let r = capture_login_shell_env(
            &crate::runtime_process::test_runtime_process_context(),
            &opts,
        )
        .await;
        match prev {
            Some(h) => crate::runtime_process::set_process_environment_var("HOME", h),
            None => crate::runtime_process::remove_process_environment_var("HOME"),
        }
        r
    }

    fn opts(dir: &Path, shell: &str, timeout_ms: Option<u64>) -> CaptureShellEnvOptions {
        CaptureShellEnvOptions {
            shell: Some(shell.to_string()),
            cwd: test_runtime_path(dir),
            timeout_ms,
        }
    }

    #[cfg(unix)]
    fn test_runtime_path(path: &Path) -> RuntimePath {
        use std::os::unix::ffi::OsStrExt;

        RuntimePath::from(NodeBufferUtf8::decode(path.as_os_str().as_bytes()))
    }

    #[cfg(windows)]
    fn test_runtime_path(path: &Path) -> RuntimePath {
        use std::os::windows::ffi::OsStrExt;

        RuntimePath::from(crate::runtime_values::RuntimeString::from_utf16(
            path.as_os_str().encode_wide().collect::<Vec<_>>(),
        ))
    }

    #[cfg(not(any(unix, windows)))]
    fn test_runtime_path(path: &Path) -> RuntimePath {
        RuntimePath::from(
            path.to_str()
                .expect("test temporary paths must be Unicode scalar strings"),
        )
    }

    fn env_get<'a>(
        env: &'a RuntimeEnv,
        key: &str,
    ) -> Option<&'a crate::runtime_values::RuntimeString> {
        env.get(&crate::runtime_values::RuntimeString::from(key))
    }

    fn env_contains(env: &RuntimeEnv, key: &str) -> bool {
        env.contains_key(&crate::runtime_values::RuntimeString::from(key))
    }

    #[tokio::test]
    async fn injects_only_path_style_vars_drops_rc_exports_secrets_and_pwd() {
        if !has_bash() {
            return;
        }
        let dir = temp_dir();
        std::fs::write(
            dir.join(".bashrc"),
            // path-like → injected; DATABASE_URL/RANDOM_CONFIG not path-like →
            // NOT injected (security); MY_API_TOKEN secret-named → scrubbed.
            "export MY_TOOL_HOME=/opt/tool\n\
             export GOPATH=/home/me/go\n\
             export DATABASE_URL=postgres://admin:hunter2@db/prod\n\
             export RANDOM_CONFIG=xyz\n\
             export MY_API_TOKEN=should_be_dropped\n",
        )
        .unwrap();
        let env = capture_with_home(&dir, opts(&dir, "/bin/bash", None)).await;
        assert!(env.is_some());
        let env = env.unwrap();
        // PATH always present (path-like).
        assert!(env_get(&env, "PATH")
            .map(|p| !p.is_empty())
            .unwrap_or(false));
        assert_eq!(env_get(&env, "MY_TOOL_HOME"), Some(&"/opt/tool".into()));
        assert_eq!(env_get(&env, "GOPATH"), Some(&"/home/me/go".into()));
        // SECURITY: an rc secret with a non-obvious name must NOT cross over.
        assert!(!env_contains(&env, "DATABASE_URL"));
        // Arbitrary non-path var excluded.
        assert!(!env_contains(&env, "RANDOM_CONFIG"));
        // Secret scrubbed.
        assert!(!env_contains(&env, "MY_API_TOKEN"));
        // Not path-like → excluded.
        assert!(!env_contains(&env, "PWD"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_multi_line_value_cannot_spoof_a_separate_injectable_var() {
        if !has_bash() {
            return;
        }
        let dir = temp_dir();
        // A crafted value embedding a newline + `LD_PRELOAD=...` must not
        // materialize a real LD_PRELOAD var (a code-execution vector).
        std::fs::write(
            dir.join(".bashrc"),
            "export EVIL_HOME=$'safe\\nLD_PRELOAD=/tmp/evil.so'\n",
        )
        .unwrap();
        let env = capture_with_home(&dir, opts(&dir, "/bin/bash", None)).await;
        // No spoofed loader var.
        assert!(!env
            .as_ref()
            .is_some_and(|env| env_contains(env, "LD_PRELOAD")));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn survives_an_rc_that_errors() {
        if !has_bash() {
            return;
        }
        let dir = temp_dir();
        std::fs::write(
            dir.join(".bashrc"),
            "this-command-does-not-exist 2>/dev/null\nexport OK_DIR=/x\n",
        )
        .unwrap();
        let env = capture_with_home(&dir, opts(&dir, "/bin/bash", None)).await;
        assert_eq!(
            env.as_ref().and_then(|env| env_get(env, "OK_DIR")),
            Some(&"/x".into())
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn returns_null_for_a_nonexistent_shell() {
        let dir = temp_dir();
        let env = capture_login_shell_env(
            &crate::runtime_process::test_runtime_process_context(),
            &opts(&dir, "/nonexistent/shell-xyz", Some(2000)),
        )
        .await;
        assert!(env.is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn returns_null_on_windows() {
        let dir = temp_dir();
        let env = capture_login_shell_env(
            &crate::runtime_process::test_runtime_process_context(),
            &opts(&dir, "pwsh", None),
        )
        .await;
        assert!(env.is_none());
        std::fs::remove_dir_all(&dir).ok();
    }
}
