//! Cross-platform shell invocation and process-tree termination.
//!
//! The engine runs a model-supplied shell command. On macOS/Linux that is
//! `bash -c <command>`; on Windows there is no bash, so the command runs through
//! PowerShell (preferred) or cmd:
//!
//!  - Windows shell preference: pwsh (PowerShell Core 7+) -> powershell (Windows
//!    PowerShell 5.1) -> cmd.exe. Overridable via `KLAUS_SHELL`.
//!  - PowerShell flags: `-NoProfile -NonInteractive -Command <command>` with the
//!    command passed as a SINGLE argv element (PowerShell parses it; no manual
//!    quoting). A UTF-8 console-encoding line is prepended so non-ASCII output is
//!    not mangled.
//!  - cmd fallback: `/c <command>`.
//!  - Tree kill: Windows has no POSIX process groups, so the whole tree is reaped
//!    with `taskkill /PID <pid> /T /F` (signal ignored). On Unix the detached
//!    process group is signalled (`kill(-pid)`), falling back to the child.
//!
//! These are all standard, documented OS idioms.

use crate::context::env_info::{current_platform, Platform};
use crate::runtime_process::{RuntimeProcessController, RuntimeSignal};
use std::collections::HashMap;
use std::sync::Mutex;

/// A snapshot of environment variables (name -> value). Mirrors the shape used
/// by the OS environment; a missing key is simply an absent entry.
pub type Env = HashMap<String, String>;

/// Force UTF-8 console output (idempotent, best-effort). PowerShell 7+ (pwsh)
/// already defaults to UTF-8, but Windows PowerShell 5.1 emits in the legacy OEM
/// code page, so non-ASCII (e.g. CJK) output would be mojibake when decoded as
/// UTF-8. Setting `[Console]::OutputEncoding` makes PowerShell *emit* UTF-8 on
/// both editions. This single knob also governs how PowerShell decodes a native
/// command's stdout, so UTF-8-emitting tools come through correctly.
const POWERSHELL_UTF8_PREFIX: &str =
    "try { [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 } catch {}\n";

/// PowerShell does NOT propagate a native command's non-zero exit code as the
/// process exit code unless the script explicitly `exit`s with it — so
/// `powershell -Command \"some-failing.exe\"` would otherwise exit 0. Append an
/// exit-code resolver that prefers the last native exit code, else maps the
/// success flag `$?` to 0/1, then exits with it (works around the 5.1 quirk where
/// a cmdlet error leaves `$LASTEXITCODE` stale).
const POWERSHELL_EXITCODE_SUFFIX: &str =
    "\n$__ae_ec = if ($null -ne $LASTEXITCODE) { $LASTEXITCODE } elseif ($?) { 0 } else { 1 }\nexit $__ae_ec";

/// Well-known absolute fallbacks when a shell is not found on PATH.
fn well_known(bin: &str) -> &'static [&'static str] {
    match bin {
        // Git for Windows ships a full MSYS bash with coreutils (ls/grep/sed/cat/…),
        // so the agent's POSIX commands run natively.
        "bash.exe" => &[
            "C:\\Program Files\\Git\\bin\\bash.exe",
            "C:\\Program Files\\Git\\usr\\bin\\bash.exe",
            "C:\\Program Files (x86)\\Git\\bin\\bash.exe",
        ],
        "pwsh.exe" => &["C:\\Program Files\\PowerShell\\7\\pwsh.exe"],
        "powershell.exe" => &["C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\powershell.exe"],
        _ => &[],
    }
}

/// The classified kind of the resolved shell executable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShellKind {
    Bash,
    Pwsh,
    Powershell,
    Cmd,
}

/// A resolved shell: the executable to run plus its classified kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShellResolution {
    /// Executable path (or bare name to be resolved by the OS at spawn time).
    pub path: String,
    pub kind: ShellKind,
}

/// Classify an override shell path by its basename.
fn classify_shell(p: &str) -> ShellKind {
    let lower = p.to_lowercase();
    let normalized = lower.replace('\\', "/");
    let base = normalized.rsplit('/').next().unwrap_or(lower.as_str());
    if base.starts_with("pwsh") {
        ShellKind::Pwsh
    } else if base.starts_with("powershell") {
        ShellKind::Powershell
    } else if base.starts_with("cmd") {
        ShellKind::Cmd
    } else {
        ShellKind::Bash
    }
}

/// A snapshot of the current process environment.
pub fn process_env() -> Env {
    std::env::vars().collect()
}

/// Locate a real bash (git-bash / MSYS / WSL) on Windows, or `None`.
pub fn find_windows_bash(env: &Env) -> Option<String> {
    find_windows_bash_with(env, bin_exists)
}

/// [`find_windows_bash`] with the presence probe supplied; see [`BinExists`].
pub(crate) fn find_windows_bash_with(env: &Env, exists: BinExists) -> Option<String> {
    which_windows(exists, "bash.exe", env)
}

/// Whether a candidate executable is present.
///
/// A seam, because the well-known fallbacks below are real absolute paths: on any Windows
/// box with Git for Windows installed, "nothing is installed anywhere" is unreachable
/// through the environment alone, and a test that only empties `Path` still resolves a
/// bash. Production always passes [`bin_exists`].
pub(crate) type BinExists = fn(&std::path::Path) -> bool;

pub(crate) fn bin_exists(path: &std::path::Path) -> bool {
    path.exists()
}

/// Locate `bin` on the Windows PATH (× known fallbacks). Pure, synchronous.
fn which_windows(exists: BinExists, bin: &str, env: &Env) -> Option<String> {
    let path_var = env
        .get("Path")
        .or_else(|| env.get("PATH"))
        .map(String::as_str)
        .unwrap_or("");
    for dir in path_var.split(';') {
        if dir.is_empty() {
            continue;
        }
        let full = std::path::Path::new(dir).join(bin);
        if exists(&full) {
            return Some(full.to_string_lossy().into_owned());
        }
    }
    for wk in well_known(bin) {
        if exists(std::path::Path::new(wk)) {
            return Some((*wk).to_string());
        }
    }
    None
}

/// Memoized Windows-shell resolution (first call wins, until reset).
static CACHED_WINDOWS_SHELL: Mutex<Option<ShellResolution>> = Mutex::new(None);

/// Resolve the Windows shell. Order matters for FIDELITY: the model writes POSIX
/// shell (bash) for the `bash` tool, and bash/PowerShell are not interchangeable
/// (`&&`, `ls -la`, `grep`, `$(...)`, `export`, text-vs-object pipes all differ).
/// So REAL bash is preferred — Git for Windows / WSL bash run the agent's
/// commands verbatim with full coreutils. Only when no bash is installed does it
/// fall back to PowerShell -> cmd, a DEGRADED mode where bash-specific syntax may
/// fail. A `KLAUS_SHELL` override wins over all of it. Memoized.
pub fn resolve_windows_shell(env: &Env) -> ShellResolution {
    let mut guard = CACHED_WINDOWS_SHELL
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    if let Some(cached) = guard.as_ref() {
        return cached.clone();
    }
    let resolved = compute_windows_shell(bin_exists, env);
    *guard = Some(resolved.clone());
    resolved
}

/// The uncached resolution body.
fn compute_windows_shell(exists: BinExists, env: &Env) -> ShellResolution {
    if let Some(override_path) = env.get("KLAUS_SHELL") {
        if !override_path.is_empty() {
            return ShellResolution {
                path: override_path.clone(),
                kind: classify_shell(override_path),
            };
        }
    }
    // 1) Real bash (git-bash / MSYS / WSL on PATH) — full POSIX fidelity.
    if let Some(bash) = which_windows(exists, "bash.exe", env) {
        return ShellResolution {
            path: bash,
            kind: ShellKind::Bash,
        };
    }
    // 2) PowerShell (degraded for bash syntax), preferring PS 7 (pwsh).
    if let Some(pwsh) = which_windows(exists, "pwsh.exe", env) {
        return ShellResolution {
            path: pwsh,
            kind: ShellKind::Pwsh,
        };
    }
    if let Some(powershell) = which_windows(exists, "powershell.exe", env) {
        return ShellResolution {
            path: powershell,
            kind: ShellKind::Powershell,
        };
    }
    // 3) Last resort: cmd.exe (always present). %ComSpec% points at it.
    let path = env
        .get("ComSpec")
        .cloned()
        .unwrap_or_else(|| "cmd.exe".to_string());
    ShellResolution {
        path,
        kind: ShellKind::Cmd,
    }
}

/// Reset the memoized Windows shell (tests).
pub fn reset_shell_cache() {
    *CACHED_WINDOWS_SHELL
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = None;
}

/// Options for [`build_shell_invocation`].
#[derive(Default)]
pub struct BuildShellOptions {
    /// Defaults to the current process platform.
    pub platform: Option<Platform>,
    /// Override the Windows shell resolver (tests / custom shells).
    pub resolve_shell: Option<Box<dyn Fn() -> ShellResolution>>,
}

/// Build the argv that runs `command` through the platform shell. The result is
/// the INNER command handed to the sandbox layer (which wraps it on Unix via
/// sandbox-exec/bwrap, or passes it through unchanged on Windows).
pub fn build_shell_invocation(command: &str, opts: BuildShellOptions) -> Vec<String> {
    let platform = opts.platform.unwrap_or_else(current_platform);
    if platform != Platform::Win32 {
        return vec!["bash".to_string(), "-c".to_string(), command.to_string()];
    }
    let shell = match &opts.resolve_shell {
        Some(resolve) => resolve(),
        None => resolve_windows_shell(&process_env()),
    };
    match shell.kind {
        // Real bash (git-bash/WSL): the agent's POSIX command runs verbatim.
        ShellKind::Bash => vec![shell.path, "-c".to_string(), command.to_string()],
        ShellKind::Cmd => vec![shell.path, "/c".to_string(), command.to_string()],
        // PowerShell: command is a single argv element so PS parses it (no manual
        // quoting). Prepend the UTF-8 console line so output isn't mojibake, and
        // append the exit-code resolver so a failing command's code propagates.
        ShellKind::Pwsh | ShellKind::Powershell => {
            let script = format!("{POWERSHELL_UTF8_PREFIX}{command}{POWERSHELL_EXITCODE_SUFFIX}");
            vec![
                shell.path,
                "-NoProfile".to_string(),
                "-NonInteractive".to_string(),
                "-Command".to_string(),
                script,
            ]
        }
    }
}

/// True when `detached` (POSIX process-group) spawning applies. `None` means the
/// current process platform.
pub fn use_detached_process_group(platform: Option<Platform>) -> bool {
    let platform = platform.unwrap_or_else(current_platform);
    platform != Platform::Win32
}

/// A process signal to deliver to a tree. Ignored on Windows (the whole tree is
/// reaped with `taskkill` regardless of signal).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    Hup,
    Int,
    Quit,
    Kill,
    Usr1,
    Usr2,
    Term,
}

impl Signal {
    /// Convert the legacy shell signal vocabulary into the unified runtime
    /// controller vocabulary. Windows intentionally ignores the requested
    /// signal and always uses the pinned hard tree-control action.
    fn runtime_signal(self) -> RuntimeSignal {
        #[cfg(windows)]
        {
            let _ = self;
            RuntimeSignal::Kill
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        match self {
            Signal::Hup => RuntimeSignal::Hangup,
            Signal::Int => RuntimeSignal::Interrupt,
            Signal::Quit => RuntimeSignal::Quit,
            Signal::Kill => RuntimeSignal::Kill,
            Signal::Usr1 => RuntimeSignal::User1,
            Signal::Usr2 => RuntimeSignal::User2,
            Signal::Term => RuntimeSignal::Terminate,
        }
    }
}

/// Terminate a runtime-owned child and its descendants through its committed
/// controller identity. The controller is created only by `RuntimeSpawner`, so
/// the Windows helper retains the target handle and Unix never signals a stale
/// reused PID.
#[allow(dead_code)]
pub(crate) fn kill_process_tree(controller: &RuntimeProcessController, signal: Signal) {
    controller.signal(signal.runtime_signal());
}

/// Terminate a detached process group after the wait owner has moved the child
/// facade into an async waiter. The owner retains this controller clone, whose
/// target identity remains generation-bound.
#[allow(dead_code)]
pub(crate) fn kill_process_group(controller: &RuntimeProcessController, signal: Signal) {
    controller.signal(signal.runtime_signal());
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serializes the tests that touch the shared shell cache (Rust runs tests in
    /// parallel, whereas the memoization is process-global).
    static TEST_GUARD: Mutex<()> = Mutex::new(());

    fn env(pairs: &[(&str, &str)]) -> Env {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    fn sv(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| (*s).to_string()).collect()
    }

    fn bash_shell() -> ShellResolution {
        ShellResolution {
            path: "C:\\Program Files\\Git\\bin\\bash.exe".to_string(),
            kind: ShellKind::Bash,
        }
    }
    fn pwsh_shell() -> ShellResolution {
        ShellResolution {
            path: "C:\\pwsh.exe".to_string(),
            kind: ShellKind::Pwsh,
        }
    }
    fn cmd_shell() -> ShellResolution {
        ShellResolution {
            path: "C:\\Windows\\System32\\cmd.exe".to_string(),
            kind: ShellKind::Cmd,
        }
    }

    // ---- build_shell_invocation ----

    #[test]
    fn uses_bash_c_on_unix() {
        assert_eq!(
            build_shell_invocation(
                "echo hi",
                BuildShellOptions {
                    platform: Some(Platform::Linux),
                    ..Default::default()
                }
            ),
            sv(&["bash", "-c", "echo hi"])
        );
        assert_eq!(
            build_shell_invocation(
                "echo hi",
                BuildShellOptions {
                    platform: Some(Platform::Darwin),
                    ..Default::default()
                }
            ),
            sv(&["bash", "-c", "echo hi"])
        );
    }

    #[test]
    fn runs_command_through_real_bash_on_windows_when_available() {
        // git-bash present → the agent's `&&`/`grep`/`$(...)` command runs verbatim.
        let argv = build_shell_invocation(
            "grep -r foo . && echo $(date)",
            BuildShellOptions {
                platform: Some(Platform::Win32),
                resolve_shell: Some(Box::new(bash_shell)),
            },
        );
        assert_eq!(
            argv,
            sv(&[
                "C:\\Program Files\\Git\\bin\\bash.exe",
                "-c",
                "grep -r foo . && echo $(date)",
            ])
        );
    }

    #[test]
    fn uses_powershell_command_with_single_argv_element() {
        let argv = build_shell_invocation(
            "Get-ChildItem",
            BuildShellOptions {
                platform: Some(Platform::Win32),
                resolve_shell: Some(Box::new(pwsh_shell)),
            },
        );
        assert_eq!(argv[0], "C:\\pwsh.exe");
        assert_eq!(
            &argv[1..4],
            &sv(&["-NoProfile", "-NonInteractive", "-Command"])[..]
        );
        // The command is a single trailing argv element (PowerShell parses it).
        assert_eq!(argv.len(), 5);
        assert!(argv[4].contains("Get-ChildItem"));
    }

    #[test]
    fn prepends_utf8_line_and_appends_exit_code_resolver() {
        let argv = build_shell_invocation(
            "echo x",
            BuildShellOptions {
                platform: Some(Platform::Win32),
                resolve_shell: Some(Box::new(pwsh_shell)),
            },
        );
        let script = argv.get(4).cloned().unwrap_or_default();
        assert!(script.contains("[Console]::OutputEncoding")); // UTF-8 prefix
        assert!(script.contains("echo x")); // the user command, verbatim
                                            // Exit-code propagation so a failing native command's code isn't swallowed.
        assert!(script.contains("$LASTEXITCODE"));
        assert!(script.trim_end().ends_with("exit $__ae_ec"));
    }

    #[test]
    fn falls_back_to_cmd_c_when_only_cmd_is_available() {
        let argv = build_shell_invocation(
            "dir",
            BuildShellOptions {
                platform: Some(Platform::Win32),
                resolve_shell: Some(Box::new(cmd_shell)),
            },
        );
        assert_eq!(argv, sv(&["C:\\Windows\\System32\\cmd.exe", "/c", "dir"]));
    }

    // ---- use_detached_process_group ----

    #[test]
    fn detaches_on_unix_not_on_windows() {
        assert!(use_detached_process_group(Some(Platform::Linux)));
        assert!(use_detached_process_group(Some(Platform::Darwin)));
        assert!(!use_detached_process_group(Some(Platform::Win32)));
    }

    #[test]
    fn shell_tree_kill_delegates_to_the_runtime_controller_signal_vocabulary() {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        assert_eq!(
            [
                Signal::Hup,
                Signal::Int,
                Signal::Quit,
                Signal::Kill,
                Signal::Usr1,
                Signal::Usr2,
                Signal::Term,
            ]
            .map(Signal::runtime_signal),
            [
                RuntimeSignal::Hangup,
                RuntimeSignal::Interrupt,
                RuntimeSignal::Quit,
                RuntimeSignal::Kill,
                RuntimeSignal::User1,
                RuntimeSignal::User2,
                RuntimeSignal::Terminate,
            ]
        );

        let source = include_str!("shell.rs");
        assert!(!source.contains("Command::new(\"taskkill\")"));
        assert!(source.contains("controller.signal(signal.runtime_signal())"));
    }

    // ---- resolve_windows_shell ----

    #[test]
    fn honors_the_klaus_shell_override_and_classifies_it() {
        let _g = TEST_GUARD.lock().unwrap_or_else(|e| e.into_inner());
        reset_shell_cache();
        let r = resolve_windows_shell(&env(&[("KLAUS_SHELL", "D:\\tools\\pwsh.exe")]));
        assert_eq!(
            r,
            ShellResolution {
                path: "D:\\tools\\pwsh.exe".to_string(),
                kind: ShellKind::Pwsh,
            }
        );
        reset_shell_cache();
    }

    #[test]
    fn classifies_a_powershell_override() {
        let _g = TEST_GUARD.lock().unwrap_or_else(|e| e.into_inner());
        reset_shell_cache();
        let r = resolve_windows_shell(&env(&[("KLAUS_SHELL", "powershell.exe")]));
        assert_eq!(r.kind, ShellKind::Powershell);
        reset_shell_cache();
    }

    #[test]
    fn classifies_a_bash_override_as_bash() {
        let _g = TEST_GUARD.lock().unwrap_or_else(|e| e.into_inner());
        reset_shell_cache();
        let r = resolve_windows_shell(&env(&[(
            "KLAUS_SHELL",
            "C:\\Program Files\\Git\\bin\\bash.exe",
        )]));
        assert_eq!(
            r,
            ShellResolution {
                path: "C:\\Program Files\\Git\\bin\\bash.exe".to_string(),
                kind: ShellKind::Bash,
            }
        );
        reset_shell_cache();
    }

    #[test]
    fn falls_back_to_comspec_cmd_only_when_neither_bash_nor_powershell_is_found() {
        let _g = TEST_GUARD.lock().unwrap_or_else(|e| e.into_inner());
        reset_shell_cache();
        // Empty PATH and nothing at the well-known locations → degraded cmd fallback via
        // ComSpec. The probe has to say "nothing exists": on a Windows host with Git for
        // Windows installed the well-known bash is really there, so emptying `Path` alone
        // still resolves a bash and this branch is unreachable.
        let r = compute_windows_shell(
            |_| false,
            &env(&[("Path", ""), ("ComSpec", "C:\\Windows\\System32\\cmd.exe")]),
        );
        assert_eq!(
            r,
            ShellResolution {
                path: "C:\\Windows\\System32\\cmd.exe".to_string(),
                kind: ShellKind::Cmd,
            }
        );
        reset_shell_cache();
    }
}
