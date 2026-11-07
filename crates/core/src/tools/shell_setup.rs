//! Consent-gated Windows bash setup.
//!
//! The `bash` tool wants REAL bash on Windows (full POSIX fidelity). When it is
//! missing the engine does NOT install anything silently — the host TELLS the
//! user, and only after the user agrees does it call [`install_windows_bash`].
//! This is a user-initiated setup step (installs Git for Windows via winget),
//! never an automatic background action.

use futures::future::BoxFuture;
use futures::FutureExt;
use std::collections::HashMap;

use crate::runtime_process::{
    RuntimeCompletedProcessRequest, RuntimeCompletedProcessSurface, RuntimeProcessContext,
};
use crate::runtime_values::{NodeBufferUtf8, RuntimeString};

/// The install command shown to the user for consent, plus a manual fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WindowsBashInstall {
    pub file: &'static str,
    pub args: &'static [&'static str],
    /// Human-readable form for the consent prompt.
    pub describe: &'static str,
    /// Where to get it if winget is unavailable.
    pub manual_url: &'static str,
}

/// The install command shown to the user for consent, plus a manual fallback.
pub const WINDOWS_BASH_INSTALL: WindowsBashInstall = WindowsBashInstall {
    file: "winget",
    args: &[
        "install",
        "-e",
        "--id",
        "Git.Git",
        "--silent",
        "--accept-package-agreements",
        "--accept-source-agreements",
    ],
    describe: "winget install -e --id Git.Git",
    manual_url: "https://git-scm.com/download/win",
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShellInstallResult {
    pub ok: bool,
    pub message: String,
    pub bash_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunOutput {
    pub status: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

/// Injectable async command runner (tests stub this; default spawns for real).
pub type CommandRunner =
    Box<dyn Fn(String, Vec<String>) -> BoxFuture<'static, RunOutput> + Send + Sync>;

/// Injectable bash discovery (tests stub this; default probes the OS).
pub type BashFinder = Box<dyn Fn(&HashMap<String, String>) -> Option<String> + Send + Sync>;

#[derive(Default)]
pub struct InstallBashOptions {
    pub env: Option<HashMap<String, String>>,
    /// Stub the command runner (tests).
    pub run: Option<CommandRunner>,
    /// Stub bash discovery (tests).
    pub bash_finder: Option<BashFinder>,
}

/// Default runner: spawn the command and collect its stdout/stderr. A spawn
/// failure (missing executable, resource limits) resolves with a `null` status
/// and empty output, mirroring the `error` event of a real spawn.
async fn default_runner(
    process_context: RuntimeProcessContext,
    file: String,
    args: Vec<String>,
) -> RunOutput {
    let request = RuntimeCompletedProcessRequest::new(
        RuntimeCompletedProcessSurface::WindowsBashSetup,
        RuntimeString::from(file),
    )
    .with_args(args.into_iter().map(RuntimeString::from).collect());
    match process_context.exec_completed(request).await {
        Ok(out) => RunOutput {
            status: out.status.code(),
            stdout: NodeBufferUtf8::decode(&out.stdout)
                .try_to_utf8()
                .expect("Node UTF-8 stdout decode is scalar text"),
            stderr: NodeBufferUtf8::decode(&out.stderr)
                .try_to_utf8()
                .expect("Node UTF-8 stderr decode is scalar text"),
        },
        Err(_) => RunOutput {
            status: None,
            stdout: String::new(),
            stderr: String::new(),
        },
    }
}

/// Render an exit status the way an interpolated `number | null` would read:
/// a missing (signal-killed / spawn-error) status becomes the literal `null`.
fn js_status(status: Option<i32>) -> String {
    match status {
        Some(n) => n.to_string(),
        None => "null".to_string(),
    }
}

/// True when the host should offer to install bash: a Windows session where no
/// real bash is on PATH (so the `bash` tool would otherwise run in degraded
/// PowerShell/cmd mode).
pub fn needs_windows_bash(platform: &str, env: &HashMap<String, String>) -> bool {
    needs_windows_bash_with(platform, env, crate::tools::shell::bin_exists)
}

/// [`needs_windows_bash`] with the executable-presence probe supplied; see
/// [`crate::tools::shell::BinExists`] for why that is a seam.
pub(crate) fn needs_windows_bash_with(
    platform: &str,
    env: &HashMap<String, String>,
    exists: crate::tools::shell::BinExists,
) -> bool {
    platform == "win32" && crate::tools::shell::find_windows_bash_with(env, exists).is_none()
}

/// Install git-bash on Windows via winget. MUST be called only after the user
/// has been told bash is missing and has agreed — this is the "user said yes"
/// step, not an automatic action. Idempotent: a no-op if bash already exists.
pub async fn install_windows_bash(
    process_context: &RuntimeProcessContext,
    opts: InstallBashOptions,
) -> ShellInstallResult {
    let env: HashMap<String, String> = opts.env.unwrap_or_else(|| std::env::vars().collect());
    let find: BashFinder = opts.bash_finder.unwrap_or_else(|| {
        Box::new(|env: &HashMap<String, String>| crate::tools::shell::find_windows_bash(env))
    });
    let run: CommandRunner = opts.run.unwrap_or_else(|| {
        let process_context = process_context.clone();
        Box::new(move |file: String, args: Vec<String>| {
            default_runner(process_context.clone(), file, args).boxed()
        })
    });

    if let Some(existing) = find(&env) {
        return ShellInstallResult {
            ok: true,
            message: "bash is already available.".to_string(),
            bash_path: Some(existing),
        };
    }

    let probe = run("winget".to_string(), vec!["--version".to_string()]).await;
    if probe.status != Some(0) {
        return ShellInstallResult {
            ok: false,
            message: format!(
                "winget is not available. Install Git for Windows manually: {}",
                WINDOWS_BASH_INSTALL.manual_url
            ),
            bash_path: None,
        };
    }

    let install_args: Vec<String> = WINDOWS_BASH_INSTALL
        .args
        .iter()
        .map(|s| s.to_string())
        .collect();
    let res = run(WINDOWS_BASH_INSTALL.file.to_string(), install_args).await;
    if res.status != Some(0) {
        let detail = if !res.stderr.is_empty() {
            res.stderr.trim()
        } else if !res.stdout.is_empty() {
            res.stdout.trim()
        } else {
            ""
        };
        let detail_part = if detail.is_empty() {
            String::new()
        } else {
            format!(" {detail}")
        };
        return ShellInstallResult {
            ok: false,
            message: format!(
                "winget install failed (exit {}).{} You can install manually: {}",
                js_status(res.status),
                detail_part,
                WINDOWS_BASH_INSTALL.manual_url
            ),
            bash_path: None,
        };
    }

    // The new bash invalidates the memoized shell choice.
    crate::tools::shell::reset_shell_cache();
    match find(&env) {
        Some(bash_path) => ShellInstallResult {
            ok: true,
            message: "Installed Git for Windows (git-bash).".to_string(),
            bash_path: Some(bash_path),
        },
        None => ShellInstallResult {
            ok: true,
            message:
                "Install reported success; restart the session so the new bash is picked up on PATH."
                    .to_string(),
            bash_path: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use std::sync::{Arc, Mutex};

    const BASH: &str = "C:\\Program Files\\Git\\bin\\bash.exe";

    async fn install_windows_bash(opts: InstallBashOptions) -> ShellInstallResult {
        super::install_windows_bash(
            &crate::runtime_process::test_runtime_process_context(),
            opts,
        )
        .await
    }

    #[test]
    fn is_false_off_windows() {
        assert!(!needs_windows_bash("linux", &HashMap::new()));
        assert!(!needs_windows_bash("darwin", &HashMap::new()));
    }

    #[test]
    fn is_true_on_windows_with_no_bash_on_path() {
        // Empty Path and nothing at the well-known locations. The probe is explicit because
        // those locations are real on a Windows host with Git for Windows installed, where
        // emptying `Path` alone still finds a bash.
        let mut env = HashMap::new();
        env.insert("Path".to_string(), String::new());
        assert!(needs_windows_bash_with("win32", &env, |_| false));
        // And with a bash present anywhere, nothing needs installing.
        assert!(!needs_windows_bash_with("win32", &env, |_| true));
    }

    #[tokio::test]
    async fn is_a_no_op_when_bash_already_exists() {
        let calls = Arc::new(Mutex::new(Vec::<(String, Vec<String>)>::new()));
        let recorder = calls.clone();
        let run: CommandRunner = Box::new(move |file: String, args: Vec<String>| {
            recorder.lock().unwrap().push((file, args));
            async {
                RunOutput {
                    status: None,
                    stdout: String::new(),
                    stderr: String::new(),
                }
            }
            .boxed()
        });
        let opts = InstallBashOptions {
            env: None,
            run: Some(run),
            bash_finder: Some(Box::new(|_env| Some(BASH.to_string()))),
        };
        let r = install_windows_bash(opts).await;
        assert!(r.ok);
        assert_eq!(r.bash_path.as_deref(), Some(BASH));
        // Never touches winget.
        assert!(calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn reports_a_clear_failure_with_manual_url_when_winget_unavailable() {
        let calls = Arc::new(Mutex::new(Vec::<(String, Vec<String>)>::new()));
        let recorder = calls.clone();
        let run: CommandRunner = Box::new(move |file: String, args: Vec<String>| {
            recorder.lock().unwrap().push((file, args));
            async {
                RunOutput {
                    status: Some(1),
                    stdout: String::new(),
                    stderr: "not found".to_string(),
                }
            }
            .boxed()
        });
        let opts = InstallBashOptions {
            env: None,
            run: Some(run),
            bash_finder: Some(Box::new(|_env| None)),
        };
        let r = install_windows_bash(opts).await;
        assert!(!r.ok);
        assert!(r.message.contains(WINDOWS_BASH_INSTALL.manual_url));
        let calls = calls.lock().unwrap();
        assert!(calls
            .iter()
            .any(|(f, a)| f.as_str() == "winget" && a == &vec!["--version".to_string()]));
    }

    #[tokio::test]
    async fn surfaces_a_winget_install_failure() {
        let run: CommandRunner = Box::new(|_file: String, args: Vec<String>| {
            let out = if args.first().map(String::as_str) == Some("--version") {
                RunOutput {
                    status: Some(0),
                    stdout: "v1".to_string(),
                    stderr: String::new(),
                }
            } else {
                RunOutput {
                    status: Some(5),
                    stdout: String::new(),
                    stderr: "install error".to_string(),
                }
            };
            async move { out }.boxed()
        });
        let opts = InstallBashOptions {
            env: None,
            run: Some(run),
            bash_finder: Some(Box::new(|_env| None)),
        };
        let r = install_windows_bash(opts).await;
        assert!(!r.ok);
        assert!(r.message.contains("install error"));
    }

    #[tokio::test]
    async fn installs_via_winget_and_returns_the_new_bash_path_on_success() {
        let installed = Arc::new(Mutex::new(false));
        let calls = Arc::new(Mutex::new(Vec::<(String, Vec<String>)>::new()));
        let inst_run = installed.clone();
        let recorder = calls.clone();
        let run: CommandRunner = Box::new(move |file: String, args: Vec<String>| {
            recorder.lock().unwrap().push((file, args.clone()));
            let out = if args.first().map(String::as_str) == Some("--version") {
                RunOutput {
                    status: Some(0),
                    stdout: "v1".to_string(),
                    stderr: String::new(),
                }
            } else {
                // The winget install "succeeded".
                *inst_run.lock().unwrap() = true;
                RunOutput {
                    status: Some(0),
                    stdout: "Successfully installed".to_string(),
                    stderr: String::new(),
                }
            };
            async move { out }.boxed()
        });
        let inst_find = installed.clone();
        let bash_finder: BashFinder = Box::new(move |_env| {
            if *inst_find.lock().unwrap() {
                Some(BASH.to_string())
            } else {
                None
            }
        });
        let opts = InstallBashOptions {
            env: None,
            run: Some(run),
            bash_finder: Some(bash_finder),
        };
        let r = install_windows_bash(opts).await;
        assert!(r.ok);
        assert_eq!(r.bash_path.as_deref(), Some(BASH));
        // It actually ran the documented winget install command.
        let expected: Vec<String> = WINDOWS_BASH_INSTALL
            .args
            .iter()
            .map(|s| s.to_string())
            .collect();
        let calls = calls.lock().unwrap();
        assert!(calls
            .iter()
            .any(|(f, a)| f.as_str() == WINDOWS_BASH_INSTALL.file && a == &expected));
    }
}
