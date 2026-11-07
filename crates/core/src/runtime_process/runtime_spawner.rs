//! Production adapter from exact logical process values to the project-owned
//! process image, spawn transaction, and shared reaper.

use std::collections::{BTreeMap, HashMap};
use std::ffi::OsString;
use std::fmt;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError, Weak};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};
// The Windows sandbox PTY bridge pumps bytes through a duplex split half with
// `write_all`/`shutdown`; unix never compiles that block, so the extension
// trait is gated with it.
#[cfg(windows)]
use tokio::io::AsyncWriteExt;
use tokio::runtime::{Handle, Id as TokioRuntimeId};
use tokio::sync::watch;

#[cfg(target_os = "macos")]
use super::macos_posix_spawn::MacOsPtySize;
use super::parent_env_snapshot::ParentEnvSnapshot;
use super::plan::{
    NativeHostEnvOverlay, NativeOsString, Rust193EnvMutation, Rust193EnvironmentPlan,
    Rust193SpawnPreimage,
};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use super::reaper::{RuntimeChild, RuntimeChildController, RuntimeDropAction};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use super::spawn::{SharedRuntimeSpawner, SharedSpawnPolicy};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use super::unix::UnixExecImage;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use super::unix_spawn::{
    ParentStdio, RuntimeChildExtraDuplex as UnixChildExtraDuplex,
    RuntimeChildExtraRead as UnixChildExtraRead, RuntimeChildExtraWrite as UnixChildExtraWrite,
    RuntimeChildStderr as UnixChildStderr, RuntimeChildStdin as UnixChildStdin,
    RuntimeChildStdout as UnixChildStdout, RuntimeExtraPipe, RuntimeExtraPipeEndpoint,
    RuntimePtyMaster as UnixPtyMaster, RuntimePtyRead as UnixPtyRead,
    RuntimePtyWrite as UnixPtyWrite, UnixExtraPipeDirection, UnixExtraStdioPlan,
    UnixStdioDisposition, UnixStdioPlan,
};
#[cfg(windows)]
use super::windows::{WindowsEnvironmentPlatform, WindowsExecImage};
#[cfg(windows)]
use super::windows_browser_broker::{
    prepare_windows_browser_bootstrap, WindowsBrowserBootstrapSeed, WindowsBrowserBrokerOwner,
};
#[cfg(windows)]
use super::windows_platform::ProductionWindowsEnvironmentPlatform;
#[cfg(windows)]
use super::windows_spawn::{
    WindowsConPtyMaster, WindowsConPtyReader, WindowsConPtySize, WindowsConPtyWriter,
    WindowsControlAction, WindowsControlStart, WindowsInheritedControlPipe, WindowsParentFiles,
    WindowsProcessController, WindowsSpawnBundle, WindowsSpawnPlan, WindowsSpawnSupervisor,
    WindowsStdioDisposition, WindowsStdioPlan,
};
use super::{
    EnvironmentImageDialect, KillTarget, LogicalSpawnRequest, ProcessAtom, ProcessEnvironmentGuard,
    ProcessImageDialect, ProcessTopology, WindowsJobPolicy,
};
use crate::runtime_process::RuntimeEnv;
use crate::runtime_values::{RuntimePath, RuntimeString};
use crate::types::sandbox::SpawnSpec;

#[cfg(windows)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RuntimeDropAction {
    KeepAlive,
    HardKill(KillTarget),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeStdio {
    Inherit,
    Null,
    Pipe,
}

impl RuntimeStdio {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn unix(self) -> UnixStdioDisposition {
        match self {
            Self::Inherit => UnixStdioDisposition::Inherit,
            Self::Null => UnixStdioDisposition::Null,
            Self::Pipe => UnixStdioDisposition::Pipe,
        }
    }

    #[cfg(windows)]
    fn windows(self) -> WindowsStdioDisposition {
        match self {
            Self::Inherit => WindowsStdioDisposition::Inherit,
            Self::Null => WindowsStdioDisposition::Null,
            Self::Pipe => WindowsStdioDisposition::Pipe,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RuntimePtySize {
    rows: u16,
    columns: u16,
    pixel_width: u16,
    pixel_height: u16,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeProcessSurface {
    GitExecFile,
    ForegroundShell,
    BackgroundShell,
    ShellSession,
    InteractivePty {
        rows: u16,
        columns: u16,
    },
    DirectExec {
        tty: bool,
        pipe_stdin: bool,
        rows: u16,
        columns: u16,
    },
}

impl RuntimeProcessSurface {
    fn policy_id(self) -> ProcessSurfacePolicyId {
        match self {
            Self::GitExecFile => ProcessSurfacePolicyId::TsGitExecFile5s,
            Self::ForegroundShell => ProcessSurfacePolicyId::TsForegroundShell,
            Self::BackgroundShell => ProcessSurfacePolicyId::TsBackgroundShell,
            Self::ShellSession => ProcessSurfacePolicyId::TsShellSession,
            Self::InteractivePty { .. } => ProcessSurfacePolicyId::RustInteractivePtyExtension,
            Self::DirectExec { .. } => ProcessSurfacePolicyId::RustDirectExecExtension,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProcessSurfacePolicyId {
    LegacyRustProcess,
    EvalArtifactGit,
    EvalWorktreeAdd,
    EvalWorktreeRemove,
    TsExtensionExecFile,
    TsFileIndexGitStream,
    TsPdfExecFile,
    TsCredentialExecFile,
    TsOAuthOpenerExecFile,
    RustEvalCommandSuccess,
    ScalarActionAllowlist,
    TsShellEnvSnapshot,
    TsRipgrepExecFile,
    TsLsp,
    TsMcpStdio,
    RustCodeModeHost,
    RustBrowserSidecar,
    BrowserRunningProbe,
    BrowserProbeQuiet,
    BrowserNativeHostRegister,
    BrowserDetachedChromeLaunch,
    BrowserPlaywrightEphemeral1611,
    BrowserPlaywrightPersistent1611,
    RustBrowserPlaywrightMacOsSwVers,
    RustBrowserPlaywrightLinuxLdd,
    RustBrowserPlaywrightLinuxLdconfig,
    RustBrowserPlaywrightWindowsWinldd,
    BrowserFfmpegEncode30s,
    TsGitExecFile5s,
    TsForegroundShell,
    TsBackgroundShell,
    TsShellSession,
    RustInteractivePtyExtension,
    RustDirectExecExtension,
}

impl ProcessSurfacePolicyId {
    const fn name(self) -> &'static str {
        match self {
            Self::LegacyRustProcess => "legacy_rust_process",
            Self::EvalArtifactGit => "eval_artifact_git",
            Self::EvalWorktreeAdd => "eval_worktree_add",
            Self::EvalWorktreeRemove => "eval_worktree_remove",
            Self::TsExtensionExecFile => "ts_extension_execfile",
            Self::TsFileIndexGitStream => "ts_file_index_git_stream",
            Self::TsPdfExecFile => "ts_pdf_execfile",
            Self::TsCredentialExecFile => "ts_credential_execfile",
            Self::TsOAuthOpenerExecFile => "ts_oauth_opener_execfile",
            Self::RustEvalCommandSuccess => "rust_eval_command_success",
            Self::ScalarActionAllowlist => "scalar_action_allowlist",
            Self::TsShellEnvSnapshot => "ts_shell_env_snapshot",
            Self::TsRipgrepExecFile => "ts_ripgrep_execfile",
            Self::TsLsp => "ts_lsp",
            Self::TsMcpStdio => "ts_mcp_stdio_1_29_0",
            Self::RustCodeModeHost => "rust_code_mode_host",
            Self::RustBrowserSidecar => "rust_browser_sidecar",
            Self::BrowserRunningProbe => "browser_running_probe",
            Self::BrowserProbeQuiet => "browser_probe_quiet",
            Self::BrowserNativeHostRegister => "browser_native_host_register",
            Self::BrowserDetachedChromeLaunch => "browser_detached_chrome_launch",
            Self::BrowserPlaywrightEphemeral1611 => "browser_playwright_ephemeral_1_61_1",
            Self::BrowserPlaywrightPersistent1611 => "browser_playwright_persistent_1_61_1",
            Self::RustBrowserPlaywrightMacOsSwVers => "browser_playwright_macos_sw_vers_1_61_1",
            Self::RustBrowserPlaywrightLinuxLdd => "browser_playwright_linux_ldd_1_61_1",
            Self::RustBrowserPlaywrightLinuxLdconfig => "browser_playwright_linux_ldconfig_1_61_1",
            Self::RustBrowserPlaywrightWindowsWinldd => "browser_playwright_windows_winldd_1_61_1",
            Self::BrowserFfmpegEncode30s => "browser_ffmpeg_encode_30s",
            Self::TsGitExecFile5s => "ts_git_exec_file_5s",
            Self::TsForegroundShell => "ts_foreground_shell",
            Self::TsBackgroundShell => "ts_background_shell",
            Self::TsShellSession => "ts_shell_session",
            Self::RustInteractivePtyExtension => "rust_interactive_pty_extension",
            Self::RustDirectExecExtension => "rust_direct_exec_extension",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ContextShutdownAction {
    RequireCompleted,
    HardKill,
}

#[derive(Debug)]
pub(crate) struct RuntimeSpawnRequest {
    file: ProcessAtom,
    args: Vec<ProcessAtom>,
    cwd: Option<ProcessAtom>,
    env: Option<RuntimeEnv>,
    rust193_env_mutations: Option<Vec<Rust193EnvMutation>>,
    image_dialect: ProcessImageDialect,
    environment_dialect: EnvironmentImageDialect,
    environment_guard: ProcessEnvironmentGuard,
    windows_job_policy: WindowsJobPolicy,
    topology: ProcessTopology,
    kill_target: KillTarget,
    stdin: RuntimeStdio,
    stdout: RuntimeStdio,
    stderr: RuntimeStdio,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    unix_extra_stdio: Vec<UnixExtraStdioPlan>,
    pty: Option<RuntimePtySize>,
    #[cfg(windows)]
    windows_hide: bool,
    #[cfg(windows)]
    windows_browser_bootstrap_seed: Option<WindowsBrowserBootstrapSeed>,
    #[cfg(windows)]
    /// Serialized sandbox request, handed to the wrapper over an inherited pipe
    /// rather than on the command line: it carries the target's environment, and a
    /// Windows command line is readable by any process on the machine.
    windows_sandbox_request: Option<Vec<u8>>,
    #[cfg(windows)]
    windows_sandbox_terminal_size: Option<RuntimePtySize>,
    surface_policy: ProcessSurfacePolicyId,
    drop_action: RuntimeDropAction,
}

impl RuntimeSpawnRequest {
    /// Compatibility constructor for frozen Rust-only callers. It deliberately
    /// carries no cwd or environment overlay and therefore retains the native
    /// parent environment dialect.
    pub(crate) fn new(file: RuntimeString) -> Self {
        Self {
            file: ProcessAtom::Runtime(file),
            args: Vec::new(),
            cwd: None,
            env: None,
            rust193_env_mutations: None,
            image_dialect: ProcessImageDialect::Rust193,
            environment_dialect: EnvironmentImageDialect::NativeParentInherit,
            environment_guard: ProcessEnvironmentGuard::SealedNodeNoPreloadV1,
            windows_job_policy: WindowsJobPolicy::None,
            topology: ProcessTopology::Inherit,
            kill_target: KillTarget::Child,
            stdin: RuntimeStdio::Inherit,
            stdout: RuntimeStdio::Inherit,
            stderr: RuntimeStdio::Inherit,
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            unix_extra_stdio: Vec::new(),
            pty: None,
            #[cfg(windows)]
            windows_hide: false,
            #[cfg(windows)]
            windows_browser_bootstrap_seed: None,
            #[cfg(windows)]
            windows_sandbox_request: None,
            #[cfg(windows)]
            windows_sandbox_terminal_size: None,
            surface_policy: ProcessSurfacePolicyId::LegacyRustProcess,
            drop_action: RuntimeDropAction::KeepAlive,
        }
    }

    /// Bind one sandbox-transformed exact spawn plan to a generated command
    /// surface. The caller cannot choose topology, kill target, or image policy.
    pub(crate) fn from_spawn_spec(spec: SpawnSpec, surface: RuntimeProcessSurface) -> Self {
        let SpawnSpec {
            file,
            args,
            cwd,
            env,
        } = spec;
        #[cfg(windows)]
        let mut args = args;
        #[cfg(windows)]
        let windows_sandbox_request =
            crate::sandbox::windows::take_request_from_spawn_args(&mut args);
        let (topology, kill_target) = if matches!(surface, RuntimeProcessSurface::GitExecFile) {
            (ProcessTopology::Inherit, KillTarget::Child)
        } else if cfg!(windows) {
            (ProcessTopology::Inherit, KillTarget::WindowsProcessTree)
        } else {
            match surface {
                RuntimeProcessSurface::GitExecFile => unreachable!("handled above"),
                RuntimeProcessSurface::InteractivePty { .. }
                | RuntimeProcessSurface::DirectExec { tty: true, .. } => (
                    ProcessTopology::UnixControllingPty,
                    KillTarget::UnixProcessGroup,
                ),
                _ => (
                    ProcessTopology::UnixNewSession,
                    KillTarget::UnixProcessGroup,
                ),
            }
        };
        let (stdin, stdout, stderr, pty, drop_action) = match surface {
            RuntimeProcessSurface::GitExecFile => (
                RuntimeStdio::Pipe,
                RuntimeStdio::Pipe,
                RuntimeStdio::Pipe,
                None,
                RuntimeDropAction::HardKill(KillTarget::Child),
            ),
            RuntimeProcessSurface::ForegroundShell | RuntimeProcessSurface::BackgroundShell => (
                RuntimeStdio::Null,
                RuntimeStdio::Pipe,
                RuntimeStdio::Pipe,
                None,
                RuntimeDropAction::KeepAlive,
            ),
            RuntimeProcessSurface::ShellSession => (
                RuntimeStdio::Pipe,
                RuntimeStdio::Pipe,
                RuntimeStdio::Pipe,
                None,
                RuntimeDropAction::KeepAlive,
            ),
            RuntimeProcessSurface::InteractivePty { rows, columns } => (
                RuntimeStdio::Pipe,
                RuntimeStdio::Pipe,
                RuntimeStdio::Pipe,
                Some(RuntimePtySize {
                    rows,
                    columns,
                    pixel_width: 0,
                    pixel_height: 0,
                }),
                RuntimeDropAction::HardKill(kill_target),
            ),
            RuntimeProcessSurface::DirectExec {
                tty: false,
                pipe_stdin,
                ..
            } => (
                if pipe_stdin {
                    RuntimeStdio::Pipe
                } else {
                    RuntimeStdio::Null
                },
                RuntimeStdio::Pipe,
                RuntimeStdio::Pipe,
                None,
                RuntimeDropAction::KeepAlive,
            ),
            RuntimeProcessSurface::DirectExec {
                tty: true,
                rows,
                columns,
                ..
            } => (
                RuntimeStdio::Pipe,
                RuntimeStdio::Pipe,
                RuntimeStdio::Pipe,
                Some(RuntimePtySize {
                    rows,
                    columns,
                    pixel_width: 0,
                    pixel_height: 0,
                }),
                RuntimeDropAction::KeepAlive,
            ),
        };
        #[cfg(windows)]
        // A sandboxed launch moves the terminal into the wrapper: the target gets a
        // ConPTY built inside the sandbox, so the host must not also open one here.
        let (pty, windows_sandbox_terminal_size) = if windows_sandbox_request.is_some() {
            (None, pty)
        } else {
            (pty, None)
        };
        Self {
            file: ProcessAtom::Runtime(file),
            args: args.into_iter().map(ProcessAtom::Runtime).collect(),
            cwd: Some(cwd.into_runtime_process_atom()),
            env: Some(env),
            rust193_env_mutations: None,
            image_dialect: ProcessImageDialect::Node20Libuv146,
            environment_dialect: EnvironmentImageDialect::Node20ParentSnapshot,
            environment_guard: ProcessEnvironmentGuard::None,
            windows_job_policy: if cfg!(windows) {
                WindowsJobPolicy::LibuvGlobal
            } else {
                WindowsJobPolicy::None
            },
            topology,
            kill_target,
            stdin,
            stdout,
            stderr,
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            unix_extra_stdio: Vec::new(),
            pty,
            #[cfg(windows)]
            windows_hide: true,
            #[cfg(windows)]
            windows_browser_bootstrap_seed: None,
            #[cfg(windows)]
            windows_sandbox_request,
            #[cfg(windows)]
            windows_sandbox_terminal_size,
            surface_policy: surface.policy_id(),
            drop_action,
        }
    }

    pub(crate) fn with_args(mut self, args: Vec<RuntimeString>) -> Self {
        self.args = args.into_iter().map(ProcessAtom::Runtime).collect();
        self
    }

    pub(crate) fn with_cwd(mut self, cwd: RuntimePath) -> Self {
        self.cwd = Some(cwd.into_runtime_process_atom());
        self
    }

    fn with_native_args(mut self, args: Vec<NativeOsString>) -> Self {
        self.args = args.into_iter().map(ProcessAtom::Native).collect();
        self
    }

    fn with_native_cwd(mut self, cwd: NativeOsString) -> Self {
        self.cwd = Some(ProcessAtom::Native(cwd));
        self
    }

    fn with_rust193_env_mutations(mut self, mutations: Vec<Rust193EnvMutation>) -> Self {
        self.rust193_env_mutations = Some(mutations);
        self.environment_dialect = EnvironmentImageDialect::Rust193CommandOverlay;
        self
    }

    pub(crate) fn with_stdio(
        mut self,
        stdin: RuntimeStdio,
        stdout: RuntimeStdio,
        stderr: RuntimeStdio,
    ) -> Self {
        self.stdin = stdin;
        self.stdout = stdout;
        self.stderr = stderr;
        self
    }

    pub(crate) fn with_hard_kill_on_drop(mut self, enabled: bool) -> Self {
        self.drop_action = if enabled {
            RuntimeDropAction::HardKill(self.kill_target)
        } else {
            RuntimeDropAction::KeepAlive
        };
        self
    }

    fn with_surface_policy(mut self, surface_policy: ProcessSurfacePolicyId) -> Self {
        self.surface_policy = surface_policy;
        self
    }

    fn context_shutdown_action(&self) -> ContextShutdownAction {
        match self.surface_policy {
            ProcessSurfacePolicyId::TsForegroundShell => ContextShutdownAction::RequireCompleted,
            ProcessSurfacePolicyId::LegacyRustProcess => match self.drop_action {
                RuntimeDropAction::KeepAlive => ContextShutdownAction::RequireCompleted,
                RuntimeDropAction::HardKill(_) => ContextShutdownAction::HardKill,
            },
            ProcessSurfacePolicyId::EvalArtifactGit
            | ProcessSurfacePolicyId::EvalWorktreeAdd
            | ProcessSurfacePolicyId::EvalWorktreeRemove
            | ProcessSurfacePolicyId::TsExtensionExecFile
            | ProcessSurfacePolicyId::TsFileIndexGitStream
            | ProcessSurfacePolicyId::TsPdfExecFile
            | ProcessSurfacePolicyId::TsCredentialExecFile
            | ProcessSurfacePolicyId::RustEvalCommandSuccess
            | ProcessSurfacePolicyId::TsShellEnvSnapshot
            | ProcessSurfacePolicyId::TsRipgrepExecFile
            | ProcessSurfacePolicyId::TsLsp
            | ProcessSurfacePolicyId::RustCodeModeHost
            | ProcessSurfacePolicyId::RustBrowserSidecar
            | ProcessSurfacePolicyId::BrowserPlaywrightEphemeral1611
            | ProcessSurfacePolicyId::BrowserPlaywrightPersistent1611
            | ProcessSurfacePolicyId::BrowserFfmpegEncode30s => ContextShutdownAction::HardKill,
            ProcessSurfacePolicyId::BrowserDetachedChromeLaunch => {
                ContextShutdownAction::RequireCompleted
            }
            ProcessSurfacePolicyId::TsOAuthOpenerExecFile
            | ProcessSurfacePolicyId::TsMcpStdio
            | ProcessSurfacePolicyId::ScalarActionAllowlist
            | ProcessSurfacePolicyId::BrowserRunningProbe
            | ProcessSurfacePolicyId::BrowserProbeQuiet
            | ProcessSurfacePolicyId::BrowserNativeHostRegister
            | ProcessSurfacePolicyId::RustBrowserPlaywrightMacOsSwVers
            | ProcessSurfacePolicyId::RustBrowserPlaywrightLinuxLdd
            | ProcessSurfacePolicyId::RustBrowserPlaywrightLinuxLdconfig
            | ProcessSurfacePolicyId::RustBrowserPlaywrightWindowsWinldd => {
                ContextShutdownAction::RequireCompleted
            }
            ProcessSurfacePolicyId::TsGitExecFile5s
            | ProcessSurfacePolicyId::TsBackgroundShell
            | ProcessSurfacePolicyId::TsShellSession
            | ProcessSurfacePolicyId::RustInteractivePtyExtension
            | ProcessSurfacePolicyId::RustDirectExecExtension => ContextShutdownAction::HardKill,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeSpawnStage {
    Initialization,
    Plan,
    Image,
    Spawn,
    StdioShape,
    Wait,
}

#[derive(Debug)]
pub(crate) struct RuntimeSpawnError {
    stage: RuntimeSpawnStage,
    message: RuntimeString,
}

impl RuntimeSpawnError {
    fn new(stage: RuntimeSpawnStage, message: impl AsRef<str>) -> Self {
        Self {
            stage,
            message: RuntimeString::from_utf8(message.as_ref()),
        }
    }

    pub(crate) fn message(&self) -> &RuntimeString {
        &self.message
    }

    pub(crate) fn stdio_shape(error: std::io::Error) -> Self {
        Self::new(RuntimeSpawnStage::StdioShape, error.to_string())
    }

    pub(crate) fn into_io_error(self) -> std::io::Error {
        self.as_io_error()
    }

    /// Whether the spawn failed because the program itself is absent, which a
    /// caller may answer with a fallback instead of a hard failure.
    pub(crate) fn is_missing_program(&self) -> bool {
        self.as_io_error().kind() == std::io::ErrorKind::NotFound
    }

    fn as_io_error(&self) -> std::io::Error {
        let message = self.host_message();
        // `win32:` is not decoration: the Windows spawner reports `GetLastError` that way,
        // and callers branch on the recovered KIND — "the program is not there" versus "the
        // program failed". Recognising only `errno` left a missing ripgrep on Windows as a
        // kindless error, which `fs::ripgrep` then classified as "no matches", so every
        // search silently returned nothing instead of saying search was unavailable.
        for marker in ["errno ", "errno=", "errno: ", "win32:"] {
            if let Some(tail) = message.rsplit_once(marker).map(|(_, tail)| tail) {
                let digits = tail
                    .chars()
                    .take_while(|character| character.is_ascii_digit() || *character == '-')
                    .collect::<String>();
                if let Ok(errno) = digits.parse::<i32>() {
                    return std::io::Error::from_raw_os_error(errno);
                }
            }
        }
        std::io::Error::other(message)
    }

    pub(crate) fn host_message(&self) -> String {
        String::from_utf8(self.message.to_node_utf8())
            .unwrap_or_else(|_| "runtime process spawn failed".to_string())
    }
}

impl fmt::Display for RuntimeSpawnError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "runtime process {:?} failed: {:?}",
            self.stage, self.message
        )
    }
}

impl std::error::Error for RuntimeSpawnError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeProcessStatus {
    code: Option<i32>,
    signal: Option<i32>,
}

impl RuntimeProcessStatus {
    pub(crate) const fn success(self) -> bool {
        matches!(self.code, Some(0))
    }

    pub(crate) const fn code(self) -> Option<i32> {
        self.code
    }

    pub(crate) const fn signal(self) -> Option<i32> {
        self.signal
    }
}

/// Opaque identity of one Core/SDK process namespace.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RuntimeProcessNamespaceId(u64);

impl RuntimeProcessNamespaceId {
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Closed bootstrap failure for the shared platform process backend.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeProcessBootstrapError {
    message: String,
}

impl RuntimeProcessBootstrapError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for RuntimeProcessBootstrapError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for RuntimeProcessBootstrapError {}

/// Closed shutdown failure. A failure retains the draining namespace so a
/// later call can recheck the same registrations without replaying actions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeProcessShutdownError {
    message: String,
}

impl RuntimeProcessShutdownError {
    fn backend(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    fn live(policy_ids: impl IntoIterator<Item = &'static str>) -> Self {
        let policies = policy_ids.into_iter().collect::<Vec<_>>().join(", ");
        Self::backend(format!(
            "runtime process namespace retains incomplete surfaces: {policies}"
        ))
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for RuntimeProcessShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for RuntimeProcessShutdownError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RuntimeProcessSupervisorPhase {
    Running,
    Draining,
    Closed,
}

struct RuntimeProcessSupervisorState {
    phase: RuntimeProcessSupervisorPhase,
    contexts: HashMap<RuntimeProcessNamespaceId, Weak<RuntimeProcessContextInner>>,
}

/// One process-global owner of the platform notifier, spawn backend, and
/// reaper. Contexts borrow namespaces from this supervisor; they never create
/// another platform backend.
pub struct RuntimeProcessSupervisor {
    platform: RuntimePlatformSpawner,
    runtime: Handle,
    runtime_id: TokioRuntimeId,
    next_namespace_id: AtomicU64,
    state: Mutex<RuntimeProcessSupervisorState>,
}

fn process_supervisor_slot() -> &'static Mutex<Option<Weak<RuntimeProcessSupervisor>>> {
    static SLOT: OnceLock<Mutex<Option<Weak<RuntimeProcessSupervisor>>>> = OnceLock::new();
    SLOT.get_or_init(|| Mutex::new(None))
}

impl RuntimeProcessSupervisor {
    pub fn initialize(handle: Handle) -> Result<Arc<Self>, RuntimeProcessBootstrapError> {
        let runtime_id = handle.id();
        let mut slot = process_supervisor_slot().lock().map_err(|_| {
            RuntimeProcessBootstrapError::new("runtime process bootstrap lock poisoned")
        })?;
        if let Some(existing) = slot.as_ref().and_then(Weak::upgrade) {
            let state = existing.state.lock().map_err(|_| {
                RuntimeProcessBootstrapError::new("runtime process supervisor lock poisoned")
            })?;
            // A CLOSED supervisor is dead weight: it no longer spawns anything,
            // so it must not veto the next bind (each #[tokio::test] owns a fresh
            // runtime, and a stale slot would fail every later test in the
            // process). Only a LIVE bind guards the one-runtime invariant.
            if state.phase != RuntimeProcessSupervisorPhase::Closed {
                if existing.runtime_id != runtime_id {
                    return Err(RuntimeProcessBootstrapError::new(
                        "runtime process supervisor is already bound to a different Tokio runtime",
                    ));
                }
                return match state.phase {
                    RuntimeProcessSupervisorPhase::Running => Ok(existing.clone()),
                    RuntimeProcessSupervisorPhase::Draining => Err(
                        RuntimeProcessBootstrapError::new("runtime process supervisor is draining"),
                    ),
                    RuntimeProcessSupervisorPhase::Closed => unreachable!("checked above"),
                };
            }
            drop(state);
            *slot = None;
        }

        let platform = RuntimePlatformSpawner::initialize(handle.clone()).map_err(|error| {
            RuntimeProcessBootstrapError::new(format!("runtime process bootstrap failed: {error}"))
        })?;
        let supervisor = Arc::new(Self {
            platform,
            runtime: handle,
            runtime_id,
            next_namespace_id: AtomicU64::new(1),
            state: Mutex::new(RuntimeProcessSupervisorState {
                phase: RuntimeProcessSupervisorPhase::Running,
                contexts: HashMap::new(),
            }),
        });
        *slot = Some(Arc::downgrade(&supervisor));
        Ok(supervisor)
    }

    pub fn initialize_current() -> Result<Arc<Self>, RuntimeProcessBootstrapError> {
        let handle = Handle::try_current().map_err(|_| {
            RuntimeProcessBootstrapError::new(
                "runtime process supervisor requires a current Tokio runtime",
            )
        })?;
        Self::initialize(handle)
    }

    pub async fn shutdown(&self) -> Result<(), RuntimeProcessShutdownError> {
        {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            if state.phase == RuntimeProcessSupervisorPhase::Closed {
                return Ok(());
            }
            state.phase = RuntimeProcessSupervisorPhase::Draining;
            state
                .contexts
                .retain(|_, context| context.upgrade().is_some());
            if !state.contexts.is_empty() {
                let mut ids = state.contexts.keys().copied().collect::<Vec<_>>();
                ids.sort();
                return Err(RuntimeProcessShutdownError::backend(format!(
                    "runtime process supervisor still owns namespaces: {}",
                    ids.into_iter()
                        .map(|id| id.get().to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )));
            }
        }
        self.platform.shutdown().await?;
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .phase = RuntimeProcessSupervisorPhase::Closed;
        Ok(())
    }

    fn release_context(&self, id: RuntimeProcessNamespaceId) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contexts
            .remove(&id);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RuntimeProcessContextPhase {
    Running,
    Draining,
    Closed,
}

struct RuntimeProcessContextState {
    phase: RuntimeProcessContextPhase,
    external_permits: usize,
    next_browser_generation: u64,
    next_registration_id: u64,
    registrations: BTreeMap<u64, Arc<ContextProcessRegistration>>,
}

struct RuntimeProcessContextInner {
    id: RuntimeProcessNamespaceId,
    supervisor: Arc<RuntimeProcessSupervisor>,
    state: Mutex<RuntimeProcessContextState>,
    epoch_tx: watch::Sender<u64>,
}

/// Cloneable facade over one explicitly admitted Core/SDK process namespace.
#[derive(Clone)]
pub struct RuntimeProcessContext {
    inner: Arc<RuntimeProcessContextInner>,
}

impl fmt::Debug for RuntimeProcessContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeProcessContext")
            .field("namespace_id", &self.namespace_id())
            .finish_non_exhaustive()
    }
}

struct ContextExternalSpawnPermit {
    context: Arc<RuntimeProcessContextInner>,
    active: bool,
}

impl Drop for ContextExternalSpawnPermit {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        self.active = false;
        let epoch = {
            let mut state = self
                .context
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            state.external_permits = state
                .external_permits
                .checked_sub(1)
                .unwrap_or_else(|| std::process::abort());
            self.context.epoch_tx.borrow().wrapping_add(1)
        };
        self.context.epoch_tx.send_replace(epoch);
    }
}

struct ContextProcessRegistrationState {
    action_started: bool,
    settle_deadline: Option<Instant>,
}

struct ContextProcessRegistration {
    policy_id: ProcessSurfacePolicyId,
    shutdown_action: ContextShutdownAction,
    controller: RuntimeProcessController,
    state: Mutex<ContextProcessRegistrationState>,
}

impl ContextProcessRegistration {
    fn trigger_shutdown(&self) {
        if self.shutdown_action == ContextShutdownAction::RequireCompleted {
            return;
        }
        let should_signal = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            if state.action_started {
                false
            } else {
                state.action_started = true;
                state.settle_deadline = Some(
                    Instant::now()
                        .checked_add(Duration::from_millis(2_000))
                        .unwrap_or_else(|| std::process::abort()),
                );
                true
            }
        };
        if should_signal {
            self.controller.signal(RuntimeSignal::Kill);
        }
    }

    fn deadline(&self) -> Option<Instant> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .settle_deadline
    }
}

struct ContextProcessLease {
    context: Weak<RuntimeProcessContextInner>,
    registration_id: u64,
    active: bool,
}

impl ContextProcessLease {
    fn disarm(&mut self) {
        if !self.active {
            return;
        }
        self.active = false;
        if let Some(context) = self.context.upgrade() {
            context
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .registrations
                .remove(&self.registration_id);
        }
    }
}

impl RuntimeProcessContext {
    pub fn initialize(
        supervisor: Arc<RuntimeProcessSupervisor>,
    ) -> Result<Self, RuntimeProcessBootstrapError> {
        let id = RuntimeProcessNamespaceId(
            supervisor
                .next_namespace_id
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |id| id.checked_add(1))
                .map_err(|_| RuntimeProcessBootstrapError::new("runtime namespace id exhausted"))?,
        );
        let (epoch_tx, _) = watch::channel(0_u64);
        let inner = Arc::new(RuntimeProcessContextInner {
            id,
            supervisor: supervisor.clone(),
            state: Mutex::new(RuntimeProcessContextState {
                phase: RuntimeProcessContextPhase::Running,
                external_permits: 0,
                next_browser_generation: 1,
                next_registration_id: 1,
                registrations: BTreeMap::new(),
            }),
            epoch_tx,
        });
        let mut state = supervisor.state.lock().map_err(|_| {
            RuntimeProcessBootstrapError::new("runtime process supervisor lock poisoned")
        })?;
        if state.phase != RuntimeProcessSupervisorPhase::Running {
            return Err(RuntimeProcessBootstrapError::new(
                "runtime process supervisor does not admit a new namespace",
            ));
        }
        state.contexts.insert(id, Arc::downgrade(&inner));
        drop(state);
        Ok(Self { inner })
    }

    /// Admit a separate process namespace under the same runtime supervisor.
    pub(crate) fn isolated_context(&self) -> Result<Self, RuntimeProcessBootstrapError> {
        Self::initialize(self.inner.supervisor.clone())
    }

    pub fn namespace_id(&self) -> RuntimeProcessNamespaceId {
        self.inner.id
    }

    pub(crate) fn spawner(&self) -> RuntimeSpawner {
        RuntimeSpawner {
            context: self.clone(),
        }
    }

    fn admit_spawn(&self) -> Result<ContextExternalSpawnPermit, RuntimeSpawnError> {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if state.phase != RuntimeProcessContextPhase::Running {
            return Err(RuntimeSpawnError::new(
                RuntimeSpawnStage::Initialization,
                "runtime process context is draining and rejects new spawns",
            ));
        }
        state.external_permits = state.external_permits.checked_add(1).ok_or_else(|| {
            RuntimeSpawnError::new(
                RuntimeSpawnStage::Initialization,
                "runtime process external permit count exhausted",
            )
        })?;
        Ok(ContextExternalSpawnPermit {
            context: self.inner.clone(),
            active: true,
        })
    }

    fn register_spawn(
        &self,
        policy_id: ProcessSurfacePolicyId,
        shutdown_action: ContextShutdownAction,
        controller: RuntimeProcessController,
    ) -> ContextProcessLease {
        let registration = Arc::new(ContextProcessRegistration {
            policy_id,
            shutdown_action,
            controller,
            state: Mutex::new(ContextProcessRegistrationState {
                action_started: false,
                settle_deadline: None,
            }),
        });
        let registration_id = {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let id = state.next_registration_id;
            state.next_registration_id = id.checked_add(1).unwrap_or_else(|| std::process::abort());
            state.registrations.insert(id, registration);
            id
        };
        ContextProcessLease {
            context: Arc::downgrade(&self.inner),
            registration_id,
            active: true,
        }
    }

    fn reserve_browser_generation(&self) -> Result<u64, RuntimeProcessPipeError> {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if state.phase != RuntimeProcessContextPhase::Running {
            return Err(RuntimeProcessPipeError::new(
                "runtime process context is draining and rejects browser startup",
            ));
        }
        let generation = state.next_browser_generation;
        if generation == 0 {
            return Err(RuntimeProcessPipeError::new(
                "browser connection generation exhausted",
            ));
        }
        state.next_browser_generation = generation.checked_add(1).unwrap_or(0);
        Ok(generation)
    }

    pub async fn shutdown(&self) -> Result<(), RuntimeProcessShutdownError> {
        shutdown_process_context(self.inner.clone()).await
    }

    /// Run one completed exec-file operation inside this namespace. The facade
    /// deliberately exposes exact logical argv/cwd and raw output bytes, but no
    /// native projection, PID, handle, or alternate wait owner.
    pub async fn exec_file(
        &self,
        request: RuntimeExecFileRequest,
    ) -> Result<RuntimeExecFileOutput, RuntimeExecFileError> {
        let RuntimeExecFileRequest {
            surface,
            file,
            args,
            cwd,
            timeout,
        } = request;
        let mut spawn_request = RuntimeSpawnRequest::new(file)
            .with_args(args)
            .with_stdio(RuntimeStdio::Null, RuntimeStdio::Pipe, RuntimeStdio::Pipe)
            .with_hard_kill_on_drop(true)
            .with_surface_policy(surface.policy_id());
        if let Some(cwd) = cwd {
            spawn_request = spawn_request.with_cwd(cwd);
        }
        let mut child = self
            .spawner()
            .spawn(spawn_request)
            .await
            .map_err(RuntimeExecFileError::from_spawn)?;
        let mut stdout = child.take_stdout().ok_or_else(|| {
            RuntimeExecFileError::new(
                RuntimeExecFileErrorStage::Output,
                "runtime exec-file stdout pipe is unavailable",
            )
        })?;
        let mut stderr = child.take_stderr().ok_or_else(|| {
            RuntimeExecFileError::new(
                RuntimeExecFileErrorStage::Output,
                "runtime exec-file stderr pipe is unavailable",
            )
        })?;
        let collect = async move {
            let mut stdout_bytes = Vec::new();
            let mut stderr_bytes = Vec::new();
            let (stdout_result, stderr_result, status_result) = tokio::join!(
                stdout.read_to_end(&mut stdout_bytes),
                stderr.read_to_end(&mut stderr_bytes),
                child.wait(),
            );
            stdout_result.map_err(|error| {
                RuntimeExecFileError::new(
                    RuntimeExecFileErrorStage::Output,
                    format!("runtime exec-file stdout read failed: {error}"),
                )
            })?;
            stderr_result.map_err(|error| {
                RuntimeExecFileError::new(
                    RuntimeExecFileErrorStage::Output,
                    format!("runtime exec-file stderr read failed: {error}"),
                )
            })?;
            let status = status_result.map_err(RuntimeExecFileError::from_spawn)?;
            Ok(RuntimeExecFileOutput {
                status: RuntimeExecFileStatus {
                    code: status.code(),
                    signal: status.signal(),
                },
                stdout: stdout_bytes,
                stderr: stderr_bytes,
            })
        };
        tokio::time::timeout(timeout, collect).await.map_err(|_| {
            RuntimeExecFileError::new(
                RuntimeExecFileErrorStage::Timeout,
                format!(
                    "runtime exec-file timed out after {} ms",
                    timeout.as_millis()
                ),
            )
        })?
    }

    pub(crate) async fn exec_completed(
        &self,
        request: RuntimeCompletedProcessRequest,
    ) -> Result<RuntimeExecFileOutput, RuntimeExecFileError> {
        let RuntimeCompletedProcessRequest {
            surface,
            file,
            args,
            cwd,
            timeout,
            max_stdout_bytes,
        } = request;
        if !surface.supports_current_target() {
            return Err(RuntimeExecFileError::new(
                RuntimeExecFileErrorStage::Initialization,
                format!(
                    "runtime completed-process surface {} is a no-spawn surface on this target",
                    surface.policy_id().name()
                ),
            ));
        }
        if timeout.is_some() && !surface.allows_explicit_timeout() {
            return Err(RuntimeExecFileError::new(
                RuntimeExecFileErrorStage::Initialization,
                format!(
                    "runtime completed-process surface {} forbids an explicit timeout",
                    surface.policy_id().name()
                ),
            ));
        }
        if max_stdout_bytes.is_some() && !surface.allows_stdout_limit() {
            return Err(RuntimeExecFileError::new(
                RuntimeExecFileErrorStage::Initialization,
                format!(
                    "runtime completed-process surface {} forbids a stdout limit",
                    surface.policy_id().name()
                ),
            ));
        }
        if surface == RuntimeCompletedProcessSurface::ShellEnvSnapshot
            && max_stdout_bytes != Some(SHELL_ENV_SNAPSHOT_MAX_STDOUT_BYTES)
        {
            return Err(RuntimeExecFileError::new(
                RuntimeExecFileErrorStage::Initialization,
                format!(
                    "runtime completed-process surface {} requires the exact {}-byte stdout limit",
                    surface.policy_id().name(),
                    SHELL_ENV_SNAPSHOT_MAX_STDOUT_BYTES
                ),
            ));
        }
        let (stdout_stdio, stderr_stdio) = surface.output_stdio();
        let mut spawn_request = RuntimeSpawnRequest::new(file)
            .with_args(args)
            .with_stdio(RuntimeStdio::Null, stdout_stdio, stderr_stdio)
            .with_hard_kill_on_drop(!surface.keep_alive_on_drop())
            .with_surface_policy(surface.policy_id());
        if surface.uses_node20_image() {
            spawn_request.image_dialect = ProcessImageDialect::Node20Libuv146;
            spawn_request.environment_dialect = EnvironmentImageDialect::Node20ParentSnapshot;
            spawn_request.environment_guard = ProcessEnvironmentGuard::None;
        }
        if let Some(cwd) = cwd {
            spawn_request = spawn_request.with_cwd(cwd);
        }
        let mut child = self
            .spawner()
            .spawn(spawn_request)
            .await
            .map_err(RuntimeExecFileError::from_spawn)?;
        let child_stdin = child.take_stdin();
        let stdout = child.take_stdout();
        let stderr = child.take_stderr();
        let collect = async move {
            drop(child_stdin);
            let read_stdout = async move {
                let Some(reader) = stdout else {
                    return Ok::<_, std::io::Error>((Vec::new(), false));
                };
                let mut bytes = Vec::new();
                if let Some(limit) = max_stdout_bytes {
                    let read_cap = limit
                        .checked_add(1)
                        .expect("completed-process stdout limit cannot overflow");
                    let mut limited = reader.take(read_cap as u64);
                    limited.read_to_end(&mut bytes).await?;
                    let overflowed = bytes.len() > limit;
                    Ok((bytes, overflowed))
                } else {
                    let mut reader = reader;
                    reader.read_to_end(&mut bytes).await?;
                    Ok((bytes, false))
                }
            };
            let read_stderr = async move {
                let Some(mut reader) = stderr else {
                    return Ok::<_, std::io::Error>(Vec::new());
                };
                let mut bytes = Vec::new();
                reader.read_to_end(&mut bytes).await?;
                Ok(bytes)
            };
            let (stdout_result, stderr_result, status_result) =
                tokio::join!(read_stdout, read_stderr, child.wait());
            let (stdout_bytes, stdout_overflowed) = stdout_result.map_err(|error| {
                RuntimeExecFileError::new(
                    RuntimeExecFileErrorStage::Output,
                    format!("runtime completed process stdout read failed: {error}"),
                )
            })?;
            let stderr_bytes = stderr_result.map_err(|error| {
                RuntimeExecFileError::new(
                    RuntimeExecFileErrorStage::Output,
                    format!("runtime completed process stderr read failed: {error}"),
                )
            })?;
            let status = status_result.map_err(RuntimeExecFileError::from_spawn)?;
            if stdout_overflowed {
                return Err(RuntimeExecFileError::new(
                    RuntimeExecFileErrorStage::Output,
                    format!(
                        "runtime completed process stdout exceeded the exact {}-byte limit",
                        max_stdout_bytes.expect("overflow requires a configured stdout limit")
                    ),
                ));
            }
            Ok(RuntimeExecFileOutput {
                status: RuntimeExecFileStatus {
                    code: status.code(),
                    signal: status.signal(),
                },
                stdout: stdout_bytes,
                stderr: stderr_bytes,
            })
        };
        match timeout {
            Some(timeout) => tokio::time::timeout(timeout, collect).await.map_err(|_| {
                RuntimeExecFileError::new(
                    RuntimeExecFileErrorStage::Timeout,
                    format!(
                        "runtime completed process timed out after {} ms",
                        timeout.as_millis()
                    ),
                )
            })?,
            None => collect.await,
        }
    }

    pub(crate) async fn spawn_oauth_opener(
        &self,
        file: RuntimeString,
        args: Vec<RuntimeString>,
    ) -> Result<SpawnedRuntimeProcess, RuntimeSpawnError> {
        self.spawner()
            .spawn(
                RuntimeSpawnRequest::new(file)
                    .with_args(args)
                    .with_stdio(RuntimeStdio::Null, RuntimeStdio::Null, RuntimeStdio::Null)
                    .with_surface_policy(ProcessSurfacePolicyId::TsOAuthOpenerExecFile),
            )
            .await
    }

    pub async fn spawn_browser_sidecar(
        &self,
        request: RuntimeBrowserSidecarRequest,
    ) -> Result<RuntimePipeProcess, RuntimeProcessPipeError> {
        use rand::RngCore;

        let generation = self.reserve_browser_generation()?;
        let mut package_ready_challenge = [0_u8; 32];
        rand::rngs::OsRng
            .try_fill_bytes(&mut package_ready_challenge)
            .map_err(|_| RuntimeProcessPipeError::new("BrowserBootstrapEntropyUnavailable"))?;
        let process_binding = build_browser_process_binding(&request);
        let spawn_request = build_browser_sidecar_spawn_request(request);
        #[cfg(windows)]
        let spawn_request = {
            let mut spawn_request = spawn_request;
            spawn_request.windows_browser_bootstrap_seed = Some(
                WindowsBrowserBootstrapSeed::generate(generation)
                    .map_err(|error| RuntimeProcessPipeError::new(error.stable_message()))?,
            );
            spawn_request
        };
        let inner = self
            .spawner()
            .spawn(spawn_request)
            .await
            .map_err(RuntimeProcessPipeError::from_spawn)?;
        #[cfg(windows)]
        let mut inner = inner;
        #[cfg(windows)]
        let (windows_bootstrap_nonce, windows_client_pid, windows_bootstrap_deadline) = {
            let deadline = Instant::now()
                .checked_add(Duration::from_millis(5_000))
                .ok_or_else(|| {
                    RuntimeProcessPipeError::new("browser bootstrap deadline overflow")
                })?;
            let (nonce, client_pid) = match inner.connect_windows_browser_broker(deadline).await {
                Ok(connected) => connected,
                Err(error) => {
                    // Whatever the child managed to say before it stopped connecting. A bare
                    // "bootstrap timed out" describes the host's wait and nothing about the
                    // sidecar, and the sidecar is where every cause of this lives: a missing
                    // addon, a rejected bootstrap, a thrown import.
                    let said = Self::drain_child_stderr_for_diagnosis(inner.take_stderr()).await;
                    let message = if said.is_empty() {
                        error.stable_message().to_string()
                    } else {
                        format!("{}; the sidecar said: {said}", error.stable_message())
                    };
                    return Err(RuntimeProcessPipeError::new(message));
                }
            };
            (nonce, client_pid, deadline)
        };
        Ok(RuntimePipeProcess::new(
            inner,
            generation,
            package_ready_challenge,
            process_binding,
            #[cfg(windows)]
            windows_bootstrap_nonce,
            #[cfg(windows)]
            windows_client_pid,
            #[cfg(windows)]
            windows_bootstrap_deadline,
        ))
    }

    /// Read whatever a child has already written to stderr, for a failure message.
    ///
    /// Bounded and deadline-capped: this runs on a path that has already failed, so it must
    /// not add a wait of its own, and a runaway child must not be able to grow the message.
    #[cfg(windows)]
    async fn drain_child_stderr_for_diagnosis(stderr: Option<RuntimeChildStderr>) -> String {
        use tokio::io::AsyncReadExt;

        const LIMIT: usize = 4096;
        let Some(mut stderr) = stderr else {
            return String::new();
        };
        let mut buffer = Vec::new();
        let _ = tokio::time::timeout(Duration::from_millis(250), async {
            let mut chunk = [0_u8; 512];
            while buffer.len() < LIMIT {
                match stderr.read(&mut chunk).await {
                    Ok(0) | Err(_) => break,
                    Ok(count) => buffer.extend_from_slice(&chunk[..count]),
                }
            }
        })
        .await;
        String::from_utf8_lossy(&buffer).trim().to_string()
    }

    pub async fn spawn_browser_playwright_child(
        &self,
        binding: &RuntimeBrowserProcessBinding,
        request: RuntimeBrowserPlaywrightLaunchRequest,
    ) -> Result<RuntimeBrowserChildProcess, RuntimeProcessPipeError> {
        let RuntimeBrowserPlaywrightLaunchRequest {
            command,
            args,
            mode,
            detached,
            cwd,
            environment,
        } = request;
        let mut spawn_request = RuntimeSpawnRequest::new(command)
            .with_args(args)
            .with_stdio(RuntimeStdio::Null, RuntimeStdio::Pipe, RuntimeStdio::Pipe)
            .with_surface_policy(mode.policy_id());
        spawn_request.env = Some(environment.unwrap_or_else(|| binding.environment.clone()));
        spawn_request.cwd = cwd
            .map(RuntimePath::into_runtime_process_atom)
            .or_else(|| binding.cwd.clone().map(ProcessAtom::Native));
        spawn_request.image_dialect = ProcessImageDialect::Node20Libuv146;
        spawn_request.environment_dialect = EnvironmentImageDialect::Node20ParentSnapshot;
        spawn_request.environment_guard = ProcessEnvironmentGuard::None;
        spawn_request.topology = if detached {
            if cfg!(windows) {
                ProcessTopology::WindowsDetached
            } else {
                ProcessTopology::UnixNewSession
            }
        } else {
            ProcessTopology::Inherit
        };
        spawn_request.kill_target = if cfg!(windows) {
            KillTarget::WindowsProcessTree
        } else if detached {
            KillTarget::UnixProcessGroup
        } else {
            KillTarget::Child
        };
        spawn_request.drop_action = RuntimeDropAction::HardKill(spawn_request.kill_target);
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            spawn_request.unix_extra_stdio = vec![
                UnixExtraStdioPlan::new(3, UnixExtraPipeDirection::Duplex),
                UnixExtraStdioPlan::new(4, UnixExtraPipeDirection::Duplex),
            ];
        }
        #[cfg(windows)]
        {
            spawn_request.windows_hide = true;
            spawn_request.windows_job_policy = WindowsJobPolicy::LibuvGlobal;
        }
        self.spawner()
            .spawn(spawn_request)
            .await
            .map(RuntimeBrowserChildProcess::new)
            .map_err(RuntimeProcessPipeError::from_spawn)
    }

    pub async fn spawn_browser_ffmpeg_child(
        &self,
        binding: &RuntimeBrowserProcessBinding,
        request: RuntimeBrowserFfmpegEncodeRequest,
    ) -> Result<RuntimeBrowserChildProcess, RuntimeProcessPipeError> {
        let RuntimeBrowserFfmpegEncodeRequest {
            list_path,
            file_path,
            fps,
        } = request;
        let ffmpeg_key = RuntimeString::from_utf8("KLAUS_BROWSER_FFMPEG");
        let command = binding
            .environment
            .get(&ffmpeg_key)
            .filter(|value| !value.is_empty())
            .cloned()
            .unwrap_or_else(|| RuntimeString::from_utf8("ffmpeg"));
        let args = vec![
            RuntimeString::from_utf8("-y"),
            RuntimeString::from_utf8("-hide_banner"),
            RuntimeString::from_utf8("-loglevel"),
            RuntimeString::from_utf8("error"),
            RuntimeString::from_utf8("-f"),
            RuntimeString::from_utf8("concat"),
            RuntimeString::from_utf8("-safe"),
            RuntimeString::from_utf8("0"),
            RuntimeString::from_utf8("-i"),
            list_path,
            RuntimeString::from_utf8("-vf"),
            RuntimeString::from_utf8(&format!("fps={fps},scale=trunc(iw/2)*2:trunc(ih/2)*2")),
            RuntimeString::from_utf8("-loop"),
            RuntimeString::from_utf8("0"),
            file_path,
        ];
        let mut spawn_request = RuntimeSpawnRequest::new(command)
            .with_args(args)
            .with_stdio(RuntimeStdio::Null, RuntimeStdio::Null, RuntimeStdio::Pipe)
            .with_surface_policy(ProcessSurfacePolicyId::BrowserFfmpegEncode30s);
        spawn_request.env = Some(binding.environment.clone());
        spawn_request.cwd = binding.cwd.clone().map(ProcessAtom::Native);
        spawn_request.image_dialect = ProcessImageDialect::Node20Libuv146;
        spawn_request.environment_dialect = EnvironmentImageDialect::Node20ParentSnapshot;
        spawn_request.environment_guard = ProcessEnvironmentGuard::None;
        spawn_request.kill_target = KillTarget::Child;
        spawn_request.drop_action = RuntimeDropAction::HardKill(KillTarget::Child);
        #[cfg(windows)]
        {
            spawn_request.windows_hide = true;
            spawn_request.windows_job_policy = WindowsJobPolicy::LibuvGlobal;
        }
        self.spawner()
            .spawn(spawn_request)
            .await
            .map(RuntimeBrowserChildProcess::new)
            .map_err(RuntimeProcessPipeError::from_spawn)
    }

    /// Execute one of the five closed Playwright broker operations represented
    /// by synchronous wire kinds 2 through 6. Executable, argv, stdio,
    /// topology, capture, and lifecycle policy are selected exclusively in
    /// Core; the request carries only operation-specific typed values.
    pub async fn execute_browser_broker_process(
        &self,
        binding: &RuntimeBrowserProcessBinding,
        operation: RuntimeBrowserBrokerProcessRequest,
    ) -> Result<RuntimeExecFileOutput, RuntimeExecFileError> {
        let operation = match operation {
            RuntimeBrowserBrokerProcessRequest::PlaywrightTreeKill(control) => {
                return control.execute_playwright_tree_kill().await;
            }
            operation => operation,
        };

        let plan = BrowserBrokerProcessSpawnPlan::from_operation(binding, operation)?;
        let BrowserBrokerProcessSpawnPlan {
            request,
            stdin_is_pipe,
            stdout_is_pipe,
            stderr_is_pipe,
            stdout_limit,
        } = plan;
        let mut child = self
            .spawner()
            .spawn(request)
            .await
            .map_err(RuntimeExecFileError::from_spawn)?;
        let child_stdin = child.take_stdin();
        let stdout = child.take_stdout();
        let stderr = child.take_stderr();
        if child_stdin.is_some() != stdin_is_pipe
            || stdout.is_some() != stdout_is_pipe
            || stderr.is_some() != stderr_is_pipe
        {
            return Err(RuntimeExecFileError::new(
                RuntimeExecFileErrorStage::Output,
                "browser broker process returned an invalid stdio receipt",
            ));
        }

        // Node's spawn-based dependency probes create a stdin pipe and close
        // the parent endpoint without writing. Dropping it here delivers EOF.
        drop(child_stdin);
        let stdout_read = read_browser_broker_process_output(stdout, "stdout", stdout_limit);
        let stderr_read = read_browser_broker_process_output(stderr, "stderr", None);
        let (stdout_result, stderr_result, status_result) =
            tokio::join!(stdout_read, stderr_read, child.wait());
        let stdout = stdout_result?;
        let stderr = stderr_result?;
        let status = status_result.map_err(RuntimeExecFileError::from_spawn)?;
        Ok(RuntimeExecFileOutput {
            status: RuntimeExecFileStatus::from(status),
            stdout,
            stderr,
        })
    }

    /// Execute one fixed browser utility probe or registration operation. The
    /// public input is a closed enum; executable and argv are derived here.
    pub async fn execute_browser_utility_process(
        &self,
        operation: RuntimeBrowserUtilityProcess,
    ) -> Result<RuntimeExecFileOutput, RuntimeExecFileError> {
        let BrowserUtilitySpawnPlan { file, args, mode } = operation.spawn_plan()?;
        let (stdout_disposition, stderr_disposition, policy) = match mode {
            BrowserUtilitySpawnMode::RunningProbe => (
                RuntimeStdio::Pipe,
                RuntimeStdio::Pipe,
                ProcessSurfacePolicyId::BrowserRunningProbe,
            ),
            BrowserUtilitySpawnMode::QuietProbe => (
                RuntimeStdio::Pipe,
                RuntimeStdio::Null,
                ProcessSurfacePolicyId::BrowserProbeQuiet,
            ),
            BrowserUtilitySpawnMode::NativeHostRegistration => (
                RuntimeStdio::Null,
                RuntimeStdio::Null,
                ProcessSurfacePolicyId::BrowserNativeHostRegister,
            ),
        };
        let mut request = RuntimeSpawnRequest::new(RuntimeString::from_utf8("<browser-utility>"));
        request.file = file;
        request.args = args;
        request = request
            .with_stdio(RuntimeStdio::Null, stdout_disposition, stderr_disposition)
            .with_hard_kill_on_drop(false)
            .with_surface_policy(policy);
        request.image_dialect = ProcessImageDialect::Node20Libuv146;
        request.environment_dialect = EnvironmentImageDialect::Node20ParentSnapshot;
        request.environment_guard = ProcessEnvironmentGuard::None;
        #[cfg(windows)]
        {
            request.windows_job_policy = WindowsJobPolicy::LibuvGlobal;
            request.windows_hide = true;
        }
        let mut child = self
            .spawner()
            .spawn(request)
            .await
            .map_err(RuntimeExecFileError::from_spawn)?;
        let stdout = child.take_stdout();
        let stderr = child.take_stderr();
        if stdout.is_some() != (stdout_disposition == RuntimeStdio::Pipe)
            || stderr.is_some() != (stderr_disposition == RuntimeStdio::Pipe)
        {
            return Err(RuntimeExecFileError::new(
                RuntimeExecFileErrorStage::Output,
                "browser utility returned an invalid stdio receipt",
            ));
        }
        let collect = async move {
            let stdout_read = read_browser_utility_output(stdout, "stdout");
            let stderr_read = read_browser_utility_output(stderr, "stderr");
            let (stdout_result, stderr_result, status_result) =
                tokio::join!(stdout_read, stderr_read, child.wait(),);
            let stdout_bytes = stdout_result?;
            let stderr_bytes = stderr_result?;
            if stdout_bytes.len().saturating_add(stderr_bytes.len())
                > BROWSER_UTILITY_MAX_CAPTURE_BYTES
            {
                return Err(RuntimeExecFileError::new(
                    RuntimeExecFileErrorStage::Output,
                    "browser utility output exceeded the shared 1048576-byte limit",
                ));
            }
            let status = status_result.map_err(RuntimeExecFileError::from_spawn)?;
            Ok(RuntimeExecFileOutput {
                status: RuntimeExecFileStatus::from(status),
                stdout: stdout_bytes,
                stderr: stderr_bytes,
            })
        };
        collect.await
    }

    /// Commit one detached Chrome launch from a target-specific closed request.
    /// Creation success is the receipt; after commit the browser is deliberately
    /// disarmed from this process namespace, matching Node's `unref()` surface.
    pub async fn spawn_browser_open_chrome(
        &self,
        request: RuntimeBrowserOpenChromeRequest,
    ) -> Result<(), RuntimeProcessPipeError> {
        let request = request.into_spawn_request()?;
        let mut child = self
            .spawner()
            .spawn(request)
            .await
            .map_err(RuntimeProcessPipeError::from_spawn)?;
        child.disarm();
        Ok(())
    }

    pub(crate) async fn spawn_ripgrep(
        &self,
        command: OsString,
        args: Vec<RuntimeString>,
        targets: Vec<PathBuf>,
        single_thread: bool,
    ) -> Result<SpawnedRuntimeProcess, RuntimeSpawnError> {
        let mut process_args = Vec::new();
        if single_thread {
            process_args.push(ProcessAtom::Runtime(RuntimeString::from_utf8("-j")));
            process_args.push(ProcessAtom::Runtime(RuntimeString::from_utf8("1")));
        }
        process_args.extend(args.into_iter().map(ProcessAtom::Runtime));
        // ripgrep accepts multiple trailing positional path args and searches all
        // of them, so a multi-folder workspace's roots become one native search
        // (results stay mtime-sorted and correctly prefixed by ripgrep itself).
        for target in &targets {
            process_args.push(ProcessAtom::Native(NativeOsString::from_host_os_str(
                target.as_os_str(),
            )));
        }
        let mut request = RuntimeSpawnRequest::new(RuntimeString::from_utf8("<native>"));
        request.file = ProcessAtom::Native(NativeOsString::from_host_os_str(&command));
        request.args = process_args;
        request = request
            .with_stdio(RuntimeStdio::Null, RuntimeStdio::Pipe, RuntimeStdio::Pipe)
            .with_hard_kill_on_drop(true)
            .with_surface_policy(ProcessSurfacePolicyId::TsRipgrepExecFile);
        self.spawner().spawn(request).await
    }

    pub(crate) async fn spawn_scoped_ripgrep(
        &self,
        spec: crate::types::sandbox::SpawnSpec,
    ) -> Result<SpawnedRuntimeProcess, RuntimeSpawnError> {
        let mut request = RuntimeSpawnRequest::new(spec.file)
            .with_args(spec.args)
            .with_cwd(spec.cwd)
            .with_stdio(RuntimeStdio::Null, RuntimeStdio::Pipe, RuntimeStdio::Pipe)
            .with_hard_kill_on_drop(true)
            .with_surface_policy(ProcessSurfacePolicyId::TsRipgrepExecFile);
        request.env = Some(spec.env);
        request.image_dialect = ProcessImageDialect::Node20Libuv146;
        request.environment_dialect = EnvironmentImageDialect::Node20ParentSnapshot;
        request.environment_guard = ProcessEnvironmentGuard::None;
        self.spawner().spawn(request).await
    }

    pub(crate) async fn spawn_lsp(
        &self,
        command: RuntimeString,
        args: Vec<RuntimeString>,
        root_dir: RuntimePath,
    ) -> Result<SpawnedRuntimeProcess, RuntimeSpawnError> {
        let mut request = RuntimeSpawnRequest::new(command)
            .with_args(args)
            .with_cwd(root_dir)
            .with_stdio(RuntimeStdio::Pipe, RuntimeStdio::Pipe, RuntimeStdio::Pipe)
            .with_surface_policy(ProcessSurfacePolicyId::TsLsp);
        request.image_dialect = ProcessImageDialect::Node20Libuv146;
        request.environment_dialect = EnvironmentImageDialect::Node20ParentSnapshot;
        request.environment_guard = ProcessEnvironmentGuard::None;
        request.windows_job_policy = if cfg!(windows) {
            WindowsJobPolicy::LibuvGlobal
        } else {
            WindowsJobPolicy::None
        };
        request.topology = if cfg!(windows) {
            ProcessTopology::Inherit
        } else {
            ProcessTopology::UnixNewSession
        };
        request.kill_target = if cfg!(windows) {
            KillTarget::WindowsProcessTree
        } else {
            KillTarget::UnixProcessGroup
        };
        #[cfg(windows)]
        {
            request.windows_hide = true;
        }
        self.spawner().spawn(request).await
    }

    /// Launch the code-mode host: a long-lived stdio peer that runs cells on
    /// this engine's behalf. Hard-killed with the context so a host cannot
    /// outlive the engine that owns it. The host inherits this process's
    /// environment, which is what the Rust image surface provides.
    pub(crate) async fn spawn_code_mode_host(
        &self,
        host_program: &std::path::Path,
    ) -> Result<SpawnedRuntimeProcess, RuntimeSpawnError> {
        let request =
            RuntimeSpawnRequest::new(RuntimeString::from_utf8(&host_program.to_string_lossy()))
                .with_stdio(RuntimeStdio::Pipe, RuntimeStdio::Pipe, RuntimeStdio::Pipe)
                .with_hard_kill_on_drop(true)
                .with_surface_policy(ProcessSurfacePolicyId::RustCodeModeHost);
        self.spawner().spawn(request).await
    }

    pub(crate) async fn spawn_mcp_stdio(
        &self,
        command: RuntimeString,
        args: Vec<RuntimeString>,
        env_overlay: Option<RuntimeEnv>,
    ) -> Result<SpawnedRuntimeProcess, RuntimeSpawnError> {
        let mut environment = ParentEnvSnapshot::capture();
        if let Some(env_overlay) = env_overlay {
            environment.overlay(&env_overlay);
        }
        let mut request = RuntimeSpawnRequest::new(command)
            .with_args(args)
            .with_stdio(RuntimeStdio::Pipe, RuntimeStdio::Pipe, RuntimeStdio::Pipe)
            .with_surface_policy(ProcessSurfacePolicyId::TsMcpStdio);
        request.env = Some(environment);
        request.image_dialect = ProcessImageDialect::Node20Libuv146;
        request.environment_dialect = EnvironmentImageDialect::Node20ParentSnapshot;
        request.environment_guard = ProcessEnvironmentGuard::None;
        self.spawner().spawn(request).await
    }
}

/// Closed browser-utility process operations. Each variant expands to one
/// fixed executable/argv recipe inside Core; callers cannot supply a generic
/// command, argument vector, environment, stdio shape, or process policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeBrowserUtilityProcess {
    WindowsTasklistChrome,
    MacOsPsChrome,
    LinuxPsChrome,
    MacOsMdfindChrome,
    MacOsPlutilInfo { plist_path: RuntimePath },
    WindowsAppPathChromeCurrentUser,
    WindowsAppPathChromeLocalMachine,
    WindowsAppPathChromeWow6432,
    WindowsChromeVersionCurrentUser,
    WindowsChromeVersionLocalMachine,
    WindowsWhereChrome,
    LinuxWhichGoogleChrome,
    LinuxWhichChromium,
    LinuxWhichChromiumBrowser,
    WindowsRegisterNativeHost { manifest_path: RuntimePath },
}

const BROWSER_UTILITY_MAX_CAPTURE_BYTES: usize = 1024 * 1024;

async fn read_browser_utility_output<R>(
    reader: Option<R>,
    stream: &'static str,
) -> Result<Vec<u8>, RuntimeExecFileError>
where
    R: AsyncRead + Unpin,
{
    let Some(reader) = reader else {
        return Ok(Vec::new());
    };
    let mut reader = reader.take((BROWSER_UTILITY_MAX_CAPTURE_BYTES + 1) as u64);
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await.map_err(|error| {
        RuntimeExecFileError::new(
            RuntimeExecFileErrorStage::Output,
            format!("browser utility {stream} read failed: {error}"),
        )
    })?;
    if bytes.len() > BROWSER_UTILITY_MAX_CAPTURE_BYTES {
        return Err(RuntimeExecFileError::new(
            RuntimeExecFileErrorStage::Output,
            format!("browser utility {stream} exceeded the 1048576-byte limit"),
        ));
    }
    Ok(bytes)
}

struct BrowserUtilitySpawnPlan {
    file: ProcessAtom,
    args: Vec<ProcessAtom>,
    mode: BrowserUtilitySpawnMode,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BrowserUtilitySpawnMode {
    RunningProbe,
    QuietProbe,
    NativeHostRegistration,
}

impl RuntimeBrowserUtilityProcess {
    fn spawn_plan(self) -> Result<BrowserUtilitySpawnPlan, RuntimeExecFileError> {
        let target_matches = match &self {
            Self::WindowsTasklistChrome
            | Self::WindowsAppPathChromeCurrentUser
            | Self::WindowsAppPathChromeLocalMachine
            | Self::WindowsAppPathChromeWow6432
            | Self::WindowsChromeVersionCurrentUser
            | Self::WindowsChromeVersionLocalMachine
            | Self::WindowsWhereChrome
            | Self::WindowsRegisterNativeHost { .. } => cfg!(windows),
            Self::MacOsPsChrome | Self::MacOsMdfindChrome | Self::MacOsPlutilInfo { .. } => {
                cfg!(target_os = "macos")
            }
            Self::LinuxPsChrome
            | Self::LinuxWhichGoogleChrome
            | Self::LinuxWhichChromium
            | Self::LinuxWhichChromiumBrowser => cfg!(target_os = "linux"),
        };
        if !target_matches {
            return Err(RuntimeExecFileError::new(
                RuntimeExecFileErrorStage::Initialization,
                "browser utility operation is unavailable on this target",
            ));
        }

        let runtime = |value: &str| ProcessAtom::Runtime(RuntimeString::from_utf8(value));
        let mode = match &self {
            Self::WindowsTasklistChrome | Self::MacOsPsChrome | Self::LinuxPsChrome => {
                BrowserUtilitySpawnMode::RunningProbe
            }
            Self::WindowsRegisterNativeHost { .. } => {
                BrowserUtilitySpawnMode::NativeHostRegistration
            }
            _ => BrowserUtilitySpawnMode::QuietProbe,
        };
        let (file, args) = match self {
            Self::WindowsTasklistChrome => (
                runtime("tasklist"),
                ["/fo", "csv", "/nh", "/fi", "imagename eq chrome.exe"]
                    .into_iter()
                    .map(runtime)
                    .collect(),
            ),
            Self::MacOsPsChrome => (
                runtime("ps"),
                ["-A", "-ww", "-o", "pid=", "-o", "command="]
                    .into_iter()
                    .map(runtime)
                    .collect(),
            ),
            Self::LinuxPsChrome => (
                runtime("ps"),
                ["-A", "-o", "pid=", "-o", "comm="]
                    .into_iter()
                    .map(runtime)
                    .collect(),
            ),
            Self::MacOsMdfindChrome => (
                runtime("mdfind"),
                vec![runtime("kMDItemCFBundleIdentifier == 'com.google.Chrome'")],
            ),
            Self::MacOsPlutilInfo { plist_path } => (
                runtime("plutil"),
                vec![
                    runtime("-convert"),
                    runtime("json"),
                    runtime("-o"),
                    runtime("-"),
                    plist_path.into_runtime_process_atom(),
                ],
            ),
            Self::WindowsAppPathChromeCurrentUser => windows_registry_query(
                "HKCU\\Software\\Microsoft\\Windows\\CurrentVersion\\App Paths\\chrome.exe",
                None,
            ),
            Self::WindowsAppPathChromeLocalMachine => windows_registry_query(
                "HKLM\\Software\\Microsoft\\Windows\\CurrentVersion\\App Paths\\chrome.exe",
                None,
            ),
            Self::WindowsAppPathChromeWow6432 => windows_registry_query(
                "HKLM\\Software\\WOW6432Node\\Microsoft\\Windows\\CurrentVersion\\App Paths\\chrome.exe",
                None,
            ),
            Self::WindowsChromeVersionCurrentUser => windows_registry_query(
                "HKCU\\Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\Google Chrome",
                Some("DisplayVersion"),
            ),
            Self::WindowsChromeVersionLocalMachine => windows_registry_query(
                "HKLM\\Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\Google Chrome",
                Some("DisplayVersion"),
            ),
            Self::WindowsWhereChrome => (runtime("where"), vec![runtime("chrome")]),
            Self::LinuxWhichGoogleChrome => {
                (runtime("which"), vec![runtime("google-chrome")])
            }
            Self::LinuxWhichChromium => (runtime("which"), vec![runtime("chromium")]),
            Self::LinuxWhichChromiumBrowser => {
                (runtime("which"), vec![runtime("chromium-browser")])
            }
            Self::WindowsRegisterNativeHost { manifest_path } => (
                runtime("reg"),
                vec![
                    runtime("add"),
                    runtime("HKCU\\Software\\Google\\Chrome\\NativeMessagingHosts\\com.klaus.browser"),
                    runtime("/ve"),
                    runtime("/t"),
                    runtime("REG_SZ"),
                    runtime("/d"),
                    manifest_path.into_runtime_process_atom(),
                    runtime("/f"),
                ],
            ),
        };
        Ok(BrowserUtilitySpawnPlan { file, args, mode })
    }
}

fn windows_registry_query(key: &str, value: Option<&str>) -> (ProcessAtom, Vec<ProcessAtom>) {
    let runtime = |value: &str| ProcessAtom::Runtime(RuntimeString::from_utf8(value));
    let mut args = vec![runtime("query"), runtime(key)];
    match value {
        Some(value) => {
            args.push(runtime("/v"));
            args.push(runtime(value));
        }
        None => args.push(runtime("/ve")),
    }
    (runtime("reg"), args)
}

/// Closed detached Chrome launch. Core derives command shape, argv order,
/// topology, stdio, environment, and ownership from the target-specific
/// constructor rather than accepting a generic spawn record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeBrowserOpenChromeRequest {
    target: RuntimeBrowserOpenChromeTarget,
    user_data_directory: Option<RuntimePath>,
    profile_directory: RuntimeString,
    url: RuntimeString,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RuntimeBrowserOpenChromeTarget {
    MacOsApplication(RuntimePath),
    WindowsExecutable(RuntimePath),
    LinuxExecutable(RuntimePath),
}

impl RuntimeBrowserOpenChromeRequest {
    pub fn macos(
        application: RuntimePath,
        user_data_directory: Option<RuntimePath>,
        profile_directory: RuntimeString,
        url: RuntimeString,
    ) -> Self {
        Self {
            target: RuntimeBrowserOpenChromeTarget::MacOsApplication(application),
            user_data_directory,
            profile_directory,
            url,
        }
    }

    pub fn windows(
        executable: RuntimePath,
        user_data_directory: Option<RuntimePath>,
        profile_directory: RuntimeString,
        url: RuntimeString,
    ) -> Self {
        Self {
            target: RuntimeBrowserOpenChromeTarget::WindowsExecutable(executable),
            user_data_directory,
            profile_directory,
            url,
        }
    }

    pub fn linux(
        executable: RuntimePath,
        user_data_directory: Option<RuntimePath>,
        profile_directory: RuntimeString,
        url: RuntimeString,
    ) -> Self {
        Self {
            target: RuntimeBrowserOpenChromeTarget::LinuxExecutable(executable),
            user_data_directory,
            profile_directory,
            url,
        }
    }

    fn into_spawn_request(self) -> Result<RuntimeSpawnRequest, RuntimeProcessPipeError> {
        let target_matches = match &self.target {
            RuntimeBrowserOpenChromeTarget::MacOsApplication(_) => cfg!(target_os = "macos"),
            RuntimeBrowserOpenChromeTarget::WindowsExecutable(_) => cfg!(windows),
            RuntimeBrowserOpenChromeTarget::LinuxExecutable(_) => cfg!(target_os = "linux"),
        };
        if !target_matches {
            return Err(RuntimeProcessPipeError::new(
                "browser Chrome launch target does not match the host platform",
            ));
        }
        let mut args = Vec::new();
        let file = match self.target {
            RuntimeBrowserOpenChromeTarget::MacOsApplication(application) => {
                args.extend([
                    ProcessAtom::Runtime(RuntimeString::from_utf8("-a")),
                    application.into_runtime_process_atom(),
                    ProcessAtom::Runtime(RuntimeString::from_utf8("--args")),
                ]);
                ProcessAtom::Runtime(RuntimeString::from_utf8("open"))
            }
            RuntimeBrowserOpenChromeTarget::WindowsExecutable(executable)
            | RuntimeBrowserOpenChromeTarget::LinuxExecutable(executable) => {
                executable.into_runtime_process_atom()
            }
        };
        if let Some(user_data_directory) = self.user_data_directory {
            let mut value = RuntimeString::from_utf8("--user-data-dir=")
                .as_utf16()
                .to_vec();
            value.extend_from_slice(user_data_directory.as_runtime_string().as_utf16());
            args.push(ProcessAtom::Runtime(RuntimeString::from_utf16(value)));
        }
        let mut profile = RuntimeString::from_utf8("--profile-directory=")
            .as_utf16()
            .to_vec();
        profile.extend_from_slice(self.profile_directory.as_utf16());
        args.push(ProcessAtom::Runtime(RuntimeString::from_utf16(profile)));
        args.push(ProcessAtom::Runtime(self.url));

        let mut request = RuntimeSpawnRequest::new(RuntimeString::from_utf8("<browser-chrome>"));
        request.file = file;
        request.args = args;
        request = request
            .with_stdio(RuntimeStdio::Null, RuntimeStdio::Null, RuntimeStdio::Null)
            .with_surface_policy(ProcessSurfacePolicyId::BrowserDetachedChromeLaunch);
        request.image_dialect = ProcessImageDialect::Node20Libuv146;
        request.environment_dialect = EnvironmentImageDialect::Node20ParentSnapshot;
        request.environment_guard = ProcessEnvironmentGuard::None;
        request.topology = if cfg!(windows) {
            ProcessTopology::WindowsDetached
        } else {
            ProcessTopology::UnixNewSession
        };
        request.kill_target = KillTarget::Child;
        request.drop_action = RuntimeDropAction::KeepAlive;
        #[cfg(windows)]
        {
            request.windows_hide = false;
            request.windows_job_policy = WindowsJobPolicy::None;
        }
        Ok(request)
    }
}

/// Opaque environment/cwd snapshot bound to one committed browser sidecar.
/// It is minted only by Core and can be reused only by the closed browser
/// child operations below.
#[derive(Clone)]
pub struct RuntimeBrowserProcessBinding {
    environment: RuntimeEnv,
    cwd: Option<NativeOsString>,
}

impl fmt::Debug for RuntimeBrowserProcessBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeBrowserProcessBinding")
            .field("environment_entries", &self.environment.len())
            .field("has_cwd", &self.cwd.is_some())
            .finish()
    }
}

fn build_browser_process_binding(
    request: &RuntimeBrowserSidecarRequest,
) -> RuntimeBrowserProcessBinding {
    let mut environment = ParentEnvSnapshot::capture();
    environment.insert(
        RuntimeString::from_utf8("KLAUS_BROWSER_HEADLESS"),
        RuntimeString::from_utf8(if request.headless { "1" } else { "0" }),
    );
    for (key, value) in &request.env {
        environment.insert(key.clone(), value.clone());
    }
    RuntimeBrowserProcessBinding {
        environment,
        cwd: request
            .cwd
            .as_ref()
            .map(|cwd| NativeOsString::from_host_os_str(cwd.as_os_str())),
    }
}

/// Closed Core operation set for Playwright broker wire kinds 2 through 6.
/// There is deliberately no generic executable/argv variant.
#[derive(Clone, Debug)]
pub enum RuntimeBrowserBrokerProcessRequest {
    PlaywrightTreeKill(RuntimeBrowserChildControl),
    PlaywrightMacOsSwVers,
    PlaywrightLinuxLdd(RuntimeBrowserLinuxLddRequest),
    PlaywrightLinuxLdconfig,
    PlaywrightWindowsWinldd(RuntimeBrowserWindowsWinlddRequest),
}

/// Typed operands for Playwright's Linux `ldd` dependency probe. Core derives
/// both the fixed command and the exact parent-environment overlay.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeBrowserLinuxLddRequest {
    file: RuntimePath,
    cwd: RuntimePath,
    ld_library_path: RuntimeString,
}

impl RuntimeBrowserLinuxLddRequest {
    pub fn new(file: RuntimePath, cwd: RuntimePath, ld_library_path: RuntimeString) -> Self {
        Self {
            file,
            cwd,
            ld_library_path,
        }
    }
}

/// Branded path for the distribution-verified Playwright winldd executable.
/// The outer distribution verifier owns the exact artifact/receipt/path/size/
/// digest checks; Core additionally rejects a path whose pinned executable
/// basename is not `PrintDeps.exe`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeBrowserWinlddExecutable {
    path: RuntimePath,
}

impl RuntimeBrowserWinlddExecutable {
    /// Admit a path after the caller has verified the immutable browser asset
    /// lock, externally attested distribution receipt, exact absolute path,
    /// file size, and SHA-256 identity.
    ///
    /// # Safety
    ///
    /// The caller must have performed every check above against a trust anchor
    /// that JavaScript and the runtime request cannot select or replace.
    #[doc(hidden)]
    pub unsafe fn from_attested_distribution_path(
        path: RuntimePath,
    ) -> Result<Self, RuntimeProcessPipeError> {
        let basename = super::path::path_basename(super::path::NodePathDialect::Win32, &path, None);
        if basename.as_runtime_string() != &RuntimeString::from_utf8("PrintDeps.exe") {
            return Err(RuntimeProcessPipeError::new(
                "browser winldd executable must have the pinned PrintDeps.exe basename",
            ));
        }
        Ok(Self { path })
    }
}

/// Typed operands for Playwright's Windows winldd dependency probe. The
/// executable is branded separately so no generic process command crosses the
/// public browser broker boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeBrowserWindowsWinlddRequest {
    executable: RuntimeBrowserWinlddExecutable,
    file: RuntimePath,
    cwd: RuntimePath,
    ld_library_path: RuntimeString,
}

impl RuntimeBrowserWindowsWinlddRequest {
    pub fn new(
        executable: RuntimeBrowserWinlddExecutable,
        file: RuntimePath,
        cwd: RuntimePath,
        ld_library_path: RuntimeString,
    ) -> Self {
        Self {
            executable,
            file,
            cwd,
            ld_library_path,
        }
    }
}

const BROWSER_PLAYWRIGHT_SW_VERS_MAX_STDOUT_BYTES: usize = 1024 * 1024;

struct BrowserBrokerProcessSpawnPlan {
    request: RuntimeSpawnRequest,
    stdin_is_pipe: bool,
    stdout_is_pipe: bool,
    stderr_is_pipe: bool,
    stdout_limit: Option<usize>,
}

impl BrowserBrokerProcessSpawnPlan {
    fn from_operation(
        binding: &RuntimeBrowserProcessBinding,
        operation: RuntimeBrowserBrokerProcessRequest,
    ) -> Result<Self, RuntimeExecFileError> {
        let (file, args, cwd, environment, stdio, policy, stdout_limit) = match operation {
            RuntimeBrowserBrokerProcessRequest::PlaywrightTreeKill(_) => {
                unreachable!("tree kill is handled before broker process planning")
            }
            RuntimeBrowserBrokerProcessRequest::PlaywrightMacOsSwVers => {
                if !cfg!(target_os = "macos") {
                    return Err(browser_broker_no_spawn_error("PlaywrightMacOsSwVers"));
                }
                (
                    ProcessAtom::Runtime(RuntimeString::from_utf8("/bin/sh")),
                    vec![
                        ProcessAtom::Runtime(RuntimeString::from_utf8("-c")),
                        ProcessAtom::Runtime(RuntimeString::from_utf8("sw_vers -productVersion")),
                    ],
                    binding.cwd.clone().map(ProcessAtom::Native),
                    binding.environment.clone(),
                    (RuntimeStdio::Null, RuntimeStdio::Pipe, RuntimeStdio::Null),
                    ProcessSurfacePolicyId::RustBrowserPlaywrightMacOsSwVers,
                    Some(BROWSER_PLAYWRIGHT_SW_VERS_MAX_STDOUT_BYTES),
                )
            }
            RuntimeBrowserBrokerProcessRequest::PlaywrightLinuxLdd(request) => {
                if !cfg!(target_os = "linux") {
                    return Err(browser_broker_no_spawn_error("PlaywrightLinuxLdd"));
                }
                let expected_cwd =
                    super::path::path_dirname(super::path::NodePathDialect::Posix, &request.file);
                if request.cwd != expected_cwd {
                    return Err(RuntimeExecFileError::new(
                        RuntimeExecFileErrorStage::Initialization,
                        "browser Linux ldd cwd must equal path.dirname(file)",
                    ));
                }
                let mut environment = binding.environment.clone();
                environment.insert(
                    RuntimeString::from_utf8("LD_LIBRARY_PATH"),
                    request.ld_library_path,
                );
                (
                    ProcessAtom::Runtime(RuntimeString::from_utf8("ldd")),
                    vec![request.file.into_runtime_process_atom()],
                    Some(request.cwd.into_runtime_process_atom()),
                    environment,
                    (RuntimeStdio::Pipe, RuntimeStdio::Pipe, RuntimeStdio::Pipe),
                    ProcessSurfacePolicyId::RustBrowserPlaywrightLinuxLdd,
                    None,
                )
            }
            RuntimeBrowserBrokerProcessRequest::PlaywrightLinuxLdconfig => {
                if !cfg!(target_os = "linux") {
                    return Err(browser_broker_no_spawn_error("PlaywrightLinuxLdconfig"));
                }
                (
                    ProcessAtom::Runtime(RuntimeString::from_utf8("/sbin/ldconfig")),
                    vec![ProcessAtom::Runtime(RuntimeString::from_utf8("-p"))],
                    binding.cwd.clone().map(ProcessAtom::Native),
                    binding.environment.clone(),
                    (RuntimeStdio::Pipe, RuntimeStdio::Pipe, RuntimeStdio::Pipe),
                    ProcessSurfacePolicyId::RustBrowserPlaywrightLinuxLdconfig,
                    None,
                )
            }
            RuntimeBrowserBrokerProcessRequest::PlaywrightWindowsWinldd(request) => {
                if !cfg!(windows) {
                    return Err(browser_broker_no_spawn_error("PlaywrightWindowsWinldd"));
                }
                let expected_cwd =
                    super::path::path_dirname(super::path::NodePathDialect::Win32, &request.file);
                if request.cwd != expected_cwd {
                    return Err(RuntimeExecFileError::new(
                        RuntimeExecFileErrorStage::Initialization,
                        "browser Windows winldd cwd must equal path.dirname(file)",
                    ));
                }
                let mut environment = binding.environment.clone();
                environment.insert(
                    RuntimeString::from_utf8("LD_LIBRARY_PATH"),
                    request.ld_library_path,
                );
                (
                    request.executable.path.into_runtime_process_atom(),
                    vec![request.file.into_runtime_process_atom()],
                    Some(request.cwd.into_runtime_process_atom()),
                    environment,
                    (RuntimeStdio::Pipe, RuntimeStdio::Pipe, RuntimeStdio::Pipe),
                    ProcessSurfacePolicyId::RustBrowserPlaywrightWindowsWinldd,
                    None,
                )
            }
        };

        let (stdin, stdout, stderr) = stdio;
        let mut request = RuntimeSpawnRequest::new(RuntimeString::from_utf8("<browser-broker>"));
        request.file = file;
        request.args = args;
        request.cwd = cwd;
        request.env = Some(environment);
        request.stdin = stdin;
        request.stdout = stdout;
        request.stderr = stderr;
        request.image_dialect = ProcessImageDialect::Node20Libuv146;
        request.environment_dialect = EnvironmentImageDialect::Node20ParentSnapshot;
        request.environment_guard = ProcessEnvironmentGuard::None;
        request.windows_job_policy = if cfg!(windows) {
            WindowsJobPolicy::LibuvGlobal
        } else {
            WindowsJobPolicy::None
        };
        request.topology = ProcessTopology::Inherit;
        request.kill_target = KillTarget::Child;
        request.drop_action = RuntimeDropAction::KeepAlive;
        request.surface_policy = policy;
        #[cfg(windows)]
        {
            request.windows_hide = true;
        }
        Ok(Self {
            request,
            stdin_is_pipe: stdin == RuntimeStdio::Pipe,
            stdout_is_pipe: stdout == RuntimeStdio::Pipe,
            stderr_is_pipe: stderr == RuntimeStdio::Pipe,
            stdout_limit,
        })
    }
}

fn browser_broker_no_spawn_error(operation: &'static str) -> RuntimeExecFileError {
    RuntimeExecFileError::new(
        RuntimeExecFileErrorStage::Initialization,
        format!("browser broker operation {operation} is a no-spawn surface on this target"),
    )
}

async fn read_browser_broker_process_output<R>(
    reader: Option<R>,
    stream: &'static str,
    limit: Option<usize>,
) -> Result<Vec<u8>, RuntimeExecFileError>
where
    R: AsyncRead + Unpin,
{
    let Some(mut reader) = reader else {
        return Ok(Vec::new());
    };
    let mut bytes = Vec::new();
    let mut overflowed = false;
    let mut chunk = [0_u8; 8192];
    loop {
        let read = reader.read(&mut chunk).await.map_err(|error| {
            RuntimeExecFileError::new(
                RuntimeExecFileErrorStage::Output,
                format!("browser broker process {stream} read failed: {error}"),
            )
        })?;
        if read == 0 {
            break;
        }
        match limit {
            Some(limit) => {
                let retained = limit.saturating_sub(bytes.len()).min(read);
                bytes.extend_from_slice(&chunk[..retained]);
                overflowed |= retained != read;
            }
            None => bytes.extend_from_slice(&chunk[..read]),
        }
    }
    if overflowed {
        return Err(RuntimeExecFileError::new(
            RuntimeExecFileErrorStage::Output,
            format!(
                "browser broker process {stream} exceeded the exact {}-byte limit",
                limit.expect("overflow requires a configured browser broker limit")
            ),
        ));
    }
    Ok(bytes)
}

/// Policy-bound native host input for the SDK's bundled browser sidecar.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeBrowserSidecarRequest {
    node_path: PathBuf,
    sidecar_path: PathBuf,
    cwd: Option<PathBuf>,
    artifact_dir: Option<PathBuf>,
    headless: bool,
    env: Vec<(RuntimeString, RuntimeString)>,
}

impl RuntimeBrowserSidecarRequest {
    pub fn new(node_path: PathBuf, sidecar_path: PathBuf) -> Self {
        Self {
            node_path,
            sidecar_path,
            cwd: None,
            artifact_dir: None,
            headless: true,
            env: Vec::new(),
        }
    }

    pub fn with_cwd(mut self, cwd: PathBuf) -> Self {
        self.cwd = Some(cwd);
        self
    }

    pub fn with_artifact_dir(mut self, artifact_dir: PathBuf) -> Self {
        self.artifact_dir = Some(artifact_dir);
        self
    }

    pub fn with_headless(mut self, headless: bool) -> Self {
        self.headless = headless;
        self
    }

    pub fn with_env(mut self, env: Vec<(RuntimeString, RuntimeString)>) -> Self {
        self.env = env;
        self
    }
}

fn build_browser_sidecar_spawn_request(
    request: RuntimeBrowserSidecarRequest,
) -> RuntimeSpawnRequest {
    let RuntimeBrowserSidecarRequest {
        node_path,
        sidecar_path,
        cwd,
        artifact_dir,
        headless,
        env,
    } = request;
    let mut spawn_request = RuntimeSpawnRequest::new(RuntimeString::from_utf8("<native>"));
    spawn_request.file =
        ProcessAtom::Native(NativeOsString::from_host_os_str(node_path.as_os_str()));
    spawn_request = spawn_request
        .with_native_args(vec![NativeOsString::from_host_os_str(
            sidecar_path.as_os_str(),
        )])
        .with_stdio(RuntimeStdio::Pipe, RuntimeStdio::Pipe, RuntimeStdio::Pipe)
        .with_hard_kill_on_drop(true)
        .with_surface_policy(ProcessSurfacePolicyId::RustBrowserSidecar);
    if let Some(cwd) = cwd {
        spawn_request =
            spawn_request.with_native_cwd(NativeOsString::from_host_os_str(cwd.as_os_str()));
    }
    let mut mutations = Vec::new();
    if let Some(artifact_dir) = artifact_dir {
        mutations.push(Rust193EnvMutation::NativeHostSet(
            NativeHostEnvOverlay::from_host_os_str(
                RuntimeString::from_utf8("KLAUS_BROWSER_ARTIFACT_DIR"),
                artifact_dir.as_os_str(),
            ),
        ));
    }
    mutations.push(Rust193EnvMutation::RuntimeSet {
        key: RuntimeString::from_utf8("KLAUS_BROWSER_HEADLESS"),
        value: RuntimeString::from_utf8(if headless { "1" } else { "0" }),
    });
    mutations.extend(
        env.into_iter()
            .map(|(key, value)| Rust193EnvMutation::RuntimeSet { key, value }),
    );
    spawn_request = spawn_request.with_rust193_env_mutations(mutations);
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        // These four descriptors are the only broker lanes admitted for the
        // bundled browser sidecar. Their directions are from the child's point
        // of view and their contiguous numbers are part of the KBU1 contract.
        spawn_request.unix_extra_stdio = vec![
            UnixExtraStdioPlan::new(3, UnixExtraPipeDirection::ChildToHost),
            UnixExtraStdioPlan::new(4, UnixExtraPipeDirection::HostToChild),
            UnixExtraStdioPlan::new(5, UnixExtraPipeDirection::ChildToHost),
            UnixExtraStdioPlan::new(6, UnixExtraPipeDirection::HostToChild),
        ];
    }
    #[cfg(windows)]
    {
        spawn_request.windows_job_policy = WindowsJobPolicy::BrowserSidecarPrivate;
        spawn_request.kill_target = KillTarget::WindowsPrivateJob;
        spawn_request.drop_action = RuntimeDropAction::HardKill(KillTarget::WindowsPrivateJob);
    }
    spawn_request
}

#[cfg(test)]
mod browser_sidecar_request_tests {
    use super::*;

    #[test]
    fn exact_overlay_order_and_surrogates_reach_the_guarded_rust193_plan() {
        let lone_high = RuntimeString::from_utf16(vec![0xd800]);
        let lone_low = RuntimeString::from_utf16(vec![0xdc00]);
        let request =
            RuntimeBrowserSidecarRequest::new(PathBuf::from("node"), PathBuf::from("sidecar.mjs"))
                .with_artifact_dir(PathBuf::from("artifacts"))
                .with_headless(false)
                .with_env(vec![
                    (
                        RuntimeString::from("KLAUS_BROWSER_ARTIFACT_DIR"),
                        RuntimeString::from("caller-artifacts"),
                    ),
                    (
                        RuntimeString::from("KLAUS_BROWSER_HEADLESS"),
                        RuntimeString::from("caller-headless"),
                    ),
                    (lone_high.clone(), lone_low.clone()),
                ]);

        let spawn = build_browser_sidecar_spawn_request(request);
        assert_eq!(spawn.image_dialect, ProcessImageDialect::Rust193);
        assert_eq!(
            spawn.environment_dialect,
            EnvironmentImageDialect::Rust193CommandOverlay
        );
        assert_eq!(
            spawn.environment_guard,
            ProcessEnvironmentGuard::SealedNodeNoPreloadV1
        );
        let mutations = spawn
            .rust193_env_mutations
            .expect("browser sidecar must use the Rust 1.93 overlay");
        assert_eq!(mutations.len(), 5);
        match &mutations[0] {
            Rust193EnvMutation::NativeHostSet(value) => assert_eq!(
                value.key(),
                &RuntimeString::from("KLAUS_BROWSER_ARTIFACT_DIR")
            ),
            other => panic!("unexpected artifact mutation: {other:?}"),
        }
        assert_eq!(
            &mutations[1..],
            &[
                Rust193EnvMutation::RuntimeSet {
                    key: RuntimeString::from("KLAUS_BROWSER_HEADLESS"),
                    value: RuntimeString::from("0"),
                },
                Rust193EnvMutation::RuntimeSet {
                    key: RuntimeString::from("KLAUS_BROWSER_ARTIFACT_DIR"),
                    value: RuntimeString::from("caller-artifacts"),
                },
                Rust193EnvMutation::RuntimeSet {
                    key: RuntimeString::from("KLAUS_BROWSER_HEADLESS"),
                    value: RuntimeString::from("caller-headless"),
                },
                Rust193EnvMutation::RuntimeSet {
                    key: lone_high,
                    value: lone_low,
                },
            ]
        );
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn browser_sidecar_owns_exact_four_directional_unix_broker_lanes() {
        let spawn = build_browser_sidecar_spawn_request(RuntimeBrowserSidecarRequest::new(
            PathBuf::from("node"),
            PathBuf::from("sidecar.mjs"),
        ));
        assert_eq!(spawn.unix_extra_stdio.len(), 4);
        assert_eq!(spawn.unix_extra_stdio[0].child_fd(), 3);
        assert_eq!(
            spawn.unix_extra_stdio[0].direction(),
            UnixExtraPipeDirection::ChildToHost
        );
        assert_eq!(spawn.unix_extra_stdio[1].child_fd(), 4);
        assert_eq!(
            spawn.unix_extra_stdio[1].direction(),
            UnixExtraPipeDirection::HostToChild
        );
        assert_eq!(spawn.unix_extra_stdio[2].child_fd(), 5);
        assert_eq!(
            spawn.unix_extra_stdio[2].direction(),
            UnixExtraPipeDirection::ChildToHost
        );
        assert_eq!(spawn.unix_extra_stdio[3].child_fd(), 6);
        assert_eq!(
            spawn.unix_extra_stdio[3].direction(),
            UnixExtraPipeDirection::HostToChild
        );
    }
}

/// Closed Core input for one broker-admitted Playwright child. Reserved
/// planner/source evidence is intentionally absent: it cannot enter the OS
/// spawn options.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeBrowserPlaywrightLaunchMode {
    Ephemeral,
    Persistent,
}

impl RuntimeBrowserPlaywrightLaunchMode {
    const fn policy_id(self) -> ProcessSurfacePolicyId {
        match self {
            Self::Ephemeral => ProcessSurfacePolicyId::BrowserPlaywrightEphemeral1611,
            Self::Persistent => ProcessSurfacePolicyId::BrowserPlaywrightPersistent1611,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeBrowserPlaywrightLaunchRequest {
    command: RuntimeString,
    args: Vec<RuntimeString>,
    mode: RuntimeBrowserPlaywrightLaunchMode,
    detached: bool,
    cwd: Option<RuntimePath>,
    environment: Option<RuntimeEnv>,
}

impl RuntimeBrowserPlaywrightLaunchRequest {
    pub fn new(command: RuntimeString, args: Vec<RuntimeString>) -> Self {
        Self {
            command,
            args,
            mode: RuntimeBrowserPlaywrightLaunchMode::Ephemeral,
            detached: false,
            cwd: None,
            environment: None,
        }
    }

    pub fn with_mode(mut self, mode: RuntimeBrowserPlaywrightLaunchMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn with_detached(mut self, detached: bool) -> Self {
        self.detached = detached;
        self
    }

    pub fn with_cwd(mut self, cwd: RuntimePath) -> Self {
        self.cwd = Some(cwd);
        self
    }

    pub fn with_environment(mut self, environment: RuntimeEnv) -> Self {
        self.environment = Some(environment);
        self
    }
}

/// Semantic-only FFmpeg request. Executable and argv are not caller fields.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeBrowserFfmpegEncodeRequest {
    list_path: RuntimeString,
    file_path: RuntimeString,
    fps: u8,
}

impl RuntimeBrowserFfmpegEncodeRequest {
    pub fn new(
        list_path: RuntimeString,
        file_path: RuntimeString,
        fps: u8,
    ) -> Result<Self, RuntimeProcessPipeError> {
        if !(1..=30).contains(&fps) {
            return Err(RuntimeProcessPipeError::new(
                "browser FFmpeg fps must be an integer from 1 through 30",
            ));
        }
        Ok(Self {
            list_path,
            file_path,
            fps,
        })
    }
}

/// Non-cloneable owner for one brokered Playwright or FFmpeg child.
pub struct RuntimeBrowserChildProcess {
    inner: SpawnedRuntimeProcess,
    pid: u32,
}

#[derive(Clone)]
pub struct RuntimeBrowserChildControl {
    controller: RuntimeProcessController,
    pid: u32,
}

impl fmt::Debug for RuntimeBrowserChildControl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeBrowserChildControl")
            .field("pid", &self.pid)
            .finish_non_exhaustive()
    }
}

impl RuntimeBrowserChildControl {
    pub fn hard_kill(&self) {
        self.controller.signal(RuntimeSignal::Kill);
    }

    async fn execute_playwright_tree_kill(
        &self,
    ) -> Result<RuntimeExecFileOutput, RuntimeExecFileError> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            if self.controller.target != KillTarget::UnixProcessGroup {
                return Err(RuntimeExecFileError::new(
                    RuntimeExecFileErrorStage::Initialization,
                    "Playwright Unix tree kill requires an owned detached process group",
                ));
            }
            let pid = i32::try_from(self.pid).map_err(|_| {
                RuntimeExecFileError::new(
                    RuntimeExecFileErrorStage::Initialization,
                    "Playwright Unix tree-kill pid is outside the signed pid_t range",
                )
            })?;
            if pid <= 0 {
                return Err(RuntimeExecFileError::new(
                    RuntimeExecFileErrorStage::Initialization,
                    "Playwright Unix tree-kill pid must be positive",
                ));
            }
            // Exact pinned Playwright primitive: kill(-owned_pid, SIGKILL).
            // The cloned Core controller retains the target identity lease for
            // the duration of this synchronous syscall.
            let result = unsafe { libc::kill(-pid, libc::SIGKILL) };
            if result == 0 {
                return Ok(RuntimeExecFileOutput {
                    status: RuntimeExecFileStatus {
                        code: Some(0),
                        signal: None,
                    },
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                });
            }
            let error = std::io::Error::last_os_error();
            return Err(RuntimeExecFileError::new(
                RuntimeExecFileErrorStage::Wait,
                format!(
                    "Playwright Unix tree kill(-{}, SIGKILL) failed: {}",
                    pid, error
                ),
            ));
        }
        #[cfg(windows)]
        {
            if self.controller.target != KillTarget::WindowsProcessTree {
                return Err(RuntimeExecFileError::new(
                    RuntimeExecFileErrorStage::Initialization,
                    "Playwright Windows tree kill requires an owned process-tree target",
                ));
            }
            match self.controller.inner.begin_control(false) {
                WindowsControlStart::Execute(action) => {
                    return execute_windows_playwright_tree_kill(&self.controller.inner, action)
                        .await;
                }
                WindowsControlStart::Join { .. } | WindowsControlStart::Disarmed => {
                    return Ok(RuntimeExecFileOutput {
                        status: RuntimeExecFileStatus {
                            code: Some(0),
                            signal: None,
                        },
                        stdout: Vec::new(),
                        stderr: Vec::new(),
                    });
                }
            }
        }
        #[allow(unreachable_code)]
        Err(RuntimeExecFileError::new(
            RuntimeExecFileErrorStage::Initialization,
            "Playwright tree kill is unsupported on this target",
        ))
    }
}

impl RuntimeBrowserChildProcess {
    fn new(inner: SpawnedRuntimeProcess) -> Self {
        let pid = inner.browser_pid();
        Self { inner, pid }
    }

    pub fn pid(&self) -> u32 {
        self.pid
    }

    pub fn control(&self) -> RuntimeBrowserChildControl {
        RuntimeBrowserChildControl {
            controller: self.inner.controller(),
            pid: self.pid,
        }
    }

    pub fn take_stdout(&mut self) -> Option<RuntimeChildStdout> {
        self.inner.take_stdout()
    }

    pub fn take_stderr(&mut self) -> Option<RuntimeChildStderr> {
        self.inner.take_stderr()
    }

    pub fn take_extra3(&mut self) -> Option<RuntimeBrowserChildDuplex> {
        self.inner.take_browser_child_duplex(3)
    }

    pub fn take_extra4(&mut self) -> Option<RuntimeBrowserChildDuplex> {
        self.inner.take_browser_child_duplex(4)
    }

    pub fn hard_kill(&self) {
        self.control().hard_kill();
    }

    pub async fn wait(&mut self) -> Result<RuntimeExecFileStatus, RuntimeProcessPipeError> {
        self.inner
            .wait()
            .await
            .map(RuntimeExecFileStatus::from)
            .map_err(RuntimeProcessPipeError::from_spawn)
    }
}

pub struct RuntimePipeProcess {
    inner: SpawnedRuntimeProcess,
    browser_generation: u64,
    browser_package_ready_challenge: [u8; 32],
    browser_process_binding: RuntimeBrowserProcessBinding,
    #[cfg(windows)]
    browser_windows_bootstrap_nonce: [u8; 32],
    #[cfg(windows)]
    browser_windows_client_pid: u32,
    #[cfg(windows)]
    browser_windows_bootstrap_deadline: Instant,
}

impl RuntimePipeProcess {
    fn new(
        inner: SpawnedRuntimeProcess,
        browser_generation: u64,
        browser_package_ready_challenge: [u8; 32],
        browser_process_binding: RuntimeBrowserProcessBinding,
        #[cfg(windows)] browser_windows_bootstrap_nonce: [u8; 32],
        #[cfg(windows)] browser_windows_client_pid: u32,
        #[cfg(windows)] browser_windows_bootstrap_deadline: Instant,
    ) -> Self {
        Self {
            inner,
            browser_generation,
            browser_package_ready_challenge,
            browser_process_binding,
            #[cfg(windows)]
            browser_windows_bootstrap_nonce,
            #[cfg(windows)]
            browser_windows_client_pid,
            #[cfg(windows)]
            browser_windows_bootstrap_deadline,
        }
    }

    pub fn browser_generation(&self) -> u64 {
        self.browser_generation
    }

    pub fn browser_package_ready_challenge(&self) -> &[u8; 32] {
        &self.browser_package_ready_challenge
    }

    pub fn browser_process_binding(&self) -> RuntimeBrowserProcessBinding {
        self.browser_process_binding.clone()
    }

    #[cfg(windows)]
    pub fn browser_windows_bootstrap_nonce(&self) -> &[u8; 32] {
        &self.browser_windows_bootstrap_nonce
    }

    #[cfg(windows)]
    pub fn browser_windows_client_pid(&self) -> u32 {
        self.browser_windows_client_pid
    }

    #[cfg(windows)]
    pub fn browser_windows_bootstrap_deadline(&self) -> Instant {
        self.browser_windows_bootstrap_deadline
    }

    pub fn try_wait(&self) -> Result<Option<RuntimeExecFileStatus>, RuntimeProcessShutdownError> {
        self.inner
            .controller
            .try_wait_terminal()
            .map(|status| status.map(RuntimeExecFileStatus::from))
    }

    pub fn take_stdin(&mut self) -> Option<RuntimeChildStdin> {
        self.inner.take_stdin()
    }

    pub fn take_stdout(&mut self) -> Option<RuntimeChildStdout> {
        self.inner.take_stdout()
    }

    pub fn take_stderr(&mut self) -> Option<RuntimeChildStderr> {
        self.inner.take_stderr()
    }

    /// Take the child-fd-3 client-to-host synchronous broker lane.
    pub fn take_browser_sync_inbound(&mut self) -> Option<RuntimeBrowserBrokerRead> {
        self.inner.take_browser_broker_read(3)
    }

    /// Take the child-fd-4 host-to-client synchronous broker lane.
    pub fn take_browser_sync_outbound(&mut self) -> Option<RuntimeBrowserBrokerWrite> {
        self.inner.take_browser_broker_write(4)
    }

    /// Take the child-fd-5 client-to-host asynchronous mux lane.
    pub fn take_browser_mux_inbound(&mut self) -> Option<RuntimeBrowserBrokerRead> {
        self.inner.take_browser_broker_read(5)
    }

    /// Take the child-fd-6 host-to-client asynchronous mux lane.
    pub fn take_browser_mux_outbound(&mut self) -> Option<RuntimeBrowserBrokerWrite> {
        self.inner.take_browser_broker_write(6)
    }

    pub async fn wait(&mut self) -> Result<RuntimeExecFileStatus, RuntimeProcessPipeError> {
        self.inner
            .wait()
            .await
            .map(RuntimeExecFileStatus::from)
            .map_err(RuntimeProcessPipeError::from_spawn)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeProcessPipeError {
    message: String,
}

impl RuntimeProcessPipeError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    fn from_spawn(error: RuntimeSpawnError) -> Self {
        Self::new(error.to_string())
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for RuntimeProcessPipeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for RuntimeProcessPipeError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeCompletedProcessSurface {
    ExtensionExecFile,
    FileIndexGitStream,
    PdfExecFile,
    // Never constructed since the credential store moved off OS keychain CLIs
    // to an encrypted file; the policy binding itself stays in the frozen
    // contract.
    #[allow(dead_code)]
    CredentialExecFile,
    // Constructed only by the feature-gated test-support surface (eval
    // harness); the policy binding itself stays in the frozen contract.
    #[cfg_attr(not(any(test, feature = "testing")), allow(dead_code))]
    EvalCommandSuccess,
    WindowsBashSetup,
    ShellEnvSnapshot,
}

pub(crate) const SHELL_ENV_SNAPSHOT_MAX_STDOUT_BYTES: usize = 4 * 1024 * 1024;

impl RuntimeCompletedProcessSurface {
    const fn policy_id(self) -> ProcessSurfacePolicyId {
        match self {
            Self::ExtensionExecFile => ProcessSurfacePolicyId::TsExtensionExecFile,
            Self::FileIndexGitStream => ProcessSurfacePolicyId::TsFileIndexGitStream,
            Self::PdfExecFile => ProcessSurfacePolicyId::TsPdfExecFile,
            Self::CredentialExecFile => ProcessSurfacePolicyId::TsCredentialExecFile,
            Self::EvalCommandSuccess => ProcessSurfacePolicyId::RustEvalCommandSuccess,
            Self::WindowsBashSetup => ProcessSurfacePolicyId::ScalarActionAllowlist,
            Self::ShellEnvSnapshot => ProcessSurfacePolicyId::TsShellEnvSnapshot,
        }
    }

    const fn output_stdio(self) -> (RuntimeStdio, RuntimeStdio) {
        match self {
            Self::FileIndexGitStream | Self::ShellEnvSnapshot => {
                (RuntimeStdio::Pipe, RuntimeStdio::Null)
            }
            Self::EvalCommandSuccess => (RuntimeStdio::Null, RuntimeStdio::Null),
            Self::ExtensionExecFile
            | Self::PdfExecFile
            | Self::CredentialExecFile
            | Self::WindowsBashSetup => (RuntimeStdio::Pipe, RuntimeStdio::Pipe),
        }
    }

    const fn allows_explicit_timeout(self) -> bool {
        matches!(self, Self::PdfExecFile | Self::ShellEnvSnapshot)
    }

    const fn allows_stdout_limit(self) -> bool {
        matches!(self, Self::ShellEnvSnapshot)
    }

    const fn keep_alive_on_drop(self) -> bool {
        matches!(self, Self::WindowsBashSetup)
    }

    const fn uses_node20_image(self) -> bool {
        matches!(self, Self::WindowsBashSetup | Self::ShellEnvSnapshot)
    }

    const fn supports_current_target(self) -> bool {
        match self {
            Self::WindowsBashSetup => cfg!(windows),
            Self::ShellEnvSnapshot => cfg!(any(target_os = "linux", target_os = "macos")),
            _ => true,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeCompletedProcessRequest {
    surface: RuntimeCompletedProcessSurface,
    file: RuntimeString,
    args: Vec<RuntimeString>,
    cwd: Option<RuntimePath>,
    timeout: Option<Duration>,
    max_stdout_bytes: Option<usize>,
}

impl RuntimeCompletedProcessRequest {
    pub(crate) fn new(surface: RuntimeCompletedProcessSurface, file: RuntimeString) -> Self {
        Self {
            surface,
            file,
            args: Vec::new(),
            cwd: None,
            timeout: None,
            max_stdout_bytes: None,
        }
    }

    pub(crate) fn with_args(mut self, args: Vec<RuntimeString>) -> Self {
        self.args = args;
        self
    }

    pub(crate) fn with_cwd(mut self, cwd: RuntimePath) -> Self {
        self.cwd = Some(cwd);
        self
    }

    pub(crate) fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub(crate) fn with_max_stdout_bytes(mut self, max_stdout_bytes: usize) -> Self {
        self.max_stdout_bytes = Some(max_stdout_bytes);
        self
    }
}

#[cfg(test)]
mod completed_process_request_tests {
    use super::*;

    #[test]
    fn frozen_completed_surfaces_default_to_no_timeout() {
        for surface in [
            RuntimeCompletedProcessSurface::ExtensionExecFile,
            RuntimeCompletedProcessSurface::FileIndexGitStream,
            RuntimeCompletedProcessSurface::PdfExecFile,
            RuntimeCompletedProcessSurface::CredentialExecFile,
            RuntimeCompletedProcessSurface::EvalCommandSuccess,
            RuntimeCompletedProcessSurface::WindowsBashSetup,
            RuntimeCompletedProcessSurface::ShellEnvSnapshot,
        ] {
            let request = RuntimeCompletedProcessRequest::new(surface, RuntimeString::from("tool"));
            assert_eq!(request.timeout, None, "{}", surface.policy_id().name());
        }
    }

    #[test]
    fn only_frozen_callsite_timed_surfaces_accept_an_explicit_timeout() {
        assert!(!RuntimeCompletedProcessSurface::ExtensionExecFile.allows_explicit_timeout());
        assert!(!RuntimeCompletedProcessSurface::FileIndexGitStream.allows_explicit_timeout());
        assert!(RuntimeCompletedProcessSurface::PdfExecFile.allows_explicit_timeout());
        assert!(!RuntimeCompletedProcessSurface::CredentialExecFile.allows_explicit_timeout());
        assert!(!RuntimeCompletedProcessSurface::EvalCommandSuccess.allows_explicit_timeout());
        assert!(!RuntimeCompletedProcessSurface::WindowsBashSetup.allows_explicit_timeout());
        assert!(RuntimeCompletedProcessSurface::ShellEnvSnapshot.allows_explicit_timeout());
    }

    #[test]
    fn completed_shell_surfaces_freeze_stdio_drop_and_shutdown_contracts() {
        assert_eq!(
            RuntimeCompletedProcessSurface::WindowsBashSetup.output_stdio(),
            (RuntimeStdio::Pipe, RuntimeStdio::Pipe)
        );
        assert!(RuntimeCompletedProcessSurface::WindowsBashSetup.keep_alive_on_drop());
        let bash_request = RuntimeSpawnRequest::new(RuntimeString::from("winget"))
            .with_stdio(RuntimeStdio::Null, RuntimeStdio::Pipe, RuntimeStdio::Pipe)
            .with_hard_kill_on_drop(false)
            .with_surface_policy(RuntimeCompletedProcessSurface::WindowsBashSetup.policy_id());
        assert_eq!(
            bash_request.context_shutdown_action(),
            ContextShutdownAction::RequireCompleted
        );

        assert_eq!(
            RuntimeCompletedProcessSurface::ShellEnvSnapshot.output_stdio(),
            (RuntimeStdio::Pipe, RuntimeStdio::Null)
        );
        assert!(!RuntimeCompletedProcessSurface::ShellEnvSnapshot.keep_alive_on_drop());
        assert!(RuntimeCompletedProcessSurface::ShellEnvSnapshot.allows_stdout_limit());
        let snapshot_request = RuntimeSpawnRequest::new(RuntimeString::from("/bin/sh"))
            .with_stdio(RuntimeStdio::Null, RuntimeStdio::Pipe, RuntimeStdio::Null)
            .with_hard_kill_on_drop(true)
            .with_surface_policy(RuntimeCompletedProcessSurface::ShellEnvSnapshot.policy_id());
        assert_eq!(
            snapshot_request.context_shutdown_action(),
            ContextShutdownAction::HardKill
        );
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn shell_env_snapshot_enforces_the_exact_max_plus_one_stdout_read() {
        let context = test_runtime_process_context();
        let missing_limit = RuntimeCompletedProcessRequest::new(
            RuntimeCompletedProcessSurface::ShellEnvSnapshot,
            RuntimeString::from("/bin/sh"),
        )
        .with_args(vec![
            RuntimeString::from("-c"),
            RuntimeString::from("exit 0"),
        ])
        .with_timeout(Duration::from_secs(2));
        let error = context.exec_completed(missing_limit).await.unwrap_err();
        assert_eq!(error.stage(), RuntimeExecFileErrorStage::Initialization);

        let overflow = RuntimeCompletedProcessRequest::new(
            RuntimeCompletedProcessSurface::ShellEnvSnapshot,
            RuntimeString::from("/bin/sh"),
        )
        .with_args(vec![
            RuntimeString::from("-c"),
            RuntimeString::from(format!(
                "head -c {} /dev/zero",
                SHELL_ENV_SNAPSHOT_MAX_STDOUT_BYTES + 1
            )),
        ])
        .with_timeout(Duration::from_secs(5))
        .with_max_stdout_bytes(SHELL_ENV_SNAPSHOT_MAX_STDOUT_BYTES);
        let error = context.exec_completed(overflow).await.unwrap_err();
        assert_eq!(error.stage(), RuntimeExecFileErrorStage::Output);
        assert!(error.message().contains("stdout exceeded"));
    }
}

/// Exact logical input for [`RuntimeProcessContext::exec_file`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeExecFileRequest {
    surface: RuntimeExecFileSurface,
    file: RuntimeString,
    args: Vec<RuntimeString>,
    cwd: Option<RuntimePath>,
    timeout: Duration,
}

impl RuntimeExecFileRequest {
    pub fn new(surface: RuntimeExecFileSurface, file: RuntimeString) -> Self {
        Self {
            surface,
            file,
            args: Vec::new(),
            cwd: None,
            timeout: Duration::from_millis(5_000),
        }
    }

    pub fn with_args(mut self, args: Vec<RuntimeString>) -> Self {
        self.args = args;
        self
    }

    pub fn with_cwd(mut self, cwd: RuntimePath) -> Self {
        self.cwd = Some(cwd);
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

/// Frozen completed-process surfaces available to Eval. Callers cannot invent
/// policy identifiers or alter ownership/termination semantics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeExecFileSurface {
    EvalArtifactGit,
    EvalWorktreeAdd,
    EvalWorktreeRemove,
}

impl RuntimeExecFileSurface {
    const fn policy_id(self) -> ProcessSurfacePolicyId {
        match self {
            Self::EvalArtifactGit => ProcessSurfacePolicyId::EvalArtifactGit,
            Self::EvalWorktreeAdd => ProcessSurfacePolicyId::EvalWorktreeAdd,
            Self::EvalWorktreeRemove => ProcessSurfacePolicyId::EvalWorktreeRemove,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeExecFileStatus {
    code: Option<i32>,
    signal: Option<i32>,
}

impl RuntimeExecFileStatus {
    pub const fn success(self) -> bool {
        matches!(self.code, Some(0))
    }

    pub const fn code(self) -> Option<i32> {
        self.code
    }

    pub const fn signal(self) -> Option<i32> {
        self.signal
    }
}

impl fmt::Display for RuntimeExecFileStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.code, self.signal) {
            (Some(code), _) => write!(formatter, "exit status: {code}"),
            (None, Some(signal)) => write!(formatter, "signal: {signal}"),
            (None, None) => formatter.write_str("unknown process status"),
        }
    }
}

impl From<RuntimeProcessStatus> for RuntimeExecFileStatus {
    fn from(status: RuntimeProcessStatus) -> Self {
        Self {
            code: status.code(),
            signal: status.signal(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeExecFileOutput {
    pub status: RuntimeExecFileStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeExecFileErrorStage {
    Initialization,
    Plan,
    Image,
    Spawn,
    Wait,
    Output,
    Timeout,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeExecFileError {
    stage: RuntimeExecFileErrorStage,
    message: String,
    not_found: bool,
}

impl RuntimeExecFileError {
    fn new(stage: RuntimeExecFileErrorStage, message: impl Into<String>) -> Self {
        Self {
            stage,
            message: message.into(),
            not_found: false,
        }
    }

    fn from_spawn(error: RuntimeSpawnError) -> Self {
        let stage = match error.stage {
            RuntimeSpawnStage::Initialization => RuntimeExecFileErrorStage::Initialization,
            RuntimeSpawnStage::Plan => RuntimeExecFileErrorStage::Plan,
            RuntimeSpawnStage::Image => RuntimeExecFileErrorStage::Image,
            RuntimeSpawnStage::Spawn => RuntimeExecFileErrorStage::Spawn,
            RuntimeSpawnStage::StdioShape => RuntimeExecFileErrorStage::Output,
            RuntimeSpawnStage::Wait => RuntimeExecFileErrorStage::Wait,
        };
        let message = error.to_string();
        let not_found = error.into_io_error().kind() == std::io::ErrorKind::NotFound;
        Self {
            stage,
            message,
            not_found,
        }
    }

    pub const fn stage(&self) -> RuntimeExecFileErrorStage {
        self.stage
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub const fn is_not_found(&self) -> bool {
        self.not_found
    }
}

impl fmt::Display for RuntimeExecFileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "runtime exec-file {:?}: {}",
            self.stage, self.message
        )
    }
}

impl std::error::Error for RuntimeExecFileError {}

impl Drop for RuntimeProcessContext {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) != 1 {
            return;
        }
        {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if state.phase != RuntimeProcessContextPhase::Running {
                return;
            }
            state.phase = RuntimeProcessContextPhase::Draining;
        }
        let inner = self.inner.clone();
        self.inner.supervisor.runtime.spawn(async move {
            let _ = shutdown_process_context(inner).await;
        });
    }
}

async fn shutdown_process_context(
    context: Arc<RuntimeProcessContextInner>,
) -> Result<(), RuntimeProcessShutdownError> {
    let mut epoch_rx = context.epoch_tx.subscribe();
    loop {
        let should_wait = {
            let mut state = context.state.lock().unwrap_or_else(PoisonError::into_inner);
            if state.phase == RuntimeProcessContextPhase::Closed {
                return Ok(());
            }
            state.phase = RuntimeProcessContextPhase::Draining;
            state.external_permits != 0
        };
        if !should_wait {
            break;
        }
        epoch_rx.changed().await.map_err(|_| {
            RuntimeProcessShutdownError::backend(
                "runtime process context admission channel closed before drain",
            )
        })?;
    }

    loop {
        let registrations = context
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .registrations
            .iter()
            .map(|(id, registration)| (*id, registration.clone()))
            .collect::<Vec<_>>();
        if registrations.is_empty() {
            context
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .phase = RuntimeProcessContextPhase::Closed;
            context.supervisor.release_context(context.id);
            return Ok(());
        }

        for (_, registration) in &registrations {
            registration.trigger_shutdown();
        }

        let mut completed = Vec::new();
        let mut retained = Vec::new();
        let now = Instant::now();
        let mut may_progress = false;
        for (id, registration) in &registrations {
            match registration.controller.try_wait_terminal()? {
                Some(_) => completed.push(*id),
                None if registration.shutdown_action == ContextShutdownAction::RequireCompleted => {
                    retained.push(registration.policy_id.name());
                }
                None => {
                    if registration
                        .deadline()
                        .is_some_and(|deadline| now >= deadline)
                    {
                        retained.push(registration.policy_id.name());
                    } else {
                        may_progress = true;
                    }
                }
            }
        }
        if !completed.is_empty() {
            let mut state = context.state.lock().unwrap_or_else(PoisonError::into_inner);
            for id in completed {
                state.registrations.remove(&id);
            }
            continue;
        }
        if !retained.is_empty() {
            return Err(RuntimeProcessShutdownError::live(retained));
        }
        if may_progress {
            tokio::time::sleep(Duration::from_millis(10)).await;
            continue;
        }
        return Err(RuntimeProcessShutdownError::backend(
            "runtime process shutdown fixed point made no progress",
        ));
    }
}

/// Context-bound facade used by every production process surface.
#[derive(Clone)]
pub(crate) struct RuntimeSpawner {
    context: RuntimeProcessContext,
}

impl RuntimeSpawner {
    pub(crate) async fn spawn(
        &self,
        request: RuntimeSpawnRequest,
    ) -> Result<SpawnedRuntimeProcess, RuntimeSpawnError> {
        let permit = self.context.admit_spawn()?;
        let policy_id = request.surface_policy;
        let shutdown_action = request.context_shutdown_action();
        let mut spawned = self
            .context
            .inner
            .supervisor
            .platform
            .spawn(request)
            .await?;
        spawned.context_registration = Some(self.context.register_spawn(
            policy_id,
            shutdown_action,
            spawned.controller(),
        ));
        drop(permit);
        Ok(spawned)
    }
}

/// Whether this host can hand out another pseudo-terminal right now.
///
/// `kern.tty.ptmx_max` is a system-wide ceiling (511 on macOS), so a single other
/// process holding thousands of masters starves everyone else. A pty test that
/// spawns anyway fails on a missing exit code, which reads as a defect in the
/// command runtime instead of as an exhausted host, so every pty test asks first.
/// The probe opens and immediately closes a master, which is exactly what the
/// spawn path would have done.
#[cfg(all(test, unix))]
pub(crate) fn pty_allocatable() -> bool {
    let fd = unsafe { ::libc::posix_openpt(::libc::O_RDWR | ::libc::O_NOCTTY) };
    if fd < 0 {
        return false;
    }
    unsafe { ::libc::close(fd) };
    true
}

/// One process-lifetime runtime/context for pure Core unit tests. Production
/// code has no getter and must inject an explicitly initialized context.
#[cfg(test)]
pub(crate) fn test_runtime_process_context() -> RuntimeProcessContext {
    static TEST_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    static TEST_CONTEXT: OnceLock<RuntimeProcessContext> = OnceLock::new();
    TEST_CONTEXT
        .get_or_init(|| {
            let runtime = TEST_RUNTIME.get_or_init(|| {
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .expect("test runtime process Tokio runtime")
            });
            let supervisor = RuntimeProcessSupervisor::initialize(runtime.handle().clone())
                .expect("test runtime process supervisor");
            RuntimeProcessContext::initialize(supervisor).expect("test runtime process context")
        })
        .clone()
}

struct RuntimePlatformSpawner {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    inner: SharedRuntimeSpawner,
    #[cfg(windows)]
    inner: WindowsSpawnSupervisor,
}

impl RuntimePlatformSpawner {
    // The tokio runtime handle binds only the unix shared spawner; the Windows
    // supervisor runs detached from it.
    #[cfg_attr(windows, allow(unused_variables))]
    fn initialize(handle: Handle) -> Result<Self, RuntimeSpawnError> {
        #[cfg(target_os = "linux")]
        let inner =
            SharedRuntimeSpawner::initialize_linux_with_runtime(handle).map_err(|error| {
                RuntimeSpawnError::new(RuntimeSpawnStage::Initialization, format!("{error:?}"))
            })?;
        #[cfg(target_os = "macos")]
        let inner =
            SharedRuntimeSpawner::initialize_macos_with_runtime(handle).map_err(|error| {
                RuntimeSpawnError::new(RuntimeSpawnStage::Initialization, format!("{error:?}"))
            })?;
        #[cfg(windows)]
        let inner = WindowsSpawnSupervisor::initialize_windows().map_err(|error| {
            RuntimeSpawnError::new(RuntimeSpawnStage::Initialization, error.to_string())
        })?;
        Ok(Self { inner })
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    async fn shutdown(&self) -> Result<(), RuntimeProcessShutdownError> {
        self.inner.shutdown().await.map_err(|error| {
            RuntimeProcessShutdownError::backend(format!("shared Unix reaper shutdown: {error:?}"))
        })
    }

    #[cfg(windows)]
    async fn shutdown(&self) -> Result<(), RuntimeProcessShutdownError> {
        self.inner.shutdown().await.map_err(|error| {
            RuntimeProcessShutdownError::backend(format!("Windows process shutdown: {error}"))
        })
    }

    pub(crate) async fn spawn(
        &self,
        request: RuntimeSpawnRequest,
    ) -> Result<SpawnedRuntimeProcess, RuntimeSpawnError> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        return self.spawn_unix(request).await;
        #[cfg(windows)]
        return self.spawn_windows(request).await;
        #[allow(unreachable_code)]
        Err(RuntimeSpawnError::new(
            RuntimeSpawnStage::Initialization,
            "runtime process backend is unsupported on this target",
        ))
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    async fn spawn_unix(
        &self,
        request: RuntimeSpawnRequest,
    ) -> Result<SpawnedRuntimeProcess, RuntimeSpawnError> {
        let _surface_policy = request.surface_policy;
        let _environment_dialect = request.environment_dialect;
        let _windows_job_policy = request.windows_job_policy;
        let stdio = UnixStdioPlan::new(
            request.stdin.unix(),
            request.stdout.unix(),
            request.stderr.unix(),
        )
        .with_extra(&request.unix_extra_stdio)
        .map_err(|error| {
            RuntimeSpawnError::new(
                RuntimeSpawnStage::Plan,
                format!("invalid runtime Unix extra stdio plan: {error:?}"),
            )
        })?;
        let policy = match request.image_dialect {
            ProcessImageDialect::Rust193 => SharedSpawnPolicy::rust193_inherit(request.drop_action),
            ProcessImageDialect::Node20Libuv146 => {
                SharedSpawnPolicy::node20(request.topology, request.drop_action)
            }
        };

        let image = match request.image_dialect {
            ProcessImageDialect::Rust193 => {
                if request.env.is_some() || request.topology != ProcessTopology::Inherit {
                    return Err(RuntimeSpawnError::new(
                        RuntimeSpawnStage::Plan,
                        "Rust193 compatibility surface cannot accept a runtime env or topology override",
                    ));
                }
                let file = request.file;
                let environment = request
                    .rust193_env_mutations
                    .map(|mutations| Rust193EnvironmentPlan::Overlay { mutations })
                    .unwrap_or(Rust193EnvironmentPlan::NativeParentInherit);
                let preimage = Rust193SpawnPreimage::from_deferred_parts(
                    file.clone(),
                    file,
                    request.args,
                    request.cwd,
                    environment,
                );
                UnixExecImage::build_rust193(&preimage, request.environment_guard).map_err(
                    |error| RuntimeSpawnError::new(RuntimeSpawnStage::Image, error.to_string()),
                )?
            }
            ProcessImageDialect::Node20Libuv146 => {
                let mut logical = LogicalSpawnRequest::new(request.file)
                    .with_args(request.args)
                    .with_detached(request.topology == ProcessTopology::UnixNewSession);
                if let Some(cwd) = request.cwd {
                    logical = logical.with_cwd(cwd);
                }
                if let Some(env) = request.env {
                    logical = logical.with_env(env);
                }
                let plan = logical
                    .normalize_node(&ParentEnvSnapshot::capture())
                    .map_err(|error| {
                        RuntimeSpawnError::new(RuntimeSpawnStage::Plan, error.to_string())
                    })?;
                UnixExecImage::build_node(&plan, request.environment_guard).map_err(|error| {
                    RuntimeSpawnError::new(RuntimeSpawnStage::Image, error.to_string())
                })?
            }
        };

        let spawned = match (request.image_dialect, request.pty) {
            (ProcessImageDialect::Rust193, None) => {
                #[cfg(target_os = "linux")]
                let spawned = self
                    .inner
                    .spawn_rust193_fork_shadow(image, stdio, policy)
                    .await;
                #[cfg(target_os = "macos")]
                let spawned = self
                    .inner
                    .spawn_rust193_macos_shadow(image, stdio, policy)
                    .await;
                spawned
            }
            (ProcessImageDialect::Node20Libuv146, None) => {
                #[cfg(target_os = "linux")]
                let spawned = self
                    .inner
                    .spawn_node20_fork_shadow(image, stdio, policy)
                    .await;
                #[cfg(target_os = "macos")]
                let spawned = self
                    .inner
                    .spawn_node20_macos_shadow(image, stdio, policy)
                    .await;
                spawned
            }
            (ProcessImageDialect::Node20Libuv146, Some(size)) => {
                #[cfg(target_os = "linux")]
                let spawned = self
                    .inner
                    .spawn_node20_pty_fork_shadow(
                        image,
                        size.rows,
                        size.columns,
                        size.pixel_width,
                        size.pixel_height,
                        policy,
                    )
                    .await;
                #[cfg(target_os = "macos")]
                let spawned = self
                    .inner
                    .spawn_node20_macos_pty_shadow(
                        image,
                        MacOsPtySize::new(size.rows, size.columns),
                        policy,
                    )
                    .await;
                spawned
            }
            (ProcessImageDialect::Rust193, Some(_)) => {
                return Err(RuntimeSpawnError::new(
                    RuntimeSpawnStage::Plan,
                    "Rust193 PTY requires its dedicated generated environment plan",
                ));
            }
        }
        .map_err(|error| RuntimeSpawnError::new(RuntimeSpawnStage::Spawn, error.to_string()))?;

        let (child, stdio) = spawned.into_parts();
        let controller = RuntimeProcessController {
            inner: child.controller(),
            target: request.kill_target,
        };
        let (stdin, stdout, stderr, pty, extra) = match stdio {
            ParentStdio::NullOrInherited => (None, None, None, None, Vec::new()),
            ParentStdio::Pipes {
                stdin,
                stdout,
                stderr,
                extra,
            } => (stdin, stdout, stderr, None, extra),
            stdio @ ParentStdio::Pty(_) => (None, None, None, stdio.into_pty(), Vec::new()),
        };
        Ok(SpawnedRuntimeProcess {
            child,
            controller,
            stdin: stdin.map(|stdin| RuntimeChildStdin(Some(stdin))),
            stdout: stdout.map(RuntimeChildStdout),
            stderr: stderr.map(RuntimeChildStderr),
            pty: pty.map(RuntimeChildPtyMaster),
            extra,
            context_registration: None,
        })
    }

    #[cfg(windows)]
    async fn spawn_windows(
        &self,
        mut request: RuntimeSpawnRequest,
    ) -> Result<SpawnedRuntimeProcess, RuntimeSpawnError> {
        use super::process_environment::with_process_environment_lock;

        let inherited_control = if let Some(payload) = request.windows_sandbox_request.take() {
            // The terminal size is known here and not in `transform`: whether the
            // host has a terminal attached is a property of this launch, not of
            // the policy. It is written into the request rather than sent as a
            // later control message so the wrapper can build the pseudoconsole
            // before the target starts — a target that begins life without a
            // terminal and is resized afterwards has already taken its
            // non-interactive branch.
            let payload = attach_terminal_size_to_sandbox_request(
                &payload,
                request.windows_sandbox_terminal_size,
            )
            .map_err(|error| {
                RuntimeSpawnError::new(
                    RuntimeSpawnStage::Plan,
                    format!("attach terminal size to Windows sandbox request: {error}"),
                )
            })?;
            let pipe = WindowsInheritedControlPipe::create(&payload).map_err(|error| {
                RuntimeSpawnError::new(
                    RuntimeSpawnStage::Plan,
                    format!("prepare Windows sandbox request pipe: {error}"),
                )
            })?;
            // The wrapper marker is already argument 0; the handle goes right
            // after it, which is the argv shape `parse_wrapper_argv` expects.
            request
                .args
                .push(ProcessAtom::Runtime(RuntimeString::from_utf8(
                    &pipe.child_handle().to_string(),
                )));
            Some(pipe)
        } else {
            None
        };
        let windows_sandbox_runtime_control = inherited_control
            .as_ref()
            .map(WindowsInheritedControlPipe::parent_writer);
        let _surface_policy = request.surface_policy;
        let _environment_dialect = request.environment_dialect;
        let pty_size = request.pty;
        let stdio = WindowsStdioPlan::new(
            request.stdin.windows(),
            request.stdout.windows(),
            request.stderr.windows(),
        );
        let image_dialect = request.image_dialect;
        let job_policy = request.windows_job_policy;
        let kill_target = request.kill_target;
        let drop_action = request.drop_action;
        let platform = ProductionWindowsEnvironmentPlatform::new();

        let (image, creation_flags, windows_browser_broker) = with_process_environment_lock(
            || match image_dialect {
                ProcessImageDialect::Rust193 => {
                    if request.env.is_some() || request.topology != ProcessTopology::Inherit {
                        return Err(RuntimeSpawnError::new(
                        RuntimeSpawnStage::Plan,
                        "Rust193 compatibility surface cannot accept a runtime env or topology override",
                    ));
                    }
                    let file = request.file;
                    let mut args = request.args;
                    let mut mutations = request.rust193_env_mutations.unwrap_or_default();
                    let windows_browser_broker = if let Some(seed) =
                        request.windows_browser_bootstrap_seed
                    {
                        let existing_keys =
                            windows_browser_environment_keys(&platform, &mutations)?;
                        let bootstrap = prepare_windows_browser_bootstrap(
                            &seed,
                            &existing_keys,
                            |left, right| {
                                platform
                                    .compare_ordinal_ignore_case(left, right)
                                    .map(|ordering| ordering == std::cmp::Ordering::Equal)
                                    .map_err(|_| ())
                            },
                        )
                        .map_err(|error| {
                            RuntimeSpawnError::new(RuntimeSpawnStage::Plan, error.stable_message())
                        })?;
                        args.push(ProcessAtom::Runtime(bootstrap.argv_token()));
                        mutations.push(Rust193EnvMutation::RuntimeSet {
                            key: bootstrap.environment_key(),
                            value: bootstrap.environment_value(),
                        });
                        let owner =
                            WindowsBrowserBrokerOwner::create(&bootstrap).map_err(|error| {
                                RuntimeSpawnError::new(
                                    RuntimeSpawnStage::Plan,
                                    error.stable_message(),
                                )
                            })?;
                        Some(PendingWindowsBrowserBroker {
                            owner,
                            nonce: bootstrap.nonce(),
                        })
                    } else {
                        None
                    };
                    let environment = if mutations.is_empty() {
                        Rust193EnvironmentPlan::NativeParentInherit
                    } else {
                        Rust193EnvironmentPlan::Overlay { mutations }
                    };
                    let preimage = Rust193SpawnPreimage::from_deferred_parts(
                        file.clone(),
                        file,
                        args,
                        request.cwd,
                        environment,
                    );
                    let image = WindowsExecImage::build_rust193(
                        &preimage,
                        request.environment_guard,
                        &platform,
                    )
                    .map_err(|error| {
                        RuntimeSpawnError::new(RuntimeSpawnStage::Image, error.to_string())
                    })?;
                    Ok((
                        image,
                        windows_creation_flags(request.windows_hide, false),
                        windows_browser_broker,
                    ))
                }
                ProcessImageDialect::Node20Libuv146 => {
                    if request.windows_browser_bootstrap_seed.is_some() {
                        return Err(RuntimeSpawnError::new(
                            RuntimeSpawnStage::Plan,
                            "browser bootstrap is restricted to the Rust193 sidecar surface",
                        ));
                    }
                    let mut logical = LogicalSpawnRequest::new(request.file)
                        .with_args(request.args)
                        .with_detached(request.topology == ProcessTopology::WindowsDetached)
                        .with_windows_hide(request.windows_hide);
                    if let Some(cwd) = request.cwd {
                        logical = logical.with_cwd(cwd);
                    }
                    if let Some(env) = request.env {
                        logical = logical.with_env(env);
                    }
                    let plan = logical
                        .normalize_node(&ParentEnvSnapshot::capture_unlocked())
                        .map_err(|error| {
                            RuntimeSpawnError::new(RuntimeSpawnStage::Plan, error.to_string())
                        })?;
                    let creation_flags =
                        windows_creation_flags(plan.windows_hide(), plan.detached());
                    let image =
                        WindowsExecImage::build_node(&plan, request.environment_guard, &platform)
                            .map_err(|error| {
                            RuntimeSpawnError::new(RuntimeSpawnStage::Image, error.to_string())
                        })?;
                    Ok((image, creation_flags, None))
                }
            },
        )?;

        let mut spawn_plan = WindowsSpawnPlan::new(creation_flags, job_policy, stdio, None)
            .with_target_identity_lease(kill_target == KillTarget::WindowsProcessTree);
        if let Some(control) = &inherited_control {
            spawn_plan = spawn_plan.with_additional_inherited_handle(control.child_handle());
        }
        if let Some(size) = pty_size {
            spawn_plan = spawn_plan.with_conpty(WindowsConPtySize::new(size.rows, size.columns));
        }
        let mut child = self
            .inner
            .spawn(&image, spawn_plan)
            .map_err(|error| RuntimeSpawnError::new(RuntimeSpawnStage::Spawn, error.to_string()))?;
        let controller = RuntimeProcessController {
            inner: child.controller(),
            target: kill_target,
            conpty_direct_kill: pty_size.is_some(),
            windows_sandbox_runtime_control: windows_sandbox_runtime_control.clone(),
        };
        let (stdin, stdout, stderr, pty) = if pty_size.is_some() {
            let pty = child.take_parent_pty().unwrap_or_else(|message| {
                panic!("committed Windows process returned an invalid ConPTY bundle: {message}")
            });
            (
                None,
                None,
                None,
                Some(RuntimeChildPtyMaster(WindowsRuntimePtyMaster::ConPty(pty))),
            )
        } else if request.windows_sandbox_terminal_size.is_some() {
            let files = child.take_parent_files().unwrap_or_else(|message| {
                panic!("committed Windows sandbox process returned invalid pipes: {message}")
            });
            let control = windows_sandbox_runtime_control
                .clone()
                .expect("Windows sandbox PTY requires a runtime control pipe");
            let pty = windows_sandbox_pty_master(files, control)
                .map_err(|error| RuntimeSpawnError::new(RuntimeSpawnStage::StdioShape, error))?;
            (None, None, None, Some(RuntimeChildPtyMaster(pty)))
        } else {
            let WindowsParentFiles {
                stdin,
                stdout,
                stderr,
            } = child.take_parent_files().unwrap_or_else(|message| {
                panic!("committed Windows process returned an invalid stdio bundle: {message}")
            });
            (
                stdin.map(|stdin| RuntimeChildStdin(Some(stdin))),
                stdout.map(RuntimeChildStdout),
                stderr.map(RuntimeChildStderr),
                None,
            )
        };
        Ok(SpawnedRuntimeProcess {
            child,
            controller,
            stdin,
            stdout,
            stderr,
            pty,
            drop_action,
            armed: true,
            windows_browser_broker,
            browser_sync_inbound: None,
            browser_sync_outbound: None,
            browser_mux_inbound: None,
            browser_mux_outbound: None,
            context_registration: None,
        })
    }
}

#[cfg(windows)]
fn windows_browser_environment_keys(
    platform: &ProductionWindowsEnvironmentPlatform,
    mutations: &[Rust193EnvMutation],
) -> Result<Vec<Vec<u16>>, RuntimeSpawnError> {
    let mut keys = platform
        .parent_environment()
        .map_err(|error| RuntimeSpawnError::new(RuntimeSpawnStage::Plan, error))?
        .into_iter()
        .map(|entry| entry.key().to_vec())
        .collect::<Vec<_>>();
    for mutation in mutations {
        let key = match mutation {
            Rust193EnvMutation::RuntimeSet { key, .. }
            | Rust193EnvMutation::RuntimeRemove { key } => key,
            Rust193EnvMutation::NativeHostSet(value) => value.key(),
        };
        keys.push(project_runtime_environment_key_windows(key));
    }
    Ok(keys)
}

#[cfg(windows)]
fn project_runtime_environment_key_windows(value: &RuntimeString) -> Vec<u16> {
    let mut output = Vec::with_capacity(value.len_utf16());
    for point in value.code_points() {
        let scalar = char::from_u32(point).unwrap_or(char::REPLACEMENT_CHARACTER);
        let mut units = [0_u16; 2];
        output.extend_from_slice(scalar.encode_utf16(&mut units));
    }
    output
}

#[cfg(windows)]
fn windows_creation_flags(windows_hide: bool, detached: bool) -> u32 {
    let mut flags = 0;
    if windows_hide {
        flags |= 0x0800_0000; // CREATE_NO_WINDOW
    }
    if detached {
        flags |= 0x0000_0008 | 0x0000_0200; // DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP
    }
    flags
}

#[cfg(windows)]
fn spawn_windows_taskkill_control(
    controller: &WindowsProcessController,
    action: WindowsControlAction,
) -> Result<(), RuntimeSpawnError> {
    use super::process_environment::with_process_environment_lock;

    let pid = action.pid().ok_or_else(|| {
        RuntimeSpawnError::new(
            RuntimeSpawnStage::Plan,
            "Windows tree control has no commit-bound target identity lease",
        )
    })?;
    let platform = ProductionWindowsEnvironmentPlatform::new();
    let (image, creation_flags) = with_process_environment_lock(|| {
        let executable = ProductionWindowsEnvironmentPlatform::canonical_taskkill_path()
            .map_err(|error| RuntimeSpawnError::new(RuntimeSpawnStage::Image, error))?;
        let logical =
            LogicalSpawnRequest::new(ProcessAtom::Runtime(RuntimeString::from_utf16(executable)))
                .with_args(
                    [
                        RuntimeString::from_utf8("/PID"),
                        RuntimeString::from_utf8(&pid.to_string()),
                        RuntimeString::from_utf8("/T"),
                        RuntimeString::from_utf8("/F"),
                    ]
                    .into_iter()
                    .map(ProcessAtom::Runtime)
                    .collect(),
                )
                .with_windows_hide(true);
        let plan = logical
            .normalize_node(&ParentEnvSnapshot::capture_unlocked())
            .map_err(|error| RuntimeSpawnError::new(RuntimeSpawnStage::Plan, error.to_string()))?;
        let creation_flags = windows_creation_flags(plan.windows_hide(), plan.detached());
        let image = WindowsExecImage::build_node(&plan, ProcessEnvironmentGuard::None, &platform)
            .map_err(|error| {
            RuntimeSpawnError::new(RuntimeSpawnStage::Image, error.to_string())
        })?;
        Ok((image, creation_flags))
    })?;

    controller
        .spawn_control(
            &image,
            WindowsSpawnPlan::new(
                creation_flags,
                WindowsJobPolicy::LibuvGlobal,
                WindowsStdioPlan::new(
                    WindowsStdioDisposition::Null,
                    WindowsStdioDisposition::Null,
                    WindowsStdioDisposition::Null,
                ),
                None,
            ),
            action,
        )
        .map_err(|error| RuntimeSpawnError::new(RuntimeSpawnStage::Spawn, error.to_string()))
}

#[cfg(windows)]
const BROWSER_PLAYWRIGHT_TREE_KILL_MAX_CAPTURE_BYTES: usize = 1024 * 1024;

#[cfg(windows)]
async fn execute_windows_playwright_tree_kill(
    controller: &WindowsProcessController,
    action: WindowsControlAction,
) -> Result<RuntimeExecFileOutput, RuntimeExecFileError> {
    use std::sync::atomic::{AtomicBool, AtomicUsize};

    use super::process_environment::with_process_environment_lock;

    let pid = action.pid().ok_or_else(|| {
        RuntimeExecFileError::new(
            RuntimeExecFileErrorStage::Initialization,
            "Windows Playwright tree kill has no target identity lease",
        )
    })?;
    let platform = ProductionWindowsEnvironmentPlatform::new();
    let (image, creation_flags) = with_process_environment_lock(|| {
        let taskkill = ProductionWindowsEnvironmentPlatform::canonical_taskkill_path()
            .map_err(|error| RuntimeSpawnError::new(RuntimeSpawnStage::Image, error))?;
        let cmd = windows_sibling_executable(&taskkill, "cmd.exe")?;
        let parent = ParentEnvSnapshot::capture_unlocked();
        validate_windows_playwright_comspec(&parent, &cmd)?;

        let mut command = Vec::new();
        command.extend("\"\"".encode_utf16());
        command.extend_from_slice(&taskkill);
        command.extend("\" /pid ".encode_utf16());
        command.extend(pid.to_string().encode_utf16());
        command.extend(" /T /F\"".encode_utf16());
        let cmd_atom = ProcessAtom::Runtime(RuntimeString::from_utf16(cmd));
        let logical = LogicalSpawnRequest::new(cmd_atom.clone())
            .with_argv0(cmd_atom)
            .with_args(
                [
                    RuntimeString::from_utf8("/d"),
                    RuntimeString::from_utf8("/s"),
                    RuntimeString::from_utf8("/c"),
                    RuntimeString::from_utf16(command),
                ]
                .into_iter()
                .map(ProcessAtom::Runtime)
                .collect(),
            )
            .with_windows_hide(false)
            .with_windows_verbatim_arguments(true);
        let plan = logical
            .normalize_node(&parent)
            .map_err(|error| RuntimeSpawnError::new(RuntimeSpawnStage::Plan, error.to_string()))?;
        let creation_flags = windows_creation_flags(plan.windows_hide(), plan.detached());
        let image = WindowsExecImage::build_node(&plan, ProcessEnvironmentGuard::None, &platform)
            .map_err(|error| {
            RuntimeSpawnError::new(RuntimeSpawnStage::Image, error.to_string())
        })?;
        Ok::<_, RuntimeSpawnError>((image, creation_flags))
    })
    .map_err(RuntimeExecFileError::from_spawn)?;

    let mut helper = controller
        .spawn_control_bundle(
            &image,
            WindowsSpawnPlan::new(
                creation_flags,
                WindowsJobPolicy::LibuvGlobalWithControlTracker,
                WindowsStdioPlan::new(
                    WindowsStdioDisposition::Pipe,
                    WindowsStdioDisposition::Pipe,
                    WindowsStdioDisposition::Pipe,
                ),
                None,
            ),
            action,
        )
        .map_err(|error| {
            RuntimeExecFileError::from_spawn(RuntimeSpawnError::new(
                RuntimeSpawnStage::Spawn,
                error.to_string(),
            ))
        })?;
    let waiter = helper.waiter();
    let WindowsParentFiles {
        stdin,
        stdout,
        stderr,
    } = helper
        .take_parent_files()
        .map_err(|message| RuntimeExecFileError::new(RuntimeExecFileErrorStage::Output, message))?;
    if stdin.is_none() || stdout.is_none() || stderr.is_none() {
        return Err(RuntimeExecFileError::new(
            RuntimeExecFileErrorStage::Output,
            "Windows Playwright tree kill returned an invalid three-pipe receipt",
        ));
    }
    drop(stdin);

    let captured = Arc::new(AtomicUsize::new(0));
    let overflowed = Arc::new(AtomicBool::new(false));
    let stdout_read = read_windows_playwright_tree_kill_output(
        stdout,
        "stdout",
        captured.clone(),
        overflowed.clone(),
    );
    let stderr_read =
        read_windows_playwright_tree_kill_output(stderr, "stderr", captured, overflowed.clone());
    let (stdout_result, stderr_result, status_result) =
        tokio::join!(stdout_read, stderr_read, waiter.wait());
    let stdout = stdout_result?;
    let stderr = stderr_result?;
    let code = status_result
        .map_err(|message| RuntimeExecFileError::new(RuntimeExecFileErrorStage::Wait, message))?;
    if overflowed.load(Ordering::Acquire) {
        return Err(RuntimeExecFileError::new(
            RuntimeExecFileErrorStage::Output,
            "Windows Playwright tree-kill output exceeded the shared 1048576-byte limit",
        ));
    }
    let code = i32::try_from(code).map_err(|_| {
        RuntimeExecFileError::new(
            RuntimeExecFileErrorStage::Wait,
            "Windows Playwright tree-kill exit code exceeded i32",
        )
    })?;
    drop(helper);
    Ok(RuntimeExecFileOutput {
        status: RuntimeExecFileStatus {
            code: Some(code),
            signal: None,
        },
        stdout,
        stderr,
    })
}

#[cfg(windows)]
fn windows_sibling_executable(
    sibling: &[u16],
    executable: &str,
) -> Result<Vec<u16>, RuntimeSpawnError> {
    let separator = sibling
        .iter()
        .rposition(|unit| *unit == b'\\' as u16 || *unit == b'/' as u16)
        .ok_or_else(|| {
            RuntimeSpawnError::new(
                RuntimeSpawnStage::Image,
                "canonical Windows taskkill path has no system-directory separator",
            )
        })?;
    let mut path = sibling[..=separator].to_vec();
    path.extend(executable.encode_utf16());
    Ok(path)
}

#[cfg(windows)]
fn validate_windows_playwright_comspec(
    environment: &RuntimeEnv,
    canonical_cmd: &[u16],
) -> Result<(), RuntimeSpawnError> {
    use std::ffi::OsString;
    use std::os::windows::ffi::{OsStrExt, OsStringExt};

    let value = environment
        .entries()
        .find(|(key, _)| runtime_ascii_eq_ignore_case(key, "ComSpec"))
        .map(|(_, value)| value);
    let Some(value) = value.filter(|value| !value.is_empty()) else {
        return Ok(());
    };
    let requested = std::path::PathBuf::from(OsString::from_wide(value.as_utf16()));
    let canonical = std::path::PathBuf::from(OsString::from_wide(canonical_cmd));
    let requested = crate::fs::node_path::canonicalize_host(requested).map_err(|error| {
        RuntimeSpawnError::new(
            RuntimeSpawnStage::Plan,
            format!("Playwright ComSpec resolution failed: {error}"),
        )
    })?;
    let canonical = crate::fs::node_path::canonicalize_host(canonical).map_err(|error| {
        RuntimeSpawnError::new(
            RuntimeSpawnStage::Plan,
            format!("canonical cmd.exe resolution failed: {error}"),
        )
    })?;
    let requested = requested.as_os_str().encode_wide().collect::<Vec<_>>();
    let canonical = canonical.as_os_str().encode_wide().collect::<Vec<_>>();
    if !windows_path_eq_ignore_ascii_case(&requested, &canonical) {
        return Err(RuntimeSpawnError::new(
            RuntimeSpawnStage::Plan,
            "non-empty ComSpec does not resolve to the canonical system cmd.exe",
        ));
    }
    Ok(())
}

#[cfg(windows)]
fn runtime_ascii_eq_ignore_case(value: &RuntimeString, expected: &str) -> bool {
    value.as_utf16().len() == expected.len()
        && value
            .as_utf16()
            .iter()
            .copied()
            .zip(expected.bytes())
            .all(|(left, right)| {
                left <= u16::from(u8::MAX) && (left as u8).eq_ignore_ascii_case(&right)
            })
}

#[cfg(windows)]
fn windows_path_eq_ignore_ascii_case(left: &[u16], right: &[u16]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .copied()
            .zip(right.iter().copied())
            .all(
                |(left, right)| match (u8::try_from(left), u8::try_from(right)) {
                    (Ok(left), Ok(right)) => left.eq_ignore_ascii_case(&right),
                    _ => left == right,
                },
            )
}

#[cfg(windows)]
async fn read_windows_playwright_tree_kill_output<R>(
    reader: Option<R>,
    stream: &'static str,
    captured: Arc<std::sync::atomic::AtomicUsize>,
    overflowed: Arc<std::sync::atomic::AtomicBool>,
) -> Result<Vec<u8>, RuntimeExecFileError>
where
    R: AsyncRead + Unpin,
{
    let Some(mut reader) = reader else {
        return Err(RuntimeExecFileError::new(
            RuntimeExecFileErrorStage::Output,
            format!("Windows Playwright tree-kill {stream} pipe is unavailable"),
        ));
    };
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 8192];
    loop {
        let read = reader.read(&mut chunk).await.map_err(|error| {
            RuntimeExecFileError::new(
                RuntimeExecFileErrorStage::Output,
                format!("Windows Playwright tree-kill {stream} read failed: {error}"),
            )
        })?;
        if read == 0 {
            break;
        }
        let before = captured
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_add(read))
            })
            .unwrap_or_else(|_| std::process::abort());
        let retained = BROWSER_PLAYWRIGHT_TREE_KILL_MAX_CAPTURE_BYTES
            .saturating_sub(before)
            .min(read);
        bytes.extend_from_slice(&chunk[..retained]);
        if retained != read {
            overflowed.store(true, Ordering::Release);
        }
    }
    Ok(bytes)
}

#[derive(Clone)]
pub(crate) struct RuntimeProcessController {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    inner: RuntimeChildController,
    #[cfg(windows)]
    inner: WindowsProcessController,
    target: KillTarget,
    #[cfg(windows)]
    conpty_direct_kill: bool,
    #[cfg(windows)]
    windows_sandbox_runtime_control: Option<Arc<Mutex<std::fs::File>>>,
}

impl RuntimeProcessController {
    pub(crate) fn signal(&self, signal: RuntimeSignal) {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let signal = match signal {
                RuntimeSignal::Hangup => libc::SIGHUP,
                RuntimeSignal::Interrupt => libc::SIGINT,
                RuntimeSignal::Quit => libc::SIGQUIT,
                RuntimeSignal::Terminate => libc::SIGTERM,
                RuntimeSignal::Kill => libc::SIGKILL,
                RuntimeSignal::User1 => libc::SIGUSR1,
                RuntimeSignal::User2 => libc::SIGUSR2,
            };
            let _ = self.inner.signal(self.target, signal);
        }
        #[cfg(windows)]
        {
            // Hard cancellation must not wait for the sandbox's cooperative
            // control reader: it may be blocked or never have finished booting.
            if let Some(control) = self
                .windows_sandbox_runtime_control
                .as_ref()
                .filter(|_| signal != RuntimeSignal::Kill)
            {
                let message = if signal == RuntimeSignal::Interrupt {
                    klaus_sandbox_win::protocol::ControlMessage::Interrupt
                } else {
                    klaus_sandbox_win::protocol::ControlMessage::Terminate
                };
                if write_windows_sandbox_runtime_control(control, message).is_ok() {
                    return;
                }
            }
            match self.target {
                // Windows signal names do not alter the pinned hard action.
                KillTarget::Child => {
                    let _ = self.inner.terminate_child();
                }
                KillTarget::WindowsProcessTree => {
                    let repeatable = signal == RuntimeSignal::Interrupt;
                    if let WindowsControlStart::Execute(action) =
                        self.inner.begin_control(repeatable)
                    {
                        let helper_failed =
                            spawn_windows_taskkill_control(&self.inner, action).is_err();
                        if helper_failed || self.conpty_direct_kill {
                            // Node's fixed tree-kill path falls back only when
                            // the hidden helper cannot be spawned. ConPTY also
                            // invokes its cloned direct-child killer.
                            let _ = self.inner.terminate_child();
                        }
                    }
                }
                KillTarget::WindowsPrivateJob => {
                    let _ = self.inner.terminate_private_job();
                }
                KillTarget::UnixProcessGroup => {
                    unreachable!("Unix process-group target reached Windows controller")
                }
            }
        }
    }

    fn try_wait_terminal(
        &self,
    ) -> Result<Option<RuntimeProcessStatus>, RuntimeProcessShutdownError> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            return match self.inner.try_wait() {
                None => Ok(None),
                Some(Ok(status)) => Ok(Some(RuntimeProcessStatus {
                    code: status.code(),
                    signal: status.signal(),
                })),
                Some(Err(error)) => Err(RuntimeProcessShutdownError::backend(format!(
                    "shared Unix reaper invariant: {error:?}"
                ))),
            };
        }
        #[cfg(windows)]
        {
            return Ok(self.inner.try_wait().map(|code| RuntimeProcessStatus {
                code: Some(code as i32),
                signal: None,
            }));
        }
        #[allow(unreachable_code)]
        Ok(None)
    }
}

/// Put this launch's terminal size into a serialized sandbox request.
///
/// The request is re-serialized rather than patched textually: the wrapper rejects a
/// frame it cannot parse, and hand-editing JSON is how a field ends up in the wrong
/// place. `None` is written explicitly, because absent and "no terminal" have to be
/// the same thing to the wrapper.
#[cfg(windows)]
fn attach_terminal_size_to_sandbox_request(
    payload: &[u8],
    size: Option<RuntimePtySize>,
) -> Result<Vec<u8>, String> {
    let mut request: klaus_sandbox_win::protocol::NativeSandboxRequest =
        serde_json::from_slice(payload).map_err(|error| error.to_string())?;
    request.request.terminal = size.map(|size| klaus_sandbox_win::protocol::TerminalSize {
        rows: size.rows,
        cols: size.columns,
    });
    // Framed here, so the pipe carries exactly what `protocol::read_frame` expects.
    let mut framed = Vec::new();
    klaus_sandbox_win::protocol::write_frame(&mut framed, &request)
        .map_err(|error| error.to_string())?;
    Ok(framed)
}

#[cfg(windows)]
fn write_windows_sandbox_runtime_control(
    writer: &Arc<Mutex<std::fs::File>>,
    control: klaus_sandbox_win::protocol::ControlMessage,
) -> std::io::Result<()> {
    let mut writer = writer
        .lock()
        .map_err(|_| std::io::Error::other("Windows sandbox control lock poisoned"))?;
    // Same framing as the request that preceded it on this pipe.
    klaus_sandbox_win::protocol::write_frame(&mut *writer, &control)
        .map_err(|error| std::io::Error::other(error.to_string()))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeSignal {
    // POSIX-only signals: constructed and consumed solely by the unix signal
    // path (tools::shell mapping and the controller's libc dispatch). Windows
    // always falls back to the pinned hard tree-control action.
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    Hangup,
    Interrupt,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    Quit,
    Terminate,
    Kill,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    User1,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    User2,
}

pub(crate) struct SpawnedRuntimeProcess {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    child: RuntimeChild,
    #[cfg(windows)]
    child: WindowsSpawnBundle,
    controller: RuntimeProcessController,
    stdin: Option<RuntimeChildStdin>,
    stdout: Option<RuntimeChildStdout>,
    stderr: Option<RuntimeChildStderr>,
    pty: Option<RuntimeChildPtyMaster>,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    extra: Vec<RuntimeExtraPipe>,
    #[cfg(windows)]
    drop_action: RuntimeDropAction,
    #[cfg(windows)]
    armed: bool,
    #[cfg(windows)]
    windows_browser_broker: Option<PendingWindowsBrowserBroker>,
    #[cfg(windows)]
    browser_sync_inbound: Option<RuntimeBrowserBrokerRead>,
    #[cfg(windows)]
    browser_sync_outbound: Option<RuntimeBrowserBrokerWrite>,
    #[cfg(windows)]
    browser_mux_inbound: Option<RuntimeBrowserBrokerRead>,
    #[cfg(windows)]
    browser_mux_outbound: Option<RuntimeBrowserBrokerWrite>,
    context_registration: Option<ContextProcessLease>,
}

#[cfg(windows)]
struct PendingWindowsBrowserBroker {
    owner: WindowsBrowserBrokerOwner,
    nonce: [u8; 32],
}

impl SpawnedRuntimeProcess {
    pub(crate) fn controller(&self) -> RuntimeProcessController {
        self.controller.clone()
    }

    fn browser_pid(&self) -> u32 {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            self.child.pid_for_browser()
        }
        #[cfg(windows)]
        {
            self.child.identity().pid()
        }
    }

    pub(crate) fn take_stdin(&mut self) -> Option<RuntimeChildStdin> {
        self.stdin.take()
    }

    pub(crate) fn take_stdout(&mut self) -> Option<RuntimeChildStdout> {
        self.stdout.take()
    }

    pub(crate) fn take_stderr(&mut self) -> Option<RuntimeChildStderr> {
        self.stderr.take()
    }

    pub(crate) fn take_pty(&mut self) -> Option<RuntimeChildPtyMaster> {
        self.pty.take()
    }

    #[cfg(windows)]
    async fn connect_windows_browser_broker(
        &mut self,
        deadline: Instant,
    ) -> Result<([u8; 32], u32), super::windows_browser_broker::WindowsBrowserBrokerError> {
        use super::windows_browser_broker::WindowsBrowserBrokerError;

        let PendingWindowsBrowserBroker { owner, nonce } = self
            .windows_browser_broker
            .take()
            .ok_or(WindowsBrowserBrokerError::PipeCreation)?;
        let connected = owner.connect(deadline).await?;
        self.child
            .verify_browser_client_identity(connected.client_pid)
            .map_err(|_| WindowsBrowserBrokerError::PipeClientIdentity)?;
        self.browser_sync_inbound = Some(RuntimeBrowserBrokerRead(connected.sync_c2h));
        self.browser_sync_outbound = Some(RuntimeBrowserBrokerWrite(connected.sync_h2c));
        self.browser_mux_inbound = Some(RuntimeBrowserBrokerRead(connected.mux_c2h));
        self.browser_mux_outbound = Some(RuntimeBrowserBrokerWrite(connected.mux_h2c));
        Ok((nonce, connected.client_pid))
    }

    fn take_browser_broker_read(&mut self, child_fd: u32) -> Option<RuntimeBrowserBrokerRead> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let index = self
                .extra
                .iter()
                .position(|pipe| pipe.child_fd() == child_fd)?;
            match self.extra.remove(index).into_parent() {
                RuntimeExtraPipeEndpoint::Read(endpoint) => {
                    Some(RuntimeBrowserBrokerRead(endpoint))
                }
                RuntimeExtraPipeEndpoint::Write(_) | RuntimeExtraPipeEndpoint::Duplex(_) => None,
            }
        }
        #[cfg(windows)]
        {
            match child_fd {
                3 => self.browser_sync_inbound.take(),
                5 => self.browser_mux_inbound.take(),
                _ => None,
            }
        }
    }

    fn take_browser_broker_write(&mut self, child_fd: u32) -> Option<RuntimeBrowserBrokerWrite> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let index = self
                .extra
                .iter()
                .position(|pipe| pipe.child_fd() == child_fd)?;
            match self.extra.remove(index).into_parent() {
                RuntimeExtraPipeEndpoint::Write(endpoint) => {
                    Some(RuntimeBrowserBrokerWrite(endpoint))
                }
                RuntimeExtraPipeEndpoint::Read(_) | RuntimeExtraPipeEndpoint::Duplex(_) => None,
            }
        }
        #[cfg(windows)]
        {
            match child_fd {
                4 => self.browser_sync_outbound.take(),
                6 => self.browser_mux_outbound.take(),
                _ => None,
            }
        }
    }

    fn take_browser_child_duplex(&mut self, child_fd: u32) -> Option<RuntimeBrowserChildDuplex> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let index = self
                .extra
                .iter()
                .position(|pipe| pipe.child_fd() == child_fd)?;
            match self.extra.remove(index).into_parent() {
                RuntimeExtraPipeEndpoint::Duplex(endpoint) => {
                    Some(RuntimeBrowserChildDuplex(endpoint))
                }
                RuntimeExtraPipeEndpoint::Read(_) | RuntimeExtraPipeEndpoint::Write(_) => None,
            }
        }
        #[cfg(windows)]
        {
            let _ = child_fd;
            None
        }
    }

    pub(crate) async fn wait_terminal(
        &mut self,
    ) -> Result<RuntimeProcessStatus, RuntimeSpawnError> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let outcome = self.child.waiter().wait().await;
            match outcome {
                Ok(status) => Ok(RuntimeProcessStatus {
                    code: status.code(),
                    signal: status.signal(),
                }),
                Err(error) => Err(RuntimeSpawnError::new(
                    RuntimeSpawnStage::Wait,
                    format!("{error:?}"),
                )),
            }
        }
        #[cfg(windows)]
        {
            let code = self
                .child
                .waiter()
                .wait()
                .await
                .map_err(|error| RuntimeSpawnError::new(RuntimeSpawnStage::Wait, error))?;
            Ok(RuntimeProcessStatus {
                code: Some(code as i32),
                signal: None,
            })
        }
    }

    pub(crate) async fn wait(&mut self) -> Result<RuntimeProcessStatus, RuntimeSpawnError> {
        let status = self.wait_terminal().await?;
        self.disarm();
        Ok(status)
    }

    pub(crate) fn disarm(&mut self) {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        self.child.disarm();
        #[cfg(windows)]
        {
            self.armed = false;
        }
        if let Some(mut registration) = self.context_registration.take() {
            registration.disarm();
        }
    }
}

#[cfg(windows)]
impl Drop for SpawnedRuntimeProcess {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let RuntimeDropAction::HardKill(_) = self.drop_action {
            self.controller.signal(RuntimeSignal::Kill);
            self.armed = false;
        }
    }
}

pub struct RuntimeChildStdin(
    #[cfg(any(target_os = "linux", target_os = "macos"))] Option<UnixChildStdin>,
    #[cfg(windows)] Option<tokio::fs::File>,
);
pub struct RuntimeChildStdout(
    #[cfg(any(target_os = "linux", target_os = "macos"))] UnixChildStdout,
    #[cfg(windows)] tokio::fs::File,
);
pub struct RuntimeChildStderr(
    #[cfg(any(target_os = "linux", target_os = "macos"))] UnixChildStderr,
    #[cfg(windows)] tokio::fs::File,
);
#[cfg(windows)]
enum WindowsRuntimePtyMaster {
    ConPty(WindowsConPtyMaster),
    Sandbox(WindowsSandboxPtyMaster),
}
#[cfg(windows)]
enum WindowsRuntimePtyReader {
    ConPty(WindowsConPtyReader),
    Sandbox(tokio::io::ReadHalf<tokio::io::DuplexStream>),
}
#[cfg(windows)]
enum WindowsRuntimePtyWriter {
    ConPty(WindowsConPtyWriter),
    Sandbox(WindowsSandboxPtyWriter),
}
#[cfg(windows)]
struct WindowsSandboxPtyMaster {
    io: tokio::io::DuplexStream,
    control: Arc<Mutex<std::fs::File>>,
}
#[cfg(windows)]
struct WindowsSandboxPtyWriter {
    writer: tokio::io::WriteHalf<tokio::io::DuplexStream>,
    control: Arc<Mutex<std::fs::File>>,
}
/// Typed parent reader for one fixed browser broker lane. No raw descriptor or
/// caller-selected fd is exposed outside the process backend.
pub struct RuntimeBrowserBrokerRead(
    #[cfg(any(target_os = "linux", target_os = "macos"))] UnixChildExtraRead,
    #[cfg(windows)] tokio::net::windows::named_pipe::NamedPipeServer,
);
/// Typed parent writer for one fixed browser broker lane. No raw descriptor or
/// caller-selected fd is exposed outside the process backend.
pub struct RuntimeBrowserBrokerWrite(
    #[cfg(any(target_os = "linux", target_os = "macos"))] UnixChildExtraWrite,
    #[cfg(windows)] tokio::net::windows::named_pipe::NamedPipeServer,
);
/// Typed duplex endpoint for Playwright child stdio slots three and four.
pub struct RuntimeBrowserChildDuplex(
    #[cfg(any(target_os = "linux", target_os = "macos"))] UnixChildExtraDuplex,
    #[cfg(windows)] tokio::fs::File,
);
pub(crate) struct RuntimeChildPtyMaster(
    #[cfg(any(target_os = "linux", target_os = "macos"))] UnixPtyMaster,
    #[cfg(windows)] WindowsRuntimePtyMaster,
);
pub(crate) struct RuntimeChildPtyReader(
    #[cfg(any(target_os = "linux", target_os = "macos"))] UnixPtyRead,
    #[cfg(windows)] WindowsRuntimePtyReader,
);
pub(crate) struct RuntimeChildPtyWriter(
    #[cfg(any(target_os = "linux", target_os = "macos"))] UnixPtyWrite,
    #[cfg(windows)] WindowsRuntimePtyWriter,
);

impl AsyncWrite for RuntimeChildStdin {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.0.as_mut() {
            Some(stdin) => Pin::new(stdin).poll_write(context, buffer),
            None => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "runtime child stdin is closed",
            ))),
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.0.as_mut() {
            Some(stdin) => Pin::new(stdin).poll_flush(context),
            None => Poll::Ready(Ok(())),
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        if let Some(stdin) = self.0.as_mut() {
            // 底层 shutdown 不会关闭管道。先排空写入，再释放写端发送 EOF。
            std::task::ready!(Pin::new(stdin).poll_shutdown(context))?;
            self.0.take();
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncRead for RuntimeBrowserBrokerRead {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_read(context, buffer)
    }
}

impl AsyncWrite for RuntimeBrowserBrokerWrite {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(context, buffer)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(context)
    }
}

impl AsyncRead for RuntimeBrowserChildDuplex {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_read(context, buffer)
    }
}

impl AsyncWrite for RuntimeBrowserChildDuplex {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(context, buffer)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(context)
    }
}

impl AsyncWrite for RuntimeChildPtyMaster {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(context, buffer)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(context)
    }
}

impl RuntimeChildPtyMaster {
    pub(crate) fn split(self) -> std::io::Result<(RuntimeChildPtyReader, RuntimeChildPtyWriter)> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let (reader, writer) = self.0.split()?;
            Ok((RuntimeChildPtyReader(reader), RuntimeChildPtyWriter(writer)))
        }
        #[cfg(windows)]
        {
            let (reader, writer) = match self.0 {
                WindowsRuntimePtyMaster::ConPty(master) => {
                    let (reader, writer) = master.split();
                    (
                        WindowsRuntimePtyReader::ConPty(reader),
                        WindowsRuntimePtyWriter::ConPty(writer),
                    )
                }
                WindowsRuntimePtyMaster::Sandbox(master) => {
                    let (reader, writer) = tokio::io::split(master.io);
                    (
                        WindowsRuntimePtyReader::Sandbox(reader),
                        WindowsRuntimePtyWriter::Sandbox(WindowsSandboxPtyWriter {
                            writer,
                            control: master.control,
                        }),
                    )
                }
            };
            Ok((RuntimeChildPtyReader(reader), RuntimeChildPtyWriter(writer)))
        }
    }
}

impl AsyncWrite for RuntimeChildPtyWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(context, buffer)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(context)
    }
}

impl RuntimeChildPtyWriter {
    /// Whether this writer feeds a Windows pseudoconsole directly. The sandbox
    /// pty bridge instead terminates in a unix-style pty inside the sandbox,
    /// whose line discipline performs its own input mapping.
    #[cfg(windows)]
    pub(crate) fn is_conpty(&self) -> bool {
        matches!(self.0, WindowsRuntimePtyWriter::ConPty(_))
    }

    pub(crate) fn resize(
        &self,
        rows: u16,
        columns: u16,
        pixel_width: u16,
        pixel_height: u16,
    ) -> std::io::Result<()> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            self.0.resize(rows, columns, pixel_width, pixel_height)
        }
        #[cfg(windows)]
        {
            let _ = (pixel_width, pixel_height);
            match &self.0 {
                WindowsRuntimePtyWriter::ConPty(writer) => writer.resize(rows, columns),
                WindowsRuntimePtyWriter::Sandbox(writer) => write_windows_sandbox_runtime_control(
                    &writer.control,
                    klaus_sandbox_win::protocol::ControlMessage::Resize(
                        klaus_sandbox_win::protocol::TerminalSize {
                            rows,
                            cols: columns,
                        },
                    ),
                ),
            }
        }
    }
}

#[cfg(windows)]
impl AsyncRead for WindowsRuntimePtyMaster {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match &mut *self {
            Self::ConPty(master) => Pin::new(master).poll_read(context, buffer),
            Self::Sandbox(master) => Pin::new(&mut master.io).poll_read(context, buffer),
        }
    }
}

#[cfg(windows)]
impl AsyncWrite for WindowsRuntimePtyMaster {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match &mut *self {
            Self::ConPty(master) => Pin::new(master).poll_write(context, buffer),
            Self::Sandbox(master) => Pin::new(&mut master.io).poll_write(context, buffer),
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        match &mut *self {
            Self::ConPty(master) => Pin::new(master).poll_flush(context),
            Self::Sandbox(master) => Pin::new(&mut master.io).poll_flush(context),
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        match &mut *self {
            Self::ConPty(master) => Pin::new(master).poll_shutdown(context),
            Self::Sandbox(master) => Pin::new(&mut master.io).poll_shutdown(context),
        }
    }
}

#[cfg(windows)]
impl AsyncRead for WindowsRuntimePtyReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match &mut *self {
            Self::ConPty(reader) => Pin::new(reader).poll_read(context, buffer),
            Self::Sandbox(reader) => Pin::new(reader).poll_read(context, buffer),
        }
    }
}

#[cfg(windows)]
impl AsyncWrite for WindowsRuntimePtyWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match &mut *self {
            Self::ConPty(writer) => Pin::new(writer).poll_write(context, buffer),
            Self::Sandbox(writer) => Pin::new(&mut writer.writer).poll_write(context, buffer),
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        match &mut *self {
            Self::ConPty(writer) => Pin::new(writer).poll_flush(context),
            Self::Sandbox(writer) => Pin::new(&mut writer.writer).poll_flush(context),
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        match &mut *self {
            Self::ConPty(writer) => Pin::new(writer).poll_shutdown(context),
            Self::Sandbox(writer) => Pin::new(&mut writer.writer).poll_shutdown(context),
        }
    }
}

#[cfg(windows)]
fn windows_sandbox_pty_master(
    files: WindowsParentFiles,
    control: Arc<Mutex<std::fs::File>>,
) -> Result<WindowsRuntimePtyMaster, String> {
    let stdin = files
        .stdin
        .ok_or_else(|| "Windows sandbox PTY stdin pipe is unavailable".to_string())?;
    let stdout = files
        .stdout
        .ok_or_else(|| "Windows sandbox PTY stdout pipe is unavailable".to_string())?;
    let stderr = files
        .stderr
        .ok_or_else(|| "Windows sandbox PTY stderr pipe is unavailable".to_string())?;
    let (frontend, bridge) = tokio::io::duplex(64 * 1024);
    let (mut bridge_reader, mut bridge_writer) = tokio::io::split(bridge);
    tokio::spawn(async move {
        let mut stdin = stdin;
        let _ = tokio::io::copy(&mut bridge_reader, &mut stdin).await;
        let _ = stdin.shutdown().await;
    });
    let (output_tx, mut output_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(64);
    spawn_windows_sandbox_output_pump(stdout, output_tx.clone());
    spawn_windows_sandbox_output_pump(stderr, output_tx);
    tokio::spawn(async move {
        while let Some(bytes) = output_rx.recv().await {
            if bridge_writer.write_all(&bytes).await.is_err() {
                break;
            }
        }
        let _ = bridge_writer.shutdown().await;
    });
    Ok(WindowsRuntimePtyMaster::Sandbox(WindowsSandboxPtyMaster {
        io: frontend,
        control,
    }))
}

#[cfg(windows)]
fn spawn_windows_sandbox_output_pump(
    mut reader: tokio::fs::File,
    output: tokio::sync::mpsc::Sender<Vec<u8>>,
) {
    tokio::spawn(async move {
        let mut buffer = vec![0_u8; 8 * 1024];
        loop {
            let read = match reader.read(&mut buffer).await {
                Ok(0) | Err(_) => break,
                Ok(read) => read,
            };
            if output.send(buffer[..read].to_vec()).await.is_err() {
                break;
            }
        }
    });
}

macro_rules! impl_async_read {
    ($type:ident) => {
        impl AsyncRead for $type {
            fn poll_read(
                mut self: Pin<&mut Self>,
                context: &mut Context<'_>,
                buffer: &mut ReadBuf<'_>,
            ) -> Poll<std::io::Result<()>> {
                Pin::new(&mut self.0).poll_read(context, buffer)
            }
        }
    };
}

impl_async_read!(RuntimeChildStdout);
impl_async_read!(RuntimeChildStderr);
impl_async_read!(RuntimeChildPtyMaster);
impl_async_read!(RuntimeChildPtyReader);

pub(crate) fn parent_runtime_env_value(key: &str) -> Option<RuntimeString> {
    ParentEnvSnapshot::capture()
        .get(&RuntimeString::from(key))
        .cloned()
}

#[cfg(test)]
mod stdin_shutdown_tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    // 2026-09-11 inspection 回归：shutdown 必须发送 EOF，而不只是 flush。
    // 保留 stdin 对象直到子进程退出，避免对象析构掩盖关闭接口的错误。
    async fn assert_shutdown_delivers_eof(payload: Vec<u8>) {
        #[cfg(unix)]
        let request = RuntimeSpawnRequest::new("/bin/cat".into());
        #[cfg(windows)]
        let request = {
            let system_root = std::env::var_os("SystemRoot").expect("Windows system root");
            let executable =
                PathBuf::from(system_root).join("System32/WindowsPowerShell/v1.0/powershell.exe");
            RuntimeSpawnRequest::new(RuntimeString::from_utf8(
                executable.to_str().expect("PowerShell path"),
            ))
            .with_args(vec![
                "-NoLogo".into(),
                "-NoProfile".into(),
                "-NonInteractive".into(),
                "-Command".into(),
                "[Console]::OpenStandardInput().CopyTo([Console]::OpenStandardOutput())".into(),
            ])
        };
        let mut child = test_runtime_process_context()
            .spawner()
            .spawn(
                request
                    .with_stdio(RuntimeStdio::Pipe, RuntimeStdio::Pipe, RuntimeStdio::Pipe)
                    .with_hard_kill_on_drop(true),
            )
            .await
            .expect("spawn EOF echo worker");
        let mut stdin = child.take_stdin().expect("worker stdin");
        let mut stdout = child.take_stdout().expect("worker stdout");
        let mut stderr = child.take_stderr().expect("worker stderr");
        let mut output = Vec::new();
        let mut errors = Vec::new();
        let completion = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(
                async {
                    stdin.write_all(&payload).await?;
                    stdin.shutdown().await
                },
                stdout.read_to_end(&mut output),
                stderr.read_to_end(&mut errors),
                child.wait_terminal(),
            )
        })
        .await;
        if completion.is_err() {
            child.controller().signal(RuntimeSignal::Kill);
            tokio::time::timeout(Duration::from_secs(5), child.wait_terminal())
                .await
                .expect("worker cleanup deadline")
                .expect("worker cleanup");
        }
        child.disarm();
        let (written, read, error_read, status) =
            completion.expect("shutdown must close the pipe while stdin remains alive");
        written.expect("write and shutdown");
        read.expect("read stdout through EOF");
        error_read.expect("read stderr through EOF");
        assert_eq!(status.expect("worker exit").code(), Some(0));
        assert_eq!(output, payload);
        assert!(errors.is_empty(), "unexpected stderr: {errors:?}");
        stdin.shutdown().await.expect("repeated shutdown");
        stdin.flush().await.expect("flush after shutdown");
        assert_eq!(
            stdin.write_all(b"after-close").await.unwrap_err().kind(),
            std::io::ErrorKind::BrokenPipe,
        );
    }

    #[tokio::test]
    async fn shutdown_empty_stdin_delivers_eof_before_drop() {
        assert_shutdown_delivers_eof(Vec::new()).await;
    }

    #[tokio::test]
    async fn shutdown_drains_large_binary_input_before_eof() {
        let payload = (0..256 * 1024).map(|index| (index % 256) as u8).collect();
        assert_shutdown_delivers_eof(payload).await;
    }
}

#[cfg(all(test, windows))]
mod windows_sandbox_single_pty_tests {
    use super::*;
    use crate::sandbox::windows::WindowsSandbox;
    use crate::types::sandbox::{ExecRequest, SandboxBackend, SandboxPolicy};

    #[test]
    fn native_sandbox_terminal_frame_preserves_the_captured_execution_level() {
        use klaus_sandbox_win::protocol::{
            NativeSandboxContext, NativeSandboxLevel, NativeSandboxRequest, SandboxRequest,
        };
        for level in [NativeSandboxLevel::Unelevated, NativeSandboxLevel::Elevated] {
            let envelope = NativeSandboxRequest {
                request: SandboxRequest {
                    command: vec!["cmd.exe".into()],
                    cwd: klaus_sandbox_win::AbsPath::new(r"C:\workspace").unwrap(),
                    env: Default::default(),
                    filesystem: Default::default(),
                    network: klaus_sandbox_win::NetworkBoundary::Denied,
                    network_route_capability_sid: None,
                    container_name: "native-terminal-frame".into(),
                    terminal: None,
                },
                native: Some(NativeSandboxContext {
                    level,
                    config_home: r"C:\config".into(),
                    unconfined_writes: false,
                }),
            };
            let bytes = attach_terminal_size_to_sandbox_request(
                &serde_json::to_vec(&envelope).unwrap(),
                Some(RuntimePtySize {
                    rows: 24,
                    columns: 80,
                    pixel_width: 0,
                    pixel_height: 0,
                }),
            )
            .unwrap();
            let framed: NativeSandboxRequest =
                klaus_sandbox_win::protocol::read_frame(&mut std::io::Cursor::new(bytes))
                    .unwrap()
                    .unwrap();
            assert_eq!(framed.native, envelope.native);
            assert_eq!(framed.request.command, envelope.request.command);
            assert_eq!(framed.request.terminal.unwrap().cols, 80);
        }
    }

    #[test]
    fn windows_sandbox_tty_uses_wrapper_pipes_and_inner_terminal_control() {
        let cwd = std::env::current_dir().expect("current directory");
        let request = ExecRequest {
            argv: vec![RuntimeString::from_utf8("cmd.exe")],
            cwd: RuntimePath::from(cwd.to_string_lossy().into_owned()),
            env: None,
            timeout_ms: None,
        };
        let spec = WindowsSandbox::new().transform(&request, &SandboxPolicy::default());
        let spawn = RuntimeSpawnRequest::from_spawn_spec(
            spec,
            RuntimeProcessSurface::InteractivePty {
                rows: 41,
                columns: 132,
            },
        );

        assert_eq!(spawn.pty, None);
        assert_eq!(
            spawn.windows_sandbox_terminal_size,
            Some(RuntimePtySize {
                rows: 41,
                columns: 132,
                pixel_width: 0,
                pixel_height: 0,
            })
        );
        assert!(spawn.windows_sandbox_request.is_some());
        assert_eq!(spawn.stdin, RuntimeStdio::Pipe);
        assert_eq!(spawn.stdout, RuntimeStdio::Pipe);
        assert_eq!(spawn.stderr, RuntimeStdio::Pipe);
    }
}
