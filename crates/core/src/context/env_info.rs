//! The system-prompt `# Environment` block.
//! Gives the model the environment facts it needs: the
//! working directory, whether it's a git repo, the platform/OS/shell, which
//! model it is, and where its own extensions are configured.

use crate::extensions::skills::loader::skill_search_roots_exact;
use crate::git::git::is_repo;
use crate::mcp::config_file::{project_mcp_config_path_exact, user_mcp_config_path_exact};
use crate::runtime_process::{
    path_resolve, NodePathDialect, NodePathHostPlatform, RuntimePathCwdSnapshot,
};
#[cfg(unix)]
use crate::runtime_values::NodeBufferUtf8;
use crate::runtime_values::{RuntimePath, RuntimeString};
use crate::schemas::config::ResolvedConfig;
use crate::tools::shell::{process_env, resolve_windows_shell, ShellKind, ShellResolution};

/// The platform label: `win32`/`darwin` pass through, every
/// other POSIX normalizes to `linux`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    Win32,
    Darwin,
    Linux,
}

impl Platform {
    pub fn as_str(&self) -> &'static str {
        match self {
            Platform::Win32 => "win32",
            Platform::Darwin => "darwin",
            Platform::Linux => "linux",
        }
    }
}

/// Normalize a raw platform id (e.g. `std::env::consts::OS` or a
/// `win32`/`darwin`-style value) to a [`Platform`].
pub fn platform_name(platform: &str) -> Platform {
    match platform {
        "win32" | "windows" => Platform::Win32,
        "darwin" | "macos" => Platform::Darwin,
        _ => Platform::Linux,
    }
}

/// The current process platform.
pub fn current_platform() -> Platform {
    platform_name(std::env::consts::OS)
}

/// `uname(2)` sysname + release (the OS type and release strings).
#[cfg(unix)]
fn uname_sr() -> (String, String) {
    use std::ffi::CStr;
    // SAFETY: `uts` is zero-initialized and `uname` fills it; we read NUL-terminated fields.
    unsafe {
        let mut uts: libc::utsname = std::mem::zeroed();
        if libc::uname(&mut uts) == 0 {
            let sysname = CStr::from_ptr(uts.sysname.as_ptr())
                .to_string_lossy()
                .into_owned();
            let release = CStr::from_ptr(uts.release.as_ptr())
                .to_string_lossy()
                .into_owned();
            (sysname, release)
        } else {
            (String::new(), String::new())
        }
    }
}

/// The OS type and release strings, as `uname(2)` reports them where it exists.
///
/// Windows has no `uname`, and returning nothing there left every model request carrying
/// an empty `OS Version` line — the model was told the host had no operating system
/// version at all. Node answers this with libuv's `uv_os_uname`, which reads the real
/// version out of `ntdll` and reports `Windows_NT <major>.<minor>.<build>`; the same pair
/// is produced here so the line means the same thing on every platform.
#[cfg(windows)]
fn uname_sr() -> (String, String) {
    use windows_sys::Wdk::System::SystemServices::RtlGetVersion;
    use windows_sys::Win32::System::SystemInformation::OSVERSIONINFOW;

    let mut info = OSVERSIONINFOW {
        dwOSVersionInfoSize: u32::try_from(std::mem::size_of::<OSVERSIONINFOW>())
            .expect("OSVERSIONINFOW fits in a u32"),
        ..Default::default()
    };
    // SAFETY: `info` is a fully initialized OSVERSIONINFOW whose size field is set, which
    // is the whole contract of RtlGetVersion. Unlike GetVersionExW it is not gated on an
    // application manifest, so it reports the running build rather than 6.2.
    let status = unsafe { RtlGetVersion(&mut info) };
    if status < 0 {
        return (String::new(), String::new());
    }
    (
        "Windows_NT".to_string(),
        format!(
            "{}.{}.{}",
            info.dwMajorVersion, info.dwMinorVersion, info.dwBuildNumber
        ),
    )
}

#[cfg(not(any(unix, windows)))]
fn uname_sr() -> (String, String) {
    (String::new(), String::new())
}

/// The OS Version line value: on POSIX `uname -sr` (e.g. "Darwin 25.4.0"), on Windows the
/// same shape from the native version query (e.g. "Windows_NT 10.0.26100").
pub fn os_version_line(plat: Platform) -> String {
    let (sysname, release) = uname_sr();
    match plat {
        Platform::Win32 => format!("{sysname} {release}"),
        _ => format!("{sysname} {release}"),
    }
}

/// The POSIX Shell line: `$SHELL` collapsed to `zsh`/`bash`
/// when it contains those, else the raw value.
///
/// Windows does not use this. `$SHELL` is a POSIX convention that is usually
/// unset there, and the shell that actually executes a command is chosen by a
/// separate resolver with a fallback chain, so the two could disagree. Windows
/// reports the resolved shell through [`windows_shell_line`] instead. The
/// `plat` argument is retained for call-site stability.
pub fn shell_line(_plat: Platform, shell: &str) -> String {
    let shell_name = if shell.contains("zsh") {
        "zsh"
    } else if shell.contains("bash") {
        "bash"
    } else {
        shell
    };
    format!("Shell: {shell_name}")
}

/// The Windows Shell line, derived from the shell the command runtime will
/// actually execute rather than from `$SHELL`.
///
/// Reporting an unconditional "use Unix shell syntax" note here was wrong
/// whenever the resolver fell back past bash: in PowerShell the advice sends
/// the model to `/dev/null` instead of `$null`, and in `cmd.exe` its built-in
/// `find` is a text searcher rather than the POSIX file finder, so a
/// POSIX-shaped `find` returns plausible-looking wrong results instead of
/// failing. Each kind now states which shell runs, which syntax to write, and
/// what to use in place of the missing coreutils.
pub fn windows_shell_line(resolution: &ShellResolution) -> String {
    let path = &resolution.path;
    match resolution.kind {
        ShellKind::Bash => format!(
            "Shell: bash ({path}) — a real POSIX shell (Git for Windows / MSYS or WSL), so \
             commands run verbatim with coreutils available. Write POSIX shell syntax."
        ),
        ShellKind::Pwsh | ShellKind::Powershell => {
            let name = if resolution.kind == ShellKind::Pwsh {
                "PowerShell 7 (pwsh)"
            } else {
                "Windows PowerShell"
            };
            format!(
                "Shell: {name} at {path} — no POSIX shell is installed, so commands run in \
                 PowerShell rather than bash. POSIX syntax and coreutils (grep, find, ls, cat, \
                 sed, awk, xargs, head, tail, wc) are unavailable and will fail. Prefer the \
                 dedicated glob, grep, file_search, read and ls tools for searching and reading. \
                 When the shell is genuinely needed, ripgrep is on PATH as `rg` (`rg PATTERN` to \
                 search contents, `rg --files` to list files); use `$null`, not /dev/null."
            )
        }
        ShellKind::Cmd => format!(
            "Shell: cmd.exe at {path} — neither a POSIX shell nor PowerShell was found, so \
             commands run in the Windows command prompt. POSIX syntax and coreutils (grep, ls, \
             cat, sed, awk, xargs, head, tail, wc) are unavailable. The built-in `find` is a \
             TEXT searcher with unrelated syntax, not the POSIX file finder — never use it to \
             locate files. Prefer the dedicated glob, grep, file_search, read and ls tools for \
             searching and reading. When the shell is genuinely needed, ripgrep is on PATH as \
             `rg` (`rg PATTERN` to search contents, `rg --files` to list files); use `NUL`, not \
             /dev/null."
        ),
    }
}

fn default_shell() -> String {
    std::env::var("SHELL").unwrap_or_else(|_| "unknown".to_string())
}

/// Options for [`environment_block`].
#[derive(Debug, Clone, Default)]
pub struct EnvBlockOptions {
    pub plat: Option<Platform>,
    pub shell: Option<String>,
    pub model: Option<RuntimeString>,
    pub is_worktree: bool,
    /// Windows only: the shell resolution to report. `None` resolves it the
    /// same way the command runtime does, so the block cannot disagree with
    /// the shell that executes the command. Pinned by tests for determinism.
    pub windows_shell: Option<ShellResolution>,
}

/// Assemble the `# Environment` markdown block.
pub fn environment_block(
    cwd: &RuntimePath,
    config: &ResolvedConfig,
    opts: EnvBlockOptions,
) -> RuntimeString {
    let plat = opts.plat.unwrap_or_else(current_platform);
    let shell = opts.shell.unwrap_or_else(default_shell);
    // Resolve each additional dir to an absolute path so the displayed set
    // matches the resolved writable roots the permission engine enforces.
    let extra: Vec<RuntimeString> = config
        .sandbox_workspace_write
        .writable_roots
        .iter()
        .map(resolve_runtime_root)
        .collect();
    let model = opts.model.unwrap_or_else(|| config.model.clone());
    // A host serving a managed model line names the product tier here so the
    // session never recites an upstream vendor id; BYO sources leave the field
    // unset and the environment block names the configured model, as before.
    let display = config
        .model_display_name
        .as_ref()
        .filter(|name| !name.is_empty())
        .unwrap_or(&model);

    let mut lines: Vec<RuntimeString> = vec![
        "# Environment".into(),
        "You have been invoked in the following environment: ".into(),
        RuntimeString::from(" - Primary working directory: ").concat(cwd.as_runtime_string()),
    ];
    if opts.is_worktree {
        lines.push(" - This directory is an isolated git worktree (a separate checkout of the same repository). Run all commands here; do not cd to the original repository root.".into());
    }
    lines.push(RuntimeString::from(format!(
        " - Is a git repository: {}",
        is_repo(cwd)
    )));
    if !extra.is_empty() {
        lines.push(" - Additional working directories:".into());
        for d in &extra {
            lines.push(RuntimeString::from("  - ").concat(d));
        }
    }
    lines.push(RuntimeString::from(format!(
        " - Platform: {}",
        plat.as_str()
    )));
    // On Windows the reported shell must be the one the command runtime will
    // actually execute, so resolve it the same way instead of reading `$SHELL`.
    let shell_line_text = if plat == Platform::Win32 {
        let resolution = opts
            .windows_shell
            .clone()
            .unwrap_or_else(|| resolve_windows_shell(&process_env()));
        windows_shell_line(&resolution)
    } else {
        shell_line(plat, &shell)
    };
    lines.push(RuntimeString::from(format!(" - {shell_line_text}")));
    lines.push(RuntimeString::from(format!(
        " - OS Version: {}",
        os_version_line(plat)
    )));
    lines.extend(extension_directory_lines(cwd));
    // The model line stays LAST: the provider-behavior trace contract canonicalizes
    // the model name by matching this sentence as the env part's suffix, so a fact
    // appended after it would hide the model from that check.
    lines.push(
        RuntimeString::from(" - You are powered by the model ")
            .concat(display)
            .concat(&RuntimeString::from(".")),
    );

    runtime_join(lines, "\n")
}

/// Where this agent's own extensions are configured. Without these the model has
/// no idea where its skills or MCP servers come from, so the moment it cannot
/// find a capability it starts guessing at asset directories on disk — typically
/// another agent tool's (`~/.claude`, `~/.codex`), which are not its own. Paths
/// come from the loaders that actually read them, so this cannot describe a
/// layout the engine does not use.
fn extension_directory_lines(cwd: &RuntimePath) -> Vec<RuntimeString> {
    let [skills_project, skills_user] = skill_search_roots_exact(cwd);
    let mcp_project = project_mcp_config_path_exact(cwd);
    let mcp_user = user_mcp_config_path_exact();
    vec![
        " - Your own extensions are configured at the paths below. A path that does not exist yet simply means nothing is configured at that scope; it is not a fault to investigate, and another agent tool's config directory is not yours to read from.".into(),
        RuntimeString::from("  - Skills: ")
            .concat(skills_project.as_runtime_string())
            .concat(&RuntimeString::from(" (project), "))
            .concat(skills_user.as_runtime_string())
            .concat(&RuntimeString::from(" (user)")),
        RuntimeString::from("  - MCP servers: ")
            .concat(mcp_project.as_runtime_string())
            .concat(&RuntimeString::from(" (project), "))
            .concat(mcp_user.as_runtime_string())
            .concat(&RuntimeString::from(" (user)")),
    ]
}

fn runtime_join(values: impl IntoIterator<Item = RuntimeString>, separator: &str) -> RuntimeString {
    let separator = RuntimeString::from(separator);
    let mut values = values.into_iter();
    let Some(mut joined) = values.next() else {
        return RuntimeString::default();
    };
    for value in values {
        joined = joined.concat(&separator).concat(&value);
    }
    joined
}

fn resolve_runtime_root(root: &RuntimeString) -> RuntimeString {
    let process_cwd = std::env::current_dir()
        .map(|path| runtime_path_from_host(&path))
        .unwrap_or_else(|_| RuntimePath::from(host_root()));
    let mut snapshot = RuntimePathCwdSnapshot::new(host_path_platform(), Ok(process_cwd));
    #[cfg(windows)]
    populate_windows_drive_cwds(&mut snapshot);
    path_resolve(
        host_path_dialect(),
        &[RuntimePath::from(root.clone())],
        &mut snapshot,
    )
    .expect("captured process cwd resolves an exact additional working directory")
    .into_runtime_string()
}

#[cfg(unix)]
fn runtime_path_from_host(path: &std::path::Path) -> RuntimePath {
    use std::os::unix::ffi::OsStrExt;
    RuntimePath::from(NodeBufferUtf8::decode(path.as_os_str().as_bytes()))
}

#[cfg(windows)]
fn runtime_path_from_host(path: &std::path::Path) -> RuntimePath {
    use std::os::windows::ffi::OsStrExt;
    RuntimePath::from(RuntimeString::from_utf16(
        path.as_os_str().encode_wide().collect::<Vec<_>>(),
    ))
}

#[cfg(windows)]
use crate::fs::node_path::populate_windows_drive_cwds;

#[cfg(windows)]
const fn host_path_dialect() -> NodePathDialect {
    NodePathDialect::Win32
}

#[cfg(not(windows))]
const fn host_path_dialect() -> NodePathDialect {
    NodePathDialect::Posix
}

#[cfg(windows)]
const fn host_path_platform() -> NodePathHostPlatform {
    NodePathHostPlatform::Windows
}

#[cfg(not(windows))]
const fn host_path_platform() -> NodePathHostPlatform {
    NodePathHostPlatform::Posix
}

#[cfg(windows)]
const fn host_root() -> &'static str {
    r"\"
}

#[cfg(not(windows))]
const fn host_root() -> &'static str {
    "/"
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::config::EngineConfig;
    use serde_json::json;

    fn config(over: serde_json::Value) -> EngineConfig {
        serde_json::from_value(over).unwrap()
    }

    /// An additional root as the block will render it.
    ///
    /// Resolution goes through the host's path dialect, so `/a` is `/a` on unix and
    /// `C:\a` on Windows. Asserting the literal input made these tests unix-only:
    /// they compared a POSIX spelling against a correct host one.
    fn rendered_root(root: &str) -> String {
        resolve_runtime_root(&RuntimeString::from(root))
            .try_to_utf8()
            .expect("test roots are scalar")
    }

    /// A path as the block spells it, for the same reason.
    fn rendered(path: &RuntimePath) -> String {
        path.as_runtime_string()
            .try_to_utf8()
            .expect("test paths are scalar")
    }

    #[test]
    fn platform_name_normalizes() {
        assert_eq!(platform_name("win32"), Platform::Win32);
        assert_eq!(platform_name("darwin"), Platform::Darwin);
        assert_eq!(platform_name("linux"), Platform::Linux);
        assert_eq!(platform_name("freebsd"), Platform::Linux);
        assert_eq!(platform_name("sunos"), Platform::Linux);
    }

    #[test]
    fn os_version_line_posix_uses_uname() {
        let (sysname, release) = uname_sr();
        assert_eq!(
            os_version_line(Platform::Darwin),
            format!("{sysname} {release}")
        );
        assert_eq!(
            os_version_line(Platform::Linux),
            format!("{sysname} {release}")
        );
    }

    #[test]
    fn shell_line_collapses() {
        assert_eq!(shell_line(Platform::Darwin, "/bin/zsh"), "Shell: zsh");
        assert_eq!(shell_line(Platform::Linux, "/usr/bin/bash"), "Shell: bash");
        assert_eq!(
            shell_line(Platform::Darwin, "/usr/local/bin/fish"),
            "Shell: /usr/local/bin/fish"
        );
        assert_eq!(shell_line(Platform::Darwin, "unknown"), "Shell: unknown");
        // The dialect note moved to `windows_shell_line`: this function no longer
        // claims a syntax the resolved shell may not actually speak.
        let win = shell_line(Platform::Win32, "C:\\\\cmd.exe");
        assert!(win.contains("Shell: C:\\\\cmd.exe"));
        assert!(!win.contains("Unix shell syntax"));
    }

    #[test]
    fn windows_shell_line_bash_keeps_posix_guidance() {
        let line = windows_shell_line(&ShellResolution {
            path: "C:\\Program Files\\Git\\bin\\bash.exe".to_string(),
            kind: ShellKind::Bash,
        });
        assert!(line.contains("Shell: bash"));
        assert!(line.contains("Write POSIX shell syntax"));
        // A real POSIX shell has coreutils, so it must not be told to avoid them.
        assert!(!line.contains("are unavailable"));
        assert!(!line.contains("NUL"));
    }

    #[test]
    fn windows_shell_line_degraded_kinds_warn_and_redirect() {
        let pwsh = windows_shell_line(&ShellResolution {
            path: "C:\\Program Files\\PowerShell\\7\\pwsh.exe".to_string(),
            kind: ShellKind::Pwsh,
        });
        assert!(pwsh.contains("PowerShell 7 (pwsh)"));
        assert!(pwsh.contains("$null"));
        assert!(pwsh.contains("rg --files"));
        assert!(pwsh.contains("glob, grep, file_search, read and ls"));
        assert!(!pwsh.contains("Write POSIX shell syntax"));

        let ps = windows_shell_line(&ShellResolution {
            path: "C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\powershell.exe".to_string(),
            kind: ShellKind::Powershell,
        });
        assert!(ps.contains("Windows PowerShell"));
        assert!(ps.contains("$null"));
        assert!(!ps.contains("Write POSIX shell syntax"));

        let cmd = windows_shell_line(&ShellResolution {
            path: "cmd.exe".to_string(),
            kind: ShellKind::Cmd,
        });
        // cmd's `find` is a text searcher: the trap that returns plausible
        // wrong results instead of failing, so it is called out by name.
        assert!(cmd.contains("TEXT searcher"));
        assert!(cmd.contains("never use it to locate files"));
        assert!(cmd.contains("NUL"));
        assert!(cmd.contains("rg --files"));
        assert!(!cmd.contains("Write POSIX shell syntax"));
        // cmd has no PowerShell either, so it must not be pointed at `$null`.
        assert!(!cmd.contains("$null"));
    }

    #[test]
    fn environment_block_renders_all_facts() {
        let cfg = config(json!({ "model": "gpt-5" }));
        let text = environment_block(
            &RuntimePath::from("/work/proj"),
            &cfg,
            EnvBlockOptions {
                plat: Some(Platform::Darwin),
                shell: Some("/bin/zsh".into()),
                ..Default::default()
            },
        );
        assert!(text.contains("# Environment"));
        assert!(text.contains("You have been invoked in the following environment:"));
        assert!(text.contains(" - Primary working directory: /work/proj"));
        assert!(text.contains(" - Is a git repository:"));
        assert!(text.contains(" - Platform: darwin"));
        assert!(text.contains(" - Shell: zsh"));
        let (sysname, release) = uname_sr();
        assert!(text.contains(&format!(" - OS Version: {sysname} {release}")));
        assert!(text.contains(" - You are powered by the model gpt-5."));
    }

    #[test]
    fn model_display_name_replaces_the_upstream_id_in_the_env_block() {
        let cfg = config(json!({ "model": "kimi-k3", "modelDisplayName": "Klaus Standard" }));
        let text = environment_block(
            &RuntimePath::from("/work/proj"),
            &cfg,
            EnvBlockOptions {
                plat: Some(Platform::Darwin),
                shell: Some("/bin/zsh".into()),
                ..Default::default()
            },
        );
        assert!(text.contains(" - You are powered by the model Klaus Standard."));
        assert!(!text.contains("kimi-k3"));
    }

    #[test]
    fn an_empty_display_name_falls_back_to_the_configured_model() {
        let cfg = config(json!({ "model": "gpt-5", "modelDisplayName": "" }));
        let text = environment_block(
            &RuntimePath::from("/work/proj"),
            &cfg,
            EnvBlockOptions {
                plat: Some(Platform::Darwin),
                shell: Some("/bin/zsh".into()),
                ..Default::default()
            },
        );
        assert!(text.contains(" - You are powered by the model gpt-5."));
    }

    #[test]
    fn additional_dirs_only_when_present_and_absolute() {
        let with = environment_block(
            &RuntimePath::from("/w"),
            &config(json!({
                "sandbox_workspace_write": {
                    "writable_roots": [rendered_root("/a"), rendered_root("/b")]
                }
            })),
            EnvBlockOptions {
                plat: Some(Platform::Darwin),
                shell: Some("/bin/zsh".into()),
                ..Default::default()
            },
        );
        assert!(with.contains(" - Additional working directories:"));
        assert!(with.contains(&format!("  - {}", rendered_root("/a"))));
        assert!(with.contains(&format!("  - {}", rendered_root("/b"))));

        let without = environment_block(
            &RuntimePath::from("/w"),
            &EngineConfig::default(),
            EnvBlockOptions {
                plat: Some(Platform::Darwin),
                shell: Some("/bin/zsh".into()),
                ..Default::default()
            },
        );
        assert!(!without.contains("Additional working directories"));

        assert!(serde_json::from_value::<EngineConfig>(json!({
            "sandbox_workspace_write": { "writable_roots": ["rel/dir"] }
        }))
        .is_err());
        let mut relative_config = EngineConfig::default();
        relative_config.sandbox_workspace_write.writable_roots = vec!["rel/dir".into()];
        relative_config
            .sandbox_workspace_write
            .normalize_writable_roots(Some(&std::env::current_dir().unwrap()))
            .unwrap();
        let rel = environment_block(
            &RuntimePath::from("/w"),
            &relative_config,
            EnvBlockOptions {
                plat: Some(Platform::Darwin),
                shell: Some("/bin/zsh".into()),
                ..Default::default()
            },
        );
        assert!(rel.contains(&format!("  - {}", rendered_root("rel/dir"))));
        assert!(!rel.contains("  - rel/dir\n"));
    }

    #[test]
    fn lists_the_agents_own_extension_directories() {
        let cwd = RuntimePath::from("/work/proj");
        let text = environment_block(
            &cwd,
            &EngineConfig::default(),
            EnvBlockOptions {
                plat: Some(Platform::Darwin),
                shell: Some("/bin/zsh".into()),
                ..Default::default()
            },
        );
        assert!(text.contains(" - Your own extensions are configured at the paths below."));
        assert!(text.contains("another agent tool's config directory is not yours"));
        // Taken from the loaders that produce them rather than spelled out, so the
        // separator is the host's. A literal `/work/proj/.klaus/skills` here is what
        // made this test unix-only.
        let [skills_project, _] = skill_search_roots_exact(&cwd);
        let mcp_project = project_mcp_config_path_exact(&cwd);
        assert!(text.contains(&format!(
            "  - Skills: {} (project), ",
            rendered(&skills_project)
        )));
        assert!(text.contains(&format!(
            "  - MCP servers: {} (project), ",
            rendered(&mcp_project)
        )));
        // The user-scope halves come from the loaders themselves, so they follow
        // whatever config home is in effect rather than a literal repeated here.
        assert!(text.contains("skills (user)"));
        assert!(text.contains("mcp.json (user)"));
    }

    #[test]
    fn model_override_wins() {
        let text = environment_block(
            &RuntimePath::from("/w"),
            &config(json!({ "model": "claude-opus-4-8" })),
            EnvBlockOptions {
                plat: Some(Platform::Darwin),
                shell: Some("/bin/zsh".into()),
                model: Some("gpt-5".into()),
                ..Default::default()
            },
        );
        assert!(text.contains(" - You are powered by the model gpt-5."));
        assert!(!text.contains("claude-opus-4-8"));
    }

    #[test]
    fn worktree_notice_placement() {
        let wt = environment_block(
            &RuntimePath::from("/w/wt"),
            &EngineConfig::default(),
            EnvBlockOptions {
                plat: Some(Platform::Darwin),
                shell: Some("/bin/zsh".into()),
                is_worktree: true,
                ..Default::default()
            },
        );
        let wt = wt.try_to_utf8().unwrap();
        let lines: Vec<&str> = wt.split('\n').collect();
        let cwd_idx = lines
            .iter()
            .position(|l| l.contains("Primary working directory:"))
            .unwrap();
        assert!(lines[cwd_idx + 1].contains("isolated git worktree"));
        assert!(lines[cwd_idx + 1].contains("do not cd to the original repository root"));

        let plain = environment_block(
            &RuntimePath::from("/w"),
            &EngineConfig::default(),
            EnvBlockOptions {
                plat: Some(Platform::Darwin),
                shell: Some("/bin/zsh".into()),
                ..Default::default()
            },
        );
        assert!(!plain.contains("isolated git worktree"));
    }
}
