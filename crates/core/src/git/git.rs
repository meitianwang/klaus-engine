//! Engine-internal git helpers. The engine queries repo state itself (for the
//! model-facing git context block) — these run `git` directly via argv (no
//! shell, no permission/sandbox layer).
//!
//! Hardened: GIT_OPTIONAL_LOCKS=0, core.hooksPath off, core.fsmonitor
//! off, a short timeout, and a non-interactive terminal. Everything degrades to
//! `None` when git is absent, the dir isn't a repo, or a command fails.

use std::path::Path;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::fs::node_path::{project_runtime_path, runtime_path_from_native};
use crate::runtime_process::{
    ParentEnvSnapshot, RuntimeEnv, RuntimeProcessContext, RuntimeProcessSurface, RuntimeSignal,
    RuntimeSpawnRequest,
};
use crate::runtime_values::{NodeChunkUtf8, RuntimePath, RuntimeString};
use crate::types::sandbox::SpawnSpec;

#[cfg(not(test))]
const GIT_TIMEOUT_MS: u64 = 5_000;
#[cfg(test)]
const GIT_TIMEOUT_MS: u64 = 30_000;

#[cfg(windows)]
const NULL_DEVICE: &str = "NUL";
#[cfg(not(windows))]
const NULL_DEVICE: &str = "/dev/null";

const GIT_MAX_BUFFER_BYTES: usize = 16 * 1024 * 1024;

/// Top-level `-c` options applied to every read so a repo can't run code on us.
fn hardening() -> Vec<RuntimeString> {
    vec![
        RuntimeString::from("-c"),
        RuntimeString::from(format!("core.hooksPath={NULL_DEVICE}")),
        RuntimeString::from("-c"),
        RuntimeString::from("core.fsmonitor=false"),
    ]
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunResult {
    pub code: i32,
    pub stdout: RuntimeString,
    pub stderr: RuntimeString,
}

/// Run a git command. Returns `None` when git is absent or errored out.
pub async fn run_git(
    process: &RuntimeProcessContext,
    args: &[RuntimeString],
    cwd: &RuntimePath,
    env: Option<&RuntimeEnv>,
) -> Option<RunResult> {
    let mut child_env = ParentEnvSnapshot::capture();
    child_env.insert("GIT_OPTIONAL_LOCKS".into(), "0".into());
    child_env.insert("GIT_TERMINAL_PROMPT".into(), "0".into());
    if let Some(env) = env {
        child_env.overlay(env);
    }
    let mut argv = hardening();
    argv.extend_from_slice(args);
    let request = RuntimeSpawnRequest::from_spawn_spec(
        SpawnSpec {
            file: RuntimeString::from("git"),
            args: argv,
            cwd: cwd.clone(),
            env: child_env,
        },
        RuntimeProcessSurface::GitExecFile,
    );
    let mut child = process.spawner().spawn(request).await.ok()?;
    drop(child.take_stdin());
    let stdout = child.take_stdout()?;
    let stderr = child.take_stderr()?;
    let controller = child.controller();
    let collect = async move {
        let (stdout, stderr, status) =
            tokio::join!(read_bounded(stdout), read_bounded(stderr), child.wait(),);
        let (stdout, stdout_overflow) = stdout.ok()?;
        let (stderr, stderr_overflow) = stderr.ok()?;
        if stdout_overflow || stderr_overflow {
            return None;
        }
        let status = status.ok()?;
        Some(RunResult {
            code: status.code().unwrap_or(1),
            stdout: NodeChunkUtf8::decode_chunk(&stdout),
            stderr: NodeChunkUtf8::decode_chunk(&stderr),
        })
    };
    match tokio::time::timeout(Duration::from_millis(GIT_TIMEOUT_MS), collect).await {
        Ok(result) => result,
        Err(_) => {
            controller.signal(RuntimeSignal::Kill);
            None
        }
    }
}

async fn read_bounded<R: AsyncRead + Unpin>(mut reader: R) -> std::io::Result<(Vec<u8>, bool)> {
    let mut retained = Vec::new();
    let mut overflow = false;
    // Keep the read buffer off the async state machine's stack. `run_git`
    // joins two of these readers under the full query/session poll chain; an
    // inline 64 KiB array makes Tokio's `join!` projection temporarily move
    // large futures and can exhaust the ordinary 2 MiB worker/test stack.
    let mut chunk = vec![0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        let remaining = GIT_MAX_BUFFER_BYTES.saturating_sub(retained.len());
        let keep = remaining.min(read);
        retained.extend_from_slice(&chunk[..keep]);
        overflow |= keep != read;
    }
    Ok((retained, overflow))
}

/// Stdout of a successful (exit 0) git command, else `None`.
async fn git_out(
    process: &RuntimeProcessContext,
    args: &[RuntimeString],
    cwd: &RuntimePath,
) -> Option<RuntimeString> {
    let r = run_git(process, args, cwd, None).await?;
    if r.code == 0 {
        Some(r.stdout)
    } else {
        None
    }
}

/// Whether `path` is a `.git` entry git itself would accept.
///
/// A bare `exists()` is not the same question. An empty directory named `.git` satisfies
/// it while `git rev-parse` refuses to run there, so the engine would announce "is a git
/// repository: yes", key project memory to that root, and then watch every git command
/// fail. Git's own rule is what is checked instead: a `.git` FILE is a worktree or
/// submodule pointer, and a `.git` DIRECTORY is a repository only if it holds `HEAD`.
pub fn is_git_entry(path: &Path) -> bool {
    match std::fs::metadata(path) {
        Ok(meta) if meta.is_file() => true,
        Ok(meta) if meta.is_dir() => path.join("HEAD").exists(),
        _ => false,
    }
}

/// Create a `.git` directory that [`is_git_entry`] accepts, for the tests and trace
/// fixtures that need a repository marker without running git. The `HEAD` file is what
/// makes the directory a repository rather than an empty lookalike.
#[cfg(any(test, feature = "testing"))]
pub fn seed_git_dir(git_dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(git_dir)?;
    std::fs::write(git_dir.join("HEAD"), b"ref: refs/heads/main\n")
}

/// The repository root: the nearest ancestor whose `.git` entry is a real one.
/// Filesystem-only — no git binary needed.
pub fn repo_root(cwd: &RuntimePath) -> Option<RuntimePath> {
    let mut dir = project_runtime_path(cwd, "git repository cwd").ok()?;
    loop {
        if is_git_entry(&dir.join(".git")) {
            return Some(runtime_path_from_native(&dir));
        }
        match dir.parent() {
            None => return None,
            Some(p) => {
                let p = p.to_path_buf();
                if p == dir {
                    return None;
                }
                dir = p;
            }
        }
    }
}

/// True when `cwd` is inside a git repository (filesystem check only).
pub fn is_repo(cwd: &RuntimePath) -> bool {
    repo_root(cwd).is_some()
}

/// Current branch name, or `None` when detached / not a repo.
pub async fn current_branch(
    process: &RuntimeProcessContext,
    cwd: &RuntimePath,
) -> Option<RuntimeString> {
    let out = git_out(process, &runtime_args(&["branch", "--show-current"]), cwd).await?;
    let value = trim_runtime(&out);
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

/// Full HEAD commit SHA, validated, or `None`.
pub async fn head_sha(process: &RuntimeProcessContext, cwd: &RuntimePath) -> Option<RuntimeString> {
    let out = git_out(process, &runtime_args(&["rev-parse", "HEAD"]), cwd).await?;
    let value = trim_runtime(&out);
    if is_sha(&value) {
        Some(value)
    } else {
        None
    }
}

/// `git status --short` text (porcelain-ish), or `None`.
pub async fn short_status(
    process: &RuntimeProcessContext,
    cwd: &RuntimePath,
) -> Option<RuntimeString> {
    let out = git_out(process, &runtime_args(&["status", "--short"]), cwd).await?;
    Some(trim_runtime_end(&out))
}

/// True when the working tree has uncommitted changes.
pub async fn is_dirty(process: &RuntimeProcessContext, cwd: &RuntimePath) -> Option<bool> {
    let out = git_out(process, &runtime_args(&["status", "--porcelain"]), cwd).await?;
    Some(!trim_runtime(&out).is_empty())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitLogEntry {
    pub sha: RuntimeString,
    pub timestamp: i64,
    pub subject: RuntimeString,
}

/// The most recent commits (newest first). Empty vec on any failure.
pub async fn recent_commits(
    process: &RuntimeProcessContext,
    cwd: &RuntimePath,
    limit: u32,
) -> Vec<CommitLogEntry> {
    let limit = RuntimeString::from(limit.to_string());
    let out = match git_out(
        process,
        &[
            "log".into(),
            "-n".into(),
            limit,
            "--pretty=format:%H%x1f%ct%x1f%s".into(),
        ],
        cwd,
    )
    .await
    {
        Some(o) => o,
        None => return Vec::new(),
    };
    let mut entries = Vec::new();
    for line in split_runtime(&out, u16::from(b'\n')) {
        let parts = split_runtime(&line, 0x1f);
        if let (Some(sha), Some(ts)) = (parts.first(), parts.get(1)) {
            if !sha.is_empty() {
                entries.push(CommitLogEntry {
                    sha: sha.clone(),
                    timestamp: runtime_ascii(ts)
                        .and_then(|value| value.parse().ok())
                        .unwrap_or(0),
                    subject: parts.get(2).cloned().unwrap_or_default(),
                });
            }
        }
    }
    entries
}

/// Oneline log (`<short sha> <subject>`), or `None`.
pub async fn recent_commits_oneline(
    process: &RuntimeProcessContext,
    cwd: &RuntimePath,
    limit: u32,
) -> Option<RuntimeString> {
    let out = git_out(
        process,
        &[
            "log".into(),
            "--oneline".into(),
            "-n".into(),
            RuntimeString::from(limit.to_string()),
        ],
        cwd,
    )
    .await?;
    Some(trim_runtime_end(&out))
}

/// The repository's default branch: `origin/HEAD`, else a probe of
/// `origin/main|master`, else a local `main|master`. `None` otherwise.
pub async fn default_branch(
    process: &RuntimeProcessContext,
    cwd: &RuntimePath,
) -> Option<RuntimeString> {
    if let Some(sym) = git_out(
        process,
        &runtime_args(&[
            "symbolic-ref",
            "--quiet",
            "--short",
            "refs/remotes/origin/HEAD",
        ]),
        cwd,
    )
    .await
    {
        let sym = trim_runtime(&sym);
        if !sym.is_empty() {
            return Some(if sym.starts_with("origin/") {
                sym.slice_utf16("origin/".encode_utf16().count()..)
            } else {
                sym
            });
        }
    }
    for reff in ["refs/remotes/origin/main", "refs/remotes/origin/master"] {
        if let Some(r) = run_git(
            process,
            &runtime_args(&["rev-parse", "--verify", "--quiet", reff]),
            cwd,
            None,
        )
        .await
        {
            if r.code == 0 {
                return reff.rsplit('/').next().map(RuntimeString::from);
            }
        }
    }
    for reff in ["refs/heads/main", "refs/heads/master"] {
        if let Some(r) = run_git(
            process,
            &runtime_args(&["rev-parse", "--verify", "--quiet", reff]),
            cwd,
            None,
        )
        .await
        {
            if r.code == 0 {
                return reff.rsplit('/').next().map(RuntimeString::from);
            }
        }
    }
    None
}

/// The `origin` fetch URL, or `None`.
pub async fn remote_url(
    process: &RuntimeProcessContext,
    cwd: &RuntimePath,
) -> Option<RuntimeString> {
    let out = git_out(
        process,
        &runtime_args(&["remote", "get-url", "origin"]),
        cwd,
    )
    .await?;
    let value = trim_runtime(&out);
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

/// The configured git user name, or `None`.
pub async fn user_name(
    process: &RuntimeProcessContext,
    cwd: &RuntimePath,
) -> Option<RuntimeString> {
    let out = git_out(process, &runtime_args(&["config", "user.name"]), cwd).await?;
    let value = trim_runtime(&out);
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

#[derive(Debug, Clone, Default)]
pub struct GitContextOptions {
    /// Truncate the status listing at this many chars (default 2000).
    pub max_status_chars: Option<usize>,
}

/// A compact, model-facing git summary for the system prompt — branch, default
/// branch, status, and recent commits, computed once at session start. `None`
/// when `cwd` is not a repo or git is unusable.
pub async fn git_context(
    process: &RuntimeProcessContext,
    cwd: &RuntimePath,
    opts: GitContextOptions,
) -> Option<RuntimeString> {
    if !is_repo(cwd) {
        return None;
    }
    let max_status = opts.max_status_chars.unwrap_or(2000);

    let branch = current_branch(process, cwd).await;
    let def = default_branch(process, cwd).await;
    let status = short_status(process, cwd).await;
    let log = recent_commits_oneline(process, cwd, 5).await;
    let user = user_name(process, cwd).await;

    // git unusable (every read failed) → omit the block entirely.
    if branch.is_none() && status.is_none() && log.is_none() {
        return None;
    }

    let mut lines: Vec<RuntimeString> = vec![
        "Git repository state (a snapshot taken at the start of this conversation; it does NOT update as you work — re-run `git status` for the live state).".into(),
    ];
    if let Some(branch) = &branch {
        lines.push(RuntimeString::from("Current branch: ").concat(branch));
    }
    if let Some(def) = &def {
        lines.push(RuntimeString::from("Default branch (use as the base for PRs): ").concat(def));
    }
    if let Some(user) = &user {
        lines.push(RuntimeString::from("Git user: ").concat(user));
    }

    let mut status_text = status.clone().unwrap_or_default();
    let mut note = RuntimeString::default();
    if status_text.len_utf16() > max_status {
        status_text = status_text.slice_utf16(..max_status);
        note = "\n… [truncated — run `git status` for the full list]".into();
    }
    let body = if status_text.is_empty() {
        RuntimeString::from("(clean)")
    } else {
        status_text
    };
    lines.push(RuntimeString::from("Status:\n").concat(&body).concat(&note));
    if let Some(log) = &log {
        lines.push(RuntimeString::from("Recent commits:\n").concat(log));
    }

    Some(runtime_join(&lines, "\n\n"))
}

fn runtime_args(values: &[&str]) -> Vec<RuntimeString> {
    values.iter().copied().map(RuntimeString::from).collect()
}

fn trim_runtime(value: &RuntimeString) -> RuntimeString {
    let units = value.as_utf16();
    let mut start = 0;
    let mut end = units.len();
    while start < end && is_ascii_space(units[start]) {
        start += 1;
    }
    while end > start && is_ascii_space(units[end - 1]) {
        end -= 1;
    }
    value.slice_utf16(start..end)
}

fn trim_runtime_end(value: &RuntimeString) -> RuntimeString {
    let units = value.as_utf16();
    let mut end = units.len();
    while end > 0 && is_ascii_space(units[end - 1]) {
        end -= 1;
    }
    value.slice_utf16(..end)
}

fn is_ascii_space(unit: u16) -> bool {
    matches!(unit, 0x09..=0x0d | 0x20)
}

fn split_runtime(value: &RuntimeString, separator: u16) -> Vec<RuntimeString> {
    let units = value.as_utf16();
    let mut output = Vec::new();
    let mut start = 0;
    for (index, unit) in units.iter().copied().enumerate() {
        if unit == separator {
            output.push(value.slice_utf16(start..index));
            start = index + 1;
        }
    }
    output.push(value.slice_utf16(start..));
    output
}

fn runtime_ascii(value: &RuntimeString) -> Option<String> {
    value
        .as_utf16()
        .iter()
        .copied()
        .map(|unit| u8::try_from(unit).ok().map(char::from))
        .collect()
}

fn is_sha(value: &RuntimeString) -> bool {
    (7..=40).contains(&value.len_utf16())
        && value.as_utf16().iter().all(|unit| {
            (u16::from(b'0')..=u16::from(b'9')).contains(unit)
                || (u16::from(b'a')..=u16::from(b'f')).contains(unit)
                || (u16::from(b'A')..=u16::from(b'F')).contains(unit)
        })
}

fn runtime_join(values: &[RuntimeString], separator: &str) -> RuntimeString {
    let separator = RuntimeString::from(separator);
    let mut values = values.iter();
    let Some(first) = values.next() else {
        return RuntimeString::default();
    };
    values.fold(first.clone(), |joined, value| {
        joined.concat(&separator).concat(value)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::process::Command as StdCommand;

    #[test]
    fn bounded_reader_future_does_not_embed_the_io_buffer() {
        let future = read_bounded(tokio::io::empty());
        assert!(
            std::mem::size_of_val(&future) < 4 * 1024,
            "read_bounded future unexpectedly embeds {} bytes",
            std::mem::size_of_val(&future)
        );
    }

    fn git(args: &[&str], cwd: &std::path::Path) {
        StdCommand::new("git")
            .args(args)
            .current_dir(cwd)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .expect("git runs");
    }

    struct TempRepo {
        _external: tokio::sync::MutexGuard<'static, ()>,
        dir: PathBuf,
        plain: PathBuf,
    }

    impl TempRepo {
        async fn new(tag: &str) -> Self {
            let external = crate::test_support::external_process_lock().await;
            let base = std::env::temp_dir();
            let uniq = format!(
                "{tag}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            );
            let dir = base.join(format!("ae-git-{uniq}"));
            let plain = base.join(format!("ae-plain-{uniq}"));
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::create_dir_all(&plain).unwrap();
            git(&["init", "-q"], &dir);
            git(&["checkout", "-q", "-b", "work"], &dir);
            git(&["config", "user.email", "a@b.c"], &dir);
            git(&["config", "user.name", "Test User"], &dir);
            std::fs::write(dir.join("a.txt"), "hello\n").unwrap();
            git(&["add", "."], &dir);
            git(&["commit", "-q", "-m", "first commit"], &dir);
            TempRepo {
                _external: external,
                dir,
                plain,
            }
        }
        fn dir(&self) -> String {
            self.dir.to_string_lossy().into_owned()
        }
        fn runtime_dir(&self) -> RuntimePath {
            runtime_path_from_native(&self.dir)
        }
        fn runtime_plain(&self) -> RuntimePath {
            runtime_path_from_native(&self.plain)
        }
    }

    impl Drop for TempRepo {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
            let _ = std::fs::remove_dir_all(&self.plain);
        }
    }

    #[tokio::test]
    async fn detects_repo_and_root() {
        let r = TempRepo::new("root").await;
        assert!(is_repo(&r.runtime_dir()));
        assert_eq!(repo_root(&r.runtime_dir()), Some(r.runtime_dir()));
        assert!(!is_repo(&r.runtime_plain()));
        assert_eq!(repo_root(&r.runtime_plain()), None);
    }

    #[tokio::test]
    async fn reads_branch_and_commits() {
        let process = crate::runtime_process::test_runtime_process_context();
        let r = TempRepo::new("branch").await;
        assert_eq!(
            current_branch(&process, &r.runtime_dir()).await,
            Some(RuntimeString::from("work"))
        );
        let commits = recent_commits(&process, &r.runtime_dir(), 5).await;
        assert!(!commits.is_empty());
        assert_eq!(commits[0].subject, "first commit");
        assert!(is_sha(&commits[0].sha) && commits[0].sha.len_utf16() == 40);
        assert_eq!(
            git_context(&process, &r.runtime_plain(), GitContextOptions::default()).await,
            None
        );
    }

    #[tokio::test]
    async fn short_status_reflects_changes() {
        let process = crate::runtime_process::test_runtime_process_context();
        let r = TempRepo::new("status").await;
        assert_eq!(
            short_status(&process, &r.runtime_dir())
                .await
                .map(|status| trim_runtime(&status)),
            Some(RuntimeString::default())
        );
        std::fs::write(std::path::Path::new(&r.dir()).join("a.txt"), "changed\n").unwrap();
        std::fs::write(std::path::Path::new(&r.dir()).join("b.txt"), "new\n").unwrap();
        let status = short_status(&process, &r.runtime_dir()).await.unwrap();
        assert!(status.contains("a.txt"));
        assert!(status.contains("b.txt"));
    }

    #[tokio::test]
    async fn default_branch_local_fallback() {
        let process = crate::runtime_process::test_runtime_process_context();
        let r = TempRepo::new("default").await;
        assert_eq!(default_branch(&process, &r.runtime_dir()).await, None);
        git(&["branch", "main"], std::path::Path::new(&r.dir()));
        assert_eq!(
            default_branch(&process, &r.runtime_dir()).await,
            Some(RuntimeString::from("main"))
        );
    }

    #[tokio::test]
    async fn git_context_summary() {
        let process = crate::runtime_process::test_runtime_process_context();
        let r = TempRepo::new("ctx").await;
        std::fs::write(std::path::Path::new(&r.dir()).join("a.txt"), "changed\n").unwrap();
        let ctx = git_context(&process, &r.runtime_dir(), GitContextOptions::default())
            .await
            .unwrap();
        assert!(ctx.contains("Current branch: work"));
        assert!(ctx.contains("Git user: Test User"));
        assert!(ctx.contains("Status:"));
        assert!(ctx.contains("a.txt"));
        assert!(ctx.contains("Recent commits:"));
        assert!(ctx.contains("first commit"));
        assert!(ctx.contains("snapshot"));
    }

    #[tokio::test]
    async fn git_context_clean_tree() {
        let process = crate::runtime_process::test_runtime_process_context();
        let r = TempRepo::new("clean").await;
        let ctx = git_context(&process, &r.runtime_dir(), GitContextOptions::default())
            .await
            .unwrap();
        assert!(ctx.contains("(clean)"));
    }
}
