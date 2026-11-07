//! Git worktree lifecycle — the engine-relevant core: slug validation,
//! branch/path derivation, create-or-resume, fail-closed change counting,
//! removal, and canonical-root resolution. The on-disk layout
//! (`.klaus/worktrees/<slug>` directories and `worktree-<slug>` branches) is
//! chosen for interop, so a worktree created by one tool can be resumed by
//! another. Interactive/product machinery (terminal-multiplexer sessions,
//! repo-hook copying, PR-reference resume, sparse paths, include-file copying,
//! UI) is out of scope — that is host/REPL layer, not engine behavior, and a
//! host adds it via hooks. The process-wide working directory is never
//! switched: the cwd switch is a per-Session mutation, so it never leaks to
//! the host process or sibling sessions.

use crate::fs::node_path::{
    dirname_runtime, host_path_dialect, project_runtime_path, resolve_runtime,
    runtime_path_from_native,
};
use crate::git::git::run_git;
use crate::runtime_process::{path_join, RuntimeProcessContext};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeDecode, RuntimeEncode, RuntimeEncoder, RuntimeJson,
    RuntimeObjectEncoder, RuntimePath, RuntimeString,
};
use regex::Regex;
use std::sync::LazyLock;

// A conservative safe-filename charset for each path segment, plus a length cap.
static VALID_SLUG_SEGMENT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-zA-Z0-9._-]+$").unwrap());
const MAX_SLUG_LENGTH: usize = 64;

/// A worktree this Session entered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorktreeSession {
    /// The cwd before entering (restored on exit).
    pub original_cwd: RuntimePath,
    pub worktree_path: RuntimePath,
    pub worktree_name: RuntimeString,
    pub worktree_branch: Option<RuntimeString>,
    pub original_branch: Option<RuntimeString>,
    /// HEAD at creation — the baseline for counting new commits (fail-closed if absent).
    pub original_head_commit: Option<RuntimeString>,
    /// True when created via a WorktreeCreate hook (non-git VCS) rather than git.
    pub hook_based: Option<bool>,
}

impl RuntimeEncode for WorktreeSession {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("originalCwd", &self.original_cwd)?;
        object.field("worktreePath", &self.worktree_path)?;
        object.field("worktreeName", &self.worktree_name)?;
        if let Some(value) = &self.worktree_branch {
            object.field("worktreeBranch", value)?;
        }
        if let Some(value) = &self.original_branch {
            object.field("originalBranch", value)?;
        }
        if let Some(value) = &self.original_head_commit {
            object.field("originalHeadCommit", value)?;
        }
        if let Some(value) = self.hook_based {
            object.field("hookBased", &value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for WorktreeSession {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                original_cwd: object.required("originalCwd", &[])?,
                worktree_path: object.required("worktreePath", &[])?,
                worktree_name: object.required("worktreeName", &[])?,
                worktree_branch: object.optional("worktreeBranch", &[])?,
                original_branch: object.optional("originalBranch", &[])?,
                original_head_commit: object.optional("originalHeadCommit", &[])?,
                hook_based: object.optional("hookBased", &[])?,
            })
        })
    }
}

/// Reject slugs that could escape `.klaus/worktrees/<slug>` via path joining or
/// that contain unexpected characters — the first line of defense against path
/// traversal in a caller-supplied worktree name.
pub fn validate_worktree_slug(slug: &str) -> Result<(), String> {
    if slug.encode_utf16().count() > MAX_SLUG_LENGTH {
        return Err(format!(
            "Invalid worktree name: must be {MAX_SLUG_LENGTH} characters or fewer"
        ));
    }
    for segment in slug.split('/') {
        if segment == "." || segment == ".." {
            return Err(format!(
                "Invalid worktree name \"{slug}\": must not contain \".\" or \"..\" segments"
            ));
        }
        if !VALID_SLUG_SEGMENT.is_match(segment) {
            return Err(format!(
                "Invalid worktree name \"{slug}\": each \"/\"-separated segment must be non-empty and contain only letters, digits, dots, underscores, and dashes"
            ));
        }
    }
    Ok(())
}

/// Flatten nested slugs (`user/feature` → `user+feature`) for the branch + dir.
/// `+` is valid in git refs and paths but excluded from the slug allowlist, so
/// the mapping is injective.
pub fn flatten_slug(slug: &str) -> String {
    slug.replace('/', "+")
}

pub fn worktree_branch_name(slug: &str) -> String {
    format!("worktree-{}", flatten_slug(slug))
}

pub fn worktrees_dir(repo_root: &RuntimePath) -> RuntimePath {
    path_join(
        host_path_dialect(),
        &[
            repo_root.clone(),
            RuntimePath::from(".klaus"),
            RuntimePath::from("worktrees"),
        ],
    )
}

pub fn worktree_path_for(repo_root: &RuntimePath, slug: &str) -> RuntimePath {
    path_join(
        host_path_dialect(),
        &[
            worktrees_dir(repo_root),
            RuntimePath::from(flatten_slug(slug)),
        ],
    )
}

/// Lexically resolve a path: relative inputs are anchored at the process cwd,
/// and `.`/`..` segments are collapsed without touching the filesystem.
fn resolve_path(path: &RuntimePath) -> Option<RuntimePath> {
    let process_cwd = std::env::current_dir().ok()?;
    resolve_runtime(
        &runtime_path_from_native(&process_cwd),
        std::slice::from_ref(path),
    )
    .ok()
}

/// The parent directory of `p`, or `p` itself when it has no parent (e.g. a
/// filesystem root).
fn dirname(path: &RuntimePath) -> RuntimePath {
    let parent = dirname_runtime(path);
    if parent.as_runtime_string().is_empty() {
        path.clone()
    } else {
        parent
    }
}

/// The MAIN repository root, resolved even from inside a linked worktree, so a
/// worktree can be created from within another. Uses the common git dir (shared
/// across all worktrees of a repo); its parent is the main worktree root.
pub async fn find_canonical_git_root(
    process: &RuntimeProcessContext,
    cwd: &RuntimePath,
) -> Option<RuntimePath> {
    let r = run_git(
        process,
        &runtime_args(&["rev-parse", "--path-format=absolute", "--git-common-dir"]),
        cwd,
        None,
    )
    .await?;
    if r.code != 0 {
        return None;
    }
    let common_dir = trim_runtime(&r.stdout);
    if common_dir.is_empty() {
        return None;
    }
    // `<root>/.git` → `<root>`; a bare/worktree common dir → its parent.
    Some(dirname(&resolve_path(&RuntimePath::from(common_dir))?))
}

/// Both git roots for `cwd` — see [`find_git_roots`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitRoots {
    pub toplevel: RuntimePath,
    pub canonical: RuntimePath,
}

impl RuntimeEncode for GitRoots {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("toplevel", &self.toplevel)?;
        object.field("canonical", &self.canonical)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for GitRoots {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                toplevel: object.required("toplevel", &[])?,
                canonical: object.required("canonical", &[])?,
            })
        })
    }
}

/// Both git roots for `cwd` in ONE `git rev-parse` (cheaper than two calls): `toplevel` is the
/// working tree `cwd` is in — for a LINKED worktree, the worktree's own root — and `canonical` is the
/// MAIN repo root (shared across all worktrees). When they differ and the toplevel sits inside the
/// canonical root, `cwd` is a nested worktree; a memory walk uses that to skip the main repo's
/// checked-in files (the worktree has its own copy). `None` outside a repo.
pub async fn find_git_roots(
    process: &RuntimeProcessContext,
    cwd: &RuntimePath,
) -> Option<GitRoots> {
    let r = run_git(
        process,
        &runtime_args(&[
            "rev-parse",
            "--path-format=absolute",
            "--show-toplevel",
            "--git-common-dir",
        ]),
        cwd,
        None,
    )
    .await?;
    if r.code != 0 {
        return None;
    }
    let lines = split_runtime(&r.stdout, u16::from(b'\n'));
    let toplevel = lines.first().map(trim_runtime).unwrap_or_default();
    let common_dir = lines.get(1).map(trim_runtime).unwrap_or_default();
    if toplevel.is_empty() || common_dir.is_empty() {
        return None;
    }
    // `<root>/.git` → `<root>`; but a BARE repo's common dir (e.g. `/repo.git`, basename ≠ `.git`) IS
    // the canonical identity — using its parent would be one level too high.
    let cd = resolve_path(&RuntimePath::from(common_dir))?;
    let canonical = if crate::fs::node_path::basename_runtime(&cd).as_runtime_string()
        == &RuntimeString::from(".git")
    {
        dirname(&cd)
    } else {
        cd
    };
    Some(GitRoots {
        toplevel: resolve_path(&RuntimePath::from(toplevel))?,
        canonical,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatedWorktree {
    pub worktree_path: RuntimePath,
    pub worktree_branch: RuntimeString,
    pub head_commit: Option<RuntimeString>,
    pub existed: bool,
}

impl RuntimeEncode for CreatedWorktree {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("worktreePath", &self.worktree_path)?;
        object.field("worktreeBranch", &self.worktree_branch)?;
        if let Some(value) = &self.head_commit {
            object.field("headCommit", value)?;
        }
        object.field("existed", &self.existed)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for CreatedWorktree {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                worktree_path: object.required("worktreePath", &[])?,
                worktree_branch: object.required("worktreeBranch", &[])?,
                head_commit: object.optional("headCommit", &[])?,
                existed: object.required("existed", &[])?,
            })
        })
    }
}

/// Fold a string so two spellings of one host path compare equal.
///
/// `git worktree list --porcelain` prints `C:/Users/…` on Windows while the engine builds
/// `C:\Users\…`, so the literal match this feeds never fired: the engine asked git to add a
/// worktree it already had and git refused with "already used by worktree at …", leaving
/// the feature unusable on that platform. The separator is folded on every host, and case
/// only where the filesystem itself ignores it.
fn fold_host_path(value: &RuntimeString) -> RuntimeString {
    const BACKSLASH: u16 = b'\\' as u16;
    const SLASH: u16 = b'/' as u16;
    let units = value
        .as_utf16()
        .iter()
        .map(|unit| match *unit {
            BACKSLASH => SLASH,
            other if cfg!(windows) && (0x41..=0x5a).contains(&other) => other + 0x20,
            other => other,
        })
        .collect::<Vec<_>>();
    RuntimeString::from_utf16(units)
}

/// Create a git worktree for `slug`, or resume it if the directory already exists.
/// Uses `git worktree add -B` (not `-b`) so it also resets any orphan branch left
/// behind by a previously-removed worktree dir, instead of failing on conflict.
/// `base_ref` defaults to `HEAD`.
pub async fn get_or_create_worktree(
    process: &RuntimeProcessContext,
    repo_root: &RuntimePath,
    slug: &str,
    base_ref: Option<&str>,
) -> Result<CreatedWorktree, String> {
    let base_ref = base_ref.unwrap_or("HEAD");
    validate_worktree_slug(slug)?;
    let worktree_path = worktree_path_for(repo_root, slug);
    let worktree_branch = RuntimeString::from(worktree_branch_name(slug));

    // Already registered? Resume it.
    let list = run_git(
        process,
        &runtime_args(&["worktree", "list", "--porcelain"]),
        repo_root,
        None,
    )
    .await;
    let existed = list
        .map(|l| {
            let needle = RuntimeString::from("worktree ")
                .concat(worktree_path.as_runtime_string())
                .concat(&RuntimeString::from("\n"));
            l.code == 0 && contains_runtime(&fold_host_path(&l.stdout), &fold_host_path(&needle))
        })
        .unwrap_or(false);
    if existed {
        let head = run_git(
            process,
            &runtime_args(&["rev-parse", "HEAD"]),
            &worktree_path,
            None,
        )
        .await;
        let head_commit = match head {
            Some(h) if h.code == 0 => Some(trim_runtime(&h.stdout)),
            _ => None,
        };
        return Ok(CreatedWorktree {
            worktree_path,
            worktree_branch,
            head_commit,
            existed: true,
        });
    }

    let native_worktrees = project_runtime_path(&worktrees_dir(repo_root), "worktree root")
        .map_err(|error| format!("{error:?}"))?;
    tokio::fs::create_dir_all(native_worktrees)
        .await
        .map_err(|error: std::io::Error| error.to_string())?;
    let add = run_git(
        process,
        &[
            "worktree".into(),
            "add".into(),
            "-B".into(),
            worktree_branch.clone(),
            worktree_path.as_runtime_string().clone(),
            RuntimeString::from(base_ref),
        ],
        repo_root,
        None,
    )
    .await;
    let ok = add.as_ref().map(|a| a.code == 0).unwrap_or(false);
    if !ok {
        let detail = add
            .as_ref()
            .map(|a| runtime_scalar(&trim_runtime(&a.stderr)))
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "git unavailable".to_string());
        return Err(format!("git worktree add failed: {detail}"));
    }
    let head = run_git(
        process,
        &runtime_args(&["rev-parse", "HEAD"]),
        &worktree_path,
        None,
    )
    .await;
    let head_commit = match head {
        Some(h) if h.code == 0 => Some(trim_runtime(&h.stdout)),
        _ => None,
    };
    Ok(CreatedWorktree {
        worktree_path,
        worktree_branch,
        head_commit,
        existed: false,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChangeSummary {
    pub changed_files: usize,
    pub commits: usize,
}

impl RuntimeEncode for ChangeSummary {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self {
            changed_files,
            commits,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("changedFiles", changed_files)?;
        object.field("commits", commits)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ChangeSummary {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                changed_files: object.required("changedFiles", &[])?,
                commits: object.required("commits", &[])?,
            })
        })
    }
}

/// Count uncommitted files + new commits in a worktree, or return `None` when the
/// state can't be reliably determined. Callers MUST treat `None` as "unknown,
/// assume unsafe" (fail-closed) — a silent 0/0 would let removal destroy real work.
pub async fn count_worktree_changes(
    process: &RuntimeProcessContext,
    worktree_path: &RuntimePath,
    original_head_commit: Option<&RuntimeString>,
) -> Option<ChangeSummary> {
    let status = run_git(
        process,
        &[
            "-C".into(),
            worktree_path.as_runtime_string().clone(),
            "status".into(),
            "--porcelain".into(),
        ],
        worktree_path,
        None,
    )
    .await?;
    if status.code != 0 {
        return None;
    }
    let changed_files = status
        .stdout
        .as_utf16()
        .split(|unit| *unit == u16::from(b'\n'))
        .filter(|line| line.iter().any(|unit| !is_ascii_space(*unit)))
        .count();

    // Without a baseline commit we can see it's a git tree but can't count commits.
    let base = match original_head_commit {
        Some(value) if !value.is_empty() => value,
        _ => return None,
    };

    let range = base.concat(&RuntimeString::from("..HEAD"));
    let rev_list = run_git(
        process,
        &[
            "-C".into(),
            worktree_path.as_runtime_string().clone(),
            "rev-list".into(),
            "--count".into(),
            range,
        ],
        worktree_path,
        None,
    )
    .await?;
    if rev_list.code != 0 {
        return None;
    }
    let commits = runtime_scalar(&trim_runtime(&rev_list.stdout))
        .parse::<usize>()
        .unwrap_or(0);
    Some(ChangeSummary {
        changed_files,
        commits,
    })
}

/// Remove a worktree and delete its branch (best-effort; ignores failures).
pub async fn remove_worktree(
    process: &RuntimeProcessContext,
    repo_root: &RuntimePath,
    worktree_path: &RuntimePath,
    worktree_branch: Option<&RuntimeString>,
) {
    let _ = run_git(
        process,
        &[
            "worktree".into(),
            "remove".into(),
            "--force".into(),
            worktree_path.as_runtime_string().clone(),
        ],
        repo_root,
        None,
    )
    .await;
    if let Some(branch) = worktree_branch.filter(|branch| !branch.is_empty()) {
        let _ = run_git(
            process,
            &["branch".into(), "-D".into(), branch.clone()],
            repo_root,
            None,
        )
        .await;
    }
}

/// The notice injected into a worktree-isolated sub-agent so it understands its environment.
/// Conveys the behavioral facts: it runs in a separate working copy of the
/// same repo at `worktree_path`; any paths in inherited parent context refer to `parent_cwd` and must
/// be translated to the worktree root; re-read files before editing (the parent may have changed
/// them); and its edits stay isolated and never touch the parent's tree.
pub fn build_worktree_notice(
    parent_cwd: &RuntimePath,
    worktree_path: &RuntimePath,
) -> RuntimeString {
    RuntimeString::from("You are running in an isolated git worktree at ")
        .concat(worktree_path.as_runtime_string())
        .concat(&RuntimeString::from(
            " — a separate working copy of the same repository (same relative file layout, independent checkout). Any file paths in context inherited from the parent agent refer to its working directory (",
        ))
        .concat(parent_cwd.as_runtime_string())
        .concat(&RuntimeString::from(
            "); translate them to your worktree root before acting on them. Re-read a file before editing if the parent may have modified it since it appeared in context. Your changes stay in this worktree and do not affect the parent's files.",
        ))
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

fn contains_runtime(value: &RuntimeString, needle: &RuntimeString) -> bool {
    needle.is_empty()
        || value
            .as_utf16()
            .windows(needle.len_utf16())
            .any(|window| window == needle.as_utf16())
}

fn runtime_scalar(value: &RuntimeString) -> String {
    String::from_utf8(value.to_node_utf8()).expect("Node UTF-8 projection is valid UTF-8")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};
    use std::process::Command as StdCommand;

    fn git(args: &[&str], cwd: &Path) {
        StdCommand::new("git")
            .args(args)
            .current_dir(cwd)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .expect("git runs");
    }

    fn git_out(args: &[&str], cwd: &Path) -> String {
        let out = StdCommand::new("git")
            .args(args)
            .current_dir(cwd)
            .output()
            .expect("git runs");
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    fn host_path(path: &RuntimePath) -> PathBuf {
        project_runtime_path(path, "worktree test path").unwrap()
    }

    fn realpath(path: &RuntimePath) -> PathBuf {
        crate::fs::node_path::canonicalize_host(host_path(path)).unwrap()
    }

    fn unique_dir(prefix: &str, tag: &str) -> PathBuf {
        let uniq = format!(
            "{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        std::env::temp_dir().join(format!("{prefix}-{uniq}"))
    }

    struct TempRepo {
        _external: tokio::sync::MutexGuard<'static, ()>,
        dir: PathBuf,
    }

    impl TempRepo {
        async fn new(tag: &str) -> Self {
            let external = crate::test_support::external_process_lock().await;
            let dir = unique_dir("ae-wt", tag);
            std::fs::create_dir_all(&dir).unwrap();
            git(&["init", "-q"], &dir);
            git(&["checkout", "-q", "-b", "work"], &dir);
            git(&["config", "user.email", "a@b.c"], &dir);
            git(&["config", "user.name", "T"], &dir);
            std::fs::write(dir.join("a.txt"), "hello\n").unwrap();
            git(&["add", "."], &dir);
            git(&["commit", "-q", "-m", "first"], &dir);
            TempRepo {
                _external: external,
                dir,
            }
        }
        fn dir(&self) -> String {
            self.dir.to_string_lossy().into_owned()
        }

        fn runtime_dir(&self) -> RuntimePath {
            runtime_path_from_native(&self.dir)
        }
    }

    impl Drop for TempRepo {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    /// A fresh repo plus its canonical root (the shared per-test setup).
    async fn setup(tag: &str) -> (TempRepo, RuntimePath, RuntimeProcessContext) {
        let process = crate::runtime_process::test_runtime_process_context();
        let repo = TempRepo::new(tag).await;
        let root = find_canonical_git_root(&process, &repo.runtime_dir())
            .await
            .unwrap_or_else(|| repo.runtime_dir());
        (repo, root, process)
    }

    // -- build_worktree_notice --

    #[test]
    fn notice_names_the_worktree_path_and_parent_cwd_and_states_changes_stay_isolated() {
        let notice = build_worktree_notice(
            &RuntimePath::from("/repo"),
            &RuntimePath::from("/repo/.klaus/worktrees/wt-abc"),
        );
        assert!(notice.contains("/repo/.klaus/worktrees/wt-abc"));
        assert!(notice.contains("/repo"));
        let lower = runtime_scalar(&notice).to_lowercase();
        assert!(lower.contains("worktree"));
        assert!(lower.contains("do not affect") || lower.contains("stay in this worktree"));
    }

    // -- validate_worktree_slug --

    #[test]
    fn accepts_plain_dotted_and_nested_slugs() {
        assert!(validate_worktree_slug("feature").is_ok());
        assert!(validate_worktree_slug("a.b-c_d").is_ok());
        assert!(validate_worktree_slug("user/feature").is_ok());
    }

    #[test]
    fn rejects_traversal_segments_and_unexpected_characters() {
        assert!(validate_worktree_slug("..").is_err());
        assert!(validate_worktree_slug("a/..").is_err());
        assert!(validate_worktree_slug(".").is_err());
        assert!(validate_worktree_slug("a/./b").is_err());
        assert!(validate_worktree_slug("a b").is_err()); // space
        assert!(validate_worktree_slug("a+b").is_err()); // "+" is reserved for flattening
        assert!(validate_worktree_slug("a//b").is_err()); // empty segment
    }

    #[test]
    fn rejects_slugs_over_the_length_cap() {
        assert!(validate_worktree_slug(&"a".repeat(65)).is_err());
        assert!(validate_worktree_slug(&"a".repeat(64)).is_ok());
    }

    // -- path/branch derivation --

    #[test]
    fn flattens_nested_slugs_injectively_with_plus() {
        assert_eq!(flatten_slug("user/feature"), "user+feature");
        assert_eq!(
            worktree_branch_name("user/feature"),
            "worktree-user+feature"
        );
        // The worktree path is joined for the real filesystem, so the separator is the
        // host's; the layout under the repo is the same on every platform.
        let expected = if cfg!(windows) {
            r"\repo\.klaus\worktrees\user+feature"
        } else {
            "/repo/.klaus/worktrees/user+feature"
        };
        assert_eq!(
            worktree_path_for(&RuntimePath::from("/repo"), "user/feature"),
            RuntimePath::from(expected)
        );
    }

    // -- find_canonical_git_root --

    #[tokio::test]
    async fn resolves_the_main_repo_root() {
        let process = crate::runtime_process::test_runtime_process_context();
        let repo = TempRepo::new("root").await;
        let found = find_canonical_git_root(&process, &repo.runtime_dir()).await;
        assert!(found.is_some());
        assert_eq!(realpath(&found.unwrap()), realpath(&repo.runtime_dir()));
    }

    #[tokio::test]
    async fn returns_none_outside_a_repo() {
        let process = crate::runtime_process::test_runtime_process_context();
        let plain = unique_dir("ae-plain", "plain");
        std::fs::create_dir_all(&plain).unwrap();
        let result = find_canonical_git_root(&process, &runtime_path_from_native(&plain)).await;
        let _ = std::fs::remove_dir_all(&plain);
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn resolves_the_main_root_even_from_inside_a_linked_worktree() {
        let (_repo, root, process) = setup("inside").await;
        let c = get_or_create_worktree(&process, &root, "inner", None)
            .await
            .unwrap();
        let from_inside = find_canonical_git_root(&process, &c.worktree_path).await;
        assert_eq!(realpath(&from_inside.unwrap()), realpath(&root));
    }

    // -- get_or_create_worktree --

    #[tokio::test]
    async fn creates_an_isolated_worktree_on_a_b_branch_writes_do_not_touch_the_main_tree() {
        let (repo, root, process) = setup("feat").await;
        let c = get_or_create_worktree(&process, &root, "feat", None)
            .await
            .unwrap();
        assert!(!c.existed);
        assert!(host_path(&c.worktree_path).exists());
        assert_eq!(c.worktree_branch, "worktree-feat");
        let sha_re = Regex::new(r"^[0-9a-f]{40}$").unwrap();
        assert!(sha_re.is_match(&runtime_scalar(c.head_commit.as_ref().unwrap())));

        std::fs::write(host_path(&c.worktree_path).join("wt-only.txt"), "x\n").unwrap();
        // The new file lives only in the worktree, never the main checkout.
        assert!(host_path(&c.worktree_path).join("wt-only.txt").exists());
        assert!(!Path::new(&repo.dir()).join("wt-only.txt").exists());
    }

    #[tokio::test]
    async fn resumes_an_existing_worktree_instead_of_recreating_it() {
        let (_repo, root, process) = setup("resume").await;
        let first = get_or_create_worktree(&process, &root, "feat", None)
            .await
            .unwrap();
        let second = get_or_create_worktree(&process, &root, "feat", None)
            .await
            .unwrap();
        assert!(second.existed);
        assert_eq!(second.worktree_path, first.worktree_path);
    }

    // -- count_worktree_changes (fail-closed) --

    #[tokio::test]
    async fn reports_a_clean_worktree_as_zero_zero() {
        let (_repo, root, process) = setup("cln").await;
        let c = get_or_create_worktree(&process, &root, "cln", None)
            .await
            .unwrap();
        assert_eq!(
            count_worktree_changes(&process, &c.worktree_path, c.head_commit.as_ref()).await,
            Some(ChangeSummary {
                changed_files: 0,
                commits: 0,
            })
        );
    }

    #[tokio::test]
    async fn counts_uncommitted_files() {
        let (_repo, root, process) = setup("dirty").await;
        let c = get_or_create_worktree(&process, &root, "dirty", None)
            .await
            .unwrap();
        std::fs::write(host_path(&c.worktree_path).join("new.txt"), "x\n").unwrap();
        let ch = count_worktree_changes(&process, &c.worktree_path, c.head_commit.as_ref()).await;
        assert!(ch.unwrap().changed_files > 0);
    }

    #[tokio::test]
    async fn counts_new_commits_past_the_baseline() {
        let (_repo, root, process) = setup("committed").await;
        let c = get_or_create_worktree(&process, &root, "committed", None)
            .await
            .unwrap();
        let worktree_host = host_path(&c.worktree_path);
        std::fs::write(worktree_host.join("b.txt"), "y\n").unwrap();
        git(&["add", "."], &worktree_host);
        git(&["commit", "-q", "-m", "work commit"], &worktree_host);
        let ch = count_worktree_changes(&process, &c.worktree_path, c.head_commit.as_ref()).await;
        assert_eq!(ch.unwrap().commits, 1);
    }

    #[tokio::test]
    async fn returns_none_unknown_unsafe_without_a_baseline_commit() {
        let (_repo, root, process) = setup("nobase").await;
        let c = get_or_create_worktree(&process, &root, "nobase", None)
            .await
            .unwrap();
        assert_eq!(
            count_worktree_changes(&process, &c.worktree_path, None).await,
            None
        );
    }

    #[tokio::test]
    async fn returns_none_for_a_non_git_path() {
        let (repo, _root, process) = setup("nonpath").await;
        let missing = Path::new(&repo.dir()).join("does-not-exist");
        assert_eq!(
            count_worktree_changes(
                &process,
                &runtime_path_from_native(&missing),
                Some(&RuntimeString::from("deadbeef")),
            )
            .await,
            None
        );
    }

    // -- remove_worktree --

    #[tokio::test]
    async fn deletes_the_worktree_directory_and_its_branch() {
        let (repo, root, process) = setup("gone").await;
        let c = get_or_create_worktree(&process, &root, "gone", None)
            .await
            .unwrap();
        assert!(host_path(&c.worktree_path).exists());
        remove_worktree(&process, &root, &c.worktree_path, Some(&c.worktree_branch)).await;
        assert!(!host_path(&c.worktree_path).exists());
        assert_eq!(
            git_out(
                &["branch", "--list", "worktree-gone"],
                Path::new(&repo.dir())
            ),
            ""
        );
    }
}
