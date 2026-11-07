//! Filesystem checkpointing via a SHADOW git repository. It snapshots and
//! restores the project's working tree WITHOUT touching the user's real
//! repository: a dedicated git dir lives under
//! `<config_home>/checkpoints/<project_hash>/.git`, while the work-tree points at
//! the actual project root. So `add`/`commit`/`restore` all operate on the
//! project's files but record history in the isolated shadow repo — the user's
//! own `.git`, branches, index, and history are never read or modified.
//!
//! This is the FILESYSTEM half of the checkpoint feature; the conversation half
//! is the session's history rewind. A host composes them: snapshot before a turn,
//! then rewind + restore to undo both.
//!
//! Isolation: the shadow git invocations run with `GIT_CONFIG_GLOBAL` pinned to
//! the null device and the system config disabled via `GIT_CONFIG_NOSYSTEM`, so
//! NONE of the user's global/system git config is read — no clean/smudge filters
//! (git-lfs, git-crypt), no `include`/`includeIf`, no aliases or commit templates
//! can run against the working tree or transform a snapshot. On top of that,
//! `--git-dir` / `--work-tree` redirect the repo, `-c` flags supply the shadow
//! identity and pin commit-signing / autocrlf / default-branch, and `run_git`'s
//! own hardening keeps hooks and fsmonitor off. (A denylist of `-c` overrides
//! alone is NOT enough — `filter.*` is an unbounded name-keyed namespace that a
//! global config can define, so config isolation, not flag enumeration, is what
//! makes a snapshot byte-faithful.)
//!
//! Restore is destructive — it reverts tracked files AND `git clean`s untracked
//! ones back to the snapshot, discarding any work done since — so the feature is
//! opt-in at the host level.

use crate::fs::node_path::{
    host_path_dialect, project_runtime_path, resolve_runtime, runtime_path_from_native,
};
use crate::fs::paths::config_home;
use crate::git::git::{run_git, RunResult};
use crate::runtime_process::{path_join, RuntimeEnv, RuntimeProcessContext};
use crate::runtime_values::{RuntimePath, RuntimeString};
use sha2::{Digest, Sha256};
use std::fmt;
use tokio::sync::OnceCell;

#[cfg(windows)]
const NULL_DEVICE: &str = "NUL";
#[cfg(not(windows))]
const NULL_DEVICE: &str = "/dev/null";

/// Fixed identity for shadow-repo commits — never the user's git identity.
const SHADOW_NAME: &str = "Klaus Checkpoint";
const SHADOW_EMAIL: &str = "checkpoint@klaus.local";

/// Options for constructing a [`CheckpointService`].
#[derive(Debug, Clone)]
pub struct CheckpointServiceOptions {
    /// Project root whose working tree is snapshotted (the shadow repo's work-tree).
    pub project_root: RuntimePath,
    /// Base directory for shadow repos. Defaults to `<config_home>/checkpoints`.
    pub base_dir: Option<RuntimePath>,
}

/// A checkpoint failure.
#[derive(Debug, Clone)]
pub struct CheckpointError {
    pub message: String,
    /// True when the failure is "git binary not available" (vs a git command error).
    pub git_missing: bool,
}

impl CheckpointError {
    /// A checkpoint failure from a git command error (not a missing binary).
    pub fn new(message: impl Into<String>) -> Self {
        CheckpointError {
            message: message.into(),
            git_missing: false,
        }
    }

    /// A checkpoint failure because the `git` binary is unavailable.
    pub fn git_unavailable(message: impl Into<String>) -> Self {
        CheckpointError {
            message: message.into(),
            git_missing: true,
        }
    }
}

impl fmt::Display for CheckpointError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for CheckpointError {}

#[derive(Debug)]
pub struct CheckpointService {
    process_context: RuntimeProcessContext,
    project_root: RuntimePath,
    /// `<config_home>/checkpoints/<project_hash>` — holds the shadow `.git`.
    shadow_dir: RuntimePath,
    /// The shadow repo's git dir (`<shadow_dir>/.git`).
    git_dir: RuntimePath,
    /// Memoizes the one-time initialization so concurrent callers share it; a
    /// failed init leaves the cell empty so a later call can retry.
    init_cell: OnceCell<()>,
}

impl CheckpointService {
    pub fn new(process_context: RuntimeProcessContext, opts: CheckpointServiceOptions) -> Self {
        let process_cwd = std::env::current_dir()
            .map(|path| runtime_path_from_native(&path))
            .unwrap_or_else(|_| RuntimePath::from("."));
        let project_root = resolve_runtime(&process_cwd, &[opts.project_root])
            .expect("captured checkpoint cwd resolves an exact project path");
        let base = opts.base_dir.unwrap_or_else(|| {
            path_join(
                host_path_dialect(),
                &[
                    RuntimePath::from(config_home()),
                    RuntimePath::from("checkpoints"),
                ],
            )
        });
        // Key the shadow repo by the project's absolute path so different projects
        // (and a moved/cloned checkout) get distinct, stable shadow histories.
        let hash = sha256_hex(project_root.as_runtime_string());
        let hash = &hash[..16];
        let shadow_dir = path_join(host_path_dialect(), &[base, RuntimePath::from(hash)]);
        let git_dir = path_join(
            host_path_dialect(),
            &[shadow_dir.clone(), RuntimePath::from(".git")],
        );
        CheckpointService {
            process_context,
            project_root,
            shadow_dir,
            git_dir,
            init_cell: OnceCell::new(),
        }
    }

    /// The shadow-repo flag prefix applied to EVERY git invocation: redirect the
    /// repo (`--git-dir`/`--work-tree`) and override the user's interfering config
    /// keys. `run_git` additionally prepends its hooks-off / fsmonitor-off hardening.
    fn flags(&self) -> Vec<RuntimeString> {
        vec![
            "--git-dir".into(),
            self.git_dir.as_runtime_string().clone(),
            "--work-tree".into(),
            self.project_root.as_runtime_string().clone(),
            "-c".into(),
            RuntimeString::from(format!("user.name={SHADOW_NAME}")),
            "-c".into(),
            RuntimeString::from(format!("user.email={SHADOW_EMAIL}")),
            // Never sign shadow commits with the user's key (would block commits / prompt).
            "-c".into(),
            "commit.gpgsign=false".into(),
            // Snapshot/restore bytes verbatim — no line-ending translation either way.
            "-c".into(),
            "core.autocrlf=false".into(),
            "-c".into(),
            "core.safecrlf=false".into(),
            // Ignore the user's GLOBAL excludes; the project's own .gitignore still applies.
            "-c".into(),
            RuntimeString::from(format!("core.excludesFile={NULL_DEVICE}")),
            // Deterministic shadow branch name (don't inherit the user's init.defaultBranch).
            "-c".into(),
            "init.defaultBranch=main".into(),
        ]
    }

    /// Environment that ISOLATES the shadow git from the user's global/system
    /// config: `GIT_CONFIG_GLOBAL` points at the null device (empty config) and
    /// `GIT_CONFIG_NOSYSTEM` disables the system config — so no user-defined
    /// clean/smudge filter, include, or alias is ever consulted while staging or
    /// checking out the working tree.
    fn env(&self) -> RuntimeEnv {
        let mut env = RuntimeEnv::new();
        env.insert("GIT_CONFIG_GLOBAL".into(), NULL_DEVICE.into());
        env.insert("GIT_CONFIG_NOSYSTEM".into(), "1".into());
        env
    }

    async fn git(&self, args: &[&str]) -> Option<RunResult> {
        let flags = self.flags();
        let mut all = flags;
        all.extend(args.iter().copied().map(RuntimeString::from));
        let env = self.env();
        run_git(&self.process_context, &all, &self.project_root, Some(&env)).await
    }

    /// True when a `git` binary is available (a snapshot/restore needs one).
    pub async fn is_git_available(process: &RuntimeProcessContext, cwd: &RuntimePath) -> bool {
        match run_git(process, &["--version".into()], cwd, None).await {
            Some(r) => r.code == 0,
            None => false,
        }
    }

    /// Create the shadow repository if it does not exist (idempotent; the work is
    /// done at most once per instance). A fresh shadow repo gets an empty initial
    /// commit so there is always a base `HEAD` to diff snapshots against.
    pub async fn initialize(&self) -> Result<(), CheckpointError> {
        self.init_cell
            .get_or_try_init(|| self.do_initialize())
            .await
            .map(|_| ())
    }

    async fn do_initialize(&self) -> Result<(), CheckpointError> {
        let native_shadow = project_runtime_path(&self.shadow_dir, "checkpoint shadow directory")
            .map_err(|error| CheckpointError::new(format!("{error:?}")))?;
        tokio::fs::create_dir_all(&native_shadow)
            .await
            .map_err(|e| CheckpointError::new(e.to_string()))?;
        let native_git_dir = project_runtime_path(&self.git_dir, "checkpoint git directory")
            .map_err(|error| CheckpointError::new(format!("{error:?}")))?;
        if native_git_dir.exists() {
            return Ok(()); // already initialized
        }

        let init = match self.git(&["init"]).await {
            None => {
                return Err(CheckpointError::git_unavailable(
                    "checkpointing requires git, but it is not available",
                ));
            }
            Some(r) => r,
        };
        if init.code != 0 {
            // A concurrent process may have created the repo between our existence
            // check and this init — treat that as success rather than failing the
            // race loser.
            if native_git_dir.exists() {
                return Ok(());
            }
            return Err(CheckpointError::new(format!(
                "failed to initialize the checkpoint repository: {}",
                runtime_scalar(&trim_runtime(&init.stderr))
            )));
        }
        // Seed an empty base commit so the first real snapshot always has a parent HEAD.
        let seed = match self
            .git(&["commit", "--allow-empty", "-m", "checkpoint: init"])
            .await
        {
            None => {
                return Err(CheckpointError::git_unavailable(
                    "checkpointing requires git, but it is not available",
                ));
            }
            Some(r) => r,
        };
        if seed.code != 0 {
            // A concurrent initializer already produced the base commit (its
            // index.lock or a pre-existing HEAD makes ours fail) — a HEAD now
            // existing means we are done.
            if let Some(head) = self.git(&["rev-parse", "--verify", "HEAD"]).await {
                if head.code == 0 {
                    return Ok(());
                }
            }
            return Err(CheckpointError::new(format!(
                "failed to seed the checkpoint repository: {}",
                runtime_scalar(&trim_runtime(&seed.stderr))
            )));
        }
        Ok(())
    }

    /// The shadow repo's current HEAD commit hash, or `None` if it can't be read.
    pub async fn head_hash(&self) -> Option<String> {
        match self.git(&["rev-parse", "HEAD"]).await {
            Some(r) if r.code == 0 => Some(runtime_scalar(&trim_runtime(&r.stdout))),
            _ => None,
        }
    }

    /// Snapshot the project's CURRENT working tree and return the commit hash: a
    /// stable id to restore to later. Stages everything (`add -A`, so deletions are
    /// captured too) respecting the project's `.gitignore`; if nothing changed
    /// since the last snapshot, returns the current HEAD without an empty commit.
    /// Returns `None` only when git is unavailable.
    pub async fn snapshot(&self, label: &str) -> Result<Option<String>, CheckpointError> {
        // Honor the documented contract: a missing git binary yields None (a no-op),
        // not a thrown error, even on the very first snapshot (which runs init).
        match self.initialize().await {
            Ok(()) => {}
            Err(e) => {
                if e.git_missing {
                    return Ok(None);
                }
                return Err(e);
            }
        }

        let add = match self.git(&["add", "-A"]).await {
            None => return Ok(None),
            Some(r) => r,
        };
        if add.code != 0 {
            return Err(CheckpointError::new(format!(
                "checkpoint snapshot (stage) failed: {}",
                runtime_scalar(&trim_runtime(&add.stderr))
            )));
        }

        let status = self.git(&["status", "--porcelain"]).await;
        if let Some(status) = &status {
            if status.code == 0 && trim_runtime(&status.stdout).is_empty() {
                // Clean — the working tree already matches HEAD; reuse it (no empty commit).
                return Ok(self.head_hash().await);
            }
        }

        let commit = match self
            .git(&[
                "commit",
                "-m",
                &format!("checkpoint: {label}"),
                "--allow-empty",
            ])
            .await
        {
            None => return Ok(None),
            Some(r) => r,
        };
        if commit.code != 0 {
            return Err(CheckpointError::new(format!(
                "checkpoint snapshot (commit) failed: {}",
                runtime_scalar(&trim_runtime(&commit.stderr))
            )));
        }
        Ok(self.head_hash().await)
    }

    /// Restore the project's working tree to a prior snapshot. DESTRUCTIVE: tracked
    /// files are reset to the snapshot AND untracked files are removed
    /// (`git clean -fd`), discarding any work done since the snapshot. The shadow
    /// HEAD is NOT moved (history is preserved), so a later snapshot still extends
    /// the same timeline. Errors on an unknown hash.
    pub async fn restore(&self, hash: &str) -> Result<(), CheckpointError> {
        self.initialize().await?;
        // Resolve the snapshot's tree first — this both validates the hash (an
        // unknown hash fails here with a clear error) and tells us whether it is
        // EMPTY. For an empty-tree snapshot (the seed commit of a brand-new project)
        // `git restore -- .` errors with "pathspec did not match", so we skip restore
        // and let `git clean` empty the tree instead — otherwise the undo would
        // error before clean runs.
        let tree = match self.git(&["ls-tree", "-r", "--name-only", hash]).await {
            None => {
                return Err(CheckpointError::git_unavailable(
                    "checkpoint restore requires git, but it is not available",
                ));
            }
            Some(r) => r,
        };
        if tree.code != 0 {
            return Err(CheckpointError::new(format!(
                "failed to restore checkpoint {hash}: {}",
                runtime_scalar(&trim_runtime(&tree.stderr))
            )));
        }

        if !trim_runtime(&tree.stdout).is_empty() {
            let restore = match self.git(&["restore", "--source", hash, "--", "."]).await {
                None => {
                    return Err(CheckpointError::git_unavailable(
                        "checkpoint restore requires git, but it is not available",
                    ));
                }
                Some(r) => r,
            };
            if restore.code != 0 {
                return Err(CheckpointError::new(format!(
                    "failed to restore checkpoint {hash}: {}",
                    runtime_scalar(&trim_runtime(&restore.stderr))
                )));
            }
        }
        // Remove untracked files/dirs created since the snapshot (respects
        // .gitignore — no `-x`, so ignored artifacts like node_modules are left
        // untouched).
        let clean = self.git(&["clean", "-f", "-d"]).await;
        match &clean {
            Some(c) if c.code == 0 => Ok(()),
            _ => {
                let detail = match &clean {
                    Some(c) => runtime_scalar(&trim_runtime(&c.stderr)),
                    None => "git unavailable".to_string(),
                };
                Err(CheckpointError::new(format!(
                    "failed to clean untracked files on restore: {detail}"
                )))
            }
        }
    }

    /// What [`Self::restore`] would touch, without touching anything. Two sources,
    /// because the restore is two operations: tracked files that differ from the
    /// snapshot (reset back) and untracked files (deleted by `git clean -f -d`).
    /// Paths are project-relative, as git prints them, and deduplicated.
    ///
    /// This is the number a confirmation prompt is allowed to show. Deriving it
    /// from the caller's own edit records would understate the damage: the restore
    /// reverts everything that moved since the snapshot, including edits no tool
    /// of ours ever reported.
    ///
    /// Validates `hash` the way `restore` does, so an unknown snapshot fails here
    /// instead of reading as "nothing would change".
    pub async fn changed_files(&self, hash: &str) -> Result<Vec<String>, CheckpointError> {
        self.initialize().await?;
        // `diff --name-only <hash>` compares the snapshot against the WORKING TREE,
        // so it needs no `add -A` and leaves the index alone.
        let diff = match self.git(&["diff", "--name-only", hash, "--"]).await {
            None => {
                return Err(CheckpointError::git_unavailable(
                    "checkpoint preview requires git, but it is not available",
                ));
            }
            Some(r) => r,
        };
        if diff.code != 0 {
            return Err(CheckpointError::new(format!(
                "failed to read checkpoint {hash}: {}",
                runtime_scalar(&trim_runtime(&diff.stderr))
            )));
        }
        let mut files = split_lines(&diff.stdout);
        // The same exclusion the restore's clean uses: no `-x`, so ignored artifacts
        // are not listed as about to be deleted.
        if let Some(untracked) = self
            .git(&["ls-files", "--others", "--exclude-standard"])
            .await
        {
            if untracked.code == 0 {
                for path in split_lines(&untracked.stdout) {
                    if !files.contains(&path) {
                        files.push(path);
                    }
                }
            }
        }
        Ok(files)
    }
}

/// Non-empty trimmed lines of a git stdout listing.
fn split_lines(value: &RuntimeString) -> Vec<String> {
    runtime_scalar(&trim_runtime(value))
        .lines()
        .map(|line| line.trim().to_string())
        .filter(|line| !line.is_empty())
        .collect()
}

/// Lowercase hex encoding of the SHA-256 digest of `s`.
fn sha256_hex(value: &RuntimeString) -> String {
    let mut h = Sha256::new();
    h.update(value.to_node_utf8());
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

fn trim_runtime(value: &RuntimeString) -> RuntimeString {
    let units = value.as_utf16();
    let mut start = 0;
    let mut end = units.len();
    while start < end && matches!(units[start], 0x09..=0x0d | 0x20) {
        start += 1;
    }
    while end > start && matches!(units[end - 1], 0x09..=0x0d | 0x20) {
        end -= 1;
    }
    value.slice_utf16(start..end)
}

fn runtime_scalar(value: &RuntimeString) -> String {
    String::from_utf8(value.to_node_utf8()).expect("Node UTF-8 projection is valid UTF-8")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};
    use std::process::Command as StdCommand;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::sync::{Mutex, MutexGuard};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    /// Serializes the one test that mutates the process environment (git config
    /// env vars are shared across the parallel test threads).
    static ENV_LOCK: Mutex<()> = Mutex::const_new(());

    /// A temp layout mirroring the shadow-git test harness: `<dir>/project` is the
    /// working tree, `<dir>/shadow` is the isolated shadow-repo base.
    struct Fixture {
        _external: MutexGuard<'static, ()>,
        dir: PathBuf,
        root: PathBuf,
        base: PathBuf,
    }

    impl Fixture {
        async fn new(tag: &str) -> Self {
            let external = crate::test_support::external_process_lock().await;
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let uniq = format!(
                "ae-checkpoint-{tag}-{}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                n
            );
            let dir = std::env::temp_dir().join(uniq);
            let root = dir.join("project");
            let base = dir.join("shadow");
            std::fs::create_dir_all(&root).unwrap();
            Fixture {
                _external: external,
                dir,
                root,
                base,
            }
        }

        fn service(&self) -> CheckpointService {
            CheckpointService::new(
                crate::runtime_process::test_runtime_process_context(),
                CheckpointServiceOptions {
                    project_root: self.root.to_string_lossy().into_owned().into(),
                    base_dir: Some(self.base.to_string_lossy().into_owned().into()),
                },
            )
        }

        fn root_str(&self) -> String {
            self.root.to_string_lossy().into_owned()
        }

        fn read(&self, rel: &str) -> String {
            std::fs::read_to_string(self.root.join(rel)).unwrap()
        }

        fn write(&self, rel: &str, content: &str) {
            std::fs::write(self.root.join(rel), content).unwrap();
        }

        fn exists_rel(&self, rel: &str) -> bool {
            self.root.join(rel).exists()
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    /// Run a real `git` command directly (used to set up a project's OWN repo).
    fn git_raw(args: &[&str], cwd: &Path) {
        StdCommand::new("git")
            .args(args)
            .current_dir(cwd)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .expect("git runs");
    }

    fn is_sha40(s: &str) -> bool {
        s.len() == 40
            && s.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    }

    #[tokio::test]
    async fn reports_git_availability() {
        let f = Fixture::new("avail").await;
        assert!(
            CheckpointService::is_git_available(
                &crate::runtime_process::test_runtime_process_context(),
                &f.root_str().into(),
            )
            .await
        );
    }

    #[tokio::test]
    async fn snapshots_and_restores_a_modified_tracked_file() {
        let f = Fixture::new("modtracked").await;
        f.write("a.txt", "original");
        let svc = f.service();
        let snap = svc.snapshot("before").await.unwrap().unwrap();
        assert!(is_sha40(&snap));

        f.write("a.txt", "CHANGED");
        assert_eq!(f.read("a.txt"), "CHANGED");

        svc.restore(&snap).await.unwrap();
        assert_eq!(f.read("a.txt"), "original");
    }

    #[tokio::test]
    async fn removes_an_untracked_file_created_after_the_snapshot_on_restore() {
        let f = Fixture::new("untracked").await;
        f.write("keep.txt", "keep");
        let svc = f.service();
        let snap = svc.snapshot("s1").await.unwrap().unwrap();

        f.write("added.txt", "new file");
        assert!(f.exists_rel("added.txt"));

        svc.restore(&snap).await.unwrap();
        assert!(!f.exists_rel("added.txt")); // cleaned
        assert_eq!(f.read("keep.txt"), "keep"); // untouched
    }

    #[tokio::test]
    async fn restores_to_an_earlier_snapshot_dropping_a_file_a_later_snapshot_tracked() {
        // S1: only a.txt. S2: adds b.txt (now tracked in the shadow repo).
        let f = Fixture::new("earlier").await;
        f.write("a.txt", "A");
        let svc = f.service();
        let s1 = svc.snapshot("s1").await.unwrap().unwrap();
        f.write("b.txt", "B");
        svc.snapshot("s2").await.unwrap();
        assert!(f.exists_rel("b.txt"));

        // Restoring to S1 must remove b.txt even though a LATER snapshot tracked it.
        svc.restore(&s1).await.unwrap();
        assert!(!f.exists_rel("b.txt"));
        assert_eq!(f.read("a.txt"), "A");
    }

    #[tokio::test]
    async fn returns_the_existing_head_no_new_commit_when_nothing_changed() {
        let f = Fixture::new("nochange").await;
        f.write("a.txt", "A");
        let svc = f.service();
        let first = svc.snapshot("s1").await.unwrap();
        let second = svc.snapshot("s1-again").await.unwrap(); // working tree unchanged
        assert_eq!(second, first);
    }

    #[tokio::test]
    async fn respects_gitignore_ignored_file_is_neither_snapshotted_nor_cleaned() {
        let f = Fixture::new("gitignore").await;
        f.write(".gitignore", "ignored.txt\n");
        f.write("tracked.txt", "T");
        let svc = f.service();
        let snap = svc.snapshot("s1").await.unwrap().unwrap();

        // Create an ignored file AND change the tracked one.
        f.write("ignored.txt", "secret");
        f.write("tracked.txt", "CHANGED");

        svc.restore(&snap).await.unwrap();
        assert_eq!(f.read("tracked.txt"), "T"); // reverted
        assert!(f.exists_rel("ignored.txt")); // NOT cleaned (ignored)
        assert_eq!(f.read("ignored.txt"), "secret");
    }

    #[tokio::test]
    async fn does_not_create_or_touch_a_git_in_the_project_directory() {
        let f = Fixture::new("nogit").await;
        f.write("a.txt", "A");
        f.service().snapshot("s1").await.unwrap();
        // The shadow repo lives under base/, never in the project root.
        assert!(!f.exists_rel(".git"));
    }

    #[tokio::test]
    async fn works_in_a_project_that_already_has_its_own_git_repo() {
        let f = Fixture::new("ownrepo").await;
        // Give the project a real repo with a commit.
        git_raw(&["init"], &f.root);
        git_raw(
            &[
                "-c",
                "user.email=t@t",
                "-c",
                "user.name=t",
                "commit",
                "--allow-empty",
                "-m",
                "real",
            ],
            &f.root,
        );
        f.write("a.txt", "A");

        let svc = f.service();
        let snap = svc.snapshot("s1").await.unwrap().unwrap();
        f.write("a.txt", "CHANGED");
        svc.restore(&snap).await.unwrap();
        assert_eq!(f.read("a.txt"), "A");
        // The user's real repo still exists.
        assert!(f.exists_rel(".git"));
    }

    #[tokio::test]
    async fn throws_a_clear_error_when_restoring_an_unknown_hash() {
        let f = Fixture::new("unknown").await;
        f.write("a.txt", "A");
        let svc = f.service();
        svc.snapshot("s1").await.unwrap();
        let err = svc.restore(&"0".repeat(40)).await.unwrap_err();
        assert!(err.message.contains("restore checkpoint"));
    }

    #[tokio::test]
    async fn restores_an_empty_brand_new_project_snapshot_by_clearing_the_tree() {
        // No files yet → snapshot returns the empty seed commit.
        let f = Fixture::new("empty").await;
        let svc = f.service();
        let seed = svc.snapshot("empty").await.unwrap().unwrap();
        assert!(is_sha40(&seed));

        f.write("agent-new.txt", "created by the agent");
        // Restore must NOT error on the empty tree; it should clean the new file instead.
        svc.restore(&seed).await.unwrap();
        assert!(!f.exists_rel("agent-new.txt"));
    }

    /// Restores/clears the two git-config env vars on drop (RAII try/finally),
    /// while holding [`ENV_LOCK`] to serialize this test against any other that
    /// touches the process environment.
    struct EnvGuard {
        _lock: MutexGuard<'static, ()>,
        prev_global: Option<String>,
        prev_system: Option<String>,
    }

    impl EnvGuard {
        async fn set(cfg: &Path) -> Self {
            let lock = ENV_LOCK.lock().await;
            let prev_global = std::env::var("GIT_CONFIG_GLOBAL").ok();
            let prev_system = std::env::var("GIT_CONFIG_SYSTEM").ok();
            crate::runtime_process::set_process_environment_var("GIT_CONFIG_GLOBAL", cfg);
            crate::runtime_process::set_process_environment_var("GIT_CONFIG_SYSTEM", "/dev/null");
            EnvGuard {
                _lock: lock,
                prev_global,
                prev_system,
            }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match &self.prev_global {
                Some(v) => {
                    crate::runtime_process::set_process_environment_var("GIT_CONFIG_GLOBAL", v)
                }
                None => crate::runtime_process::remove_process_environment_var("GIT_CONFIG_GLOBAL"),
            }
            match &self.prev_system {
                Some(v) => {
                    crate::runtime_process::set_process_environment_var("GIT_CONFIG_SYSTEM", v)
                }
                None => crate::runtime_process::remove_process_environment_var("GIT_CONFIG_SYSTEM"),
            }
        }
    }

    #[tokio::test]
    async fn isolates_from_a_user_global_clean_smudge_filter_and_stores_bytes_verbatim() {
        let f = Fixture::new("isolate").await;
        // A malicious/benign (git-lfs-like) global filter that would TRANSFORM content.
        let cfg = f.dir.join("evil.gitconfig");
        std::fs::write(
            &cfg,
            "[user]\n  name = X\n  email = x@x\n[filter \"up\"]\n  clean = tr a-z A-Z\n  smudge = cat\n",
        )
        .unwrap();

        let _env = EnvGuard::set(&cfg).await;

        f.write(".gitattributes", "data.txt filter=up\n");
        f.write("data.txt", "hello world");
        let svc = f.service();
        let snap = svc.snapshot("s1").await.unwrap().unwrap();

        f.write("data.txt", "TEMP");
        svc.restore(&snap).await.unwrap();
        // If the global filter had leaked, the snapshot would hold 'HELLO WORLD'
        // (clean) and restore would yield that; isolation preserves bytes verbatim.
        assert_eq!(f.read("data.txt"), "hello world");
    }

    #[tokio::test]
    async fn does_not_wipe_a_nested_git_repo_and_its_uncommitted_work_on_restore() {
        let f = Fixture::new("nested").await;
        f.write("a.txt", "A");
        let svc = f.service();
        let snap = svc.snapshot("s1").await.unwrap().unwrap();

        // A nested repo with uncommitted work, created AFTER the snapshot.
        let nested = f.root.join("nested");
        std::fs::create_dir_all(&nested).unwrap();
        git_raw(&["init"], &nested);
        std::fs::write(nested.join("work.txt"), "precious uncommitted work").unwrap();

        svc.restore(&snap).await.unwrap();
        // git clean -fd refuses to recurse into a directory that is itself a git
        // repo, so the nested repo and its work survive (only -ff would wipe it).
        assert!(nested.join("work.txt").exists());
        assert_eq!(
            std::fs::read_to_string(nested.join("work.txt")).unwrap(),
            "precious uncommitted work"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn does_not_follow_a_symlink_out_of_the_project_tree_on_restore() {
        use std::os::unix::fs::symlink;

        let f = Fixture::new("symlink").await;
        let outside = f.dir.join("outside-secret.txt");
        std::fs::write(&outside, "do not touch").unwrap();
        symlink(&outside, f.root.join("link")).unwrap();
        let svc = f.service();
        let snap = svc.snapshot("s1").await.unwrap().unwrap();

        f.write("a.txt", "changed"); // make restore do real work
        svc.restore(&snap).await.unwrap();
        // The symlink target (outside the work-tree) is untouched by restore/clean.
        assert_eq!(std::fs::read_to_string(&outside).unwrap(), "do not touch");
    }
}
