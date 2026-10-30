//! Shared on-disk layout for the engine's persistent state, under the engine's
//! own config home (a `~/.<tool>` convention). Subsystems put their data under
//! here: `tasks/<listId>` (TodoV2), `teams/<team>` (swarm), `worktrees/`
//! (worktree), etc.

use crate::permission::bash::node_path::{basename, dirname, is_absolute, join, resolve};
use regex::Regex;
use std::path::Path;
use std::sync::LazyLock;
use unicode_normalization::UnicodeNormalization;

/// The engine config home: `$KLAUS_CONFIG_DIR` or `~/.klaus`, NFC-normalized.
pub fn config_home() -> String {
    let override_dir = std::env::var("KLAUS_CONFIG_DIR").ok();
    let base = match override_dir {
        Some(v) if !v.is_empty() => v,
        _ => join(&[&home_dir(), ".klaus"]),
    };
    base.nfc().collect()
}

/// Replace every char outside `[A-Za-z0-9_-]` with `-`, so an id can't escape
/// its directory (path-traversal guard).
///
/// The replacement operates per UTF-16 code unit, so a supplementary-plane
/// character (e.g. an emoji) becomes TWO dashes.
pub fn sanitize_path_component(value: &str) -> String {
    value
        .encode_utf16()
        .map(|unit| match unit {
            0x2d | 0x30..=0x39 | 0x41..=0x5a | 0x5f | 0x61..=0x7a => unit as u8 as char,
            _ => '-',
        })
        .collect()
}

/// Team-name projection: JavaScript
/// `/[^a-zA-Z0-9]/g` followed by ASCII lowercase. The regex has no `u` flag,
/// so operate per UTF-16 code unit (an astral character becomes two dashes).
pub fn sanitize_team_name(value: &str) -> String {
    value
        .encode_utf16()
        .map(|unit| match unit {
            0x41..=0x5a => (unit as u8 + (b'a' - b'A')) as char,
            0x30..=0x39 | 0x61..=0x7a => unit as u8 as char,
            _ => '-',
        })
        .collect()
}

/// Directory holding one task list's per-task JSON files: `<home>/tasks/<listId>`.
pub fn tasks_dir(list_id: &str) -> String {
    join(&[&config_home(), "tasks", &sanitize_path_component(list_id)])
}

/// Root of all swarm team directories: `<home>/teams`.
pub fn teams_root() -> String {
    join(&[&config_home(), "teams"])
}

/// One team's directory: `<home>/teams/<team>`. The component is sanitized, so
/// a crafted team name can't escape `teams/`.
pub fn team_dir(team: &str) -> String {
    join(&[&teams_root(), &sanitize_team_name(team)])
}

/// Shared-format team configuration file.
pub fn team_config_path(team: &str) -> String {
    join(&[&team_dir(team), "config.json"])
}

/// A team's peer roster file.
pub fn roster_path(team: &str) -> String {
    join(&[&team_dir(team), "roster.json"])
}

/// A peer's append-only mailbox within a team.
pub fn inbox_path(team: &str, peer: &str) -> String {
    join(&[
        &team_dir(team),
        &format!("inbox-{}.jsonl", sanitize_path_component(peer)),
    ])
}

/// A peer's read cursor (count of inbox lines already delivered).
pub fn cursor_path(team: &str, peer: &str) -> String {
    join(&[
        &team_dir(team),
        &format!("inbox-{}.cursor", sanitize_path_component(peer)),
    ])
}

/// Root of the per-thread goal records: `<home>/goals`.
pub fn goals_root() -> String {
    join(&[&config_home(), "goals"])
}

/// One thread's goal record: `<home>/goals/<threadId>.json`. The component is
/// sanitized, so a crafted thread id cannot escape `goals/`.
pub fn goal_file_path(thread_id: &str) -> String {
    join(&[
        &goals_root(),
        &format!("{}.json", sanitize_path_component(thread_id)),
    ])
}

// ---- Project keying (projects/<slug> convention) ----------------------

/// Slug length cap; longer inputs are truncated and disambiguated with a hash.
const PROJECT_SLUG_MAX: usize = 200;

/// Turn an absolute project path into a stable directory slug:
/// every char outside `[A-Za-z0-9]` becomes `-`; overlong results are truncated
/// to [`PROJECT_SLUG_MAX`] and suffixed with a base-36 string hash so two long
/// paths sharing a 200-char prefix cannot collide.
pub fn project_slug(project_path: &str) -> String {
    // Like the sanitizer, the transform operates per UTF-16 code unit; the
    // resulting slug is pure ASCII, so byte length == code-unit length.
    let slug: String = project_path
        .encode_utf16()
        .map(|unit| match unit {
            0x30..=0x39 | 0x41..=0x5a | 0x61..=0x7a => unit as u8 as char,
            _ => '-',
        })
        .collect();
    if slug.len() <= PROJECT_SLUG_MAX {
        return slug;
    }
    let hash = i64::from(string_hash(project_path)).unsigned_abs();
    format!("{}-{}", &slug[..PROJECT_SLUG_MAX], to_base36(hash))
}

/// Java-style 31x string hash over UTF-16 code units with 32-bit wrapping
/// arithmetic (the slug disambiguator).
fn string_hash(s: &str) -> i32 {
    let mut h: i32 = 0;
    for unit in s.encode_utf16() {
        h = h
            .wrapping_shl(5)
            .wrapping_sub(h)
            .wrapping_add(i32::from(unit));
    }
    h
}

/// Lowercase base-36 rendering of a non-negative integer.
fn to_base36(mut n: u64) -> String {
    const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    if n == 0 {
        return "0".to_string();
    }
    let mut buf: Vec<u8> = Vec::new();
    while n > 0 {
        buf.push(DIGITS[(n % 36) as usize]);
        n /= 36;
    }
    buf.reverse();
    String::from_utf8(buf).unwrap()
}

/// Walk up from `cwd` to the nearest directory containing `.git` (dir or file);
/// that is the project root. Falls back to `cwd` itself when no repository is
/// found — per-directory keying still beats no keying.
pub fn find_project_root(cwd: &str) -> String {
    let mut dir = resolve(&[cwd]);
    loop {
        if crate::git::git::is_git_entry(Path::new(&join(&[&dir, ".git"]))) {
            return dir;
        }
        let parent = dirname(&dir);
        if parent == dir {
            return resolve(&[cwd]);
        }
        dir = parent;
    }
}

/// The `gitdir: <path>` pointer line inside a worktree's `.git` FILE.
static GITDIR_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?m)^gitdir:\s*(.+?)\s*$").unwrap());

/// Resolve a git WORKTREE checkout to its main repository root,
/// so every worktree of a repo shares one memory/projects key. A worktree's
/// `.git` is a FILE containing `gitdir: <mainRepo>/.git/worktrees/<name>`; the
/// main git dir is `<gitdir>/<commondir>` and the repo root is its parent — UNLESS
/// that git dir is itself a bare repo (basename != `.git`), in which case the
/// git dir IS the root (the bare-repo guard).
///
/// Security + fidelity contract:
///  - We ONLY resolve when the gitdir sits under a `.git/worktrees/<name>` layout,
///    which EXCLUDES submodule gitdirs (`.git/modules/…`) — a submodule keys by
///    its own checkout, not the superproject.
///  - INTEGRITY: git records a back-pointer at `<gitdir>/gitdir` holding the
///    absolute path of THIS checkout's `.git` file. We require it to point back
///    here. A `.git` file planted in an unpacked archive (pointing at some victim
///    repo elsewhere on disk) fails this check — forging it would require write
///    access to the victim repo — so a crafted `.git` cannot steer a memory
///    directory (which becomes a prompt-free writable root) to an arbitrary path.
///    Legit worktrees created anywhere (incl. siblings via `git worktree add`)
///    pass, because git writes the matching back-pointer.
///  - A bare-repo main git dir (basename != `.git`) IS the root.
///  - Any parse/stat failure or unmet guard returns `root` unchanged.
pub fn resolve_main_repo_root(root: &str) -> String {
    try_resolve_main_repo_root(root).unwrap_or_else(|| resolve(&[root]))
}

/// The fallible body of [`resolve_main_repo_root`]; `None` marks an I/O failure,
/// which the caller maps to `resolve(root)`.
fn try_resolve_main_repo_root(root: &str) -> Option<String> {
    let start = resolve(&[root]);
    let dot_git = join(&[&start, ".git"]);
    if !std::fs::metadata(&dot_git).ok()?.is_file() {
        return Some(start);
    }
    let content = read_to_string_lossy(&dot_git).ok()?;
    let target = match GITDIR_RE.captures(&content).and_then(|c| c.get(1)) {
        Some(m) if !m.as_str().is_empty() => m.as_str().to_string(),
        _ => return Some(start),
    };
    let gitdir = if is_absolute(&target) {
        target
    } else {
        resolve(&[&start, &target])
    };
    // Only a worktree gitdir (`<mainGit>/worktrees/<name>`) resolves; anything
    // else (notably `.git/modules/<name>` submodules) keys by its own checkout.
    if basename(&dirname(&gitdir)) != "worktrees" {
        return Some(start);
    }
    // Integrity: the gitdir must point back at THIS checkout's `.git` file.
    let back_raw = read_to_string_lossy(&join(&[&gitdir, "gitdir"])).ok()?;
    let back = trim(&back_raw);
    if resolve(&[back]) != dot_git {
        return Some(start);
    }
    let common_file = join(&[&gitdir, "commondir"]);
    let main_git_dir = if Path::new(&common_file).exists() {
        let common = read_to_string_lossy(&common_file).ok()?;
        resolve(&[&gitdir, trim(&common)])
    } else {
        dirname(&dirname(&gitdir))
    };
    // A bare repo's git dir is not named `.git`; the git dir itself is the root.
    Some(if basename(&main_git_dir) == ".git" {
        dirname(&main_git_dir)
    } else {
        main_git_dir
    })
}

// ---- helpers ----------------------------------------------------------------

/// The current user's home directory ("" when it cannot be determined).
fn home_dir() -> String {
    #[allow(deprecated)] // fine on modern toolchains; `home_dir` is un-deprecated
    std::env::home_dir()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// Read a file as lossily-decoded UTF-8 (invalid sequences become U+FFFD).
fn read_to_string_lossy(path: &str) -> std::io::Result<String> {
    Ok(String::from_utf8_lossy(&std::fs::read(path)?).into_owned())
}

/// Trim leading/trailing whitespace, including a BOM / zero-width no-break space.
fn trim(s: &str) -> &str {
    s.trim_matches(|c: char| c.is_whitespace() || c == '\u{feff}')
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    /// Serializes tests that read/write `KLAUS_CONFIG_DIR` (the process
    /// environment is shared across test threads).
    use crate::test_support::CONFIG_ENV_LOCK as ENV_LOCK;

    struct EnvGuard {
        _lock: std::sync::MutexGuard<'static, ()>,
        prev: Option<String>,
    }

    impl EnvGuard {
        fn set(value: Option<&str>) -> Self {
            let lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            let prev = std::env::var("KLAUS_CONFIG_DIR").ok();
            match value {
                Some(v) => {
                    crate::runtime_process::set_process_environment_var("KLAUS_CONFIG_DIR", v)
                }
                None => crate::runtime_process::remove_process_environment_var("KLAUS_CONFIG_DIR"),
            }
            EnvGuard { _lock: lock, prev }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match &self.prev {
                Some(v) => {
                    crate::runtime_process::set_process_environment_var("KLAUS_CONFIG_DIR", v)
                }
                None => crate::runtime_process::remove_process_environment_var("KLAUS_CONFIG_DIR"),
            }
        }
    }

    fn make_temp_dir(tag: &str) -> PathBuf {
        let base = std::env::temp_dir();
        let uniq = format!(
            "ae-paths-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let dir = base.join(uniq);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// A POSIX-shaped expectation rendered with the host's separator.
    ///
    /// The layout itself is identical on every platform; only the separator differs, and
    /// it is spelled out per platform here rather than taken from the code under test so
    /// an expectation cannot agree with a wrong answer.
    fn host_shaped(expected: &str) -> String {
        if cfg!(windows) {
            expected.replace('/', "\\")
        } else {
            expected.to_string()
        }
    }

    // ---- config_home ---------------------------------------------------------

    #[test]
    fn config_home_uses_override_env_var() {
        let _g = EnvGuard::set(Some("/custom/config"));
        assert_eq!(config_home(), "/custom/config");
    }

    #[test]
    fn config_home_nfc_normalizes() {
        // A decomposed "café" (e + combining acute) folds to the precomposed form.
        let _g = EnvGuard::set(Some("/tmp/cafe\u{301}"));
        assert_eq!(config_home(), "/tmp/caf\u{e9}");
    }

    #[test]
    fn config_home_defaults_to_dot_klaus_under_home() {
        let _g = EnvGuard::set(None);
        assert!(config_home().ends_with(&host_shaped("/.klaus")));
    }

    #[test]
    fn config_home_ignores_empty_override() {
        let _g = EnvGuard::set(Some(""));
        assert!(config_home().ends_with(&host_shaped("/.klaus")));
    }

    // ---- sanitize_path_component ----------------------------------------------

    #[test]
    fn sanitize_path_component_replaces_disallowed_chars() {
        assert_eq!(sanitize_path_component("my list!"), "my-list-");
        assert_eq!(sanitize_path_component("a/b\\c"), "a-b-c");
        assert_eq!(sanitize_path_component("ok_id-123"), "ok_id-123");
        assert_eq!(sanitize_path_component("../escape"), "---escape");
        assert_eq!(sanitize_path_component(""), "");
    }

    #[test]
    fn sanitize_path_component_is_per_utf16_code_unit() {
        // A BMP char is one code unit → one dash; a supplementary-plane char is
        // a surrogate pair → two dashes.
        assert_eq!(sanitize_path_component("é"), "-");
        assert_eq!(sanitize_path_component("pre😀post"), "pre--post");
    }

    #[test]
    fn sanitize_team_name_matches_claude_code() {
        assert_eq!(sanitize_team_name("My_Team Name"), "my-team-name");
        assert_eq!(sanitize_team_name("A😀B"), "a--b");
    }

    // ---- layout paths ----------------------------------------------------------

    #[test]
    fn layout_paths_are_rooted_at_config_home() {
        let _g = EnvGuard::set(Some("/cfg"));
        assert_eq!(tasks_dir("my list"), host_shaped("/cfg/tasks/my-list"));
        assert_eq!(teams_root(), host_shaped("/cfg/teams"));
        assert_eq!(team_dir("A_/b"), host_shaped("/cfg/teams/a--b"));
        assert_eq!(
            team_config_path("My Team"),
            host_shaped("/cfg/teams/my-team/config.json")
        );
        assert_eq!(roster_path("t"), host_shaped("/cfg/teams/t/roster.json"));
        assert_eq!(
            inbox_path("t", "peer 1"),
            host_shaped("/cfg/teams/t/inbox-peer-1.jsonl")
        );
        assert_eq!(
            cursor_path("t", "peer 1"),
            host_shaped("/cfg/teams/t/inbox-peer-1.cursor")
        );
    }

    // ---- project_slug ----------------------------------------------------------

    #[test]
    fn project_slug_replaces_non_alphanumerics() {
        assert_eq!(project_slug("/Users/me/proj"), "-Users-me-proj");
        // per-code-unit: the emoji (a surrogate pair) becomes two dashes
        assert_eq!(project_slug("/a😀b"), "-a--b");
        // underscores are NOT kept by the project transform (unlike the sanitizer)
        assert_eq!(project_slug("a_b"), "a-b");
    }

    #[test]
    fn project_slug_keeps_up_to_200_chars() {
        let exact = "a".repeat(200);
        assert_eq!(project_slug(&exact), exact);
    }

    #[test]
    fn project_slug_truncates_and_hashes_long_paths() {
        // 201 chars: truncated to 200 and disambiguated with the base-36 hash.
        let long = "a".repeat(201);
        assert_eq!(project_slug(&long), format!("{}-rkvsv5", "a".repeat(200)));
        // A 300-char path: two long paths sharing a 200-char prefix cannot collide.
        let p = "/x".repeat(150);
        assert_eq!(project_slug(&p), format!("{}-4y6yx2", "-x".repeat(100)));
    }

    // ---- find_project_root ------------------------------------------------------

    #[test]
    fn find_project_root_walks_up_to_git_dir() {
        let dir = make_temp_dir("root");
        let root = dir.join("repo");
        let nested = root.join("a").join("b");
        crate::git::git::seed_git_dir(&root.join(".git")).unwrap();
        std::fs::create_dir_all(&nested).unwrap();
        assert_eq!(
            find_project_root(&nested.to_string_lossy()),
            root.to_string_lossy()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn find_project_root_accepts_git_file() {
        // a worktree's `.git` is a FILE — it still marks the project root
        let dir = make_temp_dir("gitfile");
        let root = dir.join("wt");
        let nested = root.join("src");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(root.join(".git"), "gitdir: /elsewhere\n").unwrap();
        assert_eq!(
            find_project_root(&nested.to_string_lossy()),
            root.to_string_lossy()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn find_project_root_falls_back_to_cwd() {
        let dir = make_temp_dir("nogit");
        let nested = dir.join("x").join("y");
        std::fs::create_dir_all(&nested).unwrap();
        assert_eq!(
            find_project_root(&nested.to_string_lossy()),
            nested.to_string_lossy()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- resolve_main_repo_root ---------------------------------------------------

    /// Lay out `<base>/main` (main repo) + `<base>/wt` (worktree checkout) with
    /// a valid gitdir pointer, back-pointer, and commondir. Returns (wt, main).
    fn write_worktree_layout(base: &std::path::Path) -> (String, String) {
        let main = base.join("main");
        let wt = base.join("wt");
        let gitdir = main.join(".git").join("worktrees").join("wt");
        std::fs::create_dir_all(&gitdir).unwrap();
        std::fs::create_dir_all(&wt).unwrap();
        std::fs::write(
            wt.join(".git"),
            format!("gitdir: {}\n", gitdir.to_string_lossy()),
        )
        .unwrap();
        std::fs::write(
            gitdir.join("gitdir"),
            format!("{}\n", wt.join(".git").to_string_lossy()),
        )
        .unwrap();
        std::fs::write(gitdir.join("commondir"), "../..\n").unwrap();
        (
            wt.to_string_lossy().into_owned(),
            main.to_string_lossy().into_owned(),
        )
    }

    #[test]
    fn resolve_main_repo_root_resolves_worktree_to_main() {
        let dir = make_temp_dir("wtres");
        let (wt, main) = write_worktree_layout(&dir);
        assert_eq!(resolve_main_repo_root(&wt), main);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn resolve_main_repo_root_returns_plain_repo_unchanged() {
        // `.git` is a directory → not a worktree → the root itself
        let dir = make_temp_dir("plain");
        let repo = dir.join("repo");
        crate::git::git::seed_git_dir(&repo.join(".git")).unwrap();
        let r = repo.to_string_lossy().into_owned();
        assert_eq!(resolve_main_repo_root(&r), r);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn resolve_main_repo_root_returns_input_resolved_when_no_git() {
        // stat failure (no `.git` at all) → the resolved input unchanged
        let dir = make_temp_dir("none");
        let d = dir.to_string_lossy().into_owned();
        assert_eq!(resolve_main_repo_root(&d), d);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn resolve_main_repo_root_ignores_submodule_gitdirs() {
        // a `.git/modules/<name>` gitdir is NOT a worktree — keys by its own checkout
        let dir = make_temp_dir("sub");
        let main = dir.join("main");
        let sub = main.join("sub");
        let gitdir = main.join(".git").join("modules").join("sub");
        std::fs::create_dir_all(&gitdir).unwrap();
        std::fs::create_dir_all(&sub).unwrap();
        std::fs::write(
            sub.join(".git"),
            format!("gitdir: {}\n", gitdir.to_string_lossy()),
        )
        .unwrap();
        let s = sub.to_string_lossy().into_owned();
        assert_eq!(resolve_main_repo_root(&s), s);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn resolve_main_repo_root_rejects_forged_back_pointer() {
        // a planted `.git` whose gitdir does not point back here must not resolve
        let dir = make_temp_dir("forge");
        let (wt, _main) = write_worktree_layout(&dir);
        let gitdir = dir.join("main").join(".git").join("worktrees").join("wt");
        std::fs::write(gitdir.join("gitdir"), "/somewhere/else/.git\n").unwrap();
        assert_eq!(resolve_main_repo_root(&wt), wt);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn resolve_main_repo_root_missing_back_pointer_returns_root() {
        let dir = make_temp_dir("noback");
        let (wt, _main) = write_worktree_layout(&dir);
        let gitdir = dir.join("main").join(".git").join("worktrees").join("wt");
        std::fs::remove_file(gitdir.join("gitdir")).unwrap();
        assert_eq!(resolve_main_repo_root(&wt), wt);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn resolve_main_repo_root_bare_repo_gitdir_is_root() {
        // main git dir not named `.git` (bare repo) → the git dir itself is the root
        let dir = make_temp_dir("bare");
        let bare = dir.join("bare.git");
        let wt = dir.join("wt");
        let gitdir = bare.join("worktrees").join("wt");
        std::fs::create_dir_all(&gitdir).unwrap();
        std::fs::create_dir_all(&wt).unwrap();
        std::fs::write(
            wt.join(".git"),
            format!("gitdir: {}\n", gitdir.to_string_lossy()),
        )
        .unwrap();
        std::fs::write(
            gitdir.join("gitdir"),
            format!("{}\n", wt.join(".git").to_string_lossy()),
        )
        .unwrap();
        std::fs::write(gitdir.join("commondir"), "../..\n").unwrap();
        assert_eq!(
            resolve_main_repo_root(&wt.to_string_lossy()),
            bare.to_string_lossy()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn resolve_main_repo_root_without_commondir_uses_grandparent() {
        let dir = make_temp_dir("nocommon");
        let (wt, main) = write_worktree_layout(&dir);
        let gitdir = dir.join("main").join(".git").join("worktrees").join("wt");
        std::fs::remove_file(gitdir.join("commondir")).unwrap();
        assert_eq!(resolve_main_repo_root(&wt), main);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn resolve_main_repo_root_relative_gitdir_resolves_from_checkout() {
        let dir = make_temp_dir("relgd");
        let main = dir.join("main");
        let wt = dir.join("wt");
        let gitdir = main.join(".git").join("worktrees").join("wt");
        std::fs::create_dir_all(&gitdir).unwrap();
        std::fs::create_dir_all(&wt).unwrap();
        std::fs::write(wt.join(".git"), "gitdir: ../main/.git/worktrees/wt\n").unwrap();
        std::fs::write(
            gitdir.join("gitdir"),
            format!("{}\n", wt.join(".git").to_string_lossy()),
        )
        .unwrap();
        std::fs::write(gitdir.join("commondir"), "../..\n").unwrap();
        assert_eq!(
            resolve_main_repo_root(&wt.to_string_lossy()),
            main.to_string_lossy()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
