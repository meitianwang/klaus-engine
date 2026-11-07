//! Per-file LAZY memory loader — computes the memory attachments contextual to a
//! touched file, using the shared markdown-memory helpers it
//! calls. The eager loader (`memory_files`) injects the User/Project/Local hierarchy ONCE in the
//! system prompt and loads only UNCONDITIONAL rules. THIS loader fires when a file tool touches a
//! path and surfaces the memory that is *contextual* to that path:
//!   1. User-tier conditional rules whose `paths:` glob matches the touched file.
//!   2. Memory in directories NESTED between the cwd and the touched file (a subdir's own
//!      AGENTS.md / .klaus rules that the eager cwd→root walk never visited).
//!   3. cwd-and-above conditional rules whose glob matches the touched file.
//!
//! Returns the ordered `MemoryFileInfo` list across all phases. Cross-turn dedup + read-state
//! marking are the CALLER's job (the reminder provider) — its per-agent loaded-paths and
//! read-file-state tracking, which must outlive a single computation. The loader's own
//! `processed_paths` set is fresh per call (a new set is built per
//! invocation).
//!
//! Engine naming mirrors the eager loader: `AGENTS.md`→`KLAUS.md` files, `.klaus/` dot-dir,
//! `~/.klaus` user home.
//!
//! NOT built (host-only, not a logic simplification): the enterprise **Managed** conditional-rules
//! tier (a managed OS-policy rules dir has no engine equivalent), the
//! skip-project-level experiment (a default-off feature flag), and the
//! instructions-loaded audit hook.

use crate::context::memory_files::{
    process_first_file, process_md_rules, MemoryFileInfo, MemoryGates, MemoryType, AGENTS_NAMES,
    LOCAL_NAMES,
};
use crate::fs::paths::config_home;
use crate::runtime_values::RuntimeString;
use ignore::gitignore::GitignoreBuilder;
use std::collections::HashSet;
use std::path::{Component, Path, PathBuf, MAIN_SEPARATOR_STR};

/// Options for the lazy per-file loader.
#[derive(Debug, Clone, Default)]
pub struct NestedMemoryOptions {
    /// Per-tier gates (reused from the eager loader). Only `user`/`project`/`local` are consulted.
    pub gates: Option<MemoryGates>,
    /// User config home (default [`config_home`] = `~/.klaus`). Injectable for tests.
    pub user_config_dir: Option<String>,
}

/// The two directory lists produced by [`get_directories_to_process`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoriesToProcess {
    pub nested_dirs: Vec<String>,
    pub cwd_level_dirs: Vec<String>,
}

// ── path helpers (lexical, matching the loader's platform path semantics) ──

/// Whether `p` is an absolute path.
fn is_absolute(p: &str) -> bool {
    Path::new(p).is_absolute()
}

/// Lexically resolve `p` to an absolute, normalized path (against the process cwd when relative):
/// `.` segments dropped, `..` collapsed, no trailing separator. Purely lexical — no filesystem
/// access.
fn resolve_path(p: &str) -> String {
    let joined = if Path::new(p).is_absolute() {
        PathBuf::from(p)
    } else {
        match std::env::current_dir() {
            Ok(cwd) => cwd.join(p),
            Err(_) => PathBuf::from(MAIN_SEPARATOR_STR).join(p),
        }
    };
    let mut out = PathBuf::new();
    for component in joined.components() {
        match component {
            Component::Prefix(prefix) => out.push(prefix.as_os_str()),
            Component::RootDir => out.push(MAIN_SEPARATOR_STR),
            Component::CurDir => {}
            Component::ParentDir => {
                // `..` above the root is dropped (pop on the root is a no-op).
                out.pop();
            }
            Component::Normal(seg) => out.push(seg),
        }
    }
    let s = out.to_string_lossy().into_owned();
    if s.is_empty() {
        MAIN_SEPARATOR_STR.to_string()
    } else {
        s
    }
}

/// Parent directory: `/a/b` → `/a`, `/a` → `/`, `/` → `/`, a bare name → `.`.
fn dirname(p: &str) -> String {
    match Path::new(p).parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_string_lossy().into_owned(),
        Some(_) => ".".to_string(),
        None => p.to_string(),
    }
}

/// The filesystem root of a path: `/` on POSIX, `C:\` or `\\server\share\` on Windows, and
/// empty for a relative path.
///
/// Derived from `Path::components` rather than written as a literal, because the two
/// walk-up loops in [`get_directories_to_process`] stop when `dirname` reaches this
/// value. Spelling it as `"/"` made those loops never terminate on Windows:
/// `dirname("C:\\")` is `C:\`, which never equals `/`, so each iteration pushed the
/// same string again until the allocator refused a 48 GiB `Vec`. Nested memory runs on
/// every file operation, so that was every file operation on Windows.
fn parse_root(p: &str) -> String {
    let mut root = PathBuf::new();
    for component in Path::new(p).components() {
        match component {
            Component::Prefix(prefix) => root.push(prefix.as_os_str()),
            Component::RootDir => root.push(MAIN_SEPARATOR_STR),
            // The first non-root component ends the root.
            _ => break,
        }
    }
    root.to_string_lossy().into_owned()
}

/// Join known-clean segments onto `base`.
fn join_path(base: &str, segments: &[&str]) -> String {
    let mut out = PathBuf::from(base);
    for seg in segments {
        out.push(seg);
    }
    out.to_string_lossy().into_owned()
}

/// The relative path from `from` to `to` (both resolved first): shared prefix stripped, `..` per
/// remaining `from` component, then the rest of `to`. Empty when they are the same directory.
///
/// Both separators are accepted on the way in and `/` is always produced on the way out.
/// `resolve_path` returns host-native paths, so on Windows the inputs are `\`-separated
/// and splitting on `/` alone yielded a single segment covering the whole path: the
/// result then began with `..` for every input, which [`conditional_rules_matching`]
/// reads as "outside the base" and drops. The visible effect was that no conditional
/// memory rule ever matched on Windows, silently. The output is `/`-joined because it
/// is matched as a gitignore path, and gitignore is `/`-separated on every platform.
fn node_relative(from: &str, to: &str) -> String {
    let from = resolve_path(from);
    let to = resolve_path(to);
    if from == to {
        return String::new();
    }
    let segments_of = |path: &str| -> Vec<String> {
        path.split(['/', '\\'])
            .filter(|segment| !segment.is_empty())
            .map(str::to_string)
            .collect()
    };
    let from_parts = segments_of(&from);
    let to_parts = segments_of(&to);
    let mut common = 0;
    while common < from_parts.len()
        && common < to_parts.len()
        && from_parts[common] == to_parts[common]
    {
        common += 1;
    }
    let mut segments: Vec<&str> = vec![".."; from_parts.len() - common];
    segments.extend(to_parts[common..].iter().map(String::as_str));
    segments.join("/")
}

/// From the touched file, build two directory lists:
///  - `nested_dirs`: dirs strictly BETWEEN the cwd and the file's dir, that live UNDER the cwd
///    (cwd itself excluded), ordered cwd→file (outermost first). These get a FULL memory scan —
///    the eager loader's cwd→root walk never descends into them.
///  - `cwd_level_dirs`: cwd→fs-root (cwd included), ordered root→cwd. These get CONDITIONAL rules
///    only — their unconditional memory was already loaded eagerly.
///
/// The fs root is excluded from both walks (the walk stops at the parsed root).
pub fn get_directories_to_process(target_path: &str, original_cwd: &str) -> DirectoriesToProcess {
    let target_dir = dirname(&resolve_path(target_path));
    let mut nested_dirs: Vec<String> = Vec::new();
    let mut current_dir = target_dir;
    while current_dir != original_cwd && current_dir != parse_root(&current_dir) {
        // Guard against a target OUTSIDE the cwd tree: only keep dirs under the cwd.
        if current_dir.starts_with(original_cwd) {
            nested_dirs.push(current_dir.clone());
        }
        current_dir = dirname(&current_dir);
    }
    nested_dirs.reverse(); // cwd → target (outermost first)

    let mut cwd_level_dirs: Vec<String> = Vec::new();
    current_dir = original_cwd.to_string();
    while current_dir != parse_root(&current_dir) {
        cwd_level_dirs.push(current_dir.clone());
        current_dir = dirname(&current_dir);
    }
    cwd_level_dirs.reverse(); // root → cwd

    DirectoriesToProcess {
        nested_dirs,
        cwd_level_dirs,
    }
}

/// Gitignore-style match of `relative_path` against a rule's `paths:` globs. Case-insensitive
/// (matching the rule matcher's default), and a glob naming a directory covers everything under
/// it (parent-dir matches count). An unparseable glob simply never matches — a malformed rule
/// must never break the file operation that triggered this.
fn globs_match(globs: &[RuntimeString], relative_path: &str) -> bool {
    let mut builder = GitignoreBuilder::new("");
    let _ = builder.case_insensitive(true);
    for glob in globs {
        let Some(glob) = memory_glob_to_gitignore_host(glob) else {
            continue;
        };
        let _ = builder.add_line(None, &glob);
    }
    match builder.build() {
        Ok(matcher) => matcher
            .matched_path_or_any_parents(Path::new(relative_path), false)
            .is_ignore(),
        Err(_) => false,
    }
}

/// The ignore matcher accepts only scalar Rust strings. Frontmatter-created
/// globs are scalar; a runtime-decoded non-scalar glob cannot name a host path
/// and is ignored rather than being replaced or normalized.
fn memory_glob_to_gitignore_host(value: &RuntimeString) -> Option<String> {
    value.try_to_utf8().ok()
}

/// Load the CONDITIONAL rules under `rules_dir` (those WITH a
/// `paths:` glob), then keep only those whose glob matches the touched file. Project globs are
/// relative to the dir containing `.klaus` (`dirname(dirname(rules_dir))`); User globs are
/// relative to the cwd. A relative path that is empty / escapes the base (`..`) / is absolute
/// (a cross-root target) can't match a base-relative glob — and the matcher requires a relative
/// path — so it's dropped.
async fn process_conditioned_md_rules(
    target_path: &str,
    rules_dir: &str,
    r#type: MemoryType,
    processed_paths: &mut HashSet<String>,
    include_external: bool,
    cwd: &str,
) -> Vec<MemoryFileInfo> {
    let conditioned = process_md_rules(
        rules_dir,
        r#type,
        processed_paths,
        include_external,
        true,
        cwd,
        None,
    )
    .await;
    conditioned
        .into_iter()
        .filter(|file| {
            let globs = match &file.globs {
                Some(globs) if !globs.is_empty() => globs,
                _ => return false,
            };
            let base_dir = if r#type == MemoryType::Project {
                dirname(&dirname(rules_dir))
            } else {
                cwd.to_string()
            };
            let relative_path = if is_absolute(target_path) {
                node_relative(&base_dir, target_path)
            } else {
                target_path.to_string()
            };
            if relative_path.is_empty()
                || relative_path.starts_with("..")
                || is_absolute(&relative_path)
            {
                return false;
            }
            globs_match(globs, &relative_path)
        })
        .collect()
}

/// A full memory scan of ONE nested dir — its Project
/// AGENTS.md (+ `.klaus/AGENTS.md`), its Local AGENTS.local.md, its UNCONDITIONAL `.klaus/rules`,
/// and its CONDITIONAL `.klaus/rules` matching the touched file.
///
/// Tier gating follows a deliberate NESTED-loader asymmetry: only the AGENTS.md /
/// `.klaus/AGENTS.md` slot is gated by the Project tier (the project-settings
/// gate), and the Local slot by the Local tier. The
/// `.klaus/rules` passes are NOT gated — the nested loader runs them regardless of
/// the project-settings gate (unlike the EAGER loader, which DOES gate project rules). So with the
/// Project tier explicitly off, a nested dir's AGENTS.md is suppressed but its rules still
/// surface.
///
/// The two rule passes use a SEPARATE `processed_paths` copy for the unconditional pass because
/// `process_md_rules` marks EVERY `.md` it reads in `processed_paths` (including
/// conditional ones it then filters out by the absence of globs). If both passes shared one set,
/// the unconditional pass would pre-mark the conditional rule files and the conditional pass
/// would skip them. So the unconditional pass runs on a copy, the conditional pass on the
/// original, then merges the copy back.
async fn get_memory_files_for_nested_directory(
    dir: &str,
    target_path: &str,
    processed_paths: &mut HashSet<String>,
    gates: &MemoryGates,
    cwd: &str,
) -> Vec<MemoryFileInfo> {
    let mut result: Vec<MemoryFileInfo> = Vec::new();

    if gates.project != Some(false) {
        let candidates: Vec<String> = AGENTS_NAMES.iter().map(|n| join_path(dir, &[n])).collect();
        result.extend(
            process_first_file(
                &candidates,
                MemoryType::Project,
                processed_paths,
                false,
                cwd,
            )
            .await,
        );
        let dot_dir_candidates: Vec<String> = AGENTS_NAMES
            .iter()
            .map(|n| join_path(dir, &[".klaus", n]))
            .collect();
        result.extend(
            process_first_file(
                &dot_dir_candidates,
                MemoryType::Project,
                processed_paths,
                false,
                cwd,
            )
            .await,
        );
    }

    if gates.local != Some(false) {
        let candidates: Vec<String> = LOCAL_NAMES.iter().map(|n| join_path(dir, &[n])).collect();
        result.extend(
            process_first_file(&candidates, MemoryType::Local, processed_paths, false, cwd).await,
        );
    }

    // `.klaus/rules` is UNGATED by the Project tier (see above — the nested-loader asymmetry).
    let rules_dir = join_path(dir, &[".klaus", "rules"]);
    // Unconditional rules on a COPY (see above) — the conditional pass must re-read the same dir.
    let mut unconditional_processed_paths = processed_paths.clone();
    result.extend(
        process_md_rules(
            &rules_dir,
            MemoryType::Project,
            &mut unconditional_processed_paths,
            false,
            false,
            cwd,
            None,
        )
        .await,
    );
    result.extend(
        process_conditioned_md_rules(
            target_path,
            &rules_dir,
            MemoryType::Project,
            processed_paths,
            false,
            cwd,
        )
        .await,
    );
    processed_paths.extend(unconditional_processed_paths);

    result
}

/// Compute the ordered memory files contextual to `file_path`, across the 4 phases (minus the
/// per-attachment build/marking, which the caller does).
/// One fresh `processed_paths` per call. Best-effort context: every fallible step (missing dirs,
/// unreadable files, malformed globs) is absorbed where it occurs, so a malformed rule never
/// breaks the file operation that triggered this.
pub async fn get_nested_memory_for_file(
    file_path: &str,
    cwd: &str,
    opts: &NestedMemoryOptions,
) -> Vec<MemoryFileInfo> {
    let default_gates = MemoryGates::default();
    let gates = opts.gates.as_ref().unwrap_or(&default_gates);
    let user_dir = opts.user_config_dir.clone().unwrap_or_else(config_home);
    let original_cwd = resolve_path(cwd);
    let mut processed_paths: HashSet<String> = HashSet::new();
    let mut result: Vec<MemoryFileInfo> = Vec::new();

    // Phase 1 — User-tier conditional rules matching the touched file (the Managed tier would
    // be scanned first; deferred here). User imports may always reach external files
    // (`include_external` is hardcoded true).
    if gates.user != Some(false) {
        result.extend(
            process_conditioned_md_rules(
                file_path,
                &join_path(&user_dir, &["rules"]),
                MemoryType::User,
                &mut processed_paths,
                true,
                &original_cwd,
            )
            .await,
        );
    }

    // Phase 2 — the nested + cwd-level directory lists.
    let DirectoriesToProcess {
        nested_dirs,
        cwd_level_dirs,
    } = get_directories_to_process(file_path, &original_cwd);

    // Phase 3 — full memory scan of each nested dir (cwd → target).
    for dir in &nested_dirs {
        result.extend(
            get_memory_files_for_nested_directory(
                dir,
                file_path,
                &mut processed_paths,
                gates,
                &original_cwd,
            )
            .await,
        );
    }

    // Phase 4 — cwd-and-above CONDITIONAL rules matching the touched file (root → cwd).
    // Unconditional rules at these levels were already loaded eagerly, so only the conditional
    // ones are surfaced. UNGATED by the Project tier — the nested loader runs these
    // conditional rules regardless of the project-settings gate, the
    // same asymmetry as the nested `.klaus/rules` passes.
    for dir in &cwd_level_dirs {
        result.extend(
            process_conditioned_md_rules(
                file_path,
                &join_path(dir, &[".klaus", "rules"]),
                MemoryType::Project,
                &mut processed_paths,
                false,
                &original_cwd,
            )
            .await,
        );
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::RuntimePath;

    fn temp_dir() -> PathBuf {
        static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let mut dir = std::env::temp_dir();
        let unique = format!(
            "ae-nested-{}-{}-{}",
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

    fn s(p: &Path) -> String {
        p.to_string_lossy().into_owned()
    }

    /// Join a `/`-separated relative path onto `base`, one component at a time.
    ///
    /// `PathBuf::join("a/b")` keeps the literal `/` on Windows, so an expectation built
    /// that way reads `...\proj\a/b` while the code under test returns the normalized
    /// `...\proj\a\b`. Pushing per component gives the host's separator throughout.
    fn at(base: &Path, relative: &str) -> PathBuf {
        let mut path = base.to_path_buf();
        for segment in relative.split('/') {
            path.push(segment);
        }
        path
    }

    fn write(dir: &Path, rel: &str, content: &str) {
        let f = at(dir, rel);
        std::fs::create_dir_all(f.parent().unwrap()).unwrap();
        std::fs::write(f, content).unwrap();
    }

    /// Default the User tier to an isolated (empty) home so the real `~/.klaus` never bleeds in.
    async fn nested(
        dir: &Path,
        cwd_rel: &str,
        file_rel: &str,
        gates: Option<MemoryGates>,
    ) -> Vec<MemoryFileInfo> {
        let opts = NestedMemoryOptions {
            gates,
            user_config_dir: Some(s(&dir.join("userhome"))),
        };
        get_nested_memory_for_file(&s(&at(dir, file_rel)), &s(&at(dir, cwd_rel)), &opts).await
    }

    fn bodies(files: &[MemoryFileInfo]) -> Vec<RuntimeString> {
        files.iter().map(|f| f.content.clone()).collect()
    }

    fn paths_of(files: &[MemoryFileInfo]) -> Vec<RuntimePath> {
        files.iter().map(|f| f.path.clone()).collect()
    }

    // ── get_directories_to_process ──

    #[test]
    fn a_relative_path_under_the_base_never_starts_with_dotdot() {
        // What a conditional rule's glob is matched against. `node_relative` used to
        // split on `/` only, so on Windows the whole `\`-separated path counted as one
        // segment, nothing was ever a shared prefix, and the answer began with `..` —
        // which the caller reads as "outside the base" and drops. No conditional memory
        // rule matched on Windows at all, without any error saying so.
        let base = temp_dir();
        let target = at(&base, "proj/sub/file.ts");
        let relative = node_relative(&s(&base), &s(&target));
        assert_eq!(
            relative, "proj/sub/file.ts",
            "a path inside the base must come back relative and `/`-joined"
        );
        assert!(!relative.starts_with(".."));
        assert!(!is_absolute(&relative));

        // Same directory is empty, and a sibling still escapes with `..`.
        assert_eq!(node_relative(&s(&base), &s(&base)), "");
        assert!(node_relative(&s(&at(&base, "proj")), &s(&at(&base, "other"))).starts_with(".."));
        std::fs::remove_dir_all(&base).ok();
    }

    #[test]
    fn nested_dirs_are_subdirs_cwd_to_target_cwd_excluded_and_cwd_level_dirs_root_to_cwd() {
        let dir = temp_dir();
        let cwd = dir.join("proj");
        let r = get_directories_to_process(&s(&at(&cwd, "a/b/f.ts")), &s(&cwd));
        // outermost first, cwd excluded
        assert_eq!(r.nested_dirs, vec![s(&cwd.join("a")), s(&at(&cwd, "a/b"))]);
        assert_eq!(r.cwd_level_dirs.last().unwrap(), &s(&cwd)); // cwd is last (deepest)
        assert!(r.cwd_level_dirs.contains(&s(&dir))); // ancestors up the tree
        assert!(!r.nested_dirs.contains(&s(&cwd)));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_target_at_the_cwd_level_yields_no_nested_dirs() {
        let dir = temp_dir();
        let cwd = dir.join("proj");
        let r = get_directories_to_process(&s(&cwd.join("f.ts")), &s(&cwd));
        assert!(r.nested_dirs.is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_target_outside_the_cwd_tree_contributes_no_nested_dirs() {
        // startsWith guard
        let dir = temp_dir();
        let cwd = dir.join("proj");
        let r = get_directories_to_process(&s(&at(&dir, "other/f.ts")), &s(&cwd));
        assert!(r.nested_dirs.is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_root_of_a_native_path_is_what_walking_up_actually_reaches() {
        // Both loops in `get_directories_to_process` stop when `dirname` reaches this.
        // A literal "/" made them never terminate on Windows, where walking up ends at
        // `C:\`: each iteration pushed the same string until the allocator refused a
        // 48 GiB `Vec`, on every file operation.
        let absolute = s(&temp_dir().join("a").join("b"));
        let root = parse_root(&absolute);
        assert!(!root.is_empty(), "an absolute path must have a root");
        assert_eq!(
            dirname(&root),
            root,
            "the root has to be the fixed point of dirname, or the walk-up never ends"
        );

        // And walking up from any absolute path reaches it in a bounded number of steps.
        let mut current = absolute.clone();
        let mut steps = 0;
        while current != parse_root(&current) {
            current = dirname(&current);
            steps += 1;
            assert!(steps < 64, "walking up {absolute} did not terminate");
        }

        // A relative path has no root, which is what keeps the guard from firing on one.
        assert_eq!(parse_root("a/b"), "");
    }

    // ── phase 3 (nested directory full scan) ──

    #[tokio::test]
    async fn surfaces_a_nested_subdirs_own_agents_md_and_unconditional_klaus_rule() {
        let dir = temp_dir();
        write(&dir, "proj/sub/AGENTS.md", "SUB-MEM");
        write(&dir, "proj/sub/deep/.klaus/rules/r.md", "DEEP-RULE-UNCOND");
        let out = nested(&dir, "proj", "proj/sub/deep/file.ts", None).await;
        let b = bodies(&out);
        assert!(b.iter().any(|x| x == "SUB-MEM"));
        assert!(b.iter().any(|x| x == "DEEP-RULE-UNCOND"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn does_not_surface_the_cwd_level_agents_md() {
        // Already loaded eagerly — cwd is excluded from nested.
        let dir = temp_dir();
        write(&dir, "proj/AGENTS.md", "CWD-MEM");
        write(&dir, "proj/sub/AGENTS.md", "SUB-MEM");
        let out = nested(&dir, "proj", "proj/sub/file.ts", None).await;
        let b = bodies(&out);
        assert!(b.iter().any(|x| x == "SUB-MEM"));
        assert!(!b.iter().any(|x| x == "CWD-MEM"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn agents_md_beats_klaus_md_in_a_nested_slot_and_dot_dir_is_a_separate_slot() {
        // Engine precedence within the slot; `.klaus/` is its own slot.
        let dir = temp_dir();
        write(&dir, "proj/sub/AGENTS.md", "SUB-AGENTS");
        write(&dir, "proj/sub/KLAUS.md", "SUB-KLAUS");
        write(&dir, "proj/sub/.klaus/AGENTS.md", "SUB-DOTDIR");
        let out = bodies(&nested(&dir, "proj", "proj/sub/file.ts", None).await);
        assert!(out.iter().any(|x| x == "SUB-AGENTS"));
        assert!(!out.iter().any(|x| x == "SUB-KLAUS")); // first-match within the slot
        assert!(out.iter().any(|x| x == "SUB-DOTDIR")); // dot-dir is its own slot, also loaded
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_nested_conditional_rule_surfaces_only_when_its_glob_matches_the_touched_file() {
        // base dir for a nested rule = the dir containing .klaus (proj/sub);
        // relative path = deep/x.ts.
        let dir = temp_dir();
        write(
            &dir,
            "proj/sub/.klaus/rules/match.md",
            "---\npaths: deep/*.ts\n---\nNESTED-MATCH",
        );
        write(
            &dir,
            "proj/sub/.klaus/rules/miss.md",
            "---\npaths: other/*.ts\n---\nNESTED-MISS",
        );
        let out = bodies(&nested(&dir, "proj", "proj/sub/deep/x.ts", None).await);
        assert!(out.iter().any(|x| x == "NESTED-MATCH"));
        assert!(!out.iter().any(|x| x == "NESTED-MISS"));
        std::fs::remove_dir_all(&dir).ok();
    }

    // ── phase 1 (User conditional rules) ──

    #[tokio::test]
    async fn surfaces_a_user_rule_whose_glob_matches_the_touched_file_relative_to_cwd() {
        let dir = temp_dir();
        write(
            &dir,
            "userhome/rules/u.md",
            "---\npaths: \"**/*.ts\"\n---\nUSER-COND",
        );
        write(
            &dir,
            "userhome/rules/n.md",
            "---\npaths: \"**/*.py\"\n---\nUSER-COND-NOMATCH",
        );
        let out = bodies(&nested(&dir, "proj", "proj/sub/file.ts", None).await);
        assert!(out.iter().any(|x| x == "USER-COND"));
        assert!(!out.iter().any(|x| x == "USER-COND-NOMATCH"));
        std::fs::remove_dir_all(&dir).ok();
    }

    // ── phase 4 (cwd-level conditional rules) ──

    #[tokio::test]
    async fn surfaces_a_cwd_level_conditional_rule_matching_the_touched_file() {
        // Unconditional stays eager.
        let dir = temp_dir();
        write(
            &dir,
            "proj/.klaus/rules/cond.md",
            "---\npaths: sub/**\n---\nCWD-COND",
        );
        write(&dir, "proj/.klaus/rules/uncond.md", "CWD-UNCOND");
        let out = bodies(&nested(&dir, "proj", "proj/sub/file.ts", None).await);
        assert!(out.iter().any(|x| x == "CWD-COND"));
        // The unconditional cwd-level rule was already loaded by the EAGER loader — phase 4 is
        // conditional-only, so it must NOT re-surface here.
        assert!(!out.iter().any(|x| x == "CWD-UNCOND"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn surfaces_an_ancestor_level_conditional_rule() {
        // cwd_level_dirs walks above the cwd too. Rule at proj's PARENT (dir):
        // base dir = dir, relative path = proj/sub/file.ts.
        let dir = temp_dir();
        write(
            &dir,
            ".klaus/rules/anc.md",
            "---\npaths: proj/**\n---\nANCESTOR-COND",
        );
        let out = bodies(&nested(&dir, "proj", "proj/sub/file.ts", None).await);
        assert!(out.iter().any(|x| x == "ANCESTOR-COND"));
        std::fs::remove_dir_all(&dir).ok();
    }

    // ── gates + dedup ──

    #[tokio::test]
    async fn per_tier_gates_skip_their_tier() {
        let dir = temp_dir();
        write(
            &dir,
            "userhome/rules/u.md",
            "---\npaths: \"**/*.ts\"\n---\nUSER-G",
        );
        write(&dir, "proj/sub/AGENTS.md", "PROJ-G");
        write(&dir, "proj/sub/AGENTS.local.md", "LOCAL-G");
        let user_off = bodies(
            &nested(
                &dir,
                "proj",
                "proj/sub/f.ts",
                Some(MemoryGates {
                    user: Some(false),
                    ..Default::default()
                }),
            )
            .await,
        );
        assert!(!user_off.iter().any(|x| x == "USER-G"));
        let project_off = bodies(
            &nested(
                &dir,
                "proj",
                "proj/sub/f.ts",
                Some(MemoryGates {
                    project: Some(false),
                    ..Default::default()
                }),
            )
            .await,
        );
        assert!(!project_off.iter().any(|x| x == "PROJ-G"));
        let local_off = bodies(
            &nested(
                &dir,
                "proj",
                "proj/sub/f.ts",
                Some(MemoryGates {
                    local: Some(false),
                    ..Default::default()
                }),
            )
            .await,
        );
        assert!(!local_off.iter().any(|x| x == "LOCAL-G"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn project_false_suppresses_a_nested_agents_md_but_not_nested_or_cwd_level_rules() {
        // Nested memory-file slots are gated by the Project tier, but the rules passes +
        // Phase-4 cwd-level conditional rules UNGATED. So with the Project tier off: AGENTS.md
        // is hidden, rules stay.
        let dir = temp_dir();
        write(&dir, "proj/sub/AGENTS.md", "NESTED-AGENTS");
        write(&dir, "proj/sub/.klaus/rules/u.md", "NESTED-RULE-UNCOND");
        write(
            &dir,
            "proj/sub/.klaus/rules/c.md",
            "---\npaths: \"*.ts\"\n---\nNESTED-RULE-COND",
        );
        write(
            &dir,
            "proj/.klaus/rules/cwd.md",
            "---\npaths: sub/**\n---\nCWD-RULE-COND",
        );
        let out = bodies(
            &nested(
                &dir,
                "proj",
                "proj/sub/file.ts",
                Some(MemoryGates {
                    project: Some(false),
                    ..Default::default()
                }),
            )
            .await,
        );
        assert!(!out.iter().any(|x| x == "NESTED-AGENTS")); // AGENTS.md slot IS gated
        assert!(out.iter().any(|x| x == "NESTED-RULE-UNCOND")); // .klaus/rules ungated
        assert!(out.iter().any(|x| x == "NESTED-RULE-COND")); // nested conditional rule ungated
        assert!(out.iter().any(|x| x == "CWD-RULE-COND")); // Phase-4 cwd-level conditional rule ungated
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn does_not_return_the_same_file_twice_across_phases() {
        // processed_paths dedup.
        let dir = temp_dir();
        write(&dir, "proj/sub/AGENTS.md", "SUB-ONCE");
        let out = paths_of(&nested(&dir, "proj", "proj/sub/deep/file.ts", None).await);
        let sub_path = RuntimePath::from(s(&at(&dir, "proj/sub/AGENTS.md")));
        assert_eq!(out.iter().filter(|p| *p == &sub_path).count(), 1);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_malformed_conditional_rule_glob_never_throws_and_the_rest_still_loads() {
        let dir = temp_dir();
        write(&dir, "proj/sub/AGENTS.md", "SURVIVES");
        // An absolute-looking glob must be dropped silently, never abort the scan.
        write(
            &dir,
            "proj/sub/.klaus/rules/bad.md",
            "---\npaths: \"/abs/**\"\n---\nBAD",
        );
        let out = bodies(&nested(&dir, "proj", "proj/sub/file.ts", None).await);
        assert!(out.iter().any(|x| x == "SURVIVES"));
        std::fs::remove_dir_all(&dir).ok();
    }
}
