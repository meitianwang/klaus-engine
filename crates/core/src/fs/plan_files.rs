//! Plan-mode plan files.
//!
//! The plan document is named by the model, not minted randomly: `create_plan`
//! carries a short `name`, [`clean_plan_name`] projects it onto a filesystem-safe
//! base name, and [`plan_file_path`] joins it under the session's plans
//! directory. The model never has to learn the resulting path — the engine builds
//! the file itself from the tool's own arguments, which is why a plan can be
//! submitted inline in one call.
//!
//! Two invariants matter here:
//!
//! - The name is bound lazily. Until `create_plan` has run there is no document
//!   identity, so [`is_session_plan_file`] matches nothing and plan mode stays
//!   fully read-only.
//! - Permission checks must never NAME a document. Naming is how a write gets
//!   exempted, so evaluating one must not be able to create the identity that
//!   would then exempt it. Callers that only check permissions pass an already
//!   bound name through, or none at all.
//!
//! Layout: `<plans_dir>/<name>.md` for the main thread and
//! `<plans_dir>/<name>-agent-<id>.md` for a planning sub-agent.

use crate::fs::paths::{config_home, find_project_root, sanitize_path_component};
use crate::permission::bash::node_path::{basename, dirname, resolve, SEPARATOR};

/// Rollout event that pins a session's bound plan-file name for resume.
///
/// Legacy sessions on disk still carry random slug names, so the RESUME reader
/// for this event must survive even though nothing mints one any more: without
/// it a reopened old session would resolve a fresh default name, lose the write
/// exemption for the document it was actually editing, and silently start a new
/// empty file.
pub const PLAN_SLUG_ROLLOUT_EVENT: &str = "plan_slug";

/// Fallback base name when the model's `name` cleans to nothing.
pub const DEFAULT_PLAN_NAME: &str = "plan";
/// Upper bound on a cleaned plan-file base name, so an over-long `name` can never
/// produce a path the filesystem rejects.
const PLAN_NAME_MAX_LEN: usize = 60;

/// Project a model-supplied plan `name` onto a filesystem-safe base name:
/// lowercased, runs of non-alphanumerics collapsed to single dashes, capped, and
/// trimmed of outer dashes. Falls back to [`DEFAULT_PLAN_NAME`] when nothing
/// survives, so the result is always usable as a file name.
///
/// The contract: a short human-readable name becomes a safe base name.
pub fn clean_plan_name(name: &str) -> String {
    let mut dashed = String::with_capacity(name.len());
    let mut last_dash = false;
    for ch in name.trim().to_lowercase().chars() {
        if ch.is_ascii_alphanumeric() {
            dashed.push(ch);
            last_dash = false;
        } else if !last_dash {
            dashed.push('-');
            last_dash = true;
        }
    }
    let capped: String = dashed.chars().take(PLAN_NAME_MAX_LEN).collect();
    let cleaned = capped.trim_matches('-').to_string();
    if cleaned.is_empty() {
        DEFAULT_PLAN_NAME.to_string()
    } else {
        cleaned
    }
}

/// Resolve the plans directory. A configured `plansDirectory` is resolved
/// against the session's project root and honored only when it stays inside
/// that root; anything else falls back to `<config_home>/plans`.
pub fn plans_directory(configured: Option<&str>, cwd: &str) -> String {
    if let Some(dir) = configured {
        if !dir.is_empty() {
            let root = find_project_root(cwd);
            let abs = resolve(&[&root, dir]);
            // The separator has to be the host's: with a hardcoded `/` a legitimate
            // Windows plans directory never looked contained, so every project fell back
            // to the config home and lost its configured location.
            if abs == root || abs.starts_with(&format!("{root}{SEPARATOR}")) {
                return abs;
            }
        }
    }
    resolve(&[&config_home(), "plans"])
}

/// The session's main plan file: `<plans_dir>/<name>.md`, where `<name>` is
/// already cleaned by [`clean_plan_name`]. Callers pass the cleaned name so the
/// base name can never escape the plans directory.
pub fn plan_file_path(plans_dir: &str, name: &str) -> String {
    resolve(&[plans_dir, &format!("{name}.md")])
}

/// A planning sub-agent's plan file: `<plans_dir>/<name>-agent-<id>.md`. The
/// agent-id suffix is what keeps parallel planning sub-agents from clobbering
/// one document; it must never be dropped.
pub fn agent_plan_file_path(plans_dir: &str, name: &str, agent_id: &str) -> String {
    let agent = sanitize_path_component(agent_id);
    resolve(&[plans_dir, &format!("{name}-agent-{agent}.md")])
}

/// Whether `path` is one of THIS session's plan files — it must sit directly in
/// the plans directory and be named `<name>.md` or `<name>-agent-*.md`. This is
/// the predicate behind plan mode's write exemption. It must be fed an
/// already-bound name (never a freshly-derived one): naming a document is how a
/// write gets exempted, so a permission check that could name one on demand would
/// exempt the very write it is evaluating.
pub fn is_session_plan_file(path: &str, plans_dir: &str, name: &str) -> bool {
    let normalized = resolve(&[path]);
    if dirname(&normalized) != resolve(&[plans_dir]) {
        return false;
    }
    // Host basename, not a `/` split: on Windows the split returned the whole path, so
    // this predicate answered false for every plan file and plan mode's write exemption
    // never applied — the session could not write its own plan document.
    let base = basename(&normalized);
    if base.is_empty() {
        return false;
    }
    base == format!("{name}.md")
        || (base.starts_with(&format!("{name}-agent-")) && base.ends_with(".md"))
}

/// Read the plan document, or `None` when it does not exist / is unreadable.
pub fn read_plan_file(path: &str) -> Option<String> {
    std::fs::read_to_string(path).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_plan_name_lowercases_and_collapses_separators() {
        assert_eq!(
            clean_plan_name("Align Sidebar With Codex"),
            "align-sidebar-with-codex"
        );
        assert_eq!(clean_plan_name("  spaced   out  "), "spaced-out");
        assert_eq!(clean_plan_name("plan: a/b \\ c"), "plan-a-b-c");
    }

    #[test]
    fn clean_plan_name_caps_length_and_trims_dashes() {
        let long = "word ".repeat(40);
        let cleaned = clean_plan_name(&long);
        assert!(
            cleaned.chars().count() <= PLAN_NAME_MAX_LEN,
            "cleaned: {cleaned}"
        );
        assert!(
            !cleaned.starts_with('-') && !cleaned.ends_with('-'),
            "{cleaned}"
        );
    }

    #[test]
    fn clean_plan_name_falls_back_when_nothing_survives() {
        // Punctuation-only, empty, and non-ASCII names all clean to nothing, so the
        // default must carry the file name — a plan can never be unwritable because
        // of the name the model chose.
        assert_eq!(clean_plan_name(""), DEFAULT_PLAN_NAME);
        assert_eq!(clean_plan_name("---"), DEFAULT_PLAN_NAME);
        assert_eq!(clean_plan_name("!!! ???"), DEFAULT_PLAN_NAME);
        assert_eq!(clean_plan_name("排队消息对齐"), DEFAULT_PLAN_NAME);
    }

    #[test]
    fn clean_plan_name_never_escapes_the_plans_directory() {
        for name in ["../../etc/passwd", "/abs/path", "a\\..\\b", "...."] {
            let cleaned = clean_plan_name(name);
            assert!(!cleaned.contains('/'), "{name} -> {cleaned}");
            assert!(!cleaned.contains('\\'), "{name} -> {cleaned}");
        }
    }

    #[test]
    fn plans_directory_defaults_to_config_home() {
        let dir = plans_directory(None, "/tmp");
        assert_eq!(dir, resolve(&[&config_home(), "plans"]));
    }

    #[test]
    fn plans_directory_rejects_escape_outside_project_root() {
        let root = std::env::temp_dir().join(format!("klaus-plan-files-{}", std::process::id()));
        crate::git::git::seed_git_dir(&root.join(".git")).unwrap();
        let cwd = root.to_string_lossy().into_owned();
        let inside = plans_directory(Some(".klaus/specs"), &cwd);
        assert_eq!(inside, resolve(&[&cwd, ".klaus/specs"]));
        let outside = plans_directory(Some("../elsewhere"), &cwd);
        assert_eq!(outside, resolve(&[&config_home(), "plans"]));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn session_plan_file_predicate_matches_named_and_agent_files() {
        let dir = "/home/u/.klaus/plans";
        let name = "align-sidebar-with-codex";
        assert!(is_session_plan_file(
            "/home/u/.klaus/plans/align-sidebar-with-codex.md",
            dir,
            name
        ));
        assert!(is_session_plan_file(
            "/home/u/.klaus/plans/align-sidebar-with-codex-agent-a1.md",
            dir,
            name
        ));
        // Another session's document, a nested path, and a non-markdown sibling all
        // stay outside the exemption.
        assert!(!is_session_plan_file(
            "/home/u/.klaus/plans/some-other-plan.md",
            dir,
            name
        ));
        assert!(!is_session_plan_file(
            "/home/u/plans/align-sidebar-with-codex.md",
            dir,
            name
        ));
        assert!(!is_session_plan_file(
            "/home/u/.klaus/plans/sub/align-sidebar-with-codex.md",
            dir,
            name
        ));
        assert!(!is_session_plan_file(
            "/home/u/.klaus/plans/align-sidebar-with-codex.txt",
            dir,
            name
        ));
    }

    #[test]
    fn agent_plan_file_path_sanitizes_agent_id() {
        let p = agent_plan_file_path("/p", "spec", "a/../b");
        // The plans dir and the file are joined with `path.resolve`, so the separator is
        // the host's; only the layout is platform-independent.
        assert_eq!(p, resolve(&["/p", "spec-agent-a----b.md"]));
    }

    #[test]
    fn plan_file_path_joins_the_cleaned_name_under_the_plans_dir() {
        let name = clean_plan_name("Queue Bar Alignment");
        assert_eq!(
            plan_file_path("/proj/.klaus/specs", &name),
            resolve(&["/proj/.klaus/specs", "queue-bar-alignment.md"])
        );
    }
}
