//! Sensitive-file classifier for the edit gate. Writing/editing these paths yields
//! code execution (shell rc, git hooks/config) or agent/config hijack (.klaus
//! settings, .mcp.json, editor task files), so an edit targeting one forces a
//! prompt EVEN under acceptEdits. `bypassPermissions` is the one mode that skips
//! it: that mode is presented to the user as "skip all permission prompts", so an
//! engine-authored gate must not override it. All comparisons are case-folded.

use crate::fs::node_path::{
    basename_runtime, components_runtime, dirname_runtime, resolve_runtime,
};
use crate::fs::path_safety::is_within_roots;
use crate::runtime_values::{RuntimePath, RuntimeString};

/// POSIX path separator (the engine runs its path logic in POSIX semantics).
const SEP: &str = "/";

/// Basenames whose edit always prompts (code-exec / secret-exfil config).
const DANGEROUS_FILES: &[&str] = &[
    ".gitconfig",
    ".gitmodules",
    ".bashrc",
    ".bash_profile",
    ".zshrc",
    ".zprofile",
    ".profile",
    ".ripgreprc",
    ".mcp.json",
    ".klaus.json",
    // Additional guard: a .env write exfiltrates secrets.
    ".env",
];

/// Path segments whose contents are sensitive (code-exec / config hijack).
const DANGEROUS_DIRECTORIES: &[&str] = &[".git", ".vscode", ".idea", ".klaus"];

/// Agent-config subdirectories under `<cwd>/.klaus` whose edits always prompt.
const AGENT_CONFIG_SUBDIRS: [&str; 3] = ["commands", "agents", "skills"];

/// Result of classifying a path: safe, or unsafe with a reason and
/// `classifier_approvable` (inert in this engine — no auto-classifier — retained for
/// forward compatibility).
#[derive(Debug, Clone, PartialEq)]
pub enum SensitiveClassification {
    Safe,
    Unsafe {
        reason: String,
        classifier_approvable: bool,
    },
}

impl SensitiveClassification {
    /// Whether this classification is `Safe`.
    pub fn is_safe(&self) -> bool {
        matches!(self, SensitiveClassification::Safe)
    }
}

/// A leading single `www.`-style normalization is irrelevant here; we only case-fold.
fn fold(path: &RuntimePath) -> RuntimePath {
    let mut units = Vec::new();
    for point in path.as_runtime_string().code_points() {
        if let Some(scalar) = char::from_u32(point) {
            for lowercase in scalar.to_lowercase() {
                let mut encoded = [0; 2];
                units.extend_from_slice(lowercase.encode_utf16(&mut encoded));
            }
        } else {
            units.push(point as u16);
        }
    }
    RuntimePath::from(RuntimeString::from_utf16(units))
}

/// POSIX `path.basename` — the last non-empty segment split on `/` or `\`? No:
/// node's basename on POSIX splits on `/` only. Here inputs are already resolved to
/// POSIX form for the callers that need it.
fn basename(path: &RuntimePath) -> RuntimeString {
    basename_runtime(path).into_runtime_string()
}

/// Windows/edge path shapes that defeat naive matching — never auto-editable.
pub fn has_suspicious_path_pattern(path: &RuntimePath) -> bool {
    let p = path.as_runtime_string();
    // UNC / device long-path prefixes.
    if p.starts_with("\\\\") || p.starts_with("//") {
        return true;
    }
    let units = p.as_utf16();
    if units.len() >= 4
        && is_separator(units[0])
        && is_separator(units[1])
        && matches!(units[2], 0x003f | 0x002e)
        && is_separator(units[3])
    {
        return true; // \\?\  \\.\  //?/  //./
    }
    // Alternate Data Stream colon (a colon in a segment that is not the drive letter).
    let segs = split_separators(p);
    for (i, s) in segs.iter().enumerate() {
        if i == 0 && is_drive_letter(s) {
            continue; // drive letter
        }
        if s.contains(":") {
            return true;
        }
        if contains_short_name(s) {
            return true; // 8.3 short name
        }
        if (s.ends_with(".") || s.ends_with(" ")) && s != "." && s != ".." {
            return true; // trailing dot/space
        }
        if s.len_utf16() >= 3 && s.as_utf16().iter().all(|unit| *unit == 0x002e) {
            return true; // ... or more
        }
        if is_dos_device_name(s) {
            return true;
        }
    }
    false
}

fn is_separator(unit: u16) -> bool {
    matches!(unit, 0x002f | 0x005c)
}

fn split_separators(value: &RuntimeString) -> Vec<RuntimeString> {
    let units = value.as_utf16();
    let mut output = Vec::new();
    let mut start = 0;
    for index in 0..=units.len() {
        if index != units.len() && !is_separator(units[index]) {
            continue;
        }
        output.push(RuntimeString::from_utf16(units[start..index].to_vec()));
        start = index.saturating_add(1);
    }
    output
}

fn is_drive_letter(value: &RuntimeString) -> bool {
    let units = value.as_utf16();
    units.len() == 2
        && (u16::from(b'a')..=u16::from(b'z')).contains(&ascii_lower(units[0]))
        && units[1] == u16::from(b':')
}

fn contains_short_name(value: &RuntimeString) -> bool {
    value.as_utf16().windows(2).any(|pair| {
        pair[0] == u16::from(b'~') && (u16::from(b'0')..=u16::from(b'9')).contains(&pair[1])
    })
}

fn is_dos_device_name(value: &RuntimeString) -> bool {
    let folded = value
        .as_utf16()
        .iter()
        .copied()
        .map(ascii_lower)
        .collect::<Vec<_>>();
    let head_end = folded
        .iter()
        .position(|unit| *unit == 0x002e)
        .unwrap_or(folded.len());
    let head = RuntimeString::from_utf16(folded[..head_end].to_vec());
    matches!(head.len_utf16(), 3 | 4)
        && (matches!(head.len_utf16(), 3)
            && ["con", "prn", "aux", "nul"]
                .iter()
                .any(|name| &head == name)
            || matches!(head.len_utf16(), 4)
                && (head.starts_with("com") || head.starts_with("lpt"))
                && matches!(head.as_utf16()[3], 0x0031..=0x0039))
}

fn ascii_lower(unit: u16) -> u16 {
    if (u16::from(b'A')..=u16::from(b'Z')).contains(&unit) {
        unit + u16::from(b'a' - b'A')
    } else {
        unit
    }
}

fn is_agent_config_path(abs_lower: &RuntimePath, cwd: &RuntimePath) -> bool {
    let base = basename(abs_lower);
    // .klaus/settings.json | settings.local.json at any depth.
    if base == "settings.json" || base == "settings.local.json" {
        let parent = basename(&dirname_runtime(abs_lower));
        if parent == ".klaus" {
            return true;
        }
    }
    // <cwd>/.klaus/{commands,agents,skills}/**
    let cwd_lower = fold(
        &resolve_runtime(cwd, std::slice::from_ref(cwd))
            .expect("an explicit sensitive-path cwd resolves exactly"),
    );
    for sub in AGENT_CONFIG_SUBDIRS.iter() {
        let suffix = RuntimeString::from(SEP)
            .concat(&RuntimeString::from(".klaus"))
            .concat(&RuntimeString::from(SEP))
            .concat(&RuntimeString::from(*sub));
        let root = RuntimePath::from(cwd_lower.as_runtime_string().concat(&suffix));
        if is_within_roots(abs_lower, std::slice::from_ref(&root)) {
            return true;
        }
    }
    false
}

fn is_under_dangerous_directory(abs_lower: &RuntimePath) -> bool {
    let segs = components_runtime(abs_lower);
    for i in 0..segs.len() {
        let s = &segs[i];
        if !DANGEROUS_DIRECTORIES.iter().any(|name| s == name) {
            continue;
        }
        // Carve-out: .klaus/worktrees/** is a normal working tree, not config.
        if s == ".klaus" && segs.get(i + 1).is_some_and(|next| next == "worktrees") {
            continue;
        }
        return true;
    }
    false
}

/// Classify a single absolute path. `cwd` anchors the agent-config subdir check. Returns
/// `Safe` or an unsafe verdict with a reason and `classifier_approvable` (inert in
/// this engine — no auto-classifier — retained for forward compatibility).
pub fn classify_sensitive_path(path: &RuntimePath, cwd: &RuntimePath) -> SensitiveClassification {
    if has_suspicious_path_pattern(path) {
        return SensitiveClassification::Unsafe {
            reason: "suspicious-path-pattern".to_string(),
            classifier_approvable: false,
        };
    }
    let abs_lower = fold(
        &resolve_runtime(cwd, &[cwd.clone(), path.clone()])
            .expect("an explicit sensitive path resolves exactly"),
    );
    if DANGEROUS_FILES
        .iter()
        .any(|name| basename(&abs_lower) == *name)
    {
        return SensitiveClassification::Unsafe {
            reason: "sensitive-file".to_string(),
            classifier_approvable: true,
        };
    }
    if is_agent_config_path(&abs_lower, cwd) {
        return SensitiveClassification::Unsafe {
            reason: "agent-config".to_string(),
            classifier_approvable: true,
        };
    }
    if is_under_dangerous_directory(&abs_lower) {
        return SensitiveClassification::Unsafe {
            reason: "sensitive-directory".to_string(),
            classifier_approvable: true,
        };
    }
    SensitiveClassification::Safe
}

#[cfg(test)]
mod tests {
    use super::*;

    const CWD: &str = "/work";

    fn classify_sensitive_path(path: &str, cwd: &str) -> SensitiveClassification {
        super::classify_sensitive_path(&RuntimePath::from(path), &RuntimePath::from(cwd))
    }

    fn has_suspicious_path_pattern(path: &str) -> bool {
        super::has_suspicious_path_pattern(&RuntimePath::from(path))
    }

    fn resolve(paths: &[&str]) -> String {
        let mut output = std::path::PathBuf::new();
        for path in paths {
            let path = std::path::Path::new(path);
            if path.is_absolute() {
                output = path.to_path_buf();
            } else {
                output.push(path);
            }
        }
        output.to_string_lossy().into_owned()
    }

    // --- classifySensitivePath ---

    #[test]
    fn flags_dangerous_config_shell_files_by_basename() {
        for p in [
            "/work/.bashrc",
            "/work/sub/.zshrc",
            "/work/.gitconfig",
            "/work/.mcp.json",
            "/work/.klaus.json",
            "/work/.env",
        ] {
            let c = classify_sensitive_path(p, CWD);
            assert!(!c.is_safe(), "expected unsafe for {p}");
        }
    }

    #[test]
    fn flags_edits_under_dangerous_directories() {
        assert!(!classify_sensitive_path("/work/.git/config", CWD).is_safe());
        assert!(!classify_sensitive_path("/work/.vscode/tasks.json", CWD).is_safe());
        assert!(!classify_sensitive_path("/work/.idea/x.xml", CWD).is_safe());
        assert!(!classify_sensitive_path("/work/.klaus/settings.json", CWD).is_safe());
    }

    #[test]
    fn carves_out_klaus_worktrees_as_normal_working_tree() {
        assert!(classify_sensitive_path("/work/.klaus/worktrees/wt1/src/a.ts", CWD).is_safe());
    }

    #[test]
    fn flags_agent_config_subdirs_under_cwd_klaus() {
        assert!(!classify_sensitive_path("/work/.klaus/commands/foo.md", CWD).is_safe());
        assert!(!classify_sensitive_path("/work/.klaus/agents/bar.md", CWD).is_safe());
        assert!(!classify_sensitive_path("/work/.klaus/skills/baz/SKILL.md", CWD).is_safe());
    }

    #[test]
    fn flags_settings_local_json_under_any_klaus() {
        assert!(!classify_sensitive_path("/home/u/.klaus/settings.local.json", CWD).is_safe());
    }

    #[test]
    fn allows_ordinary_source_files() {
        for p in [
            "/work/src/index.ts",
            "/work/README.md",
            "/work/package.json",
        ] {
            assert!(
                classify_sensitive_path(p, CWD).is_safe(),
                "expected safe for {p}"
            );
        }
    }

    #[cfg(windows)]
    #[test]
    fn windows_parent_relative_paths_are_not_agent_config_descendants() {
        let cwd = r"C:\Users\Alice\work";
        assert!(classify_sensitive_path(r"C:\Users\Alice\work\consolidated.md", cwd).is_safe());
        assert!(
            !classify_sensitive_path(r"C:\Users\Alice\work\.klaus\agents\worker.md", cwd).is_safe()
        );
    }

    #[test]
    fn is_case_insensitive_on_the_basename() {
        assert!(!classify_sensitive_path("/work/.BASHRC", CWD).is_safe());
    }

    // --- symlinked-ancestor escape is closed (review fix) ---

    /// Compute the permission-check paths: returns the lexical absolute path
    /// plus, when it differs, the symlink-resolved real path (resolving the nearest
    /// existing ancestor and re-appending the missing tail for a not-yet-existing leaf).
    fn get_paths_for_permission_check(path: &str) -> Vec<String> {
        let abs = resolve(&[path]);
        let mut out = vec![abs.clone()];
        // Try to canonicalize the whole path first.
        if let Ok(real) = crate::fs::node_path::canonicalize_host(&abs) {
            let real = real.to_string_lossy().into_owned();
            if real != abs {
                out.push(real);
            }
            return out;
        }
        // Leaf doesn't exist yet: resolve nearest existing ancestor + re-append tail.
        if let Some(resolved) = real_parent_resolve(&abs) {
            if resolved != abs {
                out.push(resolved);
            }
        }
        out
    }

    fn real_parent_resolve(abs: &str) -> Option<String> {
        let mut tail: Vec<String> = Vec::new();
        let mut dir = abs.to_string();
        while std::path::Path::new(&dir).parent().is_some() {
            tail.insert(
                0,
                std::path::Path::new(&dir)
                    .file_name()?
                    .to_string_lossy()
                    .into_owned(),
            );
            let parent = std::path::Path::new(&dir).parent()?.to_path_buf();
            if parent == std::path::Path::new(&dir) {
                break;
            }
            dir = parent.to_string_lossy().into_owned();
            if let Ok(real_dir) = crate::fs::node_path::canonicalize_host(&dir) {
                let real_dir = real_dir.to_string_lossy().into_owned();
                let mut segs: Vec<&str> = vec![real_dir.as_str()];
                for t in &tail {
                    segs.push(t.as_str());
                }
                return Some(resolve(&segs));
            }
        }
        None
    }

    #[test]
    fn new_file_via_symlinked_ancestor_still_classifies_as_agent_config() {
        use std::fs;
        // Build a unique temp dir.
        let base = std::env::temp_dir().join(format!(
            "ae-symlink-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(base.join(".klaus").join("agents")).unwrap();
        // The whole point is a symlinked ancestor, so without the link there is nothing to
        // classify. Windows only allows creating one with the symlink privilege (elevated
        // or Developer Mode), so an OS refusal skips instead of asserting about a path that
        // was never linked.
        #[cfg(unix)]
        let linked = std::os::unix::fs::symlink(base.join(".klaus"), base.join("linkdir"));
        #[cfg(windows)]
        let linked = std::os::windows::fs::symlink_dir(base.join(".klaus"), base.join("linkdir"));
        if let Err(error) = linked {
            println!("skipping the symlinked-ancestor check: {error}");
            let _ = fs::remove_dir_all(&base);
            return;
        }

        // The real cwd for classification is the canonicalized temp dir (macOS aliases
        // /var -> /private/var etc., so use the canonical form on both sides).
        let dir_canon = fs::canonicalize(&base).unwrap();
        let dir = dir_canon.to_string_lossy().into_owned();

        let target = base
            .join("linkdir")
            .join("agents")
            .join("evil.md")
            .to_string_lossy()
            .into_owned();
        let forms = get_paths_for_permission_check(&target);
        let any_unsafe = forms
            .iter()
            .any(|f| !classify_sensitive_path(f, &dir).is_safe());
        assert!(any_unsafe);

        let _ = fs::remove_dir_all(&base);
    }

    // --- hasSuspiciousPathPattern ---

    #[test]
    fn flags_unc_device_ads_dos_dotted() {
        assert!(has_suspicious_path_pattern("\\\\server\\share\\x"));
        assert!(has_suspicious_path_pattern("//?/C:/x"));
        assert!(has_suspicious_path_pattern("/work/file.txt:stream"));
        assert!(has_suspicious_path_pattern("/work/CON"));
        assert!(has_suspicious_path_pattern(r"C:\PROGRA~1\file.txt"));
        assert!(has_suspicious_path_pattern("/work/.../x"));
    }

    #[test]
    fn passes_ordinary_paths() {
        assert!(!has_suspicious_path_pattern("/work/src/a.ts"));
        assert!(!has_suspicious_path_pattern("C:/Users/a/proj/x.ts"));
    }
}
