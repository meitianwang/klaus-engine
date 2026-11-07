//! Catastrophic-removal guard: an `rm`/`rmdir` whose operand resolves to a path
//! that destroys the machine or workspace root (`/`, the home dir, a direct
//! child of root, a Windows drive root/child, or a glob-root) must always
//! prompt — even when a user allow rule would otherwise auto-run it.

use crate::permission::bash::match_rule::strip_safe_wrappers;
use crate::permission::bash::node_path;
use regex::Regex;
use std::sync::LazyLock;

static SEP_RUN: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"[\\/]+").unwrap());
static WIN_DRIVE_ROOT: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[A-Za-z]:/?$").unwrap());
static WIN_DRIVE_CHILD: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[A-Za-z]:/[^/]+$").unwrap());

/// Collapse mixed/duplicate separators to a single forward slash.
fn normalize_separators(p: &str) -> String {
    SEP_RUN.replace_all(p, "/").into_owned()
}

/// Strip one trailing slash unless the whole path IS the root slash.
fn strip_trailing_slash(p: &str) -> String {
    if p.len() > 1 && p.ends_with('/') {
        p[..p.len() - 1].to_string()
    } else {
        p.to_string()
    }
}

/// Whether a resolved, separator-normalized path is a catastrophic removal
/// target. `home_dir` is the absolute home directory; pass "" to skip the home
/// comparison.
pub fn is_dangerous_removal_path(resolved_path: &str, home_dir: &str) -> bool {
    let s0 = normalize_separators(resolved_path);
    if s0 == "*" || s0.ends_with("/*") {
        return true;
    }
    let s = strip_trailing_slash(&s0);
    if s == "/" {
        return true;
    }
    if WIN_DRIVE_ROOT.is_match(&s) {
        return true;
    }
    if !home_dir.is_empty() {
        let h = strip_trailing_slash(&normalize_separators(home_dir));
        if s == h {
            return true;
        }
    }
    // `s` was separator-normalized above, so its parent is a POSIX question here even on
    // Windows, where the drive roots are covered by the two regexes instead.
    if node_path::posix_dirname(&s) == "/" {
        return true;
    }
    if WIN_DRIVE_CHILD.is_match(&s) {
        return true;
    }
    false
}

/// Expand a leading `~` / `~/` AND `$HOME` / `${HOME}` to `home_dir`.
fn expand_home(p: &str, home_dir: &str) -> String {
    if home_dir.is_empty() {
        return p.to_string();
    }
    if p == "~" || p == "$HOME" || p == "${HOME}" {
        return home_dir.to_string();
    }
    if let Some(rest) = p.strip_prefix("~/") {
        return format!("{home_dir}/{rest}");
    }
    if let Some(rest) = p.strip_prefix("$HOME/") {
        return format!("{home_dir}/{rest}");
    }
    if let Some(rest) = p.strip_prefix("${HOME}/") {
        return format!("{home_dir}/{rest}");
    }
    p.to_string()
}

/// Path operands of an `rm`/`rmdir` argv: drop flags, honor the POSIX `--`
/// end-of-options marker.
pub fn extract_removal_paths(args: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    let mut positional_only = false;
    for a in args {
        if positional_only {
            out.push(a.clone());
            continue;
        }
        if a == "--" {
            positional_only = true;
            continue;
        }
        if a.starts_with('-') && a != "-" {
            continue;
        }
        out.push(a.clone());
    }
    out
}

fn unquote(raw: &str) -> String {
    let mut s = raw;
    if s.starts_with('\'') || s.starts_with('"') {
        s = &s[1..];
    }
    if s.ends_with('\'') || s.ends_with('"') {
        s = &s[..s.len() - 1];
    }
    s.to_string()
}

/// If `subcommand` is an `rm`/`rmdir` targeting a catastrophic path, return that
/// path (resolved); otherwise `None`. Resolution is cwd-relative and does NOT
/// follow symlinks.
pub fn check_dangerous_removal(subcommand: &str, cwd: &str, home_dir: &str) -> Option<String> {
    let stripped = strip_safe_wrappers(subcommand.trim());
    let tokens: Vec<String> = stripped.split_whitespace().map(String::from).collect();
    // Peel a leading `xargs [flags]` prefix.
    let mut i = 0;
    if tokens.get(i).map(String::as_str) == Some("xargs") {
        i += 1;
        while i < tokens.len() && tokens[i].starts_with('-') {
            i += 1;
        }
    }
    let base = tokens.get(i).map(String::as_str);
    if base != Some("rm") && base != Some("rmdir") {
        return None;
    }
    let rest: Vec<String> = tokens[(i + 1).min(tokens.len())..].to_vec();
    for raw in extract_removal_paths(&rest) {
        let unquoted = unquote(&raw);
        if unquoted.is_empty() {
            continue;
        }
        let expanded = expand_home(&unquoted, home_dir);
        let abs = if node_path::is_absolute(&expanded) {
            node_path::resolve(&[&expanded])
        } else {
            node_path::resolve(&[cwd, &expanded])
        };
        if is_dangerous_removal_path(&abs, home_dir) {
            return Some(abs);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOME: &str = "/Users/alice";

    /// A POSIX-shaped test path as this host resolves it.
    ///
    /// [`check_dangerous_removal`] reports the *resolved* target, so on Windows `/` is the
    /// checkout's drive root and `/work/build/*` carries backslashes. Derived from the same
    /// `path.resolve` the checker uses — pinned against Node separately — rather than
    /// hardcoded, which would also assume which drive the checkout sits on.
    fn host(path: &str) -> String {
        node_path::resolve(&[path])
    }

    #[test]
    fn flags_catastrophic_absolute_targets() {
        for p in ["/", "/etc", "/usr", "/tmp", "/var", HOME] {
            assert!(is_dangerous_removal_path(p, HOME), "{p}");
        }
    }

    #[test]
    fn flags_glob_root() {
        assert!(is_dangerous_removal_path("*", HOME));
        assert!(is_dangerous_removal_path("/etc/*", HOME));
        assert!(is_dangerous_removal_path("/*", HOME));
    }

    #[test]
    fn flags_windows_drive() {
        assert!(is_dangerous_removal_path("C:/", HOME));
        assert!(is_dangerous_removal_path("C:", HOME));
        assert!(is_dangerous_removal_path("C:/Windows", HOME));
    }

    #[test]
    fn tolerates_trailing_and_duplicate_separators() {
        assert!(is_dangerous_removal_path("/etc/", HOME));
        assert!(is_dangerous_removal_path("//etc", HOME));
        assert!(is_dangerous_removal_path(&format!("{HOME}/"), HOME));
    }

    #[test]
    fn does_not_flag_nested_paths() {
        for p in [
            "/Users/alice/proj/build",
            "/etc/nginx/sites",
            "/work/dist",
            "/Users/alice/x/y",
        ] {
            assert!(!is_dangerous_removal_path(p, HOME), "{p}");
        }
    }

    #[test]
    fn catches_rm_rf_root_home_etc() {
        let home = host(HOME);
        let cwd = host("/work");
        assert_eq!(
            check_dangerous_removal("rm -rf /", &cwd, &home).as_deref(),
            Some(host("/").as_str())
        );
        assert_eq!(
            check_dangerous_removal("rm -rf ~", &cwd, &home).as_deref(),
            Some(home.as_str())
        );
        assert_eq!(
            check_dangerous_removal("rm -rf /etc", &cwd, &home).as_deref(),
            Some(host("/etc").as_str())
        );
    }

    #[test]
    fn resolves_glob_root_relative_to_cwd() {
        assert_eq!(
            check_dangerous_removal("rm build/*", &host("/work"), &host(HOME)).as_deref(),
            Some(host("/work/build/*").as_str())
        );
    }

    #[test]
    fn honors_double_dash() {
        assert_eq!(
            check_dangerous_removal("rm -rf -- /", &host("/work"), &host(HOME)).as_deref(),
            Some(host("/").as_str())
        );
    }

    #[test]
    fn strips_surrounding_quotes() {
        let cwd = host("/work");
        let home = host(HOME);
        assert_eq!(
            check_dangerous_removal("rm -rf \"/\"", &cwd, &home).as_deref(),
            Some(host("/").as_str())
        );
        assert_eq!(
            check_dangerous_removal("rm -rf '/etc'", &cwd, &home).as_deref(),
            Some(host("/etc").as_str())
        );
    }

    #[test]
    fn ignores_non_rm_and_safe_targets() {
        let cwd = host("/work");
        let home = host(HOME);
        assert_eq!(check_dangerous_removal("ls -la /etc", &cwd, &home), None);
        assert_eq!(
            check_dangerous_removal("rm -rf node_modules", &cwd, &home),
            None
        );
        assert_eq!(check_dangerous_removal("rm -rf ./build", &cwd, &home), None);
    }

    #[test]
    fn peels_xargs_prefix() {
        let cwd = host("/work");
        let home = host(HOME);
        assert_eq!(
            check_dangerous_removal("xargs rm -rf /", &cwd, &home).as_deref(),
            Some(host("/").as_str())
        );
        assert_eq!(
            check_dangerous_removal("xargs -0 rm -rf /etc", &cwd, &home).as_deref(),
            Some(host("/etc").as_str())
        );
        assert_eq!(
            check_dangerous_removal("xargs rm -rf node_modules", &cwd, &home),
            None
        );
    }

    #[test]
    fn expands_home_vars() {
        let cwd = host("/work");
        let home = host(HOME);
        for command in [
            "rm -rf $HOME",
            "rm -rf \"$HOME\"",
            "rm -rf ${HOME}",
            "rm -rf $HOME/",
        ] {
            assert_eq!(
                check_dangerous_removal(command, &cwd, &home).as_deref(),
                Some(home.as_str()),
                "{command}"
            );
        }
    }
}
