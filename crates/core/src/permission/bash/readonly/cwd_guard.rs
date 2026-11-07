//! Bare/exploited-repo cwd probe — defense-in-depth for the read-only
//! auto-allow path: a PRIOR, separately permitted command can plant a
//! bare-repo layout (HEAD / objects/ / refs/ / hooks/) at the cwd root and
//! corrupt `.git/HEAD`; a later bare `git status` would then make git treat
//! cwd as the gitdir and run a planted hook. When this returns true for a cwd
//! with a git command, the read-only classifier must NOT auto-allow that git
//! command (a deny/ask/allow RULE still applies).

use std::fs;
use std::path::Path;

/// True if `cwd` looks like a bare/exploited git directory.
pub fn is_cwd_bare_git_repo(cwd: &str) -> bool {
    let git_path = Path::new(cwd).join(".git");
    if let Ok(stats) = fs::metadata(&git_path) {
        if stats.is_file() {
            // worktree/submodule — git follows the gitdir reference, not cwd.
            return false;
        }
        if stats.is_dir() {
            // SECURITY: require a regular file. A `.git/HEAD` created as a DIRECTORY
            // passes a bare existence probe but git's setup_git_directory rejects it and
            // falls back to cwd discovery.
            match fs::metadata(git_path.join("HEAD")) {
                Ok(head) if head.is_file() => {
                    // normal repo — valid .git/HEAD, git won't fall back to cwd.
                    return false;
                }
                // .git/HEAD exists but is not a regular file, or .git exists but has no
                // HEAD — fall through to the bare-repo check.
                _ => {}
            }
        }
    }
    // No .git at all — fall through to the bare-repo indicator check.

    // No valid .git/HEAD. Flag if ANY bare-repo indicator exists at the cwd root.
    // Per-indicator probing so an error on one doesn't mask another.
    let cwd = Path::new(cwd);
    if fs::metadata(cwd.join("HEAD"))
        .map(|m| m.is_file())
        .unwrap_or(false)
    {
        return true;
    }
    if fs::metadata(cwd.join("objects"))
        .map(|m| m.is_dir())
        .unwrap_or(false)
    {
        return true;
    }
    if fs::metadata(cwd.join("refs"))
        .map(|m| m.is_dir())
        .unwrap_or(false)
    {
        return true;
    }
    false
}
