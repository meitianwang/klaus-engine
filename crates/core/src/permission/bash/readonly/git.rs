//! Read-only `git` subcommand allowlist.
//!
//! `GIT_READ_ONLY_COMMANDS` is the complete, ordered map of git subcommands
//! that the read-only classifier may auto-approve. The tables encode
//! security-critical FACTS — which git flags are read-only — so every flag,
//! every flag-argument type, and every security rationale is preserved
//! exactly. Entry order matters: longer command patterns (e.g.
//! `git remote show`) must precede their prefixes (`git remote`) because the
//! classifier matches the first pattern that prefixes the command.

use std::collections::HashMap;
use std::sync::LazyLock;

use regex::Regex;

use super::types::{ExternalCommandConfig, FlagArgType};

const NONE: FlagArgType = FlagArgType::None;
const NUMBER: FlagArgType = FlagArgType::Number;
const STRING: FlagArgType = FlagArgType::String;

/// A reusable group of flags shared by several git subcommands.
type FlagGroup = &'static [(&'static str, FlagArgType)];

const GIT_REF_SELECTION_FLAGS: FlagGroup = &[
    ("--all", NONE),
    ("--branches", NONE),
    ("--tags", NONE),
    ("--remotes", NONE),
];

const GIT_DATE_FILTER_FLAGS: FlagGroup = &[
    ("--since", STRING),
    ("--after", STRING),
    ("--until", STRING),
    ("--before", STRING),
];

const GIT_LOG_DISPLAY_FLAGS: FlagGroup = &[
    ("--oneline", NONE),
    ("--graph", NONE),
    ("--decorate", NONE),
    ("--no-decorate", NONE),
    ("--date", STRING),
    ("--relative-date", NONE),
];

const GIT_COUNT_FLAGS: FlagGroup = &[("--max-count", NUMBER), ("-n", NUMBER)];

// Stat output flags - used in git log, show, diff
const GIT_STAT_FLAGS: FlagGroup = &[
    ("--stat", NONE),
    ("--numstat", NONE),
    ("--shortstat", NONE),
    ("--name-only", NONE),
    ("--name-status", NONE),
];

// Color output flags - used in git log, show, diff
const GIT_COLOR_FLAGS: FlagGroup = &[("--color", NONE), ("--no-color", NONE)];

// Patch display flags - used in git log, show
const GIT_PATCH_FLAGS: FlagGroup = &[
    ("--patch", NONE),
    ("-p", NONE),
    ("--no-patch", NONE),
    ("--no-ext-diff", NONE),
    ("-s", NONE),
];

// Author/committer filter flags - used in git log, reflog
const GIT_AUTHOR_FILTER_FLAGS: FlagGroup = &[
    ("--author", STRING),
    ("--committer", STRING),
    ("--grep", STRING),
];

/// Merge flag groups (shared groups plus per-command extras) into one lookup
/// map. Later entries override earlier ones.
fn merge_flags(groups: &[FlagGroup]) -> HashMap<&'static str, FlagArgType> {
    let mut merged = HashMap::new();
    for group in groups {
        for (flag, arg_type) in *group {
            merged.insert(*flag, *arg_type);
        }
    }
    merged
}

/// Config with flag validation only (no extra danger callback).
fn read_only(safe_flags: HashMap<&'static str, FlagArgType>) -> ExternalCommandConfig {
    ExternalCommandConfig {
        safe_flags,
        additional_command_is_dangerous_callback: None,
        respects_double_dash: None,
    }
}

/// Config with flag validation plus an additional danger callback.
fn read_only_with_callback(
    safe_flags: HashMap<&'static str, FlagArgType>,
    callback: fn(&str, &[String]) -> bool,
) -> ExternalCommandConfig {
    ExternalCommandConfig {
        safe_flags,
        additional_command_is_dangerous_callback: Some(callback),
        respects_double_dash: None,
    }
}

// SECURITY: Block `git reflog expire` (positional subcommand) — it writes
// to .git/logs/** by expiring reflog entries. `git reflog delete` similarly
// writes. Only `git reflog` (bare = show) and `git reflog show` are safe.
// The positional-arg fallthrough in `validate_flags` would otherwise accept
// `expire` as a non-flag arg, and `--all` is in GIT_REF_SELECTION_FLAGS →
// passes.
fn reflog_is_dangerous(_raw_command: &str, args: &[String]) -> bool {
    // Block known write-capable subcommands: expire, delete, exists.
    // Allow: `show`, ref names (HEAD, refs/*, branch names).
    // The subcommand (if any) is the first positional arg. Subsequent
    // positionals after `show` or after flags are ref names (safe).
    const DANGEROUS_SUBCOMMANDS: [&str; 3] = ["expire", "delete", "exists"];
    for token in args {
        if token.is_empty() || token.starts_with('-') {
            continue;
        }
        // First non-flag positional: check if it's a dangerous subcommand.
        // If it's `show` or a ref name like `HEAD`/`refs/...`, safe.
        if DANGEROUS_SUBCOMMANDS.contains(&token.as_str()) {
            return true; // Dangerous subcommand — writes to .git/logs/**
        }
        // First positional is safe (show/HEAD/ref) — subsequent are ref args
        return false;
    }
    false // No positional = bare `git reflog` = safe (shows reflog)
}

static REMOTE_NAME_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-zA-Z0-9_-]+$").unwrap());

// Only allow optional -n, then one alphanumeric remote name
fn remote_show_is_dangerous(_raw_command: &str, args: &[String]) -> bool {
    // Filter out the known safe flag
    let positional: Vec<&String> = args.iter().filter(|a| a.as_str() != "-n").collect();
    // Must have exactly one positional arg that looks like a remote name
    if positional.len() != 1 {
        return true;
    }
    !REMOTE_NAME_PATTERN.is_match(positional[0])
}

// Only allow bare 'git remote' or 'git remote -v/--verbose'
fn remote_is_dangerous(_raw_command: &str, args: &[String]) -> bool {
    // All args must be known safe flags; no positional args allowed
    args.iter().any(|a| a != "-v" && a != "--verbose")
}

// SECURITY: Block tag creation via positional arguments. `git tag foo`
// creates .git/refs/tags/foo (41-byte file write) — NOT read-only.
// This is identical semantics to `git branch foo` (which has the same
// callback below). Without this callback, `validate_flags`'s default
// positional-arg fallthrough accepts `mytag` as a non-flag arg, and git tag
// auto-approves. While the write is constrained (path limited to
// .git/refs/tags/, content is fixed HEAD SHA), it violates the read-only
// invariant and can pollute CI/CD tag-pattern matching or make abandoned
// commits reachable via `git tag foo <commit>`.
fn tag_is_dangerous(_raw_command: &str, args: &[String]) -> bool {
    // Safe uses: `git tag` (list), `git tag -l pattern` (list filtered),
    // `git tag --contains <ref>` (list containing). A bare positional arg
    // without -l/--list is a tag name to CREATE — dangerous.
    const FLAGS_WITH_ARGS: [&str; 8] = [
        "--contains",
        "--no-contains",
        "--merged",
        "--no-merged",
        "--points-at",
        "--sort",
        "--format",
        "-n",
    ];
    let mut i = 0;
    let mut seen_list_flag = false;
    let mut seen_dash_dash = false;
    while i < args.len() {
        let token = &args[i];
        if token.is_empty() {
            i += 1;
            continue;
        }
        // `--` ends flag parsing. All subsequent tokens are positional args,
        // even if they start with `-`. `git tag -- -l` CREATES a tag named `-l`.
        if token == "--" && !seen_dash_dash {
            seen_dash_dash = true;
            i += 1;
            continue;
        }
        if !seen_dash_dash && token.starts_with('-') {
            // Check for -l/--list (exact or in a bundle). `-li` bundles -l and
            // -i — both no-arg flags. An exact comparison against `-l` misses
            // bundles like `-li`, `-il` — check individual chars for short
            // bundles.
            if token == "--list"
                || token == "-l"
                || (!token[1..].starts_with('-')
                    && token.len() > 2
                    && !token.contains('=')
                    && token[1..].contains('l'))
            {
                // Short-flag bundle like -li, -il containing 'l'
                seen_list_flag = true;
            }
            if !token.contains('=') && FLAGS_WITH_ARGS.contains(&token.as_str()) {
                i += 2;
            } else {
                i += 1;
            }
        } else {
            // Non-flag positional arg (or post-`--` positional). Safe only if
            // preceded by -l/--list (then it's a pattern, not a tag name).
            if !seen_list_flag {
                return true; // Positional arg without --list = tag creation
            }
            i += 1;
        }
    }
    false
}

// Block branch creation via positional arguments (e.g., "git branch newbranch")
// Flag validation is handled by safe_flags in the `git branch` entry
// args is tokens after "git branch"
fn branch_is_dangerous(_raw_command: &str, args: &[String]) -> bool {
    // Block branch creation: "git branch <name>" or "git branch <name> <start-point>"
    // Only safe uses are: "git branch" (list), "git branch -flags" (list with options),
    // or "git branch --contains/--merged/etc <ref>" (filtering)
    // Flags that require an argument
    const FLAGS_WITH_ARGS: [&str; 4] = [
        "--contains",
        "--no-contains",
        "--points-at",
        "--sort",
        // --abbrev intentionally absent: git does NOT consume a detached arg
        // for it (optional-attached only)
    ];
    // Flags with optional arguments (don't require, but can take one)
    const FLAGS_WITH_OPTIONAL_ARGS: [&str; 2] = ["--merged", "--no-merged"];
    let mut i = 0;
    let mut last_flag: &str = "";
    let mut seen_list_flag = false;
    let mut seen_dash_dash = false;
    while i < args.len() {
        let token = &args[i];
        if token.is_empty() {
            i += 1;
            continue;
        }
        // `--` ends flag parsing. `git branch -- -l` CREATES a branch named `-l`.
        if token == "--" && !seen_dash_dash {
            seen_dash_dash = true;
            last_flag = "";
            i += 1;
            continue;
        }
        if !seen_dash_dash && token.starts_with('-') {
            // Check for -l/--list including short-flag bundles (-li, -la, etc.)
            if token == "--list"
                || token == "-l"
                || (!token[1..].starts_with('-')
                    && token.len() > 2
                    && !token.contains('=')
                    && token[1..].contains('l'))
            {
                seen_list_flag = true;
            }
            if token.contains('=') {
                last_flag = token.split('=').next().unwrap_or("");
                i += 1;
            } else if FLAGS_WITH_ARGS.contains(&token.as_str()) {
                last_flag = token;
                i += 2;
            } else {
                last_flag = token;
                i += 1;
            }
        } else {
            // Non-flag argument (or post-`--` positional) - could be:
            // 1. A branch name (dangerous - creates a branch)
            // 2. A pattern after --list/-l (safe)
            // 3. An optional argument after --merged/--no-merged (safe)
            let last_flag_has_optional_arg = FLAGS_WITH_OPTIONAL_ARGS.contains(&last_flag);
            if !seen_list_flag && !last_flag_has_optional_arg {
                return true; // Positional arg without --list or filtering flag = branch creation
            }
            i += 1;
        }
    }
    false
}

// ---------------------------------------------------------------------------
// GIT_READ_ONLY_COMMANDS — complete map of all git subcommands
// ---------------------------------------------------------------------------

pub static GIT_READ_ONLY_COMMANDS: LazyLock<Vec<(&'static str, ExternalCommandConfig)>> =
    LazyLock::new(|| {
        vec![
            (
                "git diff",
                read_only(merge_flags(&[
                    GIT_STAT_FLAGS,
                    GIT_COLOR_FLAGS,
                    &[
                        // Display and comparison flags
                        ("--dirstat", NONE),
                        ("--summary", NONE),
                        ("--patch-with-stat", NONE),
                        ("--word-diff", NONE),
                        ("--word-diff-regex", STRING),
                        ("--color-words", NONE),
                        ("--no-renames", NONE),
                        ("--no-ext-diff", NONE),
                        ("--check", NONE),
                        ("--ws-error-highlight", STRING),
                        ("--full-index", NONE),
                        ("--binary", NONE),
                        ("--abbrev", NUMBER),
                        ("--break-rewrites", NONE),
                        ("--find-renames", NONE),
                        ("--find-copies", NONE),
                        ("--find-copies-harder", NONE),
                        ("--irreversible-delete", NONE),
                        ("--diff-algorithm", STRING),
                        ("--histogram", NONE),
                        ("--patience", NONE),
                        ("--minimal", NONE),
                        ("--ignore-space-at-eol", NONE),
                        ("--ignore-space-change", NONE),
                        ("--ignore-all-space", NONE),
                        ("--ignore-blank-lines", NONE),
                        ("--inter-hunk-context", NUMBER),
                        ("--function-context", NONE),
                        ("--exit-code", NONE),
                        ("--quiet", NONE),
                        ("--cached", NONE),
                        ("--staged", NONE),
                        ("--pickaxe-regex", NONE),
                        ("--pickaxe-all", NONE),
                        ("--no-index", NONE),
                        ("--relative", STRING),
                        // Diff filtering
                        ("--diff-filter", STRING),
                        // Short flags
                        ("-p", NONE),
                        ("-u", NONE),
                        ("-s", NONE),
                        ("-M", NONE),
                        ("-C", NONE),
                        ("-B", NONE),
                        ("-D", NONE),
                        ("-l", NONE),
                        // SECURITY: -S/-G/-O take REQUIRED string arguments
                        // (pickaxe search, pickaxe regex, orderfile). Marking
                        // them no-arg caused a parser differential with git:
                        // `git diff -S -- --output=/tmp/pwned` — the validator
                        // sees -S as no-arg → advances 1 token → breaks on `--`
                        // → --output unchecked. git sees -S requires arg →
                        // consumes `--` as the pickaxe string (standard getopt:
                        // required-arg options consume the next argv token
                        // unconditionally, BEFORE the top-level `--` check) →
                        // cursor at --output=... → parses as long option →
                        // ARBITRARY FILE WRITE.
                        // The git log config below correctly has -S/-G as
                        // 'string'.
                        ("-S", STRING),
                        ("-G", STRING),
                        ("-O", STRING),
                        ("-R", NONE),
                    ],
                ])),
            ),
            (
                "git log",
                read_only(merge_flags(&[
                    GIT_LOG_DISPLAY_FLAGS,
                    GIT_REF_SELECTION_FLAGS,
                    GIT_DATE_FILTER_FLAGS,
                    GIT_COUNT_FLAGS,
                    GIT_STAT_FLAGS,
                    GIT_COLOR_FLAGS,
                    GIT_PATCH_FLAGS,
                    GIT_AUTHOR_FILTER_FLAGS,
                    &[
                        // Additional display flags
                        ("--abbrev-commit", NONE),
                        ("--full-history", NONE),
                        ("--dense", NONE),
                        ("--sparse", NONE),
                        ("--simplify-merges", NONE),
                        ("--ancestry-path", NONE),
                        ("--source", NONE),
                        ("--first-parent", NONE),
                        ("--merges", NONE),
                        ("--no-merges", NONE),
                        ("--reverse", NONE),
                        ("--walk-reflogs", NONE),
                        ("--skip", NUMBER),
                        ("--max-age", NUMBER),
                        ("--min-age", NUMBER),
                        ("--no-min-parents", NONE),
                        ("--no-max-parents", NONE),
                        ("--follow", NONE),
                        // Commit traversal flags
                        ("--no-walk", NONE),
                        ("--left-right", NONE),
                        ("--cherry-mark", NONE),
                        ("--cherry-pick", NONE),
                        ("--boundary", NONE),
                        // Ordering flags
                        ("--topo-order", NONE),
                        ("--date-order", NONE),
                        ("--author-date-order", NONE),
                        // Format control
                        ("--pretty", STRING),
                        ("--format", STRING),
                        // Diff filtering
                        ("--diff-filter", STRING),
                        // Pickaxe search (find commits that add/remove string)
                        ("-S", STRING),
                        ("-G", STRING),
                        ("--pickaxe-regex", NONE),
                        ("--pickaxe-all", NONE),
                    ],
                ])),
            ),
            (
                "git show",
                read_only(merge_flags(&[
                    GIT_LOG_DISPLAY_FLAGS,
                    GIT_STAT_FLAGS,
                    GIT_COLOR_FLAGS,
                    GIT_PATCH_FLAGS,
                    &[
                        // Additional display flags
                        ("--abbrev-commit", NONE),
                        ("--word-diff", NONE),
                        ("--word-diff-regex", STRING),
                        ("--color-words", NONE),
                        ("--pretty", STRING),
                        ("--format", STRING),
                        ("--first-parent", NONE),
                        ("--raw", NONE),
                        // Diff filtering
                        ("--diff-filter", STRING),
                        // Short flags
                        ("-m", NONE),
                        ("--quiet", NONE),
                    ],
                ])),
            ),
            (
                "git shortlog",
                read_only(merge_flags(&[
                    GIT_REF_SELECTION_FLAGS,
                    GIT_DATE_FILTER_FLAGS,
                    &[
                        // Summary options
                        ("-s", NONE),
                        ("--summary", NONE),
                        ("-n", NONE),
                        ("--numbered", NONE),
                        ("-e", NONE),
                        ("--email", NONE),
                        ("-c", NONE),
                        ("--committer", NONE),
                        // Grouping
                        ("--group", STRING),
                        // Formatting
                        ("--format", STRING),
                        // Filtering
                        ("--no-merges", NONE),
                        ("--author", STRING),
                    ],
                ])),
            ),
            (
                "git reflog",
                read_only_with_callback(
                    merge_flags(&[
                        GIT_LOG_DISPLAY_FLAGS,
                        GIT_REF_SELECTION_FLAGS,
                        GIT_DATE_FILTER_FLAGS,
                        GIT_COUNT_FLAGS,
                        GIT_AUTHOR_FILTER_FLAGS,
                    ]),
                    reflog_is_dangerous,
                ),
            ),
            (
                "git stash list",
                read_only(merge_flags(&[
                    GIT_LOG_DISPLAY_FLAGS,
                    GIT_REF_SELECTION_FLAGS,
                    GIT_COUNT_FLAGS,
                ])),
            ),
            (
                "git ls-remote",
                read_only(merge_flags(&[&[
                    // Branch/tag filtering flags
                    ("--branches", NONE),
                    ("-b", NONE),
                    ("--tags", NONE),
                    ("-t", NONE),
                    ("--heads", NONE),
                    ("-h", NONE),
                    ("--refs", NONE),
                    // Output control flags
                    ("--quiet", NONE),
                    ("-q", NONE),
                    ("--exit-code", NONE),
                    ("--get-url", NONE),
                    ("--symref", NONE),
                    // Sorting flags
                    ("--sort", STRING),
                    // Protocol flags
                    // SECURITY: --server-option and -o are INTENTIONALLY
                    // EXCLUDED. They transmit an arbitrary attacker-controlled
                    // string to the remote git server in the protocol v2
                    // capability advertisement. This is a network WRITE
                    // primitive (sending data to remote) on what is supposed to
                    // be a read-only command. Even without command substitution
                    // (which is caught elsewhere),
                    // `--server-option="sensitive-data"` exfiltrates the value
                    // to whatever `origin` points to. The read-only path should
                    // never enable network writes.
                ]])),
            ),
            (
                "git status",
                read_only(merge_flags(&[&[
                    // Output format flags
                    ("--short", NONE),
                    ("-s", NONE),
                    ("--branch", NONE),
                    ("-b", NONE),
                    ("--porcelain", NONE),
                    ("--long", NONE),
                    ("--verbose", NONE),
                    ("-v", NONE),
                    // Untracked files handling
                    ("--untracked-files", STRING),
                    ("-u", STRING),
                    // Ignore options
                    ("--ignored", NONE),
                    ("--ignore-submodules", STRING),
                    // Column display
                    ("--column", NONE),
                    ("--no-column", NONE),
                    // Ahead/behind info
                    ("--ahead-behind", NONE),
                    ("--no-ahead-behind", NONE),
                    // Rename detection
                    ("--renames", NONE),
                    ("--no-renames", NONE),
                    ("--find-renames", STRING),
                    ("-M", STRING),
                ]])),
            ),
            (
                "git blame",
                read_only(merge_flags(&[
                    GIT_COLOR_FLAGS,
                    &[
                        // Line range
                        ("-L", STRING),
                        // Output format
                        ("--porcelain", NONE),
                        ("-p", NONE),
                        ("--line-porcelain", NONE),
                        ("--incremental", NONE),
                        ("--root", NONE),
                        ("--show-stats", NONE),
                        ("--show-name", NONE),
                        ("--show-number", NONE),
                        ("-n", NONE),
                        ("--show-email", NONE),
                        ("-e", NONE),
                        ("-f", NONE),
                        // Date formatting
                        ("--date", STRING),
                        // Ignore whitespace
                        ("-w", NONE),
                        // Ignore revisions
                        ("--ignore-rev", STRING),
                        ("--ignore-revs-file", STRING),
                        // Move/copy detection
                        ("-M", NONE),
                        ("-C", NONE),
                        ("--score-debug", NONE),
                        // Abbreviation
                        ("--abbrev", NUMBER),
                        // Other options
                        ("-s", NONE),
                        ("-l", NONE),
                        ("-t", NONE),
                    ],
                ])),
            ),
            (
                "git ls-files",
                read_only(merge_flags(&[&[
                    // File selection
                    ("--cached", NONE),
                    ("-c", NONE),
                    ("--deleted", NONE),
                    ("-d", NONE),
                    ("--modified", NONE),
                    ("-m", NONE),
                    ("--others", NONE),
                    ("-o", NONE),
                    ("--ignored", NONE),
                    ("-i", NONE),
                    ("--stage", NONE),
                    ("-s", NONE),
                    ("--killed", NONE),
                    ("-k", NONE),
                    ("--unmerged", NONE),
                    ("-u", NONE),
                    // Output format
                    ("--directory", NONE),
                    ("--no-empty-directory", NONE),
                    ("--eol", NONE),
                    ("--full-name", NONE),
                    ("--abbrev", NUMBER),
                    ("--debug", NONE),
                    ("-z", NONE),
                    ("-t", NONE),
                    ("-v", NONE),
                    ("-f", NONE),
                    // Exclude patterns
                    ("--exclude", STRING),
                    ("-x", STRING),
                    ("--exclude-from", STRING),
                    ("-X", STRING),
                    ("--exclude-per-directory", STRING),
                    ("--exclude-standard", NONE),
                    // Error handling
                    ("--error-unmatch", NONE),
                    // Recursion
                    ("--recurse-submodules", NONE),
                ]])),
            ),
            (
                "git config --get",
                read_only(merge_flags(&[&[
                    // No additional flags needed - just reading config values
                    ("--local", NONE),
                    ("--global", NONE),
                    ("--system", NONE),
                    ("--worktree", NONE),
                    ("--default", STRING),
                    ("--type", STRING),
                    ("--bool", NONE),
                    ("--int", NONE),
                    ("--bool-or-int", NONE),
                    ("--path", NONE),
                    ("--expiry-date", NONE),
                    ("-z", NONE),
                    ("--null", NONE),
                    ("--name-only", NONE),
                    ("--show-origin", NONE),
                    ("--show-scope", NONE),
                ]])),
            ),
            // NOTE: 'git remote show' must come BEFORE 'git remote' so longer
            // patterns are matched first
            (
                "git remote show",
                read_only_with_callback(merge_flags(&[&[("-n", NONE)]]), remote_show_is_dangerous),
            ),
            (
                "git remote",
                read_only_with_callback(
                    merge_flags(&[&[("-v", NONE), ("--verbose", NONE)]]),
                    remote_is_dangerous,
                ),
            ),
            // git merge-base is a read-only command for finding common ancestors
            (
                "git merge-base",
                read_only(merge_flags(&[&[
                    ("--is-ancestor", NONE), // Check if first commit is ancestor of second
                    ("--fork-point", NONE),  // Find fork point
                    ("--octopus", NONE),     // Find best common ancestors for multiple refs
                    ("--independent", NONE), // Filter independent refs
                    ("--all", NONE),         // Output all merge bases
                ]])),
            ),
            // git rev-parse is a pure read command — resolves refs to SHAs, queries repo paths
            (
                "git rev-parse",
                read_only(merge_flags(&[&[
                    // SHA resolution and verification
                    ("--verify", NONE), // Verify that exactly one argument is a valid object name
                    ("--short", STRING), // Abbreviate output (optional length via =N)
                    ("--abbrev-ref", NONE), // Symbolic name of ref
                    ("--symbolic", NONE), // Output symbolic names
                    ("--symbolic-full-name", NONE), // Full symbolic name including refs/heads/ prefix
                    // Repository path queries (all read-only)
                    ("--show-toplevel", NONE), // Absolute path of top-level directory
                    ("--show-cdup", NONE),     // Path components to traverse up to top-level
                    ("--show-prefix", NONE),   // Relative path from top-level to cwd
                    ("--git-dir", NONE),       // Path to .git directory
                    ("--git-common-dir", NONE), // Path to common directory (.git in main worktree)
                    ("--absolute-git-dir", NONE), // Absolute path to .git directory
                    ("--show-superproject-working-tree", NONE), // Superproject root (if submodule)
                    // Boolean queries
                    ("--is-inside-work-tree", NONE),
                    ("--is-inside-git-dir", NONE),
                    ("--is-bare-repository", NONE),
                    ("--is-shallow-repository", NONE),
                    ("--is-shallow-update", NONE),
                    ("--path-prefix", NONE),
                ]])),
            ),
            // git rev-list is read-only commit enumeration — lists/counts commits reachable from refs
            (
                "git rev-list",
                read_only(merge_flags(&[
                    GIT_REF_SELECTION_FLAGS,
                    GIT_DATE_FILTER_FLAGS,
                    GIT_COUNT_FLAGS,
                    GIT_AUTHOR_FILTER_FLAGS,
                    &[
                        // Counting
                        ("--count", NONE), // Output commit count instead of listing
                        // Traversal control
                        ("--reverse", NONE),
                        ("--first-parent", NONE),
                        ("--ancestry-path", NONE),
                        ("--merges", NONE),
                        ("--no-merges", NONE),
                        ("--min-parents", NUMBER),
                        ("--max-parents", NUMBER),
                        ("--no-min-parents", NONE),
                        ("--no-max-parents", NONE),
                        ("--skip", NUMBER),
                        ("--max-age", NUMBER),
                        ("--min-age", NUMBER),
                        ("--walk-reflogs", NONE),
                        // Output formatting
                        ("--oneline", NONE),
                        ("--abbrev-commit", NONE),
                        ("--pretty", STRING),
                        ("--format", STRING),
                        ("--abbrev", NUMBER),
                        ("--full-history", NONE),
                        ("--dense", NONE),
                        ("--sparse", NONE),
                        ("--source", NONE),
                        ("--graph", NONE),
                    ],
                ])),
            ),
            // git describe is read-only — describes commits relative to the most recent tag
            (
                "git describe",
                read_only(merge_flags(&[&[
                    // Tag selection
                    ("--tags", NONE),      // Consider all tags, not just annotated
                    ("--match", STRING),   // Only consider tags matching the glob pattern
                    ("--exclude", STRING), // Do not consider tags matching the glob pattern
                    // Output control
                    ("--long", NONE), // Always output long format (tag-distance-ghash)
                    ("--abbrev", NUMBER), // Abbreviate objectname to N hex digits
                    ("--always", NONE), // Show uniquely abbreviated object as fallback
                    ("--contains", NONE), // Find tag that comes after the commit
                    ("--first-match", NONE), // Prefer tags closest to the tip (stops after first match)
                    ("--exact-match", NONE), // Only output if an exact match (tag points at commit)
                    ("--candidates", NUMBER), // Limit walk before selecting best candidates
                    // Suffix/dirty markers
                    ("--dirty", NONE), // Append \"-dirty\" if working tree has modifications
                    ("--broken", NONE), // Append \"-broken\" if repository is in invalid state
                ]])),
            ),
            // git cat-file is read-only object inspection — displays type, size, or content of objects
            // NOTE: --batch (without --check) is intentionally excluded — it reads arbitrary objects
            // from stdin which could be exploited in piped commands to dump sensitive objects.
            (
                "git cat-file",
                read_only(merge_flags(&[&[
                    // Object query modes (all purely read-only)
                    ("-t", NONE), // Print type of object
                    ("-s", NONE), // Print size of object
                    ("-p", NONE), // Pretty-print object contents
                    ("-e", NONE), // Exit with zero if object exists, non-zero otherwise
                    // Batch mode — read-only check variant only
                    ("--batch-check", NONE), // For each object on stdin, print type and size (no content)
                    // Output control
                    ("--allow-undetermined-type", NONE),
                ]])),
            ),
            // git for-each-ref is read-only ref iteration — lists refs with optional formatting and filtering
            (
                "git for-each-ref",
                read_only(merge_flags(&[&[
                    // Output formatting
                    ("--format", STRING), // Format string using %(fieldname) placeholders
                    // Sorting
                    ("--sort", STRING), // Sort by key (e.g., refname, creatordate, version:refname)
                    // Limiting
                    ("--count", NUMBER), // Limit output to at most N refs
                    // Filtering
                    ("--contains", STRING), // Only list refs that contain specified commit
                    ("--no-contains", STRING), // Only list refs that do NOT contain specified commit
                    ("--merged", STRING),      // Only list refs reachable from specified commit
                    ("--no-merged", STRING),   // Only list refs NOT reachable from specified commit
                    ("--points-at", STRING),   // Only list refs pointing at specified object
                ]])),
            ),
            // git grep is read-only — searches tracked files for patterns
            (
                "git grep",
                read_only(merge_flags(&[&[
                    // Pattern matching modes
                    ("-e", STRING), // Pattern
                    ("-E", NONE),   // Extended regexp
                    ("--extended-regexp", NONE),
                    ("-G", NONE), // Basic regexp (default)
                    ("--basic-regexp", NONE),
                    ("-F", NONE), // Fixed strings
                    ("--fixed-strings", NONE),
                    ("-P", NONE), // Perl regexp
                    ("--perl-regexp", NONE),
                    // Match control
                    ("-i", NONE), // Ignore case
                    ("--ignore-case", NONE),
                    ("-v", NONE), // Invert match
                    ("--invert-match", NONE),
                    ("-w", NONE), // Word regexp
                    ("--word-regexp", NONE),
                    // Output control
                    ("-n", NONE), // Line number
                    ("--line-number", NONE),
                    ("-c", NONE), // Count
                    ("--count", NONE),
                    ("-l", NONE), // Files with matches
                    ("--files-with-matches", NONE),
                    ("-L", NONE), // Files without match
                    ("--files-without-match", NONE),
                    ("-h", NONE), // No filename
                    ("-H", NONE), // With filename
                    ("--heading", NONE),
                    ("--break", NONE),
                    ("--full-name", NONE),
                    ("--color", NONE),
                    ("--no-color", NONE),
                    ("-o", NONE), // Only matching
                    ("--only-matching", NONE),
                    // Context
                    ("-A", NUMBER), // After context
                    ("--after-context", NUMBER),
                    ("-B", NUMBER), // Before context
                    ("--before-context", NUMBER),
                    ("-C", NUMBER), // Context
                    ("--context", NUMBER),
                    // Boolean operators for multi-pattern
                    ("--and", NONE),
                    ("--or", NONE),
                    ("--not", NONE),
                    // Scope control
                    ("--max-depth", NUMBER),
                    ("--untracked", NONE),
                    ("--no-index", NONE),
                    ("--recurse-submodules", NONE),
                    ("--cached", NONE),
                    // Threads
                    ("--threads", NUMBER),
                    // Quiet
                    ("-q", NONE),
                    ("--quiet", NONE),
                ]])),
            ),
            // git stash show is read-only — displays diff of a stash entry
            (
                "git stash show",
                read_only(merge_flags(&[
                    GIT_STAT_FLAGS,
                    GIT_COLOR_FLAGS,
                    GIT_PATCH_FLAGS,
                    &[
                        // Diff options
                        ("--word-diff", NONE),
                        ("--word-diff-regex", STRING),
                        ("--diff-filter", STRING),
                        ("--abbrev", NUMBER),
                    ],
                ])),
            ),
            // git worktree list is read-only — lists linked working trees
            (
                "git worktree list",
                read_only(merge_flags(&[&[
                    ("--porcelain", NONE),
                    ("-v", NONE),
                    ("--verbose", NONE),
                    ("--expire", STRING),
                ]])),
            ),
            (
                "git tag",
                read_only_with_callback(
                    merge_flags(&[&[
                        // List mode flags
                        ("-l", NONE),
                        ("--list", NONE),
                        ("-n", NUMBER),
                        ("--contains", STRING),
                        ("--no-contains", STRING),
                        ("--merged", STRING),
                        ("--no-merged", STRING),
                        ("--sort", STRING),
                        ("--format", STRING),
                        ("--points-at", STRING),
                        ("--column", NONE),
                        ("--no-column", NONE),
                        ("-i", NONE),
                        ("--ignore-case", NONE),
                    ]]),
                    tag_is_dangerous,
                ),
            ),
            (
                "git branch",
                read_only_with_callback(
                    merge_flags(&[&[
                        // List mode flags
                        ("-l", NONE),
                        ("--list", NONE),
                        ("-a", NONE),
                        ("--all", NONE),
                        ("-r", NONE),
                        ("--remotes", NONE),
                        ("-v", NONE),
                        ("-vv", NONE),
                        ("--verbose", NONE),
                        // Display options
                        ("--color", NONE),
                        ("--no-color", NONE),
                        ("--column", NONE),
                        ("--no-column", NONE),
                        // SECURITY: --abbrev stays 'number' so validate_flags
                        // accepts --abbrev=N (attached form, safe). The
                        // DETACHED form `--abbrev N` is the bug: git treats the
                        // argument as optional-attached only — detached N
                        // becomes a POSITIONAL branch name, creating
                        // .git/refs/heads/N. validate_flags with 'number'
                        // consumes N, but the CALLBACK catches it: --abbrev is
                        // NOT in the callback's required-arg flag set
                        // (removed), so the callback sees N as a positional
                        // without list flag → dangerous. Two-layer defense:
                        // validate_flags accepts both forms, callback blocks
                        // detached.
                        ("--abbrev", NUMBER),
                        ("--no-abbrev", NONE),
                        // Filtering - these take commit/ref arguments
                        ("--contains", STRING),
                        ("--no-contains", STRING),
                        ("--merged", NONE), // Optional commit argument - handled in callback
                        ("--no-merged", NONE), // Optional commit argument - handled in callback
                        ("--points-at", STRING),
                        // Sorting
                        ("--sort", STRING),
                        // Note: --format is intentionally excluded as it could pose security risks
                        // Show current
                        ("--show-current", NONE),
                        ("-i", NONE),
                        ("--ignore-case", NONE),
                    ]]),
                    branch_is_dangerous,
                ),
            ),
        ]
    });

#[cfg(test)]
mod tests {
    use super::*;

    fn config(name: &str) -> &'static ExternalCommandConfig {
        GIT_READ_ONLY_COMMANDS
            .iter()
            .find(|(pattern, _)| *pattern == name)
            .map(|(_, config)| config)
            .unwrap_or_else(|| panic!("missing config for {name}"))
    }

    fn dangerous(name: &str, args: &[&str]) -> bool {
        let callback = config(name)
            .additional_command_is_dangerous_callback
            .unwrap_or_else(|| panic!("no danger callback for {name}"));
        let args: Vec<String> = args.iter().map(|a| a.to_string()).collect();
        callback(name, &args)
    }

    fn flag_type(name: &str, flag: &str) -> FlagArgType {
        *config(name)
            .safe_flags
            .get(flag)
            .unwrap_or_else(|| panic!("missing flag {flag} for {name}"))
    }

    // -- table shape ---------------------------------------------------------

    #[test]
    fn command_patterns_are_complete_and_ordered() {
        let names: Vec<&str> = GIT_READ_ONLY_COMMANDS
            .iter()
            .map(|(pattern, _)| *pattern)
            .collect();
        assert_eq!(
            names,
            vec![
                "git diff",
                "git log",
                "git show",
                "git shortlog",
                "git reflog",
                "git stash list",
                "git ls-remote",
                "git status",
                "git blame",
                "git ls-files",
                "git config --get",
                "git remote show",
                "git remote",
                "git merge-base",
                "git rev-parse",
                "git rev-list",
                "git describe",
                "git cat-file",
                "git for-each-ref",
                "git grep",
                "git stash show",
                "git worktree list",
                "git tag",
                "git branch",
            ]
        );
    }

    #[test]
    fn remote_show_precedes_remote() {
        // Longest patterns first: prefix matching would otherwise route
        // `git remote show origin` to the stricter bare-`git remote` config.
        let position = |name: &str| {
            GIT_READ_ONLY_COMMANDS
                .iter()
                .position(|(pattern, _)| *pattern == name)
                .unwrap()
        };
        assert!(position("git remote show") < position("git remote"));
    }

    #[test]
    fn only_expected_entries_have_danger_callbacks() {
        let with_callbacks: Vec<&str> = GIT_READ_ONLY_COMMANDS
            .iter()
            .filter(|(_, config)| config.additional_command_is_dangerous_callback.is_some())
            .map(|(pattern, _)| *pattern)
            .collect();
        assert_eq!(
            with_callbacks,
            vec![
                "git reflog",
                "git remote show",
                "git remote",
                "git tag",
                "git branch",
            ]
        );
    }

    #[test]
    fn no_entry_overrides_double_dash_handling() {
        for (pattern, config) in GIT_READ_ONLY_COMMANDS.iter() {
            assert!(
                config.respects_double_dash.is_none(),
                "{pattern} should not set respects_double_dash"
            );
        }
    }

    // -- flag table facts ----------------------------------------------------

    #[test]
    fn diff_pickaxe_flags_require_string_arguments() {
        // -S/-G/-O must consume their argument, or `git diff -S -- --output=x`
        // becomes a parser differential with git's own getopt handling.
        assert!(matches!(flag_type("git diff", "-S"), FlagArgType::String));
        assert!(matches!(flag_type("git diff", "-G"), FlagArgType::String));
        assert!(matches!(flag_type("git diff", "-O"), FlagArgType::String));
        assert!(matches!(flag_type("git diff", "-R"), FlagArgType::None));
        assert!(matches!(flag_type("git log", "-S"), FlagArgType::String));
        assert!(matches!(flag_type("git log", "-G"), FlagArgType::String));
    }

    #[test]
    fn log_merges_shared_flag_groups() {
        assert!(matches!(
            flag_type("git log", "--oneline"),
            FlagArgType::None
        ));
        assert!(matches!(flag_type("git log", "--all"), FlagArgType::None));
        assert!(matches!(
            flag_type("git log", "--since"),
            FlagArgType::String
        ));
        assert!(matches!(
            flag_type("git log", "--max-count"),
            FlagArgType::Number
        ));
        assert!(matches!(flag_type("git log", "-n"), FlagArgType::Number));
        assert!(matches!(flag_type("git log", "--stat"), FlagArgType::None));
        assert!(matches!(flag_type("git log", "--color"), FlagArgType::None));
        assert!(matches!(flag_type("git log", "-p"), FlagArgType::None));
        assert!(matches!(
            flag_type("git log", "--author"),
            FlagArgType::String
        ));
    }

    #[test]
    fn shortlog_uses_no_arg_n_flag() {
        // `git shortlog -n` means "numbered", unlike the count-taking -n in log.
        assert!(matches!(flag_type("git shortlog", "-n"), FlagArgType::None));
    }

    #[test]
    fn ls_remote_excludes_server_option() {
        // --server-option/-o would be a network write primitive (data sent to
        // the remote server) on a read-only command.
        let flags = &config("git ls-remote").safe_flags;
        assert!(!flags.contains_key("--server-option"));
        assert!(!flags.contains_key("-o"));
        assert!(matches!(
            flag_type("git ls-remote", "--sort"),
            FlagArgType::String
        ));
    }

    #[test]
    fn cat_file_allows_batch_check_but_not_batch() {
        let flags = &config("git cat-file").safe_flags;
        assert!(flags.contains_key("--batch-check"));
        assert!(!flags.contains_key("--batch"));
    }

    #[test]
    fn branch_excludes_format_and_keeps_abbrev_number() {
        let flags = &config("git branch").safe_flags;
        assert!(!flags.contains_key("--format"));
        assert!(matches!(
            flag_type("git branch", "--abbrev"),
            FlagArgType::Number
        ));
        // --merged/--no-merged take an OPTIONAL argument, handled in the callback.
        assert!(matches!(
            flag_type("git branch", "--merged"),
            FlagArgType::None
        ));
        assert!(matches!(
            flag_type("git branch", "--no-merged"),
            FlagArgType::None
        ));
    }

    // -- git reflog callback -------------------------------------------------

    #[test]
    fn reflog_bare_is_safe() {
        assert!(!dangerous("git reflog", &[]));
    }

    #[test]
    fn reflog_write_subcommands_are_dangerous() {
        assert!(dangerous("git reflog", &["expire", "--all"]));
        assert!(dangerous("git reflog", &["delete", "HEAD@{0}"]));
        assert!(dangerous("git reflog", &["exists", "refs/heads/main"]));
    }

    #[test]
    fn reflog_show_and_refs_are_safe() {
        assert!(!dangerous("git reflog", &["show"]));
        assert!(!dangerous("git reflog", &["HEAD"]));
        assert!(!dangerous("git reflog", &["refs/heads/main"]));
        // Only the FIRST positional is the subcommand; later tokens are refs.
        assert!(!dangerous("git reflog", &["show", "expire"]));
    }

    #[test]
    fn reflog_skips_flags_and_empty_tokens_before_subcommand() {
        assert!(!dangerous("git reflog", &["--all"]));
        assert!(dangerous("git reflog", &["--all", "expire"]));
        assert!(dangerous("git reflog", &["", "expire"]));
    }

    // -- git remote show callback --------------------------------------------

    #[test]
    fn remote_show_single_valid_name_is_safe() {
        assert!(!dangerous("git remote show", &["origin"]));
        assert!(!dangerous("git remote show", &["-n", "origin"]));
        assert!(!dangerous("git remote show", &["my-remote_1"]));
    }

    #[test]
    fn remote_show_requires_exactly_one_positional() {
        assert!(dangerous("git remote show", &[]));
        assert!(dangerous("git remote show", &["-n"]));
        assert!(dangerous("git remote show", &["origin", "upstream"]));
    }

    #[test]
    fn remote_show_rejects_non_alphanumeric_names() {
        assert!(dangerous("git remote show", &["../evil"]));
        assert!(dangerous("git remote show", &["origin;rm"]));
        assert!(dangerous("git remote show", &["or igin"]));
    }

    // -- git remote callback ---------------------------------------------------

    #[test]
    fn remote_bare_and_verbose_are_safe() {
        assert!(!dangerous("git remote", &[]));
        assert!(!dangerous("git remote", &["-v"]));
        assert!(!dangerous("git remote", &["--verbose"]));
        assert!(!dangerous("git remote", &["-v", "--verbose"]));
    }

    #[test]
    fn remote_any_other_arg_is_dangerous() {
        assert!(dangerous("git remote", &["add", "evil", "https://x"]));
        assert!(dangerous("git remote", &["-v", "prune"]));
        assert!(dangerous("git remote", &["show"]));
    }

    // -- git tag callback ------------------------------------------------------

    #[test]
    fn tag_bare_and_list_forms_are_safe() {
        assert!(!dangerous("git tag", &[]));
        assert!(!dangerous("git tag", &["-l"]));
        assert!(!dangerous("git tag", &["-l", "v*"]));
        assert!(!dangerous("git tag", &["--list", "v*"]));
    }

    #[test]
    fn tag_positional_without_list_is_creation() {
        assert!(dangerous("git tag", &["v1.0"]));
        assert!(dangerous("git tag", &["v1.0", "abc1234"]));
    }

    #[test]
    fn tag_filter_flags_consume_their_argument() {
        assert!(!dangerous("git tag", &["--contains", "HEAD"]));
        assert!(!dangerous("git tag", &["--merged", "main"]));
        assert!(!dangerous("git tag", &["--sort", "refname"]));
        assert!(!dangerous("git tag", &["-n", "5"]));
        // Flag at end of args: the consumed slot is simply past the end.
        assert!(!dangerous("git tag", &["--contains"]));
    }

    #[test]
    fn tag_attached_form_consumes_no_extra_token() {
        assert!(!dangerous("git tag", &["--sort=refname"]));
        // The token after an attached-form flag is a positional → creation.
        assert!(dangerous("git tag", &["--sort=refname", "v1.0"]));
    }

    #[test]
    fn tag_double_dash_positional_is_creation() {
        // `git tag -- -l` CREATES a tag literally named `-l`.
        assert!(dangerous("git tag", &["--", "-l"]));
        // But with a list flag first, post-`--` tokens are patterns.
        assert!(!dangerous("git tag", &["-l", "--", "-x"]));
    }

    #[test]
    fn tag_short_bundle_containing_l_lists() {
        assert!(!dangerous("git tag", &["-li", "v*"]));
        assert!(!dangerous("git tag", &["-il", "v*"]));
        // A bundle without `l` does not enable list mode.
        assert!(dangerous("git tag", &["-i", "v*"]));
    }

    // -- git branch callback -----------------------------------------------------

    #[test]
    fn branch_bare_and_flag_only_forms_are_safe() {
        assert!(!dangerous("git branch", &[]));
        assert!(!dangerous("git branch", &["-a"]));
        assert!(!dangerous("git branch", &["-vv"]));
        assert!(!dangerous("git branch", &["--show-current"]));
    }

    #[test]
    fn branch_positional_is_creation() {
        assert!(dangerous("git branch", &["newbranch"]));
        assert!(dangerous("git branch", &["newbranch", "main"]));
    }

    #[test]
    fn branch_list_pattern_is_safe() {
        assert!(!dangerous("git branch", &["--list", "feat*"]));
        assert!(!dangerous("git branch", &["-l", "feat*"]));
        // Short-flag bundle containing `l` enables list mode.
        assert!(!dangerous("git branch", &["-la", "feat*"]));
    }

    #[test]
    fn branch_filter_flags_consume_their_argument() {
        assert!(!dangerous("git branch", &["--contains", "abc123"]));
        assert!(!dangerous("git branch", &["--points-at", "HEAD"]));
        assert!(!dangerous("git branch", &["--sort", "refname"]));
    }

    #[test]
    fn branch_merged_takes_optional_argument() {
        assert!(!dangerous("git branch", &["--merged"]));
        assert!(!dangerous("git branch", &["--merged", "main"]));
        assert!(!dangerous("git branch", &["--no-merged", "main"]));
        // The optional-arg flag stays "last seen" — further positionals still pass.
        assert!(!dangerous("git branch", &["--merged", "main", "extra"]));
    }

    #[test]
    fn branch_detached_abbrev_argument_is_creation() {
        // git treats --abbrev's argument as attached-only; a detached N is a
        // positional branch name → creation. The attached form stays safe.
        assert!(dangerous("git branch", &["--abbrev", "10"]));
        assert!(!dangerous("git branch", &["--abbrev=10"]));
    }

    #[test]
    fn branch_double_dash_resets_optional_flag_state() {
        // `--` clears the last-flag state, so a following positional is a
        // creation even right after --merged.
        assert!(dangerous("git branch", &["--merged", "--", "x"]));
        assert!(dangerous("git branch", &["--", "-l"]));
    }
}
