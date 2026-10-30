//! Read-only command classifier — public entry [`is_read_only_command`],
//! wired into the decision engine's `is_read_only_subcommand` hook. Covers the
//! per-subcommand half of the analysis: COMMAND_ALLOWLIST flag parsing + the
//! READONLY_COMMAND_REGEXES fallback + the unprovable-glob / `$`-expansion /
//! UNC / git-config guards. Compound-level concerns (splitting, cd+git gate,
//! multi-cd) are handled upstream in the decision module, so this operates on
//! a single already-split subcommand.

pub mod commands;
pub mod cwd_guard;
pub mod external;
pub mod gh;
pub mod git;
pub mod sed;
pub mod types;
pub mod validate;

use std::sync::LazyLock;

use crate::permission::shell_quote::{self, ParseEntry};

use self::commands::{ANT_ONLY_COMMAND_ALLOWLIST, COMMAND_ALLOWLIST};
use self::external::EXTERNAL_READONLY_COMMANDS;
use self::types::CommandConfig;
use self::validate::{contains_vulnerable_unc_path, validate_flags, ValidateFlagsOptions};

/// Parse a command into tokens (shell lexer with `$VAR` preserved as literal).
fn try_parse_shell_command(command: &str) -> Option<Vec<ParseEntry>> {
    shell_quote::parse_with_env(command, &|name| Some(format!("${name}"))).ok()
}

/// Effective allowlist: drop `xargs` on Windows (UNC-in-file-contents
/// data→code bridge), and add the ant-only entries when USER_TYPE=ant.
fn get_command_allowlist() -> Vec<(&'static str, &'static CommandConfig)> {
    let mut allowlist: Vec<(&'static str, &'static CommandConfig)> = COMMAND_ALLOWLIST
        .iter()
        .map(|(name, config)| (*name, config))
        .collect();
    if cfg!(windows) {
        allowlist.retain(|(name, _)| *name != "xargs");
    }
    if std::env::var("USER_TYPE").as_deref() == Ok("ant") {
        for (name, config) in ANT_ONLY_COMMAND_ALLOWLIST.iter() {
            match allowlist
                .iter_mut()
                .find(|(existing, _)| *existing == *name)
            {
                Some(entry) => entry.1 = config,
                None => allowlist.push((*name, config)),
            }
        }
    }
    allowlist
}

/// Commands safe as xargs targets for auto-approval. SECURITY: only purely
/// read-only utilities with NO file-write / code-exec / network flags — when
/// xargs targets one of these, flag validation stops after the target, so the
/// target must have no dangerous flags at all.
const SAFE_TARGET_COMMANDS_FOR_XARGS: &[&str] = &["echo", "printf", "wc", "grep", "head", "tail"];

/// Validate a command against COMMAND_ALLOWLIST via declarative flag parsing.
/// Rejects operators, any `$`-bearing token (variable-expansion parser
/// differential), brace-expansion obfuscation, git ls-remote URLs, backticks,
/// grep/rg newlines, and runs the config's regex + danger callback.
pub fn is_command_safe_via_flag_parsing(command: &str) -> bool {
    let Some(entries) = try_parse_shell_command(command) else {
        return false;
    };

    // Operators (pipes/redirects) → not a simple command; compound splitting is
    // upstream. Glob entries carry their pattern as the token text.
    let mut tokens: Vec<String> = Vec::with_capacity(entries.len());
    for entry in entries {
        match entry {
            ParseEntry::Word(word) => tokens.push(word),
            ParseEntry::Glob(pattern) => tokens.push(pattern),
            ParseEntry::Op(_) | ParseEntry::Comment(_) => return false,
        }
    }

    if tokens.is_empty() {
        return false;
    }

    // Find the matching command config (longest multi-word prefix, e.g.
    // "git stash list").
    let mut command_config: Option<&'static CommandConfig> = None;
    let mut command_tokens = 0usize;
    let allowlist = get_command_allowlist();
    for (cmd_pattern, config) in allowlist {
        let cmd_tokens: Vec<&str> = cmd_pattern.split(' ').collect();
        if tokens.len() >= cmd_tokens.len() {
            let matches = cmd_tokens
                .iter()
                .enumerate()
                .all(|(k, part)| tokens[k] == *part);
            if matches {
                command_config = Some(config);
                command_tokens = cmd_tokens.len();
                break;
            }
        }
    }

    let Some(command_config) = command_config else {
        return false; // Not in allowlist
    };

    // git ls-remote: reject URL/remote/variable specs (data exfiltration).
    if tokens[0] == "git" && tokens.get(1).map(String::as_str) == Some("ls-remote") {
        for token in tokens.iter().skip(2) {
            if !token.is_empty() && !token.starts_with('-') {
                if token.contains("://") {
                    return false;
                }
                if token.contains('@') || token.contains(':') {
                    return false;
                }
                if token.contains('$') {
                    return false;
                }
            }
        }
    }

    // SECURITY: reject ANY token containing `$` (variable expansion). The lexer
    // preserves `$VAR` as literal text but bash expands it at runtime — a parser
    // differential that defeats validate_flags' leading-dash check AND callback
    // regexes. Also reject brace-expansion obfuscation (`{` with `,` or `..`).
    // Runs BEFORE validate_flags and callbacks.
    for token in tokens.iter().skip(command_tokens) {
        if token.is_empty() {
            continue;
        }
        if token.contains('$') {
            return false;
        }
        if token.contains('{') && (token.contains(',') || token.contains("..")) {
            return false;
        }
    }

    let options = ValidateFlagsOptions {
        command_name: Some(tokens[0].as_str()),
        raw_command: Some(command),
        xargs_target_commands: if tokens[0] == "xargs" {
            Some(SAFE_TARGET_COMMANDS_FOR_XARGS)
        } else {
            None
        },
    };
    if !validate_flags(&tokens, command_tokens, command_config, Some(&options)) {
        return false;
    }

    if let Some(regex) = &command_config.regex {
        if !regex.is_match(command).unwrap_or(false) {
            return false;
        }
    } else {
        if command.contains('`') {
            return false;
        }
        // Block newlines/CR in grep/rg patterns (injection).
        if (tokens[0] == "rg" || tokens[0] == "grep")
            && (command.contains('\n') || command.contains('\r'))
        {
            return false;
        }
    }

    if let Some(callback) = command_config.additional_command_is_dangerous_callback {
        if callback(command, &tokens[command_tokens..]) {
            return false;
        }
    }

    true
}

/// Regex matching safe invocations of a static command name (blocks shell
/// metacharacters).
fn make_regex_for_safe_command(command: &str) -> fancy_regex::Regex {
    // Static command names from READONLY_COMMANDS only — built once at first use
    // (no ReDoS, no untrusted input), honoring the repo's
    // no-runtime-regex-on-untrusted-input convention.
    fancy_regex::Regex::new(&format!(r"^{command}(?:\s|$)[^<>()$`|{{}}&;\n\r]*$")).unwrap()
}

// Simple read-only commands (converted to regexes via
// make_regex_for_safe_command). Adding here requires the command have NO
// file-write / code-exec / network flags.
static READONLY_COMMANDS: LazyLock<Vec<&'static str>> = LazyLock::new(|| {
    let mut commands: Vec<&'static str> = EXTERNAL_READONLY_COMMANDS.to_vec();
    commands.extend([
        // Time and date
        "cal", "uptime", // File content viewing
        "cat", "head", "tail", "wc", "stat", "strings", "hexdump", "od", "nl",
        // System info
        "id", "uname", "free", "df", "du", "locale", "groups", "nproc",
        // Path information
        "basename", "dirname", "realpath", // Text processing
        "cut", "paste", "tr", "column", "tac", "rev", "fold", "expand", "unexpand", "fmt", "comm",
        "cmp", "numfmt", "readlink", // File comparison
        "diff",     // true / false
        "true", "false", // Misc safe
        "sleep", "which", "type", "expr", "test", "getconf", "seq", "tsort", "pr",
    ]);
    commands
});

// Complex commands needing custom regexes. Prefer COMMAND_ALLOWLIST over
// adding here.
static READONLY_COMMAND_REGEXES: LazyLock<Vec<fancy_regex::Regex>> = LazyLock::new(|| {
    let mut regexes: Vec<fancy_regex::Regex> = READONLY_COMMANDS
        .iter()
        .map(|command| make_regex_for_safe_command(command))
        .collect();

    let literal_patterns = [
        // echo without command execution or variables; allow trailing 2>&1.
        r#"^echo(?:\s+(?:'[^']*'|"[^"$<>\n\r]*"|[^|;&`$(){}><#\\!"'\s]+))*(?:\s+2>&1)?\s*$"#,
        r"^claude -h$",
        r"^claude --help$",
        r"^uniq(?:\s+(?:-[a-zA-Z]+|--[a-zA-Z-]+(?:=\S+)?|-[fsw]\s+[0-9]+))*(?:\s|$)\s*$",
        r"^pwd$",
        r"^whoami$",
        // Dev-tool version checks — exact match only (e.g. `node -v --run <task>`
        // would run scripts).
        r"^node -v$",
        r"^node --version$",
        r"^python --version$",
        r"^python3 --version$",
        r"^history(?:\s+[0-9]+)?\s*$",
        r"^alias$",
        r"^arch(?:\s+(?:--help|-h))?\s*$",
        r"^ip addr$",
        r"^ifconfig(?:\s+[a-zA-Z][a-zA-Z0-9_-]*)?\s*$",
        // jq — inline filters + file args; block file-reading/module-loading flags
        // + env access + backticks.
        r#"^jq(?!\s+.*(?:-f\b|--from-file|--rawfile|--slurpfile|--run-tests|-L\b|--library-path|\benv\b|\$ENV\b))(?:\s+(?:-[a-zA-Z]+|--[a-zA-Z-]+(?:=\S+)?))*(?:\s+'[^'`]*'|\s+"[^"`]*"|\s+[^-\s'"][^\s]*)+\s*$"#,
        // cd / ls / find (path validation handled separately upstream).
        r#"^cd(?:\s+(?:'[^']*'|"[^"]*"|[^\s;|&`$(){}><#\\]+))?$"#,
        r"^ls(?:\s+[^<>()$`|{}&;\n\r]*)?$",
        r"^find(?:\s+(?:\\[()]|(?!-delete\b|-exec\b|-execdir\b|-ok\b|-okdir\b|-fprint0?\b|-fls\b|-fprintf\b)[^<>()$`|{}&;\n\r\s]|\s)+)?$",
    ];
    regexes.extend(
        literal_patterns
            .iter()
            .map(|pattern| fancy_regex::Regex::new(pattern).unwrap()),
    );
    regexes
});

/// True if the command contains expandable `$` OUTSIDE the quote contexts where
/// bash treats it as literal (`$` is literal only in single quotes). Quote-state
/// machine (with the single-quote-backslash fix). Globs are checked separately
/// by [`contains_unprovable_glob`], because most globs ARE provably read-only.
fn contains_unquoted_dollar_expansion(command: &str) -> bool {
    let chars: Vec<char> = command.chars().collect();
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut escaped = false;

    for i in 0..chars.len() {
        let current_char = chars[i];

        if escaped {
            escaped = false;
            continue;
        }

        // SECURITY: backslash escapes only OUTSIDE single quotes (in bash `\` is
        // literal in '...').
        if current_char == '\\' && !in_single_quote {
            escaped = true;
            continue;
        }

        if current_char == '\'' && !in_double_quote {
            in_single_quote = !in_single_quote;
            continue;
        }
        if current_char == '"' && !in_single_quote {
            in_double_quote = !in_double_quote;
            continue;
        }

        if in_single_quote {
            continue;
        }

        // `$` followed by a variable-name / special-parameter char (expands in
        // double quotes and unquoted).
        if current_char == '$' {
            if let Some(&next) = chars.get(i + 1) {
                if next.is_ascii_alphanumeric()
                    || matches!(next, '_' | '@' | '*' | '#' | '?' | '!' | '$' | '-')
                {
                    return true;
                }
            }
        }
    }

    false
}

/// True if the static text before a token's first glob char leaves the
/// expansion able to become a BARE FLAG, i.e. a token the string-based flag
/// checks would then misparse. A glob's expansion always keeps its static
/// prefix, so only these shapes are unprovable:
///
/// - no prefix at all (`*`, `*.txt`): can expand to `-evil-flag`;
/// - a prefix starting with `-` (`-*.txt`, `--inc*=x`): the expansion stays
///   flag-shaped, or the flag NAME itself is dynamic.
///
/// Everything else keeps a static, non-flag prefix (`dir/*.swift`) or sits
/// after an `=` as a flag VALUE (`--include=*.rs`), where expansion cannot
/// create a new flag — word splitting never re-splits an expanded glob.
fn glob_prefix_unprovable(prefix: &str) -> bool {
    !prefix.contains('=') && (prefix.is_empty() || prefix.starts_with('-'))
}

/// Outcome of the quote-aware unquoted-glob scan.
struct GlobScan {
    /// Some unquoted glob could expand into a bare flag.
    unprovable: bool,
    /// The command with every provable unquoted glob char replaced by a
    /// neutral alphanumeric, so the string-based checkers read each expanded
    /// word as the static word its prefix guarantees. Empty when `unprovable`.
    neutralized: String,
}

/// Scan unquoted globs with the same quote-state machine as the `$` check
/// (globs are literal inside both quote kinds). The per-token prefix
/// accumulates LITERAL chars (quoted content included, quote chars excluded)
/// and resets at unquoted whitespace / shell metacharacters.
fn scan_unquoted_globs(command: &str) -> GlobScan {
    let chars: Vec<char> = command.chars().collect();
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut escaped = false;
    let mut token_prefix = String::new();
    let mut token_glob_seen = false;
    let mut neutralized = String::new();

    for &current_char in &chars {
        if escaped {
            escaped = false;
            token_prefix.push(current_char);
            neutralized.push(current_char);
            continue;
        }

        // SECURITY: backslash escapes only OUTSIDE single quotes (in bash `\` is
        // literal in '...').
        if current_char == '\\' && !in_single_quote {
            escaped = true;
            neutralized.push(current_char);
            continue;
        }

        if current_char == '\'' && !in_double_quote {
            in_single_quote = !in_single_quote;
            neutralized.push(current_char);
            continue;
        }
        if current_char == '"' && !in_single_quote {
            in_double_quote = !in_double_quote;
            neutralized.push(current_char);
            continue;
        }

        if in_single_quote {
            token_prefix.push(current_char);
            neutralized.push(current_char);
            continue;
        }

        // Unquoted whitespace / metacharacters end the current token.
        if current_char.is_whitespace()
            || matches!(current_char, '|' | '&' | ';' | '<' | '>' | '(' | ')')
        {
            token_prefix.clear();
            token_glob_seen = false;
            neutralized.push(current_char);
            continue;
        }

        if in_double_quote {
            token_prefix.push(current_char);
            neutralized.push(current_char);
            continue;
        }

        if matches!(current_char, '*' | '?' | '[' | ']') {
            if !token_glob_seen {
                token_glob_seen = true;
                if glob_prefix_unprovable(&token_prefix) {
                    return GlobScan {
                        unprovable: true,
                        neutralized: String::new(),
                    };
                }
            }
            // A provable glob becomes a neutral alphanumeric: the expansion
            // keeps its static prefix, so the checkers may read that prefix as
            // the whole word.
            neutralized.push('g');
            continue;
        }

        token_prefix.push(current_char);
        neutralized.push(current_char);
    }

    GlobScan {
        unprovable: false,
        neutralized,
    }
}

// Block git -c (inline config → code exec via core.fsmonitor/diff.external/...).
static GIT_INLINE_CONFIG_PATTERN: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"\s-c[\s=]").unwrap());
// Block git --exec-path (path manipulation → code exec).
static GIT_EXEC_PATH_PATTERN: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"\s--exec-path[\s=]").unwrap());
// Block git --config-env (config injection via env).
static GIT_CONFIG_ENV_PATTERN: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"\s--config-env[\s=]").unwrap());

/// True if a single command string is read-only: strips trailing `2>&1`,
/// rejects vulnerable UNC paths + `$` expansion + unprovable globs, then runs
/// the allowlist flag parsing OR the regex fallback (with git
/// -c/--exec-path/--config-env guards) on the glob-neutralized form, where
/// every provable unquoted glob reads as its static prefix.
pub fn is_read_only_command(command: &str) -> bool {
    let mut test_command = command.trim();
    if test_command.ends_with(" 2>&1") {
        test_command = test_command[..test_command.len() - 5].trim();
    }

    if contains_vulnerable_unc_path(test_command) {
        return false;
    }
    if contains_unquoted_dollar_expansion(test_command) {
        return false;
    }
    let scan = scan_unquoted_globs(test_command);
    if scan.unprovable {
        return false;
    }
    let check_command = scan.neutralized.as_str();

    if is_command_safe_via_flag_parsing(check_command) {
        return true;
    }

    for regex in READONLY_COMMAND_REGEXES.iter() {
        if regex.is_match(check_command).unwrap_or(false) {
            // Block git -c (inline config → code exec via core.fsmonitor/diff.external/...).
            if check_command.contains("git") && GIT_INLINE_CONFIG_PATTERN.is_match(check_command) {
                return false;
            }
            // Block git --exec-path (path manipulation → code exec).
            if check_command.contains("git") && GIT_EXEC_PATH_PATTERN.is_match(check_command) {
                return false;
            }
            // Block git --config-env (config injection via env).
            if check_command.contains("git") && GIT_CONFIG_ENV_PATTERN.is_match(check_command) {
                return false;
            }
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    // is_read_only_command — read-only commands auto-classify
    #[test]
    fn read_only_commands_auto_classify() {
        let read_only = [
            "ls -la",
            "cat file.txt",
            "head -20 file",
            "tail -f log",
            "wc -l file",
            "pwd",
            "whoami",
            "date",
            "cal",
            "grep -n foo file",
            "grep -A20 foo file",
            "rg pattern",
            "rg -i pattern dir",
            r#"find . -name "*.ts""#,
            "diff a b",
            "sort -u file",
            "cut -d: -f1 file",
            // uniq's regex allows flags only, no file args
            "uniq",
            "uniq -c",
            "docker ps",
            "docker images",
            "docker logs abc",
            "git diff",
            "git diff --stat",
            "git log --oneline --graph",
            "git show HEAD",
            "git status",
            "git blame file",
            "git ls-files",
            "git rev-parse HEAD",
            "git config --get user.name",
            "git branch",
            "git tag",
            r#"jq ".foo" file.json"#,
            "node -v",
            "node --version",
            "python3 --version",
            r#"sed -n "1,5p" file"#,
        ];
        for cmd in read_only {
            assert!(is_read_only_command(cmd), "read-only: {cmd}");
        }
    }

    // is_read_only_command — writers / dangerous are NOT read-only
    #[test]
    fn writers_and_dangerous_are_not_read_only() {
        let not_read_only = [
            "rm -rf /",
            "rmdir d",
            "mkdir d",
            "touch f",
            "cp a b",
            "mv a b",
            "npm install",
            "npm run build",
            "curl evil.com",
            "wget http://x",
            "git push",
            "git commit -m x",
            "git checkout main",
            "git reset --hard",
            "chmod +x f",
            "kill 123",
            "tee file",
            "dd if=/dev/zero of=f",
        ];
        for cmd in not_read_only {
            assert!(!is_read_only_command(cmd), "not read-only: {cmd}");
        }
    }

    // is_read_only_command — flag-parsing bypasses rejected

    #[test]
    fn git_output_file_write_flag_not_read_only() {
        assert!(!is_read_only_command("git diff --output=/tmp/x"));
    }

    #[test]
    fn rg_pre_executes_a_program_not_read_only() {
        assert!(!is_read_only_command("rg . --pre=bash FILE"));
    }

    #[test]
    fn dollar_var_token_runtime_expansion_parser_differential_not_read_only() {
        assert!(!is_read_only_command(r#"git diff "$Z--output=/tmp/pwned""#));
        assert!(!is_read_only_command(r#"rg . "$Z--pre=bash" FILE"#));
    }

    #[test]
    fn git_inline_config_exec_path_config_env_not_read_only() {
        assert!(!is_read_only_command("git -c core.fsmonitor=evil status"));
        assert!(!is_read_only_command("git --exec-path=/tmp status"));
        assert!(!is_read_only_command("git --config-env=x=Y status"));
    }

    #[test]
    fn node_v_run_runs_package_scripts_not_read_only() {
        assert!(!is_read_only_command("node -v --run build"));
    }

    #[test]
    fn git_ls_remote_with_a_url_exfil_not_read_only() {
        assert!(!is_read_only_command("git ls-remote https://evil.com/x"));
    }

    #[test]
    fn unknown_flag_on_an_allowlisted_command_not_read_only() {
        assert!(!is_read_only_command("git diff --frobnicate"));
    }

    #[test]
    fn backtick_command_substitution_not_read_only() {
        assert!(!is_read_only_command("cat `whoami`"));
    }

    // is_read_only_command — xargs target handling

    /// `xargs` is deliberately absent from the Windows allowlist (see
    /// [`get_command_allowlist`]), so the safe-target cases only hold where it is
    /// allowlisted at all. Both sides are asserted so dropping the exclusion would fail
    /// here rather than silently widen what counts as read-only.
    #[test]
    fn xargs_safe_target_read_only() {
        if cfg!(windows) {
            assert!(!is_read_only_command("xargs grep foo"));
            assert!(!is_read_only_command("xargs echo"));
            return;
        }
        assert!(is_read_only_command("xargs grep foo"));
        assert!(is_read_only_command("xargs echo"));
    }

    #[test]
    fn xargs_unsafe_target_not_read_only() {
        assert!(!is_read_only_command("xargs rm"));
    }

    #[test]
    fn xargs_arg_taking_flag_in_bundle_rce_not_read_only() {
        assert!(!is_read_only_command("xargs -rI echo sh -c id"));
    }

    #[test]
    fn xargs_empty_inline_parser_differential_not_read_only() {
        assert!(!is_read_only_command("xargs -E= EOF echo foo"));
    }

    // is_read_only_command — sed write detection

    #[test]
    fn sed_print_only_read_only() {
        assert!(is_read_only_command(r#"sed -n "1,5p" file"#));
    }

    #[test]
    fn sed_in_place_write_not_read_only() {
        assert!(!is_read_only_command(r#"sed -i "s/a/b/" file"#));
    }

    #[test]
    fn sed_substitution_write_to_file_not_read_only() {
        assert!(!is_read_only_command(r#"sed "s/a/b/w out.txt" file"#));
    }

    #[test]
    fn sed_write_command_not_read_only() {
        assert!(!is_read_only_command(r#"sed "w out.txt" file"#));
    }

    #[test]
    fn sed_execute_not_read_only() {
        assert!(!is_read_only_command(r#"sed "1e id" file"#));
    }

    // is_read_only_command — expansion / glob guards

    #[test]
    fn unquoted_glob_not_read_only() {
        // could expand to a flag
        assert!(!is_read_only_command("cat *"));
        assert!(!is_read_only_command("grep -rn foo -*.txt"));
        assert!(!is_read_only_command("grep --inc*=x foo"));
        // a write flag plus a bare glob must not become provable
        assert!(!is_read_only_command("sort -o out *"));
    }

    #[test]
    fn prefixed_glob_is_read_only() {
        // the expansion keeps its static prefix, so it can never be a bare flag
        assert!(is_read_only_command(
            "grep -rn foo apps/ios/KlausMobile/KlausMobile/*.swift"
        ));
        assert!(is_read_only_command("grep -rn foo --include=*.rs crates/"));
        assert!(is_read_only_command("cat dir/*.txt"));
        assert!(is_read_only_command("ls src/*.rs"));
        assert!(is_read_only_command("head -5 logs/*.log"));
        // quoted globs stay literal
        assert!(is_read_only_command(r#"find . -name "*.ts""#));
    }

    #[test]
    fn unquoted_dollar_expansion_in_regex_fallback_command_not_read_only() {
        assert!(!is_read_only_command("uniq --skip-chars=0$_"));
    }

    #[test]
    fn trailing_stderr_redirect_stripped_before_classification() {
        assert!(is_read_only_command("git status 2>&1"));
    }
}
