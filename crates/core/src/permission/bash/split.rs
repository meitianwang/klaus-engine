//! Legacy Bash command splitter. Splits a
//! compound shell command into its individual subcommands so the permission
//! engine can evaluate each one independently — closing the bypass where a
//! whole-string prefix match admits `git status && rm -rf /`.
//!
//! Security-relevant behaviors (each guards a specific
//! bypass): salted placeholders for quotes/newlines/escaped-parens before the
//! shell lexer; line-continuation join with ODD/EVEN backslash parity; adjacent
//! string / glob collapse; newline → subcommand boundary; redirection stripping
//! with a static-target guard; FAIL-CLOSED single-unit fallback on any parse
//! failure.

use crate::permission::shell_quote::{parse_with_env, ParseEntry};
use regex::Regex;
use std::sync::LazyLock;

struct Placeholders {
    single_quote: String,
    double_quote: String,
    new_line: String,
    escaped_open_paren: String,
    escaped_close_paren: String,
}

fn generate_placeholders() -> Placeholders {
    let salt: [u8; 8] = rand::random();
    let salt: String = salt.iter().map(|b| format!("{b:02x}")).collect();
    Placeholders {
        single_quote: format!("__SINGLE_QUOTE_{salt}__"),
        double_quote: format!("__DOUBLE_QUOTE_{salt}__"),
        new_line: format!("__NEW_LINE_{salt}__"),
        escaped_open_paren: format!("__ESCAPED_OPEN_PAREN_{salt}__"),
        escaped_close_paren: format!("__ESCAPED_CLOSE_PAREN_{salt}__"),
    }
}

fn is_fd(s: &str) -> bool {
    matches!(s, "0" | "1" | "2")
}
fn is_fd_char(c: char) -> bool {
    matches!(c, '0' | '1' | '2')
}

const COMMAND_LIST_SEPARATORS: &[&str] = &["&&", "||", ";", ";;", "|"];
const ALL_SUPPORTED_CONTROL_OPERATORS: &[&str] = &["&&", "||", ";", ";;", "|", ">&", ">", ">>"];

/// The shell lexer with variable preservation; `None` on a parse throw.
fn try_parse(command: &str) -> Option<Vec<ParseEntry>> {
    parse_with_env(command, &|name| Some(format!("${name}"))).ok()
}

static CONTINUATION: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\\+\n").unwrap());

/// Join `\`-continuations with ODD/EVEN backslash parity.
fn join_continuations(command: &str) -> String {
    CONTINUATION
        .replace_all(command, |caps: &regex::Captures| {
            let m = &caps[0];
            let backslash_count = m.chars().count() - 1; // -1 for the newline
            if backslash_count % 2 == 1 {
                "\\".repeat(backslash_count - 1)
            } else {
                m.to_string()
            }
        })
        .into_owned()
}

/// True only if `target` is a single static shell word safe to strip from a
/// redirect (no whitespace/quote/expansion/glob/FD).
fn is_static_redirect_target(target: &str) -> bool {
    if target
        .chars()
        .any(|c| c.is_whitespace() || c == '\'' || c == '"')
    {
        return false;
    }
    if target.is_empty() {
        return false;
    }
    if target.starts_with('#') {
        return false;
    }
    !target.starts_with('!')
        && !target.starts_with('=')
        && !target.contains('$')
        && !target.contains('`')
        && !target.contains('*')
        && !target.contains('?')
        && !target.contains('[')
        && !target.contains('{')
        && !target.contains('~')
        && !target.contains('(')
        && !target.contains('<')
        && !target.starts_with('&')
}

/// An internal node in the collapse pass: a merged string, an op, a glob, or a
/// comment. Represents the intermediate list, where a `None` slot marks a subcommand boundary.
enum Part {
    Str(String),
    Op(String),
    Glob(String),
    Comment(String),
}

/// Returns the parts of a compound command where operators survive as their op
/// string and boundaries are dropped after preventing cross-boundary merges.
fn split_command_with_operators(command: &str) -> Vec<String> {
    let placeholders = generate_placeholders();
    let command_original_joined = join_continuations(command);
    let command_with_continuations_joined = join_continuations(command);

    let massaged = command_with_continuations_joined
        .replace('"', &format!("\"{}", placeholders.double_quote))
        .replace('\'', &format!("'{}", placeholders.single_quote))
        .replace('\n', &format!("\n{}\n", placeholders.new_line))
        .replace("\\(", &placeholders.escaped_open_paren)
        .replace("\\)", &placeholders.escaped_close_paren);

    let parsed = match try_parse(&massaged) {
        // FAIL-CLOSED: parse threw → treat the joined original as ONE subcommand.
        None => return vec![command_original_joined],
        Some(p) => p,
    };
    if parsed.is_empty() {
        return Vec::new();
    }

    // 1) Collapse adjacent strings and globs; NEW_LINE → boundary (`None`).
    let mut parts: Vec<Option<Part>> = Vec::new();
    for entry in parsed {
        match entry {
            ParseEntry::Word(s) => {
                if s == placeholders.new_line {
                    parts.push(None);
                    continue;
                }
                if let Some(Some(Part::Str(last))) = parts.last_mut() {
                    *last = format!("{last} {s}");
                    continue;
                }
                parts.push(Some(Part::Str(s)));
            }
            ParseEntry::Glob(pattern) => {
                if let Some(Some(Part::Str(last))) = parts.last_mut() {
                    *last = format!("{last} {pattern}");
                    continue;
                }
                parts.push(Some(Part::Glob(pattern)));
            }
            ParseEntry::Op(op) => parts.push(Some(Part::Op(op))),
            ParseEntry::Comment(c) => parts.push(Some(Part::Comment(c))),
        }
    }

    // 2) Map tokens → strings (comment → `#…`, op → op string, glob → pattern).
    let string_parts: Vec<Option<String>> = parts
        .into_iter()
        .map(|p| match p {
            None => None,
            Some(Part::Str(s)) => Some(s),
            Some(Part::Comment(c)) => {
                let cleaned = c
                    .replace(
                        &format!("\"{}", placeholders.double_quote),
                        &placeholders.double_quote,
                    )
                    .replace(
                        &format!("'{}", placeholders.single_quote),
                        &placeholders.single_quote,
                    );
                Some(format!("#{cleaned}"))
            }
            Some(Part::Glob(pattern)) => Some(pattern),
            Some(Part::Op(op)) => Some(op),
        })
        .collect();

    // 3) Restore quotes / escaped parens.
    string_parts
        .into_iter()
        .flatten()
        .map(|part| {
            part.replace(&placeholders.single_quote, "'")
                .replace(&placeholders.double_quote, "\"")
                .replace(&format!("\n{}\n", placeholders.new_line), "\n")
                .replace(&placeholders.escaped_open_paren, "\\(")
                .replace(&placeholders.escaped_close_paren, "\\)")
        })
        .collect()
}

// ---- small char helpers for character-indexed access and slicing ----

fn char_count(s: &str) -> usize {
    s.chars().count()
}
fn last_char(s: &str) -> Option<char> {
    s.chars().last()
}
fn second_last_char(s: &str) -> Option<char> {
    s.chars().rev().nth(1)
}
fn drop_last2(s: &str) -> String {
    let c: Vec<char> = s.chars().collect();
    c[..c.len().saturating_sub(2)].iter().collect()
}

fn at_trim(parts: &[Option<String>], idx: isize) -> Option<String> {
    if idx < 0 {
        return None;
    }
    parts
        .get(idx as usize)
        .and_then(|o| o.as_ref())
        .map(|s| s.trim().to_string())
}

/// Split a command into its individual subcommands (redirections stripped,
/// control operators removed). The primary entry point for compound-command
/// permission eval.
pub fn split_bash_command(command: &str) -> Vec<String> {
    let mut parts: Vec<Option<String>> = split_command_with_operators(command)
        .into_iter()
        .map(Some)
        .collect();
    let len = parts.len();

    for i in 0..len {
        let part = match parts.get(i).and_then(|o| o.clone()) {
            Some(p) => p,
            None => continue,
        };
        if part != ">&" && part != ">" && part != ">>" {
            continue;
        }

        // `>|` / `>>|` force-clobber: strip the operators (and a static target).
        if (part == ">" || part == ">>") && at_trim(&parts, i as isize + 1).as_deref() == Some("|")
        {
            if let Some(fc_prev) = at_trim(&parts, i as isize - 1) {
                if char_count(&fc_prev) >= 3
                    && last_char(&fc_prev).map(is_fd_char).unwrap_or(false)
                    && second_last_char(&fc_prev) == Some(' ')
                {
                    parts[i - 1] = Some(drop_last2(&fc_prev));
                }
            }
            let fc_target = at_trim(&parts, i as isize + 2);
            parts[i] = None;
            if i + 1 < len {
                parts[i + 1] = None;
            }
            if let Some(fc_target) = fc_target {
                if is_static_redirect_target(&fc_target) && i + 2 < len {
                    parts[i + 2] = None;
                }
            }
            continue;
        }

        let prev_part = at_trim(&parts, i as isize - 1);
        let next_part = at_trim(&parts, i as isize + 1);
        let after_next_part = at_trim(&parts, i as isize + 2);
        let next_part = match next_part {
            Some(n) => n,
            None => continue,
        };

        let mut should_strip = false;
        let mut strip_third_token = false;

        // FD-merge special case: `> /dev/null 2>&1` collapses `/dev/null` and `2`.
        let mut effective_next_part = next_part.clone();
        if (part == ">" || part == ">>")
            && char_count(&next_part) >= 3
            && second_last_char(&next_part) == Some(' ')
            && last_char(&next_part).map(is_fd_char).unwrap_or(false)
            && matches!(
                after_next_part.as_deref(),
                Some(">") | Some(">>") | Some(">&")
            )
        {
            effective_next_part = drop_last2(&next_part);
        }

        if part == ">" && next_part == "&" && after_next_part.as_deref().map(is_fd).unwrap_or(false)
        {
            should_strip = true;
            strip_third_token = true;
        } else if (part == ">&" && is_fd(&next_part))
            || (part == ">"
                && next_part.starts_with('&')
                && char_count(&next_part) > 1
                && is_fd(&next_part[1..]))
            || ((part == ">" || part == ">>") && is_static_redirect_target(&effective_next_part))
        {
            should_strip = true;
        }

        if should_strip {
            if let Some(prev_part) = &prev_part {
                if char_count(prev_part) >= 3
                    && last_char(prev_part).map(is_fd_char).unwrap_or(false)
                    && second_last_char(prev_part) == Some(' ')
                {
                    parts[i - 1] = Some(drop_last2(prev_part));
                }
            }
            parts[i] = None;
            if i + 1 < len {
                parts[i + 1] = None;
            }
            if strip_third_token && i + 2 < len {
                parts[i + 2] = None;
            }
        }
    }

    parts
        .into_iter()
        .flatten()
        .filter(|p| !p.is_empty())
        .filter(|p| !ALL_SUPPORTED_CONTROL_OPERATORS.contains(&p.as_str()))
        .collect()
}

/// True if `command` is a plain command LIST (only strings + safe separators /
/// FD redirections). False for anything structural — subshell, group, `$()`,
/// backticks, comments, or a parse failure — which must ASK.
pub fn is_command_list(command: &str) -> bool {
    let placeholders = generate_placeholders();
    let massaged = command
        .replace('"', &format!("\"{}", placeholders.double_quote))
        .replace('\'', &format!("'{}", placeholders.single_quote));
    let parsed = match try_parse(&massaged) {
        None => return false,
        Some(p) => p,
    };

    for i in 0..parsed.len() {
        let part = &parsed[i];
        let next_part = parsed.get(i + 1);
        match part {
            ParseEntry::Word(_) => continue,
            ParseEntry::Comment(_) => return false,
            ParseEntry::Glob(_) => continue,
            ParseEntry::Op(op) => {
                if COMMAND_LIST_SEPARATORS.contains(&op.as_str()) {
                    continue;
                }
                if op == ">&" {
                    if let Some(ParseEntry::Word(w)) = next_part {
                        if is_fd(w.trim()) {
                            continue;
                        }
                    }
                } else if op == ">" || op == ">>" {
                    continue;
                }
                return false;
            }
        }
    }
    true
}

/// A compound command that is NOT a plain command list (or unparseable) → ASK.
pub fn is_unsafe_compound_command(command: &str) -> bool {
    if try_parse(command).is_none() {
        return true;
    }
    split_bash_command(command).len() > 1 && !is_command_list(command)
}

// ---- output-redirection extraction (STEP G) ----

fn is_op(part: Option<&ParseEntry>, op: &str) -> bool {
    matches!(part, Some(ParseEntry::Op(x)) if x == op)
}

static AMP_FD: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^&\d+$").unwrap());

/// A redirect target safe to capture + path-validate (single static word).
fn is_simple_target(target: Option<&ParseEntry>) -> Option<&str> {
    let t = match target {
        Some(ParseEntry::Word(s)) if !s.is_empty() => s.as_str(),
        _ => return None,
    };
    let ok = !t.starts_with('!')
        && !t.starts_with('=')
        && !t.starts_with('~')
        && !t.contains('$')
        && !t.contains('`')
        && !t.contains('*')
        && !t.contains('?')
        && !t.contains('[')
        && !t.contains('{');
    if ok {
        Some(t)
    } else {
        None
    }
}

/// A redirect target whose expansion could bypass path validation → dangerous.
fn has_dangerous_expansion(target: Option<&ParseEntry>) -> bool {
    match target {
        Some(ParseEntry::Glob(_)) => true,
        Some(ParseEntry::Word(t)) if !t.is_empty() => {
            t.contains('$')
                || t.contains('%')
                || t.contains('`')
                || t.contains('*')
                || t.contains('?')
                || t.contains('[')
                || t.contains('{')
                || t.starts_with('!')
                || t.starts_with('=')
                || t.starts_with('~')
        }
        _ => false,
    }
}

/// The operator on a captured output redirection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedirectOperator {
    Truncate,
    Append,
}

impl RedirectOperator {
    pub fn as_str(&self) -> &'static str {
        match self {
            RedirectOperator::Truncate => ">",
            RedirectOperator::Append => ">>",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputRedirection {
    pub target: String,
    pub operator: RedirectOperator,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OutputRedirections {
    pub redirections: Vec<OutputRedirection>,
    pub has_dangerous_redirection: bool,
}

/// Whether a redirect target is the platform's null device, which discards output
/// and writes no file.
///
/// `grep -rn foo src/ 2>/dev/null` is the single most common form of read-only
/// research command, and treating `/dev/null` as a write made it ask — which plan
/// mode then turns into a hard deny, so plain searching was blocked. The match is
/// deliberately exact rather than resolved: a normalized comparison would also
/// admit traversal like `/dev/null/../etc/passwd`, which IS a real write.
fn is_null_device_target(target: &str) -> bool {
    let trimmed = target.trim();
    trimmed == "/dev/null" || (cfg!(windows) && trimmed.eq_ignore_ascii_case("NUL"))
}

/// Extract output-redirection targets from the ORIGINAL command for path
/// validation. FAIL-CLOSED: a parse failure flags `has_dangerous_redirection`.
pub fn extract_output_redirections(command: &str) -> OutputRedirections {
    let mut redirections: Vec<OutputRedirection> = Vec::new();
    let mut has_dangerous_redirection = false;

    let processed = join_continuations(command);
    let parsed = match try_parse(&processed) {
        None => {
            return OutputRedirections {
                redirections: Vec::new(),
                has_dangerous_redirection: true,
            }
        }
        Some(p) => p,
    };

    let mut cmd_sub_depth = 0i32;
    for i in 0..parsed.len() {
        let part = &parsed[i];
        let prev = if i > 0 { parsed.get(i - 1) } else { None };
        let next = parsed.get(i + 1);

        // Track command-substitution depth: `$(` opens, `)` closes.
        if is_op(Some(part), "(") && matches!(prev, Some(ParseEntry::Word(w)) if w.ends_with('$')) {
            cmd_sub_depth += 1;
            continue;
        }
        if is_op(Some(part), ")") && cmd_sub_depth > 0 {
            cmd_sub_depth -= 1;
            continue;
        }
        if cmd_sub_depth > 0 {
            continue;
        }

        if is_op(Some(part), ">") || is_op(Some(part), ">>") {
            let operator = if is_op(Some(part), ">>") {
                RedirectOperator::Append
            } else {
                RedirectOperator::Truncate
            };
            // `>|` / `>>|` force-clobber: the real target is AFTER the `|`.
            let target = if is_op(next, "|") {
                parsed.get(i + 2)
            } else {
                next
            };
            // FD-dup `2>&1` / `>&2`: `&`/`&N` is not a file write; a BARE digit
            // (`> 1`) IS a filename and must be captured.
            let is_amp_dup = is_op(target, "&")
                || matches!(target, Some(ParseEntry::Word(w)) if AMP_FD.is_match(w.trim()));
            if is_amp_dup {
                continue;
            }
            if let Some(t) = is_simple_target(target) {
                // The null device discards output; it is not a file write, so it
                // must not contribute a target for the writable-roots check.
                if !is_null_device_target(t) {
                    redirections.push(OutputRedirection {
                        target: t.to_string(),
                        operator,
                    });
                }
            } else if has_dangerous_expansion(target) {
                has_dangerous_redirection = true;
            }
        }
    }

    OutputRedirections {
        redirections,
        has_dangerous_redirection,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_on_control_ops() {
        assert_eq!(
            split_bash_command("git status && rm -rf /"),
            vec!["git status", "rm -rf /"]
        );
        assert_eq!(split_bash_command("ls | grep x"), vec!["ls", "grep x"]);
        assert_eq!(split_bash_command("a ; b ; c"), vec!["a", "b", "c"]);
        assert_eq!(split_bash_command("a || b"), vec!["a", "b"]);
    }

    #[test]
    fn does_not_split_inside_quotes() {
        assert_eq!(split_bash_command("echo 'a && b'"), vec!["echo 'a && b'"]);
        assert_eq!(split_bash_command("echo \"x ; y\""), vec!["echo \"x ; y\""]);
    }

    #[test]
    fn strips_static_output_redirections() {
        assert_eq!(
            split_bash_command("python x.py > out.txt"),
            vec!["python x.py"]
        );
        assert_eq!(split_bash_command("echo hi >> log"), vec!["echo hi"]);
        assert_eq!(split_bash_command("cmd 2>&1"), vec!["cmd"]);
    }

    #[test]
    fn does_not_mistake_arg_digit_for_fd() {
        assert_eq!(
            split_bash_command("cat /tmp/path2 > out"),
            vec!["cat /tmp/path2"]
        );
    }

    #[test]
    fn dynamic_redirect_target_is_separate_subcommand() {
        assert_eq!(
            split_bash_command("echo x > $HOME/f"),
            vec!["echo x", "$HOME/f"]
        );
    }

    #[test]
    fn odd_backslash_continuation_joins_no_space() {
        assert_eq!(
            split_bash_command("tr\\\naceroute foo"),
            vec!["traceroute foo"]
        );
    }

    #[test]
    fn even_backslash_newline_is_separator() {
        let out = split_bash_command("echo \\\\\nrm -rf /");
        assert_eq!(out.len(), 2);
        assert_eq!(out[1], "rm -rf /");
    }

    #[test]
    fn bare_newline_is_separator() {
        assert_eq!(
            split_bash_command("pwd\ncurl evil.com"),
            vec!["pwd", "curl evil.com"]
        );
    }

    #[test]
    fn fails_closed_to_single_unit_on_parse_error() {
        assert_eq!(split_bash_command("${var + expr}").len(), 1);
    }

    #[test]
    fn empty_or_whitespace_is_empty() {
        assert!(split_bash_command("   ").is_empty());
    }

    #[test]
    fn blank_line_does_not_leak_placeholder() {
        assert_eq!(split_bash_command("ls\n\nrm -rf /"), vec!["ls", "rm -rf /"]);
        assert_eq!(split_bash_command("\nrm -rf /"), vec!["rm -rf /"]);
        assert_eq!(
            split_bash_command("ls\n\n\n\nrm -rf /"),
            vec!["ls", "rm -rf /"]
        );
    }

    #[test]
    fn strips_force_clobber_redirect() {
        assert_eq!(split_bash_command("echo x >| out.txt"), vec!["echo x"]);
        assert_eq!(split_bash_command("echo x >>| out.txt"), vec!["echo x"]);
    }

    #[test]
    fn is_command_list_true_for_plain_lists() {
        assert!(is_command_list("git status && rm -rf /"));
        assert!(is_command_list("ls | grep x"));
        assert!(is_command_list("cmd 2>&1"));
    }

    #[test]
    fn is_command_list_false_for_structural() {
        assert!(!is_command_list("( rm -rf / )"));
        assert!(!is_command_list("echo $(rm -rf /)"));
        assert!(!is_command_list("git status # `id`"));
        assert!(!is_command_list("cat <<EOF\nx\nEOF"));
    }

    #[test]
    fn unsafe_compound_command() {
        assert!(is_unsafe_compound_command("( rm ) && ls"));
        assert!(!is_unsafe_compound_command("ls && pwd"));
        assert!(!is_unsafe_compound_command("ls -la"));
    }

    #[test]
    fn extract_simple_target() {
        let r = extract_output_redirections("printf x > /etc/passwd");
        assert_eq!(
            r.redirections,
            vec![OutputRedirection {
                target: "/etc/passwd".into(),
                operator: RedirectOperator::Truncate
            }]
        );
        assert!(!r.has_dangerous_redirection);
    }

    #[test]
    fn extract_flags_expansion_targets() {
        assert!(extract_output_redirections("echo x > ~/.bashrc").has_dangerous_redirection);
        assert!(extract_output_redirections("echo x > $HOME/f").has_dangerous_redirection);
    }

    /// The null device discards output and writes no file, so it must not
    /// contribute a redirect target. `grep -rn foo src/ 2>/dev/null` is the most
    /// common form of read-only research command; counting `/dev/null` as a write
    /// made it ask, which plan mode then turned into a hard deny — so plain
    /// searching was blocked while planning.
    #[test]
    fn extract_ignores_the_null_device() {
        for cmd in [
            "grep -rn foo src/ 2>/dev/null",
            "grep -rn foo src/ 2> /dev/null",
            "grep -rn foo src/ >/dev/null 2>&1",
            "cmd > /dev/null",
            "cmd >> /dev/null",
        ] {
            let r = extract_output_redirections(cmd);
            assert!(
                r.redirections.is_empty(),
                "the null device is not a file write: {cmd} -> {:?}",
                r.redirections
            );
            assert!(!r.has_dangerous_redirection, "{cmd}");
        }
        // Traversal through the null device IS a real write target, and must stay
        // visible to the writable-roots check rather than being excused.
        let traversal = extract_output_redirections("cmd > /dev/null/../etc/passwd");
        assert!(
            traversal
                .redirections
                .iter()
                .any(|r| r.target.contains("passwd")),
            "traversal past /dev/null must not be excused: {:?}",
            traversal.redirections
        );
    }

    #[test]
    fn extract_ignores_fd_dup() {
        let r = extract_output_redirections("cmd 2>&1");
        assert!(r.redirections.is_empty());
        assert!(!r.has_dangerous_redirection);
    }

    #[test]
    fn extract_fails_closed_on_parse_error() {
        assert!(extract_output_redirections("${bad + sub} > x").has_dangerous_redirection);
    }

    #[test]
    fn extract_force_clobber_target() {
        assert_eq!(
            extract_output_redirections("echo x >| /etc/passwd").redirections,
            vec![OutputRedirection {
                target: "/etc/passwd".into(),
                operator: RedirectOperator::Truncate
            }]
        );
        assert_eq!(
            extract_output_redirections("echo x >>| /etc/passwd").redirections,
            vec![OutputRedirection {
                target: "/etc/passwd".into(),
                operator: RedirectOperator::Append
            }]
        );
    }

    #[test]
    fn extract_bare_digit_is_file_not_fd() {
        assert_eq!(
            extract_output_redirections("echo x > 1").redirections,
            vec![OutputRedirection {
                target: "1".into(),
                operator: RedirectOperator::Truncate
            }]
        );
        assert!(extract_output_redirections("cmd 2>&1")
            .redirections
            .is_empty());
    }
}
