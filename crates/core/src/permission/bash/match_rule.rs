//! Bash permission-rule matching. Operates on a
//! SINGLE already-split subcommand. The asymmetry between ALLOW and DENY/ASK
//! candidate building is load-bearing security:
//!  - ALLOW: strip only SAFE_ENV_VARS (whitelist) + safe wrappers; compound-guard ON.
//!  - DENY/ASK: strip ALL leading env vars (to a fixed point, interleaved with
//!    wrappers); compound-guard OFF.

use crate::permission::bash::split::split_bash_command;
use regex::Regex;
use std::collections::HashSet;
use std::sync::LazyLock;

/// Env vars safe to strip for ALLOW matching — CANNOT execute code or load
/// libraries.
static SAFE_ENV_VARS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "GOEXPERIMENT",
        "GOOS",
        "GOARCH",
        "CGO_ENABLED",
        "GO111MODULE",
        "RUST_BACKTRACE",
        "RUST_LOG",
        "NODE_ENV",
        "PYTHONUNBUFFERED",
        "PYTHONDONTWRITEBYTECODE",
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD",
        "PYTEST_DEBUG",
        "ANTHROPIC_API_KEY",
        "LANG",
        "LANGUAGE",
        "LC_ALL",
        "LC_CTYPE",
        "LC_TIME",
        "CHARSET",
        "TERM",
        "COLORTERM",
        "NO_COLOR",
        "FORCE_COLOR",
        "TZ",
        "LS_COLORS",
        "LSCOLORS",
        "GREP_COLOR",
        "GREP_COLORS",
        "GCC_COLORS",
        "TIME_STYLE",
        "BLOCK_SIZE",
        "BLOCKSIZE",
    ]
    .into_iter()
    .collect()
});

/// Env vars that make a DIFFERENT binary run (injection / resolution hijack).
pub static BINARY_HIJACK_VARS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(LD_|DYLD_|PATH$)").unwrap());

static SAFE_ENV_VAR_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^([A-Za-z_][A-Za-z0-9_]*)=([A-Za-z0-9_./:-]+)[ \t]+").unwrap());

static SAFE_WRAPPER_PATTERNS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    vec![
        Regex::new(r"^timeout[ \t]+(?:(?:--(?:foreground|preserve-status|verbose)|--(?:kill-after|signal)=[A-Za-z0-9_.+-]+|--(?:kill-after|signal)[ \t]+[A-Za-z0-9_.+-]+|-v|-[ks][ \t]+[A-Za-z0-9_.+-]+|-[ks][A-Za-z0-9_.+-]+)[ \t]+)*(?:--[ \t]+)?\d+(?:\.\d+)?[smhd]?[ \t]+").unwrap(),
        Regex::new(r"^time[ \t]+(?:--[ \t]+)?").unwrap(),
        Regex::new(r"^nice(?:[ \t]+-n[ \t]+-?\d+|[ \t]+-\d+)?[ \t]+(?:--[ \t]+)?").unwrap(),
        Regex::new(r"^stdbuf(?:[ \t]+-[ioe][LN0-9]+)+[ \t]+(?:--[ \t]+)?").unwrap(),
        Regex::new(r"^nohup[ \t]+(?:--[ \t]+)?").unwrap(),
    ]
});

/// Broad leading-env-assignment pattern for DENY/ASK stripping.
static BROAD_ENV_VAR_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"^([A-Za-z_][A-Za-z0-9_]*(?:\[[^\]]*\])?)\+?=(?:'[^'\n\r]*'|"(?:\\.|[^"$`\\\n\r])*"|\\.|[^ \t\n\r$`;|&()<>\\'"])*[ \t]+"#,
    )
    .unwrap()
});

/// Drop whole-line comments (lines whose entire content is a `#…` comment).
fn strip_comment_lines(command: &str) -> String {
    let lines: Vec<&str> = command.split('\n').collect();
    let kept: Vec<&str> = lines
        .iter()
        .copied()
        .filter(|line| {
            let t = line.trim();
            !t.is_empty() && !t.starts_with('#')
        })
        .collect();
    if kept.is_empty() {
        command.to_string()
    } else {
        kept.join("\n")
    }
}

/// Strip leading SAFE env vars (whitelist) + safe wrappers, to a fixed point.
pub fn strip_safe_wrappers(command: &str) -> String {
    let mut stripped = command.to_string();
    // Phase 1: leading SAFE env vars + comment lines.
    let mut prev = String::new();
    while stripped != prev {
        prev = stripped.clone();
        stripped = strip_comment_lines(&stripped);
        if let Some(caps) = SAFE_ENV_VAR_PATTERN.captures(&stripped) {
            let name = caps.get(1).unwrap().as_str();
            if SAFE_ENV_VARS.contains(name) {
                stripped = SAFE_ENV_VAR_PATTERN.replace(&stripped, "").into_owned();
            }
        }
    }
    // Phase 2: wrappers only.
    prev.clear();
    while stripped != prev {
        prev = stripped.clone();
        stripped = strip_comment_lines(&stripped);
        for pattern in SAFE_WRAPPER_PATTERNS.iter() {
            stripped = pattern.replace(&stripped, "").into_owned();
        }
    }
    stripped.trim().to_string()
}

/// Strip ALL leading env vars (broad), optionally blocking binary-hijack names.
pub fn strip_all_leading_env_vars(command: &str, blocklist: Option<&Regex>) -> String {
    let mut stripped = command.to_string();
    let mut prev = String::new();
    while stripped != prev {
        prev = stripped.clone();
        stripped = strip_comment_lines(&stripped);
        let caps = match BROAD_ENV_VAR_PATTERN.captures(&stripped) {
            Some(c) => c,
            None => continue,
        };
        let name = caps.get(1).unwrap().as_str();
        if let Some(block) = blocklist {
            if block.is_match(name) {
                break;
            }
        }
        let full_len = caps.get(0).unwrap().as_str().len();
        stripped = stripped[full_len..].to_string();
    }
    stripped.trim().to_string()
}

// ---- rule parsing + matching ----

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParsedBashRule {
    Prefix(String),
    Wildcard(String),
}

/// Parse a Bash rule's content into a prefix or wildcard matcher. Legacy `…:*` →
/// prefix; an unescaped `*` elsewhere → wildcard; PLAIN content → prefix.
pub fn parse_bash_rule_content(content: &str) -> ParsedBashRule {
    if let Some(idx) = content.strip_suffix(":*") {
        // legacy `<x>:*` → prefix <x> (the regex `^(.+):\*$` requires ≥1 char).
        if !idx.is_empty() {
            return ParsedBashRule::Prefix(idx.to_string());
        }
    }
    if has_unescaped_wildcard(content) {
        return ParsedBashRule::Wildcard(content.to_string());
    }
    ParsedBashRule::Prefix(content.to_string())
}

fn has_unescaped_wildcard(pattern: &str) -> bool {
    if pattern.ends_with(":*") {
        return false;
    }
    let chars: Vec<char> = pattern.chars().collect();
    for i in 0..chars.len() {
        if chars[i] == '*' {
            let mut backslashes = 0;
            let mut j = i as isize - 1;
            while j >= 0 && chars[j as usize] == '\\' {
                backslashes += 1;
                j -= 1;
            }
            if backslashes % 2 == 0 {
                return true;
            }
        }
    }
    false
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PatToken {
    Wild,
    Ch(char),
}

fn build_pattern(pattern: &str) -> Vec<PatToken> {
    let chars: Vec<char> = pattern.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '\\' && i + 1 < chars.len() {
            let n = chars[i + 1];
            if n == '*' {
                out.push(PatToken::Ch('*'));
                i += 2;
                continue;
            }
            if n == '\\' {
                out.push(PatToken::Ch('\\'));
                i += 2;
                continue;
            }
            out.push(PatToken::Ch(c));
            i += 1;
            continue;
        }
        if c == '*' {
            out.push(PatToken::Wild);
            i += 1;
            continue;
        }
        out.push(PatToken::Ch(c));
        i += 1;
    }
    out
}

fn greedy_glob(pat: &[PatToken], text: &[char]) -> bool {
    let mut i = 0isize; // text index
    let mut j = 0isize; // pat index
    let mut star = -1isize;
    let mut mark = 0isize;
    let tlen = text.len() as isize;
    let plen = pat.len() as isize;
    while i < tlen {
        if j < plen && matches!(pat[j as usize], PatToken::Ch(c) if c == text[i as usize]) {
            i += 1;
            j += 1;
        } else if j < plen && pat[j as usize] == PatToken::Wild {
            star = j;
            mark = i;
            j += 1;
        } else if star != -1 {
            j = star + 1;
            mark += 1;
            i = mark;
        } else {
            return false;
        }
    }
    while j < plen && pat[j as usize] == PatToken::Wild {
        j += 1;
    }
    j == plen
}

/// Regex-free wildcard match (LeetCode-44 greedy). A trailing ` *` that is the
/// ONLY wildcard is made optional so `git *` matches bare `git`.
pub fn match_wildcard_pattern(pattern: &str, command: &str) -> bool {
    let pat = build_pattern(pattern.trim());
    let text: Vec<char> = command.chars().collect();
    if greedy_glob(&pat, &text) {
        return true;
    }
    let wildcards = pat.iter().filter(|p| **p == PatToken::Wild).count();
    if pat.len() >= 2
        && pat[pat.len() - 1] == PatToken::Wild
        && pat[pat.len() - 2] == PatToken::Ch(' ')
        && wildcards == 1
    {
        return greedy_glob(&pat[..pat.len() - 2], &text);
    }
    false
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BashRuleEntry {
    /// Original rule string (for reporting), e.g. `Bash(git status)`.
    pub source: String,
    /// Rule content inside `Bash(...)`; `None` = tool-wide `Bash`.
    pub content: Option<String>,
}

/// The class of rule set being matched, which selects candidate-build +
/// compound-guard policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleClass {
    Allow,
    DenyAsk,
}

/// The first rule in `rules` that matches `subcommand` under the given class
/// policy, else `None`.
pub fn match_bash_rule<'a>(
    subcommand: &str,
    rules: &'a [BashRuleEntry],
    rule_class: RuleClass,
) -> Option<&'a BashRuleEntry> {
    let strip_all_env = rule_class == RuleClass::DenyAsk;
    let compound_guard = rule_class == RuleClass::Allow;
    let candidates = build_candidates(subcommand, strip_all_env);
    for rule in rules {
        let content = match &rule.content {
            None => return Some(rule), // tool-wide `Bash` matches any subcommand
            Some(c) => c,
        };
        let parsed = parse_bash_rule_content(content);
        for cand in &candidates {
            if match_one(&parsed, cand, compound_guard) {
                return Some(rule);
            }
        }
    }
    None
}

fn build_candidates(sub: &str, strip_all_env: bool) -> Vec<String> {
    let command = sub.trim().to_string();
    let mut candidates = vec![command.clone()];
    let wrapped = strip_safe_wrappers(&command);
    if wrapped != command {
        candidates.push(wrapped);
    }
    if !strip_all_env {
        return candidates;
    }
    // DENY/ASK: fixed-point interleave of env-strip + wrapper-strip.
    let mut seen: HashSet<String> = candidates.iter().cloned().collect();
    let mut start = 0;
    while start < candidates.len() {
        let end = candidates.len();
        for i in start..end {
            let cmd = candidates[i].clone();
            let env_stripped = strip_all_leading_env_vars(&cmd, None);
            if seen.insert(env_stripped.clone()) {
                candidates.push(env_stripped);
            }
            let wrapper_stripped = strip_safe_wrappers(&cmd);
            if seen.insert(wrapper_stripped.clone()) {
                candidates.push(wrapper_stripped);
            }
        }
        start = end;
    }
    candidates
}

fn match_one(rule: &ParsedBashRule, candidate: &str, compound_guard: bool) -> bool {
    match rule {
        ParsedBashRule::Prefix(prefix) => {
            if compound_guard && split_bash_command(candidate).len() > 1 {
                return false;
            }
            if candidate == prefix {
                return true;
            }
            if candidate.starts_with(&format!("{prefix} ")) {
                return true;
            }
            let xargs = format!("xargs {prefix}");
            candidate == xargs || candidate.starts_with(&format!("{xargs} "))
        }
        ParsedBashRule::Wildcard(pattern) => {
            if compound_guard && split_bash_command(candidate).len() > 1 {
                return false;
            }
            match_wildcard_pattern(pattern, candidate)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(content: Option<&str>) -> BashRuleEntry {
        BashRuleEntry {
            source: format!("Bash({})", content.unwrap_or("")),
            content: content.map(String::from),
        }
    }

    #[test]
    fn parse_rule_content_variants() {
        assert_eq!(
            parse_bash_rule_content("rm"),
            ParsedBashRule::Prefix("rm".into())
        );
        assert_eq!(
            parse_bash_rule_content("git status:*"),
            ParsedBashRule::Prefix("git status".into())
        );
        assert_eq!(
            parse_bash_rule_content("npm run *"),
            ParsedBashRule::Wildcard("npm run *".into())
        );
    }

    #[test]
    fn strip_helpers() {
        assert_eq!(
            strip_safe_wrappers("NODE_ENV=prod npm run build"),
            "npm run build"
        );
        assert_eq!(
            strip_safe_wrappers("timeout 10 npm install foo"),
            "npm install foo"
        );
        assert_eq!(strip_safe_wrappers("nohup git status"), "git status");
        assert_eq!(
            strip_safe_wrappers("DOCKER_HOST=evil docker ps"),
            "DOCKER_HOST=evil docker ps"
        );
        assert_eq!(
            strip_safe_wrappers("timeout -k$(id) 10 ls"),
            "timeout -k$(id) 10 ls"
        );

        assert_eq!(
            strip_all_leading_env_vars("FOO=bar rm -rf /", None),
            "rm -rf /"
        );
        assert_eq!(strip_all_leading_env_vars("A=1 B=2 claude", None), "claude");
        assert_eq!(
            strip_all_leading_env_vars("FOO=$(id) rm", None),
            "FOO=$(id) rm"
        );
    }

    #[test]
    fn wildcard_matching() {
        assert!(match_wildcard_pattern("npm run *", "npm run build"));
        assert!(!match_wildcard_pattern("npm run *", "npm test"));
        assert!(match_wildcard_pattern("git *", "git"));
        assert!(match_wildcard_pattern("git *", "git status"));
        assert!(match_wildcard_pattern("a*b", "axxxb"));
        assert!(!match_wildcard_pattern("a*b", "axxxc"));
        assert!(match_wildcard_pattern("foo\\*", "foo*"));
        assert!(!match_wildcard_pattern("foo\\*", "foobar"));
    }

    #[test]
    fn prefix_word_boundary() {
        assert!(match_bash_rule("ls -la", &[rule(Some("ls"))], RuleClass::Allow).is_some());
        assert!(match_bash_rule("lsof -i", &[rule(Some("ls"))], RuleClass::Allow).is_none());
        assert!(match_bash_rule("rm -rf /", &[rule(Some("rm"))], RuleClass::DenyAsk).is_some());
    }

    #[test]
    fn allow_strips_only_safe_env() {
        assert!(match_bash_rule(
            "NODE_ENV=prod npm run build",
            &[rule(Some("npm run"))],
            RuleClass::Allow
        )
        .is_some());
        assert!(match_bash_rule(
            "DOCKER_HOST=evil docker ps",
            &[rule(Some("docker ps"))],
            RuleClass::Allow
        )
        .is_none());
    }

    #[test]
    fn denyask_strips_all_env() {
        assert!(
            match_bash_rule("FOO=bar rm -rf /", &[rule(Some("rm"))], RuleClass::DenyAsk).is_some()
        );
        assert!(match_bash_rule(
            "nohup FOO=bar timeout 5 claude",
            &[rule(Some("claude"))],
            RuleClass::DenyAsk
        )
        .is_some());
    }

    #[test]
    fn xargs_variant() {
        assert!(
            match_bash_rule("xargs rm file", &[rule(Some("rm"))], RuleClass::DenyAsk).is_some()
        );
        assert!(
            match_bash_rule("xargs -n1 rm file", &[rule(Some("rm"))], RuleClass::DenyAsk).is_none()
        );
    }

    #[test]
    fn compound_guard() {
        assert!(match_bash_rule(
            "cd src&& python3 evil.py",
            &[rule(Some("cd"))],
            RuleClass::Allow
        )
        .is_none());
        assert!(match_bash_rule(
            "cd src&& python3 evil.py",
            &[rule(Some("cd"))],
            RuleClass::DenyAsk
        )
        .is_some());
    }

    #[test]
    fn tool_wide_rule_matches_any() {
        assert!(match_bash_rule("anything goes", &[rule(None)], RuleClass::Allow).is_some());
    }
}
