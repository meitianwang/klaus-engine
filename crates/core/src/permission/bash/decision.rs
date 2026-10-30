//! The Bash permission decision (legacy path). Splits a compound command into
//! subcommands and evaluates each
//! independently so a per-subcommand DENY beats a whole-command allow and a
//! whole-command allow cannot admit a trailing `&& rm -rf /`.

use crate::permission::bash::dangerous_removal::check_dangerous_removal;
use crate::permission::bash::match_rule::{
    match_bash_rule, strip_safe_wrappers, BashRuleEntry, RuleClass,
};
use crate::permission::bash::node_path;
use crate::permission::bash::split::{
    extract_output_redirections, is_unsafe_compound_command, split_bash_command,
};
use regex::Regex;
use std::sync::LazyLock;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BashDecision {
    Deny {
        rule: Option<BashRuleEntry>,
    },
    Allow {
        rule: Option<BashRuleEntry>,
    },
    Ask {
        from_rule: bool,
        dangerous_removal: bool,
    },
    Passthrough,
}

#[derive(Debug, Clone, Default)]
pub struct BashRuleSets {
    pub deny: Vec<BashRuleEntry>,
    pub allow: Vec<BashRuleEntry>,
    pub ask: Vec<BashRuleEntry>,
}

/// Options for [`evaluate_bash_command`]. The two probes are optional (`None` =
/// off).
pub struct BashDecisionOptions {
    pub cwd: String,
    pub writable_roots: Vec<String>,
    /// Absolute home directory, for the `~`/home check. Empty skips it.
    pub home_dir: Option<String>,
    /// Stage B read-only classifier: a subcommand known read-only → auto-allow.
    #[allow(clippy::type_complexity)]
    pub is_read_only_subcommand: Option<Box<dyn Fn(&str) -> bool + Send + Sync>>,
    /// True if the cwd looks like a bare/exploited git repo.
    pub is_cwd_bare_repo: Option<Box<dyn Fn() -> bool + Send + Sync>>,
}

impl BashDecisionOptions {
    pub fn new(cwd: impl Into<String>) -> Self {
        BashDecisionOptions {
            cwd: cwd.into(),
            writable_roots: Vec::new(),
            home_dir: None,
            is_read_only_subcommand: None,
            is_cwd_bare_repo: None,
        }
    }
}

/// Cap on subcommands analyzed by the security check (legacy path).
const MAX_SUBCOMMANDS: usize = 50;

/// Command-injection markers: their presence never auto-allows.
static INJECTION_MARKERS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\$\(|`|\$\{|<\(|>\(").unwrap());
static LEADING_WRAP: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[\s{`]+").unwrap());
static SINGLE_QUOTED: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"'[^']*'").unwrap());
static BACKTICK_CMD: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"`([^`]+)`").unwrap());
static LEADING_DOT_SLASH: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^\.?/").unwrap());
static GIT_INTERNAL: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    vec![
        Regex::new(r"^HEAD$").unwrap(),
        Regex::new(r"^objects(?:/|$)").unwrap(),
        Regex::new(r"^refs(?:/|$)").unwrap(),
        Regex::new(r"^hooks(?:/|$)").unwrap(),
    ]
});

pub fn evaluate_bash_command(
    command: &str,
    rules: &BashRuleSets,
    opts: &BashDecisionOptions,
) -> BashDecision {
    let home_dir = opts.home_dir.clone().unwrap_or_default();

    // STEP A2: an unsafe compound cannot be safely SPLIT for auto-allow, but it
    // CAN be over-scanned for deny/ask (a conservative over-match is fail-safe).
    if is_unsafe_compound_command(command) {
        let subs = split_bash_command(command);
        let denied =
            match_deny_ask(command, &rules.deny).or_else(|| first_rule_match(&subs, &rules.deny));
        if let Some(rule) = denied {
            return BashDecision::Deny { rule: Some(rule) };
        }
        if match_deny_ask(command, &rules.ask).is_some()
            || first_rule_match(&subs, &rules.ask).is_some()
        {
            return BashDecision::Ask {
                from_rule: true,
                dangerous_removal: false,
            };
        }
        if subs
            .iter()
            .any(|s| check_dangerous_removal(s, &opts.cwd, &home_dir).is_some())
        {
            return BashDecision::Ask {
                from_rule: false,
                dangerous_removal: true,
            };
        }
        return BashDecision::Ask {
            from_rule: false,
            dangerous_removal: false,
        };
    }

    // STEP A3: injection markers anywhere auto-allow.
    let has_injection = INJECTION_MARKERS.is_match(command);

    // STEP D: split into subcommands; drop a `cd <cwd>` no-op.
    let subcommands: Vec<String> = split_bash_command(command)
        .into_iter()
        .filter(|s| !is_cd_to_cwd(s, &opts.cwd))
        .collect();
    if subcommands.is_empty() {
        return BashDecision::Passthrough;
    }

    // STEP D2: per-subcommand DENY scan over the FULL list.
    for sub in &subcommands {
        if let Some(rule) = match_deny_ask(sub, &rules.deny) {
            return BashDecision::Deny { rule: Some(rule) };
        }
    }

    // STEP D2.5: a content-specific ASK rule forces ASK and is bypass-immune.
    for sub in &subcommands {
        if match_deny_ask(sub, &rules.ask).is_some() {
            return BashDecision::Ask {
                from_rule: true,
                dangerous_removal: false,
            };
        }
    }

    // STEP D2.7: catastrophic `rm`/`rmdir` forces ASK, ALLOW-RULE-immune.
    for sub in &subcommands {
        if check_dangerous_removal(sub, &opts.cwd, &home_dir).is_some() {
            return BashDecision::Ask {
                from_rule: false,
                dangerous_removal: true,
            };
        }
    }

    // STEP D3: too many subcommands → unreliable analysis → ASK.
    if subcommands.len() > MAX_SUBCOMMANDS {
        return BashDecision::Ask {
            from_rule: false,
            dangerous_removal: false,
        };
    }

    // STEP E: structural cd gates.
    let cd_count = subcommands.iter().filter(|s| is_cd_command(s)).count();
    if cd_count > 1 {
        return BashDecision::Ask {
            from_rule: false,
            dangerous_removal: false,
        };
    }
    let compound_has_cd = cd_count > 0;
    let compound_has_git = subcommands.iter().any(|s| is_git_command(s));
    if compound_has_cd && compound_has_git {
        return BashDecision::Ask {
            from_rule: false,
            dangerous_removal: false,
        };
    }

    // STEP F: per-subcommand eval.
    let git_read_only_blocked =
        compound_has_git && opts.is_cwd_bare_repo.as_ref().map(|f| f()).unwrap_or(false);
    let is_read_only = |sub: &str| -> bool {
        if git_read_only_blocked && is_git_command(sub) {
            return false;
        }
        opts.is_read_only_subcommand
            .as_ref()
            .map(|f| f(sub))
            .unwrap_or(false)
    };
    let mut saw_ask = false;
    let mut saw_ask_from_rule = false;
    let mut saw_passthrough = false;
    for sub in &subcommands {
        match eval_subcommand(sub, rules, &is_read_only) {
            BashDecision::Deny { rule } => return BashDecision::Deny { rule },
            BashDecision::Ask { from_rule, .. } => {
                saw_ask = true;
                if from_rule {
                    saw_ask_from_rule = true;
                }
            }
            BashDecision::Passthrough => saw_passthrough = true,
            BashDecision::Allow { .. } => {}
        }
    }

    // STEP G: validate output-redirect targets on the ORIGINAL command.
    let redir = extract_output_redirections(command);
    let mut redirect_ask = redir.has_dangerous_redirection;
    if compound_has_cd && !redir.redirections.is_empty() {
        redirect_ask = true;
    }
    for r in &redir.redirections {
        if !is_inside_writable_roots(&r.target, &opts.cwd, &opts.writable_roots) {
            redirect_ask = true;
        }
        if compound_has_git && is_git_internal_path(&r.target) {
            redirect_ask = true;
        }
    }

    // STEP H: fold.
    let block_allow = has_injection || redirect_ask;
    if !saw_ask && !saw_passthrough && !block_allow {
        return BashDecision::Allow { rule: None };
    }
    if saw_ask || saw_passthrough || block_allow {
        return BashDecision::Ask {
            from_rule: saw_ask_from_rule,
            dangerous_removal: false,
        };
    }
    BashDecision::Passthrough
}

/// Match `rule_set` against `s` directly OR after stripping a leading
/// brace-group / backtick / whitespace run.
fn match_unwrapped(s: &str, rule_set: &[BashRuleEntry]) -> Option<BashRuleEntry> {
    if let Some(rule) = match_bash_rule(s, rule_set, RuleClass::DenyAsk) {
        return Some(rule.clone());
    }
    let unwrapped = LEADING_WRAP.replace(s, "");
    if unwrapped.as_ref() != s {
        if let Some(rule) = match_bash_rule(&unwrapped, rule_set, RuleClass::DenyAsk) {
            return Some(rule.clone());
        }
    }
    None
}

/// Inner commands of backtick substitutions. Single-quoted spans are removed
/// first (a backtick inside single quotes is literal).
fn extract_backtick_commands(s: &str) -> Vec<String> {
    let unquoted = SINGLE_QUOTED.replace_all(s, " ");
    BACKTICK_CMD
        .captures_iter(&unquoted)
        .map(|c| c[1].trim().to_string())
        .collect()
}

/// Match a deny/ask rule set against `sub`, also trying a structurally-unwrapped
/// form and any backtick-substitution inner command.
fn match_deny_ask(sub: &str, rule_set: &[BashRuleEntry]) -> Option<BashRuleEntry> {
    if let Some(m) = match_unwrapped(sub, rule_set) {
        return Some(m);
    }
    for inner in extract_backtick_commands(sub) {
        if let Some(mi) = match_unwrapped(&inner, rule_set) {
            return Some(mi);
        }
    }
    None
}

/// First rule (deny or ask set) matching any of `subs`, or `None`.
fn first_rule_match(subs: &[String], rule_set: &[BashRuleEntry]) -> Option<BashRuleEntry> {
    subs.iter().find_map(|sub| match_deny_ask(sub, rule_set))
}

fn eval_subcommand(
    sub: &str,
    rules: &BashRuleSets,
    is_read_only: &dyn Fn(&str) -> bool,
) -> BashDecision {
    // Order is load-bearing: deny/ask BEFORE allow.
    if let Some(rule) = match_deny_ask(sub, &rules.deny) {
        return BashDecision::Deny { rule: Some(rule) };
    }
    if match_deny_ask(sub, &rules.ask).is_some() {
        return BashDecision::Ask {
            from_rule: true,
            dangerous_removal: false,
        };
    }
    if let Some(rule) = match_bash_rule(sub, &rules.allow, RuleClass::Allow) {
        return BashDecision::Allow {
            rule: Some(rule.clone()),
        };
    }
    if is_read_only(sub) {
        return BashDecision::Allow { rule: None };
    }
    BashDecision::Passthrough
}

fn normalized_tokens(sub: &str) -> Vec<String> {
    strip_safe_wrappers(sub.trim())
        .split_whitespace()
        .map(String::from)
        .collect()
}

fn is_cd_command(sub: &str) -> bool {
    let toks = normalized_tokens(sub);
    let w = toks.first().map(String::as_str).unwrap_or("");
    w == "cd" || w == "pushd" || w == "popd"
}

fn is_git_command(sub: &str) -> bool {
    let toks = normalized_tokens(sub);
    if toks.first().map(String::as_str) == Some("git") {
        return true;
    }
    toks.first().map(String::as_str) == Some("xargs") && toks.iter().any(|t| t == "git")
}

fn is_git_internal_path(path: &str) -> bool {
    let normalized = LEADING_DOT_SLASH.replace(path, "");
    GIT_INTERNAL.iter().any(|re| re.is_match(&normalized))
}

fn is_cd_to_cwd(sub: &str, cwd: &str) -> bool {
    let parts: Vec<&str> = sub.split_whitespace().collect();
    if parts.first() != Some(&"cd") || parts.len() != 2 {
        return false;
    }
    let target = parts[1];
    let abs = if node_path::is_absolute(target) {
        node_path::resolve(&[target])
    } else {
        node_path::resolve(&[cwd, target])
    };
    abs == node_path::resolve(&[cwd])
}

fn is_inside_writable_roots(target: &str, cwd: &str, writable_roots: &[String]) -> bool {
    let abs = if node_path::is_absolute(target) {
        node_path::resolve(&[target])
    } else {
        node_path::resolve(&[cwd, target])
    };
    let mut roots = vec![cwd.to_string()];
    roots.extend(writable_roots.iter().cloned());
    for root in roots {
        let r = node_path::resolve(&[&root]);
        // The separator has to come from the same dialect that produced `abs`, or the
        // prefix never matches on Windows and every root looks like it contains nothing.
        if abs == r || abs.starts_with(&format!("{r}{}", node_path::SEPARATOR)) {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOME: &str = "/Users/alice";

    fn entry(content: &str) -> BashRuleEntry {
        BashRuleEntry {
            source: format!("Bash({content})"),
            content: Some(content.to_string()),
        }
    }

    fn sets(allow: &[&str], deny: &[&str], ask: &[&str]) -> BashRuleSets {
        BashRuleSets {
            allow: allow.iter().map(|c| entry(c)).collect(),
            deny: deny.iter().map(|c| entry(c)).collect(),
            ask: ask.iter().map(|c| entry(c)).collect(),
        }
    }

    fn opts() -> BashDecisionOptions {
        BashDecisionOptions {
            cwd: "/work".into(),
            writable_roots: Vec::new(),
            home_dir: Some(HOME.into()),
            is_read_only_subcommand: None,
            is_cwd_bare_repo: None,
        }
    }

    #[test]
    fn dangerous_removal_ask_even_with_allow_rule() {
        let d = evaluate_bash_command("rm -rf /", &sets(&["rm"], &[], &[]), &opts());
        assert_eq!(
            d,
            BashDecision::Ask {
                from_rule: false,
                dangerous_removal: true
            }
        );
    }

    #[test]
    fn deny_rule_wins_over_dangerous_removal() {
        let d = evaluate_bash_command("rm -rf /", &sets(&[], &["rm"], &[]), &opts());
        assert!(matches!(d, BashDecision::Deny { .. }));
    }

    #[test]
    fn dangerous_removal_inside_unsafe_compound() {
        let d = evaluate_bash_command("(rm -rf /)", &sets(&[], &[], &[]), &opts());
        assert_eq!(
            d,
            BashDecision::Ask {
                from_rule: false,
                dangerous_removal: true
            }
        );
    }

    #[test]
    fn safe_rm_allowed_by_rule() {
        let d = evaluate_bash_command("rm -rf ./build", &sets(&["rm"], &[], &[]), &opts());
        assert!(matches!(d, BashDecision::Allow { .. }));
    }
}
