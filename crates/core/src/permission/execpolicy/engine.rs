//! Static exec-policy classifier — a structured, rule-based matcher over
//! parsed shell commands.
//!
//! Rules are indexed by program (first token) for O(1) lookup. For a given
//! command the engine collects EVERY rule whose structured argument matcher
//! accepts the argv, then arbitrates:
//!
//!   1. MAX-AGGREGATION over severity — `forbidden` beats `prompt` beats `allow`.
//!   2. MOST-SPECIFIC-WINS on ties — among rules of equal (winning) severity, the
//!      one with the highest specificity score is reported as `matched`.
//!
//! Commands with shell operators (chaining / piping / redirection / command
//! substitution) always resolve to `prompt`. This engine NEVER executes
//! anything — it only classifies.

use std::collections::HashMap;

use crate::permission::execpolicy::matchers::{
    is_positive_integer, looks_like_option, pattern_token_matches, render_pattern_token,
    ArgMatcher, PatternToken,
};
use crate::permission::execpolicy::policy::{
    basename, current_platform, executable_lookup_key, parse_exec_policy, PolicyValidation,
};
use crate::permission::execpolicy::rules::{default_exec_rules, ExecDecision, ExecRule};
use crate::permission::shell_parse::{parse_command, ParsedCommand};

/// Construction options for [`ExecPolicy`] (beyond the rule list).
#[derive(Debug, Clone, Default)]
pub struct ExecPolicyOptions {
    /// basename → absolute paths permitted to resolve to that basename's rules.
    pub host_executables: HashMap<String, Vec<String>>,
    /// Enable absolute-path → basename fallback.
    pub resolve_host_executables: bool,
}

/// Result of evaluating one command against the policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecEvaluation {
    pub decision: ExecDecision,
    /// The winning rule that drove the decision (None when none matched).
    pub matched: Option<ExecRule>,
    /// Every rule that matched, in evaluation order (for diagnostics).
    pub matched_rules: Option<Vec<ExecRule>>,
    /// When an absolute program path resolved to a basename rule, the basename.
    pub resolved_program: Option<String>,
}

/// Severity ordering for max-aggregation: forbidden > prompt > allow.
fn severity(d: ExecDecision) -> i32 {
    match d {
        ExecDecision::Allow => 0,
        ExecDecision::Prompt => 1,
        ExecDecision::Forbidden => 2,
    }
}

#[derive(Debug)]
pub struct ExecPolicy {
    /// MultiMap: program → all rules keyed on that program.
    by_program: HashMap<String, Vec<ExecRule>>,
    /// basename → absolute paths allowed to resolve to that basename's rules.
    host_executables: HashMap<String, Vec<String>>,
    resolve_host_executables: bool,
}

/// Options for [`ExecPolicy::from_policy_text`].
#[derive(Debug, Clone, Default)]
pub struct FromPolicyTextOptions {
    pub identifier: Option<String>,
    pub extend_defaults: bool,
}

impl ExecPolicy {
    /// Build a policy from the default rule set.
    ///
    /// This inherent constructor is retained for source compatibility.
    #[allow(clippy::should_implement_trait)]
    pub fn default() -> Self {
        Self::new(default_exec_rules(), ExecPolicyOptions::default())
    }

    /// Build a policy from an explicit rule list plus options.
    pub fn new(rules: Vec<ExecRule>, opts: ExecPolicyOptions) -> ExecPolicy {
        let mut by_program: HashMap<String, Vec<ExecRule>> = HashMap::new();
        for rule in rules {
            by_program
                .entry(rule.program.clone())
                .or_default()
                .push(rule);
        }
        ExecPolicy {
            by_program,
            host_executables: opts.host_executables,
            resolve_host_executables: opts.resolve_host_executables,
        }
    }

    /// Build a policy from the text policy language and run load-time example
    /// validation, returning an error string on the first inconsistency.
    pub fn from_policy_text(text: &str, opts: FromPolicyTextOptions) -> Result<ExecPolicy, String> {
        let parsed =
            parse_exec_policy(text, opts.identifier.as_deref()).map_err(|e| e.to_string())?;
        let rules = if opts.extend_defaults {
            let mut r = default_exec_rules();
            r.extend(parsed.rules.clone());
            r
        } else {
            parsed.rules.clone()
        };
        let resolve = !parsed.host_executables.is_empty();
        let policy = ExecPolicy::new(
            rules,
            ExecPolicyOptions {
                host_executables: parsed.host_executables.clone(),
                resolve_host_executables: resolve,
            },
        );
        let mut problems = validate_policy_examples(&parsed.validations, &parsed.host_executables);
        problems.extend(policy.validate());
        if !problems.is_empty() {
            return Err(format!(
                "exec policy failed example validation:\n  {}",
                problems.join("\n  ")
            ));
        }
        Ok(policy)
    }

    pub fn evaluate(&self, command: &str) -> ExecEvaluation {
        let parsed = parse_command(command);

        // Conservative: anything that chains / pipes / redirects / substitutes → prompt.
        if parsed.has_operators {
            return ExecEvaluation {
                decision: ExecDecision::Prompt,
                matched: None,
                matched_rules: None,
                resolved_program: None,
            };
        }

        let candidates = self.match_candidates(&parsed);
        match candidates.best {
            None => ExecEvaluation {
                decision: ExecDecision::Prompt,
                matched: None,
                matched_rules: None,
                resolved_program: None,
            },
            Some(best) => ExecEvaluation {
                decision: best.decision,
                matched: Some(best),
                matched_rules: Some(candidates.matched),
                resolved_program: candidates.resolved_program,
            },
        }
    }

    /// How many rules accept `command`, ignoring shell-operator short-circuiting
    /// and the default-prompt fallback.
    pub fn match_count(&self, command: &str) -> usize {
        self.match_candidates(&parse_command(command)).matched.len()
    }

    fn match_candidates(&self, parsed: &ParsedCommand) -> MatchCandidates {
        let program = match &parsed.first_token {
            Some(p) => p.clone(),
            None => {
                return MatchCandidates {
                    matched: Vec::new(),
                    best: None,
                    resolved_program: None,
                }
            }
        };

        // Exact first-token rules first; on a miss, optionally fall back to basename.
        let mut candidates = self.by_program.get(&program);
        let mut resolved_program: Option<String> = None;
        if candidates.is_none_or(|c| c.is_empty()) && self.resolve_host_executables {
            if let Some(base) = self.basename_fallback(&program) {
                candidates = self.by_program.get(&base);
                resolved_program = Some(base);
            }
        }
        let candidates = match candidates {
            Some(c) if !c.is_empty() => c,
            _ => {
                return MatchCandidates {
                    matched: Vec::new(),
                    best: None,
                    resolved_program,
                }
            }
        };

        let args: Vec<String> = parsed.tokens.iter().skip(1).cloned().collect();
        let mut matched: Vec<ExecRule> = Vec::new();
        let mut best: Option<ExecRule> = None;
        let mut best_severity = -1;
        let mut best_specificity = -1;
        for rule in candidates {
            let result = match_rule(rule, &args);
            if !result.matched {
                continue;
            }
            matched.push(rule.clone());
            let sev = severity(rule.decision);
            if sev > best_severity
                || (sev == best_severity && result.specificity >= best_specificity)
            {
                best_severity = sev;
                best_specificity = result.specificity;
                best = Some(rule.clone());
            }
        }
        MatchCandidates {
            matched,
            best,
            resolved_program,
        }
    }

    /// Given an absolute program path, return the basename it may resolve to, or
    /// None if fallback is disallowed.
    fn basename_fallback(&self, program: &str) -> Option<String> {
        if !program.starts_with('/') {
            return None; // only absolute paths resolve
        }
        let base = executable_lookup_key(&basename(program), current_platform());
        if base == program {
            return None;
        }
        if let Some(allowed) = self.host_executables.get(&base) {
            if !allowed.iter().any(|p| p == program) {
                return None;
            }
        }
        Some(base)
    }

    /// Self-test the rule set: every `examples` entry must AGGREGATE to its rule's
    /// own `decision`, and every `not_examples` entry must NOT.
    pub fn validate(&self) -> Vec<String> {
        let mut problems: Vec<String> = Vec::new();
        // Deterministic ordering matches insertion order per program.
        for rules in self.by_program.values() {
            for rule in rules {
                let label = format_rule(rule);
                for ex in &rule.examples {
                    let got = self.evaluate(ex).decision;
                    if got != rule.decision {
                        problems.push(format!(
                            "example \"{}\" of rule {} resolved to '{}', expected '{}'",
                            ex,
                            label,
                            got.as_str(),
                            rule.decision.as_str()
                        ));
                    }
                }
                for ex in &rule.not_examples {
                    let got = self.evaluate(ex).decision;
                    if got == rule.decision {
                        problems.push(format!(
                            "notExample \"{}\" of rule {} resolved to '{}', which it must not",
                            ex,
                            label,
                            got.as_str()
                        ));
                    }
                }
            }
        }
        problems
    }
}

struct MatchCandidates {
    matched: Vec<ExecRule>,
    best: Option<ExecRule>,
    resolved_program: Option<String>,
}

/// Validate DSL `match`/`not_match` examples with PRESENCE semantics.
fn validate_policy_examples(
    validations: &[PolicyValidation],
    host_executables: &HashMap<String, Vec<String>>,
) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    for unit in validations {
        let subset = ExecPolicy::new(
            unit.rules.clone(),
            ExecPolicyOptions {
                host_executables: host_executables.clone(),
                resolve_host_executables: !host_executables.is_empty(),
            },
        );
        for ex in &unit.matches {
            if subset.match_count(ex) == 0 {
                problems.push(format!(
                    "match example \"{}\" did not match any rule of its directive",
                    ex
                ));
            }
        }
        for ex in &unit.not_matches {
            if subset.match_count(ex) > 0 {
                problems.push(format!(
                    "not_match example \"{}\" matched a rule of its directive",
                    ex
                ));
            }
        }
    }
    problems
}

/// Outcome of matching one rule's argument matcher against an argv.
struct MatchResult {
    matched: bool,
    /// Higher = more specific. Only meaningful when `matched` is true.
    specificity: i32,
}

const NO_MATCH: MatchResult = MatchResult {
    matched: false,
    specificity: 0,
};

/// Normalize a rule to its structured matcher list. `match_` wins; otherwise the
/// legacy `argv` flat-prefix is lifted to literal matchers plus a trailing rest.
fn rule_matchers(rule: &ExecRule) -> Vec<ArgMatcher> {
    if let Some(m) = &rule.match_ {
        return m.clone();
    }
    if let Some(argv) = &rule.argv {
        if !argv.is_empty() {
            let mut out: Vec<ArgMatcher> = argv
                .iter()
                .map(|tok| ArgMatcher::Literal {
                    token: PatternToken::Single(tok.clone()),
                })
                .collect();
            out.push(ArgMatcher::Rest {
                at_least_one: false,
            });
            return out;
        }
    }
    vec![ArgMatcher::Rest {
        at_least_one: false,
    }]
}

#[derive(Clone)]
enum OptionMatcher {
    Flag {
        names: PatternToken,
        required: bool,
    },
    Option {
        names: PatternToken,
        required: bool,
        value_is_pos_int: bool,
    },
}

impl OptionMatcher {
    fn names(&self) -> &PatternToken {
        match self {
            OptionMatcher::Flag { names, .. } => names,
            OptionMatcher::Option { names, .. } => names,
        }
    }
    fn required(&self) -> bool {
        match self {
            OptionMatcher::Flag { required, .. } => *required,
            OptionMatcher::Option { required, .. } => *required,
        }
    }
    fn is_option(&self) -> bool {
        matches!(self, OptionMatcher::Option { .. })
    }
}

#[derive(Clone)]
enum PositionalMatcher {
    Literal { token: PatternToken },
    Value,
    PosInt,
    Rest { at_least_one: bool },
}

/// Match a rule against an argv (tokens after the program).
fn match_rule(rule: &ExecRule, args: &[String]) -> MatchResult {
    let matchers = rule_matchers(rule);

    let option_matchers: Vec<OptionMatcher> = matchers
        .iter()
        .filter_map(|m| match m {
            ArgMatcher::Flag { names, required } => Some(OptionMatcher::Flag {
                names: names.clone(),
                required: *required,
            }),
            ArgMatcher::Option {
                names,
                required,
                value_is_pos_int,
            } => Some(OptionMatcher::Option {
                names: names.clone(),
                required: *required,
                value_is_pos_int: *value_is_pos_int,
            }),
            _ => None,
        })
        .collect();
    let positional_matchers: Vec<PositionalMatcher> = matchers
        .iter()
        .filter_map(|m| match m {
            ArgMatcher::Literal { token } => Some(PositionalMatcher::Literal {
                token: token.clone(),
            }),
            ArgMatcher::Value => Some(PositionalMatcher::Value),
            ArgMatcher::PosInt => Some(PositionalMatcher::PosInt),
            ArgMatcher::Rest { at_least_one } => Some(PositionalMatcher::Rest {
                at_least_one: *at_least_one,
            }),
            _ => None,
        })
        .collect();

    let mut positionals: Vec<String> = Vec::new();
    let mut seen_options: Vec<usize> = Vec::new();

    let strict_options = !option_matchers.is_empty() && !rule.ignore_unknown_flags;

    // ── Pass 1: consume option/flag tokens; accumulate positionals ─────────────
    let mut i = 0usize;
    while i < args.len() {
        let tok = &args[i];

        if looks_like_option(tok) {
            match find_option_matcher(&option_matchers, tok) {
                None => {
                    // Bundled short flags (e.g. `-la` → `-l -a`).
                    if let Some(bundled) = match_bundled_short_flags(&option_matchers, tok) {
                        for idx in bundled {
                            if !seen_options.contains(&idx) {
                                seen_options.push(idx);
                            }
                        }
                        i += 1;
                        continue;
                    }
                    if strict_options {
                        return NO_MATCH;
                    }
                    positionals.push(tok.clone());
                    i += 1;
                    continue;
                }
                Some(found) => {
                    if !seen_options.contains(&found.index) {
                        seen_options.push(found.index);
                    }
                    let matcher = &option_matchers[found.index];
                    if matcher.is_option() && found.inline_value.is_none() {
                        // Consume the following token as the option's value.
                        let next = args.get(i + 1);
                        match next {
                            None => return NO_MATCH,
                            Some(n) if looks_like_option(n) => return NO_MATCH,
                            Some(n) => {
                                if let OptionMatcher::Option {
                                    value_is_pos_int, ..
                                } = matcher
                                {
                                    if *value_is_pos_int && !is_positive_integer(n) {
                                        return NO_MATCH;
                                    }
                                }
                                i += 1; // skip value
                            }
                        }
                    } else if matcher.is_option() && found.inline_value.is_some() {
                        if let OptionMatcher::Option {
                            value_is_pos_int, ..
                        } = matcher
                        {
                            if *value_is_pos_int
                                && !is_positive_integer(found.inline_value.as_ref().unwrap())
                            {
                                return NO_MATCH;
                            }
                        }
                    }
                    i += 1;
                    continue;
                }
            }
        }

        positionals.push(tok.clone());
        i += 1;
    }

    // ── Required options present? ──────────────────────────────────────────────
    for (idx, om) in option_matchers.iter().enumerate() {
        if om.required() && !seen_options.contains(&idx) {
            return NO_MATCH;
        }
    }

    // ── Pass 2: resolve positionals against positional matchers ────────────────
    if !resolve_positionals(&positional_matchers, &positionals) {
        return NO_MATCH;
    }

    MatchResult {
        matched: true,
        specificity: specificity_of(&matchers),
    }
}

struct FoundOption {
    index: usize,
    /// Present when the token used the inline `--name=value` form.
    inline_value: Option<String>,
}

/// Try to interpret a `-xyz` token as a bundle of single-character short flags.
/// Returns the matched flag matcher indices iff EVERY bundled char maps to a
/// known short `flag` matcher; otherwise None.
fn match_bundled_short_flags(option_matchers: &[OptionMatcher], tok: &str) -> Option<Vec<usize>> {
    if !tok.starts_with('-')
        || tok.starts_with("--")
        || tok.chars().count() < 3
        || tok.contains('=')
    {
        return None;
    }
    let mut matched: Vec<usize> = Vec::new();
    for ch in tok.chars().skip(1) {
        let single = format!("-{}", ch);
        let found = option_matchers.iter().enumerate().find(|(_, m)| {
            matches!(m, OptionMatcher::Flag { .. }) && pattern_token_matches(m.names(), &single)
        });
        match found {
            Some((idx, _)) => matched.push(idx),
            None => return None,
        }
    }
    Some(matched)
}

/// Find the option/flag matcher index that accepts `tok` (supporting `--name=value`).
fn find_option_matcher(option_matchers: &[OptionMatcher], tok: &str) -> Option<FoundOption> {
    // Exact match first (covers flags and `option value` form).
    for (idx, m) in option_matchers.iter().enumerate() {
        if pattern_token_matches(m.names(), tok) {
            return Some(FoundOption {
                index: idx,
                inline_value: None,
            });
        }
    }
    // Inline `--name=value` / `-n=value` form (only valid for value-taking opts).
    if let Some(eq) = tok.find('=') {
        if eq > 0 {
            let name = &tok[..eq];
            let inline_value = tok[eq + 1..].to_string();
            for (idx, m) in option_matchers.iter().enumerate() {
                if m.is_option() && pattern_token_matches(m.names(), name) {
                    return Some(FoundOption {
                        index: idx,
                        inline_value: Some(inline_value),
                    });
                }
            }
        }
    }
    None
}

/// Resolve positional tokens against positional matchers.
fn resolve_positionals(matchers: &[PositionalMatcher], positionals: &[String]) -> bool {
    let rest_count = matchers
        .iter()
        .filter(|m| matches!(m, PositionalMatcher::Rest { .. }))
        .count();
    if rest_count > 1 {
        // Ill-formed rule; refuse to match.
        return false;
    }
    let rest_index = matchers
        .iter()
        .position(|m| matches!(m, PositionalMatcher::Rest { .. }));

    let rest_index = match rest_index {
        None => {
            // No wildcard: exact one-to-one positional match.
            if positionals.len() != matchers.len() {
                return false;
            }
            for (m, tok) in matchers.iter().zip(positionals.iter()) {
                if !positional_matches(m, tok) {
                    return false;
                }
            }
            return true;
        }
        Some(idx) => idx,
    };

    let prefix = &matchers[..rest_index];
    let rest_matcher = &matchers[rest_index];
    let suffix = &matchers[rest_index + 1..];
    let at_least_one = match rest_matcher {
        PositionalMatcher::Rest { at_least_one } => *at_least_one,
        _ => return false,
    };

    if positionals.len() < prefix.len() + suffix.len() {
        return false;
    }

    // Match the fixed prefix.
    for (m, tok) in prefix.iter().zip(positionals.iter()) {
        if !positional_matches(m, tok) {
            return false;
        }
    }

    // Match the fixed suffix (anchored to the end).
    let suffix_start = positionals.len() - suffix.len();
    for (offset, m) in suffix.iter().enumerate() {
        let tok = &positionals[suffix_start + offset];
        if !positional_matches(m, tok) {
            return false;
        }
    }

    // The middle tokens fall to the `rest`. Enforce >=1 when required.
    let middle_count = positionals.len() - prefix.len() - suffix.len();
    if at_least_one && middle_count < 1 {
        return false;
    }

    true
}

/// True when a single positional token satisfies a positional matcher.
fn positional_matches(m: &PositionalMatcher, tok: &str) -> bool {
    match m {
        PositionalMatcher::Literal { token } => pattern_token_matches(token, tok),
        PositionalMatcher::Value => true,
        PositionalMatcher::PosInt => is_positive_integer(tok),
        // A standalone rest is handled by the partitioner, not here.
        PositionalMatcher::Rest { .. } => true,
    }
}

/// Specificity score: concrete matchers are worth more than wildcards.
fn specificity_of(matchers: &[ArgMatcher]) -> i32 {
    let mut score = 0;
    for m in matchers {
        match m {
            ArgMatcher::Literal { token } => {
                score += match token {
                    PatternToken::Single(_) => 3,
                    PatternToken::Alts(_) => 2,
                };
            }
            ArgMatcher::Flag { required, .. } | ArgMatcher::Option { required, .. } => {
                score += if *required { 3 } else { 2 };
            }
            ArgMatcher::PosInt => score += 2,
            ArgMatcher::Value => score += 1,
            ArgMatcher::Rest { .. } => {}
        }
    }
    score
}

fn render_matcher(m: &ArgMatcher) -> String {
    match m {
        ArgMatcher::Literal { token } => render_pattern_token(token),
        ArgMatcher::Value => "<arg>".to_string(),
        ArgMatcher::PosInt => "<n>".to_string(),
        ArgMatcher::Flag { names, .. } => render_pattern_token(names),
        ArgMatcher::Option { names, .. } => format!("{} <val>", render_pattern_token(names)),
        ArgMatcher::Rest { at_least_one } => {
            if *at_least_one {
                "<args...>".to_string()
            } else {
                "[args...]".to_string()
            }
        }
    }
}

fn format_rule(rule: &ExecRule) -> String {
    let matchers: Vec<ArgMatcher> = if let Some(m) = &rule.match_ {
        m.clone()
    } else if let Some(argv) = &rule.argv {
        argv.iter()
            .map(|t| ArgMatcher::Literal {
                token: PatternToken::Single(t.clone()),
            })
            .collect()
    } else {
        Vec::new()
    };
    let rendered = matchers
        .iter()
        .map(render_matcher)
        .collect::<Vec<_>>()
        .join(" ");
    let args = if rendered.is_empty() {
        String::new()
    } else {
        format!(" {}", rendered)
    };
    format!("{}{} → {}", rule.program, args, rule.decision.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::permission::execpolicy::matchers::{
        flag, literal, option, pos_int, rest, rest_at_least_one, value, OptionOpts, PatternToken,
    };
    use crate::permission::execpolicy::policy::executable_lookup_key;
    use crate::permission::execpolicy::rules::default_exec_rules;

    trait DownstreamDefault {
        fn default() -> Self;
    }

    impl DownstreamDefault for ExecPolicy {
        fn default() -> Self {
            ExecPolicy::new(Vec::new(), ExecPolicyOptions::default())
        }
    }

    #[test]
    fn inherent_default_remains_unambiguous_for_downstream_callers() {
        let inherent = ExecPolicy::default();
        let downstream = <ExecPolicy as DownstreamDefault>::default();

        assert_eq!(
            inherent.evaluate("git status").decision,
            ExecDecision::Allow
        );
        assert_eq!(
            downstream.evaluate("git status").decision,
            ExecDecision::Prompt
        );
    }

    fn policy() -> ExecPolicy {
        ExecPolicy::default()
    }
    fn decide(cmd: &str) -> ExecDecision {
        policy().evaluate(cmd).decision
    }

    fn s(v: &str) -> PatternToken {
        PatternToken::single(v)
    }
    fn a(v: &[&str]) -> PatternToken {
        PatternToken::alts(v)
    }

    fn rule(program: &str, m: Vec<ArgMatcher>, decision: ExecDecision) -> ExecRule {
        ExecRule {
            program: program.to_string(),
            match_: Some(m),
            argv: None,
            decision,
            ignore_unknown_flags: false,
            justification: None,
            examples: Vec::new(),
            not_examples: Vec::new(),
        }
    }

    fn rule_just(program: &str, m: Vec<ArgMatcher>, decision: ExecDecision, j: &str) -> ExecRule {
        let mut r = rule(program, m, decision);
        r.justification = Some(j.to_string());
        r
    }

    fn rule_argv(program: &str, argv: &[&str], decision: ExecDecision) -> ExecRule {
        ExecRule {
            program: program.to_string(),
            match_: None,
            argv: Some(argv.iter().map(|s| s.to_string()).collect()),
            decision,
            ignore_unknown_flags: false,
            justification: None,
            examples: Vec::new(),
            not_examples: Vec::new(),
        }
    }

    // ── ExecPolicy.validate (shipped ruleset) ──────────────────────────────────
    #[test]
    fn reports_zero_problems_for_default_exec_rules() {
        let problems = policy().validate();
        assert_eq!(problems, Vec::<String>::new());
    }

    #[test]
    fn exercises_every_example_and_not_example() {
        let mut examples = 0;
        let mut not_examples = 0;
        for rule in default_exec_rules() {
            examples += rule.examples.len();
            not_examples += rule.not_examples.len();
        }
        assert!(examples > 60);
        assert!(not_examples > 8);
    }

    // ── operator guard ──────────────────────────────────────────────────────
    #[test]
    fn operator_guard_forces_prompt() {
        for cmd in &[
            "ls && rm file",
            "cat a | grep b",
            "echo hi > out.txt",
            "echo $(whoami)",
            "ls; ls",
            "ls &",
            "cat < in.txt",
        ] {
            assert_eq!(decide(cmd), ExecDecision::Prompt, "cmd={}", cmd);
        }
    }

    #[test]
    fn bare_glob_is_not_an_operator() {
        assert_eq!(decide("ls *.ts"), ExecDecision::Allow);
    }

    // ── unknown program / empty command ──────────────────────────────────────
    #[test]
    fn prompts_for_unknown_program() {
        assert_eq!(decide("frobnicate --do-it"), ExecDecision::Prompt);
    }

    #[test]
    fn prompts_for_empty_command() {
        assert_eq!(decide(""), ExecDecision::Prompt);
        assert_eq!(decide("   "), ExecDecision::Prompt);
    }

    // ── allow tier ───────────────────────────────────────────────────────────
    #[test]
    fn allow_tier_read_only_programs() {
        for cmd in &[
            "ls",
            "ls -la /tmp",
            "cat package.json",
            "cat -n file",
            "head -n 20 file",
            "head file",
            "tail -f log",
            "tail -n 100 file",
            "pwd",
            "echo hello",
            "printf %s x",
            "grep -rn foo src",
            "rg --json pattern",
            "rg --files --max-depth 2 .",
            "find . -name \"*.ts\"",
            "find src -maxdepth 2",
            "wc -l file",
            "which node",
            "sort file",
            "diff a b",
            "realpath .",
            "date",
            "whoami",
        ] {
            assert_eq!(decide(cmd), ExecDecision::Allow, "cmd={}", cmd);
        }
    }

    // ── option-with-value matching ───────────────────────────────────────────
    #[test]
    fn accepts_option_value_as_following_token() {
        assert_eq!(decide("head -n 20 file"), ExecDecision::Allow);
    }

    #[test]
    fn accepts_inline_name_value_form() {
        assert_eq!(decide("head -n=5 file"), ExecDecision::Allow);
        assert_eq!(decide("rg --max-depth=2 ."), ExecDecision::Allow);
    }

    #[test]
    fn rejects_non_pos_int_value_for_pos_int_option() {
        assert_eq!(decide("head -n abc file"), ExecDecision::Prompt);
    }

    #[test]
    fn rejects_option_followed_by_option_where_value_required() {
        assert_eq!(decide("head -n -c file"), ExecDecision::Prompt);
    }

    // ── prompt tier ──────────────────────────────────────────────────────────
    #[test]
    fn prompt_tier_mutating_network_process() {
        for cmd in &[
            "rm file",
            "rm -r dir",
            "rm -rf ./build",
            "mv a b",
            "cp -r src dst",
            "chmod +x script",
            "mkdir newdir",
            "touch file",
            "kill -9 123",
            "pkill node",
            "curl https://example.test",
            "wget https://example.test",
            "ssh host",
            "scp a host:b",
            "rsync a b",
            "sudo ls",
            "sh script.sh",
            "bash deploy.sh",
            "tee out.txt",
        ] {
            assert_eq!(decide(cmd), ExecDecision::Prompt, "cmd={}", cmd);
        }
    }

    // ── git subcommand arbitration ───────────────────────────────────────────
    #[test]
    fn git_allows_read_only_subcommands() {
        assert_eq!(decide("git status"), ExecDecision::Allow);
        assert_eq!(decide("git status --short"), ExecDecision::Allow);
        assert_eq!(decide("git diff HEAD"), ExecDecision::Allow);
        assert_eq!(decide("git log --oneline"), ExecDecision::Allow);
        assert_eq!(decide("git show HEAD"), ExecDecision::Allow);
        assert_eq!(decide("git branch"), ExecDecision::Allow);
        assert_eq!(decide("git branch -a"), ExecDecision::Allow);
        assert_eq!(decide("git remote -v"), ExecDecision::Allow);
    }

    #[test]
    fn git_escalates_network_history_subcommands() {
        assert_eq!(decide("git push"), ExecDecision::Prompt);
        assert_eq!(decide("git push origin main"), ExecDecision::Prompt);
        assert_eq!(decide("git pull"), ExecDecision::Prompt);
        assert_eq!(decide("git commit -m x"), ExecDecision::Prompt);
        assert_eq!(decide("git merge feature"), ExecDecision::Prompt);
        assert_eq!(decide("git reset --hard"), ExecDecision::Prompt);
        assert_eq!(decide("git checkout main"), ExecDecision::Prompt);
    }

    #[test]
    fn git_escalates_branch_delete_over_branch_allow() {
        assert_eq!(decide("git branch"), ExecDecision::Allow);
        assert_eq!(decide("git branch -D feature"), ExecDecision::Prompt);
        assert_eq!(decide("git branch -d old"), ExecDecision::Prompt);
    }

    #[test]
    fn git_reports_most_specific_matching_rule() {
        let ev = policy().evaluate("git push origin main");
        assert_eq!(ev.decision, ExecDecision::Prompt);
        let matched = ev.matched.unwrap();
        assert_eq!(matched.program, "git");
        assert_eq!(
            matched.match_.unwrap()[0],
            literal(a(&["push", "pull", "fetch", "clone"]))
        );
    }

    // ── rm flag combinations ─────────────────────────────────────────────────
    #[test]
    fn rm_forbids_recursive_force_delete_root_bundled() {
        assert_eq!(decide("rm -rf /"), ExecDecision::Forbidden);
        assert_eq!(decide("rm -fr /"), ExecDecision::Forbidden);
    }

    #[test]
    fn rm_forbids_recursive_force_delete_root_split() {
        assert_eq!(decide("rm -r -f /"), ExecDecision::Forbidden);
        assert_eq!(decide("rm -f -r /"), ExecDecision::Forbidden);
    }

    #[test]
    fn rm_forbids_no_preserve_root_any_position() {
        assert_eq!(
            decide("rm -rf --no-preserve-root /"),
            ExecDecision::Forbidden
        );
        assert_eq!(
            decide("rm --no-preserve-root -rf /"),
            ExecDecision::Forbidden
        );
    }

    #[test]
    fn rm_does_not_forbid_non_root_target() {
        assert_eq!(decide("rm -rf ./build"), ExecDecision::Prompt);
        assert_eq!(decide("rm -rf /tmp/x"), ExecDecision::Prompt);
        assert_eq!(decide("rm -r -f /tmp"), ExecDecision::Prompt);
    }

    // ── find destructive forms ───────────────────────────────────────────────
    #[test]
    fn find_allows_read_only_traversal() {
        assert_eq!(decide("find . -name \"*.ts\""), ExecDecision::Allow);
        assert_eq!(decide("find . -type f"), ExecDecision::Allow);
    }

    #[test]
    fn find_escalates_delete_to_prompt() {
        assert_eq!(
            decide("find . -name \"*.tmp\" -delete"),
            ExecDecision::Prompt
        );
        assert_eq!(decide("find . -delete"), ExecDecision::Prompt);
    }

    #[test]
    fn find_escalates_exec_to_prompt() {
        assert_eq!(
            decide("find . -name x -execdir cat foo bar"),
            ExecDecision::Prompt
        );
    }

    // ── forbidden tier ───────────────────────────────────────────────────────
    #[test]
    fn forbids_filesystem_formatting() {
        assert_eq!(decide("mkfs.ext4 /dev/sda"), ExecDecision::Forbidden);
        assert_eq!(decide("mkfs /dev/sda1"), ExecDecision::Forbidden);
    }

    // ── decision arbitration ─────────────────────────────────────────────────
    #[test]
    fn max_aggregates_across_matching_rules() {
        let custom = ExecPolicy::new(
            vec![
                rule("tool", vec![rest()], ExecDecision::Allow),
                rule(
                    "tool",
                    vec![literal(s("danger")), rest()],
                    ExecDecision::Prompt,
                ),
                rule(
                    "tool",
                    vec![literal(s("danger")), literal(s("now"))],
                    ExecDecision::Forbidden,
                ),
            ],
            ExecPolicyOptions::default(),
        );
        assert_eq!(custom.evaluate("tool safe").decision, ExecDecision::Allow);
        assert_eq!(
            custom.evaluate("tool danger later").decision,
            ExecDecision::Prompt
        );
        assert_eq!(
            custom.evaluate("tool danger now").decision,
            ExecDecision::Forbidden
        );
    }

    #[test]
    fn on_equal_severity_more_specific_rule_reported() {
        let custom = ExecPolicy::new(
            vec![
                rule_just("p", vec![rest()], ExecDecision::Prompt, "broad"),
                rule_just(
                    "p",
                    vec![literal(s("sub")), rest()],
                    ExecDecision::Prompt,
                    "specific",
                ),
            ],
            ExecPolicyOptions::default(),
        );
        let ev = custom.evaluate("p sub x");
        assert_eq!(ev.decision, ExecDecision::Prompt);
        assert_eq!(
            ev.matched.unwrap().justification.as_deref(),
            Some("specific")
        );
        assert_eq!(ev.matched_rules.unwrap().len(), 2);
    }

    // ── backward compatibility — legacy argv flat-prefix ─────────────────────
    #[test]
    fn legacy_argv_prefix_matches() {
        let custom = ExecPolicy::new(
            vec![
                rule_argv("svc", &["start"], ExecDecision::Allow),
                rule_argv("svc", &["stop"], ExecDecision::Prompt),
            ],
            ExecPolicyOptions::default(),
        );
        assert_eq!(
            custom.evaluate("svc start now").decision,
            ExecDecision::Allow
        );
        assert_eq!(custom.evaluate("svc stop").decision, ExecDecision::Prompt);
        assert_eq!(
            custom.evaluate("svc restart").decision,
            ExecDecision::Prompt
        );
    }

    #[test]
    fn argv_prefix_allows_extra_trailing_tokens() {
        let custom = ExecPolicy::new(
            vec![rule_argv("k", &["a", "b"], ExecDecision::Allow)],
            ExecPolicyOptions::default(),
        );
        assert_eq!(custom.evaluate("k a b c d").decision, ExecDecision::Allow);
        assert_eq!(custom.evaluate("k a").decision, ExecDecision::Prompt);
    }

    // ── public API surface ───────────────────────────────────────────────────
    #[test]
    fn exposes_evaluation_shape() {
        let ev = policy().evaluate("ls");
        // `decision` accessible.
        let _ = ev.decision;
    }

    #[test]
    fn default_exec_rules_covers_all_three_tiers() {
        let tiers: std::collections::HashSet<ExecDecision> =
            default_exec_rules().iter().map(|r| r.decision).collect();
        assert!(tiers.contains(&ExecDecision::Allow));
        assert!(tiers.contains(&ExecDecision::Prompt));
        assert!(tiers.contains(&ExecDecision::Forbidden));
    }

    #[test]
    fn custom_rule_typechecks_with_matcher_constructors() {
        let r = rule(
            "demo",
            vec![
                flag(a(&["-v", "--verbose"]), false),
                option(s("-o"), OptionOpts::default()),
                pos_int(),
                value(),
                rest_at_least_one(),
            ],
            ExecDecision::Allow,
        );
        let custom = ExecPolicy::new(vec![r], ExecPolicyOptions::default());
        assert_eq!(
            custom.evaluate("demo -v -o out 5 thing extra").decision,
            ExecDecision::Allow
        );
    }

    // ── matcher primitives ───────────────────────────────────────────────────
    #[test]
    fn pattern_token_matches_primitive() {
        use crate::permission::execpolicy::matchers::pattern_token_matches;
        assert!(pattern_token_matches(&s("a"), "a"));
        assert!(!pattern_token_matches(&s("a"), "b"));
        assert!(pattern_token_matches(&a(&["a", "b"]), "b"));
        assert!(!pattern_token_matches(&a(&["a", "b"]), "c"));
    }

    #[test]
    fn is_positive_integer_primitive() {
        use crate::permission::execpolicy::matchers::is_positive_integer;
        assert!(is_positive_integer("1"));
        assert!(is_positive_integer("20"));
        assert!(!is_positive_integer("0"));
        assert!(!is_positive_integer("-1"));
        assert!(!is_positive_integer("1.5"));
        assert!(!is_positive_integer("x"));
        assert!(is_positive_integer("007"));
    }

    #[test]
    fn looks_like_option_primitive() {
        use crate::permission::execpolicy::matchers::looks_like_option;
        assert!(looks_like_option("-r"));
        assert!(looks_like_option("--recursive"));
        assert!(looks_like_option("--name=value"));
        assert!(!looks_like_option("-"));
        assert!(!looks_like_option("--"));
        assert!(!looks_like_option("file"));
    }

    // ── rules with required options ──────────────────────────────────────────
    #[test]
    fn required_flag_only_matches_when_present() {
        let custom = ExecPolicy::new(
            vec![
                rule(
                    "q",
                    vec![flag(s("--force"), true), rest()],
                    ExecDecision::Forbidden,
                ),
                rule("q", vec![rest()], ExecDecision::Allow),
            ],
            ExecPolicyOptions::default(),
        );
        assert_eq!(
            custom.evaluate("q --force x").decision,
            ExecDecision::Forbidden
        );
        assert_eq!(custom.evaluate("q x").decision, ExecDecision::Allow);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // parsing & compilation via ExecPolicy::from_policy_text
    // ═══════════════════════════════════════════════════════════════════════

    fn load(text: &str) -> ExecPolicy {
        ExecPolicy::from_policy_text(
            text,
            FromPolicyTextOptions {
                identifier: Some("test.rules".to_string()),
                extend_defaults: false,
            },
        )
        .unwrap()
    }

    #[test]
    fn compiles_prefix_rule_to_prefix_match() {
        let policy = load(
            r#"
              # a comment
              prefix_rule(pattern = ["git", "status"], decision = "allow")
            "#,
        );
        assert_eq!(policy.evaluate("git status").decision, ExecDecision::Allow);
        assert_eq!(
            policy.evaluate("git status --short").decision,
            ExecDecision::Allow
        );
        assert_eq!(
            policy.evaluate("git status -s -b").decision,
            ExecDecision::Allow
        );
        assert_eq!(policy.evaluate("git push").decision, ExecDecision::Prompt);
        assert_eq!(policy.evaluate("git log").decision, ExecDecision::Prompt);
    }

    #[test]
    fn decision_defaults_to_allow_when_omitted() {
        let policy = load(r#"prefix_rule(pattern = ["echo"])"#);
        assert_eq!(policy.evaluate("echo hi").decision, ExecDecision::Allow);
    }

    #[test]
    fn expands_first_token_alternatives() {
        let policy =
            load(r#"prefix_rule(pattern = [["git", "jj"], "status"], decision = "allow")"#);
        assert_eq!(policy.evaluate("git status").decision, ExecDecision::Allow);
        assert_eq!(policy.evaluate("jj status").decision, ExecDecision::Allow);
        assert_eq!(policy.evaluate("hg status").decision, ExecDecision::Prompt);
    }

    #[test]
    fn supports_alternatives_in_non_leading_pattern_token() {
        let policy =
            load(r#"prefix_rule(pattern = ["pkg", ["install", "add"]], decision = "prompt")"#);
        assert_eq!(
            policy.evaluate("pkg install lodash").decision,
            ExecDecision::Prompt
        );
        assert_eq!(
            policy.evaluate("pkg add lodash").decision,
            ExecDecision::Prompt
        );
        assert_eq!(
            policy.evaluate("pkg remove lodash").decision,
            ExecDecision::Prompt
        );
        assert_eq!(policy.evaluate("pkg list").decision, ExecDecision::Prompt);
    }

    #[test]
    fn classifies_forbidden_invocations() {
        let policy = load(
            r#"
              prefix_rule(pattern = ["ls"], decision = "allow")
              prefix_rule(
                pattern = ["rm", "-rf", "/"],
                decision = "forbidden",
                justification = "Deleting the filesystem root is catastrophic.",
              )
            "#,
        );
        assert_eq!(
            policy.evaluate("rm -rf /").decision,
            ExecDecision::Forbidden
        );
        let evald = policy.evaluate("rm -rf /");
        assert!(evald
            .matched
            .unwrap()
            .justification
            .unwrap()
            .contains("catastrophic"));
        assert_eq!(policy.evaluate("ls -la").decision, ExecDecision::Allow);
    }

    #[test]
    fn accepts_quote_styles_trailing_commas_bools() {
        let policy = load(
            r#"
              prefix_rule(
                pattern = ['npm', 'ci'],
                decision = "allow",
              )
            "#,
        );
        assert_eq!(policy.evaluate("npm ci").decision, ExecDecision::Allow);
    }

    #[test]
    fn binds_pattern_positionally() {
        let policy = load(r#"prefix_rule(["cargo", "build"], "allow")"#);
        assert_eq!(
            policy.evaluate("cargo build --release").decision,
            ExecDecision::Allow
        );
    }

    // ── load-time example validation ─────────────────────────────────────────
    #[test]
    fn accepts_consistent_match_not_match_examples() {
        let policy = load(
            r#"
              prefix_rule(
                pattern = [["git", "jj"], "status"],
                decision = "allow",
                match = [["git", "status"], "jj status --color"],
                not_match = [["git", "push"]],
              )
            "#,
        );
        assert_eq!(policy.evaluate("git status").decision, ExecDecision::Allow);
        assert_eq!(policy.evaluate("jj status").decision, ExecDecision::Allow);
    }

    #[test]
    fn rejects_match_example_not_matching_rule() {
        let err = ExecPolicy::from_policy_text(
            r#"
              prefix_rule(pattern = ["git", "push"], decision = "allow", match = [["git", "status"]])
            "#,
            FromPolicyTextOptions::default(),
        )
        .unwrap_err();
        assert!(err.contains("example validation"));
    }

    #[test]
    fn rejects_not_match_example_that_matches() {
        let err = ExecPolicy::from_policy_text(
            r#"
              prefix_rule(pattern = ["git", "status"], decision = "allow", not_match = [["git", "status"]])
            "#,
            FromPolicyTextOptions::default(),
        )
        .unwrap_err();
        assert!(err.contains("example validation"));
    }

    // ── host_executable basename fallback ────────────────────────────────────
    fn host_exec_text() -> &'static str {
        r#"
          prefix_rule(pattern = ["git", "status"], decision = "allow")
          prefix_rule(pattern = ["ls"], decision = "allow")
          host_executable(name = "git", paths = ["/usr/bin/git", "/opt/homebrew/bin/git"])
        "#
    }

    #[test]
    fn resolves_listed_absolute_path_to_basename_rule() {
        let policy = load(host_exec_text());
        let evald = policy.evaluate("/usr/bin/git status");
        assert_eq!(evald.decision, ExecDecision::Allow);
        assert_eq!(evald.resolved_program.as_deref(), Some("git"));
    }

    #[test]
    fn refuses_fallback_for_path_not_in_allowlist() {
        let policy = load(host_exec_text());
        assert_eq!(
            policy.evaluate("/opt/evil/git status").decision,
            ExecDecision::Prompt
        );
    }

    #[test]
    fn allows_free_basename_fallback_when_no_entry() {
        let policy = load(host_exec_text());
        assert_eq!(policy.evaluate("/bin/ls -la").decision, ExecDecision::Allow);
    }

    #[test]
    fn does_not_resolve_basenames_without_host_executable() {
        let policy = load(r#"prefix_rule(pattern = ["git", "status"], decision = "allow")"#);
        assert_eq!(
            policy.evaluate("/usr/bin/git status").decision,
            ExecDecision::Prompt
        );
    }

    // ── PRESENCE validation semantics ────────────────────────────────────────
    #[test]
    fn validates_match_example_against_own_directive_only() {
        let policy = load(
            r#"
              prefix_rule(pattern = ["git"], decision = "allow", match = [["git", "push"]])
              prefix_rule(pattern = ["git", "push"], decision = "forbidden")
            "#,
        );
        assert_eq!(
            policy.evaluate("git push").decision,
            ExecDecision::Forbidden
        );
        assert_eq!(policy.evaluate("git status").decision, ExecDecision::Allow);
    }

    #[test]
    fn match_example_hitting_forbidden_rule_still_passes() {
        assert!(ExecPolicy::from_policy_text(
            r#"prefix_rule(pattern = ["rm", "-rf"], decision = "forbidden", match = [["rm", "-rf", "/"]])"#,
            FromPolicyTextOptions::default(),
        )
        .is_ok());
    }

    #[test]
    fn still_rejects_match_example_matching_no_rule() {
        let err = ExecPolicy::from_policy_text(
            r#"prefix_rule(pattern = ["git", "push"], decision = "allow", match = [["git", "status"]])"#,
            FromPolicyTextOptions::default(),
        )
        .unwrap_err();
        assert!(err.contains("did not match"));
    }

    #[test]
    fn still_rejects_not_match_example_that_matches() {
        let err = ExecPolicy::from_policy_text(
            r#"prefix_rule(pattern = ["git", "status"], not_match = [["git", "status"]])"#,
            FromPolicyTextOptions::default(),
        )
        .unwrap_err();
        assert!(err.contains("matched a rule"));
    }

    #[test]
    fn shell_operators_in_string_example_do_not_short_circuit() {
        assert!(ExecPolicy::from_policy_text(
            r#"prefix_rule(pattern = ["git", "status"], decision = "allow", match = ["git status && echo ok"])"#,
            FromPolicyTextOptions::default(),
        )
        .is_ok());
    }

    // ── Starlark subset ──────────────────────────────────────────────────────
    #[test]
    fn binds_and_references_variables() {
        let policy = load(
            r#"
              verb = "status"
              prefix_rule(pattern = ["git", verb], decision = "allow")
            "#,
        );
        assert_eq!(policy.evaluate("git status").decision, ExecDecision::Allow);
        assert_eq!(policy.evaluate("git push").decision, ExecDecision::Prompt);
    }

    #[test]
    fn generates_rules_with_list_comprehension() {
        let policy = load(
            r#"
              safe = ["status", "log", "diff"]
              [prefix_rule(pattern = ["git", sub], decision = "allow") for sub in safe]
            "#,
        );
        assert_eq!(policy.evaluate("git status").decision, ExecDecision::Allow);
        assert_eq!(policy.evaluate("git log").decision, ExecDecision::Allow);
        assert_eq!(
            policy.evaluate("git diff --stat").decision,
            ExecDecision::Allow
        );
        assert_eq!(policy.evaluate("git push").decision, ExecDecision::Prompt);
    }

    #[test]
    fn supports_comprehension_guard_on_truthiness() {
        let policy = load(
            r#"
              subs = ["status", "", "log"]
              [prefix_rule(pattern = ["git", s], decision = "allow") for s in subs if s]
            "#,
        );
        assert_eq!(policy.evaluate("git status").decision, ExecDecision::Allow);
        assert_eq!(policy.evaluate("git log").decision, ExecDecision::Allow);
    }

    #[test]
    fn interpolates_string_variables_in_fstring() {
        let policy = load(
            r#"
              who = "release"
              prefix_rule(pattern = ["deploy"], decision = "forbidden", justification = f"blocked for {who}")
            "#,
        );
        assert_eq!(
            policy
                .evaluate("deploy now")
                .matched
                .unwrap()
                .justification
                .as_deref(),
            Some("blocked for release")
        );
    }

    #[test]
    fn concatenates_lists_and_strings() {
        let policy = load(
            r#"
              base = ["status"]
              prefix_rule(pattern = ["git"] + base, decision = "allow")
              prefix_rule(pattern = ["echo", "a" + "b"], decision = "allow")
            "#,
        );
        assert_eq!(policy.evaluate("git status").decision, ExecDecision::Allow);
        assert_eq!(policy.evaluate("echo ab").decision, ExecDecision::Allow);
        assert_eq!(policy.evaluate("echo a").decision, ExecDecision::Prompt);
    }

    // ── executableLookupKey (re-exported through policy) ──────────────────────
    #[test]
    fn executable_lookup_key_via_policy_module() {
        assert_eq!(executable_lookup_key("GIT.EXE", "win32"), "git");
        assert_eq!(executable_lookup_key("git", "darwin"), "git");
    }
}
