//! Session-scoped permission-rule accumulation + the "always allow" suggestion.

use crate::permission::bash::split::{
    extract_output_redirections, is_command_list, is_unsafe_compound_command, split_bash_command,
};
use crate::runtime_values::RuntimeString;
use crate::types::permission::{
    PermissionBehavior, PermissionDestination, PermissionRule, PermissionUpdate,
};
use regex::Regex;
use std::sync::LazyLock;

/// Command-injection markers (mirrors decision.rs): command/process
/// substitution and parameter expansion. Their presence means we cannot derive a
/// safe verbatim allow rule, so no suggestion is offered.
static INJECTION_MARKERS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\$\(|`|\$\{|<\(|>\(").unwrap());

/// True if `s` contains a control char or exotic whitespace the bash splitter
/// would normalize to a space. Plain space (0x20), tab (0x09), and LF (0x0A) are
/// allowed.
fn has_exotic_whitespace(s: &str) -> bool {
    for c in s.chars() {
        let code = c as u32;
        if code == 0x09 || code == 0x0a {
            continue;
        }
        if code < 0x20 || code == 0x7f || code == 0x85 || code == 0x2028 || code == 0x2029 {
            return true;
        }
    }
    false
}

/// The session-scoped, MUTABLE permission rules a Session accumulates as the user
/// approves "always allow" prompts. `apply_add_rules` pushes into these vectors
/// in place so a mid-turn approval is visible to the next tool call in the same
/// turn.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionPermissionState {
    pub allow_rules: Vec<PermissionRule>,
    pub deny_rules: Vec<PermissionRule>,
    pub ask_rules: Vec<PermissionRule>,
}

/// A fresh, empty session permission state.
pub fn empty_session_permission_state() -> SessionPermissionState {
    SessionPermissionState::default()
}

/// Semantic identity of a rule for dedup (mirrors rule_matches: lowercase tool,
/// null≡'' prefix).
fn rule_key(rule: &PermissionRule) -> (RuntimeString, RuntimeString) {
    let prefix = match &rule.argument_prefix {
        Some(p) if !p.is_empty() => p.clone(),
        _ => RuntimeString::default(),
    };
    (lowercase_runtime(&rule.tool_name), prefix)
}

fn lowercase_runtime(value: &RuntimeString) -> RuntimeString {
    let mut units = Vec::with_capacity(value.len_utf16());
    for point in value.code_points() {
        if let Some(character) = char::from_u32(point) {
            for lowercase in character.to_lowercase() {
                let mut encoded = [0_u16; 2];
                units.extend_from_slice(lowercase.encode_utf16(&mut encoded));
            }
        } else {
            units.push(point as u16);
        }
    }
    RuntimeString::from_utf16(units)
}

/// Apply an `addRules` update to the session state, mutating IN PLACE (push), and
/// deduping by `(tool_name, argument_prefix)`. The behavior selects which class
/// the rule joins. `setMode` updates are ignored here.
pub fn apply_add_rules(state: &mut SessionPermissionState, update: &PermissionUpdate) {
    let (rules, behavior) = match update {
        PermissionUpdate::AddRules {
            rules, behavior, ..
        } => (rules, *behavior),
        PermissionUpdate::SetMode { .. } => return,
    };
    let target = match behavior {
        PermissionBehavior::Allow => &mut state.allow_rules,
        PermissionBehavior::Deny => &mut state.deny_rules,
        PermissionBehavior::Ask => &mut state.ask_rules,
    };
    for rule in rules {
        let key = rule_key(rule);
        if !target.iter().any(|r| rule_key(r) == key) {
            target.push(rule.clone());
        }
    }
}

/// Auto-generate the permission rule to offer when a tool call is about to ask
/// (the "always allow" suggestion). The suggested rule must never be broader than
/// what the user is approving.
///
///  - **Bash**: only a SINGLE, plain, redirect-free command earns a suggestion,
///    and the rule prefix is the command VERBATIM. A compound, injection,
///    heredoc, output redirection, or unparseable command yields NO suggestion.
///  - **Other tools**: a bare tool-name rule.
pub fn suggest_rule_for_ask(tool_name: &str, command: Option<&str>) -> Option<PermissionUpdate> {
    if tool_name.to_lowercase() == "bash" {
        let command = command?;
        let trimmed = command.trim();
        if trimmed.is_empty() {
            return None;
        }
        if has_exotic_whitespace(trimmed) {
            return None;
        }
        if INJECTION_MARKERS.is_match(trimmed) {
            return None;
        }
        if !is_command_list(trimmed) {
            return None;
        }
        if is_unsafe_compound_command(trimmed) {
            return None;
        }
        let parts = split_bash_command(trimmed);
        if parts.len() != 1 {
            return None;
        }
        let redir = extract_output_redirections(trimmed);
        if !redir.redirections.is_empty() || redir.has_dangerous_redirection {
            return None;
        }
        let prefix = parts[0].trim();
        if prefix.is_empty() {
            return None;
        }
        return Some(PermissionUpdate::AddRules {
            rules: vec![PermissionRule {
                tool_name: "bash".into(),
                argument_prefix: Some(prefix.into()),
                tool_name_prefix: None,
            }],
            behavior: PermissionBehavior::Allow,
            destination: PermissionDestination::Session,
        });
    }
    Some(PermissionUpdate::AddRules {
        rules: vec![PermissionRule {
            tool_name: tool_name.into(),
            argument_prefix: None,
            tool_name_prefix: None,
        }],
        behavior: PermissionBehavior::Allow,
        destination: PermissionDestination::Session,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn add_allow(tool: &str, prefix: Option<&str>) -> PermissionUpdate {
        PermissionUpdate::AddRules {
            rules: vec![PermissionRule {
                tool_name: RuntimeString::from(tool),
                argument_prefix: prefix.map(RuntimeString::from),
                tool_name_prefix: None,
            }],
            behavior: PermissionBehavior::Allow,
            destination: PermissionDestination::Session,
        }
    }

    fn rule(tool: &str, prefix: Option<&str>) -> PermissionRule {
        PermissionRule {
            tool_name: RuntimeString::from(tool),
            argument_prefix: prefix.map(RuntimeString::from),
            tool_name_prefix: None,
        }
    }

    #[test]
    fn routes_rule_by_behavior() {
        let mut s = empty_session_permission_state();
        apply_add_rules(
            &mut s,
            &PermissionUpdate::AddRules {
                rules: vec![rule("write", None)],
                behavior: PermissionBehavior::Allow,
                destination: PermissionDestination::Session,
            },
        );
        apply_add_rules(
            &mut s,
            &PermissionUpdate::AddRules {
                rules: vec![rule("bash", Some("rm"))],
                behavior: PermissionBehavior::Deny,
                destination: PermissionDestination::Session,
            },
        );
        apply_add_rules(
            &mut s,
            &PermissionUpdate::AddRules {
                rules: vec![rule("edit", None)],
                behavior: PermissionBehavior::Ask,
                destination: PermissionDestination::Session,
            },
        );
        assert_eq!(s.allow_rules, vec![rule("write", None)]);
        assert_eq!(s.deny_rules, vec![rule("bash", Some("rm"))]);
        assert_eq!(s.ask_rules, vec![rule("edit", None)]);
    }

    #[test]
    fn dedupes_by_tool_and_prefix() {
        let mut s = empty_session_permission_state();
        apply_add_rules(&mut s, &add_allow("bash", Some("git status")));
        apply_add_rules(&mut s, &add_allow("bash", Some("git status")));
        apply_add_rules(&mut s, &add_allow("bash", Some("git status -sb")));
        assert_eq!(
            s.allow_rules,
            vec![
                rule("bash", Some("git status")),
                rule("bash", Some("git status -sb"))
            ]
        );
    }

    #[test]
    fn dedupes_case_insensitively_on_tool() {
        let mut s = empty_session_permission_state();
        apply_add_rules(&mut s, &add_allow("Write", None));
        apply_add_rules(&mut s, &add_allow("write", None));
        assert_eq!(s.allow_rules.len(), 1);
    }

    #[test]
    fn undefined_and_empty_prefix_are_same() {
        let mut s = empty_session_permission_state();
        apply_add_rules(&mut s, &add_allow("write", None));
        apply_add_rules(&mut s, &add_allow("write", Some("")));
        assert_eq!(s.allow_rules.len(), 1);
    }

    #[test]
    fn bash_suggests_verbatim_single_command() {
        assert_eq!(
            suggest_rule_for_ask("bash", Some("git status -sb")),
            Some(add_allow("bash", Some("git status -sb")))
        );
    }

    #[test]
    fn bash_null_for_compound_injection_redirect_subshell() {
        assert_eq!(
            suggest_rule_for_ask("bash", Some("git status && rm -rf /")),
            None
        );
        assert_eq!(suggest_rule_for_ask("bash", Some("a ; b")), None);
        assert_eq!(suggest_rule_for_ask("bash", Some("a | b")), None);
        assert_eq!(suggest_rule_for_ask("bash", Some("echo $(rm -rf /)")), None);
        assert_eq!(suggest_rule_for_ask("bash", Some("echo `rm -rf /`")), None);
        assert_eq!(suggest_rule_for_ask("bash", Some("echo ${HOME}")), None);
        assert_eq!(suggest_rule_for_ask("bash", Some("cat foo > out")), None);
        assert_eq!(
            suggest_rule_for_ask("bash", Some("echo x >> /etc/passwd")),
            None
        );
        assert_eq!(suggest_rule_for_ask("bash", Some("(rm -rf /)")), None);
    }

    #[test]
    fn bash_null_for_exotic_whitespace_and_empty() {
        assert_eq!(
            suggest_rule_for_ask("bash", Some("git log\rrm -rf ~")),
            None
        );
        assert_eq!(suggest_rule_for_ask("bash", Some("echo a\u{0b}b")), None);
        assert_eq!(suggest_rule_for_ask("bash", Some("echo a\u{0c}b")), None);
        assert_eq!(suggest_rule_for_ask("bash", Some("")), None);
        assert_eq!(suggest_rule_for_ask("bash", Some("   ")), None);
        assert_eq!(suggest_rule_for_ask("bash", None), None);
    }

    #[test]
    fn non_bash_suggests_tool_name_rule() {
        assert_eq!(
            suggest_rule_for_ask("write", None),
            Some(add_allow("write", None))
        );
        assert_eq!(
            suggest_rule_for_ask("read", Some("whatever")),
            Some(add_allow("read", None))
        );
    }
}
