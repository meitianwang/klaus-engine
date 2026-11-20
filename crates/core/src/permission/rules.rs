//! Parse + match permission rule strings.

use crate::runtime_values::RuntimeString;
use crate::types::permission::PermissionRule;
use regex::Regex;
use std::sync::LazyLock;

static TOOL_NAME: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[A-Za-z_][0-9A-Za-z_-]*$").unwrap());

static TRAILING_STAR: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\s*\*\s*$").unwrap());

/// Parse a rule string into a structured rule.
///   "Read"               → { tool_name: 'Read' }
///   "Bash(git status)"   → { tool_name: 'Bash', argument_prefix: 'git status' }
///   "Bash(npm run *)"    → { tool_name: 'Bash', argument_prefix: 'npm run *' }
pub fn parse_rule_string(s: &str) -> Option<PermissionRule> {
    let t = s.trim();
    if t.is_empty() {
        return None;
    }
    // The outer delimiters are the first '(' and the final ')'. The prefix is
    // literal text, including nested/unbalanced parentheses and backslashes:
    // durable rules have always serialized literal prefixes, so introducing
    // backslash unescaping here would change existing Windows/UNC rules.
    let (tool, argument) = match t.split_once('(') {
        Some((tool, rest)) => (tool, Some(rest.strip_suffix(')')?.trim())),
        None => (t, None),
    };
    if tool.starts_with("mcp__") {
        if let Some(mcp) = parse_mcp_rule(tool) {
            // A wildcard ignores arguments during evaluation. Never accept a
            // purportedly narrower rule whose prefix would have no effect.
            if argument.is_some_and(|prefix| !prefix.is_empty()) {
                return None;
            }
            return Some(mcp);
        }
    }
    if !TOOL_NAME.is_match(tool) {
        return None;
    }
    let tool_name = RuntimeString::from(tool);
    match argument {
        None => Some(PermissionRule {
            tool_name,
            argument_prefix: None,
            tool_name_prefix: None,
        }),
        Some(arg) => Some(PermissionRule {
            tool_name,
            argument_prefix: Some(RuntimeString::from(arg)),
            tool_name_prefix: None,
        }),
    }
}

/// The shared literal grammar for structured facade input and durable output.
/// No escaping is necessary: only the first opening and final closing paren
/// delimit the prefix, and every interior character belongs to the command.
pub fn format_rule_string(tool: &str, prefix: Option<&str>) -> String {
    match prefix.filter(|value| !value.is_empty()) {
        Some(prefix) => format!("{tool}({prefix})"),
        None => tool.to_string(),
    }
}

/// Parse an `mcp__…` rule into a tool-name-PREFIX rule, or `None` when it is an
/// exact tool reference (which the normal grammar handles). The prefix always
/// ends at a `__` boundary so `mcp__git` cannot partially match the server
/// `mcp__github`.
fn parse_mcp_rule(t: &str) -> Option<PermissionRule> {
    if let Some(prefix) = t.strip_suffix('*') {
        // The remainder is the literal tool-name prefix. Require a server AND a
        // tool segment start (`mcp__server__…`).
        if prefix.split("__").count() < 3 {
            return None;
        }
        return Some(PermissionRule {
            tool_name: RuntimeString::from(t),
            argument_prefix: None,
            tool_name_prefix: Some(RuntimeString::from(prefix)),
        });
    }
    // Bare `mcp__server` (no tool segment) → the whole server (`mcp__server__*`).
    let parts: Vec<&str> = t.split("__").collect();
    if parts.len() == 2 && !parts[1].is_empty() {
        return Some(PermissionRule {
            tool_name: RuntimeString::from(t),
            argument_prefix: None,
            tool_name_prefix: Some(RuntimeString::from(format!("{}__{}__", parts[0], parts[1]))),
        });
    }
    None
}

pub fn parse_rule_strings(list: &[RuntimeString]) -> Vec<PermissionRule> {
    list.iter()
        .filter_map(|value| {
            let scalar = String::from_utf16(value.as_utf16()).ok()?;
            parse_rule_string(&scalar)
        })
        .collect()
}

/// Stored permission rules keep working across the background-control rename:
/// the legacy per-kind spellings fold onto the unified surface names, so an
/// allow/deny/ask rule written against an old name still matches calls that
/// now dispatch under the canonical identity (and vice versa).
fn legacy_background_rule_matches(tool_name: &RuntimeString, rule_name: &RuntimeString) -> bool {
    fn canonical(name: &RuntimeString) -> Option<&'static str> {
        let folded = name.try_to_utf8().ok()?.to_ascii_lowercase();
        Some(match folded.as_str() {
            "task_output" | "bash_output" | "taskoutput" | "agentoutputtool" | "bashoutputtool"
            | "agentoutput" | "bashoutput" => "TaskOutput",
            "task_stop" | "kill_shell" | "kill_bash" | "taskstop" | "killshell" | "killbash" => {
                "TaskStop"
            }
            _ => return None,
        })
    }
    match (canonical(tool_name), canonical(rule_name)) {
        (Some(tool), Some(rule)) => tool == rule,
        _ => false,
    }
}

/// Match a rule against a tool call. For argument-bearing rules we use plain
/// prefix comparison (no regex → no ReDoS), stripping a trailing `*`.
pub fn rule_matches(
    rule: &PermissionRule,
    tool_name: &RuntimeString,
    command_string: Option<&str>,
) -> bool {
    // MCP wildcard rules match by tool-name prefix (no argument component).
    if let Some(prefix) = &rule.tool_name_prefix {
        return runtime_ascii_prefix_ignore_case(tool_name, prefix);
    }
    if !runtime_ascii_eq_ignore_case(tool_name, &rule.tool_name)
        && !legacy_background_rule_matches(tool_name, &rule.tool_name)
    {
        return false;
    }
    let arg = match &rule.argument_prefix {
        None => return true,
        Some(a) if a.is_empty() => return true,
        Some(a) => a,
    };
    let command_string = match command_string {
        None => return false,
        Some(c) => c,
    };
    let arg = String::from_utf16(arg.as_utf16())
        .expect("permission argument prefixes are validated scalar configuration");
    let prefix = TRAILING_STAR.replace(&arg, "");
    let prefix = prefix.trim();
    if prefix.is_empty() {
        return true;
    }
    command_string.trim().starts_with(prefix)
}

fn runtime_ascii_eq_ignore_case(value: &RuntimeString, expected: &RuntimeString) -> bool {
    value
        .as_utf16()
        .iter()
        .copied()
        .map(ascii_lower_u16)
        .eq(expected.as_utf16().iter().copied().map(ascii_lower_u16))
}

fn runtime_ascii_prefix_ignore_case(value: &RuntimeString, prefix: &RuntimeString) -> bool {
    let mut value = value.as_utf16().iter().copied().map(ascii_lower_u16);
    prefix
        .as_utf16()
        .iter()
        .copied()
        .map(ascii_lower_u16)
        .all(|unit| value.next() == Some(unit))
}

fn ascii_lower_u16(unit: u16) -> u16 {
    if unit >= u16::from(b'A') && unit <= u16::from(b'Z') {
        unit + u16::from(b'a' - b'A')
    } else {
        unit
    }
}

pub fn matches_any_rule<'a>(
    rules: &'a [PermissionRule],
    tool_name: &RuntimeString,
    command_string: Option<&str>,
) -> Option<&'a PermissionRule> {
    rules
        .iter()
        .find(|r| rule_matches(r, tool_name, command_string))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(tool: &str, arg: Option<&str>, prefix: Option<&str>) -> PermissionRule {
        PermissionRule {
            tool_name: RuntimeString::from(tool),
            argument_prefix: arg.map(RuntimeString::from),
            tool_name_prefix: prefix.map(RuntimeString::from),
        }
    }

    fn matches(rule: &PermissionRule, tool: &str, command: Option<&str>) -> bool {
        let tool = RuntimeString::from(tool);
        rule_matches(rule, &tool, command)
    }

    #[test]
    fn legacy_background_rule_names_fold_onto_the_unified_surface() {
        // Stored rules written against the retired per-kind names keep
        // matching calls dispatched under the canonical identities.
        assert!(matches(&rule("task_stop", None, None), "TaskStop", None));
        assert!(matches(&rule("kill_shell", None, None), "TaskStop", None));
        assert!(matches(&rule("kill_bash", None, None), "TaskStop", None));
        assert!(matches(
            &rule("task_output", None, None),
            "TaskOutput",
            None
        ));
        assert!(matches(
            &rule("bash_output", None, None),
            "TaskOutput",
            None
        ));
        // And the other direction: a canonical rule matches nothing that was
        // retired, while unrelated names never cross-match.
        assert!(matches(&rule("TaskStop", None, None), "task_stop", None));
        assert!(!matches(&rule("task_stop", None, None), "TaskOutput", None));
        assert!(!matches(&rule("bash", None, None), "TaskStop", None));
        assert!(!matches(&rule("task_stop", None, None), "bash", None));
    }

    #[test]
    fn parses_bare_and_command_prefix() {
        assert_eq!(parse_rule_string("Read"), Some(rule("Read", None, None)));
        assert_eq!(
            parse_rule_string("Bash(git status)"),
            Some(rule("Bash", Some("git status"), None))
        );
    }

    #[test]
    fn parses_mcp_wildcards() {
        assert_eq!(
            parse_rule_string("mcp__github__*"),
            Some(rule("mcp__github__*", None, Some("mcp__github__")))
        );
        assert_eq!(
            parse_rule_string("mcp__github"),
            Some(rule("mcp__github", None, Some("mcp__github__")))
        );
        assert_eq!(
            parse_rule_string("mcp__github__get_*"),
            Some(rule("mcp__github__get_*", None, Some("mcp__github__get_")))
        );
        assert_eq!(
            parse_rule_string("mcp__github__get_issue"),
            Some(rule("mcp__github__get_issue", None, None))
        );
    }

    #[test]
    fn rejects_server_level_wildcard() {
        assert_eq!(parse_rule_string("mcp__*"), None);
        assert_eq!(parse_rule_string("mcp__github*"), None);
    }

    #[test]
    fn mcp_prefix_matching() {
        let r = parse_rule_string("mcp__github__*").unwrap();
        assert!(matches(&r, "mcp__github__get_issue", None));
        assert!(matches(&r, "mcp__github__create_pr", None));

        assert!(matches(
            &parse_rule_string("mcp__github").unwrap(),
            "mcp__github__get_issue",
            None
        ));

        assert!(!matches(
            &parse_rule_string("mcp__github__*").unwrap(),
            "mcp__gitlab__get_issue",
            None
        ));
        // `mcp__git` must NOT match the `mcp__github` server.
        assert!(!matches(
            &parse_rule_string("mcp__git").unwrap(),
            "mcp__github__x",
            None
        ));

        let r = parse_rule_string("mcp__github__get_*").unwrap();
        assert!(matches(&r, "mcp__github__get_issue", None));
        assert!(!matches(&r, "mcp__github__create_pr", None));

        let r = parse_rule_string("mcp__github__get_issue").unwrap();
        assert!(matches(&r, "mcp__github__get_issue", None));
        assert!(!matches(&r, "mcp__github__get_issues", None));
    }

    #[test]
    fn command_prefix_matching_strips_trailing_star() {
        let r = parse_rule_string("Bash(npm run *)").unwrap();
        assert!(matches(&r, "Bash", Some("npm run build")));
        assert!(!matches(&r, "Bash", Some("npm test")));
    }
}
