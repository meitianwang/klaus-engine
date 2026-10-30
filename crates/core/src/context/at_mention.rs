//! `@`-mention detection:
//! finds `@path`, `@"quoted path"`, and `@agent-name` references, so the engine
//! can expand them into context BEFORE the model request. PURE detection only.
//!
//! Boundary rule: a mention is recognized only at start-of-text, after ASCII
//! whitespace, or after one of four CJK punctuation marks — so `a@b.com` is never
//! a mention.

use regex::Regex;
use std::collections::HashSet;
use std::sync::LazyLock;

/// A file/directory `@`-mention, with an optional `#L<start>` / `#L<start>-<end>`
/// line range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileMention {
    /// The raw captured reference, e.g. `src/a.ts#L10-20`.
    pub raw: String,
    /// The path with any `#L…` suffix stripped.
    pub path: String,
    /// 1-based start line, when the mention carried `#L<start>`.
    pub line_start: Option<u32>,
    /// 1-based end line, when the mention carried `#L<start>-<end>`.
    pub line_end: Option<u32>,
}

// The boundary class is shared. The bare pattern's trailing `\b` is an ASCII
// word boundary (`(?-u:\b)`), so it cannot anchor on an all-non-ASCII path —
// giving ASCII `\b` boundary behavior.
static FILE_QUOTED: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(^|[\s。、？！])@"([^"]+)""#).unwrap());
static FILE_BARE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(^|[\s。、？！])@([^\s]+)(?-u:\b)").unwrap());
static AGENT_QUOTED: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(^|[\s。、？！])@"([A-Za-z0-9_:.@-]+) \(agent\)""#).unwrap());
static AGENT_BARE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(^|[\s。、？！])@(agent-[A-Za-z0-9_:.@-]+)").unwrap());
static LINE_RANGE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^([^#]+)(?:#L(\d+)(?:-(\d+))?)?(?:#[^#]*)?$").unwrap());

/// Quick sync gate: does the text contain ANY `@`-mention? A superset of the
/// extractors (it tests the QUOTED form too).
pub fn has_at_mention(text: &str) -> bool {
    FILE_BARE.is_match(text) || FILE_QUOTED.is_match(text)
}

/// Split a captured reference into its path + optional `#L<start>[-<end>]` range.
pub fn parse_file_mention_lines(raw: &str) -> FileMention {
    let caps = match LINE_RANGE.captures(raw) {
        Some(c) => c,
        None => {
            return FileMention {
                raw: raw.to_string(),
                path: raw.to_string(),
                line_start: None,
                line_end: None,
            }
        }
    };
    let path = caps.get(1).unwrap().as_str().to_string();
    let mut out = FileMention {
        raw: raw.to_string(),
        path,
        line_start: None,
        line_end: None,
    };
    if let Some(start) = caps.get(2) {
        let start: u32 = start.as_str().parse().unwrap_or(0);
        out.line_start = Some(start);
        out.line_end = Some(match caps.get(3) {
            Some(end) => end.as_str().parse().unwrap_or(start),
            None => start,
        });
    }
    out
}

/// Extract file/directory mentions from `text`: the quoted form `@"path with
/// spaces"` and the bare form `@path`. Deduped by raw reference, first-seen
/// order.
pub fn extract_file_mentions(text: &str) -> Vec<FileMention> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut out: Vec<FileMention> = Vec::new();
    let mut add = |raw: &str, out: &mut Vec<FileMention>| {
        if seen.insert(raw.to_string()) {
            out.push(parse_file_mention_lines(raw));
        }
    };
    for m in FILE_QUOTED.captures_iter(text) {
        if let Some(r) = m.get(2) {
            let r = r.as_str();
            // A quoted agent mention (`@"name (agent)"`) is captured here too — skip.
            if !r.ends_with(" (agent)") {
                add(r, &mut out);
            }
        }
    }
    for m in FILE_BARE.captures_iter(text) {
        if let Some(r) = m.get(2) {
            let r = r.as_str();
            // A bare match that opens with `"` belongs to the quoted pass.
            if !r.starts_with('"') {
                add(r, &mut out);
            }
        }
    }
    out
}

/// Extract agent mentions from `text`: the quoted `@"name (agent)"` and bare
/// `@agent-name` forms (the `agent-` prefix is kept on the bare form, dropped
/// from the quoted form). Deduped.
pub fn extract_agent_mentions(text: &str) -> Vec<String> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut out: Vec<String> = Vec::new();
    for m in AGENT_QUOTED.captures_iter(text) {
        if let Some(r) = m.get(2) {
            if seen.insert(r.as_str().to_string()) {
                out.push(r.as_str().to_string());
            }
        }
    }
    for m in AGENT_BARE.captures_iter(text) {
        if let Some(r) = m.get(2) {
            if seen.insert(r.as_str().to_string()) {
                out.push(r.as_str().to_string());
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn paths(text: &str) -> Vec<String> {
        extract_file_mentions(text)
            .into_iter()
            .map(|m| m.path)
            .collect()
    }

    #[test]
    fn bare_mention_at_boundary() {
        assert_eq!(paths("please read @src/a.ts now"), vec!["src/a.ts"]);
        assert_eq!(paths("@top.ts at the start"), vec!["top.ts"]);
    }

    #[test]
    fn email_is_not_a_mention() {
        assert_eq!(extract_file_mentions("contact a@b.com please"), Vec::new());
        assert!(!has_at_mention("a@b.com"));
        assert!(has_at_mention("see @file.ts"));
    }

    #[test]
    fn gate_is_superset_catches_cjk_quoted() {
        assert!(has_at_mention("打开 @\"路径文件\" 看看"));
        assert_eq!(paths("打开 @\"路径文件\" 看看"), vec!["路径文件"]);
    }

    #[test]
    fn quoted_mention_and_agent_exclusion() {
        assert_eq!(paths("open @\"my dir/a b.ts\" here"), vec!["my dir/a b.ts"]);
        assert_eq!(
            extract_file_mentions("ask @\"reviewer (agent)\" to look"),
            Vec::new()
        );
    }

    #[test]
    fn dedupes_first_seen_order() {
        assert_eq!(paths("@a.ts then @b.ts then @a.ts"), vec!["a.ts", "b.ts"]);
    }

    #[test]
    fn parses_line_ranges() {
        assert_eq!(
            parse_file_mention_lines("src/a.ts#L10"),
            FileMention {
                raw: "src/a.ts#L10".into(),
                path: "src/a.ts".into(),
                line_start: Some(10),
                line_end: Some(10),
            }
        );
        assert_eq!(
            parse_file_mention_lines("src/a.ts#L10-20"),
            FileMention {
                raw: "src/a.ts#L10-20".into(),
                path: "src/a.ts".into(),
                line_start: Some(10),
                line_end: Some(20),
            }
        );
        assert_eq!(
            parse_file_mention_lines("plain.ts"),
            FileMention {
                raw: "plain.ts".into(),
                path: "plain.ts".into(),
                line_start: None,
                line_end: None,
            }
        );
    }

    #[test]
    fn bare_extractor_carries_line_suffix() {
        let m = extract_file_mentions("look at @src/a.ts#L5-8 carefully");
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].path, "src/a.ts");
        assert_eq!(m[0].line_start, Some(5));
        assert_eq!(m[0].line_end, Some(8));
    }

    #[test]
    fn agent_mentions_deduped() {
        assert_eq!(
            extract_agent_mentions("hand off to @agent-reviewer now"),
            vec!["agent-reviewer"]
        );
        assert_eq!(
            extract_agent_mentions("use @\"planner (agent)\" here"),
            vec!["planner"]
        );
        assert_eq!(
            extract_agent_mentions("@agent-x and again @agent-x"),
            vec!["agent-x"]
        );
    }

    #[test]
    fn cjk_punctuation_left_boundary() {
        assert_eq!(paths("打开、@src/a.ts"), vec!["src/a.ts"]);
    }
}
