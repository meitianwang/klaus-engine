//! A tiny, dependency-free frontmatter parser.
//!
//! Recognizes a leading `---\n ... \n---\n` block and parses a deliberately
//! small subset of YAML: `key: value` scalars and block lists of the form
//!
//! ```text
//! key: value
//! list:
//!   - a
//!   - b
//! ```
//!
//! Surrounding single/double quotes are stripped from scalar values and list
//! items. Everything after the closing `---` is returned verbatim as the body.
//! If the text has no recognizable frontmatter block, `data` is empty and
//! `body` is the full input text.
//!
//! This is intentionally NOT a full YAML implementation — no nested maps, no
//! flow collections, no multi-line/block scalars, no anchors, no type coercion
//! beyond strings.

use crate::runtime_values::{
    RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode, RuntimeEncoder,
    RuntimeJson, RuntimeString,
};
use crate::types::json::JsonObject;
use regex::Regex;
use serde_json::Value;
use std::sync::LazyLock;

const FENCE: &str = "---";

/// Read a typed boolean or an exact boolean scalar without losing absence.
pub(super) fn parse_optional_bool_field(
    data: &JsonObject,
    key: &str,
) -> Result<Option<bool>, String> {
    match data.get(key) {
        None => Ok(None),
        Some(Value::Bool(value)) => Ok(Some(*value)),
        Some(Value::String(value)) if value == "true" => Ok(Some(true)),
        Some(Value::String(value)) if value == "false" => Ok(Some(false)),
        Some(_) => Err(format!("\"{key}\": expected boolean")),
    }
}

/// Coerce a frontmatter description: a
/// string is trimmed (empty → `None`), a number/boolean is stringified,
/// anything else (missing/null/array/object) → `None`. The caller falls back
/// to the markdown body when this returns `None`.
pub fn coerce_description_to_string(value: Option<&Value>) -> Option<String> {
    match value {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }
        Some(Value::Number(n)) => Some(n.to_string()),
        Some(Value::Bool(b)) => Some(b.to_string()),
        Some(_) => None,
    }
}

/// A string-or-list tool field value (e.g. `allowed-tools`). The exact runtime
/// codec is untagged: a string selects [`Self::Str`], an array of strings selects
/// [`Self::List`], and every other shape fails closed.
#[derive(Debug, Clone, PartialEq)]
pub enum ToolListInput {
    Str(RuntimeString),
    List(Vec<RuntimeString>),
}

impl RuntimeEncode for ToolListInput {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        match self {
            Self::Str(value) => value.encode_runtime(out),
            Self::List(value) => value.encode_runtime(out),
        }
    }
}

impl RuntimeDecode for ToolListInput {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        match value {
            RuntimeJson::String(value) => Ok(Self::Str(value.clone())),
            RuntimeJson::Array(_) => Vec::<RuntimeString>::decode_runtime(value).map(Self::List),
            _ => Err(RuntimeCodecError::new(
                RuntimeCodecErrorKind::NoMatchingUntaggedVariant,
            )),
        }
    }
}

/// Normalize a string-or-list tool field (e.g. `allowed-tools`) to a clean name
/// list: a string splits on commas and
/// spaces but is PARENS-AWARE — a comma or space INSIDE `(...)` is not a
/// separator, so a scoped rule like `Bash(npm run build)` or
/// `Bash(git diff, --stat)` survives as ONE entry. A list is taken as-is;
/// entries are trimmed and empties dropped. Shared by the skill loader and the
/// flat-command loader so a skill and a flat plugin command parse
/// `allowed-tools` identically.
pub fn parse_tool_list(value: Option<ToolListInput>) -> Option<Vec<RuntimeString>> {
    let value = value?;
    let list = match value {
        ToolListInput::List(list) => list,
        ToolListInput::Str(s) => split_tool_list(&s),
    };
    let cleaned: Vec<RuntimeString> = list
        .iter()
        .map(trim_runtime)
        .filter(|s| !s.is_empty())
        .collect();
    if cleaned.is_empty() {
        None
    } else {
        Some(cleaned)
    }
}

/// Split a tool-list string on commas and spaces, ignoring separators nested
/// inside `(...)` so a scoped specifier keeps its internal spaces/commas
/// (parens-aware). Only a literal SPACE separates (the separator set is
/// comma + space) — a tab/newline stays inside the token, so a value like
/// `Read<tab>Write` does NOT split into two grants (the resulting
/// tab-bearing token simply fails rule parsing and grants
/// nothing).
fn split_tool_list(value: &RuntimeString) -> Vec<RuntimeString> {
    let mut tokens: Vec<RuntimeString> = Vec::new();
    let mut current: Vec<u16> = Vec::new();
    let mut depth: usize = 0;
    for unit in value.as_utf16().iter().copied() {
        if unit == u16::from(b'(') {
            depth += 1;
            current.push(unit);
        } else if unit == u16::from(b')') {
            depth = depth.saturating_sub(1);
            current.push(unit);
        } else if depth == 0 && matches!(unit, 0x002c | 0x0020) {
            if !current.is_empty() {
                tokens.push(RuntimeString::from_utf16(std::mem::take(&mut current)));
            }
        } else {
            current.push(unit);
        }
    }
    if !current.is_empty() {
        tokens.push(RuntimeString::from_utf16(current));
    }
    tokens
}

fn trim_runtime(value: &RuntimeString) -> RuntimeString {
    fn is_ecmascript_trim_unit(unit: u16) -> bool {
        matches!(
            unit,
            0x0009..=0x000d
                | 0x0020
                | 0x00a0
                | 0x1680
                | 0x2000..=0x200a
                | 0x2028
                | 0x2029
                | 0x202f
                | 0x205f
                | 0x3000
                | 0xfeff
        )
    }

    let units = value.as_utf16();
    let start = units
        .iter()
        .position(|unit| !is_ecmascript_trim_unit(*unit))
        .unwrap_or(units.len());
    let end = units
        .iter()
        .rposition(|unit| !is_ecmascript_trim_unit(*unit))
        .map(|index| index + 1)
        .unwrap_or(start);
    value.slice_utf16(start..end)
}

static MARKDOWN_HEADER: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^#+\s+(.+)$").unwrap());

fn js_len(s: &str) -> usize {
    s.encode_utf16().count()
}

fn js_slice(s: &str, end: usize) -> String {
    let units: Vec<u16> = s.encode_utf16().collect();
    let end = end.min(units.len());
    String::from_utf16_lossy(&units[..end])
}

/// Derive a description from a markdown body: the first non-empty line,
/// stripped of a leading `#+ ` header, capped at 100 chars; else
/// `fallback`.
pub fn extract_description_from_markdown(content: &str, fallback: &str) -> String {
    for line in content.split('\n') {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let text = MARKDOWN_HEADER
            .captures(trimmed)
            .and_then(|c| c.get(1))
            .map(|m| m.as_str())
            .unwrap_or(trimmed);
        return if js_len(text) > 100 {
            format!("{}...", js_slice(text, 97))
        } else {
            text.to_string()
        };
    }
    fallback.to_string()
}

/// Strip a single layer of matching surrounding quotes from a scalar.
fn strip_quotes(raw: &str) -> String {
    let value = raw.trim();
    if value.len() >= 2 {
        let bytes = value.as_bytes();
        let first = bytes[0];
        let last = bytes[value.len() - 1];
        if (first == b'"' && last == b'"') || (first == b'\'' && last == b'\'') {
            return value[1..value.len() - 1].to_string();
        }
    }
    value.to_string()
}

/// Count leading spaces (indentation) on a raw line.
fn indent_of(line: &str) -> usize {
    line.bytes().take_while(|&b| b == b' ').count()
}

/// A parsed frontmatter block: the key/value `data` map (values are strings or
/// string lists) plus the markdown `body` that follows it.
#[derive(Debug, Clone, PartialEq)]
pub struct Frontmatter {
    pub data: JsonObject,
    pub body: String,
}

pub fn parse_frontmatter(text: &str) -> Frontmatter {
    // Normalize CRLF so the fence detection below is newline-agnostic.
    let normalized = text.replace("\r\n", "\n");

    // The block must start at the very beginning of the file.
    if !normalized.starts_with("---\n") {
        return Frontmatter {
            data: JsonObject::new(),
            body: text.to_string(),
        };
    }

    let lines: Vec<&str> = normalized.split('\n').collect();
    // lines[0] is the opening fence; find the closing fence line.
    let mut close_idx: Option<usize> = None;
    for (i, line) in lines.iter().enumerate().skip(1) {
        if *line == FENCE {
            close_idx = Some(i);
            break;
        }
    }

    // No closing fence => treat as having no frontmatter.
    let close_idx = match close_idx {
        Some(i) => i,
        None => {
            return Frontmatter {
                data: JsonObject::new(),
                body: text.to_string(),
            }
        }
    };

    let fm_lines = &lines[1..close_idx];
    // Body is everything after the closing fence line. A single leading newline
    // immediately after the fence is consumed (the conventional blank separator
    // is preserved beyond that).
    let mut body_lines = &lines[close_idx + 1..];
    if !body_lines.is_empty() && body_lines[0].is_empty() {
        body_lines = &body_lines[1..];
    }
    let body = body_lines.join("\n");

    let mut data = JsonObject::new();
    let mut i = 0;
    while i < fm_lines.len() {
        let line = fm_lines[i];
        // Skip blank lines and comment lines.
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            i += 1;
            continue;
        }

        let colon = match line.find(':') {
            Some(c) => c,
            None => {
                // Not a recognizable key line; skip it.
                i += 1;
                continue;
            }
        };

        let key = line[..colon].trim();
        let rest = line[colon + 1..].trim();
        let key_indent = indent_of(line);

        if rest.is_empty() {
            // Possible block list: collect subsequent more-indented `- item` lines.
            let mut items: Vec<String> = Vec::new();
            let mut j = i + 1;
            while j < fm_lines.len() {
                let next = fm_lines[j];
                if next.trim().is_empty() {
                    j += 1;
                    continue;
                }
                if indent_of(next) <= key_indent {
                    break;
                }
                let next_trimmed = next.trim();
                if !next_trimmed.starts_with("- ") && next_trimmed != "-" {
                    break;
                }
                let item_raw = if next_trimmed == "-" {
                    ""
                } else {
                    &next_trimmed[2..]
                };
                items.push(strip_quotes(item_raw));
                j += 1;
            }
            if !items.is_empty() {
                data.insert(
                    key.to_string(),
                    Value::Array(items.into_iter().map(Value::String).collect()),
                );
                i = j;
                continue;
            }
            // An empty value with no following list items => empty string.
            data.insert(key.to_string(), Value::String(String::new()));
            i += 1;
            continue;
        }

        data.insert(key.to_string(), Value::String(strip_quotes(rest)));
        i += 1;
    }

    Frontmatter { data, body }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn str_input(s: &str) -> Option<ToolListInput> {
        Some(ToolListInput::Str(RuntimeString::from(s)))
    }

    fn list_input(items: &[&str]) -> Option<ToolListInput> {
        Some(ToolListInput::List(
            items.iter().map(|s| RuntimeString::from(*s)).collect(),
        ))
    }

    fn runtime_strings(items: &[&str]) -> Vec<RuntimeString> {
        items
            .iter()
            .map(|value| RuntimeString::from(*value))
            .collect()
    }

    // ── parse_frontmatter ──

    #[test]
    fn parses_scalars_and_a_block_list_and_returns_the_body() {
        let text = [
            "---",
            "description: A test skill",
            "name: \"quoted-name\"",
            "allowed-tools:",
            "  - read",
            "  - write",
            "---",
            "",
            "This is the body.",
            "Second line.",
        ]
        .join("\n");

        let fm = parse_frontmatter(&text);
        assert_eq!(fm.data.get("description"), Some(&json!("A test skill")));
        assert_eq!(fm.data.get("name"), Some(&json!("quoted-name")));
        assert_eq!(
            fm.data.get("allowed-tools"),
            Some(&json!(["read", "write"]))
        );
        assert_eq!(fm.body, "This is the body.\nSecond line.");
    }

    #[test]
    fn returns_empty_data_and_full_text_when_there_is_no_frontmatter() {
        let text = "Just a plain body.\nNo frontmatter here.";
        let fm = parse_frontmatter(text);
        assert!(fm.data.is_empty());
        assert_eq!(fm.body, text);
    }

    #[test]
    fn fence_must_start_at_the_very_beginning() {
        let leading_newline = "\n---\na: b\n---\nbody";
        let fm = parse_frontmatter(leading_newline);
        assert!(fm.data.is_empty());
        assert_eq!(fm.body, leading_newline);

        let leading_space = " ---\na: b\n---\nbody";
        let fm = parse_frontmatter(leading_space);
        assert!(fm.data.is_empty());
        assert_eq!(fm.body, leading_space);

        // A longer dash run is not an opening fence.
        let four_dashes = "----\na: b\n---\nbody";
        let fm = parse_frontmatter(four_dashes);
        assert!(fm.data.is_empty());
        assert_eq!(fm.body, four_dashes);
    }

    #[test]
    fn unterminated_fence_is_treated_as_no_frontmatter() {
        let text = "---\na: b\nbody without closing fence";
        let fm = parse_frontmatter(text);
        assert!(fm.data.is_empty());
        assert_eq!(fm.body, text);
    }

    #[test]
    fn closing_fence_must_be_exact() {
        // Trailing space disqualifies the closing fence => no frontmatter.
        let text = "---\na: b\n--- \nbody";
        let fm = parse_frontmatter(text);
        assert!(fm.data.is_empty());
        assert_eq!(fm.body, text);
    }

    #[test]
    fn crlf_input_is_normalized_for_parsing() {
        let fm = parse_frontmatter("---\r\na: b\r\n---\r\nbody line\r\nnext");
        assert_eq!(fm.data.get("a"), Some(&json!("b")));
        assert_eq!(fm.body, "body line\nnext");
    }

    #[test]
    fn crlf_input_without_frontmatter_is_returned_verbatim() {
        let text = "plain\r\nbody";
        let fm = parse_frontmatter(text);
        assert!(fm.data.is_empty());
        assert_eq!(fm.body, text);
    }

    #[test]
    fn empty_frontmatter_block_yields_empty_data_and_the_body() {
        let fm = parse_frontmatter("---\n---\nbody");
        assert!(fm.data.is_empty());
        assert_eq!(fm.body, "body");
    }

    #[test]
    fn consumes_a_single_blank_separator_line_after_the_fence() {
        // No blank line: body starts right after the fence.
        assert_eq!(parse_frontmatter("---\na: b\n---\nbody").body, "body");
        // One blank line is consumed.
        assert_eq!(parse_frontmatter("---\na: b\n---\n\nbody").body, "body");
        // Only ONE blank line is consumed; the rest is preserved.
        assert_eq!(parse_frontmatter("---\na: b\n---\n\n\nbody").body, "\nbody");
        // Fence at the very end: empty body.
        assert_eq!(parse_frontmatter("---\na: b\n---").body, "");
        // Trailing newline after the fence is the consumed separator.
        assert_eq!(parse_frontmatter("---\na: b\n---\n").body, "");
    }

    #[test]
    fn skips_blank_lines_comments_and_lines_without_a_colon() {
        let text = [
            "---",
            "",
            "# a comment",
            "  # an indented comment: with a colon",
            "not a key line",
            "a: 1",
            "---",
            "body",
        ]
        .join("\n");
        let fm = parse_frontmatter(&text);
        assert_eq!(fm.data.len(), 1);
        assert_eq!(fm.data.get("a"), Some(&json!("1")));
        assert_eq!(fm.body, "body");
    }

    #[test]
    fn strips_a_single_layer_of_matching_quotes_from_scalars() {
        let text = [
            "---",
            "double: \"hello world\"",
            "single: 'hi there'",
            "mismatched: \"oops'",
            "nested: \"'both'\"",
            "lone: \"",
            "empty_double: \"\"",
            "empty_single: ''",
            "---",
            "",
        ]
        .join("\n");
        let fm = parse_frontmatter(&text);
        assert_eq!(fm.data.get("double"), Some(&json!("hello world")));
        assert_eq!(fm.data.get("single"), Some(&json!("hi there")));
        // Mismatched quotes are kept as-is.
        assert_eq!(fm.data.get("mismatched"), Some(&json!("\"oops'")));
        // Only ONE layer is stripped.
        assert_eq!(fm.data.get("nested"), Some(&json!("'both'")));
        // A lone quote (length 1) is not stripped.
        assert_eq!(fm.data.get("lone"), Some(&json!("\"")));
        assert_eq!(fm.data.get("empty_double"), Some(&json!("")));
        assert_eq!(fm.data.get("empty_single"), Some(&json!("")));
    }

    #[test]
    fn value_splits_on_the_first_colon() {
        let fm = parse_frontmatter("---\nurl: http://example.com:8080\n---\n");
        assert_eq!(fm.data.get("url"), Some(&json!("http://example.com:8080")));
    }

    #[test]
    fn key_and_value_are_trimmed() {
        let fm = parse_frontmatter("---\n  spaced  :   padded value  \n---\n");
        assert_eq!(fm.data.get("spaced"), Some(&json!("padded value")));
    }

    #[test]
    fn empty_value_with_no_list_items_becomes_an_empty_string() {
        let fm = parse_frontmatter("---\nkey:\nother: x\n---\n");
        assert_eq!(fm.data.get("key"), Some(&json!("")));
        assert_eq!(fm.data.get("other"), Some(&json!("x")));
    }

    #[test]
    fn block_list_items_must_be_more_indented_than_the_key() {
        // Same-indent items do not belong to the list => empty string value,
        // and the dash lines themselves are skipped (no colon).
        let fm = parse_frontmatter("---\nlist:\n- a\n- b\n---\n");
        assert_eq!(fm.data.get("list"), Some(&json!("")));
        assert_eq!(fm.data.len(), 1);
    }

    #[test]
    fn block_list_collects_items_and_stops_at_the_next_key() {
        let text = ["---", "list:", "  - a", "", "  - b", "other: x", "---", ""].join("\n");
        let fm = parse_frontmatter(&text);
        // Blank lines inside the list are skipped.
        assert_eq!(fm.data.get("list"), Some(&json!(["a", "b"])));
        assert_eq!(fm.data.get("other"), Some(&json!("x")));
    }

    #[test]
    fn block_list_items_support_quotes_and_bare_dashes() {
        let text = [
            "---",
            "list:",
            "  - \"quoted item\"",
            "  - 'single'",
            "  -",
            "  - plain",
            "---",
            "",
        ]
        .join("\n");
        let fm = parse_frontmatter(&text);
        // A bare `-` contributes an empty item.
        assert_eq!(
            fm.data.get("list"),
            Some(&json!(["quoted item", "single", "", "plain"]))
        );
    }

    #[test]
    fn block_list_stops_at_a_non_item_line() {
        // A more-indented line that is not a `- item` ends the collection; the
        // key falls back to an empty string.
        let fm = parse_frontmatter("---\nlist:\n  notanitem\n---\n");
        assert_eq!(fm.data.get("list"), Some(&json!("")));

        // A dash without a following space is not an item either.
        let fm = parse_frontmatter("---\nlist:\n  -x\n---\n");
        assert_eq!(fm.data.get("list"), Some(&json!("")));
    }

    #[test]
    fn indented_key_uses_its_own_indent_as_the_list_baseline() {
        let text = ["---", "  list:", "    - a", "  - b", "---", ""].join("\n");
        let fm = parse_frontmatter(&text);
        // `- b` is at the key's own indent => not part of the list.
        assert_eq!(fm.data.get("list"), Some(&json!(["a"])));
    }

    #[test]
    fn duplicate_keys_keep_the_last_value() {
        let fm = parse_frontmatter("---\na: first\na: second\n---\n");
        assert_eq!(fm.data.get("a"), Some(&json!("second")));
    }

    #[test]
    fn body_after_the_fence_is_verbatim_including_more_fences() {
        let fm = parse_frontmatter("---\na: b\n---\nline\n---\nmore");
        assert_eq!(fm.data.get("a"), Some(&json!("b")));
        assert_eq!(fm.body, "line\n---\nmore");
    }

    // ── coerce_description_to_string ──

    #[test]
    fn optional_boolean_fields_preserve_false_and_reject_invalid_shapes() {
        assert_eq!(
            parse_optional_bool_field(&JsonObject::new(), "flag"),
            Ok(None)
        );
        for (value, expected) in [
            (json!(false), false),
            (json!("false"), false),
            (json!(true), true),
            (json!("true"), true),
        ] {
            let mut data = JsonObject::new();
            data.insert("flag".into(), value);
            assert_eq!(parse_optional_bool_field(&data, "flag"), Ok(Some(expected)));
        }
        for value in [
            Value::Null,
            json!(0),
            json!("yes"),
            json!("FALSE"),
            json!([false]),
            json!({"value": false}),
        ] {
            let mut data = JsonObject::new();
            data.insert("flag".into(), value);
            assert!(parse_optional_bool_field(&data, "flag").is_err());
        }
    }

    #[test]
    fn coerce_description_handles_missing_and_null() {
        assert_eq!(coerce_description_to_string(None), None);
        assert_eq!(coerce_description_to_string(Some(&Value::Null)), None);
    }

    #[test]
    fn coerce_description_trims_strings_and_drops_empties() {
        assert_eq!(
            coerce_description_to_string(Some(&json!("  hi  "))),
            Some("hi".to_string())
        );
        assert_eq!(coerce_description_to_string(Some(&json!(""))), None);
        assert_eq!(coerce_description_to_string(Some(&json!("   "))), None);
    }

    #[test]
    fn coerce_description_stringifies_numbers_and_booleans() {
        assert_eq!(
            coerce_description_to_string(Some(&json!(5))),
            Some("5".to_string())
        );
        assert_eq!(
            coerce_description_to_string(Some(&json!(1.5))),
            Some("1.5".to_string())
        );
        assert_eq!(
            coerce_description_to_string(Some(&json!(true))),
            Some("true".to_string())
        );
        assert_eq!(
            coerce_description_to_string(Some(&json!(false))),
            Some("false".to_string())
        );
    }

    #[test]
    fn coerce_description_rejects_arrays_and_objects() {
        assert_eq!(coerce_description_to_string(Some(&json!(["a"]))), None);
        assert_eq!(coerce_description_to_string(Some(&json!({"a": 1}))), None);
    }

    // ── parse_tool_list ──

    #[test]
    fn parse_tool_list_is_parens_aware() {
        assert_eq!(
            parse_tool_list(str_input("Bash(npm run build), Bash(git diff, --stat)")),
            Some(runtime_strings(&[
                "Bash(npm run build)",
                "Bash(git diff, --stat)",
            ]))
        );
    }

    #[test]
    fn parse_tool_list_splits_on_commas_and_spaces() {
        assert_eq!(
            parse_tool_list(str_input("Read Write")),
            Some(runtime_strings(&["Read", "Write"]))
        );
        assert_eq!(
            parse_tool_list(str_input("Read,Write")),
            Some(runtime_strings(&["Read", "Write"]))
        );
        assert_eq!(
            parse_tool_list(str_input("Read, ,Write")),
            Some(runtime_strings(&["Read", "Write"]))
        );
    }

    #[test]
    fn parse_tool_list_does_not_split_on_tabs() {
        assert_eq!(
            parse_tool_list(str_input("Read\tWrite")),
            Some(runtime_strings(&["Read\tWrite"]))
        );
    }

    #[test]
    fn parse_tool_list_takes_a_list_as_is_trimming_and_dropping_empties() {
        assert_eq!(
            parse_tool_list(list_input(&["  a ", "", "b"])),
            Some(runtime_strings(&["a", "b"]))
        );
        assert_eq!(parse_tool_list(list_input(&[])), None);
        assert_eq!(parse_tool_list(list_input(&["", "  "])), None);
    }

    #[test]
    fn parse_tool_list_empty_inputs_yield_none() {
        assert_eq!(parse_tool_list(str_input("")), None);
        assert_eq!(parse_tool_list(str_input("   ")), None);
        assert_eq!(parse_tool_list(None), None);
    }

    #[test]
    fn parse_tool_list_handles_unbalanced_parens() {
        // An unmatched close paren is literal; splitting continues at depth 0.
        assert_eq!(
            parse_tool_list(str_input("a) b")),
            Some(runtime_strings(&["a)", "b"]))
        );
        // An unmatched open paren swallows the rest into one token.
        assert_eq!(
            parse_tool_list(str_input("Bash(a b, c")),
            Some(runtime_strings(&["Bash(a b, c"]))
        );
    }

    // ── extract_description_from_markdown ──

    #[test]
    fn extract_description_strips_a_markdown_header() {
        assert_eq!(
            extract_description_from_markdown("# Title\nbody", "fb"),
            "Title"
        );
        assert_eq!(
            extract_description_from_markdown("\n\n  ### Deep Header  \nrest", "fb"),
            "Deep Header"
        );
    }

    #[test]
    fn extract_description_uses_the_first_non_empty_line() {
        assert_eq!(
            extract_description_from_markdown("\n   \nfirst real line\nsecond", "fb"),
            "first real line"
        );
    }

    #[test]
    fn extract_description_keeps_a_hashes_only_or_spaceless_header_literal() {
        // No space after the hashes => not a header match; the line is used as-is.
        assert_eq!(
            extract_description_from_markdown("#NoSpace", "fb"),
            "#NoSpace"
        );
        assert_eq!(extract_description_from_markdown("#", "fb"), "#");
    }

    #[test]
    fn extract_description_caps_at_100_chars() {
        let exactly_100 = "a".repeat(100);
        assert_eq!(
            extract_description_from_markdown(&exactly_100, "fb"),
            exactly_100
        );

        let long = "a".repeat(101);
        let expected = format!("{}...", "a".repeat(97));
        assert_eq!(extract_description_from_markdown(&long, "fb"), expected);

        // The cap applies to the header text after stripping the hashes.
        let header = format!("# {}", "b".repeat(150));
        let expected = format!("{}...", "b".repeat(97));
        assert_eq!(extract_description_from_markdown(&header, "fb"), expected);
    }

    #[test]
    fn extract_description_caps_using_js_string_length() {
        let text = format!("{}🙂", "a".repeat(99));
        let expected = format!("{}...", "a".repeat(97));
        assert_eq!(extract_description_from_markdown(&text, "fb"), expected);
    }

    #[test]
    fn extract_description_falls_back_when_content_is_blank() {
        assert_eq!(
            extract_description_from_markdown("", "fallback"),
            "fallback"
        );
        assert_eq!(
            extract_description_from_markdown("\n  \n\t\n", "fallback"),
            "fallback"
        );
    }

    // ── ToolListInput exact runtime codec ──

    #[test]
    fn tool_list_input_runtime_codec_is_exact_and_untagged() {
        let exact = RuntimeString::from_utf16(vec![u16::from(b'R'), 0xd800]);
        let s = ToolListInput::Str(exact.clone());
        let encoded = crate::runtime_values::runtime_codec::encode_value(&s).unwrap();
        assert_eq!(encoded.as_str(), Some(&exact));
        assert_eq!(ToolListInput::decode_runtime(&encoded).unwrap(), s);

        let l = ToolListInput::List(vec![RuntimeString::from("Read"), exact]);
        let encoded = crate::runtime_values::runtime_codec::encode_value(&l).unwrap();
        assert_eq!(ToolListInput::decode_runtime(&encoded).unwrap(), l);

        let error = ToolListInput::decode_runtime(&RuntimeJson::Bool(true)).unwrap_err();
        assert_eq!(
            error.kind(),
            &RuntimeCodecErrorKind::NoMatchingUntaggedVariant
        );
    }

    #[test]
    fn tool_list_parser_preserves_lone_surrogates_exactly() {
        let exact = RuntimeString::from_utf16(vec![0x0020, 0xd800, 0x0020]);
        assert_eq!(
            parse_tool_list(Some(ToolListInput::Str(exact))),
            Some(vec![RuntimeString::from_utf16(vec![0xd800])])
        );
    }
}
