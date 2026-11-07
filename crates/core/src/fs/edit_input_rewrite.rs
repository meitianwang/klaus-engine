//! Pre-match input rewrites for the Edit / MultiEdit / Write tools.
//!
//! Two independent rewrites, each closing a gap between the text a model emits and
//! the bytes actually on disk:
//!
//!  - [`strip_trailing_whitespace`]: trailing whitespace is stripped from every
//!    line of a `new_string` / Write `content` before it is written, so the file
//!    never accumulates invisible end-of-line whitespace. Markdown is EXEMPT —
//!    two trailing spaces are a hard line break there, so stripping them would
//!    silently corrupt the document.
//!
//!  - [`desanitize_reserved_tags`]: the transport layer can shorten reserved tag
//!    sequences (e.g. `<function_results>` → `<fnr>`) in the text a model echoes
//!    back. When an `old_string` then fails to match, restoring the full tags can
//!    recover the real on-disk text. The fired replacements are returned so the
//!    caller can mirror the same substitutions onto the paired `new_string`.

use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeDecode, RuntimeEncode, RuntimeEncoder, RuntimeJson,
    RuntimeObjectEncoder, RuntimePath, RuntimeString,
};

/// True for Markdown paths, where trailing whitespace must be preserved.
pub fn is_markdown_path(path: &RuntimePath) -> bool {
    let units = path.as_runtime_string().as_utf16();
    let lower_ascii = |unit: u16| {
        if (u16::from(b'A')..=u16::from(b'Z')).contains(&unit) {
            unit + u16::from(b'a' - b'A')
        } else {
            unit
        }
    };
    let suffix = |ascii: &[u8]| {
        units.len() >= ascii.len()
            && units[units.len() - ascii.len()..]
                .iter()
                .copied()
                .map(lower_ascii)
                .eq(ascii.iter().copied().map(u16::from))
    };
    suffix(b".md") || suffix(b".mdx")
}

/// Strip trailing whitespace from every line of `text`, leaving the line
/// terminators (`\r\n`, `\n`, `\r`) untouched. Splitting on a capturing group
/// keeps the terminators in the array at odd indices, so only the even (content)
/// slots are trimmed and the original newline style is preserved verbatim.
pub fn strip_trailing_whitespace(text: &RuntimeString) -> RuntimeString {
    let units = text.as_utf16();
    let mut output = Vec::with_capacity(units.len());
    let mut line_start = 0;
    let mut cursor = 0;
    while cursor < units.len() {
        if !matches!(units[cursor], 0x000a | 0x000d) {
            cursor += 1;
            continue;
        }
        let mut content_end = cursor;
        while content_end > line_start && is_ecmascript_whitespace(units[content_end - 1]) {
            content_end -= 1;
        }
        output.extend_from_slice(&units[line_start..content_end]);
        if units[cursor] == 0x000d && units.get(cursor + 1) == Some(&0x000a) {
            output.extend_from_slice(&units[cursor..cursor + 2]);
            cursor += 2;
        } else {
            output.push(units[cursor]);
            cursor += 1;
        }
        line_start = cursor;
    }
    let mut content_end = units.len();
    while content_end > line_start && is_ecmascript_whitespace(units[content_end - 1]) {
        content_end -= 1;
    }
    output.extend_from_slice(&units[line_start..content_end]);
    RuntimeString::from_utf16(output)
}

fn is_ecmascript_whitespace(unit: u16) -> bool {
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

/// Reserved tag sequences and their full forms, applied in order. These are the
/// protocol tags the transport may shorten in echoed text — factual identifiers,
/// not edit content of our own choosing.
const RESERVED_TAG_REPLACEMENTS: &[(&str, &str)] = &[
    ("<fnr>", "<function_results>"),
    ("<n>", "<name>"),
    ("</n>", "</name>"),
    ("<o>", "<output>"),
    ("</o>", "</output>"),
    ("<e>", "<error>"),
    ("</e>", "</error>"),
    ("<s>", "<system>"),
    ("</s>", "</system>"),
    ("<r>", "<result>"),
    ("</r>", "</result>"),
    ("< META_START >", "<META_START>"),
    ("< META_END >", "<META_END>"),
    ("< EOT >", "<EOT>"),
    ("< META >", "<META>"),
    ("< SOS >", "<SOS>"),
    ("\n\nH:", "\n\nHuman:"),
    ("\n\nA:", "\n\nAssistant:"),
];

/// A single reserved-tag replacement that fired.
#[derive(Debug, Clone, PartialEq)]
pub struct AppliedReplacement {
    pub from: RuntimeString,
    pub to: RuntimeString,
}

impl RuntimeEncode for AppliedReplacement {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("from", &self.from)?;
        object.field("to", &self.to)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for AppliedReplacement {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                from: object.required("from", &[])?,
                to: object.required("to", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DesanitizeResult {
    /// `text` with every reserved tag restored to its full form.
    pub result: RuntimeString,
    /// The replacements that actually fired, in application order.
    pub applied_replacements: Vec<AppliedReplacement>,
}

impl RuntimeEncode for DesanitizeResult {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("result", &self.result)?;
        object.field("appliedReplacements", &self.applied_replacements)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for DesanitizeResult {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                result: object.required("result", &[])?,
                applied_replacements: object.required("appliedReplacements", &[])?,
            })
        })
    }
}

/// Restore reserved tag sequences in `text`, reporting which replacements fired so
/// the same substitutions can be mirrored onto the paired `new_string`. A no-op
/// (empty `applied_replacements`) when the text contains no reserved tags.
pub fn desanitize_reserved_tags(text: &RuntimeString) -> DesanitizeResult {
    let mut result = text.clone();
    let mut applied_replacements: Vec<AppliedReplacement> = Vec::new();
    for (from, to) in RESERVED_TAG_REPLACEMENTS {
        let before = result.clone();
        result = result.replace(from, to);
        if result != before {
            applied_replacements.push(AppliedReplacement {
                from: RuntimeString::from(*from),
                to: RuntimeString::from(*to),
            });
        }
    }
    DesanitizeResult {
        result,
        applied_replacements,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(value: &str) -> RuntimeString {
        RuntimeString::from(value)
    }

    fn strip(value: &str) -> RuntimeString {
        strip_trailing_whitespace(&r(value))
    }

    fn desanitize(value: &str) -> DesanitizeResult {
        desanitize_reserved_tags(&r(value))
    }

    fn ar(from: &str, to: &str) -> AppliedReplacement {
        AppliedReplacement {
            from: r(from),
            to: r(to),
        }
    }

    // --- stripTrailingWhitespace ---

    #[test]
    fn strips_trailing_spaces_and_tabs_keeping_terminators() {
        assert_eq!(strip("a  \nb\t\nc   "), r("a\nb\nc"));
    }

    #[test]
    fn preserves_original_newline_style() {
        assert_eq!(strip("a  \r\nb "), r("a\r\nb"));
        assert_eq!(strip("a \r\nb \nc \r"), r("a\r\nb\nc\r"));
    }

    #[test]
    fn does_not_touch_leading_or_interior_whitespace() {
        assert_eq!(
            strip("  indented and inside  here  "),
            r("  indented and inside  here")
        );
    }

    #[test]
    fn collapses_whitespace_only_line_to_empty_keeping_newline() {
        assert_eq!(strip("a\n   \nb"), r("a\n\nb"));
    }

    #[test]
    fn no_op_for_content_without_trailing_whitespace() {
        assert_eq!(strip("clean\nlines"), r("clean\nlines"));
        assert_eq!(strip(""), r(""));
    }

    // --- isMarkdownPath ---

    #[test]
    fn matches_md_and_mdx_case_insensitively_nothing_else() {
        assert!(is_markdown_path(&RuntimePath::from("/a/README.md")));
        assert!(is_markdown_path(&RuntimePath::from("/a/doc.MDX")));
        assert!(!is_markdown_path(&RuntimePath::from("/a/file.ts")));
        assert!(!is_markdown_path(&RuntimePath::from("/a/notmd")));
        assert!(!is_markdown_path(&RuntimePath::from("/a/md.txt")));
    }

    // --- desanitizeReservedTags ---

    #[test]
    fn restores_reserved_tags_and_reports_fired_replacements() {
        let DesanitizeResult {
            result,
            applied_replacements,
        } = desanitize("see <fnr> then <o>x</o>");
        assert_eq!(result, r("see <function_results> then <output>x</output>"));
        assert_eq!(
            applied_replacements,
            vec![
                ar("<fnr>", "<function_results>"),
                ar("<o>", "<output>"),
                ar("</o>", "</output>"),
            ]
        );
    }

    #[test]
    fn restores_human_assistant_turn_markers() {
        assert_eq!(
            desanitize("x\n\nH: hi\n\nA: yo").result,
            r("x\n\nHuman: hi\n\nAssistant: yo")
        );
    }

    #[test]
    fn reports_no_replacements_for_text_with_no_reserved_tags() {
        let DesanitizeResult {
            result,
            applied_replacements,
        } = desanitize("plain text <not-reserved>");
        assert_eq!(result, r("plain text <not-reserved>"));
        assert_eq!(applied_replacements, Vec::<AppliedReplacement>::new());
    }

    #[test]
    fn does_not_let_short_tag_corrupt_longer_sibling() {
        // The closing tag must not be partially rewritten by the opening-tag rule.
        assert_eq!(desanitize("<n>a</n>").result, r("<name>a</name>"));
    }
}
