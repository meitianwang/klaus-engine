//! Whitespace-flexible line matching for Edit / MultiEdit. Matching and
//! replacement stay in exact JavaScript UTF-16 storage, including lone
//! surrogates.

use crate::runtime_values::RuntimeString;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlexibleReplacement {
    pub new_content: RuntimeString,
    pub occurrences: usize,
}

fn leading_indent(line: &RuntimeString) -> RuntimeString {
    let end = line
        .as_utf16()
        .iter()
        .position(|unit| *unit != u16::from(b' ') && *unit != u16::from(b'\t'))
        .unwrap_or(line.len_utf16());
    line.slice_utf16(..end)
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

fn trim_runtime(value: &RuntimeString) -> RuntimeString {
    let units = value.as_utf16();
    let start = units
        .iter()
        .position(|unit| !is_ecmascript_whitespace(*unit))
        .unwrap_or(units.len());
    let end = units
        .iter()
        .rposition(|unit| !is_ecmascript_whitespace(*unit))
        .map_or(start, |index| index + 1);
    value.slice_utf16(start..end)
}

fn trim_start_runtime(value: &RuntimeString) -> RuntimeString {
    let start = value
        .as_utf16()
        .iter()
        .position(|unit| !is_ecmascript_whitespace(*unit))
        .unwrap_or(value.len_utf16());
    value.slice_utf16(start..)
}

fn starts_with_runtime(value: &RuntimeString, prefix: &RuntimeString) -> bool {
    value.as_utf16().starts_with(prefix.as_utf16())
}

fn split_lines(value: &RuntimeString, keep_newline: bool) -> Vec<RuntimeString> {
    let units = value.as_utf16();
    let mut output = Vec::new();
    let mut start = 0;
    for (index, unit) in units.iter().enumerate() {
        if *unit == u16::from(b'\n') {
            let end = if keep_newline { index + 1 } else { index };
            output.push(RuntimeString::from_utf16(units[start..end].to_vec()));
            start = index + 1;
        }
    }
    if start < units.len() || !keep_newline {
        output.push(RuntimeString::from_utf16(units[start..].to_vec()));
    }
    output
}

fn join_runtime(values: &[RuntimeString], separator: &str) -> RuntimeString {
    let separator = RuntimeString::from(separator);
    let mut output = RuntimeString::default();
    for (index, value) in values.iter().enumerate() {
        if index != 0 {
            output = output.concat(&separator);
        }
        output = output.concat(value);
    }
    output
}

fn apply_indentation(
    lines: &[RuntimeString],
    target_indentation: &RuntimeString,
) -> Vec<RuntimeString> {
    if lines.is_empty() {
        return Vec::new();
    }
    let reference = leading_indent(&lines[0]);
    lines
        .iter()
        .map(|line| {
            if trim_runtime(line).is_empty() {
                RuntimeString::default()
            } else if starts_with_runtime(line, &reference) {
                target_indentation.concat(&line.slice_utf16(reference.len_utf16()..))
            } else {
                target_indentation.concat(&trim_start_runtime(line))
            }
        })
        .collect()
}

fn restore_trailing_newline(original: &RuntimeString, modified: &RuntimeString) -> RuntimeString {
    let had = original.ends_with("\n");
    let has = modified.ends_with("\n");
    if had && !has {
        modified.concat(&RuntimeString::from("\n"))
    } else if !had && has {
        modified.slice_utf16(..modified.len_utf16().saturating_sub(1))
    } else {
        modified.clone()
    }
}

pub fn flexible_line_replace(
    content: &RuntimeString,
    old_string: &RuntimeString,
    new_string: &RuntimeString,
) -> Option<FlexibleReplacement> {
    let search = old_string.replace("\r\n", "\n");
    let replacement = new_string.replace("\r\n", "\n");
    if search.is_empty() {
        return None;
    }

    let mut source_lines = split_lines(content, true);
    let search_lines = split_lines(&search, false)
        .into_iter()
        .map(|line| trim_runtime(&line))
        .collect::<Vec<_>>();
    let replacement_lines = split_lines(&replacement, false);
    let search_len = search_lines.len();
    let mut occurrences = 0;
    let mut index = 0;

    while index + search_len <= source_lines.len() {
        let window = &source_lines[index..index + search_len];
        let matches = window
            .iter()
            .zip(&search_lines)
            .all(|(line, search)| trim_runtime(line) == *search);
        if matches {
            occurrences += 1;
            let indentation = leading_indent(&window[0]);
            let indented = apply_indentation(&replacement_lines, &indentation);
            let mut replacement_text = join_runtime(&indented, "\n");
            let window_had_newline = window.last().is_some_and(|line| line.ends_with("\n"));
            if !replacement.is_empty() && window_had_newline && !replacement_text.ends_with("\n") {
                replacement_text = replacement_text.concat(&RuntimeString::from("\n"));
            }
            source_lines.splice(index..index + search_len, [replacement_text]);
        }
        index += 1;
    }

    (occurrences != 0).then(|| FlexibleReplacement {
        new_content: restore_trailing_newline(content, &join_runtime(&source_lines, "")),
        occurrences,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(value: &str) -> RuntimeString {
        RuntimeString::from(value)
    }

    fn replace(content: &str, old: &str, new: &str) -> Option<FlexibleReplacement> {
        flexible_line_replace(&r(content), &r(old), &r(new))
    }

    #[test]
    fn null_for_empty_search() {
        assert!(replace("a\nb\n", "", "x").is_none());
    }

    #[test]
    fn null_for_no_match() {
        assert!(replace("alpha\nbeta\n", "gamma", "x").is_none());
    }

    #[test]
    fn single_line_indentation_drift() {
        let result = replace("    return value\n", "  return value", "return other").unwrap();
        assert_eq!(result.occurrences, 1);
        assert_eq!(result.new_content, r("    return other\n"));
    }

    #[test]
    fn preserves_lone_surrogates_while_reindenting() {
        let content = RuntimeString::from_utf16(vec![0x20, 0x20, 0xd800, 0x0a]);
        let old = RuntimeString::from_utf16(vec![0xd800]);
        let replacement = RuntimeString::from_utf16(vec![0xd800, u16::from(b'x')]);
        let result = flexible_line_replace(&content, &old, &replacement).unwrap();
        assert_eq!(
            result.new_content.as_utf16(),
            &[0x20, 0x20, 0xd800, 0x78, 0x0a]
        );
    }

    #[test]
    fn replaces_with_relative_indentation() {
        let result = flexible_line_replace(
            &r("  if (x) {\n    go()\n  }\n"),
            &r("if (x) {\n  go()\n}"),
            &r("if (y) {\n  redo()\n}"),
        )
        .unwrap();
        assert_eq!(result.new_content, r("  if (y) {\n    redo()\n  }\n"));
    }

    #[test]
    fn counts_multiple_trim_equal() {
        let content = ["  foo", "    foo", "bar", ""].join("\n");
        let result = replace(&content, "foo", "baz").unwrap();
        assert_eq!(result.occurrences, 2);
    }

    #[test]
    fn replaces_every_occurrence_with_own_indent() {
        let content = ["  foo", "    foo", ""].join("\n");
        let result = replace(&content, "foo", "baz").unwrap();
        assert_eq!(result.new_content, r(&["  baz", "    baz", ""].join("\n")));
    }

    #[test]
    fn preserves_no_trailing_newline() {
        let result = replace("    a\n    b", "b", "c").unwrap();
        assert_eq!(result.new_content, r("    a\n    c"));
    }

    #[test]
    fn preserves_trailing_newline() {
        let result = replace("    a\n    b\n", "b", "c").unwrap();
        assert_eq!(result.new_content, r("    a\n    c\n"));
    }

    #[test]
    fn normalizes_crlf_in_search() {
        let result = replace("x\n    keep me\ny\n", "  keep me\r\n".trim_end(), "kept").unwrap();
        assert_eq!(result.occurrences, 1);
        assert_eq!(result.new_content, r("x\n    kept\ny\n"));
    }

    #[test]
    fn does_not_fuse_following_line() {
        let content = ["    one", "    two", "    three", ""].join("\n");
        let result = replace(&content, "two", "TWO").unwrap();
        assert_eq!(
            result.new_content,
            r(&["    one", "    TWO", "    three", ""].join("\n"))
        );
    }
}
