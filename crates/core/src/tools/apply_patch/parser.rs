//! Parser for the `apply_patch` text format. It reads the `*** Begin Patch` /
//! `*** End Patch` envelope wrapping Add, Delete, and Update hunks and produces
//! the structured operations they describe.
//!
//! The (Lark) grammar for the format is:
//!
//! ```text
//! start: begin_patch environment_id? hunk+ end_patch
//! begin_patch: "*** Begin Patch" LF
//! environment_id: "*** Environment ID: " filename LF
//! end_patch: "*** End Patch" LF?
//!
//! hunk: add_hunk | delete_hunk | update_hunk
//! add_hunk: "*** Add File: " filename LF add_line+
//! delete_hunk: "*** Delete File: " filename LF
//! update_hunk: "*** Update File: " filename LF change_move? change?
//! filename: /(.+)/
//! add_line: "+" /(.+)/ LF -> line
//!
//! change_move: "*** Move to: " filename LF
//! change: (change_context | change_line)+ eof_line?
//! change_context: ("@@" | "@@ " /(.+)/) LF
//! change_line: ("+" | "-" | " ") /(.+)/ LF
//! eof_line: "*** End of File" LF
//! ```
//!
//! The parser below is a little more lenient than the explicit spec and allows
//! for leading/trailing whitespace around patch markers, an optional heredoc
//! envelope (`<<EOF` / `EOF`) produced by some models, and an `Update File`
//! whose first chunk omits the `@@` context header.
//!
//! Example patch:
//!
//! ```text
//! *** Begin Patch
//! *** Add File: path/to/new.txt
//! +line1
//! +line2
//! *** Update File: path/to/existing.txt
//! *** Move to: path/to/renamed.txt
//! @@ optional context header
//!  context line (space prefix = unchanged)
//! -removed line
//! +added line
//! *** Delete File: path/to/gone.txt
//! *** End Patch
//! ```

use crate::runtime_values::RuntimeString;
use std::fmt;

/// The kind of a single change line inside an Update hunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HunkChangeType {
    Keep,
    Add,
    Remove,
}

/// A single change line inside an Update hunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HunkChange {
    pub r#type: HunkChangeType,
    pub text: RuntimeString,
}

/// One `@@`-delimited block within an Update operation.
///
/// `context_header` is the text following an `@@` marker (used to disambiguate
/// the chunk location); it is absent for a bare `@@` or for a leading chunk that
/// omits the marker entirely. `is_end_of_file` is set when the chunk is
/// terminated by an explicit `*** End of File` line, which anchors the match at
/// the tail of the file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hunk {
    pub context_header: Option<RuntimeString>,
    pub changes: Vec<HunkChange>,
    pub is_end_of_file: bool,
}

/// A parsed patch operation against a single file.
///
/// `Update` carries an optional `move_path` when the patch renames the file via
/// a `*** Move to:` directive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PatchOp {
    Add {
        path: RuntimeString,
        lines: Vec<RuntimeString>,
    },
    Delete {
        path: RuntimeString,
    },
    Update {
        path: RuntimeString,
        move_path: Option<RuntimeString>,
        hunks: Vec<Hunk>,
    },
}

/// Result of parsing a full patch: the operations plus optional metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedPatch {
    pub ops: Vec<PatchOp>,
    pub environment_id: Option<RuntimeString>,
}

const BEGIN: &str = "*** Begin Patch";
const END: &str = "*** End Patch";
const ENVIRONMENT_ID: &str = "*** Environment ID: ";
const ADD: &str = "*** Add File: ";
const UPDATE: &str = "*** Update File: ";
const DELETE: &str = "*** Delete File: ";
const MOVE_TO: &str = "*** Move to: ";
const EOF: &str = "*** End of File";
const CONTEXT: &str = "@@ ";
const EMPTY_CONTEXT: &str = "@@";

/// Error raised for an unparseable patch. There are two failure modes:
/// envelope-level failures carry no line number, whereas hunk-level
/// failures expose the 1-based `line_number` where the problem was detected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatchParseError {
    pub message: RuntimeString,
    pub line_number: Option<usize>,
}

impl PatchParseError {
    pub fn new(message: impl Into<RuntimeString>) -> Self {
        PatchParseError {
            message: message.into(),
            line_number: None,
        }
    }

    pub fn with_line(message: impl Into<RuntimeString>, line_number: usize) -> Self {
        PatchParseError {
            message: message.into(),
            line_number: Some(line_number),
        }
    }
}

impl fmt::Display for PatchParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            String::from_utf8(self.message.to_node_utf8())
                .expect("Node UTF-8 patch parse error is valid UTF-8")
        )
    }
}

impl std::error::Error for PatchParseError {}

/// Parse the textual patch into structured ops, returning the operations and any
/// `Environment ID` preamble.
///
/// Returns a [`PatchParseError`] when the envelope markers are missing, a
/// directive is unrecognized, or a hunk is malformed — with descriptive
/// messages and (for hunk errors) the offending line number.
pub fn parse_patch_detailed(text: &RuntimeString) -> Result<ParsedPatch, PatchParseError> {
    // Trim the whole patch, then split on lines: trailing and leading blank
    // lines around the envelope are tolerated.
    let trimmed = trim_patch(text);
    let lines = split_lines(&trimmed);

    let hunk_lines = check_patch_boundaries(&lines)?;

    let (environment_id, remaining, start_line_number) =
        parse_environment_id_preamble(&hunk_lines)?;

    let mut ops: Vec<PatchOp> = Vec::new();
    let mut i = 0usize;
    let mut line_number = start_line_number;
    while i < remaining.len() {
        let (op, consumed) = parse_one_hunk(&remaining[i..], line_number)?;
        ops.push(op);
        line_number += consumed;
        i += consumed;
    }

    Ok(ParsedPatch {
        ops,
        environment_id,
    })
}

/// Backwards-compatible entry point: parse a patch and return just the ops.
/// Prefer [`parse_patch_detailed`] when you also need the environment id.
pub fn parse_patch(text: &RuntimeString) -> Result<Vec<PatchOp>, PatchParseError> {
    Ok(parse_patch_detailed(text)?.ops)
}

/// Trim leading and trailing whitespace lines the way `str::trim` would.
fn trim_patch(text: &RuntimeString) -> RuntimeString {
    trim_runtime(text)
}

/// Validate the `*** Begin Patch` / `*** End Patch` envelope (tolerating
/// surrounding whitespace and an optional heredoc wrapper) and return the inner
/// hunk lines. This is a lenient boundary check.
fn check_patch_boundaries(lines: &[RuntimeString]) -> Result<Vec<RuntimeString>, PatchParseError> {
    match try_strict_boundaries(lines) {
        Ok(inner) => Ok(inner),
        Err(strict_error) => {
            // Lenient: allow a heredoc envelope `<<EOF ... EOF` that some models
            // emit when they fail to pass the patch via stdin.
            let first = lines.first();
            let last = lines.last();
            if lines.len() >= 4 {
                if let (Some(first), Some(last)) = (first, last) {
                    if (runtime_eq(first, "<<EOF")
                        || runtime_eq(first, "<<'EOF'")
                        || runtime_eq(first, "<<\"EOF\""))
                        && last.ends_with("EOF")
                    {
                        let inner_lines: Vec<RuntimeString> = lines[1..lines.len() - 1].to_vec();
                        return match try_strict_boundaries(&inner_lines) {
                            Ok(inner) => Ok(inner),
                            Err(inner_error) => Err(inner_error),
                        };
                    }
                }
            }
            Err(strict_error)
        }
    }
}

/// Strictly check the first/last lines are the Begin/End markers.
fn try_strict_boundaries(lines: &[RuntimeString]) -> Result<Vec<RuntimeString>, PatchParseError> {
    let first: Option<RuntimeString> = if !lines.is_empty() {
        lines.first().map(trim_runtime)
    } else {
        None
    };
    let last: Option<RuntimeString> = if !lines.is_empty() {
        lines.last().map(trim_runtime)
    } else {
        None
    };

    if first.as_ref().is_some_and(|value| runtime_eq(value, BEGIN))
        && last.as_ref().is_some_and(|value| runtime_eq(value, END))
    {
        return Ok(lines[1..lines.len() - 1].to_vec());
    }
    if let Some(ref f) = first {
        if !runtime_eq(f, BEGIN) {
            return Err(PatchParseError::new(
                "The first line of the patch must be '*** Begin Patch'",
            ));
        }
    }
    Err(PatchParseError::new(
        "The last line of the patch must be '*** End Patch'",
    ))
}

/// Consume an optional `*** Environment ID:` preamble line. Returns the
/// remaining hunk lines and the 1-based line number at which hunks begin (they
/// are numbered from line 2, i.e. after `*** Begin Patch`, or line 3 when the
/// environment id consumed line 2).
fn parse_environment_id_preamble(
    hunk_lines: &[RuntimeString],
) -> Result<(Option<RuntimeString>, Vec<RuntimeString>, usize), PatchParseError> {
    let first = match hunk_lines.first() {
        Some(f) => f,
        None => return Ok((None, hunk_lines.to_vec(), 2)),
    };
    // Strip leading whitespace only.
    let leading_stripped = trim_start_runtime(first);
    let stripped = strip_prefix(&leading_stripped, ENVIRONMENT_ID);
    let stripped = match stripped {
        Some(s) => s,
        None => return Ok((None, hunk_lines.to_vec(), 2)),
    };
    let environment_id = trim_runtime(&stripped);
    if environment_id.is_empty() {
        return Err(PatchParseError::new(
            "apply_patch environment_id cannot be empty",
        ));
    }
    Ok((Some(environment_id), hunk_lines[1..].to_vec(), 3))
}

/// Parse a single hunk (Add / Delete / Update) from the start of `lines`,
/// returning the op and the number of lines consumed.
fn parse_one_hunk(
    lines: &[RuntimeString],
    line_number: usize,
) -> Result<(PatchOp, usize), PatchParseError> {
    let first_raw = lines.first().cloned().unwrap_or_default();
    let first = trim_runtime(&first_raw);

    if let Some(add_path) = strip_prefix(&first, ADD) {
        let mut body: Vec<RuntimeString> = Vec::new();
        let mut consumed = 1usize;
        for add_line in &lines[1..] {
            match strip_prefix(add_line, "+") {
                Some(line_to_add) => {
                    body.push(line_to_add);
                    consumed += 1;
                }
                None => break,
            }
        }
        return Ok((
            PatchOp::Add {
                path: add_path,
                lines: body,
            },
            consumed,
        ));
    }

    if let Some(delete_path) = strip_prefix(&first, DELETE) {
        return Ok((PatchOp::Delete { path: delete_path }, 1));
    }

    if let Some(update_path) = strip_prefix(&first, UPDATE) {
        let mut remaining: Vec<RuntimeString> = lines[1..].to_vec();
        let mut consumed = 1usize;

        let move_path: Option<RuntimeString> =
            remaining.first().and_then(|r| strip_prefix(r, MOVE_TO));
        if move_path.is_some() {
            remaining = remaining[1..].to_vec();
            consumed += 1;
        }

        let mut hunks: Vec<Hunk> = Vec::new();
        while !remaining.is_empty() {
            let head = remaining.first().cloned().unwrap_or_default();
            if trim_runtime(&head).is_empty() {
                consumed += 1;
                remaining = remaining[1..].to_vec();
                continue;
            }
            // A line that begins with '*' marks the start of the next hunk
            // directive.
            if head.starts_with("*") {
                break;
            }

            let (hunk, chunk_lines) =
                parse_update_file_chunk(&remaining, line_number + consumed, hunks.is_empty())?;
            hunks.push(hunk);
            consumed += chunk_lines;
            remaining = remaining[chunk_lines..].to_vec();
        }

        if hunks.is_empty() {
            return Err(PatchParseError::with_line(
                RuntimeString::from("Update file hunk for path '")
                    .concat(&update_path)
                    .concat(&RuntimeString::from("' is empty")),
                line_number,
            ));
        }

        let op = PatchOp::Update {
            path: update_path,
            move_path,
            hunks,
        };
        return Ok((op, consumed));
    }

    Err(PatchParseError::with_line(
        RuntimeString::from("'")
            .concat(&first)
            .concat(&RuntimeString::from("' is not a valid hunk header. Valid hunk headers: '*** Add File: {path}', '*** Delete File: {path}', '*** Update File: {path}'")),
        line_number,
    ))
}

/// Parse a single `@@`-delimited chunk inside an Update hunk. The first chunk of
/// an Update may omit the `@@` marker when `allow_missing_context` is true.
fn parse_update_file_chunk(
    lines: &[RuntimeString],
    line_number: usize,
    allow_missing_context: bool,
) -> Result<(Hunk, usize), PatchParseError> {
    if lines.is_empty() {
        return Err(PatchParseError::with_line(
            "Update hunk does not contain any lines",
            line_number,
        ));
    }

    #[allow(unused_assignments)]
    let mut context_header: Option<RuntimeString> = None;
    let start_index: usize;
    let head = lines.first().cloned().unwrap_or_default();
    if runtime_eq(&head, EMPTY_CONTEXT) {
        context_header = None;
        start_index = 1;
    } else if let Some(ctx) = strip_prefix(&head, CONTEXT) {
        context_header = Some(ctx);
        start_index = 1;
    } else if !allow_missing_context {
        return Err(PatchParseError::with_line(
            RuntimeString::from("Expected update hunk to start with a @@ context marker, got: '")
                .concat(&head)
                .concat(&RuntimeString::from("'")),
            line_number,
        ));
    } else {
        context_header = None;
        start_index = 0;
    }

    if start_index >= lines.len() {
        return Err(PatchParseError::with_line(
            "Update hunk does not contain any lines",
            line_number + 1,
        ));
    }

    let mut changes: Vec<HunkChange> = Vec::new();
    let mut is_end_of_file = false;
    let mut parsed = 0usize;

    let mut idx = start_index;
    while idx < lines.len() {
        let line = lines.get(idx).cloned().unwrap_or_default();
        if runtime_eq(&line, EOF) {
            if parsed == 0 {
                return Err(PatchParseError::with_line(
                    "Update hunk does not contain any lines",
                    line_number + 1,
                ));
            }
            is_end_of_file = true;
            parsed += 1;
            break;
        }

        // The marker is the first character of the line, if any. Markers
        // (' ', '+', '-') are all ASCII, so indexing by char is equivalent to
        // indexing the first UTF-16 code unit.
        let marker = line.as_utf16().first().copied();
        match marker {
            None => {
                // Empty line: an unchanged blank context line.
                changes.push(HunkChange {
                    r#type: HunkChangeType::Keep,
                    text: RuntimeString::default(),
                });
            }
            Some(value) if value == u16::from(b' ') => {
                changes.push(HunkChange {
                    r#type: HunkChangeType::Keep,
                    text: slice_from_first_char(&line),
                });
            }
            Some(value) if value == u16::from(b'+') => {
                changes.push(HunkChange {
                    r#type: HunkChangeType::Add,
                    text: slice_from_first_char(&line),
                });
            }
            Some(value) if value == u16::from(b'-') => {
                changes.push(HunkChange {
                    r#type: HunkChangeType::Remove,
                    text: slice_from_first_char(&line),
                });
            }
            Some(_) => {
                if parsed == 0 {
                    return Err(PatchParseError::with_line(
                        RuntimeString::from("Unexpected line found in update hunk: '")
                            .concat(&line)
                            .concat(&RuntimeString::from("'. Every line should start with ' ' (context line), '+' (added line), or '-' (removed line)")),
                        line_number + 1,
                    ));
                }
                // Any other leading character starts the next hunk/directive.
                break;
            }
        }
        parsed += 1;
        idx += 1;
    }

    let hunk = Hunk {
        context_header,
        changes,
        is_end_of_file,
    };
    Ok((hunk, parsed + start_index))
}

/// Drop the first character (UTF-8 aware).
fn slice_from_first_char(line: &RuntimeString) -> RuntimeString {
    line.slice_utf16(1..)
}

/// Return `s` without `prefix`, or `None` if `s` does not start with `prefix`.
fn strip_prefix(s: &RuntimeString, prefix: &str) -> Option<RuntimeString> {
    s.starts_with(prefix)
        .then(|| s.slice_utf16(prefix.encode_utf16().count()..))
}

fn runtime_eq(value: &RuntimeString, expected: &str) -> bool {
    value.as_utf16().iter().copied().eq(expected.encode_utf16())
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

fn split_lines(value: &RuntimeString) -> Vec<RuntimeString> {
    let units = value.as_utf16();
    let mut output = Vec::new();
    let mut start = 0;
    for (index, unit) in units.iter().enumerate() {
        if *unit == u16::from(b'\n') {
            output.push(RuntimeString::from_utf16(units[start..index].to_vec()));
            start = index + 1;
        }
    }
    output.push(RuntimeString::from_utf16(units[start..].to_vec()));
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_patch(text: impl Into<RuntimeString>) -> Result<Vec<PatchOp>, PatchParseError> {
        let text = text.into();
        super::parse_patch(&text)
    }

    fn parse_patch_detailed(
        text: impl Into<RuntimeString>,
    ) -> Result<ParsedPatch, PatchParseError> {
        let text = text.into();
        super::parse_patch_detailed(&text)
    }

    fn s(text: &str) -> RuntimeString {
        RuntimeString::from(text)
    }

    /// Build a patch body wrapped in the Begin/End envelope.
    fn wrap(body: &[&str]) -> String {
        let mut lines = vec!["*** Begin Patch".to_string()];
        lines.extend(body.iter().map(|s| s.to_string()));
        lines.push("*** End Patch".to_string());
        lines.join("\n")
    }

    fn keep(text: &str) -> HunkChange {
        HunkChange {
            r#type: HunkChangeType::Keep,
            text: s(text),
        }
    }
    fn add(text: &str) -> HunkChange {
        HunkChange {
            r#type: HunkChangeType::Add,
            text: s(text),
        }
    }
    fn remove(text: &str) -> HunkChange {
        HunkChange {
            r#type: HunkChangeType::Remove,
            text: s(text),
        }
    }

    // --- parse_patch — structure ---

    #[test]
    fn parses_add_delete_update_with_move_in_one_patch() {
        let ops = parse_patch(wrap(&[
            "*** Add File: path/add.py",
            "+abc",
            "+def",
            "*** Delete File: path/delete.py",
            "*** Update File: path/update.py",
            "*** Move to: path/update2.py",
            "@@ def f():",
            "-    pass",
            "+    return 123",
        ]))
        .unwrap();
        assert_eq!(
            ops,
            vec![
                PatchOp::Add {
                    path: s("path/add.py"),
                    lines: vec![s("abc"), s("def")],
                },
                PatchOp::Delete {
                    path: s("path/delete.py"),
                },
                PatchOp::Update {
                    path: s("path/update.py"),
                    move_path: Some(s("path/update2.py")),
                    hunks: vec![Hunk {
                        context_header: Some(s("def f():")),
                        changes: vec![remove("    pass"), add("    return 123")],
                        is_end_of_file: false,
                    }],
                },
            ]
        );
    }

    #[test]
    fn parses_an_update_hunk_immediately_followed_by_another_hunk() {
        let ops = parse_patch(wrap(&[
            "*** Update File: file.py",
            "@@",
            "+line",
            "*** Add File: other.py",
            "+content",
        ]))
        .unwrap();
        assert_eq!(
            ops,
            vec![
                PatchOp::Update {
                    path: s("file.py"),
                    move_path: None,
                    hunks: vec![Hunk {
                        context_header: None,
                        changes: vec![add("line")],
                        is_end_of_file: false,
                    }],
                },
                PatchOp::Add {
                    path: s("other.py"),
                    lines: vec![s("content")],
                },
            ]
        );
    }

    #[test]
    fn allows_an_update_hunk_whose_first_chunk_omits_the_header() {
        let ops = parse_patch(wrap(&["*** Update File: file2.py", " import foo", "+bar"])).unwrap();
        assert_eq!(
            ops,
            vec![PatchOp::Update {
                path: s("file2.py"),
                move_path: None,
                hunks: vec![Hunk {
                    context_header: None,
                    changes: vec![keep("import foo"), add("bar")],
                    is_end_of_file: false,
                }],
            }]
        );
    }

    #[test]
    fn parses_multiple_chunks_within_a_single_update_hunk() {
        let ops = parse_patch(wrap(&[
            "*** Update File: m.py",
            "@@ a",
            " x",
            "-y",
            "+Y",
            "@@ b",
            " p",
            "-q",
            "+Q",
        ]))
        .unwrap();
        match &ops[0] {
            PatchOp::Update { path, hunks, .. } => {
                assert_eq!(path, "m.py");
                assert_eq!(hunks[0].context_header.as_ref(), Some(&s("a")));
                assert_eq!(hunks[1].context_header.as_ref(), Some(&s("b")));
            }
            _ => panic!("expected update"),
        }
    }

    #[test]
    fn marks_the_chunk_terminated_by_end_of_file() {
        let ops = parse_patch(wrap(&[
            "*** Update File: f.py",
            "@@",
            "+line",
            "*** End of File",
        ]))
        .unwrap();
        assert_eq!(
            ops[0],
            PatchOp::Update {
                path: s("f.py"),
                move_path: None,
                hunks: vec![Hunk {
                    context_header: None,
                    changes: vec![add("line")],
                    is_end_of_file: true,
                }],
            }
        );
    }

    #[test]
    fn treats_an_empty_diff_line_as_a_blank_context_line() {
        let ops = parse_patch(wrap(&[
            "*** Update File: f.py",
            "@@ change_context",
            "",
            " context",
            "-remove",
            "+add",
        ]))
        .unwrap();
        match &ops[0] {
            PatchOp::Update { hunks, .. } => {
                assert_eq!(hunks[0].context_header.as_ref(), Some(&s("change_context")));
                assert_eq!(
                    hunks[0].changes,
                    vec![keep(""), keep("context"), remove("remove"), add("add")]
                );
            }
            _ => panic!("expected update"),
        }
    }

    #[test]
    fn tolerates_whitespace_around_the_envelope_markers_and_add_lines() {
        let text = [
            "  *** Begin Patch  ",
            "*** Add File: foo",
            "+hi",
            "*** End Patch ",
        ]
        .join("\n");
        assert_eq!(
            parse_patch(&text).unwrap(),
            vec![PatchOp::Add {
                path: s("foo"),
                lines: vec![s("hi")],
            }]
        );
    }

    #[test]
    fn accepts_an_empty_patch_no_hunks_between_the_markers() {
        assert_eq!(parse_patch(wrap(&[])).unwrap(), Vec::<PatchOp>::new());
    }

    // --- environment id preamble ---

    #[test]
    fn extracts_a_non_empty_environment_id() {
        let parsed = parse_patch_detailed(wrap(&[
            "*** Environment ID: remote",
            "*** Add File: hello.txt",
            "+hello",
        ]))
        .unwrap();
        assert_eq!(parsed.environment_id.as_ref(), Some(&s("remote")));
        assert_eq!(
            parsed.ops,
            vec![PatchOp::Add {
                path: s("hello.txt"),
                lines: vec![s("hello")],
            }]
        );
    }

    #[test]
    fn rejects_an_empty_environment_id() {
        let err = parse_patch(wrap(&[
            "*** Environment ID:   ",
            "*** Add File: hello.txt",
            "+hello",
        ]))
        .unwrap_err();
        assert!(err.message.contains("environment_id cannot be empty"));
    }

    // --- heredoc lenient envelope ---

    #[test]
    fn unwraps_heredocs() {
        let inner = wrap(&["*** Update File: file2.py", " import foo", "+bar"]);
        for (open, close) in [("<<EOF", "EOF"), ("<<'EOF'", "EOF"), ("<<\"EOF\"", "EOF")] {
            let wrapped = [open, inner.as_str(), close].join("\n");
            let ops = parse_patch(&wrapped).unwrap();
            assert_eq!(
                ops,
                vec![PatchOp::Update {
                    path: s("file2.py"),
                    move_path: None,
                    hunks: vec![Hunk {
                        context_header: None,
                        changes: vec![keep("import foo"), add("bar")],
                        is_end_of_file: false,
                    }],
                }]
            );
        }
    }

    #[test]
    fn does_not_unwrap_mismatched_heredoc_quotes() {
        let inner = wrap(&["*** Update File: file2.py", " import foo", "+bar"]);
        let wrapped = ["<<\"EOF'", inner.as_str(), "EOF"].join("\n");
        let err = parse_patch(&wrapped).unwrap_err();
        assert!(err.message.contains("first line of the patch must be"));
    }

    #[test]
    fn reports_the_missing_end_marker_inside_a_heredoc() {
        let wrapped = [
            "<<EOF",
            "*** Begin Patch",
            "*** Update File: file2.py",
            "EOF",
        ]
        .join("\n");
        let err = parse_patch(&wrapped).unwrap_err();
        assert!(err.message.contains("last line of the patch must be"));
    }

    // --- error messages and line numbers ---

    #[test]
    fn reports_the_missing_begin_marker() {
        let err = parse_patch("bad\n*** End Patch").unwrap_err();
        assert_eq!(
            err.message,
            "The first line of the patch must be '*** Begin Patch'"
        );
    }

    #[test]
    fn reports_the_missing_end_marker() {
        let err = parse_patch("*** Begin Patch\nbad").unwrap_err();
        assert_eq!(
            err.message,
            "The last line of the patch must be '*** End Patch'"
        );
    }

    #[test]
    fn reports_the_missing_begin_marker_short() {
        let err = parse_patch("*** Add File: x\n+y\n*** End Patch").unwrap_err();
        assert!(err.message.contains("Begin Patch"));
    }

    #[test]
    fn throws_on_an_unknown_directive() {
        let err = parse_patch(wrap(&["*** Frobnicate File: x"])).unwrap_err();
        assert!(err.message.contains("is not a valid hunk header"));
    }

    #[test]
    fn reports_an_empty_update_hunk_with_its_line_number() {
        let err = parse_patch(wrap(&["*** Update File: test.py"])).unwrap_err();
        assert!(err
            .message
            .contains("Update file hunk for path 'test.py' is empty"));
        assert_eq!(err.line_number, Some(2));
    }

    #[test]
    fn reports_a_bad_first_line_inside_an_update_chunk_that_lacks_context() {
        let err = parse_patch(wrap(&["*** Update File: x.py", "bad"])).unwrap_err();
        assert!(err.message.contains("Unexpected line found in update hunk"));
    }

    #[test]
    fn reports_a_chunk_that_has_no_lines_after_its_header() {
        let err = parse_patch(wrap(&["*** Update File: x.py", "@@"])).unwrap_err();
        assert!(err
            .message
            .contains("Update hunk does not contain any lines"));
    }

    #[test]
    fn preserves_lone_surrogates_in_paths_and_hunk_text() {
        let lone = RuntimeString::from_utf16(vec![0xd800]);
        let patch = RuntimeString::from("*** Begin Patch\n*** Add File: path-")
            .concat(&lone)
            .concat(&RuntimeString::from(".txt\n+value-"))
            .concat(&lone)
            .concat(&RuntimeString::from("\n*** End Patch"));

        let ops = super::parse_patch(&patch).unwrap();
        assert_eq!(
            ops,
            vec![PatchOp::Add {
                path: RuntimeString::from("path-")
                    .concat(&lone)
                    .concat(&RuntimeString::from(".txt")),
                lines: vec![RuntimeString::from("value-").concat(&lone)],
            }]
        );
    }
}
