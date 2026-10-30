//! Markdown as styled terminal lines.
//!
//! ## Why a hand-written renderer and not a parser
//!
//! Assistant replies are markdown, and a terminal has to show them as text a
//! human reads rather than as syntax. What that needs is small: headings,
//! emphasis, inline code, fenced blocks, lists, quotes, rules, links. A full
//! CommonMark parser plus a syntax-highlighting engine would bring a large
//! dependency tree and a theme format, to render a subset that a line-oriented
//! pass already covers.
//!
//! The pass is deliberately line-oriented rather than tree-building, because
//! model output STREAMS. A tree parser needs the whole document before it can
//! lay anything out; this one can be handed the settled text of a block and
//! render it immediately, which is what lets a reply appear as it arrives.
//!
//! ## What it does not do
//!
//! No syntax highlighting inside fenced blocks: the whole block takes the code
//! colour. Highlighting needs a grammar per language, and a wrong guess is worse
//! than none — mis-coloured code reads as broken code. No tables, because a
//! table needs the full block width to size columns and the terminal width is
//! the caller's to decide; a table's source lines stay readable as they are.
//!
//! Both are stated here rather than left to be discovered: a reader wondering
//! why their Rust block is monochrome should find the answer next to the code.

use anstyle::Style;

use super::paint::{self, Painter};
use super::theme::glyph;

/// How far each list level is indented. Two columns is enough to read as nesting
/// without pushing deep lists off a narrow terminal.
const LIST_INDENT: usize = 2;

/// Render `text` as styled lines, hard-wrapped to `columns`.
///
/// `columns` is the full width available; the renderer subtracts its own indents
/// from it, so a caller passing the terminal width gets text that fits.
pub fn render(painter: &Painter, text: &str, columns: usize) -> Vec<String> {
    let palette = *painter.palette();
    let mut out: Vec<String> = Vec::new();
    let mut fenced: Option<String> = None;

    for raw in text.lines() {
        // Inside a fence everything is code, including lines that look like
        // headings or lists. Checking the fence first is what keeps a `# comment`
        // in a shell block from becoming an h1.
        if let Some(fence) = &fenced {
            if is_closing_fence(raw, fence) {
                fenced = None;
                continue;
            }
            out.push(painter.paint(palette.md_code_block, raw));
            continue;
        }
        if let Some(fence) = opening_fence(raw) {
            fenced = Some(fence);
            continue;
        }

        let line = raw.trim_end();
        if line.trim().is_empty() {
            out.push(String::new());
            continue;
        }

        if let Some((level, body)) = heading(line) {
            out.extend(render_heading(painter, level, body, columns));
            continue;
        }
        if is_rule(line) {
            out.push(painter.rule(palette.md_rule, glyph::RULE_LIGHT, columns));
            continue;
        }
        if let Some(body) = quote(line) {
            let rail = painter.paint(palette.md_quote, &format!("{} ", glyph::QUOTE_RAIL));
            for wrapped in wrap(&inline(painter, body), columns.saturating_sub(2)) {
                out.push(format!("{rail}{wrapped}"));
            }
            continue;
        }
        if let Some((indent, marker, body)) = list_item(line) {
            let pad = " ".repeat(indent);
            let bullet = painter.paint(palette.body, &marker);
            let hang = " ".repeat(indent + paint::width(&marker));
            let width = columns.saturating_sub(hang.len());
            for (index, wrapped) in wrap(&inline(painter, body), width).into_iter().enumerate() {
                if index == 0 {
                    out.push(format!("{pad}{bullet}{wrapped}"));
                } else {
                    out.push(format!("{hang}{wrapped}"));
                }
            }
            continue;
        }

        out.extend(wrap(&inline(painter, line), columns));
    }

    // An unterminated fence means the text was cut mid-block. The lines were
    // already emitted as code, which is the honest reading of what arrived.
    out
}

/// The opening fence's run of backticks or tildes, if this line opens one.
fn opening_fence(line: &str) -> Option<String> {
    let trimmed = line.trim_start();
    for cell in ['`', '~'] {
        let run: String = trimmed.chars().take_while(|ch| *ch == cell).collect();
        if run.len() >= 3 {
            return Some(run);
        }
    }
    None
}

/// A fence closes on a run of the same character at least as long as the opener.
fn is_closing_fence(line: &str, opener: &str) -> bool {
    let trimmed = line.trim();
    let cell = opener.chars().next().unwrap_or('`');
    trimmed.len() >= opener.len() && trimmed.chars().all(|ch| ch == cell)
}

/// `(level, text)` for an ATX heading, where level is 1..=6.
fn heading(line: &str) -> Option<(usize, &str)> {
    let hashes = line.chars().take_while(|ch| *ch == '#').count();
    if hashes == 0 || hashes > 6 {
        return None;
    }
    let rest = &line[hashes..];
    // `#foo` is not a heading — ATX requires the space, and treating it as one
    // turns a bare `#tag` into a title.
    let body = rest.strip_prefix(' ')?;
    Some((hashes, body.trim()))
}

fn render_heading(painter: &Painter, level: usize, body: &str, columns: usize) -> Vec<String> {
    let palette = painter.palette();
    let style = match level {
        1 => palette.md_h1,
        2 => palette.md_h2,
        6 => palette.md_h6,
        _ => palette.md_h3,
    };
    let mut out = vec![painter.paint(style, body)];
    // Only the top two levels get an underline, and it is the width of the TEXT
    // rather than of the terminal: a full-width rule under every `###` turns a
    // structured reply into a stack of horizontal lines.
    if level <= 2 {
        let cell = if level == 1 {
            glyph::RULE_HEAVY
        } else {
            glyph::RULE_LIGHT
        };
        let width = paint::width(body).min(columns);
        out.push(painter.rule(style, cell, width));
    }
    out
}

/// A thematic break: three or more of `-`, `*` or `_`, and nothing else.
fn is_rule(line: &str) -> bool {
    let trimmed: String = line.chars().filter(|ch| !ch.is_whitespace()).collect();
    if trimmed.len() < 3 {
        return false;
    }
    ['-', '*', '_']
        .iter()
        .any(|cell| trimmed.chars().all(|ch| ch == *cell))
}

fn quote(line: &str) -> Option<&str> {
    let body = line.trim_start().strip_prefix('>')?;
    Some(body.strip_prefix(' ').unwrap_or(body))
}

/// `(indent, marker, body)` for a list item.
///
/// The marker keeps its own trailing space so the caller can measure the hanging
/// indent from it directly, which is what aligns a wrapped item under its text
/// instead of under its bullet.
fn list_item(line: &str) -> Option<(usize, String, &str)> {
    let indent_cols = line.len() - line.trim_start().len();
    let trimmed = line.trim_start();
    let depth = indent_cols / LIST_INDENT;
    let indent = depth * LIST_INDENT;

    for marker in ['-', '*', '+'] {
        if let Some(body) = trimmed.strip_prefix(marker) {
            if let Some(body) = body.strip_prefix(' ') {
                return Some((indent, format!("{} ", glyph::BULLET), body));
            }
        }
    }

    // Ordered: digits, then `.` or `)`, then a space. The original number is
    // kept rather than renumbered — a model that starts at 3 usually means it.
    let digits: String = trimmed.chars().take_while(char::is_ascii_digit).collect();
    if digits.is_empty() || digits.len() > 3 {
        return None;
    }
    let rest = &trimmed[digits.len()..];
    for sep in ['.', ')'] {
        if let Some(body) = rest.strip_prefix(sep) {
            if let Some(body) = body.strip_prefix(' ') {
                return Some((indent, format!("{digits}{sep} "), body));
            }
        }
    }
    None
}

/// Apply inline styling: `**bold**`, `*italic*`, `` `code` ``, `~~strike~~` and
/// `[text](url)`.
///
/// Markers are consumed rather than kept. Leaving them in and adding colour
/// shows the reader the syntax twice; the point of rendering is that the styling
/// replaces the syntax.
fn inline(painter: &Painter, text: &str) -> String {
    let palette = *painter.palette();
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::new();
    let mut index = 0;

    while index < chars.len() {
        let rest = &chars[index..];

        // Code first: a backtick span is literal, so `**` inside it is text.
        if rest[0] == '`' {
            if let Some((body, used)) = delimited(rest, "`") {
                out.push_str(&painter.paint(palette.md_code, &body));
                index += used;
                continue;
            }
        }
        if starts_with(rest, "**") {
            if let Some((body, used)) = delimited(rest, "**") {
                out.push_str(&painter.paint(Style::new().bold(), &body));
                index += used;
                continue;
            }
        }
        if starts_with(rest, "~~") {
            if let Some((body, used)) = delimited(rest, "~~") {
                out.push_str(&painter.paint(Style::new().strikethrough(), &body));
                index += used;
                continue;
            }
        }
        if rest[0] == '*' || rest[0] == '_' {
            let marker = rest[0].to_string();
            if let Some((body, used)) = delimited(rest, &marker) {
                // An underscore inside a word is part of the word
                // (`some_var_name`), not emphasis.
                let joins_word = index > 0 && chars[index - 1].is_alphanumeric() && marker == "_";
                if !joins_word && !body.trim().is_empty() {
                    out.push_str(&painter.paint(Style::new().italic(), &body));
                    index += used;
                    continue;
                }
            }
        }
        if rest[0] == '[' {
            if let Some((label, url, used)) = link(rest) {
                out.push_str(&painter.paint(palette.md_link, &label));
                // The URL follows in parentheses because a terminal cannot make
                // text clickable portably, and a link whose target is hidden is
                // a link the reader cannot follow.
                out.push_str(&painter.paint(palette.md_link_url, &format!(" ({url})")));
                index += used;
                continue;
            }
        }

        out.push(chars[index]);
        index += 1;
    }
    out
}

fn starts_with(chars: &[char], prefix: &str) -> bool {
    let prefix: Vec<char> = prefix.chars().collect();
    chars.len() >= prefix.len() && chars[..prefix.len()] == prefix[..]
}

/// The body between a leading `marker` and its next occurrence, plus how many
/// chars were consumed. `None` when the span never closes on this line.
fn delimited(chars: &[char], marker: &str) -> Option<(String, usize)> {
    let marker: Vec<char> = marker.chars().collect();
    let start = marker.len();
    let mut cursor = start;
    while cursor + marker.len() <= chars.len() {
        if chars[cursor..cursor + marker.len()] == marker[..] {
            if cursor == start {
                return None; // empty span, e.g. `**`
            }
            let body: String = chars[start..cursor].iter().collect();
            return Some((body, cursor + marker.len()));
        }
        cursor += 1;
    }
    None
}

/// `[label](url)` → `(label, url, consumed)`.
fn link(chars: &[char]) -> Option<(String, String, usize)> {
    let close = chars.iter().position(|ch| *ch == ']')?;
    if chars.get(close + 1) != Some(&'(') {
        return None;
    }
    let paren = chars[close + 2..].iter().position(|ch| *ch == ')')? + close + 2;
    let label: String = chars[1..close].iter().collect();
    let url: String = chars[close + 2..paren].iter().collect();
    if label.is_empty() || url.is_empty() {
        return None;
    }
    Some((label, url, paren + 1))
}

/// Hard-wrap already-styled text at `columns`, breaking on spaces.
///
/// Wrapping must measure the VISIBLE width, so escapes are skipped while
/// counting and carried through to the output. Counting them would wrap early
/// and leave a ragged right edge on exactly the lines that had colour in them.
fn wrap(text: &str, columns: usize) -> Vec<String> {
    if columns == 0 {
        return vec![text.to_string()];
    }
    let mut lines: Vec<String> = Vec::new();
    let mut line = String::new();
    let mut used = 0;
    // Where the last space in the current line is, so a break can rewind to it
    // rather than splitting a word.
    let mut break_at: Option<(usize, usize)> = None;

    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' {
            line.push(ch);
            for next in chars.by_ref() {
                line.push(next);
                if next.is_ascii_alphabetic() {
                    break;
                }
            }
            continue;
        }
        let ch_width = paint::width(&ch.to_string());
        // Only a VISIBLE character can force a break. A space sitting exactly on
        // the boundary gets trimmed when the line is pushed, so letting it
        // trigger the wrap would rewind to the previous space and cut a word
        // early — `aaa bbb` at 7 columns came out as `aaa`.
        if ch != ' ' && used + ch_width > columns {
            match break_at {
                Some((byte, cells)) if cells > 0 => {
                    let tail = line[byte..].trim_start().to_string();
                    line.truncate(byte);
                    lines.push(line.trim_end().to_string());
                    used = paint::width(&strip(&tail));
                    line = tail;
                }
                _ => {
                    lines.push(std::mem::take(&mut line));
                    used = 0;
                }
            }
            break_at = None;
        }
        if ch == ' ' {
            break_at = Some((line.len(), used));
        }
        line.push(ch);
        used += ch_width;
    }
    if !line.is_empty() {
        lines.push(line);
    }
    if lines.is_empty() {
        lines.push(String::new());
    }
    lines
}

fn strip(text: &str) -> String {
    let mut out = String::new();
    let mut chars = text.chars();
    while let Some(ch) = chars.next() {
        if ch != '\u{1b}' {
            out.push(ch);
            continue;
        }
        for next in chars.by_ref() {
            if next.is_ascii_alphabetic() {
                break;
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::panel::visible_width;
    use crate::tui::theme::Palette;

    fn plain() -> Painter {
        Painter::plain()
    }

    fn styled() -> Painter {
        Painter::new(Palette::dark(), true)
    }

    #[test]
    fn headings_carry_an_underline_only_at_the_top_two_levels() {
        assert_eq!(render(&plain(), "# Title", 40), vec!["Title", "═════"]);
        assert_eq!(render(&plain(), "## Sub", 40), vec!["Sub", "───"]);
        // Deeper levels are weight-only: a rule under every ### would turn a
        // structured reply into a stack of lines.
        assert_eq!(render(&plain(), "### Deep", 40), vec!["Deep"]);
        assert_eq!(render(&plain(), "###### Tiny", 40), vec!["Tiny"]);
    }

    /// The underline tracks the text, not the terminal.
    #[test]
    fn a_heading_underline_is_as_wide_as_its_text() {
        let out = render(&plain(), "# Hi", 80);
        assert_eq!(paint::width(&out[1]), 2);
    }

    #[test]
    fn a_bare_hash_is_not_a_heading() {
        assert_eq!(render(&plain(), "#nothashtag", 40), vec!["#nothashtag"]);
        // Seven hashes is past the scale, so it is text.
        assert_eq!(render(&plain(), "####### x", 40), vec!["####### x"]);
    }

    #[test]
    fn lists_keep_their_numbering_and_hang_wrapped_text() {
        assert_eq!(render(&plain(), "- one", 40), vec!["• one"]);
        assert_eq!(render(&plain(), "3. three", 40), vec!["3. three"]);
        // Nesting indents by two columns per level.
        assert_eq!(render(&plain(), "  - nested", 40), vec!["  • nested"]);

        // A wrapped item lines up under its text, not under its bullet.
        let out = render(&plain(), "- aaa bbb ccc ddd", 9);
        assert_eq!(out[0], "• aaa bbb");
        assert!(out[1].starts_with("  "), "{:?} should hang", out[1]);
    }

    #[test]
    fn a_fence_suspends_every_other_rule() {
        let text = "```sh\n# not a heading\n- not a list\n```\nafter";
        let out = render(&plain(), text, 40);
        assert_eq!(out, vec!["# not a heading", "- not a list", "after"]);
    }

    /// A fence that never closes still renders its lines rather than swallowing
    /// them: streamed output is regularly cut mid-block.
    #[test]
    fn an_unterminated_fence_still_emits_its_lines() {
        let out = render(&plain(), "```\nstill here", 40);
        assert_eq!(out, vec!["still here"]);
    }

    #[test]
    fn inline_markers_are_replaced_rather_than_shown() {
        assert_eq!(render(&plain(), "**bold** text", 40), vec!["bold text"]);
        assert_eq!(render(&plain(), "*it* and _it_", 40), vec!["it and it"]);
        assert_eq!(render(&plain(), "`code`", 40), vec!["code"]);
        assert_eq!(render(&plain(), "~~gone~~", 40), vec!["gone"]);
    }

    /// An underscore inside an identifier is part of the identifier.
    #[test]
    fn snake_case_survives_emphasis_parsing() {
        assert_eq!(render(&plain(), "some_var_name", 40), vec!["some_var_name"]);
    }

    /// Backticks win: markdown inside a code span is literal text.
    #[test]
    fn code_spans_are_literal() {
        assert_eq!(render(&plain(), "`a **b** c`", 40), vec!["a **b** c"]);
    }

    /// An unclosed marker is text, not a style that runs to end of line.
    #[test]
    fn an_unclosed_marker_stays_literal() {
        assert_eq!(render(&plain(), "**not closed", 40), vec!["**not closed"]);
        assert_eq!(render(&plain(), "a * b", 40), vec!["a * b"]);
    }

    #[test]
    fn links_show_their_target() {
        assert_eq!(
            render(&plain(), "see [docs](https://x.dev)", 60),
            vec!["see docs (https://x.dev)"]
        );
        // Not a link: no parenthesised target.
        assert_eq!(
            render(&plain(), "[just brackets]", 40),
            vec!["[just brackets]"]
        );
    }

    #[test]
    fn quotes_get_a_left_rail() {
        let out = render(&plain(), "> quoted", 40);
        assert_eq!(out, vec!["▌ quoted"]);
    }

    #[test]
    fn a_thematic_break_fills_the_width() {
        assert_eq!(paint::width(&render(&plain(), "---", 12)[0]), 12);
        assert_eq!(paint::width(&render(&plain(), "***", 12)[0]), 12);
        // Two dashes is text, three is a rule.
        assert_eq!(render(&plain(), "--", 12), vec!["--"]);
    }

    /// Wrapping measures visible width: a styled line must break at the same
    /// place as the plain one, or coloured paragraphs come out ragged.
    #[test]
    fn wrapping_ignores_escape_sequences() {
        let plain_out = render(&plain(), "**aaa** bbb ccc ddd eee", 11);
        let styled_out = render(&styled(), "**aaa** bbb ccc ddd eee", 11);
        assert_eq!(plain_out.len(), styled_out.len());
        for (index, line) in styled_out.iter().enumerate() {
            assert!(
                visible_width(line) <= 11,
                "line {index} is {} cells: {line:?}",
                visible_width(line)
            );
        }
    }

    #[test]
    fn wrapping_breaks_on_spaces_and_never_exceeds_the_width() {
        let out = render(&plain(), "alpha beta gamma delta", 12);
        for line in &out {
            assert!(paint::width(line) <= 12, "{line:?}");
        }
        assert_eq!(out[0], "alpha beta");
    }

    /// A single word longer than the width has to be cut somewhere; it must not
    /// overflow the line.
    #[test]
    fn an_overlong_word_is_split_rather_than_overflowing() {
        let out = render(&plain(), "supercalifragilistic", 8);
        assert!(out.len() > 1);
        for line in &out {
            assert!(paint::width(line) <= 8, "{line:?}");
        }
    }

    #[test]
    fn blank_lines_are_preserved_as_paragraph_separation() {
        assert_eq!(render(&plain(), "a\n\nb", 40), vec!["a", "", "b"]);
    }

    /// Wide characters must not push a line past the width.
    #[test]
    fn cjk_text_wraps_by_cells() {
        let out = render(&plain(), "中文字符串很长很长", 8);
        for line in &out {
            assert!(
                paint::width(line) <= 8,
                "{line:?} is {}",
                paint::width(line)
            );
        }
        assert!(out.len() > 1);
    }
}
