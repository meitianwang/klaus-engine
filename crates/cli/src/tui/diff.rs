//! File changes, as a diff a terminal can read.
//!
//! ## Two encodings, on purpose
//!
//! Added and removed lines carry BOTH a marker character (`+` / `-`) and a
//! background wash. That is redundant only when colour is live: over a pipe,
//! under `NO_COLOR`, or on a terminal that renders both washes as the same grey,
//! the marker is the sole thing distinguishing an addition from a deletion. A
//! diff that relies on colour alone becomes unreadable exactly where reading it
//! matters most.
//!
//! ## Two densities
//!
//! [`preview`] is the few lines shown under a tool call, so a reader can see what
//! changed without leaving the transcript. [`panel`] is the full hunk view, boxed
//! and with a line-number gutter. Both come from the same parse, so they cannot
//! disagree about what changed.
//!
//! Line numbers matter more than they look: without them a reader cannot take a
//! reported change back to the file. They come from the hunk headers rather than
//! from counting, because a diff may start at any line.

use anstyle::Style;

use super::paint::{self, Painter};
use super::panel::Panel;
use super::theme::{glyph, limit};

/// What one line of a diff is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Added,
    Removed,
    Context,
    /// A jump between hunks. Carries no text: it stands for the lines not shown.
    Gap,
}

#[derive(Debug, Clone)]
pub struct Line {
    pub kind: Kind,
    /// The line's number in the NEW file, or in the old file for a removal.
    /// `None` for a gap.
    pub number: Option<u32>,
    pub text: String,
}

/// One file's changes.
#[derive(Debug, Clone)]
pub struct FileDiff {
    pub path: String,
    pub added: usize,
    pub removed: usize,
    pub lines: Vec<Line>,
}

/// Parse a unified diff body (the part after any `---`/`+++` header).
///
/// Anything that is not a hunk header or a `+`/`-`/space line is skipped rather
/// than rendered: `diff --git`, index lines and mode changes are plumbing, and
/// showing them turns a two-line edit into a wall of metadata.
pub fn parse_unified(path: &str, body: &str) -> FileDiff {
    let mut lines: Vec<Line> = Vec::new();
    let mut added = 0;
    let mut removed = 0;
    let mut old_no: u32 = 0;
    let mut new_no: u32 = 0;
    let mut seen_hunk = false;

    for raw in body.lines() {
        if let Some((old_start, new_start)) = hunk_header(raw) {
            // A gap stands between hunks, never before the first one.
            if seen_hunk {
                lines.push(Line {
                    kind: Kind::Gap,
                    number: None,
                    text: String::new(),
                });
            }
            seen_hunk = true;
            old_no = old_start;
            new_no = new_start;
            continue;
        }
        if !seen_hunk {
            continue;
        }
        let mut chars = raw.chars();
        match chars.next() {
            Some('+') => {
                added += 1;
                lines.push(Line {
                    kind: Kind::Added,
                    number: Some(new_no),
                    text: chars.collect(),
                });
                new_no += 1;
            }
            Some('-') => {
                removed += 1;
                lines.push(Line {
                    kind: Kind::Removed,
                    number: Some(old_no),
                    text: chars.collect(),
                });
                old_no += 1;
            }
            Some(' ') => {
                lines.push(Line {
                    kind: Kind::Context,
                    number: Some(new_no),
                    text: chars.collect(),
                });
                old_no += 1;
                new_no += 1;
            }
            // `\ No newline at end of file` and blank separators carry no change.
            _ => {}
        }
    }

    FileDiff {
        path: path.to_string(),
        added,
        removed,
        lines,
    }
}

/// `@@ -old,len +new,len @@` → the two start line numbers.
fn hunk_header(line: &str) -> Option<(u32, u32)> {
    let rest = line.strip_prefix("@@ ")?;
    let (ranges, _) = rest.split_once(" @@")?;
    let (old, new) = ranges.split_once(' ')?;
    let start = |part: &str, sign: char| -> Option<u32> {
        let digits = part.strip_prefix(sign)?;
        let digits = digits.split(',').next()?;
        digits.parse().ok()
    };
    Some((start(old, '-')?, start(new, '+')?))
}

/// `+12 -3 src/foo.rs` — the one-line summary that heads a change.
pub fn header(painter: &Painter, diff: &FileDiff) -> String {
    let palette = painter.palette();
    let mut spans: Vec<(Style, String)> = Vec::new();
    if diff.added > 0 {
        spans.push((palette.diff_add_head, format!("+{} ", diff.added)));
    }
    if diff.removed > 0 {
        spans.push((palette.diff_del_head, format!("-{} ", diff.removed)));
    }
    spans.push((palette.body, diff.path.clone()));
    painter.spans(
        &spans
            .iter()
            .map(|(style, text)| (*style, text.as_str()))
            .collect::<Vec<_>>(),
    )
}

/// The compact form: at most [`limit::DIFF_PREVIEW_LINES`] CHANGED lines, with a
/// note when more were withheld.
///
/// Context lines are dropped entirely here. In a few lines of screen space the
/// changes are what the reader is after; unchanged lines would crowd them out.
pub fn preview(painter: &Painter, diff: &FileDiff, columns: usize) -> Vec<String> {
    let palette = painter.palette();
    let changed: Vec<&Line> = diff
        .lines
        .iter()
        .filter(|line| matches!(line.kind, Kind::Added | Kind::Removed))
        .collect();
    let gutter = gutter_width(&changed);
    let mut out: Vec<String> = Vec::new();

    for line in changed.iter().take(limit::DIFF_PREVIEW_LINES) {
        out.push(row(painter, line, gutter, columns, false));
    }
    let hidden = changed.len().saturating_sub(limit::DIFF_PREVIEW_LINES);
    if hidden > 0 {
        let plural = if hidden == 1 { "line" } else { "lines" };
        out.push(painter.paint(
            palette.faint,
            &format!("{} {hidden} more {plural}", glyph::ELLIPSIS),
        ));
    }
    out
}

/// The full form: every parsed line, in a bordered panel with a number gutter.
pub fn panel(painter: &Painter, diff: &FileDiff, columns: usize) -> Vec<String> {
    let palette = painter.palette();
    let refs: Vec<&Line> = diff.lines.iter().collect();
    let gutter = gutter_width(&refs);
    // The panel's own border and padding cost four columns; rows are laid out
    // against what is left so the frame stays square.
    let inner = columns.saturating_sub(4);
    let body: Vec<String> = diff
        .lines
        .iter()
        .map(|line| row(painter, line, gutter, inner, true))
        .collect();
    let title = header(painter, diff);
    Panel::new(palette.faint)
        .title(&title)
        .render(painter, &body, columns)
}

/// The widest line number in this set, so every row's gutter is the same width.
/// Two columns minimum: a one-digit gutter reads as an accident.
fn gutter_width(lines: &[&Line]) -> usize {
    lines
        .iter()
        .filter_map(|line| line.number)
        .map(|number| number.to_string().len())
        .max()
        .unwrap_or(2)
        .max(2)
}

/// One row: `<number> <marker> <text>`.
///
/// The wash is applied to the WHOLE row rather than to the text alone, so the
/// changed region reads as a band. Applying it only to the text leaves the
/// gutter unpainted and the eye loses the row.
fn row(painter: &Painter, line: &Line, gutter: usize, columns: usize, wash: bool) -> String {
    let palette = painter.palette();
    if line.kind == Kind::Gap {
        return format!(
            "{} ",
            painter.paint(palette.faint, &paint::pad_left(glyph::DIFF_GAP, gutter))
        );
    }

    let number = line.number.map(|n| n.to_string()).unwrap_or_default();
    let (marker, marker_style, row_style) = match line.kind {
        Kind::Added => (" + ", palette.diff_add_marker, palette.diff.add_bg),
        Kind::Removed => (" - ", palette.diff_del_marker, palette.diff.del_bg),
        // Three spaces, matching the markers' width, so context text lines up
        // with changed text instead of sitting three columns left of it.
        _ => ("   ", palette.body, Style::new()),
    };

    let head_cells = gutter + paint::width(marker);
    let text = paint::truncate_right(&line.text, columns.saturating_sub(head_cells));
    let plain = format!("{}{marker}{text}", paint::pad_left(&number, gutter));

    if !wash || row_style == Style::new() || !painter.is_enabled() {
        return painter.spans(&[
            (palette.faint, paint::pad_left(&number, gutter).as_str()),
            (marker_style, marker),
            (palette.body, text.as_str()),
        ]);
    }
    // With a wash on, the row is painted as one span so the background is
    // continuous — and padded to the full width, or the band stops at the end of
    // the text and looks torn.
    painter.paint(row_style, &paint::pad_right(&plain, columns))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::panel::visible_width;
    use crate::tui::theme::Palette;

    const SAMPLE: &str = "\
@@ -10,3 +10,4 @@
 unchanged one
-gone
+new one
+new two
@@ -40,2 +41,2 @@
 tail context
-old tail
+new tail
";

    fn plain() -> Painter {
        Painter::plain()
    }

    fn styled() -> Painter {
        Painter::new(Palette::dark(), true)
    }

    #[test]
    fn parsing_counts_changes_and_numbers_lines_from_the_hunk_header() {
        let diff = parse_unified("src/foo.rs", SAMPLE);
        assert_eq!(diff.added, 3);
        assert_eq!(diff.removed, 2);

        // Numbering starts where the header says, not at 1.
        let first = diff.lines.iter().find(|l| l.kind == Kind::Context).unwrap();
        assert_eq!(first.number, Some(10));
        let added: Vec<u32> = diff
            .lines
            .iter()
            .filter(|l| l.kind == Kind::Added)
            .filter_map(|l| l.number)
            .collect();
        assert_eq!(added, vec![11, 12, 42]);
    }

    /// A removal is numbered in the OLD file: that is the only place it exists.
    /// The context line ahead of it consumes a number, so the first removal here
    /// is old line 11 rather than the hunk's start.
    #[test]
    fn removals_are_numbered_in_the_old_file() {
        let diff = parse_unified("f", SAMPLE);
        let removed: Vec<u32> = diff
            .lines
            .iter()
            .filter(|l| l.kind == Kind::Removed)
            .filter_map(|l| l.number)
            .collect();
        assert_eq!(removed, vec![11, 41]);
    }

    #[test]
    fn a_gap_sits_between_hunks_and_never_before_the_first() {
        let diff = parse_unified("f", SAMPLE);
        let gaps: Vec<usize> = diff
            .lines
            .iter()
            .enumerate()
            .filter(|(_, l)| l.kind == Kind::Gap)
            .map(|(i, _)| i)
            .collect();
        assert_eq!(gaps.len(), 1, "two hunks means exactly one gap");
        assert!(gaps[0] > 0, "a gap must not lead the diff");
    }

    /// Plumbing lines are skipped: showing them buries a two-line edit.
    #[test]
    fn git_plumbing_is_skipped() {
        let body = "diff --git a/f b/f\nindex 1234567..89abcde 100644\n--- a/f\n+++ b/f\n@@ -1,1 +1,1 @@\n-a\n+b\n";
        let diff = parse_unified("f", body);
        assert_eq!(diff.added, 1);
        assert_eq!(diff.removed, 1);
        assert_eq!(
            diff.lines.len(),
            2,
            "only the two changed lines: {:?}",
            diff.lines
        );
    }

    /// Content before any hunk header is not diff content.
    #[test]
    fn text_before_the_first_hunk_is_ignored() {
        let diff = parse_unified("f", "some preamble\n+not a change\n");
        assert_eq!(diff.added, 0);
        assert!(diff.lines.is_empty());
    }

    #[test]
    fn the_header_reads_as_a_count_then_a_path() {
        let diff = parse_unified("src/foo.rs", SAMPLE);
        assert_eq!(header(&plain(), &diff), "+3 -2 src/foo.rs");

        // A pure addition shows no removal count.
        let added_only = parse_unified("n.rs", "@@ -0,0 +1,1 @@\n+only\n");
        assert_eq!(header(&plain(), &added_only), "+1 n.rs");
    }

    /// Both encodings must be present: over a pipe the marker is all there is.
    #[test]
    fn every_changed_row_carries_a_marker_even_without_colour() {
        let diff = parse_unified("f", SAMPLE);
        let rows = preview(&plain(), &diff, 60);
        assert!(rows.iter().any(|r| r.contains(" + ")), "{rows:?}");
        assert!(rows.iter().any(|r| r.contains(" - ")), "{rows:?}");
        for row in &rows {
            assert!(!row.contains('\u{1b}'), "plain output must not be styled");
        }
    }

    #[test]
    fn the_preview_shows_only_changes_and_says_how_many_it_withheld() {
        let mut body = String::from("@@ -1,1 +1,20 @@\n context line\n");
        for index in 0..10 {
            body.push_str(&format!("+added {index}\n"));
        }
        let diff = parse_unified("f", &body);
        let rows = preview(&plain(), &diff, 60);

        assert_eq!(
            rows.len(),
            limit::DIFF_PREVIEW_LINES + 1,
            "six rows plus the note"
        );
        assert!(
            !rows.iter().any(|r| r.contains("context line")),
            "context is dropped"
        );
        let note = rows.last().unwrap();
        assert!(note.contains("4 more lines"), "{note:?}");
    }

    /// Singular when one line is hidden — "1 more lines" reads as a bug.
    #[test]
    fn the_withheld_note_agrees_in_number() {
        let mut body = String::from("@@ -1,1 +1,9 @@\n");
        for index in 0..7 {
            body.push_str(&format!("+a {index}\n"));
        }
        let diff = parse_unified("f", &body);
        let note = preview(&plain(), &diff, 60).pop().unwrap();
        assert!(note.contains("1 more line"), "{note:?}");
        assert!(!note.contains("1 more lines"), "{note:?}");
    }

    #[test]
    fn a_short_diff_has_no_withheld_note() {
        let diff = parse_unified("f", "@@ -1,1 +1,1 @@\n-a\n+b\n");
        let rows = preview(&plain(), &diff, 60);
        assert_eq!(rows.len(), 2);
        assert!(!rows.iter().any(|r| r.contains("more line")));
    }

    /// Every row's gutter is the same width, or the marker column zig-zags.
    #[test]
    fn the_gutter_is_uniform_and_at_least_two_columns() {
        let diff = parse_unified("f", "@@ -8,1 +8,3 @@\n+a\n+b\n");
        let rows = preview(&plain(), &diff, 60);
        // Numbers 8 and 9: one digit each, padded to the two-column minimum.
        assert!(rows[0].starts_with(" 8 + "), "{:?}", rows[0]);
        assert!(rows[1].starts_with(" 9 + "), "{:?}", rows[1]);

        let wide = parse_unified("f", "@@ -998,1 +998,3 @@\n+a\n+b\n");
        let wide_rows = preview(&plain(), &wide, 60);
        assert!(wide_rows[0].starts_with("998 + "), "{:?}", wide_rows[0]);
    }

    /// Context text has to line up with changed text.
    #[test]
    fn context_rows_reserve_the_marker_column() {
        let diff = parse_unified("f", "@@ -1,2 +1,2 @@\n same\n+new\n");
        let rows = panel(&plain(), &diff, 60);
        let same = rows.iter().find(|r| r.contains("same")).unwrap();
        let new = rows.iter().find(|r| r.contains("new")).unwrap();
        let column = |row: &str, needle: &str| row.find(needle).unwrap();
        assert_eq!(
            column(same, "same"),
            column(new, "new"),
            "text columns must align"
        );
    }

    #[test]
    fn the_panel_is_square_and_titled_with_the_summary() {
        let diff = parse_unified("src/foo.rs", SAMPLE);
        let rows = panel(&plain(), &diff, 60);
        let widths: Vec<usize> = rows.iter().map(|r| visible_width(r)).collect();
        assert!(
            widths.windows(2).all(|pair| pair[0] == pair[1]),
            "ragged diff panel: {widths:?}"
        );
        assert!(rows[0].contains("src/foo.rs"), "{:?}", rows[0]);
        for width in widths {
            assert!(width <= 60);
        }
    }

    /// With colour on, a washed row is padded to full width so the band does not
    /// stop at the end of the text.
    #[test]
    fn a_washed_row_spans_the_whole_width() {
        let diff = parse_unified("f", "@@ -1,1 +1,1 @@\n+short\n");
        let rows = panel(&styled(), &diff, 40);
        let added = rows.iter().find(|r| r.contains("short")).unwrap();
        assert!(added.contains('\u{1b}'), "should be styled");
        // The panel keeps every row the same visible width, wash included.
        let widths: Vec<usize> = rows.iter().map(|r| visible_width(r)).collect();
        assert!(
            widths.windows(2).all(|pair| pair[0] == pair[1]),
            "{widths:?}"
        );
    }

    /// A long line is cut, not wrapped: a wrapped diff row loses its alignment
    /// with the gutter and stops being readable as a diff.
    #[test]
    fn overlong_rows_are_cut_to_the_width() {
        let long = "x".repeat(300);
        let diff = parse_unified("f", &format!("@@ -1,1 +1,1 @@\n+{long}\n"));
        for row in preview(&plain(), &diff, 40) {
            assert!(paint::width(&row) <= 40, "{} cells", paint::width(&row));
        }
        for row in panel(&plain(), &diff, 40) {
            assert!(visible_width(&row) <= 40, "{} cells", visible_width(&row));
        }
    }

    #[test]
    fn an_empty_diff_renders_nothing_but_stays_well_formed() {
        let diff = parse_unified("f", "");
        assert_eq!(diff.added, 0);
        assert!(preview(&plain(), &diff, 40).is_empty());
        // The panel still draws its frame, so the path is still reported.
        assert!(panel(&plain(), &diff, 40).len() >= 2);
    }
}
