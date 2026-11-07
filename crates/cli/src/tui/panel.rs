//! Boxes and rules: the two containers everything else is drawn inside.
//!
//! A [`Panel`] is the rounded box used for the banner, an approval request, a
//! diff and a side answer. A [`rule`] is the horizontal line with an inlaid
//! title that heads the input area.
//!
//! Both take a cell width and return finished lines. Nothing here wraps to the
//! terminal on its own: the caller knows how much room it has, and a container
//! that measured the terminal itself could not be tested headless.
//!
//! ## Why the box is built by hand
//!
//! A panel's border has to be painted in one style while its contents carry
//! their own — including ANSI already embedded by an inner renderer. Measuring
//! such a line means measuring its VISIBLE width, so [`visible_width`] strips
//! escapes before counting. Getting that wrong shows up as a border that steps
//! in and out by a few columns on exactly the lines that had colour in them.

use anstyle::Style;

use super::paint::{self, Painter};
use super::theme::glyph;

/// How wide the text is once escape sequences are discounted.
///
/// Inner renderers hand back styled text, so the naive width of a border row
/// would include every `\x1b[...m` byte and the box would come out ragged.
pub fn visible_width(text: &str) -> usize {
    paint::width(&strip_escapes(text))
}

/// The same text with SGR escape sequences removed.
fn strip_escapes(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(ch) = chars.next() {
        if ch != '\u{1b}' {
            out.push(ch);
            continue;
        }
        // `ESC [ ... m` — consume through the terminating byte so the parameter
        // digits do not land in the output and inflate the width.
        for next in chars.by_ref() {
            if next.is_ascii_alphabetic() {
                break;
            }
        }
    }
    out
}

/// A rounded box with an optional left-aligned title.
pub struct Panel<'a> {
    title: Option<&'a str>,
    border: Style,
    /// Blank rows inside the top and bottom borders. One for a dense panel such
    /// as a diff, more for the banner, which is the one thing meant to breathe.
    pad_rows: usize,
    /// Columns of space between the border and the content.
    pad_cols: usize,
}

impl<'a> Panel<'a> {
    pub fn new(border: Style) -> Self {
        Self {
            title: None,
            border,
            pad_rows: 0,
            pad_cols: 1,
        }
    }

    pub fn title(mut self, title: &'a str) -> Self {
        self.title = Some(title);
        self
    }

    pub fn pad_rows(mut self, rows: usize) -> Self {
        self.pad_rows = rows;
        self
    }

    pub fn pad_cols(mut self, cols: usize) -> Self {
        self.pad_cols = cols;
        self
    }

    /// Draw the box around `body`, sized to fit its widest line.
    ///
    /// `max_width` is a ceiling, not a target: a panel that always filled the
    /// terminal would put a three-word approval prompt inside a 200-column
    /// frame. Content wider than the ceiling is cut, because a border that wraps
    /// is worse than one that elides.
    pub fn render(&self, painter: &Painter, body: &[String], max_width: usize) -> Vec<String> {
        // Two border columns plus the padding on each side.
        let frame = 2 + self.pad_cols * 2;
        let ceiling = max_width.saturating_sub(frame);
        let title_cells = self.title.map_or(0, |title| visible_width(title) + 2);
        let inner = body
            .iter()
            .map(|line| visible_width(line))
            .chain(std::iter::once(title_cells))
            .max()
            .unwrap_or(0)
            .min(ceiling)
            .max(1);

        let pad = " ".repeat(self.pad_cols);
        let mut out = Vec::with_capacity(body.len() + 2 + self.pad_rows * 2);
        out.push(self.top(painter, inner));
        for _ in 0..self.pad_rows {
            out.push(self.row(painter, &pad, "", inner));
        }
        for line in body {
            out.push(self.row(painter, &pad, line, inner));
        }
        for _ in 0..self.pad_rows {
            out.push(self.row(painter, &pad, "", inner));
        }
        out.push(self.bottom(painter, inner));
        out
    }

    fn top(&self, painter: &Painter, inner: usize) -> String {
        let mut edge = String::from(glyph::BOX_TOP_LEFT);
        match self.title {
            // The title sits in the top border with a single space either side,
            // so the eye reads it as a label on the frame rather than content.
            //
            // The fill spans the content width PLUS the padding columns, because
            // a body row is `border + pad + content + pad + border`. Sizing it
            // against `inner` alone leaves the top edge a column short of every
            // other row, which only looks square when the label happens to be
            // exactly one cell wider than the content.
            Some(title) => {
                let label = format!("{} {title} ", glyph::BOX_HORIZONTAL);
                let used = visible_width(&label);
                let span = inner + self.pad_cols * 2;
                edge.push_str(&label);
                edge.push_str(&glyph::BOX_HORIZONTAL.repeat(span.saturating_sub(used)));
            }
            None => edge.push_str(&glyph::BOX_HORIZONTAL.repeat(inner + self.pad_cols * 2)),
        }
        edge.push_str(glyph::BOX_TOP_RIGHT);
        painter.paint(self.border, &edge)
    }

    fn bottom(&self, painter: &Painter, inner: usize) -> String {
        let edge = format!(
            "{}{}{}",
            glyph::BOX_BOTTOM_LEFT,
            glyph::BOX_HORIZONTAL.repeat(inner + self.pad_cols * 2),
            glyph::BOX_BOTTOM_RIGHT
        );
        painter.paint(self.border, &edge)
    }

    fn row(&self, painter: &Painter, pad: &str, content: &str, inner: usize) -> String {
        let visible = visible_width(content);
        // Cutting a styled line by bytes would slice an escape in half, so an
        // over-wide row keeps its escapes and loses only the plain tail.
        let content = if visible > inner {
            let mut cut = paint::take_cells(&strip_escapes(content), inner);
            cut.push_str(glyph::ELLIPSIS_TIGHT);
            paint::take_cells(&cut, inner)
        } else {
            content.to_string()
        };
        let fill = " ".repeat(inner.saturating_sub(visible_width(&content)));
        let bar = painter.paint(self.border, glyph::BOX_VERTICAL);
        format!("{bar}{pad}{content}{fill}{pad}{bar}")
    }
}

/// A full-width rule with its title inlaid at the left: `── input ─────────`.
///
/// This heads the input area instead of a prompt character. A prompt sitting on
/// the same line as the text competes with it for the left edge and makes a
/// multi-line entry look ragged; a rule above the text gives every line the same
/// left margin, and the title has room to carry state — the mode, a queue depth.
///
/// `cell` selects solid or dashed, which is how plan mode is distinguished
/// without spending a colour that a monochrome terminal would drop.
pub fn rule(painter: &Painter, style: Style, cell: &str, title: &str, columns: usize) -> String {
    let head = format!("{cell}{cell}");
    if title.is_empty() {
        return painter.rule(style, cell, columns);
    }
    let label = format!(" {title} ");
    let used = paint::width(&head) + paint::width(&label);
    let tail = columns.saturating_sub(used);
    painter.paint(style, &format!("{head}{label}{}", cell.repeat(tail)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::theme::Palette;
    use anstyle::{AnsiColor, Color};

    const RED: Style = Style::new().fg_color(Some(Color::Ansi(AnsiColor::Red)));

    fn plain() -> Painter {
        Painter::plain()
    }

    fn styled() -> Painter {
        Painter::new(Palette::dark(), true)
    }

    /// The whole point of `visible_width`: a coloured line must measure the same
    /// as the plain one, or every border containing colour comes out ragged.
    #[test]
    fn escapes_do_not_count_towards_width() {
        let coloured = styled().paint(RED, "abc");
        assert!(coloured.len() > 3, "should actually contain escapes");
        assert_eq!(visible_width(&coloured), 3);
        assert_eq!(visible_width("abc"), 3);
        assert_eq!(visible_width("中文"), 4);
    }

    /// Every row of a box has to be the same width, including the borders — this
    /// is the property that a hand-built box most easily gets wrong.
    #[test]
    fn every_row_of_a_panel_is_the_same_width() {
        let body = vec!["short".to_string(), "a much longer line".to_string()];
        let lines = Panel::new(Style::new()).render(&plain(), &body, 80);
        let widths: Vec<usize> = lines.iter().map(|line| visible_width(line)).collect();
        assert!(
            widths.windows(2).all(|pair| pair[0] == pair[1]),
            "ragged panel: {widths:?}"
        );
        // 18 content + 2 padding + 2 borders.
        assert_eq!(widths[0], 22);
    }

    #[test]
    fn a_panel_is_bounded_by_the_ceiling_not_stretched_to_it() {
        let body = vec!["tiny".to_string()];
        let narrow = Panel::new(Style::new()).render(&plain(), &body, 200);
        assert_eq!(
            visible_width(&narrow[0]),
            8,
            "a small panel must not fill the terminal"
        );

        let wide = vec!["x".repeat(100)];
        let clamped = Panel::new(Style::new()).render(&plain(), &wide, 20);
        for line in &clamped {
            assert!(
                visible_width(line) <= 20,
                "{:?} overflows the ceiling",
                visible_width(line)
            );
        }
    }

    #[test]
    fn a_title_sits_in_the_top_border_and_keeps_the_box_square() {
        let body = vec!["body".to_string()];
        let lines = Panel::new(Style::new())
            .title("approval")
            .render(&plain(), &body, 80);
        assert!(lines[0].contains("approval"), "{:?}", lines[0]);
        assert!(lines[0].starts_with(glyph::BOX_TOP_LEFT));
        assert!(lines[0].ends_with(glyph::BOX_TOP_RIGHT));
        let widths: Vec<usize> = lines.iter().map(|line| visible_width(line)).collect();
        assert!(
            widths.windows(2).all(|pair| pair[0] == pair[1]),
            "a title must not skew the frame: {widths:?}"
        );
    }

    /// A title longer than the body decides the width, otherwise the top border
    /// would be wider than the rest of the box.
    #[test]
    fn a_long_title_widens_the_panel() {
        let body = vec!["hi".to_string()];
        let lines = Panel::new(Style::new())
            .title("a considerably longer title")
            .render(&plain(), &body, 120);
        let widths: Vec<usize> = lines.iter().map(|line| visible_width(line)).collect();
        assert!(
            widths.windows(2).all(|pair| pair[0] == pair[1]),
            "{widths:?}"
        );
    }

    #[test]
    fn padding_rows_add_blank_lines_inside_the_border() {
        let body = vec!["x".to_string()];
        let dense = Panel::new(Style::new()).render(&plain(), &body, 40);
        let airy = Panel::new(Style::new())
            .pad_rows(1)
            .render(&plain(), &body, 40);
        assert_eq!(dense.len() + 2, airy.len());
    }

    #[test]
    fn a_rule_fills_exactly_the_columns_it_is_given() {
        let solid = rule(&plain(), Style::new(), glyph::RULE_LIGHT, "input", 30);
        assert_eq!(paint::width(&solid), 30);
        assert!(solid.starts_with("── input ─"), "{solid:?}");

        let dashed = rule(
            &plain(),
            Style::new(),
            glyph::RULE_DASHED,
            "input · plan",
            30,
        );
        assert_eq!(paint::width(&dashed), 30);
        assert!(dashed.starts_with("╌╌ input · plan ╌"), "{dashed:?}");

        // No title is a plain full-width rule.
        assert_eq!(
            paint::width(&rule(&plain(), Style::new(), glyph::RULE_LIGHT, "", 12)),
            12
        );
    }

    /// A title wider than the terminal must not make the rule longer than the
    /// terminal — that would wrap and leave a stray line of dashes.
    #[test]
    fn a_rule_never_overflows_a_narrow_terminal() {
        let out = rule(
            &plain(),
            Style::new(),
            glyph::RULE_LIGHT,
            "an overlong title",
            8,
        );
        assert!(paint::width(&out) >= 8);
        assert!(out.contains("an overlong title"));
    }
}
