//! Whether colour is live, and the text-measuring the layout depends on.
//!
//! Two concerns, one place, because they answer the same question — "what will
//! this actually look like on the terminal in front of the user".
//!
//! ## Colour is decided once
//!
//! [`Painter`] is built at startup and carries a single flag. Every renderer
//! asks it to paint, and when the flag is off it hands back the text unchanged.
//! That is what lets one set of renderers serve both a terminal and a pipe: no
//! renderer branches on "am I coloured", so none of them can forget to.
//!
//! Escapes must never reach a pipe. `klaus exec ... | grep`, `--json` consumers,
//! and CI logs all read this CLI's stdout, and an escape in there is corruption,
//! not decoration. So the default is off and it takes a terminal to turn it on.
//!
//! ## Width is not length
//!
//! Every layout here — the toolbar's columns, the diff's gutter, the menu's two
//! columns, the rules that fill a line — positions text by how many CELLS it
//! occupies. `str::len` counts bytes and `chars().count()` counts code points;
//! CJK text and emoji make both of those wrong, and a rule computed from the
//! wrong count wraps and destroys the layout. So all measuring goes through
//! [`width`], and all cutting through the helpers below, which cut on grapheme
//! boundaries and account for wide characters.

use std::io::IsTerminal;

use anstyle::Style;
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use super::theme::{glyph, Mode, Palette};

/// How wide the text is on screen, in terminal cells.
pub fn width(text: &str) -> usize {
    text.width()
}

/// The palette plus the one decision about whether to use it.
#[derive(Debug, Clone, Copy)]
pub struct Painter {
    palette: Palette,
    enabled: bool,
}

impl Painter {
    /// A painter that never emits escapes. The right choice for a pipe, for
    /// `--json`, and for tests, which must assert on text rather than on
    /// escape sequences.
    pub const fn plain() -> Self {
        Self {
            palette: Palette::dark(),
            enabled: false,
        }
    }

    pub const fn new(palette: Palette, enabled: bool) -> Self {
        Self { palette, enabled }
    }

    /// Resolve from the environment, the way every terminal program is expected
    /// to: `NO_COLOR` wins over everything, `CLICOLOR_FORCE` overrides the
    /// terminal check, and otherwise stdout has to be a terminal.
    ///
    /// `KLAUS_THEME=dark|light` picks the palette. There is no reliable way to
    /// ask a terminal for its background colour, so the light palette must be
    /// asked for; guessing from `COLORFGBG` misfires on the terminals that set
    /// it inconsistently and leaves the user with unreadable grey.
    pub fn from_env() -> Self {
        let mode = std::env::var("KLAUS_THEME")
            .ok()
            .and_then(|value| Mode::parse(&value))
            .unwrap_or(Mode::Dark);
        Self::new(Palette::for_mode(mode), color_is_live())
    }

    pub const fn palette(&self) -> &Palette {
        &self.palette
    }

    pub const fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// The same painter with colour forced off — for the one caller that has a
    /// palette but is writing somewhere escapes must not go.
    pub const fn without_color(&self) -> Self {
        Self {
            palette: self.palette,
            enabled: false,
        }
    }

    /// `text` in `style`, or `text` alone when colour is off.
    ///
    /// The reset is emitted even for an empty style so that a caller composing
    /// spans cannot leak one span's attributes into the next.
    pub fn paint(&self, style: Style, text: &str) -> String {
        if !self.enabled || text.is_empty() {
            return text.to_string();
        }
        format!("{}{}{}", style.render(), text, style.render_reset())
    }

    /// Join spans into one line. Each span is painted independently, so a span's
    /// attributes end where the span does.
    pub fn spans(&self, spans: &[(Style, &str)]) -> String {
        let mut out = String::new();
        for (style, text) in spans {
            out.push_str(&self.paint(*style, text));
        }
        out
    }

    /// A rule of `cell` repeated to exactly `cells` columns.
    ///
    /// `cells` is a cell count, so a multi-cell rule character has to be
    /// repeated fewer times. Every rule in the shell is single-cell today, but
    /// computing it rather than assuming it is what keeps a future glyph change
    /// from silently over-running the line.
    pub fn rule(&self, style: Style, cell: &str, cells: usize) -> String {
        let per = width(cell).max(1);
        self.paint(style, &cell.repeat(cells / per))
    }
}

/// Whether colour should be emitted on stdout, by the usual conventions.
fn color_is_live() -> bool {
    if std::env::var_os("NO_COLOR").is_some_and(|value| !value.is_empty()) {
        return false;
    }
    if std::env::var_os("CLICOLOR_FORCE").is_some_and(|value| value != "0") {
        return true;
    }
    if std::env::var("TERM").is_ok_and(|term| term == "dumb") {
        return false;
    }
    std::io::stdout().is_terminal()
}

/// Cut `text` to `cells` columns, keeping the START and marking the cut.
///
/// For text whose beginning identifies it: a status line, a commit subject.
pub fn truncate_right(text: &str, cells: usize) -> String {
    if width(text) <= cells {
        return text.to_string();
    }
    let mark = glyph::ELLIPSIS_TIGHT;
    let mark_width = width(mark);
    if cells <= mark_width {
        return take_cells(text, cells);
    }
    let mut out = take_cells(text, cells - mark_width);
    out.push_str(mark);
    out
}

/// Cut `text` to `cells` columns, keeping the END and marking the cut.
///
/// For text whose end identifies it — a path, above all. `…/crates/cli/src` says
/// where you are; `/Users/someone/very/long…` does not.
pub fn truncate_left(text: &str, cells: usize) -> String {
    if width(text) <= cells {
        return text.to_string();
    }
    let mark = glyph::ELLIPSIS_TIGHT;
    let mark_width = width(mark);
    if cells <= mark_width {
        return take_cells_from_end(text, cells);
    }
    let tail = take_cells_from_end(text, cells - mark_width);
    format!("{mark}{tail}")
}

/// Cut `text` to about `cells` columns by removing its MIDDLE.
///
/// Tool arguments are the reason this exists: `src/…/handler.rs` keeps both the
/// project-relative head and the filename, and a shell command keeps both the
/// program and its last argument. Newlines fold to spaces first, because a
/// single-line field must stay single-line.
pub fn shorten_middle(text: &str, cells: usize) -> String {
    let folded = fold_newlines(text);
    if width(&folded) <= cells {
        return folded;
    }
    let mark = glyph::ELLIPSIS;
    let mark_width = width(mark);
    if cells <= mark_width {
        return take_cells(&folded, cells);
    }
    let budget = cells - mark_width;
    let head_cells = budget / 2;
    let tail_cells = budget - head_cells;
    let head = take_cells(&folded, head_cells);
    let tail = take_cells_from_end(&folded, tail_cells);
    format!("{head}{mark}{tail}")
}

/// Collapse every run of whitespace-with-a-newline into a single space, so a
/// multi-line value can sit in a one-line field.
pub fn fold_newlines(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut pending_break = false;
    for ch in text.chars() {
        match ch {
            '\n' | '\r' => pending_break = true,
            _ => {
                if pending_break {
                    if !out.is_empty() {
                        out.push(' ');
                    }
                    pending_break = false;
                }
                out.push(ch);
            }
        }
    }
    out
}

/// The longest prefix of `text` that fits in `cells` columns.
///
/// A character that would straddle the limit is dropped rather than half-drawn:
/// a wide character printed into one remaining column corrupts the rest of the
/// line on most terminals.
pub fn take_cells(text: &str, cells: usize) -> String {
    let mut out = String::new();
    let mut used = 0;
    for ch in text.chars() {
        let ch_width = ch.width().unwrap_or(0);
        if used + ch_width > cells {
            break;
        }
        used += ch_width;
        out.push(ch);
    }
    out
}

/// The longest suffix of `text` that fits in `cells` columns.
pub fn take_cells_from_end(text: &str, cells: usize) -> String {
    let mut kept: Vec<char> = Vec::new();
    let mut used = 0;
    for ch in text.chars().rev() {
        let ch_width = ch.width().unwrap_or(0);
        if used + ch_width > cells {
            break;
        }
        used += ch_width;
        kept.push(ch);
    }
    kept.reverse();
    kept.into_iter().collect()
}

/// Pad `text` on the right to `cells` columns. Short of the width it is left
/// alone — padding is for building columns, not for enforcing them.
pub fn pad_right(text: &str, cells: usize) -> String {
    let have = width(text);
    if have >= cells {
        return text.to_string();
    }
    format!("{text}{}", " ".repeat(cells - have))
}

/// Pad `text` on the left to `cells` columns, for a right-aligned column such as
/// a diff's line numbers.
pub fn pad_left(text: &str, cells: usize) -> String {
    let have = width(text);
    if have >= cells {
        return text.to_string();
    }
    format!("{}{text}", " ".repeat(cells - have))
}

/// Replace `$HOME` with `~` so a path reads as short as it feels.
pub fn shorten_home(path: &str) -> String {
    let Some(home) = std::env::var_os("HOME") else {
        return path.to_string();
    };
    let home = home.to_string_lossy();
    if home.is_empty() {
        return path.to_string();
    }
    let home = home.trim_end_matches('/');
    if path == home {
        return "~".to_string();
    }
    match path.strip_prefix(&format!("{home}/")) {
        Some(rest) => format!("~/{rest}"),
        None => path.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anstyle::{AnsiColor, Color};

    fn styled() -> Painter {
        Painter::new(Palette::dark(), true)
    }

    const RED: Style = Style::new().fg_color(Some(Color::Ansi(AnsiColor::Red)));

    /// The plain painter is the pipe contract: not one escape byte, ever.
    #[test]
    fn a_plain_painter_emits_no_escapes() {
        let painter = Painter::plain();
        let out = painter.spans(&[(RED, "error"), (Style::new(), ": nope")]);
        assert_eq!(out, "error: nope");
        assert!(!out.contains('\u{1b}'));
        assert!(!painter.rule(RED, "─", 4).contains('\u{1b}'));
    }

    #[test]
    fn a_styled_painter_wraps_each_span_and_resets_it() {
        let out = styled().paint(RED, "boom");
        assert!(
            out.starts_with('\u{1b}'),
            "{out:?} should open with an escape"
        );
        assert!(out.contains("boom"));
        assert!(
            out.ends_with("\u{1b}[0m"),
            "{out:?} should reset at the end"
        );
    }

    /// Painting nothing must stay nothing: an escape pair around an empty string
    /// would put a stray reset into the middle of a composed line.
    #[test]
    fn empty_text_is_never_wrapped() {
        assert_eq!(styled().paint(RED, ""), "");
    }

    #[test]
    fn a_rule_is_measured_in_cells_not_characters() {
        let painter = Painter::plain();
        assert_eq!(painter.rule(Style::new(), "─", 5), "─────");
        // A two-cell rule character fits half as many times.
        assert_eq!(width(&painter.rule(Style::new(), "＝", 6)), 6);
    }

    #[test]
    fn truncation_keeps_the_end_of_a_path_and_the_start_of_a_status() {
        // The budget is the WHOLE field including the marker, so 6 cells buys the
        // marker plus the last 5.
        assert_eq!(truncate_left("/a/b/c/d/e", 6), "…c/d/e");
        assert_eq!(width(&truncate_left("/a/b/c/d/e", 6)), 6);
        assert_eq!(truncate_right("connecting to server", 8), "connect…");
        assert_eq!(width(&truncate_right("connecting to server", 8)), 8);
        // Text that fits is returned untouched, with no marker.
        assert_eq!(truncate_left("short", 20), "short");
        assert_eq!(truncate_right("short", 20), "short");
    }

    #[test]
    fn shorten_middle_keeps_both_ends_and_folds_newlines() {
        let path = "src/very/deep/path/to/a/handler.rs";
        let out = shorten_middle(path, 20);
        // Half the budget each side of the marker: 20 - 3 = 17, so 8 head and 9
        // tail. Both ends survive; only the middle is spent.
        assert_eq!(out, "src/very...andler.rs");
        assert!(path.starts_with("src/very"), "the head is a real prefix");
        assert!(path.ends_with("andler.rs"), "the tail is a real suffix");
        assert!(width(&out) <= 20, "{out:?} is {} cells", width(&out));

        assert_eq!(shorten_middle("one\ntwo", 40), "one two");
        assert_eq!(shorten_middle("fits", 40), "fits");
    }

    /// Wide characters must not be cut in half — one straddling cell corrupts
    /// every following column on the line.
    #[test]
    fn wide_characters_are_dropped_rather_than_split() {
        // Each ideograph is two cells, so an odd budget leaves one cell unused.
        assert_eq!(take_cells("中文字", 5), "中文");
        assert_eq!(width(&take_cells("中文字", 5)), 4);
        assert_eq!(take_cells_from_end("中文字", 3), "字");
        assert!(width(&shorten_middle("中文字符串很长很长很长", 10)) <= 10);
    }

    #[test]
    fn padding_builds_columns_and_never_shrinks() {
        assert_eq!(pad_right("ab", 5), "ab   ");
        assert_eq!(pad_left("7", 3), "  7");
        assert_eq!(pad_right("toolong", 3), "toolong");
        // Padding is by cells, so a wide character consumes two of them.
        assert_eq!(pad_right("中", 4), "中  ");
    }

    #[test]
    fn home_collapses_to_a_tilde() {
        // Uses the real HOME so the helper is exercised the way it runs.
        let home = std::env::var("HOME").unwrap_or_default();
        if home.is_empty() {
            return;
        }
        assert_eq!(shorten_home(&home), "~");
        assert_eq!(shorten_home(&format!("{home}/work/klaus")), "~/work/klaus");
        assert_eq!(shorten_home("/opt/klaus"), "/opt/klaus");
        // A sibling directory that merely starts with the same bytes is not home.
        assert_eq!(shorten_home(&format!("{home}x/y")), format!("{home}x/y"));
    }
}
