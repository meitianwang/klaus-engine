//! The slash-command menu: a list drawn under the input, not a popup.
//!
//! ## Why it is drawn rather than delegated
//!
//! The menu has to sit in a fixed place relative to the input area, size its two
//! columns against the terminal, and let the selected row show more description
//! than the rest. That is layout the shell already owns, so computing it here
//! keeps the whole input area under one measurement — a widget with its own idea
//! of where the cursor is would fight the toolbar for the same rows.
//!
//! Everything in this file is pure: candidates and a width in, finished lines
//! out. The shell does the drawing and the key handling.
//!
//! ## Filtering is prefix-then-substring, not fuzzy
//!
//! Fuzzy matching sounds friendlier and reads worse: for a set this small it
//! surfaces surprising entries and the ranking becomes hard to predict. A prefix
//! match is what a user typing `/mo` expects, and a substring match catches the
//! case where they remember a word from the middle of the name.

use super::paint::{self, Painter};
use super::theme::{glyph, limit};

/// One command the menu can offer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    /// Without the leading slash.
    pub name: String,
    pub description: String,
}

impl Candidate {
    pub fn new(name: &str, description: &str) -> Self {
        Self {
            name: name.to_string(),
            description: description.to_string(),
        }
    }
}

/// The menu's state: which candidates match, and which row is current.
#[derive(Debug, Clone, Default)]
pub struct Menu {
    matches: Vec<Candidate>,
    selected: usize,
    /// First visible row, so a long list scrolls instead of overflowing.
    offset: usize,
}

impl Menu {
    /// Build the menu for the text currently in the input.
    ///
    /// Returns `None` when the input is not a slash command, so the caller has
    /// one check for "should the menu be on screen at all". A line with a space
    /// in it has moved past the command name into arguments, and completing a
    /// name the user has finished typing is noise.
    pub fn for_input(input: &str, candidates: &[Candidate]) -> Option<Self> {
        let query = input.strip_prefix('/')?;
        if query.contains(char::is_whitespace) {
            return None;
        }
        let matches = filter(query, candidates);
        if matches.is_empty() {
            return None;
        }
        Some(Self {
            matches,
            selected: 0,
            offset: 0,
        })
    }

    pub fn matches(&self) -> &[Candidate] {
        &self.matches
    }

    pub fn selected(&self) -> &Candidate {
        &self.matches[self.selected]
    }

    pub fn selected_index(&self) -> usize {
        self.selected
    }

    /// How many rows this menu will occupy, so the shell can reserve them.
    pub fn rows(&self) -> usize {
        self.matches.len().min(limit::MENU_ROWS)
    }

    /// Move the selection down, wrapping. Wrapping rather than stopping: with a
    /// handful of entries, reaching the end and pressing down again to get back
    /// to the top is faster than reversing direction.
    pub fn next(&mut self) {
        self.selected = (self.selected + 1) % self.matches.len();
        self.scroll_to_selection();
    }

    pub fn previous(&mut self) {
        self.selected = if self.selected == 0 {
            self.matches.len() - 1
        } else {
            self.selected - 1
        };
        self.scroll_to_selection();
    }

    /// Keep the selected row inside the visible window.
    fn scroll_to_selection(&mut self) {
        let window = limit::MENU_ROWS;
        if self.selected < self.offset {
            self.offset = self.selected;
        } else if self.selected >= self.offset + window {
            self.offset = self.selected + 1 - window;
        }
    }

    /// What the input should become when this row is accepted.
    ///
    /// A trailing space is included: every command that takes an argument needs
    /// it, and for the ones that do not it costs a keystroke to remove and is
    /// trimmed on submit anyway.
    pub fn accept(&self) -> String {
        format!("/{} ", self.selected().name)
    }

    /// Draw the menu, sized to `columns`.
    pub fn render(&self, painter: &Painter, columns: usize) -> Vec<String> {
        let palette = painter.palette();
        let marker_cells = paint::width(glyph::MENU_CURRENT) + 1;
        let command = command_column(&self.matches, columns, marker_cells);
        // What is left after the marker, the command column and the one space of
        // separation. When there is no room for a description the separator goes
        // too — emitting it regardless overran the width by exactly one column on
        // a narrow terminal.
        let meta_cells = columns
            .saturating_sub(marker_cells)
            .saturating_sub(command)
            .saturating_sub(1);
        let separator = if meta_cells > 0 { " " } else { "" };

        let mut out: Vec<String> = Vec::new();
        for (index, candidate) in self
            .matches
            .iter()
            .enumerate()
            .skip(self.offset)
            .take(limit::MENU_ROWS)
        {
            let current = index == self.selected;
            let marker = if current {
                format!("{} ", glyph::MENU_CURRENT)
            } else {
                " ".repeat(marker_cells)
            };
            let name = paint::pad_right(
                &paint::truncate_right(&format!("/{}", candidate.name), command),
                command,
            );
            let meta = paint::truncate_right(&candidate.description, meta_cells);
            out.push(painter.spans(&[
                (
                    if current {
                        palette.menu_marker_current
                    } else {
                        palette.menu_marker
                    },
                    marker.as_str(),
                ),
                (
                    if current {
                        palette.menu_command_current
                    } else {
                        palette.menu_command
                    },
                    name.as_str(),
                ),
                (palette.body, separator),
                (
                    if current {
                        palette.menu_meta_current
                    } else {
                        palette.menu_meta
                    },
                    meta.as_str(),
                ),
            ]));
        }
        // A list longer than the window says so, or the entries below it are
        // invisible and the user believes they do not exist.
        let hidden = self
            .matches
            .len()
            .saturating_sub(self.offset + limit::MENU_ROWS);
        if hidden > 0 {
            out.push(painter.paint(
                palette.faint,
                &format!(
                    "{}{} {hidden} more",
                    " ".repeat(marker_cells),
                    glyph::ELLIPSIS
                ),
            ));
        }
        out
    }
}

/// Prefix matches first, then substring matches, each group alphabetical.
///
/// The two groups are kept apart rather than scored together so that typing an
/// exact prefix always puts that command at the top — a score that mixes the two
/// can rank a substring hit above the name the user is literally typing.
fn filter(query: &str, candidates: &[Candidate]) -> Vec<Candidate> {
    let query = query.to_ascii_lowercase();
    let mut prefix: Vec<Candidate> = Vec::new();
    let mut contains: Vec<Candidate> = Vec::new();
    for candidate in candidates {
        let name = candidate.name.to_ascii_lowercase();
        if name.starts_with(&query) {
            prefix.push(candidate.clone());
        } else if !query.is_empty() && name.contains(&query) {
            contains.push(candidate.clone());
        }
    }
    prefix.sort_by(|a, b| a.name.cmp(&b.name));
    contains.sort_by(|a, b| a.name.cmp(&b.name));
    prefix.extend(contains);
    prefix
}

/// How wide the command column should be: enough for the longest name, but
/// bounded so a single long command name cannot squeeze the descriptions out.
fn command_column(matches: &[Candidate], columns: usize, marker_cells: usize) -> usize {
    let longest = matches
        .iter()
        .map(|candidate| paint::width(&candidate.name) + 1)
        .max()
        .unwrap_or(0);
    let usable = columns.saturating_sub(marker_cells);
    // MIN and MAX are constants with MIN < MAX, so clamp cannot panic here; the
    // usable width is applied after, since on a very narrow terminal it can be
    // below the minimum and the column must not exceed what there is.
    longest
        .clamp(limit::MENU_COMMAND_MIN, limit::MENU_COMMAND_MAX)
        .min(usable)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::theme::Palette;

    fn plain() -> Painter {
        Painter::plain()
    }

    fn commands() -> Vec<Candidate> {
        vec![
            Candidate::new("help", "list the commands"),
            Candidate::new("model", "switch model from the next turn"),
            Candidate::new("mode", "switch permission mode"),
            Candidate::new("exit", "leave the session"),
            Candidate::new("compact", "compact the conversation"),
        ]
    }

    #[test]
    fn the_menu_only_exists_for_a_slash_command() {
        assert!(Menu::for_input("/", &commands()).is_some());
        assert!(Menu::for_input("/mo", &commands()).is_some());
        // Not a command line at all.
        assert!(Menu::for_input("hello", &commands()).is_none());
        assert!(Menu::for_input("", &commands()).is_none());
        // Past the name into arguments: the name is settled, so stop completing.
        assert!(Menu::for_input("/model sonnet", &commands()).is_none());
        // Nothing matches.
        assert!(Menu::for_input("/zzz", &commands()).is_none());
    }

    #[test]
    fn a_bare_slash_offers_everything_alphabetically() {
        let menu = Menu::for_input("/", &commands()).unwrap();
        let names: Vec<&str> = menu.matches().iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["compact", "exit", "help", "mode", "model"]);
    }

    /// An exact prefix must outrank a substring hit, or typing `/mode` can put
    /// something else on top. Note `model` is itself a prefix match for `mode`,
    /// so it ranks with the prefix group rather than the substring one.
    #[test]
    fn prefix_matches_come_before_substring_matches() {
        let mut candidates = commands();
        candidates.push(Candidate::new("set-mode-thing", "contains mode"));
        let menu = Menu::for_input("/mode", &candidates).unwrap();
        let names: Vec<&str> = menu.matches().iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["mode", "model", "set-mode-thing"]);
    }

    #[test]
    fn filtering_ignores_case() {
        let menu = Menu::for_input("/MO", &commands()).unwrap();
        let names: Vec<&str> = menu.matches().iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["mode", "model"]);
    }

    #[test]
    fn selection_wraps_in_both_directions() {
        let mut menu = Menu::for_input("/mo", &commands()).unwrap();
        assert_eq!(menu.selected().name, "mode");
        menu.next();
        assert_eq!(menu.selected().name, "model");
        menu.next();
        assert_eq!(menu.selected().name, "mode", "wraps to the top");
        menu.previous();
        assert_eq!(menu.selected().name, "model", "wraps to the bottom");
    }

    #[test]
    fn accepting_a_row_yields_the_command_plus_a_space() {
        let menu = Menu::for_input("/mod", &commands()).unwrap();
        assert_eq!(menu.accept(), "/mode ");
    }

    #[test]
    fn only_the_current_row_carries_the_marker() {
        let mut menu = Menu::for_input("/mo", &commands()).unwrap();
        let rows = menu.render(&plain(), 60);
        assert!(rows[0].starts_with(glyph::MENU_CURRENT), "{:?}", rows[0]);
        assert!(rows[1].starts_with("  "), "{:?}", rows[1]);

        menu.next();
        let rows = menu.render(&plain(), 60);
        assert!(rows[1].starts_with(glyph::MENU_CURRENT), "{:?}", rows[1]);
        assert!(rows[0].starts_with("  "), "{:?}", rows[0]);
    }

    /// Every row is the same width and none overflows, at any terminal size.
    #[test]
    fn rows_never_exceed_the_width() {
        let menu = Menu::for_input("/", &commands()).unwrap();
        for columns in [10, 20, 30, 40, 80, 200] {
            for row in menu.render(&plain(), columns) {
                assert!(
                    paint::width(&row) <= columns,
                    "at {columns} a row was {}: {row:?}",
                    paint::width(&row)
                );
            }
        }
    }

    #[test]
    fn the_command_column_is_bounded_at_both_ends() {
        // Where a needle starts, measured in CELLS: the marker glyph is three
        // bytes, so comparing byte offsets would report two aligned rows as
        // misaligned.
        fn column(row: &str, needle: &str) -> usize {
            paint::width(&row[..row.find(needle).expect("needle present")])
        }

        // A short set still gets the minimum, so descriptions line up.
        let short = vec![Candidate::new("a", "one"), Candidate::new("b", "two")];
        let menu = Menu::for_input("/", &short).unwrap();
        let rows = menu.render(&plain(), 80);
        assert_eq!(
            column(&rows[0], "one"),
            column(&rows[1], "two"),
            "descriptions must align"
        );

        // A very long name cannot squeeze the descriptions out.
        let long = vec![Candidate::new(&"x".repeat(60), "still visible")];
        let menu = Menu::for_input("/", &long).unwrap();
        let row = menu.render(&plain(), 80).remove(0);
        assert!(row.contains("still visible"), "{row:?}");
    }

    /// A list longer than the window has to say so, or the rest is invisible.
    #[test]
    fn a_long_list_scrolls_and_reports_what_is_hidden() {
        let many: Vec<Candidate> = (0..15)
            .map(|index| Candidate::new(&format!("cmd{index:02}"), "a command"))
            .collect();
        let mut menu = Menu::for_input("/", &many).unwrap();
        assert_eq!(menu.rows(), limit::MENU_ROWS);

        let rows = menu.render(&plain(), 60);
        assert_eq!(rows.len(), limit::MENU_ROWS + 1, "ten rows plus the note");
        assert!(rows.last().unwrap().contains("5 more"), "{:?}", rows.last());

        // Walking past the window scrolls it; the note shrinks as it goes.
        for _ in 0..12 {
            menu.next();
        }
        let rows = menu.render(&plain(), 60);
        assert!(
            rows.iter().any(|r| r.contains(glyph::MENU_CURRENT)),
            "selection stays visible"
        );
        for row in &rows {
            assert!(paint::width(row) <= 60);
        }
    }

    #[test]
    fn a_short_list_has_no_scroll_note() {
        let menu = Menu::for_input("/mo", &commands()).unwrap();
        let rows = menu.render(&plain(), 60);
        assert_eq!(rows.len(), 2);
        assert!(!rows.iter().any(|r| r.contains("more")));
    }

    #[test]
    fn styled_output_paints_the_current_row_differently() {
        let painter = Painter::new(Palette::dark(), true);
        let menu = Menu::for_input("/mo", &commands()).unwrap();
        let rows = menu.render(&painter, 60);
        assert!(rows[0].contains('\u{1b}'));
        assert_ne!(rows[0], rows[1], "the current row must look different");
    }
}
