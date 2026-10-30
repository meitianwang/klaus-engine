//! The interactive input area: the only place that touches raw mode.
//!
//! ## Raw mode is held for the prompt, not for the session
//!
//! The shell enters raw mode to read one prompt, erases what it drew, and leaves
//! before the turn runs. The transcript then prints through the ordinary writer,
//! in cooked mode, exactly as it does for `klaus exec`.
//!
//! The alternative — owning the terminal for the whole session — is what makes a
//! TUI corrupt its own output: two writers (the transcript and the redrawn input
//! area) both compute cursor positions against a screen the other is changing,
//! and any disagreement leaves half-drawn rows behind. Holding raw mode only
//! while nothing else writes removes that class of bug outright.
//!
//! The cost is stated plainly: the toolbar is on screen while you type, not while
//! a turn runs. A permanently visible toolbar needs a single writer that owns
//! every row, which is a larger change than this one.
//!
//! ## What is testable, and what is not
//!
//! [`key_from_event`] and [`Layout`] are pure and unit-tested: the key mapping is
//! where a wrong modifier silently breaks an editing command, and the layout is
//! where a miscounted row leaves debris on screen. The terminal calls themselves
//! are a thin shell over those two, because anything hidden behind a terminal
//! handle cannot be checked in a headless gate.

use std::io::{IsTerminal, Write};
use std::time::{Duration, Instant};

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use crossterm::terminal;

use super::editor::{Editor, Key, Outcome};
use super::menu::{Candidate, Menu};
use super::paint::Painter;
use super::panel;
use super::theme::{glyph, tick};
use super::toolbar::{self, Status};
use crate::error::CliResult;

/// What a prompt read ended with. Mirrors the line-based `Typed` so the REPL can
/// treat both input paths the same way.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Submitted {
    Line(String),
    /// Ctrl-C at the prompt.
    Interrupted,
    /// Ctrl-D on an empty buffer.
    End,
}

/// Whether an interactive shell can run at all.
///
/// Both directions are required: without a readable terminal there are no key
/// events, and without a writable one there is nowhere to draw. A pipe on either
/// side means the REPL must fall back to reading lines.
pub fn is_available() -> bool {
    std::io::stdin().is_terminal() && std::io::stdout().is_terminal()
}

/// Map a terminal key event onto an editor key.
///
/// `None` is a key the editor has no meaning for. Returning it rather than
/// swallowing the event is what lets the caller claim Tab and Esc for the menu.
///
/// Modifiers are checked before the plain character, because a terminal reports
/// `ctrl-a` as `Char('a')` with the CONTROL modifier set — matching on the
/// character first would insert an `a` for every control chord.
pub fn key_from_event(event: KeyEvent) -> Option<Key> {
    // Windows reports both press and release; acting on both doubles every
    // keystroke. Unix only ever sends Press, so filtering here is free there.
    if event.kind == KeyEventKind::Release {
        return None;
    }
    let ctrl = event.modifiers.contains(KeyModifiers::CONTROL);
    let alt = event.modifiers.contains(KeyModifiers::ALT);

    match event.code {
        KeyCode::Char(ch) if ctrl => match ch.to_ascii_lowercase() {
            'a' => Some(Key::CtrlA),
            'c' => Some(Key::CtrlC),
            'd' => Some(Key::CtrlD),
            'e' => Some(Key::CtrlE),
            'k' => Some(Key::CtrlK),
            'u' => Some(Key::CtrlU),
            'w' => Some(Key::CtrlW),
            // ctrl-j is the newline chord: Enter submits, so multi-line entry
            // needs a separate key and this is the one every shell uses.
            'j' => Some(Key::Newline),
            _ => None,
        },
        KeyCode::Char(ch) if alt => match ch.to_ascii_lowercase() {
            'b' => Some(Key::WordLeft),
            'f' => Some(Key::WordRight),
            _ => None,
        },
        KeyCode::Char(ch) => Some(Key::Char(ch)),
        KeyCode::Backspace => Some(Key::Backspace),
        KeyCode::Delete => Some(Key::Delete),
        KeyCode::Left => Some(Key::Left),
        KeyCode::Right => Some(Key::Right),
        KeyCode::Up => Some(Key::Up),
        KeyCode::Down => Some(Key::Down),
        KeyCode::Home => Some(Key::Home),
        KeyCode::End => Some(Key::End),
        // alt-enter is the other conventional newline chord.
        KeyCode::Enter if alt => Some(Key::Newline),
        KeyCode::Enter => Some(Key::Enter),
        KeyCode::Tab => Some(Key::Tab),
        KeyCode::Esc => Some(Key::Esc),
        _ => None,
    }
}

/// The input area as finished lines, plus where the cursor belongs in it.
///
/// Computed as a value so the row count is knowable before anything is written:
/// erasing the area needs exactly the number of rows that were drawn, and a
/// count derived after the fact is the usual reason a repaint leaves debris.
#[derive(Debug, Clone)]
pub struct Layout {
    pub lines: Vec<String>,
    /// Row of the cursor within `lines`.
    pub cursor_row: usize,
    /// Column of the cursor, in terminal cells.
    pub cursor_col: usize,
}

impl Layout {
    pub fn rows(&self) -> usize {
        self.lines.len()
    }
}

/// Build the input area: a titled rule, the buffer, an optional menu, then the
/// toolbar.
pub fn layout(
    painter: &Painter,
    editor: &Editor,
    menu: Option<&Menu>,
    status: &Status,
    columns: usize,
) -> Layout {
    let palette = painter.palette();
    let mut lines: Vec<String> = Vec::new();

    // Plan mode gets a dashed rule as well as a colour, so the distinction
    // survives a terminal that drops colour entirely.
    let (style, cell) = if status.badges.plan {
        (palette.plan_rule, glyph::RULE_DASHED)
    } else {
        (palette.separator, glyph::RULE_LIGHT)
    };
    let mut title = String::from("input");
    if status.badges.plan {
        title.push_str(" · plan");
    }
    lines.push(panel::rule(painter, style, cell, &title, columns));

    // The buffer sits at the left margin with no prompt character: a marker on
    // the first line only would leave a multi-line entry ragged, and the rule
    // above already says where input begins.
    let buffer = editor.lines();
    let (cursor_row, cursor_col) = editor.cursor_position();
    for line in &buffer {
        lines.push(painter.paint(palette.body, line));
    }
    // Row 0 is the rule, so the buffer starts at row 1.
    let cursor_row = cursor_row + 1;
    let cursor_col = super::paint::width(
        &buffer
            .get(cursor_row - 1)
            .map(|line| line.chars().take(cursor_col).collect::<String>())
            .unwrap_or_default(),
    );

    if let Some(menu) = menu {
        lines.extend(menu.render(painter, columns));
    }
    lines.extend(toolbar::render(painter, status, columns));

    Layout {
        lines,
        cursor_row,
        cursor_col,
    }
}

/// The interactive prompt.
pub struct Shell {
    editor: Editor,
    commands: Vec<Candidate>,
    /// Rows the last paint occupied, so the next one erases exactly them.
    painted: usize,
    started: Instant,
}

impl Shell {
    pub fn new(commands: Vec<Candidate>) -> Self {
        Self {
            editor: Editor::new(),
            commands,
            painted: 0,
            started: Instant::now(),
        }
    }

    /// Read one prompt. Raw mode is entered here and left before returning,
    /// whichever way the read ends.
    pub fn prompt(
        &mut self,
        painter: &Painter,
        status: &Status,
        columns: usize,
    ) -> CliResult<Submitted> {
        // `block_in_place` rather than a bare blocking loop: the engine's event
        // pump is a live task, and starving it while the user types would stall
        // anything the engine pushes at the prompt.
        tokio::task::block_in_place(|| self.prompt_blocking(painter, status, columns))
    }

    fn prompt_blocking(
        &mut self,
        painter: &Painter,
        status: &Status,
        columns: usize,
    ) -> CliResult<Submitted> {
        terminal::enable_raw_mode()?;
        let result = self.read_loop(painter, status, columns);
        // Both of these run however the read ended. Leaving raw mode on with no
        // echo looks exactly like a hang, and leaving the drawn area behind puts
        // a stale toolbar above the transcript — so neither is allowed to be
        // skipped by an early return, and neither is allowed to mask the real
        // error either.
        let _ = terminal::disable_raw_mode();
        let _ = self.erase();
        result
    }

    fn read_loop(
        &mut self,
        painter: &Painter,
        status: &Status,
        columns: usize,
    ) -> CliResult<Submitted> {
        loop {
            let menu = Menu::for_input(&self.editor.text(), &self.commands);
            let mut status = status.clone();
            status.tip = Some(toolbar::tip_for(self.started.elapsed().as_millis()).to_string());
            self.paint(&layout(
                painter,
                &self.editor,
                menu.as_ref(),
                &status,
                columns,
            ))?;

            // Poll rather than block, so the rotating hint and any clock-driven
            // field keep moving while the user thinks.
            if !event::poll(Duration::from_millis(tick::SHELL_RUNNING_MS))? {
                continue;
            }
            let Event::Key(key_event) = event::read()? else {
                continue;
            };
            let Some(key) = key_from_event(key_event) else {
                continue;
            };

            // The menu claims the keys that only mean something while it is open,
            // and only those: everything else still edits the line underneath.
            if let Some(mut menu) = menu {
                match key {
                    Key::Tab | Key::Enter => {
                        self.editor.set_text(&menu.accept());
                        continue;
                    }
                    Key::Up => {
                        menu.previous();
                        continue;
                    }
                    Key::Down => {
                        menu.next();
                        continue;
                    }
                    Key::Esc => {
                        // Dismiss by removing the slash, which is the only state
                        // the menu is keyed on.
                        let text = self.editor.text();
                        self.editor.set_text(text.trim_start_matches('/'));
                        continue;
                    }
                    _ => {}
                }
            }

            match self.editor.apply(key) {
                Outcome::Submit(text) => {
                    self.editor.remember(&text);
                    return Ok(Submitted::Line(text));
                }
                Outcome::Interrupt => return Ok(Submitted::Interrupted),
                Outcome::Eof => return Ok(Submitted::End),
                Outcome::Redraw | Outcome::Ignored => {}
            }
        }
    }

    /// Draw the area, replacing whatever the previous paint left.
    fn paint(&mut self, layout: &Layout) -> CliResult<()> {
        let mut out = std::io::stdout().lock();
        paint_frame(&mut out, self.painted, layout)?;
        out.flush()?;
        self.painted = layout.rows();
        Ok(())
    }

    /// Remove the drawn area, leaving the cursor on a fresh line.
    fn erase(&mut self) -> CliResult<()> {
        if self.painted == 0 {
            return Ok(());
        }
        let mut out = std::io::stdout().lock();
        erase_frame(&mut out, self.painted)?;
        out.flush()?;
        self.painted = 0;
        Ok(())
    }
}

/// Write the escape sequence that repaints the input area, given how many rows
/// the previous paint left on screen.
///
/// Split out from the terminal so the byte stream can be checked headless: a
/// wrong cursor-move count here is what leaves debris on a repaint, and the
/// sandbox has no pty to catch it at runtime. `writer` is anything byte-shaped;
/// the real caller hands it stdout.
fn paint_frame<W: Write>(writer: &mut W, painted: usize, layout: &Layout) -> std::io::Result<()> {
    // Move to the top of the previous paint and clear from there down, so a
    // shrinking area (a menu closing) does not leave its old rows behind.
    if painted > 0 {
        write!(writer, "\r\x1b[{painted}A\x1b[J")?;
    } else {
        write!(writer, "\r\x1b[J")?;
    }
    for (index, line) in layout.lines.iter().enumerate() {
        if index > 0 {
            write!(writer, "\r\n")?;
        }
        write!(writer, "\r{line}")?;
    }
    // Park the cursor where the buffer says it is: up from the last row to the
    // cursor's row, then right to its column.
    let below = layout.rows().saturating_sub(1 + layout.cursor_row);
    if below > 0 {
        write!(writer, "\x1b[{below}A")?;
    }
    write!(writer, "\r")?;
    if layout.cursor_col > 0 {
        write!(writer, "\x1b[{}C", layout.cursor_col)?;
    }
    Ok(())
}

/// Write the escape sequence that removes `painted` rows, leaving the cursor at
/// the top of where they were.
fn erase_frame<W: Write>(writer: &mut W, painted: usize) -> std::io::Result<()> {
    write!(writer, "\r\x1b[{painted}A\x1b[J")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::theme::Palette;

    fn press(code: KeyCode, modifiers: KeyModifiers) -> KeyEvent {
        KeyEvent::new(code, modifiers)
    }

    fn plain() -> Painter {
        Painter::plain()
    }

    /// A control chord must not also insert its letter. A terminal reports
    /// `ctrl-a` as `Char('a')` plus a modifier, so matching the character first
    /// would type an `a` on every chord.
    #[test]
    fn control_chords_do_not_insert_their_letter() {
        for (ch, expected) in [
            ('a', Key::CtrlA),
            ('c', Key::CtrlC),
            ('d', Key::CtrlD),
            ('e', Key::CtrlE),
            ('k', Key::CtrlK),
            ('u', Key::CtrlU),
            ('w', Key::CtrlW),
            ('j', Key::Newline),
        ] {
            assert_eq!(
                key_from_event(press(KeyCode::Char(ch), KeyModifiers::CONTROL)),
                Some(expected),
                "ctrl-{ch}"
            );
        }
        // The same letters without a modifier are ordinary text.
        assert_eq!(
            key_from_event(press(KeyCode::Char('a'), KeyModifiers::NONE)),
            Some(Key::Char('a'))
        );
    }

    /// Upper-case chords are the same chord: a terminal sends ctrl-shift-A as
    /// `Char('A')` with CONTROL set.
    #[test]
    fn control_chords_ignore_case() {
        assert_eq!(
            key_from_event(press(KeyCode::Char('U'), KeyModifiers::CONTROL)),
            Some(Key::CtrlU)
        );
    }

    #[test]
    fn alt_chords_move_by_word() {
        assert_eq!(
            key_from_event(press(KeyCode::Char('b'), KeyModifiers::ALT)),
            Some(Key::WordLeft)
        );
        assert_eq!(
            key_from_event(press(KeyCode::Char('f'), KeyModifiers::ALT)),
            Some(Key::WordRight)
        );
    }

    /// Enter submits; the newline chords do not. Getting this backwards makes it
    /// impossible to either send a message or write a second line.
    #[test]
    fn enter_submits_and_the_chords_insert_a_newline() {
        assert_eq!(
            key_from_event(press(KeyCode::Enter, KeyModifiers::NONE)),
            Some(Key::Enter)
        );
        assert_eq!(
            key_from_event(press(KeyCode::Enter, KeyModifiers::ALT)),
            Some(Key::Newline)
        );
        assert_eq!(
            key_from_event(press(KeyCode::Char('j'), KeyModifiers::CONTROL)),
            Some(Key::Newline)
        );
    }

    /// Windows sends a Release for every Press; acting on both types everything
    /// twice.
    #[test]
    fn key_releases_are_ignored() {
        let mut release = KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE);
        release.kind = KeyEventKind::Release;
        assert_eq!(key_from_event(release), None);

        let mut repeat = KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE);
        repeat.kind = KeyEventKind::Repeat;
        assert_eq!(
            key_from_event(repeat),
            Some(Key::Char('x')),
            "a held key still types"
        );
    }

    #[test]
    fn unmapped_keys_report_nothing() {
        assert_eq!(
            key_from_event(press(KeyCode::F(5), KeyModifiers::NONE)),
            None
        );
        assert_eq!(
            key_from_event(press(KeyCode::Char('z'), KeyModifiers::CONTROL)),
            None
        );
    }

    fn status() -> Status {
        Status {
            mode: "ask".into(),
            model: "sonnet".into(),
            cwd: "/work".into(),
            ..Status::default()
        }
    }

    /// The area is a rule, the buffer, then the three toolbar rows.
    #[test]
    fn the_layout_frames_the_buffer_between_a_rule_and_the_toolbar() {
        let mut editor = Editor::new();
        editor.set_text("hello");
        let out = layout(&plain(), &editor, None, &status(), 60);
        assert_eq!(out.rows(), 1 + 1 + 3, "rule + one line + toolbar");
        assert!(out.lines[0].contains("input"), "{:?}", out.lines[0]);
        assert_eq!(out.lines[1], "hello");
    }

    /// Plan mode is marked by a dashed rule as well as a colour, so it survives
    /// a terminal with no colour at all.
    #[test]
    fn plan_mode_changes_the_rule_itself() {
        let editor = Editor::new();
        let mut planning = status();
        planning.badges.plan = true;
        let out = layout(&plain(), &editor, None, &planning, 60);
        assert!(
            out.lines[0].starts_with(glyph::RULE_DASHED),
            "{:?}",
            out.lines[0]
        );
        assert!(out.lines[0].contains("plan"), "{:?}", out.lines[0]);

        let normal = layout(&plain(), &editor, None, &status(), 60);
        assert!(
            normal.lines[0].starts_with(glyph::RULE_LIGHT),
            "{:?}",
            normal.lines[0]
        );
    }

    /// Every row must fit: a wrapped input area pushes the transcript up and the
    /// erase then removes the wrong rows.
    #[test]
    fn no_row_of_the_area_exceeds_the_width() {
        let mut editor = Editor::new();
        editor.set_text("/mo");
        let commands = vec![
            Candidate::new("model", "switch model"),
            Candidate::new("mode", "switch permission mode"),
        ];
        let menu = Menu::for_input("/mo", &commands);
        for columns in [20, 40, 80] {
            let out = layout(&plain(), &editor, menu.as_ref(), &status(), columns);
            for line in &out.lines {
                assert!(
                    panel::visible_width(line) <= columns,
                    "at {columns} a row was {}: {line:?}",
                    panel::visible_width(line)
                );
            }
        }
    }

    #[test]
    fn an_open_menu_adds_its_rows_between_the_buffer_and_the_toolbar() {
        let mut editor = Editor::new();
        editor.set_text("/mo");
        let commands = vec![
            Candidate::new("model", "switch model"),
            Candidate::new("mode", "switch permission mode"),
        ];
        let menu = Menu::for_input("/mo", &commands).unwrap();
        let without = layout(&plain(), &editor, None, &status(), 60);
        let with = layout(&plain(), &editor, Some(&menu), &status(), 60);
        assert_eq!(with.rows(), without.rows() + menu.rows());
    }

    /// The cursor is positioned in CELLS: with wide characters a column count
    /// taken from character indices lands in the wrong place.
    #[test]
    fn the_cursor_column_is_measured_in_cells() {
        let mut editor = Editor::new();
        editor.set_text("中文");
        let out = layout(&plain(), &editor, None, &status(), 60);
        assert_eq!(out.cursor_row, 1, "row 0 is the rule");
        assert_eq!(out.cursor_col, 4, "two ideographs are four cells");
    }

    #[test]
    fn a_multiline_buffer_puts_the_cursor_on_its_own_row() {
        let mut editor = Editor::new();
        editor.set_text("one\ntwo");
        let out = layout(&plain(), &editor, None, &status(), 60);
        assert_eq!(out.cursor_row, 2, "second buffer line, after the rule");
        assert_eq!(out.cursor_col, 3);
        assert_eq!(out.lines[1], "one");
        assert_eq!(out.lines[2], "two");
    }

    /// Styling must not change the geometry, only the bytes.
    #[test]
    fn styling_does_not_move_the_cursor() {
        let mut editor = Editor::new();
        editor.set_text("hello");
        let bare = layout(&plain(), &editor, None, &status(), 60);
        let painted = layout(
            &Painter::new(Palette::dark(), true),
            &editor,
            None,
            &status(),
            60,
        );
        assert_eq!(bare.cursor_row, painted.cursor_row);
        assert_eq!(bare.cursor_col, painted.cursor_col);
        assert_eq!(bare.rows(), painted.rows());
    }

    fn frame(painted: usize, layout: &Layout) -> String {
        let mut buffer: Vec<u8> = Vec::new();
        paint_frame(&mut buffer, painted, layout).unwrap();
        String::from_utf8(buffer).unwrap()
    }

    /// The very first paint must NOT emit a cursor-up: there is nothing above it
    /// yet, so a `[NA` here would walk into the transcript and overwrite it.
    #[test]
    fn the_first_paint_clears_without_moving_up() {
        let mut editor = Editor::new();
        editor.set_text("hi");
        let out = frame(0, &layout(&plain(), &editor, None, &status(), 60));
        assert!(
            !out.contains("A\x1b[J"),
            "no cursor-up on the first paint: {out:?}"
        );
        assert!(
            out.contains("\x1b[J"),
            "but it still clears downward: {out:?}"
        );
    }

    /// A repaint rewinds by EXACTLY the rows the previous paint drew. Rewinding
    /// too few leaves stale rows; too many eats the transcript above.
    #[test]
    fn a_repaint_rewinds_by_the_previous_row_count() {
        let mut editor = Editor::new();
        editor.set_text("hi");
        let drawn = layout(&plain(), &editor, None, &status(), 60);
        let rows = drawn.rows();
        let out = frame(rows, &drawn);
        assert!(
            out.contains(&format!("\x1b[{rows}A")),
            "must rewind {rows} rows: {out:?}"
        );
    }

    /// The cursor lands where the buffer says. A single-line entry parks the
    /// cursor `toolbar` rows up from the bottom and at the text's cell column.
    #[test]
    fn the_paint_parks_the_cursor_at_the_buffer_position() {
        let mut editor = Editor::new();
        editor.set_text("abc");
        let drawn = layout(&plain(), &editor, None, &status(), 60);
        // rule + buffer + three toolbar rows = 5; cursor on row 1, so three rows
        // sit below it and the paint moves back up by three.
        let below = drawn.rows() - 1 - drawn.cursor_row;
        let out = frame(0, &drawn);
        assert!(
            out.contains(&format!("\x1b[{below}A")),
            "cursor up {below}: {out:?}"
        );
        assert!(
            out.contains(&format!("\x1b[{}C", drawn.cursor_col)),
            "cursor col: {out:?}"
        );
    }

    #[test]
    fn erase_rewinds_and_clears() {
        let mut buffer: Vec<u8> = Vec::new();
        erase_frame(&mut buffer, 4).unwrap();
        assert_eq!(String::from_utf8(buffer).unwrap(), "\r\x1b[4A\x1b[J");
    }
}
