//! The line editor, as a state machine with no terminal in it.
//!
//! ## Why this is written out rather than pulled in
//!
//! An editor that owns the terminal can only be checked by looking at it. This
//! one is a value: keys go in, a new state comes out, and every behaviour below
//! is a unit test that runs headless. That is the difference between "cursor
//! movement works on my machine" and a gate that fails when it stops working.
//!
//! ## What it does and does not handle
//!
//! Insert, delete, word and line motions, multi-line entry, and history. It does
//! NOT handle rendering, wrapping or the terminal's own line discipline; those
//! belong to the shell. It also does not implement a kill ring — one level of
//! killed text is what `ctrl-u`/`ctrl-k`/`ctrl-w` need, and a ring the user
//! cannot see the contents of is a feature that only surprises people.
//!
//! ## Positions are in CHARACTERS, not bytes or cells
//!
//! The buffer is a `Vec<char>` and the cursor is an index into it. Bytes would
//! make every motion a UTF-8 boundary problem; cells would make `left` ambiguous
//! for a wide character. Cell width matters only when drawing, so it lives with
//! the drawing code.

/// What the shell should do after feeding a key in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The buffer changed, or the cursor moved. Repaint.
    Redraw,
    /// The user submitted this text.
    Submit(String),
    /// Interrupt: `ctrl-c`.
    Interrupt,
    /// End of input: `ctrl-d` on an empty buffer.
    Eof,
    /// Nothing this editor handles. The shell may claim the key itself.
    Ignored,
}

/// A key, reduced to what the editor cares about.
///
/// Deliberately not `crossterm::event::KeyEvent`: keeping the editor free of the
/// terminal crate is what lets these tests run without a tty, and it makes the
/// mapping from real key events an explicit, reviewable step in the shell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Key {
    Char(char),
    Backspace,
    Delete,
    Left,
    Right,
    Up,
    Down,
    Home,
    End,
    /// Submit.
    Enter,
    /// Insert a newline instead of submitting (`ctrl-j`, or `alt-enter`).
    Newline,
    Tab,
    Esc,
    CtrlA,
    CtrlC,
    CtrlD,
    CtrlE,
    CtrlK,
    CtrlU,
    CtrlW,
    /// Word left / right (`alt-b` / `alt-f`).
    WordLeft,
    WordRight,
}

#[derive(Debug, Default)]
pub struct Editor {
    buffer: Vec<char>,
    cursor: usize,
    /// Submitted lines, oldest first.
    history: Vec<String>,
    /// Where in history the user is browsing. `None` means "at the live buffer".
    browsing: Option<usize>,
    /// The live buffer, parked while browsing history so that coming back down
    /// past the newest entry restores what was being typed rather than clearing
    /// it — losing a half-written prompt to an accidental Up is the single most
    /// annoying way a line editor can fail.
    parked: Option<Vec<char>>,
}

impl Editor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn text(&self) -> String {
        self.buffer.iter().collect()
    }

    pub fn cursor(&self) -> usize {
        self.cursor
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// The text before and after the cursor, for a caller that draws them apart.
    pub fn split(&self) -> (String, String) {
        (
            self.buffer[..self.cursor].iter().collect(),
            self.buffer[self.cursor..].iter().collect(),
        )
    }

    /// The buffer as display lines. Multi-line entry is one buffer with `\n` in
    /// it, so the shell asks for the split rather than tracking lines itself.
    pub fn lines(&self) -> Vec<String> {
        self.text().split('\n').map(str::to_string).collect()
    }

    /// `(line, column)` of the cursor, both zero-based, in characters.
    pub fn cursor_position(&self) -> (usize, usize) {
        let before = &self.buffer[..self.cursor];
        let line = before.iter().filter(|ch| **ch == '\n').count();
        let column = before.iter().rev().take_while(|ch| **ch != '\n').count();
        (line, column)
    }

    pub fn set_text(&mut self, text: &str) {
        self.buffer = text.chars().collect();
        self.cursor = self.buffer.len();
    }

    pub fn clear(&mut self) {
        self.buffer.clear();
        self.cursor = 0;
        self.browsing = None;
        self.parked = None;
    }

    /// Record a submitted line in history.
    ///
    /// Blank lines and an immediate repeat are dropped: neither is worth a slot,
    /// and a repeat means Up has to be pressed twice to get past it.
    pub fn remember(&mut self, line: &str) {
        let line = line.trim();
        if line.is_empty() {
            return;
        }
        if self.history.last().map(String::as_str) == Some(line) {
            return;
        }
        self.history.push(line.to_string());
    }

    pub fn history(&self) -> &[String] {
        &self.history
    }

    pub fn apply(&mut self, key: Key) -> Outcome {
        match key {
            Key::Char(ch) => {
                self.buffer.insert(self.cursor, ch);
                self.cursor += 1;
                Outcome::Redraw
            }
            Key::Newline => {
                self.buffer.insert(self.cursor, '\n');
                self.cursor += 1;
                Outcome::Redraw
            }
            Key::Enter => {
                let text = self.text();
                self.clear();
                Outcome::Submit(text)
            }
            Key::Backspace => {
                if self.cursor == 0 {
                    return Outcome::Redraw;
                }
                self.cursor -= 1;
                self.buffer.remove(self.cursor);
                Outcome::Redraw
            }
            Key::Delete => {
                if self.cursor < self.buffer.len() {
                    self.buffer.remove(self.cursor);
                }
                Outcome::Redraw
            }
            Key::Left => {
                self.cursor = self.cursor.saturating_sub(1);
                Outcome::Redraw
            }
            Key::Right => {
                if self.cursor < self.buffer.len() {
                    self.cursor += 1;
                }
                Outcome::Redraw
            }
            Key::Home | Key::CtrlA => {
                self.cursor = self.line_start();
                Outcome::Redraw
            }
            Key::End | Key::CtrlE => {
                self.cursor = self.line_end();
                Outcome::Redraw
            }
            Key::WordLeft => {
                self.cursor = self.word_left();
                Outcome::Redraw
            }
            Key::WordRight => {
                self.cursor = self.word_right();
                Outcome::Redraw
            }
            Key::CtrlU => {
                // To start of LINE, not of buffer: in a multi-line entry killing
                // everything above the cursor is never what was meant.
                let start = self.line_start();
                self.buffer.drain(start..self.cursor);
                self.cursor = start;
                Outcome::Redraw
            }
            Key::CtrlK => {
                let end = self.line_end();
                self.buffer.drain(self.cursor..end);
                Outcome::Redraw
            }
            Key::CtrlW => {
                let start = self.word_left();
                self.buffer.drain(start..self.cursor);
                self.cursor = start;
                Outcome::Redraw
            }
            Key::Up => self.browse_back(),
            Key::Down => self.browse_forward(),
            Key::CtrlC => Outcome::Interrupt,
            Key::CtrlD => {
                // Only an EMPTY buffer means "no more input". With text present
                // it deletes forward, which is what every readline does — and
                // quitting on it mid-line would throw away the line.
                if self.buffer.is_empty() {
                    Outcome::Eof
                } else {
                    self.apply(Key::Delete)
                }
            }
            Key::Tab | Key::Esc => Outcome::Ignored,
        }
    }

    /// Start of the line the cursor is on.
    fn line_start(&self) -> usize {
        self.buffer[..self.cursor]
            .iter()
            .rposition(|ch| *ch == '\n')
            .map(|index| index + 1)
            .unwrap_or(0)
    }

    /// End of the line the cursor is on.
    fn line_end(&self) -> usize {
        self.buffer[self.cursor..]
            .iter()
            .position(|ch| *ch == '\n')
            .map(|offset| self.cursor + offset)
            .unwrap_or(self.buffer.len())
    }

    /// The start of the word before the cursor.
    ///
    /// Whitespace immediately behind the cursor is skipped first, so pressing
    /// `ctrl-w` after a word plus a space kills the word rather than the space.
    fn word_left(&self) -> usize {
        let mut index = self.cursor;
        while index > 0 && self.buffer[index - 1].is_whitespace() {
            index -= 1;
        }
        while index > 0 && !self.buffer[index - 1].is_whitespace() {
            index -= 1;
        }
        index
    }

    fn word_right(&self) -> usize {
        let mut index = self.cursor;
        while index < self.buffer.len() && self.buffer[index].is_whitespace() {
            index += 1;
        }
        while index < self.buffer.len() && !self.buffer[index].is_whitespace() {
            index += 1;
        }
        index
    }

    /// Up: into older history.
    ///
    /// While the buffer spans several lines, Up moves the CURSOR instead — in a
    /// multi-line entry the arrow keys have to navigate the text, or editing the
    /// first line of a three-line prompt becomes impossible.
    fn browse_back(&mut self) -> Outcome {
        if self.buffer.contains(&'\n') {
            return self.cursor_up();
        }
        if self.history.is_empty() {
            return Outcome::Redraw;
        }
        let next = match self.browsing {
            None => {
                self.parked = Some(self.buffer.clone());
                self.history.len() - 1
            }
            Some(0) => 0,
            Some(index) => index - 1,
        };
        self.browsing = Some(next);
        self.buffer = self.history[next].chars().collect();
        self.cursor = self.buffer.len();
        Outcome::Redraw
    }

    fn browse_forward(&mut self) -> Outcome {
        if self.browsing.is_none() && self.buffer.contains(&'\n') {
            return self.cursor_down();
        }
        let Some(index) = self.browsing else {
            return Outcome::Redraw;
        };
        if index + 1 < self.history.len() {
            self.browsing = Some(index + 1);
            self.buffer = self.history[index + 1].chars().collect();
        } else {
            // Past the newest entry: back to whatever was being typed.
            self.browsing = None;
            self.buffer = self.parked.take().unwrap_or_default();
        }
        self.cursor = self.buffer.len();
        Outcome::Redraw
    }

    /// Move the cursor one display line up, keeping the column where possible.
    fn cursor_up(&mut self) -> Outcome {
        let start = self.line_start();
        if start == 0 {
            return Outcome::Redraw;
        }
        let column = self.cursor - start;
        let previous_start = self.buffer[..start - 1]
            .iter()
            .rposition(|ch| *ch == '\n')
            .map(|index| index + 1)
            .unwrap_or(0);
        let previous_len = start - 1 - previous_start;
        self.cursor = previous_start + column.min(previous_len);
        Outcome::Redraw
    }

    fn cursor_down(&mut self) -> Outcome {
        let end = self.line_end();
        if end >= self.buffer.len() {
            return Outcome::Redraw;
        }
        let column = self.cursor - self.line_start();
        let next_start = end + 1;
        let next_len = self.buffer[next_start..]
            .iter()
            .position(|ch| *ch == '\n')
            .unwrap_or(self.buffer.len() - next_start);
        self.cursor = next_start + column.min(next_len);
        Outcome::Redraw
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn typed(text: &str) -> Editor {
        let mut editor = Editor::new();
        for ch in text.chars() {
            editor.apply(Key::Char(ch));
        }
        editor
    }

    #[test]
    fn typing_inserts_at_the_cursor() {
        let mut editor = typed("helo");
        editor.apply(Key::Left);
        editor.apply(Key::Char('l'));
        assert_eq!(editor.text(), "hello");
        assert_eq!(editor.cursor(), 4);
    }

    #[test]
    fn enter_submits_and_clears() {
        let mut editor = typed("run it");
        assert_eq!(editor.apply(Key::Enter), Outcome::Submit("run it".into()));
        assert!(editor.is_empty());
        assert_eq!(editor.cursor(), 0);
    }

    #[test]
    fn backspace_at_the_start_is_harmless() {
        let mut editor = Editor::new();
        assert_eq!(editor.apply(Key::Backspace), Outcome::Redraw);
        assert_eq!(editor.text(), "");
        assert_eq!(editor.cursor(), 0);
    }

    #[test]
    fn motions_clamp_at_both_ends() {
        let mut editor = typed("ab");
        editor.apply(Key::Left);
        editor.apply(Key::Left);
        editor.apply(Key::Left);
        assert_eq!(editor.cursor(), 0);
        for _ in 0..5 {
            editor.apply(Key::Right);
        }
        assert_eq!(editor.cursor(), 2);
    }

    /// ctrl-d is EOF only on an empty buffer; with text it deletes forward.
    /// Quitting mid-line would throw the line away.
    #[test]
    fn ctrl_d_is_eof_only_when_there_is_nothing_to_delete() {
        let mut empty = Editor::new();
        assert_eq!(empty.apply(Key::CtrlD), Outcome::Eof);

        let mut editor = typed("abc");
        editor.apply(Key::Home);
        assert_eq!(editor.apply(Key::CtrlD), Outcome::Redraw);
        assert_eq!(editor.text(), "bc");
    }

    #[test]
    fn ctrl_c_reports_an_interrupt_without_touching_the_buffer() {
        let mut editor = typed("half typed");
        assert_eq!(editor.apply(Key::CtrlC), Outcome::Interrupt);
        assert_eq!(
            editor.text(),
            "half typed",
            "the line survives for the shell to decide"
        );
    }

    #[test]
    fn word_motions_skip_the_space_then_the_word() {
        let mut editor = typed("alpha beta gamma");
        editor.apply(Key::WordLeft);
        assert_eq!(editor.cursor(), 11, "start of `gamma`");
        editor.apply(Key::WordLeft);
        assert_eq!(editor.cursor(), 6, "start of `beta`");
        editor.apply(Key::WordRight);
        assert_eq!(editor.cursor(), 10, "end of `beta`");
    }

    /// ctrl-w after a trailing space kills the word, not the space.
    #[test]
    fn ctrl_w_kills_the_word_behind_trailing_space() {
        let mut editor = typed("keep this ");
        editor.apply(Key::CtrlW);
        assert_eq!(editor.text(), "keep ");
    }

    #[test]
    fn ctrl_k_and_ctrl_u_cut_to_the_line_ends() {
        let mut editor = typed("hello world");
        editor.apply(Key::Home);
        editor.apply(Key::WordRight);
        editor.apply(Key::CtrlK);
        assert_eq!(editor.text(), "hello");

        let mut editor = typed("hello world");
        editor.apply(Key::CtrlU);
        assert_eq!(editor.text(), "");
    }

    /// In a multi-line buffer these act on the LINE, not the whole buffer.
    #[test]
    fn line_kills_respect_line_boundaries() {
        let mut editor = typed("first");
        editor.apply(Key::Newline);
        for ch in "second".chars() {
            editor.apply(Key::Char(ch));
        }
        editor.apply(Key::CtrlU);
        assert_eq!(editor.text(), "first\n", "only the second line is gone");
    }

    #[test]
    fn newline_extends_the_buffer_instead_of_submitting() {
        let mut editor = typed("one");
        assert_eq!(editor.apply(Key::Newline), Outcome::Redraw);
        for ch in "two".chars() {
            editor.apply(Key::Char(ch));
        }
        assert_eq!(editor.text(), "one\ntwo");
        assert_eq!(editor.lines(), vec!["one", "two"]);
        assert_eq!(editor.apply(Key::Enter), Outcome::Submit("one\ntwo".into()));
    }

    #[test]
    fn the_cursor_reports_its_line_and_column() {
        let mut editor = typed("ab");
        editor.apply(Key::Newline);
        for ch in "cde".chars() {
            editor.apply(Key::Char(ch));
        }
        assert_eq!(editor.cursor_position(), (1, 3));
        editor.apply(Key::Home);
        assert_eq!(editor.cursor_position(), (1, 0));
    }

    #[test]
    fn history_walks_back_and_returns_to_the_live_buffer() {
        let mut editor = Editor::new();
        editor.remember("first");
        editor.remember("second");
        for ch in "half".chars() {
            editor.apply(Key::Char(ch));
        }

        editor.apply(Key::Up);
        assert_eq!(editor.text(), "second");
        editor.apply(Key::Up);
        assert_eq!(editor.text(), "first");
        // Past the oldest entry it stays put rather than clearing.
        editor.apply(Key::Up);
        assert_eq!(editor.text(), "first");

        editor.apply(Key::Down);
        assert_eq!(editor.text(), "second");
        editor.apply(Key::Down);
        assert_eq!(editor.text(), "half", "the parked line comes back");
    }

    #[test]
    fn history_drops_blanks_and_immediate_repeats() {
        let mut editor = Editor::new();
        editor.remember("same");
        editor.remember("same");
        editor.remember("   ");
        editor.remember("");
        assert_eq!(editor.history(), &["same".to_string()]);
    }

    /// In a multi-line entry the arrows navigate the text; taking them for
    /// history would make the first line of a three-line prompt unreachable.
    #[test]
    fn arrows_navigate_a_multiline_buffer_instead_of_history() {
        let mut editor = Editor::new();
        editor.remember("old entry");
        editor.set_text("line one\nline two");
        editor.apply(Key::Up);
        assert_eq!(
            editor.text(),
            "line one\nline two",
            "history must not fire here"
        );
        assert_eq!(
            editor.cursor_position().0,
            0,
            "cursor moved to the first line"
        );
        editor.apply(Key::Down);
        assert_eq!(editor.cursor_position().0, 1);
    }

    /// Moving up into a shorter line lands at its end rather than past it.
    #[test]
    fn cursor_up_clamps_to_the_shorter_line() {
        let mut editor = Editor::new();
        editor.set_text("ab\nlonger line");
        editor.apply(Key::End);
        editor.apply(Key::Up);
        assert_eq!(
            editor.cursor_position(),
            (0, 2),
            "clamped to the end of `ab`"
        );
    }

    #[test]
    fn unhandled_keys_are_reported_so_the_shell_can_claim_them() {
        let mut editor = typed("x");
        assert_eq!(editor.apply(Key::Tab), Outcome::Ignored);
        assert_eq!(editor.apply(Key::Esc), Outcome::Ignored);
        assert_eq!(editor.text(), "x");
    }

    /// Positions are character indices, so multi-byte and wide text behaves like
    /// any other text.
    #[test]
    fn multibyte_text_edits_by_character() {
        let mut editor = typed("中文字");
        assert_eq!(editor.cursor(), 3);
        editor.apply(Key::Backspace);
        assert_eq!(editor.text(), "中文");
        editor.apply(Key::Home);
        editor.apply(Key::Char('a'));
        assert_eq!(editor.text(), "a中文");
    }

    #[test]
    fn split_hands_back_both_sides_of_the_cursor() {
        let mut editor = typed("abcd");
        editor.apply(Key::Left);
        editor.apply(Key::Left);
        assert_eq!(editor.split(), ("ab".to_string(), "cd".to_string()));
    }
}
