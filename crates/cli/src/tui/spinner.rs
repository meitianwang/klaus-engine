//! Spinner frames, and the phase maths that turns a clock into one.
//!
//! Frames are chosen by wall clock rather than advanced by a counter. The live
//! region repaints on its own cadence and may skip a beat under load; a counter
//! would then run slow and, worse, run at a different rate than a second spinner
//! on screen. Deriving the index from elapsed time keeps every animation on the
//! same clock, so they stay in step and a dropped repaint costs a frame instead
//! of a permanent drift.

use std::time::Duration;

/// One animation: its frames and how long each is held.
#[derive(Debug, Clone, Copy)]
pub struct Spinner {
    frames: &'static [&'static str],
    interval_ms: u64,
}

impl Spinner {
    /// The workhorse — braille dots, for anything streaming. Narrow, single
    /// cell, and reads as motion even in a terminal that renders it dim.
    pub const DOTS: Self = Self {
        frames: &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"],
        interval_ms: 80,
    };

    /// For waiting with nothing to report: the engine is working, but no content
    /// has arrived. Deliberately not the dots — a distinct shape says "still
    /// alive, nothing to show" without needing a caption.
    pub const MOON: Self = Self {
        frames: &["🌑", "🌒", "🌓", "🌔", "🌕", "🌖", "🌗", "🌘"],
        interval_ms: 80,
    };

    /// For compaction, which is slow and rare enough to deserve its own shape.
    pub const BALLOON: Self = Self {
        frames: &[" ", ".", "o", "O", "@", "*", " "],
        interval_ms: 140,
    };

    /// A drifting dot, for the one-line reasoning indicator. Three cells wide and
    /// always three cells wide, so the text after it never shifts — a spinner
    /// that changes width drags the whole line left and right.
    pub const DRIFT: Self = Self {
        frames: &[".  ", ".. ", "...", " ..", "  .", "   "],
        interval_ms: 130,
    };

    /// The frame to draw for an animation that has been running `elapsed`.
    pub fn frame(&self, elapsed: Duration) -> &'static str {
        let step = (elapsed.as_millis() / u128::from(self.interval_ms)) as usize;
        self.frames[step % self.frames.len()]
    }

    /// How wide this spinner's cell is, so a caller can reserve the column and
    /// keep the text beside it from moving.
    pub fn cells(&self) -> usize {
        self.frames
            .iter()
            .map(|frame| super::paint::width(frame))
            .max()
            .unwrap_or(1)
    }

    pub const fn interval(&self) -> Duration {
        Duration::from_millis(self.interval_ms)
    }
}

/// `1.2s`, `45s`, `3m12s` — an elapsed time short enough to sit in a status line.
///
/// Sub-minute times keep one decimal because that is the range where a user is
/// judging whether something is stuck; past a minute the tenths are noise.
pub fn elapsed_label(elapsed: Duration) -> String {
    let secs = elapsed.as_secs_f64();
    if secs < 60.0 {
        return format!("{secs:.1}s");
    }
    let whole = elapsed.as_secs();
    let minutes = whole / 60;
    let seconds = whole % 60;
    if minutes < 60 {
        return format!("{minutes}m{seconds:02}s");
    }
    format!("{}h{:02}m", minutes / 60, minutes % 60)
}

/// `847`, `1.2k`, `2.3m` — a token count abbreviated for a status line.
///
/// Trailing `.0` is dropped so the field does not jitter between `1.0k` and
/// `1k` as the count crosses a boundary.
pub fn count_label(count: u64) -> String {
    fn scaled(value: f64, suffix: &str) -> String {
        let rounded = (value * 10.0).round() / 10.0;
        if (rounded.fract()).abs() < f64::EPSILON {
            format!("{}{suffix}", rounded as u64)
        } else {
            format!("{rounded:.1}{suffix}")
        }
    }
    match count {
        0..=999 => count.to_string(),
        1_000..=999_999 => scaled(count as f64 / 1_000.0, "k"),
        _ => scaled(count as f64 / 1_000_000.0, "m"),
    }
}

/// `12 tok/s`, or nothing.
///
/// A rate computed over a very short window is meaningless — the first token of
/// a turn arrives after the connection settles, which would report a wildly high
/// figure and then visibly fall. So it is withheld until there is enough of a
/// window to be worth reading.
pub fn rate_label(tokens: u64, elapsed: Duration) -> Option<String> {
    if tokens == 0 || elapsed.as_secs_f64() <= 0.5 {
        return None;
    }
    let rate = (tokens as f64 / elapsed.as_secs_f64()).round() as u64;
    (rate > 0).then(|| format!("{rate} tok/s"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_advance_on_the_clock_and_wrap() {
        let spinner = Spinner::DOTS;
        assert_eq!(spinner.frame(Duration::ZERO), "⠋");
        assert_eq!(spinner.frame(Duration::from_millis(80)), "⠙");
        // Ten frames at 80ms is 800ms, so the cycle restarts there.
        assert_eq!(spinner.frame(Duration::from_millis(800)), "⠋");
        // Mid-frame time floors to the frame it is inside.
        assert_eq!(spinner.frame(Duration::from_millis(159)), "⠙");
    }

    /// The drift spinner sits in a fixed-width column: if its frames differed in
    /// width the text beside it would shift on every tick.
    #[test]
    fn drift_frames_all_occupy_the_same_columns() {
        let widths: Vec<usize> = ["...", ".  ", "   "]
            .iter()
            .map(|frame| super::super::paint::width(frame))
            .collect();
        assert_eq!(widths, vec![3, 3, 3]);
        assert_eq!(Spinner::DRIFT.cells(), 3);
    }

    /// Moon frames are emoji and therefore two cells; a caller reserving one
    /// column for them would have every following field overlap.
    #[test]
    fn moon_reports_the_two_cells_emoji_actually_take() {
        assert_eq!(Spinner::MOON.cells(), 2);
        assert_eq!(Spinner::DOTS.cells(), 1);
    }

    #[test]
    fn elapsed_reads_as_tenths_then_minutes_then_hours() {
        assert_eq!(elapsed_label(Duration::from_millis(1_240)), "1.2s");
        assert_eq!(elapsed_label(Duration::from_secs(45)), "45.0s");
        assert_eq!(elapsed_label(Duration::from_secs(192)), "3m12s");
        assert_eq!(elapsed_label(Duration::from_secs(3_600)), "1h00m");
    }

    #[test]
    fn counts_abbreviate_without_a_trailing_zero() {
        assert_eq!(count_label(0), "0");
        assert_eq!(count_label(999), "999");
        assert_eq!(count_label(1_000), "1k");
        assert_eq!(count_label(1_240), "1.2k");
        assert_eq!(count_label(12_000), "12k");
        assert_eq!(count_label(2_300_000), "2.3m");
    }

    /// A rate over a sub-second window is noise, and is withheld rather than
    /// shown and then visibly corrected.
    #[test]
    fn a_rate_needs_a_window_worth_measuring() {
        assert_eq!(rate_label(100, Duration::from_millis(200)), None);
        assert_eq!(rate_label(0, Duration::from_secs(10)), None);
        assert_eq!(
            rate_label(120, Duration::from_secs(10)),
            Some("12 tok/s".to_string())
        );
    }
}
