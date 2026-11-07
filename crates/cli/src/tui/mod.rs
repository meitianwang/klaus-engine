//! The terminal interface: what a session LOOKS like.
//!
//! ## Why this is a subsystem and not a formatting helper
//!
//! Everything visual lives behind this module, and it is split so that almost
//! none of it can touch a terminal:
//!
//! - [`theme`] holds every colour, glyph and limit as data. No logic.
//! - [`paint`] decides whether colour is live, and measures text in cells.
//! - [`panel`], [`markdown`], [`diff`], [`spinner`], [`toolbar`], [`menu`] and
//!   [`editor`] are pure: borrowed input in, `String` or state out. They are
//!   unit-tested without a terminal, which is the only way this stays honest in
//!   a headless test gate.
//! - [`shell`] is the sole owner of raw mode and cursor control.
//!
//! That shape is deliberate. A TUI whose layout maths is entangled with its
//! terminal writes can only be verified by looking at it, and "looks right on my
//! machine" is not a gate. Keeping the maths pure means a wrong column count or
//! a mis-cut wide character is a failing assertion instead of a bug report.
//!
//! ## The visual grammar
//!
//! One bullet column carries state by COLOUR rather than by symbol: `•` is green
//! when a tool succeeded, red when it failed, grey while it is reasoning. A
//! reader scanning the left edge sees a single column instead of a symbol zoo,
//! and the shapes stay available for the few things that genuinely differ — the
//! spinner that replaces the bullet while work is in flight, `→` for the
//! selected row of a chooser.
//!
//! ## Degrading is not a special case
//!
//! A pipe, `--json`, `NO_COLOR`, a dumb terminal and a CI log all get plain
//! text through the same renderers, because the only difference is a flag on
//! [`paint::Painter`]. The interactive shell is the one part that cannot degrade
//! into a flag: without a terminal there are no key events to read, so the REPL
//! keeps its line-based loop and the raw-mode shell simply never starts.

pub mod diff;
pub mod editor;
pub mod markdown;
pub mod menu;
pub mod paint;
pub mod panel;
pub mod shell;
pub mod spinner;
pub mod theme;
pub mod toolbar;

pub use paint::Painter;
pub use theme::{glyph, limit, Palette, Severity};
