//! The palette, the glyphs, and the limits — every visual constant in one table.
//!
//! Style values live here and nowhere else so that a reader can see the whole
//! look at once, and so that changing it is one edit rather than a hunt through
//! the renderers. Nothing in this file touches a terminal or decides whether
//! colour is live; that belongs to [`super::paint`].
//!
//! The two palettes are not a preference toggle for its own sake. A terminal
//! with a light background renders mid-grey metadata as near-invisible, and the
//! dim backgrounds a diff needs are the inverse of the light-background ones, so
//! a single palette necessarily fails one of the two.

use anstyle::{AnsiColor, Color, Effects, RgbColor, Style};

/// Which background the palette is built for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Dark,
    Light,
}

impl Mode {
    /// The mode a `KLAUS_THEME` value names, if it names one.
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "dark" => Some(Self::Dark),
            "light" => Some(Self::Light),
            _ => None,
        }
    }
}

/// Foreground/background pairs for one side of a diff.
#[derive(Debug, Clone, Copy)]
pub struct DiffColors {
    /// Whole-line wash for an added line.
    pub add_bg: Style,
    /// Whole-line wash for a removed line.
    pub del_bg: Style,
    /// Word-level emphasis, laid over the line wash.
    pub add_hl: Style,
    pub del_hl: Style,
}

/// Every style the renderers may ask for.
///
/// Semantic names, not colour names: a renderer says "this is metadata", and the
/// palette decides what metadata looks like. That is what keeps the two modes
/// from needing two renderers.
#[derive(Debug, Clone, Copy)]
pub struct Palette {
    pub mode: Mode,

    // ---- text weights ----
    /// Ordinary body text: deliberately unstyled so the terminal's own
    /// foreground wins. Overriding it fights every user colour scheme.
    pub body: Style,
    /// Secondary text — timings, token counts, elided previews, tool output.
    pub meta: Style,
    /// Reasoning: the same grey as metadata, plus italic, because grey alone
    /// does not survive a terminal that renders all dim text identically.
    pub reasoning: Style,
    /// Faint structural furniture: diff gutters, truncation notices.
    pub faint: Style,

    // ---- semantic accents ----
    /// The brand accent: banner border and logo.
    pub brand: Style,
    /// A tool's name.
    pub tool: Style,
    /// A finished tool that succeeded.
    pub ok: Style,
    /// A finished tool that failed.
    pub bad: Style,
    /// Something in progress, or the selected row of a chooser.
    pub active: Style,
    /// A caution: approval borders, degraded configuration.
    pub warn: Style,
    /// An error message.
    pub error: Style,
    /// A shell command echoed back.
    pub command: Style,

    // ---- markdown ----
    pub md_h1: Style,
    pub md_h2: Style,
    pub md_h3: Style,
    pub md_h6: Style,
    pub md_code: Style,
    pub md_code_block: Style,
    pub md_link: Style,
    pub md_link_url: Style,
    pub md_rule: Style,
    pub md_quote: Style,

    // ---- diff ----
    pub diff: DiffColors,
    pub diff_add_marker: Style,
    pub diff_del_marker: Style,
    pub diff_add_head: Style,
    pub diff_del_head: Style,

    // ---- shell furniture ----
    /// The `── input ──` rule and the toolbar's top edge.
    pub separator: Style,
    /// Placeholder and hint text inside the input area.
    pub placeholder: Style,
    /// The working directory in the toolbar.
    pub cwd: Style,
    /// Background task counts.
    pub bg_tasks: Style,
    /// The rotating key hint.
    pub tip: Style,
    /// Mode badges.
    pub badge_yolo: Style,
    pub badge_afk: Style,
    pub badge_plan: Style,
    /// The dashed rule and title of the plan-mode input area.
    pub plan_rule: Style,

    // ---- slash menu ----
    pub menu_marker: Style,
    pub menu_marker_current: Style,
    pub menu_command: Style,
    pub menu_command_current: Style,
    pub menu_meta: Style,
    pub menu_meta_current: Style,
}

const fn fg(color: Color) -> Style {
    Style::new().fg_color(Some(color))
}

const fn rgb(r: u8, g: u8, b: u8) -> Color {
    Color::Rgb(RgbColor(r, g, b))
}

const fn ansi(color: AnsiColor) -> Color {
    Color::Ansi(color)
}

/// 8-bit greys, used where a mid-grey has to read as "quieter than body text"
/// on both truecolour and 256-colour terminals.
const fn grey(index: u8) -> Color {
    Color::Ansi256(anstyle::Ansi256Color(index))
}

impl Palette {
    pub const fn dark() -> Self {
        Self {
            mode: Mode::Dark,
            body: Style::new(),
            meta: fg(grey(245)),
            reasoning: fg(grey(245)).effects(Effects::ITALIC),
            faint: Style::new().effects(Effects::DIMMED),
            brand: fg(ansi(AnsiColor::BrightBlue)),
            tool: fg(ansi(AnsiColor::Blue)),
            ok: fg(ansi(AnsiColor::Green)),
            bad: fg(ansi(AnsiColor::Red)),
            active: fg(ansi(AnsiColor::Cyan)),
            warn: fg(ansi(AnsiColor::Yellow)),
            error: fg(ansi(AnsiColor::Red)),
            command: fg(grey(252)),

            md_h1: fg(ansi(AnsiColor::BrightWhite)).effects(Effects::BOLD),
            md_h2: fg(ansi(AnsiColor::White)).effects(Effects::BOLD),
            md_h3: Style::new().effects(Effects::BOLD),
            md_h6: Style::new().effects(Effects::DIMMED.insert(Effects::ITALIC)),
            md_code: fg(ansi(AnsiColor::BrightCyan)).effects(Effects::BOLD),
            md_code_block: fg(ansi(AnsiColor::BrightCyan)),
            md_link: fg(ansi(AnsiColor::BrightBlue)).effects(Effects::UNDERLINE),
            md_link_url: fg(ansi(AnsiColor::Cyan)).effects(Effects::UNDERLINE),
            md_rule: fg(grey(246)),
            md_quote: fg(grey(245)),

            diff: DiffColors {
                add_bg: Style::new().bg_color(Some(rgb(0x12, 0x26, 0x1e))),
                del_bg: Style::new().bg_color(Some(rgb(0x2d, 0x12, 0x14))),
                add_hl: Style::new().bg_color(Some(rgb(0x1a, 0x4a, 0x2e))),
                del_hl: Style::new().bg_color(Some(rgb(0x5c, 0x1a, 0x1d))),
            },
            diff_add_marker: fg(ansi(AnsiColor::Green)),
            diff_del_marker: fg(ansi(AnsiColor::Red)),
            diff_add_head: fg(ansi(AnsiColor::Green)).effects(Effects::BOLD),
            diff_del_head: fg(ansi(AnsiColor::Red)).effects(Effects::BOLD),

            separator: fg(rgb(0x4a, 0x55, 0x68)),
            placeholder: fg(rgb(0x7c, 0x85, 0x94)).effects(Effects::ITALIC),
            cwd: fg(rgb(0x66, 0x66, 0x66)),
            bg_tasks: fg(rgb(0x88, 0x88, 0x88)),
            tip: fg(rgb(0x55, 0x55, 0x55)),
            badge_yolo: fg(rgb(0xff, 0xff, 0x00)).effects(Effects::BOLD),
            badge_afk: fg(rgb(0xff, 0x88, 0x00)).effects(Effects::BOLD),
            badge_plan: fg(rgb(0x00, 0xaa, 0xff)).effects(Effects::BOLD),
            plan_rule: fg(rgb(0x60, 0xa5, 0xfa)),

            menu_marker: fg(rgb(0x4a, 0x55, 0x68)),
            menu_marker_current: fg(rgb(0x4f, 0x9f, 0xff)),
            menu_command: fg(rgb(0xa6, 0xad, 0xba)),
            menu_command_current: fg(rgb(0x6f, 0xb7, 0xff)).effects(Effects::BOLD),
            menu_meta: fg(rgb(0x7c, 0x85, 0x94)),
            menu_meta_current: fg(rgb(0x56, 0xa4, 0xff)),
        }
    }

    pub const fn light() -> Self {
        Self {
            mode: Mode::Light,
            body: Style::new(),
            meta: fg(grey(243)),
            reasoning: fg(grey(243)).effects(Effects::ITALIC),
            faint: Style::new().effects(Effects::DIMMED),
            brand: fg(ansi(AnsiColor::Blue)),
            tool: fg(ansi(AnsiColor::Blue)),
            ok: fg(ansi(AnsiColor::Green)),
            bad: fg(ansi(AnsiColor::Red)),
            active: fg(ansi(AnsiColor::Blue)),
            warn: fg(rgb(0x92, 0x40, 0x0e)),
            error: fg(ansi(AnsiColor::Red)),
            command: fg(grey(238)),

            md_h1: fg(ansi(AnsiColor::Black)).effects(Effects::BOLD),
            md_h2: Style::new().effects(Effects::BOLD),
            md_h3: Style::new().effects(Effects::BOLD),
            md_h6: Style::new().effects(Effects::DIMMED.insert(Effects::ITALIC)),
            md_code: fg(rgb(0x0e, 0x74, 0x90)).effects(Effects::BOLD),
            md_code_block: fg(rgb(0x0e, 0x74, 0x90)),
            md_link: fg(rgb(0x1d, 0x4e, 0xd8)).effects(Effects::UNDERLINE),
            md_link_url: fg(rgb(0x25, 0x63, 0xeb)).effects(Effects::UNDERLINE),
            md_rule: fg(grey(247)),
            md_quote: fg(grey(243)),

            diff: DiffColors {
                add_bg: Style::new().bg_color(Some(rgb(0xda, 0xfb, 0xe1))),
                del_bg: Style::new().bg_color(Some(rgb(0xff, 0xeb, 0xe9))),
                add_hl: Style::new().bg_color(Some(rgb(0xaf, 0xf5, 0xb4))),
                del_hl: Style::new().bg_color(Some(rgb(0xff, 0xc1, 0xc0))),
            },
            diff_add_marker: fg(rgb(0x16, 0x65, 0x34)),
            diff_del_marker: fg(rgb(0xdc, 0x26, 0x26)),
            diff_add_head: fg(rgb(0x16, 0x65, 0x34)).effects(Effects::BOLD),
            diff_del_head: fg(rgb(0xdc, 0x26, 0x26)).effects(Effects::BOLD),

            separator: fg(rgb(0xd1, 0xd5, 0xdb)),
            placeholder: fg(rgb(0x6b, 0x72, 0x80)).effects(Effects::ITALIC),
            cwd: fg(rgb(0x6b, 0x72, 0x80)),
            bg_tasks: fg(rgb(0x4b, 0x55, 0x63)),
            tip: fg(rgb(0x9c, 0xa3, 0xaf)),
            badge_yolo: fg(rgb(0xb4, 0x53, 0x09)).effects(Effects::BOLD),
            badge_afk: fg(rgb(0xc2, 0x41, 0x0c)).effects(Effects::BOLD),
            badge_plan: fg(rgb(0x25, 0x63, 0xeb)).effects(Effects::BOLD),
            plan_rule: fg(rgb(0x25, 0x63, 0xeb)),

            menu_marker: fg(rgb(0x9c, 0xa3, 0xaf)),
            menu_marker_current: fg(rgb(0x25, 0x63, 0xeb)),
            menu_command: fg(rgb(0x4b, 0x55, 0x63)),
            menu_command_current: fg(rgb(0x1d, 0x4e, 0xd8)).effects(Effects::BOLD),
            menu_meta: fg(rgb(0x6b, 0x72, 0x80)),
            menu_meta_current: fg(rgb(0x25, 0x63, 0xeb)),
        }
    }

    pub const fn for_mode(mode: Mode) -> Self {
        match mode {
            Mode::Dark => Self::dark(),
            Mode::Light => Self::light(),
        }
    }

    /// The style a notification of this level is drawn in.
    pub fn severity(&self, level: Severity) -> Style {
        match level {
            Severity::Info => self.active,
            Severity::Success => self.ok,
            Severity::Warning => self.warn,
            Severity::Error => self.error,
        }
    }
}

/// The four levels a notification can carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Info,
    Success,
    Warning,
    Error,
}

/// Glyphs. One table so the character set is auditable — a renderer that invents
/// its own bullet breaks the visual grammar without anyone noticing.
pub mod glyph {
    /// The universal bullet: the head of an assistant block, a tool line, a
    /// notification. Its COLOUR carries the state, not its shape, so a reader
    /// scanning the left gutter sees one column rather than a symbol zoo.
    pub const BULLET: &str = "•";
    /// Stands in for the bullet on continuation blocks, keeping the indent.
    pub const BULLET_GAP: &str = " ";
    /// Echo of what the user submitted.
    pub const USER: &str = "✨";
    /// The shell-mode prompt.
    pub const SHELL: &str = "$";
    /// A queued message waiting for the current turn to end.
    pub const QUEUED: &str = "❯";
    /// Solid and hollow: an on/off indicator (thinking) and a tab marker.
    pub const DOT_ON: &str = "●";
    pub const DOT_OFF: &str = "○";
    /// The selected row of the slash menu.
    pub const MENU_CURRENT: &str = "›";
    /// The selected row of a chooser, before its number.
    pub const CHOICE_CURRENT: &str = "→";
    /// Checked and unchecked, for multi-select.
    pub const CHECKED: &str = "✓";
    pub const UNCHECKED: &str = " ";
    /// Background task count prefix.
    pub const TASKS: &str = "⚙";
    /// Git worktree state.
    pub const GIT_DIRTY: &str = "±";
    pub const GIT_AHEAD: &str = "↑";
    pub const GIT_BEHIND: &str = "↓";
    /// The left rail of a markdown block quote.
    pub const QUOTE_RAIL: &str = "▌";
    /// Heading underlines: heavy for h1, light for the rest.
    pub const RULE_HEAVY: &str = "═";
    pub const RULE_LIGHT: &str = "─";
    /// The dashed rule that marks plan mode.
    pub const RULE_DASHED: &str = "╌";
    /// A skipped stretch of a diff.
    pub const DIFF_GAP: &str = "⋮";
    /// Rounded box drawing, for every panel.
    pub const BOX_TOP_LEFT: &str = "╭";
    pub const BOX_TOP_RIGHT: &str = "╮";
    pub const BOX_BOTTOM_LEFT: &str = "╰";
    pub const BOX_BOTTOM_RIGHT: &str = "╯";
    pub const BOX_HORIZONTAL: &str = "─";
    pub const BOX_VERTICAL: &str = "│";
    /// Elision. The single character is for tight one-line fields where three
    /// dots would cost two more columns; the three dots are for content.
    pub const ELLIPSIS_TIGHT: &str = "…";
    pub const ELLIPSIS: &str = "...";
}

/// Where content gets cut. Every limit is here rather than at its use site so
/// that "why did this get truncated" has one answer to look up.
pub mod limit {
    /// A tool's key argument, elided in the MIDDLE: the tail of a path or a
    /// command is usually what distinguishes it, so cutting only the end is
    /// worse than cutting the centre.
    pub const TOOL_ARGUMENT: usize = 50;
    /// A one-line preview of a JSON payload or status string.
    pub const PREVIEW: usize = 160;
    /// Lines of live reasoning kept on screen while it streams.
    pub const REASONING_PREVIEW_LINES: usize = 6;
    /// Lines of a notification body.
    pub const NOTIFICATION_LINES: usize = 2;
    /// Changed lines shown in a diff preview before it collapses.
    pub const DIFF_PREVIEW_LINES: usize = 6;
    /// Lines of an approval payload shown before it collapses.
    pub const APPROVAL_PREVIEW_LINES: usize = 4;
    /// Rows of the slash menu.
    pub const MENU_ROWS: usize = 10;
    /// Description lines allowed on the menu's selected row.
    pub const MENU_META_LINES: usize = 3;
    /// The command column's bounds inside the slash menu.
    pub const MENU_COMMAND_MIN: usize = 18;
    pub const MENU_COMMAND_MAX: usize = 28;
    /// Toolbar fields. `cwd` is cut on the LEFT because its tail — the directory
    /// you are actually in — is the part worth keeping.
    pub const TOOLBAR_CWD: usize = 30;
    pub const TOOLBAR_BRANCH: usize = 22;
}

/// Cadence. Values in milliseconds, so a caller converting to `Duration` does
/// not have to guess the unit.
pub mod tick {
    /// The live region's repaint interval.
    pub const LIVE_MS: u64 = 100;
    /// The input area's repaint interval while a turn runs, and while idle.
    pub const SHELL_RUNNING_MS: u64 = 100;
    pub const SHELL_IDLE_MS: u64 = 1_000;
    /// How long a key hint stays before the next one.
    pub const TIP_ROTATE_MS: u64 = 30_000;
    /// Git is shelled out to, so its answers are cached this long.
    pub const GIT_BRANCH_TTL_MS: u64 = 5_000;
    pub const GIT_STATUS_TTL_MS: u64 = 15_000;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn theme_names_parse_and_everything_else_is_rejected() {
        assert_eq!(Mode::parse("dark"), Some(Mode::Dark));
        assert_eq!(Mode::parse("LIGHT"), Some(Mode::Light));
        assert_eq!(Mode::parse("  Dark  "), Some(Mode::Dark));
        assert_eq!(Mode::parse("solarized"), None);
        assert_eq!(Mode::parse(""), None);
    }

    /// Body text must stay unstyled in both palettes. Setting a foreground for
    /// it overrides the user's own terminal colours for the bulk of the output,
    /// which is the one thing a palette must not do.
    #[test]
    fn body_text_is_never_painted() {
        assert_eq!(Palette::dark().body, Style::new());
        assert_eq!(Palette::light().body, Style::new());
    }

    /// The two palettes are distinct where it matters. A copy-paste that left
    /// the light mode carrying dark backgrounds would be invisible in review.
    #[test]
    fn the_two_palettes_differ_where_the_background_matters() {
        let dark = Palette::dark();
        let light = Palette::light();
        assert_ne!(dark.diff.add_bg, light.diff.add_bg);
        assert_ne!(dark.diff.del_bg, light.diff.del_bg);
        assert_ne!(dark.separator, light.separator);
        assert_ne!(dark.cwd, light.cwd);
        assert_ne!(dark.menu_command_current, light.menu_command_current);
    }

    #[test]
    fn severity_maps_to_the_four_accents() {
        let palette = Palette::dark();
        assert_eq!(palette.severity(Severity::Info), palette.active);
        assert_eq!(palette.severity(Severity::Success), palette.ok);
        assert_eq!(palette.severity(Severity::Warning), palette.warn);
        assert_eq!(palette.severity(Severity::Error), palette.error);
    }

    /// The bullet gap has to occupy exactly the same width as the bullet, or
    /// continuation blocks drift out of alignment with the first one.
    #[test]
    fn the_bullet_gap_matches_the_bullet_width() {
        use unicode_width::UnicodeWidthStr;
        assert_eq!(
            glyph::BULLET.width(),
            glyph::BULLET_GAP.width(),
            "a continuation block must line up under the first block's text"
        );
    }
}
