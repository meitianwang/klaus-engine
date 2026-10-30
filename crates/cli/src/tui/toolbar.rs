//! The status area under the input: three lines that answer "where am I".
//!
//! ## Why three lines and why these fields
//!
//! A session's state is not visible from its transcript. Which model is
//! answering, which permission mode is in force, which directory the tools run
//! in, how much context is left — all of it changes what the next turn will do,
//! and none of it appears in the text. Putting it under the input means it is
//! read at the moment a prompt is being written, which is when it matters.
//!
//! The split: a rule to separate the area from the transcript, a line of session
//! facts, and a line for transient notices plus the context gauge.
//!
//! ## Degrading by dropping fields, not by wrapping
//!
//! A terminal can be 60 columns wide. Wrapping the toolbar would push the
//! transcript up on every repaint and make the screen jump. So each field is
//! given a priority and the line drops the least useful ones until it fits: the
//! key hint goes first, then git, then the directory, and the mode field itself
//! degrades through three progressively shorter forms before disappearing.

use anstyle::Style;

use super::paint::{self, Painter};
use super::theme::{glyph, limit};

/// Which extra behaviours are on. Each shows as a badge, because each changes
/// what happens without being asked again.
#[derive(Debug, Clone, Copy, Default)]
pub struct Badges {
    /// Every request auto-approved.
    pub auto_approve: bool,
    /// Plan mode: the model plans instead of acting.
    pub plan: bool,
    /// Unattended: the session keeps going without a person watching.
    pub unattended: bool,
}

/// The working tree's state, when the session is inside one.
#[derive(Debug, Clone, Default)]
pub struct Git {
    pub branch: String,
    pub dirty: bool,
    pub ahead: u32,
    pub behind: u32,
}

impl Git {
    /// `main [± ↑2]` — branch plus the markers that apply.
    fn label(&self) -> String {
        let mut marks: Vec<String> = Vec::new();
        if self.dirty {
            marks.push(glyph::GIT_DIRTY.to_string());
        }
        if self.ahead > 0 {
            marks.push(format!("{}{}", glyph::GIT_AHEAD, self.ahead));
        }
        if self.behind > 0 {
            marks.push(format!("{}{}", glyph::GIT_BEHIND, self.behind));
        }
        let branch = paint::truncate_right(&self.branch, limit::TOOLBAR_BRANCH);
        if marks.is_empty() {
            branch
        } else {
            format!("{branch} [{}]", marks.join(" "))
        }
    }
}

/// Everything the toolbar draws.
#[derive(Debug, Clone, Default)]
pub struct Status {
    pub mode: String,
    pub model: String,
    /// Whether extended reasoning is on, shown as a filled or hollow dot.
    pub thinking: bool,
    pub cwd: String,
    pub git: Option<Git>,
    pub badges: Badges,
    /// Background work, as `(kind, count)`.
    pub tasks: Option<(String, usize)>,
    /// Context window occupancy, 0.0..=1.0, with the token counts behind it.
    pub context: Option<(f64, u64, u64)>,
    /// A transient message: the result of a slash command, a warning.
    pub notice: Option<String>,
    /// The rotating key hint.
    pub tip: Option<String>,
}

/// The key hints, rotated so the set stays discoverable without a help screen
/// that nobody opens.
pub const TIPS: &[&str] = &[
    "ctrl-j: newline",
    "/help: commands",
    "ctrl-w: delete word",
    "↑: history",
    "ctrl-c: interrupt",
];

/// Which hint to show, by wall clock, so it changes without needing a repaint
/// counter and stays in step with any other rotation on screen.
pub fn tip_for(elapsed_ms: u128) -> &'static str {
    let step = (elapsed_ms / u128::from(super::theme::tick::TIP_ROTATE_MS)) as usize;
    TIPS[step % TIPS.len()]
}

/// `context: 42% (12k/32k)` — occupancy first, because that is the number that
/// decides whether the next long paste will fit.
pub fn context_label(fraction: f64, used: u64, total: u64) -> String {
    let percent = (fraction.clamp(0.0, 1.0) * 100.0).round() as u32;
    if total == 0 {
        return format!("context: {percent}%");
    }
    format!(
        "context: {percent}% ({}/{})",
        super::spinner::count_label(used),
        super::spinner::count_label(total)
    )
}

/// Render the three lines. Index 0 is the rule, 1 the facts, 2 the notice line.
pub fn render(painter: &Painter, status: &Status, columns: usize) -> Vec<String> {
    let palette = painter.palette();
    vec![
        painter.rule(palette.separator, glyph::RULE_LIGHT, columns),
        facts_line(painter, status, columns),
        notice_line(painter, status, columns),
    ]
}

/// The session facts, dropped by priority until they fit.
///
/// Keep-priority, highest first: badges, mode, directory, git, hint. Badges lead
/// because each one means the session acts without asking again; the mode is
/// next because a reader must never have to guess which permission mode is in
/// force. The candidates below are an ORDERED list rather than a nested search:
/// a product of flags does not express "drop the hint before the directory", and
/// getting that backwards silently kept the hint while throwing away the path.
fn facts_line(painter: &Painter, status: &Status, columns: usize) -> String {
    let palette = painter.palette();
    let dot = if status.thinking {
        glyph::DOT_ON
    } else {
        glyph::DOT_OFF
    };

    let mut badges: Vec<(Style, String)> = Vec::new();
    if status.badges.auto_approve {
        badges.push((palette.badge_yolo, "auto".to_string()));
    }
    if status.badges.plan {
        badges.push((palette.badge_plan, "plan".to_string()));
    }
    if status.badges.unattended {
        badges.push((palette.badge_afk, "afk".to_string()));
    }

    let cwd = paint::truncate_left(&paint::shorten_home(&status.cwd), limit::TOOLBAR_CWD);
    let git = status.git.as_ref().map(Git::label);
    let tasks = status
        .tasks
        .as_ref()
        .map(|(kind, count)| format!("{} {kind}: {count}", glyph::TASKS));

    // The mode shrinks through three forms before anything else is considered
    // lost, so "which mode" survives even on a very narrow terminal. The widest
    // form names the model, and is only a candidate when there IS a model to
    // name: interpolating an empty one drew `default ( ○)`, a parenthesis around
    // nothing, which reads as a broken renderer rather than as "no model set".
    let mut modes = Vec::with_capacity(3);
    if !status.model.is_empty() {
        modes.push(format!("{} ({} {dot})", status.mode, status.model));
    }
    modes.push(format!("{} {dot}", status.mode));
    modes.push(status.mode.clone());
    // What each stage still shows, in the order they are tried.
    let stages = [
        (true, true, true),
        (true, true, false),
        (true, false, false),
        (false, false, false),
    ];

    for mode in &modes {
        for (keep_cwd, keep_git, keep_tip) in stages {
            let mut spans: Vec<(Style, String)> = badges.clone();
            spans.push((palette.body, mode.clone()));
            if keep_cwd && !cwd.is_empty() {
                spans.push((palette.cwd, cwd.clone()));
            }
            if keep_git {
                if let Some(git) = &git {
                    spans.push((palette.meta, git.clone()));
                }
            }
            // Background work is not droppable: an unattended task the reader
            // cannot see is the thing they most need to know is running.
            if let Some(tasks) = &tasks {
                spans.push((palette.bg_tasks, tasks.clone()));
            }
            if keep_tip {
                if let Some(tip) = &status.tip {
                    spans.push((palette.tip, tip.clone()));
                }
            }
            let plain: Vec<&str> = spans.iter().map(|(_, text)| text.as_str()).collect();
            if paint::width(&plain.join("  ")) <= columns {
                return join(painter, &spans);
            }
        }
    }

    // Nothing fits even at the shortest form. Badges plus the mode, cut to the
    // width: a truncated fact beats a wrapped line that shifts the whole screen.
    let mut spans = badges;
    spans.push((palette.body, status.mode.clone()));
    let plain: Vec<&str> = spans.iter().map(|(_, text)| text.as_str()).collect();
    painter.paint(
        palette.body,
        &paint::truncate_right(&plain.join("  "), columns),
    )
}

/// The notice on the left, the context gauge right-aligned.
fn notice_line(painter: &Painter, status: &Status, columns: usize) -> String {
    let palette = painter.palette();
    // The gauge is protected first — a notice is gone on the next repaint anyway
    // — but it is not exempt: on a terminal too narrow for it alone it has to be
    // cut too, or this line overruns the width and the screen jumps.
    let context = status
        .context
        .map(|(fraction, used, total)| context_label(fraction, used, total))
        .map(|label| paint::truncate_right(&label, columns))
        .unwrap_or_default();
    let notice = status.notice.clone().unwrap_or_default();

    // One column of separation, so a notice never butts against the gauge.
    let room = columns
        .saturating_sub(paint::width(&context))
        .saturating_sub(1);
    let notice = paint::truncate_right(&notice, room);
    let gap = columns
        .saturating_sub(paint::width(&notice))
        .saturating_sub(paint::width(&context));
    format!(
        "{}{}{}",
        painter.paint(palette.meta, &notice),
        " ".repeat(gap),
        painter.paint(palette.meta, &context)
    )
}

/// Join spans with two spaces, which is wide enough to read as separate fields
/// without needing a separator character.
fn join(painter: &Painter, spans: &[(Style, String)]) -> String {
    spans
        .iter()
        .map(|(style, text)| painter.paint(*style, text))
        .collect::<Vec<String>>()
        .join("  ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::panel::visible_width;

    fn plain() -> Painter {
        Painter::plain()
    }

    fn status() -> Status {
        Status {
            mode: "ask".into(),
            model: "sonnet".into(),
            thinking: true,
            cwd: "/work/klaus".into(),
            git: Some(Git {
                branch: "main".into(),
                dirty: true,
                ahead: 2,
                behind: 0,
            }),
            badges: Badges::default(),
            tasks: None,
            context: Some((0.42, 12_000, 32_000)),
            notice: None,
            tip: Some("ctrl-j: newline".into()),
        }
    }

    #[test]
    fn a_wide_terminal_shows_every_field() {
        let lines = render(&plain(), &status(), 120);
        assert_eq!(lines.len(), 3);
        assert_eq!(paint::width(&lines[0]), 120, "the rule fills the width");
        assert!(lines[1].contains("ask (sonnet ●)"), "{:?}", lines[1]);
        assert!(lines[1].contains("/work/klaus"), "{:?}", lines[1]);
        assert!(lines[1].contains("main [± ↑2]"), "{:?}", lines[1]);
        assert!(lines[1].contains("ctrl-j"), "{:?}", lines[1]);
        assert!(
            lines[2].contains("context: 42% (12k/32k)"),
            "{:?}",
            lines[2]
        );
    }

    /// No line may exceed the width at ANY width: a wrapped toolbar pushes the
    /// transcript up on every repaint and the screen jumps.
    #[test]
    fn no_line_ever_exceeds_the_terminal_width() {
        for columns in [10, 20, 30, 40, 60, 80, 100, 200] {
            for line in render(&plain(), &status(), columns) {
                assert!(
                    visible_width(&line) <= columns,
                    "at {columns} columns a line was {}: {line:?}",
                    visible_width(&line)
                );
            }
        }
    }

    /// Fields drop in priority order: hint, then git, then cwd.
    #[test]
    fn fields_drop_from_least_useful_first() {
        let wide = render(&plain(), &status(), 120)[1].clone();
        assert!(wide.contains("ctrl-j") && wide.contains("main") && wide.contains("/work/klaus"));

        let medium = render(&plain(), &status(), 34)[1].clone();
        assert!(
            !medium.contains("ctrl-j"),
            "the hint goes first: {medium:?}"
        );
        assert!(
            medium.contains("ask"),
            "the mode always survives: {medium:?}"
        );

        let narrow = render(&plain(), &status(), 12)[1].clone();
        assert!(
            narrow.contains("ask"),
            "the mode survives to the end: {narrow:?}"
        );
    }

    /// The mode field shrinks through its forms rather than disappearing: which
    /// permission mode is in force is the fact a reader must not have to guess.
    #[test]
    fn the_mode_shrinks_but_never_vanishes() {
        for columns in [8, 12, 16, 24, 40] {
            let line = render(&plain(), &status(), columns)[1].clone();
            assert!(
                line.contains("ask"),
                "at {columns} columns the mode was lost: {line:?}"
            );
        }
    }

    /// Badges are never dropped: each means the session will act unprompted.
    #[test]
    fn badges_survive_every_width() {
        let mut with_badges = status();
        with_badges.badges = Badges {
            auto_approve: true,
            plan: true,
            unattended: true,
        };
        for columns in [12, 20, 40, 80] {
            let line = render(&plain(), &with_badges, columns)[1].clone();
            assert!(line.contains("auto"), "at {columns}: {line:?}");
        }
    }

    #[test]
    fn the_thinking_dot_reflects_the_setting() {
        let mut off = status();
        off.thinking = false;
        assert!(render(&plain(), &off, 120)[1].contains("ask (sonnet ○)"));
        assert!(render(&plain(), &status(), 120)[1].contains("ask (sonnet ●)"));
    }

    /// With no model to name, the field is the mode and the dot — never an empty
    /// parenthesis. `default ( ○)` was what a session started without `--model`
    /// actually drew, and it reads as a broken renderer rather than as a fact.
    #[test]
    fn an_unknown_model_leaves_no_empty_parenthesis() {
        let mut unknown = status();
        unknown.model = String::new();
        for columns in [8, 24, 60, 120] {
            let line = render(&plain(), &unknown, columns)[1].clone();
            assert!(
                !line.contains('(') && !line.contains(')'),
                "at {columns} columns an empty model still drew brackets: {line:?}"
            );
            assert!(
                line.contains("ask"),
                "at {columns} the mode was lost: {line:?}"
            );
        }
    }

    #[test]
    fn git_shows_only_the_markers_that_apply() {
        let clean = Git {
            branch: "main".into(),
            dirty: false,
            ahead: 0,
            behind: 0,
        };
        assert_eq!(clean.label(), "main");
        let behind = Git {
            branch: "main".into(),
            dirty: false,
            ahead: 0,
            behind: 3,
        };
        assert_eq!(behind.label(), "main [↓3]");
        let both = Git {
            branch: "main".into(),
            dirty: true,
            ahead: 1,
            behind: 2,
        };
        assert_eq!(both.label(), "main [± ↑1 ↓2]");
    }

    #[test]
    fn a_long_branch_is_cut_to_its_budget() {
        let git = Git {
            branch: "a".repeat(60),
            dirty: false,
            ahead: 0,
            behind: 0,
        };
        assert!(paint::width(&git.label()) <= limit::TOOLBAR_BRANCH);
    }

    /// The path's tail is what says where you are, so the cut takes the head.
    #[test]
    fn a_long_cwd_keeps_its_tail() {
        let mut deep = status();
        deep.cwd = "/very/long/path/that/goes/on/forever/and/ever/src".into();
        let line = render(&plain(), &deep, 120)[1].clone();
        assert!(line.contains("/src"), "the tail must survive: {line:?}");
        assert!(
            line.contains(glyph::ELLIPSIS_TIGHT),
            "the cut must be marked: {line:?}"
        );
    }

    #[test]
    fn the_context_gauge_is_right_aligned_and_protected() {
        let mut noisy = status();
        noisy.notice = Some("a rather long transient message about something".into());
        let line = render(&plain(), &noisy, 60)[2].clone();
        assert!(paint::width(&line) <= 60);
        assert!(line.ends_with("context: 42% (12k/32k)"), "{line:?}");
        assert!(
            line.contains(glyph::ELLIPSIS_TIGHT),
            "the notice is what gets cut: {line:?}"
        );
    }

    #[test]
    fn context_reads_as_a_percentage_with_the_counts_behind_it() {
        assert_eq!(
            context_label(0.42, 12_000, 32_000),
            "context: 42% (12k/32k)"
        );
        assert_eq!(context_label(0.0, 0, 0), "context: 0%");
        // Out-of-range input is clamped rather than printed as 137%.
        assert_eq!(context_label(1.4, 5, 10), "context: 100% (5/10)");
    }

    #[test]
    fn tips_rotate_on_the_clock_and_wrap() {
        assert_eq!(tip_for(0), TIPS[0]);
        assert_eq!(
            tip_for(u128::from(super::super::theme::tick::TIP_ROTATE_MS)),
            TIPS[1]
        );
        let full_cycle = u128::from(super::super::theme::tick::TIP_ROTATE_MS) * TIPS.len() as u128;
        assert_eq!(tip_for(full_cycle), TIPS[0]);
    }

    #[test]
    fn an_empty_status_still_renders_three_well_formed_lines() {
        let lines = render(&plain(), &Status::default(), 40);
        assert_eq!(lines.len(), 3);
        for line in &lines {
            assert!(visible_width(line) <= 40, "{line:?}");
        }
    }
}
