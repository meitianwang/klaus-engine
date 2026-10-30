//! Protocol events, as terminal text.
//!
//! This is the only place that decides what a turn LOOKS like, so the transport
//! and command layers never carry formatting. Every function is pure: it takes a
//! borrowed protocol value plus a [`Painter`] and returns what to write. Nothing
//! here touches the client, so nothing here can change engine state.
//!
//! The engine emits the same content twice on purpose — once as a delta while
//! the model streams, again as a completed item — because a host that only wants
//! the settled view can ignore the deltas. A terminal wants the opposite, so the
//! rules below pick exactly ONE of the two for each kind of content and drop the
//! other. Getting that wrong is not cosmetic: it doubles every assistant reply.
//!
//! ## The left gutter carries state by colour
//!
//! Settled lines start with `•`, and its COLOUR says what happened: green for a
//! tool that succeeded, red for one that failed, grey for reasoning, the severity
//! accent for a notification. One column to scan instead of a zoo of symbols, and
//! the shapes stay free for the few things that genuinely differ.
//!
//! Assistant text is the exception: it streams, so it has no settled moment at
//! which a prefix could be attached. Whoever owns the live region adds the bullet
//! there, because only it knows where a block begins.

use klaus_agent_core::extensions::failure::ExtensionFailureKind;
use klaus_agent_core::runtime_values::{RuntimeEncode, RuntimeJson, RuntimeString};
use klaus_agent_core::schemas::content::{ContentBlock, OutputBlock};
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::types::errors::TerminalReason;
use klaus_agent_core::types::events::{
    CompactionMode, EngineEvent, FileChange, FileChangeKind, FileChangeStatus, ItemStatus,
    NotificationLevel, ThreadItem, ToolDomainEvent,
};
use klaus_agent_core::types::permission::{
    ApprovalKind, ApprovalRequest, PermissionDestination, PermissionUpdate,
};
use klaus_agent_protocol::{ElicitRequestWire, RunResult};

use crate::error::CliResult;
use crate::output::Printer;
use crate::text;
use crate::tui::paint::{self, Painter};
use crate::tui::theme::{glyph, limit, Severity};
use crate::tui::{markdown, panel};

/// How long a single-line preview of a JSON payload or a status string may get
/// before it is elided. Long tool arguments are the common case and they push
/// the interesting part (which tool, which file) off screen.
const PREVIEW_LIMIT: usize = limit::PREVIEW;

/// What to write for one event.
///
/// [`Rendered::Stream`] is deliberately distinct from [`Rendered::Line`]: model
/// text and command output arrive in fragments that must NOT each get a
/// newline, while everything else is a settled fact that owns its own line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rendered {
    /// Write verbatim, no newline. Part of an in-progress line.
    Stream(String),
    /// Write followed by a newline.
    Line(String),
    /// Deliberately not shown in the human view.
    Nothing,
}

impl Rendered {
    pub fn emit(&self, printer: &Printer) -> CliResult<()> {
        match self {
            Self::Stream(chunk) => printer.stream(chunk),
            Self::Line(line) => printer.line(line),
            Self::Nothing => Ok(()),
        }
    }

    /// Whether this left the cursor mid-line, so the caller knows it owes a
    /// newline before printing anything of its own.
    pub fn is_partial(&self) -> bool {
        matches!(self, Self::Stream(_))
    }
}

/// A settled line under the bullet gutter: `• <text>`, the bullet in `style`.
fn bullet(painter: &Painter, style: anstyle::Style, text: &str) -> String {
    format!("{} {text}", painter.paint(style, glyph::BULLET))
}

/// A continuation line, aligned under a bullet's text rather than under the
/// bullet itself.
fn under_bullet(text: &str) -> String {
    format!("{} {text}", glyph::BULLET_GAP)
}

/// One engine event as terminal text.
pub fn engine_event(painter: &Painter, columns: usize, event: &EngineEvent) -> Rendered {
    let palette = painter.palette();
    match event {
        // Streamed assistant text is the live view; the matching
        // `item.completed{agent_message}` is dropped below.
        EngineEvent::TextDelta { text, .. } => Rendered::Stream(text::text(text)),
        EngineEvent::PlanDelta { .. } | EngineEvent::PlanUpdated { .. } => Rendered::Nothing,
        // Reasoning is shown once, as a block, when it settles. Streaming it
        // interleaves with the answer text and reads as noise.
        EngineEvent::ThinkingDelta { .. } => Rendered::Nothing,
        EngineEvent::QuestionRequested { question } => Rendered::Line(bullet(
            painter,
            palette.meta,
            &format!(
                "Optional question: {} (answer from the desktop while this turn runs)",
                paint::shorten_middle(&text::text(&question.request.prompt), PREVIEW_LIMIT)
            ),
        )),
        EngineEvent::QuestionResolved { question_resolved } => Rendered::Line(bullet(
            painter,
            palette.meta,
            &format!("Optional question {}", text::text(&question_resolved.state)),
        )),
        EngineEvent::ItemStarted { item, .. } => item_started(painter, item),
        // An in-flight item's only interesting updates arrive as tool events,
        // which stream separately.
        EngineEvent::ItemUpdated { .. } => Rendered::Nothing,
        EngineEvent::ItemCompleted { item, .. } => item_completed(painter, columns, item),
        EngineEvent::ToolEvent { event, .. } => tool_event(painter, event),
        EngineEvent::TurnCompleted {
            usage, stop_reason, ..
        } => Rendered::Line(turn_completed(painter, *stop_reason, usage.as_ref())),
        EngineEvent::TurnFailed { error, .. } => Rendered::Line(bullet(
            painter,
            palette.bad,
            &painter.paint(palette.error, &text::text(&error.message)),
        )),
        EngineEvent::PermissionUpdate {
            destination,
            update,
        } => Rendered::Line(permission_update(painter, *destination, update)),
        EngineEvent::CompactionProgress { .. } => Rendered::Nothing,
        EngineEvent::Compaction { mode, freed_tokens } => Rendered::Line(bullet(
            painter,
            palette.meta,
            &painter.paint(
                palette.meta,
                &format!(
                    "compacted ({} run, freed {freed_tokens} tokens)",
                    compaction_mode(*mode)
                ),
            ),
        )),
        EngineEvent::ApprovalReview { review } => {
            use klaus_agent_core::types::approval_review::ApprovalReviewStatus;
            match review.status {
                ApprovalReviewStatus::Approved | ApprovalReviewStatus::Aborted => Rendered::Nothing,
                ApprovalReviewStatus::InProgress => {
                    Rendered::Line(bullet(painter, palette.meta, "Reviewing approval request"))
                }
                ApprovalReviewStatus::Denied | ApprovalReviewStatus::TimedOut => {
                    let label = if review.status == ApprovalReviewStatus::TimedOut {
                        "Approval review timed out"
                    } else {
                        "Approval review denied"
                    };
                    let message = review
                        .rationale
                        .as_ref()
                        .map(text::text)
                        .unwrap_or_else(|| label.to_string());
                    Rendered::Line(bullet(
                        painter,
                        palette.warn,
                        &painter.paint(palette.warn, &message),
                    ))
                }
            }
        }
        EngineEvent::ApprovalReviewStrictRequired { .. } => Rendered::Line(bullet(
            painter,
            palette.meta,
            "This request requires additional safety checks, some tool calls might take extra time",
        )),
        EngineEvent::ApprovalReviewWarning { message, .. } => {
            if message.starts_with("Automatic approval review approved (") {
                Rendered::Nothing
            } else {
                Rendered::Line(bullet(
                    painter,
                    palette.warn,
                    &painter.paint(palette.warn, &text::text(message)),
                ))
            }
        }
        EngineEvent::Notification { level, message } => {
            let severity = match level {
                NotificationLevel::Info => Severity::Info,
                NotificationLevel::Warn => Severity::Warning,
            };
            let style = palette.severity(severity);
            Rendered::Line(bullet(
                painter,
                style,
                &painter.paint(style, &text::text(message)),
            ))
        }
        EngineEvent::InputRejected { message, .. } => Rendered::Line(bullet(
            painter,
            palette.warn,
            &painter.paint(palette.warn, &format!("rejected: {}", text::text(message))),
        )),
        // Lifecycle bookkeeping the caller already reports (or that carries no
        // user-facing content at all).
        EngineEvent::ThreadStarted { .. }
        | EngineEvent::TurnStarted { .. }
        | EngineEvent::StreamRequestStart { .. }
        | EngineEvent::ProviderDecision { .. }
        | EngineEvent::Tombstone { .. }
        | EngineEvent::InputApplied { .. } => Rendered::Nothing,
        // Approvals are a prompt, not a report: the turn loop has to answer them,
        // so it owns their presentation.
        EngineEvent::ApprovalRequested { .. } => Rendered::Nothing,
    }
}

/// Announce work that is about to take a while, so the terminal is not silent
/// while a command runs or a tool thinks. Content-bearing items say nothing here
/// because their content has not arrived yet.
///
/// The verb is present tense (`Using`) and becomes past tense on completion,
/// which is what tells a reader whether a line is still live without needing a
/// spinner to have been visible.
fn item_started(painter: &Painter, item: &ThreadItem) -> Rendered {
    let palette = painter.palette();
    match item {
        ThreadItem::CommandExecution { command, .. } => Rendered::Line(bullet(
            painter,
            palette.active,
            &painter.spans(&[
                (palette.meta, "$ "),
                (palette.command, &text::text(command)),
            ]),
        )),
        ThreadItem::ToolCall {
            tool_name,
            arguments,
            ..
        } => Rendered::Line(bullet(
            painter,
            palette.active,
            &tool_line(painter, "Using", tool_name, Some(arguments), palette.meta),
        )),
        _ => Rendered::Nothing,
    }
}

/// `Using toolName (argument)` — the tool's name in the accent, its key argument
/// in parentheses, elided in the MIDDLE so both ends survive.
fn tool_line(
    painter: &Painter,
    verb: &str,
    tool_name: &RuntimeString,
    arguments: Option<&RuntimeJson>,
    argument_style: anstyle::Style,
) -> String {
    let palette = painter.palette();
    let mut out = painter.spans(&[
        (palette.body, &format!("{verb} ")),
        (palette.tool, &text::text(tool_name)),
    ]);
    if let Some(arguments) = arguments {
        let rendered = text::json_line(arguments);
        if rendered != "{}" {
            let argument = paint::shorten_middle(&rendered, limit::TOOL_ARGUMENT);
            out.push_str(&painter.spans(&[
                (palette.meta, " ("),
                (argument_style, &argument),
                (palette.meta, ")"),
            ]));
        }
    }
    out
}

fn item_completed(painter: &Painter, columns: usize, item: &ThreadItem) -> Rendered {
    let palette = painter.palette();
    match item {
        // Already written by `text.delta`.
        ThreadItem::AgentMessage { .. } => Rendered::Nothing,
        ThreadItem::ProposedPlan { plan, .. } => Rendered::Line(text::text(&plan.content)),
        // Reasoning is grey AND italic: grey alone does not survive a terminal
        // that renders every dim colour the same. Only the first lines are kept
        // — a long chain of thought buries the answer it led to.
        ThreadItem::Reasoning { text: body, .. } => {
            let body = text::text(body);
            let mut lines: Vec<String> = Vec::new();
            let rendered =
                markdown::render(&painter.without_color(), &body, columns.saturating_sub(2));
            for (index, line) in rendered
                .iter()
                .take(limit::REASONING_PREVIEW_LINES)
                .enumerate()
            {
                let painted = painter.paint(palette.reasoning, line);
                lines.push(if index == 0 {
                    bullet(painter, palette.meta, &painted)
                } else {
                    under_bullet(&painted)
                });
            }
            let hidden = rendered
                .len()
                .saturating_sub(limit::REASONING_PREVIEW_LINES);
            if hidden > 0 {
                lines.push(under_bullet(&painter.paint(
                    palette.faint,
                    &format!("{} {hidden} more lines of reasoning", glyph::ELLIPSIS),
                )));
            }
            if lines.is_empty() {
                return Rendered::Nothing;
            }
            Rendered::Line(lines.join("\n"))
        }
        // The output already streamed through `tool.event{stdout,stderr}`, so
        // only the verdict is new here. Printing `aggregated_output` again would
        // duplicate every line of a long build log.
        ThreadItem::CommandExecution {
            exit_code, status, ..
        } => {
            let (style, verdict) = match (status, exit_code) {
                (ItemStatus::Completed, Some(0)) => (palette.ok, "ok".to_string()),
                (_, Some(code)) => (palette.bad, format!("exit {code}")),
                (ItemStatus::Failed, None) => (palette.bad, "failed".to_string()),
                (_, None) => (palette.meta, item_status(*status).to_string()),
            };
            Rendered::Line(bullet(
                painter,
                style,
                &painter.paint(palette.meta, &verdict),
            ))
        }
        ThreadItem::FileChange {
            changes, status, ..
        } => Rendered::Line(file_change_summary(painter, changes, *status)),
        ThreadItem::ToolCall {
            tool_name,
            error,
            status,
            ..
        } => Rendered::Line(match error {
            Some(error) => bullet(
                painter,
                palette.bad,
                &format!(
                    "{}{}",
                    tool_line(painter, "Used", tool_name, None, palette.meta),
                    painter.paint(palette.error, &format!(": {}", text::text(&error.message))),
                ),
            ),
            None => bullet(
                painter,
                if *status == ItemStatus::Failed {
                    palette.bad
                } else {
                    palette.ok
                },
                &tool_line(painter, "Used", tool_name, None, palette.meta),
            ),
        }),
        ThreadItem::Error { message, .. } => Rendered::Line(bullet(
            painter,
            palette.bad,
            &painter.paint(palette.error, &text::text(message)),
        )),
    }
}

fn tool_event(painter: &Painter, event: &ToolDomainEvent) -> Rendered {
    let palette = painter.palette();
    match event {
        // Live process output. This is why a long `bash` call is watchable at all.
        ToolDomainEvent::Stdout { chunk } | ToolDomainEvent::Stderr { chunk } => {
            Rendered::Stream(text::text(chunk))
        }
        ToolDomainEvent::Status { message } => Rendered::Line(under_bullet(
            &painter.paint(palette.meta, &preview(&text::text(message))),
        )),
        ToolDomainEvent::WebSearch {
            phase,
            query,
            result_count,
        } => {
            let body = match result_count {
                Some(count) => format!(
                    "search {}: {} ({count} results)",
                    text::text(phase),
                    text::text(query)
                ),
                None => format!("search {}: {}", text::text(phase), text::text(query)),
            };
            Rendered::Line(under_bullet(&painter.paint(palette.meta, &body)))
        }
        // Each edit is reported again on the settled `file_change` item, where
        // the whole set is summarised at once.
        ToolDomainEvent::FileChange { .. } => Rendered::Nothing,
        // A sub-agent's own event stream. Inlining it interleaves two turns'
        // worth of output with no way to tell them apart; the transcript is
        // available through `klaus subagent transcript`.
        ToolDomainEvent::AgentProgress { .. } => Rendered::Nothing,
        // Browser steps are a UI affordance for a windowed host, not a log.
        ToolDomainEvent::Browser { .. } => Rendered::Nothing,
        // Desktop hosts project this state edge into their Spec control. The CLI
        // has no separate mode badge, so the tool result remains sufficient.
        ToolDomainEvent::PlanModeChanged { .. } | ToolDomainEvent::PlanUpdated { .. } => {
            Rendered::Nothing
        }
    }
}

fn turn_completed(painter: &Painter, stop_reason: TerminalReason, usage: Option<&Usage>) -> String {
    let palette = painter.palette();
    let ok = stop_reason == TerminalReason::Completed;
    let mut body = painter.paint(
        if ok { palette.meta } else { palette.warn },
        terminal_reason(stop_reason),
    );
    if let Some(usage) = usage {
        body.push_str(&painter.paint(palette.meta, &format!(" · {}", usage_summary(usage))));
    }
    bullet(painter, if ok { palette.ok } else { palette.warn }, &body)
}

/// Token counts as one line. The four fields are disjoint under Anthropic and
/// summed by the engine's own accounting, so the total is reported the same way
/// the engine computes it rather than re-derived here.
pub fn usage_summary(usage: &Usage) -> String {
    let total = usage.total_tokens.unwrap_or(
        usage.input_tokens
            + usage.output_tokens
            + usage.cache_read_input_tokens
            + usage.cache_creation_input_tokens,
    );
    format!(
        "{total} tokens (in {} / out {} / cache read {} / cache write {})",
        usage.input_tokens,
        usage.output_tokens,
        usage.cache_read_input_tokens,
        usage.cache_creation_input_tokens
    )
}

/// The final `result` event of a run.
pub fn run_result(painter: &Painter, columns: usize, result: &RunResult) -> String {
    let mut lines = Vec::new();
    let text_body = text::text(&result.final_text);
    if !text_body.is_empty() {
        lines.push(markdown::render(painter, &text_body, columns).join("\n"));
    }
    if let Some(structured) = &result.structured_output {
        lines.push(text::json_pretty(structured));
    }
    lines.push(turn_completed(
        painter,
        result.stop_reason,
        result.usage.as_ref(),
    ));
    lines.join("\n")
}

/// The approval prompt body. The turn loop appends the question itself, since
/// only it knows whether an answer can be read from the terminal.
///
/// Boxed in the caution colour because this is the one thing on screen that stops
/// until the reader acts; a request that scrolls past like a log line gets missed.
pub fn approval_request(
    painter: &Painter,
    columns: usize,
    request: &ApprovalRequest,
) -> Vec<String> {
    let palette = painter.palette();
    let mut body = vec![painter.spans(&[
        (palette.body, &format!("{} ", approval_kind(request.kind))),
        (palette.tool, &text::text(&request.tool_name)),
    ])];
    let message = text::text(&request.message);
    if !message.is_empty() {
        body.push(painter.paint(palette.body, &message));
    }
    let arguments = text::json_line(&request.input);
    if arguments != "{}" {
        for line in arguments
            .lines()
            .take(limit::APPROVAL_PREVIEW_LINES)
            .map(|line| painter.paint(palette.meta, line))
        {
            body.push(line);
        }
        let hidden = arguments
            .lines()
            .count()
            .saturating_sub(limit::APPROVAL_PREVIEW_LINES);
        if hidden > 0 {
            body.push(painter.paint(palette.faint, &format!("{} {hidden} more", glyph::ELLIPSIS)));
        }
    }
    panel::Panel::new(palette.warn)
        .title("approval")
        .render(painter, &body, columns)
}

/// An engine-side elicitation prompt. Options are numbered so the answer can be
/// given as an index, which is the only thing that works when a choice contains
/// spaces.
pub fn elicitation(painter: &Painter, request: &ElicitRequestWire) -> String {
    let palette = painter.palette();
    let mut lines = vec![bullet(
        painter,
        palette.active,
        &painter.paint(palette.body, &text::text(&request.prompt)),
    )];
    if let Some(options) = &request.options {
        for (index, option) in options.iter().enumerate() {
            lines.push(under_bullet(&painter.spans(&[
                (palette.meta, &format!("[{}] ", index + 1)),
                (palette.body, &text::text(option)),
            ])));
        }
        if request.multi_select == Some(true) {
            lines.push(under_bullet(
                &painter.paint(palette.faint, "multiple allowed, comma-separated"),
            ));
        }
    }
    lines.join("\n")
}

fn file_change_summary(
    painter: &Painter,
    changes: &[FileChange],
    status: FileChangeStatus,
) -> String {
    let palette = painter.palette();
    let (bullet_style, verdict) = match status {
        FileChangeStatus::Completed => (palette.ok, "edited"),
        FileChangeStatus::Failed => (palette.bad, "edit failed"),
    };
    let paths = changes
        .iter()
        .map(|change| {
            let (kind_style, kind) = match change.kind {
                FileChangeKind::Create => (palette.diff_add_marker, "+"),
                FileChangeKind::Modify => (palette.meta, "~"),
                FileChangeKind::Delete => (palette.diff_del_marker, "-"),
            };
            painter.spans(&[
                (kind_style, kind),
                (palette.body, " "),
                (palette.body, &text::text(&change.path)),
            ])
        })
        .collect::<Vec<_>>()
        .join(", ");
    bullet(
        painter,
        bullet_style,
        &format!("{} {paths}", painter.paint(palette.meta, verdict)),
    )
}

fn permission_update(
    painter: &Painter,
    destination: PermissionDestination,
    update: &PermissionUpdate,
) -> String {
    let palette = painter.palette();
    let body = match update {
        PermissionUpdate::SetMode { mode, .. } => format!("mode {}", wire_tag(mode)),
        PermissionUpdate::AddRules { rules, .. } => format!("{} rule(s) added", rules.len()),
    };
    bullet(
        painter,
        palette.active,
        &painter.paint(
            palette.meta,
            &format!("{body} ({})", permission_destination(destination)),
        ),
    )
}

/// A unit-enum's own wire tag, as its label.
///
/// Reading the label off the encoding rather than a hand-written table means a
/// variant added upstream shows up under its real name here instead of being
/// silently mislabelled by a stale match arm.
pub fn wire_tag<T: RuntimeEncode>(value: &T) -> String {
    // Matched by reference: `RuntimeJson` implements `Drop`, so the tag cannot be
    // moved out of the variant.
    match &text::encode(value) {
        Ok(RuntimeJson::String(tag)) => text::text(tag),
        Ok(value) => text::json_line(value),
        Err(_) => "unknown".to_string(),
    }
}

/// A persisted transcript.
///
/// This is the settled history, not the live stream, so the rules that govern
/// [`engine_event`] do not apply: nothing was shown before, so every block is
/// shown once here — including the tool calls and results the streaming view
/// summarises. Reasoning is the exception, elided to a preview, because replaying
/// a long transcript's full reasoning buries the conversation in it.
pub fn history(messages: &[Message]) -> String {
    if messages.is_empty() {
        return "(no messages)".to_string();
    }
    messages.iter().map(message).collect::<Vec<_>>().join("\n")
}

/// One persisted message, under a role header.
pub fn message(message: &Message) -> String {
    match message {
        Message::User(body) => role(
            // A meta message is engine-injected, so labelling it plain "user"
            // would attribute the engine's own re-prompt to the person.
            if body.is_meta == Some(true) {
                "user (injected)"
            } else {
                "user"
            },
            content_blocks(&body.content),
        ),
        Message::Assistant(body) => role(
            &format!("assistant {}", text::text(&body.model)),
            content_blocks(&body.content),
        ),
        Message::ToolResult(body) => role(
            if body.is_error {
                "tool error"
            } else {
                "tool result"
            },
            body.content.iter().map(output_block).collect(),
        ),
        Message::System(body) => role(
            &format!("system {}", wire_tag(&body.subtype)),
            vec![text::text(&body.text)],
        ),
        Message::CompactBoundary(body) => role("compacted", vec![text::text(&body.summary)]),
    }
}

fn role(label: &str, body: Vec<String>) -> String {
    let mut lines = vec![format!("[{label}]")];
    for block in body {
        for line in block.lines() {
            lines.push(format!("  {line}"));
        }
    }
    lines.join("\n")
}

fn content_blocks(blocks: &[ContentBlock]) -> Vec<String> {
    blocks.iter().filter_map(content_block).collect()
}

/// One content block, or `None` when it carries nothing a reader can use.
fn content_block(block: &ContentBlock) -> Option<String> {
    match block {
        ContentBlock::Text(body) => {
            let body = text::text(&body.text);
            (!body.is_empty()).then_some(body)
        }
        ContentBlock::Thinking(body) => {
            Some(format!("[thinking] {}", preview(&text::text(&body.text))))
        }
        // Encrypted by the provider: there is no plaintext to show, only the
        // fact that reasoning happened here.
        ContentBlock::RedactedThinking(_) => Some("[thinking redacted]".to_string()),
        ContentBlock::Image(body) => Some(format!("[image {}]", text::text(&body.mime_type))),
        ContentBlock::Media(body) => Some(match &body.filename {
            Some(name) => format!(
                "[media {} {}]",
                text::text(&body.mime_type),
                text::text(name)
            ),
            None => format!("[media {}]", text::text(&body.mime_type)),
        }),
        ContentBlock::Document(body) => Some(match &body.filename {
            Some(name) => format!(
                "[document {} {}]",
                text::text(&body.mime_type),
                text::text(name)
            ),
            None => format!("[document {}]", text::text(&body.mime_type)),
        }),
        ContentBlock::ToolUse(body) => Some(format!(
            "[tool] {} {}",
            text::text(&body.name),
            preview(&text::json_line(&body.input))
        )),
        ContentBlock::ToolResult(body) => Some(
            body.content
                .iter()
                .map(output_block)
                .collect::<Vec<_>>()
                .join("\n"),
        ),
        ContentBlock::WebSearchCall(body) => Some(format!(
            "[search] {} ({} results)",
            text::text(&body.query),
            body.results.len()
        )),
    }
}

fn output_block(block: &OutputBlock) -> String {
    match block {
        OutputBlock::Text(body) => text::text(&body.text),
        OutputBlock::Image(body) => format!("[image {}]", text::text(&body.mime_type)),
    }
}

pub fn terminal_reason(reason: TerminalReason) -> &'static str {
    match reason {
        TerminalReason::Completed => "done",
        TerminalReason::Aborted => "aborted",
        TerminalReason::PromptTooLong => "prompt too long",
        TerminalReason::AuthFailed => "auth failed",
        TerminalReason::TurnLimitExhausted => "turn limit exhausted",
        TerminalReason::SpendLimitExhausted => "spend limit exhausted",
        TerminalReason::Error => "error",
    }
}

fn approval_kind(kind: ApprovalKind) -> &'static str {
    match kind {
        ApprovalKind::Tool => "tool",
        ApprovalKind::Exec => "exec",
        ApprovalKind::Patch => "patch",
        ApprovalKind::Network => "network",
    }
}

fn item_status(status: ItemStatus) -> &'static str {
    match status {
        ItemStatus::InProgress => "running",
        ItemStatus::Completed => "ok",
        ItemStatus::Failed => "failed",
    }
}

// `file_change_kind`, `todo_status` and `notification_level` used to turn these
// enums into bracketed words. The palette carries that meaning now — a created
// file is a green `+`, a finished todo is a struck-through line, a warning is the
// caution accent — so a second textual encoding of the same fact would only be a
// place for the two to disagree.

fn compaction_mode(mode: CompactionMode) -> &'static str {
    match mode {
        CompactionMode::Micro => "micro",
        CompactionMode::Full => "full",
    }
}

/// Written out by hand rather than through [`wire_tag`]: the kind's own wire
/// mapping is private to core, so it is not reachable through `RuntimeEncode`.
pub fn extension_failure_kind(kind: ExtensionFailureKind) -> &'static str {
    match kind {
        ExtensionFailureKind::Hook => "hook",
        ExtensionFailureKind::Plugin => "plugin",
        ExtensionFailureKind::Skill => "skill",
    }
}

fn permission_destination(destination: PermissionDestination) -> &'static str {
    match destination {
        PermissionDestination::Session => "session",
        PermissionDestination::LocalSettings => "local settings",
        PermissionDestination::ProjectSettings => "project settings",
        PermissionDestination::UserSettings => "user settings",
    }
}

/// One line, bounded. Newlines are folded because a preview that wraps stops
/// being a preview, and the cut is on a char boundary so multi-byte text
/// survives it.
fn preview(value: &str) -> String {
    let folded = value.replace(['\n', '\r'], " ");
    if folded.chars().count() <= PREVIEW_LIMIT {
        return folded;
    }
    let head: String = folded.chars().take(PREVIEW_LIMIT).collect();
    format!("{head}…")
}

/// Best-effort single-line label for a value that only exists as a runtime
/// string on the wire.
pub fn label(value: Option<&RuntimeString>, fallback: &str) -> String {
    text::text_or(value, fallback)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::theme::Palette;

    fn plain() -> Painter {
        Painter::plain()
    }

    fn styled() -> Painter {
        Painter::new(Palette::dark(), true)
    }

    fn text_of(rendered: &Rendered) -> String {
        match rendered {
            Rendered::Line(line) => line.clone(),
            Rendered::Stream(chunk) => chunk.clone(),
            Rendered::Nothing => String::new(),
        }
    }

    /// The pipe contract for the whole renderer: with a plain painter not one
    /// escape byte may reach stdout, whatever the event.
    #[test]
    fn plain_rendering_never_emits_an_escape() {
        let painter = plain();
        let events = [
            EngineEvent::Notification {
                level: NotificationLevel::Warn,
                message: RuntimeString::from("careful"),
            },
            EngineEvent::TurnFailed {
                inference: None,
                error: klaus_agent_core::types::errors::EngineError {
                    code: klaus_agent_core::types::errors::EngineErrorCode::ProviderError,
                    message: RuntimeString::from("boom"),
                    cause: None,
                    request_id: None,
                    provider_kind: None,
                    retry_after_ms: None,
                },
            },
            EngineEvent::Compaction {
                mode: CompactionMode::Full,
                freed_tokens: 120,
            },
        ];
        for event in &events {
            let out = text_of(&engine_event(&painter, 80, event));
            assert!(
                !out.contains('\u{1b}'),
                "escape leaked for {event:?}: {out:?}"
            );
        }
    }

    /// Every settled line starts in the bullet column, so the left edge is one
    /// column to scan rather than a ragged mix of prefixes.
    #[test]
    fn settled_lines_start_with_the_bullet() {
        let painter = plain();
        let notice = text_of(&engine_event(
            &painter,
            80,
            &EngineEvent::Notification {
                level: NotificationLevel::Info,
                message: RuntimeString::from("hello"),
            },
        ));
        assert!(notice.starts_with(glyph::BULLET), "{notice:?}");
        assert!(notice.contains("hello"));
    }

    /// The bullet's colour is what distinguishes a warning from a note, so the
    /// two must not render identically once colour is on.
    #[test]
    fn severity_changes_the_bullet_colour() {
        let painter = styled();
        let info = text_of(&engine_event(
            &painter,
            80,
            &EngineEvent::Notification {
                level: NotificationLevel::Info,
                message: RuntimeString::from("same text"),
            },
        ));
        let warn = text_of(&engine_event(
            &painter,
            80,
            &EngineEvent::Notification {
                level: NotificationLevel::Warn,
                message: RuntimeString::from("same text"),
            },
        ));
        assert_ne!(info, warn, "identical text must still look different");
        assert!(info.contains('\u{1b}') && warn.contains('\u{1b}'));
    }

    /// Assistant text streams verbatim: a prefix here would land mid-word,
    /// because a delta is a fragment and not a line.
    #[test]
    fn assistant_deltas_stream_untouched() {
        let rendered = engine_event(
            &styled(),
            80,
            &EngineEvent::TextDelta {
                inference: None,
                message_id: None,
                phase: None,
                text: RuntimeString::from("half a sen"),
            },
        );
        assert!(rendered.is_partial(), "a delta owes no newline");
        assert_eq!(text_of(&rendered), "half a sen");
    }

    /// Reasoning is capped: a long chain of thought must not bury the answer it
    /// led to, and the elision has to say how much was withheld.
    #[test]
    fn reasoning_is_capped_and_reports_what_it_withheld() {
        let body: String = (0..20).map(|index| format!("line {index}\n")).collect();
        let rendered = engine_event(
            &plain(),
            80,
            &EngineEvent::ItemCompleted {
                inference: None,
                item: ThreadItem::Reasoning {
                    id: RuntimeString::from("r1"),
                    text: RuntimeString::from(body),
                },
            },
        );
        let out = text_of(&rendered);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(
            lines.len(),
            limit::REASONING_PREVIEW_LINES + 1,
            "six lines plus the note: {lines:?}"
        );
        assert!(
            lines.last().unwrap().contains("more lines of reasoning"),
            "{lines:?}"
        );
        assert!(lines[0].starts_with(glyph::BULLET));
        // Continuation lines align under the text, not under the bullet.
        assert!(lines[1].starts_with(glyph::BULLET_GAP));
    }

    /// A tool line reads as present tense while it runs and past tense once it
    /// has finished, which is what says whether it is still live.
    #[test]
    fn tool_verbs_move_from_present_to_past() {
        let started = text_of(&engine_event(
            &plain(),
            80,
            &EngineEvent::ItemStarted {
                inference: None,
                item: ThreadItem::ToolCall {
                    id: RuntimeString::from("t1"),
                    tool_name: RuntimeString::from("Read"),
                    arguments: RuntimeJson::Null,
                    result: None,
                    status: ItemStatus::InProgress,
                    error: None,
                },
            },
        ));
        assert!(started.contains("Using Read"), "{started:?}");

        let finished = text_of(&engine_event(
            &plain(),
            80,
            &EngineEvent::ItemCompleted {
                inference: None,
                item: ThreadItem::ToolCall {
                    id: RuntimeString::from("t1"),
                    tool_name: RuntimeString::from("Read"),
                    arguments: RuntimeJson::Null,
                    result: None,
                    status: ItemStatus::Completed,
                    error: None,
                },
            },
        ));
        assert!(finished.contains("Used Read"), "{finished:?}");
    }

    /// A long argument is elided in the MIDDLE, so both the leading path and the
    /// filename survive — cutting only the tail loses the part that identifies it.
    #[test]
    fn a_long_tool_argument_keeps_both_ends() {
        let long = format!("{{\"path\":\"src/{}/handler.rs\"}}", "very/deep".repeat(12));
        let arguments = text::parse_json(&long).expect("valid json fixture");
        let out = text_of(&engine_event(
            &plain(),
            80,
            &EngineEvent::ItemStarted {
                inference: None,
                item: ThreadItem::ToolCall {
                    id: RuntimeString::from("t1"),
                    tool_name: RuntimeString::from("Read"),
                    arguments,
                    result: None,
                    status: ItemStatus::InProgress,
                    error: None,
                },
            },
        ));
        assert!(
            out.contains(glyph::ELLIPSIS),
            "the cut must be marked: {out:?}"
        );
        assert!(out.contains("src/"), "the head must survive: {out:?}");
        assert!(out.contains("handler.rs"), "the tail must survive: {out:?}");
    }

    /// An approval is a box, not a log line: it is the one thing on screen that
    /// stops until the reader acts.
    #[test]
    fn an_approval_is_boxed_and_titled() {
        let request = ApprovalRequest {
            id: RuntimeString::from("a1"),
            kind: ApprovalKind::Exec,
            tool_name: RuntimeString::from("Bash"),
            message: RuntimeString::from("run a command"),
            input: RuntimeJson::Null,
            suggestions: None,
        };
        let lines = approval_request(&plain(), 60, &request);
        assert!(lines.len() >= 3, "a frame plus a body: {lines:?}");
        assert!(lines[0].contains("approval"), "{:?}", lines[0]);
        assert!(lines[0].starts_with(glyph::BOX_TOP_LEFT));
        assert!(lines.last().unwrap().starts_with(glyph::BOX_BOTTOM_LEFT));
        // Every row the same width, or the box looks broken.
        let widths: Vec<usize> = lines.iter().map(|l| panel::visible_width(l)).collect();
        assert!(widths.windows(2).all(|p| p[0] == p[1]), "{widths:?}");
        assert!(lines.iter().any(|l| l.contains("Bash")));
    }

    /// Options are offered as indices: a free-text answer cannot be matched when
    /// a choice contains spaces.
    #[test]
    fn elicitation_numbers_its_options() {
        let request = ElicitRequestWire {
            questions: None,
            attachments_supported: None,
            metadata: None,
            prompt: RuntimeString::from("which one"),
            options: Some(vec![
                RuntimeString::from("first choice"),
                RuntimeString::from("second choice"),
            ]),
            multi_select: Some(true),
        };
        let out = elicitation(&plain(), &request);
        assert!(out.contains("[1] first choice"), "{out:?}");
        assert!(out.contains("[2] second choice"), "{out:?}");
        assert!(out.contains("comma-separated"), "{out:?}");
    }

    #[test]
    fn a_failed_command_reports_its_exit_code() {
        let out = text_of(&engine_event(
            &plain(),
            80,
            &EngineEvent::ItemCompleted {
                inference: None,
                item: ThreadItem::CommandExecution {
                    id: RuntimeString::from("c1"),
                    command: RuntimeString::from("false"),
                    aggregated_output: RuntimeString::from(""),
                    exit_code: Some(1),
                    status: ItemStatus::Failed,
                },
            },
        ));
        assert!(out.contains("exit 1"), "{out:?}");
    }
}
