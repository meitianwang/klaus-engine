//! Operations on the bound thread: its context, its cost, its transcript, and the
//! three that rewrite or re-read it.
//!
//! None of these take a thread id. Every op here acts on whatever thread the
//! invocation is bound to, so `--resume` (or `session show` earlier in the same
//! invocation) is how a caller points them at a persisted thread.
//!
//! `compact` and `review` are turns wearing a management command's clothes: they
//! call the model, stream events, and can stop to ask for approval. They go
//! through [`Turn`] for exactly that reason. `rewind`, `cost`, `context` and
//! `history` are single-reply reads and go through [`show`].

use klaus_agent_core::engine::session::SessionContextSnapshot;
use klaus_agent_core::observability::cost_tracker::{CostSnapshot, ModelUsageSnapshot};
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::message::Message;
use klaus_agent_protocol::{
    CompactSessionResult, ProtocolSessionTurnFileChanges, ReviewFinding, ReviewSessionResult,
    ReviewTarget, RewindOutcome, RewindPreview, RewindScope,
};
use klaus_agent_server::app_server::methods::{
    SessionCompactStart, SessionContextSnapshot as SessionContextSnapshotMethod, SessionCost,
    SessionFileChanges, SessionHistory, SessionReviewStart, SessionRewind, SessionRewindPreview,
};
use klaus_agent_server::app_server::wire;

use crate::cli::{ReviewArgs, RewindScopeArg};
use crate::engine::EngineClient;
use crate::error::{CliError, CliResult};
use crate::output::Printer;
use crate::render;
use crate::text;
use crate::turn::{self, ApprovalPolicy, Console, Turn};

use super::{missing, settle, show};

/// Summarise the transcript so far, freeing context.
///
/// Ctrl-C interrupts the session's turn rather than cancelling just this op:
/// compaction is not one of the three the engine tracks as separately
/// cancellable, so the targeted op would be silently ignored.
pub async fn compact(
    engine: &EngineClient,
    printer: &Printer,
    console: &mut Console,
    instructions: Option<&str>,
) -> CliResult<()> {
    let mut turn = Turn::new(
        engine,
        printer,
        console,
        ApprovalPolicy::interactive(printer, false),
    );
    let result = turn
        .follow::<SessionCompactStart, _>(
            wire::SessionCompactParams {
                session_id: engine.session_id().to_string(),
                selection: None,
                instructions: instructions.map(Into::into),
                submission_id: None,
            },
            |event| event.compact.clone(),
        )
        .await?;
    printer.line(&compacted(&result))
}

/// Review code with the engine's reviewer persona.
///
/// Ctrl-C cancels the review alone. The engine tracks it as detached read-only
/// work, so there is a targeted op for it — interrupting the session's turn
/// instead would reach past the review into whatever else the session is doing.
pub async fn review(
    engine: &EngineClient,
    printer: &Printer,
    console: &mut Console,
    args: &ReviewArgs,
) -> CliResult<()> {
    let target = review_target(args)?;
    let mut turn = Turn::new(
        engine,
        printer,
        console,
        ApprovalPolicy::interactive(printer, false),
    )
    .cancelling();
    let result = turn
        .follow::<SessionReviewStart, _>(
            wire::SessionReviewParams {
                session_id: engine.session_id().to_string(),
                target: text::to_json(&target)?,
                user_facing_hint: args.hint.as_deref().map(Into::into),
                submission_id: None,
            },
            |event| event.review.clone(),
        )
        .await?;
    printer.line(&reviewed(&result))?;
    turn::concluded(result.stop_reason)
}

/// Drop the last `turns` user turns, and optionally restore the files they
/// changed.
pub async fn rewind(
    engine: &EngineClient,
    printer: &Printer,
    turns: u32,
    scope: Option<RewindScopeArg>,
    dry_run: bool,
) -> CliResult<()> {
    // Refused here rather than sent: the engine would count zero turns as a
    // successful rewind of nothing, which reads as "it worked" to a caller who
    // meant to undo something.
    if turns == 0 {
        return Err(CliError::usage(
            "rewinding zero turns does nothing; pass at least 1".to_string(),
        ));
    }
    // A dry run answers the question the real command cannot be asked twice: the
    // file half is destructive and irreversible, so "what would this take out"
    // has to be available without taking anything out.
    if dry_run {
        return show::<SessionRewindPreview>(
            engine,
            printer,
            wire::SessionRewindPreviewParams {
                session_id: engine.session_id().to_string(),
                num_turns: turns,
                submission_id: None,
            },
            |event| match &event.rewind_preview {
                Some(preview) => rewind_preview(preview),
                None => missing("rewindPreview"),
            },
        )
        .await;
    }
    let scope = scope
        .map(|scope| text::to_json(&RewindScope::from(scope)))
        .transpose()?;
    settle::<SessionRewind>(
        engine,
        printer,
        wire::SessionRewindParams {
            session_id: engine.session_id().to_string(),
            num_turns: turns,
            scope,
            submission_id: None,
        },
        |event| event.rewound.is_some(),
        |event| match &event.rewound {
            Some(outcome) => rewound(outcome),
            None => missing("rewound"),
        },
    )
    .await
}

pub async fn cost(engine: &EngineClient, printer: &Printer) -> CliResult<()> {
    show::<SessionCost>(engine, printer, engine.scope(), |event| match &event.cost {
        Some(snapshot) => cost_snapshot(snapshot),
        None => missing("cost"),
    })
    .await
}

pub async fn context(engine: &EngineClient, printer: &Printer) -> CliResult<()> {
    show::<SessionContextSnapshotMethod>(engine, printer, engine.scope(), |event| {
        match &event.context {
            Some(snapshot) => context_snapshot(snapshot),
            None => missing("context"),
        }
    })
    .await
}

pub async fn history(engine: &EngineClient, printer: &Printer) -> CliResult<()> {
    show::<SessionHistory>(engine, printer, engine.scope(), |event| {
        match &event.history {
            Some(messages) => render::history(messages),
            None => missing("history"),
        }
    })
    .await
}

/// The files this session changed.
///
/// Deliberately not a git diff: the engine records what its OWN edits did, so
/// this answers outside a repository, survives a commit made mid-session, and
/// never reports a change the session did not make. `review --uncommitted` is
/// the working-tree question.
pub async fn changes(engine: &EngineClient, printer: &Printer) -> CliResult<()> {
    show::<SessionFileChanges>(engine, printer, engine.scope(), |event| {
        match &event.file_changes {
            Some(changes) => file_changes(changes),
            None => missing("fileChanges"),
        }
    })
    .await
}

/// Which changes to review.
///
/// The flags are mutually exclusive and clap enforces that, so this only has to
/// pick the one that was given. No flag at all means the working tree, which is
/// what a reviewer wants nine times out of ten and is the same default the
/// `--uncommitted` flag spells out.
fn review_target(args: &ReviewArgs) -> CliResult<ReviewTarget> {
    if let Some(branch) = &args.base_branch {
        return Ok(ReviewTarget::BaseBranch {
            branch: RuntimeString::from(branch.as_str()),
        });
    }
    if let Some(sha) = &args.commit {
        return Ok(ReviewTarget::Commit {
            sha: RuntimeString::from(sha.as_str()),
            // The engine uses this only as a display label. The CLI has no commit
            // subject to hand it without shelling out to git, which is the
            // engine's job on this op, not the client's.
            title: None,
        });
    }
    if !args.files.is_empty() {
        return Ok(ReviewTarget::Files {
            paths: args.files.iter().map(|path| path.as_str().into()).collect(),
        });
    }
    if let Some(instructions) = &args.instructions {
        return Ok(ReviewTarget::Custom {
            instructions: RuntimeString::from(instructions.as_str()),
        });
    }
    Ok(ReviewTarget::UncommittedChanges)
}

/// What compaction did, in tokens and messages.
///
/// A no-op compaction is reported with its reason rather than as a bare "nothing
/// happened": every reason is actionable (the compactor is off, the circuit
/// breaker tripped, the summary came out larger than the input), and a caller
/// that just watched its context stay full needs to know which.
fn compacted(result: &CompactSessionResult) -> String {
    let mut lines = Vec::new();
    if result.compacted {
        lines.push(format!(
            "compacted: {} tokens freed ({} -> {}), {} messages -> {}",
            result.freed_tokens,
            result.before_tokens,
            result.after_tokens,
            result.before_messages,
            result.after_messages
        ));
    } else {
        lines.push(match &result.reason {
            Some(reason) => format!("not compacted: {}", render::wire_tag(reason)),
            None => "not compacted".to_string(),
        });
    }
    for diagnostic in &result.diagnostics {
        let mut line = format!(
            "  [{} {}]",
            render::wire_tag(&diagnostic.stage),
            render::wire_tag(&diagnostic.kind)
        );
        if let Some(message) = &diagnostic.message {
            line.push_str(&format!(" {}", text::text(message)));
        }
        lines.push(line);
    }
    if let Some(boundary) = &result.boundary {
        lines.push(render::message(&Message::CompactBoundary(boundary.clone())));
    }
    lines.join("\n")
}

/// The review verdict, then each finding.
///
/// Findings are ordered by priority because that is the only ordering a reader
/// acts on; the reviewer emits them in whatever order it wrote them. A missing
/// priority sorts last rather than first — the rubric allows omitting it when the
/// reviewer could not decide, which is not the same as P0.
fn reviewed(result: &ReviewSessionResult) -> String {
    let output = &result.output;
    let mut lines = vec![format!(
        "[{}] {} (confidence {:.2})",
        text::text(&output.overall_correctness),
        text::text(&output.overall_explanation),
        output.overall_confidence_score
    )];
    let hint = text::text(&result.user_facing_hint);
    if !hint.is_empty() {
        lines.push(format!("reviewed: {hint}"));
    }
    if output.findings.is_empty() {
        lines.push("no findings".to_string());
    } else {
        let mut findings: Vec<&ReviewFinding> = output.findings.iter().collect();
        findings.sort_by_key(|finding| finding.priority.unwrap_or(i64::MAX));
        for finding in findings {
            lines.push(review_finding(finding));
        }
    }
    lines.push(match &result.usage {
        Some(usage) => format!(
            "[{}] {}",
            render::terminal_reason(result.stop_reason),
            render::usage_summary(usage)
        ),
        None => format!("[{}]", render::terminal_reason(result.stop_reason)),
    });
    lines.join("\n")
}

fn review_finding(finding: &ReviewFinding) -> String {
    let location = &finding.code_location;
    let priority = match finding.priority {
        Some(priority) => format!("P{priority}"),
        None => "P?".to_string(),
    };
    let mut lines = vec![format!(
        "\n[{priority}] {} ({}:{}-{}, confidence {:.2})",
        text::text(&finding.title),
        text::text(&location.absolute_file_path),
        location.line_range.start,
        location.line_range.end,
        finding.confidence_score
    )];
    for line in text::text(&finding.body).lines() {
        lines.push(format!("  {line}"));
    }
    lines.join("\n")
}

/// What the rewind actually did, including the part that did not.
///
/// A rewind can half-succeed: the conversation is dropped in memory while the
/// file restore fails because checkpointing was off or git refused. The error is
/// reported alongside the counts rather than instead of them, because the
/// conversation really is shorter now.
fn rewound(outcome: &RewindOutcome) -> String {
    let mut line = format!(
        "rewound ({}): {} turn(s) dropped, {} remaining",
        render::wire_tag(&outcome.scope),
        outcome.dropped_turns,
        outcome.remaining_turns
    );
    if outcome.files_restored {
        line.push_str(", files restored");
    }
    match &outcome.error {
        Some(error) => format!("{line}\n[incomplete] {}", text::text(error)),
        None => line,
    }
}

/// A dry run's report. `canRewind: false` is the headline, not a footnote: the
/// caller asked whether an undo is possible, and it is not.
fn rewind_preview(preview: &RewindPreview) -> String {
    if !preview.can_rewind {
        let reason = match &preview.error {
            Some(error) => text::text(error),
            None => "no reason given".to_string(),
        };
        return format!("cannot undo: {reason}");
    }
    let mut line = format!(
        "would undo {} turn(s) of file changes, touching {} file(s)",
        preview.dropped_turns,
        preview.files.len()
    );
    for file in &preview.files {
        line.push_str("\n  ");
        line.push_str(&text::text(file));
    }
    line
}

fn cost_snapshot(snapshot: &CostSnapshot) -> String {
    let mut lines = vec![format!(
        "${:.4} total, {:.0}% of prompt tokens served from cache",
        snapshot.total_cost_usd,
        snapshot.cache_hit_rate * 100.0
    )];
    if snapshot.models.is_empty() {
        lines.push("(no model calls)".to_string());
    }
    for model in &snapshot.models {
        lines.push(model_usage(model));
    }
    lines.join("\n")
}

fn model_usage(model: &ModelUsageSnapshot) -> String {
    let totals = &model.totals;
    format!(
        "  {}  ${:.4}  {} request(s)  in {} / out {} / cache read {} / cache write {}",
        text::text(&totals.model),
        totals.cost_usd,
        totals.requests,
        totals.input_tokens,
        totals.output_tokens,
        totals.cache_read_input_tokens,
        totals.cache_creation_input_tokens
    )
}

/// The context window as the engine sees it, which is what decides when the next
/// automatic compaction fires.
///
/// `total_tokens` is occupancy, not billing: it shrinks when the transcript is
/// compacted, while the cumulative token totals below it never do. Printing them
/// under separate headings is the only way that difference survives.
fn context_snapshot(snapshot: &SessionContextSnapshot) -> String {
    let window = snapshot.context_window;
    let used = snapshot.total_tokens;
    let percent = if window == 0 {
        0.0
    } else {
        (used as f64 / window as f64) * 100.0
    };
    let mut lines = vec![
        format!(
            "{}  {}  {}",
            text::text(&snapshot.thread_id),
            text::text(&snapshot.model),
            text::text(&snapshot.cwd)
        ),
        format!(
            "context: {used} / {window} tokens ({percent:.1}%, {}), compacts at {} ({:.0}%), {} reserved for output",
            render::wire_tag(&snapshot.window_state),
            snapshot.compact_threshold_tokens,
            snapshot.compact_threshold * 100.0,
            snapshot.output_reserve_tokens
        ),
        format!(
            "session totals: in {} / out {} / cache read {} / cache write {} over {} message(s)",
            snapshot.token_totals.input_tokens,
            snapshot.token_totals.output_tokens,
            snapshot.token_totals.cache_read_input_tokens,
            snapshot.token_totals.cache_creation_input_tokens,
            snapshot.history_messages
        ),
    ];
    lines.push(
        match (snapshot.max_budget_usd, snapshot.budget_remaining_usd) {
            (Some(max), Some(remaining)) => format!(
                "budget: ${remaining:.4} of ${max:.4} left ({})",
                render::wire_tag(&snapshot.budget_state)
            ),
            _ => format!("budget: {}", render::wire_tag(&snapshot.budget_state)),
        },
    );
    lines.push(cost_snapshot(&snapshot.cost));
    lines.join("\n")
}

/// One section per turn, oldest first: a heading with that turn's totals, then
/// one line per file.
///
/// A zero count still prints: an edit that genuinely changed no lines is not the
/// same as one that could not be measured, and dropping either row would hide a
/// file the turn really did touch. An unmeasurable count prints `?`, and one
/// unknown file makes the turn's total unknown too — summing it as zero is what
/// turns "we don't know" into a number the reader would trust.
fn file_changes(turns: &[ProtocolSessionTurnFileChanges]) -> String {
    if turns.iter().all(|turn| turn.files.is_empty()) {
        return "no file changes".to_string();
    }
    let mut sections = Vec::with_capacity(turns.len());
    for turn in turns {
        let turn_label = turn
            .turn_id
            .as_ref()
            .map(text::text)
            .unwrap_or_else(|| "unattributed".to_string());
        let added: Option<u64> = turn.files.iter().map(|file| file.added).sum();
        let removed: Option<u64> = turn.files.iter().map(|file| file.removed).sum();
        let mut lines = vec![format!(
            "turn {turn_label}: {} file(s) changed, +{} -{}",
            turn.files.len(),
            count(added),
            count(removed)
        )];
        for file in &turn.files {
            lines.push(format!(
                "  {:<7} +{} -{}  {}",
                render::wire_tag(&file.kind),
                count(file.added),
                count(file.removed),
                text::text(&file.path)
            ));
        }
        sections.push(lines.join("\n"));
    }
    sections.join("\n")
}

/// A line count, or `?` when the engine could not measure it.
fn count(value: Option<u64>) -> String {
    value.map_or_else(|| "?".to_string(), |count| count.to_string())
}
