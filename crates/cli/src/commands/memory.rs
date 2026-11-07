//! Agent memory, dreaming, and the sub-agents that own their own memory.
//!
//! Memory is feature-gated in the engine, so most of this surface reports a
//! subsystem that may be switched off. That is why `dream` prints its refusal
//! reason and `status` prints an empty snapshot rather than either of them
//! failing: "the gate is closed" is the answer, and a CLI that turned it into an
//! error would make a disabled feature look like a broken one.
//!
//! The snapshot-decision commands answer a request the ENGINE raised: an agent
//! found a newer memory snapshot than its local state and cannot proceed until
//! someone says merge, keep or replace. They are here because they are memory's
//! only interactive surface, and they are the reason the audit list exists — a
//! request that timed out was decided by policy, and the audit is the only record
//! of which policy decided it.

use futures::StreamExt;

use klaus_agent_core::engine::session::{DreamNowCompletion, DreamNowOutcome};
use klaus_agent_core::memory::agent_memory::AgentMemoryMergeChoice;
use klaus_agent_core::memory::maintenance::{
    AutoDreamRun, AutoMemoryRun, MemoryMaintenanceSnapshot,
};
use klaus_agent_core::memory::snapshot_decision::AgentMemorySnapshotDecisionAudit;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::subagent_lifecycle::{SubagentLifecycleRecord, SubagentSnapshot};
use klaus_agent_core::store::session_store::SubAgentTranscript;
use klaus_agent_protocol::{AgentMemorySnapshotUnavailableReason, BackgroundAgentSummary};
use klaus_agent_server::app_server::methods::{
    SessionAgentMemorySnapshotDecision, SessionAgentMemorySnapshotDecisions, SessionDreamNow,
    SessionMemoryMaintenance, SessionResolveAgentMemorySnapshotDecision,
    SessionSettleAgentMemorySnapshotUnavailable, SessionSubagentLifecycle,
    SessionSubagentTranscript, SessionSubagents,
};
use klaus_agent_server::app_server::wire;
use klaus_agent_server::Method;

use crate::cli::{MemoryCommand, SnapshotCommand, SubagentCommand};
use crate::engine::EngineClient;
use crate::error::{CliError, CliResult};
use crate::output::Printer;
use crate::render;
use crate::text;

use super::{missing, settle, show};

pub async fn run(
    engine: &EngineClient,
    printer: &Printer,
    command: &MemoryCommand,
) -> CliResult<()> {
    match command {
        MemoryCommand::Status => {
            show::<SessionMemoryMaintenance>(engine, printer, engine.scope(), |event| match &event
                .memory_maintenance
            {
                Some(snapshot) => maintenance(snapshot),
                None => missing("memoryMaintenance"),
            })
            .await
        }

        MemoryCommand::Dream { wait } => dream(engine, printer, *wait).await,

        MemoryCommand::Snapshot { command } => snapshot(engine, printer, command).await,
    }
}

/// Start a dream pass, and optionally stay for the end of it.
///
/// Two events answer this method under one submission id: `dream` says whether
/// the pass STARTED, and `dream_completed` says how it ended. Without `--wait`
/// only the first is printed, which is the honest report of what a non-blocking
/// start knows. A refused start has no completion to wait for, so `--wait`
/// settles on the refusal instead of hanging until the transport dies.
///
/// Followed by hand rather than through [`settle`], which stops at the FIRST
/// envelope it accepts: printing the start and then the completion needs two.
async fn dream(engine: &EngineClient, printer: &Printer, wait: bool) -> CliResult<()> {
    let mut events = engine.events();
    let submission_id = engine.ack::<SessionDreamNow>(engine.scope()).await?;

    while let Some(event) = events.next().await {
        let event = event?;
        if event.submission_id != submission_id {
            continue;
        }
        if let Some(error) = &event.error {
            return Err(CliError::protocol(text::text(error)));
        }
        if let Some(outcome) = &event.dream {
            printer.value(event.as_ref(), |_| dream_started(outcome))?;
            if !wait || !outcome.ok {
                return Ok(());
            }
            continue;
        }
        if let Some(completion) = &event.dream_completed {
            return printer.value(event.as_ref(), |_| dream_finished(completion));
        }
    }
    Err(CliError::Protocol(
        "the engine closed the event stream before the dream pass reported".to_string(),
    ))
}

async fn snapshot(
    engine: &EngineClient,
    printer: &Printer,
    command: &SnapshotCommand,
) -> CliResult<()> {
    match command {
        SnapshotCommand::List => {
            show::<SessionAgentMemorySnapshotDecisions>(engine, printer, engine.scope(), |event| {
                match &event.agent_memory_snapshot_decisions {
                    Some(audits) if audits.is_empty() => "(no decisions)".to_string(),
                    Some(audits) => audits.iter().map(decision).collect::<Vec<_>>().join("\n\n"),
                    None => missing("agentMemorySnapshotDecisions"),
                }
            })
            .await
        }

        SnapshotCommand::Get { request_id } => {
            show::<SessionAgentMemorySnapshotDecision>(
                engine,
                printer,
                wire::SessionRequestIdParams {
                    session_id: engine.session_id().to_string(),
                    request_id: request_id.clone(),
                    submission_id: None,
                },
                |event| match &event.agent_memory_snapshot_decision_query {
                    // An unknown request id is answered, not refused: the query
                    // has an explicit not-found result, so reporting it as an
                    // error would invent a failure the engine did not have.
                    Some(query) => match &query.audit {
                        Some(audit) => decision(audit),
                        None => format!("{}: no such request", text::text(&query.request_id)),
                    },
                    None => missing("agentMemorySnapshotDecisionQuery"),
                },
            )
            .await
        }

        SnapshotCommand::Resolve { request_id, choice } => {
            decided::<SessionResolveAgentMemorySnapshotDecision>(
                engine,
                printer,
                wire::SessionMemorySnapshotDecisionParams {
                    session_id: engine.session_id().to_string(),
                    request_id: request_id.clone(),
                    decision: text::to_json(&AgentMemoryMergeChoice::from(*choice))?,
                    submission_id: None,
                },
            )
            .await
        }

        // Settling is not the same as answering: it tells the engine the decision
        // CANNOT be answered, so its own policy for that reason takes over. The
        // audit then records which policy decided, which is why the reply is
        // printed the same way a real answer is.
        SnapshotCommand::Settle {
            request_id,
            reason,
            error,
        } => {
            decided::<SessionSettleAgentMemorySnapshotUnavailable>(
                engine,
                printer,
                wire::SessionMemorySnapshotUnavailableParams {
                    session_id: engine.session_id().to_string(),
                    request_id: request_id.clone(),
                    reason: text::tag(&AgentMemorySnapshotUnavailableReason::from(*reason))?,
                    error: error.clone(),
                    submission_id: None,
                },
            )
            .await
        }
    }
}

async fn decided<M>(engine: &EngineClient, printer: &Printer, params: M::Params) -> CliResult<()>
where
    M: Method<Result = wire::SubmissionAck>,
{
    settle::<M>(
        engine,
        printer,
        params,
        |event| event.agent_memory_snapshot_decision.is_some(),
        |event| match &event.agent_memory_snapshot_decision {
            Some(audit) => decision(audit),
            None => missing("agentMemorySnapshotDecision"),
        },
    )
    .await
}

pub async fn subagent(
    engine: &EngineClient,
    printer: &Printer,
    command: &SubagentCommand,
) -> CliResult<()> {
    match command {
        // The live view: what is running right now, from the tool runtime.
        SubagentCommand::List => {
            show::<SessionSubagents>(engine, printer, engine.scope(), |event| {
                match &event.subagents {
                    Some(agents) if agents.is_empty() => "(no sub-agents)".to_string(),
                    Some(agents) => agents
                        .iter()
                        .map(background_agent)
                        .collect::<Vec<_>>()
                        .join("\n"),
                    None => missing("subagents"),
                }
            })
            .await
        }

        // The persisted view: every sub-agent this session ever spawned, including
        // the ones that finished and the ones a crash left `interrupted`. It is a
        // different question from `list`, not a richer answer to the same one.
        SubagentCommand::Lifecycle => {
            show::<SessionSubagentLifecycle>(engine, printer, engine.scope(), |event| match &event
                .subagent_lifecycle
            {
                Some(snapshot) => lifecycle(snapshot),
                None => missing("subagentLifecycle"),
            })
            .await
        }

        SubagentCommand::Transcript {
            thread_id,
            agent_id,
        } => {
            show::<SessionSubagentTranscript>(
                engine,
                printer,
                wire::SessionSubagentTranscriptParams {
                    session_id: engine.session_id().to_string(),
                    thread_id: thread_id.clone(),
                    agent_id: agent_id.clone(),
                    submission_id: None,
                },
                |event| match &event.subagent_transcript {
                    Some(transcript) => subagent_transcript(transcript),
                    None => missing("subagentTranscript"),
                },
            )
            .await
        }
    }
}

/// Automatic memory and dream runs, newest last.
///
/// The cursor is printed because it is the thing that decides whether the NEXT
/// automatic extraction has anything to do: it marks how far into the transcript
/// memory has already read, independently of the run list's order.
fn maintenance(snapshot: &MemoryMaintenanceSnapshot) -> String {
    let mut lines = Vec::new();
    if snapshot.auto_memory_runs.is_empty() && snapshot.auto_dream_runs.is_empty() {
        lines.push("(no memory or dream runs)".to_string());
    }
    for run in &snapshot.auto_memory_runs {
        lines.push(memory_run(run));
    }
    for run in &snapshot.auto_dream_runs {
        lines.push(dream_run(run));
    }
    if let Some(cursor) = &snapshot.auto_memory_cursor_message_uuid {
        lines.push(format!("memory cursor: {}", text::text(cursor)));
    }
    lines.join("\n")
}

fn memory_run(run: &AutoMemoryRun) -> String {
    let mut line = format!(
        "[memory {}] {}  {} {}  {} message(s)",
        render::wire_tag(&run.status),
        text::text(&run.run_id),
        render::wire_tag(&run.scope),
        text::text(&run.target_dir),
        run.message_count
    );
    if let Some(reason) = &run.reason {
        line.push_str(&format!("  ({})", render::wire_tag(reason)));
    }
    line.push_str(&touched(&run.files_touched));
    if let Some(error) = &run.error {
        line.push_str(&format!("\n  {}", text::text(error)));
    }
    line
}

fn dream_run(run: &AutoDreamRun) -> String {
    let mut line = format!(
        "[dream {}] {}  {} turn(s), {} session(s) reviewing",
        render::wire_tag(&run.status),
        text::text(&run.dream_run_id),
        run.turn_count,
        run.sessions_reviewing
    );
    if let Some(reason) = &run.reason {
        line.push_str(&format!("  ({})", render::wire_tag(reason)));
    }
    line.push_str(&touched(&run.files_touched));
    if let Some(summary) = &run.summary {
        line.push_str(&format!("\n  {}", text::text(summary)));
    }
    if let Some(error) = &run.error {
        line.push_str(&format!("\n  {}", text::text(error)));
    }
    line
}

/// Which files a run wrote. This is the whole observable effect of memory work,
/// so it is listed rather than counted.
fn touched(files: &[RuntimeString]) -> String {
    if files.is_empty() {
        return String::new();
    }
    format!(
        "\n  wrote {}",
        files.iter().map(text::text).collect::<Vec<_>>().join(", ")
    )
}

fn dream_started(outcome: &DreamNowOutcome) -> String {
    match (&outcome.ok, &outcome.reason) {
        (true, _) => "dreaming".to_string(),
        (false, Some(reason)) => format!("not dreaming: {}", render::wire_tag(reason)),
        (false, None) => "not dreaming".to_string(),
    }
}

fn dream_finished(completion: &DreamNowCompletion) -> String {
    let mut line = if completion.ok {
        match completion.turn_count {
            Some(turns) => format!("dream finished after {turns} turn(s)"),
            None => "dream finished".to_string(),
        }
    } else {
        "dream failed".to_string()
    };
    if let Some(reason) = &completion.reason {
        line.push_str(&format!(": {}", text::text(reason)));
    }
    if let Some(files) = &completion.files_touched {
        line.push_str(&touched(files));
    }
    line
}

/// One snapshot decision, with the diff that made it necessary.
///
/// The deadline is shown for a request that is still open, because it is the only
/// thing that says how long the agent will wait before policy answers for the
/// user. Once decided it is noise, so it is dropped.
fn decision(audit: &AgentMemorySnapshotDecisionAudit) -> String {
    let request = &audit.request;
    let diff = &request.diff;
    let mut lines = vec![format!(
        "{}  [{}]  {} {}",
        text::text(&request.request_id),
        render::wire_tag(&audit.status),
        text::text(&request.agent_type),
        render::wire_tag(&request.scope)
    )];
    let mut second = format!(
        "  snapshot {} vs local {}, +{} ~{} -{}",
        text::text(&request.snapshot_id),
        text::text(&request.local_state_id),
        diff.added_count,
        diff.modified_count,
        diff.removed_count
    );
    if diff.truncated {
        second.push_str(" (paths truncated)");
    }
    if request.first_use {
        second.push_str(", first use");
    }
    lines.push(second);
    for entry in &diff.entries {
        lines.push(format!(
            "    {} {}",
            render::wire_tag(&entry.kind),
            text::text(&entry.path)
        ));
    }
    match (&audit.decision, &audit.source) {
        (Some(choice), Some(source)) => lines.push(format!(
            "  decided {} by {}",
            render::wire_tag(choice),
            render::wire_tag(source)
        )),
        (Some(choice), None) => lines.push(format!("  decided {}", render::wire_tag(choice))),
        (None, _) => lines.push(format!("  undecided, deadline {}", request.deadline_at)),
    }
    if let Some(error) = &audit.error {
        lines.push(format!("  {}", text::text(error)));
    }
    lines.join("\n")
}

fn background_agent(agent: &BackgroundAgentSummary) -> String {
    let mut tags = vec![render::wire_tag(&agent.status)];
    if agent.teammate {
        tags.push("teammate".to_string());
    }
    if agent.idle {
        tags.push("idle".to_string());
    }
    format!(
        "{}  [{}]  {}\n  transcript {}\n  output {}",
        text::text(&agent.id),
        tags.join(", "),
        text::text(&agent.description),
        text::text(&agent.transcript),
        text::text(&agent.output_file)
    )
}

fn lifecycle(snapshot: &SubagentSnapshot) -> String {
    if snapshot.agents.is_empty() {
        return "(no sub-agent records)".to_string();
    }
    snapshot
        .agents
        .iter()
        .map(lifecycle_record)
        .collect::<Vec<_>>()
        .join("\n")
}

fn lifecycle_record(record: &SubagentLifecycleRecord) -> String {
    let context = &record.context;
    let mut tags = vec![
        render::wire_tag(&record.kind),
        render::wire_tag(&record.status),
    ];
    if record.cancellation_requested {
        tags.push("cancelling".to_string());
    }
    if context.depth > 0 {
        tags.push(format!("depth {}", context.depth));
    }
    let mut lines = vec![format!(
        "{}  [{}]  {}",
        text::text(&record.agent_id),
        tags.join(", "),
        text::text(&context.description)
    )];
    if let Some(agent_type) = &context.agent_type {
        lines.push(format!("  as {}", text::text(agent_type)));
    }
    if let Some(result) = &record.last_result {
        lines.push(format!("  {}", text::text(result)));
    }
    if let Some(error) = &record.last_error {
        lines.push(format!("  {}", text::text(error)));
    }
    lines.join("\n")
}

/// A sub-agent's transcript, with the provenance that explains it.
///
/// The metadata matters more here than in a main-thread transcript: a sub-agent
/// ran on a model and a permission mode the caller never chose directly, so
/// "why did it refuse that edit" is answered by the header, not the messages.
fn subagent_transcript(transcript: &SubAgentTranscript) -> String {
    let mut lines = Vec::new();
    if let Some(meta) = &transcript.meta {
        let mut header = format!("model {}", text::text(&meta.model));
        if let Some(agent_type) = &meta.agent_type {
            header.push_str(&format!(", as {}", text::text(agent_type)));
        }
        if let Some(mode) = &meta.permission_mode {
            header.push_str(&format!(", {}", text::text(mode)));
        }
        if let Some(effort) = &meta.effort {
            header.push_str(&format!(", effort {}", render::wire_tag(effort)));
        }
        lines.push(header);
    }
    if let Some(tools) = &transcript.surfaced_tools {
        if !tools.is_empty() {
            lines.push(format!(
                "surfaced tools: {}",
                tools.iter().map(text::text).collect::<Vec<_>>().join(", ")
            ));
        }
    }
    lines.push(render::history(&transcript.messages));
    lines.join("\n")
}
