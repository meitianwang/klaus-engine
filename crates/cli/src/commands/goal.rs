//! The thread's goal, and the steering queue.
//!
//! Both are thread state the engine mutates on its own, so both are printed from
//! the reply rather than from what was asked for: `set` can be refused, adjusted,
//! or admitted by the engine for continued work.
//!
//! The queue's three mutations are compare-and-swap. Every one of them carries a
//! revision the caller decided against and a mutation id, and the engine can
//! answer with nine different statuses, most of which mean "not applied". So the
//! status is always printed, never inferred from the absence of an error — a
//! stale-revision edit is a successful op that changed nothing.

use klaus_agent_core::engine::message_queue::{
    QueueEdit, QueueMutationResult, QueueSnapshot, QueuedMessage, QueuedValue,
};
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_protocol::types::{Input, ProtocolThreadGoal};
use klaus_agent_protocol::{ProtocolEvent, SteerPriority};
use klaus_agent_server::app_server::methods::{
    SessionClearGoal, SessionEditQueued, SessionGoal, SessionQueueSnapshot, SessionQueued,
    SessionRemoveQueued, SessionReorderQueued, SessionSetGoal, SessionSteer, SessionTakePending,
};
use klaus_agent_server::app_server::runtime_wire::RawJson;
use klaus_agent_server::app_server::wire;
use klaus_agent_server::Method;
use uuid::Uuid;

use crate::cli::{GoalCommand, QueueCommand};
use crate::engine::EngineClient;
use crate::error::{CliError, CliResult};
use crate::output::Printer;
use crate::render;
use crate::text;

use super::{missing, show};

pub async fn goal(
    engine: &EngineClient,
    printer: &Printer,
    command: &GoalCommand,
) -> CliResult<()> {
    match command {
        // A thread with no goal replies with the field absent, which is the
        // answer rather than a missing one — and this method answers with its
        // reply, so there is no stream to wait on before "no goal" is reportable.
        GoalCommand::Get => {
            show::<SessionGoal>(engine, printer, goal_target(engine), thread_goal).await
        }

        GoalCommand::Set {
            objective,
            status,
            token_budget,
            clear_token_budget,
        } => {
            if objective.is_none()
                && status.is_none()
                && token_budget.is_none()
                && !*clear_token_budget
            {
                return Err(CliError::usage(
                    "nothing to set: pass an objective, --status, --token-budget or --clear-token-budget"
                        .to_string(),
                ));
            }
            show::<SessionSetGoal>(
                engine,
                printer,
                wire::SessionSetGoalParams {
                    owned_resources: None,
                    client_request_id: None,
                    target: goal_target(engine),
                    submit: None,
                    display_text: None,
                    objective: objective.as_deref().map(RuntimeString::from),
                    // Carried as a free string on purpose: the engine owns the
                    // status vocabulary and has statuses no caller sets by hand
                    // (`usage_limited`, `budget_limited`). Validating it here
                    // would reject a status this CLI merely has not heard of.
                    status: status.clone(),
                    // Double option: the outer one says whether the budget is
                    // being touched at all, the inner one says whether it is
                    // being removed. Collapsing them would make "leave it alone"
                    // and "remove it" the same request.
                    token_budget: match (token_budget, clear_token_budget) {
                        (Some(budget), _) => Some(Some(*budget)),
                        (None, true) => Some(None),
                        (None, false) => None,
                    },
                },
                goal_set,
            )
            .await
        }

        GoalCommand::Clear => {
            show::<SessionClearGoal>(engine, printer, goal_target(engine), |event| {
                match event.goal_cleared {
                    Some(true) => "goal cleared".to_string(),
                    Some(false) => "no goal to clear".to_string(),
                    None => missing("goalCleared"),
                }
            })
            .await
        }
    }
}

pub async fn queue(
    engine: &EngineClient,
    printer: &Printer,
    command: &QueueCommand,
) -> CliResult<()> {
    match command {
        QueueCommand::List => {
            show::<SessionQueued>(engine, printer, engine.scope(), |event| {
                match &event.queued {
                    Some(messages) => queued_list(messages),
                    None => missing("queued"),
                }
            })
            .await
        }

        // `session/steer` IS the enqueue method: the engine routes it into this
        // session's message queue, tagged with the submission as its correlation
        // id.
        //
        // Its own ack is no use here. The engine writes `steered` only when a
        // RUNNING turn attaches the input, so on an idle session it never comes
        // and waiting for it would hang the ordinary case. The snapshot taken
        // straight after is the honest confirmation instead: the message is
        // either in it, or a turn already took it up.
        QueueCommand::Add {
            text: message,
            priority,
        } => {
            engine
                .ack::<SessionSteer>(wire::SessionSteerParams {
                    session_id: engine.session_id().to_string(),
                    input: text::input(&Input::Text(RuntimeString::from(message.as_str())))?,
                    priority: priority
                        .map(|priority| text::tag(&SteerPriority::from(priority)))
                        .transpose()?,
                    submission_id: None,
                    // The CLI's own transcript rendering predates the user-shaped
                    // steer history; keep the legacy tool-result shape here.
                    delivery: None,
                    display_metadata: None,
                    expected_turn_id: None,
                    queue_message_id: None,
                })
                .await?;
            show::<SessionQueueSnapshot>(engine, printer, engine.scope(), snapshot).await
        }

        // Destructive, and the reply is the only copy: the engine hands the
        // pending set over exactly once, so printing it is not a view of state
        // but the sole record that these messages existed.
        QueueCommand::Pending => {
            show::<SessionTakePending>(engine, printer, engine.scope(), |event| {
                match &event.pending {
                    Some(messages) => queued_list(messages),
                    None => missing("pending"),
                }
            })
            .await
        }

        QueueCommand::Snapshot => {
            show::<SessionQueueSnapshot>(engine, printer, engine.scope(), snapshot).await
        }

        QueueCommand::Edit {
            uuid,
            revision,
            text: message,
            mode,
            priority,
        } => {
            if message.is_none() && mode.is_none() && priority.is_none() {
                return Err(CliError::usage(
                    "nothing to edit: pass --text, --mode or --priority".to_string(),
                ));
            }
            mutate::<SessionEditQueued>(
                engine,
                printer,
                wire::SessionQueueEditParams {
                    session_id: engine.session_id().to_string(),
                    mutation_id: mutation_id(),
                    uuid: uuid.clone(),
                    expected_revision: *revision,
                    edit: text::to_json(&QueueEdit {
                        // Only text is offered. A queued message can hold
                        // structured content blocks, but a caller who wants to
                        // REPLACE one from a shell has nothing but text to type,
                        // and encoding blocks by hand on the command line would
                        // be a worse interface than leaving it to a real client.
                        value: message
                            .as_deref()
                            .map(|value| QueuedValue::Text(RuntimeString::from(value))),
                        mode: mode.as_deref().map(RuntimeString::from),
                        priority: priority.map(Into::into),
                    })?,
                    submission_id: None,
                },
            )
            .await
        }

        QueueCommand::Remove { uuid, revision } => {
            mutate::<SessionRemoveQueued>(
                engine,
                printer,
                wire::SessionQueueRemoveParams {
                    session_id: engine.session_id().to_string(),
                    mutation_id: mutation_id(),
                    uuid: uuid.clone(),
                    expected_revision: *revision,
                    submission_id: None,
                },
            )
            .await
        }

        QueueCommand::Reorder {
            uuid,
            revision,
            before,
        } => {
            mutate::<SessionReorderQueued>(
                engine,
                printer,
                wire::SessionQueueReorderParams {
                    session_id: engine.session_id().to_string(),
                    mutation_id: mutation_id(),
                    uuid: uuid.clone(),
                    // Absent means "move it last", which is the method's own
                    // convention rather than something invented here.
                    before_uuid: before.clone(),
                    expected_revision: *revision,
                    submission_id: None,
                },
            )
            .await
        }
    }
}

/// The three CAS mutations share one reply shape and one reason for existing:
/// every status other than `applied` has to reach the caller, because the queue
/// is unchanged and the revision they decided against is stale.
async fn mutate<M>(engine: &EngineClient, printer: &Printer, params: M::Params) -> CliResult<()>
where
    M: Method<Result = RawJson>,
{
    show::<M>(engine, printer, params, |event| {
        match &event.queue_mutation {
            Some(result) => queue_mutation(result),
            None => missing("queueMutation"),
        }
    })
    .await
}

/// A fresh id for one mutation attempt.
///
/// The engine deduplicates by this id, so retrying the SAME id replays the first
/// outcome instead of mutating twice. A new id per invocation is therefore
/// correct: one CLI run is one attempt, and it has nothing to replay.
fn mutation_id() -> String {
    Uuid::new_v4().to_string()
}

/// The queue as the engine reported it, for the two commands that ask for it.
fn snapshot(event: &ProtocolEvent) -> String {
    match &event.queue_snapshot {
        Some(snapshot) => queue_snapshot(snapshot),
        None => missing("queueSnapshot"),
    }
}

fn thread_goal(event: &ProtocolEvent) -> String {
    match &event.thread_goal {
        Some(goal) => goal_lines(goal),
        None => "(no goal)".to_string(),
    }
}

/// Report the durable goal and the engine's admission disposition.
fn goal_set(event: &ProtocolEvent) -> String {
    let mut lines = vec![thread_goal(event)];
    if let Some(admission) = &event.goal_admission {
        lines.push(format!("goal work: {}", text::text(admission)));
    }
    lines.join("\n")
}

fn goal_target(engine: &EngineClient) -> wire::SessionGoalParams {
    wire::SessionGoalParams {
        session_id: Some(engine.session_id().to_string()),
        thread_id: None,
        expected_goal_id: None,
        expected_goal_revision: None,
        origin: Some(wire::GoalOrigin::User),
        submission_id: None,
    }
}

fn goal_lines(goal: &ProtocolThreadGoal) -> String {
    let budget = match goal.token_budget {
        Some(budget) => format!("{} / {budget} tokens", goal.tokens_used),
        None => format!("{} tokens, no budget", goal.tokens_used),
    };
    format!(
        "[{}] {}\n  {}  {}  {}s elapsed",
        text::text(&goal.status),
        text::text(&goal.objective),
        text::text(&goal.goal_id),
        budget,
        goal.time_used_seconds
    )
}

fn queued_list(messages: &[QueuedMessage]) -> String {
    if messages.is_empty() {
        return "(queue empty)".to_string();
    }
    messages
        .iter()
        .map(queued_message)
        .collect::<Vec<_>>()
        .join("\n")
}

fn queued_message(message: &QueuedMessage) -> String {
    let mut tags = vec![text::text(&message.mode)];
    if let Some(priority) = &message.priority {
        tags.push(render::wire_tag(priority));
    }
    if message.is_meta == Some(true) {
        tags.push("meta".to_string());
    }
    if let Some(origin) = &message.origin {
        tags.push(format!("from {}", text::text(origin)));
    }
    if let Some(agent) = &message.agent_id {
        tags.push(format!("agent {}", text::text(agent)));
    }
    if message.pdf_documents > 0 {
        tags.push(format!(
            "{} pdf doc(s), {} bytes",
            message.pdf_documents, message.pdf_bytes
        ));
    }
    format!(
        "{}  [{}]  {}",
        render::label(message.uuid.as_ref(), "(no uuid)"),
        tags.join(", "),
        queued_value(&message.value)
    )
}

/// A queued payload as one line.
///
/// Structured content is reported by shape rather than rendered: the queue is a
/// list view, and a message holding an image or a tool result would otherwise
/// swamp every other row.
fn queued_value(value: &QueuedValue) -> String {
    match value {
        QueuedValue::Text(text) => text::text(text).replace('\n', " "),
        QueuedValue::Blocks(blocks) => format!("({} content block(s))", blocks.len()),
    }
}

/// The queue with its revision, which is the number every mutation must quote.
///
/// A `persistence-failed` snapshot is still shown in full: the body remains the
/// last state that WAS proven durable, so it is the most accurate answer
/// available — just not one a mutation can be built on.
fn queue_snapshot(snapshot: &QueueSnapshot) -> String {
    let mut lines = vec![format!(
        "revision {} [{}], {} item(s)",
        snapshot.revision,
        render::wire_tag(&snapshot.status),
        snapshot.items.len()
    )];
    if let Some(error) = &snapshot.error {
        lines.push(format!("[not durable] {}", text::text(error)));
    }
    for item in &snapshot.items {
        let mut line = format!(
            "  [{}{}] {}",
            render::wire_tag(&item.state),
            if item.mutable { "" } else { ", locked" },
            queued_message(&item.message)
        );
        // Reserved items belong to the running continuation, so an edit against
        // them is refused. Saying so here saves a round trip that can only fail.
        if !item.mutable {
            line.push_str("\n    (reserved by the active turn; not editable)");
        }
        lines.push(line);
    }
    lines.join("\n")
}

fn queue_mutation(result: &QueueMutationResult) -> String {
    let mut lines = vec![format!(
        "{} [{}]",
        text::text(&result.mutation_id),
        render::wire_tag(&result.status)
    )];
    if let Some(state) = &result.terminal_state {
        lines.push(format!("  terminal: {}", render::wire_tag(state)));
    }
    if let Some(error) = &result.error {
        lines.push(format!("  {}", text::text(error)));
    }
    // The post-operation snapshot comes back with every mutation, applied or
    // not, and its revision is what a retry has to quote. Printing it is what
    // makes a stale-revision failure recoverable in one more command.
    lines.push(queue_snapshot(&result.snapshot));
    lines.join("\n")
}
