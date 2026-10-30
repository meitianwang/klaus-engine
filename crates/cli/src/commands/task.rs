//! The engine's task list, and the setup hooks that run before a session works.
//!
//! Both are things the engine does on its own and the CLI can only observe or
//! nudge. The task list is written by the model through its own tool, so `list` is
//! a read of what the model believes it is doing — the CLI deliberately offers no
//! way to edit it, because a human editing the model's plan behind its back
//! desynchronises the two.
//!
//! Setup is exactly-once per request id, which is why `run` takes one and defaults
//! to a fresh uuid. Re-running with the SAME id replays the first outcome instead
//! of executing the hooks twice, and that is the property a caller retrying a
//! failed init depends on.

use klaus_agent_core::hooks::setup::{
    SetupLifecycleSnapshot, SetupRunRecord, SetupRunRequest, SetupRunResult,
};
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::tasks::task_store::{Task, TaskListQueryOptions, TaskListSnapshot};
use klaus_agent_server::app_server::methods::{
    SessionRunSetup, SessionSetupLifecycle, SessionTaskList,
};
use klaus_agent_server::app_server::wire;
use uuid::Uuid;

use crate::cli::{SetupCommand, TaskCommand};
use crate::engine::EngineClient;
use crate::error::CliResult;
use crate::output::Printer;
use crate::render;
use crate::text;

use super::{missing, settle, show};

pub async fn task(
    engine: &EngineClient,
    printer: &Printer,
    command: &TaskCommand,
) -> CliResult<()> {
    match command {
        TaskCommand::List { limit, cursor } => {
            // Sent only when a page was actually asked for. The store has its own
            // default page size, and an all-`None` options object would pin this
            // CLI to whatever that default is today.
            let options = match (limit, cursor) {
                (None, None) => None,
                _ => Some(text::to_json(&TaskListQueryOptions {
                    cursor: cursor.as_deref().map(RuntimeString::from),
                    limit: *limit,
                })?),
            };
            show::<SessionTaskList>(
                engine,
                printer,
                wire::SessionTaskListParams {
                    session_id: engine.session_id().to_string(),
                    options,
                    submission_id: None,
                },
                |event| match &event.task_list {
                    Some(snapshot) => task_list(snapshot),
                    None => missing("taskList"),
                },
            )
            .await
        }
    }
}

pub async fn setup(
    engine: &EngineClient,
    printer: &Printer,
    command: &SetupCommand,
) -> CliResult<()> {
    match command {
        SetupCommand::Run {
            trigger,
            request_id,
        } => {
            let request = SetupRunRequest {
                // A caller that wants replay semantics supplies the id it used
                // before; one that just wants the hooks to run gets a fresh id
                // and therefore a fresh execution.
                request_id: RuntimeString::from(
                    request_id
                        .clone()
                        .unwrap_or_else(|| Uuid::new_v4().to_string()),
                ),
                trigger: (*trigger).into(),
            };
            settle::<SessionRunSetup>(
                engine,
                printer,
                wire::SessionRunSetupParams {
                    session_id: engine.session_id().to_string(),
                    request: text::to_json(&request)?,
                    submission_id: None,
                },
                |event| event.setup.is_some(),
                |event| match &event.setup {
                    Some(result) => setup_run(result),
                    None => missing("setup"),
                },
            )
            .await
        }

        SetupCommand::Status => {
            show::<SessionSetupLifecycle>(engine, printer, engine.scope(), |event| {
                match &event.setup_lifecycle {
                    Some(snapshot) => setup_lifecycle(snapshot),
                    None => missing("setupLifecycle"),
                }
            })
            .await
        }
    }
}

/// One page of tasks, with the revision that identifies it.
///
/// `cursorStatus` is printed because a paged read can be answered against a list
/// that has since been rewritten: a `stale` cursor means these rows were real but
/// the page boundary no longer is, and a caller paging through has to restart
/// rather than trust the next cursor.
fn task_list(snapshot: &TaskListSnapshot) -> String {
    let mut lines = vec![format!(
        "revision {} of {}, {} task(s) of {} [{}]",
        snapshot.revision,
        text::text(&snapshot.task_list_id),
        snapshot.tasks.len(),
        snapshot.total,
        render::wire_tag(&snapshot.cursor_status)
    )];
    if snapshot.tasks.is_empty() {
        lines.push("(no tasks)".to_string());
    }
    for entry in &snapshot.tasks {
        lines.push(task_entry(entry));
    }
    if let Some(cursor) = &snapshot.next_cursor {
        lines.push(format!("next: --cursor {}", text::text(cursor)));
    }
    lines.join("\n")
}

/// One task.
///
/// Dependencies are shown as they are recorded, in both directions. A task that
/// looks stuck is usually waiting on something, and `blockedBy` is the only place
/// that says so.
fn task_entry(task: &Task) -> String {
    let mut line = format!(
        "  [{}] {}  {}",
        render::wire_tag(&task.status),
        text::text(&task.id),
        text::text(&task.subject)
    );
    if let Some(owner) = &task.owner {
        line.push_str(&format!("  ({})", text::text(owner)));
    }
    if !task.blocked_by.is_empty() {
        line.push_str(&format!(
            "\n    blocked by {}",
            task.blocked_by
                .iter()
                .map(text::text)
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !task.blocks.is_empty() {
        line.push_str(&format!(
            "\n    blocks {}",
            task.blocks
                .iter()
                .map(text::text)
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    line
}

/// What one setup invocation did.
///
/// `sourceRequestId` is the interesting field: when it is present, these hooks
/// did NOT run — the reply is a replay of an earlier request's cached result, and
/// saying which request produced it is the difference between "setup succeeded"
/// and "setup already succeeded, earlier, for someone else".
fn setup_run(result: &SetupRunResult) -> String {
    let mut lines = vec![format!(
        "[{} {}] {}  {} hook(s)",
        render::wire_tag(&result.trigger),
        render::wire_tag(&result.status),
        text::text(&result.request_id),
        result.hook_count
    )];
    if let Some(source) = &result.source_request_id {
        lines.push(format!("  replayed from request {}", text::text(source)));
    }
    for context in &result.additional_context {
        lines.push(format!("  context: {}", text::text(context)));
    }
    for message in &result.system_messages {
        lines.push(format!("  system: {}", text::text(message)));
    }
    if let Some(error) = &result.error {
        lines.push(format!("  {}", text::text(error)));
    }
    lines.join("\n")
}

/// Every setup run this session recorded, plus the two boundaries that decide
/// whether another one is even possible.
///
/// `initClosedAt` and the context admission are printed because they are what
/// makes a later `setup run --trigger init` answer `too-late` or
/// `already-completed`. Without them that refusal has no visible cause.
fn setup_lifecycle(snapshot: &SetupLifecycleSnapshot) -> String {
    let mut lines = Vec::new();
    if snapshot.runs.is_empty() {
        lines.push("(no setup runs)".to_string());
    }
    for record in &snapshot.runs {
        lines.push(setup_record(record));
    }
    if let Some(closed) = snapshot.init_closed_at {
        lines.push(format!("init closed at {closed}"));
    }
    if let Some(admission) = &snapshot.context_admission {
        lines.push(format!(
            "context admission: {} [{}]",
            text::text(&admission.request_id),
            render::wire_tag(&admission.status)
        ));
    }
    if let Some(request) = &snapshot.context_consumed_request_id {
        let at = snapshot.context_consumed_at.unwrap_or_default();
        lines.push(format!(
            "context consumed by {} at {at}",
            text::text(request)
        ));
    }
    // Terminal maintenance results outlive their lifecycle records on purpose:
    // the request id stays an exactly-once boundary for the whole session. A
    // caller that cannot find its run above but sees the id here has its answer.
    if !snapshot.idempotency_results.is_empty() {
        lines.push(format!(
            "retained results: {}",
            snapshot
                .idempotency_results
                .keys()
                .map(text::text)
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    lines.join("\n")
}

fn setup_record(record: &SetupRunRecord) -> String {
    let mut lines = vec![format!(
        "[{} {}] {}  {} hook(s), started {}",
        render::wire_tag(&record.trigger),
        render::wire_tag(&record.status),
        text::text(&record.request_id),
        record.hook_count,
        record.started_at
    )];
    for context in &record.additional_context {
        lines.push(format!("  context: {}", text::text(context)));
    }
    for message in &record.system_messages {
        lines.push(format!("  system: {}", text::text(message)));
    }
    if let Some(error) = &record.error {
        lines.push(format!("  {}", text::text(error)));
    }
    lines.join("\n")
}
