//! Thread lifecycle: listing, branching, archiving, naming.
//!
//! Every command here names its thread as an argument, so none of them depend on
//! which thread the invocation happens to be bound to — with two exceptions the
//! protocol forces. `show` reads history, and `get_history` reports the BOUND
//! thread's transcript rather than taking a thread id; `title --generate` runs a
//! titling turn, which likewise needs the thread bound to have something to read.
//! Both therefore bind first and say so, because binding is a side effect the
//! caller can observe on the rest of the invocation.

use klaus_agent_core::runtime_values::{RuntimePath, RuntimeString};
use klaus_agent_protocol::{
    ArchiveSessionResult, BoundSession, DeleteSessionResult, GenerateTitleOpResult, ListOptions,
    ProtocolEvent, RenameThreadResult, ThreadPage, ThreadSummary, UnarchiveSessionResult,
    UpdateThreadHostMetadataResult, UpdateTitleResult,
};
use klaus_agent_server::app_server::methods::{
    EngineListSessions, SessionArchive, SessionCreateSideTask, SessionDelete, SessionFork,
    SessionGenerateTitle, SessionHistory, SessionRenameThread, SessionUnarchive,
    SessionUpdateThreadHostMetadata, SessionUpdateTitle,
};
use klaus_agent_server::app_server::wire;

use crate::cli::{Globals, SessionCommand};
use crate::error::{CliError, CliResult};
use crate::output::Printer;
use crate::render;
use crate::session::{self, CliSession};
use crate::text;
use crate::turn::{ApprovalPolicy, Console, Turn};

use super::{missing, settle, show};

pub async fn run(
    session: &mut CliSession,
    printer: &Printer,
    console: &mut Console,
    globals: &Globals,
    command: &SessionCommand,
) -> CliResult<()> {
    match command {
        SessionCommand::List {
            limit,
            cursor,
            include_archived,
            role,
        } => {
            let options = ListOptions {
                cursor: cursor.as_deref().map(RuntimeString::from),
                limit: *limit,
                // Sent only when set: the store's own default is false, and
                // spelling it out here would stop a future default from applying.
                include_archived: include_archived.then_some(true),
                // Scoped to the invocation's directory only when one was given
                // explicitly. Defaulting it to the process cwd would make
                // `session list` hide threads depending on where it was run,
                // which is the opposite of what a listing is for.
                cwd: globals.cwd.as_deref().map(RuntimePath::from),
                role: role.map(Into::into),
            };
            show::<EngineListSessions>(
                &session.engine,
                printer,
                wire::EngineListSessionsParams {
                    options: Some(text::to_json(&options)?),
                    submission_id: None,
                },
                |event| match &event.sessions {
                    Some(page) => thread_page(page),
                    None => missing("sessions"),
                },
            )
            .await
        }

        SessionCommand::Show { thread_id } => {
            session.bind(thread_id).await?;
            show::<SessionHistory>(&session.engine, printer, session.engine.scope(), |event| {
                match &event.history {
                    Some(messages) => render::history(messages),
                    None => missing("history"),
                }
            })
            .await
        }

        SessionCommand::Fork {
            thread_id,
            new_thread_id,
            before_message,
            inherit_goal,
        } => {
            let event = session
                .engine
                .settle::<SessionFork>(
                    wire::SessionThreadParams {
                        inherit_goal: inherit_goal.then_some(true),
                        session_id: Some(session.engine.session_id().to_string()),
                        thread_id: thread_id.clone(),
                        new_thread_id: new_thread_id.clone(),
                        before_message_uuid: before_message.clone(),
                        after_message_uuid: None,
                        last_turn_id: None,
                        before_turn_id: None,
                        submission_id: None,
                    },
                    |event| event.session.is_some(),
                )
                .await?;
            bound(printer, &event)
        }

        SessionCommand::SideTask {
            thread_id,
            new_thread_id,
        } => {
            show::<SessionCreateSideTask>(
                &session.engine,
                printer,
                wire::SessionCreateSideTaskParams {
                    session_id: session.engine.session_id().to_string(),
                    thread_id: thread_id.clone(),
                    new_thread_id: new_thread_id.clone(),
                    submission_id: None,
                },
                bound_text,
            )
            .await
        }

        SessionCommand::Archive { thread_id } => {
            settle::<SessionArchive>(
                &session.engine,
                printer,
                thread_params(session, thread_id),
                |event| event.archived.is_some(),
                |event| match &event.archived {
                    Some(result) => archived(result),
                    None => missing("archived"),
                },
            )
            .await
        }

        SessionCommand::Unarchive { thread_id } => {
            settle::<SessionUnarchive>(
                &session.engine,
                printer,
                thread_params(session, thread_id),
                |event| event.unarchived.is_some(),
                |event| match &event.unarchived {
                    Some(result) => unarchived(result),
                    None => missing("unarchived"),
                },
            )
            .await
        }

        SessionCommand::Delete { thread_id } => {
            settle::<SessionDelete>(
                &session.engine,
                printer,
                thread_params(session, thread_id),
                |event| event.deleted.is_some(),
                |event| match &event.deleted {
                    Some(result) => deleted(result),
                    None => missing("deleted"),
                },
            )
            .await
        }

        SessionCommand::Rename {
            thread_id,
            new_thread_id,
        } => {
            settle::<SessionRenameThread>(
                &session.engine,
                printer,
                wire::SessionRenameThreadParams {
                    session_id: Some(session.engine.session_id().to_string()),
                    thread_id: thread_id.clone(),
                    new_thread_id: new_thread_id.clone(),
                    submission_id: None,
                },
                |event| event.renamed.is_some(),
                |event| match &event.renamed {
                    Some(result) => renamed(result),
                    None => missing("renamed"),
                },
            )
            .await
        }

        SessionCommand::Title {
            thread_id,
            title,
            generate,
            persist,
            instructions,
        } => {
            if *generate {
                // The titling turn reads the bound thread's transcript, so the
                // thread argument has to become the binding before the call is
                // made — `session/generateTitle` carries no thread id of its own.
                session.bind(thread_id).await?;
                // Driven as a turn, not a plain call: writing a title spends
                // model calls, so it is worth being able to stop. Ctrl-C cancels
                // the titling alone — the engine tracks it as detached read-only
                // work, and reaching for the session's turn instead would stop
                // whatever else the thread is doing.
                let submission_id = session.engine.cancellable_submission_id("title");
                let mut turn = Turn::new(
                    &session.engine,
                    printer,
                    console,
                    ApprovalPolicy::interactive(printer, false),
                )
                .cancelling();
                let reply = turn
                    .query::<SessionGenerateTitle>(
                        wire::SessionGenerateTitleParams {
                            session_id: Some(session.engine.session_id().to_string()),
                            instructions: instructions.as_deref().map(Into::into),
                            prompt: None,
                            persist: persist.then_some(true),
                            submission_id: Some(submission_id.clone()),
                        },
                        &submission_id,
                    )
                    .await?;
                if printer.is_json() {
                    return printer.raw_json(&reply);
                }
                let event = crate::engine::decode_reply(&reply)?;
                return printer.value(event.as_ref(), |event| match &event.generated_title {
                    Some(result) => generated_title(result),
                    None => missing("generatedTitle"),
                });
            }
            let title = title.as_deref().ok_or_else(|| {
                CliError::usage(
                    "pass the title as an argument, or --generate to have the engine write one"
                        .to_string(),
                )
            })?;
            settle::<SessionUpdateTitle>(
                &session.engine,
                printer,
                wire::SessionUpdateTitleParams {
                    session_id: Some(session.engine.session_id().to_string()),
                    thread_id: thread_id.clone(),
                    title: title.into(),
                    submission_id: None,
                },
                |event| event.title.is_some(),
                |event| match &event.title {
                    Some(result) => title_updated(result),
                    None => missing("title"),
                },
            )
            .await
        }

        SessionCommand::Metadata {
            thread_id,
            metadata,
        } => {
            // An absent argument clears the attachment rather than being
            // rejected: the op models "no metadata" as the absent field, so
            // there is no other way to spell removal.
            let host_metadata = match metadata {
                Some(raw) => Some(text::to_json(&session::json_argument(raw).await?)?),
                None => None,
            };
            settle::<SessionUpdateThreadHostMetadata>(
                &session.engine,
                printer,
                wire::SessionUpdateThreadHostMetadataParams {
                    session_id: Some(session.engine.session_id().to_string()),
                    thread_id: thread_id.clone(),
                    host_metadata,
                    submission_id: None,
                },
                |event| event.host_metadata_updated.is_some(),
                |event| match &event.host_metadata_updated {
                    Some(result) => metadata_updated(result),
                    None => missing("hostMetadataUpdated"),
                },
            )
            .await
        }
    }
}

/// Every lifecycle method that names a thread and nothing else.
fn thread_params(session: &CliSession, thread_id: &str) -> wire::SessionThreadParams {
    wire::SessionThreadParams {
        inherit_goal: None,
        session_id: Some(session.engine.session_id().to_string()),
        thread_id: thread_id.to_string(),
        // Fork's two extra fields, which the others are documented to ignore.
        new_thread_id: None,
        before_message_uuid: None,
        after_message_uuid: None,
        last_turn_id: None,
        before_turn_id: None,
        submission_id: None,
    }
}

/// Fork and side-task reply with the same shape, and mean the same thing to the
/// caller: a new thread exists and this invocation is now bound to it.
///
/// They reach that reply differently — one is acked and streams it, the other
/// answers with it — so this takes the settled envelope rather than making the
/// call, and each arm above submits in the shape its own method declares.
fn bound(printer: &Printer, event: &ProtocolEvent) -> CliResult<()> {
    printer.value(event, bound_text)
}

fn bound_text(event: &ProtocolEvent) -> String {
    match &event.session {
        Some(session) => bound_session(session),
        None => missing("session"),
    }
}

fn bound_session(session: &BoundSession) -> String {
    match &session.forked_from_id {
        Some(source) => format!(
            "{} (forked from {})",
            text::text(&session.thread_id),
            text::text(source)
        ),
        None => text::text(&session.thread_id),
    }
}

/// One line per thread, plus the cursor when the listing is not exhausted.
///
/// The cursor is printed because paging is the caller's job here: there is no
/// scrollback state to hold it, so a listing that ended mid-list has to hand back
/// the token for the next `--cursor`.
fn thread_page(page: &ThreadPage) -> String {
    if page.items.is_empty() {
        return "(no sessions)".to_string();
    }
    let mut lines: Vec<String> = page.items.iter().map(thread_summary).collect();
    if let Some(cursor) = &page.next_cursor {
        lines.push(format!("next: --cursor {}", text::text(cursor)));
    }
    lines.join("\n")
}

fn thread_summary(thread: &ThreadSummary) -> String {
    let mut tags = vec![render::wire_tag(&thread.role)];
    if thread.archived == Some(true) {
        tags.push("archived".to_string());
    }
    if let Some(parent) = &thread.parent_thread_id {
        tags.push(format!("parent {}", text::text(parent)));
    }
    if let Some(source) = &thread.forked_from_id {
        tags.push(format!("forked from {}", text::text(source)));
    }
    format!(
        "{}  {}  [{}]  {}",
        text::text(&thread.id),
        render::label(thread.title.as_ref(), "(untitled)"),
        tags.join(", "),
        text::text(&thread.cwd)
    )
}

fn archived(result: &ArchiveSessionResult) -> String {
    let thread_id = text::text(&result.thread_id);
    if result.archived {
        return format!("{thread_id} archived");
    }
    format!("{thread_id} was not archived")
}

/// Unarchiving reports a status rather than a boolean, and every status other
/// than the two success cases is a refusal the caller has to see: a running or
/// concurrently-modified thread was left exactly as it was.
fn unarchived(result: &UnarchiveSessionResult) -> String {
    let mut line = format!(
        "{} {}",
        text::text(&result.thread_id),
        render::wire_tag(&result.status)
    );
    if let Some(error) = &result.error {
        line.push_str(&format!(": {}", text::text(error)));
    }
    match &result.thread {
        Some(thread) => format!("{line}\n{}", thread_summary(thread)),
        None => line,
    }
}

fn deleted(result: &DeleteSessionResult) -> String {
    let thread_id = text::text(&result.thread_id);
    if result.deleted {
        return format!("{thread_id} deleted");
    }
    format!("{thread_id} was not deleted")
}

fn renamed(result: &RenameThreadResult) -> String {
    let thread_id = text::text(&result.thread_id);
    if result.renamed {
        return format!("renamed to {thread_id}");
    }
    format!("{thread_id} was not renamed")
}

fn title_updated(result: &UpdateTitleResult) -> String {
    format!(
        "{}: {}",
        text::text(&result.thread_id),
        text::text(&result.title)
    )
}

/// A generated title, and whether it stuck.
///
/// `raw` is shown only when no title was extracted from it, which is the one case
/// where it explains something: the turn produced text the engine could not read
/// as a title.
fn generated_title(result: &GenerateTitleOpResult) -> String {
    let thread_id = text::text(&result.thread_id);
    let title = &result.title;
    let mut lines = vec![match &title.title {
        Some(value) => format!("{thread_id}: {}", text::text(value)),
        None => format!("{thread_id}: no title (raw: {})", text::text(&title.raw)),
    }];
    if !title.persisted {
        lines.push("[not persisted] pass --persist to keep it".to_string());
    }
    if let Some(usage) = &title.usage {
        lines.push(format!("[{}]", render::usage_summary(usage)));
    }
    lines.join("\n")
}

fn metadata_updated(result: &UpdateThreadHostMetadataResult) -> String {
    let thread_id = text::text(&result.thread_id);
    if result.updated {
        return format!("{thread_id} metadata updated");
    }
    format!("{thread_id} metadata unchanged")
}
