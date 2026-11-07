//! Each subcommand, mapped onto the engine methods it calls.
//!
//! The split below follows the facade's method families rather than product
//! areas, for the same reason [`crate::cli`] does: a new method lands in an
//! obvious file.
//!
//! Two shapes of command live here and they are driven differently.
//!
//! A TURN command starts a turn and then follows the event stream, so it goes
//! through [`crate::turn`]. A MANAGEMENT command calls one method and prints its
//! single reply, so it goes through [`show`] or [`settle`] — whichever the
//! method's declared result says it is.
//!
//! Management replies are printed from the reply ENVELOPE, not from a typed
//! projection of one field. That is deliberate: `--json` must emit what the
//! engine actually said, and projecting first would discard everything else the
//! engine correlated to the call — including the fields whose Rust types have no
//! wire encoder of their own, which would otherwise be unprintable as JSON.

pub mod auth;
pub mod engine;
pub mod goal;
pub mod inspect;
pub mod mcp;
pub mod memory;
pub mod passthrough;
pub mod plugin;
pub mod run;
pub mod session;
pub mod task;
pub mod thread;

use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_protocol::ProtocolEvent;
use klaus_agent_server::app_server::wire;
use klaus_agent_server::Method;

use crate::cli::{Cli, Command};
use crate::engine::EngineClient;
use crate::error::CliResult;
use crate::output::Printer;
use crate::session::{CliEndpoint, CliSession};
use crate::turn::Console;

/// Run the parsed command line.
pub async fn dispatch(mut cli: Cli, process_context: RuntimeProcessContext) -> CliResult<()> {
    let printer = Printer::new(cli.globals.json);

    // These four own their own transport: they either need no engine at all, or
    // they hand the engine's protocol straight to someone else instead of
    // driving it themselves. Opening a client session for them would put a
    // second speaker on a wire that is supposed to have one.
    match &cli.command {
        Some(Command::Completion { shell }) => return passthrough::completion(*shell),
        Some(Command::Proto) => return passthrough::proto(&cli.globals, process_context).await,
        Some(Command::Serve(args)) => {
            return passthrough::serve(&cli.globals, args, process_context).await
        }
        Some(Command::AppServer { schema }) => {
            return passthrough::app_server(&cli.globals, *schema, process_context).await
        }
        _ => {}
    }

    // Signing in touches credential records and nothing else. Starting an engine
    // for it would be worse than wasteful: a machine with no model configured yet
    // is exactly the machine someone runs `klaus login` on, and a session that
    // cannot resolve a provider would fail before the login could fix that.
    match &cli.command {
        Some(Command::Login { source }) => {
            return auth::login(*source, &printer, &process_context).await
        }
        Some(Command::Logout { source }) => return auth::logout(*source, &printer),
        Some(Command::Auth { command }) => {
            return match command {
                crate::cli::AuthCommand::Status => auth::status(&printer).await,
            }
        }
        _ => {}
    }

    // Endpoint state, so no session either — but for the opposite reason to the
    // four above. These DO drive the engine, through the same declared method
    // table every other command uses; they just name methods that take no
    // session id, because the config file and the rule store outlive every
    // session. Starting one would boot MCP children to print a config value.
    if let Some(Command::Engine { command }) = &cli.command {
        let endpoint = CliEndpoint::open(&cli.globals, process_context).await?;
        let outcome = engine::run(&endpoint, &printer, &cli.globals, command).await;
        endpoint.close().await;
        return outcome;
    }

    // Asking about the goal IS asking for the goal runtime. The engine only
    // builds one when goal mode is on, and without it this whole group is a
    // façade: `set` and `clear` are refused as "goal mode is not enabled for this
    // session", and `get` is worse — it answers "(no goal)" for a thread whose
    // goal is sitting on disk, because there is no runtime to read the record.
    //
    // Goal controls enable the runtime. Creating or resuming an active goal
    // admits its next turn inside the engine after the state reply is written.
    if matches!(cli.command, Some(Command::Goal { .. })) {
        cli.globals.goal_mode = true;
    }

    let mut session = CliSession::open(&cli.globals, process_context, &printer).await?;
    let mut console = Console::new();
    let outcome = route(&cli, &mut session, &printer, &mut console).await;
    // Teardown runs even when the command failed, and its own failure is not
    // allowed to mask the command's: an engine that could not flush is worth
    // reporting, but not instead of the reason the user ran this.
    let closed = session.close().await;
    outcome.and(closed)
}

async fn route(
    cli: &Cli,
    session: &mut CliSession,
    printer: &Printer,
    console: &mut Console,
) -> CliResult<()> {
    let globals = &cli.globals;
    match &cli.command {
        // A bare prompt is the REPL's first turn.
        None => run::repl(session, printer, console, globals, cli.prompt.clone()).await,
        Some(command) => match command {
            Command::Repl { prompt } => {
                run::repl(session, printer, console, globals, prompt.clone()).await
            }
            Command::Exec(args) => run::exec(session, printer, console, args).await,
            Command::Ask { question } => run::ask(session, printer, console, question).await,

            Command::Session { command } => {
                session::run(session, printer, console, globals, command).await
            }
            Command::Mcp { command } => mcp::run(session, printer, command).await,
            Command::Plugin { command } => {
                plugin::run(&session.engine, printer, globals, command).await
            }

            Command::Skill { .. } => inspect::skills(&session.engine, printer).await,
            Command::Agent { .. } => inspect::agents(&session.engine, printer).await,
            Command::Hook { .. } => inspect::hooks(&session.engine, printer).await,
            Command::Extensions => inspect::extensions(&session.engine, printer).await,
            Command::Capabilities => inspect::capabilities(&session.engine, printer).await,

            Command::Goal { command } => goal::goal(&session.engine, printer, command).await,
            Command::Queue { command } => goal::queue(&session.engine, printer, command).await,

            Command::Memory { command } => memory::run(&session.engine, printer, command).await,
            Command::Subagent { command } => {
                memory::subagent(&session.engine, printer, command).await
            }

            Command::Task { command } => task::task(&session.engine, printer, command).await,
            Command::Setup { command } => task::setup(&session.engine, printer, command).await,

            Command::Compact { instructions } => {
                thread::compact(&session.engine, printer, console, instructions.as_deref()).await
            }
            Command::Review(args) => thread::review(&session.engine, printer, console, args).await,
            Command::Rewind {
                turns,
                scope,
                dry_run,
            } => thread::rewind(&session.engine, printer, *turns, *scope, *dry_run).await,
            Command::Cost => thread::cost(&session.engine, printer).await,
            Command::Context => thread::context(&session.engine, printer).await,
            Command::History => thread::history(&session.engine, printer).await,
            Command::Changes => thread::changes(&session.engine, printer).await,

            // Routed before the session was opened.
            Command::Engine { .. }
            | Command::Login { .. }
            | Command::Logout { .. }
            | Command::Auth { .. }
            | Command::Proto
            | Command::Serve(_)
            | Command::AppServer { .. }
            | Command::Completion { .. } => Ok(()),
        },
    }
}

/// Call one direct-result method and print its reply: the envelope's own JSON
/// under `--json`, and `render`'s text otherwise.
///
/// The engine has already settled the request by the time it answers, so there
/// is nothing to wait for and no predicate to write.
pub(crate) async fn show<M>(
    engine: &EngineClient,
    printer: &Printer,
    params: M::Params,
    render: impl FnOnce(&ProtocolEvent) -> String,
) -> CliResult<()>
where
    M: Method,
    M::Result: serde::Serialize,
{
    if printer.is_json() {
        let reply = engine.reply::<M>(params).await?;
        return printer.raw_json(&reply);
    }
    let event = engine.query::<M>(params).await?;
    printer.value(event.as_ref(), render)
}

/// Call one ack-reply method and print the streamed envelope that answers it.
///
/// `ready` is the caller's because the engine decides how much it says under one
/// submission id: a rename replies once, while compaction and review report
/// progress first, so "the first envelope with this id" is the wrong answer for
/// them.
pub(crate) async fn settle<M>(
    engine: &EngineClient,
    printer: &Printer,
    params: M::Params,
    ready: impl Fn(&ProtocolEvent) -> bool,
    render: impl FnOnce(&ProtocolEvent) -> String,
) -> CliResult<()>
where
    M: Method<Result = wire::SubmissionAck>,
{
    let event = engine.settle::<M>(params, ready).await?;
    printer.value(event.as_ref(), render)
}

/// A reply field the engine promised but did not send. Reported rather than
/// papered over: the `ready` predicate above already matched on it, so an absent
/// field here means the predicate and the renderer disagree.
pub(crate) fn missing(field: &str) -> String {
    format!("[warning] the reply carried no `{field}`")
}
