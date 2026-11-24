//! The three commands that spend a turn.
//!
//! `repl` and `exec` submit the same `run` op and differ only in who answers the
//! questions it raises: a terminal user, or nobody. `ask` is not a turn on this
//! thread at all — `side_question` runs an isolated turn whose messages never
//! join the conversation, which is why it is here rather than in `thread`.

use clap::ValueEnum as _;

use klaus_agent_core::engine::session::SideQuestionResult;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::config::WebSearchAccess;
use klaus_agent_core::types::permission::{
    ApprovalPolicy as CoreApprovalPolicy, ExternalPermissionMode,
};
use klaus_agent_core::types::provider::{OutputVerbosity, ReasoningSummary};
use klaus_agent_protocol::types::Input;
use klaus_agent_protocol::{
    ProtocolSessionConfigUpdate, ProtocolSessionConfigUpdateResult, RunOpOptions,
};
use klaus_agent_server::app_server::methods::{
    ConfigRead, SessionContextSnapshot, SessionSetCollaborationMode, SessionSetModel,
    SessionSetRuntimePolicy, SessionSideQuestion, SessionSubagents, SessionUpdateConfig,
};
use klaus_agent_server::app_server::wire;
use serde_json::Value;

use crate::cli::{CollaborationModeArg, EffortArg, ExecArgs, Globals, PermissionModeArg};
use crate::error::{CliError, CliResult};
use crate::model::ModelRef;
use crate::output::Printer;
use crate::render;
use crate::session::{self, CliSession};
use crate::text;
use crate::tui;
use crate::turn::{self, ApprovalPolicy, Console, Turn, Typed};

use super::{missing, settle};

/// What the REPL does after a slash command.
enum Slash {
    Prompt,
    Leave,
}

/// The slash commands, as printed by `/help`.
const HELP: &str = "\
commands\n  \
/help                          this list\n  \
/model <id|source:model>       switch model or source, from the next turn on\n  \
/permissions sandbox=<policy> approval=<policy>  update session permissions\n  \
/collaboration <plan|default>  update planning independently\n  \
/plan [on|off]                 toggle or set plan mode\n  \
/parallel [on|off]             toggle or set parallel sub-agent decomposition\n  \
/goal [on|off]                 toggle or set autonomous goal mode\n  \
/orchestrate [on|off|src:id]   toggle orchestration or set executor model\n  \
/config key=value ...          update model, parallel, goal, verbosity, summary, web-search\n  \
/exit, /quit                   leave\n\
keys\n  \
ctrl-c                         interrupt the running turn (twice: stop waiting)\n  \
ctrl-d                         leave\n\
a line typed while a turn is running is steered into that turn.";

/// Public commands offered by completion and documented by `/help`.
/// Deprecated input aliases remain accepted without being advertised.
fn slash_commands() -> Vec<tui::menu::Candidate> {
    use tui::menu::Candidate;
    vec![
        Candidate::new("help", "this list"),
        Candidate::new("model", "switch model or source, from the next turn on"),
        Candidate::new("permissions", "update the session runtime policy"),
        Candidate::new("collaboration", "update planning independently"),
        Candidate::new("plan", "toggle or set plan mode"),
        Candidate::new("parallel", "toggle or set parallel mode"),
        Candidate::new("goal", "toggle or set autonomous goal mode"),
        Candidate::new("orchestrate", "toggle orchestration or set executor model"),
        Candidate::new("config", "update mutable session config fields"),
        Candidate::new("exit", "leave"),
        Candidate::new("quit", "leave"),
    ]
}

/// The banner: what this session is, in a box, before the first prompt.
///
/// Boxed rather than printed as loose lines because it is the one thing on screen
/// that is not conversation, and a frame is what says so without a heading.
fn banner(printer: &Printer, session: &CliSession, columns: usize) {
    let painter = printer.painter();
    let palette = painter.palette();
    let body = vec![
        painter.spans(&[
            (palette.brand, "Klaus"),
            (palette.meta, "  the engine, in your terminal"),
        ]),
        String::new(),
        painter.spans(&[
            (palette.meta, "thread  "),
            (palette.body, &session.thread_label()),
        ]),
        painter.spans(&[
            (palette.meta, "keys    "),
            (
                palette.body,
                "/help for commands · ctrl-j newline · ctrl-d to leave",
            ),
        ]),
    ];
    for line in tui::panel::Panel::new(palette.brand)
        .pad_rows(1)
        .pad_cols(2)
        .render(painter, &body, columns.min(72))
    {
        printer.note(&line);
    }
}

/// Interactive session: one turn per line typed, until end of input.
///
/// A failed turn does not end the loop. That is the whole point of a REPL — the
/// engine refusing one prompt leaves the session, its history and its MCP
/// connections intact, so exiting would throw away work the user still wants. A
/// dead transport is different and does end it, which is why those two failures
/// are separate error variants.
///
/// Two input paths, chosen once by whether there is a terminal on both ends. The
/// interactive one draws an input area and reads keys; the line one reads lines.
/// They are not two implementations of the same thing: without a terminal there
/// are no key events to read at all, so this is a real fork rather than a flag.
pub async fn repl(
    session: &mut CliSession,
    printer: &Printer,
    console: &mut Console,
    globals: &Globals,
    first: Option<String>,
) -> CliResult<()> {
    let columns = printer.columns();
    if tui::shell::is_available() {
        banner(printer, session, columns);
    } else {
        printer.note(&format!(
            "klaus: thread {} — /help for commands, ctrl-d or /exit to leave",
            session.thread_label()
        ));
    }

    let policy = ApprovalPolicy::interactive(printer, false);
    // Steering reads stdin while the turn runs, which is only sound when the
    // lines on it are being typed. On a pipe the next line is the next PROMPT,
    // and steering with it would drop a turn of the script.
    let steerable = printer.can_prompt();
    let mut shell = tui::shell::is_available().then(|| tui::shell::Shell::new(slash_commands()));

    let mut prompt = first;
    let mut active_globals = globals.clone();
    // The toolbar's facts, resolved once and then kept in step by the slash
    // commands that change them: re-reading per prompt would put a round trip in
    // front of every keystroke, and leaving it at the invocation's flags is what
    // made `/model` change the session without changing what the toolbar claimed.
    let mut status = shell_status(session, &active_globals).await;
    // Ctrl-C at the prompt does not leave on the first press: there is no turn to
    // interrupt there, and quitting on a keystroke that means "stop that" in
    // every other context would be a trap. Two in a row is unambiguous.
    let mut leaving = false;
    loop {
        let text_input = match prompt.take() {
            Some(text_input) => text_input,
            None => {
                if shell.is_some() {
                    refresh_live_toolbar(session, &mut status).await;
                }
                match read_prompt(shell.as_mut(), printer, console, session, &status).await? {
                    Typed::Line(line) => {
                        leaving = false;
                        line
                    }
                    // End of input is how a piped script says it is done.
                    Typed::End => return Ok(()),
                    Typed::Interrupted if leaving => return Ok(()),
                    Typed::Interrupted => {
                        leaving = true;
                        printer.note("klaus: ctrl-c again to leave, or /exit");
                        continue;
                    }
                }
            }
        };
        let text_input = text_input.trim();
        if text_input.is_empty() {
            continue;
        }
        // Echo what was submitted. The interactive path erased its input area on
        // the way out, so without this the prompt that produced a reply is gone
        // from the scrollback and the transcript reads as answers with no
        // questions.
        if shell.is_some() {
            let painter = printer.painter();
            let palette = painter.palette();
            printer.line(&painter.spans(&[
                (palette.active, tui::glyph::USER),
                (palette.body, " "),
                (palette.body, text_input),
            ]))?;
        }
        if let Some(command) = text_input.strip_prefix('/') {
            match slash(session, &mut active_globals, printer, command, &mut status).await? {
                Slash::Prompt => continue,
                Slash::Leave => return Ok(()),
            }
        }

        let mut turn = Turn::new(&session.engine, printer, console, policy);
        if steerable {
            turn = turn.steerable();
        }
        match turn
            .run(Input::Text(RuntimeString::from(text_input)), None)
            .await
        {
            Ok(_) => {}
            Err(CliError::Turn(message)) => printer.note(&format!("klaus: {message}")),
            Err(error) => return Err(error),
        }
    }
}

/// Read one prompt from whichever input path this session has.
async fn read_prompt(
    shell: Option<&mut tui::shell::Shell>,
    printer: &Printer,
    console: &mut Console,
    session: &CliSession,
    status: &tui::toolbar::Status,
) -> CliResult<Typed> {
    let Some(shell) = shell else {
        let _ = session;
        return console.ask_or_interrupt(printer, "\n> ").await;
    };
    Ok(
        match shell.prompt(printer.painter(), status, printer.columns())? {
            tui::shell::Submitted::Line(line) => Typed::Line(line),
            tui::shell::Submitted::Interrupted => Typed::Interrupted,
            tui::shell::Submitted::End => Typed::End,
        },
    )
}

/// Read the canonical configuration once when the REPL opens, then apply the
/// invocation's explicit initialization overrides. Slash commands update the
/// cached display after their requests settle; typing needs no round trip.
/// Planning is displayed independently from the filesystem policy.
async fn shell_status(session: &CliSession, globals: &Globals) -> tui::toolbar::Status {
    let mut status = tui::toolbar::Status {
        mode: "read-only".to_string(),
        cwd: session.cwd.clone(),
        ..tui::toolbar::Status::default()
    };
    // A config the endpoint cannot read is not worth failing a session over: the
    // toolbar then shows what the flags said and nothing where the model goes,
    // which is the honest answer when the engine did not supply one.
    if let Ok(config) = session
        .engine
        .call::<ConfigRead>(wire::ConfigReadParams {
            file_path: None,
            cwd: Some(session.cwd.clone()),
            submission_id: None,
        })
        .await
    {
        if let Some(resolved) = config.resolved.as_object() {
            if let Some(model) = resolved.get("model").and_then(Value::as_str) {
                status.model = model.to_string();
            }
            if let Some(mode) = resolved.get("sandbox_mode").and_then(Value::as_str) {
                status.mode = mode.to_string();
            }
            if let Some(thinking) = resolved.get("thinking").and_then(Value::as_bool) {
                status.thinking = thinking;
            }
            if let Some(effort) = resolved.get("effort").and_then(Value::as_str) {
                status.thinking = effort != "none";
            }
            if resolved.get("approval_policy").and_then(Value::as_str) == Some("never") {
                status.badges.auto_approve = true;
            }
            if resolved
                .get("ultra_mode")
                .or_else(|| resolved.get("ultraMode"))
                .and_then(Value::as_bool)
                == Some(true)
            {
                status.badges.parallel = true;
            }
            if resolved.get("goal").and_then(Value::as_bool) == Some(true) {
                status.badges.unattended = true;
            }
        }
    }
    // The flags win over the resolved view: they are this session's init, and the
    // engine applied them on top of the same config.
    if let Some(model) = &globals.model {
        status.model = model.clone();
    }
    if let Some(effort) = globals.effort {
        status.thinking = !matches!(effort, EffortArg::None);
    }
    if let Ok(Some(config)) = session::session_config(globals) {
        if let Some(mode) = config
            .get("sandbox_mode")
            .or_else(|| config.get("sandboxMode"))
            .and_then(|value| value.as_string())
        {
            status.mode = text::text(mode);
        }
        if let Some(policy) = config
            .get("approval_policy")
            .or_else(|| config.get("approvalPolicy"))
            .and_then(|value| value.as_string())
        {
            status.badges.auto_approve = text::text(policy) == "never";
        }
        if config
            .get("ultra_mode")
            .or_else(|| config.get("ultraMode"))
            .and_then(|value| value.as_bool())
            == Some(true)
        {
            status.badges.parallel = true;
        }
        if config.get("goal").and_then(|value| value.as_bool()) == Some(true) {
            status.badges.unattended = true;
        }
    }
    status.badges.plan = match globals.collaboration_mode {
        Some(CollaborationModeArg::Plan) => true,
        Some(CollaborationModeArg::Default) => false,
        None => matches!(globals.permission_mode, Some(PermissionModeArg::Plan)),
    };
    status.badges.orchestrate = globals.orchestrate || globals.orchestrator_model.is_some();
    if globals.parallel {
        status.badges.parallel = true;
    }
    if globals.goal_mode {
        status.badges.unattended = true;
    }
    status
}

/// Refresh dynamic session telemetry (git tree, context occupancy, live
/// background sub-agents) right before drawing the interactive toolbar.
async fn refresh_live_toolbar(session: &CliSession, status: &mut tui::toolbar::Status) {
    status.git = read_git_status(&session.cwd);
    if let Ok(event) = session
        .engine
        .query::<SessionContextSnapshot>(session.engine.scope())
        .await
    {
        if let Some(snapshot) = &event.context {
            if snapshot.context_window > 0 {
                let used = snapshot.total_tokens as u64;
                let total = snapshot.context_window as u64;
                status.context = Some((used as f64 / total as f64, used, total));
            }
        }
    }
    if let Ok(event) = session
        .engine
        .query::<SessionSubagents>(session.engine.scope())
        .await
    {
        status.tasks = event
            .subagents
            .as_ref()
            .filter(|items| !items.is_empty())
            .map(|items| ("agents".to_string(), items.len()));
    }
}

fn read_git_status(cwd: &str) -> Option<tui::toolbar::Git> {
    let output = std::process::Command::new("git")
        .args(["-C", cwd, "status", "--porcelain=v2", "--branch"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    parse_git_porcelain_v2(&String::from_utf8_lossy(&output.stdout))
}

fn parse_git_porcelain_v2(stdout: &str) -> Option<tui::toolbar::Git> {
    let mut git = tui::toolbar::Git::default();
    for line in stdout.lines() {
        if let Some(head) = line.strip_prefix("# branch.head ") {
            let head = head.trim();
            git.branch = if head == "(detached)" {
                "HEAD".to_string()
            } else {
                head.to_string()
            };
        } else if let Some(ab) = line.strip_prefix("# branch.ab ") {
            for token in ab.split_whitespace() {
                if let Some(ahead) = token.strip_prefix('+') {
                    git.ahead = ahead.parse().unwrap_or(0);
                } else if let Some(behind) = token.strip_prefix('-') {
                    git.behind = behind.parse().unwrap_or(0);
                }
            }
        } else if !line.starts_with('#') && !line.trim().is_empty() {
            git.dirty = true;
        }
    }
    (!git.branch.is_empty()).then_some(git)
}

/// Apply local session commands. Model changes retain their ordered next-turn
/// behavior. Runtime policy and collaboration updates wait for their own apply
/// acknowledgements before updating the toolbar. Deprecated preset aliases are
/// translated to those canonical requests at this input boundary.
async fn slash(
    session: &mut CliSession,
    globals: &mut Globals,
    printer: &Printer,
    command: &str,
    status: &mut tui::toolbar::Status,
) -> CliResult<Slash> {
    let (name, rest) = match command.split_once(char::is_whitespace) {
        Some((name, rest)) => (name, rest.trim()),
        None => (command, ""),
    };
    match name {
        "exit" | "quit" => return Ok(Slash::Leave),
        "help" | "?" => printer.note(HELP),
        "model" if rest.is_empty() => printer.note("klaus: /model needs a model id"),
        "model" => match ModelRef::parse(rest) {
            ModelRef::Selection(selection) => {
                let selection = selection.to_string();
                match session
                    .reopen_with_selection(globals, &selection, printer)
                    .await
                {
                    Ok(()) => {
                        globals.model = Some(selection.clone());
                        status.model = selection.clone();
                        printer.note(&format!(
                            "klaus: {selection} — reopened this thread on it, from the next turn"
                        ));
                    }
                    Err(error) => printer.note(&format!(
                        "klaus: {selection} was not applied ({error}) — still on {}",
                        status.model
                    )),
                }
            }
            ModelRef::Name(model) => {
                session
                    .engine
                    .ack::<SessionSetModel>(wire::SessionSetModelParams {
                        session_id: session.engine.session_id().to_string(),
                        model: model.to_string(),
                        submission_id: None,
                    })
                    .await?;
                globals.model = Some(model.to_string());
                status.model = model.to_string();
                printer.note(&format!(
                    "klaus: model {model} — applies from the next turn"
                ));
            }
        },
        "permissions" => {
            let mut policy = empty_policy(session);
            for field in rest.split_whitespace() {
                match field.split_once('=') {
                    Some(("sandbox", value)) => policy.sandbox_mode = Some(value.to_string()),
                    Some(("approval", value)) => {
                        policy.approval_policy = Some(
                            serde_json::from_value(Value::String(value.into()))
                                .map_err(|error| CliError::usage(error.to_string()))?,
                        );
                    }
                    Some(("reviewer", value)) => {
                        policy.approvals_reviewer = Some(value.to_string())
                    }
                    _ => return Err(CliError::usage(
                        "/permissions takes sandbox=<policy> approval=<policy> reviewer=<reviewer>",
                    )),
                }
            }
            if policy.sandbox_mode.is_none()
                && policy.approval_policy.is_none()
                && policy.approvals_reviewer.is_none()
            {
                return Err(CliError::usage(
                    "/permissions needs at least one policy field",
                ));
            }
            session
                .engine
                .ack::<SessionSetRuntimePolicy>(policy.clone())
                .await?;
            if let Some(mode) = policy.sandbox_mode {
                status.mode = mode;
            }
            if let Some(approval) = policy.approval_policy {
                status.badges.auto_approve = matches!(approval, CoreApprovalPolicy::Never);
            }
            printer.note("klaus: runtime policy updated");
        }
        "collaboration" => {
            let mode = match rest {
                "plan" => wire::CollaborationMode::Plan,
                "default" => wire::CollaborationMode::Default,
                _ => return Err(CliError::usage("/collaboration wants plan or default")),
            };
            apply_collaboration_mode(session, globals, status, mode).await?;
            printer.note("klaus: collaboration mode updated");
        }
        "plan" => {
            let next_plan = parse_toggle_arg(rest, status.badges.plan, "/plan")?;
            let mode = if next_plan {
                wire::CollaborationMode::Plan
            } else {
                wire::CollaborationMode::Default
            };
            apply_collaboration_mode(session, globals, status, mode).await?;
            printer.note(if next_plan {
                "klaus: plan mode on"
            } else {
                "klaus: plan mode off"
            });
        }
        "parallel" | "ultra" => {
            let enabled = parse_toggle_arg(rest, status.badges.parallel, "/parallel")?;
            let update = ProtocolSessionConfigUpdate {
                ultra_mode: Some(enabled),
                ..Default::default()
            };
            submit_session_config_update(session, printer, &update, status).await?;
            globals.parallel = enabled;
        }
        "goal" => {
            let enabled = parse_toggle_arg(rest, status.badges.unattended, "/goal")?;
            let update = ProtocolSessionConfigUpdate {
                goal: Some(enabled),
                ..Default::default()
            };
            submit_session_config_update(session, printer, &update, status).await?;
            globals.goal_mode = enabled;
        }
        "orchestrate" => {
            let mut next = globals.clone();
            let note = match rest {
                "" => {
                    let enabled = !status.badges.orchestrate;
                    next.orchestrate = enabled;
                    if !enabled {
                        next.orchestrator_model = None;
                    }
                    if enabled {
                        "klaus: orchestration mode on — reopened this thread".to_string()
                    } else {
                        "klaus: orchestration mode off — reopened this thread".to_string()
                    }
                }
                "on" | "true" => {
                    next.orchestrate = true;
                    "klaus: orchestration mode on — reopened this thread".to_string()
                }
                "off" | "false" => {
                    next.orchestrate = false;
                    next.orchestrator_model = None;
                    "klaus: orchestration mode off — reopened this thread".to_string()
                }
                target => {
                    session::parse_orchestrator_target(target)?;
                    next.orchestrate = true;
                    next.orchestrator_model = Some(target.to_string());
                    format!(
                        "klaus: orchestration mode on (executor {target}) — reopened this thread"
                    )
                }
            };
            match session.reopen_with_globals(&next, printer).await {
                Ok(()) => {
                    status.badges.orchestrate =
                        next.orchestrate || next.orchestrator_model.is_some();
                    *globals = next;
                    printer.note(&note);
                }
                Err(error) => printer.note(&format!(
                    "klaus: orchestration change was not applied ({error})"
                )),
            }
        }
        "mode" => match permission_mode(rest) {
            Some(mode) => {
                apply_legacy_policy(session, mode).await?;
                set_mode(status, mode);
                printer.note("klaus: deprecated preset translated to explicit policy fields");
            }
            None => printer.note(&format!("klaus: /mode wants one of {}", modes())),
        },
        "config" => config(session, globals, printer, rest, status).await?,
        _ => printer.note(&format!(
            "klaus: no such command /{name} — /help lists them"
        )),
    }
    Ok(Slash::Prompt)
}

async fn apply_collaboration_mode(
    session: &CliSession,
    globals: &mut Globals,
    status: &mut tui::toolbar::Status,
    mode: wire::CollaborationMode,
) -> CliResult<()> {
    let planning = matches!(mode, wire::CollaborationMode::Plan);
    session
        .engine
        .call::<SessionSetCollaborationMode>(wire::SessionCollaborationModeParams {
            session_id: Some(session.engine.session_id().to_string()),
            thread_id: None,
            mode,
            expected_revision: None,
        })
        .await?;
    globals.collaboration_mode = Some(if planning {
        CollaborationModeArg::Plan
    } else {
        CollaborationModeArg::Default
    });
    status.badges.plan = planning;
    Ok(())
}

fn parse_toggle_arg(rest: &str, current: bool, cmd: &str) -> CliResult<bool> {
    match rest {
        "" => Ok(!current),
        "on" | "true" => Ok(true),
        "off" | "false" => Ok(false),
        _ => Err(CliError::usage(format!(
            "{cmd} takes on, off, or no argument to toggle"
        ))),
    }
}

/// Build an explicit policy update without changing fields the user omitted.
fn empty_policy(session: &CliSession) -> wire::SessionSetRuntimePolicyParams {
    wire::SessionSetRuntimePolicyParams {
        session_id: session.engine.session_id().to_string(),
        permission_mode: None,
        pre_plan_permission_mode: None,
        approval_policy: None,
        sandbox_mode: None,
        approvals_reviewer: None,
        sandbox_workspace_write: None,
        network_allowed: None,
        submission_id: None,
    }
}

async fn apply_legacy_policy(session: &CliSession, mode: ExternalPermissionMode) -> CliResult<()> {
    if mode == ExternalPermissionMode::Plan {
        session
            .engine
            .call::<SessionSetCollaborationMode>(wire::SessionCollaborationModeParams {
                session_id: Some(session.engine.session_id().to_string()),
                thread_id: None,
                mode: wire::CollaborationMode::Plan,
                expected_revision: None,
            })
            .await?;
        return Ok(());
    }
    let (approval, sandbox, reviewer) =
        klaus_agent_core::schemas::config::legacy_permission_policy(mode).unwrap_or((
            klaus_agent_core::types::permission::ApprovalPolicy::OnRequest,
            klaus_agent_core::types::permission::SandboxMode::ReadOnly,
            klaus_agent_core::types::permission::ApprovalsReviewer::User,
        ));
    let mut policy = empty_policy(session);
    policy.approval_policy = Some(approval);
    policy.sandbox_mode = Some(render::wire_tag(&sandbox));
    policy.approvals_reviewer = Some(render::wire_tag(&reviewer));
    session
        .engine
        .ack::<SessionSetRuntimePolicy>(policy)
        .await?;
    Ok(())
}

fn set_mode(status: &mut tui::toolbar::Status, mode: ExternalPermissionMode) {
    if mode == ExternalPermissionMode::Plan {
        status.badges.plan = true;
        return;
    }
    let sandbox = klaus_agent_core::schemas::config::legacy_permission_policy(mode)
        .map(|(_, sandbox, _)| sandbox)
        .unwrap_or(klaus_agent_core::types::permission::SandboxMode::ReadOnly);
    status.mode = render::wire_tag(&sandbox);
}

/// Apply a model selection and any compatibility input through named methods.
///
/// A `model=` naming a SELECTION cannot ride that op — it decides which provider
/// answers, which is settled when a session starts — so it is applied by reopening
/// the session first, and whatever else the command named is then applied to the
/// replacement. Doing it the other way round would settle fields on a session
/// about to be released.
async fn config(
    session: &mut CliSession,
    globals: &mut Globals,
    printer: &Printer,
    rest: &str,
    status: &mut tui::toolbar::Status,
) -> CliResult<()> {
    let mut update = ProtocolSessionConfigUpdate::default();
    let mut legacy_policy = None;
    let mut reopen = None;
    for field in rest.split_whitespace() {
        match field.split_once('=') {
            Some(("model", value)) if !value.is_empty() => match ModelRef::parse(value) {
                ModelRef::Selection(selection) => reopen = Some(selection.to_string()),
                ModelRef::Name(model) => {
                    update.model = Some(RuntimeString::from(model));
                }
            },
            Some(("parallel" | "ultra", value)) => match parse_bool_field(value) {
                Some(enabled) => update.ultra_mode = Some(enabled),
                None => {
                    printer.note("klaus: parallel= wants true or false");
                    return Ok(());
                }
            },
            Some(("goal", value)) => match parse_bool_field(value) {
                Some(enabled) => update.goal = Some(enabled),
                None => {
                    printer.note("klaus: goal= wants true or false");
                    return Ok(());
                }
            },
            Some(("verbosity", value)) => match value {
                "low" => update.model_verbosity = Some(Some(OutputVerbosity::Low)),
                "medium" => update.model_verbosity = Some(Some(OutputVerbosity::Medium)),
                "high" => update.model_verbosity = Some(Some(OutputVerbosity::High)),
                "unset" | "default" => update.model_verbosity = Some(None),
                _ => {
                    printer.note("klaus: verbosity= wants low, medium, high, or unset");
                    return Ok(());
                }
            },
            Some(("summary", value)) => match value {
                "auto" => update.reasoning_summary = Some(ReasoningSummary::Auto),
                "concise" => update.reasoning_summary = Some(ReasoningSummary::Concise),
                "detailed" => update.reasoning_summary = Some(ReasoningSummary::Detailed),
                "none" => update.reasoning_summary = Some(ReasoningSummary::None),
                _ => {
                    printer.note("klaus: summary= wants auto, concise, detailed, or none");
                    return Ok(());
                }
            },
            Some(("web-search" | "web_search", value)) => match value {
                "disabled" => update.web_search_access = Some(WebSearchAccess::Disabled),
                "cached" => update.web_search_access = Some(WebSearchAccess::Cached),
                "indexed" => update.web_search_access = Some(WebSearchAccess::Indexed),
                "live" => update.web_search_access = Some(WebSearchAccess::Live),
                _ => {
                    printer.note("klaus: web-search= wants disabled, cached, indexed, or live");
                    return Ok(());
                }
            },
            Some(("mode", value)) => match permission_mode(value) {
                Some(mode) => legacy_policy = Some(mode),
                None => {
                    printer.note(&format!("klaus: mode= wants one of {}", modes()));
                    return Ok(());
                }
            },
            _ => {
                printer.note(
                    "klaus: /config takes model=<id> parallel=<bool> goal=<bool> verbosity=<low|medium|high|unset> summary=<auto|concise|detailed|none> web-search=<disabled|cached|indexed|live>",
                );
                return Ok(());
            }
        }
    }
    let has_update = has_config_update(&update);
    if !has_update && legacy_policy.is_none() && reopen.is_none() {
        printer.note(
            "klaus: /config takes model=<id> parallel=<bool> goal=<bool> verbosity=<low|medium|high|unset> summary=<auto|concise|detailed|none> web-search=<disabled|cached|indexed|live>",
        );
        return Ok(());
    }
    if let Some(selection) = &reopen {
        if let Err(error) = session
            .reopen_with_selection(globals, selection, printer)
            .await
        {
            printer.note(&format!(
                "klaus: {selection} was not applied ({error}) — still on {}, and nothing else in this command was changed",
                status.model
            ));
            return Ok(());
        }
        globals.model = Some(selection.clone());
        status.model = selection.clone();
        printer.note(&format!(
            "klaus: {selection} — reopened this thread on it, from the next turn"
        ));
        if legacy_policy.is_none() && !has_update {
            return Ok(());
        }
    }
    if let Some(mode) = legacy_policy {
        apply_legacy_policy(session, mode).await?;
        set_mode(status, mode);
    }
    if !has_update {
        return Ok(());
    }
    submit_session_config_update(session, printer, &update, status).await?;
    if let Some(model) = &update.model {
        globals.model = Some(text::text(model));
    }
    if let Some(parallel) = update.ultra_mode {
        globals.parallel = parallel;
    }
    if let Some(goal) = update.goal {
        globals.goal_mode = goal;
    }
    Ok(())
}

fn has_config_update(update: &ProtocolSessionConfigUpdate) -> bool {
    update.model.is_some()
        || update.ultra_mode.is_some()
        || update.goal.is_some()
        || update.model_verbosity.is_some()
        || update.reasoning_summary.is_some()
        || update.web_search_access.is_some()
}

fn parse_bool_field(value: &str) -> Option<bool> {
    match value {
        "true" | "on" | "1" => Some(true),
        "false" | "off" | "0" => Some(false),
        _ => None,
    }
}

async fn submit_session_config_update(
    session: &CliSession,
    printer: &Printer,
    update: &ProtocolSessionConfigUpdate,
    status: &mut tui::toolbar::Status,
) -> CliResult<()> {
    settle::<SessionUpdateConfig>(
        &session.engine,
        printer,
        wire::SessionConfigUpdateParams {
            session_id: session.engine.session_id().to_string(),
            update: klaus_agent_server::app_server::runtime_wire::RawJson::from_serialize(
                &text::to_json(update)?,
            )
            .map_err(|error| CliError::Codec(error.to_string()))?,
            submission_id: None,
        },
        |event| event.session_config.is_some(),
        |event| match &event.session_config {
            Some(result) => session_config(result),
            None => missing("sessionConfig"),
        },
    )
    .await?;
    if let Some(model) = &update.model {
        status.model = text::text(model);
    }
    if let Some(parallel) = update.ultra_mode {
        status.badges.parallel = parallel;
    }
    if let Some(goal) = update.goal {
        status.badges.unattended = goal;
    }
    Ok(())
}

/// What the engine says the session's mutable config now is. Absent fields are
/// the ones the update did not name, so they are reported as untouched rather
/// than as unset.
fn session_config(result: &ProtocolSessionConfigUpdateResult) -> String {
    let mut parts = Vec::new();
    if let Some(model) = &result.model {
        parts.push(format!("model {}", text::text(model)));
    }
    if let Some(parallel) = result.ultra_mode {
        parts.push(format!("parallel {}", if parallel { "on" } else { "off" }));
    }
    if let Some(goal) = result.goal {
        parts.push(format!("goal {}", if goal { "on" } else { "off" }));
    }
    if let Some(verbosity) = &result.model_verbosity {
        match verbosity {
            Some(level) => parts.push(format!("verbosity {}", level.as_str())),
            None => parts.push("verbosity default".to_string()),
        }
    }
    if let Some(summary) = &result.reasoning_summary {
        parts.push(format!("summary {}", summary.as_str()));
    }
    if let Some(web_search) = &result.web_search_access {
        parts.push(format!("web-search {}", web_search.as_str()));
    }
    if parts.is_empty() {
        parts.push("config unchanged".to_string());
    }
    parts.join(", ")
}

/// Parsed through the same `clap` enum the `--permission-mode` flag uses, so a
/// spelling accepted on the command line is accepted here, aliases included.
fn permission_mode(value: &str) -> Option<ExternalPermissionMode> {
    PermissionModeArg::from_str(value, true)
        .ok()
        .map(Into::into)
}

fn modes() -> String {
    PermissionModeArg::value_variants()
        .iter()
        .filter_map(|variant| variant.to_possible_value())
        .map(|value| value.get_name().to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

/// One turn, non-interactively, then exit.
pub async fn exec(
    session: &mut CliSession,
    printer: &Printer,
    console: &mut Console,
    args: &ExecArgs,
) -> CliResult<()> {
    let prompt = prompt_text(args, console).await?;
    let options = run_options(args).await?;
    let policy = ApprovalPolicy::batch(args.auto_approve);

    let mut turn = Turn::new(&session.engine, printer, console, policy);
    let result = turn
        .run(Input::Text(RuntimeString::from(prompt.as_str())), options)
        .await?;

    // The assistant's prose already reached the screen as it streamed, and
    // `--json` already forwarded the whole `result` event, so neither is
    // reprinted here. Structured output is the exception: it exists only on the
    // result, so in human mode this is its only chance to be seen.
    if let Some(structured) = &result.structured_output {
        printer.line(&text::json_pretty(structured))?;
    }
    if let Some(path) = &args.output_last_message {
        // Written verbatim, with no trailing newline added: this file is meant to
        // be read back as the message, not printed.
        tokio::fs::write(path, text::text(&result.final_text).as_bytes())
            .await
            .map_err(|error| CliError::usage(format!("cannot write {path}: {error}")))?;
    }
    // Checked last, after both outputs are on disk and on screen. A turn that
    // failed part way still produced whatever it produced, and a caller that
    // asked for it in a file wants that file even when the exit code is telling
    // it the turn did not finish.
    turn::concluded(result.stop_reason)
}

/// A question answered against the session's context but outside its history.
///
/// Driven as a turn even though it settles on one reply: it spends model calls
/// and can raise approvals, and the engine tracks it as cancellable read-only
/// work — so ctrl-c here cancels just this question rather than reaching for the
/// session's own turn, which is not what is running.
pub async fn ask(
    session: &CliSession,
    printer: &Printer,
    console: &mut Console,
    question: &str,
) -> CliResult<()> {
    let mut turn = Turn::new(
        &session.engine,
        printer,
        console,
        ApprovalPolicy::interactive(printer, false),
    )
    .cancelling();
    let result = turn
        .follow::<SessionSideQuestion, _>(
            wire::SessionSideQuestionParams {
                session_id: session.engine.session_id().to_string(),
                question: question.to_string().into(),
                submission_id: None,
            },
            |event| event.side_question.clone(),
        )
        .await?;
    printer.line(&side_question(&result))?;
    turn::concluded(result.stop_reason)
}

fn side_question(result: &SideQuestionResult) -> String {
    let mut lines = vec![text::text(&result.response)];
    if result.synthetic {
        lines.push("[synthetic] answered without asking the model".to_string());
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

/// The prompt, from wherever this invocation put it.
///
/// Three sources, one at a time: the positional argument, a file, or stdin. An
/// argument together with `--input-file` is refused rather than silently ranked,
/// because a script that passes both is wrong about which one is being used.
async fn prompt_text(args: &ExecArgs, console: &mut Console) -> CliResult<String> {
    let prompt = match (&args.prompt, &args.input_file) {
        (Some(_), Some(_)) => {
            return Err(CliError::usage(
                "pass the prompt as an argument or with --input-file, not both".to_string(),
            ))
        }
        (Some(prompt), None) => prompt.clone(),
        (None, Some(path)) if path == "-" => console.read_to_end().await?,
        (None, Some(path)) => {
            let bytes = tokio::fs::read(path)
                .await
                .map_err(|error| CliError::usage(format!("cannot read {path}: {error}")))?;
            String::from_utf8_lossy(&bytes).into_owned()
        }
        (None, None) => console.read_to_end().await?,
    };
    if prompt.trim().is_empty() {
        return Err(CliError::usage(
            "the prompt is empty; pass it as an argument, with --input-file, or on stdin"
                .to_string(),
        ));
    }
    Ok(prompt)
}

/// Per-turn options, or `None` when the invocation asked for none.
///
/// Model and permission flags belong to session initialization. These options
/// contain only execution inputs with a declared per-turn scope.
async fn run_options(args: &ExecArgs) -> CliResult<Option<RunOpOptions>> {
    let mut options = RunOpOptions::default();
    let mut asked = false;
    if let Some(text_value) = &args.append_system_prompt {
        options.system_prompt_append = Some(RuntimeString::from(text_value.as_str()));
        asked = true;
    }
    if let Some(raw) = &args.output_schema {
        options.output_schema = Some(session::json_argument(raw).await?);
        asked = true;
    }
    Ok(asked.then_some(options))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completion_advertises_canonical_permission_and_planning_commands() {
        let commands = slash_commands();
        for (prefix, accepted) in [
            ("/perm", "/permissions "),
            ("/coll", "/collaboration "),
            ("/plan", "/plan "),
            ("/para", "/parallel "),
            ("/goal", "/goal "),
            ("/orch", "/orchestrate "),
        ] {
            let menu = tui::menu::Menu::for_input(prefix, &commands)
                .expect("the public command is offered by completion");
            assert_eq!(menu.accept(), accepted);
            assert!(
                HELP.contains(accepted.trim_end()),
                "completion must be documented"
            );
        }
        assert!(commands.iter().all(|candidate| candidate.name != "mode"));
        assert!(!HELP.contains("/mode "));
    }

    #[test]
    fn git_porcelain_v2_parses_branch_ahead_behind_and_dirty_state() {
        let raw = "\
# branch.oid 7089fa511\n\
# branch.head main\n\
# branch.upstream origin/main\n\
# branch.ab +2 -1\n\
1 .M N... 100644 100644 100644 111 222 src/lib.rs\n";
        let git = parse_git_porcelain_v2(raw).expect("valid git status");
        assert_eq!(git.branch, "main");
        assert_eq!(git.ahead, 2);
        assert_eq!(git.behind, 1);
        assert!(git.dirty);
    }

    #[test]
    fn canonical_permissions_do_not_change_the_planning_badge() {
        let mut status = tui::toolbar::Status {
            mode: "workspace-write".to_string(),
            ..Default::default()
        };
        set_mode(&mut status, ExternalPermissionMode::Plan);
        assert_eq!(status.mode, "workspace-write");
        assert!(status.badges.plan);
        set_mode(&mut status, ExternalPermissionMode::Default);
        assert_eq!(status.mode, "read-only");
        assert!(status.badges.plan);
        set_mode(&mut status, ExternalPermissionMode::AcceptEdits);
        assert_eq!(status.mode, "workspace-write");
        assert!(status.badges.plan);
    }
}
