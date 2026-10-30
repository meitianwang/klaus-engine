//! MCP servers: what is connected, how to change the set, and how to use one
//! directly.
//!
//! Everything here is session-scoped. The engine's MCP set belongs to the live
//! session, not to a config file it re-reads on its own, so `add`/`remove` change
//! this invocation and `reload` is what puts disk back in charge. That asymmetry
//! is the engine's design, and hiding it behind a "persist" flag would be a lie:
//! the CLI does not own the config file the desktop shell writes.
//!
//! Five of these ops answer with the same thing — the whole runtime snapshot —
//! because a mutation's outcome IS the new connection state. So `add` prints what
//! the server did on connect, not "ok".

use std::collections::BTreeMap;

use klaus_agent_core::runtime_process::RuntimeEnv;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_protocol::types::{
    McpAuthorizationRevokedWire, McpAuthorizationStatusWire, McpPromptResultWire,
    McpPromptsResultWire, McpResourceReadResultWire, McpRuntimeResult, McpServerRuntimeSnapshot,
    McpToolCallResultWire,
};
use klaus_agent_protocol::{ProtocolEvent, ProtocolMcpOAuthOption, ProtocolMcpServerInit};
use klaus_agent_server::app_server::methods::{
    SessionCallMcpTool, SessionGetMcpPrompt, SessionListMcpPrompts, SessionMcpAuthorizationStatus,
    SessionMcpRuntime, SessionReadMcpResource, SessionReconnectMcpServer, SessionReloadMcpServers,
    SessionRemoveMcpServer, SessionRevokeMcpAuthorization, SessionUpsertMcpServer,
};
use klaus_agent_server::app_server::wire;
use klaus_agent_server::Method;

use crate::cli::{McpAddArgs, McpCommand, McpTransportArg};
use crate::discovery;
use crate::engine::EngineClient;
use crate::error::{CliError, CliResult};
use crate::output::Printer;
use crate::render;
use crate::session::{self, CliSession};
use crate::text;

use super::{missing, settle, show};

pub async fn run(session: &CliSession, printer: &Printer, command: &McpCommand) -> CliResult<()> {
    let engine = &session.engine;
    match command {
        McpCommand::List => snapshot(engine, printer, None).await,

        // Filtered in the renderer rather than in the request: there is no
        // per-server read on the wire, so `--json` still carries every server.
        // Narrowing the JSON here would invent a reply shape the engine never
        // sent.
        McpCommand::Get { server } => snapshot(engine, printer, Some(server)).await,

        McpCommand::Add(args) => {
            mutation::<SessionUpsertMcpServer>(
                engine,
                printer,
                wire::SessionMcpServerUpsertParams {
                    session_id: engine.session_id().to_string(),
                    server: text::to_json(&server_init(args)?)?,
                    submission_id: None,
                },
                Some(&args.name),
            )
            .await
        }

        McpCommand::Remove { server } => {
            mutation::<SessionRemoveMcpServer>(engine, printer, named(engine, server), None).await
        }

        McpCommand::Reconnect { server } => {
            mutation::<SessionReconnectMcpServer>(
                engine,
                printer,
                named(engine, server),
                Some(server),
            )
            .await
        }

        // Reload replaces the whole set, so an empty read means "disconnect
        // everything" — which is a real answer, not a reason to refuse. It is
        // called out because a caller who mistyped `--cd` would otherwise watch
        // its servers vanish with no explanation.
        McpCommand::Reload => {
            let (servers, skipped) = discovery::mcp_servers(&session.cwd).await;
            for note in &skipped {
                printer.note(&format!("klaus: {note}"));
            }
            if servers.is_empty() {
                printer.note(&format!(
                    "klaus: no MCP servers configured under {}; the session will end up with none",
                    session.cwd
                ));
            }
            mutation::<SessionReloadMcpServers>(
                engine,
                printer,
                wire::SessionMcpServersReloadParams {
                    session_id: engine.session_id().to_string(),
                    servers: text::to_json(&servers)?,
                    submission_id: None,
                },
                None,
            )
            .await
        }

        McpCommand::Call {
            server,
            tool,
            arguments,
        } => {
            let arguments = match arguments {
                Some(raw) => Some(text::to_json(&session::json_argument(raw).await?)?),
                None => None,
            };
            show::<SessionCallMcpTool>(
                engine,
                printer,
                wire::SessionMcpToolCallParams {
                    session_id: engine.session_id().to_string(),
                    server_name: server.clone(),
                    tool: tool.clone(),
                    arguments,
                    submission_id: None,
                },
                |event| match &event.mcp_tool_call {
                    Some(result) => tool_call(result),
                    None => missing("mcpToolCall"),
                },
            )
            .await
        }

        McpCommand::Resource { server, uri } => {
            show::<SessionReadMcpResource>(
                engine,
                printer,
                wire::SessionMcpResourceParams {
                    session_id: engine.session_id().to_string(),
                    server_name: server.clone(),
                    uri: uri.clone(),
                    submission_id: None,
                },
                |event| match &event.mcp_resource_read {
                    Some(result) => resource_read(result),
                    None => missing("mcpResourceRead"),
                },
            )
            .await
        }

        McpCommand::Prompts => {
            show::<SessionListMcpPrompts>(engine, printer, engine.scope(), |event| {
                match &event.mcp_prompts {
                    Some(result) => prompts(result),
                    None => missing("mcpPrompts"),
                }
            })
            .await
        }

        McpCommand::Prompt {
            server,
            name,
            arguments,
        } => {
            let arguments = match arguments {
                Some(raw) => Some(text::to_json(&session::json_argument(raw).await?)?),
                None => None,
            };
            show::<SessionGetMcpPrompt>(
                engine,
                printer,
                wire::SessionMcpPromptParams {
                    session_id: engine.session_id().to_string(),
                    server_name: server.clone(),
                    name: name.clone(),
                    arguments,
                    submission_id: None,
                },
                |event| match &event.mcp_prompt {
                    Some(result) => prompt(result),
                    None => missing("mcpPrompt"),
                },
            )
            .await
        }

        McpCommand::AuthStatus { server } => {
            show::<SessionMcpAuthorizationStatus>(engine, printer, named(engine, server), |event| {
                match &event.mcp_authorization_status {
                    Some(result) => auth_status(result),
                    None => missing("mcpAuthorizationStatus"),
                }
            })
            .await
        }

        McpCommand::AuthRevoke { server } => {
            settle::<SessionRevokeMcpAuthorization>(
                engine,
                printer,
                named(engine, server),
                |event| event.mcp_authorization_revoked.is_some(),
                |event| match &event.mcp_authorization_revoked {
                    Some(result) => auth_revoked(result),
                    None => missing("mcpAuthorizationRevoked"),
                },
            )
            .await
        }
    }
}

/// The params of every method that names one server and nothing else.
fn named(engine: &EngineClient, server: &str) -> wire::SessionMcpServerParams {
    wire::SessionMcpServerParams {
        session_id: engine.session_id().to_string(),
        server_name: server.to_string(),
        submission_id: None,
    }
}

/// Read the runtime snapshot, optionally narrowed to one server in human mode.
async fn snapshot(engine: &EngineClient, printer: &Printer, only: Option<&str>) -> CliResult<()> {
    show::<SessionMcpRuntime>(engine, printer, engine.scope(), |event| {
        runtime(event, only)
    })
    .await
}

/// Change the MCP set and print the connection state that resulted.
///
/// Acked rather than answered, unlike the plain read: connecting a server takes
/// as long as the server takes, so the engine receipts the submission and streams
/// the snapshot when it knows one.
async fn mutation<M>(
    engine: &EngineClient,
    printer: &Printer,
    params: M::Params,
    only: Option<&str>,
) -> CliResult<()>
where
    M: Method<Result = wire::SubmissionAck>,
{
    settle::<M>(
        engine,
        printer,
        params,
        |event| event.mcp_runtime.is_some(),
        |event| runtime(event, only),
    )
    .await
}

/// The snapshot five methods answer with, narrowed the same way for all of them.
fn runtime(event: &ProtocolEvent, only: Option<&str>) -> String {
    match &event.mcp_runtime {
        Some(result) => match only {
            Some(name) => one_server(result, name),
            None => all_servers(result),
        },
        None => missing("mcpRuntime"),
    }
}

fn all_servers(result: &McpRuntimeResult) -> String {
    let servers = &result.runtime.servers;
    if servers.is_empty() {
        return "(no MCP servers)".to_string();
    }
    servers.iter().map(server).collect::<Vec<_>>().join("\n\n")
}

/// One server out of the snapshot.
///
/// A miss lists what there was instead of just saying "not found": the usual
/// cause is a name that differs from the config key, and the caller cannot fix
/// that without seeing the real names.
fn one_server(result: &McpRuntimeResult, name: &str) -> String {
    let servers = &result.runtime.servers;
    match servers.iter().find(|entry| text::text(&entry.name) == name) {
        Some(entry) => server(entry),
        None if servers.is_empty() => format!("no server named {name}; none are connected"),
        None => format!(
            "no server named {name}; connected: {}",
            servers
                .iter()
                .map(|entry| text::text(&entry.name))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

/// One server's status line, then what it exposes.
///
/// `auth_status` is printed next to the connection state rather than folded into
/// it because they answer different questions: a server can be `ready` and still
/// be `notLoggedIn` (nothing has needed a token yet), and only the pair tells the
/// caller whether `mcp auth-status` is worth running.
fn server(snapshot: &McpServerRuntimeSnapshot) -> String {
    let mut header = format!(
        "{}  [{} / {}]",
        text::text(&snapshot.name),
        render::wire_tag(&snapshot.state),
        render::wire_tag(&snapshot.auth_status)
    );
    if let Some(info) = &snapshot.server_info {
        let described = [
            info.title.as_ref(),
            info.name.as_ref(),
            info.version.as_ref(),
        ]
        .into_iter()
        .flatten()
        .map(text::text)
        .collect::<Vec<_>>();
        if !described.is_empty() {
            header.push_str(&format!("  {}", described.join(" ")));
        }
    }
    let mut lines = vec![header];

    if let Some(reason) = &snapshot.failure_reason {
        lines.push(format!("  reason: {}", render::wire_tag(reason)));
    }
    if let Some(error) = &snapshot.error {
        lines.push(format!("  error: {}", text::text(error)));
    }
    // Printed because it is the whole point of a `needs-auth` row: without the
    // URL the caller has nothing to act on.
    if let Some(url) = &snapshot.authorization_url {
        lines.push(format!("  authorize: {}", text::text(url)));
    }
    if let Some(instructions) = &snapshot.instructions {
        lines.push(format!("  instructions: {}", text::text(instructions)));
    }

    for tool in &snapshot.tools {
        let mut tags = vec![render::wire_tag(&tool.exposure)];
        if tool.read_only {
            tags.push("read-only".to_string());
        }
        if tool.destructive == Some(true) {
            tags.push("destructive".to_string());
        }
        let mut line = format!("  tool {} [{}]", text::text(&tool.name), tags.join(", "));
        if let Some(hint) = &tool.search_hint {
            line.push_str(&format!(" {}", text::text(hint)));
        }
        lines.push(line);
    }
    for resource in &snapshot.resources {
        lines.push(format!(
            "  resource {}  {}",
            text::text(&resource.uri),
            render::label(resource.name.as_ref(), "(unnamed)")
        ));
    }
    // Templates are listed apart from resources because a template is not
    // readable as printed: `mcp resource` would fail on the placeholder.
    for template in &snapshot.resource_templates {
        lines.push(format!(
            "  template {}  {}",
            text::text(&template.uri_template),
            render::label(template.name.as_ref(), "(unnamed)")
        ));
    }
    for prompt in &snapshot.prompts {
        let arguments = prompt
            .arguments
            .as_ref()
            .map(|arguments| {
                arguments
                    .iter()
                    .map(|argument| match argument.required {
                        Some(true) => format!("<{}>", text::text(&argument.name)),
                        _ => format!("[{}]", text::text(&argument.name)),
                    })
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .unwrap_or_default();
        lines.push(
            format!("  prompt {} {arguments}", text::text(&prompt.name))
                .trim_end()
                .to_string(),
        );
    }
    lines.join("\n")
}

/// A tool's own reply. Pretty-printed because a tool result is structured data a
/// human is reading here, not a line to grep.
fn tool_call(result: &McpToolCallResultWire) -> String {
    let mut lines = Vec::new();
    if result.is_error == Some(true) {
        lines.push(format!("[tool error] {}", text::text(&result.server_name)));
    }
    lines.push(text::json_pretty(&result.content));
    // Both halves print when both are present: `structuredContent` is the
    // machine-readable form and `content` the model-readable one, and MCP does
    // not require them to say the same thing.
    if let Some(structured) = &result.structured_content {
        lines.push(format!("structured: {}", text::json_pretty(structured)));
    }
    lines.join("\n")
}

/// A resource's contents. Text is printed; a blob is described, since writing
/// base64 to a terminal helps nobody.
fn resource_read(result: &McpResourceReadResultWire) -> String {
    if result.contents.is_empty() {
        return format!("(empty resource on {})", text::text(&result.server_name));
    }
    let mut lines = Vec::new();
    for content in &result.contents {
        let mut header = text::text_or(content.uri.as_ref(), "(no uri)");
        if let Some(mime) = &content.mime_type {
            header.push_str(&format!("  {}", text::text(mime)));
        }
        lines.push(header);
        match (&content.text, &content.blob) {
            (Some(body), _) => lines.push(text::text(body)),
            (None, Some(blob)) => lines.push(format!(
                "  ({} base64 code units of binary content, not printed)",
                blob.len_utf16()
            )),
            (None, None) => lines.push("  (no content)".to_string()),
        }
    }
    lines.join("\n")
}

fn prompts(result: &McpPromptsResultWire) -> String {
    let mut lines = Vec::new();
    for entry in &result.servers {
        for prompt in &entry.prompts {
            lines.push(format!(
                "{}/{}  {}",
                text::text(&entry.server_name),
                text::text(&prompt.name),
                render::label(prompt.description.as_ref(), "")
            ));
        }
    }
    if lines.is_empty() {
        return "(no MCP prompts)".to_string();
    }
    lines.join("\n")
}

/// A rendered prompt. Printed as-is, with no header, so it can be piped straight
/// into `klaus exec` — which is what rendering a prompt is for.
fn prompt(result: &McpPromptResultWire) -> String {
    text::text(&result.content)
}

fn auth_status(result: &McpAuthorizationStatusWire) -> String {
    format!(
        "{}  {}",
        text::text(&result.server_name),
        render::wire_tag(&result.auth_status)
    )
}

/// The state after revoking. The engine reports the post-revoke auth status, so
/// a server that still authenticates some other way (a bearer token in config)
/// says so instead of reading as logged out.
fn auth_revoked(result: &McpAuthorizationRevokedWire) -> String {
    format!(
        "{}  revoked={}  now {}",
        text::text(&result.server_name),
        result.revoked,
        render::wire_tag(&result.auth_status)
    )
}

/// Turn `mcp add` flags into the one wire variant they describe.
///
/// The transport decides which other flags are required, and clap cannot express
/// that (the requirement runs from a value, not from a flag's presence), so it is
/// checked here. Flags belonging to the other transport are refused rather than
/// ignored: silently dropping `--url` on a stdio server would connect something
/// other than what was asked for.
fn server_init(args: &McpAddArgs) -> CliResult<ProtocolMcpServerInit> {
    let name = RuntimeString::from(args.name.as_str());
    let always_load = args.always_load.then_some(true);
    let disabled_tools = (!args.disabled_tools.is_empty()).then(|| {
        args.disabled_tools
            .iter()
            .map(|tool| RuntimeString::from(tool.as_str()))
            .collect()
    });

    match args.transport {
        McpTransportArg::Stdio => {
            let command = args
                .command
                .as_deref()
                .ok_or_else(|| CliError::usage("--transport stdio needs --command".to_string()))?;
            if args.url.is_some() {
                return Err(CliError::usage(
                    "--url belongs to --transport http or sse".to_string(),
                ));
            }
            Ok(ProtocolMcpServerInit::Stdio {
                name,
                command: RuntimeString::from(command),
                args: (!args.args.is_empty()).then(|| {
                    args.args
                        .iter()
                        .map(|arg| RuntimeString::from(arg.as_str()))
                        .collect()
                }),
                env: env(&args.env)?,
                always_load,
                disabled_tools,
            })
        }
        McpTransportArg::Http | McpTransportArg::Sse => {
            let url = args.url.as_deref().ok_or_else(|| {
                CliError::usage(format!(
                    "--transport {} needs --url",
                    match args.transport {
                        McpTransportArg::Sse => "sse",
                        _ => "http",
                    }
                ))
            })?;
            if args.command.is_some() || !args.args.is_empty() || !args.env.is_empty() {
                return Err(CliError::usage(
                    "--command, --arg and --env belong to --transport stdio".to_string(),
                ));
            }
            let url = RuntimeString::from(url);
            let headers = headers(&args.headers)?;
            let oauth = oauth(args);
            match args.transport {
                McpTransportArg::Sse => Ok(ProtocolMcpServerInit::Sse {
                    name,
                    url,
                    headers,
                    oauth,
                    always_load,
                    disabled_tools,
                }),
                _ => Ok(ProtocolMcpServerInit::Http {
                    name,
                    url,
                    headers,
                    oauth,
                    always_load,
                    disabled_tools,
                }),
            }
        }
    }
}

/// `--scope` implies `--oauth`, because asking for a scope with OAuth off is not
/// a coherent request and refusing it would be pedantic. Bare `--oauth` sends
/// `true`, which the engine reads as "use whatever the server advertises".
fn oauth(args: &McpAddArgs) -> Option<ProtocolMcpOAuthOption> {
    if !args.scopes.is_empty() {
        return Some(ProtocolMcpOAuthOption::Object {
            scopes: Some(
                args.scopes
                    .iter()
                    .map(|scope| RuntimeString::from(scope.as_str()))
                    .collect(),
            ),
        });
    }
    args.oauth
        .then_some(ProtocolMcpOAuthOption::Advertised(true))
}

fn env(pairs: &[String]) -> CliResult<Option<RuntimeEnv>> {
    if pairs.is_empty() {
        return Ok(None);
    }
    let mut env = RuntimeEnv::new();
    for pair in pairs {
        let (key, value) = split_pair(pair, "--env")?;
        env.insert(RuntimeString::from(key), RuntimeString::from(value));
    }
    Ok(Some(env))
}

fn headers(pairs: &[String]) -> CliResult<Option<BTreeMap<RuntimeString, RuntimeString>>> {
    if pairs.is_empty() {
        return Ok(None);
    }
    let mut headers = BTreeMap::new();
    for pair in pairs {
        let (key, value) = split_pair(pair, "--header")?;
        headers.insert(RuntimeString::from(key), RuntimeString::from(value));
    }
    Ok(Some(headers))
}

/// Split on the FIRST `=` only: a header value or an env value may contain one,
/// and splitting on the last would silently move part of the value into the key.
fn split_pair<'a>(pair: &'a str, flag: &str) -> CliResult<(&'a str, &'a str)> {
    match pair.split_once('=') {
        Some((key, value)) if !key.is_empty() => Ok((key, value)),
        _ => Err(CliError::usage(format!(
            "{flag} takes KEY=VALUE, got `{pair}`"
        ))),
    }
}
