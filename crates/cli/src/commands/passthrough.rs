//! The commands that do not drive a session: they expose the endpoint itself.
//!
//! Everything else in this crate is a facade CLIENT — it opens a connection and
//! calls declared methods. These four are the opposite: three of them hand the
//! endpoint to somebody else, and the fourth never builds one. That is why they
//! are routed before [`crate::session::CliSession::open`] rather than through it.
//! A session opened here would be a second session on an endpoint the caller is
//! about to use for their own, which is both wasteful and visible to them:
//! session init runs disk discovery and the plugin ledger load.
//!
//! `proto` is the one place in this crate where raw ops appear on a wire, and it
//! is not an exception to the rule that they may not: nothing here BUILDS an op.
//! These modes hand over a byte stream and step out, so the ops on it are the
//! caller's, checked by the caller's own client. The rule the rest of the crate
//! keeps — reach the engine only by naming a declared method — is a rule about
//! what this process submits, not about what it is willing to carry for someone
//! else.
//!
//! The three serving modes are the same engine reached three ways, and the choice
//! is the caller's transport, not a difference in capability:
//!
//!   * `proto` — this process becomes the stdio endpoint. Identical to
//!     `klaus-agent-stdio`, exposed here so a host that already ships `klaus`
//!     does not need a second binary on PATH.
//!   * `serve` — a WebSocket (+ HTTP/SSE) listener other clients dial, including
//!     `klaus --remote`, which reaches the facade over `?surface=appServer`.
//!   * `app-server` — the JSON-RPC facade on stdio, which is the surface this
//!     CLI itself speaks; `klaus` in-process and a host across a pipe are then
//!     clients of the same declared method table.
//!
//! None of them prints on stdout except as protocol output. `serve` announces its
//! URL on stderr, and `proto`/`app-server` say nothing at all, because stdout is
//! the wire.

use std::io::Write as _;

use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_server::{
    run_app_server, schema_bundle, serve as serve_ws, Endpoint, ServeOptions,
};

use crate::cli::{Globals, ServeArgs};
use crate::error::{CliError, CliResult};
use crate::session;

/// Print the completion script for a shell.
///
/// The only command in the CLI that touches neither the engine nor the protocol,
/// which is exactly why it must be routed before the endpoint is built: asking
/// for a completion script has to work in an environment where the engine cannot
/// start at all, because a shell evaluates this at every login.
pub fn completion(shell: clap_complete::Shell) -> CliResult<()> {
    use clap::CommandFactory as _;

    let mut command = crate::cli::Cli::command();
    let name = command.get_name().to_string();
    let mut stdout = std::io::stdout();
    clap_complete::generate(shell, &mut command, name, &mut stdout);
    stdout.flush()?;
    Ok(())
}

/// Become the newline-delimited Op/Event endpoint on stdin/stdout.
///
/// The engine is assembled with the same options a normal run uses, so a host
/// that spawns `klaus proto` gets the endpoint `klaus exec` would have talked to.
/// The `--remote` family is refused rather than ignored: a caller who asked to
/// SERVE a protocol and to be a CLIENT of someone else's has asked for two
/// different things, and silently picking one is how a host ends up wondering why
/// its ops reach the wrong engine.
pub async fn proto(globals: &Globals, process_context: RuntimeProcessContext) -> CliResult<()> {
    let endpoint = build(globals, process_context).await?;
    let server =
        endpoint.protocol_server(Box::new(tokio::io::stdin()), Box::new(tokio::io::stdout()));
    server.clone().start().await;
    // Not `drop`: the Sessions this server minted own MCP/LSP children and a
    // rollout writer, and the process context cannot be released while they are
    // still stopping.
    server.shutdown_and_wait().await.map_err(CliError::Engine)?;
    Ok(())
}

/// Serve the endpoint over WebSocket (and HTTP/SSE on the same port) until the
/// process is interrupted.
///
/// One connection is one session, which is the reason this is not just `proto` on
/// a socket: the endpoint — agent, rollout store, plugin ledger — is shared, and
/// every client that dials gets its own session on it. That is what makes
/// `klaus serve` plus several `klaus --remote` clients cheaper than several
/// in-process engines.
pub async fn serve(
    globals: &Globals,
    args: &ServeArgs,
    process_context: RuntimeProcessContext,
) -> CliResult<()> {
    let endpoint = build(globals, process_context).await?;
    let sessions = endpoint.engine_sessions();
    let server = serve_ws(ServeOptions {
        windows_sandbox_context: Some(endpoint.windows_sandbox_context()),
        create_session: sessions.create_session,
        credential_store: Some(endpoint.credential_store.clone()),
        list_threads: sessions.list_threads,
        // The whole point of a shared engine is that `klaus --remote` is the same
        // client as `klaus`. Withholding these would silently make it a lesser
        // one: `--resume`, `session list` and `plugin list` would answer "requires
        // a session store" against an endpoint that plainly has one.
        lifecycle: Some(endpoint.lifecycle()),
        plugin_admin: endpoint.plugin_admin.clone(),
        host: args.host.clone(),
        port: args.port,
        token: args.token.clone(),
        // Empty means "no allow-list", which is NOT the same as an empty
        // allow-list: the latter rejects every browser-style request that carries
        // an `Origin` header at all.
        allowed_origins: (!args.allowed_origins.is_empty()).then(|| args.allowed_origins.clone()),
        // Bind failures are reported by this CLI, in its own error shape.
        silent: true,
    })
    .await
    .map_err(|error| CliError::engine(error.to_string()))?;

    // stderr, because a caller who wants the URL machine-readably is scripting a
    // pipeline whose stdout may already be spoken for.
    eprintln!("klaus: serving {} (ctrl-c to stop)", server.url);
    if let Some(token) = &args.token {
        eprintln!("klaus: clients must send `Authorization: Bearer {token}`");
    }

    interrupted().await;
    eprintln!("klaus: shutting down");
    // Resolves only once every connection's Session finished tearing down, so a
    // ctrl-c does not truncate the rollout files of clients that were mid-turn.
    server.close().await;
    Ok(())
}

/// Serve the JSON-RPC app-server facade on stdio, or print its schema bundle.
///
/// `--schema` is a pure artifact dump and builds no engine: it is the contract a
/// host generates its client types from, and needing a working provider
/// configuration to read a schema would be absurd.
pub async fn app_server(
    globals: &Globals,
    schema: bool,
    process_context: RuntimeProcessContext,
) -> CliResult<()> {
    if schema {
        let bundle = serde_json::to_string_pretty(&schema_bundle())
            .map_err(|error| CliError::engine(error.to_string()))?;
        let mut stdout = std::io::stdout();
        writeln!(stdout, "{bundle}")?;
        stdout.flush()?;
        return Ok(());
    }

    let endpoint = build(globals, process_context).await?;
    run_app_server(
        tokio::io::stdin(),
        tokio::io::stdout(),
        endpoint.app_server_options(),
    )
    .await;
    Ok(())
}

/// The endpoint the serving modes hand out.
///
/// Built through the same helper the session path uses, so `klaus serve` and
/// `klaus exec` disagree about nothing: same cwd resolution, same session
/// directory, same plugin home, same provider selection.
async fn build(globals: &Globals, process_context: RuntimeProcessContext) -> CliResult<Endpoint> {
    if globals.remote.is_some() {
        return Err(CliError::usage(
            "this command serves an engine, so it cannot also be a client of one: drop --remote"
                .to_string(),
        ));
    }
    session::build_endpoint(globals, process_context).await
}

/// Resolve when the operator asks the server to stop.
///
/// SIGTERM is handled next to ctrl-c because a supervised `klaus serve` is
/// stopped with a signal, not a keystroke, and a server that only understood
/// ctrl-c would be killed mid-turn instead of closing its sessions.
async fn interrupted() {
    #[cfg(unix)]
    {
        let mut terminate =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(signal) => signal,
                // No SIGTERM handler means ctrl-c only; that is a degraded stop,
                // not a reason to refuse to serve.
                Err(_) => {
                    let _ = tokio::signal::ctrl_c().await;
                    return;
                }
            };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
