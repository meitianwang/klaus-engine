//! Binding a session, over either transport.
//!
//! There are two ways to reach an engine and only one client. By default the CLI
//! *is* the endpoint: it assembles the engine in its own process and speaks the
//! protocol to it over an in-memory duplex, so a terminal user needs no daemon,
//! no socket and no token to secure. With `--remote` the same client speaks the
//! same protocol over a WebSocket to an engine someone else is serving.
//!
//! The point of routing both through this module is that everything above it —
//! every command, the turn loop, the renderers — sees one [`EngineClient`] and
//! cannot tell the difference. Both transports carry the SAME surface: the
//! engine's declared method table, spoken over an in-memory duplex here and over
//! a `?surface=appServer` socket there. Session identity included: resume and
//! fork are declared methods, not the WebSocket's connect-time query parameters,
//! because a lifecycle that only exists on one transport is a lifecycle the
//! in-process path would have to reimplement.
//!
//! Teardown is not symmetric, and that asymmetry is deliberate. An in-process
//! endpoint is ours to end: `session/shutdown` is what flushes rollout storage
//! and stops MCP/LSP children, and only after it lands may the facade be torn
//! down. A remote endpoint belongs to whoever served it, so the CLI releases only
//! what it created — `session/close` gives its own session back and leaves the
//! rest of the engine running, because shutting that down would kill a shared
//! server because one client exited.
//!
//! [`CliEndpoint`] is the same two transports with the session step left out, for
//! the endpoint-state commands. It exists because those methods take no session
//! id: starting one to reach them would run discovery, load plugins and boot MCP
//! children in order to print a config value, and — worse — would make reading
//! the config depend on a session the config is what configures.

use std::sync::Arc;

use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimeObject, RuntimeString};
use klaus_agent_protocol::{InitSessionResult, ProtocolEvent, ProtocolSessionInit};
use klaus_agent_server::app_server::methods::{
    Initialize, SessionClose, SessionFork, SessionResume, SessionShutdown, SessionStart,
};
use klaus_agent_server::app_server::wire;
use klaus_agent_server::{
    connect_facade, provider_named, AppServerClient, ConnectOptions, Endpoint, EndpointOptions,
    FacadeConnection,
};
use tokio::task::JoinHandle;

use crate::cli::Globals;
use crate::discovery::{self, DiscoveryOptions};
use crate::engine::{EndpointClient, EngineClient};
use crate::error::{CliError, CliResult};
use crate::model::ModelRef;
use crate::output::Printer;
use crate::text;

enum Transport {
    InProcess {
        /// Finishes once the facade client has been dropped and every session it
        /// created has been torn down, so awaiting it is how teardown waits for
        /// the engine rather than merely asking it to stop.
        server: JoinHandle<()>,
        /// Held so the endpoint (agent, stores, plugin ledger) outlives the
        /// session even though nothing above this module names it.
        _endpoint: Endpoint,
    },
    Remote(FacadeConnection),
}

/// A bound session: one client, plus whatever has to be torn down behind it.
pub struct CliSession {
    pub engine: EngineClient,
    /// The thread the session is bound to, when the CLI already knows it. A
    /// fresh session learns its id from the first turn's `thread.started`.
    pub thread_id: Option<RuntimeString>,
    /// The directory the session was opened against, already resolved. Kept
    /// because commands that re-read disk config mid-session (`mcp reload`) have
    /// to read the same roots the session was built from, not the process cwd,
    /// which `--cd` may have overridden.
    pub cwd: String,
    transport: Transport,
}

impl CliSession {
    /// Assemble or dial an endpoint, start a session on it, and bind the thread
    /// `--resume` / `--fork` named.
    pub async fn open(
        globals: &Globals,
        process_context: RuntimeProcessContext,
        printer: &Printer,
    ) -> CliResult<Self> {
        let cwd = resolve_cwd(globals)?;
        let (facade, transport) = dial(globals, process_context).await?;

        let session_id = match start(&facade, globals, &cwd, printer).await {
            Ok(session_id) => session_id,
            Err(error) => {
                // No session was started, so there is nothing to shut down —
                // but the endpoint behind the facade is live and ours, and
                // dropping it without teardown leaks the engine's children.
                end(facade, transport).await;
                return Err(error);
            }
        };

        let mut session = Self {
            engine: EngineClient::new(facade, session_id),
            thread_id: None,
            cwd,
            transport,
        };
        if let Err(error) = session.bind_lifecycle(globals).await {
            let _ = session.close().await;
            return Err(error);
        }
        Ok(session)
    }

    /// Apply the global `--resume` / `--fork` to the session just started.
    async fn bind_lifecycle(&mut self, globals: &Globals) -> CliResult<()> {
        if let Some(thread_id) = &globals.resume {
            self.bind(thread_id).await?;
        } else if let Some(thread_id) = &globals.fork {
            let event = self
                .engine
                .settle::<SessionFork>(self.thread_params(thread_id), |event| {
                    event.session.is_some()
                })
                .await?;
            self.thread_id = Some(bound_thread(&event)?);
        }
        Ok(())
    }

    /// Bind an already-open session to a different persisted thread. Used by the
    /// `session` commands, whose thread argument is per-command rather than the
    /// global `--resume`.
    pub async fn bind(&mut self, thread_id: &str) -> CliResult<RuntimeString> {
        let event = self
            .engine
            .settle::<SessionResume>(self.thread_params(thread_id), |event| {
                event.session.is_some()
            })
            .await?;
        let bound = bound_thread(&event)?;
        self.thread_id = Some(bound.clone());
        Ok(bound)
    }

    /// The params both binding methods take, scoped to this session.
    fn thread_params(&self, thread_id: &str) -> wire::SessionThreadParams {
        wire::SessionThreadParams {
            inherit_goal: None,
            session_id: Some(self.engine.session_id().to_string()),
            thread_id: thread_id.to_string(),
            new_thread_id: None,
            before_message_uuid: None,
            submission_id: None,
        }
    }

    /// Move this session onto a different model SELECTION, keeping the thread.
    ///
    /// A selection decides WHICH provider and credential answer, and the endpoint
    /// settles that once, while starting the session: there is no op that swaps a
    /// live session's provider. So changing it means replacing the session, which
    /// is what the desktop client does with any setting frozen at start — the live
    /// binding is dropped and rebuilt. The thread is resumed on the replacement,
    /// so the conversation carries over instead of starting again.
    ///
    /// The replacement is started BEFORE the old session is released, and that
    /// order is the point: a selection that does not resolve (an id naming nothing,
    /// a subscription not signed in) fails here, and failing leaves the session in
    /// hand untouched rather than stranding the REPL with no session at all.
    pub async fn reopen_with_selection(
        &mut self,
        globals: &Globals,
        selection: &str,
        printer: &Printer,
    ) -> CliResult<()> {
        let mut globals = globals.clone();
        globals.model = Some(selection.to_string());
        // Everything else about the init is rebuilt from the same globals the
        // session was opened with, so only the selection differs.
        let facade = self.engine.facade();
        let session_id = start_session(&facade, &globals, &self.cwd, printer).await?;
        // The thread to carry over. An explicit bind (`--resume`, the `session`
        // commands) recorded one; otherwise it is the session id being replaced,
        // because a session the endpoint minted an id for IS the thread filed under
        // that id — one value on the wire, in the store and in the conversation
        // list. Reading it here rather than waiting for a `thread.started` event
        // matters: the first `/model` of a session would otherwise find no thread
        // and drop the history without saying so.
        let carried = self
            .thread_id
            .as_ref()
            .map(text::text)
            .unwrap_or_else(|| self.engine.session_id().to_string());
        let replaced = std::mem::replace(&mut self.engine, EngineClient::new(facade, session_id));
        // `session/close`, never `session/shutdown`: the endpoint is shared with
        // the replacement now, and shutting it down would take the new session
        // with it. A failure to release is reported and not fatal — the new
        // session is already the live one.
        let closing = replaced
            .call::<SessionClose>(wire::SessionCloseParams {
                session_id: replaced.session_id().to_string(),
            })
            .await;
        if let Err(error) = closing {
            printer.note(&format!(
                "klaus: the previous session did not release cleanly: {error}"
            ));
        }
        // Resume onto the replacement so the history the user has been building is
        // what the next turn sees. A thread with no turns in it yet has nothing to
        // resume, which is why a failure here is reported rather than fatal — but it
        // IS reported, because continuing on an empty context after a `/model` that
        // looked like it kept the conversation is the one outcome a user cannot see.
        if let Err(error) = self.bind(&carried).await {
            printer.note(&format!(
                "klaus: this thread's history did not carry over ({error}) — the new session starts empty"
            ));
        }
        Ok(())
    }

    /// The thread id commands print, or a stand-in when no thread is bound yet.
    pub fn thread_label(&self) -> String {
        text::text_or(self.thread_id.as_ref(), "(unbound)")
    }

    /// End the session.
    ///
    /// Both paths release it through the declared surface, but they ask for
    /// different things. An in-process endpoint is ours, so `session/shutdown`
    /// tears it down and reports what it flushed. A remote endpoint is not, so
    /// `session/close` releases just the session this invocation asked the server
    /// to create — same teardown, without claiming the right to a shutdown report
    /// from an engine other clients are also using. Dropping the socket instead
    /// would leave that session registered until the server noticed.
    pub async fn close(self) -> CliResult<()> {
        let Self {
            engine, transport, ..
        } = self;
        let released = match &transport {
            Transport::InProcess { .. } => engine
                .query::<SessionShutdown>(engine.scope())
                .await
                .map(|_| ()),
            Transport::Remote(_) => engine
                .call::<SessionClose>(wire::SessionCloseParams {
                    session_id: engine.session_id().to_string(),
                })
                .await
                .map(|_| ()),
        };
        end(engine.into_facade(), transport).await;
        released
    }
}

/// An engine with no session on it: the endpoint-state surface only.
///
/// Same two transports, same teardown asymmetry, minus the session. Teardown is
/// simpler for a reason rather than by omission — `session/shutdown` exists to
/// flush what a SESSION wrote, and this never opened one, so dropping the facade
/// and letting the in-process server drain is the whole of it. The remote side
/// stays untouched for the same reason it does above: the engine is not ours.
pub struct CliEndpoint {
    pub engine: EndpointClient,
    transport: Transport,
}

impl CliEndpoint {
    /// Assemble or dial an endpoint and announce this client, nothing more.
    pub async fn open(
        globals: &Globals,
        process_context: RuntimeProcessContext,
    ) -> CliResult<Self> {
        let (facade, transport) = dial(globals, process_context).await?;
        if let Err(error) = handshake(&facade).await {
            end(facade, transport).await;
            return Err(error);
        }
        Ok(Self {
            engine: EndpointClient::new(facade),
            transport,
        })
    }

    /// The directory a config or hooks command resolves a project file against.
    pub fn cwd(globals: &Globals) -> CliResult<String> {
        resolve_cwd(globals)
    }

    pub async fn close(self) {
        end(self.engine.into_facade(), self.transport).await;
    }
}

/// The thread a binding method reported. The predicate above already matched on
/// it, so an absent `session` here means the engine replied to a bind with
/// something that is not a binding.
fn bound_thread(event: &ProtocolEvent) -> CliResult<RuntimeString> {
    match &event.session {
        Some(session) => Ok(session.thread_id.clone()),
        None => Err(CliError::Protocol("the engine bound no thread".to_string())),
    }
}

/// Open a facade connection, either to an engine assembled here or to one
/// someone else is serving.
async fn dial(
    globals: &Globals,
    process_context: RuntimeProcessContext,
) -> CliResult<(Arc<AppServerClient>, Transport)> {
    match globals.remote.as_deref() {
        Some(url) => {
            let connection = connect_facade(
                url,
                ConnectOptions {
                    token: globals.token.clone(),
                    ..Default::default()
                },
            )
            .await?;
            let facade = connection.client.clone();
            Ok((facade, Transport::Remote(connection)))
        }
        None => {
            let endpoint = build_endpoint(globals, process_context).await?;
            // Assembled from the endpoint rather than here, so this surface
            // cannot end up offering a lesser engine than `klaus app-server` or
            // a served socket does.
            let (facade, server) = AppServerClient::in_process(endpoint.app_server_options());
            Ok((
                Arc::new(facade),
                Transport::InProcess {
                    server,
                    _endpoint: endpoint,
                },
            ))
        }
    }
}

/// Handshake, then start one session and confirm the engine accepted its init.
async fn start(
    facade: &AppServerClient,
    globals: &Globals,
    cwd: &str,
    printer: &Printer,
) -> CliResult<String> {
    handshake(facade).await?;
    start_session(facade, globals, cwd, printer).await
}

/// The session half of [`start`], for a connection that has already announced
/// itself.
///
/// Split out because `initialize` is once per CONNECTION while `session/start` is
/// not: replacing a session to change its model selection opens a second one on
/// the connection already in hand, and re-announcing there answers `Already
/// initialized` instead of starting anything.
async fn start_session(
    facade: &AppServerClient,
    globals: &Globals,
    cwd: &str,
    printer: &Printer,
) -> CliResult<String> {
    // `dynamic_tools` is deliberately left empty, which is why the CLI never
    // answers `resolve_dynamic_tool_call`. That op replies to a
    // `dynamicToolCall` event, and the engine only ever emits one from the
    // adapter it builds out of the tools declared HERE — so a host that
    // declares none is never asked. Declaring any would mean the CLI itself
    // had to implement them, and a command-line client has no browser or
    // desktop to offer the model. MCP servers are how it contributes tools
    // instead, and those the engine runs on its own.
    let mut init = ProtocolSessionInit {
        cwd: Some(RuntimeString::from(cwd)),
        config: session_config(globals)?,
        collaboration_mode: globals
            .permission_mode
            .filter(|mode| matches!(mode, crate::cli::PermissionModeArg::Plan))
            .map(|_| "plan".into()),
        advisor_target: None,
        ..Default::default()
    };
    if !globals.no_discovery {
        let found = discovery::discover(&DiscoveryOptions {
            cwd: cwd.to_string(),
            skills_dirs: globals.skills_dir.clone(),
            agents_dirs: globals.agents_dir.clone(),
        })
        .await;
        for note in &found.skipped {
            printer.note(&format!("klaus: {note}"));
        }
        discovery::apply(&mut init, found);
    }

    // Lifecycle is applied afterwards, as declared methods, rather than through
    // this call's own `resume`/`fork`: those answer with a session id alone,
    // while the bind methods report the thread they landed on — which is what
    // the CLI prints and what `session` commands rebind against.
    let mut init = text::to_json(&init)?;
    // `modelSelection` belongs to the app-server facade, not to the protocol:
    // the facade resolves it into `provider` + `auth` against `[modelProviders]`
    // and the credential store, then strips it before the init reaches the
    // protocol server. `ProtocolSessionInit` therefore has no field for it, and
    // it is added to the encoded object instead.
    if let Some(selection) = model_selection(globals) {
        if let Some(object) = init.as_object_mut() {
            object.insert("modelSelection", text::string(selection));
        }
    }
    let started = facade
        .request::<SessionStart>(wire::SessionStartParams {
            init: klaus_agent_server::app_server::runtime_wire::RawJson::from_serialize(&init)
                .map_err(|error| CliError::Codec(error.to_string()))?,
            resume: None,
            fork: None,
        })
        .await?;
    let initialized: InitSessionResult = text::decode(&started.initialized)?;
    if !initialized.accepted {
        return Err(CliError::Protocol(
            "the engine rejected the session init".to_string(),
        ));
    }
    Ok(started.session_id)
}

/// Announce this client. Every connection does this before anything else,
/// session or not, because the facade refuses the rest of the table until it
/// knows who is calling.
async fn handshake(facade: &AppServerClient) -> CliResult<()> {
    // `productEvents` is deliberately not negotiated: the typed `thread/*`
    // surface is a second rendering of the same events, and the CLI already
    // renders the raw ones. Asking for both would mean printing each twice.
    facade
        .request::<Initialize>(wire::InitializeParams {
            client_info: Some(wire::ClientInfo {
                name: "klaus-cli".to_string(),
                version: Some(env!("CARGO_PKG_VERSION").to_string()),
            }),
            capabilities: None,
        })
        .await?;
    Ok(())
}

/// Drop the facade and wait for whatever was behind it to finish.
///
/// The client is dropped FIRST in both cases: it owns the reader task and,
/// in-process, the write half the facade is reading — so the server sees end of
/// input and returns, which is what makes the await below terminate rather than
/// hang.
async fn end(facade: Arc<AppServerClient>, transport: Transport) {
    drop(facade);
    match transport {
        Transport::InProcess {
            server,
            _endpoint: endpoint,
        } => {
            let _ = server.await;
            // Only now: the endpoint owns the agent and the stores the facade
            // was still writing through a moment ago.
            drop(endpoint);
        }
        Transport::Remote(connection) => connection.close_and_wait().await,
    }
}

fn resolve_cwd(globals: &Globals) -> CliResult<String> {
    match &globals.cwd {
        Some(cwd) => Ok(cwd.clone()),
        None => std::env::current_dir()
            .map(|path| path.to_string_lossy().into_owned())
            .map_err(|error| {
                CliError::engine(format!("cannot read the working directory: {error}"))
            }),
    }
}

/// Assemble an engine endpoint from the global options.
///
/// Shared with the serving commands so that `klaus exec` and `klaus serve` cannot
/// drift: the endpoint-shaped globals (`--cd`, `--session-dir`, `--plugin-home`,
/// `--no-plugins`, `--provider`) are read exactly once, here.
pub async fn build_endpoint(
    globals: &Globals,
    process_context: RuntimeProcessContext,
) -> CliResult<Endpoint> {
    Endpoint::build(EndpointOptions {
        process_context,
        cwd: Some(resolve_cwd(globals)?),
        session_dir: globals.session_dir.clone(),
        plugin_home: globals.plugin_home.clone().map(Into::into),
        // `Some(false)` only: leaving it `None` lets the endpoint keep honouring
        // `KLAUS_DISABLE_INSTALLED_PLUGINS`, which a flag that was NOT passed has
        // no business overriding.
        load_installed_plugins: globals.no_plugins.then_some(false),
        credential_store: None,
        // Selected through the engine's own resolver so a flag and `KLAUS_PROVIDER`
        // produce the same provider, including the Bedrock/Vertex/Foundry routing
        // that Anthropic applies underneath.
        provider: globals.provider.as_deref().map(provider_named),
    })
    .await
    .map_err(|error| CliError::engine(format!("failed to start engine: {error}")))
}

/// The selection `--model` names, when it names one at all.
///
/// A selection and a config `model` are mutually exclusive: the first decides
/// which provider and credential the session runs on, the second names a model
/// for a provider already decided. Which of the two a value means is read in one
/// place — [`ModelRef`] — so the init cannot end up carrying both.
fn model_selection(globals: &Globals) -> Option<&str> {
    match ModelRef::parse(globals.model.as_deref()?) {
        ModelRef::Selection(selection) => Some(selection),
        ModelRef::Name(_) => None,
    }
}

/// The session's config object: `--config-json` as given, with `--model` and
/// `--goal-mode` filled in when the object does not already name them. Same
/// precedence the endpoint applies to its environment config, so a flag and a
/// config file mean the same thing on both transports — which matters because
/// `--remote` cannot reach the served endpoint's environment at all.
pub(crate) fn session_config(globals: &Globals) -> CliResult<Option<RuntimeJson>> {
    // Matched by reference: `RuntimeJson` owns runtime handles and implements
    // `Drop`, so the object cannot be moved out of the variant.
    let mut object = match &globals.config_json {
        Some(raw) => match &text::parse_json(raw)? {
            RuntimeJson::Object(object) => object.clone(),
            _ => {
                return Err(CliError::usage(
                    "--config-json must be a JSON object".to_string(),
                ))
            }
        },
        None => RuntimeObject::new(),
    };
    // Only a model NAME goes here. A selection is carried as `modelSelection`
    // instead, and writing it into `model` as well would ask the provider the
    // selection resolved to for a model id that is really a selection string.
    if let Some(ModelRef::Name(model)) = globals.model.as_deref().map(ModelRef::parse) {
        let key = RuntimeString::from("model");
        if object.get(&key).is_none() {
            object.insert(key, RuntimeJson::String(RuntimeString::from(model)));
        }
    }
    // Only ever written as `true`: the flag turns the mode ON, so the absence of
    // it must not override a `goal` the config object — or an implied one — set
    // deliberately. An explicit `{"goal": false}` therefore still wins, which is
    // the only way to refuse the implication the `goal` commands make.
    if globals.goal_mode {
        let key = RuntimeString::from("goal");
        if object.get(&key).is_none() {
            object.insert(key, RuntimeJson::Bool(true));
        }
    }
    if let Some(mode) = globals.sandbox_mode {
        object.insert(
            "sandbox_mode",
            klaus_agent_core::runtime_values::runtime_codec::encode_value(
                &klaus_agent_core::types::permission::SandboxMode::from(mode),
            )
            .map_err(|error| CliError::Codec(error.to_string()))?,
        );
    }
    if let Some(policy) = globals.approval_policy {
        object.insert(
            "approval_policy",
            klaus_agent_core::runtime_values::runtime_codec::encode_value(
                &klaus_agent_core::types::permission::ApprovalPolicy::from(policy),
            )
            .map_err(|error| CliError::Codec(error.to_string()))?,
        );
    }
    if let Some(reviewer) = globals.approvals_reviewer {
        object.insert(
            "approvals_reviewer",
            klaus_agent_core::runtime_values::runtime_codec::encode_value(
                &klaus_agent_core::types::permission::ApprovalsReviewer::from(reviewer),
            )
            .map_err(|error| CliError::Codec(error.to_string()))?,
        );
    }
    let explicit_policy = [
        "approval_policy",
        "sandbox_mode",
        "approvals_reviewer",
        "approvalPolicy",
        "sandboxMode",
        "approvalsReviewer",
    ]
    .iter()
    .any(|key| object.contains_key(*key));
    if !explicit_policy {
        if let Some((approval, sandbox, reviewer)) = globals.permission_mode.and_then(|mode| {
            klaus_agent_core::schemas::config::legacy_permission_policy(mode.into())
        }) {
            object.insert(
                "approval_policy",
                klaus_agent_core::runtime_values::runtime_codec::encode_value(&approval)
                    .map_err(|error| CliError::Codec(error.to_string()))?,
            );
            object.insert(
                "sandbox_mode",
                klaus_agent_core::runtime_values::runtime_codec::encode_value(&sandbox)
                    .map_err(|error| CliError::Codec(error.to_string()))?,
            );
            object.insert(
                "approvals_reviewer",
                klaus_agent_core::runtime_values::runtime_codec::encode_value(&reviewer)
                    .map_err(|error| CliError::Codec(error.to_string()))?,
            );
        }
    }
    if object.is_empty() {
        return Ok(None);
    }
    Ok(Some(RuntimeJson::Object(object)))
}

/// Parse a JSON value the caller may have given either inline or as a file path.
/// Used by the flags that take a schema or a metadata object, where forcing a
/// shell user to inline a large document would be hostile.
pub async fn json_argument(raw: &str) -> CliResult<RuntimeJson> {
    let trimmed = raw.trim_start();
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        return text::parse_json(raw);
    }
    let bytes = tokio::fs::read(raw)
        .await
        .map_err(|error| CliError::usage(format!("cannot read {raw}: {error}")))?;
    let text_value = RuntimeString::from(String::from_utf8_lossy(&bytes).into_owned());
    runtime_json::parse(&text_value)
        .map_err(|error| CliError::usage(format!("{raw} is not valid JSON: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deprecated_cli_preset_is_translated_before_session_submission() {
        let config = session_config(&Globals {
            permission_mode: Some(crate::cli::PermissionModeArg::BypassPermissions),
            ..Default::default()
        })
        .unwrap()
        .unwrap();
        assert_eq!(
            config
                .get("sandbox_mode")
                .and_then(RuntimeJson::as_string)
                .map(text::text),
            Some("danger-full-access".to_string())
        );
        assert_eq!(
            config
                .get("approval_policy")
                .and_then(RuntimeJson::as_string)
                .map(text::text),
            Some("never".to_string())
        );
        assert!(config.get("permissionMode").is_none());
    }

    #[test]
    fn explicit_cli_policy_does_not_inherit_a_deprecated_full_access_grant() {
        let config = session_config(&Globals {
            permission_mode: Some(crate::cli::PermissionModeArg::BypassPermissions),
            approval_policy: Some(crate::cli::ApprovalPolicyArg::OnRequest),
            ..Default::default()
        })
        .unwrap()
        .unwrap();
        assert!(config.get("sandbox_mode").is_none());
        assert_eq!(
            config
                .get("approval_policy")
                .and_then(RuntimeJson::as_string)
                .map(text::text),
            Some("on-request".to_string())
        );
    }

    fn config_model(globals: &Globals) -> Option<String> {
        let config = session_config(globals).expect("the config object builds");
        // Matched by reference for the reason `session_config` states: the object
        // owns runtime handles and implements `Drop`, so it cannot be moved out of
        // the variant.
        match &config {
            Some(RuntimeJson::Object(object)) => match object.get(RuntimeString::from("model")) {
                Some(RuntimeJson::String(model)) => Some(text::text(model)),
                _ => None,
            },
            _ => None,
        }
    }

    fn with_model(model: Option<&str>) -> Globals {
        Globals {
            model: model.map(str::to_string),
            ..Default::default()
        }
    }

    /// One `--model` value reaches EXACTLY ONE of the two fields, never both.
    ///
    /// They are different questions. `modelSelection` decides which provider and
    /// which credential answer; `config.model` asks a provider already resolved for
    /// a model id. Sending both would choose a provider by selection and then ask
    /// it for a different model, and the endpoint has no rule for which of the two
    /// wins — so the split has to stay total and disjoint. Two functions read the
    /// same flag to make that split, which is exactly the shape that drifts when
    /// only one of them is edited.
    #[test]
    fn a_model_value_reaches_exactly_one_field() {
        for selection in [
            "auto",
            "custom:5f344546",
            "subscription:opus",
            "codex:gpt-5.6-sol",
            "qoder:auto",
            "local:llama3",
        ] {
            let globals = with_model(Some(selection));
            assert_eq!(
                model_selection(&globals),
                Some(selection),
                "{selection} is a selection"
            );
            assert_eq!(
                config_model(&globals),
                None,
                "{selection} must not also be written as a model name"
            );
        }

        for name in [
            "claude-sonnet-4-6",
            "gpt-5.5",
            "us.anthropic.claude-opus-4:1",
        ] {
            let globals = with_model(Some(name));
            assert_eq!(model_selection(&globals), None, "{name} is a model name");
            assert_eq!(
                config_model(&globals).as_deref(),
                Some(name),
                "{name} belongs in config.model"
            );
        }

        // No `--model` at all: neither field is invented.
        let bare = with_model(None);
        assert_eq!(model_selection(&bare), None);
        assert_eq!(config_model(&bare), None);
    }

    /// An explicit `--config-json` model is not overwritten by the flag.
    ///
    /// The flag fills the field in only when the object does not already name it,
    /// so a caller that spelled out a config keeps what they wrote. A selection
    /// still stays out of `config.model` even then.
    #[test]
    fn an_explicit_config_model_survives_the_flag() {
        let mut globals = with_model(Some("gpt-5.5"));
        globals.config_json = Some(r#"{"model":"from-config"}"#.to_string());
        assert_eq!(config_model(&globals).as_deref(), Some("from-config"));

        let mut globals = with_model(Some("custom:5f344546"));
        globals.config_json = Some(r#"{"model":"from-config"}"#.to_string());
        assert_eq!(model_selection(&globals), Some("custom:5f344546"));
        assert_eq!(
            config_model(&globals).as_deref(),
            Some("from-config"),
            "a selection never lands in config.model, and never displaces it"
        );
    }
}
