//! Serve the engine over WebSocket + HTTP/SSE on one port, reusing the Op/Event
//! protocol. Each connection gets its own `ProtocolServer` + Session. HTTP
//! `GET /healthz` and `/readyz` return 200 for liveness probes; every other
//! non-route request gets 404. Auth is enforced at the upgrade / request.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use hyper::body::Incoming;
use hyper::header::{CONNECTION, CONTENT_TYPE, UPGRADE};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::{watch, Notify};
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::Role;
use tokio_tungstenite::WebSocketStream;

use klaus_agent_core::credential_store::CredentialStore;
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeJson};
use klaus_agent_core::store::session_store::{ListOptions, ThreadPage};
use klaus_agent_protocol::{
    CreateSession as CreateSessionFn, CreateSessionArgs as SessionHandlers, CreateSessionFuture,
    ProtocolLifecycle, ProtocolPluginAdmin, ProtocolServer, ProtocolServerOptions,
};

use crate::app_server::{run_app_server, AppServerOptions};
use crate::auth::{bearer_allowed, bind_policy_error, is_loopback_host, origin_allowed};
use crate::endpoint_activity::{EndpointActivity, EndpointActivityKind, EndpointTestGates};
use crate::http_util::{empty_body, full_body, text_response, ResponseBody};
use crate::lifecycle::{parse_lifecycle, CreateSession, Lifecycle, ListThreads, SessionContext};
use crate::sse::{create_http_sse_router_with_activity, HttpSseOptions, HttpSseRouter};
use crate::ws_stream::{ws_to_streams, WsStreams, WsTransportHandle};

pub struct ServeOptions {
    /// Captured endpoint configuration and cwd for native Windows preparation.
    pub windows_sandbox_context: Option<crate::app_server::windows_sandbox::Context>,
    /// Mints a Session per connection (one connection = one session).
    pub create_session: CreateSession,
    /// Secret store used by provider and MCP OAuth for every connection.
    pub credential_store: Option<Arc<dyn CredentialStore>>,
    /// Enumerate persisted threads for `GET /threads`. `None` → the route replies 501.
    pub list_threads: Option<ListThreads>,
    /// Thread lifecycle hooks (list/resume/fork/archive/delete/rename/…) handed to
    /// every connection's `ProtocolServer`.
    ///
    /// Absent means those ops answer "requires a session store", which is right
    /// for an endpoint that has none and wrong for one that does: a client on this
    /// transport can then neither enumerate threads nor resume one, so `--resume`
    /// and `session list` work in process and fail over the socket. Passing the
    /// endpoint's own lifecycle is what makes the transports interchangeable, and
    /// the bearer token is already the boundary that decides who may reach it.
    pub lifecycle: Option<ProtocolLifecycle>,
    /// Endpoint-level plugin ledger manager, for the same reason as `lifecycle`:
    /// without it `plugin list` is unavailable over the socket alone.
    pub plugin_admin: Option<Arc<dyn ProtocolPluginAdmin>>,
    /// Bind host. Defaults to `127.0.0.1` (loopback-only). A non-loopback host REQUIRES `token`.
    pub host: Option<String>,
    /// Bind port. Defaults to `0` (ephemeral) — read the real port back from `RunningServer::port`.
    pub port: Option<u16>,
    /// Bearer token clients must present. Optional for a loopback bind; MANDATORY for non-loopback.
    pub token: Option<String>,
    /// Web origins allowed to connect. Any upgrade carrying an `Origin` is rejected unless listed.
    pub allowed_origins: Option<Vec<String>>,
    /// Suppress the stderr startup banner.
    pub silent: bool,
    #[cfg(test)]
    pub(crate) teardown_test_gates: EndpointTestGates,
}

#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    #[error("{0}")]
    BindPolicy(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// A running server handle.
pub struct RunningServer {
    /// The `ws://host:port` clients connect to.
    pub url: String,
    pub host: String,
    /// The actually-bound port (meaningful when `port: 0` was requested).
    pub port: u16,
    inner: Arc<ServerHandle>,
}

/// Which protocol a WebSocket connection speaks.
///
/// Selected by the client with `?surface=`, because the two are not
/// interchangeable mid-connection: the inner protocol binds one session to the
/// socket, the facade multiplexes many over it and answers a different envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Surface {
    /// The inner Op/Event protocol, one session per connection.
    Protocol,
    /// The JSON-RPC facade — the same surface `klaus app-server` serves on
    /// stdio, so a host reaches a served engine through the method table it
    /// already uses in process.
    AppServer,
}

/// One live WebSocket connection: its protocol server + a socket-close signal.
#[derive(Clone)]
struct WsConn {
    /// Absent for a facade connection: it has no per-connection session server,
    /// and closing the transport is its whole teardown, because `run_app_server`
    /// shuts down every session it created before returning from the EOF.
    ps: Option<Arc<ProtocolServer>>,
    transport: WsTransportHandle,
}

struct WsConnections {
    /// Set atomically with snapshotting `entries` so an in-flight upgrade cannot
    /// register an untracked ProtocolServer after `RunningServer::close()` has
    /// taken its shutdown snapshot.
    closing: bool,
    entries: HashMap<u64, WsConn>,
}

struct ServeState {
    wiring: ConnectionWiring,
    list_threads: Option<ListThreads>,
    token: Option<String>,
    allowed_origins: Option<Vec<String>>,
    sse: Arc<HttpSseRouter>,
    conns: Arc<Mutex<WsConnections>>,
    activity: Arc<EndpointActivity>,
    test_gates: EndpointTestGates,
    conn_id: AtomicU64,
}

/// The endpoint-wide wiring each connection's `ProtocolServer` is built from.
///
/// Grouped rather than passed one by one because it travels together everywhere:
/// these four are what make a connection a view of THIS endpoint, and a
/// connection that received some of them would be a client with an arbitrary
/// subset of the engine's features.
#[derive(Clone)]
struct ConnectionWiring {
    windows_sandbox_context: Option<crate::app_server::windows_sandbox::Context>,
    create_session: CreateSession,
    credential_store: Option<Arc<dyn CredentialStore>>,
    lifecycle: Option<ProtocolLifecycle>,
    plugin_admin: Option<Arc<dyn ProtocolPluginAdmin>>,
}

struct ServerHandle {
    shutdown: watch::Sender<bool>,
    conns: Arc<Mutex<WsConnections>>,
    sse: Arc<HttpSseRouter>,
    accept: Mutex<Option<tokio::task::JoinHandle<()>>>,
    activity: Arc<EndpointActivity>,
    close_state: Mutex<ServerCloseState>,
    close_finished: Notify,
}

#[derive(Default)]
struct ServerCloseState {
    started: bool,
    finished: bool,
}

impl RunningServer {
    /// Close all connections + the listener; resolves only after every
    /// ProtocolServer has completed Session/MCP/LSP/rollout teardown.
    pub async fn close(&self) {
        self.inner.begin_close();
        self.inner.wait_for_close().await;
    }

    #[cfg(test)]
    pub(crate) async fn wait_for_close_start(&self) {
        self.inner.activity.wait_for_admission_closed().await;
    }
}

impl ServerHandle {
    fn begin_close(self: &Arc<Self>) {
        let should_start = {
            let mut close = self.close_state.lock().unwrap();
            if close.started {
                false
            } else {
                close.started = true;
                true
            }
        };
        if !should_start {
            return;
        }

        // Close detached-task admission before stopping the listener or taking
        // connection snapshots. A task that won admission is retained below;
        // every later task is rejected synchronously.
        self.activity.close_admission();
        // Stop accepting.
        let _ = self.shutdown.send(true);
        // Terminate live WebSocket connections (destroy the socket immediately
        // rather than wait on a graceful close handshake).  Entries stay in the
        // retained registry until their start task finishes, so cancellation of
        // the initiating close future cannot make a later close miss them.
        let conns: Vec<WsConn> = {
            let mut state = self.conns.lock().unwrap();
            state.closing = true;
            state.entries.values().cloned().collect()
        };
        for c in &conns {
            c.transport.close();
            if let Some(ps) = &c.ps {
                ps.stop();
            }
        }
        self.sse.begin_close();

        let this = self.clone();
        tokio::spawn(async move {
            for connection in conns {
                if let Some(ps) = &connection.ps {
                    let _ = ps.shutdown_and_wait().await;
                }
                // Do not rely solely on the normal `run_ws_session` path to
                // observe pump exit: a panicking session/start owner can drop
                // its activity guard before the retained transport supervisor
                // publishes completion.
                connection.transport.close_and_wait().await;
            }
            // Tear down HTTP+SSE sessions too.
            this.sse.close().await;
            // Wait for the listener task to finish (socket fully closed).
            let handle = this.accept.lock().unwrap().take();
            if let Some(handle) = handle {
                let _ = handle.await;
            }
            // HTTP connection tasks, WebSocket upgrade tasks, and SSE
            // pre-registration cleanup all retain an activity guard. This is
            // the final endpoint boundary: no detached transport owner remains.
            this.activity.wait_for_idle().await;
            this.conns.lock().unwrap().entries.clear();
            let mut close = this.close_state.lock().unwrap();
            close.finished = true;
            this.close_finished.notify_waiters();
        });
    }

    async fn wait_for_close(&self) {
        loop {
            let notified = self.close_finished.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.close_state.lock().unwrap().finished {
                return;
            }
            notified.await;
        }
    }
}

/// Serve the engine over WebSocket + HTTP/SSE.
pub async fn serve(opts: ServeOptions) -> Result<RunningServer, ServeError> {
    // Empty-string host must collapse to loopback, else it would bind ALL
    // interfaces (the OS treats '' as unspecified) with no token.
    let host = match &opts.host {
        Some(h) if !h.is_empty() => h.clone(),
        _ => "127.0.0.1".to_string(),
    };
    let requested_port = opts.port.unwrap_or(0);
    let token = opts.token.clone();
    #[cfg(test)]
    let test_gates = opts.teardown_test_gates.clone();
    #[cfg(not(test))]
    let test_gates = EndpointTestGates::default();

    if let Some(msg) = bind_policy_error(&host, token.as_deref()) {
        return Err(ServeError::BindPolicy(msg));
    }

    // The HTTP + SSE transport shares this same server/port/auth.
    let activity = Arc::new(EndpointActivity::default());
    let sse = create_http_sse_router_with_activity(
        HttpSseOptions {
            create_session: opts.create_session.clone(),
            credential_store: opts.credential_store.clone(),
            lifecycle: opts.lifecycle.clone(),
            plugin_admin: opts.plugin_admin.clone(),
            token: token.clone(),
            allowed_origins: opts.allowed_origins.clone(),
        },
        activity.clone(),
        test_gates.clone(),
    );

    let listener = TcpListener::bind((host.as_str(), requested_port)).await?;
    let port = listener.local_addr()?.port();

    let conns = Arc::new(Mutex::new(WsConnections {
        closing: false,
        entries: HashMap::new(),
    }));
    let state = Arc::new(ServeState {
        wiring: ConnectionWiring {
            windows_sandbox_context: Some(
                opts.windows_sandbox_context
                    .unwrap_or_default()
                    .capture_cwd(),
            ),
            create_session: opts.create_session,
            credential_store: opts.credential_store,
            lifecycle: opts.lifecycle,
            plugin_admin: opts.plugin_admin,
        },
        list_threads: opts.list_threads,
        token: token.clone(),
        allowed_origins: opts.allowed_origins,
        sse: sse.clone(),
        conns: conns.clone(),
        activity: activity.clone(),
        test_gates,
        conn_id: AtomicU64::new(0),
    });

    let (shutdown_tx, _shutdown_rx0) = watch::channel(false);
    let accept = {
        let state = state.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        let connection_shutdown = shutdown_tx.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    // Once close is observable, do not probabilistically race
                    // a continuously-ready listener backlog. Activity admission
                    // is already closed, and the listener owner exits now.
                    biased;
                    _ = shutdown_rx.changed() => break,
                    accepted = listener.accept() => {
                        let (stream, _peer) = match accepted {
                            Ok(x) => x,
                            Err(_) => continue,
                        };
                        let Some(activity_guard) = state
                            .activity
                            .begin(EndpointActivityKind::HttpConnection)
                        else {
                            continue;
                        };
                        let mut connection_shutdown_rx = connection_shutdown.subscribe();
                        let io = TokioIo::new(stream);
                        let svc_state = state.clone();
                        let test_gates = state.test_gates.clone();
                        let service = service_fn(move |req: Request<Incoming>| {
                            let s = svc_state.clone();
                            async move { Ok::<_, Infallible>(handle_request(req, s).await) }
                        });
                        tokio::spawn(async move {
                            let _activity_guard = activity_guard;
                            test_gates.pause_http_after_admission().await;
                            let connection = http1::Builder::new()
                                .serve_connection(io, service)
                                .with_upgrades();
                            tokio::pin!(connection);
                            tokio::select! {
                                biased;
                                _ = wait_for_shutdown(&mut connection_shutdown_rx) => {
                                    // Do not cancel an in-flight handler. Hyper
                                    // stops admitting requests and the same
                                    // connection future is polled to completion.
                                    connection.as_mut().graceful_shutdown();
                                    let _ = connection.await;
                                }
                                _ = connection.as_mut() => {}
                            }
                        });
                    }
                }
            }
        })
    };

    let url = format!("ws://{}:{}", format_host(&host), port);
    if !opts.silent {
        print_banner(&host, port, token.as_deref());
    }

    Ok(RunningServer {
        url,
        host,
        port,
        inner: Arc::new(ServerHandle {
            shutdown: shutdown_tx,
            conns,
            sse,
            accept: Mutex::new(Some(accept)),
            activity,
            close_state: Mutex::new(ServerCloseState::default()),
            close_finished: Notify::new(),
        }),
    })
}

async fn handle_request(req: Request<Incoming>, state: Arc<ServeState>) -> Response<ResponseBody> {
    // A WebSocket upgrade is handled before any HTTP route (it ignores the path,
    // like the underlying upgrade event).
    if is_ws_upgrade(&req) {
        return handle_ws_upgrade(req, &state);
    }

    let method = req.method().clone();
    let path = req.uri().path().to_string();

    if method == Method::GET && (path == "/healthz" || path == "/readyz") {
        return text_response(200, "ok");
    }

    // List persisted threads (same auth as everything else). 501 if unwired.
    if method == Method::GET && path == "/threads" {
        if !bearer_allowed(req.headers(), state.token.as_deref()) {
            return text_response(401, "unauthorized");
        }
        return match &state.list_threads {
            None => text_response(501, "thread listing not supported"),
            Some(lt) => match lt(ListOptions::default()).await {
                Ok(page) => exact_json_response(200, &page_to_json(&page)),
                Err(e) => text_response(500, &e.to_string()),
            },
        };
    }

    // GET /sessions/{id}/events, POST /sessions/{id}/ops (+ CORS preflight).
    if let Some(resp) = state.sse.handle(req).await {
        return resp;
    }

    text_response(404, "not found")
}

fn is_ws_upgrade(req: &Request<Incoming>) -> bool {
    let h = req.headers();
    let is_upgrade = h
        .get(UPGRADE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.eq_ignore_ascii_case("websocket"))
        .unwrap_or(false);
    is_upgrade && h.contains_key("sec-websocket-key")
}

fn handle_ws_upgrade(
    mut req: Request<Incoming>,
    state: &Arc<ServeState>,
) -> Response<ResponseBody> {
    if !origin_allowed(req.headers(), state.allowed_origins.as_deref()) {
        return reject_upgrade(403, "origin not allowed");
    }
    if !bearer_allowed(req.headers(), state.token.as_deref()) {
        return reject_upgrade(401, "invalid or missing bearer token");
    }
    let accept = match req.headers().get("sec-websocket-key") {
        Some(k) => derive_accept_key(k.as_bytes()),
        None => return reject_upgrade(400, "bad request"),
    };
    let Some(activity_guard) = state.activity.begin(EndpointActivityKind::WebSocketUpgrade) else {
        return reject_upgrade(503, "server is shutting down");
    };
    let surface = match parse_surface(&req.uri().to_string()) {
        Ok(surface) => surface,
        // Rejected rather than downgraded: handing a client that asked for the
        // facade a protocol server would answer none of the methods it is about
        // to call, and it would look like an engine that hung.
        Err(reason) => return reject_upgrade(400, &reason),
    };
    let lifecycle = parse_lifecycle(Some(&req.uri().to_string()));
    let wiring = state.wiring.clone();
    let conns = state.conns.clone();
    let test_gates = state.test_gates.clone();
    let id = state.conn_id.fetch_add(1, Ordering::Relaxed);

    let on_upgrade = hyper::upgrade::on(&mut req);
    tokio::spawn(async move {
        let _activity_guard = activity_guard;
        if let Ok(upgraded) = on_upgrade.await {
            let io = TokioIo::new(upgraded);
            let ws = WebSocketStream::from_raw_socket(io, Role::Server, None).await;
            run_ws_session(ws, wiring, lifecycle, conns, id, test_gates, surface).await;
        }
    });

    Response::builder()
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .header(CONNECTION, "Upgrade")
        .header(UPGRADE, "websocket")
        .header("sec-websocket-accept", accept)
        .body(empty_body())
        .unwrap()
}

async fn run_ws_session<S>(
    ws: WebSocketStream<S>,
    wiring: ConnectionWiring,
    lifecycle: Lifecycle,
    conns: Arc<Mutex<WsConnections>>,
    id: u64,
    test_gates: EndpointTestGates,
    surface: Surface,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let WsStreams {
        input,
        output,
        transport,
    } = ws_to_streams(ws);

    if surface == Surface::AppServer {
        // No wrapped factory and no connect-time resume/fork: the facade's
        // `session/start` carries both per session, which is the only way a
        // connection that multiplexes sessions can express them at all.
        test_gates.pause_websocket_before_registration().await;
        let registered = register_ws_conn(
            &conns,
            id,
            WsConn {
                ps: None,
                transport: transport.clone(),
            },
        );
        if !registered {
            transport.close();
            transport.close_and_wait().await;
            return;
        }
        run_app_server(
            input,
            output,
            AppServerOptions {
                windows_sandbox_context: wiring.windows_sandbox_context,
                create_session: wiring.create_session,
                credential_store: wiring.credential_store,
                lifecycle: wiring.lifecycle,
                plugin_admin: wiring.plugin_admin,
                // This process IS the endpoint, so the config home's own
                // permission rules and user config are the ones it serves.
                permission_rules_path: None,
                config_path: None,
                startup_init_ack_budget: None,
                session_unload_delay: None,
            },
        )
        .await;
        transport.close_and_wait().await;
        conns.lock().unwrap().entries.remove(&id);
        return;
    }

    // Wrap the host factory with this connection's resume/fork intent.
    let create: CreateSessionFn = {
        let cs = wiring.create_session.clone();
        let resume = lifecycle.resume.clone();
        let fork = lifecycle.fork.clone();
        let session_credential_store = wiring.credential_store.clone();
        Arc::new(move |h: SessionHandlers| {
            let cs = cs.clone();
            let resume = resume.clone();
            let fork = fork.clone();
            let credential_store = session_credential_store.clone();
            Box::pin(async move {
                cs(SessionContext {
                    durable_permissions: h.durable_permissions,
                    approval_handler: h.approval_handler,
                    elicit_handler: h.elicit_handler,
                    mcp_elicitation_presenter: h.mcp_elicitation_presenter,
                    mcp_authorize_handler: h.mcp_authorize_handler,
                    credential_store,
                    init: h.init,
                    provider: h.provider,
                    mcp_server_configs: h.mcp_server_configs,
                    capability_roots: h.capability_roots,
                    selected_capability_roots: h.selected_capability_roots,
                    dynamic_tools: h.dynamic_tools,
                    agents: h.agents,
                    skills: h.skills,
                    output_styles: h.output_styles,
                    hooks: h.hooks,
                    pricing: h.pricing,
                    resume,
                    fork,
                    fork_live: None,
                    side_task: None,
                })
                .await
            }) as CreateSessionFuture
        })
    };

    let ps = Arc::new(ProtocolServer::new(ProtocolServerOptions {
        create_session: create,
        // Endpoint-wide, and shared by every connection on purpose: they are all
        // views of the one store this process serves. Distinct from `lifecycle`
        // above, which is only this connection's resume/fork intent.
        lifecycle: wiring.lifecycle,
        plugin_admin: wiring.plugin_admin,
        credential_store: wiring.credential_store,
        input,
        output,
    }));
    test_gates.pause_websocket_before_registration().await;
    let registered = register_ws_conn(
        &conns,
        id,
        WsConn {
            ps: Some(ps.clone()),
            transport: transport.clone(),
        },
    );
    if !registered {
        transport.close();
        let _ = ps.shutdown_and_wait().await;
        transport.close_and_wait().await;
        return;
    }

    // Resolves when the socket EOFs (client gone) or the connection is closed.
    ps.clone().start().await;
    // `ProtocolServer::start()` includes awaited teardown on every exit path.
    transport.close_and_wait().await;
    conns.lock().unwrap().entries.remove(&id);
}

/// Track a connection so a graceful close can reach it. `false` means the server
/// is already closing and nothing was registered — the caller must tear its own
/// connection down, because the shutdown snapshot will never see it.
fn register_ws_conn(conns: &Mutex<WsConnections>, id: u64, conn: WsConn) -> bool {
    let mut state = conns.lock().unwrap();
    if state.closing {
        return false;
    }
    state.entries.insert(id, conn);
    true
}

/// Read `?surface=` off the upgrade URL. Absent (and the explicit `protocol`)
/// keeps the inner Op/Event protocol, which is what every existing client
/// expects on this port.
fn parse_surface(url: &str) -> Result<Surface, String> {
    let Some((_, query)) = url.split_once('?') else {
        return Ok(Surface::Protocol);
    };
    let requested = url::form_urlencoded::parse(query.as_bytes())
        .find(|(key, _)| key == "surface")
        .map(|(_, value)| value.into_owned());
    match requested.as_deref() {
        None | Some("") | Some("protocol") => Ok(Surface::Protocol),
        Some("appServer") => Ok(Surface::AppServer),
        Some(other) => Err(format!(
            "unknown surface `{other}`: expected `protocol` or `appServer`"
        )),
    }
}

async fn wait_for_shutdown(shutdown: &mut watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    while shutdown.changed().await.is_ok() {
        if *shutdown.borrow_and_update() {
            return;
        }
    }
}

/// Reject a WebSocket upgrade with a plain HTTP error response (the client's
/// handshake fails with this status).
fn reject_upgrade(code: u16, message: &str) -> Response<ResponseBody> {
    Response::builder()
        .status(StatusCode::from_u16(code).unwrap())
        .header(CONNECTION, "close")
        .header(CONTENT_TYPE, "text/plain")
        .body(full_body(message.to_string()))
        .unwrap()
}

/// Encode a `ThreadPage` as `{ items, nextCursor? }` without projecting exact
/// runtime strings through Serde's scalar-only `String` domain.
fn page_to_json(page: &ThreadPage) -> RuntimeJson {
    runtime_codec::encode_value(page).expect("ThreadPage exact encoding must be infallible")
}

fn exact_json_response(code: u16, value: &RuntimeJson) -> Response<ResponseBody> {
    Response::builder()
        .status(StatusCode::from_u16(code).unwrap())
        .header(CONTENT_TYPE, "application/json")
        .body(full_body(runtime_json::stringify(value).to_node_utf8()))
        .unwrap()
}

/// Bracket an IPv6 host for URL use.
fn format_host(host: &str) -> String {
    if host.contains(':') {
        format!("[{host}]")
    } else {
        host.to_string()
    }
}

fn print_banner(host: &str, port: u16, token: Option<&str>) {
    let h = format_host(host);
    let mut lines = vec![
        "klaus-agent-server (WebSocket)".to_string(),
        format!("  listening on: ws://{h}:{port}"),
        format!("  healthz:      http://{h}:{port}/healthz"),
    ];
    if is_loopback_host(host) {
        lines.push("  note: loopback only — use SSH port-forwarding for remote access".to_string());
    } else {
        let has_token = token.map(|t| !t.is_empty()).unwrap_or(false);
        lines.push(format!(
            "  note: reachable off-box; bearer token {}",
            if has_token { "required" } else { "MISSING" }
        ));
    }
    eprintln!("{}", lines.join("\n"));
}
