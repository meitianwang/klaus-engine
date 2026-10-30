//! HTTP + Server-Sent-Events transport, the cloud-friendly sibling of the
//! WebSocket one. It reuses the SAME Op/Event `ProtocolServer`:
//!
//!   GET  /sessions/{id}/events  → an SSE stream = the session's OUTPUT (engine
//!                                 events). One open stream = one session.
//!   POST /sessions/{id}/ops     → the session's INPUT: the JSON body is one Op.
//!
//! The two-channel split (POST in, SSE out) is inherent to HTTP: SSE is one-way.
//! The `{id}` correlates them; the client picks it. The session lives for the
//! life of the SSE stream and is torn down when that stream closes.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use http::response::Builder;
use http_body_util::{BodyExt, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::header::{CACHE_CONTROL, CONNECTION, CONTENT_TYPE};
use hyper::{Method, Request, Response, StatusCode, Uri};
use tokio::sync::{mpsc, watch, Notify};
use tokio::task::JoinHandle;

use klaus_agent_core::credential_store::CredentialStore;
use klaus_agent_protocol::{
    CreateSession as CreateSessionFn, CreateSessionArgs as SessionHandlers, CreateSessionFuture,
    ProtocolLifecycle, ProtocolPluginAdmin, ProtocolServer, ProtocolServerOptions,
};

use crate::auth::{bearer_allowed, origin_allowed};
use crate::endpoint_activity::{
    EndpointActivity, EndpointActivityGuard, EndpointActivityKind, EndpointTestGates,
};
use crate::http_util::{empty_body, full_body, ResponseBody};
use crate::lifecycle::{parse_lifecycle, CreateSession, SessionContext};
use crate::line_stream::{ChannelReader, ChannelWriter};

/// Max bytes for a POST /ops body — an Op is tiny; cap it so a huge body can't
/// OOM the server.
const MAX_OP_BODY_BYTES: usize = 1 << 20; // 1 MiB

pub struct HttpSseOptions {
    pub create_session: CreateSession,
    pub credential_store: Option<Arc<dyn CredentialStore>>,
    /// Thread lifecycle hooks given to every SSE session's `ProtocolServer`, so
    /// this transport answers `list_sessions`/`resume_session`/… the same way the
    /// in-process one does instead of "requires a session store".
    pub lifecycle: Option<ProtocolLifecycle>,
    /// Endpoint-level plugin ledger manager, for the same reason as `lifecycle`.
    pub plugin_admin: Option<Arc<dyn ProtocolPluginAdmin>>,
    pub token: Option<String>,
    pub allowed_origins: Option<Vec<String>>,
}

/// One live SSE session: the input line sender feeding the `ProtocolServer` and
/// the retained owner for protocol, forwarding, and registry completion.
struct SseEntry {
    /// Taken as soon as teardown is signalled so a concurrent POST cannot be
    /// accepted after the ProtocolServer has stopped reading.
    input: Option<mpsc::UnboundedSender<String>>,
    owner: Arc<SseSessionOwner>,
    /// Generation tag: distinguishes a re-opened id from the entry a slow op read started against.
    gen: u64,
}

/// Retained completion for both detached pieces of one SSE transport.  Router
/// close waits on this boundary rather than only on `ProtocolServer`, because
/// the forwarding pump and the `start()` owner must also have exited and the
/// registry entry must have been removed.
#[derive(Default)]
struct SseSessionCompletion {
    finished: Mutex<bool>,
    notify: Notify,
    #[cfg(test)]
    wait_entered: Notify,
}

impl SseSessionCompletion {
    async fn wait(&self) {
        #[cfg(test)]
        self.wait_entered.notify_one();
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if *self.finished.lock().unwrap() {
                return;
            }
            notified.await;
        }
    }

    fn finish(&self) {
        let mut finished = self.finished.lock().unwrap();
        if !*finished {
            *finished = true;
            self.notify.notify_waiters();
        }
    }
}

struct SseSessionOwner {
    ps: Arc<ProtocolServer>,
    cancel: watch::Sender<bool>,
    completion: Arc<SseSessionCompletion>,
}

impl SseSessionOwner {
    fn new(ps: Arc<ProtocolServer>, cancel: watch::Sender<bool>) -> Arc<Self> {
        Arc::new(Self {
            ps,
            cancel,
            completion: Arc::new(SseSessionCompletion::default()),
        })
    }

    /// Transport cancellation and protocol stop are synchronous so no POST or
    /// continuously-ready output branch can win after teardown is observable.
    fn signal_stop(&self) {
        let _ = self.cancel.send(true);
        self.ps.stop();
    }

    async fn wait(&self) {
        self.completion.wait().await;
    }

    fn completion(&self) -> Arc<SseSessionCompletion> {
        self.completion.clone()
    }
}

/// Owns every resource created before registration becomes visible.  If the
/// GET future is cancelled at the test gate (or any future await), Drop starts
/// retained cleanup and transfers the activity guard to it.
struct PendingSseSessionOwner {
    owner: Arc<SseSessionOwner>,
    input: Option<mpsc::UnboundedSender<String>>,
    forward: Option<JoinHandle<()>>,
    registration_guard: Option<EndpointActivityGuard>,
}

impl PendingSseSessionOwner {
    fn new(
        owner: Arc<SseSessionOwner>,
        input: mpsc::UnboundedSender<String>,
        forward: JoinHandle<()>,
        registration_guard: EndpointActivityGuard,
    ) -> Self {
        Self {
            owner,
            input: Some(input),
            forward: Some(forward),
            registration_guard: Some(registration_guard),
        }
    }

    fn owner(&self) -> Arc<SseSessionOwner> {
        self.owner.clone()
    }

    fn take_input(&mut self) -> mpsc::UnboundedSender<String> {
        self.input
            .take()
            .expect("pending SSE input must be transferred exactly once")
    }

    /// Rejected registration is still an asynchronously retained owner.  The
    /// caller joins its completion, while cancellation of that caller cannot
    /// cancel the spawned cleanup or release the registration guard early.
    async fn reject_and_wait(mut self) {
        let owner = self.owner.clone();
        self.spawn_rejected_cleanup();
        owner.wait().await;
    }

    /// Install the single registered-session coordinator.  It owns both child
    /// tasks, always executes shared ProtocolServer shutdown, and publishes
    /// completion only after the generation-checked registry removal.
    fn activate(mut self, sessions: Arc<Mutex<SseSessions>>, id: String, gen: u64) {
        debug_assert!(self.input.is_none());
        let owner = self.owner.clone();
        let forward = self
            .forward
            .take()
            .expect("SSE forward task must have one retained owner");
        let registration_guard = self.registration_guard.take();
        tokio::spawn(async move {
            let start_owner = tokio::spawn(owner.ps.clone().start());
            let _ = start_owner.await;

            // A normal protocol EOF and an explicit transport close converge
            // here.  Even a panicked start task cannot skip shared teardown.
            owner.signal_stop();
            let _ = owner.ps.shutdown_and_wait().await;
            stop_forward_task(forward).await;
            finish_teardown(&sessions, &id, gen, &owner.completion);
        });
        // Successful insertion is already visible to close(); that registry
        // owner now retains completion, so the admission guard can be released.
        drop(registration_guard);
    }

    fn spawn_rejected_cleanup(&mut self) {
        let Some(forward) = self.forward.take() else {
            return;
        };
        let input = self.input.take();
        let registration_guard = self.registration_guard.take();
        let owner = self.owner.clone();

        // Make rejection observable before returning control or spawning the
        // async waiter. Dropping input also gives a concurrently-started reader
        // EOF, while stop handles the before-start case.
        drop(input);
        owner.signal_stop();
        tokio::spawn(async move {
            let _ = owner.ps.shutdown_and_wait().await;
            stop_forward_task(forward).await;
            owner.completion.finish();
            drop(registration_guard);
        });
    }
}

impl Drop for PendingSseSessionOwner {
    fn drop(&mut self) {
        self.spawn_rejected_cleanup();
    }
}

/// The forward pump contains no protocol teardown and is therefore safe to
/// abort after signalling its biased cancellation branch.  Awaiting the join
/// still proves that its final body sender has been dropped before completion.
async fn stop_forward_task(forward: JoinHandle<()>) {
    forward.abort();
    let _ = forward.await;
}

struct SseSessions {
    /// Serialized with insertion/snapshotting so `close()` cannot miss an
    /// in-flight GET that is about to register a ProtocolServer.
    closing: bool,
    entries: HashMap<String, SseEntry>,
}

#[derive(Default)]
struct SseCloseState {
    started: bool,
    finished: bool,
}

#[derive(Default)]
struct SseCloseCompletion {
    state: Mutex<SseCloseState>,
    finished: Notify,
}

impl SseCloseCompletion {
    async fn wait(&self) {
        loop {
            let notified = self.finished.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.state.lock().unwrap().finished {
                return;
            }
            notified.await;
        }
    }

    fn finish(&self) {
        let mut state = self.state.lock().unwrap();
        if !state.finished {
            state.finished = true;
            self.finished.notify_waiters();
        }
    }
}

/// Spawn the sole router-owned close future.  It is deliberately detached from
/// every `close()` caller so aborting the initiator cannot discard ownership.
fn spawn_sse_close_owner(
    completions: Vec<Arc<SseSessionCompletion>>,
    sessions: Arc<Mutex<SseSessions>>,
    close: Arc<SseCloseCompletion>,
    activity: Arc<EndpointActivity>,
) {
    tokio::spawn(async move {
        for completion in completions {
            completion.wait().await;
        }
        // A GET admitted before close may still be between ProtocolServer
        // construction and registry insertion. Its guard remains retained
        // through the rejected-registration cleanup.
        activity
            .wait_for_kind_idle(EndpointActivityKind::SseRegistration)
            .await;
        sessions.lock().unwrap().entries.clear();
        close.finish();
    });
}

pub struct HttpSseRouter {
    create_session: CreateSession,
    credential_store: Option<Arc<dyn CredentialStore>>,
    lifecycle: Option<ProtocolLifecycle>,
    plugin_admin: Option<Arc<dyn ProtocolPluginAdmin>>,
    token: Option<String>,
    allowed_origins: Option<Vec<String>>,
    sessions: Arc<Mutex<SseSessions>>,
    gen: AtomicU64,
    close: Arc<SseCloseCompletion>,
    activity: Arc<EndpointActivity>,
    test_gates: EndpointTestGates,
}

pub fn create_http_sse_router(opts: HttpSseOptions) -> Arc<HttpSseRouter> {
    create_http_sse_router_with_activity(
        opts,
        Arc::new(EndpointActivity::default()),
        EndpointTestGates::default(),
    )
}

pub(crate) fn create_http_sse_router_with_activity(
    opts: HttpSseOptions,
    activity: Arc<EndpointActivity>,
    test_gates: EndpointTestGates,
) -> Arc<HttpSseRouter> {
    Arc::new(HttpSseRouter {
        create_session: opts.create_session,
        credential_store: opts.credential_store,
        lifecycle: opts.lifecycle,
        plugin_admin: opts.plugin_admin,
        token: opts.token,
        allowed_origins: opts.allowed_origins,
        sessions: Arc::new(Mutex::new(SseSessions {
            closing: false,
            entries: HashMap::new(),
        })),
        gen: AtomicU64::new(0),
        close: Arc::new(SseCloseCompletion::default()),
        activity,
        test_gates,
    })
}

impl HttpSseRouter {
    /// Handle a request if it's an HTTP+SSE route. `Some(resp)` when handled,
    /// `None` to fall through.
    pub async fn handle(&self, req: Request<Incoming>) -> Option<Response<ResponseBody>> {
        let method = req.method().clone();
        let uri = req.uri().clone();
        let path = uri.path();
        let origin = req
            .headers()
            .get("origin")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let cors = self.cors_origin(origin.as_deref());

        // CORS preflight for browser clients (a GET/POST carrying Authorization
        // is not a "simple" request).
        if method == Method::OPTIONS && (events_id(path).is_some() || ops_id(path).is_some()) {
            let b = apply_cors(Response::builder().status(StatusCode::NO_CONTENT), &cors);
            return Some(b.body(empty_body()).unwrap());
        }

        if method == Method::GET {
            if let Some(id_enc) = events_id(path) {
                if !origin_allowed(req.headers(), self.allowed_origins.as_deref()) {
                    return Some(self.deny(403, "origin not allowed", &cors));
                }
                if !bearer_allowed(req.headers(), self.token.as_deref()) {
                    return Some(self.deny(401, "invalid or missing bearer token", &cors));
                }
                let id = match decode_uri_component(id_enc) {
                    Ok(id) => id,
                    Err(_) => return Some(self.deny(400, "malformed session id", &cors)),
                };
                return Some(self.open_sse(&id, &uri, &cors).await);
            }
        }

        if method == Method::POST {
            if let Some(id_enc) = ops_id(path) {
                if !origin_allowed(req.headers(), self.allowed_origins.as_deref()) {
                    return Some(self.deny(403, "origin not allowed", &cors));
                }
                if !bearer_allowed(req.headers(), self.token.as_deref()) {
                    return Some(self.deny(401, "invalid or missing bearer token", &cors));
                }
                let id = match decode_uri_component(id_enc) {
                    Ok(id) => id,
                    Err(_) => return Some(self.deny(400, "malformed session id", &cors)),
                };
                return Some(self.post_op(&id, req, &cors).await);
            }
        }

        None
    }

    /// Tear down every live SSE session and wait for each persistent Session's
    /// MCP/LSP/rollout cleanup to complete.
    pub async fn close(&self) {
        self.begin_close();
        self.close.wait().await;
    }

    /// Synchronously close registration and start one router-owned teardown task.
    /// The task, rather than an arbitrary caller future, owns the wait so a
    /// cancelled close caller cannot orphan the completion.
    pub(crate) fn begin_close(&self) {
        self.activity.close_admission();
        let should_start = {
            let mut close = self.close.state.lock().unwrap();
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

        let completions: Vec<Arc<SseSessionCompletion>> = {
            let mut state = self.sessions.lock().unwrap();
            state.closing = true;
            state
                .entries
                .values_mut()
                .map(|entry| {
                    let _ = entry.input.take();
                    entry.owner.signal_stop();
                    entry.owner.completion()
                })
                .collect()
        };
        spawn_sse_close_owner(
            completions,
            self.sessions.clone(),
            self.close.clone(),
            self.activity.clone(),
        );
    }

    async fn open_sse(&self, id: &str, uri: &Uri, cors: &Option<String>) -> Response<ResponseBody> {
        let Some(registration_guard) = self.activity.begin(EndpointActivityKind::SseRegistration)
        else {
            return self.deny(503, "server is shutting down", cors);
        };
        {
            let state = self.sessions.lock().unwrap();
            if state.closing {
                return self.deny(503, "server is shutting down", cors);
            }
            if state.entries.contains_key(id) {
                return self.deny(409, "session id already in use", cors);
            }
        }

        // ops → ProtocolServer, events ← ProtocolServer, SSE bytes → client.
        let (input_tx, input_rx) = mpsc::unbounded_channel::<String>();
        let (output_tx, output_rx) = mpsc::unbounded_channel::<String>();
        let (body_tx, body_rx) = mpsc::unbounded_channel::<Bytes>();
        let (cancel_tx, _cancel_rx0) = watch::channel(false);

        // Initial comment — flushes headers, defeats proxy buffering.
        let _ = body_tx.send(Bytes::from_static(b": ok\n\n"));

        // Forward task: each engine-event line becomes one SSE `data:` event.
        let forward = {
            let mut cancel_rx = cancel_tx.subscribe();
            let body_tx = body_tx.clone();
            let mut output_rx = output_rx;
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        biased;
                        _ = cancel_rx.changed() => break,
                        line = output_rx.recv() => match line {
                            Some(l) => {
                                if let Some(bytes) = format_sse_data(&l) {
                                    if body_tx.send(bytes).is_err() { break; }
                                }
                            }
                            None => break,
                        }
                    }
                }
                // Dropping body_tx ends the SSE stream.
            })
        };

        // Carry this stream's connect-time resume/fork intent into create_session.
        let lifecycle = parse_lifecycle(Some(&uri.to_string()));
        let cs = self.create_session.clone();
        let session_credential_store = self.credential_store.clone();
        let create: CreateSessionFn = Arc::new(move |h: SessionHandlers| {
            let cs = cs.clone();
            let resume = lifecycle.resume.clone();
            let fork = lifecycle.fork.clone();
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
        });

        let ps = Arc::new(ProtocolServer::new(ProtocolServerOptions {
            create_session: create,
            // The endpoint's, shared across sessions. Not to be confused with the
            // per-stream `lifecycle` above, which is only a resume/fork intent.
            lifecycle: self.lifecycle.clone(),
            plugin_admin: self.plugin_admin.clone(),
            credential_store: self.credential_store.clone(),
            input: Box::new(ChannelReader::new(input_rx)),
            output: Box::new(ChannelWriter::new(output_tx)),
        }));
        let owner = SseSessionOwner::new(ps, cancel_tx);
        let mut pending = PendingSseSessionOwner::new(owner, input_tx, forward, registration_guard);
        self.test_gates.pause_sse_before_registration().await;

        let gen = self.gen.fetch_add(1, Ordering::Relaxed);
        let registration_error = {
            let mut state = self.sessions.lock().unwrap();
            if state.closing {
                Some((503, "server is shutting down"))
            } else if state.entries.contains_key(id) {
                Some((409, "session id already in use"))
            } else {
                state.entries.insert(
                    id.to_string(),
                    SseEntry {
                        input: Some(pending.take_input()),
                        owner: pending.owner(),
                        gen,
                    },
                );
                None
            }
        };
        if let Some((status, message)) = registration_error {
            pending.reject_and_wait().await;
            return self.deny(status, message, cors);
        }

        // The retained coordinator owns ProtocolServer::start, the forwarding
        // pump, shared shutdown, and generation-checked registry cleanup.
        pending.activate(self.sessions.clone(), id.to_string(), gen);

        // Build the streaming SSE body. A drop guard folded into the stream state
        // tears the session down when the client disconnects (the body is dropped).
        let guard = SseDropGuard {
            sessions: self.sessions.clone(),
            id: id.to_string(),
            gen,
        };
        let state = SseBodyState {
            rx: body_rx,
            _guard: guard,
        };
        let stream = futures::stream::unfold(state, |mut st| async move {
            st.rx
                .recv()
                .await
                .map(|bytes| (Ok::<Frame<Bytes>, Infallible>(Frame::data(bytes)), st))
        });
        let body = StreamBody::new(stream).boxed_unsync();

        let b = Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "text/event-stream")
            .header(CACHE_CONTROL, "no-cache, no-transform")
            .header(CONNECTION, "keep-alive");
        apply_cors(b, cors).body(body).unwrap()
    }

    async fn post_op(
        &self,
        id: &str,
        req: Request<Incoming>,
        cors: &Option<String>,
    ) -> Response<ResponseBody> {
        let gen = {
            let state = self.sessions.lock().unwrap();
            match state.entries.get(id) {
                Some(e) if e.input.is_some() => e.gen,
                Some(_) => return self.deny(409, "session closed", cors),
                None => {
                    return self.deny(404, "unknown session (open its /events stream first)", cors)
                }
            }
        };

        // Read the body with a hard byte cap so a huge POST can't exhaust memory
        // (an Op is tiny). Past the cap we STOP accumulating but keep draining the
        // rest so responding + destroying mid-upload doesn't race a reset against
        // the 413 the client is waiting for.
        let mut body = req.into_body();
        let mut chunks: Vec<u8> = Vec::new();
        let mut size = 0usize;
        let mut too_large = false;
        loop {
            match body.frame().await {
                Some(Ok(frame)) => {
                    if let Ok(data) = frame.into_data() {
                        if too_large {
                            continue; // draining the rest; don't buffer
                        }
                        size += data.len();
                        if size > MAX_OP_BODY_BYTES {
                            too_large = true;
                            continue;
                        }
                        chunks.extend_from_slice(&data);
                    }
                }
                Some(Err(_)) => return self.deny(400, "failed to read body", cors),
                None => break,
            }
        }
        if too_large {
            return self.deny(413, "op body too large", cors);
        }
        let line = String::from_utf8_lossy(&chunks).trim().to_string();
        if line.is_empty() {
            return self.deny(400, "empty op body", cors);
        }

        // The session may have torn down during the awaited body read — re-check
        // liveness (same id, same generation), and guard the push.
        let sent = {
            let state = self.sessions.lock().unwrap();
            let input = state
                .entries
                .get(id)
                .filter(|entry| entry.gen == gen)
                .and_then(|entry| entry.input.as_ref());
            input.is_some_and(|input| input.send(format!("{line}\n")).is_ok())
        };
        if !sent {
            return self.deny(409, "session closed", cors);
        }

        let b = Response::builder()
            .status(StatusCode::ACCEPTED)
            .header(CONTENT_TYPE, "text/plain");
        apply_cors(b, cors)
            .body(full_body("accepted".to_string()))
            .unwrap()
    }

    /// The allow-listed origin to echo back, when the request's Origin is listed.
    fn cors_origin(&self, origin: Option<&str>) -> Option<String> {
        let o = origin?;
        let allowed = self.allowed_origins.as_deref().unwrap_or(&[]);
        if allowed.iter().any(|a| a == o) {
            Some(o.to_string())
        } else {
            None
        }
    }

    fn deny(&self, code: u16, message: &str, cors: &Option<String>) -> Response<ResponseBody> {
        let b = Response::builder()
            .status(StatusCode::from_u16(code).unwrap())
            .header(CONTENT_TYPE, "text/plain");
        apply_cors(b, cors)
            .body(full_body(message.to_string()))
            .unwrap()
    }
}

/// Emit CORS headers when the request's Origin is explicitly allow-listed.
fn apply_cors(b: Builder, cors: &Option<String>) -> Builder {
    match cors {
        Some(o) => b
            .header("access-control-allow-origin", o.clone())
            .header(
                "access-control-allow-headers",
                "authorization, content-type",
            )
            .header("access-control-allow-methods", "GET, POST, OPTIONS"),
        None => b,
    }
}

/// Signal synchronous teardown while retaining the entry until the async
/// ProtocolServer cleanup completes.  Retention is what lets router `close()`
/// find and await a disconnect already in progress.
fn signal_teardown(sessions: &Mutex<SseSessions>, id: &str, gen: u64) {
    let teardown = {
        let mut state = sessions.lock().unwrap();
        let Some(entry) = state.entries.get_mut(id).filter(|entry| entry.gen == gen) else {
            return;
        };
        (entry.input.take(), entry.owner.clone())
    };
    let (input, owner) = teardown;
    drop(input);
    owner.signal_stop();
}

/// Remove an entry only after `ProtocolServer::start()`, shared shutdown, and
/// the forwarding task have completed. The generation check prevents an old
/// owner from touching a later session that reused the same id. Removal and
/// completion publication share the close-snapshot mutex, eliminating a gap in
/// which close could miss an owner that had not published completion yet.
fn finish_teardown(
    sessions: &Mutex<SseSessions>,
    id: &str,
    gen: u64,
    completion: &SseSessionCompletion,
) {
    let entry = {
        let mut state = sessions.lock().unwrap();
        let entry = if state.entries.get(id).map(|entry| entry.gen) == Some(gen) {
            state.entries.remove(id)
        } else {
            None
        };
        // Publish while holding the same registry lock used by close snapshot.
        // Close therefore either retains this still-present owner or observes
        // it absent only after its completion is already visible.
        completion.finish();
        entry
    };
    if let Some(entry) = entry {
        entry.owner.signal_stop();
    }
}

struct SseDropGuard {
    sessions: Arc<Mutex<SseSessions>>,
    id: String,
    gen: u64,
}
impl Drop for SseDropGuard {
    fn drop(&mut self) {
        // Client disconnected (or the stream ended) → tear the session down.
        signal_teardown(&self.sessions, &self.id, self.gen);
    }
}
struct SseBodyState {
    rx: mpsc::UnboundedReceiver<Bytes>,
    _guard: SseDropGuard,
}

/// Frame one `…\n` engine-event line as one SSE `data:` event. `None` for an
/// effectively empty line.
fn format_sse_data(chunk: &str) -> Option<Bytes> {
    let line = chunk.trim_end_matches('\n');
    if line.is_empty() {
        return None;
    }
    Some(Bytes::from(format!("data: {line}\n\n")))
}

/// `^/sessions/([^/]+)/events$` — returns the (still-encoded) id segment.
fn events_id(path: &str) -> Option<&str> {
    let rest = path.strip_prefix("/sessions/")?;
    let id = rest.strip_suffix("/events")?;
    if id.is_empty() || id.contains('/') {
        None
    } else {
        Some(id)
    }
}

/// `^/sessions/([^/]+)/ops$` — returns the (still-encoded) id segment.
fn ops_id(path: &str) -> Option<&str> {
    let rest = path.strip_prefix("/sessions/")?;
    let id = rest.strip_suffix("/ops")?;
    if id.is_empty() || id.contains('/') {
        None
    } else {
        Some(id)
    }
}

/// Decode a URI path segment, failing on malformed percent-encoding or invalid
/// UTF-8 (a bad id is a clean 400, never a crash). Does not treat `+` specially.
fn decode_uri_component(s: &str) -> Result<String, ()> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return Err(());
            }
            let h = hex_val(bytes[i + 1]).ok_or(())?;
            let l = hex_val(bytes[i + 2]).ok_or(())?;
            out.push((h << 4) | l);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|_| ())
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn retained_close_owner_waits_for_session_completion_after_caller_abort() {
        let session_completion = Arc::new(SseSessionCompletion::default());
        let sessions = Arc::new(Mutex::new(SseSessions {
            closing: true,
            entries: HashMap::new(),
        }));
        let close = Arc::new(SseCloseCompletion::default());
        close.state.lock().unwrap().started = true;
        let activity = Arc::new(EndpointActivity::default());

        spawn_sse_close_owner(
            vec![session_completion.clone()],
            sessions,
            close.clone(),
            activity,
        );
        tokio::time::timeout(
            Duration::from_secs(1),
            session_completion.wait_entered.notified(),
        )
        .await
        .expect("router close owner did not join retained SSE completion");
        assert!(
            !close.state.lock().unwrap().finished,
            "router close finished before the SSE transport owner"
        );

        let first_close = {
            let close = close.clone();
            tokio::spawn(async move { close.wait().await })
        };
        first_close.abort();
        assert!(first_close.await.unwrap_err().is_cancelled());
        assert!(
            !close.state.lock().unwrap().finished,
            "aborting a close waiter discarded the retained owner"
        );

        let second_close = {
            let close = close.clone();
            tokio::spawn(async move { close.wait().await })
        };
        session_completion.finish();
        tokio::time::timeout(Duration::from_secs(1), second_close)
            .await
            .expect("retained SSE close did not finish after owner completion")
            .expect("second close waiter panicked");
        assert!(close.state.lock().unwrap().finished);
    }

    #[test]
    fn formats_sse_frames() {
        assert_eq!(
            format_sse_data("hello\n").unwrap(),
            Bytes::from("data: hello\n\n")
        );
        assert_eq!(
            format_sse_data("a\n\n").unwrap(),
            Bytes::from("data: a\n\n")
        );
        assert!(format_sse_data("\n").is_none());
        assert!(format_sse_data("").is_none());
    }

    #[test]
    fn matches_routes() {
        assert_eq!(events_id("/sessions/abc/events"), Some("abc"));
        assert_eq!(ops_id("/sessions/abc/ops"), Some("abc"));
        assert!(events_id("/sessions//events").is_none());
        assert!(events_id("/sessions/a/b/events").is_none());
        assert!(events_id("/healthz").is_none());
        assert!(ops_id("/sessions/abc/events").is_none());
    }

    #[test]
    fn decodes_uri_component_and_rejects_bad() {
        assert_eq!(decode_uri_component("abc").unwrap(), "abc");
        assert_eq!(decode_uri_component("a%20b").unwrap(), "a b");
        assert!(decode_uri_component("%zz").is_err());
        assert!(decode_uri_component("%2").is_err());
    }
}
