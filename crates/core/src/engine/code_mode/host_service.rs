//! The host side of the code-mode connection: serve one client's sessions.
//!
//! This is the mirror image of [`super::remote_session`]. The host owns the
//! cells (in-process V8 runtimes, one session each) and answers the client's
//! operations; whenever a cell needs a nested tool call or wants to surface
//! status text, the host asks the CLIENT, because only the client's engine has
//! the pipeline that enforces permissions and hooks.
//!
//! The host is deliberately defensive about its client: request and session ids
//! must be fresh, in-flight work is bounded, and one client that disappears
//! tears down exactly its own state.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, oneshot, Mutex, Notify, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::engine::code_mode::host::{
    CapabilitySet, ClientToHost, DelegateRequest, DelegateRequestId, DelegateResponse,
    EncodedFrame, FramedReader, FramedWriter, HandshakeRejectReason, HostHello, HostRequest,
    HostResponse, HostToClient, ProtocolVersion, RequestId, SessionId, SupportedProtocolVersions,
    WireReplEvalRequest, WireReplOutcome, WireResult, WireRuntimeResponse,
    MAX_PENDING_DELEGATE_CALLS,
};
use crate::engine::code_mode::protocol::{CellId, CodeModeNestedToolCall, CodeModeToolKind};
use crate::engine::code_mode::runtime::repl_session::{
    ReplExecRequest, ReplInvokeFn, ReplSession, ReplToolBinding,
};
use crate::engine::code_mode::runtime::InProcessCodeModeSession;
use crate::engine::code_mode::session::{
    CodeModeSessionDelegate, NotificationFuture, StartedCell, ToolInvocationFuture,
};
use crate::runtime_values::{RuntimeJson, RuntimeString};
use crate::sync_ext::MutexExt;
use crate::types::errors::CodeModeToolRejection;

#[cfg(test)]
#[path = "host_service_tests.rs"]
mod tests;

/// Concurrent client operations one host will work on.
const MAX_IN_FLIGHT_REQUESTS: usize = 256;
/// Cells one host will keep running at once.
const MAX_ACTIVE_CELLS: usize = 128;
/// Retired ids kept so a replay is caught rather than served twice.
const MAX_RECENT_REQUEST_IDS: usize = 4096;
const MAX_RECENT_SESSION_IDS: usize = 4096;
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const CELL_MESSAGE_CAPACITY: usize = 128;
const OUTGOING_CAPACITY: usize = 128;

/// Serve one code-mode connection over the process standard streams.
pub async fn run_stdio() -> Result<(), RuntimeString> {
    serve(tokio::io::stdin(), tokio::io::stdout()).await
}

/// Serve one code-mode connection over an ordered input/output pair.
pub async fn serve<R, W>(reader: R, writer: W) -> Result<(), RuntimeString>
where
    R: AsyncRead + Send + Unpin + 'static,
    W: AsyncWrite + Send + Unpin + 'static,
{
    let mut reader = FramedReader::new(reader);
    let mut writer = FramedWriter::new(writer);
    if !negotiate(&mut reader, &mut writer).await? {
        return Ok(());
    }

    let (outgoing_tx, mut outgoing_rx) = mpsc::channel::<EncodedFrame>(OUTGOING_CAPACITY);
    let peer = Arc::new(HostPeer::new(outgoing_tx));
    let state = Arc::new(HostState::new(Arc::clone(&peer)));

    let writer_disconnected = peer.disconnection_token();
    let writer_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = writer_disconnected.cancelled() => return Ok(()),
                frame = outgoing_rx.recv() => {
                    let Some(frame) = frame else {
                        return Ok(());
                    };
                    let result = tokio::select! {
                        _ = writer_disconnected.cancelled() => return Ok(()),
                        result = writer.write_frame(&frame) => result,
                    };
                    if let Err(error) = result {
                        return Err(RuntimeString::from(format!(
                            "failed to write code-mode host message: {error}"
                        )));
                    }
                }
            }
        }
    });
    let writer_peer = Arc::clone(&peer);
    let writer_supervisor = tokio::spawn(async move {
        match writer_task.await {
            Ok(Ok(())) if !writer_peer.is_disconnected() => {
                writer_peer.fail(RuntimeString::from(
                    "code-mode writer task exited unexpectedly",
                ));
            }
            Ok(Ok(())) => {}
            Ok(Err(error)) => writer_peer.fail(error),
            Err(error) => writer_peer.fail(RuntimeString::from(format!(
                "code-mode writer task failed: {error}"
            ))),
        }
    });

    let input_result = async {
        loop {
            let message = tokio::select! {
                _ = peer.disconnected() => break,
                message = reader.read() => message.map_err(|error| {
                    RuntimeString::from(format!("failed to read code-mode client message: {error}"))
                })?,
            };
            let Some(message) = message else {
                break;
            };
            match message {
                ClientToHost::ClientHello(_) => {
                    return Err(RuntimeString::from(
                        "received a second code-mode client hello",
                    ));
                }
                ClientToHost::Request { id, request } => state.spawn_request(id, request)?,
                ClientToHost::CancelRequest { id } => state.cancel_request(id),
                ClientToHost::DelegateResponse { id, result } => {
                    peer.complete(id, result.into_result()).await;
                }
            }
        }
        Ok(())
    }
    .await;

    peer.disconnect();
    if tokio::time::timeout(SHUTDOWN_TIMEOUT, state.disconnect())
        .await
        .is_err()
    {
        peer.fail(RuntimeString::from(
            "timed out shutting down code-mode host state",
        ));
    }
    drop(state);
    tokio::time::timeout(SHUTDOWN_TIMEOUT, writer_supervisor)
        .await
        .map_err(|_| RuntimeString::from("timed out supervising code-mode writer task"))?
        .map_err(|error| {
            RuntimeString::from(format!("code-mode writer supervisor task failed: {error}"))
        })?;
    let failure = peer.failure();
    drop(peer);
    input_result?;
    match failure {
        Some(failure) => Err(failure),
        None => Ok(()),
    }
}

/// Agree on a protocol version and capability set, or reject and close.
async fn negotiate<R, W>(
    reader: &mut FramedReader<R>,
    writer: &mut FramedWriter<W>,
) -> Result<bool, RuntimeString>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let first_message = reader.read().await.map_err(|error| {
        RuntimeString::from(format!("failed to read code-mode client hello: {error}"))
    })?;
    let Some(first_message) = first_message else {
        return Ok(false);
    };
    let ClientToHost::ClientHello(client_hello) = first_message else {
        reject(
            writer,
            HandshakeRejectReason::InvalidHello {
                message: RuntimeString::from("first message must be connection/hello"),
            },
        )
        .await?;
        return Ok(false);
    };

    let supported_versions = SupportedProtocolVersions::try_new([ProtocolVersion::V1])
        .map_err(|error| RuntimeString::from(error.to_string()))?;
    if !client_hello
        .supported_versions()
        .contains(ProtocolVersion::V1)
    {
        reject(
            writer,
            HandshakeRejectReason::NoCompatibleVersion { supported_versions },
        )
        .await?;
        return Ok(false);
    }

    let host_capabilities = CapabilitySet::empty();
    if let Some(capability) = client_hello
        .required_capabilities()
        .iter()
        .find(|capability| !host_capabilities.contains(capability))
    {
        reject(
            writer,
            HandshakeRejectReason::MissingRequiredCapability {
                capability: capability.clone(),
            },
        )
        .await?;
        return Ok(false);
    }

    writer
        .write(&HostToClient::HostHello(HostHello::new(
            ProtocolVersion::V1,
            host_capabilities,
        )))
        .await
        .map_err(|error| {
            RuntimeString::from(format!("failed to write code-mode host hello: {error}"))
        })?;
    Ok(true)
}

async fn reject<W>(
    writer: &mut FramedWriter<W>,
    reason: HandshakeRejectReason,
) -> Result<(), RuntimeString>
where
    W: AsyncWrite + Unpin,
{
    writer
        .write(&HostToClient::HandshakeRejected { reason })
        .await
        .map_err(|error| {
            RuntimeString::from(format!("failed to reject code-mode client hello: {error}"))
        })
}

// ---- host state --------------------------------------------------------------------

struct HostState {
    sessions: StdMutex<HashMap<SessionId, Arc<InProcessCodeModeSession>>>,
    /// Persistent REPL runtimes, one per session, created on first use.
    repl_sessions: StdMutex<HashMap<SessionId, Arc<HostReplSession>>>,
    seen_session_ids: StdMutex<SeenIds<SessionId>>,
    requests: StdMutex<RequestRegistry>,
    request_tasks: TaskTracker,
    request_permits: Arc<Semaphore>,
    active_cell_permits: Arc<Semaphore>,
    closing: AtomicBool,
    peer: Arc<HostPeer>,
}

impl HostState {
    fn new(peer: Arc<HostPeer>) -> Self {
        Self {
            sessions: StdMutex::new(HashMap::new()),
            repl_sessions: StdMutex::new(HashMap::new()),
            seen_session_ids: StdMutex::new(SeenIds::new(MAX_RECENT_SESSION_IDS)),
            requests: StdMutex::new(RequestRegistry::new()),
            request_tasks: TaskTracker::new(),
            request_permits: Arc::new(Semaphore::new(MAX_IN_FLIGHT_REQUESTS)),
            active_cell_permits: Arc::new(Semaphore::new(MAX_ACTIVE_CELLS)),
            closing: AtomicBool::new(false),
            peer,
        }
    }

    /// Admit one client operation. A duplicate id is a protocol violation and
    /// ends the connection; being over the concurrency bound is an ordinary
    /// failed answer.
    fn spawn_request(
        self: &Arc<Self>,
        request_id: RequestId,
        request: HostRequest,
    ) -> Result<(), RuntimeString> {
        let cancellation = self
            .requests
            .lock_recover()
            .start(request_id, RequestKind::from(&request))?;
        let Ok(permit) = Arc::clone(&self.request_permits).try_acquire_owned() else {
            self.respond(
                request_id,
                Err(RuntimeString::from(
                    "code-mode host has too many in-flight requests",
                )),
            );
            self.finish_request(request_id);
            return Ok(());
        };
        let state = Arc::clone(self);
        let request_task = self.request_tasks.spawn(async move {
            let _permit = permit;
            state
                .handle_request(request_id, request, cancellation)
                .await;
            state.finish_request(request_id);
        });
        let peer = Arc::clone(&self.peer);
        tokio::spawn(async move {
            if let Err(error) = request_task.await {
                peer.fail(RuntimeString::from(format!(
                    "code-mode request task failed: {error}"
                )));
            }
        });
        Ok(())
    }

    async fn handle_request(
        &self,
        request_id: RequestId,
        request: HostRequest,
        cancellation: CancellationToken,
    ) {
        if self.closing.load(Ordering::Acquire) {
            self.respond(
                request_id,
                Err(RuntimeString::from("code-mode host is shutting down")),
            );
            return;
        }
        match request {
            HostRequest::OpenSession { session_id } => {
                let result = self
                    .open_session(session_id.clone())
                    .map(|()| HostResponse::SessionReady { session_id });
                self.respond(request_id, result);
            }
            HostRequest::Execute {
                session_id,
                request,
            } => {
                self.execute(request_id, session_id, request, cancellation)
                    .await
            }
            HostRequest::Wait {
                session_id,
                request,
            } => {
                let result = match self.session(&session_id) {
                    Ok(session) => {
                        let request = match request.try_into() {
                            Ok(request) => request,
                            Err(error) => {
                                self.respond(
                                    request_id,
                                    Err(RuntimeString::from(format!(
                                        "invalid code-mode wait request: {error}"
                                    ))),
                                );
                                return;
                            }
                        };
                        tokio::select! {
                            biased;
                            _ = cancellation.cancelled() => {
                                Err(RuntimeString::from("code-mode request cancelled"))
                            }
                            result = session.wait(request) => result.map(|outcome| {
                                HostResponse::WaitCompleted { outcome: outcome.into() }
                            }),
                        }
                    }
                    Err(error) => Err(error),
                };
                self.respond(request_id, result);
            }
            HostRequest::Terminate {
                session_id,
                cell_id,
            } => {
                let result = match self.session(&session_id) {
                    Ok(session) => session.terminate(cell_id.into()).await.map(|outcome| {
                        HostResponse::WaitCompleted {
                            outcome: outcome.into(),
                        }
                    }),
                    Err(error) => Err(error),
                };
                self.respond(request_id, result);
            }
            HostRequest::ShutdownSession { session_id } => {
                // Interrupt REPL evaluations first so their routes drain and
                // the session-cell wait below covers them.
                if let Some(repl) = self.repl_sessions.lock_recover().remove(&session_id) {
                    repl.cancel_all();
                }
                let session = self.sessions.lock_recover().remove(&session_id);
                let result = match session {
                    Some(session) => match session.shutdown().await {
                        // Only report the session closed once its cells have
                        // finished reporting, so the client's teardown ordering
                        // matches the host's.
                        Ok(()) => {
                            self.peer.wait_for_session_cells(&session_id).await;
                            Ok(HostResponse::SessionClosed { session_id })
                        }
                        Err(error) => Err(error),
                    },
                    None => Err(unknown_session(&session_id)),
                };
                self.respond(request_id, result);
            }
            HostRequest::ReplEval {
                session_id,
                request,
            } => {
                self.repl_eval(request_id, session_id, request, cancellation)
                    .await
            }
            HostRequest::ReplTerminate {
                session_id,
                exec_id,
            } => {
                let result = match self.repl_sessions.lock_recover().get(&session_id) {
                    Some(repl) => {
                        repl.terminate(&exec_id);
                        Ok(HostResponse::ReplTerminated { exec_id })
                    }
                    None => Err(unknown_session(&session_id)),
                };
                self.respond(request_id, result);
            }
        }
    }

    async fn execute(
        &self,
        request_id: RequestId,
        session_id: SessionId,
        request: crate::engine::code_mode::host::WireExecuteRequest,
        cancellation: CancellationToken,
    ) {
        if cancellation.is_cancelled() {
            self.respond(
                request_id,
                Err(RuntimeString::from("code-mode request cancelled")),
            );
            return;
        }
        let request = match request.try_into() {
            Ok(request) => request,
            Err(error) => {
                self.respond(
                    request_id,
                    Err(RuntimeString::from(format!(
                        "invalid code-mode execute request: {error}"
                    ))),
                );
                return;
            }
        };
        let session = match self.session(&session_id) {
            Ok(session) => session,
            Err(error) => {
                self.respond(request_id, Err(error));
                return;
            }
        };
        let Ok(active_cell_permit) = Arc::clone(&self.active_cell_permits).try_acquire_owned()
        else {
            self.respond(
                request_id,
                Err(RuntimeString::from(
                    "code-mode host has too many active cells",
                )),
            );
            return;
        };
        match session.execute(request).await {
            Ok(started) => {
                self.respond(
                    request_id,
                    Ok(HostResponse::ExecutionStarted {
                        cell_id: started.cell_id.clone().into(),
                    }),
                );
                // The cell now owns its own forwarding task; this request stays
                // alive until the first frontier has been sent so the client's
                // two answers keep their order.
                let initial_response_sent =
                    self.peer
                        .start_cell(session_id, request_id, started, active_cell_permit);
                let _ = initial_response_sent.await;
            }
            Err(error) => self.respond(request_id, Err(error)),
        }
    }

    /// Run one REPL evaluation inside the session's persistent runtime.
    ///
    /// The contract mirrors the cell flow's teardown ordering: once the
    /// evaluation is admitted (its delegate route registered), the request is
    /// ALWAYS answered with `ReplCompleted` — an infrastructure failure of the
    /// runtime itself travels inside the outcome's error — and the route is
    /// closed afterwards, so the client sees `CellClosed` and reclaims its
    /// registry entry. Pre-admission rejections answer with an error and
    /// never emit `CellClosed`.
    async fn repl_eval(
        &self,
        request_id: RequestId,
        session_id: SessionId,
        request: WireReplEvalRequest,
        cancellation: CancellationToken,
    ) {
        // The session envelope must be open: REPL evaluations share the
        // session's delegate routing and shutdown ordering.
        if let Err(error) = self.session(&session_id) {
            self.respond(request_id, Err(error));
            return;
        }
        let repl = {
            let mut sessions = self.repl_sessions.lock_recover();
            Arc::clone(
                sessions
                    .entry(session_id.clone())
                    .or_insert_with(|| Arc::new(HostReplSession::new())),
            )
        };
        let exec_id = request.exec_id.clone();
        let exec_cell = CellId::new(exec_id.clone());
        // A terminate request or a client-side request cancellation both
        // interrupt through this token; the runtime's own script-time budget
        // and wall-clock limits apply unchanged inside the evaluation.
        let eval_token = cancellation.child_token();
        if !repl.register(&exec_id, eval_token.clone()) {
            self.respond(
                request_id,
                Err(RuntimeString::from(
                    "code-mode host already runs a REPL evaluation with exec id ",
                )
                .concat(&exec_id)),
            );
            return;
        }
        if !self
            .peer
            .start_repl_route(session_id.clone(), exec_cell.clone())
        {
            repl.unregister(&exec_id);
            self.respond(
                request_id,
                Err(RuntimeString::from(
                    "code-mode host could not open the REPL delegate route",
                )),
            );
            return;
        }
        let invoke = build_repl_invoke(
            Arc::clone(&self.peer),
            session_id.clone(),
            exec_cell.clone(),
            Arc::clone(&repl),
        );
        let exec_request = ReplExecRequest {
            code: request.code,
            timeout_ms: request.timeout_ms,
            tools: request
                .tools
                .into_iter()
                .map(|tool| ReplToolBinding {
                    key: tool.name,
                    name: tool.tool_name,
                })
                .collect(),
        };
        let outcome = repl
            .session
            .execute_with(exec_request, Some(invoke), &eval_token)
            .await;
        repl.unregister(&exec_id);
        let response = match outcome {
            Ok(outcome) => Ok(HostResponse::ReplCompleted {
                outcome: WireReplOutcome {
                    result: outcome.result,
                    result_defined: outcome.result_defined,
                    result_text: outcome.result_text,
                    stdout: outcome.stdout,
                    stderr: outcome.stderr,
                    error: outcome.error,
                },
            }),
            // The runtime itself failed (its thread died or could not start).
            // Fold it into the outcome so the admitted-evaluation contract
            // holds: one ReplCompleted answer, then CellClosed.
            Err(message) => Ok(HostResponse::ReplCompleted {
                outcome: WireReplOutcome {
                    result: RuntimeJson::Null,
                    result_defined: false,
                    result_text: RuntimeString::default(),
                    stdout: RuntimeString::default(),
                    stderr: RuntimeString::default(),
                    error: Some(RuntimeString::from(message)),
                },
            }),
        };
        self.respond(request_id, response);
        self.peer.close_cell(session_id, exec_cell);
    }

    fn open_session(&self, session_id: SessionId) -> Result<(), RuntimeString> {
        let mut sessions = self.sessions.lock_recover();
        if sessions.contains_key(&session_id) {
            return Err(RuntimeString::from(format!(
                "code-mode session {session_id} is already open"
            )));
        }
        if self.closing.load(Ordering::Acquire) {
            return Err(RuntimeString::from("code-mode host is shutting down"));
        }
        // A reused id would let a new session inherit a retired one's identity.
        if !self.seen_session_ids.lock_recover().remember(&session_id) {
            return Err(RuntimeString::from(format!(
                "code-mode session {session_id} was reused"
            )));
        }
        let delegate = Arc::new(RemoteDelegate {
            session_id: session_id.clone(),
            peer: Arc::clone(&self.peer),
        });
        let peer = Arc::downgrade(&self.peer);
        let task_failure_handler = Arc::new(move |reason: String| {
            if let Some(peer) = peer.upgrade() {
                peer.fail(RuntimeString::from(reason));
            }
        });
        sessions.insert(
            session_id,
            Arc::new(
                InProcessCodeModeSession::with_delegate_and_task_failure_handler(
                    delegate,
                    task_failure_handler,
                ),
            ),
        );
        Ok(())
    }

    fn session(
        &self,
        session_id: &SessionId,
    ) -> Result<Arc<InProcessCodeModeSession>, RuntimeString> {
        self.sessions
            .lock_recover()
            .get(session_id)
            .cloned()
            .ok_or_else(|| unknown_session(session_id))
    }

    fn respond(&self, id: RequestId, result: Result<HostResponse, RuntimeString>) {
        self.peer.respond(id, result);
    }

    fn cancel_request(&self, request_id: RequestId) {
        self.requests.lock_recover().cancel(request_id);
    }

    fn finish_request(&self, request_id: RequestId) {
        self.requests.lock_recover().finish(request_id);
    }

    /// Stop accepting work, cancel what is in flight, then shut every session
    /// down so no cell outlives the connection that created it.
    async fn disconnect(&self) {
        self.closing.store(true, Ordering::Release);
        self.requests.lock_recover().cancel_all();
        self.request_tasks.close();
        self.request_tasks.wait().await;
        let sessions = self
            .sessions
            .lock_recover()
            .drain()
            .map(|(_, session)| session)
            .collect::<Vec<_>>();
        for session in sessions {
            let _ = session.shutdown().await;
        }
    }
}

fn unknown_session(session_id: &SessionId) -> RuntimeString {
    RuntimeString::from(format!("unknown code-mode session {session_id}"))
}

#[derive(Clone, Copy)]
enum RequestKind {
    OpenSession,
    Execute,
    Wait,
    Terminate,
    ShutdownSession,
    ReplEval,
    ReplTerminate,
}

impl RequestKind {
    fn from(request: &HostRequest) -> Self {
        match request {
            HostRequest::OpenSession { .. } => Self::OpenSession,
            HostRequest::Execute { .. } => Self::Execute,
            HostRequest::Wait { .. } => Self::Wait,
            HostRequest::Terminate { .. } => Self::Terminate,
            HostRequest::ShutdownSession { .. } => Self::ShutdownSession,
            HostRequest::ReplEval { .. } => Self::ReplEval,
            HostRequest::ReplTerminate { .. } => Self::ReplTerminate,
        }
    }

    /// Only the observing operations may be cancelled; teardown must finish so
    /// both sides agree on what still exists.
    fn is_cancellable(self) -> bool {
        matches!(self, Self::Execute | Self::Wait | Self::ReplEval)
    }
}

struct ActiveRequest {
    kind: RequestKind,
    cancellation: CancellationToken,
}

struct RequestRegistry {
    active: HashMap<RequestId, ActiveRequest>,
    recent: SeenIds<RequestId>,
}

impl RequestRegistry {
    fn new() -> Self {
        Self {
            active: HashMap::new(),
            recent: SeenIds::new(MAX_RECENT_REQUEST_IDS),
        }
    }

    fn start(
        &mut self,
        request_id: RequestId,
        kind: RequestKind,
    ) -> Result<CancellationToken, RuntimeString> {
        if self.active.contains_key(&request_id) || self.recent.contains(&request_id) {
            return Err(RuntimeString::from(format!(
                "duplicate code-mode request id {request_id}"
            )));
        }
        let cancellation = CancellationToken::new();
        self.active.insert(
            request_id,
            ActiveRequest {
                kind,
                cancellation: cancellation.clone(),
            },
        );
        Ok(cancellation)
    }

    fn cancel(&self, request_id: RequestId) {
        if let Some(request) = self.active.get(&request_id) {
            if request.kind.is_cancellable() {
                request.cancellation.cancel();
            }
        }
    }

    fn finish(&mut self, request_id: RequestId) {
        if self.active.remove(&request_id).is_none() {
            return;
        }
        self.recent.remember(&request_id);
    }

    fn cancel_all(&self) {
        for request in self.active.values() {
            request.cancellation.cancel();
        }
    }
}

/// A bounded set of retired ids: enough history to catch a replay without
/// growing for the life of the connection.
struct SeenIds<T> {
    ids: HashSet<T>,
    order: VecDeque<T>,
    capacity: usize,
}

impl<T: Clone + Eq + std::hash::Hash> SeenIds<T> {
    fn new(capacity: usize) -> Self {
        Self {
            ids: HashSet::new(),
            order: VecDeque::new(),
            capacity,
        }
    }

    fn contains(&self, id: &T) -> bool {
        self.ids.contains(id)
    }

    /// Record an id, reporting `false` when it was already known.
    fn remember(&mut self, id: &T) -> bool {
        if !self.ids.insert(id.clone()) {
            return false;
        }
        self.order.push_back(id.clone());
        while self.order.len() > self.capacity {
            if let Some(expired) = self.order.pop_front() {
                self.ids.remove(&expired);
            }
        }
        true
    }
}

// ---- the client-facing peer --------------------------------------------------------

/// The host's view of its client: outbound frames, delegate correlation, and
/// per-cell message routing.
struct HostPeer {
    outgoing_tx: mpsc::Sender<EncodedFrame>,
    pending: Mutex<HashMap<DelegateRequestId, PendingDelegate>>,
    delegate_permits: Arc<Semaphore>,
    cell_routes: StdMutex<HashMap<(SessionId, CellId), CellRoute>>,
    cell_routes_changed: Notify,
    next_request_id: AtomicI64,
    disconnected: CancellationToken,
    failure: StdMutex<Option<RuntimeString>>,
}

struct PendingDelegate {
    response_tx: oneshot::Sender<Result<DelegateResponse, RuntimeString>>,
    dispatched: bool,
    _permit: OwnedSemaphorePermit,
}

/// Messages for one cell. A cell's route exists before its forwarding task, so
/// a delegate call raised during admission is queued instead of lost.
enum CellRoute {
    Pending(VecDeque<CellMessage>),
    Active(mpsc::Sender<CellMessage>),
}

enum CellMessage {
    Delegate {
        id: DelegateRequestId,
        request: DelegateRequest,
        dispatched_tx: oneshot::Sender<Result<(), RuntimeString>>,
    },
    Closed,
}

enum PeerSendError {
    /// The message itself cannot be represented; the operation may still be
    /// answered with the failure.
    Payload(RuntimeString),
    /// The connection can no longer carry frames.
    Unavailable(RuntimeString),
}

impl std::fmt::Display for PeerSendError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Payload(message) | Self::Unavailable(message) => {
                write!(formatter, "{message:?}")
            }
        }
    }
}

impl HostPeer {
    fn new(outgoing_tx: mpsc::Sender<EncodedFrame>) -> Self {
        Self {
            outgoing_tx,
            pending: Mutex::new(HashMap::new()),
            delegate_permits: Arc::new(Semaphore::new(MAX_PENDING_DELEGATE_CALLS)),
            cell_routes: StdMutex::new(HashMap::new()),
            cell_routes_changed: Notify::new(),
            next_request_id: AtomicI64::new(1),
            disconnected: CancellationToken::new(),
            failure: StdMutex::new(None),
        }
    }

    fn send(&self, message: HostToClient) -> Result<(), PeerSendError> {
        let frame = EncodedFrame::encode(&message)
            .map_err(|error| PeerSendError::Payload(RuntimeString::from(error.to_string())))?;
        match self.outgoing_tx.try_send(frame) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.disconnect();
                Err(PeerSendError::Unavailable(RuntimeString::from(
                    "code-mode host outgoing queue is full",
                )))
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.disconnect();
                Err(PeerSendError::Unavailable(RuntimeString::from(
                    "code-mode client connection closed",
                )))
            }
        }
    }

    /// Answer one operation. A response too large to send is replaced by the
    /// failure describing why, so the client is never left waiting.
    fn respond(&self, id: RequestId, result: Result<HostResponse, RuntimeString>) {
        if let Err(PeerSendError::Payload(error)) = self.send(HostToClient::Response {
            id,
            result: WireResult::from_result(result),
        }) {
            let _ = self.send(HostToClient::Response {
                id,
                result: WireResult::Err {
                    message: RuntimeString::from(format!(
                        "code-mode host response exceeds the frame limit: {error:?}"
                    )),
                },
            });
        }
    }

    fn initial_response(&self, id: RequestId, result: Result<WireRuntimeResponse, RuntimeString>) {
        if let Err(PeerSendError::Payload(error)) = self.send(HostToClient::InitialResponse {
            id,
            result: WireResult::from_result(result),
        }) {
            let _ = self.send(HostToClient::InitialResponse {
                id,
                result: WireResult::Err {
                    message: RuntimeString::from(format!(
                        "code-mode initial response exceeds the frame limit: {error:?}"
                    )),
                },
            });
        }
    }

    /// Ask the client's delegate to do something for a running cell, and wait
    /// for its answer.
    async fn call(
        self: &Arc<Self>,
        session_id: SessionId,
        request: DelegateRequest,
        cancellation_token: CancellationToken,
    ) -> Result<DelegateResponse, RuntimeString> {
        if self.disconnected.is_cancelled() {
            return Err(RuntimeString::from("code-mode client connection closed"));
        }
        let Ok(permit) = Arc::clone(&self.delegate_permits).try_acquire_owned() else {
            return Err(RuntimeString::from(
                "code-mode host has too many pending delegate calls",
            ));
        };
        let id = DelegateRequestId::new(self.next_request_id.fetch_add(1, Ordering::Relaxed));
        let (response_tx, response_rx) = oneshot::channel();
        self.pending.lock().await.insert(
            id,
            PendingDelegate {
                response_tx,
                dispatched: false,
                _permit: permit,
            },
        );
        let mut pending = PendingDelegateRequest::new(Arc::clone(self), id);
        let cell_id: CellId = match &request {
            DelegateRequest::InvokeTool { invocation } => invocation.cell_id.clone().into(),
            DelegateRequest::Notify { cell_id, .. } => cell_id.clone().into(),
        };
        let (dispatched_tx, dispatched_rx) = oneshot::channel();
        if let Err(error) = self.route_cell_message(
            (session_id, cell_id),
            CellMessage::Delegate {
                id,
                request,
                dispatched_tx,
            },
        ) {
            self.pending.lock().await.remove(&id);
            pending.disarm();
            return Err(error);
        }

        let dispatched = tokio::select! {
            dispatched = dispatched_rx => dispatched.map_err(|_| {
                RuntimeString::from("code-mode cell route closed before dispatching delegate request")
            })?,
            _ = self.disconnected.cancelled() => {
                self.pending.lock().await.remove(&id);
                pending.disarm();
                return Err(RuntimeString::from("code-mode client connection closed"));
            }
        };
        if let Err(error) = dispatched {
            self.pending.lock().await.remove(&id);
            pending.disarm();
            return Err(error);
        }

        tokio::select! {
            response = response_rx => {
                pending.disarm();
                response.map_err(|_| {
                    RuntimeString::from("code-mode client closed before returning delegate output")
                })?
            }
            _ = cancellation_token.cancelled() => {
                if self.remove_pending(id).await.is_some() {
                    let _ = self.send(HostToClient::CancelDelegateRequest { id });
                }
                pending.disarm();
                Err(RuntimeString::from("code mode delegate request cancelled"))
            }
            _ = self.disconnected.cancelled() => {
                self.pending.lock().await.remove(&id);
                pending.disarm();
                Err(RuntimeString::from("code-mode client connection closed"))
            }
        }
    }

    async fn complete(
        &self,
        id: DelegateRequestId,
        response: Result<DelegateResponse, RuntimeString>,
    ) {
        if let Some(pending) = self.remove_pending(id).await {
            let _ = pending.response_tx.send(response);
        }
    }

    /// Begin forwarding for one admitted cell. Returns a receiver that fires
    /// once the cell's first frontier has been sent.
    fn start_cell(
        self: &Arc<Self>,
        session_id: SessionId,
        request_id: RequestId,
        started: StartedCell,
        active_cell_permit: OwnedSemaphorePermit,
    ) -> oneshot::Receiver<()> {
        let (initial_response_sent_tx, initial_response_sent_rx) = oneshot::channel();
        let key = (session_id, started.cell_id.clone());
        let (messages_tx, messages_rx) = mpsc::channel(CELL_MESSAGE_CAPACITY);
        let previous = self
            .cell_routes
            .lock_recover()
            .insert(key.clone(), CellRoute::Active(messages_tx.clone()));
        match previous {
            Some(CellRoute::Pending(messages)) => {
                for message in messages {
                    if messages_tx.try_send(message).is_err() {
                        self.disconnect();
                        return initial_response_sent_rx;
                    }
                }
            }
            // Two live routes for one cell would deliver a delegate call twice.
            Some(CellRoute::Active(_)) => {
                self.disconnect();
                return initial_response_sent_rx;
            }
            None => {}
        }
        let peer = Arc::clone(self);
        self.spawn_critical("cell forwarding", async move {
            drive_cell(
                peer,
                key,
                request_id,
                started,
                messages_rx,
                initial_response_sent_tx,
                active_cell_permit,
            )
            .await;
        });
        initial_response_sent_rx
    }

    fn close_cell(&self, session_id: SessionId, cell_id: CellId) {
        let _ = self.route_cell_message((session_id, cell_id), CellMessage::Closed);
    }

    /// Open the delegate route for one REPL evaluation. The route relays the
    /// evaluation's nested tool calls until it is closed with
    /// [`Self::close_cell`]. Returns false when a route for the same key is
    /// already active, which the caller treats as a failed admission.
    fn start_repl_route(self: &Arc<Self>, session_id: SessionId, cell_id: CellId) -> bool {
        let key = (session_id, cell_id);
        let (messages_tx, mut messages_rx) = mpsc::channel(CELL_MESSAGE_CAPACITY);
        let previous = self
            .cell_routes
            .lock_recover()
            .insert(key.clone(), CellRoute::Active(messages_tx.clone()));
        match previous {
            Some(CellRoute::Pending(messages)) => {
                for message in messages {
                    if messages_tx.try_send(message).is_err() {
                        self.disconnect();
                        return false;
                    }
                }
            }
            // Two live routes for one key would deliver a delegate call twice.
            Some(CellRoute::Active(_)) => {
                self.disconnect();
                return false;
            }
            None => {}
        }
        let peer = Arc::clone(self);
        self.spawn_critical("repl route", async move {
            relay_route_until_closed(&peer, &key, &mut messages_rx).await;
        });
        true
    }

    fn disconnect(&self) {
        self.disconnected.cancel();
    }

    fn fail(&self, reason: RuntimeString) {
        {
            let mut failure = self.failure.lock_recover();
            if failure.is_none() {
                *failure = Some(reason);
            }
        }
        self.disconnect();
    }

    fn failure(&self) -> Option<RuntimeString> {
        self.failure.lock_recover().clone()
    }

    fn is_disconnected(&self) -> bool {
        self.disconnected.is_cancelled()
    }

    async fn disconnected(&self) {
        self.disconnected.cancelled().await;
    }

    fn disconnection_token(&self) -> CancellationToken {
        self.disconnected.clone()
    }

    /// Wait until no cell of this session has a route left.
    async fn wait_for_session_cells(&self, session_id: &SessionId) {
        loop {
            let changed = self.cell_routes_changed.notified();
            if !self
                .cell_routes
                .lock_recover()
                .keys()
                .any(|(route_session_id, _)| route_session_id == session_id)
            {
                return;
            }
            tokio::select! {
                _ = changed => {}
                _ = self.disconnected.cancelled() => return,
            }
        }
    }

    /// Send a delegate request only if it is still pending, so a call cancelled
    /// during queueing is never dispatched.
    async fn send_delegate_if_pending(
        &self,
        id: DelegateRequestId,
        session_id: SessionId,
        request: DelegateRequest,
        dispatched_tx: oneshot::Sender<Result<(), RuntimeString>>,
    ) {
        let result = {
            let mut pending = self.pending.lock().await;
            let Some(pending) = pending.get_mut(&id) else {
                let _ = dispatched_tx.send(Err(RuntimeString::from(
                    "code-mode delegate request was cancelled before dispatch",
                )));
                return;
            };
            match self.send(HostToClient::DelegateRequest {
                id,
                session_id,
                request,
            }) {
                Ok(()) => {
                    pending.dispatched = true;
                    Ok(())
                }
                Err(error) => Err(RuntimeString::from(error.to_string())),
            }
        };
        let _ = dispatched_tx.send(result);
    }

    fn route_cell_message(
        &self,
        key: (SessionId, CellId),
        message: CellMessage,
    ) -> Result<(), RuntimeString> {
        use std::collections::hash_map::Entry;

        let result = match self.cell_routes.lock_recover().entry(key) {
            Entry::Occupied(mut entry) => match entry.get_mut() {
                CellRoute::Pending(messages) if messages.len() < CELL_MESSAGE_CAPACITY => {
                    messages.push_back(message);
                    Ok(())
                }
                CellRoute::Pending(_) => {
                    Err(RuntimeString::from("code-mode cell message queue is full"))
                }
                CellRoute::Active(sender) => sender.try_send(message).map_err(|_| {
                    RuntimeString::from("code-mode cell message queue is unavailable")
                }),
            },
            Entry::Vacant(entry) => {
                entry.insert(CellRoute::Pending(VecDeque::from([message])));
                Ok(())
            }
        };
        if result.is_err() {
            self.disconnect();
        }
        result
    }

    async fn remove_pending(&self, id: DelegateRequestId) -> Option<PendingDelegate> {
        self.pending.lock().await.remove(&id)
    }

    fn spawn_critical<F>(self: &Arc<Self>, task_name: &'static str, future: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let task = tokio::spawn(future);
        let peer = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(error) = task.await {
                peer.fail(RuntimeString::from(format!(
                    "code-mode {task_name} task failed: {error}"
                )));
            }
        });
    }

    fn remove_cell_route(&self, key: &(SessionId, CellId)) {
        if self.cell_routes.lock_recover().remove(key).is_some() {
            self.cell_routes_changed.notify_waiters();
        }
    }
}

/// One cell's forwarding loop: deliver the first frontier, then keep relaying
/// delegate calls until the cell closes.
async fn drive_cell(
    peer: Arc<HostPeer>,
    key: (SessionId, CellId),
    request_id: RequestId,
    started: StartedCell,
    mut messages_rx: mpsc::Receiver<CellMessage>,
    initial_response_sent_tx: oneshot::Sender<()>,
    _active_cell_permit: OwnedSemaphorePermit,
) {
    let mut initial_response_sent_tx = Some(initial_response_sent_tx);
    let initial_response = started.initial_response();
    tokio::pin!(initial_response);
    // Phase one: the frontier is owed, but delegate calls may already arrive.
    let closed = loop {
        tokio::select! {
            biased;
            result = &mut initial_response => {
                peer.initial_response(request_id, result.map(Into::into));
                if let Some(sent) = initial_response_sent_tx.take() {
                    let _ = sent.send(());
                }
                break false;
            }
            message = messages_rx.recv() => match message {
                Some(CellMessage::Delegate { id, request, dispatched_tx }) => {
                    peer.send_delegate_if_pending(id, key.0.clone(), request, dispatched_tx).await;
                }
                Some(CellMessage::Closed) | None => break true,
            },
            _ = peer.disconnected.cancelled() => {
                peer.remove_cell_route(&key);
                return;
            }
        }
    };

    if closed {
        // The cell closed before its frontier resolved; the client is still
        // owed exactly one initial response.
        peer.initial_response(request_id, initial_response.await.map(Into::into));
        if let Some(sent) = initial_response_sent_tx.take() {
            let _ = sent.send(());
        }
        let _ = peer.send(HostToClient::CellClosed {
            session_id: key.0.clone(),
            cell_id: (&key.1).into(),
        });
        peer.remove_cell_route(&key);
    } else {
        // Phase two: the frontier is delivered; relay until the cell closes.
        relay_route_until_closed(&peer, &key, &mut messages_rx).await;
    }
}

/// Relay a route's delegate messages until the route closes, then report the
/// cell closed and remove the route. Shared by cell forwarding (phase two)
/// and REPL evaluation routes.
async fn relay_route_until_closed(
    peer: &Arc<HostPeer>,
    key: &(SessionId, CellId),
    messages_rx: &mut mpsc::Receiver<CellMessage>,
) {
    loop {
        tokio::select! {
            message = messages_rx.recv() => match message {
                Some(CellMessage::Delegate { id, request, dispatched_tx }) => {
                    peer.send_delegate_if_pending(id, key.0.clone(), request, dispatched_tx).await;
                }
                Some(CellMessage::Closed) | None => break,
            },
            _ = peer.disconnected.cancelled() => {
                peer.remove_cell_route(key);
                return;
            }
        }
    }
    let _ = peer.send(HostToClient::CellClosed {
        session_id: key.0.clone(),
        cell_id: (&key.1).into(),
    });
    peer.remove_cell_route(key);
}

/// Cancels its delegate request if the caller's future is dropped before the
/// answer arrives, but only once the request was actually dispatched.
struct PendingDelegateRequest {
    peer: Arc<HostPeer>,
    id: Option<DelegateRequestId>,
}

impl PendingDelegateRequest {
    fn new(peer: Arc<HostPeer>, id: DelegateRequestId) -> Self {
        Self { peer, id: Some(id) }
    }

    fn disarm(&mut self) {
        self.id = None;
    }
}

impl Drop for PendingDelegateRequest {
    fn drop(&mut self) {
        let Some(id) = self.id.take() else {
            return;
        };
        let peer = Arc::clone(&self.peer);
        tokio::spawn(async move {
            if let Some(pending) = peer.remove_pending(id).await {
                if pending.dispatched {
                    let _ = peer.send(HostToClient::CancelDelegateRequest { id });
                }
            }
        });
    }
}

// ---- the host-side delegate --------------------------------------------------------

/// The delegate a host session runs against: every callback becomes a request
/// to the CLIENT, so nested tool calls execute in the client's engine.
struct RemoteDelegate {
    session_id: SessionId,
    peer: Arc<HostPeer>,
}

impl CodeModeSessionDelegate for RemoteDelegate {
    fn invoke_tool<'a>(
        &'a self,
        invocation: CodeModeNestedToolCall,
        cancellation_token: CancellationToken,
    ) -> ToolInvocationFuture<'a> {
        Box::pin(async move {
            let response = self
                .peer
                .call(
                    self.session_id.clone(),
                    DelegateRequest::InvokeTool {
                        invocation: invocation.into(),
                    },
                    cancellation_token,
                )
                .await
                .map_err(CodeModeToolRejection::plain)?;
            match response {
                DelegateResponse::ToolResult { result } => Ok(result),
                DelegateResponse::NotificationDelivered => Err(CodeModeToolRejection::plain(
                    "code-mode client returned an invalid tool result",
                )),
            }
        })
    }

    fn notify<'a>(
        &'a self,
        call_id: RuntimeString,
        cell_id: CellId,
        text: RuntimeString,
        cancellation_token: CancellationToken,
    ) -> NotificationFuture<'a> {
        Box::pin(async move {
            match self
                .peer
                .call(
                    self.session_id.clone(),
                    DelegateRequest::Notify {
                        call_id,
                        cell_id: cell_id.into(),
                        text,
                    },
                    cancellation_token,
                )
                .await?
            {
                DelegateResponse::NotificationDelivered => Ok(()),
                DelegateResponse::ToolResult { .. } => Err(RuntimeString::from(
                    "code-mode client returned an invalid notification result",
                )),
            }
        })
    }

    fn cell_closed(&self, cell_id: &CellId) {
        self.peer
            .close_cell(self.session_id.clone(), cell_id.clone());
    }
}

// ---- host-side REPL sessions -------------------------------------------------------

/// The REPL runtime one session owns on the host, plus the interruption
/// tokens of its running evaluations. Evaluations serialize inside the
/// runtime itself; the token registry gives `repl/terminate` a handle on the
/// evaluation the client named.
struct HostReplSession {
    session: ReplSession,
    active: StdMutex<HashMap<RuntimeString, CancellationToken>>,
    next_rpc_id: AtomicU64,
}

impl HostReplSession {
    fn new() -> Self {
        Self {
            session: ReplSession::default(),
            active: StdMutex::new(HashMap::new()),
            next_rpc_id: AtomicU64::new(1),
        }
    }

    /// Register an evaluation's interrupt token; false when the client reused
    /// a live exec id.
    fn register(&self, exec_id: &RuntimeString, token: CancellationToken) -> bool {
        self.active
            .lock_recover()
            .insert(exec_id.clone(), token)
            .is_none()
    }

    fn unregister(&self, exec_id: &RuntimeString) {
        self.active.lock_recover().remove(exec_id);
    }

    fn terminate(&self, exec_id: &RuntimeString) {
        if let Some(token) = self.active.lock_recover().get(exec_id) {
            token.cancel();
        }
    }

    fn cancel_all(&self) {
        for token in self.active.lock_recover().values() {
            token.cancel();
        }
    }
}

/// The nested-tool bridge for one host-side REPL evaluation: every call
/// becomes a delegate request keyed by the evaluation's exec id, so the
/// client's engine runs the tool through its full pipeline. The delegate
/// call is bounded by the evaluation's own lifecycle — when the evaluation
/// settles, the runtime drops the in-flight bridge futures and their
/// pending-request guards cancel the calls across the wire.
fn build_repl_invoke(
    peer: Arc<HostPeer>,
    session_id: SessionId,
    exec_cell: CellId,
    repl: Arc<HostReplSession>,
) -> ReplInvokeFn {
    Arc::new(move |name: RuntimeString, input: RuntimeJson| {
        let peer = Arc::clone(&peer);
        let session_id = session_id.clone();
        let exec_cell = exec_cell.clone();
        let rpc_id = repl.next_rpc_id.fetch_add(1, Ordering::Relaxed);
        Box::pin(async move {
            let invocation = CodeModeNestedToolCall {
                cell_id: exec_cell,
                runtime_tool_call_id: RuntimeString::from(format!("repl-rpc-{rpc_id}")),
                tool_name: name,
                tool_kind: CodeModeToolKind::Function,
                input: Some(input),
            };
            let response = peer
                .call(
                    session_id,
                    DelegateRequest::InvokeTool {
                        invocation: invocation.into(),
                    },
                    CancellationToken::new(),
                )
                .await
                .map_err(CodeModeToolRejection::plain)?;
            match response {
                DelegateResponse::ToolResult { result } => Ok(result),
                DelegateResponse::NotificationDelivered => Err(CodeModeToolRejection::plain(
                    "code-mode client returned an invalid tool result",
                )),
            }
        })
    })
}
