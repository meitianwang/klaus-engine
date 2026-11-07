//! The connection driver: the single task that owns all correlation state for
//! one host connection.
//!
//! Everything that could race lives here and is touched only by this task:
//! request ids, live sessions and cells, and in-flight delegate calls. Callers
//! interact through commands and receive answers on oneshot channels, so no
//! lock is held across an await.
//!
//! The driver treats a CONTRACT violation by the host (an unknown request id, a
//! response for the wrong cell, a duplicate delegate id) as fatal: continuing
//! would mean answering a caller with another caller's result. Ordinary
//! operation failures are just answers.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::engine::code_mode::host::{
    ClientToHost, DelegateRequest, DelegateRequestId, DelegateResponse, EncodedFrame, HostRequest,
    HostResponse, HostToClient, RequestId, SessionId, WireCellId, WireReplEvalRequest,
    WireReplOutcome, WireResult, WireRuntimeResponse, WireWaitRequest, MAX_PENDING_DELEGATE_CALLS,
};
use crate::engine::code_mode::protocol::{
    CellId, CodeModeNestedToolCall, ExecuteRequest, WaitOutcome, WaitRequest,
};
use crate::engine::code_mode::session::{CodeModeSessionDelegate, StartedCell};
use crate::runtime_values::RuntimeString;
use crate::sync_ext::MutexExt;

use super::driver_support::{
    notify_cell_closed, public_runtime_response, public_wait_outcome, remote_cell_id,
    remote_wait_request, runtime_response_cell_id, wait_outcome_cell_id, CancellableRequest,
    CellAdmissionError, CellOwner, DeferredWait, DelegateTarget, DeliveredExecute, DriverCommand,
    DriverEvent, FailedSession, InitialResponse, PendingRequest, RemoteSession, SessionCleanup,
    SessionRegistry, UnclaimedExecute,
};

/// How many delegate ids are remembered after completion, so a host that
/// replays an id is caught instead of silently starting a second call.
const MAX_RECENT_DELEGATE_REQUEST_IDS: usize = 4096;

pub(super) struct DriverLifecycle {
    pub(super) alive: Arc<AtomicBool>,
    pub(super) failure: Arc<StdMutex<Option<RuntimeString>>>,
    pub(super) cancellation: CancellationToken,
}

// ---- request tracking --------------------------------------------------------------

enum CancellationAction {
    Send(RequestId),
    /// The caller of a started-but-unclaimed execute vanished: cancel the
    /// request AND terminate the cell it already created.
    Terminate {
        request_id: RequestId,
        execute: UnclaimedExecute,
    },
}

struct RequestTracker {
    pending: HashMap<RequestId, PendingRequest>,
    unclaimed_executes: HashMap<RequestId, UnclaimedExecute>,
    initial_responses: HashMap<RequestId, InitialResponse>,
    deferred_waits: VecDeque<DeferredWait>,
    next_request_id: i64,
}

impl RequestTracker {
    fn new() -> Self {
        Self {
            pending: HashMap::new(),
            unclaimed_executes: HashMap::new(),
            initial_responses: HashMap::new(),
            deferred_waits: VecDeque::new(),
            next_request_id: 1,
        }
    }

    fn contains_pending_open(&self, session: &RemoteSession) -> bool {
        self.pending.values().any(|pending| {
            matches!(
                pending,
                PendingRequest::OpenSession {
                    session: pending_session,
                    ..
                } if pending_session.id == session.id
            )
        })
    }

    fn allocate_id(&mut self) -> Result<RequestId, RuntimeString> {
        let id = self.next_request_id;
        self.next_request_id = self
            .next_request_id
            .checked_add(1)
            .ok_or_else(|| RuntimeString::from("code-mode host request id space exhausted"))?;
        Ok(RequestId::new(id))
    }

    fn insert_pending(
        &mut self,
        id: RequestId,
        pending: PendingRequest,
        event_tx: &mpsc::Sender<DriverEvent>,
    ) {
        self.pending.insert(id, pending);
        if let Some(cancellation) = self
            .pending
            .get_mut(&id)
            .and_then(PendingRequest::cancellation_mut)
        {
            cancellation.spawn_watcher(id, event_tx.clone());
        }
    }

    fn remove_pending(&mut self, id: RequestId) -> Option<PendingRequest> {
        self.pending.remove(&id)
    }

    fn insert_initial_response(&mut self, id: RequestId, response: InitialResponse) {
        self.initial_responses.insert(id, response);
    }

    fn remove_initial_response(&mut self, id: RequestId) -> Option<InitialResponse> {
        self.initial_responses.remove(&id)
    }

    fn insert_unclaimed_execute(&mut self, id: RequestId, execute: UnclaimedExecute) {
        self.unclaimed_executes.insert(id, execute);
    }

    fn claim_execute(&mut self, id: RequestId) {
        self.unclaimed_executes.remove(&id);
    }

    fn collect_cancellations(&mut self) -> Vec<CancellationAction> {
        let mut actions = self
            .pending
            .iter_mut()
            .filter_map(|(id, pending)| {
                let cancellation = pending.cancellation_mut()?;
                (cancellation.is_cancelled() && cancellation.mark_reported())
                    .then_some(CancellationAction::Send(*id))
            })
            .collect::<Vec<_>>();
        let abandoned = self
            .unclaimed_executes
            .iter_mut()
            .filter_map(|(id, execute)| {
                (execute.cancellation.is_cancelled() && execute.cancellation.mark_reported())
                    .then_some(*id)
            })
            .collect::<Vec<_>>();
        for request_id in abandoned {
            if let Some(execute) = self.unclaimed_executes.remove(&request_id) {
                actions.push(CancellationAction::Terminate {
                    request_id,
                    execute,
                });
            }
        }
        actions
    }

    fn mark_cancelled(&mut self, id: RequestId) -> Option<CancellationAction> {
        if let Some(cancellation) = self
            .pending
            .get_mut(&id)
            .and_then(PendingRequest::cancellation_mut)
        {
            return cancellation
                .mark_reported()
                .then_some(CancellationAction::Send(id));
        }
        let execute = self.unclaimed_executes.get_mut(&id)?;
        if !execute.cancellation.mark_reported() {
            return None;
        }
        self.unclaimed_executes
            .remove(&id)
            .map(|execute| CancellationAction::Terminate {
                request_id: id,
                execute,
            })
    }

    fn has_cancelled_wait(&self, session: &RemoteSession, cell_id: &WireCellId) -> bool {
        self.pending.values().any(|pending| {
            matches!(
                pending,
                PendingRequest::Wait {
                    session: pending_session,
                    cell_id: pending_cell_id,
                    cancellation,
                    ..
                } if pending_session == session
                    && pending_cell_id == cell_id
                    && cancellation.is_cancelled()
            )
        })
    }

    fn push_deferred_wait(&mut self, wait: DeferredWait) {
        self.deferred_waits.push_back(wait);
    }

    fn take_deferred_waits(&mut self) -> VecDeque<DeferredWait> {
        std::mem::take(&mut self.deferred_waits)
    }

    fn remove_unclaimed_for_session(&mut self, session_id: &SessionId) {
        self.unclaimed_executes
            .retain(|_, execute| &execute.session.id != session_id);
    }

    fn fail_all(&mut self, reason: &RuntimeString) {
        for (_, pending) in self.pending.drain() {
            pending.fail(reason.clone());
        }
        self.unclaimed_executes.clear();
        for (_, initial) in self.initial_responses.drain() {
            let _ = initial.response_tx.send(Err(reason.clone()));
        }
        for wait in self.deferred_waits.drain(..) {
            let _ = wait.response_tx.send(Err(reason.clone()));
        }
    }
}

// ---- delegate runtime --------------------------------------------------------------

#[derive(Clone, Eq, Hash, PartialEq)]
struct CellKey {
    session_id: SessionId,
    cell_id: CellId,
}

impl CellKey {
    fn for_owner(owner: &CellOwner) -> Self {
        Self {
            session_id: owner.session_id.clone(),
            cell_id: owner.cell_id.clone(),
        }
    }
}

/// One in-flight delegate call. Revoking cancels the delegate future AND its
/// completion path, so a future that finishes later can no longer answer.
struct DelegateCall {
    cell: CellKey,
    cancellation: CancellationToken,
    completion_stop: CancellationToken,
}

impl DelegateCall {
    fn revoke(&self) {
        self.cancellation.cancel();
        self.completion_stop.cancel();
    }
}

enum DelegateTask {
    InvokeTool(CodeModeNestedToolCall),
    Notify {
        call_id: RuntimeString,
        cell_id: CellId,
        text: RuntimeString,
    },
}

enum DelegateStartError {
    Duplicate(DelegateRequestId),
    CapacityExceeded,
}

struct DelegateEffects {
    response: Option<(DelegateRequestId, Result<DelegateResponse, RuntimeString>)>,
    closed_cells: Vec<CellOwner>,
}

impl DelegateEffects {
    fn empty() -> Self {
        Self {
            response: None,
            closed_cells: Vec::new(),
        }
    }

    fn append(&mut self, mut other: Self) {
        debug_assert!(self.response.is_none());
        self.response = other.response.take();
        self.closed_cells.append(&mut other.closed_cells);
    }
}

struct DelegateRuntime {
    calls: HashMap<DelegateRequestId, DelegateCall>,
    seen_requests: HashSet<DelegateRequestId>,
    request_order: VecDeque<DelegateRequestId>,
    event_tx: mpsc::Sender<DriverEvent>,
}

impl DelegateRuntime {
    fn new(event_tx: mpsc::Sender<DriverEvent>) -> Self {
        Self {
            calls: HashMap::new(),
            seen_requests: HashSet::new(),
            request_order: VecDeque::new(),
            event_tx,
        }
    }

    fn start(
        &mut self,
        id: DelegateRequestId,
        target: DelegateTarget,
        request: DelegateRequest,
    ) -> Result<(), DelegateStartError> {
        if self.calls.contains_key(&id) || self.seen_requests.contains(&id) {
            return Err(DelegateStartError::Duplicate(id));
        }
        self.remember_request(id);
        if self.calls.len() >= MAX_PENDING_DELEGATE_CALLS {
            return Err(DelegateStartError::CapacityExceeded);
        }
        let cancellation = CancellationToken::new();
        // The engine-visible cell id always wins over the id on the wire, so a
        // delegate only ever sees ids from its own generation.
        let task_request = match request {
            DelegateRequest::InvokeTool { invocation } => {
                let mut invocation: CodeModeNestedToolCall = invocation.into();
                invocation.cell_id = target.cell_id.clone();
                DelegateTask::InvokeTool(invocation)
            }
            DelegateRequest::Notify { call_id, text, .. } => DelegateTask::Notify {
                call_id,
                cell_id: target.cell_id.clone(),
                text,
            },
        };
        let delegate = target.delegate;
        let task_cancellation = cancellation.clone();
        let delegate_task = tokio::spawn(async move {
            match task_request {
                DelegateTask::InvokeTool(invocation) => delegate
                    .invoke_tool(invocation, task_cancellation)
                    .await
                    .map(|result| DelegateResponse::ToolResult { result })
                    .map_err(|rejection| rejection.message),
                DelegateTask::Notify {
                    call_id,
                    cell_id,
                    text,
                } => delegate
                    .notify(call_id, cell_id, text, task_cancellation)
                    .await
                    .map(|()| DelegateResponse::NotificationDelivered),
            }
        });
        let completion_stop = CancellationToken::new();
        self.calls.insert(
            id,
            DelegateCall {
                cell: CellKey {
                    session_id: target.session_id,
                    cell_id: target.cell_id,
                },
                cancellation,
                completion_stop: completion_stop.clone(),
            },
        );
        let event_tx = self.event_tx.clone();
        tokio::spawn(async move {
            let result = tokio::select! {
                biased;
                _ = completion_stop.cancelled() => return,
                result = delegate_task => match result {
                    Ok(result) => result,
                    Err(err) => Err(RuntimeString::from(format!(
                        "code-mode delegate task failed: {err}"
                    ))),
                },
            };
            tokio::select! {
                biased;
                _ = completion_stop.cancelled() => {}
                _ = event_tx.send(DriverEvent::DelegateCompleted { id, result }) => {}
            }
        });
        Ok(())
    }

    fn cancel(&mut self, id: DelegateRequestId) {
        if let Some(call) = self.calls.remove(&id) {
            call.revoke();
        }
    }

    fn complete(
        &mut self,
        id: DelegateRequestId,
        result: Result<DelegateResponse, RuntimeString>,
    ) -> DelegateEffects {
        if self.calls.remove(&id).is_none() {
            return DelegateEffects::empty();
        }
        let mut effects = DelegateEffects::empty();
        effects.response = Some((id, result));
        effects
    }

    fn close_cell(&mut self, owner: CellOwner) -> DelegateEffects {
        let key = CellKey::for_owner(&owner);
        self.calls.retain(|_, call| {
            if call.cell != key {
                return true;
            }
            call.revoke();
            false
        });
        let mut effects = DelegateEffects::empty();
        effects.closed_cells.push(owner);
        effects
    }

    fn close_cells(&mut self, owners: Vec<CellOwner>) -> DelegateEffects {
        let mut effects = DelegateEffects::empty();
        for owner in owners {
            effects.append(self.close_cell(owner));
        }
        effects
    }

    fn fail_all(&mut self, failed_sessions: Vec<FailedSession>) {
        for (_, call) in self.calls.drain() {
            call.revoke();
        }
        for session in failed_sessions {
            session.cleanup.fail(session.cells);
        }
    }

    fn remember_request(&mut self, id: DelegateRequestId) {
        self.seen_requests.insert(id);
        self.request_order.push_back(id);
        while self.request_order.len() > MAX_RECENT_DELEGATE_REQUEST_IDS {
            if let Some(expired) = self.request_order.pop_front() {
                self.seen_requests.remove(&expired);
            }
        }
    }
}

// ---- the driver --------------------------------------------------------------------

pub(super) struct ConnectionDriver {
    command_rx: mpsc::Receiver<DriverCommand>,
    event_rx: mpsc::Receiver<DriverEvent>,
    event_tx: mpsc::Sender<DriverEvent>,
    execute_claim_rx: mpsc::UnboundedReceiver<RequestId>,
    outgoing_tx: mpsc::Sender<EncodedFrame>,
    requests: RequestTracker,
    sessions: SessionRegistry,
    delegates: DelegateRuntime,
    alive: Arc<AtomicBool>,
    failure: Arc<StdMutex<Option<RuntimeString>>>,
    cancellation: CancellationToken,
    failed: bool,
}

impl ConnectionDriver {
    pub(super) fn new(
        command_rx: mpsc::Receiver<DriverCommand>,
        event_rx: mpsc::Receiver<DriverEvent>,
        event_tx: mpsc::Sender<DriverEvent>,
        outgoing_tx: mpsc::Sender<EncodedFrame>,
        lifecycle: DriverLifecycle,
    ) -> (Self, mpsc::UnboundedSender<RequestId>) {
        let (execute_claim_tx, execute_claim_rx) = mpsc::unbounded_channel();
        (
            Self {
                command_rx,
                event_rx,
                event_tx: event_tx.clone(),
                execute_claim_rx,
                outgoing_tx,
                requests: RequestTracker::new(),
                sessions: SessionRegistry::new(),
                delegates: DelegateRuntime::new(event_tx),
                alive: lifecycle.alive,
                failure: lifecycle.failure,
                cancellation: lifecycle.cancellation,
                failed: false,
            },
            execute_claim_tx,
        )
    }

    pub(super) async fn run(mut self) {
        loop {
            tokio::select! {
                biased;
                _ = self.cancellation.cancelled() => {
                    self.fail(RuntimeString::from("code-mode host connection closed"));
                    return;
                }
                event = self.event_rx.recv() => {
                    let Some(event) = event else {
                        self.fail(RuntimeString::from("code-mode host event stream closed"));
                        return;
                    };
                    if !self.cancel_dropped_callers() || !self.handle_event(event) {
                        return;
                    }
                }
                claim = self.execute_claim_rx.recv() => {
                    let Some(request_id) = claim else {
                        self.fail(RuntimeString::from("code-mode execute claim stream closed"));
                        return;
                    };
                    self.requests.claim_execute(request_id);
                }
                command = self.command_rx.recv() => {
                    let Some(command) = command else {
                        self.fail(RuntimeString::from("code-mode host command stream closed"));
                        return;
                    };
                    if !self.cancel_dropped_callers() || !self.handle_command(command) {
                        return;
                    }
                }
            }
        }
    }

    fn handle_event(&mut self, event: DriverEvent) -> bool {
        let keep_running = match event {
            DriverEvent::HostMessage(message) => self.handle_host_message(message),
            DriverEvent::DelegateCompleted { id, result } => self.complete_delegate(id, result),
            DriverEvent::RequestCancelled(id) => self.cancel_request(id),
            DriverEvent::Failed(reason) => {
                self.fail(reason);
                false
            }
        };
        if keep_running {
            self.flush_deferred_waits()
        } else {
            false
        }
    }

    fn handle_command(&mut self, command: DriverCommand) -> bool {
        match command {
            DriverCommand::OpenSession {
                session,
                delegate,
                cleanup,
                caller_cancellation,
                response_tx,
            } => self.open_session(session, delegate, cleanup, caller_cancellation, response_tx),
            DriverCommand::Execute {
                session,
                request,
                caller_cancellation,
                response_tx,
            } => self.execute(session, request, caller_cancellation, response_tx),
            DriverCommand::Wait {
                session,
                request,
                caller_cancellation,
                response_tx,
            } => self.wait(session, request, caller_cancellation, response_tx),
            DriverCommand::Terminate {
                session,
                cell_id,
                response_tx,
            } => self.terminate(session, cell_id, response_tx),
            DriverCommand::ShutdownSession {
                session,
                response_tx,
            } => self.shutdown_session(session, response_tx),
            DriverCommand::ReplEval {
                session,
                request,
                caller_cancellation,
                response_tx,
            } => self.repl_eval(session, request, caller_cancellation, response_tx),
            DriverCommand::ReplTerminate {
                session,
                exec_id,
                response_tx,
            } => self.repl_terminate(session, exec_id, response_tx),
        }
    }

    fn open_session(
        &mut self,
        session: RemoteSession,
        delegate: Arc<dyn CodeModeSessionDelegate>,
        cleanup: SessionCleanup,
        caller_cancellation: CancellationToken,
        response_tx: oneshot::Sender<Result<(), RuntimeString>>,
    ) -> bool {
        if self.sessions.contains(&session.id) || self.requests.contains_pending_open(&session) {
            let _ = response_tx.send(Err(RuntimeString::from(format!(
                "code-mode session {} is already open",
                session.id
            ))));
            return true;
        }
        let cancellation = CancellableRequest::new(caller_cancellation);
        self.send_request(
            HostRequest::OpenSession {
                session_id: session.id.clone(),
            },
            PendingRequest::OpenSession {
                session,
                delegate,
                cleanup,
                cancellation,
                response_tx,
            },
        )
    }

    fn execute(
        &mut self,
        session: RemoteSession,
        request: ExecuteRequest,
        caller_cancellation: CancellationToken,
        response_tx: oneshot::Sender<Result<DeliveredExecute, RuntimeString>>,
    ) -> bool {
        if let Err(err) = self.sessions.require_ready(&session) {
            let _ = response_tx.send(Err(err));
            return true;
        }
        let request = match request.try_into() {
            Ok(request) => request,
            Err(err) => {
                let _ = response_tx.send(Err(RuntimeString::from(format!(
                    "failed to encode code-mode execute request: {err}"
                ))));
                return true;
            }
        };
        let (initial_response_tx, initial_response_rx) = oneshot::channel();
        let cancellation = CancellableRequest::new(caller_cancellation);
        self.send_request(
            HostRequest::Execute {
                session_id: session.id.clone(),
                request,
            },
            PendingRequest::Execute {
                session,
                response_tx,
                initial_response_tx,
                initial_response_rx,
                cancellation,
            },
        )
    }

    fn wait(
        &mut self,
        session: RemoteSession,
        request: WaitRequest,
        caller_cancellation: CancellationToken,
        response_tx: oneshot::Sender<Result<WaitOutcome, RuntimeString>>,
    ) -> bool {
        if let Err(err) = self.sessions.require_ready(&session) {
            let _ = response_tx.send(Err(err));
            return true;
        }
        let request = match remote_wait_request(&session, request) {
            Ok(request) => request,
            Err(err) => {
                let _ = response_tx.send(Err(err));
                return true;
            }
        };
        // A cancelled-but-unanswered wait still owns this cell's next frontier.
        if self.requests.has_cancelled_wait(&session, &request.cell_id) {
            self.requests.push_deferred_wait(DeferredWait {
                session,
                request,
                caller_cancellation,
                response_tx,
            });
            return true;
        }
        self.start_wait(session, request, caller_cancellation, response_tx)
    }

    fn start_wait(
        &mut self,
        session: RemoteSession,
        request: WireWaitRequest,
        caller_cancellation: CancellationToken,
        response_tx: oneshot::Sender<Result<WaitOutcome, RuntimeString>>,
    ) -> bool {
        let cell_id = request.cell_id.clone();
        self.send_request(
            HostRequest::Wait {
                session_id: session.id.clone(),
                request,
            },
            PendingRequest::Wait {
                session,
                cell_id,
                cancellation: CancellableRequest::new(caller_cancellation),
                response_tx,
            },
        )
    }

    fn terminate(
        &mut self,
        session: RemoteSession,
        cell_id: CellId,
        response_tx: oneshot::Sender<Result<WaitOutcome, RuntimeString>>,
    ) -> bool {
        if let Err(err) = self.sessions.require_ready(&session) {
            let _ = response_tx.send(Err(err));
            return true;
        }
        let cell_id = match remote_cell_id(&session, &cell_id) {
            Ok(cell_id) => cell_id,
            Err(err) => {
                let _ = response_tx.send(Err(err));
                return true;
            }
        };
        let pending_cell_id = cell_id.clone();
        self.send_request(
            HostRequest::Terminate {
                session_id: session.id.clone(),
                cell_id,
            },
            PendingRequest::Terminate {
                session,
                cell_id: pending_cell_id,
                response_tx,
            },
        )
    }

    fn shutdown_session(
        &mut self,
        session: RemoteSession,
        response_tx: oneshot::Sender<Result<(), RuntimeString>>,
    ) -> bool {
        if let Err(err) = self.sessions.begin_shutdown(&session) {
            let _ = response_tx.send(Err(err));
            return true;
        }
        self.send_request(
            HostRequest::ShutdownSession {
                session_id: session.id.clone(),
            },
            PendingRequest::ShutdownSession {
                session,
                response_tx,
            },
        )
    }

    fn repl_eval(
        &mut self,
        session: RemoteSession,
        request: WireReplEvalRequest,
        caller_cancellation: CancellationToken,
        response_tx: oneshot::Sender<Result<WireReplOutcome, RuntimeString>>,
    ) -> bool {
        if let Err(err) = self.sessions.require_ready(&session) {
            let _ = response_tx.send(Err(err));
            return true;
        }
        // Admit the evaluation under its exec id so delegate frames carrying
        // that id route to this session's delegate exactly like cell frames.
        // This task is the only toucher of the registry and host frames are
        // processed in later loop iterations, so admitting before the frame
        // is queued cannot race the first nested call. A frame that fails to
        // encode leaves the id registered; the entry is inert and reclaimed
        // with the session.
        let exec_id = WireCellId::new(request.exec_id.clone());
        match self.sessions.admit_cell(&session, exec_id.clone()) {
            Ok(_) => {}
            Err(CellAdmissionError::MissingSession) => {
                let _ = response_tx.send(Err(RuntimeString::from(
                    "code-mode session closed during repl eval",
                )));
                return true;
            }
            Err(CellAdmissionError::DuplicateCell) => {
                let _ = response_tx.send(Err(RuntimeString::from("duplicate REPL exec id ")
                    .concat(exec_id.as_runtime_string())
                    .concat(&RuntimeString::from(format!(" in session {}", session.id)))));
                return true;
            }
        }
        let cancellation = CancellableRequest::new(caller_cancellation);
        self.send_request(
            HostRequest::ReplEval {
                session_id: session.id.clone(),
                request,
            },
            PendingRequest::ReplEval {
                session,
                exec_id,
                cancellation,
                response_tx,
            },
        )
    }

    fn repl_terminate(
        &mut self,
        session: RemoteSession,
        exec_id: RuntimeString,
        response_tx: oneshot::Sender<Result<(), RuntimeString>>,
    ) -> bool {
        if let Err(err) = self.sessions.require_ready(&session) {
            let _ = response_tx.send(Err(err));
            return true;
        }
        self.send_request(
            HostRequest::ReplTerminate {
                session_id: session.id.clone(),
                exec_id: exec_id.clone(),
            },
            PendingRequest::ReplTerminate {
                exec_id,
                response_tx,
            },
        )
    }

    fn send_request(&mut self, request: HostRequest, pending: PendingRequest) -> bool {
        let request_id = match self.requests.allocate_id() {
            Ok(id) => id,
            Err(err) => {
                pending.fail(err);
                return false;
            }
        };
        let message = ClientToHost::Request {
            id: request_id,
            request,
        };
        let frame = match EncodedFrame::encode(&message) {
            Ok(frame) => frame,
            Err(err) => {
                pending.fail(RuntimeString::from(format!(
                    "code-mode request exceeds the host frame limit: {err}"
                )));
                return true;
            }
        };
        self.requests
            .insert_pending(request_id, pending, &self.event_tx);
        self.queue_frame(frame)
    }

    fn flush_deferred_waits(&mut self) -> bool {
        let mut deferred = self.requests.take_deferred_waits();
        while let Some(wait) = deferred.pop_front() {
            if wait.caller_cancellation.is_cancelled() {
                let _ = wait
                    .response_tx
                    .send(Err(RuntimeString::from("code-mode request cancelled")));
                continue;
            }
            if self
                .requests
                .has_cancelled_wait(&wait.session, &wait.request.cell_id)
            {
                self.requests.push_deferred_wait(wait);
                continue;
            }
            if !self.start_wait(
                wait.session,
                wait.request,
                wait.caller_cancellation,
                wait.response_tx,
            ) {
                for wait in deferred {
                    let _ = wait
                        .response_tx
                        .send(Err(RuntimeString::from("code-mode host connection closed")));
                }
                return false;
            }
        }
        true
    }

    fn handle_host_message(&mut self, message: HostToClient) -> bool {
        match message {
            HostToClient::Response { id, result } => {
                self.complete_request(id, result.into_result())
            }
            HostToClient::InitialResponse { id, result } => {
                self.complete_initial_response(id, result.into_result())
            }
            HostToClient::DelegateRequest {
                id,
                session_id,
                request,
            } => self.start_delegate(id, session_id, request),
            HostToClient::CancelDelegateRequest { id } => {
                self.delegates.cancel(id);
                true
            }
            HostToClient::CellClosed {
                session_id,
                cell_id,
            } => self.close_cell(session_id, cell_id),
            HostToClient::HostHello(_) | HostToClient::HandshakeRejected { .. } => {
                self.fail(RuntimeString::from(
                    "code-mode host sent a second handshake response",
                ));
                false
            }
        }
    }

    fn complete_request(
        &mut self,
        id: RequestId,
        result: Result<HostResponse, RuntimeString>,
    ) -> bool {
        let Some(pending) = self.requests.remove_pending(id) else {
            self.fail(RuntimeString::from(format!(
                "code-mode host returned unknown request id {id}"
            )));
            return false;
        };
        match pending {
            PendingRequest::OpenSession {
                session,
                delegate,
                cleanup,
                cancellation,
                response_tx,
            } => match result {
                Ok(HostResponse::SessionReady { session_id }) if session_id == session.id => {
                    let abandoned = cancellation.is_cancelled() || response_tx.is_closed();
                    self.sessions
                        .insert_ready(session.clone(), delegate, cleanup);
                    if abandoned || response_tx.send(Ok(())).is_err() {
                        return self.shutdown_abandoned_session(session);
                    }
                }
                Ok(_) => {
                    return self.fatal_response(
                        response_tx,
                        "code-mode host returned an invalid open-session response",
                    );
                }
                Err(err) => {
                    let _ = response_tx.send(Err(err));
                }
            },
            PendingRequest::Execute {
                session,
                response_tx,
                initial_response_tx,
                initial_response_rx,
                cancellation,
            } => match result {
                Ok(HostResponse::ExecutionStarted { cell_id }) => {
                    let remote_cell_id = cell_id.clone();
                    let public_id = match self.sessions.admit_cell(&session, cell_id) {
                        Ok(public_id) => public_id,
                        Err(CellAdmissionError::MissingSession) => {
                            let _ = response_tx.send(Err(RuntimeString::from(
                                "code-mode session closed during execute",
                            )));
                            return true;
                        }
                        Err(CellAdmissionError::DuplicateCell) => {
                            let reason = RuntimeString::from("code-mode host reused live cell ")
                                .concat(remote_cell_id.as_runtime_string())
                                .concat(&RuntimeString::from(format!(
                                    " in session {}",
                                    session.id
                                )));
                            let _ = response_tx.send(Err(reason.clone()));
                            self.fail(reason);
                            return false;
                        }
                    };
                    self.requests.insert_initial_response(
                        id,
                        InitialResponse {
                            generation: session.generation,
                            cell_id: remote_cell_id.clone(),
                            response_tx: initial_response_tx,
                        },
                    );
                    let started = StartedCell::from_result_receiver(public_id, initial_response_rx);
                    if cancellation.is_cancelled() || response_tx.is_closed() {
                        return self.terminate_abandoned_cell(session, remote_cell_id);
                    }
                    let delivered = DeliveredExecute {
                        request_id: id,
                        started,
                    };
                    if response_tx.send(Ok(delivered)).is_err() {
                        return self.terminate_abandoned_cell(session, remote_cell_id);
                    }
                    self.requests.insert_unclaimed_execute(
                        id,
                        UnclaimedExecute {
                            session,
                            cell_id: remote_cell_id,
                            cancellation,
                        },
                    );
                }
                Ok(_) => {
                    return self.fatal_response(
                        response_tx,
                        "code-mode host returned an invalid execute response",
                    );
                }
                Err(err) => {
                    let _ = response_tx.send(Err(err));
                }
            },
            PendingRequest::Wait {
                session,
                cell_id,
                cancellation: _,
                response_tx,
            } => {
                let result = match result {
                    Ok(HostResponse::WaitCompleted { outcome }) => {
                        if wait_outcome_cell_id(&outcome) != &cell_id {
                            let reason = wrong_cell(wait_outcome_cell_id(&outcome), &cell_id);
                            let _ = response_tx.send(Err(reason.clone()));
                            self.fail(reason);
                            return false;
                        }
                        Ok(public_wait_outcome(session.generation, outcome.into()))
                    }
                    Ok(_) => {
                        return self.fatal_response(
                            response_tx,
                            "code-mode host returned an invalid cell response",
                        );
                    }
                    Err(err) => Err(err),
                };
                let _ = response_tx.send(result);
            }
            PendingRequest::Terminate {
                session,
                cell_id,
                response_tx,
            } => {
                let result = match result {
                    Ok(HostResponse::WaitCompleted { outcome }) => {
                        if wait_outcome_cell_id(&outcome) != &cell_id {
                            let reason = wrong_cell(wait_outcome_cell_id(&outcome), &cell_id);
                            let _ = response_tx.send(Err(reason.clone()));
                            self.fail(reason);
                            return false;
                        }
                        public_wait_outcome(session.generation, outcome.into())
                    }
                    Ok(_) => {
                        return self.fatal_response(
                            response_tx,
                            "code-mode host returned an invalid cell response",
                        );
                    }
                    Err(err) => {
                        let _ = response_tx.send(Err(err));
                        return true;
                    }
                };
                let _ = response_tx.send(Ok(result));
            }
            PendingRequest::ShutdownSession {
                session,
                response_tx,
            } => match result {
                Ok(HostResponse::SessionClosed { session_id }) if session_id == session.id => {
                    let effects = self.close_session_locally(&session.id);
                    if !self.apply_delegate_effects(effects) {
                        return false;
                    }
                    let _ = response_tx.send(Ok(()));
                }
                Ok(_) => {
                    return self.fatal_response(
                        response_tx,
                        "code-mode host returned an invalid shutdown response",
                    );
                }
                // A host that cannot close a session has lost track of it; the
                // connection is no longer trustworthy.
                Err(err) => {
                    let _ = response_tx.send(Err(err.clone()));
                    self.fail(err);
                    return false;
                }
            },
            PendingRequest::ReplEval {
                session,
                exec_id,
                cancellation: _,
                response_tx,
            } => match result {
                // The host answers an ADMITTED evaluation with its outcome —
                // an infrastructure failure of the runtime included, as the
                // outcome's error — and then reports the exec closed, which
                // reclaims the registry entry exactly like a cell teardown.
                Ok(HostResponse::ReplCompleted { outcome }) => {
                    let _ = response_tx.send(Ok(outcome));
                }
                Ok(_) => {
                    return self.fatal_response(
                        response_tx,
                        "code-mode host returned an invalid repl eval response",
                    );
                }
                // A pre-admission rejection: no CellClosed frame will follow,
                // so reclaim the optimistic admission here.
                Err(err) => {
                    if let Ok(owner) = self.sessions.remove_cell(&session.id, &exec_id) {
                        let effects = self.delegates.close_cell(owner);
                        if !self.apply_delegate_effects(effects) {
                            return false;
                        }
                    }
                    let _ = response_tx.send(Err(err));
                }
            },
            PendingRequest::ReplTerminate {
                exec_id,
                response_tx,
                ..
            } => match result {
                Ok(HostResponse::ReplTerminated {
                    exec_id: terminated,
                }) if terminated == exec_id => {
                    let _ = response_tx.send(Ok(()));
                }
                Ok(_) => {
                    return self.fatal_response(
                        response_tx,
                        "code-mode host returned an invalid repl terminate response",
                    );
                }
                Err(err) => {
                    let _ = response_tx.send(Err(err));
                }
            },
        }
        true
    }

    /// Answer one caller with a contract violation and take the connection down.
    fn fatal_response<T>(
        &mut self,
        response_tx: oneshot::Sender<Result<T, RuntimeString>>,
        reason: &str,
    ) -> bool {
        let reason = RuntimeString::from(reason);
        let _ = response_tx.send(Err(reason.clone()));
        self.fail(reason);
        false
    }

    fn cancel_dropped_callers(&mut self) -> bool {
        for action in self.requests.collect_cancellations() {
            if !self.apply_cancellation(action) {
                return false;
            }
        }
        true
    }

    fn cancel_request(&mut self, id: RequestId) -> bool {
        self.requests
            .mark_cancelled(id)
            .is_none_or(|action| self.apply_cancellation(action))
    }

    fn apply_cancellation(&mut self, action: CancellationAction) -> bool {
        match action {
            CancellationAction::Send(id) => self.send_cancel_request(id),
            CancellationAction::Terminate {
                request_id,
                execute,
            } => {
                if !self.send_cancel_request(request_id) {
                    return false;
                }
                self.terminate_abandoned_cell(execute.session, execute.cell_id)
            }
        }
    }

    fn send_cancel_request(&mut self, id: RequestId) -> bool {
        let frame = match EncodedFrame::encode(&ClientToHost::CancelRequest { id }) {
            Ok(frame) => frame,
            Err(err) => {
                self.fail(RuntimeString::from(format!(
                    "failed to encode code-mode cancellation request: {err}"
                )));
                return false;
            }
        };
        self.queue_frame(frame)
    }

    /// The opener of a session vanished after the host committed it: close it
    /// remotely so the host does not keep the session forever.
    fn shutdown_abandoned_session(&mut self, session: RemoteSession) -> bool {
        let Some(should_shutdown) = self.sessions.begin_abandoned_shutdown(&session.id) else {
            self.fail(RuntimeString::from(format!(
                "code-mode host committed abandoned session {} without local state",
                session.id
            )));
            return false;
        };
        if !should_shutdown {
            return true;
        }
        let (response_tx, response_rx) = oneshot::channel();
        drop(response_rx);
        self.send_request(
            HostRequest::ShutdownSession {
                session_id: session.id.clone(),
            },
            PendingRequest::ShutdownSession {
                session,
                response_tx,
            },
        )
    }

    fn terminate_abandoned_cell(&mut self, session: RemoteSession, cell_id: WireCellId) -> bool {
        let Some(is_closing) = self.sessions.is_closing(&session.id) else {
            self.fail(RuntimeString::from(format!(
                "code-mode host admitted an abandoned cell in unknown session {}",
                session.id
            )));
            return false;
        };
        if is_closing {
            return true;
        }
        let (response_tx, response_rx) = oneshot::channel();
        drop(response_rx);
        self.send_request(
            HostRequest::Terminate {
                session_id: session.id.clone(),
                cell_id: cell_id.clone(),
            },
            PendingRequest::Terminate {
                session,
                cell_id,
                response_tx,
            },
        )
    }

    fn complete_initial_response(
        &mut self,
        id: RequestId,
        result: Result<WireRuntimeResponse, RuntimeString>,
    ) -> bool {
        let Some(initial) = self.requests.remove_initial_response(id) else {
            self.fail(RuntimeString::from(format!(
                "code-mode host returned initial response for unknown request id {id}"
            )));
            return false;
        };
        let response = match result {
            Ok(response) if runtime_response_cell_id(&response) == &initial.cell_id => {
                Ok(public_runtime_response(initial.generation, response.into()))
            }
            Ok(response) => {
                let reason =
                    RuntimeString::from("code-mode host returned initial response for cell ")
                        .concat(runtime_response_cell_id(&response).as_runtime_string())
                        .concat(&RuntimeString::from(" instead of "))
                        .concat(initial.cell_id.as_runtime_string());
                let _ = initial.response_tx.send(Err(reason.clone()));
                self.fail(reason);
                return false;
            }
            Err(err) => Err(err),
        };
        let _ = initial.response_tx.send(response);
        true
    }

    fn start_delegate(
        &mut self,
        id: DelegateRequestId,
        session_id: SessionId,
        request: DelegateRequest,
    ) -> bool {
        let wire_cell_id = match &request {
            DelegateRequest::InvokeTool { invocation } => &invocation.cell_id,
            DelegateRequest::Notify { cell_id, .. } => cell_id,
        };
        let target = match self.sessions.delegate_target(&session_id, wire_cell_id) {
            Ok(target) => target,
            Err(err) => {
                self.fail(err);
                return false;
            }
        };
        match self.delegates.start(id, target, request) {
            Ok(()) => true,
            Err(DelegateStartError::Duplicate(id)) => {
                self.fail(RuntimeString::from(format!(
                    "duplicate code-mode delegate request id {id}"
                )));
                false
            }
            Err(DelegateStartError::CapacityExceeded) => self.send_delegate_response(
                id,
                Err(RuntimeString::from(format!(
                    "code-mode host exceeded the limit of {MAX_PENDING_DELEGATE_CALLS} pending delegate calls"
                ))),
            ),
        }
    }

    fn complete_delegate(
        &mut self,
        id: DelegateRequestId,
        result: Result<DelegateResponse, RuntimeString>,
    ) -> bool {
        let effects = self.delegates.complete(id, result);
        self.apply_delegate_effects(effects)
    }

    fn send_delegate_response(
        &mut self,
        id: DelegateRequestId,
        result: Result<DelegateResponse, RuntimeString>,
    ) -> bool {
        let message = ClientToHost::DelegateResponse {
            id,
            result: WireResult::from_result(result),
        };
        let frame = match EncodedFrame::encode(&message) {
            Ok(frame) => frame,
            Err(err) => {
                // The result itself is too large to send; answer with the
                // failure so the host's cell is not left waiting forever.
                let fallback = ClientToHost::DelegateResponse {
                    id,
                    result: WireResult::Err {
                        message: RuntimeString::from(format!(
                            "code-mode delegate response exceeds the host frame limit: {err}"
                        )),
                    },
                };
                match EncodedFrame::encode(&fallback) {
                    Ok(frame) => frame,
                    Err(fallback_err) => {
                        self.fail(RuntimeString::from(format!(
                            "failed to encode code-mode delegate error response: {fallback_err}"
                        )));
                        return false;
                    }
                }
            }
        };
        self.queue_frame(frame)
    }

    fn close_cell(&mut self, session_id: SessionId, cell_id: WireCellId) -> bool {
        let owner = match self.sessions.remove_cell(&session_id, &cell_id) {
            Ok(owner) => owner,
            Err(err) => {
                self.fail(err);
                return false;
            }
        };
        let effects = self.delegates.close_cell(owner);
        self.apply_delegate_effects(effects)
    }

    fn close_session_locally(&mut self, session_id: &SessionId) -> DelegateEffects {
        self.requests.remove_unclaimed_for_session(session_id);
        let owners = self.sessions.remove_session(session_id);
        self.delegates.close_cells(owners)
    }

    fn apply_delegate_effects(&mut self, effects: DelegateEffects) -> bool {
        if let Some((id, result)) = effects.response {
            if !self.send_delegate_response(id, result) {
                return false;
            }
        }
        for closed in effects.closed_cells {
            notify_cell_closed(&closed.delegate, &closed.cell_id);
        }
        true
    }

    fn queue_frame(&mut self, frame: EncodedFrame) -> bool {
        match self.outgoing_tx.try_send(frame) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.fail(RuntimeString::from("code-mode host outgoing queue is full"));
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.fail(RuntimeString::from("code-mode host writer closed"));
                false
            }
        }
    }

    /// Record the first failure reason, then fail every caller and report every
    /// live cell closed. Later failures keep the first reason.
    fn fail(&mut self, reason: RuntimeString) {
        if self.failed {
            return;
        }
        self.failed = true;
        self.alive.store(false, Ordering::Release);
        let reason = {
            let mut failure = self.failure.lock_recover();
            failure.get_or_insert(reason).clone()
        };
        self.requests.fail_all(&reason);
        let failed_sessions = self.sessions.drain();
        self.delegates.fail_all(failed_sessions);
        self.cancellation.cancel();
    }
}

impl Drop for ConnectionDriver {
    fn drop(&mut self) {
        self.fail(RuntimeString::from(
            "code-mode connection driver stopped unexpectedly",
        ));
    }
}

fn wrong_cell(returned: &WireCellId, expected: &WireCellId) -> RuntimeString {
    RuntimeString::from("code-mode host returned cell ")
        .concat(returned.as_runtime_string())
        .concat(&RuntimeString::from(" for request targeting "))
        .concat(expected.as_runtime_string())
}
