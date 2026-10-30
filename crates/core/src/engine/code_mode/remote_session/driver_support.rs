//! Driver support: cleanup signalling, cell-id translation, the session
//! registry, and the driver's command/event vocabulary.

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::engine::code_mode::host::{
    DelegateRequestId, DelegateResponse, HostToClient, RequestId, SessionId, WireCellId,
    WireReplEvalRequest, WireReplOutcome, WireRuntimeResponse, WireWaitOutcome, WireWaitRequest,
};
use crate::engine::code_mode::protocol::{
    CellId, ExecuteRequest, RuntimeResponse, WaitOutcome, WaitRequest,
};
use crate::engine::code_mode::session::{CodeModeSessionDelegate, StartedCell};
use crate::runtime_values::RuntimeString;

/// Report a cell closed without letting a panicking delegate take the driver
/// down: teardown must finish for every OTHER cell too.
pub(super) fn notify_cell_closed(delegate: &Arc<dyn CodeModeSessionDelegate>, cell_id: &CellId) {
    let _ = std::panic::catch_unwind(AssertUnwindSafe(|| delegate.cell_closed(cell_id)));
}

// ---- cleanup -----------------------------------------------------------------------

struct CleanupInner {
    complete: CancellationToken,
}

/// Completes once a session's cells have all been reported closed, so a
/// shutdown can wait for teardown it did not itself drive.
#[derive(Clone)]
pub(super) struct SessionCleanup {
    inner: Arc<CleanupInner>,
}

impl SessionCleanup {
    pub(super) fn new() -> Self {
        Self {
            inner: Arc::new(CleanupInner {
                complete: CancellationToken::new(),
            }),
        }
    }

    pub(super) fn fail(&self, cells: Vec<CellOwner>) {
        for owner in cells {
            notify_cell_closed(&owner.delegate, &owner.cell_id);
        }
        self.inner.complete.cancel();
    }

    pub(super) async fn wait(&self) {
        self.inner.complete.cancelled().await;
    }

    pub(super) fn is_complete(&self) -> bool {
        self.inner.complete.is_cancelled()
    }
}

// ---- cell ids ----------------------------------------------------------------------

/// A cell id as the ENGINE sees it. The first generation passes host ids
/// through unchanged; a later generation (a replaced host connection) is
/// prefixed, so a stale id from a previous host can never address a live cell.
pub(super) fn public_cell_id(generation: u64, cell_id: &WireCellId) -> CellId {
    if generation == 1 {
        CellId::new(cell_id.as_runtime_string().clone())
    } else {
        CellId::new(
            RuntimeString::from(format!("g{generation}:")).concat(cell_id.as_runtime_string()),
        )
    }
}

fn public_cell_id_from_protocol(generation: u64, cell_id: &CellId) -> CellId {
    public_cell_id(
        generation,
        &WireCellId::new(cell_id.as_runtime_string().clone()),
    )
}

/// Translate an engine-visible cell id back to its host form, rejecting ids
/// that belong to a different generation.
pub(super) fn remote_cell_id(
    session: &RemoteSession,
    cell_id: &CellId,
) -> Result<WireCellId, RuntimeString> {
    let text = cell_id.as_runtime_string();
    if session.generation == 1 {
        if text.starts_with("g") && text.contains(":") {
            return Err(stale_generation(cell_id));
        }
        return Ok(WireCellId::new(text.clone()));
    }
    let prefix = format!("g{}:", session.generation);
    if !text.starts_with(&prefix) {
        return Err(stale_generation(cell_id));
    }
    Ok(WireCellId::new(
        text.slice_utf16(prefix.encode_utf16().count()..),
    ))
}

fn stale_generation(cell_id: &CellId) -> RuntimeString {
    RuntimeString::from("cell ")
        .concat(cell_id.as_runtime_string())
        .concat(&RuntimeString::from(
            " belongs to a stale code-mode host generation",
        ))
}

pub(super) fn remote_wait_request(
    session: &RemoteSession,
    request: WaitRequest,
) -> Result<WireWaitRequest, RuntimeString> {
    let cell_id = remote_cell_id(session, &request.cell_id)?;
    WireWaitRequest::try_from(WaitRequest {
        cell_id: request.cell_id.clone(),
        ..request
    })
    .map(|wire| WireWaitRequest { cell_id, ..wire })
    .map_err(|error| {
        RuntimeString::from(format!("failed to encode code-mode wait request: {error}"))
    })
}

pub(super) fn public_runtime_response(
    generation: u64,
    response: RuntimeResponse,
) -> RuntimeResponse {
    match response {
        RuntimeResponse::Yielded {
            cell_id,
            content_items,
        } => RuntimeResponse::Yielded {
            cell_id: public_cell_id_from_protocol(generation, &cell_id),
            content_items,
        },
        RuntimeResponse::Terminated {
            cell_id,
            content_items,
        } => RuntimeResponse::Terminated {
            cell_id: public_cell_id_from_protocol(generation, &cell_id),
            content_items,
        },
        RuntimeResponse::Result {
            cell_id,
            content_items,
            error_text,
        } => RuntimeResponse::Result {
            cell_id: public_cell_id_from_protocol(generation, &cell_id),
            content_items,
            error_text,
        },
    }
}

pub(super) fn public_wait_outcome(generation: u64, outcome: WaitOutcome) -> WaitOutcome {
    match outcome {
        WaitOutcome::LiveCell(response) => {
            WaitOutcome::LiveCell(public_runtime_response(generation, response))
        }
        WaitOutcome::MissingCell(response) => {
            WaitOutcome::MissingCell(public_runtime_response(generation, response))
        }
    }
}

pub(super) fn runtime_response_cell_id(response: &WireRuntimeResponse) -> &WireCellId {
    match response {
        WireRuntimeResponse::Yielded { cell_id, .. }
        | WireRuntimeResponse::Terminated { cell_id, .. }
        | WireRuntimeResponse::Result { cell_id, .. } => cell_id,
    }
}

pub(super) fn wait_outcome_cell_id(outcome: &WireWaitOutcome) -> &WireCellId {
    match outcome {
        WireWaitOutcome::LiveCell(response) | WireWaitOutcome::MissingCell(response) => {
            runtime_response_cell_id(response)
        }
    }
}

// ---- session registry --------------------------------------------------------------

pub(super) struct CellOwner {
    pub(super) session_id: SessionId,
    pub(super) cell_id: CellId,
    pub(super) delegate: Arc<dyn CodeModeSessionDelegate>,
}

pub(super) struct DelegateTarget {
    pub(super) session_id: SessionId,
    pub(super) cell_id: CellId,
    pub(super) delegate: Arc<dyn CodeModeSessionDelegate>,
}

pub(super) struct FailedSession {
    pub(super) cleanup: SessionCleanup,
    pub(super) cells: Vec<CellOwner>,
}

pub(super) enum CellAdmissionError {
    MissingSession,
    DuplicateCell,
}

struct SessionRecord {
    remote: RemoteSession,
    delegate: Arc<dyn CodeModeSessionDelegate>,
    cleanup: SessionCleanup,
    phase: SessionPhase,
    /// Live cells only: client memory tracks concurrency, not session lifetime.
    cells: HashMap<WireCellId, CellId>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum SessionPhase {
    Ready,
    Closing,
}

pub(super) struct SessionRegistry {
    records: HashMap<SessionId, SessionRecord>,
}

impl SessionRegistry {
    pub(super) fn new() -> Self {
        Self {
            records: HashMap::new(),
        }
    }

    pub(super) fn contains(&self, session_id: &SessionId) -> bool {
        self.records.contains_key(session_id)
    }

    pub(super) fn insert_ready(
        &mut self,
        session: RemoteSession,
        delegate: Arc<dyn CodeModeSessionDelegate>,
        cleanup: SessionCleanup,
    ) {
        self.records.insert(
            session.id.clone(),
            SessionRecord {
                remote: session,
                delegate,
                cleanup,
                phase: SessionPhase::Ready,
                cells: HashMap::new(),
            },
        );
    }

    pub(super) fn require_ready(&self, session: &RemoteSession) -> Result<(), RuntimeString> {
        let record = self
            .records
            .get(&session.id)
            .ok_or_else(|| unknown_session(&session.id))?;
        if record.remote != *session {
            return Err(RuntimeString::from("stale code-mode session generation"));
        }
        if record.phase != SessionPhase::Ready {
            return Err(RuntimeString::from("code-mode session is shutting down"));
        }
        Ok(())
    }

    pub(super) fn begin_shutdown(&mut self, session: &RemoteSession) -> Result<(), RuntimeString> {
        let record = self
            .records
            .get_mut(&session.id)
            .ok_or_else(|| unknown_session(&session.id))?;
        if record.remote != *session {
            return Err(RuntimeString::from("stale code-mode session generation"));
        }
        if record.phase == SessionPhase::Closing {
            return Err(RuntimeString::from("code-mode session is already closing"));
        }
        record.phase = SessionPhase::Closing;
        Ok(())
    }

    /// Begin closing a session whose opener went away. `None` means the session
    /// is unknown; `Some(false)` means a shutdown is already under way.
    pub(super) fn begin_abandoned_shutdown(&mut self, session_id: &SessionId) -> Option<bool> {
        let record = self.records.get_mut(session_id)?;
        if record.phase == SessionPhase::Closing {
            return Some(false);
        }
        record.phase = SessionPhase::Closing;
        Some(true)
    }

    pub(super) fn is_closing(&self, session_id: &SessionId) -> Option<bool> {
        self.records
            .get(session_id)
            .map(|record| record.phase == SessionPhase::Closing)
    }

    pub(super) fn admit_cell(
        &mut self,
        session: &RemoteSession,
        cell_id: WireCellId,
    ) -> Result<CellId, CellAdmissionError> {
        let Some(record) = self.records.get_mut(&session.id) else {
            return Err(CellAdmissionError::MissingSession);
        };
        if record.cells.contains_key(&cell_id) {
            return Err(CellAdmissionError::DuplicateCell);
        }
        let public_id = public_cell_id(session.generation, &cell_id);
        record.cells.insert(cell_id, public_id.clone());
        Ok(public_id)
    }

    pub(super) fn delegate_target(
        &self,
        session_id: &SessionId,
        cell_id: &WireCellId,
    ) -> Result<DelegateTarget, RuntimeString> {
        let session = self.records.get(session_id).ok_or_else(|| {
            RuntimeString::from(format!(
                "code-mode host delegated for unknown session {session_id}"
            ))
        })?;
        let public_id = session.cells.get(cell_id).cloned().ok_or_else(|| {
            RuntimeString::from("code-mode host delegated for unknown cell ")
                .concat(cell_id.as_runtime_string())
                .concat(&RuntimeString::from(format!(" in session {session_id}")))
        })?;
        Ok(DelegateTarget {
            session_id: session_id.clone(),
            cell_id: public_id,
            delegate: Arc::clone(&session.delegate),
        })
    }

    pub(super) fn remove_cell(
        &mut self,
        session_id: &SessionId,
        cell_id: &WireCellId,
    ) -> Result<CellOwner, RuntimeString> {
        let session = self.records.get_mut(session_id).ok_or_else(|| {
            RuntimeString::from("code-mode host closed cell ")
                .concat(cell_id.as_runtime_string())
                .concat(&RuntimeString::from(format!(
                    " in unknown session {session_id}"
                )))
        })?;
        let public_id = session.cells.remove(cell_id).ok_or_else(|| {
            RuntimeString::from(format!(
                "code-mode host closed unknown cell in session {session_id}"
            ))
        })?;
        Ok(CellOwner {
            session_id: session_id.clone(),
            cell_id: public_id,
            delegate: Arc::clone(&session.delegate),
        })
    }

    pub(super) fn remove_session(&mut self, session_id: &SessionId) -> Vec<CellOwner> {
        let Some(session) = self.records.remove(session_id) else {
            return Vec::new();
        };
        session
            .cells
            .into_values()
            .map(|cell_id| CellOwner {
                session_id: session_id.clone(),
                cell_id,
                delegate: Arc::clone(&session.delegate),
            })
            .collect()
    }

    pub(super) fn drain(&mut self) -> Vec<FailedSession> {
        let sessions = std::mem::take(&mut self.records);
        sessions
            .into_iter()
            .map(|(session_id, session)| {
                let cells = session
                    .cells
                    .into_values()
                    .map(|cell_id| CellOwner {
                        session_id: session_id.clone(),
                        cell_id,
                        delegate: Arc::clone(&session.delegate),
                    })
                    .collect();
                FailedSession {
                    cleanup: session.cleanup,
                    cells,
                }
            })
            .collect()
    }
}

fn unknown_session(session_id: &SessionId) -> RuntimeString {
    RuntimeString::from(format!("unknown code-mode session {session_id}"))
}

// ---- driver vocabulary -------------------------------------------------------------

/// One logical session on a connection, tagged with the open attempt that
/// produced it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RemoteSession {
    pub(super) id: SessionId,
    pub(super) generation: u64,
}

pub(super) enum DriverCommand {
    OpenSession {
        session: RemoteSession,
        delegate: Arc<dyn CodeModeSessionDelegate>,
        cleanup: SessionCleanup,
        caller_cancellation: CancellationToken,
        response_tx: oneshot::Sender<Result<(), RuntimeString>>,
    },
    Execute {
        session: RemoteSession,
        request: ExecuteRequest,
        caller_cancellation: CancellationToken,
        response_tx: oneshot::Sender<Result<DeliveredExecute, RuntimeString>>,
    },
    Wait {
        session: RemoteSession,
        request: WaitRequest,
        caller_cancellation: CancellationToken,
        response_tx: oneshot::Sender<Result<WaitOutcome, RuntimeString>>,
    },
    Terminate {
        session: RemoteSession,
        cell_id: CellId,
        response_tx: oneshot::Sender<Result<WaitOutcome, RuntimeString>>,
    },
    ShutdownSession {
        session: RemoteSession,
        response_tx: oneshot::Sender<Result<(), RuntimeString>>,
    },
    ReplEval {
        session: RemoteSession,
        request: WireReplEvalRequest,
        caller_cancellation: CancellationToken,
        response_tx: oneshot::Sender<Result<WireReplOutcome, RuntimeString>>,
    },
    ReplTerminate {
        session: RemoteSession,
        exec_id: RuntimeString,
        response_tx: oneshot::Sender<Result<(), RuntimeString>>,
    },
}

pub(super) enum DriverEvent {
    HostMessage(HostToClient),
    DelegateCompleted {
        id: DelegateRequestId,
        result: Result<DelegateResponse, RuntimeString>,
    },
    RequestCancelled(RequestId),
    Failed(RuntimeString),
}

/// A request whose caller may vanish. Cancellation is reported ONCE, and the
/// watcher stops as soon as the request leaves the tracker.
pub(super) struct CancellableRequest {
    caller_cancellation: CancellationToken,
    watcher_stop: CancellationToken,
    reported: bool,
}

impl CancellableRequest {
    pub(super) fn new(caller_cancellation: CancellationToken) -> Self {
        Self {
            caller_cancellation,
            watcher_stop: CancellationToken::new(),
            reported: false,
        }
    }

    pub(super) fn is_cancelled(&self) -> bool {
        self.caller_cancellation.is_cancelled()
    }

    pub(super) fn mark_reported(&mut self) -> bool {
        if self.reported {
            return false;
        }
        self.reported = true;
        true
    }

    pub(super) fn spawn_watcher(&self, id: RequestId, event_tx: mpsc::Sender<DriverEvent>) {
        let caller_cancellation = self.caller_cancellation.clone();
        let watcher_stop = self.watcher_stop.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = caller_cancellation.cancelled() => {
                    let _ = event_tx.send(DriverEvent::RequestCancelled(id)).await;
                }
                _ = watcher_stop.cancelled() => {}
            }
        });
    }
}

impl Drop for CancellableRequest {
    fn drop(&mut self) {
        self.watcher_stop.cancel();
    }
}

/// The first frontier of a started cell, still owed to its caller.
pub(super) struct InitialResponse {
    pub(super) generation: u64,
    pub(super) cell_id: WireCellId,
    pub(super) response_tx: oneshot::Sender<Result<RuntimeResponse, RuntimeString>>,
}

pub(super) struct DeliveredExecute {
    pub(super) request_id: RequestId,
    pub(super) started: StartedCell,
}

/// A started cell whose caller has not yet acknowledged receipt. If the caller
/// disappears before claiming it, the cell is terminated rather than leaked.
pub(super) struct UnclaimedExecute {
    pub(super) session: RemoteSession,
    pub(super) cell_id: WireCellId,
    pub(super) cancellation: CancellableRequest,
}

pub(super) enum PendingRequest {
    OpenSession {
        session: RemoteSession,
        delegate: Arc<dyn CodeModeSessionDelegate>,
        cleanup: SessionCleanup,
        cancellation: CancellableRequest,
        response_tx: oneshot::Sender<Result<(), RuntimeString>>,
    },
    Execute {
        session: RemoteSession,
        response_tx: oneshot::Sender<Result<DeliveredExecute, RuntimeString>>,
        initial_response_tx: oneshot::Sender<Result<RuntimeResponse, RuntimeString>>,
        initial_response_rx: oneshot::Receiver<Result<RuntimeResponse, RuntimeString>>,
        cancellation: CancellableRequest,
    },
    Wait {
        session: RemoteSession,
        cell_id: WireCellId,
        cancellation: CancellableRequest,
        response_tx: oneshot::Sender<Result<WaitOutcome, RuntimeString>>,
    },
    Terminate {
        session: RemoteSession,
        cell_id: WireCellId,
        response_tx: oneshot::Sender<Result<WaitOutcome, RuntimeString>>,
    },
    ShutdownSession {
        session: RemoteSession,
        response_tx: oneshot::Sender<Result<(), RuntimeString>>,
    },
    ReplEval {
        session: RemoteSession,
        exec_id: WireCellId,
        cancellation: CancellableRequest,
        response_tx: oneshot::Sender<Result<WireReplOutcome, RuntimeString>>,
    },
    ReplTerminate {
        exec_id: RuntimeString,
        response_tx: oneshot::Sender<Result<(), RuntimeString>>,
    },
}

/// A wait held back because an earlier wait on the same cell was cancelled but
/// the host has not answered it yet: two concurrent waits on one cell would
/// race for the same frontier.
pub(super) struct DeferredWait {
    pub(super) session: RemoteSession,
    pub(super) request: WireWaitRequest,
    pub(super) caller_cancellation: CancellationToken,
    pub(super) response_tx: oneshot::Sender<Result<WaitOutcome, RuntimeString>>,
}

impl PendingRequest {
    pub(super) fn cancellation_mut(&mut self) -> Option<&mut CancellableRequest> {
        match self {
            Self::OpenSession { cancellation, .. }
            | Self::Execute { cancellation, .. }
            | Self::Wait { cancellation, .. }
            | Self::ReplEval { cancellation, .. } => Some(cancellation),
            Self::Terminate { .. } | Self::ShutdownSession { .. } | Self::ReplTerminate { .. } => {
                None
            }
        }
    }

    pub(super) fn fail(self, reason: RuntimeString) {
        match self {
            Self::OpenSession { response_tx, .. } | Self::ShutdownSession { response_tx, .. } => {
                let _ = response_tx.send(Err(reason));
            }
            Self::Execute { response_tx, .. } => {
                let _ = response_tx.send(Err(reason));
            }
            Self::Wait { response_tx, .. } | Self::Terminate { response_tx, .. } => {
                let _ = response_tx.send(Err(reason));
            }
            Self::ReplEval { response_tx, .. } => {
                let _ = response_tx.send(Err(reason));
            }
            Self::ReplTerminate { response_tx, .. } => {
                let _ = response_tx.send(Err(reason));
            }
        }
    }
}
