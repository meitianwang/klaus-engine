//! The host side of the protocol. Writes [`Op`]s to the output stream and reads
//! [`ProtocolEvent`]s from the input stream.
//!
//! A SINGLE internal reader consumes the input stream and fans each event out to
//! (a) every live [`ProtocolClient::events`] iterator and (b) any pending query
//! (`get_history`/`get_cost`/… and `run_to_completion`) awaiting its matching
//! reply. This lets a streamed turn and an awaited query coexist on the one
//! stream.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::task::{Context, Poll};

use futures::channel::mpsc as fmpsc;
use futures::{FutureExt, Stream, StreamExt};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::oneshot;

use klaus_agent_core::engine::message_queue::QueuedMessage;
use klaus_agent_core::engine::session::{Input, SessionContextSnapshot, SideQuestionResult};
use klaus_agent_core::observability::cost_tracker::CostSnapshot;
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeJson, RuntimeString};
use klaus_agent_core::schemas::message::Message;
use klaus_agent_core::store::session_store::{ListOptions, SubAgentTranscript, ThreadPage};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::permission::{ApprovalResponse, ExternalPermissionMode};

use crate::new_uuid;
use crate::types::{
    AgentMemoryMergeChoice, AgentMemorySnapshotDecisionAudit,
    AgentMemorySnapshotDecisionQueryResult, AgentMemorySnapshotUnavailableReason,
    ArchiveSessionResult, BackgroundAgentSummary, BackgroundProcessOutput,
    BackgroundProcessSnapshot, BackgroundProcessStopResult, BoundSession, CompactSessionResult,
    DeleteSessionResult, DreamNowCompletion, DreamNowOutcome, DreamNowReason, ElicitResponseWire,
    ExtensionRuntimeSnapshot, GenerateTitleOpResult, InitSessionResult, InputValidationErrorDetail,
    InstalledPluginMutationResult, InstalledPlugins, McpAuthorizationResolvedWire,
    McpAuthorizationResponseWire, McpAuthorizationRevokedWire, McpAuthorizationStatusWire,
    McpElicitationResolvedWire, McpElicitationResponse, McpRuntimeResult,
    MemoryMaintenanceQueryErrorDetail, MemoryMaintenanceSnapshot, Op, OpKind,
    PluginRuntimeSnapshot, ProtocolDynamicToolResolved, ProtocolDynamicToolResponse,
    ProtocolErrorDetail, ProtocolEvent, ProtocolMcpServerInit, ProtocolPluginCatalog,
    ProtocolPluginCatalogRoot, ProtocolSessionConfigUpdate, ProtocolSessionConfigUpdateResult,
    ProtocolSessionInit, QueueEdit, QueueMutationResult, QueueSnapshot, QueueSnapshotStatus,
    RenameThreadResult, ReviewSessionResult, ReviewTarget, RewindScope, RunOpOptions, RunResult,
    RuntimeCapabilityResolution, SetupLifecycleSnapshot, SetupRunRequest, SetupRunResult,
    ShutdownCompletion, SteerPriority, SteerResult, SubagentSnapshot, TaskListQueryOptions,
    TaskListSnapshot, UnarchiveSessionResult, UpdateThreadHostMetadataResult, UpdateTitleResult,
};

const CLIENT_SHUTDOWN_TIMEOUT_MS: u64 = 95_000;
const CLIENT_TRANSPORT_CLOSE_TIMEOUT_MS: u64 = 5_000;
const CLIENT_INCOMPLETE_FRAME_ERROR: &str =
    "protocol transport closed after an incomplete frame write";

#[derive(Clone, Copy)]
struct ClientShutdownTimeouts {
    completion: std::time::Duration,
    transport_close: std::time::Duration,
}

impl Default for ClientShutdownTimeouts {
    fn default() -> Self {
        Self {
            completion: std::time::Duration::from_millis(CLIENT_SHUTDOWN_TIMEOUT_MS),
            transport_close: std::time::Duration::from_millis(CLIENT_TRANSPORT_CLOSE_TIMEOUT_MS),
        }
    }
}

/// A query's failure: either the op reported a correlated error, or the stream
/// closed before the reply arrived.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ClientError {
    /// The op named by this query FAILED on the server (carries its message).
    #[error("{0}")]
    Op(String),
    /// A submission was rejected by the core input validator.
    #[error("{message}")]
    InvalidInput {
        message: String,
        detail: InputValidationErrorDetail,
    },
    /// The authoritative Memory/Dream lifecycle snapshot is unavailable after
    /// a terminal persistence inconsistency.
    #[error("{message}")]
    MemoryMaintenance {
        message: String,
        detail: MemoryMaintenanceQueryErrorDetail,
    },
    /// The input stream closed before the query's reply arrived.
    #[error("protocol stream closed")]
    Closed,
    /// An exact runtime string could not be projected into this scalar-only API.
    #[error("protocol string is not scalar UTF-8: {0}")]
    NonScalarString(String),
}

/// Options for constructing a [`ProtocolClient`].
pub struct ProtocolClientOptions {
    /// Receives newline-delimited JSON ops (e.g. a child process's stdin).
    pub output: Box<dyn AsyncWrite + Unpin + Send>,
    /// Source of newline-delimited JSON protocol events (e.g. a child's stdout).
    pub input: Box<dyn AsyncRead + Unpin + Send>,
}

/// A pending query awaiting a reply event that matches (submission id + field
/// present, or an error).
trait Waiter: Send {
    /// If `evt` answers this waiter, settle it and return true.
    fn try_settle(&mut self, evt: &ProtocolEvent) -> bool;
    /// The stream closed before a reply; reject.
    fn close(&mut self);
}

type EventExtractor<T> = Box<dyn Fn(&ProtocolEvent) -> Option<T> + Send>;

struct TypedWaiter<T> {
    id: RuntimeString,
    extract: EventExtractor<T>,
    tx: Option<oneshot::Sender<Result<T, ClientError>>>,
}

impl<T: Send> Waiter for TypedWaiter<T> {
    fn try_settle(&mut self, evt: &ProtocolEvent) -> bool {
        if evt.submission_id != self.id {
            return false;
        }
        // This op's reply carries either the expected field OR a
        // submission-correlated error.
        if evt.error.is_some() {
            if let Some(tx) = self.tx.take() {
                let error = protocol_event_error(evt)
                    .expect("a protocol event with error must project to ClientError");
                let _ = tx.send(Err(error));
            }
            return true;
        }
        if let Some(val) = (self.extract)(evt) {
            if let Some(tx) = self.tx.take() {
                let _ = tx.send(Ok(val));
            }
            return true;
        }
        false
    }
    fn close(&mut self) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(Err(ClientError::Closed));
        }
    }
}

/// A checked steer can remain pending until a future tool continuation consumes
/// it. A durability failure makes that continuation impossible, so this waiter
/// also observes the queue's global terminal snapshot instead of waiting only
/// for a submission-correlated reply that can never be produced.
struct SteerWaiter {
    id: RuntimeString,
    tx: Option<oneshot::Sender<Result<SteerResult, ClientError>>>,
}

impl Waiter for SteerWaiter {
    fn try_settle(&mut self, evt: &ProtocolEvent) -> bool {
        if let Some(snapshot) = &evt.queue_snapshot {
            if snapshot.status == QueueSnapshotStatus::PersistenceFailed {
                if let Some(tx) = self.tx.take() {
                    let error = snapshot
                        .error
                        .as_ref()
                        .map(runtime_op_error)
                        .unwrap_or_else(|| ClientError::Op("queue persistence failed".to_string()));
                    let _ = tx.send(Err(error));
                }
                return true;
            }
        }
        if evt.submission_id != self.id {
            return false;
        }
        if evt.error.is_some() {
            if let Some(tx) = self.tx.take() {
                let error = protocol_event_error(evt)
                    .expect("a protocol event with error must project to ClientError");
                let _ = tx.send(Err(error));
            }
            return true;
        }
        if let Some(result) = evt.steered.clone() {
            if let Some(tx) = self.tx.take() {
                let _ = tx.send(Ok(result));
            }
            return true;
        }
        false
    }

    fn close(&mut self) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(Err(ClientError::Closed));
        }
    }
}

fn runtime_op_error(error: &RuntimeString) -> ClientError {
    match error.try_to_utf8() {
        Ok(message) => ClientError::Op(message),
        Err(error) => ClientError::NonScalarString(error.to_string()),
    }
}

fn protocol_event_error(event: &ProtocolEvent) -> Option<ClientError> {
    let error = event.error.as_ref()?;
    Some(match error.try_to_utf8() {
        Ok(message) => match &event.error_detail {
            Some(ProtocolErrorDetail::InputValidation(detail)) => ClientError::InvalidInput {
                message,
                detail: detail.clone(),
            },
            Some(ProtocolErrorDetail::MemoryMaintenance(detail)) => {
                ClientError::MemoryMaintenance {
                    message,
                    detail: detail.clone(),
                }
            }
            Some(ProtocolErrorDetail::Unknown(_)) | None => ClientError::Op(message),
        },
        Err(error) => ClientError::NonScalarString(error.to_string()),
    })
}

/// The reader's shared fan-out state, guarded by one lock so registration and
/// end-of-stream never race.
struct ReaderState {
    ended: bool,
    subscribers: Vec<fmpsc::UnboundedSender<Arc<ProtocolEvent>>>,
    waiters: Vec<Box<dyn Waiter>>,
}

#[derive(Default)]
struct ReaderTaskSlot {
    closing: bool,
    handle: Option<tokio::task::AbortHandle>,
}

impl ReaderTaskSlot {
    fn publish(&mut self, handle: tokio::task::AbortHandle) {
        if self.closing {
            handle.abort();
        }
        self.handle = Some(handle);
    }

    fn close(&mut self) {
        self.closing = true;
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

struct ClientInner {
    output: tokio::sync::Mutex<Option<Box<dyn AsyncWrite + Unpin + Send>>>,
    /// Closes write admission independently of the output mutex. A hostile
    /// writer may hold that mutex forever, but it must not let later callers
    /// enqueue protocol frames after endpoint shutdown has started.
    transport_closing: AtomicBool,
    /// Once any prefix of a frame has reached the writer, cancelling that
    /// frame's future makes the byte stream unsafe to reuse. Retain one stable
    /// error so even the privileged shutdown owner cannot append another frame
    /// to the abandoned prefix.
    write_poison: StdMutex<Option<ClientError>>,
    input: StdMutex<Option<Box<dyn AsyncRead + Unpin + Send>>>,
    reader_task: StdMutex<ReaderTaskSlot>,
    shutdown_timeouts: ClientShutdownTimeouts,
    started: AtomicBool,
    state: StdMutex<ReaderState>,
    /// One wire shutdown submission is shared by every clone/caller. This makes
    /// duplicate or concurrent high-level shutdown requests idempotent even
    /// though the transport stops admitting new frames after the first one.
    shutdown_result: std::sync::OnceLock<
        tokio::sync::watch::Receiver<Option<Result<ShutdownCompletion, ClientError>>>,
    >,
}

impl ClientInner {
    fn write_poison(&self) -> Option<ClientError> {
        self.write_poison.lock().unwrap().clone()
    }

    fn poison_incomplete_frame(&self) -> ClientError {
        self.transport_closing.store(true, Ordering::Release);
        let mut poison = self.write_poison.lock().unwrap();
        poison
            .get_or_insert_with(|| ClientError::Op(CLIENT_INCOMPLETE_FRAME_ERROR.into()))
            .clone()
    }
}

/// Cancellation guard for one encoded protocol frame. `AsyncWrite::poll_write`
/// may accept only a prefix, so `write_all` alone cannot tell the shared writer
/// that its owning future disappeared between write polls.
struct ClientFrameWriteGuard {
    inner: Arc<ClientInner>,
    bytes_written: usize,
    settled: bool,
}

impl ClientFrameWriteGuard {
    fn new(inner: Arc<ClientInner>) -> Self {
        Self {
            inner,
            bytes_written: 0,
            settled: false,
        }
    }

    fn record_write(&mut self, bytes_written: usize) {
        self.bytes_written += bytes_written;
    }

    fn poison(&mut self) -> ClientError {
        self.settled = true;
        self.inner.poison_incomplete_frame()
    }

    fn complete(&mut self) {
        self.settled = true;
    }
}

impl Drop for ClientFrameWriteGuard {
    fn drop(&mut self) {
        if !self.settled && self.bytes_written > 0 {
            let _ = self.inner.poison_incomplete_frame();
        }
    }
}

/// The host side of the protocol. Cheaply cloneable (shares one reader).
#[derive(Clone)]
pub struct ProtocolClient {
    inner: Arc<ClientInner>,
}

#[derive(Debug)]
pub struct ProtocolDreamNowTask {
    pub ok: bool,
    pub reason: Option<DreamNowReason>,
    pub done: Option<oneshot::Receiver<Result<DreamNowCompletion, ClientError>>>,
}

impl ProtocolClient {
    pub fn new(opts: ProtocolClientOptions) -> Self {
        Self::new_with_shutdown_timeouts(opts, ClientShutdownTimeouts::default())
    }

    fn new_with_shutdown_timeouts(
        opts: ProtocolClientOptions,
        shutdown_timeouts: ClientShutdownTimeouts,
    ) -> Self {
        ProtocolClient {
            inner: Arc::new(ClientInner {
                output: tokio::sync::Mutex::new(Some(opts.output)),
                transport_closing: AtomicBool::new(false),
                write_poison: StdMutex::new(None),
                input: StdMutex::new(Some(opts.input)),
                reader_task: StdMutex::new(ReaderTaskSlot::default()),
                shutdown_timeouts,
                started: AtomicBool::new(false),
                state: StdMutex::new(ReaderState {
                    ended: false,
                    subscribers: Vec::new(),
                    waiters: Vec::new(),
                }),
                shutdown_result: std::sync::OnceLock::new(),
            }),
        }
    }

    /// Write a raw op as a single JSON line.
    pub async fn send(&self, op: &Op) {
        let _ = self.send_checked(op).await;
    }

    async fn send_checked(&self, op: &Op) -> Result<(), ClientError> {
        self.send_checked_with_admission(op, false).await
    }

    async fn send_checked_with_admission(
        &self,
        op: &Op,
        shutdown_owner: bool,
    ) -> Result<(), ClientError> {
        if let Some(error) = self.inner.write_poison() {
            return Err(error);
        }
        if !shutdown_owner && self.inner.transport_closing.load(Ordering::Acquire) {
            return Err(ClientError::Closed);
        }
        let mut line = runtime_codec::encode_bytes(op)
            .map_err(|error| ClientError::Op(format!("failed to encode protocol op: {error}")))?;
        line.push(b'\n');
        let mut output = self.inner.output.lock().await;
        if let Some(error) = self.inner.write_poison() {
            return Err(error);
        }
        if !shutdown_owner && self.inner.transport_closing.load(Ordering::Acquire) {
            return Err(ClientError::Closed);
        }
        let out = output.as_mut().ok_or(ClientError::Closed)?;
        let mut frame_write = ClientFrameWriteGuard::new(self.inner.clone());
        while frame_write.bytes_written < line.len() {
            let offset = frame_write.bytes_written;
            match out.write(&line[offset..]).await {
                Ok(0) => {
                    if frame_write.bytes_written > 0 {
                        return Err(frame_write.poison());
                    }
                    return Err(ClientError::Op(
                        "failed to write protocol op: writer returned zero bytes".into(),
                    ));
                }
                Ok(bytes_written) => frame_write.record_write(bytes_written),
                Err(error) => {
                    if frame_write.bytes_written > 0 {
                        return Err(frame_write.poison());
                    }
                    return Err(ClientError::Op(format!(
                        "failed to write protocol op: {error}"
                    )));
                }
            }
        }
        frame_write.complete();
        out.flush()
            .await
            .map_err(|error| ClientError::Op(format!("failed to flush protocol op: {error}")))
    }

    async fn close_transport(&self) {
        self.inner.transport_closing.store(true, Ordering::Release);
        self.inner.reader_task.lock().unwrap().close();
        let _ = self.inner.input.lock().unwrap().take();
        finish(&self.inner);
        // A concurrent caller may be stuck inside a hostile AsyncWrite while
        // holding the output mutex. Endpoint shutdown remains bounded even in
        // that case; the reader and every public waiter are already terminal.
        let _ = tokio::time::timeout(self.inner.shutdown_timeouts.transport_close, async {
            let _ = self.inner.output.lock().await.take();
        })
        .await;
    }

    /// Send a `run` op with a freshly generated submission id; returns that id
    /// (drive via [`events`](Self::events)).
    pub async fn run(&self, input: Input, options: Option<RunOpOptions>) -> String {
        let submission_id = new_uuid();
        self.send(&Op {
            id: submission_id.clone().into(),
            op: OpKind::Run { input, options },
        })
        .await;
        submission_id
    }

    /// Run one turn and resolve with its final outcome. Rejects if the stream
    /// closes first. Intermediate events still flow to any
    /// [`events`](Self::events) consumer.
    pub async fn run_to_completion(
        &self,
        input: Input,
        options: Option<RunOpOptions>,
    ) -> Result<RunResult, ClientError> {
        let op = Op {
            id: new_uuid().into(),
            op: OpKind::Run { input, options },
        };
        self.query(op, |e| e.result.clone()).await
    }

    /// Answer a parked approval request on the server.
    pub async fn approve(&self, request_id: String, response: ApprovalResponse) {
        self.send(&Op {
            id: new_uuid().into(),
            op: OpKind::Approve {
                request_id: request_id.into(),
                response,
            },
        })
        .await;
    }

    /// Abort the server's running turn.
    pub async fn interrupt(&self) {
        self.send(&Op {
            id: new_uuid().into(),
            op: OpKind::Interrupt {
                origin: None,
                expected_turn_id: None,
            },
        })
        .await;
    }

    /// Cancel one in-flight read-only helper without interrupting the main turn.
    pub async fn cancel_request(&self, request_id: impl Into<RuntimeString>) {
        self.send(&Op {
            id: new_uuid().into(),
            op: OpKind::CancelRequest {
                request_id: request_id.into(),
            },
        })
        .await;
    }

    /// Rewind the session by `num_turns` user turns. The server replies with a
    /// `rewound` protocol event carrying the outcome.
    pub async fn rewind(&self, num_turns: u32, scope: Option<RewindScope>) -> String {
        let id = new_uuid();
        self.send(&Op {
            id: id.clone().into(),
            op: OpKind::Rewind { num_turns, scope },
        })
        .await;
        id
    }

    /// What a rewind of `num_turns` would do, having done nothing. The server
    /// replies with a `rewind_preview` protocol event.
    pub async fn rewind_preview(&self, num_turns: u32) -> String {
        let id = new_uuid();
        self.send(&Op {
            id: id.clone().into(),
            op: OpKind::RewindPreview { num_turns },
        })
        .await;
        id
    }

    /// Inject a message into the running turn (mid-turn steering).
    pub async fn steer(&self, input: Input, priority: Option<SteerPriority>) {
        self.steer_with_delivery(input, priority, None).await
    }

    /// Inject a message into the running turn, telling the drain how the message
    /// must land in history once applied. See
    /// [`klaus_agent_core::engine::message_queue::SteerDelivery`].
    pub async fn steer_with_delivery(
        &self,
        input: Input,
        priority: Option<SteerPriority>,
        delivery: Option<klaus_agent_core::engine::message_queue::SteerDelivery>,
    ) {
        self.send(&Op {
            id: new_uuid().into(),
            op: OpKind::Steer {
                input,
                priority,
                delivery,
                display_metadata: None,
                expected_turn_id: None,
                queue_message_id: None,
            },
        })
        .await
    }

    /// Inject a message and wait until the engine attaches it to a real tool
    /// continuation, or returns a correlated validation error.
    pub async fn steer_to_completion(
        &self,
        input: Input,
        priority: Option<SteerPriority>,
    ) -> Result<SteerResult, ClientError> {
        self.steer_to_completion_with_hook(input, priority, || async {})
            .await
    }

    #[cfg(test)]
    pub(crate) async fn steer_to_completion_after_send<Fut>(
        &self,
        input: Input,
        priority: Option<SteerPriority>,
        after_send: Fut,
    ) -> Result<SteerResult, ClientError>
    where
        Fut: Future<Output = ()> + Send,
    {
        self.steer_to_completion_with_hook(input, priority, || after_send)
            .await
    }

    async fn steer_to_completion_with_hook<F, Fut>(
        &self,
        input: Input,
        priority: Option<SteerPriority>,
        after_send: F,
    ) -> Result<SteerResult, ClientError>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = ()> + Send,
    {
        let op = Op {
            id: new_uuid().into(),
            op: OpKind::Steer {
                input,
                priority,
                delivery: None,
                display_metadata: None,
                expected_turn_id: None,
                queue_message_id: None,
            },
        };
        self.ensure_reader();
        let (tx, rx) = oneshot::channel();
        {
            // Register before the op can reach the server. A fast failed flush
            // may publish the terminal queue snapshot before send_checked
            // returns to this task.
            let mut state = self.inner.state.lock().unwrap();
            if state.ended {
                return Err(ClientError::Closed);
            }
            state.waiters.push(Box::new(SteerWaiter {
                id: op.id.clone(),
                tx: Some(tx),
            }));
        }
        if let Err(error) = self.send_checked(&op).await {
            self.close_transport().await;
            return Err(error);
        }
        after_send().await;
        match rx.await {
            Ok(result) => result,
            Err(_) => Err(ClientError::Closed),
        }
    }

    /// Switch the model for subsequent turns.
    pub async fn set_model(&self, model: String) {
        self.send(&Op {
            id: new_uuid().into(),
            op: OpKind::SetModel {
                model: model.into(),
            },
        })
        .await;
    }

    /// Switch the permission mode.
    pub async fn set_permission_mode(&self, mode: ExternalPermissionMode) {
        self.send(&Op {
            id: new_uuid().into(),
            op: OpKind::SetPermissionMode { mode },
        })
        .await;
    }

    /// Answer a parked ask_user (elicitation) request on the server.
    pub async fn resolve_elicitation(&self, request_id: String, response: ElicitResponseWire) {
        self.send(&Op {
            id: new_uuid().into(),
            op: OpKind::ResolveElicitation {
                request_id: request_id.into(),
                response,
            },
        })
        .await;
    }

    pub async fn resolve_mcp_elicitation(
        &self,
        request_id: String,
        response: McpElicitationResponse,
    ) -> Result<McpElicitationResolvedWire, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::ResolveMcpElicitation {
                    request_id: request_id.into(),
                    response,
                },
            },
            |event| event.mcp_elicitation_resolved.clone(),
        )
        .await
    }

    /// Resolve one interactive MCP OAuth request without echoing the code back.
    pub async fn resolve_mcp_authorization(
        &self,
        request_id: String,
        response: McpAuthorizationResponseWire,
    ) -> Result<McpAuthorizationResolvedWire, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::ResolveMcpAuthorization {
                    request_id: request_id.into(),
                    response,
                },
            },
            |event| event.mcp_authorization_resolved.clone(),
        )
        .await
    }

    /// Read the core-owned credential status for one configured MCP server.
    pub async fn get_mcp_authorization_status(
        &self,
        server_name: String,
    ) -> Result<McpAuthorizationStatusWire, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::GetMcpAuthorizationStatus {
                    server_name: server_name.into(),
                },
            },
            |event| event.mcp_authorization_status.clone(),
        )
        .await
    }

    /// Revoke the persisted credential and close the live authenticated client.
    pub async fn revoke_mcp_authorization(
        &self,
        server_name: String,
    ) -> Result<McpAuthorizationRevokedWire, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::RevokeMcpAuthorization {
                    server_name: server_name.into(),
                },
            },
            |event| event.mcp_authorization_revoked.clone(),
        )
        .await
    }

    pub async fn get_mcp_runtime(&self) -> Result<McpRuntimeResult, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::GetMcpRuntime {
                    detail: None,
                    cursor: None,
                },
            },
            |event| event.mcp_runtime.clone(),
        )
        .await
    }

    pub async fn reconnect_mcp_server(
        &self,
        server_name: String,
    ) -> Result<McpRuntimeResult, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::ReconnectMcpServer {
                    server_name: server_name.into(),
                },
            },
            |event| event.mcp_runtime.clone(),
        )
        .await
    }

    pub async fn upsert_mcp_server(
        &self,
        server: ProtocolMcpServerInit,
    ) -> Result<McpRuntimeResult, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::UpsertMcpServer { server },
            },
            |event| event.mcp_runtime.clone(),
        )
        .await
    }

    pub async fn remove_mcp_server(
        &self,
        server_name: String,
    ) -> Result<McpRuntimeResult, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::RemoveMcpServer {
                    server_name: server_name.into(),
                },
            },
            |event| event.mcp_runtime.clone(),
        )
        .await
    }

    pub async fn reload_mcp_servers(
        &self,
        servers: Vec<ProtocolMcpServerInit>,
    ) -> Result<McpRuntimeResult, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::ReloadMcpServers { servers },
            },
            |event| event.mcp_runtime.clone(),
        )
        .await
    }

    pub async fn get_runtime_capabilities(
        &self,
    ) -> Result<RuntimeCapabilityResolution, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::GetRuntimeCapabilities,
            },
            |event| event.runtime_capabilities.clone(),
        )
        .await
    }

    /// Read the core-owned effective extension graph for the active Session.
    pub async fn get_extension_runtime(&self) -> Result<ExtensionRuntimeSnapshot, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::GetExtensionRuntime,
            },
            |event| event.extension_runtime.as_deref().cloned(),
        )
        .await
    }

    /// Read the persisted installed-plugin ledger owned by this endpoint.
    pub async fn list_installed_plugins(&self) -> Result<InstalledPlugins, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::ListInstalledPlugins,
            },
            |event| event.installed_plugins.as_deref().cloned(),
        )
        .await
    }

    /// Read the endpoint's effective startup plugin graph and load diagnostics.
    pub async fn get_plugin_runtime(&self) -> Result<PluginRuntimeSnapshot, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::GetPluginRuntime,
            },
            |event| event.plugin_runtime.as_deref().cloned(),
        )
        .await
    }

    /// Persist an installed-plugin enabled flag. The result states when it applies.
    pub async fn set_installed_plugin_enabled(
        &self,
        name: String,
        enabled: bool,
    ) -> Result<InstalledPluginMutationResult, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::SetInstalledPluginEnabled {
                    name: name.into(),
                    enabled,
                },
            },
            |event| event.plugin_mutation.as_deref().cloned(),
        )
        .await
    }

    /// Remove an installed-plugin record and optionally its confined files.
    pub async fn uninstall_plugin(
        &self,
        name: String,
        remove_files: bool,
    ) -> Result<InstalledPluginMutationResult, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::UninstallPlugin {
                    name: name.into(),
                    remove_files,
                },
            },
            |event| event.plugin_mutation.as_deref().cloned(),
        )
        .await
    }

    /// Materialize an already-resolved plugin package directory into the store and
    /// record it in the ledger. The caller owns fetching the package; the engine
    /// owns the store copy and the ledger write.
    pub async fn install_plugin(
        &self,
        marketplace: String,
        source_dir: String,
        version: Option<String>,
        expected_installed_version: Option<String>,
    ) -> Result<InstalledPluginMutationResult, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::InstallPlugin {
                    marketplace: marketplace.into(),
                    source_dir: source_dir.into(),
                    version: version.map(Into::into),
                    expected_installed_version: expected_installed_version.map(Into::into),
                },
            },
            |event| event.plugin_mutation.as_deref().cloned(),
        )
        .await
    }

    /// The packages readable under each marketplace root, joined with the ledger's
    /// install/enable state and each package's declared contributions.
    pub async fn list_plugin_catalog(
        &self,
        roots: Vec<ProtocolPluginCatalogRoot>,
    ) -> Result<ProtocolPluginCatalog, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::ListPluginCatalog { roots },
            },
            |event| event.plugin_catalog.as_deref().cloned(),
        )
        .await
    }

    /// Resolve a dynamic tool request emitted by the active turn.
    pub async fn resolve_dynamic_tool_call(
        &self,
        call_id: String,
        response: ProtocolDynamicToolResponse,
    ) -> Result<ProtocolDynamicToolResolved, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::ResolveDynamicToolCall {
                    call_id: call_id.into(),
                    response,
                },
            },
            |event| event.dynamic_tool_resolved.clone(),
        )
        .await
    }

    /// Update the only session settings that remain mutable after binding.
    pub async fn update_session_config(
        &self,
        update: ProtocolSessionConfigUpdate,
    ) -> Result<ProtocolSessionConfigUpdateResult, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::UpdateSessionConfig { update },
            },
            |event| event.session_config.clone(),
        )
        .await
    }

    /// Ask the server to shut down and wait for its final, correlated cleanup
    /// completion. Concurrent and repeated calls share one wire submission and
    /// return the same retained result.
    pub async fn shutdown(&self) -> Result<ShutdownCompletion, ClientError> {
        let mut completion = self
            .inner
            .shutdown_result
            .get_or_init(|| {
                // Close ordinary write admission before the retained shutdown
                // owner competes for the output mutex. A send admitted earlier
                // either finishes ahead of the shutdown frame or observes this
                // flag at the mutex boundary and emits nothing; no clone can
                // append a frame once shutdown starts.
                self.inner.transport_closing.store(true, Ordering::Release);
                let (send, receive) = tokio::sync::watch::channel(None);
                let client = self.clone();
                // The client, not the first high-level caller, owns the one wire
                // query. Cancelling that caller therefore cannot reset the
                // idempotency cell and submit a duplicate shutdown operation.
                tokio::spawn(async move {
                    let result = match tokio::time::timeout(
                        client.inner.shutdown_timeouts.completion,
                        std::panic::AssertUnwindSafe(client.query_shutdown(
                            Op {
                                id: new_uuid().into(),
                                op: OpKind::Shutdown,
                            },
                            |event| event.shutdown_completed.as_deref().cloned(),
                        ))
                        .catch_unwind(),
                    )
                    .await
                    {
                        Ok(Ok(result)) => result,
                        Ok(Err(_)) => {
                            Err(ClientError::Op("protocol shutdown task panicked".into()))
                        }
                        Err(_) => Err(ClientError::Op(format!(
                            "protocol shutdown timed out after {}ms",
                            client.inner.shutdown_timeouts.completion.as_millis()
                        ))),
                    };
                    // A shutdown ACK is the final protocol event. Close the
                    // local endpoint for both success and failure so callers do
                    // not depend on a cooperative peer to deliver EOF.
                    client.close_transport().await;
                    let _ = send.send(Some(result));
                });
                receive
            })
            .clone();
        loop {
            if let Some(result) = completion.borrow().clone() {
                return result;
            }
            if completion.changed().await.is_err() {
                return Err(ClientError::Op(
                    "protocol shutdown task ended without a completion".into(),
                ));
            }
        }
    }

    /// The conversation so far.
    pub async fn get_history(&self) -> Result<Vec<Message>, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::GetHistory,
            },
            |e| e.history.clone(),
        )
        .await
    }

    /// What each of this session's turns changed on disk, per file.
    pub async fn get_file_changes(
        &self,
    ) -> Result<Vec<crate::types::ProtocolSessionTurnFileChanges>, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::GetFileChanges,
            },
            |e| e.file_changes.clone(),
        )
        .await
    }

    /// Token/USD cost snapshot.
    pub async fn get_cost(&self) -> Result<CostSnapshot, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::GetCost,
            },
            |e| e.cost.clone(),
        )
        .await
    }

    /// UI-facing context-window and usage snapshot.
    pub async fn context_snapshot(&self) -> Result<SessionContextSnapshot, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::ContextSnapshot,
            },
            |e| e.context.clone(),
        )
        .await
    }

    /// Background sub-agents' progress snapshot.
    pub async fn get_subagents(&self) -> Result<Vec<BackgroundAgentSummary>, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::GetSubagents,
            },
            |e| e.subagents.clone(),
        )
        .await
    }

    /// Complete core-owned lifecycle snapshot for every sub-agent kind.
    pub async fn get_subagent_lifecycle(&self) -> Result<SubagentSnapshot, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::GetSubagentLifecycle,
            },
            |e| e.subagent_lifecycle.clone(),
        )
        .await
    }

    /// Authoritative background-process lifecycle snapshot for this Session.
    pub async fn get_background_processes(&self) -> Result<BackgroundProcessSnapshot, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::GetBackgroundProcesses,
            },
            |event| event.background_processes.clone(),
        )
        .await
    }

    /// Read retained output without advancing the model-facing `bash_output` cursor.
    pub async fn read_background_process_output(
        &self,
        shell_id: impl Into<RuntimeString>,
        after_offset: Option<u64>,
        max_bytes: Option<usize>,
    ) -> Result<BackgroundProcessOutput, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::ReadBackgroundProcessOutput {
                    shell_id: shell_id.into(),
                    after_offset,
                    max_bytes,
                },
            },
            |event| event.background_process_output.clone(),
        )
        .await
    }

    /// Request that a Session-owned background process stop.
    pub async fn stop_background_process(
        &self,
        shell_id: impl Into<RuntimeString>,
        signal: Option<RuntimeString>,
    ) -> Result<BackgroundProcessStopResult, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::StopBackgroundProcess {
                    shell_id: shell_id.into(),
                    signal,
                },
            },
            |event| event.background_process_stop.clone(),
        )
        .await
    }

    /// Reconnect-safe automatic Memory extraction/Dream lifecycle audit state.
    pub async fn get_memory_maintenance(&self) -> Result<MemoryMaintenanceSnapshot, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::GetMemoryMaintenance,
            },
            |event| event.memory_maintenance.clone(),
        )
        .await
    }

    /// Load a persisted sub-agent transcript by parent thread id and agent id.
    pub async fn load_subagent_transcript(
        &self,
        thread_id: String,
        agent_id: String,
    ) -> Result<SubAgentTranscript, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::LoadSubagentTranscript {
                    thread_id: thread_id.into(),
                    agent_id: agent_id.into(),
                },
            },
            |e| e.subagent_transcript.clone(),
        )
        .await
    }

    /// The steering queue (read-only snapshot).
    pub async fn get_queued(&self) -> Result<Vec<QueuedMessage>, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::GetQueued,
            },
            |e| e.queued.clone(),
        )
        .await
    }

    /// Remove + return the queued main-thread steering messages.
    pub async fn take_pending(&self) -> Result<Vec<QueuedMessage>, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::TakePending,
            },
            |e| e.pending.clone(),
        )
        .await
    }

    /// Reconnect-safe authoritative queue state. Live revisions are also emitted
    /// as unsolicited `queueSnapshot` events on [`events`](Self::events).
    pub async fn get_queue_snapshot(&self) -> Result<QueueSnapshot, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::GetQueueSnapshot,
            },
            |event| event.queue_snapshot.clone(),
        )
        .await
    }

    pub async fn edit_queued(
        &self,
        mutation_id: impl Into<RuntimeString>,
        uuid: impl Into<RuntimeString>,
        expected_revision: u64,
        edit: QueueEdit,
    ) -> Result<QueueMutationResult, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::EditQueued {
                    mutation_id: mutation_id.into(),
                    uuid: uuid.into(),
                    expected_revision,
                    edit,
                },
            },
            |event| event.queue_mutation.clone(),
        )
        .await
    }

    pub async fn remove_queued(
        &self,
        mutation_id: impl Into<RuntimeString>,
        uuid: impl Into<RuntimeString>,
        expected_revision: u64,
    ) -> Result<QueueMutationResult, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::RemoveQueued {
                    mutation_id: mutation_id.into(),
                    uuid: uuid.into(),
                    expected_revision,
                },
            },
            |event| event.queue_mutation.clone(),
        )
        .await
    }

    pub async fn reorder_queued(
        &self,
        mutation_id: impl Into<RuntimeString>,
        uuid: impl Into<RuntimeString>,
        before_uuid: Option<RuntimeString>,
        expected_revision: u64,
    ) -> Result<QueueMutationResult, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::ReorderQueued {
                    mutation_id: mutation_id.into(),
                    uuid: uuid.into(),
                    before_uuid,
                    expected_revision,
                },
            },
            |event| event.queue_mutation.clone(),
        )
        .await
    }

    /// Run one correlated init/maintenance Setup pass. The request id is the
    /// idempotency key retained by the core lifecycle ledger.
    pub async fn run_setup(&self, request: SetupRunRequest) -> Result<SetupRunResult, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::RunSetup { request },
            },
            |event| event.setup.clone(),
        )
        .await
    }

    /// Reconnect-safe Setup lifecycle audit state.
    pub async fn get_setup_lifecycle(&self) -> Result<SetupLifecycleSnapshot, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::GetSetupLifecycle,
            },
            |event| event.setup_lifecycle.clone(),
        )
        .await
    }

    /// Authoritative persisted TodoV2 page for the bound session.
    pub async fn get_task_list(
        &self,
        options: Option<TaskListQueryOptions>,
    ) -> Result<TaskListSnapshot, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::GetTaskList { options },
            },
            |event| event.task_list.clone(),
        )
        .await
    }

    /// Resolve and apply one Agent Memory snapshot request exactly once.
    pub async fn resolve_agent_memory_snapshot_decision(
        &self,
        request_id: impl Into<RuntimeString>,
        decision: AgentMemoryMergeChoice,
    ) -> Result<AgentMemorySnapshotDecisionAudit, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::ResolveAgentMemorySnapshotDecision {
                    request_id: request_id.into(),
                    decision,
                },
            },
            |event| event.agent_memory_snapshot_decision.clone(),
        )
        .await
    }

    /// Explicitly settle a request when the Host cannot provide a choice. The
    /// Session's configured policy decides whether this remains terminal or
    /// applies a fallback decision.
    pub async fn settle_agent_memory_snapshot_unavailable(
        &self,
        request_id: impl Into<RuntimeString>,
        reason: AgentMemorySnapshotUnavailableReason,
        error: Option<RuntimeString>,
    ) -> Result<AgentMemorySnapshotDecisionAudit, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::SettleAgentMemorySnapshotUnavailable {
                    request_id: request_id.into(),
                    reason,
                    error,
                },
            },
            |event| event.agent_memory_snapshot_decision.clone(),
        )
        .await
    }

    /// Query one authoritative audit without turning a missing request into a
    /// transport error.
    pub async fn get_agent_memory_snapshot_decision(
        &self,
        request_id: impl Into<RuntimeString>,
    ) -> Result<AgentMemorySnapshotDecisionQueryResult, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::GetAgentMemorySnapshotDecision {
                    request_id: request_id.into(),
                },
            },
            |event| event.agent_memory_snapshot_decision_query.clone(),
        )
        .await
    }

    /// Reconnect-safe snapshot of every Agent Memory decision audit.
    pub async fn get_agent_memory_snapshot_decisions(
        &self,
    ) -> Result<Vec<AgentMemorySnapshotDecisionAudit>, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::GetAgentMemorySnapshotDecisions,
            },
            |event| event.agent_memory_snapshot_decisions.clone(),
        )
        .await
    }

    /// Trigger manual compaction on the bound session.
    pub async fn compact_session(
        &self,
        instructions: Option<RuntimeString>,
    ) -> Result<CompactSessionResult, ClientError> {
        self.compact_session_with_selection(instructions, None)
            .await
    }

    /// Trigger selected or whole-history manual compaction on the bound session.
    pub async fn compact_session_with_selection(
        &self,
        instructions: Option<RuntimeString>,
        selection: Option<crate::types::CompactSelection>,
    ) -> Result<CompactSessionResult, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::CompactSession {
                    instructions,
                    selection,
                },
            },
            |e| e.compact.clone(),
        )
        .await
    }

    /// Ask a read-only side question against the bound session's history.
    pub async fn side_question(&self, question: String) -> Result<SideQuestionResult, ClientError> {
        self.side_question_input(Input::Text(question.into())).await
    }

    pub async fn side_question_input(
        &self,
        question: Input,
    ) -> Result<SideQuestionResult, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::SideQuestion { question },
            },
            |e| e.side_question.clone(),
        )
        .await
    }

    /// Ask a side question that can be cancelled independently of the main turn.
    pub async fn side_question_with_signal(
        &self,
        question: String,
        signal: AbortSignal,
    ) -> Result<SideQuestionResult, ClientError> {
        self.side_question_input_with_signal(Input::Text(question.into()), signal)
            .await
    }

    pub async fn side_question_input_with_signal(
        &self,
        question: Input,
        signal: AbortSignal,
    ) -> Result<SideQuestionResult, ClientError> {
        let id = new_uuid();
        self.query_with_signal(
            Op {
                id: id.into(),
                op: OpKind::SideQuestion { question },
            },
            |event| event.side_question.clone(),
            signal,
            "side question cancelled",
        )
        .await
    }

    /// Run an isolated review task on the bound session.
    pub async fn review_session(
        &self,
        target: ReviewTarget,
        user_facing_hint: Option<RuntimeString>,
    ) -> Result<ReviewSessionResult, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::ReviewSession {
                    target,
                    user_facing_hint,
                },
            },
            |e| e.review.clone(),
        )
        .await
    }

    /// Run an isolated review task that can be cancelled independently of the
    /// main turn.
    pub async fn review_session_with_signal(
        &self,
        target: ReviewTarget,
        user_facing_hint: Option<RuntimeString>,
        signal: AbortSignal,
    ) -> Result<ReviewSessionResult, ClientError> {
        let id = new_uuid();
        self.query_with_signal(
            Op {
                id: id.into(),
                op: OpKind::ReviewSession {
                    target,
                    user_facing_hint,
                },
            },
            |event| event.review.clone(),
            signal,
            "review cancelled",
        )
        .await
    }

    /// Trigger manual memory consolidation on the bound session.
    pub async fn dream_now(&self) -> Result<DreamNowOutcome, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::DreamNow,
            },
            |e| e.dream,
        )
        .await
    }

    /// Trigger manual memory consolidation and retain its terminal completion.
    /// The event subscription is installed before the op is sent, so an
    /// immediately-completing fork cannot race past the caller.
    pub async fn dream_now_with_completion(&self) -> Result<ProtocolDreamNowTask, ClientError> {
        let mut events = self.events();
        let op = Op {
            id: new_uuid().into(),
            op: OpKind::DreamNow,
        };
        let submission_id = op.id.clone();
        let start = self.query(op, |event| event.dream).await?;
        if !start.ok {
            return Ok(ProtocolDreamNowTask {
                ok: false,
                reason: start.reason,
                done: None,
            });
        }
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            while let Some(event) = events.next().await {
                if event.submission_id != submission_id {
                    continue;
                }
                if let Some(error) = &event.error {
                    let error = match error.try_to_utf8() {
                        Ok(message) => ClientError::Op(message),
                        Err(error) => ClientError::NonScalarString(error.to_string()),
                    };
                    let _ = tx.send(Err(error));
                    return;
                }
                if let Some(completion) = &event.dream_completed {
                    let _ = tx.send(Ok(completion.clone()));
                    return;
                }
            }
            let _ = tx.send(Err(ClientError::Closed));
        });
        Ok(ProtocolDreamNowTask {
            ok: true,
            reason: None,
            done: Some(rx),
        })
    }

    /// Generate a session title from the bound session's history.
    pub async fn generate_title(
        &self,
        instructions: Option<String>,
        prompt: Option<String>,
        persist: Option<bool>,
    ) -> Result<GenerateTitleOpResult, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::GenerateTitle {
                    instructions: instructions.map(Into::into),
                    prompt: prompt.map(Into::into),
                    persist,
                },
            },
            |e| e.generated_title.clone(),
        )
        .await
    }

    /// Generate a title that can be cancelled independently of the main turn.
    pub async fn generate_title_with_signal(
        &self,
        instructions: Option<String>,
        prompt: Option<String>,
        persist: Option<bool>,
        signal: AbortSignal,
    ) -> Result<GenerateTitleOpResult, ClientError> {
        let id = new_uuid();
        self.query_with_signal(
            Op {
                id: id.into(),
                op: OpKind::GenerateTitle {
                    instructions: instructions.map(Into::into),
                    prompt: prompt.map(Into::into),
                    persist,
                },
            },
            |event| event.generated_title.clone(),
            signal,
            "title generation cancelled",
        )
        .await
    }

    /// List persisted sessions.
    pub async fn list_sessions(
        &self,
        options: Option<ListOptions>,
    ) -> Result<ThreadPage, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::ListSessions { options },
            },
            |e| e.sessions.clone(),
        )
        .await
    }

    pub async fn create_side_task(
        &self,
        thread_id: String,
        new_thread_id: Option<String>,
    ) -> Result<BoundSession, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::CreateSideTask {
                    thread_id: thread_id.into(),
                    new_thread_id: new_thread_id.map(Into::into),
                },
            },
            |event| event.session.clone(),
        )
        .await
    }

    /// Bind this protocol endpoint to a persisted session before the first run.
    pub async fn resume_session(&self, thread_id: String) -> Result<BoundSession, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::ResumeSession {
                    thread_id: thread_id.into(),
                },
            },
            |e| e.session.clone(),
        )
        .await
    }

    /// Bind this protocol endpoint to a newly persisted fork before the first run.
    pub async fn fork_session(
        &self,
        thread_id: String,
        new_thread_id: Option<String>,
    ) -> Result<BoundSession, ClientError> {
        self.fork_session_before(thread_id, new_thread_id, None)
            .await
    }

    /// Fork like [`Self::fork_session`], cutting the inherited history right
    /// BEFORE the message carrying `before_message_uuid` (source unchanged).
    pub async fn fork_session_before(
        &self,
        thread_id: String,
        new_thread_id: Option<String>,
        before_message_uuid: Option<String>,
    ) -> Result<BoundSession, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::ForkSession {
                    thread_id: thread_id.into(),
                    new_thread_id: new_thread_id.map(Into::into),
                    before_message_uuid: before_message_uuid.map(Into::into),
                    inherit_goal: None,
                },
            },
            |e| e.session.clone(),
        )
        .await
    }

    /// Re-key a persisted, inactive thread onto a new id in the session store.
    pub async fn rename_thread(
        &self,
        thread_id: String,
        new_thread_id: String,
    ) -> Result<RenameThreadResult, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::RenameThread {
                    thread_id: thread_id.into(),
                    new_thread_id: new_thread_id.into(),
                },
            },
            |e| e.renamed.clone(),
        )
        .await
    }

    /// Replace a persisted thread's opaque host-owned metadata attachment
    /// (`None` removes it). The engine stores the value verbatim.
    pub async fn update_thread_host_metadata(
        &self,
        thread_id: String,
        host_metadata: Option<RuntimeJson>,
    ) -> Result<UpdateThreadHostMetadataResult, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::UpdateThreadHostMetadata {
                    thread_id: thread_id.into(),
                    host_metadata,
                },
            },
            |e| e.host_metadata_updated.clone(),
        )
        .await
    }

    /// Move a persisted session to the archive root.
    pub async fn archive_session(
        &self,
        thread_id: String,
    ) -> Result<ArchiveSessionResult, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::ArchiveSession {
                    thread_id: thread_id.into(),
                },
            },
            |e| e.archived.clone(),
        )
        .await
    }

    /// Idempotently restore an archived session to the active namespace.
    pub async fn unarchive_session(
        &self,
        thread_id: String,
    ) -> Result<UnarchiveSessionResult, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::UnarchiveSession {
                    thread_id: thread_id.into(),
                },
            },
            |event| event.unarchived.clone(),
        )
        .await
    }

    /// Delete a persisted session's rollout data.
    pub async fn delete_session(
        &self,
        thread_id: String,
    ) -> Result<DeleteSessionResult, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::DeleteSession {
                    thread_id: thread_id.into(),
                },
            },
            |e| e.deleted.clone(),
        )
        .await
    }

    /// Update Rust-owned persisted session title metadata.
    pub async fn update_title(
        &self,
        thread_id: String,
        title: String,
    ) -> Result<UpdateTitleResult, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::UpdateTitle {
                    thread_id: thread_id.into(),
                    title: title.into(),
                },
            },
            |e| e.title.clone(),
        )
        .await
    }

    /// Yield protocol events as they arrive. Ends when the input stream closes.
    pub fn events(&self) -> EventStream {
        self.ensure_reader();
        let (tx, rx) = fmpsc::unbounded();
        {
            let mut st = self.inner.state.lock().unwrap();
            if !st.ended {
                st.subscribers.push(tx);
            }
            // If already ended, `tx` drops here and `rx` ends immediately.
        }
        EventStream { rx }
    }

    /// Send an op and resolve with its reply event's field value (rejects if the
    /// stream closes).
    async fn query<T, F>(&self, op: Op, extract: F) -> Result<T, ClientError>
    where
        T: Send + 'static,
        F: Fn(&ProtocolEvent) -> Option<T> + Send + 'static,
    {
        self.ensure_reader();
        let (tx, rx) = oneshot::channel::<Result<T, ClientError>>();
        {
            let mut st = self.inner.state.lock().unwrap();
            if st.ended {
                return Err(ClientError::Closed);
            }
            st.waiters.push(Box::new(TypedWaiter {
                id: op.id.clone(),
                extract: Box::new(extract),
                tx: Some(tx),
            }));
        }
        if let Err(error) = self.send_checked(&op).await {
            self.close_transport().await;
            return Err(error);
        }
        match rx.await {
            Ok(res) => res,
            Err(_) => Err(ClientError::Closed),
        }
    }

    /// The one retained shutdown owner is the only writer allowed after normal
    /// admission closes. This keeps waiter registration and error propagation
    /// identical to an ordinary correlated query without reopening the endpoint.
    async fn query_shutdown<T, F>(&self, op: Op, extract: F) -> Result<T, ClientError>
    where
        T: Send + 'static,
        F: Fn(&ProtocolEvent) -> Option<T> + Send + 'static,
    {
        debug_assert!(matches!(op.op, OpKind::Shutdown));
        self.ensure_reader();
        let (tx, rx) = oneshot::channel::<Result<T, ClientError>>();
        {
            let mut state = self.inner.state.lock().unwrap();
            if state.ended {
                return Err(ClientError::Closed);
            }
            state.waiters.push(Box::new(TypedWaiter {
                id: op.id.clone(),
                extract: Box::new(extract),
                tx: Some(tx),
            }));
        }
        self.send_checked_with_admission(&op, true).await?;
        match rx.await {
            Ok(result) => result,
            Err(_) => Err(ClientError::Closed),
        }
    }

    async fn query_with_signal<T, F>(
        &self,
        op: Op,
        extract: F,
        signal: AbortSignal,
        cancelled_error: &'static str,
    ) -> Result<T, ClientError>
    where
        T: Send + 'static,
        F: Fn(&ProtocolEvent) -> Option<T> + Send + 'static,
    {
        if signal.is_cancelled() {
            return Err(ClientError::Op(cancelled_error.to_string()));
        }
        self.ensure_reader();
        let (tx, rx) = oneshot::channel::<Result<T, ClientError>>();
        {
            let mut state = self.inner.state.lock().unwrap();
            if state.ended {
                return Err(ClientError::Closed);
            }
            state.waiters.push(Box::new(TypedWaiter {
                id: op.id.clone(),
                extract: Box::new(extract),
                tx: Some(tx),
            }));
        }
        let request_id = op.id.clone();
        self.send(&op).await;
        let mut rx = rx;
        tokio::select! {
            biased;
            result = &mut rx => match result {
                Ok(result) => result,
                Err(_) => Err(ClientError::Closed),
            },
            _ = signal.cancelled() => {
                self.cancel_request(request_id).await;
                match rx.await {
                    Ok(result) => result,
                    Err(_) => Err(ClientError::Closed),
                }
            }
        }
    }

    /// Start the single input reader once; fan out each event to subscribers +
    /// waiters.
    fn ensure_reader(&self) {
        if self.inner.started.swap(true, Ordering::SeqCst) {
            return;
        }
        let input = self.inner.input.lock().unwrap().take();
        let Some(input) = input else {
            return;
        };
        let inner = self.inner.clone();
        let task = tokio::spawn(async move {
            let mut lines = BufReader::new(input).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let evt: ProtocolEvent =
                    match runtime_codec::decode_text(&RuntimeString::from_utf8(trimmed)) {
                        Ok(e) => e,
                        Err(_) => continue,
                    };
                let evt = Arc::new(evt);
                let mut st = inner.state.lock().unwrap();
                st.subscribers
                    .retain(|s| s.unbounded_send(evt.clone()).is_ok());
                // Settle any query waiting on this reply (iterate
                // backwards so removal is safe).
                let mut i = st.waiters.len();
                while i > 0 {
                    i -= 1;
                    if st.waiters[i].try_settle(&evt) {
                        st.waiters.remove(i);
                    }
                }
            }
            finish(&inner);
        });
        self.inner
            .reader_task
            .lock()
            .unwrap()
            .publish(task.abort_handle());
    }

    /// Configure the session before the first `run`/`resume_session`/`fork_session`.
    pub async fn init_session(
        &self,
        init: ProtocolSessionInit,
    ) -> Result<InitSessionResult, ClientError> {
        self.query(
            Op {
                id: new_uuid().into(),
                op: OpKind::InitSession { init },
            },
            |e| e.initialized.clone(),
        )
        .await
    }
}

/// End-of-stream: end every events() iterator and reject every pending query.
fn finish(inner: &Arc<ClientInner>) {
    let waiters = {
        let mut st = inner.state.lock().unwrap();
        if st.ended {
            return;
        }
        st.ended = true;
        st.subscribers.clear(); // drop senders → receivers end
        std::mem::take(&mut st.waiters)
    };
    for mut w in waiters {
        w.close();
    }
}

/// The async stream returned by [`ProtocolClient::events`].
pub struct EventStream {
    rx: fmpsc::UnboundedReceiver<Arc<ProtocolEvent>>,
}

impl Stream for EventStream {
    type Item = Arc<ProtocolEvent>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.rx).poll_next(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use tokio::io::AsyncWriteExt;

    const TEST_SHUTDOWN_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(50);
    const TEST_CLOSE_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(25);

    #[test]
    fn protocol_event_error_preserves_maintenance_type_and_degrades_unknown_detail() {
        let typed = ProtocolEvent {
            submission_id: "maintenance-query".into(),
            error: Some("maintenance state unavailable".into()),
            error_detail: Some(ProtocolErrorDetail::MemoryMaintenance(
                MemoryMaintenanceQueryErrorDetail {
                    code: crate::types::MemoryMaintenanceQueryErrorCode::PersistenceInconsistent,
                    run_kind: Some(crate::types::MemoryMaintenanceRunKind::AutoMemory),
                    run_id: Some("memory-run".into()),
                },
            )),
            ..Default::default()
        };
        match protocol_event_error(&typed).expect("typed event error") {
            ClientError::MemoryMaintenance { message, detail } => {
                assert_eq!(message, "maintenance state unavailable");
                assert_eq!(
                    detail.code,
                    crate::types::MemoryMaintenanceQueryErrorCode::PersistenceInconsistent
                );
                assert_eq!(
                    detail.run_kind,
                    Some(crate::types::MemoryMaintenanceRunKind::AutoMemory)
                );
                assert_eq!(detail.run_id, Some("memory-run".into()));
            }
            other => panic!("expected typed maintenance error, got {other:?}"),
        }

        let unknown = ProtocolEvent {
            submission_id: "future-error".into(),
            error: Some("future typed error".into()),
            error_detail: Some(ProtocolErrorDetail::Unknown(
                klaus_agent_core::runtime_values::RuntimeJson::Bool(true),
            )),
            ..Default::default()
        };
        assert!(matches!(
            protocol_event_error(&unknown),
            Some(ClientError::Op(message)) if message == "future typed error"
        ));
    }

    #[tokio::test]
    async fn typed_waiter_prioritizes_maintenance_error_over_a_conflicting_success_payload() {
        let event = ProtocolEvent {
            submission_id: "maintenance-query".into(),
            error: Some("maintenance state unavailable".into()),
            error_detail: Some(ProtocolErrorDetail::MemoryMaintenance(
                MemoryMaintenanceQueryErrorDetail {
                    code: crate::types::MemoryMaintenanceQueryErrorCode::PersistenceInconsistent,
                    run_kind: Some(crate::types::MemoryMaintenanceRunKind::AutoMemory),
                    run_id: Some("memory-run".into()),
                },
            )),
            memory_maintenance: Some(MemoryMaintenanceSnapshot::default()),
            ..Default::default()
        };
        let (send, receive) = oneshot::channel();
        let mut waiter = TypedWaiter {
            id: "maintenance-query".into(),
            extract: Box::new(|event| event.memory_maintenance.clone()),
            tx: Some(send),
        };

        assert!(waiter.try_settle(&event));
        match receive.await.expect("waiter sends its verdict") {
            Err(ClientError::MemoryMaintenance { detail, .. }) => {
                assert_eq!(
                    detail.code,
                    crate::types::MemoryMaintenanceQueryErrorCode::PersistenceInconsistent
                );
            }
            other => panic!("error must win over a conflicting success payload: {other:?}"),
        }
    }

    fn client_with_short_shutdown(
        input: Box<dyn AsyncRead + Unpin + Send>,
        output: Box<dyn AsyncWrite + Unpin + Send>,
    ) -> ProtocolClient {
        ProtocolClient::new_with_shutdown_timeouts(
            ProtocolClientOptions { input, output },
            ClientShutdownTimeouts {
                completion: TEST_SHUTDOWN_TIMEOUT,
                transport_close: TEST_CLOSE_TIMEOUT,
            },
        )
    }

    async fn wait_until_true(flag: &AtomicBool) {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !flag.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("test I/O future was never polled");
    }

    fn shutdown_error(result: Result<ShutdownCompletion, ClientError>) -> String {
        result
            .expect_err("shutdown was expected to fail")
            .to_string()
    }

    struct PendingReader;

    impl AsyncRead for PendingReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Pending
        }
    }

    struct ImmediateErrorWriter {
        writes: Arc<AtomicUsize>,
    }

    impl AsyncWrite for ImmediateErrorWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "immediate write failure",
            )))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    struct PendingWriter {
        polled: Arc<AtomicBool>,
        dropped: Arc<AtomicBool>,
        writes: Arc<AtomicUsize>,
    }

    impl Drop for PendingWriter {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    impl AsyncWrite for PendingWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            self.polled.store(true, Ordering::SeqCst);
            Poll::Pending
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Pending
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Pending
        }
    }

    struct PartialThenPendingWriter {
        blocked: Arc<AtomicBool>,
        bytes: Arc<StdMutex<Vec<u8>>>,
        polls: Arc<AtomicUsize>,
        prefix_len: usize,
        wrote_prefix: bool,
    }

    impl AsyncWrite for PartialThenPendingWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            if !self.wrote_prefix {
                let accepted = self.prefix_len.min(buf.len());
                assert!(accepted > 0, "test writer needs a non-empty frame");
                self.bytes
                    .lock()
                    .unwrap()
                    .extend_from_slice(&buf[..accepted]);
                self.wrote_prefix = true;
                return Poll::Ready(Ok(accepted));
            }
            self.blocked.store(true, Ordering::SeqCst);
            Poll::Pending
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    struct PendingOnceThenRecordingWriter {
        bytes: Arc<StdMutex<Vec<u8>>>,
        first_poll_pending: bool,
        pending_observed: Arc<AtomicBool>,
        polls: Arc<AtomicUsize>,
    }

    impl AsyncWrite for PendingOnceThenRecordingWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            if !self.first_poll_pending {
                self.first_poll_pending = true;
                self.pending_observed.store(true, Ordering::SeqCst);
                return Poll::Pending;
            }
            self.bytes.lock().unwrap().extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// A reader that fails on first poll (a dying pipe: EPIPE/ECONNRESET).
    struct ErroringReader;
    impl AsyncRead for ErroringReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "ECONNRESET",
            )))
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_query_rejects_if_the_stream_closes_before_its_reply() {
        // Isolated client: an input we can close, no server behind it.
        let (test_w, client_input) = tokio::io::duplex(1024);
        let client = ProtocolClient::new(ProtocolClientOptions {
            input: Box::new(client_input),
            output: Box::new(tokio::io::sink()),
        });
        let c = client.clone();
        let pending = tokio::spawn(async move { c.get_history().await });
        tokio::task::yield_now().await;
        drop(test_w); // EOF the client's input
        let res = pending.await.unwrap();
        assert!(matches!(res, Err(ClientError::Closed)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stream_error_rejects_pending_queries_without_crashing() {
        let client = ProtocolClient::new(ProtocolClientOptions {
            input: Box::new(ErroringReader),
            output: Box::new(tokio::io::sink()),
        });
        // The waiter must still reject, and the raw error must not crash the task.
        let res = client.get_history().await;
        assert!(matches!(res, Err(ClientError::Closed)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn checked_steer_rejects_on_an_uncorrelated_terminal_queue_snapshot() {
        let (server_output, client_input) = tokio::io::duplex(4096);
        let (client_output, server_input) = tokio::io::duplex(4096);
        let client = ProtocolClient::new(ProtocolClientOptions {
            input: Box::new(client_input),
            output: Box::new(client_output),
        });

        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_input).lines();
            let line = lines
                .next_line()
                .await
                .expect("read checked steer")
                .expect("checked steer frame");
            let op: Op = runtime_codec::decode_text(&RuntimeString::from_utf8(&line))
                .expect("checked steer decodes");
            assert!(matches!(op.op, OpKind::Steer { .. }));

            // Deliberately use another submission id: queue durability is a
            // session-wide terminal condition, not merely this op's reply.
            let event = ProtocolEvent {
                submission_id: "another-operation".into(),
                queue_snapshot: Some(QueueSnapshot {
                    revision: 0,
                    items: Vec::new(),
                    status: QueueSnapshotStatus::PersistenceFailed,
                    error: Some("forced queue flush failure".into()),
                }),
                ..Default::default()
            };
            let mut frame = runtime_codec::encode_bytes(&event).expect("terminal event encodes");
            frame.push(b'\n');
            let mut server_output = server_output;
            server_output
                .write_all(&frame)
                .await
                .expect("write terminal event");
            server_output.flush().await.expect("flush terminal event");
        });

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            client.steer_to_completion(Input::Text("must not hang".into()), None),
        )
        .await
        .expect("terminal queue state must settle checked steer")
        .expect_err("terminal queue state must reject checked steer");
        assert!(matches!(
            error,
            ClientError::Op(message) if message == "forced queue flush failure"
        ));
        server.await.expect("synthetic protocol peer");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn events_stream_ends_when_input_closes() {
        use futures::StreamExt;
        let (test_w, client_input) = tokio::io::duplex(1024);
        let client = ProtocolClient::new(ProtocolClientOptions {
            input: Box::new(client_input),
            output: Box::new(tokio::io::sink()),
        });
        let mut events = client.events();
        drop(test_w);
        assert!(events.next().await.is_none());
    }

    #[tokio::test]
    async fn shutdown_write_error_closes_pending_input_and_retains_the_error() {
        use futures::StreamExt;

        let writes = Arc::new(AtomicUsize::new(0));
        let client = client_with_short_shutdown(
            Box::new(PendingReader),
            Box::new(ImmediateErrorWriter {
                writes: writes.clone(),
            }),
        );
        let mut events = client.events();

        let first = shutdown_error(
            tokio::time::timeout(std::time::Duration::from_secs(2), client.shutdown())
                .await
                .expect("shutdown must not wait for pending input"),
        );
        assert_eq!(
            first,
            "failed to write protocol op: immediate write failure"
        );
        assert!(events.next().await.is_none());

        let retained = shutdown_error(client.shutdown().await);
        assert_eq!(retained, first);
        assert_eq!(writes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn permanently_pending_shutdown_write_times_out_and_ends_events() {
        use futures::StreamExt;

        let polled = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicBool::new(false));
        let writes = Arc::new(AtomicUsize::new(0));
        let client = client_with_short_shutdown(
            Box::new(PendingReader),
            Box::new(PendingWriter {
                polled: polled.clone(),
                dropped: dropped.clone(),
                writes: writes.clone(),
            }),
        );
        let mut events = client.events();
        let shutdown_client = client.clone();
        let shutdown = tokio::spawn(async move { shutdown_client.shutdown().await });
        wait_until_true(&polled).await;

        let first = shutdown_error(
            tokio::time::timeout(std::time::Duration::from_secs(2), shutdown)
                .await
                .expect("pending output must be bounded")
                .unwrap(),
        );
        assert_eq!(
            first,
            format!(
                "protocol shutdown timed out after {}ms",
                TEST_SHUTDOWN_TIMEOUT.as_millis()
            )
        );
        assert!(events.next().await.is_none());
        assert!(dropped.load(Ordering::SeqCst));
        let write_polls = writes.load(Ordering::SeqCst);
        assert!(write_polls >= 1);
        assert_eq!(shutdown_error(client.shutdown().await), first);
        assert_eq!(writes.load(Ordering::SeqCst), write_polls);
    }

    #[tokio::test]
    async fn shutdown_times_out_when_peer_consumes_op_but_never_replies_or_closes() {
        use futures::StreamExt;

        let (client_output, server_input) = tokio::io::duplex(4096);
        let client = client_with_short_shutdown(Box::new(PendingReader), Box::new(client_output));
        let mut events = client.events();
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_input).lines();
            let line = lines.next_line().await.unwrap().unwrap();
            let op: Op = runtime_codec::decode_text(&RuntimeString::from_utf8(&line)).unwrap();
            assert!(matches!(op.op, OpKind::Shutdown));
            assert!(
                lines.next_line().await.unwrap().is_none(),
                "client emitted more than one shutdown op"
            );
        });

        let error = shutdown_error(
            tokio::time::timeout(std::time::Duration::from_secs(2), client.shutdown())
                .await
                .expect("missing ACK/EOF must be bounded"),
        );
        assert_eq!(
            error,
            format!(
                "protocol shutdown timed out after {}ms",
                TEST_SHUTDOWN_TIMEOUT.as_millis()
            )
        );
        assert!(events.next().await.is_none());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_first_shutdown_caller_retains_one_timed_out_wire_operation() {
        let (client_output, server_input) = tokio::io::duplex(4096);
        let client = client_with_short_shutdown(Box::new(PendingReader), Box::new(client_output));
        let (observed_send, observed_receive) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_input).lines();
            let line = lines.next_line().await.unwrap().unwrap();
            let op: Op = runtime_codec::decode_text(&RuntimeString::from_utf8(&line)).unwrap();
            assert!(matches!(op.op, OpKind::Shutdown));
            let _ = observed_send.send(());
            assert!(
                lines.next_line().await.unwrap().is_none(),
                "caller cancellation emitted a duplicate shutdown op"
            );
        });

        let first_client = client.clone();
        let first = tokio::spawn(async move { first_client.shutdown().await });
        observed_receive.await.unwrap();
        first.abort();
        let _ = first.await;

        let second_client = client.clone();
        let third_client = client.clone();
        let second = tokio::spawn(async move { second_client.shutdown().await });
        let third = tokio::spawn(async move { third_client.shutdown().await });
        let second = shutdown_error(
            tokio::time::timeout(std::time::Duration::from_secs(2), second)
                .await
                .expect("retained shutdown owner must complete")
                .unwrap(),
        );
        let third = shutdown_error(third.await.unwrap());
        let retained = shutdown_error(client.shutdown().await);
        assert_eq!(second, third);
        assert_eq!(retained, second);
        assert_eq!(
            retained,
            format!(
                "protocol shutdown timed out after {}ms",
                TEST_SHUTDOWN_TIMEOUT.as_millis()
            )
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_pending_send_cannot_unbound_shutdown_or_reopen_write_admission() {
        use futures::StreamExt;

        let polled = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicBool::new(false));
        let writes = Arc::new(AtomicUsize::new(0));
        let client = client_with_short_shutdown(
            Box::new(PendingReader),
            Box::new(PendingWriter {
                polled: polled.clone(),
                dropped: dropped.clone(),
                writes: writes.clone(),
            }),
        );
        let mut events = client.events();
        let sending_client = client.clone();
        let pending_send = tokio::spawn(async move {
            sending_client
                .send(&Op {
                    id: "pending-before-shutdown".into(),
                    op: OpKind::Interrupt {
                        origin: None,
                        expected_turn_id: None,
                    },
                })
                .await;
        });
        wait_until_true(&polled).await;

        let error = shutdown_error(
            tokio::time::timeout(std::time::Duration::from_secs(2), client.shutdown())
                .await
                .expect("a send holding the output mutex must not unbound shutdown"),
        );
        assert_eq!(
            error,
            format!(
                "protocol shutdown timed out after {}ms",
                TEST_SHUTDOWN_TIMEOUT.as_millis()
            )
        );
        assert!(events.next().await.is_none());
        assert_eq!(writes.load(Ordering::SeqCst), 1);
        assert!(!dropped.load(Ordering::SeqCst));

        pending_send.abort();
        let _ = pending_send.await;
        let rejected = client
            .send_checked(&Op {
                id: "after-shutdown".into(),
                op: OpKind::Interrupt {
                    origin: None,
                    expected_turn_id: None,
                },
            })
            .await;
        assert!(matches!(rejected, Err(ClientError::Closed)));
        assert_eq!(writes.load(Ordering::SeqCst), 1);
        drop(client);
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_partial_frame_poisons_transport_without_appending_shutdown() {
        let blocked = Arc::new(AtomicBool::new(false));
        let bytes = Arc::new(StdMutex::new(Vec::new()));
        let polls = Arc::new(AtomicUsize::new(0));
        let client = client_with_short_shutdown(
            Box::new(PendingReader),
            Box::new(PartialThenPendingWriter {
                blocked: blocked.clone(),
                bytes: bytes.clone(),
                polls: polls.clone(),
                prefix_len: 7,
                wrote_prefix: false,
            }),
        );

        let sending_client = client.clone();
        let send = tokio::spawn(async move {
            sending_client
                .send_checked(&Op {
                    id: "cancel-after-prefix".into(),
                    op: OpKind::Interrupt {
                        origin: None,
                        expected_turn_id: None,
                    },
                })
                .await
        });
        wait_until_true(&blocked).await;
        let abandoned_prefix = bytes.lock().unwrap().clone();
        assert_eq!(abandoned_prefix.len(), 7);
        assert!(!abandoned_prefix.ends_with(b"\n"));
        let polls_before_cancel = polls.load(Ordering::SeqCst);

        send.abort();
        let _ = send.await;

        let first_client = client.clone();
        let second_client = client.clone();
        let first = tokio::spawn(async move { first_client.shutdown().await });
        let second = tokio::spawn(async move { second_client.shutdown().await });
        let first = shutdown_error(
            tokio::time::timeout(std::time::Duration::from_secs(2), first)
                .await
                .expect("poisoned shutdown must not wait for I/O")
                .unwrap(),
        );
        let second = shutdown_error(second.await.unwrap());
        assert_eq!(first, CLIENT_INCOMPLETE_FRAME_ERROR);
        assert_eq!(second, first);
        assert_eq!(shutdown_error(client.shutdown().await), first);

        let rejected = client
            .send_checked(&Op {
                id: "after-partial-cancellation".into(),
                op: OpKind::Interrupt {
                    origin: None,
                    expected_turn_id: None,
                },
            })
            .await;
        assert_eq!(
            rejected.expect_err("poison must remain stable").to_string(),
            first
        );
        assert_eq!(polls.load(Ordering::SeqCst), polls_before_cancel);
        assert_eq!(*bytes.lock().unwrap(), abandoned_prefix);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_zero_byte_write_still_allows_one_shutdown_frame() {
        let bytes = Arc::new(StdMutex::new(Vec::new()));
        let pending_observed = Arc::new(AtomicBool::new(false));
        let polls = Arc::new(AtomicUsize::new(0));
        let (mut server_output, client_input) = tokio::io::duplex(4096);
        let client = client_with_short_shutdown(
            Box::new(client_input),
            Box::new(PendingOnceThenRecordingWriter {
                bytes: bytes.clone(),
                first_poll_pending: false,
                pending_observed: pending_observed.clone(),
                polls: polls.clone(),
            }),
        );

        let sending_client = client.clone();
        let send = tokio::spawn(async move {
            sending_client
                .send_checked(&Op {
                    id: "cancel-before-first-byte".into(),
                    op: OpKind::Interrupt {
                        origin: None,
                        expected_turn_id: None,
                    },
                })
                .await
        });
        wait_until_true(&pending_observed).await;
        assert!(bytes.lock().unwrap().is_empty());
        send.abort();
        let _ = send.await;

        let first_client = client.clone();
        let second_client = client.clone();
        let first = tokio::spawn(async move { first_client.shutdown().await });
        let second = tokio::spawn(async move { second_client.shutdown().await });
        let shutdown_line = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let line = {
                    let written = bytes.lock().unwrap();
                    written
                        .iter()
                        .position(|byte| *byte == b'\n')
                        .map(|end| written[..end].to_vec())
                };
                if let Some(line) = line {
                    break line;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("shutdown frame was not written");
        let shutdown_line = String::from_utf8(shutdown_line).unwrap();
        let shutdown_op: Op =
            runtime_codec::decode_text(&RuntimeString::from_utf8(&shutdown_line)).unwrap();
        assert!(matches!(shutdown_op.op, OpKind::Shutdown));

        let expected = ShutdownCompletion {
            ok: true,
            flush_completed: true,
            cleanup_completed: true,
            timed_out: false,
            error: None,
        };
        let mut completion_line = runtime_codec::encode_bytes(&ProtocolEvent {
            submission_id: shutdown_op.id,
            shutdown_completed: Some(Box::new(expected.clone())),
            ..Default::default()
        })
        .unwrap();
        completion_line.push(b'\n');
        server_output.write_all(&completion_line).await.unwrap();
        server_output.flush().await.unwrap();

        assert_eq!(first.await.unwrap().unwrap(), expected);
        assert_eq!(second.await.unwrap().unwrap(), expected);
        assert_eq!(client.shutdown().await.unwrap(), expected);
        let written = bytes.lock().unwrap();
        assert_eq!(written.iter().filter(|byte| **byte == b'\n').count(), 1);
        assert_eq!(polls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn send_emits_lone_surrogate_as_a_json_escape() {
        let (client_output, host_input) = tokio::io::duplex(4096);
        let client = ProtocolClient::new(ProtocolClientOptions {
            input: Box::new(tokio::io::empty()),
            output: Box::new(client_output),
        });

        client
            .send(&Op {
                id: "surrogate-send".into(),
                op: OpKind::CancelRequest {
                    request_id: klaus_agent_core::runtime_values::runtime_json::parse(
                        &RuntimeString::from_utf8(r#""\ud800""#),
                    )
                    .unwrap()
                    .into_string()
                    .unwrap(),
                },
            })
            .await;

        let line = BufReader::new(host_input)
            .lines()
            .next_line()
            .await
            .unwrap()
            .unwrap();
        assert!(line.contains(r#""requestId":"\ud800""#), "{line}");
        assert!(!line.contains('\u{fdd0}'), "{line}");
    }

    #[tokio::test]
    async fn event_reader_accepts_a_lone_surrogate_escape() {
        use futures::StreamExt;

        let (mut server_output, client_input) = tokio::io::duplex(4096);
        let client = ProtocolClient::new(ProtocolClientOptions {
            input: Box::new(client_input),
            output: Box::new(tokio::io::sink()),
        });
        let mut events = client.events();
        server_output
            .write_all(
                br#"{"submissionId":"surrogate-event","event":{"type":"notification","level":"info","message":"\ud800"}}
"#,
            )
            .await
            .unwrap();
        drop(server_output);

        let event = events.next().await.expect("lone-surrogate event");
        assert!(matches!(
            &event.event,
            Some(klaus_agent_core::types::events::EngineEvent::Notification { message, .. })
                if message == &RuntimeString::from_utf16(vec![0xd800])
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dream_now_with_completion_cannot_miss_an_immediate_completion_event() {
        let (client_side, server_side) = tokio::io::duplex(4096);
        let (client_input, client_output) = tokio::io::split(client_side);
        let (server_input, mut server_output) = tokio::io::split(server_side);
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_input).lines();
            let line = lines.next_line().await.unwrap().unwrap();
            let op: Op = runtime_codec::decode_text(&RuntimeString::from_utf8(&line)).unwrap();
            let start = ProtocolEvent {
                submission_id: op.id.clone(),
                dream: Some(DreamNowOutcome {
                    ok: true,
                    reason: None,
                }),
                ..Default::default()
            };
            let completed = ProtocolEvent {
                submission_id: op.id,
                dream_completed: Some(DreamNowCompletion {
                    ok: true,
                    reason: None,
                    files_touched: Some(vec!["/memory/consolidated.md".into()]),
                    turn_count: Some(2),
                }),
                ..Default::default()
            };
            for event in [start, completed] {
                let mut line = runtime_codec::encode_bytes(&event).unwrap();
                line.push(b'\n');
                server_output.write_all(&line).await.unwrap();
            }
            server_output.flush().await.unwrap();
        });
        let client = ProtocolClient::new(ProtocolClientOptions {
            input: Box::new(client_input),
            output: Box::new(client_output),
        });

        let mut task = client.dream_now_with_completion().await.unwrap();
        assert!(task.ok);
        assert_eq!(task.reason, None::<DreamNowReason>);
        let completed = task.done.take().unwrap().await.unwrap().unwrap();
        assert!(completed.ok);
        assert_eq!(completed.turn_count, Some(2));
        assert_eq!(
            completed.files_touched,
            Some(vec!["/memory/consolidated.md".into()])
        );
        server.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_shutdown_calls_share_one_submission_and_completion() {
        let (client_side, server_side) = tokio::io::duplex(4096);
        let (client_input, client_output) = tokio::io::split(client_side);
        let (server_input, mut server_output) = tokio::io::split(server_side);
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_input).lines();
            let line = lines.next_line().await.unwrap().unwrap();
            let op: Op = runtime_codec::decode_text(&RuntimeString::from_utf8(&line)).unwrap();
            assert!(matches!(op.op, OpKind::Shutdown));
            let completion = ShutdownCompletion {
                ok: true,
                flush_completed: true,
                cleanup_completed: true,
                timed_out: false,
                error: None,
            };
            let mut line = runtime_codec::encode_bytes(&ProtocolEvent {
                submission_id: op.id,
                shutdown_completed: Some(Box::new(completion.clone())),
                ..Default::default()
            })
            .unwrap();
            line.push(b'\n');
            server_output.write_all(&line).await.unwrap();
            server_output.flush().await.unwrap();

            assert!(
                lines.next_line().await.unwrap().is_none(),
                "duplicate high-level shutdown emitted a second wire op"
            );
            completion
        });
        let client = ProtocolClient::new(ProtocolClientOptions {
            input: Box::new(client_input),
            output: Box::new(client_output),
        });
        let mut events = client.events();

        let (first, second) = tokio::join!(client.shutdown(), client.shutdown());
        let expected = server.await.unwrap();
        assert_eq!(first.unwrap(), expected);
        assert_eq!(second.unwrap(), expected);
        assert_eq!(client.shutdown().await.unwrap(), expected);
        let final_event = events.next().await.expect("final shutdown ACK event");
        assert_eq!(final_event.shutdown_completed.as_deref(), Some(&expected));
        assert!(events.next().await.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_start_closes_clone_write_admission_before_ack() {
        let (client_side, server_side) = tokio::io::duplex(4096);
        let (client_input, client_output) = tokio::io::split(client_side);
        let (server_input, mut server_output) = tokio::io::split(server_side);
        let (observed_send, observed_receive) = tokio::sync::oneshot::channel();
        let (release_send, release_receive) = tokio::sync::oneshot::channel();
        let expected = ShutdownCompletion {
            ok: true,
            flush_completed: true,
            cleanup_completed: true,
            timed_out: false,
            error: None,
        };
        let server_expected = expected.clone();
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_input).lines();
            let line = lines.next_line().await.unwrap().unwrap();
            let op: Op = runtime_codec::decode_text(&RuntimeString::from_utf8(&line)).unwrap();
            assert!(matches!(op.op, OpKind::Shutdown));
            observed_send.send(()).unwrap();
            release_receive.await.unwrap();

            let mut line = runtime_codec::encode_bytes(&ProtocolEvent {
                submission_id: op.id,
                shutdown_completed: Some(Box::new(server_expected)),
                ..Default::default()
            })
            .unwrap();
            line.push(b'\n');
            server_output.write_all(&line).await.unwrap();
            server_output.flush().await.unwrap();
            assert!(
                lines.next_line().await.unwrap().is_none(),
                "a clone emitted an operation after shutdown started"
            );
        });
        let client = ProtocolClient::new(ProtocolClientOptions {
            input: Box::new(client_input),
            output: Box::new(client_output),
        });

        let shutdown_client = client.clone();
        let shutdown = tokio::spawn(async move { shutdown_client.shutdown().await });
        observed_receive.await.unwrap();
        let rejected = client
            .send_checked(&Op {
                id: "after-shutdown-start".into(),
                op: OpKind::Interrupt {
                    origin: None,
                    expected_turn_id: None,
                },
            })
            .await;
        assert!(matches!(rejected, Err(ClientError::Closed)));

        release_send.send(()).unwrap();
        assert_eq!(shutdown.await.unwrap().unwrap(), expected);
        server.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelling_first_shutdown_caller_does_not_resubmit_the_wire_op() {
        let (client_side, server_side) = tokio::io::duplex(4096);
        let (client_input, client_output) = tokio::io::split(client_side);
        let (server_input, mut server_output) = tokio::io::split(server_side);
        let (observed_send, observed_receive) = tokio::sync::oneshot::channel();
        let (release_send, release_receive) = tokio::sync::oneshot::channel();
        let expected = ShutdownCompletion {
            ok: true,
            flush_completed: true,
            cleanup_completed: true,
            timed_out: false,
            error: None,
        };
        let server_expected = expected.clone();
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_input).lines();
            let line = lines.next_line().await.unwrap().unwrap();
            let op: Op = runtime_codec::decode_text(&RuntimeString::from_utf8(&line)).unwrap();
            assert!(matches!(op.op, OpKind::Shutdown));
            let _ = observed_send.send(());
            release_receive.await.unwrap();

            let mut line = runtime_codec::encode_bytes(&ProtocolEvent {
                submission_id: op.id,
                shutdown_completed: Some(Box::new(server_expected)),
                ..Default::default()
            })
            .unwrap();
            line.push(b'\n');
            server_output.write_all(&line).await.unwrap();
            server_output.flush().await.unwrap();

            assert!(
                lines.next_line().await.unwrap().is_none(),
                "cancelling the first caller emitted a duplicate shutdown op"
            );
        });
        let client = ProtocolClient::new(ProtocolClientOptions {
            input: Box::new(client_input),
            output: Box::new(client_output),
        });

        let first_client = client.clone();
        let first = tokio::spawn(async move { first_client.shutdown().await });
        observed_receive.await.unwrap();
        first.abort();
        let _ = first.await;

        let second_client = client.clone();
        let second = tokio::spawn(async move { second_client.shutdown().await });
        release_send.send(()).unwrap();
        assert_eq!(second.await.unwrap().unwrap(), expected);
        assert_eq!(client.shutdown().await.unwrap(), expected);
        server.await.unwrap();
    }
}
