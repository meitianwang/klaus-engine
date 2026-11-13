//! The app-server connection service: JSON-RPC envelope on the outside, one
//! inner Op/Event `ProtocolServer` per session on the inside.
//!
//! Responsibilities of this layer (and nothing more):
//!   * `initialize` handshake gate — reject anything else first.
//!   * Session multiplexing — many sessions over ONE connection, each backed by
//!     its own `ProtocolServer` running on an in-memory duplex pipe.
//!   * Envelope translation — ops go in as raw JSON lines, protocol events come
//!     back out as `session/event` notifications tagged with the session id.
//!   * Reverse RPC — `approval.requested` / elicitation events become
//!     server->client JSON-RPC requests; the client's response is translated
//!     back into an `approve` / `resolve_elicitation` op.
//!
//! The inner Op/Event wire schema is passed through untouched: this layer
//! translates the outer JSON-RPC envelope onto the inner submission/event
//! protocol without re-modeling the payloads.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use futures::{future::BoxFuture, stream::FuturesUnordered, StreamExt};
use serde_json::{Map, Value};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::{broadcast, mpsc, oneshot};

use klaus_agent_core::config_governance::ConfigGovernanceSource;
use klaus_agent_core::config_store::{self, ConfigEdit, ConfigStoreError, MergeStrategy};
use klaus_agent_core::credential_store::{
    is_valid_model_provider_id, model_credential_key, CredentialStore,
};
use klaus_agent_core::engine::session::LiveForkSnapshot;
use klaus_agent_core::hooks::store::{self as hooks_store, HooksStoreError};
use klaus_agent_core::permission::rule_store::{self, DurableRules};
use klaus_agent_core::runtime_values::{RuntimeJson, RuntimeObject, RuntimeString};
use klaus_agent_core::types::permission::{PermissionBehavior, PermissionRule};
use klaus_agent_protocol::{
    CreateSession as CreateSessionFn, CreateSessionArgs as SessionHandlers, CreateSessionFuture,
    ProtocolLifecycle, ProtocolPluginAdmin, ProtocolServer, ProtocolServerOptions,
};

use crate::lifecycle::{CreateSession, SessionContext, SideTaskStart};

use super::model_sources;
use super::runtime_wire::{RawJson, RuntimeValue};
use super::wire;
use exact_requests::Incoming;

#[path = "dictation.rs"]
mod dictation;
#[path = "exact_payload.rs"]
pub(super) mod exact_payload;
#[path = "exact_requests.rs"]
mod exact_requests;
#[path = "goal_objective.rs"]
mod goal_objective;
#[path = "goals.rs"]
mod goals;
#[path = "queue_service.rs"]
mod queue;
#[path = "realtime.rs"]
mod realtime;
#[path = "session_unload.rs"]
mod session_unload;
#[path = "text_completion.rs"]
mod text_completion;
use exact_payload::{field as exact_field, text as exact_text, Payload};
use goal_objective::handle_read_goal_objective;
use goals::handle_goal_request;

#[cfg(test)]
#[path = "outgoing_backpressure_tests.rs"]
mod outgoing_backpressure_tests;

/// Duplex pipe capacity between this layer and each inner `ProtocolServer`.
const SESSION_PIPE_CAPACITY: usize = 1 << 20;
const OUTGOING_CAPACITY: usize = 128;
const CONNECTION_DRAIN_BUDGET: std::time::Duration = std::time::Duration::from_secs(30);

/// A follower can resume across a short disconnect without letting an idle or
/// slow client retain the engine's entire transcript in memory.
const SESSION_STREAM_MAX_EVENTS: usize = 2_048;
const SESSION_STREAM_MAX_BYTES: usize = 16 * 1024 * 1024;

/// Ceiling for concurrently processing heavy (spawned) requests. Saturation
/// rejects new ones with `-32001` so clients back off instead of queueing
/// unboundedly — the layer's overload contract.
pub(crate) const MAX_INFLIGHT_HEAVY_REQUESTS: u64 = 64;

/// Options for [`run_app_server`].
pub struct AppServerOptions {
    /// Captured endpoint configuration and cwd for native Windows preparation.
    pub windows_sandbox_context: Option<super::windows_sandbox::Context>,
    /// Mints a Session per `session/start` (same seam the WS server uses).
    pub create_session: CreateSession,
    /// Secret store shared by provider and MCP authentication.
    pub credential_store: Option<Arc<dyn CredentialStore>>,
    /// Session lifecycle ops (list/resume/fork/archive/...) cloned into every
    /// inner `ProtocolServer` so `session/op` reaches them like native stdio.
    pub lifecycle: Option<ProtocolLifecycle>,
    /// Endpoint-level plugin ledger manager shared by every session.
    pub plugin_admin: Option<Arc<dyn ProtocolPluginAdmin>>,
    /// Where the engine keeps its durable permission rules. `None` uses the
    /// config home (`permission::rule_store::default_rules_path`); an embedder
    /// that runs several endpoints against separate state sets it explicitly.
    pub permission_rules_path: Option<PathBuf>,
    /// Where the engine keeps its user config. `None` uses the config home
    /// (`config_store::user_config_path`), same reasoning as the rule store.
    pub config_path: Option<PathBuf>,
    /// Ceiling for the inner init_session ACK wait inside `session/start`.
    /// `None` applies STARTUP_INIT_ACK_BUDGET_SECS; tests inject a smaller
    /// budget to exercise the stalled-startup path without sitting through
    /// the production deadline.
    pub startup_init_ack_budget: Option<std::time::Duration>,
    /// Quiet period before an unwatched, idle session is unloaded by the
    /// endpoint. `None` resolves the configured delay (default 1800 seconds);
    /// tests inject a smaller delay to exercise the unload path.
    pub session_unload_delay: Option<std::time::Duration>,
}

/// Response shapes that must settle in the sole ordered event pump.
enum OrderedResponseKind {
    Goal,
    RuntimeConfig,
}
struct OrderedResponse {
    state: std::sync::Weak<AppServerState>,
    id: Value,
    kind: OrderedResponseKind,
}

/// One live session: its inner protocol server plus the plumbing to reach it.
struct SessionHandle {
    ps: Arc<ProtocolServer>,
    /// Raw op lines are written here; a per-session task feeds the pipe.
    op_tx: mpsc::UnboundedSender<Payload>,
    /// Submission ids of in-flight `turn/start` runs (for `turn/completed`).
    turns: Arc<Mutex<std::collections::HashSet<String>>>,
    /// One-shot waiters keyed by submission id, settled by the TERMINAL reply
    /// envelope of that submission (`init_session` and every promoted request
    /// method). Stream events (`{ event: ... }`) never settle a waiter.
    waiters: Arc<Mutex<HashMap<String, oneshot::Sender<WaiterReply>>>>,
    ordered_responses: Mutex<HashMap<String, OrderedResponse>>,
    interrupt_changes: Mutex<HashMap<String, queue::InterruptChange>>,
    event_stream_closed: AtomicBool,
    realtime_events: broadcast::Sender<RuntimeJson>,
    read_items: Mutex<super::thread_read::LiveItems>,
    stream: Mutex<SessionStream>,
    /// Metadata of a newly registered thread before its lazy core is created.
    read_seed: Option<klaus_agent_core::store::session_store::ThreadReadSnapshot>,
}

impl SessionHandle {
    fn register_waiter(&self, submission_id: String, waiter: oneshot::Sender<WaiterReply>) {
        let mut waiters = self.waiters.lock().unwrap();
        // Registration and EOF share the lock: a late receiver must fail instead of
        // retaining a sender after the only task capable of replying has exited.
        if !self.event_stream_closed.load(Ordering::Acquire) {
            waiters.insert(submission_id, waiter);
        }
    }

    async fn close_waiters(&self) {
        {
            let mut waiters = self.waiters.lock().unwrap();
            self.event_stream_closed.store(true, Ordering::Release);
            waiters.clear();
        }
        self.interrupt_changes.lock().unwrap().clear();
        let replies: Vec<_> = self
            .ordered_responses
            .lock()
            .unwrap()
            .drain()
            .map(|(_, reply)| reply)
            .collect();
        for reply in replies {
            if let Some(state) = reply.state.upgrade() {
                let id = reply.id;
                state
                    .send(wire::error_response(
                        &id,
                        wire::ENGINE_ERROR,
                        "session terminated before the request completed",
                    ))
                    .await;
            }
        }
    }

    fn admit_turn(&self, turn_id: &str, op: impl Into<Payload>) -> Result<(), &'static str> {
        const CLOSED: &str = "session terminated before the turn was accepted";
        if self.ps.is_closing() {
            return Err(CLOSED);
        }
        self.turns.lock().unwrap().insert(turn_id.to_string());
        if self
            .op_tx
            .send(Payload::Runtime(exact_payload::object([
                ("id", turn_id.into()),
                (
                    "op",
                    match op.into() {
                        Payload::Scalar(value) => exact_payload::runtime(value),
                        Payload::Runtime(value) => value,
                        Payload::Object(_) | Payload::Encoded(_) => unreachable!(
                            "op queue accepts validated scalar or native runtime operations"
                        ),
                    },
                ),
            ])))
            .is_err()
        {
            // Nothing reached the inner server, so no terminal event can
            // release this reservation. Keep unrelated in-flight work intact.
            self.turns.lock().unwrap().remove(turn_id);
            self.read_items.lock().unwrap().finish(turn_id);
            return Err(CLOSED);
        }
        Ok(())
    }
}

struct WaiterReply {
    value: RuntimeJson,
    stream_revision: u64,
    active_items: Vec<RuntimeJson>,
    covered_message_ids: Vec<String>,
    covered_tool_results: Vec<RuntimeJson>,
}

#[derive(Clone)]
struct RetainedSessionEvent {
    revision: u64,
    event: RuntimeJson,
    bytes: usize,
}

struct SessionStream {
    id: String,
    revision: u64,
    bytes: usize,
    events: VecDeque<RetainedSessionEvent>,
}

impl SessionStream {
    fn new() -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            revision: 0,
            bytes: 0,
            events: VecDeque::new(),
        }
    }

    fn record(&mut self, event: &RuntimeJson) -> u64 {
        self.revision = self.revision.saturating_add(1);
        let bytes = klaus_agent_core::runtime_values::runtime_json::stringify(event)
            .to_node_utf8()
            .len();
        self.bytes = self.bytes.saturating_add(bytes);
        self.events.push_back(RetainedSessionEvent {
            revision: self.revision,
            event: event.clone(),
            bytes,
        });
        while self.events.len() > SESSION_STREAM_MAX_EVENTS || self.bytes > SESSION_STREAM_MAX_BYTES
        {
            let Some(removed) = self.events.pop_front() else {
                break;
            };
            self.bytes = self.bytes.saturating_sub(removed.bytes);
        }
        self.revision
    }

    fn available_from_revision(&self) -> u64 {
        self.events
            .front()
            .map_or(self.revision.saturating_add(1), |event| event.revision)
    }

    fn replay_after(&self, revision: u64) -> Option<Vec<RuntimeJson>> {
        if revision > self.revision || revision.saturating_add(1) < self.available_from_revision() {
            return None;
        }
        Some(
            self.events
                .iter()
                .filter(|event| event.revision > revision)
                .map(|event| {
                    exact_payload::object([
                        ("revision", event.revision.into()),
                        ("event", event.event.clone()),
                    ])
                })
                .collect(),
        )
    }
}

/// What a parked reverse request resolves into when the client answers.
enum ReverseKind {
    Approval { request_id: String },
    Elicitation { request_id: String },
}

struct PendingReverse {
    session_id: String,
    kind: ReverseKind,
}

/// A native operation retains its bounded admission slot through output backpressure.
pub(super) struct Outgoing {
    pub(super) message: Payload,
    pub(super) cursor_operation: Option<super::cursor::Operation>,
    pub(super) workbuddy_operation: Option<super::workbuddy::Operation>,
    pub(super) provider_operation: Option<super::provider_operations::Operation>,
}

pub(super) async fn write_app_output<O: AsyncWrite + Unpin>(
    mut output: O,
    mut out_rx: mpsc::Receiver<Outgoing>,
) {
    while let Some(outgoing) = out_rx.recv().await {
        let Outgoing {
            mut message,
            cursor_operation,
            workbuddy_operation,
            provider_operation,
        } = outgoing;
        let publication_error = cursor_operation
            .as_ref()
            .and_then(|operation| operation.publication_error())
            .or_else(|| {
                workbuddy_operation
                    .as_ref()
                    .and_then(|operation| operation.publication_error())
            })
            .map(str::to_owned)
            .or_else(|| {
                provider_operation
                    .as_ref()
                    .and_then(|operation| operation.publication_error())
            });
        if let Some(error) = &publication_error {
            message =
                wire::error_response(&message.response_id(), wire::ENGINE_ERROR, error).into();
        }
        let Ok(mut line) = message.bytes() else {
            continue;
        };
        line.push(b'\n');
        let write = async {
            output.write_all(&line).await?;
            output.flush().await
        };
        let written = if publication_error.is_none() {
            let provider_invalidated = async {
                match provider_operation.as_ref() {
                    Some(operation) => operation.invalidated().await,
                    None => std::future::pending::<()>().await,
                }
            };
            let native_invalidated = async {
                if cursor_operation.is_none() && workbuddy_operation.is_none() {
                    std::future::pending::<()>().await;
                }
                loop {
                    if cursor_operation
                        .as_ref()
                        .and_then(|operation| operation.publication_error())
                        .is_some()
                        || workbuddy_operation
                            .as_ref()
                            .and_then(|operation| operation.publication_error())
                            .is_some()
                    {
                        return;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            };
            tokio::select! {
                biased;
                _ = provider_invalidated => break,
                _ = native_invalidated => break,
                result = write => result,
            }
        } else {
            write.await
        };
        if written.is_err() {
            break;
        }
        drop(provider_operation);
        drop(cursor_operation);
        drop(workbuddy_operation);
    }
}

pub(super) struct AppServerState {
    connection_closed: Arc<AtomicBool>,
    windows_sandbox_context: super::windows_sandbox::Context,
    initialized: AtomicBool,
    realtime: realtime::RealtimeSessions,
    dictation: dictation::DictationSessions,
    sessions: Mutex<HashMap<String, Arc<SessionHandle>>>,
    queue_revisions: Mutex<HashMap<String, u64>>,
    queue_store: super::queue_store::Store,
    /// Lazily-built ENDPOINT-level inner server for `engine/op` (lifecycle,
    /// plugin ledger, ... — ops the native stdio server accepts without a
    /// bound session). Its replies stream out as `engine/event`.
    control: Mutex<Option<Arc<SessionHandle>>>,
    pending_reverse: Mutex<HashMap<u64, PendingReverse>>,
    /// Notification methods suppressed for this connection (exact match),
    /// negotiated via `initialize.params.capabilities.optOutNotificationMethods`.
    notification_opt_out: Mutex<HashSet<String>>,
    /// Sessions this connection unsubscribed from: their notifications are
    /// suppressed until `session/subscribe` re-arms them. Request/reply traffic
    /// is unaffected — only the event stream is gated.
    unsubscribed_sessions: Mutex<HashSet<String>>,
    /// Per-session subscription state feeding the idle-unload machine: `true`
    /// while a client watches the session's event stream.
    session_subscriptions: Mutex<HashMap<String, tokio::sync::watch::Sender<bool>>>,
    /// Per-session lifecycle status feeding the idle-unload machine and the
    /// broadcast `session/statusChanged` notification.
    session_status: session_unload::SessionStatusRegistry,
    /// Sessions currently being unloaded; `session/subscribe` and
    /// `session/start` refuse them until teardown finishes.
    pending_session_unloads: Mutex<HashSet<String>>,
    /// Cancellation tokens for the per-session idle-unload watchers.
    unload_watchers: Mutex<HashMap<String, tokio::sync::oneshot::Sender<()>>>,
    /// Endpoint override for the unload quiet period; `None` resolves from the
    /// engine configuration file.
    session_unload_delay_override: Option<std::time::Duration>,
    /// Whether this connection opted into the Thread/Turn/Item product surface.
    product_events: AtomicBool,
    /// Whether session notifications include `streamId` / `streamRevision`.
    revisioned_session_events: AtomicBool,
    /// Heavy (spawned) requests currently in flight; gates the `-32001` reject.
    inflight_heavy: AtomicU64,
    cursor_operations: Arc<super::cursor::Operations>,
    workbuddy_operations: Arc<super::workbuddy::Operations>,
    provider_operations: Arc<super::provider_operations::Operations>,
    maintenance: Arc<super::maintenance::Tasks>,
    subscription_speech_options: super::subscription_speech::Options,
    /// One large import at a time; saturation rejects before normalization.
    import_slots: Arc<tokio::sync::Semaphore>,
    /// Import publication and lazy session registration share identity admission.
    importing_threads: Mutex<HashSet<String>>,
    next_submission: AtomicU64,
    next_reverse: AtomicU64,
    out_tx: mpsc::Sender<Outgoing>,
    request_queues: super::request_serialization::RequestSerializationQueues,
    create_session: CreateSession,
    credential_store: Option<Arc<dyn CredentialStore>>,
    claude_usage_options: super::claude_usage::UsageOptions,
    lifecycle: Option<ProtocolLifecycle>,
    plugin_admin: Option<Arc<dyn ProtocolPluginAdmin>>,
    /// The engine's durable permission-rule store. Read when a session starts and
    /// appended to when an approval grants a rule beyond the session, so the rule
    /// has exactly one owner (see `permission::rule_store`).
    permission_rules_path: PathBuf,
    /// Serializes the rule store's read-modify-write: `add`/`remove` and the
    /// approval-grant persist all rewrite the whole file, so two that overlap
    /// (a rules-page edit racing an approval grant) would otherwise lose one.
    permission_rules_lock: tokio::sync::Mutex<()>,
    /// The engine's user config file — the User layer every session resolves
    /// over. Read and written here so a settings page and a session agree on one
    /// set of bytes (see `config_store`).
    config_path: PathBuf,
    /// Serializes config read-modify-write for the same reason the rule store
    /// lock exists: two overlapping batches would otherwise lose one.
    config_lock: tokio::sync::Mutex<()>,
    /// Serializes hooks-file writes, which are whole-file replacements.
    hooks_lock: tokio::sync::Mutex<()>,
    /// Ceiling for the inner init_session ACK wait inside `session/start`.
    startup_init_ack_budget: std::time::Duration,
}

impl AppServerState {
    #[cfg(test)]
    pub(super) fn queued_requests(&self, key: &str) -> Option<usize> {
        self.request_queues.queued(key)
    }

    #[cfg(test)]
    pub(super) fn outgoing_capacity(&self) -> usize {
        self.out_tx.capacity()
    }

    async fn send(&self, message: impl Into<Payload>) {
        let _ = self
            .out_tx
            .send(Outgoing {
                message: message.into(),
                cursor_operation: None,
                workbuddy_operation: None,
                provider_operation: None,
            })
            .await;
    }

    /// Send a notification unless the connection opted out of its method.
    async fn notify(&self, method: &str, params: Value) {
        if self.notification_opt_out.lock().unwrap().contains(method) {
            return;
        }
        self.send(wire::notification(method, params)).await;
    }

    /// Notify for one session, honoring its subscription state.
    async fn notify_session(&self, session_id: &str, method: &str, params: Value) {
        if self
            .unsubscribed_sessions
            .lock()
            .unwrap()
            .contains(session_id)
        {
            return;
        }
        self.notify(method, params).await;
    }

    /// Attach the stream cursor negotiated by the client, then honor the
    /// existing per-session notification gate. Every projection of one raw
    /// event carries the same revision so clients can deduplicate surfaces.
    async fn notify_session_revision(
        &self,
        session_id: &str,
        method: &str,
        mut params: Value,
        stream_id: &str,
        stream_revision: u64,
    ) {
        if self.revisioned_session_events.load(Ordering::Acquire) {
            if let Some(object) = params.as_object_mut() {
                object.insert("streamId".to_string(), Value::String(stream_id.to_string()));
                object.insert(
                    "streamRevision".to_string(),
                    Value::Number(stream_revision.into()),
                );
            }
        }
        self.notify_session(session_id, method, params).await;
    }

    async fn notify_runtime(&self, method: &str, params: RuntimeJson) {
        let suppressed = self.notification_opt_out.lock().unwrap().contains(method);
        if !suppressed {
            self.send(exact_payload::notification(method, params)).await;
        }
    }

    async fn notify_session_runtime(
        &self,
        session_id: &str,
        method: &str,
        mut params: RuntimeJson,
        stream: Option<(&str, u64)>,
    ) {
        if self
            .unsubscribed_sessions
            .lock()
            .unwrap()
            .contains(session_id)
        {
            return;
        }
        if self.revisioned_session_events.load(Ordering::Acquire) {
            if let (Some(object), Some((id, revision))) = (params.as_object_mut(), stream) {
                object.insert("streamId", id.into());
                object.insert("streamRevision", revision.into());
            }
        }
        self.notify_runtime(method, params).await;
    }

    fn next_submission_id(&self, prefix: &str) -> String {
        format!(
            "{prefix}_{}",
            self.next_submission.fetch_add(1, Ordering::Relaxed)
        )
    }

    fn session(&self, session_id: &str) -> Option<Arc<SessionHandle>> {
        self.sessions.lock().unwrap().get(session_id).cloned()
    }
}

/// Run one app-server connection over `input`/`output` until EOF. All sessions
/// created on the connection are shut down before this returns.
pub async fn run_app_server<I, O>(input: I, output: O, opts: AppServerOptions)
where
    I: AsyncRead + Unpin + Send + 'static,
    O: AsyncWrite + Unpin + Send + 'static,
{
    run_app_server_with_usage(input, output, opts, Default::default()).await;
}

pub(super) async fn run_app_server_with_usage<I, O>(
    input: I,
    output: O,
    opts: AppServerOptions,
    claude_usage_options: super::claude_usage::UsageOptions,
) where
    I: AsyncRead + Unpin + Send + 'static,
    O: AsyncWrite + Unpin + Send + 'static,
{
    run_app_server_with_provider_options(
        input,
        output,
        opts,
        claude_usage_options,
        Default::default(),
    )
    .await;
}

pub(super) async fn run_app_server_with_provider_options<I, O>(
    input: I,
    output: O,
    opts: AppServerOptions,
    claude_usage_options: super::claude_usage::UsageOptions,
    subscription_speech_options: super::subscription_speech::Options,
) where
    I: AsyncRead + Unpin + Send + 'static,
    O: AsyncWrite + Unpin + Send + 'static,
{
    run_app_server_inner(
        input,
        output,
        opts,
        claude_usage_options,
        subscription_speech_options,
        |_| {},
    )
    .await;
}

#[cfg(test)]
pub(super) async fn run_app_server_observed<I, O>(
    input: I,
    output: O,
    opts: AppServerOptions,
    observer: oneshot::Sender<Arc<AppServerState>>,
) where
    I: AsyncRead + Unpin + Send + 'static,
    O: AsyncWrite + Unpin + Send + 'static,
{
    run_app_server_inner(
        input,
        output,
        opts,
        Default::default(),
        Default::default(),
        move |state| {
            let _ = observer.send(state);
        },
    )
    .await;
}

async fn run_app_server_inner<I, O>(
    input: I,
    output: O,
    opts: AppServerOptions,
    claude_usage_options: super::claude_usage::UsageOptions,
    subscription_speech_options: super::subscription_speech::Options,
    on_state: impl FnOnce(Arc<AppServerState>) + Send + 'static,
) where
    I: AsyncRead + Unpin + Send + 'static,
    O: AsyncWrite + Unpin + Send + 'static,
{
    let (out_tx, out_rx) = mpsc::channel::<Outgoing>(OUTGOING_CAPACITY);
    let mut writer = tokio::spawn(write_app_output(output, out_rx));

    let maintenance = super::maintenance::Tasks::new(
        opts.lifecycle
            .as_ref()
            .and_then(|lifecycle| lifecycle.rollout_maintenance.clone()),
    );
    let config_path = opts
        .config_path
        .unwrap_or_else(config_store::user_config_path);
    let queue_store = super::queue_store::Store::new(
        model_sources::config_root_of(&config_path).join("thread-queue.sqlite3"),
    );
    let lifecycle = queue::wrap_lifecycle(opts.lifecycle, queue_store.clone());
    let state = Arc::new(AppServerState {
        connection_closed: Arc::new(AtomicBool::new(false)),
        windows_sandbox_context: opts
            .windows_sandbox_context
            .unwrap_or_default()
            .capture_cwd(),
        initialized: AtomicBool::new(false),
        realtime: realtime::RealtimeSessions::default(),
        dictation: dictation::DictationSessions::default(),
        sessions: Mutex::new(HashMap::new()),
        queue_revisions: Mutex::new(HashMap::new()),
        queue_store,
        control: Mutex::new(None),
        pending_reverse: Mutex::new(HashMap::new()),
        notification_opt_out: Mutex::new(HashSet::new()),
        unsubscribed_sessions: Mutex::new(HashSet::new()),
        session_subscriptions: Mutex::new(HashMap::new()),
        session_status: session_unload::SessionStatusRegistry::default(),
        pending_session_unloads: Mutex::new(HashSet::new()),
        unload_watchers: Mutex::new(HashMap::new()),
        product_events: AtomicBool::new(false),
        revisioned_session_events: AtomicBool::new(false),
        inflight_heavy: AtomicU64::new(0),
        cursor_operations: Arc::new(super::cursor::Operations::default()),
        workbuddy_operations: Arc::new(super::workbuddy::Operations::default()),
        provider_operations: Arc::new(super::provider_operations::Operations::default()),
        maintenance,
        subscription_speech_options,
        import_slots: Arc::new(tokio::sync::Semaphore::new(1)),
        importing_threads: Mutex::new(HashSet::new()),
        next_submission: AtomicU64::new(1),
        next_reverse: AtomicU64::new(1),
        out_tx,
        request_queues: Default::default(),
        create_session: opts.create_session,
        credential_store: opts.credential_store,
        claude_usage_options,
        lifecycle,
        plugin_admin: opts.plugin_admin,
        permission_rules_path: opts
            .permission_rules_path
            .unwrap_or_else(rule_store::default_rules_path),
        permission_rules_lock: tokio::sync::Mutex::new(()),
        config_path,
        config_lock: tokio::sync::Mutex::new(()),
        hooks_lock: tokio::sync::Mutex::new(()),
        startup_init_ack_budget: opts
            .startup_init_ack_budget
            .unwrap_or_else(|| std::time::Duration::from_secs(STARTUP_INIT_ACK_BUDGET_SECS)),
        session_unload_delay_override: opts.session_unload_delay,
    });

    on_state(state.clone());
    state.maintenance.start(state.config_path.clone());
    let queue_watcher = queue::watch(&state);
    let catalog_watcher = watch_model_catalogs(&state);
    let login_lifetime = super::antigravity::login::ConnectionGuard::new(
        Arc::as_ptr(&state) as usize,
        state.connection_closed.clone(),
    );
    let trae_login_lifetime =
        super::trae::login::ConnectionGuard::new(Arc::as_ptr(&state) as usize);
    let kiro_login_lifetime =
        super::kiro::login::ConnectionGuard::new(Arc::as_ptr(&state) as usize);
    let mut lines = BufReader::new(input).lines();
    let mut request_tails = FuturesUnordered::<BoxFuture<'static, ()>>::new();
    loop {
        let line = tokio::select! {
            biased;
            _ = state.out_tx.closed() => break,
            Some(()) = request_tails.next(), if !request_tails.is_empty() => continue,
            line = lines.next_line() => match line { Ok(Some(line)) => line, _ => break },
        };
        if line.trim().is_empty() {
            continue;
        }
        let incoming = match exact_requests::classify(&line) {
            Ok(incoming) => incoming,
            Err(error) => {
                let state = state.clone();
                admit_request_tail(
                    &mut request_tails,
                    Box::pin(async move {
                        state
                            .send(wire::error_response(
                                &Value::Null,
                                wire::PARSE_ERROR,
                                &format!("invalid JSON: {error}"),
                            ))
                            .await;
                    }),
                )
                .await;
                continue;
            }
        };
        match incoming {
            Incoming::Request { id, method, params } => {
                let mut scope = super::request_serialization::thread_scope(&method, &params);
                if method == "turn/interrupt"
                    && scope
                        .as_deref()
                        .is_some_and(|key| state.request_queues.control_can_bypass_preparation(key))
                {
                    scope = None;
                }
                let request: BoxFuture<'static, ()> =
                    Box::pin(handle_request(state.clone(), id, method, params));
                let ready = match scope {
                    Some(key) => state.request_queues.enqueue(key, request),
                    None => Some(request),
                };
                if let Some(request) = ready {
                    admit_request_tail(&mut request_tails, request).await;
                }
            }
            Incoming::Notification => {
                // `initialized` and any future client notifications are
                // accepted and ignored.
            }
            Incoming::Response { id, result } => {
                handle_reverse_response(&state, &id, result);
            }
            Incoming::Invalid => {
                let state = state.clone();
                admit_request_tail(
                    &mut request_tails,
                    Box::pin(async move {
                        state
                            .send(wire::error_response(
                                &Value::Null,
                                wire::INVALID_REQUEST,
                                "message is neither a request, notification nor response",
                            ))
                            .await;
                    }),
                )
                .await;
            }
        }
    }

    state.connection_closed.store(true, Ordering::Release);
    // The connection is gone: every session loses its subscriber, and every
    // idle-unload watcher must stop — a watcher holds endpoint state open,
    // and the drain below shuts every session down anyway.
    for tx in state.session_subscriptions.lock().unwrap().values() {
        let _ = tx.send(false);
    }
    for (_, cancel) in state.unload_watchers.lock().unwrap().drain() {
        let _ = cancel.send(());
    }
    let drain_deadline = tokio::time::Instant::now() + CONNECTION_DRAIN_BUDGET;
    // EOF closes input admission, but a half-closed client can still read every
    // accepted reply. Preserve those continuations within one connection budget.
    let _ = tokio::time::timeout_at(drain_deadline, async {
        while request_tails.next().await.is_some() {}
    })
    .await;
    drop(request_tails);
    drop(login_lifetime);
    drop(trae_login_lifetime);
    drop(kiro_login_lifetime);
    catalog_watcher.abort();
    let _ = catalog_watcher.await;
    queue_watcher.abort();
    let _ = queue_watcher.await;

    let cleanup = async move {
        state.cursor_operations.cancel_all();
        state.workbuddy_operations.cancel_all();
        state.provider_operations.cancel_all();
        state.maintenance.shutdown().await;
        realtime::stop_all(&state).await;
        dictation::stop_all(&state).await;
        let handles: Vec<Arc<SessionHandle>> = state
            .sessions
            .lock()
            .unwrap()
            .drain()
            .map(|(_, h)| h)
            .collect();
        for handle in handles {
            let _ = handle.ps.shutdown_and_wait().await;
        }
        let control = state.control.lock().unwrap().take();
        if let Some(control) = control {
            let _ = control.ps.shutdown_and_wait().await;
        }
        state.queue_store.close();
        drop(state);
    };
    tokio::pin!(cleanup);
    if tokio::time::timeout_at(drain_deadline, cleanup.as_mut())
        .await
        .is_err()
    {
        // Releasing the receiver wakes cleanup publishers without abandoning
        // their session shutdown futures at an arbitrary await boundary.
        writer.abort();
        let _ = (&mut writer).await;
        cleanup.await;
    } else if tokio::time::timeout_at(drain_deadline, &mut writer)
        .await
        .is_err()
    {
        writer.abort();
        let _ = writer.await;
    }
}

/// Run each request's synchronous admission before reading its successor. A full
/// output channel suspends only the already-admitted continuation, not controls.
async fn admit_request_tail(
    tails: &mut FuturesUnordered<BoxFuture<'static, ()>>,
    mut request: BoxFuture<'static, ()>,
) {
    if futures::poll!(request.as_mut()).is_pending() {
        tails.push(request);
    }
}

/// Read the original messages through store hooks only. In particular, the child
/// hook must not use the continuation loader's archive-recovery or writer locks.
/// Paging and detail knobs for a durable history read: opaque turn-id cursors, a
/// clamped page size, and a three-state item view.
/// A paging refusal is an invalid REQUEST, not an engine failure: the caller
/// named a cursor or view this thread cannot honor.
#[derive(Debug)]
struct HistoryPagingError(String);

impl std::fmt::Display for HistoryPagingError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for HistoryPagingError {}

#[derive(Debug, Clone, Default)]
struct HistoryPaging {
    cursor: Option<String>,
    limit: Option<usize>,
    items_view: Option<String>,
    sort_direction: Option<String>,
    /// Child transcripts stay whole: they are short by construction and their
    /// readers expect every row in one reply.
    whole: bool,
}

async fn read_thread_history(
    state: Arc<AppServerState>,
    id: Value,
    thread_id: String,
    agent_id: Option<String>,
    paging: HistoryPaging,
) {
    if thread_id.trim().is_empty() || agent_id.as_ref().is_some_and(|id| id.trim().is_empty()) {
        return send_invalid_params_message(&state, &id, "threadId and agentId must be nonempty")
            .await;
    }
    let result: Result<Payload, crate::lifecycle::BoxError> = async {
        let lifecycle = state
            .lifecycle
            .as_ref()
            .ok_or("history requires a session store")?;
        if agent_id.is_none() && !paging.whole {
            if let Some(hook) = lifecycle.read_thread_page.clone() {
                let request = klaus_agent_protocol::server::ThreadPageRequest {
                    cursor: paging.cursor.clone(),
                    limit: paging.limit,
                    items_view: paging.items_view.clone(),
                    sort_direction: paging.sort_direction.clone(),
                    turn_id: None,
                    before_item_id: None,
                };
                if let Some(snapshot) = hook(thread_id.clone(), request).await? {
                    let view = paging.items_view.clone().unwrap_or_else(|| "full".into());
                    return indexed_history_payload(&state, &thread_id, snapshot, &view);
                }
            }
        }
        let (items, meta, surfaced_tools) = if let Some(agent_id) = &agent_id {
            let read = lifecycle
                .read_subagent_history
                .as_ref()
                .ok_or("read-only child history is unavailable")?;
            let snapshot = read(thread_id.clone(), agent_id.clone()).await?;
            (
                snapshot.raw_items,
                Some(snapshot.meta),
                Some(snapshot.surfaced_tools),
            )
        } else {
            let read = lifecycle
                .read_thread
                .as_ref()
                .ok_or("read-only thread history is unavailable")?;
            let snapshot = read(thread_id.clone()).await?;
            if snapshot.meta.id != thread_id {
                return Err("thread history identity mismatch".into());
            }
            (snapshot.raw_items, None, None)
        };
        let audit_truncated = items.iter().any(|item| {
            matches!(item, klaus_agent_core::schemas::rollout::RolloutItem::Event(event)
                if event.event == "rollout_migration"
                    && event.payload.as_ref()
                        .and_then(|payload| payload.get("auditTruncated"))
                        .and_then(RuntimeJson::as_bool) == Some(true))
        });
        let mut history = klaus_agent_core::store::thread_history::project_thread_history(items);
        if agent_id.is_none() {
            if let Some(session) = state
                .session(&thread_id)
                .and_then(|handle| handle.ps.existing_session())
            {
                // Reuse only an already-loaded owner. Saved crash-recovery
                // inference must never mark its active lease interrupted.
                history.turn_index.merge_live(session.turn_index_snapshot());
            }
        }
        let mut reply = super::thread_read::exact_value(&history)?;
        // Page the projected rows by turn BEFORE any image substitution: the
        // page a client receives must be exactly the turns it asked for, and
        // the substitution below is per-row work a page must not pay for rows
        // it does not carry.
        // Rows come from the SAME encoding the unpaged reply used, so a page
        // carries exactly what a whole read carried (tool outputs merged into
        // their result rows included); paging only slices and re-shapes them.
        let rows = reply
            .get_mut("history")
            .and_then(RuntimeJson::as_array_mut)
            .map(std::mem::take)
            .unwrap_or_default();
        let display_source: Vec<_> = rows
            .iter()
            .filter(|row| {
                row.get("type")
                    .and_then(RuntimeJson::as_str)
                    .is_some_and(|kind| kind == "system")
            })
            .cloned()
            .collect();
        let (page, rows, shells) = super::thread_read::project_history_page(
            rows,
            &history,
            &thread_id,
            super::thread_read::PageWindow {
                cursor: paging.cursor.as_deref(),
                limit: paging.limit,
                items_view: paging.items_view.as_deref(),
                sort_direction: paging.sort_direction.as_deref(),
                whole: paging.whole,
            },
        )
        .map_err(|message| crate::lifecycle::BoxError::from(HistoryPagingError(message)))?;
        let presentations = super::thread_read::presentation_records(&display_source, &rows);
        let object = reply
            .as_object_mut()
            .ok_or("history codec did not return an object")?;
        object.insert("history", RuntimeJson::Array(rows));
        if !presentations.is_empty() {
            object.insert("presentationRecords", RuntimeJson::Array(presentations));
        }
        let mut fields = exact_payload::runtime_fields(reply);
        if audit_truncated {
            fields.push(("auditTruncated".into(), Payload::Scalar(true.into())));
        }
        fields.push(("page".into(), Payload::Scalar(serde_json::to_value(page)?)));
        fields.push(("turns".into(), Payload::Runtime(RuntimeJson::Array(shells))));
        fields.push(("threadId".into(), Payload::Scalar(thread_id.into())));
        if let Some(agent_id) = agent_id {
            fields.push(("agentId".into(), Payload::Scalar(agent_id.into())));
        }
        if let Some(meta) = meta {
            fields.push((
                "meta".into(),
                Payload::Runtime(super::thread_read::exact_value(&meta)?),
            ));
        }
        if let Some(tools) = surfaced_tools {
            fields.push((
                "surfacedTools".into(),
                Payload::Runtime(super::thread_read::exact_value(&tools)?),
            ));
        }
        Ok(Payload::Object(fields))
    }
    .await;
    match result {
        Ok(reply) => state.send(exact_payload::response(&id, reply)).await,
        Err(error) => {
            let code = if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound)
            {
                wire::UNKNOWN_SESSION
            } else if error.downcast_ref::<HistoryPagingError>().is_some()
                || error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|error| error.kind() == std::io::ErrorKind::InvalidInput)
            {
                wire::INVALID_PARAMS
            } else {
                wire::ENGINE_ERROR
            };
            state
                .send(wire::error_response(&id, code, &error.to_string()))
                .await;
        }
    }
}

/// Read directly through the store hook. No scope resolution or initialization
/// is allowed: a read of an archived or sleeping thread must leave it that way.
/// Page ONE turn's messages by message cursor, carrying no other turn's rows.
async fn read_thread_file_changes(state: Arc<AppServerState>, id: Value, thread_id: String) {
    if thread_id.trim().is_empty() {
        return send_invalid_params_message(&state, &id, "threadId must be nonempty").await;
    }
    let result: Result<Payload, crate::lifecycle::BoxError> = async {
        let hook = state
            .lifecycle
            .as_ref()
            .and_then(|lifecycle| lifecycle.read_thread_file_changes.clone())
            .ok_or("saved change metadata is unavailable")?;
        let changes = hook(thread_id.clone()).await?.ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotFound, "saved conversation not found")
        })?;
        Ok(Payload::Runtime(exact_payload::object([
            ("threadId", RuntimeJson::from(thread_id)),
            (
                "fileChanges",
                super::thread_read::file_changes_value(changes)?,
            ),
        ])))
    }
    .await;
    match result {
        Ok(reply) => state.send(exact_payload::response(&id, reply)).await,
        Err(error) => {
            state
                .send(wire::error_response(
                    &id,
                    if error
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound)
                    {
                        wire::UNKNOWN_SESSION
                    } else {
                        wire::ENGINE_ERROR
                    },
                    &error.to_string(),
                ))
                .await
        }
    }
}

async fn read_thread_items(
    state: Arc<AppServerState>,
    id: Value,
    params: wire::EngineReadThreadItemsParams,
) {
    if params.thread_id.trim().is_empty() || params.turn_id.trim().is_empty() {
        return send_invalid_params_message(&state, &id, "threadId and turnId must be nonempty")
            .await;
    }
    if params.cursor.is_some() && params.before_item_id.is_some() {
        return send_invalid_params_message(
            &state,
            &id,
            "cursor and beforeItemId cannot be combined",
        )
        .await;
    }
    if params
        .before_item_id
        .as_ref()
        .is_some_and(|anchor| anchor.trim().is_empty())
        || params.before_item_id.is_some()
            && params
                .sort_direction
                .as_deref()
                .is_some_and(|order| order != "desc")
    {
        return send_invalid_params_message(
            &state,
            &id,
            "beforeItemId requires a nonempty anchor and descending order",
        )
        .await;
    }
    let result: Result<Payload, crate::lifecycle::BoxError> = async {
        let lifecycle = state
            .lifecycle
            .as_ref()
            .ok_or("history requires a session store")?;
        let read = lifecycle
            .read_thread
            .as_ref()
            .ok_or("read-only thread history is unavailable")?;
        if let Some(hook) = lifecycle.read_thread_page.clone() {
            let request = klaus_agent_protocol::server::ThreadPageRequest {
                turn_id: Some(params.turn_id.clone()),
                before_item_id: params.before_item_id.clone(),
                cursor: params.cursor.clone(),
                limit: params.limit,
                sort_direction: params.sort_direction.clone(),
                ..Default::default()
            };
            if let Some(snapshot) = hook(params.thread_id.clone(), request).await? {
                return indexed_items_payload(&state, &params, snapshot);
            }
        }
        let snapshot = read(params.thread_id.clone()).await?;
        if snapshot.meta.id != params.thread_id {
            return Err("thread history identity mismatch".into());
        }
        let mut history =
            klaus_agent_core::store::thread_history::project_thread_history(snapshot.raw_items);
        if let Some(session) = state
            .session(&params.thread_id)
            .and_then(|handle| handle.ps.existing_session())
        {
            // Reuse only an already-loaded owner, exactly like the turn page.
            history.turn_index.merge_live(session.turn_index_snapshot());
        }
        let mut reply = super::thread_read::exact_value(&history)?;
        let rows = reply
            .get_mut("history")
            .and_then(RuntimeJson::as_array_mut)
            .map(std::mem::take)
            .unwrap_or_default();
        let display_source: Vec<_> = rows
            .iter()
            .filter(|row| {
                row.get("type")
                    .and_then(RuntimeJson::as_str)
                    .is_some_and(|kind| kind == "system")
            })
            .cloned()
            .collect();
        let (page, rows) = super::thread_read::project_turn_items_page(
            rows,
            &history,
            &params.thread_id,
            &params.turn_id,
            params
                .cursor
                .as_deref()
                .or(params.before_item_id.as_deref()),
            params.limit,
            params.sort_direction.as_deref(),
        )
        .map_err(|message| crate::lifecycle::BoxError::from(HistoryPagingError(message)))?;
        let presentations = super::thread_read::presentation_records(&display_source, &rows);
        super::thread_read::guard_presentation_page(&rows, &presentations)
            .map_err(|message| crate::lifecycle::BoxError::from(HistoryPagingError(message)))?;
        let object = reply
            .as_object_mut()
            .ok_or("history codec did not return an object")?;
        object.insert("history", RuntimeJson::Array(rows));
        if !presentations.is_empty() {
            object.insert("presentationRecords", RuntimeJson::Array(presentations));
        }
        let mut fields = exact_payload::runtime_fields(reply);
        fields.push(("page".into(), Payload::Scalar(serde_json::to_value(page)?)));
        fields.push(("threadId".into(), Payload::Scalar(params.thread_id.into())));
        fields.push(("turnId".into(), Payload::Scalar(params.turn_id.into())));
        Ok(Payload::Object(fields))
    }
    .await;
    match result {
        Ok(reply) => state.send(exact_payload::response(&id, reply)).await,
        Err(error) => {
            let code = if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound)
            {
                wire::UNKNOWN_SESSION
            } else if error.downcast_ref::<HistoryPagingError>().is_some()
                || error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|error| error.kind() == std::io::ErrorKind::InvalidInput)
            {
                wire::INVALID_PARAMS
            } else {
                wire::ENGINE_ERROR
            };
            state
                .send(wire::error_response(&id, code, &error.to_string()))
                .await
        }
    }
}

/// Serve a turn window from the store's durable turn index: the slice is
/// projected, the view applied, and the page envelope plus turn shells come
/// straight from the index, so the reply costs the window instead of the file.
fn indexed_history_payload(
    state: &Arc<AppServerState>,
    thread_id: &str,
    mut snapshot: klaus_agent_core::store::thread_index::ThreadPageSnapshot,
    view: &str,
) -> Result<Payload, crate::lifecycle::BoxError> {
    if let Some(session) = state
        .session(thread_id)
        .and_then(|handle| handle.ps.existing_session())
    {
        snapshot
            .history
            .turn_index
            .merge_live(session.turn_index_snapshot());
    }
    let mut reply = super::thread_read::exact_value(&snapshot.history)?;
    let rows = reply
        .get_mut("history")
        .and_then(RuntimeJson::as_array_mut)
        .map(std::mem::take)
        .unwrap_or_default();
    let projected =
        super::thread_read::project_indexed_rows(&snapshot.history, thread_id, rows, view)
            .map_err(|message| crate::lifecycle::BoxError::from(HistoryPagingError(message)))?;
    let object = reply
        .as_object_mut()
        .ok_or("history codec did not return an object")?;
    object.insert("history", RuntimeJson::Array(projected));
    if !snapshot.presentation_records.is_empty() {
        object.insert(
            "presentationRecords",
            RuntimeJson::Array(snapshot.presentation_records),
        );
    }
    let mut fields = exact_payload::runtime_fields(reply);
    fields.push(("supportsItemAnchors".into(), Payload::Scalar(true.into())));
    fields.push((
        "fileChanges".into(),
        Payload::Runtime(super::thread_read::file_changes_value(
            snapshot.file_changes,
        )?),
    ));
    fields.push((
        "page".into(),
        Payload::Scalar(serde_json::to_value(&snapshot.page)?),
    ));
    fields.push((
        "turns".into(),
        Payload::Scalar(serde_json::to_value(&snapshot.shells)?),
    ));
    fields.push((
        "threadId".into(),
        Payload::Scalar(thread_id.to_string().into()),
    ));
    Ok(Payload::Object(fields))
}

/// Serve one turn's message page from the store's durable turn index.
fn indexed_items_payload(
    state: &Arc<AppServerState>,
    params: &wire::EngineReadThreadItemsParams,
    mut snapshot: klaus_agent_core::store::thread_index::ThreadPageSnapshot,
) -> Result<Payload, crate::lifecycle::BoxError> {
    if let Some(session) = state
        .session(&params.thread_id)
        .and_then(|handle| handle.ps.existing_session())
    {
        snapshot
            .history
            .turn_index
            .merge_live(session.turn_index_snapshot());
    }
    let mut reply = super::thread_read::exact_value(&snapshot.history)?;
    let rows = reply
        .get_mut("history")
        .and_then(RuntimeJson::as_array_mut)
        .map(std::mem::take)
        .unwrap_or_default();
    let rows = super::thread_read::project_indexed_items_rows(rows)
        .map_err(|message| crate::lifecycle::BoxError::from(HistoryPagingError(message)))?;
    super::thread_read::guard_presentation_page(&rows, &snapshot.presentation_records)
        .map_err(|message| crate::lifecycle::BoxError::from(HistoryPagingError(message)))?;
    let object = reply
        .as_object_mut()
        .ok_or("history codec did not return an object")?;
    object.insert("history", RuntimeJson::Array(rows));
    if !snapshot.presentation_records.is_empty() {
        object.insert(
            "presentationRecords",
            RuntimeJson::Array(snapshot.presentation_records),
        );
    }
    let mut fields = exact_payload::runtime_fields(reply);
    fields.push(("supportsItemAnchors".into(), Payload::Scalar(true.into())));
    fields.push((
        "fileChanges".into(),
        Payload::Runtime(super::thread_read::file_changes_value(
            snapshot.file_changes,
        )?),
    ));
    fields.push((
        "page".into(),
        Payload::Scalar(serde_json::to_value(snapshot.page)?),
    ));
    fields.push((
        "threadId".into(),
        Payload::Scalar(params.thread_id.clone().into()),
    ));
    fields.push((
        "turnId".into(),
        Payload::Scalar(params.turn_id.clone().into()),
    ));
    Ok(Payload::Object(fields))
}

async fn read_thread(state: Arc<AppServerState>, id: Value, params: wire::EngineReadThreadParams) {
    let handle = state.session(&params.thread_id);
    let live = handle
        .as_ref()
        .and_then(|handle| handle.ps.existing_session());
    // The store snapshot is strictly read-only. Accepted in-memory messages and
    // streaming items are overlaid below; even an existing writer is untouched.
    let read = state
        .lifecycle
        .as_ref()
        .and_then(|lifecycle| lifecycle.read_thread.clone());
    let has_store_reader = read.is_some();
    let snapshot_result = match read {
        Some(read) => read(params.thread_id.clone()).await,
        None => Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "thread has no persistent store",
        )) as crate::lifecycle::BoxError),
    };
    let snapshot = match snapshot_result {
        Ok(snapshot) => snapshot,
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound)
                && live.is_none()
                && handle.as_ref().is_some_and(|handle| {
                    handle.read_seed.is_some() && !handle.read_items.lock().unwrap().ever_started
                }) =>
        {
            handle.as_ref().unwrap().read_seed.clone().unwrap()
        }
        Err(error)
            if live.as_ref().is_some_and(|session| {
                error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound)
                    && (!has_store_reader
                        || (session.visible_history().is_empty()
                            && session.turn_index_snapshot().turn_started_at_ms.is_empty()))
            }) =>
        {
            let session = live.as_ref().unwrap();
            klaus_agent_core::store::session_store::ThreadReadSnapshot {
                meta: session.meta.clone(),
                raw_items: Vec::new(),
                updated_at: session.meta.created_at,
                archived: false,
            }
        }
        Err(error) => {
            let code = if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound)
            {
                wire::UNKNOWN_SESSION
            } else {
                wire::ENGINE_ERROR
            };
            return state
                .send(wire::error_response(&id, code, &error.to_string()))
                .await;
        }
    };
    let submissions = handle
        .as_ref()
        .map(|handle| handle.turns.lock().unwrap().clone())
        .unwrap_or_default();
    let (active, items) = handle
        .as_ref()
        .map(|handle| {
            let items = handle.read_items.lock().unwrap();
            let active = submissions
                .iter()
                .filter_map(|id| items.submission_turns.get(id).cloned())
                .collect();
            (active, items.snapshot())
        })
        .unwrap_or_default();
    let mut flags = Vec::new();
    for request in state
        .pending_reverse
        .lock()
        .unwrap()
        .values()
        .filter(|request| request.session_id == params.thread_id)
    {
        let flag = match request.kind {
            ReverseKind::Approval { .. } => "waitingOnApproval",
            ReverseKind::Elicitation { .. } => "waitingOnUserInput",
        };
        if !flags.iter().any(|existing| existing == flag) {
            flags.push(flag.to_string());
        }
    }
    match super::thread_read::project(
        snapshot,
        &params,
        live.as_deref(),
        &active,
        !submissions.is_empty(),
        flags,
        &items,
    ) {
        Ok(result) => state.send(exact_payload::response(&id, result)).await,
        Err(error) => {
            state
                .send(wire::error_response(&id, wire::INVALID_PARAMS, &error))
                .await
        }
    }
}

/// Build (once) the endpoint-level inner `ProtocolServer` behind `engine/op`.
/// It shares lifecycle/plugin_admin with the session servers; its event lines
/// stream out verbatim as `engine/event` notifications.
fn ensure_control_handle(state: &Arc<AppServerState>) -> Arc<SessionHandle> {
    if let Some(handle) = state.control.lock().unwrap().clone() {
        return handle;
    }
    let (host_side, engine_side) = tokio::io::duplex(SESSION_PIPE_CAPACITY);
    let (engine_read, engine_write) = tokio::io::split(engine_side);
    let (host_read, mut host_write) = tokio::io::split(host_side);
    // engine/op never sends init_session, so this factory is unreachable; a
    // stray init on the control channel fails loudly instead of binding.
    let create: CreateSessionFn = Arc::new(|_h: SessionHandlers| {
        Box::pin(async { Err("engine/op control channel does not bind sessions".into()) })
            as CreateSessionFuture
    });
    let ps = Arc::new(ProtocolServer::new(ProtocolServerOptions {
        create_session: create,
        lifecycle: state.lifecycle.clone(),
        plugin_admin: state.plugin_admin.clone(),
        credential_store: state.credential_store.clone(),
        input: Box::new(engine_read),
        output: Box::new(engine_write),
    }));
    ps.install_provider_decorator(model_sources::provider_decorator(
        &model_sources::config_root_of(&state.config_path),
    ));
    tokio::spawn({
        let ps = ps.clone();
        async move { ps.start().await }
    });
    let (op_tx, mut op_rx) = mpsc::unbounded_channel::<Payload>();
    tokio::spawn(async move {
        while let Some(op) = op_rx.recv().await {
            let mut line = match op.bytes() {
                Ok(line) => line,
                Err(error) => {
                    eprintln!("[app-server] session operation encoding failed: {error}");
                    break;
                }
            };
            line.push(b'\n');
            if host_write.write_all(&line).await.is_err() {
                break;
            }
            let _ = host_write.flush().await;
        }
    });
    let handle = Arc::new(SessionHandle {
        ps,
        op_tx,
        turns: Arc::new(Mutex::new(std::collections::HashSet::new())),
        waiters: Arc::new(Mutex::new(HashMap::new())),
        ordered_responses: Mutex::new(HashMap::new()),
        interrupt_changes: Mutex::new(HashMap::new()),
        event_stream_closed: AtomicBool::new(false),
        realtime_events: broadcast::channel(512).0,
        read_items: Mutex::new(super::thread_read::LiveItems::default()),
        stream: Mutex::new(SessionStream::new()),
        read_seed: None,
    });
    *state.control.lock().unwrap() = Some(handle.clone());
    tokio::spawn({
        let state = state.clone();
        let handle = handle.clone();
        async move {
            let mut lines = BufReader::new(host_read).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if line.trim().is_empty() {
                    continue;
                }
                let Ok(event) = exact_payload::parse(&line) else {
                    continue;
                };
                // Promoted engine-level requests (engine/listSessions, ...)
                // park a waiter; its terminal reply must settle the request
                // instead of streaming out as engine/event.
                if let Some(deleted) = event
                    .get("deleted")
                    .and_then(|value| exact_payload::scalar(value).ok())
                {
                    notify_thread_deleted(&state, &serde_json::json!({"deleted":deleted})).await;
                }
                if claim_waiter(&handle, &event).await {
                    continue;
                }
                state
                    .notify_runtime("engine/event", exact_payload::object([("event", event)]))
                    .await;
            }
            handle.close_waiters().await;
        }
    });
    handle
}

/// Broadcast `thread/deleted` for a committed delete reply. Every connection
/// learns the thread is gone at commit time; the requester therefore never has
/// to deduce the outcome from its own reply, and observers keep an accurate
/// session list without polling. Only a successful delete is announced.
async fn notify_thread_deleted(state: &Arc<AppServerState>, event: &Value) {
    let Some(deleted) = event.get("deleted") else {
        return;
    };
    if !deleted
        .get("deleted")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return;
    }
    let Some(thread_id) = deleted.get("threadId").and_then(Value::as_str) else {
        return;
    };
    queue::notify_deleted(state, thread_id).await;
    state
        .notify(
            "thread/deleted",
            serde_json::to_value(wire::ThreadDeletedNotification {
                thread_id: thread_id.to_string(),
            })
            .unwrap(),
        )
        .await;
}

fn watch_model_catalogs(state: &Arc<AppServerState>) -> tokio::task::JoinHandle<()> {
    let weak = Arc::downgrade(state);
    let mut receiver = model_sources::subscribe_catalog_changes();
    tokio::spawn(async move {
        loop {
            let change = match receiver.recv().await {
                Ok(change) => change,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
            };
            let Some(state) = weak.upgrade() else {
                return;
            };
            if !state.initialized.load(Ordering::Acquire)
                || model_sources::config_root_of(&state.config_path) != change.config_root
            {
                continue;
            }
            state
                .notify(
                    "model/catalogChanged",
                    serde_json::to_value(wire::ModelCatalogChangedNotification {
                        source: change.source,
                    })
                    .unwrap(),
                )
                .await;
        }
    })
}

/// Settle the parked waiter for this envelope's submission id, if any. Stream
/// events (`{ event: ... }`) are never terminal replies, so they pass through.
async fn claim_waiter(handle: &Arc<SessionHandle>, event: &RuntimeJson) -> bool {
    if event.get("event").is_some() {
        return false;
    }
    let Some(submission_id) =
        exact_text(exact_field(event, "submissionId")).filter(|id| !id.is_empty())
    else {
        return false;
    };
    let response = handle
        .ordered_responses
        .lock()
        .unwrap()
        .remove(&submission_id);
    if let Some(response) = response {
        let preserve_stream = matches!(response.kind, OrderedResponseKind::RuntimeConfig);
        if let Some(state) = response.state.upgrade() {
            let mut reply = event.clone();
            if let Some(object) = reply.as_object_mut() {
                object.remove("submissionId");
            }
            if let Some(error) = reply.get("error").filter(|error| error.is_string()) {
                let mut fields = vec![
                    ("code".into(), Payload::Scalar(wire::ENGINE_ERROR.into())),
                    ("message".into(), Payload::Runtime(error.clone())),
                ];
                if let Some(detail) = reply.get("errorDetail") {
                    fields.push(("data".into(), Payload::Runtime(detail.clone())));
                }
                state
                    .send(Payload::Object(vec![
                        ("id".into(), Payload::Scalar(response.id)),
                        ("error".into(), Payload::Object(fields)),
                    ]))
                    .await;
                return true;
            }
            let result = if preserve_stream {
                exact_payload::object([("submissionId", submission_id.clone().into())])
            } else {
                reply
            };
            // The sole pump publishes this before subsequent state notifications.
            state
                .send(exact_payload::response(&response.id, result))
                .await;
        }
        return !preserve_stream;
    }
    let waiter = handle.waiters.lock().unwrap().remove(&submission_id);
    if let Some(waiter) = waiter {
        let stream_revision = handle.stream.lock().unwrap().revision;
        let mut covered_tool_results = Vec::new();
        let mut covered_message_ids = Vec::new();
        let mut seen = HashSet::new();
        if let Some(history) = event.get("history").and_then(RuntimeJson::as_array) {
            for message in history {
                let Some(uuid) = exact_text(exact_field(message, "uuid")) else {
                    continue;
                };
                if exact_text(exact_field(message, "type")).as_deref() == Some("tool_result") {
                    if let (Some(tool_id), Some(turn_id)) = (
                        exact_text(exact_field(message, "toolUseId")),
                        exact_text(exact_field(exact_field(event, "historyTurnIds"), &uuid)),
                    ) {
                        covered_tool_results.push(exact_payload::object([
                            ("turnId", turn_id.into()),
                            ("toolUseId", tool_id.into()),
                        ]));
                    }
                }
                let mut ids = vec![uuid.clone()];
                if let Some(blocks) = message.get("content").and_then(RuntimeJson::as_array) {
                    for (position, block) in blocks.iter().enumerate() {
                        let id = match exact_text(exact_field(block, "type")).as_deref() {
                            Some("text") => exact_text(exact_field(block, "messageId")),
                            Some("thinking") => Some(
                                exact_text(exact_field(block, "messageId"))
                                    .unwrap_or_else(|| format!("{uuid}-{position}")),
                            ),
                            Some("tool_use") => exact_text(exact_field(block, "id")),
                            _ => None,
                        };
                        ids.extend(id);
                    }
                }
                for id in ids {
                    if seen.insert(id.clone()) {
                        covered_message_ids.push(id);
                    }
                }
            }
        }
        let active_items = if event.get("history").is_some() {
            let snapshot = handle.read_items.lock().unwrap().snapshot();
            let mut turns = snapshot.iter().collect::<Vec<_>>();
            turns.sort_by_key(|(id, _)| *id);
            turns
                .into_iter()
                .flat_map(|(id, items)| {
                    items.iter().map(move |item| {
                        exact_payload::object([
                            ("turnId", id.clone().into()),
                            ("item", item.clone()),
                        ])
                    })
                })
                .collect()
        } else {
            Vec::new()
        };
        let _ = waiter.send(WaiterReply {
            value: event.clone(),
            stream_revision,
            active_items,
            covered_message_ids,
            covered_tool_results,
        });
        true
    } else {
        false
    }
}

/// Deferred post-reply action run after a promoted request's terminal reply
/// has been sent (e.g. session teardown after `session/shutdown`).
type PromotedReplyAction = Box<dyn FnOnce(&Arc<AppServerState>) + Send>;

/// Forward one promoted request through a session/control handle: park a
/// waiter, feed the op, and answer the JSON-RPC request with the terminal
/// reply envelope minus `submissionId`. Engine error replies pass through
/// verbatim (`error` / `errorDetail` keys) so clients see the exact native
/// stdio reply shape. A client-supplied submission id becomes the inner
/// correlation id verbatim (so `cancel_request` keeps working); it must not
/// collide with anything in flight.
async fn forward_promoted_request(
    state: Arc<AppServerState>,
    handle: Arc<SessionHandle>,
    id: Value,
    op: impl Into<Payload>,
    requested_submission_id: Option<String>,
    on_reply: Option<PromotedReplyAction>,
) {
    let op = match op.into() {
        Payload::Scalar(value) => exact_payload::runtime(value),
        Payload::Runtime(value) => value,
        Payload::Object(_) | Payload::Encoded(_) => {
            unreachable!("op queue accepts validated scalar or native runtime operations")
        }
    };
    let op_type = exact_text(exact_field(&op, "type"));
    let approval_override = op_type.as_deref() == Some("approve_approval_review_denial");
    let prediction_key = match op_type.as_deref() {
        Some("predict_message") => Some("prediction"),
        Some("prediction_context") => Some("predictionContext"),
        _ => None,
    };
    let submission_id = match requested_submission_id {
        Some(requested) => {
            if requested.is_empty() {
                return state
                    .send(wire::error_response(
                        &id,
                        wire::INVALID_PARAMS,
                        "submissionId must not be empty",
                    ))
                    .await;
            }
            let collides = handle.waiters.lock().unwrap().contains_key(&requested)
                || handle
                    .ordered_responses
                    .lock()
                    .unwrap()
                    .contains_key(&requested)
                || handle.turns.lock().unwrap().contains(&requested);
            if collides {
                return state
                    .send(wire::error_response(
                        &id,
                        wire::DUPLICATE_SUBMISSION,
                        &format!("submissionId already in flight: {requested}"),
                    ))
                    .await;
            }
            requested
        }
        None => state.next_submission_id("req"),
    };
    if matches!(
        op_type.as_deref(),
        Some("get_goal" | "set_goal" | "clear_goal")
    ) {
        handle.ordered_responses.lock().unwrap().insert(
            submission_id.clone(),
            OrderedResponse {
                state: Arc::downgrade(&state),
                id: id.clone(),
                kind: OrderedResponseKind::Goal,
            },
        );
        if handle
            .op_tx
            .send(Payload::Runtime(exact_payload::object([
                ("id", submission_id.clone().into()),
                ("op", op),
            ])))
            .is_err()
        {
            handle
                .ordered_responses
                .lock()
                .unwrap()
                .remove(&submission_id);
            state
                .send(wire::error_response(
                    &id,
                    wire::ENGINE_ERROR,
                    "session terminated before the goal request was accepted",
                ))
                .await;
        }
        return;
    }
    let (reply_tx, reply_rx) = oneshot::channel::<WaiterReply>();
    handle.register_waiter(submission_id.clone(), reply_tx);
    let _ = handle.op_tx.send(Payload::Runtime(exact_payload::object([
        ("id", submission_id.clone().into()),
        ("op", op),
    ])));
    tokio::spawn(async move {
        match reply_rx.await {
            Ok(reply) => {
                let mut reply = reply.value;
                if let Some(reply_object) = reply.as_object_mut() {
                    reply_object.remove("submissionId");
                }
                if let Some(on_reply) = on_reply {
                    on_reply(&state);
                }
                if let Some(key) = prediction_key {
                    if let Some(result) = reply.get(key) {
                        reply = result.clone();
                    } else {
                        let message = reply
                            .get("error")
                            .and_then(RuntimeJson::as_string)
                            .cloned()
                            .unwrap_or_else(|| "prediction request ended without a result".into());
                        state
                            .send(wire::error_response(
                                &id,
                                wire::ENGINE_ERROR,
                                &message.try_to_utf8().unwrap_or_else(|_| {
                                    "prediction request ended without a scalar error".into()
                                }),
                            ))
                            .await;
                        return;
                    }
                }
                if approval_override
                    && reply
                        .get("approvalReviewDenialApproved")
                        .and_then(RuntimeJson::as_bool)
                        == Some(true)
                {
                    reply = exact_payload::object([] as [(&str, RuntimeJson); 0]);
                }
                state.send(exact_payload::response(&id, reply)).await;
            }
            Err(_) => {
                state
                    .send(wire::error_response(
                        &id,
                        wire::ENGINE_ERROR,
                        "session terminated before the request completed",
                    ))
                    .await;
            }
        }
    });
}

/// Ask the inner server for history and bind that ordered reply to the exact
/// live-stream revision observed at the same point in the event pump. Events
/// emitted after the cut remain available through the normal notification
/// stream and the retained replay window.
async fn forward_follow_snapshot(
    state: Arc<AppServerState>,
    handle: Arc<SessionHandle>,
    id: Value,
    params: wire::SessionFollowParams,
) {
    let submission_id = match params.submission_id {
        Some(requested) => {
            if requested.is_empty() {
                return state
                    .send(wire::error_response(
                        &id,
                        wire::INVALID_PARAMS,
                        "submissionId must not be empty",
                    ))
                    .await;
            }
            let collides = handle.waiters.lock().unwrap().contains_key(&requested)
                || handle
                    .ordered_responses
                    .lock()
                    .unwrap()
                    .contains_key(&requested)
                || handle.turns.lock().unwrap().contains(&requested);
            if collides {
                return state
                    .send(wire::error_response(
                        &id,
                        wire::DUPLICATE_SUBMISSION,
                        &format!("submissionId already in flight: {requested}"),
                    ))
                    .await;
            }
            requested
        }
        None => state.next_submission_id("follow"),
    };
    let (reply_tx, reply_rx) = oneshot::channel::<WaiterReply>();
    handle.register_waiter(submission_id.clone(), reply_tx);
    if handle
        .op_tx
        .send(
            serde_json::json!({
                "id": submission_id,
                "op": { "type": if params.include_original_history == Some(true) {
                    "get_original_history"
                } else {
                    "get_history"
                } }
            })
            .into(),
        )
        .is_err()
    {
        handle.waiters.lock().unwrap().remove(&submission_id);
        state
            .send(wire::error_response(
                &id,
                wire::ENGINE_ERROR,
                "session terminated before the follow snapshot completed",
            ))
            .await;
        return;
    }
    tokio::spawn(async move {
        match reply_rx.await {
            Ok(mut reply) => {
                if let Some(object) = reply.value.as_object_mut() {
                    object.remove("submissionId");
                    if params.include_original_history == Some(true) {
                        object.insert("activeItems", RuntimeJson::Array(reply.active_items));
                        object.insert(
                            "coveredMessageIds",
                            RuntimeJson::Array(
                                reply
                                    .covered_message_ids
                                    .into_iter()
                                    .map(RuntimeJson::from)
                                    .collect(),
                            ),
                        );
                        object.insert(
                            "coveredToolResults",
                            RuntimeJson::Array(reply.covered_tool_results),
                        );
                    }
                }
                let result = {
                    let stream = handle.stream.lock().unwrap();
                    exact_payload::object([
                        ("sessionId", params.session_id.into()),
                        ("streamId", stream.id.clone().into()),
                        ("revision", reply.stream_revision.into()),
                        ("mode", "snapshot".into()),
                        ("snapshot", reply.value),
                        (
                            "availableFromRevision",
                            stream.available_from_revision().into(),
                        ),
                    ])
                };
                state.send(exact_payload::response(&id, result)).await;
            }
            Err(_) => {
                state
                    .send(wire::error_response(
                        &id,
                        wire::ENGINE_ERROR,
                        "session terminated before the follow snapshot completed",
                    ))
                    .await
            }
        }
    });
}

/// Resolve the handle one promoted request targets: an explicit sessionId
/// addresses that bound session (unknown -> error reply), an absent one
/// addresses the endpoint-scoped control session — the channel lifecycle and
/// listing ops ride before any session is bound.
async fn resolve_scope_handle(
    state: &Arc<AppServerState>,
    id: &Value,
    session_id: Option<&str>,
) -> Option<Arc<SessionHandle>> {
    match session_id {
        Some(session_id) => match state.session(session_id) {
            Some(handle) => Some(handle),
            None => {
                send_unknown_session(state, id, session_id).await;
                None
            }
        },
        None => Some(ensure_control_handle(state)),
    }
}

/// Feed one op through a session handle with ack-reply semantics: the
/// JSON-RPC result is the `SubmissionAck`, and every engine reply envelope
/// keeps streaming on `session/rawEvent` correlated by the submission id.
/// This is the promoted shape for write/long-running ops whose replies are
/// not a single terminal envelope (same contract as `turn/start`).
async fn feed_acked_op(
    state: &Arc<AppServerState>,
    id: &Value,
    session_id: Option<&str>,
    requested_submission_id: Option<String>,
    op: impl Into<Payload>,
) {
    let op = match op.into() {
        Payload::Scalar(value) => exact_payload::runtime(value),
        Payload::Runtime(value) => value,
        Payload::Object(_) | Payload::Encoded(_) => {
            unreachable!("an internal op is a validated JSON object")
        }
    };
    let Some(handle) = resolve_scope_handle(state, id, session_id).await else {
        return;
    };
    let submission_id = match requested_submission_id {
        Some(requested) => {
            if requested.is_empty() {
                return state
                    .send(wire::error_response(
                        id,
                        wire::INVALID_PARAMS,
                        "submissionId must not be empty",
                    ))
                    .await;
            }
            let collides = handle.waiters.lock().unwrap().contains_key(&requested)
                || handle
                    .ordered_responses
                    .lock()
                    .unwrap()
                    .contains_key(&requested)
                || handle.turns.lock().unwrap().contains(&requested);
            if collides {
                return state
                    .send(wire::error_response(
                        id,
                        wire::DUPLICATE_SUBMISSION,
                        &format!("submissionId already in flight: {requested}"),
                    ))
                    .await;
            }
            requested
        }
        None => state.next_submission_id("sub"),
    };
    let op_type = exact_text(exact_field(&op, "type"));
    let config_update = exact_field(&op, "update");
    // Legacy model/permission updates acknowledge enqueueing so a caller can
    // release work ahead of its serial barrier. Live controls settle on apply.
    let wait_for_apply = op_type.as_deref() == Some("set_runtime_policy")
        || (op_type.as_deref() == Some("update_session_config")
            && [
                "goal",
                "maximumGoalTokenBudget",
                "modelVerbosity",
                "reasoningSummary",
                "webSearchAccess",
                "approvalReview",
                "subagentModelCatalog",
                "subagentModelOptions",
            ]
            .iter()
            .any(|key| config_update.get(*key).is_some()));
    if wait_for_apply {
        handle.ordered_responses.lock().unwrap().insert(
            submission_id.clone(),
            OrderedResponse {
                state: Arc::downgrade(state),
                id: id.clone(),
                kind: OrderedResponseKind::RuntimeConfig,
            },
        );
        if handle
            .op_tx
            .send(exact_payload::object([("id", submission_id.clone().into()), ("op", op)]).into())
            .is_err()
        {
            handle
                .ordered_responses
                .lock()
                .unwrap()
                .remove(&submission_id);
            state
                .send(wire::error_response(
                    id,
                    wire::ENGINE_ERROR,
                    "session terminated before the runtime configuration settled",
                ))
                .await;
        }
        return;
    }
    let _ = handle
        .op_tx
        .send(exact_payload::object([("id", submission_id.clone().into()), ("op", op)]).into());
    let result = wire::SubmissionAck { submission_id };
    state
        .send(wire::response(id, serde_json::to_value(result).unwrap()))
        .await;
}

/// Every named turn entry uses the same reservation, transport and ACK path.
async fn start_named_turn(
    state: &Arc<AppServerState>,
    id: &Value,
    session_id: &str,
    submission: Option<String>,
    op: impl Into<Payload>,
) {
    let Some(handle) = state.session(session_id) else {
        return send_unknown_session(state, id, session_id).await;
    };
    // Admitting a turn re-arms the event stream, so a headless turn on an
    // unwatched session still reaches the client.
    session_unload::rearm_subscription(state, session_id, &handle);
    let turn_id = match submission {
        Some(requested) => {
            if requested.is_empty() {
                return send_invalid_params_message(state, id, "submissionId must not be empty")
                    .await;
            }
            if handle.waiters.lock().unwrap().contains_key(&requested)
                || handle.turns.lock().unwrap().contains(&requested)
            {
                return state
                    .send(wire::error_response(
                        id,
                        wire::DUPLICATE_SUBMISSION,
                        &format!("submissionId already in flight: {requested}"),
                    ))
                    .await;
            }
            requested
        }
        None => state.next_submission_id("turn"),
    };
    if let Err(error) = handle.admit_turn(&turn_id, op) {
        return state
            .send(wire::error_response(id, wire::ENGINE_ERROR, error))
            .await;
    }
    session_unload::refresh_status(state, session_id).await;
    state
        .send(wire::response(
            id,
            serde_json::to_value(wire::TurnStartResult { turn_id }).expect("turn acknowledgement"),
        ))
        .await;
}

async fn submit_named_turn(state: Arc<AppServerState>, id: Value, params: wire::TurnSubmitParams) {
    use klaus_agent_protocol::server::{
        TurnSubmission, TurnSubmissionKind, TurnSubmissionMode, TurnSubmissionOutcome,
        TurnSubmissionRejection,
    };
    let Some(handle) = state.session(&params.session_id) else {
        return send_unknown_session(&state, &id, &params.session_id).await;
    };
    // Admitting a turn re-arms the event stream, so a headless turn on an
    // unwatched session still reaches the client.
    session_unload::rearm_subscription(&state, &params.session_id, &handle);
    let submission_id = params
        .submission_id
        .clone()
        .unwrap_or_else(|| state.next_submission_id("turn"));
    let invalid = |message: String| wire::TurnSubmitResult {
        status: wire::TurnSubmitStatus::NotSubmitted,
        submission_id: submission_id.clone(),
        turn_id: None,
        reason: Some(wire::TurnSubmitReason {
            code: wire::TurnSubmitReasonCode::InvalidInput,
            message,
            active_turn_id: None,
            turn_kind: None,
        }),
    };
    let mut op = exact_payload::object([("type", "run".into()), ("input", params.input.0)]);
    if let Some(options) = params.options {
        op.as_object_mut().unwrap().insert("options", options.0);
    }
    let decoded = klaus_agent_core::runtime_values::runtime_codec::decode_value::<
        klaus_agent_protocol::types::OpKind,
    >(&op);
    let (input, options) = match decoded {
        Ok(klaus_agent_protocol::types::OpKind::Run { input, options }) => (input, options),
        Ok(_) => unreachable!("run decoder returns a run operation"),
        Err(error) => {
            return state
                .send(wire::response(
                    &id,
                    serde_json::to_value(invalid(error.to_string())).unwrap(),
                ))
                .await;
        }
    };
    let delivery = match params.delivery {
        Some(value) => {
            match klaus_agent_core::runtime_values::runtime_codec::decode_value(&value.into()) {
                Ok(value) => Some(value),
                Err(error) => {
                    return state
                        .send(wire::response(
                            &id,
                            serde_json::to_value(invalid(error.to_string())).unwrap(),
                        ))
                        .await
                }
            }
        }
        None => None,
    };
    let duplicate = {
        let mut turns = handle.turns.lock().unwrap();
        handle.waiters.lock().unwrap().contains_key(&submission_id)
            || handle
                .ordered_responses
                .lock()
                .unwrap()
                .contains_key(&submission_id)
            || !turns.insert(submission_id.clone())
    };
    if duplicate {
        return state
            .send(wire::error_response(
                &id,
                wire::DUPLICATE_SUBMISSION,
                "submissionId already in flight",
            ))
            .await;
    }
    let status_session_id = params.session_id.clone();
    tokio::spawn(async move {
        let outcome = handle
            .ps
            .submit_turn(TurnSubmission {
                submission_id: submission_id.clone(),
                input,
                options,
                mode: match params.mode {
                    wire::TurnSubmitMode::StartIfIdle => TurnSubmissionMode::StartIfIdle,
                    wire::TurnSubmitMode::ContinueIfIdle => TurnSubmissionMode::ContinueIfIdle,
                    wire::TurnSubmitMode::Steer => TurnSubmissionMode::Steer,
                    wire::TurnSubmitMode::StartOrSteer => TurnSubmissionMode::StartOrSteer,
                },
                expected_turn_id: params.expected_turn_id,
                queue_message_id: params.queue_message_id,
                delivery,
                display_metadata: params.display_metadata.map(|value| value.0),
            })
            .await;
        let (status, turn_id, reason) = match outcome {
            TurnSubmissionOutcome::Started { turn_id } => {
                (wire::TurnSubmitStatus::Started, Some(turn_id), None)
            }
            TurnSubmissionOutcome::Steered { turn_id } => {
                (wire::TurnSubmitStatus::Steered, Some(turn_id), None)
            }
            TurnSubmissionOutcome::NotSubmitted {
                code,
                message,
                active_turn_id,
                turn_kind,
            } => (
                wire::TurnSubmitStatus::NotSubmitted,
                None,
                Some(wire::TurnSubmitReason {
                    code: match code {
                        TurnSubmissionRejection::NoActiveTurn => {
                            wire::TurnSubmitReasonCode::NoActiveTurn
                        }
                        TurnSubmissionRejection::ExpectedTurnMismatch => {
                            wire::TurnSubmitReasonCode::ExpectedTurnMismatch
                        }
                        TurnSubmissionRejection::NotIdle => wire::TurnSubmitReasonCode::NotIdle,
                        TurnSubmissionRejection::SessionClosing => {
                            wire::TurnSubmitReasonCode::SessionClosing
                        }
                        TurnSubmissionRejection::ActiveTurnNotSteerable => {
                            wire::TurnSubmitReasonCode::ActiveTurnNotSteerable
                        }
                        TurnSubmissionRejection::EmptyInput => {
                            wire::TurnSubmitReasonCode::EmptyInput
                        }
                        TurnSubmissionRejection::InvalidInput => {
                            wire::TurnSubmitReasonCode::InvalidInput
                        }
                    },
                    message,
                    active_turn_id,
                    turn_kind: turn_kind.map(|kind| match kind {
                        TurnSubmissionKind::Review => wire::TurnSubmitKind::Review,
                        TurnSubmissionKind::Compact => wire::TurnSubmitKind::Compact,
                    }),
                }),
            ),
        };
        if status != wire::TurnSubmitStatus::Started {
            handle.turns.lock().unwrap().remove(&submission_id);
        }
        session_unload::refresh_status(&state, &status_session_id).await;
        state
            .send(wire::response(
                &id,
                serde_json::to_value(wire::TurnSubmitResult {
                    status,
                    submission_id,
                    turn_id,
                    reason,
                })
                .expect("turn submission result"),
            ))
            .await;
    });
}

fn runtime_text_is_blank(value: &RuntimeString) -> bool {
    value
        .as_utf16()
        .iter()
        .all(|unit| char::from_u32(u32::from(*unit)).is_some_and(char::is_whitespace))
}

/// Dispatch one client request. Long-running methods spawn; the handshake and
/// pure-forward methods reply inline.
async fn handle_request(state: Arc<AppServerState>, id: Value, method: String, params: RawJson) {
    if method != "initialize" && !state.initialized.load(Ordering::Acquire) {
        state
            .send(wire::error_response(
                &id,
                wire::NOT_INITIALIZED,
                "Not initialized",
            ))
            .await;
        return;
    }
    macro_rules! scalar_params {
        () => {
            match params.scalar() {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            }
        };
    }
    match method.as_str() {
        "thread/queue/list"
        | "thread/queue/add"
        | "thread/queue/update"
        | "thread/queue/delete"
        | "thread/queue/reorder"
        | "thread/queue/start"
        | "thread/queue/pause"
        | "thread/queue/resume"
        | "thread/queue/import"
        | "thread/queue/clear"
        | "thread/queue/discard" => {
            queue::handle(&state, &id, &method, &params).await;
        }
        "turn/submit" => {
            let params = match params.deserialize::<wire::TurnSubmitParams>() {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            submit_named_turn(state, id, params).await;
        }
        "turn/start" => {
            let params = match params.deserialize::<wire::TurnStartParams>() {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            let op = exact_requests::op(&params, "run");
            start_named_turn(&state, &id, &params.session_id, params.submission_id, op).await;
        }
        "session/getPendingQuestions" => {
            let p = match params.deserialize::<wire::SessionPendingQuestionsParams>() {
                Ok(p) => p,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            let Some(handle) = state.session(&p.session_id) else {
                return send_unknown_session(&state, &id, &p.session_id).await;
            };
            let Some(session) = handle.ps.existing_session() else {
                return state
                    .send(wire::error_response(
                        &id,
                        wire::ENGINE_ERROR,
                        "The question session is unavailable",
                    ))
                    .await;
            };
            let questions = klaus_agent_core::runtime_values::runtime_codec::encode_value(
                &session.pending_questions(),
            )
            .expect("question codec");
            state
                .send(exact_payload::response(
                    &id,
                    exact_payload::object([("questions", questions)]),
                ))
                .await;
        }
        "session/answerQuestion" => {
            let p = match params.deserialize::<wire::SessionAnswerQuestionParams>() {
                Ok(p) => p,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            let Some(handle) = state.session(&p.session_id) else {
                return send_unknown_session(&state, &id, &p.session_id).await;
            };
            let Some(session) = handle.ps.existing_session() else {
                return state
                    .send(wire::error_response(
                        &id,
                        wire::ENGINE_ERROR,
                        "The question session is unavailable",
                    ))
                    .await;
            };
            tokio::spawn(async move {
                let answers =
                    klaus_agent_core::runtime_values::runtime_codec::decode_value(&p.answers.0);
                let attachments = p
                    .attachments
                    .map(|value| {
                        klaus_agent_core::runtime_values::runtime_codec::decode_value(&value.0)
                    })
                    .transpose();
                let result = match (answers, attachments) {
                    (Ok(answers), Ok(attachments)) => {
                        session
                            .answer_question(
                                p.turn_id.into(),
                                p.request_id.into(),
                                p.client_response_id.into(),
                                answers,
                                attachments,
                            )
                            .await
                    }
                    (Err(error), _) | (_, Err(error)) => Err(RuntimeString::from(format!(
                        "Invalid question answer: {error}"
                    ))),
                };
                match result {
                    Ok(receipt) => {
                        let value =
                            klaus_agent_core::runtime_values::runtime_codec::encode_value(&receipt)
                                .expect("question receipt codec");
                        state.send(exact_payload::response(&id, value)).await;
                    }
                    Err(error) => {
                        state
                            .send(Payload::Object(vec![
                                ("id".into(), id.clone().into()),
                                (
                                    "error".into(),
                                    exact_payload::object([
                                        ("code", wire::ENGINE_ERROR.into()),
                                        ("message", error.into()),
                                    ])
                                    .into(),
                                ),
                            ]))
                            .await
                    }
                }
            });
        }
        "initialize" => {
            if state.initialized.swap(true, Ordering::AcqRel) {
                state
                    .send(wire::error_response(
                        &id,
                        wire::ALREADY_INITIALIZED,
                        "Already initialized",
                    ))
                    .await;
                return;
            }
            // Client info is accepted for observability; capabilities install
            // the per-connection notification opt-out set.
            if let Ok(params) = params.deserialize::<wire::InitializeParams>() {
                if let Some(capabilities) = params.capabilities {
                    let mut opt_out = state.notification_opt_out.lock().unwrap();
                    opt_out.extend(capabilities.opt_out_notification_methods);
                    drop(opt_out);
                    state
                        .product_events
                        .store(capabilities.product_events, Ordering::Release);
                    state
                        .revisioned_session_events
                        .store(capabilities.revisioned_session_events, Ordering::Release);
                }
            }
            let result = wire::InitializeResult {
                // No version suffix: the engine does not carry one (see the
                // workspace manifest), and appending the fixed 0.0.0 would hand
                // hosts a number that looks like a release identity. What a host
                // needs to negotiate against is `protocol_version`, right below.
                user_agent: "klaus-app-server".to_string(),
                protocol_version: wire::APP_SERVER_PROTOCOL_VERSION,
                capabilities: wire::ServerCapabilities {
                    task_resource_scope: 1,
                    composer_predictions: true,
                    task_commands: cfg!(target_os = "macos"),
                    task_command_external_network: cfg!(target_os = "macos"),
                    task_identity: true,
                },
            };
            state
                .send(wire::response(&id, serde_json::to_value(result).unwrap()))
                .await;
        }
        "engine/profileActivity" => {
            if let Err(error) = params.deserialize::<wire::EngineProfileActivityParams>() {
                return send_invalid_params(&state, &id, &error).await;
            }
            let Some(read) = state
                .lifecycle
                .as_ref()
                .and_then(|lifecycle| lifecycle.read_profile_activity.clone())
            else {
                return state
                    .send(wire::error_response(
                        &id,
                        wire::ENGINE_ERROR,
                        "read-only profile activity is unavailable",
                    ))
                    .await;
            };
            let Some(guard) = HeavyRequestGuard::acquire(&state, &id).await else {
                return;
            };
            tokio::spawn(async move {
                let _guard = guard;
                match read().await {
                    Ok(activity) => match serde_json::to_value(activity) {
                        Ok(result) => state.send(wire::response(&id, result)).await,
                        Err(error) => {
                            state
                                .send(wire::error_response(
                                    &id,
                                    wire::ENGINE_ERROR,
                                    &error.to_string(),
                                ))
                                .await
                        }
                    },
                    Err(error) => {
                        state
                            .send(wire::error_response(
                                &id,
                                wire::ENGINE_ERROR,
                                &error.to_string(),
                            ))
                            .await
                    }
                }
            });
        }
        "engine/readSessionSummary" => {
            let params = match params.deserialize::<wire::EngineReadSessionSummaryParams>() {
                Ok(params) if !params.session_id.trim().is_empty() => params,
                Ok(_) => {
                    return state
                        .send(wire::error_response(
                            &id,
                            wire::INVALID_PARAMS,
                            "sessionId must be nonempty",
                        ))
                        .await;
                }
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            let Some(read) = state
                .lifecycle
                .as_ref()
                .and_then(|lifecycle| lifecycle.read_session_summary.clone())
            else {
                return state
                    .send(wire::error_response(
                        &id,
                        wire::ENGINE_ERROR,
                        "metadata-only session reader is unavailable",
                    ))
                    .await;
            };
            let Some(guard) = HeavyRequestGuard::acquire(&state, &id).await else {
                return;
            };
            tokio::spawn(async move {
                let _guard = guard;
                match read(params.session_id).await {
                    Ok(row) => match row
                        .as_ref()
                        .map(super::thread_read::exact_value)
                        .transpose()
                    {
                        Ok(session) => {
                            state
                                .send(exact_payload::response(
                                    &id,
                                    exact_payload::object([(
                                        "session",
                                        session.unwrap_or(RuntimeJson::Null),
                                    )]),
                                ))
                                .await
                        }
                        Err(error) => {
                            state
                                .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                                .await
                        }
                    },
                    Err(error) => {
                        state
                            .send(wire::error_response(
                                &id,
                                wire::ENGINE_ERROR,
                                &error.to_string(),
                            ))
                            .await
                    }
                }
            });
        }
        "engine/readThreadHistory" | "engine/readSubagentHistory" => {
            let (thread_id, agent_id, paging) = if method == "engine/readThreadHistory" {
                match params.deserialize::<wire::EngineReadThreadHistoryParams>() {
                    Ok(params) => (
                        params.thread_id,
                        None,
                        HistoryPaging {
                            cursor: params.cursor,
                            limit: params.limit,
                            items_view: params.items_view,
                            sort_direction: params.sort_direction,
                            whole: false,
                        },
                    ),
                    Err(error) => return send_invalid_params(&state, &id, &error).await,
                }
            } else {
                match params.deserialize::<wire::EngineReadSubagentHistoryParams>() {
                    Ok(params) => (
                        params.thread_id,
                        Some(params.agent_id),
                        HistoryPaging {
                            whole: true,
                            ..HistoryPaging::default()
                        },
                    ),
                    Err(error) => return send_invalid_params(&state, &id, &error).await,
                }
            };
            let Some(guard) = HeavyRequestGuard::acquire(&state, &id).await else {
                return;
            };
            tokio::spawn(async move {
                let _guard = guard;
                read_thread_history(state, id, thread_id, agent_id, paging).await;
            });
        }
        "engine/readThreadFileChanges" => {
            let params = match params.deserialize::<wire::EngineReadThreadFileChangesParams>() {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            let Some(guard) = HeavyRequestGuard::acquire(&state, &id).await else {
                return;
            };
            tokio::spawn(async move {
                let _guard = guard;
                read_thread_file_changes(state, id, params.thread_id).await;
            });
        }
        "engine/readThreadItems" => {
            let params = match params.deserialize::<wire::EngineReadThreadItemsParams>() {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            let Some(guard) = HeavyRequestGuard::acquire(&state, &id).await else {
                return;
            };
            tokio::spawn(async move {
                let _guard = guard;
                read_thread_items(state, id, params).await;
            });
        }
        "engine/readThread" => {
            let params = match params.deserialize::<wire::EngineReadThreadParams>() {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            if params.thread_id.trim().is_empty() || !(1..=10).contains(&params.limit.unwrap_or(1))
            {
                return state
                    .send(wire::error_response(
                        &id,
                        wire::INVALID_PARAMS,
                        "threadId must be nonempty and limit must be between 1 and 10",
                    ))
                    .await;
            }
            let Some(guard) = HeavyRequestGuard::acquire(&state, &id).await else {
                return;
            };
            tokio::spawn(async move {
                let _guard = guard;
                read_thread(state, id, params).await;
            });
        }
        "engine/importExternalSession" => {
            let permit = match state.import_slots.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    return state
                        .send(wire::error_response(
                            &id,
                            wire::OVERLOADED,
                            "An external import is already in progress; retry later",
                        ))
                        .await;
                }
            };
            let params = match params.deserialize::<wire::EngineImportExternalSessionParams>() {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            let Some(import) = state
                .lifecycle
                .as_ref()
                .and_then(|lifecycle| lifecycle.import_external_session.clone())
            else {
                return state
                    .send(wire::error_response(
                        &id,
                        wire::ENGINE_ERROR,
                        "external import requires an atomic session store",
                    ))
                    .await;
            };
            let Some(guard) = HeavyRequestGuard::acquire(&state, &id).await else {
                return;
            };
            tokio::spawn(async move {
                let _permit = permit;
                let _guard = guard;
                let thread_id = params.thread_id.clone();
                let mut prepared = match super::external_import::prepare(params) {
                    Ok(prepared) => prepared,
                    Err(error) => return send_invalid_params_message(&state, &id, &error).await,
                };
                let reservation = ImportIdentityGuard::acquire(&state, thread_id.clone());
                prepared.allow_create = !reservation.already_bound;
                let outcome = import(prepared).await;
                drop(reservation);
                drop(_permit);
                match outcome {
                    Ok(result) => {
                        state
                            .send(wire::response(
                                &id,
                                serde_json::to_value(wire::EngineImportExternalSessionResult {
                                    thread_id,
                                    persisted: true,
                                    already_imported: result.already_imported,
                                    message_count: result.message_count,
                                })
                                .unwrap(),
                            ))
                            .await
                    }
                    Err(error) => {
                        let code = if error
                            .downcast_ref::<std::io::Error>()
                            .is_some_and(|error| error.kind() == std::io::ErrorKind::WouldBlock)
                        {
                            wire::OVERLOADED
                        } else {
                            wire::ENGINE_ERROR
                        };
                        state
                            .send(wire::error_response(&id, code, &error.to_string()))
                            .await;
                    }
                }
            });
        }
        "engine/builtinSkills" => {
            // Answered inline from the compiled roster: these skills are
            // code-defined, so there is nothing to ask a session about and no
            // reason to make a host wait for one. A host with no way to ask ends
            // up hand-copying the roster instead, and then drifting from it.
            state
                .send(wire::response(
                    &id,
                    serde_json::to_value(builtin_skill_roster()).unwrap(),
                ))
                .await;
        }
        "engine/builtinAgents" => {
            // Same reasoning as the skill roster: the built-in agent roles and
            // their declared model tiers are compiled into the engine, so the
            // host reads them here instead of restating them.
            state
                .send(wire::response(
                    &id,
                    serde_json::to_value(builtin_agent_roster()).unwrap(),
                ))
                .await;
        }
        "engine/permissionRules" => {
            // Answered without a session, like the skill roster: the rule store is
            // the engine's own endpoint state, and a host's settings page can be
            // open with no session bound at all.
            tokio::spawn(async move {
                let result = {
                    let _guard = state.permission_rules_lock.lock().await;
                    rule_store::load_strict(&state.permission_rules_path).await
                };
                match result {
                    Ok(rules) => {
                        state
                            .send(wire::response(&id, durable_rules_result(&rules)))
                            .await
                    }
                    Err(error) => {
                        state
                            .send(wire::error_response(
                                &id,
                                wire::ENGINE_ERROR,
                                &error.to_string(),
                            ))
                            .await
                    }
                }
            });
        }
        "session/reloadPermissionRules" => {
            let params = match params.deserialize::<wire::PermissionRulesReloadParams>() {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            let Some(handle) = state.session(&params.session_id) else {
                return send_unknown_session(&state, &id, &params.session_id).await;
            };
            tokio::spawn(async move {
                let response = {
                    let _guard = state.permission_rules_lock.lock().await;
                    match rule_store::load_strict(&state.permission_rules_path).await {
                        Ok(rules) => match validate_live_durable_rules(&rules)
                            .and_then(|()| handle.ps.replace_durable_permission_rules(&rules))
                        {
                            Ok(()) => wire::response(&id, durable_rules_result(&rules)),
                            Err(error) => wire::error_response(&id, wire::ENGINE_ERROR, &error),
                        },
                        Err(error) => {
                            wire::error_response(&id, wire::ENGINE_ERROR, &error.to_string())
                        }
                    }
                };
                state.send(response).await;
            });
        }
        // The Qoder subscription, for the same reason: it is endpoint state, and a
        // settings page signs in before any session exists. Each arm is a thin call
        // into `qoder`, which owns the credential, the signer and the bridge.
        "qoder/transcribeAudio" | "qoder/cancelTranscription" => {
            let params = scalar_params!();
            dispatch_qoder_speech(state, id, &method, params).await;
        }
        "qoder/status"
        | "qoder/loginStart"
        | "qoder/loginPoll"
        | "qoder/logout"
        | "qoder/importLocal"
        | "qoder/adoptCredential"
        | "qoder/models"
        | "qoder/usage"
        | "qoder/generateImage"
        | "qoder/setThinkingLevel" => {
            let params = scalar_params!();
            dispatch_qoder(state, id, &method, params).await;
        }
        "qwenwork/status"
        | "qwenwork/loginStart"
        | "qwenwork/loginPoll"
        | "qwenwork/logout"
        | "qwenwork/importLocal"
        | "qwenwork/adoptCredential"
        | "qwenwork/models"
        | "qwenwork/refreshToken"
        | "qwenwork/usage"
        | "qwenwork/generateImage"
        | "qwenwork/generateVideo"
        | "qwenwork/generateMusic"
        | "qwenwork/search"
        | "qwenwork/capabilities"
        | "qwenwork/cancelOperation"
        | "qwenwork/transcribeAudio"
        | "qwenwork/cancelTranscription"
        | "qwenwork/setThinkingLevel" => {
            let params = scalar_params!();
            dispatch_qwenwork(state, id, &method, params).await;
        }
        // WorkBuddy's gateway signs nothing, so there is no `setThinkingLevel`
        // side channel here: the engine sends `reasoning_effort` itself, from the
        // ladder the cloud catalog declares per model.
        "cursor/status"
        | "cursor/loginStart"
        | "cursor/loginPoll"
        | "cursor/loginCancel"
        | "cursor/adoptCredential"
        | "cursor/logout"
        | "cursor/models"
        | "cursor/usage"
        | "cursor/generateImage"
        | "cursor/generateAudio"
        | "cursor/generateVideo"
        | "cursor/search"
        | "cursor/cancelOperation" => {
            let params = scalar_params!();
            dispatch_cursor(state, id, &method, params).await;
        }
        "workbuddy/status"
        | "workbuddy/loginStart"
        | "workbuddy/loginPoll"
        | "workbuddy/logout"
        | "workbuddy/importLocal"
        | "workbuddy/adoptCredential"
        | "workbuddy/models"
        | "workbuddy/refreshToken"
        | "workbuddy/usage"
        | "workbuddy/capabilities"
        | "workbuddy/generateImage"
        | "workbuddy/generateVideo"
        | "workbuddy/synthesizeSpeech"
        | "workbuddy/transcribeAudio"
        | "workbuddy/search"
        | "workbuddy/fetchWebPage"
        | "workbuddy/cancelOperation" => {
            let params = scalar_params!();
            dispatch_workbuddy(state, id, &method, params).await;
        }
        "kiro/status"
        | "kiro/loginStart"
        | "kiro/loginPoll"
        | "kiro/loginCancel"
        | "kiro/adoptCredential"
        | "kiro/logout"
        | "kiro/models"
        | "kiro/usage"
        | "kiro/capabilities"
        | "kiro/search"
        | "kiro/cancelOperation" => {
            let params = scalar_params!();
            dispatch_kiro(state, id, &method, params).await;
        }
        "trae/status"
        | "trae/loginStart"
        | "trae/loginPoll"
        | "trae/loginCancel"
        | "trae/adoptCredential"
        | "trae/logout"
        | "trae/models"
        | "trae/usage"
        | "trae/capabilities"
        | "trae/search"
        | "trae/generateImage"
        | "trae/generateVideo"
        | "trae/transcribeAudio"
        | "trae/cancelOperation" => {
            let params = scalar_params!();
            dispatch_trae(state, id, &method, params).await;
        }
        "antigravity/status"
        | "antigravity/loginStart"
        | "antigravity/loginPoll"
        | "antigravity/loginCancel"
        | "antigravity/adoptCredential"
        | "antigravity/logout"
        | "antigravity/models"
        | "antigravity/usage"
        | "antigravity/capabilities"
        | "antigravity/generateImage"
        | "antigravity/search"
        | "antigravity/transcribeAudio"
        | "antigravity/cancelOperation" => {
            let params = scalar_params!();
            dispatch_antigravity(state, id, &method, params).await;
        }
        // Kimi's coding gateway is the ordinary Anthropic wire with a plain
        // credential, so there is no `setThinkingLevel` side channel either: the
        // engine sends the effort itself, from the ladder the cloud catalog
        // declares per model. Unlike the others there is no `adoptCredential` —
        // Kimi leaves no local store this engine can read — and there is a
        // `setApiKey` instead, because the console key is the documented route.
        "kimi/status" | "kimi/setApiKey" | "kimi/loginStart" | "kimi/loginPoll" | "kimi/logout"
        | "kimi/importLocal" | "kimi/models" | "kimi/usage" => {
            let params = scalar_params!();
            dispatch_kimi(state, id, &method, params).await;
        }
        // Bailian Token Plan: the same plain-credential Anthropic wire as Kimi for
        // inference, plus a console session login — the plan key reaches no quota
        // route, but the console's own gateway serves the allowance to a browser
        // session token, and the service publishes a loopback flow to obtain one.
        "bailian/status"
        | "bailian/setApiKey"
        | "bailian/logout"
        | "bailian/models"
        | "bailian/usage"
        | "bailian/mediaCredential"
        | "bailian/consoleLogin"
        | "bailian/consoleStatus"
        | "bailian/consoleLogout" => {
            let params = scalar_params!();
            dispatch_bailian(state, id, &method, params).await;
        }
        // MiniMax M Plan: the same plain-credential Anthropic wire, with no login
        // flow. Unlike Bailian it answers usage with real allowance windows, and its
        // `setApiKey` detects the site because both sites mint the same key shape.
        "minimax/status"
        | "minimax/setApiKey"
        | "minimax/logout"
        | "minimax/models"
        | "minimax/usage"
        | "minimax/mediaCredential" => {
            let params = scalar_params!();
            dispatch_minimax(state, id, &method, params).await;
        }
        // Zhipu Coding Plan: the same plain-credential Anthropic wire, with no
        // login flow and no usage figures, because the plan meters credits against
        // an allowance only its own console reports.
        "zhipu/status" | "zhipu/setApiKey" | "zhipu/logout" | "zhipu/models" | "zhipu/usage" => {
            let params = scalar_params!();
            dispatch_zhipu(state, id, &method, params).await;
        }
        // Ark Coding Plan: the same plain-credential Anthropic wire. Its allowance
        // is readable with the plan key where the gateway publishes it, so `usage`
        // projects real windows rather than a console pointer.
        "ark/status" | "ark/setApiKey" | "ark/logout" | "ark/models" | "ark/usage" => {
            let params = scalar_params!();
            dispatch_ark(state, id, &method, params).await;
        }
        "ark/consoleLogin" | "ark/consoleStatus" | "ark/consoleLogout" => {
            let params = scalar_params!();
            dispatch_ark_console(state, id, &method, params).await;
        }
        // The Codex catalog, endpoint state for the same reason: a model picker
        // reads it with no session bound, and the engine is the only fetcher of it.
        "claude/transcribeAudio" | "codex/transcribeAudio" => {
            let params = match params.deserialize::<wire::SubscriptionTranscribeParams>() {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            let source = if method.starts_with("claude/") {
                super::subscription_speech::Source::Claude
            } else {
                super::subscription_speech::Source::Codex
            };
            let operation = match state
                .provider_operations
                .register(source.name(), params.operation_id.as_deref())
            {
                Ok(operation) => operation,
                Err(error) => {
                    return state
                        .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                        .await;
                }
            };
            tokio::spawn(async move {
                let mut operation = operation;
                let signal = operation.signal();
                let result = tokio::select! {
                    biased;
                    _ = state.out_tx.closed() => return,
                    result = super::subscription_speech::transcribe(
                        super::model_sources::config_root_of(&state.config_path),
                        state.credential_store.clone(), source, params, signal,
                        state.subscription_speech_options.clone(), state.claude_usage_options.clone(),
                    ) => result,
                };
                let message = match result {
                    Ok(result) => {
                        operation.bind_publication_check(result.publication_check);
                        wire::response(
                            &id,
                            serde_json::to_value(result.value).expect("transcript encodes"),
                        )
                    }
                    Err(error) => wire::error_response(&id, wire::ENGINE_ERROR, &error),
                };
                let _ = state
                    .out_tx
                    .send(Outgoing {
                        message: message.into(),
                        cursor_operation: None,
                        provider_operation: Some(operation),

                        workbuddy_operation: None,
                    })
                    .await;
            });
        }
        "claude/cancelTranscription" | "codex/cancelTranscription" => {
            let params = match params.deserialize::<wire::SubscriptionCancelTranscriptionParams>() {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            let source = if method.starts_with("claude/") {
                "claude"
            } else {
                "codex"
            };
            match state
                .provider_operations
                .cancel(source, &params.operation_id)
            {
                Ok(cancelled) => {
                    state
                        .send(wire::response(
                            &id,
                            serde_json::json!({"cancelled":cancelled}),
                        ))
                        .await
                }
                Err(error) => {
                    state
                        .send(wire::error_response(&id, wire::INVALID_PARAMS, &error))
                        .await
                }
            }
        }
        "codex/models" => {
            // A settings page's read answers from the catalog at hand and
            // refreshes behind itself; a picker's read waits for the live one.
            let read = params
                .deserialize::<wire::CodexModelsParams>()
                .map(|params| match params.read {
                    wire::QoderCatalogRead::Warm => super::codex_catalog::CatalogRead::Warm,
                    wire::QoderCatalogRead::Cached => super::codex_catalog::CatalogRead::Cached,
                })
                .unwrap_or(super::codex_catalog::CatalogRead::Warm);
            tokio::spawn(async move {
                let config_root = super::model_sources::config_root_of(&state.config_path);
                let catalog = super::codex_catalog::list(&config_root, read).await;
                state.send(wire::response(&id, catalog)).await;
            });
        }
        "claude/usage" => {
            if let Err(error) = params.deserialize::<wire::QoderParams>() {
                return state
                    .send(wire::error_response(
                        &id,
                        wire::INVALID_PARAMS,
                        &error.to_string(),
                    ))
                    .await;
            }
            tokio::spawn(async move {
                let root = super::model_sources::config_root_of(&state.config_path);
                let result = super::claude_usage::usage(
                    &root,
                    state.credential_store.clone(),
                    &state.claude_usage_options,
                )
                .await;
                match result {
                    Ok(value) => state.send(wire::response(&id, value)).await,
                    Err(error) => {
                        state
                            .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                            .await;
                    }
                }
            });
        }
        "model/selection/resolve" => {
            let params = match params.deserialize::<wire::ModelSelectionResolveParams>() {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            tokio::spawn(async move {
                match super::model_selection::resolve(&state.config_path, &params).await {
                    Ok(mut result) => {
                        if let Some(targets) = params.model_tier_targets {
                            let mut init = serde_json::json!({"modelTierTargets": targets});
                            if let Err(error) = resolve_model_provider(&state, &mut init).await {
                                return state
                                    .send(wire::error_response(&id, wire::INVALID_PARAMS, &error))
                                    .await;
                            }
                            let mut projected = serde_json::Map::new();
                            for (name, target) in
                                init["modelTierTargets"].as_object().into_iter().flatten()
                            {
                                let value = if target.is_null() || target["type"] == "unavailable" {
                                    target.clone()
                                } else {
                                    serde_json::json!({"type":"ready","source":target["source"],"modelId":target["modelId"],"model":target.pointer("/provider/model"),"capabilities":target["capabilities"],"pricing":target.get("pricing")})
                                };
                                projected.insert(name.clone(), value);
                            }
                            result.model_tier_targets = Some(Value::Object(projected));
                        }
                        state
                            .send(wire::response(&id, serde_json::to_value(result).unwrap()))
                            .await
                    }
                    Err(error) => {
                        state
                            .send(wire::error_response(&id, wire::INVALID_PARAMS, &error))
                            .await
                    }
                }
            });
        }
        // The Claude lineup, endpoint state for the same reason. Answered inline:
        // unlike the two above it reads neither the network nor a credential, so
        // there is nothing to await.
        "subscription/models" => {
            state
                .send(wire::response(&id, super::model_sources::claude_lineup()))
                .await;
        }
        "engine/addPermissionRules" => {
            let params = match params.deserialize::<wire::PermissionRulesEditParams>() {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            let rules = match rules_from_wire(Some(&params.rules)) {
                Ok(rules) => rules,
                Err(error) => {
                    return state
                        .send(wire::error_response(&id, wire::INVALID_PARAMS, &error))
                        .await;
                }
            };
            if rules.is_empty() {
                return state
                    .send(wire::error_response(
                        &id,
                        wire::INVALID_PARAMS,
                        "rules must carry at least one toolName",
                    ))
                    .await;
            }
            let behavior = behavior_from_wire(params.behavior.as_deref());
            tokio::spawn(async move {
                let result = {
                    let _guard = state.permission_rules_lock.lock().await;
                    rule_store::add(&state.permission_rules_path, behavior, &rules).await
                };
                match result {
                    Ok(next) => {
                        state
                            .send(wire::response(&id, durable_rules_result(&next)))
                            .await
                    }
                    Err(error) => {
                        state
                            .send(wire::error_response(
                                &id,
                                wire::ENGINE_ERROR,
                                &error.to_string(),
                            ))
                            .await
                    }
                }
            });
        }
        "engine/removePermissionRule" => {
            let params = match params.deserialize::<wire::PermissionRuleRemoveParams>() {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            let behavior = behavior_from_wire(params.behavior.as_deref());
            tokio::spawn(async move {
                let result = {
                    let _guard = state.permission_rules_lock.lock().await;
                    rule_store::remove(&state.permission_rules_path, behavior, &params.rule).await
                };
                match result {
                    Ok(next) => {
                        state
                            .send(wire::response(&id, durable_rules_result(&next)))
                            .await
                    }
                    Err(error) => {
                        state
                            .send(wire::error_response(
                                &id,
                                wire::ENGINE_ERROR,
                                &error.to_string(),
                            ))
                            .await
                    }
                }
            });
        }
        "engine/windowsSandboxReadiness" => {
            if let Err(error) = params.deserialize::<Option<wire::WindowsSandboxReadinessParams>>()
            {
                return send_invalid_params(&state, &id, &error).await;
            }
            tokio::spawn(async move {
                match state
                    .windows_sandbox_context
                    .readiness(&state.config_path)
                    .await
                {
                    Ok(result) => {
                        state
                            .send(wire::response(&id, serde_json::to_value(result).unwrap()))
                            .await
                    }
                    Err(error) => {
                        state
                            .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                            .await
                    }
                }
            });
        }
        "engine/startWindowsSandboxSetup" => {
            let params = match params.deserialize::<wire::WindowsSandboxSetupStartParams>() {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            tokio::spawn(async move {
                let plan = match state
                    .windows_sandbox_context
                    .load_setup(&state.config_path, params.cwd)
                    .await
                {
                    Ok(plan) => plan,
                    Err(error) => {
                        return state
                            .send(wire::error_response(
                                &id,
                                wire::ENGINE_ERROR,
                                &format!("Failed to load config: {error}"),
                            ))
                            .await;
                    }
                };
                state
                    .send(wire::response(
                        &id,
                        serde_json::to_value(wire::WindowsSandboxSetupStartResult {
                            started: true,
                        })
                        .unwrap(),
                    ))
                    .await;
                let result = state
                    .windows_sandbox_context
                    .prepare(params.mode, plan)
                    .await;
                state
                    .notify(
                        "engine/windowsSandboxSetupCompleted",
                        serde_json::to_value(wire::WindowsSandboxSetupCompletedNotification {
                            mode: params.mode,
                            success: result.is_ok(),
                            error: result.err(),
                        })
                        .unwrap(),
                    )
                    .await;
            });
        }
        "config/read" => {
            let params = match params.deserialize::<wire::ConfigReadParams>() {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            tokio::spawn(async move {
                let target = config_target(&state, params.file_path.as_deref());
                match read_config_result(&state, &target, params.cwd.as_deref()).await {
                    Ok(result) => state.send(wire::response(&id, result)).await,
                    Err(error) => send_config_error(&state, &id, &error).await,
                }
            });
        }
        "config/value/write" => {
            let params = match params.deserialize::<wire::ConfigValueWriteParams>() {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            let edit = match config_edit(&wire::ConfigEditParam {
                key_path: params.key_path,
                value: params.value,
                merge_strategy: params.merge_strategy,
            }) {
                Ok(edit) => edit,
                Err(error) => return send_config_error(&state, &id, &error).await,
            };
            tokio::spawn(async move {
                write_config(
                    state,
                    id,
                    params.file_path,
                    vec![edit],
                    params.expected_version,
                )
                .await;
            });
        }
        "config/batchWrite" => {
            let params = match params.deserialize::<wire::ConfigBatchWriteParams>() {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            let edits = match params
                .edits
                .iter()
                .map(config_edit)
                .collect::<Result<Vec<_>, _>>()
            {
                Ok(edits) => edits,
                Err(error) => return send_config_error(&state, &id, &error).await,
            };
            tokio::spawn(async move {
                write_config(state, id, params.file_path, edits, params.expected_version).await;
            });
        }
        "session/migrateBundledDiagram" => {
            let params = match params.deserialize::<wire::SessionBundledDiagramMigrationParams>() {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            tokio::spawn(async move {
                let Some(admin) = state.plugin_admin.as_ref() else {
                    state
                        .send(wire::error_response(
                            &id,
                            wire::ENGINE_ERROR,
                            "Plugin administration is unavailable",
                        ))
                        .await;
                    return;
                };
                let result = {
                    let _guard = state.config_lock.lock().await;
                    admin.migrate_bundled_diagram(&params.source_dir).await
                };
                match result {
                    Ok(migrated) => state.send(wire::response(
                        &id,
                        serde_json::json!({ "migrated": migrated, "restartRequired": migrated }),
                    )).await,
                    Err(error) => state.send(wire::error_response(
                        &id,
                        wire::ENGINE_ERROR,
                        &error.to_string(),
                    )).await,
                }
            });
        }
        "hooks/read" => {
            let params = match params.deserialize::<wire::HooksReadParams>() {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            tokio::spawn(async move {
                let target = hooks_target(params.file_path.as_deref());
                match hooks_store::load(&target).await {
                    Ok(hooks) => {
                        state
                            .send(wire::response(&id, hooks_result(&target, hooks)))
                            .await
                    }
                    Err(error) => send_hooks_error(&state, &id, &error).await,
                }
            });
        }
        "hooks/write" => {
            let params = match params.deserialize::<wire::HooksWriteParams>() {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            let hooks = match params.hooks {
                Value::Object(map) => map,
                Value::Null => serde_json::Map::new(),
                _ => {
                    return state
                        .send(wire::error_response(
                            &id,
                            wire::INVALID_PARAMS,
                            "hooks must be an object",
                        ))
                        .await;
                }
            };
            tokio::spawn(async move {
                let target = hooks_target(params.file_path.as_deref());
                let result = {
                    let _guard = state.hooks_lock.lock().await;
                    hooks_store::store(&target, &hooks).await
                };
                match result {
                    Ok(()) => {
                        state
                            .send(wire::response(&id, hooks_result(&target, hooks)))
                            .await
                    }
                    Err(error) => send_hooks_error(&state, &id, &error).await,
                }
            });
        }
        "model/credential/write" => {
            let params = match params.deserialize::<wire::ModelCredentialWriteParams>() {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            write_model_credential(state, id, params).await;
        }
        "model/credential/delete" => {
            let params = match params.deserialize::<wire::ModelCredentialDeleteParams>() {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            delete_model_credential(state, id, params).await;
        }
        "model/credential/read" => {
            let params = match params.deserialize::<wire::ModelCredentialReadParams>() {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            read_model_credential(state, id, params).await;
        }
        "engine/compressRollouts" => {
            if let Err(error) = params.deserialize::<wire::EngineCompressRolloutsParams>() {
                return send_invalid_params(&state, &id, &error).await;
            }
            match state.maintenance.kick_compression() {
                Ok(()) => state.send(wire::response(&id, serde_json::json!({}))).await,
                Err(error) => {
                    state
                        .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                        .await
                }
            }
        }
        "engine/migrateRollouts" => {
            let params = match params.deserialize::<wire::EngineMigrateRolloutsParams>() {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            let request = match super::maintenance::migration_request(params) {
                Ok(request) => request,
                Err(error) => return send_invalid_params_message(&state, &id, &error).await,
            };
            let Some(guard) = HeavyRequestGuard::acquire(&state, &id).await else {
                return;
            };
            tokio::spawn(async move {
                let _guard = guard;
                match state.maintenance.migrate(request).await {
                    Ok(report) => {
                        state
                            .send(exact_payload::response(&id, Payload::Runtime(report)))
                            .await
                    }
                    Err(error) => {
                        state
                            .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                            .await
                    }
                }
            });
        }
        "session/start" => {
            let params = match params.deserialize::<wire::SessionStartParams>() {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            let Some(guard) = HeavyRequestGuard::acquire(&state, &id).await else {
                return;
            };
            tokio::spawn(async move {
                let _guard = guard;
                session_start(state, id, params).await;
            });
        }
        "session/prepare" => {
            let params = match params.deserialize::<wire::SessionPrepareParams>() {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            let Some(handle) = state.session(&params.session_id) else {
                return send_unknown_session(&state, &id, &params.session_id).await;
            };
            let Some(guard) = HeavyRequestGuard::acquire(&state, &id).await else {
                return;
            };
            tokio::spawn(async move {
                let _guard = guard;
                match handle.ps.prepare_session().await {
                    Ok(_) => {
                        state
                            .send(wire::response(
                                &id,
                                serde_json::to_value(wire::SessionPrepareResult {
                                    session_id: params.session_id,
                                    ready: true,
                                })
                                .expect("preparation receipt encodes"),
                            ))
                            .await
                    }
                    Err(error) => {
                        state
                            .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                            .await
                    }
                }
            });
        }
        "session/forkLive" => {
            let params = match params.deserialize::<wire::SessionForkLiveParams>() {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            let Some(guard) = HeavyRequestGuard::acquire(&state, &id).await else {
                return;
            };
            tokio::spawn(async move {
                let _guard = guard;
                session_fork_live(state, id, params).await;
            });
        }
        "session/startSideTask" => {
            let params = match params.deserialize::<wire::SessionStartSideTaskParams>() {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            let Some(guard) = HeavyRequestGuard::acquire(&state, &id).await else {
                return;
            };
            tokio::spawn(async move {
                let _guard = guard;
                session_start_side_task(state, id, params).await;
            });
        }
        "session/beginModelUpdate" => {
            let params = match params.deserialize::<wire::SessionBeginModelUpdateParams>() {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            if let Err(error) = validate_model_change_id(&params.change_id) {
                return state
                    .send(wire::error_response(&id, wire::INVALID_PARAMS, &error))
                    .await;
            }
            let Some(handle) = state.session(&params.session_id) else {
                return send_unknown_session(&state, &id, &params.session_id).await;
            };
            match handle
                .ps
                .begin_model_update(params.change_id.clone(), params.expected_revision)
            {
                Ok(revision) => {
                    state
                        .send(wire::response(
                            &id,
                            serde_json::to_value(wire::SessionBeginModelUpdateResult {
                                revision,
                                change_id: params.change_id,
                            })
                            .unwrap(),
                        ))
                        .await
                }
                Err(error) => {
                    state
                        .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                        .await
                }
            }
        }
        "session/failModelUpdate" => {
            let params = match params.deserialize::<wire::SessionFailModelUpdateParams>() {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            if let Err(error) = validate_model_change_id(&params.change_id) {
                return state
                    .send(wire::error_response(&id, wire::INVALID_PARAMS, &error))
                    .await;
            }
            if params.error.is_empty() || params.error.len() > 16_384 {
                return state
                    .send(wire::error_response(
                        &id,
                        wire::INVALID_PARAMS,
                        "model update error must be nonempty and at most 16384 bytes",
                    ))
                    .await;
            }
            let Some(session) = state
                .session(&params.session_id)
                .and_then(|handle| handle.ps.existing_session())
            else {
                return send_unknown_session(&state, &id, &params.session_id).await;
            };
            session.fail_model_update(&params.change_id, params.error);
            state
                .send(wire::response(
                    &id,
                    serde_json::to_value(wire::SessionFailModelUpdateResult {
                        revision: session.model_revision(),
                        change_id: params.change_id,
                    })
                    .unwrap(),
                ))
                .await;
        }
        "session/modelRuntime" => {
            let params = match params.deserialize::<wire::SessionModelRuntimeParams>() {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            let Some(session) = state
                .session(&params.session_id)
                .and_then(|handle| handle.ps.existing_session())
            else {
                return send_unknown_session(&state, &id, &params.session_id).await;
            };
            match model_runtime_value(&session) {
                Ok(runtime) => state.send(wire::response(&id, runtime)).await,
                Err(error) => {
                    state
                        .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                        .await
                }
            }
        }
        "session/reconfigureModel" => {
            let params = match params.deserialize::<wire::SessionReconfigureModelParams>() {
                Ok(params) => params,
                Err(error) => {
                    fail_model_update_from_params(
                        &state,
                        &scalar_params!(),
                        "invalid model update request parameters".to_string(),
                    );
                    return send_invalid_params(&state, &id, &error).await;
                }
            };
            let Some(guard) = HeavyRequestGuard::acquire(&state, &id).await else {
                fail_model_update_from_params(
                    &state,
                    &serde_json::to_value(&params).unwrap(),
                    "model preparation rejected because request ingress is overloaded".to_string(),
                );
                return;
            };
            tokio::spawn(async move {
                let _guard = guard;
                session_reconfigure_model(state, id, params).await;
            });
        }
        "session/resumePending" | "session/resumeQueued" => {
            let (session_id, submission_id, op) = if method == "session/resumePending" {
                let params = match params.deserialize::<wire::SessionResumePendingParams>() {
                    Ok(params) => params,
                    Err(error) => return send_invalid_params(&state, &id, &error).await,
                };
                if params.after_submission_id.is_empty() {
                    return send_invalid_params_message(
                        &state,
                        &id,
                        "afterSubmissionId must not be empty",
                    )
                    .await;
                }
                (
                    params.session_id,
                    params.submission_id,
                    serde_json::json!({"type":"resume_pending","afterSubmissionId":params.after_submission_id}),
                )
            } else {
                let params = match params.deserialize::<wire::SessionResumeQueuedParams>() {
                    Ok(params) => params,
                    Err(error) => return send_invalid_params(&state, &id, &error).await,
                };
                (
                    params.session_id,
                    params.submission_id,
                    serde_json::json!({"type":"resume_queued"}),
                )
            };
            start_named_turn(&state, &id, &session_id, submission_id, op).await;
        }
        "turn/interrupt" => {
            let params = match params.deserialize::<wire::TurnInterruptParams>() {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            if params
                .expected_turn_id
                .as_ref()
                .is_some_and(String::is_empty)
            {
                return send_invalid_params_message(
                    &state,
                    &id,
                    "expectedTurnId must not be empty",
                )
                .await;
            }
            queue::interrupt(state, id, params).await;
        }
        "session/follow" => {
            let params = match params.deserialize::<wire::SessionFollowParams>() {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            let Some(handle) = state.session(&params.session_id) else {
                return send_unknown_session(&state, &id, &params.session_id).await;
            };
            if params.after_revision.is_some() != params.stream_id.is_some() {
                return state
                    .send(wire::error_response(
                        &id,
                        wire::INVALID_PARAMS,
                        "streamId and afterRevision must be supplied together",
                    ))
                    .await;
            }
            if let (Some(stream_id), Some(after_revision)) =
                (params.stream_id.as_deref(), params.after_revision)
            {
                let replay = {
                    let stream = handle.stream.lock().unwrap();
                    (stream.id == stream_id)
                        .then(|| stream.replay_after(after_revision))
                        .flatten()
                        .map(|events| {
                            exact_payload::object([
                                ("sessionId", params.session_id.clone().into()),
                                ("streamId", stream.id.clone().into()),
                                ("revision", stream.revision.into()),
                                ("mode", "replay".into()),
                                ("events", RuntimeJson::Array(events)),
                                (
                                    "availableFromRevision",
                                    stream.available_from_revision().into(),
                                ),
                            ])
                        })
                };
                if let Some(mut result) = replay {
                    if exact_field(&result, "events")
                        .as_array()
                        .is_some_and(Vec::is_empty)
                    {
                        result
                            .as_object_mut()
                            .expect("follow result")
                            .remove("events");
                    }
                    state.send(exact_payload::response(&id, result)).await;
                    return;
                }
            }
            forward_follow_snapshot(state, handle, id, params).await;
        }
        "session/subscribe" | "session/unsubscribe" => {
            let params = match params.deserialize::<wire::SessionSubscriptionParams>() {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            let Some(handle) = state.session(&params.session_id) else {
                return send_unknown_session(&state, &id, &params.session_id).await;
            };
            // Event-stream gate only: request/reply traffic for the session is
            // unaffected either way.
            let subscribed = method == "session/subscribe";
            if subscribed
                && state
                    .pending_session_unloads
                    .lock()
                    .unwrap()
                    .contains(&params.session_id)
            {
                // The session is mid-unload; the client retries and either
                // re-attaches or rebuilds once teardown finishes.
                return state
                    .send(wire::error_response(
                        &id,
                        wire::INVALID_REQUEST,
                        &format!(
                            "session {} is closing; retry after the session is closed",
                            params.session_id
                        ),
                    ))
                    .await;
            }
            {
                let mut gated = state.unsubscribed_sessions.lock().unwrap();
                if subscribed {
                    gated.remove(&params.session_id);
                } else {
                    gated.insert(params.session_id.clone());
                }
            }
            if let Some(tx) = state
                .session_subscriptions
                .lock()
                .unwrap()
                .get(&params.session_id)
            {
                let _ = tx.send(subscribed);
            }
            if subscribed {
                session_unload::ensure_watcher(&state, &params.session_id, &handle);
            }
            let result = wire::SessionSubscriptionResult {
                session_id: params.session_id,
                subscribed,
            };
            state
                .send(wire::response(&id, serde_json::to_value(result).unwrap()))
                .await;
        }
        "engine/completeText" | "engine/cancelTextCompletion" => {
            let params = scalar_params!();
            text_completion::dispatch(state.clone(), id, &method, params).await
        }
        "engine/dictationStart" | "engine/dictationAppendAudio" | "engine/dictationEnd" => {
            let params = scalar_params!();
            dictation::dispatch(state.clone(), id, method, params).await
        }
        "session/realtimeCapabilities" => {
            let params = scalar_params!();
            realtime::dispatch(state.clone(), id, method, params).await
        }
        "session/realtimeStart" => {
            let params = scalar_params!();
            realtime::dispatch(state.clone(), id, method, params).await
        }
        "session/realtimeStop" => {
            let params = scalar_params!();
            realtime::dispatch(state.clone(), id, method, params).await
        }
        "session/realtimeInterrupt" => {
            let params = scalar_params!();
            realtime::dispatch(state.clone(), id, method, params).await
        }
        "session/realtimeAppendText" => {
            let params = scalar_params!();
            realtime::dispatch(state.clone(), id, method, params).await
        }
        "session/realtimeAppendSpeech" => {
            let params = scalar_params!();
            realtime::dispatch(state.clone(), id, method, params).await
        }
        "session/realtimeAppendAudio" => {
            let params = scalar_params!();
            realtime::dispatch(state.clone(), id, method, params).await
        }
        "session/realtimeHandoff" => {
            let params = scalar_params!();
            realtime::dispatch(state.clone(), id, method, params).await
        }
        "session/close" => {
            let params = match params.deserialize::<wire::SessionCloseParams>() {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
            let handle = state
                .sessions
                .lock()
                .unwrap()
                .get(&params.session_id)
                .cloned();
            let Some(handle) = handle else {
                return send_unknown_session(&state, &id, &params.session_id).await;
            };
            let Some(guard) = HeavyRequestGuard::acquire(&state, &id).await else {
                return;
            };
            // Retain the same identity until drain succeeds. A concurrent close
            // shares the retained shutdown result; a failed drain must not turn
            // into "unknown session" and become a false cleanup receipt on retry.
            tokio::spawn(async move {
                let _guard = guard;
                if let Err(error) = realtime::stop_session(&state, &params.session_id).await {
                    return state
                        .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                        .await;
                }
                match handle.ps.shutdown_and_wait().await {
                    Ok(()) => {
                        {
                            let mut sessions = state.sessions.lock().unwrap();
                            if sessions
                                .get(&params.session_id)
                                .is_some_and(|current| Arc::ptr_eq(current, &handle))
                            {
                                sessions.remove(&params.session_id);
                            }
                        }
                        session_unload::discard_session_bookkeeping(&state, &params.session_id)
                            .await;
                        state.send(wire::response(&id, serde_json::json!({}))).await
                    }
                    Err(error) => {
                        state
                            .send(wire::error_response(
                                &id,
                                wire::ENGINE_ERROR,
                                &format!("session close did not drain: {error}"),
                            ))
                            .await
                    }
                }
            });
        }

        _ => {
            if handle_promoted_request(&state, &id, &method, params.clone()).await {
                return;
            }
            if handle_acked_request(&state, &id, &method, params).await {
                return;
            }
            state
                .send(wire::error_response(
                    &id,
                    wire::METHOD_NOT_FOUND,
                    &format!("unknown method: {method}"),
                ))
                .await;
        }
    }
}

/// Write a `permission_update` event's rules to the store when its destination
/// outlives the session. `session` destinations are live session state only.
/// The store lock is taken only once a durable grant is confirmed, so the common
/// case (every non-permission stream event) pays nothing.
async fn persist_durable_grant(state: &AppServerState, handle: &SessionHandle, event: &Value) {
    let Some(inner) = event.get("event") else {
        return;
    };
    if inner.get("type").and_then(Value::as_str) != Some("permission_update") {
        return;
    }
    if inner.get("destination").and_then(Value::as_str) == Some("session") {
        return;
    }
    let Some(update) = inner.get("update") else {
        return;
    };
    if update.get("type").and_then(Value::as_str) != Some("addRules") {
        return;
    }
    let behavior = match update.get("behavior").and_then(Value::as_str) {
        Some("deny") => PermissionBehavior::Deny,
        Some("ask") => PermissionBehavior::Ask,
        _ => PermissionBehavior::Allow,
    };
    let rules = match rules_from_wire(update.get("rules")) {
        Ok(rules) => rules,
        Err(error) => {
            eprintln!("[app-server] refused invalid durable permission rule: {error}");
            return;
        }
    };
    if rules.is_empty() {
        return;
    }
    let grant = match klaus_agent_core::runtime_values::runtime_codec::decode_text::<
        klaus_agent_core::types::permission::PermissionUpdate,
    >(&RuntimeString::from(update.to_string()))
    {
        Ok(grant) => grant,
        Err(error) => {
            eprintln!("[app-server] invalid durable permission update: {error}");
            return;
        }
    };
    let _guard = state.permission_rules_lock.lock().await;
    match rule_store::add(&state.permission_rules_path, behavior, &rules).await {
        Ok(_) => handle.ps.confirm_durable_permission_grant(&grant, &rules),
        Err(error) => eprintln!("[app-server] permission rule store write failed: {error}"),
    }
}

/// Live replacement must not silently drop an invalid deny/ask row. Store
/// administration keeps legacy raw rows visible and removable for repair.
fn validate_live_durable_rules(rules: &DurableRules) -> Result<(), String> {
    for (class, entries) in [
        ("allow", &rules.allow),
        ("deny", &rules.deny),
        ("ask", &rules.ask),
    ] {
        for (index, rule) in entries.iter().enumerate() {
            if klaus_agent_core::permission::rules::parse_rule_string(rule).is_none() {
                return Err(format!(
                    "durable {class} rule at index {index} cannot be evaluated; correct or remove it before reloading"
                ));
            }
        }
    }
    Ok(())
}

/// The `{ allow, deny, ask }` reply every rule-store method answers with.
fn durable_rules_result(rules: &DurableRules) -> Value {
    serde_json::to_value(wire::PermissionRulesResult {
        allow: rules.allow.clone(),
        deny: rules.deny.clone(),
        ask: rules.ask.clone(),
    })
    .expect("a string-list result serializes")
}

/// Unknown/absent behavior means `allow`: that is the grant an approval produces,
/// and a typo must not silently write a DENY rule.
fn behavior_from_wire(behavior: Option<&str>) -> PermissionBehavior {
    match behavior {
        Some("deny") => PermissionBehavior::Deny,
        Some("ask") => PermissionBehavior::Ask,
        _ => PermissionBehavior::Allow,
    }
}

/// `[{ toolName, argumentPrefix? }]` → parsed rules. Used for both the event
/// projection and the host's own rule edits, so one shape is understood once.
fn rules_from_wire(value: Option<&Value>) -> Result<Vec<PermissionRule>, String> {
    use klaus_agent_core::permission::rules::{format_rule_string, parse_rule_string};
    let items = value
        .and_then(Value::as_array)
        .ok_or_else(|| "rules must be an array".to_string())?;
    items
        .iter()
        .map(|item| {
            let tool = item
                .get("toolName")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|tool| !tool.is_empty() && !tool.contains(['(', ')']))
                .ok_or_else(|| "rule toolName must be a nonempty tool identifier".to_string())?;
            let prefix = match item
                .get("argumentPrefix")
                .or_else(|| item.get("ruleContent"))
            {
                Some(Value::String(prefix)) => {
                    Some(prefix.trim()).filter(|prefix| !prefix.is_empty())
                }
                None | Some(Value::Null) => None,
                _ => return Err("rule argumentPrefix must be a string".to_string()),
            };
            // One grammar handles both durable storage and evaluation. Invalid
            // entries reject the batch instead of saving inert fallback rules.
            parse_rule_string(&format_rule_string(tool, prefix)).ok_or_else(|| {
                "permission rule cannot be represented by the rule grammar".to_string()
            })
        })
        .collect()
}

/// The config file a request targets: an explicit `filePath` (a project's own
/// config) or the endpoint's user config.
fn config_target(state: &AppServerState, file_path: Option<&str>) -> PathBuf {
    match file_path {
        Some(path) if !path.trim().is_empty() => PathBuf::from(path),
        _ => state.config_path.clone(),
    }
}

/// Unknown/absent strategy means `upsert`: writing one nested field must not
/// throw away its siblings because a caller forgot to name the strategy.
fn config_edit(edit: &wire::ConfigEditParam) -> Result<ConfigEdit, ConfigStoreError> {
    let merge = match edit.merge_strategy.as_deref() {
        None | Some("") | Some("upsert") => MergeStrategy::Upsert,
        Some("replace") => MergeStrategy::Replace,
        Some("delete") => MergeStrategy::Delete,
        Some(other) => {
            return Err(ConfigStoreError::Rejected(format!(
                "unknown merge strategy '{other}' (upsert | replace | delete)"
            )));
        }
    };
    Ok(ConfigEdit {
        key_path: edit.key_path.clone(),
        value: edit.value.clone(),
        merge,
    })
}

/// A config reply: the target file as stored, plus the engine view a session
/// would resolve. The resolved view stacks user under project so a host can mark
/// the keys a repo takes over, which is exactly the case where writing the user
/// file would look like it did nothing.
async fn config_result(
    state: &AppServerState,
    target: &Path,
    cwd: Option<&str>,
    document: config_store::ConfigDocument,
) -> Result<Value, ConfigStoreError> {
    let mut layers = Vec::new();
    if target == state.config_path {
        layers.push(document.engine_layer(ConfigGovernanceSource::User));
    } else {
        let user = config_store::load(&state.config_path).await?;
        layers.push(user.engine_layer(ConfigGovernanceSource::User));
        layers.push(document.engine_layer(ConfigGovernanceSource::Project));
    }
    if let Some(cwd) = cwd.filter(|cwd| !cwd.trim().is_empty()) {
        let project_path = config_store::project_config_path(Path::new(cwd));
        if project_path != *target && tokio::fs::try_exists(&project_path).await.unwrap_or(false) {
            let project = config_store::load(&project_path).await?;
            layers.push(project.engine_layer(ConfigGovernanceSource::Project));
        }
    }

    let view = config_store::resolve_view(&layers)?;

    serde_json::to_value(wire::ConfigResult {
        path: target.display().to_string(),
        version: document.version,
        values: Value::Object(document.values),
        resolved: Value::Object(view.resolved),
        sources: Value::Object(view.sources),
        migration_warnings: (!document.migration_warnings.is_empty()).then(|| {
            document
                .migration_warnings
                .into_iter()
                .map(|warning| wire::ConfigMigrationWarning {
                    code: warning.code,
                    message: warning.message,
                    keys: warning.keys,
                })
                .collect()
        }),
    })
    .map_err(|error| ConfigStoreError::Rejected(error.to_string()))
}

async fn read_config_result(
    state: &AppServerState,
    target: &Path,
    cwd: Option<&str>,
) -> Result<Value, ConfigStoreError> {
    let document = config_store::load(target).await?;
    config_result(state, target, cwd, document).await
}

async fn write_config(
    state: Arc<AppServerState>,
    id: Value,
    file_path: Option<String>,
    edits: Vec<ConfigEdit>,
    expected_version: Option<String>,
) {
    let target = config_target(&state, file_path.as_deref());
    let written = {
        let _guard = state.config_lock.lock().await;
        config_store::apply_edits(&target, &edits, expected_version.as_deref()).await
    };
    let document = match written {
        Ok(document) => document,
        Err(error) => return send_config_error(&state, &id, &error).await,
    };
    match config_result(&state, &target, None, document).await {
        Ok(result) => state.send(wire::response(&id, result)).await,
        Err(error) => send_config_error(&state, &id, &error).await,
    }
}

/// Config failures carry their store code as error `data.reason`: a stale-version
/// write is retried after re-reading, a rejected value is shown to the user, and
/// a host that only prints the message still gets a sentence that says which.
async fn send_config_error(state: &AppServerState, id: &Value, error: &ConfigStoreError) {
    let code = match error {
        ConfigStoreError::InvalidKeyPath(_) => wire::INVALID_PARAMS,
        _ => wire::ENGINE_ERROR,
    };
    state
        .send(wire::error_response_with_data(
            id,
            code,
            &error.to_string(),
            serde_json::json!({ "reason": error.code() }),
        ))
        .await;
}

/// Store a BYO model provider's API key in the endpoint credential store. The
/// secret is validated for a safe id, then written under the shared
/// [`model_credential_key`] so session assembly reads it back by the same key.
async fn write_model_credential(
    state: Arc<AppServerState>,
    id: Value,
    params: wire::ModelCredentialWriteParams,
) {
    let Some(store) = model_credential_store(&state, &id, &params.id).await else {
        return;
    };
    let key = model_credential_key(&params.id);
    tokio::spawn(async move {
        match store.set(&key, &params.api_key).await {
            Ok(()) => {
                state
                    .send(wire::response(&id, model_credential_ack()))
                    .await
            }
            Err(error) => {
                state
                    .send(wire::error_response(
                        &id,
                        wire::ENGINE_ERROR,
                        &format!("failed to store model credential: {error}"),
                    ))
                    .await
            }
        }
    });
}

/// Drop a BYO model provider's stored API key (a model the user removed).
async fn delete_model_credential(
    state: Arc<AppServerState>,
    id: Value,
    params: wire::ModelCredentialDeleteParams,
) {
    let Some(store) = model_credential_store(&state, &id, &params.id).await else {
        return;
    };
    let key = model_credential_key(&params.id);
    tokio::spawn(async move {
        match store.delete(&key).await {
            Ok(()) => {
                state
                    .send(wire::response(&id, model_credential_ack()))
                    .await
            }
            Err(error) => {
                state
                    .send(wire::error_response(
                        &id,
                        wire::ENGINE_ERROR,
                        &format!("failed to delete model credential: {error}"),
                    ))
                    .await
            }
        }
    });
}

/// Validate the provider id and hand back the endpoint credential store, or send
/// the appropriate error and return `None`. Shared by write and delete so both
/// reject an unsafe id and a missing store the same way.
async fn model_credential_store(
    state: &Arc<AppServerState>,
    id: &Value,
    provider_id: &str,
) -> Option<Arc<dyn CredentialStore>> {
    if !is_valid_model_provider_id(provider_id) {
        state.send(wire::error_response(
            id,
            wire::INVALID_PARAMS,
            "model provider id must contain only ASCII letters, digits, '.', '_', or '-' and be at most 128 bytes",
        )).await;
        return None;
    }
    match state.credential_store.clone() {
        Some(store) => Some(store),
        None => {
            state
                .send(wire::error_response(
                    id,
                    wire::ENGINE_ERROR,
                    "model credential storage requires an endpoint credential store",
                ))
                .await;
            None
        }
    }
}

fn model_credential_ack() -> Value {
    serde_json::to_value(wire::ModelCredentialResult { ok: true }).unwrap_or_default()
}

/// Read a stored model key back for a shell-side provider call. Session auth
/// never uses this — the engine reads the key itself — so the only reader is a
/// host feature (voice transcription) that must call the provider directly.
async fn read_model_credential(
    state: Arc<AppServerState>,
    id: Value,
    params: wire::ModelCredentialReadParams,
) {
    let Some(store) = model_credential_store(&state, &id, &params.id).await else {
        return;
    };
    let key = model_credential_key(&params.id);
    tokio::spawn(async move {
        match store.get(&key).await {
            Ok(api_key) => {
                let result = wire::ModelCredentialReadResult {
                    api_key: api_key.filter(|value| !value.trim().is_empty()),
                };
                state
                    .send(wire::response(
                        &id,
                        serde_json::to_value(result).unwrap_or_default(),
                    ))
                    .await;
            }
            Err(error) => {
                state
                    .send(wire::error_response(
                        &id,
                        wire::ENGINE_ERROR,
                        &format!("failed to read model credential: {error}"),
                    ))
                    .await
            }
        }
    });
}

/// Turn a `modelSelection` into a concrete `provider` + `auth` on the init, from
/// the config file's `[modelProviders]` and the endpoint credential store. This
/// is where a BYO model becomes a live provider: the DEFINITION lives in the
/// config file and the KEY in the credential store, so the host sends only the
/// selection string and the secret never crosses the wire (the engine reads it
/// itself at provider construction via the stored-api-key auth).
///
/// Left untouched when the host already set a `provider` (a subscription session
/// resolves its own OAuth/bridge auth), when there is no selection, or when the
/// selection resolves to no keyed provider — the last so the engine raises
/// auth_required instead of starting on a half-built provider.
const INIT_RESOLVER_FIELDS: &[&str] = &[
    "provider",
    "auth",
    "modelSelection",
    "modelTierSelections",
    "modelTierTargets",
    "advisorTarget",
    "orchestratorTarget",
    "orchestrationMode",
    "featureTargets",
    "modelSource",
    "capabilities",
    "capabilitiesByModel",
    "pricing",
];
const INIT_RESOLVER_CONFIG_FIELDS: &[&str] = &["model", "modelTiers", "contextWindow"];

async fn resolve_raw_model_provider(
    state: &AppServerState,
    init: &mut exact_requests::InitObject,
) -> Result<(), String> {
    let mut projection = Map::new();
    for name in INIT_RESOLVER_FIELDS {
        if let Some(raw) = init.get(name) {
            projection.insert(
                (*name).into(),
                raw.scalar().map_err(|error| format!("{name}: {error}"))?,
            );
        }
    }
    // Only an object can receive the resolver's config edits. Other existing
    // JSON shapes remain with the inner init validator, including explicit null.
    let raw_config = init.get("config");
    let config_is_object = raw_config.is_some_and(|raw| raw.as_str().trim_start().starts_with('{'));
    let can_merge_config = raw_config.is_none() || config_is_object;
    let mut config = raw_config
        .filter(|_| config_is_object)
        .map(exact_requests::InitObject::new)
        .transpose()?;
    if let Some(config) = &config {
        let mut fields = Map::new();
        for name in INIT_RESOLVER_CONFIG_FIELDS {
            if let Some(raw) = config.get(name) {
                fields.insert(
                    (*name).into(),
                    raw.scalar()
                        .map_err(|error| format!("config.{name}: {error}"))?,
                );
            }
        }
        projection.insert("config".into(), Value::Object(fields));
    }
    let before = projection.clone();
    let mut resolved = Value::Object(projection);
    resolve_model_provider(state, &mut resolved).await?;
    let resolved = resolved
        .as_object()
        .expect("model provider resolver preserves its object");
    for name in INIT_RESOLVER_FIELDS {
        if before.get(*name) != resolved.get(*name) {
            match resolved.get(*name) {
                Some(value) => init.set(
                    name,
                    RawJson::from_serialize(value).map_err(|error| error.to_string())?,
                ),
                None => init.remove(name),
            }
        }
    }
    if can_merge_config && before.get("config") != resolved.get("config") {
        let config = config.get_or_insert(exact_requests::InitObject::new(
            &RawJson::from_json("{}".into()).expect("empty object is JSON"),
        )?);
        for name in INIT_RESOLVER_CONFIG_FIELDS {
            let old = before.get("config").and_then(|value| value.get(*name));
            let new = resolved.get("config").and_then(|value| value.get(*name));
            if old != new {
                match new {
                    Some(value) => config.set(
                        name,
                        RawJson::from_serialize(value).map_err(|error| error.to_string())?,
                    ),
                    None => config.remove(name),
                }
            }
        }
        init.set("config", config.raw());
    }
    Ok(())
}

async fn resolve_model_provider(state: &AppServerState, init: &mut Value) -> Result<(), String> {
    let targets = init
        .as_object_mut()
        .and_then(|object| object.remove("modelTierTargets"));
    let advisor = init
        .as_object_mut()
        .and_then(|object| object.remove("advisorTarget"));
    let orchestrator = init
        .as_object_mut()
        .and_then(|object| object.remove("orchestratorTarget"));
    let features = init
        .as_object_mut()
        .and_then(|object| object.remove("featureTargets"));
    let source = init
        .get("modelSelection")
        .and_then(Value::as_str)
        .and_then(|value| {
            value
                .split_once(':')
                .map(|(source, model)| (source.to_string(), model.to_string()))
        });
    resolve_main_model_provider(state, init).await?;
    if let Some((source, requested_id)) = source {
        let model_id = if source == "custom" {
            requested_id
        } else {
            init.pointer("/provider/model")
                .and_then(Value::as_str)
                .unwrap_or(&requested_id)
                .to_string()
        };
        init.as_object_mut()
            .ok_or("model init must be an object")?
            .entry("modelSource")
            .or_insert(serde_json::json!({"source":source,"modelId":model_id}));
    }
    if let Some(targets) = targets {
        let targets: wire::ModelTierTargets = serde_json::from_value(targets)
            .map_err(|error| format!("invalid modelTierTargets: {error}"))?;
        let mut resolved = serde_json::Map::new();
        for (tier, target) in [
            ("high", targets.high),
            ("medium", targets.medium),
            ("low", targets.low),
        ] {
            let value = match target {
                None => Value::Null,
                Some(target) => resolve_tier_target(state, target).await?,
            };
            resolved.insert(tier.to_string(), value);
        }
        init.as_object_mut()
            .ok_or("model init must be an object")?
            .insert("modelTierTargets".into(), Value::Object(resolved));
    }
    if let Some(advisor) = advisor {
        let target: Option<wire::ModelTierTarget> = serde_json::from_value(advisor)
            .map_err(|error| format!("invalid advisorTarget: {error}"))?;
        let value = match target {
            None => Value::Null,
            Some(target) => resolve_tier_target(state, target).await?,
        };
        init.as_object_mut()
            .ok_or("model init must be an object")?
            .insert("advisorTarget".into(), value);
    }
    if let Some(orchestrator) = orchestrator {
        let target: Option<wire::ModelTierTarget> = serde_json::from_value(orchestrator)
            .map_err(|error| format!("invalid orchestratorTarget: {error}"))?;
        let value = match target {
            None => Value::Null,
            Some(target) => resolve_tier_target(state, target).await?,
        };
        init.as_object_mut()
            .ok_or("model init must be an object")?
            .insert("orchestratorTarget".into(), value);
    }
    if let Some(features) = features {
        let features: Option<wire::FeatureTargets> = serde_json::from_value(features)
            .map_err(|error| format!("invalid featureTargets: {error}"))?;
        let value = match features {
            None => Value::Null,
            Some(features) => {
                let mut resolved = serde_json::Map::new();
                for (name, target) in [
                    ("title", features.title),
                    ("prediction", features.prediction),
                    ("memory", features.memory),
                ] {
                    let value = match target {
                        None => Value::Null,
                        Some(target) => resolve_tier_target(state, target).await?,
                    };
                    resolved.insert(name.to_string(), value);
                }
                Value::Object(resolved)
            }
        };
        init.as_object_mut()
            .ok_or("model init must be an object")?
            .insert("featureTargets".into(), value);
    }
    Ok(())
}

async fn resolve_tier_target(
    state: &AppServerState,
    target: wire::ModelTierTarget,
) -> Result<Value, String> {
    let wire::ModelTierTarget::Source { source, model_id } = target else {
        let mut value = serde_json::to_value(target).map_err(|error| error.to_string())?;
        qualify_tier_oauth(&mut value);
        return Ok(value);
    };
    if ![
        "trae",
        "kiro",
        "subscription",
        "codex",
        "qoder",
        "qwenwork",
        "workbuddy",
        "cursor",
        "kimi",
        "bailian",
        "minimax",
        "zhipu",
        "ark",
        "custom",
    ]
    .contains(&source.as_str())
    {
        return Ok(
            serde_json::json!({"type":"unavailable","source":source,"modelId":model_id,"code":"HOST_TARGET_REQUIRED","message":"This model source requires a host-resolved provider target."}),
        );
    }
    if model_id.trim().is_empty()
        || ["auto", "high", "medium", "low", "inherit"].contains(&model_id.as_str())
    {
        return Ok(
            serde_json::json!({"type":"unavailable","source":source,"modelId":model_id,"code":"MODEL_TARGET_UNAVAILABLE","message":"Model target source references must name concrete catalog model IDs."}),
        );
    }
    let mut init = serde_json::json!({"modelSelection":format!("{source}:{model_id}")});
    if let Err(error) = resolve_main_model_provider(state, &mut init).await {
        return Ok(
            serde_json::json!({"type":"unavailable","source":source,"modelId":model_id,"code":"MODEL_TARGET_UNAVAILABLE","message":error}),
        );
    }
    if source != "custom"
        && !init
            .pointer("/provider/model")
            .and_then(Value::as_str)
            .is_some_and(|model| {
                model_sources::model_target_matches_provider(&source, &model_id, model)
            })
    {
        return Ok(
            serde_json::json!({"type":"unavailable","source":source,"modelId":model_id,"code":"MODEL_TARGET_UNAVAILABLE","message":"The selected catalog model is unavailable; source defaults are not used for model targets."}),
        );
    }
    let capabilities = init
        .get("capabilities")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let mut capabilities = capabilities.as_object().cloned().unwrap_or_default();
    if let Some(window) = init.pointer("/config/contextWindow") {
        capabilities.insert("contextWindow".into(), window.clone());
    }
    let mut value = serde_json::json!({
        "type":"resolved","source":source,"modelId":model_id,
        "provider":init["provider"],"auth":init["auth"],"capabilities":capabilities,
    });
    if let Some(pricing) = init.get("pricing") {
        value["pricing"] = pricing.clone();
    }
    qualify_tier_oauth(&mut value);
    Ok(value)
}

pub(super) fn qualify_tier_oauth(value: &mut Value) {
    if value["type"] != "resolved" {
        return;
    }
    let Some(source) = value["source"]
        .as_str()
        .filter(|source| matches!(*source, "subscription" | "codex"))
        .map(str::to_string)
    else {
        return;
    };
    let Some(auth) = value.get_mut("auth").and_then(Value::as_object_mut) else {
        return;
    };
    if auth.get("type").and_then(Value::as_str) != Some("oauth_token")
        || auth.contains_key("credentialId")
    {
        return;
    }
    // The login generation is the token itself: a multi-account credential
    // already carries its per-account namespace from the resolver, and a
    // single-account login keeps the historical token-hash namespace so its
    // refreshed tokens stay reachable.
    let generation = auth
        .get("refreshToken")
        .or_else(|| auth.get("accessToken"))
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(|generation| super::model_sources::oauth_credential_id(&source, generation));
    if let Some(generation) = generation {
        auth.insert("credentialId".into(), serde_json::json!(generation));
    }
}

async fn resolve_main_model_provider(
    state: &AppServerState,
    init: &mut Value,
) -> Result<(), String> {
    let Some(object) = init.as_object_mut() else {
        return Ok(());
    };
    validate_model_source(object)?;
    preserve_model_selection_metadata(object);
    validate_model_source(object)?;
    // `modelSelection` and `modelTierSelections` are app-server-only fields: strip
    // them before the init is forwarded to the (typed) protocol server, whether or
    // not they resolve.
    let selection = object
        .remove("modelSelection")
        .and_then(|value| value.as_str().map(str::to_string));
    // Which model backs each tier is the host's preference, in the source's own id
    // space; what those models can do is answered by the resolver below.
    let tier_selections = object
        .remove("modelTierSelections")
        .and_then(|value| match value {
            Value::Object(map) => Some(map),
            _ => None,
        })
        .unwrap_or_default();
    if object.get("provider").is_some_and(|value| !value.is_null()) {
        return Ok(());
    }
    let Some(selection) = selection else {
        return Ok(());
    };
    // A subscription selection is settled first and on its own terms: its provider
    // comes from a login, not from `[modelProviders]`, so it never touches the
    // credential store or the config section below. It answers the model knowledge
    // with it — capability set, tier map, rates — because the engine infers none of
    // that from a model id and a client that filled it in instead was a second
    // source of truth that drifted.
    if let Some(resolved) = model_sources::resolve(
        &selection,
        &model_sources::config_root_of(&state.config_path),
        &tier_selections,
    )
    .await?
    {
        pin_selected_model_over_config_file(object, &resolved.provider);
        object.insert("provider".to_string(), resolved.provider);
        object.insert("auth".to_string(), resolved.auth);
        object.insert("capabilities".to_string(), resolved.capabilities);
        if !resolved.capabilities_by_model.is_empty() {
            object.insert(
                "capabilitiesByModel".to_string(),
                Value::Array(resolved.capabilities_by_model),
            );
        }
        if let Some(pricing) = resolved.pricing {
            object.insert("pricing".to_string(), pricing);
        }
        if !resolved.model_tiers.is_empty() {
            if let Some(config) = object
                .entry("config")
                .or_insert_with(|| serde_json::json!({}))
                .as_object_mut()
            {
                config
                    .entry("modelTiers")
                    .or_insert_with(|| Value::Object(resolved.model_tiers));
            }
        }
        return Ok(());
    }
    let Some(store) = state.credential_store.clone() else {
        return Err(format!(
            "cannot resolve model {selection}: this endpoint has no credential store"
        ));
    };
    let Ok(document) = config_store::load(&state.config_path).await else {
        return Err(format!(
            "cannot resolve model {selection}: the config file could not be read"
        ));
    };
    let providers = document.product_section("modelProviders");
    let empty = serde_json::Map::new();
    let providers = providers.as_object().unwrap_or(&empty);
    let (id, entry) =
        select_model_provider(providers, &selection, &tier_selections, &store).await?;
    let Some(provider) = model_provider_init(&entry) else {
        return Err(format!(
            "model {selection} resolved to entry {id}, whose provider kind is not one this engine can build"
        ));
    };
    let context_window = entry
        .get("maxContextTokens")
        .and_then(Value::as_i64)
        .filter(|window| *window > 0);
    pin_selected_model_over_config_file(object, &provider);
    object.insert("provider".to_string(), provider);
    object.insert(
        "auth".to_string(),
        serde_json::json!({ "type": "api_key_stored", "credentialId": id }),
    );
    // A BYO model declares its own capabilities in the `[modelProviders]` entry
    // (the settings UI writes them); forward that set so the engine reads it
    // instead of inferring anything from the model id. Absent = minimal.
    if let Some(capabilities) = entry.get("capabilities").filter(|value| value.is_object()) {
        object.insert("capabilities".to_string(), capabilities.clone());
    }
    // The session's tier map: sibling entries' roles become high/medium/low,
    // restricted to entries on the SAME wire as the selected model (provider
    // kind + base URL + stored credential) — the session has one provider, so
    // a tier naming a different endpoint would send traffic to the wrong
    // account. A role identical to the main model needs no entry.
    let main_model = entry
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let (tiers, tier_capabilities) =
        byo_model_tiers(providers, &tier_selections, &id, main_model, &entry, &store).await;
    if !tiers.is_empty() {
        if let Some(config) = object
            .entry("config")
            .or_insert_with(|| serde_json::json!({}))
            .as_object_mut()
        {
            config
                .entry("modelTiers")
                .or_insert_with(|| Value::Object(tiers));
        }
    }
    if !tier_capabilities.is_empty() {
        object
            .entry("capabilitiesByModel")
            .or_insert_with(|| Value::Array(tier_capabilities));
    }
    if let Some(window) = context_window {
        if let Some(config) = object
            .entry("config")
            .or_insert_with(|| serde_json::json!({}))
            .as_object_mut()
        {
            config
                .entry("contextWindow")
                .or_insert_with(|| serde_json::json!(window));
        }
    }
    Ok(())
}

/// Identity is shared with the picker facade; credentials qualify only that
/// chosen entry and must never silently move a session onto another model.
async fn select_model_provider(
    providers: &serde_json::Map<String, Value>,
    selection: &str,
    tier_selections: &serde_json::Map<String, Value>,
    store: &Arc<dyn CredentialStore>,
) -> Result<(String, Value), String> {
    let id = super::model_selection::custom_model_id(providers, selection, tier_selections)?;
    if !provider_has_key(store, id).await {
        return Err(format!(
            "the configured model {id} has no stored API key — set one with `klaus engine credential set {id}`"
        ));
    }
    let entry = providers
        .get(id)
        .expect("the shared selection names an existing entry");
    Ok((id.to_string(), entry.clone()))
}

/// The tier map a BYO session gets from the host's per-source tier picks:
/// `modelTierSelections` maps high/medium/low to a `[modelProviders]` entry id,
/// and a picked entry only qualifies when it rides the SAME wire as the selected
/// model — same provider kind, same base URL, and the same stored credential.
/// The session has a single provider, so letting a tier point at another
/// endpoint would bill the wrong account. Also returns the qualifying entries'
/// declared capability sets, keyed for the wire `capabilitiesByModel` field.
async fn byo_model_tiers(
    providers: &serde_json::Map<String, Value>,
    tier_selections: &serde_json::Map<String, Value>,
    main_id: &str,
    main_model: &str,
    main_entry: &Value,
    store: &Arc<dyn CredentialStore>,
) -> (serde_json::Map<String, Value>, Vec<Value>) {
    let main_key = store
        .get(&model_credential_key(main_id))
        .await
        .ok()
        .flatten();
    let wire_identity = |entry: &Value| {
        let kind = match entry
            .get("provider")
            .and_then(Value::as_str)
            .unwrap_or("anthropic")
        {
            "openai" => "openai",
            "gemini" => "gemini",
            _ => "anthropic",
        };
        let root = entry
            .get("baseUrl")
            .and_then(Value::as_str)
            .filter(|url| !url.is_empty())
            .map(|url| {
                let root = provider_base_root(kind, url);
                reqwest::Url::parse(&root)
                    .map(|url| url.to_string().trim_end_matches('/').to_string())
                    .unwrap_or(root)
            });
        let api = if kind == "openai" {
            entry
                .get("openaiApi")
                .and_then(Value::as_str)
                .filter(|api| !api.is_empty())
                .unwrap_or("chat")
        } else {
            ""
        };
        let auth = if kind == "anthropic"
            && entry.get("authScheme").and_then(Value::as_str) == Some("bearer")
        {
            "bearer"
        } else {
            "native"
        };
        (kind.to_string(), root, api.to_string(), auth.to_string())
    };
    let main_wire = wire_identity(main_entry);
    let same_wire = |entry: &Value| wire_identity(entry) == main_wire;
    let mut tiers = serde_json::Map::new();
    let mut tier_capabilities = Vec::new();
    let mut seen_models: Vec<&str> = Vec::new();
    for tier in ["high", "medium", "low"] {
        let Some(picked) = tier_selections
            .get(tier)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|id| !id.is_empty() && *id != main_id)
        else {
            continue;
        };
        let Some(picked_entry) = providers.get(picked) else {
            continue;
        };
        let Some(picked_model) = picked_entry.get("model").and_then(Value::as_str) else {
            continue;
        };
        if picked_model == main_model || !same_wire(picked_entry) {
            continue;
        }
        let picked_key = store
            .get(&model_credential_key(picked))
            .await
            .ok()
            .flatten();
        if picked_key.is_none() || picked_key != main_key {
            continue;
        }
        tiers.insert(tier.to_string(), Value::String(picked_model.to_string()));
        if seen_models.contains(&picked_model) {
            continue;
        }
        seen_models.push(picked_model);
        let capabilities = picked_entry
            .get("capabilities")
            .filter(|value| value.is_object())
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        // Every distinct tier model is bound, even without metadata. Otherwise
        // the provider would inherit the main model's media and token limits.
        tier_capabilities.push(serde_json::json!({
            "model": picked_model,
            "capabilities": capabilities,
        }));
    }
    // Model discovery is independent from the three auxiliary role choices.
    // Every admitted sibling must use the same protocol, endpoint and key.
    for (id, entry) in providers {
        if id == main_id || !same_wire(entry) {
            continue;
        }
        let Some(model) = entry.get("model").and_then(Value::as_str) else {
            continue;
        };
        if model == main_model || seen_models.contains(&model) {
            continue;
        }
        let key = store.get(&model_credential_key(id)).await.ok().flatten();
        if key.is_none() || key != main_key {
            continue;
        }
        seen_models.push(model);
        tier_capabilities.push(serde_json::json!({"model":model,"capabilities":entry.get("capabilities").filter(|value|value.is_object()).cloned().unwrap_or_else(||serde_json::json!({}))}));
    }
    (tiers, tier_capabilities)
}

async fn provider_has_key(store: &Arc<dyn CredentialStore>, id: &str) -> bool {
    if !is_valid_model_provider_id(id) {
        return false;
    }
    store
        .get(&model_credential_key(id))
        .await
        .ok()
        .flatten()
        .is_some_and(|value| !value.trim().is_empty())
}

/// Build the wire `provider` selection from a `[modelProviders]` entry. The kind
/// (`provider`) decides the engine provider: openai (Chat unless the row asks for
/// Responses), gemini, else Anthropic-compatible — the same mapping the host used
/// to do inline, now driven off the stored definition.
fn pin_selected_model_over_config_file(init: &mut Map<String, Value>, provider: &Value) {
    let Some(model) = provider.get("model").cloned() else {
        return;
    };
    let config = init
        .entry("config")
        .or_insert_with(|| serde_json::json!({}));
    if let Some(config) = config.as_object_mut() {
        // An explicit per-request model wins, but a source pick must not inherit
        // a different model from config.toml during session reconfiguration.
        config.entry("model").or_insert(model);
    }
}

fn model_provider_init(entry: &Value) -> Option<Value> {
    let model = entry
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let kind = entry
        .get("provider")
        .and_then(Value::as_str)
        .unwrap_or("anthropic");
    let mut provider = match kind {
        "openai" => {
            let api = entry
                .get("openaiApi")
                .and_then(Value::as_str)
                .filter(|api| !api.is_empty())
                .unwrap_or("chat");
            serde_json::json!({ "type": "openai", "model": model, "api": api })
        }
        "gemini" => serde_json::json!({ "type": "gemini", "model": model }),
        _ => serde_json::json!({ "type": "anthropic", "model": model }),
    };
    if let Some(base_url) = entry
        .get("baseUrl")
        .and_then(Value::as_str)
        .filter(|url| !url.is_empty())
    {
        provider["baseUrl"] = serde_json::json!(provider_base_root(kind, base_url));
    }
    // A gateway in front of the Anthropic surface may validate only one
    // carrier for the stored key. The entry names it; absent means the
    // Anthropic-native `x-api-key`, so every existing entry is unchanged.
    if kind != "openai"
        && kind != "gemini"
        && entry
            .get("authScheme")
            .and_then(Value::as_str)
            .is_some_and(|scheme| scheme == "bearer")
    {
        provider["apiKeyScheme"] = serde_json::json!("bearer");
    }
    Some(provider)
}

/// The base URL a provider expects, out of what the user actually typed.
///
/// Every provider composes its own request path onto this value, so the field
/// wants an API ROOT. What people have in hand is the endpoint their gateway
/// documented (`…/v1/chat/completions`), and pasting that got the path appended
/// a second time — a URL that only resolves on gateways lenient enough to ignore
/// the extra segments, and that sends the Responses surface somewhere that does
/// not exist at all. The suffix the named provider re-appends itself is stripped
/// here, at the one place a config entry becomes a provider spec, so a
/// hand-written config gets the same treatment as the settings form.
fn provider_base_root(kind: &str, base_url: &str) -> String {
    let suffixes: &[&str] = match kind {
        "openai" => &["/chat/completions", "/responses"],
        // The Gemini surface appends a versioned model path with the model name
        // in it, which nobody can paste ahead of time.
        "gemini" => &[],
        _ => &["/v1/messages", "/v1"],
    };
    let mut root = base_url.trim_end_matches('/');
    for suffix in suffixes {
        if let Some(stripped) = root.strip_suffix(suffix) {
            root = stripped.trim_end_matches('/');
            break;
        }
    }
    root.to_string()
}

/// The hooks file a request targets: an explicit `filePath` (a project's own
/// hooks) or the endpoint's user hooks file.
fn hooks_target(file_path: Option<&str>) -> PathBuf {
    match file_path {
        Some(path) if !path.trim().is_empty() => PathBuf::from(path),
        _ => hooks_store::user_hooks_path(),
    }
}

fn hooks_result(target: &Path, hooks: serde_json::Map<String, Value>) -> Value {
    serde_json::to_value(wire::HooksResult {
        path: target.display().to_string(),
        hooks: Value::Object(hooks),
        events: hooks_store::hook_event_names()
            .into_iter()
            .map(str::to_string)
            .collect(),
    })
    .expect("a hooks result serializes")
}

async fn send_hooks_error(state: &AppServerState, id: &Value, error: &HooksStoreError) {
    state
        .send(wire::error_response_with_data(
            id,
            wire::ENGINE_ERROR,
            &error.to_string(),
            serde_json::json!({ "reason": error.code() }),
        ))
        .await;
}

/// The `qoder/*` methods, in one place.
///
/// Every arm answers with the module's own JSON verbatim. The login flow's shapes
/// are its business, not this layer's: re-describing them here would be a second
/// definition to keep in step, and the host already treats them as opaque.
///
/// A refusal is returned as an ERROR response rather than an `ok: false` body,
/// because a caller that has to inspect a payload to learn the call failed will
/// eventually forget to.
/// Not `async` itself: the work happens on a spawned task, exactly like the other
/// endpoint-state methods, so the dispatcher is never blocked on a network call.
async fn dispatch_qoder_speech(state: Arc<AppServerState>, id: Value, method: &str, params: Value) {
    use super::qoder::speech::Source;
    let source = Source::Qoder;
    if method.ends_with("/cancelTranscription") {
        let params =
            match serde_json::from_value::<wire::SubscriptionCancelTranscriptionParams>(params) {
                Ok(params) => params,
                Err(error) => return send_invalid_params(&state, &id, &error).await,
            };
        match state
            .provider_operations
            .cancel(source.name(), &params.operation_id)
        {
            Ok(cancelled) => {
                state
                    .send(wire::response(
                        &id,
                        serde_json::json!({"cancelled":cancelled}),
                    ))
                    .await
            }
            Err(error) => {
                state
                    .send(wire::error_response(&id, wire::INVALID_PARAMS, &error))
                    .await
            }
        }
        return;
    }
    let params = match serde_json::from_value::<wire::SubscriptionTranscribeParams>(params) {
        Ok(params) => params,
        Err(error) => return send_invalid_params(&state, &id, &error).await,
    };
    let operation = match state
        .provider_operations
        .register(source.name(), params.operation_id.as_deref())
    {
        Ok(operation) => operation,
        Err(error) => {
            return state
                .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                .await
        }
    };
    let root = super::model_sources::config_root_of(&state.config_path);
    match source {
        Source::Qoder => super::qoder::set_config_root(&root),
        Source::QwenWork => unreachable!("QwenWork prepares its own transcription credential"),
    }
    tokio::spawn(async move {
        let mut operation = operation;
        let signal = operation.signal();
        let outcome = tokio::select! {
            biased;
            _ = state.out_tx.closed() => return,
            outcome = super::qoder::speech::transcribe_audio(source, params, signal) => outcome,
        };
        let message = match outcome {
            Ok(response) => {
                operation.bind_publication_check(response.publication_check);
                wire::response(&id, response.value)
            }
            Err(error) => {
                wire::error_response(&id, wire::ENGINE_ERROR, &source.source_error(&error))
            }
        };
        let _ = state
            .out_tx
            .send(Outgoing {
                message: message.into(),
                cursor_operation: None,
                workbuddy_operation: None,
                provider_operation: Some(operation),
            })
            .await;
    });
}

async fn dispatch_qoder(state: Arc<AppServerState>, id: Value, method: &str, params: Value) {
    // The credential follows the endpoint's config root, so every method here sees
    // the same profile the sessions do.
    super::qoder::set_config_root(&super::model_sources::config_root_of(&state.config_path));
    let method = method.to_string();
    tokio::spawn(async move {
        let outcome: Result<Value, String> = match method.as_str() {
            "qoder/status" => Ok(super::qoder::auth_status()),
            "qoder/loginStart" => super::qoder::start().await.map_err(render_qoder_error),
            "qoder/loginPoll" => match decode_qoder::<wire::QoderPollParams>(&params) {
                Ok(params) => super::qoder::poll(&params.flow)
                    .await
                    .map_err(render_qoder_error),
                Err(error) => Err(error),
            },
            "qoder/logout" => super::qoder::logout().map_err(render_qoder_error),
            "qoder/importLocal" => super::qoder::import_local().map_err(render_qoder_error),
            "qoder/adoptCredential" => match decode_qoder::<wire::QoderAdoptParams>(&params) {
                Ok(params) => {
                    super::qoder::adopt_credential(params.user_info).map_err(render_qoder_error)
                }
                Err(error) => Err(error),
            },
            "qoder/models" => match decode_qoder::<wire::QoderModelsParams>(&params) {
                Ok(params) => {
                    // Renew a nearly-expired token before spending it on the
                    // catalog: Qoder has no revoke and no server-side session, so
                    // the stored token is the only thing standing between this
                    // request and a 401. Signed out this is a no-op, and the read
                    // below is what reports that.
                    match super::qoder::ensure_fresh_token().await {
                        Ok(()) => {
                            let models = match params.read {
                                wire::QoderCatalogRead::Warm => {
                                    super::qoder::fetch_models_warm().await
                                }
                                wire::QoderCatalogRead::Cached => {
                                    super::qoder::fetch_models().await
                                }
                            };
                            models
                                .map(|models| {
                                    super::conversation_catalog::response("qoder", models)
                                })
                                .map_err(|error| error.message)
                        }
                        Err(error) => Err(error),
                    }
                }
                Err(error) => Err(error),
            },
            "qoder/usage" => super::qoder::usage().await,
            "qoder/generateImage" => match decode_qoder::<wire::QoderImageParams>(&params) {
                Ok(params) => super::qoder::generate_image(
                    &params.prompt,
                    params.size.as_deref(),
                    &params.session_id,
                )
                .await
                // Base64 rather than a byte array: the bytes cross a JSON-RPC wire,
                // and an array of a few hundred thousand numbers is both enormous
                // and unreadable.
                .map(|bytes| {
                    use base64::Engine as _;
                    serde_json::json!({
                        "imageBase64": base64::engine::general_purpose::STANDARD.encode(bytes)
                    })
                })
                .map_err(|error| error.message),
                Err(error) => Err(error),
            },
            "qoder/setThinkingLevel" => match decode_qoder::<wire::QoderThinkingParams>(&params) {
                Ok(params) => {
                    super::qoder::set_thinking_level(params.level);
                    Ok(serde_json::json!({ "ok": true }))
                }
                Err(error) => Err(error),
            },
            other => Err(format!("unknown method {other}")),
        };
        match outcome {
            Ok(value) => state.send(wire::response(&id, value)).await,
            Err(error) => {
                state
                    .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                    .await
            }
        }
    });
}

async fn dispatch_qwenwork(state: Arc<AppServerState>, id: Value, method: &str, params: Value) {
    super::qwenwork::set_config_root(&super::model_sources::config_root_of(&state.config_path));
    match method {
        "qwenwork/cancelOperation" | "qwenwork/cancelTranscription" => {
            let result =
                decode_qoder::<wire::QwenWorkCancelOperationParams>(&params).and_then(|params| {
                    state
                        .provider_operations
                        .cancel("qwenwork", &params.operation_id)
                        .map(|cancelled| serde_json::json!({"ok":true,"cancelled":cancelled}))
                });
            match result {
                Ok(value) => state.send(wire::response(&id, value)).await,
                Err(error) => {
                    state
                        .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                        .await
                }
            }
            return;
        }
        "qwenwork/transcribeAudio" => {
            match decode_qoder::<wire::SubscriptionTranscribeParams>(&params) {
                Ok(params) => {
                    let operation_id = params.operation_id.clone();
                    spawn_qwenwork_operation(state, id, operation_id, move |signal| {
                        super::qwenwork::capabilities::transcribe_audio(params, signal)
                    })
                    .await;
                }
                Err(error) => {
                    state
                        .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                        .await
                }
            }
            return;
        }
        "qwenwork/generateImage" => {
            match decode_qoder::<wire::QwenWorkImageParams>(&params) {
                Ok(params) => {
                    let operation_id = params.operation_id.clone();
                    spawn_qwenwork_operation(state, id, operation_id, move |signal| {
                        super::qwenwork::generate_image(params, signal)
                    })
                    .await;
                }
                Err(error) => {
                    state
                        .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                        .await
                }
            }
            return;
        }
        "qwenwork/generateVideo" => {
            match decode_qoder::<wire::QwenWorkVideoParams>(&params) {
                Ok(params) => {
                    let operation_id = params.operation_id.clone();
                    spawn_qwenwork_operation(state, id, operation_id, move |signal| {
                        super::qwenwork::generate_video(params, signal)
                    })
                    .await;
                }
                Err(error) => {
                    state
                        .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                        .await
                }
            }
            return;
        }
        "qwenwork/generateMusic" => {
            match decode_qoder::<wire::QwenWorkMusicParams>(&params) {
                Ok(params) => {
                    let operation_id = params.operation_id.clone();
                    spawn_qwenwork_operation(state, id, operation_id, move |signal| {
                        super::qwenwork::generate_music(params, signal)
                    })
                    .await;
                }
                Err(error) => {
                    state
                        .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                        .await
                }
            }
            return;
        }
        "qwenwork/search" => {
            match decode_qoder::<wire::QwenWorkSearchParams>(&params) {
                Ok(params) => {
                    let operation_id = params.operation_id.clone();
                    spawn_qwenwork_operation(state, id, operation_id, move |signal| {
                        super::qwenwork::search(params, signal)
                    })
                    .await;
                }
                Err(error) => {
                    state
                        .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                        .await
                }
            }
            return;
        }
        _ => {}
    }
    let method = method.to_string();
    tokio::spawn(async move {
        let outcome: Result<Value, String> = match method.as_str() {
            "qwenwork/status" => Ok(super::qwenwork::auth_status()),
            "qwenwork/loginStart" => super::qwenwork::start().await.map_err(render_qoder_error),
            "qwenwork/loginPoll" => match decode_qoder::<wire::QoderPollParams>(&params) {
                Ok(params) => super::qwenwork::poll(&params.flow)
                    .await
                    .map_err(render_qoder_error),
                Err(error) => Err(error),
            },
            "qwenwork/logout" => super::qwenwork::logout().map_err(render_qoder_error),
            "qwenwork/refreshToken" => super::qwenwork::refresh_token_now()
                .await
                .map(|()| serde_json::json!({ "ok": true })),
            "qwenwork/importLocal" => super::qwenwork::import_local().map_err(render_qoder_error),
            "qwenwork/adoptCredential" => match decode_qoder::<wire::QoderAdoptParams>(&params) {
                Ok(params) => {
                    super::qwenwork::adopt_credential(params.user_info, params.machine_id)
                        .map_err(render_qoder_error)
                }
                Err(error) => Err(error),
            },
            "qwenwork/models" => match decode_qoder::<wire::QoderModelsParams>(&params) {
                Ok(params) => match super::qwenwork::ensure_fresh_token().await {
                    Ok(()) => {
                        let models = match params.read {
                            wire::QoderCatalogRead::Warm => {
                                super::qwenwork::fetch_models_warm().await
                            }
                            wire::QoderCatalogRead::Cached => super::qwenwork::fetch_models().await,
                        };
                        models
                            .map(|models| super::conversation_catalog::response("qwenwork", models))
                            // The host recovers from a dead copied token by
                            // rereading the official desktop store, which only
                            // it can decrypt; it keys that recovery on this
                            // stable code rather than on the gateway's prose.
                            .map_err(|error| {
                                if error.login_expired {
                                    "qwenwork_login_expired".to_string()
                                } else {
                                    error.message
                                }
                            })
                    }
                    Err(error) => Err(error),
                },
                Err(error) => Err(error),
            },
            "qwenwork/usage" => super::qwenwork::usage().await,
            "qwenwork/capabilities" => decode_qoder::<wire::QwenWorkCapabilitiesParams>(&params)
                .map(|_| super::qwenwork::capabilities::capabilities()),
            "qwenwork/setThinkingLevel" => {
                match decode_qoder::<wire::QoderThinkingParams>(&params) {
                    Ok(params) => {
                        super::qwenwork::set_thinking_level(params.level);
                        Ok(serde_json::json!({ "ok": true }))
                    }
                    Err(error) => Err(error),
                }
            }
            other => Err(format!("unknown method {other}")),
        };
        match outcome {
            Ok(value) => state.send(wire::response(&id, value)).await,
            Err(error) => {
                state
                    .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                    .await
            }
        }
    });
}

async fn spawn_qwenwork_operation<F, Fut>(
    state: Arc<AppServerState>,
    id: Value,
    operation_id: Option<String>,
    work: F,
) where
    F: FnOnce(klaus_agent_core::types::abort::AbortSignal) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<super::qwenwork::NativeResponse, String>> + Send,
{
    let operation = match state
        .provider_operations
        .register("qwenwork", operation_id.as_deref())
    {
        Ok(operation) => operation,
        Err(error) => {
            state
                .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                .await;
            return;
        }
    };
    tokio::spawn(async move {
        let mut operation = operation;
        let signal = operation.signal();
        let result = tokio::select! { biased;
            _ = state.out_tx.closed() => return,
            _ = signal.cancelled() => Err("qwenwork_operation_cancelled".into()),
            result = work(signal.clone()) => result,
        };
        let message = match result {
            Ok(response) => {
                let generation = response.generation;
                operation.bind_publication_check(Arc::new(move || {
                    super::qwenwork::capabilities::current_generation() == generation
                }));
                wire::response(&id, response.value)
            }
            Err(error) => wire::error_response(&id, wire::ENGINE_ERROR, &error),
        };
        let _ = state
            .out_tx
            .send(Outgoing {
                message: message.into(),
                cursor_operation: None,
                workbuddy_operation: None,
                provider_operation: Some(operation),
            })
            .await;
    });
}

async fn dispatch_cursor(state: Arc<AppServerState>, id: Value, method: &str, params: Value) {
    super::cursor::set_config_root(&super::model_sources::config_root_of(&state.config_path));
    match method {
        "cursor/cancelOperation" => {
            let result = decode_qoder::<wire::CursorCancelOperationParams>(&params)
                .and_then(|params| state.cursor_operations.cancel(&params.operation_id));
            match result {
                Ok(value) => state.send(wire::response(&id, value)).await,
                Err(error) => {
                    state
                        .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                        .await
                }
            }
            return;
        }
        "cursor/generateImage" => {
            match decode_qoder::<wire::CursorImageParams>(&params) {
                Ok(params) => {
                    let operation_id = params.operation_id.clone();
                    spawn_cursor_operation(state, id, operation_id, move |signal| {
                        super::cursor::generate_image(params, signal)
                    })
                    .await;
                }
                Err(error) => {
                    state
                        .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                        .await
                }
            }
            return;
        }
        "cursor/generateAudio" => {
            match decode_qoder::<wire::CursorAudioParams>(&params) {
                Ok(params) => {
                    let operation_id = params.operation_id.clone();
                    spawn_cursor_operation(state, id, operation_id, move |signal| {
                        super::cursor::generate_audio(params, signal)
                    })
                    .await;
                }
                Err(error) => {
                    state
                        .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                        .await
                }
            }
            return;
        }
        "cursor/search" => {
            match decode_qoder::<wire::CursorSearchParams>(&params) {
                Ok(params) => {
                    let operation_id = params.operation_id.clone();
                    spawn_cursor_operation(state, id, operation_id, move |signal| {
                        super::cursor::search(params, signal)
                    })
                    .await;
                }
                Err(error) => {
                    state
                        .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                        .await
                }
            }
            return;
        }
        "cursor/generateVideo" => {
            match decode_qoder::<wire::CursorVideoParams>(&params) {
                Ok(params) => {
                    let operation_id = params.operation_id.clone();
                    spawn_cursor_operation(state, id, operation_id, move |signal| {
                        super::cursor::generate_video(params, signal)
                    })
                    .await;
                }
                Err(error) => {
                    state
                        .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                        .await
                }
            }
            return;
        }
        _ => {}
    }
    let method = method.to_string();
    tokio::spawn(async move {
        let result: Result<Value, String> =
            super::cursor::diagnostic_request(id.as_u64(), &method, async {
                match method.as_str() {
                    "cursor/status" => Ok(super::cursor::status_live().await),
                    "cursor/loginStart" => decode_qoder::<wire::CursorLoginStartParams>(&params)
                        .map(|params| super::cursor::login_start(params.client_version)),
                    "cursor/loginPoll" => {
                        match decode_qoder::<wire::CursorLoginFlowParams>(&params) {
                            Ok(params) => Ok(super::cursor::login_poll(&params.flow.flow_id).await),
                            Err(error) => Err(error),
                        }
                    }
                    "cursor/loginCancel" => decode_qoder::<wire::CursorLoginFlowParams>(&params)
                        .map(|params| super::cursor::login_cancel(&params.flow.flow_id)),
                    "cursor/adoptCredential" => {
                        match decode_qoder::<wire::CursorAdoptParams>(&params) {
                            Ok(params) => Ok(super::cursor::adopt_credential(
                                params.user_info,
                                params.expected_credential_id.as_deref(),
                            )),
                            Err(error) => Err(error),
                        }
                    }
                    "cursor/logout" => match decode_qoder::<wire::CursorLogoutParams>(&params) {
                        Ok(params) => {
                            super::cursor::logout(params.expected_credential_id.as_deref())
                        }
                        Err(error) => Err(error),
                    },
                    "cursor/models" => match decode_qoder::<wire::CursorModelsParams>(&params) {
                        Ok(params) => super::cursor::read_models(params.read, params.force_refresh)
                            .await
                            .map(|models| super::conversation_catalog::response("cursor", models)),
                        Err(error) => Err(error),
                    },
                    "cursor/usage" => Ok(super::cursor::usage().await),
                    _ => Err(format!("unknown method {method}")),
                }
            })
            .await;
        match result {
            Ok(value) => state.send(wire::response(&id, value)).await,
            Err(error) => {
                state
                    .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                    .await
            }
        }
    });
}

async fn spawn_cursor_operation<F, Fut>(
    state: Arc<AppServerState>,
    id: Value,
    operation_id: Option<String>,
    work: F,
) where
    F: FnOnce(klaus_agent_core::types::abort::AbortSignal) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<super::cursor::NativeResponse, String>> + Send,
{
    let operation = match state.cursor_operations.register(operation_id.as_deref()) {
        Ok(operation) => operation,
        Err(error) => {
            state
                .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                .await;
            return;
        }
    };
    tokio::spawn(async move {
        let mut operation = operation;
        let signal = operation.signal();
        let result = tokio::select! {
            biased;
            _ = state.out_tx.closed() => return,
            _ = signal.cancelled() => Err("cursor_operation_cancelled".into()),
            result = work(signal.clone()) => result,
        };
        let result = if signal.is_cancelled() {
            Err("cursor_operation_cancelled".into())
        } else {
            result
        };
        let message = match result {
            Ok(response) => {
                operation.bind_generation(response.generation);
                wire::response(&id, response.value)
            }
            Err(error) => wire::error_response(&id, wire::ENGINE_ERROR, &error),
        };
        let _ = state
            .out_tx
            .send(Outgoing {
                message: message.into(),
                cursor_operation: Some(operation),
                workbuddy_operation: None,

                provider_operation: None,
            })
            .await;
    });
}

async fn spawn_workbuddy_operation<F, Fut>(
    state: Arc<AppServerState>,
    id: Value,
    operation_id: Option<String>,
    work: F,
) where
    F: FnOnce(klaus_agent_core::types::abort::AbortSignal) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<super::workbuddy::NativeResponse, String>> + Send,
{
    let operation = match state.workbuddy_operations.register(operation_id.as_deref()) {
        Ok(operation) => operation,
        Err(error) => {
            state
                .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                .await;
            return;
        }
    };
    tokio::spawn(async move {
        let mut operation = operation;
        let signal = operation.signal();
        let result = tokio::select! { biased; _=state.out_tx.closed()=>return, _=signal.cancelled()=>Err("workbuddy_operation_cancelled".into()), result=work(signal.clone())=>result };
        let message = if signal.is_cancelled() {
            wire::error_response(&id, wire::ENGINE_ERROR, "workbuddy_operation_cancelled")
        } else {
            match result {
                Ok(response) => {
                    operation.bind(response.identity);
                    wire::response(&id, response.value)
                }
                Err(error) => wire::error_response(&id, wire::ENGINE_ERROR, &error),
            }
        };
        let _ = state
            .out_tx
            .send(Outgoing {
                message: message.into(),
                cursor_operation: None,
                workbuddy_operation: Some(operation),

                provider_operation: None,
            })
            .await;
    });
}

async fn dispatch_workbuddy(state: Arc<AppServerState>, id: Value, method: &str, params: Value) {
    super::workbuddy::set_config_root(&super::model_sources::config_root_of(&state.config_path));
    macro_rules! operation {
        ($params:ty, $handler:path) => {{
            match decode_qoder::<$params>(&params) {
                Ok(params) => {
                    let operation_id = params.operation_id.clone();
                    spawn_workbuddy_operation(state, id, operation_id, move |signal| {
                        $handler(params, signal)
                    })
                    .await;
                }
                Err(error) => {
                    state
                        .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                        .await
                }
            }
            return;
        }};
    }
    match method {
        "workbuddy/cancelOperation" => {
            let result = decode_qoder::<wire::WorkBuddyCancelParams>(&params)
                .and_then(|params| state.workbuddy_operations.cancel(&params.operation_id));
            match result {
                Ok(value) => state.send(wire::response(&id, value)).await,
                Err(error) => {
                    state
                        .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                        .await
                }
            };
            return;
        }
        "workbuddy/capabilities" => {
            match decode_qoder::<wire::WorkBuddyOperationParams>(&params) {
                Ok(params) => {
                    spawn_workbuddy_operation(
                        state,
                        id,
                        params.operation_id,
                        super::workbuddy::capabilities,
                    )
                    .await
                }
                Err(error) => {
                    state
                        .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                        .await
                }
            }
            return;
        }
        "workbuddy/generateImage" => {
            operation!(wire::WorkBuddyImageParams, super::workbuddy::generate_image)
        }
        "workbuddy/generateVideo" => {
            operation!(wire::WorkBuddyVideoParams, super::workbuddy::generate_video)
        }
        "workbuddy/synthesizeSpeech" => operation!(
            wire::WorkBuddySpeechParams,
            super::workbuddy::synthesize_speech
        ),
        "workbuddy/transcribeAudio" => operation!(
            wire::WorkBuddyTranscribeParams,
            super::workbuddy::transcribe_audio
        ),
        "workbuddy/search" => operation!(wire::WorkBuddySearchParams, super::workbuddy::search),
        "workbuddy/fetchWebPage" => {
            operation!(wire::WorkBuddyFetchParams, super::workbuddy::fetch_web_page)
        }
        "workbuddy/logout" | "workbuddy/adoptCredential" => state.workbuddy_operations.cancel_all(),
        _ => {}
    }

    let method = method.to_string();
    tokio::spawn(async move {
        let outcome: Result<Value, String> = match method.as_str() {
            "workbuddy/status" => Ok(super::workbuddy::auth::status()),
            "workbuddy/loginStart" => {
                match decode_qoder::<wire::WorkBuddyLoginStartParams>(&params) {
                    Ok(params) => super::workbuddy::start(params.region)
                        .await
                        .map_err(render_qoder_error),
                    Err(error) => Err(error),
                }
            }
            "workbuddy/loginPoll" => match decode_qoder::<wire::QoderPollParams>(&params) {
                Ok(params) => super::workbuddy::poll(&params.flow)
                    .await
                    .map_err(render_qoder_error),
                Err(error) => Err(error),
            },
            "workbuddy/logout" => super::workbuddy::logout().map_err(render_qoder_error),
            "workbuddy/refreshToken" => super::workbuddy::refresh_token_now()
                .await
                .map(|()| serde_json::json!({ "ok": true })),
            "workbuddy/importLocal" => super::workbuddy::import_local().map_err(render_qoder_error),
            "workbuddy/adoptCredential" => match decode_qoder::<wire::QoderAdoptParams>(&params) {
                Ok(params) => {
                    super::workbuddy::adopt_credential(params.user_info, params.machine_id)
                        .map_err(render_qoder_error)
                }
                Err(error) => Err(error),
            },
            "workbuddy/models" => match decode_qoder::<wire::QoderModelsParams>(&params) {
                Ok(params) => match super::workbuddy::ensure_fresh_token().await {
                    Ok(()) => {
                        let models = match params.read {
                            wire::QoderCatalogRead::Warm => {
                                super::workbuddy::fetch_models_warm().await
                            }
                            wire::QoderCatalogRead::Cached => {
                                super::workbuddy::fetch_models().await
                            }
                        };
                        models
                            .map(|models| {
                                super::conversation_catalog::response("workbuddy", models)
                            })
                            // The host recovers from a dead copied token by
                            // rereading WorkBuddy's own store, which only it can
                            // decrypt; it keys that recovery on this stable code
                            // rather than on the gateway's prose.
                            .map_err(|error| {
                                if error.login_expired {
                                    "workbuddy_login_expired".to_string()
                                } else {
                                    error.message
                                }
                            })
                    }
                    Err(error) => Err(error),
                },
                Err(error) => Err(error),
            },
            "workbuddy/usage" => super::workbuddy::usage().await,
            other => Err(format!("unknown method {other}")),
        };
        match outcome {
            Ok(value) => state.send(wire::response(&id, value)).await,
            Err(error) => {
                state
                    .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                    .await
            }
        }
    });
}

async fn spawn_antigravity_operation<F, Fut>(
    state: Arc<AppServerState>,
    id: Value,
    operation_id: Option<String>,
    session_id: Option<String>,
    work: F,
) where
    F: FnOnce(klaus_agent_core::types::abort::AbortSignal) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<Value, String>> + Send,
{
    if let Some(session_id) = session_id.as_deref() {
        if state.session(session_id).is_none() {
            send_unknown_session(&state, &id, session_id).await;
            return;
        }
    }
    let mut operation = match state
        .provider_operations
        .register("antigravity", operation_id.as_deref())
    {
        Ok(operation) => operation,
        Err(error) => {
            state
                .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                .await;
            return;
        }
    };
    let generation = super::antigravity::auth::generation();
    let owner = Arc::downgrade(&state);
    operation.bind_publication_check(Arc::new(move || {
        super::antigravity::auth::generation() == generation
            && owner.upgrade().is_some_and(|state| {
                session_id
                    .as_deref()
                    .is_none_or(|session| state.session(session).is_some())
            })
    }));
    tokio::spawn(async move {
        let signal = operation.signal();
        let outcome = tokio::select! {
            biased;
            _=state.out_tx.closed()=>return,
            _=operation.invalidated()=>Err(operation.publication_error().unwrap_or_else(|| "antigravity_operation_cancelled".into())),
            outcome=work(signal.clone())=>outcome,
        };
        let message = match outcome {
            Ok(value) => wire::response(&id, value),
            Err(error) => wire::error_response(&id, wire::ENGINE_ERROR, &error),
        };
        let _ = state
            .out_tx
            .send(Outgoing {
                message: message.into(),
                cursor_operation: None,
                workbuddy_operation: None,
                provider_operation: Some(operation),
            })
            .await;
    });
}

async fn dispatch_kiro(state: Arc<AppServerState>, id: Value, method: &str, params: Value) {
    super::kiro::set_config_root(&super::model_sources::config_root_of(&state.config_path));
    if method == "kiro/cancelOperation" {
        let result = decode_qoder::<wire::WorkBuddyCancelParams>(&params).and_then(|params| {
            state
                .provider_operations
                .cancel("kiro", &params.operation_id)
        });
        match result {
            Ok(cancelled) => {
                state
                    .send(wire::response(
                        &id,
                        serde_json::json!({"ok":true,"cancelled":cancelled}),
                    ))
                    .await
            }
            Err(error) => {
                state
                    .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                    .await
            }
        }
        return;
    }
    if method == "kiro/search" {
        let params = match decode_qoder::<wire::TraeSearchParams>(&params) {
            Ok(params) => params,
            Err(error) => {
                state
                    .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                    .await;
                return;
            }
        };
        if let Some(session) = params.session_id.as_deref() {
            if state.session(session).is_none() {
                send_unknown_session(&state, &id, session).await;
                return;
            }
        }
        let mut operation = match state
            .provider_operations
            .register("kiro", params.operation_id.as_deref())
        {
            Ok(value) => value,
            Err(error) => {
                state
                    .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                    .await;
                return;
            }
        };
        let generation = super::kiro::auth::generation();
        let owner = Arc::downgrade(&state);
        operation.bind_publication_check(Arc::new(move || {
            super::kiro::auth::generation() == generation
                && owner.upgrade().is_some_and(|s| {
                    !s.connection_closed.load(Ordering::Acquire)
                        && params
                            .session_id
                            .as_deref()
                            .is_none_or(|id| s.session(id).is_some())
                })
        }));
        tokio::spawn(async move {
            let outcome = tokio::select! {
                biased;
                _=state.out_tx.closed()=>return,
                _=operation.invalidated()=>Err(operation.publication_error().unwrap_or_else(||"kiro_search_cancelled".into())),
                outcome=super::kiro::search(&params.query,operation.signal())=>outcome,
            };
            let message = match outcome {
                Ok(value) if operation.publication_error().is_none() => wire::response(&id, value),
                Ok(_) => wire::error_response(&id, wire::ENGINE_ERROR, "kiro_credential_changed"),
                Err(error) => wire::error_response(&id, wire::ENGINE_ERROR, &error),
            };
            let _ = state
                .out_tx
                .send(Outgoing {
                    message: message.into(),
                    cursor_operation: None,
                    workbuddy_operation: None,
                    provider_operation: Some(operation),
                })
                .await;
        });
        return;
    }
    let owner = Arc::as_ptr(&state) as usize;
    let outcome = match method {
        "kiro/loginPoll" | "kiro/loginCancel" => {
            match decode_qoder::<wire::TraeLoginFlowParams>(&params) {
                Ok(params) => Ok(if method == "kiro/loginPoll" {
                    super::kiro::login::poll(owner, &params.flow.flow_id).await
                } else {
                    super::kiro::login::cancel(owner, &params.flow.flow_id).await
                }),
                Err(error) => Err(error),
            }
        }
        "kiro/adoptCredential" => match decode_qoder::<wire::KiroAdoptParams>(&params) {
            Ok(params) => super::kiro::auth::adopt(params.credential, params.local_follow).await,
            Err(error) => Err(error),
        },
        "kiro/models" => match decode_qoder::<wire::QoderModelsParams>(&params) {
            Ok(params) => super::kiro::models(params.read == wire::QoderCatalogRead::Warm).await,
            Err(error) => Err(error),
        },
        _ => match decode_qoder::<wire::QoderParams>(&params) {
            Err(error) => Err(error),
            Ok(_) => match method {
                "kiro/status" => Ok(super::kiro::auth::status()),
                "kiro/loginStart" => {
                    Ok(super::kiro::login::start(owner, state.connection_closed.clone()).await)
                }
                "kiro/logout" => super::kiro::auth::logout().await,
                "kiro/usage" => super::kiro::usage().await,
                "kiro/capabilities" => super::kiro::capabilities().await,
                _ => Err("kiro_method_unavailable".into()),
            },
        },
    };
    match outcome {
        Ok(value) => state.send(wire::response(&id, value)).await,
        Err(error) => {
            state
                .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                .await
        }
    }
}

async fn dispatch_trae(state: Arc<AppServerState>, id: Value, method: &str, params: Value) {
    super::trae::set_config_root(&super::model_sources::config_root_of(&state.config_path));
    match method {
        "trae/cancelOperation" => {
            let result = decode_qoder::<wire::WorkBuddyCancelParams>(&params).and_then(|params| {
                state
                    .provider_operations
                    .cancel("trae", &params.operation_id)
            });
            match result {
                Ok(cancelled) => {
                    state
                        .send(wire::response(
                            &id,
                            serde_json::json!({"cancelled":cancelled}),
                        ))
                        .await
                }
                Err(error) => {
                    state
                        .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                        .await
                }
            }
            return;
        }
        "trae/transcribeAudio" => {
            match decode_qoder::<wire::SubscriptionTranscribeParams>(&params) {
                Ok(params) => {
                    spawn_trae_operation(
                        state,
                        id,
                        params.operation_id.clone(),
                        None,
                        move |signal| super::trae::audio::transcribe(params, signal),
                    )
                    .await
                }
                Err(error) => {
                    state
                        .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                        .await
                }
            }
            return;
        }
        "trae/search" => {
            match decode_qoder::<wire::TraeSearchParams>(&params) {
                Ok(params) => {
                    let value = match serde_json::to_value(&params) {
                        Ok(value) => value,
                        Err(error) => {
                            state
                                .send(wire::error_response(
                                    &id,
                                    wire::ENGINE_ERROR,
                                    &error.to_string(),
                                ))
                                .await;
                            return;
                        }
                    };
                    spawn_trae_operation(
                        state,
                        id,
                        params.operation_id,
                        params.session_id,
                        move |signal| super::trae::native::search(value, signal),
                    )
                    .await;
                }
                Err(error) => {
                    state
                        .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                        .await
                }
            }
            return;
        }
        "trae/generateImage" => {
            match decode_qoder::<wire::TraeImageParams>(&params) {
                Ok(params) => {
                    let value = match serde_json::to_value(&params) {
                        Ok(value) => value,
                        Err(error) => {
                            state
                                .send(wire::error_response(
                                    &id,
                                    wire::ENGINE_ERROR,
                                    &error.to_string(),
                                ))
                                .await;
                            return;
                        }
                    };
                    spawn_trae_operation(
                        state,
                        id,
                        params.operation_id,
                        params.session_id,
                        move |signal| super::trae::native::generate_image(value, signal),
                    )
                    .await;
                }
                Err(error) => {
                    state
                        .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                        .await
                }
            }
            return;
        }
        "trae/generateVideo" => {
            match decode_qoder::<wire::TraeVideoParams>(&params) {
                Ok(params) => {
                    let value = match serde_json::to_value(&params) {
                        Ok(value) => value,
                        Err(error) => {
                            state
                                .send(wire::error_response(
                                    &id,
                                    wire::ENGINE_ERROR,
                                    &error.to_string(),
                                ))
                                .await;
                            return;
                        }
                    };
                    spawn_trae_operation(
                        state,
                        id,
                        params.operation_id,
                        params.session_id,
                        move |signal| super::trae::native::generate_video(value, signal),
                    )
                    .await;
                }
                Err(error) => {
                    state
                        .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                        .await
                }
            }
            return;
        }
        _ => {}
    }
    let method = method.to_owned();
    tokio::spawn(async move {
        let outcome = match method.as_str() {
            "trae/status" => Ok(super::trae::auth::status()),
            "trae/loginStart" => match decode_qoder::<wire::TraeLoginStartParams>(&params) {
                Ok(params) => Ok(super::trae::login::start(
                    Arc::as_ptr(&state) as usize,
                    state.connection_closed.clone(),
                    &params.excluded_device_ids,
                    params.local_device_proof.as_ref(),
                )
                .await),
                Err(error) => Err(error),
            },
            "trae/loginPoll" => match decode_qoder::<wire::TraeLoginFlowParams>(&params) {
                Ok(params) => Ok(super::trae::login::poll(
                    Arc::as_ptr(&state) as usize,
                    &params.flow.flow_id,
                )
                .await),
                Err(error) => Err(error),
            },
            "trae/loginCancel" => match decode_qoder::<wire::TraeLoginFlowParams>(&params) {
                Ok(params) => Ok(super::trae::login::cancel(
                    Arc::as_ptr(&state) as usize,
                    &params.flow.flow_id,
                )
                .await),
                Err(error) => Err(error),
            },
            "trae/adoptCredential" => match decode_qoder::<wire::TraeAdoptParams>(&params) {
                Ok(params) => super::trae::auth::adopt(params.credential).await,
                Err(error) => Err(error),
            },
            "trae/logout" => super::trae::auth::logout().await,
            "trae/models" => match decode_qoder::<wire::QoderModelsParams>(&params) {
                Ok(params) => {
                    super::trae::models(params.read == wire::QoderCatalogRead::Warm).await
                }
                Err(error) => Err(error),
            },
            "trae/usage" => super::trae::usage().await,
            "trae/capabilities" => super::trae::capabilities().await,
            _ => Err("trae_method_unavailable".into()),
        };
        match outcome {
            Ok(value) => state.send(wire::response(&id, value)).await,
            Err(error) => {
                state
                    .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                    .await
            }
        }
    });
}

async fn spawn_trae_operation<F, Fut>(
    state: Arc<AppServerState>,
    id: Value,
    operation_id: Option<String>,
    session_id: Option<String>,
    work: F,
) where
    F: FnOnce(klaus_agent_core::types::abort::AbortSignal) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<super::trae::native::NativeResponse, String>> + Send,
{
    if let Some(session) = session_id.as_deref() {
        if state.session(session).is_none() {
            send_unknown_session(&state, &id, session).await;
            return;
        }
    }
    let mut operation = match state
        .provider_operations
        .register("trae", operation_id.as_deref())
    {
        Ok(operation) => operation,
        Err(error) => {
            state
                .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                .await;
            return;
        }
    };
    let generation = super::trae::auth::generation();
    let owner = Arc::downgrade(&state);
    operation.bind_publication_check(Arc::new(move || {
        super::trae::auth::generation() == generation
            && owner.upgrade().is_some_and(|state| {
                session_id
                    .as_deref()
                    .is_none_or(|session| state.session(session).is_some())
            })
    }));
    tokio::spawn(async move {
        let outcome = tokio::select! {
            biased;
            _=state.out_tx.closed()=>return,
            _=operation.invalidated()=>Err(operation.publication_error().unwrap_or_else(||"trae_operation_cancelled".into())),
            outcome=work(operation.signal())=>outcome,
        };
        let message = match outcome {
            Ok(response) if (response.publication_check)() => wire::response(&id, response.value),
            Ok(_) => wire::error_response(&id, wire::ENGINE_ERROR, "trae_credential_changed"),
            Err(error) => wire::error_response(&id, wire::ENGINE_ERROR, &error),
        };
        let _ = state
            .out_tx
            .send(Outgoing {
                message: message.into(),
                cursor_operation: None,
                workbuddy_operation: None,
                provider_operation: Some(operation),
            })
            .await;
    });
}

async fn dispatch_antigravity(state: Arc<AppServerState>, id: Value, method: &str, params: Value) {
    super::antigravity::set_config_root(&super::model_sources::config_root_of(&state.config_path));
    match method {
        "antigravity/transcribeAudio" => {
            match decode_qoder::<wire::AntigravityTranscribeParams>(&params) {
                Ok(params) => {
                    spawn_antigravity_operation(
                        state,
                        id,
                        params.operation_id.clone(),
                        None,
                        move |signal| super::antigravity::transcribe_audio(params, signal),
                    )
                    .await
                }
                Err(error) => {
                    state
                        .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                        .await
                }
            }
            return;
        }
        "antigravity/cancelOperation" => {
            let result = decode_qoder::<wire::WorkBuddyCancelParams>(&params).and_then(|params| {
                state
                    .provider_operations
                    .cancel("antigravity", &params.operation_id)
            });
            match result {
                Ok(cancelled) => {
                    state
                        .send(wire::response(
                            &id,
                            serde_json::json!({"cancelled":cancelled}),
                        ))
                        .await
                }
                Err(error) => {
                    state
                        .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                        .await
                }
            }
            return;
        }
        "antigravity/generateImage" => {
            match decode_qoder::<wire::AntigravityImageParams>(&params) {
                Ok(params) => {
                    spawn_antigravity_operation(
                        state,
                        id,
                        params.operation_id.clone(),
                        params.session_id.clone(),
                        move |signal| super::antigravity::generate_image(params, signal),
                    )
                    .await
                }
                Err(error) => {
                    state
                        .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                        .await
                }
            }
            return;
        }
        "antigravity/search" => {
            match decode_qoder::<wire::AntigravitySearchParams>(&params) {
                Ok(params) => {
                    spawn_antigravity_operation(
                        state,
                        id,
                        params.operation_id.clone(),
                        params.session_id.clone(),
                        move |signal| super::antigravity::search(params, signal),
                    )
                    .await
                }
                Err(error) => {
                    state
                        .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                        .await
                }
            }
            return;
        }
        _ => {}
    }
    let method = method.to_string();
    tokio::spawn(async move {
        let outcome = match method.as_str() {
            "antigravity/status" => Ok(super::antigravity::auth::status()),
            "antigravity/loginStart" => {
                match decode_qoder::<wire::AntigravityLoginStartParams>(&params) {
                    Ok(_) => Ok(super::antigravity::login::start(
                        Arc::as_ptr(&state) as usize,
                        state.connection_closed.clone(),
                    )
                    .await),
                    Err(error) => Err(error),
                }
            }
            "antigravity/loginPoll" => {
                match decode_qoder::<wire::AntigravityLoginFlowParams>(&params) {
                    Ok(params) => Ok(super::antigravity::login::poll(
                        Arc::as_ptr(&state) as usize,
                        &params.flow.flow_id,
                    )
                    .await),
                    Err(error) => Err(error),
                }
            }
            "antigravity/loginCancel" => {
                match decode_qoder::<wire::AntigravityLoginFlowParams>(&params) {
                    Ok(params) => Ok(super::antigravity::login::cancel(
                        Arc::as_ptr(&state) as usize,
                        &params.flow.flow_id,
                    )
                    .await),
                    Err(error) => Err(error),
                }
            }
            "antigravity/adoptCredential" => {
                match decode_qoder::<wire::AntigravityAdoptParams>(&params) {
                    Ok(params) => super::antigravity::auth::adopt(params.credential).await,
                    Err(error) => Err(error),
                }
            }
            "antigravity/logout" => super::antigravity::auth::logout().await,
            "antigravity/models" => match decode_qoder::<wire::QoderModelsParams>(&params) {
                Ok(params) => {
                    super::antigravity::models(params.read == wire::QoderCatalogRead::Warm).await
                }
                Err(error) => Err(error),
            },
            "antigravity/usage" => super::antigravity::usage().await,
            "antigravity/capabilities" => super::antigravity::capabilities().await,
            _ => Err("antigravity_method_unavailable".into()),
        };
        match outcome {
            Ok(value) => state.send(wire::response(&id, value)).await,
            Err(error) => {
                state
                    .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                    .await
            }
        }
    });
}

async fn dispatch_kimi(state: Arc<AppServerState>, id: Value, method: &str, params: Value) {
    super::kimi::set_config_root(&super::model_sources::config_root_of(&state.config_path));
    let method = method.to_string();
    tokio::spawn(async move {
        let outcome: Result<Value, String> = match method.as_str() {
            "kimi/status" => Ok(super::kimi::status()),
            "kimi/setApiKey" => match decode_qoder::<wire::KimiApiKeyParams>(&params) {
                Ok(params) => super::kimi::set_api_key(&params.api_key, params.api_base.as_deref())
                    .await
                    .map_err(render_qoder_error),
                Err(error) => Err(error),
            },
            "kimi/loginStart" => super::kimi::start().await.map_err(render_qoder_error),
            "kimi/loginPoll" => match decode_qoder::<wire::QoderPollParams>(&params) {
                Ok(params) => super::kimi::poll(&params.flow)
                    .await
                    .map_err(render_qoder_error),
                Err(error) => Err(error),
            },
            "kimi/logout" => super::kimi::logout().map_err(render_qoder_error),
            "kimi/importLocal" => super::kimi::import_local().map_err(render_qoder_error),
            "kimi/models" => match decode_qoder::<wire::QoderModelsParams>(&params) {
                Ok(params) => match super::kimi::ensure_fresh_token().await {
                    Ok(()) => {
                        let models = match params.read {
                            wire::QoderCatalogRead::Warm => super::kimi::fetch_models_warm().await,
                            wire::QoderCatalogRead::Cached => super::kimi::fetch_models().await,
                        };
                        models
                            .map(|models| super::conversation_catalog::response("kimi", models))
                            // A refused credential is keyed on this stable code so
                            // the host sends the user back to a login rather than
                            // retrying a secret that has gone.
                            .map_err(|error| {
                                if error.login_expired {
                                    "kimi_login_expired".to_string()
                                } else {
                                    error.message
                                }
                            })
                    }
                    Err(error) => Err(error),
                },
                Err(error) => Err(error),
            },
            "kimi/usage" => super::kimi::usage().await,
            other => Err(format!("unknown method {other}")),
        };
        match outcome {
            Ok(value) => state.send(wire::response(&id, value)).await,
            Err(error) => {
                state
                    .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                    .await
            }
        }
    });
}

async fn dispatch_bailian(state: Arc<AppServerState>, id: Value, method: &str, params: Value) {
    super::bailian::set_config_root(&super::model_sources::config_root_of(&state.config_path));
    let method = method.to_string();
    tokio::spawn(async move {
        let outcome: Result<Value, String> = match method.as_str() {
            "bailian/status" => Ok(super::bailian::status()),
            "bailian/setApiKey" => match decode_qoder::<wire::BailianApiKeyParams>(&params) {
                Ok(params) => {
                    super::bailian::set_api_key(&params.api_key, params.region.as_deref())
                        .await
                        .map_err(render_qoder_error)
                }
                Err(error) => Err(error),
            },
            "bailian/logout" => super::bailian::logout().map_err(render_qoder_error),
            // The plan's speech route is run by the host, so it needs the stored
            // credential and the plan gateway root that goes with it. The base URL
            // comes from here rather than being rebuilt in the host so the site list
            // has exactly one definition.
            "bailian/mediaCredential" => Ok(super::bailian::media_credential()),
            "bailian/models" => match decode_qoder::<wire::QoderModelsParams>(&params) {
                Ok(params) => {
                    let models = match params.read {
                        wire::QoderCatalogRead::Warm => super::bailian::fetch_models_warm().await,
                        wire::QoderCatalogRead::Cached => super::bailian::fetch_models().await,
                    };
                    models
                        .map(|models| super::conversation_catalog::response("bailian", models))
                        // A refused key is keyed on this stable code so the host
                        // sends the user back to re-enter it rather than retrying a
                        // secret that has gone.
                        .map_err(|error| {
                            if error.login_expired {
                                "bailian_key_rejected".to_string()
                            } else {
                                error.message
                            }
                        })
                }
                Err(error) => Err(error),
            },
            "bailian/usage" => super::bailian::usage().await,
            "bailian/consoleLogin" => super::bailian::console_login_start(),
            "bailian/consoleStatus" => Ok(super::bailian::console_status()),
            "bailian/consoleLogout" => {
                super::bailian::clear_console_token();
                Ok(serde_json::json!({ "ok": true }))
            }
            other => Err(format!("unknown method {other}")),
        };
        match outcome {
            Ok(value) => state.send(wire::response(&id, value)).await,
            Err(error) => {
                state
                    .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                    .await
            }
        }
    });
}

async fn dispatch_minimax(state: Arc<AppServerState>, id: Value, method: &str, params: Value) {
    super::minimax::set_config_root(&super::model_sources::config_root_of(&state.config_path));
    let method = method.to_string();
    tokio::spawn(async move {
        let outcome: Result<Value, String> = match method.as_str() {
            "minimax/status" => Ok(super::minimax::status()),
            "minimax/setApiKey" => match decode_qoder::<wire::MinimaxApiKeyParams>(&params) {
                Ok(params) => {
                    super::minimax::set_api_key(&params.api_key, params.region.as_deref())
                        .await
                        .map_err(render_qoder_error)
                }
                Err(error) => Err(error),
            },
            "minimax/logout" => super::minimax::logout().map_err(render_qoder_error),
            "minimax/models" => match decode_qoder::<wire::QoderModelsParams>(&params) {
                Ok(params) => {
                    let models = match params.read {
                        wire::QoderCatalogRead::Warm => super::minimax::fetch_models_warm().await,
                        wire::QoderCatalogRead::Cached => super::minimax::fetch_models().await,
                    };
                    models
                        .map(|models| super::conversation_catalog::response("minimax", models))
                        // A refused key is keyed on this stable code so the host
                        // sends the user back to re-enter it rather than retrying a
                        // secret that has gone.
                        .map_err(|error| {
                            if error.login_expired {
                                "minimax_key_rejected".to_string()
                            } else {
                                error.message
                            }
                        })
                }
                Err(error) => Err(error),
            },
            "minimax/usage" => super::minimax::usage().await,
            // The plan's media routes are run by the host, so it needs the stored
            // credential and the service root that goes with it. The base URL comes
            // from here rather than being rebuilt in the host so the site list has
            // exactly one definition.
            "minimax/mediaCredential" => Ok(super::minimax::media_credential()),
            other => Err(format!("unknown method {other}")),
        };
        match outcome {
            Ok(value) => state.send(wire::response(&id, value)).await,
            Err(error) => {
                state
                    .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                    .await
            }
        }
    });
}

fn decode_qoder<T: serde::de::DeserializeOwned>(params: &Value) -> Result<T, String> {
    serde_json::from_value::<T>(params.clone()).map_err(|error| error.to_string())
}

async fn dispatch_ark(state: Arc<AppServerState>, id: Value, method: &str, params: Value) {
    super::ark::set_config_root(&super::model_sources::config_root_of(&state.config_path));
    let method = method.to_string();
    tokio::spawn(async move {
        let outcome: Result<Value, String> = match method.as_str() {
            "ark/status" => Ok(super::ark::status()),
            "ark/setApiKey" => match decode_qoder::<wire::ArkApiKeyParams>(&params) {
                Ok(params) => super::ark::set_api_key(&params.api_key, params.region.as_deref())
                    .await
                    .map_err(render_qoder_error),
                Err(error) => Err(error),
            },
            "ark/logout" => super::ark::logout().map_err(render_qoder_error),
            "ark/models" => match decode_qoder::<wire::QoderModelsParams>(&params) {
                Ok(params) => {
                    let models = match params.read {
                        wire::QoderCatalogRead::Warm => super::ark::fetch_models_warm().await,
                        wire::QoderCatalogRead::Cached => super::ark::fetch_models().await,
                    };
                    models
                        .map(|models| super::conversation_catalog::response("ark", models))
                        // A refused key is keyed on this stable code so the host
                        // sends the user back to re-enter it rather than retrying a
                        // secret that has gone.
                        .map_err(|error| {
                            if error.login_expired {
                                "ark_key_rejected".to_string()
                            } else {
                                error.message
                            }
                        })
                }
                Err(error) => Err(error),
            },
            "ark/usage" => super::ark::usage().await,
            other => Err(format!("unknown method {other}")),
        };
        match outcome {
            Ok(value) => state.send(wire::response(&id, value)).await,
            Err(error) => {
                state
                    .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                    .await
            }
        }
    });
}

async fn dispatch_ark_console(state: Arc<AppServerState>, id: Value, method: &str, params: Value) {
    super::ark::set_config_root(&super::model_sources::config_root_of(&state.config_path));
    let method = method.to_string();
    tokio::spawn(async move {
        let outcome: Result<Value, String> = match method.as_str() {
            // Binding the listener is synchronous and returns at once; the browser
            // round trip completes on the user's clock and the host polls status.
            "ark/consoleLogin" => super::ark::console::console_login_start(),
            "ark/consoleStatus" => Ok(super::ark::console::console_status()),
            "ark/consoleLogout" => {
                super::ark::console::console_logout().map_err(render_qoder_error)
            }
            other => Err(format!("unknown method {other}")),
        };
        let _ = params;
        match outcome {
            Ok(value) => state.send(wire::response(&id, value)).await,
            Err(error) => {
                state
                    .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                    .await
            }
        }
    });
}

async fn dispatch_zhipu(state: Arc<AppServerState>, id: Value, method: &str, params: Value) {
    super::zhipu::set_config_root(&super::model_sources::config_root_of(&state.config_path));
    let method = method.to_string();
    tokio::spawn(async move {
        let outcome: Result<Value, String> = match method.as_str() {
            "zhipu/status" => Ok(super::zhipu::status()),
            "zhipu/setApiKey" => match decode_qoder::<wire::ZhipuApiKeyParams>(&params) {
                Ok(params) => super::zhipu::set_api_key(&params.api_key, params.region.as_deref())
                    .await
                    .map_err(render_qoder_error),
                Err(error) => Err(error),
            },
            "zhipu/logout" => super::zhipu::logout().map_err(render_qoder_error),
            "zhipu/models" => match decode_qoder::<wire::QoderModelsParams>(&params) {
                Ok(params) => {
                    let models = match params.read {
                        wire::QoderCatalogRead::Warm => super::zhipu::fetch_models_warm().await,
                        wire::QoderCatalogRead::Cached => super::zhipu::fetch_models().await,
                    };
                    models
                        .map(|models| super::conversation_catalog::response("zhipu", models))
                        // A refused key is keyed on this stable code so the host
                        // sends the user back to re-enter it rather than retrying a
                        // secret that has gone.
                        .map_err(|error| {
                            if error.login_expired {
                                "zhipu_key_rejected".to_string()
                            } else {
                                error.message
                            }
                        })
                }
                Err(error) => Err(error),
            },
            "zhipu/usage" => super::zhipu::usage().await,
            other => Err(format!("unknown method {other}")),
        };
        match outcome {
            Ok(value) => state.send(wire::response(&id, value)).await,
            Err(error) => {
                state
                    .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                    .await
            }
        }
    });
}

/// The login flow reports failure as a JSON value; this is the message inside it.
fn render_qoder_error(error: Value) -> String {
    error
        .get("error")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| error.to_string())
}

/// The compiled built-in skill roster, projected for `engine/builtinSkills`.
/// Derived from `builtin_skills()` rather than restated, so a skill added there
/// shows up here without a second edit.
fn builtin_skill_roster() -> wire::EngineBuiltinSkillsResult {
    // The built-in bodies are code-defined ASCII, so the UTF-8 projection cannot
    // fail; `expect` states that rather than hiding a truncated roster behind a
    // silent default.
    let text = |value: &klaus_agent_core::runtime_values::RuntimeString| {
        value
            .try_to_utf8()
            .expect("a built-in skill field is scalar text")
    };
    wire::EngineBuiltinSkillsResult {
        builtin_skills: klaus_agent_core::extensions::skills::builtin_skills::builtin_skills()
            .into_iter()
            .map(|skill| wire::BuiltinSkillRow {
                name: text(&skill.name),
                description: text(&skill.description),
                when_to_use: skill.when_to_use.as_ref().map(text),
                argument_hint: skill.argument_hint.as_ref().map(text),
                // Absent means the default `true` (Skill::user_invocable).
                user_invocable: skill.user_invocable.unwrap_or(true),
            })
            .collect(),
    }
}

/// The compiled built-in agent roster, projected for `engine/builtinAgents`.
/// Derived from `BUILTIN_AGENTS` rather than restated, so a role added or
/// re-tiered there shows up here without a second edit.
fn builtin_agent_roster() -> wire::EngineBuiltinAgentsResult {
    // The built-in descriptions are code-defined ASCII, so the UTF-8 projection
    // cannot fail; `expect` states that rather than hiding a truncated roster
    // behind a silent default.
    let text = |value: &klaus_agent_core::runtime_values::RuntimeString| {
        value
            .try_to_utf8()
            .expect("a built-in agent field is scalar text")
    };
    wire::EngineBuiltinAgentsResult {
        builtin_agents: klaus_agent_core::engine::BUILTIN_AGENTS
            .iter()
            .map(|agent| wire::BuiltinAgentRow {
                name: text(&agent.name),
                description: text(&agent.description),
                model: agent.model.as_ref().map(text),
                effort: agent.effort.as_ref().map(|effort| match effort {
                    klaus_agent_core::schemas::effort::EffortValue::Level(level) => {
                        level.as_str().to_string()
                    }
                    klaus_agent_core::schemas::effort::EffortValue::Number(number) => {
                        number.to_string()
                    }
                    klaus_agent_core::schemas::effort::EffortValue::Custom(value) => text(value),
                }),
                tools: agent
                    .tools
                    .as_ref()
                    .map(|tools| tools.iter().map(text).collect()),
                disallowed_tools: agent
                    .disallowed_tools
                    .as_ref()
                    .map(|tools| tools.iter().map(text).collect()),
            })
            .collect(),
    }
}

/// Fixed-op session methods promoted from the `session/op` escape hatch:
/// JSON-RPC method -> inner op type. Params are `SessionRequestParams`; the
/// result is the reply envelope minus `submissionId` (e.g. `{ history: ... }`).
/// Covers the parameterless getters plus the side-task promotion ops.
fn promoted_session_op_type(method: &str) -> Option<&'static str> {
    Some(match method {
        "session/history" => "get_history",
        "session/listApprovalReviewDenials" => "list_approval_review_denials",
        "session/fileChanges" => "get_file_changes",
        "session/takePending" => "take_pending",
        "session/queueSnapshot" => "get_queue_snapshot",
        "session/memoryMaintenance" => "get_memory_maintenance",
        "session/setupLifecycle" => "get_setup_lifecycle",
        "session/agentMemorySnapshotDecisions" => "get_agent_memory_snapshot_decisions",
        "session/mcpRuntime" => "get_mcp_runtime",
        // Prompts are listed across every connected server at once, so this
        // takes no server name — a per-server variant would be a second answer
        // to the same question.
        "session/listMcpPrompts" => "list_mcp_prompts",
        "session/runtimeCapabilities" => "get_runtime_capabilities",
        "session/extensionRuntime" => "get_extension_runtime",
        "session/pluginRuntime" => "get_plugin_runtime",
        "session/installedPlugins" => "list_installed_plugins",
        "session/cost" => "get_cost",
        "session/contextSnapshot" => "context_snapshot",
        "session/subagents" => "get_subagents",
        "session/subagentLifecycle" => "get_subagent_lifecycle",
        "session/backgroundProcesses" => "get_background_processes",
        "session/queued" => "get_queued",
        "session/collaborationMode" | "session/plans" => "get_planning",
        _ => return None,
    })
}

/// Dispatch one promoted request method (the formal replacements for the
/// `session/op` / `engine/op` escape-hatch residual inventory). Returns false
/// when the method is not a promoted one.
async fn handle_promoted_request(
    state: &Arc<AppServerState>,
    id: &Value,
    method: &str,
    params: RawJson,
) -> bool {
    match method {
        "session/savePlanDocument" | "session/acceptPlan" | "session/planFeedback" => {
            let decoded = match method {
                "session/savePlanDocument" => params
                    .deserialize::<wire::SessionSavePlanDocumentParams>()
                    .map(|p| {
                        (
                            p.session_id.clone(),
                            exact_requests::op(&p, "save_plan_document"),
                        )
                    }),
                "session/acceptPlan" => params
                    .deserialize::<wire::SessionAcceptPlanParams>()
                    .map(|p| (p.session_id.clone(), exact_requests::op(&p, "accept_plan"))),
                _ => params
                    .deserialize::<wire::SessionPlanFeedbackParams>()
                    .map(|p| {
                        (
                            p.session_id.clone(),
                            exact_requests::op(&p, "plan_feedback"),
                        )
                    }),
            };
            let (session_id, op) = match decoded {
                Ok(decoded) => decoded,
                Err(error) => {
                    send_invalid_params(state, id, &error).await;
                    return true;
                }
            };
            let Some(handle) = state.session(&session_id) else {
                send_unknown_session(state, id, &session_id).await;
                return true;
            };
            forward_promoted_request(state.clone(), handle, id.clone(), op, None, None).await;
            true
        }
        "engine/listSessions" => {
            let params = match params.deserialize::<wire::EngineListSessionsParams>() {
                Ok(params) => params,
                Err(error) => {
                    send_invalid_params(state, id, &error).await;
                    return true;
                }
            };
            let op = exact_requests::op(&params, "list_sessions");
            let handle = ensure_control_handle(state);
            forward_promoted_request(
                state.clone(),
                handle,
                id.clone(),
                op,
                params.submission_id,
                None,
            )
            .await;
            true
        }
        "engine/shutdown" => {
            let params = match params.deserialize::<wire::EngineShutdownParams>() {
                Ok(params) => params,
                Err(error) => {
                    send_invalid_params(state, id, &error).await;
                    return true;
                }
            };
            state.connection_closed.store(true, Ordering::Release);
            super::antigravity::login::disconnect(Arc::as_ptr(state) as usize);
            let handle = ensure_control_handle(state);
            forward_promoted_request(
                state.clone(),
                handle,
                id.clone(),
                serde_json::json!({ "type": "shutdown" }),
                params.submission_id,
                None,
            )
            .await;
            true
        }
        "session/predictionContext" => {
            let params = match params.deserialize::<wire::SessionRequestParams>() {
                Ok(params) => params,
                Err(error) => {
                    send_invalid_params(state, id, &error).await;
                    return true;
                }
            };
            let Some(handle) = resolve_scope_handle(state, id, params.session_id.as_deref()).await
            else {
                return true;
            };
            forward_promoted_request(
                state.clone(),
                handle,
                id.clone(),
                serde_json::json!({ "type": "prediction_context" }),
                params.submission_id,
                None,
            )
            .await;
            true
        }
        "session/predictMessage" => {
            let params = match params.deserialize::<wire::SessionPredictMessageParams>() {
                Ok(params) => params,
                Err(error) => {
                    send_invalid_params(state, id, &error).await;
                    return true;
                }
            };
            let Some(handle) = resolve_scope_handle(state, id, Some(&params.session_id)).await
            else {
                return true;
            };
            let mut op = exact_requests::op(&params, "predict_message");
            op.as_object_mut()
                .expect("prediction request object")
                .remove("requestId");
            forward_promoted_request(
                state.clone(),
                handle,
                id.clone(),
                op,
                Some(params.request_id),
                None,
            )
            .await;
            true
        }
        "session/generateTitle" => {
            let params = match params.deserialize::<wire::SessionGenerateTitleParams>() {
                Ok(params) => params,
                Err(error) => {
                    send_invalid_params(state, id, &error).await;
                    return true;
                }
            };
            let Some(handle) = resolve_scope_handle(state, id, params.session_id.as_deref()).await
            else {
                return true;
            };
            let op = exact_requests::op(&params, "generate_title");
            forward_promoted_request(
                state.clone(),
                handle,
                id.clone(),
                op,
                params.submission_id,
                None,
            )
            .await;
            true
        }
        "session/taskIdentity" => {
            let params = match params.deserialize::<wire::SessionTaskIdentityParams>() {
                Ok(params) => params,
                Err(error) => {
                    send_invalid_params(state, id, &error).await;
                    return true;
                }
            };
            let Some(handle) = state.session(&params.session_id) else {
                send_unknown_session(state, id, &params.session_id).await;
                return true;
            };
            let state = state.clone();
            let id = id.clone();
            tokio::spawn(async move {
                let result = match handle.ps.prepare_session().await {
                    Ok(session) => session
                        .task_identity(&params.task_id)
                        .await
                        .map(|identity| wire::SessionTaskIdentityResult {
                            task_identity: identity.map(|identity| wire::TaskIdentity {
                                task_id: identity.task.id,
                                generation: identity.generation,
                                fingerprint: identity.fingerprint,
                                status: identity.task.status.to_string(),
                                subject: identity.task.subject,
                                description: identity.task.description,
                            }),
                        })
                        .map_err(|error| error.to_string()),
                    Err(error) => Err(error),
                };
                match result {
                    Ok(result) => {
                        state
                            .send(exact_payload::response(
                                &id,
                                Payload::Encoded(
                                    RawJson::from_serialize(&result)
                                        .expect("task identity is valid runtime JSON"),
                                ),
                            ))
                            .await;
                    }
                    Err(error) => {
                        state
                            .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                            .await;
                    }
                }
            });
            true
        }
        "session/taskList" => {
            let params = match params.deserialize::<wire::SessionTaskListParams>() {
                Ok(params) => params,
                Err(error) => {
                    send_invalid_params(state, id, &error).await;
                    return true;
                }
            };
            let Some(handle) = state.session(&params.session_id) else {
                send_unknown_session(state, id, &params.session_id).await;
                return true;
            };
            let op = exact_requests::op(&params, "get_task_list");
            forward_promoted_request(
                state.clone(),
                handle,
                id.clone(),
                op,
                params.submission_id,
                None,
            )
            .await;
            true
        }
        "session/createSideTask" => {
            let params = match params.deserialize::<wire::SessionCreateSideTaskParams>() {
                Ok(params) => params,
                Err(error) => {
                    send_invalid_params(state, id, &error).await;
                    return true;
                }
            };
            let Some(handle) = state.session(&params.session_id) else {
                send_unknown_session(state, id, &params.session_id).await;
                return true;
            };
            forward_promoted_request(
                state.clone(),
                handle,
                id.clone(),
                {
                    let mut op = serde_json::json!({
                        "type": "create_side_task",
                        "threadId": params.thread_id,
                    });
                    if let Some(new_thread_id) = params.new_thread_id {
                        op["newThreadId"] = Value::String(new_thread_id);
                    }
                    op
                },
                params.submission_id,
                None,
            )
            .await;
            true
        }
        "session/collaborationMode"
        | "session/plans"
        | "session/readPlanDocument"
        | "session/setCollaborationMode"
        | "session/cancelPlanImplementation"
        | "session/settlePlanImplementation" => {
            let parsed = if method == "session/setCollaborationMode" {
                params
                    .deserialize::<wire::SessionCollaborationModeParams>()
                    .and_then(serde_json::to_value)
            } else if method == "session/cancelPlanImplementation" {
                params
                    .deserialize::<wire::SessionPlanDocumentParams>()
                    .and_then(serde_json::to_value)
            } else if method == "session/settlePlanImplementation" {
                params
                    .deserialize::<wire::SessionSettlePlanImplementationParams>()
                    .and_then(serde_json::to_value)
            } else if method == "session/readPlanDocument" {
                params
                    .deserialize::<wire::SessionReadPlanDocumentParams>()
                    .and_then(serde_json::to_value)
            } else {
                params
                    .deserialize::<wire::SessionPlanningAddressParams>()
                    .and_then(serde_json::to_value)
            };
            let params = match parsed {
                Ok(params) => params,
                Err(error) => {
                    send_invalid_params(state, id, &error).await;
                    return true;
                }
            };
            let session_id = params["sessionId"].as_str();
            let thread_id = params["threadId"].as_str().or(session_id);
            let Some(thread_id) = thread_id.filter(|s| !s.trim().is_empty()) else {
                send_invalid_params_message(state, id, "A sessionId or threadId is required").await;
                return true;
            };
            let live_handle = session_id
                .and_then(|id| state.session(id))
                .or_else(|| state.session(thread_id))
                .or_else(|| {
                    state
                        .sessions
                        .lock()
                        .unwrap()
                        .values()
                        .find(|handle| {
                            handle.ps.existing_session().is_some_and(|session| {
                                session.id.try_to_utf8().ok().as_deref() == Some(thread_id)
                            })
                        })
                        .cloned()
                });
            let live = live_handle
                .as_ref()
                .and_then(|handle| handle.ps.existing_session());
            if let Some(live) = &live {
                if let Some(thread) = params["threadId"].as_str() {
                    if live.id.try_to_utf8().ok().as_deref() != Some(thread) {
                        send_invalid_params_message(
                            state,
                            id,
                            "Session and thread identities differ",
                        )
                        .await;
                        return true;
                    }
                }
            }
            let runtime = live
                .as_ref()
                .map(|s| s.planning_runtime())
                .unwrap_or_else(|| {
                    Arc::new(klaus_agent_core::planning::runtime::PlanningRuntime::new(
                        thread_id.into(),
                        Some(
                            state
                                .config_path
                                .parent()
                                .unwrap_or_else(|| Path::new("."))
                                .to_path_buf(),
                        ),
                        false,
                        false,
                    ))
                });
            let result = (|| -> Result<RuntimeJson, String> {
                let mut result = RuntimeObject::new();
                if method == "session/setCollaborationMode" {
                    runtime
                        .set_mode(
                            klaus_agent_core::planning::CollaborationMode::parse(
                                params["mode"].as_str().expect("typed mode"),
                            )
                            .map_err(|e| e.to_string())?,
                            params["expectedRevision"].as_u64(),
                        )
                        .map_err(|e| klaus_agent_core::planning::runtime::host(&e))?;
                }
                if method == "session/cancelPlanImplementation"
                    || method == "session/settlePlanImplementation"
                {
                    let plan_id: RuntimeString = params["planId"]
                        .as_str()
                        .expect("typed plan identity")
                        .into();
                    let revision = params["revision"].as_u64().expect("typed plan revision");
                    if method == "session/cancelPlanImplementation" {
                        runtime.cancel(&plan_id, revision)
                    } else {
                        runtime.settle(
                            &plan_id,
                            revision,
                            &params["clientRequestId"]
                                .as_str()
                                .expect("typed request identity")
                                .into(),
                            &params["outcome"]
                                .as_str()
                                .expect("typed admission outcome")
                                .into(),
                        )
                    }
                    .map_err(|e| klaus_agent_core::planning::runtime::host(&e))?;
                }
                if method == "session/readPlanDocument" {
                    let plan_id: RuntimeString = params["planId"]
                        .as_str()
                        .expect("typed plan identity")
                        .into();
                    let revision = params["revision"].as_u64().expect("typed plan revision");
                    let document = runtime
                        .read_document(&plan_id, revision)
                        .map_err(|e| klaus_agent_core::planning::runtime::host(&e))?;
                    result.insert("document", document);
                }
                let snapshot = runtime
                    .snapshot()
                    .map_err(|e| klaus_agent_core::planning::runtime::host(&e))?;
                let snapshot =
                    klaus_agent_core::runtime_values::runtime_codec::encode_value(&snapshot)
                        .map_err(|e| e.to_string())?;
                result.insert("planning", snapshot);
                Ok(RuntimeJson::Object(result))
            })();
            match result {
                Ok(result)
                    if matches!(
                        method,
                        "session/setCollaborationMode"
                            | "session/cancelPlanImplementation"
                            | "session/settlePlanImplementation"
                    ) && live.is_some() =>
                {
                    let state = state.clone();
                    let id = id.clone();
                    let live = live.expect("checked live session");
                    tokio::spawn(async move {
                        match live.synchronize_goal_mode().await {
                            Ok(()) => {
                                state.send(exact_payload::response(&id, result)).await;
                                if let Some(handle) = live_handle {
                                    handle.ps.notify_goal_idle();
                                }
                            }
                            Err(error) => {
                                state
                                    .send(wire::error_response(
                                        &id,
                                        wire::ENGINE_ERROR,
                                        &error.try_to_utf8().unwrap_or_default(),
                                    ))
                                    .await
                            }
                        }
                    });
                }
                Ok(result) => {
                    state
                        .send(exact_payload::response(id, result.clone()))
                        .await;
                    if live.is_none()
                        && matches!(
                            method,
                            "session/setCollaborationMode"
                                | "session/readPlanDocument"
                                | "session/cancelPlanImplementation"
                                | "session/settlePlanImplementation"
                        )
                    {
                        state
                            .notify_session_runtime(
                                thread_id,
                                "session/event",
                                exact_payload::object([
                                    ("sessionId", thread_id.into()),
                                    (
                                        "event",
                                        exact_payload::object([
                                            ("submissionId", "".into()),
                                            ("planning", exact_field(&result, "planning").clone()),
                                        ]),
                                    ),
                                ]),
                                None,
                            )
                            .await;
                    }
                }
                Err(error) => {
                    state
                        .send(wire::error_response(id, wire::ENGINE_ERROR, &error))
                        .await
                }
            };
            true
        }
        "session/approveApprovalReviewDenial" => {
            let params =
                match params.deserialize::<wire::SessionApproveApprovalReviewDenialParams>() {
                    Ok(params) => params,
                    Err(error) => {
                        send_invalid_params(state, id, &error).await;
                        return true;
                    }
                };
            let Some(handle) = state.session(&params.session_id) else {
                send_unknown_session(state, id, &params.session_id).await;
                return true;
            };
            forward_promoted_request(
                state.clone(),
                handle,
                id.clone(),
                exact_requests::op(&params, "approve_approval_review_denial"),
                None,
                None,
            )
            .await;
            true
        }
        "session/answerUserInput" => {
            let params = match params.deserialize::<wire::SessionAnswerUserInputParams>() {
                Ok(params) => params,
                Err(error) => {
                    send_invalid_params(state, id, &error).await;
                    return true;
                }
            };
            let Some(handle) = state.session(&params.session_id) else {
                send_unknown_session(state, id, &params.session_id).await;
                return true;
            };
            let mut op = exact_requests::op(&params, "answer_user_input");
            if let Some(answers) = op.get_mut("answers").and_then(RuntimeJson::as_object_mut) {
                let keys: Vec<_> = answers.keys().collect();
                for key in keys {
                    let answer = answers
                        .get(&key)
                        .and_then(|answer| answer.get("answers"))
                        .cloned()
                        .unwrap_or(RuntimeJson::Null);
                    answers.insert(key, answer);
                }
            }
            forward_promoted_request(state.clone(), handle, id.clone(), op, None, None).await;
            true
        }
        "session/readGoalObjective" => {
            handle_read_goal_objective(state, id, params).await;
            true
        }
        "session/goal" | "session/clearGoal" | "session/setGoal" => {
            handle_goal_request(state, id, method, params).await;
            true
        }
        "session/sendSubagentMessage" => {
            let params = match params.deserialize::<wire::SessionSendSubagentMessageParams>() {
                Ok(params) => params,
                Err(error) => {
                    send_invalid_params(state, id, &error).await;
                    return true;
                }
            };
            let Some(handle) = state.session(&params.session_id) else {
                send_unknown_session(state, id, &params.session_id).await;
                return true;
            };
            if params.agent_id.trim().is_empty() || runtime_text_is_blank(&params.message) {
                state
                    .send(wire::error_response(
                        id,
                        wire::INVALID_PARAMS,
                        "agentId and message must not be empty",
                    ))
                    .await;
                return true;
            }
            forward_promoted_request(
                state.clone(),
                handle,
                id.clone(),
                exact_requests::op(&params, "send_subagent_message"),
                params.submission_id,
                None,
            )
            .await;
            true
        }
        "session/subagentTranscript" => {
            let params = match params.deserialize::<wire::SessionSubagentTranscriptParams>() {
                Ok(params) => params,
                Err(error) => {
                    send_invalid_params(state, id, &error).await;
                    return true;
                }
            };
            let Some(handle) = state.session(&params.session_id) else {
                send_unknown_session(state, id, &params.session_id).await;
                return true;
            };
            forward_promoted_request(
                state.clone(),
                handle,
                id.clone(),
                serde_json::json!({
                    "type": "load_subagent_transcript",
                    "threadId": params.thread_id,
                    "agentId": params.agent_id,
                }),
                params.submission_id,
                None,
            )
            .await;
            true
        }
        "session/mcpAuthorizationStatus" => {
            let params = match params.deserialize::<wire::SessionMcpServerParams>() {
                Ok(params) => params,
                Err(error) => {
                    send_invalid_params(state, id, &error).await;
                    return true;
                }
            };
            let Some(handle) = state.session(&params.session_id) else {
                send_unknown_session(state, id, &params.session_id).await;
                return true;
            };
            forward_promoted_request(
                state.clone(),
                handle,
                id.clone(),
                serde_json::json!({
                    "type": "get_mcp_authorization_status",
                    "serverName": params.server_name,
                }),
                params.submission_id,
                None,
            )
            .await;
            true
        }
        "session/agentMemorySnapshotDecision" => {
            let params = match params.deserialize::<wire::SessionRequestIdParams>() {
                Ok(params) => params,
                Err(error) => {
                    send_invalid_params(state, id, &error).await;
                    return true;
                }
            };
            let Some(handle) = state.session(&params.session_id) else {
                send_unknown_session(state, id, &params.session_id).await;
                return true;
            };
            forward_promoted_request(
                state.clone(),
                handle,
                id.clone(),
                serde_json::json!({
                    "type": "get_agent_memory_snapshot_decision",
                    "requestId": params.request_id,
                }),
                params.submission_id,
                None,
            )
            .await;
            true
        }
        "session/shutdown" => {
            let params = match params.deserialize::<wire::SessionRequestParams>() {
                Ok(params) => params,
                Err(error) => {
                    send_invalid_params(state, id, &error).await;
                    return true;
                }
            };
            let Some(handle) = resolve_scope_handle(state, id, params.session_id.as_deref()).await
            else {
                return true;
            };
            // On the terminal reply a bound session is unregistered and its
            // inner server torn down — mirrors session/close, but the reply
            // carries the native `shutdownCompleted` payload. Control-scope
            // shutdown leaves the shared control session alone.
            let cleanup = params.session_id.clone().map(|session_id| {
                let handle = handle.clone();
                Box::new(move |state: &Arc<AppServerState>| {
                    state.sessions.lock().unwrap().remove(&session_id);
                    tokio::spawn(async move {
                        let _ = handle.ps.shutdown_and_wait().await;
                    });
                }) as PromotedReplyAction
            });
            let state = state.clone();
            let id = id.clone();
            tokio::spawn(async move {
                if let Some(session_id) = params.session_id.as_deref() {
                    if let Err(error) = realtime::stop_session(&state, session_id).await {
                        return state
                            .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                            .await;
                    }
                }
                forward_promoted_request(
                    state,
                    handle,
                    id,
                    serde_json::json!({ "type": "shutdown" }),
                    params.submission_id,
                    cleanup,
                )
                .await;
            });
            true
        }
        // MCP request/response calls. These answer once with a correlated
        // envelope, so they are requests rather than acked submissions: a caller
        // invoking a tool needs the tool's result, and an ack would hand back a
        // submission id and leave the result in a stream nobody is reading.
        "session/callMcpTool" => {
            let params = match params.deserialize::<wire::SessionMcpToolCallParams>() {
                Ok(params) => params,
                Err(error) => {
                    send_invalid_params(state, id, &error).await;
                    return true;
                }
            };
            let Some(handle) = state.session(&params.session_id) else {
                send_unknown_session(state, id, &params.session_id).await;
                return true;
            };
            let op = exact_requests::op(&params, "call_mcp_tool");
            forward_promoted_request(
                state.clone(),
                handle,
                id.clone(),
                op,
                params.submission_id,
                None,
            )
            .await;
            true
        }
        "session/readMcpResource" => {
            let params = match params.deserialize::<wire::SessionMcpResourceParams>() {
                Ok(params) => params,
                Err(error) => {
                    send_invalid_params(state, id, &error).await;
                    return true;
                }
            };
            let Some(handle) = state.session(&params.session_id) else {
                send_unknown_session(state, id, &params.session_id).await;
                return true;
            };
            forward_promoted_request(
                state.clone(),
                handle,
                id.clone(),
                serde_json::json!({
                    "type": "read_mcp_resource",
                    "serverName": params.server_name,
                    "uri": params.uri,
                }),
                params.submission_id,
                None,
            )
            .await;
            true
        }
        "session/getMcpPrompt" => {
            let params = match params.deserialize::<wire::SessionMcpPromptParams>() {
                Ok(params) => params,
                Err(error) => {
                    send_invalid_params(state, id, &error).await;
                    return true;
                }
            };
            let Some(handle) = state.session(&params.session_id) else {
                send_unknown_session(state, id, &params.session_id).await;
                return true;
            };
            let op = exact_requests::op(&params, "get_mcp_prompt");
            forward_promoted_request(
                state.clone(),
                handle,
                id.clone(),
                op,
                params.submission_id,
                None,
            )
            .await;
            true
        }
        // Queue mutations. Each answers with the authoritative post-operation
        // snapshot: that snapshot IS the point of the call, because the caller
        // supplied `expectedRevision` and needs to know whether the guard held.
        "session/editQueued" => {
            let params = match params.deserialize::<wire::SessionQueueEditParams>() {
                Ok(params) => params,
                Err(error) => {
                    send_invalid_params(state, id, &error).await;
                    return true;
                }
            };
            let Some(handle) = state.session(&params.session_id) else {
                send_unknown_session(state, id, &params.session_id).await;
                return true;
            };
            forward_promoted_request(
                state.clone(),
                handle,
                id.clone(),
                exact_requests::op(&params, "edit_queued"),
                params.submission_id,
                None,
            )
            .await;
            true
        }
        "session/rewindPreview" => {
            let params = match params.deserialize::<wire::SessionRewindPreviewParams>() {
                Ok(params) => params,
                Err(error) => {
                    send_invalid_params(state, id, &error).await;
                    return true;
                }
            };
            let Some(handle) = state.session(&params.session_id) else {
                send_unknown_session(state, id, &params.session_id).await;
                return true;
            };
            forward_promoted_request(
                state.clone(),
                handle,
                id.clone(),
                serde_json::json!({ "type": "rewind_preview", "numTurns": params.num_turns }),
                params.submission_id,
                None,
            )
            .await;
            true
        }
        "session/removeQueued" => {
            let params = match params.deserialize::<wire::SessionQueueRemoveParams>() {
                Ok(params) => params,
                Err(error) => {
                    send_invalid_params(state, id, &error).await;
                    return true;
                }
            };
            let Some(handle) = state.session(&params.session_id) else {
                send_unknown_session(state, id, &params.session_id).await;
                return true;
            };
            forward_promoted_request(
                state.clone(),
                handle,
                id.clone(),
                serde_json::json!({
                    "type": "remove_queued",
                    "mutationId": params.mutation_id,
                    "uuid": params.uuid,
                    "expectedRevision": params.expected_revision,
                }),
                params.submission_id,
                None,
            )
            .await;
            true
        }
        "session/reorderQueued" => {
            let params = match params.deserialize::<wire::SessionQueueReorderParams>() {
                Ok(params) => params,
                Err(error) => {
                    send_invalid_params(state, id, &error).await;
                    return true;
                }
            };
            let Some(handle) = state.session(&params.session_id) else {
                send_unknown_session(state, id, &params.session_id).await;
                return true;
            };
            let mut op = serde_json::json!({
                "type": "reorder_queued",
                "mutationId": params.mutation_id,
                "uuid": params.uuid,
                "expectedRevision": params.expected_revision,
            });
            if let Some(before_uuid) = params.before_uuid {
                op["beforeUuid"] = Value::String(before_uuid);
            }
            forward_promoted_request(
                state.clone(),
                handle,
                id.clone(),
                op,
                params.submission_id,
                None,
            )
            .await;
            true
        }
        "session/backgroundProcessOutput" => {
            let params = match params.deserialize::<wire::SessionBackgroundProcessOutputParams>() {
                Ok(params) => params,
                Err(error) => {
                    send_invalid_params(state, id, &error).await;
                    return true;
                }
            };
            let Some(handle) = state.session(&params.session_id) else {
                send_unknown_session(state, id, &params.session_id).await;
                return true;
            };
            let mut op = serde_json::json!({
                "type": "read_background_process_output",
                "shellId": params.shell_id,
            });
            if let Some(value) = params.after_offset {
                op["afterOffset"] = serde_json::json!(value);
            }
            if let Some(value) = params.max_bytes {
                op["maxBytes"] = serde_json::json!(value);
            }
            forward_promoted_request(
                state.clone(),
                handle,
                id.clone(),
                op,
                params.submission_id,
                None,
            )
            .await;
            true
        }
        "session/stopBackgroundProcess" => {
            let params = match params.deserialize::<wire::SessionBackgroundProcessStopParams>() {
                Ok(params) => params,
                Err(error) => {
                    send_invalid_params(state, id, &error).await;
                    return true;
                }
            };
            if params
                .signal
                .as_deref()
                .is_some_and(|signal| signal != "SIGTERM" && signal != "SIGKILL")
            {
                send_invalid_params_message(state, id, "signal must be SIGTERM or SIGKILL").await;
                return true;
            }
            let Some(handle) = state.session(&params.session_id) else {
                send_unknown_session(state, id, &params.session_id).await;
                return true;
            };
            let mut op = serde_json::json!({
                "type": "stop_background_process",
                "shellId": params.shell_id,
            });
            if let Some(value) = params.signal {
                op["signal"] = Value::String(value);
            }
            forward_promoted_request(
                state.clone(),
                handle,
                id.clone(),
                op,
                params.submission_id,
                None,
            )
            .await;
            true
        }
        _ => {
            let Some(op_type) = promoted_session_op_type(method) else {
                return false;
            };
            let params = match params.deserialize::<wire::SessionRequestParams>() {
                Ok(params) => params,
                Err(error) => {
                    send_invalid_params(state, id, &error).await;
                    return true;
                }
            };
            let Some(handle) = resolve_scope_handle(state, id, params.session_id.as_deref()).await
            else {
                return true;
            };
            forward_promoted_request(
                state.clone(),
                handle,
                id.clone(),
                serde_json::json!({ "type": op_type }),
                params.submission_id,
                None,
            )
            .await;
            true
        }
    }
}

/// Dispatch one promoted ack-reply method (write/long-running ops whose
/// replies stream on `session/rawEvent` instead of a single terminal
/// envelope). Returns false when the method is not an ack-promoted one.
async fn handle_acked_request(
    state: &Arc<AppServerState>,
    id: &Value,
    method: &str,
    params: RawJson,
) -> bool {
    macro_rules! parse {
        ($ty:ty) => {
            match params.deserialize::<$ty>() {
                Ok(params) => params,
                Err(error) => {
                    send_invalid_params(state, id, &error).await;
                    return true;
                }
            }
        };
    }
    match method {
        "session/approve"
        | "session/resolveElicitation"
        | "session/resolveMcpAuthorization"
        | "session/resolveMcpElicitation" => {
            let op_type = match method {
                "session/approve" => "approve",
                "session/resolveElicitation" => "resolve_elicitation",
                "session/resolveMcpAuthorization" => "resolve_mcp_authorization",
                _ => "resolve_mcp_elicitation",
            };
            let p = parse!(wire::SessionRequestResolutionParams);
            let op = exact_requests::op(&p, op_type);
            feed_acked_op(state, id, Some(&p.session_id), p.submission_id, op).await;
        }
        "session/cancelPrediction" => {
            let p = parse!(wire::SessionCancelPredictionParams);
            feed_acked_op(
                state,
                id,
                Some(&p.session_id),
                None,
                serde_json::json!({ "type": "cancel_prediction", "requestId": p.request_id }),
            )
            .await;
        }
        "session/cancelRequest" => {
            let p = parse!(wire::SessionRequestIdParams);
            feed_acked_op(
                state,
                id,
                Some(&p.session_id),
                p.submission_id,
                serde_json::json!({ "type": "cancel_request", "requestId": p.request_id }),
            )
            .await;
        }
        "session/revokeMcpAuthorization"
        | "session/reconnectMcpServer"
        | "session/removeMcpServer" => {
            let op_type = match method {
                "session/revokeMcpAuthorization" => "revoke_mcp_authorization",
                "session/reconnectMcpServer" => "reconnect_mcp_server",
                _ => "remove_mcp_server",
            };
            let p = parse!(wire::SessionMcpServerParams);
            feed_acked_op(
                state,
                id,
                Some(&p.session_id),
                p.submission_id,
                serde_json::json!({ "type": op_type, "serverName": p.server_name }),
            )
            .await;
        }
        "session/upsertMcpServer" => {
            let p = parse!(wire::SessionMcpServerUpsertParams);
            let op = exact_requests::op(&p, "upsert_mcp_server");
            feed_acked_op(state, id, Some(&p.session_id), p.submission_id, op).await;
        }
        "session/reloadMcpServers" => {
            let p = parse!(wire::SessionMcpServersReloadParams);
            let op = exact_requests::op(&p, "reload_mcp_servers");
            feed_acked_op(state, id, Some(&p.session_id), p.submission_id, op).await;
        }
        "session/setInstalledPluginEnabled" => {
            let p = parse!(wire::SessionPluginEnabledParams);
            feed_acked_op(
                state,
                id,
                p.session_id.as_deref(),
                p.submission_id,
                serde_json::json!({
                    "type": "set_installed_plugin_enabled",
                    "name": p.name,
                    "enabled": p.enabled,
                }),
            )
            .await;
        }
        "session/uninstallPlugin" => {
            let p = parse!(wire::SessionPluginUninstallParams);
            let mut op = serde_json::json!({ "type": "uninstall_plugin", "name": p.name });
            if let Some(remove_files) = p.remove_files {
                op["removeFiles"] = Value::Bool(remove_files);
            }
            feed_acked_op(state, id, p.session_id.as_deref(), p.submission_id, op).await;
        }
        "session/installPlugin" => {
            let p = parse!(wire::SessionPluginInstallParams);
            let mut op = serde_json::json!({
                "type": "install_plugin",
                "marketplace": p.marketplace,
                "sourceDir": p.source_dir,
            });
            if let Some(version) = p.version {
                op["version"] = Value::String(version);
            }
            if let Some(expected_installed_version) = p.expected_installed_version {
                op["expectedInstalledVersion"] = Value::String(expected_installed_version);
            }
            feed_acked_op(state, id, p.session_id.as_deref(), p.submission_id, op).await;
        }
        "session/pluginCatalog" => {
            let p = parse!(wire::SessionPluginCatalogParams);
            let roots: Vec<Value> = p
                .roots
                .into_iter()
                .map(|root| serde_json::json!({ "marketplace": root.marketplace, "dir": root.dir }))
                .collect();
            feed_acked_op(
                state,
                id,
                p.session_id.as_deref(),
                p.submission_id,
                serde_json::json!({ "type": "list_plugin_catalog", "roots": roots }),
            )
            .await;
        }
        "session/resolveDynamicToolCall" => {
            let p = parse!(wire::SessionDynamicToolCallParams);
            let op = exact_requests::op(&p, "resolve_dynamic_tool_call");
            feed_acked_op(state, id, Some(&p.session_id), p.submission_id, op).await;
        }
        "session/rewind" => {
            let p = parse!(wire::SessionRewindParams);
            let op = exact_requests::op(&p, "rewind");
            feed_acked_op(state, id, Some(&p.session_id), p.submission_id, op).await;
        }
        "session/steer" => {
            let p = parse!(wire::SessionSteerParams);
            let op = exact_requests::op(&p, "steer");
            feed_acked_op(state, id, Some(&p.session_id), p.submission_id, op).await;
        }
        "session/appendDisplayMessage" => {
            let p = parse!(wire::SessionAppendDisplayMessageParams);
            let op = serde_json::json!({
                "type": "append_display_message",
                "messageId": p.message_id,
                "input": p.text,
                "displayMetadata": p.metadata,
            });
            feed_acked_op(state, id, Some(&p.session_id), p.submission_id, op).await;
        }
        "session/setSkills" => {
            let p = parse!(wire::SessionSetSkillsParams);
            let op = exact_requests::op(&p, "set_skills");
            feed_acked_op(state, id, Some(&p.session_id), p.submission_id, op).await;
        }
        "session/setModel" => {
            let p = parse!(wire::SessionSetModelParams);
            feed_acked_op(
                state,
                id,
                Some(&p.session_id),
                p.submission_id,
                serde_json::json!({ "type": "set_model", "model": p.model }),
            )
            .await;
        }
        "session/setPermissionMode" => {
            let p = parse!(wire::SessionPermissionModeParams);
            feed_acked_op(
                state,
                id,
                Some(&p.session_id),
                p.submission_id,
                serde_json::json!({ "type": "set_permission_mode", "mode": p.mode }),
            )
            .await;
        }
        "session/setRuntimePolicy" => {
            let p = parse!(wire::SessionSetRuntimePolicyParams);
            let mut op = serde_json::json!({ "type": "set_runtime_policy" });
            if let Some(mode) = p.permission_mode {
                op["permissionMode"] = Value::String(mode);
            }
            if let Some(value) = p.sandbox_workspace_write {
                op["sandboxWorkspaceWrite"] =
                    serde_json::to_value(value).expect("typed workspace sandbox encodes");
            }
            if let Some(value) = p.network_allowed {
                op["networkAllowed"] = Value::Bool(value);
            }
            if let Some(mode) = p.pre_plan_permission_mode {
                op["prePlanPermissionMode"] = Value::String(mode);
            }
            if let Some(policy) = p.approval_policy {
                op["approvalPolicy"] =
                    serde_json::to_value(policy).expect("typed approval policy encodes");
            }
            if let Some(mode) = p.sandbox_mode {
                op["sandboxMode"] = Value::String(mode);
            }
            if let Some(reviewer) = p.approvals_reviewer {
                op["approvalsReviewer"] = Value::String(reviewer);
            }
            feed_acked_op(state, id, Some(&p.session_id), p.submission_id, op).await;
        }
        "session/setRuntimeModel" => {
            let p = parse!(wire::SessionSetRuntimeModelParams);
            feed_acked_op(
                state,
                id,
                Some(&p.session_id),
                p.submission_id,
                serde_json::json!({ "type": "set_runtime_model", "model": p.model }),
            )
            .await;
        }
        "session/updateConfig" => {
            let p = parse!(wire::SessionConfigUpdateParams);
            let controls = match p
                .update
                .deserialize::<std::collections::BTreeMap<String, RawJson>>()
            {
                Ok(controls) => controls,
                Err(error) => {
                    send_invalid_params(state, id, &error).await;
                    return true;
                }
            };
            if controls
                .get("goal")
                .is_some_and(|goal| goal.deserialize::<bool>().is_err())
            {
                send_invalid_params_message(state, id, "update.goal must be a boolean").await;
                return true;
            }
            if controls.get("maximumGoalTokenBudget").is_some_and(|value| {
                value.deserialize::<Option<i64>>().map_or(true, |budget| {
                    budget.is_some_and(|budget| !(1..=9_007_199_254_740_991).contains(&budget))
                })
            }) {
                send_invalid_params_message(
                    state,
                    id,
                    "update.maximumGoalTokenBudget must be a positive safe integer or null",
                )
                .await;
                return true;
            }
            let op = exact_requests::op(&p, "update_session_config");
            feed_acked_op(state, id, Some(&p.session_id), p.submission_id, op).await;
        }
        "session/compactStart" => {
            let p = parse!(wire::SessionCompactParams);
            let op = exact_requests::op(&p, "compact_session");
            feed_acked_op(state, id, Some(&p.session_id), p.submission_id, op).await;
        }
        // Self-review acks like every other streaming turn. It spends model
        // calls and can stop to ask for approval, so a caller has to be reading
        // the stream while it runs; answering it with the verdict instead would
        // hand back one value only after the point where it could still be
        // driven, and the verdict would never appear in the stream at all.
        "session/reviewStart" => {
            let p = parse!(wire::SessionReviewParams);
            let op = exact_requests::op(&p, "review_session");
            feed_acked_op(state, id, Some(&p.session_id), p.submission_id, op).await;
        }
        "session/sideQuestion" => {
            let p = parse!(wire::SessionSideQuestionParams);
            let op = exact_requests::op(&p, "side_question");
            feed_acked_op(state, id, Some(&p.session_id), p.submission_id, op).await;
        }
        "session/dreamNow" => {
            let p = parse!(wire::SessionRequestParams);
            feed_acked_op(
                state,
                id,
                p.session_id.as_deref(),
                p.submission_id,
                serde_json::json!({ "type": "dream_now" }),
            )
            .await;
        }
        "session/runSetup" => {
            let p = parse!(wire::SessionRunSetupParams);
            let op = exact_requests::op(&p, "run_setup");
            feed_acked_op(state, id, Some(&p.session_id), p.submission_id, op).await;
        }
        "session/resolveAgentMemorySnapshotDecision" => {
            let p = parse!(wire::SessionMemorySnapshotDecisionParams);
            let op = exact_requests::op(&p, "resolve_agent_memory_snapshot_decision");
            feed_acked_op(state, id, Some(&p.session_id), p.submission_id, op).await;
        }
        "session/settleAgentMemorySnapshotUnavailable" => {
            let p = parse!(wire::SessionMemorySnapshotUnavailableParams);
            let mut op = serde_json::json!({
                "type": "settle_agent_memory_snapshot_unavailable",
                "requestId": p.request_id,
                "reason": p.reason,
            });
            if let Some(error) = p.error {
                op["error"] = Value::String(error);
            }
            feed_acked_op(state, id, Some(&p.session_id), p.submission_id, op).await;
        }
        "session/resumeSession"
        | "session/forkSession"
        | "session/archiveSession"
        | "session/unarchiveSession"
        | "session/deleteSession" => {
            let op_type = match method {
                "session/resumeSession" => "resume_session",
                "session/forkSession" => "fork_session",
                "session/archiveSession" => "archive_session",
                "session/unarchiveSession" => "unarchive_session",
                _ => "delete_session",
            };
            let p = parse!(wire::SessionThreadParams);
            let mut op = serde_json::json!({ "type": op_type, "threadId": p.thread_id });
            if op_type == "fork_session" {
                if let Some(inherit_goal) = p.inherit_goal {
                    op["inheritGoal"] = Value::Bool(inherit_goal);
                }
                if let Some(new_thread_id) = p.new_thread_id {
                    op["newThreadId"] = Value::String(new_thread_id);
                }
                if let Some(before_message_uuid) = p.before_message_uuid {
                    op["beforeMessageUuid"] = Value::String(before_message_uuid);
                }
            }
            if op_type == "fork_session" && p.inherit_goal == Some(true) {
                let source = state.sessions.lock().unwrap().values().find_map(|handle| {
                    handle
                        .ps
                        .existing_session()
                        .filter(|session| session.id == p.thread_id.as_str())
                });
                if let Some(source) = source {
                    let state = state.clone();
                    let id = id.clone();
                    tokio::spawn(async move {
                        match source.flush_goal_progress().await {
                            Ok(()) => {
                                feed_acked_op(
                                    &state,
                                    &id,
                                    p.session_id.as_deref(),
                                    p.submission_id,
                                    op,
                                )
                                .await
                            }
                            Err(error) => {
                                state
                                    .send(wire::error_response(
                                        &id,
                                        wire::ENGINE_ERROR,
                                        &error.try_to_utf8().unwrap_or_default(),
                                    ))
                                    .await
                            }
                        }
                    });
                    return true;
                }
            }
            feed_acked_op(state, id, p.session_id.as_deref(), p.submission_id, op).await;
        }
        "session/renameThread" => {
            let p = parse!(wire::SessionRenameThreadParams);
            feed_acked_op(
                state,
                id,
                p.session_id.as_deref(),
                p.submission_id,
                serde_json::json!({
                    "type": "rename_thread",
                    "threadId": p.thread_id,
                    "newThreadId": p.new_thread_id,
                }),
            )
            .await;
        }
        "session/updateThreadHostMetadata" => {
            let p = parse!(wire::SessionUpdateThreadHostMetadataParams);
            let op = exact_requests::op(&p, "update_thread_host_metadata");
            feed_acked_op(state, id, p.session_id.as_deref(), p.submission_id, op).await;
        }
        "session/updateTitle" => {
            let p = parse!(wire::SessionUpdateTitleParams);
            let op = exact_requests::op(&p, "update_title");
            feed_acked_op(state, id, p.session_id.as_deref(), p.submission_id, op).await;
        }
        _ => return false,
    }
    true
}

async fn send_invalid_params(state: &AppServerState, id: &Value, error: &serde_json::Error) {
    send_invalid_params_message(state, id, &format!("invalid params: {error}")).await;
}

async fn send_invalid_params_message(state: &AppServerState, id: &Value, message: &str) {
    state
        .send(wire::error_response(id, wire::INVALID_PARAMS, message))
        .await;
}

/// RAII slot for one heavy in-flight request. `acquire` rejects with `-32001`
/// when the ceiling is reached (the caller already got its error response).
struct HeavyRequestGuard {
    state: Arc<AppServerState>,
}

struct ImportIdentityGuard {
    state: Arc<AppServerState>,
    thread_id: String,
    already_bound: bool,
}

impl ImportIdentityGuard {
    fn acquire(state: &Arc<AppServerState>, thread_id: String) -> Self {
        let mut imports = state.importing_threads.lock().unwrap();
        let already_bound = state.sessions.lock().unwrap().contains_key(&thread_id);
        imports.insert(thread_id.clone());
        Self {
            state: state.clone(),
            thread_id,
            already_bound,
        }
    }
}

impl Drop for ImportIdentityGuard {
    fn drop(&mut self) {
        self.state
            .importing_threads
            .lock()
            .unwrap()
            .remove(&self.thread_id);
    }
}

impl HeavyRequestGuard {
    async fn acquire(state: &Arc<AppServerState>, id: &Value) -> Option<Self> {
        let previous = state.inflight_heavy.fetch_add(1, Ordering::AcqRel);
        if previous >= MAX_INFLIGHT_HEAVY_REQUESTS {
            state.inflight_heavy.fetch_sub(1, Ordering::AcqRel);
            state
                .send(wire::error_response(
                    id,
                    wire::OVERLOADED,
                    "Server overloaded; retry later.",
                ))
                .await;
            return None;
        }
        Some(Self {
            state: state.clone(),
        })
    }
}

impl Drop for HeavyRequestGuard {
    fn drop(&mut self) {
        self.state.inflight_heavy.fetch_sub(1, Ordering::AcqRel);
    }
}

async fn send_unknown_session(state: &AppServerState, id: &Value, session_id: &str) {
    state
        .send(wire::error_response(
            id,
            wire::UNKNOWN_SESSION,
            &format!("unknown session: {session_id}"),
        ))
        .await;
}

/// The thread a `session/start` is about, when the request names one: the id the
/// host put in the init, or the thread it asked to resume. `None` means nothing
/// named a thread and the caller mints the identity.
///
/// A resume WINS over `init.threadId`, because the store's own id wins inside
/// the engine (`resume_session` overwrites `opts.id` with the stored meta id) —
/// taking the init's value would hand back a handle the thread does not answer
/// to. The two disagreeing at all is a contradiction in the request, so it is
/// refused instead of silently resolved one way.
fn session_thread_id(
    init: &exact_requests::InitObject,
    resume: Option<&str>,
) -> Result<Option<String>, String> {
    let named = |value: Option<&str>| {
        value
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .map(str::to_string)
    };
    let in_init_value = init.control_string("threadId")?;
    let in_init = named(in_init_value.as_deref());
    let resumed = named(resume);
    if let (Some(resumed), Some(in_init)) = (&resumed, &in_init) {
        if resumed != in_init {
            return Err(format!(
                "resume {resumed} disagrees with init.threadId {in_init}: one session is one thread"
            ));
        }
    }
    Ok(resumed.or(in_init))
}

/// `session/start`: spin up an inner `ProtocolServer` on a duplex pipe, drive
/// `init_session` through it and reply once the session is bound.
///
/// The handle the session is addressed by IS its thread id: the id on the wire,
/// the id in the store and the id the host filed the conversation under are one
/// value. A per-connection counter (`sess_N`) used to sit here, which forced
/// every host to keep a handle→thread map and translate at every callback — and
/// a callback that forgot ran under an id no thread owned (measured in the
/// desktop client: a generated image saved into `~/.klaus/sessions/sess_2/`, a
/// directory belonging to no conversation, which the media channel then refused
/// to serve).
async fn session_start(state: Arc<AppServerState>, id: Value, params: wire::SessionStartParams) {
    session_start_impl(state, id, params, None, None).await;
}

async fn session_start_side_task(
    state: Arc<AppServerState>,
    id: Value,
    params: wire::SessionStartSideTaskParams,
) {
    let init_thread_id = match params.init.control_string("threadId") {
        Ok(thread_id) => thread_id,
        Err(error) => return send_invalid_params_message(&state, &id, &error).await,
    };
    if params.parent_thread_id.is_empty()
        || params.parent_thread_id != params.parent_thread_id.trim()
        || params
            .source_session_id
            .as_ref()
            .is_some_and(|source| source.is_empty() || source != source.trim())
        || init_thread_id.as_deref().is_some_and(|thread| {
            thread.is_empty() || thread != thread.trim() || thread == params.parent_thread_id
        })
    {
        return send_invalid_params_message(
            &state,
            &id,
            "side-task ids must be nonempty without surrounding whitespace, and the child must differ from its parent",
        ).await;
    }
    // A parent mid-unload cannot own a new side task; the client retries and
    // either re-attaches or rebuilds once teardown finishes.
    let pending_parent = params
        .source_session_id
        .as_deref()
        .unwrap_or(params.parent_thread_id.as_str());
    if state
        .pending_session_unloads
        .lock()
        .unwrap()
        .contains(pending_parent)
    {
        return state
            .send(wire::error_response(
                &id,
                wire::INVALID_REQUEST,
                &format!("session {pending_parent} is closing; retry after the session is closed"),
            ))
            .await;
    }
    let source = match params.source_session_id.as_ref() {
        Some(source_id) => {
            let Some(source) = state.session(source_id) else {
                return send_unknown_session(&state, &id, source_id).await;
            };
            Some(source)
        }
        None => state.session(&params.parent_thread_id),
    };
    let live_snapshot = match source {
        Some(source) => {
            if source.ps.is_closing() {
                state
                    .send(wire::error_response(
                        &id,
                        wire::ENGINE_ERROR,
                        "cannot create a side task from a closing session",
                    ))
                    .await;
                return;
            }
            match source.ps.existing_session() {
                Some(session) => {
                    if session.id != params.parent_thread_id.as_str() {
                        return send_invalid_params_message(
                            &state,
                            &id,
                            "sourceSessionId does not own parentThreadId",
                        )
                        .await;
                    }
                    match session.side_task_snapshot() {
                        Ok(snapshot) => Some(Arc::new(snapshot)),
                        Err(error) => {
                            state
                                .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                                .await;
                            return;
                        }
                    }
                }
                None => None,
            }
        }
        None => None,
    };
    session_start_impl(
        state,
        id,
        wire::SessionStartParams {
            init: params.init,
            resume: None,
            fork: None,
        },
        None,
        Some(SideTaskStart {
            parent_thread_id: params.parent_thread_id,
            live_snapshot,
        }),
    )
    .await;
}

async fn session_fork_live(
    state: Arc<AppServerState>,
    id: Value,
    params: wire::SessionForkLiveParams,
) {
    let init_thread_id = match params.init.control_string("threadId") {
        Ok(thread_id) => thread_id,
        Err(error) => return send_invalid_params_message(&state, &id, &error).await,
    };
    if init_thread_id
        .as_deref()
        .is_some_and(|thread_id| thread_id.is_empty() || thread_id != thread_id.trim())
    {
        return send_invalid_params_message(
            &state,
            &id,
            "init.threadId must be nonempty and must not contain surrounding whitespace",
        )
        .await;
    }
    let Some(source) = state.session(&params.source_session_id) else {
        return send_unknown_session(&state, &id, &params.source_session_id).await;
    };
    let Some(session) = source.ps.existing_session() else {
        state
            .send(wire::error_response(
                &id,
                wire::ENGINE_ERROR,
                "live fork requires an initialized, open source session",
            ))
            .await;
        return;
    };
    let captured = if params.inherit_goal == Some(true) {
        session
            .persistent_goal_fork_snapshot(params.before_message_uuid.as_deref())
            .await
    } else {
        session.persistent_fork_snapshot(params.before_message_uuid.as_deref())
    };
    let snapshot = match captured {
        Ok(snapshot) => Arc::new(snapshot),
        Err(error) => {
            state
                .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                .await;
            return;
        }
    };
    session_start_impl(
        state,
        id,
        wire::SessionStartParams {
            init: params.init,
            resume: None,
            fork: None,
        },
        Some(snapshot),
        None,
    )
    .await;
}

/// The inner protocol's init_session ACK must settle within this budget. The
/// host's own session/start deadline is 60s; failing here first turns a stalled
/// startup into a named engine error instead of a silent host-side timeout.
const STARTUP_INIT_ACK_BUDGET_SECS: u64 = 50;

async fn session_start_impl(
    state: Arc<AppServerState>,
    id: Value,
    params: wire::SessionStartParams,
    fork_live: Option<Arc<LiveForkSnapshot>>,
    side_task: Option<SideTaskStart>,
) {
    let materialize_fork = fork_live.is_some();
    let materialize_side_task = side_task.is_some();
    let mut init = match exact_requests::InitObject::new(&params.init) {
        Ok(init) => init,
        Err(error) => return send_invalid_params_message(&state, &id, &error).await,
    };
    let has_selected_agent = init
        .get("agentDefinitionName")
        .is_some_and(|name| name.as_str().trim() != "null");
    let selected_agent_name = init.get("agentDefinitionName").cloned();
    let session_id = match session_thread_id(&init, params.resume.as_deref()) {
        Ok(Some(named)) => named,
        // Nothing named a thread (a conversation the host did not name, a fork,
        // an endpoint-level control session): mint the id HERE and hand it to
        // the inner session through the init, so it adopts this one instead of
        // minting a second identity the caller can never see.
        Ok(None) => {
            let minted = uuid::Uuid::new_v4().to_string();
            init.set(
                "threadId",
                RawJson::from_serialize(&minted).expect("thread id encodes"),
            );
            minted
        }
        Err(message) => {
            state
                .send(wire::error_response(&id, wire::INVALID_PARAMS, &message))
                .await;
            return;
        }
    };

    if state
        .pending_session_unloads
        .lock()
        .unwrap()
        .contains(&session_id)
    {
        // The named session is mid-unload; the client retries and resumes from
        // stored history once teardown finishes.
        return state
            .send(wire::error_response(
                &id,
                wire::INVALID_REQUEST,
                &format!("session {session_id} is closing; retry after the session is closed"),
            ))
            .await;
    }

    // Startup diagnostics: a session/start that stalls used to surface only as
    // the host's silent 60s deadline with nothing in the engine log. One stderr
    // line per stage lets a stall name itself, and the init ACK wait below
    // carries an explicit budget that fails with a named error.
    let start_stage = std::time::Instant::now();
    eprintln!("[session-start] {session_id} stage=start-begin elapsedMs=0");

    // Wrap the host factory with this session's resume/fork intent — the same
    // adaptation the WS server applies per connection.
    let create: CreateSessionFn = {
        let cs = state.create_session.clone();
        let resume = params.resume.clone();
        let fork = params.fork.clone();
        let fork_live = fork_live.clone();
        let side_task = side_task.clone();
        let credential_store = state.credential_store.clone();
        Arc::new(move |h: SessionHandlers| {
            let cs = cs.clone();
            let resume = resume.clone();
            let fork = fork.clone();
            let fork_live = fork_live.clone();
            let side_task = side_task.clone();
            let credential_store = credential_store.clone();
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
                    fork_live,
                    side_task,
                })
                .await
            }) as CreateSessionFuture
        })
    };

    let (host_side, engine_side) = tokio::io::duplex(SESSION_PIPE_CAPACITY);
    let (engine_read, engine_write) = tokio::io::split(engine_side);
    let (host_read, mut host_write) = tokio::io::split(host_side);

    let ps = Arc::new(ProtocolServer::new(ProtocolServerOptions {
        create_session: create,
        lifecycle: state.lifecycle.clone(),
        plugin_admin: state.plugin_admin.clone(),
        credential_store: state.credential_store.clone(),
        input: Box::new(engine_read),
        output: Box::new(engine_write),
    }));
    ps.install_provider_decorator(model_sources::provider_decorator(
        &model_sources::config_root_of(&state.config_path),
    ));
    let source_state = Arc::downgrade(&state);
    ps.install_subagent_target_resolver(Arc::new(move |source, model_id| {
        let source_state = source_state.clone();
        Box::pin(async move {
            let state = source_state
                .upgrade()
                .ok_or("The model endpoint is closed.")?;
            let target =
                resolve_tier_target(&state, wire::ModelTierTarget::Source { source, model_id })
                    .await?;
            let decorate = model_sources::provider_decorator(&model_sources::config_root_of(
                &state.config_path,
            ));
            klaus_agent_protocol::server::resolved_model_target(
                &target,
                state.credential_store.clone(),
                &decorate,
            )
            .await
        })
    }));
    tokio::spawn({
        let ps = ps.clone();
        async move { ps.start().await }
    });

    // Feed raw op lines into the inner server's stdin.
    let (op_tx, mut op_rx) = mpsc::unbounded_channel::<Payload>();
    tokio::spawn(async move {
        while let Some(op) = op_rx.recv().await {
            let Ok(mut line) = op.bytes() else {
                continue;
            };
            line.push(b'\n');
            if host_write.write_all(&line).await.is_err() {
                break;
            }
            let _ = host_write.flush().await;
        }
    });

    let read_seed = if params.resume.is_none()
        && params.fork.is_none()
        && !materialize_fork
        && !materialize_side_task
    {
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(i64::MAX as u128) as i64;
        Some(klaus_agent_core::store::session_store::ThreadReadSnapshot {
            meta: klaus_agent_core::schemas::rollout::SessionMeta {
                id: session_id.clone().into(),
                created_at,
                cwd: init
                    .control_string("cwd")
                    .ok()
                    .flatten()
                    .unwrap_or_default()
                    .into(),
                model: init
                    .control_string("model")
                    .ok()
                    .flatten()
                    .unwrap_or_default()
                    .into(),
                title: None,
                originator: None,
                cli_version: None,
                source: None,
                role: None,
                parent_thread_id: None,
                forked_from_id: None,
                git: None,
                host_metadata: None,
            },
            raw_items: Vec::new(),
            updated_at: created_at,
            archived: false,
        })
    } else {
        None
    };
    let handle = Arc::new(SessionHandle {
        ps,
        op_tx,
        turns: Arc::new(Mutex::new(std::collections::HashSet::new())),
        waiters: Arc::new(Mutex::new(HashMap::new())),
        ordered_responses: Mutex::new(HashMap::new()),
        interrupt_changes: Mutex::new(HashMap::new()),
        event_stream_closed: AtomicBool::new(false),
        realtime_events: broadcast::channel(512).0,
        read_items: Mutex::new(super::thread_read::LiveItems::default()),
        stream: Mutex::new(SessionStream::new()),
        read_seed,
    });
    // Registered under the thread id, and refused when that thread is already
    // live here: two handles for one thread would be two writers for one
    // rollout, and the loser's map entry would evict the winner's.
    let already_live = {
        let imports = state.importing_threads.lock().unwrap();
        let mut sessions = state.sessions.lock().unwrap();
        if sessions.contains_key(&session_id) || imports.contains(&session_id) {
            true
        } else {
            sessions.insert(session_id.clone(), handle.clone());
            false
        }
    };
    if already_live {
        let _ = handle.ps.shutdown_and_wait().await;
        state
            .send(wire::error_response(
                &id,
                wire::SESSION_ALREADY_LIVE,
                &format!("session already live: {session_id}"),
            ))
            .await;
        return;
    }

    // Pump inner protocol events out as notifications / reverse requests.
    tokio::spawn(pump_session_events(
        state.clone(),
        session_id.clone(),
        handle.clone(),
        host_read,
    ));

    // Drive init_session and park until its reply event lands.
    let init_submission = state.next_submission_id("init");
    let (init_tx, init_rx) = oneshot::channel::<WaiterReply>();
    handle.register_waiter(init_submission.clone(), init_tx);
    // Startup retains its established permissive read, but stored rules must
    // never be flattened into the immutable project/config layer.
    let seeded = {
        let _guard = state.permission_rules_lock.lock().await;
        let rules = rule_store::load(&state.permission_rules_path).await;
        handle.ps.replace_durable_permission_rules(&rules)
    };
    eprintln!(
        "[session-start] {session_id} stage=rules-seeded elapsedMs={}",
        start_stage.elapsed().as_millis()
    );
    if let Err(error) = seeded {
        remove_owned_session(&state, &session_id, &handle).await;
        let _ = handle.ps.shutdown_and_wait().await;
        state
            .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
            .await;
        return;
    }
    // Resolve a BYO `modelSelection` into a concrete provider + stored-key auth
    // from the config file's `[modelProviders]`. No-op for subscription sessions
    // (which arrive with `provider` already set) and for hosts that still send a
    // fully-formed provider/auth.
    //
    // A named selection that cannot be honoured fails the start. Continuing would
    // run the session on whatever provider the endpoint defaults to, which is a
    // different model and possibly a different account than the caller asked for.
    if let Err(error) = resolve_raw_model_provider(&state, &mut init).await {
        remove_owned_session(&state, &session_id, &handle).await;
        let _ = handle.ps.shutdown_and_wait().await;
        state
            .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
            .await;
        return;
    }
    eprintln!(
        "[session-start] {session_id} stage=provider-resolved elapsedMs={}",
        start_stage.elapsed().as_millis()
    );
    let _ = handle.op_tx.send(Payload::Object(vec![
        ("id".into(), Value::String(init_submission).into()),
        (
            "op".into(),
            Payload::Object(vec![
                ("type".into(), Value::String("init_session".into()).into()),
                ("init".into(), Payload::Encoded(init.raw())),
            ]),
        ),
    ]));
    eprintln!(
        "[session-start] {session_id} stage=init-dispatched elapsedMs={}",
        start_stage.elapsed().as_millis()
    );

    match tokio::time::timeout(state.startup_init_ack_budget, init_rx).await {
        Ok(Ok(event)) => {
            eprintln!(
                "[session-start] {session_id} stage=init-ack-settled elapsedMs={}",
                start_stage.elapsed().as_millis()
            );
            let event = event.value;
            if let Some(error) = exact_text(exact_field(&event, "error")) {
                remove_owned_session(&state, &session_id, &handle).await;
                let _ = handle.ps.shutdown_and_wait().await;
                state
                    .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                    .await;
                return;
            }
            // Idle-unload bookkeeping: the session is live now — it starts
            // subscribed and idle, and its watcher unloads it once it stays
            // unwatched and quiet for the delay. A start that fails before this
            // point never registers and broadcasts nothing.
            state.session_status.register(&session_id);
            state
                .notify(
                    "session/statusChanged",
                    serde_json::to_value(wire::SessionStatusChangedNotification {
                        session_id: session_id.clone(),
                        status: wire::SessionStatus::Idle,
                    })
                    .expect("session status notification serializes"),
                )
                .await;
            state
                .session_subscriptions
                .lock()
                .unwrap()
                .insert(session_id.clone(), tokio::sync::watch::channel(true).0);
            session_unload::ensure_watcher(&state, &session_id, &handle);

            let mut initialized = exact_field(&event, "initialized").clone();
            if materialize_fork || materialize_side_task {
                // Unlike ordinary lazy session/start, a live fork must return
                // only after its captured transcript owns a durable new rollout.
                // Side tasks must also materialize before startup can report
                // their actual identity and resolved behavior. Ordinary selected
                // roots resolve through the explicit preparation phase.
                let materialized = materialize_session(
                    &handle,
                    fork_live.as_deref(),
                    side_task
                        .as_ref()
                        .map(|side| (side.parent_thread_id.as_str(), session_id.as_str())),
                )
                .await;
                match materialized {
                    Err(error) => {
                        remove_owned_session(&state, &session_id, &handle).await;
                        let _ = handle.ps.shutdown_and_wait().await;
                        state
                            .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                            .await;
                        return;
                    }
                    Ok(Some(Value::Object(identity))) => {
                        for (key, value) in identity {
                            initialized
                                .as_object_mut()
                                .expect("initialized reply is an object")
                                .insert(key, exact_payload::runtime(value));
                        }
                    }
                    Ok(_) => {}
                }
            }
            if has_selected_agent {
                initialized
                    .as_object_mut()
                    .expect("initialized reply is an object")
                    .insert(
                        "agentDefinitionName",
                        selected_agent_name
                            .map(|name| {
                                name.to_runtime()
                                    .expect("selected agent name is runtime JSON")
                                    .0
                            })
                            .unwrap_or(RuntimeJson::Null),
                    );
            }
            let result = wire::SessionStartResult {
                session_id,
                initialized: RuntimeValue(initialized),
            };
            state
                .send(exact_payload::response(
                    &id,
                    Payload::Encoded(
                        RawJson::from_serialize(&result).expect("session start result encodes"),
                    ),
                ))
                .await;
        }
        Ok(Err(_)) => {
            remove_owned_session(&state, &session_id, &handle).await;
            state
                .send(wire::error_response(
                    &id,
                    wire::ENGINE_ERROR,
                    "session terminated before init_session completed",
                ))
                .await;
        }
        Err(_) => {
            eprintln!(
                "[session-start] {session_id} stage=init-ack-budget-exceeded elapsedMs={}",
                start_stage.elapsed().as_millis()
            );
            remove_owned_session(&state, &session_id, &handle).await;
            state.send(wire::error_response(
                &id,
                wire::ENGINE_ERROR,
                &format!(
                    "the engine session did not acknowledge startup within {}s; the [session-start] stage lines in the engine log name the stalled step",
                    state.startup_init_ack_budget.as_secs()
                ),
            )).await;
            // The stalled inner dispatch may be the very task shutdown waits
            // for; cleaning up inline would swallow the error we just reported.
            let stalled_handle = handle.clone();
            tokio::spawn(async move {
                let _ = stalled_handle.ps.shutdown_and_wait().await;
            });
        }
    }
}

async fn remove_owned_session(
    state: &Arc<AppServerState>,
    session_id: &str,
    handle: &Arc<SessionHandle>,
) {
    let removed = {
        let mut sessions = state.sessions.lock().unwrap();
        if sessions
            .get(session_id)
            .is_some_and(|current| Arc::ptr_eq(current, handle))
        {
            sessions.remove(session_id);
            true
        } else {
            false
        }
    };
    if removed {
        // A session that never went live must not leave idle-unload
        // bookkeeping behind.
        session_unload::discard_session_bookkeeping(state, session_id).await;
    }
}

async fn materialize_session(
    handle: &SessionHandle,
    snapshot: Option<&LiveForkSnapshot>,
    side_identity: Option<(&str, &str)>,
) -> Result<Option<Value>, String> {
    let session = handle.ps.prepare_session().await?;
    if let Some(snapshot) = snapshot {
        session.flush_persistent_fork(snapshot).await?;
    }
    if let Some((parent_thread_id, target_thread_id)) = side_identity {
        session.validate_side_task_identity(parent_thread_id)?;
        let thread_id = session
            .id
            .try_to_utf8()
            .map_err(|error| error.to_string())?;
        if thread_id != target_thread_id {
            return Err("endpoint factory returned a different side-task identity".to_string());
        }
        let parent = session
            .meta
            .parent_thread_id
            .as_ref()
            .ok_or("side task has no parent identity")?
            .try_to_utf8()
            .map_err(|error| error.to_string())?;
        return Ok(Some(serde_json::json!({
            "threadId": thread_id,
            "role": "side_task",
            "parentThreadId": parent,
            "ephemeral": session.is_ephemeral(),
        })));
    }
    Ok(None)
}

fn model_runtime_value(
    session: &klaus_agent_core::engine::session::Session,
) -> Result<Value, String> {
    let (
        revision,
        config,
        capabilities,
        change_id,
        pending_change_id,
        model_update_error,
        advisor_target,
        orchestrator_target,
        orchestration_mode,
        feature_targets,
    ) = loop {
        let (revision, config, capabilities) = session.model_runtime();
        let (change_id, pending_change_id, model_update_error) = session.model_update_status();
        let advisor_target = session.advisor_target_runtime();
        let orchestrator_target = session.orchestrator_target_runtime();
        let orchestration_mode = session.orchestration_mode_runtime();
        let feature_targets = session.feature_targets_runtime();
        if session.model_revision() == revision {
            break (
                revision,
                config,
                capabilities,
                change_id,
                pending_change_id,
                model_update_error,
                advisor_target,
                orchestrator_target,
                orchestration_mode,
                feature_targets,
            );
        }
    };
    let model = config
        .model
        .try_to_utf8()
        .map_err(|error| error.to_string())?;
    serde_json::to_value(wire::SessionModelRuntimeResult {
        revision,
        model,
        model_tiers: serde_json::to_value(config.model_tiers).map_err(|error| error.to_string())?,
        model_tier_targets: session.model_tier_targets_runtime(),
        advisor_target,
        orchestrator_target,
        feature_targets,
        effort: serde_json::to_value(config.effort).map_err(|error| error.to_string())?,
        thinking: serde_json::to_value(config.thinking).map_err(|error| error.to_string())?,
        ultra_mode: config.ultra_mode,
        orchestration_mode,
        fast_mode: config.fast_mode,
        context_window: config.context_window,
        max_output_tokens: config
            .max_output_tokens
            .unwrap_or(capabilities.max_output_tokens),
        max_output_retry_tokens: config.max_output_retry_tokens,
        capabilities: serde_json::to_value(capabilities).map_err(|error| error.to_string())?,
        change_id,
        pending_change_id,
        model_update_error,
    })
    .map_err(|error| error.to_string())
}

const MODEL_SOURCES: &[&str] = &[
    "trae",
    "kiro",
    "antigravity",
    "subscription",
    "codex",
    "qoder",
    "qwenwork",
    "workbuddy",
    "cursor",
    "kimi",
    "bailian",
    "minimax",
    "zhipu",
    "ark",
    "custom",
    "local",
];

/// `modelSource` is the non-secret attribution identity that inference events
/// and the composer's actual-request label carry. It must never become a
/// channel for a credential or an upstream URL, so its field set is closed here
/// rather than left to a permissive `RuntimeJson`. Tier targets are validated
/// by serde on `wire::ModelTierTarget` (`deny_unknown_fields`), so they need no
/// separate check.
fn validate_model_source(init: &Map<String, Value>) -> Result<(), String> {
    let Some(source) = init.get("modelSource") else {
        return Ok(());
    };
    let source = source.as_object().ok_or("modelSource must be an object")?;
    if source
        .keys()
        .any(|key| !["source", "modelId", "selection"].contains(&key.as_str()))
    {
        return Err(
            "modelSource may contain only non-secret source, modelId, and selection".to_string(),
        );
    }
    for (field, required) in [("source", true), ("modelId", false), ("selection", false)] {
        match source.get(field).and_then(Value::as_str) {
            Some(value)
                if !value.is_empty()
                    && value.trim() == value
                    && !value.chars().any(char::is_control) => {}
            Some(_) => {
                return Err(format!(
                    "modelSource.{field} must be a clean nonempty string"
                ));
            }
            None if required => {
                return Err(format!("modelSource.{field} must name a model source"));
            }
            None => {}
        }
    }
    if let Some(source_name) = source.get("source").and_then(Value::as_str) {
        if !MODEL_SOURCES.contains(&source_name) {
            return Err("modelSource.source must name a supported model source".to_string());
        }
        if let Some((prefix, _)) = source
            .get("selection")
            .and_then(Value::as_str)
            .and_then(|selection| selection.split_once(':'))
        {
            if MODEL_SOURCES.contains(&prefix) && prefix != source_name {
                return Err(
                    "modelSource.selection must belong to its modelSource source".to_string(),
                );
            }
        }
    }
    Ok(())
}

fn preserve_model_selection_metadata(init: &mut Map<String, Value>) {
    let selection = init
        .get("modelSelection")
        .and_then(Value::as_str)
        .map(str::to_string);
    if !init.contains_key("modelSource") {
        if let Some((source, model_id)) = selection
            .as_deref()
            .and_then(|selection| selection.split_once(':'))
        {
            if MODEL_SOURCES.contains(&source) && !model_id.is_empty() {
                init.insert(
                    "modelSource".to_string(),
                    serde_json::json!({
                        "source": source,
                        "modelId": model_id,
                    }),
                );
            }
        }
    }
    if let Some(source) = init.get_mut("modelSource").and_then(Value::as_object_mut) {
        let selection = selection.unwrap_or_else(|| {
            format!(
                "{}:{}",
                source["source"].as_str().unwrap(),
                source["modelId"].as_str().unwrap()
            )
        });
        source
            .entry("selection")
            .or_insert(Value::String(selection));
    }
}

fn validate_inference_init(init: &Value) -> Result<(), String> {
    let object = init.as_object().ok_or("model init must be an object")?;
    validate_model_source(object)?;
    for key in object.keys() {
        if ![
            "provider",
            "auth",
            "modelSelection",
            "modelTierSelections",
            "modelTierTargets",
            "advisorTarget",
            "orchestratorTarget",
            "orchestrationMode",
            "featureTargets",
            "modelSource",
            "capabilities",
            "capabilitiesByModel",
            "pricing",
            "config",
        ]
        .contains(&key.as_str())
        {
            return Err(format!(
                "model init cannot change {key}; use its dedicated session method"
            ));
        }
    }
    if object
        .get("provider")
        .is_some_and(|value| !value.is_object())
    {
        return Err("provider must be an object".to_string());
    }
    if object
        .get("modelSelection")
        .is_some_and(|value| value.as_str().is_none_or(|value| value.trim().is_empty()))
    {
        return Err("modelSelection must be a nonempty model ID".to_string());
    }
    let has_provider = object.contains_key("provider");
    let has_selection = object.contains_key("modelSelection");
    let tiers_only = !has_provider
        && !has_selection
        && object.len() == 1
        && object.contains_key("modelTierTargets");
    if has_provider == has_selection && !tiers_only {
        return Err("model init requires exactly one provider or modelSelection".to_string());
    }
    if let Some(tiers) = object.get("modelTierSelections") {
        let tiers = tiers
            .as_object()
            .ok_or("modelTierSelections must be an object")?;
        if tiers.iter().any(|(tier, value)| {
            !["high", "medium", "low"].contains(&tier.as_str())
                || value.as_str().is_none_or(|value| value.trim().is_empty())
        }) {
            return Err(
                "modelTierSelections must map high, medium, or low to nonempty model IDs"
                    .to_string(),
            );
        }
    }
    if let Some(targets) = object.get("modelTierTargets") {
        // Decode through the same typed shape the facade resolves with, so the
        // rejection rules live in one place: `deny_unknown_fields` refuses an
        // unrecognized tier slot or a stray field on a target, and a `resolved`
        // target missing its provider/auth/capabilities fails as incomplete.
        serde_json::from_value::<wire::ModelTierTargets>(targets.clone()).map_err(|error| {
            format!("modelTierTargets must hold source, resolved, or unavailable targets: {error}")
        })?;
    }
    if let Some(advisor) = object.get("advisorTarget") {
        serde_json::from_value::<Option<wire::ModelTierTarget>>(advisor.clone()).map_err(
            |error| {
                format!("advisorTarget must be null or a source, resolved, or unavailable target: {error}")
            },
        )?;
    }
    if let Some(orchestrator) = object.get("orchestratorTarget") {
        serde_json::from_value::<Option<wire::ModelTierTarget>>(orchestrator.clone()).map_err(
            |error| {
                format!("orchestratorTarget must be null or a source, resolved, or unavailable target: {error}")
            },
        )?;
    }
    if let Some(mode) = object.get("orchestrationMode") {
        if !mode.is_null() && !mode.is_boolean() {
            return Err("orchestrationMode must be a boolean or null".to_string());
        }
    }
    if let Some(features) = object.get("featureTargets") {
        serde_json::from_value::<Option<wire::FeatureTargets>>(features.clone()).map_err(
            |error| {
                format!("featureTargets must hold null or per-feature source, resolved, or unavailable targets: {error}")
            },
        )?;
    }
    if let Some(config) = object.get("config") {
        let config = config.as_object().ok_or("model config must be an object")?;
        for key in config.keys() {
            if ![
                "model",
                "modelTiers",
                "effort",
                "ultraMode",
                "modelVerbosity",
                "reasoningSummary",
                "webSearch",
                "approvalReview",
                "thinking",
                "fastMode",
                "contextWindow",
                "maxOutputTokens",
                "maxOutputRetryTokens",
            ]
            .contains(&key.as_str())
            {
                return Err(format!(
                    "model config cannot change {key}; use its dedicated session method"
                ));
            }
        }
        if let Some(search) = config.get("webSearch") {
            let search = search.as_object().ok_or("webSearch must be an object")?;
            if search.keys().any(|key| key != "access") {
                return Err("model config may change only webSearch.access; other search fields require their dedicated settings update".into());
            }
        }
        for key in ["contextWindow", "maxOutputTokens", "maxOutputRetryTokens"] {
            if config.get(key).is_some_and(|value| {
                !value.is_null() && value.as_u64().is_none_or(|value| value == 0)
            }) {
                return Err(format!("{key} must be a positive integer or null"));
            }
        }
    }
    Ok(())
}

fn validate_model_change_id(change_id: &str) -> Result<(), String> {
    if change_id.is_empty()
        || change_id.trim() != change_id
        || change_id.len() > 1024
        || change_id.chars().any(char::is_control)
    {
        return Err(
            "changeId must be a nonempty identifier without surrounding whitespace or control characters"
                .to_string(),
        );
    }
    Ok(())
}

fn fail_model_update_from_params(state: &AppServerState, params: &Value, error: String) {
    let Some(session_id) = params.get("sessionId").and_then(Value::as_str) else {
        return;
    };
    let Some(change_id) = params.get("changeId").and_then(Value::as_str) else {
        return;
    };
    if let Some(session) = state
        .session(session_id)
        .and_then(|handle| handle.ps.existing_session())
    {
        session.fail_model_update(change_id, error);
    }
}

async fn session_reconfigure_model(
    state: Arc<AppServerState>,
    id: Value,
    params: wire::SessionReconfigureModelParams,
) {
    let Some(handle) = state.session(&params.session_id) else {
        return send_unknown_session(&state, &id, &params.session_id).await;
    };
    let Some(session) = handle.ps.existing_session() else {
        return state
            .send(wire::error_response(
                &id,
                wire::ENGINE_ERROR,
                "model reconfiguration requires an existing live session",
            ))
            .await;
    };
    let validation = validate_inference_init(&params.init).and_then(|_| {
        params
            .change_id
            .as_deref()
            .map(validate_model_change_id)
            .transpose()
            .map(|_| ())
    });
    if let Err(error) = validation {
        if let Some(change_id) = params.change_id.as_deref() {
            session.fail_model_update(change_id, error.clone());
        }
        return state
            .send(wire::error_response(&id, wire::INVALID_PARAMS, &error))
            .await;
    }
    let expected = params
        .expected_revision
        .unwrap_or_else(|| session.model_revision());
    let mut init = params.init;
    let prepare = async {
        resolve_model_provider(&state, &mut init).await?;
        let init = klaus_agent_core::runtime_values::runtime_codec::decode_text(
            &RuntimeString::from(init.to_string()),
        )
        .map_err(|error| format!("invalid model init: {error}"))?;
        handle
            .ps
            .reconfigure_model_with_change_id(init, expected, params.change_id.clone())
            .await
    };
    let result = tokio::time::timeout(std::time::Duration::from_secs(75), async {
        tokio::select! {
            biased;
            _ = handle.ps.wait_until_closing() => Err("model preparation cancelled because the session is closing".to_string()),
            result = prepare => result,
        }
    })
    .await
    .unwrap_or_else(|_| Err("model provider preparation timed out after 75 seconds".to_string()));
    match result {
        Ok(revision) => {
            state
                .send(wire::response(
                    &id,
                    serde_json::to_value(wire::SessionReconfigureModelResult {
                        revision,
                        change_id: params.change_id,
                    })
                    .unwrap(),
                ))
                .await
        }
        Err(error) => {
            if let Some(change_id) = params.change_id.as_deref() {
                session.fail_model_update(change_id, error.clone());
            }
            state
                .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                .await;
        }
    }
}

/// Read inner protocol event lines for one session and translate them:
///   * parked waiter replies (init_session) settle their oneshot;
///   * approval / elicitation asks become server->client requests;
///   * a durable permission grant is written to the engine's rule store;
///   * a `result` reply of a tracked turn emits `turn/completed`;
///   * everything else passes through as `session/event`.
async fn pump_session_events(
    state: Arc<AppServerState>,
    session_id: String,
    handle: Arc<SessionHandle>,
    host_read: tokio::io::ReadHalf<tokio::io::DuplexStream>,
) {
    let mut lines = BufReader::new(host_read).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        if line.trim().is_empty() {
            continue;
        }
        let mut event = match exact_payload::parse(&line) {
            Ok(event) => event,
            Err(error) => {
                eprintln!("[app-server] invalid session event: {error}");
                break;
            }
        };
        let kind = exact_text(exact_field(exact_field(&event, "event"), "type"));
        if kind.as_deref() == Some("turn.started") {
            for key in ["goalContinuation", "inputOrigin", "goalId"] {
                if let Some(value) = event.get(key).cloned() {
                    if let Some(inner) = event.get_mut("event").and_then(RuntimeJson::as_object_mut)
                    {
                        inner.insert(key, value);
                    }
                }
            }
        }
        let submission_id = exact_text(exact_field(&event, "submissionId")).unwrap_or_default();
        if event.get("turnInterrupt").is_some() || event.get("error").is_some() {
            queue::publish_interrupt_change(&state, &handle, &submission_id).await;
        }
        if !submission_id.is_empty() && kind.as_deref() == Some("turn.started") {
            handle.turns.lock().unwrap().insert(submission_id.clone());
            session_unload::refresh_status(&state, &session_id).await;
        }
        let observed = {
            let mut items = handle.read_items.lock().unwrap();
            items.observe(&submission_id, &event).map(|()| {
                exact_text(exact_field(exact_field(&event, "event"), "turnId"))
                    .or_else(|| items.submission_turns.get(&submission_id).cloned())
            })
        };
        let core_turn_id = match observed {
            Ok(id) => id,
            Err(error) => {
                eprintln!("[app-server] invalid live-item control field: {error}");
                state
                    .notify_session_runtime(
                        &session_id,
                        "session/event",
                        exact_payload::object([
                            ("sessionId", session_id.clone().into()),
                            ("event", event.clone()),
                        ]),
                        None,
                    )
                    .await;
                break;
            }
        };
        let _ = handle.realtime_events.send(event.clone());
        if claim_waiter(&handle, &event).await {
            continue;
        }
        if exact_field(exact_field(&event, "pendingResume"), "started").as_bool() == Some(false)
            || event.get("error").is_some()
        {
            // The turns guard must drop before the status recomputation:
            // it locks the same mutex, and a std guard held across the await
            // would deadlock the pump.
            let removed = handle.turns.lock().unwrap().remove(&submission_id);
            if removed {
                session_unload::refresh_status(&state, &session_id).await;
            }
        }
        if kind.as_deref() == Some("approval.requested") {
            if let Some(request) = exact_field(&event, "event").get("request") {
                if let Some(request_id) = exact_text(exact_field(request, "id")) {
                    send_reverse_request_runtime(
                        &state,
                        &session_id,
                        ReverseKind::Approval { request_id },
                        "approval/request",
                        exact_payload::object([
                            ("sessionId", session_id.clone().into()),
                            ("submissionId", submission_id.clone().into()),
                            ("request", request.clone()),
                        ]),
                    )
                    .await;
                    continue;
                }
            }
        }
        if let Some(elicitation) = event.get("elicitation").filter(|request| {
            exact_text(exact_field(
                exact_field(exact_field(request, "request"), "metadata"),
                "kind",
            ))
            .as_deref()
                != Some("request_user_input")
        }) {
            if let Some(request_id) = exact_text(exact_field(elicitation, "requestId")) {
                send_reverse_request_runtime(
                    &state,
                    &session_id,
                    ReverseKind::Elicitation { request_id },
                    "elicitation/request",
                    exact_payload::object([
                        ("sessionId", session_id.clone().into()),
                        ("submissionId", submission_id.clone().into()),
                        ("elicitation", elicitation.clone()),
                    ]),
                )
                .await;
                continue;
            }
        }
        let (stream_id, stream_revision) = {
            let mut stream = handle.stream.lock().unwrap();
            let revision = stream.record(&event);
            (stream.id.clone(), revision)
        };
        // Only the permission control DTO crosses its existing scalar validator.
        // Transcript, arguments, deltas and other opaque content never do.
        if kind.as_deref() == Some("permission_update") {
            match exact_payload::scalar(exact_field(&event, "event")) {
                Ok(inner) => {
                    persist_durable_grant(&state, &handle, &serde_json::json!({"event":inner}))
                        .await
                }
                Err(error) => eprintln!("[app-server] invalid permission control event: {error}"),
            }
        }
        if let Some(decision) = event.get("goalContinuationDecision") {
            if let Ok(decision) = klaus_agent_core::runtime_values::runtime_codec::decode_value::<
                klaus_agent_protocol::types::ProtocolGoalContinuation,
            >(decision)
            {
                state
                    .notify_session_revision(
                        &session_id,
                        "goal.continuation",
                        serde_json::to_value(wire::GoalContinuationNotification {
                            session_id: session_id.clone(),
                            stream_id: None,
                            stream_revision: None,
                            decision_id: decision.decision_id.try_to_utf8().unwrap_or_default(),
                            prior_submission_id: decision
                                .prior_submission_id
                                .try_to_utf8()
                                .unwrap_or_default(),
                            goal_id: decision.goal_id.and_then(|id| id.try_to_utf8().ok()),
                            successor_submission_id: decision
                                .successor_submission_id
                                .and_then(|id| id.try_to_utf8().ok()),
                            pending: decision.pending,
                            continued: decision.continued,
                            prior_completion_notification_disposition: decision
                                .prior_completion_notification_disposition
                                .map(Into::into),
                        })
                        .expect("continuation notification"),
                        &stream_id,
                        stream_revision,
                    )
                    .await;
            }
        }
        let tracked_turn_completed =
            event.get("result").is_some() && handle.turns.lock().unwrap().remove(&submission_id);
        if kind.as_deref() == Some("turn.failed") {
            handle.turns.lock().unwrap().remove(&submission_id);
            handle.read_items.lock().unwrap().finish(&submission_id);
        }
        if tracked_turn_completed || kind.as_deref() == Some("turn.failed") {
            session_unload::refresh_status(&state, &session_id).await;
        }
        if tracked_turn_completed {
            handle.read_items.lock().unwrap().finish(&submission_id);
            let mut params = exact_payload::object([
                ("sessionId", session_id.clone().into()),
                ("turnId", submission_id.clone().into()),
                ("result", exact_field(&event, "result").clone()),
            ]);
            if let Some(disposition) = event.get("terminalNotificationDisposition") {
                params
                    .as_object_mut()
                    .expect("notification object")
                    .insert("terminalNotificationDisposition", disposition.clone());
            }
            state
                .notify_session_runtime(
                    &session_id,
                    "turn/completed",
                    params,
                    Some((&stream_id, stream_revision)),
                )
                .await;
        }
        if state.product_events.load(Ordering::Acquire) {
            let projected = emit_exact_product_notifications(
                &state,
                &session_id,
                &submission_id,
                core_turn_id.as_deref(),
                &event,
                &stream_id,
                stream_revision,
            )
            .await;
            if !projected && !tracked_turn_completed {
                state
                    .notify_session_runtime(
                        &session_id,
                        "session/rawEvent",
                        exact_payload::object([
                            ("sessionId", session_id.clone().into()),
                            ("event", event.clone()),
                        ]),
                        Some((&stream_id, stream_revision)),
                    )
                    .await;
            }
        }
        if let Some(deleted) = event.get("deleted") {
            if let Ok(deleted) = exact_payload::scalar(deleted) {
                notify_thread_deleted(&state, &serde_json::json!({"deleted":deleted})).await;
            }
        }
        state
            .notify_session_runtime(
                &session_id,
                "session/event",
                exact_payload::object([("sessionId", session_id.clone().into()), ("event", event)]),
                Some((&stream_id, stream_revision)),
            )
            .await;
    }
    handle.close_waiters().await;
}

/// Project one raw protocol event onto the typed Thread/Turn/Item product
/// notification surface. Engine events already speak the product vocabulary
/// (`thread.started`, `item.*`, deltas); this lifts them to first-class
/// JSON-RPC notifications instead of leaving them inside the envelope.
/// Returns whether a product notification was actually emitted — envelopes
/// that were not projected must flow on `session/rawEvent` instead.
async fn emit_exact_product_notifications(
    state: &Arc<AppServerState>,
    session_id: &str,
    submission_id: &str,
    core_turn_id: Option<&str>,
    event: &RuntimeJson,
    stream_id: &str,
    stream_revision: u64,
) -> bool {
    let notify = async |method: &str, params: RuntimeJson| {
        state
            .notify_session_runtime(
                session_id,
                method,
                params,
                Some((stream_id, stream_revision)),
            )
            .await
    };
    if let Some(submission) = event.get("goalSubmission") {
        if let (Some(message), Some(display), Some(text)) = (
            exact_text(exact_field(submission, "messageUuid")),
            exact_text(exact_field(submission, "displayUuid")),
            submission.get("text").filter(|text| text.is_string()),
        ) {
            if !message.is_empty() && !display.is_empty() {
                notify(
                    "item/completed",
                    exact_payload::object([
                        ("sessionId", session_id.into()),
                        (
                            "item",
                            exact_payload::object([
                                ("id", message.into()),
                                ("type", "user_message".into()),
                                ("status", "completed".into()),
                                ("text", text.clone()),
                                ("isMeta", false.into()),
                            ]),
                        ),
                    ]),
                )
                .await;
                return true;
            }
        }
        return false;
    }
    if let Some(params) = background_process_changed_notification(session_id, event) {
        notify("backgroundProcess/changed", params).await;
        return true;
    }
    if let Some(params) = task_list_changed_notification(session_id, event) {
        notify("taskList/changed", params).await;
        return true;
    }
    let Some(inner) = event.get("event") else {
        return false;
    };
    let Some(kind) = exact_text(exact_field(inner, "type")) else {
        return false;
    };
    let mut params = exact_payload::object([
        ("sessionId", session_id.into()),
        ("submissionId", submission_id.into()),
    ]);
    let object = params.as_object_mut().expect("notification object");
    match kind.as_str() {
        "approval_review_strict_required" => {
            let (Some(turn_id), Some(started_at_ms)) =
                (inner.get("turnId"), inner.get("startedAtMs"))
            else {
                return false;
            };
            notify(
                "autoApprovalReview/strictReviewRequired",
                exact_payload::object([
                    ("sessionId", session_id.into()),
                    ("turnId", turn_id.clone()),
                    ("startedAtMs", started_at_ms.clone()),
                ]),
            )
            .await;
            true
        }
        "approval_review_warning" => {
            let Some(message) = inner.get("message") else {
                return false;
            };
            let mut params = exact_payload::object([
                ("sessionId", session_id.into()),
                ("message", message.clone()),
            ]);
            if let Some(kind) = inner.get("kind") {
                params
                    .as_object_mut()
                    .expect("warning object")
                    .insert("kind", kind.clone());
            }
            notify("approvalReview/warning", params).await;
            true
        }
        "approval_review" => {
            let Some(review) = inner.get("review") else {
                return false;
            };
            let Some((method, projected)) =
                approval_review_product_notification(session_id, review)
            else {
                return false;
            };
            notify(method, projected).await;
            true
        }
        "question.requested" | "question.resolved" => {
            let (method, key) = if kind == "question.requested" {
                ("question/requested", "question")
            } else {
                ("question/resolved", "questionResolved")
            };
            let Some(question) = inner.get(key) else {
                return false;
            };
            object.insert(key, question.clone());
            notify(method, params).await;
            true
        }
        "thread.started" => {
            let Some(id) = exact_text(exact_field(inner, "threadId")) else {
                return false;
            };
            object.insert("threadId", id.into());
            notify("thread/started", params).await;
            true
        }
        "turn.started" | "turn.failed" => {
            let mut detail = inner.clone();
            for key in ["goalContinuation", "inputOrigin", "goalId"] {
                if let Some(value) = event.get(key) {
                    detail
                        .as_object_mut()
                        .expect("turn detail")
                        .insert(key, value.clone());
                }
            }
            for key in [
                "goalContinuation",
                "inputOrigin",
                "goalId",
                "terminalNotificationDisposition",
            ] {
                if let Some(value) = event.get(key) {
                    object.insert(key, value.clone());
                }
            }
            object.insert("detail", detail);
            notify(
                if kind == "turn.started" {
                    "turn/started"
                } else {
                    "turn/failed"
                },
                params,
            )
            .await;
            true
        }
        "item.started" | "item.updated" | "item.completed" | "text.delta" | "thinking.delta" => {
            let method = match kind.as_str() {
                "item.started" => "item/started",
                "item.updated" => "item/updated",
                "item.completed" => "item/completed",
                "text.delta" => "item/agentMessage/delta",
                _ => "item/reasoning/delta",
            };
            if kind.ends_with("delta") {
                let Some(text) = inner.get("text").filter(|text| text.is_string()) else {
                    return false;
                };
                object.insert("delta", text.clone());
                for key in ["messageId", "phase"] {
                    if let Some(value) = inner.get(key).filter(|value| value.is_string()) {
                        object.insert(key, value.clone());
                    }
                }
            } else {
                let Some(item) = inner.get("item") else {
                    return false;
                };
                object.insert("item", item.clone());
            }
            if let Some(id) = core_turn_id {
                object.insert("turnId", id.into());
            }
            // Inference is a scalar, closed service-owned DTO; unknown native
            // annotations retain the previous omission behavior.
            if let Some(inference) = public_inference(inner) {
                object.insert(
                    "inference",
                    exact_payload::runtime(serde_json::to_value(inference).expect("inference DTO")),
                );
            }
            notify(method, params).await;
            true
        }
        _ => false,
    }
}

pub(super) fn approval_review_product_notification(
    session_id: &str,
    review: &RuntimeJson,
) -> Option<(&'static str, RuntimeJson)> {
    let status = exact_text(review.get("status")?)?;
    let started = status == "in_progress";
    let mut action = review.get("action")?.clone();
    let action_object = action.as_object_mut()?;
    if let Some(kind) = action_object.get("type").and_then(exact_text) {
        let product_kind = match kind.as_str() {
            "write_stdin" => "writeStdin",
            "apply_patch" => "applyPatch",
            "network_access" => "networkAccess",
            "mcp_tool_call" => "mcpToolCall",
            "browser_tool_call" => "browserToolCall",
            "request_permissions" => "requestPermissions",
            "command" => "command",
            "execve" => "execve",
            _ => return None,
        };
        action_object.insert("type", product_kind.into());
    }
    if let Some(protocol) = action_object.get("protocol").and_then(exact_text) {
        match protocol.as_str() {
            "socks5_tcp" => {
                action_object.insert("protocol", "socks5Tcp".into());
            }
            "socks5_udp" => {
                action_object.insert("protocol", "socks5Udp".into());
            }
            _ => {}
        }
    }
    if action_object.get("source").and_then(exact_text).as_deref() == Some("unified_exec") {
        action_object.insert("source", "unifiedExec".into());
    }
    let product_status = match status.as_str() {
        "in_progress" => "inProgress",
        "timed_out" => "timedOut",
        "approved" => "approved",
        "denied" => "denied",
        "aborted" => "aborted",
        _ => return None,
    };
    let null = RuntimeJson::Null;
    let detail = exact_payload::object([
        ("status", product_status.into()),
        (
            "riskLevel",
            review.get("riskLevel").unwrap_or(&null).clone(),
        ),
        (
            "userAuthorization",
            review.get("userAuthorization").unwrap_or(&null).clone(),
        ),
        (
            "rationale",
            review.get("rationale").unwrap_or(&null).clone(),
        ),
    ]);
    let mut params = exact_payload::object([
        ("sessionId", session_id.into()),
        ("turnId", review.get("turnId")?.clone()),
        ("startedAtMs", review.get("startedAtMs")?.clone()),
        ("reviewId", review.get("id")?.clone()),
        (
            "targetItemId",
            review.get("targetItemId").unwrap_or(&null).clone(),
        ),
        ("review", detail),
        ("action", action),
    ]);
    if !started {
        let object = params.as_object_mut()?;
        object.insert("completedAtMs", review.get("completedAtMs")?.clone());
        object.insert(
            "decisionSource",
            review
                .get("decisionSource")
                .cloned()
                .unwrap_or_else(|| "agent".into()),
        );
    }
    Some((
        if started {
            "item/autoApprovalReview/started"
        } else {
            "item/autoApprovalReview/completed"
        },
        params,
    ))
}

fn public_inference(event: &RuntimeJson) -> Option<wire::PublicInference> {
    serde_json::from_value(exact_payload::scalar(event.get("inference")?).ok()?).ok()
}

fn background_process_changed_notification(
    session_id: &str,
    event: &RuntimeJson,
) -> Option<RuntimeJson> {
    Some(exact_payload::object([
        ("sessionId", session_id.into()),
        ("event", event.get("backgroundProcessEvent")?.clone()),
    ]))
}

fn task_list_changed_notification(session_id: &str, event: &RuntimeJson) -> Option<RuntimeJson> {
    let change = event.get("taskListChange")?;
    let mut list = RuntimeObject::new();
    list.insert("taskListId", exact_text(change.get("taskListId")?)?.into());
    if let Some(id) = change.get("planId").and_then(exact_text) {
        list.insert("planId", id.into());
    }
    list.insert(
        "planKind",
        exact_text(exact_field(change, "planKind"))
            .unwrap_or_else(|| "tasks".into())
            .into(),
    );
    list.insert("generation", exact_text(change.get("generation")?)?.into());
    list.insert("revision", change.get("revision")?.as_u64()?.into());
    list.insert(
        "fingerprint",
        exact_text(change.get("fingerprint")?)?.into(),
    );
    list.insert(
        "tasks",
        RuntimeJson::Array(change.get("tasks")?.as_array()?.clone()),
    );
    Some(exact_payload::object([
        ("sessionId", session_id.into()),
        ("taskList", RuntimeJson::Object(list)),
    ]))
}

async fn send_reverse_request_runtime(
    state: &Arc<AppServerState>,
    session_id: &str,
    kind: ReverseKind,
    method: &str,
    params: RuntimeJson,
) {
    let reverse_id = state.next_reverse.fetch_add(1, Ordering::Relaxed);
    state.pending_reverse.lock().unwrap().insert(
        reverse_id,
        PendingReverse {
            session_id: session_id.to_string(),
            kind,
        },
    );
    state
        .send(exact_payload::object([
            ("id", reverse_id.into()),
            ("method", method.into()),
            ("params", params),
        ]))
        .await;
    session_unload::refresh_status(state, session_id).await;
}

/// The client answered (or failed) a reverse request: translate its response
/// into the resolving inner op. Errors fail closed (deny / decline).
fn handle_reverse_response(
    state: &Arc<AppServerState>,
    id: &Value,
    result: Result<RawJson, RawJson>,
) {
    let Some(reverse_id) = id.as_u64() else {
        return;
    };
    let pending = state.pending_reverse.lock().unwrap().remove(&reverse_id);
    let Some(pending) = pending else {
        return;
    };
    {
        // Answering the request flipped the session's lifecycle status;
        // publish it without blocking the response path on the broadcast.
        let state = Arc::clone(state);
        let session_id = pending.session_id.clone();
        tokio::spawn(async move {
            session_unload::refresh_status(&state, &session_id).await;
        });
    }
    let Some(handle) = state.session(&pending.session_id) else {
        return;
    };
    let submission_id = state.next_submission_id("sub");
    let response_value = result
        .ok()
        .and_then(|value| value.get("response").ok().flatten())
        .and_then(|value| value.to_runtime().ok())
        .map(|value| value.0)
        .filter(RuntimeJson::is_object);
    let (kind, request_id, fallback) = match pending.kind {
        ReverseKind::Approval { request_id } => (
            "approve",
            request_id,
            exact_payload::object([("decision", "deny".into())]),
        ),
        ReverseKind::Elicitation { request_id } => (
            "resolve_elicitation",
            request_id,
            exact_payload::object([("kind", "decline".into())]),
        ),
    };
    let op = exact_payload::object([
        ("type", kind.into()),
        ("requestId", request_id.into()),
        ("response", response_value.unwrap_or(fallback)),
    ]);
    let _ = handle
        .op_tx
        .send(exact_payload::object([("id", submission_id.into()), ("op", op)]).into());
}

#[cfg(test)]
mod model_provider_tests {
    use super::*;
    use klaus_agent_core::mcp::credential_store::InMemoryCredentialStore;

    #[test]
    fn inference_init_accepts_and_preserves_host_tier_target_shapes() {
        let init = serde_json::json!({
            "provider": {"type": "openai", "model": "main"},
            "modelSource": {"source": "custom", "modelId": "entry-main"},
            "modelTierTargets": {
                "high": {"type": "source", "source": "subscription", "modelId": "opus"},
                "medium": {"type": "resolved", "source": "local", "modelId": "local-model",
                    "provider": {"type": "openai", "model": "local-model"},
                    "auth": {"type": "bearer", "token": "private-route-token"},
                    "capabilities": {"tools": false}, "pricing": []},
                "low": {"type": "unavailable", "source": "kimi", "modelId": "gone",
                    "code": "MODEL_UNAVAILABLE", "message": "The model is unavailable"}
            }
        });
        validate_inference_init(&init).unwrap();
        let decoded: klaus_agent_protocol::types::ProtocolSessionInit =
            klaus_agent_core::runtime_values::runtime_codec::decode_text(&RuntimeString::from(
                init.to_string(),
            ))
            .unwrap();
        let encoded =
            klaus_agent_core::runtime_values::runtime_codec::encode_text(&decoded).unwrap();
        let encoded: Value = serde_json::from_str(&encoded.try_to_utf8().unwrap()).unwrap();
        assert_eq!(encoded, init);
        let mut following = init.clone();
        following["modelTierTargets"] =
            serde_json::json!({"high": null, "medium": null, "low": null});
        validate_inference_init(&following).unwrap();
        for bad_target in [
            serde_json::json!({"unexpected": null}),
            serde_json::json!({"high": {"type": "resolved", "source": "local", "modelId": "m"}}),
            serde_json::json!({"low": {"type": "source", "source": "custom", "modelId": "m", "cwd": "/elsewhere"}}),
        ] {
            following["modelTierTargets"] = bad_target;
            assert!(validate_inference_init(&following).is_err());
        }
    }

    #[test]
    fn public_model_source_rejects_secret_and_lifecycle_fields() {
        for source in [
            serde_json::json!({"source": "custom", "modelId": "m", "token": "secret"}),
            serde_json::json!({"source": "custom", "modelId": "m", "baseUrl": "https://private.invalid"}),
            serde_json::json!({"source": "custom", "modelId": " m "}),
            serde_json::json!({"source": "klaus", "modelId": "retired-model"}),
            serde_json::json!({"source": "custom", "modelId": "m", "selection": "codex:m"}),
        ] {
            assert!(validate_inference_init(&serde_json::json!({
                "provider": {"type": "openai", "model": "m"}, "modelSource": source,
            }))
            .is_err());
        }
    }

    #[test]
    fn selection_metadata_survives_provider_resolution_field_removal() {
        let mut init = serde_json::json!({"modelSelection": "custom:entry-main"})
            .as_object()
            .unwrap()
            .clone();
        preserve_model_selection_metadata(&mut init);
        init.remove("modelSelection");
        assert_eq!(
            init["modelSource"],
            serde_json::json!({
                "source": "custom", "modelId": "entry-main", "selection": "custom:entry-main",
            })
        );
    }

    #[test]
    fn request_inference_projection_keeps_captured_identity_without_secrets() {
        let inference = serde_json::json!({
            "revision": 3, "changeId": "change-3", "model": "model-a", "source": "custom",
            "tier": "high", "selection": "custom:entry-a",
        });
        let mut annotated = inference.clone();
        annotated["auth"] = serde_json::json!({"token": "must-not-publish"});
        let projected = public_inference(&exact_payload::runtime(
            serde_json::json!({"inference": annotated}),
        ))
        .unwrap();
        assert_eq!(serde_json::to_value(projected).unwrap(), inference);
        assert!(public_inference(&exact_payload::runtime(
            serde_json::json!({"type": "text.delta", "text": "old history"})
        ))
        .is_none());
    }

    #[tokio::test]
    async fn event_stream_close_releases_registered_and_late_reply_waiters() {
        let create: CreateSessionFn =
            Arc::new(|_| Box::pin(async { Err("the waiter fixture must not initialize".into()) }));
        let ps = Arc::new(ProtocolServer::new(ProtocolServerOptions {
            create_session: create,
            lifecycle: None,
            plugin_admin: None,
            credential_store: None,
            input: Box::new(tokio::io::empty()),
            output: Box::new(tokio::io::sink()),
        }));
        let (op_tx, _op_rx) = mpsc::unbounded_channel();
        let handle = SessionHandle {
            ps,
            op_tx,
            turns: Arc::new(Mutex::new(HashSet::new())),
            waiters: Arc::new(Mutex::new(HashMap::new())),
            ordered_responses: Mutex::new(HashMap::new()),
            interrupt_changes: Mutex::new(HashMap::new()),
            event_stream_closed: AtomicBool::new(false),
            realtime_events: broadcast::channel(512).0,
            read_items: Mutex::new(super::super::thread_read::LiveItems::default()),
            stream: Mutex::new(SessionStream::new()),
            read_seed: None,
        };
        let (before, before_reply) = oneshot::channel();
        handle.register_waiter("before-eof".into(), before);
        handle.close_waiters().await;
        let (after, after_reply) = oneshot::channel();
        handle.register_waiter("after-eof".into(), after);
        for receiver in [before_reply, after_reply] {
            let result = tokio::time::timeout(std::time::Duration::from_millis(250), receiver)
                .await
                .expect("an ended event stream must not retain a reply consumer");
            assert!(result.is_err());
        }
        assert!(handle.waiters.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn closed_turn_channel_rolls_back_only_the_rejected_submission() {
        let create: CreateSessionFn = Arc::new(|_| {
            Box::pin(async { Err("the idle admission fixture must not initialize".into()) })
        });
        let ps = Arc::new(ProtocolServer::new(ProtocolServerOptions {
            create_session: create,
            lifecycle: None,
            plugin_admin: None,
            credential_store: None,
            input: Box::new(tokio::io::empty()),
            output: Box::new(tokio::io::sink()),
        }));
        let (op_tx, op_rx) = mpsc::unbounded_channel();
        drop(op_rx);
        let (waiter_tx, _waiter_rx) = oneshot::channel();
        let mut items = super::super::thread_read::LiveItems::default();
        items
            .submission_turns
            .insert("other-turn".into(), "core-turn".into());
        items
            .observe(
                "other-turn",
                &exact_payload::runtime(serde_json::json!({
                    "event":{"type":"item.started","item":{"id":"item"}}
                })),
            )
            .unwrap();
        let handle = SessionHandle {
            ps,
            op_tx,
            turns: Arc::new(Mutex::new(HashSet::from(["other-turn".into()]))),
            waiters: Arc::new(Mutex::new(HashMap::from([(
                "other-request".into(),
                waiter_tx,
            )]))),
            event_stream_closed: AtomicBool::new(false),
            realtime_events: broadcast::channel(512).0,
            ordered_responses: Mutex::new(HashMap::new()),
            interrupt_changes: Mutex::new(HashMap::new()),
            read_items: Mutex::new(items),
            stream: Mutex::new(SessionStream::new()),
            read_seed: None,
        };
        assert!(!handle.ps.is_closing());
        for _ in 0..2 {
            assert_eq!(
                handle.admit_turn("rejected", serde_json::json!({"type":"run","input":"text"})),
                Err("session terminated before the turn was accepted")
            );
            assert_eq!(
                *handle.turns.lock().unwrap(),
                HashSet::from(["other-turn".to_string()])
            );
            assert_eq!(handle.waiters.lock().unwrap().len(), 1);
            let items = handle.read_items.lock().unwrap();
            assert_eq!(items.submission_turns.len(), 1);
            assert_eq!(items.snapshot().len(), 1);
            assert_eq!(items.submission_turns["other-turn"], "core-turn");
            assert_eq!(
                items.snapshot()["core-turn"],
                vec![exact_payload::runtime(serde_json::json!({"id":"item"}))]
            );
        }
        handle.ps.shutdown_and_wait().await.unwrap();
    }

    #[test]
    fn task_list_change_becomes_one_typed_full_state_notification() {
        let notification = task_list_changed_notification(
            "session-1",
            &exact_payload::runtime(serde_json::json!({
                "submissionId": "",
                "taskListChange": {
                    "sessionId": "inner-id-is-not-forwarded",
                    "taskListId": "session-1",
                    "planId": "plan-1",
                    "planKind": "plan",
                    "generation": "gen-1",
                    "revision": 7,
                    "fingerprint": "sha256:tasks",
                    "tasks": [{ "id": "1", "subject": "Ship it" }]
                }
            })),
        )
        .expect("a complete task-list change projects");

        assert_eq!(
            exact_payload::scalar(&notification).unwrap(),
            serde_json::json!({
                "sessionId": "session-1",
                "taskList": {
                    "taskListId": "session-1",
                    "planId": "plan-1",
                    "planKind": "plan",
                    "generation": "gen-1",
                    "revision": 7,
                    "fingerprint": "sha256:tasks",
                    "tasks": [{ "id": "1", "subject": "Ship it" }]
                }
            })
        );
    }

    #[test]
    fn background_process_event_becomes_one_typed_product_notification() {
        let notification = background_process_changed_notification(
            "session-1",
            &exact_payload::runtime(serde_json::json!({
                "submissionId": "",
                "backgroundProcessEvent": {
                    "subtype": "task_progress",
                    "shellId": "shell-1",
                    "afterOffset": 0,
                    "nextOffset": 5,
                    "output": "hello"
                }
            })),
        )
        .expect("background process event projects");

        assert_eq!(
            exact_payload::scalar(&notification).unwrap(),
            serde_json::json!({
                "sessionId": "session-1",
                "event": {
                    "subtype": "task_progress",
                    "shellId": "shell-1",
                    "afterOffset": 0,
                    "nextOffset": 5,
                    "output": "hello"
                }
            })
        );
    }

    fn providers() -> serde_json::Map<String, Value> {
        serde_json::json!({
            "byo-default": {
                "provider": "openai", "model": "gpt-4o", "openaiApi": "chat",
                "default": true, "maxContextTokens": 128000
            },
            "byo-opus": { "provider": "anthropic", "model": "claude-x" },
            "byo-nokey": { "provider": "gemini", "model": "gem-x" }
        })
        .as_object()
        .unwrap()
        .clone()
    }

    async fn store_with_keys(ids: &[&str]) -> Arc<dyn CredentialStore> {
        let store: Arc<dyn CredentialStore> = Arc::new(InMemoryCredentialStore::new());
        for id in ids {
            store
                .set(&model_credential_key(id), "sk-test")
                .await
                .unwrap();
        }
        store
    }

    #[tokio::test]
    async fn custom_selection_resolves_to_its_entry_when_keyed() {
        let providers = providers();
        let store = store_with_keys(&["byo-opus", "byo-default"]).await;
        let tiers = serde_json::Map::new();
        let (id, entry) = select_model_provider(&providers, "custom:byo-opus", &tiers, &store)
            .await
            .expect("a keyed custom selection resolves");
        assert_eq!(id, "byo-opus");
        let provider = model_provider_init(&entry).unwrap();
        assert_eq!(provider["type"], serde_json::json!("anthropic"));
        assert_eq!(provider["model"], serde_json::json!("claude-x"));
    }

    /// The settings form asks for a base URL and people paste the endpoint their
    /// gateway documented, so the entry has to resolve to the API root the
    /// provider extends — otherwise the request path is appended twice.
    #[test]
    fn a_pasted_endpoint_url_resolves_to_the_api_root_the_provider_extends() {
        let init = |entry: Value| model_provider_init(&entry).unwrap()["baseUrl"].clone();

        assert_eq!(
            init(serde_json::json!({
                "provider": "openai", "model": "qwen3.8", "openaiApi": "chat",
                "baseUrl": "http://gateway.example/compatible-mode/v1/chat/completions"
            })),
            serde_json::json!("http://gateway.example/compatible-mode/v1")
        );
        assert_eq!(
            init(serde_json::json!({
                "provider": "openai", "model": "gpt-5", "openaiApi": "responses",
                "baseUrl": "https://gateway.example/v1/responses/"
            })),
            serde_json::json!("https://gateway.example/v1")
        );
        assert_eq!(
            init(serde_json::json!({
                "provider": "anthropic", "model": "claude-x",
                "baseUrl": "https://gateway.example/v1/messages"
            })),
            serde_json::json!("https://gateway.example")
        );
        // A root that already is one survives untouched, trailing slash aside.
        assert_eq!(
            init(serde_json::json!({
                "provider": "openai", "model": "gpt-4o", "baseUrl": "https://api.openai.com/v1/"
            })),
            serde_json::json!("https://api.openai.com/v1")
        );
        assert_eq!(
            init(serde_json::json!({
                "provider": "anthropic", "model": "kimi",
                "baseUrl": "https://api.moonshot.cn/anthropic"
            })),
            serde_json::json!("https://api.moonshot.cn/anthropic")
        );
    }

    /// A gateway in front of the Anthropic surface may validate the stored key
    /// on only one carrier. The entry names that carrier and the provider
    /// spec must carry it through, or the request goes out on the default
    /// header and the gateway rejects it.
    #[test]
    fn a_bearer_auth_scheme_entry_carries_the_scheme_into_the_provider_spec() {
        let scheme = |entry: Value| {
            model_provider_init(&entry)
                .unwrap()
                .get("apiKeyScheme")
                .cloned()
        };

        assert_eq!(
            scheme(serde_json::json!({
                "provider": "anthropic", "model": "claude-x", "authScheme": "bearer"
            })),
            Some(serde_json::json!("bearer"))
        );
        // Absent keeps the Anthropic-native carrier, so every existing entry
        // is unchanged.
        assert_eq!(
            scheme(serde_json::json!({
                "provider": "anthropic", "model": "claude-x"
            })),
            None
        );
        // The OpenAI surface is bearer-native already; the field must not
        // leak into its spec.
        assert_eq!(
            scheme(serde_json::json!({
                "provider": "openai", "model": "gpt-5", "authScheme": "bearer"
            })),
            None
        );
    }

    #[tokio::test]
    async fn tier_selection_maps_high_to_the_picked_entry() {
        let providers = providers();
        let store = store_with_keys(&["byo-opus", "byo-default"]).await;
        let tiers = serde_json::json!({ "high": "byo-opus" })
            .as_object()
            .unwrap()
            .clone();
        let (id, _) = select_model_provider(&providers, "high", &tiers, &store)
            .await
            .expect("high maps to the host-picked entry");
        assert_eq!(id, "byo-opus");
        // A tier with no host pick falls through to the default entry.
        let empty = serde_json::Map::new();
        let (id, _) = select_model_provider(&providers, "high", &empty, &store)
            .await
            .expect("an unmapped tier takes the default");
        assert_eq!(id, "byo-default");
    }

    #[tokio::test]
    async fn auto_takes_the_default_entry() {
        let providers = providers();
        let store = store_with_keys(&["byo-default"]).await;
        let tiers = serde_json::Map::new();
        let (id, entry) = select_model_provider(&providers, "auto", &tiers, &store)
            .await
            .expect("auto takes the default");
        assert_eq!(id, "byo-default");
        let provider = model_provider_init(&entry).unwrap();
        assert_eq!(provider["type"], serde_json::json!("openai"));
        assert_eq!(provider["api"], serde_json::json!("chat"));
    }

    /// A NAMED selection whose entry has no stored key is refused. It used to
    /// fall through to the keyed default, which ran the turn on a different model
    /// and a different account than the caller named, silently.
    #[tokio::test]
    async fn a_keyless_named_selection_is_refused_rather_than_substituted() {
        let providers = providers();
        let store = store_with_keys(&["byo-default"]).await;
        let error = select_model_provider(
            &providers,
            "custom:byo-nokey",
            &serde_json::Map::new(),
            &store,
        )
        .await
        .expect_err("a keyless named selection must not resolve to another entry");
        assert!(error.contains("byo-nokey"), "{error}");
        assert!(error.contains("no stored API key"), "{error}");
    }

    /// A selection naming an entry that does not exist is refused too: a typo must
    /// not spend a turn on whatever happens to be the default.
    #[tokio::test]
    async fn an_unknown_named_selection_is_refused() {
        let providers = providers();
        let store = store_with_keys(&["byo-default"]).await;
        let error = select_model_provider(
            &providers,
            "custom:no-such-entry",
            &serde_json::Map::new(),
            &store,
        )
        .await
        .expect_err("an unknown id must not resolve");
        assert!(error.contains("no-such-entry"), "{error}");
    }

    #[tokio::test]
    async fn no_stored_key_anywhere_is_an_error() {
        let providers = providers();
        let store = store_with_keys(&[]).await;
        let error = select_model_provider(&providers, "auto", &serde_json::Map::new(), &store)
            .await
            .expect_err("nothing keyed means nothing to run on");
        assert!(error.contains("no stored API key"), "{error}");
    }

    /// Tier derivation from the host's picks: only entries on the SAME wire as
    /// the selected model (provider kind + base URL + identical stored key)
    /// become tiers; each carries its declared capabilities onto the wire.
    #[tokio::test]
    async fn byo_tiers_follow_host_picks_on_the_same_wire() {
        let providers = serde_json::json!({
            "main": { "provider": "anthropic", "model": "claude-main", "default": true },
            "cheap": {
                "provider": "anthropic", "model": "claude-cheap",
                "capabilities": { "structuredOutput": false }
            },
            "other-wire": {
                "provider": "anthropic", "model": "claude-elsewhere",
                "baseUrl": "https://other.example.com"
            },
            "other-key": { "provider": "anthropic", "model": "claude-stranger" }
        })
        .as_object()
        .unwrap()
        .clone();
        let store: Arc<dyn CredentialStore> = Arc::new(InMemoryCredentialStore::new());
        for (id, key) in [("main", "sk-a"), ("cheap", "sk-a"), ("other-wire", "sk-a")] {
            store.set(&model_credential_key(id), key).await.unwrap();
        }
        // The high-tier entry holds a DIFFERENT key: its tier must not resolve
        // — the session has one provider, and that key would bill elsewhere.
        store
            .set(&model_credential_key("other-key"), "sk-b")
            .await
            .unwrap();
        let picks = serde_json::json!({
            "low": "cheap", "medium": "other-wire", "high": "other-key"
        })
        .as_object()
        .unwrap()
        .clone();

        let main_entry = providers.get("main").unwrap().clone();
        let (tiers, tier_capabilities) = byo_model_tiers(
            &providers,
            &picks,
            "main",
            "claude-main",
            &main_entry,
            &store,
        )
        .await;

        assert_eq!(
            tiers.get("low").and_then(Value::as_str),
            Some("claude-cheap")
        );
        assert!(tiers.get("medium").is_none(), "other base URL is out");
        assert!(tiers.get("high").is_none(), "a different key is out");
        assert_eq!(tier_capabilities.len(), 1);
        assert_eq!(
            tier_capabilities[0]["model"],
            serde_json::json!("claude-cheap")
        );
    }

    #[tokio::test]
    async fn complete_catalog_admits_unassigned_models_only_on_the_same_transport() {
        let providers = serde_json::json!({
            "main":{"provider":"openai","model":"main","openaiApi":"responses","baseUrl":"https://gateway.example/v1/responses"},
            "review":{"provider":"openai","model":"review","openaiApi":"responses","baseUrl":"https://GATEWAY.example:443/v1/","capabilities":{"requiresSandboxedReview":true}},
            "chat":{"provider":"openai","model":"chat","openaiApi":"chat","baseUrl":"https://gateway.example/v1"},
            "other-key":{"provider":"openai","model":"other-key","openaiApi":"responses","baseUrl":"https://gateway.example/v1"},
            "other-host":{"provider":"openai","model":"other-host","openaiApi":"responses","baseUrl":"https://other.example/v1"}
        }).as_object().unwrap().clone();
        let store: Arc<dyn CredentialStore> = Arc::new(InMemoryCredentialStore::new());
        for id in ["main", "review", "chat", "other-host"] {
            store
                .set(&model_credential_key(id), "same-key")
                .await
                .unwrap();
        }
        store
            .set(&model_credential_key("other-key"), "different-key")
            .await
            .unwrap();
        let (tiers, metadata) = byo_model_tiers(
            &providers,
            &serde_json::Map::new(),
            "main",
            "main",
            &providers["main"],
            &store,
        )
        .await;
        assert!(tiers.is_empty());
        assert_eq!(
            metadata,
            vec![
                serde_json::json!({"model":"review","capabilities":{"requiresSandboxedReview":true}})
            ]
        );
    }

    #[tokio::test]
    async fn byo_tiers_skip_the_main_entry_and_model() {
        let providers = serde_json::json!({
            "main": { "provider": "anthropic", "model": "claude-main", "default": true }
        })
        .as_object()
        .unwrap()
        .clone();
        let store = store_with_keys(&["main"]).await;
        let picks = serde_json::json!({ "low": "main" })
            .as_object()
            .unwrap()
            .clone();
        let main_entry = providers.get("main").unwrap().clone();
        let (tiers, tier_capabilities) = byo_model_tiers(
            &providers,
            &picks,
            "main",
            "claude-main",
            &main_entry,
            &store,
        )
        .await;
        // Picking the main entry for a tier adds nothing: the tier IS the main model.
        assert!(tiers.is_empty());
        assert!(tier_capabilities.is_empty());
    }

    #[tokio::test]
    async fn byo_tiers_preserve_media_metadata_and_bind_undeclared_models_independently() {
        let providers = serde_json::json!({
            "main": {
                "provider": "openai", "model": "main",
                "capabilities": {"imageLimits":{"maxWidth":2000},"maxMediaBlocks":600}
            },
            "declared": {
                "provider": "openai", "model": "declared",
                "capabilities": {
                    "imageLimits":{"maxHeight":1200,"maxBase64Size":10485760,"targetRawSize":512000},
                    "maxMediaBlocks":42
                }
            },
            "unknown": {"provider":"openai","model":"unknown","maxContextTokens":1000000}
        }).as_object().unwrap().clone();
        let store: Arc<dyn CredentialStore> = Arc::new(InMemoryCredentialStore::new());
        for id in ["main", "declared", "unknown"] {
            store
                .set(&model_credential_key(id), "fixture-key")
                .await
                .unwrap();
        }
        let picks = serde_json::json!({"low":"declared","medium":"unknown"})
            .as_object()
            .unwrap()
            .clone();
        let (_, capabilities) = byo_model_tiers(
            &providers,
            &picks,
            "main",
            "main",
            &providers["main"],
            &store,
        )
        .await;
        assert_eq!(capabilities.len(), 2);
        let declared = capabilities
            .iter()
            .find(|entry| entry["model"] == "declared")
            .unwrap();
        assert_eq!(
            declared["capabilities"],
            providers["declared"]["capabilities"]
        );
        let unknown = capabilities
            .iter()
            .find(|entry| entry["model"] == "unknown")
            .unwrap();
        assert_eq!(unknown["capabilities"], serde_json::json!({}));
    }
}

#[cfg(test)]
#[path = "../../tests/r20261018_runtime_content_wire.rs"]
mod runtime_content_wire_regressions;
