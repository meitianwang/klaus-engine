//! The client half of the facade.
//!
//! A host talks to the engine through [`AppServerClient`] and nothing else. That
//! is the whole point of it existing: before this, a host that wanted something
//! the facade had not declared could reach past the facade and post a raw inner
//! op, so the facade's method list stopped being the contract and became a
//! suggestion. There is no such door here — [`AppServerClient::request`] is
//! generic over [`Method`], so the only callable things are the rows of the
//! method table, and the compiler picks both the params type and the result type
//! from the row.
//!
//! Transport is generic on purpose. [`AppServerClient::in_process`] wires the
//! client to a server in the same process over an in-memory duplex, which is how
//! a CLI or a desktop shell that embeds the engine reaches it: no socket, no
//! token, no daemon. [`AppServerClient::new`] takes any byte streams, so the
//! same client speaks to a served engine over a WebSocket without a second
//! implementation. Anything above this layer cannot tell which one it got.
//!
//! Three kinds of traffic come back, and each has its own shape here because
//! each has different ownership:
//!   * replies, correlated by request id and awaited by the caller;
//!   * notifications, fanned out to every subscriber (the event stream);
//!   * server->client requests, which exactly one consumer must ANSWER — an
//!     approval the engine will not proceed without.
//!
//! The last one is why [`ReverseRequest`] answers itself when dropped. An
//! unanswered ask does not fail loudly; it parks the engine's turn until the
//! transport dies, which is the worst failure a host can hand a user. Dropping
//! one is therefore reported as a refusal, which the service translates into the
//! `deny` / `decline` the engine already defaults to.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures::channel::mpsc as fmpsc;
use futures::Stream;
use klaus_agent_core::runtime_values::RuntimeString;
use serde::Serialize;
#[cfg(test)]
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot, watch, Mutex as AsyncMutex};
use tokio::task::JoinHandle;

use super::methods::Method;
use super::runtime_wire::{RawJson, RuntimeValue};
use super::service::exact_payload::Payload;
use super::service::{run_app_server, AppServerOptions};
use super::wire;

/// Duplex ring size for the in-process transport. One JSON-RPC line has to fit
/// with room to spare (a history reply is the big one); past that, size buys
/// nothing because both ends are drained by their own task.
const IN_PROCESS_PIPE_CAPACITY: usize = 1 << 20;

/// Why a call did not produce its declared result.
#[derive(Debug, Clone, PartialEq)]
pub enum ClientError {
    /// The engine answered with a JSON-RPC error. `code` is one of the
    /// constants in [`super::wire`], so a caller can branch on a stale config
    /// version or an unknown session rather than only display the message.
    Rpc {
        code: i64,
        message: RuntimeString,
        data: Option<RawJson>,
    },
    /// The params did not encode. Unreachable for the declared types; kept
    /// rather than panicked because a host embedding this should not die on it.
    Encode(String),
    /// The reply did not decode as the method's declared result type, which
    /// means the two halves of this crate disagree about the method.
    Decode(String),
    /// The connection ended before the reply arrived.
    Closed,
}

fn deserialize_present_raw_json<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<RawJson>, D::Error> {
    <RawJson as serde::Deserialize<'de>>::deserialize(deserializer).map(Some)
}

impl ClientError {
    /// The JSON-RPC error code, when the failure came from the engine.
    pub fn code(&self) -> Option<i64> {
        match self {
            Self::Rpc { code, .. } => Some(*code),
            _ => None,
        }
    }

    fn from_error_object(error: &RawJson) -> Result<Self, Self> {
        #[derive(serde::Deserialize, schemars::JsonSchema)]
        struct RpcError {
            code: i64,
            #[serde(with = "super::runtime_wire::string")]
            #[schemars(with = "String")]
            message: RuntimeString,
            #[serde(default, deserialize_with = "deserialize_present_raw_json")]
            data: Option<RawJson>,
        }
        let error: RpcError = error
            .deserialize()
            .map_err(|error| Self::Decode(format!("JSON-RPC error: {error}")))?;
        Ok(Self::Rpc {
            code: error.code,
            message: error.message,
            data: error.data,
        })
    }
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rpc { code, message, .. } => {
                let display = message.to_node_utf8();
                write!(
                    f,
                    "{} (code {code})",
                    std::str::from_utf8(&display).expect("terminal string projection is UTF-8")
                )
            }
            Self::Encode(detail) => write!(f, "cannot encode the request: {detail}"),
            Self::Decode(detail) => write!(f, "cannot decode the reply: {detail}"),
            Self::Closed => write!(f, "the engine connection closed before the reply arrived"),
        }
    }
}

impl std::error::Error for ClientError {}

/// A server-initiated notification, method name plus params verbatim.
#[derive(Debug, Clone)]
pub struct Notification {
    pub method: String,
    pub params: RawJson,
    session_id: Option<String>,
}

impl Notification {
    /// The session a session-scoped notification belongs to.
    pub fn session_id(&self) -> Option<&str> {
        self.session_id.as_deref()
    }

    /// The raw inner-protocol envelope carried by `session/event` and
    /// `session/rawEvent`.
    ///
    /// Handed back unmodelled because it is not this layer's model: the
    /// envelope's schema belongs to the protocol crate, and a host that wants it
    /// typed decodes it with that crate's own codec. Restating the shape here
    /// would be a second definition of it, free to drift from the one the engine
    /// actually writes.
    pub fn event(&self) -> Result<Option<RuntimeValue>, ClientError> {
        match self.method.as_str() {
            "session/event" | "session/rawEvent" => self
                .params
                .get("event")
                .and_then(|event| event.map(|value| value.to_runtime()).transpose())
                .map_err(ClientError::Decode),
            _ => Ok(None),
        }
    }
}

/// A question the engine is blocked on.
///
/// Must be answered. See the module docs for why dropping one is a refusal
/// rather than silence.
pub struct ReverseRequest {
    pub method: String,
    pub params: RawJson,
    id: RawJson,
    session_id: Option<String>,
    submission_id: Option<String>,
    outgoing: mpsc::UnboundedSender<Payload>,
    answered: bool,
}

impl ReverseRequest {
    /// The session that is blocked.
    pub fn session_id(&self) -> Option<&str> {
        self.session_id.as_deref()
    }

    /// The submission the ask interrupted, so a host following one op can tell
    /// whether the question belongs to it.
    pub fn submission_id(&self) -> Option<&str> {
        self.submission_id.as_deref()
    }

    /// Answer it. `response` is the raw inner-protocol response payload
    /// (`ApprovalResponse` for `approval/request`, `ElicitResponseWire` for
    /// `elicitation/request`), wrapped in the declared result shape here so a
    /// caller cannot forget the wrapper.
    pub fn respond<T: Serialize>(mut self, response: T) {
        self.answered = true;
        let message = match RawJson::from_serialize(&response) {
            Ok(response) => Payload::Object(vec![
                ("id".into(), self.id.clone().into()),
                (
                    "result".into(),
                    Payload::Object(vec![("response".into(), response.into())]),
                ),
            ]),
            Err(error) => reverse_error(
                &self.id,
                &format!("cannot encode the host response: {error}"),
            ),
        };
        let _ = self.outgoing.send(message);
    }

    /// Refuse it with a reason. The engine applies its own safe default (deny a
    /// tool, decline a form), so this is the honest answer when the host has no
    /// terminal to ask at.
    pub fn refuse(mut self, reason: &str) {
        self.answered = true;
        let _ = self.outgoing.send(reverse_error(&self.id, reason));
    }
}

impl Drop for ReverseRequest {
    fn drop(&mut self) {
        if self.answered {
            return;
        }
        let _ = self.outgoing.send(reverse_error(
            &self.id,
            "the host dropped the request without answering it",
        ));
    }
}

fn reverse_error(id: &RawJson, reason: &str) -> Payload {
    Payload::Object(vec![
        ("id".into(), id.clone().into()),
        (
            "error".into(),
            serde_json::json!({ "code": wire::ENGINE_ERROR, "message": reason }).into(),
        ),
    ])
}

/// The notification stream handed out by [`AppServerClient::notifications`].
///
/// Unbounded and fanned out rather than a broadcast ring: a host that stops
/// polling for a moment (printing, prompting) must not silently lose events, and
/// a lost event in this protocol is a turn that never appears to finish.
pub struct Notifications {
    rx: fmpsc::UnboundedReceiver<Arc<Notification>>,
    shared: Arc<Shared>,
}

impl Notifications {
    /// The terminal transport or decode failure after this stream ends.
    pub fn terminal_error(&self) -> Option<ClientError> {
        self.shared.failure.lock().unwrap().clone()
    }
}

impl Stream for Notifications {
    type Item = Arc<Notification>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.rx).poll_next(cx)
    }
}

struct Shared {
    waiters: Mutex<HashMap<u64, PendingReply>>,
    subscribers: Mutex<Vec<fmpsc::UnboundedSender<Arc<Notification>>>>,
    ended: AtomicBool,
    failure: Mutex<Option<ClientError>>,
    end_signal: watch::Sender<bool>,
}

impl Default for Shared {
    fn default() -> Self {
        Self {
            waiters: Mutex::new(HashMap::new()),
            subscribers: Mutex::new(Vec::new()),
            ended: AtomicBool::new(false),
            failure: Mutex::new(None),
            end_signal: watch::channel(false).0,
        }
    }
}

impl Shared {
    /// Settle every waiter with the terminal failure before ending subscribers.
    fn end(&self, error: ClientError) {
        let mut failure = self.failure.lock().unwrap();
        if failure.is_some() {
            return;
        }
        *failure = Some(error.clone());
        self.ended.store(true, Ordering::Release);
        for (_, waiter) in self.waiters.lock().unwrap().drain() {
            let _ = waiter.tx.send(Err(error.clone()));
        }
        self.subscribers.lock().unwrap().clear();
        self.end_signal.send_replace(true);
    }

    fn error(&self) -> ClientError {
        self.failure
            .lock()
            .unwrap()
            .clone()
            .unwrap_or(ClientError::Closed)
    }
}

/// A connected facade client.
pub struct AppServerClient {
    outgoing: mpsc::UnboundedSender<Payload>,
    shared: Arc<Shared>,
    /// Server->client requests, single-consumer by construction: they are
    /// answered, not observed, so fanning them out would mean several hosts
    /// answering one question.
    reverse: AsyncMutex<mpsc::UnboundedReceiver<ReverseRequest>>,
    next_id: AtomicU64,
    reader: Mutex<Option<JoinHandle<()>>>,
}

impl AppServerClient {
    /// Speak the facade over any byte streams.
    pub fn new<I, O>(input: I, output: O) -> Self
    where
        I: AsyncRead + Unpin + Send + 'static,
        O: AsyncWrite + Unpin + Send + 'static,
    {
        let shared = Arc::new(Shared::default());
        let writer_shared = shared.clone();
        let mut writer_end = shared.end_signal.subscribe();
        let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Payload>();
        tokio::spawn(async move {
            let mut output = output;
            loop {
                if *writer_end.borrow() {
                    return;
                }
                let message = tokio::select! {
                    biased;
                    _ = writer_end.changed() => return,
                    message = out_rx.recv() => match message {
                        Some(message) => message,
                        None => return,
                    },
                };
                let mut line = match message.bytes() {
                    Ok(line) => line,
                    Err(error) => {
                        writer_shared.end(ClientError::Encode(error.to_string()));
                        return;
                    }
                };
                line.push(b'\n');
                let published = tokio::select! {
                    biased;
                    _ = writer_end.changed() => return,
                    result = async {
                        output.write_all(&line).await?;
                        output.flush().await
                    } => result,
                };
                if published.is_err() {
                    writer_shared.end(ClientError::Closed);
                    return;
                }
            }
        });

        let (reverse_tx, reverse_rx) = mpsc::unbounded_channel();
        let reader = tokio::spawn(read_messages(
            input,
            shared.clone(),
            out_tx.clone(),
            reverse_tx,
        ));

        Self {
            outgoing: out_tx,
            shared,
            reverse: AsyncMutex::new(reverse_rx),
            next_id: AtomicU64::new(1),
            reader: Mutex::new(Some(reader)),
        }
    }

    /// Assemble a facade server in this process and connect to it.
    ///
    /// The returned handle finishes once the client is dropped and the server has
    /// torn down every session it created, so a host that wants a clean exit
    /// drops the client and awaits it.
    pub fn in_process(opts: AppServerOptions) -> (Self, JoinHandle<()>) {
        let (client_side, server_side) = tokio::io::duplex(IN_PROCESS_PIPE_CAPACITY);
        let (server_read, server_write) = tokio::io::split(server_side);
        let (client_read, client_write) = tokio::io::split(client_side);
        let server = tokio::spawn(run_app_server(server_read, server_write, opts));
        (Self::new(client_read, client_write), server)
    }

    /// Call one declared method.
    ///
    /// `M` binds the name, the params and the result together, so this is the
    /// only call shape a host needs and the only one it can misuse by naming a
    /// method that does not exist.
    pub async fn request<M: Method>(&self, params: M::Params) -> Result<M::Result, ClientError> {
        let params = RawJson::from_serialize(&params)
            .map_err(|error| ClientError::Encode(error.to_string()))?;
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.shared.waiters.lock().unwrap().insert(
            id,
            PendingReply {
                tx,
                validate: validate_result::<M>,
            },
        );
        let pending = PendingRequest {
            shared: &self.shared,
            id,
        };
        // Parked BEFORE the end flag is read: a reader that stops concurrently
        // either settles this waiter on its way out or is observed here. The
        // other order leaves the call awaiting a reply nobody will ever send.
        if self.shared.ended.load(Ordering::Acquire)
            || self
                .outgoing
                .send(Payload::Object(vec![
                    ("id".into(), serde_json::Value::from(id).into()),
                    ("method".into(), serde_json::Value::from(M::NAME).into()),
                    ("params".into(), params.into()),
                ]))
                .is_err()
        {
            self.shared.waiters.lock().unwrap().remove(&id);
            return Err(self.shared.error());
        }
        let result = match rx.await {
            Ok(Ok(result)) => result
                .deserialize()
                .map_err(|error| ClientError::Decode(format!("{}: {error}", M::NAME))),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(self.shared.error()),
        };
        drop(pending);
        result
    }

    /// Subscribe to notifications.
    ///
    /// Subscribe BEFORE sending the request whose events you intend to follow.
    /// A submission id tags a stream, not a value, and the engine may emit the
    /// first event of it before the reply to the call that started it.
    pub fn notifications(&self) -> Notifications {
        let (tx, rx) = fmpsc::unbounded();
        let mut subscribers = self.shared.subscribers.lock().unwrap();
        if !self.shared.ended.load(Ordering::Acquire) {
            subscribers.push(tx);
        }
        // An already-ended connection drops `tx` here, so the stream ends
        // immediately instead of never yielding.
        Notifications {
            rx,
            shared: self.shared.clone(),
        }
    }

    /// Take the next question the engine is blocked on. `None` once the
    /// connection has ended.
    ///
    /// Cancel-safe, so it can sit in a `select!` beside the notification stream:
    /// a question that arrives while another arm wins stays queued.
    pub async fn next_server_request(&self) -> Option<ReverseRequest> {
        self.reverse.lock().await.recv().await
    }

    /// Whether the connection has ended.
    pub fn is_closed(&self) -> bool {
        self.shared.ended.load(Ordering::Acquire)
    }
}

impl Drop for AppServerClient {
    fn drop(&mut self) {
        // The reader holds a sender for the write side, because that is how it
        // answers a dropped reverse request. So it has to be stopped
        // explicitly: left alive it keeps the writer's channel open, the write
        // half never closes, and an in-process server never sees the EOF that
        // ends it.
        if let Some(reader) = self.reader.lock().unwrap().take() {
            reader.abort();
        }
        self.shared.end(ClientError::Closed);
    }
}

struct PendingRequest<'a> {
    shared: &'a Shared,
    id: u64,
}

struct PendingReply {
    tx: oneshot::Sender<Result<RawJson, ClientError>>,
    validate: fn(&RawJson) -> Result<(), ClientError>,
}

fn validate_result<M: Method>(result: &RawJson) -> Result<(), ClientError> {
    result
        .deserialize::<M::Result>()
        .map(|_| ())
        .map_err(|error| ClientError::Decode(format!("{}: {error}", M::NAME)))
}

impl Drop for PendingRequest<'_> {
    fn drop(&mut self) {
        self.shared.waiters.lock().unwrap().remove(&self.id);
    }
}

async fn read_messages<I>(
    input: I,
    shared: Arc<Shared>,
    outgoing: mpsc::UnboundedSender<Payload>,
    reverse: mpsc::UnboundedSender<ReverseRequest>,
) where
    I: AsyncRead + Unpin + Send + 'static,
{
    let mut lines = BufReader::new(input).lines();
    let mut ended = shared.end_signal.subscribe();
    loop {
        if *ended.borrow() {
            return;
        }
        let next = tokio::select! {
            biased;
            _ = ended.changed() => return,
            next = lines.next_line() => next,
        };
        let line = match next {
            Ok(Some(line)) => line,
            Ok(None) => break,
            Err(error) => {
                shared.end(if error.kind() == std::io::ErrorKind::InvalidData {
                    ClientError::Decode(format!("JSON-RPC frame: {error}"))
                } else {
                    ClientError::Closed
                });
                return;
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        let message = match decode_message(line) {
            Ok(message) => message,
            Err(error) => {
                shared.end(error);
                return;
            }
        };
        match message {
            ClientMessage::Response { id, result } => {
                let waiter = shared.waiters.lock().unwrap().remove(&id);
                if let Some(waiter) = waiter {
                    // Validate before reading the next frame. An immediate EOF
                    // must not turn a malformed result into a generic close.
                    if let Ok(result) = &result {
                        if let Err(error) = (waiter.validate)(result) {
                            shared.end(error.clone());
                            let _ = waiter.tx.send(Err(error));
                            return;
                        }
                    }
                    let _ = waiter.tx.send(result);
                }
            }
            ClientMessage::Notification(notification) => {
                let notification = Arc::new(notification);
                shared
                    .subscribers
                    .lock()
                    .unwrap()
                    .retain(|subscriber| subscriber.unbounded_send(notification.clone()).is_ok());
            }
            ClientMessage::Request {
                id,
                method,
                params,
                session_id,
                submission_id,
            } => {
                // A send failure returns the request, whose `Drop` refuses it.
                // That is the safety net for a host that stopped listening: the
                // engine gets an answer either way.
                let _ = reverse.send(ReverseRequest {
                    method,
                    params,
                    id,
                    session_id,
                    submission_id,
                    outgoing: outgoing.clone(),
                    answered: false,
                });
            }
        }
    }
    shared.end(ClientError::Closed);
}

enum ClientMessage {
    Response {
        id: u64,
        result: Result<RawJson, ClientError>,
    },
    Notification(Notification),
    Request {
        id: RawJson,
        method: String,
        params: RawJson,
        session_id: Option<String>,
        submission_id: Option<String>,
    },
}

/// Decode only the envelope and typed controls; opaque subtrees remain raw.
fn decode_message(line: String) -> Result<ClientMessage, ClientError> {
    let raw = RawJson::from_json(line).map_err(|error| ClientError::Decode(error.to_string()))?;
    let fields = raw.object_fields().map_err(ClientError::Decode)?;
    let get = |name: &str| {
        fields
            .iter()
            .find_map(|(key, value)| (key == name).then_some(value))
    };
    let method = get("method")
        .map(|value| value.deserialize::<String>())
        .transpose()
        .map_err(|error| ClientError::Decode(format!("JSON-RPC method: {error}")))?;
    let id = get("id");
    if let Some(method) = method {
        if get("result").is_some() || get("error").is_some() {
            return Err(ClientError::Decode(
                "a method frame cannot also be a response".into(),
            ));
        }
        let params = get("params").cloned().unwrap_or_default();
        let declared_notification =
            id.is_none() && validate_notification_payload(&method, &params)?;
        let declared_reverse =
            id.is_some() && matches!(method.as_str(), "approval/request" | "elicitation/request");
        let (session_id, submission_id) = if declared_notification || declared_reverse {
            validate_notification_controls(&params)?
        } else {
            // Unknown methods own their parameter shapes. Optional routing
            // helpers expose scalar strings when present without validating or
            // changing the opaque payload delivered to the consumer.
            (
                params.control_string("sessionId").ok().flatten(),
                params.control_string("submissionId").ok().flatten(),
            )
        };
        return match id {
            Some(id) => {
                id.deserialize::<u64>().map_err(|error| {
                    ClientError::Decode(format!("JSON-RPC request id: {error}"))
                })?;
                Ok(ClientMessage::Request {
                    id: id.clone(),
                    method,
                    params,
                    session_id,
                    submission_id,
                })
            }
            None => Ok(ClientMessage::Notification(Notification {
                method,
                params,
                session_id,
            })),
        };
    }
    let id = id
        .ok_or_else(|| ClientError::Decode("JSON-RPC response is missing an id".into()))?
        .deserialize::<u64>()
        .map_err(|error| ClientError::Decode(format!("JSON-RPC response id: {error}")))?;
    let result = match (get("result"), get("error")) {
        (Some(result), None) => Ok(result.clone()),
        (None, Some(error)) => Err(ClientError::from_error_object(error)?),
        _ => {
            return Err(ClientError::Decode(
                "JSON-RPC response requires exactly one result or error".into(),
            ))
        }
    };
    Ok(ClientMessage::Response { id, result })
}

fn validate_notification_controls(
    params: &RawJson,
) -> Result<(Option<String>, Option<String>), ClientError> {
    // JSON-RPC permits positional params as well. Named notifications expose
    // these shared controls only when their params are an object.
    if !params.as_str().trim_start().starts_with('{') {
        return Ok((None, None));
    }
    let mut session_id = None;
    let mut submission_id = None;
    for (name, value) in params.object_fields().map_err(ClientError::Decode)? {
        match name.as_str() {
            "sessionId" | "submissionId" | "turnId" | "streamId" | "threadId" | "requestId" => {
                let value = value
                    .deserialize::<Option<String>>()
                    .map_err(|error| ClientError::Decode(format!("{name}: {error}")))?;
                match name.as_str() {
                    "sessionId" => session_id = value,
                    "submissionId" => submission_id = value,
                    _ => {}
                }
            }
            "streamRevision" => {
                value
                    .deserialize::<Option<u64>>()
                    .map_err(|error| ClientError::Decode(format!("{name}: {error}")))?;
            }
            _ => {}
        }
    }
    Ok((session_id, submission_id))
}

fn validate_notification_payload(method: &str, params: &RawJson) -> Result<bool, ClientError> {
    fn typed<T: serde::de::DeserializeOwned>(params: &RawJson) -> Result<(), serde_json::Error> {
        params.deserialize::<T>().map(|_| ())
    }
    let valid = match method {
        "session/event" | "session/rawEvent" => typed::<wire::SessionEventNotification>(params),
        "autoApprovalReview/strictReviewRequired" => {
            typed::<wire::ApprovalReviewStrictRequiredNotification>(params)
        }
        "approvalReview/warning" => typed::<wire::ApprovalReviewWarningNotification>(params),
        "item/autoApprovalReview/started" => {
            typed::<wire::ApprovalReviewStartedNotification>(params)
        }
        "item/autoApprovalReview/completed" => {
            typed::<wire::ApprovalReviewCompletedNotification>(params)
        }
        "question/requested" => typed::<wire::QuestionRequestedNotification>(params),
        "question/resolved" => typed::<wire::QuestionResolvedNotification>(params),
        "taskList/changed" => typed::<wire::TaskListChangedNotification>(params),
        "backgroundProcess/changed" => typed::<wire::BackgroundProcessChangedNotification>(params),
        "goal.continuation" => typed::<wire::GoalContinuationNotification>(params),
        "turn/completed" => typed::<wire::TurnCompletedNotification>(params),
        "session/closed" => typed::<wire::SessionClosedNotification>(params),
        "session/statusChanged" => typed::<wire::SessionStatusChangedNotification>(params),
        "thread/deleted" => typed::<wire::ThreadDeletedNotification>(params),
        "thread/started" => typed::<wire::ThreadStartedNotification>(params),
        "turn/started" | "turn/failed" => typed::<wire::TurnLifecycleNotification>(params),
        "item/started" | "item/updated" | "item/completed" => {
            typed::<wire::ItemNotification>(params)
        }
        "item/agentMessage/delta" | "item/reasoning/delta" => {
            typed::<wire::DeltaNotification>(params)
        }
        // Undeclared notification payloads remain opaque for forward compatibility.
        _ => return Ok(false),
    };
    valid
        .map(|()| true)
        .map_err(|error| ClientError::Decode(format!("{method}: {error}")))
}

#[cfg(test)]
#[path = "../../tests/r20261018_exact_sdk_transport.rs"]
mod r20261018_exact_sdk_transport;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_server::methods;
    use futures::StreamExt;
    use tokio::io::{AsyncBufReadExt, BufReader};

    /// A stand-in server: reads one line at a time and answers with whatever the
    /// test scripted. Deliberately not the real service — these tests are about
    /// the client's correlation, fan-out and refusal behaviour, and pulling a
    /// real engine in would test something else and skip when it is unavailable.
    fn scripted(replies: Vec<Value>) -> (AppServerClient, tokio::task::JoinHandle<Vec<Value>>) {
        let (client_side, server_side) = tokio::io::duplex(64 * 1024);
        let (server_read, mut server_write) = tokio::io::split(server_side);
        let (client_read, client_write) = tokio::io::split(client_side);
        let server = tokio::spawn(async move {
            let mut received = Vec::new();
            let mut lines = BufReader::new(server_read).lines();
            let mut replies = replies.into_iter();
            while let Ok(Some(line)) = lines.next_line().await {
                let Ok(message) = serde_json::from_str::<Value>(&line) else {
                    continue;
                };
                received.push(message.clone());
                let Some(mut reply) = replies.next() else {
                    continue;
                };
                // Echo the request id so the reply correlates.
                if let Some(id) = message.get("id") {
                    if reply.get("id").is_none() {
                        reply["id"] = id.clone();
                    }
                }
                let mut line = serde_json::to_vec(&reply).unwrap();
                line.push(b'\n');
                if server_write.write_all(&line).await.is_err() {
                    break;
                }
            }
            received
        });
        (AppServerClient::new(client_read, client_write), server)
    }

    #[tokio::test]
    async fn a_reply_decodes_as_the_declared_result() {
        let (client, _server) = scripted(vec![serde_json::json!({
            "result": { "userAgent": "klaus/test", "protocolVersion": 1 }
        })]);
        let result = client
            .request::<methods::Initialize>(Default::default())
            .await
            .expect("the scripted reply is a valid InitializeResult");
        assert_eq!(result.user_agent, "klaus/test");
        assert_eq!(result.protocol_version, 1);
    }

    #[tokio::test]
    async fn the_method_name_on_the_wire_comes_from_the_table() {
        let (client, server) = scripted(vec![serde_json::json!({
            "result": { "userAgent": "klaus/test", "protocolVersion": 1 }
        })]);
        let _ = client
            .request::<methods::Initialize>(Default::default())
            .await;
        drop(client);
        let received = server.await.unwrap();
        assert_eq!(received[0]["method"], serde_json::json!("initialize"));
    }

    #[tokio::test]
    async fn an_error_reply_keeps_its_code() {
        let (client, _server) = scripted(vec![serde_json::json!({
            "error": { "code": wire::UNKNOWN_SESSION, "message": "no such session" }
        })]);
        let error = client
            .request::<methods::Initialize>(Default::default())
            .await
            .expect_err("an error reply is not a result");
        assert_eq!(error.code(), Some(wire::UNKNOWN_SESSION));
    }

    #[tokio::test]
    async fn a_closed_connection_ends_the_call_instead_of_hanging() {
        let (client_side, server_side) = tokio::io::duplex(64 * 1024);
        let (client_read, client_write) = tokio::io::split(client_side);
        // No server at all, which is the shape of a crashed engine: the call has
        // to end on the EOF rather than wait for a reply nobody will send.
        drop(server_side);
        let client = AppServerClient::new(client_read, client_write);
        let error = client
            .request::<methods::Initialize>(Default::default())
            .await
            .expect_err("nothing answered");
        assert_eq!(error, ClientError::Closed);
    }

    #[tokio::test]
    async fn notifications_fan_out_to_every_subscriber() {
        let (client_side, server_side) = tokio::io::duplex(64 * 1024);
        let (server_read, mut server_write) = tokio::io::split(server_side);
        let (client_read, client_write) = tokio::io::split(client_side);
        drop(server_read);
        let client = AppServerClient::new(client_read, client_write);
        let mut first = client.notifications();
        let mut second = client.notifications();
        let line = b"{\"method\":\"session/event\",\"params\":{\"sessionId\":\"s1\",\"event\":{\"submissionId\":\"sub_1\"}}}\n";
        server_write.write_all(line).await.unwrap();

        for stream in [&mut first, &mut second] {
            let notification = stream.next().await.expect("one notification each");
            assert_eq!(notification.session_id(), Some("s1"));
            assert_eq!(
                notification
                    .event()
                    .expect("valid event")
                    .and_then(|event| event.get("submissionId").cloned())
                    .and_then(|value| value.as_string().cloned()),
                Some(RuntimeString::from("sub_1"))
            );
        }
    }

    #[tokio::test]
    async fn a_dropped_reverse_request_refuses_instead_of_parking_the_engine() {
        let (client_side, server_side) = tokio::io::duplex(64 * 1024);
        let (server_read, mut server_write) = tokio::io::split(server_side);
        let (client_read, client_write) = tokio::io::split(client_side);
        let client = AppServerClient::new(client_read, client_write);
        let mut lines = BufReader::new(server_read).lines();
        server_write
            .write_all(
                b"{\"id\":7,\"method\":\"approval/request\",\"params\":{\"sessionId\":\"s1\"}}\n",
            )
            .await
            .unwrap();

        let ask = client
            .next_server_request()
            .await
            .expect("the reverse request arrives");
        assert_eq!(ask.method, "approval/request");
        drop(ask);

        let line = lines.next_line().await.unwrap().expect("an answer");
        let answer: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(answer["id"], serde_json::json!(7));
        assert_eq!(
            answer["error"]["code"],
            serde_json::json!(wire::ENGINE_ERROR)
        );
    }

    #[tokio::test]
    async fn an_answered_reverse_request_carries_the_response_wrapper() {
        let (client_side, server_side) = tokio::io::duplex(64 * 1024);
        let (server_read, mut server_write) = tokio::io::split(server_side);
        let (client_read, client_write) = tokio::io::split(client_side);
        let client = AppServerClient::new(client_read, client_write);
        let mut lines = BufReader::new(server_read).lines();
        server_write
            .write_all(b"{\"id\":9,\"method\":\"approval/request\",\"params\":{\"sessionId\":\"s1\",\"submissionId\":\"sub_2\"}}\n")
            .await
            .unwrap();

        let ask = client.next_server_request().await.expect("the ask arrives");
        assert_eq!(ask.submission_id(), Some("sub_2"));
        ask.respond(serde_json::json!({ "decision": "allow" }));

        let line = lines.next_line().await.unwrap().expect("an answer");
        let answer: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(
            answer["result"]["response"]["decision"],
            serde_json::json!("allow")
        );
    }
}

#[cfg(test)]
mod approval_review_notification_tests {
    use super::*;

    #[test]
    fn review_notification_validation_keeps_runtime_content_exact() {
        let params = RawJson::from_json(r#"{"sessionId":"session-1","turnId":"turn-1","startedAtMs":1,"completedAtMs":2,"reviewId":"review-1","targetItemId":null,"decisionSource":"agent","review":{"status":"denied","riskLevel":"high","userAuthorization":"low","rationale":"\ud800"},"action":{"type":"command","source":"shell","command":"echo \udfff","cwd":"/tmp"}}"#.into()).unwrap();
        assert!(
            validate_notification_payload("item/autoApprovalReview/completed", &params).unwrap()
        );
        let runtime = params.to_runtime().unwrap().0;
        assert_eq!(
            runtime
                .get("review")
                .unwrap()
                .get("rationale")
                .unwrap()
                .as_string()
                .unwrap()
                .as_utf16(),
            &[0xd800]
        );
        let warning = RawJson::from_json(
            r#"{"sessionId":"session-1","message":"\udfff","kind":"tooManyDenials"}"#.into(),
        )
        .unwrap();
        assert!(validate_notification_payload("approvalReview/warning", &warning).unwrap());
    }
}
