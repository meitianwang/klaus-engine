// Copyright 2026 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified: integrates scoped connection ownership and streaming with the local provider API.

//! Keeps connection establishment outside the classification path.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{SinkExt, Stream, StreamExt};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, oneshot, watch, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::handshake::{client::generate_key, derive_accept_key};
use tokio_tungstenite::tungstenite::protocol::{Message, Role};
use tokio_tungstenite::WebSocketStream;
use tokio_util::task::TaskTracker;

use super::{
    ApprovalClassifierTransport, ClassifierAttribution, ClassifierAuthRecovery, ClassifierRequest,
    ClassifierStream,
};
use crate::provider::auth::CredentialChangeState;
use crate::provider::interceptor::{report_attempt, ProviderRequestInfo, SharedRequestInterceptor};
use crate::runtime_values::{runtime_json, RuntimeJson, RuntimeString};
use crate::types::abort::AbortSignal;
use crate::types::errors::{ProviderError, ProviderErrorKind};

const INITIAL_CONNECTIONS: usize = if cfg!(test) { 2 } else { 8 };
const MAX_CONCURRENT_REQUESTS: usize = 16;
const MAX_CONCURRENT_OPENS: usize = 4;
const REFILL_LOW_WATERMARK: usize = INITIAL_CONNECTIONS / 2;
const CONNECT_COOLDOWN: Duration = Duration::from_secs(5 * 60);
const MAX_WEBSOCKET_AGE: Duration = Duration::from_secs(55 * 60);
const INTERRUPT_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_SSE_EVENT_BYTES: usize = 16 * 1024 * 1024;
const MAX_BUFFERED_WEBSOCKET_BYTES: usize = 8 * 1024 * 1024;

pub(crate) type RawClassifierStream =
    Pin<Box<dyn Stream<Item = Result<RuntimeJson, ProviderError>> + Send>>;

pub(crate) struct ClassifierEndpoint {
    pub url: String,
    pub headers: HashMap<String, String>,
    pub client: reqwest::Client,
    pub websocket_client: reqwest::Client,
    pub auth_changes: Option<watch::Receiver<CredentialChangeState>>,
    pub auth_state: Option<CredentialChangeState>,
    pub classifier_route: bool,
    pub connect_timeout: Duration,
    pub idle_timeout: Duration,
    pub attempt: Option<Arc<ClassifierAttempt>>,
}

pub(crate) struct ClassifierAttempt {
    interceptor: SharedRequestInterceptor,
    url: String,
    started: std::time::Instant,
    settled: AtomicBool,
}

impl ClassifierAttempt {
    pub(crate) fn new(interceptor: SharedRequestInterceptor, url: String) -> Self {
        Self {
            interceptor,
            url,
            started: std::time::Instant::now(),
            settled: AtomicBool::new(false),
        }
    }

    pub(crate) fn settle(&self, ok: bool, status: Option<u16>, error: Option<&str>) {
        if self.settled.swap(true, Ordering::AcqRel) {
            return;
        }
        report_attempt(
            Some(&self.interceptor),
            &ProviderRequestInfo {
                provider_id: "openai",
                model: "gpt-5.6-luna",
                url: &self.url,
                attempt: 1,
            },
            self.started,
            ok,
            status,
            error,
        );
    }
}

impl Drop for ClassifierAttempt {
    fn drop(&mut self) {
        self.settle(
            false,
            None,
            Some("classifier request did not open a stream"),
        );
    }
}

#[async_trait::async_trait]
pub(crate) trait ClassifierConnector: Send + Sync {
    /// Resolve the effective route and auth together for every lease and open.
    async fn resolve(&self, signal: &AbortSignal) -> Result<ClassifierEndpoint, ProviderError>;
    async fn auth_recovery(&self, signal: &AbortSignal) -> Option<Box<dyn ClassifierAuthRecovery>>;
    fn prepare(
        &self,
        request: &ClassifierRequest,
        thread_id: &str,
        endpoint: &ClassifierEndpoint,
    ) -> Result<RuntimeJson, ProviderError>;
    fn map_stream(
        &self,
        request: ClassifierRequest,
        stream: RawClassifierStream,
    ) -> ClassifierStream;
}

pub(crate) struct ResponsesClassifierTransport {
    pool: Arc<ConnectionPool>,
}

impl ResponsesClassifierTransport {
    pub(crate) fn new(
        connector: Arc<dyn ClassifierConnector>,
        attribution: ClassifierAttribution,
    ) -> Self {
        Self {
            pool: Arc::new(ConnectionPool {
                connector,
                attribution,
                idle: Mutex::new(Vec::new()),
                classifications: Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS)),
                sockets: Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS)),
                replenishing: Arc::new(tokio::sync::Mutex::new(())),
                retry_after: Mutex::new(None),
                shutdown: AbortSignal::new(),
                tasks: TaskTracker::new(),
                closed: Mutex::new(false),
            }),
        }
    }
}

#[async_trait::async_trait]
impl ApprovalClassifierTransport for ResponsesClassifierTransport {
    async fn auth_recovery(&self, signal: &AbortSignal) -> Option<Box<dyn ClassifierAuthRecovery>> {
        let inner = tokio::select! {
            _ = self.pool.shutdown.cancelled() => return None,
            _ = signal.cancelled() => return None,
            recovery = self.pool.connector.auth_recovery(signal) => recovery?,
        };
        Some(Box::new(PoolAuthRecovery {
            inner,
            pool: Arc::clone(&self.pool),
        }))
    }
    async fn prewarm(&self) {
        let guard = tokio::select! {
            _ = self.pool.shutdown.cancelled() => return,
            guard = Arc::clone(&self.pool.replenishing).lock_owned() => guard,
        };
        if let Some(refill) = self.pool.start_refill(guard) {
            let _ = refill.await;
        }
    }

    fn stream(&self, request: ClassifierRequest, signal: AbortSignal) -> ClassifierStream {
        let pool = Arc::clone(&self.pool);
        let connector = Arc::clone(&pool.connector);
        let raw_request = request.clone();
        let raw = async_stream::stream! {
            let (tx, mut rx) = mpsc::channel(32);
            let task_pool = Arc::clone(&pool);
            let registered = {
                let closed = pool.closed.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                if *closed {
                    false
                } else {
                    pool.tasks.spawn(async move {
                        task_pool.run_request(raw_request, signal, tx).await;
                    });
                    true
                }
            };
            if !registered {
                yield Err(cancelled_error());
                return;
            }
            while let Some(event) = rx.recv().await {
                yield event;
            }
        };
        connector.map_stream(request, Box::pin(raw))
    }

    async fn shutdown(&self) {
        *self
            .pool
            .closed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.pool.shutdown.cancel();
        self.pool.classifications.close();
        self.pool.sockets.close();
        self.pool
            .idle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        self.pool.tasks.close();
        self.pool.tasks.wait().await;
    }
}

impl Drop for ResponsesClassifierTransport {
    fn drop(&mut self) {
        *self
            .pool
            .closed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.pool.shutdown.cancel();
        self.pool.classifications.close();
        self.pool.sockets.close();
        self.pool
            .idle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }
}

struct ConnectionPool {
    connector: Arc<dyn ClassifierConnector>,
    attribution: ClassifierAttribution,
    idle: Mutex<Vec<PooledConnection>>,
    classifications: Arc<Semaphore>,
    sockets: Arc<Semaphore>,
    replenishing: Arc<tokio::sync::Mutex<()>>,
    retry_after: Mutex<Option<Instant>>,
    shutdown: AbortSignal,
    tasks: TaskTracker,
    closed: Mutex<bool>,
}

struct PoolAuthRecovery {
    inner: Box<dyn ClassifierAuthRecovery>,
    pool: Arc<ConnectionPool>,
}

#[async_trait::async_trait]
impl ClassifierAuthRecovery for PoolAuthRecovery {
    async fn next(&mut self) -> bool {
        let recovered = tokio::select! {
            _ = self.pool.shutdown.cancelled() => return false,
            recovered = self.inner.next() => recovered,
        };
        if recovered {
            self.pool
                .idle
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clear();
        }
        recovered
    }
}

#[derive(PartialEq, Eq)]
struct ConnectionKey {
    url: String,
    headers_hash: [u8; 32],
    revision: Option<u64>,
    classifier_route: bool,
}

impl ClassifierEndpoint {
    fn key(&self) -> ConnectionKey {
        let mut headers: Vec<_> = self.headers.iter().collect();
        headers.sort_by(|left, right| left.0.cmp(right.0));
        let mut hash = Sha256::new();
        for (name, value) in headers {
            hash.update(name.to_ascii_lowercase().as_bytes());
            hash.update([0]);
            hash.update(value.as_bytes());
            hash.update([0]);
        }
        ConnectionKey {
            url: self.url.clone(),
            headers_hash: hash.finalize().into(),
            revision: self.auth_state.map(|state| state.revision),
            classifier_route: self.classifier_route,
        }
    }
}

struct PooledConnection {
    socket: SocketPump,
    key: ConnectionKey,
    thread_id: String,
    expires_at: Instant,
    auth_changes: Option<watch::Receiver<CredentialChangeState>>,
    _permit: OwnedSemaphorePermit,
}

impl PooledConnection {
    fn healthy(&self, key: &ConnectionKey) -> bool {
        self.key == *key
            && Instant::now() < self.expires_at
            && self
                .auth_changes
                .as_ref()
                .is_none_or(|changes| !changes.has_changed().unwrap_or(true))
            && !self.socket.task.is_finished()
    }
}

enum SocketCommand {
    Send {
        message: Message,
        result: oneshot::Sender<Result<(), tokio_tungstenite::tungstenite::Error>>,
    },
}

struct SocketMessage {
    message: Result<Message, tokio_tungstenite::tungstenite::Error>,
    permit: Option<OwnedSemaphorePermit>,
    bytes: usize,
    terminal: bool,
}

/// A continuously polled connection keeps idle ping handling and closure detection live.
struct SocketPump {
    commands: mpsc::Sender<SocketCommand>,
    messages: mpsc::Receiver<SocketMessage>,
    task: tokio::task::JoinHandle<()>,
}

impl SocketPump {
    fn new(mut socket: WebSocketStream<reqwest::Upgraded>, tasks: &TaskTracker) -> Self {
        let (commands, mut command_rx) = mpsc::channel::<SocketCommand>(32);
        let (message_tx, messages) = mpsc::channel::<SocketMessage>(32);
        let buffer = Arc::new(Semaphore::new(MAX_BUFFERED_WEBSOCKET_BYTES));
        let task = tasks.spawn(async move {
            let mut pending: Option<SocketMessage> = None;
            loop {
                let needs_permit = pending.as_ref().is_some_and(|message| message.permit.is_none());
                let ready = pending.as_ref().is_some_and(|message| message.permit.is_some());
                let bytes = pending.as_ref().map_or(0, |message| message.bytes as u32);
                tokio::select! {
                    command = command_rx.recv() => {
                        let Some(SocketCommand::Send { message, result }) = command else { break; };
                        let sent = socket.send(message).await;
                        let failed = sent.is_err();
                        let _ = result.send(sent);
                        if failed { break; }
                    },
                    permit = Arc::clone(&buffer).acquire_many_owned(bytes), if needs_permit => {
                        let Ok(permit) = permit else { break; };
                        pending.as_mut().expect("pending socket frame").permit = Some(permit);
                    },
                    permit = message_tx.reserve(), if ready => {
                        let Ok(permit) = permit else { break; };
                        let message = pending.take().expect("ready socket frame");
                        let terminal = message.terminal;
                        permit.send(message);
                        if terminal { break; }
                    },
                    message = socket.next(), if pending.is_none() => {
                        let Some(message) = message else { break; };
                        match message {
                            Ok(Message::Ping(payload)) => {
                                if let Err(error) = socket.send(Message::Pong(payload)).await {
                                    pending = Some(SocketMessage { message: Err(error), permit: None, bytes: 0, terminal: true });
                                }
                            },
                            Ok(Message::Pong(_)) => {},
                            message => {
                                let (bytes, terminal) = match &message {
                                    Ok(Message::Text(text)) => (text.len(), false),
                                    Ok(Message::Binary(bytes)) => (bytes.len(), false),
                                    Ok(Message::Close(_)) | Err(_) => (0, true),
                                    _ => (0, false),
                                };
                                if bytes > MAX_BUFFERED_WEBSOCKET_BYTES {
                                    let error = tokio_tungstenite::tungstenite::Error::Io(std::io::Error::new(
                                        std::io::ErrorKind::InvalidData, "classifier WebSocket frame exceeds the bounded read buffer",
                                    ));
                                    pending = Some(SocketMessage { message: Err(error), permit: None, bytes: 0, terminal: true });
                                } else {
                                    pending = Some(SocketMessage { message, permit: None, bytes, terminal });
                                }
                            },
                        }
                    },
                }
            }
        });
        Self {
            commands,
            messages,
            task,
        }
    }

    async fn send(
        &mut self,
        message: Message,
    ) -> Result<(), tokio_tungstenite::tungstenite::Error> {
        let (result, response) = oneshot::channel();
        if self
            .commands
            .send(SocketCommand::Send { message, result })
            .await
            .is_ok()
        {
            if let Ok(result) = response.await {
                return result;
            }
        }
        Err(tokio_tungstenite::tungstenite::Error::ConnectionClosed)
    }

    async fn next(&mut self) -> Option<Result<Message, tokio_tungstenite::tungstenite::Error>> {
        self.messages.recv().await.map(|message| message.message)
    }
}

impl Drop for SocketPump {
    fn drop(&mut self) {
        self.task.abort();
    }
}

enum Connection {
    Websocket(PooledConnection),
    Http {
        endpoint: ClassifierEndpoint,
        thread_id: String,
    },
}

struct ConnectionLease {
    connection: Connection,
    endpoint: ClassifierEndpoint,
    _permit: OwnedSemaphorePermit,
}

impl ConnectionPool {
    fn replenish(self: &Arc<Self>) {
        let Ok(guard) = Arc::clone(&self.replenishing).try_lock_owned() else {
            return;
        };
        self.start_refill(guard);
    }

    fn start_refill(
        self: &Arc<Self>,
        guard: OwnedMutexGuard<()>,
    ) -> Option<tokio::task::JoinHandle<()>> {
        if self.shutdown.is_cancelled() {
            return None;
        }
        let mut retry_after = self
            .retry_after
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if retry_after.is_some_and(|deadline| Instant::now() < deadline) {
            return None;
        }
        *retry_after = None;
        drop(retry_after);
        let pool = Arc::clone(self);
        let closed = self
            .closed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *closed {
            return None;
        }
        Some(self.tasks.spawn(async move {
            let _guard = guard;
            let mut opening = JoinSet::new();
            let mut failed = false;
            loop {
                let idle = pool
                    .idle
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .len();
                while !failed
                    && !pool.shutdown.is_cancelled()
                    && idle + opening.len() < INITIAL_CONNECTIONS
                    && opening.len() < MAX_CONCURRENT_OPENS
                {
                    let Ok(permit) = Arc::clone(&pool.sockets).try_acquire_owned() else {
                        break;
                    };
                    let pool = Arc::clone(&pool);
                    opening.spawn(async move { pool.open_connection(permit).await });
                }
                let Some(result) = opening.join_next().await else {
                    break;
                };
                match result {
                    Ok(Ok(connection)) => {
                        let mut idle = pool
                            .idle
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        if !pool.shutdown.is_cancelled() && idle.len() < INITIAL_CONNECTIONS {
                            idle.push(connection);
                        }
                    }
                    Ok(Err(error)) => {
                        failed = true;
                        if error
                            .code
                            .as_ref()
                            .is_some_and(|code| code == "classifier_connect_timeout")
                        {
                            *pool
                                .retry_after
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                                Some(Instant::now() + CONNECT_COOLDOWN);
                        }
                    }
                    Err(_) => failed = true,
                }
            }
        }))
    }

    async fn open_connection(
        &self,
        permit: OwnedSemaphorePermit,
    ) -> Result<PooledConnection, ProviderError> {
        let endpoint = tokio::select! {
            _ = self.shutdown.cancelled() => return Err(cancelled_error()),
            endpoint = self.connector.resolve(&self.shutdown) => endpoint?,
        };
        let key = endpoint.key();
        let thread_id = uuid::Uuid::new_v4().to_string();
        let mut headers = session_headers(&endpoint, &self.attribution, &thread_id)?;
        headers.insert(
            "openai-beta",
            HeaderValue::from_static("responses_websockets=2026-02-06"),
        );
        let challenge = generate_key();
        headers.insert("connection", HeaderValue::from_static("Upgrade"));
        headers.insert("upgrade", HeaderValue::from_static("websocket"));
        headers.insert("sec-websocket-version", HeaderValue::from_static("13"));
        insert_header(&mut headers, "sec-websocket-key", &challenge)?;
        let connect = async {
            let response = endpoint
                .websocket_client
                .get(&endpoint.url)
                .version(reqwest::Version::HTTP_11)
                .headers(headers)
                .send()
                .await
                .map_err(network_error)?;
            if response.status() != reqwest::StatusCode::SWITCHING_PROTOCOLS {
                if let Some(attempt) = &endpoint.attempt {
                    attempt.settle(
                        false,
                        Some(response.status().as_u16()),
                        Some("WebSocket upgrade rejected"),
                    );
                }
                return Err(response_error(response).await);
            }
            let valid_upgrade = response
                .headers()
                .get("upgrade")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.eq_ignore_ascii_case("websocket"));
            let valid_connection = response
                .headers()
                .get("connection")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| {
                    value
                        .split(',')
                        .any(|token| token.trim().eq_ignore_ascii_case("upgrade"))
                });
            let expected_accept = derive_accept_key(challenge.as_bytes());
            let valid_accept = response
                .headers()
                .get("sec-websocket-accept")
                .and_then(|value| value.to_str().ok())
                == Some(expected_accept.as_str());
            if !valid_upgrade || !valid_connection || !valid_accept {
                return Err(transport_error(
                    "classifier WebSocket handshake was invalid",
                ));
            }
            let upgraded = response.upgrade().await.map_err(network_error)?;
            if let Some(attempt) = &endpoint.attempt {
                attempt.settle(true, Some(101), None);
            }
            let config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default();
            Ok(WebSocketStream::from_raw_socket(upgraded, Role::Client, Some(config)).await)
        };
        let socket = tokio::select! {
            _ = self.shutdown.cancelled() => return Err(cancelled_error()),
            result = tokio::time::timeout(endpoint.connect_timeout, connect) => match result {
                Ok(result) => result?,
                Err(_) => return Err(coded_error(ProviderErrorKind::Network, "classifier WebSocket connection timed out", "classifier_connect_timeout")),
            },
        };
        if self.shutdown.is_cancelled() {
            return Err(cancelled_error());
        }
        if endpoint
            .auth_changes
            .as_ref()
            .is_some_and(|changes| changes.has_changed().unwrap_or(true))
        {
            return Err(auth_resolution_error(&endpoint));
        }
        Ok(PooledConnection {
            socket: SocketPump::new(socket, &self.tasks),
            key,
            thread_id,
            expires_at: Instant::now() + MAX_WEBSOCKET_AGE,
            auth_changes: endpoint.auth_changes,
            _permit: permit,
        })
    }

    async fn lease(
        self: &Arc<Self>,
        signal: &AbortSignal,
    ) -> Result<ConnectionLease, ProviderError> {
        let permit = tokio::select! {
            _ = self.shutdown.cancelled() => return Err(cancelled_error()),
            _ = signal.cancelled() => return Err(cancelled_error()),
            permit = Arc::clone(&self.classifications).acquire_owned() => permit.map_err(|_| cancelled_error())?,
        };
        let endpoint = tokio::select! {
            _ = self.shutdown.cancelled() => return Err(cancelled_error()),
            _ = signal.cancelled() => return Err(cancelled_error()),
            endpoint = self.connector.resolve(signal) => endpoint?,
        };
        let key = endpoint.key();
        let connection = loop {
            let idle = self
                .idle
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pop();
            match idle {
                Some(connection) if connection.healthy(&key) => break Some(connection),
                Some(_) => {}
                None => break None,
            }
        };
        let connection = match connection {
            Some(connection) => {
                if self
                    .idle
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .len()
                    <= REFILL_LOW_WATERMARK
                {
                    self.replenish();
                }
                Connection::Websocket(connection)
            }
            None => {
                self.replenish();
                // Request admission never waits for a WebSocket to open.
                Connection::Http {
                    endpoint: ClassifierEndpoint {
                        url: endpoint.url.clone(),
                        headers: endpoint.headers.clone(),
                        client: endpoint.client.clone(),
                        websocket_client: endpoint.websocket_client.clone(),
                        auth_changes: endpoint.auth_changes.clone(),
                        auth_state: endpoint.auth_state,
                        classifier_route: endpoint.classifier_route,
                        connect_timeout: endpoint.connect_timeout,
                        idle_timeout: endpoint.idle_timeout,
                        attempt: endpoint.attempt.clone(),
                    },
                    thread_id: uuid::Uuid::new_v4().to_string(),
                }
            }
        };
        if endpoint
            .auth_changes
            .as_ref()
            .is_some_and(|changes| changes.has_changed().unwrap_or(true))
        {
            return Err(auth_resolution_error(&endpoint));
        }
        Ok(ConnectionLease {
            connection,
            endpoint,
            _permit: permit,
        })
    }

    async fn run_request(
        self: Arc<Self>,
        request: ClassifierRequest,
        signal: AbortSignal,
        tx: mpsc::Sender<Result<RuntimeJson, ProviderError>>,
    ) {
        let lease = match self.lease(&signal).await {
            Ok(lease) => lease,
            Err(error) => {
                send_terminal(&tx, Err(error)).await;
                return;
            }
        };
        let thread_id = match &lease.connection {
            Connection::Websocket(connection) => &connection.thread_id,
            Connection::Http { thread_id, .. } => thread_id,
        };
        let body = match self.connector.prepare(&request, thread_id, &lease.endpoint) {
            Ok(body) => body,
            Err(error) => {
                send_terminal(&tx, Err(error)).await;
                return;
            }
        };
        let ConnectionLease {
            connection,
            endpoint,
            _permit,
        } = lease;
        match connection {
            Connection::Websocket(mut connection) => {
                let reusable = run_websocket(
                    &mut connection.socket,
                    body,
                    &endpoint,
                    &signal,
                    &self.shutdown,
                    &tx,
                )
                .await;
                if reusable && connection.healthy(&endpoint.key()) {
                    let mut idle = self
                        .idle
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if !self.shutdown.is_cancelled() {
                        idle.push(connection);
                    }
                }
            }
            Connection::Http {
                endpoint,
                thread_id,
            } => {
                run_http(
                    body,
                    endpoint,
                    &request.attribution,
                    &thread_id,
                    &signal,
                    &self.shutdown,
                    &tx,
                )
                .await;
            }
        }
    }
}

fn session_headers(
    endpoint: &ClassifierEndpoint,
    attribution: &ClassifierAttribution,
    thread_id: &str,
) -> Result<HeaderMap, ProviderError> {
    let mut headers = HeaderMap::new();
    for (name, value) in &endpoint.headers {
        insert_header(&mut headers, name, value)?;
    }
    if !attribution.session_id.is_empty() {
        insert_header(&mut headers, "session-id", &attribution.session_id)?;
    }
    insert_header(&mut headers, "thread-id", thread_id)?;
    if endpoint.classifier_route {
        headers.insert("x-codex-guardian", HeaderValue::from_static("classifier"));
    }
    headers.insert("x-openai-subagent", HeaderValue::from_static("guardian"));
    insert_header(&mut headers, "x-codex-window-id", &format!("{thread_id}:0"))?;
    headers.insert(
        "x-openai-internal-codex-responses-lite",
        HeaderValue::from_static("true"),
    );
    insert_header(&mut headers, "x-client-request-id", thread_id)?;
    if let Some(originator) = &attribution.originator {
        insert_header(&mut headers, "originator", originator)?;
    }
    Ok(headers)
}

fn insert_header(headers: &mut HeaderMap, name: &str, value: &str) -> Result<(), ProviderError> {
    let name = HeaderName::from_bytes(name.as_bytes())
        .map_err(|_| transport_error("invalid classifier header name"))?;
    let value = HeaderValue::from_str(value)
        .map_err(|_| transport_error("invalid classifier header value"))?;
    headers.insert(name, value);
    Ok(())
}

async fn owner_changed(
    changes: &mut Option<watch::Receiver<CredentialChangeState>>,
    owner: Option<u64>,
) {
    let Some(changes) = changes else {
        std::future::pending::<()>().await;
        return;
    };
    loop {
        if Some(changes.borrow_and_update().owner_generation) != owner {
            return;
        }
        if changes.changed().await.is_err() {
            return;
        }
    }
}

fn owner_matches(
    changes: &Option<watch::Receiver<CredentialChangeState>>,
    owner: Option<u64>,
) -> bool {
    changes
        .as_ref()
        .map(|changes| changes.borrow().owner_generation)
        == owner
}

async fn send_event(
    tx: &mpsc::Sender<Result<RuntimeJson, ProviderError>>,
    event: Result<RuntimeJson, ProviderError>,
    signal: &AbortSignal,
    shutdown: &AbortSignal,
) -> bool {
    tokio::select! {
        result = tx.send(event) => result.is_ok(),
        _ = signal.cancelled() => false,
        _ = shutdown.cancelled() => false,
    }
}

async fn send_terminal(
    tx: &mpsc::Sender<Result<RuntimeJson, ProviderError>>,
    event: Result<RuntimeJson, ProviderError>,
) {
    let _ = tokio::time::timeout(INTERRUPT_DRAIN_TIMEOUT, tx.send(event)).await;
}

async fn run_websocket(
    socket: &mut SocketPump,
    mut body: RuntimeJson,
    endpoint: &ClassifierEndpoint,
    signal: &AbortSignal,
    shutdown: &AbortSignal,
    tx: &mpsc::Sender<Result<RuntimeJson, ProviderError>>,
) -> bool {
    let owner = endpoint.auth_state.map(|state| state.owner_generation);
    if !owner_matches(&endpoint.auth_changes, owner) {
        send_terminal(tx, Err(auth_changed_error())).await;
        return true;
    }
    if signal.is_cancelled() || shutdown.is_cancelled() || tx.is_closed() {
        return true;
    }
    body["type"] = RuntimeJson::String("response.create".into());
    let text = runtime_json::stringify(&body)
        .try_to_utf8()
        .unwrap_or_default();
    let dispatch = socket.send(Message::Text(text));
    let sent = tokio::select! {
        _ = signal.cancelled() => return false,
        _ = shutdown.cancelled() => return false,
        _ = tx.closed() => return false,
        sent = tokio::time::timeout(endpoint.idle_timeout, dispatch) => sent,
    };
    if !matches!(sent, Ok(Ok(()))) {
        if let Some(attempt) = &endpoint.attempt {
            attempt.settle(false, None, Some("WebSocket request send failed"));
        }
        send_terminal(tx, Err(transport_error("classifier WebSocket send failed"))).await;
        return false;
    }
    if let Some(attempt) = &endpoint.attempt {
        attempt.settle(true, None, None);
    }
    let mut auth_changes = endpoint.auth_changes.clone();
    let mut response_id: Option<RuntimeString> = None;
    let mut cancelling = false;
    let mut deadline = None;
    let mut interrupt_sent = false;
    let mut acknowledged = false;
    let mut completed = None;
    let mut owner_lost = false;
    loop {
        if cancelling && !interrupt_sent {
            if let Some(response_id) = &response_id {
                let interrupt = crate::runtime_values::runtime_json::parse(&RuntimeString::from(
                    "{\"type\":\"response.interrupt\",\"mode\":\"discard_partial_items\"}",
                ))
                .expect("static interrupt object");
                let mut interrupt = interrupt;
                interrupt["response_id"] = RuntimeJson::String(response_id.clone());
                let text = runtime_json::stringify(&interrupt)
                    .try_to_utf8()
                    .unwrap_or_default();
                let remaining = deadline.unwrap_or_else(Instant::now);
                if !matches!(
                    tokio::time::timeout_at(remaining, socket.send(Message::Text(text))).await,
                    Ok(Ok(()))
                ) {
                    break;
                }
                interrupt_sent = true;
            }
        }
        if cancelling && acknowledged && completed.is_some() {
            send_terminal(tx, Ok(completed.take().expect("completed event"))).await;
            if owner_lost {
                send_terminal(tx, Err(auth_changed_error())).await;
            }
            return true;
        }
        let timeout = deadline.unwrap_or_else(|| Instant::now() + endpoint.idle_timeout);
        let next = tokio::select! {
            _ = signal.cancelled(), if !cancelling => { cancelling = true; deadline = Some(Instant::now() + INTERRUPT_DRAIN_TIMEOUT); continue; },
            _ = shutdown.cancelled(), if !cancelling => { cancelling = true; deadline = Some(Instant::now() + INTERRUPT_DRAIN_TIMEOUT); continue; },
            _ = tx.closed(), if !cancelling => { cancelling = true; deadline = Some(Instant::now() + INTERRUPT_DRAIN_TIMEOUT); continue; },
            _ = owner_changed(&mut auth_changes, owner), if !cancelling => {
                owner_lost = true;
                cancelling = true;
                deadline = Some(Instant::now() + INTERRUPT_DRAIN_TIMEOUT);
                continue;
            },
            next = tokio::time::timeout_at(timeout, socket.next()) => next,
        };
        let message = match next {
            Ok(Some(Ok(message))) => message,
            _ => break,
        };
        if !cancelling && !owner_matches(&auth_changes, owner) {
            owner_lost = true;
            cancelling = true;
            deadline = Some(Instant::now() + INTERRUPT_DRAIN_TIMEOUT);
        }
        let text = match message {
            Message::Text(text) => text,
            Message::Binary(bytes) => match String::from_utf8(bytes) {
                Ok(text) => text,
                Err(_) => break,
            },
            Message::Ping(_) | Message::Pong(_) => continue,
            Message::Close(_) => break,
            Message::Frame(_) => continue,
        };
        let Ok(event) = runtime_json::parse(&RuntimeString::from(text)) else {
            continue;
        };
        let kind = event
            .get("type")
            .and_then(RuntimeJson::as_str)
            .and_then(|kind| kind.try_to_utf8().ok())
            .unwrap_or_default();
        if kind == "response.created" {
            response_id = event
                .get("response")
                .and_then(|response| response.get("id"))
                .and_then(RuntimeJson::as_str)
                .cloned();
        }
        if cancelling
            && matches!(
                kind.as_str(),
                "response.interrupt.accepted" | "response.interrupt.failed"
            )
        {
            let acknowledgement = event
                .get("response_id")
                .and_then(RuntimeJson::as_str)
                .cloned();
            if !interrupt_sent
                || acknowledgement != response_id
                || kind == "response.interrupt.failed"
            {
                break;
            }
            acknowledged = true;
            continue;
        }
        if kind == "response.completed" {
            if cancelling {
                completed = Some(event);
                continue;
            }
            // Completion is queued before releasing its exclusive lease.
            if send_event(tx, Ok(event.clone()), signal, shutdown).await {
                return true;
            }
            completed = Some(event);
            cancelling = true;
            deadline = Some(Instant::now() + INTERRUPT_DRAIN_TIMEOUT);
            continue;
        }
        let terminal_error = matches!(
            kind.as_str(),
            "error" | "response.failed" | "response.incomplete"
        );
        if !cancelling && !send_event(tx, Ok(event), signal, shutdown).await {
            cancelling = true;
            deadline = Some(Instant::now() + INTERRUPT_DRAIN_TIMEOUT);
        }
        if terminal_error {
            break;
        }
    }
    // Preserve terminal usage even when the interrupt handshake fails.
    if let Some(completed) = completed {
        send_terminal(tx, Ok(completed)).await;
    }
    let error = if owner_lost {
        auth_changed_error()
    } else if cancelling {
        cancelled_error()
    } else {
        transport_error("classifier WebSocket closed before response.completed")
    };
    send_terminal(tx, Err(error)).await;
    false
}

async fn run_http(
    body: RuntimeJson,
    endpoint: ClassifierEndpoint,
    attribution: &ClassifierAttribution,
    thread_id: &str,
    signal: &AbortSignal,
    shutdown: &AbortSignal,
    tx: &mpsc::Sender<Result<RuntimeJson, ProviderError>>,
) {
    let headers = match session_headers(&endpoint, attribution, thread_id) {
        Ok(headers) => headers,
        Err(error) => {
            send_terminal(tx, Err(error)).await;
            return;
        }
    };
    let body = runtime_json::stringify(&body)
        .try_to_utf8()
        .unwrap_or_default();
    let mut changes = endpoint.auth_changes.clone();
    let owner = endpoint.auth_state.map(|state| state.owner_generation);
    if !owner_matches(&changes, owner) {
        send_terminal(tx, Err(auth_changed_error())).await;
        return;
    }
    let establish = endpoint
        .client
        .post(&endpoint.url)
        .headers(headers)
        .header("content-type", "application/json")
        .body(body)
        .send();
    let response = tokio::select! {
        _ = signal.cancelled() => return,
        _ = shutdown.cancelled() => return,
        _ = tx.closed() => return,
        _ = owner_changed(&mut changes, owner) => { send_terminal(tx, Err(auth_changed_error())).await; return; },
        response = tokio::time::timeout(endpoint.idle_timeout, establish) => match response {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => { send_terminal(tx, Err(network_error(error))).await; return; },
            Err(_) => { send_terminal(tx, Err(transport_error("classifier HTTP headers timed out"))).await; return; },
        },
    };
    if !owner_matches(&changes, owner) {
        send_terminal(tx, Err(auth_changed_error())).await;
        return;
    }
    if !response.status().is_success() {
        if let Some(attempt) = &endpoint.attempt {
            attempt.settle(
                false,
                Some(response.status().as_u16()),
                Some("HTTP request rejected"),
            );
        }
        let error = tokio::select! {
            _ = signal.cancelled() => return,
            _ = shutdown.cancelled() => return,
            _ = tx.closed() => return,
            error = tokio::time::timeout(endpoint.idle_timeout, response_error(response)) =>
                error.unwrap_or_else(|_| transport_error("classifier HTTP error body timed out")),
        };
        send_terminal(tx, Err(error)).await;
        return;
    }
    if let Some(attempt) = &endpoint.attempt {
        attempt.settle(true, Some(response.status().as_u16()), None);
    }
    let mut bytes = response.bytes_stream();
    let mut decoder = SseEvents::default();
    loop {
        let next = tokio::select! {
            _ = signal.cancelled() => return,
            _ = shutdown.cancelled() => return,
            _ = tx.closed() => return,
            _ = owner_changed(&mut changes, owner) => { send_terminal(tx, Err(auth_changed_error())).await; return; },
            next = tokio::time::timeout(endpoint.idle_timeout, bytes.next()) => next,
        };
        let chunk = match next {
            Ok(Some(Ok(chunk))) => chunk,
            Ok(Some(Err(error))) => {
                send_terminal(tx, Err(network_error(error))).await;
                return;
            }
            _ => {
                send_terminal(
                    tx,
                    Err(transport_error(
                        "classifier HTTP ended before response.completed",
                    )),
                )
                .await;
                return;
            }
        };
        if !owner_matches(&changes, owner) {
            send_terminal(tx, Err(auth_changed_error())).await;
            return;
        }
        if let Err(error) = decoder.push(&chunk) {
            send_terminal(tx, Err(error)).await;
            return;
        }
        while let Some(text) = decoder.next() {
            if !owner_matches(&changes, owner) {
                send_terminal(tx, Err(auth_changed_error())).await;
                return;
            }
            let Ok(event) = runtime_json::parse(&text) else {
                continue;
            };
            let terminal = event
                .get("type")
                .and_then(RuntimeJson::as_str)
                .is_some_and(|kind| {
                    kind == "response.completed"
                        || kind == "response.failed"
                        || kind == "response.incomplete"
                        || kind == "error"
                });
            if !send_event(tx, Ok(event), signal, shutdown).await || terminal {
                return;
            }
        }
    }
}

struct SseEvents(crate::provider::openai::SseDecoder);

impl Default for SseEvents {
    fn default() -> Self {
        Self(crate::provider::openai::SseDecoder::new())
    }
}

impl SseEvents {
    fn push(&mut self, bytes: &[u8]) -> Result<(), ProviderError> {
        if self.0.buffered_bytes().saturating_add(bytes.len()) > MAX_SSE_EVENT_BYTES {
            return Err(transport_error(
                "classifier SSE event exceeded its byte limit",
            ));
        }
        self.0.push(bytes);
        Ok(())
    }

    fn next(&mut self) -> Option<RuntimeString> {
        self.0.next_event().map(|event| event.data)
    }
}

pub(super) fn coded_error(kind: ProviderErrorKind, message: &str, code: &str) -> ProviderError {
    ProviderError {
        kind,
        message: message.into(),
        status: None,
        code: Some(code.into()),
        error_type: None,
        media_origins: None,
        media_repair_complete: false,
        credential_recovery: None,
        retry_after_ms: None,
        request_id: None,
    }
}

fn transport_error(message: &str) -> ProviderError {
    coded_error(ProviderErrorKind::Network, message, "classifier_transport")
}

fn cancelled_error() -> ProviderError {
    coded_error(
        ProviderErrorKind::Unknown,
        "classifier request was cancelled",
        "classifier_cancelled",
    )
}

fn auth_changed_error() -> ProviderError {
    coded_error(
        ProviderErrorKind::Auth,
        "authentication changed during classification",
        "classifier_auth_changed",
    )
}

fn auth_resolution_error(endpoint: &ClassifierEndpoint) -> ProviderError {
    if owner_matches(
        &endpoint.auth_changes,
        endpoint.auth_state.map(|state| state.owner_generation),
    ) {
        coded_error(
            ProviderErrorKind::Network,
            "authentication refreshed while resolving classifier routing",
            "classifier_auth_revision_changed",
        )
    } else {
        auth_changed_error()
    }
}

fn network_error(error: reqwest::Error) -> ProviderError {
    crate::provider::classify::classify_error(&crate::provider::classify::RawProviderError {
        message: error.to_string(),
        status: error.status().map(|status| status.as_u16()),
        ..Default::default()
    })
}

async fn response_error(response: reqwest::Response) -> ProviderError {
    let status = response.status().as_u16();
    let headers = response
        .headers()
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_owned(), value.to_owned()))
        })
        .collect();
    let mut chunks = response.bytes_stream();
    let mut bytes = Vec::with_capacity(8 * 1024);
    while let Some(Ok(chunk)) = chunks.next().await {
        let remaining = (8 * 1024usize).saturating_sub(bytes.len());
        bytes.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
        if bytes.len() == 8 * 1024 {
            break;
        }
    }
    crate::provider::classify::classify_error(&crate::provider::openai::raw_http_error(
        status,
        &headers,
        &String::from_utf8_lossy(&bytes),
    ))
}

#[cfg(test)]
mod tests;
