//! Hand-rolled MCP transports over JSON-RPC 2.0.
//!
//! There is no MCP client library here, so this module implements the wire
//! transports directly:
//!  - [`StdioTransport`] spawns a child process and exchanges newline-delimited
//!    JSON-RPC over its stdin/stdout (one JSON object per line, UTF-8),
//!  - [`SseTransport`] speaks the legacy HTTP + Server-Sent-Events protocol (a
//!    GET event stream plus a POST message channel),
//!  - [`HttpTransport`] speaks the streamable HTTP protocol (POST a request, read
//!    the reply as JSON or an SSE stream; honor a returned session id),
//!  - [`InMemoryTransport`] links two peers over a channel pair (for tests and the
//!    in-process server variant).
//!
//! Every transport delivers inbound traffic as [`TransportEvent`]s on a channel
//! the caller supplies to [`Transport::start`], so a [`crate::mcp::client::Client`]
//! can pump them uniformly regardless of the wire.

use crate::mcp::oauth::{
    auth, AuthOptions, AuthResult, McpFetch, OAuthClientProvider, OAuthTokens,
};
use crate::runtime_process::{
    RuntimeChildStdin, RuntimeEnv, RuntimeProcessContext, RuntimeSignal, SpawnedRuntimeProcess,
};
use crate::runtime_values::{runtime_json, NodeBufferUtf8, RuntimeJson, RuntimeString, WhatwgUtf8};
use crate::types::abort::AbortSignal;
use async_trait::async_trait;
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
use tokio::sync::Mutex as AsyncMutex;

const HTTP_CLOSE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// A transport or protocol failure. `code` carries an HTTP status or JSON-RPC
/// error code when one is available, which the connect path uses to tell a
/// 401/403 ("needs auth") apart from a hard failure. `aborted` marks a
/// cooperative cancellation (the caller's abort signal fired).
#[derive(Debug, Clone)]
pub struct McpError {
    pub message: RuntimeString,
    pub code: Option<i64>,
    pub aborted: bool,
}

impl McpError {
    pub fn new(message: impl Into<RuntimeString>) -> Self {
        McpError {
            message: message.into(),
            code: None,
            aborted: false,
        }
    }

    pub fn with_code(message: impl Into<RuntimeString>, code: i64) -> Self {
        McpError {
            message: message.into(),
            code: Some(code),
            aborted: false,
        }
    }

    pub fn aborted() -> Self {
        McpError {
            message: "The operation was aborted".into(),
            code: None,
            aborted: true,
        }
    }
}

impl std::fmt::Display for McpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.message)
    }
}

impl std::error::Error for McpError {}

pub type McpResult<T> = Result<T, McpError>;

// ---------------------------------------------------------------------------
// JSON-RPC framing (pure, testable)
// ---------------------------------------------------------------------------

/// Serialize one JSON-RPC message to a single newline-terminated line. This is
/// the stdio framing: one JSON object per line, no embedded newlines (JSON
/// serialization never emits a raw newline inside a string).
pub fn encode_ndjson(message: &RuntimeJson) -> RuntimeString {
    runtime_json::stringify(message).concat(&RuntimeString::from("\n"))
}

/// Parse one newline-delimited JSON-RPC line into a message value. Surrounding
/// whitespace (including a trailing `\r`) is trimmed first.
pub fn decode_ndjson(line: &RuntimeString) -> Result<RuntimeJson, runtime_json::RuntimeJsonError> {
    runtime_json::parse(line)
}

/// One decoded Server-Sent-Events block: its `event` type (default `message`
/// when absent) and the concatenated `data` payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseEvent {
    pub event: Option<RuntimeString>,
    pub data: RuntimeString,
    pub id: Option<RuntimeString>,
}

/// Parse a single SSE event block (the text between two blank lines). Multiple
/// `data:` lines are joined with `\n`; a single leading space after the colon is
/// stripped; lines beginning with `:` are comments and ignored.
pub fn parse_sse_event(block: &RuntimeString) -> SseEvent {
    let mut event = None;
    let mut id = None;
    let mut data_lines = Vec::new();
    for raw in block.as_utf16().split(|unit| *unit == u16::from(b'\n')) {
        let line = raw.strip_suffix(&[u16::from(b'\r')]).unwrap_or(raw);
        if line.is_empty() || line.first() == Some(&u16::from(b':')) {
            continue;
        }
        let (field, value) = match line.iter().position(|unit| *unit == u16::from(b':')) {
            Some(index) => {
                let value = &line[index + 1..];
                (
                    &line[..index],
                    value.strip_prefix(&[u16::from(b' ')]).unwrap_or(value),
                )
            }
            None => (line, &[][..]),
        };
        if field == "event".encode_utf16().collect::<Vec<_>>() {
            event = Some(RuntimeString::from_utf16(value.to_vec()));
        } else if field == "data".encode_utf16().collect::<Vec<_>>() {
            data_lines.push(RuntimeString::from_utf16(value.to_vec()));
        } else if field == "id".encode_utf16().collect::<Vec<_>>() {
            id = Some(RuntimeString::from_utf16(value.to_vec()));
        }
    }
    let data = join_runtime_strings(&data_lines, "\n");
    SseEvent { event, data, id }
}

/// Incremental SSE decoder: feed raw response chunks, receive complete events.
/// Event boundaries are blank lines (`\n\n`); `\r\n` is normalized to `\n`.
pub struct SseDecoder {
    buf: Vec<u16>,
    utf8: WhatwgUtf8,
}

impl Default for SseDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl SseDecoder {
    pub fn new() -> Self {
        SseDecoder {
            buf: Vec::new(),
            utf8: WhatwgUtf8::new(),
        }
    }

    /// Append a chunk of decoded text and return any events it completed.
    pub fn push(&mut self, chunk: &RuntimeString) -> Vec<SseEvent> {
        let units = chunk.as_utf16();
        let mut index = 0;
        while index < units.len() {
            if units[index] == u16::from(b'\r') && units.get(index + 1) == Some(&u16::from(b'\n')) {
                self.buf.push(u16::from(b'\n'));
                index += 2;
            } else {
                self.buf.push(units[index]);
                index += 1;
            }
        }
        let mut events = Vec::new();
        loop {
            let Some(index) = self
                .buf
                .windows(2)
                .position(|window| window == [u16::from(b'\n'), u16::from(b'\n')])
            else {
                break;
            };
            let block = RuntimeString::from_utf16(self.buf[..index].to_vec());
            self.buf.drain(..index + 2);
            if !block.is_empty() {
                events.push(parse_sse_event(&block));
            }
        }
        events
    }

    /// Decode one HTTP body chunk with the WHATWG UTF-8 state machine before
    /// applying SSE framing.
    pub fn push_bytes(&mut self, chunk: &[u8]) -> Vec<SseEvent> {
        let decoded = self.utf8.feed(chunk);
        self.push(&decoded)
    }

    pub fn finish(&mut self) -> Vec<SseEvent> {
        let decoded = std::mem::take(&mut self.utf8).finish();
        self.push(&decoded)
    }
}

fn join_runtime_strings(values: &[RuntimeString], separator: &str) -> RuntimeString {
    let Some(mut output) = values.first().cloned() else {
        return RuntimeString::default();
    };
    let separator = RuntimeString::from(separator);
    for value in &values[1..] {
        output = output.concat(&separator).concat(value);
    }
    output
}

fn whatwg_decode_complete(bytes: &[u8]) -> RuntimeString {
    let mut decoder = WhatwgUtf8::new();
    decoder.feed(bytes).concat(&decoder.finish())
}

/// URL parsing consumes Web-IDL `USVString`; this is the semantic HTTP URL
/// terminal, not a general runtime-string projection.
fn url_terminal_input(value: &RuntimeString) -> String {
    let value = trim_ecmascript(value);
    value
        .code_points()
        .map(|point| char::from_u32(point).unwrap_or(char::REPLACEMENT_CHARACTER))
        .collect()
}

fn trim_ecmascript(value: &RuntimeString) -> RuntimeString {
    let is_whitespace = |unit: u16| {
        matches!(
            unit,
            0x0009..=0x000d
                | 0x0020
                | 0x00a0
                | 0x1680
                | 0x2000..=0x200a
                | 0x2028
                | 0x2029
                | 0x202f
                | 0x205f
                | 0x3000
                | 0xfeff
        )
    };
    let units = value.as_utf16();
    let start = units
        .iter()
        .position(|unit| !is_whitespace(*unit))
        .unwrap_or(units.len());
    let end = units
        .iter()
        .rposition(|unit| !is_whitespace(*unit))
        .map_or(start, |index| index + 1);
    value.slice_utf16(start..end)
}

// ---------------------------------------------------------------------------
// Transport trait
// ---------------------------------------------------------------------------

/// One inbound transport event delivered to the pump.
#[derive(Debug)]
pub enum TransportEvent {
    /// A decoded JSON-RPC message (request, response, or notification).
    Message(RuntimeJson),
    /// A transport-level error (surfaced but non-fatal to the pump).
    Error(McpError),
    /// The transport closed; the pump fails any in-flight requests.
    Close,
}

/// The transport seam a [`crate::mcp::client::Client`] talks to. Object-safe so a
/// client can hold `Arc<dyn Transport>` regardless of the concrete wire.
///
/// `start` is single-use: inbound traffic is delivered on `sink` until the
/// transport closes. Calling `start` twice returns an error.
#[async_trait]
pub trait Transport: Send + Sync {
    /// Begin the transport; deliver inbound events on `sink`.
    async fn start(&self, sink: mpsc::UnboundedSender<TransportEvent>) -> McpResult<()>;
    /// Send one JSON-RPC message.
    async fn send(&self, message: RuntimeJson) -> McpResult<()>;
    /// Close the transport and release its resources. Idempotent.
    async fn close(&self) -> McpResult<()>;
    /// The session id the server assigned (streamable HTTP), if any.
    fn session_id(&self) -> Option<String> {
        None
    }
    /// Whether this transport can complete an interactive OAuth code exchange.
    fn supports_finish_auth(&self) -> bool {
        false
    }
    /// Replace the bearer used by subsequent remote requests without resetting
    /// the MCP transport/session. Non-HTTP transports return `false`.
    fn replace_bearer_token(&self, _access_token: String) -> bool {
        false
    }
    /// Exchange an authorization `code` for tokens on this transport.
    async fn finish_auth(&self, _code: String) -> McpResult<()> {
        Err(McpError::new(
            "finishAuth is not supported by this transport",
        ))
    }
}

// ---------------------------------------------------------------------------
// OAuth-aware remote transport
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum RemoteTransportKind {
    Http,
    Sse,
}

/// Wraps a remote transport with the existing MCP OAuth state machine.
///
/// A 401/403 drives discovery, dynamic registration, PKCE, refresh, and either
/// an interactive redirect or an authenticated retry. Interactive code exchange
/// happens on this same wrapper so its in-memory PKCE verifier is retained; the
/// caller then creates a fresh transport which reads the persisted token bundle.
pub struct OAuthTransport {
    kind: RemoteTransportKind,
    server_url: String,
    configured_headers: HashMap<String, String>,
    provider: Arc<dyn OAuthClientProvider>,
    fetch: Option<Arc<dyn McpFetch>>,
    inner: AsyncMutex<Option<Arc<dyn Transport>>>,
    sink: Mutex<Option<mpsc::UnboundedSender<TransportEvent>>>,
    started: AtomicBool,
    auth_lock: AsyncMutex<()>,
    auth_generation: AtomicU64,
}

impl OAuthTransport {
    pub fn http(
        server_url: String,
        configured_headers: HashMap<String, String>,
        provider: Arc<dyn OAuthClientProvider>,
        fetch: Option<Arc<dyn McpFetch>>,
    ) -> Self {
        Self::new(
            RemoteTransportKind::Http,
            server_url,
            configured_headers,
            provider,
            fetch,
        )
    }

    pub fn sse(
        server_url: String,
        configured_headers: HashMap<String, String>,
        provider: Arc<dyn OAuthClientProvider>,
        fetch: Option<Arc<dyn McpFetch>>,
    ) -> Self {
        Self::new(
            RemoteTransportKind::Sse,
            server_url,
            configured_headers,
            provider,
            fetch,
        )
    }

    fn new(
        kind: RemoteTransportKind,
        server_url: String,
        configured_headers: HashMap<String, String>,
        provider: Arc<dyn OAuthClientProvider>,
        fetch: Option<Arc<dyn McpFetch>>,
    ) -> Self {
        Self {
            kind,
            server_url,
            configured_headers,
            provider,
            fetch,
            inner: AsyncMutex::new(None),
            sink: Mutex::new(None),
            started: AtomicBool::new(false),
            auth_lock: AsyncMutex::new(()),
            auth_generation: AtomicU64::new(0),
        }
    }

    async fn build_inner(&self) -> McpResult<Arc<dyn Transport>> {
        let mut headers = self.configured_headers.clone();
        let tokens =
            self.provider.tokens().await.map_err(|error| {
                McpError::new(format!("MCP OAuth credential read failed: {error}"))
            })?;
        if let Some(OAuthTokens { access_token, .. }) = tokens {
            let access_token = runtime_oauth_access_token_to_header_host(&access_token)?;
            replace_bearer_header(&mut headers, &access_token);
        }
        let transport: Arc<dyn Transport> = match self.kind {
            RemoteTransportKind::Http => {
                Arc::new(HttpTransport::new(self.server_url.clone(), headers))
            }
            RemoteTransportKind::Sse => {
                Arc::new(SseTransport::new(self.server_url.clone(), headers))
            }
        };
        Ok(transport)
    }

    async fn install_fresh_inner(
        &self,
        sink: mpsc::UnboundedSender<TransportEvent>,
    ) -> McpResult<Arc<dyn Transport>> {
        let transport = self.build_inner().await?;
        transport.start(sink).await?;
        *self.inner.lock().await = Some(transport.clone());
        Ok(transport)
    }

    async fn authenticate_after_unauthorized(&self) -> McpResult<AuthResult> {
        auth(
            self.provider.as_ref(),
            AuthOptions {
                server_url: self.server_url.clone(),
                fetch: self.fetch.clone(),
                ..Default::default()
            },
        )
        .await
        .map_err(|error| McpError::new(format!("MCP OAuth authorization failed: {error}")))
    }

    async fn retry_after_auth<T, F, Fut>(
        &self,
        observed_generation: u64,
        operation: F,
    ) -> McpResult<T>
    where
        F: FnOnce(Arc<dyn Transport>) -> Fut,
        Fut: std::future::Future<Output = McpResult<T>>,
    {
        let guard = self.auth_lock.lock().await;
        let transport = self
            .inner
            .lock()
            .await
            .clone()
            .ok_or_else(|| McpError::new("OAuth transport is not started"))?;
        if self.auth_generation.load(Ordering::SeqCst) == observed_generation {
            match self.authenticate_after_unauthorized().await? {
                AuthResult::Redirect => {
                    return Err(McpError::with_code("MCP authorization required", 401));
                }
                AuthResult::Authorized => {}
            }
            let access_token = self
                .provider
                .tokens()
                .await
                .map_err(|error| {
                    McpError::new(format!(
                        "MCP OAuth credential read after refresh failed: {error}"
                    ))
                })?
                .map(|tokens| tokens.access_token)
                .ok_or_else(|| {
                    McpError::new("MCP OAuth refresh returned no usable access token")
                })?;
            let access_token = runtime_oauth_access_token_to_header_host(&access_token)?;
            if access_token.trim().is_empty() {
                return Err(McpError::new(
                    "MCP OAuth refresh returned no usable access token",
                ));
            }
            if !transport.replace_bearer_token(access_token) {
                return Err(McpError::new(
                    "MCP OAuth transport cannot update its bearer in place",
                ));
            }
            self.auth_generation.fetch_add(1, Ordering::SeqCst);
        }
        drop(guard);
        operation(transport).await
    }
}

fn is_auth_error(error: &McpError) -> bool {
    matches!(error.code, Some(401 | 403))
}

#[async_trait]
impl Transport for OAuthTransport {
    async fn start(&self, sink: mpsc::UnboundedSender<TransportEvent>) -> McpResult<()> {
        if self.started.swap(true, Ordering::SeqCst) {
            return Err(McpError::new("OAuthTransport already started"));
        }
        *self.sink.lock().unwrap() = Some(sink.clone());
        let transport = self.build_inner().await?;
        match transport.start(sink).await {
            Ok(()) => {
                *self.inner.lock().await = Some(transport);
                Ok(())
            }
            Err(error) if is_auth_error(&error) => {
                let _guard = self.auth_lock.lock().await;
                match self.authenticate_after_unauthorized().await? {
                    AuthResult::Redirect => {
                        Err(McpError::with_code("MCP authorization required", 401))
                    }
                    AuthResult::Authorized => {
                        let sink = self
                            .sink
                            .lock()
                            .unwrap()
                            .clone()
                            .ok_or_else(|| McpError::new("OAuth transport is not started"))?;
                        self.install_fresh_inner(sink).await.map(|_| ())
                    }
                }
            }
            Err(error) => Err(error),
        }
    }

    async fn send(&self, message: RuntimeJson) -> McpResult<()> {
        let auth_generation = self.auth_generation.load(Ordering::SeqCst);
        let transport = self
            .inner
            .lock()
            .await
            .clone()
            .ok_or_else(|| McpError::new("OAuth transport is not started"))?;
        match transport.send(message.clone()).await {
            Ok(()) => Ok(()),
            Err(error) if is_auth_error(&error) => {
                self.retry_after_auth(auth_generation, move |transport| async move {
                    transport.send(message).await
                })
                .await
            }
            Err(error) => Err(error),
        }
    }

    async fn close(&self) -> McpResult<()> {
        let transport = self.inner.lock().await.take();
        if let Some(transport) = transport {
            transport.close().await?;
        }
        Ok(())
    }

    fn supports_finish_auth(&self) -> bool {
        true
    }

    async fn finish_auth(&self, code: String) -> McpResult<()> {
        let _guard = self.auth_lock.lock().await;
        let result = auth(
            self.provider.as_ref(),
            AuthOptions {
                server_url: self.server_url.clone(),
                authorization_code: Some(code),
                fetch: self.fetch.clone(),
                ..Default::default()
            },
        )
        .await
        .map_err(|error| McpError::new(format!("MCP OAuth code exchange failed: {error}")))?;
        if result != AuthResult::Authorized {
            return Err(McpError::new(
                "MCP OAuth code exchange did not authorize the transport",
            ));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// In-memory transport (linked pair)
// ---------------------------------------------------------------------------

/// One end of an in-process transport pair. Messages sent on one end surface as
/// inbound messages on the other. Used for tests and the `in-process` server
/// variant.
pub struct InMemoryTransport {
    /// Messages we send are pushed here (the peer's inbound channel).
    outgoing: Mutex<Option<mpsc::UnboundedSender<RuntimeJson>>>,
    /// The peer pushes messages for us here; taken by `start`.
    incoming: Mutex<Option<mpsc::UnboundedReceiver<RuntimeJson>>>,
    /// A clone of the sink, so `close` can emit our own `Close` event.
    sink: Mutex<Option<mpsc::UnboundedSender<TransportEvent>>>,
    started: AtomicBool,
}

impl InMemoryTransport {
    fn new(
        outgoing: mpsc::UnboundedSender<RuntimeJson>,
        incoming: mpsc::UnboundedReceiver<RuntimeJson>,
    ) -> Self {
        InMemoryTransport {
            outgoing: Mutex::new(Some(outgoing)),
            incoming: Mutex::new(Some(incoming)),
            sink: Mutex::new(None),
            started: AtomicBool::new(false),
        }
    }

    /// Create a linked pair of transports. Each end's `send` surfaces on the
    /// other end's inbound stream.
    pub fn create_linked_pair() -> (InMemoryTransport, InMemoryTransport) {
        let (tx_a, rx_a) = mpsc::unbounded_channel::<RuntimeJson>();
        let (tx_b, rx_b) = mpsc::unbounded_channel::<RuntimeJson>();
        // A's outbound goes into tx_b (B receives on rx_b); A receives on rx_a.
        let a = InMemoryTransport::new(tx_b, rx_a);
        let b = InMemoryTransport::new(tx_a, rx_b);
        (a, b)
    }
}

#[async_trait]
impl Transport for InMemoryTransport {
    async fn start(&self, sink: mpsc::UnboundedSender<TransportEvent>) -> McpResult<()> {
        if self.started.swap(true, Ordering::SeqCst) {
            return Err(McpError::new("InMemoryTransport already started"));
        }
        *self.sink.lock().unwrap() = Some(sink.clone());
        let mut rx = self
            .incoming
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| McpError::new("InMemoryTransport has no inbound channel"))?;
        tokio::spawn(async move {
            while let Some(v) = rx.recv().await {
                if sink.send(TransportEvent::Message(v)).is_err() {
                    return;
                }
            }
            let _ = sink.send(TransportEvent::Close);
        });
        Ok(())
    }

    async fn send(&self, message: RuntimeJson) -> McpResult<()> {
        let guard = self.outgoing.lock().unwrap();
        match guard.as_ref() {
            Some(tx) => tx
                .send(message)
                .map_err(|_| McpError::new("InMemoryTransport peer is gone")),
            None => Err(McpError::new("InMemoryTransport is closed")),
        }
    }

    async fn close(&self) -> McpResult<()> {
        // Dropping `outgoing` closes the peer's inbound channel, so the peer emits Close.
        *self.outgoing.lock().unwrap() = None;
        if let Some(sink) = self.sink.lock().unwrap().take() {
            let _ = sink.send(TransportEvent::Close);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Stdio transport
// ---------------------------------------------------------------------------

/// Spawns the configured command and exchanges newline-delimited JSON-RPC over
/// its stdin/stdout.
pub struct StdioTransport {
    process_context: RuntimeProcessContext,
    command: String,
    args: Vec<String>,
    env: Option<RuntimeEnv>,
    child: AsyncMutex<Option<SpawnedRuntimeProcess>>,
    stdin: AsyncMutex<Option<RuntimeChildStdin>>,
    started: AtomicBool,
}

impl StdioTransport {
    pub fn new(
        process_context: RuntimeProcessContext,
        command: String,
        args: Vec<String>,
        env: Option<RuntimeEnv>,
    ) -> Self {
        StdioTransport {
            process_context,
            command,
            args,
            env,
            child: AsyncMutex::new(None),
            stdin: AsyncMutex::new(None),
            started: AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl Transport for StdioTransport {
    async fn start(&self, sink: mpsc::UnboundedSender<TransportEvent>) -> McpResult<()> {
        if self.started.swap(true, Ordering::SeqCst) {
            return Err(McpError::new("StdioTransport already started"));
        }
        let mut child = self
            .process_context
            .spawn_mcp_stdio(
                RuntimeString::from(self.command.as_str()),
                self.args
                    .iter()
                    .map(|arg| RuntimeString::from(arg.as_str()))
                    .collect(),
                self.env.clone(),
            )
            .await
            .map_err(|e| McpError::new(format!("failed to spawn MCP server: {e}")))?;
        let stdout = child
            .take_stdout()
            .ok_or_else(|| McpError::new("child stdout unavailable"))?;
        // Drain stderr so a chatty child never blocks on a full pipe.
        if let Some(stderr) = child.take_stderr() {
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(_)) = lines.next_line().await {}
            });
        }
        let stdin = child
            .take_stdin()
            .ok_or_else(|| McpError::new("child stdin unavailable"))?;
        *self.stdin.lock().await = Some(stdin);
        *self.child.lock().await = Some(child);

        tokio::spawn(async move {
            let mut reader = BufReader::new(stdout);
            let mut line = Vec::new();
            loop {
                line.clear();
                match reader.read_until(b'\n', &mut line).await {
                    Ok(0) => break,
                    Ok(_) => {
                        let line = NodeBufferUtf8::decode(&line);
                        if trim_ecmascript(&line).is_empty() {
                            continue;
                        }
                        match decode_ndjson(&line) {
                            Ok(v) => {
                                if sink.send(TransportEvent::Message(v)).is_err() {
                                    return;
                                }
                            }
                            Err(e) => {
                                let _ = sink.send(TransportEvent::Error(McpError::new(format!(
                                    "invalid JSON-RPC line: {e}"
                                ))));
                            }
                        }
                    }
                    Err(e) => {
                        let _ = sink.send(TransportEvent::Error(McpError::new(format!(
                            "stdout read error: {e}"
                        ))));
                        break;
                    }
                }
            }
            let _ = sink.send(TransportEvent::Close);
        });
        Ok(())
    }

    async fn send(&self, message: RuntimeJson) -> McpResult<()> {
        let line = encode_ndjson(&message);
        let mut guard = self.stdin.lock().await;
        let stdin = guard
            .as_mut()
            .ok_or_else(|| McpError::new("StdioTransport is not connected"))?;
        stdin
            .write_all(&line.to_node_utf8())
            .await
            .map_err(|e| McpError::new(format!("stdin write error: {e}")))?;
        stdin
            .flush()
            .await
            .map_err(|e| McpError::new(format!("stdin flush error: {e}")))?;
        Ok(())
    }

    async fn close(&self) -> McpResult<()> {
        *self.stdin.lock().await = None;
        if let Some(mut child) = self.child.lock().await.take() {
            child.controller().signal(RuntimeSignal::Kill);
            let _ = child.wait().await;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Remote transports (HTTP + SSE) over reqwest
// ---------------------------------------------------------------------------

/// Build a reqwest header map from a plain string map, ignoring any header that
/// fails to parse (never a hard connect failure).
fn header_map(headers: &HashMap<String, String>) -> reqwest::header::HeaderMap {
    let mut map = reqwest::header::HeaderMap::new();
    for (k, v) in headers {
        if let (Ok(name), Ok(val)) = (
            reqwest::header::HeaderName::from_bytes(k.as_bytes()),
            reqwest::header::HeaderValue::from_str(v),
        ) {
            map.insert(name, val);
        }
    }
    map
}

fn replace_bearer_header(headers: &mut HashMap<String, String>, access_token: &str) {
    headers.retain(|name, _| !name.eq_ignore_ascii_case("authorization"));
    headers.insert(
        "Authorization".to_string(),
        format!("Bearer {access_token}"),
    );
}

fn runtime_oauth_access_token_to_header_host(access_token: &RuntimeString) -> McpResult<String> {
    access_token.try_to_utf8().map_err(|error| {
        McpError::new(format!(
            "MCP OAuth access token is not a Unicode-scalar HTTP header value: {error}"
        ))
    })
}

/// Classify a reqwest error / HTTP status into an [`McpError`] carrying the
/// numeric status when present (so the connect path can detect 401/403).
fn http_error(status: reqwest::StatusCode, context: &str) -> McpError {
    McpError::with_code(
        format!("{context}: HTTP {}", status.as_u16()),
        status.as_u16() as i64,
    )
}

/// The legacy Server-Sent-Events transport: a long-lived GET event stream plus a
/// POST channel whose endpoint the server announces via an `endpoint` event.
pub struct SseTransport {
    url: String,
    headers: Mutex<HashMap<String, String>>,
    client: reqwest::Client,
    message_url: Arc<AsyncMutex<Option<String>>>,
    endpoint_ready: Arc<tokio::sync::Notify>,
    started: AtomicBool,
    sink: Mutex<Option<mpsc::UnboundedSender<TransportEvent>>>,
}

impl SseTransport {
    pub fn new(url: String, headers: HashMap<String, String>) -> Self {
        SseTransport {
            url,
            headers: Mutex::new(headers),
            client: reqwest::Client::new(),
            message_url: Arc::new(AsyncMutex::new(None)),
            endpoint_ready: Arc::new(tokio::sync::Notify::new()),
            started: AtomicBool::new(false),
            sink: Mutex::new(None),
        }
    }
}

#[async_trait]
impl Transport for SseTransport {
    async fn start(&self, sink: mpsc::UnboundedSender<TransportEvent>) -> McpResult<()> {
        if self.started.swap(true, Ordering::SeqCst) {
            return Err(McpError::new("SseTransport already started"));
        }
        *self.sink.lock().unwrap() = Some(sink.clone());
        let headers = self.headers.lock().unwrap().clone();
        let resp = self
            .client
            .get(&self.url)
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .headers(header_map(&headers))
            .send()
            .await
            .map_err(|e| McpError::new(format!("SSE connect failed: {e}")))?;
        if !resp.status().is_success() {
            return Err(http_error(resp.status(), "SSE connect failed"));
        }

        let base = reqwest::Url::parse(&self.url)
            .map_err(|e| McpError::new(format!("invalid SSE url: {e}")))?;
        let message_url = self.message_url.clone();
        let endpoint_ready = self.endpoint_ready.clone();
        tokio::spawn(async move {
            let mut stream = resp.bytes_stream();
            let mut decoder = SseDecoder::new();
            while let Some(chunk) = stream.next().await {
                let bytes = match chunk {
                    Ok(b) => b,
                    Err(e) => {
                        let _ = sink.send(TransportEvent::Error(McpError::new(format!(
                            "SSE read error: {e}"
                        ))));
                        break;
                    }
                };
                for ev in decoder.push_bytes(&bytes) {
                    if ev.event.as_ref().is_some_and(|event| event == "endpoint") {
                        if let Ok(joined) = base.join(&url_terminal_input(&ev.data)) {
                            *message_url.lock().await = Some(joined.to_string());
                            endpoint_ready.notify_waiters();
                        }
                    } else {
                        // "message" (or a data-only event) carries a JSON-RPC payload.
                        if let Ok(v) = decode_ndjson(&ev.data) {
                            if sink.send(TransportEvent::Message(v)).is_err() {
                                return;
                            }
                        }
                    }
                }
            }
            let _ = sink.send(TransportEvent::Close);
        });

        // Wait until the server announced the POST endpoint, so the first send
        // (initialize) has somewhere to go. Bounded so a silent server cannot
        // hang the connect indefinitely.
        if self.message_url.lock().await.is_none() {
            let notified = self.endpoint_ready.notified();
            tokio::select! {
                _ = notified => {}
                _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                    if self.message_url.lock().await.is_none() {
                        return Err(McpError::new("SSE server did not send an endpoint event"));
                    }
                }
            }
        }
        Ok(())
    }

    async fn send(&self, message: RuntimeJson) -> McpResult<()> {
        let target = {
            let guard = self.message_url.lock().await;
            guard.clone()
        };
        let target = target.ok_or_else(|| McpError::new("SSE endpoint is not ready"))?;
        let headers = self.headers.lock().unwrap().clone();
        let body = runtime_json::stringify(&message).to_node_utf8();
        let resp = self
            .client
            .post(&target)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .headers(header_map(&headers))
            .body(body)
            .send()
            .await
            .map_err(|e| McpError::new(format!("SSE POST failed: {e}")))?;
        if !resp.status().is_success() {
            return Err(http_error(resp.status(), "SSE POST failed"));
        }
        Ok(())
    }

    async fn close(&self) -> McpResult<()> {
        if let Some(sink) = self.sink.lock().unwrap().take() {
            let _ = sink.send(TransportEvent::Close);
        }
        Ok(())
    }

    fn replace_bearer_token(&self, access_token: String) -> bool {
        replace_bearer_header(&mut self.headers.lock().unwrap(), &access_token);
        true
    }
}

/// The streamable HTTP transport: each request is POSTed and its reply is read
/// back as JSON or an SSE stream on the same response; the server may assign a
/// session id echoed on subsequent requests.
pub struct HttpTransport {
    url: String,
    headers: Mutex<HashMap<String, String>>,
    client: reqwest::Client,
    session_id: Arc<Mutex<Option<String>>>,
    started: AtomicBool,
    sink: Mutex<Option<mpsc::UnboundedSender<TransportEvent>>>,
    closed: AbortSignal,
}

impl HttpTransport {
    pub fn new(url: String, headers: HashMap<String, String>) -> Self {
        HttpTransport {
            url,
            headers: Mutex::new(headers),
            client: reqwest::Client::new(),
            session_id: Arc::new(Mutex::new(None)),
            started: AtomicBool::new(false),
            sink: Mutex::new(None),
            closed: AbortSignal::new(),
        }
    }
}

#[async_trait]
impl Transport for HttpTransport {
    async fn start(&self, sink: mpsc::UnboundedSender<TransportEvent>) -> McpResult<()> {
        if self.started.swap(true, Ordering::SeqCst) {
            return Err(McpError::new("HttpTransport already started"));
        }
        *self.sink.lock().unwrap() = Some(sink);
        Ok(())
    }

    async fn send(&self, message: RuntimeJson) -> McpResult<()> {
        let sink = self
            .sink
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| McpError::new("HttpTransport is not started"))?;
        let headers = self.headers.lock().unwrap().clone();
        let body = runtime_json::stringify(&message).to_node_utf8();
        let mut req = self
            .client
            .post(&self.url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(
                reqwest::header::ACCEPT,
                "application/json, text/event-stream",
            )
            .headers(header_map(&headers))
            .body(body);
        if let Some(sid) = self.session_id.lock().unwrap().clone() {
            req = req.header("mcp-session-id", sid);
        }
        let resp = tokio::select! {
            biased;
            _ = self.closed.cancelled() => return Err(McpError::new("HttpTransport is closed")),
            response = req.send() => response
                .map_err(|e| McpError::new(format!("HTTP POST failed: {e}")))?,
        };
        if let Some(sid) = resp
            .headers()
            .get("mcp-session-id")
            .and_then(|v| v.to_str().ok())
        {
            let mut session_id = self.session_id.lock().unwrap();
            if !self.closed.is_cancelled() {
                *session_id = Some(sid.to_string());
            }
        }
        let status = resp.status();
        if status == reqwest::StatusCode::ACCEPTED {
            return Ok(());
        }
        if !status.is_success() {
            return Err(http_error(status, "HTTP POST failed"));
        }
        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        if content_type.contains("text/event-stream") {
            let closed = self.closed.clone();
            tokio::spawn(async move {
                let mut stream = resp.bytes_stream();
                let mut decoder = SseDecoder::new();
                loop {
                    let chunk = tokio::select! {
                        biased;
                        _ = closed.cancelled() => return,
                        chunk = stream.next() => chunk,
                    };
                    let Some(chunk) = chunk else { break };
                    let Ok(bytes) = chunk else { break };
                    for ev in decoder.push_bytes(&bytes) {
                        if let Ok(v) = decode_ndjson(&ev.data) {
                            if sink.send(TransportEvent::Message(v)).is_err() {
                                return;
                            }
                        }
                    }
                }
            });
        } else {
            let body = tokio::select! {
                biased;
                _ = self.closed.cancelled() => return Err(McpError::new("HttpTransport is closed")),
                body = resp.bytes() => body
                    .map_err(|e| McpError::new(format!("HTTP response read failed: {e}")))?,
            };
            let body = whatwg_decode_complete(&body);
            let body = decode_ndjson(&body)
                .map_err(|e| McpError::new(format!("HTTP response parse failed: {e}")))?;
            let _ = sink.send(TransportEvent::Message(body));
        }
        Ok(())
    }

    async fn close(&self) -> McpResult<()> {
        // Local completion must not depend on the server acknowledging DELETE.
        // Take the identity once so concurrent/repeated closes are idempotent.
        self.closed.cancel();
        if let Some(sink) = self.sink.lock().unwrap().take() {
            let _ = sink.send(TransportEvent::Close);
        }
        let session = self.session_id.lock().unwrap().take();
        if let Some(sid) = session {
            let headers = self.headers.lock().unwrap().clone();
            let _ = self
                .client
                .delete(&self.url)
                .header("mcp-session-id", sid)
                .headers(header_map(&headers))
                .timeout(HTTP_CLOSE_TIMEOUT)
                .send()
                .await;
        }
        Ok(())
    }

    fn session_id(&self) -> Option<String> {
        self.session_id.lock().unwrap().clone()
    }

    fn replace_bearer_token(&self, access_token: String) -> bool {
        replace_bearer_header(&mut self.headers.lock().unwrap(), &access_token);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // Gated with its only user, the unix-only stdio transport case below.
    #[cfg(unix)]
    use std::time::Duration;

    fn runtime(text: &str) -> RuntimeJson {
        runtime_json::parse(&RuntimeString::from(text)).unwrap()
    }

    macro_rules! json {
        ($($tokens:tt)+) => {
            runtime(stringify!($($tokens)+))
        };
    }

    #[test]
    fn bearer_replacement_removes_case_insensitive_stale_authorization_headers() {
        let mut headers = HashMap::from([
            ("authorization".to_string(), "Bearer stale".to_string()),
            ("x-routing-key".to_string(), "route-a".to_string()),
        ]);

        replace_bearer_header(&mut headers, "fresh");

        let authorization_headers = headers
            .iter()
            .filter(|(name, _)| name.eq_ignore_ascii_case("authorization"))
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(
            authorization_headers,
            vec![("Authorization", "Bearer fresh")]
        );
        assert_eq!(
            headers.get("x-routing-key").map(String::as_str),
            Some("route-a")
        );
    }

    #[test]
    fn ndjson_round_trips_one_object_per_line() {
        let msg = json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize" });
        let line = encode_ndjson(&msg);
        assert!(line.ends_with("\n"));
        assert!(!line.slice_utf16(..line.len_utf16() - 1).contains("\n"));
        let back = decode_ndjson(&line).unwrap();
        assert_eq!(back, msg);
    }

    #[test]
    fn decode_ndjson_trims_trailing_carriage_return() {
        let v = decode_ndjson(&RuntimeString::from("{\"a\":1}\r")).unwrap();
        assert_eq!(v, json!({ "a": 1 }));
    }

    #[test]
    fn ndjson_preserves_lone_surrogates_without_aliasing_literal_marker_text() {
        let decoded = decode_ndjson(&RuntimeString::from(
            r#"{"jsonrpc":"2.0","surrogate":"\ud800","literal":"\uFDD0ud800"}"#,
        ))
        .unwrap();
        assert_ne!(decoded["surrogate"], decoded["literal"]);

        let encoded = encode_ndjson(&decoded);
        assert_eq!(
            encoded,
            RuntimeString::from(
                "{\"jsonrpc\":\"2.0\",\"surrogate\":\"\\ud800\",\"literal\":\"﷐ud800\"}\n"
            )
        );
        assert_eq!(decode_ndjson(&encoded).unwrap(), decoded);
    }

    #[test]
    fn parses_sse_event_fields_and_joins_data() {
        let ev = parse_sse_event(&RuntimeString::from(
            "event: message\ndata: {\"a\":1}\ndata: more",
        ));
        assert_eq!(ev.event, Some(RuntimeString::from("message")));
        assert_eq!(ev.data, "{\"a\":1}\nmore");
    }

    #[test]
    fn parses_endpoint_event_without_leading_space() {
        let ev = parse_sse_event(&RuntimeString::from(
            "event:endpoint\ndata:/messages?sessionId=abc",
        ));
        assert_eq!(ev.event, Some(RuntimeString::from("endpoint")));
        assert_eq!(ev.data, "/messages?sessionId=abc");
    }

    #[test]
    fn sse_decoder_splits_on_blank_lines_across_chunks() {
        let mut d = SseDecoder::new();
        assert!(d.push(&RuntimeString::from("event: message\n")).is_empty());
        let evs = d.push(&RuntimeString::from(
            "data: hello\n\nevent: message\ndata: two\n\n",
        ));
        assert_eq!(evs.len(), 2);
        assert_eq!(evs[0].data, "hello");
        assert_eq!(evs[1].data, "two");
    }

    #[test]
    fn sse_decoder_normalizes_crlf() {
        let mut d = SseDecoder::new();
        let evs = d.push(&RuntimeString::from("data: x\r\n\r\n"));
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].data, "x");
    }

    #[test]
    fn sse_parser_preserves_lone_surrogate_data_units() {
        let block = RuntimeString::from_utf16([
            b'd' as u16,
            b'a' as u16,
            b't' as u16,
            b'a' as u16,
            b':' as u16,
            b' ' as u16,
            b'A' as u16,
            0xd800,
            b'B' as u16,
        ]);
        assert_eq!(
            parse_sse_event(&block).data,
            RuntimeString::from_utf16([b'A' as u16, 0xd800, b'B' as u16])
        );
    }

    #[test]
    fn http_sse_utf8_decoder_keeps_multibyte_sequences_across_chunks() {
        let mut decoder = SseDecoder::new();
        assert!(decoder.push_bytes(b"data: \xf0\x9f").is_empty());
        let events = decoder.push_bytes(b"\x98\x80\n\n");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, RuntimeString::from("😀"));
    }

    #[tokio::test]
    async fn in_memory_pair_delivers_messages_both_ways() {
        let (a, b) = InMemoryTransport::create_linked_pair();
        let (tx_a, mut rx_a) = mpsc::unbounded_channel();
        let (tx_b, mut rx_b) = mpsc::unbounded_channel();
        a.start(tx_a).await.unwrap();
        b.start(tx_b).await.unwrap();
        a.send(json!({ "from": "a" })).await.unwrap();
        b.send(json!({ "from": "b" })).await.unwrap();
        match rx_b.recv().await.unwrap() {
            TransportEvent::Message(v) => assert_eq!(v, json!({ "from": "a" })),
            other => panic!("unexpected {other:?}"),
        }
        match rx_a.recv().await.unwrap() {
            TransportEvent::Message(v) => assert_eq!(v, json!({ "from": "b" })),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn in_memory_close_signals_peer() {
        let (a, b) = InMemoryTransport::create_linked_pair();
        let (tx_a, _rx_a) = mpsc::unbounded_channel();
        let (tx_b, mut rx_b) = mpsc::unbounded_channel();
        a.start(tx_a).await.unwrap();
        b.start(tx_b).await.unwrap();
        a.close().await.unwrap();
        // The peer eventually observes Close.
        loop {
            match rx_b.recv().await {
                Some(TransportEvent::Close) => break,
                Some(_) => continue,
                None => break,
            }
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stdio_transport_uses_the_injected_runtime_process_context() {
        let transport = StdioTransport::new(
            crate::runtime_process::test_runtime_process_context(),
            "/bin/sh".to_string(),
            vec![
                "-c".to_string(),
                "IFS= read -r line; printf '%s\\n' \"$line\"".to_string(),
            ],
            None,
        );
        let (sink, mut events) = mpsc::unbounded_channel();
        transport.start(sink).await.expect("start stdio transport");
        let message = json!({ "jsonrpc": "2.0", "id": 7, "method": "ping" });
        transport.send(message.clone()).await.expect("send request");

        let event = tokio::time::timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("stdio response deadline")
            .expect("stdio response event");
        match event {
            TransportEvent::Message(actual) => assert_eq!(actual, message),
            other => panic!("unexpected stdio event: {other:?}"),
        }
        transport.close().await.expect("close stdio transport");
    }

    #[tokio::test]
    async fn double_start_is_rejected() {
        let (a, _b) = InMemoryTransport::create_linked_pair();
        let (tx1, _r1) = mpsc::unbounded_channel();
        let (tx2, _r2) = mpsc::unbounded_channel();
        a.start(tx1).await.unwrap();
        assert!(a.start(tx2).await.is_err());
    }
}
