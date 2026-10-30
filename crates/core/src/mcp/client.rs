//! The hand-rolled MCP client and the `connect_mcp_server` entry point.
//!
//! [`Client`] speaks JSON-RPC 2.0 over any [`Transport`]: it owns the request-id
//! counter and a pending-request map, drives the `initialize` handshake, answers
//! the server's `roots/list`, and exposes the `tools/list`, `tools/call`,
//! `resources/*`, and `prompts/*` methods with per-call timeouts and
//! progress-notification handling (reset-on-progress bounded by a total ceiling).
//!
//! [`connect_mcp_server`] wires a server config to a transport, runs the
//! handshake, lists and wraps the server's tools, and DEGRADES rather than
//! throwing: any setup/connect/list failure yields a [`ManagedClient`] in state
//! `failed` (or `needs-auth` for a 401/403) with no tools and a no-op close.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::task::task_tracker::TaskTrackerToken;
use tokio_util::task::TaskTracker;
use url::Url;

use crate::runtime_process::RuntimeProcessContext;
use crate::runtime_values::{
    runtime_codec, runtime_json, RuntimeJson as Value, RuntimeObject, RuntimePath, RuntimeString,
};
use crate::types::abort::AbortSignal;
use crate::types::errors::ToolError;
use crate::types::events::ToolDomainEvent;
use crate::types::json::Json;
use crate::types::tool::{
    BoxFuture, McpPromptArgument, McpPromptDescriptor, McpResourceContent, McpResourceDescriptor,
    Tool, ToolProgress,
};

use super::credential_store::{CredentialStore, InMemoryCredentialStore};
use super::elicitation::{parse_mcp_elicitation_request, McpElicitationRouter};
use super::needs_auth_cache::{default_needs_auth_cache, NEEDS_AUTH_TTL_MS};
use super::oauth::McpFetch;
use super::oauth_provider::{
    mcp_oauth_server_key, McpAuthStatus, McpOAuthConfig, McpOAuthProvider, McpOAuthProviderOptions,
};
use super::tool_wrapper::{McpToolCall, WrapMcpToolOptions};
use super::transports::{
    HttpTransport, InMemoryTransport, McpError, OAuthTransport, SseTransport, StdioTransport,
    Transport, TransportEvent,
};
use super::types::{
    McpCallResult, McpConnectionState, McpContentBlock, McpEmbeddedResource,
    McpResourceTemplateDescriptor, McpServerConfig, McpServerInfo, McpStartupFailureReason,
    McpToolAnnotations, McpToolDescriptor,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// The MCP protocol version advertised in the initialize handshake.
const PROTOCOL_VERSION: &str = "2025-11-25";
/// Client identity advertised to MCP servers during initialization.
const CLIENT_NAME: &str = "klaus-agent";
const CLIENT_VERSION: &str = "0.1.0";

/// Default loopback redirect the host's authorize boundary receives the OAuth code on.
const DEFAULT_OAUTH_REDIRECT_URI: &str = "http://127.0.0.1:33418/callback";
/// Default time budget for the initial connect + initialize handshake.
const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 30_000;
/// Default per-call timeout for `call_tool`.
const DEFAULT_CALL_TIMEOUT_MS: u64 = 60_000;
/// Allow cancellation replies to reach a responsive peer before dropping its request tasks.
const SERVER_REQUEST_CLOSE_GRACE: Duration = Duration::from_secs(1);
/// Hard ceiling for a `call_tool` that keeps reporting progress. Every
/// `notifications/progress` refreshes the per-call timeout so a genuinely long
/// server tool isn't killed at `DEFAULT_CALL_TIMEOUT_MS` while it's actively
/// working — but a server that streams progress forever still can't hang the
/// turn past this total cap.
const MAX_TOTAL_CALL_TIMEOUT_MS: u64 = 10 * 60_000;
/// Server `instructions` longer than this are truncated before injection.
const MAX_MCP_INSTRUCTIONS: usize = 2048;

fn runtime_object<K, I>(entries: I) -> Value
where
    K: Into<RuntimeString>,
    I: IntoIterator<Item = (K, Value)>,
{
    let mut object = RuntimeObject::new();
    for (key, value) in entries {
        object.insert(key, value);
    }
    Value::Object(object)
}

fn runtime_array(values: impl IntoIterator<Item = Value>) -> Value {
    Value::Array(values.into_iter().collect())
}

fn runtime_concat(parts: impl IntoIterator<Item = RuntimeString>) -> RuntimeString {
    parts
        .into_iter()
        .fold(RuntimeString::default(), |output, part| {
            output.concat(&part)
        })
}

// ---------------------------------------------------------------------------
// Public surface: options, handler seam, managed client
// ---------------------------------------------------------------------------

/// What the host is asked to do to complete an MCP OAuth flow: turn a URL into a code.
#[derive(Debug, Clone)]
pub struct McpAuthorizeRequest {
    pub server_name: String,
    /// The authorization URL to open (carries PKCE challenge + state).
    pub authorization_url: String,
    /// The redirect_uri the engine registered; the host must capture the code there.
    pub redirect_uri: String,
}

/// The host's answer: the captured authorization code, or a decline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpAuthorizeResponse {
    Code { code: String, state: Option<String> },
    Decline { reason: Option<String> },
}

/// Host-delegated boundary for the one interactive step of MCP OAuth: opening
/// the authorization URL and capturing the redirect's `code`. Keeping it
/// injectable is what lets the core stay headless.
#[async_trait::async_trait]
pub trait McpAuthorizeHandler: Send + Sync {
    async fn authorize(
        &self,
        request: McpAuthorizeRequest,
        signal: AbortSignal,
    ) -> McpAuthorizeResponse;
}

/// Observes an authorization URL emitted by the core OAuth state machine.
pub type McpAuthorizationUrlHandler = Arc<dyn Fn(String, Url) + Send + Sync>;

/// Options for [`connect_mcp_server`].
#[derive(Clone)]
pub struct ConnectMcpOptions {
    pub process_context: RuntimeProcessContext,
    pub connect_timeout_ms: Option<u64>,
    pub call_timeout_ms: Option<u64>,
    /// Workspace root reported to servers via `roots/list` (as `file://<cwd>`).
    pub cwd: Option<RuntimePath>,
    /// Resolves a stored bearer token (keyed `mcp:<server>:token`) for http auth.
    pub credential_store: Option<Arc<dyn CredentialStore>>,
    /// OAuth: completes the interactive flow (open URL -> capture code).
    pub mcp_authorize_handler: Option<Arc<dyn McpAuthorizeHandler>>,
    /// Aborts an in-progress interactive authorization (forwarded to the handler).
    pub signal: Option<AbortSignal>,
    /// OAuth: the redirect_uri the host receives the authorization code on.
    pub oauth_redirect_uri: Option<String>,
    /// OAuth: observes the PKCE authorization URL before interactive completion.
    pub on_mcp_authorization_url: Option<McpAuthorizationUrlHandler>,
    /// OAuth: directory for cross-process refresh lockfiles (default `~/.klaus`).
    pub oauth_lock_dir: Option<String>,
    /// OAuth: bypass the needs-auth cache so a connect is actually attempted.
    pub skip_needs_auth_cache: Option<bool>,
    /// Injectable OAuth HTTP seam; production uses reqwest.
    pub oauth_fetch: Option<Arc<dyn McpFetch>>,
    /// Session-scoped router for server-initiated structured elicitation.
    pub mcp_elicitation_router: Option<McpElicitationRouter>,
}

impl ConnectMcpOptions {
    pub fn new(process_context: RuntimeProcessContext) -> Self {
        Self {
            process_context,
            connect_timeout_ms: None,
            call_timeout_ms: None,
            cwd: None,
            credential_store: None,
            mcp_authorize_handler: None,
            signal: None,
            oauth_redirect_uri: None,
            on_mcp_authorization_url: None,
            oauth_lock_dir: None,
            skip_needs_auth_cache: None,
            oauth_fetch: None,
            mcp_elicitation_router: None,
        }
    }
}

struct ClientLease {
    client: Client,
}

impl Drop for ClientLease {
    fn drop(&mut self) {
        let client = self.client.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move { safe_close_client(&client).await });
        } else {
            std::thread::spawn(move || {
                if let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    runtime.block_on(safe_close_client(&client));
                }
            });
        }
    }
}

/// A connected (or failed) MCP server, exposing the tools it provides plus a
/// `close` to tear the transport down.
#[derive(Clone)]
pub struct ManagedClient {
    declaration: Option<McpServerConfig>,
    pub name: RuntimeString,
    pub state: McpConnectionState,
    /// Resources the server advertises (empty if unsupported/none).
    pub resources: Vec<McpResourceDescriptor>,
    /// Resource TEMPLATES the server advertises (empty if unsupported/none).
    /// Kept apart from `resources`: a template is a URI pattern the caller has
    /// to fill in, so listing it as a concrete resource yields a row nothing can
    /// read.
    pub resource_templates: Vec<McpResourceTemplateDescriptor>,
    /// Prompts the server advertises (empty if unsupported/none).
    pub prompts: Vec<McpPromptDescriptor>,
    /// The server's `initialize` instructions (truncated), if any.
    pub instructions: Option<RuntimeString>,
    /// The server's `initialize` `serverInfo`, if it reported one.
    pub server_info: Option<McpServerInfo>,
    /// How this connection authenticates. Independent of `state`: an OAuth
    /// server that is `Ready` still reports `OAuth` here, and the host needs
    /// both facts to decide whether an authorize action belongs on the row.
    pub auth_status: McpAuthStatus,
    /// For a `needs-auth` OAuth server, the authorization URL, if one was produced.
    pub authorization_url: Option<String>,
    /// Why the dial settled the way it did, when the cause is actionable. A host
    /// cannot branch on a free-text error, so the classification travels
    /// alongside it rather than being re-derived from the message.
    pub failure_reason: Option<McpStartupFailureReason>,
    /// The failure text, verbatim. Without it a failed row can only say
    /// "failed", which is indistinguishable from every other failure.
    pub error: Option<RuntimeString>,
    /// The server's tools, wrapped as engine tools. Shared so a mid-session
    /// `tools/list_changed` refetch can replace the set in place.
    tools: Arc<Mutex<Vec<Arc<dyn Tool>>>>,
    raw_tools: Option<Arc<Mutex<Vec<McpToolDescriptor>>>>,
    tool_config: Option<Arc<Mutex<McpServerConfig>>>,
    tool_refresh_lock: Option<Arc<Mutex<()>>>,
    lease: Option<Arc<ClientLease>>,
    client: Option<Client>,
    call_timeout_ms: u64,
}

impl ManagedClient {
    pub(crate) fn declaration(&self) -> Option<McpServerConfig> {
        self.tool_config
            .as_ref()
            .map(|config| config.lock().unwrap().clone())
            .or_else(|| self.declaration.clone())
    }
    /// The current wrapped tool set (a snapshot; a live re-list may replace it).
    pub fn tools(&self) -> Vec<Arc<dyn Tool>> {
        self.tools.lock().unwrap().clone()
    }

    pub(crate) fn reconfigure_tool_visibility(&self, config: &McpServerConfig) -> bool {
        let (Some(client), Some(raw), Some(current), Some(lock), Some(lease)) = (
            &self.client,
            &self.raw_tools,
            &self.tool_config,
            &self.tool_refresh_lock,
            &self.lease,
        ) else {
            return false;
        };
        let _guard = lock.lock().unwrap();
        *current.lock().unwrap() = config.clone();
        let listed = raw.lock().unwrap().clone();
        let wrapped = wrap_listed(
            client,
            &self.name.try_to_utf8().unwrap_or_default(),
            self.call_timeout_ms,
            config_always_load(config),
            listed,
            config,
            lease.clone(),
        );
        *self.tools.lock().unwrap() = wrapped;
        true
    }

    /// Read a resource's contents by uri.
    pub async fn read_resource(&self, uri: &RuntimeString) -> Vec<McpResourceContent> {
        match &self.client {
            Some(c) => read_resource_safe(c, uri, self.call_timeout_ms).await,
            None => Vec::new(),
        }
    }

    /// Call one of this server's tools by its RAW (server-declared) name.
    ///
    /// This is the HOST-driven path (a UI invoking a tool directly), distinct
    /// from the model-driven wrapped tools: it takes the raw name and returns the
    /// normalized result, with no permission gate or exposure logic, because the
    /// caller is a human action, not the model. A degraded client has no live
    /// connection, so the call reports that rather than pretending to run.
    pub async fn call_tool(
        &self,
        tool: &RuntimeString,
        args: Json,
        signal: Option<AbortSignal>,
    ) -> Result<McpCallResult, RuntimeString> {
        match &self.client {
            Some(c) => c
                .call_tool(tool, args, self.call_timeout_ms, None, signal)
                .await
                .map_err(|e| e.message),
            None => Err(RuntimeString::from(
                "mcp_server_unavailable: no live connection",
            )),
        }
    }

    /// Render a server prompt by name.
    pub async fn get_prompt(
        &self,
        name: &RuntimeString,
        args: Option<RuntimeObject>,
    ) -> RuntimeString {
        match &self.client {
            Some(c) => get_prompt_safe(c, name, args, self.call_timeout_ms).await,
            None => RuntimeString::default(),
        }
    }

    /// Tear the transport down. Idempotent; a no-op for a degraded client.
    pub async fn close(&self) {
        if let Some(c) = &self.client {
            safe_close_client(c).await;
        }
    }
}

/// Store/replace the bearer token used for an http MCP server's Authorization.
pub async fn store_mcp_token(store: &Arc<dyn CredentialStore>, server_name: &str, token: &str) {
    let _ = store.set(&format!("mcp:{server_name}:token"), token).await;
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// A progress report forwarded from a `notifications/progress` message.
#[derive(Debug, Clone)]
pub struct ProgressInfo {
    pub progress: f64,
    pub total: Option<f64>,
    pub message: Option<RuntimeString>,
}

/// A progress callback stored per in-flight request, keyed by its progress token.
type ProgressCb = Arc<dyn Fn(ProgressInfo) -> BoxFuture<'static, ()> + Send + Sync>;
/// The request owns already accepted progress even if cancellation wins its
/// response race. Registration and close share a short synchronous lock; the
/// pump polls each callback itself and no forwarding task is created here.
struct ProgressDelivery {
    admission: Mutex<Option<TaskTracker>>,
}

impl ProgressDelivery {
    fn new() -> Self {
        Self {
            admission: Mutex::new(Some(TaskTracker::new())),
        }
    }

    fn enter(&self) -> Option<TaskTrackerToken> {
        self.admission
            .lock()
            .unwrap()
            .as_ref()
            .map(TaskTracker::token)
    }

    async fn close_and_wait(&self) {
        let tracker = self.admission.lock().unwrap().take();
        if let Some(tracker) = tracker {
            tracker.close();
            tracker.wait().await;
        }
    }
}

/// A notification handler (e.g. `tools/list_changed`), dispatched fire-and-forget.
type NotifHandler = Arc<dyn Fn(Value) -> BoxFuture<'static, ()> + Send + Sync>;
/// A server-to-client request handler (e.g. `roots/list`). The second argument
/// is cancelled when the server sends `notifications/cancelled` for this
/// request id, letting long-lived handlers (elicitation) settle early.
type ReqHandler = Arc<
    dyn Fn(Value, AbortSignal) -> BoxFuture<'static, Result<Value, RequestHandlerError>>
        + Send
        + Sync,
>;

struct RequestHandlerError {
    code: i64,
    message: RuntimeString,
}

impl RequestHandlerError {
    fn invalid_params(message: impl Into<RuntimeString>) -> Self {
        Self {
            code: -32602,
            message: message.into(),
        }
    }

    fn internal(message: impl Into<RuntimeString>) -> Self {
        Self {
            code: -32603,
            message: message.into(),
        }
    }
}

/// Per-request tuning: the timeout, an optional abort signal, an optional
/// progress callback, and the reset-on-progress ceiling.
#[derive(Clone, Default)]
pub struct RequestOptions {
    pub timeout_ms: u64,
    pub signal: Option<AbortSignal>,
    pub on_progress: Option<ProgressCb>,
    pub reset_timeout_on_progress: bool,
    pub max_total_timeout_ms: Option<u64>,
}

struct ClientInner {
    transport: Mutex<Option<Arc<dyn Transport>>>,
    next_id: AtomicI64,
    pending: Mutex<HashMap<i64, oneshot::Sender<Result<Value, McpError>>>>,
    progress: Mutex<HashMap<i64, ProgressCb>>,
    notif_handlers: Mutex<HashMap<String, NotifHandler>>,
    request_handlers: Mutex<HashMap<String, ReqHandler>>,
    request_tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    close_signal: AbortSignal,
    close_completion: std::sync::OnceLock<watch::Receiver<bool>>,
    supports_elicitation: bool,
    /// Session-wide "a user is answering an elicitation" flag; every request's
    /// timeout pauses while it is set.
    elicitation_pause: Option<watch::Receiver<bool>>,
    /// In-flight server-to-client requests by JSON-RPC id, so a server
    /// `notifications/cancelled` can settle the matching one (Codex cancels its
    /// pending elicitation request the same way).
    server_requests: Mutex<HashMap<String, AbortSignal>>,
    server_caps: Mutex<Option<Value>>,
    instructions: Mutex<Option<RuntimeString>>,
    server_info: Mutex<Option<McpServerInfo>>,
}

/// A JSON-RPC MCP client. Cheap to clone (an `Arc` handle).
#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

impl Client {
    /// Construct a client advertising our capabilities and a `roots/list` handler
    /// that reports the workspace root. `cwd` defaults to the process cwd.
    pub fn new(cwd: Option<RuntimePath>) -> Client {
        Self::new_for_server(cwd, String::new(), None)
    }

    fn new_for_server(
        cwd: Option<RuntimePath>,
        server_name: String,
        elicitation_router: Option<McpElicitationRouter>,
    ) -> Client {
        let root = cwd.unwrap_or_else(|| {
            std::env::current_dir()
                .map(|path| crate::fs::node_path::runtime_path_from_native(&path))
                .unwrap_or_default()
        });
        let root_uri = RuntimeString::from("file://").concat(root.as_runtime_string());
        let elicitation_router = elicitation_router.unwrap_or_else(McpElicitationRouter::disabled);
        let supports_elicitation = elicitation_router.has_presenter();
        // Only a router that can actually surface a card can pause the budget;
        // a disabled router never enters the pause state.
        let elicitation_pause =
            supports_elicitation.then(|| elicitation_router.pause_state().subscribe());
        let close_signal = AbortSignal::new();
        let inner = Arc::new(ClientInner {
            transport: Mutex::new(None),
            next_id: AtomicI64::new(0),
            pending: Mutex::new(HashMap::new()),
            progress: Mutex::new(HashMap::new()),
            notif_handlers: Mutex::new(HashMap::new()),
            request_handlers: Mutex::new(HashMap::new()),
            request_tasks: Mutex::new(Vec::new()),
            close_signal: close_signal.clone(),
            close_completion: std::sync::OnceLock::new(),
            supports_elicitation,
            elicitation_pause,
            server_requests: Mutex::new(HashMap::new()),
            server_caps: Mutex::new(None),
            instructions: Mutex::new(None),
            server_info: Mutex::new(None),
        });
        let ru = root_uri;
        inner.request_handlers.lock().unwrap().insert(
            "roots/list".to_string(),
            Arc::new(move |_params, _cancel: AbortSignal| {
                let ru = ru.clone();
                Box::pin(async move {
                    Ok(runtime_object([(
                        "roots",
                        runtime_array([runtime_object([("uri", Value::String(ru))])]),
                    )]))
                }) as BoxFuture<'static, Result<Value, RequestHandlerError>>
            }),
        );
        for method in ["elicitation/create", "openai/form"] {
            let router = elicitation_router.clone();
            let server_name = RuntimeString::from(server_name.clone());
            let close_signal = close_signal.clone();
            let method = method.to_string();
            let method_for_handler = method.clone();
            inner.request_handlers.lock().unwrap().insert(
                method,
                Arc::new(move |params, cancel_signal: AbortSignal| {
                    let router = router.clone();
                    let server_name = server_name.clone();
                    let close_signal = close_signal.clone();
                    let method = method_for_handler.clone();
                    Box::pin(async move {
                        let request = parse_mcp_elicitation_request(&method, params)
                            .map_err(RequestHandlerError::invalid_params)?;
                        runtime_codec::encode_value(
                            &router
                                .route(server_name, request, close_signal, cancel_signal)
                                .await,
                        )
                        .map_err(|error| RequestHandlerError::internal(error.to_string()))
                    }) as BoxFuture<'static, Result<Value, RequestHandlerError>>
                }),
            );
        }
        Client { inner }
    }

    /// Start the transport, run the initialize handshake, and send the
    /// `initialized` notification.
    pub async fn connect(
        &self,
        transport: Arc<dyn Transport>,
        opts: RequestOptions,
    ) -> Result<(), McpError> {
        *self.inner.transport.lock().unwrap() = Some(transport.clone());
        let (tx, rx) = mpsc::unbounded_channel();
        transport.start(tx).await?;
        let inner = self.inner.clone();
        tokio::spawn(async move { pump(inner, rx).await });

        let mut capabilities = runtime_object([("roots", Value::Object(RuntimeObject::new()))]);
        if self.inner.supports_elicitation {
            capabilities.as_object_mut().unwrap().insert(
                "elicitation",
                runtime_object([
                    ("form", Value::Object(RuntimeObject::new())),
                    ("url", Value::Object(RuntimeObject::new())),
                ]),
            );
            capabilities.as_object_mut().unwrap().insert(
                "extensions",
                runtime_object([("openai/form", Value::Object(RuntimeObject::new()))]),
            );
        }
        let params = runtime_object([
            ("protocolVersion", Value::from(PROTOCOL_VERSION)),
            ("capabilities", capabilities),
            (
                "clientInfo",
                runtime_object([
                    ("name", Value::from(CLIENT_NAME)),
                    ("version", Value::from(CLIENT_VERSION)),
                ]),
            ),
        ]);
        let result = request(
            &self.inner,
            "initialize",
            Some(params),
            RequestOptions {
                timeout_ms: opts.timeout_ms,
                signal: opts.signal.clone(),
                ..Default::default()
            },
        )
        .await?;
        if let Some(caps) = result.get("capabilities") {
            *self.inner.server_caps.lock().unwrap() = Some(caps.clone());
        }
        if let Some(instr) = result.get("instructions").and_then(Value::as_str) {
            *self.inner.instructions.lock().unwrap() = Some(instr.clone());
        }
        // `serverInfo` is the only place a host learns which BUILD answered. It is
        // recorded even when every field is absent so a server that reports an
        // empty object is distinguishable from one that reported nothing.
        if let Some(info) = result.get("serverInfo") {
            *self.inner.server_info.lock().unwrap() = Some(McpServerInfo {
                name: info.get("name").and_then(Value::as_str).cloned(),
                version: info.get("version").and_then(Value::as_str).cloned(),
                title: info.get("title").and_then(Value::as_str).cloned(),
            });
        }
        notify(&self.inner, "notifications/initialized", None).await?;
        Ok(())
    }

    /// The server's advertised capabilities (from the initialize result), if any.
    pub fn server_capabilities(&self) -> Option<Value> {
        self.inner.server_caps.lock().unwrap().clone()
    }

    /// The server's initialize instructions, if any.
    pub fn instructions(&self) -> Option<RuntimeString> {
        self.inner.instructions.lock().unwrap().clone()
    }

    /// The server's `initialize` `serverInfo`, if it reported one.
    pub fn server_info(&self) -> Option<McpServerInfo> {
        self.inner.server_info.lock().unwrap().clone()
    }

    /// Register a notification handler (replaces any prior handler for `method`).
    pub fn set_notification_handler(&self, method: &str, handler: NotifHandler) {
        self.inner
            .notif_handlers
            .lock()
            .unwrap()
            .insert(method.to_string(), handler);
    }

    /// `tools/list` -> the raw tool descriptors.
    pub async fn list_tools(
        &self,
        opts: RequestOptions,
    ) -> Result<Vec<McpToolDescriptor>, McpError> {
        let result = request(&self.inner, "tools/list", None, opts).await?;
        let tools = result
            .get("tools")
            .and_then(Value::as_array)
            .map(|arr| arr.iter().map(parse_tool_descriptor).collect())
            .unwrap_or_default();
        Ok(tools)
    }

    /// `tools/call` -> a normalized [`McpCallResult`].
    pub async fn call_tool(
        &self,
        name: &RuntimeString,
        args: Json,
        timeout_ms: u64,
        on_progress: Option<ToolProgress>,
        signal: Option<AbortSignal>,
    ) -> Result<McpCallResult, McpError> {
        // Forward MCP progress to the engine's tool event stream as status lines.
        let progress_delivery = Arc::new(ProgressDelivery::new());
        let mcp_progress: Option<ProgressCb> = on_progress.map(|op| {
            let progress_delivery = progress_delivery.clone();
            Arc::new(move |info: ProgressInfo| -> BoxFuture<'static, ()> {
                let Some(token) = progress_delivery.enter() else {
                    return Box::pin(async {});
                };
                let pct = match info.total {
                    Some(t) if t > 0.0 => format!("{}/{}", fmt_num(info.progress), fmt_num(t)),
                    _ => fmt_num(info.progress),
                };
                let msg = match &info.message {
                    Some(message) => {
                        runtime_concat([RuntimeString::from(format!("{pct} ")), message.clone()])
                    }
                    None => RuntimeString::from(pct),
                };
                let progress = op(ToolDomainEvent::Status {
                    message: RuntimeString::from("mcp progress: ").concat(&msg),
                });
                Box::pin(async move {
                    let _token = token;
                    progress.await;
                })
            }) as ProgressCb
        });
        let reset = mcp_progress.is_some();
        let opts = RequestOptions {
            timeout_ms,
            signal,
            on_progress: mcp_progress,
            reset_timeout_on_progress: reset,
            max_total_timeout_ms: if reset {
                Some(MAX_TOTAL_CALL_TIMEOUT_MS)
            } else {
                None
            },
        };
        let raw = request(
            &self.inner,
            "tools/call",
            Some(runtime_object([
                ("name", Value::String(name.clone())),
                ("arguments", args),
            ])),
            opts,
        )
        .await;
        // request sends cancellation before this wait, so physical cancellation
        // remains reachable while accepted progress is waiting for persistence.
        progress_delivery.close_and_wait().await;
        let raw = raw?;
        Ok(normalize_call_result(&raw))
    }

    async fn list_resources(&self, timeout_ms: u64) -> Result<Value, McpError> {
        request(
            &self.inner,
            "resources/list",
            None,
            timeout_opts(timeout_ms),
        )
        .await
    }

    async fn list_resource_templates(&self, timeout_ms: u64) -> Result<Value, McpError> {
        request(
            &self.inner,
            "resources/templates/list",
            None,
            timeout_opts(timeout_ms),
        )
        .await
    }

    async fn read_resource(&self, uri: &RuntimeString, timeout_ms: u64) -> Result<Value, McpError> {
        request(
            &self.inner,
            "resources/read",
            Some(runtime_object([("uri", Value::String(uri.clone()))])),
            timeout_opts(timeout_ms),
        )
        .await
    }

    async fn list_prompts(&self, timeout_ms: u64) -> Result<Value, McpError> {
        request(&self.inner, "prompts/list", None, timeout_opts(timeout_ms)).await
    }

    async fn get_prompt(
        &self,
        name: &RuntimeString,
        args: Option<RuntimeObject>,
        timeout_ms: u64,
    ) -> Result<Value, McpError> {
        let mut params = RuntimeObject::new();
        params.insert("name", Value::String(name.clone()));
        if let Some(args) = args {
            params.insert("arguments", Value::Object(args));
        }
        request(
            &self.inner,
            "prompts/get",
            Some(Value::Object(params)),
            timeout_opts(timeout_ms),
        )
        .await
    }

    /// Close the transport and fail any in-flight requests. Idempotent.
    pub async fn close(&self) -> Result<(), McpError> {
        self.inner.close_signal.cancel();
        // Drop notification handlers so their captured client handles don't keep
        // the connection alive.
        self.inner.notif_handlers.lock().unwrap().clear();
        let drained: Vec<_> = self.inner.pending.lock().unwrap().drain().collect();
        for (_, tx) in drained {
            let _ = tx.send(Err(McpError::new("client closed")));
        }
        let mut completion = self
            .inner
            .close_completion
            .get_or_init(|| {
                let (send, receive) = watch::channel(false);
                let inner = self.inner.clone();
                // Connection ownership outlives any individual close waiter.
                // Every caller must join the same request/transport drain.
                tokio::spawn(async move {
                    drain_request_tasks(&inner).await;
                    let transport = inner.transport.lock().unwrap().take();
                    if let Some(transport) = transport {
                        let _ = transport.close().await;
                    }
                    let _ = send.send(true);
                });
                receive
            })
            .clone();
        loop {
            if *completion.borrow() {
                return Ok(());
            }
            if completion.changed().await.is_err() {
                return Err(McpError::new("client close ended without a completion"));
            }
        }
    }
}

fn timeout_opts(timeout_ms: u64) -> RequestOptions {
    RequestOptions {
        timeout_ms,
        ..Default::default()
    }
}

/// Pump inbound transport events into the pending map / handlers until close.
async fn pump(inner: Arc<ClientInner>, mut rx: mpsc::UnboundedReceiver<TransportEvent>) {
    loop {
        let ev = tokio::select! {
            biased;
            _ = inner.close_signal.cancelled() => break,
            ev = rx.recv() => ev,
        };
        let Some(ev) = ev else { break };
        match ev {
            TransportEvent::Message(v) => dispatch(&inner, v).await,
            TransportEvent::Error(_) => {}
            TransportEvent::Close => break,
        }
    }
    // Fail any request still waiting on this connection.
    let drained: Vec<_> = inner.pending.lock().unwrap().drain().collect();
    for (_, tx) in drained {
        let _ = tx.send(Err(McpError::new("connection closed")));
    }
    // Settle in-flight server requests (elicitation waits) too.
    let requests: Vec<_> = inner.server_requests.lock().unwrap().drain().collect();
    for (_, signal) in requests {
        signal.cancel();
    }
    inner.close_signal.cancel();
    let _ = Client { inner }.close().await;
}

async fn drain_request_tasks(inner: &Arc<ClientInner>) {
    loop {
        let tasks = std::mem::take(&mut *inner.request_tasks.lock().unwrap());
        if tasks.is_empty() {
            break;
        }
        for task in tasks {
            let _ = task.await;
        }
    }
}

/// JSON-RPC ids arrive as numbers or strings; the server-request map and
/// `notifications/cancelled` params must agree on one key shape.
fn request_id_key(id: &Value) -> String {
    match id.as_i64() {
        Some(number) => number.to_string(),
        None => id
            .as_str()
            .map(|text| String::from_utf8_lossy(&text.to_node_utf8()).into_owned())
            .unwrap_or_default(),
    }
}

/// Route one inbound JSON-RPC message: a response resolves a pending request; a
/// server request is answered via a registered handler; a notification is
/// dispatched (progress routed by token).
async fn dispatch(inner: &Arc<ClientInner>, v: Value) {
    let is_response =
        v.get("id").is_some() && (v.get("result").is_some() || v.get("error").is_some());
    if is_response {
        if let Some(id) = v.get("id").and_then(Value::as_i64) {
            inner.progress.lock().unwrap().remove(&id);
            let waiter = inner.pending.lock().unwrap().remove(&id);
            if let Some(tx) = waiter {
                if let Some(err) = v.get("error") {
                    let code = err.get("code").and_then(Value::as_i64);
                    let msg = err
                        .get("message")
                        .and_then(Value::as_str)
                        .cloned()
                        .unwrap_or_else(|| RuntimeString::from("MCP error"));
                    let _ = tx.send(Err(McpError {
                        message: msg,
                        code,
                        aborted: false,
                    }));
                } else {
                    let _ = tx.send(Ok(v.get("result").cloned().unwrap_or(Value::Null)));
                }
            }
        }
        return;
    }

    // A server-to-client request carries both a method and an id.
    if v.get("method").is_some() && v.get("id").is_some() {
        // Serialize task admission with the drain snapshot. Once close signals,
        // no request can be registered after the shared cleanup has joined it.
        let mut request_tasks = inner.request_tasks.lock().unwrap();
        if inner.close_signal.is_cancelled() {
            return;
        }
        let method = v
            .get("method")
            .and_then(Value::as_str)
            .cloned()
            .unwrap_or_default();
        let id = v.get("id").cloned().unwrap_or(Value::Null);
        let params = v.get("params").cloned().unwrap_or(Value::Null);
        let handler = inner
            .request_handlers
            .lock()
            .unwrap()
            .iter()
            .find(|(candidate, _)| method == candidate.as_str())
            .map(|(_, handler)| handler.clone());
        // Track the in-flight server request so `notifications/cancelled`
        // (sent by hosts when their question dies) can settle it.
        let request_key = request_id_key(&id);
        let request_signal = AbortSignal::new();
        inner
            .server_requests
            .lock()
            .unwrap()
            .insert(request_key.clone(), request_signal.clone());
        let request_inner = inner.clone();
        let task = tokio::spawn(async move {
            let response = async {
                if let Some(handler) = handler {
                    match handler(params, request_signal).await {
                        Ok(result) => runtime_object([
                            ("jsonrpc", Value::from("2.0")),
                            ("id", id),
                            ("result", result),
                        ]),
                        Err(error) => runtime_object([
                            ("jsonrpc", Value::from("2.0")),
                            ("id", id),
                            (
                                "error",
                                runtime_object([
                                    ("code", Value::from(error.code)),
                                    ("message", Value::String(error.message)),
                                ]),
                            ),
                        ]),
                    }
                } else {
                    runtime_object([
                        ("jsonrpc", Value::from("2.0")),
                        ("id", id),
                        (
                            "error",
                            runtime_object([
                                ("code", Value::from(-32601_i64)),
                                (
                                    "message",
                                    Value::String(
                                        RuntimeString::from("Method not found: ").concat(&method),
                                    ),
                                ),
                            ]),
                        ),
                    ])
                }
            };
            let respond = async {
                let response = response.await;
                let transport = request_inner.transport.lock().unwrap().clone();
                if let Some(transport) = transport {
                    let _ = transport.send(response).await;
                }
            };
            tokio::pin!(respond);
            // A responsive peer still receives its cancelled elicitation reply.
            // A stuck handler or response write gets only a bounded grace period.
            tokio::select! {
                biased;
                _ = request_inner.close_signal.cancelled() => {
                    let _ = tokio::time::timeout(SERVER_REQUEST_CLOSE_GRACE, &mut respond).await;
                }
                _ = &mut respond => {}
            }
            request_inner
                .server_requests
                .lock()
                .unwrap()
                .remove(&request_key);
        });
        request_tasks.push(task);
        return;
    }

    // A notification has a method and no id.
    if let Some(method) = v.get("method").and_then(Value::as_str) {
        if method == "notifications/cancelled" {
            // The server gave up on one of its requests (its question died);
            // settle the matching in-flight handler so the pending elicitation
            // resolves as cancelled and the host's card closes.
            let params = v.get("params").cloned().unwrap_or(Value::Null);
            let Some(request_id) = params.get("requestId") else {
                return;
            };
            let key = request_id_key(request_id);
            if let Some(signal) = inner.server_requests.lock().unwrap().remove(&key) {
                signal.cancel();
            }
            return;
        }
        if method == "notifications/progress" {
            let params = v.get("params").cloned().unwrap_or(Value::Null);
            if let Some(token) = params.get("progressToken").and_then(Value::as_i64) {
                let cb = inner.progress.lock().unwrap().get(&token).cloned();
                if let Some(cb) = cb {
                    cb(ProgressInfo {
                        progress: params
                            .get("progress")
                            .and_then(Value::as_f64)
                            .unwrap_or(0.0),
                        total: params.get("total").and_then(Value::as_f64),
                        message: params.get("message").and_then(Value::as_str).cloned(),
                    })
                    .await;
                }
            }
            return;
        }
        let handler = inner
            .notif_handlers
            .lock()
            .unwrap()
            .iter()
            .find(|(candidate, _)| method == candidate.as_str())
            .map(|(_, handler)| handler.clone());
        if let Some(h) = handler {
            let params = v.get("params").cloned().unwrap_or(Value::Null);
            tokio::spawn(h(params));
        }
    }
}

/// Attach `_meta.progressToken` to request params without any fallible unwrap.
///
/// Caller-supplied params are external data, so anything that is not an object
/// is replaced by a fresh object, and an existing object-valued `_meta` is
/// preserved and extended in place rather than overwritten.
fn attach_progress_token(params: Option<Value>, id: i64) -> Value {
    let mut obj = match params {
        Some(v) if v.is_object() => v,
        _ => Value::Object(RuntimeObject::new()),
    };
    if let Some(map) = obj.as_object_mut() {
        if !map.get("_meta").is_some_and(Value::is_object) {
            map.insert("_meta", Value::Object(RuntimeObject::new()));
        }
        if let Some(meta) = map.get_mut("_meta").and_then(Value::as_object_mut) {
            meta.insert("progressToken", Value::from(id));
        }
    }
    obj
}

/// Issue a JSON-RPC request and await its response, honoring the timeout (reset
/// on progress up to a total ceiling) and the abort signal.
async fn request(
    inner: &Arc<ClientInner>,
    method: &str,
    params: Option<Value>,
    opts: RequestOptions,
) -> Result<Value, McpError> {
    let id = inner.next_id.fetch_add(1, Ordering::SeqCst);
    let (tx, rx) = oneshot::channel();

    // Attach the progress token to params when a progress callback is present.
    let mut params = params;
    if opts.on_progress.is_some() {
        params = Some(attach_progress_token(params.take(), id));
    }

    // Wrap the progress callback so a report also refreshes the deadline.
    // `start` is shared with the await loop because both the deadline and the
    // reset-on-progress ceiling measure active time: an elicitation pause
    // shifts the anchor forward by the paused duration.
    let start = Arc::new(Mutex::new(Instant::now()));
    let timeout = Duration::from_millis(opts.timeout_ms);
    let max_total = opts.max_total_timeout_ms.map(Duration::from_millis);
    let deadline = Arc::new(Mutex::new(*start.lock().unwrap() + timeout));
    let stored_cb: Option<ProgressCb> = opts.on_progress.as_ref().map(|user| {
        if opts.reset_timeout_on_progress {
            let user = user.clone();
            let deadline = deadline.clone();
            let start = start.clone();
            Arc::new(move |info: ProgressInfo| {
                let mut next = Instant::now() + timeout;
                if let Some(mt) = max_total {
                    let cap = *start.lock().unwrap() + mt;
                    if next > cap {
                        next = cap;
                    }
                }
                *deadline.lock().unwrap() = next;
                user(info)
            }) as ProgressCb
        } else {
            user.clone()
        }
    });
    if let Some(cb) = &stored_cb {
        inner.progress.lock().unwrap().insert(id, cb.clone());
    }
    inner.pending.lock().unwrap().insert(id, tx);

    // Build and send the message.
    let mut msg = RuntimeObject::new();
    msg.insert("jsonrpc", Value::from("2.0"));
    msg.insert("id", Value::from(id));
    msg.insert("method", Value::from(method));
    if let Some(p) = params {
        msg.insert("params", p);
    }
    let transport = inner.transport.lock().unwrap().clone();
    let transport = match transport {
        Some(t) => t,
        None => {
            inner.pending.lock().unwrap().remove(&id);
            inner.progress.lock().unwrap().remove(&id);
            return Err(McpError::new("not connected to a transport"));
        }
    };
    if let Err(e) = transport.send(Value::Object(msg)).await {
        inner.pending.lock().unwrap().remove(&id);
        inner.progress.lock().unwrap().remove(&id);
        return Err(e);
    }

    // Await the response, honoring the active-time deadline + abort. While an
    // elicitation pause is active the deadline does not advance; when the pause
    // ends, the anchor and the deadline both shift by the paused duration so
    // the remaining budget is exactly what it was when the pause began.
    // While this connection's session is waiting for an elicitation answer the
    // budget stops advancing: it measures active server time, not wall-clock
    // time spent waiting for a human (Codex `active_time_timeout` semantics).
    let mut pause = inner.elicitation_pause.clone();
    tokio::pin!(rx);
    loop {
        let paused = pause
            .as_mut()
            .is_some_and(|state| *state.borrow_and_update());
        let now = Instant::now();
        let dl = *deadline.lock().unwrap();
        let anchor = *start.lock().unwrap();
        let effective = match max_total {
            Some(mt) => dl.min(anchor + mt),
            None => dl,
        };
        if !paused && now >= effective {
            inner.pending.lock().unwrap().remove(&id);
            inner.progress.lock().unwrap().remove(&id);
            send_cancelled(inner, id, "timeout").await;
            return Err(McpError::with_code("Request timed out", -32001));
        }
        // While paused the stale deadline must not spin: the timer arm is
        // disarmed until the pause ends and the deadline shifts forward.
        let sleep = async {
            if paused {
                std::future::pending::<()>().await;
            } else {
                tokio::time::sleep_until(tokio::time::Instant::from_std(effective)).await;
            }
        };
        let signal = opts.signal.clone();
        tokio::select! {
            res = &mut rx => {
                inner.progress.lock().unwrap().remove(&id);
                return match res {
                    Ok(inner_res) => inner_res,
                    Err(_) => Err(McpError::new("connection closed before a response")),
                };
            }
            _ = async {
                match signal {
                    Some(sig) => sig.cancelled().await,
                    None => std::future::pending::<()>().await,
                }
            } => {
                inner.pending.lock().unwrap().remove(&id);
                inner.progress.lock().unwrap().remove(&id);
                send_cancelled(inner, id, "aborted").await;
                return Err(McpError::aborted());
            }
            changed = async {
                match pause.as_mut() {
                    Some(state) => state.changed().await,
                    None => std::future::pending::<Result<(), watch::error::RecvError>>().await,
                }
            } => {
                // The pause sender only drops with the router; treat a closed
                // channel as "no further pauses" and fall back to wall clock.
                if changed.is_err() {
                    pause = None;
                    continue;
                }
                let paused_now = pause.as_mut().is_some_and(|state| *state.borrow_and_update());
                if paused_now != paused {
                    let paused_for = now.elapsed();
                    *start.lock().unwrap() += paused_for;
                    *deadline.lock().unwrap() += paused_for;
                }
            }
            _ = sleep => { /* re-evaluate: the deadline may have advanced on progress */ }
        }
    }
}

/// Send a fire-and-forget JSON-RPC notification.
async fn notify(
    inner: &Arc<ClientInner>,
    method: &str,
    params: Option<Value>,
) -> Result<(), McpError> {
    let mut msg = RuntimeObject::new();
    msg.insert("jsonrpc", Value::from("2.0"));
    msg.insert("method", Value::from(method));
    if let Some(p) = params {
        msg.insert("params", p);
    }
    let transport = inner.transport.lock().unwrap().clone();
    match transport {
        Some(t) => t.send(Value::Object(msg)).await,
        None => Err(McpError::new("not connected to a transport")),
    }
}

/// Best-effort `notifications/cancelled` for a request we gave up on.
async fn send_cancelled(inner: &Arc<ClientInner>, id: i64, reason: &str) {
    let msg = runtime_object([
        ("jsonrpc", Value::from("2.0")),
        ("method", Value::from("notifications/cancelled")),
        (
            "params",
            runtime_object([
                ("requestId", Value::from(id)),
                ("reason", Value::from(reason)),
            ]),
        ),
    ]);
    let transport = inner.transport.lock().unwrap().clone();
    if let Some(t) = transport {
        let _ = t.send(msg).await;
    }
}

/// Current wall-clock time in milliseconds since the Unix epoch.
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Format a number the way a JSON number renders: whole values without a decimal.
fn fmt_num(n: f64) -> String {
    if n.is_finite() && n.fract() == 0.0 && n.abs() < 1e15 {
        format!("{}", n as i64)
    } else {
        format!("{n}")
    }
}

// ---------------------------------------------------------------------------
// Result / descriptor normalization
// ---------------------------------------------------------------------------

fn parse_tool_descriptor(t: &Value) -> McpToolDescriptor {
    McpToolDescriptor {
        name: t
            .get("name")
            .and_then(Value::as_str)
            .cloned()
            .unwrap_or_default(),
        title: t.get("title").and_then(Value::as_str).cloned(),
        description: t.get("description").and_then(Value::as_str).cloned(),
        input_schema: t.get("inputSchema").cloned(),
        annotations: t
            .get("annotations")
            .filter(|a| a.is_object())
            .map(|a| McpToolAnnotations {
                read_only_hint: a.get("readOnlyHint").and_then(Value::as_bool),
                idempotent_hint: a.get("idempotentHint").and_then(Value::as_bool),
                destructive_hint: a.get("destructiveHint").and_then(Value::as_bool),
                open_world_hint: a.get("openWorldHint").and_then(Value::as_bool),
            }),
        meta: t.get("_meta").and_then(|m| m.as_object().cloned()),
    }
}

/// Normalize an SDK-style `tools/call` result into [`McpCallResult`].
fn normalize_call_result(raw: &Value) -> McpCallResult {
    let content = raw
        .get("content")
        .and_then(Value::as_array)
        .map(|arr| arr.iter().map(normalize_block).collect())
        .unwrap_or_default();
    McpCallResult {
        content,
        is_error: raw.get("isError").and_then(Value::as_bool),
        meta: raw.get("_meta").filter(|m| !m.is_null()).cloned(),
        structured_content: raw
            .get("structuredContent")
            .filter(|m| !m.is_null())
            .cloned(),
    }
}

/// Narrow one content block to the fields the wrapper consumes.
fn normalize_block(block: &Value) -> McpContentBlock {
    if !block.is_object() {
        return McpContentBlock {
            block_type: "text".into(),
            text: Some(value_to_string(block)),
            data: None,
            mime_type: None,
            resource: None,
            uri: None,
            name: None,
            description: None,
        };
    }
    let s = |k: &str| block.get(k).and_then(Value::as_str).cloned();
    let resource = block.get("resource").filter(|r| r.is_object()).map(|r| {
        let rs = |k: &str| r.get(k).and_then(Value::as_str).cloned();
        McpEmbeddedResource {
            uri: rs("uri"),
            text: rs("text"),
            blob: rs("blob"),
            mime_type: rs("mimeType"),
        }
    });
    McpContentBlock {
        block_type: block
            .get("type")
            .and_then(Value::as_str)
            .cloned()
            .unwrap_or_else(|| RuntimeString::from("unknown")),
        text: s("text"),
        data: s("data"),
        mime_type: s("mimeType"),
        resource,
        uri: s("uri"),
        name: s("name"),
        description: s("description"),
    }
}

fn value_to_string(v: &Value) -> RuntimeString {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => "null".into(),
        other => runtime_json::stringify(other),
    }
}

// ---------------------------------------------------------------------------
// connect_mcp_server
// ---------------------------------------------------------------------------

fn config_name(c: &McpServerConfig) -> &str {
    match c {
        McpServerConfig::Stdio { name, .. }
        | McpServerConfig::Http { name, .. }
        | McpServerConfig::Sse { name, .. }
        | McpServerConfig::InProcess { name, .. }
        | McpServerConfig::HostManaged { name, .. } => name,
    }
}

fn config_always_load(c: &McpServerConfig) -> bool {
    c.always_load()
}

/// The OAuth server key for an oauth-enabled remote config, else `None`.
fn oauth_server_key_of(config: &McpServerConfig) -> Option<String> {
    match config {
        McpServerConfig::Http {
            name,
            url,
            headers,
            oauth: Some(_),
            ..
        } => Some(mcp_oauth_server_key(&McpOAuthConfig {
            transport: "http".into(),
            name: name.clone(),
            url: url.clone(),
            headers: headers.clone().map(|h| h.into_iter().collect()),
        })),
        McpServerConfig::Sse {
            name,
            url,
            headers,
            oauth: Some(_),
            ..
        } => Some(mcp_oauth_server_key(&McpOAuthConfig {
            transport: "sse".into(),
            name: name.clone(),
            url: url.clone(),
            headers: headers.clone().map(|h| h.into_iter().collect()),
        })),
        _ => None,
    }
}

/// Merge configured headers with a stored `mcp:<server>:token` bearer, if any.
async fn build_remote_request_init(
    server_name: &str,
    config_headers: Option<&HashMap<String, String>>,
    store: Option<&Arc<dyn CredentialStore>>,
) -> HashMap<String, String> {
    let mut headers = config_headers.cloned().unwrap_or_default();
    if let Some(store) = store {
        if let Some(token) = store
            .get(&format!("mcp:{server_name}:token"))
            .await
            .ok()
            .flatten()
        {
            headers.insert("Authorization".into(), format!("Bearer {token}"));
        }
    }
    headers
}

/// Build the transport for a server config, injecting a stored bearer for the
/// remote (http/sse) transports.
async fn create_transport(
    config: &McpServerConfig,
    opts: &ConnectMcpOptions,
) -> Result<Arc<dyn Transport>, McpError> {
    match config {
        McpServerConfig::Stdio {
            command, args, env, ..
        } => Ok(Arc::new(StdioTransport::new(
            opts.process_context.clone(),
            command.clone(),
            args.clone().unwrap_or_default(),
            env.clone(),
        ))),
        McpServerConfig::InProcess { server, .. } => {
            let (client_transport, server_transport) = InMemoryTransport::create_linked_pair();
            server.connect(Box::new(server_transport)).await;
            Ok(Arc::new(client_transport))
        }
        McpServerConfig::Http {
            name,
            url,
            headers,
            oauth,
            ..
        } => {
            if let Some(oauth) = oauth {
                let provider = build_oauth_provider(config, name, url, headers, oauth, opts)?;
                Ok(Arc::new(OAuthTransport::http(
                    url.clone(),
                    headers.clone().unwrap_or_default(),
                    provider,
                    opts.oauth_fetch.clone(),
                )))
            } else {
                let resolved = resolve_remote_headers(name, headers, opts).await;
                Ok(Arc::new(HttpTransport::new(url.clone(), resolved)))
            }
        }
        McpServerConfig::Sse {
            name,
            url,
            headers,
            oauth,
            ..
        } => {
            if let Some(oauth) = oauth {
                let provider = build_oauth_provider(config, name, url, headers, oauth, opts)?;
                Ok(Arc::new(OAuthTransport::sse(
                    url.clone(),
                    headers.clone().unwrap_or_default(),
                    provider,
                    opts.oauth_fetch.clone(),
                )))
            } else {
                let resolved = resolve_remote_headers(name, headers, opts).await;
                Ok(Arc::new(SseTransport::new(url.clone(), resolved)))
            }
        }
        // Host-managed servers have no engine-side endpoint: the host binds
        // and injects the live server at session init. Reaching this arm is a
        // wiring bug, not a runtime condition.
        McpServerConfig::HostManaged { name, .. } => Err(McpError::new(format!(
            "host-managed MCP server {name} must be connected by the host, not the engine"
        ))),
    }
}

fn build_oauth_provider(
    config: &McpServerConfig,
    name: &str,
    url: &str,
    _headers: &Option<HashMap<String, String>>,
    oauth: &super::types::McpOAuthOption,
    opts: &ConnectMcpOptions,
) -> Result<Arc<McpOAuthProvider>, McpError> {
    let store = opts
        .credential_store
        .clone()
        .ok_or_else(|| McpError::new("MCP OAuth requires a credential store"))?;
    let scopes = match oauth {
        super::types::McpOAuthOption::Advertised => None,
        super::types::McpOAuthOption::Object { scopes } => scopes.clone(),
    };
    let observer = opts.on_mcp_authorization_url.clone();
    let server_name = name.to_string();
    Ok(Arc::new(McpOAuthProvider::new(McpOAuthProviderOptions {
        credential_store: store,
        server_key: oauth_server_key_of(config)
            .ok_or_else(|| McpError::new("MCP OAuth server key is unavailable"))?,
        server_name: server_name.clone(),
        server_url: url.to_string(),
        redirect_uri: opts
            .oauth_redirect_uri
            .clone()
            .unwrap_or_else(|| DEFAULT_OAUTH_REDIRECT_URI.to_string()),
        scopes,
        on_authorization_url: Arc::new(move |url| {
            if let Some(observer) = &observer {
                observer(server_name.clone(), url);
            }
        }),
        lock_dir: opts.oauth_lock_dir.clone(),
        fetch: opts.oauth_fetch.clone(),
    })))
}

/// Compute the request headers for a remote transport: the OAuth-stored bearer
/// (when the server uses OAuth) or the static `mcp:<server>:token` bearer, plus
/// the configured routing headers.
async fn resolve_remote_headers(
    name: &str,
    headers: &Option<HashMap<String, String>>,
    opts: &ConnectMcpOptions,
) -> HashMap<String, String> {
    build_remote_request_init(name, headers.as_ref(), opts.credential_store.as_ref()).await
}

/// Connect to a single MCP server and list+wrap its tools. Degrades rather than
/// throwing: any failure yields a [`ManagedClient`] in state `failed` (or
/// `needs-auth`) with no tools and a no-op close.
pub async fn connect_mcp_server(
    config: &McpServerConfig,
    opts: &ConnectMcpOptions,
) -> ManagedClient {
    let mut client = connect_mcp_server_inner(config, opts).await;
    client.declaration = Some(config.clone());
    client
}

async fn connect_mcp_server_inner(
    config: &McpServerConfig,
    opts: &ConnectMcpOptions,
) -> ManagedClient {
    let connect_timeout_ms = opts
        .connect_timeout_ms
        .unwrap_or(DEFAULT_CONNECT_TIMEOUT_MS);
    let call_timeout_ms = opts.call_timeout_ms.unwrap_or(DEFAULT_CALL_TIMEOUT_MS);
    let signal = opts.signal.clone();
    let name = config_name(config).to_string();

    let oauth_key = oauth_server_key_of(config);
    // OAuth credentials persist into the host's store, or a per-call throwaway.
    let oauth_store: Option<Arc<dyn CredentialStore>> = if oauth_key.is_some() {
        Some(
            opts.credential_store
                .clone()
                .unwrap_or_else(|| Arc::new(InMemoryCredentialStore::new())),
        )
    } else {
        None
    };
    // Only persist a needs-auth verdict to disk when there is a durable host store.
    let cache = if oauth_key.is_some() && opts.credential_store.is_some() {
        Some(default_needs_auth_cache(opts.oauth_lock_dir.as_deref()))
    } else {
        None
    };
    // Transports for this connect read the OAuth-stored bearer through `oauth_store`.
    let mut base_opts = opts.clone();
    if let Some(store) = &oauth_store {
        base_opts.credential_store = Some(store.clone());
    }

    // Needs-auth shortcut: a server recently known to need auth, with no stored
    // token, would just 401 again — skip the round-trip and report needs-auth.
    //
    // The stored-token lookup ALSO decides `auth_status`, which every path out of
    // this function reports, so it runs once here rather than only inside the
    // shortcut. A degraded client that claims `unsupported` for an OAuth server is
    // exactly the row where the page would then hide the authorize action.
    let has_oauth_token = match (&oauth_key, &oauth_store) {
        (Some(key), Some(store)) => store
            .get(&format!("mcp:oauth:{key}:tokens"))
            .await
            .ok()
            .flatten()
            .is_some(),
        _ => false,
    };
    let auth_status = auth_status_of(config, has_oauth_token);
    if let (Some(key), Some(cache)) = (&oauth_key, &cache) {
        if opts.skip_needs_auth_cache != Some(true)
            && !has_oauth_token
            && cache.is_fresh(key, NEEDS_AUTH_TTL_MS, now_ms()).await
        {
            return with_auth_status(
                stated_client(&name, McpConnectionState::NeedsAuth, None),
                auth_status,
            );
        }
    }

    let pending_auth_url: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let pending_auth_url_sink = pending_auth_url.clone();
    let upstream_observer = base_opts.on_mcp_authorization_url.clone();
    base_opts.on_mcp_authorization_url = Some(Arc::new(move |server_name, url| {
        *pending_auth_url_sink.lock().unwrap() = Some(url.to_string());
        if let Some(observer) = &upstream_observer {
            observer(server_name, url);
        }
    }));

    let mut transport = match create_transport(config, &base_opts).await {
        Ok(t) => t,
        Err(err) => return degraded_client_for(&name, &err, auth_status),
    };
    let mut client = Client::new_for_server(
        opts.cwd.clone(),
        name.clone(),
        opts.mcp_elicitation_router.clone(),
    );
    let mut lease = Arc::new(ClientLease {
        client: client.clone(),
    });

    let mut connected = false;
    match client
        .connect(
            transport.clone(),
            RequestOptions {
                timeout_ms: connect_timeout_ms,
                signal: signal.clone(),
                ..Default::default()
            },
        )
        .await
    {
        Ok(()) => {}
        Err(err) => {
            // A 401/403 means "needs auth", not "failed".
            if !is_auth_error(&err) {
                safe_close(&transport).await;
                return degraded_client_for(&name, &err, auth_status);
            }
            let redirect_uri = opts
                .oauth_redirect_uri
                .clone()
                .unwrap_or_else(|| DEFAULT_OAUTH_REDIRECT_URI.to_string());
            let pending_auth_url = pending_auth_url.lock().unwrap().clone();
            if let Some(authorize_handler) = opts
                .mcp_authorize_handler
                .as_ref()
                .filter(|_| pending_auth_url.is_some() && transport.supports_finish_auth())
            {
                if let Some((new_client, new_transport, new_lease)) = complete_interactive_auth(
                    &transport,
                    config,
                    &base_opts,
                    pending_auth_url.as_deref().unwrap_or(""),
                    &redirect_uri,
                    authorize_handler,
                    signal.clone().unwrap_or_default(),
                    connect_timeout_ms,
                )
                .await
                {
                    transport = new_transport;
                    client = new_client;
                    lease = new_lease;
                    connected = true;
                }
            }
            if !connected {
                safe_close(&transport).await;
                return needs_auth_client(
                    mark_needs_auth(&name, &oauth_key, &cache, pending_auth_url).await,
                    auth_status,
                    has_oauth_token,
                );
            }
        }
    }

    // Only list tools when the server advertises the `tools` capability.
    let server_always_load = config_always_load(config);
    let raw_tools = Arc::new(Mutex::new(Vec::new()));
    let tool_config = Arc::new(Mutex::new(config.clone()));
    let tool_refresh_lock = Arc::new(Mutex::new(()));
    let mut tools: Vec<Arc<dyn Tool>> = Vec::new();
    if server_capabilities_has_tools(&client) {
        match client
            .list_tools(RequestOptions {
                timeout_ms: connect_timeout_ms,
                signal: signal.clone(),
                ..Default::default()
            })
            .await
        {
            Ok(listed) => {
                *raw_tools.lock().unwrap() = listed.clone();
                tools = wrap_listed(
                    &client,
                    &name,
                    call_timeout_ms,
                    server_always_load,
                    listed,
                    config,
                    lease.clone(),
                )
            }
            Err(err) => {
                safe_close_client(&client).await;
                if is_auth_error(&err) {
                    let pending_auth_url = pending_auth_url.lock().unwrap().clone();
                    return needs_auth_client(
                        mark_needs_auth(&name, &oauth_key, &cache, pending_auth_url).await,
                        auth_status,
                        has_oauth_token,
                    );
                }
                return degraded_client_for(&name, &err, auth_status);
            }
        }
    }

    // Connected: drop any stale needs-auth verdict for this server.
    if let (Some(key), Some(cache)) = (&oauth_key, &cache) {
        cache.clear(key).await;
    }

    let resources = list_resources_safe(&client, connect_timeout_ms).await;
    let resource_templates = list_resource_templates_safe(&client, connect_timeout_ms).await;
    let prompts = list_prompts_safe(&client, connect_timeout_ms).await;
    let instructions = truncate_instructions(client.instructions());

    let tools_arc = Arc::new(Mutex::new(tools));
    let managed = ManagedClient {
        declaration: None,
        name: RuntimeString::from(name.clone()),
        state: McpConnectionState::Ready,
        resources,
        resource_templates,
        prompts,
        instructions,
        server_info: client.server_info(),
        auth_status,
        authorization_url: None,
        failure_reason: None,
        error: None,
        tools: tools_arc.clone(),
        raw_tools: Some(raw_tools.clone()),
        tool_config: Some(tool_config.clone()),
        tool_refresh_lock: Some(tool_refresh_lock.clone()),
        lease: Some(lease.clone()),
        client: Some(client.clone()),
        call_timeout_ms,
    };

    // Mid-session tool re-discovery: subscribe to `tools/list_changed` when the
    // server declares it can change its tool list. A monotonic generation guard
    // makes the LATEST-fired relist the only one allowed to commit.
    if server_capabilities_tools_list_changed(&client) {
        let gen = Arc::new(AtomicI64::new(0));
        let handler_client = client.clone();
        let server_name = name.clone();
        let config_for_relist = tool_config.clone();
        let raw_for_relist = raw_tools.clone();
        let weak_lease = Arc::downgrade(&lease);
        let weak_tools = Arc::downgrade(&tools_arc);
        let refresh_lock = tool_refresh_lock.clone();
        client.set_notification_handler(
            "notifications/tools/list_changed",
            Arc::new(move |_params| {
                let gen = gen.clone();
                let client = handler_client.clone();
                let weak_tools = weak_tools.clone();
                let server_name = server_name.clone();
                let config_for_relist = config_for_relist.clone();
                let raw_for_relist = raw_for_relist.clone();
                let weak_lease = weak_lease.clone();
                let refresh_lock = refresh_lock.clone();
                Box::pin(async move {
                    let g = gen.fetch_add(1, Ordering::SeqCst) + 1;
                    if let Ok(relisted) = client
                        .list_tools(RequestOptions {
                            timeout_ms: connect_timeout_ms,
                            ..Default::default()
                        })
                        .await
                    {
                        if g == gen.load(Ordering::SeqCst) {
                            let _guard = refresh_lock.lock().unwrap();
                            if g != gen.load(Ordering::SeqCst) {
                                return;
                            }
                            let (Some(lease), Some(tools_arc)) =
                                (weak_lease.upgrade(), weak_tools.upgrade())
                            else {
                                return;
                            };
                            *raw_for_relist.lock().unwrap() = relisted.clone();
                            let config = config_for_relist.lock().unwrap().clone();
                            let wrapped = wrap_listed(
                                &client,
                                &server_name,
                                call_timeout_ms,
                                config_always_load(&config),
                                relisted,
                                &config,
                                lease,
                            );
                            *tools_arc.lock().unwrap() = wrapped;
                        }
                    }
                }) as BoxFuture<'static, ()>
            }),
        );
    }

    managed
}

fn server_capabilities_has_tools(client: &Client) -> bool {
    client
        .server_capabilities()
        .and_then(|c| c.get("tools").cloned())
        .map(|t| !t.is_null())
        .unwrap_or(false)
}

fn server_capabilities_tools_list_changed(client: &Client) -> bool {
    client
        .server_capabilities()
        .and_then(|c| c.get("tools").and_then(|t| t.get("listChanged")).cloned())
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
}

/// Wrap a fresh `tools/list` result into engine tools.
fn wrap_listed(
    client: &Client,
    server_name: &str,
    call_timeout_ms: u64,
    server_always_load: bool,
    listed: Vec<McpToolDescriptor>,
    config: &McpServerConfig,
    lease: Arc<ClientLease>,
) -> Vec<Arc<dyn Tool>> {
    // The server's own tool filter: an allow list first (when set, nothing else
    // is registered), then the deny list of names the user switched off. Filtering
    // HERE rather than at call time is what keeps a disabled tool out of the
    // model's tool list entirely instead of letting it be called and refused.
    let opts = config.options();
    let filtered: Vec<McpToolDescriptor> = listed
        .into_iter()
        .filter(|t| {
            let name = t.name.try_to_utf8().unwrap_or_default();
            if let Some(ref enabled) = opts.enabled_tools {
                if !enabled.iter().any(|e| e == &name) {
                    return false;
                }
            }
            if let Some(ref disabled) = opts.disabled_tools {
                if disabled.iter().any(|d| d == &name) {
                    return false;
                }
            }
            true
        })
        .collect();

    // Model-visible names go through the disambiguation pipeline (two servers may
    // serve the same tool name).
    let tool_pairs: Vec<(String, String)> = filtered
        .iter()
        .map(|t| {
            (
                server_name.to_string(),
                t.name.try_to_utf8().unwrap_or_default(),
            )
        })
        .collect();
    let normalized = crate::mcp::tool_naming::normalize_tool_names(
        &tool_pairs,
        &crate::mcp::tool_naming::NormalizeOptions::default(),
    );

    let make_call = make_tool_call(client.clone(), call_timeout_ms, lease);
    let prepared_config = Arc::new(config.clone());
    filtered
        .into_iter()
        .zip(normalized)
        .map(|(t, (model_name, _raw))| {
            let native_registration =
                crate::mcp::approval_provenance::PreparedMcpRegistration::from_config(
                    Arc::clone(&prepared_config),
                    &t.name,
                );
            crate::mcp::tool_wrapper::wrap_native_mcp_tool(
                server_name,
                t,
                make_call.clone(),
                Some(WrapMcpToolOptions {
                    max_result_chars: None,
                    server_always_load: Some(server_always_load),
                    deferred_by_default: None,
                    override_name: Some(model_name),
                    approval_registration: Some(
                        crate::types::permission::ApprovalMcpRegistration {
                            native_registration,
                            ..Default::default()
                        },
                    ),
                }),
                prepared_config.clone(),
            )
        })
        .collect()
}

/// Build the tool-invocation closure the wrapper drives per call.
fn make_tool_call(client: Client, call_timeout_ms: u64, lease: Arc<ClientLease>) -> McpToolCall {
    Arc::new(
        move |name: RuntimeString,
              args: RuntimeObject,
              on_progress: Option<ToolProgress>,
              signal: AbortSignal| {
            let client = client.clone();
            let lease = lease.clone();
            Box::pin(async move {
                let _lease = lease;
                client
                    .call_tool(
                        &name,
                        Value::Object(args),
                        call_timeout_ms,
                        on_progress,
                        Some(signal),
                    )
                    .await
                    .map_err(|e| ToolError::new(e.message))
            }) as BoxFuture<'static, Result<McpCallResult, ToolError>>
        },
    )
}

fn interactive_auth_allowed(connect_opts: &ConnectMcpOptions) -> bool {
    // Named user reconnects have no turn signal; lazy model connections carry one.
    connect_opts.signal.is_none()
        || connect_opts
            .mcp_elicitation_router
            .as_ref()
            .is_none_or(|router| router.policy_allows_elicitation())
}

/// Drive the host's authorize handler, finish the OAuth code exchange on the
/// FAILED transport, then connect a FRESH transport + client.
#[allow(clippy::too_many_arguments)]
async fn complete_interactive_auth(
    failed_transport: &Arc<dyn Transport>,
    config: &McpServerConfig,
    connect_opts: &ConnectMcpOptions,
    authorization_url: &str,
    redirect_uri: &str,
    handler: &Arc<dyn McpAuthorizeHandler>,
    signal: AbortSignal,
    timeout_ms: u64,
) -> Option<(Client, Arc<dyn Transport>, Arc<ClientLease>)> {
    if !interactive_auth_allowed(connect_opts) {
        return None;
    }
    let resp = handler
        .authorize(
            McpAuthorizeRequest {
                server_name: config_name(config).to_string(),
                authorization_url: authorization_url.to_string(),
                redirect_uri: redirect_uri.to_string(),
            },
            signal.clone(),
        )
        .await;
    if !interactive_auth_allowed(connect_opts) {
        return None;
    }
    let (code, state) = match resp {
        McpAuthorizeResponse::Code { code, state } => (code, state),
        McpAuthorizeResponse::Decline { .. } => return None,
    };
    let expected_state = Url::parse(authorization_url).ok().and_then(|url| {
        url.query_pairs()
            .find(|(key, _)| key == "state")
            .map(|(_, value)| value.into_owned())
    });
    if expected_state.is_some() && expected_state != state {
        safe_close(failed_transport).await;
        return None;
    }
    let exchanged = failed_transport.finish_auth(code).await;
    safe_close(failed_transport).await;
    exchanged.ok()?;
    let transport = create_transport(config, connect_opts).await.ok()?;
    let client = Client::new_for_server(
        connect_opts.cwd.clone(),
        config_name(config).to_string(),
        connect_opts.mcp_elicitation_router.clone(),
    );
    let lease = Arc::new(ClientLease {
        client: client.clone(),
    });
    client
        .connect(
            transport.clone(),
            RequestOptions {
                timeout_ms,
                signal: Some(signal),
                ..Default::default()
            },
        )
        .await
        .ok()?;
    Some((client, transport, lease))
}

async fn mark_needs_auth(
    name: &str,
    oauth_key: &Option<String>,
    cache: &Option<super::needs_auth_cache::NeedsAuthCache>,
    url: Option<String>,
) -> ManagedClient {
    if let (Some(key), Some(cache)) = (oauth_key, cache) {
        cache.mark(key, now_ms()).await;
    }
    stated_client(name, McpConnectionState::NeedsAuth, url)
}

/// Classify an error as an auth (401/403) failure.
fn is_auth_error(err: &McpError) -> bool {
    if let Some(code) = err.code {
        return code == 401 || code == 403;
    }
    let units = err.message.as_utf16();
    contains_ascii_case_insensitive(units, b"unauthor")
        || contains_ascii_case_insensitive(units, b"forbidden")
        || contains_ascii_word(units, b"401")
        || contains_ascii_word(units, b"403")
}

/// Match the ASCII-only fragments used by JavaScript's auth-error regexp
/// directly over UTF-16. Unrelated lone surrogates remain exact and are never
/// projected through a scalar `String` merely to classify the error.
fn contains_ascii_case_insensitive(units: &[u16], needle: &[u8]) -> bool {
    units.windows(needle.len()).any(|window| {
        window
            .iter()
            .copied()
            .zip(needle.iter().copied())
            .all(|(unit, expected)| ascii_lower(unit) == u16::from(expected.to_ascii_lowercase()))
    })
}

fn contains_ascii_word(units: &[u16], needle: &[u8]) -> bool {
    units
        .windows(needle.len())
        .enumerate()
        .any(|(index, window)| {
            window
                .iter()
                .copied()
                .zip(needle.iter().copied())
                .all(|(unit, expected)| unit == u16::from(expected))
                && index
                    .checked_sub(1)
                    .and_then(|previous| units.get(previous))
                    .is_none_or(|unit| !is_ascii_word(*unit))
                && units
                    .get(index + needle.len())
                    .is_none_or(|unit| !is_ascii_word(*unit))
        })
}

fn ascii_lower(unit: u16) -> u16 {
    if (u16::from(b'A')..=u16::from(b'Z')).contains(&unit) {
        unit + u16::from(b'a' - b'A')
    } else {
        unit
    }
}

fn is_ascii_word(unit: u16) -> bool {
    unit == u16::from(b'_')
        || (u16::from(b'0')..=u16::from(b'9')).contains(&unit)
        || (u16::from(b'A')..=u16::from(b'Z')).contains(&unit)
        || (u16::from(b'a')..=u16::from(b'z')).contains(&unit)
}

/// A degraded client: no client handle, no tools, idempotent no-op close.
fn failed_client(name: &str) -> ManagedClient {
    stated_client(name, McpConnectionState::Failed, None)
}

fn stated_client(
    name: &str,
    state: McpConnectionState,
    authorization_url: Option<String>,
) -> ManagedClient {
    ManagedClient {
        declaration: None,
        name: RuntimeString::from(name),
        state,
        resources: Vec::new(),
        resource_templates: Vec::new(),
        prompts: Vec::new(),
        instructions: None,
        server_info: None,
        auth_status: McpAuthStatus::Unsupported,
        authorization_url,
        failure_reason: None,
        error: None,
        tools: Arc::new(Mutex::new(Vec::new())),
        raw_tools: None,
        tool_config: None,
        tool_refresh_lock: None,
        lease: None,
        client: None,
        call_timeout_ms: DEFAULT_CALL_TIMEOUT_MS,
    }
}

/// Stamp the authentication fact onto a client one of the degraded paths built.
/// Those paths have the config in hand; `stated_client` does not, and a row that
/// reports `unsupported` for an OAuth server is exactly the row where the page
/// would then hide the authorize action.
fn with_auth_status(mut client: ManagedClient, auth_status: McpAuthStatus) -> ManagedClient {
    client.auth_status = auth_status;
    client
}

/// A failed client that carries WHY it failed. `failed_client` alone reports a
/// state the UI can only render as "failed", which reads the same for a missing
/// binary, a bad argument, and a crashed server.
fn failed_client_with(name: &str, error: RuntimeString) -> ManagedClient {
    let mut client = failed_client(name);
    client.error = Some(error);
    client
}

/// The degraded client an error should produce. An ABORTED error is not a fault:
/// the dial was interrupted (session interrupt, shutdown mid-handshake), so it
/// settles as `Cancelled` and the host must not paint it as a broken server.
fn degraded_client_for(name: &str, err: &McpError, auth_status: McpAuthStatus) -> ManagedClient {
    if err.aborted {
        return with_auth_status(
            stated_client(name, McpConnectionState::Cancelled, None),
            auth_status,
        );
    }
    with_auth_status(failed_client_with(name, err.message.clone()), auth_status)
}

/// A needs-auth client, tagged. When a credential WAS stored and the server still
/// refused, the cause is specifically that the user has to re-authorize — the one
/// needs-auth case that is not simply "log in for the first time", and the only
/// one where re-running the flow is the fix rather than a first-time setup.
fn needs_auth_client(
    mut client: ManagedClient,
    auth_status: McpAuthStatus,
    had_token: bool,
) -> ManagedClient {
    client.auth_status = auth_status;
    if had_token {
        client.failure_reason = Some(McpStartupFailureReason::ReauthenticationRequired);
    }
    client
}

/// The authentication fact for a config, decided before any dial.
/// `has_oauth_token` separates `OAuth` (a stored credential will be presented)
/// from `NotLoggedIn` (the flow exists but nothing is stored yet).
fn auth_status_of(config: &McpServerConfig, has_oauth_token: bool) -> McpAuthStatus {
    let (headers, oauth) = match config {
        // Local transports carry no notion of authentication at all: whatever
        // credential the server needs came in through `env`, which is not an MCP
        // auth mechanism and must not be reported as one.
        McpServerConfig::Stdio { .. }
        | McpServerConfig::InProcess { .. }
        | McpServerConfig::HostManaged { .. } => return McpAuthStatus::Unsupported,
        McpServerConfig::Http { headers, oauth, .. }
        | McpServerConfig::Sse { headers, oauth, .. } => (headers, oauth),
    };
    if oauth.is_some() {
        return if has_oauth_token {
            McpAuthStatus::OAuth
        } else {
            McpAuthStatus::NotLoggedIn
        };
    }
    if headers.as_ref().is_some_and(|map| !map.is_empty()) {
        return McpAuthStatus::BearerToken;
    }
    McpAuthStatus::Unsupported
}

pub(crate) fn authorization_revoked_client(name: &str) -> ManagedClient {
    // The credential is gone, so the fact is `not-logged-in`, and the reason the
    // connection is down is that it has to be re-established interactively.
    let mut client = stated_client(name, McpConnectionState::NeedsAuth, None);
    client.auth_status = McpAuthStatus::NotLoggedIn;
    client.failure_reason = Some(McpStartupFailureReason::ReauthenticationRequired);
    client
}

/// Truncate instructions to at most [`MAX_MCP_INSTRUCTIONS`] characters TOTAL.
fn truncate_instructions(raw: Option<RuntimeString>) -> Option<RuntimeString> {
    let raw = raw?;
    if raw.is_empty() {
        return None;
    }
    if raw.len_utf16() <= MAX_MCP_INSTRUCTIONS {
        return Some(raw);
    }
    let marker = RuntimeString::from("… [truncated]");
    let keep = MAX_MCP_INSTRUCTIONS.saturating_sub(marker.len_utf16());
    Some(raw.slice_utf16(..keep).concat(&marker))
}

/// `resources/templates/list`, tolerantly: a server that does not implement it
/// simply has no templates, which is not a connection fault.
async fn list_resource_templates_safe(
    client: &Client,
    timeout: u64,
) -> Vec<McpResourceTemplateDescriptor> {
    match client.list_resource_templates(timeout).await {
        Ok(r) => r
            .get("resourceTemplates")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(|x| {
                        let uri_template = x.get("uriTemplate").and_then(Value::as_str).cloned()?;
                        Some(McpResourceTemplateDescriptor {
                            uri_template,
                            name: x.get("name").and_then(Value::as_str).cloned(),
                            mime_type: x.get("mimeType").and_then(Value::as_str).cloned(),
                            description: x.get("description").and_then(Value::as_str).cloned(),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default(),
        Err(_) => Vec::new(),
    }
}

async fn list_resources_safe(client: &Client, timeout: u64) -> Vec<McpResourceDescriptor> {
    match client.list_resources(timeout).await {
        Ok(r) => r
            .get("resources")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .map(|x| McpResourceDescriptor {
                        uri: x
                            .get("uri")
                            .and_then(Value::as_str)
                            .cloned()
                            .unwrap_or_default(),
                        name: x.get("name").and_then(Value::as_str).cloned(),
                        mime_type: x.get("mimeType").and_then(Value::as_str).cloned(),
                        description: x.get("description").and_then(Value::as_str).cloned(),
                    })
                    .collect()
            })
            .unwrap_or_default(),
        Err(_) => Vec::new(),
    }
}

async fn list_prompts_safe(client: &Client, timeout: u64) -> Vec<McpPromptDescriptor> {
    match client.list_prompts(timeout).await {
        Ok(r) => r
            .get("prompts")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .map(|x| {
                        let arguments = x
                            .get("arguments")
                            .and_then(Value::as_array)
                            .map(|args| {
                                args.iter()
                                    .map(|a| McpPromptArgument {
                                        name: a
                                            .get("name")
                                            .and_then(Value::as_str)
                                            .cloned()
                                            .unwrap_or_default(),
                                        description: a
                                            .get("description")
                                            .and_then(Value::as_str)
                                            .cloned(),
                                        required: a.get("required").and_then(Value::as_bool),
                                    })
                                    .collect::<Vec<_>>()
                            })
                            .filter(|v| !v.is_empty());
                        McpPromptDescriptor {
                            name: x
                                .get("name")
                                .and_then(Value::as_str)
                                .cloned()
                                .unwrap_or_default(),
                            description: x.get("description").and_then(Value::as_str).cloned(),
                            arguments,
                        }
                    })
                    .collect()
            })
            .unwrap_or_default(),
        Err(_) => Vec::new(),
    }
}

async fn read_resource_safe(
    client: &Client,
    uri: &RuntimeString,
    timeout: u64,
) -> Vec<McpResourceContent> {
    match client.read_resource(uri, timeout).await {
        Ok(r) => r
            .get("contents")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .map(|c| McpResourceContent {
                        uri: c.get("uri").and_then(Value::as_str).cloned(),
                        text: c.get("text").and_then(Value::as_str).cloned(),
                        mime_type: c.get("mimeType").and_then(Value::as_str).cloned(),
                        blob: c.get("blob").and_then(Value::as_str).cloned(),
                    })
                    .collect()
            })
            .unwrap_or_default(),
        Err(_) => Vec::new(),
    }
}

async fn get_prompt_safe(
    client: &Client,
    name: &RuntimeString,
    args: Option<RuntimeObject>,
    timeout: u64,
) -> RuntimeString {
    let r = match client.get_prompt(name, args, timeout).await {
        Ok(r) => r,
        Err(_) => return RuntimeString::default(),
    };
    let mut parts = Vec::new();
    if let Some(messages) = r.get("messages").and_then(Value::as_array) {
        for m in messages {
            let content = m.get("content");
            if let Some(text) = content.and_then(Value::as_str) {
                parts.push(text.clone());
            } else if let Some(text) = content.and_then(|c| c.get("text")).and_then(Value::as_str) {
                parts.push(text.clone());
            }
        }
    }
    join_runtime_strings(&parts, "\n")
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

async fn safe_close(transport: &Arc<dyn Transport>) {
    let _ = transport.close().await;
}

async fn safe_close_client(client: &Client) {
    let _ = client.close().await;
}

#[cfg(test)]
mod tests {
    use super::super::elicitation::{
        McpElicitationAction, McpElicitationEvent, McpElicitationPresenter, McpElicitationResponse,
        McpElicitationRouter,
    };
    use super::super::types::{McpInProcessServer, McpOAuthOption, McpServerOptions};
    use super::*;
    use crate::mcp::connection_manager::{McpConnectionManager, McpConnectionManagerOptions};
    use crate::schemas::content::OutputBlock;
    use crate::tools::builtins::test_ctx::make_ctx;
    use crate::types::tool::ToolExposure;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicBool, AtomicU64};
    use url::Url;

    fn test_connect_options() -> ConnectMcpOptions {
        ConnectMcpOptions::new(crate::runtime_process::test_runtime_process_context())
    }

    macro_rules! json {
        (@array [$($elements:expr,)*]) => {
            vec![$($elements,)*]
        };
        (@array [$($elements:expr),*]) => {
            vec![$($elements),*]
        };
        (@array [$($elements:expr,)*] null $($rest:tt)*) => {
            json!(@array [$($elements,)* Value::Null] $($rest)*)
        };
        (@array [$($elements:expr,)*] true $($rest:tt)*) => {
            json!(@array [$($elements,)* Value::Bool(true)] $($rest)*)
        };
        (@array [$($elements:expr,)*] false $($rest:tt)*) => {
            json!(@array [$($elements,)* Value::Bool(false)] $($rest)*)
        };
        (@array [$($elements:expr,)*] [$($array:tt)*] $($rest:tt)*) => {
            json!(@array [$($elements,)* json!([$($array)*])] $($rest)*)
        };
        (@array [$($elements:expr,)*] {$($object:tt)*} $($rest:tt)*) => {
            json!(@array [$($elements,)* json!({$($object)*})] $($rest)*)
        };
        (@array [$($elements:expr,)*] $next:expr, $($rest:tt)*) => {
            json!(@array [$($elements,)* json!($next),] $($rest)*)
        };
        (@array [$($elements:expr,)*] $last:expr) => {
            json!(@array [$($elements,)* json!($last)])
        };
        (@array [$($elements:expr),*] , $($rest:tt)*) => {
            json!(@array [$($elements,)*] $($rest)*)
        };

        (@object $object:ident) => {};
        (@object $object:ident ,) => {};
        (@object $object:ident $key:literal : null, $($rest:tt)*) => {{
            $object.insert($key, Value::Null);
            json!(@object $object $($rest)*);
        }};
        (@object $object:ident $key:literal : null) => {{
            $object.insert($key, Value::Null);
        }};
        (@object $object:ident $key:literal : true, $($rest:tt)*) => {{
            $object.insert($key, Value::Bool(true));
            json!(@object $object $($rest)*);
        }};
        (@object $object:ident $key:literal : true) => {{
            $object.insert($key, Value::Bool(true));
        }};
        (@object $object:ident $key:literal : false, $($rest:tt)*) => {{
            $object.insert($key, Value::Bool(false));
            json!(@object $object $($rest)*);
        }};
        (@object $object:ident $key:literal : false) => {{
            $object.insert($key, Value::Bool(false));
        }};
        (@object $object:ident $key:literal : [$($array:tt)*], $($rest:tt)*) => {{
            $object.insert($key, json!([$($array)*]));
            json!(@object $object $($rest)*);
        }};
        (@object $object:ident $key:literal : [$($array:tt)*]) => {{
            $object.insert($key, json!([$($array)*]));
        }};
        (@object $object:ident $key:literal : {$($nested:tt)*}, $($rest:tt)*) => {{
            $object.insert($key, json!({$($nested)*}));
            json!(@object $object $($rest)*);
        }};
        (@object $object:ident $key:literal : {$($nested:tt)*}) => {{
            $object.insert($key, json!({$($nested)*}));
        }};
        (@object $object:ident $key:literal : $value:expr, $($rest:tt)*) => {{
            $object.insert($key, json!($value));
            json!(@object $object $($rest)*);
        }};
        (@object $object:ident $key:literal : $value:expr) => {{
            $object.insert($key, json!($value));
        }};

        (null) => { Value::Null };
        (true) => { Value::Bool(true) };
        (false) => { Value::Bool(false) };
        ([]) => { Value::Array(Vec::new()) };
        ([$($tokens:tt)+]) => {
            Value::Array(json!(@array [] $($tokens)+))
        };
        ({}) => { Value::Object(RuntimeObject::new()) };
        ({$($tokens:tt)+}) => {{
            let mut object = RuntimeObject::new();
            json!(@object object $($tokens)+);
            Value::Object(object)
        }};
        ($value:expr) => {
            runtime_codec::encode_value(&$value).expect("test exact JSON expression must encode")
        };
    }

    #[test]
    fn attach_progress_token_creates_meta_for_absent_or_non_object_params() {
        for params in [
            None,
            Some(json!(5)),
            Some(json!("scalar")),
            Some(json!([1, 2])),
        ] {
            let out = attach_progress_token(params, 42);
            assert_eq!(out["_meta"]["progressToken"], json!(42));
        }
    }

    #[test]
    fn attach_progress_token_preserves_sibling_keys_and_existing_meta() {
        let params = Some(json!({
            "name": "tool",
            "_meta": { "persist": true }
        }));
        let out = attach_progress_token(params, 7);
        // Sibling params and prior _meta entries survive; the token is added.
        assert_eq!(out["name"], json!("tool"));
        assert_eq!(out["_meta"]["persist"], json!(true));
        assert_eq!(out["_meta"]["progressToken"], json!(7));
    }

    #[test]
    fn attach_progress_token_replaces_non_object_meta() {
        let params = Some(json!({ "_meta": "not-an-object" }));
        let out = attach_progress_token(params, 9);
        assert_eq!(out["_meta"]["progressToken"], json!(9));
    }

    #[test]
    fn auth_error_classifier_preserves_unrelated_surrogate_units() {
        let message = RuntimeString::from_utf16(
            [0xd800]
                .into_iter()
                .chain(" UnAuThOrized ".encode_utf16())
                .chain([0xdc00])
                .collect::<Vec<_>>(),
        );
        assert!(is_auth_error(&McpError {
            message: message.clone(),
            code: None,
            aborted: false,
        }));
        assert_eq!(message.as_utf16().first(), Some(&0xd800));
        assert_eq!(message.as_utf16().last(), Some(&0xdc00));
        assert!(!is_auth_error(&McpError::new("X401Y")));
        assert!(is_auth_error(&McpError::new("status 401 returned")));
    }

    // ---- a minimal in-process MCP server that speaks the wire protocol ----

    type ToolHandler = Arc<dyn Fn(Value) -> Value + Send + Sync>;
    type PromptHandler = Arc<dyn Fn(RuntimeObject) -> Value + Send + Sync>;

    #[derive(Clone)]
    struct TestTool {
        name: RuntimeString,
        description: RuntimeString,
        input_schema: Value,
        annotations: Option<Value>,
        handler: ToolHandler,
    }

    #[derive(Clone)]
    struct TestPrompt {
        name: RuntimeString,
        description: Option<RuntimeString>,
        arguments: Vec<Value>,
        handler: PromptHandler,
    }

    #[derive(Clone)]
    struct TestResource {
        uri: RuntimeString,
        name: Option<RuntimeString>,
        mime_type: Option<RuntimeString>,
        description: Option<RuntimeString>,
    }

    pub struct TestMcpServer {
        name: RuntimeString,
        version: RuntimeString,
        instructions: Option<RuntimeString>,
        capabilities: Value,
        tools: Arc<Mutex<Vec<TestTool>>>,
        resources: Arc<Mutex<Vec<TestResource>>>,
        prompts: Arc<Mutex<Vec<TestPrompt>>>,
        transport: Arc<Mutex<Option<Arc<dyn Transport>>>>,
        next_id: Arc<AtomicI64>,
        pending: Arc<Mutex<HashMap<i64, oneshot::Sender<Value>>>>,
        connection_count: Arc<AtomicU64>,
        tools_list_count: Arc<AtomicU64>,
        close_count: Arc<AtomicU64>,
        initialize_params: Arc<Mutex<Option<Value>>>,
    }

    impl TestMcpServer {
        fn new(name: &str, version: &str) -> Self {
            TestMcpServer {
                name: name.into(),
                version: version.into(),
                instructions: None,
                capabilities: json!({}),
                tools: Arc::new(Mutex::new(Vec::new())),
                resources: Arc::new(Mutex::new(Vec::new())),
                prompts: Arc::new(Mutex::new(Vec::new())),
                transport: Arc::new(Mutex::new(None)),
                next_id: Arc::new(AtomicI64::new(0)),
                pending: Arc::new(Mutex::new(HashMap::new())),
                connection_count: Arc::new(AtomicU64::new(0)),
                tools_list_count: Arc::new(AtomicU64::new(0)),
                close_count: Arc::new(AtomicU64::new(0)),
                initialize_params: Arc::new(Mutex::new(None)),
            }
        }

        fn with_instructions(mut self, instructions: &str) -> Self {
            self.instructions = Some(instructions.into());
            self
        }

        fn with_capabilities(mut self, caps: Value) -> Self {
            self.capabilities = caps;
            self
        }

        fn register_tool(
            &self,
            name: &str,
            description: &str,
            input_schema: Value,
            annotations: Option<Value>,
            handler: ToolHandler,
        ) {
            self.tools.lock().unwrap().push(TestTool {
                name: name.into(),
                description: description.into(),
                input_schema,
                annotations,
                handler,
            });
            // If already connected and the server declares listChanged, announce it.
            let list_changed = self
                .capabilities
                .get("tools")
                .and_then(|t| t.get("listChanged"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if list_changed {
                let transport = self.transport.lock().unwrap().clone();
                if let Some(t) = transport {
                    tokio::spawn(async move {
                        let _ = t
                            .send(json!({
                                "jsonrpc": "2.0",
                                "method": "notifications/tools/list_changed"
                            }))
                            .await;
                    });
                }
            }
        }

        fn register_prompt(
            &self,
            name: &str,
            description: Option<&str>,
            arguments: Vec<Value>,
            handler: PromptHandler,
        ) {
            self.prompts.lock().unwrap().push(TestPrompt {
                name: name.into(),
                description: description.map(RuntimeString::from),
                arguments,
                handler,
            });
        }

        fn register_resource(
            &self,
            uri: &str,
            name: Option<&str>,
            mime_type: Option<&str>,
            description: Option<&str>,
        ) {
            self.resources.lock().unwrap().push(TestResource {
                uri: uri.into(),
                name: name.map(RuntimeString::from),
                mime_type: mime_type.map(RuntimeString::from),
                description: description.map(RuntimeString::from),
            });
        }

        /// Query the client's roots (server-to-client request).
        async fn list_roots(&self) -> Value {
            let id = self.next_id.fetch_add(1, Ordering::SeqCst);
            self.request_client(id, "roots/list", None).await
        }

        async fn request_client(&self, id: i64, method: &str, params: Option<Value>) -> Value {
            let (tx, rx) = oneshot::channel();
            self.pending.lock().unwrap().insert(id, tx);
            let transport = self.transport.lock().unwrap().clone();
            if let Some(t) = transport {
                let mut request = json!({ "jsonrpc": "2.0", "id": id, "method": method });
                if let Some(params) = params {
                    request
                        .as_object_mut()
                        .unwrap()
                        .insert("params".to_string(), params);
                }
                let _ = t.send(request).await;
            }
            rx.await.unwrap_or(Value::Null)
        }

        fn client_capabilities(&self) -> Option<Value> {
            self.initialize_params
                .lock()
                .unwrap()
                .as_ref()
                .and_then(|params| params.get("capabilities"))
                .cloned()
        }

        fn connection_count(&self) -> u64 {
            self.connection_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl McpInProcessServer for TestMcpServer {
        async fn connect(&self, transport: Box<dyn Transport>) {
            self.connection_count.fetch_add(1, Ordering::SeqCst);
            let transport: Arc<dyn Transport> = Arc::from(transport);
            *self.transport.lock().unwrap() = Some(transport.clone());
            let (tx, rx) = mpsc::unbounded_channel();
            let _ = transport.start(tx).await;
            let state = ServerState {
                name: self.name.clone(),
                version: self.version.clone(),
                instructions: self.instructions.clone(),
                capabilities: self.capabilities.clone(),
                tools: self.tools.clone(),
                resources: self.resources.clone(),
                prompts: self.prompts.clone(),
                pending: self.pending.clone(),
                tools_list_count: self.tools_list_count.clone(),
                close_count: self.close_count.clone(),
                initialize_params: self.initialize_params.clone(),
            };
            tokio::spawn(async move { serve(state, rx, transport).await });
        }
    }

    struct ServerState {
        name: RuntimeString,
        version: RuntimeString,
        instructions: Option<RuntimeString>,
        capabilities: Value,
        tools: Arc<Mutex<Vec<TestTool>>>,
        resources: Arc<Mutex<Vec<TestResource>>>,
        prompts: Arc<Mutex<Vec<TestPrompt>>>,
        pending: Arc<Mutex<HashMap<i64, oneshot::Sender<Value>>>>,
        tools_list_count: Arc<AtomicU64>,
        close_count: Arc<AtomicU64>,
        initialize_params: Arc<Mutex<Option<Value>>>,
    }

    async fn serve(
        state: ServerState,
        mut rx: mpsc::UnboundedReceiver<TransportEvent>,
        transport: Arc<dyn Transport>,
    ) {
        while let Some(ev) = rx.recv().await {
            match ev {
                TransportEvent::Message(v) => {
                    // Response to our own roots/list request.
                    if v.get("id").is_some()
                        && (v.get("result").is_some() || v.get("error").is_some())
                        && v.get("method").is_none()
                    {
                        if let Some(id) = v.get("id").and_then(Value::as_i64) {
                            if let Some(tx) = state.pending.lock().unwrap().remove(&id) {
                                let _ = tx.send(v.get("result").cloned().unwrap_or(Value::Null));
                            }
                        }
                        continue;
                    }
                    // A request from the client.
                    if let (Some(method), Some(id)) = (
                        v.get("method").and_then(Value::as_str),
                        v.get("id").cloned(),
                    ) {
                        let params = v.get("params").cloned().unwrap_or(Value::Null);
                        if let Some(result) = handle_request(&state, method, &params) {
                            let _ = transport
                                .send(json!({ "jsonrpc": "2.0", "id": id, "result": result }))
                                .await;
                        } else {
                            let _ = transport
                                .send(json!({
                                    "jsonrpc": "2.0",
                                    "id": id,
                                    "error": { "code": -32601, "message": "method not found" }
                                }))
                                .await;
                        }
                    }
                    // Notifications (initialized) are ignored.
                }
                TransportEvent::Close => {
                    state.close_count.fetch_add(1, Ordering::SeqCst);
                    break;
                }
                TransportEvent::Error(_) => {}
            }
        }
    }

    fn handle_request(
        state: &ServerState,
        method: &RuntimeString,
        params: &Value,
    ) -> Option<Value> {
        match () {
            _ if method == "initialize" => {
                *state.initialize_params.lock().unwrap() = Some(params.clone());
                // Advertise the tools/prompts capabilities the server actually
                // has, merged over any explicitly-configured capabilities (which
                // carry flags like listChanged).
                let mut caps = state.capabilities.clone();
                let caps_obj = caps.as_object_mut().unwrap();
                if !state.tools.lock().unwrap().is_empty() && !caps_obj.contains_key("tools") {
                    caps_obj.insert("tools", json!({}));
                }
                if !state.prompts.lock().unwrap().is_empty() && !caps_obj.contains_key("prompts") {
                    caps_obj.insert("prompts", json!({}));
                }
                let mut result = json!({
                    "protocolVersion": PROTOCOL_VERSION,
                    "capabilities": caps,
                    "serverInfo": { "name": state.name, "version": state.version },
                });
                if let Some(instr) = &state.instructions {
                    result
                        .as_object_mut()
                        .unwrap()
                        .insert("instructions", json!(instr));
                }
                Some(result)
            }
            _ if method == "tools/list" => {
                state.tools_list_count.fetch_add(1, Ordering::SeqCst);
                let tools: Vec<Value> = state
                    .tools
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|t| {
                        let mut obj = json!({
                            "name": t.name,
                            "description": t.description,
                            "inputSchema": t.input_schema,
                        });
                        if let Some(anno) = &t.annotations {
                            obj.as_object_mut()
                                .unwrap()
                                .insert("annotations", anno.clone());
                        }
                        obj
                    })
                    .collect();
                Some(json!({ "tools": tools }))
            }
            _ if method == "tools/call" => {
                let name = params
                    .get("name")
                    .and_then(Value::as_str)
                    .cloned()
                    .unwrap_or_default();
                let args = params.get("arguments").cloned().unwrap_or(json!({}));
                let handler = state
                    .tools
                    .lock()
                    .unwrap()
                    .iter()
                    .find(|t| t.name == name)
                    .map(|t| t.handler.clone());
                handler.map(|h| h(args))
            }
            _ if method == "prompts/list" => {
                let prompts: Vec<Value> = state
                    .prompts
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|p| {
                        let mut obj = json!({ "name": p.name, "arguments": p.arguments });
                        if let Some(d) = &p.description {
                            obj.as_object_mut().unwrap().insert("description", json!(d));
                        }
                        obj
                    })
                    .collect();
                Some(json!({ "prompts": prompts }))
            }
            _ if method == "prompts/get" => {
                let name = params
                    .get("name")
                    .and_then(Value::as_str)
                    .cloned()
                    .unwrap_or_default();
                let args: RuntimeObject = params
                    .get("arguments")
                    .and_then(Value::as_object)
                    .cloned()
                    .unwrap_or_default();
                let handler = state
                    .prompts
                    .lock()
                    .unwrap()
                    .iter()
                    .find(|p| p.name == name)
                    .map(|p| p.handler.clone());
                handler.map(|h| h(args))
            }
            _ if method == "resources/list" => {
                let resources: Vec<Value> = state
                    .resources
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|r| {
                        json!({
                            "uri": r.uri,
                            "name": r.name,
                            "mimeType": r.mime_type,
                            "description": r.description,
                        })
                    })
                    .collect();
                Some(json!({ "resources": resources }))
            }
            _ if method == "resources/read" => Some(json!({ "contents": [] })),
            _ => None,
        }
    }

    struct BadServer;

    #[async_trait]
    impl McpInProcessServer for BadServer {
        async fn connect(&self, _transport: Box<dyn Transport>) {
            // A server that never drives its transport: dropping the server end
            // closes the client's connection, so the handshake fails and the
            // managed client degrades to `failed`.
        }
    }

    struct RecoveringServer {
        ready: AtomicBool,
        inner: TestMcpServer,
    }

    #[async_trait]
    impl McpInProcessServer for RecoveringServer {
        async fn connect(&self, transport: Box<dyn Transport>) {
            if self.ready.load(Ordering::SeqCst) {
                self.inner.connect(transport).await;
            }
        }
    }

    // ---- helpers ----

    async fn call_text(tool: &Arc<dyn Tool>, input: Value) -> RuntimeString {
        let ctx = make_ctx(".");
        let result = tool.call(input, &ctx, None).await.unwrap();
        let text = result
            .serializable
            .model_content
            .iter()
            .filter_map(|b| match b {
                OutputBlock::Text(t) => Some(t.text.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        join_runtime_strings(&text, "")
    }

    async fn wait_for<F: Fn() -> bool>(cond: F) {
        for _ in 0..300 {
            if cond() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("condition not met within timeout");
    }

    fn unique_test_dir(prefix: &str) -> String {
        let base = std::env::var("TMPDIR")
            .or_else(|_| std::env::var("TEMP"))
            .unwrap_or_else(|_| ".".to_string());
        format!(
            "{base}/{prefix}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )
    }

    fn parse_http_json(bytes: &[u8]) -> Value {
        let mut decoder = crate::runtime_values::WhatwgUtf8::new();
        let head = decoder.feed(bytes);
        let text = head.concat(&decoder.finish());
        runtime_json::parse(&text).unwrap_or(Value::Null)
    }

    fn tool_names(mc: &ManagedClient) -> Vec<RuntimeString> {
        let mut names: Vec<RuntimeString> = mc.tools().iter().map(|t| t.name()).collect();
        names.sort();
        names
    }

    fn empty_schema() -> Value {
        json!({ "type": "object" })
    }

    struct RecordingMcpElicitationPresenter {
        events: mpsc::UnboundedSender<McpElicitationEvent>,
    }

    #[async_trait]
    impl McpElicitationPresenter for RecordingMcpElicitationPresenter {
        async fn present(&self, event: McpElicitationEvent) {
            let _ = self.events.send(event);
        }
    }

    // ---- in-process transport tests ----

    #[tokio::test]
    async fn in_process_connects_and_round_trips_a_tool() {
        let server = Arc::new(TestMcpServer::new("inproc", "0.0.0"));
        server.register_tool(
            "add",
            "add two numbers",
            json!({ "type": "object", "properties": { "a": { "type": "number" }, "b": { "type": "number" } } }),
            None,
            Arc::new(|args: Value| {
                let a = args.get("a").and_then(Value::as_f64).unwrap_or(0.0);
                let b = args.get("b").and_then(Value::as_f64).unwrap_or(0.0);
                json!({ "content": [ { "type": "text", "text": fmt_num(a + b) } ] })
            }),
        );
        let config = McpServerConfig::InProcess {
            name: "ip".into(),
            server: server.clone(),
            options: McpServerOptions::default(),
        };
        let client = connect_mcp_server(&config, &test_connect_options()).await;
        assert_eq!(client.state, McpConnectionState::Ready);
        let add = client
            .tools()
            .into_iter()
            .find(|t| t.name() == "mcp__ip__add")
            .expect("add tool present");
        assert!(call_text(&add, json!({ "a": 2, "b": 3 }))
            .await
            .contains("5"));
        client.close().await;
    }

    #[tokio::test]
    async fn mcp_server_elicitation_routes_structured_response() {
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        let next_public_id = Arc::new(AtomicU64::new(0));
        let id_counter = next_public_id.clone();
        let router = McpElicitationRouter::new(
            Some(Arc::new(RecordingMcpElicitationPresenter {
                events: event_tx,
            })),
            Arc::new(move || {
                format!(
                    "public-elicit-{}",
                    id_counter.fetch_add(1, Ordering::SeqCst)
                )
            }),
        );
        let turn_signal = AbortSignal::new();
        router.set_turn_signal(Some(turn_signal.clone()));

        let first_server = Arc::new(TestMcpServer::new("first", "1.0.0"));
        let second_server = Arc::new(TestMcpServer::new("second", "1.0.0"));
        let first_config = McpServerConfig::InProcess {
            name: "first".to_string(),
            server: first_server.clone(),
            options: McpServerOptions::default(),
        };
        let second_config = McpServerConfig::InProcess {
            name: "second".to_string(),
            server: second_server.clone(),
            options: McpServerOptions::default(),
        };
        let connect_options = ConnectMcpOptions {
            mcp_elicitation_router: Some(router.clone()),
            ..test_connect_options()
        };
        let first_client = connect_mcp_server(&first_config, &connect_options).await;
        let second_client = connect_mcp_server(&second_config, &connect_options).await;
        let client_capabilities = first_server.client_capabilities().unwrap();
        assert_eq!(
            client_capabilities["elicitation"],
            json!({ "form": {}, "url": {} })
        );
        assert_eq!(
            client_capabilities["extensions"],
            json!({ "openai/form": {} })
        );

        let request_params = json!({
            "_meta": { "persist": true },
            "message": "Allow this app?",
            "requestedSchema": {
                "type": "object",
                "properties": {
                    "account": {
                        "type": "string",
                        "title": "Account",
                        "format": "email",
                        "minLength": 3
                    },
                    "days": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 30
                    },
                    "remember": { "type": "boolean", "default": true },
                    "scope": {
                        "type": "string",
                        "oneOf": [
                            { "const": "read", "title": "Read only" },
                            { "const": "write", "title": "Read and write" }
                        ]
                    },
                    "labels": {
                        "type": "array",
                        "items": {
                            "anyOf": [
                                { "const": "work", "title": "Work" },
                                { "const": "personal", "title": "Personal" }
                            ]
                        }
                    }
                },
                "required": ["account", "scope"]
            }
        });
        let first_request = {
            let server = first_server.clone();
            let params = request_params.clone();
            tokio::spawn(async move {
                server
                    .request_client(41, "elicitation/create", Some(params))
                    .await
            })
        };
        let second_request = {
            let server = second_server.clone();
            let params = request_params.clone();
            tokio::spawn(async move {
                server
                    .request_client(41, "elicitation/create", Some(params))
                    .await
            })
        };
        let first_event = event_rx.recv().await.expect("first elicitation event");
        let second_event = event_rx.recv().await.expect("second elicitation event");
        assert_ne!(first_event.request_id, second_event.request_id);
        assert_eq!(
            first_event.request.meta(),
            Some(&json!({ "persist": true }))
        );

        for event in [first_event, second_event] {
            let response = if event.server_name == "first" {
                McpElicitationResponse {
                    action: McpElicitationAction::Accept,
                    content: Some(json!({
                        "account": "person@example.com",
                        "days": 7,
                        "remember": true,
                        "scope": "read",
                        "labels": ["work"]
                    })),
                    meta: Some(json!({ "persist": true })),
                }
            } else {
                McpElicitationResponse {
                    action: McpElicitationAction::Decline,
                    content: Some(json!({ "mustBeRemoved": true })),
                    meta: None,
                }
            };
            router
                .resolve(&event.request_id, response)
                .expect("resolve public elicitation id");
        }
        let first_result = first_request.await.unwrap();
        let second_result = second_request.await.unwrap();
        let results = [first_result, second_result];
        assert!(results.iter().any(|result| {
            result.get("action") == Some(&json!("accept"))
                && result.get("content").and_then(|value| value.get("scope"))
                    == Some(&json!("read"))
                && result.get("_meta") == Some(&json!({ "persist": true }))
        }));
        assert!(results.iter().any(|result| {
            result.get("action") == Some(&json!("decline")) && result.get("content").is_none()
        }));
        assert_eq!(router.pending_count(), 0);

        let aborted_signal = AbortSignal::new();
        router.set_turn_signal(Some(aborted_signal.clone()));
        let aborted_request = {
            let server = first_server.clone();
            tokio::spawn(async move {
                server
                    .request_client(42, "elicitation/create", Some(request_params))
                    .await
            })
        };
        let aborted_event = event_rx.recv().await.expect("aborted elicitation event");
        assert_eq!(aborted_event.server_name, "first");
        aborted_signal.cancel();
        assert_eq!(
            aborted_request.await.unwrap().get("action"),
            Some(&json!("cancel"))
        );

        let close_signal = AbortSignal::new();
        router.set_turn_signal(Some(close_signal));
        let closed_request = {
            let server = second_server.clone();
            tokio::spawn(async move {
                server
                    .request_client(
                        43,
                        "openai/form",
                        Some(json!({
                            "_meta": { "persist": false },
                            "message": "Select a template",
                            "requestedSchema": { "type": "object" }
                        })),
                    )
                    .await
            })
        };
        let closed_event = event_rx.recv().await.expect("closed elicitation event");
        assert_eq!(closed_event.server_name, "second");
        second_client.close().await;
        assert_eq!(
            closed_request.await.unwrap().get("action"),
            Some(&json!("cancel"))
        );

        let unsupported_server = Arc::new(TestMcpServer::new("unsupported", "1.0.0"));
        let unsupported_config = McpServerConfig::InProcess {
            name: "unsupported".to_string(),
            server: unsupported_server.clone(),
            options: McpServerOptions::default(),
        };
        let unsupported_client =
            connect_mcp_server(&unsupported_config, &test_connect_options()).await;
        assert!(unsupported_server
            .client_capabilities()
            .and_then(|caps| caps.get("elicitation").cloned())
            .is_none());
        let declined = unsupported_server
            .request_client(
                41,
                "elicitation/create",
                Some(json!({
                    "message": "Non-compliant request",
                    "requestedSchema": { "type": "object", "properties": {} }
                })),
            )
            .await;
        assert_eq!(declined.get("action"), Some(&json!("decline")));

        first_client.close().await;
        unsupported_client.close().await;
        assert_eq!(router.pending_count(), 0);
    }

    /// The per-TOOL off switch: a name on the server's deny list is not
    /// registered at all, so the model never sees it and the other tools on the
    /// same server keep working. Refusing it at CALL time instead would still
    /// advertise it and burn a turn on a tool the user turned off.
    #[tokio::test]
    async fn denied_tools_are_dropped_from_the_registered_set() {
        let server = Arc::new(TestMcpServer::new("filtered", "1.0.0"));
        for name in ["kept", "denied"] {
            server.register_tool(
                name,
                "a tool",
                empty_schema(),
                None,
                Arc::new(|_| json!({ "content": [] })),
            );
        }
        let config = |disabled: Option<Vec<String>>| McpServerConfig::InProcess {
            name: "filtered".to_string(),
            server: server.clone(),
            options: McpServerOptions {
                always_load: Some(true),
                disabled_tools: disabled,
                ..Default::default()
            },
        };
        let names = |manager: &McpConnectionManager| -> Vec<String> {
            let mut names: Vec<String> = manager
                .clients()
                .iter()
                .flat_map(|client| client.tools())
                .map(|tool| tool.name().try_to_utf8().unwrap_or_default())
                .collect();
            names.sort();
            names
        };

        let mut manager = McpConnectionManager::new(
            crate::runtime_process::test_runtime_process_context(),
            McpConnectionManagerOptions::default(),
        );
        manager.connect_all(vec![config(None)]).await;
        assert_eq!(
            names(&manager),
            vec![
                "mcp__filtered__denied".to_string(),
                "mcp__filtered__kept".to_string()
            ]
        );

        // Denying one tool is a config change, so the full-set reload re-dials and
        // the server comes back with only the tool that is still on.
        manager
            .reload(vec![config(Some(vec!["denied".to_string()]))])
            .await
            .unwrap();
        assert_eq!(names(&manager), vec!["mcp__filtered__kept".to_string()]);
    }

    /// A DISABLED server is not a server the session has: it is never dialed, and
    /// carrying the off switch through a reload closes a client that was live.
    /// The enable state rides ON the config for exactly this reason — a host that
    /// only writes the on-disk `enabled: false` must not end up with the server
    /// running anyway.
    #[tokio::test]
    async fn disabled_configs_are_never_dialed_and_close_a_live_client() {
        let server = Arc::new(TestMcpServer::new("switchable", "1.0.0"));
        server.register_tool(
            "noop",
            "noop tool",
            empty_schema(),
            None,
            Arc::new(|_| json!({ "content": [] })),
        );
        let config = |enabled: Option<bool>| McpServerConfig::InProcess {
            name: "switchable".to_string(),
            server: server.clone(),
            options: McpServerOptions {
                always_load: Some(true),
                enabled,
                ..Default::default()
            },
        };

        let mut manager = McpConnectionManager::new(
            crate::runtime_process::test_runtime_process_context(),
            McpConnectionManagerOptions::default(),
        );
        manager.connect_all(vec![config(Some(false))]).await;
        assert!(manager.config("switchable").is_none());
        assert!(manager.clients().is_empty());

        // Enabled: it dials and serves tools.
        manager.connect_all(vec![config(None)]).await;
        assert_eq!(
            manager.clients().first().map(|client| client.state),
            Some(McpConnectionState::Ready)
        );

        // Turning it off through the full-set reload closes the live client.
        manager.reload(vec![config(Some(false))]).await.unwrap();
        assert!(manager.clients().is_empty());
        assert!(manager.config("switchable").is_none());
    }

    /// The push half of the status surface has to report the transitions THEMSELVES,
    /// including the `connecting` window before a dial settles. That window is the
    /// only honest way for a host to say "starting", and it is seconds long for a
    /// cold stdio server; without it a page can only choose between showing nothing
    /// and showing a failure for a server that is fine.
    #[tokio::test]
    async fn status_sink_receives_each_lifecycle_transition_in_order() {
        let seen: Arc<Mutex<Vec<(RuntimeString, McpConnectionState)>>> =
            Arc::new(Mutex::new(Vec::new()));
        let slot = Arc::new(crate::mcp::types::McpStatusObserverSlot::new());
        {
            let seen = seen.clone();
            slot.set(Some(Arc::new(
                move |update: crate::mcp::types::McpServerStatusUpdate| {
                    seen.lock()
                        .unwrap()
                        .push((update.server_name.clone(), update.state));
                },
            )));
        }

        let server = Arc::new(TestMcpServer::new("observed", "1.0.0"));
        server.register_tool(
            "noop",
            "noop tool",
            empty_schema(),
            None,
            Arc::new(|_| json!({ "content": [] })),
        );
        let config = McpServerConfig::InProcess {
            name: "observed".to_string(),
            server: server.clone(),
            options: McpServerOptions {
                always_load: Some(true),
                ..Default::default()
            },
        };

        let mut manager = McpConnectionManager::new(
            crate::runtime_process::test_runtime_process_context(),
            McpConnectionManagerOptions {
                status_observer: Some(slot.clone()),
                ..Default::default()
            },
        );
        manager.connect_all(vec![config]).await;
        // Removing closes the connection, which is a transition of its own: a host
        // that only heard about dials would keep painting a row for a server the
        // session no longer has.
        assert!(manager.remove("observed").await);

        let recorded = seen.lock().unwrap().clone();
        assert_eq!(
            recorded,
            vec![
                (
                    RuntimeString::from("observed"),
                    McpConnectionState::Connecting
                ),
                (RuntimeString::from("observed"), McpConnectionState::Ready),
                (RuntimeString::from("observed"), McpConnectionState::Closed),
            ]
        );

        // Clearing the slot stops delivery: a host that detaches must not keep
        // receiving into a sink it no longer owns.
        slot.set(None);
        let mut detached = McpConnectionManager::new(
            crate::runtime_process::test_runtime_process_context(),
            McpConnectionManagerOptions {
                status_observer: Some(slot),
                ..Default::default()
            },
        );
        detached
            .connect_all(vec![McpServerConfig::InProcess {
                name: "detached".to_string(),
                server,
                options: McpServerOptions {
                    always_load: Some(true),
                    ..Default::default()
                },
            }])
            .await;
        assert_eq!(seen.lock().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn refetches_tools_on_list_changed() {
        let server = Arc::new(
            TestMcpServer::new("lc", "0.0.0")
                .with_capabilities(json!({ "tools": { "listChanged": true } })),
        );
        let ok = Arc::new(|_a: Value| json!({ "content": [ { "type": "text", "text": "ok" } ] }))
            as ToolHandler;
        server.register_tool("first", "first", empty_schema(), None, ok.clone());
        let config = McpServerConfig::InProcess {
            name: "lc".into(),
            server: server.clone(),
            options: McpServerOptions::default(),
        };
        let client = connect_mcp_server(&config, &test_connect_options()).await;
        assert_eq!(tool_names(&client), vec!["mcp__lc__first".to_string()]);

        server.register_tool("second", "second", empty_schema(), None, ok.clone());
        wait_for(|| {
            tool_names(&client) == vec!["mcp__lc__first".to_string(), "mcp__lc__second".to_string()]
        })
        .await;
        client.close().await;
    }

    #[tokio::test]
    async fn converges_after_a_burst_of_list_changed() {
        let server = Arc::new(
            TestMcpServer::new("burst", "0.0.0")
                .with_capabilities(json!({ "tools": { "listChanged": true } })),
        );
        let ok = Arc::new(|_a: Value| json!({ "content": [ { "type": "text", "text": "ok" } ] }))
            as ToolHandler;
        server.register_tool("a", "a", empty_schema(), None, ok.clone());
        let config = McpServerConfig::InProcess {
            name: "burst".into(),
            server: server.clone(),
            options: McpServerOptions::default(),
        };
        let client = connect_mcp_server(&config, &test_connect_options()).await;
        assert_eq!(tool_names(&client), vec!["mcp__burst__a".to_string()]);

        server.register_tool("b", "b", empty_schema(), None, ok.clone());
        server.register_tool("c", "c", empty_schema(), None, ok.clone());
        wait_for(|| {
            tool_names(&client)
                == vec![
                    "mcp__burst__a".to_string(),
                    "mcp__burst__b".to_string(),
                    "mcp__burst__c".to_string(),
                ]
        })
        .await;
        client.close().await;
    }

    #[tokio::test]
    async fn carries_read_only_hint_through_tools_list() {
        let server = Arc::new(TestMcpServer::new("anno", "0.0.0"));
        let ok = Arc::new(|_a: Value| json!({ "content": [ { "type": "text", "text": "ok" } ] }))
            as ToolHandler;
        server.register_tool(
            "search",
            "a read-only search",
            empty_schema(),
            Some(json!({ "readOnlyHint": true })),
            ok.clone(),
        );
        server.register_tool("mutate", "a writer", empty_schema(), None, ok.clone());
        server.register_tool(
            "risky",
            "read-only but destructive",
            empty_schema(),
            Some(json!({ "readOnlyHint": true, "destructiveHint": true })),
            ok.clone(),
        );
        let config = McpServerConfig::InProcess {
            name: "anno".into(),
            server: server.clone(),
            options: McpServerOptions::default(),
        };
        let client = connect_mcp_server(&config, &test_connect_options()).await;
        let find = |n: &str| {
            client
                .tools()
                .into_iter()
                .find(|t| t.name() == n)
                .unwrap_or_else(|| panic!("missing {n}"))
        };
        let search = find("mcp__anno__search");
        let mutate = find("mcp__anno__mutate");
        let risky = find("mcp__anno__risky");
        let empty = json!({});
        assert!(search.is_read_only(&empty));
        assert!(search.is_concurrency_safe(&empty));
        assert!(!mutate.is_read_only(&empty));
        assert!(!risky.is_read_only(&empty));
        assert!(risky.is_concurrency_safe(&empty));
        client.close().await;
    }

    #[tokio::test]
    async fn captures_instructions_prompts_and_answers_roots() {
        let server = Arc::new(
            TestMcpServer::new("depth", "0.0.0").with_instructions("USE THE DEPTH SERVER"),
        );
        server.register_prompt(
            "greet",
            Some("greet someone"),
            vec![json!({ "name": "who", "required": true })],
            Arc::new(|args: RuntimeObject| {
                let who = args
                    .get("who")
                    .and_then(Value::as_str)
                    .cloned()
                    .unwrap_or_default();
                let text = RuntimeString::from("Hello ").concat(&who);
                json!({ "messages": [ { "role": "user", "content": { "type": "text", "text": text } } ] })
            }),
        );
        let config = McpServerConfig::InProcess {
            name: "depth".into(),
            server: server.clone(),
            options: McpServerOptions::default(),
        };
        let client = connect_mcp_server(
            &config,
            &ConnectMcpOptions {
                cwd: Some("/work/space".into()),
                ..test_connect_options()
            },
        )
        .await;
        assert_eq!(client.state, McpConnectionState::Ready);
        assert_eq!(
            client.instructions,
            Some(RuntimeString::from("USE THE DEPTH SERVER"))
        );

        let greet = client
            .prompts
            .iter()
            .find(|p| p.name == "greet")
            .expect("greet prompt");
        assert_eq!(greet.arguments.as_ref().unwrap()[0].name, "who");
        let mut args = RuntimeObject::new();
        args.insert("who", Value::String(RuntimeString::from("Ada")));
        assert!(client
            .get_prompt(&RuntimeString::from("greet"), Some(args))
            .await
            .contains("Hello Ada"));

        let roots = server.list_roots().await;
        assert_eq!(
            roots
                .get("roots")
                .and_then(Value::as_array)
                .and_then(|roots| roots.first())
                .and_then(|r| r.get("uri"))
                .and_then(Value::as_str)
                .cloned(),
            Some(RuntimeString::from("file:///work/space"))
        );
        client.close().await;
    }

    #[tokio::test]
    async fn runtime_snapshot_reports_exposed_surface_and_shutdown_state() {
        let server =
            Arc::new(TestMcpServer::new("snap", "0.0.0").with_instructions("SNAPSHOT SERVER"));
        server.register_tool(
            "lookup",
            "lookup data",
            empty_schema(),
            Some(json!({ "readOnlyHint": true })),
            Arc::new(|_args: Value| json!({ "content": [ { "type": "text", "text": "ok" } ] })),
        );
        server.register_resource(
            "res://one",
            Some("One"),
            Some("text/plain"),
            Some("first resource"),
        );
        server.register_prompt(
            "greet",
            Some("greet someone"),
            vec![json!({ "name": "who", "required": true })],
            Arc::new(|_args: RuntimeObject| {
                json!({ "messages": [ { "role": "user", "content": { "type": "text", "text": "hello" } } ] })
            }),
        );

        let mut manager = McpConnectionManager::new(
            crate::runtime_process::test_runtime_process_context(),
            McpConnectionManagerOptions::default(),
        );
        manager
            .connect_all(vec![McpServerConfig::InProcess {
                name: "snap".into(),
                server,
                options: McpServerOptions {
                    always_load: Some(true),
                    ..Default::default()
                },
            }])
            .await;

        let snapshot = manager.runtime_snapshot();
        assert_eq!(snapshot.servers.len(), 1);
        let server = &snapshot.servers[0];
        assert_eq!(server.name, "snap");
        assert_eq!(server.state, McpConnectionState::Ready);
        assert_eq!(
            server.instructions,
            Some(RuntimeString::from("SNAPSHOT SERVER"))
        );

        assert_eq!(server.tools.len(), 1);
        let tool = &server.tools[0];
        assert_eq!(tool.name, "mcp__snap__lookup");
        assert_eq!(tool.exposure, ToolExposure::Direct);
        assert!(tool.read_only);
        assert_eq!(tool.destructive, Some(false));

        assert_eq!(server.resources.len(), 1);
        assert_eq!(server.resources[0].uri, "res://one");
        assert_eq!(server.resources[0].name, Some(RuntimeString::from("One")));
        assert_eq!(
            server.resources[0].mime_type,
            Some(RuntimeString::from("text/plain"))
        );

        assert_eq!(server.prompts.len(), 1);
        assert_eq!(server.prompts[0].name, "greet");
        assert_eq!(server.prompts[0].arguments.as_ref().unwrap()[0].name, "who");

        manager.shutdown().await;

        let closed = manager.runtime_snapshot();
        assert_eq!(closed.servers[0].state, McpConnectionState::Closed);
    }

    #[tokio::test]
    async fn visibility_change_reuses_catalog_and_retired_client_waits_for_captured_users() {
        let server = Arc::new(
            TestMcpServer::new("original", "1.0.0")
                .with_capabilities(json!({ "tools": { "listChanged": true } })),
        );
        for name in ["public", "hidden"] {
            server.register_tool(
                name,
                "visible tool",
                empty_schema(),
                None,
                Arc::new(|_| json!({ "content": [] })),
            );
        }
        server.register_resource("res://original", Some("Original"), None, None);
        let config = McpServerConfig::InProcess {
            name: "runtime".into(),
            server: server.clone(),
            options: McpServerOptions {
                enabled_tools: Some(vec!["public".into()]),
                ..Default::default()
            },
        };
        let mut manager = McpConnectionManager::new(
            crate::runtime_process::test_runtime_process_context(),
            McpConnectionManagerOptions::default(),
        );
        manager.connect_all(vec![config.clone()]).await;
        assert_eq!(server.connection_count(), 1);
        assert_eq!(server.tools_list_count.load(Ordering::SeqCst), 1);
        let old_client = manager.clients().remove(0);
        let old_tool = old_client.tools().remove(0);
        assert_eq!(old_tool.name(), "mcp__runtime__public");

        let mut visibility = config.clone();
        if let McpServerConfig::InProcess { options, .. } = &mut visibility {
            options.enabled_tools = Some(vec!["hidden".into()]);
        }
        manager.reload(vec![visibility]).await.unwrap();
        assert_eq!(server.connection_count(), 1);
        assert_eq!(server.tools_list_count.load(Ordering::SeqCst), 1);
        assert_eq!(manager.get_tools()[0].name(), "mcp__runtime__hidden");
        assert_eq!(old_tool.name(), "mcp__runtime__public");

        server.register_tool(
            "newly_listed",
            "newly listed tool",
            empty_schema(),
            None,
            Arc::new(|_| json!({ "content": [] })),
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            while manager.clients()[0]
                .raw_tools
                .as_ref()
                .unwrap()
                .lock()
                .unwrap()
                .len()
                < 3
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("server notification re-lists its tools");
        let mut newly_visible = config.clone();
        if let McpServerConfig::InProcess { options, .. } = &mut newly_visible {
            options.enabled_tools = Some(vec!["newly_listed".into()]);
        }
        manager.reload(vec![newly_visible]).await.unwrap();
        assert_eq!(server.connection_count(), 1);
        assert_eq!(server.tools_list_count.load(Ordering::SeqCst), 2);
        assert_eq!(manager.get_tools()[0].name(), "mcp__runtime__newly_listed");

        let replacement = Arc::new(TestMcpServer::new("replacement", "1.0.0"));
        replacement.register_tool(
            "new",
            "new tool",
            empty_schema(),
            None,
            Arc::new(|_| json!({ "content": [] })),
        );
        manager
            .reload(vec![McpServerConfig::InProcess {
                name: "runtime".into(),
                server: replacement,
                options: McpServerOptions::default(),
            }])
            .await
            .unwrap();
        assert_eq!(server.close_count.load(Ordering::SeqCst), 0);
        assert!(old_client
            .client
            .as_ref()
            .unwrap()
            .read_resource(&RuntimeString::from("res://original"), 1_000)
            .await
            .is_ok());
        drop(old_client);
        assert_eq!(server.close_count.load(Ordering::SeqCst), 0);
        drop(old_tool);
        tokio::time::timeout(Duration::from_secs(2), async {
            while server.close_count.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("retired transport closes after its last captured tool is released");
    }

    #[tokio::test]
    async fn mcp_runtime_mutations_refresh_session_surface() {
        let recovering_inner = TestMcpServer::new("recovering", "1.0.0");
        recovering_inner.register_tool(
            "recovered",
            "recovered tool",
            empty_schema(),
            None,
            Arc::new(|_| json!({ "content": [] })),
        );
        let recovering = Arc::new(RecoveringServer {
            ready: AtomicBool::new(false),
            inner: recovering_inner,
        });
        let recovering_config = McpServerConfig::InProcess {
            name: "runtime".to_string(),
            server: recovering.clone(),
            options: McpServerOptions {
                always_load: Some(true),
                ..Default::default()
            },
        };
        let mut manager = McpConnectionManager::new(
            crate::runtime_process::test_runtime_process_context(),
            McpConnectionManagerOptions {
                connect_timeout_ms: Some(100),
                ..Default::default()
            },
        );
        manager.connect_all(vec![recovering_config.clone()]).await;

        let failed = manager.runtime_snapshot();
        assert_eq!(failed.servers[0].state, McpConnectionState::Failed);
        assert!(failed.servers[0].tools.is_empty());

        recovering.ready.store(true, Ordering::SeqCst);
        assert_eq!(
            manager.reconnect("runtime").await,
            McpConnectionState::Ready
        );
        assert_eq!(
            manager.runtime_snapshot().servers[0].tools[0].name,
            "mcp__runtime__recovered"
        );

        let replacement = Arc::new(TestMcpServer::new("replacement", "1.0.0"));
        replacement.register_tool(
            "replacement",
            "replacement tool",
            empty_schema(),
            None,
            Arc::new(|_| json!({ "content": [] })),
        );
        manager
            .upsert(McpServerConfig::InProcess {
                name: "runtime".to_string(),
                server: replacement.clone(),
                options: McpServerOptions {
                    always_load: Some(true),
                    ..Default::default()
                },
            })
            .await;
        let replaced = manager.runtime_snapshot();
        assert_eq!(replaced.servers.len(), 1);
        assert_eq!(
            replaced.servers[0].tools[0].name,
            "mcp__runtime__replacement"
        );

        let added = Arc::new(TestMcpServer::new("added", "1.0.0"));
        manager
            .upsert(McpServerConfig::InProcess {
                name: "added".to_string(),
                server: added.clone(),
                options: McpServerOptions::default(),
            })
            .await;
        assert_eq!(manager.runtime_snapshot().servers.len(), 2);
        assert_eq!(replacement.connection_count(), 1);
        assert_eq!(added.connection_count(), 1);
        manager
            .reload(vec![
                manager.config("runtime").unwrap(),
                manager.config("added").unwrap(),
            ])
            .await
            .unwrap();
        assert_eq!(replacement.connection_count(), 1);
        assert_eq!(added.connection_count(), 1);
        assert!(!manager.remove("missing").await);
        let duplicate = Arc::new(TestMcpServer::new("duplicate", "1.0.0"));
        let before_duplicate_reload = manager.runtime_snapshot();
        assert!(manager
            .reload(vec![
                McpServerConfig::InProcess {
                    name: "duplicate".to_string(),
                    server: duplicate.clone(),
                    options: McpServerOptions::default(),
                },
                McpServerConfig::InProcess {
                    name: "duplicate".to_string(),
                    server: duplicate,
                    options: McpServerOptions {
                        always_load: Some(true),
                        ..Default::default()
                    },
                },
            ])
            .await
            .is_err());
        assert_eq!(manager.runtime_snapshot(), before_duplicate_reload);
        assert!(manager.remove("runtime").await);
        assert_eq!(
            manager
                .runtime_snapshot()
                .servers
                .iter()
                .map(|server| server.name.clone())
                .collect::<Vec<_>>(),
            vec![RuntimeString::from("added")]
        );

        let reloaded = Arc::new(TestMcpServer::new("reloaded", "1.0.0"));
        manager
            .reload(vec![McpServerConfig::InProcess {
                name: "reloaded".to_string(),
                server: reloaded,
                options: McpServerOptions::default(),
            }])
            .await
            .unwrap();
        assert_eq!(
            manager
                .runtime_snapshot()
                .servers
                .iter()
                .map(|server| server.name.clone())
                .collect::<Vec<_>>(),
            vec![RuntimeString::from("reloaded")]
        );

        manager.shutdown().await;
        assert_eq!(
            manager.runtime_snapshot().servers[0].state,
            McpConnectionState::Closed
        );
    }

    #[tokio::test]
    async fn degrades_to_failed_when_server_throws_on_connect() {
        let config = McpServerConfig::InProcess {
            name: "bad".into(),
            server: Arc::new(BadServer),
            options: McpServerOptions::default(),
        };
        let client = connect_mcp_server(&config, &test_connect_options()).await;
        assert_eq!(client.state, McpConnectionState::Failed);
        assert!(client.tools().is_empty());
    }

    #[tokio::test]
    async fn truncates_instructions_to_the_cap() {
        let raw = RuntimeString::from("x".repeat(MAX_MCP_INSTRUCTIONS + 100));
        let out = truncate_instructions(Some(raw)).unwrap();
        assert!(out.len_utf16() <= MAX_MCP_INSTRUCTIONS);
        assert!(out.ends_with("… [truncated]"));
    }

    // ---- OAuth Streamable HTTP fixture ------------------------------------

    mod oauth_http_server {
        use super::*;
        use std::io::{BufRead, BufReader as StdBufReader, Read, Write};
        use std::net::{TcpListener, TcpStream};

        pub struct OAuthHttpTestServer {
            pub mcp_url: String,
            shutdown: Arc<AtomicBool>,
            token_grants: Arc<Mutex<Vec<String>>>,
            authorization_headers: Arc<Mutex<Vec<String>>>,
        }

        impl OAuthHttpTestServer {
            pub fn start() -> Self {
                Self::start_with_runtime_rejection(false)
            }

            pub fn start_with_runtime_rejection(reject_unexpired_tools_token: bool) -> Self {
                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                let port = listener.local_addr().unwrap().port();
                listener.set_nonblocking(true).unwrap();
                let base_url = format!("http://127.0.0.1:{port}");
                let shutdown = Arc::new(AtomicBool::new(false));
                let token_grants = Arc::new(Mutex::new(Vec::new()));
                let authorization_headers = Arc::new(Mutex::new(Vec::new()));
                let reject_unexpired_tools_token =
                    Arc::new(AtomicBool::new(reject_unexpired_tools_token));
                {
                    let base_url = base_url.clone();
                    let shutdown = shutdown.clone();
                    let token_grants = token_grants.clone();
                    let authorization_headers = authorization_headers.clone();
                    let reject_unexpired_tools_token = reject_unexpired_tools_token.clone();
                    std::thread::spawn(move || loop {
                        if shutdown.load(Ordering::SeqCst) {
                            break;
                        }
                        match listener.accept() {
                            Ok((stream, _)) => {
                                // `accept` inherits the listener's nonblocking flag on
                                // BSD/macOS, but `handle_connection` reads the request
                                // with blocking IO — leaving it nonblocking makes the
                                // first read fail with WouldBlock and silently drop the
                                // connection whenever the bytes have not landed yet.
                                let _ = stream.set_nonblocking(false);
                                let base_url = base_url.clone();
                                let token_grants = token_grants.clone();
                                let authorization_headers = authorization_headers.clone();
                                let reject_unexpired_tools_token =
                                    reject_unexpired_tools_token.clone();
                                std::thread::spawn(move || {
                                    handle_connection(
                                        stream,
                                        &base_url,
                                        token_grants,
                                        authorization_headers,
                                        reject_unexpired_tools_token,
                                    )
                                });
                            }
                            // WouldBlock when the nonblocking listener is idle;
                            // EINTR / ECONNABORTED under load. Neither
                            // invalidates the listener, so retry rather than
                            // killing the accept loop — exit is owned solely by
                            // `shutdown`.
                            Err(_) => {
                                std::thread::sleep(Duration::from_millis(5));
                            }
                        }
                    });
                }
                Self {
                    mcp_url: format!("{base_url}/mcp"),
                    shutdown,
                    token_grants,
                    authorization_headers,
                }
            }

            pub fn token_grants(&self) -> Vec<String> {
                self.token_grants.lock().unwrap().clone()
            }

            pub fn authorization_headers(&self) -> Vec<String> {
                self.authorization_headers.lock().unwrap().clone()
            }
        }

        impl Drop for OAuthHttpTestServer {
            fn drop(&mut self) {
                self.shutdown.store(true, Ordering::SeqCst);
            }
        }

        fn handle_connection(
            stream: TcpStream,
            base_url: &str,
            token_grants: Arc<Mutex<Vec<String>>>,
            authorization_headers: Arc<Mutex<Vec<String>>>,
            reject_unexpired_tools_token: Arc<AtomicBool>,
        ) {
            let mut reader = StdBufReader::new(stream);
            let mut request_line = String::new();
            if reader.read_line(&mut request_line).is_err() || request_line.is_empty() {
                return;
            }
            let mut parts = request_line.split_whitespace();
            let method = parts.next().unwrap_or("").to_string();
            let raw_path = parts.next().unwrap_or("/");
            let path = raw_path.split('?').next().unwrap_or(raw_path).to_string();
            let mut headers = HashMap::new();
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).is_err() {
                    return;
                }
                if line == "\r\n" || line == "\n" || line.is_empty() {
                    break;
                }
                if let Some((name, value)) = line.trim_end().split_once(':') {
                    headers.insert(name.to_ascii_lowercase(), value.trim().to_string());
                }
            }
            let body_len = headers
                .get("content-length")
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(0);
            let mut body = vec![0; body_len];
            if body_len > 0 && reader.read_exact(&mut body).is_err() {
                return;
            }

            let response = if method == "GET"
                && path.starts_with("/.well-known/oauth-protected-resource")
            {
                json_response(
                    200,
                    json!({
                        "resource": format!("{base_url}/mcp"),
                        "authorization_servers": [base_url]
                    }),
                )
            } else if method == "GET"
                && (path.contains("/.well-known/oauth-authorization-server")
                    || path.contains("/.well-known/openid-configuration"))
            {
                json_response(
                    200,
                    json!({
                        "issuer": base_url,
                        "authorization_endpoint": format!("{base_url}/authorize"),
                        "token_endpoint": format!("{base_url}/token"),
                        "registration_endpoint": format!("{base_url}/register"),
                        "response_types_supported": ["code"],
                        "grant_types_supported": ["authorization_code", "refresh_token"],
                        "code_challenge_methods_supported": ["S256"],
                        "token_endpoint_auth_methods_supported": ["none"]
                    }),
                )
            } else if method == "POST" && path == "/register" {
                json_response(
                    201,
                    json!({
                        "client_id": "local-client",
                        "redirect_uris": [DEFAULT_OAUTH_REDIRECT_URI],
                        "grant_types": ["authorization_code", "refresh_token"],
                        "response_types": ["code"],
                        "token_endpoint_auth_method": "none"
                    }),
                )
            } else if method == "POST" && path == "/token" {
                let form: HashMap<String, String> =
                    url::form_urlencoded::parse(&body).into_owned().collect();
                let grant = form.get("grant_type").cloned().unwrap_or_default();
                token_grants.lock().unwrap().push(grant);
                json_response(
                    200,
                    json!({
                        "access_token": "oauth-access-secret",
                        "refresh_token": "oauth-refresh-secret",
                        "token_type": "Bearer",
                        "expires_in": 3600,
                        "scope": "read"
                    }),
                )
            } else if method == "POST" && path == "/mcp" {
                let authorization = headers.get("authorization").cloned().unwrap_or_default();
                authorization_headers
                    .lock()
                    .unwrap()
                    .push(authorization.clone());
                let message = parse_http_json(&body);
                let message_method = message.get("method").and_then(Value::as_str);
                let is_message_method =
                    |expected: &str| message_method.is_some_and(|method| method == expected);
                let accepted_bearer = authorization == "Bearer oauth-access-secret"
                    || authorization == "Bearer unexpired-access-secret";
                let reject_runtime_token = is_message_method("tools/list")
                    && authorization == "Bearer unexpired-access-secret"
                    && reject_unexpired_tools_token.swap(false, Ordering::SeqCst);
                if !accepted_bearer || reject_runtime_token {
                    format!(
                        "HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Bearer resource_metadata=\"{base_url}/.well-known/oauth-protected-resource\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    )
                } else if !is_message_method("initialize")
                    && headers.get("mcp-session-id").map(String::as_str) != Some("session-1")
                {
                    "HTTP/1.1 409 Conflict\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        .to_string()
                } else {
                    match (message.get("id").cloned(), message_method) {
                        (Some(id), Some(method)) if method == "initialize" => json_response_with_headers(
                            200,
                            json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "result": {
                                    "protocolVersion": PROTOCOL_VERSION,
                                    "capabilities": { "tools": {} },
                                    "serverInfo": { "name": "oauth-fixture", "version": "1.0.0" }
                                }
                            }),
                            &[("mcp-session-id", "session-1")],
                        ),
                        (Some(id), Some(method)) if method == "tools/list" => json_response(
                            200,
                            json!({ "jsonrpc": "2.0", "id": id, "result": { "tools": [] } }),
                        ),
                        (Some(id), Some(method)) if method == "resources/list" => json_response(
                            200,
                            json!({ "jsonrpc": "2.0", "id": id, "result": { "resources": [] } }),
                        ),
                        (Some(id), Some(method)) if method == "prompts/list" => json_response(
                            200,
                            json!({ "jsonrpc": "2.0", "id": id, "result": { "prompts": [] } }),
                        ),
                        // A 202 with no body is a valid response to a notification, but this
                        // server only ever sees requests (they carry an `id`), and a request
                        // that gets 202 instead of a JSON-RPC body leaves the caller waiting
                        // for its 30s connect timeout. Answering `resources/templates/list`
                        // with an empty list keeps the connect path instant.
                        (Some(id), Some(method)) if method == "resources/templates/list" => json_response(
                            200,
                            json!({ "jsonrpc": "2.0", "id": id, "result": { "resourceTemplates": [] } }),
                        ),
                        _ => "HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_string(),
                    }
                }
            } else {
                "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    .to_string()
            };
            let _ = reader.get_mut().write_all(response.as_bytes());
            let _ = reader.get_mut().flush();
        }

        fn json_response(status: u16, body: Value) -> String {
            json_response_with_headers(status, body, &[])
        }

        fn json_response_with_headers(
            status: u16,
            body: Value,
            headers: &[(&str, &str)],
        ) -> String {
            let body = body.to_string();
            let reason = match status {
                200 => "OK",
                201 => "Created",
                _ => "OK",
            };
            let headers = headers
                .iter()
                .map(|(name, value)| format!("{name}: {value}\r\n"))
                .collect::<String>();
            format!(
                "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
        }
    }

    struct RecordingAuthorizeHandler {
        requests: Arc<Mutex<Vec<McpAuthorizeRequest>>>,
    }

    struct WrongStateAuthorizeHandler;

    #[async_trait]
    impl McpAuthorizeHandler for WrongStateAuthorizeHandler {
        async fn authorize(
            &self,
            _request: McpAuthorizeRequest,
            _signal: AbortSignal,
        ) -> McpAuthorizeResponse {
            McpAuthorizeResponse::Code {
                code: "authorization-code-secret".to_string(),
                state: Some("wrong-state".to_string()),
            }
        }
    }

    #[async_trait]
    impl McpAuthorizeHandler for RecordingAuthorizeHandler {
        async fn authorize(
            &self,
            request: McpAuthorizeRequest,
            _signal: AbortSignal,
        ) -> McpAuthorizeResponse {
            let state = Url::parse(&request.authorization_url).ok().and_then(|url| {
                url.query_pairs()
                    .find(|(key, _)| key == "state")
                    .map(|(_, value)| value.into_owned())
            });
            self.requests.lock().unwrap().push(request);
            McpAuthorizeResponse::Code {
                code: "authorization-code-secret".to_string(),
                state,
            }
        }
    }

    struct PolicyAwareAuthorizeHandler {
        requests: Arc<Mutex<Vec<McpAuthorizeRequest>>>,
        allowed: Arc<AtomicBool>,
        revoke_before_answer: bool,
    }

    #[async_trait]
    impl McpAuthorizeHandler for PolicyAwareAuthorizeHandler {
        async fn authorize(
            &self,
            request: McpAuthorizeRequest,
            signal: AbortSignal,
        ) -> McpAuthorizeResponse {
            let answer = RecordingAuthorizeHandler {
                requests: self.requests.clone(),
            }
            .authorize(request, signal)
            .await;
            if self.revoke_before_answer {
                self.allowed.store(false, Ordering::Release);
            }
            answer
        }
    }

    #[tokio::test]
    async fn model_oauth_admission_revalidates_live_policy_and_preserves_user_login() {
        for (
            model_triggered,
            initially_allowed,
            revoke_before_answer,
            expected_requests,
            expected_ready,
        ) in [
            (true, false, false, 0, false),
            (true, true, true, 1, false),
            (false, false, false, 1, true),
        ] {
            let server = oauth_http_server::OAuthHttpTestServer::start();
            let store = Arc::new(InMemoryCredentialStore::new());
            let requests = Arc::new(Mutex::new(Vec::new()));
            let lock_dir = unique_test_dir("klaus-mcp-oauth-live-policy");
            let allowed = Arc::new(AtomicBool::new(initially_allowed));
            let current = Arc::clone(&allowed);
            let router = McpElicitationRouter::new(None, Arc::new(|| "oauth-policy".to_owned()));
            router.set_approval_policy_provider(Arc::new(move || {
                crate::types::permission::ApprovalPolicy::Granular(
                    crate::types::permission::GranularApprovalConfig {
                        sandbox_approval: false,
                        rules: false,
                        skill_approval: false,
                        request_permissions: false,
                        mcp_elicitations: current.load(Ordering::Acquire),
                    },
                )
            }));
            let config = McpServerConfig::Http {
                name: "oauth-policy-fixture".to_owned(),
                url: server.mcp_url.clone(),
                headers: None,
                oauth: Some(McpOAuthOption::Advertised),
                options: McpServerOptions::default(),
            };
            let client = connect_mcp_server(
                &config,
                &ConnectMcpOptions {
                    credential_store: Some(store.clone()),
                    mcp_authorize_handler: Some(Arc::new(PolicyAwareAuthorizeHandler {
                        requests: requests.clone(),
                        allowed,
                        revoke_before_answer,
                    })),
                    signal: model_triggered.then(AbortSignal::new),
                    mcp_elicitation_router: Some(router),
                    oauth_lock_dir: Some(lock_dir.clone()),
                    skip_needs_auth_cache: Some(true),
                    ..test_connect_options()
                },
            )
            .await;
            assert_eq!(requests.lock().unwrap().len(), expected_requests);
            assert_eq!(client.state == McpConnectionState::Ready, expected_ready);
            assert_eq!(server.token_grants().len(), usize::from(expected_ready));
            let key = oauth_server_key_of(&config).unwrap();
            let stored = store.get(&format!("mcp:oauth:{key}:tokens")).await.unwrap();
            assert_eq!(stored.is_some(), expected_ready);
            client.close().await;
            let _ = std::fs::remove_dir_all(lock_dir);
        }
    }

    #[tokio::test]
    async fn oauth_connect_authorizes_persists_and_reconnects_with_a_fresh_transport() {
        let server = oauth_http_server::OAuthHttpTestServer::start();
        let store = Arc::new(InMemoryCredentialStore::new());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let lock_dir = unique_test_dir("klaus-mcp-oauth-connect");
        let config = McpServerConfig::Http {
            name: "oauth-fixture".to_string(),
            url: server.mcp_url.clone(),
            headers: None,
            oauth: Some(McpOAuthOption::Advertised),
            options: McpServerOptions::default(),
        };

        let client = connect_mcp_server(
            &config,
            &ConnectMcpOptions {
                credential_store: Some(store.clone()),
                mcp_authorize_handler: Some(Arc::new(RecordingAuthorizeHandler {
                    requests: requests.clone(),
                })),
                oauth_lock_dir: Some(lock_dir.clone()),
                skip_needs_auth_cache: Some(true),
                ..test_connect_options()
            },
        )
        .await;

        assert_eq!(client.state, McpConnectionState::Ready);
        {
            let requests = requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].server_name, "oauth-fixture");
            assert_eq!(requests[0].redirect_uri, DEFAULT_OAUTH_REDIRECT_URI);
            let authorization_url = Url::parse(&requests[0].authorization_url).unwrap();
            let query: HashMap<String, String> =
                authorization_url.query_pairs().into_owned().collect();
            assert_eq!(
                query.get("client_id").map(String::as_str),
                Some("local-client")
            );
            assert_eq!(
                query.get("code_challenge_method").map(String::as_str),
                Some("S256")
            );
            assert!(query
                .get("code_challenge")
                .is_some_and(|value| !value.is_empty()));
            assert!(query.get("state").is_some_and(|value| !value.is_empty()));
        }

        assert_eq!(server.token_grants(), vec!["authorization_code"]);
        assert!(server
            .authorization_headers()
            .iter()
            .any(|value| value == "Bearer oauth-access-secret"));
        let server_key = oauth_server_key_of(&config).unwrap();
        let stored = store
            .get(&format!("mcp:oauth:{server_key}:tokens"))
            .await
            .unwrap()
            .expect("OAuth token bundle persisted");
        assert!(stored.contains("oauth-access-secret"));
        assert!(stored.contains("oauth-refresh-secret"));

        client.close().await;
        let _ = std::fs::remove_dir_all(lock_dir);
    }

    #[tokio::test]
    async fn oauth_connect_rejects_a_mismatched_state_before_code_exchange() {
        let server = oauth_http_server::OAuthHttpTestServer::start();
        let store = Arc::new(InMemoryCredentialStore::new());
        let lock_dir = unique_test_dir("klaus-mcp-oauth-state");
        let config = McpServerConfig::Http {
            name: "oauth-fixture".to_string(),
            url: server.mcp_url.clone(),
            headers: None,
            oauth: Some(McpOAuthOption::Advertised),
            options: McpServerOptions::default(),
        };

        let client = connect_mcp_server(
            &config,
            &ConnectMcpOptions {
                credential_store: Some(store.clone()),
                mcp_authorize_handler: Some(Arc::new(WrongStateAuthorizeHandler)),
                oauth_lock_dir: Some(lock_dir.clone()),
                skip_needs_auth_cache: Some(true),
                ..test_connect_options()
            },
        )
        .await;

        assert_eq!(client.state, McpConnectionState::NeedsAuth);
        assert!(server.token_grants().is_empty());
        let server_key = oauth_server_key_of(&config).unwrap();
        assert!(store
            .get(&format!("mcp:oauth:{server_key}:tokens"))
            .await
            .unwrap()
            .is_none());
        let _ = std::fs::remove_dir_all(lock_dir);
    }

    #[tokio::test]
    async fn oauth_connect_refreshes_persisted_tokens_before_the_remote_handshake() {
        use crate::mcp::oauth::{
            OAuthClientInformationFull, OAuthClientProvider, OAuthDiscoveryState, OAuthMetadata,
            OAuthTokens,
        };

        let server = oauth_http_server::OAuthHttpTestServer::start();
        let store = Arc::new(InMemoryCredentialStore::new());
        let lock_dir = unique_test_dir("klaus-mcp-oauth-refresh");
        let config = McpServerConfig::Http {
            name: "oauth-fixture".to_string(),
            url: server.mcp_url.clone(),
            headers: None,
            oauth: Some(McpOAuthOption::Advertised),
            options: McpServerOptions::default(),
        };
        let base_url = server.mcp_url.trim_end_matches("/mcp").to_string();
        let provider = McpOAuthProvider::new(McpOAuthProviderOptions {
            credential_store: store.clone(),
            server_key: oauth_server_key_of(&config).unwrap(),
            server_name: "oauth-fixture".to_string(),
            server_url: server.mcp_url.clone(),
            redirect_uri: DEFAULT_OAUTH_REDIRECT_URI.to_string(),
            scopes: None,
            on_authorization_url: Arc::new(|_url| {}),
            lock_dir: Some(lock_dir.clone()),
            fetch: None,
        });
        provider
            .save_discovery_state(OAuthDiscoveryState {
                authorization_server_url: Some(RuntimeString::from(base_url.clone())),
                resource_metadata_url: None,
                resource_metadata: None,
                authorization_server_metadata: Some(OAuthMetadata {
                    issuer: RuntimeString::from(base_url.clone()),
                    authorization_endpoint: RuntimeString::from(format!("{base_url}/authorize")),
                    token_endpoint: RuntimeString::from(format!("{base_url}/token")),
                    registration_endpoint: Some(RuntimeString::from(format!(
                        "{base_url}/register"
                    ))),
                    response_types_supported: vec![RuntimeString::from("code")],
                    ..Default::default()
                }),
            })
            .await
            .unwrap();
        provider
            .save_client_information(OAuthClientInformationFull {
                client_id: RuntimeString::from("local-client"),
                redirect_uris: vec![RuntimeString::from(DEFAULT_OAUTH_REDIRECT_URI)],
                ..Default::default()
            })
            .await
            .unwrap();
        provider
            .save_tokens(OAuthTokens {
                access_token: RuntimeString::from("stale-access-secret"),
                refresh_token: Some(RuntimeString::from("persisted-refresh-secret")),
                token_type: RuntimeString::from("Bearer"),
                expires_in: Some(1.0),
                ..Default::default()
            })
            .await
            .unwrap();

        let client = connect_mcp_server(
            &config,
            &ConnectMcpOptions {
                credential_store: Some(store),
                oauth_lock_dir: Some(lock_dir.clone()),
                skip_needs_auth_cache: Some(true),
                ..test_connect_options()
            },
        )
        .await;

        assert_eq!(client.state, McpConnectionState::Ready);
        assert_eq!(server.token_grants(), vec!["refresh_token"]);
        assert!(server
            .authorization_headers()
            .iter()
            .any(|value| value == "Bearer oauth-access-secret"));
        client.close().await;
        let _ = std::fs::remove_dir_all(lock_dir);
    }

    #[tokio::test]
    async fn oauth_runtime_refresh_preserves_the_initialized_mcp_session() {
        use crate::mcp::oauth::{
            OAuthClientInformationFull, OAuthClientProvider, OAuthDiscoveryState, OAuthMetadata,
            OAuthTokens,
        };

        let server = oauth_http_server::OAuthHttpTestServer::start_with_runtime_rejection(true);
        let store = Arc::new(InMemoryCredentialStore::new());
        let lock_dir = unique_test_dir("klaus-mcp-oauth-runtime-refresh");
        let config = McpServerConfig::Http {
            name: "oauth-fixture".to_string(),
            url: server.mcp_url.clone(),
            headers: None,
            oauth: Some(McpOAuthOption::Advertised),
            options: McpServerOptions::default(),
        };
        let base_url = server.mcp_url.trim_end_matches("/mcp").to_string();
        let provider = McpOAuthProvider::new(McpOAuthProviderOptions {
            credential_store: store.clone(),
            server_key: oauth_server_key_of(&config).unwrap(),
            server_name: "oauth-fixture".to_string(),
            server_url: server.mcp_url.clone(),
            redirect_uri: DEFAULT_OAUTH_REDIRECT_URI.to_string(),
            scopes: None,
            on_authorization_url: Arc::new(|_url| {}),
            lock_dir: Some(lock_dir.clone()),
            fetch: None,
        });
        provider
            .save_discovery_state(OAuthDiscoveryState {
                authorization_server_url: Some(RuntimeString::from(base_url.clone())),
                resource_metadata_url: None,
                resource_metadata: None,
                authorization_server_metadata: Some(OAuthMetadata {
                    issuer: RuntimeString::from(base_url.clone()),
                    authorization_endpoint: RuntimeString::from(format!("{base_url}/authorize")),
                    token_endpoint: RuntimeString::from(format!("{base_url}/token")),
                    registration_endpoint: Some(RuntimeString::from(format!(
                        "{base_url}/register"
                    ))),
                    response_types_supported: vec![RuntimeString::from("code")],
                    ..Default::default()
                }),
            })
            .await
            .unwrap();
        provider
            .save_client_information(OAuthClientInformationFull {
                client_id: RuntimeString::from("local-client"),
                redirect_uris: vec![RuntimeString::from(DEFAULT_OAUTH_REDIRECT_URI)],
                ..Default::default()
            })
            .await
            .unwrap();
        provider
            .save_tokens(OAuthTokens {
                access_token: RuntimeString::from("unexpired-access-secret"),
                refresh_token: Some(RuntimeString::from("persisted-refresh-secret")),
                token_type: RuntimeString::from("Bearer"),
                expires_in: Some(3600.0),
                ..Default::default()
            })
            .await
            .unwrap();

        let client = connect_mcp_server(
            &config,
            &ConnectMcpOptions {
                credential_store: Some(store),
                oauth_lock_dir: Some(lock_dir.clone()),
                skip_needs_auth_cache: Some(true),
                ..test_connect_options()
            },
        )
        .await;

        assert_eq!(client.state, McpConnectionState::Ready);
        assert_eq!(server.token_grants(), vec!["refresh_token"]);
        assert!(server
            .authorization_headers()
            .iter()
            .any(|value| value == "Bearer unexpired-access-secret"));
        assert!(server
            .authorization_headers()
            .iter()
            .any(|value| value == "Bearer oauth-access-secret"));
        client.close().await;
        let _ = std::fs::remove_dir_all(lock_dir);
    }

    // ---- SSE transport tests (against a hand-rolled local SSE server) ----

    mod sse_server {
        use super::*;
        use std::io::{BufRead, BufReader as StdBufReader, Read, Write};
        use std::net::TcpListener;
        use std::sync::mpsc as std_mpsc;

        static SESSION_SEQ: AtomicU64 = AtomicU64::new(0);

        pub struct SseTestServer {
            pub url: String,
            last_auth: Arc<Mutex<Option<String>>>,
            shutdown: Arc<AtomicBool>,
        }

        impl SseTestServer {
            pub fn start(handle: tokio::runtime::Handle) -> SseTestServer {
                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                let port = listener.local_addr().unwrap().port();
                listener.set_nonblocking(true).unwrap();
                let last_auth = Arc::new(Mutex::new(None));
                let shutdown = Arc::new(AtomicBool::new(false));
                let sessions: Arc<Mutex<HashMap<String, Arc<InMemoryTransport>>>> =
                    Arc::new(Mutex::new(HashMap::new()));
                {
                    let last_auth = last_auth.clone();
                    let shutdown = shutdown.clone();
                    let sessions = sessions.clone();
                    std::thread::spawn(move || loop {
                        if shutdown.load(Ordering::SeqCst) {
                            break;
                        }
                        match listener.accept() {
                            Ok((stream, _)) => {
                                // Same reason as the OAuth fixture server: restore
                                // blocking mode so `handle_conn`'s blocking reads cannot
                                // fail with WouldBlock on a not-yet-arrived request.
                                let _ = stream.set_nonblocking(false);
                                let last_auth = last_auth.clone();
                                let sessions = sessions.clone();
                                let handle = handle.clone();
                                std::thread::spawn(move || {
                                    handle_conn(stream, last_auth, sessions, handle)
                                });
                            }
                            // WouldBlock when idle; EINTR / ECONNABORTED under
                            // load. Neither invalidates the listener, so retry
                            // rather than killing the accept loop — exit is owned
                            // solely by `shutdown`.
                            Err(_) => {
                                std::thread::sleep(std::time::Duration::from_millis(5));
                            }
                        }
                    });
                }
                SseTestServer {
                    url: format!("http://127.0.0.1:{port}/sse"),
                    last_auth,
                    shutdown,
                }
            }

            pub fn auth_seen(&self) -> Option<String> {
                self.last_auth.lock().unwrap().clone()
            }
        }

        impl Drop for SseTestServer {
            fn drop(&mut self) {
                self.shutdown.store(true, Ordering::SeqCst);
            }
        }

        fn empty_response(status: &str) -> String {
            format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        }

        fn write_empty_response(stream: &mut std::net::TcpStream, status: &str) {
            let _ = stream.write_all(empty_response(status).as_bytes());
            let _ = stream.flush();
        }

        #[test]
        fn one_request_empty_responses_explicitly_close_the_connection() {
            assert_eq!(
                empty_response("202 Accepted"),
                "HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
            assert_eq!(
                empty_response("404 Not Found"),
                "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
        }

        fn handle_conn(
            mut stream: std::net::TcpStream,
            last_auth: Arc<Mutex<Option<String>>>,
            sessions: Arc<Mutex<HashMap<String, Arc<InMemoryTransport>>>>,
            handle: tokio::runtime::Handle,
        ) {
            stream.set_nonblocking(false).ok();
            let read_stream = stream.try_clone().unwrap();
            let mut reader = StdBufReader::new(read_stream);
            let mut request_line = String::new();
            if reader.read_line(&mut request_line).is_err() {
                return;
            }
            let mut parts = request_line.split_whitespace();
            let method = parts.next().unwrap_or("").to_string();
            let path = parts.next().unwrap_or("").to_string();
            let mut headers: HashMap<String, String> = HashMap::new();
            loop {
                let mut h = String::new();
                match reader.read_line(&mut h) {
                    Ok(0) => break,
                    Ok(_) => {
                        let t = h.trim_end();
                        if t.is_empty() {
                            break;
                        }
                        if let Some((k, v)) = t.split_once(':') {
                            headers.insert(k.trim().to_lowercase(), v.trim().to_string());
                        }
                    }
                    Err(_) => return,
                }
            }

            if method == "GET" && path.starts_with("/sse") {
                *last_auth.lock().unwrap() = headers.get("authorization").cloned();
                let (end_a, end_b) = InMemoryTransport::create_linked_pair();
                let end_b = Arc::new(end_b);
                let session_id = format!("s{}", SESSION_SEQ.fetch_add(1, Ordering::SeqCst));

                let server = Arc::new(TestMcpServer::new("fixture", "0.0.0"));
                server.register_tool(
                    "echo",
                    "echo back",
                    json!({ "type": "object", "properties": { "msg": { "type": "string" } }, "required": ["msg"] }),
                    None,
                    Arc::new(|args: Value| {
                        let msg = args
                            .get("msg")
                            .and_then(Value::as_str)
                            .cloned()
                            .unwrap_or_default();
                        let text = RuntimeString::from("echo:").concat(&msg);
                        json!({ "content": [ { "type": "text", "text": text } ] })
                    }),
                );

                let (tx_ev, mut rx_ev) = mpsc::unbounded_channel();
                {
                    let end_b = end_b.clone();
                    handle.block_on(async move { end_b.start(tx_ev).await.unwrap() });
                }
                let (ready_tx, ready_rx) = std_mpsc::channel();
                let server = server.clone();
                handle.spawn(async move {
                    server.connect(Box::new(end_a)).await;
                    let _ = ready_tx.send(());
                });
                ready_rx
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .expect("SSE test server did not finish in-memory MCP setup");
                sessions.lock().unwrap().insert(session_id.clone(), end_b);

                let _ = write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\n\r\n"
                );
                let _ = write!(
                    stream,
                    "event: endpoint\r\ndata: /messages?sessionId={session_id}\r\n\r\n"
                );
                let _ = stream.flush();

                loop {
                    match rx_ev.blocking_recv() {
                        Some(TransportEvent::Message(v)) => {
                            let data = runtime_json::stringify(&v).to_node_utf8();
                            if stream.write_all(b"event: message\r\ndata: ").is_err()
                                || stream.write_all(&data).is_err()
                                || stream.write_all(b"\r\n\r\n").is_err()
                            {
                                break;
                            }
                            if stream.flush().is_err() {
                                break;
                            }
                        }
                        Some(TransportEvent::Close) | None => break,
                        Some(TransportEvent::Error(_)) => {}
                    }
                }
                sessions.lock().unwrap().remove(&session_id);
                return;
            }

            if method == "POST" && path.starts_with("/messages") {
                let sid = path
                    .split_once("sessionId=")
                    .map(|(_, s)| s.to_string())
                    .unwrap_or_default();
                let len: usize = headers
                    .get("content-length")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);
                let mut body = vec![0u8; len];
                if reader.read_exact(&mut body).is_err() {
                    return;
                }
                let v = parse_http_json(&body);
                let end = sessions.lock().unwrap().get(&sid).cloned();
                if let Some(end) = end {
                    handle.block_on(async move {
                        let _ = end.send(v).await;
                    });
                    write_empty_response(&mut stream, "202 Accepted");
                } else {
                    write_empty_response(&mut stream, "404 Not Found");
                }
                return;
            }

            write_empty_response(&mut stream, "404 Not Found");
        }
    }

    #[tokio::test]
    async fn sse_connects_lists_round_trips_and_injects_stored_bearer() {
        let srv = sse_server::SseTestServer::start(tokio::runtime::Handle::current());
        let store: Arc<dyn CredentialStore> = Arc::new(InMemoryCredentialStore::new());
        let _ = store.set("mcp:fix:token", "secret-xyz").await;
        let config = McpServerConfig::Sse {
            name: "fix".into(),
            url: srv.url.clone(),
            headers: None,
            oauth: None,
            options: McpServerOptions::default(),
        };
        let client = connect_mcp_server(
            &config,
            &ConnectMcpOptions {
                credential_store: Some(store),
                ..test_connect_options()
            },
        )
        .await;
        assert_eq!(client.state, McpConnectionState::Ready);
        let echo = client
            .tools()
            .into_iter()
            .find(|t| t.name() == "mcp__fix__echo")
            .expect("echo tool present");
        assert!(call_text(&echo, json!({ "msg": "hi" }))
            .await
            .contains("echo:hi"));
        // The auth header is recorded by the server's connection thread; poll
        // briefly so a scheduling gap under parallel test load can't race the read.
        let mut seen = srv.auth_seen();
        for _ in 0..100 {
            if seen.is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            seen = srv.auth_seen();
        }
        assert_eq!(seen.as_deref(), Some("Bearer secret-xyz"));
        client.close().await;
    }

    #[tokio::test]
    async fn sse_injects_configured_headers_without_a_store() {
        let srv = sse_server::SseTestServer::start(tokio::runtime::Handle::current());
        let mut headers = HashMap::new();
        headers.insert("Authorization".to_string(), "Bearer cfg-token".to_string());
        let config = McpServerConfig::Sse {
            name: "h".into(),
            url: srv.url.clone(),
            headers: Some(headers),
            oauth: None,
            options: McpServerOptions::default(),
        };
        let client = connect_mcp_server(&config, &test_connect_options()).await;
        assert_eq!(client.state, McpConnectionState::Ready);
        // The auth header is recorded by the server's connection thread; poll
        // briefly so a scheduling gap under parallel test load can't race the read.
        let mut seen = srv.auth_seen();
        for _ in 0..100 {
            if seen.is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            seen = srv.auth_seen();
        }
        assert_eq!(seen.as_deref(), Some("Bearer cfg-token"));
        client.close().await;
    }

    /// A scripted MCP server over an in-memory transport: the test answers the
    /// handshake and drives server-to-client requests and notifications by hand.
    struct ScriptedServer {
        inbound: mpsc::UnboundedReceiver<TransportEvent>,
        transport: Arc<InMemoryTransport>,
    }

    impl ScriptedServer {
        async fn start(router: Option<McpElicitationRouter>) -> (Client, Self) {
            let (client_end, server_end) = InMemoryTransport::create_linked_pair();
            let client = match router {
                Some(router) => Client::new_for_server(None, "scripted".to_string(), Some(router)),
                None => Client::new(None),
            };
            // The handshake blocks until the scripted side answers initialize,
            // so it runs beside the test rather than ahead of it.
            let handshake = {
                let client = client.clone();
                tokio::spawn(async move {
                    client
                        .connect(
                            Arc::new(client_end),
                            RequestOptions {
                                timeout_ms: 2_000,
                                ..Default::default()
                            },
                        )
                        .await
                })
            };
            let (tx, rx) = mpsc::unbounded_channel();
            server_end.start(tx).await.expect("server transport");
            let mut server = Self {
                inbound: rx,
                transport: Arc::new(server_end),
            };
            server.answer_initialize().await;
            handshake.await.expect("handshake task").expect("handshake");
            (client, server)
        }

        async fn next_message(&mut self) -> Value {
            loop {
                match self.inbound.recv().await.expect("client message") {
                    TransportEvent::Message(message) => return message,
                    _ => continue,
                }
            }
        }

        async fn next_request(&mut self) -> (i64, RuntimeString, Value) {
            loop {
                let message = self.next_message().await;
                let method = message.get("method").and_then(Value::as_str).cloned();
                let id = message.get("id").and_then(Value::as_i64);
                let params = message.get("params").cloned().unwrap_or(Value::Null);
                if let (Some(method), Some(id)) = (method, id) {
                    return (id, method, params);
                }
            }
        }

        async fn send(&self, message: Value) {
            self.transport.send(message).await.expect("send");
        }

        /// The response frame for one server-to-client request, skipping
        /// interleaved notifications.
        async fn next_response(&mut self, id: i64) -> Value {
            loop {
                let message = self.next_message().await;
                if message.get("id").and_then(Value::as_i64) == Some(id) {
                    return message;
                }
            }
        }

        async fn answer_initialize(&mut self) {
            let (id, method, _) = self.next_request().await;
            assert_eq!(method, RuntimeString::from("initialize"));
            self.send(json!({ "jsonrpc": "2.0", "id": id, "result": {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "serverInfo": { "name": "scripted", "version": "0" } } }))
                .await;
        }
    }

    #[tokio::test]
    async fn cancelled_tool_request_waits_for_accepted_progress_after_sending_cancel() {
        let (client, mut server) = ScriptedServer::start(None).await;
        let signal = AbortSignal::new();
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let delivered = Arc::new(AtomicU64::new(0));
        let progress: ToolProgress = Box::new({
            let entered = entered.clone();
            let release = release.clone();
            let delivered = delivered.clone();
            move |_event| {
                let entered = entered.clone();
                let release = release.clone();
                let delivered = delivered.clone();
                Box::pin(async move {
                    entered.add_permits(1);
                    release.acquire_owned().await.unwrap().forget();
                    delivered.fetch_add(1, Ordering::SeqCst);
                })
            }
        });
        let mut call = tokio::spawn({
            let client = client.clone();
            let signal = signal.clone();
            async move {
                client
                    .call_tool(
                        &"progress-tool".into(),
                        json!({}),
                        2_000,
                        Some(progress),
                        Some(signal),
                    )
                    .await
            }
        });
        let (id, method, _) = server.next_request().await;
        assert_eq!(method, "tools/call");
        server
            .send(json!({"jsonrpc":"2.0", "method":"notifications/progress",
            "params":{"progressToken":id,"progress":1,"total":2}}))
            .await;
        tokio::time::timeout(Duration::from_secs(2), entered.acquire_owned())
            .await
            .expect("the pump accepts progress")
            .unwrap()
            .forget();
        signal.cancel();
        let cancelled = tokio::time::timeout(Duration::from_secs(2), server.next_message())
            .await
            .expect("cancellation remains reachable during persistence");
        assert_eq!(
            cancelled.get("method").and_then(Value::as_str),
            Some(&"notifications/cancelled".into())
        );
        assert_eq!(
            cancelled
                .pointer("/params/requestId")
                .and_then(Value::as_i64),
            Some(id)
        );
        assert!(tokio::time::timeout(Duration::from_millis(30), &mut call)
            .await
            .is_err());
        assert_eq!(delivered.load(Ordering::SeqCst), 0);
        release.add_permits(1);
        assert!(tokio::time::timeout(Duration::from_secs(2), call)
            .await
            .unwrap()
            .unwrap()
            .is_err());
        assert_eq!(delivered.load(Ordering::SeqCst), 1);
        client.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_tool_call_budget_pauses_while_an_elicitation_waits() {
        let (event_tx, mut events) = mpsc::unbounded_channel();
        let router = McpElicitationRouter::new(
            Some(Arc::new(RecordingMcpElicitationPresenter {
                events: event_tx,
            })),
            Arc::new(|| "scripted-elicit".to_string()),
        );
        let (client, mut server) = ScriptedServer::start(Some(router.clone())).await;

        let call = tokio::spawn({
            let client = client.clone();
            async move {
                client
                    .call_tool(
                        &RuntimeString::from("slow"),
                        Value::Object(RuntimeObject::new()),
                        120,
                        None,
                        None,
                    )
                    .await
            }
        });
        let (call_id, method, _) = server.next_request().await;
        assert_eq!(method, RuntimeString::from("tools/call"));
        // The server asks the user a question; the wait must not consume budget.
        server
            .send(json!({ "jsonrpc": "2.0", "id": 77, "method": "elicitation/create", "params": {
                "message": "Allow scripted?", "requestedSchema": { "type": "object", "properties": {} } } }))
            .await;
        let event = events.recv().await.expect("elicitation presented");
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert!(
            !call.is_finished(),
            "the paused budget must not expire during the wait"
        );
        router
            .resolve(&event.request_id, McpElicitationResponse::cancel())
            .expect("resolve");
        let response = server.next_response(77).await;
        assert_eq!(
            response
                .get("result")
                .and_then(|result| result.get("action"))
                .and_then(Value::as_str),
            Some(&RuntimeString::from("cancel"))
        );
        server
            .send(json!({ "jsonrpc": "2.0", "id": call_id, "result": {
                "content": [ { "type": "text", "text": "done" } ] } }))
            .await;
        let result = call.await.expect("call task").expect("call result");
        assert!(result.content.iter().any(|block| {
            block.block_type == "text" && block.text == Some(RuntimeString::from("done"))
        }));
        let _ = client.close().await;
    }

    #[tokio::test]
    async fn notifications_cancelled_settles_a_pending_elicitation() {
        let (event_tx, mut events) = mpsc::unbounded_channel();
        let router = McpElicitationRouter::new(
            Some(Arc::new(RecordingMcpElicitationPresenter {
                events: event_tx,
            })),
            Arc::new(|| "scripted-elicit".to_string()),
        );
        let (client, mut server) = ScriptedServer::start(Some(router.clone())).await;
        server
            .send(json!({ "jsonrpc": "2.0", "id": 77, "method": "elicitation/create", "params": {
                "message": "Allow scripted?", "requestedSchema": { "type": "object", "properties": {} } } }))
            .await;
        let _event = events.recv().await.expect("elicitation presented");
        assert_eq!(router.pending_count(), 1);
        // The host's question died: it cancels its own server request.
        server
            .send(
                json!({ "jsonrpc": "2.0", "method": "notifications/cancelled",
                "params": { "requestId": 77, "reason": "the question died" } }),
            )
            .await;
        let response = server.next_response(77).await;
        assert_eq!(
            response
                .get("result")
                .and_then(|result| result.get("action"))
                .and_then(Value::as_str),
            Some(&RuntimeString::from("cancel"))
        );
        assert_eq!(router.pending_count(), 0);
        let _ = client.close().await;
    }
}
