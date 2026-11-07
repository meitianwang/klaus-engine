//! MCP (Model Context Protocol) configuration and the shared data shapes the
//! client subsystem exchanges over the wire.
//!
//! [`McpServerConfig`] is the central type: a discriminated union on the
//! transport, so each transport carries only its relevant fields. The transport
//! is chosen EXPLICITLY by the caller — there is no automatic protocol fallback
//! (e.g. `http` does not silently retry as `sse`). `http` is Streamable HTTP;
//! `sse` is the legacy Server-Sent-Events transport (some servers still only
//! speak it). Both remote transports accept custom `headers` for a static
//! bearer/api-key.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::runtime_process::RuntimeEnv;
use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObject, RuntimeObjectEncoder, RuntimeString,
};

fn decode_provider_unit_enum<T: Copy>(
    value: &RuntimeJson,
    variants: &[RuntimeEnumVariant<'_>],
    values: &[T],
) -> Result<T, RuntimeCodecError> {
    runtime_codec::decode_external(value, variants, |variant| match variant {
        RuntimeExternalVariant::Unit { variant } => values
            .get(variant)
            .copied()
            .ok_or_else(|| RuntimeCodecError::new(RuntimeCodecErrorKind::UnknownEnumVariant)),
        RuntimeExternalVariant::Payload { .. } => Err(RuntimeCodecError::new(
            RuntimeCodecErrorKind::InvalidEnumShape,
        )),
    })
}

/// Per-server configuration options shared across all transport variants.
///
/// Every field is optional because host-injected servers (in-process, plugin-
/// declared) may carry none of them. Absent means "use the connection manager's
/// default" or "this feature is not configured".
#[derive(Debug, Clone, Default, PartialEq)]
pub struct McpServerOptions {
    /// Native loader receipt; client declarations cannot construct this value.
    pub native_origin: Option<crate::mcp::approval_provenance::NativeMcpOrigin>,
    /// Opt this server's tools out of deferred loading (eager-load all its
    /// schemas regardless of the model's search-tool capability).
    pub always_load: Option<bool>,
    /// When `false`, the engine skips initializing this MCP server: it is never
    /// dialed and contributes no tools. Absent = true. This is the ON-DISK
    /// enable state (`"enabled": false` on a `mcpServers` entry), so a client
    /// that only writes the file still gets an honest answer out of the engine.
    pub enabled: Option<bool>,
    /// When `true`, the session fails to start if this server fails to initialize
    /// (instead of silently degrading to no tools).
    pub required: Option<bool>,
    /// When `true`, every tool from this server is advertised as safe for
    /// parallel tool calls (the model may call them concurrently).
    pub supports_parallel_tool_calls: Option<bool>,
    /// Startup timeout in milliseconds. Overrides the manager's
    /// `connect_timeout_ms` for this server.
    pub startup_timeout_ms: Option<u64>,
    /// Default per-call timeout in milliseconds. Overrides the manager's
    /// `call_timeout_ms` for this server.
    pub tool_timeout_ms: Option<u64>,
    /// Explicit allow-list of tools exposed from this server. When set, only
    /// these tools will be registered after `tools/list`.
    pub enabled_tools: Option<Vec<String>>,
    /// Explicit deny-list of tools. These tools will be removed after applying
    /// `enabled_tools`.
    pub disabled_tools: Option<Vec<String>>,
    /// Optional OAuth scopes to request during MCP login.
    pub scopes: Option<Vec<String>>,
    /// Optional OAuth resource parameter (RFC 8707).
    pub oauth_resource: Option<String>,
}

impl McpServerOptions {
    /// Whether this server is effectively enabled. Absent = true.
    pub fn is_enabled(&self) -> bool {
        self.enabled != Some(false)
    }
}

/// Configuration for a single MCP server connection. A discriminated union on
/// the transport, so each variant carries only its relevant fields.
///
/// This is an in-memory value: it is constructed in code (the friendly on-disk
/// file shape is mapped into it by the config-file layer) and never serialized
/// as a whole — the `in-process` variant holds a live server object that has no
/// JSON representation.
#[derive(Clone)]
pub enum McpServerConfig {
    /// A local subprocess speaking MCP over stdio.
    Stdio {
        name: String,
        command: String,
        args: Option<Vec<String>>,
        /// Exact JavaScript environment properties, including enumeration
        /// order and UTF-16 code units.
        env: Option<RuntimeEnv>,
        options: McpServerOptions,
    },
    /// A remote server over Streamable HTTP.
    Http {
        name: String,
        url: String,
        headers: Option<HashMap<String, String>>,
        oauth: Option<McpOAuthOption>,
        options: McpServerOptions,
    },
    /// A remote server over the legacy Server-Sent-Events transport.
    Sse {
        name: String,
        url: String,
        headers: Option<HashMap<String, String>>,
        oauth: Option<McpOAuthOption>,
        options: McpServerOptions,
    },
    /// A server running in THIS process. The client links to it over an
    /// in-memory transport pair (no sockets, no subprocess), so a host can
    /// expose engine-local tools as a first-class MCP server.
    InProcess {
        name: String,
        server: Arc<dyn McpInProcessServer>,
        options: McpServerOptions,
    },
    /// A server owned and run by the HOST application (e.g. the desktop
    /// shell's in-process browser MCP). The engine never spawns or connects
    /// it from this declaration: the host binds the live endpoint at session
    /// init, and this config exists so plugin manifests can declare the
    /// server for attribution, enablement and contribution listing like any
    /// other first-class MCP source.
    HostManaged {
        name: String,
        options: McpServerOptions,
    },
}

impl McpServerConfig {
    /// The server's declared name (present on every variant).
    pub fn name(&self) -> &str {
        match self {
            McpServerConfig::Stdio { name, .. }
            | McpServerConfig::Http { name, .. }
            | McpServerConfig::Sse { name, .. }
            | McpServerConfig::InProcess { name, .. }
            | McpServerConfig::HostManaged { name, .. } => name,
        }
    }

    /// The wire discriminant of this variant's transport
    /// (`"stdio"` / `"http"` / `"sse"` / `"in-process"` / `"host-managed"`).
    pub fn transport(&self) -> &'static str {
        match self {
            McpServerConfig::Stdio { .. } => "stdio",
            McpServerConfig::Http { .. } => "http",
            McpServerConfig::Sse { .. } => "sse",
            McpServerConfig::InProcess { .. } => "in-process",
            McpServerConfig::HostManaged { .. } => "host-managed",
        }
    }

    /// Whether this server opts its tools out of deferred loading. A missing or
    /// `false` flag is not always-load.
    pub fn always_load(&self) -> bool {
        self.options().always_load == Some(true)
    }

    /// Shared options for this server.
    pub fn options(&self) -> &McpServerOptions {
        match self {
            McpServerConfig::Stdio { options, .. }
            | McpServerConfig::Http { options, .. }
            | McpServerConfig::Sse { options, .. }
            | McpServerConfig::InProcess { options, .. }
            | McpServerConfig::HostManaged { options, .. } => options,
        }
    }

    pub(crate) fn options_mut(&mut self) -> &mut McpServerOptions {
        match self {
            Self::Stdio { options, .. }
            | Self::Http { options, .. }
            | Self::Sse { options, .. }
            | Self::InProcess { options, .. }
            | Self::HostManaged { options, .. } => options,
        }
    }

    /// Whether this server is effectively enabled.
    pub fn is_enabled(&self) -> bool {
        self.options().is_enabled()
    }

    /// Content signature used to suppress duplicate plugin-declared MCP
    /// servers: stdio is identified by
    /// command+args, remote transports by URL, and auth/env/name/alwaysLoad do
    /// not affect whether two declarations would launch the same server.
    pub fn dedup_signature(&self) -> Option<String> {
        match self {
            McpServerConfig::Stdio { command, args, .. } => {
                let argv = stdio_signature_argv(command, args.as_deref());
                let encoded = runtime_codec::encode_value(&argv).ok()?;
                Some(format!("stdio:{encoded}"))
            }
            McpServerConfig::Http { url, .. } | McpServerConfig::Sse { url, .. } => {
                Some(format!("url:{}", unwrap_ccr_proxy_url(url)))
            }
            McpServerConfig::InProcess { .. } => None,
            // Host-managed servers have no engine-side address: the endpoint
            // arrives from the host at runtime, so two declarations of the
            // same name are the same service by definition.
            McpServerConfig::HostManaged { name, .. } => Some(format!("host-managed:{name}")),
        }
    }
}

fn stdio_signature_argv(command: &str, args: Option<&[String]>) -> Vec<String> {
    let mut argv = vec![command.to_string()];
    if let Some(args) = args {
        argv.extend(args.iter().cloned());
    }
    argv
}

fn unwrap_ccr_proxy_url(url: &str) -> String {
    const CCR_PROXY_PATH_MARKERS: [&str; 2] =
        ["/v2/session_ingress/shttp/mcp/", "/v2/ccr-sessions/"];
    if !CCR_PROXY_PATH_MARKERS
        .iter()
        .any(|marker| url.contains(marker))
    {
        return url.to_string();
    }
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return url.to_string();
    };
    parsed
        .query_pairs()
        .find(|(key, _)| key == "mcp_url")
        .map(|(_, value)| value.into_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| url.to_string())
}

impl PartialEq for McpServerConfig {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                McpServerConfig::Stdio {
                    name,
                    command,
                    args,
                    env,
                    options,
                },
                McpServerConfig::Stdio {
                    name: name2,
                    command: command2,
                    args: args2,
                    env: env2,
                    options: options2,
                },
            ) => {
                name == name2
                    && command == command2
                    && args == args2
                    && env == env2
                    && options == options2
            }
            (
                McpServerConfig::Http {
                    name,
                    url,
                    headers,
                    oauth,
                    options,
                },
                McpServerConfig::Http {
                    name: name2,
                    url: url2,
                    headers: headers2,
                    oauth: oauth2,
                    options: options2,
                },
            ) => {
                name == name2
                    && url == url2
                    && headers == headers2
                    && oauth == oauth2
                    && options == options2
            }
            (
                McpServerConfig::Sse {
                    name,
                    url,
                    headers,
                    oauth,
                    options,
                },
                McpServerConfig::Sse {
                    name: name2,
                    url: url2,
                    headers: headers2,
                    oauth: oauth2,
                    options: options2,
                },
            ) => {
                name == name2
                    && url == url2
                    && headers == headers2
                    && oauth == oauth2
                    && options == options2
            }
            (
                McpServerConfig::InProcess {
                    name,
                    server,
                    options,
                },
                McpServerConfig::InProcess {
                    name: name2,
                    server: server2,
                    options: options2,
                },
            ) => name == name2 && options == options2 && Arc::ptr_eq(server, server2),
            (
                McpServerConfig::HostManaged { name, options },
                McpServerConfig::HostManaged {
                    name: name2,
                    options: options2,
                },
            ) => name == name2 && options == options2,
            _ => false,
        }
    }
}

impl std::fmt::Debug for McpServerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            McpServerConfig::Stdio {
                name,
                command,
                args,
                env,
                options,
            } => f
                .debug_struct("Stdio")
                .field("name", name)
                .field("command", command)
                .field("args", args)
                .field("env", env)
                .field("options", options)
                .finish(),
            McpServerConfig::Http {
                name,
                url,
                headers,
                oauth,
                options,
            } => f
                .debug_struct("Http")
                .field("name", name)
                .field("url", url)
                .field("headers", headers)
                .field("oauth", oauth)
                .field("options", options)
                .finish(),
            McpServerConfig::Sse {
                name,
                url,
                headers,
                oauth,
                options,
            } => f
                .debug_struct("Sse")
                .field("name", name)
                .field("url", url)
                .field("headers", headers)
                .field("oauth", oauth)
                .field("options", options)
                .finish(),
            McpServerConfig::InProcess { name, options, .. } => f
                .debug_struct("InProcess")
                .field("name", name)
                .field("server", &"<dyn McpInProcessServer>")
                .field("options", options)
                .finish(),
            McpServerConfig::HostManaged { name, options } => f
                .debug_struct("HostManaged")
                .field("name", name)
                .field("options", options)
                .finish(),
        }
    }
}

/// Enable OAuth 2.0 for a remote (http/sse) MCP server. When set, the connection
/// authenticates via the full OAuth flow (PKCE + dynamic client registration +
/// token refresh) instead of a static bearer.
///
/// Wire shape: `true | { scopes?: string[] }`. It is parsed from the friendly
/// config by the config-file layer (there is no automatic serde round-trip), so
/// this stays a plain enum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpOAuthOption {
    /// `true`: authenticate using the server's advertised scopes.
    Advertised,
    /// `{ scopes?: string[] }`: an object form. `scopes = Some(..)` requests
    /// those specific scopes; `scopes = None` is the bare `{}` (object form with
    /// no explicit scopes).
    Object { scopes: Option<Vec<String>> },
}

/// An in-process server we can attach to one end of an in-memory transport pair.
///
/// The host implements this; `connect` is handed the SERVER end of a linked
/// in-memory pair (the client links to the other end). It takes ownership of the
/// transport and drives it for the life of the connection.
#[async_trait]
pub trait McpInProcessServer: Send + Sync {
    /// Start this server on `transport` (the server end of an in-memory pair).
    async fn connect(&self, transport: Box<dyn crate::mcp::transports::Transport>);
}

/// Lifecycle state of a managed MCP connection.
///
/// - `Connecting`: handshake in progress
/// - `Ready`: connected, tools listed
/// - `Failed`: connect/list failed; degraded with no tools
/// - `NeedsAuth`: server returned 401/403; provide a token and reconnect
/// - `Cancelled`: the dial was aborted before it settled (session interrupt /
///   shutdown mid-handshake). Distinct from `Failed`: nothing is wrong with the
///   server, so a host must not paint it as broken.
/// - `Closed`: transport closed / shut down
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum McpConnectionState {
    Connecting,
    Ready,
    Failed,
    /// Serializes as `"needs-auth"`.
    NeedsAuth,
    Cancelled,
    Closed,
}

impl RuntimeEncode for McpConnectionState {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Connecting => "connecting",
            Self::Ready => "ready",
            Self::Failed => "failed",
            Self::NeedsAuth => "needs-auth",
            Self::Cancelled => "cancelled",
            Self::Closed => "closed",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for McpConnectionState {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("connecting", &[]),
                RuntimeEnumVariant::new("ready", &[]),
                RuntimeEnumVariant::new("failed", &[]),
                RuntimeEnumVariant::new("needs-auth", &[]),
                RuntimeEnumVariant::new("cancelled", &[]),
                RuntimeEnumVariant::new("closed", &[]),
            ],
            &[
                Self::Connecting,
                Self::Ready,
                Self::Failed,
                Self::NeedsAuth,
                Self::Cancelled,
                Self::Closed,
            ],
        )
    }
}

impl McpConnectionState {
    /// The wire string form (`"connecting" | "ready" | "failed" | "needs-auth" |
    /// "cancelled" | "closed"`).
    pub fn as_str(&self) -> &'static str {
        match self {
            McpConnectionState::Connecting => "connecting",
            McpConnectionState::Ready => "ready",
            McpConnectionState::Failed => "failed",
            McpConnectionState::NeedsAuth => "needs-auth",
            McpConnectionState::Cancelled => "cancelled",
            McpConnectionState::Closed => "closed",
        }
    }
}

/// Why a connection settled in `Failed`, when the cause is one a host can act
/// on. A bare error string cannot be branched on: the MCP page needs to tell
/// "this server wants you to log in again" apart from "this server is broken",
/// because only the first one has a button.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum McpStartupFailureReason {
    /// Stored credentials were rejected; the user must re-authorize.
    /// Serializes as `"reauthentication-required"`.
    ReauthenticationRequired,
}

impl RuntimeEncode for McpStartupFailureReason {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::ReauthenticationRequired => "reauthentication-required",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for McpStartupFailureReason {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[RuntimeEnumVariant::new("reauthentication-required", &[])],
            &[Self::ReauthenticationRequired],
        )
    }
}

impl McpStartupFailureReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ReauthenticationRequired => "reauthentication-required",
        }
    }
}

/// The `serverInfo` a server reports from `initialize`. Kept because it is the
/// only place a host learns WHICH build it is talking to; a version mismatch is
/// otherwise invisible when a server misbehaves.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct McpServerInfo {
    pub name: Option<RuntimeString>,
    pub version: Option<RuntimeString>,
    pub title: Option<RuntimeString>,
}

impl RuntimeEncode for McpServerInfo {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        if let Some(value) = &self.name {
            object.field("name", value)?;
        }
        if let Some(value) = &self.version {
            object.field("version", value)?;
        }
        if let Some(value) = &self.title {
            object.field("title", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for McpServerInfo {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                name: object.optional("name", &[])?,
                version: object.optional("version", &[])?,
                title: object.optional("title", &[])?,
            })
        })
    }
}

/// One `resources/templates/list` entry. A template is NOT a resource: it is a
/// URI pattern the caller has to fill in, so a host that renders it in the same
/// list as concrete resources produces rows that cannot be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpResourceTemplateDescriptor {
    pub uri_template: RuntimeString,
    pub name: Option<RuntimeString>,
    pub mime_type: Option<RuntimeString>,
    pub description: Option<RuntimeString>,
}

impl RuntimeEncode for McpResourceTemplateDescriptor {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("uriTemplate", &self.uri_template)?;
        if let Some(value) = &self.name {
            object.field("name", value)?;
        }
        if let Some(value) = &self.mime_type {
            object.field("mimeType", value)?;
        }
        if let Some(value) = &self.description {
            object.field("description", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for McpResourceTemplateDescriptor {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                uri_template: object.required("uriTemplate", &[])?,
                name: object.optional("name", &[])?,
                mime_type: object.optional("mimeType", &[])?,
                description: object.optional("description", &[])?,
            })
        })
    }
}

/// One MCP server's lifecycle transition, reported as it happens.
///
/// This is the PUSH half of the status surface: the runtime snapshot answers
/// "what is exposed right now", this one reports "that just changed". Without it
/// a host has to poll the whole connection set, and polling is what forces the
/// choice between a stale page and a page that re-dials every server to look at
/// it.
///
/// `auth_status` is the SAME type the authorization-status reply already uses
/// (`oauth_provider::McpAuthStatus`), not a parallel enum: two enums with one
/// name would put a different wire string on the same fact depending on which
/// reply produced it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpServerStatusUpdate {
    pub server_name: RuntimeString,
    pub state: McpConnectionState,
    pub auth_status: crate::mcp::oauth_provider::McpAuthStatus,
    pub failure_reason: Option<McpStartupFailureReason>,
    pub error: Option<RuntimeString>,
}

impl RuntimeEncode for McpServerStatusUpdate {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("serverName", &self.server_name)?;
        object.field("state", &self.state)?;
        object.field("authStatus", &self.auth_status)?;
        if let Some(value) = &self.failure_reason {
            object.field("failureReason", value)?;
        }
        if let Some(value) = &self.error {
            object.field("error", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for McpServerStatusUpdate {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                server_name: object.required("serverName", &[])?,
                state: object.required("state", &[])?,
                auth_status: object.required("authStatus", &[])?,
                failure_reason: object.optional("failureReason", &[])?,
                error: object.optional("error", &[])?,
            })
        })
    }
}

/// Sink for MCP lifecycle transitions.
///
/// Synchronous on purpose: a dial must never wait on the host to accept a
/// notification. The host side enqueues onto its own ordered write path, which
/// is where back-pressure and shutdown admission belong.
pub type McpStatusSink = Arc<dyn Fn(McpServerStatusUpdate) + Send + Sync>;

/// A swappable sink slot.
///
/// The connection manager is built during session construction, but the host's
/// sink is installed afterwards (the same shape as the extension-failure sink).
/// Handing the manager this slot at construction and filling it later keeps the
/// sink out of `SessionInit`, where a new field would have to be spelled out at
/// every construction site including the tests.
#[derive(Default)]
pub struct McpStatusObserverSlot {
    inner: std::sync::Mutex<Option<McpStatusSink>>,
}

impl McpStatusObserverSlot {
    pub fn new() -> Self {
        Self::default()
    }

    /// Install or clear the sink. Replacing is allowed: a reconnected host
    /// installs a fresh sink and the stale one must stop receiving.
    pub fn set(&self, sink: Option<McpStatusSink>) {
        *self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = sink;
    }

    /// Deliver one transition. The sink is cloned out before the call so the
    /// lock is never held while host code runs.
    pub fn notify(&self, update: McpServerStatusUpdate) {
        let sink = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(sink) = sink {
            sink(update);
        }
    }
}

/// The normalized result shape a server's `callTool` response is mapped into
/// before it is handed to the tool wrapper. Fields are defensive: an MCP server
/// can return any of these, and any may be missing.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct McpCallResult {
    pub content: Vec<McpContentBlock>,
    pub is_error: Option<bool>,
    pub meta: Option<RuntimeJson>,
    pub structured_content: Option<RuntimeJson>,
}

/// One normalized MCP content block. Covers the MCP content union: `text`,
/// `image`/`audio` (base64 `data` + `mimeType`), `resource` (an embedded
/// resource object with text or a blob), and `resource_link` (`uri`/`name`/
/// `description`). Fields are all optional — a server can return any variant and
/// any field may be missing.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct McpContentBlock {
    /// The block's `type` discriminator (kept as a string — untrusted).
    pub block_type: RuntimeString,
    pub text: Option<RuntimeString>,
    pub data: Option<RuntimeString>,
    pub mime_type: Option<RuntimeString>,
    /// `resource` block: the embedded resource's contents.
    pub resource: Option<McpEmbeddedResource>,
    /// `resource_link` block fields.
    pub uri: Option<RuntimeString>,
    pub name: Option<RuntimeString>,
    pub description: Option<RuntimeString>,
}

/// The embedded resource carried by a `resource` content block.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct McpEmbeddedResource {
    pub uri: Option<RuntimeString>,
    pub text: Option<RuntimeString>,
    pub blob: Option<RuntimeString>,
    pub mime_type: Option<RuntimeString>,
}

/// The raw MCP tool descriptor (as returned by `tools/list`), narrowed to what
/// the engine uses.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct McpToolDescriptor {
    pub name: RuntimeString,
    pub title: Option<RuntimeString>,
    pub description: Option<RuntimeString>,
    pub input_schema: Option<RuntimeJson>,
    /// The MCP `annotations` behavior hints. These are SERVER-ASSERTED and
    /// untrusted; the engine consumes them faithfully in the tool wrapper.
    pub annotations: Option<McpToolAnnotations>,
    /// Server-provided `_meta` from `tools/list`. The engine reads two keys for
    /// deferred loading: `anthropic/searchHint` (string) and
    /// `anthropic/alwaysLoad` (boolean).
    pub meta: Option<RuntimeObject>,
}

impl RuntimeEncode for McpCallResult {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        if !self.content.is_empty() {
            object.field("content", &self.content)?;
        }
        if let Some(value) = self.is_error {
            object.field("isError", &value)?;
        }
        if let Some(value) = &self.meta {
            object.field("_meta", value)?;
        }
        if let Some(value) = &self.structured_content {
            object.field("structuredContent", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for McpCallResult {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                content: object.defaulted("content", &[], Vec::new)?,
                is_error: object.optional("isError", &[])?,
                meta: object.optional("_meta", &[])?,
                structured_content: object.optional("structuredContent", &[])?,
            })
        })
    }
}

impl RuntimeEncode for McpContentBlock {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("type", &self.block_type)?;
        if let Some(value) = &self.text {
            object.field("text", value)?;
        }
        if let Some(value) = &self.data {
            object.field("data", value)?;
        }
        if let Some(value) = &self.mime_type {
            object.field("mimeType", value)?;
        }
        if let Some(value) = &self.resource {
            object.field("resource", value)?;
        }
        if let Some(value) = &self.uri {
            object.field("uri", value)?;
        }
        if let Some(value) = &self.name {
            object.field("name", value)?;
        }
        if let Some(value) = &self.description {
            object.field("description", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for McpContentBlock {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                block_type: object.required("type", &[])?,
                text: object.optional("text", &[])?,
                data: object.optional("data", &[])?,
                mime_type: object.optional("mimeType", &[])?,
                resource: object.optional("resource", &[])?,
                uri: object.optional("uri", &[])?,
                name: object.optional("name", &[])?,
                description: object.optional("description", &[])?,
            })
        })
    }
}

impl RuntimeEncode for McpEmbeddedResource {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        if let Some(value) = &self.uri {
            object.field("uri", value)?;
        }
        if let Some(value) = &self.text {
            object.field("text", value)?;
        }
        if let Some(value) = &self.blob {
            object.field("blob", value)?;
        }
        if let Some(value) = &self.mime_type {
            object.field("mimeType", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for McpEmbeddedResource {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                uri: object.optional("uri", &[])?,
                text: object.optional("text", &[])?,
                blob: object.optional("blob", &[])?,
                mime_type: object.optional("mimeType", &[])?,
            })
        })
    }
}

impl RuntimeEncode for McpToolDescriptor {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("name", &self.name)?;
        if let Some(value) = &self.title {
            object.field("title", value)?;
        }
        if let Some(value) = &self.description {
            object.field("description", value)?;
        }
        if let Some(value) = &self.input_schema {
            object.field("inputSchema", value)?;
        }
        if let Some(value) = &self.annotations {
            object.field("annotations", value)?;
        }
        if let Some(value) = &self.meta {
            object.field("_meta", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for McpToolDescriptor {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                name: object.required("name", &[])?,
                title: object.optional("title", &[])?,
                description: object.optional("description", &[])?,
                input_schema: object.optional("inputSchema", &[])?,
                annotations: object.optional("annotations", &[])?,
                meta: object.optional("_meta", &[])?,
            })
        })
    }
}

/// The MCP `annotations` behavior hints (spec: readOnlyHint / idempotentHint /
/// destructiveHint / openWorldHint). Server-asserted and untrusted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct McpToolAnnotations {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub read_only_hint: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub idempotent_hint: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub destructive_hint: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub open_world_hint: Option<bool>,
}

impl RuntimeEncode for McpToolAnnotations {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self {
            read_only_hint,
            idempotent_hint,
            destructive_hint,
            open_world_hint,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        if let Some(value) = read_only_hint {
            object.field("readOnlyHint", value)?;
        }
        if let Some(value) = idempotent_hint {
            object.field("idempotentHint", value)?;
        }
        if let Some(value) = destructive_hint {
            object.field("destructiveHint", value)?;
        }
        if let Some(value) = open_world_hint {
            object.field("openWorldHint", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for McpToolAnnotations {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                read_only_hint: object.optional("readOnlyHint", &[])?,
                idempotent_hint: object.optional("idempotentHint", &[])?,
                destructive_hint: object.optional("destructiveHint", &[])?,
                open_world_hint: object.optional("openWorldHint", &[])?,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::runtime_json;

    fn runtime(text: &str) -> RuntimeJson {
        runtime_json::parse(&RuntimeString::from(text)).unwrap()
    }

    #[test]
    fn connection_state_serializes_to_wire_strings() {
        assert_eq!(
            runtime_codec::encode_value(&McpConnectionState::Connecting).unwrap(),
            RuntimeJson::from("connecting")
        );
        assert_eq!(
            runtime_codec::encode_value(&McpConnectionState::Ready).unwrap(),
            RuntimeJson::from("ready")
        );
        assert_eq!(
            runtime_codec::encode_value(&McpConnectionState::Failed).unwrap(),
            RuntimeJson::from("failed")
        );
        assert_eq!(
            runtime_codec::encode_value(&McpConnectionState::NeedsAuth).unwrap(),
            RuntimeJson::from("needs-auth")
        );
        assert_eq!(
            runtime_codec::encode_value(&McpConnectionState::Closed).unwrap(),
            RuntimeJson::from("closed")
        );
    }

    #[test]
    fn connection_state_round_trips_and_as_str_matches() {
        let s = runtime_codec::decode_value::<McpConnectionState>(&RuntimeJson::from("needs-auth"))
            .unwrap();
        assert_eq!(s, McpConnectionState::NeedsAuth);
        assert_eq!(McpConnectionState::NeedsAuth.as_str(), "needs-auth");
        assert_eq!(McpConnectionState::Connecting.as_str(), "connecting");
        assert_eq!(McpConnectionState::Closed.as_str(), "closed");
    }

    #[test]
    fn server_config_discriminant_and_accessors() {
        let stdio = McpServerConfig::Stdio {
            name: "s".into(),
            command: "cmd".into(),
            args: Some(vec!["-y".into()]),
            env: None,
            options: McpServerOptions::default(),
        };
        assert_eq!(stdio.transport(), "stdio");
        assert_eq!(stdio.name(), "s");
        assert!(!stdio.always_load());

        let http = McpServerConfig::Http {
            name: "h".into(),
            url: "https://acme.example/mcp".into(),
            headers: None,
            oauth: Some(McpOAuthOption::Advertised),
            options: McpServerOptions {
                always_load: Some(true),
                ..Default::default()
            },
        };
        assert_eq!(http.transport(), "http");
        assert_eq!(http.name(), "h");
        assert!(http.always_load());

        let sse = McpServerConfig::Sse {
            name: "e".into(),
            url: "https://acme.example/sse".into(),
            headers: None,
            oauth: None,
            // an explicit `false` is NOT always-load
            options: McpServerOptions {
                always_load: Some(false),
                ..Default::default()
            },
        };
        assert_eq!(sse.transport(), "sse");
        assert!(!sse.always_load());
    }

    #[test]
    fn oauth_option_shapes() {
        assert_eq!(McpOAuthOption::Advertised, McpOAuthOption::Advertised);
        assert_ne!(
            McpOAuthOption::Advertised,
            McpOAuthOption::Object { scopes: None }
        );
        match (McpOAuthOption::Object {
            scopes: Some(vec!["read".into(), "write".into()]),
        }) {
            McpOAuthOption::Object { scopes } => {
                assert_eq!(scopes, Some(vec!["read".to_string(), "write".to_string()]));
            }
            McpOAuthOption::Advertised => panic!("expected object form"),
        }
        // bare `{}` object form: object present, no explicit scopes.
        assert!(matches!(
            McpOAuthOption::Object { scopes: None },
            McpOAuthOption::Object { scopes: None }
        ));
    }

    #[test]
    fn call_result_deserializes_wire_shape() {
        let v = runtime(
            r#"{
                "content": [
                    { "type": "text", "text": "hello" },
                    { "type": "image", "data": "AAAA", "mimeType": "image/png" },
                    { "type": "resource", "resource": { "uri": "file://x", "text": "body", "mimeType": "text/plain" } },
                    { "type": "resource_link", "uri": "file://y", "name": "n", "description": "d" }
                ],
                "isError": true,
                "_meta": { "k": 1 },
                "structuredContent": { "a": [1, 2] }
            }"#,
        );
        let r = runtime_codec::decode_value::<McpCallResult>(&v).unwrap();
        assert_eq!(r.content.len(), 4);
        assert_eq!(r.content[0].block_type, "text");
        assert_eq!(r.content[0].text, Some(RuntimeString::from("hello")));
        assert_eq!(r.content[1].block_type, "image");
        assert_eq!(r.content[1].data, Some(RuntimeString::from("AAAA")));
        assert_eq!(
            r.content[1].mime_type,
            Some(RuntimeString::from("image/png"))
        );
        let res = r.content[2].resource.as_ref().unwrap();
        assert_eq!(res.uri, Some(RuntimeString::from("file://x")));
        assert_eq!(res.text, Some(RuntimeString::from("body")));
        assert_eq!(res.mime_type, Some(RuntimeString::from("text/plain")));
        assert_eq!(r.content[3].block_type, "resource_link");
        assert_eq!(r.content[3].uri, Some(RuntimeString::from("file://y")));
        assert_eq!(r.content[3].name, Some(RuntimeString::from("n")));
        assert_eq!(r.content[3].description, Some(RuntimeString::from("d")));
        assert_eq!(r.is_error, Some(true));
        assert_eq!(r.meta, Some(runtime(r#"{"k":1}"#)));
        assert_eq!(r.structured_content, Some(runtime(r#"{"a":[1,2]}"#)));
    }

    #[test]
    fn call_result_defaults_when_fields_missing() {
        let r = runtime_codec::decode_value::<McpCallResult>(&RuntimeJson::Object(
            RuntimeObject::new(),
        ))
        .unwrap();
        assert!(r.content.is_empty());
        assert_eq!(r.is_error, None);
        assert_eq!(r.meta, None);
        assert_eq!(r.structured_content, None);
    }

    #[test]
    fn content_block_type_field_is_renamed_and_options_omitted() {
        let b = McpContentBlock {
            block_type: "text".into(),
            text: Some("hi".into()),
            ..Default::default()
        };
        let v = runtime_codec::encode_value(&b).unwrap();
        assert_eq!(
            v.get("type").and_then(|x| x.as_str()).cloned(),
            Some(RuntimeString::from("text"))
        );
        assert_eq!(
            v.get("text").and_then(|x| x.as_str()).cloned(),
            Some(RuntimeString::from("hi"))
        );
        // absent optionals are omitted, not serialized as null
        assert!(v.get("data").is_none());
        assert!(v.get("mimeType").is_none());
        assert!(v.get("blockType").is_none());
    }

    #[test]
    fn tool_descriptor_deserializes_hints_and_meta() {
        let v = runtime(
            r#"{
                "name": "do_thing",
                "description": "does a thing",
                "inputSchema": { "type": "object" },
                "annotations": { "readOnlyHint": true, "destructiveHint": false },
                "_meta": { "anthropic/searchHint": "search things", "anthropic/alwaysLoad": true }
            }"#,
        );
        let d = runtime_codec::decode_value::<McpToolDescriptor>(&v).unwrap();
        assert_eq!(d.name, "do_thing");
        assert_eq!(d.description, Some(RuntimeString::from("does a thing")));
        assert_eq!(d.input_schema, Some(runtime(r#"{"type":"object"}"#)));
        let ann = d.annotations.unwrap();
        assert_eq!(ann.read_only_hint, Some(true));
        assert_eq!(ann.destructive_hint, Some(false));
        assert_eq!(ann.idempotent_hint, None);
        assert_eq!(ann.open_world_hint, None);
        let meta = d.meta.unwrap();
        assert_eq!(
            meta.get("anthropic/searchHint")
                .and_then(|x| x.as_str())
                .cloned(),
            Some(RuntimeString::from("search things"))
        );
        assert_eq!(
            meta.get("anthropic/alwaysLoad").and_then(|x| x.as_bool()),
            Some(true)
        );
    }

    #[test]
    fn mcp_wire_dtos_preserve_lone_surrogates_without_a_serde_round_trip() {
        let value = runtime(
            r#"{
                "content":[{"type":"text","text":"A\ud800B"}],
                "_meta":{"key\udc00":"value\ud800"},
                "structuredContent":{"literal":"\uFDD0ud800"}
            }"#,
        );
        let decoded = runtime_codec::decode_value::<McpCallResult>(&value).unwrap();
        assert_eq!(
            decoded.content[0].text,
            Some(RuntimeString::from_utf16([
                b'A' as u16,
                0xd800,
                b'B' as u16,
            ]))
        );
        assert_eq!(runtime_codec::encode_value(&decoded).unwrap(), value);
    }

    #[test]
    fn server_config_is_cloneable_and_debug() {
        let http = McpServerConfig::Http {
            name: "h".into(),
            url: "https://acme.example/mcp".into(),
            headers: Some(HashMap::from([("X-Api-Key".to_string(), "k".to_string())])),
            oauth: Some(McpOAuthOption::Object {
                scopes: Some(vec!["read".into()]),
            }),
            options: McpServerOptions::default(),
        };
        let cloned = http.clone();
        assert_eq!(cloned.name(), "h");
        assert_eq!(cloned.transport(), "http");
        // manual Debug renders the variant name
        assert!(format!("{http:?}").starts_with("Http"));
    }
}
