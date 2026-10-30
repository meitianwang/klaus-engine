//! Network transports for the engine's Op/Event protocol.
//!
//! The core embeds the engine in-process and the protocol drives it over local
//! stdio; this crate adds the third surface: drive the engine OVER THE NETWORK.
//! A single [`serve`] exposes TWO transports on one port, both reusing the exact
//! same newline-delimited Op/Event `ProtocolServer`:
//!
//!   * WebSocket — bidirectional, great for interactive clients. Use [`connect`].
//!   * HTTP + SSE — `POST /sessions/{id}/ops` in, `GET /sessions/{id}/events`
//!     (Server-Sent Events) out; cloud-friendly (curl / CDN / proxy). Use
//!     [`connect_http`].
//!
//! Either way one connection/stream = one session.
//!
//! Security (both transports): a loopback bind needs no token; a non-loopback
//! bind REQUIRES one (fail-closed), checked as `Authorization: Bearer <token>`;
//! browser-style cross-origin requests are rejected unless allow-listed. Health
//! probes: `GET /healthz`, `GET /readyz`.

// The app-server schema bundle is one large nested `json!` literal; the
// default macro recursion limit is too small for it.
#![recursion_limit = "512"]

pub mod app_server;
pub mod auth;
pub mod connect;
pub mod connect_http;
pub mod endpoint;
mod endpoint_activity;
pub mod exec_server;
pub mod fd_limit;
mod http_util;
#[cfg(test)]
mod integration_tests;
pub mod lifecycle;
mod line_stream;
mod plugin_admin;
pub mod serve;
pub mod sse;
#[cfg(windows)]
pub mod windows_process_mode;
pub mod ws_stream;

pub use app_server::{
    run_app_server, schema_bundle, AppServerClient, AppServerOptions, ClientError, Method,
    Notification, Notifications, RawJson, ReverseRequest, RuntimeValue, ALL_METHODS,
};
pub use connect::{
    connect, connect_facade, ClientConnection, ConnectError, ConnectOptions, FacadeConnection,
};
pub use connect_http::{
    connect_http, list_threads, ConnectHttpOptions, HttpClientConnection, HttpError,
};
pub use endpoint::{
    config_from_env, env_nonempty, env_truthy, provider_from_env, provider_named, Endpoint,
    EndpointOptions,
};
pub use exec_server::{serve_exec_server, ExecServeError, ExecServeOptions, RunningExecServer};
pub use fd_limit::raise_open_file_limit;
pub use lifecycle::{
    engine_sessions, protocol_create_session, protocol_lifecycle, CreateSession, EngineSessions,
    ListThreads, SessionContext,
};
pub use plugin_admin::{load_endpoint_installed_plugins, protocol_plugin_admin};
pub use serve::{serve, RunningServer, ServeError, ServeOptions};
pub use ws_stream::{ws_to_streams, WsStreams};

#[cfg(test)]
pub(crate) fn test_process_context() -> klaus_agent_core::runtime_process::RuntimeProcessContext {
    use klaus_agent_core::runtime_process::{RuntimeProcessContext, RuntimeProcessSupervisor};

    struct TestProcessOwner {
        _runtime: tokio::runtime::Runtime,
        context: RuntimeProcessContext,
    }

    static OWNER: std::sync::OnceLock<TestProcessOwner> = std::sync::OnceLock::new();
    OWNER
        .get_or_init(|| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("server test process runtime must initialize");
            let supervisor = RuntimeProcessSupervisor::initialize(runtime.handle().clone())
                .expect("server test process supervisor must initialize");
            let context = RuntimeProcessContext::initialize(supervisor)
                .expect("server test process context must initialize");
            TestProcessOwner {
                _runtime: runtime,
                context,
            }
        })
        .context
        .clone()
}

// Re-export the protocol pieces a consumer of this crate needs, so they import
// from one place.
pub use klaus_agent_protocol::{
    AbortSignal, ArchiveSessionResult, BoundSession, CompactBoundaryMessage, CompactProgress,
    CompactProgressPhase, CompactSessionResult, CompactTrigger, CompactionMode,
    DeleteSessionResult, ExtensionRuntimeSnapshot, ExtensionSourceKind, GenerateTitleOpResult,
    HookHandlerKind, InitSessionResult, InstalledPluginMutationResult, InstalledPluginRecord,
    InstalledPlugins, LspServerConfig, Op, PluginActivationScope, PluginRuntimeSnapshot,
    ProtocolClient, ProtocolCommandHook, ProtocolDynamicToolCall,
    ProtocolDynamicToolCallOutputContentItem, ProtocolDynamicToolFunctionSpec,
    ProtocolDynamicToolNamespaceSpec, ProtocolDynamicToolNamespaceTool,
    ProtocolDynamicToolResolved, ProtocolDynamicToolResponse, ProtocolDynamicToolSpec,
    ProtocolEvent, ProtocolLifecycle, ProtocolModelPricing, ProtocolPluginAdmin, ProtocolServer,
    ProtocolSessionConfigUpdate, ProtocolSessionConfigUpdateResult, ProtocolSessionInit,
    ProviderAuthInit, ProviderInit, RewindScope, RunOpOptions, SteerPriority, UpdateTitleResult,
};
