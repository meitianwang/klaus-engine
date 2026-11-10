//! Owns a set of MCP server connections, aggregates their tools, and exposes
//! their states. Connection is parallel and fault-tolerant: one server failing
//! never blocks the others. Configs are retained so dropped/failed connections
//! can be re-dialed with backoff (supervision).

use crate::mcp::client::{
    authorization_revoked_client, connect_mcp_server, ConnectMcpOptions, ManagedClient,
};
use crate::mcp::credential_store::CredentialStore;
use crate::mcp::runtime_snapshot::{mcp_runtime_snapshot_from_clients, McpRuntimeSnapshot};
use crate::mcp::types::{
    McpConnectionState, McpServerConfig, McpServerStatusUpdate, McpStatusObserverSlot,
};
use crate::runtime_process::RuntimeProcessContext;
use crate::runtime_values::{RuntimePath, RuntimeString};
use crate::types::errors::ToolError;
use crate::types::json::Json;
use crate::types::permission::PermissionResult;
use crate::types::tool::{
    McpResourceContent, PathTargets, PermissionPathKind, Tool, ToolContext, ToolExposure,
    ToolProgress, ToolResult, ValidationResult,
};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Construction options for [`McpConnectionManager`].
#[derive(Default, Clone)]
pub struct McpConnectionManagerOptions {
    pub credential_store: Option<Arc<dyn CredentialStore>>,
    /// Forwarded to each `connect_mcp_server` call.
    pub connect_timeout_ms: Option<u64>,
    pub call_timeout_ms: Option<u64>,
    /// Complete dial options. Explicit legacy fields above override matching
    /// fields when present so existing callers remain source-compatible.
    pub connect_options: Option<ConnectMcpOptions>,
    /// Receives every lifecycle transition. Absent by default: an embedder that
    /// does not render connection status should not pay for the notifications.
    pub status_observer: Option<Arc<McpStatusObserverSlot>>,
}

/// Backoff policy for [`McpConnectionManager::reconnect_all_failed`].
#[derive(Default, Clone)]
pub struct ReconnectOptions {
    pub max_attempts: Option<u32>,
    pub base_delay_ms: Option<u64>,
    pub max_delay_ms: Option<u64>,
}

/// A resource advertised by a ready client, tagged with its server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaggedResource {
    pub server: String,
    pub uri: RuntimeString,
    pub name: Option<RuntimeString>,
    pub mime_type: Option<RuntimeString>,
}

struct Entry {
    config: McpServerConfig,
    client: ManagedClient,
}

fn same_connection_identity(left: &McpServerConfig, right: &McpServerConfig) -> bool {
    fn connection_config(mut config: McpServerConfig) -> McpServerConfig {
        let options = match &mut config {
            McpServerConfig::Stdio { options, .. }
            | McpServerConfig::Http { options, .. }
            | McpServerConfig::Sse { options, .. }
            | McpServerConfig::InProcess { options, .. }
            | McpServerConfig::HostManaged { options, .. } => options,
        };
        options.native_origin = None;
        options.enabled = None;
        options.required = None;
        options.startup_timeout_ms = None;
        options.always_load = None;
        options.supports_parallel_tool_calls = None;
        options.enabled_tools = None;
        options.disabled_tools = None;
        config
    }
    connection_config(left.clone()) == connection_config(right.clone())
}

/// Owns a set of MCP server connections, aggregates their tools, and exposes
/// their states.
pub struct McpConnectionManager {
    /// Name-keyed connections in insertion order (upsert keeps a name's slot).
    entries: Vec<(String, Entry)>,
    connect_opts: ConnectMcpOptions,
    initialized: bool,
    status_observer: Option<Arc<McpStatusObserverSlot>>,
}

impl McpConnectionManager {
    pub fn new(process_context: RuntimeProcessContext, opts: McpConnectionManagerOptions) -> Self {
        let mut connect_opts = opts
            .connect_options
            .unwrap_or_else(|| ConnectMcpOptions::new(process_context.clone()));
        connect_opts.process_context = process_context;
        if opts.credential_store.is_some() {
            connect_opts.credential_store = opts.credential_store;
        }
        if opts.connect_timeout_ms.is_some() {
            connect_opts.connect_timeout_ms = opts.connect_timeout_ms;
        }
        if opts.call_timeout_ms.is_some() {
            connect_opts.call_timeout_ms = opts.call_timeout_ms;
        }
        Self {
            entries: Vec::new(),
            connect_opts,
            initialized: false,
            status_observer: opts.status_observer,
        }
    }

    /// Report one transition to the host, if it installed a sink.
    ///
    /// Emitted from the transitions THEMSELVES rather than derived by diffing
    /// snapshots: a dial that fails and is re-dialed passes through the same
    /// end state twice, and a diff cannot tell that apart from nothing having
    /// happened.
    fn announce(&self, server_name: &str, client: &ManagedClient) {
        let Some(observer) = &self.status_observer else {
            return;
        };
        observer.notify(McpServerStatusUpdate {
            server_name: RuntimeString::from(server_name),
            state: client.state,
            auth_status: client.auth_status,
            failure_reason: client.failure_reason,
            error: client.error.clone(),
        });
    }

    /// Report a transition that has no client behind it (the pre-dial
    /// `connecting`, and the post-teardown `closed`).
    fn announce_state(&self, server_name: &str, state: McpConnectionState) {
        let Some(observer) = &self.status_observer else {
            return;
        };
        observer.notify(McpServerStatusUpdate {
            server_name: RuntimeString::from(server_name),
            state,
            auth_status: crate::mcp::oauth_provider::McpAuthStatus::Unsupported,
            failure_reason: None,
            error: None,
        });
    }

    /// Refresh per-attempt context before a lazy connect or reconnect. Stable
    /// credential/handler options remain owned by the manager.
    pub fn set_connection_context(
        &mut self,
        cwd: Option<RuntimePath>,
        signal: Option<crate::types::abort::AbortSignal>,
    ) {
        self.connect_opts.cwd = cwd;
        self.connect_opts.signal = signal;
    }

    /// Connect to all configured servers in parallel. Tolerant: rejected
    /// connections are recorded as failed clients rather than propagated. May be
    /// called more than once to add further servers; existing clients are kept.
    /// A config the user turned OFF (`enabled: false`) is dropped here rather
    /// than dialed: the enable state travels with the config, so a host that
    /// only writes the file cannot end up with a running server it disabled.
    pub async fn connect_all(&mut self, configs: Vec<McpServerConfig>) {
        let configs = configs
            .into_iter()
            .filter(McpServerConfig::is_enabled)
            .filter(|config| self.get_entry(config.name()).is_none())
            .collect::<Vec<_>>();
        // `connecting` goes out for every server BEFORE the parallel dial: it is
        // the only window in which a host can honestly show "starting", and it is
        // long (a cold stdio server is seconds). Without it the page has to pick
        // between showing nothing and showing "failed" for a server that is fine.
        for config in &configs {
            self.announce_state(config.name(), McpConnectionState::Connecting);
        }
        let opts = self.connect_opts.clone();
        let results = futures::future::join_all(configs.into_iter().map(|config| {
            let opts = opts.clone();
            async move {
                let client = connect_mcp_server(&config, &opts).await;
                (config, client)
            }
        }))
        .await;
        for (config, client) in results {
            let name = config.name().to_string();
            self.announce(&name, &client);
            self.set_entry(name, Entry { config, client });
        }
        self.initialized = true;
    }

    async fn dial(&mut self, config: McpServerConfig, bypass_needs_auth_cache: bool) {
        let mut opts = self.connect_opts.clone();
        if bypass_needs_auth_cache {
            opts.skip_needs_auth_cache = Some(true);
        }
        self.announce_state(config.name(), McpConnectionState::Connecting);
        let client = connect_mcp_server(&config, &opts).await;
        let name = config.name().to_string();
        self.announce(&name, &client);
        self.set_entry(name, Entry { config, client });
    }

    /// All resources advertised by ready clients, tagged with their server.
    pub fn resources(&self) -> Vec<TaggedResource> {
        let mut out: Vec<TaggedResource> = Vec::new();
        for (name, entry) in &self.entries {
            if entry.client.state != McpConnectionState::Ready {
                continue;
            }
            for r in &entry.client.resources {
                out.push(TaggedResource {
                    server: name.clone(),
                    uri: r.uri.clone(),
                    name: r.name.clone(),
                    mime_type: r.mime_type.clone(),
                });
            }
        }
        out
    }

    /// Read a resource from a specific server.
    pub async fn read_resource(
        &self,
        server: &str,
        uri: &RuntimeString,
    ) -> Vec<McpResourceContent> {
        match self.get_entry(server) {
            Some(entry) if entry.client.state == McpConnectionState::Ready => {
                entry.client.read_resource(uri).await
            }
            _ => Vec::new(),
        }
    }

    /// Host-driven tool call on a named server. `not_found` when the server is
    /// not managed; the ready-state gate lives in the client itself.
    pub async fn call_tool(
        &self,
        server: &str,
        tool: &RuntimeString,
        args: crate::types::json::Json,
        signal: Option<crate::types::abort::AbortSignal>,
    ) -> Result<crate::mcp::types::McpCallResult, RuntimeString> {
        match self.get_entry(server) {
            Some(entry) => entry.client.call_tool(tool, args, signal).await,
            None => Err(RuntimeString::from(format!(
                "not_found: MCP server '{server}'"
            ))),
        }
    }

    /// Render a named prompt on a named server. `not_found` when the server is
    /// not managed.
    pub async fn get_prompt(
        &self,
        server: &str,
        name: &RuntimeString,
        args: Option<crate::runtime_values::RuntimeObject>,
    ) -> Result<RuntimeString, RuntimeString> {
        match self.get_entry(server) {
            Some(entry) => Ok(entry.client.get_prompt(name, args).await),
            None => Err(RuntimeString::from(format!(
                "not_found: MCP server '{server}'"
            ))),
        }
    }

    /// Re-dial one server (closing any prior connection first).
    pub async fn reconnect(&mut self, server_name: &str) -> McpConnectionState {
        let (config, client) = match self.get_entry(server_name) {
            None => return McpConnectionState::Closed,
            Some(e) => (e.config.clone(), e.client.clone()),
        };
        client.close().await;
        self.dial(config, true).await;
        self.get_entry(server_name)
            .map(|e| e.client.state)
            .unwrap_or(McpConnectionState::Failed)
    }

    /// Supervision: re-dial every server not currently `ready`, retrying with
    /// exponential backoff. Returns the names that became ready.
    pub async fn reconnect_all_failed(&mut self, opts: ReconnectOptions) -> Vec<String> {
        let max_attempts = opts.max_attempts.unwrap_or(3);
        let base_delay_ms = opts.base_delay_ms.unwrap_or(500);
        let max_delay_ms = opts.max_delay_ms.unwrap_or(8_000);
        let mut recovered: Vec<String> = Vec::new();

        let names: Vec<String> = self.entries.iter().map(|(n, _)| n.clone()).collect();
        for name in names {
            let ready =
                self.get_entry(&name).map(|e| e.client.state) == Some(McpConnectionState::Ready);
            if ready {
                continue;
            }
            for attempt in 1..=max_attempts {
                let state = self.reconnect(&name).await;
                if state == McpConnectionState::Ready {
                    recovered.push(name.clone());
                    break;
                }
                if attempt < max_attempts {
                    let delay = base_delay_ms
                        .saturating_mul(2u64.saturating_pow(attempt - 1))
                        .min(max_delay_ms);
                    sleep(delay).await;
                }
            }
        }
        recovered
    }

    /// All tools from `ready` clients, aggregated. On a tool-name collision
    /// across servers, later occurrences get a numeric suffix (`name_2`, ...).
    pub fn get_tools(&self) -> Vec<Arc<dyn Tool>> {
        let mut seen: HashMap<RuntimeString, u32> = HashMap::new();
        let mut out: Vec<Arc<dyn Tool>> = Vec::new();
        for (_, entry) in &self.entries {
            if entry.client.state != McpConnectionState::Ready {
                continue;
            }
            for tool in entry.client.tools() {
                let tname = tool.name();
                match seen.get(&tname).copied() {
                    None => {
                        seen.insert(tname, 1);
                        out.push(tool);
                    }
                    Some(prior) => {
                        let next = prior + 1;
                        let renamed = tname.concat(&RuntimeString::from(format!("_{next}")));
                        seen.insert(tname, next);
                        out.push(Arc::new(RenamedTool {
                            inner: tool,
                            name: renamed,
                        }));
                    }
                }
            }
        }
        out
    }

    /// Current state of each managed connection, keyed by server name.
    pub fn states(&self) -> HashMap<String, McpConnectionState> {
        let mut states = HashMap::new();
        for (name, entry) in &self.entries {
            states.insert(name.clone(), entry.client.state);
        }
        states
    }

    /// All managed clients (every state), so a host can expose their
    /// resources/prompts to the session. Pairs with [`Self::get_tools`].
    pub fn clients(&self) -> Vec<ManagedClient> {
        self.entries.iter().map(|(_, e)| e.client.clone()).collect()
    }

    /// Retained configuration for one managed server.
    pub fn config(&self, server_name: &str) -> Option<McpServerConfig> {
        self.get_entry(server_name)
            .map(|entry| entry.config.clone())
    }

    pub fn is_initialized(&self) -> bool {
        self.initialized
    }

    /// Add or replace a server. A matching connection identity reuses its
    /// transport and cached tool descriptors; removed connections close after
    /// their captured tools and resource clients are released.
    pub async fn upsert(&mut self, config: McpServerConfig) -> McpConnectionState {
        let name = config.name().to_string();
        if !config.is_enabled() {
            self.remove(&name).await;
            return McpConnectionState::Closed;
        }
        if let Some((_, entry)) = self
            .entries
            .iter_mut()
            .find(|(entry_name, _)| entry_name.as_str() == name)
        {
            if entry.config == config
                || (same_connection_identity(&entry.config, &config)
                    && entry.client.reconfigure_tool_visibility(&config))
            {
                entry.config = config;
                let state = entry.client.state;
                self.initialized = true;
                return state;
            }
        }
        self.dial(config, false).await;
        self.initialized = true;
        self.get_entry(&name)
            .map(|entry| entry.client.state)
            .unwrap_or(McpConnectionState::Failed)
    }

    /// Remove one retained server; its connection closes after captured users
    /// release it. Unknown names return `false`.
    pub async fn remove(&mut self, server_name: &str) -> bool {
        let Some(index) = self
            .entries
            .iter()
            .position(|(name, _)| name == server_name)
        else {
            return false;
        };
        self.entries.remove(index);
        self.announce_state(server_name, McpConnectionState::Closed);
        true
    }

    /// Diff a complete config set. Unchanged connections survive without
    /// re-listing; replaced connections close after their old users release them.
    pub async fn reload(&mut self, configs: Vec<McpServerConfig>) -> Result<(), String> {
        let mut order = HashMap::new();
        for (index, config) in configs.iter().enumerate() {
            let name = config.name().to_string();
            if order.insert(name.clone(), index).is_some() {
                return Err(format!("duplicate MCP server name: {name}"));
            }
        }

        let removed = self
            .entries
            .iter()
            .filter(|(name, _)| !order.contains_key(name))
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        for name in removed {
            self.remove(&name).await;
        }
        for config in configs {
            self.upsert(config).await;
        }
        self.entries.sort_by_key(|(name, _)| order[name]);
        self.initialized = true;
        Ok(())
    }

    /// Close a live authenticated client after credential revoke while retaining
    /// its config and reporting a real `needs-auth` runtime state.
    pub async fn mark_authorization_revoked(&mut self, server_name: &str) -> bool {
        let Some(entry) = self
            .entries
            .iter_mut()
            .find(|(name, _)| name == server_name)
            .map(|(_, entry)| entry)
        else {
            return false;
        };
        entry.client.close().await;
        entry.client = authorization_revoked_client(server_name);
        let announced = entry.client.clone();
        self.announce(server_name, &announced);
        true
    }

    /// Read-only snapshot of the currently exposed MCP runtime surface.
    pub fn runtime_snapshot(&self) -> McpRuntimeSnapshot {
        mcp_runtime_snapshot_from_clients(&self.clients())
    }

    /// Close every managed connection. Best-effort; never throws.
    pub async fn shutdown(&mut self) {
        let clients: Vec<ManagedClient> =
            self.entries.iter().map(|(_, e)| e.client.clone()).collect();
        // Cleanup runs as its own task so an interrupt that cancels the
        // requesting shutdown cannot abandon server processes mid-close.
        let shutdown_task = tokio::spawn(async move {
            futures::future::join_all(clients.iter().map(|c| c.close())).await;
        });
        if let Err(error) = shutdown_task.await {
            tracing::warn!("MCP client shutdown task failed: {error}");
        }
        for (_, e) in &mut self.entries {
            e.client.state = McpConnectionState::Closed;
        }
    }

    // ---- name-keyed insertion-ordered map helpers ----

    fn get_entry(&self, name: &str) -> Option<&Entry> {
        self.entries.iter().find(|(n, _)| n == name).map(|(_, e)| e)
    }

    fn set_entry(&mut self, name: String, entry: Entry) {
        if let Some(slot) = self.entries.iter_mut().find(|(n, _)| *n == name) {
            slot.1 = entry;
        } else {
            self.entries.push((name, entry));
        }
    }
}

/// A tool that presents a different `name` than the tool it wraps, delegating
/// every other behavior to the inner tool (the collision-suffix rename).
struct RenamedTool {
    inner: Arc<dyn Tool>,
    name: RuntimeString,
}

#[async_trait]
impl Tool for RenamedTool {
    fn native_mcp_binding(&self) -> Option<crate::mcp::tool_wrapper::NativeMcpToolBinding> {
        self.inner.native_mcp_binding()
    }
    fn rebind_native_mcp(&self, replacement: Arc<dyn Tool>) -> Arc<dyn Tool> {
        Arc::new(Self {
            inner: self.inner.rebind_native_mcp(replacement),
            name: self.name.clone(),
        })
    }
    fn approval_metadata(&self) -> Option<crate::types::permission::ApprovalActionMetadata> {
        self.inner.approval_metadata().map(|mut metadata| {
            metadata.registered_tool_name = self.name.clone();
            metadata
        })
    }

    fn name(&self) -> RuntimeString {
        self.name.clone()
    }
    fn aliases(&self) -> Vec<RuntimeString> {
        self.inner.aliases()
    }
    fn description(&self) -> RuntimeString {
        self.inner.description()
    }
    fn input_schema(&self) -> Json {
        self.inner.input_schema()
    }
    fn output_schema(&self) -> Option<Json> {
        self.inner.output_schema()
    }
    fn exposure(&self) -> ToolExposure {
        self.inner.exposure()
    }
    fn search_hint(&self) -> Option<RuntimeString> {
        self.inner.search_hint()
    }
    fn max_result_size_chars(&self) -> Option<u64> {
        self.inner.max_result_size_chars()
    }
    fn mcp_server(&self) -> Option<RuntimeString> {
        self.inner.mcp_server()
    }
    fn is_enabled(&self) -> bool {
        self.inner.is_enabled()
    }
    fn is_concurrency_safe(&self, input: &Json) -> bool {
        self.inner.is_concurrency_safe(input)
    }
    fn is_read_only(&self, input: &Json) -> bool {
        self.inner.is_read_only(input)
    }
    fn is_destructive(&self, input: &Json) -> Option<bool> {
        self.inner.is_destructive(input)
    }
    fn parse_input(&self, input: &Json) -> Result<Json, ToolError> {
        self.inner.parse_input(input)
    }
    async fn validate_input(&self, input: &Json, ctx: &ToolContext) -> Option<ValidationResult> {
        self.inner.validate_input(input, ctx).await
    }
    async fn check_permissions(&self, input: &Json, ctx: &ToolContext) -> Option<PermissionResult> {
        self.inner.check_permissions(input, ctx).await
    }
    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.inner.call(input, ctx, on_progress).await
    }
    async fn call_with_id(
        &self,
        call_id: &RuntimeString,
        input: Json,
        ctx: &ToolContext,
        on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.inner
            .call_with_id(call_id, input, ctx, on_progress)
            .await
    }
    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        self.inner.summarize(input)
    }
    fn to_command_string(&self, input: &Json) -> Option<RuntimeString> {
        self.inner.to_command_string(input)
    }
    fn get_path(&self, input: &Json) -> Option<PathTargets> {
        self.inner.get_path(input)
    }
    fn get_raw_path(&self, input: &Json) -> Option<Json> {
        self.inner.get_raw_path(input)
    }
    fn permission_path_kind(&self) -> Option<PermissionPathKind> {
        self.inner.permission_path_kind()
    }
}

/// Sleep for `ms` milliseconds without keeping the runtime alive on its own.
async fn sleep(ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

#[cfg(test)]
mod approval_metadata_tests {
    use super::*;
    use crate::mcp::tool_wrapper::{wrap_mcp_tool, McpToolCall};
    use crate::mcp::types::{McpCallResult, McpToolDescriptor};
    use crate::types::permission::ApprovalActionSource;

    #[test]
    fn collision_rename_retains_registered_server_and_raw_tool_identity() {
        let call: McpToolCall =
            Arc::new(|_, _, _, _| Box::pin(async { Ok(McpCallResult::default()) }));
        let inner = wrap_mcp_tool(
            "registered-server",
            McpToolDescriptor {
                name: "raw-tool".into(),
                title: Some("Actual title".into()),
                ..Default::default()
            },
            call,
            None,
        );
        let renamed = RenamedTool {
            inner,
            name: "visible_collision_2".into(),
        };
        let metadata = renamed.approval_metadata().unwrap();
        assert_eq!(metadata.registered_tool_name, "visible_collision_2");
        assert!(
            matches!(metadata.source, ApprovalActionSource::Mcp(metadata)
            if metadata.server == "registered-server" && metadata.tool_name == "raw-tool"
                && metadata.tool_title == Some("Actual title".into()))
        );
    }
}
