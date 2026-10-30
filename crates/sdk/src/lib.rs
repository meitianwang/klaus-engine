//! The ergonomic, in-process embedding surface over [`klaus_agent_core`].
//!
//! The recommended entry point for embedding the engine in your own program (a
//! desktop app, a cloud service, a script). It wraps the lower-level
//! [`Agent`]/[`Session`] into an `AgentEngine -> Thread -> run()/run_streamed()`
//! shape with sane defaults — including persistence/resume and a unified, scoped
//! `rewind` — while exposing a small, stable surface instead of core's full API.
//! This runs the engine IN-PROCESS: no child process, no IPC.
//!
//! The Task 29 browser-host implementation is an internal shadow until the
//! atomic browser cutover; downstream crates cannot select its executable or
//! path authority.
//!
//! ```compile_fail,E0603
//! use klaus_agent_sdk::browser_host::OpenChromeProcessPlan;
//! ```

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use async_stream::stream;
use async_trait::async_trait;
use futures::{FutureExt, StreamExt};
use serde_json::Value;
use tokio::sync::Mutex as AsyncMutex;

// Internal-only imports (the lower-level core surface this facade drives).
use klaus_agent_core::browser::{BrowserProcessBinding, BrowserRuntime};
use klaus_agent_core::capabilities::{RuntimeCapabilityRoot, SelectedRuntimeCapabilityRoot};
use klaus_agent_core::engine::agent::{
    Agent, AgentOptions, LspServersThunk, OutputStylesThunk, RefreshToolsThunk, SkillsThunk,
    StartSessionOptions, WorkflowsThunk,
};
use klaus_agent_core::engine::deps::EngineDepsOverrides;
use klaus_agent_core::engine::rewind::{BoxError, RewindController, RewindSession, RollbackResult};
use klaus_agent_core::engine::session::{EnqueueOptions, Session, Turn};
use klaus_agent_core::mcp::credential_store::CredentialStore;
pub use klaus_agent_core::memory::agent_memory::{
    AgentMemoryHost, AgentMemoryMergeChoice, AgentMemoryScope,
};
use klaus_agent_core::observability::cost_tracker::ModelPricing;
use klaus_agent_core::observability::logger::Logger;
use klaus_agent_core::observability::otel::OtelObserver;
use klaus_agent_core::provider::anthropic::AnthropicProviderOptions;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::tools::builtins::index::PluginCatalogThunk;
use klaus_agent_core::tools::builtins::web_fetch::PageFetcher;
use klaus_agent_core::types::provider::SystemPromptPart;

#[allow(dead_code, unused_imports)]
mod browser_host;
mod browser_runtime;

pub use browser_host::{run_browser_cli_main, run_browser_native_host_main};

// Re-export the core types + authoring helpers an embedder commonly needs, so
// consumers import only from this crate.
pub use browser_runtime::{
    LocalBrowserCapabilityRoot, LocalBrowserRuntime, PlaywrightBrowserRuntime,
    PlaywrightBrowserRuntimeOptions, PlaywrightEnvOverlay,
};
pub use klaus_agent_core::browser;
pub use klaus_agent_core::capabilities;
pub use klaus_agent_core::context::compact::{
    CompactionDiagnostic, CompactionDiagnosticKind, CompactionDiagnosticStage, CompactionNoopReason,
};
pub use klaus_agent_core::context::system_prompt::{
    build_system_prompt, create_system_prompt_provider, SystemPromptInput,
};
pub use klaus_agent_core::engine::agent_definitions::AgentDefinition;
pub use klaus_agent_core::engine::message_queue::{
    QueueEdit, QueueItemState, QueueMutationResult, QueueMutationStatus, QueuePriority,
    QueueSnapshot, QueueSnapshotItem, QueueSnapshotStatus, QueueStateSubscriber,
    QueueTerminalState, QueuedMessage, QueuedValue,
};
pub use klaus_agent_core::engine::rewind::{RewindOutcome, RewindScope};
pub use klaus_agent_core::engine::session::{
    BackgroundAgentInfo, CompactOutcome, DreamNowCompletion, DreamNowOutcome, DreamNowReason,
    DreamNowTask, GenerateTitleResult, Input, SessionContextSnapshot, SideQuestionResult,
    SystemPromptProvider, TurnOptions, TurnResult, UserContentPart,
};
pub use klaus_agent_core::extensions::plugins::catalog::{
    LocalPluginCatalog, PluginCatalog, PluginCatalogEntry, PluginCatalogSearchOptions,
    PluginInstallFromCatalogOptions, KLAUS_PLUGIN_CATALOG_DIR_ENV,
};
pub use klaus_agent_core::extensions::plugins::identity::PluginId;
pub use klaus_agent_core::extensions::plugins::installed::{
    default_plugin_home, installed_plugins_path, load_installed_plugins_report,
    read_installed_plugins, record_installed_plugin, record_installed_plugin_package,
    set_installed_plugin_enabled, uninstall_installed_plugin, write_installed_plugins,
    InstalledPluginRecord, InstalledPlugins,
};
pub use klaus_agent_core::extensions::plugins::loader::{
    LoadedPlugin, PluginDisabledEntry, PluginLoadIssue, PluginLoadIssueKind, PluginLoadReport,
};
pub use klaus_agent_core::extensions::plugins::remote::{
    install_plugin, parse_plugin_source, PluginSource, PluginSpec,
};
pub use klaus_agent_core::extensions::plugins::runtime::{
    PluginCapabilitySummary, PluginPermissionScope, PluginRuntimeEntry, PluginRuntimeSnapshot,
};
pub use klaus_agent_core::extensions::plugins::store::{
    PluginInstallResult, PluginStore, DEFAULT_PLUGIN_VERSION,
};
pub use klaus_agent_core::extensions::skills::loader::Skill;
pub use klaus_agent_core::hooks::setup::{
    SetupLifecycleEvent, SetupLifecycleSnapshot, SetupRunRecord, SetupRunRequest, SetupRunResult,
    SetupRunStatus, SetupTrigger,
};
pub use klaus_agent_core::hooks::types::{
    CombinedHookOutcome, Hook, HookDecision, HookEvent, HookFn, HookHandler, HookInput,
    HookOutcome, HookResult, HookTrigger, HOOK_EVENTS,
};
pub use klaus_agent_core::mcp::client::ManagedClient;
pub use klaus_agent_core::mcp::types::McpServerConfig;
pub use klaus_agent_core::memory::maintenance::{
    AutoDreamRun, AutoDreamRunReason, AutoMemoryRun, AutoMemoryRunReason, MemoryMaintenanceEvent,
    MemoryMaintenanceEventSink, MemoryMaintenanceQueryError, MemoryMaintenanceQueryErrorCode,
    MemoryMaintenanceQueryErrorDetail, MemoryMaintenanceRunKind, MemoryMaintenanceSnapshot,
    MemoryMaintenanceStatus, MemoryMaintenanceTrigger, MemoryMaintenanceTurn,
};
pub use klaus_agent_core::memory::snapshot_decision::{
    AgentMemorySnapshotApplyError, AgentMemorySnapshotApplyOutcome,
    AgentMemorySnapshotDecisionAudit, AgentMemorySnapshotDecisionPolicy,
    AgentMemorySnapshotDecisionRegistry, AgentMemorySnapshotDecisionRequest,
    AgentMemorySnapshotDecisionSource, AgentMemorySnapshotDecisionStatus, AgentMemorySnapshotDiff,
    AgentMemorySnapshotDiffEntry, AgentMemorySnapshotDiffKind, AgentMemorySnapshotMarkerRead,
    AgentMemorySnapshotPolicyAction, AgentMemorySnapshotRegisterResult,
    AgentMemorySnapshotRegistryError, AgentMemorySnapshotSyncMarker,
    AgentMemorySnapshotTransitionResult, AGENT_MEMORY_SNAPSHOT_DECISION_ROLLOUT_EVENT,
    MAX_SNAPSHOT_DIFF_PATHS,
};
pub use klaus_agent_core::observability::cost_tracker::CostSnapshot;
pub use klaus_agent_core::provider::anthropic::AnthropicProvider;
pub use klaus_agent_core::provider::claude_subscription::create_claude_subscription_provider;
pub use klaus_agent_core::provider::codex_subscription::create_codex_subscription_provider;
pub use klaus_agent_core::provider::gemini::GeminiProvider;
pub use klaus_agent_core::provider::gemini_subscription::create_gemini_subscription_provider;
pub use klaus_agent_core::provider::interceptor::{
    ProviderRequestDirectives, ProviderRequestInfo, ProviderRequestInterceptor,
    ProviderResponseInfo, SharedRequestInterceptor,
};
pub use klaus_agent_core::provider::model_catalog::{
    with_model_catalog, CapabilityOverrides, CatalogEntry, CatalogMatch, CatalogRefresh,
    ModelCatalog, MODEL_CATALOG_URL_ENV,
};
pub use klaus_agent_core::provider::openai::OpenAIProvider;
pub use klaus_agent_core::runtime_process::{
    RuntimeProcessBootstrapError, RuntimeProcessContext, RuntimeProcessNamespaceId,
    RuntimeProcessShutdownError, RuntimeProcessSupervisor,
};
pub use klaus_agent_core::runtime_values::RuntimeString;
pub use klaus_agent_core::schemas::config::{EngineConfig, SandboxWorkspaceWrite};
pub use klaus_agent_core::schemas::message::Message;
pub use klaus_agent_core::store::search::{
    SessionSearchHit, SessionSearchOptions, SessionSearchPage,
};
pub use klaus_agent_core::store::session_store::{
    ListOptions, SessionStore, SubAgentTranscript, ThreadPage, ThreadSummary,
    UnarchiveSessionResult, UnarchiveSessionStatus,
};
pub use klaus_agent_core::tasks::task_store::{
    Task, TaskListChange, TaskListChangeSubscriber, TaskListCursorStatus, TaskListQueryOptions,
    TaskListSnapshot, TaskStatus,
};
pub use klaus_agent_core::tools::build_tool::text_result;
pub use klaus_agent_core::types::abort::AbortSignal;
pub use klaus_agent_core::types::errors::{
    InputValidationError, InputValidationErrorCode, InputValidationErrorDetail,
};
pub use klaus_agent_core::types::events::EngineEvent;
pub use klaus_agent_core::types::permission::{ApprovalHandler, ExternalPermissionMode};
pub use klaus_agent_core::types::provider::ModelProvider;
pub use klaus_agent_core::types::tool::{ElicitHandler, Tool};

/// A boxed engine-event stream (a streamed turn's events).
pub type EventStream = Pin<Box<dyn futures::Stream<Item = EngineEvent> + Send>>;

/// Errors surfaced by the SDK facade.
#[derive(Debug, thiserror::Error)]
pub enum SdkError {
    #[error("process context mismatch: expected namespace {expected}, got {actual}")]
    ProcessContextMismatch { expected: u64, actual: u64 },
    #[error(
        "browser_runtime and local_browser_runtime cannot both select the direct browser slot"
    )]
    BrowserRuntimeConflict,
    #[error(
        "capability_roots and local_browser_capability_roots cannot select the same capability source"
    )]
    BrowserCapabilityRootConflict,
    #[error(transparent)]
    ProcessShutdown(#[from] RuntimeProcessShutdownError),
    #[error(transparent)]
    AgentMemorySnapshotDecision(#[from] AgentMemorySnapshotApplyError),
    #[error(transparent)]
    MemoryMaintenance(#[from] MemoryMaintenanceQueryError),
    #[error("resumeThread requires a store — pass `store` to AgentEngine")]
    ResumeRequiresStore,
    #[error("listThreads requires a store — pass `store` to AgentEngine")]
    ListRequiresStore,
    #[error("forkThreadById requires a store — pass `store` to AgentEngine")]
    ForkByIdRequiresStore,
    #[error("archiveThread requires a store — pass `store` to AgentEngine")]
    ArchiveRequiresStore,
    #[error("unarchiveThread requires a store — pass `store` to AgentEngine")]
    UnarchiveRequiresStore,
    #[error("deleteThread requires a store — pass `store` to AgentEngine")]
    DeleteRequiresStore,
    #[error("updateThreadTitle requires a store — pass `store` to AgentEngine")]
    UpdateTitleRequiresStore,
    #[error("loadSubagentTranscript requires a store — pass `store` to AgentEngine")]
    LoadSubagentTranscriptRequiresStore,
    #[error("fork is only available on threads created via AgentEngine")]
    ForkUnavailable,
    #[error("{0}")]
    Session(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

fn validate_browser_process_binding(
    process_context: &RuntimeProcessContext,
    runtime: &Arc<dyn BrowserRuntime>,
) -> Result<(), SdkError> {
    match runtime.process_binding() {
        BrowserProcessBinding::SpawnFree => Ok(()),
        BrowserProcessBinding::ContextBound(actual) if actual == process_context.namespace_id() => {
            Ok(())
        }
        BrowserProcessBinding::ContextBound(actual) => Err(SdkError::ProcessContextMismatch {
            expected: process_context.namespace_id().get(),
            actual: actual.get(),
        }),
    }
}

/// Persistence backing for an [`AgentEngine`]: a directory path (a file-backed
/// store, good for a desktop app) or a custom [`SessionStore`] (e.g. a DB-backed
/// store for a cloud service).
pub enum StoreOption {
    /// A directory path — a file-backed [`JsonlSessionStore`].
    Dir(String),
    /// A custom store.
    Custom(Arc<dyn SessionStore>),
}

/// Override the system prompt. Either replace it entirely (`Text`), keep the
/// default and append text every turn (`Append`), or take full programmatic
/// control (`Provider`).
pub enum SystemPrompt {
    /// REPLACES the engine's default prompt entirely.
    Text(String),
    /// KEEPS the default prompt and appends this text every turn.
    Append { append: String },
    /// Full programmatic control (core's `system_prompt_provider`).
    Provider(SystemPromptProvider),
}

/// The low-level `Agent` options escape hatch: mutate the fully-built
/// [`AgentOptions`] just before the [`Agent`] is constructed — chiefly the
/// `deps` / `compactor` test seams. Applied LAST, so it wins over the
/// first-class fields on conflict. The required process namespace and every
/// context-bound browser runtime are revalidated after this hook and cannot be
/// replaced with a different namespace.
pub type AgentOptionsHook = Box<dyn FnOnce(&mut AgentOptions) + Send>;

/// Options for constructing an [`AgentEngine`]. The process context is required;
/// all other fields default to an ephemeral, API-key Anthropic engine.
///
/// The pre-cutover default constructor is intentionally unavailable:
///
/// ```compile_fail
/// use klaus_agent_sdk::AgentEngineOptions;
///
/// let _ = AgentEngineOptions::default();
/// ```
///
/// A struct update cannot omit the required process namespace either:
///
/// ```compile_fail
/// use klaus_agent_sdk::AgentEngineOptions;
///
/// let _ = AgentEngineOptions {
///     provider: None,
///     ..Default::default()
/// };
/// ```
pub struct AgentEngineOptions {
    /// Required process namespace. Every spawn-capable service owned by this
    /// engine must be bound to this exact context.
    pub process_context: RuntimeProcessContext,
    /// Model provider. Defaults to `AnthropicProvider` (reads `ANTHROPIC_API_KEY`).
    pub provider: Option<Arc<dyn ModelProvider>>,
    /// Model id override; defaults to the provider's default model.
    pub model: Option<String>,
    /// Working directory the agent's tools operate in. Defaults to the process cwd.
    pub cwd: Option<String>,
    /// Permission mode for tool calls.
    pub permission_mode: Option<ExternalPermissionMode>,
    /// Runtime network selection; omission derives it from the sandbox config.
    pub network_allowed: Option<bool>,
    /// Answer tool-permission prompts in `default` mode.
    pub approval_handler: Option<Arc<dyn ApprovalHandler>>,
    /// Answer `ask_user` (elicitation) prompts.
    pub elicit_handler: Option<Arc<dyn ElicitHandler>>,
    /// Persist threads so they survive a restart and can be resumed.
    pub store: Option<StoreOption>,
    /// Engine config using the public schema keys.
    pub config: Option<Value>,
    /// Session-cumulative spend ceiling in USD. Shortcut for `config.maxBudgetUsd`.
    pub max_budget_usd: Option<f64>,
    /// Custom tools the agent can call (REPLACES the built-in tool set).
    pub tools: Option<Vec<Arc<dyn Tool>>>,
    /// Subagent definitions the agent can delegate to.
    pub agents: Option<Vec<AgentDefinition>>,
    /// Lifecycle hooks (pre/post tool-use, etc.).
    pub hooks: Option<Vec<Hook>>,
    /// Skills provider — a thunk returning the skills discoverable this session.
    pub skills: Option<SkillsThunk>,
    /// Already-connected MCP clients whose tools/prompts/resources are folded in.
    pub mcp_clients: Option<Vec<ManagedClient>>,
    /// Host/user/project MCP server declarations used to suppress duplicate
    /// plugin-declared MCP servers before they are connected.
    pub mcp_server_configs: Option<Vec<McpServerConfig>>,
    /// Recompute the live tool set BETWEEN turns.
    pub refresh_tools: Option<RefreshToolsThunk>,
    /// Override the system prompt.
    pub system_prompt: Option<SystemPrompt>,
    /// Isolate file edits in a git worktree. Shortcut for `config.worktree`.
    pub worktree: Option<bool>,
    /// Named workflows the `workflow` tool can run by name (auto-enables `config.workflow`).
    pub workflows: Option<WorkflowsThunk>,
    /// Language servers the engine starts on demand (auto-enables `config.lsp`).
    pub lsp_servers: Option<LspServersThunk>,
    /// Host adapter for the per-agent persistent memory subsystem (auto-enables `config.agentMemory`).
    pub agent_memory: Option<Arc<dyn AgentMemoryHost>>,
    /// Explicit first-use and unavailable-Host behavior for Agent Memory
    /// snapshot decisions. This exact policy is cloned into every started,
    /// resumed, and forked thread.
    pub agent_memory_snapshot_policy: Option<AgentMemorySnapshotDecisionPolicy>,
    /// Output-style source: named personas whose body is appended to the system prompt.
    pub output_styles: Option<OutputStylesThunk>,
    /// Active output-style NAME selected from `output_styles`. Shortcut for `config.outputStyle`.
    pub output_style: Option<String>,
    /// Cost table (model-substring → USD/MTok rate) for otherwise-unpriced models.
    pub pricing: Option<std::collections::HashMap<String, ModelPricing>>,
    /// OpenTelemetry metrics sink fed every engine event. `Some(None)` disables auto-instrumentation.
    pub observer: Option<Option<Arc<Mutex<OtelObserver>>>>,
    /// Structured logger fed turn-lifecycle + error events.
    pub logger: Option<Arc<Logger>>,
    /// MCP tools to APPEND to the built-in tool set.
    pub mcp_tools: Option<Vec<Arc<dyn Tool>>>,
    /// Secret store for per-sub-agent MCP server auth.
    pub credential_store: Option<Arc<dyn CredentialStore>>,
    /// JS-rendering page fetcher for `web_fetch`.
    pub web_fetch_page: Option<Arc<dyn PageFetcher>>,
    /// Opaque registered browser automation runtime for browser_* tools.
    /// Third-party remote/in-process executors require an explicit unsafe
    /// spawn-free attestation; use `local_browser_runtime` for SDK Playwright.
    pub browser_runtime: Option<Arc<dyn BrowserRuntime>>,
    /// Sealed local-spawn browser capability. Its namespace and runtime are
    /// private and can currently be minted only by `PlaywrightBrowserRuntime`.
    pub local_browser_runtime: Option<LocalBrowserRuntime>,
    /// Host/plugin runtime capability roots. Prefer this for capability-aware
    /// hosts; public browser runtimes in these roots carry the same unsafe
    /// spawn-free attestation as `browser_runtime`.
    pub capability_roots: Option<Vec<RuntimeCapabilityRoot>>,
    /// Sealed local-spawn browser capability roots. Each runtime is checked
    /// against this engine's process namespace before the root is admitted.
    pub local_browser_capability_roots: Option<Vec<LocalBrowserCapabilityRoot>>,
    /// Default selected roots for sessions opened through this engine.
    pub selected_capability_roots: Option<Vec<SelectedRuntimeCapabilityRoot>>,
    /// Already-loaded plugin bundles. The engine merges their skills, commands,
    /// agents, hooks, output styles, and declared MCP/LSP servers.
    pub plugins: Option<Vec<LoadedPlugin>>,
    /// Host-provided plugin catalog used by `plugin_search` and `request_plugin_install`.
    pub plugin_catalog: Option<PluginCatalogThunk>,
    /// Full plugin loader state, including enabled plugins plus disabled entries,
    /// load errors, and warnings. When `plugins` is absent, enabled plugins from
    /// this report seed the engine.
    pub plugin_load_report: Option<PluginLoadReport>,
    /// Installed-plugin state directory. Defaults to `~/.klaus/plugins`.
    pub plugin_home: Option<String>,
    /// Ship the built-in sub-agent roles (`general-purpose`/`Explore`/`Plan`); default `true`.
    pub include_default_agents: Option<bool>,
    /// Escape hatch for the truly low-level core `Agent` options not surfaced above.
    pub agent_options: Option<AgentOptionsHook>,
}

impl AgentEngineOptions {
    pub fn new(process_context: RuntimeProcessContext) -> Self {
        Self {
            process_context,
            provider: None,
            model: None,
            cwd: None,
            permission_mode: None,
            network_allowed: None,
            approval_handler: None,
            elicit_handler: None,
            store: None,
            config: None,
            max_budget_usd: None,
            tools: None,
            agents: None,
            hooks: None,
            skills: None,
            mcp_clients: None,
            mcp_server_configs: None,
            refresh_tools: None,
            system_prompt: None,
            worktree: None,
            workflows: None,
            lsp_servers: None,
            agent_memory: None,
            agent_memory_snapshot_policy: None,
            output_styles: None,
            output_style: None,
            pricing: None,
            observer: None,
            logger: None,
            mcp_tools: None,
            credential_store: None,
            web_fetch_page: None,
            browser_runtime: None,
            local_browser_runtime: None,
            capability_roots: None,
            local_browser_capability_roots: None,
            selected_capability_roots: None,
            plugins: None,
            plugin_catalog: None,
            plugin_load_report: None,
            plugin_home: None,
            include_default_agents: None,
            agent_options: None,
        }
    }
}

/// Engine-level state shared into every [`Thread`] this engine mints (so the
/// fork capability can spawn new threads from the same factory).
struct EngineShared {
    agent: Agent,
    process_context: RuntimeProcessContext,
    store: Option<Arc<dyn SessionStore>>,
    permission_mode: Option<ExternalPermissionMode>,
    network_allowed: Option<bool>,
    selected_capability_roots: Option<Vec<SelectedRuntimeCapabilityRoot>>,
    agent_memory_snapshot_policy: Option<AgentMemorySnapshotDecisionPolicy>,
    /// Persistent system-prompt append (from `SystemPrompt::Append`), folded into every turn.
    persistent_append: Option<String>,
    plugin_home: PathBuf,
}

impl EngineShared {
    fn session_options(&self) -> StartSessionOptions {
        StartSessionOptions {
            permission_mode: self.permission_mode,
            network_allowed: self.network_allowed,
            selected_capability_roots: self.selected_capability_roots.clone(),
            agent_memory_snapshot_policy: self.agent_memory_snapshot_policy.clone(),
            plugin_home: Some(self.plugin_home.to_string_lossy().into_owned()),
            ..Default::default()
        }
    }

    /// Wrap a core [`Session`] as a [`Thread`], injecting the engine-level fork
    /// capability + persistent append.
    fn wrap(shared: &Arc<EngineShared>, session: Arc<Session>) -> Thread {
        let sh = shared.clone();
        let forked_from_id = session.id.clone();
        let fork: ForkFn = Arc::new(move |history: Vec<Message>| {
            let sh = sh.clone();
            let forked_from_id = forked_from_id.clone();
            Box::pin(async move { sh.spawn_seeded(history, forked_from_id).await })
                as Pin<Box<dyn Future<Output = Result<Thread, SdkError>> + Send>>
        });
        Thread::new(
            session,
            ThreadInternals {
                fork: Some(fork),
                append: shared.persistent_append.clone(),
            },
        )
    }

    /// Start a session seeded with a COPY of prior history — the primitive behind
    /// [`Thread::fork`].
    async fn spawn_seeded(
        self: &Arc<EngineShared>,
        initial_history: Vec<Message>,
        forked_from_id: RuntimeString,
    ) -> Result<Thread, SdkError> {
        let sopts = StartSessionOptions {
            initial_history: Some(initial_history),
            forked_from_id: Some(forked_from_id),
            ..self.session_options()
        };
        let session = match &self.store {
            Some(store) => self.agent.open_session(store.clone(), sopts).await?,
            None => self
                .agent
                .try_start_session_async(sopts)
                .await
                .map_err(klaus_agent_core::engine::session::SessionInitError::into_io_error)?,
        };
        Ok(EngineShared::wrap(self, session))
    }
}

/// The top-level handle. Construct once, then open a [`Thread`] per conversation
/// (or resume one).
pub struct AgentEngine {
    shared: Arc<EngineShared>,
}

impl AgentEngine {
    /// Construct an engine bound to the required process namespace.
    ///
    /// Construction is fallible; the old infallible return shape is rejected:
    ///
    /// ```compile_fail
    /// use klaus_agent_sdk::{AgentEngine, AgentEngineOptions};
    ///
    /// fn legacy(options: AgentEngineOptions) -> AgentEngine {
    ///     AgentEngine::new(options)
    /// }
    /// ```
    pub fn new(opts: AgentEngineOptions) -> Result<AgentEngine, SdkError> {
        let AgentEngineOptions {
            process_context,
            provider,
            model,
            cwd,
            permission_mode,
            network_allowed,
            approval_handler,
            elicit_handler,
            store,
            config,
            max_budget_usd,
            tools,
            agents,
            hooks,
            skills,
            mcp_clients,
            mcp_server_configs,
            refresh_tools,
            system_prompt,
            worktree,
            workflows,
            lsp_servers,
            agent_memory,
            agent_memory_snapshot_policy,
            output_styles,
            output_style,
            pricing,
            observer,
            logger,
            mcp_tools,
            credential_store,
            web_fetch_page,
            browser_runtime,
            local_browser_runtime,
            capability_roots,
            local_browser_capability_roots,
            selected_capability_roots,
            plugins,
            plugin_catalog,
            plugin_load_report,
            plugin_home,
            include_default_agents,
            agent_options,
        } = opts;

        let local_browser_runtime = local_browser_runtime.map(LocalBrowserRuntime::into_runtime);
        let mut validated_local_browser_roots = Vec::new();
        for capability_root in local_browser_capability_roots.unwrap_or_default() {
            let (source, capability) = capability_root.into_parts();
            validated_local_browser_roots.push(RuntimeCapabilityRoot {
                source,
                browser_runtime: Some(capability.into_runtime()),
                browser_tool_mapping: None,
            });
        }

        // Merged engine config: the host's partial config, plus the `model` /
        // `maxBudgetUsd` shortcuts and the subsystem auto-enable flags.
        // A subsystem's host dependency flips its config flag on so it "just works";
        // an explicit `config.<flag>` the host passed always wins (it is already in
        // `config_map` from the spread, so the `contains_key` guard skips it).
        let mut config_map: serde_json::Map<String, Value> = match config {
            Some(Value::Object(m)) => m,
            _ => serde_json::Map::new(),
        };
        if let Some(m) = &model {
            if !m.is_empty() {
                config_map.insert("model".to_string(), Value::String(m.clone()));
            }
        }
        if let Some(mb) = max_budget_usd {
            config_map.insert("maxBudgetUsd".to_string(), Value::from(mb));
        }
        if let Some(worktree) = worktree.filter(|_| !config_map.contains_key("worktree")) {
            config_map.insert("worktree".to_string(), Value::Bool(worktree));
        }
        if let Some(os) = &output_style {
            if !config_map.contains_key("outputStyle") {
                config_map.insert("outputStyle".to_string(), Value::String(os.clone()));
            }
        }
        if lsp_servers.is_some() && !config_map.contains_key("lsp") {
            config_map.insert("lsp".to_string(), Value::Bool(true));
        }
        if workflows.is_some() && !config_map.contains_key("workflow") {
            config_map.insert("workflow".to_string(), Value::Bool(true));
        }
        if agent_memory.is_some() && !config_map.contains_key("agentMemory") {
            config_map.insert("agentMemory".to_string(), Value::Bool(true));
        }

        // Resolve the `system_prompt` option into core primitives: a `Text` or
        // `Provider` becomes a `system_prompt_provider` (full control of the base
        // prompt); `Append` is kept and stacked onto every turn so the engine's
        // default prompt is preserved.
        let mut system_prompt_provider: Option<SystemPromptProvider> = None;
        let mut persistent_append: Option<String> = None;
        match system_prompt {
            Some(SystemPrompt::Text(sp)) => {
                let text = sp;
                system_prompt_provider = Some(Arc::new(move || {
                    let text = text.clone();
                    Box::pin(async move {
                        vec![SystemPromptPart {
                            name: RuntimeString::from("custom"),
                            text: RuntimeString::from(text),
                            cacheable: Some(true),
                            cache_scope: None,
                        }]
                    })
                        as Pin<Box<dyn Future<Output = Vec<SystemPromptPart>> + Send>>
                }));
            }
            Some(SystemPrompt::Provider(p)) => system_prompt_provider = Some(p),
            Some(SystemPrompt::Append { append }) => persistent_append = Some(append),
            None => {}
        }

        let provider = provider.unwrap_or_else(|| {
            Arc::new(AnthropicProvider::new(AnthropicProviderOptions::default()))
                as Arc<dyn ModelProvider>
        });
        // Capability answers pass through the model catalog: the cached
        // document overlays the provider's built-ins, and a configured remote
        // source is revalidated in the background so a running engine picks
        // up capability changes without a restart or a blocked construction.
        let model_catalog = ModelCatalog::load(None);
        let provider = with_model_catalog(provider, Arc::clone(&model_catalog));
        if ModelCatalog::configured_source().is_some() {
            let catalog = Arc::clone(&model_catalog);
            tokio::spawn(async move {
                let _ = catalog.refresh_from_env_default().await;
            });
        }
        let config_value = if config_map.is_empty() {
            None
        } else {
            Some(Value::Object(config_map))
        };
        let mut agent_opts = AgentOptions {
            process_context: process_context.clone(),
            provider,
            config: config_value,
            tools,
            web_fetch_page,
            browser_runtime,
            capability_roots: capability_roots.unwrap_or_default(),
            // Truthy-string parity: an empty cwd falls back to the process cwd.
            cwd: cwd.filter(|s| !s.is_empty()),
            deps: EngineDepsOverrides::default(),
            compactor: None,
            approval_handler,
            approval_reviewer: None,
            elicit_handler,
            credential_store,
            hooks: hooks.unwrap_or_default(),
            observer,
            logger,
            pricing,
            agents,
            include_default_agents,
            skills,
            output_styles,
            workflows,
            mcp_catalog: None,
            plugin_catalog,
            lsp_servers,
            agent_memory,
            mcp_clients,
            mcp_server_configs: mcp_server_configs.unwrap_or_default(),
            mcp_tools,
            refresh_tools,
            system_prompt_provider,
            plugins: plugins.unwrap_or_default(),
            plugin_load_report,
        };
        // The escape hatch is applied LAST, but it may not break the engine's
        // required process-namespace invariant.
        if let Some(hook) = agent_options {
            hook(&mut agent_opts);
        }
        if agent_opts.process_context.namespace_id() != process_context.namespace_id() {
            return Err(SdkError::ProcessContextMismatch {
                expected: process_context.namespace_id().get(),
                actual: agent_opts.process_context.namespace_id().get(),
            });
        }
        if let Some(local_browser_runtime) = local_browser_runtime {
            if agent_opts.browser_runtime.is_some() {
                return Err(SdkError::BrowserRuntimeConflict);
            }
            // The sealed adapter was not exposed to the public AgentOptions
            // hook, so the hook cannot wrap it and hide its local-spawn binding.
            agent_opts.browser_runtime = Some(local_browser_runtime);
        }
        for local_root in validated_local_browser_roots {
            if agent_opts.capability_roots.iter().any(|existing| {
                existing.source.kind == local_root.source.kind
                    && existing.source.id == local_root.source.id
            }) {
                return Err(SdkError::BrowserCapabilityRootConflict);
            }
            // Sealed adapters were not exposed to the public AgentOptions hook,
            // so the hook cannot wrap one and hide its local-spawn binding.
            agent_opts.capability_roots.push(local_root);
        }
        if let Some(runtime) = &agent_opts.browser_runtime {
            validate_browser_process_binding(&process_context, runtime)?;
        }
        for root in &agent_opts.capability_roots {
            if let Some(runtime) = &root.browser_runtime {
                validate_browser_process_binding(&process_context, runtime)?;
            }
        }
        let agent = Agent::new(agent_opts);

        let store: Option<Arc<dyn SessionStore>> = match store {
            Some(StoreOption::Dir(dir)) => {
                Some(Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
                    dir: Some(dir),
                    archive_dir: None,
                })))
            }
            Some(StoreOption::Custom(s)) => Some(s),
            None => None,
        };

        Ok(AgentEngine {
            shared: Arc::new(EngineShared {
                agent,
                process_context,
                store,
                permission_mode,
                network_allowed,
                selected_capability_roots,
                agent_memory_snapshot_policy,
                persistent_append,
                plugin_home: plugin_home
                    .map(PathBuf::from)
                    .unwrap_or_else(default_plugin_home),
            }),
        })
    }

    /// Build an engine from the installed-plugin ledger in `plugin_home` (or the
    /// default `~/.klaus/plugins`). Enabled records become live plugins; disabled
    /// records and load errors remain visible through `plugin_runtime_snapshot`.
    /// Pass explicit `plugins` or `plugin_load_report` to own loading yourself.
    pub async fn new_from_installed_plugins(
        mut opts: AgentEngineOptions,
    ) -> Result<AgentEngine, SdkError> {
        let plugin_home = opts
            .plugin_home
            .as_ref()
            .map(PathBuf::from)
            .unwrap_or_else(default_plugin_home);
        if opts.plugins.is_none() && opts.plugin_load_report.is_none() {
            opts.plugin_load_report = Some(
                load_installed_plugins_report(
                    &plugin_home,
                    klaus_agent_core::extensions::command_loader::CommandLoadOptions::default(),
                )
                .await?,
            );
        }
        if opts.plugin_home.is_none() {
            opts.plugin_home = Some(plugin_home.to_string_lossy().into_owned());
        }
        AgentEngine::new(opts)
    }

    /// Close this engine's exact process namespace. Hosts that own the process
    /// supervisor remain responsible for its final shutdown after all engines
    /// and browser runtimes have closed.
    pub async fn shutdown(&self) -> Result<(), SdkError> {
        self.shared.process_context.shutdown().await?;
        Ok(())
    }

    /// Open a new conversation. Persisted (resumable) when a `store` was
    /// configured, else ephemeral.
    pub async fn start_thread(&self) -> Result<Thread, SdkError> {
        let sopts = self.shared.session_options();
        let session = match &self.shared.store {
            Some(store) => self.shared.agent.open_session(store.clone(), sopts).await?,
            None => self
                .shared
                .agent
                .try_start_session_async(sopts)
                .await
                .map_err(klaus_agent_core::engine::session::SessionInitError::into_io_error)?,
        };
        Ok(EngineShared::wrap(&self.shared, session))
    }

    /// Reopen a previously persisted conversation by its id. Requires a
    /// configured `store`.
    pub async fn resume_thread(&self, thread_id: &str) -> Result<Thread, SdkError> {
        let store = self
            .shared
            .store
            .clone()
            .ok_or(SdkError::ResumeRequiresStore)?;
        let session = self
            .shared
            .agent
            .resume_session(store, thread_id, self.shared.session_options())
            .await?;
        Ok(EngineShared::wrap(&self.shared, session))
    }

    /// Branch a NEW thread off `source`, seeded with a deep copy of its history up
    /// to now. The fork gets a fresh id and — when a `store` is configured — is
    /// independently persisted; the two threads then diverge.
    pub async fn fork_thread(&self, source: &Thread) -> Result<Thread, SdkError> {
        source.fork().await
    }

    /// Branch a NEW persisted thread from an existing persisted thread id.
    pub async fn fork_thread_by_id(&self, thread_id: &str) -> Result<Thread, SdkError> {
        let store = self
            .shared
            .store
            .clone()
            .ok_or(SdkError::ForkByIdRequiresStore)?;
        let session = self
            .shared
            .agent
            .fork_session(store, thread_id, self.shared.session_options())
            .await?;
        Ok(EngineShared::wrap(&self.shared, session))
    }

    /// List persisted threads (newest first, paginated). Requires a configured
    /// `store`.
    pub async fn list_threads(&self, options: ListOptions) -> Result<ThreadPage, SdkError> {
        let store = self
            .shared
            .store
            .as_ref()
            .ok_or(SdkError::ListRequiresStore)?;
        Ok(store.list(options).await?)
    }

    /// Full-text search across persisted threads (newest first). Matches only
    /// human-visible conversation text; file-backed stores accelerate the
    /// candidate pass with the bundled ripgrep. Requires a configured `store`.
    pub async fn search_threads(
        &self,
        options: SessionSearchOptions,
    ) -> Result<SessionSearchPage, SdkError> {
        let store = self
            .shared
            .store
            .as_ref()
            .ok_or(SdkError::ListRequiresStore)?;
        Ok(store
            .search(
                &options,
                Some(&self.shared.process_context),
                &AbortSignal::new(),
            )
            .await?)
    }

    /// Archive a persisted thread. Requires a configured store.
    pub async fn archive_thread(&self, thread_id: &str) -> Result<(), SdkError> {
        let store = self
            .shared
            .store
            .as_ref()
            .ok_or(SdkError::ArchiveRequiresStore)?;
        store.archive(thread_id).await?;
        Ok(())
    }

    /// Idempotently restore a persisted thread to the active namespace.
    pub async fn unarchive_thread(
        &self,
        thread_id: &str,
    ) -> Result<UnarchiveSessionResult, SdkError> {
        let store = self
            .shared
            .store
            .as_ref()
            .ok_or(SdkError::UnarchiveRequiresStore)?;
        Ok(store.unarchive(thread_id).await)
    }

    /// Delete a persisted thread's rollout data. Requires a configured store.
    pub async fn delete_thread(&self, thread_id: &str) -> Result<(), SdkError> {
        let store = self
            .shared
            .store
            .as_ref()
            .ok_or(SdkError::DeleteRequiresStore)?;
        Ok(store.delete(thread_id).await?)
    }

    /// Update Rust-owned persisted thread title metadata.
    pub async fn update_thread_title(&self, thread_id: &str, title: &str) -> Result<(), SdkError> {
        let store = self
            .shared
            .store
            .as_ref()
            .ok_or(SdkError::UpdateTitleRequiresStore)?;
        Ok(store.update_title(thread_id, title).await?)
    }

    /// Load a persisted sub-agent transcript by parent thread id and agent id.
    pub async fn load_subagent_transcript(
        &self,
        thread_id: &str,
        agent_id: &str,
    ) -> Result<SubAgentTranscript, SdkError> {
        let store = self
            .shared
            .store
            .as_ref()
            .ok_or(SdkError::LoadSubagentTranscriptRequiresStore)?;
        Ok(store.load_sub_agent(thread_id, agent_id).await?)
    }

    /// Read-only plugin runtime state: enabled plugins, effective capability
    /// summaries, disabled entries, load warnings/errors, and refresh/reconnect
    /// counters.
    pub fn plugin_runtime_snapshot(&self) -> PluginRuntimeSnapshot {
        self.shared.agent.plugin_runtime_snapshot()
    }

    /// Runtime capability roots visible at the engine level before any
    /// per-thread root selection is applied.
    pub fn runtime_capability_descriptors(
        &self,
    ) -> Vec<klaus_agent_core::capabilities::RuntimeCapabilityDescriptor> {
        self.shared.agent.runtime_capability_descriptors()
    }

    /// Read the persisted installed-plugin ledger for this engine's plugin home.
    pub async fn installed_plugins(&self) -> Result<InstalledPlugins, SdkError> {
        Ok(read_installed_plugins(&self.shared.plugin_home).await?)
    }

    /// Enable or disable an installed plugin in the persisted ledger. This does
    /// not mutate this engine's plugin graph or any existing thread. Reconstruct
    /// the engine with [`AgentEngine::new_from_installed_plugins`] to activate it.
    pub async fn set_installed_plugin_enabled(
        &self,
        name: &str,
        enabled: bool,
    ) -> Result<Option<InstalledPluginRecord>, SdkError> {
        Ok(set_installed_plugin_enabled(&self.shared.plugin_home, name, enabled).await?)
    }

    /// Remove an installed plugin record and optionally delete its installed
    /// files, constrained to this engine's plugin home. This engine and its
    /// existing threads retain their startup plugin graph until reconstruction.
    pub async fn uninstall_plugin(
        &self,
        name: &str,
        remove_files: bool,
    ) -> Result<Option<InstalledPluginRecord>, SdkError> {
        Ok(uninstall_installed_plugin(&self.shared.plugin_home, name, remove_files).await?)
    }
}

/// A shareable capability that spawns a new thread seeded with a deep copy of
/// `history` — the primitive behind [`Thread::fork`].
type ForkFn = Arc<
    dyn Fn(Vec<Message>) -> Pin<Box<dyn Future<Output = Result<Thread, SdkError>> + Send>>
        + Send
        + Sync,
>;

/// Engine-injected capabilities a [`Thread`] needs but does not construct itself.
#[derive(Default, Clone)]
struct ThreadInternals {
    fork: Option<ForkFn>,
    /// Persistent system-prompt append (from `SystemPrompt::Append`), folded into every turn.
    append: Option<String>,
}

/// A shared future representing "every prior op on this thread has settled". Each
/// op synchronously reserves its place (cloning the current tail as its `prior`
/// and installing a fresh tail), then awaits `prior` before running — so
/// run / run_streamed / rewind / fork queue in call order and never overlap on
/// the un-reentrant [`Session`].
type ChainFuture = futures::future::Shared<Pin<Box<dyn Future<Output = ()> + Send>>>;

fn ready_chain() -> ChainFuture {
    let f: Pin<Box<dyn Future<Output = ()> + Send>> = Box::pin(async {});
    f.shared()
}

/// Releases a thread's serialization slot when dropped (so one failed / cancelled
/// turn never wedges the next), mirroring the swallow-and-advance chain.
struct ChainRelease(Option<tokio::sync::oneshot::Sender<()>>);

impl Drop for ChainRelease {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
}

/// A single conversation. Run turns sequentially with [`Thread::run`] /
/// [`Thread::run_streamed`] (await each before the next); history accumulates so
/// later turns see earlier ones. Undo with [`Thread::rewind`].
pub struct Thread {
    session: Arc<Session>,
    rewinder: Arc<AsyncMutex<RewindController>>,
    internals: ThreadInternals,
    chain: Mutex<ChainFuture>,
}

impl Thread {
    fn new(session: Arc<Session>, internals: ThreadInternals) -> Thread {
        let adapter: Arc<dyn RewindSession> = Arc::new(SessionRewindAdapter(session.clone()));
        let rewinder = RewindController::new(adapter);
        Thread {
            session,
            rewinder: Arc::new(AsyncMutex::new(rewinder)),
            internals,
            chain: Mutex::new(ready_chain()),
        }
    }

    /// Fold the thread's persistent system-prompt append (if any) into a turn's options.
    fn with_append(&self, mut opts: TurnOptions) -> TurnOptions {
        let Some(base) = &self.internals.append else {
            return opts;
        };
        let per = opts.system_prompt_append.take();
        opts.system_prompt_append = Some(match per {
            Some(p) if !p.is_empty() => RuntimeString::from(base.as_str())
                .concat(&RuntimeString::from("\n\n"))
                .concat(&p),
            _ => RuntimeString::from(base.as_str()),
        });
        opts
    }

    /// Synchronously reserve this op's slot in the serialization chain, returning
    /// the `prior` future to await and the release sender to fire on completion.
    fn reserve(&self) -> (ChainFuture, tokio::sync::oneshot::Sender<()>) {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let mut chain = self.chain.lock().unwrap();
        let prior = chain.clone();
        let tail: Pin<Box<dyn Future<Output = ()> + Send>> = Box::pin(async move {
            let _ = rx.await;
        });
        *chain = tail.shared();
        (prior, tx)
    }

    /// This thread's id — stable across resume; pass it to [`AgentEngine::resume_thread`].
    pub fn id(&self) -> &RuntimeString {
        &self.session.id
    }

    /// Thread id this thread was forked from, if it is a branch.
    pub fn forked_from_id(&self) -> Option<&RuntimeString> {
        self.session.meta.forked_from_id.as_ref()
    }

    /// Run one turn and resolve with its final result (text, items, usage, stop reason).
    pub async fn run(&self, input: Input, opts: TurnOptions) -> TurnResult {
        let turn_opts = self.with_append(opts);
        let (prior, release) = self.reserve();
        let _release = ChainRelease(Some(release));
        prior.await;
        self.rewinder.lock().await.before_turn().await;
        self.session.run(input, turn_opts).await
    }

    /// Run one turn and stream engine events as they happen. Drain the stream
    /// before the next turn.
    pub fn run_streamed(&self, input: Input, opts: TurnOptions) -> EventStream {
        let turn_opts = self.with_append(opts);
        // Reserve the chain slot SYNCHRONOUSLY (so a concurrent call waits), and
        // release it only once this stream is fully consumed.
        let (prior, release) = self.reserve();
        let session = self.session.clone();
        let rewinder = self.rewinder.clone();
        Box::pin(stream! {
            let _release = ChainRelease(Some(release));
            prior.await;
            rewinder.lock().await.before_turn().await;
            let turn: Turn = session.run_streamed(input, turn_opts);
            let mut events = turn.events;
            while let Some(ev) = events.next().await {
                yield ev;
            }
        })
    }

    /// Undo the last `num_turns` user turns. `scope` selects which halves to revert:
    /// `Conversation`, `Files` (needs `config.checkpointing`), or `Both` (default).
    /// Serialized against turns.
    pub async fn rewind(&self, num_turns: f64, scope: Option<RewindScope>) -> RewindOutcome {
        let scope = scope.unwrap_or(RewindScope::Both);
        let (prior, release) = self.reserve();
        let _release = ChainRelease(Some(release));
        prior.await;
        self.rewinder.lock().await.rewind(num_turns, scope).await
    }

    /// Abort the in-flight turn.
    pub fn interrupt(&self) {
        self.session.interrupt();
    }

    /// Branch a new thread off this one, seeded with a deep copy of the history so
    /// far. Serialized against this thread's turns, so the snapshot is always taken
    /// at a clean turn boundary. Errors if this thread wasn't created by an
    /// [`AgentEngine`].
    pub async fn fork(&self) -> Result<Thread, SdkError> {
        let fork = self
            .internals
            .fork
            .clone()
            .ok_or(SdkError::ForkUnavailable)?;
        let (prior, release) = self.reserve();
        let _release = ChainRelease(Some(release));
        prior.await;
        fork(self.session.history()).await
    }

    /// A read-only snapshot of the background sub-agents this thread has spawned.
    pub fn subagents(&self) -> Vec<BackgroundAgentInfo> {
        self.session.background_agent_summaries()
    }

    /// Authoritative automatic Memory extraction/Dream audit state.
    pub fn memory_maintenance(&self) -> Result<MemoryMaintenanceSnapshot, SdkError> {
        Ok(self.session.memory_maintenance_snapshot()?)
    }

    /// Replace the live automatic Memory/Dream lifecycle handler. The snapshot
    /// remains available for recovery if the handler misses an event.
    pub fn set_memory_maintenance_event_handler(
        &self,
        handler: Option<MemoryMaintenanceEventSink>,
    ) {
        self.session.set_memory_maintenance_event_sink(handler);
    }

    /// Subscribe before starting snapshot-bearing work. Core publishes the
    /// persisted `requested` audit before waiting for either the in-process
    /// memory Host or this external resolver.
    pub fn subscribe_agent_memory_snapshot_decisions(
        &self,
    ) -> tokio::sync::broadcast::Receiver<AgentMemorySnapshotDecisionAudit> {
        self.session.subscribe_agent_memory_snapshot_decisions()
    }

    /// Reconnect-safe authoritative snapshot-decision state.
    pub async fn agent_memory_snapshot_decisions(
        &self,
    ) -> Result<Vec<AgentMemorySnapshotDecisionAudit>, SdkError> {
        Ok(self.session.agent_memory_snapshot_decisions().await?)
    }

    /// Query one correlated snapshot-decision request by its stable id.
    pub async fn agent_memory_snapshot_decision(
        &self,
        request_id: &RuntimeString,
    ) -> Result<Option<AgentMemorySnapshotDecisionAudit>, SdkError> {
        Ok(self
            .session
            .agent_memory_snapshot_decision(request_id)
            .await?)
    }

    /// Resolve and apply one snapshot choice through core's exactly-once CAS.
    /// This intentionally does not join the turn serialization chain: a running
    /// sub-agent may be waiting for this Host response.
    pub async fn resolve_agent_memory_snapshot_decision(
        &self,
        request_id: &RuntimeString,
        decision: AgentMemoryMergeChoice,
    ) -> Result<AgentMemorySnapshotDecisionAudit, SdkError> {
        Ok(self
            .session
            .resolve_agent_memory_snapshot_decision(request_id, decision)
            .await?)
    }

    /// Settle a pending request at the Host timeout boundary. Core preserves
    /// either the configured fallback decision or the exact `timed-out` state.
    pub async fn mark_agent_memory_snapshot_timed_out(
        &self,
        request_id: &RuntimeString,
        error: Option<RuntimeString>,
    ) -> Result<AgentMemorySnapshotDecisionAudit, SdkError> {
        Ok(self
            .session
            .mark_agent_memory_snapshot_timed_out(request_id, error)
            .await?)
    }

    /// Settle a pending request when its Host connection disappears.
    pub async fn mark_agent_memory_snapshot_host_disconnected(
        &self,
        request_id: &RuntimeString,
        error: Option<RuntimeString>,
    ) -> Result<AgentMemorySnapshotDecisionAudit, SdkError> {
        Ok(self
            .session
            .mark_agent_memory_snapshot_host_disconnected(request_id, error)
            .await?)
    }

    /// Settle a pending request after all Host handler paths are absent.
    pub async fn mark_agent_memory_snapshot_no_handler(
        &self,
        request_id: &RuntimeString,
        error: Option<RuntimeString>,
    ) -> Result<AgentMemorySnapshotDecisionAudit, SdkError> {
        Ok(self
            .session
            .mark_agent_memory_snapshot_no_handler(request_id, error)
            .await?)
    }

    /// Run a correlated init/maintenance Setup pass.
    pub async fn run_setup(&self, request: SetupRunRequest) -> SetupRunResult {
        self.session.run_setup_hooks(request).await
    }

    /// Reconnect-safe Setup lifecycle state.
    pub fn setup_lifecycle(&self) -> SetupLifecycleSnapshot {
        self.session.setup_lifecycle_snapshot()
    }

    /// Authoritative persisted TodoV2 page for this session.
    ///
    /// The snapshot supersedes queued callbacks at or below its internal
    /// publication cutoff only when they have not crossed callback delivery's
    /// admission point. A Host callback already admitted before the query is
    /// logically earlier than the snapshot and may finish after this future;
    /// queries never wait for arbitrary callback code.
    pub async fn get_task_list(
        &self,
        options: TaskListQueryOptions,
    ) -> std::io::Result<TaskListSnapshot> {
        self.session.get_task_list(options).await
    }

    /// Observe ordered full-state TodoV2 invalidations.
    ///
    /// Callback admission is ordered against authoritative queries, but the
    /// callback body runs on an isolated OS thread. Blocking it cannot delay a
    /// later [`Self::get_task_list`] response.
    pub fn subscribe_task_list_changes(
        &self,
        subscriber: TaskListChangeSubscriber,
    ) -> Box<dyn Fn() + Send + Sync> {
        self.session.subscribe_task_list_changes(subscriber)
    }

    /// Steer the conversation MID-TURN without ending it. With no turn running the
    /// message simply waits in the queue. `priority`: `Now` (jump the queue),
    /// `Next` (default when `None`), or `Later`.
    // Keep InputValidationError's public field layout and this SDK return type
    // source-compatible for callers that construct and destructure it by value.
    #[allow(clippy::result_large_err)]
    pub fn send(
        &self,
        input: Input,
        priority: Option<QueuePriority>,
    ) -> Result<(), InputValidationError> {
        self.session.enqueue(
            input,
            EnqueueOptions {
                priority,
                ..Default::default()
            },
        )
    }

    /// Steering messages still queued (a read-only snapshot).
    pub fn queued(&self) -> Vec<Arc<QueuedMessage>> {
        self.session.queued_messages()
    }

    /// Authoritative revisioned queue state, including reserved/running items.
    pub fn queue_snapshot(&self) -> QueueSnapshot {
        self.session.queue_snapshot()
    }

    /// Observe every queue revision in the same order core commits it.
    pub fn subscribe_queue_changes(
        &self,
        subscriber: QueueStateSubscriber,
    ) -> Box<dyn Fn() + Send + Sync> {
        self.session.subscribe_queue_changes(subscriber)
    }

    /// Compare-and-swap edit of one externally mutable queued item.
    pub async fn edit_queued(
        &self,
        mutation_id: RuntimeString,
        uuid: RuntimeString,
        expected_revision: u64,
        edit: QueueEdit,
    ) -> QueueMutationResult {
        self.session
            .edit_queued_message(mutation_id, uuid, expected_revision, edit)
            .await
    }

    /// Compare-and-swap removal of one externally mutable queued item.
    pub async fn remove_queued(
        &self,
        mutation_id: RuntimeString,
        uuid: RuntimeString,
        expected_revision: u64,
    ) -> QueueMutationResult {
        self.session
            .remove_queued_message(mutation_id, uuid, expected_revision)
            .await
    }

    /// Compare-and-swap reorder within an item's priority lane.
    pub async fn reorder_queued(
        &self,
        mutation_id: RuntimeString,
        uuid: RuntimeString,
        before_uuid: Option<RuntimeString>,
        expected_revision: u64,
    ) -> QueueMutationResult {
        self.session
            .reorder_queued_message(mutation_id, uuid, before_uuid, expected_revision)
            .await
    }

    /// Durably remove + return the main-thread steering messages still queued
    /// after a turn settled. A persistence failure is explicit; callers never
    /// receive items from a speculative, uncommitted queue revision.
    pub async fn take_pending(&self) -> Result<Vec<Arc<QueuedMessage>>, RuntimeString> {
        self.session.take_pending_main_messages().await
    }

    /// Switch the model for SUBSEQUENT turns. A per-turn `TurnOptions.model` still
    /// wins for that one turn.
    pub fn set_model(&self, model: String) -> Result<(), SdkError> {
        self.session.set_model(model).map_err(SdkError::Session)
    }

    /// Switch the permission mode. Read LIVE by the permission gate, so calling this
    /// mid-turn ALSO changes the gating of the remaining tool calls in the in-flight turn.
    pub fn set_permission_mode(&self, mode: ExternalPermissionMode) -> Result<(), SdkError> {
        self.session
            .set_permission_mode(mode)
            .map_err(SdkError::Session)
    }

    /// The conversation so far (read-only).
    pub fn history(&self) -> Vec<Message> {
        self.session.history()
    }

    /// Token-usage + cost snapshot for this thread.
    pub fn cost(&self) -> CostSnapshot {
        self.session.cost_snapshot()
    }

    /// UI-facing context-window and usage snapshot for this thread.
    pub fn context_snapshot(&self) -> SessionContextSnapshot {
        self.session.context_snapshot()
    }

    /// Trigger manual compaction for this thread. Serialized against turns.
    pub async fn compact(
        &self,
        instructions: Option<RuntimeString>,
    ) -> Result<CompactOutcome, SdkError> {
        let (prior, release) = self.reserve();
        let _release = ChainRelease(Some(release));
        prior.await;
        self.session
            .compact(instructions)
            .await
            .map_err(SdkError::Session)
    }

    /// Ask a read-only side question against this thread's history.
    pub async fn side_question(&self, question: String) -> Result<SideQuestionResult, SdkError> {
        self.session
            .ask_side_question(question)
            .await
            .map_err(SdkError::Session)
    }

    /// Ask a side question with cancellation independent from the main turn.
    pub async fn side_question_with_signal(
        &self,
        question: String,
        signal: AbortSignal,
    ) -> Result<SideQuestionResult, SdkError> {
        self.session
            .ask_side_question_with_signal(question, signal)
            .await
            .map_err(SdkError::Session)
    }

    /// Ask a read-only side question with inline multimodal input.
    pub async fn side_question_input(
        &self,
        question: Input,
    ) -> Result<SideQuestionResult, SdkError> {
        self.session
            .ask_side_question_input(question)
            .await
            .map_err(SdkError::Session)
    }

    pub async fn side_question_input_with_signal(
        &self,
        question: Input,
        signal: AbortSignal,
    ) -> Result<SideQuestionResult, SdkError> {
        self.session
            .ask_side_question_input_with_signal(question, signal)
            .await
            .map_err(SdkError::Session)
    }

    /// Trigger manual memory consolidation for this thread.
    pub async fn dream_now(&self) -> Result<DreamNowTask, SdkError> {
        self.session.dream_now().await.map_err(SdkError::Session)
    }

    /// Generate a concise session title from this thread's history.
    pub async fn generate_title(
        &self,
        instructions: Option<String>,
        prompt: Option<String>,
        persist: bool,
    ) -> Result<GenerateTitleResult, SdkError> {
        self.session
            .generate_title(instructions, prompt, persist)
            .await
            .map_err(SdkError::Session)
    }

    /// Generate a title with cancellation independent from the main turn.
    pub async fn generate_title_with_signal(
        &self,
        instructions: Option<String>,
        prompt: Option<String>,
        persist: bool,
        signal: AbortSignal,
    ) -> Result<GenerateTitleResult, SdkError> {
        self.session
            .generate_title_with_signal(instructions, prompt, persist, signal)
            .await
            .map_err(SdkError::Session)
    }

    /// Authoritatively stop this thread after every core resource and rollout
    /// write has finished. Unlike [`Self::dispose`], cleanup, flush, and timeout
    /// failures are returned to the Host.
    pub async fn shutdown(&self) -> Result<(), SdkError> {
        self.session
            .shutdown_and_wait()
            .await
            .map_err(SdkError::Session)
    }

    /// Best-effort compatibility wrapper. New Hosts that need a safe reclaim
    /// boundary should call [`Self::shutdown`] and inspect its result.
    pub async fn dispose(&self) {
        self.session.dispose().await;
    }
}

/// Adapts a core [`Session`] to the [`RewindSession`] mechanism seam the
/// [`RewindController`] drives (conversation rollback + filesystem checkpointing).
struct SessionRewindAdapter(Arc<Session>);

#[async_trait]
impl RewindSession for SessionRewindAdapter {
    fn history(&self) -> Vec<Message> {
        self.0.history()
    }
    fn resumed_checkpoints(&self) -> Vec<Option<String>> {
        self.0.resumed_checkpoints().to_vec()
    }
    async fn snapshot(&self, label: &str) -> Result<Option<String>, BoxError> {
        Ok(self.0.snapshot(Some(label.to_string())).await)
    }
    async fn record_checkpoint(&self, hash: Option<String>) {
        self.0.record_checkpoint(hash).await;
    }
    async fn rollback(&self, num_turns: f64) -> Result<RollbackResult, BoxError> {
        self.0
            .rollback(num_turns as u32)
            .await
            .map(|o| RollbackResult {
                dropped_turns: o.dropped_turns,
                remaining_turns: o.remaining_turns,
                messages: o.messages,
            })
            .map_err(|e| e.into())
    }
    async fn restore_checkpoint(&self, hash: &str) -> Result<(), BoxError> {
        self.0.restore_checkpoint(hash).await.map_err(|e| e.into())
    }
    async fn checkpoint_changed_files(&self, hash: &str) -> Result<Vec<String>, BoxError> {
        self.0
            .checkpoint_changed_files(hash)
            .await
            .map_err(|e| e.into())
    }
}

#[cfg(test)]
struct TestProcessOwner {
    _runtime: tokio::runtime::Runtime,
    supervisor: Arc<RuntimeProcessSupervisor>,
    context: RuntimeProcessContext,
}

#[cfg(test)]
fn test_process_owner() -> &'static TestProcessOwner {
    static OWNER: std::sync::OnceLock<TestProcessOwner> = std::sync::OnceLock::new();
    OWNER.get_or_init(|| {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("SDK test process runtime must initialize");
        let supervisor = RuntimeProcessSupervisor::initialize(runtime.handle().clone())
            .expect("SDK test process supervisor must initialize");
        let context = RuntimeProcessContext::initialize(supervisor.clone())
            .expect("SDK test process context must initialize");
        TestProcessOwner {
            _runtime: runtime,
            supervisor,
            context,
        }
    })
}

#[cfg(test)]
pub(crate) fn test_process_context() -> RuntimeProcessContext {
    test_process_owner().context.clone()
}

#[cfg(test)]
fn test_new_process_context() -> RuntimeProcessContext {
    RuntimeProcessContext::initialize(test_process_owner().supervisor.clone())
        .expect("SDK test process namespace must initialize")
}

#[cfg(test)]
mod tests {
    use super::*;
    use klaus_agent_core::engine::deps::noop_compactor;
    use klaus_agent_core::engine::subagent_lifecycle::SubagentLifecycleRecord;
    use klaus_agent_core::runtime_values::runtime_json;
    use klaus_agent_core::schemas::content::{ContentBlock, HarnessEnvelope};
    use klaus_agent_core::schemas::rollout::{TurnContextSnapshot, UsageLedgerItem};
    use klaus_agent_core::store::session_store::{
        AppendEventOptions, CheckpointRecord, CompactionRecord, LiveRollout, RollbackRecord,
    };
    use klaus_agent_core::tasks::task_store::{create_task, CreateTaskInput};
    use klaus_agent_core::testing::{ev, fake_provider, scripted_call_model, ScriptedTurns};
    use klaus_agent_core::types::abort::AbortSignal;
    use klaus_agent_core::types::errors::ToolError;
    use klaus_agent_core::types::json::{Json, JsonSchema};
    use klaus_agent_core::types::provider::{
        CallModel, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    };
    use klaus_agent_core::types::tool::{ToolContext, ToolProgress, ToolResult};
    use serde_json::json;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

    #[derive(Debug, Default)]
    struct PanickingSdkMaintenanceRollout {
        append_panicked: AtomicBool,
        append_panicked_notify: tokio::sync::Notify,
    }

    impl PanickingSdkMaintenanceRollout {
        async fn wait_until_append_panicked(&self) {
            loop {
                let notified = self.append_panicked_notify.notified();
                if self.append_panicked.load(Ordering::SeqCst) {
                    return;
                }
                notified.await;
            }
        }
    }

    #[async_trait::async_trait]
    impl LiveRollout for PanickingSdkMaintenanceRollout {
        async fn prepare_batch(
            &self,
            maximum_records: usize,
        ) -> std::io::Result<klaus_agent_core::store::session_store::PreparedRolloutBatch<'_>>
        {
            klaus_agent_core::testing::immediate_rollout::prepare_immediate_test_rollout_batch(
                self,
                maximum_records,
            )
        }

        async fn append_message(&self, _message: Message) {}

        async fn append_turn_context(&self, _ctx: TurnContextSnapshot) {}

        async fn append_compaction(&self, _record: CompactionRecord) {}

        async fn append_event(&self, event: RuntimeString, _opts: Option<AppendEventOptions>) {
            if event == klaus_agent_core::memory::maintenance::MEMORY_MAINTENANCE_ROLLOUT_EVENT {
                self.append_panicked.store(true, Ordering::SeqCst);
                self.append_panicked_notify.notify_waiters();
                panic!("injected SDK memory maintenance append panic");
            }
        }

        async fn append_rollback(&self, _record: RollbackRecord) {}

        async fn append_checkpoint(&self, _record: CheckpointRecord) {}

        async fn append_usage(&self, _record: UsageLedgerItem) {}

        async fn append_subagent_lifecycle(&self, _record: SubagentLifecycleRecord) {}

        async fn flush(&self) -> std::io::Result<()> {
            Ok(())
        }

        async fn shutdown(&self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn maintenance_query_error_remains_typed_at_the_sdk_boundary() {
        let core_error = MemoryMaintenanceQueryError::persistence_inconsistent(
            "maintenance state unavailable",
            Some(MemoryMaintenanceRunKind::AutoDream),
            Some("dream-run".into()),
        );
        let sdk_error = SdkError::from(core_error.clone());

        match sdk_error {
            SdkError::MemoryMaintenance(error) => assert_eq!(error, core_error),
            other => panic!("expected typed maintenance error, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn maintenance_query_facade_preserves_a_real_recovery_persistence_failure() {
        let engine = new_engine(bypass(scripted(0)));
        let rollout = Arc::new(PanickingSdkMaintenanceRollout::default());
        let started = AutoMemoryRun {
            run_id: "sdk-recovered-maintenance".into(),
            session_id: "sdk-maintenance-session".into(),
            trigger: MemoryMaintenanceTrigger::Auto,
            scope: AgentMemoryScope::Project,
            target_dir: "/sdk/memory".into(),
            status: MemoryMaintenanceStatus::Started,
            started_at: 1,
            ended_at: None,
            reason: None,
            error: None,
            message_count: 1,
            cursor_message_uuid: None,
            files_touched: Vec::new(),
            turn_count: None,
        };
        let session = engine.shared.agent.start_session(StartSessionOptions {
            id: Some("sdk-maintenance-session".into()),
            permission_mode: Some(ExternalPermissionMode::BypassPermissions),
            rollout: Some(rollout.clone()),
            initial_memory_maintenance: Some(MemoryMaintenanceSnapshot {
                auto_memory_runs: vec![started],
                ..Default::default()
            }),
            ..Default::default()
        });
        let thread = Thread::new(session, ThreadInternals::default());

        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            rollout.wait_until_append_panicked(),
        )
        .await
        .expect("maintenance recovery reached its real rollout boundary");
        let error = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                match thread.memory_maintenance() {
                    Err(error) => break error,
                    Ok(_) => tokio::task::yield_now().await,
                }
            }
        })
        .await
        .expect("SDK facade observes the retained maintenance failure");
        match error {
            SdkError::MemoryMaintenance(error) => {
                assert_eq!(
                    error.detail.code,
                    MemoryMaintenanceQueryErrorCode::PersistenceInconsistent
                );
                assert_eq!(
                    error.detail.run_kind,
                    Some(MemoryMaintenanceRunKind::AutoMemory)
                );
                assert_eq!(
                    error.detail.run_id,
                    Some("sdk-recovered-maintenance".into())
                );
            }
            other => panic!("expected real typed maintenance failure, got {other:?}"),
        }
        thread.dispose().await;
    }

    fn unique_temp_dir(tag: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("ae-sdk-{tag}-{pid}-{n}-{nanos}"));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn provider() -> Arc<dyn ModelProvider> {
        Arc::new(fake_provider())
    }

    struct AttestedSpawnFreeBrowserExecutor;

    // SAFETY: this deterministic test runtime never creates, controls, or
    // delegates to a local process; execute returns before any side effect.
    #[async_trait::async_trait]
    unsafe impl klaus_agent_core::browser::BrowserExecutor for AttestedSpawnFreeBrowserExecutor {
        async fn execute(
            &self,
            _request: klaus_agent_core::browser::BrowserRuntimeRequest,
            _signal: AbortSignal,
            _emit: klaus_agent_core::browser::BrowserEventSink,
        ) -> Result<
            klaus_agent_core::browser::BrowserRuntimeResponse,
            klaus_agent_core::browser::BrowserRuntimeError,
        > {
            Err(klaus_agent_core::browser::BrowserRuntimeError::new(
                klaus_agent_core::browser::BrowserRuntimeErrorCode::Unavailable,
                "spawn-free test runtime",
            ))
        }
    }

    fn spawn_free_test_browser_runtime() -> Arc<dyn BrowserRuntime> {
        // SAFETY: this deterministic executor always returns before any side
        // effect and never creates, controls, or delegates to a local process.
        unsafe {
            klaus_agent_core::browser::attest_spawn_free(Arc::new(AttestedSpawnFreeBrowserExecutor))
        }
    }

    fn context_bound_test_browser_runtime(
        context: &RuntimeProcessContext,
    ) -> Arc<dyn BrowserRuntime> {
        // SAFETY: the deterministic executor performs no process operation at
        // all, so binding it to this namespace is a conservative valid claim.
        unsafe {
            klaus_agent_core::browser::context_bound(
                context.namespace_id(),
                Arc::new(AttestedSpawnFreeBrowserExecutor),
            )
        }
    }

    fn exact_json(source: &str) -> Json {
        runtime_json::parse(&RuntimeString::from(source)).expect("valid exact runtime JSON fixture")
    }

    fn scalar_test_value(value: &RuntimeString, label: &str) -> String {
        value.try_to_utf8().unwrap_or_else(|error| {
            panic!("{label} must be scalar in this host-bound test: {error}")
        })
    }

    fn scalar_test_path(path: &Path, label: &str) -> String {
        path.to_str()
            .unwrap_or_else(|| panic!("{label} must be scalar in this host-bound test"))
            .to_owned()
    }

    fn stdio_mcp(name: &str, command: &str, args: &[&str]) -> McpServerConfig {
        McpServerConfig::Stdio {
            name: name.to_string(),
            command: command.to_string(),
            args: Some(args.iter().map(|arg| arg.to_string()).collect()),
            env: None,
            options: klaus_agent_core::mcp::types::McpServerOptions {
                always_load: Some(true),
                ..Default::default()
            },
        }
    }

    fn mcp_only_plugin(name: &str, server: McpServerConfig) -> LoadedPlugin {
        LoadedPlugin {
            name: name.to_string(),
            version: Some("1.0.0".to_string()),
            display_name: None,
            description: None,
            plugin_namespace: name.to_string(),
            keywords: Vec::new(),
            dependencies: Vec::new(),
            user_config: klaus_agent_core::types::json::JsonObject::new(),
            settings: klaus_agent_core::types::json::JsonObject::new(),
            dir: format!("/plugins/{name}"),
            skills: Vec::new(),
            commands: Vec::new(),
            agents: Vec::new(),
            hooks: Vec::new(),
            output_styles: Vec::new(),
            mcp_servers: vec![server],
            lsp_servers: klaus_agent_core::types::json::JsonObject::new(),
            runtime_capabilities: Vec::new(),
            interface_capabilities: Vec::new(),
            app_connectors: Vec::new(),
        }
    }

    /// The escape hatch that injects a scripted `call_model` (offline, deterministic).
    fn scripted_hook(turns: ScriptedTurns) -> AgentOptionsHook {
        Box::new(move |o: &mut AgentOptions| {
            o.deps.call_model = Some(scripted_call_model(turns));
            o.deps.compactor = Some(noop_compactor());
        })
    }

    /// N single-reply turns: `reply 1`, `reply 2`, …
    fn scripted(turns: usize) -> AgentOptionsHook {
        let list: ScriptedTurns = (0..turns)
            .map(|i| {
                vec![
                    ev::message_start(None),
                    ev::text(&format!("reply {}", i + 1)),
                    ev::stop(None, None),
                ]
            })
            .collect();
        scripted_hook(list)
    }

    fn is_retained_skill_catalog(message: &Message) -> bool {
        matches!(message, Message::User(user)
            if user.is_meta == Some(true)
                && matches!(user.content.as_slice(), [ContentBlock::Text(text)]
                    if text.harness_envelope == Some(HarnessEnvelope::SystemReminder)
                        && text.text.starts_with("<skill-catalog:")))
    }

    fn user_turns(history: &[Message]) -> usize {
        history
            .iter()
            .filter(|message| {
                matches!(message, Message::User(_)) && !is_retained_skill_catalog(message)
            })
            .count()
    }

    fn assert_history_user_texts(history: &[Message], expected: &[&str]) {
        assert_eq!(
            history
                .iter()
                .filter(|message| is_retained_skill_catalog(message))
                .count(),
            1,
            "the trusted skill catalog remains in history exactly once"
        );
        let users = history
            .iter()
            .filter(|message| !is_retained_skill_catalog(message))
            .filter_map(|message| match message {
                Message::User(user) => Some(user),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(users.len(), expected.len());
        for (user, expected_text) in users.iter().zip(expected) {
            assert_eq!(user.is_meta, None);
            let [ContentBlock::Text(text)] = user.content.as_slice() else {
                panic!("each real user turn must retain its exact text block");
            };
            assert_eq!(text.text, RuntimeString::from(*expected_text));
            assert_eq!(text.harness_envelope, None);
            assert!(!user.uuid.is_empty());
        }
        let uuids = users
            .iter()
            .map(|user| &user.uuid)
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(uuids.len(), users.len(), "user turn UUIDs stay unique");
    }

    fn bypass(hook: AgentOptionsHook) -> AgentEngineOptions {
        AgentEngineOptions {
            provider: Some(provider()),
            permission_mode: Some(ExternalPermissionMode::BypassPermissions),
            agent_options: Some(hook),
            ..AgentEngineOptions::new(super::test_process_context())
        }
    }

    /// Points this test process at a private config directory instead of the
    /// developer's real one.
    ///
    /// The engine resolves its config directory on every call, so a test that
    /// never sets `KLAUS_CONFIG_DIR` reads whatever the machine has. With a real
    /// `~/.klaus/config.toml` present, model resolution took its values and
    /// `set_model` looked like it had no effect on the next turn: four cases
    /// failed on that machine and nowhere else. Every test engine is built here,
    /// so isolating once per process removes the dependency by construction.
    ///
    /// Isolation is UNCONDITIONAL except for a directory this helper already
    /// installed. The running desktop app injects
    /// `KLAUS_CONFIG_DIR=<real ~/.klaus>` into every shell it spawns, so inside
    /// the developer's own agent the variable is always set — and an earlier
    /// "only isolate when it is unset" check therefore never isolated, letting
    /// the real profile (with `worktree = true`) leak into the suite and turn
    /// `worktree_shortcut_surfaces_the_enter_worktree_tool_and_config_overrides`
    /// red. The decision is a pure function of the current value so it can be
    /// pinned by a test.
    fn isolated_config_dir_decision(
        current: Option<&std::ffi::OsStr>,
    ) -> Option<std::path::PathBuf> {
        if let Some(value) = current {
            // Already pointed at one of our own per-process temp dirs: keep it.
            if value.to_string_lossy().contains("klaus-sdk-config-") {
                return None;
            }
        }
        Some(std::env::temp_dir().join(format!(
            "klaus-sdk-config-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after the epoch")
                .as_nanos()
        )))
    }

    fn isolate_config_dir() {
        static ISOLATED: std::sync::LazyLock<()> = std::sync::LazyLock::new(|| {
            let current = std::env::var_os("KLAUS_CONFIG_DIR");
            let Some(dir) = isolated_config_dir_decision(current.as_deref()) else {
                return;
            };
            std::fs::create_dir_all(&dir).expect("create isolated SDK config dir");
            klaus_agent_core::runtime_process::set_process_environment_var(
                "KLAUS_CONFIG_DIR",
                &dir,
            );
        });
        std::sync::LazyLock::force(&ISOLATED);
    }

    fn new_engine(options: AgentEngineOptions) -> AgentEngine {
        isolate_config_dir();
        AgentEngine::new(options).expect("SDK test engine must initialize")
    }

    struct SdkSetupHook {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl HookFn for SdkSetupHook {
        async fn run(&self, input: HookInput) -> HookOutcome {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            let additional_context = match input.trigger {
                Some(HookTrigger::Init) => "SDK INIT CONTEXT".into(),
                Some(HookTrigger::Maintenance) => {
                    RuntimeString::from(format!("SDK MAINTENANCE CONTEXT {call}"))
                }
                _ => return HookOutcome::default(),
            };
            HookOutcome {
                result: Some(HookResult::Success),
                additional_context: Some(additional_context),
                ..HookOutcome::default()
            }
        }
    }

    #[tokio::test]
    async fn setup_facade_preserves_init_idempotency_and_repeatable_maintenance_context() {
        let calls = Arc::new(AtomicUsize::new(0));
        let engine = new_engine(AgentEngineOptions {
            provider: Some(provider()),
            hooks: Some(vec![Hook {
                event: HookEvent::Setup,
                matcher: None,
                handler: HookHandler::Fn(Arc::new(SdkSetupHook {
                    calls: calls.clone(),
                })),
                // Setup executes inline even for a normally-background hook.
                is_async: true,
            }]),
            agent_options: Some(scripted(0)),
            ..AgentEngineOptions::new(super::test_process_context())
        });
        let thread = engine.start_thread().await.unwrap();

        let init = thread
            .run_setup(SetupRunRequest {
                request_id: "sdk-init-1".into(),
                trigger: SetupTrigger::Init,
            })
            .await;
        assert_eq!(init.status, SetupRunStatus::Completed);
        assert_eq!(init.additional_context, vec!["SDK INIT CONTEXT"]);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        assert_eq!(
            thread
                .run_setup(SetupRunRequest {
                    request_id: "sdk-init-1".into(),
                    trigger: SetupTrigger::Init,
                })
                .await,
            init,
            "the same correlated init request must be exactly idempotent"
        );
        let cached_init = thread
            .run_setup(SetupRunRequest {
                request_id: "sdk-init-2".into(),
                trigger: SetupTrigger::Init,
            })
            .await;
        assert_eq!(cached_init.status, SetupRunStatus::AlreadyCompleted);
        assert_eq!(cached_init.source_request_id, Some("sdk-init-1".into()));
        assert_eq!(cached_init.additional_context, init.additional_context);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let maintenance_1 = thread
            .run_setup(SetupRunRequest {
                request_id: "sdk-maintenance-1".into(),
                trigger: SetupTrigger::Maintenance,
            })
            .await;
        assert_eq!(maintenance_1.status, SetupRunStatus::Completed);
        assert_eq!(
            maintenance_1.additional_context,
            vec!["SDK MAINTENANCE CONTEXT 2"]
        );
        assert_eq!(
            thread
                .run_setup(SetupRunRequest {
                    request_id: "sdk-maintenance-1".into(),
                    trigger: SetupTrigger::Maintenance,
                })
                .await,
            maintenance_1
        );
        let maintenance_2 = thread
            .run_setup(SetupRunRequest {
                request_id: "sdk-maintenance-2".into(),
                trigger: SetupTrigger::Maintenance,
            })
            .await;
        assert_eq!(maintenance_2.status, SetupRunStatus::Completed);
        assert_eq!(
            maintenance_2.additional_context,
            vec!["SDK MAINTENANCE CONTEXT 3"]
        );
        assert_eq!(calls.load(Ordering::SeqCst), 3);

        let lifecycle = thread.setup_lifecycle();
        assert_eq!(lifecycle.runs.len(), 3);
        assert_eq!(
            lifecycle.completed_init().unwrap().request_id,
            RuntimeString::from("sdk-init-1")
        );
        thread.shutdown().await.unwrap();
    }

    struct TaskListCleanup(PathBuf);

    impl Drop for TaskListCleanup {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).ok();
        }
    }

    #[tokio::test]
    async fn todo_v2_facade_queries_authoritative_state_and_unsubscribes_changes() {
        let engine = new_engine(bypass(scripted(0)));
        let thread = engine.start_thread().await.unwrap();
        let initial = thread
            .get_task_list(TaskListQueryOptions::default())
            .await
            .unwrap();
        assert_eq!(
            &initial.task_list_id,
            thread.id(),
            "an SDK thread must resolve its own authoritative standalone list"
        );
        let task_dir = klaus_agent_core::fs::paths::tasks_dir(&scalar_test_value(
            &initial.task_list_id,
            "SDK TodoV2 task-list id",
        ));
        let _cleanup = TaskListCleanup(PathBuf::from(task_dir));

        let changes = Arc::new(Mutex::new(Vec::<TaskListChange>::new()));
        let observed = changes.clone();
        let unsubscribe = thread.subscribe_task_list_changes(Arc::new(move |change| {
            observed.lock().unwrap().push(change);
        }));

        let first = create_task(
            &initial.task_list_id,
            CreateTaskInput {
                subject: "SDK first task".into(),
                description: "created through the authoritative task store".into(),
                ..CreateTaskInput::default()
            },
        )
        .await
        .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if !changes.lock().unwrap().is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the first TodoV2 callback must be admitted before querying its snapshot");
        let after_first = thread
            .get_task_list(TaskListQueryOptions::default())
            .await
            .unwrap();
        assert_eq!(after_first.revision, initial.revision + 1);
        assert_eq!(after_first.tasks, vec![first.clone()]);
        assert_eq!(changes.lock().unwrap().len(), 1);
        assert_eq!(changes.lock().unwrap()[0].revision, after_first.revision);
        assert_eq!(changes.lock().unwrap()[0].tasks, vec![first]);

        unsubscribe();
        let second = create_task(
            &initial.task_list_id,
            CreateTaskInput {
                subject: "SDK second task".into(),
                description: "must not reach the removed subscriber".into(),
                ..CreateTaskInput::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(
            changes.lock().unwrap().len(),
            1,
            "an explicitly unsubscribed SDK callback must receive no later revision"
        );
        let final_snapshot = thread
            .get_task_list(TaskListQueryOptions::default())
            .await
            .unwrap();
        assert_eq!(final_snapshot.revision, after_first.revision + 1);
        assert_eq!(
            final_snapshot.tasks,
            vec![after_first.tasks[0].clone(), second]
        );
        thread.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn todo_v2_query_does_not_wait_for_an_already_admitted_host_callback() {
        let engine = new_engine(bypass(scripted(0)));
        let thread = engine.start_thread().await.unwrap();
        let initial = thread
            .get_task_list(TaskListQueryOptions::default())
            .await
            .unwrap();
        let task_dir = klaus_agent_core::fs::paths::tasks_dir(&scalar_test_value(
            &initial.task_list_id,
            "SDK blocking TodoV2 callback task-list id",
        ));
        let _cleanup = TaskListCleanup(PathBuf::from(task_dir));

        let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let entered_tx = Arc::new(Mutex::new(Some(entered_tx)));
        let (exited_tx, exited_rx) = tokio::sync::oneshot::channel();
        let exited_tx = Arc::new(Mutex::new(Some(exited_tx)));
        let unsubscribe = thread.subscribe_task_list_changes({
            let gate = gate.clone();
            Arc::new(move |_| {
                if let Some(entered) = entered_tx.lock().unwrap().take() {
                    let _ = entered.send(());
                }
                let (released, ready) = &*gate;
                let mut released = released.lock().unwrap();
                while !*released {
                    released = ready.wait(released).unwrap();
                }
                if let Some(exited) = exited_tx.lock().unwrap().take() {
                    let _ = exited.send(());
                }
            })
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), entered_rx)
            .await
            .expect("TodoV2 callback admission timed out")
            .expect("TodoV2 callback dispatcher exited before admission");

        let snapshot = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            thread.get_task_list(TaskListQueryOptions::default()),
        )
        .await
        .expect("an admitted Host callback must not block an authoritative query")
        .unwrap();
        assert_eq!(snapshot.revision, initial.revision);

        {
            let (released, ready) = &*gate;
            *released.lock().unwrap() = true;
            ready.notify_all();
        }
        tokio::time::timeout(std::time::Duration::from_secs(5), exited_rx)
            .await
            .expect("released TodoV2 callback did not exit")
            .expect("TodoV2 callback dispatcher dropped its completion");
        unsubscribe();
        thread.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn thread_shutdown_is_an_idempotent_authoritative_completion_boundary() {
        let engine = new_engine(bypass(scripted(0)));
        let thread = engine.start_thread().await.unwrap();

        thread.shutdown().await.unwrap();
        // The retained core completion is shared by repeated Host calls; a
        // compatibility dispose after it must also remain harmless.
        thread.shutdown().await.unwrap();
        thread.dispose().await;
    }

    #[tokio::test]
    async fn snapshot_policy_is_cloned_through_start_fork_and_resume_factories() {
        let dir = unique_temp_dir("snapshot-policy-factory");
        let policy = AgentMemorySnapshotDecisionPolicy {
            timeout_ms: 321,
            first_use: AgentMemorySnapshotPolicyAction::RequireHost,
            timeout: AgentMemorySnapshotPolicyAction::Keep,
            disconnect: AgentMemorySnapshotPolicyAction::Merge,
            no_handler: AgentMemorySnapshotPolicyAction::Replace,
        };
        let engine = new_engine(AgentEngineOptions {
            provider: Some(provider()),
            store: Some(StoreOption::Dir(scalar_test_path(
                &dir,
                "snapshot policy test store",
            ))),
            agent_memory_snapshot_policy: Some(policy.clone()),
            agent_options: Some(scripted(0)),
            ..AgentEngineOptions::new(super::test_process_context())
        });

        assert_eq!(
            engine.shared.session_options().agent_memory_snapshot_policy,
            Some(policy.clone())
        );
        let started = engine.start_thread().await.unwrap();
        let started_id = started.id().clone();
        assert_eq!(
            engine.shared.session_options().agent_memory_snapshot_policy,
            Some(policy.clone())
        );
        let forked = engine.fork_thread(&started).await.unwrap();
        assert_eq!(
            engine.shared.session_options().agent_memory_snapshot_policy,
            Some(policy.clone())
        );
        forked.dispose().await;
        started.dispose().await;

        let resumed = engine
            .resume_thread(&scalar_test_value(
                &started_id,
                "snapshot policy resumed thread id",
            ))
            .await
            .unwrap();
        assert_eq!(
            engine.shared.session_options().agent_memory_snapshot_policy,
            Some(policy)
        );
        resumed.dispose().await;
        std::fs::remove_dir_all(dir).ok();
    }

    #[derive(Debug, Default)]
    struct FailingSdkSnapshotRecoveryRollout {
        snapshot_append_pending: AtomicBool,
        flush_entered: AtomicBool,
        flush_entered_notify: tokio::sync::Notify,
        flush_released: AtomicBool,
        flush_release_notify: tokio::sync::Notify,
    }

    impl FailingSdkSnapshotRecoveryRollout {
        async fn wait_until_flush_entered(&self) {
            loop {
                let notified = self.flush_entered_notify.notified();
                if self.flush_entered.load(Ordering::SeqCst) {
                    return;
                }
                notified.await;
            }
        }

        fn release_flush_failure(&self) {
            self.flush_released.store(true, Ordering::SeqCst);
            self.flush_release_notify.notify_waiters();
        }
    }

    #[async_trait::async_trait]
    impl LiveRollout for FailingSdkSnapshotRecoveryRollout {
        async fn prepare_batch(
            &self,
            maximum_records: usize,
        ) -> std::io::Result<klaus_agent_core::store::session_store::PreparedRolloutBatch<'_>>
        {
            klaus_agent_core::testing::immediate_rollout::prepare_immediate_test_rollout_batch(
                self,
                maximum_records,
            )
        }

        async fn append_message(&self, _message: Message) {}

        async fn append_turn_context(&self, _ctx: TurnContextSnapshot) {}

        async fn append_compaction(&self, _record: CompactionRecord) {}

        async fn append_event(&self, event: RuntimeString, _opts: Option<AppendEventOptions>) {
            if event == AGENT_MEMORY_SNAPSHOT_DECISION_ROLLOUT_EVENT {
                self.snapshot_append_pending.store(true, Ordering::SeqCst);
            }
        }

        async fn append_rollback(&self, _record: RollbackRecord) {}

        async fn append_checkpoint(&self, _record: CheckpointRecord) {}

        async fn append_usage(&self, _record: UsageLedgerItem) {}

        async fn append_subagent_lifecycle(&self, _record: SubagentLifecycleRecord) {}

        async fn flush(&self) -> std::io::Result<()> {
            if self.snapshot_append_pending.swap(false, Ordering::SeqCst) {
                self.flush_entered.store(true, Ordering::SeqCst);
                self.flush_entered_notify.notify_waiters();
                loop {
                    let notified = self.flush_release_notify.notified();
                    if self.flush_released.load(Ordering::SeqCst) {
                        break;
                    }
                    notified.await;
                }
                Err(std::io::Error::other(
                    "injected SDK snapshot recovery rollout flush failure",
                ))
            } else {
                Ok(())
            }
        }

        async fn shutdown(&self) -> std::io::Result<()> {
            Ok(())
        }
    }

    async fn sdk_snapshot_recovery_audit(tag: &str) -> (PathBuf, AgentMemorySnapshotDecisionAudit) {
        use klaus_agent_core::memory::agent_memory::{
            default_agent_memory_layout, AgentMemoryLayout,
        };
        use klaus_agent_core::memory::snapshot_decision::prepare_agent_memory_snapshot_request;

        let dir = unique_temp_dir(tag);
        let memory_root = dir.join("memory-fixture");
        let layout = default_agent_memory_layout(
            Some(scalar_test_path(&memory_root, "SDK recovery memory root").as_str()),
            None,
        );
        let agent_type = format!("sdk-recovery-agent-{tag}");
        let snapshot = layout.snapshot_dir_for(&agent_type);
        std::fs::create_dir_all(&snapshot).unwrap();
        std::fs::write(
            Path::new(&snapshot).join("snapshot.json"),
            r#"{"updatedAt":"2026-07-19T00:00:00Z"}"#,
        )
        .unwrap();
        std::fs::write(Path::new(&snapshot).join("remote.md"), "remote\n").unwrap();
        let local = layout.dir_for(&agent_type, AgentMemoryScope::User);
        std::fs::create_dir_all(&local).unwrap();
        std::fs::write(Path::new(&local).join("local.md"), "local\n").unwrap();
        let request = prepare_agent_memory_snapshot_request(&layout, &agent_type, 1, 2)
            .await
            .unwrap()
            .expect("production snapshot preparation creates a recovery request");

        (
            dir,
            AgentMemorySnapshotDecisionAudit {
                request,
                status: AgentMemorySnapshotDecisionStatus::Requested,
                decision: None,
                source: None,
                decided_at: None,
                ended_at: None,
                error: None,
            },
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn snapshot_query_facade_preserves_typed_persistence_errors_and_missing_success() {
        let engine = new_engine(bypass(scripted(0)));
        let clean = Thread::new(
            engine.shared.agent.start_session(StartSessionOptions {
                permission_mode: Some(ExternalPermissionMode::BypassPermissions),
                ..Default::default()
            }),
            ThreadInternals::default(),
        );
        assert_eq!(
            clean
                .agent_memory_snapshot_decision(&RuntimeString::from("missing-snapshot"))
                .await
                .expect("a missing snapshot request remains a successful query"),
            None
        );
        clean.shutdown().await.unwrap();

        for query_all in [false, true] {
            let tag = if query_all {
                "sdk-fatal-query-all"
            } else {
                "sdk-fatal-query-one"
            };
            let (fixture_dir, recovery_audit) = sdk_snapshot_recovery_audit(tag).await;
            let request_id = recovery_audit.request.request_id.clone();
            let failing_rollout = Arc::new(FailingSdkSnapshotRecoveryRollout::default());
            let session = engine.shared.agent.start_session(StartSessionOptions {
                permission_mode: Some(ExternalPermissionMode::BypassPermissions),
                rollout: Some(failing_rollout.clone()),
                initial_agent_memory_snapshot_decisions: Some(vec![recovery_audit]),
                ..Default::default()
            });
            let thread = Arc::new(Thread::new(session, ThreadInternals::default()));
            let query_thread = thread.clone();
            let mut query = if query_all {
                tokio::spawn(async move {
                    query_thread
                        .agent_memory_snapshot_decisions()
                        .await
                        .map(|_| ())
                })
            } else {
                tokio::spawn(async move {
                    query_thread
                        .agent_memory_snapshot_decision(&request_id)
                        .await
                        .map(|_| ())
                })
            };

            tokio::time::timeout(
                std::time::Duration::from_secs(2),
                failing_rollout.wait_until_flush_entered(),
            )
            .await
            .expect("SDK snapshot recovery flush was entered");
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(100), &mut query)
                    .await
                    .is_err(),
                "SDK snapshot query replied before recovery durability resolved"
            );
            failing_rollout.release_flush_failure();
            let error = tokio::time::timeout(std::time::Duration::from_secs(2), query)
                .await
                .expect("SDK snapshot query completed after flush failure")
                .expect("SDK snapshot query task joined")
                .expect_err("SDK snapshot recovery failure must reject the query");
            let detail = match error {
                SdkError::AgentMemorySnapshotDecision(
                    AgentMemorySnapshotApplyError::PersistenceInconsistent(detail),
                ) => detail,
                other => panic!("unexpected SDK snapshot query failure: {other:?}"),
            };
            assert!(
                detail
                    .try_to_utf8()
                    .expect("scalar SDK persistence failure")
                    .contains("injected SDK snapshot recovery rollout flush failure"),
                "typed SDK persistence detail did not preserve the rollout failure: {detail:?}"
            );
            thread.dispose().await;
            std::fs::remove_dir_all(fixture_dir).ok();
        }
    }

    #[tokio::test]
    async fn snapshot_thread_query_and_resolve_preserve_exactly_once_core_error() {
        use klaus_agent_core::memory::agent_memory::{
            default_agent_memory_layout, AgentMemoryLayout,
        };
        use klaus_agent_core::memory::snapshot_decision::prepare_agent_memory_snapshot_request;
        use klaus_agent_core::runtime_values::runtime_codec;
        use klaus_agent_core::store::session_store::AppendEventOptions;

        let dir = unique_temp_dir("snapshot-thread-api");
        let engine = new_engine(AgentEngineOptions {
            provider: Some(provider()),
            store: Some(StoreOption::Dir(scalar_test_path(
                &dir,
                "snapshot API test store",
            ))),
            agent_options: Some(scripted(0)),
            ..AgentEngineOptions::new(super::test_process_context())
        });
        let thread = engine.start_thread().await.unwrap();
        let thread_id = thread.id().clone();
        thread.dispose().await;

        let memory_root = dir.join("memory-fixture");
        let layout = default_agent_memory_layout(
            Some(scalar_test_path(&memory_root, "snapshot API memory root").as_str()),
            None,
        );
        let snapshot = layout.snapshot_dir_for("writer");
        std::fs::create_dir_all(&snapshot).unwrap();
        std::fs::write(
            Path::new(&snapshot).join("snapshot.json"),
            r#"{"updatedAt":"2026-07-18T00:00:00Z"}"#,
        )
        .unwrap();
        std::fs::write(Path::new(&snapshot).join("remote.md"), "remote\n").unwrap();
        let local = layout.dir_for("writer", AgentMemoryScope::User);
        std::fs::create_dir_all(&local).unwrap();
        std::fs::write(Path::new(&local).join("local.md"), "local\n").unwrap();
        let request = prepare_agent_memory_snapshot_request(&layout, "writer", 10, 20)
            .await
            .unwrap()
            .unwrap();
        let request_id = request.request_id.clone();
        let applied = AgentMemorySnapshotDecisionAudit {
            request,
            status: AgentMemorySnapshotDecisionStatus::Applied,
            decision: Some(AgentMemoryMergeChoice::Keep),
            source: Some(AgentMemorySnapshotDecisionSource::Host),
            decided_at: Some(12),
            ended_at: Some(13),
            error: None,
        };
        let scalar_thread_id = scalar_test_value(&thread_id, "snapshot API thread id");
        let rollout = engine
            .shared
            .store
            .as_ref()
            .unwrap()
            .reopen(&scalar_thread_id)
            .await
            .unwrap();
        rollout
            .append_event(
                AGENT_MEMORY_SNAPSHOT_DECISION_ROLLOUT_EVENT.into(),
                Some(AppendEventOptions {
                    turn_id: None,
                    payload: Some(runtime_codec::encode_value(&applied).unwrap()),
                }),
            )
            .await;
        rollout.shutdown().await.unwrap();

        let resumed = engine.resume_thread(&scalar_thread_id).await.unwrap();
        let mut events = resumed.subscribe_agent_memory_snapshot_decisions();
        assert_eq!(
            resumed.agent_memory_snapshot_decisions().await.unwrap(),
            vec![applied.clone()]
        );
        assert_eq!(
            resumed
                .agent_memory_snapshot_decision(&request_id)
                .await
                .unwrap(),
            Some(applied.clone())
        );
        assert_eq!(
            resumed
                .resolve_agent_memory_snapshot_decision(&request_id, AgentMemoryMergeChoice::Keep,)
                .await
                .unwrap(),
            applied
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), events.recv())
                .await
                .is_err(),
            "an idempotent resolve must not publish a duplicate audit transition"
        );
        assert!(matches!(
            resumed
                .resolve_agent_memory_snapshot_decision(
                    &request_id,
                    AgentMemoryMergeChoice::Replace,
                )
                .await,
            Err(SdkError::AgentMemorySnapshotDecision(
                AgentMemorySnapshotApplyError::Registry(
                    AgentMemorySnapshotRegistryError::DecisionConflict
                )
            ))
        ));
        assert_eq!(
            resumed
                .agent_memory_snapshot_decision(&request_id)
                .await
                .unwrap(),
            Some(applied)
        );
        resumed.dispose().await;
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn rejects_browser_runtime_bound_to_a_different_process_namespace() {
        let engine_context = super::test_new_process_context();
        let expected = engine_context.namespace_id().get();
        let browser_context = super::test_new_process_context();
        let actual = browser_context.namespace_id().get();
        assert_ne!(expected, actual);

        let result = AgentEngine::new(AgentEngineOptions {
            local_browser_runtime: Some(
                PlaywrightBrowserRuntime::with_defaults(browser_context)
                    .into_local_browser_runtime(),
            ),
            ..AgentEngineOptions::new(engine_context)
        });

        match result {
            Err(SdkError::ProcessContextMismatch {
                expected: rejected_expected,
                actual: rejected_actual,
            }) => {
                assert_eq!(rejected_expected, expected);
                assert_eq!(rejected_actual, actual);
            }
            Err(error) => panic!("unexpected SDK construction error: {error}"),
            Ok(_) => panic!("mismatched browser process context must be rejected"),
        }
    }

    #[test]
    fn rejects_local_browser_capability_root_bound_to_a_different_process_namespace() {
        let engine_context = super::test_new_process_context();
        let expected = engine_context.namespace_id().get();
        let browser_context = super::test_new_process_context();
        let actual = browser_context.namespace_id().get();
        assert_ne!(expected, actual);

        let local_root = PlaywrightBrowserRuntime::with_defaults(browser_context)
            .into_local_browser_runtime()
            .into_capability_root(
                klaus_agent_core::capabilities::RuntimeCapabilitySource::host("local-browser"),
            );
        let result = AgentEngine::new(AgentEngineOptions {
            local_browser_capability_roots: Some(vec![local_root]),
            ..AgentEngineOptions::new(engine_context)
        });

        match result {
            Err(SdkError::ProcessContextMismatch {
                expected: rejected_expected,
                actual: rejected_actual,
            }) => {
                assert_eq!(rejected_expected, expected);
                assert_eq!(rejected_actual, actual);
            }
            Err(error) => panic!("unexpected SDK construction error: {error}"),
            Ok(_) => panic!("mismatched local browser capability root must be rejected"),
        }
    }

    #[test]
    fn rejects_agent_options_hook_that_replaces_the_process_namespace() {
        let engine_context = super::test_new_process_context();
        let expected = engine_context.namespace_id().get();
        let replacement_context = super::test_new_process_context();
        let actual = replacement_context.namespace_id().get();

        let result = AgentEngine::new(AgentEngineOptions {
            agent_options: Some(Box::new(move |options| {
                options.process_context = replacement_context;
            })),
            ..AgentEngineOptions::new(engine_context)
        });

        match result {
            Err(SdkError::ProcessContextMismatch {
                expected: rejected_expected,
                actual: rejected_actual,
            }) => {
                assert_eq!(rejected_expected, expected);
                assert_eq!(rejected_actual, actual);
            }
            Err(error) => panic!("unexpected SDK construction error: {error}"),
            Ok(_) => panic!("agent options hook must not replace the process context"),
        }
    }

    #[test]
    fn rejects_hook_context_bound_browser_runtime_from_a_different_namespace() {
        let engine_context = super::test_new_process_context();
        let expected = engine_context.namespace_id().get();
        let browser_context = super::test_new_process_context();
        let actual = browser_context.namespace_id().get();
        assert_ne!(expected, actual);
        let hook_runtime = context_bound_test_browser_runtime(&browser_context);

        let result = AgentEngine::new(AgentEngineOptions {
            agent_options: Some(Box::new(move |options| {
                options.browser_runtime = Some(hook_runtime);
            })),
            ..AgentEngineOptions::new(engine_context)
        });

        match result {
            Err(SdkError::ProcessContextMismatch {
                expected: rejected_expected,
                actual: rejected_actual,
            }) => {
                assert_eq!(rejected_expected, expected);
                assert_eq!(rejected_actual, actual);
            }
            Err(error) => panic!("unexpected SDK construction error: {error}"),
            Ok(_) => panic!("a hook runtime from another namespace must be rejected"),
        }
    }

    #[test]
    fn rejects_hook_capability_root_from_a_different_process_namespace() {
        let engine_context = super::test_new_process_context();
        let expected = engine_context.namespace_id().get();
        let browser_context = super::test_new_process_context();
        let actual = browser_context.namespace_id().get();
        assert_ne!(expected, actual);
        let hook_runtime = context_bound_test_browser_runtime(&browser_context);

        let result = AgentEngine::new(AgentEngineOptions {
            agent_options: Some(Box::new(move |options| {
                options.capability_roots.push(
                    RuntimeCapabilityRoot::host("hook-browser").with_browser_runtime(hook_runtime),
                );
            })),
            ..AgentEngineOptions::new(engine_context)
        });

        match result {
            Err(SdkError::ProcessContextMismatch {
                expected: rejected_expected,
                actual: rejected_actual,
            }) => {
                assert_eq!(rejected_expected, expected);
                assert_eq!(rejected_actual, actual);
            }
            Err(error) => panic!("unexpected SDK construction error: {error}"),
            Ok(_) => panic!("a hook capability root from another namespace must be rejected"),
        }
    }

    #[test]
    fn accepts_an_explicitly_attested_spawn_free_browser_runtime() {
        let result = AgentEngine::new(AgentEngineOptions {
            browser_runtime: Some(spawn_free_test_browser_runtime()),
            ..AgentEngineOptions::new(super::test_new_process_context())
        });

        assert!(result.is_ok());
    }

    #[test]
    fn accepts_a_sealed_local_browser_runtime_for_the_engine_namespace() {
        let engine_context = super::test_new_process_context();
        let local_runtime = PlaywrightBrowserRuntime::with_defaults(engine_context.clone())
            .into_local_browser_runtime();

        let result = AgentEngine::new(AgentEngineOptions {
            local_browser_runtime: Some(local_runtime),
            ..AgentEngineOptions::new(engine_context)
        });

        assert!(result.is_ok());
    }

    #[test]
    fn accepts_a_sealed_local_browser_capability_root_for_the_engine_namespace() {
        let engine_context = super::test_new_process_context();
        let local_root = PlaywrightBrowserRuntime::with_defaults(engine_context.clone())
            .into_local_browser_runtime()
            .into_capability_root(
                klaus_agent_core::capabilities::RuntimeCapabilitySource::host("local-browser"),
            );

        let result = AgentEngine::new(AgentEngineOptions {
            local_browser_capability_roots: Some(vec![local_root]),
            ..AgentEngineOptions::new(engine_context)
        });

        assert!(result.is_ok());
    }

    #[test]
    fn rejects_hook_runtime_that_would_hide_a_sealed_local_runtime() {
        let engine_context = super::test_new_process_context();
        let local_browser_runtime = PlaywrightBrowserRuntime::with_defaults(engine_context.clone())
            .into_local_browser_runtime();
        let hook_runtime = spawn_free_test_browser_runtime();

        let result = AgentEngine::new(AgentEngineOptions {
            local_browser_runtime: Some(local_browser_runtime),
            agent_options: Some(Box::new(move |options| {
                options.browser_runtime = Some(hook_runtime);
            })),
            ..AgentEngineOptions::new(engine_context)
        });

        match result {
            Err(SdkError::BrowserRuntimeConflict) => {}
            Err(error) => panic!("unexpected SDK construction error: {error}"),
            Ok(_) => panic!("a hook runtime must not hide a sealed local runtime"),
        }
    }

    #[test]
    fn rejects_hook_root_that_would_replace_a_sealed_local_capability_root() {
        let engine_context = super::test_new_process_context();
        let local_root = PlaywrightBrowserRuntime::with_defaults(engine_context.clone())
            .into_local_browser_runtime()
            .into_capability_root(
                klaus_agent_core::capabilities::RuntimeCapabilitySource::host("local-browser"),
            );
        let hook_runtime = spawn_free_test_browser_runtime();

        let result = AgentEngine::new(AgentEngineOptions {
            local_browser_capability_roots: Some(vec![local_root]),
            agent_options: Some(Box::new(move |options| {
                options.capability_roots.push(
                    RuntimeCapabilityRoot::host("local-browser").with_browser_runtime(hook_runtime),
                );
            })),
            ..AgentEngineOptions::new(engine_context)
        });

        match result {
            Err(SdkError::BrowserCapabilityRootConflict) => {}
            Err(error) => panic!("unexpected SDK construction error: {error}"),
            Ok(_) => panic!("a hook root must not replace a sealed local capability root"),
        }
    }

    #[tokio::test]
    async fn sdk_manages_installed_plugin_lifecycle() {
        let home = unique_temp_dir("plugin-home");
        let plugin_dir = home.join("docs");
        std::fs::create_dir_all(&plugin_dir).unwrap();
        std::fs::write(plugin_dir.join("plugin.json"), "{}").unwrap();
        let mut state = InstalledPlugins::default();
        state.plugins.insert(
            "docs".into(),
            InstalledPluginRecord {
                name: "docs".into(),
                plugin_id: None,
                marketplace: None,
                version: Some("1.0.0".into()),
                install_path: scalar_test_path(&plugin_dir, "test plugin path").into(),
                data_path: None,
                source: "catalog:docs".into(),
                scope: None,
                dependencies: Vec::new(),
                installed_at_ms: 1,
                enabled: true,
            },
        );
        write_installed_plugins(&home, &state).await.unwrap();

        let engine = new_engine(AgentEngineOptions {
            provider: Some(provider()),
            plugin_home: Some(scalar_test_path(&home, "test plugin home")),
            ..AgentEngineOptions::new(super::test_process_context())
        });

        assert_eq!(engine.installed_plugins().await.unwrap().plugins.len(), 1);
        let disabled = engine
            .set_installed_plugin_enabled("docs", false)
            .await
            .unwrap()
            .expect("record");
        assert!(!disabled.enabled);
        let removed = engine
            .uninstall_plugin("docs", true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(removed.name, "docs");
        assert!(!plugin_dir.exists());
        assert!(engine.installed_plugins().await.unwrap().plugins.is_empty());
    }

    #[tokio::test]
    async fn sdk_async_constructor_loads_enabled_installed_plugins() {
        let home = unique_temp_dir("plugin-home-load");
        let plugin_dir = home.join("docs");
        std::fs::create_dir_all(&plugin_dir).unwrap();
        std::fs::write(plugin_dir.join("plugin.json"), r#"{"name":"docs"}"#).unwrap();
        let mut state = InstalledPlugins::default();
        state.plugins.insert(
            "docs".into(),
            InstalledPluginRecord {
                name: "docs".into(),
                plugin_id: None,
                marketplace: None,
                version: Some("1.0.0".into()),
                install_path: scalar_test_path(&plugin_dir, "test plugin path").into(),
                data_path: None,
                source: "catalog:docs".into(),
                scope: None,
                dependencies: Vec::new(),
                installed_at_ms: 1,
                enabled: true,
            },
        );
        write_installed_plugins(&home, &state).await.unwrap();

        let engine = AgentEngine::new_from_installed_plugins(AgentEngineOptions {
            provider: Some(provider()),
            plugin_home: Some(scalar_test_path(&home, "test plugin home")),
            ..AgentEngineOptions::new(super::test_process_context())
        })
        .await
        .unwrap();

        let snapshot = engine.plugin_runtime_snapshot();
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.entries[0].name, "docs");
    }

    #[test]
    fn sdk_exposes_plugin_runtime_snapshot_with_mcp_dedup_warnings() {
        let engine = new_engine(AgentEngineOptions {
            provider: Some(provider()),
            plugins: Some(vec![mcp_only_plugin(
                "plugin-docs",
                stdio_mcp("plugin_docs", "docs-mcp", &["serve"]),
            )]),
            mcp_server_configs: Some(vec![stdio_mcp("manual_docs", "docs-mcp", &["serve"])]),
            ..AgentEngineOptions::new(super::test_process_context())
        });

        let snapshot = engine.plugin_runtime_snapshot();
        assert_eq!(snapshot.entries.len(), 1);
        assert!(snapshot.entries[0].permission_scope.mcp_servers.is_empty());
        assert_eq!(snapshot.warnings.len(), 1);
        assert_eq!(
            snapshot.warnings[0].kind,
            PluginLoadIssueKind::DuplicateMcpServerSuppressed
        );
    }

    /// A minimal closure-backed tool for exercising the `tools` field.
    struct TestTool {
        name: RuntimeString,
        schema: Json,
        run: Box<dyn Fn(Json) -> ToolResult + Send + Sync>,
    }

    #[async_trait]
    impl Tool for TestTool {
        fn name(&self) -> RuntimeString {
            self.name.clone()
        }
        fn description(&self) -> RuntimeString {
            "test tool".into()
        }
        fn input_schema(&self) -> Json {
            self.schema.clone()
        }
        fn is_enabled(&self) -> bool {
            true
        }
        fn is_concurrency_safe(&self, _input: &Json) -> bool {
            true
        }
        fn is_read_only(&self, _input: &Json) -> bool {
            true
        }
        async fn call(
            &self,
            input: Json,
            _ctx: &ToolContext,
            _on_progress: Option<ToolProgress>,
        ) -> Result<ToolResult, ToolError> {
            Ok((self.run)(input))
        }
    }

    fn object_schema() -> JsonSchema {
        serde_json::from_value(json!({ "type": "object" })).unwrap()
    }

    #[tokio::test]
    async fn runs_a_turn_in_process_and_returns_the_final_text() {
        let engine = new_engine(bypass(scripted_hook(vec![vec![
            ev::message_start(None),
            ev::text("hello from the sdk"),
            ev::stop(None, None),
        ]])));
        let thread = engine.start_thread().await.unwrap();
        let result = thread
            .run(Input::Text("hi".into()), TurnOptions::default())
            .await;
        assert_eq!(result.final_text, "hello from the sdk");
        thread.dispose().await;
    }

    #[tokio::test]
    async fn run_returns_lone_surrogate_final_text_without_host_projection() {
        let exact = RuntimeString::from_utf16([0xd800]);
        let engine = new_engine(bypass(scripted_hook(vec![vec![
            ev::message_start(None),
            ModelStreamEvent::TextDelta {
                text: exact.clone(),
            },
            ev::stop(None, None),
        ]])));
        let thread = engine.start_thread().await.unwrap();

        let result = thread
            .run(Input::Text("hi".into()), TurnOptions::default())
            .await;

        assert_eq!(result.final_text, exact);
        assert_ne!(result.final_text, RuntimeString::from("\u{fffd}"));
        thread.dispose().await;
    }

    #[tokio::test]
    async fn readonly_helpers_accept_independent_pre_cancelled_signals() {
        let engine = new_engine(bypass(scripted(1)));
        let thread = engine.start_thread().await.unwrap();

        let side_signal = AbortSignal::new();
        side_signal.cancel();
        let side = thread
            .side_question_with_signal("where are we?".into(), side_signal)
            .await;
        assert!(matches!(
            side,
            Err(SdkError::Session(message)) if message == "side question cancelled"
        ));

        let title_signal = AbortSignal::new();
        title_signal.cancel();
        let title = thread
            .generate_title_with_signal(None, None, false, title_signal)
            .await;
        assert!(matches!(
            title,
            Err(SdkError::Session(message)) if message == "title generation cancelled"
        ));
        thread.dispose().await;
    }

    #[tokio::test]
    async fn streams_events_runs_multiple_turns_and_rewinds_conversation() {
        let engine = new_engine(bypass(scripted(2)));
        let thread = engine.start_thread().await.unwrap();

        let mut streamed = RuntimeString::default();
        let mut s = thread.run_streamed(Input::Text("first".into()), TurnOptions::default());
        while let Some(e) = s.next().await {
            if let EngineEvent::TextDelta { text, .. } = e {
                streamed = streamed.concat(&text);
            }
        }
        drop(s);
        thread
            .run(Input::Text("second".into()), TurnOptions::default())
            .await;

        assert!(streamed.contains("reply 1"));
        assert_history_user_texts(&thread.history(), &["first", "second"]);

        let out = thread.rewind(1.0, Some(RewindScope::Conversation)).await;
        assert_eq!(out.scope, RewindScope::Conversation);
        assert_eq!(out.dropped_turns, 1);
        assert_eq!(out.remaining_turns, 1);
        assert_history_user_texts(&thread.history(), &["first"]);
        thread.dispose().await;
    }

    #[tokio::test]
    async fn persists_a_thread_and_resumes_it_by_id() {
        let dir = unique_temp_dir("resume");
        let engine = new_engine(AgentEngineOptions {
            provider: Some(provider()),
            permission_mode: Some(ExternalPermissionMode::BypassPermissions),
            store: Some(StoreOption::Dir(scalar_test_path(
                &dir,
                "test resume store path",
            ))),
            agent_options: Some(scripted(1)),
            ..AgentEngineOptions::new(super::test_process_context())
        });
        let thread = engine.start_thread().await.unwrap();
        let id = thread.id().clone();
        thread
            .run(Input::Text("remember this".into()), TurnOptions::default())
            .await;
        thread.dispose().await;

        let resumed = engine
            .resume_thread(&scalar_test_value(&id, "persisted thread id"))
            .await
            .unwrap();
        assert_eq!(resumed.id(), &id);
        assert!(user_turns(&resumed.history()) >= 1);
        resumed.dispose().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn resume_thread_without_a_store_errors() {
        let engine = new_engine(AgentEngineOptions {
            provider: Some(provider()),
            agent_options: Some(scripted(0)),
            ..AgentEngineOptions::new(super::test_process_context())
        });
        match engine.resume_thread("whatever").await {
            Err(err) => {
                assert!(matches!(err, SdkError::ResumeRequiresStore));
                assert!(err.to_string().to_lowercase().contains("store"));
            }
            Ok(_) => panic!("expected resume_thread without a store to error"),
        }
    }

    #[tokio::test]
    async fn fork_thread_by_id_without_a_store_errors() {
        let engine = new_engine(AgentEngineOptions {
            provider: Some(provider()),
            agent_options: Some(scripted(0)),
            ..AgentEngineOptions::new(super::test_process_context())
        });
        match engine.fork_thread_by_id("whatever").await {
            Err(err) => {
                assert!(matches!(err, SdkError::ForkByIdRequiresStore));
                assert!(err.to_string().to_lowercase().contains("store"));
            }
            Ok(_) => panic!("expected fork_thread_by_id without a store to error"),
        }
    }

    #[tokio::test]
    async fn lists_persisted_threads() {
        let dir = unique_temp_dir("list");
        let engine = new_engine(AgentEngineOptions {
            provider: Some(provider()),
            permission_mode: Some(ExternalPermissionMode::BypassPermissions),
            store: Some(StoreOption::Dir(scalar_test_path(
                &dir,
                "test list store path",
            ))),
            agent_options: Some(scripted(2)),
            ..AgentEngineOptions::new(super::test_process_context())
        });
        let t1 = engine.start_thread().await.unwrap();
        t1.run(Input::Text("one".into()), TurnOptions::default())
            .await;
        let id1 = t1.id().clone();
        t1.dispose().await;
        let t2 = engine.start_thread().await.unwrap();
        t2.run(Input::Text("two".into()), TurnOptions::default())
            .await;
        let id2 = t2.id().clone();
        t2.dispose().await;

        let page = engine.list_threads(ListOptions::default()).await.unwrap();
        assert!(page.items.iter().any(|item| item.id == id1));
        assert!(page.items.iter().any(|item| item.id == id2));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn sdk_readonly_side_question_input_keeps_persisted_history() {
        use klaus_agent_core::engine::session::UserContentPart;
        let dir = unique_temp_dir("readonly-parts");
        let engine = new_engine(AgentEngineOptions {
            provider: Some(provider()),
            permission_mode: Some(ExternalPermissionMode::BypassPermissions),
            store: Some(StoreOption::Dir(scalar_test_path(&dir, "readonly store"))),
            agent_options: Some(scripted_hook(vec![
                vec![
                    ev::message_start(None),
                    ev::text("parent answer"),
                    ev::stop(None, None),
                ],
                vec![
                    ev::message_start(None),
                    ev::text("attachment answer"),
                    ev::stop(None, None),
                ],
            ])),
            ..AgentEngineOptions::new(super::test_process_context())
        });
        let thread = engine.start_thread().await.unwrap();
        thread
            .run(Input::Text("parent".into()), TurnOptions::default())
            .await;
        let before = thread.history();
        let id = scalar_test_value(thread.id(), "readonly thread id");
        let result = thread
            .side_question_input(Input::Parts(vec![
                UserContentPart::Text {
                    text: "extracted document text".into(),
                },
                UserContentPart::Image {
                    base64: "iVBORw0KGgo=".into(),
                    mime_type: "image/png".into(),
                    source_path: None,
                },
            ]))
            .await
            .unwrap();
        assert_eq!(result.response, "attachment answer");
        assert_eq!(thread.history(), before);
        assert!(thread
            .side_question_input(Input::Parts(vec![]))
            .await
            .is_err());
        thread.dispose().await;
        let resumed = engine.resume_thread(&id).await.unwrap();
        assert_eq!(
            resumed.history(),
            before,
            "neither side input nor response was persisted"
        );
        resumed.dispose().await;
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn sdk_manages_persisted_thread_metadata_and_lifecycle() {
        let dir = unique_temp_dir("thread-lifecycle");
        let engine = new_engine(AgentEngineOptions {
            provider: Some(provider()),
            permission_mode: Some(ExternalPermissionMode::BypassPermissions),
            store: Some(StoreOption::Dir(scalar_test_path(
                &dir,
                "test lifecycle store path",
            ))),
            agent_options: Some(scripted_hook(vec![
                vec![
                    ev::message_start(None),
                    ev::text("reply 1"),
                    ev::stop(None, None),
                ],
                vec![
                    ev::message_start(None),
                    ev::text("side answer"),
                    ev::stop(None, None),
                ],
                vec![
                    ev::message_start(None),
                    ev::text(r#"{"title":"SDK lifecycle"}"#),
                    ev::stop(None, None),
                ],
                vec![
                    ev::message_start(None),
                    ev::text("dream done"),
                    ev::stop(None, None),
                ],
            ])),
            ..AgentEngineOptions::new(super::test_process_context())
        });
        let thread = engine.start_thread().await.unwrap();
        thread
            .run(Input::Text("one".into()), TurnOptions::default())
            .await;
        let id = thread.id().clone();
        let snapshot = thread.context_snapshot();
        assert_eq!(snapshot.thread_id, id);
        assert!(snapshot.context_window > 0);
        let compact = thread
            .compact(Some(RuntimeString::from_utf16(vec![
                0x0070, 0xd800, 0x0072, 0xdc00,
            ])))
            .await
            .unwrap();
        assert!(!compact.compacted);
        let history_len = thread.history().len();
        let side = thread.side_question("where are we?".into()).await.unwrap();
        assert_eq!(side.response, "side answer");
        assert!(!side.synthetic);
        let generated = thread.generate_title(None, None, true).await.unwrap();
        assert_eq!(
            generated.title.as_ref(),
            Some(&RuntimeString::from("SDK lifecycle"))
        );
        assert!(generated.persisted);
        assert_eq!(thread.history().len(), history_len);
        let dream = thread.dream_now().await.unwrap();
        assert!(!dream.ok);
        assert_eq!(dream.reason, Some(DreamNowReason::GateClosed));
        assert!(dream.done.is_none());
        thread.dispose().await;

        let generated_page = engine.list_threads(ListOptions::default()).await.unwrap();
        let generated_summary = generated_page
            .items
            .iter()
            .find(|item| item.id == id)
            .unwrap();
        assert_eq!(
            generated_summary.title.as_ref(),
            Some(&RuntimeString::from("SDK lifecycle"))
        );

        let scalar_id = scalar_test_value(&id, "persisted thread id");
        engine
            .update_thread_title(&scalar_id, "SDK title")
            .await
            .unwrap();
        let page = engine.list_threads(ListOptions::default()).await.unwrap();
        let summary = page.items.iter().find(|item| item.id == id).unwrap();
        assert_eq!(
            summary.title.as_ref(),
            Some(&RuntimeString::from("SDK title"))
        );

        let fork = engine.fork_thread_by_id(&scalar_id).await.unwrap();
        assert_ne!(fork.id(), &id);
        assert_eq!(fork.forked_from_id(), Some(&id));
        assert!(user_turns(&fork.history()) >= 1);
        fork.dispose().await;

        engine.archive_thread(&scalar_id).await.unwrap();
        let with_archived = engine
            .list_threads(ListOptions {
                include_archived: Some(true),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(with_archived
            .items
            .iter()
            .any(|item| item.id == id && item.archived == Some(true)));

        let restored = engine.unarchive_thread(&scalar_id).await.unwrap();
        assert_eq!(restored.status, UnarchiveSessionStatus::Restored);
        assert_eq!(restored.thread_id, id);
        let repeated = engine.unarchive_thread(&scalar_id).await.unwrap();
        assert_eq!(repeated.status, UnarchiveSessionStatus::AlreadyActive);
        let active = engine.list_threads(ListOptions::default()).await.unwrap();
        assert_eq!(active.items.iter().filter(|item| item.id == id).count(), 1);
        assert!(active
            .items
            .iter()
            .any(|item| item.id == id && item.archived == Some(false)));

        engine.delete_thread(&scalar_id).await.unwrap();
        let after_delete = engine
            .list_threads(ListOptions {
                include_archived: Some(true),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(!after_delete.items.iter().any(|item| item.id == id));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn serializes_overlapping_run_calls() {
        let engine = new_engine(bypass(scripted(2)));
        let thread = engine.start_thread().await.unwrap();

        // Fire two turns WITHOUT awaiting the first — they must queue, not overlap.
        let (a, b) = futures::future::join(
            thread.run(Input::Text("first".into()), TurnOptions::default()),
            thread.run(Input::Text("second".into()), TurnOptions::default()),
        )
        .await;

        assert_eq!(a.final_text, "reply 1");
        assert_eq!(b.final_text, "reply 2");
        assert_history_user_texts(&thread.history(), &["first", "second"]);
        thread.dispose().await;
    }

    #[tokio::test]
    async fn queues_a_steering_message_and_drains_it() {
        let engine = new_engine(bypass(scripted(1)));
        let thread = engine.start_thread().await.unwrap();

        thread
            .send(Input::Text("steer me".into()), Some(QueuePriority::Now))
            .expect("valid steering input");
        let queued = thread.queued();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].priority, Some(QueuePriority::Now));
        assert_eq!(queued[0].session_id.as_ref(), Some(thread.id()));
        let snapshot = thread.queue_snapshot();
        assert_eq!(snapshot.status, QueueSnapshotStatus::Authoritative);
        assert_eq!(snapshot.error, None);
        let uuid = snapshot.items[0].message.uuid.clone().unwrap();
        let revisions = Arc::new(Mutex::new(Vec::new()));
        let observed = revisions.clone();
        let _unsubscribe = thread.subscribe_queue_changes(Arc::new(move |snapshot| {
            observed.lock().unwrap().push(snapshot.revision);
        }));
        let edited = thread
            .edit_queued(
                "sdk-queue-edit".into(),
                uuid.clone(),
                snapshot.revision,
                QueueEdit {
                    value: Some(QueuedValue::Text("edited steer".into())),
                    ..Default::default()
                },
            )
            .await;
        assert_eq!(edited.status, QueueMutationStatus::Applied);
        assert_eq!(edited.snapshot.revision, snapshot.revision + 1);
        assert_eq!(edited.snapshot.status, QueueSnapshotStatus::Authoritative);

        let taken = thread.take_pending().await.expect("durable take pending");
        assert_eq!(taken.len(), 1);
        assert_eq!(taken[0].value, QueuedValue::Text("edited steer".into()));
        assert_eq!(thread.queued().len(), 0);
        assert_eq!(
            revisions.lock().unwrap().as_slice(),
            &[snapshot.revision + 1, snapshot.revision + 2]
        );
        let missing = thread
            .remove_queued("sdk-remove-after-take".into(), uuid, snapshot.revision + 2)
            .await;
        assert_eq!(missing.status, QueueMutationStatus::Missing);
        assert_eq!(missing.terminal_state, Some(QueueTerminalState::Taken));
        thread.dispose().await;
    }

    #[tokio::test]
    async fn rejects_invalid_pdf_steering_before_it_reaches_the_queue() {
        let engine = new_engine(bypass(scripted(1)));
        let thread = engine.start_thread().await.unwrap();

        let result: Result<(), InputValidationError> = thread.send(
            Input::Parts(vec![UserContentPart::Document {
                base64: "%%%not-base64%%%".into(),
                mime_type: "application/pdf".into(),
                filename: Some("bad.pdf".into()),
            }]),
            None,
        );
        let error = result.expect_err("invalid PDF must not be queued");

        assert_eq!(
            error.detail.code,
            InputValidationErrorCode::InvalidDocumentBase64
        );
        assert!(thread.queued().is_empty());
        thread.dispose().await;
    }

    #[tokio::test]
    async fn parses_output_schema_into_structured_output() {
        let engine = new_engine(bypass(scripted_hook(vec![vec![
            ev::message_start(None),
            ev::text("{\"answer\":42}"),
            ev::stop(None, None),
        ]])));
        let thread = engine.start_thread().await.unwrap();
        let result = thread
            .run(
                Input::Text("give me json".into()),
                TurnOptions {
                    output_schema: Some(object_schema()),
                    ..Default::default()
                },
            )
            .await;
        assert_eq!(
            result.structured_output,
            Some(exact_json(r#"{"answer":42}"#))
        );
        thread.dispose().await;
    }

    #[tokio::test]
    async fn set_model_switches_the_model_on_the_next_turn() {
        let seen: Arc<Mutex<Vec<RuntimeString>>> = Arc::new(Mutex::new(Vec::new()));
        let seen2 = seen.clone();
        let hook: AgentOptionsHook = Box::new(move |o: &mut AgentOptions| {
            let inner = Arc::new(scripted_call_model(vec![
                vec![
                    ev::message_start(None),
                    ev::text("ok"),
                    ev::stop(None, None),
                ],
                vec![
                    ev::message_start(None),
                    ev::text("ok"),
                    ev::stop(None, None),
                ],
            ]));
            let seen2 = seen2.clone();
            let cm: CallModel = Box::new(
                move |req: ModelRequest, signal: AbortSignal| -> ModelStream {
                    seen2.lock().unwrap().push(req.model.clone());
                    inner(req, signal)
                },
            );
            o.deps.call_model = Some(cm);
        });
        let engine = new_engine(bypass(hook));
        let thread = engine.start_thread().await.unwrap();
        thread
            .run(Input::Text("first".into()), TurnOptions::default())
            .await;
        thread.set_model("claude-sonnet-4-6".into()).unwrap();
        thread
            .run(Input::Text("second".into()), TurnOptions::default())
            .await;

        {
            let seen = seen.lock().unwrap();
            assert_eq!(seen[1], "claude-sonnet-4-6");
            assert_ne!(seen[0], "claude-sonnet-4-6");
        }
        thread.dispose().await;
    }

    #[tokio::test]
    async fn forks_a_thread_and_the_branch_diverges() {
        let engine = new_engine(bypass(scripted(3)));
        let t1 = engine.start_thread().await.unwrap();
        t1.run(Input::Text("a".into()), TurnOptions::default())
            .await;
        t1.run(Input::Text("b".into()), TurnOptions::default())
            .await;

        let source_history = t1.history();
        let fork = t1.fork().await.unwrap();
        assert_ne!(fork.id(), t1.id());
        assert_eq!(fork.forked_from_id(), Some(t1.id()));
        assert_history_user_texts(&fork.history(), &["a", "b"]);
        assert_eq!(fork.history(), source_history);

        fork.run(Input::Text("c".into()), TurnOptions::default())
            .await;
        assert_history_user_texts(&fork.history(), &["a", "b", "c"]);
        assert_history_user_texts(&t1.history(), &["a", "b"]);
        assert_eq!(t1.history(), source_history);
        t1.dispose().await;
        fork.dispose().await;
    }

    #[tokio::test]
    async fn a_persisted_fork_resumes_with_its_full_inherited_history() {
        let dir = unique_temp_dir("fork");
        let engine = new_engine(AgentEngineOptions {
            provider: Some(provider()),
            permission_mode: Some(ExternalPermissionMode::BypassPermissions),
            store: Some(StoreOption::Dir(scalar_test_path(
                &dir,
                "test fork store path",
            ))),
            agent_options: Some(scripted(3)),
            ..AgentEngineOptions::new(super::test_process_context())
        });
        let t1 = engine.start_thread().await.unwrap();
        t1.run(Input::Text("original turn".into()), TurnOptions::default())
            .await;
        let fork = t1.fork().await.unwrap();
        let fork_id = fork.id().clone();
        assert_eq!(fork.forked_from_id(), Some(t1.id()));
        fork.run(Input::Text("fork turn".into()), TurnOptions::default())
            .await;
        let page = engine.list_threads(ListOptions::default()).await.unwrap();
        let summary = page.items.iter().find(|item| item.id == fork_id).unwrap();
        assert_eq!(summary.forked_from_id.as_ref(), Some(t1.id()));
        let fork_history = fork.history();
        assert_history_user_texts(&fork_history, &["original turn", "fork turn"]);
        t1.dispose().await;
        fork.dispose().await;

        let resumed = engine
            .resume_thread(&scalar_test_value(&fork_id, "persisted fork id"))
            .await
            .unwrap();
        assert_eq!(resumed.forked_from_id(), Some(t1.id()));
        assert_history_user_texts(&resumed.history(), &["original turn", "fork turn"]);
        assert_eq!(resumed.history(), fork_history);
        resumed.dispose().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn fork_is_serialized_against_an_in_flight_turn() {
        let engine = new_engine(bypass(scripted(2)));
        let t = engine.start_thread().await.unwrap();
        // Start a turn but do NOT await it, then fork immediately: fork must queue
        // behind the running turn so it never snapshots a mid-tool-loop tail.
        let (result, forked) = futures::future::join(
            t.run(Input::Text("first".into()), TurnOptions::default()),
            t.fork(),
        )
        .await;
        let forked = forked.unwrap();
        assert_eq!(result.final_text, "reply 1");
        assert_history_user_texts(&forked.history(), &["first"]);
        assert_eq!(forked.history(), t.history());
        t.dispose().await;
        forked.dispose().await;
    }

    #[tokio::test]
    async fn system_prompt_append_keeps_default_and_appends_every_turn() {
        let seen: Arc<Mutex<Vec<SystemPromptPart>>> = Arc::new(Mutex::new(Vec::new()));
        let seen2 = seen.clone();
        let hook: AgentOptionsHook = Box::new(move |o: &mut AgentOptions| {
            let inner = Arc::new(scripted_call_model(vec![vec![
                ev::message_start(None),
                ev::text("ok"),
                ev::stop(None, None),
            ]]));
            let seen2 = seen2.clone();
            let cm: CallModel = Box::new(
                move |req: ModelRequest, signal: AbortSignal| -> ModelStream {
                    *seen2.lock().unwrap() = req.system.clone();
                    inner(req, signal)
                },
            );
            o.deps.call_model = Some(cm);
        });
        let engine = new_engine(AgentEngineOptions {
            provider: Some(provider()),
            permission_mode: Some(ExternalPermissionMode::BypassPermissions),
            system_prompt: Some(SystemPrompt::Append {
                append: "APPENDED-XYZ".into(),
            }),
            agent_options: Some(hook),
            ..AgentEngineOptions::new(super::test_process_context())
        });
        let thread = engine.start_thread().await.unwrap();
        thread
            .run(Input::Text("hi".into()), TurnOptions::default())
            .await;
        {
            let sys = seen.lock().unwrap();
            assert!(sys.iter().any(|p| p.name == "base"));
            assert!(sys.iter().any(|p| p.text.contains("APPENDED-XYZ")));
        }
        thread.dispose().await;
    }

    #[tokio::test]
    async fn system_prompt_string_replaces_the_default() {
        let seen: Arc<Mutex<Vec<SystemPromptPart>>> = Arc::new(Mutex::new(Vec::new()));
        let seen2 = seen.clone();
        let hook: AgentOptionsHook = Box::new(move |o: &mut AgentOptions| {
            let inner = Arc::new(scripted_call_model(vec![vec![
                ev::message_start(None),
                ev::text("ok"),
                ev::stop(None, None),
            ]]));
            let seen2 = seen2.clone();
            let cm: CallModel = Box::new(
                move |req: ModelRequest, signal: AbortSignal| -> ModelStream {
                    *seen2.lock().unwrap() = req.system.clone();
                    inner(req, signal)
                },
            );
            o.deps.call_model = Some(cm);
        });
        let engine = new_engine(AgentEngineOptions {
            provider: Some(provider()),
            permission_mode: Some(ExternalPermissionMode::BypassPermissions),
            system_prompt: Some(SystemPrompt::Text("REPLACED-PROMPT".into())),
            agent_options: Some(hook),
            ..AgentEngineOptions::new(super::test_process_context())
        });
        let thread = engine.start_thread().await.unwrap();
        thread
            .run(Input::Text("hi".into()), TurnOptions::default())
            .await;
        {
            let sys = seen.lock().unwrap();
            assert!(sys
                .iter()
                .any(|p| p.name == "custom" && p.text == "REPLACED-PROMPT"));
            assert!(!sys.iter().any(|p| p.name == "base"));
        }
        thread.dispose().await;
    }

    #[tokio::test]
    async fn exposes_the_background_subagent_snapshot() {
        let engine = new_engine(bypass(scripted(1)));
        let thread = engine.start_thread().await.unwrap();
        assert!(thread.subagents().is_empty());
        thread
            .run(Input::Text("hi".into()), TurnOptions::default())
            .await;
        // Still a valid (possibly empty) snapshot.
        let _ = thread.subagents();
        thread.dispose().await;
    }

    #[tokio::test]
    async fn exposes_a_custom_tool_and_runs_it() {
        let greeted: Arc<Mutex<RuntimeString>> = Arc::new(Mutex::new(RuntimeString::default()));
        let greeted2 = greeted.clone();
        let greet = Arc::new(TestTool {
            name: "greet".into(),
            schema: exact_json(
                r#"{"type":"object","properties":{"who":{"type":"string"}},"required":["who"],"additionalProperties":false}"#,
            ),
            run: Box::new(move |args: Json| {
                let who = args
                    .get("who")
                    .and_then(Json::as_string)
                    .cloned()
                    .unwrap_or_default();
                *greeted2.lock().unwrap() = who.clone();
                text_result(
                    Json::Object(Default::default()),
                    RuntimeString::from("hi ").concat(&who),
                    false,
                )
            }),
        }) as Arc<dyn Tool>;
        let engine = new_engine(AgentEngineOptions {
            provider: Some(provider()),
            permission_mode: Some(ExternalPermissionMode::BypassPermissions),
            tools: Some(vec![greet]),
            agent_options: Some(scripted_hook(vec![
                vec![
                    ev::message_start(None),
                    ev::tool_use("g1", "greet", &exact_json(r#"{"who":"Ada"}"#))[0].clone(),
                    ev::tool_use("g1", "greet", &exact_json(r#"{"who":"Ada"}"#))[1].clone(),
                    ev::tool_use("g1", "greet", &exact_json(r#"{"who":"Ada"}"#))[2].clone(),
                    ev::stop(Some(ModelStopReason::ToolUse), None),
                ],
                vec![
                    ev::message_start(None),
                    ev::text("greeted them"),
                    ev::stop(None, None),
                ],
            ])),
            ..AgentEngineOptions::new(super::test_process_context())
        });
        let thread = engine.start_thread().await.unwrap();
        let result = thread
            .run(Input::Text("say hi to Ada".into()), TurnOptions::default())
            .await;
        assert_eq!(*greeted.lock().unwrap(), RuntimeString::from("Ada"));
        assert_eq!(result.final_text, "greeted them");
        thread.dispose().await;
    }

    #[test]
    fn sdk_test_config_isolation_replaces_a_real_profile_override() {
        // The running app injects KLAUS_CONFIG_DIR=<real ~/.klaus> into every shell, so the
        // value being present is NOT proof of isolation. The helper must replace a real or
        // arbitrary override, and must leave its own temp dir alone so a nested engine keeps
        // the same private config.
        use std::ffi::OsStr;
        let replaced = isolated_config_dir_decision(Some(OsStr::new("/Users/me/.klaus")));
        assert!(
            replaced.is_some(),
            "a real-profile override must be replaced, not trusted"
        );
        assert!(
            isolated_config_dir_decision(Some(OsStr::new("/tmp/klaus-sdk-config-1-2"))).is_none(),
            "an already-isolated config dir is kept"
        );
        assert!(
            isolated_config_dir_decision(None).is_some(),
            "an unset config dir is isolated by construction"
        );
    }

    #[tokio::test]
    async fn worktree_shortcut_surfaces_the_enter_worktree_tool_and_config_overrides() {
        async fn tool_names(mutate: impl FnOnce(&mut AgentEngineOptions)) -> Vec<RuntimeString> {
            let seen: Arc<Mutex<Vec<RuntimeString>>> = Arc::new(Mutex::new(Vec::new()));
            let seen2 = seen.clone();
            let hook: AgentOptionsHook = Box::new(move |o: &mut AgentOptions| {
                let inner = Arc::new(scripted_call_model(vec![vec![
                    ev::message_start(None),
                    ev::text("ok"),
                    ev::stop(None, None),
                ]]));
                let seen2 = seen2.clone();
                let cm: CallModel = Box::new(
                    move |req: ModelRequest, signal: AbortSignal| -> ModelStream {
                        *seen2.lock().unwrap() =
                            req.tools.iter().map(|tool| tool.name().clone()).collect();
                        inner(req, signal)
                    },
                );
                o.deps.call_model = Some(cm);
            });
            let mut opts = AgentEngineOptions {
                provider: Some(provider()),
                permission_mode: Some(ExternalPermissionMode::BypassPermissions),
                agent_options: Some(hook),
                ..AgentEngineOptions::new(super::test_process_context())
            };
            mutate(&mut opts);
            let engine = new_engine(opts);
            let t = engine.start_thread().await.unwrap();
            t.run(Input::Text("hi".into()), TurnOptions::default())
                .await;
            t.dispose().await;
            let out = seen.lock().unwrap().clone();
            out
        }

        assert!(!tool_names(|_| {})
            .await
            .contains(&RuntimeString::from("EnterWorktree")));
        assert!(tool_names(|o| o.worktree = Some(true))
            .await
            .contains(&RuntimeString::from("EnterWorktree")));
        assert!(!tool_names(|o| {
            o.worktree = Some(true);
            o.config = Some(json!({ "worktree": false }));
        })
        .await
        .contains(&RuntimeString::from("EnterWorktree")));
    }
}
