//! The top-level [`Agent`] factory: it holds the engine-wide configuration and
//! mints [`Session`]s from it. `startSession` resolves the per-session config
//! (defaulting the model from the provider), assembles the tool set (built-ins +
//! MCP, or a host override), wires the sub-agent registry / skills / workflows,
//! and constructs the Session; `openSession`/`resumeSession` add persistence
//! (a store-backed rollout and, on resume, history + worktree/journal recovery).
//!
//! ## Module wiring
//!
//! The factory wires together several engine modules:
//!
//! - `crate::engine::session`: `Session::new(SessionInit) -> Arc<Session>`, the
//!   `SessionInit` struct (snake_case fields), the `RolloutSink` alias
//!   (`= Arc<dyn crate::store::session_store::LiveRollout>`), the async
//!   `SystemPromptProvider`, `SwarmInit { team, peer: Option<String> }`, and
//!   `resolve_config_layers(Vec<ConfigGovernanceLayer>) ->
//!   Result<ResolvedConfig, String>`.
//! - `crate::engine::deps`: `EngineDeps { compactor: Arc<dyn Compactor>, .. }`,
//!   `EngineDepsOverrides` (Default, non-Clone — it holds a boxed model-call), and
//!   `production_deps(Arc<dyn ModelProvider>, EngineDepsOverrides) -> EngineDeps`.
//! - The host's partial engine config is carried as a `serde_json::Value` object
//!   of the (camelCase) keys the host set; the config governance resolver fills
//!   the rest with schema defaults and rejects unknown top-level keys.
//! - `crate::tools::builtins::index::default_builtins`: the single shared
//!   assembler of the default tool set (gated by config), plus the
//!   `web_fetch::PageFetcher` renderer trait.
//! - `crate::engine::workflow::{builtin_workflows::resolve_workflows,
//!   discover::discover_workflows}` and `crate::tools::builtins::workflow::WorkflowRef`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};

use crate::browser::BrowserHostToolMapping;
use crate::capabilities::{
    RuntimeCapabilities, RuntimeCapabilityName, RuntimeCapabilityRegistry, RuntimeCapabilityRoot,
    SelectedRuntimeCapabilityRoot,
};
use crate::config_governance::{ConfigGovernanceLayer, ConfigGovernanceSource};
use crate::config_store;
use crate::context::compact::engine::{create_compactor, CompactorOptions};
use crate::extensions::commands::Command;
use crate::extensions::output_styles::OutputStyle;
use crate::extensions::plugins::loader::{
    LoadedPlugin, PluginLoadIssue, PluginLoadIssueKind, PluginLoadReport,
};
use crate::extensions::plugins::runtime::{
    PluginRegistry, PluginRuntimeEntry, PluginRuntimeSnapshot,
};
use crate::extensions::runtime_snapshot::{
    EffectiveExtension, EffectiveHook, ExtensionKind, ExtensionRuntimeSnapshot, ExtensionSource,
    HookHandlerKind, PricingRuntimeSnapshot, SuppressedExtension,
};
use crate::extensions::skills::builtin_skills::{builtin_skills, resolve_skills};
use crate::extensions::skills::conditional_skills::{
    create_conditional_skill_gate, ConditionalSkillGate,
};
use crate::extensions::skills::loader::Skill;
use crate::git::git::repo_root;
use crate::hooks::bus::HookBus;
use crate::hooks::setup::{
    SetupLifecycleEvent, SetupLifecycleSnapshot, SETUP_LIFECYCLE_ROLLOUT_EVENT,
};
use crate::hooks::types::{Hook, HookEvent, HookHandler};
use crate::lsp::types::LspServerConfig;
use crate::mcp::client::ManagedClient;
use crate::mcp::credential_store::CredentialStore;
use crate::mcp::types::McpServerConfig;
use crate::memory::agent_memory::AgentMemoryHost;
use crate::memory::maintenance::{
    MemoryMaintenanceEvent, MemoryMaintenanceSnapshot, MemoryMaintenanceStatus,
    MEMORY_MAINTENANCE_ROLLOUT_EVENT,
};
use crate::memory::snapshot_decision::{
    AgentMemorySnapshotDecisionAudit, AGENT_MEMORY_SNAPSHOT_DECISION_ROLLOUT_EVENT,
};
use crate::network_proxy::TrustedWindowsSandboxContext;
use crate::observability::cost_tracker::{
    CostTracker, ModelPricing, PricingTable, DEFAULT_PRICING,
};
use crate::observability::logger::Logger;
use crate::observability::otel::OtelObserver;
use crate::runtime_process::RuntimeEnv;
use crate::runtime_process::RuntimeProcessContext;
use crate::runtime_values::{runtime_codec, RuntimeJson, RuntimePath, RuntimeString};
// NodeBufferUtf8 feeds only the unix native-path decode below (OsStr bytes
// exist only on unix).
#[cfg(unix)]
use crate::runtime_values::NodeBufferUtf8;
use crate::sandbox::process_hardening::harden_process;
use crate::schemas::config::ResolvedConfig;
use crate::schemas::content::ContentBlock;
use crate::schemas::message::Message;
use crate::schemas::rollout::{RolloutItem, SessionMeta, SessionRole, UsageLedgerItem};
use crate::store::projection::project_checkpoints;
use crate::store::session_store::SessionStore;
#[cfg(test)]
use crate::store::session_store::{ResumeResult, ResumedLiveRollout};
use crate::tools::builtins::index::{
    default_builtins, BuiltinsOptions, McpCatalogThunk, PluginCatalogThunk,
};
use crate::tools::builtins::plugin_management::PluginManageFn;
use crate::tools::builtins::tool_search::ensure_tool_search;
use crate::tools::builtins::web_fetch::PageFetcher;
use crate::tools::builtins::workflow::WorkflowRef;
use crate::types::abort::AbortSignal;
use crate::types::json::Json;
use crate::types::permission::{ApprovalHandler, ApprovalReviewer, ExternalPermissionMode};
use crate::types::provider::{CallModel, ModelProvider, ModelRequest, ModelStream};
use crate::types::tool::{ElicitHandler, Tool, WorkflowJournalEntry};
use crate::worktree::worktree::{worktrees_dir, WorktreeSession};

use crate::browser::BrowserRuntime;
use crate::engine::agent_definitions::AgentDefinition;
use crate::engine::builtin_agents::{resolve_agent_definitions, BUILTIN_AGENTS};
use crate::engine::code_mode::protocol::{
    CODE_MODE_CELL_COMPLETED_EVENT, CODE_MODE_CELL_TERMINATED_EVENT, CODE_MODE_CELL_YIELDED_EVENT,
};
use crate::engine::deps::{production_deps, Compactor, EngineDeps, EngineDepsOverrides};
use crate::engine::message_queue::{
    MessageQueue, QueuePersistenceState, QUEUE_STATE_ROLLOUT_EVENT,
};
use crate::engine::resume_repair::filter_unresolved_tool_uses;
use crate::engine::session::{
    resolve_config_layers, LiveForkSnapshot, RolloutSink, Session, SessionInit, SessionInitError,
    SessionPluginUpdate, SwarmIdentity, SwarmInit, SystemPromptProvider, TurnIndex,
};
use crate::engine::subagent_lifecycle::SubagentLifecycleRecord;
use crate::engine::workflow::builtin_workflows::resolve_workflows;
use crate::engine::workflow::discover::discover_workflows;
use crate::fs::plan_files::PLAN_SLUG_ROLLOUT_EVENT;
use crate::schemas::background_process::BackgroundProcessRecord;
use futures::FutureExt;
use serde_json::Value;

/// A thunk that recomputes the live skills each turn.
pub type SkillsThunk = Arc<dyn Fn() -> Vec<Skill> + Send + Sync>;
/// A thunk that recomputes the live output styles.
pub type OutputStylesThunk = Arc<dyn Fn() -> Vec<OutputStyle> + Send + Sync>;
/// A thunk that recomputes the live named workflows.
pub type WorkflowsThunk = Arc<dyn Fn() -> Vec<WorkflowRef> + Send + Sync>;
/// A thunk that recomputes the live language-server set.
pub type LspServersThunk = Arc<dyn Fn() -> Vec<LspServerConfig> + Send + Sync>;
/// A thunk that recomputes the live tool set between turns (`refreshTools`).
pub type RefreshToolsThunk = Arc<dyn Fn() -> Vec<Arc<dyn Tool>> + Send + Sync>;

/// Configuration for an [`Agent`]: the provider, engine config, and every
/// host-injectable capability. All optional capabilities default to absent.
pub struct AgentOptions {
    /// Required process namespace shared by this Agent and every Session it
    /// creates. Spawn-bearing services must use this exact context rather than
    /// bootstrapping a process owner lazily.
    pub process_context: RuntimeProcessContext,
    pub provider: Arc<dyn ModelProvider>,
    /// The host's partial engine config: a JSON object of the (camelCase) keys the
    /// host set. Absent keys fall back to the schema defaults in `resolve_config`.
    pub config: Option<Value>,
    /// Override the built-in tool set entirely.
    pub tools: Option<Vec<Arc<dyn Tool>>>,
    /// Inject a JS-rendering page fetcher for `web_fetch`. Default: the built-in
    /// static HTTP fetch + HTML→text. Ignored when `tools` overrides the set.
    pub web_fetch_page: Option<Arc<dyn PageFetcher>>,
    /// Inject a browser automation runtime. When present and the host has not
    /// overridden the tool set, the engine exposes browser_* protocol tools.
    pub browser_runtime: Option<Arc<dyn BrowserRuntime>>,
    /// Host/plugin runtime capability roots. `browser_runtime` is retained as a
    /// compatibility shortcut and is normalized into this registry at startup.
    pub capability_roots: Vec<RuntimeCapabilityRoot>,
    pub cwd: Option<String>,
    /// Override engine deps (callModel, clock) — the test seam.
    pub deps: EngineDepsOverrides,
    /// Override the model-backed context compactor.
    pub compactor: Option<Arc<dyn Compactor>>,
    pub approval_handler: Option<Arc<dyn ApprovalHandler>>,
    pub approval_reviewer: Option<Arc<dyn ApprovalReviewer>>,
    pub elicit_handler: Option<Arc<dyn ElicitHandler>>,
    /// Secret store for per-sub-agent MCP server auth.
    pub credential_store: Option<Arc<dyn CredentialStore>>,
    pub hooks: Vec<Hook>,
    /// OTel metrics sink fed every event. Outer `None` = default (auto); `Some(None)`
    /// = disabled; `Some(Some(o))` = a custom observer.
    pub observer: Option<Option<Arc<std::sync::Mutex<OtelObserver>>>>,
    /// Structured logger fed turn-lifecycle + error events.
    pub logger: Option<Arc<Logger>>,
    /// Host-injected cost table (model-substring → USD/MTok rate).
    pub pricing: Option<std::collections::HashMap<String, ModelPricing>>,
    /// Named sub-agent roles the `task` tool can delegate to, MERGED over the
    /// built-in roles (a definition here with the same name replaces the built-in).
    pub agents: Option<Vec<AgentDefinition>>,
    /// Ship the built-in agent roles (default true). When false, only `agents` are
    /// available.
    pub include_default_agents: Option<bool>,
    /// Skill source: enables the `skill` tool.
    pub skills: Option<SkillsThunk>,
    /// Output-style source.
    pub output_styles: Option<OutputStylesThunk>,
    /// Named workflows the `workflow` tool can run by name.
    pub workflows: Option<WorkflowsThunk>,
    /// MCP-server-store catalog backend factory.
    pub mcp_catalog: Option<McpCatalogThunk>,
    /// Plugin-store catalog backend factory.
    pub plugin_catalog: Option<PluginCatalogThunk>,
    /// Language servers the engine starts on demand.
    pub lsp_servers: Option<LspServersThunk>,
    /// Host adapter for per-agent memory.
    pub agent_memory: Option<Arc<dyn AgentMemoryHost>>,
    /// Host-connected MCP clients, exposed to the resource tools.
    pub mcp_clients: Option<Vec<ManagedClient>>,
    /// Host/user/project MCP server declarations already represented outside
    /// the plugin layer. The engine uses these as content-signature dedup
    /// targets so plugin MCP servers do not launch a duplicate process or
    /// remote connection.
    pub mcp_server_configs: Vec<McpServerConfig>,
    /// MCP tools to APPEND to the engine's built-in tool set. Ignored when `tools`
    /// overrides the set.
    pub mcp_tools: Option<Vec<Arc<dyn Tool>>>,
    /// Agent-level default for recomputing the live tool set between turns.
    pub refresh_tools: Option<RefreshToolsThunk>,
    /// Agent-level default system-prompt provider.
    pub system_prompt_provider: Option<SystemPromptProvider>,
    /// Already-loaded plugin bundles. `Agent::new` registers them and merges
    /// their engine-facing capabilities into session construction.
    pub plugins: Vec<LoadedPlugin>,
    /// Optional full loader report, preserving disabled plugins plus load
    /// errors/warnings for diagnostics surfaces.
    pub plugin_load_report: Option<PluginLoadReport>,
}

/// Per-session overrides layered over an [`AgentOptions`]. Each field, when set,
/// wins over the agent-level default.
#[derive(Default)]
pub struct StartSessionOptions {
    pub durable_permissions: Option<Arc<crate::permission::durable::DurablePermissionLayer>>,
    pub id: Option<RuntimeString>,
    pub cwd: Option<RuntimePath>,
    /// Per-session override for the user's config home (`~/.klaus`). Used by
    /// tests to keep the session from reading the developer's real
    /// `AGENTS.md`.
    pub user_config_dir: Option<RuntimePath>,
    /// Per-session provider override. This is the stdio/app-server equivalent of
    /// constructing a fresh SDK `AgentEngine` with a different provider for one
    /// window/session.
    pub provider: Option<Arc<dyn ModelProvider>>,
    /// Per-session partial engine config. Absent keys inherit the agent-level
    /// config/defaults; present keys win at the session-flags layer.
    pub config: Option<Value>,
    /// Explicit per-session network selection, independent of filesystem mode.
    pub network_allowed: Option<bool>,
    pub permission_mode: Option<ExternalPermissionMode>,
    pub pre_plan_permission_mode: Option<ExternalPermissionMode>,
    pub tools: Option<Vec<Arc<dyn Tool>>>,
    pub web_fetch_page: Option<Arc<dyn PageFetcher>>,
    pub browser_runtime: Option<Arc<dyn BrowserRuntime>>,
    /// Picks the `web_search` backend from the session's resolved config, so a
    /// config-layer search source (a named model subscription) can swap the
    /// backend without the connection knowing the credentials.
    pub web_search_factory: Option<crate::tools::builtins::web_search::WebSearchBackendFactory>,
    /// Resolves page extraction from the merged user, project and session config.
    pub web_fetch_factory: Option<crate::tools::builtins::web_fetch::PageFetcherFactory>,
    pub capability_roots: Option<Vec<RuntimeCapabilityRoot>>,
    pub selected_capability_roots: Option<Vec<SelectedRuntimeCapabilityRoot>>,
    /// Host callback to recompute the live tool set between turns.
    pub refresh_tools: Option<RefreshToolsThunk>,
    pub approval_handler: Option<Arc<dyn ApprovalHandler>>,
    pub approval_reviewer: Option<Arc<dyn ApprovalReviewer>>,
    pub elicit_handler: Option<Arc<dyn ElicitHandler>>,
    pub mcp_elicitation_presenter:
        Option<Arc<dyn crate::mcp::elicitation::McpElicitationPresenter>>,
    pub mcp_authorize_handler: Option<Arc<dyn crate::mcp::client::McpAuthorizeHandler>>,
    pub mcp_oauth_redirect_uri: Option<String>,
    pub credential_store: Option<Arc<dyn CredentialStore>>,
    pub initial_history: Option<Vec<Message>>,
    /// Original bounded review evidence reconstructed from raw rollout records.
    pub initial_approval_context: Option<super::approval_capture::ReviewParentCheckpoint>,
    /// Latest lifecycle state for surviving review records on resume.
    pub initial_approval_reviews: Option<Vec<crate::types::approval_review::ApprovalReviewEvent>>,
    /// Persisted per-message tool-result budget replacement records for a
    /// resumed session (see [`SessionInit::initial_tool_result_budget_records`]).
    pub initial_tool_result_budget_records: Option<Vec<(RuntimeString, RuntimeString)>>,
    pub hidden_context_message_ids: Option<Vec<RuntimeString>>,
    pub session_role: Option<SessionRole>,
    pub parent_thread_id: Option<RuntimeString>,
    /// Thread id this session is forked from. `fork_session` sets this and
    /// `open_session` persists it in the new rollout header.
    pub forked_from_id: Option<RuntimeString>,
    /// Resume-reconstructed rewind checkpoint ledger.
    pub initial_checkpoints: Option<Vec<Option<String>>>,
    pub rollout: Option<RolloutSink>,
    /// Persistence backend (set automatically by `open_session`/`resume_session`).
    pub store: Option<Arc<dyn SessionStore>>,
    pub hooks: Option<Vec<Hook>>,
    /// Static sub-agent definitions added for this session. They override
    /// endpoint/plugin/builtin definitions with the same name.
    pub agents: Option<Vec<AgentDefinition>>,
    pub system_prompt_provider: Option<SystemPromptProvider>,
    /// Select the effective definition used by this session's root execution.
    pub agent_definition_name: Option<RuntimeString>,
    /// Replace the generated base prompt for this session.
    pub base_instructions: Option<RuntimeString>,
    /// Append a distinct developer prompt part after the selected base prompt.
    pub developer_instructions: Option<RuntimeString>,
    pub skills: Option<SkillsThunk>,
    pub output_styles: Option<OutputStylesThunk>,
    /// Strongly typed output-style selection for serializable hosts.
    pub selected_output_style: Option<RuntimeString>,
    /// Ordered per-session pricing table. Order is substring-match precedence.
    pub pricing: Option<PricingTable>,
    pub workflows: Option<WorkflowsThunk>,
    pub mcp_catalog: Option<McpCatalogThunk>,
    pub plugin_catalog: Option<PluginCatalogThunk>,
    /// Host/session plugin home for installed-plugin lifecycle tools.
    pub plugin_home: Option<String>,
    pub lsp_servers: Option<LspServersThunk>,
    /// Explicit shell environment supplied by a serializable host. This wins
    /// over lazy login-shell capture, including when intentionally empty.
    pub shell_env: Option<RuntimeEnv>,
    /// Private Host-to-engine broker route. Public config and process
    /// environment cannot populate this value.
    #[doc(hidden)]
    pub trusted_windows_sandbox_context: Option<TrustedWindowsSandboxContext>,
    pub agent_memory: Option<Arc<dyn AgentMemoryHost>>,
    pub agent_memory_snapshot_policy:
        Option<crate::memory::snapshot_decision::AgentMemorySnapshotDecisionPolicy>,
    /// This session's swarm identity (team + optional peer).
    pub swarm: Option<SwarmIdentity>,
    pub mcp_clients: Option<Vec<ManagedClient>>,
    pub mcp_tools: Option<Vec<Arc<dyn Tool>>>,
    /// MCP servers to connect for this session. Used by stdio/app hosts whose
    /// serializable init envelope can describe a subprocess or remote MCP server,
    /// while live in-process clients still use `mcp_clients`.
    pub mcp_server_configs: Option<Vec<McpServerConfig>>,
    /// Restore an active worktree (set automatically by `resume_session`).
    pub current_worktree: Option<WorktreeSession>,
    /// Seed the workflow result journal (set automatically by `resume_session`).
    pub prior_workflow_journal: Option<Vec<WorkflowJournalEntry>>,
    /// Seed the tool_search-discovered tools (set automatically by `resume_session`).
    pub initial_surfaced_tools: Option<Vec<RuntimeString>>,
    /// Seed cumulative billing state reconstructed from rollout records. This is
    /// set automatically by `resume_session`; forks intentionally leave it empty.
    #[doc(hidden)]
    pub initial_usage_ledger: Option<Vec<UsageLedgerItem>>,
    /// Seed the authoritative sub-agent lifecycle rebuilt from rollout records.
    /// This is set automatically by `resume_session`; forks start empty.
    #[doc(hidden)]
    pub initial_subagent_lifecycle: Option<Vec<SubagentLifecycleRecord>>,
    /// Seed the authoritative background-process lifecycle rebuilt from rollout
    /// records. This is set automatically by `resume_session`; forks start empty.
    #[doc(hidden)]
    pub initial_background_processes: Option<Vec<BackgroundProcessRecord>>,
    /// Seed automatic memory-maintenance state reconstructed from rollout records.
    /// Forks intentionally start with an empty audit history.
    #[doc(hidden)]
    pub initial_memory_maintenance: Option<MemoryMaintenanceSnapshot>,
    /// Seed durable Setup init/context-consumption state reconstructed on resume.
    /// Forks intentionally start with a fresh Setup lifecycle.
    #[doc(hidden)]
    pub initial_setup_lifecycle: Option<SetupLifecycleSnapshot>,
    /// Seed Agent Memory snapshot decision audits reconstructed from rollout.
    /// Forks intentionally start with an empty decision registry.
    #[doc(hidden)]
    pub initial_agent_memory_snapshot_decisions: Option<Vec<AgentMemorySnapshotDecisionAudit>>,
}

/// A shareable model-call handle: the [`CallModel`] seam re-boxed per session.
type SharedCallModel = Arc<dyn Fn(ModelRequest, AbortSignal) -> ModelStream + Send + Sync>;

/// The host's engine-deps overrides, decomposed into individually-shareable
/// fields at [`Agent`] construction. The model-call seam is a non-cloneable
/// [`CallModel`] box, so it is held as an [`Arc`] and re-boxed for every session;
/// the remaining seams are already `Arc`-backed and clone cheaply.
struct SharedDepsOverrides {
    call_model: Option<SharedCallModel>,
    estimate_tokens: Option<crate::engine::deps::EstimateTokensFn>,
    compactor: Option<Arc<dyn Compactor>>,
    uuid: Option<crate::engine::deps::UuidFn>,
    now: Option<crate::engine::deps::NowFn>,
}

impl SharedDepsOverrides {
    /// Decompose the host's overrides into shareable fields (Arc-wrapping the
    /// otherwise non-cloneable boxed model-call).
    fn from_overrides(o: EngineDepsOverrides) -> Self {
        SharedDepsOverrides {
            call_model: o.call_model.map(Arc::from),
            estimate_tokens: o.estimate_tokens,
            compactor: o.compactor,
            uuid: o.uuid,
            now: o.now,
        }
    }

    /// Reconstruct a fresh [`EngineDepsOverrides`] for one session: a new boxed
    /// model-call delegating to the shared handle, plus clones of the Arc seams.
    fn to_overrides(&self) -> EngineDepsOverrides {
        EngineDepsOverrides {
            call_model: self.call_model.as_ref().map(|cm| {
                let cm = cm.clone();
                Box::new(move |req, signal| cm(req, signal)) as CallModel
            }),
            estimate_tokens: self.estimate_tokens.clone(),
            compactor: self.compactor.clone(),
            uuid: self.uuid.clone(),
            now: self.now.clone(),
        }
    }
}

/// Bridges the context-compaction engine's compactor to the engine-deps
/// [`Compactor`] seam. The two share semantics but differ in message ownership
/// (owned vs borrowed), so this adapter forwards
/// each call and converts the result.
struct RealCompactorBridge(Arc<dyn crate::context::compact::engine::Compactor>);

#[async_trait::async_trait]
impl Compactor for RealCompactorBridge {
    fn fork_precompute(
        &self,
        call_model: CallModel,
        config: &ResolvedConfig,
    ) -> Result<Arc<dyn Compactor>, String> {
        Ok(Arc::new(Self(self.0.fork_precompute(call_model, config)?)))
    }
    fn native_precompute_context(&self) -> Option<RuntimeString> {
        self.0.native_precompute_context()
    }
    fn fork_isolated(
        &self,
        call_model: CallModel,
        config: &ResolvedConfig,
    ) -> Result<Arc<dyn Compactor>, String> {
        Ok(Arc::new(Self(self.0.fork_isolated(call_model, config)?)))
    }
    fn rebind_summary(
        &self,
        provider: Arc<dyn ModelProvider>,
        config: &ResolvedConfig,
    ) -> Result<Option<Arc<dyn Compactor>>, String> {
        let transport = provider.clone();
        let call_model: CallModel =
            Box::new(move |request, signal| transport.stream(request, signal));
        let compactor = self.0.rebind_summary(call_model, config)?;
        compactor.bind_native_provider(provider);
        Ok(Some(Arc::new(Self(compactor))))
    }
    fn rebind_model(
        &self,
        provider: Arc<dyn ModelProvider>,
        config: &ResolvedConfig,
    ) -> Result<Arc<dyn Compactor>, String> {
        self.rebind_summary(provider, config)?
            .ok_or_else(|| "Cannot bind summary model".to_owned())
    }
    fn bind_output_observer(
        &self,
        observer: Option<crate::context::compact::full_compact::CompactionOutputSink>,
    ) {
        self.0.bind_output_observer(observer);
    }
    fn bind_session_memory(
        &self,
        provider: Option<
            Arc<dyn crate::context::compact::full_compact::SessionMemoryCompactProvider>,
        >,
    ) {
        self.0.bind_session_memory(provider);
    }
    fn prepare_request(&self, request: &ModelRequest, signal: &AbortSignal) {
        self.0.prepare_request(request, signal);
    }
    fn reactive_enabled(&self) -> bool {
        self.0.reactive_enabled()
    }
    fn native_summary_active(&self) -> bool {
        self.0.native_summary_active()
    }
    async fn manual_compact(
        &self,
        messages: &[Message],
        config: &ResolvedConfig,
        instructions: Option<&RuntimeString>,
    ) -> crate::engine::deps::CompactionResult {
        into_deps_result(
            self.0
                .manual_compact(messages.to_vec(), config, instructions)
                .await,
        )
    }
    async fn local_compact(
        &self,
        messages: &[Message],
        config: &ResolvedConfig,
        instructions: Option<&RuntimeString>,
    ) -> crate::engine::deps::CompactionResult {
        into_deps_result(
            self.0
                .local_compact(messages.to_vec(), config, instructions)
                .await,
        )
    }
    async fn local_compact_exact(
        &self,
        messages: &[Message],
        config: &ResolvedConfig,
        instructions: Option<&RuntimeString>,
    ) -> crate::engine::deps::CompactionResult {
        into_deps_result(
            self.0
                .local_compact_exact(messages.to_vec(), config, instructions)
                .await,
        )
    }
    async fn post_turn_compact(
        &self,
        messages: &[Message],
        config: &ResolvedConfig,
        instructions: Option<&RuntimeString>,
    ) -> crate::engine::deps::CompactionResult {
        into_deps_result(
            self.0
                .post_turn_compact(messages.to_vec(), config, instructions)
                .await,
        )
    }
    async fn threshold_compact(
        &self,
        messages: &[Message],
        config: &ResolvedConfig,
        instructions: Option<&RuntimeString>,
    ) -> crate::engine::deps::CompactionResult {
        into_deps_result(
            self.0
                .threshold_compact(messages.to_vec(), config, instructions)
                .await,
        )
    }
    async fn reactive_compact(
        &self,
        messages: &[Message],
        config: &ResolvedConfig,
        instructions: Option<&RuntimeString>,
    ) -> crate::engine::deps::CompactionResult {
        into_deps_result(
            self.0
                .reactive_compact(messages.to_vec(), config, instructions)
                .await,
        )
    }
    async fn maybe_compact(
        &self,
        messages: &[Message],
        estimated_tokens: u64,
        config: &ResolvedConfig,
        instructions: Option<&RuntimeString>,
    ) -> crate::engine::deps::CompactionResult {
        into_deps_result(
            self.0
                .maybe_compact(messages.to_vec(), estimated_tokens, config, instructions)
                .await,
        )
    }
    async fn partial_compact(
        &self,
        messages: &[Message],
        index: usize,
        direction: crate::context::compact::full_compact::PartialCompactionDirection,
        config: &ResolvedConfig,
        instructions: Option<&RuntimeString>,
    ) -> crate::engine::deps::CompactionResult {
        into_deps_result(
            self.0
                .partial_compact(messages.to_vec(), index, direction, config, instructions)
                .await,
        )
    }
}

/// Convert the compaction engine's result into the engine-deps result (same
/// fields; distinct types across the two module boundaries).
fn into_deps_result(
    r: crate::context::compact::full_compact::CompactionResult,
) -> crate::engine::deps::CompactionResult {
    crate::engine::deps::CompactionResult {
        messages: r.messages,
        freed_tokens: r.freed_tokens,
        compacted: r.compacted,
        reason: r.reason,
        diagnostics: r.diagnostics,
    }
}

fn merge_plugin_capabilities_into_options(opts: &mut AgentOptions) {
    if opts.plugins.is_empty() {
        return;
    }

    let plugin_agents = plugin_agent_definitions(&opts.plugins);
    if !plugin_agents.is_empty() {
        let explicit = opts.agents.take().unwrap_or_default();
        opts.agents = Some(merge_agent_definitions(plugin_agents, explicit));
    }

    let plugin_skills = opts
        .plugins
        .iter()
        .flat_map(|plugin| plugin.skills.iter().cloned())
        .collect::<Vec<_>>();
    if !plugin_skills.is_empty() {
        let explicit = opts.skills.take();
        opts.skills = Some(Arc::new(move || {
            let explicit_skills = explicit.as_ref().map(|f| f()).unwrap_or_default();
            merge_skills(plugin_skills.clone(), explicit_skills)
        }));
    }

    let plugin_styles = opts
        .plugins
        .iter()
        .flat_map(|plugin| plugin.output_styles.iter().cloned())
        .collect::<Vec<_>>();
    if !plugin_styles.is_empty() {
        let explicit = opts.output_styles.take();
        opts.output_styles = Some(Arc::new(move || {
            let explicit_styles = explicit.as_ref().map(|f| f()).unwrap_or_default();
            merge_output_styles(plugin_styles.clone(), explicit_styles)
        }));
    }

    let plugin_hooks = opts
        .plugins
        .iter()
        .flat_map(|plugin| plugin.hooks.iter().cloned())
        .collect::<Vec<_>>();
    if !plugin_hooks.is_empty() {
        let mut hooks = plugin_hooks;
        hooks.extend(std::mem::take(&mut opts.hooks));
        opts.hooks = hooks;
    }

    let plugin_lsp_servers = plugin_lsp_server_configs(&opts.plugins);
    if !plugin_lsp_servers.is_empty() {
        let explicit = opts.lsp_servers.take();
        opts.lsp_servers = Some(Arc::new(move || {
            let explicit_servers = explicit.as_ref().map(|f| f()).unwrap_or_default();
            merge_lsp_servers(plugin_lsp_servers.clone(), explicit_servers)
        }));
    }
}

fn plugin_slash_commands(plugins: &[LoadedPlugin]) -> Vec<Command> {
    plugins
        .iter()
        .flat_map(|plugin| plugin.commands.iter().cloned())
        .collect()
}

#[derive(Default)]
struct PluginMcpServerResolution {
    servers: Vec<McpServerConfig>,
    effective_server_names_by_plugin: HashMap<String, Vec<String>>,
    warnings: Vec<PluginLoadIssue>,
}

fn plugin_mcp_server_configs(
    plugins: &[LoadedPlugin],
    manual_servers: &[McpServerConfig],
) -> PluginMcpServerResolution {
    let mut resolution = PluginMcpServerResolution::default();
    let mut manual_signatures: HashMap<String, String> = HashMap::new();
    for server in manual_servers {
        if let Some(signature) = server.dedup_signature() {
            manual_signatures
                .entry(signature)
                .or_insert_with(|| server.name().to_string());
        }
    }

    let mut seen_plugin_signatures: HashMap<String, String> = HashMap::new();
    for plugin in plugins {
        let mut effective_names = Vec::new();
        for server in &plugin.mcp_servers {
            let server_name = server.name().to_string();
            let duplicate_of = server.dedup_signature().and_then(|signature| {
                if let Some(name) = manual_signatures.get(&signature) {
                    return Some(name.clone());
                }
                if let Some(name) = seen_plugin_signatures.get(&signature) {
                    return Some(name.clone());
                }
                seen_plugin_signatures
                    .insert(signature, format!("{}:{}", plugin.name, server_name));
                None
            });

            if let Some(duplicate_of) = duplicate_of {
                resolution.warnings.push(PluginLoadIssue {
                    kind: PluginLoadIssueKind::DuplicateMcpServerSuppressed,
                    dir: RuntimeString::from(plugin.dir.clone()),
                    message: RuntimeString::from(format!(
                        "Suppressing plugin MCP server \"{}:{}\": duplicates \"{}\".",
                        plugin.name, server_name, duplicate_of
                    )),
                });
                continue;
            }

            effective_names.push(server_name);
            // Host-managed declarations name a server the host owns and binds
            // at session init; the engine attributes it to the plugin (so the
            // browser tool mapping and contribution listing see it) but never
            // connects it. The live endpoint arrives as a host-injected
            // session server and is connected through the normal path.
            if !matches!(server, McpServerConfig::HostManaged { .. }) {
                resolution.servers.push(server.clone());
            }
        }
        resolution
            .effective_server_names_by_plugin
            .insert(plugin.name.clone(), effective_names);
    }
    resolution
}

pub(crate) fn merge_session_mcp_servers(
    session_servers: Vec<McpServerConfig>,
    plugin_servers: Vec<McpServerConfig>,
) -> Vec<McpServerConfig> {
    let mut out = Vec::with_capacity(session_servers.len() + plugin_servers.len());
    let mut seen_signatures: HashSet<String> = HashSet::new();
    let mut seen_names: HashSet<String> = HashSet::new();
    for server in session_servers {
        seen_names.insert(server.name().to_string());
        if let Some(signature) = server.dedup_signature() {
            seen_signatures.insert(signature);
        }
        out.push(server);
    }
    for server in plugin_servers {
        if !seen_names.insert(server.name().to_string()) {
            continue;
        }
        if let Some(signature) = server.dedup_signature() {
            if seen_signatures.contains(&signature) {
                continue;
            }
            seen_signatures.insert(signature);
        }
        out.push(server);
    }
    out
}

fn plugin_agent_definitions(plugins: &[LoadedPlugin]) -> Vec<AgentDefinition> {
    plugins
        .iter()
        .flat_map(|plugin| plugin.agents.iter().map(plugin_agent_definition))
        .collect()
}

fn plugin_agent_definition(
    def: &crate::extensions::agent_loader::AgentDefinition,
) -> AgentDefinition {
    AgentDefinition {
        name: def.name.clone().into(),
        description: def.description.clone().into(),
        tools: def
            .tools
            .as_ref()
            .map(|tools| tools.iter().cloned().map(Into::into).collect()),
        disallowed_tools: def
            .disallowed_tools
            .as_ref()
            .map(|tools| tools.iter().cloned().map(Into::into).collect()),
        model: def.model.clone().map(Into::into),
        runtime_policy: def.runtime_policy.clone(),
        effort: def.effort.clone(),
        ultra_mode: def.ultra_mode,
        prompt: def.prompt.clone().map(Into::into),
        ..Default::default()
    }
}

fn merge_agent_definitions(
    base: Vec<AgentDefinition>,
    overrides: Vec<AgentDefinition>,
) -> Vec<AgentDefinition> {
    let mut merged: Vec<AgentDefinition> = Vec::with_capacity(base.len() + overrides.len());
    for item in base.into_iter().chain(overrides) {
        match merged
            .iter_mut()
            .find(|existing| existing.name == item.name)
        {
            Some(existing) => *existing = item,
            None => merged.push(item),
        }
    }
    merged
}

fn merge_skills(base: Vec<Skill>, overrides: Vec<Skill>) -> Vec<Skill> {
    let mut merged: Vec<Skill> = Vec::with_capacity(base.len() + overrides.len());
    for item in base.into_iter().chain(overrides) {
        match merged
            .iter_mut()
            .find(|existing| existing.name == item.name)
        {
            Some(existing) => *existing = item,
            None => merged.push(item),
        }
    }
    merged
}

fn merge_output_styles(base: Vec<OutputStyle>, overrides: Vec<OutputStyle>) -> Vec<OutputStyle> {
    let mut merged: Vec<OutputStyle> = Vec::with_capacity(base.len() + overrides.len());
    for item in base.into_iter().chain(overrides) {
        match merged
            .iter_mut()
            .find(|existing| existing.name == item.name)
        {
            Some(existing) => *existing = item,
            None => merged.push(item),
        }
    }
    merged
}

fn plugin_lsp_server_configs(plugins: &[LoadedPlugin]) -> Vec<LspServerConfig> {
    plugins
        .iter()
        .flat_map(|plugin| {
            plugin
                .lsp_servers
                .iter()
                .filter_map(|(language, raw)| plugin_lsp_server_config(language, raw))
        })
        .collect()
}

fn plugin_lsp_server_config(language: &str, raw: &Value) -> Option<LspServerConfig> {
    let obj = raw.as_object()?;
    let command = RuntimeString::from(obj.get("command")?.as_str()?);
    let args = obj.get("args").and_then(string_array);
    let extensions = obj
        .get("extensions")
        .and_then(string_array)
        .filter(|items| !items.is_empty())
        .unwrap_or_else(|| vec![RuntimeString::from(language)]);
    let root_dir = obj
        .get("rootDir")
        .or_else(|| obj.get("root_dir"))
        .and_then(Value::as_str)
        .map(RuntimeString::from);

    Some(LspServerConfig {
        language: RuntimeString::from(language),
        extensions,
        command,
        args,
        root_dir,
    })
}

fn string_array(value: &Value) -> Option<Vec<RuntimeString>> {
    Some(
        value
            .as_array()?
            .iter()
            .filter_map(|item| item.as_str().map(RuntimeString::from))
            .collect(),
    )
}

fn merge_lsp_servers(
    mut base: Vec<LspServerConfig>,
    overrides: Vec<LspServerConfig>,
) -> Vec<LspServerConfig> {
    for item in overrides {
        match base
            .iter_mut()
            .find(|existing| existing.language == item.language)
        {
            Some(existing) => *existing = item,
            None => base.push(item),
        }
    }
    base
}

fn runtime_capability_registry_from_options(
    opts: &AgentOptions,
    effective_plugin_mcp_servers: &HashMap<String, Vec<String>>,
) -> RuntimeCapabilityRegistry {
    runtime_capability_registry_from_plugins(opts, &opts.plugins, effective_plugin_mcp_servers)
}

fn runtime_capability_registry_from_plugins(
    opts: &AgentOptions,
    plugins: &[LoadedPlugin],
    effective_plugin_mcp_servers: &HashMap<String, Vec<String>>,
) -> RuntimeCapabilityRegistry {
    let mut registry = RuntimeCapabilityRegistry::from_roots(opts.capability_roots.clone());
    for plugin in plugins {
        if !plugin
            .runtime_capabilities
            .iter()
            .any(|cap| cap == RuntimeCapabilityName::Browser.as_str())
        {
            continue;
        }
        if let Some(mapping) = browser_tool_mapping_for_plugin(
            plugin,
            effective_plugin_mcp_servers
                .get(&plugin.name)
                .map(Vec::as_slice),
        ) {
            registry.register(
                RuntimeCapabilityRoot::plugin(
                    plugin.name.clone(),
                    plugin.version.clone(),
                    plugin.dir.clone(),
                )
                .with_browser_tool_mapping(mapping),
            );
        }
    }
    if let Some(runtime) = &opts.browser_runtime {
        registry.register(
            RuntimeCapabilityRoot::host("host.browser").with_browser_runtime(runtime.clone()),
        );
    }
    registry
}

fn browser_tool_mapping_for_plugin(
    plugin: &LoadedPlugin,
    effective_server_names: Option<&[String]>,
) -> Option<BrowserHostToolMapping> {
    let server_names = match effective_server_names {
        Some(names) => names.to_vec(),
        None => plugin
            .mcp_servers
            .iter()
            .map(|server| server.name().to_string())
            .collect::<Vec<_>>(),
    };
    if server_names.is_empty() {
        return None;
    }
    let selected = server_names
        .iter()
        .find(|name| name.as_str() == "browser")
        .or_else(|| {
            if server_names.len() == 1 {
                server_names.first()
            } else {
                None
            }
        })?;
    Some(BrowserHostToolMapping::for_mcp_server(selected))
}

#[derive(Clone)]
struct Sourced<T> {
    value: T,
    source: ExtensionSource,
}

#[derive(Clone)]
struct NamedSource {
    name: RuntimeString,
    source: ExtensionSource,
}

#[derive(Clone, Default)]
struct AgentExtensionSources {
    plugin_agents: Vec<Sourced<AgentDefinition>>,
    host_agents: Vec<Sourced<AgentDefinition>>,
    plugin_skills: Vec<Sourced<Skill>>,
    host_skills: Option<SkillsThunk>,
    plugin_output_styles: Vec<Sourced<OutputStyle>>,
    host_output_styles: Option<OutputStylesThunk>,
    plugin_hooks: Vec<Sourced<Hook>>,
    host_hooks: Vec<Sourced<Hook>>,
}

impl AgentExtensionSources {
    fn from_options(opts: &AgentOptions) -> Self {
        let plugin_agents = opts
            .plugins
            .iter()
            .flat_map(|plugin| {
                plugin.agents.iter().map(|definition| Sourced {
                    value: plugin_agent_definition(definition),
                    source: ExtensionSource::plugin(plugin.name.clone()),
                })
            })
            .collect();
        let plugin_skills = opts
            .plugins
            .iter()
            .flat_map(|plugin| {
                plugin.skills.iter().cloned().map(|skill| Sourced {
                    value: skill,
                    source: ExtensionSource::plugin(plugin.name.clone()),
                })
            })
            .collect();
        let plugin_output_styles = opts
            .plugins
            .iter()
            .flat_map(|plugin| {
                plugin.output_styles.iter().cloned().map(|style| Sourced {
                    value: style,
                    source: ExtensionSource::plugin(plugin.name.clone()),
                })
            })
            .collect();
        let plugin_hooks = opts
            .plugins
            .iter()
            .flat_map(|plugin| {
                plugin.hooks.iter().cloned().map(|hook| Sourced {
                    value: hook,
                    source: ExtensionSource::plugin(plugin.name.clone()),
                })
            })
            .collect();

        Self {
            plugin_agents,
            host_agents: opts
                .agents
                .clone()
                .unwrap_or_default()
                .into_iter()
                .map(|value| Sourced {
                    value,
                    source: ExtensionSource::host(),
                })
                .collect(),
            plugin_skills,
            host_skills: opts.skills.clone(),
            plugin_output_styles,
            host_output_styles: opts.output_styles.clone(),
            plugin_hooks,
            host_hooks: opts
                .hooks
                .iter()
                .cloned()
                .map(|value| Sourced {
                    value,
                    source: ExtensionSource::host(),
                })
                .collect(),
        }
    }

    fn with_plugins(&self, plugins: &[LoadedPlugin]) -> Self {
        Self {
            plugin_agents: plugins
                .iter()
                .flat_map(|plugin| {
                    plugin.agents.iter().map(|definition| Sourced {
                        value: plugin_agent_definition(definition),
                        source: ExtensionSource::plugin(plugin.name.clone()),
                    })
                })
                .collect(),
            host_agents: self.host_agents.clone(),
            plugin_skills: plugins
                .iter()
                .flat_map(|plugin| {
                    plugin.skills.iter().cloned().map(|value| Sourced {
                        value,
                        source: ExtensionSource::plugin(plugin.name.clone()),
                    })
                })
                .collect(),
            host_skills: self.host_skills.clone(),
            plugin_output_styles: plugins
                .iter()
                .flat_map(|plugin| {
                    plugin.output_styles.iter().cloned().map(|value| Sourced {
                        value,
                        source: ExtensionSource::plugin(plugin.name.clone()),
                    })
                })
                .collect(),
            host_output_styles: self.host_output_styles.clone(),
            plugin_hooks: plugins
                .iter()
                .flat_map(|plugin| {
                    plugin.hooks.iter().cloned().map(|value| Sourced {
                        value,
                        source: ExtensionSource::plugin(plugin.name.clone()),
                    })
                })
                .collect(),
            host_hooks: self.host_hooks.clone(),
        }
    }
}

fn named_source<T>(items: &[Sourced<T>], name: impl Fn(&T) -> RuntimeString) -> Vec<NamedSource> {
    items
        .iter()
        .map(|item| NamedSource {
            name: name(&item.value),
            source: item.source.clone(),
        })
        .collect()
}

fn runtime_agent_name_to_extension_snapshot(name: &RuntimeString) -> RuntimeString {
    name.clone()
}

fn runtime_agent_source(items: &[Sourced<AgentDefinition>]) -> Vec<NamedSource> {
    items
        .iter()
        .map(|item| NamedSource {
            name: runtime_agent_name_to_extension_snapshot(&item.value.name),
            source: item.source.clone(),
        })
        .collect()
}

fn effective_extensions(
    kind: ExtensionKind,
    effective_names: impl IntoIterator<Item = RuntimeString>,
    candidates: &[NamedSource],
    suppressed: &mut Vec<SuppressedExtension>,
) -> Vec<EffectiveExtension> {
    let mut winners: HashMap<RuntimeString, usize> = HashMap::new();
    for (index, candidate) in candidates.iter().enumerate() {
        winners.insert(candidate.name.clone(), index);
    }
    for (index, candidate) in candidates.iter().enumerate() {
        let Some(&winner_index) = winners.get(&candidate.name) else {
            continue;
        };
        if winner_index != index {
            suppressed.push(SuppressedExtension {
                kind,
                name: candidate.name.clone(),
                source: candidate.source.clone(),
                replaced_by: candidates[winner_index].source.clone(),
                reason: RuntimeString::from("overridden_by_higher_precedence_source"),
            });
        }
    }
    effective_names
        .into_iter()
        .filter_map(|name| {
            winners.get(&name).map(|index| EffectiveExtension {
                name,
                source: candidates[*index].source.clone(),
            })
        })
        .collect()
}

fn ordered_pricing_from_map(pricing: &HashMap<String, ModelPricing>) -> PricingTable {
    let mut entries = pricing
        .iter()
        .map(|(model, pricing)| (model.clone(), *pricing))
        .collect::<Vec<_>>();
    entries.sort_by(|a, b| {
        b.0.len()
            .cmp(&a.0.len())
            .then_with(|| a.0.to_ascii_lowercase().cmp(&b.0.to_ascii_lowercase()))
            .then_with(|| a.0.cmp(&b.0))
    });
    entries
}

/// Top-level factory: configures defaults, mints sessions.
pub struct Agent {
    opts: AgentOptions,
    extension_sources: AgentExtensionSources,
    plugin_registry: PluginRegistry,
    runtime_capabilities: RuntimeCapabilityRegistry,
    slash_commands: Vec<Command>,
    plugin_mcp_servers: Vec<McpServerConfig>,
    refreshed_plugins: RwLock<Option<Arc<RefreshedPlugins>>>,
    plugin_sessions: Mutex<Vec<SessionPluginBinding>>,
    plugin_management_handler: Arc<OnceLock<PluginManageFn>>,
    host_lsp_servers: Option<LspServersThunk>,
    /// Host deps overrides in shareable form (the boxed model-call seam is not
    /// cloneable, so it is Arc-wrapped once and re-boxed per session).
    deps_overrides: SharedDepsOverrides,
}

struct RefreshedPlugins {
    sources: AgentExtensionSources,
    registry: PluginRegistry,
    capabilities: RuntimeCapabilityRegistry,
    slash_commands: Vec<Command>,
    mcp_servers: Vec<McpServerConfig>,
    lsp_servers: Vec<LspServerConfig>,
}

struct SessionPluginBinding {
    session: Weak<Session>,
    session_agents: Vec<AgentDefinition>,
    session_hooks: Vec<Hook>,
    session_skills: Option<SkillsThunk>,
    session_output_styles: Option<OutputStylesThunk>,
    session_lsp_servers: Option<LspServersThunk>,
    host_lsp_servers: Option<LspServersThunk>,
    session_capability_roots: Vec<RuntimeCapabilityRoot>,
    session_browser_runtime: Option<Arc<dyn BrowserRuntime>>,
    browser_tool_overrides: HashSet<RuntimeString>,
    include_builtins: bool,
    host_supplied_tools: bool,
}

impl SessionPluginBinding {
    fn prepare(&self, session: &Arc<Session>, state: &RefreshedPlugins) -> SessionPluginUpdate {
        let sources = &state.sources;
        let session_agents: Vec<Sourced<AgentDefinition>> = self
            .session_agents
            .iter()
            .cloned()
            .map(|value| Sourced {
                value,
                source: ExtensionSource::session(),
            })
            .collect();
        let endpoint = merge_agent_definitions(
            sources
                .plugin_agents
                .iter()
                .map(|item| item.value.clone())
                .collect(),
            sources
                .host_agents
                .iter()
                .map(|item| item.value.clone())
                .collect(),
        );
        let user = merge_agent_definitions(endpoint, self.session_agents.clone());
        let effective = resolve_agent_definitions(Some(user.clone()), self.include_builtins);
        let registry = if self.host_supplied_tools {
            resolve_agent_definitions(Some(user), false)
        } else {
            effective
        };
        let plugin_skills = sources
            .plugin_skills
            .iter()
            .map(|item| item.value.clone())
            .collect::<Vec<_>>();
        let host_skills = sources.host_skills.clone();
        let session_skills = self.session_skills.clone();
        let include_builtins = self.include_builtins;
        let has_skills_source = self.include_builtins
            || !plugin_skills.is_empty()
            || host_skills.is_some()
            || session_skills.is_some();
        let effective_skills = resolve_skills(
            Some(merge_skills(
                merge_skills(
                    plugin_skills,
                    host_skills.as_ref().map(|f| f()).unwrap_or_default(),
                ),
                session_skills.as_ref().map(|f| f()).unwrap_or_default(),
            )),
            include_builtins,
        );
        // Freeze one generation before staging it. A tool or child that already
        // captured this closure must never observe a later host publication.
        let skills: Option<SkillsThunk> =
            has_skills_source.then(|| Arc::new(move || effective_skills.clone()) as SkillsThunk);
        let session_hooks = self
            .session_hooks
            .iter()
            .cloned()
            .map(|value| Sourced {
                value,
                source: ExtensionSource::session(),
            })
            .collect::<Vec<_>>();
        let sourced_hooks = sources
            .plugin_hooks
            .iter()
            .chain(sources.host_hooks.iter())
            .chain(session_hooks.iter())
            .cloned()
            .collect::<Vec<_>>();
        let mut runtime = session.extension_runtime_snapshot();
        runtime.suppressed.clear();
        let mut agent_candidates = Vec::new();
        if self.include_builtins && !self.host_supplied_tools {
            agent_candidates.extend(BUILTIN_AGENTS.iter().map(|agent| NamedSource {
                name: runtime_agent_name_to_extension_snapshot(&agent.name),
                source: ExtensionSource::builtin(),
            }));
        }
        agent_candidates.extend(runtime_agent_source(&sources.plugin_agents));
        agent_candidates.extend(runtime_agent_source(&sources.host_agents));
        agent_candidates.extend(runtime_agent_source(&session_agents));
        runtime.agents = effective_extensions(
            ExtensionKind::Agent,
            registry
                .iter()
                .map(|agent| runtime_agent_name_to_extension_snapshot(&agent.name)),
            &agent_candidates,
            &mut runtime.suppressed,
        );
        let mut skill_candidates = Vec::new();
        if self.include_builtins {
            skill_candidates.extend(builtin_skills().into_iter().map(|skill| NamedSource {
                name: skill.name,
                source: ExtensionSource::builtin(),
            }));
        }
        skill_candidates.extend(named_source(&sources.plugin_skills, |skill| {
            skill.name.clone()
        }));
        skill_candidates.extend(
            sources
                .host_skills
                .as_ref()
                .map(|f| f())
                .unwrap_or_default()
                .into_iter()
                .map(|skill| NamedSource {
                    name: skill.name,
                    source: ExtensionSource::host(),
                }),
        );
        skill_candidates.extend(
            self.session_skills
                .as_ref()
                .map(|f| f())
                .unwrap_or_default()
                .into_iter()
                .map(|skill| NamedSource {
                    name: skill.name,
                    source: ExtensionSource::session(),
                }),
        );
        runtime.skills = effective_extensions(
            ExtensionKind::Skill,
            skills
                .as_ref()
                .map(|f| f())
                .unwrap_or_default()
                .into_iter()
                .map(|skill| skill.name),
            &skill_candidates,
            &mut runtime.suppressed,
        );
        runtime.hooks = sourced_hooks
            .iter()
            .map(|item| {
                let (handler_kind, timeout_ms) = match &item.value.handler {
                    HookHandler::Fn(_) => (HookHandlerKind::Function, None),
                    HookHandler::Command { timeout_ms, .. } => {
                        (HookHandlerKind::Command, *timeout_ms)
                    }
                };
                EffectiveHook {
                    event: item.value.event,
                    matcher: item.value.matcher.clone().map(RuntimeString::from),
                    handler_kind,
                    is_async: item.value.is_async,
                    timeout_ms,
                    source: item.source.clone(),
                }
            })
            .collect();
        let host_styles = sources
            .host_output_styles
            .as_ref()
            .map(|f| f())
            .unwrap_or_default();
        let session_styles = self
            .session_output_styles
            .as_ref()
            .map(|f| f())
            .unwrap_or_default();
        let output_styles = merge_output_styles(
            merge_output_styles(
                sources
                    .plugin_output_styles
                    .iter()
                    .map(|item| item.value.clone())
                    .collect(),
                host_styles.clone(),
            ),
            session_styles.clone(),
        );
        let mut style_candidates =
            named_source(&sources.plugin_output_styles, |style| style.name.clone());
        style_candidates.extend(host_styles.iter().map(|style| NamedSource {
            name: style.name.clone(),
            source: ExtensionSource::host(),
        }));
        style_candidates.extend(session_styles.iter().map(|style| NamedSource {
            name: style.name.clone(),
            source: ExtensionSource::session(),
        }));
        runtime.output_styles = effective_extensions(
            ExtensionKind::OutputStyle,
            output_styles.iter().map(|style| style.name.clone()),
            &style_candidates,
            &mut runtime.suppressed,
        );
        let lsp_servers = self
            .session_lsp_servers
            .as_ref()
            .map(|f| f())
            .unwrap_or_else(|| {
                merge_lsp_servers(
                    state.lsp_servers.clone(),
                    self.host_lsp_servers
                        .as_ref()
                        .map(|f| f())
                        .unwrap_or_default(),
                )
            });
        let mut capabilities = state.capabilities.clone();
        for root in &self.session_capability_roots {
            capabilities.register(root.clone());
        }
        if let Some(browser) = &self.session_browser_runtime {
            capabilities.register(
                RuntimeCapabilityRoot::host("session.browser")
                    .with_browser_runtime(browser.clone()),
            );
        }
        runtime.plugin_runtime = state.registry.snapshot();
        SessionPluginUpdate {
            agents: registry,
            skills,
            commands: state.slash_commands.clone(),
            hooks: sourced_hooks.into_iter().map(|item| item.value).collect(),
            plugin_mcp_servers: state.mcp_servers.clone(),
            extension_runtime: runtime,
            output_styles,
            lsp_servers,
            runtime_capabilities: capabilities,
            browser_tool_overrides: self.browser_tool_overrides.clone(),
            host_supplied_tools: self.host_supplied_tools,
        }
    }
}

struct ResumedSessionAdmission {
    session: Arc<Session>,
    accepted: tokio::sync::oneshot::Sender<()>,
}

/// Caller-side cancellation guard for resume admission. Dropping the result
/// receiver wakes the retained owner asynchronously, but Tokio may not poll
/// that owner before a paused clock advances past every shutdown deadline. The
/// caller's own future is dropped synchronously on cancellation, so this guard
/// installs the unpublished-Session cleanup fence and starts retained teardown
/// before cancellation is acknowledged.
struct ResumedSessionCallerGuard {
    session: Arc<Session>,
    armed: bool,
}

impl ResumedSessionCallerGuard {
    fn new(session: Arc<Session>) -> Self {
        Self {
            session,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ResumedSessionCallerGuard {
    fn drop(&mut self) {
        if self.armed {
            self.session.initiate_abandoned_resume_shutdown();
        }
    }
}

/// Cancellation/panic guard for the atomic store lease returned before a
/// resumed Session exists. Every non-commit path retains asynchronous rollout
/// shutdown so a current-PID writer sidecar cannot outlive failed
/// initialization.
struct ResumedRolloutInitGuard {
    rollout: Option<RolloutSink>,
}

impl ResumedRolloutInitGuard {
    fn new(rollout: RolloutSink) -> Self {
        Self {
            rollout: Some(rollout),
        }
    }

    fn rollout(&self) -> RolloutSink {
        self.rollout
            .as_ref()
            .expect("resume rollout init guard")
            .clone()
    }

    fn commit(mut self) {
        self.rollout.take();
    }

    async fn fail(mut self, primary: std::io::Error) -> std::io::Error {
        let Some(rollout) = self.rollout.take() else {
            return primary;
        };
        // The task, not this caller future, owns cleanup. Cancelling the
        // explicit error path merely detaches the JoinHandle and cannot cancel
        // writer drain/sidecar removal.
        let cleanup_owner = tokio::spawn(async move { rollout.shutdown().await });
        match cleanup_owner.await {
            Ok(Ok(())) => primary,
            Ok(Err(cleanup)) => std::io::Error::new(
                primary.kind(),
                format!("{primary}; atomic resume rollout cleanup failed: {cleanup}"),
            ),
            Err(cleanup) => std::io::Error::new(
                primary.kind(),
                format!("{primary}; atomic resume rollout cleanup task failed: {cleanup}"),
            ),
        }
    }
}

impl Drop for ResumedRolloutInitGuard {
    fn drop(&mut self) {
        let Some(rollout) = self.rollout.take() else {
            return;
        };
        // `resume_session` already requires Tokio-backed store I/O. Retain
        // cleanup independently from the cancelled/panicking initialization
        // future; never panic again while unwinding if the runtime is gone.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = rollout.shutdown().await;
            });
        }
    }
}

fn tools_are_paired(messages: &[Message]) -> bool {
    let mut pending = HashSet::new();
    for message in messages {
        match message {
            Message::Assistant(assistant) => {
                for block in &assistant.content {
                    if let ContentBlock::ToolUse(call) = block {
                        if !pending.insert(&call.id) {
                            return false;
                        }
                    }
                }
            }
            Message::ToolResult(result) => {
                if !pending.remove(&result.tool_use_id) {
                    return false;
                }
            }
            _ => {}
        }
    }
    pending.is_empty()
}

pub(crate) fn completed_context_prefix(messages: &[Message], turns: &TurnIndex) -> Vec<Message> {
    // Retain earlier discussion and compaction ancestry through a completed
    // boundary. Incomplete indexed turns cannot contribute dangling tool calls.
    let checkpoint = messages
        .iter()
        .rposition(|message| matches!(message, Message::CompactBoundary(_)));
    let prefix = |end: usize| {
        let end = end.max(checkpoint.map_or(0, |position| position + 1));
        let start = checkpoint.unwrap_or(0);
        messages[start..end]
            .iter()
            .filter(|message| {
                matches!(message, Message::CompactBoundary(_))
                    || turns
                        .message_turn
                        .get(message_uuid(message))
                        .is_none_or(|turn_id| {
                            turns
                                .turn_status
                                .get(turn_id)
                                .is_some_and(|status| status == "completed")
                        })
            })
            .cloned()
            .collect::<Vec<_>>()
    };
    // Completion belongs to lifecycle state; results may follow the final reply.
    let mut checked = HashSet::new();
    for (position, message) in messages.iter().enumerate().rev() {
        let Some(turn_id) = turns.message_turn.get(message_uuid(message)) else {
            continue;
        };
        if !checked.insert(turn_id)
            || turns
                .turn_status
                .get(turn_id)
                .is_none_or(|status| status != "completed")
        {
            continue;
        }
        let candidate: Vec<Message> = messages
            .iter()
            .filter(|message| turns.message_turn.get(message_uuid(message)) == Some(turn_id))
            .cloned()
            .collect();
        if candidate
            .iter()
            .any(|message| matches!(message, Message::Assistant(_)))
            && tools_are_paired(&candidate)
        {
            let inherited = prefix(position + 1);
            if tools_are_paired(&inherited) {
                return inherited;
            }
        }
    }

    // Older imports have no lifecycle metadata. Accept only a user-delimited
    // turn with a tool-free final answer and every call/result still paired.
    let mut end = messages.len();
    while let Some(start) = messages[..end].iter().rposition(|message| {
        matches!(message, Message::User(user) if user.is_meta != Some(true) && user.question_answer.is_none())
    }) {
        let candidate = &messages[start..end];
        end = start;
        if candidate.iter().any(|message| turns.message_turn.contains_key(message_uuid(message))) {
            continue;
        }
        let final_answer = candidate.iter().rev().find_map(|message| match message {
            Message::Assistant(assistant) => Some(assistant),
            _ => None,
        });
        if final_answer.is_some_and(|assistant| {
            !assistant.content.iter().any(|block| matches!(block, ContentBlock::ToolUse(_)))
        }) && tools_are_paired(candidate)
        {
            let inherited = prefix(start + candidate.len());
            if tools_are_paired(&inherited) {
                return inherited;
            }
        }
    }
    checkpoint.map_or_else(Vec::new, |position| prefix(position + 1))
}

fn message_uuid(message: &Message) -> &RuntimeString {
    match message {
        Message::User(message) => &message.uuid,
        Message::Assistant(message) => &message.uuid,
        Message::ToolResult(message) => &message.uuid,
        Message::System(message) => &message.uuid,
        Message::CompactBoundary(message) => &message.uuid,
    }
}

impl Agent {
    /// Build an agent from options. Resolves the effective sub-agent roles once
    /// and, when `process_hardening` is set, strips loader-injection env vars from
    /// this process so no later child spawn inherits them.
    pub fn new(mut opts: AgentOptions) -> Self {
        let mut plugin_load_report = opts.plugin_load_report.take();
        if opts.plugins.is_empty() {
            if let Some(report) = plugin_load_report.as_mut() {
                opts.plugins = std::mem::take(&mut report.enabled);
            }
        }
        let extension_sources = AgentExtensionSources::from_options(&opts);
        let plugin_mcp_resolution =
            plugin_mcp_server_configs(&opts.plugins, &opts.mcp_server_configs);
        let mut plugin_registry = PluginRegistry::new();
        for plugin in &opts.plugins {
            plugin_registry
                .register(plugin)
                .unwrap_or_else(|err| panic!("plugin registration failed: {err:?}"));
            if let Some(effective_servers) = plugin_mcp_resolution
                .effective_server_names_by_plugin
                .get(&plugin.name)
            {
                plugin_registry.replace_plugin_mcp_scope(&plugin.name, effective_servers.clone());
            }
        }
        if let Some(report) = &plugin_load_report {
            plugin_registry.record_load_issues(report);
        }
        plugin_registry.record_warnings(plugin_mcp_resolution.warnings.clone());
        let runtime_capabilities = runtime_capability_registry_from_options(
            &opts,
            &plugin_mcp_resolution.effective_server_names_by_plugin,
        );
        plugin_registry.record_runtime_capabilities(runtime_capabilities.descriptors());
        let slash_commands = plugin_slash_commands(&opts.plugins);
        let plugin_mcp_servers = plugin_mcp_resolution.servers;
        let host_lsp_servers = opts.lsp_servers.clone();
        merge_plugin_capabilities_into_options(&mut opts);
        // Extract the deps overrides into shareable fields (the model-call box is not
        // cloneable, so it is Arc-wrapped once and reused across every session).
        let deps_overrides = SharedDepsOverrides::from_overrides(std::mem::take(&mut opts.deps));
        // Opt-in: strip loader-injection env vars from this process (belt-and-suspenders
        // over per-spawn scrubEnv). Global + idempotent.
        if opts
            .config
            .as_ref()
            .and_then(|v| v.get("processHardening"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            let mut env: std::collections::HashMap<String, String> = std::env::vars().collect();
            let result = harden_process(&mut env);
            for name in &result.stripped {
                crate::runtime_process::remove_process_environment_var(name);
            }
        }
        Agent {
            opts,
            extension_sources,
            plugin_registry,
            runtime_capabilities,
            slash_commands,
            plugin_mcp_servers,
            refreshed_plugins: RwLock::new(None),
            plugin_sessions: Mutex::new(Vec::new()),
            plugin_management_handler: Arc::new(OnceLock::new()),
            host_lsp_servers,
            deps_overrides,
        }
    }

    /// Snapshot the loaded plugin runtime registry.
    pub fn plugin_snapshot(&self) -> Vec<PluginRuntimeEntry> {
        self.refreshed_plugins
            .read()
            .expect("plugin registry lock poisoned")
            .as_ref()
            .map(|state| state.registry.list())
            .unwrap_or_else(|| self.plugin_registry.list())
    }

    /// Snapshot the loaded plugin runtime state, including disabled plugins and
    /// loader warnings/errors for host system-init or diagnostics surfaces.
    pub fn set_plugin_management_handler(&self, handler: PluginManageFn) -> Result<(), String> {
        self.plugin_management_handler
            .set(handler)
            .map_err(|_| "plugin management handler is already configured".to_string())
    }

    pub fn plugin_runtime_snapshot(&self) -> PluginRuntimeSnapshot {
        self.refreshed_plugins
            .read()
            .expect("plugin registry lock poisoned")
            .as_ref()
            .map(|state| state.registry.snapshot())
            .unwrap_or_else(|| self.plugin_registry.snapshot())
    }

    /// Publish a complete plugin generation to existing sessions and future
    /// sessions. A running session commits its staged generation at the next
    /// unoccupied turn-admission boundary.
    pub fn refresh_plugins(&self, report: PluginLoadReport) -> Result<(), String> {
        let plugins = &report.enabled;
        let mcp = plugin_mcp_server_configs(plugins, &self.opts.mcp_server_configs);
        let mut registry = PluginRegistry::new();
        for plugin in plugins {
            registry
                .register(plugin)
                .map_err(|error| format!("plugin registration failed: {error:?}"))?;
            if let Some(names) = mcp.effective_server_names_by_plugin.get(&plugin.name) {
                registry.replace_plugin_mcp_scope(&plugin.name, names.clone());
            }
        }
        registry.record_load_issues(&report);
        registry.record_warnings(mcp.warnings);
        let capabilities = runtime_capability_registry_from_plugins(
            &self.opts,
            plugins,
            &mcp.effective_server_names_by_plugin,
        );
        registry.record_runtime_capabilities(capabilities.descriptors());
        let state = RefreshedPlugins {
            sources: self.extension_sources.with_plugins(plugins),
            registry,
            capabilities,
            slash_commands: plugin_slash_commands(plugins),
            mcp_servers: mcp.servers,
            lsp_servers: plugin_lsp_server_configs(plugins),
        };
        let mut sessions = self
            .plugin_sessions
            .lock()
            .expect("plugin session registry lock poisoned");
        sessions.retain(|binding| {
            binding
                .session
                .upgrade()
                .is_some_and(|session| !session.is_disposed())
        });
        let prepared = sessions
            .iter()
            .filter_map(|binding| {
                binding
                    .session
                    .upgrade()
                    .map(|session| (binding.prepare(&session, &state), session))
            })
            .collect::<Vec<_>>();
        for (update, session) in &prepared {
            let mut names = std::collections::HashSet::new();
            for server in merge_session_mcp_servers(
                session.manual_mcp_servers(),
                update.plugin_mcp_servers.clone(),
            ) {
                if !names.insert(server.name().to_string()) {
                    return Err(format!("duplicate MCP server name: {}", server.name()));
                }
            }
        }
        for (update, session) in prepared {
            session.stage_plugin_update(update)?;
        }
        *self
            .refreshed_plugins
            .write()
            .expect("plugin registry lock poisoned") = Some(Arc::new(state));
        Ok(())
    }

    /// Replace the host-discovered catalog without recreating a loaded session.
    /// Existing turns retain their captured generation; admission of the next
    /// turn publishes the replacement, including an explicitly empty catalog.
    pub fn replace_session_skills(
        &self,
        session: &Arc<Session>,
        skills: Vec<Skill>,
    ) -> Result<(), String> {
        let mut sessions = self
            .plugin_sessions
            .lock()
            .expect("plugin session registry lock poisoned");
        let binding = sessions
            .iter_mut()
            .find(|binding| {
                binding
                    .session
                    .upgrade()
                    .is_some_and(|candidate| Arc::ptr_eq(&candidate, session))
            })
            .ok_or_else(|| "session does not accept host skill updates".to_string())?;
        let refreshed = self
            .refreshed_plugins
            .read()
            .expect("plugin registry lock poisoned")
            .clone();
        let initial;
        let state = if let Some(state) = refreshed.as_deref() {
            state
        } else {
            initial = RefreshedPlugins {
                sources: self.extension_sources.clone(),
                registry: self.plugin_registry.clone(),
                capabilities: self.runtime_capabilities.clone(),
                slash_commands: self.slash_commands.clone(),
                mcp_servers: self.plugin_mcp_servers.clone(),
                lsp_servers: plugin_lsp_server_configs(&self.opts.plugins),
            };
            &initial
        };
        let previous = binding.session_skills.take();
        binding.session_skills = Some(Arc::new(move || skills.clone()));
        let update = binding.prepare(session, state);
        if let Err(error) = session.stage_plugin_update(update) {
            binding.session_skills = previous;
            return Err(error);
        }
        Ok(())
    }

    pub fn has_active_plugin_contexts(&self) -> bool {
        self.plugin_sessions
            .lock()
            .expect("plugin session registry lock poisoned")
            .iter()
            .filter_map(|binding| binding.session.upgrade())
            .any(|session| session.has_plugin_captured_contexts())
    }

    pub async fn wait_for_loaded_plugin_contexts(&self) {
        let sessions = self
            .plugin_sessions
            .lock()
            .expect("plugin session registry lock poisoned")
            .iter()
            .filter_map(|binding| binding.session.upgrade())
            .collect::<Vec<_>>();
        for session in sessions {
            session.wait_for_plugin_captured_contexts().await;
        }
    }

    pub fn runtime_capability_descriptors(
        &self,
    ) -> Vec<crate::capabilities::RuntimeCapabilityDescriptor> {
        self.refreshed_plugins
            .read()
            .expect("plugin registry lock poisoned")
            .as_ref()
            .map(|state| state.capabilities.descriptors())
            .unwrap_or_else(|| self.runtime_capabilities.descriptors())
    }

    fn runtime_capability_registry_for_session(
        &self,
        sopts: &StartSessionOptions,
    ) -> RuntimeCapabilityRegistry {
        let mut registry = self
            .refreshed_plugins
            .read()
            .expect("plugin registry lock poisoned")
            .as_ref()
            .map(|state| state.capabilities.clone())
            .unwrap_or_else(|| self.runtime_capabilities.clone());
        if let Some(roots) = &sopts.capability_roots {
            for root in roots {
                registry.register(root.clone());
            }
        }
        if let Some(runtime) = &sopts.browser_runtime {
            registry.register(
                RuntimeCapabilityRoot::host("session.browser")
                    .with_browser_runtime(runtime.clone()),
            );
        }
        registry
    }

    fn runtime_capabilities_for_session(&self, sopts: &StartSessionOptions) -> RuntimeCapabilities {
        self.runtime_capability_registry_for_session(sopts)
            .resolve_selected(sopts.selected_capability_roots.as_deref())
            .capabilities
    }

    /// One stored config file as a governance layer. A missing file is an empty
    /// layer ("nothing configured" and "every default" are the same state); a
    /// malformed one is fatal, because starting on defaults would silently
    /// discard everything the user had configured and look like a reset.
    fn stored_config_layer(path: &Path, source: ConfigGovernanceSource) -> ConfigGovernanceLayer {
        match config_store::load_blocking(path) {
            Ok(document) => document.engine_layer(source),
            Err(error) => panic!("invalid engine config: {error}"),
        }
    }

    /// The stored on-disk layers (user config, then project config). Under
    /// test these are read only when the test explicitly redirected the config
    /// home (`KLAUS_CONFIG_DIR`, see `TestConfigHome`): otherwise every unit
    /// test would inherit whatever the developer has enabled in their real
    /// `~/.klaus/config.toml` (e.g. autoMemory), making the suite depend on
    /// the machine it runs on.
    fn stored_config_layers(&self, sopts: &StartSessionOptions) -> Vec<ConfigGovernanceLayer> {
        #[cfg(test)]
        {
            let redirected = std::env::var("KLAUS_CONFIG_DIR")
                .map(|value| !value.is_empty())
                .unwrap_or(false);
            if !redirected {
                return Vec::new();
            }
        }
        let mut layers = vec![Self::stored_config_layer(
            &config_store::user_config_path(),
            ConfigGovernanceSource::User,
        )];
        // A cwd that is not valid UTF-8 cannot name a file on disk, so there is
        // no project config to read for it.
        if let Some(cwd) = sopts
            .cwd
            .as_ref()
            .and_then(|cwd| cwd.as_runtime_string().try_to_utf8().ok())
        {
            layers.push(Self::stored_config_layer(
                &config_store::project_config_path(Path::new(&cwd)),
                ConfigGovernanceSource::Project,
            ));
        }
        layers
    }

    /// Resolve the session config and default the MODEL from the provider when the
    /// host didn't pin one — so the default is coherent per provider instead of
    /// the schema's constant.
    ///
    /// The layers, lowest first: the engine's own `config.toml` (User), the
    /// project's `<cwd>/.klaus/config.toml`, then the host's per-session flags.
    /// The stored files are read HERE rather than handed in by the host, because
    /// a host that re-derives them owns the same fact twice: its idea of a gate
    /// and the engine's schema drift with nothing to catch it, and a settings
    /// change only lands on the next session.
    fn resolve_session_config(&self, sopts: &StartSessionOptions) -> ResolvedConfig {
        let provider = sopts.provider.as_ref().unwrap_or(&self.opts.provider);
        let mut layers = self.stored_config_layers(sopts);
        let config_base = sopts
            .cwd
            .as_ref()
            .map(|cwd| {
                crate::fs::node_path::project_runtime_path(cwd, "session configuration base")
                    .unwrap_or_else(|error| panic!("invalid session configuration base: {error:?}"))
            })
            .or_else(|| self.opts.cwd.as_ref().map(std::path::PathBuf::from));

        // The host's partial config is a JSON object of the (camelCase) keys it set;
        // absent keys fall back to the stored layers and then the schema defaults.
        // It rides the SessionFlags layer because that is what it is: what THIS
        // host asked for on top of what the user configured.
        let partial = self
            .opts
            .config
            .clone()
            .unwrap_or_else(|| Value::Object(serde_json::Map::new()));
        layers.push(
            crate::engine::session_config::config_layer_from_value_with_base(
                ConfigGovernanceSource::SessionFlags,
                partial,
                config_base.as_deref(),
            )
            .unwrap_or_else(|e| panic!("invalid engine config: {e}")),
        );

        if let Some(partial) = &sopts.config {
            let layer = crate::engine::session_config::config_layer_from_value_with_base(
                ConfigGovernanceSource::SessionFlags,
                partial.clone(),
                config_base.as_deref(),
            )
            .unwrap_or_else(|e| panic!("invalid session engine config: {e}"));
            layers.push(layer);
        }
        // Decode retired session fields before defaults obscure key presence.
        // Any explicit canonical policy owns permission selection.
        let legacy_plan = sopts.permission_mode == Some(ExternalPermissionMode::Plan);
        let explicit_policy = layers.iter().any(|layer| {
            ["approval_policy", "sandbox_mode", "approvals_reviewer"]
                .iter()
                .any(|key| layer.values.contains_key(*key))
        });
        let legacy_preset = if legacy_plan {
            sopts.pre_plan_permission_mode
        } else {
            sopts.permission_mode
        };
        if !explicit_policy {
            if let Some((approval, sandbox, reviewer)) =
                legacy_preset.and_then(crate::schemas::config::legacy_permission_policy)
            {
                layers.push(
                    crate::engine::session_config::config_layer_from_value(
                        ConfigGovernanceSource::SessionFlags,
                        serde_json::json!({
                            "approval_policy": approval,
                            "sandbox_mode": sandbox,
                            "approvals_reviewer": reviewer,
                        }),
                    )
                    .unwrap_or_else(|message| panic!("invalid legacy session policy: {message}")),
                );
            }
        }
        // An invalid config is fatal: `start_session` is not fallible, mirroring the
        // engine's fail-fast config resolution.
        let mut config =
            resolve_config_layers(layers).unwrap_or_else(|e| panic!("invalid engine config: {e}"));
        crate::context::compact::window::bind_context_runtime_environment(
            &mut config,
            &crate::runtime_process::ParentEnvSnapshot::capture(),
        );
        if legacy_plan {
            config.permission_mode = ExternalPermissionMode::Plan;
        }
        // Default the MODEL from the provider when the host didn't pin one — so the
        // default is coherent per provider instead of the schema's constant.
        let agent_pinned_model = self
            .opts
            .config
            .as_ref()
            .and_then(|v| v.get("model"))
            .map(|m| !m.is_null())
            .unwrap_or(false);
        let session_pinned_model = sopts
            .config
            .as_ref()
            .and_then(|v| v.get("model"))
            .map(|m| !m.is_null())
            .unwrap_or(false);
        if agent_pinned_model || session_pinned_model {
            config
        } else {
            ResolvedConfig {
                model: provider.info().default_model.clone(),
                ..config
            }
        }
    }

    /// Start a session from these options.
    pub fn start_session(&self, sopts: StartSessionOptions) -> Arc<Session> {
        self.try_start_session(sopts)
            .unwrap_or_else(|error| panic!("{error}"))
    }

    pub fn try_start_session(
        &self,
        sopts: StartSessionOptions,
    ) -> Result<Arc<Session>, SessionInitError> {
        self.try_start_session_with_queue(sopts, None, None)
    }

    /// Starts a session after its authenticated administrator bundle is admitted.
    pub async fn try_start_session_async(
        &self,
        sopts: StartSessionOptions,
    ) -> Result<Arc<Session>, SessionInitError> {
        self.try_start_session_with_queue_async(sopts, None).await
    }

    async fn try_start_session_with_queue_async(
        &self,
        sopts: StartSessionOptions,
        initial_queue: Option<(MessageQueue, bool)>,
    ) -> Result<Arc<Session>, SessionInitError> {
        let provider = sopts
            .provider
            .clone()
            .unwrap_or_else(|| self.opts.provider.clone());
        let prepared = super::session::Session::prepare_review_requirements_for_startup(
            provider,
            sopts.user_config_dir.as_ref(),
        )
        .await?;
        self.try_start_session_with_queue(sopts, initial_queue, Some(prepared))
    }

    fn agent_definitions_for_session(
        &self,
        sopts: &StartSessionOptions,
        sources: &AgentExtensionSources,
    ) -> (Vec<AgentDefinition>, Vec<AgentDefinition>) {
        let endpoint = merge_agent_definitions(
            sources
                .plugin_agents
                .iter()
                .map(|item| item.value.clone())
                .collect(),
            sources
                .host_agents
                .iter()
                .map(|item| item.value.clone())
                .collect(),
        );
        let user = merge_agent_definitions(endpoint, sopts.agents.clone().unwrap_or_default());
        let effective = resolve_agent_definitions(
            Some(user.clone()),
            self.opts.include_default_agents != Some(false),
        );
        // A host owning the task tool also owns its registry, without injected built-ins.
        let registry = if sopts.tools.is_some() || self.opts.tools.is_some() {
            resolve_agent_definitions(Some(user), false)
        } else {
            effective.clone()
        };
        (registry, effective)
    }

    fn resolve_root_agent_definition(
        name: Option<&RuntimeString>,
        definitions: &[AgentDefinition],
        config: &mut ResolvedConfig,
    ) -> Result<Option<AgentDefinition>, SessionInitError> {
        name.map(|name| {
            let registry =
                crate::engine::agent_definitions::AgentRegistry::new(definitions.to_vec());
            let definition =
                registry
                    .get(name)
                    .cloned()
                    .ok_or_else(|| SessionInitError::AgentDefinition {
                        message: format!("unknown agent definition {name:?}"),
                    })?;
            definition
                .apply_root_config(config)
                .map_err(|message| SessionInitError::AgentDefinition { message })?;
            Ok(definition)
        })
        .transpose()
    }

    /// Resume-only construction path. The Host options never carry queue state;
    /// this queue can only be rebuilt from the session's own rollout.
    fn try_start_session_with_queue(
        &self,
        mut sopts: StartSessionOptions,
        initial_queue: Option<(MessageQueue, bool)>,
        prepared: Option<super::session::PreparedReviewRequirements>,
    ) -> Result<Arc<Session>, SessionInitError> {
        let legacy_plan = sopts.permission_mode == Some(ExternalPermissionMode::Plan);
        if sopts.pre_plan_permission_mode == Some(ExternalPermissionMode::Plan) {
            return Err(SessionInitError::SandboxSelection {
                message: "pre-Plan permission mode cannot itself be Plan".into(),
            });
        }
        if sopts.pre_plan_permission_mode.is_some() && !legacy_plan {
            return Err(SessionInitError::SandboxSelection {
                message: "prePlanPermissionMode is valid only for a legacy planning input".into(),
            });
        }
        let mut plugin_sessions = self
            .plugin_sessions
            .lock()
            .expect("plugin session registry lock poisoned");
        let refreshed = self
            .refreshed_plugins
            .read()
            .expect("plugin registry lock poisoned")
            .clone();
        let sources = refreshed
            .as_ref()
            .map(|state| &state.sources)
            .unwrap_or(&self.extension_sources);
        let provider = sopts
            .provider
            .clone()
            .unwrap_or_else(|| self.opts.provider.clone());
        let mut config = self.resolve_session_config(&sopts);
        let task_scoped = config.task_resource_scope.is_some();
        let isolated_sources = AgentExtensionSources::default();
        let sources = if task_scoped {
            &isolated_sources
        } else {
            sources
        };
        if task_scoped {
            config.output_style = None;
            config.memory.enabled = false;
            config.memory.user = false;
            config.memory.project = false;
            config.memory.local = false;
            config.auto_memory = false;
            config.agent_memory = false;
            config.memory_recall = false;
            config.agent_memory_snapshot = false;
            config.auto_dream = false;
            config.fork = false;
            config.worktree = false;
            sopts.agents = Some(Vec::new());
            sopts.agent_definition_name = None;
            sopts.hooks = Some(Vec::new());
            sopts.skills = None;
            sopts.output_styles = None;
            sopts.selected_output_style = None;
            sopts.shell_env = None;
        }
        if let Some(selected) = &sopts.selected_output_style {
            config.output_style = Some(selected.clone());
        }
        let cwd = sopts
            .cwd
            .clone()
            .or_else(|| self.opts.cwd.clone().map(RuntimePath::from))
            .unwrap_or_else(default_cwd);
        // Resolve every named extension from its actual tier. Session definitions
        // override endpoint definitions, which override plugins and built-ins.
        let session_agents = sopts
            .agents
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(|value| Sourced {
                value,
                source: ExtensionSource::session(),
            })
            .collect::<Vec<_>>();
        let include_builtins = !task_scoped && self.opts.include_default_agents != Some(false);
        let (registry_agents, effective_agents) = if task_scoped {
            (Vec::new(), Vec::new())
        } else {
            self.agent_definitions_for_session(&sopts, sources)
        };
        let host_supplied_tools = sopts.tools.is_some() || self.opts.tools.is_some();
        let manual_mcp_servers = sopts.mcp_server_configs.clone().unwrap_or_default();
        let mut plugin_binding = SessionPluginBinding {
            session: Weak::new(),
            session_agents: sopts.agents.clone().unwrap_or_default(),
            session_hooks: sopts.hooks.clone().unwrap_or_default(),
            session_skills: sopts.skills.clone(),
            session_output_styles: sopts.output_styles.clone(),
            session_lsp_servers: sopts.lsp_servers.clone(),
            host_lsp_servers: self.host_lsp_servers.clone(),
            session_capability_roots: sopts.capability_roots.clone().unwrap_or_default(),
            session_browser_runtime: sopts.browser_runtime.clone(),
            browser_tool_overrides: self
                .opts
                .mcp_tools
                .iter()
                .flatten()
                .chain(sopts.mcp_tools.iter().flatten())
                .map(|tool| tool.name())
                .collect(),
            include_builtins,
            host_supplied_tools,
        };
        let root_agent_definition = Self::resolve_root_agent_definition(
            sopts.agent_definition_name.as_ref(),
            &registry_agents,
            &mut config,
        )?;
        let deps = self.build_deps(provider.clone(), &config);

        // Skills remain live between turns. Build one thunk from all tiers so a
        // session injection augments endpoint/plugin skills instead of replacing them.
        let plugin_skills = sources
            .plugin_skills
            .iter()
            .map(|item| item.value.clone())
            .collect::<Vec<_>>();
        let host_skills = sources.host_skills.clone();
        let session_skills = sopts.skills.clone();
        let initial_host_skills = host_skills.as_ref().map(|f| f()).unwrap_or_default();
        let initial_session_skills = session_skills.as_ref().map(|f| f()).unwrap_or_default();
        let initial_user_skills = merge_skills(
            merge_skills(plugin_skills.clone(), initial_host_skills.clone()),
            initial_session_skills.clone(),
        );
        let initial_effective_skills = resolve_skills(Some(initial_user_skills), include_builtins);
        let skills: Option<SkillsThunk> = if include_builtins
            || !plugin_skills.is_empty()
            || host_skills.is_some()
            || session_skills.is_some()
        {
            let plugin = plugin_skills.clone();
            let host = host_skills.clone();
            let session = session_skills.clone();
            Some(Arc::new(move || {
                let endpoint = merge_skills(
                    plugin.clone(),
                    host.as_ref().map(|f| f()).unwrap_or_default(),
                );
                let user =
                    merge_skills(endpoint, session.as_ref().map(|f| f()).unwrap_or_default());
                resolve_skills(Some(user), include_builtins)
            }))
        } else {
            None
        };
        // Output styles are resolved once by Session, so evaluate all three
        // external tiers once and freeze exactly the list used for selection.
        let plugin_output_styles = sources
            .plugin_output_styles
            .iter()
            .map(|item| item.value.clone())
            .collect::<Vec<_>>();
        let initial_host_output_styles = sources
            .host_output_styles
            .as_ref()
            .map(|f| f())
            .unwrap_or_default();
        let initial_session_output_styles = sopts
            .output_styles
            .as_ref()
            .map(|f| f())
            .unwrap_or_default();
        let effective_output_styles = merge_output_styles(
            merge_output_styles(plugin_output_styles, initial_host_output_styles.clone()),
            initial_session_output_styles.clone(),
        );
        let output_styles: Option<OutputStylesThunk> = if sources.host_output_styles.is_some()
            || sopts.output_styles.is_some()
            || !effective_output_styles.is_empty()
        {
            let frozen = effective_output_styles.clone();
            Some(Arc::new(move || frozen.clone()))
        } else {
            None
        };

        // Named workflows come from three tiers, first-wins: host-programmatic > disk
        // (`.klaus/workflows`) > built-ins. Discovery shares the include-defaults
        // policy; the retired workflow flag no longer removes disk programs.
        let host_workflows = sopts
            .workflows
            .clone()
            .or_else(|| self.opts.workflows.clone())
            .filter(|_| !task_scoped);
        let include_builtin_workflows =
            !task_scoped && self.opts.include_default_agents != Some(false);
        let workflows: Option<WorkflowsThunk> =
            if include_builtin_workflows || host_workflows.is_some() {
                let host = host_workflows.clone();
                let cwd_owned = runtime_path_to_workflow_discovery_host(&cwd);
                Some(Arc::new(move || {
                    let host_list = host.as_ref().map(|h| h()).unwrap_or_default();
                    let names: HashSet<RuntimeString> =
                        host_list.iter().map(|w| w.name.clone()).collect();
                    let disk: Vec<WorkflowRef> = if include_builtin_workflows {
                        discover_workflows(&cwd_owned)
                            .into_iter()
                            .filter(|w| !names.contains(&w.name))
                            .collect()
                    } else {
                        Vec::new()
                    };
                    let mut combined = host_list;
                    combined.extend(disk);
                    resolve_workflows(Some(combined), include_builtin_workflows)
                }))
            } else {
                None
            };
        let lsp_servers = sopts
            .lsp_servers
            .clone()
            .or_else(|| {
                refreshed.as_ref().map_or_else(
                    || self.opts.lsp_servers.clone(),
                    |state| {
                        let plugin = state.lsp_servers.clone();
                        let host = self.host_lsp_servers.clone();
                        Some(Arc::new(move || {
                            merge_lsp_servers(
                                plugin.clone(),
                                host.as_ref().map(|thunk| thunk()).unwrap_or_default(),
                            )
                        }) as LspServersThunk)
                    },
                )
            })
            .filter(|_| !task_scoped);
        let mcp_catalog = sopts
            .mcp_catalog
            .clone()
            .or_else(|| self.opts.mcp_catalog.clone())
            .filter(|_| !task_scoped);
        let plugin_catalog = sopts
            .plugin_catalog
            .clone()
            .or_else(|| self.opts.plugin_catalog.clone())
            .filter(|_| !task_scoped);
        let plugin_management_home = sopts.plugin_home.clone();
        let agent_memory = sopts
            .agent_memory
            .clone()
            .or_else(|| self.opts.agent_memory.clone())
            .filter(|_| !task_scoped);
        let mcp_clients = if task_scoped {
            sopts.mcp_clients.clone()
        } else {
            sopts
                .mcp_clients
                .clone()
                .or_else(|| self.opts.mcp_clients.clone())
        };
        // One conditional-skill gate, shared by the skill TOOL and the Session, so a
        // `paths`-gated skill is hidden until a matching file is touched. Only when we
        // build the default tools (a host-supplied tool set owns its own skill tool).
        let skill_gate: Option<Arc<ConditionalSkillGate>> = if !host_supplied_tools {
            Some(Arc::new(create_conditional_skill_gate()))
        } else {
            None
        };
        // MCP tools are APPENDED to the default builtins (not the host-override path).
        let mut mcp_tools: Vec<Arc<dyn Tool>> = if task_scoped {
            Vec::new()
        } else {
            self.opts.mcp_tools.clone().unwrap_or_default()
        };
        mcp_tools.extend(sopts.mcp_tools.clone().unwrap_or_default());
        // Host-injected JS page renderer for web_fetch.
        let web_fetch_page = sopts
            .web_fetch_factory
            .as_ref()
            .map(|factory| factory(config.web_fetch.as_ref()))
            .transpose()
            .map_err(|message| SessionInitError::PageExtraction { message })?
            .flatten()
            .or_else(|| sopts.web_fetch_page.clone())
            .or_else(|| self.opts.web_fetch_page.clone());
        let runtime_capability_registry = if task_scoped {
            let mut registry = RuntimeCapabilityRegistry::default();
            for root in sopts.capability_roots.iter().flatten() {
                registry.register(root.clone());
            }
            if let Some(runtime) = &sopts.browser_runtime {
                registry.register(
                    RuntimeCapabilityRoot::host("session.browser")
                        .with_browser_runtime(runtime.clone()),
                );
            }
            registry
        } else {
            self.runtime_capability_registry_for_session(&sopts)
        };
        let runtime_capabilities = if task_scoped {
            runtime_capability_registry
                .resolve_selected(sopts.selected_capability_roots.as_deref())
                .capabilities
        } else {
            self.runtime_capabilities_for_session(&sopts)
        };
        let browser_capabilities_for_tools = runtime_capabilities.clone();
        let browser_runtime = runtime_capabilities.browser_runtime;

        let mut tools: Vec<Arc<dyn Tool>> = if let Some(t) = sopts.tools.clone() {
            t
        } else if let Some(t) = self.opts.tools.clone().filter(|_| !task_scoped) {
            t
        } else {
            // The default built-in tool set — assembled by the single shared
            // `default_builtins`, gated by config. Host-injected capabilities
            // (agents / skills / gate / workflows / web-fetch page renderer) are
            // threaded through; MCP tools are APPENDED on top (a host that passes
            // its own tool set owns its MCP tools too).
            let mut builtins = default_builtins(
                &config,
                BuiltinsOptions {
                    agents: if effective_agents.is_empty() {
                        None
                    } else {
                        Some(effective_agents.clone())
                    },
                    skills: skills.clone(),
                    skill_gate: skill_gate.clone(),
                    workflows: workflows.clone(),
                    web_fetch_page: web_fetch_page.clone(),
                    browser_runtime: browser_capabilities_for_tools.browser_runtime.clone(),
                    mcp_catalog: mcp_catalog.clone(),
                    plugin_catalog: plugin_catalog.clone(),
                    plugin_management_home,
                    plugin_management_handler: Some(self.plugin_management_handler.clone()),
                    // The carrier for code-mode cells: a host subprocess is
                    // launched from this engine's process context.
                    process_context: Some(self.opts.process_context.clone()),
                    web_search_factory: sopts.web_search_factory.clone(),
                    ..Default::default()
                },
            );
            if task_scoped {
                builtins.retain(|tool| {
                    crate::permission::task_scope::task_scope_builtin_allowed(tool.as_ref())
                });
            }
            // Host-supplied extra tools (MCP wrappers and protocol dynamic tools)
            // OWN their names: a same-named builtin is dropped before the extend,
            // so the model never sees two tools answering to one name. In practice
            // only dynamic tools can collide — MCP wrappers are namespaced
            // `mcp__<server>__…` — and shadowing is exactly what a host declaring
            // e.g. its own `web_search` wants: the builtin (with its injected-or-
            // default backend) yields to the host implementation wholesale.
            if !mcp_tools.is_empty() {
                let host_owned: HashSet<RuntimeString> =
                    mcp_tools.iter().map(|tool| tool.name()).collect();
                builtins.retain(|tool| !host_owned.contains(&tool.name()));
            }
            builtins.extend(mcp_tools);
            if !config.disabled_tools.is_empty() {
                let disabled: HashSet<RuntimeString> =
                    config.disabled_tools.iter().cloned().collect();
                builtins.retain(|tool| !disabled.contains(&tool.name()));
            }
            ensure_tool_search(builtins)
        };
        if task_scoped {
            tools.retain(|tool| {
                crate::permission::task_scope::task_scope_tool_allowed(tool.as_ref())
            });
        }
        if let Some(definition) = &root_agent_definition {
            let available = tools.clone();
            definition.retain_tools(&mut tools);
            crate::engine::browser_tool_dependencies::retain_browser_tool_dependencies(
                &mut tools,
                available,
                browser_capabilities_for_tools.browser_tool_mapping.as_ref(),
                definition.disallowed_tools.as_deref().unwrap_or_default(),
                &config.disabled_tools,
            );
        }
        let approval_handler = sopts
            .approval_handler
            .clone()
            .or_else(|| self.opts.approval_handler.clone());
        let approval_reviewer = sopts
            .approval_reviewer
            .clone()
            .or_else(|| self.opts.approval_reviewer.clone());
        let elicit_handler = sopts
            .elicit_handler
            .clone()
            .or_else(|| self.opts.elicit_handler.clone());
        let credential_store = sopts
            .credential_store
            .clone()
            .or_else(|| self.opts.credential_store.clone());
        let session_hooks = sopts
            .hooks
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(|value| Sourced {
                value,
                source: ExtensionSource::session(),
            })
            .collect::<Vec<_>>();
        let sourced_hooks = sources
            .plugin_hooks
            .iter()
            .chain(sources.host_hooks.iter())
            .chain(session_hooks.iter())
            .cloned()
            .collect::<Vec<_>>();
        let hooks = sourced_hooks
            .iter()
            .map(|item| item.value.clone())
            .collect::<Vec<_>>();
        let hook_bus = if hooks.is_empty() {
            None
        } else {
            Some(Arc::new(HookBus::new(
                self.opts.process_context.clone(),
                hooks,
            )))
        };
        // Per-session override wins, else the agent-level default.
        let refresh_tools = sopts
            .refresh_tools
            .clone()
            .or_else(|| self.opts.refresh_tools.clone())
            .filter(|_| !task_scoped);
        let system_prompt_provider = sopts
            .system_prompt_provider
            .clone()
            .or_else(|| self.opts.system_prompt_provider.clone())
            .filter(|_| !task_scoped);
        let context_window_override = sopts
            .config
            .as_ref()
            .and_then(|v| v.get("contextWindow"))
            .or_else(|| {
                self.opts
                    .config
                    .as_ref()
                    .and_then(|v| v.get("contextWindow"))
            })
            .and_then(|v| v.as_u64());

        let mut plugin_mcp_servers = merge_session_mcp_servers(
            sopts.mcp_server_configs.unwrap_or_default(),
            refreshed
                .as_ref()
                .map(|state| state.mcp_servers.clone())
                .unwrap_or_else(|| self.plugin_mcp_servers.clone()),
        );
        if task_scoped {
            plugin_mcp_servers = manual_mcp_servers.clone();
        }

        let mut suppressed = Vec::new();
        let mut agent_candidates = Vec::new();
        if include_builtins && !host_supplied_tools {
            agent_candidates.extend(BUILTIN_AGENTS.iter().map(|agent| NamedSource {
                name: runtime_agent_name_to_extension_snapshot(&agent.name),
                source: ExtensionSource::builtin(),
            }));
        }
        agent_candidates.extend(runtime_agent_source(&sources.plugin_agents));
        agent_candidates.extend(runtime_agent_source(&sources.host_agents));
        agent_candidates.extend(runtime_agent_source(&session_agents));
        let effective_agent_entries = effective_extensions(
            ExtensionKind::Agent,
            registry_agents
                .iter()
                .map(|agent| runtime_agent_name_to_extension_snapshot(&agent.name)),
            &agent_candidates,
            &mut suppressed,
        );

        let mut skill_candidates = Vec::new();
        if include_builtins {
            skill_candidates.extend(builtin_skills().into_iter().map(|skill| NamedSource {
                name: skill.name,
                source: ExtensionSource::builtin(),
            }));
        }
        skill_candidates.extend(named_source(&sources.plugin_skills, |skill| {
            skill.name.clone()
        }));
        skill_candidates.extend(initial_host_skills.iter().map(|skill| NamedSource {
            name: skill.name.clone(),
            source: ExtensionSource::host(),
        }));
        skill_candidates.extend(initial_session_skills.iter().map(|skill| NamedSource {
            name: skill.name.clone(),
            source: ExtensionSource::session(),
        }));
        let effective_skill_entries = effective_extensions(
            ExtensionKind::Skill,
            initial_effective_skills
                .iter()
                .map(|skill| skill.name.clone()),
            &skill_candidates,
            &mut suppressed,
        );

        let mut output_style_candidates =
            named_source(&sources.plugin_output_styles, |style| style.name.clone());
        output_style_candidates.extend(initial_host_output_styles.iter().map(|style| {
            NamedSource {
                name: style.name.clone(),
                source: ExtensionSource::host(),
            }
        }));
        output_style_candidates.extend(initial_session_output_styles.iter().map(|style| {
            NamedSource {
                name: style.name.clone(),
                source: ExtensionSource::session(),
            }
        }));
        let effective_output_style_entries = effective_extensions(
            ExtensionKind::OutputStyle,
            effective_output_styles
                .iter()
                .map(|style| style.name.clone()),
            &output_style_candidates,
            &mut suppressed,
        );

        let effective_hooks = sourced_hooks
            .iter()
            .map(|item| {
                let (handler_kind, timeout_ms) = match &item.value.handler {
                    HookHandler::Fn(_) => (HookHandlerKind::Function, None),
                    HookHandler::Command { timeout_ms, .. } => {
                        (HookHandlerKind::Command, *timeout_ms)
                    }
                };
                EffectiveHook {
                    event: item.value.event,
                    matcher: item.value.matcher.clone().map(RuntimeString::from),
                    handler_kind,
                    is_async: item.value.is_async,
                    timeout_ms,
                    source: item.source.clone(),
                }
            })
            .collect::<Vec<_>>();

        let (pricing, pricing_source) = match sopts.pricing.clone() {
            Some(pricing) => (Some(pricing), ExtensionSource::session()),
            None => match self.opts.pricing.as_ref() {
                Some(pricing) => (
                    Some(ordered_pricing_from_map(pricing)),
                    ExtensionSource::host(),
                ),
                None => (None, ExtensionSource::builtin()),
            },
        };
        let pricing_model_ids = pricing
            .as_ref()
            .unwrap_or(&DEFAULT_PRICING)
            .iter()
            .map(|(model, _)| RuntimeString::from(model.clone()))
            .collect();
        let extension_runtime = ExtensionRuntimeSnapshot {
            agents: effective_agent_entries,
            skills: effective_skill_entries,
            output_styles: effective_output_style_entries,
            selected_output_style: None,
            hooks: effective_hooks,
            pricing: PricingRuntimeSnapshot {
                model_ids: pricing_model_ids,
                source: pricing_source,
            },
            suppressed,
            plugin_runtime: if task_scoped {
                PluginRuntimeSnapshot::default()
            } else {
                refreshed
                    .as_ref()
                    .map(|state| state.registry.snapshot())
                    .unwrap_or_else(|| self.plugin_registry.snapshot())
            },
        };

        let session = Session::try_new_with_review_requirements(
            SessionInit {
                process_context: self.opts.process_context.clone(),
                id: sopts.id,
                user_config_dir: sopts.user_config_dir,
                provider,
                deps,
                config,
                network_allowed: sopts.network_allowed,
                durable_permissions: sopts.durable_permissions,
                trusted_windows_sandbox_context: sopts.trusted_windows_sandbox_context,
                context_window_override,
                pricing,
                tools,
                root_agent_definition,
                refresh_tools,
                cwd,
                approval_handler,
                approval_reviewer,
                elicit_handler,
                mcp_elicitation_presenter: sopts.mcp_elicitation_presenter,
                mcp_authorize_handler: sopts.mcp_authorize_handler,
                mcp_oauth_redirect_uri: sopts.mcp_oauth_redirect_uri,
                credential_store,
                hook_bus,
                rollout: sopts.rollout,
                store: sopts.store,
                mcp_clients: mcp_clients
                    .unwrap_or_default()
                    .into_iter()
                    .map(Arc::new)
                    .collect(),
                plugin_mcp_servers,
                slash_commands: if task_scoped {
                    Vec::new()
                } else {
                    refreshed
                        .as_ref()
                        .map(|state| state.slash_commands.clone())
                        .unwrap_or_else(|| self.slash_commands.clone())
                },
                initial_history: sopts.initial_history,
                initial_approval_context: sopts.initial_approval_context,
                initial_approval_reviews: sopts.initial_approval_reviews,
                initial_tool_result_budget_records: sopts.initial_tool_result_budget_records,
                hidden_context_message_ids: sopts.hidden_context_message_ids,
                initial_usage_ledger: sopts.initial_usage_ledger,
                initial_subagent_lifecycle: sopts.initial_subagent_lifecycle,
                initial_background_processes: sopts.initial_background_processes,
                initial_memory_maintenance: sopts.initial_memory_maintenance,
                initial_setup_lifecycle: sopts.initial_setup_lifecycle,
                initial_agent_memory_snapshot_decisions: sopts
                    .initial_agent_memory_snapshot_decisions,
                session_role: sopts.session_role,
                parent_thread_id: sopts.parent_thread_id,
                forked_from_id: sopts.forked_from_id,
                initial_checkpoints: sopts.initial_checkpoints,
                system_prompt_provider,
                base_instructions: sopts.base_instructions,
                developer_instructions: sopts.developer_instructions,
                observer: self.opts.observer.clone(),
                logger: self.opts.logger.clone(),
                agents: if registry_agents.is_empty() {
                    None
                } else {
                    Some(registry_agents)
                },
                skills,
                output_styles,
                skill_gate,
                current_worktree: sopts.current_worktree,
                prior_workflow_journal: sopts.prior_workflow_journal,
                initial_surfaced_tools: sopts.initial_surfaced_tools,
                swarm: sopts.swarm.map(|s| SwarmInit {
                    team: s.team,
                    peer: Some(s.peer),
                }),
                lsp_servers,
                shell_env: sopts.shell_env,
                agent_memory,
                agent_memory_snapshot_policy: sopts.agent_memory_snapshot_policy,
                browser_runtime,
                runtime_capabilities: runtime_capability_registry,
                selected_capability_roots: sopts.selected_capability_roots,
                extension_runtime,
            },
            initial_queue,
            prepared,
        )?;
        session.set_manual_mcp_servers(manual_mcp_servers);
        plugin_binding.session = Arc::downgrade(&session);
        plugin_sessions.retain(|binding| binding.session.strong_count() > 0);
        if !task_scoped {
            plugin_sessions.push(plugin_binding);
        }
        Ok(session)
    }

    /// Start a session that persists to the given store.
    /// Classify persistent goal ownership using the same config and root definition as session creation.
    /// This reads configuration only and never resolves a provider or starts a Session.
    pub fn goal_pursuit_eligible_options(
        &self,
        options: &StartSessionOptions,
    ) -> Result<bool, SessionInitError> {
        if options.parent_thread_id.is_some()
            || !matches!(options.session_role, None | Some(SessionRole::Main))
        {
            return Ok(false);
        }
        let mut config = self.resolve_session_config(options);
        let refreshed = self
            .refreshed_plugins
            .read()
            .expect("plugin registry lock poisoned")
            .clone();
        let sources = refreshed
            .as_ref()
            .map(|state| &state.sources)
            .unwrap_or(&self.extension_sources);
        let (agents, _) = self.agent_definitions_for_session(options, sources);
        Self::resolve_root_agent_definition(
            options.agent_definition_name.as_ref(),
            &agents,
            &mut config,
        )?;
        Ok(config.task_resource_scope.is_none())
    }

    pub async fn open_session(
        &self,
        store: Arc<dyn SessionStore>,
        mut sopts: StartSessionOptions,
    ) -> std::io::Result<Arc<Session>> {
        let role = sopts.session_role.unwrap_or(SessionRole::Main);
        self.mint_new_thread_rollout(store, &mut sopts, role)
            .await?;
        self.start_persisted_session(sopts).await
    }

    /// Mint the rollout for a NEW persisted thread (an ordinary open, or a
    /// persisted side task) and attach it plus the store to `sopts`.
    ///
    /// A SEEDED persisted session (a fork, or a side task inheriting its parent's
    /// completed prefix) must persist its inherited history, or a later
    /// `resume_session` would silently drop everything before the seed point.
    async fn mint_new_thread_rollout(
        &self,
        store: Arc<dyn SessionStore>,
        sopts: &mut StartSessionOptions,
        role: SessionRole,
    ) -> std::io::Result<()> {
        let provider = sopts
            .provider
            .clone()
            .unwrap_or_else(|| self.opts.provider.clone());
        let mut config = self.resolve_session_config(sopts);
        let refreshed = self
            .refreshed_plugins
            .read()
            .expect("plugin registry lock poisoned")
            .clone();
        let sources = refreshed
            .as_ref()
            .map(|state| &state.sources)
            .unwrap_or(&self.extension_sources);
        let (registry_agents, _) = self.agent_definitions_for_session(sopts, sources);
        Self::resolve_root_agent_definition(
            sopts.agent_definition_name.as_ref(),
            &registry_agents,
            &mut config,
        )
        .map_err(SessionInitError::into_io_error)?;
        let cwd = sopts
            .cwd
            .clone()
            .or_else(|| self.opts.cwd.clone().map(RuntimePath::from))
            .unwrap_or_else(default_cwd);
        let deps = self.build_deps(provider, &config);
        let id = sopts
            .id
            .clone()
            .unwrap_or_else(|| RuntimeString::from(new_uuid()));
        let now = (deps.now)();
        let meta = SessionMeta {
            id: id.clone(),
            created_at: now,
            cwd: cwd.as_runtime_string().clone(),
            model: config.model.clone(),
            title: None,
            originator: None,
            cli_version: None,
            source: None,
            role: Some(role),
            parent_thread_id: sopts.parent_thread_id.clone(),
            forked_from_id: sopts.forked_from_id.clone(),
            git: None,
            // The host attaches its bookkeeping after creation via
            // update_host_metadata; a fresh thread starts without one.
            host_metadata: None,
        };
        let rollout: RolloutSink = Arc::from(store.create(meta).await?);
        if let Some(history) = &sopts.initial_history {
            for m in history {
                rollout.append_message(m.clone()).await;
            }
        }
        sopts.id = Some(id);
        sopts.cwd = Some(cwd);
        sopts.rollout = Some(rollout);
        sopts.store = Some(store);
        Ok(())
    }

    /// Start a session whose rollout was just minted: persist the initial review
    /// checkpoint, and drain the minted rollout when startup fails.
    async fn start_persisted_session(
        &self,
        sopts: StartSessionOptions,
    ) -> std::io::Result<Arc<Session>> {
        let rollout_cleanup = sopts
            .rollout
            .clone()
            .expect("a persisted start carries its minted rollout");
        let has_initial_review_context = sopts.initial_approval_context.is_some();
        match self.try_start_session_async(sopts).await {
            Ok(session) => {
                if has_initial_review_context {
                    session.persist_review_checkpoint().await;
                    if let Err(error) = session.flush_rollout().await {
                        session.dispose().await;
                        return Err(std::io::Error::other(error));
                    }
                }
                Ok(session)
            }
            Err(error) => {
                let primary = error.into_io_error();
                match rollout_cleanup.shutdown().await {
                    Ok(()) => Err(primary),
                    Err(cleanup) => Err(std::io::Error::new(
                        primary.kind(),
                        format!("{primary}; rollout cleanup failed: {cleanup}"),
                    )),
                }
            }
        }
    }

    /// Start a side task off a persisted parent thread.
    ///
    /// A side task inherits the parent's completed context prefix as hidden context
    /// prefix. With a session store it persists its own rollout under its side
    /// thread id — the side-task role keeps it out of the default listing — so
    /// close/resume and crash recovery rebuild it exactly like an ordinary
    /// thread. Without a store it stays the ephemeral variant that persists
    /// nothing. The host owns the side task's lifetime and idle expiry; the
    /// engine records the role so the turn loop can inject the side-task prompt.
    pub async fn create_side_task(
        &self,
        store: Arc<dyn SessionStore>,
        parent_thread_id: &str,
        sopts: StartSessionOptions,
    ) -> std::io::Result<Arc<Session>> {
        let resumed = store.resume(parent_thread_id).await?;
        let inherited = completed_context_prefix(&resumed.messages, &resumed.turn_index);
        let approval_context =
            super::approval_capture::ReviewParentHistory::from_rollout(&resumed.raw_items)
                .map_err(|error| {
                    std::io::Error::other(
                        String::from_utf8(error.to_node_utf8())
                            .expect("Node UTF-8 projection is valid"),
                    )
                })?
                .prefix_for_messages(&inherited)
                .checkpoint();
        self.start_side_task(
            resumed.meta.id,
            inherited,
            approval_context,
            sopts,
            Some(store),
        )
        .await
    }

    /// Create an ephemeral side task from an engine-captured completed context prefix.
    /// The source may still be running; no parent rollout is read or written.
    pub async fn create_side_task_from_live(
        &self,
        snapshot: &super::session::LiveSideTaskSnapshot,
        mut sopts: StartSessionOptions,
        store: Option<Arc<dyn SessionStore>>,
    ) -> std::io::Result<Arc<Session>> {
        sopts.cwd = sopts.cwd.or_else(|| Some(snapshot.cwd.clone()));
        self.start_side_task(
            snapshot.source_thread_id.clone(),
            snapshot.history.clone(),
            snapshot.approval_context.clone(),
            sopts,
            store,
        )
        .await
    }

    async fn start_side_task(
        &self,
        parent_thread_id: RuntimeString,
        inherited: Vec<Message>,
        approval_context: super::approval_capture::ReviewParentCheckpoint,
        mut sopts: StartSessionOptions,
        store: Option<Arc<dyn SessionStore>>,
    ) -> std::io::Result<Arc<Session>> {
        if sopts.id.as_ref() == Some(&parent_thread_id) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "a side task must use a new thread id",
            ));
        }
        sopts.hidden_context_message_ids =
            Some(inherited.iter().map(message_uuid).cloned().collect());
        sopts.initial_approval_context = Some(
            super::approval_capture::ReviewParentHistory::inherit_snapshot(
                &super::approval_capture::ReviewParentHistory::restore(&approval_context)
                    .snapshot(),
                false,
            )
            .checkpoint(),
        );
        sopts.initial_approval_reviews = Some(Vec::new());
        sopts.initial_history = Some(inherited);
        sopts.session_role = Some(SessionRole::SideTask);
        sopts.parent_thread_id = Some(parent_thread_id);
        sopts.forked_from_id = None;
        sopts.initial_usage_ledger = Some(Vec::new());
        sopts.initial_subagent_lifecycle = Some(Vec::new());
        sopts.initial_background_processes = Some(Vec::new());
        sopts.initial_memory_maintenance = Some(MemoryMaintenanceSnapshot::default());
        sopts.initial_setup_lifecycle = Some(SetupLifecycleSnapshot::default());
        sopts.initial_agent_memory_snapshot_decisions = Some(Vec::new());
        if sopts.id.is_none() {
            sopts.id = Some(RuntimeString::from(new_uuid()));
        }
        if sopts.cwd.is_none() {
            sopts.cwd = Some(
                self.opts
                    .cwd
                    .clone()
                    .map(RuntimePath::from)
                    .unwrap_or_else(default_cwd),
            );
        }
        match store {
            Some(store) => {
                // Persisted side task: its own rollout under the side thread id.
                // The side-task role keeps it out of the default listing while
                // close/resume and crash recovery rebuild it like any thread.
                self.mint_new_thread_rollout(store, &mut sopts, SessionRole::SideTask)
                    .await?;
                self.start_persisted_session(sopts).await
            }
            None => {
                // No session store: the ephemeral variant persists nothing.
                sopts.rollout = None;
                sopts.store = None;
                self.try_start_session_async(sopts)
                    .await
                    .map_err(SessionInitError::into_io_error)
            }
        }
    }

    /// Fork a persisted session into a new rollout while retaining the source
    /// thread relation in both the live session and persisted header.
    pub async fn fork_session(
        &self,
        store: Arc<dyn SessionStore>,
        thread_id: &str,
        sopts: StartSessionOptions,
    ) -> std::io::Result<Arc<Session>> {
        self.fork_session_before(store, thread_id, None, sopts)
            .await
    }

    /// Persist a previously captured live transcript as a new ordinary thread.
    /// A side task's inherited hidden prefix becomes explicitly visible here;
    /// only the new thread is promoted, and no state is changed on the source.
    pub async fn fork_live_session(
        &self,
        store: Arc<dyn SessionStore>,
        snapshot: &LiveForkSnapshot,
        mut sopts: StartSessionOptions,
    ) -> std::io::Result<Arc<Session>> {
        if sopts.id.as_ref() == Some(&snapshot.source_thread_id) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "a live fork must use a new thread id",
            ));
        }
        sopts.initial_approval_context = Some(
            super::approval_capture::ReviewParentHistory::inherit_snapshot(
                &super::approval_capture::ReviewParentHistory::restore(&snapshot.approval_context)
                    .snapshot(),
                true,
            )
            .checkpoint(),
        );
        sopts.initial_approval_reviews = Some(Vec::new());
        sopts.initial_history = Some(snapshot.history.clone());
        sopts.hidden_context_message_ids = None;
        sopts.session_role = Some(SessionRole::Main);
        sopts.parent_thread_id = None;
        sopts.forked_from_id = Some(snapshot.source_thread_id.clone());
        sopts.cwd = sopts.cwd.or_else(|| Some(snapshot.cwd.clone()));
        sopts.initial_usage_ledger = Some(Vec::new());
        sopts.initial_subagent_lifecycle = Some(Vec::new());
        sopts.initial_background_processes = Some(Vec::new());
        sopts.initial_memory_maintenance = Some(MemoryMaintenanceSnapshot::default());
        sopts.initial_setup_lifecycle = Some(SetupLifecycleSnapshot::default());
        sopts.initial_agent_memory_snapshot_decisions = Some(Vec::new());
        let session = self.open_session(store, sopts).await?;
        session
            .planning_runtime()
            .inherit_from(&snapshot.planning, &snapshot.retained_planning_turns)
            .map_err(|error| std::io::Error::other(crate::planning::runtime::host(&error)))?;
        if let Some(inherited_goal) = &snapshot.inherited_goal {
            let target = session.id.try_to_utf8().map_err(|error| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, error.to_string())
            })?;
            if let Err(error) =
                crate::goal::store::copy_goal_snapshot_deferred(&target, inherited_goal).await
            {
                session.initiate_shutdown();
                return Err(error);
            }
        }
        Ok(session)
    }

    /// Fork like [`Self::fork_session`], but cut the inherited history right
    /// BEFORE the message carrying `before_message_uuid`: the branch keeps
    /// everything strictly older than that message and the source thread is
    /// untouched. An unknown uuid fails loudly instead of silently forking the
    /// full transcript.
    pub async fn fork_session_before(
        &self,
        store: Arc<dyn SessionStore>,
        thread_id: &str,
        before_message_uuid: Option<&str>,
        mut sopts: StartSessionOptions,
    ) -> std::io::Result<Arc<Session>> {
        let resumed = store.resume(thread_id).await?;
        let mut history = resumed.messages;
        if let Some(cut_uuid) = before_message_uuid {
            let cut = history
                .iter()
                .position(|message| {
                    let uuid = match message {
                        Message::User(user) => &user.uuid,
                        Message::Assistant(assistant) => &assistant.uuid,
                        Message::ToolResult(tool) => &tool.uuid,
                        Message::System(system) => &system.uuid,
                        Message::CompactBoundary(boundary) => &boundary.uuid,
                    };
                    *uuid == *cut_uuid
                })
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("fork cut point {cut_uuid} does not exist in thread {thread_id}"),
                    )
                })?;
            history.truncate(cut);
        }
        let mut retained_planning_turns: std::collections::HashSet<RuntimeString> = history
            .iter()
            .filter_map(|message| {
                resumed
                    .turn_index
                    .message_turn
                    .get(message_uuid(message))
                    .cloned()
            })
            .collect();
        let planning_root = sopts
            .user_config_dir
            .as_ref()
            .map(|root| {
                std::path::PathBuf::from(crate::planning::runtime::host(root.as_runtime_string()))
            })
            .unwrap_or_else(|| std::path::PathBuf::from(crate::fs::paths::config_home()));
        let planning_source = crate::planning::runtime::PlanningRuntime::new(
            thread_id.into(),
            Some(planning_root),
            false,
            false,
        )
        .snapshot()
        .map_err(|error| std::io::Error::other(crate::planning::runtime::host(&error)))?;
        if before_message_uuid.is_none() {
            retained_planning_turns.extend(
                planning_source
                    .plans
                    .iter()
                    .map(|plan| plan.turn_id.clone()),
            );
            retained_planning_turns.extend(
                planning_source
                    .questions
                    .iter()
                    .filter_map(|q| q.get("turnId").and_then(RuntimeJson::as_str).cloned()),
            );
        }
        let parent_approval =
            super::approval_capture::ReviewParentHistory::from_rollout(&resumed.raw_items)
                .map_err(|error| {
                    std::io::Error::other(
                        String::from_utf8(error.to_node_utf8())
                            .expect("Node UTF-8 projection is valid"),
                    )
                })?;
        let parent_approval = if before_message_uuid.is_some() {
            parent_approval.prefix_for_messages(&history)
        } else {
            parent_approval
        };
        sopts.initial_approval_context = Some(
            super::approval_capture::ReviewParentHistory::inherit_snapshot(
                &parent_approval.snapshot(),
                true,
            )
            .checkpoint(),
        );
        sopts.initial_approval_reviews = Some(Vec::new());
        sopts.initial_history = Some(history);
        sopts.forked_from_id = Some(resumed.meta.id);
        sopts.initial_usage_ledger = Some(Vec::new());
        sopts.initial_subagent_lifecycle = Some(Vec::new());
        sopts.initial_background_processes = Some(Vec::new());
        sopts.initial_memory_maintenance = Some(MemoryMaintenanceSnapshot::default());
        sopts.initial_setup_lifecycle = Some(SetupLifecycleSnapshot::default());
        sopts.initial_agent_memory_snapshot_decisions = Some(Vec::new());
        let session = self.open_session(store, sopts).await?;
        session
            .planning_runtime()
            .inherit_from(&planning_source, &retained_planning_turns)
            .map_err(|error| std::io::Error::other(crate::planning::runtime::host(&error)))?;
        Ok(session)
    }

    /// Resume a persisted session: load its history AND re-attach a writer so the
    /// continuation keeps persisting to the same rollout. The continuation runs in
    /// the session's RECORDED cwd by default (set `sopts.cwd` to override).
    pub async fn resume_session(
        &self,
        store: Arc<dyn SessionStore>,
        thread_id: &str,
        mut sopts: StartSessionOptions,
    ) -> std::io::Result<Arc<Session>> {
        if let Some(replacement) = sopts.rollout.take() {
            let primary = std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "resume_session requires the writer lease returned by SessionStore::resume_for_write; wrap that leased rollout instead of replacing it",
            );
            let cleanup_owner = tokio::spawn(async move { replacement.shutdown().await });
            return match cleanup_owner.await {
                Ok(Ok(())) => Err(primary),
                Ok(Err(cleanup)) => Err(std::io::Error::new(
                    primary.kind(),
                    format!("{primary}; rejected replacement rollout cleanup failed: {cleanup}"),
                )),
                Err(cleanup) => Err(std::io::Error::new(
                    primary.kind(),
                    format!(
                        "{primary}; rejected replacement rollout cleanup task failed: {cleanup}"
                    ),
                )),
            };
        }
        let crate::store::session_store::ResumedRuntimeRollout {
            resume:
                crate::store::session_store::RuntimeResumeResult {
                    meta,
                    messages,
                    recovery_items: raw_items,
                    persisted_user_uuids,
                    usage,
                    file_changes,
                    turn_index,
                    ..
                },
            rollout,
        } = store.resume_runtime_for_write(thread_id).await?;
        // Side tasks are ephemeral, so a persisted rollout carrying the side-task
        // role can only be a leftover from an older build. It keeps its role (the
        // turn loop still frames it as a side task) but has no lifecycle state, and
        // the default listing hides it, so nothing surfaces it again.
        let effective_role = meta.role.unwrap_or(SessionRole::Main);
        let rollout_guard = ResumedRolloutInitGuard::new(Arc::from(rollout));
        let initial_queue =
            match queue_from_runtime(&raw_items, &meta.id, Some(&persisted_user_uuids)) {
                Ok(queue) => queue,
                Err(error) => return Err(rollout_guard.fail(error).await),
            };
        let initial_prediction_source = match prediction_source_from_rollout(&raw_items) {
            Ok(source) => source,
            Err(error) => return Err(rollout_guard.fail(error).await),
        };
        let initial_approval_context = match sopts.initial_approval_context.clone() {
            Some(checkpoint) => checkpoint,
            None => match super::approval_capture::ReviewParentHistory::from_rollout(&raw_items) {
                Ok(history) => history.checkpoint(),
                Err(error) => {
                    return Err(rollout_guard
                        .fail(std::io::Error::other(
                            String::from_utf8(error.to_node_utf8())
                                .expect("Node UTF-8 projection is valid"),
                        ))
                        .await);
                }
            },
        };
        let initial_approval_reviews = match sopts.initial_approval_reviews.clone() {
            Some(reviews) => reviews,
            None => match super::approval_capture::approval_reviews_from_rollout(&raw_items) {
                Ok(reviews) => reviews,
                Err(error) => {
                    return Err(rollout_guard
                        .fail(std::io::Error::other(
                            String::from_utf8(error.to_node_utf8())
                                .expect("Node UTF-8 projection is valid"),
                        ))
                        .await);
                }
            },
        };
        let rollout = rollout_guard.rollout();
        // Re-enter the worktree the session was last in, if any. A host-managed
        // (hook-based) worktree is only honored when this resumed session is actually
        // configured with a worktree hook.
        let has_worktree_hook = self
            .opts
            .hooks
            .iter()
            .chain(sopts.hooks.iter().flatten())
            .any(|h| {
                matches!(
                    h.event,
                    HookEvent::WorktreeCreate | HookEvent::WorktreeRemove
                )
            });
        let current_worktree = sopts.current_worktree.clone().or_else(|| {
            latest_worktree_state(
                &raw_items,
                &RuntimePath::from(meta.cwd.clone()),
                has_worktree_hook,
            )
        });
        let prior_workflow_journal = sopts
            .prior_workflow_journal
            .clone()
            .unwrap_or_else(|| workflow_journal_from_rollout(&raw_items));
        let initial_surfaced_tools = sopts
            .initial_surfaced_tools
            .clone()
            .unwrap_or_else(|| surfaced_tools_from_rollout(&raw_items));
        let recovery_pricing = sopts
            .pricing
            .clone()
            .or_else(|| self.opts.pricing.as_ref().map(ordered_pricing_from_map));
        let initial_usage_ledger = sopts
            .initial_usage_ledger
            .clone()
            .unwrap_or_else(|| usage_ledger_from_runtime(&usage, recovery_pricing.as_ref()));
        let initial_subagent_lifecycle = sopts
            .initial_subagent_lifecycle
            .clone()
            .unwrap_or_else(|| subagent_lifecycle_from_rollout(&raw_items));
        let initial_background_processes = sopts
            .initial_background_processes
            .clone()
            .unwrap_or_else(|| background_processes_from_rollout(&raw_items));
        let initial_memory_maintenance = sopts
            .initial_memory_maintenance
            .clone()
            .unwrap_or_else(|| memory_maintenance_from_rollout(&raw_items));
        let initial_setup_lifecycle = sopts
            .initial_setup_lifecycle
            .clone()
            .unwrap_or_else(|| setup_lifecycle_from_rollout(&raw_items));
        let initial_agent_memory_snapshot_decisions = sopts
            .initial_agent_memory_snapshot_decisions
            .clone()
            .unwrap_or_else(|| agent_memory_snapshot_decisions_from_rollout(&raw_items));
        // Reconstruct the rewind checkpoint ledger so a resumed session can restore
        // files to a historical turn. Empty when the session never checkpointed.
        let initial_checkpoints = sopts
            .initial_checkpoints
            .clone()
            .unwrap_or_else(|| project_checkpoints(&raw_items));

        sopts.id = Some(meta.id.clone());
        sopts.session_role = Some(effective_role);
        sopts.parent_thread_id = meta.parent_thread_id.clone();
        sopts.cwd = Some(
            sopts
                .cwd
                .clone()
                .or_else(|| current_worktree.as_ref().map(|w| w.worktree_path.clone()))
                .unwrap_or_else(|| RuntimePath::from(meta.cwd.clone())),
        );
        let mut initial_history = filter_unresolved_tool_uses(&messages);
        if let Some(notice) = code_mode_recovery_notice(&raw_items) {
            initial_history.push(notice);
        }
        sopts.initial_history = Some(initial_history);
        sopts.initial_approval_context = Some(initial_approval_context);
        sopts.initial_approval_reviews = Some(initial_approval_reviews);
        // The budget's replacement bodies live only in the request view, so the
        // persisted history still carries the ORIGINAL bodies; resume must
        // replay the records to re-apply them byte-identically.
        sopts.initial_tool_result_budget_records =
            Some(crate::store::projection::project_tool_result_budget_records(&raw_items));
        sopts.forked_from_id = meta.forked_from_id.clone();
        sopts.initial_checkpoints = Some(initial_checkpoints);
        sopts.rollout = Some(rollout);
        sopts.store = Some(store);
        sopts.current_worktree = current_worktree;
        if !prior_workflow_journal.is_empty() {
            sopts.prior_workflow_journal = Some(prior_workflow_journal);
        }
        if !initial_surfaced_tools.is_empty() {
            sopts.initial_surfaced_tools = Some(initial_surfaced_tools);
        }
        if !initial_usage_ledger.is_empty() {
            sopts.initial_usage_ledger = Some(initial_usage_ledger);
        }
        if !initial_subagent_lifecycle.is_empty() {
            sopts.initial_subagent_lifecycle = Some(initial_subagent_lifecycle);
        }
        if !initial_background_processes.is_empty() {
            sopts.initial_background_processes = Some(initial_background_processes);
        }
        if initial_memory_maintenance != MemoryMaintenanceSnapshot::default() {
            sopts.initial_memory_maintenance = Some(initial_memory_maintenance);
        }
        if initial_setup_lifecycle != SetupLifecycleSnapshot::default() {
            sopts.initial_setup_lifecycle = Some(initial_setup_lifecycle);
        }
        if !initial_agent_memory_snapshot_decisions.is_empty() {
            sopts.initial_agent_memory_snapshot_decisions =
                Some(initial_agent_memory_snapshot_decisions);
        }
        let session = match self
            .try_start_session_with_queue_async(sopts, initial_queue)
            .await
        {
            Ok(session) => session,
            Err(error) => return Err(rollout_guard.fail(error.into_io_error()).await),
        };
        session.restore_ordinary_parent_seed(&raw_items);
        if let Err(error) = session.restore_retained_goal_instructions(&raw_items).await {
            session.dispose().await;
            return Err(rollout_guard.fail(error).await);
        }
        if let Some(goal) = session.goal_runtime() {
            if let Err(error) = goal.recover_interrupted_run().await {
                session.dispose().await;
                return Err(rollout_guard.fail(error).await);
            }
        }
        // Reopen the same plan document a plan-mode session was building, so a
        // resumed session's write exemption and plan reminders keep pointing at the
        // original file. This reader must outlive the name-minting it replaced:
        // sessions persisted before the rename have random-slug documents on disk,
        // and without the pinned name a reopened one would resolve a fresh default,
        // lose the exemption for the file it was really editing, and start a new
        // empty document beside it.
        if let Some(name) = plan_name_from_rollout(&raw_items) {
            session.set_plan_name(name);
        }
        // What earlier turns changed on disk. Without this the produced-output
        // surface of a reopened thread claims the session changed nothing, which
        // reads as "the work is gone" rather than "the engine forgot".
        session.restore_file_changes(file_changes);
        session.context_hint_state.restore_rollout(&raw_items);
        session.restore_context_usage_checkpoint(&raw_items);
        // Which turn each message belongs to, and how each turn ENDED. The rollout
        // records it (`turn_lifecycle`), the replay rebuilds it, and dropping it
        // here made every resumed turn look like a normal completed one — a turn
        // the user stopped came back without its stopped label, its elapsed time,
        // or its resume entry, and folded itself away.
        session.restore_turn_index(turn_index);
        if let Some(checkpoint) = initial_prediction_source {
            session.restore_prediction_source_checkpoint(checkpoint);
        } else {
            // Legacy traces have no admission watermark. Preserve their retained context fallback.
            session.restore_prediction_source_turn(
                raw_items
                    .iter()
                    .rev()
                    .find_map(|item| match item {
                        RolloutItem::TurnContext { ctx } => Some(ctx.turn_id.clone()),
                        RolloutItem::Rollback { .. } => Some(None),
                        _ => None,
                    })
                    .flatten(),
            );
        }
        // Session now owns the same atomic writer lease. From this point the
        // retained recovery-admission owner below is responsible for
        // cancellation and failure cleanup.
        rollout_guard.commit();
        Self::admit_resumed_session_after_recovery(session).await
    }

    /// Do not expose a resumed Session until every constructor recovery owner
    /// has crossed its durability boundary. Queue normalization is ordered
    /// first; Setup, maintenance, and Snapshot recovery then settle together,
    /// followed by one final rollout flush before handoff. The retained owner also closes a
    /// fully constructed Session if the caller is cancelled while waiting, so
    /// its rollout writer and background services cannot escape without a Host.
    async fn admit_resumed_session_after_recovery(
        session: Arc<Session>,
    ) -> std::io::Result<Arc<Session>> {
        let (send, receive) = tokio::sync::oneshot::channel();
        let mut caller_guard = ResumedSessionCallerGuard::new(session.clone());
        tokio::spawn(async move {
            let mut send = send;
            let durability_session = session.clone();
            let mut recovery_wait = Box::pin(async move {
                match std::panic::AssertUnwindSafe(durability_session.await_queue_durability())
                    .catch_unwind()
                    .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        return Err(format!(
                            "failed to persist recovered queue normalization before Session admission: {}",
                            String::from_utf16_lossy(error.as_utf16())
                        ));
                    }
                    Err(_) => {
                        return Err(
                            "recovered queue normalization panicked before Session admission"
                                .to_string(),
                        );
                    }
                }

                let setup_session = durability_session.clone();
                let maintenance_session = durability_session.clone();
                let snapshot_session = durability_session.clone();
                let (setup, maintenance, snapshot) = tokio::join!(
                    std::panic::AssertUnwindSafe(
                        setup_session.await_pending_setup_recovery_receipts()
                    )
                    .catch_unwind(),
                    std::panic::AssertUnwindSafe(
                        maintenance_session.await_pending_memory_maintenance_recovery_owners()
                    )
                    .catch_unwind(),
                    std::panic::AssertUnwindSafe(
                        snapshot_session.await_pending_snapshot_recovery_owners()
                    )
                    .catch_unwind(),
                );
                let mut recovery_errors = Vec::new();
                match setup {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => recovery_errors.push(format!(
                        "Setup recovery failed: {}",
                        String::from_utf16_lossy(error.as_utf16())
                    )),
                    Err(_) => recovery_errors.push("Setup recovery panicked".to_string()),
                }
                match maintenance {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => recovery_errors.push(format!(
                        "memory maintenance recovery failed: {}",
                        String::from_utf16_lossy(error.as_utf16())
                    )),
                    Err(_) => {
                        recovery_errors.push("memory maintenance recovery panicked".to_string())
                    }
                }
                match snapshot {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => recovery_errors.push(format!(
                        "Snapshot recovery failed: {}",
                        String::from_utf16_lossy(error.as_utf16())
                    )),
                    Err(_) => recovery_errors.push("Snapshot recovery panicked".to_string()),
                }
                if !recovery_errors.is_empty() {
                    return Err(format!(
                        "failed to persist constructor recovery before Session admission: {}",
                        recovery_errors.join("; ")
                    ));
                }

                // Reload this session's persisted sub-agents into live workers and
                // repopulate the `task_output` handle table BEFORE the session is
                // handed over, so a child the previous process spawned is
                // addressable from the first turn of this one. Without this the
                // lifecycle ledger still reports the children while the handle
                // table is empty, and the parent cannot read a background task
                // whose work is sitting on disk. A failure here is a warning
                // inside the call, not a resume failure: one unreadable child
                // transcript must not make the whole session unresumable, and the
                // lazy hydration on the message path still retries that child.
                match std::panic::AssertUnwindSafe(
                    durability_session.recover_ordinary_subagents_on_resume(),
                )
                .catch_unwind()
                .await
                {
                    Ok(()) => {}
                    Err(_) => recovery_errors.push("Sub-agent reload panicked".to_string()),
                }
                if !recovery_errors.is_empty() {
                    return Err(format!(
                        "failed to reload sub-agents before Session admission: {}",
                        recovery_errors.join("; ")
                    ));
                }

                match std::panic::AssertUnwindSafe(durability_session.flush_rollout())
                    .catch_unwind()
                    .await
                {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(error)) => Err(format!(
                        "failed to flush recovered Session before admission: {error}"
                    )),
                    Err(_) => Err(
                        "recovered Session final rollout flush panicked before admission"
                            .to_string(),
                    ),
                }
            });
            let recovery_result = tokio::select! {
                result = &mut recovery_wait => Some(result),
                _ = send.closed() => None,
            };
            drop(recovery_wait);
            let Some(recovery_result) = recovery_result else {
                // The caller disappeared while the retained checkpoint was
                // or constructor recovery was still pending. Drop the waiter
                // first. This Session was never published, so its dedicated
                // abandonment path may abort a non-cooperative Queue receipt,
                // install a failed durability fence, and then run the complete
                // typed shutdown without retaining the rollout writer forever.
                let _ = session.shutdown_abandoned_resume().await;
                return;
            };
            let result = match recovery_result {
                Ok(()) => {
                    Self::offer_resumed_session(session, send, || {}).await;
                    return;
                }
                Err(error) => {
                    let cleanup = session.shutdown_and_wait_detailed().await;
                    let mut message = error;
                    if let Some(cleanup_error) = cleanup.error_message() {
                        message.push_str("; constructed Session cleanup failed: ");
                        message.push_str(&cleanup_error);
                    }
                    std::io::Error::other(message)
                }
            };
            let _ = send.send(Err(result));
        });

        let admission = match receive.await {
            Ok(Ok(admission)) => admission,
            Ok(Err(error)) => {
                // The retained owner sends an error only after the constructed
                // Session has completed its authoritative shutdown. Do not let
                // the caller-side cancellation guard start a second rollout
                // shutdown while returning that already-cleaned-up verdict.
                caller_guard.disarm();
                return Err(error);
            }
            Err(_) => {
                // No retained-owner verdict exists. Keep the guard armed so
                // dropping this caller still installs the unpublished-Session
                // abandonment fence and retains teardown.
                return Err(std::io::Error::other(
                    "recovered Session admission owner ended without a durability verdict",
                ));
            }
        };
        // Receiving, acknowledging, and returning happen in one poll with no
        // intervening await. Once the owner sees this ACK, cancellation can no
        // longer strand a Session in the oneshot slot.
        if admission.accepted.send(()).is_err() {
            let cleanup = admission.session.shutdown_and_wait_detailed().await;
            let mut message =
                "recovered Session admission owner ended before accepting the handoff".to_string();
            if let Some(cleanup_error) = cleanup.error_message() {
                message.push_str("; constructed Session cleanup failed: ");
                message.push_str(&cleanup_error);
            }
            // This explicit await established the same complete cleanup
            // boundary the retained owner would have owned. Avoid a duplicate
            // abandonment shutdown while returning the handoff failure.
            caller_guard.disarm();
            return Err(std::io::Error::other(message));
        }
        caller_guard.disarm();
        Ok(admission.session)
    }

    async fn offer_resumed_session<AfterOffer>(
        session: Arc<Session>,
        send: tokio::sync::oneshot::Sender<std::io::Result<ResumedSessionAdmission>>,
        after_offer: AfterOffer,
    ) where
        AfterOffer: FnOnce() + Send + 'static,
    {
        let (accepted, acceptance) = tokio::sync::oneshot::channel();
        let offered = send.send(Ok(ResumedSessionAdmission {
            session: session.clone(),
            accepted,
        }));
        if offered.is_err() {
            let _ = session.shutdown_and_wait_detailed().await;
            return;
        }
        let accepted = std::panic::AssertUnwindSafe(async move {
            after_offer();
            acceptance.await
        })
        .catch_unwind()
        .await;
        if !matches!(accepted, Ok(Ok(()))) {
            let _ = session.shutdown_and_wait_detailed().await;
        }
    }

    /// Build the engine deps: production deps from the provider + overrides, with a
    /// compactor (host-supplied wins, else the real micro+full compactor).
    fn build_deps(&self, provider: Arc<dyn ModelProvider>, config: &ResolvedConfig) -> EngineDeps {
        let base = production_deps(provider.clone(), self.deps_overrides.to_overrides());
        let user_compactor = self
            .opts
            .compactor
            .clone()
            .or_else(|| self.deps_overrides.compactor.clone());
        let compactor: Arc<dyn Compactor> = user_compactor.unwrap_or_else(|| {
            // The compactor's model-call + uuid closures are reconstructed from the
            // provider (matching production_deps' own construction), since the base
            // deps' closures are not shareable.
            let provider = provider.clone();
            let call_model: CallModel = Box::new(move |req, signal| provider.stream(req, signal));
            let uuid: crate::context::compact::full_compact::UuidFn = Box::new(new_uuid);
            let real: Arc<dyn crate::context::compact::engine::Compactor> =
                Arc::new(create_compactor(CompactorOptions {
                    call_model,
                    // Summarize on the conversation model; a mapped tier can have
                    // a smaller context window. Live turn overrides arrive through
                    // prepare_request together with the actual cache prefix.
                    model: config.model.clone(),
                    max_output_tokens: config.max_output_tokens,
                    uuid,
                }));
            let built: Arc<dyn Compactor> = Arc::new(RealCompactorBridge(real));
            built
        });
        EngineDeps { compactor, ..base }
    }
}

/// Capture the current native working directory as exact runtime path identity.
fn default_cwd() -> RuntimePath {
    std::env::current_dir()
        .map(|path| runtime_path_from_host_path(&path))
        .unwrap_or_default()
}

#[cfg(unix)]
fn runtime_path_from_host_path(path: &Path) -> RuntimePath {
    use std::os::unix::ffi::OsStrExt;

    RuntimePath::from(NodeBufferUtf8::decode(path.as_os_str().as_bytes()))
}

#[cfg(windows)]
fn runtime_path_from_host_path(path: &Path) -> RuntimePath {
    use std::os::windows::ffi::OsStrExt;

    RuntimePath::from(RuntimeString::from_utf16(
        path.as_os_str().encode_wide().collect::<Vec<_>>(),
    ))
}

#[cfg(not(any(unix, windows)))]
fn runtime_path_from_host_path(path: &Path) -> RuntimePath {
    RuntimePath::from(
        path.to_str()
            .expect("supported host paths must be Unicode scalar strings"),
    )
}

/// Named host projection for disk workflow discovery.
fn runtime_path_to_workflow_discovery_host(path: &RuntimePath) -> String {
    String::from_utf8(path.as_runtime_string().to_node_utf8())
        .expect("Node UTF-8 path projection is valid UTF-8")
}

/// Named host projection for persisted-worktree safety validation.
fn runtime_path_to_worktree_recovery_host(path: &RuntimePath) -> String {
    String::from_utf8(path.as_runtime_string().to_node_utf8())
        .expect("Node UTF-8 path projection is valid UTF-8")
}

fn join_runtime_strings(values: &[RuntimeString], separator: &str) -> RuntimeString {
    let separator = RuntimeString::from(separator);
    let mut joined = RuntimeString::default();
    for (index, value) in values.iter().enumerate() {
        if index != 0 {
            joined = joined.concat(&separator);
        }
        joined = joined.concat(value);
    }
    joined
}

/// Mint a fresh random (v4) UUID string (for a session id / compaction marker).
fn new_uuid() -> String {
    use rand::RngCore;
    let mut b = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut b);
    // Set the version (4) and RFC-4122 variant bits.
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[0],
        b[1],
        b[2],
        b[3],
        b[4],
        b[5],
        b[6],
        b[7],
        b[8],
        b[9],
        b[10],
        b[11],
        b[12],
        b[13],
        b[14],
        b[15]
    )
}

/// Recover the session's active worktree (if any) from its rollout: the LATEST
/// `worktree_state` event wins — a null payload means it was exited. The payload
/// is validated for both shape and path safety before it is trusted.
fn latest_worktree_state(
    raw_items: &[RolloutItem],
    session_cwd: &RuntimePath,
    has_worktree_hook: bool,
) -> Option<WorktreeSession> {
    let mut latest: Option<WorktreeSession> = None;
    for item in raw_items {
        let ev = match item {
            RolloutItem::Event(ev) => ev,
            _ => continue,
        };
        if ev.event != "worktree_state" {
            continue;
        }
        latest = match &ev.payload {
            None => None,
            Some(p) => parse_worktree_session(p),
        };
    }
    let latest = latest?;
    safe_resumed_worktree(latest, session_cwd, has_worktree_hook)
}

/// Reject a resumed worktree whose paths a tampered rollout could use to redirect
/// the session's cwd to an arbitrary location (fail-closed).
fn safe_resumed_worktree(
    wt: WorktreeSession,
    session_cwd: &RuntimePath,
    has_worktree_hook: bool,
) -> Option<WorktreeSession> {
    if !crate::fs::node_path::is_absolute_runtime(&wt.worktree_path)
        || !crate::fs::node_path::is_absolute_runtime(&wt.original_cwd)
    {
        return None;
    }
    let root = repo_root(session_cwd)?;
    let root_host = runtime_path_to_worktree_recovery_host(&root);
    let original_host = runtime_path_to_worktree_recovery_host(&wt.original_cwd);
    let worktree_host = runtime_path_to_worktree_recovery_host(&wt.worktree_path);
    if !is_path_inside(&original_host, &root_host, true) {
        return None;
    }
    if wt.hook_based == Some(true) {
        return if has_worktree_hook && is_existing_dir(&worktree_host) {
            Some(wt)
        } else {
            None
        };
    }
    let worktrees_host = runtime_path_to_worktree_recovery_host(&worktrees_dir(&root));
    if is_path_inside(&worktree_host, &worktrees_host, false) {
        Some(wt)
    } else {
        None
    }
}

/// True when `child` resolves (symlinks included) to a descendant of `parent` (or,
/// when `allow_equal`, to `parent` itself).
fn is_path_inside(child: &str, parent: &str, allow_equal: bool) -> bool {
    let c = match canonicalize(child) {
        Some(c) => c,
        None => return false,
    };
    let p = match canonicalize(parent) {
        Some(p) => p,
        None => return false,
    };
    if c == p {
        return allow_equal;
    }
    Path::new(&c).starts_with(Path::new(&p))
}

/// Realpath a path (resolving symlinks); `None` if it doesn't exist / can't resolve.
fn canonicalize(p: &str) -> Option<String> {
    crate::fs::node_path::canonicalize_host(p)
        .ok()
        .and_then(|path| path.into_os_string().into_string().ok())
}

/// Whether `p` is an existing directory.
fn is_existing_dir(p: &str) -> bool {
    std::fs::metadata(p).map(|m| m.is_dir()).unwrap_or(false)
}

/// Recover the admission watermark independently of physical trace append order.
fn prediction_source_from_rollout(
    raw_items: &[RolloutItem],
) -> std::io::Result<Option<(u64, Option<RuntimeString>)>> {
    let invalid = || {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid prediction source admission checkpoint",
        )
    };
    let mut latest: Option<(u64, Option<RuntimeString>)> = None;
    let mut seen = HashMap::new();
    for item in raw_items {
        let RolloutItem::Event(event) = item else {
            continue;
        };
        if event.event != crate::engine::session::PREDICTION_SOURCE_ROLLOUT_EVENT {
            continue;
        }
        let raw = event
            .payload
            .as_ref()
            .and_then(RuntimeJson::as_object)
            .and_then(|payload| payload.get("generation"))
            .and_then(RuntimeJson::as_str)
            .and_then(|generation| generation.try_to_utf8().ok())
            .ok_or_else(invalid)?;
        let generation = raw
            .parse::<u64>()
            .ok()
            .filter(|generation| {
                *generation > 0
                    && generation.checked_add(1).is_some()
                    && generation.to_string() == raw
            })
            .ok_or_else(invalid)?;
        if event.turn_id.as_ref().is_some_and(RuntimeString::is_empty) {
            return Err(invalid());
        }
        if let Some(previous) = seen.insert(generation, event.turn_id.clone()) {
            if previous != event.turn_id {
                return Err(invalid());
            }
        }
        if latest
            .as_ref()
            .is_none_or(|(current, _)| *current < generation)
        {
            latest = Some((generation, event.turn_id.clone()));
        }
    }
    Ok(latest)
}

#[cfg(test)]
mod prediction_source_checkpoint_tests {
    use super::*;
    use crate::schemas::rollout::{EventItem, RollbackItem};

    fn checkpoint(generation: RuntimeJson, turn_id: Option<&str>) -> RolloutItem {
        let mut payload = crate::runtime_values::RuntimeObject::new();
        payload.insert("generation", generation);
        RolloutItem::Event(EventItem {
            event: crate::engine::session::PREDICTION_SOURCE_ROLLOUT_EVENT.into(),
            turn_id: turn_id.map(RuntimeString::from),
            payload: Some(RuntimeJson::Object(payload)),
        })
    }

    #[test]
    fn prediction_source_replay_preserves_exact_large_generations_despite_append_order() {
        let newest = (1u64 << 53) + 2;
        let older = newest - 1;
        let items = vec![
            checkpoint(RuntimeJson::from(newest.to_string()), Some("newest")),
            checkpoint(RuntimeJson::from(older.to_string()), Some("older")),
        ];
        assert_eq!(
            prediction_source_from_rollout(&items).unwrap(),
            Some((newest, Some("newest".into())))
        );
    }

    #[test]
    fn prediction_source_replay_retains_clear_watermark_above_delayed_sources() {
        let items = vec![
            checkpoint(RuntimeJson::from("8"), Some("removed")),
            checkpoint(RuntimeJson::from("10"), None),
            RolloutItem::Rollback(RollbackItem { num_turns: 1 }),
            checkpoint(RuntimeJson::from("9"), Some("delayed")),
        ];
        assert_eq!(
            prediction_source_from_rollout(&items).unwrap(),
            Some((10, None))
        );
    }

    #[test]
    fn prediction_source_replay_rejects_invalid_or_conflicting_checkpoints() {
        for value in [
            "0",
            "01",
            "+1",
            "invalid",
            "18446744073709551615",
            "18446744073709551616",
        ] {
            assert!(
                prediction_source_from_rollout(&[checkpoint(
                    RuntimeJson::from(value),
                    Some("turn")
                )])
                .is_err(),
                "{value}"
            );
        }
        assert!(
            prediction_source_from_rollout(&[checkpoint(RuntimeJson::from(1), Some("turn"))])
                .is_err()
        );
        let first = checkpoint(RuntimeJson::from("1"), Some("first"));
        assert!(prediction_source_from_rollout(&[
            first.clone(),
            checkpoint(RuntimeJson::from("2"), Some("newest")),
            checkpoint(RuntimeJson::from("1"), Some("conflicting older")),
        ])
        .is_err());
        assert!(prediction_source_from_rollout(&[
            first.clone(),
            checkpoint(RuntimeJson::from("1"), Some("other"))
        ])
        .is_err());
        assert_eq!(
            prediction_source_from_rollout(&[first.clone(), first]).unwrap(),
            Some((1, Some("first".into())))
        );
        assert_eq!(prediction_source_from_rollout(&[]).unwrap(), None);
    }
}

/// Recover the workflow result journal from a session's rollout: every
/// `workflow_agent` event's payload, validated for shape.
fn workflow_journal_from_rollout(raw_items: &[RolloutItem]) -> Vec<WorkflowJournalEntry> {
    let mut out: Vec<WorkflowJournalEntry> = Vec::new();
    for item in raw_items {
        let ev = match item {
            RolloutItem::Event(ev) => ev,
            _ => continue,
        };
        if ev.event != "workflow_agent" {
            continue;
        }
        if let Some(entry) = parse_workflow_journal_entry(ev.payload.as_ref()) {
            out.push(entry);
        }
    }
    out
}

/// Recover the tool_search-discovered tools from a session's rollout: the union of
/// every `tools_surfaced` event's `names`, first-seen order preserved. Tolerant of
/// malformed payloads.
fn surfaced_tools_from_rollout(raw_items: &[RolloutItem]) -> Vec<RuntimeString> {
    let mut seen: HashSet<RuntimeString> = HashSet::new();
    let mut out: Vec<RuntimeString> = Vec::new();
    for item in raw_items {
        let ev = match item {
            RolloutItem::Event(ev) => ev,
            _ => continue,
        };
        if ev.event != "tools_surfaced" {
            continue;
        }
        let obj = match ev.payload.as_ref().and_then(|p| p.as_object()) {
            Some(o) => o,
            None => continue,
        };
        if let Some(RuntimeJson::Array(names)) = obj.get("names") {
            for n in names {
                if let RuntimeJson::String(s) = n {
                    if seen.insert(s.clone()) {
                        out.push(s.clone());
                    }
                }
            }
        }
    }
    out
}

/// Recover immutable billing records in rollout order. Typed records retain the
/// original computed USD value. Top-level assistant messages are used only for
/// legacy or partially upgraded rollouts and are deduplicated by message id;
/// compaction replacement histories are deliberately never scanned.
pub(crate) fn usage_ledger_from_rollout(
    meta: &SessionMeta,
    raw_items: &[RolloutItem],
    pricing: Option<&PricingTable>,
) -> Vec<UsageLedgerItem> {
    let (_, usage) = crate::store::runtime_resume::history_metadata(meta, raw_items);
    usage_ledger_from_runtime(&usage, pricing)
}

fn usage_ledger_from_runtime(
    usage: &[crate::store::runtime_resume::RuntimeUsage],
    pricing: Option<&PricingTable>,
) -> Vec<UsageLedgerItem> {
    use crate::store::runtime_resume::RuntimeUsage;
    let typed_assistant_ids = usage
        .iter()
        .filter_map(|item| match item {
            RuntimeUsage::Typed(record) => Some(record.assistant_message_id.clone()),
            RuntimeUsage::Legacy { .. } => None,
        })
        .collect::<HashSet<_>>();
    let mut legacy_pricer = match pricing {
        Some(table) => CostTracker::with_pricing(table.clone()),
        None => CostTracker::new(),
    };
    let mut seen_requests = HashSet::new();
    let mut seen_assistants = HashSet::new();
    let mut records = Vec::new();

    for item in usage {
        match item {
            RuntimeUsage::Typed(record) => {
                if record.request_id.is_empty()
                    || record.assistant_message_id.is_empty()
                    || !seen_requests.insert(record.request_id.clone())
                    || !seen_assistants.insert(record.assistant_message_id.clone())
                {
                    continue;
                }
                records.push(record.clone());
            }
            RuntimeUsage::Legacy {
                assistant_message_id,
                model,
                usage,
            } if !typed_assistant_ids.contains(assistant_message_id)
                && seen_assistants.insert(assistant_message_id.clone()) =>
            {
                let Some(usage) = usage else {
                    continue;
                };
                let request_id = assistant_message_id.clone();
                if !seen_requests.insert(request_id.clone()) {
                    continue;
                }
                let Ok(request_id_scalar) = request_id.try_to_utf8() else {
                    continue;
                };
                let Ok(assistant_id_scalar) = assistant_message_id.try_to_utf8() else {
                    continue;
                };
                let Ok(model_scalar) = model.try_to_utf8() else {
                    continue;
                };
                records.push(legacy_pricer.record(
                    request_id_scalar,
                    assistant_id_scalar,
                    &model_scalar,
                    usage,
                ));
                // Recovery needs each immutable record, not an intermediate
                // running total that could overflow on malformed old usage.
                legacy_pricer.reset();
            }
            _ => {}
        }
    }

    records
}

/// Select the final persisted lifecycle state for each agent in rollout order.
/// The registry owns stable snapshot ordering; recovery ordering only decides
/// which transition wins for a repeated `agentId`.
fn subagent_lifecycle_from_rollout(raw_items: &[RolloutItem]) -> Vec<SubagentLifecycleRecord> {
    let mut latest = BTreeMap::new();
    for item in raw_items {
        if let RolloutItem::SubagentLifecycle(item) = item {
            latest.insert(item.record.agent_id.clone(), item.record.clone());
        }
    }
    latest.into_values().collect()
}

/// Select the final persisted lifecycle state for each shell in rollout order.
fn background_processes_from_rollout(raw_items: &[RolloutItem]) -> Vec<BackgroundProcessRecord> {
    let mut latest = BTreeMap::new();
    for item in raw_items {
        if let RolloutItem::BackgroundProcessLifecycle(item) = item {
            latest.insert(item.record.shell_id.clone(), item.record.clone());
        }
    }
    latest.into_values().collect()
}

/// The latest persisted plan-document name, when the session ever bound one.
/// Malformed records are skipped: an unreadable name only costs plan-file
/// continuity, never resumability.
///
/// The payload key stays `"slug"` — the event is engine-internal (it is appended
/// with `turn_id: None`, so the facade never projects it to a client), and keeping
/// the old key means records written before the document was model-named still
/// read back unchanged. Renaming it would strand every existing session's plan
/// file to save nothing.
fn plan_name_from_rollout(raw_items: &[RolloutItem]) -> Option<RuntimeString> {
    raw_items.iter().rev().find_map(|item| match item {
        RolloutItem::Event(event) if event.event == PLAN_SLUG_ROLLOUT_EVENT => event
            .payload
            .as_ref()
            .and_then(|payload| payload.get("slug"))
            .and_then(RuntimeJson::as_string)
            .cloned(),
        _ => None,
    })
}

/// Decode only the newest complete queue checkpoint. Queue state is
/// authoritative rather than diagnostic: a malformed latest record must fail
/// resume instead of silently resurrecting an older state or an empty queue.
#[cfg(test)]
fn queue_from_rollout(
    raw_items: &[RolloutItem],
    expected_session_id: &RuntimeString,
) -> std::io::Result<Option<(MessageQueue, bool)>> {
    queue_from_runtime(raw_items, expected_session_id, None)
}

fn queue_from_runtime(
    raw_items: &[RolloutItem],
    expected_session_id: &RuntimeString,
    all_persisted_users: Option<&HashSet<RuntimeString>>,
) -> std::io::Result<Option<(MessageQueue, bool)>> {
    let Some(event) = raw_items.iter().rev().find_map(|item| match item {
        RolloutItem::Event(event) if event.event == QUEUE_STATE_ROLLOUT_EVENT => Some(event),
        _ => None,
    }) else {
        return Ok(None);
    };
    let payload = event.payload.as_ref().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "latest queue checkpoint is missing its payload",
        )
    })?;
    let state = runtime_codec::decode_value::<QueuePersistenceState>(payload).map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("latest queue checkpoint is malformed: {error}"),
        )
    })?;
    let persisted_user_uuids = raw_items
        .iter()
        .filter_map(|item| match item {
            RolloutItem::Message {
                message: Message::User(message),
            } => Some(message.uuid.clone()),
            _ => None,
        })
        .collect();
    MessageQueue::from_persistence_state_with_history(
        state,
        expected_session_id,
        all_persisted_users.unwrap_or(&persisted_user_uuids),
    )
    .map(Some)
    .map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("latest queue checkpoint is invalid: {error}"),
        )
    })
}

/// Replay persisted transitions into the same bounded authoritative snapshot
/// used by a live Session. Malformed/unknown audit payloads are ignored so one
/// historical diagnostic record cannot make the conversation unresumable.
fn memory_maintenance_from_rollout(raw_items: &[RolloutItem]) -> MemoryMaintenanceSnapshot {
    let mut snapshot = MemoryMaintenanceSnapshot::default();
    // The public snapshot intentionally retains only 64 audits per kind, but
    // replay still sees the full rollout. Preserve terminal IDs for the whole
    // replay pass so an old Started record cannot resurrect a terminal run after
    // that run has fallen out of the query window.
    let mut terminal_auto_memory_ids = HashSet::new();
    let mut terminal_auto_dream_ids = HashSet::new();
    for item in raw_items {
        let RolloutItem::Event(event) = item else {
            continue;
        };
        if event.event != MEMORY_MAINTENANCE_ROLLOUT_EVENT {
            continue;
        }
        let Some(payload) = event.payload.as_ref() else {
            continue;
        };
        let Ok(event) = runtime_codec::decode_value::<MemoryMaintenanceEvent>(payload) else {
            continue;
        };
        let already_terminal = match &event {
            MemoryMaintenanceEvent::AutoMemory { run } => {
                terminal_auto_memory_ids.contains(&run.run_id)
            }
            MemoryMaintenanceEvent::AutoDream { run } => {
                terminal_auto_dream_ids.contains(&run.dream_run_id)
            }
        };
        if already_terminal || !snapshot.apply(&event) {
            continue;
        }
        match &event {
            MemoryMaintenanceEvent::AutoMemory { run }
                if run.status != MemoryMaintenanceStatus::Started =>
            {
                terminal_auto_memory_ids.insert(run.run_id.clone());
            }
            MemoryMaintenanceEvent::AutoDream { run }
                if run.status != MemoryMaintenanceStatus::Started =>
            {
                terminal_auto_dream_ids.insert(run.dream_run_id.clone());
            }
            _ => {}
        }
    }
    snapshot
}

/// Replay Setup lifecycle transitions. A rollout containing a real turn but no
/// Setup events predates this contract; on resume its init gate must still be
/// closed rather than allowing initialization after conversation history has
/// already reached SessionStart/the provider.
fn setup_lifecycle_from_rollout(raw_items: &[RolloutItem]) -> SetupLifecycleSnapshot {
    let mut snapshot = SetupLifecycleSnapshot::default();
    let mut has_real_turn = false;
    for item in raw_items {
        match item {
            RolloutItem::TurnContext { .. } => has_real_turn = true,
            RolloutItem::Event(event) if event.event == SETUP_LIFECYCLE_ROLLOUT_EVENT => {
                let Some(payload) = event.payload.as_ref() else {
                    continue;
                };
                if let Ok(event) = runtime_codec::decode_value::<SetupLifecycleEvent>(payload) {
                    snapshot.apply(&event);
                }
            }
            _ => {}
        }
    }
    if has_real_turn && snapshot.init_closed_at.is_none() {
        snapshot.close_legacy_init();
    }
    snapshot
}

/// Restore the last authoritative audit transition for each deterministic
/// snapshot request. Malformed historical diagnostics are ignored.
fn agent_memory_snapshot_decisions_from_rollout(
    raw_items: &[RolloutItem],
) -> Vec<AgentMemorySnapshotDecisionAudit> {
    let mut latest = BTreeMap::new();
    for item in raw_items {
        let RolloutItem::Event(event) = item else {
            continue;
        };
        if event.event != AGENT_MEMORY_SNAPSHOT_DECISION_ROLLOUT_EVENT {
            continue;
        }
        let Some(payload) = event.payload.as_ref() else {
            continue;
        };
        if let Ok(audit) = runtime_codec::decode_value::<AgentMemorySnapshotDecisionAudit>(payload)
        {
            latest.insert(audit.request.request_id.clone(), audit);
        }
    }
    latest.into_values().collect()
}

/// Recover code-mode cells that were yielded but never completed/terminated in
/// the persisted trace. The JS runtime itself is process-local, so resume cannot
/// restore the cell; it can only prevent the model from waiting on a lost cell id.
fn code_mode_recovery_notice(raw_items: &[RolloutItem]) -> Option<Message> {
    let cells = code_mode_in_flight_cells(raw_items);
    if cells.is_empty() {
        return None;
    }
    let cell_list = join_runtime_strings(&cells, ", ");
    let text = RuntimeString::from(
        "Code-mode runtime state from the previous process cannot be resumed. The following exec cell_id values were still in-flight: ",
    )
    .concat(&cell_list)
    .concat(&RuntimeString::from(
        ". Do not call wait for those cell_id values. Nested tool calls may already have completed or partially executed: inspect completed results and the current state before repeating side effects. Reuse finished outputs, and start a new exec cell only for work that is still needed.",
    ));
    Some(Message::User(crate::schemas::message::UserMessage {
        question_answer: None,
        uuid: "code-mode-recovery".into(),
        is_meta: Some(true),
        content: vec![crate::schemas::content::ContentBlock::Text(
            crate::schemas::content::TextBlock {
                text,
                citations: None,
                harness_envelope: Some(crate::schemas::content::HarnessEnvelope::SystemReminder),
                message_id: None,
                phase: None,
            },
        )],
    }))
}

fn code_mode_in_flight_cells(raw_items: &[RolloutItem]) -> Vec<RuntimeString> {
    let mut in_flight: Vec<CodeModeCellKey> = Vec::new();
    let mut seen: HashSet<CodeModeCellKey> = HashSet::new();
    for item in raw_items {
        let ev = match item {
            RolloutItem::Event(ev) => ev,
            _ => continue,
        };
        let Some(cell) = code_mode_cell_key(ev.payload.as_ref()) else {
            continue;
        };
        if ev.event == CODE_MODE_CELL_YIELDED_EVENT {
            if seen.insert(cell.clone()) {
                in_flight.push(cell);
            }
        } else if ev.event == CODE_MODE_CELL_COMPLETED_EVENT
            || ev.event == CODE_MODE_CELL_TERMINATED_EVENT
        {
            seen.remove(&cell);
            in_flight.retain(|key| key != &cell);
        }
    }
    in_flight.into_iter().map(|cell| cell.cell_id).collect()
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct CodeModeCellKey {
    cell_id: RuntimeString,
    runtime_id: Option<RuntimeString>,
}

fn code_mode_cell_key(payload: Option<&Json>) -> Option<CodeModeCellKey> {
    let obj = payload?.as_object()?;
    let cell_id = obj
        .get("cellId")
        .or_else(|| obj.get("cell_id"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .cloned()?;
    let runtime_id = obj
        .get("runtimeId")
        .or_else(|| obj.get("runtime_id"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .cloned();
    Some(CodeModeCellKey {
        cell_id,
        runtime_id,
    })
}

/// Validate one persisted journal entry; `None` if malformed. A non-empty runId, a
/// non-negative integer idx, a string hash, and a non-negative outputTokens are
/// required; `result` is any JSON value (including null).
fn parse_workflow_journal_entry(payload: Option<&Json>) -> Option<WorkflowJournalEntry> {
    let obj = payload?.as_object()?;
    let run_id = obj.get("runId").and_then(|v| v.as_str())?;
    if run_id.is_empty() {
        return None;
    }
    let idx = obj.get("idx").and_then(|v| v.as_u64())?;
    let hash = obj.get("hash").and_then(|v| v.as_str())?;
    let output_tokens = obj.get("outputTokens").and_then(|v| v.as_u64())?;
    let result = obj.get("result").cloned().unwrap_or(RuntimeJson::Null);
    Some(WorkflowJournalEntry {
        run_id: run_id.clone(),
        idx,
        hash: hash.clone(),
        result,
        output_tokens,
    })
}

/// Validate a persisted worktree payload, returning `None` if malformed. The three
/// path fields are required strings; the branch/commit fields and `hookBased` are
/// optional.
fn parse_worktree_session(payload: &Json) -> Option<WorktreeSession> {
    runtime_codec::decode_value(payload).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::browser::{attest_spawn_free, BrowserExecutor};
    use crate::capabilities::RuntimeCapabilityRoot;
    use crate::engine::message_queue::{
        QueueEdit, QueueMutationStatus, QueueTerminalState, QueuedValue,
    };
    use crate::engine::session::{EnqueueOptions, Input, TurnOptions};
    use crate::engine::subagent_lifecycle::{
        SubagentContext, SubagentKind, SubagentLifecycleRecord, SubagentStatus,
    };
    use crate::extensions::commands::{CommandMeta, PromptCommand};
    use crate::hooks::setup::{SetupRunRecord, SetupRunStatus, SetupTrigger};
    use crate::mcp::transports::{Transport, TransportEvent};
    use crate::mcp::types::{McpInProcessServer, McpServerConfig, McpServerOptions};
    use crate::memory::agent_memory::{AgentMemoryLayout, AgentMemoryScope};
    use crate::memory::maintenance::{
        AutoDreamRun, AutoDreamRunReason, AutoMemoryRun, AutoMemoryRunReason,
        MemoryMaintenanceStatus, MemoryMaintenanceTrigger,
    };
    use crate::memory::snapshot_decision::{
        prepare_agent_memory_snapshot_request, AgentMemorySnapshotDecisionStatus,
    };
    use crate::provider::openai::{OpenAIProvider, OpenAIProviderOptions, OpenAiApi};
    use crate::runtime_values::RuntimeObject;
    use crate::schemas::content::{ContentBlock, HarnessEnvelope, TextBlock, ToolUseBlock};
    use crate::schemas::message::{AssistantMessage, Usage, UserMessage};
    use crate::schemas::rollout::{EventItem, RolloutItem, TurnContextSnapshot};
    use crate::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
    use crate::store::session_store::{
        AppendEventOptions, CompactionRecord, LiveRollout, RollbackRecord,
    };
    use crate::testing::{ev, fake_provider, scripted_call_model};
    use async_trait::async_trait;
    use futures::StreamExt;
    use serde_json::json;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use tokio::sync::mpsc;

    static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

    macro_rules! exact_json {
        ($($json:tt)+) => {{
            crate::runtime_values::runtime_json::parse(&RuntimeString::from(stringify!($($json)+)))
                .expect("test JSON literal must parse as exact runtime JSON")
        }};
    }

    fn exact_tool_use_events(
        id: &str,
        name: &str,
        input: &RuntimeJson,
    ) -> Vec<crate::types::provider::ModelStreamEvent> {
        use crate::types::provider::ModelStreamEvent;

        vec![
            ModelStreamEvent::ToolUseStart {
                id: id.into(),
                name: name.into(),
                thought_signature: None,
                tool_kind: None,
                input_format: None,
            },
            ModelStreamEvent::ToolUseInputDelta {
                id: id.into(),
                partial_json: crate::runtime_values::runtime_json::stringify(input),
            },
            ModelStreamEvent::ToolUseStop { id: id.into() },
        ]
    }

    fn rpc_success(id: RuntimeJson, result: RuntimeJson) -> RuntimeJson {
        let mut message = RuntimeObject::new();
        message.insert("jsonrpc", RuntimeJson::from("2.0"));
        message.insert("id", id);
        message.insert("result", result);
        RuntimeJson::Object(message)
    }

    fn rpc_method_not_found(id: RuntimeJson) -> RuntimeJson {
        let mut message = RuntimeObject::new();
        message.insert("jsonrpc", RuntimeJson::from("2.0"));
        message.insert("id", id);
        message.insert(
            "error",
            exact_json!({ "code": -32601, "message": "method not found" }),
        );
        RuntimeJson::Object(message)
    }

    fn temp_dir() -> PathBuf {
        let mut dir = std::env::temp_dir();
        let unique = format!(
            "ae-agent-resume-{}-{}-{}",
            std::process::id(),
            TEMP_COUNTER.fetch_add(1, Ordering::SeqCst),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        dir.push(unique);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[derive(Debug, Clone, Copy)]
    enum QueueRecoveryFlushMode {
        Pass,
        BlockFirst,
        BlockFirstN(usize),
        FailFirst,
    }

    #[derive(Debug)]
    struct ControlledQueueRecoveryRollout {
        mode: QueueRecoveryFlushMode,
        flush_count: AtomicUsize,
        flush_entered: tokio::sync::Notify,
        flush_release: tokio::sync::Semaphore,
        shutdown_count: AtomicUsize,
        shutdown_started: AtomicUsize,
        shutdown_finished: tokio::sync::Notify,
    }

    impl ControlledQueueRecoveryRollout {
        fn new(mode: QueueRecoveryFlushMode) -> Arc<Self> {
            Arc::new(Self {
                mode,
                flush_count: AtomicUsize::new(0),
                flush_entered: tokio::sync::Notify::new(),
                flush_release: tokio::sync::Semaphore::new(0),
                shutdown_count: AtomicUsize::new(0),
                shutdown_started: AtomicUsize::new(0),
                shutdown_finished: tokio::sync::Notify::new(),
            })
        }

        async fn wait_for_first_flush(&self) {
            self.wait_for_flush_count(1).await;
        }

        async fn wait_for_flush_count(&self, expected: usize) {
            loop {
                let entered = self.flush_entered.notified();
                if self.flush_count.load(Ordering::SeqCst) >= expected {
                    return;
                }
                entered.await;
            }
        }

        fn release_first_flush(&self) {
            self.flush_release.add_permits(1);
        }

        fn release_flushes(&self, count: usize) {
            self.flush_release.add_permits(count);
        }

        async fn wait_for_shutdown(&self) {
            loop {
                let finished = self.shutdown_finished.notified();
                if self.shutdown_count.load(Ordering::SeqCst) > 0 {
                    return;
                }
                finished.await;
            }
        }

        async fn before_flush(&self) -> std::io::Result<()> {
            let call = self.flush_count.fetch_add(1, Ordering::SeqCst);
            let should_block = match self.mode {
                QueueRecoveryFlushMode::BlockFirst => call == 0,
                QueueRecoveryFlushMode::BlockFirstN(count) => call < count,
                QueueRecoveryFlushMode::Pass | QueueRecoveryFlushMode::FailFirst => false,
            };
            if !should_block && call != 0 {
                return Ok(());
            }
            self.flush_entered.notify_one();
            match self.mode {
                QueueRecoveryFlushMode::Pass => Ok(()),
                QueueRecoveryFlushMode::BlockFirst | QueueRecoveryFlushMode::BlockFirstN(_)
                    if should_block =>
                {
                    self.flush_release
                        .acquire()
                        .await
                        .expect("recovery flush semaphore remains open")
                        .forget();
                    Ok(())
                }
                QueueRecoveryFlushMode::BlockFirst | QueueRecoveryFlushMode::BlockFirstN(_) => {
                    Ok(())
                }
                QueueRecoveryFlushMode::FailFirst => Err(std::io::Error::other(
                    "injected recovered queue normalization failure",
                )),
            }
        }

        fn record_shutdown(&self) {
            self.shutdown_count.fetch_add(1, Ordering::SeqCst);
            self.shutdown_finished.notify_one();
        }
    }

    #[async_trait]
    impl crate::store::session_store::LiveRollout for ControlledQueueRecoveryRollout {
        async fn prepare_batch(
            &self,
            maximum_records: usize,
        ) -> std::io::Result<crate::store::session_store::PreparedRolloutBatch<'_>> {
            crate::testing::immediate_rollout::prepare_immediate_test_rollout_batch(
                self,
                maximum_records,
            )
        }

        async fn append_message(&self, _message: Message) {}

        async fn append_turn_context(&self, _ctx: TurnContextSnapshot) {}

        async fn append_compaction(&self, _record: CompactionRecord) {}

        async fn append_event(&self, _event: RuntimeString, _opts: Option<AppendEventOptions>) {}

        async fn append_rollback(&self, _record: RollbackRecord) {}

        async fn append_checkpoint(&self, _record: crate::store::session_store::CheckpointRecord) {}

        async fn append_usage(&self, _record: UsageLedgerItem) {}

        async fn append_subagent_lifecycle(&self, _record: SubagentLifecycleRecord) {}

        async fn append_background_process_lifecycle(&self, _record: BackgroundProcessRecord) {}

        async fn flush(&self) -> std::io::Result<()> {
            self.before_flush().await
        }

        async fn shutdown(&self) -> std::io::Result<()> {
            self.record_shutdown();
            Ok(())
        }
    }

    #[derive(Debug)]
    struct ControlledQueueRecoveryLiveRollout {
        inner: Box<dyn crate::store::session_store::LiveRollout>,
        control: Arc<ControlledQueueRecoveryRollout>,
    }

    impl ControlledQueueRecoveryLiveRollout {
        fn should_record_event(&self, event: &RuntimeString) -> bool {
            // Rejected checkpoints must not become durable during cleanup.
            !(event == QUEUE_STATE_ROLLOUT_EVENT
                && matches!(self.control.mode, QueueRecoveryFlushMode::FailFirst)
                && self.control.flush_count.load(Ordering::SeqCst) == 0)
        }
    }

    #[async_trait]
    impl crate::store::session_store::LiveRollout for ControlledQueueRecoveryLiveRollout {
        async fn prepare_batch(
            &self,
            maximum_records: usize,
        ) -> std::io::Result<crate::store::session_store::PreparedRolloutBatch<'_>> {
            let admission = self.inner.prepare_batch(maximum_records).await?;
            Ok(crate::store::session_store::PreparedRolloutBatch::new(
                move |items| {
                    assert!(items.len() <= maximum_records);
                    admission.commit(
                        items
                            .into_iter()
                            .filter(|item| match item {
                                RolloutItem::Event(event) => self.should_record_event(&event.event),
                                _ => true,
                            })
                            .collect(),
                    );
                },
            ))
        }

        async fn append_message(&self, message: Message) {
            self.inner.append_message(message).await;
        }

        async fn append_turn_context(&self, ctx: TurnContextSnapshot) {
            self.inner.append_turn_context(ctx).await;
        }

        async fn append_compaction(&self, record: CompactionRecord) {
            self.inner.append_compaction(record).await;
        }

        async fn append_event(&self, event: RuntimeString, opts: Option<AppendEventOptions>) {
            if !self.should_record_event(&event) {
                return;
            }
            self.inner.append_event(event, opts).await;
        }

        async fn append_rollback(&self, record: RollbackRecord) {
            self.inner.append_rollback(record).await;
        }

        async fn append_checkpoint(&self, record: crate::store::session_store::CheckpointRecord) {
            self.inner.append_checkpoint(record).await;
        }

        async fn append_usage(&self, record: UsageLedgerItem) {
            self.inner.append_usage(record).await;
        }

        async fn append_subagent_lifecycle(&self, record: SubagentLifecycleRecord) {
            self.inner.append_subagent_lifecycle(record).await;
        }

        async fn append_background_process_lifecycle(&self, record: BackgroundProcessRecord) {
            self.inner.append_background_process_lifecycle(record).await;
        }

        async fn flush(&self) -> std::io::Result<()> {
            self.control.before_flush().await?;
            self.inner.flush().await
        }

        async fn shutdown(&self) -> std::io::Result<()> {
            self.control.shutdown_started.fetch_add(1, Ordering::SeqCst);
            let result = self.inner.shutdown().await;
            self.control.record_shutdown();
            result
        }
    }

    struct ControlledQueueRecoveryStore {
        inner: Arc<JsonlSessionStore>,
        control: Arc<ControlledQueueRecoveryRollout>,
    }

    #[async_trait]
    impl SessionStore for ControlledQueueRecoveryStore {
        async fn create(&self, meta: SessionMeta) -> std::io::Result<Box<dyn LiveRollout>> {
            self.inner.create(meta).await
        }

        async fn resume(&self, thread_id: &str) -> std::io::Result<ResumeResult> {
            self.inner.resume(thread_id).await
        }

        async fn resume_for_write(&self, thread_id: &str) -> std::io::Result<ResumedLiveRollout> {
            let resumed = self.inner.resume_for_write(thread_id).await?;
            Ok(ResumedLiveRollout {
                resume: resumed.resume,
                rollout: Box::new(ControlledQueueRecoveryLiveRollout {
                    inner: resumed.rollout,
                    control: self.control.clone(),
                }),
            })
        }

        async fn reopen(&self, thread_id: &str) -> std::io::Result<Box<dyn LiveRollout>> {
            self.inner.reopen(thread_id).await
        }

        async fn list(
            &self,
            options: crate::store::session_store::ListOptions,
        ) -> std::io::Result<crate::store::session_store::ThreadPage> {
            self.inner.list(options).await
        }

        async fn archive(&self, thread_id: &str) -> std::io::Result<String> {
            self.inner.archive(thread_id).await
        }

        async fn unarchive(
            &self,
            thread_id: &str,
        ) -> crate::store::session_store::UnarchiveSessionResult {
            self.inner.unarchive(thread_id).await
        }

        async fn delete(&self, thread_id: &str) -> std::io::Result<()> {
            self.inner.delete(thread_id).await
        }

        async fn update_title(&self, thread_id: &str, title: &str) -> std::io::Result<()> {
            self.inner.update_title(thread_id, title).await
        }

        async fn open_sub_agent(
            &self,
            thread_id: &str,
            agent_id: &str,
            meta: crate::store::session_store::SubAgentMeta,
        ) -> std::io::Result<Box<dyn LiveRollout>> {
            self.inner.open_sub_agent(thread_id, agent_id, meta).await
        }

        async fn load_sub_agent(
            &self,
            thread_id: &str,
            agent_id: &str,
        ) -> std::io::Result<crate::store::session_store::SubAgentTranscript> {
            self.inner.load_sub_agent(thread_id, agent_id).await
        }

        async fn list_sub_agents(&self, thread_id: &str) -> std::io::Result<Vec<String>> {
            self.inner.list_sub_agents(thread_id).await
        }

        async fn count_sessions_touched_since(
            &self,
            since_ms: f64,
            exclude_id: Option<&str>,
        ) -> std::io::Result<usize> {
            self.inner
                .count_sessions_touched_since(since_ms, exclude_id)
                .await
        }
    }

    fn controlled_queue_recovery_store(
        inner: Arc<JsonlSessionStore>,
        control: Arc<ControlledQueueRecoveryRollout>,
    ) -> Arc<dyn SessionStore> {
        Arc::new(ControlledQueueRecoveryStore { inner, control })
    }

    async fn seed_reserved_queue_checkpoint(
        agent: &Agent,
        store: &Arc<JsonlSessionStore>,
    ) -> String {
        let session = agent
            .open_session(store.clone(), StartSessionOptions::default())
            .await
            .unwrap();
        session
            .enqueue(
                Input::Text("reserved across recovery".into()),
                EnqueueOptions::default(),
            )
            .unwrap();
        session.await_queue_durability().await.unwrap();
        let pending = session.queue_snapshot();
        let thread_id = session.id.try_to_utf8().unwrap();
        let queued_uuid = pending.items[0]
            .message
            .uuid
            .as_ref()
            .unwrap()
            .try_to_utf8()
            .unwrap();
        session.shutdown_and_wait().await.unwrap();

        let checkpoint = json!({
            "schemaVersion": 1,
            "sessionId": thread_id.clone(),
            "revision": pending.revision + 1,
            "items": [{
                "message": {
                    "value": "reserved across recovery",
                    "mode": "prompt",
                    "uuid": queued_uuid,
                    "sessionId": thread_id,
                },
                "pdfBytes": 0,
                "pdfDocuments": 0,
            }],
            "reserved": [queued_uuid],
            "tombstones": [],
            "mutations": [],
        });
        let payload = crate::runtime_values::runtime_json::parse(&RuntimeString::from(
            serde_json::to_string(&checkpoint).unwrap(),
        ))
        .unwrap();
        let live = store.reopen(&thread_id).await.unwrap();
        live.append_event(
            QUEUE_STATE_ROLLOUT_EVENT.into(),
            Some(AppendEventOptions {
                turn_id: None,
                payload: Some(payload),
            }),
        )
        .await;
        live.flush().await.unwrap();
        live.shutdown().await.unwrap();
        thread_id
    }

    async fn seed_constructor_recovery_prefixes(
        agent: &Agent,
        store: &Arc<JsonlSessionStore>,
    ) -> (RuntimeString, RuntimeString, RuntimeString, RuntimeString) {
        let session = agent
            .open_session(store.clone(), StartSessionOptions::default())
            .await
            .unwrap();
        let session_id = session.id.clone();
        session.shutdown_and_wait().await.unwrap();

        let setup_request_id = RuntimeString::from("resume-setup-started");
        let memory_run_id = RuntimeString::from("resume-memory-started");
        let snapshot_root = temp_dir();
        let snapshot_layout = crate::memory::agent_memory::default_agent_memory_layout(
            Some(snapshot_root.to_string_lossy().as_ref()),
            None,
        );
        let snapshot_dir = snapshot_layout.snapshot_dir_for("writer");
        std::fs::create_dir_all(&snapshot_dir).unwrap();
        std::fs::write(
            Path::new(&snapshot_dir).join("snapshot.json"),
            r#"{"updatedAt":"2026-07-18T00:00:00Z"}"#,
        )
        .unwrap();
        std::fs::write(Path::new(&snapshot_dir).join("MEMORY.md"), "snapshot").unwrap();
        let snapshot_request =
            prepare_agent_memory_snapshot_request(&snapshot_layout, "writer", 10, 20)
                .await
                .unwrap()
                .expect("the seeded snapshot differs from empty local memory");
        let snapshot_request_id = snapshot_request.request_id.clone();

        let rollout = store
            .reopen(&session_id.try_to_utf8().unwrap())
            .await
            .unwrap();
        rollout
            .append_event(
                SETUP_LIFECYCLE_ROLLOUT_EVENT.into(),
                Some(AppendEventOptions {
                    turn_id: None,
                    payload: Some(
                        runtime_codec::encode_value(&SetupLifecycleEvent::Run {
                            run: SetupRunRecord {
                                request_id: setup_request_id.clone(),
                                session_id: session_id.clone(),
                                trigger: SetupTrigger::Maintenance,
                                status: SetupRunStatus::Started,
                                started_at: 10,
                                ended_at: None,
                                hook_count: 1,
                                additional_context: Vec::new(),
                                system_messages: Vec::new(),
                                error: None,
                            },
                        })
                        .unwrap(),
                    ),
                }),
            )
            .await;
        rollout
            .append_event(
                MEMORY_MAINTENANCE_ROLLOUT_EVENT.into(),
                Some(AppendEventOptions {
                    turn_id: None,
                    payload: Some(
                        runtime_codec::encode_value(&MemoryMaintenanceEvent::AutoMemory {
                            run: AutoMemoryRun {
                                run_id: memory_run_id.clone(),
                                session_id: session_id.clone(),
                                trigger: MemoryMaintenanceTrigger::Auto,
                                scope: AgentMemoryScope::Project,
                                target_dir: "/memory".into(),
                                status: MemoryMaintenanceStatus::Started,
                                started_at: 10,
                                ended_at: None,
                                reason: None,
                                error: None,
                                message_count: 1,
                                cursor_message_uuid: None,
                                files_touched: Vec::new(),
                                turn_count: None,
                            },
                        })
                        .unwrap(),
                    ),
                }),
            )
            .await;
        rollout
            .append_event(
                AGENT_MEMORY_SNAPSHOT_DECISION_ROLLOUT_EVENT.into(),
                Some(AppendEventOptions {
                    turn_id: None,
                    payload: Some(
                        runtime_codec::encode_value(&AgentMemorySnapshotDecisionAudit {
                            request: snapshot_request,
                            status: AgentMemorySnapshotDecisionStatus::Requested,
                            decision: None,
                            source: None,
                            decided_at: None,
                            ended_at: None,
                            error: None,
                        })
                        .unwrap(),
                    ),
                }),
            )
            .await;
        rollout.flush().await.unwrap();
        rollout.shutdown().await.unwrap();
        std::fs::remove_dir_all(snapshot_root).ok();

        (
            session_id,
            setup_request_id,
            memory_run_id,
            snapshot_request_id,
        )
    }

    /// The rollout files the store directory holds right now. Lifecycle lock
    /// bookkeeping shares the directory, so only `.jsonl` entries count.
    fn rollout_files(dir: &Path) -> Vec<String> {
        let mut files = std::fs::read_dir(dir)
            .map(|entries| {
                entries
                    .filter_map(Result::ok)
                    .map(|entry| entry.file_name().to_string_lossy().into_owned())
                    .filter(|name| name.ends_with(".jsonl"))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        files.sort();
        files
    }

    fn store_at(dir: &Path) -> JsonlSessionStore {
        JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(
                dir.to_str()
                    .expect("test store directory is a scalar host path")
                    .to_owned(),
            ),
            archive_dir: None,
        })
    }

    fn meta() -> SessionMeta {
        SessionMeta {
            id: "sess-1".into(),
            created_at: 1_700_000_000,
            cwd: "/x".into(),
            model: "m".into(),
            title: None,
            originator: None,
            cli_version: None,
            source: None,
            role: None,
            parent_thread_id: None,
            forked_from_id: None,
            git: None,
            host_metadata: None,
        }
    }

    fn user(uuid: &str) -> Message {
        Message::User(UserMessage {
            question_answer: None,
            uuid: uuid.into(),
            content: vec![ContentBlock::Text(TextBlock {
                text: "prompt".into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_meta: None,
        })
    }

    fn is_skill_catalog_context(message: &Message) -> bool {
        matches!(message, Message::User(user)
            if user.is_meta == Some(true)
                && matches!(user.content.as_slice(), [ContentBlock::Text(text)]
                    if text.harness_envelope == Some(HarnessEnvelope::SystemReminder)
                        && text.text.starts_with("<skill-catalog:")))
    }

    fn text_message_content(message: &Message) -> &RuntimeString {
        let content = match message {
            Message::User(user) => &user.content,
            Message::Assistant(assistant) => &assistant.content,
            other => panic!("expected a text message, got {other:?}"),
        };
        let [ContentBlock::Text(text)] = content.as_slice() else {
            panic!("expected exactly one text block, got {content:?}");
        };
        &text.text
    }

    fn assistant_with_tool_use(uuid: &str, tool_use_id: &str) -> Message {
        Message::Assistant(AssistantMessage {
            uuid: uuid.into(),
            content: vec![ContentBlock::ToolUse(ToolUseBlock {
                id: tool_use_id.into(),
                name: "bash".into(),
                input: exact_json!({ "command": "echo hi" }),
                tool_kind: None,
                thought_signature: None,
            })],
            model: "m".into(),
            usage: None,
        })
    }

    #[test]
    fn side_task_retains_earlier_completed_context_without_an_active_tail() {
        let answer = |uuid: &str| {
            Message::Assistant(AssistantMessage {
                uuid: uuid.into(),
                content: vec![ContentBlock::Text(TextBlock {
                    text: "completed answer".into(),
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                })],
                model: "m".into(),
                usage: None,
            })
        };
        let first_user = user("user-1");
        let first_assistant = answer("assistant-1");
        let incomplete_user = user("user-2");
        assert_eq!(
            completed_context_prefix(
                &[first_user.clone(), first_assistant.clone(), incomplete_user,],
                &TurnIndex::default()
            ),
            vec![first_user.clone(), first_assistant.clone()]
        );

        let second_user = user("user-3");
        let second_assistant = answer("assistant-2");
        assert_eq!(
            completed_context_prefix(
                &[
                    first_user.clone(),
                    first_assistant.clone(),
                    second_user.clone(),
                    second_assistant.clone(),
                ],
                &TurnIndex::default()
            ),
            vec![first_user, first_assistant, second_user, second_assistant]
        );
    }

    #[test]
    fn completed_side_context_retains_a_checkpoint_before_any_new_completion() {
        let summary = Message::CompactBoundary(crate::schemas::message::CompactBoundaryMessage {
            uuid: "compacted-context".into(),
            summary: "Earlier project decisions".into(),
            native_compaction: None,
        });
        let messages = vec![
            user("old-request"),
            assistant_with_tool_use("old-call", "old-unpaired"),
            summary.clone(),
            user("active-after-compact"),
        ];
        let mut index = TurnIndex::default();
        index
            .message_turn
            .insert("active-after-compact".into(), "active".into());
        index.turn_status.insert("active".into(), "running".into());
        assert_eq!(completed_context_prefix(&messages, &index), vec![summary]);
    }

    #[test]
    fn completed_side_context_excludes_failed_and_interrupted_tool_tails() {
        let answer = |id: &str| {
            assistant_with_usage(
                id,
                Usage {
                    input_tokens: 1,
                    output_tokens: 1,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    total_tokens: None,
                },
            )
        };
        let summary = Message::CompactBoundary(crate::schemas::message::CompactBoundaryMessage {
            uuid: "summary".into(),
            summary: "Earlier decisions".into(),
            native_compaction: None,
        });
        let messages = vec![
            summary.clone(),
            user("done-user"),
            answer("done-answer"),
            user("failed-user"),
            assistant_with_tool_use("failed-call", "unresolved-failure"),
            user("next-user"),
            answer("next-answer"),
            user("active-user"),
            assistant_with_tool_use("active-call", "unresolved-active"),
        ];
        for excluded_status in ["failed", "interrupted"] {
            let mut index = TurnIndex::default();
            for (turn, status, ids) in [
                ("done", "completed", vec!["done-user", "done-answer"]),
                (
                    "excluded",
                    excluded_status,
                    vec!["failed-user", "failed-call"],
                ),
                ("next", "completed", vec!["next-user", "next-answer"]),
                ("active", "running", vec!["active-user", "active-call"]),
            ] {
                index.turn_status.insert(turn.into(), status.into());
                for id in ids {
                    index.message_turn.insert(id.into(), turn.into());
                }
            }
            let inherited = completed_context_prefix(&messages, &index);
            assert_eq!(
                inherited,
                vec![
                    summary.clone(),
                    messages[1].clone(),
                    messages[2].clone(),
                    messages[5].clone(),
                    messages[6].clone()
                ]
            );
            assert!(tools_are_paired(&inherited));
        }
    }

    #[tokio::test]
    async fn side_task_role_preserves_ordinary_tools_without_root_plan_progress() {
        let side_requests = Arc::new(Mutex::new(Vec::new()));
        let side = test_agent_with_runtime(
            None,
            Vec::new(),
            Some(recording_call_model(side_requests.clone())),
            None,
        )
        .start_session(StartSessionOptions {
            session_role: Some(SessionRole::SideTask),
            ..Default::default()
        });
        side.run(Input::Text("hello".into()), TurnOptions::default())
            .await;

        let main_requests = Arc::new(Mutex::new(Vec::new()));
        let main = test_agent_with_runtime(
            None,
            Vec::new(),
            Some(recording_call_model(main_requests.clone())),
            None,
        )
        .start_session(StartSessionOptions::default());
        main.run(Input::Text("hello".into()), TurnOptions::default())
            .await;

        let side_requests = side_requests.lock().unwrap();
        let main_requests = main_requests.lock().unwrap();
        assert_eq!(side_requests.len(), 1);
        assert_eq!(main_requests.len(), 1);
        let side_tools = side_requests[0]
            .tools
            .iter()
            .map(|tool| tool.name.clone())
            .collect::<Vec<_>>();
        let main_tools = main_requests[0]
            .tools
            .iter()
            .map(|tool| tool.name.clone())
            .collect::<Vec<_>>();
        assert!(!side_tools.is_empty());
        assert_eq!(
            main_tools
                .iter()
                .filter(|name| **name == "update_plan")
                .count(),
            1,
            "the main task owns exactly one execution progress control"
        );
        assert_eq!(
            side_tools
                .iter()
                .filter(|name| **name == "update_plan")
                .count(),
            0,
            "a side task cannot mutate the root progress checklist"
        );
        let ordinary_main_tools = main_tools
            .into_iter()
            .filter(|name| name != "update_plan")
            .collect::<Vec<_>>();
        assert_eq!(side_tools, ordinary_main_tools);
    }

    #[tokio::test]
    async fn side_task_hides_inherited_turn_and_writes_no_rollout() {
        let dir = temp_dir();
        let store = Arc::new(store_at(&dir));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let live = store.create(meta()).await.unwrap();
        live.append_message(user("parent-user")).await;
        live.append_message(assistant_with_usage(
            "parent-assistant",
            Usage {
                input_tokens: 2,
                output_tokens: 1,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
                total_tokens: None,
            },
        ))
        .await;
        live.shutdown().await.unwrap();
        let rollouts_before = rollout_files(&dir);

        let agent = test_agent_with_runtime(
            Some(json!({ "memory": { "enabled": false } })),
            Vec::new(),
            Some(recording_call_model(requests.clone())),
            None,
        );
        let side = agent
            .create_side_task(store.clone(), "sess-1", StartSessionOptions::default())
            .await
            .unwrap();
        let side_id = side.id.try_to_utf8().unwrap();

        let inherited_history = side.history();
        assert_eq!(inherited_history.len(), 2);
        assert!(side.visible_history().is_empty());
        let context = side.context_snapshot();
        assert_eq!(context.history_messages, 2);
        assert!(context.total_tokens > 0);

        side.run(Input::Text("side prompt".into()), TurnOptions::default())
            .await;
        let history = side.history();
        assert!(history.iter().any(is_skill_catalog_context));
        let conversation: Vec<_> = history
            .iter()
            .filter(|message| !is_skill_catalog_context(message))
            .collect();
        assert_eq!(conversation.len(), 4);
        assert_eq!(
            &conversation[..2],
            inherited_history.iter().collect::<Vec<_>>().as_slice()
        );
        assert!(matches!(conversation[2], Message::User(user) if user.is_meta != Some(true)));
        assert_eq!(text_message_content(conversation[2]), "side prompt");
        assert!(matches!(conversation[3], Message::Assistant(_)));
        assert_eq!(text_message_content(conversation[3]), "ok");
        let visible_history = side.visible_history();
        let visible_conversation: Vec<_> = visible_history
            .iter()
            .filter(|message| !is_skill_catalog_context(message))
            .collect();
        assert_eq!(visible_conversation.as_slice(), &conversation[2..]);
        assert!(visible_history.iter().all(|message| {
            inherited_history
                .iter()
                .all(|inherited| message_uuid(message) != message_uuid(inherited))
        }));
        {
            let requests = requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert!(requests[0].messages.iter().any(is_skill_catalog_context));
            assert_eq!(
                requests[0].messages.as_slice(),
                &history[..history.len() - 1]
            );
            assert!(requests[0].system.iter().any(|part| {
                part.name == "side-task"
                    && part.text.contains("inherited")
                    && part.text.contains("parent task")
                    && part
                        .text
                        .contains("never claim the prior conversation is unavailable")
                    && part.text.contains("Sub-agents are off-limits")
                    && part.text.contains("unless the user explicitly asks")
            }));
        }
        side.dispose().await;

        // Ephemeral: no rollout was created, so the side task left nothing behind
        // and cannot be resumed by id.
        assert_eq!(
            rollout_files(&dir),
            rollouts_before,
            "side task must not create a rollout"
        );
        assert!(test_agent()
            .resume_session(store.clone(), &side_id, StartSessionOptions::default())
            .await
            .is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn side_task_with_no_complete_parent_turn_keeps_its_first_turn_visible() {
        let dir = temp_dir();
        let store = Arc::new(store_at(&dir));
        let live = store.create(meta()).await.unwrap();
        live.append_message(user("incomplete-parent-user")).await;
        live.shutdown().await.unwrap();

        let agent = test_agent_with_runtime(
            None,
            Vec::new(),
            Some(scripted_call_model(vec![vec![
                ev::message_start(None),
                ev::text("first side answer"),
                ev::stop(None, None),
            ]])),
            None,
        );
        let side = agent
            .create_side_task(store.clone(), "sess-1", StartSessionOptions::default())
            .await
            .unwrap();
        assert!(side.history().is_empty());

        side.run(
            Input::Text("first side prompt".into()),
            TurnOptions::default(),
        )
        .await;
        let visible_history = side.visible_history();
        assert!(visible_history.iter().any(is_skill_catalog_context));
        assert_eq!(side.history(), visible_history);
        let conversation: Vec<_> = visible_history
            .iter()
            .filter(|message| !is_skill_catalog_context(message))
            .collect();
        assert_eq!(conversation.len(), 2);
        assert!(matches!(conversation[0], Message::User(user) if user.is_meta != Some(true)));
        assert_eq!(text_message_content(conversation[0]), "first side prompt");
        assert!(matches!(conversation[1], Message::Assistant(_)));
        assert_eq!(text_message_content(conversation[1]), "first side answer");
        assert!(visible_history
            .iter()
            .all(|message| message_uuid(message) != "incomplete-parent-user"));
        side.dispose().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    fn assistant_with_usage(uuid: &str, usage: Usage) -> Message {
        Message::Assistant(AssistantMessage {
            uuid: uuid.into(),
            content: vec![ContentBlock::Text(TextBlock {
                text: format!("answer {uuid}").into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            model: "fake-model".into(),
            usage: Some(usage),
        })
    }

    fn task_tool_turn() -> Vec<crate::types::provider::ModelStreamEvent> {
        let mut turn = vec![ev::message_start(None)];
        turn.extend(exact_tool_use_events(
            "task-call",
            "task",
            // Synchronous on purpose: both callers observe a child that already
            // finished inside the parent's turn, and a detached child outlives it.
            &exact_json!({"model": "low",
                "description": "recover lifecycle",
                "prompt": "complete the persisted child",
                "run_in_background": false
            }),
        ));
        turn.push(ev::stop(
            Some(crate::types::provider::ModelStopReason::ToolUse),
            None,
        ));
        turn
    }

    fn lifecycle_record(
        agent_id: &str,
        kind: SubagentKind,
        status: SubagentStatus,
        updated_at_ms: i64,
    ) -> SubagentLifecycleRecord {
        SubagentLifecycleRecord {
            agent_id: agent_id.into(),
            kind,
            status,
            context: SubagentContext {
                session_id: "sess-1".into(),
                parent_agent_id: Some("parent-agent".into()),
                parent_thread_id: Some("sess-1".into()),
                agent_type: Some("general-purpose".into()),
                cwd: "/x".into(),
                description: format!("recover {agent_id}").into(),
                depth: 1,
                resume_from: None,
                spawn_turn_id: None,
            },
            started_at_ms: 100,
            updated_at_ms,
            completed_at_ms: status.is_terminal().then_some(updated_at_ms),
            last_result: (status == SubagentStatus::Completed)
                .then(|| RuntimeString::from("workflow done")),
            last_error: (status == SubagentStatus::Failed)
                .then(|| RuntimeString::from("workflow failed")),
            cancellation_requested: status == SubagentStatus::Cancelled,
            model: None,
            model_source: None,
        }
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn task_resource_scope_does_not_inherit_private_extensions_or_plugin_hooks() {
        let directory =
            std::env::temp_dir().join(format!("task-scope-extensions-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let copied = calls.clone();
        let mut options = test_agent_options(None, vec![loaded_plugin()]);
        let requests: Arc<Mutex<Vec<ModelRequest>>> = Arc::new(Mutex::new(Vec::new()));
        options.deps.call_model = Some(recording_call_model(requests.clone()));
        options.skills = Some(Arc::new(move || {
            copied.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            vec![Skill {
                name: "private-owner-skill".into(),
                body: "private owner contents".into(),
                ..Default::default()
            }]
        }));
        let agent = Agent::new(options);
        calls.store(0, std::sync::atomic::Ordering::SeqCst);
        let session = agent.start_session(StartSessionOptions { cwd: Some(runtime_path_from_host_path(&directory)), mcp_tools: Some(vec![Arc::new(HostNamedTool::new("web_search")), Arc::new(HostNamedTool::new("send_message_to_friend")), Arc::new(HostNamedTool::new("mcp__computer_use__click"))]), config: Some(json!({"taskResourceScope":{"taskId":"isolated", "mode":"scoped-auto", "readRoots":[], "writeRoots":[]}, "memory":{"enabled":true,"user":true,"project":true,"local":true}})), ..Default::default() });
        let graph = session.extension_runtime_snapshot();
        assert!(graph.skills.is_empty());
        assert!(graph.agents.is_empty());
        assert!(graph.hooks.is_empty());
        assert!(graph.output_styles.is_empty());
        assert!(graph.plugin_runtime.entries.is_empty());
        assert!(!session.lsp_available());
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert!(agent.plugin_sessions.lock().unwrap().is_empty());
        session
            .run(Input::Text("hello".into()), TurnOptions::default())
            .await;
        let names = request_tool_names(&requests.lock().unwrap()[0]);
        for denied in [
            "REPL",
            "workflow",
            "save_memory",
            "snip",
            "task",
            "exec_command",
            "web_fetch",
            "plugin_manage",
            "CronCreate",
        ] {
            assert!(
                !names.iter().any(|name| *name == denied),
                "unexpected scoped tool {denied}"
            );
        }
        for allowed in [
            "read",
            "write",
            "bash",
            "think",
            "web_search",
            "send_message_to_friend",
            "mcp__computer_use__click",
        ] {
            assert!(
                names.iter().any(|name| *name == allowed),
                "missing scoped tool {allowed}"
            );
        }
        session.shutdown_and_wait().await.unwrap();
        std::fs::remove_dir_all(directory).unwrap();
    }

    fn approval_parent_seed() -> (
        Vec<Message>,
        super::super::approval_capture::ReviewParentCheckpoint,
    ) {
        let first = user("parent-input");
        let mut answer = user("parent-answer");
        let mut question = RuntimeObject::new();
        question.insert("id", RuntimeJson::String("question".into()));
        question.insert("question", RuntimeJson::String("May I publish?".into()));
        let mut answers = crate::runtime_values::RuntimeMap::new();
        answers.insert("question".into(), "Yes".into());
        if let Message::User(message) = &mut answer {
            message.question_answer = Some(Box::new(crate::engine::questions::QuestionAnswer {
                request_id: "request".into(),
                turn_id: "parent-turn".into(),
                tool_call_id: "question-call".into(),
                client_response_id: "response".into(),
                questions: vec![question],
                answers,
            }));
        }
        let mut context = super::super::approval_capture::ReviewParentHistory::default();
        context.accept(&first, "parent-turn").unwrap();
        context.accept(&answer, "parent-turn").unwrap();
        context
            .record_manual_approval(
                &json!({ "type": "command", "command": "publish", "cwd": "/workspace" }),
                "parent-turn",
            )
            .unwrap();
        (vec![first, answer], context.checkpoint())
    }

    #[tokio::test]
    async fn persistent_fork_preserves_inherited_review_provenance_across_resume() {
        use super::super::approval_capture::ReviewParentHistory;
        use super::super::approval_context::SectionHistory;
        let directory = temp_dir();
        let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(directory.to_str().unwrap().to_owned()),
            archive_dir: None,
        }));
        let agent = test_agent();
        let config_root = RuntimePath::from(directory.to_str().unwrap());
        let (messages, context) = approval_parent_seed();
        let parent = agent.start_session(StartSessionOptions {
            user_config_dir: Some(config_root.clone()),
            initial_history: Some(messages),
            initial_approval_context: Some(context),
            ..StartSessionOptions::default()
        });
        let snapshot = parent.persistent_fork_snapshot(None).unwrap();
        let fork = agent
            .fork_live_session(
                store.clone(),
                &snapshot,
                StartSessionOptions {
                    user_config_dir: Some(config_root.clone()),
                    ..StartSessionOptions::default()
                },
            )
            .await
            .unwrap();
        let fork_id = fork.id.try_to_utf8().unwrap();
        let captured = fork.persistent_fork_snapshot(None).unwrap();
        let context = ReviewParentHistory::restore(&captured.approval_context).snapshot();
        assert!(context
            .items
            .iter()
            .any(super::super::approval_context::is_inherited_manual_approval));
        assert_eq!(context.retained.verified_answers().count(), 0);
        assert!(!context.retained.verified_answers_complete());
        assert!(!context.items().any(|item| matches!(item, super::super::approval_context::model::ResponseItem::Message { role, .. } if role == "developer")));
        fork.shutdown_and_wait().await.unwrap();
        let resumed = agent
            .resume_session(
                store.clone(),
                &fork_id,
                StartSessionOptions {
                    user_config_dir: Some(config_root),
                    ..StartSessionOptions::default()
                },
            )
            .await
            .unwrap();
        let restored = resumed.persistent_fork_snapshot(None).unwrap();
        let context = ReviewParentHistory::restore(&restored.approval_context).snapshot();
        assert!(context
            .items
            .iter()
            .any(super::super::approval_context::is_inherited_manual_approval));
        assert_eq!(context.retained.verified_answers().count(), 0);
        assert!(!context.retained.verified_answers_complete());
        assert!(!context.items().any(|item| matches!(item, super::super::approval_context::model::ResponseItem::Message { role, .. } if role == "developer")));
        resumed.shutdown_and_wait().await.unwrap();
        parent.shutdown_and_wait().await.unwrap();
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn interrupted_fork_seed_replay_does_not_recapture_parent_answers() {
        use super::super::approval_capture::ReviewParentHistory;
        let directory = temp_dir();
        let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(directory.to_str().unwrap().to_owned()),
            archive_dir: None,
        }));
        let mut header = meta();
        header.cwd = directory.to_str().unwrap().into();
        header.role = Some(SessionRole::Main);
        header.forked_from_id = Some("parent".into());
        let thread_id = header.id.try_to_utf8().unwrap();
        let rollout = store.create(header).await.unwrap();
        let (messages, _) = approval_parent_seed();
        for message in messages {
            rollout.append_message(message).await;
        }
        // Creation stopped before its review checkpoint was written.
        rollout.flush().await.unwrap();
        rollout.shutdown().await.unwrap();
        let agent = test_agent();
        let session = agent
            .resume_session(
                store,
                &thread_id,
                StartSessionOptions {
                    user_config_dir: Some(RuntimePath::from(directory.to_str().unwrap())),
                    ..StartSessionOptions::default()
                },
            )
            .await
            .unwrap();
        let snapshot = session.persistent_fork_snapshot(None).unwrap();
        let context = ReviewParentHistory::restore(&snapshot.approval_context).snapshot();
        assert!(context.retained.has_inherited_user_messages());
        assert!(!context.retained.verified_answers_complete());
        assert_eq!(context.retained.verified_answers().count(), 0);
        session.shutdown_and_wait().await.unwrap();
        std::fs::remove_dir_all(directory).unwrap();
    }

    fn test_agent() -> Agent {
        test_agent_with_config(None)
    }

    fn test_agent_with_config(config: Option<Value>) -> Agent {
        test_agent_with_config_and_plugins(config, Vec::new())
    }

    fn test_agent_with_config_and_plugins(
        config: Option<Value>,
        plugins: Vec<LoadedPlugin>,
    ) -> Agent {
        test_agent_with_runtime(config, plugins, None, None)
    }

    fn test_agent_with_runtime(
        config: Option<Value>,
        plugins: Vec<LoadedPlugin>,
        call_model: Option<CallModel>,
        pricing: Option<HashMap<String, ModelPricing>>,
    ) -> Agent {
        Agent::new(AgentOptions {
            deps: EngineDepsOverrides {
                call_model,
                ..EngineDepsOverrides::default()
            },
            pricing,
            ..test_agent_options(config, plugins)
        })
    }

    /// Baseline options for the tests that need to override one field: `AgentOptions`
    /// has no `Default`, so without this every such test restates ~30 `None`s and the
    /// one line that matters gets lost in them.
    fn test_agent_options(config: Option<Value>, plugins: Vec<LoadedPlugin>) -> AgentOptions {
        AgentOptions {
            process_context: crate::runtime_process::test_runtime_process_context(),
            provider: Arc::new(fake_provider()),
            config,
            tools: None,
            web_fetch_page: None,
            browser_runtime: None,
            capability_roots: Vec::new(),
            cwd: None,
            deps: EngineDepsOverrides::default(),
            compactor: None,
            approval_handler: None,
            approval_reviewer: None,
            elicit_handler: None,
            credential_store: None,
            hooks: Vec::new(),
            observer: Some(None),
            logger: None,
            pricing: None,
            agents: None,
            include_default_agents: None,
            skills: None,
            output_styles: None,
            workflows: None,
            mcp_catalog: None,
            plugin_catalog: None,
            lsp_servers: None,
            agent_memory: None,
            mcp_clients: None,
            mcp_server_configs: Vec::new(),
            mcp_tools: None,
            refresh_tools: None,
            system_prompt_provider: None,
            plugins,
            plugin_load_report: None,
        }
    }

    fn recording_call_model(requests: Arc<Mutex<Vec<ModelRequest>>>) -> CallModel {
        Box::new(
            move |req: ModelRequest, signal: AbortSignal| -> ModelStream {
                requests.lock().unwrap().push(req);
                let turn = vec![
                    ev::message_start(None),
                    ev::text("ok"),
                    ev::stop(None, None),
                ];
                Box::pin(
                    futures::stream::iter(turn)
                        .take_while(move |_| futures::future::ready(!signal.is_cancelled())),
                )
            },
        )
    }

    #[tokio::test]
    async fn authenticated_cloud_requirements_are_loaded_before_session_admission() {
        use crate::engine::approval_requirements::{ManagedRequirementOrigin, ManagedReviewer};
        use crate::engine::approval_requirements_cloud::{
            ManagedReviewCloudAuth, ManagedReviewCloudAuthRecovery, ManagedReviewCloudAuthSource,
            ManagedReviewCloudIdentity,
        };
        use crate::types::permission::{ApprovalsReviewer, SandboxMode};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        struct CloudAuth {
            base: String,
            client: reqwest::Client,
        }
        #[async_trait]
        impl ManagedReviewCloudAuthSource for CloudAuth {
            async fn current(&self) -> Result<Option<ManagedReviewCloudAuth>, String> {
                Ok(Some(ManagedReviewCloudAuth {
                    identity: ManagedReviewCloudIdentity {
                        chatgpt_user_id: Some("test-user".into()),
                        account_id: Some("test-account".into()),
                        plan_type: Some("business".into()),
                        backend_base_url: self.base.clone(),
                        owner_generation: 1,
                    },
                    headers: std::collections::HashMap::from([(
                        "Authorization".into(),
                        "Bearer fixture-token".into(),
                    )]),
                    client: self.client.clone(),
                }))
            }
            async fn recover_unauthorized(
                &self,
                _step: usize,
                _owner: &ManagedReviewCloudIdentity,
            ) -> ManagedReviewCloudAuthRecovery {
                ManagedReviewCloudAuthRecovery::Unavailable
            }
        }
        struct CloudProvider {
            info: crate::types::provider::ProviderInfo,
            auth: Arc<CloudAuth>,
            calls: Arc<std::sync::atomic::AtomicUsize>,
        }
        impl ModelProvider for CloudProvider {
            fn info(&self) -> &crate::types::provider::ProviderInfo {
                &self.info
            }
            fn capabilities(
                &self,
                model: &RuntimeString,
            ) -> crate::types::provider::ProviderCapabilities {
                fake_provider().capabilities(model)
            }
            fn stream(&self, request: ModelRequest, _signal: AbortSignal) -> ModelStream {
                self.calls.fetch_add(1, Ordering::SeqCst);
                if request.model == "fallback-source" {
                    return Box::pin(futures::stream::iter(vec![ev::error(
                        crate::types::errors::ProviderError {
                            kind: crate::types::errors::ProviderErrorKind::InvalidRequest,
                            message: "The requested model was not found.".into(),
                            status: Some(404),
                            code: Some("model_not_found".into()),
                            error_type: None,
                            media_origins: None,
                            media_repair_complete: false,
                            credential_recovery: None,
                            retry_after_ms: None,
                            request_id: None,
                        },
                    )]));
                }
                Box::pin(futures::stream::iter(vec![
                    ev::message_start(request.model.try_to_utf8().ok().as_deref()),
                    ev::text("ok"),
                    ev::stop(None, None),
                ]))
            }
            fn managed_review_cloud_auth_source(
                &self,
            ) -> Option<Arc<dyn ManagedReviewCloudAuthSource>> {
                Some(self.auth.clone())
            }
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut bytes = [0; 1024];
                let count = stream.read(&mut bytes).await.unwrap();
                assert!(count > 0);
                assert!(
                    request.len() + count <= 8192,
                    "fixture request exceeded its header budget"
                );
                request.extend_from_slice(&bytes[..count]);
                if request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                    break;
                }
            }
            let request = String::from_utf8(request).unwrap();
            assert!(request.starts_with("GET /api/codex/config/bundle HTTP/1.1"));
            assert!(request
                .to_ascii_lowercase()
                .contains("authorization: bearer fixture-token"));
            let body = serde_json::json!({"requirements_toml":{"enterprise_managed":[{"id":"native-account-policy","name":"Administrator","contents":"allowed_approvals_reviewers = ['auto_review']\n[auto_review]\nrequired_on_models = ['blocked-model']"}]}}).to_string();
            let head = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
            stream.write_all(head.as_bytes()).await.unwrap();
            stream.write_all(body.as_bytes()).await.unwrap();
        });
        let directory = temp_dir();
        let mut options = test_agent_options(
            Some(
                json!({"agentMemory":false,"memoryRecall":false,"fork":false,"sandbox_mode":"danger-full-access"}),
            ),
            Vec::new(),
        );
        let model_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        options.provider = Arc::new(CloudProvider {
            info: fake_provider().info().clone(),
            calls: model_calls.clone(),
            auth: Arc::new(CloudAuth {
                base,
                client: reqwest::Client::builder().no_proxy().build().unwrap(),
            }),
        });
        let agent = Agent::new(options);
        let session_options = || StartSessionOptions {
            user_config_dir: Some(runtime_path_from_host_path(&directory)),
            ..Default::default()
        };
        let error = agent
            .try_start_session(session_options())
            .err()
            .expect("synchronous startup must require cloud admission");
        assert_eq!(error.code(), "approval_requirements_failed");
        let session = agent
            .try_start_session_async(session_options())
            .await
            .unwrap();
        server.await.unwrap();
        let bundle = session.review_requirements_test_snapshot();
        assert!(bundle.review_cloud.is_some());
        assert_eq!(
            bundle.review_requirements.allowed_reviewers,
            Some(vec![ManagedReviewer::AutoReview])
        );
        assert!(matches!(
            bundle.review_requirements.sources.allowed_reviewers,
            Some(ManagedRequirementOrigin::TrustedBundle { .. })
        ));
        assert_eq!(
            session.runtime_policy_state().approvals_reviewer,
            Some(ApprovalsReviewer::Automatic)
        );
        assert_eq!(
            session.runtime_policy_state().sandbox_mode,
            Some(SandboxMode::DangerFullAccess)
        );
        let revision = session.model_revision();
        assert!(session.set_model("blocked-model").is_err());
        assert!(session.set_active_turn_model("blocked-model").is_err());
        assert_eq!(session.model_revision(), revision);
        assert!(session.pending_model().is_none());
        let refused = session
            .run(
                Input::Text("Inspect only.".into()),
                super::super::session::TurnOptions {
                    model: Some("blocked-model".into()),
                    ..Default::default()
                },
            )
            .await;
        assert!(refused.error.is_some());
        assert_eq!(model_calls.load(Ordering::SeqCst), 0);
        let ordinary = agent
            .try_start_session_async(StartSessionOptions {
                user_config_dir: Some(runtime_path_from_host_path(&directory)),
                config: Some(json!({"sandbox_mode":"workspace-write"})),
                ..Default::default()
            })
            .await
            .unwrap();
        let workspace_policy = ordinary.runtime_policy_state();
        assert_eq!(
            workspace_policy.sandbox_mode,
            Some(SandboxMode::WorkspaceWrite)
        );
        assert_eq!(
            workspace_policy.approvals_reviewer,
            Some(ApprovalsReviewer::Automatic)
        );
        let completed = ordinary
            .run(
                Input::Text("Inspect only.".into()),
                super::super::session::TurnOptions {
                    model: Some("blocked-model".into()),
                    ..Default::default()
                },
            )
            .await;
        assert!(completed.error.is_none(), "{:?}", completed.error);
        assert_eq!(completed.final_text.try_to_utf8().unwrap(), "ok");
        assert_eq!(ordinary.runtime_policy_state(), workspace_policy);
        assert_eq!(model_calls.load(Ordering::SeqCst), 1);
        ordinary.set_model("blocked-model").unwrap();
        assert!(
            ordinary
                .set_permission_mode(ExternalPermissionMode::BypassPermissions)
                .is_err(),
            "a deprecated preset must pass the same managed requirements"
        );
        let before = ordinary.runtime_policy_state();
        assert_eq!(before.sandbox_mode, Some(SandboxMode::WorkspaceWrite));
        assert_eq!(
            before.approvals_reviewer,
            Some(ApprovalsReviewer::Automatic)
        );
        let revision = ordinary.model_revision();
        let error = ordinary
            .set_runtime_policy(super::super::session::RuntimePolicyUpdate {
                approval_policy: None,
                sandbox_mode: Some(SandboxMode::DangerFullAccess),
                approvals_reviewer: None,

                network_allowed: Some(true),

                sandbox_workspace_write: None,
            })
            .unwrap_err();
        assert!(error.contains("requires automatic approval review"));
        assert_eq!(ordinary.runtime_policy_state(), before);
        assert_eq!(ordinary.model_revision(), revision);
        ordinary.shutdown_and_wait().await.unwrap();
        let completed = session
            .run(
                Input::Text("Report the current status.".into()),
                Default::default(),
            )
            .await;
        assert!(completed.error.is_none(), "{:?}", completed.error);
        assert_eq!(completed.final_text.try_to_utf8().unwrap(), "ok");
        assert_eq!(model_calls.load(Ordering::SeqCst), 2);
        let fallback = agent
            .try_start_session_async(StartSessionOptions {
                user_config_dir: Some(runtime_path_from_host_path(&directory)),
                config: Some(json!({"model":"fallback-source","fallbackModels":["blocked-model"]})),
                ..Default::default()
            })
            .await
            .unwrap();
        let refused = fallback
            .run(
                Input::Text("Report the current status.".into()),
                Default::default(),
            )
            .await;
        assert!(refused.error.as_ref().is_some_and(|error| error
            .message
            .try_to_utf8()
            .unwrap()
            .contains("requires automatic approval review")));
        assert_eq!(model_calls.load(Ordering::SeqCst), 3, "only the original model may enter its provider; the protected fallback must be refused during final preparation");
        fallback.shutdown_and_wait().await.unwrap();
        let inference_revision = session.model_revision();
        let registration_revision = session.mcp_registration_revision();
        session.reload_mcp_servers(Vec::new()).await.unwrap();
        assert!(session.mcp_registration_revision() > registration_revision);
        assert_eq!(session.model_revision(), inference_revision);
        let per_attempt = bundle.load_review_requirements().await.unwrap();
        assert_eq!(
            per_attempt.allowed_reviewers,
            bundle.review_requirements.allowed_reviewers
        );
        assert_eq!(
            bundle
                .load_review_requirements_sync()
                .unwrap()
                .allowed_reviewers,
            per_attempt.allowed_reviewers
        );
        session.shutdown_and_wait().await.unwrap();
        drop(bundle);
        drop(session);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn stale_inference_publication_does_not_publish_prepared_requirements() {
        use crate::engine::approval_requirements::ManagedReviewRequirements;
        use crate::types::permission::ApprovalsReviewer;
        let directory = temp_dir();
        let agent = Agent::new(test_agent_options(
            Some(json!({"agentMemory":false,"memoryRecall":false,"fork":false})),
            Vec::new(),
        ));
        let session = agent.start_session(StartSessionOptions {
            user_config_dir: Some(runtime_path_from_host_path(&directory)),
            ..Default::default()
        });
        let initial = session.review_requirements_test_snapshot();
        let provider: Arc<dyn ModelProvider> = Arc::new(fake_provider());
        let requirements = Arc::new(ManagedReviewRequirements {
            required_models: vec!["protected".into()],
            ..Default::default()
        });
        let prepared = super::super::session::PreparedReviewRequirements {
            provider: provider.clone(),
            cloud: None,
            requirements: requirements.clone(),
        };
        let mut other = initial.config.clone();
        other.model = "other".into();
        session
            .reconfigure_inference(initial.provider.clone(), other, None, None, 0)
            .unwrap();
        let before = session.review_requirements_test_snapshot();
        let before_policy = session.runtime_policy_state();
        let mut selected = before.config.clone();
        selected.model = "protected".into();
        selected.approvals_reviewer = None;
        let publication = |expected_revision| super::super::session::InferencePublication {
            expected_revision,
            change_id: None,
            targets: None,
            advisor_target: None,
            feature_targets: Default::default(),
            source: None,
            rates: None,
            publication_key: None,
            review_requirements: Some(prepared.clone()),
        };
        assert!(session
            .reconfigure_inference_with_targets(
                provider.clone(),
                selected.clone(),
                None,
                None,
                publication(0)
            )
            .is_err());
        assert!(Arc::ptr_eq(
            &session.review_requirements_test_snapshot(),
            &before
        ));
        assert_eq!(session.runtime_policy_state(), before_policy);
        assert!(!Arc::ptr_eq(
            &session
                .review_requirements_test_snapshot()
                .review_requirements,
            &requirements
        ));
        session
            .reconfigure_inference_with_targets(provider, selected, None, None, publication(1))
            .unwrap();
        assert!(Arc::ptr_eq(
            &session
                .review_requirements_test_snapshot()
                .review_requirements,
            &requirements
        ));
        assert_eq!(
            session.runtime_policy_state().approvals_reviewer,
            Some(ApprovalsReviewer::Automatic)
        );
        session.shutdown_and_wait().await.unwrap();
        std::fs::remove_dir_all(directory).unwrap();
    }

    /// Compaction keeps the main model's context capacity even when a smaller
    /// medium-tier model is configured for background work.
    #[tokio::test]
    async fn build_deps_keeps_compaction_on_the_main_model() {
        struct RecordingProvider {
            info: crate::types::provider::ProviderInfo,
            requests: Arc<Mutex<Vec<String>>>,
        }
        #[async_trait]
        impl ModelProvider for RecordingProvider {
            fn info(&self) -> &crate::types::provider::ProviderInfo {
                &self.info
            }
            fn capabilities(
                &self,
                model: &RuntimeString,
            ) -> crate::types::provider::ProviderCapabilities {
                fake_provider().capabilities(model)
            }
            fn stream(&self, req: ModelRequest, signal: AbortSignal) -> ModelStream {
                self.requests
                    .lock()
                    .unwrap()
                    .push(req.model.try_to_utf8().unwrap());
                let turn = vec![
                    ev::message_start(None),
                    ev::text("summary"),
                    ev::stop(None, None),
                ];
                Box::pin(
                    futures::stream::iter(turn)
                        .take_while(move |_| futures::future::ready(!signal.is_cancelled())),
                )
            }
        }

        let requests: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let agent = test_agent();
        let provider: Arc<dyn ModelProvider> = Arc::new(RecordingProvider {
            info: fake_provider().info().clone(),
            requests: requests.clone(),
        });
        let mut config = crate::schemas::config::default_config();
        config.model = "main-model".into();
        config.model_tiers.medium = Some("mid-model".into());
        let deps = agent.build_deps(provider, &config);

        let messages = vec![
            Message::User(UserMessage {
                question_answer: None,
                uuid: "u1".into(),
                content: vec![ContentBlock::Text(TextBlock {
                    text: "start".into(),
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                })],
                is_meta: None,
            }),
            Message::Assistant(AssistantMessage {
                uuid: "a1".into(),
                content: vec![ContentBlock::Text(TextBlock {
                    text: "ack".into(),
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                })],
                model: "main-model".into(),
                usage: None,
            }),
            Message::User(UserMessage {
                question_answer: None,
                uuid: "u2".into(),
                content: vec![ContentBlock::Text(TextBlock {
                    text: "continue".into(),
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                })],
                is_meta: None,
            }),
            Message::Assistant(AssistantMessage {
                uuid: "a2".into(),
                content: vec![ContentBlock::Text(TextBlock {
                    text: "next step".into(),
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                })],
                model: "main-model".into(),
                usage: None,
            }),
        ];
        let result = deps
            .compactor
            .reactive_compact(&messages, &config, None)
            .await;

        // The toy history inflates under summarization, so the result itself is
        // rejected by the inflation guard — but the model call already happened,
        // and it ran on the main model.
        let _ = result;
        assert_eq!(requests.lock().unwrap().as_slice(), ["main-model"]);
    }

    fn browser_tool_call_model(requests: Arc<Mutex<Vec<ModelRequest>>>) -> CallModel {
        let call_count = Arc::new(AtomicUsize::new(0));
        Box::new(
            move |req: ModelRequest, signal: AbortSignal| -> ModelStream {
                requests.lock().unwrap().push(req);
                let idx = call_count.fetch_add(1, Ordering::SeqCst);
                let turn = match idx {
                    0 => {
                        let mut events = vec![ev::message_start(None)];
                        events.extend(exact_tool_use_events(
                            "search-tu-1",
                            "tool_search",
                            &exact_json!({ "query": "select:mcp__browser__navigate" }),
                        ));
                        events.push(ev::stop(
                            Some(crate::types::provider::ModelStopReason::ToolUse),
                            None,
                        ));
                        events
                    }
                    1 => {
                        let mut events = vec![ev::message_start(None)];
                        events.extend(exact_tool_use_events(
                            "mcp-tu-1",
                            "mcp__browser__navigate",
                            &exact_json!({ "url": "https://example.com/" }),
                        ));
                        events.push(ev::stop(
                            Some(crate::types::provider::ModelStopReason::ToolUse),
                            None,
                        ));
                        events
                    }
                    _ => vec![
                        ev::message_start(None),
                        ev::text("done"),
                        ev::stop(None, None),
                    ],
                };
                Box::pin(
                    futures::stream::iter(turn)
                        .take_while(move |_| futures::future::ready(!signal.is_cancelled())),
                )
            },
        )
    }

    fn deferred_mcp_call_model(requests: Arc<Mutex<Vec<ModelRequest>>>) -> CallModel {
        let call_count = Arc::new(AtomicUsize::new(0));
        Box::new(
            move |req: ModelRequest, signal: AbortSignal| -> ModelStream {
                requests.lock().unwrap().push(req);
                let idx = call_count.fetch_add(1, Ordering::SeqCst);
                let turn = match idx {
                    0 => {
                        let mut events = vec![ev::message_start(None)];
                        events.extend(exact_tool_use_events(
                            "search-tu-1",
                            "tool_search",
                            &exact_json!({ "query": "session documentation lookup" }),
                        ));
                        events.push(ev::stop(
                            Some(crate::types::provider::ModelStopReason::ToolUse),
                            None,
                        ));
                        events
                    }
                    1 => {
                        let mut events = vec![ev::message_start(None)];
                        events.extend(exact_tool_use_events(
                            "mcp-tu-1",
                            "mcp__session_docs__lookup",
                            &exact_json!({}),
                        ));
                        events.push(ev::stop(
                            Some(crate::types::provider::ModelStopReason::ToolUse),
                            None,
                        ));
                        events
                    }
                    _ => vec![
                        ev::message_start(None),
                        ev::text("done"),
                        ev::stop(None, None),
                    ],
                };
                Box::pin(
                    futures::stream::iter(turn)
                        .take_while(move |_| futures::future::ready(!signal.is_cancelled())),
                )
            },
        )
    }

    struct NoopBrowserRuntime;

    #[async_trait]
    unsafe impl BrowserExecutor for NoopBrowserRuntime {
        async fn execute(
            &self,
            _request: crate::browser::BrowserRuntimeRequest,
            _signal: AbortSignal,
            _emit: crate::browser::BrowserEventSink,
        ) -> Result<crate::browser::BrowserRuntimeResponse, crate::browser::BrowserRuntimeError>
        {
            Ok(crate::browser::BrowserRuntimeResponse::default())
        }
    }

    fn spawn_free<T>(executor: Arc<T>) -> Arc<dyn BrowserRuntime>
    where
        T: BrowserExecutor + 'static,
    {
        // SAFETY: this no-op test executor only returns an in-memory fixture
        // response and has no local process authority.
        unsafe { attest_spawn_free(executor) }
    }

    fn request_tool_names(request: &ModelRequest) -> Vec<RuntimeString> {
        request.tools.iter().map(|tool| tool.name.clone()).collect()
    }

    #[tokio::test]
    async fn host_owned_task_registry_clears_claimed_builtin_identity_at_start_and_refresh() {
        for endpoint_tools in [false, true] {
            let requests = Arc::new(Mutex::new(Vec::new()));
            let mut options = test_agent_options(
                Some(json!({"agentMemory":false,"memoryRecall":false,"fork":false})),
                vec![],
            );
            options.deps.call_model = Some(recording_call_model(requests));
            options.tools = endpoint_tools.then(Vec::new);
            options.agents = Some(vec![AgentDefinition {
                name: "Explore".into(),
                builtin_identity: Some("klaus:builtin:explore:1".into()),
                ..Default::default()
            }]);
            let agent = Agent::new(options);
            let session = agent.start_session(StartSessionOptions {
                tools: (!endpoint_tools).then(Vec::new),
                agents: Some(vec![AgentDefinition {
                    name: "custom".into(),
                    builtin_identity: Some("klaus:builtin:plan:1".into()),
                    ..Default::default()
                }]),
                ..Default::default()
            });
            for refreshed in [false, true] {
                if refreshed {
                    agent.refresh_plugins(PluginLoadReport::default()).unwrap();
                    session
                        .run(Input::Text("refresh".into()), TurnOptions::default())
                        .await;
                }
                let registry = session.current_agent_registry();
                assert!(registry
                    .get(&"Explore".into())
                    .unwrap()
                    .builtin_identity
                    .is_none());
                assert!(registry
                    .get(&"custom".into())
                    .unwrap()
                    .builtin_identity
                    .is_none());
                assert!(!registry.has(&"Plan".into()));
            }
            session.shutdown_and_wait().await.unwrap();
        }
    }

    #[tokio::test]
    async fn engine_owned_builtin_identity_survives_registry_start_and_refresh() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let mut options = test_agent_options(
            Some(json!({"agentMemory":false,"memoryRecall":false,"fork":false})),
            vec![],
        );
        options.deps.call_model = Some(recording_call_model(requests));
        let agent = Agent::new(options);
        let session = agent.start_session(StartSessionOptions::default());
        for refreshed in [false, true] {
            if refreshed {
                agent.refresh_plugins(PluginLoadReport::default()).unwrap();
                session
                    .run(Input::Text("refresh".into()), TurnOptions::default())
                    .await;
            }
            let registry = session.current_agent_registry();
            for (name, identity) in [
                ("Explore", "klaus:builtin:explore:1"),
                ("Plan", "klaus:builtin:plan:1"),
            ] {
                assert_eq!(
                    registry.get(&name.into()).unwrap().builtin_identity,
                    Some(identity.into()),
                );
            }
        }
        session.shutdown_and_wait().await.unwrap();
    }

    #[tokio::test]
    async fn root_agent_definition_selects_execution_not_just_registration() {
        use crate::schemas::effort::{EffortLevel, EffortValue};
        use crate::types::permission::{ApprovalPolicy, SandboxMode};

        let requests = Arc::new(Mutex::new(Vec::new()));
        let mut options = test_agent_options(
            Some(json!({"model":"parent-model", "modelTiers":{"high":"specialist-model"}})),
            vec![],
        );
        options.deps.call_model = Some(recording_call_model(requests.clone()));
        options.agents = Some(vec![AgentDefinition {
            name: "specialist".into(),
            prompt: Some("endpoint role must lose".into()),
            ..Default::default()
        }]);
        let agent = Agent::new(options);
        let definition = AgentDefinition {
            name: "specialist".into(),
            prompt: Some("session specialist instructions".into()),
            system_prompt: Some("deprecated alias must lose".into()),
            critical_system_reminder: Some("critical specialist boundary".into()),
            model: Some("high".into()),
            effort: Some(EffortValue::Level(EffortLevel::High)),
            runtime_policy: Some(super::super::session::RuntimePolicyUpdate {
                approval_policy: Some(ApprovalPolicy::Never),
                sandbox_mode: Some(SandboxMode::ReadOnly),
                approvals_reviewer: None,
                sandbox_workspace_write: None,
                network_allowed: None,
            }),
            tools: Some(vec!["read".into(), "write".into()]),
            disallowed_tools: Some(vec!["write".into()]),
            ..Default::default()
        };
        let session = agent
            .try_start_session(StartSessionOptions {
                agent_definition_name: Some("specialist".into()),
                agents: Some(vec![definition.clone()]),
                base_instructions: Some("host base instructions".into()),
                developer_instructions: Some("host developer instructions".into()),
                user_config_dir: Some(temp_dir().to_str().unwrap().into()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(
            session.runtime_policy_state().approval_policy,
            Some(ApprovalPolicy::Never)
        );
        assert_eq!(
            session.runtime_policy_state().sandbox_mode,
            Some(SandboxMode::ReadOnly)
        );
        let result = session
            .run(Input::Text("inspect".into()), TurnOptions::default())
            .await;
        assert_eq!(
            result.stop_reason,
            crate::types::errors::TerminalReason::Completed
        );
        {
            let requests = requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].model, "specialist-model");
            assert_eq!(requests[0].effort, definition.effort);
            assert_eq!(request_tool_names(&requests[0]), vec!["read"]);
            for text in [
                "host base instructions",
                "host developer instructions",
                "session specialist instructions",
                "critical specialist boundary",
            ] {
                assert!(
                    requests[0].system.iter().any(|part| part.text == text),
                    "{text}"
                );
            }
            assert!(!requests[0].system.iter().any(|part| {
                part.text == "deprecated alias must lose" || part.text == "endpoint role must lose"
            }));
        }
        let ordinary = agent
            .try_start_session(StartSessionOptions {
                agents: Some(vec![definition]),
                tools: Some(vec![]),
                ..Default::default()
            })
            .unwrap();
        ordinary
            .run(Input::Text("ordinary".into()), TurnOptions::default())
            .await;
        let requests = requests.lock().unwrap();
        assert_eq!(requests[1].model, "parent-model");
        assert!(!requests[1]
            .system
            .iter()
            .any(|part| part.text == "session specialist instructions"));
    }

    #[tokio::test]
    async fn root_agent_definition_unknown_fails_even_with_empty_registry() {
        let mut options = test_agent_options(None, vec![]);
        options.include_default_agents = Some(false);
        let agent = Agent::new(options);
        for name in ["missing", "", " missing "] {
            let result = agent.try_start_session(StartSessionOptions {
                agent_definition_name: Some(name.into()),
                ..Default::default()
            });
            assert!(matches!(
                result,
                Err(SessionInitError::AgentDefinition { message })
                    if message.contains("unknown agent definition")
            ));
        }
    }

    #[tokio::test]
    async fn root_agent_definition_validates_before_persistence_and_records_selected_model() {
        let dir = temp_dir();
        let store = Arc::new(store_at(&dir));
        let agent = test_agent();
        let rejected = agent
            .open_session(
                store.clone(),
                StartSessionOptions {
                    id: Some("root-specialist".into()),
                    agent_definition_name: Some("missing".into()),
                    ..Default::default()
                },
            )
            .await;
        assert!(matches!(rejected, Err(error) if error.kind() == std::io::ErrorKind::InvalidInput));
        assert!(store.resume("root-specialist").await.is_err());
        let session = agent
            .open_session(
                store.clone(),
                StartSessionOptions {
                    id: Some("root-specialist".into()),
                    agent_definition_name: Some("specialist".into()),
                    agents: Some(vec![AgentDefinition {
                        name: "specialist".into(),
                        model: Some("specialist-model".into()),
                        tools: Some(vec![]),
                        ..Default::default()
                    }]),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        session.dispose().await;
        assert_eq!(
            store.resume("root-specialist").await.unwrap().meta.model,
            "specialist-model"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn root_agent_definition_alias_empty_tools_and_max_turns_are_enforced() {
        let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
        let captured = requests.clone();
        let call_model: CallModel = Box::new(move |request, _signal| {
            captured.lock().unwrap().push(request);
            let mut events = vec![ev::message_start(None)];
            events.extend(exact_tool_use_events(
                "forbidden",
                "write",
                &exact_json!({}),
            ));
            events.push(ev::stop(
                Some(crate::types::provider::ModelStopReason::ToolUse),
                None,
            ));
            Box::pin(futures::stream::iter(events))
        });
        let agent = test_agent_with_runtime(None, vec![], Some(call_model), None);
        let session = agent
            .try_start_session(StartSessionOptions {
                agent_definition_name: Some("limited".into()),
                agents: Some(vec![AgentDefinition {
                    name: "limited".into(),
                    system_prompt: Some("legacy specialist role".into()),
                    tools: Some(vec![]),
                    max_turns: Some(1),
                    ..Default::default()
                }]),
                ..Default::default()
            })
            .unwrap();
        let result = session
            .run(Input::Text("work".into()), TurnOptions::default())
            .await;
        assert_eq!(
            result.stop_reason,
            crate::types::errors::TerminalReason::TurnLimitExhausted
        );
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].tools.is_empty());
        assert!(requests[0]
            .system
            .iter()
            .any(|part| part.text == "legacy specialist role"));
    }

    /// A host-declared extra tool wearing a builtin's name (the protocol's
    /// dynamic tools arrive through the same `mcp_tools` slot).
    struct HostNamedTool {
        name: String,
        schema: crate::types::json::JsonSchema,
    }

    impl HostNamedTool {
        fn new(name: &str) -> Self {
            let mut schema = crate::types::json::JsonSchema::new();
            schema.insert(
                "type".to_owned(),
                serde_json::Value::String("object".to_owned()),
            );
            Self {
                name: name.to_owned(),
                schema,
            }
        }
    }

    #[async_trait]
    impl Tool for HostNamedTool {
        fn static_name(&self) -> &str {
            &self.name
        }

        fn static_description(&self) -> &str {
            "host-owned implementation"
        }

        fn task_scope_host_capability(&self) -> bool {
            ["list_friends", "send_message_to_friend", "web_search"].contains(&self.name.as_str())
        }

        fn static_mcp_server(&self) -> Option<&str> {
            self.name
                .starts_with("mcp__computer_use__")
                .then_some("computer_use")
        }

        fn static_input_schema(&self) -> &crate::types::json::JsonSchema {
            &self.schema
        }

        fn is_enabled(&self) -> bool {
            true
        }

        fn is_concurrency_safe(&self, _input: &Json) -> bool {
            false
        }

        fn is_read_only(&self, _input: &Json) -> bool {
            true
        }

        async fn call(
            &self,
            _input: Json,
            _ctx: &crate::types::tool::ToolContext,
            _on_progress: Option<crate::types::tool::ToolProgress>,
        ) -> Result<crate::types::tool::ToolResult, crate::types::errors::ToolError> {
            Err(crate::types::errors::ToolError::new(
                "not exercised by this test",
            ))
        }
    }

    /// A host extra tool that answers to a builtin's name must REPLACE the
    /// builtin, not stand beside it: the model may never see two tools with one
    /// name, and the name's owner is the host that declared it.
    #[tokio::test]
    async fn host_extra_tool_replaces_same_named_builtin() {
        let requests: Arc<Mutex<Vec<ModelRequest>>> = Arc::new(Mutex::new(Vec::new()));
        let agent = test_agent_with_runtime(
            None,
            Vec::new(),
            Some(recording_call_model(requests.clone())),
            None,
        );
        let session = agent.start_session(StartSessionOptions {
            mcp_tools: Some(vec![Arc::new(HostNamedTool::new("web_search"))]),
            ..Default::default()
        });
        let result = session
            .run(Input::Text("hello".into()), TurnOptions::default())
            .await;
        assert_eq!(
            result.stop_reason,
            crate::types::errors::TerminalReason::Completed
        );

        let requests = requests.lock().unwrap();
        let names = request_tool_names(&requests[0]);
        assert_eq!(
            names.iter().filter(|name| **name == "web_search").count(),
            1,
            "exactly one web_search must reach the model, got {names:?}"
        );
        let spec = requests[0]
            .tools
            .iter()
            .find(|tool| tool.name == "web_search")
            .expect("web_search present");
        // The survivor is the HOST tool — the builtin (whose description is the
        // engine's own search copy) was dropped, not the declared one.
        assert_eq!(spec.description, "host-owned implementation");
    }

    struct ToolListMcpServer;

    #[async_trait]
    impl McpInProcessServer for ToolListMcpServer {
        async fn connect(&self, transport: Box<dyn Transport>) {
            let transport: Arc<dyn Transport> = Arc::from(transport);
            let (tx, mut rx) = mpsc::unbounded_channel();
            let _ = transport.start(tx).await;
            tokio::spawn(async move {
                while let Some(event) = rx.recv().await {
                    let TransportEvent::Message(message) = event else {
                        continue;
                    };
                    let Some(method) = message.get("method").and_then(|v| v.as_str()) else {
                        continue;
                    };
                    let Some(id) = message.get("id").cloned() else {
                        continue;
                    };
                    let result = if method == "initialize" {
                        Some(exact_json!({
                            "protocolVersion": "2025-06-18",
                            "capabilities": { "tools": {} },
                            "serverInfo": { "name": "plugin-mcp", "version": "1.0.0" },
                            "instructions": "Use plugin MCP tools for plugin docs."
                        }))
                    } else if method == "tools/list" {
                        Some(exact_json!({
                            "tools": [{
                                "name": "lookup",
                                "description": "Lookup plugin docs",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {},
                                    "additionalProperties": false
                                }
                            }]
                        }))
                    } else {
                        None
                    };
                    match result {
                        Some(result) => {
                            let _ = transport.send(rpc_success(id, result)).await;
                        }
                        None => {
                            let _ = transport.send(rpc_method_not_found(id)).await;
                        }
                    }
                }
            });
        }
    }

    struct CountingMcpServer {
        connects: Arc<AtomicUsize>,
        lists: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl McpInProcessServer for CountingMcpServer {
        async fn connect(&self, transport: Box<dyn Transport>) {
            self.connects.fetch_add(1, Ordering::SeqCst);
            let transport: Arc<dyn Transport> = Arc::from(transport);
            let (tx, mut rx) = mpsc::unbounded_channel();
            let _ = transport.start(tx).await;
            let lists = self.lists.clone();
            tokio::spawn(async move {
                while let Some(event) = rx.recv().await {
                    let TransportEvent::Message(message) = event else {
                        continue;
                    };
                    let Some(method) = message.get("method").and_then(|value| value.as_str())
                    else {
                        continue;
                    };
                    let Some(id) = message.get("id").cloned() else {
                        continue;
                    };
                    let result = if method == "initialize" {
                        Some(exact_json!({
                            "protocolVersion": "2025-06-18",
                            "capabilities": { "tools": {} },
                            "serverInfo": { "name": "counting", "version": "1.0.0" }
                        }))
                    } else if method == "tools/list" {
                        lists.fetch_add(1, Ordering::SeqCst);
                        Some(exact_json!({ "tools": [] }))
                    } else {
                        None
                    };
                    let _ = transport
                        .send(match result {
                            Some(result) => rpc_success(id, result),
                            None => rpc_method_not_found(id),
                        })
                        .await;
                }
            });
        }
    }

    fn hot_plugin(server: Arc<dyn McpInProcessServer>) -> LoadedPlugin {
        let mut plugin = loaded_plugin();
        plugin.output_styles.clear();
        plugin.lsp_servers.clear();
        plugin.hooks.push(Hook {
            event: crate::hooks::types::HookEvent::PostToolUse,
            matcher: None,
            handler: HookHandler::Command {
                command: "true".to_string(),
                timeout_ms: None,
            },
            is_async: false,
        });
        plugin.mcp_servers = vec![McpServerConfig::InProcess {
            name: "plugin_mcp".to_string(),
            server,
            options: McpServerOptions {
                always_load: Some(true),
                ..Default::default()
            },
        }];
        plugin
    }

    #[tokio::test]
    async fn loaded_session_adopts_plugin_generation_at_turn_boundary_without_relisting_unchanged_mcp(
    ) {
        let connects = Arc::new(AtomicUsize::new(0));
        let lists = Arc::new(AtomicUsize::new(0));
        let server: Arc<dyn McpInProcessServer> = Arc::new(CountingMcpServer {
            connects: connects.clone(),
            lists: lists.clone(),
        });
        let agent = test_agent_with_config_and_plugins(None, vec![hot_plugin(server.clone())]);
        let session = agent.start_session(StartSessionOptions::default());
        assert_eq!(session.mcp_runtime_snapshot().await.servers.len(), 1);
        assert_eq!(
            (
                connects.load(Ordering::SeqCst),
                lists.load(Ordering::SeqCst)
            ),
            (1, 1)
        );
        let held = session.run_streamed(Input::Text("held".into()), TurnOptions::default());
        agent.refresh_plugins(PluginLoadReport::default()).unwrap();
        assert!(session
            .visible_skill_names()
            .contains(&RuntimeString::from("plugin-skill")));
        assert_eq!(session.extension_runtime_snapshot().hooks.len(), 1);
        let overlapping =
            session.run_streamed(Input::Text("overlap".into()), TurnOptions::default());
        let mut events = overlapping.events;
        assert!(matches!(
            events.next().await,
            Some(crate::types::events::EngineEvent::TurnFailed { error, .. })
                if error.message.try_to_utf8().unwrap_or_default().contains("plugin update pending")
        ));
        drop(events);
        drop(held);

        let disabled = session.run_streamed(Input::Text("disabled".into()), TurnOptions::default());
        assert!(!session
            .visible_skill_names()
            .contains(&RuntimeString::from("plugin-skill")));
        assert!(!session
            .agent_definition_names()
            .contains(&RuntimeString::from("plugin-reader")));
        assert!(session.extension_runtime_snapshot().hooks.is_empty());
        assert!(session.plugin_projection().unwrap().commands.is_empty());
        session.reconcile_plugin_mcp_for_turn().await.unwrap();
        assert!(session.mcp_runtime_snapshot().await.servers.is_empty());
        drop(disabled);

        agent
            .refresh_plugins(PluginLoadReport {
                enabled: vec![hot_plugin(server.clone())],
                ..Default::default()
            })
            .unwrap();
        let enabled = session.run_streamed(Input::Text("enabled".into()), TurnOptions::default());
        assert!(session
            .visible_skill_names()
            .contains(&RuntimeString::from("plugin-skill")));
        assert!(session
            .agent_definition_names()
            .contains(&RuntimeString::from("plugin-reader")));
        assert_eq!(session.extension_runtime_snapshot().hooks.len(), 1);
        assert_eq!(
            session.plugin_projection().unwrap().commands[0].name(),
            "plugin-cmd"
        );
        session.reconcile_plugin_mcp_for_turn().await.unwrap();
        assert_eq!(session.mcp_runtime_snapshot().await.servers.len(), 1);
        assert_eq!(
            (
                connects.load(Ordering::SeqCst),
                lists.load(Ordering::SeqCst)
            ),
            (2, 2)
        );
        drop(enabled);

        agent
            .refresh_plugins(PluginLoadReport {
                enabled: vec![hot_plugin(server)],
                ..Default::default()
            })
            .unwrap();
        let unchanged =
            session.run_streamed(Input::Text("unchanged".into()), TurnOptions::default());
        session.reconcile_plugin_mcp_for_turn().await.unwrap();
        assert_eq!(
            (
                connects.load(Ordering::SeqCst),
                lists.load(Ordering::SeqCst)
            ),
            (2, 2)
        );
        drop(unchanged);
        agent.refresh_plugins(PluginLoadReport::default()).unwrap();
        assert!(session.current_hook_bus().is_none());
        session.dispose().await;
        assert!(session.current_hook_bus().is_none());
    }

    #[tokio::test]
    async fn plugin_cleanup_waits_for_background_context_after_root_turn_exits() {
        let agent = test_agent_with_config_and_plugins(None, vec![loaded_plugin()]);
        let session = agent.start_session(StartSessionOptions::default());
        session
            .record_subagent_started(
                "background-child",
                crate::engine::subagent_lifecycle::SubagentKind::Background,
                crate::engine::subagent_lifecycle::SubagentContext {
                    session_id: "test-session".into(),
                    parent_agent_id: None,
                    parent_thread_id: None,
                    agent_type: None,
                    cwd: ".".into(),
                    description: "background fixture".into(),
                    depth: 1,
                    resume_from: None,
                    spawn_turn_id: None,
                },
            )
            .await
            .unwrap();
        assert!(agent.has_active_plugin_contexts());
        let wait = agent.wait_for_loaded_plugin_contexts();
        tokio::pin!(wait);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(30), &mut wait)
                .await
                .is_err()
        );
        session
            .mark_subagent_completed("background-child", None)
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), wait)
            .await
            .expect("cleanup waits until the background context releases its snapshot");
        assert!(!agent.has_active_plugin_contexts());
        session.dispose().await;
    }

    #[tokio::test]
    async fn loaded_session_replaces_style_and_lsp_at_next_turn() {
        let agent = test_agent_with_config_and_plugins(
            Some(json!({"outputStyle":"focused", "lsp":true})),
            vec![loaded_plugin()],
        );
        let existing = agent.start_session(StartSessionOptions::default());
        assert_eq!(existing.active_output_style_name(), Some("focused".into()));
        assert!(existing.lsp_available());
        assert_eq!(
            existing
                .extension_runtime_snapshot()
                .plugin_runtime
                .entries
                .len(),
            1
        );
        let held = existing.run_streamed(Input::Text("held".into()), TurnOptions::default());
        agent.refresh_plugins(PluginLoadReport::default()).unwrap();
        assert!(agent.plugin_runtime_snapshot().entries.is_empty());
        assert_eq!(existing.active_output_style_name(), Some("focused".into()));
        assert!(existing.lsp_available());
        assert_eq!(
            existing
                .extension_runtime_snapshot()
                .plugin_runtime
                .entries
                .len(),
            1
        );
        let new_session = agent.start_session(StartSessionOptions::default());
        assert!(new_session
            .extension_runtime_snapshot()
            .plugin_runtime
            .entries
            .is_empty());
        drop(held);
        let next = existing.run_streamed(Input::Text("next".into()), TurnOptions::default());
        assert_eq!(existing.active_output_style_name(), None);
        assert!(!existing.lsp_available());
        assert!(existing
            .extension_runtime_snapshot()
            .output_styles
            .is_empty());
        assert!(existing
            .extension_runtime_snapshot()
            .plugin_runtime
            .entries
            .is_empty());
        drop(next);
        agent.refresh_plugins(PluginLoadReport::default()).unwrap();
        existing.dispose().await;
        new_session.dispose().await;
    }

    struct DeferredToolMcpServer {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl McpInProcessServer for DeferredToolMcpServer {
        async fn connect(&self, transport: Box<dyn Transport>) {
            let transport: Arc<dyn Transport> = Arc::from(transport);
            let calls = self.calls.clone();
            let (tx, mut rx) = mpsc::unbounded_channel();
            let _ = transport.start(tx).await;
            tokio::spawn(async move {
                while let Some(event) = rx.recv().await {
                    let TransportEvent::Message(message) = event else {
                        continue;
                    };
                    let Some(method) = message.get("method").and_then(|value| value.as_str())
                    else {
                        continue;
                    };
                    let Some(id) = message.get("id").cloned() else {
                        continue;
                    };
                    let result = if method == "initialize" {
                        Some(exact_json!({
                            "protocolVersion": "2025-06-18",
                            "capabilities": { "tools": {} },
                            "serverInfo": { "name": "session-docs", "version": "1.0.0" }
                        }))
                    } else if method == "tools/list" {
                        Some(exact_json!({
                            "tools": [{
                                "name": "lookup",
                                "description": "Lookup session documentation",
                                "annotations": { "readOnlyHint": true },
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {},
                                    "additionalProperties": false
                                }
                            }]
                        }))
                    } else if method == "tools/call" {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Some(exact_json!({
                            "content": [{ "type": "text", "text": "session docs" }]
                        }))
                    } else {
                        None
                    };
                    match result {
                        Some(result) => {
                            let _ = transport.send(rpc_success(id, result)).await;
                        }
                        None => {
                            let _ = transport.send(rpc_method_not_found(id)).await;
                        }
                    }
                }
            });
        }
    }

    struct BrowserRuntimeMcpServer {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl McpInProcessServer for BrowserRuntimeMcpServer {
        async fn connect(&self, transport: Box<dyn Transport>) {
            let transport: Arc<dyn Transport> = Arc::from(transport);
            let calls = self.calls.clone();
            let (tx, mut rx) = mpsc::unbounded_channel();
            let _ = transport.start(tx).await;
            tokio::spawn(async move {
                while let Some(event) = rx.recv().await {
                    let TransportEvent::Message(message) = event else {
                        continue;
                    };
                    let Some(method) = message.get("method").and_then(|v| v.as_str()) else {
                        continue;
                    };
                    let Some(id) = message.get("id").cloned() else {
                        continue;
                    };
                    let result = if method == "initialize" {
                        Some(exact_json!({
                            "protocolVersion": "2025-06-18",
                            "capabilities": { "tools": {} },
                            "serverInfo": { "name": "browser-runtime", "version": "1.0.0" },
                            "instructions": "Provides browser runtime tools."
                        }))
                    } else if method == "tools/list" {
                        Some(exact_json!({
                            "tools": [{
                                "name": "navigate",
                                "description": "Navigate the plugin browser runtime",
                                "annotations": { "readOnlyHint": true },
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {},
                                    "additionalProperties": true
                                }
                            }]
                        }))
                    } else if method == "tools/call" {
                        calls.fetch_add(1, Ordering::SeqCst);
                        let name = message
                            .get("params")
                            .and_then(|p| p.get("name"))
                            .and_then(RuntimeJson::as_str);
                        let url = message
                            .get("params")
                            .and_then(|p| p.get("arguments"))
                            .and_then(|a| a.get("operation"))
                            .and_then(|op| op.get("url"))
                            .and_then(RuntimeJson::as_str)
                            .cloned()
                            .unwrap_or_default();
                        if name.is_some_and(|name| name == "navigate") {
                            let mut result = exact_json!({
                                "content": [{ "type": "text", "text": "navigated" }],
                                "structuredContent": {
                                    "ok": true,
                                    "response": { "url": "", "title": "Example" }
                                }
                            });
                            result["structuredContent"]["response"]["url"] =
                                RuntimeJson::String(url);
                            Some(result)
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    match result {
                        Some(result) => {
                            let _ = transport.send(rpc_success(id, result)).await;
                        }
                        None => {
                            let _ = transport.send(rpc_method_not_found(id)).await;
                        }
                    }
                }
            });
        }
    }

    fn loaded_plugin() -> LoadedPlugin {
        let mut lsp_servers = crate::types::json::JsonObject::new();
        lsp_servers.insert(
            "rust".to_string(),
            json!({
                "command": "rust-analyzer",
                "extensions": ["rs"]
            }),
        );

        LoadedPlugin {
            name: "docs".to_string(),
            version: Some("1.0.0".to_string()),
            display_name: None,
            description: None,
            plugin_namespace: "docs".to_string(),
            keywords: Vec::new(),
            dependencies: Vec::new(),
            user_config: crate::types::json::JsonObject::new(),
            settings: crate::types::json::JsonObject::new(),
            dir: "/plugins/docs".to_string(),
            skills: vec![Skill {
                name: "plugin-skill".into(),
                description: "plugin skill".into(),
                body: "Use the plugin skill.".into(),
                path: "/plugins/docs/skills/plugin-skill/SKILL.md".into(),
                ..Default::default()
            }],
            commands: vec![Command::Prompt(PromptCommand::new(
                "plugin-cmd",
                CommandMeta::default(),
                |args: RuntimeString| async move {
                    Ok(vec![ContentBlock::Text(TextBlock {
                        text: RuntimeString::from("plugin command:").concat(&args),
                        citations: None,
                        harness_envelope: None,
                        message_id: None,
                        phase: None,
                    })])
                },
            ))],
            agents: vec![crate::extensions::agent_loader::AgentDefinition {
                name: "plugin-reader".to_string(),
                description: "Read plugin docs".to_string(),
                tools: Some(vec!["read".to_string()]),
                disallowed_tools: None,
                model: None,
                runtime_policy: None,
                effort: None,
                ultra_mode: None,
                prompt: Some("Read the docs.".to_string()),
            }],
            hooks: Vec::new(),
            output_styles: vec![OutputStyle {
                name: "focused".into(),
                description: Some("focused answers".into()),
                body: "Answer tersely.".into(),
            }],
            mcp_servers: vec![McpServerConfig::InProcess {
                name: "plugin_mcp".to_string(),
                server: Arc::new(ToolListMcpServer),
                options: McpServerOptions {
                    always_load: Some(true),
                    ..Default::default()
                },
            }],
            lsp_servers,
            runtime_capabilities: Vec::new(),
            interface_capabilities: Vec::new(),
            app_connectors: Vec::new(),
        }
    }

    fn browser_plugin() -> LoadedPlugin {
        browser_plugin_with_server(Arc::new(ToolListMcpServer))
    }

    fn browser_plugin_with_server(server: Arc<dyn McpInProcessServer>) -> LoadedPlugin {
        LoadedPlugin {
            name: "browser-plugin".to_string(),
            version: Some("1.0.0".to_string()),
            display_name: None,
            description: None,
            plugin_namespace: "browser-plugin".to_string(),
            keywords: Vec::new(),
            dependencies: Vec::new(),
            user_config: crate::types::json::JsonObject::new(),
            settings: crate::types::json::JsonObject::new(),
            dir: "/plugins/browser-plugin".to_string(),
            skills: Vec::new(),
            commands: Vec::new(),
            agents: Vec::new(),
            hooks: Vec::new(),
            output_styles: Vec::new(),
            mcp_servers: vec![McpServerConfig::InProcess {
                name: "browser".to_string(),
                server,
                options: McpServerOptions {
                    always_load: Some(true),
                    ..Default::default()
                },
            }],
            lsp_servers: crate::types::json::JsonObject::new(),
            runtime_capabilities: vec!["browser".to_string()],
            interface_capabilities: Vec::new(),
            app_connectors: Vec::new(),
        }
    }

    fn stdio_mcp(name: &str, command: &str, args: &[&str]) -> McpServerConfig {
        McpServerConfig::Stdio {
            name: name.to_string(),
            command: command.to_string(),
            args: Some(args.iter().map(|arg| arg.to_string()).collect()),
            env: None,
            options: McpServerOptions {
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
            user_config: crate::types::json::JsonObject::new(),
            settings: crate::types::json::JsonObject::new(),
            dir: format!("/plugins/{name}"),
            skills: Vec::new(),
            commands: Vec::new(),
            agents: Vec::new(),
            hooks: Vec::new(),
            output_styles: Vec::new(),
            mcp_servers: vec![server],
            lsp_servers: crate::types::json::JsonObject::new(),
            runtime_capabilities: Vec::new(),
            interface_capabilities: Vec::new(),
            app_connectors: Vec::new(),
        }
    }

    fn event(name: &str, payload: Option<Json>) -> RolloutItem {
        RolloutItem::Event(EventItem {
            event: name.into(),
            turn_id: None,
            payload,
        })
    }

    #[test]
    fn snapshot_decision_replay_restores_latest_audit_per_request() {
        use crate::memory::agent_memory::AgentMemoryMergeChoice;
        use crate::memory::snapshot_decision::{
            AgentMemorySnapshotDecisionRequest, AgentMemorySnapshotDecisionSource,
            AgentMemorySnapshotDecisionStatus, AgentMemorySnapshotDiff,
        };

        let request = AgentMemorySnapshotDecisionRequest {
            request_id: "amsr_replay".into(),
            agent_type: "writer".into(),
            scope: AgentMemoryScope::User,
            snapshot_timestamp: "2026-07-18T00:00:00Z".into(),
            snapshot_id: "sha256:snapshot".into(),
            local_state_id: "sha256:local".into(),
            diff: AgentMemorySnapshotDiff {
                added_count: 0,
                modified_count: 1,
                removed_count: 0,
                entries: Vec::new(),
                truncated: false,
            },
            first_use: false,
            requested_at: 10,
            deadline_at: 20,
        };
        let requested = AgentMemorySnapshotDecisionAudit {
            request: request.clone(),
            status: AgentMemorySnapshotDecisionStatus::Requested,
            decision: None,
            source: None,
            decided_at: None,
            ended_at: None,
            error: None,
        };
        let applied = AgentMemorySnapshotDecisionAudit {
            request,
            status: AgentMemorySnapshotDecisionStatus::Applied,
            decision: Some(AgentMemoryMergeChoice::Keep),
            source: Some(AgentMemorySnapshotDecisionSource::Host),
            decided_at: Some(12),
            ended_at: Some(13),
            error: None,
        };
        let items = vec![
            event(
                AGENT_MEMORY_SNAPSHOT_DECISION_ROLLOUT_EVENT,
                Some(runtime_codec::encode_value(&requested).unwrap()),
            ),
            event(
                AGENT_MEMORY_SNAPSHOT_DECISION_ROLLOUT_EVENT,
                Some(exact_json!({"malformed": true})),
            ),
            event(
                AGENT_MEMORY_SNAPSHOT_DECISION_ROLLOUT_EVENT,
                Some(runtime_codec::encode_value(&applied).unwrap()),
            ),
        ];

        assert_eq!(
            agent_memory_snapshot_decisions_from_rollout(&items),
            vec![applied]
        );
    }

    #[test]
    fn memory_maintenance_replay_restores_latest_transition_for_reconnect() {
        let base = AutoMemoryRun {
            run_id: "memory-run-1".into(),
            session_id: "session-1".into(),
            trigger: MemoryMaintenanceTrigger::Auto,
            scope: AgentMemoryScope::Project,
            target_dir: "/tmp/memory".into(),
            status: MemoryMaintenanceStatus::Started,
            started_at: 10,
            ended_at: None,
            reason: None,
            error: None,
            message_count: 2,
            cursor_message_uuid: None,
            files_touched: Vec::new(),
            turn_count: None,
        };
        let mut failed = base.clone();
        failed.status = MemoryMaintenanceStatus::Failed;
        failed.ended_at = Some(20);
        failed.reason = Some(AutoMemoryRunReason::ToolFailure);
        failed.error = Some("write failed".into());
        let dream_started = AutoDreamRun {
            dream_run_id: "dream-run-1".into(),
            session_id: "session-1".into(),
            trigger: MemoryMaintenanceTrigger::Auto,
            status: MemoryMaintenanceStatus::Started,
            started_at: 11,
            ended_at: None,
            reason: None,
            error: None,
            last_consolidated_at: None,
            sessions_reviewing: 0,
            files_touched: Vec::new(),
            turns: Vec::new(),
            turn_count: 0,
            summary: None,
        };
        let mut dream_completed = dream_started.clone();
        dream_completed.status = MemoryMaintenanceStatus::Completed;
        dream_completed.ended_at = Some(21);
        dream_completed.last_consolidated_at = Some(20);
        dream_completed.sessions_reviewing = 4;
        let raw = vec![
            event(
                MEMORY_MAINTENANCE_ROLLOUT_EVENT,
                Some(
                    runtime_codec::encode_value(&MemoryMaintenanceEvent::AutoMemory {
                        run: base.clone(),
                    })
                    .unwrap(),
                ),
            ),
            event(
                MEMORY_MAINTENANCE_ROLLOUT_EVENT,
                Some(
                    runtime_codec::encode_value(&MemoryMaintenanceEvent::AutoMemory {
                        run: failed.clone(),
                    })
                    .unwrap(),
                ),
            ),
            // A stale duplicate written after the terminal transition must not
            // roll authoritative state or its cursor back to Started.
            event(
                MEMORY_MAINTENANCE_ROLLOUT_EVENT,
                Some(
                    runtime_codec::encode_value(&MemoryMaintenanceEvent::AutoMemory { run: base })
                        .unwrap(),
                ),
            ),
            event(
                MEMORY_MAINTENANCE_ROLLOUT_EVENT,
                Some(
                    runtime_codec::encode_value(&MemoryMaintenanceEvent::AutoDream {
                        run: dream_started.clone(),
                    })
                    .unwrap(),
                ),
            ),
            event(
                MEMORY_MAINTENANCE_ROLLOUT_EVENT,
                Some(
                    runtime_codec::encode_value(&MemoryMaintenanceEvent::AutoDream {
                        run: dream_completed.clone(),
                    })
                    .unwrap(),
                ),
            ),
            event(
                MEMORY_MAINTENANCE_ROLLOUT_EVENT,
                Some(
                    runtime_codec::encode_value(&MemoryMaintenanceEvent::AutoDream {
                        run: dream_started,
                    })
                    .unwrap(),
                ),
            ),
        ];

        let mut snapshot = memory_maintenance_from_rollout(&raw);
        assert_eq!(snapshot.auto_memory_runs, vec![failed]);
        assert_eq!(snapshot.auto_dream_runs, vec![dream_completed]);
        assert!(snapshot.interrupt_nonterminal_for_recovery(30).is_empty());
    }

    #[test]
    fn memory_maintenance_replay_terminal_tombstones_outlive_query_window() {
        let started = AutoDreamRun {
            dream_run_id: "evicted-terminal".into(),
            session_id: "session-1".into(),
            trigger: MemoryMaintenanceTrigger::Auto,
            status: MemoryMaintenanceStatus::Started,
            started_at: 1,
            ended_at: None,
            reason: None,
            error: None,
            last_consolidated_at: None,
            sessions_reviewing: 0,
            files_touched: Vec::new(),
            turns: Vec::new(),
            turn_count: 0,
            summary: None,
        };
        let mut completed = started.clone();
        completed.status = MemoryMaintenanceStatus::Completed;
        completed.ended_at = Some(2);
        let encode = |run| {
            event(
                MEMORY_MAINTENANCE_ROLLOUT_EVENT,
                Some(
                    runtime_codec::encode_value(&MemoryMaintenanceEvent::AutoDream { run })
                        .unwrap(),
                ),
            )
        };
        let mut raw = vec![encode(started.clone()), encode(completed)];
        for index in 0..64 {
            raw.push(encode(AutoDreamRun {
                dream_run_id: format!("later-skip-{index}").into(),
                session_id: "session-1".into(),
                trigger: MemoryMaintenanceTrigger::Auto,
                status: MemoryMaintenanceStatus::Skipped,
                started_at: 10 + index,
                ended_at: Some(10 + index),
                reason: Some(AutoDreamRunReason::AlreadyRunning),
                error: None,
                last_consolidated_at: None,
                sessions_reviewing: 0,
                files_touched: Vec::new(),
                turns: Vec::new(),
                turn_count: 0,
                summary: None,
            }));
        }
        // The terminal run has now left the 64-entry query window. Its stale
        // Started record must still be rejected by full-rollout replay.
        raw.push(encode(started));

        let mut snapshot = memory_maintenance_from_rollout(&raw);
        assert_eq!(snapshot.auto_dream_runs.len(), 64);
        assert!(snapshot
            .auto_dream_runs
            .iter()
            .all(|run| run.dream_run_id != "evicted-terminal"));
        assert!(snapshot.interrupt_nonterminal_for_recovery(100).is_empty());
    }

    #[test]
    fn setup_replay_restores_success_consumption_and_unknown_recovered_work() {
        let started = SetupRunRecord {
            request_id: "init-1".into(),
            session_id: "session-1".into(),
            trigger: SetupTrigger::Init,
            status: SetupRunStatus::Started,
            started_at: 10,
            ended_at: None,
            hook_count: 1,
            additional_context: Vec::new(),
            system_messages: Vec::new(),
            error: None,
        };
        let mut completed = started.clone();
        completed.status = SetupRunStatus::Completed;
        completed.ended_at = Some(20);
        completed.additional_context = vec!["persisted context".into()];
        let interrupted = SetupRunRecord {
            request_id: "maintenance-1".into(),
            session_id: "session-1".into(),
            trigger: SetupTrigger::Maintenance,
            status: SetupRunStatus::Started,
            started_at: 40,
            ended_at: None,
            hook_count: 1,
            additional_context: Vec::new(),
            system_messages: Vec::new(),
            error: None,
        };
        let encode = |lifecycle: SetupLifecycleEvent| {
            event(
                SETUP_LIFECYCLE_ROLLOUT_EVENT,
                Some(runtime_codec::encode_value(&lifecycle).unwrap()),
            )
        };
        let raw = vec![
            encode(SetupLifecycleEvent::Run { run: started }),
            encode(SetupLifecycleEvent::Run {
                run: completed.clone(),
            }),
            encode(SetupLifecycleEvent::InitClosed {
                session_id: "session-1".into(),
                at: 30,
            }),
            encode(SetupLifecycleEvent::ContextConsumed {
                session_id: "session-1".into(),
                request_id: "init-1".into(),
                at: 31,
            }),
            encode(SetupLifecycleEvent::Run { run: interrupted }),
        ];

        let mut snapshot = setup_lifecycle_from_rollout(&raw);
        assert_eq!(snapshot.completed_init(), Some(&completed));
        assert!(snapshot.pending_init_context().is_none());
        assert_eq!(snapshot.context_consumed_request_id, Some("init-1".into()));
        let recovery = snapshot.interrupt_nonterminal_for_recovery(50);
        assert_eq!(recovery.len(), 1);
        assert_eq!(
            snapshot
                .run(&RuntimeString::from("maintenance-1"))
                .unwrap()
                .status,
            SetupRunStatus::OutcomeUnknown
        );
    }

    #[test]
    fn legacy_rollout_with_a_real_turn_closes_setup_init_on_resume() {
        let raw = vec![RolloutItem::TurnContext {
            ctx: TurnContextSnapshot {
                cwd: "/workspace".into(),
                model: "model".into(),
                permission_mode: ExternalPermissionMode::Default,
                turn_id: None,
                started_at_ms: None,
                approval_policy: None,
                sandbox_mode: None,
                approvals_reviewer: None,
                network_allowed: None,
                writable_roots: None,
            },
        }];

        let snapshot = setup_lifecycle_from_rollout(&raw);
        assert_eq!(snapshot.init_closed_at, Some(0));
        assert!(snapshot.completed_init().is_none());
    }

    #[test]
    fn queue_recovery_rejects_a_malformed_latest_checkpoint() {
        let raw = vec![event(
            QUEUE_STATE_ROLLOUT_EVENT,
            Some(exact_json!({"schemaVersion": 1, "sessionId": "session-1"})),
        )];

        let error = queue_from_rollout(&raw, &RuntimeString::from("session-1"))
            .expect_err("authoritative queue corruption must fail resume");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("latest queue checkpoint"));
    }

    #[tokio::test]
    async fn resume_restores_the_turn_index_so_a_stopped_turn_stays_stopped() {
        // A reopened thread has to report how each turn ENDED. Without the restore,
        // `get_history` answered with empty turn statuses, so the client saw a turn
        // the user had stopped as a normal completed one: no stopped label, no
        // elapsed time, collapsible, and no resume entry.
        let dir = temp_dir();
        let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(dir.to_string_lossy().into_owned()),
            archive_dir: None,
        }));
        let agent = test_agent();
        let session = agent
            .open_session(store.clone(), StartSessionOptions::default())
            .await
            .unwrap();
        let thread_id = session.id.try_to_utf8().unwrap();

        let signal = AbortSignal::new();
        signal.cancel();
        let stopped = session
            .run(
                Input::Text("stop me".into()),
                TurnOptions {
                    signal: Some(signal),
                    ..TurnOptions::default()
                },
            )
            .await;
        assert_eq!(
            stopped.stop_reason,
            crate::types::errors::TerminalReason::Aborted
        );
        let live_index = session.turn_index_snapshot();
        let stopped_turn = live_index
            .turn_status
            .iter()
            .find(|(_, status)| **status == "interrupted")
            .map(|(turn_id, _)| turn_id.clone())
            .expect("the aborted turn records an interrupted status while live");
        session.shutdown_and_wait().await.unwrap();

        let resumed = agent
            .resume_session(store, &thread_id, StartSessionOptions::default())
            .await
            .unwrap();
        let index = resumed.turn_index_snapshot();
        assert_eq!(
            index.turn_status.get(&stopped_turn),
            Some(&RuntimeString::from("interrupted")),
            "a resumed thread must still report the stopped turn as interrupted"
        );
        assert!(
            index.turn_duration_ms.contains_key(&stopped_turn),
            "the stopped turn's elapsed time must survive the resume"
        );
        resumed.shutdown_and_wait().await.unwrap();
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn prediction_persists_an_overtaken_admission_without_replacing_the_live_source() {
        let dir = temp_dir();
        let store = Arc::new(store_at(&dir));
        let agent = test_agent();
        let session = agent
            .open_session(store.clone(), StartSessionOptions::default())
            .await
            .unwrap();
        let thread_id = session.id.try_to_utf8().unwrap();
        let mut index = session.turn_index_snapshot();
        index.turn_status.insert("later".into(), "completed".into());
        session.restore_turn_index(index);
        session
            .record_prediction_source_checkpoint(2, Some("later".into()))
            .await;
        session
            .record_prediction_source_checkpoint(1, Some("earlier".into()))
            .await;
        assert_eq!(
            session.prediction_context().await.unwrap().turn_id,
            Some("later".into())
        );
        session.shutdown_and_wait().await.unwrap();
        let stored = store.resume(&thread_id).await.unwrap();
        let generations = stored
            .raw_items
            .iter()
            .filter_map(|item| {
                let RolloutItem::Event(event) = item else {
                    return None;
                };
                if event.event != crate::engine::session::PREDICTION_SOURCE_ROLLOUT_EVENT {
                    return None;
                }
                event
                    .payload
                    .as_ref()?
                    .as_object()?
                    .get("generation")?
                    .as_str()
                    .cloned()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            generations,
            vec![RuntimeString::from("2"), RuntimeString::from("1")],
            "a completed older admission still needs its own durable checkpoint"
        );
        assert_eq!(
            prediction_source_from_rollout(&stored.raw_items).unwrap(),
            Some((2, Some("later".into())))
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn resume_prediction_source_preserves_overlapping_admission_order_and_epoch() {
        struct PausedTurnClock {
            target: Mutex<Option<std::thread::ThreadId>>,
            entered: tokio::sync::Notify,
            released: Mutex<bool>,
            release: std::sync::Condvar,
            timed_out: std::sync::atomic::AtomicBool,
        }
        impl PausedTurnClock {
            fn now(&self) -> i64 {
                let pause = {
                    let mut target = self.target.lock().unwrap();
                    if target.as_ref() == Some(&std::thread::current().id()) {
                        *target = None;
                        true
                    } else {
                        false
                    }
                };
                if pause {
                    self.entered.notify_one();
                    let (released, _) = self
                        .release
                        .wait_timeout_while(
                            self.released.lock().unwrap(),
                            std::time::Duration::from_secs(10),
                            |released| !*released,
                        )
                        .unwrap();
                    if !*released {
                        self.timed_out.store(true, Ordering::SeqCst);
                    }
                }
                1_000
            }

            fn release(&self) {
                *self
                    .released
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
                self.release.notify_all();
            }
        }
        struct ReleasePausedTurn(Arc<PausedTurnClock>);
        impl Drop for ReleasePausedTurn {
            fn drop(&mut self) {
                self.0.release();
            }
        }

        fn turn_for_input(session: &crate::engine::session::Session, input: &str) -> RuntimeString {
            let index = session.turn_index_snapshot();
            session
                .history()
                .into_iter()
                .find_map(|message| match message {
                    Message::User(user)
                        if user.content.iter().any(
                            |block| matches!(block, ContentBlock::Text(text) if text.text == input),
                        ) =>
                    {
                        index.message_turn.get(&user.uuid).cloned()
                    }
                    _ => None,
                })
                .expect("a completed run must map its input to a turn")
        }

        let dir = temp_dir();
        let store = Arc::new(store_at(&dir));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let clock = Arc::new(PausedTurnClock {
            target: Mutex::new(None),
            entered: tokio::sync::Notify::new(),
            released: Mutex::new(false),
            release: std::sync::Condvar::new(),
            timed_out: std::sync::atomic::AtomicBool::new(false),
        });
        let _release_on_failure = ReleasePausedTurn(clock.clone());
        let mut options = test_agent_options(None, Vec::new());
        options.deps.call_model = Some(recording_call_model(requests.clone()));
        options.deps.now = Some({
            let clock = clock.clone();
            Arc::new(move || clock.now())
        });
        let agent = Agent::new(options);
        let session = agent
            .open_session(store.clone(), StartSessionOptions::default())
            .await
            .unwrap();
        let thread_id = session.id.try_to_utf8().unwrap();
        let completed = crate::types::errors::TerminalReason::Completed;
        // Close Setup Init before arming the turn-start clock barrier.
        assert_eq!(
            session
                .run(Input::Text("warmup".into()), TurnOptions::default())
                .await
                .stop_reason,
            completed
        );
        let warmup_turn = turn_for_input(&session, "warmup");
        let runtime = tokio::runtime::Handle::current();
        let earlier = tokio::task::spawn_blocking({
            let session = session.clone();
            let clock = clock.clone();
            move || {
                *clock.target.lock().unwrap() = Some(std::thread::current().id());
                runtime.block_on(session.run(
                    Input::Text("earlier admitted".into()),
                    TurnOptions::default(),
                ))
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), clock.entered.notified())
            .await
            .expect("the earlier admitted turn must reach its start-clock barrier");
        assert_eq!(requests.lock().unwrap().len(), 1);
        assert_eq!(
            session
                .run(Input::Text("later admitted".into()), TurnOptions::default())
                .await
                .stop_reason,
            completed
        );
        let later_turn = turn_for_input(&session, "later admitted");
        assert_ne!(later_turn, warmup_turn);
        assert!(!clock.timed_out.load(Ordering::SeqCst));
        clock.release();
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(5), earlier)
                .await
                .expect("the clock release must let the earlier turn complete")
                .unwrap()
                .stop_reason,
            completed
        );
        assert!(!clock.timed_out.load(Ordering::SeqCst));
        let earlier_turn = turn_for_input(&session, "earlier admitted");
        assert_ne!(earlier_turn, warmup_turn);
        assert_ne!(earlier_turn, later_turn);
        let live = session.prediction_context().await.unwrap();
        assert_eq!(live.turn_id, Some(later_turn.clone()));
        assert!(!live.running);
        let index = session.turn_index_snapshot();
        for turn_id in [&warmup_turn, &earlier_turn, &later_turn] {
            assert_eq!(index.turn_started_at_ms.get(turn_id), Some(&1_000));
            assert_eq!(index.turn_finished_at_ms.get(turn_id), Some(&1_000));
        }
        session.shutdown_and_wait().await.unwrap();

        let persisted = store.resume(&thread_id).await.unwrap();
        let context_order = persisted
            .raw_items
            .iter()
            .filter_map(|item| match item {
                RolloutItem::TurnContext { ctx } => ctx.turn_id.clone(),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            context_order,
            vec![warmup_turn, later_turn.clone(), earlier_turn.clone()],
            "physical context append order must differ from admission order"
        );
        let resumed = agent
            .resume_session(store.clone(), &thread_id, StartSessionOptions::default())
            .await
            .unwrap();
        let context = resumed.prediction_context().await.unwrap();
        assert_eq!(
            context.turn_id,
            Some(later_turn.clone()),
            "recovery must preserve the highest admitted generation, not the last context line"
        );
        assert!(!context.running);
        assert_eq!(
            resumed
                .predict_message_with_signal(
                    "dynamic prompt".into(),
                    earlier_turn.try_to_utf8().unwrap(),
                    None,
                    AbortSignal::new(),
                )
                .await
                .unwrap(),
            None
        );
        assert_eq!(requests.lock().unwrap().len(), 3);
        assert_eq!(
            resumed
                .run(Input::Text("after resume".into()), TurnOptions::default())
                .await
                .stop_reason,
            completed
        );
        let continued_turn = resumed.prediction_context().await.unwrap().turn_id.unwrap();
        assert_ne!(continued_turn, later_turn);
        assert_ne!(continued_turn, earlier_turn);
        resumed.shutdown_and_wait().await.unwrap();

        let continued = agent
            .resume_session(store.clone(), &thread_id, StartSessionOptions::default())
            .await
            .unwrap();
        assert_eq!(
            continued.prediction_context().await.unwrap().turn_id,
            Some(continued_turn.clone()),
            "new admissions must advance beyond every generation recovered from disk"
        );
        assert_eq!(continued.rollback(1).await.unwrap().dropped_turns, 1);
        let retained_history = continued.history();
        assert_eq!(
            continued.prediction_context().await.unwrap().turn_id,
            Some(earlier_turn.clone())
        );
        continued.shutdown_and_wait().await.unwrap();

        let rewound = agent
            .resume_session(store, &thread_id, StartSessionOptions::default())
            .await
            .unwrap();
        assert_eq!(
            rewound.prediction_context().await.unwrap().turn_id,
            Some(earlier_turn),
            "a persisted rollback must clear older admission markers and use retained history"
        );
        assert_eq!(rewound.history(), retained_history);
        assert_eq!(
            rewound
                .predict_message_with_signal(
                    "dynamic prompt".into(),
                    continued_turn.try_to_utf8().unwrap(),
                    None,
                    AbortSignal::new(),
                )
                .await
                .unwrap(),
            None
        );
        assert_eq!(requests.lock().unwrap().len(), 4);
        rewound.shutdown_and_wait().await.unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn resume_prediction_source_follows_message_free_terminal_order_and_rollback() {
        let dir = temp_dir();
        let store = Arc::new(store_at(&dir));
        let live = store.create(meta()).await.unwrap();
        for (turn_id, messages, status) in [
            ("retained-turn", Some(("u1", "a1")), "completed"),
            ("older-message-turn", Some(("u2", "a2")), "completed"),
            ("latest-empty-turn", None, "failed"),
        ] {
            live.append_turn_context(TurnContextSnapshot {
                cwd: "/x".into(),
                model: "fake-model".into(),
                permission_mode: ExternalPermissionMode::Default,
                turn_id: Some(turn_id.into()),
                started_at_ms: Some(1_000),
                approval_policy: None,
                sandbox_mode: None,
                approvals_reviewer: None,
                network_allowed: None,
                writable_roots: None,
            })
            .await;
            if let Some((user_id, assistant_id)) = messages {
                live.append_message(user(user_id)).await;
                live.append_message(assistant_with_usage(
                    assistant_id,
                    Usage {
                        input_tokens: 0,
                        output_tokens: 0,
                        cache_read_input_tokens: 0,
                        cache_creation_input_tokens: 0,
                        total_tokens: None,
                    },
                ))
                .await;
            }
            let mut payload = RuntimeObject::new();
            payload.insert("status", RuntimeJson::from(status));
            payload.insert("startedAtMs", RuntimeJson::from(1_000i64));
            payload.insert("finishedAtMs", RuntimeJson::from(1_000i64));
            payload.insert("durationMs", RuntimeJson::from(0i64));
            live.append_event(
                "turn_lifecycle".into(),
                Some(AppendEventOptions {
                    turn_id: Some(turn_id.into()),
                    payload: Some(RuntimeJson::Object(payload)),
                }),
            )
            .await;
        }
        live.shutdown().await.unwrap();
        let before = store.resume("sess-1").await.unwrap().messages;
        assert_eq!(before.len(), 4);
        let agent = test_agent_with_runtime(
            None,
            Vec::new(),
            Some(scripted_call_model(vec![vec![
                ev::message_start(None),
                ev::text(r#"{"suggestion":"Continue after the failure"}"#),
                ev::stop(None, None),
            ]])),
            None,
        );
        let resumed = agent
            .resume_session(store.clone(), "sess-1", StartSessionOptions::default())
            .await
            .unwrap();
        let index = resumed.turn_index_snapshot();
        for turn_id in ["retained-turn", "older-message-turn", "latest-empty-turn"] {
            let turn_id = RuntimeString::from(turn_id);
            assert_eq!(index.turn_started_at_ms.get(&turn_id), Some(&1_000));
            assert_eq!(index.turn_finished_at_ms.get(&turn_id), Some(&1_000));
        }
        assert_eq!(
            index
                .turn_status
                .get(&RuntimeString::from("latest-empty-turn")),
            Some(&RuntimeString::from("failed"))
        );
        assert!(index
            .message_turn
            .values()
            .all(|id| id != "latest-empty-turn"));
        let context = resumed.prediction_context().await.unwrap();
        assert_eq!(context.turn_id, Some("latest-empty-turn".into()));
        assert!(!context.running);
        assert_eq!(
            resumed
                .predict_message_with_signal(
                    "dynamic prompt".into(),
                    "older-message-turn".into(),
                    None,
                    AbortSignal::new(),
                )
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            resumed
                .predict_message_with_signal(
                    "dynamic prompt".into(),
                    "latest-empty-turn".into(),
                    None,
                    AbortSignal::new(),
                )
                .await
                .unwrap(),
            Some("Continue after the failure".into())
        );
        assert_eq!(resumed.history(), before);
        assert_eq!(resumed.turn_index_snapshot(), index);
        resumed.shutdown_and_wait().await.unwrap();

        let live = store.reopen("sess-1").await.unwrap();
        live.append_rollback(RollbackRecord { num_turns: 1 }).await;
        live.shutdown().await.unwrap();
        let rewound = agent
            .resume_session(store, "sess-1", StartSessionOptions::default())
            .await
            .unwrap();
        let context = rewound.prediction_context().await.unwrap();
        assert_eq!(context.turn_id, Some("retained-turn".into()));
        assert!(!context.running);
        assert_eq!(rewound.history(), before[..2]);
        rewound.shutdown_and_wait().await.unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn resume_session_normalizes_nonterminal_background_processes() {
        let dir = temp_dir();
        let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(dir.to_string_lossy().into_owned()),
            archive_dir: None,
        }));
        let agent = test_agent();
        let session = agent
            .open_session(store.clone(), StartSessionOptions::default())
            .await
            .unwrap();
        let thread_id = session.id.try_to_utf8().unwrap();
        session.shutdown_and_wait().await.unwrap();

        let live = store.reopen(&thread_id).await.unwrap();
        live.append_background_process_lifecycle(BackgroundProcessRecord {
            shell_id: "cold-shell".into(),
            command: "sleep 30".into(),
            cwd: "/tmp".into(),
            status: crate::schemas::background_process::BackgroundProcessStatus::Running,
            started_at_ms: 1,
            updated_at_ms: 2,
            completed_at_ms: None,
            exit_code: None,
            termination_signal: None,
            failure: None,
            stop_requested: false,
            output_start_offset: 0,
            output_next_offset: 0,
            dropped_bytes: 0,
            output_available: false,
        })
        .await;
        live.shutdown().await.unwrap();

        let resumed = agent
            .resume_session(store.clone(), &thread_id, StartSessionOptions::default())
            .await
            .unwrap();
        let process = resumed
            .background_process_snapshot()
            .processes
            .into_iter()
            .find(|process| process.shell_id == "cold-shell")
            .expect("recovered background process");
        assert_eq!(
            process.status,
            crate::schemas::background_process::BackgroundProcessStatus::Stopped
        );
        assert!(process.stop_requested);
        resumed.shutdown_and_wait().await.unwrap();

        let stored = store.resume(&thread_id).await.unwrap();
        let final_record = background_processes_from_rollout(&stored.raw_items)
            .into_iter()
            .find(|process| process.shell_id == "cold-shell")
            .expect("normalized background process persisted");
        assert_eq!(final_record, process);
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn malformed_queue_full_resume_releases_atomic_writer_lease_and_can_retry() {
        let dir = temp_dir();
        let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(dir.to_string_lossy().into_owned()),
            archive_dir: None,
        }));
        let agent = test_agent();
        let session = agent
            .open_session(store.clone(), StartSessionOptions::default())
            .await
            .unwrap();
        let thread_id = session.id.try_to_utf8().unwrap();
        session.shutdown_and_wait().await.unwrap();

        let malformed = store.reopen(&thread_id).await.unwrap();
        malformed
            .append_event(
                QUEUE_STATE_ROLLOUT_EVENT.into(),
                Some(AppendEventOptions {
                    turn_id: None,
                    payload: Some(exact_json!({"schemaVersion": 1, "sessionId": "missing-fields"})),
                }),
            )
            .await;
        malformed.flush().await.unwrap();
        malformed.shutdown().await.unwrap();

        let error = match agent
            .resume_session(store.clone(), &thread_id, StartSessionOptions::default())
            .await
        {
            Ok(session) => {
                let _ = session.shutdown_and_wait().await;
                panic!("malformed authoritative queue state must fail full resume")
            }
            Err(error) => error,
        };
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("latest queue checkpoint"));

        // Acquiring a writer immediately is the cleanup proof: a leaked
        // current-PID sidecar would return WouldBlock for the rest of the
        // process lifetime.
        let repair = store.reopen(&thread_id).await.unwrap();
        let checkpoint = json!({
            "schemaVersion": 1,
            "sessionId": thread_id.clone(),
            "revision": 0,
            "items": [],
            "reserved": [],
            "tombstones": [],
            "mutations": [],
        });
        let payload = crate::runtime_values::runtime_json::parse(&RuntimeString::from(
            serde_json::to_string(&checkpoint).unwrap(),
        ))
        .unwrap();
        repair
            .append_event(
                QUEUE_STATE_ROLLOUT_EVENT.into(),
                Some(AppendEventOptions {
                    turn_id: None,
                    payload: Some(payload),
                }),
            )
            .await;
        repair.flush().await.unwrap();
        repair.shutdown().await.unwrap();

        let resumed = agent
            .resume_session(store, &thread_id, StartSessionOptions::default())
            .await
            .unwrap();
        assert!(resumed.queue_snapshot().items.is_empty());
        resumed.shutdown_and_wait().await.unwrap();
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn cancelled_handoff_after_successful_offer_closes_the_undelivered_session() {
        let rollout = ControlledQueueRecoveryRollout::new(QueueRecoveryFlushMode::Pass);
        let session = test_agent().start_session(StartSessionOptions {
            rollout: Some(rollout.clone()),
            ..StartSessionOptions::default()
        });
        let (send, receive) = tokio::sync::oneshot::channel();
        let (offered_send, offered_receive) = tokio::sync::oneshot::channel();
        tokio::spawn(Agent::offer_resumed_session(session, send, move || {
            let _ = offered_send.send(());
        }));

        offered_receive
            .await
            .expect("the Session was placed in the result channel");
        // This is the exact send-success / receive-not-polled cancellation
        // window: dropping the channel also drops the acceptance sender stored
        // inside the unread handoff.
        drop(receive);
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            rollout.wait_for_shutdown(),
        )
        .await
        .expect("the retained owner closes a Session abandoned in the result slot");
        assert_eq!(rollout.shutdown_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn resume_handoff_owner_panic_is_caught_and_cannot_return_a_live_session() {
        let rollout = ControlledQueueRecoveryRollout::new(QueueRecoveryFlushMode::Pass);
        let session = test_agent().start_session(StartSessionOptions {
            rollout: Some(rollout.clone()),
            ..StartSessionOptions::default()
        });
        let (send, receive) = tokio::sync::oneshot::channel();
        tokio::spawn(Agent::offer_resumed_session(session, send, || {
            panic!("injected owner panic after result-channel send")
        }));

        let admission = receive
            .await
            .expect("offer reached the result channel")
            .expect("queue durability itself succeeded");
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            rollout.wait_for_shutdown(),
        )
        .await
        .expect("the owner catches its panic and closes the offered Session");
        assert!(
            admission.accepted.send(()).is_err(),
            "the caller must observe that the retained owner rejected the handoff"
        );
        drop(admission.session);
        assert_eq!(rollout.shutdown_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn resume_rejects_and_closes_a_raw_replacement_rollout_that_bypasses_store_lease() {
        let dir = temp_dir();
        let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(dir.to_string_lossy().into_owned()),
            archive_dir: None,
        }));
        let rollout = ControlledQueueRecoveryRollout::new(QueueRecoveryFlushMode::Pass);
        let result = test_agent()
            .resume_session(
                store,
                "not-opened",
                StartSessionOptions {
                    rollout: Some(rollout.clone()),
                    ..StartSessionOptions::default()
                },
            )
            .await;
        let error = match result {
            Ok(session) => {
                let _ = session.shutdown_and_wait().await;
                panic!("a raw replacement rollout must not bypass the atomic store lease")
            }
            Err(error) => error,
        };
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("wrap that leased rollout"));
        assert_eq!(
            rollout.shutdown_count.load(Ordering::SeqCst),
            1,
            "ownership transferred in StartSessionOptions is closed before rejection returns"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn resume_session_waits_for_recovered_queue_normalization_before_admission() {
        let dir = temp_dir();
        let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(dir.to_string_lossy().into_owned()),
            archive_dir: None,
        }));
        let agent = Arc::new(test_agent());
        let thread_id = seed_reserved_queue_checkpoint(&agent, &store).await;
        let rollout = ControlledQueueRecoveryRollout::new(QueueRecoveryFlushMode::BlockFirst);
        let resume_store = controlled_queue_recovery_store(store.clone(), rollout.clone());
        let mut resume = tokio::spawn({
            let agent = agent.clone();
            let store = resume_store.clone();
            let thread_id = thread_id.clone();
            async move {
                agent
                    .resume_session(store, &thread_id, StartSessionOptions::default())
                    .await
            }
        });

        rollout.wait_for_first_flush().await;
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut resume)
                .await
                .is_err(),
            "the core resume API must not expose a Session before normalization is durable"
        );
        rollout.release_first_flush();
        let resumed = tokio::time::timeout(std::time::Duration::from_secs(5), &mut resume)
            .await
            .expect("resume completes after the durability barrier")
            .expect("resume owner task")
            .expect("durable recovered Session");
        assert!(
            resumed.queue_snapshot().items.is_empty(),
            "the crash-owned reserved item is normalized before admission"
        );
        resumed.shutdown_and_wait().await.unwrap();
        assert_eq!(rollout.shutdown_count.load(Ordering::SeqCst), 1);
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn resume_session_waits_for_all_constructor_recovery_prefixes_before_admission() {
        let dir = temp_dir();
        let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(dir.to_string_lossy().into_owned()),
            archive_dir: None,
        }));
        let agent = Arc::new(test_agent());
        let (session_id, setup_request_id, memory_run_id, snapshot_request_id) =
            seed_constructor_recovery_prefixes(&agent, &store).await;
        let thread_id = session_id.try_to_utf8().unwrap();
        let rollout = ControlledQueueRecoveryRollout::new(QueueRecoveryFlushMode::BlockFirstN(3));
        let resume_store = controlled_queue_recovery_store(store.clone(), rollout.clone());
        let mut resume = tokio::spawn({
            let agent = agent.clone();
            let store = resume_store.clone();
            let thread_id = thread_id.clone();
            async move {
                agent
                    .resume_session(store, &thread_id, StartSessionOptions::default())
                    .await
            }
        });

        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            rollout.wait_for_flush_count(3),
        )
        .await
        .expect("all three constructor recovery owners reach their flush barriers");
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut resume)
                .await
                .is_err(),
            "Setup, maintenance, and Snapshot recovery must all remain behind their own durability prefixes"
        );
        rollout.release_flushes(2);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut resume)
                .await
                .is_err(),
            "releasing only two of three constructor recovery owners must not expose the Session"
        );
        rollout.release_flushes(1);

        let resumed = tokio::time::timeout(std::time::Duration::from_secs(5), &mut resume)
            .await
            .expect("resume completes after every constructor recovery prefix")
            .expect("resume owner task")
            .expect("durable recovered Session");
        assert_eq!(
            resumed
                .setup_lifecycle_snapshot()
                .run(&setup_request_id)
                .expect("Setup recovery audit")
                .status,
            SetupRunStatus::OutcomeUnknown
        );
        assert!(resumed
            .memory_maintenance_snapshot()
            .expect("maintenance recovery snapshot")
            .auto_memory_runs
            .iter()
            .any(|run| {
                run.run_id == memory_run_id
                    && run.status == MemoryMaintenanceStatus::Failed
                    && run.reason == Some(AutoMemoryRunReason::Interrupted)
            }));
        assert_eq!(
            resumed
                .agent_memory_snapshot_decision(&snapshot_request_id)
                .await
                .expect("Snapshot recovery durability")
                .expect("Snapshot recovery audit")
                .status,
            AgentMemorySnapshotDecisionStatus::Disconnected
        );
        assert!(
            rollout.flush_count.load(Ordering::SeqCst) >= 4,
            "the three per-prefix flushes are followed by one final admission flush"
        );

        resumed.shutdown_and_wait().await.unwrap();
        assert_eq!(rollout.shutdown_count.load(Ordering::SeqCst), 1);
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn resume_session_failure_closes_the_constructed_session_before_returning() {
        let dir = temp_dir();
        let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(dir.to_string_lossy().into_owned()),
            archive_dir: None,
        }));
        let agent = test_agent();
        let thread_id = seed_reserved_queue_checkpoint(&agent, &store).await;
        let rollout = ControlledQueueRecoveryRollout::new(QueueRecoveryFlushMode::FailFirst);
        let resume_store = controlled_queue_recovery_store(store.clone(), rollout.clone());

        let result = agent
            .resume_session(resume_store, &thread_id, StartSessionOptions::default())
            .await;
        let error = match result {
            Ok(session) => {
                let _ = session.shutdown_and_wait().await;
                panic!("failed recovery normalization must not return a Session")
            }
            Err(error) => error,
        };
        assert!(error
            .to_string()
            .contains("failed to persist recovered queue normalization"));
        assert_eq!(
            rollout.shutdown_count.load(Ordering::SeqCst),
            1,
            "failure is returned only after the constructed rollout is closed"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancelled_resume_retains_recovery_ownership_and_closes_the_orphan_session() {
        let dir = temp_dir();
        let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(dir.to_string_lossy().into_owned()),
            archive_dir: None,
        }));
        let agent = Arc::new(test_agent());
        let thread_id = seed_reserved_queue_checkpoint(&agent, &store).await;
        let rollout = ControlledQueueRecoveryRollout::new(QueueRecoveryFlushMode::BlockFirst);
        let resume_store = controlled_queue_recovery_store(store.clone(), rollout.clone());
        let resume = tokio::spawn({
            let agent = agent.clone();
            let store = resume_store.clone();
            let thread_id = thread_id.clone();
            async move {
                agent
                    .resume_session(store, &thread_id, StartSessionOptions::default())
                    .await
            }
        });

        rollout.wait_for_first_flush().await;
        resume.abort();
        let _ = resume.await;
        rollout.release_first_flush();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            rollout.wait_for_shutdown(),
        )
        .await
        .expect("retained recovery owner closes the undelivered Session");
        assert_eq!(rollout.shutdown_count.load(Ordering::SeqCst), 1);
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn cancelled_resume_does_not_wait_forever_for_a_stuck_recovery_receipt() {
        let dir = temp_dir();
        let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(dir.to_string_lossy().into_owned()),
            archive_dir: None,
        }));
        let agent = Arc::new(test_agent());
        let thread_id = seed_reserved_queue_checkpoint(&agent, &store).await;
        let rollout = ControlledQueueRecoveryRollout::new(QueueRecoveryFlushMode::BlockFirst);
        let resume_store = controlled_queue_recovery_store(store.clone(), rollout.clone());
        let resume = tokio::spawn({
            let agent = agent.clone();
            let store = resume_store.clone();
            let thread_id = thread_id.clone();
            async move {
                agent
                    .resume_session(store, &thread_id, StartSessionOptions::default())
                    .await
            }
        });

        rollout.wait_for_first_flush().await;
        tokio::time::pause();
        resume.abort();
        let _ = resume.await;
        // Never release the checkpoint flush. The retained owner must notice
        // the dropped caller and enter the Session's bounded typed shutdown;
        // advancing beyond all shutdown phases proves it does not depend on
        // the orphaned durability future resolving by itself.
        for _ in 0..3 {
            for _ in 0..16 {
                tokio::task::yield_now().await;
            }
            tokio::time::advance(std::time::Duration::from_secs(120)).await;
        }
        // Poll with small virtual-time steps: under parallel test load the
        // shutdown machinery may still be crossing timer phases, and a pure
        // yield loop can observe the counter before those timers fire.
        for _ in 0..512 {
            if rollout.shutdown_count.load(Ordering::SeqCst) > 0 {
                break;
            }
            tokio::time::advance(std::time::Duration::from_millis(100)).await;
            tokio::task::yield_now().await;
        }
        assert_eq!(rollout.shutdown_started.load(Ordering::SeqCst), 1,
            "all producers must be fenced and raw shutdown entered within the virtual cleanup deadlines");
        // Virtual timer advancement does not drive filesystem worker I/O.
        // Keep the checkpoint blocked and await the actual close receipt.
        tokio::time::resume();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            rollout.wait_for_shutdown(),
        )
        .await
        .expect("the retained raw close completes independently of the stuck checkpoint");
        assert_eq!(
            rollout.shutdown_count.load(Ordering::SeqCst),
            1,
            "caller cancellation must drive bounded cleanup even when the checkpoint never resolves"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn recovered_queue_checkpoint_is_durable_and_idempotent_across_repeated_resume() {
        let dir = temp_dir();
        let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(dir.to_string_lossy().into_owned()),
            archive_dir: None,
        }));
        let agent = test_agent();
        let thread_id = seed_reserved_queue_checkpoint(&agent, &store).await;

        let first = agent
            .resume_session(store.clone(), &thread_id, StartSessionOptions::default())
            .await
            .unwrap();
        let first_snapshot = first.queue_snapshot();
        assert!(first_snapshot.items.is_empty());
        let persisted = store.resume(&thread_id).await.unwrap();
        let (_, needs_normalization) = queue_from_rollout(&persisted.raw_items, &first.id)
            .unwrap()
            .expect("normalization checkpoint is present when resume returns");
        assert!(
            !needs_normalization,
            "the correlated resume result is already backed by a stable checkpoint"
        );
        first.shutdown_and_wait().await.unwrap();

        let second = agent
            .resume_session(store.clone(), &thread_id, StartSessionOptions::default())
            .await
            .unwrap();
        assert_eq!(second.queue_snapshot(), first_snapshot);
        let persisted_again = store.resume(&thread_id).await.unwrap();
        let (_, needs_normalization_again) =
            queue_from_rollout(&persisted_again.raw_items, &second.id)
                .unwrap()
                .expect("stable checkpoint survives repeated resume");
        assert!(!needs_normalization_again);
        second.shutdown_and_wait().await.unwrap();
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn queue_state_and_mutation_idempotency_survive_real_session_resume() {
        let dir = temp_dir();
        let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(dir.to_string_lossy().into_owned()),
            archive_dir: None,
        }));
        let agent = test_agent();
        let session = agent
            .open_session(store.clone(), StartSessionOptions::default())
            .await
            .unwrap();
        let original_session = Arc::clone(&session);
        let thread_id = session.id.try_to_utf8().unwrap();
        session
            .enqueue(Input::Text("first".into()), EnqueueOptions::default())
            .unwrap();
        session
            .enqueue(Input::Text("second".into()), EnqueueOptions::default())
            .unwrap();
        session
            .await_queue_durability()
            .await
            .expect("both initial queue checkpoints become authoritative");
        let initial = session.queue_snapshot();
        let first_uuid = initial.items[0].message.uuid.clone().unwrap();
        let second_uuid = initial.items[1].message.uuid.clone().unwrap();

        let edited = session
            .edit_queued_message(
                "resume-edit".into(),
                first_uuid.clone(),
                initial.revision,
                QueueEdit {
                    value: Some(QueuedValue::Text("edited".into())),
                    ..Default::default()
                },
            )
            .await;
        assert_eq!(edited.status, QueueMutationStatus::Applied);
        let removed = session
            .remove_queued_message(
                "resume-remove".into(),
                second_uuid.clone(),
                edited.snapshot.revision,
            )
            .await;
        assert_eq!(removed.status, QueueMutationStatus::Applied);
        assert_eq!(removed.terminal_state, Some(QueueTerminalState::Removed));
        let stale = session
            .remove_queued_message("resume-stale".into(), first_uuid.clone(), initial.revision)
            .await;
        assert_eq!(stale.status, QueueMutationStatus::StaleRevision);
        let authoritative_before_restart = session.queue_snapshot();

        // The mutation result is itself the durability boundary: a fresh read
        // must already contain both the queue revision and mutation-id ledger,
        // without relying on Session shutdown to flush it later.
        let durable_after_ack = store.resume(&thread_id).await.unwrap();
        let (queue_after_ack, normalized) =
            queue_from_rollout(&durable_after_ack.raw_items, &session.id)
                .unwrap()
                .expect("Applied result already persisted a queue checkpoint");
        assert!(!normalized);
        assert_eq!(
            queue_after_ack.queue_snapshot(),
            authoritative_before_restart
        );
        assert_eq!(
            queue_after_ack
                .edit_queued(
                    "resume-edit".into(),
                    first_uuid.clone(),
                    initial.revision,
                    QueueEdit {
                        value: Some(QueuedValue::Text("edited".into())),
                        ..Default::default()
                    },
                )
                .await,
            edited
        );
        session.shutdown_and_wait().await.unwrap();

        let persisted = store.resume(&thread_id).await.unwrap();
        let recovered_from_rollout = queue_from_rollout(&persisted.raw_items, &session.id).unwrap();
        assert!(recovered_from_rollout.is_some());

        let resumed = agent
            .resume_session(store.clone(), &thread_id, StartSessionOptions::default())
            .await
            .unwrap();
        assert!(!Arc::ptr_eq(&original_session, &resumed));
        assert_eq!(resumed.queue_snapshot(), authoritative_before_restart);
        assert_eq!(
            resumed.queue_snapshot().items[0].message.value,
            QueuedValue::Text("edited".into())
        );

        assert_eq!(
            resumed
                .edit_queued_message(
                    "resume-edit".into(),
                    first_uuid.clone(),
                    initial.revision,
                    QueueEdit {
                        value: Some(QueuedValue::Text("edited".into())),
                        ..Default::default()
                    },
                )
                .await,
            edited
        );
        assert_eq!(
            resumed
                .remove_queued_message(
                    "resume-remove".into(),
                    second_uuid.clone(),
                    edited.snapshot.revision,
                )
                .await,
            removed
        );
        assert_eq!(
            resumed
                .remove_queued_message("resume-stale".into(), first_uuid.clone(), initial.revision,)
                .await,
            stale
        );
        assert_eq!(
            resumed
                .remove_queued_message(
                    "resume-edit".into(),
                    first_uuid.clone(),
                    authoritative_before_restart.revision,
                )
                .await
                .status,
            QueueMutationStatus::MutationConflict
        );

        let removed_after_resume = resumed
            .remove_queued_message(
                "resume-remove-after".into(),
                first_uuid.clone(),
                authoritative_before_restart.revision,
            )
            .await;
        assert_eq!(removed_after_resume.status, QueueMutationStatus::Applied);
        assert!(removed_after_resume.snapshot.items.is_empty());
        resumed.shutdown_and_wait().await.unwrap();

        let resumed_again = agent
            .resume_session(store, &thread_id, StartSessionOptions::default())
            .await
            .unwrap();
        assert_eq!(
            resumed_again
                .remove_queued_message(
                    "resume-remove-after".into(),
                    first_uuid.clone(),
                    authoritative_before_restart.revision,
                )
                .await,
            removed_after_resume
        );
        let missing = resumed_again
            .remove_queued_message(
                "resume-missing-tombstone".into(),
                first_uuid,
                resumed_again.queue_snapshot().revision,
            )
            .await;
        assert_eq!(missing.status, QueueMutationStatus::Missing);
        assert_eq!(missing.terminal_state, Some(QueueTerminalState::Removed));
        resumed_again.shutdown_and_wait().await.unwrap();
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_memory_maintenance_live_order_matches_durable_replay() {
        let dir = temp_dir();
        let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(dir.to_string_lossy().into_owned()),
            archive_dir: None,
        }));
        let agent = test_agent();
        let session = agent
            .open_session(store.clone(), StartSessionOptions::default())
            .await
            .unwrap();
        let thread_id = session.id.try_to_utf8().unwrap();
        let live = Arc::new(Mutex::new(Vec::new()));
        session.set_ordered_memory_maintenance_event_sink(Some({
            let live = live.clone();
            Arc::new(move |event| live.lock().unwrap().push(event))
        }));

        const RUNS: usize = 16;
        let barrier = Arc::new(tokio::sync::Barrier::new(RUNS + 1));
        let mut tasks = Vec::new();
        for index in 0..RUNS {
            let session = session.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                if index % 2 == 0 {
                    let started = AutoMemoryRun {
                        run_id: format!("concurrent-memory-{index}").into(),
                        session_id: session.id.clone(),
                        trigger: MemoryMaintenanceTrigger::Auto,
                        scope: AgentMemoryScope::Project,
                        target_dir: "/tmp/memory".into(),
                        status: MemoryMaintenanceStatus::Started,
                        started_at: index as i64,
                        ended_at: None,
                        reason: None,
                        error: None,
                        message_count: 1,
                        cursor_message_uuid: None,
                        files_touched: Vec::new(),
                        turn_count: None,
                    };
                    assert!(
                        session
                            .commit_memory_maintenance_event_durable(
                                MemoryMaintenanceEvent::AutoMemory {
                                    run: started.clone(),
                                },
                                None,
                                true
                            )
                            .await
                    );
                    tokio::task::yield_now().await;
                    let mut failed = started;
                    failed.status = MemoryMaintenanceStatus::Failed;
                    failed.ended_at = Some(100 + index as i64);
                    failed.reason = Some(AutoMemoryRunReason::ForkFailure);
                    failed.error = Some("synthetic concurrent failure".into());
                    assert!(
                        session
                            .commit_memory_maintenance_event_durable(
                                MemoryMaintenanceEvent::AutoMemory { run: failed },
                                None,
                                true
                            )
                            .await
                    );
                } else {
                    let started = AutoDreamRun {
                        dream_run_id: format!("concurrent-dream-{index}").into(),
                        session_id: session.id.clone(),
                        trigger: MemoryMaintenanceTrigger::Auto,
                        status: MemoryMaintenanceStatus::Started,
                        started_at: index as i64,
                        ended_at: None,
                        reason: None,
                        error: None,
                        last_consolidated_at: None,
                        sessions_reviewing: 1,
                        files_touched: Vec::new(),
                        turns: Vec::new(),
                        turn_count: 0,
                        summary: None,
                    };
                    assert!(
                        session
                            .commit_memory_maintenance_event_durable(
                                MemoryMaintenanceEvent::AutoDream {
                                    run: started.clone(),
                                },
                                None,
                                true
                            )
                            .await
                    );
                    tokio::task::yield_now().await;
                    let mut completed = started;
                    completed.status = MemoryMaintenanceStatus::Completed;
                    completed.ended_at = Some(100 + index as i64);
                    assert!(
                        session
                            .commit_memory_maintenance_event_durable(
                                MemoryMaintenanceEvent::AutoDream { run: completed },
                                None,
                                true
                            )
                            .await
                    );
                }
            }));
        }
        barrier.wait().await;
        for task in tasks {
            task.await.unwrap();
        }

        let live_snapshot = session
            .memory_maintenance_snapshot()
            .expect("live maintenance snapshot");
        session.shutdown_and_wait().await.unwrap();
        let persisted = store.resume(&thread_id).await.unwrap();
        let persisted_events = persisted
            .raw_items
            .iter()
            .filter_map(|item| {
                let RolloutItem::Event(event) = item else {
                    return None;
                };
                if event.event != MEMORY_MAINTENANCE_ROLLOUT_EVENT {
                    return None;
                }
                event.payload.as_ref().map(|payload| {
                    runtime_codec::decode_value::<MemoryMaintenanceEvent>(payload).unwrap()
                })
            })
            .collect::<Vec<_>>();

        assert_eq!(persisted_events, *live.lock().unwrap());
        assert_eq!(persisted_events.len(), RUNS * 2);
        assert_eq!(
            memory_maintenance_from_rollout(&persisted.raw_items),
            live_snapshot
        );
        assert_eq!(live_snapshot.auto_memory_runs.len(), RUNS / 2);
        assert_eq!(live_snapshot.auto_dream_runs.len(), RUNS / 2);
        assert!(live_snapshot
            .auto_memory_runs
            .iter()
            .all(|run| run.status == MemoryMaintenanceStatus::Failed));
        assert!(live_snapshot
            .auto_dream_runs
            .iter()
            .all(|run| run.status == MemoryMaintenanceStatus::Completed));
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn persisted_started_memory_maintenance_is_interrupted_and_cursor_restored_on_resume() {
        let dir = temp_dir();
        let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(dir.to_string_lossy().into_owned()),
            archive_dir: None,
        }));
        let agent = test_agent();
        let session = agent
            .open_session(store.clone(), StartSessionOptions::default())
            .await
            .unwrap();
        let thread_id = session.id.try_to_utf8().unwrap();
        session.set_memory_message_cursor_for_test(Some("cursor-1".into()));
        assert!(
            session
                .commit_memory_maintenance_event_durable(
                    MemoryMaintenanceEvent::AutoMemory {
                        run: AutoMemoryRun {
                            run_id: "persisted-cursor-source".into(),
                            session_id: session.id.clone(),
                            trigger: MemoryMaintenanceTrigger::Auto,
                            scope: AgentMemoryScope::Project,
                            target_dir: "/tmp/memory".into(),
                            status: MemoryMaintenanceStatus::Skipped,
                            started_at: 5,
                            ended_at: Some(5),
                            reason: Some(AutoMemoryRunReason::DirectMemoryWrite),
                            error: None,
                            message_count: 1,
                            cursor_message_uuid: Some("cursor-1".into()),
                            files_touched: Vec::new(),
                            turn_count: None,
                        },
                    },
                    Some("cursor-1".into()),
                    true
                )
                .await
        );
        let memory_run = AutoMemoryRun {
            run_id: "persisted-memory-run".into(),
            session_id: session.id.clone(),
            trigger: MemoryMaintenanceTrigger::Auto,
            scope: AgentMemoryScope::Project,
            target_dir: "/tmp/memory".into(),
            status: MemoryMaintenanceStatus::Started,
            started_at: 10,
            ended_at: None,
            reason: None,
            error: None,
            message_count: 1,
            cursor_message_uuid: Some("cursor-1".into()),
            files_touched: Vec::new(),
            turn_count: None,
        };
        assert!(
            session
                .commit_memory_maintenance_event_durable(
                    MemoryMaintenanceEvent::AutoMemory { run: memory_run },
                    None,
                    true
                )
                .await
        );
        assert!(
            session
                .commit_memory_maintenance_event_durable(
                    MemoryMaintenanceEvent::AutoDream {
                        run: AutoDreamRun {
                            dream_run_id: "persisted-dream-run".into(),
                            session_id: session.id.clone(),
                            trigger: MemoryMaintenanceTrigger::Auto,
                            status: MemoryMaintenanceStatus::Started,
                            started_at: 11,
                            ended_at: None,
                            reason: None,
                            error: None,
                            last_consolidated_at: None,
                            sessions_reviewing: 3,
                            files_touched: Vec::new(),
                            turns: Vec::new(),
                            turn_count: 0,
                            summary: None,
                        },
                    },
                    None,
                    true
                )
                .await
        );
        session.shutdown_and_wait().await.unwrap();

        let resumed = agent
            .resume_session(store.clone(), &thread_id, StartSessionOptions::default())
            .await
            .unwrap();
        let snapshot = resumed
            .memory_maintenance_snapshot()
            .expect("resumed maintenance snapshot");
        assert_eq!(snapshot.auto_memory_runs.len(), 2);
        assert_eq!(snapshot.auto_dream_runs.len(), 1);
        assert_eq!(
            snapshot
                .auto_memory_runs
                .iter()
                .find(|run| run.run_id == "persisted-memory-run")
                .unwrap()
                .reason,
            Some(AutoMemoryRunReason::Interrupted)
        );
        assert_eq!(
            snapshot.auto_dream_runs[0].reason,
            Some(AutoDreamRunReason::Interrupted)
        );
        assert_eq!(resumed.memory_message_cursor(), Some("cursor-1".into()));
        resumed.shutdown_and_wait().await.unwrap();

        let resumed_again = agent
            .resume_session(store, &thread_id, StartSessionOptions::default())
            .await
            .unwrap();
        let snapshot_again = resumed_again
            .memory_maintenance_snapshot()
            .expect("second resumed maintenance snapshot");
        assert_eq!(snapshot_again, snapshot);
        assert_eq!(
            resumed_again.memory_message_cursor(),
            Some("cursor-1".into())
        );
        resumed_again.shutdown_and_wait().await.unwrap();
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn resolve_session_config_layers_session_permission_over_agent_config() {
        let agent = test_agent_with_config(Some(json!({
            "permissionMode": "default"
        })));

        let config = agent.resolve_session_config(&StartSessionOptions {
            permission_mode: Some(ExternalPermissionMode::Plan),
            ..StartSessionOptions::default()
        });

        assert_eq!(config.permission_mode, ExternalPermissionMode::Plan);
        assert_eq!(config.model, "fake-model");
    }

    #[test]
    fn explicit_canonical_session_policy_wins_over_a_deprecated_preset() {
        let agent = test_agent_with_config(Some(json!({
            "sandbox_mode": "read-only", "approval_policy": "on-request"
        })));
        let config = agent.resolve_session_config(&StartSessionOptions {
            permission_mode: Some(ExternalPermissionMode::BypassPermissions),
            ..Default::default()
        });
        assert_eq!(
            config.sandbox_mode,
            Some(crate::types::permission::SandboxMode::ReadOnly)
        );
        assert_eq!(
            config.approval_policy,
            Some(crate::types::permission::ApprovalPolicy::OnRequest)
        );
        assert_eq!(config.permission_mode, ExternalPermissionMode::Default);
    }

    #[test]
    fn resolve_session_config_layers_session_config_over_agent_config() {
        let agent = test_agent_with_config(Some(json!({
            "model": "agent-model",
            "contextWindow": 1000,
            "codeMode": false
        })));

        let config = agent.resolve_session_config(&StartSessionOptions {
            config: Some(json!({
                "model": "session-model",
                "contextWindow": 2048,
                "codeMode": true
            })),
            ..StartSessionOptions::default()
        });

        assert_eq!(config.model, "session-model");
        assert_eq!(config.context_window, 2048);
        assert!(
            config.retired_code_mode,
            "retired codeMode keys still deserialize"
        );
    }

    /// One config home, one temp dir, released with the lock: `KLAUS_CONFIG_DIR`
    /// is process-wide, so config tests cannot run against the developer's own
    /// `~/.klaus` or against each other.
    struct TestConfigHome {
        dir: std::path::PathBuf,
        _guard: std::sync::MutexGuard<'static, ()>,
    }

    impl TestConfigHome {
        fn new(tag: &str) -> Self {
            let guard = crate::test_support::CONFIG_ENV_LOCK
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let dir = std::env::temp_dir().join(format!(
                "klaus-agent-config-{tag}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir_all(&dir).unwrap();
            crate::runtime_process::set_process_environment_var("KLAUS_CONFIG_DIR", &dir);
            Self { dir, _guard: guard }
        }

        fn write(&self, body: &str) {
            std::fs::write(self.dir.join("config.toml"), body).unwrap();
        }
    }

    impl Drop for TestConfigHome {
        fn drop(&mut self) {
            crate::runtime_process::remove_process_environment_var("KLAUS_CONFIG_DIR");
            std::fs::remove_dir_all(&self.dir).ok();
        }
    }

    #[tokio::test]
    async fn page_fetch_factory_observes_user_project_and_session_source_layers() {
        let home = TestConfigHome::new("page-source");
        home.write("[webFetch]\nsource = \"workbuddy\"\n");
        let cwd = home.dir.join("repo");
        std::fs::create_dir_all(cwd.join(".klaus")).unwrap();
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = observed.clone();
        let factory: crate::tools::builtins::web_fetch::PageFetcherFactory =
            Arc::new(move |config| {
                sink.lock()
                    .unwrap()
                    .push(config.and_then(|config| config.source.clone()));
                Ok(None)
            });
        let agent = test_agent();
        for (project, session) in [
            (None, None),
            (Some("builtin"), None),
            (Some("builtin"), Some("workbuddy")),
        ] {
            if let Some(source) = project {
                std::fs::write(
                    cwd.join(".klaus/config.toml"),
                    format!("[webFetch]\nsource = {source:?}\n"),
                )
                .unwrap();
            }
            let session = agent
                .try_start_session(StartSessionOptions {
                    cwd: Some(RuntimePath::from(cwd.to_string_lossy().to_string())),
                    config: session.map(|source| json!({"webFetch":{"source":source}})),
                    web_fetch_factory: Some(factory.clone()),
                    ..Default::default()
                })
                .unwrap();
            session.shutdown_once().await;
        }
        assert_eq!(
            *observed.lock().unwrap(),
            vec![
                Some("workbuddy".to_string()),
                Some("builtin".to_string()),
                Some("workbuddy".to_string())
            ]
        );
    }

    #[tokio::test]
    async fn page_fetch_factory_rejection_is_a_typed_session_error() {
        let _home = TestConfigHome::new("page-source-error");
        let result = test_agent().try_start_session(StartSessionOptions {
            web_fetch_factory: Some(Arc::new(|_| Err("Unavailable source".into()))),
            ..Default::default()
        });
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("source must be rejected"),
        };
        assert!(matches!(error, SessionInitError::PageExtraction { .. }));
        assert_eq!(error.code(), "page_extraction_source_rejected");
        assert_eq!(
            error.into_io_error().kind(),
            std::io::ErrorKind::InvalidInput
        );
    }

    /// Retired main-conversation limits in existing config files are ignored so
    /// upgrading never blocks a session from starting.
    #[test]
    fn resolve_session_config_ignores_retired_max_turns() {
        let home = TestConfigHome::new("user");
        home.write("maxTurns = 9\ncodeMode = true\n");
        let agent = test_agent();

        let config = agent.resolve_session_config(&StartSessionOptions::default());
        assert!(
            config.retired_code_mode,
            "retired codeMode keys still deserialize"
        );

        let overridden = agent.resolve_session_config(&StartSessionOptions {
            config: Some(json!({ "maxTurns": 2 })),
            ..StartSessionOptions::default()
        });
        assert!(
            overridden.retired_code_mode,
            "an unset key keeps the stored value"
        );
    }

    /// A repo's own config outranks the user's, and product sections in either
    /// file are stored data the engine never resolves as config.
    #[test]
    fn resolve_session_config_layers_project_config_over_user_config() {
        let home = TestConfigHome::new("project");
        home.write("maxTurns = 9\nworkflow = false\n[desktop]\ntheme = \"dark\"\n");
        let cwd = home.dir.join("repo");
        std::fs::create_dir_all(cwd.join(".klaus")).unwrap();
        std::fs::write(cwd.join(".klaus/config.toml"), "maxTurns = 4\n").unwrap();
        let agent = test_agent();

        let config = agent.resolve_session_config(&StartSessionOptions {
            cwd: Some(RuntimePath::from(cwd.to_string_lossy().to_string())),
            ..StartSessionOptions::default()
        });

        assert!(
            config.workflow,
            "retired workflow=false is accepted but the effective capability stays on"
        );
    }

    #[test]
    fn resolve_session_config_keeps_windows_flags_until_all_layers_are_merged() {
        use crate::schemas::config::WindowsSandboxMode::{Elevated, Unelevated};
        for (user, project, expected) in [
            (
                "[features]\nelevated_windows_sandbox = true\n",
                "[features]\nelevated_windows_sandbox = false\n",
                None,
            ),
            (
                "[windows]\nsandbox = \"unelevated\"\n",
                "[features]\nelevated_windows_sandbox = true\n",
                Some(Unelevated),
            ),
        ] {
            let home = TestConfigHome::new("windows-flag-layers");
            home.write(user);
            let cwd = home.dir.join("repo");
            std::fs::create_dir_all(cwd.join(".klaus")).unwrap();
            let project_path = cwd.join(".klaus/config.toml");
            std::fs::write(&project_path, project).unwrap();
            let agent = test_agent();
            let cwd = RuntimePath::from(cwd.to_string_lossy().to_string());
            let config = agent.resolve_session_config(&StartSessionOptions {
                cwd: Some(cwd.clone()),
                ..Default::default()
            });
            assert_eq!(
                crate::sandbox::effective_windows_config(&config)
                    .unwrap()
                    .and_then(|windows| windows.sandbox),
                expected
            );
            let config = agent.resolve_session_config(&StartSessionOptions {
                cwd: Some(cwd),
                config: Some(json!({"features": {"elevated_windows_sandbox": true}})),
                ..Default::default()
            });
            assert_eq!(
                crate::sandbox::effective_windows_config(&config)
                    .unwrap()
                    .and_then(|windows| windows.sandbox),
                expected.or(Some(Elevated))
            );
            assert_eq!(
                std::fs::read_to_string(home.dir.join("config.toml")).unwrap(),
                user
            );
            assert_eq!(std::fs::read_to_string(project_path).unwrap(), project);
        }
    }

    #[test]
    fn resolve_session_config_merges_only_supplied_sandbox_table_fields() {
        let home = TestConfigHome::new("sandbox-table-fields");
        let user = "[sandbox_workspace_write]\nwritable_roots = [\"shared\"]\nexclude_tmpdir_env_var = true\nexclude_slash_tmp = true\n[windows]\nallow_mxc = false\n";
        home.write(user);
        let cwd = home.dir.join("repo");
        std::fs::create_dir_all(cwd.join(".klaus")).unwrap();
        let project = "[sandbox_workspace_write]\nnetwork_access = true\n[windows]\nsandbox = \"unelevated\"\n[features]\nprefer_mxc = true\nwindows_sandbox_service = true\n";
        let project_path = cwd.join(".klaus/config.toml");
        std::fs::write(&project_path, project).unwrap();
        let config = test_agent().resolve_session_config(&StartSessionOptions {
            cwd: Some(RuntimePath::from(cwd.to_string_lossy().to_string())),
            config: Some(json!({"sandbox_workspace_write": {"network_access": false}})),
            ..Default::default()
        });
        assert_eq!(
            config.sandbox_workspace_write.writable_roots,
            vec![RuntimeString::from(
                home.dir.join("shared").to_string_lossy().to_string()
            )]
        );
        assert!(!config.sandbox_workspace_write.network_access);
        assert!(config.sandbox_workspace_write.exclude_tmpdir_env_var);
        assert!(config.sandbox_workspace_write.exclude_slash_tmp);
        assert_eq!(config.windows.as_ref().unwrap().allow_mxc, Some(false));
        assert!(config.features.windows_sandbox_service);
        assert_eq!(
            crate::sandbox::effective_windows_config(&config)
                .unwrap()
                .unwrap()
                .sandbox,
            Some(crate::schemas::config::WindowsSandboxMode::Unelevated)
        );
        assert_eq!(
            std::fs::read_to_string(home.dir.join("config.toml")).unwrap(),
            user
        );
        assert_eq!(std::fs::read_to_string(project_path).unwrap(), project);
    }

    #[tokio::test]
    async fn session_provider_override_drives_default_model_and_context_window() {
        let agent = test_agent();
        let provider = Arc::new(OpenAIProvider::new(OpenAIProviderOptions {
            default_model: Some("gpt-4.1-mini".to_string()),
            api: Some(OpenAiApi::Responses),
            // Context window is now a client-declared capability, not inferred
            // from the model id.
            capabilities: Some(crate::types::provider::ModelCapabilities {
                context_window: Some(1_047_576),
                ..Default::default()
            }),
            ..Default::default()
        })) as Arc<dyn ModelProvider>;

        let session = agent.start_session(StartSessionOptions {
            provider: Some(provider),
            ..StartSessionOptions::default()
        });

        let snapshot = session.context_snapshot();
        assert_eq!(snapshot.model, "gpt-4.1-mini");
        assert_eq!(snapshot.context_window, 1_047_576);
    }

    #[test]
    fn loaded_plugins_are_registered_and_merged_into_session_capabilities() {
        let agent = test_agent_with_config_and_plugins(
            Some(json!({
                "lsp": true,
                "outputStyle": "focused"
            })),
            vec![loaded_plugin()],
        );

        let plugins = agent.plugin_snapshot();
        assert_eq!(plugins.len(), 1);
        assert_eq!(plugins[0].name, "docs");
        assert!(plugins[0].enabled);
        assert_eq!(
            plugins[0].permission_scope.lsp_servers,
            vec!["rust".to_string()]
        );

        let session = agent.start_session(StartSessionOptions::default());
        assert!(session
            .agent_definition_names()
            .iter()
            .any(|name| name == "plugin-reader"));
        assert!(session
            .visible_skill_names()
            .iter()
            .any(|name| name == "plugin-skill"));
        assert_eq!(
            session.active_output_style_name().as_ref(),
            Some(&RuntimeString::from("focused"))
        );
        assert!(session.lsp_available());
    }

    #[test]
    fn plugin_load_report_seeds_agent_registry_and_diagnostics_snapshot() {
        let agent = Agent::new(AgentOptions {
            process_context: crate::runtime_process::test_runtime_process_context(),
            provider: Arc::new(fake_provider()),
            config: None,
            tools: None,
            web_fetch_page: None,
            browser_runtime: None,
            capability_roots: Vec::new(),
            cwd: None,
            deps: EngineDepsOverrides::default(),
            compactor: None,
            approval_handler: None,
            approval_reviewer: None,
            elicit_handler: None,
            credential_store: None,
            hooks: Vec::new(),
            observer: Some(None),
            logger: None,
            pricing: None,
            agents: None,
            include_default_agents: None,
            skills: None,
            output_styles: None,
            workflows: None,
            mcp_catalog: None,
            plugin_catalog: None,
            lsp_servers: None,
            agent_memory: None,
            mcp_clients: None,
            mcp_server_configs: Vec::new(),
            mcp_tools: None,
            refresh_tools: None,
            system_prompt_provider: None,
            plugins: Vec::new(),
            plugin_load_report: Some(PluginLoadReport {
                enabled: vec![browser_plugin()],
                disabled: vec![crate::extensions::plugins::loader::PluginDisabledEntry {
                    name: Some("off".into()),
                    dir: "/plugins/off".into(),
                    reason: "disabled by manifest".into(),
                }],
                errors: vec![crate::extensions::plugins::loader::PluginLoadIssue {
                    kind: crate::extensions::plugins::loader::PluginLoadIssueKind::InvalidManifest,
                    dir: "/plugins/bad".into(),
                    message: "bad manifest".into(),
                }],
                warnings: vec![crate::extensions::plugins::loader::PluginLoadIssue {
                    kind: crate::extensions::plugins::loader::PluginLoadIssueKind::UnsupportedRuntimeCapability,
                    dir: "/plugins/warn".into(),
                    message: "unsupported runtime capability".into(),
                }],
            }),
        });

        let snapshot = agent.plugin_runtime_snapshot();
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.entries[0].name, "browser-plugin");
        assert_eq!(snapshot.disabled.len(), 1);
        assert_eq!(snapshot.errors.len(), 1);
        assert_eq!(snapshot.warnings.len(), 1);
        assert_eq!(agent.runtime_capability_descriptors().len(), 1);
    }

    /// The whole point of loading plugins in the engine rather than flattening them
    /// into session init on the host side: every contribution stays ATTRIBUTED to the
    /// plugin that shipped it. Desktop's plugin page projects the extension runtime by
    /// `source.kind == plugin` + `source.id`, so an unlabelled (or host-labelled)
    /// contribution is invisible there even though it is live in the session.
    #[test]
    fn ledger_loaded_plugin_contributions_are_attributed_to_the_plugin_in_the_runtime_snapshot() {
        let mut plugin = loaded_plugin();
        plugin.hooks = vec![Hook {
            event: HookEvent::PreToolUse,
            matcher: Some("Bash".to_string()),
            handler: HookHandler::Command {
                command: "true".to_string(),
                timeout_ms: Some(1_000),
            },
            is_async: false,
        }];

        // Arrive the way a real boot does: the ledger loader hands over a report and
        // `Agent::new` adopts its enabled plugins, rather than the caller passing
        // `plugins` directly.
        let agent = Agent::new(AgentOptions {
            plugin_load_report: Some(PluginLoadReport {
                enabled: vec![plugin],
                disabled: Vec::new(),
                errors: Vec::new(),
                warnings: Vec::new(),
            }),
            ..test_agent_options(Some(json!({ "outputStyle": "focused" })), Vec::new())
        });

        let session = agent.start_session(StartSessionOptions::default());
        let runtime = session.extension_runtime_snapshot();

        let plugin_named = |source: &ExtensionSource| {
            source.kind == crate::extensions::runtime_snapshot::ExtensionSourceKind::Plugin
                && source.id.as_ref() == Some(&RuntimeString::from("docs"))
        };
        assert!(
            runtime
                .skills
                .iter()
                .any(|entry| entry.name == "plugin-skill" && plugin_named(&entry.source)),
            "plugin skill lost its source: {:?}",
            runtime.skills
        );
        assert!(
            runtime
                .agents
                .iter()
                .any(|entry| entry.name == "plugin-reader" && plugin_named(&entry.source)),
            "plugin agent lost its source: {:?}",
            runtime.agents
        );
        assert!(
            runtime
                .output_styles
                .iter()
                .any(|entry| entry.name == "focused" && plugin_named(&entry.source)),
            "plugin output style lost its source: {:?}",
            runtime.output_styles
        );
        assert!(
            runtime
                .hooks
                .iter()
                .any(|entry| entry.event == HookEvent::PreToolUse && plugin_named(&entry.source)),
            "plugin hook lost its source: {:?}",
            runtime.hooks
        );
        // Same plugin, same name, on the plugin-runtime side of the snapshot — that is
        // the join key the page uses to fold contributions under their plugin row.
        assert!(runtime
            .plugin_runtime
            .entries
            .iter()
            .any(|entry| entry.name == "docs" && entry.enabled));
    }

    #[test]
    fn plugin_mcp_servers_are_deduped_against_manual_and_earlier_plugin_servers() {
        let plugin_a = mcp_only_plugin("plugin-a", stdio_mcp("docs_a", "docs-mcp", &["serve"]));
        let plugin_b = mcp_only_plugin("plugin-b", stdio_mcp("docs_b", "docs-mcp", &["serve"]));
        let plugin_c = mcp_only_plugin("plugin-c", stdio_mcp("manual_dup", "manual-mcp", &[]));
        let agent = Agent::new(AgentOptions {
            process_context: crate::runtime_process::test_runtime_process_context(),
            provider: Arc::new(fake_provider()),
            config: None,
            tools: None,
            web_fetch_page: None,
            browser_runtime: None,
            capability_roots: Vec::new(),
            cwd: None,
            deps: EngineDepsOverrides::default(),
            compactor: None,
            approval_handler: None,
            approval_reviewer: None,
            elicit_handler: None,
            credential_store: None,
            hooks: Vec::new(),
            observer: Some(None),
            logger: None,
            pricing: None,
            agents: None,
            include_default_agents: None,
            skills: None,
            output_styles: None,
            workflows: None,
            mcp_catalog: None,
            plugin_catalog: None,
            lsp_servers: None,
            agent_memory: None,
            mcp_clients: None,
            mcp_server_configs: vec![stdio_mcp("manual_docs", "manual-mcp", &[])],
            mcp_tools: None,
            refresh_tools: None,
            system_prompt_provider: None,
            plugins: vec![plugin_a, plugin_b, plugin_c],
            plugin_load_report: None,
        });

        let snapshot = agent.plugin_runtime_snapshot();
        let entry = |name: &str| {
            snapshot
                .entries
                .iter()
                .find(|entry| entry.name == name)
                .expect("plugin entry")
        };
        assert_eq!(
            entry("plugin-a").permission_scope.mcp_servers,
            vec!["docs_a"]
        );
        assert!(entry("plugin-b").permission_scope.mcp_servers.is_empty());
        assert!(entry("plugin-c").permission_scope.mcp_servers.is_empty());
        assert_eq!(snapshot.warnings.len(), 2);
        assert!(snapshot
            .warnings
            .iter()
            .all(|warning| warning.kind == PluginLoadIssueKind::DuplicateMcpServerSuppressed));
        assert!(snapshot
            .warnings
            .iter()
            .any(|warning| warning.message.contains("plugin-a:docs_a")));
        assert!(snapshot
            .warnings
            .iter()
            .any(|warning| warning.message.contains("manual_docs")));
    }

    #[test]
    fn host_managed_plugin_mcp_is_attributed_but_not_connected() {
        let plugin = mcp_only_plugin(
            "browser-plugin",
            McpServerConfig::HostManaged {
                name: "klaus-browser".to_string(),
                options: McpServerOptions::default(),
            },
        );
        let agent = Agent::new(AgentOptions {
            process_context: crate::runtime_process::test_runtime_process_context(),
            provider: Arc::new(fake_provider()),
            config: None,
            tools: None,
            web_fetch_page: None,
            browser_runtime: None,
            capability_roots: Vec::new(),
            cwd: None,
            deps: EngineDepsOverrides::default(),
            compactor: None,
            approval_handler: None,
            approval_reviewer: None,
            elicit_handler: None,
            credential_store: None,
            hooks: Vec::new(),
            observer: Some(None),
            logger: None,
            pricing: None,
            agents: None,
            include_default_agents: None,
            skills: None,
            output_styles: None,
            workflows: None,
            mcp_catalog: None,
            plugin_catalog: None,
            lsp_servers: None,
            agent_memory: None,
            mcp_clients: None,
            mcp_server_configs: Vec::new(),
            mcp_tools: None,
            refresh_tools: None,
            system_prompt_provider: None,
            plugins: vec![plugin],
            plugin_load_report: None,
        });

        let snapshot = agent.plugin_runtime_snapshot();
        let entry = snapshot
            .entries
            .iter()
            .find(|entry| entry.name == "browser-plugin")
            .expect("plugin entry");
        // Attribution: the plugin owns the server name for scoping and
        // contribution listing.
        assert_eq!(entry.permission_scope.mcp_servers, vec!["klaus-browser"]);
        // No suppression warning: a host-managed declaration is not a
        // duplicate of anything the engine would connect itself.
        assert!(snapshot.warnings.is_empty(), "{:?}", snapshot.warnings);
        // The engine never puts the declaration into its connect set; the
        // live endpoint arrives as a host-injected session server.
        assert!(
            !agent
                .plugin_mcp_servers
                .iter()
                .any(|server| server.name() == "klaus-browser"),
            "host-managed declarations must not enter the engine connect set"
        );
    }

    #[test]
    fn suppressed_plugin_browser_mcp_does_not_register_browser_runtime_root() {
        let mut plugin =
            mcp_only_plugin("browser-plugin", stdio_mcp("browser", "browser-mcp", &[]));
        plugin.runtime_capabilities = vec!["browser".to_string()];
        let agent = Agent::new(AgentOptions {
            process_context: crate::runtime_process::test_runtime_process_context(),
            provider: Arc::new(fake_provider()),
            config: None,
            tools: None,
            web_fetch_page: None,
            browser_runtime: None,
            capability_roots: Vec::new(),
            cwd: None,
            deps: EngineDepsOverrides::default(),
            compactor: None,
            approval_handler: None,
            approval_reviewer: None,
            elicit_handler: None,
            credential_store: None,
            hooks: Vec::new(),
            observer: Some(None),
            logger: None,
            pricing: None,
            agents: None,
            include_default_agents: None,
            skills: None,
            output_styles: None,
            workflows: None,
            mcp_catalog: None,
            plugin_catalog: None,
            lsp_servers: None,
            agent_memory: None,
            mcp_clients: None,
            mcp_server_configs: vec![stdio_mcp("manual_browser", "browser-mcp", &[])],
            mcp_tools: None,
            refresh_tools: None,
            system_prompt_provider: None,
            plugins: vec![plugin],
            plugin_load_report: None,
        });

        assert!(agent.runtime_capability_descriptors().is_empty());
        let snapshot = agent.plugin_runtime_snapshot();
        assert_eq!(snapshot.entries[0].permission_scope.mcp_servers.len(), 0);
        assert_eq!(snapshot.warnings.len(), 1);
        assert_eq!(
            snapshot.warnings[0].kind,
            PluginLoadIssueKind::DuplicateMcpServerSuppressed
        );
    }

    #[tokio::test]
    async fn plugin_capability_roots_expose_browser_tools_and_runtime_snapshot() {
        let requests: Arc<Mutex<Vec<ModelRequest>>> = Arc::new(Mutex::new(Vec::new()));
        let agent = Agent::new(AgentOptions {
            process_context: crate::runtime_process::test_runtime_process_context(),
            provider: Arc::new(fake_provider()),
            config: None,
            tools: None,
            web_fetch_page: None,
            browser_runtime: None,
            capability_roots: vec![RuntimeCapabilityRoot::plugin(
                "docs",
                Some("1.0.0"),
                "/plugins/docs",
            )
            .with_browser_runtime(spawn_free(Arc::new(NoopBrowserRuntime)))],
            cwd: None,
            deps: EngineDepsOverrides {
                call_model: Some(recording_call_model(requests.clone())),
                estimate_tokens: Some(Arc::new(|_| 0)),
                compactor: None,
                uuid: None,
                now: Some(Arc::new(|| 0)),
            },
            compactor: None,
            approval_handler: None,
            approval_reviewer: None,
            elicit_handler: None,
            credential_store: None,
            hooks: Vec::new(),
            observer: Some(None),
            logger: None,
            pricing: None,
            agents: None,
            include_default_agents: None,
            skills: None,
            output_styles: None,
            workflows: None,
            mcp_catalog: None,
            plugin_catalog: None,
            lsp_servers: None,
            agent_memory: None,
            mcp_clients: None,
            mcp_server_configs: Vec::new(),
            mcp_tools: None,
            refresh_tools: None,
            system_prompt_provider: None,
            plugins: vec![loaded_plugin()],
            plugin_load_report: None,
        });

        let plugins = agent.plugin_snapshot();
        assert_eq!(plugins.len(), 1);
        assert_eq!(plugins[0].runtime_capabilities, vec!["browser".to_string()]);

        let session = agent.start_session(StartSessionOptions::default());
        let result = session
            .run(Input::Text("hello".into()), TurnOptions::default())
            .await;
        assert_eq!(
            result.stop_reason,
            crate::types::errors::TerminalReason::Completed
        );

        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let tool_names = request_tool_names(&requests[0]);
        assert!(tool_names.iter().any(|name| name == "browser_navigate"));
        assert!(tool_names.iter().any(|name| name == "browser_snapshot"));
    }

    #[test]
    fn browser_runtime_capability_declared_by_plugin_registers_tool_backed_root() {
        let agent = test_agent_with_config_and_plugins(None, vec![browser_plugin()]);

        let descriptors = agent.runtime_capability_descriptors();
        assert_eq!(descriptors.len(), 1);
        assert_eq!(
            descriptors[0].source.kind,
            crate::capabilities::RuntimeCapabilitySourceKind::Plugin
        );
        assert_eq!(descriptors[0].source.id, "browser-plugin");

        let resolved = agent.runtime_capabilities_for_session(&StartSessionOptions {
            selected_capability_roots: Some(vec![SelectedRuntimeCapabilityRoot::plugin(
                "browser-plugin",
            )]),
            ..StartSessionOptions::default()
        });
        assert!(resolved.browser_runtime.is_none());
        assert!(resolved.browser_tool_mapping.is_some());
    }

    #[tokio::test]
    async fn plugin_browser_runtime_executes_through_plugin_mcp_tool() {
        let requests: Arc<Mutex<Vec<ModelRequest>>> = Arc::new(Mutex::new(Vec::new()));
        let mcp_calls = Arc::new(AtomicUsize::new(0));
        let agent = Agent::new(AgentOptions {
            process_context: crate::runtime_process::test_runtime_process_context(),
            provider: Arc::new(fake_provider()),
            config: None,
            tools: None,
            web_fetch_page: None,
            browser_runtime: None,
            capability_roots: Vec::new(),
            cwd: None,
            deps: EngineDepsOverrides {
                call_model: Some(browser_tool_call_model(requests.clone())),
                estimate_tokens: Some(Arc::new(|_| 0)),
                compactor: None,
                uuid: None,
                now: Some(Arc::new(|| 0)),
            },
            compactor: None,
            approval_handler: None,
            approval_reviewer: None,
            elicit_handler: None,
            credential_store: None,
            hooks: Vec::new(),
            observer: Some(None),
            logger: None,
            pricing: None,
            agents: None,
            include_default_agents: None,
            skills: None,
            output_styles: None,
            workflows: None,
            mcp_catalog: None,
            plugin_catalog: None,
            lsp_servers: None,
            agent_memory: None,
            mcp_clients: None,
            mcp_server_configs: Vec::new(),
            mcp_tools: None,
            refresh_tools: None,
            system_prompt_provider: None,
            plugins: vec![browser_plugin_with_server(Arc::new(
                BrowserRuntimeMcpServer {
                    calls: mcp_calls.clone(),
                },
            ))],
            plugin_load_report: None,
        });

        let session = agent.start_session(StartSessionOptions {
            selected_capability_roots: Some(vec![SelectedRuntimeCapabilityRoot::plugin(
                "browser-plugin",
            )]),
            ..StartSessionOptions::default()
        });
        assert!(session
            .runtime_capability_snapshot_for_turn(&TurnOptions::default())
            .capabilities
            .browser_tool_mapping
            .is_some());
        let result = session
            .run(Input::Text("open the page".into()), TurnOptions::default())
            .await;

        assert_eq!(
            result.stop_reason,
            crate::types::errors::TerminalReason::Completed
        );
        assert_eq!(mcp_calls.load(Ordering::SeqCst), 1);
        let requests = requests.lock().unwrap();
        // The plugin's browser capability no longer registers engine builtins:
        // its own MCP tool is the model-visible surface, while the mapping stays
        // adopted for internal dispatch.
        assert!(
            !request_tool_names(&requests[0])
                .iter()
                .any(|name| name.starts_with("browser_")),
            "mapping-backed plugin capability must not register engine browser builtins"
        );
        assert!(
            requests
                .last()
                .unwrap()
                .messages
                .iter()
                .any(|message| matches!(
                    message,
                    crate::schemas::message::Message::ToolResult(result)
                        if result.tool_use_id == "mcp-tu-1" && !result.is_error
                )),
            "the plugin MCP browser tool must execute without an error envelope"
        );
    }

    #[test]
    fn session_capability_roots_override_agent_plugin_browser_runtime() {
        let agent_runtime: Arc<dyn BrowserRuntime> = spawn_free(Arc::new(NoopBrowserRuntime));
        let session_runtime: Arc<dyn BrowserRuntime> = spawn_free(Arc::new(NoopBrowserRuntime));
        let agent = Agent::new(AgentOptions {
            process_context: crate::runtime_process::test_runtime_process_context(),
            provider: Arc::new(fake_provider()),
            config: None,
            tools: None,
            web_fetch_page: None,
            browser_runtime: None,
            capability_roots: vec![RuntimeCapabilityRoot::plugin(
                "docs",
                Some("1.0.0"),
                "/plugins/docs",
            )
            .with_browser_runtime(agent_runtime)],
            cwd: None,
            deps: EngineDepsOverrides::default(),
            compactor: None,
            approval_handler: None,
            approval_reviewer: None,
            elicit_handler: None,
            credential_store: None,
            hooks: Vec::new(),
            observer: Some(None),
            logger: None,
            pricing: None,
            agents: None,
            include_default_agents: None,
            skills: None,
            output_styles: None,
            workflows: None,
            mcp_catalog: None,
            plugin_catalog: None,
            lsp_servers: None,
            agent_memory: None,
            mcp_clients: None,
            mcp_server_configs: Vec::new(),
            mcp_tools: None,
            refresh_tools: None,
            system_prompt_provider: None,
            plugins: Vec::new(),
            plugin_load_report: None,
        });

        let resolved = agent.runtime_capabilities_for_session(&StartSessionOptions {
            capability_roots: Some(vec![RuntimeCapabilityRoot::host("session.browser")
                .with_browser_runtime(session_runtime.clone())]),
            ..StartSessionOptions::default()
        });

        assert!(Arc::ptr_eq(
            resolved.browser_runtime.as_ref().unwrap(),
            &session_runtime,
        ));
    }

    #[tokio::test]
    async fn loaded_plugin_commands_are_wired_into_started_sessions() {
        let requests: Arc<Mutex<Vec<ModelRequest>>> = Arc::new(Mutex::new(Vec::new()));
        let agent = Agent::new(AgentOptions {
            process_context: crate::runtime_process::test_runtime_process_context(),
            provider: Arc::new(fake_provider()),
            config: Some(json!({ "memory": { "enabled": false } })),
            tools: None,
            web_fetch_page: None,
            browser_runtime: None,
            capability_roots: Vec::new(),
            cwd: None,
            deps: EngineDepsOverrides {
                call_model: Some(recording_call_model(requests.clone())),
                estimate_tokens: Some(Arc::new(|_| 0)),
                compactor: None,
                uuid: None,
                now: Some(Arc::new(|| 0)),
            },
            compactor: None,
            approval_handler: None,
            approval_reviewer: None,
            elicit_handler: None,
            credential_store: None,
            hooks: Vec::new(),
            observer: Some(None),
            logger: None,
            pricing: None,
            agents: None,
            include_default_agents: None,
            skills: None,
            output_styles: None,
            workflows: None,
            mcp_catalog: None,
            plugin_catalog: None,
            lsp_servers: None,
            agent_memory: None,
            mcp_clients: None,
            mcp_server_configs: Vec::new(),
            mcp_tools: None,
            refresh_tools: None,
            system_prompt_provider: None,
            plugins: vec![loaded_plugin()],
            plugin_load_report: None,
        });

        let session = agent.start_session(StartSessionOptions::default());
        let result = session
            .run(
                Input::Text("/plugin-cmd deploy prod".into()),
                TurnOptions::default(),
            )
            .await;
        assert_eq!(
            result.stop_reason,
            crate::types::errors::TerminalReason::Completed
        );

        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].messages.iter().any(is_skill_catalog_context));
        let command_messages: Vec<_> = requests[0]
            .messages
            .iter()
            .filter(|message| !is_skill_catalog_context(message))
            .collect();
        assert_eq!(command_messages.len(), 1);
        let Message::User(user) = command_messages[0] else {
            panic!("expected user message");
        };
        assert_ne!(user.is_meta, Some(true));
        assert_eq!(
            user.content,
            vec![ContentBlock::Text(TextBlock {
                text: "plugin command:deploy prod".into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })]
        );
    }

    #[tokio::test]
    async fn loaded_plugin_mcp_servers_are_connected_into_started_sessions() {
        let requests: Arc<Mutex<Vec<ModelRequest>>> = Arc::new(Mutex::new(Vec::new()));
        let agent = Agent::new(AgentOptions {
            process_context: crate::runtime_process::test_runtime_process_context(),
            provider: Arc::new(fake_provider()),
            config: None,
            tools: None,
            web_fetch_page: None,
            browser_runtime: None,
            capability_roots: Vec::new(),
            cwd: None,
            deps: EngineDepsOverrides {
                call_model: Some(recording_call_model(requests.clone())),
                estimate_tokens: Some(Arc::new(|_| 0)),
                compactor: None,
                uuid: None,
                now: Some(Arc::new(|| 0)),
            },
            compactor: None,
            approval_handler: None,
            approval_reviewer: None,
            elicit_handler: None,
            credential_store: None,
            hooks: Vec::new(),
            observer: Some(None),
            logger: None,
            pricing: None,
            agents: None,
            include_default_agents: None,
            skills: None,
            output_styles: None,
            workflows: None,
            mcp_catalog: None,
            plugin_catalog: None,
            lsp_servers: None,
            agent_memory: None,
            mcp_clients: None,
            mcp_server_configs: Vec::new(),
            mcp_tools: None,
            refresh_tools: None,
            system_prompt_provider: None,
            plugins: vec![loaded_plugin()],
            plugin_load_report: None,
        });

        let session = agent.start_session(StartSessionOptions::default());
        let result = session
            .run(Input::Text("use plugin mcp".into()), TurnOptions::default())
            .await;
        assert_eq!(
            result.stop_reason,
            crate::types::errors::TerminalReason::Completed
        );

        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let names: Vec<RuntimeString> = requests[0]
            .tools
            .iter()
            .map(|spec| spec.name().clone())
            .collect();
        assert!(
            names.iter().any(|name| name == "mcp__plugin_mcp__lookup"),
            "tool specs did not include plugin MCP tool: {names:?}"
        );
    }

    #[tokio::test]
    async fn root_agent_definition_blocks_lazily_connected_mcp_tools() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let mcp_calls = Arc::new(AtomicUsize::new(0));
        let agent = test_agent_with_runtime(
            None,
            vec![],
            Some(deferred_mcp_call_model(requests.clone())),
            None,
        );
        let session = agent
            .try_start_session(StartSessionOptions {
                agent_definition_name: Some("reader".into()),
                agents: Some(vec![AgentDefinition {
                    name: "reader".into(),
                    tools: Some(vec!["read".into()]),
                    ..Default::default()
                }]),
                mcp_server_configs: Some(vec![McpServerConfig::InProcess {
                    name: "session_docs".to_string(),
                    server: Arc::new(DeferredToolMcpServer {
                        calls: mcp_calls.clone(),
                    }),
                    options: McpServerOptions {
                        always_load: Some(false),
                        ..Default::default()
                    },
                }]),
                ..Default::default()
            })
            .unwrap();
        assert!(session.plugin_mcp_clients_snapshot().is_empty());
        let result = session
            .run(
                Input::Text("look up documentation".into()),
                TurnOptions::default(),
            )
            .await;
        assert_eq!(
            result.stop_reason,
            crate::types::errors::TerminalReason::Completed
        );
        {
            let requests = requests.lock().unwrap();
            assert_eq!(requests.len(), 3);
            for request in requests.iter() {
                assert_eq!(request_tool_names(request), vec!["read"]);
            }
        }
        assert_eq!(mcp_calls.load(Ordering::SeqCst), 0);
        let connected = Session::mcp_tools_from_clients(&session.plugin_mcp_clients_snapshot());
        assert!(connected
            .iter()
            .any(|tool| tool.name() == "mcp__session_docs__lookup"));
        session.dispose().await;
    }

    #[tokio::test]
    async fn deferred_session_mcp_tools_install_tool_search() {
        let requests: Arc<Mutex<Vec<ModelRequest>>> = Arc::new(Mutex::new(Vec::new()));
        let mcp_calls = Arc::new(AtomicUsize::new(0));
        let agent = Agent::new(AgentOptions {
            process_context: crate::runtime_process::test_runtime_process_context(),
            provider: Arc::new(fake_provider()),
            config: None,
            tools: None,
            web_fetch_page: None,
            browser_runtime: None,
            capability_roots: Vec::new(),
            cwd: None,
            deps: EngineDepsOverrides {
                call_model: Some(deferred_mcp_call_model(requests.clone())),
                estimate_tokens: Some(Arc::new(|_| 0)),
                compactor: None,
                uuid: None,
                now: Some(Arc::new(|| 0)),
            },
            compactor: None,
            approval_handler: None,
            approval_reviewer: None,
            elicit_handler: None,
            credential_store: None,
            hooks: Vec::new(),
            observer: Some(None),
            logger: None,
            pricing: None,
            agents: None,
            include_default_agents: None,
            skills: None,
            output_styles: None,
            workflows: None,
            mcp_catalog: None,
            plugin_catalog: None,
            lsp_servers: None,
            agent_memory: None,
            mcp_clients: None,
            mcp_server_configs: Vec::new(),
            mcp_tools: None,
            refresh_tools: None,
            system_prompt_provider: None,
            plugins: Vec::new(),
            plugin_load_report: None,
        });
        let session = agent.start_session(StartSessionOptions {
            mcp_server_configs: Some(vec![McpServerConfig::InProcess {
                name: "session_docs".to_string(),
                server: Arc::new(DeferredToolMcpServer {
                    calls: mcp_calls.clone(),
                }),
                options: McpServerOptions {
                    always_load: Some(false),
                    ..Default::default()
                },
            }]),
            ..StartSessionOptions::default()
        });

        let result = session
            .run(
                Input::Text("look up the session documentation".into()),
                TurnOptions::default(),
            )
            .await;

        assert_eq!(
            result.stop_reason,
            crate::types::errors::TerminalReason::Completed
        );
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        let first_tools = request_tool_names(&requests[0]);
        assert!(first_tools.iter().any(|name| name == "tool_search"));
        assert!(!first_tools
            .iter()
            .any(|name| name == "mcp__session_docs__lookup"));
        let second_tools = request_tool_names(&requests[1]);
        assert!(second_tools.iter().any(|name| name == "tool_search"));
        assert!(second_tools
            .iter()
            .any(|name| name == "mcp__session_docs__lookup"));
        assert_eq!(mcp_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn session_mcp_runtime_mutations_refresh_model_surface() {
        let requests: Arc<Mutex<Vec<ModelRequest>>> = Arc::new(Mutex::new(Vec::new()));
        let agent = Agent::new(AgentOptions {
            process_context: crate::runtime_process::test_runtime_process_context(),
            provider: Arc::new(fake_provider()),
            config: None,
            tools: None,
            web_fetch_page: None,
            browser_runtime: None,
            capability_roots: Vec::new(),
            cwd: None,
            deps: EngineDepsOverrides {
                call_model: Some(recording_call_model(requests.clone())),
                estimate_tokens: Some(Arc::new(|_| 0)),
                compactor: None,
                uuid: None,
                now: Some(Arc::new(|| 0)),
            },
            compactor: None,
            approval_handler: None,
            approval_reviewer: None,
            elicit_handler: None,
            credential_store: None,
            hooks: Vec::new(),
            observer: Some(None),
            logger: None,
            pricing: None,
            agents: None,
            include_default_agents: None,
            skills: None,
            output_styles: None,
            workflows: None,
            mcp_catalog: None,
            plugin_catalog: None,
            lsp_servers: None,
            agent_memory: None,
            mcp_clients: None,
            mcp_server_configs: Vec::new(),
            mcp_tools: None,
            refresh_tools: None,
            system_prompt_provider: None,
            plugins: Vec::new(),
            plugin_load_report: None,
        });
        let session = agent.start_session(StartSessionOptions::default());

        session
            .upsert_mcp_server(McpServerConfig::InProcess {
                name: "mutable".to_string(),
                server: Arc::new(ToolListMcpServer),
                options: McpServerOptions {
                    always_load: Some(true),
                    ..Default::default()
                },
            })
            .await
            .unwrap();
        session
            .run(Input::Text("with MCP".into()), TurnOptions::default())
            .await;
        session.remove_mcp_server("mutable").await.unwrap();
        session
            .run(Input::Text("without MCP".into()), TurnOptions::default())
            .await;

        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        let tool_names = |request: &ModelRequest| {
            request
                .tools
                .iter()
                .map(|tool| tool.name().clone())
                .collect::<Vec<_>>()
        };
        assert!(tool_names(&requests[0])
            .iter()
            .any(|name| name == "mcp__mutable__lookup"));
        assert!(!tool_names(&requests[1])
            .iter()
            .any(|name| name == "mcp__mutable__lookup"));
    }

    #[tokio::test]
    async fn resume_session_repairs_unresolved_tool_uses_from_persisted_history() {
        let dir = temp_dir();
        let store = Arc::new(store_at(&dir));
        let live = store.create(meta()).await.unwrap();
        live.append_message(user("u1")).await;
        live.append_message(assistant_with_tool_use("a1", "tu-orphan"))
            .await;
        live.shutdown().await.unwrap();

        let agent = test_agent();
        let session = agent
            .resume_session(store.clone(), "sess-1", StartSessionOptions::default())
            .await
            .unwrap();

        let history = session.history();
        assert_eq!(history.len(), 1);
        assert!(matches!(history[0], Message::User(_)));

        session.dispose().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn resume_restores_cumulative_usage_without_compaction_double_count() {
        let dir = temp_dir();
        let store = Arc::new(store_at(&dir));
        let usage_one = Usage {
            input_tokens: 10_000,
            output_tokens: 1_000,
            cache_read_input_tokens: 2_000,
            cache_creation_input_tokens: 500,
            total_tokens: None,
        };
        let usage_two = Usage {
            input_tokens: 5_000,
            output_tokens: 2_000,
            cache_read_input_tokens: 1_000,
            cache_creation_input_tokens: 1_000,
            total_tokens: None,
        };
        let original = test_agent_with_runtime(
            Some(json!({ "contextWindow": 5_000_000 })),
            Vec::new(),
            Some(scripted_call_model(vec![
                vec![
                    ev::message_start(None),
                    ev::text("one"),
                    ev::stop(None, Some(usage_one)),
                ],
                vec![
                    ev::message_start(None),
                    ev::text("two"),
                    ev::stop(None, Some(usage_two)),
                ],
            ])),
            Some(HashMap::from([(
                "fake-model".to_string(),
                ModelPricing {
                    input_per_m_tok: 1.0,
                    output_per_m_tok: 2.0,
                    cache_read_per_m_tok: 0.5,
                    cache_write_per_m_tok: 1.5,
                },
            )])),
        );
        let session = original
            .open_session(store.clone(), StartSessionOptions::default())
            .await
            .unwrap();
        let thread_id = session.id.clone();
        let thread_id_host = thread_id
            .try_to_utf8()
            .expect("generated session id is a scalar host store key");
        session
            .run(Input::Text("first".into()), TurnOptions::default())
            .await;
        session
            .run(Input::Text("second".into()), TurnOptions::default())
            .await;
        let replacement_history = session.history();
        let live_cost = session.cost_snapshot();
        assert_eq!(live_cost.models[0].totals.requests, 2);
        assert!((live_cost.total_cost_usd - 0.02475).abs() < 1e-10);
        session.dispose().await;

        let reopened = store.reopen(&thread_id_host).await.unwrap();
        reopened
            .append_compaction(CompactionRecord {
                approval_parent_history: None,
                summary: "compacted".into(),
                replaced_range: (0, replacement_history.len() as u64),
                source_message_ids: None,
                replacement_history: Some(replacement_history),
            })
            .await;
        reopened
            .append_rollback(RollbackRecord { num_turns: 1 })
            .await;
        reopened.shutdown().await.unwrap();

        let resumed = test_agent_with_runtime(
            Some(json!({ "contextWindow": 5_000_000 })),
            Vec::new(),
            Some(scripted_call_model(Vec::new())),
            Some(HashMap::from([(
                "fake-model".to_string(),
                ModelPricing {
                    input_per_m_tok: 100.0,
                    output_per_m_tok: 100.0,
                    cache_read_per_m_tok: 100.0,
                    cache_write_per_m_tok: 100.0,
                },
            )])),
        )
        .resume_session(
            store.clone(),
            &thread_id_host,
            StartSessionOptions::default(),
        )
        .await
        .unwrap();

        let cost = resumed.cost_snapshot();
        assert_eq!(cost.models.len(), 1);
        assert_eq!(cost.models[0].totals.requests, 2);
        assert!((cost.total_cost_usd - 0.02475).abs() < 1e-10);
        let expected_cache_hit_rate = 3_000.0 / 19_500.0;
        assert!((cost.cache_hit_rate - expected_cache_hit_rate).abs() < 1e-10);
        assert!((cost.models[0].cache_hit_rate - expected_cache_hit_rate).abs() < 1e-10);
        let context = resumed.context_snapshot();
        assert_eq!(context.token_totals.input_tokens, 15_000);
        assert_eq!(context.token_totals.output_tokens, 3_000);
        assert_eq!(context.token_totals.cache_read_input_tokens, 3_000);
        assert_eq!(context.token_totals.cache_creation_input_tokens, 1_500);
        assert!((context.cost.total_cost_usd - cost.total_cost_usd).abs() < 1e-10);
        assert!((context.cost.cache_hit_rate - expected_cache_hit_rate).abs() < 1e-10);
        resumed.dispose().await;

        let persisted = store.resume(&thread_id_host).await.unwrap();
        let ledger = persisted
            .raw_items
            .iter()
            .enumerate()
            .filter_map(|(index, item)| match item {
                RolloutItem::UsageLedger(record) => Some((index, record)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(ledger.len(), 2);
        assert!(
            (ledger
                .iter()
                .map(|(_, record)| record.cost_usd)
                .sum::<f64>()
                - 0.02475)
                .abs()
                < 1e-10
        );
        for (ledger_index, record) in ledger {
            let message_index = persisted
                .raw_items
                .iter()
                .position(|item| {
                    matches!(
                        item,
                        RolloutItem::Message {
                            message: Message::Assistant(assistant)
                        } if assistant.uuid == record.assistant_message_id
                    )
                })
                .expect("assistant message for usage ledger");
            assert!(ledger_index < message_index);
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn legacy_usage_recovery_scans_only_original_assistant_records() {
        let dir = temp_dir();
        let store = Arc::new(store_at(&dir));
        let live = store.create(meta()).await.unwrap();
        let first = assistant_with_usage(
            "a1",
            Usage {
                input_tokens: 1_000_000,
                output_tokens: 0,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
                total_tokens: None,
            },
        );
        let second = assistant_with_usage(
            "a2",
            Usage {
                input_tokens: 0,
                output_tokens: 1_000_000,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
                total_tokens: None,
            },
        );
        let replacement_history = vec![user("u1"), first.clone(), user("u2"), second.clone()];
        for message in &replacement_history {
            live.append_message(message.clone()).await;
        }
        live.append_compaction(CompactionRecord {
            approval_parent_history: None,
            summary: "legacy compaction".into(),
            replaced_range: (0, replacement_history.len() as u64),
            source_message_ids: None,
            replacement_history: Some(replacement_history),
        })
        .await;
        live.append_rollback(RollbackRecord { num_turns: 1 }).await;
        live.shutdown().await.unwrap();

        let resumed = test_agent_with_runtime(
            None,
            Vec::new(),
            Some(scripted_call_model(Vec::new())),
            Some(HashMap::from([(
                "fake-model".to_string(),
                ModelPricing {
                    input_per_m_tok: 1.0,
                    output_per_m_tok: 2.0,
                    cache_read_per_m_tok: 0.0,
                    cache_write_per_m_tok: 0.0,
                },
            )])),
        )
        .resume_session(store.clone(), "sess-1", StartSessionOptions::default())
        .await
        .unwrap();

        let cost = resumed.cost_snapshot();
        assert_eq!(cost.models[0].totals.requests, 2);
        assert_eq!(cost.total_cost_usd, 3.0);
        assert_eq!(resumed.history().len(), 2);
        resumed.dispose().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn fork_session_before_cuts_inherited_history_at_the_requested_message() {
        let dir = temp_dir();
        let store = Arc::new(store_at(&dir));
        let agent = test_agent_with_runtime(
            None,
            Vec::new(),
            Some(scripted_call_model(vec![
                vec![
                    ev::message_start(None),
                    ev::text("first reply"),
                    ev::stop(None, None),
                ],
                vec![
                    ev::message_start(None),
                    ev::text("second reply"),
                    ev::stop(None, None),
                ],
            ])),
            None,
        );
        let session = agent
            .open_session(
                store.clone(),
                StartSessionOptions {
                    id: Some("sess-fork-cut".into()),
                    user_config_dir: Some(dir.to_str().unwrap().into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        session
            .run(Input::Text("first".into()), TurnOptions::default())
            .await;
        session
            .run(Input::Text("second".into()), TurnOptions::default())
            .await;
        let history = session.history();
        assert!(history.iter().any(is_skill_catalog_context));
        let conversation: Vec<_> = history
            .iter()
            .filter(|message| !is_skill_catalog_context(message))
            .collect();
        assert_eq!(conversation.len(), 4);
        assert!(matches!(conversation[0], Message::User(user) if user.is_meta != Some(true)));
        assert!(matches!(conversation[1], Message::Assistant(_)));
        assert!(matches!(conversation[2], Message::User(user) if user.is_meta != Some(true)));
        assert!(matches!(conversation[3], Message::Assistant(_)));
        assert_eq!(
            conversation
                .iter()
                .map(|message| text_message_content(message))
                .collect::<Vec<_>>(),
            vec!["first", "first reply", "second", "second reply"]
        );
        // Cut before the second user message: the branch keeps only turn one.
        let cut_uuid = message_uuid(conversation[2]).try_to_utf8().unwrap();
        let cut_index = history
            .iter()
            .position(|message| message_uuid(message) == cut_uuid.as_str())
            .unwrap();
        let expected_prefix = history[..cut_index].to_vec();
        session.dispose().await;

        let forked = agent
            .fork_session_before(
                store.clone(),
                "sess-fork-cut",
                Some(cut_uuid.as_str()),
                StartSessionOptions {
                    id: Some("sess-fork-cut-branch".into()),
                    user_config_dir: Some(dir.to_str().unwrap().into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let branch_history = forked.history();
        assert_eq!(branch_history, expected_prefix);
        assert!(branch_history.iter().any(is_skill_catalog_context));
        let branch_conversation: Vec<_> = branch_history
            .iter()
            .filter(|message| !is_skill_catalog_context(message))
            .collect();
        assert_eq!(branch_conversation.as_slice(), &conversation[..2]);
        forked.dispose().await;

        // An unknown cut point fails loudly instead of forking the full
        // transcript, and the source thread stays untouched either way.
        let missing = agent
            .fork_session_before(
                store.clone(),
                "sess-fork-cut",
                Some("no-such-uuid"),
                StartSessionOptions::default(),
            )
            .await;
        assert!(missing.is_err());
        let source = store.resume("sess-fork-cut").await.unwrap();
        assert_eq!(source.messages, history);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn fork_usage_recovery_excludes_inherited_history_prefix() {
        let mut fork_meta = meta();
        fork_meta.forked_from_id = Some("source-thread".into());
        let usage = Usage {
            input_tokens: 10,
            output_tokens: 2,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
            total_tokens: None,
        };
        let items = vec![
            RolloutItem::UsageLedger(crate::schemas::rollout::UsageLedgerItem {
                inference: None,
                request_id: "req-inherited".into(),
                assistant_message_id: "inherited".into(),
                model: "fake-model".into(),
                usage: usage.clone(),
                cost_usd: 7.0,
            }),
            RolloutItem::Message {
                message: assistant_with_usage("inherited", usage.clone()),
            },
            RolloutItem::TurnContext {
                ctx: crate::schemas::rollout::TurnContextSnapshot {
                    cwd: "/x".into(),
                    model: "fake-model".into(),
                    permission_mode: ExternalPermissionMode::Default,
                    turn_id: Some("fork-turn".into()),
                    started_at_ms: None,
                    approval_policy: None,
                    sandbox_mode: None,
                    approvals_reviewer: None,
                    network_allowed: None,
                    writable_roots: None,
                },
            },
            RolloutItem::UsageLedger(crate::schemas::rollout::UsageLedgerItem {
                inference: None,
                request_id: "req-fork-owned".into(),
                assistant_message_id: "fork-owned".into(),
                model: "fake-model".into(),
                usage: usage.clone(),
                cost_usd: 3.0,
            }),
            RolloutItem::Message {
                message: assistant_with_usage("fork-owned", usage),
            },
        ];

        let recovered = usage_ledger_from_rollout(&fork_meta, &items, None);
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].request_id, "req-fork-owned");
        assert_eq!(recovered[0].assistant_message_id, "fork-owned");
        assert_eq!(recovered[0].cost_usd, 3.0);
    }

    #[tokio::test]
    async fn subagent_lifecycle_survives_cold_resume() {
        let dir = temp_dir();
        let store = Arc::new(store_at(&dir));
        let original = test_agent_with_runtime(
            None,
            Vec::new(),
            Some(scripted_call_model(vec![
                task_tool_turn(),
                vec![
                    ev::message_start(None),
                    ev::text("child completed"),
                    ev::stop(None, None),
                ],
                vec![
                    ev::message_start(None),
                    ev::text("parent completed"),
                    ev::stop(None, None),
                ],
            ])),
            None,
        );
        let session = original
            .open_session(
                store.clone(),
                StartSessionOptions {
                    id: Some("sess-1".into()),
                    permission_mode: Some(ExternalPermissionMode::BypassPermissions),
                    config: Some(serde_json::json!({"allow": ["task"]})),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        session
            .run(Input::Text("delegate".into()), TurnOptions::default())
            .await;
        let completed = session.subagent_lifecycle_snapshot();
        assert_eq!(completed.agents.len(), 1);
        assert_eq!(completed.agents[0].kind, SubagentKind::Foreground);
        assert_eq!(completed.agents[0].status, SubagentStatus::Completed);
        let completed_id = completed.agents[0].agent_id.clone();
        session.dispose().await;

        let forked = original
            .fork_session(store.clone(), "sess-1", StartSessionOptions::default())
            .await
            .unwrap();
        assert!(forked.subagent_lifecycle_snapshot().agents.is_empty());
        forked.dispose().await;

        let reopened = store.reopen("sess-1").await.unwrap();
        reopened
            .append_subagent_lifecycle(lifecycle_record(
                "workflow-failed",
                SubagentKind::Workflow,
                SubagentStatus::Failed,
                200,
            ))
            .await;
        reopened
            .append_subagent_lifecycle(lifecycle_record(
                "background-running",
                SubagentKind::Background,
                SubagentStatus::Running,
                210,
            ))
            .await;
        reopened
            .append_subagent_lifecycle(lifecycle_record(
                "persistent-idle",
                SubagentKind::Persistent,
                SubagentStatus::Idle,
                220,
            ))
            .await;
        reopened.shutdown().await.unwrap();

        let resumed = test_agent_with_runtime(
            None,
            Vec::new(),
            Some(scripted_call_model(Vec::new())),
            None,
        )
        .resume_session(store.clone(), "sess-1", StartSessionOptions::default())
        .await
        .unwrap();
        let agents = resumed
            .subagent_lifecycle_snapshot()
            .agents
            .into_iter()
            .map(|record| (record.agent_id.clone(), record))
            .collect::<HashMap<_, _>>();
        assert_eq!(agents.len(), 4);
        let completed = &agents[&RuntimeString::from(&completed_id)];
        assert_eq!(completed.status, SubagentStatus::Completed);
        assert_eq!(completed.kind, SubagentKind::Foreground);
        assert_eq!(
            completed.last_result.as_ref(),
            Some(&RuntimeString::from("child completed"))
        );
        assert_eq!(
            completed.context.description,
            "complete the persisted child"
        );
        assert_eq!(completed.context.parent_agent_id, None);
        let workflow = &agents[&RuntimeString::from("workflow-failed")];
        assert_eq!(workflow.status, SubagentStatus::Failed);
        assert_eq!(workflow.kind, SubagentKind::Workflow);
        assert_eq!(
            workflow.last_error.as_ref(),
            Some(&RuntimeString::from("workflow failed"))
        );
        assert_eq!(
            workflow.context.parent_agent_id.as_ref(),
            Some(&RuntimeString::from("parent-agent"))
        );
        for agent_id in ["background-running", "persistent-idle"] {
            let recovered = &agents[&RuntimeString::from(agent_id)];
            assert_eq!(recovered.status, SubagentStatus::Interrupted);
            assert!(recovered.completed_at_ms.is_some());
            assert!(recovered.updated_at_ms >= 210);
            assert_eq!(
                recovered.last_error.as_ref(),
                Some(&RuntimeString::from(
                    crate::engine::subagent_lifecycle::RECOVERY_INTERRUPTED_REASON
                ))
            );
        }
        resumed.dispose().await;

        let persisted = store.resume("sess-1").await.unwrap();
        let interrupted = persisted
            .raw_items
            .iter()
            .filter(|item| {
                matches!(
                    item,
                    RolloutItem::SubagentLifecycle(item)
                        if item.record.status == SubagentStatus::Interrupted
                )
            })
            .count();
        assert_eq!(interrupted, 2);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn resume_session_reports_lost_in_flight_code_mode_cells() {
        let dir = temp_dir();
        let store = Arc::new(store_at(&dir));
        let live = store.create(meta()).await.unwrap();
        live.append_message(user("u1")).await;
        live.append_event(
            CODE_MODE_CELL_YIELDED_EVENT.into(),
            Some(AppendEventOptions {
                turn_id: None,
                payload: Some(exact_json!({ "cellId": "1" })),
            }),
        )
        .await;
        live.shutdown().await.unwrap();

        let agent = test_agent();
        let session = agent
            .resume_session(store.clone(), "sess-1", StartSessionOptions::default())
            .await
            .unwrap();

        let history = session.history();
        assert_eq!(history.len(), 2);
        let Message::User(meta_notice) = &history[1] else {
            panic!("expected code-mode recovery notice");
        };
        assert_eq!(meta_notice.is_meta, Some(true));
        let text = match &meta_notice.content[0] {
            ContentBlock::Text(t) => {
                assert_eq!(t.harness_envelope, Some(HarnessEnvelope::SystemReminder));
                &t.text
            }
            _ => panic!("expected text notice"),
        };
        assert!(
            text.contains("cell_id values were still in-flight: 1"),
            "{text:?}"
        );
        assert!(text.contains("Do not call wait"), "{text:?}");

        session.dispose().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn code_mode_in_flight_cells_tracks_terminal_events() {
        let items = vec![
            event(
                CODE_MODE_CELL_YIELDED_EVENT,
                Some(exact_json!({"cellId": "1"})),
            ),
            event(
                CODE_MODE_CELL_YIELDED_EVENT,
                Some(exact_json!({"cell_id": "2"})),
            ),
            event(
                CODE_MODE_CELL_COMPLETED_EVENT,
                Some(exact_json!({"cellId": "1"})),
            ),
            event("unrelated", Some(exact_json!({"cellId": "3"}))),
            event(
                CODE_MODE_CELL_TERMINATED_EVENT,
                Some(exact_json!({"cellId": "missing"})),
            ),
            event(
                CODE_MODE_CELL_YIELDED_EVENT,
                Some(exact_json!({"cellId": "3", "runtimeId": "old"})),
            ),
            event(
                CODE_MODE_CELL_COMPLETED_EVENT,
                Some(exact_json!({"cellId": "3", "runtimeId": "new"})),
            ),
        ];

        assert_eq!(code_mode_in_flight_cells(&items), vec!["2", "3"]);
    }

    #[test]
    fn parse_worktree_session_requires_the_three_path_fields() {
        assert!(parse_worktree_session(&exact_json!({"originalCwd": "/a"})).is_none());
        let wt = parse_worktree_session(&exact_json!({
            "originalCwd": "/a",
            "worktreePath": "/a/.klaus/worktrees/x",
            "worktreeName": "x",
            "hookBased": true
        }))
        .unwrap();
        assert_eq!(wt.original_cwd, RuntimePath::from("/a"));
        assert_eq!(wt.worktree_name, "x");
        assert_eq!(wt.hook_based, Some(true));
        assert!(wt.worktree_branch.is_none());
    }

    #[test]
    fn parse_workflow_journal_entry_validates_shape() {
        assert!(parse_workflow_journal_entry(Some(&exact_json!({"runId": ""}))).is_none());
        assert!(
            parse_workflow_journal_entry(Some(&exact_json!({"runId": "r", "idx": -1}))).is_none()
        );
        let e = parse_workflow_journal_entry(Some(&exact_json!({
            "runId": "r",
            "idx": 2,
            "hash": "h",
            "outputTokens": 5,
            "result": {"ok": true}
        })))
        .unwrap();
        assert_eq!(e.run_id, "r");
        assert_eq!(e.idx, 2);
        assert_eq!(e.output_tokens, 5);
        // A missing `result` defaults to null.
        let e2 = parse_workflow_journal_entry(Some(&exact_json!({
            "runId": "r", "idx": 0, "hash": "h", "outputTokens": 0
        })))
        .unwrap();
        assert_eq!(e2.result, RuntimeJson::Null);
    }

    #[test]
    fn workflow_journal_from_rollout_collects_valid_entries() {
        let items = vec![
            event("other", Some(exact_json!({"runId": "skip"}))),
            event(
                "workflow_agent",
                Some(exact_json!({"runId": "r", "idx": 0, "hash": "h", "outputTokens": 1})),
            ),
            event("workflow_agent", Some(exact_json!({"bad": true}))),
        ];
        let out = workflow_journal_from_rollout(&items);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].run_id, "r");
    }

    #[test]
    fn surfaced_tools_from_rollout_unions_names_in_order() {
        let items = vec![
            event("tools_surfaced", Some(exact_json!({"names": ["a", "b"]}))),
            event("tools_surfaced", Some(exact_json!({"names": ["b", "c"]}))),
            event("tools_surfaced", Some(exact_json!([1, 2, 3]))), // array payload → skipped
            event("tools_surfaced", None),
        ];
        assert_eq!(surfaced_tools_from_rollout(&items), vec!["a", "b", "c"]);
    }

    #[test]
    fn latest_worktree_state_takes_the_last_and_null_clears() {
        let items = vec![
            event(
                "worktree_state",
                Some(exact_json!({
                    "originalCwd": "/a", "worktreePath": "/a/wt", "worktreeName": "x"
                })),
            ),
            event("worktree_state", None),
        ];
        // The last event has a null payload → cleared → None (also fails path safety anyway).
        assert!(latest_worktree_state(&items, &RuntimePath::from("/nonexistent"), false).is_none());
    }
}
