//! The default built-in tool set: [`default_builtins`] assembles the full,
//! engine-relevant built-in tools from a [`ResolvedConfig`] and a set of
//! host-injectable capabilities ([`BuiltinsOptions`]). This is the single
//! canonical assembler — [`crate::engine::agent::Agent`] calls it rather than
//! duplicating the list.
//!
//! ## The workflow program-execution seam
//!
//! The `workflow` tool runs a model-authored orchestration program. Execution is
//! modeled as the injected
//! [`crate::engine::workflow::runtime::WorkflowScriptRunner`] seam and driven
//! through [`crate::engine::workflow::runtime::run_workflow`]. This module wires
//! the tool to that runtime via [`EngineWorkflowRunner`], whose production
//! script runner is the engine's hybrid JavaScript / JSON-flow runner
//! ([`crate::engine::js_runtime::HybridWorkflowScriptRunner`]): ordinary
//! workflow scripts run as JavaScript in Node/V8, while native flow programs
//! remain accepted for compatibility. Every agent / budget / journal effect
//! flows through the workflow runtime unchanged. A host may still substitute
//! its own executor through the tool's [`WorkflowRunner`] seam.

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;

use crate::browser::BrowserRuntime;
use crate::engine::agent_definitions::AgentDefinition;
use crate::engine::code_mode::remote_session::{
    default_host_program, ProcessOwnedCodeModeSessionProvider,
};
use crate::engine::code_mode::runtime::InProcessCodeModeSessionProvider;
use crate::engine::code_mode::session::CodeModeSessionProvider;
use crate::engine::flow::{compose_workflow_script, is_flow_source};
use crate::engine::js_runtime::HybridWorkflowScriptRunner;
use crate::engine::workflow::runtime::{
    run_workflow, ResolveWorkflowFn as EngineResolveWorkflowFn,
    WorkflowRunInput as EngineWorkflowRunInput, WorkflowRuntimeDeps as EngineWorkflowRuntimeDeps,
    WorkflowScriptRunner,
};
use crate::extensions::plugins::catalog::{default_plugin_catalog, PluginCatalog};
use crate::extensions::skills::catalog::SkillCatalog;
use crate::extensions::skills::conditional_skills::ConditionalSkillGate;
use crate::extensions::skills::loader::Skill;
use crate::extensions::skills::skill_tool::create_skill_tool;
use crate::goal::tools::create_goal_tools;
use crate::mcp::mcp_hub::McpCatalog;
use crate::runtime_process::RuntimeProcessContext;
use crate::runtime_values::RuntimeString;
use crate::schemas::config::ResolvedConfig;
use crate::tools::apply_patch::tool::create_apply_patch_tool;
use crate::types::events::ToolDomainEvent;
use crate::types::tool::Tool;

use super::ask_user::{create_ask_user_async_tool, create_ask_user_tool};
use super::bash::{create_bash_tool_with, BashToolOptions};
use super::bash_output::create_bash_output_tool;
use super::browser::create_browser_tools;
use super::cron_tools::create_cron_tools;
use super::edit::create_edit_tool;
use super::file_search::create_file_search_tool;
use super::glob::create_glob_tool;
use super::grep::create_grep_tool;
use super::interactive_shell::{create_exec_command_tool, create_write_stdin_tool};
use super::kill_shell::create_kill_shell_tool;
use super::ls::create_ls_tool;
use super::lsp_tools::create_lsp_tools;
use super::mcp_prompts::{create_get_mcp_prompt_tool, create_list_mcp_prompts_tool};
use super::mcp_resources::{create_list_mcp_resources_tool, create_read_mcp_resource_tool};
use super::mcp_search::{create_mcp_search_tool, McpSearchToolOptions};
use super::multi_edit::create_multi_edit_tool;
use super::notebook_edit::create_notebook_edit_tool;
use super::plugin_management::{
    create_plugin_list_tool, create_plugin_manage_tool, PluginManageFn, PluginManagementToolOptions,
};
use super::plugin_search::{create_plugin_search_tool, PluginSearchToolOptions};
use super::read::create_read_tool;
use super::repl::create_repl_tool;
use super::request_mcp_install::{create_request_mcp_install_tool, RequestMcpInstallToolOptions};
use super::request_plugin_install::{
    create_request_plugin_install_tool, RequestPluginInstallToolOptions,
};
use super::request_skill_install::{
    create_request_skill_install_tool, RequestSkillInstallToolOptions,
};
use super::save_memory::create_save_memory_tool;
use super::skill_search::{create_skill_search_tool, SkillSearchToolOptions};
use super::sleep::create_sleep_tool;
use super::swarm_tools::{create_send_message_tool, create_swarm_tools};
use super::task::{create_task_tool, TaskToolOptions};
use super::task_control::{create_task_output_tool, create_task_stop_tool};
use super::task_tools::{
    create_task_create_tool, create_task_get_tool, create_task_list_tool, create_task_update_tool,
    create_task_write_tool,
};
use super::think::create_think_tool;
use super::web_fetch::{
    create_web_fetch_tool, BrowserRuntimePageFetcher, PageFetcher, WebFetchToolOptions,
};
use super::web_search::{
    create_web_search_tool, WebSearchBackend, WebSearchBackendFactory, WebSearchToolOptions,
};
use super::workflow::{
    create_workflow_tool, WorkflowMeta, WorkflowRunInput, WorkflowRunResult, WorkflowRunner,
    WorkflowRuntimeDeps, WorkflowToolOptions, WorkflowsThunk,
};
use super::worktree_tools::create_worktree_tools;
use super::write::create_write_tool;

/// A thunk that recomputes the live skills each turn.
pub type SkillsThunk = Arc<dyn Fn() -> Vec<Skill> + Send + Sync>;
/// A factory for MCP catalog backends. Each tool owns its backend, so callers
/// provide a factory rather than one shared boxed trait object.
pub type McpCatalogThunk = Arc<dyn Fn() -> Option<Box<dyn McpCatalog>> + Send + Sync>;
/// A factory for plugin catalog backends. Each tool owns its backend.
pub type PluginCatalogThunk = Arc<dyn Fn() -> Option<Box<dyn PluginCatalog>> + Send + Sync>;
/// A factory for skill catalog backends. Each tool owns its backend.
pub type SkillCatalogThunk = Arc<dyn Fn() -> Option<Box<dyn SkillCatalog>> + Send + Sync>;

/// Host-injectable capabilities for [`default_builtins`].
///
/// Skill-store catalogs are derived from the built-in default directly. MCP-store
/// catalogs can be host-injected via a factory because each catalog tool owns a
/// backend box.
#[derive(Default)]
pub struct BuiltinsOptions {
    /// Include the sub-agent `task` tool (default `true`).
    pub task: Option<bool>,
    /// Provide a web-search backend to enable the `web_search` tool; else the
    /// engine's built-in default backend.
    pub web_search: Option<WebSearchBackend>,
    /// Chooses the search backend from the session's resolved config, so a
    /// config-layer source (a named model subscription) wins over the
    /// connection-level backend above.
    pub web_search_factory: Option<WebSearchBackendFactory>,
    /// Named sub-agent roles to advertise on the `task` tool.
    pub agents: Option<Vec<AgentDefinition>>,
    /// Skill source: when provided, the `skill` tool is included and runs a named
    /// skill as a forked sub-agent.
    pub skills: Option<SkillsThunk>,
    /// Conditional-skill gate: when provided, the `skill` tool sees only visible
    /// (unconditional + activated) skills.
    pub skill_gate: Option<Arc<ConditionalSkillGate>>,
    /// Named workflows the `workflow` tool can run by name.
    pub workflows: Option<WorkflowsThunk>,
    /// Host-injected JS-rendering page fetcher for `web_fetch`.
    pub web_fetch_page: Option<Arc<dyn PageFetcher>>,
    /// Host-injected browser automation runtime. When present, expose browser_*
    /// protocol tools; the runtime itself is placed on each ToolContext by the
    /// session. Hosts that surface browser automation through an MCP tool
    /// mapping do not register these builtins; their browser surface is owned
    /// by the mapped MCP server's tools instead.
    pub browser_runtime: Option<Arc<dyn BrowserRuntime>>,
    /// MCP-server-store catalog backend factory.
    pub mcp_catalog: Option<McpCatalogThunk>,
    /// Skill-store catalog backend factory.
    pub skill_catalog: Option<SkillCatalogThunk>,
    /// Plugin-store catalog backend factory.
    pub plugin_catalog: Option<PluginCatalogThunk>,
    /// Host-injected installed-plugin ledger directory. When set, expose
    /// plugin_list/plugin_manage lifecycle tools for that directory.
    pub plugin_management_home: Option<String>,
    pub plugin_management_handler: Option<Arc<OnceLock<PluginManageFn>>>,
    /// The process context a code-mode host subprocess is launched from. Without
    /// one, `code_mode_host` cannot be honoured and cells run in this process.
    pub process_context: Option<RuntimeProcessContext>,
}

fn mcp_catalog_backend(opts: &BuiltinsOptions) -> Option<Box<dyn McpCatalog>> {
    opts.mcp_catalog.as_ref().and_then(|factory| factory())
}

fn skill_catalog_backend(opts: &BuiltinsOptions) -> Option<Box<dyn SkillCatalog>> {
    opts.skill_catalog.as_ref().and_then(|factory| factory())
}

fn plugin_catalog_backend(opts: &BuiltinsOptions) -> Option<Box<dyn PluginCatalog>> {
    opts.plugin_catalog
        .as_ref()
        .map(|factory| factory())
        .unwrap_or_else(default_plugin_catalog)
}

/// Project the engine's [`AgentDefinition`] onto the `task` tool's faithful subset.
pub(crate) fn to_task_agent(def: &AgentDefinition) -> super::task::AgentDefinition {
    super::task::AgentDefinition {
        name: def.name.clone(),
        description: def.description.clone(),
        isolation: def.isolation,
        background: def.background,
    }
}

/// The full built-in tool set (engine-relevant; no TUI/notebook tools).
pub fn default_builtins(config: &ResolvedConfig, opts: BuiltinsOptions) -> Vec<Arc<dyn Tool>> {
    // Explicit host backends override browser-backed search. The default
    // resolves the current browser capability at tool-call time. A config-layer
    // subscription source outranks the connection-level backend: the resolved
    // config is the user's later, more specific choice.
    let web_search_backend = opts
        .web_search_factory
        .as_ref()
        .and_then(|factory| factory(&config.web_search))
        .or_else(|| opts.web_search.clone());
    let web_fetch_page = opts.web_fetch_page.clone().or_else(|| {
        opts.browser_runtime.as_ref().map(|runtime| {
            Arc::new(BrowserRuntimePageFetcher::new(runtime.clone())) as Arc<dyn PageFetcher>
        })
    });

    let mut tools: Vec<Arc<dyn Tool>> = vec![
        // file
        create_read_tool(),
        create_write_tool(),
        create_save_memory_tool(),
        create_edit_tool(),
        create_multi_edit_tool(),
        create_notebook_edit_tool(),
        create_apply_patch_tool(),
        create_ls_tool(),
        // search
        create_grep_tool(),
        create_glob_tool(),
        create_file_search_tool(),
        // shell
        create_bash_tool_with(BashToolOptions {
            default_timeout_ms: Some(config.tool_timeout_ms),
            max_output_bytes: Some(config.max_aggregated_output_bytes as usize),
        }),
        create_bash_output_tool(),
        create_exec_command_tool(),
        create_write_stdin_tool(),
        create_kill_shell_tool(),
        create_sleep_tool(),
        // Web search uses browser-rendered Bing unless a backend is injected.
        create_web_fetch_tool(WebFetchToolOptions {
            max_chars: None,
            fetch_impl: None,
            max_body_bytes: None,
            fetch_page: web_fetch_page,
            allowed_domains: config
                .web_fetch
                .as_ref()
                .and_then(|w| w.allowed_domains.clone()),
            blocked_domains: config
                .web_fetch
                .as_ref()
                .and_then(|w| w.blocked_domains.clone()),
        }),
        create_web_search_tool(WebSearchToolOptions {
            search: web_search_backend,
            search_factory: opts.web_search_factory.clone(),
            config: Some(config.web_search.clone()),
            max_uses: config.web_search.max_uses,
            allowed_domains: config.web_search.allowed_domains.clone(),
            blocked_domains: config.web_search.blocked_domains.clone(),
        }),
        // planning / interaction
        create_think_tool(),
        super::advisor::create_advisor_tool(),
        create_ask_user_tool(),
        create_ask_user_async_tool(),
        // programmatic tool orchestration: JavaScript with persistent state.
        create_repl_tool(code_mode_provider(config, &opts)),
        // background-task control (degrade to "not found" with no tasks).
        create_task_output_tool(),
        create_task_stop_tool(),
        // MCP resources (degrade to "no resources" with no connected clients).
        create_list_mcp_resources_tool(),
        create_read_mcp_resource_tool(),
        // MCP prompts (model-invokable).
        create_list_mcp_prompts_tool(),
        create_get_mcp_prompt_tool(),
    ];
    if opts.browser_runtime.is_some() {
        tools.extend(create_browser_tools());
    }
    // Sub-agent delegation via the `task` tool (advertises the engine's roles).
    if opts.task != Some(false) && config.max_sub_agent_depth > 0 {
        tools.push(create_task_tool(TaskToolOptions {
            model_catalog: config.subagent_model_catalog,
            model_options: config.subagent_model_options.clone(),
            agents: opts
                .agents
                .as_deref()
                .map(|a| a.iter().map(to_task_agent).collect())
                .unwrap_or_default(),
        }));
    }
    // The `skill` tool runs a skill as a forked sub-agent; only useful when skills
    // exist. With a conditional-skill gate, only visible (unconditional +
    // activated) skills are invocable.
    if let Some(thunk) = &opts.skills {
        let thunk = thunk.clone();
        match &opts.skill_gate {
            Some(gate) => {
                let gate = gate.clone();
                tools.push(create_skill_tool(move || gate.filter(&thunk())));
            }
            None => tools.push(create_skill_tool(move || thunk())),
        }
    }
    // Skill store (read-only browse + user-confirmed install): only with a
    // host-injected catalog backend and the config switch on.
    if config.skill_catalog.enabled {
        let search_backend = skill_catalog_backend(&opts);
        let install_backend = skill_catalog_backend(&opts);
        if search_backend.is_some() && install_backend.is_some() {
            tools.push(create_skill_search_tool(SkillSearchToolOptions {
                backend: search_backend,
            }));
            tools.push(create_request_skill_install_tool(
                RequestSkillInstallToolOptions {
                    backend: install_backend,
                },
            ));
        }
    }
    // MCP server store (same shape as the skill store): only with a host-injected
    // catalog backend and the config switch on.
    if config.mcp_catalog.enabled {
        let search_backend = mcp_catalog_backend(&opts);
        let install_backend = mcp_catalog_backend(&opts);
        if search_backend.is_some() && install_backend.is_some() {
            tools.push(create_mcp_search_tool(McpSearchToolOptions {
                backend: search_backend,
            }));
            tools.push(create_request_mcp_install_tool(
                RequestMcpInstallToolOptions {
                    backend: install_backend,
                },
            ));
        }
    }
    let plugin_search_backend = plugin_catalog_backend(&opts);
    let plugin_install_backend = plugin_catalog_backend(&opts);
    if plugin_search_backend.is_some() && plugin_install_backend.is_some() {
        tools.push(create_plugin_search_tool(PluginSearchToolOptions {
            backend: plugin_search_backend,
        }));
        tools.push(create_request_plugin_install_tool(
            RequestPluginInstallToolOptions {
                backend: plugin_install_backend,
            },
        ));
    }
    if let Some(home) = opts.plugin_management_home.clone() {
        let opts = PluginManagementToolOptions {
            home,
            handler: opts.plugin_management_handler.clone(),
        };
        tools.push(create_plugin_list_tool(opts.clone()));
        tools.push(create_plugin_manage_tool(opts));
    }
    tools.push(create_task_write_tool());
    tools.push(create_task_create_tool());
    tools.push(create_task_get_tool());
    tools.push(create_task_update_tool());
    tools.push(create_task_list_tool());
    // Worktree isolation (EnterWorktree/ExitWorktree) — opt-in.
    if config.worktree {
        tools.extend(create_worktree_tools());
    }
    // Workflow is an always-available engine capability. The retired config
    // flag remains decodable for old clients but cannot disable orchestration.
    tools.push(create_workflow_tool(WorkflowToolOptions {
        workflows: opts.workflows.clone(),
        allow_inline_script: Some(true),
        runner: Arc::new(EngineWorkflowRunner::new()),
    }));
    // Swarm coordination (TeamCreate/TeamDelete/ListPeers/SendMessage) — on by
    // default via config.swarm.
    if config.swarm {
        tools.extend(create_swarm_tools());
    } else {
        tools.push(create_send_message_tool());
    }
    // Scheduled prompts (CronCreate/CronList/CronDelete) — on by default via
    // config.cron.
    if config.cron {
        tools.extend(create_cron_tools());
    }
    // Goal mode (get_goal/create_goal/update_goal) — opt-in via config.goal,
    // because an active goal keeps starting turns on its own.
    if config.goal {
        tools.extend(create_goal_tools());
    }
    // LSP code intelligence (diagnostics/hover/definition/...) — opt-in.
    if config.lsp {
        tools.extend(create_lsp_tools());
    }
    tools
}

/// Where code-mode cells run for this session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CodeModeCarrier {
    /// Cells run in this process.
    InProcess,
    /// Cells run in a host subprocess; `required` forbids degrading to
    /// in-process when that host cannot be launched.
    Host { required: bool },
}

/// Decide the carrier.
///
/// The host carrier needs a process context to launch its subprocess; without
/// one the engine cannot honour the switch, so cells stay in this process rather
/// than failing every turn.
fn code_mode_carrier(config: &ResolvedConfig, has_process_context: bool) -> CodeModeCarrier {
    if config.code_mode_host && has_process_context {
        CodeModeCarrier::Host {
            required: config.code_mode_host_required,
        }
    } else {
        CodeModeCarrier::InProcess
    }
}

fn code_mode_provider(
    config: &ResolvedConfig,
    opts: &BuiltinsOptions,
) -> Arc<dyn CodeModeSessionProvider> {
    match code_mode_carrier(config, opts.process_context.is_some()) {
        CodeModeCarrier::InProcess => Arc::new(InProcessCodeModeSessionProvider),
        CodeModeCarrier::Host { required } => {
            let process_context = opts
                .process_context
                .clone()
                .expect("the host carrier is only chosen with a process context");
            let provider = ProcessOwnedCodeModeSessionProvider::with_host_program(
                process_context,
                default_host_program(),
            );
            Arc::new(if required {
                provider.without_in_process_fallback()
            } else {
                provider
            })
        }
    }
}

/// The production `workflow` runner: bridges the tool's runtime deps to the
/// engine's [`run_workflow`], driving the injected [`WorkflowScriptRunner`]
/// seam. The production script runner is the hybrid JavaScript / JSON-flow
/// runner ([`HybridWorkflowScriptRunner`]).
struct EngineWorkflowRunner {
    script_runner: Arc<dyn WorkflowScriptRunner>,
}

impl EngineWorkflowRunner {
    fn new() -> Self {
        EngineWorkflowRunner {
            script_runner: Arc::new(HybridWorkflowScriptRunner),
        }
    }
}

/// Prepare one source for the workflow runtime: a flow program (a JSON
/// document) is composed into the runtime's script form; anything else passes
/// through untouched for the script runner to diagnose. A flow program whose
/// meta cannot compose still gets a placeholder header so the interpreter can
/// report the real, path-precise validation error.
fn prepare_workflow_source(source: RuntimeString) -> RuntimeString {
    if !is_flow_source(&source) {
        return source;
    }
    match compose_workflow_script(&source) {
        Ok(composed) => composed,
        Err(_) => RuntimeString::from(
            "export const meta = {\"name\":\"invalid\",\"description\":\"flow program pending validation\"}\n",
        )
        .concat(&source),
    }
}

#[async_trait]
impl WorkflowRunner for EngineWorkflowRunner {
    async fn run<'a>(
        &self,
        input: WorkflowRunInput,
        deps: WorkflowRuntimeDeps<'a>,
    ) -> Result<WorkflowRunResult, RuntimeString> {
        let engine_input = EngineWorkflowRunInput {
            script: prepare_workflow_source(input.script),
            args: Some(input.args),
            resume_from_run_id: input.resume_from_run_id,
            budget_tokens: input.budget_tokens,
        };
        // Workflows resolved by name (nested `workflow` steps) get the same
        // source preparation as the top-level program.
        let inner_resolve = deps.resolve_workflow.clone();
        let resolve: EngineResolveWorkflowFn =
            Arc::new(move |name: &RuntimeString| inner_resolve(name).map(prepare_workflow_source));
        // The tool's `emit` is borrowed from the live ToolContext while the
        // engine runtime needs an owned sink, so statuses (phase / log / agent
        // lifecycle) are forwarded through a channel drained alongside the run.
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let persist = deps.emit.clone();
        let emit_owned: Arc<dyn Fn(ToolDomainEvent) + Send + Sync> = Arc::new(move |ev| {
            let _ = tx.send(persist(ev));
        });
        let engine_deps = EngineWorkflowRuntimeDeps {
            process_context: deps.process_context,
            spawn_workflow_agent: deps.spawn_workflow_agent,
            journal: deps.journal,
            emit: Some(emit_owned),
            resolve_workflow: Some(resolve),
            signal: deps.signal,
            max_concurrency: deps.max_concurrency,
        };
        let run = run_workflow(engine_input, engine_deps, self.script_runner.clone());
        let forward = async {
            // Ends when the run drops its emit handle (the sender).
            while let Some(persist) = rx.recv().await {
                persist.await;
            }
        };
        let (outcome, ()) = tokio::join!(run, forward);
        match outcome {
            Ok(r) => Ok(WorkflowRunResult {
                run_id: r.run_id,
                result: r.result,
                meta: WorkflowMeta { name: r.meta.name },
                agents_spawned: r.agents_spawned,
                output_tokens_spent: r.output_tokens_spent,
            }),
            Err(e) => Err(e.message),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::browser::{
        attest_spawn_free, BrowserExecutor, BrowserRuntime, BrowserRuntimeError,
        BrowserRuntimeRequest, BrowserRuntimeResponse,
    };
    use crate::extensions::plugins::catalog::{
        PluginCatalog, PluginCatalogEntry, PluginCatalogSearchOptions,
        PluginInstallFromCatalogOptions,
    };
    use crate::mcp::mcp_hub::{
        McpCatalog, McpCatalogEntry, McpCatalogSearchOptions, McpInstallFromCatalogOptions,
    };
    use crate::mcp::types::McpServerConfig;
    use crate::runtime_values::{runtime_json, RuntimeJson, RuntimeObject};
    use crate::schemas::content::OutputBlock;
    use crate::tools::builtins::workflow::WorkflowRef;
    use crate::types::abort::AbortSignal;
    use std::sync::Mutex;
    type BoxError = Box<dyn std::error::Error + Send + Sync>;

    struct EmptyMcpCatalog;
    struct EmptyPluginCatalog;

    struct NoopBrowserRuntime;

    #[async_trait]
    unsafe impl BrowserExecutor for NoopBrowserRuntime {
        async fn execute(
            &self,
            _request: BrowserRuntimeRequest,
            _signal: AbortSignal,
            _emit: crate::browser::BrowserEventSink,
        ) -> Result<BrowserRuntimeResponse, BrowserRuntimeError> {
            Ok(BrowserRuntimeResponse::default())
        }
    }

    struct RecordingFetchBrowserRuntime {
        requests: Arc<Mutex<Vec<BrowserRuntimeRequest>>>,
    }

    #[async_trait]
    unsafe impl BrowserExecutor for RecordingFetchBrowserRuntime {
        async fn execute(
            &self,
            request: BrowserRuntimeRequest,
            _signal: AbortSignal,
            _emit: crate::browser::BrowserEventSink,
        ) -> Result<BrowserRuntimeResponse, BrowserRuntimeError> {
            self.requests.lock().unwrap().push(request);
            Ok(BrowserRuntimeResponse {
                url: Some(RuntimeString::from("https://browser.example/rendered")),
                text: Some(RuntimeString::from("rendered via default browser runtime")),
                ..BrowserRuntimeResponse::default()
            })
        }
    }

    fn spawn_free<T>(executor: Arc<T>) -> Arc<dyn BrowserRuntime>
    where
        T: BrowserExecutor + 'static,
    {
        // SAFETY: these in-memory test executors cannot create or control
        // local processes.
        unsafe { attest_spawn_free(executor) }
    }

    #[async_trait]
    impl McpCatalog for EmptyMcpCatalog {
        async fn search(
            &self,
            _query: &RuntimeString,
            _opts: McpCatalogSearchOptions,
        ) -> Result<Vec<McpCatalogEntry>, BoxError> {
            Ok(Vec::new())
        }

        async fn get(
            &self,
            _slug: &RuntimeString,
            _signal: Option<AbortSignal>,
        ) -> Result<Option<McpCatalogEntry>, BoxError> {
            Ok(None)
        }

        async fn install(
            &self,
            _slug: &RuntimeString,
            _opts: McpInstallFromCatalogOptions,
        ) -> Result<Option<McpServerConfig>, BoxError> {
            Ok(None)
        }
    }

    #[async_trait]
    impl PluginCatalog for EmptyPluginCatalog {
        async fn search(
            &self,
            _query: &RuntimeString,
            _opts: PluginCatalogSearchOptions,
        ) -> Result<Vec<PluginCatalogEntry>, BoxError> {
            Ok(Vec::new())
        }

        async fn get(
            &self,
            _slug: &RuntimeString,
            _signal: Option<AbortSignal>,
        ) -> Result<Option<PluginCatalogEntry>, BoxError> {
            Ok(None)
        }

        async fn install(
            &self,
            _slug: &RuntimeString,
            _opts: PluginInstallFromCatalogOptions,
        ) -> Result<Option<crate::extensions::plugins::loader::LoadedPlugin>, BoxError> {
            Ok(None)
        }
    }

    fn base_config() -> ResolvedConfig {
        ResolvedConfig::default()
    }

    fn names(tools: &[Arc<dyn Tool>]) -> Vec<String> {
        tools
            .iter()
            .map(|tool| {
                String::from_utf8(tool.name().to_node_utf8())
                    .expect("built-in tool names project to valid Node UTF-8")
            })
            .collect()
    }

    fn exact_json(source: &str) -> RuntimeJson {
        runtime_json::parse(&RuntimeString::from(source)).expect("valid exact JSON fixture")
    }

    fn workflow_script_input(script: RuntimeString) -> RuntimeJson {
        let mut input = RuntimeObject::new();
        input.insert("script", RuntimeJson::String(script));
        RuntimeJson::Object(input)
    }

    fn workflow_script_args_input(script: RuntimeString, args: RuntimeJson) -> RuntimeJson {
        let mut input = RuntimeObject::new();
        input.insert("script", RuntimeJson::String(script));
        input.insert("args", args);
        RuntimeJson::Object(input)
    }

    #[test]
    fn includes_the_core_file_search_shell_and_web_tools() {
        let tools = default_builtins(&base_config(), BuiltinsOptions::default());
        let n = names(&tools);
        for expected in [
            "read",
            "write",
            "edit",
            "multi_edit",
            "notebook_edit",
            "apply_patch",
            "ls",
            "grep",
            "glob",
            "file_search",
            "bash",
            "web_fetch",
            "web_search",
            "think",
            "task",
        ] {
            assert!(
                n.contains(&expected.to_string()),
                "missing {expected}: {n:?}"
            );
        }
    }

    #[test]
    fn default_config_includes_swarm_tools() {
        let n = names(&default_builtins(
            &ResolvedConfig::default(),
            BuiltinsOptions::default(),
        ));
        for expected in [
            "TeamCreate",
            "TeamDelete",
            "ListPeers",
            "SendMessage",
            "WaitForTeammates",
        ] {
            assert!(
                n.contains(&expected.to_string()),
                "missing {expected}: {n:?}"
            );
        }
    }

    #[test]
    fn browser_tools_are_exposed_only_when_a_runtime_is_injected() {
        let without_runtime = names(&default_builtins(
            &ResolvedConfig::default(),
            BuiltinsOptions::default(),
        ));
        assert!(
            !without_runtime.iter().any(|n| n.starts_with("browser_")),
            "{without_runtime:?}"
        );

        let with_runtime = names(&default_builtins(
            &ResolvedConfig::default(),
            BuiltinsOptions {
                browser_runtime: Some(spawn_free(Arc::new(NoopBrowserRuntime))),
                ..Default::default()
            },
        ));
        for expected in [
            "browser_navigate",
            "browser_snapshot",
            "browser_screenshot",
            "browser_click",
            "browser_type",
            "browser_eval",
            "browser_console",
            "browser_network",
            "browser_close",
        ] {
            assert!(
                with_runtime.contains(&expected.to_string()),
                "missing {expected}: {with_runtime:?}"
            );
        }
    }

    #[tokio::test]
    async fn default_web_fetch_uses_injected_browser_runtime_as_page_fetcher() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let tools = default_builtins(
            &ResolvedConfig::default(),
            BuiltinsOptions {
                browser_runtime: Some(spawn_free(Arc::new(RecordingFetchBrowserRuntime {
                    requests: requests.clone(),
                }))),
                ..Default::default()
            },
        );
        let web_fetch = tools
            .iter()
            .find(|t| t.name() == "web_fetch")
            .expect("web_fetch present")
            .clone();
        let ctx = crate::tools::builtins::test_ctx::make_ctx(".");

        let res = web_fetch
            .call(
                exact_json(r#"{"url":"https://browser.example/"}"#),
                &ctx,
                None,
            )
            .await
            .unwrap();
        let text = match &res.serializable.model_content[0] {
            OutputBlock::Text(t) => t.text.clone(),
            _ => panic!("expected text"),
        };

        assert!(text.contains("rendered via default browser runtime"));
        assert_eq!(
            res.serializable
                .output
                .get("finalUrl")
                .and_then(RuntimeJson::as_string)
                .cloned(),
            Some(RuntimeString::from("https://browser.example/rendered"))
        );
        assert_eq!(requests.lock().unwrap().len(), 1);
    }

    #[test]
    fn plugin_catalog_tools_are_exposed_only_when_a_catalog_is_injected() {
        let without_catalog = names(&default_builtins(
            &ResolvedConfig::default(),
            BuiltinsOptions {
                plugin_catalog: Some(Arc::new(|| None)),
                ..Default::default()
            },
        ));
        assert!(!without_catalog.contains(&"plugin_search".to_string()));
        assert!(!without_catalog.contains(&"request_plugin_install".to_string()));

        let with_catalog = names(&default_builtins(
            &ResolvedConfig::default(),
            BuiltinsOptions {
                plugin_catalog: Some(Arc::new(|| Some(Box::new(EmptyPluginCatalog)))),
                ..Default::default()
            },
        ));
        assert!(with_catalog.contains(&"plugin_search".to_string()));
        assert!(with_catalog.contains(&"request_plugin_install".to_string()));
    }

    #[test]
    fn plugin_management_tools_are_exposed_only_when_plugin_home_is_injected() {
        let without_home = names(&default_builtins(
            &ResolvedConfig::default(),
            BuiltinsOptions::default(),
        ));
        assert!(!without_home.contains(&"plugin_list".to_string()));
        assert!(!without_home.contains(&"plugin_manage".to_string()));

        let with_home = names(&default_builtins(
            &ResolvedConfig::default(),
            BuiltinsOptions {
                plugin_management_home: Some("/tmp/klaus-plugins".to_string()),
                ..Default::default()
            },
        ));
        assert!(with_home.contains(&"plugin_list".to_string()));
        assert!(with_home.contains(&"plugin_manage".to_string()));
    }

    #[test]
    fn mcp_catalog_tools_honor_config_and_host_catalog() {
        let catalog = Arc::new(|| Some(Box::new(EmptyMcpCatalog) as Box<dyn McpCatalog>));
        let mut config = ResolvedConfig::default();
        let n = names(&default_builtins(
            &config,
            BuiltinsOptions {
                mcp_catalog: Some(catalog.clone()),
                ..Default::default()
            },
        ));
        assert!(n.contains(&"mcp_search".to_string()), "{n:?}");
        assert!(n.contains(&"request_mcp_install".to_string()), "{n:?}");

        config.mcp_catalog.enabled = false;
        let n = names(&default_builtins(
            &config,
            BuiltinsOptions {
                mcp_catalog: Some(catalog),
                ..Default::default()
            },
        ));
        assert!(!n.contains(&"mcp_search".to_string()), "{n:?}");
        assert!(!n.contains(&"request_mcp_install".to_string()), "{n:?}");
    }

    #[test]
    fn task_tool_can_be_suppressed() {
        let tools = default_builtins(
            &base_config(),
            BuiltinsOptions {
                task: Some(false),
                ..Default::default()
            },
        );
        assert!(!names(&tools).contains(&"task".to_string()));
        for control in ["task_output", "task_stop", "SendMessage"] {
            assert!(names(&tools).contains(&control.to_string()));
        }
    }

    #[test]
    fn zero_nesting_budget_keeps_existing_agent_controls() {
        let mut config = base_config();
        config.max_sub_agent_depth = 0;
        let tools = default_builtins(&config, BuiltinsOptions::default());
        assert!(!names(&tools).contains(&"task".to_string()));
        for control in ["task_output", "task_stop", "SendMessage"] {
            assert!(names(&tools).contains(&control.to_string()));
        }
    }

    #[test]
    fn durable_task_tools_are_the_only_structured_task_surface() {
        let n = names(&default_builtins(
            &base_config(),
            BuiltinsOptions::default(),
        ));
        assert!(n.contains(&"TaskCreate".to_string()) && n.contains(&"TaskList".to_string()));
        assert!(!n.contains(&"todo_write".to_string()));
    }

    #[test]
    fn workflow_tool_remains_full_when_legacy_flag_is_false() {
        // Legacy false no longer disables the engine capability.
        let mut config = base_config();
        config.workflow = false;
        assert!(
            names(&default_builtins(&config, BuiltinsOptions::default()))
                .contains(&"workflow".to_string())
        );

        // config.workflow on → workflow tool present (FULL mode).
        config.workflow = true;
        assert!(
            names(&default_builtins(&config, BuiltinsOptions::default()))
                .contains(&"workflow".to_string())
        );

        // Named workflows remain advertised with the retired flag off.
        config.workflow = false;
        let workflows: WorkflowsThunk = Arc::new(|| {
            vec![WorkflowRef {
                name: RuntimeString::from("demo"),
                description: None,
                script: RuntimeString::from(
                    r#"{"meta":{"name":"demo","description":"d"},"body":[]}"#,
                ),
            }]
        });
        let tools = default_builtins(
            &config,
            BuiltinsOptions {
                workflows: Some(workflows),
                ..Default::default()
            },
        );
        assert!(names(&tools).contains(&"workflow".to_string()));
    }

    /// The wired workflow tool with an agent spawner that echoes each prompt
    /// back as the agent's final text.
    fn wired_workflow_tool() -> (Arc<dyn Tool>, crate::types::tool::ToolContext) {
        use crate::testing::make_usage;
        use crate::tools::builtins::test_ctx::make_ctx;
        use crate::types::errors::TerminalReason;
        use crate::types::events::{EngineEvent, ThreadItem};
        use crate::types::tool::{EngineEventStream, SpawnSubAgent, SpawnSubAgentOptions};

        let mut config = base_config();
        config.workflow = true;
        let tools = default_builtins(&config, BuiltinsOptions::default());
        let workflow = tools
            .iter()
            .find(|t| t.name() == "workflow")
            .expect("workflow tool present")
            .clone();

        let mut ctx = make_ctx(".");
        let spawn: SpawnSubAgent = Arc::new(|o: SpawnSubAgentOptions| {
            let events = vec![
                EngineEvent::ItemCompleted {
                    inference: None,
                    item: ThreadItem::AgentMessage {
                        phase: None,
                        id: RuntimeString::from("m"),
                        text: RuntimeString::from("echo: ").concat(&o.prompt),
                    },
                },
                EngineEvent::TurnCompleted {
                    turn_id: "w".into(),
                    usage: Some(make_usage(None, Some(3))),
                    stop_reason: TerminalReason::Completed,
                },
            ];
            Box::pin(futures::stream::iter(events)) as EngineEventStream
        });
        ctx.spawn_workflow_agent = Some(spawn);
        (workflow, ctx)
    }

    #[tokio::test]
    async fn wired_workflow_tool_runs_a_flow_program_end_to_end() {
        use serde_json::json;

        let (workflow, ctx) = wired_workflow_tool();
        let program = json!({
            "meta": { "name": "wired", "description": "d" },
            "body": [
                { "agent": { "prompt": "q ${args.topic}" }, "as": "a" },
                { "return": { "answer": { "$": "a" } } }
            ]
        });
        let res = workflow
            .call(
                workflow_script_args_input(
                    RuntimeString::from(program.to_string()),
                    exact_json(r#"{"topic":"flow"}"#),
                ),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            res.serializable.output.get("result"),
            Some(&exact_json(r#"{"answer":"echo: q flow"}"#))
        );
        assert_eq!(
            res.serializable.output.get("agentsSpawned"),
            Some(&RuntimeJson::from(1))
        );
        assert_eq!(
            res.serializable.output.get("outputTokens"),
            Some(&RuntimeJson::from(3))
        );
    }

    #[tokio::test]
    async fn wired_workflow_tool_reports_flow_validation_errors() {
        use serde_json::json;

        let (workflow, ctx) = wired_workflow_tool();
        // Bad meta: the composition step reports the flow diagnostic.
        let res = workflow
            .call(
                workflow_script_input(RuntimeString::from(
                    json!({ "meta": { "name": "x" }, "body": [] }).to_string(),
                )),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        match &res.serializable.model_content[0] {
            crate::schemas::content::OutputBlock::Text(t) => {
                assert_eq!(t.text, "meta: missing required key \"description\"")
            }
            _ => panic!("expected a text block"),
        }

        // Bad body: the interpreter reports the path-precise diagnostic.
        let res = workflow
            .call(
                workflow_script_input(RuntimeString::from(
                    json!({
                        "meta": { "name": "x", "description": "y" },
                        "body": [ { "invoke": { "tool": "read" } } ]
                    })
                    .to_string(),
                )),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        match &res.serializable.model_content[0] {
            crate::schemas::content::OutputBlock::Text(t) => assert!(
                t.text
                    .starts_with("body[0]: no step tag found among keys [invoke]"),
                "{:?}",
                t.text
            ),
            _ => panic!("expected a text block"),
        }
    }

    #[tokio::test]
    async fn wired_workflow_tool_runs_javascript_scripts() {
        let _external = crate::test_support::external_process_lock().await;
        if !crate::engine::js_runtime::node_available(
            &crate::runtime_process::test_runtime_process_context(),
        )
        .await
        {
            return;
        }
        let (workflow, ctx) = wired_workflow_tool();
        let res = workflow
            .call(workflow_script_input(RuntimeString::from(
                "export const meta = { name: 'x', description: 'y' }\nconst f = ((x) => () => x + 1)(41);\nreturn { value: f(), nan: NaN, dropped: undefined }",
            )), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            res.serializable.output["result"],
            exact_json(r#"{"value":42,"nan":null}"#)
        );
        match &res.serializable.model_content[0] {
            crate::schemas::content::OutputBlock::Text(t) => {
                assert!(t.text.contains("Workflow \"x\" finished"), "{:?}", t.text)
            }
            _ => panic!("expected a text block"),
        }
    }

    #[tokio::test]
    async fn wired_workflow_tool_forwards_phase_and_log_statuses() {
        use crate::types::events::ToolDomainEvent;
        use serde_json::json;
        use std::sync::Mutex;

        let (workflow, mut ctx) = wired_workflow_tool();
        let seen: Arc<Mutex<Vec<RuntimeString>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        ctx.emit = std::sync::Arc::new(move |ev| {
            if let ToolDomainEvent::Status { message } = ev {
                sink.lock().unwrap().push(message);
            };
            Box::pin(async {})
        });
        let program = json!({
            "meta": { "name": "wired", "description": "d" },
            "body": [ { "phase": "Research" }, { "log": "working" } ]
        });
        workflow
            .call(
                workflow_script_input(RuntimeString::from(program.to_string())),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            seen.lock().unwrap().as_slice(),
            [
                RuntimeString::from("phase: Research"),
                RuntimeString::from("working")
            ]
        );
    }

    #[tokio::test]
    async fn wired_workflow_tool_resolves_registered_flow_workflows_by_name() {
        use serde_json::json;

        let child = json!({
            "meta": { "name": "child", "description": "c" },
            "body": [ { "return": "from-child-${args.n}" } ]
        });
        let mut config = base_config();
        config.workflow = true;
        let child_src = child.to_string();
        let workflows: WorkflowsThunk = Arc::new(move || {
            vec![WorkflowRef {
                name: RuntimeString::from("child"),
                description: None,
                script: RuntimeString::from(child_src.clone()),
            }]
        });
        let tools = default_builtins(
            &config,
            BuiltinsOptions {
                workflows: Some(workflows),
                ..Default::default()
            },
        );
        let workflow = tools
            .iter()
            .find(|t| t.name() == "workflow")
            .expect("workflow tool present")
            .clone();
        let mut ctx = crate::tools::builtins::test_ctx::make_ctx(".");
        let spawn: crate::types::tool::SpawnSubAgent =
            Arc::new(|_o: crate::types::tool::SpawnSubAgentOptions| {
                Box::pin(futures::stream::empty::<crate::types::events::EngineEvent>())
                    as crate::types::tool::EngineEventStream
            });
        ctx.spawn_workflow_agent = Some(spawn);

        // Run the registered flow workflow by name.
        let res = workflow
            .call(exact_json(r#"{"name":"child","args":{"n":1}}"#), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            res.serializable.output.get("result"),
            Some(&RuntimeJson::from("from-child-1"))
        );

        // And nested from an inline parent program.
        let parent = json!({
            "meta": { "name": "parent", "description": "d" },
            "body": [
                { "workflow": { "name": "child", "args": { "n": 2 } }, "as": "c" },
                { "return": { "$": "c" } }
            ]
        });
        let res = workflow
            .call(
                workflow_script_input(RuntimeString::from(parent.to_string())),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            res.serializable.output.get("result"),
            Some(&RuntimeJson::from("from-child-2"))
        );
    }

    #[test]
    fn the_code_mode_carrier_follows_the_host_switch_and_its_prerequisites() {
        let mut config = base_config();

        assert_eq!(
            code_mode_carrier(&config, /*has_process_context*/ true),
            CodeModeCarrier::InProcess,
            "without the host switch cells stay in this process"
        );

        // The switch alone is not enough: launching a host needs a process
        // context, so the engine keeps serving in-process rather than failing.
        config.code_mode_host = true;
        assert_eq!(
            code_mode_carrier(&config, /*has_process_context*/ false),
            CodeModeCarrier::InProcess,
            "a host carrier without a process context cannot be honoured"
        );
        assert_eq!(
            code_mode_carrier(&config, /*has_process_context*/ true),
            CodeModeCarrier::Host { required: false },
            "the switch plus a process context selects the host carrier"
        );

        // Requiring isolation forbids the in-process fallback.
        config.code_mode_host_required = true;
        assert_eq!(
            code_mode_carrier(&config, /*has_process_context*/ true),
            CodeModeCarrier::Host { required: true }
        );
        // Requiring isolation cannot conjure a context: the switch is still
        // unserviceable without one.
        assert_eq!(
            code_mode_carrier(&config, /*has_process_context*/ false),
            CodeModeCarrier::InProcess
        );
    }

    #[test]
    fn swarm_and_lsp_gated_on_config() {
        let mut config = base_config();
        config.swarm = false;
        config.lsp = false;
        let n = names(&default_builtins(&config, BuiltinsOptions::default()));
        assert!(n.iter().any(|t| t == "SendMessage"));
        for team_tool in ["TeamCreate", "TeamDelete", "ListPeers", "WaitForTeammates"] {
            assert!(!n.iter().any(|name| name == team_tool), "{team_tool}");
        }
        assert!(!n.iter().any(|t| t.starts_with("lsp_")));
        config.swarm = true;
        config.lsp = true;
        let n = names(&default_builtins(&config, BuiltinsOptions::default()));
        assert!(n.iter().any(|t| t == "SendMessage"));
        assert!(n.iter().any(|t| t.starts_with("lsp_")));
    }

    #[test]
    fn default_config_includes_cron_tools() {
        let n = names(&default_builtins(
            &ResolvedConfig::default(),
            BuiltinsOptions::default(),
        ));
        for expected in ["CronCreate", "CronList", "CronDelete"] {
            assert!(
                n.contains(&expected.to_string()),
                "missing {expected}: {n:?}"
            );
        }
    }

    #[test]
    fn cron_tools_gated_on_config() {
        let mut config = base_config();
        config.cron = false;
        let n = names(&default_builtins(&config, BuiltinsOptions::default()));
        assert!(!n.iter().any(|t| t.starts_with("Cron")), "{n:?}");
        config.cron = true;
        let n = names(&default_builtins(&config, BuiltinsOptions::default()));
        for expected in ["CronCreate", "CronList", "CronDelete"] {
            assert!(
                n.contains(&expected.to_string()),
                "missing {expected}: {n:?}"
            );
        }
    }
}
