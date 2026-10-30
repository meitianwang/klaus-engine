//! Defect: legacy planning settings could replace the selected permission preset.
//! Root cause: collaboration and permission control shared one runtime mode.
//! Work item: 20261017 independent collaboration and ordinary policy migration.

use std::sync::Arc;
use std::sync::OnceLock;

use klaus_agent_core::engine::agent::{Agent, AgentOptions, StartSessionOptions};
use klaus_agent_core::engine::deps::EngineDepsOverrides;
use klaus_agent_core::engine::session::RuntimePolicyUpdate;
use klaus_agent_core::runtime_process::{RuntimeProcessContext, RuntimeProcessSupervisor};
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::permission::ExternalPermissionMode;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStream, ProviderCapabilities, ProviderInfo,
};
use serde_json::json;

struct NoopProvider {
    info: ProviderInfo,
}

impl NoopProvider {
    fn new() -> Self {
        Self {
            info: ProviderInfo {
                name: "regression-noop".into(),
                base_url: None,
                env_key: None,
                default_model: "regression-noop".into(),
            },
        }
    }
}

impl ModelProvider for NoopProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, _model: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: false,
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: 200_000,
            max_output_tokens: 8_192,
            native_pdf: true,
            vision: true,
            image_limits: None,
            max_media_blocks: None,
        }
    }

    fn stream(&self, _req: ModelRequest, _signal: AbortSignal) -> ModelStream {
        Box::pin(futures::stream::empty())
    }
}

pub(super) fn process_supervisor() -> Arc<RuntimeProcessSupervisor> {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    static SUPERVISOR: OnceLock<Arc<RuntimeProcessSupervisor>> = OnceLock::new();
    SUPERVISOR
        .get_or_init(|| {
            let runtime = RUNTIME.get_or_init(|| {
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .unwrap()
            });
            RuntimeProcessSupervisor::initialize(runtime.handle().clone()).unwrap()
        })
        .clone()
}

pub(super) fn agent_options(process_context: RuntimeProcessContext) -> AgentOptions {
    AgentOptions {
        process_context,
        provider: Arc::new(NoopProvider::new()),
        config: Some(json!({
            "approval_policy": "never",
            "sandbox_mode": "danger-full-access"
        })),
        tools: Some(Vec::new()),
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
        plugin_load_report: None,
    }
}

#[tokio::test]
async fn legacy_planning_input_migrates_without_coupling_future_policy_changes() {
    use klaus_agent_core::planning::CollaborationMode;
    use klaus_agent_core::types::permission::{ApprovalPolicy, SandboxMode};
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut options = agent_options(process.clone());
    options.config = Some(json!({
        "sandbox_mode": "read-only", "approval_policy": "on-request"
    }));
    let agent = Agent::new(options);
    let session = agent.start_session(StartSessionOptions {
        permission_mode: Some(ExternalPermissionMode::Plan),
        ..Default::default()
    });
    assert_eq!(session.collaboration_mode(), CollaborationMode::Plan);
    assert_eq!(
        session.runtime_policy_state().sandbox_mode,
        Some(SandboxMode::ReadOnly)
    );
    session
        .set_runtime_policy(RuntimePolicyUpdate {
            approval_policy: Some(ApprovalPolicy::Never),
            sandbox_mode: Some(SandboxMode::DangerFullAccess),
            approvals_reviewer: None,
            network_allowed: Some(true),
            sandbox_workspace_write: None,
        })
        .unwrap();
    let restored = session.runtime_policy_state();
    assert_eq!(restored.sandbox_mode, Some(SandboxMode::DangerFullAccess));
    assert_eq!(restored.approval_policy, Some(ApprovalPolicy::Never));
    assert!(restored.network_allowed);
    assert_eq!(session.collaboration_mode(), CollaborationMode::Plan);
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
}
