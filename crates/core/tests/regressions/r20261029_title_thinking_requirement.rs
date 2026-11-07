//! Title requests must satisfy the selected model's thinking contract.
//!
//! What broke: title generation always tried to turn thinking off. Models that
//! reject the disabled shape (e.g. K3.1 Pro) returned 400, so the conversation
//! stayed untitled even though ordinary turns worked.
//!
//! Root cause: the title query path set thinking to `None` unconditionally,
//! which downstream providers translated to `thinking:{"type":"disabled"}`.
//!
//! Fixed by: choosing the title thinking config from the model's declared
//! `canDisableThinking` capability — disable when allowed, otherwise fall back
//! to the lowest available reasoning rung. Fixing change: 2026-10-29 working
//! tree change; the file name uses the fix date until a commit id is assigned.

use klaus_agent_core::engine::agent::{Agent, AgentOptions, StartSessionOptions};
use klaus_agent_core::engine::deps::EngineDepsOverrides;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::content::{ContentBlock, TextBlock};
use klaus_agent_core::schemas::message::{AssistantMessage, Message, Usage, UserMessage};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    ModelCapabilities, ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo, ThinkingCapability, ThinkingConfig,
};
use std::sync::{Arc, Mutex};

const TASK_PROMPT: &str = "Refactor the sidebar project ordering";

struct RecordingProvider {
    info: ProviderInfo,
    can_disable_thinking: bool,
    requests: Mutex<Vec<ModelRequest>>,
}

impl RecordingProvider {
    fn new(can_disable_thinking: bool) -> Arc<Self> {
        Arc::new(Self {
            info: ProviderInfo {
                name: "title-provider".into(),
                default_model: "main-model".into(),
                base_url: None,
                env_key: None,
            },
            can_disable_thinking,
            requests: Mutex::new(Vec::new()),
        })
    }
}

impl ModelProvider for RecordingProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, _model: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: false,
            thinking: true,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: 200_000,
            max_output_tokens: 12_000,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }

    fn model_metadata(&self, _model: &RuntimeString) -> ModelCapabilities {
        ModelCapabilities {
            thinking: Some(ThinkingCapability {
                adaptive: true,
                interleaved: false,
                can_disable_thinking: self.can_disable_thinking,
            }),
            ..Default::default()
        }
    }

    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        self.requests.lock().unwrap().push(request);
        Box::pin(futures::stream::iter(vec![
            ModelStreamEvent::TextDelta {
                text: "{\"title\":\"Refactor project ordering\"}".into(),
            },
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: Usage {
                    input_tokens: 100,
                    output_tokens: 20,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    total_tokens: None,
                },
            },
        ]))
    }
}

fn text(value: impl Into<RuntimeString>) -> ContentBlock {
    ContentBlock::Text(TextBlock {
        text: value.into(),
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    })
}

fn history() -> Vec<Message> {
    vec![
        Message::User(UserMessage {
            question_answer: None,
            uuid: "user-1".into(),
            content: vec![text("previous user message")],
            is_meta: None,
        }),
        Message::Assistant(AssistantMessage {
            uuid: "assistant-1".into(),
            content: vec![text("Recorded detailed task progress.")],
            model: "main-model".into(),
            usage: None,
        }),
    ]
}

fn agent_options(
    process_context: RuntimeProcessContext,
    provider: Arc<RecordingProvider>,
    cwd: String,
) -> AgentOptions {
    AgentOptions {
        process_context,
        provider,
        config: Some(serde_json::json!({ "model": "main-model" })),
        tools: Some(Vec::new()),
        web_fetch_page: None,
        browser_runtime: None,
        capability_roots: Vec::new(),
        cwd: Some(cwd),
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

async fn run_title_generation(can_disable: bool) -> (String, Vec<ModelRequest>) {
    let dir = std::env::temp_dir().join(format!(
        "klaus-title-thinking-requirement-{can_disable}-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&dir).unwrap();

    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let provider = RecordingProvider::new(can_disable);
    let agent = Agent::new(agent_options(
        process,
        provider.clone(),
        dir.to_string_lossy().into_owned(),
    ));
    let root = dir.to_string_lossy().into_owned();
    let session = agent.start_session(StartSessionOptions {
        cwd: Some(root.clone().into()),
        user_config_dir: Some(root.into()),
        initial_history: Some(history()),
        config: Some(serde_json::json!({
            "model": "main-model",
            "thinking": true,
            "maxOutputTokens": 12_000,
            "contextWindow": 200_000,
        })),
        ..Default::default()
    });

    let result = session
        .generate_title(None, Some(TASK_PROMPT.to_string()), false)
        .await
        .expect("title generation runs");
    assert!(result.title.is_some(), "the model returns a title");

    let requests = provider.requests.lock().unwrap().clone();
    std::fs::remove_dir_all(&dir).ok();
    (dir.to_string_lossy().into_owned(), requests)
}

#[tokio::test]
async fn title_disables_thinking_when_model_allows_it() {
    let (_dir, requests) = run_title_generation(true).await;
    assert_eq!(requests.len(), 1, "one title request");
    assert_eq!(
        requests[0].thinking,
        Some(ThinkingConfig::Disabled),
        "a model that can disable thinking must receive the disabled shape"
    );
}

#[tokio::test]
async fn title_keeps_lowest_thinking_rung_when_model_rejects_disabled() {
    let (_dir, requests) = run_title_generation(false).await;
    assert_eq!(requests.len(), 1, "one title request");
    assert_eq!(
        requests[0].thinking,
        Some(ThinkingConfig::Adaptive),
        "a model that cannot disable thinking must receive the lowest rung"
    );
}
