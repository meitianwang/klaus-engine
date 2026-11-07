//! Title generation must be one fixed-shape request, not a session clone.
//!
//! What broke: sessions in repositories with substantial project context
//! stopped producing titles while context-poor chats kept working. The title
//! query reused the read-only query path wholesale, so it carried the whole
//! model history, the session's project system prompt and memory index, and
//! the session's reasoning selection (effort plus thinking mode) into a
//! request capped at 256 output tokens. On a thinking model the reasoning
//! spend consumed that cap before any title text arrived.
//!
//! Root cause: the title request inherited session context and session
//! reasoning instead of running the fixed title contract, so its behaviour
//! depended on which model the title slot pointed at.
//!
//! Fixed by 1531fbc4: title queries run without history, without the session
//! system prompt, without the session's reasoning selection, and without an
//! artificial tiny output cap. Pinned here so the contract cannot regress.

use klaus_agent_core::engine::agent::{Agent, AgentOptions, StartSessionOptions};
use klaus_agent_core::engine::deps::EngineDepsOverrides;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::content::{ContentBlock, TextBlock};
use klaus_agent_core::schemas::message::{AssistantMessage, Message, Usage, UserMessage};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo, SystemPromptPart,
};
use std::sync::{Arc, Mutex};

const PROJECT_MARKER: &str = "PROJECT-INSTRUCTIONS-MARKER";
const HISTORY_MARKER: &str = "HISTORY-MARKER";
const TASK_PROMPT: &str = "Refactor the sidebar project ordering";

struct RecordingProvider {
    info: ProviderInfo,
    requests: Mutex<Vec<ModelRequest>>,
}

impl RecordingProvider {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            info: ProviderInfo {
                name: "title-provider".into(),
                default_model: "main-model".into(),
                base_url: None,
                env_key: None,
            },
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
            // A thinking model is the case that exposed the defect.
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
            content: vec![text(format!("{HISTORY_MARKER} continue the task"))],
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

fn message_text(message: &Message) -> String {
    let content = match message {
        Message::User(message) => &message.content,
        Message::Assistant(message) => &message.content,
        _ => return String::new(),
    };
    content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text(block) => Some(block.text.to_string()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[tokio::test]
async fn title_query_carries_only_its_prompt_and_model_default_reasoning() {
    let dir = std::env::temp_dir().join(format!(
        "klaus-title-query-isolation-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&dir).unwrap();

    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let provider = RecordingProvider::new();
    let agent = Agent::new(AgentOptions {
        process_context: process,
        provider: provider.clone(),
        config: Some(serde_json::json!({ "model": "main-model" })),
        tools: Some(Vec::new()),
        web_fetch_page: None,
        browser_runtime: None,
        capability_roots: Vec::new(),
        cwd: Some(dir.to_string_lossy().into_owned()),
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
    let root = dir.to_string_lossy().into_owned();
    let session = agent.start_session(StartSessionOptions {
        cwd: Some(root.clone().into()),
        user_config_dir: Some(root.into()),
        initial_history: Some(history()),
        system_prompt_provider: Some(Arc::new(|| {
            Box::pin(async {
                vec![SystemPromptPart {
                    name: "project".into(),
                    text: PROJECT_MARKER.into(),
                    cacheable: None,
                    cache_scope: None,
                }]
            })
        })),
        config: Some(serde_json::json!({
            "model": "main-model",
            "effort": "high",
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
    assert_eq!(requests.len(), 1, "one title request");
    let request = &requests[0];

    assert!(
        request
            .system
            .iter()
            .all(|part| !part.text.to_string().contains(PROJECT_MARKER)),
        "the title request must not carry the session's project system prompt"
    );
    assert_eq!(
        request.messages.len(),
        1,
        "the title request carries the task prompt only, not the session history"
    );
    let prompt = message_text(&request.messages[0]);
    assert!(
        prompt.contains(TASK_PROMPT),
        "the host task prompt reaches the title model"
    );
    assert!(
        !prompt.contains(HISTORY_MARKER),
        "session history must not reach the title model"
    );
    assert!(
        request.effort.is_none(),
        "the title request uses the model default effort, not the session's"
    );
    assert!(
        request.thinking.is_none(),
        "the title request does not inherit the session's thinking mode"
    );
    assert_ne!(
        request.max_output_tokens, 256,
        "an output cap that small is spent by reasoning before any title text"
    );

    std::fs::remove_dir_all(&dir).ok();
}
