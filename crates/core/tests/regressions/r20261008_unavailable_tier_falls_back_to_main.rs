//! Defect: an auxiliary path whose configured tier was currently unavailable
//! (signed out, model retired) failed the whole request instead of running on
//! the session's main model, and the compaction summary was pinned to a
//! separate auxiliary tier instead of riding the main model.
//! Root cause: `InferenceBundle::select` treated an `Unavailable` slot as a hard
//! error, and `summary_deps` rebound the compactor onto the medium target.
//! Fixed by: the 2026-10-08 tier-semantics work item — unavailable slots and
//! unset slots both resolve to the main target, and the summary rides the main
//! model like any other main-loop request.

use klaus_agent_core::engine::agent::{Agent, AgentOptions, StartSessionOptions};
use klaus_agent_core::engine::deps::EngineDepsOverrides;
use klaus_agent_core::engine::session::{InferenceTargetState, InferenceTargets};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::content::{ContentBlock, TextBlock};
use klaus_agent_core::schemas::message::{AssistantMessage, Message, Usage, UserMessage};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use std::sync::{Arc, Mutex};

struct RecordingProvider {
    info: ProviderInfo,
    requests: Mutex<Vec<ModelRequest>>,
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
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: 32_000,
            max_output_tokens: 12_000,
            native_pdf: true,
            vision: true,
            image_limits: None,
            max_media_blocks: None,
        }
    }

    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        let title = request.system.iter().any(|part| {
            part.text
                .contains("Generate a concise, sentence-case title")
        });
        self.requests.lock().unwrap().push(request);
        Box::pin(futures::stream::iter(vec![
            ModelStreamEvent::TextDelta {
                text: if title {
                    "{\"title\":\"Unavailable tier falls back\"}"
                } else {
                    "## Goals\nContinue the existing task on the main model."
                }
                .into(),
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

fn agent_options(
    process_context: RuntimeProcessContext,
    provider: Arc<RecordingProvider>,
    cwd: String,
) -> AgentOptions {
    AgentOptions {
        process_context,
        provider,
        config: Some(serde_json::json!({"model": "main-model"})),
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

fn text(value: impl Into<RuntimeString>) -> ContentBlock {
    ContentBlock::Text(TextBlock {
        text: value.into(),
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    })
}

fn unavailable_targets() -> InferenceTargets {
    let unavailable = |model_id: &str| {
        Some(InferenceTargetState::Unavailable {
            source: "custom".to_string(),
            model_id: model_id.to_string(),
            code: "MODEL_REMOVED".to_string(),
            message: "The selected model was deleted.".to_string(),
        })
    };
    InferenceTargets {
        high: unavailable("deleted-high"),
        medium: unavailable("deleted-medium"),
        low: unavailable("deleted-low"),
    }
}

#[tokio::test]
async fn unavailable_tier_targets_degrade_to_the_main_model_for_title_and_compaction() {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let dir = std::env::temp_dir().join(format!(
        "klaus-unavailable-tier-fallback-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let provider = Arc::new(RecordingProvider {
        info: ProviderInfo {
            name: "unavailable-tier-provider".into(),
            base_url: None,
            env_key: None,
            default_model: "main-model".into(),
        },
        requests: Mutex::new(Vec::new()),
    });
    let history = (0..12)
        .flat_map(|index| {
            [
                Message::User(UserMessage {
                    question_answer: None,
                    uuid: format!("user-{index}").into(),
                    content: vec![text(format!("Continue task step {index}."))],
                    is_meta: None,
                }),
                Message::Assistant(AssistantMessage {
                    uuid: format!("assistant-{index}").into(),
                    content: vec![text("Recorded detailed task progress. ".repeat(1000))],
                    model: "main-model".into(),
                    usage: None,
                }),
            ]
        })
        .collect();
    let agent = Agent::new(agent_options(
        process.clone(),
        provider.clone(),
        dir.to_string_lossy().into_owned(),
    ));
    let session = agent.start_session(StartSessionOptions {
        user_config_dir: Some(dir.to_string_lossy().into_owned().into()),
        initial_history: Some(history),
        ..Default::default()
    });
    session
        .install_inference_targets(Some(unavailable_targets()), None, None)
        .unwrap();

    // The low tier is configured but unavailable: the title still runs, on the
    // session's main model, instead of failing the auxiliary path.
    session.generate_title(None, None, false).await.unwrap();
    let title_requests = provider.requests.lock().unwrap().clone();
    assert!(!title_requests.is_empty(), "title must not fail");
    assert!(
        title_requests
            .iter()
            .all(|request| request.model == "main-model"),
        "an unavailable low tier degrades to the main model"
    );
    provider.requests.lock().unwrap().clear();

    // The medium tier is configured but unavailable: the compaction summary
    // rides the main model like any other main-loop request.
    let outcome = session.compact(None).await.unwrap();
    let requests = provider.requests.lock().unwrap().clone();
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();

    assert!(outcome.compacted, "{outcome:?}");
    assert!(!requests.is_empty(), "compaction must summarize");
    assert!(
        requests.iter().all(|request| request.model == "main-model"),
        "the summary rides the main model even with tier targets installed"
    );
}
