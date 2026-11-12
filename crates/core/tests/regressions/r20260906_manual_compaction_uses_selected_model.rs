//! Defect: manual compaction used the startup model after the session selected
//! another model, including the startup model's output budget.
//! Root cause: manual compaction bypassed the effective model/config resolution
//! used by normal turns and reused the compactor's startup configuration.
//! Fixed by: 2026-09-06 context compaction model-selection work item.

use klaus_agent_core::engine::agent::{Agent, AgentOptions, StartSessionOptions};
use klaus_agent_core::engine::deps::EngineDepsOverrides;
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

    fn capabilities(&self, model: &RuntimeString) -> ProviderCapabilities {
        let switched = model == "switched-large";
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: false,
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: if switched { 128_000 } else { 32_000 },
            max_output_tokens: if switched { 12_000 } else { 8192 },
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
                    "{\"title\":\"Selected main model\"}"
                } else {
                    "## Goals\nContinue the existing task with the selected model."
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
        config: Some(serde_json::json!({"model": "initial-small"})),
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

#[tokio::test]
async fn public_session_manual_compaction_uses_selected_model_and_its_output_cap() {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let dir = std::env::temp_dir().join(format!("klaus-compact-model-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let provider = Arc::new(RecordingProvider {
        info: ProviderInfo {
            name: "compaction-model-provider".into(),
            base_url: None,
            env_key: None,
            default_model: "initial-small".into(),
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
                    content: vec![text("Recorded detailed task progress. ".repeat(500))],
                    model: "initial-small".into(),
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

    session.set_model("switched-large").unwrap();
    let outcome = session.compact(None).await;
    let requests = provider.requests.lock().unwrap().clone();
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();

    let outcome = outcome.unwrap();
    assert_eq!(
        requests.len(),
        1,
        "one summary call is sufficient; outcome={outcome:?}; requests={:?}",
        requests
            .iter()
            .map(|request| (
                request.model.clone(),
                request.max_output_tokens,
                request
                    .system
                    .iter()
                    .map(|part| String::from_utf8_lossy(&part.text.to_node_utf8())
                        .chars()
                        .take(200)
                        .collect::<String>())
                    .collect::<Vec<_>>()
            ))
            .collect::<Vec<_>>()
    );
    let request = &requests[0];
    eprintln!(
        "manual summary model={:?} max_output_tokens={}",
        request.model, request.max_output_tokens
    );
    assert_eq!(request.model, "switched-large");
    assert_eq!(request.max_output_tokens, 12_000);
    assert!(
        outcome.compacted,
        "manual compaction must complete: {outcome:?}"
    );
}

#[tokio::test]
async fn global_model_tiers_null_low_and_medium_follow_the_selected_legacy_main_model() {
    use klaus_agent_core::engine::session::InferenceTargets;
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let dir = std::env::temp_dir().join(format!(
        "klaus-global-compact-model-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let provider = Arc::new(RecordingProvider {
        info: ProviderInfo {
            name: "compaction-model-provider".into(),
            base_url: None,
            env_key: None,
            default_model: "initial-small".into(),
        },
        requests: Mutex::new(Vec::new()),
    });
    let history: Vec<Message> = (0..6)
        .flat_map(|index| {
            [
                Message::User(UserMessage {
                    question_answer: None,
                    uuid: format!("user-{index}").into(),
                    content: vec![text(format!("Task {index}"))],
                    is_meta: None,
                }),
                Message::Assistant(AssistantMessage {
                    uuid: format!("assistant-{index}").into(),
                    content: vec![text("Recorded task progress. ".repeat(1000))],
                    model: "initial-small".into(),
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
        .install_inference_targets(Some(InferenceTargets::default()), None, None)
        .unwrap();
    session.set_model("switched-large").unwrap();
    session.generate_title(None, None, false).await.unwrap();
    let title_requests = provider.requests.lock().unwrap().clone();
    assert!(!title_requests.is_empty());
    assert!(title_requests
        .iter()
        .all(|request| request.model == "switched-large" && request.max_output_tokens == 2048));
    provider.requests.lock().unwrap().clear();
    let outcome = session.compact(None).await.unwrap();
    let requests = provider.requests.lock().unwrap().clone();
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
    assert!(outcome.compacted, "{outcome:?}");
    assert!(!requests.is_empty());
    assert!(requests
        .iter()
        .all(|request| request.model == "switched-large" && request.max_output_tokens == 12000));
}
