//! Per-feature auxiliary model targets: a ready slot rides its own model,
//! while an unset or unavailable slot follows the session's main model so a
//! maintenance path never fails on auxiliary configuration. Wire-level
//! acceptance (unknown slots rejected, null slots unset) is pinned by the
//! live-reconfigure server regression.

use klaus_agent_core::engine::agent::{Agent, AgentOptions, StartSessionOptions};
use klaus_agent_core::engine::deps::EngineDepsOverrides;
use klaus_agent_core::engine::session::{FeatureTargets, InferenceTarget, InferenceTargetState};
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

impl RecordingProvider {
    fn new(name: &str, default_model: &str) -> Arc<Self> {
        Arc::new(Self {
            info: ProviderInfo {
                name: name.into(),
                default_model: default_model.into(),
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
        self.requests.lock().unwrap().push(request);
        Box::pin(futures::stream::iter(vec![
            ModelStreamEvent::TextDelta {
                text: "{\"title\":\"Feature target title\"}".into(),
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

fn seeded_history() -> Vec<Message> {
    (0..2)
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
                    content: vec![text("Recorded detailed task progress.")],
                    model: "main-model".into(),
                    usage: None,
                }),
            ]
        })
        .collect()
}

struct Fixture {
    process: RuntimeProcessContext,
    dir: std::path::PathBuf,
    session: Arc<klaus_agent_core::engine::session::Session>,
    main: Arc<RecordingProvider>,
}

impl Fixture {
    fn new(label: &str) -> Self {
        let process = RuntimeProcessContext::initialize(
            super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
        )
        .unwrap();
        let dir = std::env::temp_dir().join(format!(
            "klaus-feature-model-targets-{label}-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let main = RecordingProvider::new("main-provider", "main-model");
        let agent = Agent::new(agent_options(
            process.clone(),
            main.clone(),
            dir.to_string_lossy().into_owned(),
        ));
        let session = agent.start_session(StartSessionOptions {
            user_config_dir: Some(dir.to_string_lossy().into_owned().into()),
            initial_history: Some(seeded_history()),
            ..Default::default()
        });
        Self {
            process,
            dir,
            session,
            main,
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.dir).ok();
    }
}

fn unavailable(source: &str, model_id: &str) -> InferenceTargetState {
    InferenceTargetState::Unavailable {
        source: source.to_string(),
        model_id: model_id.to_string(),
        code: "MODEL_REMOVED".to_string(),
        message: "The selected model was deleted.".to_string(),
    }
}

#[tokio::test]
async fn ready_title_feature_target_rides_own_model_and_readback_reports_slots() {
    let fixture = Fixture::new("ready");
    let feature = RecordingProvider::new("title-provider", "title-model");
    fixture
        .session
        .install_feature_targets(FeatureTargets {
            title: Some(InferenceTargetState::Ready(InferenceTarget {
                source: "custom".into(),
                model_id: "title-entry".into(),
                model: "title-model".into(),
                provider: feature.clone(),
                pricing: Vec::new(),
                review_requirements: None,
            })),
            prediction: Some(unavailable("custom", "gone-entry")),
            memory: None,
        })
        .unwrap();

    fixture
        .session
        .generate_title(None, None, false)
        .await
        .unwrap();

    let title_requests = feature.requests.lock().unwrap().clone();
    assert_eq!(
        title_requests.len(),
        1,
        "the ready slot rides its own model"
    );
    assert_eq!(title_requests[0].model, "title-model");
    assert!(
        fixture.main.requests.lock().unwrap().is_empty(),
        "the main model must not receive feature-target traffic"
    );

    let runtime = fixture.session.feature_targets_runtime();
    assert_eq!(runtime["title"]["type"], "ready", "{runtime}");
    assert_eq!(runtime["title"]["source"], "custom", "{runtime}");
    assert_eq!(runtime["title"]["modelId"], "title-entry", "{runtime}");
    assert_eq!(runtime["title"]["model"], "title-model", "{runtime}");
    assert!(
        !runtime["title"].to_string().contains("secret"),
        "{runtime}"
    );
    assert_eq!(runtime["prediction"]["type"], "unavailable", "{runtime}");
    assert_eq!(runtime["prediction"]["code"], "MODEL_REMOVED", "{runtime}");
    assert!(runtime["memory"].is_null(), "{runtime}");

    fixture.session.shutdown_and_wait().await.unwrap();
    fixture.process.shutdown().await.unwrap();
}

#[tokio::test]
async fn unset_or_unavailable_title_feature_target_follows_main_model() {
    let fixture = Fixture::new("fallback");

    // An unavailable slot degrades to the session's main model instead of
    // failing the auxiliary path.
    fixture
        .session
        .install_feature_targets(FeatureTargets {
            title: Some(unavailable("custom", "deleted-title")),
            ..Default::default()
        })
        .unwrap();
    fixture
        .session
        .generate_title(None, None, false)
        .await
        .unwrap();
    {
        let requests = fixture.main.requests.lock().unwrap().clone();
        assert_eq!(requests.len(), 1, "an unavailable slot follows main");
        assert_eq!(requests[0].model, "main-model");
    }

    // An unset slot keeps following the session's main model.
    fixture
        .session
        .install_feature_targets(FeatureTargets::default())
        .unwrap();
    fixture
        .session
        .generate_title(None, None, false)
        .await
        .unwrap();
    {
        let requests = fixture.main.requests.lock().unwrap().clone();
        assert_eq!(requests.len(), 2, "an unset slot follows main");
        assert_eq!(requests[1].model, "main-model");
    }
    let runtime = fixture.session.feature_targets_runtime();
    assert!(runtime["title"].is_null(), "{runtime}");

    fixture.session.shutdown_and_wait().await.unwrap();
    fixture.process.shutdown().await.unwrap();
}
