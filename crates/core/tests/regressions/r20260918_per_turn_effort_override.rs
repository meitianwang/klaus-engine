//! Defect: a host delivering work into another conversation (cross-thread
//! `send_message_to_thread`) could not set the reasoning effort for just that
//! turn — `TurnOptions` carried `model` but not `effort`, so effort always came
//! from the session's own inference config. The only ways to change effort were
//! `session/reconfigureModel` (which persists and re-tunes the conversation for
//! all its later turns) or a global default.
//! Root cause: the per-turn override path existed for `model`
//! (`effective_turn_model`) but had no `effort` twin, and `session_run.rs` read
//! `inference_config().effort` directly.
//! Fixed by: adding `TurnOptions::effort`, `effective_turn_effort`, the
//! `RunOpOptions.effort` wire field, and reading the per-turn value in
//! `session_run.rs`. This pins the observable contract: a per-turn effort wins
//! over the session config, an absent one falls back to it, and neither writes
//! back to the session.

use klaus_agent_core::engine::agent::{Agent, AgentOptions, StartSessionOptions};
use klaus_agent_core::engine::deps::EngineDepsOverrides;
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::effort::{EffortLevel, EffortValue};
use klaus_agent_core::schemas::message::Usage;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use serde_json::json;
use std::sync::{Arc, Mutex};

struct RecordingProvider {
    info: ProviderInfo,
    requests: Mutex<Vec<ModelRequest>>,
}

impl ModelProvider for RecordingProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: false,
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: 32_000,
            max_output_tokens: 8192,
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
                text: "Done.".into(),
            },
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: Usage {
                    input_tokens: 10,
                    output_tokens: 1,
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
    session_effort: Option<&str>,
) -> AgentOptions {
    let mut config = json!({"model": "test-model"});
    if let Some(effort) = session_effort {
        config["effort"] = json!(effort);
    }
    AgentOptions {
        process_context,
        provider,
        config: Some(config),
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

struct Harness {
    process: RuntimeProcessContext,
    provider: Arc<RecordingProvider>,
    session: Arc<klaus_agent_core::engine::session::Session>,
    dir: std::path::PathBuf,
}

async fn harness(session_effort: Option<&str>) -> Harness {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let dir = std::env::temp_dir().join(format!("klaus-turn-effort-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let provider = Arc::new(RecordingProvider {
        info: ProviderInfo {
            name: "turn-effort-provider".into(),
            base_url: None,
            env_key: None,
            default_model: "test-model".into(),
        },
        requests: Mutex::new(Vec::new()),
    });
    let agent = Agent::new(agent_options(
        process.clone(),
        provider.clone(),
        dir.to_string_lossy().into_owned(),
        session_effort,
    ));
    let session = agent.start_session(StartSessionOptions {
        cwd: Some(dir.to_string_lossy().into_owned().into()),
        user_config_dir: Some(dir.to_string_lossy().into_owned().into()),
        ..Default::default()
    });
    Harness {
        process,
        provider,
        session,
        dir,
    }
}

impl Harness {
    /// Effort on the most recent provider request, or a panic if none was made.
    /// Keeping the two failures apart matters: "no request at all" is a broken
    /// harness, while `None` is a legitimate absence of effort.
    fn last_effort(&self) -> Option<EffortValue> {
        let requests = self.provider.requests.lock().unwrap();
        let request = requests
            .last()
            .expect("the turn must have produced a provider request");
        request.effort.clone()
    }

    async fn teardown(self) {
        self.session.shutdown_and_wait().await.unwrap();
        self.process.shutdown().await.unwrap();
        std::fs::remove_dir_all(&self.dir).unwrap();
    }
}

#[tokio::test]
async fn a_per_turn_effort_override_reaches_the_provider_without_touching_the_session() {
    let h = harness(Some("low")).await;

    // A turn with no override uses the session's own configured effort.
    let baseline = h
        .session
        .run(Input::Text("baseline".into()), TurnOptions::default())
        .await;
    assert!(baseline.error.is_none(), "{:?}", baseline.error);
    assert_eq!(
        h.last_effort(),
        Some(EffortValue::Level(EffortLevel::Low)),
        "an ordinary turn reads the session config"
    );

    // A turn carrying an explicit effort override wins for THIS turn.
    let overridden = h
        .session
        .run(
            Input::Text("override".into()),
            TurnOptions {
                effort: Some(EffortValue::Level(EffortLevel::Max)),
                ..Default::default()
            },
        )
        .await;
    assert!(overridden.error.is_none(), "{:?}", overridden.error);
    assert_eq!(
        h.last_effort(),
        Some(EffortValue::Level(EffortLevel::Max)),
        "the per-turn override must reach the provider request"
    );

    // The override is per-turn, not persisted: the next ordinary turn is back to
    // the session config. This is what makes it safe to deliver into someone
    // else's conversation.
    let after = h
        .session
        .run(Input::Text("after".into()), TurnOptions::default())
        .await;
    assert!(after.error.is_none(), "{:?}", after.error);
    assert_eq!(
        h.last_effort(),
        Some(EffortValue::Level(EffortLevel::Low)),
        "the override must not leak into later turns"
    );

    h.teardown().await;
}

#[tokio::test]
async fn an_absent_per_turn_effort_falls_back_to_the_session_config_including_none() {
    // No session effort configured and no override: the request carries None
    // rather than a fabricated default.
    let h = harness(None).await;
    let result = h
        .session
        .run(Input::Text("hi".into()), TurnOptions::default())
        .await;
    assert!(result.error.is_none(), "{:?}", result.error);
    assert_eq!(
        h.last_effort(),
        None,
        "absent on both sides must stay absent"
    );

    // An override still applies when the session configured nothing.
    let overridden = h
        .session
        .run(
            Input::Text("hi again".into()),
            TurnOptions {
                effort: Some(EffortValue::Number(42)),
                ..Default::default()
            },
        )
        .await;
    assert!(overridden.error.is_none(), "{:?}", overridden.error);
    assert_eq!(
        h.last_effort(),
        Some(EffortValue::Number(42)),
        "a numeric override reaches the provider unchanged"
    );

    h.teardown().await;
}
