//! Defect: a failed proactive summary could fall through to ordinary inference.
//! Root cause: a failed checkpoint was treated as an optional maintenance result.
//! Work item: 20260905 stop the unfinished turn when required compaction fails.

use klaus_agent_core::engine::agent::{Agent, AgentOptions, StartSessionOptions};
use klaus_agent_core::engine::deps::{Compactor, EngineDepsOverrides};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::content::{ContentBlock, TextBlock};
use klaus_agent_core::schemas::message::{
    AssistantMessage, CompactBoundaryMessage, Message, Usage, UserMessage,
};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{ProviderError, ProviderErrorKind, TerminalReason};
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

fn text(value: impl Into<RuntimeString>) -> ContentBlock {
    ContentBlock::Text(TextBlock {
        text: value.into(),
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    })
}

fn usage() -> Usage {
    Usage {
        input_tokens: 19_500,
        output_tokens: 500,
        cache_read_input_tokens: 0,
        cache_creation_input_tokens: 0,
        total_tokens: None,
    }
}

fn history() -> Vec<Message> {
    std::iter::once(Message::CompactBoundary(CompactBoundaryMessage {
        native_compaction: None,
        uuid: "history-boundary".into(),
        summary: "Earlier task context.".into(),
    }))
    .chain((0..20).flat_map(|index| {
        [
            Message::User(UserMessage {
                question_answer: None,
                uuid: format!("history-user-{index}").into(),
                content: vec![text("preserve this task")],
                is_meta: None,
            }),
            Message::Assistant(AssistantMessage {
                uuid: format!("history-assistant-{index}").into(),
                content: vec![text("x".repeat(8_000))],
                model: "context-regression".into(),
                usage: Some(usage()),
            }),
        ]
    }))
    .collect()
}

fn response(value: impl Into<RuntimeString>) -> ModelStream {
    Box::pin(futures::stream::iter(vec![
        ModelStreamEvent::MessageStart {
            model: "context-regression".into(),
        },
        ModelStreamEvent::TextDelta { text: value.into() },
        ModelStreamEvent::MessageStop {
            stop_reason: ModelStopReason::EndTurn,
            usage: usage(),
        },
    ]))
}

struct RecoveryProvider {
    info: ProviderInfo,
    api_calls: AtomicUsize,
    summary_calls: AtomicUsize,
}

impl RecoveryProvider {
    fn new() -> Self {
        Self {
            info: ProviderInfo {
                name: "context-regression".into(),
                default_model: "context-regression".into(),
                base_url: None,
                env_key: None,
            },
            api_calls: AtomicUsize::new(0),
            summary_calls: AtomicUsize::new(0),
        }
    }
}

impl ModelProvider for RecoveryProvider {
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
            context_window: 40_000,
            max_output_tokens: 1_000,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }

    fn stream(&self, request: ModelRequest, _signal: AbortSignal) -> ModelStream {
        let summary_prompt = request
            .messages
            .last()
            .is_some_and(|message| match message {
                Message::User(user) => user.content.iter().any(|block| {
                    matches!(
                        block,
                        ContentBlock::Text(text) if text.text == "Create the fixture checkpoint."
                    )
                }),
                _ => false,
            });
        if request.system.iter().any(|part| part.name == "compact") || summary_prompt {
            self.summary_calls.fetch_add(1, Ordering::SeqCst);
            return Box::pin(futures::stream::iter([ModelStreamEvent::Error {
                error: ProviderError {
                    kind: ProviderErrorKind::Network,
                    message: "Controlled summary failure".into(),
                    status: Some(503),
                    code: None,
                    error_type: None,
                    media_origins: None,
                    media_repair_complete: false,
                    credential_recovery: None,
                    retry_after_ms: None,
                    request_id: None,
                },
            }]));
        }
        let call = self.api_calls.fetch_add(1, Ordering::SeqCst);
        response(format!("progress-{call}"))
    }
}

fn agent_options(
    process_context: RuntimeProcessContext,
    provider: Arc<dyn ModelProvider>,
    compactor: Option<Arc<dyn Compactor>>,
) -> AgentOptions {
    AgentOptions {
        process_context,
        provider,
        config: Some(json!({
            "permissionMode": "bypassPermissions",
            "contextWindow": 40_000,
            "autoCompactWindow": 20_000,
            "compactPrompt": "Create the fixture checkpoint.",
            "maxOutputTokens": 1_000,
            "outputReserveTokens": 0,
            "compactBufferTokens": 1_000,
            "compactThreshold": null,
            "earlyMicrocompact": false,
            "idleMicrocompact": false,
            "speculativeCompaction": false,
            "compactionSelfCheck": false,
            "loopDetection": false,
            "sandbox": "process",
            "sandboxNetworkAllowed": true,
            "requireRealSandbox": false,
            "sandboxRetryOnDenial": false
        })),
        tools: Some(Vec::new()),
        web_fetch_page: None,
        browser_runtime: None,
        capability_roots: Vec::new(),
        cwd: None,
        deps: EngineDepsOverrides::default(),
        compactor,
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

async fn run_session(
    provider: Arc<RecoveryProvider>,
    compactor: Option<Arc<dyn Compactor>>,
    token_budget: u64,
) -> klaus_agent_core::engine::session::TurnResult {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .expect("process context");
    let root =
        std::env::temp_dir().join(format!("klaus-context-regression-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).expect("test directory");
    let agent = Agent::new(agent_options(process.clone(), provider, compactor));
    let session = agent.start_session(StartSessionOptions {
        cwd: Some(root.to_string_lossy().into_owned().into()),
        user_config_dir: Some(root.to_string_lossy().into_owned().into()),
        initial_history: Some(history()),
        base_instructions: Some("Complete the preserved task.".into()),
        ..StartSessionOptions::default()
    });
    let result = session
        .run(
            Input::Text("continue".into()),
            TurnOptions {
                token_budget: Some(token_budget),
                ..TurnOptions::default()
            },
        )
        .await;
    session.shutdown_and_wait().await.expect("session shutdown");
    process.shutdown().await.expect("process shutdown");
    std::fs::remove_dir_all(root).expect("test cleanup");
    result
}

#[tokio::test]
async fn a_failed_proactive_summary_never_falls_through_to_normal_inference() {
    let provider = Arc::new(RecoveryProvider::new());
    let result = run_session(provider.clone(), None, 2_000).await;
    assert_eq!(result.stop_reason, TerminalReason::Error);
    let error = result
        .error
        .as_ref()
        .expect("failed compaction is reported");
    assert!(
        error.message.contains("Controlled summary failure"),
        "{error:?}"
    );
    let summary_calls = provider.summary_calls.load(Ordering::SeqCst);
    assert!((1..=6).contains(&summary_calls), "{summary_calls}");
    assert_eq!(provider.api_calls.load(Ordering::SeqCst), 0);
}
