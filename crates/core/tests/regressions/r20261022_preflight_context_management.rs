//! Defect: full-window turns bypassed proactive compaction and resent rejected input.
//! Root cause: overflow recovery was local to one query and failed summaries fell through.
//! Work item 20261022: persist the active window and gate every normal continuation on compaction.

use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{ContextWindowState, Input, Session, TurnOptions};
use klaus_agent_core::hooks::types::{
    Hook, HookEvent, HookFn, HookHandler, HookInput, HookOutcome,
};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{RuntimeJson, RuntimeObject, RuntimeString};
use klaus_agent_core::schemas::content::{ContentBlock, OutputBlock, TextBlock};
use klaus_agent_core::schemas::message::{AssistantMessage, Message, Usage, UserMessage};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{
    ProviderError, ProviderErrorKind, TerminalReason, ToolError,
};
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::json::{Json, JsonSchema};
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::tool::{
    Tool, ToolContext, ToolProgress, ToolResult, ToolResultSerializable,
};
use serde_json::json;
use std::sync::{Arc, Mutex};

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

fn text(value: impl Into<RuntimeString>) -> TextBlock {
    TextBlock {
        text: value.into(),
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    }
}

fn error(kind: ProviderErrorKind) -> ModelStreamEvent {
    ModelStreamEvent::Error {
        error: ProviderError {
            kind,
            message: "Controlled provider rejection".into(),
            status: Some(400),
            retry_after_ms: None,
            request_id: Some("fixture-request".into()),
            code: None,
            error_type: None,
            media_origins: None,
            media_repair_complete: false,
            credential_recovery: None,
        },
    }
}

fn stop(reason: ModelStopReason, tokens: u64) -> ModelStreamEvent {
    ModelStreamEvent::MessageStop {
        stop_reason: reason,
        usage: Usage {
            input_tokens: tokens,
            output_tokens: 1,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
            total_tokens: None,
        },
    }
}

#[derive(Default)]
struct State {
    trace: Vec<&'static str>,
    requests: Vec<ModelRequest>,
    reject_normal: bool,
    normal_input_tokens: Option<u64>,
    tool_step: bool,
    summary_failures: usize,
    summary_pending: bool,
    hook_pending: bool,
    hook_started: Arc<tokio::sync::Notify>,
    fallback_small: bool,
}

struct Provider {
    info: ProviderInfo,
    state: Arc<Mutex<State>>,
}

impl ModelProvider for Provider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }
    fn capabilities(&self, model: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: true,
            tools: true,
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: if model == "small-fixture" {
                10_000
            } else {
                100_000
            },
            max_output_tokens: 1_000,
            vision: true,
            native_pdf: true,
            image_limits: None,
            max_media_blocks: None,
            supports_image_detail_original: false,
        }
    }
    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        let mut state = self.state.lock().unwrap();
        let summary = request.skip_cache_write == Some(true);
        state.trace.push(if summary { "summary" } else { "normal" });
        state.requests.push(request);
        if summary {
            if state.summary_pending {
                return Box::pin(
                    futures::stream::iter([ModelStreamEvent::TextDelta {
                        text: "INCOMPLETE_SUMMARY".into(),
                    }])
                    .chain(futures::stream::pending()),
                );
            }
            if state.summary_failures > 0 {
                state.summary_failures -= 1;
                return Box::pin(futures::stream::iter([error(ProviderErrorKind::Network)]));
            }
            return Box::pin(futures::stream::iter([ModelStreamEvent::TextDelta {
                text: "Task constraints: keep the original evidence and complete the latest request.".into() },
                stop(ModelStopReason::EndTurn, 100)]));
        }
        if state.fallback_small {
            state.fallback_small = false;
            return Box::pin(futures::stream::iter([ModelStreamEvent::Error {
                error: ProviderError {
                    kind: ProviderErrorKind::Unknown,
                    message: "model not found".into(),
                    status: Some(404),
                    retry_after_ms: None,
                    request_id: None,
                    code: None,
                    error_type: None,
                    media_origins: None,
                    media_repair_complete: false,
                    credential_recovery: None,
                },
            }]));
        }
        if state.reject_normal {
            state.reject_normal = false;
            return Box::pin(futures::stream::iter([error(
                ProviderErrorKind::PromptTooLong,
            )]));
        }
        if state.tool_step {
            state.tool_step = false;
            let mut events = klaus_agent_core::testing::ev::tool_use(
                "step-1",
                "fixture_step",
                &RuntimeJson::Object(RuntimeObject::new()),
            );
            events.push(stop(ModelStopReason::ToolUse, 90_000));
            return Box::pin(futures::stream::iter(events));
        }
        Box::pin(futures::stream::iter([
            ModelStreamEvent::TextDelta {
                text: "Completed.".into(),
            },
            stop(
                ModelStopReason::EndTurn,
                state.normal_input_tokens.unwrap_or(100),
            ),
        ]))
    }
}

struct StepTool {
    schema: JsonSchema,
}

struct PendingHook {
    started: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl HookFn for PendingHook {
    async fn run(&self, _: HookInput) -> HookOutcome {
        self.started.notify_one();
        std::future::pending().await
    }
}

#[async_trait::async_trait]
impl Tool for StepTool {
    fn static_name(&self) -> &str {
        "fixture_step"
    }
    fn static_description(&self) -> &str {
        "Return a completed task step."
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _: &Json) -> bool {
        true
    }
    fn is_read_only(&self, _: &Json) -> bool {
        true
    }
    async fn call(
        &self,
        _: Json,
        _: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        Ok(ToolResult {
            serializable: ToolResultSerializable {
                output: RuntimeJson::Null,
                model_content: vec![OutputBlock::Text(text("Step complete."))],
                is_error: None,
                mcp_meta: None,
                extra_user_blocks: None,
            },
            side_effects: None,
        })
    }
}

struct Fixture {
    process: RuntimeProcessContext,
    agent: Agent,
    session: Arc<Session>,
    store: Arc<JsonlSessionStore>,
    dir: std::path::PathBuf,
    state: Arc<Mutex<State>>,
}

impl Fixture {
    async fn new(state: State, automatic: bool) -> Self {
        Self::with_auto_window(state, automatic, None).await
    }

    async fn with_auto_window(state: State, automatic: bool, auto_window: Option<u64>) -> Self {
        let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
        let dir = std::env::temp_dir().join(format!("klaus-preflight-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let hook_pending = state.hook_pending;
        let hook_started = state.hook_started.clone();
        let fallback_small = state.fallback_small;
        let state = Arc::new(Mutex::new(state));
        let mut options = agent_options(process.clone());
        options.provider = Arc::new(Provider {
            info: ProviderInfo {
                name: "fixture".into(),
                default_model: "fixture".into(),
                base_url: None,
                env_key: None,
            },
            state: state.clone(),
        });
        options.cwd = Some(dir.to_string_lossy().into_owned());
        options.tools = Some(vec![Arc::new(StepTool {
            schema: json!({"type":"object","properties":{}})
                .as_object()
                .unwrap()
                .clone(),
        })]);
        options.include_default_agents = Some(false);
        if hook_pending {
            options.hooks.push(Hook {
                event: HookEvent::PreCompact,
                matcher: None,
                handler: HookHandler::Fn(Arc::new(PendingHook {
                    started: hook_started,
                })),
                is_async: false,
            });
        }
        options.config = Some(json!({"model":"fixture", "contextWindow":100_000,
            "maxOutputTokens":1_000,"streamTotalTimeoutMs":200,
            "streamIdleTimeoutMs":150, "prewarm":false,"thinking":false,"autoCompaction":automatic,
            "autoMemory":false,"autoDream":false,"memoryRecall":false,
            "loopDetection":false,"approval_policy":"never","sandbox_mode":"danger-full-access"}));
        if fallback_small {
            options.config.as_mut().unwrap()["fallbackModels"] = json!(["small-fixture"]);
            options.config.as_mut().unwrap()["maxRetries"] = json!(0);
        }
        if let Some(window) = auto_window {
            options.config.as_mut().unwrap()["autoCompactWindow"] = json!(window);
        }
        let agent = Agent::new(options);
        let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(dir.join("sessions").to_string_lossy().into_owned()),
            archive_dir: None,
        }));
        let mut start = Self::start(&dir);
        start.initial_history = Some(vec![
            Message::User(UserMessage {
                uuid: "original-task".into(),
                content: vec![ContentBlock::Text(text("Keep the original task."))],
                is_meta: None,
                question_answer: None,
            }),
            Message::Assistant(AssistantMessage {
                uuid: "evidence-1".into(),
                model: "fixture".into(),
                content: vec![ContentBlock::Text(text("e".repeat(8_000)))],
                usage: None,
            }),
            Message::Assistant(AssistantMessage {
                uuid: "evidence-2".into(),
                model: "fixture".into(),
                content: vec![ContentBlock::Text(text("f".repeat(8_000)))],
                usage: None,
            }),
        ]);
        let session = agent.open_session(store.clone(), start).await.unwrap();
        Self {
            process,
            agent,
            session,
            store,
            dir,
            state,
        }
    }
    fn start(dir: &std::path::Path) -> StartSessionOptions {
        StartSessionOptions {
            cwd: Some(dir.to_string_lossy().into_owned().into()),
            user_config_dir: Some(dir.to_string_lossy().into_owned().into()),
            base_instructions: Some("Preserve the active task.".into()),
            ..Default::default()
        }
    }
    async fn run(&self) -> Vec<EngineEvent> {
        let mut turn = self.session.run_streamed(
            Input::Text("Continue the current task.".into()),
            TurnOptions::default(),
        );
        let mut events = Vec::new();
        while let Some(event) = turn.events.next().await {
            events.push(event);
        }
        events
    }
    fn trace(&self) -> Vec<&'static str> {
        self.state.lock().unwrap().trace.clone()
    }
    async fn resume(&mut self) {
        self.session.flush_rollout().await.unwrap();
        let id = self.session.id.try_to_utf8().unwrap();
        self.session.shutdown_and_wait().await.unwrap();
        self.session = self
            .agent
            .resume_session(self.store.clone(), &id, Self::start(&self.dir))
            .await
            .unwrap();
    }
    async fn close(self) {
        self.session.shutdown_and_wait().await.unwrap();
        self.process.shutdown().await.unwrap();
        std::fs::remove_dir_all(self.dir).unwrap();
    }
}

fn failed(events: &[EngineEvent]) -> bool {
    events
        .iter()
        .any(|event| matches!(event, EngineEvent::TurnFailed { .. }))
}

#[tokio::test]
async fn an_upstream_overflow_ends_the_turn_and_forces_the_next_turn_to_compact() {
    let fixture = Fixture::new(
        State {
            reject_normal: true,
            ..Default::default()
        },
        true,
    )
    .await;
    let first = fixture.run().await;
    assert!(failed(&first));
    assert_eq!(fixture.trace(), ["normal"]);
    assert_eq!(
        fixture.session.context_snapshot().window_state,
        ContextWindowState::OverLimit
    );
    let full = fixture.session.context_snapshot();
    assert_eq!(
        full.total_tokens, full.context_window,
        "A provider overflow marks the current window full"
    );
    assert_eq!(
        full.token_totals,
        Default::default(),
        "The active-window marker must not invent billed usage"
    );
    let next = fixture.run().await;
    assert!(!failed(&next), "{next:?}");
    assert_eq!(fixture.trace(), ["normal", "summary", "normal"]);
    assert_ne!(
        fixture.session.context_snapshot().window_state,
        ContextWindowState::OverLimit
    );
    fixture.close().await;
}

#[tokio::test]
async fn overflow_pressure_survives_restart_and_clears_only_after_a_committed_summary() {
    let mut fixture = Fixture::new(
        State {
            reject_normal: true,
            ..Default::default()
        },
        true,
    )
    .await;
    assert!(failed(&fixture.run().await));
    fixture.resume().await;
    assert_eq!(
        fixture.session.context_snapshot().window_state,
        ContextWindowState::OverLimit
    );
    assert!(!failed(&fixture.run().await));
    fixture.resume().await;
    assert_ne!(
        fixture.session.context_snapshot().window_state,
        ContextWindowState::OverLimit
    );
    let before = fixture.trace().len();
    assert!(!failed(&fixture.run().await));
    assert_eq!(&fixture.trace()[before..], ["normal"]);
    fixture.close().await;
}

#[tokio::test]
async fn a_long_task_compacts_between_completed_tool_steps() {
    let fixture = Fixture::new(
        State {
            tool_step: true,
            ..Default::default()
        },
        true,
    )
    .await;
    let events = fixture.run().await;
    assert!(!failed(&events), "{events:?}");
    assert_eq!(fixture.trace(), ["normal", "summary", "normal"]);
    fixture.close().await;
}

#[tokio::test]
async fn transient_summary_failures_retry_finitely_before_normal_inference() {
    let fixture = Fixture::new(
        State {
            reject_normal: true,
            ..Default::default()
        },
        true,
    )
    .await;
    assert!(failed(&fixture.run().await));
    fixture.state.lock().unwrap().summary_failures = 2;
    let events = fixture.run().await;
    assert!(!failed(&events), "{events:?}");
    assert_eq!(
        fixture.trace(),
        ["normal", "summary", "summary", "summary", "normal"]
    );
    fixture.close().await;
}

#[tokio::test]
async fn exhausted_summary_attempts_end_the_unfinished_turn_and_keep_pressure() {
    let fixture = Fixture::new(
        State {
            reject_normal: true,
            ..Default::default()
        },
        true,
    )
    .await;
    assert!(failed(&fixture.run().await));
    fixture.state.lock().unwrap().summary_failures = usize::MAX;
    let original = fixture.session.history();
    let events = fixture.run().await;
    assert!(events.iter().any(|e| matches!(e, EngineEvent::TurnFailed { error, .. } if error.message.contains("Context compaction failed"))), "{events:?}");
    let trace = fixture.trace();
    assert_eq!(trace.iter().filter(|kind| **kind == "normal").count(), 1);
    assert!(trace.iter().filter(|kind| **kind == "summary").count() <= 6);
    assert_eq!(
        fixture.session.context_snapshot().window_state,
        ContextWindowState::OverLimit
    );
    assert!(original
        .iter()
        .all(|message| fixture.session.history().contains(message)));
    assert!(!events
        .iter()
        .any(|event| matches!(event, EngineEvent::Compaction { .. })));
    fixture.close().await;
}

#[tokio::test]
async fn an_incomplete_summary_never_allows_an_ordinary_retry() {
    let fixture = Fixture::new(
        State {
            reject_normal: true,
            ..Default::default()
        },
        true,
    )
    .await;
    assert!(failed(&fixture.run().await));
    fixture.state.lock().unwrap().summary_pending = true;
    let events = fixture.run().await;
    assert!(failed(&events));
    assert_eq!(
        fixture
            .trace()
            .iter()
            .filter(|kind| **kind == "normal")
            .count(),
        1
    );
    assert_eq!(
        fixture.session.context_snapshot().window_state,
        ContextWindowState::OverLimit
    );
    assert!(!fixture.session.history().iter().any(|message| matches!(message, Message::CompactBoundary(b) if b.summary.contains("INCOMPLETE_SUMMARY"))));
    fixture.close().await;
}

#[tokio::test]
async fn disabled_automatic_compaction_preserves_provider_admission_after_an_overflow() {
    let fixture = Fixture::new(
        State {
            reject_normal: true,
            ..Default::default()
        },
        false,
    )
    .await;
    assert!(failed(&fixture.run().await));
    let next = fixture.run().await;
    assert!(!failed(&next));
    assert_eq!(fixture.trace(), ["normal", "normal"]);
    fixture.close().await;
}

#[tokio::test]
async fn disabled_automatic_compaction_does_not_refuse_successful_high_usage() {
    let fixture = Fixture::new(
        State {
            normal_input_tokens: Some(96_000),
            ..Default::default()
        },
        false,
    )
    .await;
    assert!(!failed(&fixture.run().await));
    assert!(fixture.session.context_snapshot().total_tokens >= 96_000);
    assert!(!failed(&fixture.run().await));
    assert_eq!(fixture.trace(), ["normal", "normal"]);
    fixture.close().await;
}

#[tokio::test]
async fn rollback_invalidates_persisted_pressure_for_the_discarded_input() {
    let mut fixture = Fixture::new(
        State {
            reject_normal: true,
            ..Default::default()
        },
        true,
    )
    .await;
    assert!(failed(&fixture.run().await));
    fixture.session.rollback(1).await.unwrap();
    fixture.resume().await;
    assert_ne!(
        fixture.session.context_snapshot().window_state,
        ContextWindowState::OverLimit
    );
    assert!(!failed(&fixture.run().await));
    assert_eq!(fixture.trace(), ["normal", "normal"]);
    fixture.close().await;
}

#[tokio::test]
async fn a_successful_manual_summary_clears_the_overflow_pressure() {
    let fixture = Fixture::new(
        State {
            reject_normal: true,
            ..Default::default()
        },
        true,
    )
    .await;
    assert!(failed(&fixture.run().await));
    let result = fixture.session.compact(None).await.unwrap();
    assert!(result.compacted, "{result:?}");
    assert_ne!(
        fixture.session.context_snapshot().window_state,
        ContextWindowState::OverLimit
    );
    let before = fixture.trace().len();
    assert!(!failed(&fixture.run().await));
    assert_eq!(&fixture.trace()[before..], ["normal"]);
    fixture.close().await;
}

#[tokio::test]
async fn a_pending_foreground_compaction_hook_remains_cancellable() {
    let fixture = Fixture::new(
        State {
            reject_normal: true,
            hook_pending: true,
            ..Default::default()
        },
        true,
    )
    .await;
    assert!(failed(&fixture.run().await));
    let original = fixture.session.history();
    let started = fixture.state.lock().unwrap().hook_started.clone();
    let turn = fixture.session.run_streamed(
        Input::Text("Continue the current task.".into()),
        TurnOptions::default(),
    );
    let running = tokio::spawn(async move { turn.events.collect::<Vec<_>>().await });
    tokio::time::timeout(std::time::Duration::from_secs(3), started.notified())
        .await
        .expect("The foreground compaction hook must start");
    assert!(!running.is_finished());
    fixture.session.interrupt();
    let events = tokio::time::timeout(std::time::Duration::from_secs(3), running)
        .await
        .expect("Cancellation must interrupt the pending compaction hook")
        .unwrap();
    assert!(
        events.iter().any(|event| matches!(
            event,
            EngineEvent::TurnCompleted {
                stop_reason: TerminalReason::Aborted,
                ..
            }
        )),
        "{events:?}"
    );
    assert_eq!(fixture.trace(), ["normal"]);
    assert_eq!(
        fixture.session.context_snapshot().window_state,
        ContextWindowState::OverLimit
    );
    assert!(original
        .iter()
        .all(|message| fixture.session.history().contains(message)));
    assert!(!fixture
        .session
        .history()
        .iter()
        .any(|message| matches!(message, Message::CompactBoundary(_))));
    fixture.close().await;
}

#[tokio::test]
async fn manual_compaction_has_exclusive_cancellable_admission() {
    let fixture = Fixture::new(
        State {
            summary_pending: true,
            ..Default::default()
        },
        true,
    )
    .await;
    let original = fixture.session.history();
    let running = {
        let session = fixture.session.clone();
        tokio::spawn(async move { session.compact(None).await })
    };
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while fixture.trace().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("The manual summary must start");
    assert!(fixture
        .session
        .compact(None)
        .await
        .unwrap_err()
        .contains("another operation"));
    let events = fixture.run().await;
    assert!(failed(&events), "{events:?}");
    assert_eq!(fixture.trace(), ["summary"]);
    fixture.session.interrupt();
    let result = running.await.unwrap();
    assert!(result.unwrap_err().contains("interrupted"));
    assert_eq!(fixture.session.history(), original);
    fixture.state.lock().unwrap().summary_pending = false;
    assert!(!failed(&fixture.run().await));
    fixture.close().await;
}

#[tokio::test]
async fn manual_compaction_retries_transient_summary_failures() {
    let fixture = Fixture::new(
        State {
            summary_failures: 2,
            ..Default::default()
        },
        true,
    )
    .await;
    let result = fixture.session.compact(None).await.unwrap();
    assert!(result.compacted, "{result:?}");
    assert_eq!(fixture.trace(), ["summary", "summary", "summary"]);
    fixture.close().await;
}

#[tokio::test]
async fn a_pending_manual_compaction_hook_remains_cancellable() {
    let fixture = Fixture::new(
        State {
            hook_pending: true,
            ..Default::default()
        },
        true,
    )
    .await;
    let original = fixture.session.history();
    let started = fixture.state.lock().unwrap().hook_started.clone();
    let running = {
        let session = fixture.session.clone();
        tokio::spawn(async move { session.compact(None).await })
    };
    tokio::time::timeout(std::time::Duration::from_secs(3), started.notified())
        .await
        .expect("The manual compaction hook must start");
    assert!(!running.is_finished());
    fixture.session.interrupt();
    let result = tokio::time::timeout(std::time::Duration::from_secs(3), running)
        .await
        .expect("Cancellation must interrupt the pending manual hook")
        .unwrap();
    assert!(result.unwrap_err().contains("interrupted"));
    assert_eq!(fixture.session.history(), original);
    assert!(fixture.trace().is_empty());
    fixture.close().await;
}

#[tokio::test]
async fn an_uncompressible_latest_input_below_capacity_can_cross_the_early_watermark() {
    let fixture = Fixture::with_auto_window(State::default(), true, Some(20_000)).await;
    fixture.session.rollback(1).await.unwrap();
    let events = fixture
        .session
        .run_streamed(
            Input::Text("Latest task details. ".repeat(2_000).into()),
            TurnOptions::default(),
        )
        .events
        .collect::<Vec<_>>()
        .await;
    assert!(!failed(&events), "{events:?}");
    assert_eq!(fixture.trace(), ["normal"]);
    fixture.close().await;
}

#[tokio::test]
async fn a_smaller_fallback_model_reenters_preflight_before_receiving_history() {
    let fixture = Fixture::new(
        State {
            fallback_small: true,
            ..Default::default()
        },
        true,
    )
    .await;
    let events = fixture.run().await;
    {
        let state = fixture.state.lock().unwrap();
        assert_eq!(state.requests.first().unwrap().model, "fixture");
        for request in state.requests.iter().filter(|request| {
            request.model == "small-fixture" && request.skip_cache_write != Some(true)
        }) {
            let input =
                klaus_agent_core::context::tokens::estimate_messages_tokens(&request.messages)
                    + klaus_agent_core::context::tokens::estimate_prefix_tokens(
                        &request.system,
                        &request.tools,
                    );
            assert!(
                input < 10_000 * 95 / 100,
                "An oversized fallback request escaped preflight: {input}"
            );
        }
        assert!(
            failed(&events)
                || state
                    .requests
                    .iter()
                    .any(|request| request.model == "small-fixture")
        );
    }
    fixture.close().await;
}
