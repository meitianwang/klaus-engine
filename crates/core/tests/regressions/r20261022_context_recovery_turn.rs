//! Defect: rough request estimates could reject provider-admissible input.
//! Root cause: local estimates were treated as authoritative admission decisions.
//! Work item 20261022: preserve completed checkpoints and provider admission authority.

use std::sync::{Arc, Mutex};

use futures::StreamExt;
use klaus_agent_core::context::compact::engine::Compactor as ContextCompactor;
use klaus_agent_core::context::compact::{create_compactor, CompactorOptions};
use klaus_agent_core::context::tokens::{estimate_messages_tokens, estimate_prefix_tokens};
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::deps::{CompactionResult, Compactor};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::hooks::types::{
    Hook, HookEvent, HookFn, HookHandler, HookInput, HookOutcome, HookTrigger,
};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::config::{default_config, ResolvedConfig};
use klaus_agent_core::schemas::content::{ContentBlock, TextBlock};
use klaus_agent_core::schemas::message::{
    AssistantMessage, CompactBoundaryMessage, Message, Usage, UserMessage,
};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo, SystemPromptPart,
};
use serde_json::json;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

const LATEST: &str = "Continue automatically and retain this exact input.";
const BUDGET: u64 = 95_000;
const RESTORED: &str = "CANDIDATE_ONLY_RESTORED_CONTEXT";
#[derive(PartialEq)]
enum Case {
    Recover,
    OversizedCheckpoint,
    RestoredContext,
    RefreshedPrefix,
    AnchoredPrefixGrowth,
}
type Requests = Arc<Mutex<Vec<ModelRequest>>>;

fn text(value: impl Into<RuntimeString>) -> ContentBlock {
    ContentBlock::Text(TextBlock {
        text: value.into(),
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    })
}

fn user(id: &str, value: impl Into<RuntimeString>) -> Message {
    Message::User(UserMessage {
        uuid: id.into(),
        content: vec![text(value)],
        is_meta: None,
        question_answer: None,
    })
}

fn stop() -> ModelStreamEvent {
    ModelStreamEvent::MessageStop {
        stop_reason: ModelStopReason::EndTurn,
        usage: Usage {
            input_tokens: 100,
            output_tokens: 1,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
            total_tokens: None,
        },
    }
}

struct Provider {
    info: ProviderInfo,
    requests: Requests,
}

impl ModelProvider for Provider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }
    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: true,
            tools: false,
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: 100_000,
            max_output_tokens: 1_000,
            vision: true,
            native_pdf: true,
            image_limits: None,
            max_media_blocks: None,
            supports_image_detail_original: false,
        }
    }
    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        let mut requests = self.requests.lock().unwrap();
        requests.push(request);
        Box::pin(futures::stream::iter([
            ModelStreamEvent::TextDelta {
                text: "Recovered in the original turn.".into(),
            },
            stop(),
        ]))
    }
}

struct Recovery {
    inner: Arc<dyn klaus_agent_core::context::compact::engine::Compactor>,
    oversized_checkpoint: bool,
}

struct RestoreHook;

#[async_trait::async_trait]
impl HookFn for RestoreHook {
    async fn run(&self, input: HookInput) -> HookOutcome {
        HookOutcome {
            additional_context: (input.trigger == Some(HookTrigger::Compact))
                .then(|| format!("{RESTORED}{}", "h".repeat(400_000)).into()),
            ..Default::default()
        }
    }
}

#[async_trait::async_trait]
impl Compactor for Recovery {
    fn prepare_request(&self, request: &ModelRequest, signal: &AbortSignal) {
        self.inner.prepare_request(request, signal);
    }
    async fn maybe_compact(
        &self,
        messages: &[Message],
        _: u64,
        _: &ResolvedConfig,
        _: Option<&RuntimeString>,
    ) -> CompactionResult {
        CompactionResult {
            messages: messages.to_vec(),
            freed_tokens: 0,
            compacted: false,
            reason: None,
            diagnostics: vec![],
        }
    }
    async fn reactive_compact(
        &self,
        messages: &[Message],
        config: &ResolvedConfig,
        instructions: Option<&RuntimeString>,
    ) -> CompactionResult {
        if self.oversized_checkpoint {
            return CompactionResult {
                messages: vec![Message::CompactBoundary(CompactBoundaryMessage {
                    uuid: "completed-oversized-summary".into(),
                    summary: "s".repeat(410_000).into(),
                    native_compaction: None,
                })],
                freed_tokens: 1,
                compacted: true,
                reason: None,
                diagnostics: vec![],
            };
        }
        let result = self
            .inner
            .reactive_compact(messages.to_vec(), config, instructions)
            .await;
        CompactionResult {
            messages: result.messages,
            freed_tokens: result.freed_tokens,
            compacted: result.compacted,
            reason: result.reason,
            diagnostics: result.diagnostics,
        }
    }
}

async fn run(case: Case) -> (Vec<EngineEvent>, Vec<ModelRequest>, Vec<Message>, usize) {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let dir = std::env::temp_dir().join(format!("klaus-reactive-turn-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let requests = Requests::default();
    let summary_calls = Arc::new(Mutex::new(0));
    let seen = summary_calls.clone();
    let mut summary_config = default_config();
    summary_config.model = "regression-noop".into();
    summary_config.context_window = 120_000;
    summary_config.max_output_tokens = Some(1_000);
    let inner = create_compactor(CompactorOptions {
        call_model: Box::new(|_, _| panic!("The summary transport is explicitly bound")),
        model: "regression-noop".into(),
        max_output_tokens: Some(1_000),
        uuid: Box::new(|| uuid::Uuid::new_v4().to_string()),
    })
    .rebind_summary(
        Box::new(move |_, _| -> ModelStream {
            let mut calls = seen.lock().unwrap();
            *calls += 1;
            if *calls > 4 {
                return Box::pin(futures::stream::pending());
            }
            Box::pin(futures::stream::iter([
                ModelStreamEvent::TextDelta {
                    text: "Preserve the task constraints and completed work.".into(),
                },
                stop(),
            ]))
        }),
        &summary_config,
    )
    .unwrap();
    let mut options = agent_options(process.clone());
    options.cwd = Some(dir.to_string_lossy().into_owned());
    options.provider = Arc::new(Provider {
        info: ProviderInfo {
            name: "regression-noop".into(),
            default_model: "regression-noop".into(),
            base_url: None,
            env_key: None,
        },
        requests: requests.clone(),
    });
    options.compactor = Some(Arc::new(Recovery {
        inner,
        oversized_checkpoint: case == Case::OversizedCheckpoint,
    }));
    if case == Case::RestoredContext {
        options.hooks.push(Hook {
            event: HookEvent::SessionStart,
            matcher: None,
            handler: HookHandler::Fn(Arc::new(RestoreHook)),
            is_async: false,
        });
    }
    if matches!(case, Case::RefreshedPrefix | Case::AnchoredPrefixGrowth) {
        let summary_calls = summary_calls.clone();
        let requests = requests.clone();
        let anchored_growth = case == Case::AnchoredPrefixGrowth;
        options.system_prompt_provider = Some(Arc::new(move || {
            let grew = if anchored_growth {
                !requests.lock().unwrap().is_empty()
            } else {
                *summary_calls.lock().unwrap() > 0
            };
            Box::pin(async move {
                vec![SystemPromptPart {
                    name: "fixture-policy".into(),
                    text: if grew {
                        "g".repeat(400_000).into()
                    } else {
                        "Current policy.".into()
                    },
                    cacheable: None,
                    cache_scope: None,
                }]
            })
        }));
    }
    options.config = Some(json!({
        "model":"regression-noop", "contextWindow":100_000, "maxOutputTokens":1_000,
        "streamTotalTimeoutMs":1_000, "prewarm":false,
        "thinking":false, "memory":{"enabled":false}, "autoMemory":false, "autoDream":false,
        "memoryRecall":false,
        "loopDetection":false,
        "approval_policy":"never", "sandbox_mode":"danger-full-access"
    }));
    let mut history = vec![user("original-task", "Preserve the task constraints.")];
    let old_assistant_count = if case == Case::AnchoredPrefixGrowth {
        0
    } else {
        6
    };
    for index in 0..old_assistant_count {
        history.push(Message::Assistant(AssistantMessage {
            uuid: format!("original-{index}").into(),
            model: "regression-noop".into(),
            content: vec![text("e".repeat(80_000))],
            usage: None,
        }));
    }
    let session = Agent::new(options).start_session(StartSessionOptions {
        user_config_dir: Some(dir.to_string_lossy().into_owned().into()),
        initial_history: Some(history),
        base_instructions: (!matches!(case, Case::RefreshedPrefix | Case::AnchoredPrefixGrowth))
            .then(|| "Continue the existing task.".into()),
        ..Default::default()
    });
    let mut turn = session.run_streamed(Input::Text(LATEST.into()), TurnOptions::default());
    let mut events = Vec::new();
    while let Some(event) = turn.events.next().await {
        events.push(event);
    }
    if case == Case::AnchoredPrefixGrowth {
        let next = session.run_streamed(
            Input::Text("Continue after the instruction prefix changes.".into()),
            TurnOptions::default(),
        );
        events.extend(next.events.collect::<Vec<_>>().await);
    }
    let history = session.history();
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
    let requests = requests.lock().unwrap().clone();
    let calls = *summary_calls.lock().unwrap();
    (events, requests, history, calls)
}

#[tokio::test]
async fn preflight_compacts_before_the_first_normal_request() {
    let (events, requests, _, summary_calls) = run(Case::Recover).await;
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, EngineEvent::TurnFailed { .. })),
        "{events:?}"
    );
    assert!(events
        .iter()
        .any(|e| matches!(e, EngineEvent::Compaction { .. })));
    assert_eq!(
        requests.len(),
        1,
        "Compaction must precede the first normal request"
    );
    assert!(summary_calls <= 4);
    let recovered = &requests[0];
    assert!(
        estimate_messages_tokens(&recovered.messages)
            + estimate_prefix_tokens(&recovered.system, &recovered.tools)
            < BUDGET
    );
    assert!(recovered
        .messages
        .iter()
        .any(|m| matches!(m, Message::User(u)
        if u.content.iter().any(|b| matches!(b, ContentBlock::Text(t) if t.text == LATEST)))));
}

#[tokio::test]
async fn a_completed_checkpoint_is_committed_before_provider_admission() {
    let (events, requests, history, _) = run(Case::OversizedCheckpoint).await;
    assert_eq!(
        requests.len(),
        1,
        "A rough estimate must not bypass the provider's admission decision"
    );
    assert!(events
        .iter()
        .any(|event| matches!(event, EngineEvent::Compaction { .. })));
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{events:?}"
    );
    assert!(history.iter().any(
        |message| matches!(message, Message::CompactBoundary(boundary)
        if boundary.uuid == "completed-oversized-summary")
    ));
    assert!(!history
        .iter()
        .any(|message| matches!(message, Message::Assistant(assistant)
        if assistant.uuid == "original-0")));
    assert!(history
        .iter()
        .any(|message| matches!(message, Message::User(user)
        if user.uuid == "original-task")));
    assert!(history
        .iter()
        .any(|message| matches!(message, Message::User(user)
        if user.content.iter().any(|block| matches!(block, ContentBlock::Text(text)
            if text.text == LATEST)))));
}

#[tokio::test]
async fn summary_completion_does_not_reinject_a_session_start_compact_hook() {
    let (events, requests, history, _) = run(Case::RestoredContext).await;
    assert_eq!(requests.len(), 1);
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{events:?}"
    );
    assert!(events
        .iter()
        .any(|event| matches!(event, EngineEvent::Compaction { .. })));
    for message in history.iter().chain(&requests[0].messages) {
        assert!(!matches!(message, Message::User(user)
            if user.content.iter().any(|block| matches!(block, ContentBlock::Text(text)
                if text.text.contains(RESTORED)))));
    }
}

#[tokio::test]
async fn a_refreshed_prefix_reaches_the_provider_after_a_completed_checkpoint() {
    let (events, requests, _, summary_calls) = run(Case::RefreshedPrefix).await;
    assert_eq!(requests.len(), 1);
    assert_eq!(summary_calls, 1);
    assert!(estimate_prefix_tokens(&requests[0].system, &requests[0].tools) > BUDGET);
    assert!(requests[0]
        .system
        .iter()
        .any(|part| part.name == "fixture-policy" && part.text == "g".repeat(400_000)));
    assert!(requests[0]
        .messages
        .iter()
        .any(|message| matches!(message, Message::CompactBoundary(_))));
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{events:?}"
    );
}

#[tokio::test]
async fn authoritative_usage_and_prefix_growth_do_not_manufacture_a_window_failure() {
    let (events, requests, _, summary_calls) = run(Case::AnchoredPrefixGrowth).await;
    assert_eq!(requests.len(), 2);
    assert_eq!(summary_calls, 0);
    assert!(estimate_prefix_tokens(&requests[0].system, &requests[0].tools) < BUDGET);
    assert!(estimate_prefix_tokens(&requests[1].system, &requests[1].tools) > BUDGET);
    assert!(requests[1]
        .messages
        .iter()
        .any(|message| matches!(message, Message::Assistant(assistant)
        if assistant.usage.as_ref().is_some_and(|usage| usage.input_tokens == 100))));
    assert!(
        !events.iter().any(|event| matches!(
            event,
            EngineEvent::TurnFailed { .. } | EngineEvent::Compaction { .. }
        )),
        "{events:?}"
    );
}
