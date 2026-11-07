//! Defect: PostCompact hooks could not distinguish manual compaction from
//! automatic recovery, so trigger-specific hooks also ran for the wrong source.
//! Root cause: Session omitted the trigger and HookBus treated PostCompact
//! matchers as tool matchers, admitting every hook when tool_name was absent.
//! Fixed by: 2026-09-06 context-management retirement follow-up.

use async_trait::async_trait;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::deps::{CompactionResult, Compactor};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::hooks::types::{
    Hook, HookEvent, HookFn, HookHandler, HookInput, HookOutcome, HookTrigger,
};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::config::ResolvedConfig;
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
use std::sync::{Arc, Mutex};

const MODEL_CONTEXT_WINDOW: u64 = 200_000;
const PROACTIVE_CONTEXT_WINDOW: u64 = 20_000;

#[derive(Clone, Copy, Debug)]
enum Path {
    Manual,
    Proactive,
    Reactive,
}

impl Path {
    fn auto_compact_window(self) -> Option<u64> {
        match self {
            Self::Proactive => Some(PROACTIVE_CONTEXT_WINDOW),
            Self::Manual | Self::Reactive => None,
        }
    }

    fn trigger(self) -> HookTrigger {
        match self {
            Self::Manual => HookTrigger::Manual,
            Self::Proactive | Self::Reactive => HookTrigger::Auto,
        }
    }
}

type HookCalls = Arc<Mutex<Vec<(String, HookInput)>>>;

struct RecordingHook {
    name: String,
    calls: HookCalls,
}

#[async_trait]
impl HookFn for RecordingHook {
    async fn run(&self, input: HookInput) -> HookOutcome {
        self.calls.lock().unwrap().push((self.name.clone(), input));
        HookOutcome::default()
    }
}

fn hooks(calls: &HookCalls) -> Vec<Hook> {
    [("manual", "manual"), ("auto", "auto"), ("wildcard", "*")]
        .into_iter()
        .map(|(name, matcher)| Hook {
            event: HookEvent::PostCompact,
            matcher: Some(matcher.to_string()),
            handler: HookHandler::Fn(Arc::new(RecordingHook {
                name: name.to_string(),
                calls: calls.clone(),
            })),
            is_async: false,
        })
        .collect()
}

struct ControlledCompactor {
    succeed: bool,
    manual: AtomicUsize,
    proactive: AtomicUsize,
    reactive: AtomicUsize,
}

impl ControlledCompactor {
    fn result(&self, messages: &[Message]) -> CompactionResult {
        CompactionResult {
            messages: if self.succeed {
                vec![Message::CompactBoundary(CompactBoundaryMessage {
                    native_compaction: None,
                    uuid: "hook-regression-boundary".into(),
                    summary: "Continue the existing implementation task.".into(),
                })]
            } else {
                messages.to_vec()
            },
            freed_tokens: if self.succeed { 20_000 } else { 0 },
            compacted: self.succeed,
            reason: None,
            diagnostics: Vec::new(),
        }
    }
}

#[async_trait]
impl Compactor for ControlledCompactor {
    async fn manual_compact(
        &self,
        messages: &[Message],
        _config: &ResolvedConfig,
        _instructions: Option<&RuntimeString>,
    ) -> CompactionResult {
        self.manual.fetch_add(1, Ordering::SeqCst);
        self.result(messages)
    }

    async fn maybe_compact(
        &self,
        messages: &[Message],
        _estimated_tokens: u64,
        _config: &ResolvedConfig,
        _instructions: Option<&RuntimeString>,
    ) -> CompactionResult {
        self.proactive.fetch_add(1, Ordering::SeqCst);
        self.result(messages)
    }

    async fn threshold_compact(
        &self,
        messages: &[Message],
        _config: &ResolvedConfig,
        _instructions: Option<&RuntimeString>,
    ) -> CompactionResult {
        self.proactive.fetch_add(1, Ordering::SeqCst);
        self.result(messages)
    }

    async fn reactive_compact(
        &self,
        messages: &[Message],
        _config: &ResolvedConfig,
        _instructions: Option<&RuntimeString>,
    ) -> CompactionResult {
        self.reactive.fetch_add(1, Ordering::SeqCst);
        self.result(messages)
    }
}

struct ControlledProvider {
    info: ProviderInfo,
    path: Path,
    calls: AtomicUsize,
}

impl ModelProvider for ControlledProvider {
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
            context_window: MODEL_CONTEXT_WINDOW,
            max_output_tokens: 1_000,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }

    fn stream(&self, _request: ModelRequest, _signal: AbortSignal) -> ModelStream {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if matches!(self.path, Path::Reactive) && call == 0 {
            return Box::pin(futures::stream::iter(vec![ModelStreamEvent::Error {
                error: ProviderError {
                    kind: ProviderErrorKind::PromptTooLong,
                    message: "prompt is too long".into(),
                    status: Some(400),
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
        Box::pin(futures::stream::iter(vec![
            ModelStreamEvent::MessageStart {
                model: "hook-regression".into(),
            },
            ModelStreamEvent::TextDelta {
                text: "Completed the implementation.".into(),
            },
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: Usage {
                    input_tokens: 100,
                    output_tokens: 10,
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
    (0..4)
        .flat_map(|index| {
            [
                Message::User(UserMessage {
                    question_answer: None,
                    uuid: format!("history-user-{index}").into(),
                    content: vec![text("Continue the implementation.")],
                    is_meta: None,
                }),
                Message::Assistant(AssistantMessage {
                    uuid: format!("history-assistant-{index}").into(),
                    content: vec![text("Earlier implementation detail. ".repeat(1_000))],
                    model: "hook-regression".into(),
                    usage: None,
                }),
            ]
        })
        .collect()
}

async fn assert_session_hooks(path: Path, succeed: bool) {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let root = std::env::temp_dir().join(format!("klaus-compact-hooks-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let compactor = Arc::new(ControlledCompactor {
        succeed,
        manual: AtomicUsize::new(0),
        proactive: AtomicUsize::new(0),
        reactive: AtomicUsize::new(0),
    });
    let provider = Arc::new(ControlledProvider {
        info: ProviderInfo {
            name: "hook-regression".into(),
            default_model: "hook-regression".into(),
            base_url: None,
            env_key: None,
        },
        path,
        calls: AtomicUsize::new(0),
    });
    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
    options.provider = provider.clone();
    options.compactor = Some(compactor.clone());
    options.hooks = hooks(&calls);
    let mut config = json!({
        "model": "hook-regression",
        "permissionMode": "bypassPermissions",
        "contextWindow": MODEL_CONTEXT_WINDOW,
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
    });
    if let Some(window) = path.auto_compact_window() {
        config["autoCompactWindow"] = json!(window);
    }
    options.config = Some(config);
    let agent = Agent::new(options);
    let session = agent.start_session(StartSessionOptions {
        cwd: Some(root.to_string_lossy().into_owned().into()),
        user_config_dir: Some(root.to_string_lossy().into_owned().into()),
        initial_history: Some(history()),
        ..Default::default()
    });
    let snapshot = session.context_snapshot();
    assert_eq!(snapshot.context_window, MODEL_CONTEXT_WINDOW);
    assert_eq!(
        snapshot.compact_threshold_tokens,
        path.auto_compact_window()
            .unwrap_or(MODEL_CONTEXT_WINDOW * 90 / 100),
        "{path:?} must distinguish the automatic trigger from actual model admission"
    );
    let (manual_outcome, turn_outcome) = match path {
        Path::Manual => (Some(session.compact(None).await), None),
        Path::Proactive | Path::Reactive => {
            if matches!(path, Path::Reactive) {
                let first = session
                    .run(Input::Text("begin".into()), TurnOptions::default())
                    .await;
                assert_eq!(
                    first.error.as_ref().unwrap().provider_kind,
                    Some(ProviderErrorKind::PromptTooLong)
                );
                assert!(
                    calls.lock().unwrap().is_empty(),
                    "No compaction runs in the rejected turn"
                );
            }
            (
                None,
                Some(
                    session
                        .run(Input::Text("continue".into()), TurnOptions::default())
                        .await,
                ),
            )
        }
    };
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();

    if let Some(outcome) = manual_outcome {
        assert_eq!(outcome.unwrap().compacted, succeed);
    }
    if let Some(outcome) = turn_outcome {
        let expected_stop = if !succeed {
            assert!(outcome
                .error
                .as_ref()
                .unwrap()
                .message
                .contains("Context compaction failed"));
            TerminalReason::Error
        } else {
            TerminalReason::Completed
        };
        assert_eq!(outcome.stop_reason, expected_stop, "{path:?}");
    }
    let compactor_calls = [
        compactor.manual.load(Ordering::SeqCst),
        compactor.proactive.load(Ordering::SeqCst),
        compactor.reactive.load(Ordering::SeqCst),
    ];
    let expected_compactor_calls = match path {
        Path::Manual => [1, 0, 0],
        Path::Proactive => [0, 1, 0],
        Path::Reactive => [0, 1, 0],
    };
    assert_eq!(compactor_calls, expected_compactor_calls, "{path:?}");
    let expected_provider_calls = match path {
        Path::Manual => 0,
        Path::Proactive => usize::from(succeed),
        Path::Reactive => {
            if succeed {
                2
            } else {
                1
            }
        }
    };
    assert_eq!(
        provider.calls.load(Ordering::SeqCst),
        expected_provider_calls
    );

    let calls = calls.lock().unwrap();
    let mut names: Vec<_> = calls.iter().map(|(name, _)| name.as_str()).collect();
    names.sort_unstable();
    let expected = if !succeed {
        Vec::new()
    } else if matches!(path, Path::Manual) {
        vec!["manual", "wildcard"]
    } else {
        vec!["auto", "wildcard"]
    };
    assert_eq!(names, expected, "{path:?} PostCompact matcher selection");
    for (_, input) in calls.iter() {
        assert_eq!(input.event, HookEvent::PostCompact);
        assert_eq!(input.trigger, Some(path.trigger()), "{path:?}");
    }
}

#[tokio::test]
async fn manual_compaction_dispatches_only_manual_and_wildcard_post_hooks() {
    assert_session_hooks(Path::Manual, true).await;
}

#[tokio::test]
async fn proactive_compaction_dispatches_only_auto_and_wildcard_post_hooks() {
    assert_session_hooks(Path::Proactive, true).await;
}

#[tokio::test]
async fn next_turn_overflow_recovery_dispatches_only_auto_and_wildcard_post_hooks() {
    assert_session_hooks(Path::Reactive, true).await;
}

#[tokio::test]
async fn unsuccessful_compaction_never_dispatches_post_hooks() {
    for path in [Path::Manual, Path::Proactive, Path::Reactive] {
        assert_session_hooks(path, false).await;
    }
}
