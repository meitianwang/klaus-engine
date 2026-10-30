//! Defect: selected compaction had no session entry point and skipped manual policy.
//! Root cause: the segment primitive was disconnected from stable identities and adoption.
//! Work item: preserve the complementary segment through the ordinary manual lifecycle.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use klaus_agent_core::context::compact::full_compact::PartialCompactionDirection;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::hooks::types::{
    Hook, HookDecision, HookEvent, HookFn, HookHandler, HookInput, HookOutcome, HookTrigger,
};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeString};
use klaus_agent_core::schemas::content::{ContentBlock, TextBlock};
use klaus_agent_core::schemas::message::{AssistantMessage, Message, Usage, UserMessage};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

const SUMMARY: &str = "Selected task summary with exact acceptance criteria.";
const RESTORED: &str = "Restore the active validation requirement.";

struct Provider {
    info: ProviderInfo,
    requests: Mutex<Vec<ModelRequest>>,
}

impl ModelProvider for Provider {
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
            context_window: 200_000,
            max_output_tokens: 2_000,
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
                text: SUMMARY.into(),
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

type Calls = Arc<Mutex<Vec<HookInput>>>;
struct RecordingHook {
    calls: Calls,
    outcome: HookOutcome,
}
#[async_trait]
impl HookFn for RecordingHook {
    async fn run(&self, input: HookInput) -> HookOutcome {
        self.calls.lock().unwrap().push(input);
        self.outcome.clone()
    }
}
fn hook(event: HookEvent, calls: &Calls, outcome: HookOutcome) -> Hook {
    Hook {
        event,
        matcher: Some(
            match event {
                HookEvent::SessionStart => "compact",
                _ => "manual",
            }
            .into(),
        ),
        handler: HookHandler::Fn(Arc::new(RecordingHook {
            calls: calls.clone(),
            outcome,
        })),
        is_async: false,
    }
}
fn text(body: impl Into<RuntimeString>) -> ContentBlock {
    ContentBlock::Text(TextBlock {
        text: body.into(),
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
                    uuid: format!("user-{index}").into(),
                    content: vec![text(format!("Task step {index}."))],
                    is_meta: None,
                }),
                Message::Assistant(AssistantMessage {
                    uuid: format!("assistant-{index}").into(),
                    content: vec![text("Detailed completed progress. ".repeat(100))],
                    model: "selected-session".into(),
                    usage: None,
                }),
            ]
        })
        .collect()
}
fn has_id(messages: &[Message], id: &str) -> bool {
    messages.iter().any(|message| match message {
        Message::User(value) => value.uuid == id,
        Message::Assistant(value) => value.uuid == id,
        _ => false,
    })
}

#[tokio::test]
async fn selected_manual_compaction_preserves_the_other_side_and_runs_the_existing_hooks() {
    for direction in [
        PartialCompactionDirection::From,
        PartialCompactionDirection::UpTo,
    ] {
        let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
        let dir =
            std::env::temp_dir().join(format!("klaus-selected-session-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("AGENTS.md"),
            "WORKSPACE_SELECTION_POLICY: preserve user authority.\n",
        )
        .unwrap();
        let provider = Arc::new(Provider {
            info: ProviderInfo {
                name: "selected-session".into(),
                default_model: "selected-session".into(),
                base_url: None,
                env_key: None,
            },
            requests: Mutex::new(Vec::new()),
        });
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut options = agent_options(process.clone());
        options.provider = provider.clone();
        options.tools = Some(Vec::new());
        options.hooks = vec![
            hook(
                HookEvent::PreCompact,
                &calls,
                HookOutcome {
                    additional_context: Some("Keep selected constraints exact.".into()),
                    ..Default::default()
                },
            ),
            hook(
                HookEvent::SessionStart,
                &calls,
                HookOutcome {
                    additional_context: Some(RESTORED.into()),
                    ..Default::default()
                },
            ),
            hook(HookEvent::PostCompact, &calls, HookOutcome::default()),
        ];
        options.config = Some(
            serde_json::json!({ "model":"selected-session", "maxOutputTokens":2000, "autoCompaction":false, "speculativeCompaction":false, "compactionSelfCheck":false }),
        );
        let agent = Agent::new(options);
        let original = history();
        let session = agent.start_session(StartSessionOptions {
            cwd: Some(dir.to_string_lossy().into_owned().into()),
            user_config_dir: Some(dir.to_string_lossy().into_owned().into()),
            initial_history: Some(original.clone()),
            ..Default::default()
        });
        let outcome = session
            .compact_selected(
                Some("Preserve the pending task.".into()),
                "user-2".into(),
                direction,
            )
            .await
            .unwrap();
        assert!(
            outcome.compacted,
            "selected summary should be accepted: {outcome:?}"
        );
        let kept = session.history();
        match direction {
            PartialCompactionDirection::From => {
                assert!(original[..4].iter().all(|message| kept.contains(message)));
                assert!(has_id(&kept, "user-0"));
                assert!(has_id(&kept, "assistant-1"));
                assert!(has_id(&kept, "user-2"));
                assert!(!has_id(&kept, "assistant-3"));
            }
            PartialCompactionDirection::UpTo => {
                assert!(original[4..].iter().all(|message| kept.contains(message)));
                assert!(has_id(&kept, "user-0"));
                assert!(!has_id(&kept, "assistant-1"));
                assert!(has_id(&kept, "user-2"));
                assert!(has_id(&kept, "assistant-3"));
            }
        }
        let rendered = runtime_json::stringify(&runtime_codec::encode_value(&kept).unwrap());
        assert!(rendered.contains(SUMMARY));
        assert!(!rendered.contains(RESTORED));
        let inputs = calls.lock().unwrap().clone();
        assert_eq!(
            inputs.iter().map(|input| input.event).collect::<Vec<_>>(),
            vec![HookEvent::PreCompact, HookEvent::PostCompact]
        );
        assert_eq!(inputs[1].trigger, Some(HookTrigger::Manual));
        assert_eq!(inputs[1].message, Some(SUMMARY.into()));
        let requests = provider.requests.lock().unwrap().clone();
        assert_eq!(requests.len(), 1);
        let prompt = runtime_json::stringify(&runtime_codec::encode_value(&requests[0]).unwrap());
        assert!(prompt.contains("WORKSPACE_SELECTION_POLICY"));
        assert!(prompt.contains("Preserve the pending task."));
        assert!(prompt.contains("Keep selected constraints exact."));
        session.shutdown_and_wait().await.unwrap();
        process.shutdown().await.unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[tokio::test]
async fn missing_selection_and_hook_veto_never_summarize_or_rewrite_history() {
    for veto in [false, true] {
        let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
        let provider = Arc::new(Provider {
            info: ProviderInfo {
                name: "selected-session".into(),
                default_model: "selected-session".into(),
                base_url: None,
                env_key: None,
            },
            requests: Mutex::new(Vec::new()),
        });
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut options = agent_options(process.clone());
        options.provider = provider.clone();
        options.tools = Some(Vec::new());
        options.hooks = vec![hook(
            HookEvent::PreCompact,
            &calls,
            HookOutcome {
                decision: Some(HookDecision::Block),
                ..Default::default()
            },
        )];
        let agent = Agent::new(options);
        let session = agent.start_session(StartSessionOptions {
            initial_history: Some(history()),
            ..Default::default()
        });
        let before = session.history();
        let error = session
            .compact_selected(
                None,
                if veto { "user-2" } else { "missing" }.into(),
                PartialCompactionDirection::From,
            )
            .await
            .unwrap_err();
        assert_eq!(
            error,
            if veto {
                "pre-compact-hook-rejected"
            } else {
                "selected compaction message is no longer in context"
            }
        );
        assert!(provider.requests.lock().unwrap().is_empty());
        assert_eq!(session.history(), before);
        assert_eq!(calls.lock().unwrap().len(), usize::from(veto));
        session.shutdown_and_wait().await.unwrap();
        process.shutdown().await.unwrap();
    }
}
