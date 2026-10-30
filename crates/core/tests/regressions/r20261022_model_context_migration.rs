//! Defect: a smaller selected model received history that only the previous
//! provider could accept. Root cause: publication discarded the summary source.
//! Work item: 2026-10-22 model context migration before normal request admission.

use std::sync::{Arc, Mutex};

use futures::StreamExt;
use klaus_agent_core::context::tokens::{estimate_context_tokens, estimate_prefix_tokens};
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{resolve_config, Input, Session, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::content::{ContentBlock, TextBlock};
use klaus_agent_core::schemas::message::{AssistantMessage, Message, Usage, UserMessage};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{ProviderError, ProviderErrorKind};
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use serde_json::json;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

const SUMMARY: &str = "The original task constraints and completed work remain available. Continue with the latest user request.";
const MODEL: &str = "shared-model";
type Requests = Arc<Mutex<Vec<(String, ModelRequest)>>>;

#[derive(Clone, Copy)]
enum Reply {
    Success,
    Failure,
    Cancel,
    Insufficient,
}

struct Provider {
    info: ProviderInfo,
    window: u64,
    requests: Requests,
    reply: Reply,
}

impl Provider {
    fn new(name: &str, window: u64, requests: &Requests, reply: Reply) -> Arc<Self> {
        Arc::new(Self {
            info: ProviderInfo {
                name: name.into(),
                default_model: MODEL.into(),
                base_url: None,
                env_key: None,
            },
            window,
            requests: requests.clone(),
            reply,
        })
    }
}

impl ModelProvider for Provider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, model: &RuntimeString) -> ProviderCapabilities {
        let window = if model == "selected-small" {
            128_000
        } else {
            self.window
        };
        ProviderCapabilities {
            streaming: true,
            tools: false,
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: window,
            max_output_tokens: if window == 500_000 { 4_000 } else { 2_000 },
            vision: true,
            native_pdf: true,
            image_limits: None,
            max_media_blocks: None,
            supports_image_detail_original: false,
        }
    }

    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        self.requests
            .lock()
            .unwrap()
            .push((self.info.name.try_to_utf8().unwrap(), request.clone()));
        if matches!(self.reply, Reply::Cancel) {
            signal.cancel();
            return Box::pin(futures::stream::empty());
        }
        if matches!(self.reply, Reply::Failure) {
            return Box::pin(futures::stream::iter([ModelStreamEvent::Error {
                error: ProviderError {
                    kind: ProviderErrorKind::Timeout,
                    message: "Controlled summary timeout".into(),
                    status: None,
                    retry_after_ms: None,
                    request_id: None,
                    code: None,
                    error_type: None,
                    media_origins: None,
                    media_repair_complete: false,
                },
            }]));
        }
        let text = if matches!(self.reply, Reply::Insufficient) {
            "x".repeat(1_100_000).into()
        } else if self.window == 500_000 {
            SUMMARY.into()
        } else {
            "The new model continued.".into()
        };
        Box::pin(futures::stream::iter([
            ModelStreamEvent::MessageStart {
                model: request.model,
            },
            ModelStreamEvent::TextDelta { text },
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: Usage {
                    input_tokens: 100,
                    output_tokens: 30,
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

fn history(large: bool) -> Vec<Message> {
    (0..3)
        .flat_map(|index| {
            [
                Message::User(UserMessage {
                    uuid: format!("original-user-{index}").into(),
                    question_answer: None,
                    content: vec![text(format!("Preserve the task requirement {index}."))],
                    is_meta: None,
                }),
                Message::Assistant(AssistantMessage {
                    uuid: format!("original-assistant-{index}").into(),
                    model: MODEL.into(),
                    content: vec![text(if large {
                        "m".repeat(400_000)
                    } else {
                        "Recorded progress.".into()
                    })],
                    usage: None,
                }),
            ]
        })
        .collect()
}

struct Fixture {
    process: RuntimeProcessContext,
    session: Arc<Session>,
    requests: Requests,
    target_reply: Reply,
    dir: std::path::PathBuf,
}

impl Fixture {
    fn new(reply: Reply, large: bool, automatic: bool) -> Self {
        Self::with_history(reply, history(large), automatic)
    }

    fn with_history(reply: Reply, initial_history: Vec<Message>, automatic: bool) -> Self {
        Self::with_history_and_budget(reply, initial_history, automatic, None)
    }

    fn with_history_and_budget(
        reply: Reply,
        initial_history: Vec<Message>,
        automatic: bool,
        auto_window: Option<u64>,
    ) -> Self {
        let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
        let dir =
            std::env::temp_dir().join(format!("klaus-context-migration-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let requests = Requests::default();
        let mut options = agent_options(process.clone());
        options.provider = Provider::new("previous-source", 500_000, &requests, reply);
        options.cwd = Some(dir.to_string_lossy().into_owned());
        options.config = Some(json!({
            "model":MODEL, "contextWindow":500_000, "prewarm":false, "fork":false, "thinking":false,
            "memory":{"enabled":false}, "autoMemory":false, "autoDream":false, "memoryRecall":false,
            "loopDetection":false, "autoCompaction":automatic,
            "approval_policy":"never", "sandbox_mode":"danger-full-access"
        }));
        if let Some(window) = auto_window {
            options.config.as_mut().unwrap()["autoCompactWindow"] = json!(window);
        }
        let session = Agent::new(options).start_session(StartSessionOptions {
            user_config_dir: Some(dir.to_string_lossy().into_owned().into()),
            initial_history: Some(initial_history),
            base_instructions: Some("Continue the user's existing task.".into()),
            ..Default::default()
        });
        Self {
            process,
            session,
            requests,
            target_reply: if matches!(reply, Reply::Insufficient) {
                Reply::Insufficient
            } else {
                Reply::Success
            },
            dir,
        }
    }

    fn select(&self, name: &str, window: u64) {
        let selected = resolve_config(
            json!({ "model":MODEL, "contextWindow":window, "prewarm":false,
            "thinking":false,
            "modelSource":{"source":name,"modelId":MODEL,"selection":format!("{name}:{MODEL}")} }),
        )
        .unwrap();
        self.session
            .reconfigure_inference(
                Provider::new(name, window, &self.requests, self.target_reply),
                selected,
                None,
                None,
                self.session.model_revision(),
            )
            .unwrap();
    }

    async fn run(&self) -> Vec<EngineEvent> {
        let mut turn = self.session.run_streamed(
            Input::Text("Continue the existing task without losing its requirements.".into()),
            TurnOptions::default(),
        );
        let mut events = Vec::new();
        while let Some(event) = turn.events.next().await {
            events.push(event);
        }
        events
    }

    fn sources(&self) -> Vec<String> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .map(|(source, _)| source.clone())
            .collect()
    }

    async fn close(self) {
        self.session.shutdown_and_wait().await.unwrap();
        self.process.shutdown().await.unwrap();
        std::fs::remove_dir_all(self.dir).unwrap();
    }
}

#[tokio::test]
async fn smaller_window_uses_previous_source_before_the_first_target_request() {
    let f = Fixture::new(Reply::Success, true, true);
    let before = f.session.context_snapshot().total_tokens;
    f.select("target-source", 256_000);
    assert_eq!(f.session.context_snapshot().context_window, 256_000);
    let events = f.run().await;
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{events:?}"
    );
    assert_eq!(f.sources(), ["previous-source", "target-source"]);
    let requests = f.requests.lock().unwrap().clone();
    assert_eq!(requests[0].1.model, MODEL);
    assert_eq!(requests[0].1.max_output_tokens, 4_000);
    assert_eq!(
        requests[1].1.model, MODEL,
        "same model ids must still use the correct provider"
    );
    let target_tokens = estimate_context_tokens(&requests[1].1.messages)
        + estimate_prefix_tokens(&requests[1].1.system, &requests[1].1.tools);
    eprintln!("previous_window=500000 target_window=256000 original_tokens={before} target_request_tokens={target_tokens}");
    assert!(before > 256_000);
    assert!(target_tokens < 256_000 * 95 / 100);
    assert!(requests[1].1.messages.iter().any(|message| matches!(message, Message::CompactBoundary(boundary) if boundary.summary.contains(SUMMARY))));
    assert!(requests[1].1.messages.iter().any(|message| matches!(message, Message::User(user) if user.content.iter().any(|block| matches!(block, ContentBlock::Text(text) if text.text.contains("Continue the existing task"))))));
    assert!(events
        .iter()
        .any(|event| matches!(event, EngineEvent::Compaction { .. })));
    let next = f.run().await;
    assert!(
        !next
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{next:?}"
    );
    assert_eq!(
        f.sources(),
        ["previous-source", "target-source", "target-source"]
    );
    f.close().await;
}

#[tokio::test]
async fn rapid_unused_selections_keep_the_original_large_source() {
    let f = Fixture::new(Reply::Success, true, true);
    f.select("intermediate-source", 256_000);
    f.select("smallest-source", 128_000);
    let events = f.run().await;
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{events:?}"
    );
    assert_eq!(f.sources(), ["previous-source", "smallest-source"]);
    f.close().await;
}

#[tokio::test]
async fn a_fitting_history_does_not_spend_a_summary_request() {
    let f = Fixture::new(Reply::Success, false, true);
    f.select("target-source", 256_000);
    let events = f.run().await;
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{events:?}"
    );
    assert_eq!(f.sources(), ["target-source"]);
    f.close().await;
}

#[tokio::test]
async fn a_failed_summary_retains_history_and_exposes_its_failure() {
    let f = Fixture::new(Reply::Failure, true, true);
    f.select("target-source", 256_000);
    let events = f.run().await;
    assert!(!f.sources().is_empty());
    assert!(f.sources().iter().all(|source| source == "previous-source"));
    assert!(events.iter().any(|event| matches!(event, EngineEvent::TurnFailed { error, .. } if error.message.contains("Controlled summary timeout"))), "{events:?}");
    assert!(f.session.history().iter().any(|message| matches!(message, Message::Assistant(assistant) if assistant.uuid == "original-assistant-0")));
    assert!(!events
        .iter()
        .any(|event| matches!(event, EngineEvent::Compaction { .. })));
    f.close().await;
}

#[tokio::test]
async fn cancellation_cannot_commit_a_summary_or_call_the_smaller_model() {
    let f = Fixture::new(Reply::Cancel, true, true);
    f.select("target-source", 256_000);
    let events = f.run().await;
    assert!(
        events.iter().any(|event| matches!(
            event,
            EngineEvent::TurnCompleted { .. } | EngineEvent::TurnFailed { .. }
        )),
        "A cancelled migration must settle the turn: {events:?}"
    );
    assert!(!f.sources().is_empty());
    assert!(f.sources().iter().all(|source| source == "previous-source"));
    assert!(!events
        .iter()
        .any(|event| matches!(event, EngineEvent::Compaction { .. })));
    assert!(f.session.history().iter().any(|message| matches!(message, Message::Assistant(assistant) if assistant.uuid == "original-assistant-0")));
    f.close().await;
}

#[tokio::test]
async fn disabled_compaction_blocks_an_oversized_downshift_without_a_provider_call() {
    let f = Fixture::new(Reply::Success, true, false);
    f.select("target-source", 256_000);
    let events = f.run().await;
    assert!(f.sources().is_empty());
    assert!(events.iter().any(|event| matches!(event, EngineEvent::TurnFailed { error, .. } if error.message.contains("Automatic context compaction is disabled"))), "{events:?}");
    f.close().await;
}

#[tokio::test]
async fn a_completed_summary_is_committed_before_target_provider_admission() {
    let f = Fixture::new(Reply::Insufficient, true, true);
    f.select("target-source", 256_000);
    let events = f.run().await;
    let requests = f.requests.lock().unwrap().clone();
    assert!(!requests.is_empty());
    assert_eq!(requests[0].0, "previous-source");
    let ordinary: Vec<_> = requests
        .iter()
        .filter(|(_, request)| request.skip_cache_write != Some(true))
        .collect();
    assert_eq!(ordinary.len(), 1);
    assert_eq!(ordinary[0].0, "target-source");
    assert!(ordinary[0]
        .1
        .messages
        .iter()
        .any(|message| matches!(message,
        Message::CompactBoundary(boundary) if boundary.summary.len_utf16() >= 1_100_000)));
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{events:?}"
    );
    assert!(events
        .iter()
        .any(|event| matches!(event, EngineEvent::Compaction { .. })));
    assert!(f.session.history().iter().any(|message| matches!(message,
        Message::CompactBoundary(boundary) if boundary.summary.len_utf16() >= 1_100_000)));
    assert!(!f.session.history().iter().any(|message| matches!(message,
        Message::Assistant(assistant) if assistant.uuid == "original-assistant-0")));
    for index in 0..3 {
        let id = format!("original-user-{index}");
        assert!(f.session.history().iter().any(|message| matches!(message,
            Message::User(user) if user.uuid == id.as_str())));
    }
    f.close().await;
}

fn near_limit_history() -> Vec<Message> {
    let mut initial = history(false);
    if let Some(Message::Assistant(assistant)) = initial.last_mut() {
        assistant.usage = Some(Usage {
            input_tokens: 240_000,
            output_tokens: 0,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
            total_tokens: Some(240_000),
        });
    }
    initial
}

#[tokio::test]
async fn disabled_compaction_allows_history_between_the_trigger_and_hard_budget() {
    let f = Fixture::with_history(Reply::Failure, near_limit_history(), false);
    f.select("target-source", 256_000);
    let events = f.run().await;
    assert_eq!(f.sources(), ["target-source"]);
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{events:?}"
    );
    f.close().await;
}

#[tokio::test]
async fn a_failed_foreground_summary_never_falls_through_to_the_target_model() {
    let f = Fixture::with_history(Reply::Failure, near_limit_history(), true);
    f.select("target-source", 256_000);
    let events = f.run().await;
    assert!(!f.sources().is_empty());
    assert!(f.sources().iter().all(|source| source == "previous-source"));
    assert!(
        events.iter().any(
            |event| matches!(event, EngineEvent::TurnFailed { error, .. }
        if error.message.contains("Controlled summary timeout"))
        ),
        "{events:?}"
    );
    assert!(f.session.history().iter().any(
        |message| matches!(message, Message::Assistant(a) if a.uuid == "original-assistant-0")
    ));
    f.close().await;
}

#[tokio::test]
async fn a_zero_automatic_threshold_does_not_force_an_unnecessary_summary() {
    let f = Fixture::new(Reply::Failure, false, true);
    f.select("target-source", 12_000);
    let events = f.run().await;
    assert_eq!(f.sources(), ["target-source"]);
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{events:?}"
    );
    f.close().await;
}

#[tokio::test]
async fn a_staged_model_selection_keeps_the_previous_summary_model() {
    let f = Fixture::new(Reply::Success, true, true);
    f.session.set_model("selected-small").unwrap();
    let events = f.run().await;
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{events:?}"
    );
    let requests = f.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].1.model, MODEL);
    assert_eq!(requests[1].1.model, "selected-small");
    f.close().await;
}

#[tokio::test]
async fn an_earlier_session_budget_summarizes_on_the_selected_source() {
    let mut initial = history(false);
    if let Some(Message::Assistant(assistant)) = initial.last_mut() {
        assistant.usage = Some(Usage {
            input_tokens: 200_000,
            output_tokens: 0,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
            total_tokens: Some(200_000),
        });
    }
    let fixture = Fixture::with_history_and_budget(Reply::Failure, initial, true, Some(100_000));
    assert!(fixture.session.context_snapshot().total_tokens >= 200_000);
    fixture.select("target-source", 256_000);
    assert_eq!(
        fixture.session.context_snapshot().compact_threshold_tokens,
        100_000
    );
    let events = fixture.run().await;
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{events:?}"
    );
    let requests = fixture.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|(source, _)| source == "target-source"));
    assert_eq!(requests[0].1.skip_cache_write, Some(true));
    assert_ne!(requests[1].1.skip_cache_write, Some(true));
    assert!(requests[1]
        .1
        .messages
        .iter()
        .any(|message| matches!(message, Message::CompactBoundary(_))));
    fixture.close().await;
}
