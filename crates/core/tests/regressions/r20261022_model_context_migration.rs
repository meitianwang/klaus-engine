//! Defect: switching to a smaller-window model called the deselected model for
//! compaction, failing whenever the old model source was out of quota.
//! Work item: 2026-10-22 / 2026-11-13 target-only context downshift before admission.

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
        let is_summary = request.skip_cache_write == Some(true);
        self.requests
            .lock()
            .unwrap()
            .push((self.info.name.try_to_utf8().unwrap(), request.clone()));
        if is_summary && matches!(self.reply, Reply::Cancel) {
            signal.cancel();
            return Box::pin(futures::stream::empty());
        }
        if is_summary && matches!(self.reply, Reply::Failure) {
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
                    credential_recovery: None,
                },
            }]));
        }
        let text = if is_summary && matches!(self.reply, Reply::Insufficient) {
            "x".repeat(1_100_000).into()
        } else if is_summary {
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
        // The deselected source always fails so any call back to it breaks tests immediately.
        options.provider = Provider::new("previous-source", 500_000, &requests, Reply::Failure);
        options.cwd = Some(dir.to_string_lossy().into_owned());
        options.config = Some(json!({
            "model":MODEL, "contextWindow":500_000, "prewarm":false, "fork":false, "thinking":false,
            "memory":{"enabled":false}, "autoMemory":false, "autoDream":false, "memoryRecall":false,
            "loopDetection":false, "autoCompaction":automatic, "compactionStreamMaxRetries":0,
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
            target_reply: reply,
            dir,
        }
    }

    fn select(&self, name: &str, window: u64) {
        let selected = resolve_config(
            json!({ "model":MODEL, "contextWindow":window, "prewarm":false,
            "thinking":false, "compactionStreamMaxRetries":0,
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
async fn smaller_window_compacts_exclusively_on_target_source_before_normal_request() {
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
    assert!(
        f.sources().iter().all(|source| source == "target-source"),
        "deselected source must never be called: {:?}",
        f.sources()
    );
    let requests = f.requests.lock().unwrap().clone();
    assert!(requests.len() >= 2);
    for (_, req) in &requests[..requests.len() - 1] {
        assert_eq!(req.skip_cache_write, Some(true));
        assert!(estimate_context_tokens(&req.messages) < 256_000);
    }
    let final_req = &requests.last().unwrap().1;
    assert_ne!(final_req.skip_cache_write, Some(true));
    assert_eq!(final_req.model, MODEL);
    let target_tokens = estimate_context_tokens(&final_req.messages)
        + estimate_prefix_tokens(&final_req.system, &final_req.tools);
    assert!(before > 256_000);
    assert!(target_tokens < 256_000 * 95 / 100);
    assert!(final_req.messages.iter().any(|message| matches!(message, Message::CompactBoundary(boundary) if boundary.summary.contains(SUMMARY))));
    assert!(final_req.messages.iter().any(|message| matches!(message, Message::User(user) if user.content.iter().any(|block| matches!(block, ContentBlock::Text(text) if text.text.contains("Continue the existing task"))))));
    assert!(events
        .iter()
        .any(|event| matches!(event, EngineEvent::Compaction { .. })));
    let prior_count = f.sources().len();
    let next = f.run().await;
    assert!(
        !next
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{next:?}"
    );
    assert_eq!(f.sources().len(), prior_count + 1);
    assert_eq!(f.sources().last().unwrap(), "target-source");
    f.close().await;
}

#[tokio::test]
async fn rapid_unused_selections_compact_only_on_the_final_selected_source() {
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
    assert!(!f.sources().is_empty());
    assert!(f.sources().iter().all(|source| source == "smallest-source"));
    for (_, req) in f.requests.lock().unwrap().iter() {
        assert!(estimate_context_tokens(&req.messages) < 128_000);
    }
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
async fn a_failed_target_summary_retains_history_and_exposes_its_failure() {
    let f = Fixture::new(Reply::Failure, true, true);
    f.select("target-source", 256_000);
    let events = f.run().await;
    assert!(!f.sources().is_empty());
    assert!(f.sources().iter().all(|source| source == "target-source"));
    assert!(events.iter().any(|event| matches!(event, EngineEvent::TurnFailed { error, .. } if error.message.contains("Controlled summary timeout"))), "{events:?}");
    assert!(f.session.history().iter().any(|message| matches!(message, Message::Assistant(assistant) if assistant.uuid == "original-assistant-0")));
    assert!(!events
        .iter()
        .any(|event| matches!(event, EngineEvent::Compaction { .. })));
    f.close().await;
}

#[tokio::test]
async fn cancellation_cannot_commit_a_summary_or_send_an_ordinary_turn() {
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
    assert!(f.sources().iter().all(|source| source == "target-source"));
    assert!(f
        .requests
        .lock()
        .unwrap()
        .iter()
        .all(|(_, req)| req.skip_cache_write == Some(true)));
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
async fn a_completed_single_pass_summary_is_committed_before_target_provider_admission() {
    let f = Fixture::with_history(Reply::Insufficient, near_limit_history(), true);
    f.select("target-source", 256_000);
    let events = f.run().await;
    let requests = f.requests.lock().unwrap().clone();
    assert!(!requests.is_empty());
    assert!(requests.iter().all(|(source, _)| source == "target-source"));
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
    let f = Fixture::with_history(Reply::Success, near_limit_history(), false);
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
async fn a_failed_foreground_summary_never_falls_through_to_ordinary_inference() {
    let f = Fixture::with_history(Reply::Failure, near_limit_history(), true);
    f.select("target-source", 256_000);
    let events = f.run().await;
    assert_eq!(f.sources(), ["target-source"]);
    assert!(f
        .requests
        .lock()
        .unwrap()
        .iter()
        .all(|(_, req)| req.skip_cache_write == Some(true)));
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
    let f = Fixture::new(Reply::Success, false, true);
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
async fn a_staged_model_selection_compacts_exclusively_with_the_staged_model() {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let dir = std::env::temp_dir().join(format!("klaus-staged-downshift-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let requests = Requests::default();
    let mut options = agent_options(process.clone());
    options.provider = Provider::new("same-source", 500_000, &requests, Reply::Success);
    options.cwd = Some(dir.to_string_lossy().into_owned());
    options.config = Some(json!({
        "model":MODEL, "contextWindow":500_000, "prewarm":false, "fork":false, "thinking":false,
        "memory":{"enabled":false}, "autoMemory":false, "autoDream":false, "memoryRecall":false,
        "loopDetection":false, "autoCompaction":true, "compactionStreamMaxRetries":0,
        "approval_policy":"never", "sandbox_mode":"danger-full-access"
    }));
    let session = Agent::new(options).start_session(StartSessionOptions {
        user_config_dir: Some(dir.to_string_lossy().into_owned().into()),
        initial_history: Some(history(true)),
        base_instructions: Some("Continue the user's existing task.".into()),
        ..Default::default()
    });
    session.set_model("selected-small").unwrap();
    let events: Vec<_> = session
        .run_streamed(
            Input::Text("Continue the existing task.".into()),
            TurnOptions::default(),
        )
        .events
        .collect()
        .await;
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{events:?}"
    );
    let recorded = requests.lock().unwrap().clone();
    assert!(recorded.len() >= 2);
    assert!(
        recorded
            .iter()
            .all(|(_, request)| request.model == "selected-small"),
        "every summary and turn request must use the staged model: {recorded:?}"
    );
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
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
    let fixture = Fixture::with_history_and_budget(Reply::Success, initial, true, Some(100_000));
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
