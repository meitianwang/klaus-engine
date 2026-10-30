//! Defect: maintenance summaries raced turn completion and discarded completed answers.
//! Root cause: speculative replacement and independent failure policies bypassed the turn lifecycle.
//! Fix: 5dea2d9b; create and commit source-preserving context checkpoints.

use async_trait::async_trait;
use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::deps::{CompactionResult, Compactor};
use klaus_agent_core::engine::message_queue::QueuePriority;
use klaus_agent_core::engine::session::{EnqueueOptions, Input, Session, TurnOptions};
use klaus_agent_core::hooks::types::{
    Hook, HookEvent, HookFn, HookHandler, HookInput, HookOutcome, HookResult,
};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::config::ResolvedConfig;
use klaus_agent_core::schemas::content::{ContentBlock, TextBlock};
use klaus_agent_core::schemas::message::{CompactBoundaryMessage, Message, Usage, UserMessage};
use klaus_agent_core::schemas::rollout::RolloutItem;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::projection::project_for_prompt;
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::TerminalReason;
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::Notify;

#[derive(Default)]
struct State {
    trace: Vec<&'static str>,
    summaries: usize,
    fail_summary: bool,
    pause_summary: bool,
    emit_completed_output: bool,
    stop_after_compact: bool,
    enqueue_at_stop: bool,
    continue_remaining: usize,
}

struct Provider {
    info: ProviderInfo,
    state: Arc<Mutex<State>>,
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
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
            supports_image_detail_original: false,
        }
    }
    fn stream(&self, _: ModelRequest, _: AbortSignal) -> ModelStream {
        let mut state = self.state.lock().unwrap();
        state.trace.push("normal");
        let stop_reason = if state.continue_remaining > 0 {
            state.continue_remaining -= 1;
            ModelStopReason::Continue
        } else {
            ModelStopReason::EndTurn
        };
        Box::pin(futures::stream::iter([
            ModelStreamEvent::TextDelta {
                text: "Completed answer.".into(),
            },
            ModelStreamEvent::MessageStop {
                stop_reason,
                usage: Usage {
                    input_tokens: 80_000,
                    output_tokens: 100,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    total_tokens: None,
                },
            },
        ]))
    }
}

struct Summary {
    state: Arc<Mutex<State>>,
    started: Arc<Notify>,
    release: Arc<Notify>,
    output: Mutex<Option<klaus_agent_core::context::compact::full_compact::CompactionOutputSink>>,
}

impl Summary {
    async fn run(&self, messages: &[Message]) -> CompactionResult {
        let (call, pause, fail, emit_output) = {
            let mut state = self.state.lock().unwrap();
            state.summaries += 1;
            state.trace.push("summary");
            (
                state.summaries,
                state.pause_summary,
                state.fail_summary,
                state.emit_completed_output,
            )
        };
        if emit_output {
            let output = { self.output.lock().unwrap().clone() };
            if let Some(output) = output {
                output(Message::Assistant(klaus_agent_core::schemas::message::AssistantMessage {
                    uuid: "completed-summary-output".into(), model: "lifecycle-model".into(), usage: None,
                    content: vec![ContentBlock::Text(TextBlock {
                        text: "Completed maintenance summary.".into(), citations: None,
                        harness_envelope: Some(klaus_agent_core::schemas::content::HarnessEnvelope::CompactionOutput),
                        message_id: Some("completed-summary-output".into()), phase: None,
                    })],
                })).await;
            }
        }
        if pause {
            self.started.notify_one();
            self.release.notified().await;
        }
        CompactionResult {
            messages: if fail {
                messages.to_vec()
            } else {
                vec![Message::CompactBoundary(CompactBoundaryMessage {
                    uuid: format!("checkpoint-{call}").into(),
                    summary: "Preserved task.".into(),
                    native_compaction: None,
                })]
            },
            freed_tokens: 100,
            compacted: !fail,
            reason: None,
            diagnostics: Vec::new(),
        }
    }
}

#[async_trait]
impl Compactor for Summary {
    fn bind_output_observer(
        &self,
        observer: Option<klaus_agent_core::context::compact::full_compact::CompactionOutputSink>,
    ) {
        *self.output.lock().unwrap() = observer;
    }
    async fn maybe_compact(
        &self,
        messages: &[Message],
        _: u64,
        _: &ResolvedConfig,
        _: Option<&RuntimeString>,
    ) -> CompactionResult {
        self.run(messages).await
    }
    async fn reactive_compact(
        &self,
        messages: &[Message],
        _: &ResolvedConfig,
        _: Option<&RuntimeString>,
    ) -> CompactionResult {
        self.run(messages).await
    }
    async fn post_turn_compact(
        &self,
        messages: &[Message],
        _: &ResolvedConfig,
        _: Option<&RuntimeString>,
    ) -> CompactionResult {
        self.run(messages).await
    }
}

struct LifecycleHook {
    state: Arc<Mutex<State>>,
    session: Arc<Mutex<Weak<Session>>>,
}

#[async_trait]
impl HookFn for LifecycleHook {
    async fn run(&self, input: HookInput) -> HookOutcome {
        let (enqueue, abort) = {
            let mut state = self.state.lock().unwrap();
            state.trace.push(match input.event {
                HookEvent::Stop => "stop",
                HookEvent::PreCompact => "pre",
                HookEvent::PostCompact => "post",
                _ => panic!("unexpected fixture hook"),
            });
            let enqueue =
                input.event == HookEvent::Stop && std::mem::take(&mut state.enqueue_at_stop);
            (
                enqueue,
                input.event == HookEvent::PostCompact && state.stop_after_compact,
            )
        };
        let session = self.session.lock().unwrap().upgrade().unwrap();
        if enqueue {
            session
                .enqueue(
                    Input::Text("Keep this next request queued.".into()),
                    EnqueueOptions {
                        priority: Some(QueuePriority::Later),
                        ..Default::default()
                    },
                )
                .unwrap();
        }
        if input.event == HookEvent::PostCompact {
            assert!(
                has_checkpoint(&session.history()),
                "PostCompact must observe the committed replacement"
            );
        }
        HookOutcome {
            result: abort.then_some(HookResult::FailedAbort),
            ..Default::default()
        }
    }
}

struct Fixture {
    session: Arc<Session>,
    process: RuntimeProcessContext,
    store: Arc<JsonlSessionStore>,
    root: std::path::PathBuf,
    state: Arc<Mutex<State>>,
    started: Arc<Notify>,
    release: Arc<Notify>,
}

impl Fixture {
    async fn new(state: State, post_threshold: u64, auto_window: u64) -> Self {
        let process = RuntimeProcessContext::initialize(
            super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
        )
        .unwrap();
        let root = std::env::temp_dir().join(format!(
            "klaus-compaction-lifecycle-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let state = Arc::new(Mutex::new(state));
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let holder = Arc::new(Mutex::new(Weak::new()));
        let mut options =
            super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
        options.provider = Arc::new(Provider {
            info: ProviderInfo {
                name: "lifecycle-provider".into(),
                default_model: "lifecycle-model".into(),
                base_url: None,
                env_key: None,
            },
            state: state.clone(),
        });
        options.compactor = Some(Arc::new(Summary {
            state: state.clone(),
            started: started.clone(),
            release: release.clone(),
            output: Mutex::new(None),
        }));
        options.tools = Some(Vec::new());
        options.cwd = Some(root.to_string_lossy().into_owned());
        options.config = Some(serde_json::json!({
            "model":"lifecycle-model", "contextWindow":100_000, "autoCompactWindow":auto_window,
            "postTurnCompactThresholdPercent":post_threshold, "maxOutputTokens":1_000,
            "outputReserveTokens":0, "prewarm":false, "thinking":false,
            "autoMemory":false, "autoDream":false, "memoryRecall":false,
            "loopDetection":false, "approval_policy":"never", "sandbox_mode":"danger-full-access"
        }));
        for event in [
            HookEvent::Stop,
            HookEvent::PreCompact,
            HookEvent::PostCompact,
        ] {
            options.hooks.push(Hook {
                event,
                matcher: None,
                handler: HookHandler::Fn(Arc::new(LifecycleHook {
                    state: state.clone(),
                    session: holder.clone(),
                })),
                is_async: false,
            });
        }
        let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(root.join("sessions").to_string_lossy().into_owned()),
            archive_dir: None,
        }));
        let session = Agent::new(options)
            .open_session(
                store.clone(),
                StartSessionOptions {
                    cwd: Some(root.to_string_lossy().into_owned().into()),
                    user_config_dir: Some(root.to_string_lossy().into_owned().into()),
                    base_instructions: Some("Finish the user's task.".into()),
                    initial_history: Some(vec![Message::User(UserMessage {
                        uuid: "original-task".into(),
                        content: vec![ContentBlock::Text(TextBlock {
                            text: "Preserve this original requirement.".into(),
                            citations: None,
                            harness_envelope: None,
                            message_id: None,
                            phase: None,
                        })],
                        is_meta: None,
                        question_answer: None,
                    })]),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        *holder.lock().unwrap() = Arc::downgrade(&session);
        Self {
            session,
            process,
            store,
            root,
            state,
            started,
            release,
        }
    }
    async fn run(&self) -> Vec<EngineEvent> {
        self.session
            .run_streamed(
                Input::Text("Complete the task.".into()),
                TurnOptions::default(),
            )
            .events
            .collect()
            .await
    }
    async fn durable(&self) -> Vec<RolloutItem> {
        self.session.flush_rollout().await.unwrap();
        self.store
            .read_thread(&self.session.id.try_to_utf8().unwrap())
            .await
            .unwrap()
            .raw_items
    }
    async fn close(self) {
        self.release.notify_waiters();
        self.session.shutdown_and_wait().await.unwrap();
        self.process.shutdown().await.unwrap();
        std::fs::remove_dir_all(self.root).unwrap();
    }
}

fn has_checkpoint(messages: &[Message]) -> bool {
    messages
        .iter()
        .any(|item| matches!(item, Message::CompactBoundary(_)))
}

fn has_answer(messages: &[Message]) -> bool {
    messages.iter().any(|item| matches!(item, Message::Assistant(assistant)
        if assistant.content.iter().any(|block| matches!(block, ContentBlock::Text(text) if text.text == "Completed answer."))))
}

#[tokio::test]
async fn post_turn_checkpoint_commits_before_completion_and_after_stop_hooks() {
    let fixture = Fixture::new(State::default(), 50, 95_000).await;
    let events = fixture.run().await;
    let compacted = events
        .iter()
        .position(|event| matches!(event, EngineEvent::Compaction { .. }))
        .unwrap();
    let completed = events
        .iter()
        .position(|event| {
            matches!(
                event,
                EngineEvent::TurnCompleted {
                    stop_reason: TerminalReason::Completed,
                    ..
                }
            )
        })
        .unwrap();
    assert!(compacted < completed);
    assert_eq!(
        fixture.state.lock().unwrap().trace,
        ["normal", "stop", "pre", "summary", "post"]
    );
    let durable = fixture.durable().await;
    assert_eq!(project_for_prompt(&durable), fixture.session.history());
    assert!(has_checkpoint(&fixture.session.history()));
    fixture.close().await;
}

#[tokio::test]
async fn failed_post_turn_summary_preserves_the_completed_answer_and_has_no_post_hook() {
    let fixture = Fixture::new(
        State {
            fail_summary: true,
            ..Default::default()
        },
        50,
        95_000,
    )
    .await;
    let events = fixture.run().await;
    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::TurnCompleted {
            stop_reason: TerminalReason::Completed,
            ..
        }
    )));
    assert!(!events.iter().any(|event| matches!(
        event,
        EngineEvent::TurnFailed { .. } | EngineEvent::Compaction { .. }
    )));
    assert!(has_answer(&fixture.session.history()));
    assert!(!has_checkpoint(&fixture.session.history()));
    let durable = fixture.durable().await;
    assert!(has_answer(&project_for_prompt(&durable)));
    assert_eq!(
        fixture.state.lock().unwrap().trace,
        ["normal", "stop", "pre", "summary"]
    );
    fixture.close().await;
}

#[tokio::test]
async fn cancelling_post_turn_summary_preserves_the_answer_without_a_checkpoint() {
    let fixture = Fixture::new(
        State {
            pause_summary: true,
            ..Default::default()
        },
        50,
        95_000,
    )
    .await;
    let session = fixture.session.clone();
    let running = tokio::spawn(async move {
        session
            .run_streamed(
                Input::Text("Complete the task.".into()),
                TurnOptions::default(),
            )
            .events
            .collect::<Vec<_>>()
            .await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        fixture.started.notified(),
    )
    .await
    .unwrap();
    fixture.session.interrupt();
    let events = running.await.unwrap();
    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::TurnCompleted {
            stop_reason: TerminalReason::Aborted,
            ..
        }
    )));
    assert!(has_answer(&fixture.session.history()));
    assert!(!has_checkpoint(&fixture.session.history()));
    assert!(!fixture
        .durable()
        .await
        .iter()
        .any(|item| matches!(item, RolloutItem::Compacted(_))));
    fixture.close().await;
}

#[tokio::test]
async fn post_compact_stop_aborts_after_retaining_the_committed_checkpoint() {
    let fixture = Fixture::new(
        State {
            stop_after_compact: true,
            ..Default::default()
        },
        50,
        95_000,
    )
    .await;
    let events = fixture.run().await;
    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::TurnCompleted {
            stop_reason: TerminalReason::Aborted,
            ..
        }
    )));
    assert!(has_checkpoint(&fixture.session.history()));
    let durable = fixture.durable().await;
    assert_eq!(project_for_prompt(&durable), fixture.session.history());
    fixture.close().await;
}

#[tokio::test]
async fn pending_next_input_skips_post_turn_compaction() {
    let fixture = Fixture::new(
        State {
            enqueue_at_stop: true,
            ..Default::default()
        },
        50,
        95_000,
    )
    .await;
    let events = fixture.run().await;
    assert!(!events
        .iter()
        .any(|event| matches!(event, EngineEvent::Compaction { .. })));
    fixture.session.await_queue_durability().await.unwrap();
    assert!(!fixture.session.queued_messages().is_empty());
    fixture.close().await;
}

#[tokio::test]
async fn repeated_successful_step_compactions_do_not_trip_a_refill_breaker() {
    let fixture = Fixture::new(
        State {
            continue_remaining: 6,
            ..Default::default()
        },
        0,
        70_000,
    )
    .await;
    let events = fixture.run().await;
    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::TurnCompleted {
            stop_reason: TerminalReason::Completed,
            ..
        }
    )));
    assert!(!events
        .iter()
        .any(|event| matches!(event, EngineEvent::TurnFailed { .. })));
    assert_eq!(fixture.state.lock().unwrap().summaries, 6);
    fixture.close().await;
}

#[tokio::test]
async fn cancelled_post_turn_operation_does_not_publish_completed_summary_items() {
    let fixture = Fixture::new(
        State {
            pause_summary: true,
            emit_completed_output: true,
            ..Default::default()
        },
        50,
        95_000,
    )
    .await;
    let session = fixture.session.clone();
    let running = tokio::spawn(async move {
        session
            .run_streamed(
                Input::Text("Complete the task.".into()),
                TurnOptions::default(),
            )
            .events
            .collect::<Vec<_>>()
            .await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        fixture.started.notified(),
    )
    .await
    .unwrap();
    fixture.session.interrupt();
    let events = running.await.unwrap();
    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::TurnCompleted {
            stop_reason: TerminalReason::Aborted,
            ..
        }
    )));
    let is_output = |message: &Message| matches!(message, Message::Assistant(assistant) if assistant.uuid == "completed-summary-output");
    assert!(!fixture.session.history().iter().any(is_output));
    assert!(has_answer(&fixture.session.history()));
    let durable = fixture.durable().await;
    assert!(!durable
        .iter()
        .any(|item| matches!(item, RolloutItem::Message { message } if is_output(message))));
    assert!(!durable
        .iter()
        .any(|item| matches!(item, RolloutItem::Compacted(_))));
    fixture.close().await;
}
