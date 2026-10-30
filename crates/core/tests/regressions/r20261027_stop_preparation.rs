//! Defect: Stop waited for request preparation that did not observe cancellation.
//! Root cause: prompt and turn-entry hook futures were awaited before the loop's abort check.
//! Work item 20261027: cancel pending preparation before publishing interruption and accepting a successor.

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};
use async_trait::async_trait;
use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::hooks::types::{
    Hook, HookEvent, HookFn, HookHandler, HookInput, HookOutcome,
};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::TerminalReason;
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

const WAIT: Duration = Duration::from_secs(10);
const STOP_BUDGET: Duration = Duration::from_secs(2);
const OLD_INPUT: &str = "Stop this turn while its request is being prepared";
const NEW_INPUT: &str = "Run this immediate successor exactly once";

#[derive(Clone, Copy, Debug)]
enum Preparation {
    SystemPrompt,
    SessionStart,
    UserPromptSubmit,
}

#[derive(Default)]
struct HeldPreparation {
    entered: Notify,
    release: Notify,
    calls: AtomicUsize,
    dropped: AtomicUsize,
    returned: AtomicUsize,
}

struct DropPreparation(Arc<HeldPreparation>);

impl Drop for DropPreparation {
    fn drop(&mut self) {
        self.0.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

impl HeldPreparation {
    async fn hold_first(self: &Arc<Self>) {
        if self.calls.fetch_add(1, Ordering::SeqCst) != 0 {
            return;
        }
        let _drop = DropPreparation(self.clone());
        self.entered.notify_one();
        // Only the fixture may release the first preparation. Cancellation must
        // retire its future without waiting for this independent barrier.
        self.release.notified().await;
        self.returned.fetch_add(1, Ordering::SeqCst);
    }
}

struct HeldHook(Arc<HeldPreparation>);

#[async_trait]
impl HookFn for HeldHook {
    async fn run(&self, _: HookInput) -> HookOutcome {
        self.0.hold_first().await;
        HookOutcome::default()
    }
}

struct CountingProvider {
    info: ProviderInfo,
    calls: AtomicUsize,
}

impl ModelProvider for CountingProvider {
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
            max_output_tokens: 8_192,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }

    fn stream(&self, _: ModelRequest, _: AbortSignal) -> ModelStream {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(futures::stream::iter([
            ModelStreamEvent::MessageStart {
                model: "preparation-test".into(),
            },
            ModelStreamEvent::TextDelta {
                text: "The successor completed.".into(),
            },
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: Usage {
                    input_tokens: 10,
                    output_tokens: 4,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    total_tokens: None,
                },
            },
        ]))
    }
}

fn user_messages(history: &[Message], input: &str) -> usize {
    history
        .iter()
        .filter(|message| {
            matches!(message, Message::User(user) if user.is_meta != Some(true)
                && user.content.iter().any(|block| matches!(block,
                    ContentBlock::Text(text) if text.text == input)))
        })
        .count()
}

async fn stop_preparation(stage: Preparation) {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let root = tempfile::tempdir().unwrap();
    let held = Arc::new(HeldPreparation::default());
    let provider = Arc::new(CountingProvider {
        info: ProviderInfo {
            name: "preparation-test".into(),
            default_model: "preparation-test".into(),
            base_url: None,
            env_key: None,
        },
        calls: AtomicUsize::new(0),
    });
    let mut options = agent_options(process.clone());
    options.provider = provider.clone();
    options.config = Some(json!({
        "model":"preparation-test", "prewarm":false, "goal":false,
        "autoMemory":false, "autoDream":false,
        "approval_policy":"never", "sandbox_mode":"danger-full-access"
    }));
    match stage {
        Preparation::SystemPrompt => {
            let held = held.clone();
            options.system_prompt_provider = Some(Arc::new(move || {
                let held = held.clone();
                Box::pin(async move {
                    held.hold_first().await;
                    Vec::new()
                })
            }));
        }
        Preparation::SessionStart | Preparation::UserPromptSubmit => {
            options.hooks = vec![Hook {
                event: match stage {
                    Preparation::SessionStart => HookEvent::SessionStart,
                    _ => HookEvent::UserPromptSubmit,
                },
                matcher: None,
                handler: HookHandler::Fn(Arc::new(HeldHook(held.clone()))),
                is_async: false,
            }];
        }
    }
    let directory = root.path().to_string_lossy().into_owned();
    let session = Agent::new(options).start_session(StartSessionOptions {
        cwd: Some(directory.clone().into()),
        user_config_dir: Some(directory.into()),
        ..Default::default()
    });
    let turn = session.run_streamed(Input::Text(OLD_INPUT.into()), TurnOptions::default());
    let mut task = tokio::spawn(async move { turn.events.collect::<Vec<_>>().await });
    tokio::time::timeout(WAIT, held.entered.notified())
        .await
        .expect("the first turn must enter its request preparation");
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);

    let started = tokio::time::Instant::now();
    session.interrupt_by_user();
    let result = tokio::time::timeout(STOP_BUDGET, &mut task).await;
    let elapsed = started.elapsed();
    let dropped_at_stop = held.dropped.load(Ordering::SeqCst);
    let returned_at_stop = held.returned.load(Ordering::SeqCst);
    let (finished_while_blocked, interrupted) = match result {
        Ok(result) => (true, result.unwrap()),
        Err(_) => {
            // Preserve baseline failure evidence while releasing every owned
            // future before the assertions can unwind the runtime.
            held.release.notify_one();
            let events = tokio::time::timeout(WAIT, &mut task)
                .await
                .expect("releasing preparation must drain the baseline turn")
                .unwrap();
            (false, events)
        }
    };
    let calls_at_stop = provider.calls.load(Ordering::SeqCst);
    let history_at_stop = session.history();
    let guidance_at_stop = history_at_stop
        .iter()
        .filter(|message| {
            matches!(message, Message::User(user) if user.is_meta == Some(true)
                && user.uuid.try_to_utf8().unwrap().starts_with("turn-interrupted-"))
        })
        .count();

    // The completed event stream is the handoff boundary: no polling, delay,
    // repeated submission, or release of the old preparation precedes this run.
    let successor = tokio::time::timeout(
        WAIT,
        session
            .run_streamed(Input::Text(NEW_INPUT.into()), TurnOptions::default())
            .events
            .collect::<Vec<_>>(),
    )
    .await
    .expect("the immediate successor must finish");
    held.release.notify_one();
    tokio::task::yield_now().await;
    let history = session.history();
    let calls_after_successor = provider.calls.load(Ordering::SeqCst);
    let returned_after_release = held.returned.load(Ordering::SeqCst);
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();

    eprintln!("stage={stage:?}; finished_while_blocked={finished_while_blocked}; elapsed={elapsed:?}; dropped_at_stop={dropped_at_stop}; returned_at_stop={returned_at_stop}; calls_at_stop={calls_at_stop}; calls_after_successor={calls_after_successor}; guidance_at_stop={guidance_at_stop}; old_messages={}; new_messages={}; returned_after_release={returned_after_release}", user_messages(&history, OLD_INPUT), user_messages(&history, NEW_INPUT));
    assert!(
        finished_while_blocked && elapsed < STOP_BUDGET,
        "Stop must finish while request preparation remains blocked: stage={stage:?}, elapsed={elapsed:?}"
    );
    assert_eq!(
        dropped_at_stop, 1,
        "retire the old preparation before Stop finishes"
    );
    assert_eq!(
        returned_at_stop, 0,
        "Stop must not wait for the external barrier"
    );
    assert_eq!(
        calls_at_stop, 0,
        "the interrupted preparation must never call the model"
    );
    assert_eq!(
        guidance_at_stop, 1,
        "interruption guidance must precede the successor"
    );
    assert_eq!(user_messages(&history_at_stop, OLD_INPUT), 1);
    assert_eq!(user_messages(&history, OLD_INPUT), 1);
    assert_eq!(user_messages(&history, NEW_INPUT), 1);
    assert_eq!(
        calls_after_successor, 1,
        "only the immediate successor may call the model"
    );
    assert_eq!(
        returned_after_release, 0,
        "releasing an old barrier cannot resume retired work"
    );
    assert_eq!(
        interrupted
            .iter()
            .filter(|event| matches!(
                event,
                EngineEvent::TurnCompleted {
                    stop_reason: TerminalReason::Aborted,
                    ..
                }
            ))
            .count(),
        1,
        "the original turn must publish exactly one interrupted terminal"
    );
    assert!(!interrupted
        .iter()
        .any(|event| matches!(event, EngineEvent::TurnFailed { .. })));
    assert_eq!(
        successor
            .iter()
            .filter(|event| matches!(
                event,
                EngineEvent::TurnCompleted {
                    stop_reason: TerminalReason::Completed,
                    ..
                }
            ))
            .count(),
        1,
        "the successor must publish exactly one successful terminal"
    );
    assert!(!successor.iter().any(|event| matches!(
        event,
        EngineEvent::TurnFailed { .. }
            | EngineEvent::TurnCompleted {
                stop_reason: TerminalReason::Aborted,
                ..
            }
    )));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_pending_system_prompt_preserves_immediate_successor() {
    stop_preparation(Preparation::SystemPrompt).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_pending_session_start_hook_preserves_immediate_successor() {
    stop_preparation(Preparation::SessionStart).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_pending_prompt_submit_hook_preserves_immediate_successor() {
    stop_preparation(Preparation::UserPromptSubmit).await;
}
