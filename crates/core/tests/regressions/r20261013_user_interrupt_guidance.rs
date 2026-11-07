//! Defect: a user-interrupted turn left the model with no record of what the
//! interrupt did and did not stop, so the next turn acted on stale assumptions —
//! stopping an independent background shell or sub-agent unintentionally.
//!
//! Root cause: an aborted turn recorded only its terminal status; nothing told
//! the model the interrupt was user-initiated or what survived it.
//!
//! Fix scope: `Session::interrupt_by_user` marks the innermost live turn, and the
//! turn's terminal writer persists a model-visible, `is_meta` guidance message
//! BEFORE the `turn_lifecycle` record, then flushes it. Because it is a real
//! message it replays into the next request AND survives a restart through
//! history replay. The plain `interrupt` (session shutdown, poisoned queue) sets
//! no marker, so no guidance is written — claiming a user intent that never
//! happened would itself be false.
//!
//! Work item: 2026-10-13 recovery-scope alignment (sub-agent reload +
//! interrupt guidance).

use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::deps::NoopCompactor;
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::content::{ContentBlock, HarnessEnvelope};
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::schemas::rollout::RolloutItem;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// The marker prefix the guidance message carries, asserted verbatim so a wording
/// change is a deliberate edit rather than a silent loss.
const GUIDANCE_MARKER: &str = "The user interrupted the previous turn on purpose.";

/// A provider whose FIRST call opens a turn that never finishes on its own: it
/// yields a message start and then parks until the abort signal fires. That lets
/// the test interrupt a genuinely in-flight turn. Every later call completes and
/// records its request, so the test can prove the persisted guidance actually
/// reaches the model instead of only sitting in the rollout.
struct ParkUntilCancelled {
    info: ProviderInfo,
    calls: AtomicUsize,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
    /// Only the first session parks. A resumed session must complete so the test
    /// can run a follow-up turn and inspect what reached the model.
    park_first_call: bool,
}

impl ModelProvider for ParkUntilCancelled {
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
            context_window: 200_000,
            max_output_tokens: 8_192,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }
    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().unwrap().push(request);
        if call == 0 && self.park_first_call {
            return Box::pin(async_stream::stream! {
                yield ModelStreamEvent::MessageStart { model: "park".into() };
                // Park until the turn is cancelled. No MessageStop is emitted: the
                // abort ends the stream, and the engine settles the turn as
                // interrupted from the cancellation itself.
                signal.cancelled().await;
            });
        }
        Box::pin(async_stream::stream! {
            yield ModelStreamEvent::MessageStart { model: "park".into() };
            yield ModelStreamEvent::TextDelta { text: "done".into() };
            yield ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: Usage {
                    input_tokens: 1,
                    output_tokens: 1,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    total_tokens: None,
                },
            };
        })
    }
}

fn agent_options(
    process: RuntimeProcessContext,
    root: &std::path::Path,
) -> (
    klaus_agent_core::engine::agent::AgentOptions,
    Arc<ParkUntilCancelled>,
) {
    agent_options_recording(process, root, Arc::new(Mutex::new(Vec::new())))
}

fn agent_options_recording(
    process: RuntimeProcessContext,
    root: &std::path::Path,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
) -> (
    klaus_agent_core::engine::agent::AgentOptions,
    Arc<ParkUntilCancelled>,
) {
    agent_options_with(process, root, requests, true)
}

/// Same, but the provider completes immediately instead of parking, so a resumed
/// session can run a real follow-up turn.
fn agent_options_completing(
    process: RuntimeProcessContext,
    root: &std::path::Path,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
) -> (
    klaus_agent_core::engine::agent::AgentOptions,
    Arc<ParkUntilCancelled>,
) {
    agent_options_with(process, root, requests, false)
}

fn agent_options_with(
    process: RuntimeProcessContext,
    root: &std::path::Path,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
    park_first_call: bool,
) -> (
    klaus_agent_core::engine::agent::AgentOptions,
    Arc<ParkUntilCancelled>,
) {
    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process);
    options.cwd = Some(root.to_string_lossy().into_owned());
    options.compactor = Some(Arc::new(NoopCompactor));
    options
        .config
        .as_mut()
        .unwrap()
        .as_object_mut()
        .unwrap()
        .extend(
            json!({
                "model": "park",
                "prewarm": false,
                "autoMemory": false,
                "agentMemory": false,
                "goal": false,
            })
            .as_object()
            .unwrap()
            .clone(),
        );
    let provider = Arc::new(ParkUntilCancelled {
        info: ProviderInfo {
            name: "park".into(),
            base_url: None,
            env_key: None,
            default_model: "park".into(),
        },
        calls: AtomicUsize::new(0),
        requests,
        park_first_call,
    });
    options.provider = provider.clone();
    (options, provider)
}

/// The guidance message persisted for `thread_id`, if any.
fn persisted_guidance(items: &[RolloutItem]) -> Option<RuntimeString> {
    items.iter().find_map(|item| match item {
        RolloutItem::Message {
            message: Message::User(user),
        } if user.is_meta == Some(true) => user.content.iter().find_map(|block| match block {
            ContentBlock::Text(text) if text.text.contains(GUIDANCE_MARKER) => {
                Some(text.text.clone())
            }
            _ => None,
        }),
        _ => None,
    })
}

/// Whether a model-visible history carries the interrupt guidance.
fn history_has_guidance(history: &[Message]) -> bool {
    history.iter().any(|message| {
        let Message::User(user) = message else {
            return false;
        };
        if user.is_meta != Some(true) {
            return false;
        }
        user.content.iter().any(|block| {
            matches!(block, ContentBlock::Text(text) if text.text.contains(GUIDANCE_MARKER) && text.harness_envelope == Some(HarnessEnvelope::SystemReminder))
        })
    })
}

async fn run_and_interrupt(
    root: &std::path::Path,
    by_user: bool,
) -> (Arc<JsonlSessionStore>, String, RuntimeProcessContext) {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let (options, _provider) = agent_options(process.clone(), root);
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let agent = Agent::new(options);
    let sopts = StartSessionOptions {
        cwd: Some(root.to_string_lossy().into_owned().into()),
        user_config_dir: Some(root.to_string_lossy().into_owned().into()),
        ..Default::default()
    };
    let session = agent.open_session(store.clone(), sopts).await.unwrap();
    let thread_id = session.id.try_to_utf8().unwrap();
    let running = tokio::spawn({
        let session = session.clone();
        async move {
            session
                .run(Input::Text("do work".into()), TurnOptions::default())
                .await
        }
    });
    // Let the turn reach the parked model call before interrupting.
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    if by_user {
        session.interrupt_by_user();
    } else {
        session.interrupt();
    }
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), running).await;
    session.flush_rollout().await.unwrap();
    // Release the rollout writer lease so the caller can cold-resume the same
    // thread the way a restarted process would.
    session.shutdown_and_wait().await.unwrap();
    (store, thread_id, process)
}

#[tokio::test]
async fn a_user_interrupt_persists_guidance_that_survives_a_restart() {
    let root = std::env::temp_dir().join(format!(
        "klaus-interrupt-guidance-user-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&root).unwrap();

    let (store, thread_id, process) = run_and_interrupt(&root, /* by_user */ true).await;

    let snapshot = store.read_thread(&thread_id).await.unwrap();
    let guidance = persisted_guidance(&snapshot.raw_items)
        .expect("a user interrupt persists its model-visible guidance");
    // The guidance states what actually survived, not a generic warning.
    let guidance_text = guidance.try_to_utf8().unwrap();
    assert!(
        guidance_text.contains("still running"),
        "guidance must say background shells survive: {guidance_text}"
    );
    assert!(
        guidance_text.contains("partially executed"),
        "guidance must warn an in-flight tool may have half-applied: {guidance_text}"
    );

    assert!(
        guidance_text.contains("Accepted sub-agents")
            && guidance_text.contains("continue independently"),
        "guidance must preserve accepted child ownership: {guidance_text}"
    );
    assert!(
        !guidance_text.contains("Sub-agents that were running have been cancelled"),
        "guidance must not claim a child cascade that did not occur"
    );

    // The guidance is written BEFORE the terminal lifecycle record, matching the
    // ordering a client that re-reads the rollout on the abort event expects.
    let guidance_index = snapshot
        .raw_items
        .iter()
        .position(|item| {
            matches!(item, RolloutItem::Message { message: Message::User(user) }
                if user.is_meta == Some(true)
                    && user.content.iter().any(|block| matches!(block, ContentBlock::Text(text) if text.text.contains(GUIDANCE_MARKER) && text.harness_envelope == Some(HarnessEnvelope::SystemReminder))))
        })
        .expect("guidance present");
    let lifecycle_index = snapshot
        .raw_items
        .iter()
        .rposition(
            |item| matches!(item, RolloutItem::Event(event) if event.event == "turn_lifecycle"),
        )
        .expect("terminal lifecycle record present");
    assert!(
        guidance_index < lifecycle_index,
        "guidance ({guidance_index}) must precede the terminal record ({lifecycle_index})"
    );

    // A cold resume replays the guidance into the model-visible history, so the
    // model still sees it after a restart without any re-injection.
    let requests: Arc<Mutex<Vec<ModelRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let agent = {
        let (options, _provider) = agent_options_completing(
            RuntimeProcessContext::initialize(
                super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
            )
            .unwrap(),
            &root,
            requests.clone(),
        );
        Agent::new(options)
    };
    let resumed = agent
        .resume_session(
            store.clone(),
            &thread_id,
            StartSessionOptions {
                cwd: Some(root.to_string_lossy().into_owned().into()),
                user_config_dir: Some(root.to_string_lossy().into_owned().into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let history = resumed.history();
    assert!(
        history_has_guidance(&history),
        "a resumed history replays the persisted interrupt guidance"
    );

    // The strongest form of the claim: run one more turn and read what actually
    // went to the model. A persisted message that never reached the request would
    // protect nothing, so the guidance must be present in the wire messages.
    resumed
        .run(
            Input::Text("what should I do now".into()),
            TurnOptions::default(),
        )
        .await;
    // Scoped so the guard is gone before the awaits below: holding it across
    // them would block the provider thread this test itself waits on.
    {
        let sent = requests.lock().unwrap();
        assert!(!sent.is_empty(), "the follow-up turn called the model");
        assert!(
            sent.iter()
                .any(|request| request.messages.iter().any(|message| {
                    let Message::User(user) = message else {
                        return false;
                    };
                    user.is_meta == Some(true)
                        && user.content.iter().any(|block| {
                            matches!(block, ContentBlock::Text(text) if text.text.contains(GUIDANCE_MARKER) && text.harness_envelope == Some(HarnessEnvelope::SystemReminder))
                        })
                })),
            "the guidance must reach the model, not just the rollout"
        );
    }

    resumed.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    std::fs::remove_dir_all(&root).ok();
}

#[tokio::test]
async fn a_non_user_abort_persists_no_guidance() {
    let root = std::env::temp_dir().join(format!(
        "klaus-interrupt-guidance-system-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&root).unwrap();

    let (store, thread_id, process) = run_and_interrupt(&root, /* by_user */ false).await;

    let snapshot = store.read_thread(&thread_id).await.unwrap();
    // The turn still records its interrupted status, but nothing claims a user
    // intent that never happened.
    assert!(
        snapshot.raw_items.iter().any(|item| matches!(item,
            RolloutItem::Event(event) if event.event == "turn_lifecycle"
                && event.payload.as_ref().is_some_and(|payload| payload
                    .get("status")
                    .and_then(|status| status.as_str())
                    .is_some_and(|status| status.contains("interrupted"))))),
        "the aborted turn still records its interrupted status"
    );
    assert!(
        persisted_guidance(&snapshot.raw_items).is_none(),
        "a shutdown/queue abort must NOT persist user-interrupt guidance"
    );

    process.shutdown().await.unwrap();
    std::fs::remove_dir_all(&root).ok();
}
