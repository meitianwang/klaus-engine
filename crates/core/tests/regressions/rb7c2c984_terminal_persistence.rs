//! Broke: a completed turn became observable before its original messages and
//! terminal record had crossed the rollout writer's persistence boundary.
//! Root cause: terminal publication awaited no acknowledgement from the writer.
//! Native work item: b7c2c984-235c-4169-a129-beba4ca4ef77.

use async_trait::async_trait;
use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::deps::NoopCompactor;
use klaus_agent_core::engine::session::{Input, Session, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{RuntimeJson, RuntimeString};
use klaus_agent_core::schemas::background_process::BackgroundProcessRecord;
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::schemas::rollout::{
    RolloutItem, SessionMeta, TurnContextSnapshot, UsageLedgerItem,
};
use klaus_agent_core::schemas::subagent_lifecycle::SubagentLifecycleRecord;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::{
    AppendEventOptions, CheckpointRecord, CompactionRecord, LiveRollout, RollbackRecord,
    SessionStore,
};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{ProviderError, ProviderErrorKind, TerminalReason};
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use serde_json::json;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

const THREAD: &str = "terminal-persistence";
const INPUT: &str = "Keep the original answer and its completion together.";
const ANSWER: &str = "The complete original answer is available.";
const PROVIDER_FAILURE: &str = "Controlled provider quota refusal.";
const FLUSH_FAILURE: &str = "Controlled terminal persistence failure.";
const WAIT: Duration = Duration::from_secs(10);

#[derive(Clone, Copy)]
enum Outcome {
    Complete,
    Abort,
    Fail,
}

#[derive(Debug)]
struct Gate {
    terminal_seen: AtomicBool,
    terminal_flush_claimed: AtomicBool,
    entered: Semaphore,
    release: Semaphore,
    fail: bool,
}

impl Gate {
    fn new(fail: bool) -> Self {
        Self {
            terminal_seen: AtomicBool::new(false),
            terminal_flush_claimed: AtomicBool::new(false),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            fail,
        }
    }
}

/// All real JSONL writes are forwarded. Only the acknowledgement following an
/// actual turn_lifecycle record is controlled; setup and guidance flushes pass.
#[derive(Debug)]
struct ControlledRollout {
    inner: Arc<dyn LiveRollout>,
    gate: Arc<Gate>,
}

impl ControlledRollout {
    fn is_terminal_event(event: &RuntimeString, options: Option<&AppendEventOptions>) -> bool {
        event == "turn_lifecycle"
            && options.is_some_and(|options| {
                options.turn_id.is_some()
                    && options
                        .payload
                        .as_ref()
                        .and_then(|value| value.get("status"))
                        .and_then(RuntimeJson::as_string)
                        .is_some_and(|status| {
                            matches!(
                                status.try_to_utf8().ok().as_deref(),
                                Some("completed" | "interrupted" | "failed")
                            )
                        })
            })
    }
}

#[async_trait]
impl LiveRollout for ControlledRollout {
    async fn prepare_batch(
        &self,
        maximum_records: usize,
    ) -> io::Result<klaus_agent_core::store::session_store::PreparedRolloutBatch<'_>> {
        let admission = self.inner.prepare_batch(maximum_records).await?;
        Ok(
            klaus_agent_core::store::session_store::PreparedRolloutBatch::new(move |items| {
                assert!(items.len() <= maximum_records);
                let terminal = items.iter().any(|item| match item {
                    RolloutItem::Event(event) => Self::is_terminal_event(
                        &event.event,
                        Some(&AppendEventOptions {
                            turn_id: event.turn_id.clone(),
                            payload: event.payload.clone(),
                        }),
                    ),
                    _ => false,
                });
                admission.commit(items);
                if terminal {
                    self.gate.terminal_seen.store(true, Ordering::SeqCst);
                }
            }),
        )
    }

    async fn append_message(&self, message: Message) {
        self.inner.append_message(message).await;
    }
    async fn append_turn_context(&self, ctx: TurnContextSnapshot) {
        self.inner.append_turn_context(ctx).await;
    }
    async fn append_compaction(&self, record: CompactionRecord) {
        self.inner.append_compaction(record).await;
    }
    async fn append_event(&self, event: RuntimeString, options: Option<AppendEventOptions>) {
        let terminal = Self::is_terminal_event(&event, options.as_ref());
        self.inner.append_event(event, options).await;
        if terminal {
            self.gate.terminal_seen.store(true, Ordering::SeqCst);
        }
    }
    async fn append_rollback(&self, record: RollbackRecord) {
        self.inner.append_rollback(record).await;
    }
    async fn append_checkpoint(&self, record: CheckpointRecord) {
        self.inner.append_checkpoint(record).await;
    }
    async fn append_usage(&self, record: UsageLedgerItem) {
        self.inner.append_usage(record).await;
    }
    async fn append_subagent_lifecycle(&self, record: SubagentLifecycleRecord) {
        self.inner.append_subagent_lifecycle(record).await;
    }
    async fn append_background_process_lifecycle(&self, record: BackgroundProcessRecord) {
        self.inner.append_background_process_lifecycle(record).await;
    }
    fn inspection_revision(&self) -> Option<u64> {
        self.inner.inspection_revision()
    }
    async fn update_title(&self, title: &str) -> io::Result<()> {
        self.inner.update_title(title).await
    }
    async fn update_host_metadata(&self, metadata: Option<RuntimeJson>) -> io::Result<()> {
        self.inner.update_host_metadata(metadata).await
    }
    async fn flush(&self) -> io::Result<()> {
        if self.gate.terminal_seen.load(Ordering::SeqCst) {
            let first = !self
                .gate
                .terminal_flush_claimed
                .swap(true, Ordering::SeqCst);
            if first {
                self.gate.entered.add_permits(1);
            }
            // Persistence remains unavailable for a best-effort failure record.
            // Neither failure attempt waits on a test release permit.
            if self.gate.fail {
                return Err(io::Error::other(FLUSH_FAILURE));
            }
            if first {
                self.gate
                    .release
                    .acquire()
                    .await
                    .expect("test release remains open")
                    .forget();
            }
        }
        self.inner.flush().await
    }
    async fn shutdown(&self) -> io::Result<()> {
        self.inner.shutdown().await
    }
}

struct Provider {
    info: ProviderInfo,
    outcome: Outcome,
    entered: Arc<Semaphore>,
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
            max_output_tokens: 8192,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }
    fn stream(&self, _: ModelRequest, signal: AbortSignal) -> ModelStream {
        self.entered.add_permits(1);
        match self.outcome {
            Outcome::Complete => Box::pin(futures::stream::iter(vec![
                ModelStreamEvent::TextDelta {
                    text: ANSWER.into(),
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
            ])),
            Outcome::Abort => Box::pin(async_stream::stream! {
                yield ModelStreamEvent::MessageStart { model: "terminal-fixture".into() };
                signal.cancelled().await;
            }),
            Outcome::Fail => Box::pin(futures::stream::iter(vec![ModelStreamEvent::Error {
                error: ProviderError {
                    kind: ProviderErrorKind::Quota,
                    message: PROVIDER_FAILURE.into(),
                    status: Some(403),
                    code: None,
                    error_type: None,
                    media_origins: None,
                    media_repair_complete: false,
                    retry_after_ms: None,
                    request_id: None,
                },
            }])),
        }
    }
}

struct Fixture {
    session: Arc<Session>,
    store: Arc<JsonlSessionStore>,
    gate: Arc<Gate>,
    provider_entered: Arc<Semaphore>,
    _root: super::context_fixture::TemporaryDirectory,
}

async fn fixture(outcome: Outcome, fail_flush: bool) -> Fixture {
    let root = super::context_fixture::tempdir().unwrap();
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.path().join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let inner: Arc<dyn LiveRollout> = Arc::from(
        store
            .create(SessionMeta {
                id: THREAD.into(),
                created_at: 1,
                cwd: root.path().to_string_lossy().into_owned().into(),
                model: "terminal-fixture".into(),
                title: None,
                originator: None,
                cli_version: None,
                source: None,
                role: None,
                parent_thread_id: None,
                forked_from_id: None,
                git: None,
                host_metadata: None,
            })
            .await
            .unwrap(),
    );
    let gate = Arc::new(Gate::new(fail_flush));
    let provider_entered = Arc::new(Semaphore::new(0));
    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process);
    options.cwd = Some(root.path().to_string_lossy().into_owned());
    options.compactor = Some(Arc::new(NoopCompactor));
    options.include_default_agents = Some(false);
    options
        .config
        .as_mut()
        .unwrap()
        .as_object_mut()
        .unwrap()
        .extend(
            json!({
                "model":"terminal-fixture", "prewarm":false, "autoMemory":false,
                "agentMemory":false, "autoDream":false, "memoryRecall":false, "goal":false,
                "autoCompaction":false, "idleMicrocompact":false, "earlyMicrocompact":false,
                "speculativeCompaction":false, "loopDetection":false, "maxRetries":0,
            })
            .as_object()
            .unwrap()
            .clone(),
        );
    options.provider = Arc::new(Provider {
        info: ProviderInfo {
            name: "terminal-fixture".into(),
            default_model: "terminal-fixture".into(),
            base_url: None,
            env_key: None,
        },
        outcome,
        entered: provider_entered.clone(),
    });
    let session = Agent::new(options).start_session(StartSessionOptions {
        id: Some(THREAD.into()),
        cwd: Some(root.path().to_string_lossy().into_owned().into()),
        user_config_dir: Some(root.path().to_string_lossy().into_owned().into()),
        system_prompt_provider: Some(Arc::new(|| Box::pin(async { Vec::new() }))),
        rollout: Some(Arc::new(ControlledRollout {
            inner,
            gate: gate.clone(),
        })),
        store: Some(store.clone()),
        ..Default::default()
    });
    Fixture {
        session,
        store,
        gate,
        provider_entered,
        _root: root,
    }
}

#[derive(Debug, PartialEq, Eq)]
enum TerminalObservation {
    Completed,
    Failed,
}

type Observed = Arc<Mutex<Vec<TerminalObservation>>>;

fn run(fixture: &Fixture) -> (JoinHandle<Vec<EngineEvent>>, Observed) {
    let mut events = fixture
        .session
        .run_streamed(Input::Text(INPUT.into()), TurnOptions::default())
        .events;
    let observed: Observed = Arc::new(Mutex::new(Vec::new()));
    let capture = observed.clone();
    let running = tokio::spawn(async move {
        let mut collected = Vec::new();
        while let Some(event) = events.next().await {
            match &event {
                EngineEvent::TurnCompleted { .. } => {
                    capture.lock().unwrap().push(TerminalObservation::Completed)
                }
                EngineEvent::TurnFailed { .. } => {
                    capture.lock().unwrap().push(TerminalObservation::Failed)
                }
                _ => {}
            }
            collected.push(event);
        }
        collected
    });
    (running, observed)
}

async fn await_terminal_flush(
    fixture: &Fixture,
    running: &mut JoinHandle<Vec<EngineEvent>>,
    observed: &Observed,
) {
    tokio::select! {
        permit = fixture.gate.entered.acquire() => permit.unwrap().forget(),
        result = &mut *running => panic!("the public turn settled without awaiting its terminal persistence boundary: {result:?}"),
        _ = tokio::time::sleep(WAIT) => panic!("the real terminal record never reached its persistence boundary"),
    }
    assert!(fixture.gate.terminal_seen.load(Ordering::SeqCst));
    assert!(
        observed.lock().unwrap().is_empty(),
        "terminal events must not be visible while their persistence acknowledgement is blocked"
    );
    assert!(!running.is_finished());
}

async fn finish(fixture: &Fixture, running: JoinHandle<Vec<EngineEvent>>) -> Vec<EngineEvent> {
    fixture.gate.release.add_permits(1);
    tokio::time::timeout(WAIT, running)
        .await
        .expect("released terminal persistence must settle")
        .unwrap()
}

fn successful(events: &[EngineEvent]) -> usize {
    events
        .iter()
        .filter(|event| {
            matches!(
                event,
                EngineEvent::TurnCompleted {
                    stop_reason: TerminalReason::Completed,
                    ..
                }
            )
        })
        .count()
}

async fn assert_originals_and_terminal(fixture: &Fixture, events: &[EngineEvent], status: &str) {
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                EngineEvent::TurnCompleted { .. } | EngineEvent::TurnFailed { .. }
            ))
            .count(),
        1
    );
    // No caller-side flush is allowed here: the public terminal itself owns the
    // guarantee needed by an immediate provider-free history read.
    let snapshot = fixture.store.read_thread(THREAD).await.unwrap();
    let messages = snapshot
        .raw_items
        .iter()
        .filter_map(|item| match item {
            RolloutItem::Message { message } => Some(message.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        messages,
        fixture.session.history(),
        "all original messages are readable immediately after terminal publication"
    );
    assert!(messages.iter().any(|message| matches!(message, Message::User(user) if user.content.iter().any(|block| matches!(block, ContentBlock::Text(text) if text.text == INPUT)))));
    let turn_id = events
        .iter()
        .find_map(|event| match event {
            EngineEvent::TurnStarted { turn_id } => Some(turn_id),
            _ => None,
        })
        .unwrap();
    let terminal = snapshot
        .raw_items
        .iter()
        .find_map(|item| match item {
            RolloutItem::Event(event)
                if event.event == "turn_lifecycle" && event.turn_id.as_ref() == Some(turn_id) =>
            {
                Some(event)
            }
            _ => None,
        })
        .expect("the owning terminal record must be readable with its original messages");
    assert_eq!(
        terminal
            .payload
            .as_ref()
            .unwrap()
            .get("status")
            .and_then(RuntimeJson::as_string),
        Some(&RuntimeString::from(status))
    );
    if status == "completed" {
        let assistant = messages
            .iter()
            .find_map(|message| match message {
                Message::Assistant(assistant)
                    if assistant.content.iter().any(
                        |block| matches!(block, ContentBlock::Text(text) if text.text == ANSWER),
                    ) =>
                {
                    Some(assistant)
                }
                _ => None,
            })
            .expect("the complete original final assistant must be durable");
        assert_eq!(
            terminal
                .payload
                .as_ref()
                .unwrap()
                .get("finalMessageId")
                .and_then(RuntimeJson::as_string),
            Some(&assistant.uuid)
        );
    }
}

async fn assert_failed_terminal_keeps_metadata(fixture: &Fixture) {
    let snapshot = fixture.store.read_thread(THREAD).await.unwrap();
    let payload = snapshot
        .raw_items
        .iter()
        .rev()
        .find_map(|item| match item {
            RolloutItem::Event(event) if event.event == "turn_lifecycle" => event.payload.as_ref(),
            _ => None,
        })
        .expect("the failure journal must retain its terminal metadata");
    assert_eq!(
        payload.get("status").and_then(RuntimeJson::as_string),
        Some(&RuntimeString::from("failed"))
    );
    assert!(payload
        .get("finishedAtMs")
        .and_then(RuntimeJson::as_i64)
        .is_some());
    assert!(payload
        .get("durationMs")
        .and_then(RuntimeJson::as_u64)
        .is_some());
    assert!(payload
        .get("finalMessageId")
        .is_none_or(RuntimeJson::is_null));
}

#[tokio::test]
async fn public_completion_waits_for_terminal_persistence_and_immediate_history_is_complete() {
    let fixture = fixture(Outcome::Complete, false).await;
    let (mut running, observed) = run(&fixture);
    await_terminal_flush(&fixture, &mut running, &observed).await;
    let events = finish(&fixture, running).await;
    assert_eq!(successful(&events), 1);
    assert!(!events
        .iter()
        .any(|event| matches!(event, EngineEvent::TurnFailed { .. })));
    assert_originals_and_terminal(&fixture, &events, "completed").await;
    fixture.session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn terminal_persistence_failure_publishes_a_clear_failure_without_success() {
    let fixture = fixture(Outcome::Complete, true).await;
    let (mut running, observed) = run(&fixture);
    let events = tokio::time::timeout(WAIT, &mut running)
        .await
        .expect("persistent writer failure must settle without waiting for a release")
        .unwrap();
    assert!(fixture.gate.terminal_seen.load(Ordering::SeqCst));
    assert!(fixture.gate.terminal_flush_claimed.load(Ordering::SeqCst));
    assert!(observed
        .lock()
        .unwrap()
        .contains(&TerminalObservation::Failed));
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnCompleted { .. })),
        "a writer failure replaces the terminal rather than following it"
    );
    let failures = events
        .iter()
        .filter_map(|event| match event {
            EngineEvent::TurnFailed { error, .. } => Some(error),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        failures.len(),
        1,
        "one persistence failure replaces the successful terminal"
    );
    assert!(
        failures[0]
            .message
            .try_to_utf8()
            .unwrap()
            .contains(FLUSH_FAILURE),
        "the writer's persistence failure must survive terminal publication: {:?}",
        failures[0]
    );
    let cleanup_error = fixture
        .session
        .shutdown_and_wait()
        .await
        .expect_err("persistent storage failure must remain visible during cleanup");
    assert!(cleanup_error.contains(FLUSH_FAILURE));
    assert_failed_terminal_keeps_metadata(&fixture).await;
}

#[tokio::test]
async fn an_aborted_turn_keeps_its_abort_semantics_after_terminal_persistence() {
    let fixture = fixture(Outcome::Abort, false).await;
    let (mut running, observed) = run(&fixture);
    tokio::time::timeout(WAIT, fixture.provider_entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    fixture.session.interrupt_by_user();
    await_terminal_flush(&fixture, &mut running, &observed).await;
    let events = finish(&fixture, running).await;
    assert_eq!(successful(&events), 0);
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                EngineEvent::TurnCompleted {
                    stop_reason: TerminalReason::Aborted,
                    ..
                }
            ))
            .count(),
        1
    );
    assert!(!events
        .iter()
        .any(|event| matches!(event, EngineEvent::TurnFailed { .. })));
    assert_originals_and_terminal(&fixture, &events, "interrupted").await;
    fixture.session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn a_provider_failure_keeps_its_original_reason_after_terminal_persistence() {
    let fixture = fixture(Outcome::Fail, false).await;
    let (mut running, observed) = run(&fixture);
    await_terminal_flush(&fixture, &mut running, &observed).await;
    let events = finish(&fixture, running).await;
    assert_eq!(successful(&events), 0);
    let errors = events
        .iter()
        .filter_map(|event| match event {
            EngineEvent::TurnFailed { error, .. } => Some(error),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(errors.len(), 1);
    assert!(errors[0]
        .message
        .try_to_utf8()
        .unwrap()
        .contains(PROVIDER_FAILURE));
    assert_eq!(errors[0].provider_kind, Some(ProviderErrorKind::Quota));
    assert_originals_and_terminal(&fixture, &events, "failed").await;
    fixture.session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn a_provider_failure_keeps_its_reason_when_terminal_persistence_also_fails() {
    let fixture = fixture(Outcome::Fail, true).await;
    let (running, _) = run(&fixture);
    let events = tokio::time::timeout(WAIT, running)
        .await
        .expect("persistent failure of the error journal must not hang terminal publication")
        .unwrap();
    assert!(fixture.gate.terminal_flush_claimed.load(Ordering::SeqCst));
    assert!(!events
        .iter()
        .any(|event| matches!(event, EngineEvent::TurnCompleted { .. })));
    let errors = events
        .iter()
        .filter_map(|event| match event {
            EngineEvent::TurnFailed { error, .. } => Some(error),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(errors.len(), 1);
    let message = errors[0].message.try_to_utf8().unwrap();
    assert!(
        message.contains(PROVIDER_FAILURE),
        "the original provider failure remains visible: {message}"
    );
    assert!(
        message.contains(FLUSH_FAILURE),
        "the persistence failure remains visible: {message}"
    );
    assert_eq!(errors[0].provider_kind, Some(ProviderErrorKind::Quota));
    let cleanup_error = fixture
        .session
        .shutdown_and_wait()
        .await
        .expect_err("persistent storage failure must remain visible during cleanup");
    assert!(cleanup_error.contains(FLUSH_FAILURE));
    assert_failed_terminal_keeps_metadata(&fixture).await;
}
