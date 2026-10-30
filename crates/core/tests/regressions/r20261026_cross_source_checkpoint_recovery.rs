//! Defect: switching sources could summarize an opaque checkpoint on an unavailable source,
//! and a later switch still failed when one restored exchange exceeded the summary budget.
//! Root cause: migration used the previous transport, then refused to truncate or drop
//! original messages, so the turn never started on the selected model.
//! Work item: 20261026 authorized cross-source checkpoint recovery; 20261029 model switch must continue.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{resolve_config, Input, Session, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeString};
use klaus_agent_core::schemas::message::{Message, NativeCompactionCheckpoint, Usage};
use klaus_agent_core::schemas::rollout::RolloutItem;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{ProviderError, ProviderErrorKind};
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo, RemoteCompactionResult, RemoteCompactionSupport,
};
use serde_json::json;
use tokio::sync::Notify;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

// Both transports deliberately advertise the same model identifier.
const MODEL: &str = "shared-model-id";
const LATEST: &str = "LATEST: keep the original requirement and use the recorded tool result.";
const SUMMARY: &str = "Recovered task: preserve the original requirement and TOOL_EVIDENCE.";
const PARTIAL: &str = "UNCOMMITTED_SECOND_CHUNK";
const LARGE_WINDOW: u64 = 200_000;
const SMALL_WINDOW: u64 = 16_000;

#[derive(Clone, Copy, PartialEq, Eq)]
enum TargetBehavior {
    Complete,
    FailSecond,
    PauseSecond,
}

struct Calls {
    source_streams: AtomicUsize,
    source_compactions: AtomicUsize,
    source_unavailable: AtomicBool,
    source_error: ProviderErrorKind,
    target: Mutex<Vec<ModelRequest>>,
    behavior: TargetBehavior,
    second_started: Notify,
}

impl Calls {
    fn new(source_error: ProviderErrorKind, behavior: TargetBehavior) -> Arc<Self> {
        Arc::new(Self {
            source_streams: AtomicUsize::new(0),
            source_compactions: AtomicUsize::new(0),
            source_unavailable: AtomicBool::new(false),
            source_error,
            target: Mutex::new(Vec::new()),
            behavior,
            second_started: Notify::new(),
        })
    }
}

struct Provider {
    info: ProviderInfo,
    source: bool,
    window: u64,
    calls: Arc<Calls>,
}

fn error(kind: ProviderErrorKind, message: &str) -> ProviderError {
    ProviderError {
        kind,
        message: message.into(),
        status: None,
        code: None,
        error_type: None,
        request_id: None,
        media_origins: None,
        media_repair_complete: false,
        retry_after_ms: None,
    }
}

fn stop() -> ModelStreamEvent {
    ModelStreamEvent::MessageStop {
        stop_reason: ModelStopReason::EndTurn,
        usage: Usage {
            input_tokens: 100,
            output_tokens: 20,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
            total_tokens: None,
        },
    }
}

#[async_trait]
impl ModelProvider for Provider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: true,
            tools: true,
            thinking: true,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: self.window,
            max_output_tokens: 1_000,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
            supports_image_detail_original: false,
        }
    }

    fn remote_compaction_support(&self, _: &RuntimeString) -> RemoteCompactionSupport {
        if self.source {
            RemoteCompactionSupport::ResponsesV2
        } else {
            RemoteCompactionSupport::Unsupported
        }
    }

    async fn native_checkpoint_compatibility(
        &self,
        checkpoint: &NativeCompactionCheckpoint,
        _: &RuntimeString,
        _: &AbortSignal,
    ) -> Result<bool, ProviderError> {
        Ok(self.source && checkpoint.source == "checkpoint-source")
    }

    async fn compact_remote(
        &self,
        request: ModelRequest,
        _: AbortSignal,
    ) -> Result<RemoteCompactionResult, ProviderError> {
        assert!(
            self.source,
            "Recovery must never request another native checkpoint"
        );
        self.calls.source_compactions.fetch_add(1, Ordering::SeqCst);
        if self.calls.source_unavailable.load(Ordering::SeqCst) {
            return Err(error(
                self.calls.source_error,
                "The old source is unavailable",
            ));
        }
        assert!(encoded(&request.messages).contains("ORIGINAL_REQUIREMENT"));
        Ok(RemoteCompactionResult {
            checkpoint: NativeCompactionCheckpoint {
                source: "checkpoint-source".into(),
                auth_scope: "old-auth-scope".into(),
                model: MODEL.into(),
                comp_hash: None,
                response_id: "old-response-id".into(),
                encrypted_content: "OPAQUE_CHECKPOINT_ONLY_OLD_SOURCE_CAN_READ".into(),
                retained_input: Vec::new(),
                retained_input_metadata: Vec::new(),
            },
            usage: None,
        })
    }

    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        if self.source {
            self.calls.source_streams.fetch_add(1, Ordering::SeqCst);
            return Box::pin(futures::stream::iter([ModelStreamEvent::Error {
                error: error(self.calls.source_error, "The old source is unavailable"),
            }]));
        }
        let index = {
            let mut requests = self.calls.target.lock().unwrap();
            requests.push(request);
            requests.len()
        };
        if index == 2 && self.calls.behavior != TargetBehavior::Complete {
            if self.calls.behavior == TargetBehavior::FailSecond {
                return Box::pin(futures::stream::iter([
                    ModelStreamEvent::TextDelta {
                        text: PARTIAL.into(),
                    },
                    ModelStreamEvent::Error {
                        error: error(
                            ProviderErrorKind::InvalidRequest,
                            "Target summary fixture failed",
                        ),
                    },
                ]));
            }
            let calls = self.calls.clone();
            return Box::pin(
                futures::stream::iter([ModelStreamEvent::TextDelta {
                    text: PARTIAL.into(),
                }])
                .chain(futures::stream::once(async move {
                    calls.second_started.notify_one();
                    signal.cancelled().await;
                    ModelStreamEvent::Error {
                        error: error(
                            ProviderErrorKind::InvalidRequest,
                            "Target summary cancelled",
                        ),
                    }
                })),
            );
        }
        Box::pin(futures::stream::iter([
            ModelStreamEvent::TextDelta {
                text: SUMMARY.into(),
            },
            stop(),
        ]))
    }
}

fn provider(source: bool, window: u64, calls: &Arc<Calls>) -> Arc<dyn ModelProvider> {
    Arc::new(Provider {
        info: ProviderInfo {
            name: if source {
                "checkpoint-source"
            } else {
                "recovery-target"
            }
            .into(),
            base_url: None,
            env_key: None,
            default_model: MODEL.into(),
        },
        source,
        window,
        calls: calls.clone(),
    })
}

fn config(source: bool, window: u64) -> serde_json::Value {
    let source = if source {
        "checkpoint-source"
    } else {
        "recovery-target"
    };
    json!({
        "model": MODEL, "contextWindow": window, "maxOutputTokens": 1_000,
        "autoCompaction": true, "prewarm": false, "thinking": false,
        "loopDetection": false, "autoMemory": false, "autoDream": false,
        "memoryRecall": false, "maxRetries": 0, "compactionStreamMaxRetries": 0,
        "approval_policy": "never", "sandbox_mode": "danger-full-access",
        "compactPrompt": "Summarize this recovered task without dropping its requirement or tool evidence.",
        "modelSource": {"source": source, "modelId": MODEL, "selection": format!("{source}:{MODEL}")}
    })
}

fn message(value: serde_json::Value) -> Message {
    runtime_codec::decode_text(&value.to_string().into()).unwrap()
}

fn raw_history_sized(large: bool, giant: bool) -> Vec<Message> {
    let mut messages = vec![
        message(
            json!({"type":"user","uuid":"original-user","content":[{"type":"text","text":"ORIGINAL_REQUIREMENT: preserve the exact evidence."}]}),
        ),
        message(
            json!({"type":"assistant","uuid":"tool-call-message","model":MODEL,"content":[
                {"type":"thinking","text":"Readable reasoning","encryptedContent":"OLD_ENCRYPTED_REASONING"},
                {"type":"thinking","text":"Readable signed reasoning","signature":"OLD_SIGNED_REASONING"},
                {"type":"redacted_thinking","data":"OLD_REDACTED_REASONING"},
                {"type":"tool_use","id":"recorded-read","name":"read","input":{"path":"evidence.txt"},"thoughtSignature":"OLD_TOOL_SIGNATURE"}
            ]}),
        ),
        message(
            json!({"type":"tool_result","uuid":"tool-result-message","toolUseId":"recorded-read","content":[{"type":"text","text":"TOOL_EVIDENCE: the durable result is 42."}],"isError":false}),
        ),
        message(
            json!({"type":"user","uuid":"second-user","content":[{"type":"text","text":"SECOND_REQUIREMENT: keep the recorded result in order."}]}),
        ),
    ];
    if large {
        // Distinct blocks force multiple bounded summaries rather than one oversized call.
        for index in 0..24 {
            messages.push(message(json!({"type":"assistant","uuid":format!("large-{index}"),"model":MODEL,"content":[{"type":"text","text":format!("ARCHIVE_{index}: {}", "Measured evidence record. ".repeat(200))}]})));
        }
    } else {
        messages.push(message(json!({"type":"assistant","uuid":"original-answer","model":MODEL,"content":[{"type":"text","text":"The original evidence is ready."}]})));
    }
    if giant {
        messages.push(message(json!({
            "type":"user",
            "uuid":"giant-exchange",
            "content":[{"type":"text","text":format!("GIANT_EXCHANGE {}", "measured ".repeat(12_000))}]
        })));
    }
    messages
}

fn encoded(messages: &[Message]) -> String {
    runtime_codec::encode_text(&messages.to_vec())
        .unwrap()
        .try_to_utf8()
        .unwrap()
}

fn assert_portable(messages: &[Message]) {
    let text = encoded(messages);
    for forbidden in [
        "OPAQUE_CHECKPOINT_ONLY_OLD_SOURCE_CAN_READ",
        "OLD_ENCRYPTED_REASONING",
        "OLD_SIGNED_REASONING",
        "OLD_REDACTED_REASONING",
        "OLD_TOOL_SIGNATURE",
    ] {
        assert!(
            !text.contains(forbidden),
            "The target received source-owned opaque content: {forbidden}"
        );
    }
}

struct Fixture {
    _dir: tempfile::TempDir,
    process: RuntimeProcessContext,
    store: Arc<JsonlSessionStore>,
    session: Arc<Session>,
    calls: Arc<Calls>,
}

impl Fixture {
    async fn new(large: bool, behavior: TargetBehavior, source_error: ProviderErrorKind) -> Self {
        Self::open(large, behavior, source_error, false).await
    }

    async fn new_giant(behavior: TargetBehavior, source_error: ProviderErrorKind) -> Self {
        Self::open(false, behavior, source_error, true).await
    }

    async fn open(
        large: bool,
        behavior: TargetBehavior,
        source_error: ProviderErrorKind,
        giant: bool,
    ) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
        let calls = Calls::new(source_error, behavior);
        let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(dir.path().join("sessions").to_string_lossy().into_owned()),
            archive_dir: None,
        }));
        let agent = Self::agent(&process, dir.path(), true, LARGE_WINDOW, &calls);
        let session = agent
            .open_session(
                store.clone(),
                StartSessionOptions {
                    initial_history: Some(raw_history_sized(large, giant)),
                    user_config_dir: Some(dir.path().to_string_lossy().into_owned().into()),
                    base_instructions: Some("Continue the durable evidence task.".into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let outcome = session.compact(None).await.unwrap();
        assert!(
            outcome.compacted,
            "The fixture must create a real native checkpoint"
        );
        session.flush_rollout().await.unwrap();
        let persisted = store
            .read_thread(&session.id.try_to_utf8().unwrap())
            .await
            .unwrap();
        assert!(persisted.raw_items.iter().any(|item| matches!(
            item,
            RolloutItem::Message {
                message: Message::ToolResult(_)
            }
        )));
        let checkpoint = persisted
            .raw_items
            .iter()
            .find_map(|item| match item {
                RolloutItem::Compacted(item) => Some(item),
                _ => None,
            })
            .expect("Session.compact must persist a source-aware replacement");
        assert!(checkpoint
            .source_message_ids
            .as_ref()
            .unwrap()
            .contains(&"tool-result-message".into()));
        assert!(checkpoint.replacement_history.as_ref().unwrap().iter().any(|message| matches!(message, Message::CompactBoundary(boundary) if boundary.native_compaction.is_some())));
        calls.source_unavailable.store(true, Ordering::SeqCst);
        Self {
            _dir: dir,
            process,
            store,
            session,
            calls,
        }
    }

    fn agent(
        process: &RuntimeProcessContext,
        root: &std::path::Path,
        source: bool,
        window: u64,
        calls: &Arc<Calls>,
    ) -> Agent {
        let mut options = agent_options(process.clone());
        options.provider = provider(source, window, calls);
        options.config = Some(config(source, window));
        options.cwd = Some(root.to_string_lossy().into_owned());
        options.include_default_agents = Some(false);
        Agent::new(options)
    }

    fn switch(&self, window: u64) {
        self.session
            .reconfigure_inference(
                provider(false, window, &self.calls),
                resolve_config(config(false, window)).unwrap(),
                None,
                None,
                self.session.model_revision(),
            )
            .unwrap();
    }

    async fn cold_resume(&mut self, window: u64) {
        let id = self.session.id.try_to_utf8().unwrap();
        self.session.shutdown_and_wait().await.unwrap();
        let agent = Self::agent(&self.process, self._dir.path(), false, window, &self.calls);
        self.session = agent
            .resume_session(
                self.store.clone(),
                &id,
                StartSessionOptions {
                    user_config_dir: Some(self._dir.path().to_string_lossy().into_owned().into()),
                    base_instructions: Some("Continue the durable evidence task.".into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }

    async fn run(&self) -> Vec<EngineEvent> {
        tokio::time::timeout(
            Duration::from_secs(20),
            self.session
                .run_streamed(Input::Text(LATEST.into()), TurnOptions::default())
                .events
                .collect(),
        )
        .await
        .expect("Recovery must settle within its bounded fixture")
    }

    async fn compactions(&self) -> Vec<klaus_agent_core::schemas::rollout::CompactedItem> {
        self.session.flush_rollout().await.unwrap();
        self.store
            .read_thread(&self.session.id.try_to_utf8().unwrap())
            .await
            .unwrap()
            .raw_items
            .into_iter()
            .filter_map(|item| match item {
                RolloutItem::Compacted(item) => Some(item),
                _ => None,
            })
            .collect()
    }

    fn assert_no_old_calls(&self) {
        assert_eq!(
            self.calls.source_streams.load(Ordering::SeqCst),
            0,
            "No request may return to the unavailable source"
        );
        assert_eq!(
            self.calls.source_compactions.load(Ordering::SeqCst),
            1,
            "Only fixture creation may compact on the old source"
        );
    }

    async fn shutdown(self) {
        self.session.shutdown_and_wait().await.unwrap();
        self.process.shutdown().await.unwrap();
    }
}

fn assert_success(events: &[EngineEvent]) {
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{events:?}"
    );
}

fn assert_original_order(request: &ModelRequest) {
    assert_portable(&request.messages);
    let text = encoded(&request.messages);
    let original = text
        .find("ORIGINAL_REQUIREMENT")
        .expect("Original user input must be recovered");
    let tool_call = text
        .find("recorded-read")
        .expect("Original tool call must survive recovery");
    let evidence = text
        .find("TOOL_EVIDENCE")
        .expect("Original tool output must survive recovery");
    let second = text
        .find("SECOND_REQUIREMENT")
        .expect("Later historical user input must survive recovery");
    let latest = text
        .find(LATEST)
        .expect("The new turn input must survive recovery");
    assert!(
        original < tool_call && tool_call < evidence && evidence < second && second < latest,
        "Recovered user and tool order changed: {text}"
    );
    assert_eq!(
        text.matches(LATEST).count(),
        1,
        "The latest input must not be duplicated"
    );
}

#[tokio::test]
async fn warm_switch_recovers_raw_history_without_calling_a_quota_exhausted_source() {
    let fixture = Fixture::new(false, TargetBehavior::Complete, ProviderErrorKind::Quota).await;
    fixture.switch(LARGE_WINDOW);
    assert_success(&fixture.run().await);
    fixture.assert_no_old_calls();
    {
        let requests = fixture.calls.target.lock().unwrap();
        assert_eq!(
            requests.len(),
            1,
            "A fitting raw history must continue directly"
        );
        assert_original_order(&requests[0]);
    }
    fixture.shutdown().await;
}

#[tokio::test]
async fn cold_resume_recovers_raw_history_with_only_the_target_transport() {
    let mut fixture = Fixture::new(false, TargetBehavior::Complete, ProviderErrorKind::Auth).await;
    fixture.cold_resume(LARGE_WINDOW).await;
    assert_success(&fixture.run().await);
    fixture.assert_no_old_calls();
    {
        let requests = fixture.calls.target.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_original_order(&requests[0]);
    }
    // Recovery must remain durable after another cold resume.
    fixture.cold_resume(LARGE_WINDOW).await;
    assert_success(&fixture.run().await);
    fixture.assert_no_old_calls();
    for request in fixture.calls.target.lock().unwrap().iter() {
        assert_portable(&request.messages);
    }
    fixture.shutdown().await;
}

#[tokio::test]
async fn oversized_raw_history_is_summarized_in_bounded_target_only_chunks() {
    let fixture = Fixture::new(true, TargetBehavior::Complete, ProviderErrorKind::Auth).await;
    fixture.switch(SMALL_WINDOW);
    assert_success(&fixture.run().await);
    fixture.assert_no_old_calls();
    {
        let requests = fixture.calls.target.lock().unwrap();
        assert!(
            requests.len() >= 3,
            "Large original history requires multiple summaries and a continuation"
        );
        assert!(
            requests.len() <= 64,
            "Recovery must have a finite inference bound"
        );
        for request in requests.iter() {
            assert_portable(&request.messages);
            assert_eq!(request.model, MODEL);
            let estimated =
                klaus_agent_core::context::tokens::estimate_context_tokens(&request.messages);
            assert!(
                estimated < SMALL_WINDOW,
                "Target request exceeded its context window: {estimated}"
            );
        }
        let evidence = requests
            .iter()
            .map(|request| encoded(&request.messages))
            .collect::<String>();
        for index in 0..24 {
            assert!(
                evidence.contains(&format!("ARCHIVE_{index}:")),
                "Recovery silently dropped original chunk {index}"
            );
        }
        let continuation = encoded(&requests.last().unwrap().messages);
        assert!(continuation.contains(LATEST));
        assert!(continuation.contains("TOOL_EVIDENCE"));
    }
    fixture.shutdown().await;
}

#[tokio::test]
async fn one_oversized_exchange_still_switches_to_the_selected_model() {
    let fixture = Fixture::new_giant(TargetBehavior::Complete, ProviderErrorKind::Auth).await;
    fixture.switch(SMALL_WINDOW);
    assert_success(&fixture.run().await);
    fixture.assert_no_old_calls();
    let requests = fixture.calls.target.lock().unwrap();
    assert!(!requests.is_empty(), "the selected model must be called");
    for request in requests.iter() {
        assert_portable(&request.messages);
        let estimated =
            klaus_agent_core::context::tokens::estimate_context_tokens(&request.messages);
        assert!(
            estimated < SMALL_WINDOW,
            "request still exceeds the selected window: {estimated}"
        );
    }
    let continuation = encoded(&requests.last().unwrap().messages);
    assert!(continuation.contains(LATEST), "{continuation}");
    drop(requests);
    fixture.shutdown().await;
}

async fn assert_failed_recovery_is_atomic(cancel: bool) {
    let fixture = Fixture::new(
        true,
        if cancel {
            TargetBehavior::PauseSecond
        } else {
            TargetBehavior::FailSecond
        },
        ProviderErrorKind::Quota,
    )
    .await;
    fixture.switch(SMALL_WINDOW);
    let before = fixture.compactions().await;
    let events = if cancel {
        let session = fixture.session.clone();
        let running = tokio::spawn(async move {
            session
                .run_streamed(Input::Text(LATEST.into()), TurnOptions::default())
                .events
                .collect::<Vec<_>>()
                .await
        });
        tokio::time::timeout(
            Duration::from_secs(10),
            fixture.calls.second_started.notified(),
        )
        .await
        .expect("The second target chunk must start before cancellation");
        fixture.session.interrupt_by_user();
        tokio::time::timeout(Duration::from_secs(10), running)
            .await
            .unwrap()
            .unwrap()
    } else {
        fixture.run().await
    };
    fixture.assert_no_old_calls();
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::Compaction { .. })),
        "An incomplete candidate must not publish success: {events:?}"
    );
    if !cancel {
        assert!(
            events
                .iter()
                .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
            "Target summary failure must surface: {events:?}"
        );
    }
    assert_eq!(
        fixture.compactions().await,
        before,
        "Failure must not durably replace the old checkpoint with a partial candidate"
    );
    let history = encoded(&fixture.session.history());
    assert!(
        !history.contains(PARTIAL),
        "Uncommitted summary output leaked into authoritative history"
    );
    assert!(history.contains("OPAQUE_CHECKPOINT_ONLY_OLD_SOURCE_CAN_READ"));
    assert!(
        history.contains(LATEST),
        "The admitted new input must survive a failed recovery"
    );
    for request in fixture.calls.target.lock().unwrap().iter() {
        assert_portable(&request.messages);
    }
    fixture.shutdown().await;
}

#[tokio::test]
async fn target_summary_failure_does_not_publish_a_partial_replacement() {
    assert_failed_recovery_is_atomic(false).await;
}

#[tokio::test]
async fn cancellation_after_one_completed_target_chunk_keeps_the_original_checkpoint() {
    assert_failed_recovery_is_atomic(true).await;
}

#[tokio::test]
async fn missing_original_source_fails_explicitly_before_target_inference() {
    let mut fixture = Fixture::new(false, TargetBehavior::Complete, ProviderErrorKind::Auth).await;
    let id = fixture.session.id.try_to_utf8().unwrap();
    fixture.session.shutdown_and_wait().await.unwrap();
    // Model a damaged durable source: retain the actual compaction and its watermark,
    // but remove the original records it references. This is never a normal fixture.
    let sessions = fixture._dir.path().join("sessions");
    let rollout = std::fs::read_dir(&sessions)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().is_some_and(|ext| ext == "jsonl"))
        .unwrap();
    let original = std::fs::read_to_string(&rollout).unwrap();
    let damaged = original
        .lines()
        .filter(|line| {
            !matches!(
                klaus_agent_core::store::rollout::parse_rollout_line(line),
                Some(RolloutItem::Message { .. })
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(&rollout, format!("{damaged}\n")).unwrap();
    let agent = Fixture::agent(
        &fixture.process,
        fixture._dir.path(),
        false,
        LARGE_WINDOW,
        &fixture.calls,
    );
    fixture.session = agent
        .resume_session(
            fixture.store.clone(),
            &id,
            StartSessionOptions {
                user_config_dir: Some(fixture._dir.path().to_string_lossy().into_owned().into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let events = fixture.run().await;
    let failure = events
        .iter()
        .find(|event| matches!(event, EngineEvent::TurnFailed { .. }))
        .expect("Missing raw history must be an explicit turn failure");
    let failure = format!("{failure:?}").to_lowercase();
    assert!(
        failure.contains("histor") || failure.contains("source") || failure.contains("recover"),
        "Missing raw source needs an actionable diagnostic: {failure}"
    );
    assert!(
        fixture.calls.target.lock().unwrap().is_empty(),
        "An opaque-only fallback must not be sent to the target"
    );
    fixture.assert_no_old_calls();
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_fork_recovers_ancestor_raw_input_without_inheriting_later_parent_messages() {
    let mut fixture = Fixture::new(false, TargetBehavior::Complete, ProviderErrorKind::Quota).await;
    let parent_id = fixture.session.id.try_to_utf8().unwrap();
    fixture.session.shutdown_and_wait().await.unwrap();
    let target = Fixture::agent(
        &fixture.process,
        fixture._dir.path(),
        false,
        LARGE_WINDOW,
        &fixture.calls,
    );
    fixture.session = target
        .fork_session(
            fixture.store.clone(),
            &parent_id,
            StartSessionOptions {
                user_config_dir: Some(fixture._dir.path().to_string_lossy().into_owned().into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    // The ancestor continues after the child captured its inherited checkpoint.
    let resumed = fixture.store.resume_for_write(&parent_id).await.unwrap();
    resumed.rollout.append_message(message(json!({
        "type":"user", "uuid":"later-parent-user", "content":[{
            "type":"text", "text":"LATER_PARENT_SECRET must stay outside the child's inherited prefix."
        }]
    }))).await;
    resumed.rollout.shutdown().await.unwrap();
    assert_success(&fixture.run().await);
    fixture.assert_no_old_calls();
    {
        let requests = fixture.calls.target.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_original_order(&requests[0]);
        assert!(
            !encoded(&requests[0].messages).contains("LATER_PARENT_SECRET"),
            "Recovery must respect the fork's ancestor source boundary"
        );
    }
    fixture.shutdown().await;
}
