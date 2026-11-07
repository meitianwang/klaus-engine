//! Defect: compaction hooks could reject work without preventing a rewrite,
//! or lose context that should be restored after an accepted summary.
//! Root cause: lifecycle results were consumed differently at manual and automatic entry points.
//! Work item: 01a1588f preserve hook decisions and restart context through Session.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::deps::{CompactionResult, Compactor};
use klaus_agent_core::engine::session::{Input, Session, TurnOptions};
use klaus_agent_core::hooks::types::{
    Hook, HookDecision, HookEvent, HookFn, HookHandler, HookInput, HookOutcome, HookResult,
    HookTrigger,
};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeString};
use klaus_agent_core::schemas::config::ResolvedConfig;
use klaus_agent_core::schemas::content::{ContentBlock, OutputBlock, TextBlock};
use klaus_agent_core::schemas::message::{
    AssistantMessage, CompactBoundaryMessage, Message, Usage, UserMessage,
};
use klaus_agent_core::tools::builtins::read::create_read_tool;
use klaus_agent_core::tools::builtins::write::create_write_tool;
use klaus_agent_core::tools::freshness::NOT_READ_MESSAGE;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::TerminalReason;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use serde_json::json;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

const MODEL: &str = "compact-hook-policy";
const SUMMARY: &str = "Accepted summary: preserve the existing implementation task.";
const RESTORED_CONTEXT: &str = "COMPACT_START_CONTEXT: keep the active validation constraint.";
const HOOK_INSTRUCTIONS: &str = "Preserve the exact active acceptance criteria.";
const ORIGINAL_FILE: &str = "original file contents\n";

#[derive(Clone, Copy)]
enum Entry {
    Manual,
    Automatic,
}

impl Entry {
    fn trigger(self) -> HookTrigger {
        match self {
            Self::Manual => HookTrigger::Manual,
            Self::Automatic => HookTrigger::Auto,
        }
    }
}

type HookCalls = Arc<Mutex<Vec<(String, HookInput)>>>;

struct RecordingHook {
    name: &'static str,
    calls: HookCalls,
    outcome: HookOutcome,
}

#[async_trait]
impl HookFn for RecordingHook {
    async fn run(&self, input: HookInput) -> HookOutcome {
        self.calls.lock().unwrap().push((self.name.into(), input));
        self.outcome.clone()
    }
}

fn hook(
    event: HookEvent,
    matcher: &str,
    name: &'static str,
    calls: &HookCalls,
    outcome: HookOutcome,
) -> Hook {
    Hook {
        event,
        matcher: Some(matcher.to_string()),
        handler: HookHandler::Fn(Arc::new(RecordingHook {
            name,
            calls: calls.clone(),
            outcome,
        })),
        is_async: false,
    }
}

struct ControlledCompactor {
    succeed: bool,
    calls: AtomicUsize,
    instructions: Mutex<Vec<Option<RuntimeString>>>,
    prepared_requests: Mutex<Vec<ModelRequest>>,
}

impl ControlledCompactor {
    fn compact(
        &self,
        messages: &[Message],
        instructions: Option<&RuntimeString>,
    ) -> CompactionResult {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.instructions
            .lock()
            .unwrap()
            .push(instructions.cloned());
        CompactionResult {
            messages: if self.succeed {
                vec![Message::CompactBoundary(CompactBoundaryMessage {
                    native_compaction: None,
                    uuid: "accepted-hook-boundary".into(),
                    summary: SUMMARY.into(),
                })]
            } else {
                messages.to_vec()
            },
            freed_tokens: if self.succeed { 25_000 } else { 0 },
            compacted: self.succeed,
            reason: None,
            diagnostics: Vec::new(),
        }
    }
}

#[async_trait]
impl Compactor for ControlledCompactor {
    fn prepare_request(&self, request: &ModelRequest, _signal: &AbortSignal) {
        self.prepared_requests.lock().unwrap().push(request.clone());
    }

    async fn manual_compact(
        &self,
        messages: &[Message],
        _config: &ResolvedConfig,
        instructions: Option<&RuntimeString>,
    ) -> CompactionResult {
        self.compact(messages, instructions)
    }

    async fn maybe_compact(
        &self,
        messages: &[Message],
        _estimated_tokens: u64,
        _config: &ResolvedConfig,
        instructions: Option<&RuntimeString>,
    ) -> CompactionResult {
        self.compact(messages, instructions)
    }

    async fn reactive_compact(
        &self,
        messages: &[Message],
        _config: &ResolvedConfig,
        instructions: Option<&RuntimeString>,
    ) -> CompactionResult {
        self.compact(messages, instructions)
    }
}

struct RecordingProvider {
    info: ProviderInfo,
    requests: Mutex<Vec<ModelRequest>>,
    file_operations: bool,
}

fn usage() -> Usage {
    Usage {
        input_tokens: 100,
        output_tokens: 10,
        cache_read_input_tokens: 0,
        cache_creation_input_tokens: 0,
        total_tokens: None,
    }
}

fn stop(reason: ModelStopReason) -> ModelStreamEvent {
    ModelStreamEvent::MessageStop {
        stop_reason: reason,
        usage: usage(),
    }
}

fn tool_request(id: &str, name: &str, arguments: serde_json::Value) -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::MessageStart {
            model: MODEL.into(),
        },
        ModelStreamEvent::ToolUseStart {
            id: id.into(),
            name: name.into(),
            thought_signature: None,
            tool_kind: None,
            input_format: None,
        },
        ModelStreamEvent::ToolUseInputDelta {
            id: id.into(),
            partial_json: arguments.to_string().into(),
        },
        ModelStreamEvent::ToolUseStop { id: id.into() },
        stop(ModelStopReason::ToolUse),
    ]
}

impl ModelProvider for RecordingProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, _model: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: self.file_operations,
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: 200_000,
            max_output_tokens: 1_000,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }

    fn stream(&self, request: ModelRequest, _signal: AbortSignal) -> ModelStream {
        let mut requests = self.requests.lock().unwrap();
        let index = requests.len();
        requests.push(request);
        drop(requests);
        let events = if self.file_operations && index == 0 {
            tool_request(
                "read-before-compact",
                "read",
                json!({"path": "tracked.txt"}),
            )
        } else if self.file_operations && index == 2 {
            tool_request(
                "write-after-compact",
                "write",
                json!({"path": "tracked.txt", "content": "unreviewed replacement\n"}),
            )
        } else {
            vec![
                ModelStreamEvent::MessageStart {
                    model: MODEL.into(),
                },
                ModelStreamEvent::TextDelta {
                    text: "The requested turn is complete.".into(),
                },
                stop(ModelStopReason::EndTurn),
            ]
        };
        Box::pin(futures::stream::iter(events))
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
                    uuid: format!("hook-history-user-{index}").into(),
                    content: vec![text("Continue the current implementation task.")],
                    is_meta: None,
                    question_answer: None,
                }),
                Message::Assistant(AssistantMessage {
                    uuid: format!("hook-history-assistant-{index}").into(),
                    content: vec![text("Earlier implementation details. ".repeat(1_000))],
                    model: MODEL.into(),
                    usage: None,
                }),
            ]
        })
        .collect()
}

struct FixtureDirectory(std::path::PathBuf);
impl FixtureDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("compaction-hooks-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn path(&self) -> &std::path::Path {
        &self.0
    }
}
impl Drop for FixtureDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Fixture {
    session: Arc<Session>,
    process: RuntimeProcessContext,
    directory: FixtureDirectory,
    provider: Arc<RecordingProvider>,
    compactor: Arc<ControlledCompactor>,
}

impl Fixture {
    fn new(entry: Entry, hooks: Vec<Hook>, succeed: bool, file_operations: bool) -> Self {
        let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
        let directory = FixtureDirectory::new();
        std::fs::write(directory.path().join("tracked.txt"), ORIGINAL_FILE).unwrap();
        let provider = Arc::new(RecordingProvider {
            info: ProviderInfo {
                name: MODEL.into(),
                default_model: MODEL.into(),
                base_url: None,
                env_key: None,
            },
            requests: Mutex::new(Vec::new()),
            file_operations,
        });
        let compactor = Arc::new(ControlledCompactor {
            succeed,
            calls: AtomicUsize::new(0),
            instructions: Mutex::new(Vec::new()),
            prepared_requests: Mutex::new(Vec::new()),
        });
        let mut options = agent_options(process.clone());
        options.provider = provider.clone();
        options.compactor = Some(compactor.clone());
        options.hooks = hooks;
        options.tools = Some(if file_operations {
            vec![create_read_tool(), create_write_tool()]
        } else {
            Vec::new()
        });
        options.config = Some(json!({
            "model": MODEL,
            "permissionMode": "bypassPermissions",
            "contextWindow": 200_000,
            "autoCompactWindow": if matches!(entry, Entry::Automatic) { Some(20_000) } else { None },
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
        }));
        if matches!(entry, Entry::Manual) {
            options
                .config
                .as_mut()
                .unwrap()
                .as_object_mut()
                .unwrap()
                .remove("autoCompactWindow");
        }
        let agent = Agent::new(options);
        let session = agent.start_session(StartSessionOptions {
            cwd: Some(directory.path().to_string_lossy().into_owned().into()),
            user_config_dir: Some(directory.path().to_string_lossy().into_owned().into()),
            initial_history: Some(if file_operations {
                Vec::new()
            } else {
                history()
            }),
            ..Default::default()
        });
        Self {
            session,
            process,
            directory,
            provider,
            compactor,
        }
    }

    async fn close(self) {
        self.session.shutdown_and_wait().await.unwrap();
        self.process.shutdown().await.unwrap();
    }
}

fn lifecycle_hooks(calls: &HookCalls) -> Vec<Hook> {
    vec![
        hook(
            HookEvent::PreCompact,
            "*",
            "pre",
            calls,
            HookOutcome {
                additional_context: Some(HOOK_INSTRUCTIONS.into()),
                ..Default::default()
            },
        ),
        hook(
            HookEvent::PostCompact,
            "*",
            "post",
            calls,
            HookOutcome::default(),
        ),
        hook(
            HookEvent::SessionStart,
            "compact",
            "compact-start",
            calls,
            HookOutcome {
                additional_context: Some(RESTORED_CONTEXT.into()),
                ..Default::default()
            },
        ),
    ]
}

#[tokio::test]
async fn manual_pre_compact_block_and_abort_preserve_history_without_summary_calls() {
    for outcome in [
        HookOutcome {
            decision: Some(HookDecision::Block),
            ..Default::default()
        },
        HookOutcome {
            result: Some(HookResult::FailedAbort),
            ..Default::default()
        },
    ] {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let fixture = Fixture::new(
            Entry::Manual,
            vec![hook(
                HookEvent::PreCompact,
                "manual",
                "pre",
                &calls,
                outcome,
            )],
            true,
            false,
        );
        let before = fixture.session.history();
        let result = fixture
            .session
            .compact(Some("user summary requirement".into()))
            .await;
        assert_eq!(result.unwrap_err(), "pre-compact-hook-rejected");
        assert_eq!(fixture.compactor.calls.load(Ordering::SeqCst), 0);
        assert!(fixture.provider.requests.lock().unwrap().is_empty());
        assert_eq!(fixture.session.history(), before);
        let inputs = calls.lock().unwrap().clone();
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].1.trigger, Some(HookTrigger::Manual));
        fixture.close().await;
    }
}

async fn assert_successful_hook_lifecycle(entry: Entry) {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let fixture = Fixture::new(entry, lifecycle_hooks(&calls), true, false);
    assert!(calls.lock().unwrap().is_empty());
    match entry {
        Entry::Manual => {
            assert!(fixture.session.compact(None).await.unwrap().compacted);
            let outcome = fixture
                .session
                .run(
                    Input::Text("continue after manual compact".into()),
                    TurnOptions::default(),
                )
                .await;
            assert_eq!(outcome.stop_reason, TerminalReason::Completed);
        }
        Entry::Automatic => {
            let outcome = fixture
                .session
                .run(
                    Input::Text("continue and compact when necessary".into()),
                    TurnOptions::default(),
                )
                .await;
            assert_eq!(outcome.stop_reason, TerminalReason::Completed);
        }
    }
    assert_eq!(fixture.compactor.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.compactor.instructions.lock().unwrap().as_slice(),
        &[Some(RuntimeString::from(HOOK_INSTRUCTIONS))]
    );
    let inputs = calls.lock().unwrap().clone();
    assert_eq!(
        inputs
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>(),
        vec!["pre", "post"]
    );
    let post = &inputs[1].1;
    assert_eq!(post.event, HookEvent::PostCompact);
    assert_eq!(post.trigger, Some(entry.trigger()));
    assert_eq!(post.message, Some(SUMMARY.into()));
    let requests = fixture.provider.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    let rendered = runtime_json::stringify(
        &klaus_agent_core::runtime_values::runtime_codec::encode_value(&requests[0].messages)
            .unwrap(),
    );
    assert!(!rendered.contains(RESTORED_CONTEXT));
    assert!(!fixture
        .session
        .history()
        .iter()
        .any(|message| matches!(message,
        Message::User(user) if user.is_meta == Some(true) && user.content.iter().any(|block|
            matches!(block, ContentBlock::Text(text) if text.text.contains(RESTORED_CONTEXT))))));
    fixture.close().await;
}

#[tokio::test]
async fn manual_compaction_forwards_accepted_summary_without_restarting_the_session() {
    assert_successful_hook_lifecycle(Entry::Manual).await;
}

#[tokio::test]
async fn automatic_compaction_forwards_accepted_summary_without_restarting_the_session() {
    assert_successful_hook_lifecycle(Entry::Automatic).await;
}

#[tokio::test]
async fn unsuccessful_compaction_does_not_run_post_or_compact_session_start_hooks() {
    for entry in [Entry::Manual, Entry::Automatic] {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let fixture = Fixture::new(entry, lifecycle_hooks(&calls), false, false);
        let before = fixture.session.history();
        match entry {
            Entry::Manual => {
                assert!(!fixture.session.compact(None).await.unwrap().compacted);
                assert_eq!(fixture.session.history(), before);
            }
            Entry::Automatic => {
                let outcome = fixture
                    .session
                    .run(
                        Input::Text("continue without a successful compact".into()),
                        TurnOptions::default(),
                    )
                    .await;
                assert_eq!(outcome.stop_reason, TerminalReason::Error);
                assert!(outcome
                    .error
                    .as_ref()
                    .unwrap()
                    .message
                    .contains("Context compaction failed"));
                assert!(!fixture
                    .session
                    .history()
                    .iter()
                    .any(|message| matches!(message, Message::CompactBoundary(_))));
            }
        }
        assert_eq!(fixture.compactor.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            calls
                .lock()
                .unwrap()
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>(),
            vec!["pre"]
        );
        fixture.close().await;
    }
}

#[tokio::test]
async fn manual_compaction_requires_a_fresh_read_before_writing_an_existing_file() {
    let fixture = Fixture::new(Entry::Manual, Vec::new(), true, true);
    let read = fixture
        .session
        .run(
            Input::Text("read tracked.txt".into()),
            TurnOptions::default(),
        )
        .await;
    assert_eq!(read.stop_reason, TerminalReason::Completed);
    assert!(fixture.session.history().iter().any(|message| matches!(message,
        Message::ToolResult(result) if result.tool_use_id == "read-before-compact" && !result.is_error)));
    assert_eq!(fixture.provider.requests.lock().unwrap().len(), 2);
    assert!(fixture.session.compact(None).await.unwrap().compacted);
    let write = fixture
        .session
        .run(
            Input::Text("write tracked.txt without reading again".into()),
            TurnOptions::default(),
        )
        .await;
    assert_eq!(write.stop_reason, TerminalReason::Completed);
    let after = fixture.session.history();
    let failure = after
        .iter()
        .find_map(|message| match message {
            Message::ToolResult(result) if result.tool_use_id == "write-after-compact" => {
                Some(result)
            }
            _ => None,
        })
        .expect("write request must pass through the ordinary tool pipeline");
    assert!(failure.is_error);
    assert!(failure.content.iter().any(
        |block| matches!(block, OutputBlock::Text(text) if text.text.contains(NOT_READ_MESSAGE))
    ));
    assert_eq!(
        std::fs::read_to_string(fixture.directory.path().join("tracked.txt")).unwrap(),
        ORIGINAL_FILE
    );
    fixture.close().await;
}

#[tokio::test]
async fn manual_compaction_loads_current_workspace_instructions_without_a_prior_turn() {
    let fixture = Fixture::new(Entry::Manual, Vec::new(), true, false);
    std::fs::write(
        fixture.directory.path().join("AGENTS.md"),
        "CURRENT_MANUAL_WORKSPACE_INSTRUCTION",
    )
    .unwrap();
    assert!(fixture.session.compact(None).await.unwrap().compacted);
    assert!(fixture.provider.requests.lock().unwrap().is_empty());
    let prepared = fixture.compactor.prepared_requests.lock().unwrap().clone();
    assert!(
        !prepared.is_empty(),
        "manual summary must capture a real request prefix"
    );
    let rendered = runtime_json::stringify(
        &klaus_agent_core::runtime_values::runtime_codec::encode_value(
            &prepared.last().unwrap().messages,
        )
        .unwrap(),
    );
    assert!(rendered.contains("CURRENT_MANUAL_WORKSPACE_INSTRUCTION"));
    fixture.close().await;
}

#[tokio::test]
async fn manual_compaction_refreshes_cached_workspace_instructions_for_the_next_turn() {
    let fixture = Fixture::new(Entry::Manual, Vec::new(), true, false);
    let agents = fixture.directory.path().join("AGENTS.md");
    std::fs::write(&agents, "EARLIER_CACHED_WORKSPACE_INSTRUCTION").unwrap();
    let first = fixture
        .session
        .run(
            Input::Text("prime the instruction prefix".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(first.error.is_none());
    std::fs::write(agents, "REFRESHED_WORKSPACE_INSTRUCTION").unwrap();
    assert!(fixture.session.compact(None).await.unwrap().compacted);
    let next = fixture
        .session
        .run(
            Input::Text("continue after summary".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(next.error.is_none());
    let requests = fixture.provider.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    let before = runtime_json::stringify(
        &klaus_agent_core::runtime_values::runtime_codec::encode_value(&requests[0].messages)
            .unwrap(),
    );
    let after = runtime_json::stringify(
        &klaus_agent_core::runtime_values::runtime_codec::encode_value(&requests[1].messages)
            .unwrap(),
    );
    assert!(before.contains("EARLIER_CACHED_WORKSPACE_INSTRUCTION"));
    assert!(after.contains("REFRESHED_WORKSPACE_INSTRUCTION"));
    assert!(!after.contains("EARLIER_CACHED_WORKSPACE_INSTRUCTION"));
    fixture.close().await;
}
