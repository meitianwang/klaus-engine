//! Goal mention expansion retains owned content across accounting changes and
//! refuses stale control authority after asynchronous reads.

use super::*;
use klaus_agent_core::goal::store;
use klaus_agent_core::goal::types::{GoalAccountingMode, GoalUpdate, ThreadGoal};
use klaus_agent_core::schemas::content::TextBlock;
use klaus_agent_core::types::errors::TerminalReason;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Semaphore;

const WAIT: Duration = Duration::from_secs(5);
const READ_TEXT: &str = "OWNED_READ_TEXT <data> & exact payload";
const EXTRA_TEXT: &str = "OWNED_EXTRA_TEXT \r\nunchanged";

fn text(value: impl Into<RuntimeString>) -> TextBlock {
    TextBlock {
        text: value.into(),
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    }
}

fn read_text() -> TextBlock {
    let mut block = text(READ_TEXT);
    block.message_id = Some("owned-read-text".into());
    block.phase = Some("analysis".into());
    block
}

fn extra_text() -> TextBlock {
    let mut block = text(EXTRA_TEXT);
    block.message_id = Some("owned-extra-text".into());
    block.phase = Some("final".into());
    block
}

fn usage() -> Usage {
    Usage {
        input_tokens: 11,
        output_tokens: 7,
        cache_read_input_tokens: 31,
        cache_creation_input_tokens: 29,
        total_tokens: Some(999),
    }
}

fn now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

struct ReadRelease(Arc<Semaphore>);
impl Drop for ReadRelease {
    fn drop(&mut self) {
        self.0.add_permits(1);
    }
}

struct HeldRead {
    image: ImageBlock,
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Tool for HeldRead {
    fn name(&self) -> RuntimeString {
        "read".into()
    }
    fn description(&self) -> RuntimeString {
        "Read the owned image and full typed extra payload.".into()
    }
    fn input_schema(&self) -> Json {
        runtime_json::parse(&RuntimeString::from(r#"{"type":"object"}"#)).unwrap()
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_read_only(&self, _: &Json) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _: &Json) -> bool {
        true
    }
    async fn call(
        &self,
        input: Json,
        _: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        assert_eq!(
            self.calls.fetch_add(1, Ordering::SeqCst),
            0,
            "one actual mention read"
        );
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
        let mut result = text_result(input, "read fixture", false);
        result.serializable.model_content = vec![
            OutputBlock::Text(read_text()),
            OutputBlock::Image(self.image.clone()),
        ];
        result.serializable.extra_user_blocks =
            Some(vec![ContentBlock::Text(extra_text()), nested(&self.image)]);
        Ok(result)
    }
}

struct CaptureProvider {
    info: ProviderInfo,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
}
impl ModelProvider for CaptureProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }
    fn capabilities(&self, model: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: true,
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: 200_000,
            max_output_tokens: 8_192,
            native_pdf: false,
            vision: true,
            image_limits: Some(limits(model)),
            max_media_blocks: NonZeroU32::new(20),
        }
    }
    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        let model = request.model.clone();
        let mut requests = self.requests.lock().unwrap();
        assert!(
            requests.len() < 2,
            "only foreground and one admitted continuation may call the provider"
        );
        requests.push(request);
        drop(requests);
        Box::pin(futures::stream::iter(vec![
            ModelStreamEvent::MessageStart { model },
            ModelStreamEvent::TextDelta {
                text: "The requested image check made measured progress.".into(),
            },
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: usage(),
            },
        ]))
    }
}

struct HeldFixture {
    session: Arc<Session>,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
    read_calls: Arc<AtomicUsize>,
    original: ImageBlock,
    path: std::path::PathBuf,
    process: RuntimeProcessContext,
    _directory: FixtureDirectory,
}
impl HeldFixture {
    async fn new() -> Self {
        let directory = FixtureDirectory::new();
        let path = directory.path.join("owned-mention.png");
        std::fs::write(
            &path,
            b"real mention path; typed content is supplied by read",
        )
        .unwrap();
        let original = image(512, 256, false, "owned-source.png");
        let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let entered = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let read_calls = Arc::new(AtomicUsize::new(0));
        let mut options = agent_options(process.clone());
        options.provider = Arc::new(CaptureProvider {
            info: ProviderInfo {
                name: "held-mention-fixture".into(),
                default_model: "tall".into(),
                base_url: None,
                env_key: None,
            },
            requests: requests.clone(),
        });
        options.include_default_agents = Some(false);
        options.tools = Some(vec![Arc::new(HeldRead {
            image: original.clone(),
            entered: entered.clone(),
            release: release.clone(),
            calls: read_calls.clone(),
        })]);
        let agent = Agent::new(options);
        let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(
                directory
                    .path
                    .join("sessions")
                    .to_string_lossy()
                    .into_owned(),
            ),
            archive_dir: None,
        }));
        let session = agent.open_session(store, StartSessionOptions {
            cwd: Some(directory.path.to_string_lossy().into_owned().into()),
            user_config_dir: Some(directory.path.to_string_lossy().into_owned().into()),
            config: Some(json!({"model":"tall","goal":true,"prewarm":false,"thinking":false,"fork":false,"autoMemory":false,"autoDream":false,"agentMemory":false,"memoryRecall":false,"cron":false,"speculativeCompaction":false,"earlyMicrocompact":false,"idleMicrocompact":false,"compactionSelfCheck":false,"loopDetection":false,"maxRetries":0})),
            ..Default::default()
        }).await.unwrap();
        Self {
            session,
            requests,
            entered,
            release,
            read_calls,
            original,
            path,
            process,
            _directory: directory,
        }
    }
    fn objective(&self) -> RuntimeString {
        format!(
            "Inspect @{} and preserve <control> & the attachment.",
            self.path.display()
        )
        .into()
    }
    fn key(&self) -> String {
        self.session.id.try_to_utf8().unwrap()
    }
    async fn start(&self, budget: i64) -> ThreadGoal {
        let created = self
            .session
            .set_thread_goal(Some(self.objective()), None, Some(Some(budget)))
            .await
            .unwrap()
            .goal;
        let result = self
            .session
            .run(
                Input::Text("Begin the owned image check.".into()),
                TurnOptions::default(),
            )
            .await;
        assert!(result.error.is_none(), "turn error: {:?}", result.error);
        assert_eq!(self.requests.lock().unwrap().len(), 1);
        assert_eq!(self.read_calls.load(Ordering::SeqCst), 0);
        let saved = self.session.thread_goal().await.unwrap().unwrap();
        assert_eq!(saved.goal_id, created.goal_id);
        assert_eq!(
            saved.tokens_used, 18,
            "only actual disjoint provider input/output is charged"
        );
        saved
    }
    async fn hold(&self) -> (tokio::task::JoinHandle<Vec<EngineEvent>>, ReadRelease) {
        let release = ReadRelease(self.release.clone());
        let turn = self
            .session
            .run_goal_idle_streamed(TurnOptions::default())
            .await
            .unwrap()
            .expect("the persistent ordinary goal must admit one real idle turn");
        let consumer = tokio::spawn(async move {
            let mut stream = turn.events;
            let mut events = Vec::new();
            while let Some(event) = stream.next().await {
                assert!(events.len() < 128, "bounded held-read event fixture");
                events.push(event);
            }
            events
        });
        tokio::time::timeout(WAIT, self.entered.acquire())
            .await
            .expect("the actual read must enter before mutation")
            .unwrap()
            .forget();
        assert_eq!(self.read_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            self.requests.lock().unwrap().len(),
            1,
            "the continuation has not entered its provider"
        );
        (consumer, release)
    }
    fn raw_expansion(&self) -> Vec<ContentBlock> {
        vec![
            ContentBlock::Text(text(format!("Contents of @{}:", self.path.display()))),
            ContentBlock::Text(read_text()),
            ContentBlock::Image(self.original.clone()),
            ContentBlock::Text(extra_text()),
            nested(&self.original),
        ]
    }
    async fn finish(self) {
        self.session.shutdown_and_wait().await.unwrap();
        self.process.shutdown().await.unwrap();
    }
}

async fn finish_turn(
    mut consumer: tokio::task::JoinHandle<Vec<EngineEvent>>,
    release: ReadRelease,
) -> Vec<EngineEvent> {
    drop(release);
    match tokio::time::timeout(WAIT, &mut consumer).await {
        Ok(events) => events.unwrap(),
        Err(_) => {
            consumer.abort();
            let _ = consumer.await;
            panic!("the held read turn must settle after release");
        }
    }
}

async fn accounting_only_case(budget: i64) {
    let fixture = HeldFixture::new().await;
    let original_goal = fixture.start(budget).await;
    let history_before = fixture.session.history();
    let (consumer, release) = fixture.hold().await;
    let before = store::get_record(&fixture.key()).await.unwrap();
    // This is a genuine external durable accounting writer, not a second Session turn.
    store::account_usage(
        &fixture.key(),
        GoalAccountingMode::ActiveOnly,
        37,
        2,
        Some(original_goal.goal_id.clone()),
        now_ms(),
    )
    .await
    .unwrap();
    let after = store::get_record(&fixture.key()).await.unwrap();
    assert_eq!(
        after.intent_revision, before.intent_revision,
        "accounting preserves durable control authority"
    );
    assert!(
        after.revision > before.revision,
        "the accounting update actually committed"
    );
    let fresh = after.goal.unwrap();
    assert_eq!(fresh.tokens_used, 55);
    assert_eq!(
        fresh.time_used_seconds,
        before.goal.unwrap().time_used_seconds + 2
    );
    assert_eq!(fresh.token_budget, Some(budget));
    assert_eq!(
        fresh.status,
        if budget == 50 {
            ThreadGoalStatus::BudgetLimited
        } else {
            ThreadGoalStatus::Active
        }
    );
    let events = finish_turn(consumer, release).await;
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{events:?}"
    );
    assert!(
        events.iter().any(|event| matches!(
            event,
            EngineEvent::TurnCompleted {
                stop_reason: TerminalReason::Completed,
                ..
            }
        )),
        "{events:?}"
    );
    let requests = fixture.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    assert!(images(&requests[0].messages).is_empty());
    let user = requests[1]
        .messages
        .iter()
        .find_map(|message| match message {
            Message::User(user)
                if user.content.iter().any(
                    |block| matches!(block, ContentBlock::Text(block) if block.text == READ_TEXT),
                ) =>
            {
                Some(user)
            }
            _ => None,
        })
        .expect("the current controlled user contains the actual complete expansion");
    let [ContentBlock::Text(prompt), ContentBlock::Text(mention), ContentBlock::Text(read), ContentBlock::Image(direct), ContentBlock::Text(extra), ContentBlock::ToolResult(nested_result)] =
        user.content.as_slice()
    else {
        panic!(
            "the exact fresh-prefix/read/extra order must survive: {:?}",
            user.content
        );
    };
    assert_eq!(
        mention,
        &text(format!("Contents of @{}:", fixture.path.display()))
    );
    assert_eq!(read, &read_text());
    assert_eq!(extra, &extra_text());
    assert!(
        prompt.text.contains("- Tokens used: 55"),
        "{:?}",
        prompt.text
    );
    assert!(
        prompt.text.contains(&format!("- Token budget: {budget}")),
        "{:?}",
        prompt.text
    );
    assert!(!prompt.text.contains("- Tokens used: 18"));
    assert!(prompt
        .text
        .contains("&lt;control&gt; &amp; the attachment."));
    if budget == 50 {
        assert!(
            prompt.text.contains(&format!(
                "- Time spent pursuing goal: {} seconds",
                fresh.time_used_seconds
            )),
            "{:?}",
            prompt.text
        );
        assert!(prompt.text.contains("do not start new substantive work"));
    } else {
        assert!(prompt.text.contains("- Tokens remaining: 945"));
    }
    assert_eq!(
        nested_result.tool_use_id,
        RuntimeString::from("nested-image")
    );
    assert_eq!(nested_result.is_error, None);
    assert_eq!(
        nested_result.content,
        vec![OutputBlock::Image(direct.clone())]
    );
    let admitted = images(&requests[1].messages);
    assert_eq!(
        admitted.len(),
        2,
        "both direct and nested extra images must survive"
    );
    for image in admitted {
        assert_dimensions(&image, 128, 64);
        assert!(image.source_path.is_none());
    }
    assert_eq!(fixture.read_calls.load(Ordering::SeqCst), 1);
    let history = fixture.session.history();
    assert_eq!(
        &history[..history_before.len()],
        history_before.as_slice(),
        "the existing history prefix cannot be rewritten"
    );
    let historical = history
        .iter()
        .find_map(|message| match message {
            Message::User(saved) if saved.uuid == user.uuid => Some(saved),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        &historical.content[1..],
        fixture.raw_expansion().as_slice(),
        "history retains complete original read and extra block bytes"
    );
    assert_eq!(images(&history), vec![fixture.original.clone(); 2]);
    let settled = fixture.session.thread_goal().await.unwrap().unwrap();
    assert_eq!(settled.goal_id, original_goal.goal_id);
    assert_eq!(
        settled.tokens_used, 73,
        "actual18 foreground + external37 + actual18 continuation, charged once"
    );
    assert_eq!(settled.status, fresh.status);
    assert!(settled.time_used_seconds >= fresh.time_used_seconds);
    // Clear the owned goal only after accounting and payload assertions have settled.
    assert!(fixture.session.clear_thread_goal().await.unwrap());
    assert!(fixture.session.thread_goal().await.unwrap().is_none());
    assert_eq!(fixture.requests.lock().unwrap().len(), 2);
    fixture.finish().await;
}

#[tokio::test]
async fn held_mention_retains_typed_content_and_refreshes_accounting_without_revoking_authority() {
    accounting_only_case(1000).await;
}

#[tokio::test]
async fn held_mention_budget_wrapup_uses_current_accounting_time_and_retains_original_history() {
    accounting_only_case(50).await;
}

#[derive(Clone, Copy, Debug)]
enum ControlChange {
    ObjectiveEdit,
    Replacement,
    SavedWriterAba,
}

async fn revoked_case(change: ControlChange) {
    let fixture = HeldFixture::new().await;
    let original = fixture.start(1000).await;
    let (consumer, release) = fixture.hold().await;
    let before = store::get_record(&fixture.key()).await.unwrap();
    match change {
        ControlChange::ObjectiveEdit => {
            let edited = fixture
                .session
                .set_thread_goal(
                    Some("A different current objective with no old attachment.".into()),
                    None,
                    None,
                )
                .await
                .unwrap()
                .goal;
            assert_eq!(
                edited.goal_id, original.goal_id,
                "objective edits retain the generation"
            );
        }
        ControlChange::Replacement => {
            assert!(fixture.session.clear_thread_goal().await.unwrap());
            let replacement = fixture
                .session
                .set_thread_goal(
                    Some("A replacement objective with no old attachment.".into()),
                    None,
                    Some(Some(1000)),
                )
                .await
                .unwrap()
                .goal;
            assert_ne!(replacement.goal_id, original.goal_id);
        }
        ControlChange::SavedWriterAba => {
            // The external writer leaves local atomics untouched; durable intent must fence ABA.
            for objective in [
                RuntimeString::from("A temporary saved objective."),
                fixture.objective(),
            ] {
                let saved = store::update_goal(
                    &fixture.key(),
                    GoalUpdate {
                        objective: Some(objective),
                        expected_goal_id: Some(original.goal_id.clone()),
                        ..Default::default()
                    },
                    now_ms(),
                )
                .await
                .unwrap()
                .unwrap()
                .unwrap();
                assert_eq!(saved.goal_id, original.goal_id);
            }
        }
    }
    let controlled = store::get_record(&fixture.key()).await.unwrap();
    assert!(
        controlled.intent_revision > before.intent_revision,
        "{change:?}"
    );
    let expected = controlled.goal.unwrap();
    if matches!(change, ControlChange::SavedWriterAba) {
        assert_eq!(expected.objective, original.objective);
    }
    let events = finish_turn(consumer, release).await;
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "supersession is an abort, not a runtime failure: {events:?}"
    );
    assert!(
        events.iter().any(|event| matches!(
            event,
            EngineEvent::TurnCompleted {
                stop_reason: TerminalReason::Aborted,
                ..
            }
        )),
        "the old controlled input must abort: {change:?}: {events:?}"
    );
    assert_eq!(fixture.read_calls.load(Ordering::SeqCst), 1);
    let requests = fixture.requests.lock().unwrap().clone();
    assert_eq!(
        requests.len(),
        1,
        "an old expansion may not enter a provider with a new prompt: {change:?}"
    );
    assert!(images(&requests[0].messages).is_empty());
    let saved = fixture.session.thread_goal().await.unwrap().unwrap();
    assert_eq!(saved.goal_id, expected.goal_id);
    assert_eq!(saved.objective, expected.objective);
    assert_eq!(saved.status, ThreadGoalStatus::Active);
    assert_eq!(
        saved.tokens_used, expected.tokens_used,
        "no unadmitted provider usage may be charged"
    );
    assert_eq!(saved.token_budget, expected.token_budget);
    fixture.finish().await;
}

#[tokio::test]
async fn a_same_generation_edit_during_actual_read_rejects_the_old_expansion() {
    revoked_case(ControlChange::ObjectiveEdit).await;
}

#[tokio::test]
async fn a_replacement_during_actual_read_cannot_adopt_the_old_expansion() {
    revoked_case(ControlChange::Replacement).await;
}

#[tokio::test]
async fn saved_writer_objective_aba_during_actual_read_does_not_restore_expansion_authority() {
    revoked_case(ControlChange::SavedWriterAba).await;
}
