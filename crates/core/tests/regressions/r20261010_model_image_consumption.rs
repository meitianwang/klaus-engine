//! Defect: image admission used default limits while queue projections and
//! tool persistence irreversibly resized payloads before the next model capture.
//! Root cause: media policy ran on stored messages rather than the owned request
//! view, and tool extra-user blocks bypassed the image gate.
//! Work item: 2026-10-10 captured-model image consumption.
//! Scope: public Session requests, actual tool/queue consumption, goal delivery.

use std::num::NonZeroU32;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use futures::StreamExt;
use klaus_agent_core::context::images::image_dimensions;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::message_queue::{
    queued_value_has_model_content, queued_value_to_content_blocks,
    queued_value_to_tool_result_parts, MessageQueue, QueueEdit, QueueMutationStatus, QueuePriority,
    QueuedMessage, QueuedValue, SteerDelivery,
};
use klaus_agent_core::engine::session::{
    EnqueueOptions, Input, Session, TurnOptions, UserContentPart,
};
use klaus_agent_core::goal::types::ThreadGoalStatus;
use klaus_agent_core::runtime_process::{
    remove_process_environment_var, set_process_environment_var, RuntimeProcessContext,
};
use klaus_agent_core::runtime_values::{runtime_json, RuntimeString};
use klaus_agent_core::schemas::content::{ContentBlock, ImageBlock, OutputBlock, ToolResultBlock};
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::tools::build_tool::text_result;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::ToolError;
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::json::Json;
use klaus_agent_core::types::provider::{
    ImageLimits, ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};
use serde_json::json;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

fn image(width: u32, height: u32, noisy: bool, path: &str) -> ImageBlock {
    let mut state = 0x1234_5678_u32;
    let pixels = image::RgbImage::from_fn(width, height, |_, _| {
        if noisy {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            image::Rgb([state as u8, (state >> 8) as u8, (state >> 16) as u8])
        } else {
            image::Rgb([12, 34, 56])
        }
    });
    let mut bytes = Vec::new();
    pixels
        .write_with_encoder(image::codecs::png::PngEncoder::new(&mut bytes))
        .unwrap();
    ImageBlock {
        mime_type: "image/png".into(),
        base64: BASE64_STANDARD.encode(bytes).into(),
        detail: None,

        source_path: Some(path.into()),
    }
}

fn input(image: &ImageBlock) -> Input {
    Input::Parts(vec![UserContentPart::Image {
        mime_type: image.mime_type.clone(),
        base64: image.base64.clone(),
        source_path: image.source_path.clone(),
    }])
}

fn nested(image: &ImageBlock) -> ContentBlock {
    ContentBlock::ToolResult(ToolResultBlock {
        tool_use_id: "nested-image".into(),
        content: vec![OutputBlock::Image(image.clone())],
        is_error: None,
    })
}

fn content_images(blocks: &[ContentBlock], images: &mut Vec<ImageBlock>) {
    for block in blocks {
        match block {
            ContentBlock::Image(image) => images.push(image.clone()),
            ContentBlock::ToolResult(result) => output_images(&result.content, images),
            _ => {}
        }
    }
}

fn output_images(blocks: &[OutputBlock], images: &mut Vec<ImageBlock>) {
    for block in blocks {
        if let OutputBlock::Image(image) = block {
            images.push(image.clone());
        }
    }
}

fn images(messages: &[Message]) -> Vec<ImageBlock> {
    let mut images = Vec::new();
    for message in messages {
        match message {
            Message::User(user) => content_images(&user.content, &mut images),
            Message::Assistant(assistant) => content_images(&assistant.content, &mut images),
            Message::ToolResult(result) => {
                output_images(&result.content, &mut images);
                if let Some(extra) = &result.extra_user_blocks {
                    content_images(extra, &mut images);
                }
            }
            _ => {}
        }
    }
    images
}

fn assert_dimensions(image: &ImageBlock, width: u32, height: u32) {
    let dimensions = image_dimensions(image).expect("request image must remain decodable");
    assert_eq!((dimensions.width, dimensions.height), (width, height));
}

fn limits(model: &RuntimeString) -> ImageLimits {
    let (width, height, cap) = match model.try_to_utf8().unwrap().as_str() {
        "wide" => (3_000, 1_500, 200_000),
        "tall" => (128, 256, 200_000),
        "encoded" => (512, 512, 16_000),
        _ => (256, 128, 200_000),
    };
    ImageLimits {
        max_width: NonZeroU32::new(width),
        max_height: NonZeroU32::new(height),
        max_base64_size: NonZeroU32::new(cap),
        target_raw_size: None,
    }
}

#[derive(Clone, Copy)]
enum Scenario {
    Text,
    Tool { switch_to: Option<&'static str> },
    GoalContinuation,
}

struct Provider {
    info: ProviderInfo,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
    session: Arc<Mutex<Weak<Session>>>,
    calls: AtomicUsize,
    scenario: Scenario,
}

impl ModelProvider for Provider {
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
            vision: model != "blind",
            image_limits: Some(limits(model)),
            max_media_blocks: NonZeroU32::new(20),
        }
    }

    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        self.requests.lock().unwrap().push(request.clone());
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let session = self.session.lock().unwrap().upgrade().unwrap();
        let scenario = self.scenario;
        Box::pin(async_stream::stream! {
            if let Scenario::Tool { switch_to: Some(model) } = scenario {
                if call == 0 {
                    session.set_active_turn_model(model).unwrap();
                }
            }
            if matches!(scenario, Scenario::GoalContinuation) && call == 1 {
                session.goal_runtime().unwrap()
                    .update_goal_status(ThreadGoalStatus::Complete).await;
            }
            yield ModelStreamEvent::MessageStart { model: request.model.clone() };
            let tool = matches!(scenario, Scenario::Tool { .. }) && call == 0;
            if tool {
                yield ModelStreamEvent::ToolUseStart {
                    id: "media-call".into(), name: "media_output".into(),
                    thought_signature: None, tool_kind: None, input_format: None,
                };
                yield ModelStreamEvent::ToolUseInputDelta {
                    id: "media-call".into(), partial_json: "{}".into(),
                };
                yield ModelStreamEvent::ToolUseStop { id: "media-call".into() };
            } else {
                assert!(call < 8, "unexpected continuation loop");
                yield ModelStreamEvent::TextDelta { text: "The requested check is complete.".into() };
            }
            yield ModelStreamEvent::MessageStop {
                stop_reason: if tool { ModelStopReason::ToolUse } else { ModelStopReason::EndTurn },
                usage: Usage {
                    input_tokens: 1, output_tokens: 1, cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0, total_tokens: None,
                },
            };
        })
    }
}

struct MediaTool {
    name: &'static str,
    image: ImageBlock,
    extra: Vec<ContentBlock>,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Tool for MediaTool {
    fn static_name(&self) -> &str {
        self.name
    }

    fn static_description(&self) -> &str {
        "Return a deterministic image fixture."
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
        _: Json,
        _: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut result = text_result(Json::Null, "image fixture", false);
        result.serializable.model_content = vec![OutputBlock::Image(self.image.clone())];
        result.serializable.extra_user_blocks = Some(self.extra.clone());
        Ok(result)
    }
}

struct FixtureDirectory {
    path: std::path::PathBuf,
    previous_config_dir: Option<std::ffi::OsString>,
}

impl FixtureDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("model-image-goal-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        let previous_config_dir = std::env::var_os("KLAUS_CONFIG_DIR");
        set_process_environment_var("KLAUS_CONFIG_DIR", &path);
        Self {
            path,
            previous_config_dir,
        }
    }
}

impl Drop for FixtureDirectory {
    fn drop(&mut self) {
        match self.previous_config_dir.take() {
            Some(previous) => set_process_environment_var("KLAUS_CONFIG_DIR", previous),
            None => remove_process_environment_var("KLAUS_CONFIG_DIR"),
        }
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

struct Fixture {
    session: Arc<Session>,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
    tool_calls: Arc<AtomicUsize>,
}

impl Fixture {
    async fn new(
        model: &str,
        scenario: Scenario,
        image: ImageBlock,
        extra: Vec<ContentBlock>,
        persistent_root: Option<&std::path::Path>,
    ) -> Self {
        let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let session_slot = Arc::new(Mutex::new(Weak::new()));
        let provider = Arc::new(Provider {
            info: ProviderInfo {
                name: "image-consumption-fixture".into(),
                default_model: model.into(),
                base_url: None,
                env_key: None,
            },
            requests: requests.clone(),
            session: session_slot.clone(),
            calls: AtomicUsize::new(0),
            scenario,
        });
        let tool_calls = Arc::new(AtomicUsize::new(0));
        let mut options = agent_options(process);
        options.provider = provider;
        options.include_default_agents = Some(false);
        options.tools = Some(vec![Arc::new(MediaTool {
            name: if matches!(scenario, Scenario::GoalContinuation) {
                "read"
            } else {
                "media_output"
            },
            image,
            extra,
            calls: tool_calls.clone(),
        })]);
        let agent = Agent::new(options);
        let session_options = StartSessionOptions {
            cwd: persistent_root.map(|root| root.to_string_lossy().into_owned().into()),
            user_config_dir: persistent_root.map(|root| root.to_string_lossy().into_owned().into()),
            config: Some(json!({
                "model": model, "prewarm": false,
                "goal": matches!(scenario, Scenario::GoalContinuation),
                "thinking": false, "fork": false,
                "autoMemory": false, "autoDream": false, "memoryRecall": false,
                "speculativeCompaction": false, "earlyMicrocompact": false,
                "idleMicrocompact": false, "compactionSelfCheck": false,
                "loopDetection": false,
            })),
            ..Default::default()
        };
        let session = match persistent_root {
            Some(root) => {
                let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
                    dir: Some(root.join("sessions").to_string_lossy().into_owned()),
                    archive_dir: None,
                }));
                agent.open_session(store, session_options).await.unwrap()
            }
            None => agent.start_session(session_options),
        };
        *session_slot.lock().unwrap() = Arc::downgrade(&session);
        Self {
            session,
            requests,
            tool_calls,
        }
    }

    async fn run(&self, input: Input) {
        let result = self.session.run(input, TurnOptions::default()).await;
        assert!(result.error.is_none(), "turn error: {:?}", result.error);
    }

    fn recorded(&self) -> Vec<ModelRequest> {
        self.requests.lock().unwrap().clone()
    }
}

#[test]
fn queue_readiness_peek_and_projection_keep_the_original_payload() {
    let original = image(2_400, 1_200, false, "queue-original.png");
    let mut mismatched = original.clone();
    mismatched.mime_type = "image/jpeg".into();
    let value = QueuedValue::Blocks(vec![
        ContentBlock::Image(original.clone()),
        nested(&mismatched),
    ]);
    let queue = MessageQueue::new();
    queue
        .enqueue(QueuedMessage {
            trigger_turn: None,
            value: value.clone(),
            mode: "prompt".into(),
            is_meta: None,
            origin: None,
            priority: Some(QueuePriority::Now),
            uuid: None,
            session_id: None,
            agent_id: None,
            correlation_id: None,
            pdf_bytes: 0,
            pdf_documents: 0,
            delivery: Some(SteerDelivery::User),
            display_metadata: None,
        })
        .unwrap();
    let before = queue.queue_snapshot();
    for _ in 0..3 {
        let item = queue
            .peek(Some(&|cmd| queued_value_has_model_content(&cmd.value)))
            .unwrap();
        assert_eq!(item.value, value);
        assert_eq!(
            queued_value_to_content_blocks(&item.value),
            match &value {
                QueuedValue::Blocks(blocks) => blocks.clone(),
                _ => unreachable!(),
            }
        );
        let parts = queued_value_to_tool_result_parts(&item.value);
        assert_eq!(
            parts.extra_user_blocks,
            queued_value_to_content_blocks(&value)
        );
        assert!(parts.output_blocks.is_empty());
    }
    assert_eq!(queue.queue_snapshot(), before);
    assert!(queued_value_has_model_content(&QueuedValue::Blocks(vec![
        nested(&original)
    ])));
    assert_dimensions(&original, 2_400, 1_200);
}

#[tokio::test]
async fn initial_input_uses_model_axes_and_history_can_be_consumed_by_a_wider_model() {
    let original = image(2_400, 1_200, false, "initial-original.png");
    let fixture = Fixture::new("tall", Scenario::Text, original.clone(), Vec::new(), None).await;
    fixture.run(input(&original)).await;
    let first = fixture.recorded();
    assert_eq!(first[0].image_limits, Some(limits(&"tall".into())));
    assert_eq!(first[0].max_media_blocks, NonZeroU32::new(20));
    let admitted = images(&first[0].messages);
    assert_eq!(admitted.len(), 1);
    assert_dimensions(&admitted[0], 128, 64);
    assert!(admitted[0].source_path.is_none());
    let saved = fixture.session.history();
    assert_eq!(images(&saved), vec![original.clone()]);

    fixture.session.set_model("wide").unwrap();
    fixture
        .run(Input::Text(
            "Consume the same history with the new model.".into(),
        ))
        .await;
    let requests = fixture.recorded();
    assert_eq!(requests[1].model, "wide");
    assert_eq!(images(&requests[1].messages), vec![original.clone()]);
    // Usage measured for the previous model cannot anchor a different request projection.
    let mut expected = saved.clone();
    for message in &mut expected {
        if let Message::Assistant(assistant) = message {
            assert!(assistant.usage.is_some());
            assistant.usage = None;
        }
    }
    assert_eq!(
        &fixture.session.history()[..saved.len()],
        expected.as_slice()
    );
    fixture.session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn initial_input_respects_an_independent_encoded_size_ceiling() {
    let original = image(160, 96, true, "encoded-original.png");
    assert!(original.base64.len_utf16() > 16_000);
    let fixture = Fixture::new(
        "encoded",
        Scenario::Text,
        original.clone(),
        Vec::new(),
        None,
    )
    .await;
    fixture.run(input(&original)).await;
    let requests = fixture.recorded();
    let admitted = images(&requests[0].messages);
    assert_eq!(
        admitted.len(),
        1,
        "the fixture must be compressed rather than omitted"
    );
    assert!(admitted[0].base64.len_utf16() <= 16_000);
    assert_ne!(admitted[0].base64, original.base64);
    assert!(admitted[0].source_path.is_none());
    assert_eq!(images(&fixture.session.history()), vec![original]);
    fixture.session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn actual_tool_output_extra_user_and_nested_blocks_share_the_captured_gate() {
    let original = image(512, 256, false, "tool-original.png");
    let extra = vec![ContentBlock::Image(original.clone()), nested(&original)];
    let fixture = Fixture::new(
        "tall",
        Scenario::Tool { switch_to: None },
        original.clone(),
        extra,
        None,
    )
    .await;
    fixture
        .run(Input::Text("Call the image fixture.".into()))
        .await;
    assert_eq!(fixture.tool_calls.load(Ordering::SeqCst), 1);
    let requests = fixture.recorded();
    assert_eq!(requests.len(), 2);
    let admitted = images(&requests[1].messages);
    assert_eq!(admitted.len(), 3);
    for image in admitted {
        assert_dimensions(&image, 128, 64);
        assert!(image.source_path.is_none());
    }
    assert_eq!(
        images(&fixture.session.history()),
        vec![original.clone(); 3]
    );
    fixture.session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn tool_output_and_extra_user_blocks_obey_the_encoded_size_ceiling() {
    let original = image(160, 96, true, "encoded-tool-source.png");
    assert!(original.base64.len_utf16() > 16_000);
    let fixture = Fixture::new(
        "encoded",
        Scenario::Tool { switch_to: None },
        original.clone(),
        vec![ContentBlock::Image(original.clone()), nested(&original)],
        None,
    )
    .await;
    fixture
        .run(Input::Text("Call the encoded image fixture.".into()))
        .await;
    let requests = fixture.recorded();
    assert_eq!(requests.len(), 2);
    let admitted = images(&requests[1].messages);
    assert_eq!(admitted.len(), 3);
    for image in admitted {
        assert!(image.base64.len_utf16() <= 16_000);
        assert_ne!(image.base64, original.base64);
        assert!(image.source_path.is_none());
    }
    assert_eq!(images(&fixture.session.history()), vec![original; 3]);
    fixture.session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn queued_user_inline_and_nested_attachments_use_the_model_at_actual_consumption() {
    let original = image(2_400, 1_200, false, "queued-source.png");
    let fixture = Fixture::new(
        "tall",
        Scenario::Tool {
            switch_to: Some("wide"),
        },
        original.clone(),
        Vec::new(),
        None,
    )
    .await;
    for delivery in [SteerDelivery::User, SteerDelivery::Delegation] {
        fixture
            .session
            .enqueue(
                input(&original),
                EnqueueOptions {
                    priority: Some(QueuePriority::Now),
                    delivery: Some(delivery),
                    ..Default::default()
                },
            )
            .unwrap();
    }
    fixture
        .session
        .enqueue(
            Input::Text("nested placeholder".into()),
            EnqueueOptions {
                priority: Some(QueuePriority::Now),
                delivery: Some(SteerDelivery::Delegation),
                ..Default::default()
            },
        )
        .unwrap();
    let snapshot = fixture.session.queue_snapshot();
    let nested_id = snapshot.items[2].message.uuid.clone().unwrap();
    let edited = fixture
        .session
        .edit_queued_message(
            "install-nested-image".into(),
            nested_id,
            snapshot.revision,
            QueueEdit {
                value: Some(QueuedValue::Blocks(vec![nested(&original)])),
                ..Default::default()
            },
        )
        .await;
    assert_eq!(edited.status, QueueMutationStatus::Applied);
    let queued_before = fixture.session.queued_messages();
    for _ in 0..3 {
        for item in fixture.session.queued_messages() {
            assert!(queued_value_has_model_content(&item.value));
            let content = queued_value_to_content_blocks(&item.value);
            let mut projected = Vec::new();
            content_images(&content, &mut projected);
            assert_eq!(projected, vec![original.clone()]);
        }
    }
    fixture
        .run(Input::Text(
            "Call the image fixture and consume the queue.".into(),
        ))
        .await;
    let requests = fixture.recorded();
    assert_eq!(requests.len(), 2);
    assert!(images(&requests[0].messages).is_empty());
    assert_eq!(requests[1].model, "wide");
    assert_eq!(requests[1].image_limits, Some(limits(&"wide".into())));
    assert_eq!(images(&requests[1].messages), vec![original.clone(); 4]);
    assert!(
        fixture.session.queued_messages().is_empty(),
        "the actual drain must consume all three inputs"
    );
    assert_eq!(
        images(&fixture.session.history()),
        vec![original.clone(); 4]
    );
    for item in queued_before {
        let mut retained = Vec::new();
        content_images(&queued_value_to_content_blocks(&item.value), &mut retained);
        assert_eq!(
            retained,
            vec![original.clone()],
            "drain must not mutate the reserved queue source"
        );
    }
    fixture.session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn tool_images_survive_a_blind_producer_for_a_later_vision_model() {
    let original = image(2_400, 1_200, false, "blind-tool-source.png");
    let fixture = Fixture::new(
        "blind",
        Scenario::Tool {
            switch_to: Some("wide"),
        },
        original.clone(),
        vec![ContentBlock::Image(original.clone())],
        None,
    )
    .await;
    fixture
        .run(Input::Text(
            "Call the image fixture, then switch models.".into(),
        ))
        .await;
    let requests = fixture.recorded();
    assert_eq!(requests[1].model, "wide");
    assert_eq!(images(&requests[1].messages), vec![original.clone(); 2]);
    assert_eq!(images(&fixture.session.history()), vec![original; 2]);
    fixture.session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn a_blind_consumer_omits_nested_tool_images_only_from_its_request_view() {
    let original = image(512, 256, false, "blind-consumer-source.png");
    let fixture = Fixture::new(
        "tall",
        Scenario::Tool {
            switch_to: Some("blind"),
        },
        original.clone(),
        vec![ContentBlock::Image(original.clone()), nested(&original)],
        None,
    )
    .await;
    fixture
        .run(Input::Text(
            "Call the fixture, then select the non-vision model.".into(),
        ))
        .await;
    let requests = fixture.recorded();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[1].model, "blind");
    assert!(
        images(&requests[1].messages).is_empty(),
        "nested tool-result images must not leak to a non-vision request"
    );
    assert_eq!(images(&fixture.session.history()), vec![original; 3]);
    fixture.session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn goal_generated_continuation_gates_expanded_images_without_rewriting_history() {
    let directory = FixtureDirectory::new();
    let path = directory.path.join("model-image-goal.png");
    std::fs::write(&path, b"mention fixture; content comes from the read tool").unwrap();
    let original = image(512, 256, false, "goal-read-source.png");
    let fixture = Fixture::new(
        "tall",
        Scenario::GoalContinuation,
        original.clone(),
        Vec::new(),
        Some(&directory.path),
    )
    .await;
    fixture
        .session
        .set_thread_goal(
            Some(
                format!(
                    "Inspect @{} and complete the requested check.",
                    path.display()
                )
                .into(),
            ),
            None,
            None,
        )
        .await
        .unwrap();
    fixture.run(Input::Text("Begin the goal.".into())).await;
    let continuation = fixture
        .session
        .run_goal_idle_streamed(TurnOptions::default())
        .await
        .unwrap()
        .expect("the active persistent goal must admit one continuation");
    let events: Vec<EngineEvent> = continuation.events.collect().await;
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "the image continuation must finish without a turn failure: {events:?}"
    );
    let requests = fixture.recorded();
    assert_eq!(
        requests.len(),
        2,
        "the goal must generate and consume a second turn"
    );
    assert!(images(&requests[0].messages).is_empty());
    assert_eq!(
        fixture.tool_calls.load(Ordering::SeqCst),
        1,
        "the continuation mention must use the actual read tool"
    );
    let admitted = images(&requests[1].messages);
    assert_eq!(admitted.len(), 1);
    assert_dimensions(&admitted[0], 128, 64);
    assert!(admitted[0].source_path.is_none());
    assert_eq!(images(&fixture.session.history()), vec![original]);
    assert_eq!(
        fixture.session.thread_goal().await.unwrap().unwrap().status,
        ThreadGoalStatus::Complete
    );
    fixture.session.shutdown_and_wait().await.unwrap();
}

#[path = "r20261017_goal_mention_expansion_authority.rs"]
mod goal_mention_expansion_authority;

#[path = "r20261021_nonvisual_image_history.rs"]
mod nonvisual_image_history;
