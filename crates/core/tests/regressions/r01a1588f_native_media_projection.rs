//! Defect: native image/document history could exceed outgoing media limits.
//! Root cause: aggregate projection existed only after one transport normalized it.
//! Work item: 01a1588f project media under captured request constraints.

use std::borrow::Cow;
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use klaus_agent_core::context::images::IMAGE_MAX_BASE64_CHARS;
use klaus_agent_core::context::media_budget::{
    MediaBudget, MEDIA_BASE64_HEADROOM, MEDIA_COUNT_HEADROOM, MEDIA_REMOVED_MARKER,
    OTHER_MEDIA_BASE64_BYTES,
};
use klaus_agent_core::context::request_media_budget::{native_media_usage, project_native_media};
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::content::{
    ContentBlock, DocumentBlock, ImageBlock, OutputBlock, TextBlock, ToolResultBlock,
};
use klaus_agent_core::schemas::message::{Message, ToolResultMessage, Usage, UserMessage};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::projection::project_for_prompt;
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::tools::build_tool::text_result;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{InputValidationErrorCode, ToolError};
use klaus_agent_core::types::json::{Json, JsonSchema};
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};
use serde_json::json;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

fn text(value: impl Into<RuntimeString>) -> TextBlock {
    TextBlock {
        text: value.into(),
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    }
}

fn image(length: usize) -> ImageBlock {
    ImageBlock {
        mime_type: "image/png".into(),
        base64: "A".repeat(length).into(),
        detail: None,
        source_path: None,
    }
}

fn document(index: usize, length: usize) -> ContentBlock {
    ContentBlock::Document(DocumentBlock {
        mime_type: "application/pdf".into(),
        base64: "A".repeat(length).into(),
        filename: Some(format!("document-{index}.pdf").into()),
    })
}

fn user(index: usize, content: Vec<ContentBlock>) -> Message {
    Message::User(UserMessage {
        uuid: format!("media-user-{index}").into(),
        content,
        is_meta: None,
        question_answer: None,
    })
}

fn budget(max_blocks: usize, max_base64_bytes: usize) -> MediaBudget {
    MediaBudget {
        max_blocks,
        max_base64_bytes,
        count_headroom: MEDIA_COUNT_HEADROOM,
        base64_headroom: MEDIA_BASE64_HEADROOM,
    }
}

#[test]
fn count_boundaries_preserve_newest_media_and_apply_headroom_only_after_excess() {
    for maximum in [100, 600] {
        let source = (0..=maximum)
            .map(|index| user(index, vec![document(index, 4)]))
            .collect::<Vec<_>>();
        let limits = budget(maximum, OTHER_MEDIA_BASE64_BYTES);
        let compliant =
            project_native_media(&source[..maximum], limits, IMAGE_MAX_BASE64_CHARS).unwrap();
        assert!(matches!(compliant, Cow::Borrowed(_)));
        let projected = project_native_media(&source, limits, IMAGE_MAX_BASE64_CHARS).unwrap();
        assert!(matches!(projected, Cow::Owned(_)));
        assert_eq!(native_media_usage(&projected).blocks, maximum - 20);
        assert_eq!(native_media_usage(&source).blocks, maximum + 1);
        assert_eq!(projected.last(), source.last());
        for message in &projected[..21] {
            assert!(matches!(message, Message::User(user)
                if user.content == vec![ContentBlock::Text(text(MEDIA_REMOVED_MARKER))]));
        }
    }
}

#[test]
fn byte_and_count_excesses_are_satisfied_together_without_decoding_payloads() {
    let source = vec![user(
        0,
        [40, 0, 30, 50, 40]
            .into_iter()
            .enumerate()
            .map(|(index, length)| document(index, length))
            .collect(),
    )];
    let projected = project_native_media(
        &source,
        MediaBudget {
            max_blocks: 4,
            max_base64_bytes: 100,
            count_headroom: 2,
            base64_headroom: 10,
        },
        IMAGE_MAX_BASE64_CHARS,
    )
    .unwrap();
    assert_eq!(native_media_usage(&projected).blocks, 2);
    assert_eq!(native_media_usage(&projected).base64_bytes, 90);
    assert_eq!(native_media_usage(&source).base64_bytes, 160);
    let exact = vec![user(0, vec![document(0, 100)])];
    assert!(matches!(
        project_native_media(&exact, budget(100, 100), IMAGE_MAX_BASE64_CHARS).unwrap(),
        Cow::Borrowed(_)
    ));
    let disabled_bytes =
        project_native_media(&source, budget(100, 0), IMAGE_MAX_BASE64_CHARS).unwrap();
    assert!(matches!(disabled_bytes, Cow::Borrowed(_)));
}

#[test]
fn nested_tool_output_is_removed_before_sibling_media_and_extra_user_blocks_count() {
    let source = vec![
        user(
            0,
            vec![
                document(0, 4),
                ContentBlock::ToolResult(ToolResultBlock {
                    tool_use_id: "first-call".into(),
                    content: vec![
                        OutputBlock::Image(image(4)),
                        OutputBlock::Text(text("keep")),
                    ],
                    is_error: None,
                }),
            ],
        ),
        Message::ToolResult(ToolResultMessage {
            uuid: "native-result".into(),
            tool_use_id: "second-call".into(),
            content: vec![OutputBlock::Image(image(4))],
            extra_user_blocks: Some(vec![document(1, 4)]),
            is_error: false,
            tool_kind: None,
            native_tool_output: None,
            output: None,
        }),
    ];
    let projected = project_native_media(
        &source,
        MediaBudget {
            max_blocks: 3,
            count_headroom: 0,
            ..budget(3, 0)
        },
        IMAGE_MAX_BASE64_CHARS,
    )
    .unwrap();
    let Message::User(first) = &projected[0] else {
        panic!("expected user message");
    };
    assert_eq!(first.content[0], document(0, 4));
    let ContentBlock::ToolResult(result) = &first.content[1] else {
        panic!("expected tool result");
    };
    assert_eq!(result.content, vec![OutputBlock::Text(text("keep"))]);
    assert_eq!(projected[1], source[1]);
    assert_eq!(native_media_usage(&projected).blocks, 3);
    assert_eq!(native_media_usage(&source).blocks, 4);
}

#[test]
fn invalid_final_images_are_reported_before_an_aggregate_projection_can_hide_them() {
    let source = vec![user(0, vec![ContentBlock::Image(image(9)), document(0, 4)])];
    let limits = MediaBudget {
        max_blocks: 1,
        count_headroom: 0,
        ..budget(1, 0)
    };
    let error = project_native_media(&source, limits, 8).unwrap_err();
    assert_eq!(error.detail.code, InputValidationErrorCode::RequestTooLarge);
    assert_eq!(error.detail.feature, Some("image_payload".into()));
    assert!(error.message.contains("messages[0].content[0]"));
    assert_eq!(native_media_usage(&source).blocks, 2);
    let exact = vec![user(0, vec![ContentBlock::Image(image(8))])];
    assert!(project_native_media(&exact, budget(1, 0), 8).is_ok());
}

#[test]
fn parallel_tool_outputs_precede_sibling_documents_in_one_user_carrier() {
    let mut source = vec![user(0, vec![document(0, 4)])];
    for index in 1..=2 {
        source.push(Message::ToolResult(ToolResultMessage {
            uuid: format!("parallel-result-{index}").into(),
            tool_use_id: format!("parallel-call-{index}").into(),
            content: vec![OutputBlock::Image(image(4))],
            extra_user_blocks: Some(vec![document(index, 4)]),
            is_error: false,
            tool_kind: None,
            native_tool_output: None,
            output: None,
        }));
    }
    let projected = project_native_media(
        &source,
        MediaBudget {
            max_blocks: 2,
            count_headroom: 0,
            ..budget(2, 0)
        },
        IMAGE_MAX_BASE64_CHARS,
    )
    .unwrap();
    assert_eq!(native_media_usage(&projected).blocks, 2);
    for message in &projected[1..] {
        let Message::ToolResult(result) = message else {
            panic!("expected tool result");
        };
        assert!(result.content.is_empty());
        assert_eq!(result.extra_user_blocks.as_ref().unwrap().len(), 1);
    }
    assert_eq!(native_media_usage(&source).blocks, 5);
}

struct Probe(JsonSchema);

#[async_trait]
impl Tool for Probe {
    fn static_name(&self) -> &str {
        "media_probe"
    }
    fn static_description(&self) -> &str {
        "Complete one bounded fixture step."
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.0
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
        Ok(text_result(Json::Null, "Step complete.", false))
    }
}

struct Provider {
    info: ProviderInfo,
    requests: Mutex<Vec<ModelRequest>>,
}

impl ModelProvider for Provider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }
    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: true,
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: 200_000,
            max_output_tokens: 1024,
            native_pdf: true,
            vision: true,
            image_limits: None,
            max_media_blocks: NonZeroU32::new(42),
        }
    }
    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        let first = {
            let mut requests = self.requests.lock().unwrap();
            let first = requests.is_empty();
            requests.push(request.clone());
            first
        };
        let mut events = vec![ModelStreamEvent::MessageStart {
            model: request.model,
        }];
        if first {
            events.extend([
                ModelStreamEvent::ToolUseStart {
                    id: "media-step".into(),
                    name: "media_probe".into(),
                    thought_signature: None,
                    tool_kind: None,
                    input_format: None,
                },
                ModelStreamEvent::ToolUseInputDelta {
                    id: "media-step".into(),
                    partial_json: "{}".into(),
                },
                ModelStreamEvent::ToolUseStop {
                    id: "media-step".into(),
                },
            ]);
        } else {
            events.push(ModelStreamEvent::TextDelta {
                text: "Finished.".into(),
            });
        }
        events.push(ModelStreamEvent::MessageStop {
            stop_reason: if first {
                ModelStopReason::ToolUse
            } else {
                ModelStopReason::EndTurn
            },
            usage: Usage {
                input_tokens: 5_000,
                output_tokens: 20,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
                total_tokens: None,
            },
        });
        Box::pin(futures::stream::iter(events))
    }
}

#[tokio::test]
async fn captured_media_projection_does_not_rewrite_live_or_durable_history() {
    let directory =
        std::env::temp_dir().join(format!("klaus-native-media-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let provider = Arc::new(Provider {
        info: ProviderInfo {
            name: "native-media-fixture".into(),
            default_model: "media-model".into(),
            base_url: None,
            env_key: None,
        },
        requests: Mutex::new(Vec::new()),
    });
    let mut options =
        agent_options(RuntimeProcessContext::initialize(process_supervisor()).unwrap());
    options.provider = provider.clone();
    options.include_default_agents = Some(false);
    options.tools = Some(vec![Arc::new(Probe(
        json!({"type":"object","properties":{}})
            .as_object()
            .unwrap()
            .clone(),
    ))]);
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(directory.join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let original = (0..43)
        .map(|index| user(index, vec![document(index, 4)]))
        .collect::<Vec<_>>();
    let root = directory.to_string_lossy().into_owned();
    let session = Agent::new(options)
        .open_session(
            store.clone(),
            StartSessionOptions {
                cwd: Some(root.clone().into()),
                user_config_dir: Some(root.into()),
                initial_history: Some(original.clone()),
                system_prompt_provider: Some(Arc::new(|| Box::pin(async { Vec::new() }))),
                config: Some(
                    json!({"model":"media-model", "prewarm":false, "thinking":false,
            "autoMemory":false, "autoDream":false, "agentMemory":false, "memoryRecall":false,
            "speculativeCompaction":false, "earlyMicrocompact":false, "idleMicrocompact":false,
            "compactionSelfCheck":false, "loopDetection":false, "memory":{"enabled":false}}),
                ),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    for _ in 0..2 {
        let result = session
            .run(
                Input::Text("Continue the fixture task.".into()),
                TurnOptions::default(),
            )
            .await;
        assert!(result.error.is_none(), "{:?}", result.error);
    }
    {
        let requests = provider.requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        for request in requests.iter() {
            assert_eq!(request.max_media_blocks.unwrap().get(), 42);
            assert_eq!(native_media_usage(&request.messages).blocks, 22);
            assert!(request.messages.contains(original.last().unwrap()));
        }
    }
    assert_eq!(native_media_usage(&session.history()).blocks, 43);
    for message in &original {
        assert!(session.history().contains(message));
    }
    session.flush_rollout().await.unwrap();
    let persisted = store
        .read_thread(&session.id.try_to_utf8().unwrap())
        .await
        .unwrap();
    let restored = project_for_prompt(&persisted.raw_items);
    assert_eq!(native_media_usage(&restored).blocks, 43);
    for message in &original {
        assert!(restored.contains(message));
    }
    session.shutdown_and_wait().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}
