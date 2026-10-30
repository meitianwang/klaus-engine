//! Defect: advertised native audio/video inputs had no typed engine path.
//! Root cause: user content admitted only images and PDF documents.
//! Work item: 20261019 native cloud media input.

use std::sync::{Arc, Mutex};

use futures::StreamExt;

use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::message_queue::{
    queued_value_has_model_content, queued_value_to_tool_result_parts, QueuedValue,
};
use klaus_agent_core::engine::session::{
    EnqueueOptions, Input, Session, TurnOptions, UserContentPart,
};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::content::{ContentBlock, MediaBlock};
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::InputValidationErrorCode;
use klaus_agent_core::types::provider::{
    ModelCapabilities, ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use serde_json::json;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

struct MediaProvider {
    info: ProviderInfo,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
}

impl ModelProvider for MediaProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn model_metadata(&self, model: &RuntimeString) -> ModelCapabilities {
        ModelCapabilities {
            media_input_mime_types: if model == "multimodal" {
                vec![
                    "audio/wav".into(),
                    "audio/webm;codecs=opus".into(),
                    "video/mp4".into(),
                ]
            } else {
                Vec::new()
            },
            ..Default::default()
        }
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

    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        self.requests.lock().unwrap().push(request.clone());
        Box::pin(futures::stream::iter(vec![
            ModelStreamEvent::MessageStart {
                model: request.model,
            },
            ModelStreamEvent::TextDelta {
                text: "Media received.".into(),
            },
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: Usage {
                    input_tokens: 12,
                    output_tokens: 3,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    total_tokens: None,
                },
            },
        ]))
    }
}

pub(super) fn fixture() -> (Arc<Session>, Arc<Mutex<Vec<ModelRequest>>>) {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let mut options = agent_options(process);
    options.provider = Arc::new(MediaProvider {
        info: ProviderInfo {
            name: "native-media-fixture".into(),
            default_model: "multimodal".into(),
            base_url: None,
            env_key: None,
        },
        requests: requests.clone(),
    });
    options.include_default_agents = Some(false);
    let session = Agent::new(options).start_session(StartSessionOptions {
        config: Some(json!({
            "model": "multimodal", "prewarm": false, "thinking": false,
            "autoMemory": false, "autoDream": false, "memoryRecall": false,
            "speculativeCompaction": false, "earlyMicrocompact": false,
            "idleMicrocompact": false, "compactionSelfCheck": false,
            "loopDetection": false,
        })),
        ..Default::default()
    });
    (session, requests)
}

pub(super) fn part(mime: &str, data: &str, filename: Option<&str>) -> UserContentPart {
    UserContentPart::Media {
        mime_type: mime.into(),
        base64: data.into(),
        filename: filename.map(Into::into),
    }
}

#[tokio::test]
async fn public_session_preserves_audio_and_video_without_pdf_or_vision_support() {
    let (session, requests) = fixture();
    let result = session
        .run(
            Input::Parts(vec![
                part("AUDIO/WAV", "UklGRg==", Some("folder/voice.wav")),
                part("video/mp4", "AAAAHGZ0eXA=", Some("clip.mp4")),
            ]),
            TurnOptions::default(),
        )
        .await;
    assert!(result.error.is_none(), "{:?}", result.error);
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let Message::User(user) = requests[0].messages.last().unwrap() else {
        panic!("user input missing");
    };
    assert!(matches!(&user.content[0], ContentBlock::Media(media)
        if media.mime_type == "audio/wav" && media.base64 == "UklGRg=="
        && media.filename.as_ref().is_some_and(|name| name == "voice.wav")));
    assert!(matches!(&user.content[1], ContentBlock::Media(media)
        if media.mime_type == "video/mp4" && media.base64 == "AAAAHGZ0eXA="));
}

#[tokio::test]
async fn public_admission_rejects_unsupported_mime_model_and_invalid_payload_before_streaming() {
    let (session, requests) = fixture();
    for input in [
        part("audio/mpeg", "UklGRg==", None),
        part("application/pdf", "JVBERi0=", None),
        part("audio/wav", "%%%", None),
        part("audio/wav", "", None),
        part("audio/wav", "UklGRg==", Some("..")),
    ] {
        let error = session
            .validate_input_for_turn(Input::Parts(vec![input]), &TurnOptions::default())
            .unwrap_err();
        assert_eq!(
            error.detail.code,
            InputValidationErrorCode::UnsupportedInput
        );
    }
    let error = session
        .validate_input_for_turn(
            Input::Parts(vec![part("audio/wav", "UklGRg==", None)]),
            &TurnOptions {
                model: Some("text-only".into()),
                ..Default::default()
            },
        )
        .unwrap_err();
    assert_eq!(
        error.detail.code,
        InputValidationErrorCode::UnsupportedInput
    );
    assert!(requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn public_admission_bounds_native_media_before_decoding() {
    let (session, _) = fixture();
    let oversized =
        "A".repeat(klaus_agent_core::provider::native_media::MAX_MEDIA_INPUT_BASE64_BYTES + 4);
    let error = session
        .validate_input_for_turn(
            Input::Parts(vec![part("audio/wav", &oversized, None)]),
            &TurnOptions::default(),
        )
        .unwrap_err();
    assert_eq!(error.detail.code, InputValidationErrorCode::RequestTooLarge);
    let error = session
        .validate_input_for_turn(
            Input::Parts(vec![part("audio/wav", "UklGRg==", None); 21]),
            &TurnOptions::default(),
        )
        .unwrap_err();
    assert_eq!(error.detail.code, InputValidationErrorCode::RequestTooLarge);
}

#[test]
fn queue_tool_result_projection_retains_native_media_as_user_content() {
    let media = ContentBlock::Media(MediaBlock {
        mime_type: "audio/webm;codecs=opus".into(),
        base64: "UklGRg==".into(),
        filename: None,
    });
    let queued = QueuedValue::Blocks(vec![media.clone()]);
    assert!(queued_value_has_model_content(&queued));
    let parts = queued_value_to_tool_result_parts(&queued);
    assert!(parts.output_blocks.is_empty());
    assert_eq!(parts.extra_user_blocks, vec![media]);
}

#[tokio::test]
async fn queued_media_revalidates_the_selected_model_and_keeps_refused_input_pending() {
    let (session, requests) = fixture();
    session
        .enqueue(
            Input::Parts(vec![part("audio/wav", "UklGRg==", None)]),
            EnqueueOptions::default(),
        )
        .unwrap();
    let before = session.queued_messages();
    assert!(session
        .resume_pending_streamed(TurnOptions {
            model: Some("text-only".into()),
            ..Default::default()
        })
        .await
        .is_err());
    assert_eq!(session.queued_messages(), before);
    assert!(requests.lock().unwrap().is_empty());
    let turn = session
        .resume_pending_streamed(TurnOptions::default())
        .await
        .unwrap()
        .unwrap();
    let _: Vec<_> = turn.events.collect().await;
    assert!(session.queue_snapshot().items.is_empty());
    assert!(requests.lock().unwrap().iter().any(|request|
        request.messages.iter().any(|message| matches!(message,
            Message::User(user) if user.content.iter().any(|block| matches!(block, ContentBlock::Media(_)))
        ))
    ));
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn pending_batch_media_limit_is_checked_without_losing_queued_inputs() {
    let (session, requests) = fixture();
    for _ in 0..21 {
        session
            .enqueue(
                Input::Parts(vec![part("audio/wav", "UklGRg==", None)]),
                EnqueueOptions::default(),
            )
            .unwrap();
    }
    let before = session.queued_messages();
    assert!(session
        .resume_pending_streamed(TurnOptions::default())
        .await
        .is_err());
    assert_eq!(session.queued_messages(), before);
    assert!(requests.lock().unwrap().is_empty());
    session.shutdown_and_wait().await.unwrap();
}
