//! Defect: host rendering records could reach normalized model requests.
//! Root cause: request projections copied durable history and encoded display records into summary text.
//! Work item 20261017: private request views preserve full stored conversation snapshots.

use klaus_agent_core::context::compact::{create_full_compactor, FullCompactorOptions};
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json};
use klaus_agent_core::schemas::config::default_config;
use klaus_agent_core::schemas::content::{ContentBlock, TextBlock};
use klaus_agent_core::schemas::message::{
    AssistantMessage, Message, SystemMessage, SystemSubtype, Usage, UserMessage,
};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    CallModel, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent, SystemPromptPart,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const DISPLAY: &str = "display-request-private-sentinel";
const WARNING: &str = "Retain this ordinary system warning.";

fn user(id: &str, text: &str) -> Message {
    Message::User(UserMessage {
        question_answer: None,
        uuid: id.into(),
        content: vec![ContentBlock::Text(TextBlock {
            text: text.into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })],
        is_meta: None,
    })
}

fn history(body: &str) -> Vec<Message> {
    vec![
        user("normal-user", body),
        Message::System(SystemMessage {
            uuid: "display-record".into(),
            text: format!(
                r#"{{"messageUuid":"normal-user","localRender":{{"label":"{DISPLAY}"}}}}"#
            )
            .into(),
            subtype: SystemSubtype::UserDisplay,
        }),
        Message::System(SystemMessage {
            uuid: "ordinary-warning".into(),
            text: WARNING.into(),
            subtype: SystemSubtype::Warning,
        }),
        Message::Assistant(AssistantMessage {
            uuid: "normal-assistant".into(),
            content: vec![ContentBlock::Text(TextBlock {
                text: "The prior answer remains ordinary conversation.".into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            model: "main".into(),
            usage: None,
        }),
        user("latest-user", "Continue the actual task."),
    ]
}

fn model(captured: Arc<Mutex<Vec<ModelRequest>>>, summary: &'static str) -> CallModel {
    Box::new(
        move |request: ModelRequest, _: AbortSignal| -> ModelStream {
            let mut captured = captured.lock().unwrap();
            assert!(captured.len() < 64, "bounded summary transport fixture");
            captured.push(request);
            Box::pin(futures::stream::iter(vec![
                ModelStreamEvent::TextDelta {
                    text: summary.into(),
                },
                ModelStreamEvent::MessageStop {
                    stop_reason: ModelStopReason::EndTurn,
                    usage: Usage {
                        input_tokens: 1,
                        output_tokens: 1,
                        cache_read_input_tokens: 0,
                        cache_creation_input_tokens: 0,
                        total_tokens: None,
                    },
                },
            ]))
        },
    )
}

fn options(call_model: CallModel) -> FullCompactorOptions {
    let next = AtomicUsize::new(0);
    FullCompactorOptions {
        call_model,
        model: "summary".into(),
        max_output_tokens: Some(128),
        uuid: Box::new(move || format!("summary-{}", next.fetch_add(1, Ordering::SeqCst))),
    }
}

fn cached_request() -> ModelRequest {
    ModelRequest {
        supports_image_detail_original: false,
        model_verbosity: None,
        reasoning_summary: Default::default(),
        model: "captured-main-model".into(),
        image_limits: None,
        max_media_blocks: None,
        system: vec![SystemPromptPart {
            name: "captured-main-prefix".into(),
            text: "Keep this actual cached request prefix.".into(),
            cacheable: Some(true),
            cache_scope: None,
        }],
        messages: Vec::new(),
        tools: Vec::new(),
        deferred_tools: Vec::new(),
        tool_choice: None,
        max_output_tokens: 128,
        task_budget: Some(37),
        output_schema: None,
        web_search: None,
        thinking: None,
        effort: None,
        skip_cache_write: None,
        cache_ttl: None,
        cache_key: None,
        context_management: None,
        context_hints: None,
        thinking_context: None,
    }
}

fn assert_private_display_absent(request: &ModelRequest) {
    let encoded = runtime_json::stringify(&runtime_codec::encode_value(request).unwrap());
    assert!(
        !encoded.contains(DISPLAY),
        "private rendering metadata must not reach any normalized summary request"
    );
}

async fn assert_direct_or_cached_summary_projection(cached: bool) {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let compactor = create_full_compactor(options(model(captured.clone(), "A concise summary.")));
    if cached {
        compactor.prepare_request(&cached_request(), &AbortSignal::new());
    }
    let original = history("Keep the normal human request.");
    let snapshot = original.clone();
    let result = compactor
        .full_compact(original.clone(), &default_config(), None, false)
        .await;
    assert!(result.compacted);
    assert_eq!(
        original, snapshot,
        "provider projection must not mutate durable source records"
    );
    assert!(matches!(
        result.messages.last(),
        Some(Message::CompactBoundary(_))
    ));
    let seen = captured.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_private_display_absent(&seen[0]);
    for message in original.iter().filter(|message| !matches!(message, Message::System(system) if system.subtype == SystemSubtype::UserDisplay)) {
        assert!(seen[0].messages.contains(message), "ordinary conversation and typed warnings remain visible to the summary");
    }
    if cached {
        assert_eq!(seen[0].system[0].name, "captured-main-prefix");
    }
}

#[tokio::test]
async fn direct_summary_keeps_display_records_out_of_provider_input() {
    assert_direct_or_cached_summary_projection(false).await;
}

#[tokio::test]
async fn captured_summary_keeps_display_records_out_of_provider_input() {
    assert_direct_or_cached_summary_projection(true).await;
}

#[tokio::test(start_paused = true)]
async fn failed_summary_returns_the_complete_original_display_snapshot() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let calls = captured.clone();
    let compactor = create_full_compactor(options(Box::new(move |request, _| {
        calls.lock().unwrap().push(request);
        Box::pin(futures::stream::empty())
    })));
    let original = history("Preserve this request if no summary is available.");
    let mut config = default_config();
    config.compaction_stream_max_retries = 0;
    let result = compactor
        .full_compact(original.clone(), &config, None, false)
        .await;
    assert!(!result.compacted);
    assert_eq!(result.messages, original);
    let seen = captured.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_private_display_absent(&seen[0]);
}
