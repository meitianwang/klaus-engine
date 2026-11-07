//! Defect: a selected checkpoint could rewrite unselected conversation history.
//! Root cause: selection and tool-exchange validation were not kept at the replacement boundary.
//! Work item: 20261020 preserve the unselected side and reject split tool exchanges.

use klaus_agent_core::context::compact::full_compact::PartialCompactionDirection;
use klaus_agent_core::context::compact::{create_full_compactor, FullCompactorOptions};
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeJson, RuntimeObject, RuntimeString};
use klaus_agent_core::schemas::config::default_config;
use klaus_agent_core::schemas::content::{ContentBlock, OutputBlock, TextBlock, ToolUseBlock};
use klaus_agent_core::schemas::message::{
    AssistantMessage, Message, ToolResultMessage, Usage, UserMessage,
};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    CallModel, ModelRequest, ModelStopReason, ModelStreamEvent,
};
use std::sync::{Arc, Mutex};

fn text(value: &str) -> TextBlock {
    TextBlock {
        text: value.into(),
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    }
}
fn user(id: &str, value: &str) -> Message {
    Message::User(UserMessage {
        uuid: id.into(),
        content: vec![ContentBlock::Text(text(value))],
        is_meta: None,
        question_answer: None,
    })
}
fn assistant(id: &str, value: &str) -> Message {
    Message::Assistant(AssistantMessage {
        uuid: id.into(),
        model: "fixture".into(),
        content: vec![ContentBlock::Text(text(value))],
        usage: None,
    })
}
fn history() -> Vec<Message> {
    vec![
        user("goal", "Keep the initial task."),
        assistant("old", "Completed the initial step."),
        user("selected", "Continue with the pending step."),
        assistant("new", "Pending step evidence."),
    ]
}
fn success() -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::TextDelta {
            text: "Selected segment checkpoint.".into(),
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
    ]
}
fn options(call_model: CallModel) -> FullCompactorOptions {
    FullCompactorOptions {
        call_model,
        model: "fixture".into(),
        max_output_tokens: Some(8_192),
        uuid: Box::new(|| uuid::Uuid::new_v4().to_string()),
    }
}
fn recorded(requests: &Arc<Mutex<Vec<ModelRequest>>>) -> CallModel {
    let requests = requests.clone();
    Box::new(move |request, _| {
        requests.lock().unwrap().push(request);
        Box::pin(futures::stream::iter(success()))
    })
}
fn summary_is(message: &Message) {
    assert!(
        matches!(message,Message::CompactBoundary(value) if value.summary=="Selected segment checkpoint.")
    );
}

#[tokio::test]
async fn selected_sides_preserve_the_other_side_and_place_the_checkpoint_at_its_boundary() {
    for direction in [
        PartialCompactionDirection::From,
        PartialCompactionDirection::UpTo,
    ] {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let compactor = create_full_compactor(options(recorded(&requests)));
        let original = history();
        let custom = RuntimeString::from("Preserve the pending decision.");
        let result = compactor
            .partial_compact(
                original.clone(),
                2,
                direction,
                &default_config(),
                Some(&custom),
            )
            .await;
        assert!(result.compacted);
        assert_eq!(result.messages.len(), 4);
        let seen = requests.lock().unwrap();
        assert_eq!(seen.len(), 1);
        let sent = &seen[0].messages;
        assert!(matches!(sent.last(),Some(Message::User(value)) if value.is_meta==Some(true)));
        match direction {
            PartialCompactionDirection::From => {
                assert_eq!(&result.messages[..2], &original[..2]);
                assert_eq!(result.messages[2], original[2]);
                summary_is(&result.messages[3]);
                assert_eq!(&sent[..sent.len() - 1], &original[2..]);
            }
            PartialCompactionDirection::UpTo => {
                assert_eq!(result.messages[0], original[0]);
                summary_is(&result.messages[1]);
                assert_eq!(&result.messages[2..], &original[2..]);
                assert_eq!(&sent[..sent.len() - 1], &original[..2]);
            }
        }
    }
}

#[tokio::test]
async fn empty_invalid_or_split_exchange_selections_keep_history_without_a_request() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let compactor = create_full_compactor(options(recorded(&requests)));
    let original = history();
    for (index, direction) in [
        (0, PartialCompactionDirection::UpTo),
        (original.len(), PartialCompactionDirection::From),
        (usize::MAX, PartialCompactionDirection::UpTo),
    ] {
        let result = compactor
            .partial_compact(original.clone(), index, direction, &default_config(), None)
            .await;
        assert!(!result.compacted);
        assert_eq!(result.messages, original);
    }
    let paired = vec![
        user("task", "Keep the task."),
        Message::Assistant(AssistantMessage {
            uuid: "call".into(),
            model: "fixture".into(),
            usage: None,
            content: vec![ContentBlock::ToolUse(ToolUseBlock {
                id: "read".into(),
                name: "read".into(),
                input: RuntimeJson::Object(RuntimeObject::new()),
                tool_kind: None,
                thought_signature: None,
            })],
        }),
        Message::ToolResult(ToolResultMessage {
            uuid: "result".into(),
            tool_use_id: "read".into(),
            content: vec![OutputBlock::Text(text("Read output."))],
            is_error: false,
            tool_kind: None,
            native_tool_output: None,
            output: None,
            extra_user_blocks: None,
        }),
    ];
    for direction in [
        PartialCompactionDirection::From,
        PartialCompactionDirection::UpTo,
    ] {
        let result = compactor
            .partial_compact(paired.clone(), 2, direction, &default_config(), None)
            .await;
        assert!(!result.compacted);
        assert_eq!(result.messages, paired);
    }
    assert!(requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cancellation_during_selected_summary_keeps_the_entire_original_history() {
    let signal = AbortSignal::new();
    let cancel = signal.clone();
    let compactor = create_full_compactor(options(Box::new(move |_, _| {
        cancel.cancel();
        Box::pin(futures::stream::iter(success()))
    })));
    let request: ModelRequest = runtime_codec::decode_text(&RuntimeString::from(
        r#"{"model":"fixture","system":[],"messages":[],"tools":[],"maxOutputTokens":8192}"#,
    ))
    .unwrap();
    compactor.prepare_request(&request, &signal);
    let original = history();
    let result = compactor
        .partial_compact(
            original.clone(),
            2,
            PartialCompactionDirection::From,
            &default_config(),
            None,
        )
        .await;
    assert!(!result.compacted);
    assert_eq!(result.messages, original);
}
