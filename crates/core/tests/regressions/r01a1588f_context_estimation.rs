//! Defect: unanchored context estimates depended on media encoding and rounding.
//! Root cause: request admission used a different structural estimator.
//! Work item: 01a1588f stabilize context estimates before authoritative usage.

use klaus_agent_core::context::tokens::{
    estimate_context_tokens_with_prefix, estimate_message_tokens,
};
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::content::{ContentBlock, DocumentBlock, ImageBlock, TextBlock};
use klaus_agent_core::schemas::message::{AssistantMessage, Message, Usage, UserMessage};

fn text(value: impl Into<RuntimeString>) -> ContentBlock {
    ContentBlock::Text(TextBlock {
        text: value.into(),
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    })
}

fn user(content: Vec<ContentBlock>) -> Message {
    Message::User(UserMessage {
        uuid: "estimation-user".into(),
        content,
        is_meta: None,
        question_answer: None,
    })
}

#[test]
fn unanchored_text_rounds_utf8_bytes_and_native_media_has_a_fixed_cost() {
    for (value, expected) in [
        ("", 0),
        ("a", 1),
        ("ab", 1),
        ("abcd", 1),
        ("abcde", 2),
        ("中文", 2),
        ("😀😀😀", 3),
    ] {
        assert_eq!(estimate_message_tokens(&user(vec![text(value)])), expected);
    }
    for size in [4, 100_000] {
        let message = user(vec![
            ContentBlock::Image(ImageBlock {
                mime_type: "image/png".into(),
                base64: "A".repeat(size).into(),
                detail: None,
                source_path: None,
            }),
            ContentBlock::Document(DocumentBlock {
                mime_type: "application/pdf".into(),
                base64: "A".repeat(size).into(),
                filename: None,
            }),
        ]);
        assert_eq!(estimate_message_tokens(&message), 4_000);
    }
}

#[test]
fn unanchored_text_estimates_lone_surrogates_without_changing_runtime_identity() {
    let value = RuntimeString::from_utf16(vec![0xd800; 5]);

    assert_eq!(estimate_message_tokens(&user(vec![text(value.clone())])), 4);
    assert_eq!(value.as_utf16(), &[0xd800; 5]);
    assert!(value.try_to_utf8().is_err());
}

#[test]
fn authoritative_usage_covers_the_prefix_and_only_new_context_is_estimated() {
    let mut history = vec![user(vec![text("old".repeat(20_000))])];
    history.push(Message::Assistant(AssistantMessage {
        uuid: "estimation-assistant".into(),
        content: vec![text("answer")],
        model: "fixture".into(),
        usage: Some(Usage {
            input_tokens: 12_000,
            output_tokens: 100,
            cache_read_input_tokens: 8_000,
            cache_creation_input_tokens: 2_000,
            total_tokens: None,
        }),
    }));
    history.push(user(vec![text("x".repeat(400))]));
    assert_eq!(
        estimate_context_tokens_with_prefix(&history, 50_000),
        22_200
    );
    if let Message::Assistant(assistant) = &mut history[1] {
        assistant.usage.as_mut().unwrap().total_tokens = Some(15_000);
    }
    assert_eq!(
        estimate_context_tokens_with_prefix(&history, 50_000),
        15_100
    );
}
