//! Defect: a usable summary was discarded when the provider reached its output limit.
//! Root cause: every stop reason other than end_turn took the same failed-retry path.
//! Work item 20261105: retain partial summary text and report the degradation.

use klaus_agent_core::context::compact::full_compact::{
    create_full_compactor, FullCompactorOptions,
};
use klaus_agent_core::context::compact::outcome::CompactionDiagnosticKind;
use klaus_agent_core::schemas::config::default_config;
use klaus_agent_core::schemas::content::{ContentBlock, TextBlock};
use klaus_agent_core::schemas::message::{Message, Usage, UserMessage};
use klaus_agent_core::types::provider::{ModelStopReason, ModelStreamEvent};

#[tokio::test]
async fn partial_summary_survives_the_provider_output_limit() {
    let compactor = create_full_compactor(FullCompactorOptions {
        call_model: Box::new(|_, _| {
            Box::pin(futures::stream::iter(vec![
                ModelStreamEvent::TextDelta {
                    text: "## Progress\nThe asset form was submitted; deregistration remains."
                        .into(),
                },
                ModelStreamEvent::MessageStop {
                    stop_reason: ModelStopReason::MaxTokens,
                    usage: Usage {
                        input_tokens: 109_779,
                        output_tokens: 48_000,
                        cache_read_input_tokens: 0,
                        cache_creation_input_tokens: 0,
                        total_tokens: None,
                    },
                },
            ]))
        }),
        model: "efficient".into(),
        max_output_tokens: Some(48_000),
        uuid: Box::new(|| "partial-summary".into()),
    });
    let original = vec![Message::User(UserMessage {
        uuid: "request".into(),
        content: vec![ContentBlock::Text(TextBlock {
            text: "Deregister the retired asset.".into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })],
        is_meta: None,
        question_answer: None,
    })];
    let mut config = default_config();
    config.compaction_stream_max_retries = 0;

    let result = compactor.reactive_compact(original, &config, None).await;

    assert!(result.compacted, "{result:?}");
    assert!(matches!(
        result.messages.last(),
        Some(Message::CompactBoundary(boundary))
            if boundary.summary.try_to_utf8().unwrap().contains("deregistration remains")
    ));
    assert_eq!(result.diagnostics.len(), 1);
    assert_eq!(
        result.diagnostics[0].kind,
        CompactionDiagnosticKind::OutputTruncated
    );
}

#[tokio::test]
async fn output_limit_without_summary_text_remains_a_failure() {
    let compactor = create_full_compactor(FullCompactorOptions {
        call_model: Box::new(|_, _| {
            Box::pin(futures::stream::iter(vec![ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::MaxTokens,
                usage: Usage {
                    input_tokens: 109_779,
                    output_tokens: 48_000,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    total_tokens: None,
                },
            }]))
        }),
        model: "efficient".into(),
        max_output_tokens: Some(48_000),
        uuid: Box::new(|| "empty-summary".into()),
    });
    let original = vec![Message::User(UserMessage {
        uuid: "request".into(),
        content: vec![ContentBlock::Text(TextBlock {
            text: "Deregister the retired asset.".into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })],
        is_meta: None,
        question_answer: None,
    })];
    let mut config = default_config();
    config.compaction_stream_max_retries = 0;

    let result = compactor
        .reactive_compact(original.clone(), &config, None)
        .await;

    assert!(!result.compacted);
    assert_eq!(result.messages, original);
    assert_eq!(result.diagnostics.len(), 1);
    assert_eq!(
        result.diagnostics[0].kind,
        CompactionDiagnosticKind::OutputTruncated
    );
    assert_eq!(
        result.diagnostics[0]
            .message
            .as_ref()
            .unwrap()
            .try_to_utf8()
            .unwrap(),
        "The summary response reached the output token limit without producing usable text."
    );
}
