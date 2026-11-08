//! Defect: a usable summary was discarded when the summary response stopped
//! with a tool call, even though the summary request offers no tools.
//! Root cause: every stop reason other than end_turn (and the tolerated
//! output-limit case) took the same failed-retry path.
//! Work item 20261108: retain the streamed summary text and ignore the
//! tool-call stop; keep failing only when no usable text arrived.

use klaus_agent_core::context::compact::full_compact::{
    create_full_compactor, FullCompactorOptions,
};
use klaus_agent_core::context::compact::outcome::CompactionDiagnosticKind;
use klaus_agent_core::schemas::config::default_config;
use klaus_agent_core::schemas::content::{ContentBlock, TextBlock};
use klaus_agent_core::schemas::message::{Message, Usage, UserMessage};
use klaus_agent_core::types::provider::{ModelStopReason, ModelStreamEvent};

fn user_message() -> Message {
    Message::User(UserMessage {
        uuid: "request".into(),
        content: vec![ContentBlock::Text(TextBlock {
            text: "Summarize the retained work.".into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })],
        is_meta: None,
        question_answer: None,
    })
}

fn stop_usage() -> Usage {
    Usage {
        input_tokens: 488_448,
        output_tokens: 10_584,
        cache_read_input_tokens: 0,
        cache_creation_input_tokens: 0,
        total_tokens: None,
    }
}

#[tokio::test]
async fn summary_survives_a_tool_call_stop() {
    let compactor = create_full_compactor(FullCompactorOptions {
        call_model: Box::new(|_, _| {
            Box::pin(futures::stream::iter(vec![
                ModelStreamEvent::TextDelta {
                    text: "## Progress\nThe migration landed; rollback notes remain.".into(),
                },
                ModelStreamEvent::MessageStop {
                    stop_reason: ModelStopReason::ToolUse,
                    usage: stop_usage(),
                },
            ]))
        }),
        model: "efficient".into(),
        max_output_tokens: Some(16_000),
        uuid: Box::new(|| "tooluse-summary".into()),
    });
    let mut config = default_config();
    config.compaction_stream_max_retries = 0;

    let result = compactor
        .reactive_compact(vec![user_message()], &config, None)
        .await;

    assert!(result.compacted, "{result:?}");
    assert!(
        matches!(
            result.messages.last(),
            Some(Message::CompactBoundary(boundary))
                if boundary.summary.try_to_utf8().unwrap().contains("rollback notes remain")
        ),
        "{:?}",
        result.messages.last()
    );
    assert!(
        result
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.kind == CompactionDiagnosticKind::ProviderError),
        "{result:?}"
    );
}

#[tokio::test]
async fn tool_call_stop_without_text_still_fails() {
    let compactor = create_full_compactor(FullCompactorOptions {
        call_model: Box::new(|_, _| {
            Box::pin(futures::stream::iter(vec![ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::ToolUse,
                usage: stop_usage(),
            }]))
        }),
        model: "efficient".into(),
        max_output_tokens: Some(16_000),
        uuid: Box::new(|| "tooluse-empty".into()),
    });
    let mut config = default_config();
    config.compaction_stream_max_retries = 0;

    let result = compactor
        .reactive_compact(vec![user_message()], &config, None)
        .await;

    assert!(!result.compacted, "{result:?}");
    assert!(
        result.diagnostics.iter().any(|diagnostic| diagnostic
            .message
            .as_ref()
            .is_some_and(|message| message.try_to_utf8().unwrap().contains("tool call"))),
        "{result:?}"
    );
}
