use super::*;
use klaus_agent_core::testing::ev;

fn stream(events: Vec<ModelStreamEvent>) -> ModelStream {
    Box::pin(futures::stream::iter(events))
}
fn text(value: &str) -> ModelStreamEvent {
    ModelStreamEvent::TextDelta { text: value.into() }
}
fn params() -> wire::EngineCompleteTextParams {
    serde_json::from_value(serde_json::json!({"operationId":"voice-one","source":"qoder","modelId":"dfmodel","system":"Keep the text.","prompt":"First. Final.","maxOutputTokens":24576})).unwrap()
}

#[tokio::test]
async fn complete_text_requires_successful_terminal_reason_without_losing_the_tail() {
    let signal = AbortSignal::new();
    let result = collect(
        stream(vec![
            text("First. "),
            text("Final."),
            ev::stop(Some(ModelStopReason::EndTurn), None),
        ]),
        signal,
    )
    .await
    .unwrap();
    assert_eq!(result, "First. Final.");
    for reason in [
        ModelStopReason::MaxTokens,
        ModelStopReason::Continue,
        ModelStopReason::Refusal,
        ModelStopReason::ToolUse,
    ] {
        assert_eq!(
            collect(
                stream(vec![text("partial"), ev::stop(Some(reason), None)]),
                AbortSignal::new()
            )
            .await
            .unwrap_err(),
            "text_completion_incomplete"
        );
    }
    assert_eq!(
        collect(stream(vec![text("partial")]), AbortSignal::new())
            .await
            .unwrap_err(),
        "text_completion_incomplete"
    );
}

#[tokio::test]
async fn completed_items_replace_their_own_deltas_once() {
    let result = collect(
        stream(vec![
            text("First draft"),
            ModelStreamEvent::TextMessageCompleted {
                id: "a".into(),
                phase: None,
                text: "First corrected.".into(),
            },
            ModelStreamEvent::TextMessage {
                id: "b".into(),
                phase: None,
            },
            text(" Final draft"),
            ModelStreamEvent::TextMessageCompleted {
                id: "b".into(),
                phase: None,
                text: " Final corrected.".into(),
            },
            ev::stop(Some(ModelStopReason::StopSequence), None),
        ]),
        AbortSignal::new(),
    )
    .await
    .unwrap();
    assert_eq!(result, "First corrected. Final corrected.");
}

#[tokio::test]
async fn output_is_bounded_and_empty_or_tool_output_cannot_succeed() {
    assert_eq!(
        collect(
            stream(vec![text(&"x".repeat(MAX_OUTPUT_BYTES + 1))]),
            AbortSignal::new()
        )
        .await
        .unwrap_err(),
        "text_completion_result_too_large"
    );
    assert_eq!(
        collect(
            stream(vec![
                text("  "),
                ev::stop(Some(ModelStopReason::EndTurn), None)
            ]),
            AbortSignal::new()
        )
        .await
        .unwrap_err(),
        "text_completion_empty"
    );
    assert_eq!(
        collect(
            stream(vec![ModelStreamEvent::ToolUseStop { id: "tool".into() }]),
            AbortSignal::new()
        )
        .await
        .unwrap_err(),
        "text_completion_tools_not_allowed"
    );
}

#[tokio::test]
async fn cancellation_drops_a_stalled_stream_without_returning_partial_text() {
    let signal = AbortSignal::new();
    let cancelled = signal.clone();
    let stream =
        Box::pin(futures::stream::iter(vec![text("partial")]).chain(futures::stream::pending()));
    let task = tokio::spawn(collect(stream, signal));
    tokio::task::yield_now().await;
    cancelled.cancel();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err(),
        "provider_operation_cancelled"
    );
}

#[test]
fn input_and_wire_contract_reject_unbounded_or_credential_bearing_requests() {
    assert!(validate(&params()).is_ok());
    for mutate in [0, 1, 2, 3] {
        let mut p = params();
        match mutate {
            0 => p.prompt = "x".repeat(MAX_INPUT_BYTES + 1),
            1 => p.max_output_tokens = 0,
            2 => p.max_output_tokens = MAX_OUTPUT_TOKENS + 1,
            _ => p.model_id = "model\0key".into(),
        }
        assert!(validate(&p).is_err());
    }
    let mut value = serde_json::to_value(params()).unwrap();
    value["provider"] = serde_json::json!({"apiKey":"must-not-accept"});
    assert!(serde_json::from_value::<wire::EngineCompleteTextParams>(value).is_err());
}

#[tokio::test]
async fn replaced_account_interrupts_pending_text_before_publication() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let valid = Arc::new(AtomicBool::new(true));
    let flag = valid.clone();
    let check: PublicationCheck = Arc::new(move || flag.load(Ordering::SeqCst));
    let stream =
        Box::pin(futures::stream::iter(vec![text("partial")]).chain(futures::stream::pending()));
    let task =
        tokio::spawn(
            async move { collect_checked(stream, AbortSignal::new(), Some(&check)).await },
        );
    tokio::task::yield_now().await;
    valid.store(false, Ordering::SeqCst);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err(),
        "provider_credential_changed"
    );
}

#[test]
fn token_rotation_preserves_the_account_identity_but_account_replacement_does_not() {
    let old = (
        "machine".into(),
        serde_json::json!({"uid":"123","access_token":"old"}),
    );
    let rotated = (
        "machine".into(),
        serde_json::json!({"uid":123,"access_token":"new"}),
    );
    let replaced = (
        "machine".into(),
        serde_json::json!({"uid":"456","access_token":"new"}),
    );
    assert_eq!(account_identity(&old), account_identity(&rotated));
    assert_ne!(account_identity(&old), account_identity(&replaced));
}
