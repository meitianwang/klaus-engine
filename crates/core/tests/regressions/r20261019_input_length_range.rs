//! Defect: bounded input-length rejections bypassed context-overflow recovery.
//! Root cause: the provider classifier missed the allowed input range wording.
//! Work item 20261019: recognize input ranges without reclassifying other parameters.

use super::r20260906_provider_context_errors::{serve_response, summary_request};
use futures::StreamExt;
use klaus_agent_core::provider::anthropic::{AnthropicProvider, AnthropicProviderOptions};
use klaus_agent_core::provider::classify::{classify_error, RawProviderError};
use klaus_agent_core::provider::openai::{OpenAIProvider, OpenAIProviderOptions};
use klaus_agent_core::provider::retry::RetryOptions;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::ProviderErrorKind;
use klaus_agent_core::types::provider::{ModelProvider, ModelStreamEvent};
use std::time::Duration;

async fn assert_rejection(message: &str, expected: ProviderErrorKind) {
    for anthropic in [false, true] {
        for streaming in [false, true] {
            let envelope = serde_json::json!({
                "type": "error",
                "error": {
                    "code": "invalid_parameter_error",
                    "type": "invalid_request_error",
                    "message": message,
                    "request_id": "req_input_range",
                },
            });
            let (status, content_type, body) = if streaming {
                (
                    "200 OK",
                    "text/event-stream",
                    format!("event: error\ndata: {envelope}\n\n"),
                )
            } else {
                ("400 Bad Request", "application/json", envelope.to_string())
            };
            let (base_url, server) = serve_response(status, content_type, body).await;
            let retry = Some(RetryOptions {
                max_attempts: Some(1),
                congestion_hold_ms: Some(0),
                ..Default::default()
            });
            let provider: Box<dyn ModelProvider> = if anthropic {
                Box::new(AnthropicProvider::new(AnthropicProviderOptions {
                    api_key: Some("fixture-only".into()),
                    base_url: Some(base_url),
                    retry,
                    ..Default::default()
                }))
            } else {
                Box::new(OpenAIProvider::new(OpenAIProviderOptions {
                    api_key: Some("fixture-only".into()),
                    base_url: Some(base_url),
                    retry,
                    ..Default::default()
                }))
            };
            let error = tokio::time::timeout(Duration::from_secs(5), async {
                let mut stream = provider.stream(summary_request(), AbortSignal::new());
                while let Some(event) = stream.next().await {
                    if let ModelStreamEvent::Error { error } = event {
                        return error;
                    }
                }
                panic!("the rejected request must emit a provider error");
            })
            .await
            .expect("parameter rejections must settle promptly");
            server.await.unwrap();
            assert_eq!(
                error.kind, expected,
                "anthropic={anthropic}, streaming={streaming}, message={message}"
            );
            assert_eq!(error.message.try_to_utf8().unwrap(), message);
            assert_eq!(error.status, if streaming { None } else { Some(400) });
            assert_eq!(
                error.code.as_ref().unwrap().try_to_utf8().unwrap(),
                "invalid_parameter_error"
            );
            assert_eq!(
                error.error_type.as_ref().unwrap().try_to_utf8().unwrap(),
                "invalid_request_error"
            );
            assert_eq!(
                error.request_id.as_ref().unwrap().try_to_utf8().unwrap(),
                "req_input_range"
            );
        }
    }
}

#[tokio::test]
async fn bounded_input_ranges_enable_context_recovery() {
    for message in [
        "<400> InternalError.Algo.InvalidParameter: Range of input length should be [1, 983616]",
        "Range of input length should be [1,32768] (request-id: req_input_range)",
        "RANGE OF INPUT LENGTH SHOULD BE [ 1 , 120000 ]",
    ] {
        assert_rejection(message, ProviderErrorKind::PromptTooLong).await;
    }
}

#[tokio::test]
async fn unrelated_parameter_rejections_remain_invalid_requests() {
    for message in [
        "max_completion_tokens must be greater than thinking_budget",
        "Range of output length should be [1, 16384]",
        "Range of input length should be [1, unlimited]",
        "Range of input length should be [1, 0]",
        "Range of input length should be [8, 8192]",
        "input length must be greater than zero",
        "developer is not one of ['system', 'assistant', 'user', 'tool', 'function']",
    ] {
        assert_rejection(message, ProviderErrorKind::InvalidRequest).await;
    }
}

#[test]
fn bounded_input_wording_preserves_other_failure_statuses() {
    for (status, expected) in [
        (401, ProviderErrorKind::Auth),
        (403, ProviderErrorKind::Auth),
        (429, ProviderErrorKind::RateLimit),
        (500, ProviderErrorKind::Overloaded),
    ] {
        let error = classify_error(&RawProviderError {
            status: Some(status),
            code: Some("invalid_parameter_error".into()),
            body_type: Some("invalid_request_error".into()),
            message: "Range of input length should be [1, 983616]".into(),
            ..Default::default()
        });
        assert_eq!(error.kind, expected, "status={status}");
    }
}
