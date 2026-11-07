//! Stream reconnection retries with provider timing and parent cancellation.

use super::full_compact::CompactionResult;
use super::outcome::{
    CompactionDiagnostic, CompactionDiagnosticKind, CompactionDiagnosticStage, CompactionNoopReason,
};
use crate::schemas::message::Message;
use crate::types::abort::AbortSignal;
use crate::types::errors::{ProviderError, ProviderErrorKind};
use rand::Rng;
use std::future::Future;
use std::time::Duration;

pub const DEFAULT_STREAM_MAX_RETRIES: u64 = 5;
pub const MAX_STREAM_MAX_RETRIES: u64 = 100;

/// One initial attempt plus the configured number of reconnects. Cancellation
/// remains authoritative during both the provider stream and its retry delay.
pub(crate) async fn run<F, Fut>(
    messages: &[Message],
    signal: &AbortSignal,
    max_retries: u64,
    mut operation: F,
) -> CompactionResult
where
    F: FnMut() -> Fut,
    Fut: Future<Output = CompactionResult>,
{
    for retry in 0..=max_retries.min(MAX_STREAM_MAX_RETRIES) {
        let result = tokio::select! {
            biased;
            _ = signal.cancelled() => return failed(messages, "Context compaction was interrupted."),
            result = operation() => result,
        };
        if result.compacted || retry == max_retries.min(MAX_STREAM_MAX_RETRIES) {
            return result;
        }
        let Some(delay) = result.diagnostics.iter().find_map(|diagnostic| {
            if diagnostic.stage != CompactionDiagnosticStage::Summary
                || diagnostic.kind == CompactionDiagnosticKind::PromptTooLong
            {
                return None;
            }
            diagnostic.provider_error.as_ref().map_or_else(
                || Some(backoff(retry + 1)),
                |error| stream_retry_delay(error, retry + 1),
            )
        }) else {
            return result;
        };
        tokio::select! {
            biased;
            _ = signal.cancelled() => return failed(messages, "Context compaction was interrupted."),
            _ = tokio::time::sleep(delay) => {},
        }
    }
    unreachable!("The finite retry loop always returns")
}

/// Server retry advice replaces the local exponential delay, including an
/// explicit zero. Context and usage failures require another recovery path.
pub(crate) fn stream_retry_delay(error: &ProviderError, retry: u64) -> Option<Duration> {
    if error.kind == ProviderErrorKind::PromptTooLong
        || error
            .code
            .as_ref()
            .is_some_and(|code| code == "session_budget_exceeded" || code == "budget_exceeded")
    {
        return None;
    }
    Some(
        error
            .retry_after_ms
            .map_or_else(|| backoff(retry), Duration::from_millis),
    )
}

/// Start at 200 ms, doubling with each retry and up to ten percent jitter.
pub(crate) fn backoff(retry: u64) -> Duration {
    let exponent = retry.saturating_sub(1).min(i32::MAX as u64) as i32;
    let base_ms = 200.0 * 2.0_f64.powi(exponent);
    let jitter = rand::thread_rng().gen_range(0.9..1.1);
    Duration::from_millis((base_ms * jitter) as u64)
}

fn failed(messages: &[Message], message: &str) -> CompactionResult {
    CompactionResult {
        messages: messages.to_vec(),
        freed_tokens: 0,
        compacted: false,
        reason: Some(CompactionNoopReason::NoProgress),
        diagnostics: vec![CompactionDiagnostic::new(
            CompactionDiagnosticStage::Summary,
            CompactionDiagnosticKind::ProviderError,
            Some(message.into()),
        )],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn error(kind: ProviderErrorKind, retry_after_ms: Option<u64>) -> ProviderError {
        ProviderError {
            kind,
            message: "summary transport failed".into(),
            status: None,
            code: None,
            error_type: None,
            media_origins: None,
            media_repair_complete: false,
            credential_recovery: None,
            retry_after_ms,
            request_id: None,
        }
    }

    fn rejected(error: ProviderError) -> CompactionResult {
        CompactionResult {
            messages: Vec::new(),
            freed_tokens: 0,
            compacted: false,
            reason: Some(CompactionNoopReason::NoProgress),
            diagnostics: vec![CompactionDiagnostic::from_provider(
                CompactionDiagnosticStage::Summary,
                CompactionDiagnosticKind::ProviderError,
                error,
            )],
        }
    }

    #[test]
    fn exponential_backoff_stays_inside_its_jitter_bounds() {
        for retry in 1..=6 {
            let base = 200_u64 * (1 << (retry - 1));
            let delay = backoff(retry).as_millis() as u64;
            assert!((base * 9 / 10..base * 11 / 10).contains(&delay));
        }
    }

    #[test]
    fn service_retry_advice_overrides_local_backoff() {
        let limited = error(ProviderErrorKind::RateLimit, Some(7_500));
        assert_eq!(
            stream_retry_delay(&limited, 5),
            Some(Duration::from_millis(7_500))
        );
        let immediate = error(ProviderErrorKind::Network, Some(0));
        assert_eq!(stream_retry_delay(&immediate, 5), Some(Duration::ZERO));
        let full = error(ProviderErrorKind::PromptTooLong, Some(1));
        assert_eq!(stream_retry_delay(&full, 1), None);
        for kind in [
            ProviderErrorKind::Auth,
            ProviderErrorKind::InvalidRequest,
            ProviderErrorKind::Unknown,
        ] {
            assert_eq!(
                stream_retry_delay(&error(kind, Some(1)), 1),
                Some(Duration::from_millis(1))
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn default_retry_budget_has_one_initial_call_and_five_reconnects() {
        let calls = AtomicU64::new(0);
        let signal = AbortSignal::new();
        let result = run(&[], &signal, DEFAULT_STREAM_MAX_RETRIES, || async {
            calls.fetch_add(1, Ordering::SeqCst);
            rejected(error(ProviderErrorKind::Network, Some(0)))
        })
        .await;
        assert!(!result.compacted);
        assert_eq!(calls.load(Ordering::SeqCst), 6);
    }

    #[tokio::test(start_paused = true)]
    async fn premature_eof_diagnostics_use_the_same_finite_retry_budget() {
        let calls = AtomicU64::new(0);
        let signal = AbortSignal::new();
        let result = run(&[], &signal, 2, || async {
            calls.fetch_add(1, Ordering::SeqCst);
            failed(
                &[],
                "Summary stream ended without a complete text response.",
            )
        })
        .await;
        assert!(!result.compacted);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_during_server_wait_does_not_open_another_stream() {
        let calls = AtomicU64::new(0);
        let signal = AbortSignal::new();
        let operation = run(&[], &signal, DEFAULT_STREAM_MAX_RETRIES, || async {
            calls.fetch_add(1, Ordering::SeqCst);
            rejected(error(ProviderErrorKind::RateLimit, Some(60_000)))
        });
        tokio::pin!(operation);
        tokio::select! {
            _ = &mut operation => panic!("the server wait must remain pending"),
            _ = tokio::time::sleep(Duration::from_millis(1)) => {},
        }
        signal.cancel();
        let result = operation.await;
        assert!(!result.compacted);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
