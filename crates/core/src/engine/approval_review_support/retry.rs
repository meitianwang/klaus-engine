// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified to bind runtime-owned errors, metadata and session contracts.

//! Applies one review's attempt budget, deadline and cancellation to host-owned sessions.
//! Resubmissions are independent; this layer keeps no state between approval requests.

use super::{ReviewAnalytics, ReviewError, ReviewOutcome, SessionErrorInfo};
use rand::Rng;
use std::future::Future;
use std::time::Duration;
use tokio::time::sleep_until;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

pub struct ReviewSessionLimits {
    pub max_attempts: i64,
    pub deadline: Instant,
}

impl Default for ReviewSessionLimits {
    fn default() -> Self {
        Self {
            max_attempts: super::MAX_REVIEW_ATTEMPTS,
            deadline: Instant::now() + super::REVIEW_TIMEOUT,
        }
    }
}

/// Retries only recoverable review failures within one shared deadline.
/// Returns the final attempt's evidence without carrying it into subsequent attempts.
pub async fn run_with_retry<Evidence, F, Attempt>(
    limits: ReviewSessionLimits,
    external_cancel: Option<&CancellationToken>,
    mut run_attempt: F,
) -> (ReviewOutcome, ReviewAnalytics, Option<Evidence>)
where
    F: FnMut(Instant) -> Attempt,
    Attempt: Future<Output = (ReviewOutcome, ReviewAnalytics, Option<Evidence>)>,
{
    let ReviewSessionLimits {
        max_attempts,
        deadline,
    } = limits;
    assert!(max_attempts > 0, "approval review must run at least once");
    let mut attempt_count = 1;
    loop {
        let (outcome, mut analytics_result, evidence) = run_attempt(deadline).await;
        analytics_result.attempt_count = attempt_count;
        if attempt_count >= max_attempts || !should_retry_review(&outcome) {
            return (outcome, analytics_result, evidence);
        }
        let retry_at = match &outcome {
            ReviewOutcome::Error(ReviewError::Session { retry_at, .. }) => *retry_at,
            _ => None,
        };
        if let Some(error) = wait_before_review_retry(
            attempt_count,
            max_attempts,
            &outcome,
            retry_at,
            deadline,
            external_cancel,
        )
        .await
        {
            return (ReviewOutcome::Error(error), analytics_result, None);
        }
        attempt_count += 1;
    }
}

async fn wait_before_review_retry(
    attempt_count: i64,
    max_attempts: i64,
    outcome: &ReviewOutcome,
    retry_not_before: Option<Instant>,
    deadline: Instant,
    external_cancel: Option<&CancellationToken>,
) -> Option<ReviewError> {
    let exponential_delay = 200.0 * 2.0_f64.powi(attempt_count.saturating_sub(1) as i32);
    let jitter = rand::thread_rng().gen_range(0.9..1.1);
    let retry_delay = Duration::from_millis((exponential_delay * jitter) as u64);
    let now = Instant::now();
    let requested_retry_at = (now + retry_delay).max(retry_not_before.unwrap_or(now));
    let retry_at = requested_retry_at.min(deadline);
    let (failure_reason, error_info) = match outcome {
        ReviewOutcome::Error(error) => (
            Some(error.failure_reason()),
            if let ReviewError::Session { error_info, .. } = error {
                error_info.as_ref()
            } else {
                None
            },
        ),
        ReviewOutcome::CachedApproval | ReviewOutcome::Completed(_) => (None, None),
    };
    tracing::debug!(
        attempt_count,
        next_attempt = attempt_count + 1,
        max_attempts,
        ?failure_reason,
        ?error_info,
        wait_ms = retry_at.saturating_duration_since(now).as_millis() as u64,
        backoff_ms = retry_delay.as_millis() as u64,
        retry_not_before_ms = retry_not_before
            .map(|instant| instant.saturating_duration_since(now).as_millis() as u64),
        remaining_budget_ms = deadline.saturating_duration_since(now).as_millis() as u64,
        deadline_limited = requested_retry_at >= deadline,
        "Approval review retry scheduled"
    );
    tokio::select! {
        _ = sleep_until(retry_at) => {
            (Instant::now() >= deadline).then_some(ReviewError::Timeout)
        }
        _ = async {
            if let Some(cancel_token) = external_cancel {
                cancel_token.cancelled().await;
            } else {
                std::future::pending::<()>().await;
            }
        } => Some(ReviewError::Cancelled),
    }
}

pub fn should_retry_review(outcome: &ReviewOutcome) -> bool {
    match outcome {
        ReviewOutcome::Error(ReviewError::Parse { .. } | ReviewError::StaleAuthorization) => true,
        ReviewOutcome::Error(ReviewError::Session {
            error_info: Some(error),
            ..
        }) => match error {
            SessionErrorInfo::RateLimitExceeded
            | SessionErrorInfo::FlexUnavailable
            | SessionErrorInfo::ServerOverloaded
            | SessionErrorInfo::InternalServerError => true,
            SessionErrorInfo::HttpConnectionFailed { http_status_code }
            | SessionErrorInfo::ResponseStreamConnectionFailed { http_status_code }
            | SessionErrorInfo::ResponseStreamDisconnected { http_status_code }
            | SessionErrorInfo::ResponseTooManyFailedAttempts { http_status_code } => {
                matches!(http_status_code, None | Some(408 | 429 | 500..=599))
            }
            SessionErrorInfo::ContextWindowExceeded
            | SessionErrorInfo::SessionBudgetExceeded
            | SessionErrorInfo::UsageLimitExceeded
            | SessionErrorInfo::CyberPolicy
            | SessionErrorInfo::BioPolicy
            | SessionErrorInfo::MisalignmentPolicyViolation
            | SessionErrorInfo::TooManyDenials
            | SessionErrorInfo::Unauthorized
            | SessionErrorInfo::BadRequest
            | SessionErrorInfo::InvalidPrompt
            | SessionErrorInfo::SandboxError
            | SessionErrorInfo::ActiveTurnNotSteerable
            | SessionErrorInfo::ThreadRollbackFailed
            | SessionErrorInfo::Other => false,
        },
        ReviewOutcome::CachedApproval
        | ReviewOutcome::Completed(_)
        | ReviewOutcome::Error(
            ReviewError::InputBudgetExceeded
            | ReviewError::PromptBuild { .. }
            | ReviewError::Session {
                error_info: None, ..
            }
            | ReviewError::Timeout
            | ReviewError::Cancelled,
        ) => false,
    }
}

#[cfg(test)]
#[path = "retry_tests.rs"]
mod tests;
