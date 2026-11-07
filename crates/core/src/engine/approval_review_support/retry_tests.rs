// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified for runtime component contracts and deterministic regressions.

use super::super::outcome::ReviewFailureReason;
use super::*;
use crate::engine::approval_review_support::{
    parse_assessment, run_before_review_deadline, ReviewSessionOutcome, MAX_REVIEW_ATTEMPTS,
    REVIEW_TIMEOUT,
};

#[test]
fn retry_classification_preserves_failure_categories_and_transport_statuses() {
    for info in [
        SessionErrorInfo::RateLimitExceeded,
        SessionErrorInfo::FlexUnavailable,
        SessionErrorInfo::ServerOverloaded,
        SessionErrorInfo::InternalServerError,
    ] {
        assert!(should_retry_review(&ReviewOutcome::Error(
            ReviewError::session_with_error_info("temporary", info)
        )));
    }
    for status in [
        None,
        Some(200),
        Some(400),
        Some(401),
        Some(403),
        Some(408),
        Some(429),
        Some(500),
        Some(599),
    ] {
        let expected = matches!(status, None | Some(408 | 429 | 500..=599));
        for info in [
            SessionErrorInfo::HttpConnectionFailed {
                http_status_code: status,
            },
            SessionErrorInfo::ResponseStreamConnectionFailed {
                http_status_code: status,
            },
            SessionErrorInfo::ResponseStreamDisconnected {
                http_status_code: status,
            },
            SessionErrorInfo::ResponseTooManyFailedAttempts {
                http_status_code: status,
            },
        ] {
            assert_eq!(
                should_retry_review(&ReviewOutcome::Error(ReviewError::session_with_error_info(
                    "transport",
                    info
                ))),
                expected
            );
        }
    }
    for info in [
        SessionErrorInfo::ContextWindowExceeded,
        SessionErrorInfo::SessionBudgetExceeded,
        SessionErrorInfo::UsageLimitExceeded,
        SessionErrorInfo::CyberPolicy,
        SessionErrorInfo::BioPolicy,
        SessionErrorInfo::MisalignmentPolicyViolation,
        SessionErrorInfo::TooManyDenials,
        SessionErrorInfo::Unauthorized,
        SessionErrorInfo::BadRequest,
        SessionErrorInfo::InvalidPrompt,
        SessionErrorInfo::SandboxError,
        SessionErrorInfo::ActiveTurnNotSteerable,
        SessionErrorInfo::ThreadRollbackFailed,
        SessionErrorInfo::Other,
    ] {
        assert!(!should_retry_review(&ReviewOutcome::Error(
            ReviewError::session_with_error_info("terminal", info)
        )));
    }
    assert!(should_retry_review(&ReviewOutcome::Error(
        ReviewError::parse("malformed response")
    )));
    assert!(should_retry_review(&ReviewOutcome::Error(
        ReviewError::StaleAuthorization
    )));
    for error in [
        ReviewError::InputBudgetExceeded,
        ReviewError::prompt_build("context"),
        ReviewError::session("unclassified"),
        ReviewError::Timeout,
        ReviewError::Cancelled,
    ] {
        assert!(!should_retry_review(&ReviewOutcome::Error(error)));
    }
    assert!(!should_retry_review(&ReviewOutcome::Completed(
        parse_assessment(r#"{"outcome":"deny"}"#).unwrap()
    )));
    assert!(!should_retry_review(&ReviewOutcome::CachedApproval));
    assert_eq!(
        ReviewError::parse("bad JSON").failure_reason(),
        ReviewFailureReason::ParseError
    );
    assert_eq!(
        ReviewError::InputBudgetExceeded.failure_reason(),
        ReviewFailureReason::SessionError
    );
}

#[tokio::test(start_paused = true)]
async fn stale_authorization_shares_three_attempts_one_deadline_and_latest_evidence() {
    let started = Instant::now();
    let deadline = started + REVIEW_TIMEOUT;
    let mut attempts = 0;
    let (outcome, analytics, evidence) = run_with_retry(
        ReviewSessionLimits {
            max_attempts: MAX_REVIEW_ATTEMPTS,
            deadline,
        },
        None,
        |attempt_deadline| {
            assert_eq!(attempt_deadline, deadline);
            attempts += 1;
            let attempt = attempts;
            async move {
                (
                    ReviewOutcome::Error(ReviewError::StaleAuthorization),
                    ReviewAnalytics::without_session(),
                    Some(attempt),
                )
            }
        },
    )
    .await;
    assert!(matches!(
        outcome,
        ReviewOutcome::Error(ReviewError::StaleAuthorization)
    ));
    assert_eq!(
        (attempts, analytics.attempt_count, evidence),
        (3, 3, Some(3))
    );
    let elapsed = Instant::now() - started;
    assert!(elapsed >= Duration::from_millis(540) && elapsed < Duration::from_millis(660));
}

#[tokio::test(start_paused = true)]
async fn retry_after_is_preserved_and_only_final_attempt_evidence_is_returned() {
    let started = Instant::now();
    let mut attempts = 0;
    let (outcome, analytics, evidence) =
        run_with_retry(ReviewSessionLimits::default(), None, |_| {
            attempts += 1;
            let attempt = attempts;
            async move {
                let outcome = if attempt == 1 {
                    ReviewOutcome::Error(ReviewError::Session {
                        message: "wait for provider".into(),
                        error_info: Some(SessionErrorInfo::RateLimitExceeded),
                        retry_at: Some(started + Duration::from_secs(2)),
                    })
                } else {
                    ReviewOutcome::Completed(parse_assessment(r#"{"outcome":"allow"}"#).unwrap())
                };
                (outcome, ReviewAnalytics::without_session(), Some(attempt))
            }
        })
        .await;
    assert!(matches!(outcome, ReviewOutcome::Completed(_)));
    assert_eq!((analytics.attempt_count, evidence), (2, Some(2)));
    assert_eq!(Instant::now() - started, Duration::from_secs(2));
}

#[tokio::test(start_paused = true)]
async fn retry_wait_cannot_extend_deadline_and_drops_attempt_evidence_on_timeout() {
    let mut attempts = 0;
    let (outcome, analytics, evidence) = run_with_retry(
        ReviewSessionLimits {
            max_attempts: 3,
            deadline: Instant::now() + Duration::from_millis(100),
        },
        None,
        |_| {
            attempts += 1;
            async {
                (
                    ReviewOutcome::Error(ReviewError::StaleAuthorization),
                    ReviewAnalytics::without_session(),
                    Some("old evidence"),
                )
            }
        },
    )
    .await;
    assert!(matches!(
        outcome,
        ReviewOutcome::Error(ReviewError::Timeout)
    ));
    assert_eq!((attempts, analytics.attempt_count, evidence), (1, 1, None));
}

#[tokio::test(start_paused = true)]
async fn cancellation_and_preparation_deadline_keep_distinct_outcomes() {
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let error = wait_before_review_retry(
        1,
        3,
        &ReviewOutcome::Error(ReviewError::StaleAuthorization),
        None,
        Instant::now() + REVIEW_TIMEOUT,
        Some(&cancelled),
    )
    .await;
    assert!(matches!(error, Some(ReviewError::Cancelled)));
    let result = run_before_review_deadline(
        Instant::now() + REVIEW_TIMEOUT,
        Some(&cancelled),
        std::future::pending::<()>(),
    )
    .await;
    assert!(matches!(result, Err(ReviewSessionOutcome::Aborted)));
    let result = run_before_review_deadline(
        Instant::now() + Duration::from_secs(1),
        None,
        std::future::pending::<()>(),
    )
    .await;
    assert!(matches!(result, Err(ReviewSessionOutcome::TimedOut)));
    let limits = ReviewSessionLimits::default();
    assert_eq!(limits.max_attempts, 3);
    assert_eq!(limits.deadline - Instant::now(), Duration::from_secs(90));
}
