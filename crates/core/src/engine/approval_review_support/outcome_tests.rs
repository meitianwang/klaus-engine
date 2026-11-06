// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified for runtime component contracts and deterministic regressions.

use super::super::retry::should_retry_review;
use super::*;

fn provider_error(
    kind: ProviderErrorKind,
    code: Option<&str>,
    status: Option<u16>,
) -> ProviderError {
    ProviderError {
        kind,
        message: "request failed".into(),
        status,
        code: code.map(Into::into),
        error_type: None,
        media_origins: None,
        media_repair_complete: false,
        credential_recovery: None,
        retry_after_ms: None,
        request_id: None,
    }
}

#[test]
fn provider_machine_codes_preserve_each_review_failure_class() {
    let status = Some(429);
    let cases = [
        ("rate_limit_exceeded", SessionErrorInfo::RateLimitExceeded),
        ("flex_unavailable", SessionErrorInfo::FlexUnavailable),
        ("server_overloaded", SessionErrorInfo::ServerOverloaded),
        (
            "internal_server_error",
            SessionErrorInfo::InternalServerError,
        ),
        (
            "http_connection_failed",
            SessionErrorInfo::HttpConnectionFailed {
                http_status_code: status,
            },
        ),
        (
            "response_stream_connection_failed",
            SessionErrorInfo::ResponseStreamConnectionFailed {
                http_status_code: status,
            },
        ),
        (
            "response_stream_disconnected",
            SessionErrorInfo::ResponseStreamDisconnected {
                http_status_code: status,
            },
        ),
        (
            "response_too_many_failed_attempts",
            SessionErrorInfo::ResponseTooManyFailedAttempts {
                http_status_code: status,
            },
        ),
        (
            "context_window_exceeded",
            SessionErrorInfo::ContextWindowExceeded,
        ),
        (
            "session_budget_exceeded",
            SessionErrorInfo::SessionBudgetExceeded,
        ),
        ("usage_limit_exceeded", SessionErrorInfo::UsageLimitExceeded),
        ("cyber_policy", SessionErrorInfo::CyberPolicy),
        ("bio_policy", SessionErrorInfo::BioPolicy),
        (
            "misalignment_policy_violation",
            SessionErrorInfo::MisalignmentPolicyViolation,
        ),
        ("too_many_denials", SessionErrorInfo::TooManyDenials),
        ("unauthorized", SessionErrorInfo::Unauthorized),
        ("bad_request", SessionErrorInfo::BadRequest),
        ("invalid_prompt", SessionErrorInfo::InvalidPrompt),
        ("sandbox_error", SessionErrorInfo::SandboxError),
        (
            "active_turn_not_steerable",
            SessionErrorInfo::ActiveTurnNotSteerable,
        ),
        (
            "thread_rollback_failed",
            SessionErrorInfo::ThreadRollbackFailed,
        ),
    ];
    for (code, expected) in cases {
        let error = provider_error(ProviderErrorKind::RateLimit, Some(code), status);
        assert_eq!(
            SessionErrorInfo::from_provider_error(&error),
            expected,
            "{code}"
        );
    }
}

#[test]
fn terminal_policy_codes_override_retryable_kind_and_http_status() {
    for code in [
        "cyber_policy",
        "bio_policy_violation",
        "misalignment_policy_violation",
        "invalid_prompt",
        "usage_not_included",
    ] {
        let error = provider_error(ProviderErrorKind::Overloaded, Some(code), Some(503));
        let info = SessionErrorInfo::from_provider_error(&error);
        assert!(
            !should_retry_review(&ReviewOutcome::Error(ReviewError::session_with_error_info(
                error, info
            ))),
            "{code}"
        );
    }
    let mut error = provider_error(
        ProviderErrorKind::Overloaded,
        Some("unrecognized"),
        Some(503),
    );
    error.error_type = Some("invalid_prompt".into());
    assert_eq!(
        SessionErrorInfo::from_provider_error(&error),
        SessionErrorInfo::InvalidPrompt
    );
}

#[test]
fn stream_phase_and_status_survive_conversion_without_retrying_local_deadlines() {
    let stream = provider_error(
        ProviderErrorKind::Network,
        Some("klaus_stream_interrupted"),
        None,
    );
    assert_eq!(
        SessionErrorInfo::from_provider_error(&stream),
        SessionErrorInfo::ResponseStreamDisconnected {
            http_status_code: None
        }
    );
    for (status, expected_retry) in [
        (Some(408), true),
        (Some(429), true),
        (Some(500), true),
        (Some(400), false),
        (None, false),
    ] {
        let error = provider_error(ProviderErrorKind::Unknown, None, status);
        let info = SessionErrorInfo::from_provider_error(&error);
        assert_eq!(
            should_retry_review(&ReviewOutcome::Error(ReviewError::session_with_error_info(
                error, info
            ))),
            expected_retry,
            "{status:?}"
        );
    }
    let deadline = provider_error(ProviderErrorKind::Timeout, None, None);
    assert_eq!(
        SessionErrorInfo::from_provider_error(&deadline),
        SessionErrorInfo::Other
    );
}

#[test]
fn explicit_http_timeouts_retry_without_retrying_local_deadlines_or_terminal_signals() {
    for (status, code, expected, expected_retry) in [
        (
            Some(408),
            "request_timeout",
            SessionErrorInfo::HttpConnectionFailed {
                http_status_code: Some(408),
            },
            true,
        ),
        (None, "request_timeout", SessionErrorInfo::Other, false),
        (
            Some(408),
            "invalid_prompt",
            SessionErrorInfo::InvalidPrompt,
            false,
        ),
    ] {
        let mut error = provider_error(ProviderErrorKind::Timeout, Some(code), status);
        error.error_type = Some("request_timeout".into());
        let info = SessionErrorInfo::from_provider_error(&error);
        assert_eq!(info, expected, "{status:?}: {code}");
        assert_eq!(
            should_retry_review(&ReviewOutcome::Error(ReviewError::session_with_error_info(
                error, info
            ))),
            expected_retry,
            "{status:?}: {code}"
        );
    }
}

#[test]
fn engine_conversion_keeps_provider_details_and_local_budget_semantics() {
    let provider = provider_error(ProviderErrorKind::RateLimit, Some("bio_policy"), Some(429));
    let error = EngineError::from_provider(provider, "retry exhausted");
    assert_eq!(
        SessionErrorInfo::from_engine_error(&error),
        Some(SessionErrorInfo::BioPolicy)
    );
    let mut local = EngineError {
        code: EngineErrorCode::BudgetExhausted,
        message: "session budget exceeded".into(),
        cause: None,
        request_id: None,
        provider_kind: None,
        retry_after_ms: None,
    };
    assert_eq!(
        SessionErrorInfo::from_engine_error(&local),
        Some(SessionErrorInfo::SessionBudgetExceeded)
    );
    local.code = EngineErrorCode::Aborted;
    assert_eq!(SessionErrorInfo::from_engine_error(&local), None);
}

#[test]
fn review_errors_display_actionable_failure_text() {
    let cases = [
        (
            ReviewError::InputBudgetExceeded,
            "Approval review input budget exceeded",
        ),
        (
            ReviewError::prompt_build("missing policy"),
            "Unable to prepare approval review: missing policy",
        ),
        (
            ReviewError::session("provider unavailable"),
            "Approval review session failed: provider unavailable",
        ),
        (
            ReviewError::parse("invalid JSON"),
            "Invalid approval assessment: invalid JSON",
        ),
        (ReviewError::Timeout, "Approval review timed out"),
        (ReviewError::Cancelled, "Approval review was cancelled"),
        (
            ReviewError::StaleAuthorization,
            "Approval review authorization changed before completion",
        ),
    ];
    for (error, expected) in cases {
        assert_eq!(error.to_string(), expected);
        let typed: &dyn std::error::Error = &error;
        assert!(typed.source().is_none());
    }
}
