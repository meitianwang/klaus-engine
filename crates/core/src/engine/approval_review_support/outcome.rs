// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified to bind runtime-owned errors, metadata and session contracts.

//! Completed assessments and failures retain distinct semantics.

use super::assessment::{parse_assessment, ReviewAssessment};
use crate::types::errors::{EngineError, EngineErrorCode, ProviderError, ProviderErrorKind};
use std::fmt;
use tokio::time::Instant;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionErrorInfo {
    RateLimitExceeded,
    FlexUnavailable,
    ServerOverloaded,
    InternalServerError,
    HttpConnectionFailed { http_status_code: Option<u16> },
    ResponseStreamConnectionFailed { http_status_code: Option<u16> },
    ResponseStreamDisconnected { http_status_code: Option<u16> },
    ResponseTooManyFailedAttempts { http_status_code: Option<u16> },
    ContextWindowExceeded,
    SessionBudgetExceeded,
    UsageLimitExceeded,
    CyberPolicy,
    BioPolicy,
    MisalignmentPolicyViolation,
    TooManyDenials,
    Unauthorized,
    BadRequest,
    InvalidPrompt,
    SandboxError,
    ActiveTurnNotSteerable,
    ThreadRollbackFailed,
    Other,
}

impl SessionErrorInfo {
    /// Preserves machine-readable failure details before the coarser provider kind.
    pub fn from_provider_error(error: &ProviderError) -> Self {
        for signal in [&error.code, &error.error_type].into_iter().flatten() {
            let signal = String::from_utf8_lossy(&signal.to_node_utf8()).into_owned();
            if let Some(info) = Self::from_signal(&signal, error.status) {
                return info;
            }
        }
        Self::from_provider_kind(error.kind, error.status)
    }

    /// Local budget and input failures do not carry a provider response.
    pub fn from_engine_error(error: &EngineError) -> Option<Self> {
        if let Some(provider) = error.provider_error() {
            return Some(Self::from_provider_error(provider));
        }
        if let Some(kind) = error.provider_kind {
            return Some(Self::from_provider_kind(kind, None));
        }
        Some(match error.code {
            EngineErrorCode::AuthFailed => Self::Unauthorized,
            EngineErrorCode::PromptTooLong => Self::ContextWindowExceeded,
            EngineErrorCode::BudgetExhausted => Self::SessionBudgetExceeded,
            EngineErrorCode::InvalidInput => Self::BadRequest,
            EngineErrorCode::Internal => Self::InternalServerError,
            EngineErrorCode::Aborted => return None,
            EngineErrorCode::ProviderError
            | EngineErrorCode::CompactionThrash
            | EngineErrorCode::LoopDetected => Self::Other,
        })
    }

    fn from_signal(signal: &str, http_status_code: Option<u16>) -> Option<Self> {
        Some(match signal {
            "rate_limit_exceeded" | "rate_limit_error" => Self::RateLimitExceeded,
            "flex_unavailable" => Self::FlexUnavailable,
            "server_overloaded" | "server_is_overloaded" | "overloaded_error" => {
                Self::ServerOverloaded
            }
            "internal_server_error" | "server_error" => Self::InternalServerError,
            "http_connection_failed" => Self::HttpConnectionFailed { http_status_code },
            "response_stream_connection_failed" => {
                Self::ResponseStreamConnectionFailed { http_status_code }
            }
            "response_stream_disconnected" | "klaus_stream_interrupted" => {
                Self::ResponseStreamDisconnected { http_status_code }
            }
            "response_too_many_failed_attempts" => {
                Self::ResponseTooManyFailedAttempts { http_status_code }
            }
            "context_window_exceeded"
            | "context_length_exceeded"
            | "model_context_window_exceeded"
            | "prompt_too_long" => Self::ContextWindowExceeded,
            "session_budget_exceeded" => Self::SessionBudgetExceeded,
            "usage_limit_exceeded"
            | "usage_limit_reached"
            | "usage_not_included"
            | "insufficient_quota"
            | "billing_error" => Self::UsageLimitExceeded,
            "cyber_policy" => Self::CyberPolicy,
            "bio_policy" | "bio_policy_violation" => Self::BioPolicy,
            "misalignment_policy_violation" => Self::MisalignmentPolicyViolation,
            "too_many_denials" => Self::TooManyDenials,
            "unauthorized" | "authentication_error" | "permission_error" => Self::Unauthorized,
            "bad_request" | "invalid_request_error" => Self::BadRequest,
            "invalid_prompt" => Self::InvalidPrompt,
            "sandbox_error" => Self::SandboxError,
            "active_turn_not_steerable" => Self::ActiveTurnNotSteerable,
            "thread_rollback_failed" => Self::ThreadRollbackFailed,
            _ => return None,
        })
    }

    fn from_provider_kind(kind: ProviderErrorKind, http_status_code: Option<u16>) -> Self {
        match kind {
            ProviderErrorKind::RateLimit => Self::RateLimitExceeded,
            ProviderErrorKind::Overloaded => Self::ServerOverloaded,
            ProviderErrorKind::Network => Self::HttpConnectionFailed { http_status_code },
            ProviderErrorKind::Auth => Self::Unauthorized,
            ProviderErrorKind::PromptTooLong | ProviderErrorKind::RequestTooLarge => {
                Self::ContextWindowExceeded
            }
            ProviderErrorKind::Quota => Self::UsageLimitExceeded,
            ProviderErrorKind::InvalidRequest => Self::BadRequest,
            ProviderErrorKind::Timeout if http_status_code == Some(408) => {
                Self::HttpConnectionFailed { http_status_code }
            }
            ProviderErrorKind::Unknown
                if matches!(http_status_code, Some(408 | 429 | 500..=599)) =>
            {
                Self::HttpConnectionFailed { http_status_code }
            }
            ProviderErrorKind::Unknown
            | ProviderErrorKind::MaxOutput
            | ProviderErrorKind::Withheld
            | ProviderErrorKind::Timeout => Self::Other,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReviewFailureReason {
    PromptBuildError,
    SessionError,
    ParseError,
    Timeout,
    Cancelled,
    StaleAuthorization,
}

#[derive(Debug)]
pub enum ReviewOutcome {
    CachedApproval,
    Completed(ReviewAssessment),
    Error(ReviewError),
}

#[derive(Debug)]
pub enum ReviewError {
    InputBudgetExceeded,
    PromptBuild {
        message: String,
    },
    Session {
        message: String,
        error_info: Option<SessionErrorInfo>,
        retry_at: Option<Instant>,
    },
    Parse {
        message: String,
    },
    Timeout,
    Cancelled,
    StaleAuthorization,
}

impl ReviewError {
    pub fn prompt_build(error: impl ToString) -> Self {
        Self::PromptBuild {
            message: error.to_string(),
        }
    }
    pub fn session(error: impl ToString) -> Self {
        Self::Session {
            message: error.to_string(),
            error_info: None,
            retry_at: None,
        }
    }
    #[cfg(test)]
    pub fn session_with_error_info(error: impl ToString, error_info: SessionErrorInfo) -> Self {
        Self::Session {
            message: error.to_string(),
            error_info: Some(error_info),
            retry_at: None,
        }
    }
    pub fn parse(error: impl ToString) -> Self {
        Self::Parse {
            message: error.to_string(),
        }
    }
    pub fn failure_reason(&self) -> ReviewFailureReason {
        match self {
            Self::PromptBuild { .. } => ReviewFailureReason::PromptBuildError,
            Self::Session { .. } | Self::InputBudgetExceeded => ReviewFailureReason::SessionError,
            Self::Parse { .. } => ReviewFailureReason::ParseError,
            Self::Timeout => ReviewFailureReason::Timeout,
            Self::Cancelled => ReviewFailureReason::Cancelled,
            Self::StaleAuthorization => ReviewFailureReason::StaleAuthorization,
        }
    }
}

impl fmt::Display for ReviewError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InputBudgetExceeded => {
                formatter.write_str("Approval review input budget exceeded")
            }
            Self::PromptBuild { message } => {
                write!(formatter, "Unable to prepare approval review: {message}")
            }
            Self::Session { message, .. } => {
                write!(formatter, "Approval review session failed: {message}")
            }
            Self::Parse { message } => {
                write!(formatter, "Invalid approval assessment: {message}")
            }
            Self::Timeout => formatter.write_str("Approval review timed out"),
            Self::Cancelled => formatter.write_str("Approval review was cancelled"),
            Self::StaleAuthorization => {
                formatter.write_str("Approval review authorization changed before completion")
            }
        }
    }
}

impl std::error::Error for ReviewError {}

#[derive(Debug)]
pub enum ReviewSessionOutcome {
    Completed(Result<Option<String>, String>),
    PromptBuildFailed(String),
    InputBudgetExceeded,
    SessionFailed {
        error: String,
        error_info: Option<SessionErrorInfo>,
        retry_at: Option<Instant>,
    },
    TimedOut,
    Aborted,
    StaleAuthorization,
}

impl From<ReviewSessionOutcome> for ReviewOutcome {
    fn from(outcome: ReviewSessionOutcome) -> Self {
        match outcome {
            ReviewSessionOutcome::Completed(Ok(Some(message))) => {
                match parse_assessment(&message) {
                    Ok(assessment) => Self::Completed(assessment),
                    Err(error) => Self::Error(ReviewError::parse(error)),
                }
            }
            ReviewSessionOutcome::Completed(Ok(None)) => Self::Error(ReviewError::session(
                "Approval review completed without an assessment payload",
            )),
            ReviewSessionOutcome::Completed(Err(error)) => Self::Error(ReviewError::session(error)),
            ReviewSessionOutcome::PromptBuildFailed(error) => {
                Self::Error(ReviewError::prompt_build(error))
            }
            ReviewSessionOutcome::InputBudgetExceeded => {
                Self::Error(ReviewError::InputBudgetExceeded)
            }
            ReviewSessionOutcome::SessionFailed {
                error,
                error_info,
                retry_at,
            } => Self::Error(ReviewError::Session {
                message: error,
                error_info,
                retry_at,
            }),
            ReviewSessionOutcome::TimedOut => Self::Error(ReviewError::Timeout),
            ReviewSessionOutcome::Aborted => Self::Error(ReviewError::Cancelled),
            ReviewSessionOutcome::StaleAuthorization => {
                Self::Error(ReviewError::StaleAuthorization)
            }
        }
    }
}

#[cfg(test)]
#[path = "outcome_tests.rs"]
mod tests;
