// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified to bind runtime-owned errors, metadata and session contracts.

//! Reusable approval conversations, bounded attempts, assessments and turn denials.

#[path = "approval_review_support/assessment.rs"]
mod assessment;
#[path = "approval_review_support/circuit_breaker.rs"]
mod circuit_breaker;
#[path = "approval_review_support/conversation.rs"]
mod conversation;
#[path = "approval_review_support/deadline.rs"]
mod deadline;
#[path = "approval_review_support/outcome.rs"]
mod outcome;
#[path = "approval_review_support/pool.rs"]
mod pool;
#[path = "approval_review_support/retry.rs"]
mod retry;

pub use assessment::{
    output_contract_prompt, output_schema, parse_assessment, AssessmentOutcome, RiskLevel,
    UserAuthorization,
};
pub use circuit_breaker::{
    DenialCircuitBreaker, DenialCircuitBreakerAction, DenialCircuitBreakerPolicy,
};
pub use conversation::{ConversationCheckpoint, ConversationState};
pub use deadline::run_before_review_deadline;
pub use outcome::{ReviewError, ReviewOutcome, ReviewSessionOutcome, SessionErrorInfo};
pub use pool::{
    ReviewAnalytics, ReviewSessionKind, ReviewSessionResult, ReviewerPool, ReviewerRequest,
    ReviewerSession, ReviewerTasks, SessionDisposition,
};
pub use retry::{run_with_retry, ReviewSessionLimits};

pub const MAX_REVIEW_ATTEMPTS: i64 = 3;
pub const REVIEW_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(90);
