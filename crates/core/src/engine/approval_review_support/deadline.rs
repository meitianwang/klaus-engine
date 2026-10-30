// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified to bind runtime-owned errors, metadata and session contracts.

//! Bounds session startup and context work by the review's existing deadline and cancellation.

use super::ReviewSessionOutcome;
use std::future::Future;
use tokio_util::sync::CancellationToken;

pub async fn run_before_review_deadline<T>(
    deadline: tokio::time::Instant,
    external_cancel: Option<&CancellationToken>,
    future: impl Future<Output = T>,
) -> Result<T, ReviewSessionOutcome> {
    tokio::select! {
        _ = tokio::time::sleep_until(deadline) => Err(ReviewSessionOutcome::TimedOut),
        result = future => Ok(result),
        _ = async {
            if let Some(cancel_token) = external_cancel {
                cancel_token.cancelled().await;
            } else {
                std::future::pending::<()>().await;
            }
        } => Err(ReviewSessionOutcome::Aborted),
    }
}
