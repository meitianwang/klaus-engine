//! Cooperative cancellation for async work.
//!
//! Turns, provider streams, tool calls, and approval handlers all take an
//! `AbortSignal`. A [`tokio_util::sync::CancellationToken`]
//! provides the contract: `is_cancelled()` reports the cancelled state, and
//! awaiting `cancelled()` completes when cancellation is requested.

/// The cancellation handle threaded through async work.
pub type AbortSignal = tokio_util::sync::CancellationToken;
