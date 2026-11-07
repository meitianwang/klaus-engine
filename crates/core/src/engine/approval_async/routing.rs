use super::{AsyncReviewConfig, ModelReviewPolicy, ReviewScope, ScoreProgress};
use std::future::Future;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReviewFallbackReason {
    MissingScore,
    StaleScore,
    ScoringFailure,
    AuthorizationChanged,
    IncompatibleCompaction,
    ElevatedRisk,
    InvalidScore,
    FreshRequired,
    Policy,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CachedApproval {
    /// The first computer-use allowance cannot cancel an in-progress assessment.
    pub releases_pending_review: bool,
}

/// Captured after resolving current authorization; scores are read again afterward.
pub struct CacheCheck<'a, A> {
    pub call_id: Option<&'a str>,
    pub category: ReviewScope,
    pub action_fits: bool,
    pub compaction_compatible: bool,
    pub is_computer_use_js: bool,
    pub current_authorization: Option<&'a A>,
    pub retained_context_complete: bool,
}

pub fn cached_evidence<A: Clone + PartialEq>(
    progress: &ScoreProgress<A>,
    config: &AsyncReviewConfig,
    policy: &ModelReviewPolicy,
    input: CacheCheck<'_, A>,
) -> Result<CachedApproval, ReviewFallbackReason> {
    let cached = progress.inspect(input.call_id);
    if !input.action_fits || cached.oversized {
        return Err(ReviewFallbackReason::ScoringFailure);
    }
    if !input.compaction_compatible {
        return Err(ReviewFallbackReason::IncompatibleCompaction);
    }
    if input.category == ReviewScope::ComputerUse
        && policy.allows_initial_computer_use()
        && input.is_computer_use_js
        && cached.js_executions == 1
    {
        return Ok(CachedApproval {
            releases_pending_review: false,
        });
    }
    let Some(current) = input.current_authorization else {
        return Err(ReviewFallbackReason::AuthorizationChanged);
    };
    if !input.retained_context_complete {
        return Err(ReviewFallbackReason::Policy);
    }
    if cached.lag > config.max_tool_call_lag {
        return Err(ReviewFallbackReason::StaleScore);
    }
    if cached.has_unscored_failure {
        return Err(ReviewFallbackReason::ScoringFailure);
    }
    match cached.action_risk {
        None => Err(ReviewFallbackReason::MissingScore),
        Some(score) if score < config.review_threshold => {
            if cached.authorization.as_ref() != Some(current) {
                Err(ReviewFallbackReason::AuthorizationChanged)
            } else if input.call_id.is_some() && !cached.score_at_or_before_action {
                Err(ReviewFallbackReason::MissingScore)
            } else {
                Ok(CachedApproval {
                    releases_pending_review: cached.score_at_or_before_action,
                })
            }
        }
        Some(score) if score >= config.review_threshold => Err(ReviewFallbackReason::ElevatedRisk),
        Some(_) => Err(ReviewFallbackReason::InvalidScore),
    }
}

/// The subscriber is created before the first check, closing publication races.
/// The synchronous reviewer owns cancellation/draining and final action revalidation.
pub async fn wait_for_cached_approval<A, Check, CheckFuture>(
    progress: &ScoreProgress<A>,
    mut check: Check,
) where
    Check: FnMut() -> CheckFuture,
    CheckFuture: Future<Output = Result<CachedApproval, ReviewFallbackReason>>,
{
    let mut updates = progress.subscribe();
    loop {
        if check()
            .await
            .is_ok_and(|approval| approval.releases_pending_review)
        {
            return;
        }
        if updates.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

pub enum ReviewRace<T> {
    Synchronous(T),
    Cached { drained: T },
    Aborted { drained: T },
    TimedOut { drained: T },
}

/// A completed blocking assessment wins a simultaneous race. A cached winner
/// cancels and joins its redundant reviewer before validating the original action.
pub async fn race_review<T, Revalidate, RevalidateFuture>(
    review: impl Future<Output = T>,
    cached: impl Future<Output = ()>,
    review_cancellation: &tokio_util::sync::CancellationToken,
    external_cancellation: &tokio_util::sync::CancellationToken,
    deadline: tokio::time::Instant,
    revalidate: Revalidate,
) -> ReviewRace<T>
where
    Revalidate: FnOnce() -> RevalidateFuture,
    RevalidateFuture: Future<Output = bool>,
{
    tokio::pin!(review);
    tokio::select! {
        biased;
        result = &mut review => ReviewRace::Synchronous(result),
        () = cached => {
            review_cancellation.cancel();
            let drained = review.await;
            if !revalidate().await || external_cancellation.is_cancelled() {
                ReviewRace::Aborted { drained }
            } else if tokio::time::Instant::now() >= deadline {
                ReviewRace::TimedOut { drained }
            } else {
                ReviewRace::Cached { drained }
            }
        }
    }
}
