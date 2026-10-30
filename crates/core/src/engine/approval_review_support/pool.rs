// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified to bind runtime-owned errors, metadata and session contracts.

//! Owns the reusable reviewer and temporary forks for one parent thread.
//! The host supplies agent startup; the host supplies captured context. Selection stays serialized;
//! concurrent reviews fork committed context. Lifetime guards cancel agents; the host runtime
//! performs cleanup and tracks its completion. The pool never runs a second shutdown protocol.
//! Startup and fork futures stay boxed to bound the orchestration stack frames.

use std::future::Future;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use futures::future::BoxFuture;
use tokio::sync::Mutex;
use tokio::sync::Semaphore;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::{run_before_review_deadline, ReviewSessionOutcome};

/// Identity of the selected private review runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReviewSessionKind {
    TrunkNew,
    TrunkReused,
    EphemeralForked,
}

/// Attempt metadata. Provider accounting and identities remain owned by the host.
#[derive(Debug, Default, Clone)]
pub struct ReviewAnalytics {
    pub attempt_count: i64,
    pub session_kind: Option<ReviewSessionKind>,
}
impl ReviewAnalytics {
    pub fn without_session() -> Self {
        Self::default()
    }
}

/// Background work owned by the reviewer for one parent runtime.
/// Stop cancels the work and joins this tracker before the parent closes its history.
#[derive(Default)]
pub struct ReviewerTasks {
    pub tasks: tokio_util::task::TaskTracker,
    pub cancellation: CancellationToken,
}

/// Context bookkeeping for a reviewer. Lifetime and cleanup belong to the host runtime.
/// The host owns the opaque compatibility context and committed history snapshots.
pub trait ReviewerSession: Send + Sync + 'static {
    type Setup: Send + Sync + 'static;
    type Context: Clone + PartialEq + Send + Sync;
    type Snapshot: Send + Sync;

    fn context(&self) -> &Self::Context;
    fn snapshot(&self) -> impl Future<Output = Option<Self::Snapshot>> + Send;
}

/// Executes one approval on a selected session. The host must drain the submitted
/// turn before returning Reusable or Rollback, and must keep the issuing action and permissions bound.
/// The pool reserves the session while running the request; the host must capture, validate,
/// and publish any reusable checkpoint before returning, without yielding after validation.
pub trait ReviewerRequest: Send + Sync {
    type Session: ReviewerSession;

    fn setup(&self) -> Arc<<Self::Session as ReviewerSession>::Setup>;
    fn context(
        &self,
        previous: Option<&Self::Session>,
    ) -> <Self::Session as ReviewerSession>::Context;
    /// Requires a newly spawned session without a cached snapshot for this attempt.
    fn requires_fresh_session(&self) -> bool {
        false
    }
    fn deadline(&self) -> Instant;
    fn cancellation(&self) -> Option<&CancellationToken>;
    fn run(
        &self,
        session: &Self::Session,
        kind: ReviewSessionKind,
    ) -> impl Future<Output = ReviewSessionResult> + Send;
}

/// Result of one review, including whether its session can serve the next request.
pub struct ReviewSessionResult {
    pub outcome: ReviewSessionOutcome,
    pub disposition: SessionDisposition,
    pub analytics: ReviewAnalytics,
}

/// How the pool may reuse a session and its committed history after an attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionDisposition {
    Reusable,
    /// Drop this attempt's context; continue from the last committed review.
    Rollback,
    Discard,
}

/// Per-parent reviewer state. The same pool serves prewarm, review, invalidation and shutdown.
pub struct ReviewerPool<S: ReviewerSession> {
    trunk: Mutex<Option<Arc<Trunk<S>>>>,
    runtime: Arc<ReviewerTasks>,
    spawn: Box<SpawnReviewer<S>>,
}

type SpawnReviewer<S> = dyn Fn(
        Arc<<S as ReviewerSession>::Setup>,
        <S as ReviewerSession>::Context,
        ReviewSessionKind,
        Option<<S as ReviewerSession>::Snapshot>,
        CancellationToken,
    ) -> BoxFuture<'static, Result<S, String>>
    + Send
    + Sync;

struct Trunk<S: ReviewerSession> {
    session: Arc<S>,
    review_lock: Arc<Semaphore>,
    cancellation: CancellationToken,
    rollback: AtomicBool,
}

impl<S: ReviewerSession> Drop for Trunk<S> {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

impl<S: ReviewerSession> ReviewerPool<S> {
    /// Installs the reviewer startup function once. It must finish or clean up partial startup
    /// even when the caller drops its future, and preserve the supplied cancellation token.
    pub fn new(
        runtime: Arc<ReviewerTasks>,
        spawn: impl Fn(
                Arc<S::Setup>,
                S::Context,
                ReviewSessionKind,
                Option<S::Snapshot>,
                CancellationToken,
            ) -> BoxFuture<'static, Result<S, String>>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        Self {
            trunk: Mutex::new(None),
            runtime,
            spawn: Box::new(spawn),
        }
    }
}

impl<S: ReviewerSession> ReviewerPool<S> {
    /// Returns the current reviewer handle for host inspection and feedback collection.
    pub async fn trunk(&self) -> Option<Arc<S>> {
        self.trunk
            .lock()
            .await
            .as_ref()
            .map(|trunk| Arc::clone(&trunk.session))
    }

    /// Prepares the first reviewer without replacing a review that won the startup race.
    pub async fn prewarm(&self, setup: Arc<S::Setup>, context: S::Context) -> Result<(), String> {
        let cancellation = self.runtime.cancellation.child_token();
        let guard = cancellation.clone().drop_guard();
        let session = (self.spawn)(
            setup,
            context,
            ReviewSessionKind::TrunkNew,
            /*snapshot*/ None,
            cancellation.clone(),
        )
        .await?;
        let mut trunk = self.trunk.lock().await;
        if !cancellation.is_cancelled() && trunk.is_none() {
            *trunk = Some(Arc::new(Trunk {
                session: Arc::new(session),
                review_lock: Arc::new(Semaphore::new(/*permits*/ 1)),
                cancellation: guard.disarm(),
                rollback: AtomicBool::new(/*v*/ false),
            }));
        }
        Ok(())
    }

    /// Permanently stops this parent's reviewer pool and waits for tracked runtimes.
    pub async fn shutdown(&self) {
        self.runtime.cancellation.cancel();
        self.trunk.lock().await.take();
        self.runtime.tasks.close();
        self.runtime.tasks.wait().await;
    }

    /// Selects one reviewer; busy or incompatible trunks use an isolated temporary session.
    #[allow(
        clippy::await_holding_invalid_type,
        reason = "reviewer selection and spawning stay serialized"
    )]
    pub async fn review<R>(&self, request: R) -> (ReviewSessionOutcome, ReviewAnalytics)
    where
        R: ReviewerRequest<Session = S>,
    {
        let requires_fresh_session = request.requires_fresh_session();
        let mut spawned_trunk = false;
        let (trunk, context, reserved_permit) = match run_before_review_deadline(
            request.deadline(),
            request.cancellation(),
            self.trunk.lock(),
        )
        .await
        {
            Ok(mut state) => {
                let mut snapshot = None;
                let context = request.context(state.as_ref().map(|trunk| trunk.session.as_ref()));
                // Claim cached reviewers before validating them: retirement cancels
                // the session before releasing its review permit.
                let mut reserved_permit = state
                    .as_ref()
                    .and_then(|trunk| Arc::clone(&trunk.review_lock).try_acquire_owned().ok());
                let retire_idle = state.as_ref().is_some_and(|trunk| {
                    requires_fresh_session
                        || trunk.cancellation.is_cancelled()
                        || trunk.session.context() != &context
                        || trunk.rollback.load(Ordering::Acquire)
                }) && reserved_permit.is_some();
                if retire_idle {
                    let trunk = state.as_ref().expect("retiring a cached reviewer");
                    if !requires_fresh_session
                        && !trunk.cancellation.is_cancelled()
                        && trunk.session.context() == &context
                        && trunk.rollback.load(Ordering::Acquire)
                    {
                        snapshot = trunk.session.snapshot().await;
                    }
                    state.take();
                    drop(reserved_permit.take());
                }
                if state.is_none() {
                    let cancellation = self.runtime.cancellation.child_token();
                    let lifetime = cancellation.clone().drop_guard();
                    let session = match run_before_review_deadline(
                        request.deadline(),
                        request.cancellation(),
                        (self.spawn)(
                            request.setup(),
                            context.clone(),
                            ReviewSessionKind::TrunkNew,
                            snapshot,
                            cancellation.clone(),
                        ),
                    )
                    .await
                    {
                        Ok(Ok(session)) => Arc::new(session),
                        Ok(Err(error)) => {
                            return (
                                ReviewSessionOutcome::PromptBuildFailed(error),
                                ReviewAnalytics::without_session(),
                            );
                        }
                        Err(outcome) => {
                            return (outcome, ReviewAnalytics::without_session());
                        }
                    };
                    let review_lock = Arc::new(Semaphore::new(/*permits*/ 1));
                    // Reserve the first review before other requests can see this session.
                    reserved_permit = match Arc::clone(&review_lock).try_acquire_owned() {
                        Ok(permit) => Some(permit),
                        Err(error) => {
                            return (
                                ReviewSessionOutcome::PromptBuildFailed(error.to_string()),
                                ReviewAnalytics::without_session(),
                            );
                        }
                    };
                    *state = Some(Arc::new(Trunk {
                        session,
                        review_lock,
                        cancellation: lifetime.disarm(),
                        rollback: AtomicBool::new(/*v*/ false),
                    }));
                    spawned_trunk = true;
                }
                (state.as_ref().cloned(), context, reserved_permit)
            }
            Err(outcome) => return (outcome, ReviewAnalytics::without_session()),
        };
        let Some(trunk) = trunk else {
            return (
                ReviewSessionOutcome::Completed(Err(
                    "Approval review session was not available after spawn".to_string(),
                )),
                ReviewAnalytics::without_session(),
            );
        };
        if trunk.session.context() != &context || (requires_fresh_session && !spawned_trunk) {
            return Box::pin(self.review_ephemeral(&request, context, /*snapshot*/ None)).await;
        }
        let guard = match reserved_permit {
            Some(guard) => guard,
            None => {
                return Box::pin(self.review_ephemeral(
                    &request,
                    context,
                    trunk.session.snapshot().await,
                ))
                .await;
            }
        };
        let kind = if spawned_trunk {
            ReviewSessionKind::TrunkNew
        } else {
            ReviewSessionKind::TrunkReused
        };
        // Dropping a review before it drains its turn must not leave a reusable agent.
        let review_lifetime = trunk.cancellation.clone().drop_guard();
        let ReviewSessionResult {
            outcome,
            disposition,
            analytics,
        } = request.run(&trunk.session, kind).await;
        if disposition == SessionDisposition::Rollback {
            // Publish retirement before releasing the permit. Concurrent reviews only
            // fork the previously committed snapshot, never this invalidated attempt.
            trunk.rollback.store(true, Ordering::Release);
        }
        if disposition != SessionDisposition::Discard {
            review_lifetime.disarm();
        } else {
            drop(review_lifetime);
        }
        drop(guard);
        if disposition == SessionDisposition::Discard {
            let mut state = self.trunk.lock().await;
            if state
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &trunk))
            {
                state.take();
            }
        }
        (outcome, analytics)
    }

    async fn review_ephemeral<R>(
        &self,
        request: &R,
        context: S::Context,
        snapshot: Option<S::Snapshot>,
    ) -> (ReviewSessionOutcome, ReviewAnalytics)
    where
        R: ReviewerRequest<Session = S>,
    {
        let cancellation = self.runtime.cancellation.child_token();
        let _lifetime = cancellation.clone().drop_guard();
        let session = match run_before_review_deadline(
            request.deadline(),
            request.cancellation(),
            (self.spawn)(
                request.setup(),
                context,
                ReviewSessionKind::EphemeralForked,
                snapshot,
                cancellation.clone(),
            ),
        )
        .await
        {
            Ok(Ok(session)) => Arc::new(session),
            Ok(Err(error)) => {
                return (
                    ReviewSessionOutcome::PromptBuildFailed(error),
                    ReviewAnalytics::without_session(),
                );
            }
            Err(outcome) => return (outcome, ReviewAnalytics::without_session()),
        };
        let ReviewSessionResult {
            outcome, analytics, ..
        } = request
            .run(&session, ReviewSessionKind::EphemeralForked)
            .await;
        (outcome, analytics)
    }
}

#[cfg(test)]
#[path = "pool_tests.rs"]
mod tests;
