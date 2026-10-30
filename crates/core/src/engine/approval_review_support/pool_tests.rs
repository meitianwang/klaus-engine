// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified for runtime component contracts and deterministic regressions.

use super::*;
use crate::engine::approval_review_support::{ConversationCheckpoint, ConversationState};
use std::sync::atomic::AtomicUsize;
use std::sync::Mutex as StdMutex;

#[derive(Clone)]
struct SpawnRecord {
    id: usize,
    context: u64,
    kind: ReviewSessionKind,
    snapshot_history: Option<Vec<usize>>,
    cancellation: CancellationToken,
}

struct Session {
    id: usize,
    context: u64,
    checkpoint: StdMutex<Option<ConversationCheckpoint<Vec<usize>, u64>>>,
}
impl ReviewerSession for Session {
    type Setup = ();
    type Context = u64;
    type Snapshot = ConversationCheckpoint<Vec<usize>, u64>;
    fn context(&self) -> &u64 {
        &self.context
    }
    async fn snapshot(&self) -> Option<Self::Snapshot> {
        self.checkpoint.lock().unwrap().clone()
    }
}

struct Request {
    context: u64,
    fresh: bool,
    disposition: SessionDisposition,
    selected: Arc<StdMutex<Vec<(usize, ReviewSessionKind)>>>,
    entered: Arc<Semaphore>,
    release: Option<Arc<Semaphore>>,
}
impl ReviewerRequest for Request {
    type Session = Session;
    fn setup(&self) -> Arc<()> {
        Arc::new(())
    }
    fn context(&self, _: Option<&Session>) -> u64 {
        self.context
    }
    fn requires_fresh_session(&self) -> bool {
        self.fresh
    }
    fn deadline(&self) -> Instant {
        Instant::now() + std::time::Duration::from_secs(10)
    }
    fn cancellation(&self) -> Option<&CancellationToken> {
        None
    }
    async fn run(&self, session: &Session, kind: ReviewSessionKind) -> ReviewSessionResult {
        {
            let mut selected = self.selected.lock().unwrap();
            assert!(selected.len() < 16, "the fixture has bounded review work");
            selected.push((session.id, kind));
        }
        self.entered.add_permits(1);
        if let Some(release) = &self.release {
            release.acquire().await.unwrap().forget();
        }
        ReviewSessionResult {
            outcome: ReviewSessionOutcome::Completed(Ok(Some(r#"{"outcome":"allow"}"#.into()))),
            disposition: self.disposition,
            analytics: ReviewAnalytics {
                session_kind: Some(kind),
                ..Default::default()
            },
        }
    }
}

struct Rig {
    pool: Arc<ReviewerPool<Session>>,
    spawns: Arc<StdMutex<Vec<SpawnRecord>>>,
    selected: Arc<StdMutex<Vec<(usize, ReviewSessionKind)>>>,
    entered: Arc<Semaphore>,
    cleaned: Arc<AtomicUsize>,
}
impl Rig {
    fn new() -> Self {
        let runtime = Arc::new(ReviewerTasks::default());
        let tracked = runtime.clone();
        let spawns = Arc::new(StdMutex::new(Vec::new()));
        let recorded = spawns.clone();
        let cleaned = Arc::new(AtomicUsize::new(0));
        let clean_count = cleaned.clone();
        let pool = Arc::new(ReviewerPool::new(
            runtime,
            move |_,
                  context,
                  kind,
                  snapshot: Option<ConversationCheckpoint<Vec<usize>, u64>>,
                  cancellation| {
                let id = {
                    let mut records = recorded.lock().unwrap();
                    assert!(records.len() < 16, "the fixture has bounded runtime work");
                    let id = records.len();
                    records.push(SpawnRecord {
                        id,
                        context,
                        kind,
                        snapshot_history: snapshot
                            .as_ref()
                            .map(|snapshot| snapshot.history().clone()),
                        cancellation: cancellation.clone(),
                    });
                    id
                };
                let clean_count = clean_count.clone();
                let stopped = cancellation.clone();
                tracked.tasks.spawn(async move {
                    stopped.cancelled().await;
                    clean_count.fetch_add(1, Ordering::SeqCst);
                });
                Box::pin(async move {
                    let checkpoint = match snapshot {
                        Some(snapshot) => snapshot,
                        None => {
                            let mut conversation = ConversationState::default();
                            conversation.complete_review(context);
                            conversation.commit_snapshot(vec![id]);
                            conversation.snapshot().unwrap().clone()
                        }
                    };
                    Ok(Session {
                        id,
                        context,
                        checkpoint: StdMutex::new(Some(checkpoint)),
                    })
                })
            },
        ));
        Self {
            pool,
            spawns,
            selected: Arc::new(StdMutex::new(Vec::new())),
            entered: Arc::new(Semaphore::new(0)),
            cleaned,
        }
    }
    fn request(&self, context: u64, disposition: SessionDisposition) -> Request {
        Request {
            context,
            fresh: false,
            disposition,
            selected: self.selected.clone(),
            entered: self.entered.clone(),
            release: None,
        }
    }
    async fn wait_entered(&self) {
        self.entered.acquire().await.unwrap().forget();
    }
    async fn shutdown(&self) {
        self.pool.shutdown().await;
        {
            let records = self.spawns.lock().unwrap();
            assert!(records
                .iter()
                .all(|record| record.cancellation.is_cancelled()));
            assert_eq!(self.cleaned.load(Ordering::SeqCst), records.len());
        }
        assert!(self.pool.trunk().await.is_none());
    }
}

#[tokio::test]
async fn compatible_idle_trunk_reuses_its_runtime_and_prewarm_never_replaces_it() {
    let rig = Rig::new();
    rig.pool.prewarm(Arc::new(()), 1).await.unwrap();
    rig.pool.prewarm(Arc::new(()), 1).await.unwrap();
    assert_eq!(rig.pool.trunk().await.unwrap().id, 0);
    assert!(rig.spawns.lock().unwrap()[1].cancellation.is_cancelled());
    rig.pool
        .review(rig.request(1, SessionDisposition::Reusable))
        .await;
    rig.pool
        .review(rig.request(1, SessionDisposition::Reusable))
        .await;
    assert_eq!(
        *rig.selected.lock().unwrap(),
        vec![
            (0, ReviewSessionKind::TrunkReused),
            (0, ReviewSessionKind::TrunkReused)
        ]
    );
    rig.shutdown().await;
}

#[tokio::test]
async fn busy_review_forks_only_the_committed_compatible_checkpoint() {
    let rig = Rig::new();
    let release = Arc::new(Semaphore::new(0));
    let mut request = rig.request(1, SessionDisposition::Reusable);
    request.release = Some(release.clone());
    let pool = rig.pool.clone();
    let busy = tokio::spawn(async move { pool.review(request).await });
    rig.wait_entered().await;
    rig.pool
        .review(rig.request(1, SessionDisposition::Reusable))
        .await;
    rig.pool
        .review(rig.request(2, SessionDisposition::Reusable))
        .await;
    {
        let spawns = rig.spawns.lock().unwrap();
        assert_eq!(
            (spawns[0].id, spawns[0].kind, spawns[0].context),
            (0, ReviewSessionKind::TrunkNew, 1)
        );
        assert_eq!(spawns[1].kind, ReviewSessionKind::EphemeralForked);
        assert_eq!(spawns[1].snapshot_history, Some(vec![0]));
        assert!(spawns[1].cancellation.is_cancelled());
        assert_eq!(spawns[2].context, 2);
        assert_eq!(spawns[2].snapshot_history, None);
        assert!(!spawns[0].cancellation.is_cancelled());
    }
    release.add_permits(1);
    busy.await.unwrap();
    assert_eq!(rig.pool.trunk().await.unwrap().id, 0);
    rig.shutdown().await;
}

#[tokio::test]
async fn rollback_restarts_from_last_commit_while_discard_starts_without_history() {
    for disposition in [SessionDisposition::Rollback, SessionDisposition::Discard] {
        let rig = Rig::new();
        rig.pool.review(rig.request(1, disposition)).await;
        rig.pool
            .review(rig.request(1, SessionDisposition::Reusable))
            .await;
        {
            let spawns = rig.spawns.lock().unwrap();
            assert!(spawns[0].cancellation.is_cancelled());
            assert_eq!(spawns[1].kind, ReviewSessionKind::TrunkNew);
            assert_eq!(
                spawns[1].snapshot_history,
                if disposition == SessionDisposition::Rollback {
                    Some(vec![0])
                } else {
                    None
                }
            );
        }
        rig.shutdown().await;
    }
}

#[tokio::test]
async fn dropping_an_undrained_review_cancels_its_runtime_before_reuse() {
    let rig = Rig::new();
    let mut request = rig.request(1, SessionDisposition::Reusable);
    request.release = Some(Arc::new(Semaphore::new(0)));
    let pool = rig.pool.clone();
    let active = tokio::spawn(async move { pool.review(request).await });
    rig.wait_entered().await;
    active.abort();
    assert!(active.await.unwrap_err().is_cancelled());
    assert!(rig.spawns.lock().unwrap()[0].cancellation.is_cancelled());
    rig.pool
        .review(rig.request(1, SessionDisposition::Reusable))
        .await;
    assert_eq!(rig.spawns.lock().unwrap()[1].snapshot_history, None);
    rig.shutdown().await;
}

#[tokio::test]
async fn fresh_attempts_replace_idle_trunks_but_do_not_disturb_busy_trunks() {
    for busy in [false, true] {
        let rig = Rig::new();
        rig.pool.prewarm(Arc::new(()), 1).await.unwrap();
        let release = Arc::new(Semaphore::new(0));
        let active = if busy {
            let mut request = rig.request(1, SessionDisposition::Reusable);
            request.release = Some(release.clone());
            let pool = rig.pool.clone();
            let active = tokio::spawn(async move { pool.review(request).await });
            rig.wait_entered().await;
            Some(active)
        } else {
            None
        };
        let mut request = rig.request(1, SessionDisposition::Reusable);
        request.fresh = true;
        rig.pool.review(request).await;
        assert_eq!(rig.spawns.lock().unwrap()[1].snapshot_history, None);
        if let Some(active) = active {
            release.add_permits(1);
            active.await.unwrap();
        }
        assert_eq!(rig.pool.trunk().await.unwrap().id, if busy { 0 } else { 1 });
        rig.shutdown().await;
    }
}

#[tokio::test]
async fn incompatible_idle_context_retires_the_old_trunk_without_inheriting_history() {
    let rig = Rig::new();
    rig.pool
        .review(rig.request(1, SessionDisposition::Reusable))
        .await;
    rig.pool
        .review(rig.request(2, SessionDisposition::Reusable))
        .await;
    {
        let records = rig.spawns.lock().unwrap();
        assert_eq!(records.len(), 2);
        assert!(records[0].cancellation.is_cancelled());
        assert_eq!(records[1].context, 2);
        assert_eq!(records[1].kind, ReviewSessionKind::TrunkNew);
        assert_eq!(records[1].snapshot_history, None);
    }
    rig.shutdown().await;
}
