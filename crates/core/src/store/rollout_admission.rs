//! Ordered capacity acquisition for a rollout's single bounded writer.

use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, OwnedMutexGuard};

#[derive(Debug, Default)]
pub(crate) struct OrderedRolloutAdmission {
    lane: Arc<Mutex<()>>,
}

impl OrderedRolloutAdmission {
    pub(crate) async fn lock(&self) -> OwnedMutexGuard<()> {
        self.lane.clone().lock_owned().await
    }

    /// Read the authoritative sender only after joining the ordered admission
    /// lane. A shutdown that acquired the lane first can close admission before
    /// this caller obtains capacity or changes any live state.
    pub(crate) async fn reserve_many<T: Send>(
        &self,
        count: usize,
        sender: impl FnOnce() -> Option<mpsc::Sender<T>>,
    ) -> Option<PreparedRolloutSend<T>> {
        let lane = self.lock().await;
        let sender = sender()?;
        assert!(count > 0 && count <= sender.max_capacity());
        let mut permits = Vec::with_capacity(count);
        for _ in 0..count {
            permits.push(sender.clone().reserve_owned().await.ok()?);
        }
        Some(PreparedRolloutSend { permits, lane })
    }
}

pub(crate) struct PreparedRolloutSend<T> {
    permits: Vec<mpsc::OwnedPermit<T>>,
    lane: OwnedMutexGuard<()>,
}

impl<T> PreparedRolloutSend<T> {
    /// There is no suspension point between the owner's state mutation and this
    /// send. The next record or barrier enters only after this commit releases
    /// the lane. Dropping this value instead sends nothing.
    pub(crate) fn send_many(self, items: Vec<T>) {
        let Self { permits, lane } = self;
        assert!(items.len() <= permits.len());
        for (permit, item) in permits.into_iter().zip(items) {
            permit.send(item);
        }
        drop(lane);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Mutex as StdMutex,
    };
    use std::time::Duration;

    #[tokio::test]
    async fn a_paused_writer_limits_admitted_records_and_blocks_the_next_producer() {
        let (tx, mut rx) = mpsc::channel(256);
        let admission = Arc::new(OrderedRolloutAdmission::default());
        for value in 0..256 {
            admission
                .reserve_many(1, || Some(tx.clone()))
                .await
                .unwrap()
                .send_many(vec![value]);
        }
        let accepted = Arc::new(AtomicBool::new(false));
        let worker = tokio::spawn({
            let admission = admission.clone();
            let tx = tx.clone();
            let accepted = accepted.clone();
            async move {
                let permit = admission.reserve_many(1, || Some(tx)).await.unwrap();
                accepted.store(true, Ordering::SeqCst);
                permit.send_many(vec![256]);
            }
        });
        tokio::task::yield_now().await;
        assert!(!accepted.load(Ordering::SeqCst));
        assert_eq!(rx.len(), 256);
        assert_eq!(rx.recv().await, Some(0));
        worker.await.unwrap();
        for expected in 1..=256 {
            assert_eq!(rx.recv().await, Some(expected));
        }
    }

    #[tokio::test]
    async fn cancelling_a_capacity_waiter_does_not_publish_or_stall_a_later_barrier() {
        let (tx, mut rx) = mpsc::channel(1);
        let admission = Arc::new(OrderedRolloutAdmission::default());
        admission
            .reserve_many(1, || Some(tx.clone()))
            .await
            .unwrap()
            .send_many(vec![1]);
        let waiter = tokio::spawn({
            let admission = admission.clone();
            let tx = tx.clone();
            async move {
                admission
                    .reserve_many(1, || Some(tx))
                    .await
                    .unwrap()
                    .send_many(vec![2])
            }
        });
        tokio::task::yield_now().await;
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert_eq!(rx.recv().await, Some(1));
        let barrier = tokio::time::timeout(
            Duration::from_secs(1),
            admission.reserve_many(1, || Some(tx.clone())),
        )
        .await
        .unwrap()
        .unwrap();
        barrier.send_many(vec![3]);
        assert_eq!(rx.recv().await, Some(3));
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn unused_capacity_is_released_without_publishing_a_record() {
        let (tx, mut rx) = mpsc::channel(1);
        let admission = OrderedRolloutAdmission::default();
        let prepared = admission
            .reserve_many(1, || Some(tx.clone()))
            .await
            .unwrap();
        assert_eq!(tx.capacity(), 0);
        drop(prepared);
        assert_eq!(tx.capacity(), 1);
        assert!(rx.try_recv().is_err());
        admission
            .reserve_many(1, || Some(tx))
            .await
            .unwrap()
            .send_many(vec![4]);
        assert_eq!(rx.recv().await, Some(4));
    }

    #[tokio::test]
    async fn a_barrier_cannot_overtake_capacity_already_owned_by_a_mutation() {
        let (tx, mut rx) = mpsc::channel(2);
        let admission = Arc::new(OrderedRolloutAdmission::default());
        let prepared = admission
            .reserve_many(1, || Some(tx.clone()))
            .await
            .unwrap();
        let barrier = tokio::spawn({
            let admission = admission.clone();
            let tx = tx.clone();
            async move {
                admission
                    .reserve_many(1, || Some(tx))
                    .await
                    .unwrap()
                    .send_many(vec!["flush"])
            }
        });
        tokio::task::yield_now().await;
        assert!(rx.try_recv().is_err());
        prepared.send_many(vec!["mutation"]);
        barrier.await.unwrap();
        assert_eq!(rx.recv().await, Some("mutation"));
        assert_eq!(rx.recv().await, Some("flush"));
    }

    #[tokio::test]
    async fn shutdown_rechecks_the_authoritative_sender_before_admission() {
        let (tx, mut rx) = mpsc::channel::<u8>(1);
        let state = Arc::new(StdMutex::new(Some(tx)));
        let admission = Arc::new(OrderedRolloutAdmission::default());
        let shutdown = admission.lock().await;
        let waiter = tokio::spawn({
            let admission = admission.clone();
            let state = state.clone();
            async move {
                admission
                    .reserve_many(1, || state.lock().unwrap().clone())
                    .await
            }
        });
        tokio::task::yield_now().await;
        drop(state.lock().unwrap().take());
        drop(shutdown);
        assert!(waiter.await.unwrap().is_none());
        assert_eq!(rx.recv().await, None);
    }

    #[tokio::test]
    async fn a_failed_writer_wakes_a_blocked_producer_and_releases_the_lane() {
        let (tx, rx) = mpsc::channel(1);
        let admission = Arc::new(OrderedRolloutAdmission::default());
        admission
            .reserve_many(1, || Some(tx.clone()))
            .await
            .unwrap()
            .send_many(vec![1]);
        let waiter = tokio::spawn({
            let admission = admission.clone();
            let tx = tx.clone();
            async move { admission.reserve_many(1, || Some(tx)).await }
        });
        tokio::task::yield_now().await;
        drop(rx);
        assert!(waiter.await.unwrap().is_none());
        assert!(admission.reserve_many(1, || Some(tx)).await.is_none());
        tokio::time::timeout(Duration::from_secs(1), admission.lock())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_multi_record_mutation_commits_before_the_next_barrier() {
        let (tx, mut rx) = mpsc::channel(4);
        let admission = Arc::new(OrderedRolloutAdmission::default());
        let prepared = admission
            .reserve_many(3, || Some(tx.clone()))
            .await
            .unwrap();
        let barrier = tokio::spawn({
            let admission = admission.clone();
            let tx = tx.clone();
            async move {
                admission
                    .reserve_many(1, || Some(tx))
                    .await
                    .unwrap()
                    .send_many(vec!["flush"])
            }
        });
        tokio::task::yield_now().await;
        prepared.send_many(vec!["message", "usage"]);
        barrier.await.unwrap();
        assert_eq!(rx.recv().await, Some("message"));
        assert_eq!(rx.recv().await, Some("usage"));
        assert_eq!(rx.recv().await, Some("flush"));
        assert_eq!(tx.capacity(), 4);
    }

    #[tokio::test]
    async fn cancelling_a_partial_multi_record_reservation_returns_all_capacity() {
        let (tx, mut rx) = mpsc::channel(3);
        let admission = Arc::new(OrderedRolloutAdmission::default());
        admission
            .reserve_many(1, || Some(tx.clone()))
            .await
            .unwrap()
            .send_many(vec![1]);
        let waiter = tokio::spawn({
            let admission = admission.clone();
            let tx = tx.clone();
            async move { admission.reserve_many(3, || Some(tx)).await }
        });
        for _ in 0..10 {
            tokio::task::yield_now().await;
            if tx.capacity() == 0 {
                break;
            }
        }
        assert_eq!(tx.capacity(), 0);
        waiter.abort();
        assert!(waiter.await.is_err());
        assert_eq!(tx.capacity(), 2);
        assert_eq!(rx.recv().await, Some(1));
        admission
            .reserve_many(3, || Some(tx.clone()))
            .await
            .unwrap()
            .send_many(vec![2, 3, 4]);
        for expected in 2..=4 {
            assert_eq!(rx.recv().await, Some(expected));
        }
        assert_eq!(tx.capacity(), 3);
    }
}
