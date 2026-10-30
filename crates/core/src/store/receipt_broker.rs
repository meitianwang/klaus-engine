//! Bounded, cancellation-safe handoff of already observed provider usage.

use crate::schemas::rollout::{RolloutItem, UsageLedgerItem};
use crate::store::session_store::LiveRollout;
use crate::sync_ext::MutexExt;
use crate::types::abort::AbortSignal;
use std::collections::VecDeque;
use std::io;
use std::sync::{Arc, Mutex};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

const RECEIPT_CAPACITY: usize = 256;

#[derive(Debug, Clone)]
struct Failure {
    kind: io::ErrorKind,
    message: String,
}

impl Failure {
    fn error(&self) -> io::Error {
        io::Error::new(self.kind, self.message.clone())
    }
}

struct CommittedReceipt {
    sequence: u64,
    record: UsageLedgerItem,
    // Capacity remains owned until the raw writer admits this record.
    _capacity: OwnedSemaphorePermit,
}

#[derive(Default)]
struct State {
    closed: bool,
    outstanding: usize,
    committed: u64,
    admitted: u64,
    processed: u64,
    queue: VecDeque<CommittedReceipt>,
    // Failed records remain owned and bounded, and every fence reports failure.
    failed: Vec<CommittedReceipt>,
    error: Option<Failure>,
    worker_finished: bool,
}

struct Shared {
    raw: Arc<dyn LiveRollout>,
    capacity: Arc<Semaphore>,
    state: Mutex<State>,
    changed: Notify,
    closed: AbortSignal,
}

/// One broker belongs to one session and retains a single lazy worker. Its raw
/// sink must bypass any session wrapper that fences receipts before appending.
pub(crate) struct ReceiptBroker {
    shared: Arc<Shared>,
    worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl std::fmt::Debug for ReceiptBroker {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.shared.state.lock_recover();
        formatter
            .debug_struct("ReceiptBroker")
            .field("closed", &state.closed)
            .field("outstanding", &state.outstanding)
            .field("committed", &state.committed)
            .field("admitted", &state.admitted)
            .finish()
    }
}

impl ReceiptBroker {
    /// Construction is safe outside a Tokio runtime. The first reservation
    /// starts the worker; creating a synchronous session never spawns a task.
    pub(crate) fn new(raw: Arc<dyn LiveRollout>) -> Arc<Self> {
        Arc::new(Self {
            shared: Arc::new(Shared {
                raw,
                capacity: Arc::new(Semaphore::new(RECEIPT_CAPACITY)),
                state: Mutex::new(State::default()),
                changed: Notify::new(),
                closed: AbortSignal::new(),
            }),
            worker: Mutex::new(None),
        })
    }

    fn start(&self) -> io::Result<()> {
        let mut worker = self.worker.lock_recover();
        if worker.is_none() {
            let runtime = tokio::runtime::Handle::try_current().map_err(|_| {
                io::Error::other("usage receipt persistence requires an async runtime")
            })?;
            let exit = WorkerExit(self.shared.clone());
            *worker = Some(runtime.spawn(run_worker(self.shared.clone(), exit)));
        }
        Ok(())
    }

    /// Reserve before invoking the physical provider, without holding any raw
    /// writer capacity or ordered admission lane across that request.
    pub(crate) async fn reserve(self: &Arc<Self>, signal: &AbortSignal) -> io::Result<ReceiptSlot> {
        if signal.is_cancelled() {
            return Err(cancelled());
        }
        {
            let state = self.shared.state.lock_recover();
            check_admission(&state)?;
        }
        self.start()?;
        let capacity = tokio::select! {
            biased;
            _ = signal.cancelled() => return Err(cancelled()),
            _ = self.shared.closed.cancelled() => return Err(self.admission_error()),
            capacity = self.shared.capacity.clone().acquire_owned() => {
                capacity.map_err(|_| self.admission_error())?
            }
        };
        let mut state = self.shared.state.lock_recover();
        check_admission(&state)?;
        if signal.is_cancelled() {
            return Err(cancelled());
        }
        state.outstanding += 1;
        Ok(ReceiptSlot {
            shared: self.shared.clone(),
            capacity: Some(capacity),
        })
    }

    fn admission_error(&self) -> io::Error {
        let state = self.shared.state.lock_recover();
        state
            .error
            .as_ref()
            .map(Failure::error)
            .unwrap_or_else(closed)
    }

    /// Fence only receipts committed before this atomic snapshot. Outstanding
    /// unobserved requests do not participate, even if they own every slot.
    /// The caller performs its raw flush or subsequent append after this fence.
    pub(crate) async fn flush_committed(&self) -> io::Result<()> {
        let target = self.shared.state.lock_recover().committed;
        loop {
            let changed = self.shared.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let state = self.shared.state.lock_recover();
                if let Some(error) = &state.error {
                    return Err(error.error());
                }
                if state.admitted >= target {
                    return Ok(());
                }
            }
            changed.await;
        }
    }

    /// Close admission synchronously so capacity waiters cannot launch another
    /// physical request. Existing reservations retain their right to commit.
    pub(crate) fn close_admission(&self) {
        close_shared(&self.shared);
    }

    /// Wait for every accepted request to settle and all committed receipts to
    /// enter the raw writer. Cancelling this wait does not cancel the worker.
    /// The session must defer raw close if it times out waiting here.
    pub(crate) async fn shutdown(&self) -> io::Result<()> {
        self.close_admission();
        loop {
            let changed = self.shared.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let state = self.shared.state.lock_recover();
                if state.outstanding == 0
                    && (state.processed == state.committed || state.worker_finished)
                {
                    return state
                        .error
                        .as_ref()
                        .map_or(Ok(()), |error| Err(error.error()));
                }
            }
            changed.await;
        }
    }
}

impl Drop for ReceiptBroker {
    fn drop(&mut self) {
        // Dropping JoinHandle detaches rather than aborting. The worker owns the
        // raw sink until accepted reservations and committed receipts settle.
        self.close_admission();
    }
}

/// A request owns one slot until its final consumer-observed usage is recorded,
/// or discards it by dropping this value without observing usage.
pub(crate) struct ReceiptSlot {
    shared: Arc<Shared>,
    capacity: Option<OwnedSemaphorePermit>,
}

impl ReceiptSlot {
    /// Accounting and this commit are synchronous, so cancellation cannot split
    /// the memory update from the handoff of its immutable ledger record.
    pub(crate) fn commit(mut self, record: UsageLedgerItem) {
        let capacity = self.capacity.take().expect("receipt slot commits once");
        {
            let mut state = self.shared.state.lock_recover();
            state.committed += 1;
            let sequence = state.committed;
            state.queue.push_back(CommittedReceipt {
                sequence,
                record,
                _capacity: capacity,
            });
            state.outstanding -= 1;
        }
        self.shared.changed.notify_waiters();
    }
}

impl Drop for ReceiptSlot {
    fn drop(&mut self) {
        if self.capacity.is_some() {
            self.shared.state.lock_recover().outstanding -= 1;
            self.shared.changed.notify_waiters();
        }
    }
}

/// Absence means persistence is disabled; closure, failure and cancellation are
/// errors and must never silently launch an unpersistable physical request.
pub(crate) async fn reserve_receipt(
    broker: Option<&Arc<ReceiptBroker>>,
    signal: &AbortSignal,
) -> io::Result<Option<ReceiptSlot>> {
    if signal.is_cancelled() {
        return Err(cancelled());
    }
    match broker {
        Some(broker) => broker.reserve(signal).await.map(Some),
        None => Ok(None),
    }
}

fn check_admission(state: &State) -> io::Result<()> {
    if let Some(error) = &state.error {
        Err(error.error())
    } else if state.closed {
        Err(closed())
    } else {
        Ok(())
    }
}

fn closed() -> io::Error {
    io::Error::new(
        io::ErrorKind::BrokenPipe,
        "usage receipt admission is closed",
    )
}

fn cancelled() -> io::Error {
    io::Error::new(
        io::ErrorKind::Interrupted,
        "usage receipt reservation cancelled",
    )
}

fn close_shared(shared: &Shared) {
    shared.state.lock_recover().closed = true;
    shared.capacity.close();
    shared.closed.cancel();
    shared.changed.notify_waiters();
}

struct WorkerExit(Arc<Shared>);

impl Drop for WorkerExit {
    fn drop(&mut self) {
        {
            let mut state = self.0.state.lock_recover();
            state.worker_finished = true;
            if (state.outstanding != 0 || state.processed != state.committed)
                && state.error.is_none()
            {
                state.error = Some(Failure {
                    kind: io::ErrorKind::BrokenPipe,
                    message: "usage receipt worker stopped before draining accepted receipts"
                        .into(),
                });
            }
        }
        close_shared(&self.0);
    }
}

async fn run_worker(shared: Arc<Shared>, _exit: WorkerExit) {
    loop {
        let changed = shared.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let receipt = {
            let mut state = shared.state.lock_recover();
            match state.queue.pop_front() {
                Some(receipt) => Some(receipt),
                None if state.closed && state.outstanding == 0 => return,
                None => None,
            }
        };
        let Some(receipt) = receipt else {
            changed.await;
            continue;
        };
        let failure = shared.state.lock_recover().error.clone();
        let result = match failure {
            Some(error) => Err(error.error()),
            None => match shared.raw.prepare_append().await {
                Ok(prepared) => {
                    prepared.commit(RolloutItem::UsageLedger(receipt.record.clone()));
                    Ok(())
                }
                Err(error) => {
                    tracing::warn!(%error, "Usage receipt persistence failed");
                    Err(error)
                }
            },
        };
        let failed = result.is_err();
        {
            let mut state = shared.state.lock_recover();
            state.processed = receipt.sequence;
            match result {
                Ok(()) => state.admitted = receipt.sequence,
                Err(error) => {
                    state.error.get_or_insert_with(|| Failure {
                        kind: error.kind(),
                        message: error.to_string(),
                    });
                    state.failed.push(receipt);
                }
            }
        }
        if failed {
            close_shared(&shared);
        }
        shared.changed.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::message::{Message, Usage};
    use crate::schemas::rollout::TurnContextSnapshot;
    use crate::schemas::subagent_lifecycle::SubagentLifecycleRecord;
    use crate::store::session_store::{
        AppendEventOptions, CheckpointRecord, CompactionRecord, PreparedRolloutBatch,
        RollbackRecord,
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    #[derive(Debug)]
    struct RawRollout {
        permits: Semaphore,
        fail: AtomicBool,
        entered: AtomicUsize,
        flushes: AtomicUsize,
        items: Mutex<Vec<RolloutItem>>,
    }

    impl RawRollout {
        fn paused() -> Arc<Self> {
            Arc::new(Self {
                permits: Semaphore::new(0),
                fail: AtomicBool::new(false),
                entered: AtomicUsize::new(0),
                flushes: AtomicUsize::new(0),
                items: Mutex::new(Vec::new()),
            })
        }

        fn allow(&self, count: usize) {
            self.permits.add_permits(count);
        }
    }

    #[async_trait::async_trait]
    impl LiveRollout for RawRollout {
        async fn prepare_batch(&self, _: usize) -> io::Result<PreparedRolloutBatch<'_>> {
            self.entered.fetch_add(1, Ordering::SeqCst);
            self.permits.acquire().await.unwrap().forget();
            if self.fail.load(Ordering::SeqCst) {
                return Err(io::Error::other("raw writer failed"));
            }
            Ok(PreparedRolloutBatch::new(move |items| {
                self.items.lock_recover().extend(items);
            }))
        }
        async fn append_message(&self, _: Message) {
            unreachable!("broker must use raw prepared admission")
        }
        async fn append_turn_context(&self, _: TurnContextSnapshot) {
            unreachable!("broker must use raw prepared admission")
        }
        async fn append_compaction(&self, _: CompactionRecord) {
            unreachable!("broker must use raw prepared admission")
        }
        async fn append_event(
            &self,
            _: crate::runtime_values::RuntimeString,
            _: Option<AppendEventOptions>,
        ) {
            unreachable!("broker must use raw prepared admission")
        }
        async fn append_rollback(&self, _: RollbackRecord) {
            unreachable!("broker must use raw prepared admission")
        }
        async fn append_checkpoint(&self, _: CheckpointRecord) {
            unreachable!("broker must use raw prepared admission")
        }
        async fn append_usage(&self, _: UsageLedgerItem) {
            unreachable!("broker must use raw prepared admission")
        }
        async fn append_subagent_lifecycle(&self, _: SubagentLifecycleRecord) {
            unreachable!("broker must use raw prepared admission")
        }
        async fn flush(&self) -> io::Result<()> {
            self.flushes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn shutdown(&self) -> io::Result<()> {
            unreachable!("only the session closes the raw writer")
        }
    }

    fn record(id: &str) -> UsageLedgerItem {
        UsageLedgerItem {
            request_id: id.into(),
            assistant_message_id: id.into(),
            model: "captured-model".into(),
            usage: Usage {
                input_tokens: 12,
                output_tokens: 2,
                cache_read_input_tokens: 3,
                cache_creation_input_tokens: 1,
                total_tokens: Some(18),
            },
            cost_usd: 0.0002,
            inference: None,
        }
    }

    async fn drain(broker: &ReceiptBroker) -> io::Result<()> {
        tokio::time::timeout(Duration::from_secs(1), broker.shutdown())
            .await
            .expect("receipt shutdown must settle")
    }

    #[test]
    fn construction_and_unused_drop_do_not_require_an_async_runtime() {
        let broker = ReceiptBroker::new(RawRollout::paused());
        assert!(broker.worker.lock_recover().is_none());
        assert!(futures::executor::block_on(broker.flush_committed()).is_ok());
        assert!(futures::executor::block_on(broker.shutdown()).is_ok());
    }

    #[tokio::test]
    async fn a_paused_raw_writer_bounds_all_reserved_and_committed_receipts() {
        let raw = RawRollout::paused();
        let broker = ReceiptBroker::new(raw.clone());
        let signal = AbortSignal::new();
        for index in 0..RECEIPT_CAPACITY {
            broker
                .reserve(&signal)
                .await
                .unwrap()
                .commit(record(&index.to_string()));
        }
        tokio::task::yield_now().await;
        assert_eq!(raw.entered.load(Ordering::SeqCst), 1);
        assert_eq!(broker.shared.capacity.available_permits(), 0);
        let waiting = broker.reserve(&signal);
        tokio::pin!(waiting);
        assert!(futures::poll!(&mut waiting).is_pending());
        raw.allow(1);
        let next = tokio::time::timeout(Duration::from_secs(1), &mut waiting)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(raw.items.lock_recover().len(), 1);
        drop(next);
        raw.allow(RECEIPT_CAPACITY - 1);
        drain(&broker).await.unwrap();
        let items = raw.items.lock_recover();
        assert_eq!(items.len(), RECEIPT_CAPACITY);
        for (index, item) in items.iter().enumerate() {
            assert_eq!(item, &RolloutItem::UsageLedger(record(&index.to_string())));
        }
        assert_eq!(raw.flushes.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn flush_ignores_every_slot_reserved_by_unobserved_requests() {
        let raw = RawRollout::paused();
        let broker = ReceiptBroker::new(raw.clone());
        let signal = AbortSignal::new();
        let mut slots = Vec::new();
        for _ in 0..RECEIPT_CAPACITY {
            slots.push(broker.reserve(&signal).await.unwrap());
        }
        tokio::time::timeout(Duration::from_secs(1), broker.flush_committed())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(raw.entered.load(Ordering::SeqCst), 0);
        assert_eq!(raw.flushes.load(Ordering::SeqCst), 0);
        drop(slots);
        drain(&broker).await.unwrap();
        assert!(raw.items.lock_recover().is_empty());
    }

    #[tokio::test]
    async fn cancelling_capacity_waits_and_discarding_slots_publish_no_record() {
        let raw = RawRollout::paused();
        let broker = ReceiptBroker::new(raw.clone());
        let signal = AbortSignal::new();
        let mut slots = Vec::new();
        for _ in 0..RECEIPT_CAPACITY {
            slots.push(broker.reserve(&signal).await.unwrap());
        }
        let cancelled_signal = AbortSignal::new();
        let wait = broker.reserve(&cancelled_signal);
        tokio::pin!(wait);
        assert!(futures::poll!(&mut wait).is_pending());
        cancelled_signal.cancel();
        assert_eq!(wait.await.err().unwrap().kind(), io::ErrorKind::Interrupted);
        {
            let abandoned = broker.reserve(&signal);
            tokio::pin!(abandoned);
            assert!(futures::poll!(&mut abandoned).is_pending());
        }
        drop(slots.pop());
        drop(broker.reserve(&signal).await.unwrap());
        drop(slots);
        drain(&broker).await.unwrap();
        assert_eq!(broker.shared.capacity.available_permits(), RECEIPT_CAPACITY);
        assert!(raw.items.lock_recover().is_empty());
    }

    #[tokio::test]
    async fn a_flush_snapshot_fences_commits_but_not_later_observations() {
        let raw = RawRollout::paused();
        let broker = ReceiptBroker::new(raw.clone());
        let signal = AbortSignal::new();
        let older_request = broker.reserve(&signal).await.unwrap();
        broker
            .reserve(&signal)
            .await
            .unwrap()
            .commit(record("first-committed"));
        let flush = broker.flush_committed();
        tokio::pin!(flush);
        assert!(futures::poll!(&mut flush).is_pending());
        older_request.commit(record("later-observed"));
        raw.allow(1);
        tokio::time::timeout(Duration::from_secs(1), &mut flush)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            raw.items.lock_recover().as_slice(),
            &[RolloutItem::UsageLedger(record("first-committed"))]
        );
        let later_flush = broker.flush_committed();
        tokio::pin!(later_flush);
        assert!(futures::poll!(&mut later_flush).is_pending());
        raw.allow(1);
        later_flush.await.unwrap();
        drain(&broker).await.unwrap();
        assert_eq!(raw.items.lock_recover().len(), 2);
    }

    #[tokio::test]
    async fn shutdown_waits_for_accepted_slots_and_survives_a_cancelled_waiter() {
        let raw = RawRollout::paused();
        let broker = ReceiptBroker::new(raw.clone());
        let signal = AbortSignal::new();
        let slot = broker.reserve(&signal).await.unwrap();
        {
            let shutdown = broker.shutdown();
            tokio::pin!(shutdown);
            assert!(futures::poll!(&mut shutdown).is_pending());
        }
        assert_eq!(
            broker.reserve(&signal).await.err().unwrap().kind(),
            io::ErrorKind::BrokenPipe
        );
        slot.commit(record("accepted-before-close"));
        let flush = broker.flush_committed();
        tokio::pin!(flush);
        assert!(futures::poll!(&mut flush).is_pending());
        raw.allow(1);
        drain(&broker).await.unwrap();
        flush.await.unwrap();
        assert_eq!(
            raw.items.lock_recover().as_slice(),
            &[RolloutItem::UsageLedger(record("accepted-before-close"))]
        );
    }

    #[tokio::test]
    async fn broker_drop_retains_the_worker_until_accepted_usage_is_admitted() {
        let raw = RawRollout::paused();
        let broker = ReceiptBroker::new(raw.clone());
        let shared = broker.shared.clone();
        let slot = broker.reserve(&AbortSignal::new()).await.unwrap();
        drop(broker);
        slot.commit(record("late-drop"));
        raw.allow(1);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let changed = shared.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if shared.state.lock_recover().worker_finished {
                    break;
                }
                changed.await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            raw.items.lock_recover().as_slice(),
            &[RolloutItem::UsageLedger(record("late-drop"))]
        );
    }

    #[tokio::test]
    async fn raw_failure_wakes_waiters_and_shutdown_retains_failed_records() {
        let raw = RawRollout::paused();
        let broker = ReceiptBroker::new(raw.clone());
        let signal = AbortSignal::new();
        let accepted = broker.reserve(&signal).await.unwrap();
        for _ in 1..RECEIPT_CAPACITY {
            broker
                .reserve(&signal)
                .await
                .unwrap()
                .commit(record("before-error"));
        }
        let waiter = broker.reserve(&signal);
        tokio::pin!(waiter);
        assert!(futures::poll!(&mut waiter).is_pending());
        raw.fail.store(true, Ordering::SeqCst);
        raw.allow(1);
        assert_eq!(waiter.await.err().unwrap().to_string(), "raw writer failed");
        assert_eq!(
            broker.flush_committed().await.unwrap_err().to_string(),
            "raw writer failed"
        );
        let shutdown = broker.shutdown();
        tokio::pin!(shutdown);
        assert!(futures::poll!(&mut shutdown).is_pending());
        accepted.commit(record("after-error"));
        assert_eq!(
            drain(&broker).await.unwrap_err().to_string(),
            "raw writer failed"
        );
        let state = broker.shared.state.lock_recover();
        assert_eq!(state.outstanding, 0);
        assert_eq!(state.processed, state.committed);
        assert_eq!(state.failed.len(), RECEIPT_CAPACITY);
        assert_eq!(
            state.failed.last().unwrap().record.request_id,
            "after-error"
        );
    }
}
