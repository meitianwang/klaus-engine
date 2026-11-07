use super::*;
use crate::schemas::rollout::EventItem;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use tokio::sync::Semaphore;

#[derive(Debug)]
struct HeldRollout {
    capacity: Semaphore,
    records: Mutex<Vec<RolloutItem>>,
    closes: AtomicUsize,
    panic_commit: AtomicBool,
    hold_flush: AtomicBool,
    flush_entered: Semaphore,
    flush_release: Semaphore,
}

impl HeldRollout {
    fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            capacity: Semaphore::new(capacity),
            records: Mutex::new(Vec::new()),
            closes: AtomicUsize::new(0),
            panic_commit: AtomicBool::new(false),
            hold_flush: AtomicBool::new(false),
            flush_entered: Semaphore::new(0),
            flush_release: Semaphore::new(0),
        })
    }

    fn names(&self) -> Vec<RuntimeString> {
        self.records
            .lock()
            .unwrap()
            .iter()
            .map(|record| match record {
                RolloutItem::Event(event) => event.event.clone(),
                _ => panic!("unexpected test record"),
            })
            .collect()
    }
}

fn event(name: &str) -> RolloutItem {
    RolloutItem::Event(EventItem {
        event: name.into(),
        turn_id: None,
        payload: None,
    })
}

#[async_trait]
impl LiveRollout for HeldRollout {
    async fn prepare_batch(&self, maximum_records: usize) -> io::Result<PreparedRolloutBatch<'_>> {
        self.capacity.acquire().await.unwrap().forget();
        Ok(PreparedRolloutBatch::new(move |records| {
            assert!(records.len() <= maximum_records);
            assert!(
                !self.panic_commit.load(Ordering::Acquire),
                "injected commit panic"
            );
            self.records.lock().unwrap().extend(records);
        }))
    }
    async fn append_message(&self, _: Message) {
        unreachable!()
    }
    async fn append_turn_context(&self, _: TurnContextSnapshot) {
        unreachable!()
    }
    async fn append_compaction(&self, _: CompactionRecord) {
        unreachable!()
    }
    async fn append_event(&self, name: RuntimeString, _: Option<AppendEventOptions>) {
        self.prepare_append()
            .await
            .unwrap()
            .commit(event(&String::from_utf8(name.to_node_utf8()).unwrap()));
    }
    async fn append_rollback(&self, _: RollbackRecord) {
        unreachable!()
    }
    async fn append_checkpoint(&self, _: CheckpointRecord) {
        unreachable!()
    }
    async fn append_usage(&self, _: UsageLedgerItem) {
        unreachable!()
    }
    async fn append_subagent_lifecycle(&self, _: SubagentLifecycleRecord) {
        unreachable!()
    }
    async fn flush(&self) -> io::Result<()> {
        if self.hold_flush.load(Ordering::Acquire) {
            self.flush_entered.add_permits(1);
            self.flush_release.acquire().await.unwrap().forget();
        }
        Ok(())
    }
    async fn shutdown(&self) -> io::Result<()> {
        self.closes.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}

#[tokio::test]
async fn a_held_flush_allows_a_new_prefix_but_still_fences_raw_shutdown() {
    let raw = HeldRollout::new(1);
    let rollout = SessionRollout::new(raw.clone());
    raw.hold_flush.store(true, Ordering::Release);
    let owner = {
        let rollout = rollout.clone();
        tokio::spawn(async move { rollout.flush().await })
    };
    raw.flush_entered.acquire().await.unwrap().forget();
    // Dropping the receipt waiter must not abandon its already accepted owner.
    drop(owner);
    rollout
        .register_prefix(vec![event("next-root")])
        .unwrap()
        .start()
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), rollout.wait_prefixes())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(raw.names(), [RuntimeString::from("next-root")]);
    let mut close = Box::pin(rollout.shutdown());
    assert!(futures::poll!(close.as_mut()).is_pending());
    assert_eq!(raw.closes.load(Ordering::Acquire), 0);
    raw.flush_release.add_permits(1);
    close.await.unwrap();
    assert_eq!(raw.closes.load(Ordering::Acquire), 1);
}

#[test]
fn immediate_prefixes_preserve_synchronous_construction_without_a_runtime() {
    let raw = HeldRollout::new(1);
    let rollout = SessionRollout::new(raw.clone());
    rollout
        .register_prefix(vec![event("recovery")])
        .unwrap()
        .start()
        .unwrap();
    assert_eq!(raw.names(), [RuntimeString::from("recovery")]);
    assert!(futures::executor::block_on(rollout.wait_prefixes()).is_ok());
}

#[tokio::test]
async fn a_prepared_ordinary_write_cannot_be_overtaken_by_a_new_prefix() {
    let raw = HeldRollout::new(1);
    let rollout = SessionRollout::new(raw.clone());
    let ordinary = rollout.prepare_append().await.unwrap();
    rollout
        .register_prefix(vec![event("turn")])
        .unwrap()
        .start()
        .unwrap();
    raw.capacity.add_permits(1);
    tokio::task::yield_now().await;
    assert!(raw.names().is_empty());
    ordinary.commit(event("ordinary"));
    rollout.flush().await.unwrap();
    assert_eq!(
        raw.names(),
        [RuntimeString::from("ordinary"), RuntimeString::from("turn")]
    );
}

#[tokio::test]
async fn cancelling_a_capacity_wait_releases_its_generation_for_the_next_prefix() {
    let raw = HeldRollout::new(0);
    let rollout = SessionRollout::new(raw.clone());
    let mut ordinary = Box::pin(rollout.prepare_append());
    assert!(futures::poll!(ordinary.as_mut()).is_pending());
    rollout
        .register_prefix(vec![event("next")])
        .unwrap()
        .start()
        .unwrap();
    drop(ordinary);
    raw.capacity.add_permits(1);
    rollout.flush().await.unwrap();
    assert_eq!(raw.names(), [RuntimeString::from("next")]);
}

#[tokio::test]
async fn an_abandoned_startup_owner_fails_waiters_but_still_releases_the_raw_writer() {
    let raw = HeldRollout::new(1);
    let rollout = SessionRollout::new(raw.clone());
    let startup = rollout.register_prefix(vec![event("startup")]).unwrap();
    let mut ordinary = Box::pin(rollout.prepare_append());
    assert!(futures::poll!(ordinary.as_mut()).is_pending());
    drop(startup);
    assert!(ordinary.await.is_err());
    assert!(rollout.flush().await.is_err());
    assert!(rollout.shutdown().await.is_err());
    assert_eq!(raw.closes.load(Ordering::Acquire), 1);
    assert!(raw.names().is_empty());
}

#[tokio::test]
async fn cancelling_a_close_waiter_does_not_abandon_prefix_drain_or_raw_close() {
    let raw = HeldRollout::new(0);
    let rollout = SessionRollout::new(raw.clone());
    rollout
        .register_prefix(vec![event("accepted")])
        .unwrap()
        .start()
        .unwrap();
    let mut close = Box::pin(rollout.shutdown());
    assert!(futures::poll!(close.as_mut()).is_pending());
    drop(close);
    assert_eq!(raw.closes.load(Ordering::Acquire), 0);
    assert!(rollout.register_prefix(vec![event("late")]).is_err());
    raw.capacity.add_permits(1);
    rollout.shutdown().await.unwrap();
    assert_eq!(raw.names(), [RuntimeString::from("accepted")]);
    assert_eq!(raw.closes.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn prefix_commit_panics_fail_the_receipt_and_do_not_leave_an_unjoinable_tail() {
    let raw = HeldRollout::new(1);
    raw.panic_commit.store(true, Ordering::Release);
    let rollout = SessionRollout::new(raw.clone());
    rollout
        .register_prefix(vec![event("panics")])
        .unwrap()
        .start()
        .unwrap();
    assert!(rollout.wait_prefixes().await.is_err());
    assert!(rollout.prepare_append().await.is_err());
    assert!(rollout.shutdown().await.is_err());
    assert_eq!(raw.closes.load(Ordering::Acquire), 1);
}
