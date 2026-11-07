#![cfg(target_os = "macos")]

#[allow(dead_code)]
#[path = "../src/runtime_process/macos_notifier.rs"]
mod macos_notifier;

use std::collections::VecDeque;
use std::sync::{Arc, Barrier, Mutex, PoisonError};

use macos_notifier::{
    HiddenLifecycleHandle, HiddenLifecyclePhase, HiddenRegistrationIdentity, KqueueDeleteOutcome,
    KqueueDriver, KqueueDriverError, KqueueDriverFactory, KqueueEvent, KqueueEventDisposition,
    KqueueExitProof, KqueueInstallOutcome, KqueueInstallRequest, KqueueWatchId, MacOsNotifier,
    MacOsNotifierInitializationError, MacOsProcessIdentity, ProvenExitCollector,
    SharedRetainedHiddenScheduler, WatchTableLockProbe,
};

#[derive(Default)]
struct FakeDriverState {
    install_outcomes: VecDeque<Result<KqueueInstallOutcome, KqueueDriverError>>,
    delete_outcomes: VecDeque<KqueueDeleteOutcome>,
    installs: Vec<KqueueInstallRequest>,
    deletes: Vec<(MacOsProcessIdentity, KqueueWatchId)>,
    next_delete_barrier: Option<DeleteBarrier>,
}

#[derive(Clone)]
struct DeleteBarrier {
    entered: Arc<Barrier>,
    release: Arc<Barrier>,
}

impl DeleteBarrier {
    fn wait_until_entered(&self) {
        self.entered.wait();
    }

    fn release(&self) {
        self.release.wait();
    }
}

#[derive(Clone, Default)]
struct FakeDriver {
    state: Arc<Mutex<FakeDriverState>>,
}

impl FakeDriver {
    fn install_outcomes(
        outcomes: impl IntoIterator<Item = Result<KqueueInstallOutcome, KqueueDriverError>>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(FakeDriverState {
                install_outcomes: outcomes.into_iter().collect(),
                ..FakeDriverState::default()
            })),
        }
    }

    fn push_delete_outcome(&self, outcome: KqueueDeleteOutcome) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .delete_outcomes
            .push_back(outcome);
    }

    fn installs(&self) -> Vec<KqueueInstallRequest> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .installs
            .clone()
    }

    fn deletes(&self) -> Vec<(MacOsProcessIdentity, KqueueWatchId)> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .deletes
            .clone()
    }

    fn block_next_delete(&self) -> DeleteBarrier {
        let barrier = DeleteBarrier {
            entered: Arc::new(Barrier::new(2)),
            release: Arc::new(Barrier::new(2)),
        };
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .next_delete_barrier = Some(barrier.clone());
        barrier
    }
}

impl KqueueDriver for FakeDriver {
    fn install_oneshot_exit(
        &self,
        request: KqueueInstallRequest,
    ) -> Result<KqueueInstallOutcome, KqueueDriverError> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.installs.push(request);
        state
            .install_outcomes
            .pop_front()
            .unwrap_or(Ok(KqueueInstallOutcome::Installed))
    }

    fn delete_oneshot_exit(
        &self,
        identity: MacOsProcessIdentity,
        watch_id: KqueueWatchId,
    ) -> KqueueDeleteOutcome {
        let (outcome, barrier) = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.deletes.push((identity, watch_id));
            (
                state
                    .delete_outcomes
                    .pop_front()
                    .unwrap_or(KqueueDeleteOutcome::Deleted),
                state.next_delete_barrier.take(),
            )
        };
        if let Some(barrier) = barrier {
            barrier.entered.wait();
            barrier.release.wait();
        }
        outcome
    }
}

struct FakeFactory {
    driver: FakeDriver,
    initialization_log: Arc<Mutex<Vec<&'static str>>>,
    fail: bool,
}

impl KqueueDriverFactory for FakeFactory {
    fn initialize_supervisor_owned(
        &self,
    ) -> Result<Arc<dyn KqueueDriver>, MacOsNotifierInitializationError> {
        let mut log = self
            .initialization_log
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        log.push("kqueue");
        if self.fail {
            return Err(MacOsNotifierInitializationError::KqueueCreateFailed {
                errno: Some(libc::EMFILE),
            });
        }
        log.push("asyncfd");
        Ok(Arc::new(self.driver.clone()))
    }
}

#[derive(Default)]
struct CollectorState {
    observations: Vec<(MacOsProcessIdentity, KqueueExitProof)>,
    table_was_unlocked: Vec<bool>,
    probe: Option<WatchTableLockProbe>,
}

#[derive(Default)]
struct FakeCollector {
    state: Mutex<CollectorState>,
}

impl FakeCollector {
    fn install_probe(&self, probe: WatchTableLockProbe) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .probe = Some(probe);
    }

    fn observations(&self) -> Vec<(MacOsProcessIdentity, KqueueExitProof)> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .observations
            .clone()
    }

    fn table_was_unlocked(&self) -> Vec<bool> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .table_was_unlocked
            .clone()
    }
}

impl ProvenExitCollector for FakeCollector {
    fn collect_proven_exit(&self, identity: MacOsProcessIdentity, proof: KqueueExitProof) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let unlocked = state
            .probe
            .as_ref()
            .is_none_or(WatchTableLockProbe::can_lock_now);
        state.table_was_unlocked.push(unlocked);
        state.observations.push((identity, proof));
    }
}

fn identity(generation: u64, pid: i32) -> MacOsProcessIdentity {
    MacOsProcessIdentity::new(generation, pid).expect("valid test identity")
}

type TestNotifier = (
    MacOsNotifier,
    Arc<FakeCollector>,
    Arc<SharedRetainedHiddenScheduler>,
    Arc<Mutex<Vec<&'static str>>>,
);

fn notifier(driver: FakeDriver) -> TestNotifier {
    let collector = Arc::new(FakeCollector::default());
    let scheduler = Arc::new(SharedRetainedHiddenScheduler::default());
    let initialization_log = Arc::new(Mutex::new(Vec::new()));
    let factory = FakeFactory {
        driver,
        initialization_log: Arc::clone(&initialization_log),
        fail: false,
    };
    let notifier = MacOsNotifier::initialize(&factory, collector.clone(), scheduler.clone())
        .expect("fake notifier initializes");
    collector.install_probe(notifier.watch_table_lock_probe_for_test());
    (notifier, collector, scheduler, initialization_log)
}

#[test]
fn bootstrap_finishes_kqueue_and_asyncfd_before_admission_exists() {
    let driver = FakeDriver::default();
    let (notifier, _collector, _scheduler, log) = notifier(driver);
    let pending = notifier
        .prepare_watch(identity(1, 42))
        .expect("initialized notifier admits a watch");

    assert_eq!(
        *log.lock().unwrap_or_else(PoisonError::into_inner),
        ["kqueue", "asyncfd"]
    );
    assert_eq!(pending.watch_id().get(), 1);

    let failed_log = Arc::new(Mutex::new(Vec::new()));
    let failed_factory = FakeFactory {
        driver: FakeDriver::default(),
        initialization_log: Arc::clone(&failed_log),
        fail: true,
    };
    let result = MacOsNotifier::initialize(
        &failed_factory,
        Arc::new(FakeCollector::default()),
        Arc::new(SharedRetainedHiddenScheduler::default()),
    );
    assert!(matches!(
        result,
        Err(MacOsNotifierInitializationError::KqueueCreateFailed {
            errno: Some(libc::EMFILE)
        })
    ));
}

#[test]
fn macos_watch_contract_uses_proc_oneshot_note_exit_and_watch_id_udata() {
    let driver = FakeDriver::default();
    let (notifier, _collector, _scheduler, _log) = notifier(driver.clone());
    let pending = notifier
        .prepare_watch(identity(3, 42))
        .expect("watch installs");
    let requests = driver.installs();

    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0],
        KqueueInstallRequest::for_process(42, pending.watch_id())
    );
    assert_eq!(requests[0].filter, -5);
    assert_eq!(requests[0].flags, 0x01 | 0x10);
    assert_eq!(requests[0].fflags, 0x8000_0000);
    assert_eq!(requests[0].udata, pending.watch_id().get());
}

#[test]
fn early_note_exit_only_latches_until_atomic_commit() {
    let driver = FakeDriver::default();
    let (notifier, collector, _scheduler, _log) = notifier(driver);
    let process = identity(7, 402);
    let pending = notifier.prepare_watch(process).expect("watch installs");

    assert_eq!(
        notifier.dispatch_event(KqueueEvent::note_exit(402, pending.watch_id().get())),
        KqueueEventDisposition::LatchedBeforeCommit
    );
    assert!(collector.observations().is_empty());
    assert!(notifier.notified_before_commit_for_test(pending.watch_id()));

    assert_eq!(
        notifier
            .commit(&pending, process)
            .expect("the exact transaction commits"),
        KqueueEventDisposition::CollectedPublishedExit
    );
    assert_eq!(
        collector.observations(),
        [(process, KqueueExitProof::RetrievedNoteExit)]
    );
    assert_eq!(collector.table_was_unlocked(), [true]);
}

#[test]
fn installed_watch_never_scans_without_its_exact_knote() {
    let driver = FakeDriver::default();
    let (notifier, collector, scheduler, _log) = notifier(driver);
    let process = identity(9, 403);
    let pending = notifier.prepare_watch(process).expect("watch installs");

    assert_eq!(
        notifier.commit(&pending, process).expect("watch publishes"),
        KqueueEventDisposition::AwaitingExactNoteExit
    );
    assert!(collector.observations().is_empty());
    assert!(scheduler.pollable_snapshot().is_empty());
    assert_eq!(notifier.generic_scan_count_for_test(), 0);

    assert_eq!(
        notifier.dispatch_event(KqueueEvent::note_exit(403, pending.watch_id().get())),
        KqueueEventDisposition::CollectedPublishedExit
    );
    assert_eq!(
        collector.observations(),
        [(process, KqueueExitProof::RetrievedNoteExit)]
    );
    assert_eq!(collector.table_was_unlocked(), [true]);
}

#[test]
fn esrch_is_a_successful_no_knote_sentinel_collected_only_after_commit() {
    let driver = FakeDriver::install_outcomes([Ok(KqueueInstallOutcome::AlreadyExitedSentinel)]);
    let (notifier, collector, _scheduler, _log) = notifier(driver);
    let process = identity(11, 404);
    let pending = notifier
        .prepare_watch(process)
        .expect("ESRCH remains a successful spawn attachment");

    assert!(collector.observations().is_empty());
    assert_eq!(
        notifier
            .commit(&pending, process)
            .expect("the sentinel commits through the same boundary"),
        KqueueEventDisposition::CollectedPublishedExit
    );
    assert_eq!(
        collector.observations(),
        [(process, KqueueExitProof::AlreadyExitedSentinel)]
    );
    assert_eq!(collector.table_was_unlocked(), [true]);
}

#[test]
fn watch_ids_never_reuse_after_fatal_install_or_retirement() {
    let fatal = KqueueDriverError::KeventFatal {
        errno: libc::EINVAL,
    };
    let driver = FakeDriver::install_outcomes([Err(fatal), Ok(KqueueInstallOutcome::Installed)]);
    let (notifier, collector, _scheduler, _log) = notifier(driver.clone());

    assert_eq!(notifier.prepare_watch(identity(1, 410)), Err(fatal.into()));
    let live = notifier
        .prepare_watch(identity(2, 411))
        .expect("next watch installs");
    assert_eq!(driver.installs()[0].udata, 1);
    assert_eq!(live.watch_id().get(), 2);
    assert_eq!(
        notifier.commit(&live, identity(2, 411)).expect("publishes"),
        KqueueEventDisposition::AwaitingExactNoteExit
    );
    assert_eq!(
        notifier.dispatch_event(KqueueEvent::note_exit(411, live.watch_id().get())),
        KqueueEventDisposition::CollectedPublishedExit
    );
    assert_eq!(collector.observations().len(), 1);

    let next = notifier
        .prepare_watch(identity(3, 412))
        .expect("retired ID is not reused");
    assert_eq!(next.watch_id().get(), 3);
}

#[test]
fn invalid_mismatched_and_retired_events_are_discarded_without_pid_scan() {
    let driver = FakeDriver::default();
    let (notifier, collector, _scheduler, _log) = notifier(driver);
    let process = identity(12, 420);
    let pending = notifier.prepare_watch(process).expect("watch installs");
    notifier.commit(&pending, process).expect("publishes");

    assert_eq!(
        notifier.dispatch_event(KqueueEvent::note_exit(999, pending.watch_id().get())),
        KqueueEventDisposition::DiscardedMismatched
    );
    assert_eq!(
        notifier.dispatch_event(KqueueEvent::note_exit(420, 999_999)),
        KqueueEventDisposition::DiscardedInvalid
    );
    assert_eq!(
        notifier.dispatch_event(KqueueEvent::new(420, pending.watch_id().get(), 0, 0)),
        KqueueEventDisposition::DiscardedInvalid
    );
    assert!(collector.observations().is_empty());
    assert_eq!(notifier.generic_scan_count_for_test(), 0);

    assert_eq!(
        notifier.dispatch_event(KqueueEvent::note_exit(420, pending.watch_id().get())),
        KqueueEventDisposition::CollectedPublishedExit
    );
    assert_eq!(
        notifier.dispatch_event(KqueueEvent::note_exit(420, pending.watch_id().get())),
        KqueueEventDisposition::DiscardedRetired
    );
    assert_eq!(collector.observations().len(), 1);
}

#[test]
fn stale_watch_id_cannot_consume_a_reused_numeric_pid() {
    let driver = FakeDriver::default();
    let (notifier, collector, _scheduler, _log) = notifier(driver);
    let original_identity = identity(13, 425);
    let original = notifier
        .prepare_watch(original_identity)
        .expect("original watch installs");
    notifier
        .commit(&original, original_identity)
        .expect("original watch publishes");
    assert_eq!(
        notifier.dispatch_event(KqueueEvent::note_exit(425, original.watch_id().get())),
        KqueueEventDisposition::CollectedPublishedExit
    );

    let reused_identity = identity(14, 425);
    let reused = notifier
        .prepare_watch(reused_identity)
        .expect("same numeric PID receives a new exact watch");
    notifier
        .commit(&reused, reused_identity)
        .expect("reused PID watch publishes");
    assert_ne!(reused.watch_id(), original.watch_id());
    assert_eq!(
        notifier.dispatch_event(KqueueEvent::note_exit(425, original.watch_id().get())),
        KqueueEventDisposition::DiscardedRetired
    );
    assert_eq!(collector.observations().len(), 1);
    assert_eq!(
        notifier.dispatch_event(KqueueEvent::note_exit(425, reused.watch_id().get())),
        KqueueEventDisposition::CollectedPublishedExit
    );
    assert_eq!(
        collector.observations(),
        [
            (original_identity, KqueueExitProof::RetrievedNoteExit),
            (reused_identity, KqueueExitProof::RetrievedNoteExit),
        ]
    );
}

#[test]
fn mismatched_generation_cannot_publish_or_replace_exact_binding() {
    let driver = FakeDriver::default();
    let (notifier, collector, _scheduler, _log) = notifier(driver);
    let process = identity(20, 430);
    let pending = notifier.prepare_watch(process).expect("watch installs");

    let wrong = identity(21, 430);
    assert!(notifier.commit(&pending, wrong).is_err());
    assert!(collector.observations().is_empty());
    assert_eq!(
        notifier
            .commit(&pending, process)
            .expect("original binding remains intact"),
        KqueueEventDisposition::AwaitingExactNoteExit
    );
}

#[test]
fn retained_hidden_scheduler_enrolls_only_after_retired_safe() {
    let driver = FakeDriver::default();
    driver.push_delete_outcome(KqueueDeleteOutcome::UnexpectedError { errno: libc::EBADF });
    let (notifier, _collector, scheduler, _log) = notifier(driver.clone());
    let process = identity(30, 440);
    let pending = notifier.prepare_watch(process).expect("watch installs");
    let hidden = HiddenRegistrationIdentity::new(501, process);
    let lifecycle = HiddenLifecycleHandle::new(hidden, HiddenLifecyclePhase::ActionPending);

    assert_eq!(
        notifier.begin_hidden_retirement(&pending, &lifecycle),
        KqueueEventDisposition::DeleteUnproven
    );
    assert!(scheduler.all_enrollments_for_test().is_empty());
    assert_eq!(
        notifier.dispatch_event(KqueueEvent::note_exit(440, pending.watch_id().get())),
        KqueueEventDisposition::RetiredHidden
    );
    assert_eq!(
        scheduler.all_enrollments_for_test(),
        [(
            pending.watch_id(),
            hidden,
            HiddenLifecyclePhase::ActionPending
        )]
    );
    assert!(scheduler.pollable_snapshot().is_empty());

    lifecycle.mark_action_settled();
    assert_eq!(
        scheduler.pollable_snapshot(),
        [(pending.watch_id(), hidden)]
    );
    assert_eq!(
        notifier.dispatch_event(KqueueEvent::note_exit(440, pending.watch_id().get())),
        KqueueEventDisposition::RetiredHidden
    );
}

#[test]
fn successful_delete_enrolls_the_shared_scheduler_in_watch_id_order() {
    let driver = FakeDriver::default();
    let (notifier, _collector, scheduler, _log) = notifier(driver.clone());
    let first_process = identity(41, 450);
    let second_process = identity(42, 451);
    let first = notifier.prepare_watch(first_process).expect("first watch");
    let second = notifier
        .prepare_watch(second_process)
        .expect("second watch");
    let first_hidden = HiddenRegistrationIdentity::new(601, first_process);
    let second_hidden = HiddenRegistrationIdentity::new(602, second_process);
    let first_lifecycle = HiddenLifecycleHandle::new(first_hidden, HiddenLifecyclePhase::Settling);
    let second_lifecycle =
        HiddenLifecycleHandle::new(second_hidden, HiddenLifecyclePhase::ActionPending);
    second_lifecycle.mark_retained_invariant();

    assert_eq!(
        notifier.begin_hidden_retirement(&second, &second_lifecycle),
        KqueueEventDisposition::RetiredHidden
    );
    assert_eq!(
        notifier.begin_hidden_retirement(&first, &first_lifecycle),
        KqueueEventDisposition::RetiredHidden
    );
    assert_eq!(
        scheduler.pollable_snapshot(),
        [
            (first.watch_id(), first_hidden),
            (second.watch_id(), second_hidden)
        ]
    );
}

#[test]
fn unexpected_delete_then_action_then_exact_retry_is_immediately_pollable() {
    let driver = FakeDriver::default();
    driver.push_delete_outcome(KqueueDeleteOutcome::UnexpectedError { errno: libc::EBADF });
    driver.push_delete_outcome(KqueueDeleteOutcome::Deleted);
    let (notifier, _collector, scheduler, _log) = notifier(driver.clone());
    let process = identity(51, 460);
    let pending = notifier.prepare_watch(process).expect("watch installs");
    let hidden = HiddenRegistrationIdentity::new(701, process);
    let lifecycle = HiddenLifecycleHandle::new(hidden, HiddenLifecyclePhase::ActionPending);

    assert_eq!(
        notifier.begin_hidden_retirement(&pending, &lifecycle),
        KqueueEventDisposition::DeleteUnproven
    );
    lifecycle.mark_action_settled();
    assert_eq!(lifecycle.wake_epoch_for_test(), 0);
    let retry_result = notifier.retry_hidden_retirement(&pending, &lifecycle);

    assert_eq!(lifecycle.phase(), HiddenLifecyclePhase::Settling);
    assert_eq!(retry_result, KqueueEventDisposition::RetiredHidden);
    assert_eq!(
        notifier.retry_hidden_retirement(&pending, &lifecycle),
        KqueueEventDisposition::RetiredHidden
    );
    assert_eq!(
        driver.deletes(),
        [(process, pending.watch_id()), (process, pending.watch_id())]
    );
    assert_eq!(lifecycle.wake_epoch_for_test(), 1);
    assert_eq!(
        scheduler.pollable_snapshot(),
        [(pending.watch_id(), hidden)]
    );
}

#[test]
fn retirement_first_tick_skips_action_pending_then_action_wakes_pollable() {
    let driver = FakeDriver::default();
    let (notifier, _collector, scheduler, _log) = notifier(driver.clone());
    let process = identity(52, 461);
    let pending = notifier.prepare_watch(process).expect("watch installs");
    let hidden = HiddenRegistrationIdentity::new(702, process);
    let lifecycle = HiddenLifecycleHandle::new(hidden, HiddenLifecyclePhase::ActionPending);

    assert_eq!(
        notifier.begin_hidden_retirement(&pending, &lifecycle),
        KqueueEventDisposition::RetiredHidden
    );
    assert!(
        scheduler.pollable_snapshot().is_empty(),
        "a scheduler tick cannot wait while the hard action is pending"
    );
    assert_eq!(lifecycle.wake_epoch_for_test(), 0);
    lifecycle.mark_action_settled();
    assert_eq!(lifecycle.wake_epoch_for_test(), 1);
    assert_eq!(
        scheduler.pollable_snapshot(),
        [(pending.watch_id(), hidden)]
    );
    assert_eq!(
        notifier.dispatch_event(KqueueEvent::note_exit(461, pending.watch_id().get())),
        KqueueEventDisposition::RetiredHidden,
        "the exact already-enrolled hidden watch is an idempotent success"
    );
}

#[test]
fn blocking_delete_racing_note_exit_enrolls_once_and_both_paths_succeed() {
    let driver = FakeDriver::default();
    let barrier = driver.block_next_delete();
    let (notifier, _collector, scheduler, _log) = notifier(driver.clone());
    let notifier = Arc::new(notifier);
    let process = identity(53, 462);
    let pending = notifier.prepare_watch(process).expect("watch installs");
    let hidden = HiddenRegistrationIdentity::new(703, process);
    let lifecycle = HiddenLifecycleHandle::new(hidden, HiddenLifecyclePhase::Settling);

    let delete_notifier = Arc::clone(&notifier);
    let delete_lifecycle = lifecycle.clone();
    let delete = std::thread::spawn(move || {
        delete_notifier.begin_hidden_retirement(&pending, &delete_lifecycle)
    });
    barrier.wait_until_entered();
    let event_result =
        notifier.dispatch_event(KqueueEvent::note_exit(462, pending.watch_id().get()));
    barrier.release();
    let delete_result = delete.join().expect("delete thread joins");

    assert_eq!(event_result, KqueueEventDisposition::RetiredHidden);
    assert_eq!(delete_result, KqueueEventDisposition::RetiredHidden);
    assert_eq!(driver.deletes(), [(process, pending.watch_id())]);
    assert_eq!(lifecycle.wake_epoch_for_test(), 1);
    assert_eq!(
        scheduler.all_enrollments_for_test(),
        [(pending.watch_id(), hidden, HiddenLifecyclePhase::Settling)]
    );
}

#[test]
fn action_before_retirement_enrollment_barrier_is_not_lost() {
    let driver = FakeDriver::default();
    let barrier = driver.block_next_delete();
    let (notifier, _collector, scheduler, _log) = notifier(driver);
    let notifier = Arc::new(notifier);
    let process = identity(54, 463);
    let pending = notifier.prepare_watch(process).expect("watch installs");
    let hidden = HiddenRegistrationIdentity::new(704, process);
    let lifecycle = HiddenLifecycleHandle::new(hidden, HiddenLifecyclePhase::ActionPending);

    let delete_notifier = Arc::clone(&notifier);
    let delete_lifecycle = lifecycle.clone();
    let delete = std::thread::spawn(move || {
        delete_notifier.begin_hidden_retirement(&pending, &delete_lifecycle)
    });
    barrier.wait_until_entered();
    lifecycle.mark_action_settled();
    assert_eq!(lifecycle.wake_epoch_for_test(), 0);
    barrier.release();
    let delete_result = delete.join().expect("delete thread joins");

    assert_eq!(lifecycle.phase(), HiddenLifecyclePhase::Settling);
    assert_eq!(delete_result, KqueueEventDisposition::RetiredHidden);
    assert_eq!(lifecycle.wake_epoch_for_test(), 1);
    assert_eq!(
        scheduler.pollable_snapshot(),
        [(pending.watch_id(), hidden)]
    );
}

#[test]
fn early_note_exit_then_hidden_attachment_failure_retires_exactly_once() {
    let driver = FakeDriver::default();
    driver.push_delete_outcome(KqueueDeleteOutcome::AlreadyAbsent {
        errno: libc::ENOENT,
    });
    let (notifier, _collector, scheduler, _log) = notifier(driver);
    let process = identity(55, 464);
    let pending = notifier.prepare_watch(process).expect("watch installs");
    let hidden = HiddenRegistrationIdentity::new(705, process);
    let lifecycle = HiddenLifecycleHandle::new(hidden, HiddenLifecyclePhase::Settling);

    assert_eq!(
        notifier.dispatch_event(KqueueEvent::note_exit(464, pending.watch_id().get())),
        KqueueEventDisposition::LatchedBeforeCommit
    );
    assert_eq!(
        notifier.begin_hidden_retirement(&pending, &lifecycle),
        KqueueEventDisposition::RetiredHidden
    );
    assert_eq!(
        scheduler.all_enrollments_for_test(),
        [(pending.watch_id(), hidden, HiddenLifecyclePhase::Settling)]
    );
    assert_eq!(
        scheduler.pollable_snapshot(),
        [(pending.watch_id(), hidden)]
    );
}

#[test]
fn early_note_exit_latch_retires_hidden_without_delete_or_second_event() {
    let driver = FakeDriver::default();
    driver.push_delete_outcome(KqueueDeleteOutcome::UnexpectedError { errno: libc::EBADF });
    let (notifier, _collector, scheduler, _log) = notifier(driver.clone());
    let process = identity(56, 465);
    let pending = notifier.prepare_watch(process).expect("watch installs");
    let hidden = HiddenRegistrationIdentity::new(706, process);
    let lifecycle = HiddenLifecycleHandle::new(hidden, HiddenLifecyclePhase::ActionPending);

    assert_eq!(
        notifier.dispatch_event(KqueueEvent::note_exit(465, pending.watch_id().get())),
        KqueueEventDisposition::LatchedBeforeCommit
    );
    assert_eq!(
        notifier.begin_hidden_retirement(&pending, &lifecycle),
        KqueueEventDisposition::RetiredHidden,
        "the retrieved one-shot event is already exact retirement proof"
    );
    assert!(
        driver.deletes().is_empty(),
        "a retrieved one-shot must not depend on EV_DELETE"
    );
    assert_eq!(
        scheduler.all_enrollments_for_test(),
        [(
            pending.watch_id(),
            hidden,
            HiddenLifecyclePhase::ActionPending
        )]
    );
    assert!(scheduler.pollable_snapshot().is_empty());
    lifecycle.mark_action_settled();
    assert_eq!(
        scheduler.pollable_snapshot(),
        [(pending.watch_id(), hidden)]
    );
    assert_eq!(lifecycle.wake_epoch_for_test(), 1);
}

#[test]
fn same_hidden_identity_with_wrong_lifecycle_arc_is_fail_closed() {
    let driver = FakeDriver::default();
    driver.push_delete_outcome(KqueueDeleteOutcome::UnexpectedError { errno: libc::EBADF });
    driver.push_delete_outcome(KqueueDeleteOutcome::Deleted);
    let (notifier, _collector, scheduler, _log) = notifier(driver.clone());
    let process = identity(57, 466);
    let pending = notifier.prepare_watch(process).expect("watch installs");
    let hidden = HiddenRegistrationIdentity::new(707, process);
    let original = HiddenLifecycleHandle::new(hidden, HiddenLifecyclePhase::ActionPending);
    let wrong_arc = HiddenLifecycleHandle::new(hidden, HiddenLifecyclePhase::ActionPending);

    assert_eq!(
        notifier.begin_hidden_retirement(&pending, &original),
        KqueueEventDisposition::DeleteUnproven
    );
    assert_eq!(
        notifier.begin_hidden_retirement(&pending, &wrong_arc),
        KqueueEventDisposition::DiscardedMismatched
    );
    assert_eq!(
        notifier.retry_hidden_retirement(&pending, &wrong_arc),
        KqueueEventDisposition::DiscardedMismatched
    );
    assert_eq!(driver.deletes(), [(process, pending.watch_id())]);
    assert!(scheduler.all_enrollments_for_test().is_empty());
    assert_eq!(original.phase(), HiddenLifecyclePhase::ActionPending);
    assert_eq!(original.wake_epoch_for_test(), 0);

    assert_eq!(
        notifier.retry_hidden_retirement(&pending, &original),
        KqueueEventDisposition::RetiredHidden
    );
    wrong_arc.mark_action_settled();
    assert_eq!(wrong_arc.wake_epoch_for_test(), 0);
    assert_eq!(original.phase(), HiddenLifecyclePhase::ActionPending);
    assert!(scheduler.pollable_snapshot().is_empty());
    assert_eq!(
        notifier.retry_hidden_retirement(&pending, &wrong_arc),
        KqueueEventDisposition::DiscardedMismatched
    );
    assert_eq!(
        notifier.dispatch_event(KqueueEvent::note_exit(466, pending.watch_id().get())),
        KqueueEventDisposition::RetiredHidden
    );
    original.mark_action_settled();
    assert_eq!(
        scheduler.pollable_snapshot(),
        [(pending.watch_id(), hidden)]
    );
    assert_eq!(original.wake_epoch_for_test(), 1);
    assert_eq!(driver.deletes().len(), 2);
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn production_kqueue_is_cloexec_asyncfd_and_preserves_watch_id_udata() {
    use std::os::fd::{AsRawFd, BorrowedFd};
    use std::process::Command;
    use std::time::Duration;

    use macos_notifier::ProductionMacOsKqueue;

    let driver = ProductionMacOsKqueue::initialize().expect("production kqueue initializes");
    let descriptor_flags = unsafe { libc::fcntl(driver.as_raw_fd(), libc::F_GETFD) };
    assert_ne!(descriptor_flags, -1);
    assert_ne!(descriptor_flags & libc::FD_CLOEXEC, 0);
    let _borrowed = unsafe { BorrowedFd::borrow_raw(driver.as_raw_fd()) };

    let mut child = Command::new("/bin/sh")
        .args(["-c", "sleep 0.15; exit 7"])
        .spawn()
        .expect("spawn fixture child");
    let pid = i32::try_from(child.id()).expect("pid fits i32");
    let watch_id = KqueueWatchId::from_nonzero_for_test(0x1234);
    assert_eq!(
        driver.install_oneshot_exit(KqueueInstallRequest::for_process(pid, watch_id)),
        Ok(KqueueInstallOutcome::Installed)
    );

    let events = tokio::time::timeout(Duration::from_secs(5), driver.next_events())
        .await
        .expect("kqueue became readable")
        .expect("kqueue read succeeds");
    assert!(events
        .iter()
        .any(|event| { event.observed_pid() == pid && event.raw_watch_id() == watch_id.get() }));
    assert_eq!(child.wait().expect("reap fixture child").code(), Some(7));
}
