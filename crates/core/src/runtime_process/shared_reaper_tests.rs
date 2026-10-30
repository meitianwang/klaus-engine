use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

use super::linux_notifier::{PreparedPidfd, TestLinuxNotifierHandle};
use super::macos_notifier::{
    KqueueDeleteOutcome, KqueueDriver, KqueueDriverError, KqueueEvent, KqueueInstallOutcome,
    KqueueInstallRequest, KqueueWatchId, MacOsPrepareWatchError, MacOsProcessIdentity,
};
use super::plan::{Rust193EnvironmentPlan, Rust193SpawnPreimage};
use super::reaper::{
    DirectChildActionResult, KernelSignalObservation, KernelWaitObservation, ReaperAdmissionError,
    ReaperCommitError, ReaperInvariant, ReaperNotifierInvariant, ReaperPollResult,
    ReaperShutdownError, RegistrationIdentity, RuntimeDropAction, SharedUnixReaper,
    SignalActionResult, TestMacOsNotifierHandle, UnixChildKernel, UnpublishedUnixCleanupOutcome,
};
use super::spawn::{
    SharedRuntimeSpawner, SharedSpawnError, SharedSpawnPolicy, SharedSpawnPolicyError,
};
use super::unix::UnixExecImage;
use super::unix_spawn::{
    UnixShadowSpawnError, UnixSpawnTestControl, UnixStdioDisposition, UnixStdioPlan,
    UnregisteredUnixChild,
};
use super::{
    KillTarget, NodeSpawnPlan, ProcessAtom, ProcessEnvironmentGuard, ProcessEnvironmentPlan,
    ProcessImageDialect, ProcessTopology, RuntimeEnv,
};
use crate::runtime_values::RuntimeString;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum KernelEvent {
    Wait(RegistrationIdentity),
    ProvenWait(RegistrationIdentity),
    Signal(RegistrationIdentity),
    GroupSignal(RegistrationIdentity),
}

type PidfdPlan = Option<(
    oneshot::Receiver<io::Result<()>>,
    Arc<Mutex<Vec<RegistrationIdentity>>>,
)>;

#[derive(Default)]
struct FakeKernelState {
    waits: HashMap<u64, VecDeque<KernelWaitObservation>>,
    next_waits: VecDeque<KernelWaitObservation>,
    next_proven_waits: VecDeque<KernelWaitObservation>,
    signals: HashMap<u64, VecDeque<KernelSignalObservation>>,
    next_signals: VecDeque<KernelSignalObservation>,
    next_group_signals: VecDeque<KernelSignalObservation>,
    pidfd_plans: VecDeque<PidfdPlan>,
    pidfd_prepare_count: usize,
    panic_waits: HashSet<u64>,
    required_parent_endpoints_closed: Option<Arc<AtomicBool>>,
    required_hidden_delete_proven: Option<Arc<AtomicBool>>,
    next_signal_barrier: Option<Arc<MacOsInstallBarrier>>,
    events: Vec<KernelEvent>,
}

#[derive(Default)]
struct FakeKernel {
    state: Mutex<FakeKernelState>,
}

impl FakeKernel {
    fn push_wait(&self, identity: RegistrationIdentity, observation: KernelWaitObservation) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .waits
            .entry(identity.generation_for_test())
            .or_default()
            .push_back(observation);
    }

    fn push_next_wait(&self, observation: KernelWaitObservation) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .next_waits
            .push_back(observation);
    }

    fn push_pidfd_plan(&self, plan: PidfdPlan) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pidfd_plans
            .push_back(plan);
    }

    fn pidfd_prepare_count(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pidfd_prepare_count
    }

    fn take_events(&self) -> Vec<KernelEvent> {
        std::mem::take(
            &mut self
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .events,
        )
    }

    fn push_signal(&self, identity: RegistrationIdentity, observation: KernelSignalObservation) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .signals
            .entry(identity.generation_for_test())
            .or_default()
            .push_back(observation);
    }

    fn push_next_signal(&self, observation: KernelSignalObservation) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .next_signals
            .push_back(observation);
    }

    fn push_next_group_signal(&self, observation: KernelSignalObservation) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .next_group_signals
            .push_back(observation);
    }

    fn panic_next_wait(&self, identity: RegistrationIdentity) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .panic_waits
            .insert(identity.generation_for_test());
    }

    fn require_parent_endpoints_closed_before_signal(&self, closed: Arc<AtomicBool>) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .required_parent_endpoints_closed = Some(closed);
    }

    fn require_hidden_delete_proven_before_wait(&self, proven: Arc<AtomicBool>) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .required_hidden_delete_proven = Some(proven);
    }

    fn block_next_signal(&self) -> Arc<MacOsInstallBarrier> {
        let barrier = Arc::new(MacOsInstallBarrier::default());
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        assert!(
            state
                .next_signal_barrier
                .replace(Arc::clone(&barrier))
                .is_none(),
            "only one deterministic hard-action barrier may be armed"
        );
        barrier
    }

    fn wait_count(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .events
            .iter()
            .filter(|event| matches!(event, KernelEvent::Wait(_)))
            .count()
    }

    fn signal_identities(&self) -> Vec<RegistrationIdentity> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .events
            .iter()
            .filter_map(|event| match event {
                KernelEvent::Signal(identity) => Some(*identity),
                KernelEvent::Wait(_) | KernelEvent::ProvenWait(_) | KernelEvent::GroupSignal(_) => {
                    None
                }
            })
            .collect()
    }

    fn group_signal_identities(&self) -> Vec<RegistrationIdentity> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .events
            .iter()
            .filter_map(|event| match event {
                KernelEvent::GroupSignal(identity) => Some(*identity),
                KernelEvent::Wait(_) | KernelEvent::ProvenWait(_) | KernelEvent::Signal(_) => None,
            })
            .collect()
    }

    fn push_next_proven_wait(&self, observation: KernelWaitObservation) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .next_proven_waits
            .push_back(observation);
    }
}

impl UnixChildKernel for FakeKernel {
    fn wait_nonblocking(&self, identity: RegistrationIdentity) -> KernelWaitObservation {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(proven) = &state.required_hidden_delete_proven {
            assert!(
                proven.load(Ordering::Acquire),
                "hidden cleanup cannot WNOHANG until exact EV_DELETE is proven safe"
            );
        }
        state.events.push(KernelEvent::Wait(identity));
        if state.panic_waits.remove(&identity.generation_for_test()) {
            drop(state);
            panic!("deterministic worker scan panic");
        }
        if let Some(observation) = state.next_waits.pop_front() {
            return observation;
        }
        state
            .waits
            .entry(identity.generation_for_test())
            .or_default()
            .pop_front()
            .unwrap_or(KernelWaitObservation::Running)
    }

    fn wait_proven_exit(&self, identity: RegistrationIdentity) -> KernelWaitObservation {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.events.push(KernelEvent::ProvenWait(identity));
        state
            .next_proven_waits
            .pop_front()
            .unwrap_or(KernelWaitObservation::Exited(0))
    }

    fn signal_child(
        &self,
        identity: RegistrationIdentity,
        _signal: libc::c_int,
    ) -> KernelSignalObservation {
        let barrier = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.events.push(KernelEvent::Signal(identity));
            if let Some(closed) = &state.required_parent_endpoints_closed {
                assert!(
                    closed.load(Ordering::Acquire),
                    "parent endpoints must close before the hidden hard action"
                );
            }
            state.next_signal_barrier.take()
        };
        if let Some(barrier) = barrier {
            barrier.arrive_and_wait();
        }
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(observation) = state.next_signals.pop_front() {
            return observation;
        }
        state
            .signals
            .entry(identity.generation_for_test())
            .or_default()
            .pop_front()
            .unwrap_or(KernelSignalObservation::Delivered)
    }

    fn signal_process_group(
        &self,
        identity: RegistrationIdentity,
        _signal: libc::c_int,
    ) -> KernelSignalObservation {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.events.push(KernelEvent::GroupSignal(identity));
        if let Some(closed) = &state.required_parent_endpoints_closed {
            assert!(
                closed.load(Ordering::Acquire),
                "parent endpoints must close before the hidden group hard action"
            );
        }
        state
            .next_group_signals
            .pop_front()
            .unwrap_or(KernelSignalObservation::Delivered)
    }

    fn prepare_pidfd(&self, identity: RegistrationIdentity) -> Option<PreparedPidfd> {
        let (receiver, signals) = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.pidfd_prepare_count += 1;
            state.pidfd_plans.pop_front().flatten()
        }?;
        Some(PreparedPidfd::ready_for_test(identity, receiver, signals))
    }
}

fn fake_reaper() -> (SharedUnixReaper, Arc<FakeKernel>) {
    let kernel = Arc::new(FakeKernel::default());
    let reaper = SharedUnixReaper::ready_with_kernel_for_test(kernel.clone());
    (reaper, kernel)
}

#[test]
fn unix_process_group_drop_target_is_admitted_by_the_shared_reaper() {
    let (reaper, _kernel) = fake_reaper();
    let permit = reaper.admit_spawn(RuntimeDropAction::HardKill(KillTarget::UnixProcessGroup));
    assert!(
        permit.is_ok(),
        "group-owning Node/PTY registrations must be admitted before spawn"
    );
}

#[tokio::test]
async fn process_group_drop_is_group_first_and_falls_back_to_the_exact_child_on_any_error() {
    for (pid, group_observation, expected_group) in [
        (
            52_001,
            KernelSignalObservation::AlreadyGone,
            SignalActionResult::AlreadyGone,
        ),
        (
            52_002,
            KernelSignalObservation::Error(libc::EPERM),
            SignalActionResult::Failed { errno: libc::EPERM },
        ),
    ] {
        let (reaper, kernel) = fake_reaper();
        let child = reaper
            .adopt_pid_with_target_for_test(
                pid,
                RuntimeDropAction::HardKill(KillTarget::UnixProcessGroup),
                KillTarget::UnixProcessGroup,
            )
            .await
            .expect("admit group-owned child");
        let identity = child.identity_for_test();
        let _ = kernel.take_events();
        kernel.push_next_group_signal(group_observation);
        kernel.push_next_signal(KernelSignalObservation::Delivered);

        drop(child);

        assert_eq!(
            kernel.take_events(),
            vec![
                KernelEvent::GroupSignal(identity),
                KernelEvent::Signal(identity),
            ],
        );
        assert_eq!(
            reaper.drop_action_result_for_test(identity),
            Some(DirectChildActionResult::ProcessGroup {
                group: expected_group,
                direct_child_fallback: Some(SignalActionResult::Delivered),
            }),
        );
        kernel.push_wait(identity, KernelWaitObservation::Exited(libc::SIGKILL));
        assert!(matches!(
            reaper.poll(identity),
            ReaperPollResult::Published(Ok(_))
        ));
        reaper.shutdown().await.expect("group child reaped");
    }
}

#[tokio::test]
async fn process_group_success_never_sends_a_redundant_direct_child_signal() {
    let (reaper, kernel) = fake_reaper();
    let child = reaper
        .adopt_pid_with_target_for_test(
            52_003,
            RuntimeDropAction::HardKill(KillTarget::UnixProcessGroup),
            KillTarget::UnixProcessGroup,
        )
        .await
        .expect("admit group-owned child");
    let identity = child.identity_for_test();
    let _ = kernel.take_events();
    drop(child);
    assert_eq!(kernel.group_signal_identities(), vec![identity]);
    assert!(kernel.signal_identities().is_empty());
    assert_eq!(
        reaper.drop_action_result_for_test(identity),
        Some(DirectChildActionResult::ProcessGroup {
            group: SignalActionResult::Delivered,
            direct_child_fallback: None,
        }),
    );
    kernel.push_wait(identity, KernelWaitObservation::Exited(libc::SIGKILL));
    assert!(matches!(
        reaper.poll(identity),
        ReaperPollResult::Published(Ok(_))
    ));
    reaper.shutdown().await.expect("group child reaped");
}

fn fake_notified_reaper() -> (SharedUnixReaper, TestLinuxNotifierHandle, Arc<FakeKernel>) {
    let kernel = Arc::new(FakeKernel::default());
    let (reaper, notifier) = SharedUnixReaper::initialize_with_test_linux_notifier(kernel.clone());
    (reaper, notifier, kernel)
}

#[tokio::test(start_paused = true)]
async fn unpublished_postspawn_failure_closes_endpoints_then_transfers_the_only_wait_right() {
    let (reaper, kernel) = fake_reaper();
    let mut permit = reaper
        .admit_spawn_with_target(RuntimeDropAction::KeepAlive, KillTarget::Child)
        .expect("reserve hidden registration before descriptor work");
    {
        let _admission = permit
            .authorize_unix_spawn()
            .expect("authorize exactly one backend spawn");
    }
    let endpoints_closed = Arc::new(AtomicBool::new(false));
    kernel.require_parent_endpoints_closed_before_signal(Arc::clone(&endpoints_closed));
    kernel.push_next_wait(KernelWaitObservation::Exited(libc::SIGKILL));
    let close_flag = Arc::clone(&endpoints_closed);

    let cleanup =
        permit.cleanup_unpublished_unix(UnregisteredUnixChild::for_test(52_005), move || {
            close_flag.store(true, Ordering::Release);
        });
    tokio::pin!(cleanup);
    assert!(futures::poll!(cleanup.as_mut()).is_pending());
    reaper.scan_hidden_cleanup_registrations();
    let outcome = cleanup.await;
    let UnpublishedUnixCleanupOutcome::Reaped {
        identity,
        action_result,
    } = outcome
    else {
        panic!("terminal cleanup must return Reaped: {outcome:?}")
    };
    assert_eq!(action_result, DirectChildActionResult::Delivered);
    assert_eq!(
        kernel.take_events(),
        vec![KernelEvent::Signal(identity), KernelEvent::Wait(identity)],
    );
    assert!(endpoints_closed.load(Ordering::Acquire));
    assert_eq!(reaper.registration_count_for_test(), 0);
    assert_eq!(reaper.active_spawn_permits_for_test(), 0);
}

#[tokio::test(start_paused = true)]
async fn unpublished_postspawn_failure_returns_in_two_seconds_and_retains_live_identity() {
    let (reaper, kernel) = fake_reaper();
    let mut permit = reaper
        .admit_spawn_with_target(RuntimeDropAction::KeepAlive, KillTarget::Child)
        .expect("reserve hidden registration");
    {
        let _admission = permit.authorize_unix_spawn().expect("authorize spawn");
    }
    let cleanup = permit.cleanup_unpublished_unix(UnregisteredUnixChild::for_test(52_006), || {});
    tokio::pin!(cleanup);
    assert!(futures::poll!(cleanup.as_mut()).is_pending());
    tokio::time::advance(Duration::from_millis(1_999)).await;
    assert!(futures::poll!(cleanup.as_mut()).is_pending());
    tokio::time::advance(Duration::from_millis(1)).await;

    let outcome = cleanup.await;
    let UnpublishedUnixCleanupOutcome::ProcessStillLive {
        identity,
        action_result,
    } = outcome
    else {
        panic!("live target must be retained after the fixed bound: {outcome:?}")
    };
    assert_eq!(action_result, DirectChildActionResult::Delivered);
    assert_eq!(reaper.active_spawn_permits_for_test(), 0);
    assert_eq!(reaper.registration_count_for_test(), 1);
    assert_eq!(kernel.signal_identities(), vec![identity]);

    kernel.push_wait(identity, KernelWaitObservation::Exited(libc::SIGKILL));
    reaper.scan_hidden_cleanup_registrations();
    wait_for_registration_retirement(&reaper).await;
    assert_eq!(kernel.signal_identities(), vec![identity]);
}

#[derive(Default)]
struct FakeMacOsDriverState {
    install_outcomes: VecDeque<Result<KqueueInstallOutcome, KqueueDriverError>>,
    delete_outcomes: VecDeque<KqueueDeleteOutcome>,
    installs: Vec<KqueueInstallRequest>,
    deletes: Vec<(MacOsProcessIdentity, KqueueWatchId)>,
    next_install_barrier: Option<Arc<MacOsInstallBarrier>>,
    delete_proven: Option<Arc<AtomicBool>>,
    fallback_delete_outcome: Option<KqueueDeleteOutcome>,
}

#[derive(Default)]
struct MacOsInstallBarrierState {
    entered: bool,
    release: bool,
}

#[derive(Default)]
struct MacOsInstallBarrier {
    state: Mutex<MacOsInstallBarrierState>,
    changed: Condvar,
}

impl MacOsInstallBarrier {
    fn arrive_and_wait(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.entered = true;
        self.changed.notify_all();
        while !state.release {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    fn wait_until_entered(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        while !state.entered {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    fn release(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.release = true;
        self.changed.notify_all();
    }
}

#[derive(Default)]
struct FakeMacOsDriver {
    state: Mutex<FakeMacOsDriverState>,
}

impl FakeMacOsDriver {
    fn push_install_outcome(&self, outcome: Result<KqueueInstallOutcome, KqueueDriverError>) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .install_outcomes
            .push_back(outcome);
    }

    fn push_delete_outcome(&self, outcome: KqueueDeleteOutcome) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .delete_outcomes
            .push_back(outcome);
    }

    fn block_next_install(&self) -> Arc<MacOsInstallBarrier> {
        let barrier = Arc::new(MacOsInstallBarrier::default());
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        assert!(
            state
                .next_install_barrier
                .replace(Arc::clone(&barrier))
                .is_none(),
            "only one deterministic EV_ADD barrier may be armed"
        );
        barrier
    }

    fn mark_delete_proven_on_success(&self, proven: Arc<AtomicBool>) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        assert!(state.delete_proven.replace(proven).is_none());
    }

    fn set_fallback_delete_outcome(&self, outcome: KqueueDeleteOutcome) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .fallback_delete_outcome = Some(outcome);
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
}

impl KqueueDriver for FakeMacOsDriver {
    fn install_oneshot_exit(
        &self,
        request: KqueueInstallRequest,
    ) -> Result<KqueueInstallOutcome, KqueueDriverError> {
        let barrier = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.installs.push(request);
            state.next_install_barrier.take()
        };
        if let Some(barrier) = barrier {
            barrier.arrive_and_wait();
        }
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .install_outcomes
            .pop_front()
            .unwrap_or(Ok(KqueueInstallOutcome::Installed))
    }

    fn delete_oneshot_exit(
        &self,
        identity: MacOsProcessIdentity,
        watch_id: KqueueWatchId,
    ) -> KqueueDeleteOutcome {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.deletes.push((identity, watch_id));
        let outcome = state
            .delete_outcomes
            .pop_front()
            .or(state.fallback_delete_outcome)
            .unwrap_or(KqueueDeleteOutcome::Deleted);
        if matches!(
            outcome,
            KqueueDeleteOutcome::Deleted
                | KqueueDeleteOutcome::AlreadyAbsent {
                    errno: libc::ENOENT | libc::ESRCH
                }
        ) {
            if let Some(proven) = &state.delete_proven {
                proven.store(true, Ordering::Release);
            }
        }
        outcome
    }
}

fn fake_macos_reaper() -> (
    SharedUnixReaper,
    TestMacOsNotifierHandle,
    Arc<FakeKernel>,
    Arc<FakeMacOsDriver>,
) {
    let kernel = Arc::new(FakeKernel::default());
    let driver = Arc::new(FakeMacOsDriver::default());
    let (reaper, notifier) =
        SharedUnixReaper::initialize_with_test_macos_notifier(kernel.clone(), driver.clone());
    (reaper, notifier, kernel, driver)
}

fn commit_test_pid_blocking(
    permit: super::reaper::ReaperSpawnPermit,
    pid: libc::pid_t,
) -> Result<super::reaper::RuntimeChild, ReaperCommitError> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("commit runtime")
        .block_on(permit.commit_test_pid(pid))
}

async fn wait_for_cached_outcome(child: &super::reaper::RuntimeChild) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while child.try_wait().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("notifier must publish terminal state");
}

async fn wait_for_registration_retirement(reaper: &SharedUnixReaper) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while reaper.registration_count_for_test() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("notifier must retire the terminal registration");
}

async fn wait_for_notifier_invariant(reaper: &SharedUnixReaper) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while reaper.notifier_invariant_for_test().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("worker loss must close admission before commit");
}

#[tokio::test]
async fn linux_notifier_bootstrap_must_finish_before_spawn_admission_becomes_ready() {
    let shadow = SharedUnixReaper::initialize_shadow();
    assert!(matches!(
        shadow.admit_spawn(RuntimeDropAction::KeepAlive),
        Err(ReaperAdmissionError::PlatformNotifierUnavailable)
    ));

    let (reaper, _notifier, _kernel) = fake_notified_reaper();
    let permit = reaper
        .admit_spawn(RuntimeDropAction::KeepAlive)
        .expect("the fake stream and long-lived worker are installed before readiness");
    drop(permit);
    reaper.shutdown().await.expect("empty notifier shuts down");
}

#[tokio::test]
async fn macos_installed_knote_collects_only_after_exact_note_exit_without_wnohang_scan() {
    let (reaper, mut notifier, kernel, driver) = fake_macos_reaper();
    kernel.push_next_proven_wait(KernelWaitObservation::Exited(17 << 8));
    let mut child = reaper
        .adopt_pid_for_test(50_001, RuntimeDropAction::KeepAlive)
        .await
        .expect("installed macOS watch publishes the child");
    let identity = child.identity_for_test();
    let install = driver
        .installs()
        .into_iter()
        .next()
        .expect("one exact kqueue attachment");

    assert_eq!(install.pid, 50_001);
    assert!(kernel.take_events().is_empty(), "commit must not WNOHANG");
    notifier
        .notify_events_and_wait([KqueueEvent::note_exit(50_001, install.watch_id.get())])
        .await;
    wait_for_cached_outcome(&child).await;
    assert_eq!(
        child
            .try_wait()
            .expect("NOTE_EXIT publishes the cache")
            .expect("successful exact wait")
            .code(),
        Some(17)
    );
    assert_eq!(
        kernel.take_events(),
        vec![KernelEvent::ProvenWait(identity)]
    );
    assert_eq!(reaper.registration_count_for_test(), 0);

    child.disarm();
    drop(child);
    reaper.shutdown().await.expect("macOS worker shuts down");
}

#[tokio::test]
async fn macos_esrch_sentinel_commits_then_uses_the_same_proven_blocking_wait() {
    let (reaper, _notifier, kernel, driver) = fake_macos_reaper();
    driver.push_install_outcome(Ok(KqueueInstallOutcome::AlreadyExitedSentinel));
    kernel.push_next_proven_wait(KernelWaitObservation::Exited(23 << 8));

    let mut child = reaper
        .adopt_pid_for_test(50_002, RuntimeDropAction::KeepAlive)
        .await
        .expect("ESRCH remains a successful spawn path");
    let identity = child.identity_for_test();
    assert_eq!(
        child
            .try_wait()
            .expect("ESRCH commit caches terminal status")
            .expect("successful exact wait")
            .code(),
        Some(23)
    );
    assert_eq!(
        kernel.take_events(),
        vec![KernelEvent::ProvenWait(identity)]
    );
    assert_eq!(reaper.registration_count_for_test(), 0);

    child.disarm();
    drop(child);
    reaper.shutdown().await.expect("ESRCH namespace closes");
}

#[derive(Default)]
struct MacOsPublicationBarrierState {
    entered: bool,
    release: bool,
}

#[derive(Default)]
struct MacOsPublicationBarrierKernel {
    state: Mutex<MacOsPublicationBarrierState>,
    changed: Condvar,
    proven_waits: Mutex<Vec<RegistrationIdentity>>,
}

impl MacOsPublicationBarrierKernel {
    fn wait_until_map_insert_barrier(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        while !state.entered {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    fn release_map_insert(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.release = true;
        self.changed.notify_all();
    }
}

impl UnixChildKernel for MacOsPublicationBarrierKernel {
    fn wait_nonblocking(&self, _identity: RegistrationIdentity) -> KernelWaitObservation {
        panic!("normal macOS publication cannot WNOHANG")
    }

    fn wait_proven_exit(&self, identity: RegistrationIdentity) -> KernelWaitObservation {
        self.proven_waits
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(identity);
        KernelWaitObservation::Exited(29 << 8)
    }

    fn signal_child(
        &self,
        _identity: RegistrationIdentity,
        _signal: libc::c_int,
    ) -> KernelSignalObservation {
        KernelSignalObservation::Delivered
    }

    fn before_macos_registration_map_insert(&self, _identity: RegistrationIdentity) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.entered = true;
        self.changed.notify_all();
        while !state.release {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn macos_map_watch_and_lifecycle_publication_is_one_barrier_before_deferred_collection() {
    let kernel = Arc::new(MacOsPublicationBarrierKernel::default());
    let driver = Arc::new(FakeMacOsDriver::default());
    let (reaper, mut notifier) =
        SharedUnixReaper::initialize_with_test_macos_notifier(kernel.clone(), driver.clone());
    let permit = reaper
        .admit_spawn(RuntimeDropAction::KeepAlive)
        .expect("macOS notifier admits the transaction");
    let commit = std::thread::spawn(move || commit_test_pid_blocking(permit, 50_005));

    kernel.wait_until_map_insert_barrier();
    let install = driver
        .installs()
        .into_iter()
        .next()
        .expect("watch installed before the publication barrier");
    let event = KqueueEvent::note_exit(50_005, install.watch_id.get());
    let event_task = tokio::spawn(async move {
        notifier.notify_events_and_wait([event]).await;
        notifier
    });
    tokio::task::yield_now().await;
    let event_finished_before_publication = event_task.is_finished();
    kernel.release_map_insert();
    assert!(
        !event_finished_before_publication,
        "worker cannot consume a Published watch before the exact map row exists"
    );

    let mut child = commit
        .join()
        .expect("commit thread joins")
        .expect("atomic macOS publication succeeds");
    let _notifier = tokio::time::timeout(Duration::from_secs(2), event_task)
        .await
        .expect("worker consumes the deferred event")
        .expect("event task joins");
    wait_for_cached_outcome(&child).await;
    assert_eq!(
        child
            .try_wait()
            .expect("deferred collector caches status")
            .expect("successful exact wait")
            .code(),
        Some(29)
    );
    assert_eq!(
        kernel
            .proven_waits
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len(),
        1
    );
    child.disarm();
    drop(child);
    reaper.shutdown().await.expect("barrier namespace closes");
}

#[derive(Default)]
struct MacOsDeferredCollectorBarrierKernel {
    state: Mutex<MacOsPublicationBarrierState>,
    changed: Condvar,
    proven_waits: Mutex<Vec<RegistrationIdentity>>,
}

impl MacOsDeferredCollectorBarrierKernel {
    fn wait_until_deferred_collection(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        while !state.entered {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    fn release_deferred_collection(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.release = true;
        self.changed.notify_all();
    }
}

impl UnixChildKernel for MacOsDeferredCollectorBarrierKernel {
    fn wait_nonblocking(&self, _identity: RegistrationIdentity) -> KernelWaitObservation {
        panic!("a proven macOS commit cannot use WNOHANG")
    }

    fn wait_proven_exit(&self, identity: RegistrationIdentity) -> KernelWaitObservation {
        self.proven_waits
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(identity);
        KernelWaitObservation::Exited(31 << 8)
    }

    fn signal_child(
        &self,
        _identity: RegistrationIdentity,
        _signal: libc::c_int,
    ) -> KernelSignalObservation {
        KernelSignalObservation::Delivered
    }

    fn before_macos_deferred_collection(&self, _identity: RegistrationIdentity) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.entered = true;
        self.changed.notify_all();
        while !state.release {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }
}

#[tokio::test]
async fn macos_committed_exit_proof_outranks_worker_loss_before_deferred_collection() {
    let kernel = Arc::new(MacOsDeferredCollectorBarrierKernel::default());
    let driver = Arc::new(FakeMacOsDriver::default());
    driver.push_install_outcome(Ok(KqueueInstallOutcome::AlreadyExitedSentinel));
    let (reaper, notifier) =
        SharedUnixReaper::initialize_with_test_macos_notifier(kernel.clone(), driver);
    let permit = reaper
        .admit_spawn(RuntimeDropAction::KeepAlive)
        .expect("ESRCH transaction is admitted");
    let commit = std::thread::spawn(move || commit_test_pid_blocking(permit, 50_007));

    kernel.wait_until_deferred_collection();
    let registrations_before_collection = reaper.registration_count_for_test();
    notifier.close();
    wait_for_notifier_invariant(&reaper).await;
    kernel.release_deferred_collection();
    assert_eq!(registrations_before_collection, 1);

    let mut child = commit
        .join()
        .expect("deferred collector thread joins")
        .expect("the committed ESRCH proof remains a successful spawn");
    assert_eq!(
        child
            .try_wait()
            .expect("the proven wait populated the terminal cache")
            .expect("worker loss cannot overwrite the committed proof")
            .code(),
        Some(31)
    );
    assert_eq!(
        kernel
            .proven_waits
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len(),
        1
    );
    assert_eq!(reaper.registration_count_for_test(), 0);
    child.disarm();
    drop(child);
    assert_eq!(
        reaper.shutdown().await,
        Err(ReaperShutdownError::NotifierInvariant(
            ReaperNotifierInvariant::StreamClosed
        ))
    );
}

#[tokio::test]
async fn macos_fatal_attachment_never_publishes_and_uses_bounded_hidden_cleanup() {
    let (reaper, _notifier, kernel, driver) = fake_macos_reaper();
    let fatal = KqueueDriverError::KeventFatal {
        errno: libc::EINVAL,
    };
    driver.push_install_outcome(Err(fatal));
    kernel.push_next_wait(KernelWaitObservation::Exited(libc::SIGKILL));
    let parent_closed = Arc::new(AtomicBool::new(false));
    kernel.require_parent_endpoints_closed_before_signal(Arc::clone(&parent_closed));
    let permit = reaper
        .admit_spawn(RuntimeDropAction::KeepAlive)
        .expect("admission precedes PID creation");

    let error = match permit
        .commit_test_pid_with_close(50_003, || {
            parent_closed.store(true, Ordering::Release);
        })
        .await
    {
        Ok(_) => panic!("fatal EV_ADD cannot publish RuntimeChild"),
        Err(error) => error,
    };
    let ReaperCommitError::MacOsAttachment {
        identity,
        original_error,
        action_result,
    } = error
    else {
        panic!("terminal hidden cleanup must retain the attachment error: {error:?}")
    };
    assert_eq!(identity.pid_for_test(), 50_003);
    assert_eq!(original_error, MacOsPrepareWatchError::Driver(fatal));
    assert_eq!(action_result, DirectChildActionResult::Delivered);
    assert!(
        driver.deletes().is_empty(),
        "failed EV_ADD installed no knote"
    );
    assert_eq!(
        kernel.take_events(),
        vec![KernelEvent::Signal(identity), KernelEvent::Wait(identity)]
    );
    assert_eq!(reaper.registration_count_for_test(), 0);
    assert_eq!(reaper.active_spawn_permits_for_test(), 0);
    reaper.shutdown().await.expect("hidden child retired");
}

#[tokio::test]
async fn macos_worker_loss_wakes_waiters_and_closes_future_admission() {
    let (reaper, notifier, kernel, _driver) = fake_macos_reaper();
    let child = reaper
        .adopt_pid_for_test(50_004, RuntimeDropAction::KeepAlive)
        .await
        .expect("watch publishes before worker loss");
    let identity = child.identity_for_test();
    let waiter = child.waiter();
    assert!(kernel.take_events().is_empty());

    notifier.close();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), waiter.wait())
            .await
            .expect("worker loss wakes exact waiter"),
        Err(ReaperInvariant::NotifierClosed { identity })
    );
    assert!(matches!(
        reaper.admit_spawn(RuntimeDropAction::KeepAlive),
        Err(ReaperAdmissionError::PlatformNotifierUnavailable)
    ));
    assert_eq!(
        reaper.notifier_invariant_for_test(),
        Some(ReaperNotifierInvariant::StreamClosed)
    );
    assert!(kernel.take_events().is_empty());
    drop(child);
    assert_eq!(
        reaper.shutdown().await,
        Err(ReaperShutdownError::ReaperFailures(vec![(
            identity,
            ReaperInvariant::NotifierClosed { identity }
        )]))
    );
}

#[tokio::test]
async fn macos_hidden_installed_knote_retries_delete_before_retired_safe_polling() {
    let kernel = Arc::new(FakeKernel::default());
    let driver = Arc::new(FakeMacOsDriver::default());
    let install_barrier = driver.block_next_install();
    let delete_proven = Arc::new(AtomicBool::new(false));
    driver.mark_delete_proven_on_success(Arc::clone(&delete_proven));
    driver.push_delete_outcome(KqueueDeleteOutcome::UnexpectedError { errno: libc::EIO });
    driver.push_delete_outcome(KqueueDeleteOutcome::Deleted);
    kernel.require_hidden_delete_proven_before_wait(Arc::clone(&delete_proven));
    kernel.push_next_wait(KernelWaitObservation::Exited(libc::SIGKILL));

    let (reaper, notifier) =
        SharedUnixReaper::initialize_with_test_macos_notifier(kernel.clone(), driver.clone());
    let permit = reaper
        .admit_spawn(RuntimeDropAction::KeepAlive)
        .expect("admission precedes the installed EV_ADD race");
    let (commit_tx, commit_rx) = oneshot::channel();
    std::thread::spawn(move || {
        let _ = commit_tx.send(commit_test_pid_blocking(permit, 50_006));
    });

    install_barrier.wait_until_entered();
    let install = driver
        .installs()
        .into_iter()
        .next()
        .expect("the exact EV_ADD completed before worker loss");
    notifier.close();
    wait_for_notifier_invariant(&reaper).await;
    install_barrier.release();

    let error = match tokio::time::timeout(Duration::from_secs(2), commit_rx)
        .await
        .expect("retained scheduler completes hidden cleanup")
        .expect("commit thread reports its result")
    {
        Ok(_) => panic!("worker loss after EV_ADD cannot publish RuntimeChild"),
        Err(error) => error,
    };
    let ReaperCommitError::NotifierLost {
        identity,
        original_error,
        action_result,
    } = error
    else {
        panic!("terminal hidden cleanup retains the worker-loss cause: {error:?}")
    };
    assert_eq!(original_error, ReaperNotifierInvariant::StreamClosed);
    assert_eq!(action_result, DirectChildActionResult::Delivered);
    assert!(delete_proven.load(Ordering::Acquire));
    let process_identity =
        MacOsProcessIdentity::new(identity.generation_for_test(), identity.pid_for_test())
            .expect("exact hidden identity");
    assert_eq!(
        driver.deletes(),
        vec![
            (process_identity, install.watch_id),
            (process_identity, install.watch_id),
        ],
        "the sole scheduler retries EV_DELETE for the same generation, PID, and watch"
    );
    assert_eq!(
        kernel.take_events(),
        vec![KernelEvent::Signal(identity), KernelEvent::Wait(identity)],
        "the first and only WNOHANG occurs after RetiredSafe enrollment"
    );
    assert_eq!(reaper.registration_count_for_test(), 0);
    assert_eq!(reaper.active_spawn_permits_for_test(), 0);
    assert_eq!(
        reaper.shutdown().await,
        Err(ReaperShutdownError::NotifierInvariant(
            ReaperNotifierInvariant::StreamClosed
        ))
    );
}

#[tokio::test]
async fn macos_retired_safe_enrollment_stays_dormant_until_hard_action_settles() {
    let kernel = Arc::new(FakeKernel::default());
    let driver = Arc::new(FakeMacOsDriver::default());
    let install_barrier = driver.block_next_install();
    let action_barrier = kernel.block_next_signal();
    let (reaper, notifier) =
        SharedUnixReaper::initialize_with_test_macos_notifier(kernel.clone(), driver.clone());
    let permit = reaper
        .admit_spawn(RuntimeDropAction::KeepAlive)
        .expect("admission precedes worker loss");
    let (commit_tx, commit_rx) = oneshot::channel();
    std::thread::spawn(move || {
        let _ = commit_tx.send(commit_test_pid_blocking(permit, 50_008));
    });

    install_barrier.wait_until_entered();
    notifier.close();
    wait_for_notifier_invariant(&reaper).await;
    install_barrier.release();
    action_barrier.wait_until_entered();
    tokio::time::sleep(Duration::from_millis(60)).await;
    let waits_while_action_pending = kernel.wait_count();
    kernel.push_next_wait(KernelWaitObservation::Exited(libc::SIGKILL));
    action_barrier.release();
    assert_eq!(
        waits_while_action_pending, 0,
        "RetiredSafe plus ActionPending is dormant on every scheduler tick"
    );

    let error = match tokio::time::timeout(Duration::from_secs(2), commit_rx)
        .await
        .expect("settled dormant enrollment wakes immediately")
        .expect("commit thread reports its result")
    {
        Ok(_) => panic!("worker loss cannot publish RuntimeChild"),
        Err(error) => error,
    };
    let ReaperCommitError::NotifierLost {
        identity,
        action_result,
        ..
    } = error
    else {
        panic!("terminal hidden cleanup preserves notifier loss: {error:?}")
    };
    assert_eq!(action_result, DirectChildActionResult::Delivered);
    assert_eq!(kernel.signal_identities(), vec![identity]);
    assert_eq!(kernel.wait_count(), 1);
    assert_eq!(driver.deletes().len(), 1);
    assert_eq!(reaper.registration_count_for_test(), 0);
    assert_eq!(
        reaper.shutdown().await,
        Err(ReaperShutdownError::NotifierInvariant(
            ReaperNotifierInvariant::StreamClosed
        ))
    );
}

#[tokio::test]
async fn macos_action_first_retained_handoff_waits_for_later_delete_proof_and_exit() {
    let kernel = Arc::new(FakeKernel::default());
    let driver = Arc::new(FakeMacOsDriver::default());
    let install_barrier = driver.block_next_install();
    driver.set_fallback_delete_outcome(KqueueDeleteOutcome::UnexpectedError { errno: libc::EIO });
    let (reaper, notifier) =
        SharedUnixReaper::initialize_with_test_macos_notifier(kernel.clone(), driver.clone());
    let permit = reaper
        .admit_spawn(RuntimeDropAction::KeepAlive)
        .expect("admission precedes worker loss");
    let (commit_tx, commit_rx) = oneshot::channel();
    std::thread::spawn(move || {
        let _ = commit_tx.send(commit_test_pid_blocking(permit, 50_009));
    });

    install_barrier.wait_until_entered();
    let install = driver
        .installs()
        .into_iter()
        .next()
        .expect("one installed watch");
    notifier.close();
    wait_for_notifier_invariant(&reaper).await;
    install_barrier.release();
    tokio::time::timeout(Duration::from_secs(1), async {
        while kernel.signal_identities().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("hidden hard action is recorded");
    assert_eq!(
        kernel.wait_count(),
        0,
        "DeletePending forbids status consumption even after the action"
    );

    let error = match tokio::time::timeout(Duration::from_secs(5), commit_rx)
        .await
        .expect("the exact 2,000 ms handoff is bounded")
        .expect("commit thread reports its result")
    {
        Ok(_) => panic!("worker loss cannot publish RuntimeChild"),
        Err(error) => error,
    };
    let ReaperCommitError::ProcessStillLive {
        identity,
        original_error,
        action_result,
    } = error
    else {
        panic!("unproven delete retains the hidden registration: {error:?}")
    };
    assert_eq!(original_error, ReaperNotifierInvariant::StreamClosed);
    assert_eq!(action_result, DirectChildActionResult::Delivered);
    assert_eq!(reaper.active_spawn_permits_for_test(), 0);
    assert_eq!(reaper.registration_count_for_test(), 1);

    driver.set_fallback_delete_outcome(KqueueDeleteOutcome::Deleted);
    tokio::time::timeout(Duration::from_secs(1), async {
        while kernel.wait_count() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("later exact EV_DELETE activates the retained enrollment");
    kernel.push_wait(identity, KernelWaitObservation::Exited(libc::SIGKILL));
    wait_for_registration_retirement(&reaper).await;
    assert_eq!(kernel.signal_identities(), vec![identity]);
    let process_identity =
        MacOsProcessIdentity::new(identity.generation_for_test(), identity.pid_for_test())
            .expect("exact retained identity");
    assert!(
        driver
            .deletes()
            .iter()
            .all(|entry| *entry == (process_identity, install.watch_id)),
        "every delete retry remains bound to the same generation, PID, and watch"
    );
    assert_eq!(
        reaper.shutdown().await,
        Err(ReaperShutdownError::NotifierInvariant(
            ReaperNotifierInvariant::StreamClosed
        ))
    );
}

#[tokio::test]
async fn cancelling_macos_bounded_cleanup_nonfallibly_hands_off_the_same_retired_watch() {
    let (reaper, notifier, kernel, driver) = fake_macos_reaper();
    let permit = reaper
        .admit_spawn(RuntimeDropAction::KeepAlive)
        .expect("admit before notifier loss");
    notifier.close();
    wait_for_notifier_invariant(&reaper).await;

    let mut commit = Box::pin(permit.commit_test_pid(50_010));
    assert!(futures::poll!(commit.as_mut()).is_pending());
    let identity = kernel
        .signal_identities()
        .into_iter()
        .next()
        .expect("hidden action identity");
    let install = driver
        .installs()
        .into_iter()
        .next()
        .expect("the lost worker path still retires its installed watch");
    assert_eq!(reaper.active_spawn_permits_for_test(), 1);
    drop(commit);

    assert_eq!(reaper.active_spawn_permits_for_test(), 0);
    assert_eq!(reaper.registration_count_for_test(), 1);
    assert_eq!(kernel.signal_identities(), vec![identity]);
    assert_eq!(driver.deletes().len(), 1);
    tokio::time::timeout(Duration::from_secs(1), async {
        while kernel.wait_count() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("retained cancellation handoff remains scheduler-owned");
    kernel.push_wait(identity, KernelWaitObservation::Exited(libc::SIGKILL));
    wait_for_registration_retirement(&reaper).await;
    assert_eq!(kernel.signal_identities(), vec![identity]);
    assert_eq!(
        driver.deletes(),
        vec![(
            MacOsProcessIdentity::new(identity.generation_for_test(), identity.pid_for_test(),)
                .expect("exact cancelled identity"),
            install.watch_id,
        )]
    );
    assert_eq!(
        reaper.shutdown().await,
        Err(ReaperShutdownError::NotifierInvariant(
            ReaperNotifierInvariant::StreamClosed
        ))
    );
}

#[tokio::test(start_paused = true)]
async fn fast_exit_is_published_by_the_commit_immediate_scan_without_a_signal_edge() {
    let (reaper, _notifier, kernel) = fake_notified_reaper();
    kernel.push_next_wait(KernelWaitObservation::Exited(23 << 8));
    let started = tokio::time::Instant::now();

    let mut child = reaper
        .adopt_pid_for_test(40_901, RuntimeDropAction::KeepAlive)
        .await
        .expect("admit fast-exit child");
    assert_eq!(tokio::time::Instant::now(), started);
    assert_eq!(
        child
            .try_wait()
            .expect("commit cached terminal status")
            .expect("successful fast exit")
            .code(),
        Some(23)
    );
    assert_eq!(reaper.registration_count_for_test(), 0);

    child.disarm();
    drop(child);
    reaper.shutdown().await.expect("fast-exit namespace closes");
}

#[derive(Default)]
struct PublicationBarrierState {
    initial_poll_completed: bool,
    release_publication: bool,
    wait_count: usize,
    events: Vec<KernelEvent>,
}

#[derive(Default)]
struct PublicationBarrierKernel {
    state: Mutex<PublicationBarrierState>,
    changed: Condvar,
}

impl PublicationBarrierKernel {
    fn wait_until_initial_poll_completed(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        while !state.initial_poll_completed {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    fn release_publication(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.release_publication = true;
        self.changed.notify_all();
    }

    fn events(&self) -> Vec<KernelEvent> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .events
            .clone()
    }
}

impl UnixChildKernel for PublicationBarrierKernel {
    fn wait_nonblocking(&self, identity: RegistrationIdentity) -> KernelWaitObservation {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.events.push(KernelEvent::Wait(identity));
        state.wait_count += 1;
        if state.wait_count == 1 {
            KernelWaitObservation::Running
        } else {
            KernelWaitObservation::Exited(31 << 8)
        }
    }

    fn wait_proven_exit(&self, _identity: RegistrationIdentity) -> KernelWaitObservation {
        panic!("Linux publication-barrier fixture must not take a macOS proven wait")
    }

    fn signal_child(
        &self,
        identity: RegistrationIdentity,
        _signal: libc::c_int,
    ) -> KernelSignalObservation {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .events
            .push(KernelEvent::Signal(identity));
        KernelSignalObservation::Delivered
    }

    fn after_commit_initial_poll(&self, _identity: RegistrationIdentity) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.initial_poll_completed = true;
        self.changed.notify_all();
        while !state.release_publication {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }
}

#[tokio::test]
async fn publication_barrier_recovers_an_exit_after_worker_consumes_the_only_sigchld_edge() {
    let kernel = Arc::new(PublicationBarrierKernel::default());
    let (reaper, mut notifier) =
        SharedUnixReaper::initialize_with_test_linux_notifier(kernel.clone());
    let permit = reaper
        .admit_spawn(RuntimeDropAction::KeepAlive)
        .expect("admit while notifier is live");

    let commit = std::thread::spawn(move || commit_test_pid_blocking(permit, 40_912));
    kernel.wait_until_initial_poll_completed();
    notifier.notify_coalesced_and_wait(1).await;
    assert_eq!(
        kernel.events().len(),
        1,
        "the worker consumed the only edge while the row was Unpublished"
    );
    kernel.release_publication();

    let mut child = commit
        .join()
        .expect("commit thread")
        .expect("live notifier publishes the child");
    let identity = child.identity_for_test();
    assert_eq!(
        child
            .try_wait()
            .expect("post-publication barrier caches the missed exit")
            .expect("successful child exit")
            .code(),
        Some(31)
    );
    assert_eq!(
        kernel.events(),
        vec![KernelEvent::Wait(identity), KernelEvent::Wait(identity)],
        "Running before publication and Exited after publication require no second edge"
    );
    child.disarm();
    drop(child);
    reaper.shutdown().await.expect("published exit retires");
}

#[tokio::test]
async fn coalesced_sigchld_scans_each_exact_registration_and_never_a_foreign_pid() {
    let (reaper, notifier, kernel) = fake_notified_reaper();
    let mut child_a = reaper
        .adopt_pid_for_test(40_902, RuntimeDropAction::KeepAlive)
        .await
        .expect("admit child A");
    let mut child_b = reaper
        .adopt_pid_for_test(40_903, RuntimeDropAction::KeepAlive)
        .await
        .expect("admit child B");
    let identity_a = child_a.identity_for_test();
    let identity_b = child_b.identity_for_test();
    let _ = kernel.take_events();

    kernel.push_wait(identity_a, KernelWaitObservation::Exited(0));
    kernel.push_wait(identity_b, KernelWaitObservation::Exited(5 << 8));
    notifier.notify_coalesced(64);
    wait_for_cached_outcome(&child_a).await;
    wait_for_cached_outcome(&child_b).await;

    assert_eq!(
        kernel.take_events(),
        vec![KernelEvent::Wait(identity_a), KernelEvent::Wait(identity_b)],
        "one coalesced edge scans only the generation-bound project map"
    );
    child_a.disarm();
    child_b.disarm();
    drop((child_a, child_b));
    reaper.shutdown().await.expect("coalesced scan closes");
}

#[tokio::test]
async fn pidfd_preparation_failure_falls_back_to_the_already_live_sigchld_stream() {
    let (reaper, notifier, kernel) = fake_notified_reaper();
    kernel.push_pidfd_plan(None);
    let mut child = reaper
        .adopt_pid_for_test(40_904, RuntimeDropAction::KeepAlive)
        .await
        .expect("pidfd failure cannot change spawn success");
    let identity = child.identity_for_test();
    assert_eq!(kernel.pidfd_prepare_count(), 1);

    kernel.push_wait(identity, KernelWaitObservation::Exited(0));
    notifier.notify_coalesced(1);
    wait_for_cached_outcome(&child).await;

    child.disarm();
    drop(child);
    reaper.shutdown().await.expect("fallback scan closes");
}

#[tokio::test]
async fn pidfd_readiness_scans_only_its_exact_generation_bound_registration() {
    let (reaper, notifier, kernel) = fake_notified_reaper();
    let (pidfd_tx, pidfd_rx) = oneshot::channel();
    let pidfd_signals = Arc::new(Mutex::new(Vec::new()));
    kernel.push_pidfd_plan(Some((pidfd_rx, Arc::clone(&pidfd_signals))));
    let mut child_a = reaper
        .adopt_pid_for_test(40_905, RuntimeDropAction::KeepAlive)
        .await
        .expect("admit pidfd child");
    let mut child_b = reaper
        .adopt_pid_for_test(40_906, RuntimeDropAction::KeepAlive)
        .await
        .expect("admit fallback child");
    let identity_a = child_a.identity_for_test();
    let identity_b = child_b.identity_for_test();
    let _ = kernel.take_events();

    kernel.push_wait(identity_a, KernelWaitObservation::Exited(11 << 8));
    pidfd_tx
        .send(Ok(()))
        .expect("deliver deterministic pidfd readiness");
    wait_for_cached_outcome(&child_a).await;
    assert_eq!(kernel.take_events(), vec![KernelEvent::Wait(identity_a)]);
    assert_eq!(child_b.try_wait(), None);

    kernel.push_wait(identity_b, KernelWaitObservation::Exited(0));
    notifier.notify_coalesced(1);
    wait_for_cached_outcome(&child_b).await;
    child_a.disarm();
    child_b.disarm();
    drop((child_a, child_b));
    reaper.shutdown().await.expect("pidfd namespace closes");
}

#[tokio::test]
async fn direct_child_signal_prefers_pidfd_and_falls_back_to_generation_locked_kill() {
    let (pidfd_reaper, pidfd_notifier, pidfd_kernel) = fake_notified_reaper();
    let (_pidfd_tx, pidfd_rx) = oneshot::channel();
    let pidfd_signals = Arc::new(Mutex::new(Vec::new()));
    pidfd_kernel.push_pidfd_plan(Some((pidfd_rx, Arc::clone(&pidfd_signals))));
    let pidfd_child = pidfd_reaper
        .adopt_pid_for_test(40_908, RuntimeDropAction::HardKill(KillTarget::Child))
        .await
        .expect("admit child with pidfd capability");
    let pidfd_identity = pidfd_child.identity_for_test();
    drop(pidfd_child);
    assert_eq!(
        *pidfd_signals.lock().unwrap_or_else(PoisonError::into_inner),
        vec![pidfd_identity]
    );
    assert!(pidfd_kernel.signal_identities().is_empty());
    pidfd_kernel.push_wait(pidfd_identity, KernelWaitObservation::Exited(libc::SIGKILL));
    pidfd_notifier.notify_coalesced(1);
    wait_for_registration_retirement(&pidfd_reaper).await;
    pidfd_reaper
        .shutdown()
        .await
        .expect("pidfd-signalled child retires");

    let (fallback_reaper, fallback_notifier, fallback_kernel) = fake_notified_reaper();
    fallback_kernel.push_pidfd_plan(None);
    let fallback_child = fallback_reaper
        .adopt_pid_for_test(40_909, RuntimeDropAction::HardKill(KillTarget::Child))
        .await
        .expect("admit child without pidfd capability");
    let fallback_identity = fallback_child.identity_for_test();
    drop(fallback_child);
    assert_eq!(fallback_kernel.signal_identities(), vec![fallback_identity]);
    fallback_kernel.push_wait(
        fallback_identity,
        KernelWaitObservation::Exited(libc::SIGKILL),
    );
    fallback_notifier.notify_coalesced(1);
    wait_for_registration_retirement(&fallback_reaper).await;
    fallback_reaper
        .shutdown()
        .await
        .expect("kill fallback child retires");
}

#[tokio::test]
async fn notifier_worker_loss_wakes_waiters_and_is_retained_as_an_invariant() {
    let (reaper, notifier, kernel) = fake_notified_reaper();
    let child = reaper
        .adopt_pid_for_test(40_907, RuntimeDropAction::KeepAlive)
        .await
        .expect("admit child before notifier loss");
    let identity = child.identity_for_test();
    let waiter = child.waiter();
    let _ = kernel.take_events();

    notifier.close();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), waiter.wait())
            .await
            .expect("worker loss wakes the waiter"),
        Err(ReaperInvariant::NotifierClosed { identity })
    );
    assert!(
        kernel.take_events().is_empty(),
        "published KeepAlive policy cannot be replaced by notifier-loss hard kill"
    );
    assert_eq!(
        reaper.notifier_invariant_for_test(),
        Some(ReaperNotifierInvariant::StreamClosed)
    );
    drop(child);
    assert_eq!(
        reaper.shutdown().await,
        Err(ReaperShutdownError::ReaperFailures(vec![(
            identity,
            ReaperInvariant::NotifierClosed { identity }
        )]))
    );
}

#[tokio::test(start_paused = true)]
async fn hidden_cleanup_terminal_within_two_seconds_returns_the_original_notifier_error() {
    let (reaper, notifier, kernel) = fake_notified_reaper();
    let permit = reaper
        .admit_spawn(RuntimeDropAction::KeepAlive)
        .expect("admit while worker is live");
    notifier.close();
    wait_for_notifier_invariant(&reaper).await;
    kernel.push_next_wait(KernelWaitObservation::Running);

    let commit = permit.commit_test_pid(40_910);
    tokio::pin!(commit);
    assert!(futures::poll!(commit.as_mut()).is_pending());
    tokio::task::yield_now().await;
    let identity = kernel
        .signal_identities()
        .into_iter()
        .next()
        .expect("hidden cleanup hard action");
    kernel.push_wait(identity, KernelWaitObservation::Exited(libc::SIGKILL));
    tokio::time::advance(Duration::from_millis(25)).await;
    tokio::task::yield_now().await;

    let error = match commit.await {
        Ok(_) => panic!("a lost notifier must not publish RuntimeChild"),
        Err(error) => error,
    };
    assert_eq!(
        error,
        ReaperCommitError::NotifierLost {
            identity,
            original_error: ReaperNotifierInvariant::StreamClosed,
            action_result: DirectChildActionResult::Delivered,
        }
    );
    assert_eq!(kernel.signal_identities(), vec![identity]);
    assert_eq!(reaper.active_spawn_permits_for_test(), 0);
    assert_eq!(reaper.registration_count_for_test(), 0);
    assert_eq!(
        reaper.shutdown().await,
        Err(ReaperShutdownError::NotifierInvariant(
            ReaperNotifierInvariant::StreamClosed
        ))
    );
}

#[tokio::test(start_paused = true)]
async fn hidden_cleanup_closes_parent_endpoints_before_consuming_the_hard_action() {
    let (reaper, notifier, kernel) = fake_notified_reaper();
    let permit = reaper
        .admit_spawn(RuntimeDropAction::KeepAlive)
        .expect("admit before notifier loss");
    notifier.close();
    wait_for_notifier_invariant(&reaper).await;
    let endpoints_closed = Arc::new(AtomicBool::new(false));
    kernel.require_parent_endpoints_closed_before_signal(Arc::clone(&endpoints_closed));
    kernel.push_next_wait(KernelWaitObservation::Exited(libc::SIGKILL));

    let close_flag = Arc::clone(&endpoints_closed);
    let error = match permit
        .commit_test_pid_with_close(40_920, move || {
            close_flag.store(true, Ordering::Release);
        })
        .await
    {
        Ok(_) => panic!("lost notifier cannot publish RuntimeChild"),
        Err(error) => error,
    };
    let ReaperCommitError::NotifierLost { identity, .. } = error else {
        panic!("terminal cleanup must return original notifier error: {error:?}")
    };
    assert!(endpoints_closed.load(Ordering::Acquire));
    assert_eq!(kernel.signal_identities(), vec![identity]);
}

#[tokio::test(start_paused = true)]
async fn hidden_group_cleanup_records_group_failure_and_direct_fallback_separately() {
    let (reaper, notifier, kernel) = fake_notified_reaper();
    let permit = reaper
        .admit_spawn_with_target(RuntimeDropAction::KeepAlive, KillTarget::UnixProcessGroup)
        .expect("bind group cleanup target before spawn");
    notifier.close();
    wait_for_notifier_invariant(&reaper).await;
    let endpoints_closed = Arc::new(AtomicBool::new(false));
    kernel.require_parent_endpoints_closed_before_signal(Arc::clone(&endpoints_closed));
    kernel.push_next_group_signal(KernelSignalObservation::Error(libc::EPERM));
    kernel.push_next_signal(KernelSignalObservation::AlreadyGone);
    kernel.push_next_wait(KernelWaitObservation::Exited(libc::SIGKILL));

    let close_flag = Arc::clone(&endpoints_closed);
    let result = permit
        .commit_test_pid_with_close(52_004, move || {
            close_flag.store(true, Ordering::Release);
        })
        .await;
    let error = match result {
        Ok(_) => panic!("notifier loss cannot publish a group child"),
        Err(error) => error,
    };
    let ReaperCommitError::NotifierLost {
        identity,
        original_error,
        action_result,
    } = error
    else {
        panic!("terminal cleanup returns original notifier error: {error:?}")
    };
    assert_eq!(original_error, ReaperNotifierInvariant::StreamClosed);
    assert_eq!(
        action_result,
        DirectChildActionResult::ProcessGroup {
            group: SignalActionResult::Failed { errno: libc::EPERM },
            direct_child_fallback: Some(SignalActionResult::AlreadyGone),
        },
    );
    assert_eq!(
        kernel.take_events(),
        vec![
            KernelEvent::GroupSignal(identity),
            KernelEvent::Signal(identity),
            KernelEvent::Wait(identity),
        ],
    );
    assert!(endpoints_closed.load(Ordering::Acquire));
    assert_eq!(reaper.registration_count_for_test(), 0);
}

#[tokio::test(start_paused = true)]
async fn hidden_cleanup_live_after_two_seconds_returns_typed_retained_handoff_then_reaps() {
    let (reaper, notifier, kernel) = fake_notified_reaper();
    let permit = reaper
        .admit_spawn(RuntimeDropAction::KeepAlive)
        .expect("admit while worker is live");
    notifier.close();
    wait_for_notifier_invariant(&reaper).await;

    let commit = permit.commit_test_pid(40_916);
    tokio::pin!(commit);
    assert!(futures::poll!(commit.as_mut()).is_pending());
    assert_eq!(reaper.active_spawn_permits_for_test(), 1);
    tokio::time::advance(Duration::from_millis(1_999)).await;
    assert!(futures::poll!(commit.as_mut()).is_pending());
    assert_eq!(reaper.active_spawn_permits_for_test(), 1);
    tokio::time::advance(Duration::from_millis(1)).await;

    let error = match commit.await {
        Ok(_) => panic!("a lost notifier must not publish RuntimeChild"),
        Err(error) => error,
    };
    let ReaperCommitError::ProcessStillLive {
        identity,
        original_error,
        action_result,
    } = error
    else {
        panic!("two-second live target must return ProcessStillLive: {error:?}")
    };
    assert_eq!(original_error, ReaperNotifierInvariant::StreamClosed);
    assert_eq!(action_result, DirectChildActionResult::Delivered);
    assert_eq!(kernel.signal_identities(), vec![identity]);
    assert_eq!(reaper.active_spawn_permits_for_test(), 0);
    assert_eq!(reaper.registration_count_for_test(), 1);

    kernel.push_wait(identity, KernelWaitObservation::Exited(libc::SIGKILL));
    tokio::time::advance(Duration::from_millis(25)).await;
    wait_for_registration_retirement(&reaper).await;
    assert_eq!(kernel.signal_identities(), vec![identity]);
    assert_eq!(
        reaper.shutdown().await,
        Err(ReaperShutdownError::NotifierInvariant(
            ReaperNotifierInvariant::StreamClosed
        ))
    );
}

#[tokio::test(start_paused = true)]
async fn hidden_cleanup_signal_esrch_and_failure_still_settle_without_retrying_the_action() {
    for (pid, signal_observation, expected_action) in [
        (
            40_917,
            KernelSignalObservation::AlreadyGone,
            DirectChildActionResult::AlreadyGone,
        ),
        (
            40_918,
            KernelSignalObservation::Error(libc::EPERM),
            DirectChildActionResult::Failed { errno: libc::EPERM },
        ),
    ] {
        let (reaper, notifier, kernel) = fake_notified_reaper();
        let permit = reaper
            .admit_spawn(RuntimeDropAction::KeepAlive)
            .expect("admit before notifier loss");
        notifier.close();
        wait_for_notifier_invariant(&reaper).await;
        kernel.push_next_signal(signal_observation);
        kernel.push_next_wait(KernelWaitObservation::Exited(libc::SIGKILL));

        let error = match permit.commit_test_pid(pid).await {
            Ok(_) => panic!("lost notifier cannot publish RuntimeChild"),
            Err(error) => error,
        };
        let ReaperCommitError::NotifierLost {
            identity,
            original_error,
            action_result,
        } = error
        else {
            panic!("terminal cleanup returns the original notifier error: {error:?}")
        };
        assert_eq!(original_error, ReaperNotifierInvariant::StreamClosed);
        assert_eq!(action_result, expected_action);
        assert_eq!(kernel.signal_identities(), vec![identity]);
        assert_eq!(reaper.registration_count_for_test(), 0);
    }
}

#[tokio::test(start_paused = true)]
async fn hidden_cleanup_wait_failure_is_a_retained_invariant_not_process_still_live() {
    let (reaper, notifier, kernel) = fake_notified_reaper();
    let permit = reaper
        .admit_spawn(RuntimeDropAction::KeepAlive)
        .expect("admit before notifier loss");
    notifier.close();
    wait_for_notifier_invariant(&reaper).await;
    kernel.push_next_wait(KernelWaitObservation::Error(libc::ECHILD));

    let error = match permit.commit_test_pid(40_921).await {
        Ok(_) => panic!("lost notifier cannot publish RuntimeChild"),
        Err(error) => error,
    };
    let ReaperCommitError::RetainedInvariant {
        identity,
        original_error,
        action_result,
        reaper_error,
    } = error
    else {
        panic!("wait ownership loss needs its own retained invariant: {error:?}")
    };
    assert_eq!(original_error, ReaperNotifierInvariant::StreamClosed);
    assert_eq!(action_result, DirectChildActionResult::Delivered);
    assert_eq!(
        reaper_error,
        ReaperInvariant::WaitOwnershipLost { identity }
    );
    assert_eq!(reaper.active_spawn_permits_for_test(), 0);
    assert_eq!(reaper.registration_count_for_test(), 1);
}

#[tokio::test(start_paused = true)]
async fn hidden_cleanup_settle_keeps_shutdown_behind_the_spawn_permit_until_handoff() {
    let (reaper, notifier, kernel) = fake_notified_reaper();
    let permit = reaper
        .admit_spawn(RuntimeDropAction::KeepAlive)
        .expect("hold a permit before notifier loss");
    notifier.close();
    wait_for_notifier_invariant(&reaper).await;

    let commit = permit.commit_test_pid(40_915);
    tokio::pin!(commit);
    assert!(futures::poll!(commit.as_mut()).is_pending());
    let shutdown = reaper.shutdown();
    tokio::pin!(shutdown);
    assert!(futures::poll!(shutdown.as_mut()).is_pending());
    assert_eq!(reaper.active_spawn_permits_for_test(), 1);
    tokio::time::advance(Duration::from_millis(1_999)).await;
    assert!(futures::poll!(commit.as_mut()).is_pending());
    assert!(futures::poll!(shutdown.as_mut()).is_pending());
    tokio::time::advance(Duration::from_millis(1)).await;

    let error = match commit.await {
        Ok(_) => panic!("lost notifier cannot publish RuntimeChild"),
        Err(error) => error,
    };
    let ReaperCommitError::ProcessStillLive { identity, .. } = error else {
        panic!("bounded settle must retain a live child: {error:?}")
    };
    assert_eq!(
        shutdown.await,
        Err(ReaperShutdownError::LiveRegistrations(vec![identity]))
    );
    kernel.push_wait(identity, KernelWaitObservation::Exited(libc::SIGKILL));
    tokio::time::advance(Duration::from_millis(25)).await;
    wait_for_registration_retirement(&reaper).await;
}

#[tokio::test(start_paused = true)]
async fn cancelling_the_bounded_settle_performs_a_nonfallible_retained_handoff() {
    let (reaper, notifier, kernel) = fake_notified_reaper();
    let permit = reaper
        .admit_spawn(RuntimeDropAction::KeepAlive)
        .expect("admit before notifier loss");
    notifier.close();
    wait_for_notifier_invariant(&reaper).await;

    let mut commit = Box::pin(permit.commit_test_pid(40_919));
    assert!(futures::poll!(commit.as_mut()).is_pending());
    let identity = kernel
        .signal_identities()
        .into_iter()
        .next()
        .expect("hidden action identity");
    assert_eq!(reaper.active_spawn_permits_for_test(), 1);
    drop(commit);

    assert_eq!(reaper.active_spawn_permits_for_test(), 0);
    assert_eq!(reaper.registration_count_for_test(), 1);
    assert_eq!(kernel.signal_identities(), vec![identity]);
    kernel.push_wait(identity, KernelWaitObservation::Exited(libc::SIGKILL));
    tokio::time::advance(Duration::from_millis(25)).await;
    wait_for_registration_retirement(&reaper).await;
    assert_eq!(kernel.signal_identities(), vec![identity]);
}

#[tokio::test]
async fn worker_scan_panic_retains_hidden_cleanup_in_the_same_long_lived_worker() {
    let (reaper, notifier, kernel) = fake_notified_reaper();
    let trigger = reaper
        .adopt_pid_for_test(40_913, RuntimeDropAction::KeepAlive)
        .await
        .expect("publish a row that will panic the worker scan");
    let trigger_identity = trigger.identity_for_test();
    let held_permit = reaper
        .admit_spawn(RuntimeDropAction::KeepAlive)
        .expect("hold a permit across worker loss");
    let _ = kernel.take_events();

    kernel.panic_next_wait(trigger_identity);
    notifier.notify_coalesced(1);
    wait_for_notifier_invariant(&reaper).await;
    assert_eq!(
        reaper.notifier_invariant_for_test(),
        Some(ReaperNotifierInvariant::WorkerLost)
    );
    assert_eq!(
        kernel.take_events(),
        vec![KernelEvent::Wait(trigger_identity)],
        "the real notifier scan future panics inside the worker"
    );

    kernel.push_next_wait(KernelWaitObservation::Running);
    let commit = held_permit.commit_test_pid(40_914);
    tokio::pin!(commit);
    assert!(futures::poll!(commit.as_mut()).is_pending());
    tokio::task::yield_now().await;
    let commit_events = kernel.take_events();
    let identity = commit_events
        .iter()
        .find_map(|event| match event {
            KernelEvent::Signal(identity) => Some(*identity),
            KernelEvent::Wait(_) | KernelEvent::ProvenWait(_) | KernelEvent::GroupSignal(_) => None,
        })
        .expect("hidden hard action identity");
    assert_eq!(commit_events.first(), Some(&KernelEvent::Signal(identity)));
    assert_eq!(
        commit_events
            .iter()
            .filter(|event| **event == KernelEvent::Signal(identity))
            .count(),
        1,
        "precommit notifier loss hard-signals exactly once"
    );
    assert!(commit_events.contains(&KernelEvent::Wait(identity)));

    kernel.push_wait(identity, KernelWaitObservation::Exited(libc::SIGKILL));
    tokio::time::sleep(Duration::from_millis(25)).await;
    let error = match commit.await {
        Ok(_) => panic!("worker loss cannot publish a RuntimeChild"),
        Err(error) => error,
    };
    assert_eq!(
        error,
        ReaperCommitError::NotifierLost {
            identity,
            original_error: ReaperNotifierInvariant::WorkerLost,
            action_result: DirectChildActionResult::Delivered,
        }
    );
    assert_eq!(reaper.registration_count_for_test(), 1);
    let retained_events = kernel.take_events();
    assert!(!retained_events.is_empty());
    assert!(retained_events
        .iter()
        .all(|event| *event == KernelEvent::Wait(identity)));

    drop(trigger);
    assert_eq!(
        reaper.shutdown().await,
        Err(ReaperShutdownError::ReaperFailures(vec![(
            trigger_identity,
            ReaperInvariant::NotifierClosed {
                identity: trigger_identity,
            },
        )]))
    );
}

fn runtime(value: impl Into<RuntimeString>) -> ProcessAtom {
    ProcessAtom::Runtime(value.into())
}

fn rust_image(file: &str, args: &[&str]) -> UnixExecImage {
    let preimage = Rust193SpawnPreimage::from_deferred_parts(
        runtime(file),
        runtime(file),
        args.iter().map(|arg| runtime(*arg)).collect(),
        None,
        Rust193EnvironmentPlan::NativeParentInherit,
    );
    UnixExecImage::build_rust193_with_parent_snapshot(
        &preimage,
        ProcessEnvironmentGuard::None,
        || panic!("native inherit must not snapshot"),
    )
    .expect("valid Rust193 Unix image")
}

fn node_image(file: &str) -> UnixExecImage {
    let plan = NodeSpawnPlan::from_normalized_parts(
        runtime(file),
        runtime(file),
        Vec::new(),
        None,
        ProcessEnvironmentPlan::NodeRuntime(RuntimeEnv::new()),
        false,
        false,
        false,
    )
    .expect("valid Node plan");
    UnixExecImage::build_node(&plan, ProcessEnvironmentGuard::None).expect("valid Node image")
}

fn inherit_stdio() -> UnixStdioPlan {
    UnixStdioPlan::new(
        UnixStdioDisposition::Inherit,
        UnixStdioDisposition::Inherit,
        UnixStdioDisposition::Inherit,
    )
}

fn pipe_stdio() -> UnixStdioPlan {
    UnixStdioPlan::new(
        UnixStdioDisposition::Pipe,
        UnixStdioDisposition::Pipe,
        UnixStdioDisposition::Pipe,
    )
}

#[tokio::test]
async fn shared_reaper_has_one_wait_owner_and_one_cached_outcome_for_all_waiters() {
    let (reaper, kernel) = fake_reaper();
    let mut child = reaper
        .adopt_pid_for_test(41_001, RuntimeDropAction::KeepAlive)
        .await
        .expect("admit fake child");
    let identity = child.identity_for_test();
    assert_eq!(
        kernel.wait_count(),
        2,
        "commit performs pre-publication and post-publication exact scans"
    );

    let waiter_a = child.waiter();
    let cancelled_waiter = child.waiter();
    let waiter_b = child.waiter();
    drop(cancelled_waiter);
    for _ in 0..10_000 {
        assert_eq!(child.try_wait(), None);
    }
    assert_eq!(kernel.wait_count(), 2, "try_wait must remain cache-only");

    kernel.push_wait(identity, KernelWaitObservation::Exited(7 << 8));
    let ReaperPollResult::Published(Ok(published)) = reaper.poll(identity) else {
        panic!("first terminal observation must publish exactly once")
    };
    assert_eq!(published.code(), Some(7));
    assert_eq!(
        reaper.registration_count_for_test(),
        0,
        "terminal success retires the exact map entry while Arc waiters keep the cache"
    );
    let wait_calls_after_terminal = kernel.wait_count();
    assert_eq!(
        reaper.poll(identity),
        ReaperPollResult::UnknownRegistration(identity)
    );
    assert_eq!(kernel.wait_count(), wait_calls_after_terminal);

    let outcome_a = waiter_a.wait().await.expect("waiter A terminal");
    let outcome_b = waiter_b.wait().await.expect("waiter B terminal");
    let late = child.waiter().wait().await.expect("late waiter terminal");
    assert_eq!(outcome_a.code(), Some(7));
    assert_eq!(outcome_a, outcome_b);
    assert_eq!(outcome_a, late);
    assert_eq!(child.try_wait(), Some(Ok(outcome_a)));
    child.disarm();
    drop(child);
    reaper.shutdown().await.expect("terminal namespace closes");
}

#[tokio::test]
async fn wait_ownership_loss_wakes_every_waiter_and_is_retained_by_shutdown() {
    let (reaper, kernel) = fake_reaper();
    let child = reaper
        .adopt_pid_for_test(41_002, RuntimeDropAction::KeepAlive)
        .await
        .expect("admit fake child");
    let identity = child.identity_for_test();
    let waiter_a = child.waiter();
    let waiter_b = child.waiter();
    kernel.push_wait(identity, KernelWaitObservation::Error(libc::ECHILD));

    let expected = ReaperInvariant::WaitOwnershipLost { identity };
    assert_eq!(
        reaper.poll(identity),
        ReaperPollResult::Published(Err(expected))
    );
    assert_eq!(waiter_a.wait().await, Err(expected));
    assert_eq!(waiter_b.wait().await, Err(expected));
    assert_eq!(child.try_wait(), Some(Err(expected)));
    drop(child);
    assert_eq!(
        reaper.shutdown().await,
        Err(ReaperShutdownError::ReaperFailures(vec![(
            identity, expected
        )]))
    );
    assert_eq!(reaper.registration_count_for_test(), 1);
}

#[tokio::test]
async fn keep_alive_drop_never_signals_and_hard_kill_drop_signals_once_then_reaps_once() {
    let (keep_reaper, keep_kernel) = fake_reaper();
    let keep = keep_reaper
        .adopt_pid_for_test(41_003, RuntimeDropAction::KeepAlive)
        .await
        .expect("admit keep-alive child");
    let keep_identity = keep.identity_for_test();
    drop(keep);
    assert!(keep_kernel.signal_identities().is_empty());
    keep_kernel.push_wait(keep_identity, KernelWaitObservation::Exited(0));
    assert!(matches!(
        keep_reaper.poll(keep_identity),
        ReaperPollResult::Published(Ok(_))
    ));
    keep_reaper
        .shutdown()
        .await
        .expect("reaped keep-alive child");

    let (kill_reaper, kill_kernel) = fake_reaper();
    let kill = kill_reaper
        .adopt_pid_for_test(41_004, RuntimeDropAction::HardKill(KillTarget::Child))
        .await
        .expect("admit kill child");
    let kill_identity = kill.identity_for_test();
    drop(kill);
    assert_eq!(kill_kernel.signal_identities(), vec![kill_identity]);
    assert_eq!(
        kill_reaper.shutdown().await,
        Err(ReaperShutdownError::LiveRegistrations(vec![kill_identity]))
    );
    kill_kernel.push_wait(kill_identity, KernelWaitObservation::Exited(libc::SIGKILL));
    assert!(matches!(
        kill_reaper.poll(kill_identity),
        ReaperPollResult::Published(Ok(_))
    ));
    assert_eq!(
        kill_reaper.poll(kill_identity),
        ReaperPollResult::UnknownRegistration(kill_identity)
    );
    assert_eq!(kill_kernel.signal_identities(), vec![kill_identity]);
    kill_reaper
        .shutdown()
        .await
        .expect("reaped hard-killed child");
}

#[tokio::test]
async fn live_registration_retains_supervisor_after_every_facade_and_owner_drop() {
    let (reaper, kernel) = fake_reaper();
    let probe = reaper.retention_probe_for_test();
    let child = reaper
        .adopt_pid_for_test(41_010, RuntimeDropAction::KeepAlive)
        .await
        .expect("admit retained keep-alive child");
    let identity = child.identity_for_test();

    drop(child);
    drop(reaper);
    assert!(
        probe.is_retained(),
        "the live registration must outlive all facades and its dropped owner"
    );

    let retained = probe
        .reaper_for_test()
        .expect("registration-held supervisor lease");
    kernel.push_wait(identity, KernelWaitObservation::Exited(0));
    assert!(matches!(
        retained.poll(identity),
        ReaperPollResult::Published(Ok(_))
    ));
    retained
        .shutdown()
        .await
        .expect("terminal removal releases retained supervisor lease");
    drop(retained);
    assert!(!probe.is_retained());
}

#[tokio::test]
async fn signal_esrch_or_failure_never_fabricates_terminal_or_retries_action() {
    for (pid, signal_result, expected_action) in [
        (
            41_005,
            KernelSignalObservation::AlreadyGone,
            DirectChildActionResult::AlreadyGone,
        ),
        (
            41_006,
            KernelSignalObservation::Error(libc::EPERM),
            DirectChildActionResult::Failed { errno: libc::EPERM },
        ),
    ] {
        let (reaper, kernel) = fake_reaper();
        let child = reaper
            .adopt_pid_for_test(pid, RuntimeDropAction::HardKill(KillTarget::Child))
            .await
            .expect("admit fake child");
        let identity = child.identity_for_test();
        kernel.push_signal(identity, signal_result);
        assert_eq!(
            child.drop_action_result_for_test(),
            DirectChildActionResult::SkippedDisarmed
        );
        drop(child);
        assert_eq!(kernel.signal_identities(), vec![identity]);
        assert_eq!(
            reaper.drop_action_result_for_test(identity),
            Some(expected_action)
        );
        assert_eq!(
            reaper.shutdown().await,
            Err(ReaperShutdownError::LiveRegistrations(vec![identity]))
        );
        assert_eq!(kernel.signal_identities(), vec![identity]);
        kernel.push_wait(identity, KernelWaitObservation::Exited(0));
        assert!(matches!(
            reaper.poll(identity),
            ReaperPollResult::Published(Ok(_))
        ));
        reaper.shutdown().await.expect("terminal retry closes");
    }
}

#[tokio::test]
async fn terminal_before_drop_disarms_child_and_same_numeric_pid_reuse_is_generation_bound() {
    let (reaper, kernel) = fake_reaper();
    let child_a = reaper
        .adopt_pid_for_test(41_007, RuntimeDropAction::HardKill(KillTarget::Child))
        .await
        .expect("admit A");
    let identity_a = child_a.identity_for_test();
    kernel.push_wait(identity_a, KernelWaitObservation::Exited(0));
    assert!(matches!(
        reaper.poll(identity_a),
        ReaperPollResult::Published(Ok(_))
    ));

    let child_b = reaper
        .adopt_pid_for_test(41_007, RuntimeDropAction::HardKill(KillTarget::Child))
        .await
        .expect("admit reused numeric PID B");
    let identity_b = child_b.identity_for_test();
    assert_eq!(identity_a.pid_for_test(), identity_b.pid_for_test());
    assert_ne!(
        identity_a.generation_for_test(),
        identity_b.generation_for_test()
    );

    drop(child_a);
    assert!(kernel.signal_identities().is_empty());
    drop(child_b);
    assert_eq!(kernel.signal_identities(), vec![identity_b]);
    kernel.push_wait(identity_b, KernelWaitObservation::Exited(libc::SIGKILL));
    assert!(matches!(
        reaper.poll(identity_b),
        ReaperPollResult::Published(Ok(_))
    ));
    reaper.shutdown().await.expect("both generations collected");
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BarrierMode {
    None,
    BlockWait,
    BlockSignal,
}

struct BarrierKernelState {
    mode: BarrierMode,
    entered: bool,
    released: bool,
    wait_result: KernelWaitObservation,
    events: Vec<KernelEvent>,
}

struct BarrierKernel {
    state: Mutex<BarrierKernelState>,
    changed: Condvar,
}

impl BarrierKernel {
    fn new() -> Self {
        Self {
            state: Mutex::new(BarrierKernelState {
                mode: BarrierMode::None,
                entered: false,
                released: false,
                wait_result: KernelWaitObservation::Running,
                events: Vec::new(),
            }),
            changed: Condvar::new(),
        }
    }

    fn arm(&self, mode: BarrierMode, wait_result: KernelWaitObservation) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.mode = mode;
        state.entered = false;
        state.released = false;
        state.wait_result = wait_result;
        state.events.clear();
    }

    fn wait_until_entered(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        while !state.entered {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    fn release(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.released = true;
        self.changed.notify_all();
    }

    fn events(&self) -> Vec<KernelEvent> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .events
            .clone()
    }
}

impl UnixChildKernel for BarrierKernel {
    fn wait_nonblocking(&self, identity: RegistrationIdentity) -> KernelWaitObservation {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.events.push(KernelEvent::Wait(identity));
        if state.mode == BarrierMode::BlockWait {
            state.entered = true;
            self.changed.notify_all();
            while !state.released {
                state = self
                    .changed
                    .wait(state)
                    .unwrap_or_else(PoisonError::into_inner);
            }
        }
        state.wait_result
    }

    fn wait_proven_exit(&self, _identity: RegistrationIdentity) -> KernelWaitObservation {
        panic!("Linux shutdown-barrier fixture must not take a macOS proven wait")
    }

    fn signal_child(
        &self,
        identity: RegistrationIdentity,
        _signal: libc::c_int,
    ) -> KernelSignalObservation {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.events.push(KernelEvent::Signal(identity));
        if state.mode == BarrierMode::BlockSignal {
            state.entered = true;
            self.changed.notify_all();
            while !state.released {
                state = self
                    .changed
                    .wait(state)
                    .unwrap_or_else(PoisonError::into_inner);
            }
        }
        KernelSignalObservation::Delivered
    }
}

#[tokio::test]
async fn hidden_cleanup_action_pending_cannot_be_polled_before_the_hard_action_receipt() {
    let kernel = Arc::new(BarrierKernel::new());
    let (reaper, notifier) = SharedUnixReaper::initialize_with_test_linux_notifier(kernel.clone());
    let permit = reaper
        .admit_spawn(RuntimeDropAction::KeepAlive)
        .expect("admit while fake notifier is live");
    notifier.close();
    wait_for_notifier_invariant(&reaper).await;
    kernel.arm(
        BarrierMode::BlockSignal,
        KernelWaitObservation::Exited(libc::SIGKILL),
    );

    let commit_thread = std::thread::spawn(move || commit_test_pid_blocking(permit, 40_911));
    kernel.wait_until_entered();
    tokio::time::sleep(Duration::from_millis(75)).await;
    let action_pending_events = kernel.events();
    assert_eq!(action_pending_events.len(), 1);
    let KernelEvent::Signal(identity) = action_pending_events[0] else {
        panic!("ActionPending may contain only the one hard signal")
    };

    kernel.release();
    let commit_result =
        tokio::task::spawn_blocking(move || commit_thread.join().expect("commit thread"))
            .await
            .expect("join commit waiter");
    let error = match commit_result {
        Ok(_) => panic!("lost notifier cannot publish RuntimeChild"),
        Err(error) => error,
    };
    assert_eq!(
        error,
        ReaperCommitError::NotifierLost {
            identity,
            original_error: ReaperNotifierInvariant::StreamClosed,
            action_result: DirectChildActionResult::Delivered,
        }
    );
    assert_eq!(
        kernel.events(),
        vec![KernelEvent::Signal(identity), KernelEvent::Wait(identity)]
    );
    assert_eq!(reaper.registration_count_for_test(), 0);
    assert_eq!(
        reaper.shutdown().await,
        Err(ReaperShutdownError::NotifierInvariant(
            ReaperNotifierInvariant::StreamClosed
        ))
    );
}

#[tokio::test]
async fn wait_wins_race_under_lifecycle_mutex_and_suppresses_stale_drop_signal() {
    let kernel = Arc::new(BarrierKernel::new());
    let reaper = SharedUnixReaper::ready_with_kernel_for_test(kernel.clone());
    let child = reaper
        .adopt_pid_for_test(41_008, RuntimeDropAction::HardKill(KillTarget::Child))
        .await
        .expect("admit child");
    let identity = child.identity_for_test();
    kernel.arm(BarrierMode::BlockWait, KernelWaitObservation::Exited(0));

    let poll_reaper = reaper.clone();
    let poll_thread = std::thread::spawn(move || poll_reaper.poll(identity));
    kernel.wait_until_entered();
    let (drop_done_tx, drop_done_rx) = std::sync::mpsc::channel();
    let drop_thread = std::thread::spawn(move || {
        drop(child);
        drop_done_tx.send(()).expect("report child drop");
    });
    assert_eq!(
        drop_done_rx.recv_timeout(Duration::from_millis(25)),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout),
        "drop must wait behind the registration lifecycle mutex"
    );
    kernel.release();
    assert!(matches!(
        poll_thread.join().expect("poll thread"),
        ReaperPollResult::Published(Ok(_))
    ));
    drop_done_rx
        .recv()
        .expect("drop completes after publication");
    drop_thread.join().expect("drop thread");
    assert_eq!(kernel.events(), vec![KernelEvent::Wait(identity)]);
}

#[tokio::test]
async fn kill_wins_race_under_lifecycle_mutex_before_reaper_wait() {
    let kernel = Arc::new(BarrierKernel::new());
    let reaper = SharedUnixReaper::ready_with_kernel_for_test(kernel.clone());
    let child = reaper
        .adopt_pid_for_test(41_009, RuntimeDropAction::HardKill(KillTarget::Child))
        .await
        .expect("admit child");
    let identity = child.identity_for_test();
    kernel.arm(
        BarrierMode::BlockSignal,
        KernelWaitObservation::Exited(libc::SIGKILL),
    );

    let drop_thread = std::thread::spawn(move || drop(child));
    kernel.wait_until_entered();
    let poll_reaper = reaper.clone();
    let (poll_done_tx, poll_done_rx) = std::sync::mpsc::channel();
    let poll_thread = std::thread::spawn(move || {
        let result = poll_reaper.poll(identity);
        poll_done_tx.send(result).expect("report poll result");
    });
    assert_eq!(
        poll_done_rx.recv_timeout(Duration::from_millis(25)),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout),
        "wait must remain behind the in-flight signal critical section"
    );
    kernel.release();
    drop_thread.join().expect("drop thread");
    assert!(matches!(
        poll_done_rx.recv().expect("poll completes"),
        ReaperPollResult::Published(Ok(_))
    ));
    poll_thread.join().expect("poll thread");
    assert_eq!(
        kernel.events(),
        vec![KernelEvent::Signal(identity), KernelEvent::Wait(identity)]
    );
}

#[tokio::test]
async fn shutdown_waits_existing_permit_rejects_new_admission_and_never_invents_signal() {
    let (reaper, _kernel) = fake_reaper();
    let permit = reaper
        .admit_spawn(RuntimeDropAction::KeepAlive)
        .expect("admit in-flight spawn");
    assert_eq!(reaper.active_spawn_permits_for_test(), 1);

    let shutdown_reaper = reaper.clone();
    let shutdown = tokio::spawn(async move { shutdown_reaper.shutdown().await });
    tokio::task::yield_now().await;
    assert!(!shutdown.is_finished());
    assert!(matches!(
        reaper.admit_spawn(RuntimeDropAction::KeepAlive),
        Err(ReaperAdmissionError::SupervisorDraining)
    ));
    drop(permit);
    assert_eq!(shutdown.await.expect("shutdown task"), Ok(()));
}

#[test]
fn shutdown_permit_barrier_covers_commit_immediate_wait_and_cache_publication() {
    let kernel = Arc::new(BarrierKernel::new());
    let reaper = SharedUnixReaper::ready_with_kernel_for_test(kernel.clone());
    let permit = reaper
        .admit_spawn(RuntimeDropAction::KeepAlive)
        .expect("admit pre-shutdown spawn");
    kernel.arm(BarrierMode::BlockWait, KernelWaitObservation::Exited(0));

    let commit_thread = std::thread::spawn(move || {
        commit_test_pid_blocking(permit, 41_011).expect("ready notifier publishes child")
    });
    kernel.wait_until_entered();
    let active_during_immediate_scan = reaper.active_spawn_permits_for_test();
    let shutdown_reaper = reaper.clone();
    let shutdown_thread = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("shutdown runtime")
            .block_on(shutdown_reaper.shutdown())
    });

    kernel.release();
    let mut child = commit_thread.join().expect("commit thread");
    let shutdown_result = shutdown_thread.join().expect("shutdown thread");
    child.disarm();
    drop(child);

    assert_eq!(
        active_during_immediate_scan, 1,
        "the admitted permit must remain active through commit's mandatory immediate scan"
    );
    assert_eq!(shutdown_result, Ok(()));
}

#[tokio::test]
async fn every_outstanding_spawn_permit_has_distinct_pre_reserved_map_capacity() {
    const PERMIT_COUNT: usize = 64;

    let (reaper, kernel) = fake_reaper();
    let mut permits = Vec::with_capacity(PERMIT_COUNT);
    for _ in 0..PERMIT_COUNT {
        permits.push(
            reaper
                .admit_spawn(RuntimeDropAction::KeepAlive)
                .expect("reserve concurrent spawn slot"),
        );
        assert!(
            reaper.registration_capacity_for_test() >= reaper.active_spawn_permits_for_test(),
            "capacity must cover every outstanding permit, not only one shared spare slot"
        );
    }
    let capacity_before_commit = reaper.registration_capacity_for_test();

    let mut children = Vec::with_capacity(PERMIT_COUNT);
    for (index, permit) in permits.into_iter().enumerate() {
        children.push(
            permit
                .commit_test_pid(42_000 + index as libc::pid_t)
                .await
                .expect("ready notifier publishes child"),
        );
    }
    assert_eq!(reaper.active_spawn_permits_for_test(), 0);
    assert_eq!(reaper.registration_count_for_test(), PERMIT_COUNT);
    assert_eq!(
        reaper.registration_capacity_for_test(),
        capacity_before_commit,
        "non-fallible commit must not grow the registration map after child ownership transfer"
    );

    for child in &mut children {
        let identity = child.identity_for_test();
        kernel.push_wait(identity, KernelWaitObservation::Exited(0));
        assert!(matches!(
            reaper.poll(identity),
            ReaperPollResult::Published(Ok(_))
        ));
        child.disarm();
    }
    drop(children);
    reaper
        .shutdown()
        .await
        .expect("all reserved registrations retired");
}

#[tokio::test]
async fn missing_platform_notifier_and_unsupported_policies_fail_before_descriptor_or_fork() {
    let spawner = SharedRuntimeSpawner::initialize_shadow();
    let mut control = UnixSpawnTestControl::default();
    let result = spawner
        .spawn_rust193_fork_with_test_control(
            rust_image("/bin/true", &[]),
            pipe_stdio(),
            SharedSpawnPolicy::rust193_inherit(RuntimeDropAction::KeepAlive),
            &mut control,
        )
        .await;
    assert!(matches!(
        result,
        Err(SharedSpawnError::Reaper(
            ReaperAdmissionError::PlatformNotifierUnavailable
        ))
    ));
    assert_eq!(control.descriptor_actions(), 0);
    assert_eq!(control.fork_calls(), 0);

    let mut node_control = UnixSpawnTestControl::default();
    let node_result = spawner
        .spawn_rust193_fork_with_test_control(
            node_image("/bin/true"),
            pipe_stdio(),
            SharedSpawnPolicy::rust193_inherit(RuntimeDropAction::KeepAlive),
            &mut node_control,
        )
        .await;
    assert!(matches!(
        node_result,
        Err(SharedSpawnError::Policy(
            SharedSpawnPolicyError::ImageDialectMismatch {
                image: ProcessImageDialect::Node20Libuv146,
                requested: ProcessImageDialect::Rust193,
            }
        ))
    ));
    assert_eq!(node_control.descriptor_actions(), 0);
    assert_eq!(node_control.fork_calls(), 0);

    let mut topology_control = UnixSpawnTestControl::default();
    let topology_result = spawner
        .spawn_rust193_fork_with_test_control(
            rust_image("/bin/true", &[]),
            pipe_stdio(),
            SharedSpawnPolicy::for_test(
                ProcessImageDialect::Rust193,
                ProcessTopology::UnixNewSession,
                RuntimeDropAction::HardKill(KillTarget::UnixProcessGroup),
            ),
            &mut topology_control,
        )
        .await;
    assert!(matches!(
        topology_result,
        Err(SharedSpawnError::Policy(
            SharedSpawnPolicyError::UnsupportedTopology {
                requested: ProcessTopology::UnixNewSession,
            }
        ))
    ));
    assert_eq!(topology_control.descriptor_actions(), 0);
    assert_eq!(topology_control.fork_calls(), 0);

    let mut group_control = UnixSpawnTestControl::default();
    let group_result = spawner
        .spawn_rust193_fork_with_test_control(
            rust_image("/bin/true", &[]),
            pipe_stdio(),
            SharedSpawnPolicy::for_test(
                ProcessImageDialect::Rust193,
                ProcessTopology::Inherit,
                RuntimeDropAction::HardKill(KillTarget::UnixProcessGroup),
            ),
            &mut group_control,
        )
        .await;
    assert!(matches!(
        group_result,
        Err(SharedSpawnError::Policy(
            SharedSpawnPolicyError::UnsupportedDropTarget {
                target: KillTarget::UnixProcessGroup,
            }
        ))
    ));
    assert_eq!(group_control.descriptor_actions(), 0);
    assert_eq!(group_control.fork_calls(), 0);
}

#[tokio::test]
async fn exec_failure_closes_parent_endpoints_reaps_unpublished_child_then_releases_permit() {
    let reaper = SharedUnixReaper::ready_with_libc_for_test();
    let spawner = SharedRuntimeSpawner::with_reaper_for_test(reaper.clone());
    let mut control = UnixSpawnTestControl::default();
    let result = spawner
        .spawn_rust193_fork_with_test_control(
            rust_image("/definitely/not/a/klaus-executable", &[]),
            pipe_stdio(),
            SharedSpawnPolicy::rust193_inherit(RuntimeDropAction::KeepAlive),
            &mut control,
        )
        .await;

    assert!(matches!(
        result,
        Err(SharedSpawnError::Unix(
            UnixShadowSpawnError::ChildExec { .. }
        ))
    ));
    assert_eq!(control.fork_calls(), 1);
    assert!(control.exec_failure_pid().is_some());
    assert!(control.exec_failure_parent_endpoints_closed_before_wait());
    assert_eq!(control.exec_failure_wait_successes(), 1);
    assert_eq!(reaper.active_spawn_permits_for_test(), 0);
    assert_eq!(reaper.registration_count_for_test(), 0);
}

#[tokio::test]
async fn rust193_unpublished_spawn_commits_to_shared_reaper_and_only_it_waits() {
    let reaper = SharedUnixReaper::ready_with_libc_for_test();
    let spawner = SharedRuntimeSpawner::with_reaper_for_test(reaper.clone());
    let spawned = spawner
        .spawn_rust193_fork_shadow(
            rust_image("/bin/sh", &["-c", "exit 9"]),
            inherit_stdio(),
            SharedSpawnPolicy::rust193_inherit(RuntimeDropAction::KeepAlive),
        )
        .await
        .expect("shared Rust193 fork spawn");
    let (mut child, stdio) = spawned.into_parts();
    assert!(stdio.is_null_or_inherited());
    let identity = child.identity_for_test();
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(outcome) = child.try_wait() {
            assert_eq!(outcome.expect("cached terminal").code(), Some(9));
            break;
        }
        match reaper.poll(identity) {
            ReaperPollResult::Running if Instant::now() < deadline => std::thread::yield_now(),
            ReaperPollResult::Running => panic!("child did not become terminal"),
            ReaperPollResult::Published(Ok(status)) => {
                assert_eq!(status.code(), Some(9));
                break;
            }
            ReaperPollResult::UnknownRegistration(_) if child.try_wait().is_some() => continue,
            other => panic!("unexpected shared reaper result: {other:?}"),
        }
    }
    assert_eq!(
        child.try_wait().expect("cached outcome").unwrap().code(),
        Some(9)
    );
    child.disarm();
    drop(child);
    reaper
        .shutdown()
        .await
        .expect("shared registration cleaned");
}

#[test]
fn source_guards_forbid_global_wait_and_per_child_driver_threads() {
    let linux_notifier_source = include_str!("linux_notifier.rs");
    let macos_notifier_source = include_str!("macos_notifier.rs");
    let reaper_source = include_str!("reaper.rs");
    let spawn_source = include_str!("spawn.rs");
    let unix_spawn_source = include_str!("unix_spawn.rs");
    assert!(!reaper_source.contains("waitpid(-1"));
    assert!(!linux_notifier_source.contains("waitpid(-1"));
    assert!(!reaper_source.contains("thread::spawn"));
    assert!(!linux_notifier_source.contains("thread::spawn"));
    assert!(!macos_notifier_source.contains("thread::spawn"));
    assert!(!reaper_source.contains("tokio::spawn"));
    assert!(!linux_notifier_source.contains("tokio::spawn"));
    assert_eq!(
        linux_notifier_source.matches(".spawn(run_worker(").count(),
        1
    );
    assert_eq!(
        reaper_source
            .matches(".spawn(run_macos_notifier_worker(")
            .count(),
        1
    );
    assert_eq!(
        reaper_source
            .matches("tokio::time::interval(Duration::from_millis(25))")
            .count(),
        1,
        "one long-lived macOS worker owns the sole hidden-cleanup scheduler"
    );
    assert!(reaper_source.contains("Arc::new(ProductionMacOsKqueue::initialize()?)"));
    assert!(reaper_source.contains("MacOsEventSource::Production(Arc::clone(&kqueue))"));
    assert!(!macos_notifier_source.contains("ProductionMacOsKqueueFactory"));
    assert!(linux_notifier_source.contains("libc::SYS_pidfd_open"));
    assert!(linux_notifier_source.contains("libc::SYS_pidfd_send_signal"));
    assert!(linux_notifier_source.contains("libc::O_NONBLOCK"));
    assert!(linux_notifier_source.contains("libc::FD_CLOEXEC"));
    assert!(linux_notifier_source.contains("runtime: &Handle"));
    assert!(linux_notifier_source.contains("let _runtime_guard = runtime.enter();"));
    assert!(!reaper_source.contains("RuntimeChild::from_pid"));
    assert!(reaper_source.contains("waitpid(identity.pid"));
    assert!(reaper_source.contains("kill(identity.pid"));
    assert!(unix_spawn_source.contains("#[cfg(test)]\npub(super) struct ShadowRuntimeChild"));
    assert!(!unix_spawn_source.contains("RuntimeChild::from_pid"));
    assert!(unix_spawn_source.contains("_admission: UnixSpawnAdmission<'_>"));
    assert_eq!(spawn_source.matches("spawn_unregistered(").count(), 2);
    let admitted_routes = spawn_source.matches(".admit_spawn_with_target(").count();
    assert_eq!(admitted_routes, 7);
    assert_eq!(
        spawn_source.matches(".authorize_unix_spawn()").count(),
        admitted_routes,
        "every admitted route must authorize exactly one backend spawn"
    );
    assert_eq!(
        spawn_source.matches(".commit_unix(").count(),
        admitted_routes,
        "every admitted route must retain the shared-reaper commit path"
    );
    assert_eq!(
        spawn_source.matches(".cleanup_unpublished_unix(").count(),
        admitted_routes,
        "every route hidden-cleans if typed Tokio stdio attachment fails"
    );
}
