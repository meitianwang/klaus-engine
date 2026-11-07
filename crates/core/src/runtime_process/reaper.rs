//! Project-owned shared Unix child ownership and reaping shadow.
//!
//! Linux initializes the frozen SIGCHLD/pidfd notifier before admission, while
//! macOS initializes one shared kqueue control/event allocation and one
//! long-lived worker.
//! The spawn adapter itself remains a shadow with no production command
//! callsite, and this layer never substitutes global polling or a thread per
//! child.

use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::os::unix::process::ExitStatusExt;
#[cfg(any(target_os = "macos", test))]
use std::panic::AssertUnwindSafe;
use std::process::{abort, ExitStatus};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Weak;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

#[cfg(any(target_os = "macos", test))]
use futures::FutureExt;
use libc::{c_int, pid_t};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use tokio::runtime::Handle;
#[cfg(any(target_os = "macos", test))]
use tokio::sync::oneshot;
use tokio::sync::{watch, Notify};

#[cfg(any(target_os = "linux", test))]
use super::linux_notifier::LinuxNotifierInitializationError;
#[cfg(test)]
use super::linux_notifier::TestLinuxNotifierHandle;
#[cfg(any(target_os = "linux", test))]
use super::linux_notifier::{
    LinuxNotifierWorkerHandle, PidfdReadyFuture, PreparedLinuxNotifier, PreparedPidfd,
};
#[cfg(target_os = "macos")]
use super::macos_notifier::ProductionMacOsKqueue;
#[cfg(any(target_os = "macos", test))]
use super::macos_notifier::{
    HiddenLifecycleHandle, HiddenLifecyclePhase, HiddenRegistrationIdentity, KqueueDriver,
    KqueueEvent, KqueueEventDisposition, KqueueExitProof, KqueueWatchId, MacOsNotifier,
    MacOsNotifierInitializationError, MacOsPrepareWatchError, MacOsProcessIdentity,
    PendingKqueueWatch, ProvenExitCollector, SharedRetainedHiddenScheduler,
};
use super::unix_spawn::UnregisteredUnixChild;
use super::KillTarget;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct RegistrationIdentity {
    generation: u64,
    pid: pid_t,
}

impl RegistrationIdentity {
    pub(super) const fn pid_for_kernel(self) -> pid_t {
        self.pid
    }

    #[cfg(test)]
    pub(super) const fn generation_for_test(self) -> u64 {
        self.generation
    }

    #[cfg(test)]
    pub(super) const fn pid_for_test(self) -> pid_t {
        self.pid
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct SharedExitStatus(ExitStatus);

impl SharedExitStatus {
    pub(super) fn code(self) -> Option<i32> {
        self.0.code()
    }

    pub(super) fn signal(self) -> Option<i32> {
        self.0.signal()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ReaperInvariant {
    WaitFailed {
        identity: RegistrationIdentity,
        errno: c_int,
    },
    WaitOwnershipLost {
        identity: RegistrationIdentity,
    },
    WrongWaitIdentity {
        expected: RegistrationIdentity,
        observed_pid: pid_t,
    },
    NotifierClosed {
        identity: RegistrationIdentity,
    },
    ProvenExitNotTerminal {
        identity: RegistrationIdentity,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ReaperNotifierInvariant {
    StreamClosed,
    WorkerLost,
    WorkerJoinFailed,
}

pub(super) type SharedWaitOutcome = Result<SharedExitStatus, ReaperInvariant>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RuntimeDropAction {
    KeepAlive,
    HardKill(KillTarget),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DirectChildActionResult {
    Delivered,
    AlreadyGone,
    Failed {
        errno: c_int,
    },
    ProcessGroup {
        group: SignalActionResult,
        direct_child_fallback: Option<SignalActionResult>,
    },
    SkippedTerminal,
    SkippedDisarmed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SignalActionResult {
    Delivered,
    AlreadyGone,
    Failed { errno: c_int },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ReaperAdmissionError {
    PlatformNotifierUnavailable,
    SupervisorDraining,
    GenerationExhausted,
    SpawnPermitCountExhausted,
    CapacityReservationFailed,
    UnsupportedDropTarget { target: KillTarget },
}

impl fmt::Display for ReaperAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PlatformNotifierUnavailable => formatter.write_str(
                "shared Unix reaper has no initialized SIGCHLD/pidfd or kqueue notifier",
            ),
            Self::SupervisorDraining => {
                formatter.write_str("shared Unix reaper is draining and rejects new spawns")
            }
            Self::GenerationExhausted => {
                formatter.write_str("shared Unix reaper registration generation exhausted")
            }
            Self::SpawnPermitCountExhausted => {
                formatter.write_str("shared Unix reaper spawn permit count exhausted")
            }
            Self::CapacityReservationFailed => {
                formatter.write_str("shared Unix reaper could not reserve registration capacity")
            }
            Self::UnsupportedDropTarget { target } => {
                write!(formatter, "unsupported shadow drop target: {target:?}")
            }
        }
    }
}

impl std::error::Error for ReaperAdmissionError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum ReaperPollResult {
    Running,
    Published(SharedWaitOutcome),
    Cached(SharedWaitOutcome),
    UnknownRegistration(RegistrationIdentity),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ReaperCommitError {
    NotifierLost {
        identity: RegistrationIdentity,
        original_error: ReaperNotifierInvariant,
        action_result: DirectChildActionResult,
    },
    ProcessStillLive {
        identity: RegistrationIdentity,
        original_error: ReaperNotifierInvariant,
        action_result: DirectChildActionResult,
    },
    RetainedInvariant {
        identity: RegistrationIdentity,
        original_error: ReaperNotifierInvariant,
        action_result: DirectChildActionResult,
        reaper_error: ReaperInvariant,
    },
    #[cfg(any(target_os = "macos", test))]
    MacOsAttachment {
        identity: RegistrationIdentity,
        original_error: MacOsPrepareWatchError,
        action_result: DirectChildActionResult,
    },
    #[cfg(any(target_os = "macos", test))]
    MacOsAttachmentProcessStillLive {
        identity: RegistrationIdentity,
        original_error: MacOsPrepareWatchError,
        action_result: DirectChildActionResult,
    },
    #[cfg(any(target_os = "macos", test))]
    MacOsAttachmentRetainedInvariant {
        identity: RegistrationIdentity,
        original_error: MacOsPrepareWatchError,
        action_result: DirectChildActionResult,
        reaper_error: ReaperInvariant,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum UnpublishedUnixCleanupOutcome {
    Reaped {
        identity: RegistrationIdentity,
        action_result: DirectChildActionResult,
    },
    ProcessStillLive {
        identity: RegistrationIdentity,
        action_result: DirectChildActionResult,
    },
    RetainedInvariant {
        identity: RegistrationIdentity,
        action_result: DirectChildActionResult,
        reaper_error: ReaperInvariant,
    },
}

const HIDDEN_CLEANUP_SETTLE: Duration = Duration::from_millis(2_000);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum ReaperShutdownError {
    LiveRegistrations(Vec<RegistrationIdentity>),
    ReaperFailures(Vec<(RegistrationIdentity, ReaperInvariant)>),
    NotifierInvariant(ReaperNotifierInvariant),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum KernelWaitObservation {
    Running,
    Exited(c_int),
    Error(c_int),
    WrongPid(pid_t),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum KernelSignalObservation {
    Delivered,
    AlreadyGone,
    Error(c_int),
}

/// Opaque kernel capability used only while a generation-bound registration
/// owns the wait right. There is intentionally no API taking a caller PID.
pub(super) trait UnixChildKernel: Send + Sync + 'static {
    fn wait_nonblocking(&self, identity: RegistrationIdentity) -> KernelWaitObservation;

    fn wait_proven_exit(&self, identity: RegistrationIdentity) -> KernelWaitObservation;

    fn signal_child(
        &self,
        identity: RegistrationIdentity,
        signal: c_int,
    ) -> KernelSignalObservation;

    fn signal_process_group(
        &self,
        identity: RegistrationIdentity,
        signal: c_int,
    ) -> KernelSignalObservation {
        let _ = (identity, signal);
        abort()
    }

    #[cfg(any(target_os = "linux", test))]
    fn prepare_pidfd(&self, identity: RegistrationIdentity) -> Option<PreparedPidfd> {
        let _ = identity;
        None
    }

    #[cfg(test)]
    fn after_commit_initial_poll(&self, identity: RegistrationIdentity) {
        let _ = identity;
    }

    #[cfg(test)]
    fn before_macos_registration_map_insert(&self, identity: RegistrationIdentity) {
        let _ = identity;
    }

    #[cfg(test)]
    fn before_macos_deferred_collection(&self, identity: RegistrationIdentity) {
        let _ = identity;
    }
}

struct LibcUnixChildKernel {
    #[cfg(target_os = "linux")]
    notifier_runtime: Option<Handle>,
}

impl LibcUnixChildKernel {
    fn without_linux_notifier() -> Self {
        Self {
            #[cfg(target_os = "linux")]
            notifier_runtime: None,
        }
    }

    #[cfg(target_os = "linux")]
    fn with_linux_notifier(runtime: Handle) -> Self {
        Self {
            notifier_runtime: Some(runtime),
        }
    }
}

impl UnixChildKernel for LibcUnixChildKernel {
    fn wait_nonblocking(&self, identity: RegistrationIdentity) -> KernelWaitObservation {
        let mut status = 0;
        loop {
            // The exact owned PID is mandatory. This module never performs a
            // process-global wait and therefore cannot steal an unrelated
            // std/Tokio/third-party child.
            let observed = unsafe { libc::waitpid(identity.pid, &mut status, libc::WNOHANG) };
            if observed == identity.pid {
                return KernelWaitObservation::Exited(status);
            }
            if observed == 0 {
                return KernelWaitObservation::Running;
            }
            if observed == -1 {
                let errno = last_errno();
                if errno == libc::EINTR {
                    continue;
                }
                return KernelWaitObservation::Error(errno);
            }
            return KernelWaitObservation::WrongPid(observed);
        }
    }

    fn wait_proven_exit(&self, identity: RegistrationIdentity) -> KernelWaitObservation {
        let mut status = 0;
        loop {
            let observed = unsafe { libc::waitpid(identity.pid, &mut status, 0) };
            if observed == identity.pid {
                return KernelWaitObservation::Exited(status);
            }
            if observed == -1 {
                let errno = last_errno();
                if errno == libc::EINTR {
                    continue;
                }
                return KernelWaitObservation::Error(errno);
            }
            if observed == 0 {
                return KernelWaitObservation::Running;
            }
            return KernelWaitObservation::WrongPid(observed);
        }
    }

    fn signal_child(
        &self,
        identity: RegistrationIdentity,
        signal: c_int,
    ) -> KernelSignalObservation {
        loop {
            let result = unsafe { libc::kill(identity.pid, signal) };
            if result == 0 {
                return KernelSignalObservation::Delivered;
            }
            let errno = last_errno();
            if errno == libc::EINTR {
                continue;
            }
            return if errno == libc::ESRCH {
                KernelSignalObservation::AlreadyGone
            } else {
                KernelSignalObservation::Error(errno)
            };
        }
    }

    fn signal_process_group(
        &self,
        identity: RegistrationIdentity,
        signal: c_int,
    ) -> KernelSignalObservation {
        let process_group = identity.pid.checked_neg().unwrap_or_else(|| abort());
        if process_group >= 0 {
            abort();
        }
        loop {
            let result = unsafe { libc::kill(process_group, signal) };
            if result == 0 {
                return KernelSignalObservation::Delivered;
            }
            let errno = last_errno();
            if errno == libc::EINTR {
                continue;
            }
            return if errno == libc::ESRCH {
                KernelSignalObservation::AlreadyGone
            } else {
                KernelSignalObservation::Error(errno)
            };
        }
    }

    #[cfg(any(target_os = "linux", test))]
    fn prepare_pidfd(&self, identity: RegistrationIdentity) -> Option<PreparedPidfd> {
        #[cfg(target_os = "linux")]
        {
            self.notifier_runtime
                .as_ref()
                .and_then(|runtime| PreparedPidfd::prepare_linux(identity, runtime))
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = identity;
            None
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SupervisorPhase {
    Running,
    Draining,
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NotifierReadiness {
    Missing,
    Initializing,
    Ready,
    ReadyForTests,
    Failed,
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NotifierBackend {
    Missing,
    Linux,
    MacOs,
    GenericTest,
}

struct SupervisorState {
    phase: SupervisorPhase,
    notifier: NotifierReadiness,
    notifier_backend: NotifierBackend,
    notifier_invariant: Option<ReaperNotifierInvariant>,
    next_generation: u64,
    active_spawn_permits: usize,
    permit_epoch: u64,
    registrations: HashMap<u64, Arc<Registration>>,
}

struct SupervisorInner {
    state: Mutex<SupervisorState>,
    permit_epoch_tx: watch::Sender<u64>,
    registration_notify: Arc<Notify>,
    #[cfg(any(target_os = "linux", test))]
    notifier_worker: Mutex<Option<LinuxNotifierWorkerHandle>>,
    #[cfg(any(target_os = "macos", test))]
    macos_notifier: Mutex<Option<Arc<MacOsNotifier>>>,
    #[cfg(any(target_os = "macos", test))]
    macos_notifier_worker: Mutex<Option<MacOsNotifierWorkerHandle>>,
    kernel: Arc<dyn UnixChildKernel>,
}

/// Cloneable facade over one shared namespace. `RuntimeChild` retains the
/// inner allocation, so dropping this facade cannot abandon a wait right.
#[derive(Clone)]
pub(super) struct SharedUnixReaper {
    inner: Arc<SupervisorInner>,
}

#[derive(Clone)]
pub(super) struct WeakSharedUnixReaper {
    inner: Weak<SupervisorInner>,
}

impl WeakSharedUnixReaper {
    pub(super) fn upgrade(&self) -> Option<SharedUnixReaper> {
        self.inner.upgrade().map(|inner| SharedUnixReaper { inner })
    }
}

#[cfg(test)]
pub(super) struct SupervisorRetentionProbe(Weak<SupervisorInner>);

#[cfg(test)]
impl SupervisorRetentionProbe {
    pub(super) fn is_retained(&self) -> bool {
        self.0.strong_count() != 0
    }

    pub(super) fn reaper_for_test(&self) -> Option<SharedUnixReaper> {
        self.0.upgrade().map(|inner| SharedUnixReaper { inner })
    }
}

#[cfg(any(target_os = "macos", test))]
struct MacOsReaperCollector {
    reaper: WeakSharedUnixReaper,
}

#[cfg(any(target_os = "macos", test))]
impl ProvenExitCollector for MacOsReaperCollector {
    fn collect_proven_exit(&self, identity: MacOsProcessIdentity, proof: KqueueExitProof) {
        let Some(reaper) = self.reaper.upgrade() else {
            abort();
        };
        reaper.collect_macos_proven_exit(identity, proof);
    }
}

#[cfg(any(target_os = "macos", test))]
enum MacOsEventSource {
    #[cfg(target_os = "macos")]
    Production(Arc<ProductionMacOsKqueue>),
    #[cfg(test)]
    Test {
        events: watch::Receiver<(u64, Vec<KqueueEvent>)>,
        acknowledged: watch::Sender<u64>,
        pending_epoch: Option<u64>,
    },
}

#[cfg(any(target_os = "macos", test))]
impl MacOsEventSource {
    #[cfg(test)]
    fn for_test() -> (Self, TestMacOsNotifierHandle) {
        let (events_tx, events_rx) = watch::channel((0, Vec::new()));
        let (acknowledged_tx, acknowledged_rx) = watch::channel(0);
        (
            Self::Test {
                events: events_rx,
                acknowledged: acknowledged_tx,
                pending_epoch: None,
            },
            TestMacOsNotifierHandle {
                events: Some(events_tx),
                acknowledged: acknowledged_rx,
            },
        )
    }

    async fn recv(&mut self) -> Result<Option<Vec<KqueueEvent>>, ()> {
        match self {
            #[cfg(target_os = "macos")]
            Self::Production(kqueue) => kqueue.next_events().await.map(Some).map_err(|_| ()),
            #[cfg(test)]
            Self::Test {
                events,
                pending_epoch,
                ..
            } => {
                if events.changed().await.is_err() {
                    return Ok(None);
                }
                let (epoch, batch) = events.borrow_and_update().clone();
                *pending_epoch = Some(epoch);
                Ok(Some(batch))
            }
        }
    }

    fn acknowledge(&mut self) {
        match self {
            #[cfg(target_os = "macos")]
            Self::Production(_) => {}
            #[cfg(test)]
            Self::Test {
                acknowledged,
                pending_epoch,
                ..
            } => {
                let epoch = pending_epoch
                    .take()
                    .expect("test macOS event batch must be pending");
                acknowledged.send_replace(epoch);
            }
        }
    }
}

#[cfg(test)]
pub(super) struct TestMacOsNotifierHandle {
    events: Option<watch::Sender<(u64, Vec<KqueueEvent>)>>,
    acknowledged: watch::Receiver<u64>,
}

#[cfg(test)]
impl TestMacOsNotifierHandle {
    pub(super) async fn notify_events_and_wait(
        &mut self,
        events: impl IntoIterator<Item = KqueueEvent>,
    ) {
        let batch = events.into_iter().collect::<Vec<_>>();
        assert!(!batch.is_empty(), "test kqueue batch must contain an event");
        let mut target = 0;
        self.events
            .as_ref()
            .expect("test macOS notifier is open")
            .send_modify(|(epoch, queued)| {
                *epoch = epoch.wrapping_add(1);
                target = *epoch;
                *queued = batch;
            });
        while *self.acknowledged.borrow_and_update() != target {
            self.acknowledged
                .changed()
                .await
                .expect("macOS worker acknowledges the exact event batch");
        }
    }

    pub(super) fn close(mut self) {
        self.events.take();
    }
}

#[cfg(any(target_os = "macos", test))]
struct MacOsNotifierWorkerHandle {
    shutdown_tx: Option<oneshot::Sender<()>>,
    join: Option<tokio::task::JoinHandle<()>>,
}

#[cfg(any(target_os = "macos", test))]
impl MacOsNotifierWorkerHandle {
    fn start(
        runtime: Handle,
        source: MacOsEventSource,
        notifier: Arc<MacOsNotifier>,
        reaper: WeakSharedUnixReaper,
        registration_notify: Arc<Notify>,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let exit_guard = MacOsWorkerExitGuard::new(reaper.clone());
        let join = runtime.spawn(run_macos_notifier_worker(
            source,
            notifier,
            reaper,
            registration_notify,
            shutdown_rx,
            exit_guard,
        ));
        Self {
            shutdown_tx: Some(shutdown_tx),
            join: Some(join),
        }
    }

    async fn shutdown(mut self) -> Result<(), ReaperNotifierInvariant> {
        if let Some(shutdown) = self.shutdown_tx.take() {
            let _ = shutdown.send(());
        }
        self.join
            .take()
            .expect("macOS notifier worker join handle")
            .await
            .map_err(|_| ReaperNotifierInvariant::WorkerJoinFailed)
    }
}

#[cfg(any(target_os = "macos", test))]
impl Drop for MacOsNotifierWorkerHandle {
    fn drop(&mut self) {
        if let Some(join) = self.join.take() {
            join.abort();
        }
    }
}

#[cfg(any(target_os = "macos", test))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MacOsActiveWorkerExit {
    Shutdown,
    ReaperGone,
    StreamClosed,
}

#[cfg(any(target_os = "macos", test))]
async fn run_macos_notifier_worker(
    mut source: MacOsEventSource,
    notifier: Arc<MacOsNotifier>,
    reaper: WeakSharedUnixReaper,
    registration_notify: Arc<Notify>,
    mut shutdown_rx: oneshot::Receiver<()>,
    mut exit_guard: MacOsWorkerExitGuard,
) {
    let mut hidden_cleanup_interval = tokio::time::interval(Duration::from_millis(25));
    hidden_cleanup_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let active = AssertUnwindSafe(run_active_macos_worker(
        &mut source,
        &notifier,
        &reaper,
        &registration_notify,
        &mut shutdown_rx,
        &mut hidden_cleanup_interval,
    ))
    .catch_unwind()
    .await;
    let failure = match active {
        Ok(MacOsActiveWorkerExit::Shutdown | MacOsActiveWorkerExit::ReaperGone) => {
            exit_guard.disarm();
            return;
        }
        Ok(MacOsActiveWorkerExit::StreamClosed) => ReaperNotifierInvariant::StreamClosed,
        Err(_) => ReaperNotifierInvariant::WorkerLost,
    };
    exit_guard.set_failure(failure);
    let Some(strong) = reaper.upgrade() else {
        exit_guard.disarm();
        return;
    };
    strong.publish_notifier_failure(failure);
    drop(strong);
    run_retained_macos_hidden_scheduler(
        notifier,
        reaper,
        registration_notify,
        shutdown_rx,
        hidden_cleanup_interval,
        exit_guard,
    )
    .await;
}

#[cfg(any(target_os = "macos", test))]
async fn run_active_macos_worker(
    source: &mut MacOsEventSource,
    notifier: &Arc<MacOsNotifier>,
    reaper: &WeakSharedUnixReaper,
    registration_notify: &Arc<Notify>,
    shutdown_rx: &mut oneshot::Receiver<()>,
    hidden_cleanup_interval: &mut tokio::time::Interval,
) -> MacOsActiveWorkerExit {
    loop {
        tokio::select! {
            _ = &mut *shutdown_rx => return MacOsActiveWorkerExit::Shutdown,
            batch = source.recv() => {
                let Ok(Some(batch)) = batch else {
                    return MacOsActiveWorkerExit::StreamClosed;
                };
                for event in batch {
                    let _ = notifier.dispatch_event(event);
                }
                if !run_macos_hidden_maintenance(notifier, reaper) {
                    return MacOsActiveWorkerExit::ReaperGone;
                }
                source.acknowledge();
            }
            _ = registration_notify.notified() => {
                if !run_macos_hidden_maintenance(notifier, reaper) {
                    return MacOsActiveWorkerExit::ReaperGone;
                }
            }
            _ = hidden_cleanup_interval.tick() => {
                if !run_macos_hidden_maintenance(notifier, reaper) {
                    return MacOsActiveWorkerExit::ReaperGone;
                }
            }
        }
    }
}

#[cfg(any(target_os = "macos", test))]
async fn run_retained_macos_hidden_scheduler(
    notifier: Arc<MacOsNotifier>,
    reaper: WeakSharedUnixReaper,
    registration_notify: Arc<Notify>,
    mut shutdown_rx: oneshot::Receiver<()>,
    mut hidden_cleanup_interval: tokio::time::Interval,
    mut exit_guard: MacOsWorkerExitGuard,
) {
    loop {
        tokio::select! {
            _ = &mut shutdown_rx => {
                exit_guard.disarm();
                return;
            }
            _ = registration_notify.notified() => {
                if !run_macos_hidden_maintenance(&notifier, &reaper) {
                    exit_guard.disarm();
                    return;
                }
            }
            _ = hidden_cleanup_interval.tick() => {
                if !run_macos_hidden_maintenance(&notifier, &reaper) {
                    exit_guard.disarm();
                    return;
                }
            }
        }
    }
}

#[cfg(any(target_os = "macos", test))]
fn run_macos_hidden_maintenance(notifier: &MacOsNotifier, reaper: &WeakSharedUnixReaper) -> bool {
    notifier.retry_all_hidden_retirements();
    let Some(strong) = reaper.upgrade() else {
        return false;
    };
    strong.scan_macos_hidden_cleanup_registrations(notifier);
    true
}

#[cfg(any(target_os = "macos", test))]
struct MacOsWorkerExitGuard {
    reaper: WeakSharedUnixReaper,
    failure: ReaperNotifierInvariant,
    armed: bool,
}

#[cfg(any(target_os = "macos", test))]
impl MacOsWorkerExitGuard {
    fn new(reaper: WeakSharedUnixReaper) -> Self {
        Self {
            reaper,
            failure: ReaperNotifierInvariant::WorkerLost,
            armed: true,
        }
    }

    fn set_failure(&mut self, failure: ReaperNotifierInvariant) {
        self.failure = failure;
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

#[cfg(any(target_os = "macos", test))]
impl Drop for MacOsWorkerExitGuard {
    fn drop(&mut self) {
        if self.armed {
            if let Some(reaper) = self.reaper.upgrade() {
                reaper.publish_notifier_failure(self.failure);
            }
        }
    }
}

impl SharedUnixReaper {
    /// Fail-closed production shadow. It can validate policy and report the
    /// missing notifier, but it cannot admit a child.
    pub(super) fn initialize_shadow() -> Self {
        Self::new(
            Arc::new(LibcUnixChildKernel::without_linux_notifier()),
            NotifierReadiness::Missing,
            NotifierBackend::Missing,
        )
    }

    #[cfg(target_os = "linux")]
    pub(super) fn initialize_linux() -> Result<Self, LinuxNotifierInitializationError> {
        let runtime = Handle::try_current()
            .map_err(|_| LinuxNotifierInitializationError::TokioRuntimeUnavailable)?;
        Self::initialize_linux_with_runtime(runtime)
    }

    #[cfg(target_os = "linux")]
    pub(super) fn initialize_linux_with_runtime(
        runtime: Handle,
    ) -> Result<Self, LinuxNotifierInitializationError> {
        let prepared = PreparedLinuxNotifier::production_with_runtime(runtime)?;
        let runtime = prepared.runtime_handle();
        let reaper = Self::new(
            Arc::new(LibcUnixChildKernel::with_linux_notifier(runtime)),
            NotifierReadiness::Initializing,
            NotifierBackend::Linux,
        );
        reaper.start_linux_notifier(prepared)?;
        Ok(reaper)
    }

    #[cfg(test)]
    pub(super) fn initialize_with_test_linux_notifier(
        kernel: Arc<dyn UnixChildKernel>,
    ) -> (Self, TestLinuxNotifierHandle) {
        let (prepared, notifier) = PreparedLinuxNotifier::for_test();
        let reaper = Self::new(
            kernel,
            NotifierReadiness::Initializing,
            NotifierBackend::Linux,
        );
        reaper
            .start_linux_notifier(prepared)
            .expect("test notifier worker must start before readiness");
        (reaper, notifier)
    }

    #[cfg(target_os = "macos")]
    pub(super) fn initialize_macos() -> Result<Self, MacOsNotifierInitializationError> {
        let runtime = Handle::try_current()
            .map_err(|_| MacOsNotifierInitializationError::TokioRuntimeUnavailable)?;
        Self::initialize_macos_with_runtime(runtime)
    }

    #[cfg(target_os = "macos")]
    pub(super) fn initialize_macos_with_runtime(
        runtime: Handle,
    ) -> Result<Self, MacOsNotifierInitializationError> {
        let _runtime_guard = runtime.enter();
        let kqueue = Arc::new(ProductionMacOsKqueue::initialize()?);
        let driver: Arc<dyn KqueueDriver> = kqueue.clone();
        let reaper = Self::new(
            Arc::new(LibcUnixChildKernel::without_linux_notifier()),
            NotifierReadiness::Initializing,
            NotifierBackend::MacOs,
        );
        reaper.start_macos_notifier(
            runtime,
            driver,
            MacOsEventSource::Production(Arc::clone(&kqueue)),
        )?;
        Ok(reaper)
    }

    #[cfg(test)]
    pub(super) fn initialize_with_test_macos_notifier(
        kernel: Arc<dyn UnixChildKernel>,
        driver: Arc<dyn KqueueDriver>,
    ) -> (Self, TestMacOsNotifierHandle) {
        let runtime = Handle::try_current().expect("test macOS notifier requires a Tokio runtime");
        let (source, notifier) = MacOsEventSource::for_test();
        let reaper = Self::new(
            kernel,
            NotifierReadiness::Initializing,
            NotifierBackend::MacOs,
        );
        reaper
            .start_macos_notifier(runtime, driver, source)
            .expect("test macOS notifier worker starts before readiness");
        (reaper, notifier)
    }

    #[cfg(any(target_os = "linux", test))]
    fn start_linux_notifier(
        &self,
        prepared: PreparedLinuxNotifier,
    ) -> Result<(), LinuxNotifierInitializationError> {
        let worker = prepared.start(
            self.downgrade(),
            Arc::clone(&self.inner.registration_notify),
        );
        let mut worker_slot = self
            .inner
            .notifier_worker
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if worker_slot.is_some() {
            std::process::abort();
        }
        *worker_slot = Some(worker);
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if state.notifier != NotifierReadiness::Initializing {
            return Err(LinuxNotifierInitializationError::WorkerLostDuringInitialization);
        }
        state.notifier = NotifierReadiness::Ready;
        Ok(())
    }

    #[cfg(any(target_os = "macos", test))]
    fn start_macos_notifier(
        &self,
        runtime: Handle,
        driver: Arc<dyn KqueueDriver>,
        source: MacOsEventSource,
    ) -> Result<(), MacOsNotifierInitializationError> {
        let retained_hidden = Arc::new(SharedRetainedHiddenScheduler::default());
        let collector: Arc<dyn ProvenExitCollector> = Arc::new(MacOsReaperCollector {
            reaper: self.downgrade(),
        });
        let notifier = Arc::new(MacOsNotifier::from_driver(
            driver,
            collector,
            retained_hidden,
        ));
        let worker = MacOsNotifierWorkerHandle::start(
            runtime,
            source,
            Arc::clone(&notifier),
            self.downgrade(),
            Arc::clone(&self.inner.registration_notify),
        );
        let mut notifier_slot = self
            .inner
            .macos_notifier
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut worker_slot = self
            .inner
            .macos_notifier_worker
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if notifier_slot.is_some() || worker_slot.is_some() {
            abort();
        }
        *notifier_slot = Some(notifier);
        *worker_slot = Some(worker);
        drop(worker_slot);
        drop(notifier_slot);

        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if state.notifier != NotifierReadiness::Initializing {
            return Err(MacOsNotifierInitializationError::WorkerLostDuringInitialization);
        }
        state.notifier = NotifierReadiness::Ready;
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn ready_with_kernel_for_test(kernel: Arc<dyn UnixChildKernel>) -> Self {
        Self::new(
            kernel,
            NotifierReadiness::ReadyForTests,
            NotifierBackend::GenericTest,
        )
    }

    #[cfg(test)]
    pub(super) fn ready_with_libc_for_test() -> Self {
        Self::new(
            Arc::new(LibcUnixChildKernel::without_linux_notifier()),
            NotifierReadiness::ReadyForTests,
            NotifierBackend::GenericTest,
        )
    }

    fn new(
        kernel: Arc<dyn UnixChildKernel>,
        notifier: NotifierReadiness,
        notifier_backend: NotifierBackend,
    ) -> Self {
        let (permit_epoch_tx, _) = watch::channel(0);
        Self {
            inner: Arc::new(SupervisorInner {
                state: Mutex::new(SupervisorState {
                    phase: SupervisorPhase::Running,
                    notifier,
                    notifier_backend,
                    notifier_invariant: None,
                    next_generation: 1,
                    active_spawn_permits: 0,
                    permit_epoch: 0,
                    registrations: HashMap::new(),
                }),
                permit_epoch_tx,
                registration_notify: Arc::new(Notify::new()),
                #[cfg(any(target_os = "linux", test))]
                notifier_worker: Mutex::new(None),
                #[cfg(any(target_os = "macos", test))]
                macos_notifier: Mutex::new(None),
                #[cfg(any(target_os = "macos", test))]
                macos_notifier_worker: Mutex::new(None),
                kernel,
            }),
        }
    }

    fn downgrade(&self) -> WeakSharedUnixReaper {
        WeakSharedUnixReaper {
            inner: Arc::downgrade(&self.inner),
        }
    }

    pub(super) fn admit_spawn(
        &self,
        drop_action: RuntimeDropAction,
    ) -> Result<ReaperSpawnPermit, ReaperAdmissionError> {
        let cleanup_target = match drop_action {
            RuntimeDropAction::KeepAlive | RuntimeDropAction::HardKill(KillTarget::Child) => {
                KillTarget::Child
            }
            RuntimeDropAction::HardKill(target) => target,
        };
        self.admit_spawn_with_target(drop_action, cleanup_target)
    }

    pub(super) fn admit_spawn_with_target(
        &self,
        drop_action: RuntimeDropAction,
        cleanup_target: KillTarget,
    ) -> Result<ReaperSpawnPermit, ReaperAdmissionError> {
        validate_drop_action(drop_action, cleanup_target)?;
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if state.phase != SupervisorPhase::Running {
            return Err(ReaperAdmissionError::SupervisorDraining);
        }
        if !matches!(
            state.notifier,
            NotifierReadiness::Ready | NotifierReadiness::ReadyForTests
        ) {
            return Err(ReaperAdmissionError::PlatformNotifierUnavailable);
        }
        let generation = state.next_generation;
        state.next_generation = state
            .next_generation
            .checked_add(1)
            .ok_or(ReaperAdmissionError::GenerationExhausted)?;
        let reserved_slots = state
            .active_spawn_permits
            .checked_add(1)
            .ok_or(ReaperAdmissionError::SpawnPermitCountExhausted)?;
        state
            .registrations
            .try_reserve(reserved_slots)
            .map_err(|_| ReaperAdmissionError::CapacityReservationFailed)?;
        state.active_spawn_permits = reserved_slots;
        drop(state);

        let (terminal_tx, _) = watch::channel(None);
        Ok(ReaperSpawnPermit {
            inner: Arc::clone(&self.inner),
            prepared: Some(PreparedRegistration {
                generation,
                drop_action,
                cleanup_target,
                terminal_tx,
            }),
            hidden_cleanup: None,
            backend_admission_issued: false,
        })
    }

    pub(super) fn poll(&self, identity: RegistrationIdentity) -> ReaperPollResult {
        let (registration, backend) = {
            let state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            (
                state.registrations.get(&identity.generation).cloned(),
                state.notifier_backend,
            )
        };
        if backend == NotifierBackend::MacOs {
            // A published macOS row is collectable only by an exact NOTE_EXIT
            // or ESRCH proof. Hidden macOS rows are reached only through the
            // RetiredSafe scheduler and call Registration::poll directly.
            abort();
        }
        let Some(registration) = registration else {
            return ReaperPollResult::UnknownRegistration(identity);
        };
        if registration.identity != identity {
            return ReaperPollResult::UnknownRegistration(identity);
        }
        let result = registration.poll(self.inner.kernel.as_ref());
        if matches!(
            result,
            ReaperPollResult::Published(Ok(_)) | ReaperPollResult::Cached(Ok(_))
        ) {
            self.retire_terminal_success(&registration);
        }
        result
    }

    pub(super) fn scan_all_live_registrations(&self) {
        let mut identities = {
            let state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            state
                .registrations
                .values()
                .filter(|registration| registration.is_notifier_pollable())
                .map(|registration| registration.identity)
                .collect::<Vec<_>>()
        };
        identities.sort_by_key(|identity| identity.generation);
        for identity in identities {
            let _ = self.poll(identity);
        }
    }

    #[cfg(any(target_os = "linux", test))]
    pub(super) fn take_pending_pidfd_readiness(&self) -> Vec<PidfdReadyFuture> {
        let registrations = {
            let state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            state.registrations.values().cloned().collect::<Vec<_>>()
        };
        registrations
            .into_iter()
            .filter(|registration| registration.is_notifier_pollable())
            .filter_map(|registration| registration.take_pidfd_readiness())
            .collect()
    }

    pub(super) fn scan_hidden_cleanup_registrations(&self) {
        let mut identities = {
            let state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            state
                .registrations
                .values()
                .filter(|registration| registration.is_hidden_cleanup())
                .map(|registration| registration.identity)
                .collect::<Vec<_>>()
        };
        identities.sort_by_key(|identity| identity.generation);
        for identity in identities {
            let _ = self.poll(identity);
        }
    }

    #[cfg(any(target_os = "macos", test))]
    fn collect_macos_proven_exit(&self, identity: MacOsProcessIdentity, _proof: KqueueExitProof) {
        let registration_identity = RegistrationIdentity {
            generation: identity.generation(),
            pid: identity.pid(),
        };
        let registration = {
            let state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            state
                .registrations
                .get(&registration_identity.generation)
                .filter(|registration| registration.identity == registration_identity)
                .cloned()
                .unwrap_or_else(|| abort())
        };
        let result = registration.poll_proven_exit(self.inner.kernel.as_ref());
        if matches!(
            result,
            ReaperPollResult::Published(Ok(_)) | ReaperPollResult::Cached(Ok(_))
        ) {
            self.retire_terminal_success(&registration);
        }
    }

    #[cfg(any(target_os = "macos", test))]
    fn scan_macos_hidden_cleanup_registrations(&self, notifier: &MacOsNotifier) {
        for (watch_id, hidden_identity) in notifier.pollable_hidden_snapshot() {
            let process = hidden_identity.process();
            let identity = RegistrationIdentity {
                generation: process.generation(),
                pid: process.pid(),
            };
            if hidden_identity.hidden_generation() != identity.generation {
                abort();
            }
            let registration = {
                let state = self
                    .inner
                    .state
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                state.registrations.get(&identity.generation).cloned()
            };
            let Some(registration) = registration else {
                continue;
            };
            if registration.identity != identity {
                abort();
            }
            let lifecycle = registration
                .macos_hidden_lifecycle(watch_id, hidden_identity)
                .unwrap_or_else(|| abort());
            let result = registration.poll(self.inner.kernel.as_ref());
            match result {
                ReaperPollResult::Published(Ok(_)) | ReaperPollResult::Cached(Ok(_)) => {
                    notifier
                        .complete_hidden_retirement(watch_id, &lifecycle)
                        .unwrap_or_else(|_| abort());
                    self.retire_terminal_success(&registration);
                }
                ReaperPollResult::Published(Err(_)) | ReaperPollResult::Cached(Err(_)) => {
                    lifecycle.mark_retained_invariant();
                }
                ReaperPollResult::Running => {}
                ReaperPollResult::UnknownRegistration(_) => abort(),
            }
        }

        let mut unwatched = {
            let state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            state
                .registrations
                .values()
                .filter(|registration| registration.is_macos_unwatched_hidden_cleanup())
                .cloned()
                .collect::<Vec<_>>()
        };
        unwatched.sort_by_key(|registration| registration.identity.generation);
        for registration in unwatched {
            let result = registration.poll(self.inner.kernel.as_ref());
            if matches!(
                result,
                ReaperPollResult::Published(Ok(_)) | ReaperPollResult::Cached(Ok(_))
            ) {
                self.retire_terminal_success(&registration);
            }
        }
    }

    #[cfg(any(target_os = "macos", test))]
    fn macos_notifier(&self) -> Arc<MacOsNotifier> {
        self.inner
            .macos_notifier
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            .unwrap_or_else(|| abort())
    }

    pub(super) fn publish_notifier_failure(&self, failure: ReaperNotifierInvariant) {
        let registrations = {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if matches!(
                state.notifier,
                NotifierReadiness::Failed | NotifierReadiness::Closed
            ) {
                return;
            }
            state.notifier = NotifierReadiness::Failed;
            state.notifier_invariant = Some(failure);
            state.registrations.values().cloned().collect::<Vec<_>>()
        };
        for registration in registrations {
            registration.publish_notifier_failure();
        }
    }

    fn retire_terminal_success(&self, registration: &Arc<Registration>) {
        let removed = {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            match state.registrations.get(&registration.identity.generation) {
                Some(current) if Arc::ptr_eq(current, registration) => state
                    .registrations
                    .remove(&registration.identity.generation),
                Some(_) => abort(),
                None => None,
            }
        };
        if let Some(removed) = removed {
            removed.release_supervisor_lease();
        }
    }

    /// Close admission and prove the namespace is empty. This shared layer has
    /// no authority to invent a surface shutdown signal: live rows remain
    /// registered and make shutdown fail closed. A later notifier observation
    /// followed by a retry can complete shutdown.
    pub(super) async fn shutdown(&self) -> Result<(), ReaperShutdownError> {
        let mut permit_epoch_rx = self.inner.permit_epoch_tx.subscribe();
        loop {
            let should_wait = {
                let mut state = self
                    .inner
                    .state
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                if state.phase == SupervisorPhase::Running {
                    state.phase = SupervisorPhase::Draining;
                }
                state.active_spawn_permits != 0
            };
            if !should_wait {
                break;
            }
            if permit_epoch_rx.changed().await.is_err() {
                abort();
            }
        }

        let registrations = {
            let state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            state.registrations.values().cloned().collect::<Vec<_>>()
        };
        let mut live = Vec::new();
        let mut failed = Vec::new();
        let mut terminal = Vec::new();
        for registration in registrations {
            match registration.cached_outcome() {
                None => live.push(registration.identity),
                Some(Ok(_)) => terminal.push(registration),
                Some(Err(error)) => failed.push((registration.identity, error)),
            }
        }

        if !failed.is_empty() {
            return Err(ReaperShutdownError::ReaperFailures(failed));
        }
        if !live.is_empty() {
            live.sort_by_key(|identity| identity.generation);
            return Err(ReaperShutdownError::LiveRegistrations(live));
        }

        let retired = {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let mut retired = Vec::with_capacity(terminal.len());
            for registration in terminal {
                if let Some(removed) = state
                    .registrations
                    .remove(&registration.identity.generation)
                {
                    if !Arc::ptr_eq(&removed, &registration) {
                        abort();
                    }
                    retired.push(removed);
                }
            }
            if !state.registrations.is_empty() {
                abort()
            }
            retired
        };
        for registration in retired {
            registration.release_supervisor_lease();
        }

        #[cfg(any(target_os = "linux", test))]
        {
            let worker = self
                .inner
                .notifier_worker
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            if let Some(worker) = worker {
                if let Err(error) = worker.shutdown().await {
                    self.publish_notifier_failure(error);
                    return Err(ReaperShutdownError::NotifierInvariant(error));
                }
            }
        }
        #[cfg(any(target_os = "macos", test))]
        {
            let worker = self
                .inner
                .macos_notifier_worker
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            if let Some(worker) = worker {
                if let Err(error) = worker.shutdown().await {
                    self.publish_notifier_failure(error);
                    return Err(ReaperShutdownError::NotifierInvariant(error));
                }
            }
            self.inner
                .macos_notifier
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
        }
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(error) = state.notifier_invariant {
            return Err(ReaperShutdownError::NotifierInvariant(error));
        }
        state.notifier = NotifierReadiness::Closed;
        state.phase = SupervisorPhase::Closed;
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn registration_count_for_test(&self) -> usize {
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .registrations
            .len()
    }

    #[cfg(test)]
    pub(super) fn active_spawn_permits_for_test(&self) -> usize {
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .active_spawn_permits
    }

    #[cfg(test)]
    pub(super) fn retention_probe_for_test(&self) -> SupervisorRetentionProbe {
        SupervisorRetentionProbe(Arc::downgrade(&self.inner))
    }

    #[cfg(test)]
    pub(super) fn registration_capacity_for_test(&self) -> usize {
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .registrations
            .capacity()
    }

    #[cfg(test)]
    pub(super) fn notifier_invariant_for_test(&self) -> Option<ReaperNotifierInvariant> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .notifier_invariant
    }

    #[cfg(test)]
    pub(super) fn drop_action_result_for_test(
        &self,
        identity: RegistrationIdentity,
    ) -> Option<DirectChildActionResult> {
        let registration = self
            .inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .registrations
            .get(&identity.generation)
            .cloned()?;
        (registration.identity == identity).then(|| registration.drop_action_result_for_test())
    }

    #[cfg(test)]
    pub(super) async fn adopt_pid_for_test(
        &self,
        pid: pid_t,
        drop_action: RuntimeDropAction,
    ) -> Result<RuntimeChild, ReaperAdmissionError> {
        let child = self
            .admit_spawn(drop_action)?
            .commit_test_pid(pid)
            .await
            .unwrap_or_else(|_| abort());
        Ok(child)
    }

    #[cfg(test)]
    pub(super) async fn adopt_pid_with_target_for_test(
        &self,
        pid: pid_t,
        drop_action: RuntimeDropAction,
        cleanup_target: KillTarget,
    ) -> Result<RuntimeChild, ReaperAdmissionError> {
        let child = self
            .admit_spawn_with_target(drop_action, cleanup_target)?
            .commit_test_pid(pid)
            .await
            .unwrap_or_else(|_| abort());
        Ok(child)
    }
}

fn validate_drop_action(
    action: RuntimeDropAction,
    cleanup_target: KillTarget,
) -> Result<(), ReaperAdmissionError> {
    if !matches!(
        cleanup_target,
        KillTarget::Child | KillTarget::UnixProcessGroup
    ) {
        return Err(ReaperAdmissionError::UnsupportedDropTarget {
            target: cleanup_target,
        });
    }
    match action {
        RuntimeDropAction::KeepAlive => Ok(()),
        RuntimeDropAction::HardKill(target) if target == cleanup_target => Ok(()),
        RuntimeDropAction::HardKill(target) => {
            Err(ReaperAdmissionError::UnsupportedDropTarget { target })
        }
    }
}

fn notifier_is_ready(readiness: NotifierReadiness) -> bool {
    matches!(
        readiness,
        NotifierReadiness::Ready | NotifierReadiness::ReadyForTests
    )
}

struct PreparedRegistration {
    generation: u64,
    drop_action: RuntimeDropAction,
    cleanup_target: KillTarget,
    terminal_tx: watch::Sender<Option<SharedWaitOutcome>>,
}

pub(super) struct ReaperSpawnPermit {
    inner: Arc<SupervisorInner>,
    prepared: Option<PreparedRegistration>,
    hidden_cleanup: Option<Arc<Registration>>,
    backend_admission_issued: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HiddenPermitFinalization {
    Terminal(SharedWaitOutcome),
    Retained,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HiddenCommitCause {
    Notifier(ReaperNotifierInvariant),
    #[cfg(any(target_os = "macos", test))]
    MacOsAttachment(MacOsPrepareWatchError),
}

/// One-shot proof that notifier readiness, admission, map capacity, lifecycle
/// action, and a never-reused generation were all fixed before fd/fork work.
pub(super) struct UnixSpawnAdmission<'a> {
    _exclusive_permit: PhantomData<&'a mut ReaperSpawnPermit>,
}

impl ReaperSpawnPermit {
    pub(super) fn authorize_unix_spawn(
        &mut self,
    ) -> Result<UnixSpawnAdmission<'_>, ReaperAdmissionError> {
        if self.prepared.is_none() || self.backend_admission_issued {
            abort();
        }
        let state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if !notifier_is_ready(state.notifier) {
            return Err(ReaperAdmissionError::PlatformNotifierUnavailable);
        }
        drop(state);
        self.backend_admission_issued = true;
        Ok(UnixSpawnAdmission {
            _exclusive_permit: PhantomData,
        })
    }

    pub(super) async fn commit_unix(
        mut self,
        child: UnregisteredUnixChild,
        close_parent_endpoints: impl FnOnce(),
    ) -> Result<RuntimeChild, ReaperCommitError> {
        if !self.backend_admission_issued {
            abort();
        }
        let pid = child.pid_while_owned();
        let registration = self.take_registration_for_pid(pid);
        self.commit_registration(
            registration,
            || child.transfer_wait_right(),
            close_parent_endpoints,
        )
        .await
    }

    pub(super) async fn cleanup_unpublished_unix(
        mut self,
        child: UnregisteredUnixChild,
        close_parent_endpoints: impl FnOnce(),
    ) -> UnpublishedUnixCleanupOutcome {
        if !self.backend_admission_issued {
            abort();
        }
        let pid = child.pid_while_owned();
        let registration = self.take_registration_for_pid(pid);
        self.insert_hidden_registration(&registration, || child.transfer_wait_right());
        // Parent/reactor setup failed before normal macOS watch preparation, so
        // this exact generation enters the notifier's unwatched-hidden scheduler.
        // If a future caller already owns a pending watch it must use the common
        // settlement routine with that retirement binding instead of dropping it.
        let (identity, action_result, finalization) = self
            .settle_hidden_registration(
                registration,
                #[cfg(any(target_os = "macos", test))]
                None,
                close_parent_endpoints,
            )
            .await;
        match finalization {
            HiddenPermitFinalization::Terminal(Ok(_)) => UnpublishedUnixCleanupOutcome::Reaped {
                identity,
                action_result,
            },
            HiddenPermitFinalization::Terminal(Err(reaper_error)) => {
                UnpublishedUnixCleanupOutcome::RetainedInvariant {
                    identity,
                    action_result,
                    reaper_error,
                }
            }
            HiddenPermitFinalization::Retained => UnpublishedUnixCleanupOutcome::ProcessStillLive {
                identity,
                action_result,
            },
        }
    }

    #[cfg(test)]
    pub(super) async fn commit_test_pid(
        self,
        pid: pid_t,
    ) -> Result<RuntimeChild, ReaperCommitError> {
        self.commit_test_pid_with_close(pid, || {}).await
    }

    #[cfg(test)]
    pub(super) async fn commit_test_pid_with_close(
        mut self,
        pid: pid_t,
        close_parent_endpoints: impl FnOnce(),
    ) -> Result<RuntimeChild, ReaperCommitError> {
        let registration = self.take_registration_for_pid(pid);
        self.commit_registration(registration, || {}, close_parent_endpoints)
            .await
    }

    fn take_registration_for_pid(&mut self, pid: pid_t) -> Arc<Registration> {
        let prepared = self.prepared.take().unwrap_or_else(|| abort());
        #[cfg(any(target_os = "linux", test))]
        let backend = self.notifier_backend();
        #[cfg(any(target_os = "linux", test))]
        let identity = RegistrationIdentity {
            generation: prepared.generation,
            pid,
        };
        #[cfg(any(target_os = "linux", test))]
        let registration = Registration::new(
            prepared,
            pid,
            Arc::clone(&self.inner),
            (backend == NotifierBackend::Linux)
                .then(|| self.inner.kernel.prepare_pidfd(identity))
                .flatten(),
        );
        #[cfg(not(any(target_os = "linux", test)))]
        let registration = Registration::new(prepared, pid, Arc::clone(&self.inner));
        Arc::new(registration)
    }

    fn notifier_backend(&self) -> NotifierBackend {
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .notifier_backend
    }

    async fn commit_registration(
        self,
        registration: Arc<Registration>,
        transfer_wait_right: impl FnOnce(),
        close_parent_endpoints: impl FnOnce(),
    ) -> Result<RuntimeChild, ReaperCommitError> {
        #[cfg(any(target_os = "macos", test))]
        if self.notifier_backend() == NotifierBackend::MacOs {
            let notifier = SharedUnixReaper {
                inner: Arc::clone(&self.inner),
            }
            .macos_notifier();
            let process_identity = MacOsProcessIdentity::new(
                registration.identity.generation,
                registration.identity.pid,
            )
            .unwrap_or_else(|| abort());
            return match notifier.prepare_watch_retaining_failure(process_identity) {
                Ok(pending) => {
                    self.finish_macos_registration_commit(
                        registration,
                        pending,
                        notifier,
                        transfer_wait_right,
                        close_parent_endpoints,
                    )
                    .await
                }
                Err(failure) => {
                    let (pending, original_error) = failure.into_parts();
                    self.insert_hidden_registration(&registration, transfer_wait_right);
                    self.finish_hidden_registration(
                        registration,
                        pending.map(|pending| (pending, Arc::clone(&notifier))),
                        HiddenCommitCause::MacOsAttachment(original_error),
                        close_parent_endpoints,
                    )
                    .await
                }
            };
        }

        let initially_ready = self.insert_registration(&registration, transfer_wait_right);
        self.finish_linux_or_generic_registration_commit(
            registration,
            initially_ready,
            close_parent_endpoints,
        )
        .await
    }

    fn insert_registration(
        &self,
        registration: &Arc<Registration>,
        transfer_wait_right: impl FnOnce(),
    ) -> bool {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if state
            .registrations
            .contains_key(&registration.identity.generation)
            || state.active_spawn_permits == 0
        {
            abort();
        }
        let initially_ready = notifier_is_ready(state.notifier);
        if !initially_ready {
            registration.publish_kind(RegistrationKind::HiddenCleanup);
        }
        transfer_wait_right();
        if state
            .registrations
            .insert(registration.identity.generation, Arc::clone(registration))
            .is_some()
        {
            abort();
        }
        initially_ready
    }

    fn insert_hidden_registration(
        &self,
        registration: &Arc<Registration>,
        transfer_wait_right: impl FnOnce(),
    ) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if state
            .registrations
            .contains_key(&registration.identity.generation)
            || state.active_spawn_permits == 0
        {
            abort();
        }
        registration.publish_kind(RegistrationKind::HiddenCleanup);
        transfer_wait_right();
        if state
            .registrations
            .insert(registration.identity.generation, Arc::clone(registration))
            .is_some()
        {
            abort();
        }
    }

    async fn finish_linux_or_generic_registration_commit(
        self,
        registration: Arc<Registration>,
        initially_ready: bool,
        close_parent_endpoints: impl FnOnce(),
    ) -> Result<RuntimeChild, ReaperCommitError> {
        if initially_ready {
            let _ = registration.poll(self.inner.kernel.as_ref());
        }
        #[cfg(test)]
        if initially_ready {
            self.inner
                .kernel
                .after_commit_initial_poll(registration.identity);
        }

        let original_error = {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if initially_ready && notifier_is_ready(state.notifier) {
                registration.publish_kind(RegistrationKind::Published);
                // Publishing and this second exact WNOHANG observation are one
                // state-locked commit barrier. A SIGCHLD edge consumed while the
                // row was Unpublished therefore cannot strand a live wait right.
                let publication_poll = registration.poll(self.inner.kernel.as_ref());
                let retired = if matches!(
                    publication_poll,
                    ReaperPollResult::Published(Ok(_)) | ReaperPollResult::Cached(Ok(_))
                ) {
                    let removed = state
                        .registrations
                        .remove(&registration.identity.generation)
                        .unwrap_or_else(|| abort());
                    if !Arc::ptr_eq(&removed, &registration) {
                        abort();
                    }
                    Some(removed)
                } else {
                    None
                };
                let runtime_child = RuntimeChild::new(registration, Arc::clone(&self.inner));
                self.finish_committed_permit_in_state(&mut state);
                drop(state);
                if let Some(retired) = retired {
                    retired.release_supervisor_lease();
                }
                self.inner.registration_notify.notify_one();
                return Ok(runtime_child);
            }
            if initially_ready {
                registration.publish_kind(RegistrationKind::HiddenCleanup);
            }
            state.notifier_invariant.unwrap_or_else(|| abort())
        };

        self.finish_hidden_registration(
            registration,
            #[cfg(any(target_os = "macos", test))]
            None,
            HiddenCommitCause::Notifier(original_error),
            close_parent_endpoints,
        )
        .await
    }

    #[cfg(any(target_os = "macos", test))]
    async fn finish_macos_registration_commit(
        self,
        registration: Arc<Registration>,
        pending: PendingKqueueWatch,
        notifier: Arc<MacOsNotifier>,
        transfer_wait_right: impl FnOnce(),
        close_parent_endpoints: impl FnOnce(),
    ) -> Result<RuntimeChild, ReaperCommitError> {
        let mut transfer_wait_right = Some(transfer_wait_right);
        let transition = {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if notifier_is_ready(state.notifier) && state.notifier_backend == NotifierBackend::MacOs
            {
                if state
                    .registrations
                    .contains_key(&registration.identity.generation)
                    || state.active_spawn_permits == 0
                {
                    abort();
                }
                let transition = notifier
                    .commit_with_publication(&pending, pending.identity(), |proven_exit_pending| {
                        #[cfg(test)]
                        self.inner
                            .kernel
                            .before_macos_registration_map_insert(registration.identity);
                        transfer_wait_right.take().unwrap_or_else(|| abort())();
                        if state
                            .registrations
                            .insert(registration.identity.generation, Arc::clone(&registration))
                            .is_some()
                        {
                            abort();
                        }
                        registration.publish_macos_registration(proven_exit_pending);
                    })
                    .unwrap_or_else(|_| abort());
                Ok(transition)
            } else {
                if state
                    .registrations
                    .contains_key(&registration.identity.generation)
                    || state.active_spawn_permits == 0
                {
                    abort();
                }
                registration.publish_kind(RegistrationKind::HiddenCleanup);
                transfer_wait_right.take().unwrap_or_else(|| abort())();
                if state
                    .registrations
                    .insert(registration.identity.generation, Arc::clone(&registration))
                    .is_some()
                {
                    abort();
                }
                Err(state.notifier_invariant.unwrap_or_else(|| abort()))
            }
        };

        let action = match transition {
            Ok(action) => action,
            Err(original_error) => {
                return self
                    .finish_hidden_registration(
                        registration,
                        Some((pending, notifier)),
                        HiddenCommitCause::Notifier(original_error),
                        close_parent_endpoints,
                    )
                    .await;
            }
        };

        #[cfg(test)]
        self.inner
            .kernel
            .before_macos_deferred_collection(registration.identity);
        let disposition = notifier
            .finish_commit_action(action)
            .unwrap_or_else(|_| abort());
        if !matches!(
            disposition,
            KqueueEventDisposition::AwaitingExactNoteExit
                | KqueueEventDisposition::CollectedPublishedExit
        ) {
            abort();
        }
        let runtime_child = RuntimeChild::new(registration, Arc::clone(&self.inner));
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        self.finish_committed_permit_in_state(&mut state);
        drop(state);
        self.inner.registration_notify.notify_one();
        Ok(runtime_child)
    }

    async fn finish_hidden_registration(
        self,
        registration: Arc<Registration>,
        #[cfg(any(target_os = "macos", test))] macos_retirement: Option<(
            PendingKqueueWatch,
            Arc<MacOsNotifier>,
        )>,
        cause: HiddenCommitCause,
        close_parent_endpoints: impl FnOnce(),
    ) -> Result<RuntimeChild, ReaperCommitError> {
        let (identity, action_result, finalization) = self
            .settle_hidden_registration(
                registration,
                #[cfg(any(target_os = "macos", test))]
                macos_retirement,
                close_parent_endpoints,
            )
            .await;
        let error = match (cause, finalization) {
            (
                HiddenCommitCause::Notifier(original_error),
                HiddenPermitFinalization::Terminal(Ok(_)),
            ) => ReaperCommitError::NotifierLost {
                identity,
                original_error,
                action_result,
            },
            (
                HiddenCommitCause::Notifier(original_error),
                HiddenPermitFinalization::Terminal(Err(reaper_error)),
            ) => ReaperCommitError::RetainedInvariant {
                identity,
                original_error,
                action_result,
                reaper_error,
            },
            (HiddenCommitCause::Notifier(original_error), HiddenPermitFinalization::Retained) => {
                ReaperCommitError::ProcessStillLive {
                    identity,
                    original_error,
                    action_result,
                }
            }
            #[cfg(any(target_os = "macos", test))]
            (
                HiddenCommitCause::MacOsAttachment(original_error),
                HiddenPermitFinalization::Terminal(Ok(_)),
            ) => ReaperCommitError::MacOsAttachment {
                identity,
                original_error,
                action_result,
            },
            #[cfg(any(target_os = "macos", test))]
            (
                HiddenCommitCause::MacOsAttachment(original_error),
                HiddenPermitFinalization::Terminal(Err(reaper_error)),
            ) => ReaperCommitError::MacOsAttachmentRetainedInvariant {
                identity,
                original_error,
                action_result,
                reaper_error,
            },
            #[cfg(any(target_os = "macos", test))]
            (
                HiddenCommitCause::MacOsAttachment(original_error),
                HiddenPermitFinalization::Retained,
            ) => ReaperCommitError::MacOsAttachmentProcessStillLive {
                identity,
                original_error,
                action_result,
            },
        };
        Err(error)
    }

    async fn settle_hidden_registration(
        mut self,
        registration: Arc<Registration>,
        #[cfg(any(target_os = "macos", test))] macos_retirement: Option<(
            PendingKqueueWatch,
            Arc<MacOsNotifier>,
        )>,
        close_parent_endpoints: impl FnOnce(),
    ) -> (
        RegistrationIdentity,
        DirectChildActionResult,
        HiddenPermitFinalization,
    ) {
        close_parent_endpoints();
        #[cfg(any(target_os = "macos", test))]
        if let Some((pending, notifier)) = &macos_retirement {
            let lifecycle = registration.bind_macos_hidden(*pending);
            let disposition = notifier.begin_hidden_retirement(pending, &lifecycle);
            if !matches!(
                disposition,
                KqueueEventDisposition::RetiredHidden | KqueueEventDisposition::DeleteUnproven
            ) {
                abort();
            }
        }

        let action_result = if registration.cached_outcome().is_none() {
            registration.begin_hidden_cleanup(self.inner.kernel.as_ref())
        } else {
            DirectChildActionResult::SkippedTerminal
        };
        self.hidden_cleanup = Some(Arc::clone(&registration));
        let waiter = registration.waiter();
        self.inner.registration_notify.notify_one();

        let observed = if let Some(outcome) = registration.cached_outcome() {
            Some(outcome)
        } else {
            tokio::select! {
                biased;
                outcome = waiter.wait() => Some(outcome),
                _ = tokio::time::sleep(HIDDEN_CLEANUP_SETTLE) => None,
            }
        };
        let finalization = self.finish_hidden_permit(&registration, observed.is_none());
        let identity = registration.identity;
        (identity, action_result, finalization)
    }

    fn finish_hidden_permit(
        &mut self,
        registration: &Arc<Registration>,
        retain_if_live: bool,
    ) -> HiddenPermitFinalization {
        let finalization = registration.finalize_hidden_settlement(retain_if_live);
        #[cfg(any(target_os = "macos", test))]
        if matches!(finalization, HiddenPermitFinalization::Terminal(Ok(_))) {
            if let Some(hidden) = registration.macos_hidden_binding() {
                let notifier = self
                    .inner
                    .macos_notifier
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .clone()
                    .unwrap_or_else(|| abort());
                let _ = notifier
                    .complete_hidden_retirement(hidden.pending.watch_id(), &hidden.lifecycle);
            }
        }
        let owned = self.hidden_cleanup.take().unwrap_or_else(|| abort());
        if !Arc::ptr_eq(&owned, registration) {
            abort();
        }

        let retired = {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let retired = if matches!(finalization, HiddenPermitFinalization::Terminal(Ok(_))) {
                match state.registrations.get(&registration.identity.generation) {
                    Some(current) if Arc::ptr_eq(current, registration) => state
                        .registrations
                        .remove(&registration.identity.generation),
                    Some(_) => abort(),
                    None => None,
                }
            } else {
                None
            };
            self.finish_committed_permit_in_state(&mut state);
            retired
        };
        if let Some(retired) = retired {
            retired.release_supervisor_lease();
        }
        finalization
    }

    fn finish_committed_permit_in_state(&self, state: &mut SupervisorState) {
        if self.prepared.is_some() {
            abort();
        }
        if state.active_spawn_permits == 0 {
            abort();
        }
        finish_spawn_permit(state, &self.inner.permit_epoch_tx);
    }
}

impl Drop for ReaperSpawnPermit {
    fn drop(&mut self) {
        if let Some(registration) = self.hidden_cleanup.clone() {
            let _ = self.finish_hidden_permit(&registration, true);
            return;
        }
        if self.prepared.is_none() {
            return;
        }
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if state.active_spawn_permits == 0 {
            abort();
        }
        finish_spawn_permit(&mut state, &self.inner.permit_epoch_tx);
    }
}

fn finish_spawn_permit(state: &mut SupervisorState, permit_epoch_tx: &watch::Sender<u64>) {
    state.active_spawn_permits -= 1;
    state.permit_epoch = state.permit_epoch.wrapping_add(1);
    permit_epoch_tx.send_replace(state.permit_epoch);
}

struct Registration {
    identity: RegistrationIdentity,
    drop_action: RuntimeDropAction,
    cleanup_target: KillTarget,
    lifecycle: Mutex<RegistrationLifecycle>,
    terminal_tx: watch::Sender<Option<SharedWaitOutcome>>,
    // A live/retained registration keeps its namespace supervisor alive even
    // after every facade and unique child owner has been dropped. Successful
    // terminal retirement breaks this intentional lease cycle only after
    // exact removal from the generation map.
    supervisor_lease: Mutex<Option<Arc<SupervisorInner>>>,
    #[cfg(any(target_os = "linux", test))]
    pidfd: Mutex<Option<PreparedPidfd>>,
    hidden_cleanup_pollable: AtomicBool,
    #[cfg(any(target_os = "macos", test))]
    macos_hidden: Mutex<Option<MacOsHiddenRegistration>>,
}

#[cfg(any(target_os = "macos", test))]
#[derive(Clone)]
struct MacOsHiddenRegistration {
    pending: PendingKqueueWatch,
    lifecycle: HiddenLifecycleHandle,
}

impl Registration {
    fn new(
        prepared: PreparedRegistration,
        pid: pid_t,
        supervisor: Arc<SupervisorInner>,
        #[cfg(any(target_os = "linux", test))] pidfd: Option<PreparedPidfd>,
    ) -> Self {
        Self {
            identity: RegistrationIdentity {
                generation: prepared.generation,
                pid,
            },
            drop_action: prepared.drop_action,
            cleanup_target: prepared.cleanup_target,
            lifecycle: Mutex::new(RegistrationLifecycle {
                kind: RegistrationKind::Unpublished,
                terminal: None,
                owner_dropped: false,
                drop_action_state: DropActionState::Armed,
                retained_invariant: false,
                #[cfg(any(target_os = "macos", test))]
                macos_proven_exit_pending: false,
            }),
            terminal_tx: prepared.terminal_tx,
            supervisor_lease: Mutex::new(Some(supervisor)),
            #[cfg(any(target_os = "linux", test))]
            pidfd: Mutex::new(pidfd),
            hidden_cleanup_pollable: AtomicBool::new(false),
            #[cfg(any(target_os = "macos", test))]
            macos_hidden: Mutex::new(None),
        }
    }

    #[cfg(any(target_os = "linux", test))]
    fn take_pidfd_readiness(&self) -> Option<PidfdReadyFuture> {
        self.pidfd
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_mut()?
            .take_readiness()
    }

    #[cfg(any(target_os = "linux", test))]
    fn clear_pidfd(&self) {
        self.pidfd
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
    }

    fn signal_direct_child(
        &self,
        kernel: &dyn UnixChildKernel,
        signal: c_int,
    ) -> KernelSignalObservation {
        #[cfg(any(target_os = "linux", test))]
        {
            let pidfd = self.pidfd.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(pidfd) = pidfd.as_ref() {
                return pidfd.signal_child(signal);
            }
        }
        kernel.signal_child(self.identity, signal)
    }

    fn perform_hard_action(
        &self,
        kernel: &dyn UnixChildKernel,
        target: KillTarget,
        signal: c_int,
    ) -> DirectChildActionResult {
        match target {
            KillTarget::Child => direct_action_result(self.signal_direct_child(kernel, signal)),
            KillTarget::UnixProcessGroup => {
                let group =
                    signal_action_result(kernel.signal_process_group(self.identity, signal));
                let direct_child_fallback = if group == SignalActionResult::Delivered {
                    None
                } else {
                    Some(signal_action_result(
                        self.signal_direct_child(kernel, signal),
                    ))
                };
                DirectChildActionResult::ProcessGroup {
                    group,
                    direct_child_fallback,
                }
            }
            KillTarget::WindowsProcessTree | KillTarget::WindowsPrivateJob => abort(),
        }
    }

    fn publish_kind(&self, kind: RegistrationKind) {
        let mut lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if lifecycle.kind != RegistrationKind::Unpublished {
            abort();
        }
        lifecycle.kind = kind;
    }

    #[cfg(any(target_os = "macos", test))]
    fn publish_macos_registration(&self, proven_exit_pending: bool) {
        let mut lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if lifecycle.kind != RegistrationKind::Unpublished || lifecycle.macos_proven_exit_pending {
            abort();
        }
        lifecycle.kind = RegistrationKind::Published;
        lifecycle.macos_proven_exit_pending = proven_exit_pending;
    }

    #[cfg(any(target_os = "macos", test))]
    fn bind_macos_hidden(&self, pending: PendingKqueueWatch) -> HiddenLifecycleHandle {
        if pending.identity().generation() != self.identity.generation
            || pending.identity().pid() != self.identity.pid
        {
            abort();
        }
        let lifecycle = HiddenLifecycleHandle::new(
            HiddenRegistrationIdentity::new(self.identity.generation, pending.identity()),
            HiddenLifecyclePhase::ActionPending,
        );
        let mut hidden = self
            .macos_hidden
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if hidden.is_some() {
            abort();
        }
        *hidden = Some(MacOsHiddenRegistration {
            pending,
            lifecycle: lifecycle.clone(),
        });
        lifecycle
    }

    #[cfg(any(target_os = "macos", test))]
    fn macos_hidden_lifecycle(
        &self,
        watch_id: KqueueWatchId,
        hidden_identity: HiddenRegistrationIdentity,
    ) -> Option<HiddenLifecycleHandle> {
        self.macos_hidden
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .filter(|hidden| {
                hidden.pending.watch_id() == watch_id
                    && hidden.lifecycle.registration() == hidden_identity
            })
            .map(|hidden| hidden.lifecycle.clone())
    }

    #[cfg(any(target_os = "macos", test))]
    fn macos_hidden_binding(&self) -> Option<MacOsHiddenRegistration> {
        self.macos_hidden
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn is_hidden_cleanup(&self) -> bool {
        self.hidden_cleanup_pollable.load(Ordering::Acquire)
    }

    #[cfg(any(target_os = "macos", test))]
    fn is_macos_unwatched_hidden_cleanup(&self) -> bool {
        self.is_hidden_cleanup()
            && self
                .macos_hidden
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_none()
    }

    fn is_notifier_pollable(&self) -> bool {
        match self
            .lifecycle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .kind
        {
            RegistrationKind::Published => true,
            RegistrationKind::HiddenCleanup => self.hidden_cleanup_pollable.load(Ordering::Acquire),
            RegistrationKind::Unpublished => false,
        }
    }

    fn begin_hidden_cleanup(&self, kernel: &dyn UnixChildKernel) -> DirectChildActionResult {
        let mut lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if lifecycle.kind != RegistrationKind::HiddenCleanup || lifecycle.terminal.is_some() {
            abort();
        }
        let result = consume_drop_action(&mut lifecycle, || {
            self.perform_hard_action(kernel, self.cleanup_target, libc::SIGKILL)
        });
        self.hidden_cleanup_pollable.store(true, Ordering::Release);
        drop(lifecycle);
        #[cfg(any(target_os = "macos", test))]
        if let Some(hidden) = self.macos_hidden_binding() {
            hidden.lifecycle.mark_action_settled();
        }
        result
    }

    fn finalize_hidden_settlement(&self, retain_if_live: bool) -> HiddenPermitFinalization {
        let mut lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if lifecycle.kind != RegistrationKind::HiddenCleanup {
            abort();
        }
        if let Some(outcome) = lifecycle.terminal {
            if outcome.is_err() {
                lifecycle.retained_invariant = true;
                #[cfg(any(target_os = "macos", test))]
                if let Some(hidden) = self.macos_hidden_binding() {
                    hidden.lifecycle.mark_retained_invariant();
                }
            }
            return HiddenPermitFinalization::Terminal(outcome);
        }
        if !retain_if_live
            || !self.hidden_cleanup_pollable.load(Ordering::Acquire)
            || !matches!(lifecycle.drop_action_state, DropActionState::Settling(_))
        {
            abort();
        }
        lifecycle.retained_invariant = true;
        #[cfg(any(target_os = "macos", test))]
        if let Some(hidden) = self.macos_hidden_binding() {
            hidden.lifecycle.mark_retained_invariant();
        }
        HiddenPermitFinalization::Retained
    }

    fn publish_notifier_failure(&self) {
        let mut lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if lifecycle.kind != RegistrationKind::Published || lifecycle.terminal.is_some() {
            return;
        }
        #[cfg(any(target_os = "macos", test))]
        if lifecycle.macos_proven_exit_pending {
            // Commit established an exact NOTE_EXIT/ESRCH proof while holding
            // map -> watch -> lifecycle. That proof outranks a notifier-loss
            // race until its deferred blocking wait consumes the exact PID.
            return;
        }
        let outcome = Err(ReaperInvariant::NotifierClosed {
            identity: self.identity,
        });
        lifecycle.drop_action_state = DropActionState::Disarmed;
        lifecycle.terminal = Some(outcome);
        #[cfg(any(target_os = "linux", test))]
        self.clear_pidfd();
        self.terminal_tx.send_replace(Some(outcome));
    }

    fn release_supervisor_lease(&self) {
        let lease = self
            .supervisor_lease
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if lease.is_none() {
            abort();
        }
    }

    fn poll(&self, kernel: &dyn UnixChildKernel) -> ReaperPollResult {
        let mut lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(outcome) = lifecycle.terminal {
            return ReaperPollResult::Cached(outcome);
        }
        if lifecycle.kind == RegistrationKind::HiddenCleanup
            && !self.hidden_cleanup_pollable.load(Ordering::Acquire)
        {
            abort();
        }
        let observation = kernel.wait_nonblocking(self.identity);
        let outcome = match observation {
            KernelWaitObservation::Running => return ReaperPollResult::Running,
            KernelWaitObservation::Exited(raw) => Ok(SharedExitStatus(ExitStatus::from_raw(raw))),
            KernelWaitObservation::Error(errno) if errno == libc::ECHILD => {
                Err(ReaperInvariant::WaitOwnershipLost {
                    identity: self.identity,
                })
            }
            KernelWaitObservation::Error(errno) => Err(ReaperInvariant::WaitFailed {
                identity: self.identity,
                errno,
            }),
            KernelWaitObservation::WrongPid(observed_pid) => {
                Err(ReaperInvariant::WrongWaitIdentity {
                    expected: self.identity,
                    observed_pid,
                })
            }
        };
        lifecycle.drop_action_state = DropActionState::Disarmed;
        lifecycle.terminal = Some(outcome);
        #[cfg(any(target_os = "linux", test))]
        self.clear_pidfd();
        self.terminal_tx.send_replace(Some(outcome));
        ReaperPollResult::Published(outcome)
    }

    #[cfg(any(target_os = "macos", test))]
    fn poll_proven_exit(&self, kernel: &dyn UnixChildKernel) -> ReaperPollResult {
        let mut lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(outcome) = lifecycle.terminal {
            return ReaperPollResult::Cached(outcome);
        }
        if lifecycle.kind != RegistrationKind::Published {
            abort();
        }
        let outcome = match kernel.wait_proven_exit(self.identity) {
            KernelWaitObservation::Exited(raw) => Ok(SharedExitStatus(ExitStatus::from_raw(raw))),
            KernelWaitObservation::Running => Err(ReaperInvariant::ProvenExitNotTerminal {
                identity: self.identity,
            }),
            KernelWaitObservation::Error(errno) if errno == libc::ECHILD => {
                Err(ReaperInvariant::WaitOwnershipLost {
                    identity: self.identity,
                })
            }
            KernelWaitObservation::Error(errno) => Err(ReaperInvariant::WaitFailed {
                identity: self.identity,
                errno,
            }),
            KernelWaitObservation::WrongPid(observed_pid) => {
                Err(ReaperInvariant::WrongWaitIdentity {
                    expected: self.identity,
                    observed_pid,
                })
            }
        };
        lifecycle.macos_proven_exit_pending = false;
        lifecycle.drop_action_state = DropActionState::Disarmed;
        lifecycle.terminal = Some(outcome);
        #[cfg(any(target_os = "linux", test))]
        self.clear_pidfd();
        self.terminal_tx.send_replace(Some(outcome));
        ReaperPollResult::Published(outcome)
    }

    fn cached_outcome(&self) -> Option<SharedWaitOutcome> {
        self.lifecycle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .terminal
    }

    fn owner_dropped(&self, kernel: &dyn UnixChildKernel) -> DirectChildActionResult {
        let mut lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if lifecycle.owner_dropped {
            return existing_action_result(&lifecycle);
        }
        lifecycle.owner_dropped = true;
        match self.drop_action {
            RuntimeDropAction::KeepAlive => {
                lifecycle.drop_action_state = DropActionState::Disarmed;
                DirectChildActionResult::SkippedDisarmed
            }
            RuntimeDropAction::HardKill(KillTarget::Child) => {
                consume_drop_action(&mut lifecycle, || {
                    self.perform_hard_action(kernel, KillTarget::Child, libc::SIGKILL)
                })
            }
            RuntimeDropAction::HardKill(KillTarget::UnixProcessGroup) => {
                consume_drop_action(&mut lifecycle, || {
                    self.perform_hard_action(kernel, KillTarget::UnixProcessGroup, libc::SIGKILL)
                })
            }
            RuntimeDropAction::HardKill(_) => abort(),
        }
    }

    fn disarm_owner(&self) {
        let mut lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        lifecycle.drop_action_state = DropActionState::Disarmed;
    }

    fn waiter(&self) -> RuntimeChildWaiter {
        RuntimeChildWaiter {
            identity: self.identity,
            terminal_rx: self.terminal_tx.subscribe(),
        }
    }

    #[cfg(test)]
    fn drop_action_result_for_test(&self) -> DirectChildActionResult {
        let lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        existing_action_result(&lifecycle)
    }
}

struct RegistrationLifecycle {
    kind: RegistrationKind,
    terminal: Option<SharedWaitOutcome>,
    owner_dropped: bool,
    drop_action_state: DropActionState,
    retained_invariant: bool,
    #[cfg(any(target_os = "macos", test))]
    macos_proven_exit_pending: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DropActionState {
    Armed,
    ActionStarted,
    Settling(DirectChildActionResult),
    Disarmed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RegistrationKind {
    Unpublished,
    Published,
    HiddenCleanup,
}

fn consume_drop_action(
    lifecycle: &mut RegistrationLifecycle,
    perform_action: impl FnOnce() -> DirectChildActionResult,
) -> DirectChildActionResult {
    if lifecycle.terminal.is_some() {
        lifecycle.drop_action_state = DropActionState::Disarmed;
        return DirectChildActionResult::SkippedTerminal;
    }
    match lifecycle.drop_action_state {
        DropActionState::ActionStarted => abort(),
        DropActionState::Settling(result) => return result,
        DropActionState::Disarmed => return DirectChildActionResult::SkippedDisarmed,
        DropActionState::Armed => {}
    }
    // The receipt transition precedes the side effect and remains protected by
    // the same mutex used by wait/cache publication.
    lifecycle.drop_action_state = DropActionState::ActionStarted;
    let result = perform_action();
    lifecycle.drop_action_state = DropActionState::Settling(result);
    result
}

fn signal_action_result(observation: KernelSignalObservation) -> SignalActionResult {
    match observation {
        KernelSignalObservation::Delivered => SignalActionResult::Delivered,
        KernelSignalObservation::AlreadyGone => SignalActionResult::AlreadyGone,
        KernelSignalObservation::Error(errno) => SignalActionResult::Failed { errno },
    }
}

fn direct_action_result(observation: KernelSignalObservation) -> DirectChildActionResult {
    match signal_action_result(observation) {
        SignalActionResult::Delivered => DirectChildActionResult::Delivered,
        SignalActionResult::AlreadyGone => DirectChildActionResult::AlreadyGone,
        SignalActionResult::Failed { errno } => DirectChildActionResult::Failed { errno },
    }
}

fn existing_action_result(lifecycle: &RegistrationLifecycle) -> DirectChildActionResult {
    match lifecycle.drop_action_state {
        DropActionState::Armed | DropActionState::Disarmed => {
            DirectChildActionResult::SkippedDisarmed
        }
        DropActionState::ActionStarted => abort(),
        DropActionState::Settling(result) => result,
    }
}

/// The sole non-cloneable lifecycle owner. It contains no stdio and exposes no
/// numeric PID control entrypoint.
pub(super) struct RuntimeChild {
    registration: Arc<Registration>,
    _supervisor: Arc<SupervisorInner>,
    armed: bool,
}

impl RuntimeChild {
    fn new(registration: Arc<Registration>, supervisor: Arc<SupervisorInner>) -> Self {
        Self {
            registration,
            _supervisor: supervisor,
            armed: true,
        }
    }

    pub(super) fn try_wait(&self) -> Option<SharedWaitOutcome> {
        self.registration.cached_outcome()
    }

    pub(super) fn waiter(&self) -> RuntimeChildWaiter {
        self.registration.waiter()
    }

    pub(super) fn controller(&self) -> RuntimeChildController {
        RuntimeChildController {
            registration: Arc::clone(&self.registration),
            supervisor: Arc::clone(&self._supervisor),
        }
    }

    pub(super) fn pid_for_browser(&self) -> u32 {
        u32::try_from(self.registration.identity.pid_for_kernel())
            .unwrap_or_else(|_| std::process::abort())
    }

    pub(super) fn disarm(&mut self) {
        if self.armed {
            self.registration.disarm_owner();
            self.armed = false;
        }
    }

    #[cfg(test)]
    pub(super) fn identity_for_test(&self) -> RegistrationIdentity {
        self.registration.identity
    }

    #[cfg(test)]
    pub(super) fn drop_action_result_for_test(&self) -> DirectChildActionResult {
        self.registration.drop_action_result_for_test()
    }
}

#[derive(Clone)]
pub(super) struct RuntimeChildController {
    registration: Arc<Registration>,
    supervisor: Arc<SupervisorInner>,
}

impl RuntimeChildController {
    pub(super) fn try_wait(&self) -> Option<SharedWaitOutcome> {
        self.registration.cached_outcome()
    }

    pub(super) fn signal(&self, target: KillTarget, signal: c_int) -> DirectChildActionResult {
        self.registration
            .perform_hard_action(self.supervisor.kernel.as_ref(), target, signal)
    }
}

impl Drop for RuntimeChild {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let _ = self
            .registration
            .owner_dropped(self._supervisor.kernel.as_ref());
        self.armed = false;
    }
}

pub(super) struct RuntimeChildWaiter {
    identity: RegistrationIdentity,
    terminal_rx: watch::Receiver<Option<SharedWaitOutcome>>,
}

impl RuntimeChildWaiter {
    pub(super) async fn wait(mut self) -> SharedWaitOutcome {
        loop {
            if let Some(outcome) = *self.terminal_rx.borrow_and_update() {
                return outcome;
            }
            if self.terminal_rx.changed().await.is_err() {
                return Err(ReaperInvariant::NotifierClosed {
                    identity: self.identity,
                });
            }
        }
    }
}

fn last_errno() -> c_int {
    std::io::Error::last_os_error()
        .raw_os_error()
        .unwrap_or(libc::EINVAL)
}
