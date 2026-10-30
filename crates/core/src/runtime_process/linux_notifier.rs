//! Linux shared-reaper notifier.
//!
//! The production bootstrap owns one Tokio SIGCHLD stream and one long-lived
//! worker. Pidfds are optional accelerators inside that worker; the stream is
//! installed first and remains the correctness fallback for every child.

use std::fmt;
use std::future::Future;
use std::io;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};

use futures::stream::{FuturesUnordered, StreamExt};
use futures::FutureExt;
use tokio::runtime::Handle;
use tokio::sync::{oneshot, Notify};

#[cfg(test)]
use tokio::sync::watch;

#[cfg(target_os = "linux")]
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
#[cfg(target_os = "linux")]
use std::panic::catch_unwind;
#[cfg(target_os = "linux")]
use tokio::io::unix::AsyncFd;
#[cfg(target_os = "linux")]
use tokio::signal::unix::{signal, Signal, SignalKind};

use super::reaper::{
    KernelSignalObservation, ReaperNotifierInvariant, RegistrationIdentity, WeakSharedUnixReaper,
};

pub(super) type PidfdReadyFuture =
    Pin<Box<dyn Future<Output = (RegistrationIdentity, io::Result<()>)> + Send + 'static>>;

/// A pre-commit, already-attached pidfd readiness future. Dropping this value
/// closes the pidfd through `AsyncFd<OwnedFd>` ownership.
pub(super) struct PreparedPidfd {
    readiness: Option<PidfdReadyFuture>,
    signal: Arc<dyn PidfdSignalCapability>,
}

impl PreparedPidfd {
    pub(super) fn take_readiness(&mut self) -> Option<PidfdReadyFuture> {
        self.readiness.take()
    }

    pub(super) fn signal_child(&self, signal: libc::c_int) -> KernelSignalObservation {
        self.signal.signal_child(signal)
    }

    #[cfg(test)]
    pub(super) fn ready_for_test(
        identity: RegistrationIdentity,
        receiver: oneshot::Receiver<io::Result<()>>,
        signals: Arc<Mutex<Vec<RegistrationIdentity>>>,
    ) -> Self {
        Self {
            readiness: Some(Box::pin(async move {
                let result = receiver.await.unwrap_or_else(|_| {
                    Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "test pidfd readiness sender closed",
                    ))
                });
                (identity, result)
            })),
            signal: Arc::new(TestPidfdSignalCapability { identity, signals }),
        }
    }

    /// Optional Linux pidfd preparation. Every failure path owns and drops any
    /// returned descriptor before falling back to the preinstalled SIGCHLD
    /// stream; no errno is promoted into a spawn failure.
    #[cfg(target_os = "linux")]
    pub(super) fn prepare_linux(identity: RegistrationIdentity, runtime: &Handle) -> Option<Self> {
        let raw = unsafe { libc::syscall(libc::SYS_pidfd_open, identity.pid_for_kernel(), 0) };
        if raw < 0 || raw > i32::MAX as libc::c_long {
            return None;
        }
        let descriptor = unsafe { OwnedFd::from_raw_fd(raw as RawFd) };
        if !set_descriptor_flag(
            descriptor.as_raw_fd(),
            libc::F_GETFL,
            libc::F_SETFL,
            libc::O_NONBLOCK,
        ) || !set_descriptor_flag(
            descriptor.as_raw_fd(),
            libc::F_GETFD,
            libc::F_SETFD,
            libc::FD_CLOEXEC,
        ) {
            return None;
        }

        let async_fd = match catch_unwind(AssertUnwindSafe(|| {
            let _runtime_guard = runtime.enter();
            AsyncFd::new(descriptor)
        })) {
            Ok(Ok(async_fd)) => async_fd,
            Ok(Err(_)) | Err(_) => return None,
        };
        let signal = Arc::new(LinuxPidfdCapability {
            descriptor: async_fd,
        });
        let readiness_descriptor = Arc::clone(&signal);
        Some(Self {
            readiness: Some(Box::pin(async move {
                let result = readiness_descriptor
                    .descriptor
                    .readable()
                    .await
                    .map(|_guard| ());
                (identity, result)
            })),
            signal,
        })
    }
}

trait PidfdSignalCapability: Send + Sync + 'static {
    fn signal_child(&self, signal: libc::c_int) -> KernelSignalObservation;
}

#[cfg(target_os = "linux")]
struct LinuxPidfdCapability {
    descriptor: AsyncFd<OwnedFd>,
}

#[cfg(target_os = "linux")]
impl PidfdSignalCapability for LinuxPidfdCapability {
    fn signal_child(&self, signal: libc::c_int) -> KernelSignalObservation {
        loop {
            let result = unsafe {
                libc::syscall(
                    libc::SYS_pidfd_send_signal,
                    self.descriptor.get_ref().as_raw_fd(),
                    signal,
                    std::ptr::null::<libc::siginfo_t>(),
                    0,
                )
            };
            if result == 0 {
                return KernelSignalObservation::Delivered;
            }
            let errno = io::Error::last_os_error()
                .raw_os_error()
                .unwrap_or(libc::EINVAL);
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
}

#[cfg(test)]
struct TestPidfdSignalCapability {
    identity: RegistrationIdentity,
    signals: Arc<Mutex<Vec<RegistrationIdentity>>>,
}

#[cfg(test)]
impl PidfdSignalCapability for TestPidfdSignalCapability {
    fn signal_child(&self, _signal: libc::c_int) -> KernelSignalObservation {
        self.signals
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(self.identity);
        KernelSignalObservation::Delivered
    }
}

#[cfg(target_os = "linux")]
fn set_descriptor_flag(fd: RawFd, get: libc::c_int, set: libc::c_int, flag: libc::c_int) -> bool {
    let current = loop {
        let result = unsafe { libc::fcntl(fd, get) };
        if result >= 0 {
            break result;
        }
        if io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
            return false;
        }
    };
    loop {
        let result = unsafe { libc::fcntl(fd, set, current | flag) };
        if result == 0 {
            return true;
        }
        if io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
            return false;
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum LinuxNotifierInitializationError {
    TokioRuntimeUnavailable,
    SignalInstallFailed { errno: Option<i32> },
    SignalInstallPanicked,
    WorkerLostDuringInitialization,
}

impl fmt::Display for LinuxNotifierInitializationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TokioRuntimeUnavailable => {
                formatter.write_str("Linux shared reaper requires a live Tokio runtime")
            }
            Self::SignalInstallFailed { errno } => {
                write!(
                    formatter,
                    "failed to install the Linux SIGCHLD stream: {errno:?}"
                )
            }
            Self::SignalInstallPanicked => formatter.write_str(
                "Tokio rejected Linux SIGCHLD stream initialization before spawn admission",
            ),
            Self::WorkerLostDuringInitialization => formatter.write_str(
                "Linux shared-reaper worker was lost before spawn admission became ready",
            ),
        }
    }
}

impl std::error::Error for LinuxNotifierInitializationError {}

enum LinuxNotifierSource {
    #[cfg(target_os = "linux")]
    Signal(Signal),
    #[cfg(test)]
    Test {
        stream: watch::Receiver<u64>,
        acknowledged: watch::Sender<u64>,
        pending_epoch: Option<u64>,
    },
}

impl LinuxNotifierSource {
    async fn recv(&mut self) -> Option<()> {
        match self {
            #[cfg(target_os = "linux")]
            Self::Signal(stream) => stream.recv().await,
            #[cfg(test)]
            Self::Test {
                stream,
                pending_epoch,
                ..
            } => {
                stream.changed().await.ok()?;
                *pending_epoch = Some(*stream.borrow_and_update());
                Some(())
            }
        }
    }

    #[cfg(test)]
    fn acknowledge(&mut self) {
        match self {
            #[cfg(target_os = "linux")]
            Self::Signal(_) => {}
            Self::Test {
                acknowledged,
                pending_epoch,
                ..
            } => {
                let epoch = pending_epoch
                    .take()
                    .expect("test notifier event must be pending");
                acknowledged.send_replace(epoch);
            }
        }
    }

    #[cfg(not(test))]
    fn acknowledge(&mut self) {}
}

pub(super) struct PreparedLinuxNotifier {
    runtime: Handle,
    source: LinuxNotifierSource,
}

impl PreparedLinuxNotifier {
    #[cfg(target_os = "linux")]
    pub(super) fn runtime_handle(&self) -> Handle {
        self.runtime.clone()
    }

    #[cfg(target_os = "linux")]
    pub(super) fn production() -> Result<Self, LinuxNotifierInitializationError> {
        let runtime = Handle::try_current()
            .map_err(|_| LinuxNotifierInitializationError::TokioRuntimeUnavailable)?;
        Self::production_with_runtime(runtime)
    }

    #[cfg(target_os = "linux")]
    pub(super) fn production_with_runtime(
        runtime: Handle,
    ) -> Result<Self, LinuxNotifierInitializationError> {
        let _runtime_guard = runtime.enter();
        let stream = match catch_unwind(AssertUnwindSafe(|| signal(SignalKind::child()))) {
            Ok(Ok(stream)) => stream,
            Ok(Err(error)) => {
                return Err(LinuxNotifierInitializationError::SignalInstallFailed {
                    errno: error.raw_os_error(),
                });
            }
            Err(_) => return Err(LinuxNotifierInitializationError::SignalInstallPanicked),
        };
        Ok(Self {
            runtime,
            source: LinuxNotifierSource::Signal(stream),
        })
    }

    #[cfg(test)]
    pub(super) fn for_test() -> (Self, TestLinuxNotifierHandle) {
        let runtime = Handle::try_current().expect("test notifier requires a Tokio runtime");
        let (sender, receiver) = watch::channel(0_u64);
        let (acknowledged_tx, acknowledged_rx) = watch::channel(0_u64);
        (
            Self {
                runtime,
                source: LinuxNotifierSource::Test {
                    stream: receiver,
                    acknowledged: acknowledged_tx,
                    pending_epoch: None,
                },
            },
            TestLinuxNotifierHandle {
                sender: Some(sender),
                acknowledged: acknowledged_rx,
            },
        )
    }

    pub(super) fn start(
        self,
        reaper: WeakSharedUnixReaper,
        registration_notify: Arc<Notify>,
    ) -> LinuxNotifierWorkerHandle {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let guard = WorkerExitGuard::new(reaper.clone());
        let join = self.runtime.spawn(run_worker(
            self.source,
            reaper,
            registration_notify,
            shutdown_rx,
            guard,
        ));
        LinuxNotifierWorkerHandle {
            shutdown_tx: Some(shutdown_tx),
            join: Some(join),
        }
    }
}

#[cfg(test)]
pub(super) struct TestLinuxNotifierHandle {
    sender: Option<watch::Sender<u64>>,
    acknowledged: watch::Receiver<u64>,
}

#[cfg(test)]
impl TestLinuxNotifierHandle {
    pub(super) fn notify_coalesced(&self, count: u64) {
        assert!(count != 0, "a coalesced notification must contain an edge");
        self.sender
            .as_ref()
            .expect("test notifier is open")
            .send_modify(|epoch| *epoch = epoch.wrapping_add(count));
    }

    pub(super) async fn notify_coalesced_and_wait(&mut self, count: u64) {
        assert!(count != 0, "a coalesced notification must contain an edge");
        let mut target = 0;
        self.sender
            .as_ref()
            .expect("test notifier is open")
            .send_modify(|epoch| {
                *epoch = epoch.wrapping_add(count);
                target = *epoch;
            });
        while *self.acknowledged.borrow_and_update() != target {
            self.acknowledged
                .changed()
                .await
                .expect("test notifier worker must acknowledge the edge");
        }
    }

    pub(super) fn close(mut self) {
        self.sender.take();
    }
}

pub(super) struct LinuxNotifierWorkerHandle {
    shutdown_tx: Option<oneshot::Sender<()>>,
    join: Option<tokio::task::JoinHandle<()>>,
}

impl LinuxNotifierWorkerHandle {
    pub(super) async fn shutdown(mut self) -> Result<(), ReaperNotifierInvariant> {
        if let Some(sender) = self.shutdown_tx.take() {
            let _ = sender.send(());
        }
        let join = self.join.take().expect("notifier worker join handle");
        join.await
            .map_err(|_| ReaperNotifierInvariant::WorkerJoinFailed)
    }
}

impl Drop for LinuxNotifierWorkerHandle {
    fn drop(&mut self) {
        if let Some(join) = self.join.take() {
            join.abort();
        }
    }
}

async fn run_worker(
    mut source: LinuxNotifierSource,
    reaper: WeakSharedUnixReaper,
    registration_notify: Arc<Notify>,
    mut shutdown_rx: oneshot::Receiver<()>,
    mut exit_guard: WorkerExitGuard,
) {
    let active_result = AssertUnwindSafe(run_active_worker(
        &mut source,
        &reaper,
        &registration_notify,
        &mut shutdown_rx,
    ))
    .catch_unwind()
    .await;
    let failure = match active_result {
        Ok(ActiveWorkerExit::Shutdown | ActiveWorkerExit::ReaperGone) => {
            exit_guard.disarm();
            return;
        }
        Ok(ActiveWorkerExit::StreamClosed) => ReaperNotifierInvariant::StreamClosed,
        Err(_) => ReaperNotifierInvariant::WorkerLost,
    };
    exit_guard.set_failure(failure);
    if let Some(strong) = reaper.upgrade() {
        strong.publish_notifier_failure(failure);
    } else {
        exit_guard.disarm();
        return;
    }
    run_retained_hidden_scheduler(reaper, registration_notify, shutdown_rx, exit_guard).await;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ActiveWorkerExit {
    Shutdown,
    ReaperGone,
    StreamClosed,
}

async fn run_active_worker(
    source: &mut LinuxNotifierSource,
    reaper: &WeakSharedUnixReaper,
    registration_notify: &Arc<Notify>,
    shutdown_rx: &mut oneshot::Receiver<()>,
) -> ActiveWorkerExit {
    let mut pidfds = FuturesUnordered::<PidfdReadyFuture>::new();
    loop {
        let Some(strong) = reaper.upgrade() else {
            return ActiveWorkerExit::ReaperGone;
        };
        for pidfd in strong.take_pending_pidfd_readiness() {
            pidfds.push(pidfd);
        }
        drop(strong);

        tokio::select! {
            _ = &mut *shutdown_rx => return ActiveWorkerExit::Shutdown,
            event = source.recv() => {
                if event.is_none() {
                    return ActiveWorkerExit::StreamClosed;
                }
                if let Some(strong) = reaper.upgrade() {
                    strong.scan_all_live_registrations();
                } else {
                    return ActiveWorkerExit::ReaperGone;
                }
                source.acknowledge();
            }
            _ = registration_notify.notified() => {}
            Some((identity, readiness)) = pidfds.next(), if !pidfds.is_empty() => {
                if readiness.is_ok() {
                    if let Some(strong) = reaper.upgrade() {
                        let _ = strong.poll(identity);
                    } else {
                        return ActiveWorkerExit::ReaperGone;
                    }
                }
            }
        }
    }
}

async fn run_retained_hidden_scheduler(
    reaper: WeakSharedUnixReaper,
    registration_notify: Arc<Notify>,
    mut shutdown_rx: oneshot::Receiver<()>,
    mut exit_guard: WorkerExitGuard,
) {
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(25));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = &mut shutdown_rx => {
                exit_guard.disarm();
                return;
            }
            _ = registration_notify.notified() => {
                if let Some(strong) = reaper.upgrade() {
                    strong.scan_hidden_cleanup_registrations();
                } else {
                    exit_guard.disarm();
                    return;
                }
            }
            _ = interval.tick() => {
                if let Some(strong) = reaper.upgrade() {
                    strong.scan_hidden_cleanup_registrations();
                } else {
                    exit_guard.disarm();
                    return;
                }
            }
        }
    }
}

struct WorkerExitGuard {
    reaper: WeakSharedUnixReaper,
    failure: ReaperNotifierInvariant,
    armed: bool,
}

impl WorkerExitGuard {
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

impl Drop for WorkerExitGuard {
    fn drop(&mut self) {
        if self.armed {
            if let Some(reaper) = self.reaper.upgrade() {
                reaper.publish_notifier_failure(self.failure);
            }
        }
    }
}
