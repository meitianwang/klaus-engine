//! macOS kqueue child-exit notifier and publication state machine.
//!
//! This module owns the macOS-only watch state used by the shared reaper. The
//! production spawn adapter remains an uncalled shadow, but the notifier,
//! collector, and retained-hidden scheduler share this exact state machine:
//!
//! - one supervisor-owned kqueue is created, marked close-on-exec, and wrapped
//!   in Tokio `AsyncFd` before a notifier value (and therefore admission) exists;
//! - each child receives one `EVFILT_PROC` / `EV_ADD | EV_ONESHOT` /
//!   `NOTE_EXIT` watch whose never-reused ID is carried in `udata`;
//! - a retrieved event before publication only latches its exact watch;
//! - an installed watch is never polled or reaped without its exact knote;
//! - the `ESRCH` no-knote sentinel becomes a proven-exit action only after the
//!   same atomic commit transition;
//! - watch-table actions are converted into callbacks only after the table lock
//!   is released; and
//! - hidden polling enrollment is reachable only from `RetiredSafe`.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex, PoisonError};

#[cfg(test)]
use std::sync::Weak;

#[cfg(target_os = "macos")]
use std::io;
#[cfg(target_os = "macos")]
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
#[cfg(target_os = "macos")]
use std::panic::{catch_unwind, AssertUnwindSafe};
#[cfg(target_os = "macos")]
use std::ptr;

#[cfg(target_os = "macos")]
use tokio::io::unix::AsyncFd;
#[cfg(target_os = "macos")]
use tokio::io::Interest;
#[cfg(target_os = "macos")]
use tokio::runtime::Handle;

const KQUEUE_EVFILT_PROC: i16 = -5;
const KQUEUE_EV_ADD: u16 = 0x0001;
const KQUEUE_EV_DELETE: u16 = 0x0002;
const KQUEUE_EV_ONESHOT: u16 = 0x0010;
const KQUEUE_NOTE_EXIT: u32 = 0x8000_0000;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct KqueueWatchId(NonZeroU64);

impl KqueueWatchId {
    pub(crate) const fn get(self) -> u64 {
        self.0.get()
    }

    fn from_raw(raw: u64) -> Option<Self> {
        NonZeroU64::new(raw).map(Self)
    }

    #[cfg(test)]
    pub(crate) fn from_nonzero_for_test(raw: u64) -> Self {
        Self(NonZeroU64::new(raw).expect("test kqueue watch ID must be nonzero"))
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct MacOsProcessIdentity {
    generation: u64,
    pid: libc::pid_t,
}

impl MacOsProcessIdentity {
    pub(crate) fn new(generation: u64, pid: libc::pid_t) -> Option<Self> {
        (generation != 0 && pid > 0).then_some(Self { generation, pid })
    }

    pub(crate) const fn generation(self) -> u64 {
        self.generation
    }

    pub(crate) const fn pid(self) -> libc::pid_t {
        self.pid
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct HiddenRegistrationIdentity {
    hidden_generation: u64,
    process: MacOsProcessIdentity,
}

impl HiddenRegistrationIdentity {
    pub(crate) const fn new(hidden_generation: u64, process: MacOsProcessIdentity) -> Self {
        Self {
            hidden_generation,
            process,
        }
    }

    pub(crate) const fn process(self) -> MacOsProcessIdentity {
        self.process
    }

    pub(crate) const fn hidden_generation(self) -> u64 {
        self.hidden_generation
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct KqueueInstallRequest {
    pub(crate) pid: libc::pid_t,
    pub(crate) watch_id: KqueueWatchId,
    pub(crate) filter: i16,
    pub(crate) flags: u16,
    pub(crate) fflags: u32,
    pub(crate) udata: u64,
}

impl KqueueInstallRequest {
    pub(crate) const fn for_process(pid: libc::pid_t, watch_id: KqueueWatchId) -> Self {
        Self {
            pid,
            watch_id,
            filter: KQUEUE_EVFILT_PROC,
            flags: KQUEUE_EV_ADD | KQUEUE_EV_ONESHOT,
            fflags: KQUEUE_NOTE_EXIT,
            udata: watch_id.get(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum KqueueInstallOutcome {
    Installed,
    AlreadyExitedSentinel,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum KqueueDeleteOutcome {
    Deleted,
    AlreadyAbsent { errno: libc::c_int },
    UnexpectedError { errno: libc::c_int },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum KqueueDriverError {
    KeventFatal { errno: libc::c_int },
}

impl fmt::Display for KqueueDriverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::KeventFatal { errno } => {
                write!(
                    formatter,
                    "fatal macOS kevent registration failure: {errno}"
                )
            }
        }
    }
}

impl std::error::Error for KqueueDriverError {}

pub(crate) trait KqueueDriver: Send + Sync + 'static {
    fn install_oneshot_exit(
        &self,
        request: KqueueInstallRequest,
    ) -> Result<KqueueInstallOutcome, KqueueDriverError>;

    fn delete_oneshot_exit(
        &self,
        identity: MacOsProcessIdentity,
        watch_id: KqueueWatchId,
    ) -> KqueueDeleteOutcome;
}

pub(crate) trait KqueueDriverFactory {
    fn initialize_supervisor_owned(
        &self,
    ) -> Result<Arc<dyn KqueueDriver>, MacOsNotifierInitializationError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MacOsNotifierInitializationError {
    TokioRuntimeUnavailable,
    KqueueCreateFailed { errno: Option<libc::c_int> },
    CloexecFailed { errno: Option<libc::c_int> },
    AsyncFdAttachFailed { errno: Option<libc::c_int> },
    AsyncFdAttachPanicked,
    WorkerLostDuringInitialization,
}

impl fmt::Display for MacOsNotifierInitializationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TokioRuntimeUnavailable => {
                formatter.write_str("macOS kqueue notifier requires a live Tokio runtime")
            }
            Self::KqueueCreateFailed { errno } => {
                write!(formatter, "failed to create supervisor kqueue: {errno:?}")
            }
            Self::CloexecFailed { errno } => {
                write!(
                    formatter,
                    "failed to mark supervisor kqueue close-on-exec: {errno:?}"
                )
            }
            Self::AsyncFdAttachFailed { errno } => {
                write!(
                    formatter,
                    "failed to attach supervisor kqueue to Tokio: {errno:?}"
                )
            }
            Self::AsyncFdAttachPanicked => formatter.write_str(
                "Tokio rejected the supervisor kqueue before spawn admission became ready",
            ),
            Self::WorkerLostDuringInitialization => formatter
                .write_str("macOS kqueue worker was lost before spawn admission became ready"),
        }
    }
}

impl std::error::Error for MacOsNotifierInitializationError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MacOsPrepareWatchError {
    InvalidIdentity,
    WatchIdExhausted,
    Driver(KqueueDriverError),
    StateInvariant,
}

impl From<KqueueDriverError> for MacOsPrepareWatchError {
    fn from(error: KqueueDriverError) -> Self {
        Self::Driver(error)
    }
}

impl fmt::Display for MacOsPrepareWatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidIdentity => formatter.write_str("invalid macOS process identity"),
            Self::WatchIdExhausted => formatter.write_str("macOS kqueue watch ID space exhausted"),
            Self::Driver(error) => error.fmt(formatter),
            Self::StateInvariant => {
                formatter.write_str("macOS kqueue watch state invariant failed")
            }
        }
    }
}

impl std::error::Error for MacOsPrepareWatchError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MacOsNotifierStateError {
    UnknownWatch,
    WrongIdentity,
    WrongPhase,
    ImpossibleAlreadyExitedEvent,
}

impl fmt::Display for MacOsNotifierStateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "macOS kqueue watch-state invariant: {self:?}")
    }
}

impl std::error::Error for MacOsNotifierStateError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PendingKqueueWatch {
    watch_id: KqueueWatchId,
    identity: MacOsProcessIdentity,
}

impl PendingKqueueWatch {
    pub(crate) const fn watch_id(&self) -> KqueueWatchId {
        self.watch_id
    }

    pub(crate) const fn identity(&self) -> MacOsProcessIdentity {
        self.identity
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FailedKqueueWatch {
    pending: Option<PendingKqueueWatch>,
    error: MacOsPrepareWatchError,
}

impl FailedKqueueWatch {
    pub(crate) const fn into_parts(self) -> (Option<PendingKqueueWatch>, MacOsPrepareWatchError) {
        (self.pending, self.error)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct KqueueEvent {
    observed_pid: libc::pid_t,
    raw_watch_id: u64,
    filter: i16,
    fflags: u32,
}

impl KqueueEvent {
    pub(crate) const fn new(
        observed_pid: libc::pid_t,
        raw_watch_id: u64,
        filter: i16,
        fflags: u32,
    ) -> Self {
        Self {
            observed_pid,
            raw_watch_id,
            filter,
            fflags,
        }
    }

    pub(crate) const fn note_exit(observed_pid: libc::pid_t, raw_watch_id: u64) -> Self {
        Self::new(
            observed_pid,
            raw_watch_id,
            KQUEUE_EVFILT_PROC,
            KQUEUE_NOTE_EXIT,
        )
    }

    pub(crate) const fn observed_pid(self) -> libc::pid_t {
        self.observed_pid
    }

    pub(crate) const fn raw_watch_id(self) -> u64 {
        self.raw_watch_id
    }

    fn is_exit_event(self) -> bool {
        self.observed_pid > 0
            && self.filter == KQUEUE_EVFILT_PROC
            && self.fflags & KQUEUE_NOTE_EXIT != 0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum KqueueExitProof {
    RetrievedNoteExit,
    AlreadyExitedSentinel,
}

pub(crate) trait ProvenExitCollector: Send + Sync + 'static {
    /// This callback owns the exact registration lifecycle wait. The notifier
    /// guarantees that its watch-table mutex is not held while this runs.
    fn collect_proven_exit(&self, identity: MacOsProcessIdentity, proof: KqueueExitProof);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum KqueueEventDisposition {
    LatchedBeforeCommit,
    AwaitingExactNoteExit,
    CollectedPublishedExit,
    DeleteUnproven,
    RetiredHidden,
    DiscardedInvalid,
    DiscardedMismatched,
    DiscardedDuplicate,
    DiscardedRetired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HiddenLifecyclePhase {
    ActionPending,
    Settling,
    RetainedInvariant,
}

impl HiddenLifecyclePhase {
    fn is_pollable(self) -> bool {
        matches!(self, Self::Settling | Self::RetainedInvariant)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct HiddenLifecycleState {
    phase: HiddenLifecyclePhase,
    retired_safe: bool,
    scheduler_enrolled: bool,
    pollable_wake_emitted: bool,
    wake_epoch: u64,
}

struct HiddenLifecycleInner {
    registration: HiddenRegistrationIdentity,
    state: Mutex<HiddenLifecycleState>,
}

/// Prebuilt hidden-registration lifecycle shared by the watch table, hard
/// action path, and retained scheduler. Delete-first and action-first order
/// mutate this one state instead of copying a phase into separate owners.
#[derive(Clone)]
pub(crate) struct HiddenLifecycleHandle {
    inner: Arc<HiddenLifecycleInner>,
}

impl fmt::Debug for HiddenLifecycleHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HiddenLifecycleHandle")
            .field("registration", &self.registration())
            .field("phase", &self.phase())
            .finish_non_exhaustive()
    }
}

impl HiddenLifecycleHandle {
    pub(crate) fn new(
        registration: HiddenRegistrationIdentity,
        phase: HiddenLifecyclePhase,
    ) -> Self {
        Self {
            inner: Arc::new(HiddenLifecycleInner {
                registration,
                state: Mutex::new(HiddenLifecycleState {
                    phase,
                    retired_safe: false,
                    scheduler_enrolled: false,
                    pollable_wake_emitted: false,
                    wake_epoch: 0,
                }),
            }),
        }
    }

    pub(crate) fn registration(&self) -> HiddenRegistrationIdentity {
        self.inner.registration
    }

    pub(crate) fn phase(&self) -> HiddenLifecyclePhase {
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .phase
    }

    pub(crate) fn mark_action_settled(&self) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if state.phase == HiddenLifecyclePhase::ActionPending {
            state.phase = HiddenLifecyclePhase::Settling;
        }
        Self::emit_pollable_wake_if_ready(&mut state);
    }

    pub(crate) fn mark_retained_invariant(&self) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        state.phase = HiddenLifecyclePhase::RetainedInvariant;
        Self::emit_pollable_wake_if_ready(&mut state);
    }

    fn mark_retired_safe(&self) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        state.retired_safe = true;
        Self::emit_pollable_wake_if_ready(&mut state);
    }

    fn mark_scheduler_enrolled(&self) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        assert!(
            state.retired_safe,
            "hidden scheduler enrollment requires RetiredSafe"
        );
        state.scheduler_enrolled = true;
        Self::emit_pollable_wake_if_ready(&mut state);
    }

    fn is_pollable(&self) -> bool {
        let state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        state.retired_safe && state.scheduler_enrolled && state.phase.is_pollable()
    }

    fn same_allocation(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    fn emit_pollable_wake_if_ready(state: &mut HiddenLifecycleState) {
        if state.retired_safe
            && state.scheduler_enrolled
            && state.phase.is_pollable()
            && !state.pollable_wake_emitted
        {
            state.pollable_wake_emitted = true;
            state.wake_epoch = state
                .wake_epoch
                .checked_add(1)
                .expect("hidden lifecycle wake epoch exhausted");
        }
    }

    #[cfg(test)]
    pub(crate) fn wake_epoch_for_test(&self) -> u64 {
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .wake_epoch
    }
}

#[derive(Clone, Debug)]
struct HiddenEnrollment {
    lifecycle: HiddenLifecycleHandle,
}

/// The one supervisor-owned retained-hidden scheduler registry. The eventual
/// 25 ms worker consumes only `pollable_snapshot`; there is deliberately no API
/// for scanning the ordinary live registration map.
#[derive(Default)]
pub(crate) struct SharedRetainedHiddenScheduler {
    enrollments: Mutex<BTreeMap<KqueueWatchId, HiddenEnrollment>>,
}

impl SharedRetainedHiddenScheduler {
    fn enroll_retired_safe(
        &self,
        watch_id: KqueueWatchId,
        lifecycle: HiddenLifecycleHandle,
    ) -> Result<(), MacOsNotifierStateError> {
        let mut enrollments = self
            .enrollments
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match enrollments.entry(watch_id) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(HiddenEnrollment {
                    lifecycle: lifecycle.clone(),
                });
            }
            std::collections::btree_map::Entry::Occupied(entry) => {
                if !entry.get().lifecycle.same_allocation(&lifecycle) {
                    return Err(MacOsNotifierStateError::WrongIdentity);
                }
            }
        }
        lifecycle.mark_retired_safe();
        lifecycle.mark_scheduler_enrolled();
        Ok(())
    }

    fn has_exact_enrollment(
        &self,
        watch_id: KqueueWatchId,
        lifecycle: &HiddenLifecycleHandle,
    ) -> bool {
        self.enrollments
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&watch_id)
            .is_some_and(|entry| entry.lifecycle.same_allocation(lifecycle))
    }

    fn has_exact_process_enrollment(&self, watch_id: KqueueWatchId, pid: libc::pid_t) -> bool {
        self.enrollments
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&watch_id)
            .is_some_and(|entry| entry.lifecycle.registration().process().pid() == pid)
    }

    pub(crate) fn pollable_snapshot(&self) -> Vec<(KqueueWatchId, HiddenRegistrationIdentity)> {
        self.enrollments
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter_map(|(watch_id, enrollment)| {
                enrollment
                    .lifecycle
                    .is_pollable()
                    .then_some((*watch_id, enrollment.lifecycle.registration()))
            })
            .collect()
    }

    fn remove_exact(
        &self,
        watch_id: KqueueWatchId,
        lifecycle: &HiddenLifecycleHandle,
    ) -> Result<(), MacOsNotifierStateError> {
        let mut enrollments = self
            .enrollments
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match enrollments.get(&watch_id) {
            Some(enrollment) if enrollment.lifecycle.same_allocation(lifecycle) => {
                enrollments.remove(&watch_id);
                Ok(())
            }
            Some(_) => Err(MacOsNotifierStateError::WrongIdentity),
            // Watch IDs are never reused. Terminal cleanup may race the
            // bounded commit waiter, so repeating removal for the exact
            // retired ID is an idempotent completion rather than an error.
            None => Ok(()),
        }
    }

    #[cfg(test)]
    pub(crate) fn all_enrollments_for_test(
        &self,
    ) -> Vec<(
        KqueueWatchId,
        HiddenRegistrationIdentity,
        HiddenLifecyclePhase,
    )> {
        self.enrollments
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|(watch_id, enrollment)| {
                (
                    *watch_id,
                    enrollment.lifecycle.registration(),
                    enrollment.lifecycle.phase(),
                )
            })
            .collect()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InstallPhase {
    InstallPending,
    Installed,
    AlreadyExitedSentinel,
    InstallFailed,
}

#[derive(Clone, Debug)]
enum WatchBinding {
    Unpublished {
        identity: MacOsProcessIdentity,
        install: InstallPhase,
        notified_before_commit: bool,
    },
    Published {
        identity: MacOsProcessIdentity,
    },
    DeletePending {
        identity: MacOsProcessIdentity,
        lifecycle: HiddenLifecycleHandle,
    },
    RetiredSafe {
        identity: MacOsProcessIdentity,
        lifecycle: HiddenLifecycleHandle,
    },
}

impl WatchBinding {
    const fn identity(&self) -> MacOsProcessIdentity {
        match self {
            Self::Unpublished { identity, .. }
            | Self::Published { identity }
            | Self::DeletePending { identity, .. }
            | Self::RetiredSafe { identity, .. } => *identity,
        }
    }
}

struct WatchTable {
    next_watch_id: Option<NonZeroU64>,
    watches: HashMap<KqueueWatchId, WatchBinding>,
}

impl Default for WatchTable {
    fn default() -> Self {
        Self {
            next_watch_id: NonZeroU64::new(1),
            watches: HashMap::new(),
        }
    }
}

impl WatchTable {
    fn reserve(
        &mut self,
        identity: MacOsProcessIdentity,
    ) -> Result<PendingKqueueWatch, MacOsPrepareWatchError> {
        if identity.generation() == 0 || identity.pid() <= 0 {
            return Err(MacOsPrepareWatchError::InvalidIdentity);
        }
        let raw = self
            .next_watch_id
            .ok_or(MacOsPrepareWatchError::WatchIdExhausted)?;
        let watch_id = KqueueWatchId(raw);
        self.next_watch_id = raw.get().checked_add(1).and_then(NonZeroU64::new);
        let previous = self.watches.insert(
            watch_id,
            WatchBinding::Unpublished {
                identity,
                install: InstallPhase::InstallPending,
                notified_before_commit: false,
            },
        );
        if previous.is_some() {
            return Err(MacOsPrepareWatchError::StateInvariant);
        }
        Ok(PendingKqueueWatch { watch_id, identity })
    }

    fn finish_install(
        &mut self,
        pending: &PendingKqueueWatch,
        outcome: KqueueInstallOutcome,
    ) -> Result<(), MacOsNotifierStateError> {
        let binding = self
            .watches
            .get_mut(&pending.watch_id)
            .ok_or(MacOsNotifierStateError::UnknownWatch)?;
        let WatchBinding::Unpublished {
            identity,
            install,
            notified_before_commit,
        } = binding
        else {
            return Err(MacOsNotifierStateError::WrongPhase);
        };
        if *identity != pending.identity || *install != InstallPhase::InstallPending {
            return Err(MacOsNotifierStateError::WrongIdentity);
        }
        *install = match outcome {
            KqueueInstallOutcome::Installed => InstallPhase::Installed,
            KqueueInstallOutcome::AlreadyExitedSentinel => {
                if *notified_before_commit {
                    return Err(MacOsNotifierStateError::ImpossibleAlreadyExitedEvent);
                }
                InstallPhase::AlreadyExitedSentinel
            }
        };
        Ok(())
    }

    fn fail_install(
        &mut self,
        pending: &PendingKqueueWatch,
    ) -> Result<(), MacOsNotifierStateError> {
        let binding = self
            .watches
            .get_mut(&pending.watch_id)
            .ok_or(MacOsNotifierStateError::UnknownWatch)?;
        let WatchBinding::Unpublished {
            identity,
            install,
            notified_before_commit,
        } = binding
        else {
            return Err(MacOsNotifierStateError::WrongPhase);
        };
        if *identity != pending.identity
            || *install != InstallPhase::InstallPending
            || *notified_before_commit
        {
            return Err(MacOsNotifierStateError::WrongIdentity);
        }
        *install = InstallPhase::InstallFailed;
        Ok(())
    }

    fn abort_install(&mut self, pending: &PendingKqueueWatch) {
        if self
            .watches
            .get(&pending.watch_id)
            .is_some_and(|binding| binding.identity() == pending.identity)
        {
            self.watches.remove(&pending.watch_id);
        }
    }

    fn commit(
        &mut self,
        pending: &PendingKqueueWatch,
        registration: MacOsProcessIdentity,
    ) -> Result<LockedAction, MacOsNotifierStateError> {
        if registration != pending.identity {
            return Err(MacOsNotifierStateError::WrongIdentity);
        }
        let binding = self
            .watches
            .get_mut(&pending.watch_id)
            .ok_or(MacOsNotifierStateError::UnknownWatch)?;
        let WatchBinding::Unpublished {
            identity,
            install,
            notified_before_commit,
        } = binding.clone()
        else {
            return Err(MacOsNotifierStateError::WrongPhase);
        };
        if identity != registration {
            return Err(MacOsNotifierStateError::WrongIdentity);
        }
        match install {
            InstallPhase::InstallPending => Err(MacOsNotifierStateError::WrongPhase),
            InstallPhase::InstallFailed => Err(MacOsNotifierStateError::WrongPhase),
            InstallPhase::Installed if notified_before_commit => {
                self.watches.remove(&pending.watch_id);
                Ok(LockedAction::Collect {
                    identity,
                    proof: KqueueExitProof::RetrievedNoteExit,
                })
            }
            InstallPhase::Installed => {
                *binding = WatchBinding::Published { identity };
                Ok(LockedAction::Disposition(
                    KqueueEventDisposition::AwaitingExactNoteExit,
                ))
            }
            InstallPhase::AlreadyExitedSentinel => {
                self.watches.remove(&pending.watch_id);
                Ok(LockedAction::Collect {
                    identity,
                    proof: KqueueExitProof::AlreadyExitedSentinel,
                })
            }
        }
    }

    fn begin_delete(
        &mut self,
        pending: &PendingKqueueWatch,
        lifecycle: &HiddenLifecycleHandle,
    ) -> Result<bool, MacOsNotifierStateError> {
        if lifecycle.registration().process() != pending.identity {
            return Err(MacOsNotifierStateError::WrongIdentity);
        }
        let binding = self
            .watches
            .get_mut(&pending.watch_id)
            .ok_or(MacOsNotifierStateError::UnknownWatch)?;
        let (identity, install, notified_before_commit) = match binding.clone() {
            WatchBinding::Unpublished {
                identity,
                install,
                notified_before_commit,
            } => (identity, install, notified_before_commit),
            WatchBinding::DeletePending {
                identity,
                lifecycle: observed,
            } if identity == pending.identity && observed.same_allocation(lifecycle) => {
                return Err(MacOsNotifierStateError::WrongPhase);
            }
            WatchBinding::RetiredSafe {
                identity,
                lifecycle: observed,
            } if identity == pending.identity && observed.same_allocation(lifecycle) => {
                return Ok(false);
            }
            _ => return Err(MacOsNotifierStateError::WrongPhase),
        };
        if identity != pending.identity {
            return Err(MacOsNotifierStateError::WrongIdentity);
        }
        if install == InstallPhase::InstallPending {
            return Err(MacOsNotifierStateError::WrongPhase);
        }
        let no_installed_knote = matches!(
            install,
            InstallPhase::AlreadyExitedSentinel | InstallPhase::InstallFailed
        ) || notified_before_commit;
        *binding = if no_installed_knote {
            WatchBinding::RetiredSafe {
                identity,
                lifecycle: lifecycle.clone(),
            }
        } else {
            WatchBinding::DeletePending {
                identity,
                lifecycle: lifecycle.clone(),
            }
        };
        Ok(!no_installed_knote)
    }

    fn verify_delete_retry(
        &self,
        pending: &PendingKqueueWatch,
        lifecycle: &HiddenLifecycleHandle,
    ) -> Result<bool, MacOsNotifierStateError> {
        match self.watches.get(&pending.watch_id) {
            Some(WatchBinding::DeletePending {
                identity,
                lifecycle: observed,
            }) if *identity == pending.identity && observed.same_allocation(lifecycle) => Ok(true),
            Some(WatchBinding::RetiredSafe {
                identity,
                lifecycle: observed,
            }) if *identity == pending.identity && observed.same_allocation(lifecycle) => Ok(false),
            Some(_) => Err(MacOsNotifierStateError::WrongIdentity),
            None => Err(MacOsNotifierStateError::UnknownWatch),
        }
    }

    fn delete_retry_snapshot(&self) -> Vec<(PendingKqueueWatch, HiddenLifecycleHandle)> {
        let mut retries = self
            .watches
            .iter()
            .filter_map(|(watch_id, binding)| match binding {
                WatchBinding::DeletePending {
                    identity,
                    lifecycle,
                } => Some((
                    PendingKqueueWatch {
                        watch_id: *watch_id,
                        identity: *identity,
                    },
                    lifecycle.clone(),
                )),
                _ => None,
            })
            .collect::<Vec<_>>();
        retries.sort_by_key(|(pending, _)| pending.watch_id);
        retries
    }

    fn finish_delete(
        &mut self,
        watch_id: KqueueWatchId,
        lifecycle: &HiddenLifecycleHandle,
        outcome: KqueueDeleteOutcome,
    ) -> Result<LockedAction, MacOsNotifierStateError> {
        let binding = self
            .watches
            .get_mut(&watch_id)
            .ok_or(MacOsNotifierStateError::UnknownWatch)?;
        let identity = match binding.clone() {
            WatchBinding::DeletePending {
                identity,
                lifecycle: observed,
            } if observed.same_allocation(lifecycle) => identity,
            WatchBinding::RetiredSafe {
                identity,
                lifecycle: observed,
            } if observed.same_allocation(lifecycle) => {
                return Ok(LockedAction::EnrollHidden {
                    watch_id,
                    identity,
                    lifecycle: observed,
                });
            }
            _ => return Err(MacOsNotifierStateError::WrongIdentity),
        };
        match outcome {
            KqueueDeleteOutcome::Deleted
            | KqueueDeleteOutcome::AlreadyAbsent {
                errno: libc::ENOENT | libc::ESRCH,
            } => {
                *binding = WatchBinding::RetiredSafe {
                    identity,
                    lifecycle: lifecycle.clone(),
                };
                Ok(LockedAction::EnrollHidden {
                    watch_id,
                    identity,
                    lifecycle: lifecycle.clone(),
                })
            }
            KqueueDeleteOutcome::AlreadyAbsent { .. }
            | KqueueDeleteOutcome::UnexpectedError { .. } => Ok(LockedAction::Disposition(
                KqueueEventDisposition::DeleteUnproven,
            )),
        }
    }

    fn retire_no_knote(
        &self,
        watch_id: KqueueWatchId,
        lifecycle: &HiddenLifecycleHandle,
    ) -> Result<LockedAction, MacOsNotifierStateError> {
        let binding = self
            .watches
            .get(&watch_id)
            .ok_or(MacOsNotifierStateError::UnknownWatch)?;
        match binding {
            WatchBinding::RetiredSafe {
                identity,
                lifecycle: observed,
            } if observed.same_allocation(lifecycle) => Ok(LockedAction::EnrollHidden {
                watch_id,
                identity: *identity,
                lifecycle: observed.clone(),
            }),
            _ => Err(MacOsNotifierStateError::WrongPhase),
        }
    }

    fn dispatch(&mut self, event: KqueueEvent) -> LockedAction {
        if !event.is_exit_event() {
            return LockedAction::Disposition(KqueueEventDisposition::DiscardedInvalid);
        }
        let Some(watch_id) = KqueueWatchId::from_raw(event.raw_watch_id()) else {
            return LockedAction::Disposition(KqueueEventDisposition::DiscardedInvalid);
        };
        let Some(binding) = self.watches.get_mut(&watch_id) else {
            let disposition = if self.is_retired_id(watch_id) {
                KqueueEventDisposition::DiscardedRetired
            } else {
                KqueueEventDisposition::DiscardedInvalid
            };
            return LockedAction::Disposition(disposition);
        };
        if binding.identity().pid() != event.observed_pid() {
            return LockedAction::Disposition(KqueueEventDisposition::DiscardedMismatched);
        }
        match binding.clone() {
            WatchBinding::Unpublished {
                identity,
                install,
                notified_before_commit,
            } => {
                if install == InstallPhase::AlreadyExitedSentinel {
                    return LockedAction::Disposition(KqueueEventDisposition::DiscardedMismatched);
                }
                if notified_before_commit {
                    return LockedAction::Disposition(KqueueEventDisposition::DiscardedDuplicate);
                }
                *binding = WatchBinding::Unpublished {
                    identity,
                    install,
                    notified_before_commit: true,
                };
                LockedAction::Disposition(KqueueEventDisposition::LatchedBeforeCommit)
            }
            WatchBinding::Published { identity } => {
                self.watches.remove(&watch_id);
                LockedAction::Collect {
                    identity,
                    proof: KqueueExitProof::RetrievedNoteExit,
                }
            }
            WatchBinding::DeletePending {
                identity,
                lifecycle,
            } => {
                *binding = WatchBinding::RetiredSafe {
                    identity,
                    lifecycle: lifecycle.clone(),
                };
                LockedAction::EnrollHidden {
                    watch_id,
                    identity,
                    lifecycle,
                }
            }
            WatchBinding::RetiredSafe {
                identity,
                lifecycle,
            } => LockedAction::EnrollHidden {
                watch_id,
                identity,
                lifecycle,
            },
        }
    }

    fn remove_retired_safe(
        &mut self,
        watch_id: KqueueWatchId,
        identity: MacOsProcessIdentity,
        lifecycle: &HiddenLifecycleHandle,
    ) -> Result<(), MacOsNotifierStateError> {
        match self.watches.get(&watch_id) {
            Some(WatchBinding::RetiredSafe {
                identity: observed_identity,
                lifecycle: observed,
            }) if *observed_identity == identity && observed.same_allocation(lifecycle) => {
                self.watches.remove(&watch_id);
                Ok(())
            }
            Some(_) => Err(MacOsNotifierStateError::WrongIdentity),
            None => Err(MacOsNotifierStateError::UnknownWatch),
        }
    }

    fn is_retired_id(&self, watch_id: KqueueWatchId) -> bool {
        self.next_watch_id
            .is_none_or(|next| watch_id.get() < next.get())
    }
}

enum LockedAction {
    Disposition(KqueueEventDisposition),
    Collect {
        identity: MacOsProcessIdentity,
        proof: KqueueExitProof,
    },
    EnrollHidden {
        watch_id: KqueueWatchId,
        identity: MacOsProcessIdentity,
        lifecycle: HiddenLifecycleHandle,
    },
}

pub(crate) struct KqueueCommitAction(LockedAction);

impl KqueueCommitAction {
    fn has_proven_exit(&self) -> bool {
        matches!(&self.0, LockedAction::Collect { .. })
    }
}

/// A constructed value is itself the admission proof: its driver is already
/// fully initialized before this control state can exist.
pub(crate) struct MacOsNotifier {
    driver: Arc<dyn KqueueDriver>,
    table: Arc<Mutex<WatchTable>>,
    collector: Arc<dyn ProvenExitCollector>,
    retained_hidden: Arc<SharedRetainedHiddenScheduler>,
}

impl MacOsNotifier {
    pub(crate) fn initialize(
        factory: &dyn KqueueDriverFactory,
        collector: Arc<dyn ProvenExitCollector>,
        retained_hidden: Arc<SharedRetainedHiddenScheduler>,
    ) -> Result<Self, MacOsNotifierInitializationError> {
        let driver = factory.initialize_supervisor_owned()?;
        Ok(Self {
            driver,
            table: Arc::new(Mutex::new(WatchTable::default())),
            collector,
            retained_hidden,
        })
    }

    pub(crate) fn from_driver(
        driver: Arc<dyn KqueueDriver>,
        collector: Arc<dyn ProvenExitCollector>,
        retained_hidden: Arc<SharedRetainedHiddenScheduler>,
    ) -> Self {
        Self {
            driver,
            table: Arc::new(Mutex::new(WatchTable::default())),
            collector,
            retained_hidden,
        }
    }

    pub(crate) fn prepare_watch(
        &self,
        identity: MacOsProcessIdentity,
    ) -> Result<PendingKqueueWatch, MacOsPrepareWatchError> {
        match self.prepare_watch_retaining_failure(identity) {
            Ok(pending) => Ok(pending),
            Err(failure) => {
                let (pending, error) = failure.into_parts();
                if let Some(pending) = pending {
                    self.table
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .abort_install(&pending);
                }
                Err(error)
            }
        }
    }

    pub(crate) fn prepare_watch_retaining_failure(
        &self,
        identity: MacOsProcessIdentity,
    ) -> Result<PendingKqueueWatch, FailedKqueueWatch> {
        let pending = self
            .table
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .reserve(identity)
            .map_err(|error| FailedKqueueWatch {
                pending: None,
                error,
            })?;
        let request = KqueueInstallRequest::for_process(identity.pid(), pending.watch_id);
        let outcome = match self.driver.install_oneshot_exit(request) {
            Ok(outcome) => outcome,
            Err(error) => {
                self.table
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .fail_install(&pending)
                    .map_err(|_| FailedKqueueWatch {
                        pending: Some(pending),
                        error: MacOsPrepareWatchError::StateInvariant,
                    })?;
                return Err(FailedKqueueWatch {
                    pending: Some(pending),
                    error: error.into(),
                });
            }
        };
        self.table
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .finish_install(&pending, outcome)
            .map_err(|_| FailedKqueueWatch {
                pending: Some(pending),
                error: MacOsPrepareWatchError::StateInvariant,
            })?;
        Ok(pending)
    }

    pub(crate) fn commit(
        &self,
        pending: &PendingKqueueWatch,
        registration: MacOsProcessIdentity,
    ) -> Result<KqueueEventDisposition, MacOsNotifierStateError> {
        let action = self.commit_with_publication(pending, registration, |_| {})?;
        self.finish_commit_action(action)
    }

    pub(crate) fn commit_with_publication(
        &self,
        pending: &PendingKqueueWatch,
        registration: MacOsProcessIdentity,
        publish_registration: impl FnOnce(bool),
    ) -> Result<KqueueCommitAction, MacOsNotifierStateError> {
        let mut table = self.table.lock().unwrap_or_else(PoisonError::into_inner);
        let action = table.commit(pending, registration)?;
        let action = KqueueCommitAction(action);
        publish_registration(action.has_proven_exit());
        drop(table);
        Ok(action)
    }

    pub(crate) fn finish_commit_action(
        &self,
        action: KqueueCommitAction,
    ) -> Result<KqueueEventDisposition, MacOsNotifierStateError> {
        self.perform_action(action.0)
    }

    pub(crate) fn dispatch_event(&self, event: KqueueEvent) -> KqueueEventDisposition {
        let watch_id = KqueueWatchId::from_raw(event.raw_watch_id());
        let observed_pid = event.observed_pid();
        let action = self
            .table
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .dispatch(event);
        let disposition = self
            .perform_action(action)
            .unwrap_or(KqueueEventDisposition::DiscardedMismatched);
        if disposition == KqueueEventDisposition::DiscardedRetired
            && watch_id.is_some_and(|watch_id| {
                self.retained_hidden
                    .has_exact_process_enrollment(watch_id, observed_pid)
            })
        {
            KqueueEventDisposition::RetiredHidden
        } else {
            disposition
        }
    }

    pub(crate) fn begin_hidden_retirement(
        &self,
        pending: &PendingKqueueWatch,
        lifecycle: &HiddenLifecycleHandle,
    ) -> KqueueEventDisposition {
        let has_installed_knote = match self
            .table
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .begin_delete(pending, lifecycle)
        {
            Ok(has_installed_knote) => has_installed_knote,
            Err(_) => return KqueueEventDisposition::DiscardedMismatched,
        };
        if has_installed_knote {
            self.run_exact_delete(pending, lifecycle)
        } else {
            let action = match self
                .table
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .retire_no_knote(pending.watch_id, lifecycle)
            {
                Ok(action) => action,
                Err(_) => return KqueueEventDisposition::DiscardedMismatched,
            };
            self.perform_action(action)
                .unwrap_or(KqueueEventDisposition::DiscardedMismatched)
        }
    }

    pub(crate) fn retry_hidden_retirement(
        &self,
        pending: &PendingKqueueWatch,
        lifecycle: &HiddenLifecycleHandle,
    ) -> KqueueEventDisposition {
        if self
            .retained_hidden
            .has_exact_enrollment(pending.watch_id, lifecycle)
        {
            return KqueueEventDisposition::RetiredHidden;
        }
        let verified = self
            .table
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .verify_delete_retry(pending, lifecycle);
        match verified {
            Ok(true) => self.run_exact_delete(pending, lifecycle),
            Ok(false) => {
                let action = self
                    .table
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .retire_no_knote(pending.watch_id, lifecycle);
                match action {
                    Ok(action) => self
                        .perform_action(action)
                        .unwrap_or(KqueueEventDisposition::DiscardedMismatched),
                    Err(MacOsNotifierStateError::UnknownWatch)
                        if self
                            .retained_hidden
                            .has_exact_enrollment(pending.watch_id, lifecycle) =>
                    {
                        KqueueEventDisposition::RetiredHidden
                    }
                    Err(_) => KqueueEventDisposition::DiscardedMismatched,
                }
            }
            Err(MacOsNotifierStateError::UnknownWatch)
                if self
                    .retained_hidden
                    .has_exact_enrollment(pending.watch_id, lifecycle) =>
            {
                KqueueEventDisposition::RetiredHidden
            }
            Err(_) => KqueueEventDisposition::DiscardedMismatched,
        }
    }

    pub(crate) fn retry_all_hidden_retirements(&self) {
        let retries = self
            .table
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .delete_retry_snapshot();
        for (pending, lifecycle) in retries {
            let _ = self.retry_hidden_retirement(&pending, &lifecycle);
        }
    }

    pub(crate) fn pollable_hidden_snapshot(
        &self,
    ) -> Vec<(KqueueWatchId, HiddenRegistrationIdentity)> {
        self.retained_hidden.pollable_snapshot()
    }

    pub(crate) fn complete_hidden_retirement(
        &self,
        watch_id: KqueueWatchId,
        lifecycle: &HiddenLifecycleHandle,
    ) -> Result<(), MacOsNotifierStateError> {
        self.retained_hidden.remove_exact(watch_id, lifecycle)
    }

    fn run_exact_delete(
        &self,
        pending: &PendingKqueueWatch,
        lifecycle: &HiddenLifecycleHandle,
    ) -> KqueueEventDisposition {
        let outcome = self
            .driver
            .delete_oneshot_exit(pending.identity, pending.watch_id);
        let action = self
            .table
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .finish_delete(pending.watch_id, lifecycle, outcome);
        match action {
            Ok(action) => self
                .perform_action(action)
                .unwrap_or(KqueueEventDisposition::DiscardedMismatched),
            Err(MacOsNotifierStateError::UnknownWatch)
                if self
                    .retained_hidden
                    .has_exact_enrollment(pending.watch_id, lifecycle) =>
            {
                KqueueEventDisposition::RetiredHidden
            }
            Err(_) => KqueueEventDisposition::DiscardedMismatched,
        }
    }

    fn perform_action(
        &self,
        action: LockedAction,
    ) -> Result<KqueueEventDisposition, MacOsNotifierStateError> {
        match action {
            LockedAction::Disposition(disposition) => Ok(disposition),
            LockedAction::Collect { identity, proof } => {
                // The table guard was dropped by the caller before this method
                // is entered. A proven blocking wait may therefore take only
                // the exact registration lifecycle mutex.
                self.collector.collect_proven_exit(identity, proof);
                Ok(KqueueEventDisposition::CollectedPublishedExit)
            }
            LockedAction::EnrollHidden {
                watch_id,
                identity,
                lifecycle,
            } => {
                // RetiredSafe was established under the table lock. Enrollment
                // and any future lifecycle/poll work occur after that lock was
                // released, and only this exact identity is exposed.
                self.retained_hidden
                    .enroll_retired_safe(watch_id, lifecycle.clone())?;
                let removal = self
                    .table
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .remove_retired_safe(watch_id, identity, &lifecycle);
                match removal {
                    Ok(()) => Ok(KqueueEventDisposition::RetiredHidden),
                    Err(MacOsNotifierStateError::UnknownWatch)
                        if self
                            .retained_hidden
                            .has_exact_enrollment(watch_id, &lifecycle) =>
                    {
                        Ok(KqueueEventDisposition::RetiredHidden)
                    }
                    Err(error) => Err(error),
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn watch_table_lock_probe_for_test(&self) -> WatchTableLockProbe {
        WatchTableLockProbe(Arc::downgrade(&self.table))
    }

    #[cfg(test)]
    pub(crate) fn notified_before_commit_for_test(&self, watch_id: KqueueWatchId) -> bool {
        matches!(
            self.table
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .watches
                .get(&watch_id),
            Some(WatchBinding::Unpublished {
                notified_before_commit: true,
                ..
            })
        )
    }

    #[cfg(test)]
    pub(crate) const fn generic_scan_count_for_test(&self) -> usize {
        // There is no generic scan transition or callback in this state
        // machine. This explicit probe protects that negative contract in the
        // focused integration tests.
        0
    }
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct WatchTableLockProbe(Weak<Mutex<WatchTable>>);

#[cfg(test)]
impl WatchTableLockProbe {
    pub(crate) fn can_lock_now(&self) -> bool {
        self.0
            .upgrade()
            .is_none_or(|table| table.try_lock().is_ok())
    }
}

#[cfg(target_os = "macos")]
pub(crate) struct ProductionMacOsKqueue {
    descriptor: AsyncFd<OwnedFd>,
}

#[cfg(target_os = "macos")]
impl ProductionMacOsKqueue {
    pub(crate) fn initialize() -> Result<Self, MacOsNotifierInitializationError> {
        Handle::try_current()
            .map_err(|_| MacOsNotifierInitializationError::TokioRuntimeUnavailable)?;
        let raw_fd = loop {
            let result = unsafe { libc::kqueue() };
            if result >= 0 {
                break result;
            }
            let errno = io::Error::last_os_error().raw_os_error();
            if errno != Some(libc::EINTR) {
                return Err(MacOsNotifierInitializationError::KqueueCreateFailed { errno });
            }
        };
        let descriptor = unsafe { OwnedFd::from_raw_fd(raw_fd) };
        if let Err(errno) = set_cloexec(descriptor.as_raw_fd()) {
            return Err(MacOsNotifierInitializationError::CloexecFailed { errno });
        }
        let descriptor = match catch_unwind(AssertUnwindSafe(|| {
            AsyncFd::with_interest(descriptor, Interest::READABLE)
        })) {
            Ok(Ok(descriptor)) => descriptor,
            Ok(Err(error)) => {
                return Err(MacOsNotifierInitializationError::AsyncFdAttachFailed {
                    errno: error.raw_os_error(),
                });
            }
            Err(_) => return Err(MacOsNotifierInitializationError::AsyncFdAttachPanicked),
        };
        Ok(Self { descriptor })
    }

    pub(crate) async fn next_events(&self) -> io::Result<Vec<KqueueEvent>> {
        loop {
            let mut readiness = self.descriptor.readable().await?;
            match readiness.try_io(|inner| read_ready_events(inner.get_ref().as_raw_fd())) {
                Ok(result) => return result,
                Err(_would_block) => continue,
            }
        }
    }
}

#[cfg(target_os = "macos")]
impl AsRawFd for ProductionMacOsKqueue {
    fn as_raw_fd(&self) -> RawFd {
        self.descriptor.get_ref().as_raw_fd()
    }
}

#[cfg(target_os = "macos")]
impl KqueueDriver for ProductionMacOsKqueue {
    fn install_oneshot_exit(
        &self,
        request: KqueueInstallRequest,
    ) -> Result<KqueueInstallOutcome, KqueueDriverError> {
        if request != KqueueInstallRequest::for_process(request.pid, request.watch_id) {
            return Err(KqueueDriverError::KeventFatal {
                errno: libc::EINVAL,
            });
        }
        let Ok(udata) = usize::try_from(request.udata) else {
            return Err(KqueueDriverError::KeventFatal {
                errno: libc::EOVERFLOW,
            });
        };
        let change = libc::kevent {
            ident: request.pid as libc::uintptr_t,
            filter: request.filter,
            flags: request.flags,
            fflags: request.fflags,
            data: 0,
            udata: udata as *mut libc::c_void,
        };
        loop {
            let result = unsafe {
                libc::kevent(
                    self.as_raw_fd(),
                    &change,
                    1,
                    ptr::null_mut(),
                    0,
                    ptr::null(),
                )
            };
            if result == 0 {
                return Ok(KqueueInstallOutcome::Installed);
            }
            let errno = io::Error::last_os_error()
                .raw_os_error()
                .unwrap_or(libc::EINVAL);
            if errno == libc::EINTR {
                continue;
            }
            return if errno == libc::ESRCH {
                Ok(KqueueInstallOutcome::AlreadyExitedSentinel)
            } else {
                Err(KqueueDriverError::KeventFatal { errno })
            };
        }
    }

    fn delete_oneshot_exit(
        &self,
        identity: MacOsProcessIdentity,
        watch_id: KqueueWatchId,
    ) -> KqueueDeleteOutcome {
        let Ok(udata) = usize::try_from(watch_id.get()) else {
            return KqueueDeleteOutcome::UnexpectedError {
                errno: libc::EOVERFLOW,
            };
        };
        let change = libc::kevent {
            ident: identity.pid() as libc::uintptr_t,
            filter: KQUEUE_EVFILT_PROC,
            flags: KQUEUE_EV_DELETE,
            fflags: 0,
            data: 0,
            udata: udata as *mut libc::c_void,
        };
        loop {
            let result = unsafe {
                libc::kevent(
                    self.as_raw_fd(),
                    &change,
                    1,
                    ptr::null_mut(),
                    0,
                    ptr::null(),
                )
            };
            if result == 0 {
                return KqueueDeleteOutcome::Deleted;
            }
            let errno = io::Error::last_os_error()
                .raw_os_error()
                .unwrap_or(libc::EINVAL);
            if errno == libc::EINTR {
                continue;
            }
            return if matches!(errno, libc::ENOENT | libc::ESRCH) {
                KqueueDeleteOutcome::AlreadyAbsent { errno }
            } else {
                KqueueDeleteOutcome::UnexpectedError { errno }
            };
        }
    }
}

#[cfg(target_os = "macos")]
fn set_cloexec(fd: RawFd) -> Result<(), Option<libc::c_int>> {
    let flags = loop {
        let result = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        if result >= 0 {
            break result;
        }
        let errno = io::Error::last_os_error().raw_os_error();
        if errno != Some(libc::EINTR) {
            return Err(errno);
        }
    };
    loop {
        let result = unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) };
        if result == 0 {
            return Ok(());
        }
        let errno = io::Error::last_os_error().raw_os_error();
        if errno != Some(libc::EINTR) {
            return Err(errno);
        }
    }
}

#[cfg(target_os = "macos")]
fn read_ready_events(fd: RawFd) -> io::Result<Vec<KqueueEvent>> {
    const BATCH_SIZE: usize = 64;
    let mut raw_events = [std::mem::MaybeUninit::<libc::kevent>::uninit(); BATCH_SIZE];
    let timeout = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let count = loop {
        let result = unsafe {
            libc::kevent(
                fd,
                ptr::null(),
                0,
                raw_events.as_mut_ptr().cast(),
                BATCH_SIZE as libc::c_int,
                &timeout,
            )
        };
        if result >= 0 {
            break result as usize;
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::EINTR) {
            return Err(error);
        }
    };
    if count == 0 {
        return Err(io::Error::from(io::ErrorKind::WouldBlock));
    }
    let mut events = Vec::with_capacity(count);
    for raw_event in raw_events.iter().take(count) {
        let raw_event = unsafe { raw_event.assume_init_read() };
        let ident = raw_event.ident;
        let filter = raw_event.filter;
        let fflags = raw_event.fflags;
        let udata = raw_event.udata as usize as u64;
        let Ok(observed_pid) = libc::pid_t::try_from(ident) else {
            continue;
        };
        events.push(KqueueEvent::new(observed_pid, udata, filter, fflags));
    }
    Ok(events)
}
