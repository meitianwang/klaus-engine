//! Rust 1.93 Unix fork-fallback shadow.
//!
//! This is deliberately narrower than the eventual platform backend. It owns
//! one real, synchronous Rust 1.93 fork/exec fallback transaction for standard
//! pipe/inherit/null stdio, but it is not wired to a production call site and
//! does not claim to implement the complete runtime process supervisor. Every
//! other image dialect or spawn route is rejected before descriptor creation.

use std::fmt;
use std::io;
#[cfg(test)]
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
#[cfg(test)]
use std::os::unix::process::ExitStatusExt;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
#[cfg(test)]
use std::process::ExitStatus;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::task::{ready, Context, Poll};

use libc::{c_char, c_int, pid_t};
use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncRead, AsyncWrite, Interest, ReadBuf};
use tokio::runtime::Handle;

use super::process_environment::with_process_environment_lock;
use super::reaper::UnixSpawnAdmission;
use super::unix::{UnixEnvironmentImage, UnixExecImage};
use super::{ProcessImageDialect, ProcessTopology};

const EXEC_ERROR_FOOTER: [u8; 4] = *b"NOEX";
const EXEC_ERROR_RECORD_LEN: usize = 8;
const DEV_NULL: &[u8] = b"/dev/null\0";

/// Serializes every descriptor creation and fork owned by this shadow.
///
/// The final backend will replace this local lock with the project-wide
/// descriptor/fork lock shared by PTY, Node socketpair, and every other route.
pub(super) static SHADOW_FORK_DESCRIPTOR_LOCK: Mutex<()> = Mutex::new(());

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum UnixStdioDisposition {
    Pipe,
    Inherit,
    Null,
}

pub(super) const MAX_EXTRA_STDIO: usize = 4;
pub(super) const MAX_UNIX_STDIO: usize = 3 + MAX_EXTRA_STDIO;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum UnixExtraPipeDirection {
    ChildToHost,
    HostToChild,
    Duplex,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct UnixExtraStdioPlan {
    child_fd: u32,
    direction: UnixExtraPipeDirection,
}

impl UnixExtraStdioPlan {
    pub(super) const fn new(child_fd: u32, direction: UnixExtraPipeDirection) -> Self {
        Self {
            child_fd,
            direction,
        }
    }

    pub(super) const fn child_fd(self) -> u32 {
        self.child_fd
    }

    pub(super) const fn direction(self) -> UnixExtraPipeDirection {
        self.direction
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum UnixStdioPlanError {
    TooManyExtraPipes { actual: usize },
    NonContiguousExtraFd { expected: u32, actual: u32 },
    PtyCannotHaveExtraPipes,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct UnixStdioPlan {
    stdin: UnixStdioDisposition,
    stdout: UnixStdioDisposition,
    stderr: UnixStdioDisposition,
    extra: [Option<UnixExtraStdioPlan>; MAX_EXTRA_STDIO],
    extra_len: usize,
    pty: bool,
}

impl UnixStdioPlan {
    pub(super) const fn new(
        stdin: UnixStdioDisposition,
        stdout: UnixStdioDisposition,
        stderr: UnixStdioDisposition,
    ) -> Self {
        Self {
            stdin,
            stdout,
            stderr,
            extra: [None; MAX_EXTRA_STDIO],
            extra_len: 0,
            pty: false,
        }
    }

    pub(super) fn with_extra(
        mut self,
        extra: &[UnixExtraStdioPlan],
    ) -> Result<Self, UnixStdioPlanError> {
        if self.pty && !extra.is_empty() {
            return Err(UnixStdioPlanError::PtyCannotHaveExtraPipes);
        }
        if extra.len() > MAX_EXTRA_STDIO {
            return Err(UnixStdioPlanError::TooManyExtraPipes {
                actual: extra.len(),
            });
        }
        for (offset, entry) in extra.iter().copied().enumerate() {
            let expected = (3 + offset) as u32;
            if entry.child_fd != expected {
                return Err(UnixStdioPlanError::NonContiguousExtraFd {
                    expected,
                    actual: entry.child_fd,
                });
            }
            self.extra[offset] = Some(entry);
        }
        self.extra_len = extra.len();
        Ok(self)
    }

    pub(super) const fn as_pty(mut self) -> Self {
        assert!(self.extra_len == 0, "PTY cannot contain extra pipes");
        self.pty = true;
        self
    }

    pub(super) const fn dispositions(self) -> [UnixStdioDisposition; 3] {
        [self.stdin, self.stdout, self.stderr]
    }

    pub(super) fn extra(self) -> impl ExactSizeIterator<Item = UnixExtraStdioPlan> {
        self.extra
            .into_iter()
            .take(self.extra_len)
            .map(|entry| entry.unwrap_or_else(|| std::process::abort()))
    }

    pub(super) const fn stdio_count(self) -> usize {
        3 + self.extra_len
    }

    pub(super) const fn has_extra(self) -> bool {
        self.extra_len != 0
    }

    pub(super) const fn is_pty(self) -> bool {
        self.pty
    }

    pub(super) fn disposition_at(self, index: usize) -> Option<UnixStdioDisposition> {
        match index {
            0 => Some(self.stdin),
            1 => Some(self.stdout),
            2 => Some(self.stderr),
            index if index < self.stdio_count() => Some(UnixStdioDisposition::Pipe),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum UnixShadowSpawnRoute {
    Rust193ForkFallback,
    Rust193PosixSpawn,
    Node20LibuvFork,
    Node20LibuvPosixSpawn,
}

/// Explicitly limited plan accepted by the fork-fallback shadow.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Rust193DirectPipeShadowPlan {
    image_dialect: ProcessImageDialect,
    route: UnixShadowSpawnRoute,
    stdio: UnixStdioPlan,
    topology: ProcessTopology,
}

impl Rust193DirectPipeShadowPlan {
    pub(super) const fn rust193_fork_fallback(stdio: UnixStdioPlan) -> Self {
        Self {
            image_dialect: ProcessImageDialect::Rust193,
            route: UnixShadowSpawnRoute::Rust193ForkFallback,
            stdio,
            topology: ProcessTopology::Inherit,
        }
    }

    pub(super) const fn node20_fork(stdio: UnixStdioPlan, topology: ProcessTopology) -> Self {
        Self {
            image_dialect: ProcessImageDialect::Node20Libuv146,
            route: UnixShadowSpawnRoute::Node20LibuvFork,
            stdio,
            topology,
        }
    }

    #[cfg(test)]
    pub(super) const fn for_test(
        image_dialect: ProcessImageDialect,
        route: UnixShadowSpawnRoute,
        stdio: UnixStdioPlan,
    ) -> Self {
        Self {
            image_dialect,
            route,
            stdio,
            topology: ProcessTopology::Inherit,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum UnixShadowDescriptorStage {
    StdinPipe,
    StdoutPipe,
    StderrPipe,
    StdinNull,
    StdoutNull,
    StderrNull,
    ExtraSocketPair,
    ExtraDirectionShutdown,
    ExecErrorChannel,
    SetCloseOnExec,
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum UnixPtyStage {
    OpenMaster,
    Grant,
    Unlock,
    Name,
    OpenSlave,
    WindowSize,
    Duplicate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum UnixExecErrorRecordError {
    WrongLength { actual: usize },
    WrongFooter,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum UnixShadowSpawnError {
    ImageDialectMismatch {
        image: ProcessImageDialect,
        requested: ProcessImageDialect,
    },
    UnsupportedImageDialect {
        requested: ProcessImageDialect,
    },
    UnsupportedSpawnRoute {
        requested: UnixShadowSpawnRoute,
    },
    Descriptor {
        stage: UnixShadowDescriptorStage,
        errno: c_int,
    },
    #[cfg(target_os = "linux")]
    Pty {
        stage: UnixPtyStage,
        errno: c_int,
    },
    Fork {
        errno: c_int,
    },
    ChildExec {
        errno: c_int,
    },
    ParentStdioShapeMismatch,
    UnsupportedPtyForRust193,
    PtyRequiresDedicatedRoute,
    Wait {
        errno: c_int,
    },
    HiddenCleanup {
        kill_errno: Option<c_int>,
        wait_errno: Option<c_int>,
    },
}

impl fmt::Display for UnixShadowSpawnError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ImageDialectMismatch { image, requested } => write!(
                formatter,
                "shadow image dialect {image:?} does not match requested dialect {requested:?}"
            ),
            Self::UnsupportedImageDialect { requested } => {
                write!(formatter, "unsupported shadow image dialect: {requested:?}")
            }
            Self::UnsupportedSpawnRoute { requested } => {
                write!(formatter, "unsupported shadow spawn route: {requested:?}")
            }
            Self::Descriptor { stage, errno } => {
                write!(
                    formatter,
                    "descriptor action {stage:?} failed with errno {errno}"
                )
            }
            #[cfg(target_os = "linux")]
            Self::Pty { stage, errno } => {
                write!(formatter, "PTY action {stage:?} failed with errno {errno}")
            }
            Self::Fork { errno } => write!(formatter, "fork failed with errno {errno}"),
            Self::ChildExec { errno } => {
                write!(formatter, "child setup or exec failed with errno {errno}")
            }
            Self::ParentStdioShapeMismatch => {
                formatter.write_str("spawned parent stdio did not match the requested shape")
            }
            Self::UnsupportedPtyForRust193 => {
                formatter.write_str("Rust 1.93 stdio plan cannot select the Node PTY route")
            }
            Self::PtyRequiresDedicatedRoute => {
                formatter.write_str("PTY stdio requires the dedicated controlling-PTY route")
            }
            Self::Wait { errno } => write!(formatter, "waitpid failed with errno {errno}"),
            Self::HiddenCleanup {
                kill_errno,
                wait_errno,
            } => write!(
                formatter,
                "hidden child cleanup failed (kill={kill_errno:?}, wait={wait_errno:?})"
            ),
        }
    }
}

impl std::error::Error for UnixShadowSpawnError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum UnixSpawnTestFault {
    None,
    DropStdoutBeforeShapeValidation,
    CorruptExecErrorFooter,
}

/// Deterministic observations for focused transaction tests only.
#[derive(Debug)]
pub(super) struct UnixSpawnTestControl {
    enabled: bool,
    fault: UnixSpawnTestFault,
    descriptor_actions: usize,
    fork_calls: usize,
    closed_parent_fd_actions: usize,
    exec_failure_pid: Option<pid_t>,
    exec_failure_wait_successes: usize,
    exec_failure_parent_endpoints_closed_before_wait: bool,
    invariant_failure_pid: Option<pid_t>,
    invariant_failure_wait_successes: usize,
    hidden_cleanup_pid: Option<pid_t>,
    hidden_cleanup_kill_calls: usize,
    hidden_cleanup_wait_successes: usize,
    hidden_cleanup_parent_endpoints_closed_before_kill: bool,
}

impl Default for UnixSpawnTestControl {
    fn default() -> Self {
        Self {
            enabled: true,
            fault: UnixSpawnTestFault::None,
            descriptor_actions: 0,
            fork_calls: 0,
            closed_parent_fd_actions: 0,
            exec_failure_pid: None,
            exec_failure_wait_successes: 0,
            exec_failure_parent_endpoints_closed_before_wait: false,
            invariant_failure_pid: None,
            invariant_failure_wait_successes: 0,
            hidden_cleanup_pid: None,
            hidden_cleanup_kill_calls: 0,
            hidden_cleanup_wait_successes: 0,
            hidden_cleanup_parent_endpoints_closed_before_kill: false,
        }
    }
}

impl UnixSpawnTestControl {
    pub(super) fn disabled() -> Self {
        Self {
            enabled: false,
            ..Self::default()
        }
    }

    #[cfg(test)]
    pub(super) fn with_fault(fault: UnixSpawnTestFault) -> Self {
        Self {
            fault,
            ..Self::default()
        }
    }

    fn record_descriptor_action(&mut self) {
        if self.enabled {
            self.descriptor_actions += 1;
        }
    }

    fn record_closed(&mut self) {
        if self.enabled {
            self.closed_parent_fd_actions += 1;
        }
    }

    #[cfg(test)]
    pub(super) const fn descriptor_actions(&self) -> usize {
        self.descriptor_actions
    }

    #[cfg(test)]
    pub(super) const fn fork_calls(&self) -> usize {
        self.fork_calls
    }

    #[cfg(test)]
    pub(super) const fn closed_parent_fd_actions(&self) -> usize {
        self.closed_parent_fd_actions
    }

    #[cfg(test)]
    pub(super) const fn exec_failure_pid(&self) -> Option<pid_t> {
        self.exec_failure_pid
    }

    #[cfg(test)]
    pub(super) const fn exec_failure_wait_successes(&self) -> usize {
        self.exec_failure_wait_successes
    }

    #[cfg(test)]
    pub(super) const fn exec_failure_parent_endpoints_closed_before_wait(&self) -> bool {
        self.exec_failure_parent_endpoints_closed_before_wait
    }

    #[cfg(test)]
    pub(super) const fn invariant_failure_pid(&self) -> Option<pid_t> {
        self.invariant_failure_pid
    }

    #[cfg(test)]
    pub(super) const fn invariant_failure_wait_successes(&self) -> usize {
        self.invariant_failure_wait_successes
    }

    #[cfg(test)]
    pub(super) const fn hidden_cleanup_pid(&self) -> Option<pid_t> {
        self.hidden_cleanup_pid
    }

    #[cfg(test)]
    pub(super) const fn hidden_cleanup_kill_calls(&self) -> usize {
        self.hidden_cleanup_kill_calls
    }

    #[cfg(test)]
    pub(super) const fn hidden_cleanup_wait_successes(&self) -> usize {
        self.hidden_cleanup_wait_successes
    }

    #[cfg(test)]
    pub(super) const fn hidden_cleanup_parent_endpoints_closed_before_kill(&self) -> bool {
        self.hidden_cleanup_parent_endpoints_closed_before_kill
    }
}

/// Real but deliberately unconnected Rust 1.93 fork-fallback adapter.
pub(super) struct Rust193ForkFallbackShadowSpawner;

impl Rust193ForkFallbackShadowSpawner {
    #[cfg(test)]
    pub(super) fn spawn(
        image: UnixExecImage,
        plan: Rust193DirectPipeShadowPlan,
    ) -> Result<ShadowSpawnedRuntimeProcess, UnixShadowSpawnError> {
        let mut control = UnixSpawnTestControl::disabled();
        Self::spawn_inner(image, plan, &mut control)
    }

    #[cfg(test)]
    pub(super) fn spawn_with_test_control(
        image: UnixExecImage,
        plan: Rust193DirectPipeShadowPlan,
        control: &mut UnixSpawnTestControl,
    ) -> Result<ShadowSpawnedRuntimeProcess, UnixShadowSpawnError> {
        Self::spawn_inner(image, plan, control)
    }

    pub(super) fn spawn_unregistered(
        _admission: UnixSpawnAdmission<'_>,
        image: UnixExecImage,
        plan: Rust193DirectPipeShadowPlan,
    ) -> Result<UnregisteredUnixSpawnedProcess, UnixShadowSpawnError> {
        let mut control = UnixSpawnTestControl::disabled();
        Self::spawn_unregistered_inner(image, plan, &mut control)
    }

    #[cfg(test)]
    pub(super) fn spawn_unregistered_with_test_control(
        _admission: UnixSpawnAdmission<'_>,
        image: UnixExecImage,
        plan: Rust193DirectPipeShadowPlan,
        control: &mut UnixSpawnTestControl,
    ) -> Result<UnregisteredUnixSpawnedProcess, UnixShadowSpawnError> {
        Self::spawn_unregistered_inner(image, plan, control)
    }

    #[cfg(test)]
    fn spawn_inner(
        image: UnixExecImage,
        plan: Rust193DirectPipeShadowPlan,
        control: &mut UnixSpawnTestControl,
    ) -> Result<ShadowSpawnedRuntimeProcess, UnixShadowSpawnError> {
        Ok(Self::spawn_unregistered_inner(image, plan, control)?.into_shadow())
    }

    fn spawn_unregistered_inner(
        image: UnixExecImage,
        plan: Rust193DirectPipeShadowPlan,
        control: &mut UnixSpawnTestControl,
    ) -> Result<UnregisteredUnixSpawnedProcess, UnixShadowSpawnError> {
        validate_shadow_route(&image, plan)?;

        let fork_guard = SHADOW_FORK_DESCRIPTOR_LOCK
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let stdio = PreparedStdio::build(plan.stdio, control)?;
        Self::spawn_unregistered_prepared_inner(image, plan, control, stdio, fork_guard)
    }

    pub(super) fn spawn_unregistered_with_prepared_and_guard(
        _admission: UnixSpawnAdmission<'_>,
        image: UnixExecImage,
        plan: Rust193DirectPipeShadowPlan,
        stdio: PreparedStdio,
        fork_guard: MutexGuard<'_, ()>,
    ) -> Result<UnregisteredUnixSpawnedProcess, UnixShadowSpawnError> {
        let mut control = UnixSpawnTestControl::disabled();
        validate_shadow_route(&image, plan)?;
        Self::spawn_unregistered_prepared_inner(image, plan, &mut control, stdio, fork_guard)
    }

    fn spawn_unregistered_prepared_inner(
        image: UnixExecImage,
        plan: Rust193DirectPipeShadowPlan,
        control: &mut UnixSpawnTestControl,
        mut stdio: PreparedStdio,
        fork_guard: MutexGuard<'_, ()>,
    ) -> Result<UnregisteredUnixSpawnedProcess, UnixShadowSpawnError> {
        let (error_read, error_write) = create_exec_error_channel(control)?;

        control.fork_calls += usize::from(control.enabled);
        let pid = with_process_environment_lock(|| {
            // SAFETY: every child input and descriptor action was precomputed;
            // the child branch performs only async-signal-safe libc calls and
            // terminates through exec or `_exit`.
            unsafe {
                let pid = libc::fork();
                if pid == 0 {
                    child_main(
                        &image,
                        &stdio,
                        plan,
                        error_read.as_raw_fd(),
                        error_write.as_raw_fd(),
                    );
                }
                pid
            }
        });

        if pid < 0 {
            let errno = last_errno();
            drop(fork_guard);
            close_owned(error_read, control);
            close_owned(error_write, control);
            stdio.close_all(control);
            return Err(UnixShadowSpawnError::Fork { errno });
        }

        drop(fork_guard);
        close_owned(error_write, control);
        stdio.close_child_ends(control);

        let mut transaction = UnpublishedChildTransaction::new(pid, stdio, control);
        match read_exec_error_record(error_read.as_raw_fd(), control.fault) {
            Ok(ExecHandshake::Success) => {
                close_owned(error_read, control);
            }
            Ok(ExecHandshake::ChildError(errno)) => {
                close_owned(error_read, control);
                transaction.close_parent_endpoints(control);
                control.exec_failure_parent_endpoints_closed_before_wait =
                    transaction.parent_endpoints_are_closed();
                let wait_result = wait_unpublished_child_blocking(pid);
                control.exec_failure_pid = Some(pid);
                if wait_result.is_ok() {
                    control.exec_failure_wait_successes += 1;
                    transaction.mark_reaped();
                }
                wait_result?;
                return Err(UnixShadowSpawnError::ChildExec { errno });
            }
            Err(error) => {
                close_owned(error_read, control);
                transaction.close_parent_endpoints(control);
                let wait_result = wait_unpublished_child_blocking(pid);
                if wait_result.is_ok() {
                    transaction.mark_reaped();
                    control.invariant_failure_wait_successes += 1;
                }
                control.invariant_failure_pid = Some(pid);
                assert!(
                    wait_result.is_ok(),
                    "Rust 1.93 exec-error invariant wait failed: {wait_result:?}"
                );
                panic!("Rust 1.93 exec-error channel invariant failed: {error:?}");
            }
        }

        if control.fault == UnixSpawnTestFault::DropStdoutBeforeShapeValidation {
            transaction.drop_stdout_for_test(control);
        }
        if !transaction.parent_shape_matches(plan.stdio) {
            transaction.close_parent_endpoints(control);
            #[cfg(test)]
            {
                return transaction
                    .kill_and_reap_hidden(control)
                    .and(Err(UnixShadowSpawnError::ParentStdioShapeMismatch));
            }
            #[cfg(not(test))]
            std::process::abort();
        }

        Ok(transaction.commit_unregistered())
    }
}

fn validate_shadow_route(
    image: &UnixExecImage,
    plan: Rust193DirectPipeShadowPlan,
) -> Result<(), UnixShadowSpawnError> {
    if image.image_dialect() != plan.image_dialect {
        return Err(UnixShadowSpawnError::ImageDialectMismatch {
            image: image.image_dialect(),
            requested: plan.image_dialect,
        });
    }
    match (plan.image_dialect, plan.route) {
        (ProcessImageDialect::Rust193, UnixShadowSpawnRoute::Rust193ForkFallback) => {
            if plan.topology != ProcessTopology::Inherit {
                return Err(UnixShadowSpawnError::UnsupportedSpawnRoute {
                    requested: plan.route,
                });
            }
        }
        (ProcessImageDialect::Node20Libuv146, UnixShadowSpawnRoute::Node20LibuvFork) => {
            if plan.topology == ProcessTopology::WindowsDetached {
                return Err(UnixShadowSpawnError::UnsupportedSpawnRoute {
                    requested: plan.route,
                });
            }
        }
        (ProcessImageDialect::Rust193 | ProcessImageDialect::Node20Libuv146, _) => {
            return Err(UnixShadowSpawnError::UnsupportedSpawnRoute {
                requested: plan.route,
            });
        }
    }
    Ok(())
}

pub(super) struct PreparedStdio {
    child: [Option<OwnedFd>; 3],
    parent: [Option<OwnedFd>; 3],
    extra: Vec<PreparedExtraStdio>,
    pty_master: Option<OwnedFd>,
}

pub(super) struct PreparedExtraStdio {
    child_fd: u32,
    direction: UnixExtraPipeDirection,
    child: Option<OwnedFd>,
    parent: Option<OwnedFd>,
}

impl PreparedExtraStdio {
    pub(super) const fn new(
        child_fd: u32,
        direction: UnixExtraPipeDirection,
        child: OwnedFd,
        parent: OwnedFd,
    ) -> Self {
        Self {
            child_fd,
            direction,
            child: Some(child),
            parent: Some(parent),
        }
    }
}

impl PreparedStdio {
    pub(super) fn build_rust193(plan: UnixStdioPlan) -> Result<Self, UnixShadowSpawnError> {
        let mut control = UnixSpawnTestControl::disabled();
        Self::build(plan, &mut control)
    }

    pub(super) const fn from_owned_parts(
        child: [Option<OwnedFd>; 3],
        parent: [Option<OwnedFd>; 3],
    ) -> Self {
        Self {
            child,
            parent,
            extra: Vec::new(),
            pty_master: None,
        }
    }

    pub(super) fn from_owned_parts_with_extra(
        child: [Option<OwnedFd>; 3],
        parent: [Option<OwnedFd>; 3],
        extra: Vec<PreparedExtraStdio>,
    ) -> Self {
        Self {
            child,
            parent,
            extra,
            pty_master: None,
        }
    }

    pub(super) const fn from_pty_parts(child: [Option<OwnedFd>; 3], master: OwnedFd) -> Self {
        Self {
            child,
            parent: [None, None, None],
            extra: Vec::new(),
            pty_master: Some(master),
        }
    }

    pub(super) fn child_raw_fds(&self) -> [Option<RawFd>; 3] {
        self.child
            .each_ref()
            .map(|fd| fd.as_ref().map(AsRawFd::as_raw_fd))
    }

    pub(super) fn all_child_raw_fds(&self) -> [Option<RawFd>; MAX_UNIX_STDIO] {
        let mut result = [None; MAX_UNIX_STDIO];
        for (index, fd) in self.child_raw_fds().into_iter().enumerate() {
            result[index] = fd;
        }
        for extra in &self.extra {
            let index = usize::try_from(extra.child_fd).unwrap_or_else(|_| std::process::abort());
            if index >= result.len() || result[index].is_some() {
                std::process::abort();
            }
            result[index] = extra.child.as_ref().map(AsRawFd::as_raw_fd);
        }
        result
    }

    fn build(
        plan: UnixStdioPlan,
        control: &mut UnixSpawnTestControl,
    ) -> Result<Self, UnixShadowSpawnError> {
        if plan.is_pty() {
            return Err(UnixShadowSpawnError::UnsupportedPtyForRust193);
        }
        let mut prepared = Self {
            child: [None, None, None],
            parent: [None, None, None],
            extra: Vec::new(),
            pty_master: None,
        };
        for (index, disposition) in plan.dispositions().into_iter().enumerate() {
            match disposition {
                UnixStdioDisposition::Pipe => {
                    let stage = pipe_stage(index);
                    let (reader, writer) = anonymous_pipe(stage, control)?;
                    if index == 0 {
                        prepared.child[index] = Some(reader);
                        prepared.parent[index] = Some(writer);
                    } else {
                        prepared.parent[index] = Some(reader);
                        prepared.child[index] = Some(writer);
                    }
                }
                UnixStdioDisposition::Null => {
                    prepared.child[index] = Some(open_dev_null(index, control)?);
                }
                UnixStdioDisposition::Inherit => {}
            }
        }
        // Broker lanes above fd 2 mirror the Node 20 socketpair contract: one
        // close-on-exec AF_UNIX pair per lane with the unused transfer
        // direction shut down at creation, so every spawn route hands the
        // child byte-identical lane endpoints.
        for requested in plan.extra() {
            let (parent, child) = match extra_socketpair(control) {
                Ok(pair) => pair,
                Err(error) => {
                    prepared.close_all(control);
                    return Err(error);
                }
            };
            if let Err(error) = shutdown_extra_direction(&parent, &child, requested.direction()) {
                prepared.close_all(control);
                return Err(error);
            }
            prepared.extra.push(PreparedExtraStdio::new(
                requested.child_fd(),
                requested.direction(),
                child,
                parent,
            ));
        }
        Ok(prepared)
    }

    fn close_child_ends(&mut self, control: &mut UnixSpawnTestControl) {
        for child in &mut self.child {
            if let Some(fd) = child.take() {
                close_owned(fd, control);
            }
        }
        for extra in &mut self.extra {
            if let Some(fd) = extra.child.take() {
                close_owned(fd, control);
            }
        }
    }

    pub(super) fn close_child_ends_for_parent(&mut self) {
        let mut control = UnixSpawnTestControl::disabled();
        self.close_child_ends(&mut control);
    }

    fn close_parent_endpoints(&mut self, control: &mut UnixSpawnTestControl) {
        for parent in &mut self.parent {
            if let Some(fd) = parent.take() {
                close_owned(fd, control);
            }
        }
        for extra in &mut self.extra {
            if let Some(fd) = extra.parent.take() {
                close_owned(fd, control);
            }
        }
        if let Some(fd) = self.pty_master.take() {
            close_owned(fd, control);
        }
    }

    fn close_all(&mut self, control: &mut UnixSpawnTestControl) {
        self.close_child_ends(control);
        self.close_parent_endpoints(control);
    }
}

#[cfg(target_os = "linux")]
pub(super) fn prepare_node20_pty_stdio(
    rows: u16,
    columns: u16,
    pixel_width: u16,
    pixel_height: u16,
) -> Result<PreparedStdio, UnixShadowSpawnError> {
    use std::ffi::CStr;

    let master_raw = retry_pty_fd(UnixPtyStage::OpenMaster, || unsafe {
        libc::posix_openpt(libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC)
    })?;
    let master = unsafe { OwnedFd::from_raw_fd(master_raw) };
    retry_pty_zero(UnixPtyStage::Grant, || unsafe {
        libc::grantpt(master.as_raw_fd())
    })?;
    retry_pty_zero(UnixPtyStage::Unlock, || unsafe {
        libc::unlockpt(master.as_raw_fd())
    })?;

    let mut slave_name = vec![0 as libc::c_char; libc::PATH_MAX as usize];
    loop {
        let result = unsafe {
            libc::ptsname_r(
                master.as_raw_fd(),
                slave_name.as_mut_ptr(),
                slave_name.len(),
            )
        };
        if result == 0 {
            break;
        }
        if result != libc::EINTR {
            return Err(UnixShadowSpawnError::Pty {
                stage: UnixPtyStage::Name,
                errno: result,
            });
        }
    }
    let slave_name = unsafe { CStr::from_ptr(slave_name.as_ptr()) };
    let slave_raw = retry_pty_fd(UnixPtyStage::OpenSlave, || unsafe {
        libc::open(
            slave_name.as_ptr(),
            libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC,
        )
    })?;
    let slave = unsafe { OwnedFd::from_raw_fd(slave_raw) };
    let size = libc::winsize {
        ws_row: rows,
        ws_col: columns,
        ws_xpixel: pixel_width,
        ws_ypixel: pixel_height,
    };
    retry_pty_zero(UnixPtyStage::WindowSize, || unsafe {
        libc::ioctl(
            slave.as_raw_fd(),
            libc::TIOCSWINSZ,
            std::ptr::addr_of!(size),
        )
    })?;

    let master = move_pty_above_stdio(master)?;
    let slave = move_pty_above_stdio(slave)?;
    let child_stdout = duplicate_pty_descriptor(slave.as_raw_fd())?;
    let child_stderr = duplicate_pty_descriptor(slave.as_raw_fd())?;
    Ok(PreparedStdio::from_pty_parts(
        [Some(slave), Some(child_stdout), Some(child_stderr)],
        master,
    ))
}

#[cfg(target_os = "linux")]
fn retry_pty_fd(
    stage: UnixPtyStage,
    mut operation: impl FnMut() -> RawFd,
) -> Result<RawFd, UnixShadowSpawnError> {
    loop {
        let fd = operation();
        if fd != -1 {
            return Ok(fd);
        }
        let errno = last_errno();
        if errno != libc::EINTR {
            return Err(UnixShadowSpawnError::Pty { stage, errno });
        }
    }
}

#[cfg(target_os = "linux")]
fn retry_pty_zero(
    stage: UnixPtyStage,
    mut operation: impl FnMut() -> c_int,
) -> Result<(), UnixShadowSpawnError> {
    loop {
        if operation() != -1 {
            return Ok(());
        }
        let errno = last_errno();
        if errno != libc::EINTR {
            return Err(UnixShadowSpawnError::Pty { stage, errno });
        }
    }
}

#[cfg(target_os = "linux")]
fn move_pty_above_stdio(fd: OwnedFd) -> Result<OwnedFd, UnixShadowSpawnError> {
    if fd.as_raw_fd() >= 3 {
        return Ok(fd);
    }
    let moved = duplicate_pty_descriptor(fd.as_raw_fd())?;
    drop(fd);
    Ok(moved)
}

#[cfg(target_os = "linux")]
fn duplicate_pty_descriptor(fd: RawFd) -> Result<OwnedFd, UnixShadowSpawnError> {
    loop {
        let duplicated = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 3) };
        if duplicated != -1 {
            return Ok(unsafe { OwnedFd::from_raw_fd(duplicated) });
        }
        let errno = last_errno();
        if errno != libc::EINTR {
            return Err(UnixShadowSpawnError::Pty {
                stage: UnixPtyStage::Duplicate,
                errno,
            });
        }
    }
}

struct UnpublishedChildTransaction {
    pid: pid_t,
    stdio: PreparedStdio,
    owns_wait_right: bool,
}

impl UnpublishedChildTransaction {
    fn new(pid: pid_t, stdio: PreparedStdio, _control: &mut UnixSpawnTestControl) -> Self {
        Self {
            pid,
            stdio,
            owns_wait_right: true,
        }
    }

    fn close_parent_endpoints(&mut self, control: &mut UnixSpawnTestControl) {
        self.stdio.close_parent_endpoints(control);
    }

    fn drop_stdout_for_test(&mut self, control: &mut UnixSpawnTestControl) {
        if let Some(fd) = self.stdio.parent[1].take() {
            close_owned(fd, control);
        }
    }

    fn parent_shape_matches(&self, plan: UnixStdioPlan) -> bool {
        if plan.is_pty() {
            return self.stdio.pty_master.is_some()
                && self.stdio.parent.iter().all(Option::is_none)
                && self.stdio.extra.is_empty();
        }
        let standard_matches =
            plan.dispositions()
                .into_iter()
                .enumerate()
                .all(|(index, disposition)| {
                    self.stdio.parent[index].is_some()
                        == (disposition == UnixStdioDisposition::Pipe)
                });
        let extra_matches = self.stdio.extra.len() == plan.extra_len
            && self
                .stdio
                .extra
                .iter()
                .zip(plan.extra())
                .all(|(prepared, requested)| {
                    prepared.child_fd == requested.child_fd()
                        && prepared.direction == requested.direction()
                        && prepared.parent.is_some()
                });
        standard_matches && extra_matches && self.stdio.pty_master.is_none()
    }

    fn parent_endpoints_are_closed(&self) -> bool {
        self.stdio.parent.iter().all(Option::is_none)
            && self.stdio.extra.iter().all(|extra| extra.parent.is_none())
            && self.stdio.pty_master.is_none()
    }

    #[cfg(test)]
    fn kill_and_reap_hidden(
        &mut self,
        control: &mut UnixSpawnTestControl,
    ) -> Result<(), UnixShadowSpawnError> {
        control.hidden_cleanup_pid = Some(self.pid);
        control.hidden_cleanup_parent_endpoints_closed_before_kill =
            self.parent_endpoints_are_closed();
        control.hidden_cleanup_kill_calls += 1;
        let kill_errno = match retrying_kill(self.pid, libc::SIGKILL) {
            Ok(()) => None,
            Err(errno) if errno == libc::ESRCH => None,
            Err(errno) => Some(errno),
        };
        let wait_errno = match wait_unpublished_child_blocking(self.pid) {
            Ok(_) => {
                control.hidden_cleanup_wait_successes += 1;
                self.owns_wait_right = false;
                None
            }
            Err(UnixShadowSpawnError::Wait { errno }) => Some(errno),
            Err(_) => Some(libc::EINVAL),
        };
        if kill_errno.is_none() && wait_errno.is_none() {
            Ok(())
        } else {
            Err(UnixShadowSpawnError::HiddenCleanup {
                kill_errno,
                wait_errno,
            })
        }
    }

    fn mark_reaped(&mut self) {
        self.owns_wait_right = false;
    }

    fn commit_unregistered(mut self) -> UnregisteredUnixSpawnedProcess {
        let parent = if let Some(master) = self.stdio.pty_master.take() {
            if self.stdio.parent.iter().any(Option::is_some) || !self.stdio.extra.is_empty() {
                std::process::abort();
            }
            UnattachedParentStdio::Pty(master)
        } else {
            let extra = self
                .stdio
                .extra
                .iter_mut()
                .map(|prepared| UnattachedRuntimeExtraPipe {
                    child_fd: prepared.child_fd,
                    direction: prepared.direction,
                    parent: prepared
                        .parent
                        .take()
                        .unwrap_or_else(|| std::process::abort()),
                })
                .collect::<Vec<_>>();
            let stdin = self.stdio.parent[0].take();
            let stdout = self.stdio.parent[1].take();
            let stderr = self.stdio.parent[2].take();
            if stdin.is_none() && stdout.is_none() && stderr.is_none() && extra.is_empty() {
                UnattachedParentStdio::NullOrInherited
            } else {
                UnattachedParentStdio::Pipes {
                    stdin,
                    stdout,
                    stderr,
                    extra,
                }
            }
        };
        let child = UnregisteredUnixChild {
            pid: self.pid,
            owns_wait_right: true,
        };
        self.owns_wait_right = false;
        UnregisteredUnixSpawnedProcess {
            child,
            stdio: parent,
        }
    }
}

impl Drop for UnpublishedChildTransaction {
    fn drop(&mut self) {
        if !self.owns_wait_right {
            return;
        }
        // This shadow does not yet own the common generation-bound reaper.
        // Numeric-PID cleanup from an unexpected unwind could cross a
        // wait/reuse window, so fail closed at the parent process instead.
        std::process::abort();
    }
}

/// Successful Unix spawn whose sole wait right has not yet crossed the common
/// reaper commit boundary. Dropping either carrier before transfer is fatal.
pub(super) struct UnregisteredUnixSpawnedProcess {
    child: UnregisteredUnixChild,
    stdio: UnattachedParentStdio,
}

impl UnregisteredUnixSpawnedProcess {
    pub(super) fn from_posix_spawn(pid: pid_t, mut stdio: PreparedStdio) -> Self {
        let mut control = UnixSpawnTestControl::disabled();
        stdio.close_child_ends(&mut control);
        UnpublishedChildTransaction::new(pid, stdio, &mut control).commit_unregistered()
    }

    pub(super) fn into_parts(self) -> (UnregisteredUnixChild, UnattachedParentStdio) {
        (self.child, self.stdio)
    }

    #[cfg(test)]
    fn into_shadow(self) -> ShadowSpawnedRuntimeProcess {
        let (child, stdio) = self.into_parts();
        let pid = child.pid_while_owned();
        child.transfer_wait_right();
        ShadowSpawnedRuntimeProcess {
            child: ShadowRuntimeChild::new(pid),
            stdio,
        }
    }
}

pub(super) struct UnregisteredUnixChild {
    pid: pid_t,
    owns_wait_right: bool,
}

impl UnregisteredUnixChild {
    pub(super) const fn pid_while_owned(&self) -> pid_t {
        self.pid
    }

    pub(super) fn transfer_wait_right(mut self) {
        self.owns_wait_right = false;
    }

    #[cfg(test)]
    pub(super) const fn for_test(pid: pid_t) -> Self {
        Self {
            pid,
            owns_wait_right: true,
        }
    }
}

impl Drop for UnregisteredUnixChild {
    fn drop(&mut self) {
        if self.owns_wait_right {
            std::process::abort();
        }
    }
}

#[cfg(test)]
pub(super) struct ShadowSpawnedRuntimeProcess {
    child: ShadowRuntimeChild,
    stdio: UnattachedParentStdio,
}

#[cfg(test)]
impl ShadowSpawnedRuntimeProcess {
    pub(super) fn into_parts(self) -> (ShadowRuntimeChild, UnattachedParentStdio) {
        (self.child, self.stdio)
    }
}

pub(super) enum UnattachedParentStdio {
    NullOrInherited,
    Pipes {
        stdin: Option<OwnedFd>,
        stdout: Option<OwnedFd>,
        stderr: Option<OwnedFd>,
        extra: Vec<UnattachedRuntimeExtraPipe>,
    },
    Pty(OwnedFd),
}

pub(super) struct UnattachedRuntimeExtraPipe {
    child_fd: u32,
    direction: UnixExtraPipeDirection,
    parent: OwnedFd,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ParentStdioAttachmentError {
    Nonblocking { errno: Option<c_int> },
    Reactor { errno: Option<c_int> },
    ReactorPanicked,
}

impl UnattachedParentStdio {
    pub(super) fn attach(
        self,
        runtime: &Handle,
    ) -> Result<ParentStdio, ParentStdioAttachmentError> {
        let _runtime_guard = runtime.enter();
        match self {
            Self::NullOrInherited => Ok(ParentStdio::NullOrInherited),
            Self::Pipes {
                stdin,
                stdout,
                stderr,
                extra,
            } => {
                let stdin = stdin
                    .map(|fd| RuntimeAsyncFd::attach(fd, Interest::WRITABLE).map(RuntimeChildStdin))
                    .transpose()?;
                let stdout = stdout
                    .map(|fd| {
                        RuntimeAsyncFd::attach(fd, Interest::READABLE).map(RuntimeChildStdout)
                    })
                    .transpose()?;
                let stderr = stderr
                    .map(|fd| {
                        RuntimeAsyncFd::attach(fd, Interest::READABLE).map(RuntimeChildStderr)
                    })
                    .transpose()?;
                let extra = extra
                    .into_iter()
                    .map(|pipe| {
                        let parent = match pipe.direction {
                            UnixExtraPipeDirection::ChildToHost => {
                                RuntimeExtraPipeEndpoint::Read(RuntimeChildExtraRead(
                                    RuntimeAsyncFd::attach(pipe.parent, Interest::READABLE)?,
                                ))
                            }
                            UnixExtraPipeDirection::HostToChild => {
                                RuntimeExtraPipeEndpoint::Write(RuntimeChildExtraWrite(
                                    RuntimeAsyncFd::attach(pipe.parent, Interest::WRITABLE)?,
                                ))
                            }
                            UnixExtraPipeDirection::Duplex => RuntimeExtraPipeEndpoint::Duplex(
                                RuntimeChildExtraDuplex(RuntimeAsyncFd::attach(
                                    pipe.parent,
                                    Interest::READABLE | Interest::WRITABLE,
                                )?),
                            ),
                        };
                        Ok(RuntimeExtraPipe {
                            child_fd: pipe.child_fd,
                            parent,
                        })
                    })
                    .collect::<Result<Vec<_>, ParentStdioAttachmentError>>()?;
                Ok(ParentStdio::Pipes {
                    stdin,
                    stdout,
                    stderr,
                    extra,
                })
            }
            Self::Pty(master) => Ok(ParentStdio::Pty(RuntimePtyMaster(RuntimeAsyncFd::attach(
                master,
                Interest::READABLE | Interest::WRITABLE,
            )?))),
        }
    }

    #[cfg(test)]
    pub(super) fn into_standard_pipes(
        self,
    ) -> (
        Option<BlockingRuntimeChildStdin>,
        Option<BlockingRuntimeChildStdout>,
        Option<BlockingRuntimeChildStderr>,
    ) {
        match self {
            Self::NullOrInherited => (None, None, None),
            Self::Pipes {
                stdin,
                stdout,
                stderr,
                extra,
            } => {
                assert!(extra.is_empty(), "extra pipes require typed extraction");
                (
                    stdin.map(BlockingRuntimeChildStdin),
                    stdout.map(BlockingRuntimeChildStdout),
                    stderr.map(BlockingRuntimeChildStderr),
                )
            }
            Self::Pty(_) => panic!("PTY is one duplex master, not three standard pipes"),
        }
    }

    pub(super) const fn is_null_or_inherited(&self) -> bool {
        matches!(self, Self::NullOrInherited)
    }
}

pub(super) enum ParentStdio {
    NullOrInherited,
    Pipes {
        stdin: Option<RuntimeChildStdin>,
        stdout: Option<RuntimeChildStdout>,
        stderr: Option<RuntimeChildStderr>,
        extra: Vec<RuntimeExtraPipe>,
    },
    Pty(RuntimePtyMaster),
}

impl ParentStdio {
    pub(super) const fn is_null_or_inherited(&self) -> bool {
        matches!(self, Self::NullOrInherited)
    }

    pub(super) fn into_standard_pipes(
        self,
    ) -> (
        Option<RuntimeChildStdin>,
        Option<RuntimeChildStdout>,
        Option<RuntimeChildStderr>,
    ) {
        match self {
            Self::NullOrInherited => (None, None, None),
            Self::Pipes {
                stdin,
                stdout,
                stderr,
                extra,
            } => {
                assert!(extra.is_empty(), "extra pipes require typed extraction");
                (stdin, stdout, stderr)
            }
            Self::Pty(_) => panic!("PTY is one duplex master, not three standard pipes"),
        }
    }

    pub(super) fn into_pty(self) -> Option<RuntimePtyMaster> {
        match self {
            Self::Pty(master) => Some(master),
            Self::NullOrInherited | Self::Pipes { .. } => None,
        }
    }
}

pub(super) struct RuntimeExtraPipe {
    child_fd: u32,
    parent: RuntimeExtraPipeEndpoint,
}

impl RuntimeExtraPipe {
    pub(super) const fn child_fd(&self) -> u32 {
        self.child_fd
    }

    pub(super) fn into_parent(self) -> RuntimeExtraPipeEndpoint {
        self.parent
    }
}

pub(super) enum RuntimeExtraPipeEndpoint {
    Read(RuntimeChildExtraRead),
    Write(RuntimeChildExtraWrite),
    Duplex(RuntimeChildExtraDuplex),
}

struct RuntimeAsyncFd {
    descriptor: AsyncFd<OwnedFd>,
}

impl RuntimeAsyncFd {
    fn attach(fd: OwnedFd, interest: Interest) -> Result<Self, ParentStdioAttachmentError> {
        set_nonblocking(fd.as_raw_fd()).map_err(|error| {
            ParentStdioAttachmentError::Nonblocking {
                errno: error.raw_os_error(),
            }
        })?;
        let descriptor =
            match catch_unwind(AssertUnwindSafe(|| AsyncFd::with_interest(fd, interest))) {
                Ok(Ok(descriptor)) => descriptor,
                Ok(Err(error)) => {
                    return Err(ParentStdioAttachmentError::Reactor {
                        errno: error.raw_os_error(),
                    });
                }
                Err(_) => return Err(ParentStdioAttachmentError::ReactorPanicked),
            };
        Ok(Self { descriptor })
    }

    fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
        pty: bool,
    ) -> Poll<io::Result<()>> {
        loop {
            let mut readiness = ready!(self.descriptor.poll_read_ready_mut(cx))?;
            let unfilled = buffer.initialize_unfilled();
            match readiness.try_io(|descriptor| {
                read_descriptor(descriptor.get_ref().as_raw_fd(), unfilled, pty)
            }) {
                Ok(Ok(read)) => {
                    buffer.advance(read);
                    return Poll::Ready(Ok(()));
                }
                Ok(Err(error)) => return Poll::Ready(Err(error)),
                Err(_would_block) => continue,
            }
        }
    }

    fn poll_write(&mut self, cx: &mut Context<'_>, buffer: &[u8]) -> Poll<io::Result<usize>> {
        loop {
            let mut readiness = ready!(self.descriptor.poll_write_ready_mut(cx))?;
            match readiness
                .try_io(|descriptor| write_descriptor(descriptor.get_ref().as_raw_fd(), buffer))
            {
                Ok(result) => return Poll::Ready(result),
                Err(_would_block) => continue,
            }
        }
    }

    #[cfg(test)]
    fn raw_fd(&self) -> RawFd {
        self.descriptor.get_ref().as_raw_fd()
    }
}

pub(super) struct RuntimeChildStdin(RuntimeAsyncFd);
pub(super) struct RuntimeChildStdout(RuntimeAsyncFd);
pub(super) struct RuntimeChildStderr(RuntimeAsyncFd);
pub(super) struct RuntimeChildExtraRead(RuntimeAsyncFd);
pub(super) struct RuntimeChildExtraWrite(RuntimeAsyncFd);
pub(super) struct RuntimeChildExtraDuplex(RuntimeAsyncFd);
pub(super) struct RuntimePtyMaster(RuntimeAsyncFd);
pub(super) struct RuntimePtyRead(RuntimeAsyncFd);
pub(super) struct RuntimePtyWrite(RuntimeAsyncFd);

macro_rules! async_read_endpoint {
    ($name:ident, $pty:expr) => {
        impl AsyncRead for $name {
            fn poll_read(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buffer: &mut ReadBuf<'_>,
            ) -> Poll<io::Result<()>> {
                self.0.poll_read(cx, buffer, $pty)
            }
        }
    };
}

macro_rules! async_write_endpoint {
    ($name:ident) => {
        impl AsyncWrite for $name {
            fn poll_write(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buffer: &[u8],
            ) -> Poll<io::Result<usize>> {
                self.0.poll_write(cx, buffer)
            }

            fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }

            fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
        }
    };
}

async_write_endpoint!(RuntimeChildStdin);
async_read_endpoint!(RuntimeChildStdout, false);
async_read_endpoint!(RuntimeChildStderr, false);
async_read_endpoint!(RuntimeChildExtraRead, false);
async_write_endpoint!(RuntimeChildExtraWrite);
async_read_endpoint!(RuntimeChildExtraDuplex, false);
async_write_endpoint!(RuntimeChildExtraDuplex);
async_read_endpoint!(RuntimePtyMaster, true);
async_write_endpoint!(RuntimePtyMaster);
async_read_endpoint!(RuntimePtyRead, true);
async_write_endpoint!(RuntimePtyWrite);

#[cfg(test)]
macro_rules! async_endpoint_test_observations {
    ($name:ident) => {
        impl $name {
            pub(super) fn raw_fd_for_test(&self) -> RawFd {
                self.0.raw_fd()
            }

            pub(super) fn is_fifo(&self) -> bool {
                descriptor_is_fifo(self.0.raw_fd())
            }

            pub(super) fn is_close_on_exec(&self) -> bool {
                descriptor_is_close_on_exec(self.0.raw_fd())
            }

            pub(super) fn is_nonblocking(&self) -> bool {
                descriptor_is_nonblocking(self.0.raw_fd())
            }
        }
    };
}

#[cfg(test)]
async_endpoint_test_observations!(RuntimeChildStdin);
#[cfg(test)]
async_endpoint_test_observations!(RuntimeChildStdout);
#[cfg(test)]
async_endpoint_test_observations!(RuntimeChildStderr);
#[cfg(test)]
async_endpoint_test_observations!(RuntimeChildExtraRead);
#[cfg(test)]
async_endpoint_test_observations!(RuntimeChildExtraWrite);
#[cfg(test)]
async_endpoint_test_observations!(RuntimeChildExtraDuplex);
#[cfg(test)]
async_endpoint_test_observations!(RuntimePtyMaster);

#[cfg(test)]
pub(super) struct BlockingRuntimeChildStdin(OwnedFd);
#[cfg(test)]
pub(super) struct BlockingRuntimeChildStdout(OwnedFd);
#[cfg(test)]
pub(super) struct BlockingRuntimeChildStderr(OwnedFd);

#[cfg(test)]
macro_rules! blocking_endpoint_test_observations {
    ($name:ident) => {
        impl $name {
            pub(super) fn raw_fd_for_test(&self) -> RawFd {
                self.0.as_raw_fd()
            }

            pub(super) fn is_fifo(&self) -> bool {
                descriptor_is_fifo(self.0.as_raw_fd())
            }

            pub(super) fn is_close_on_exec(&self) -> bool {
                descriptor_is_close_on_exec(self.0.as_raw_fd())
            }

            pub(super) fn is_nonblocking(&self) -> bool {
                descriptor_is_nonblocking(self.0.as_raw_fd())
            }
        }
    };
}

#[cfg(test)]
blocking_endpoint_test_observations!(BlockingRuntimeChildStdin);
#[cfg(test)]
blocking_endpoint_test_observations!(BlockingRuntimeChildStdout);
#[cfg(test)]
blocking_endpoint_test_observations!(BlockingRuntimeChildStderr);

#[cfg(test)]
impl Write for BlockingRuntimeChildStdin {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        write_descriptor(self.0.as_raw_fd(), buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
impl Read for BlockingRuntimeChildStdout {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        read_descriptor(self.0.as_raw_fd(), buffer, false)
    }
}

#[cfg(test)]
impl Read for BlockingRuntimeChildStderr {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        read_descriptor(self.0.as_raw_fd(), buffer, false)
    }
}

#[cfg(test)]
macro_rules! sync_async_read_endpoint_for_test {
    ($name:ident, $pty:expr) => {
        impl Read for $name {
            fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
                read_descriptor(self.0.raw_fd(), buffer, $pty)
            }
        }
    };
}

#[cfg(test)]
macro_rules! sync_async_write_endpoint_for_test {
    ($name:ident) => {
        impl Write for $name {
            fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
                write_descriptor(self.0.raw_fd(), buffer)
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
    };
}

#[cfg(test)]
sync_async_write_endpoint_for_test!(RuntimeChildStdin);
#[cfg(test)]
sync_async_read_endpoint_for_test!(RuntimeChildStdout, false);
#[cfg(test)]
sync_async_read_endpoint_for_test!(RuntimeChildStderr, false);
#[cfg(test)]
sync_async_read_endpoint_for_test!(RuntimeChildExtraRead, false);
#[cfg(test)]
sync_async_write_endpoint_for_test!(RuntimeChildExtraWrite);
#[cfg(test)]
sync_async_read_endpoint_for_test!(RuntimeChildExtraDuplex, false);
#[cfg(test)]
sync_async_write_endpoint_for_test!(RuntimeChildExtraDuplex);
#[cfg(test)]
sync_async_read_endpoint_for_test!(RuntimePtyMaster, true);
#[cfg(test)]
sync_async_write_endpoint_for_test!(RuntimePtyMaster);

impl RuntimePtyMaster {
    pub(super) fn split(self) -> io::Result<(RuntimePtyRead, RuntimePtyWrite)> {
        let raw = self.0.descriptor.get_ref().as_raw_fd();
        let duplicate = loop {
            let result = unsafe { libc::fcntl(raw, libc::F_DUPFD_CLOEXEC, 0) };
            if result != -1 {
                break result;
            }
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::EINTR) {
                return Err(error);
            }
        };
        let duplicate = unsafe { OwnedFd::from_raw_fd(duplicate) };
        let writer = RuntimeAsyncFd::attach(duplicate, Interest::WRITABLE).map_err(|error| {
            io::Error::other(format!("failed to attach duplicated PTY writer: {error:?}"))
        })?;
        Ok((RuntimePtyRead(self.0), RuntimePtyWrite(writer)))
    }

    pub(super) fn resize(
        &self,
        rows: u16,
        columns: u16,
        pixel_width: u16,
        pixel_height: u16,
    ) -> io::Result<()> {
        let size = libc::winsize {
            ws_row: rows,
            ws_col: columns,
            ws_xpixel: pixel_width,
            ws_ypixel: pixel_height,
        };
        loop {
            let result = unsafe {
                libc::ioctl(
                    self.0.descriptor.get_ref().as_raw_fd(),
                    libc::TIOCSWINSZ,
                    std::ptr::addr_of!(size),
                )
            };
            if result == 0 {
                return Ok(());
            }
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::EINTR) {
                return Err(error);
            }
        }
    }
}

impl RuntimePtyWrite {
    pub(super) fn resize(
        &self,
        rows: u16,
        columns: u16,
        pixel_width: u16,
        pixel_height: u16,
    ) -> io::Result<()> {
        let size = libc::winsize {
            ws_row: rows,
            ws_col: columns,
            ws_xpixel: pixel_width,
            ws_ypixel: pixel_height,
        };
        loop {
            let result = unsafe {
                libc::ioctl(
                    self.0.descriptor.get_ref().as_raw_fd(),
                    libc::TIOCSWINSZ,
                    std::ptr::addr_of!(size),
                )
            };
            if result == 0 {
                return Ok(());
            }
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::EINTR) {
                return Err(error);
            }
        }
    }
}

fn set_nonblocking(fd: RawFd) -> io::Result<()> {
    let current = loop {
        let result = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        if result != -1 {
            break result;
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::EINTR) {
            return Err(error);
        }
    };
    if current & libc::O_NONBLOCK != 0 {
        return Ok(());
    }
    loop {
        if unsafe { libc::fcntl(fd, libc::F_SETFL, current | libc::O_NONBLOCK) } != -1 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::EINTR) {
            return Err(error);
        }
    }
}

fn read_descriptor(fd: RawFd, buffer: &mut [u8], pty: bool) -> io::Result<usize> {
    loop {
        let result = unsafe { libc::read(fd, buffer.as_mut_ptr().cast(), buffer.len()) };
        if result >= 0 {
            return Ok(result as usize);
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::EINTR) {
            continue;
        }
        #[cfg(target_os = "linux")]
        if pty && error.raw_os_error() == Some(libc::EIO) {
            return Ok(0);
        }
        #[cfg(not(target_os = "linux"))]
        let _ = pty;
        return Err(error);
    }
}

fn write_descriptor(fd: RawFd, buffer: &[u8]) -> io::Result<usize> {
    loop {
        let result = unsafe { libc::write(fd, buffer.as_ptr().cast(), buffer.len()) };
        if result >= 0 {
            return Ok(result as usize);
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::EINTR) {
            return Err(error);
        }
    }
}

#[cfg(test)]
pub(super) struct ShadowRuntimeChild {
    pid: pid_t,
    terminal: Option<ShadowExitStatus>,
    successful_wait_syscalls: usize,
}

#[cfg(test)]
impl ShadowRuntimeChild {
    fn new(pid: pid_t) -> Self {
        Self {
            pid,
            terminal: None,
            successful_wait_syscalls: 0,
        }
    }

    pub(super) fn wait(&mut self) -> Result<ShadowExitStatus, UnixShadowSpawnError> {
        if let Some(status) = self.terminal {
            return Ok(status);
        }
        let raw = wait_unpublished_child_blocking(self.pid)?;
        self.successful_wait_syscalls += 1;
        let status = ShadowExitStatus(ExitStatus::from_raw(raw));
        self.terminal = Some(status);
        Ok(status)
    }

    #[cfg(test)]
    pub(super) const fn successful_wait_syscalls_for_test(&self) -> usize {
        self.successful_wait_syscalls
    }
}

#[cfg(test)]
impl Drop for ShadowRuntimeChild {
    fn drop(&mut self) {
        if self.terminal.is_some() {
            return;
        }
        // The future supervisor will retain the wait registration after a
        // facade drops. Until that identity owner exists, neither numeric-PID
        // kill nor silent wait-right abandonment is safe.
        std::process::abort();
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ShadowExitStatus(ExitStatus);

#[cfg(test)]
impl ShadowExitStatus {
    pub(super) fn code(self) -> Option<i32> {
        self.0.code()
    }
}

/// One close-on-exec AF_UNIX stream pair for a broker lane above fd 2,
/// mirroring libuv's Node 20 socketpair transport for extra stdio.
fn extra_socketpair(
    control: &mut UnixSpawnTestControl,
) -> Result<(OwnedFd, OwnedFd), UnixShadowSpawnError> {
    let mut raw = [-1; 2];
    #[cfg(target_os = "linux")]
    let socket_type = libc::SOCK_STREAM | libc::SOCK_CLOEXEC;
    #[cfg(not(target_os = "linux"))]
    let socket_type = libc::SOCK_STREAM;
    if unsafe { libc::socketpair(libc::AF_UNIX, socket_type, 0, raw.as_mut_ptr()) } == -1 {
        return Err(UnixShadowSpawnError::Descriptor {
            stage: UnixShadowDescriptorStage::ExtraSocketPair,
            errno: last_errno(),
        });
    }
    control.record_descriptor_action();
    // SAFETY: both descriptors were freshly returned by the successful call.
    let parent = unsafe { OwnedFd::from_raw_fd(raw[0]) };
    let child = unsafe { OwnedFd::from_raw_fd(raw[1]) };
    #[cfg(not(target_os = "linux"))]
    for fd in [parent.as_raw_fd(), child.as_raw_fd()] {
        loop {
            if unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) } == 0 {
                break;
            }
            let errno = last_errno();
            if errno != libc::EINTR {
                return Err(UnixShadowSpawnError::Descriptor {
                    stage: UnixShadowDescriptorStage::SetCloseOnExec,
                    errno,
                });
            }
        }
    }
    Ok((parent, child))
}

/// Shut down the transfer direction a lane never uses, exactly as the macOS
/// Node 20 route restricts its socketpair lanes.
fn shutdown_extra_direction(
    parent: &OwnedFd,
    child: &OwnedFd,
    direction: UnixExtraPipeDirection,
) -> Result<(), UnixShadowSpawnError> {
    let (fd, how) = match direction {
        UnixExtraPipeDirection::ChildToHost => (parent.as_raw_fd(), libc::SHUT_WR),
        UnixExtraPipeDirection::HostToChild => (child.as_raw_fd(), libc::SHUT_WR),
        UnixExtraPipeDirection::Duplex => return Ok(()),
    };
    loop {
        if unsafe { libc::shutdown(fd, how) } == 0 {
            return Ok(());
        }
        let errno = last_errno();
        if errno != libc::EINTR {
            return Err(UnixShadowSpawnError::Descriptor {
                stage: UnixShadowDescriptorStage::ExtraDirectionShutdown,
                errno,
            });
        }
    }
}

fn anonymous_pipe(
    stage: UnixShadowDescriptorStage,
    control: &mut UnixSpawnTestControl,
) -> Result<(OwnedFd, OwnedFd), UnixShadowSpawnError> {
    let mut fds = [-1; 2];
    #[cfg(any(
        target_os = "android",
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "illumos",
        target_os = "linux",
        target_os = "netbsd",
        target_os = "openbsd"
    ))]
    let result = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) };
    #[cfg(not(any(
        target_os = "android",
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "illumos",
        target_os = "linux",
        target_os = "netbsd",
        target_os = "openbsd"
    )))]
    let result = unsafe { libc::pipe(fds.as_mut_ptr()) };
    if result == -1 {
        return Err(UnixShadowSpawnError::Descriptor {
            stage,
            errno: last_errno(),
        });
    }
    control.record_descriptor_action();
    // SAFETY: both descriptors were freshly returned by the successful call.
    let reader = unsafe { OwnedFd::from_raw_fd(fds[0]) };
    let writer = unsafe { OwnedFd::from_raw_fd(fds[1]) };
    #[cfg(not(any(
        target_os = "android",
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "illumos",
        target_os = "linux",
        target_os = "netbsd",
        target_os = "openbsd"
    )))]
    {
        set_close_on_exec(reader.as_raw_fd(), control)?;
        set_close_on_exec(writer.as_raw_fd(), control)?;
    }
    Ok((reader, writer))
}

#[cfg(target_os = "linux")]
fn create_exec_error_channel(
    control: &mut UnixSpawnTestControl,
) -> Result<(OwnedFd, OwnedFd), UnixShadowSpawnError> {
    let mut fds = [-1; 2];
    let result = unsafe {
        libc::socketpair(
            libc::AF_UNIX,
            libc::SOCK_SEQPACKET | libc::SOCK_CLOEXEC,
            0,
            fds.as_mut_ptr(),
        )
    };
    if result == -1 {
        return Err(UnixShadowSpawnError::Descriptor {
            stage: UnixShadowDescriptorStage::ExecErrorChannel,
            errno: last_errno(),
        });
    }
    control.record_descriptor_action();
    // SAFETY: both descriptors were freshly returned by `socketpair`.
    let reader = unsafe { OwnedFd::from_raw_fd(fds[0]) };
    let writer = unsafe { OwnedFd::from_raw_fd(fds[1]) };
    Ok((reader, writer))
}

#[cfg(not(target_os = "linux"))]
fn create_exec_error_channel(
    control: &mut UnixSpawnTestControl,
) -> Result<(OwnedFd, OwnedFd), UnixShadowSpawnError> {
    anonymous_pipe(UnixShadowDescriptorStage::ExecErrorChannel, control)
}

fn open_dev_null(
    index: usize,
    control: &mut UnixSpawnTestControl,
) -> Result<OwnedFd, UnixShadowSpawnError> {
    let stage = null_stage(index);
    let access = if index == 0 {
        libc::O_RDONLY
    } else {
        libc::O_WRONLY
    };
    let raw = loop {
        let raw = unsafe { rust193_open(DEV_NULL.as_ptr().cast(), access | libc::O_CLOEXEC, 0) };
        if raw != -1 || last_errno() != libc::EINTR {
            break raw;
        }
    };
    if raw == -1 {
        return Err(UnixShadowSpawnError::Descriptor {
            stage,
            errno: last_errno(),
        });
    }
    control.record_descriptor_action();
    // SAFETY: `open` returned a new owned descriptor.
    Ok(unsafe { OwnedFd::from_raw_fd(raw) })
}

#[cfg(all(target_os = "linux", not(target_env = "musl")))]
unsafe fn rust193_open(path: *const c_char, flags: c_int, mode: c_int) -> c_int {
    unsafe { libc::open64(path, flags, mode) }
}

#[cfg(not(all(target_os = "linux", not(target_env = "musl"))))]
unsafe fn rust193_open(path: *const c_char, flags: c_int, mode: c_int) -> c_int {
    unsafe { libc::open(path, flags, mode) }
}

#[cfg(not(any(
    target_os = "android",
    target_os = "dragonfly",
    target_os = "freebsd",
    target_os = "illumos",
    target_os = "linux",
    target_os = "netbsd",
    target_os = "openbsd"
)))]
fn set_close_on_exec(
    fd: RawFd,
    control: &mut UnixSpawnTestControl,
) -> Result<(), UnixShadowSpawnError> {
    let result = unsafe { libc::ioctl(fd, libc::FIOCLEX) };
    if result == -1 {
        return Err(UnixShadowSpawnError::Descriptor {
            stage: UnixShadowDescriptorStage::SetCloseOnExec,
            errno: last_errno(),
        });
    }
    control.record_descriptor_action();
    Ok(())
}

fn pipe_stage(index: usize) -> UnixShadowDescriptorStage {
    match index {
        0 => UnixShadowDescriptorStage::StdinPipe,
        1 => UnixShadowDescriptorStage::StdoutPipe,
        _ => UnixShadowDescriptorStage::StderrPipe,
    }
}

fn null_stage(index: usize) -> UnixShadowDescriptorStage {
    match index {
        0 => UnixShadowDescriptorStage::StdinNull,
        1 => UnixShadowDescriptorStage::StdoutNull,
        _ => UnixShadowDescriptorStage::StderrNull,
    }
}

unsafe fn child_main(
    image: &UnixExecImage,
    stdio: &PreparedStdio,
    plan: Rust193DirectPipeShadowPlan,
    error_read: RawFd,
    error_write: RawFd,
) -> ! {
    // The read half must not survive into child setup; every other extra
    // descriptor is close-on-exec and therefore cannot retain stdio EOF after
    // a successful image replacement.
    unsafe {
        libc::close(error_read);
    }
    let errno = unsafe { child_exec(image, stdio, plan) };
    let record = encode_rust193_exec_error(errno);
    if !unsafe { write_exec_error_record(error_write, &record) } {
        unsafe { libc::abort() }
    }
    unsafe { libc::_exit(1) }
}

unsafe fn child_exec(
    image: &UnixExecImage,
    stdio: &PreparedStdio,
    plan: Rust193DirectPipeShadowPlan,
) -> c_int {
    if plan.image_dialect == ProcessImageDialect::Node20Libuv146 {
        for signal in 1..32 {
            if signal == libc::SIGKILL || signal == libc::SIGSTOP {
                continue;
            }
            if unsafe { libc::signal(signal, libc::SIG_DFL) } == libc::SIG_ERR {
                return last_errno();
            }
        }
        match plan.topology {
            ProcessTopology::Inherit => {}
            ProcessTopology::UnixNewProcessGroup => {
                if unsafe { libc::setpgid(0, 0) } == -1 {
                    return last_errno();
                }
            }
            ProcessTopology::UnixNewSession => unsafe {
                // Node/libuv deliberately does not inspect the detached setsid
                // result before continuing with child setup.
                libc::setsid();
            },
            ProcessTopology::UnixControllingPty => {
                if unsafe { libc::setsid() } == -1 {
                    return last_errno();
                }
            }
            ProcessTopology::WindowsDetached => return libc::EINVAL,
        }
    }

    for (target, source) in stdio.child.iter().enumerate() {
        if let Some(source) = source {
            if unsafe { retrying_dup2(source.as_raw_fd(), target as RawFd) } == -1 {
                return last_errno();
            }
        }
    }

    // Broker lanes: a fresh lane endpoint may already occupy another lane's
    // target number, so first move every conflicting source above the lane
    // region (dup2 also cannot clear close-on-exec when source == target).
    let extra_count = stdio.extra.len();
    if extra_count > 0 {
        let floor = (3 + extra_count) as RawFd;
        let mut sources = [-1 as RawFd; MAX_EXTRA_STDIO];
        for (offset, extra) in stdio.extra.iter().enumerate() {
            let Some(child) = extra.child.as_ref() else {
                return libc::EBADF;
            };
            sources[offset] = child.as_raw_fd();
        }
        for source in sources.iter_mut().take(extra_count) {
            if *source >= 3 && *source < floor {
                let moved = unsafe { retrying_dupfd_cloexec(*source, floor) };
                if moved == -1 {
                    return last_errno();
                }
                *source = moved;
            }
        }
        for (offset, extra) in stdio.extra.iter().enumerate() {
            let target = extra.child_fd as RawFd;
            if unsafe { retrying_dup2(sources[offset], target) } == -1 {
                return last_errno();
            }
        }
    }

    if plan.image_dialect == ProcessImageDialect::Node20Libuv146
        && plan.topology == ProcessTopology::UnixControllingPty
        && unsafe { libc::ioctl(0, libc::TIOCSCTTY.into(), 0) } == -1
    {
        return last_errno();
    }

    if let Some(cwd) = image.cwd() {
        if unsafe { libc::chdir(cwd.as_ptr()) } == -1 {
            return last_errno();
        }
    }

    if plan.image_dialect == ProcessImageDialect::Rust193 {
        let previous = unsafe { libc::signal(libc::SIGPIPE, libc::SIG_DFL) };
        if previous == libc::SIG_ERR {
            return last_errno();
        }
    }

    unsafe { install_child_environment(image.environment()) };
    unsafe { libc::execvp(image.file().as_ptr(), image.argv_with_null().as_ptr()) };
    last_errno()
}

unsafe fn retrying_dup2(source: RawFd, target: RawFd) -> c_int {
    loop {
        let result = unsafe { libc::dup2(source, target) };
        if result != -1 || last_errno() != libc::EINTR {
            return result;
        }
    }
}

/// Async-signal-safe `F_DUPFD_CLOEXEC` used by the forked child to move a
/// broker lane endpoint above the lane target region before renumbering.
unsafe fn retrying_dupfd_cloexec(source: RawFd, floor: RawFd) -> c_int {
    loop {
        let result = unsafe { libc::fcntl(source, libc::F_DUPFD_CLOEXEC, floor) };
        if result != -1 || last_errno() != libc::EINTR {
            return result;
        }
    }
}

unsafe fn write_exec_error_record(fd: RawFd, record: &[u8; EXEC_ERROR_RECORD_LEN]) -> bool {
    // Rust 1.93 asserts only that `FileDesc::write` returned `Ok`; it does not
    // retry EINTR or reject a theoretically short successful write here.
    unsafe { libc::write(fd, record.as_ptr().cast(), record.len()) != -1 }
}

unsafe fn install_child_environment(environment: &UnixEnvironmentImage) {
    if let UnixEnvironmentImage::Constructed { envp_with_null, .. } = environment {
        unsafe { set_environ(envp_with_null.as_ptr().cast_mut().cast()) };
    }
}

#[cfg(target_vendor = "apple")]
unsafe fn set_environ(environment: *mut *mut c_char) {
    unsafe extern "C" {
        fn _NSGetEnviron() -> *mut *mut *mut c_char;
    }
    unsafe {
        *_NSGetEnviron() = environment;
    }
}

#[cfg(not(target_vendor = "apple"))]
unsafe fn set_environ(environment: *mut *mut c_char) {
    unsafe extern "C" {
        static mut environ: *mut *mut c_char;
    }
    unsafe {
        environ = environment;
    }
}

enum ExecHandshake {
    Success,
    ChildError(c_int),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExecHandshakeInvariant {
    Read { errno: c_int },
    Invalid(UnixExecErrorRecordError),
}

fn read_exec_error_record(
    fd: RawFd,
    fault: UnixSpawnTestFault,
) -> Result<ExecHandshake, ExecHandshakeInvariant> {
    let mut record = [0_u8; EXEC_ERROR_RECORD_LEN];
    loop {
        let read = unsafe { libc::read(fd, record.as_mut_ptr().cast(), record.len()) };
        if read == 0 {
            return Ok(ExecHandshake::Success);
        }
        if read == -1 {
            let errno = last_errno();
            if errno == libc::EINTR {
                continue;
            }
            return Err(ExecHandshakeInvariant::Read { errno });
        }
        if fault == UnixSpawnTestFault::CorruptExecErrorFooter && read == 8 {
            record[7] ^= 0xff;
        }
        return decode_rust193_exec_error(&record[..read as usize])
            .map(ExecHandshake::ChildError)
            .map_err(ExecHandshakeInvariant::Invalid);
    }
}

pub(super) fn encode_rust193_exec_error(errno: c_int) -> [u8; EXEC_ERROR_RECORD_LEN] {
    let bytes = (errno as u32).to_be_bytes();
    [
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        EXEC_ERROR_FOOTER[0],
        EXEC_ERROR_FOOTER[1],
        EXEC_ERROR_FOOTER[2],
        EXEC_ERROR_FOOTER[3],
    ]
}

pub(super) fn decode_rust193_exec_error(record: &[u8]) -> Result<c_int, UnixExecErrorRecordError> {
    if record.len() != EXEC_ERROR_RECORD_LEN {
        return Err(UnixExecErrorRecordError::WrongLength {
            actual: record.len(),
        });
    }
    if record[4..] != EXEC_ERROR_FOOTER {
        return Err(UnixExecErrorRecordError::WrongFooter);
    }
    Ok(i32::from_be_bytes([
        record[0], record[1], record[2], record[3],
    ]))
}

fn wait_unpublished_child_blocking(pid: pid_t) -> Result<c_int, UnixShadowSpawnError> {
    let mut status = 0;
    loop {
        let waited = unsafe { libc::waitpid(pid, &mut status, 0) };
        if waited == pid {
            return Ok(status);
        }
        let errno = last_errno();
        if waited == -1 && errno == libc::EINTR {
            continue;
        }
        return Err(UnixShadowSpawnError::Wait { errno });
    }
}

#[cfg(test)]
fn retrying_kill(pid: pid_t, signal: c_int) -> Result<(), c_int> {
    loop {
        let result = unsafe { libc::kill(pid, signal) };
        if result == 0 {
            return Ok(());
        }
        let errno = last_errno();
        if errno != libc::EINTR {
            return Err(errno);
        }
    }
}

fn close_owned(fd: OwnedFd, control: &mut UnixSpawnTestControl) {
    control.record_closed();
    drop(fd);
}

fn last_errno() -> c_int {
    io::Error::last_os_error()
        .raw_os_error()
        .unwrap_or(libc::EINVAL)
}

#[cfg(test)]
fn descriptor_is_fifo(fd: RawFd) -> bool {
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    let result = unsafe { libc::fstat(fd, stat.as_mut_ptr()) };
    result == 0 && unsafe { stat.assume_init() }.st_mode & libc::S_IFMT == libc::S_IFIFO
}

#[cfg(test)]
fn descriptor_is_close_on_exec(fd: RawFd) -> bool {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    flags != -1 && flags & libc::FD_CLOEXEC != 0
}

#[cfg(test)]
fn descriptor_is_nonblocking(fd: RawFd) -> bool {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    flags != -1 && flags & libc::O_NONBLOCK != 0
}
