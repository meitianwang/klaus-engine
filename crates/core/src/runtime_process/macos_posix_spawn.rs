//! Frozen macOS process-image selection and injected `posix_spawn` driver.
//!
//! Node 20/libuv 1.46 and Rust 1.93 deliberately enter through distinct
//! predicates and distinct syscall recipes.  The real Darwin adapter lives
//! behind `cfg(target_os = "macos")`; tests on every Unix host drive the same
//! recipes through an injected syscall surface.  No branch delegates to a
//! standard-library command builder.

use std::ffi::{CStr, CString};
use std::os::fd::RawFd;

#[cfg(target_os = "macos")]
use std::io;
#[cfg(target_os = "macos")]
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
#[cfg(target_os = "macos")]
use std::sync::{MutexGuard, PoisonError};

use libc::{c_int, pid_t};

#[cfg(target_os = "macos")]
use super::reaper::UnixSpawnAdmission;
use super::unix::{UnixEnvironmentImage, UnixExecImage};
#[cfg(target_os = "macos")]
use super::unix_spawn::{
    PreparedExtraStdio, PreparedStdio, Rust193DirectPipeShadowPlan,
    Rust193ForkFallbackShadowSpawner, UnregisteredUnixSpawnedProcess, MAX_UNIX_STDIO,
    SHADOW_FORK_DESCRIPTOR_LOCK,
};
use super::unix_spawn::{
    UnixExtraPipeDirection, UnixShadowSpawnError, UnixShadowSpawnRoute, UnixStdioDisposition,
    UnixStdioPlan,
};
use super::{ProcessImageDialect, ProcessTopology};

const DARWIN_NAME_MAX: usize = 255;
const DARWIN_PATH_MAX: usize = 1024;
const DARWIN_DEFAULT_PATH: &[u8] = b"/usr/bin:/bin:/usr/sbin:/sbin";
const DEV_NULL: &CStr = c"/dev/null";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Node20MacOsSpawnFeatures {
    topology: ProcessTopology,
    ordinary_stdio: bool,
    set_uid: Option<libc::uid_t>,
    set_gid: Option<libc::gid_t>,
}

impl Node20MacOsSpawnFeatures {
    pub(super) const fn ordinary(topology: ProcessTopology) -> Self {
        Self {
            topology,
            ordinary_stdio: true,
            set_uid: None,
            set_gid: None,
        }
    }

    #[cfg(target_os = "macos")]
    const fn with_ordinary_stdio(mut self, ordinary_stdio: bool) -> Self {
        self.ordinary_stdio = ordinary_stdio;
        self
    }

    #[cfg(target_os = "macos")]
    const fn controlling_pty() -> Self {
        Self {
            topology: ProcessTopology::UnixControllingPty,
            ordinary_stdio: false,
            set_uid: None,
            set_gid: None,
        }
    }

    #[cfg(test)]
    pub(super) const fn with_credentials(mut self, set_uid: bool, set_gid: bool) -> Self {
        self.set_uid = if set_uid { Some(0) } else { None };
        self.set_gid = if set_gid { Some(0) } else { None };
        self
    }
}

#[cfg(target_os = "macos")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct MacOsPtySize {
    pub(super) rows: u16,
    pub(super) columns: u16,
    pub(super) pixel_width: u16,
    pub(super) pixel_height: u16,
}

#[cfg(target_os = "macos")]
impl MacOsPtySize {
    pub(super) const fn new(rows: u16, columns: u16) -> Self {
        Self {
            rows,
            columns,
            pixel_width: 0,
            pixel_height: 0,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Rust193ProgramKind {
    PathLookup,
    Relative,
    Absolute,
}

impl Rust193ProgramKind {
    fn from_file(file: &CStr) -> Self {
        let bytes = file.to_bytes();
        if bytes.starts_with(b"/") {
            Self::Absolute
        } else if bytes.contains(&b'/') {
            Self::Relative
        } else {
            Self::PathLookup
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Rust193MacOsSpawnFeatures {
    gid: bool,
    uid: bool,
    groups: bool,
    chroot: bool,
    callbacks: bool,
    mutated_path: bool,
    program_kind: Rust193ProgramKind,
    cwd_present: bool,
    create_pidfd: bool,
    process_group: Option<pid_t>,
    setsid: bool,
}

impl Rust193MacOsSpawnFeatures {
    pub(super) const fn source_default() -> Self {
        Self {
            gid: false,
            uid: false,
            groups: false,
            chroot: false,
            callbacks: false,
            mutated_path: false,
            program_kind: Rust193ProgramKind::PathLookup,
            cwd_present: false,
            create_pidfd: false,
            process_group: None,
            setsid: false,
        }
    }

    pub(super) fn for_image(image: &UnixExecImage) -> Result<Self, MacOsPosixSpawnError> {
        require_dialect(image, ProcessImageDialect::Rust193)?;
        let mutated_path = image
            .rust193_path_changed()
            .unwrap_or_else(|| std::process::abort());
        Ok(Self {
            mutated_path,
            program_kind: Rust193ProgramKind::from_file(image.file()),
            cwd_present: image.cwd().is_some(),
            ..Self::source_default()
        })
    }

    #[cfg(test)]
    pub(super) const fn with_mutated_path_and_bare_program(
        mut self,
        mutated_path: bool,
        bare_program: bool,
    ) -> Self {
        self.mutated_path = mutated_path;
        self.program_kind = if bare_program {
            Rust193ProgramKind::PathLookup
        } else {
            Rust193ProgramKind::Relative
        };
        self
    }

    #[cfg(test)]
    pub(super) const fn with_source_restrictions(
        mut self,
        gid: bool,
        uid: bool,
        groups: bool,
        chroot: bool,
        callbacks: bool,
    ) -> Self {
        self.gid = gid;
        self.uid = uid;
        self.groups = groups;
        self.chroot = chroot;
        self.callbacks = callbacks;
        self
    }

    #[cfg(test)]
    pub(super) const fn with_cwd_and_program(
        mut self,
        cwd_present: bool,
        program_kind: Rust193ProgramKind,
    ) -> Self {
        self.cwd_present = cwd_present;
        self.program_kind = program_kind;
        self
    }

    #[cfg(test)]
    pub(super) const fn with_process_group(mut self, process_group: Option<pid_t>) -> Self {
        self.process_group = process_group;
        self
    }

    #[cfg(test)]
    pub(super) const fn with_setsid(mut self, setsid: bool) -> Self {
        self.setsid = setsid;
        self
    }

    #[cfg(test)]
    pub(super) const fn with_create_pidfd(mut self, create_pidfd: bool) -> Self {
        self.create_pidfd = create_pidfd;
        self
    }
}

struct Rust193ImageEligibility {
    features: Rust193MacOsSpawnFeatures,
}

impl Rust193ImageEligibility {
    fn derive(image: &UnixExecImage) -> Result<Self, MacOsPosixSpawnError> {
        Ok(Self {
            features: Rust193MacOsSpawnFeatures::for_image(image)?,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum MacOsPosixSpawnRouteError {
    UnsupportedNodeTopology(ProcessTopology),
    Rust193PidfdUnavailable,
}

pub(super) const fn select_node20_libuv146_route(
    features: Node20MacOsSpawnFeatures,
) -> Result<UnixShadowSpawnRoute, MacOsPosixSpawnRouteError> {
    if !features.ordinary_stdio {
        return Ok(UnixShadowSpawnRoute::Node20LibuvFork);
    }
    match features.topology {
        ProcessTopology::Inherit | ProcessTopology::UnixNewSession => {
            Ok(UnixShadowSpawnRoute::Node20LibuvPosixSpawn)
        }
        ProcessTopology::UnixNewProcessGroup | ProcessTopology::UnixControllingPty => {
            Ok(UnixShadowSpawnRoute::Node20LibuvFork)
        }
        ProcessTopology::WindowsDetached => Err(
            MacOsPosixSpawnRouteError::UnsupportedNodeTopology(features.topology),
        ),
    }
}

pub(super) const fn select_rust193_route(
    features: Rust193MacOsSpawnFeatures,
) -> Result<UnixShadowSpawnRoute, MacOsPosixSpawnRouteError> {
    if features.create_pidfd {
        return Err(MacOsPosixSpawnRouteError::Rust193PidfdUnavailable);
    }
    if features.gid
        || features.uid
        || features.groups
        || features.chroot
        || features.callbacks
        || (features.mutated_path
            && matches!(features.program_kind, Rust193ProgramKind::PathLookup))
        || (features.cwd_present && matches!(features.program_kind, Rust193ProgramKind::Relative))
    {
        Ok(UnixShadowSpawnRoute::Rust193ForkFallback)
    } else {
        Ok(UnixShadowSpawnRoute::Rust193PosixSpawn)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct MacOsSpawnFlags(u32);

impl MacOsSpawnFlags {
    pub(super) const CLOEXEC_DEFAULT: Self = Self(1 << 0);
    pub(super) const SET_SIGDEFAULT: Self = Self(1 << 1);
    pub(super) const SET_SIGMASK: Self = Self(1 << 2);
    pub(super) const SET_SID: Self = Self(1 << 3);
    pub(super) const SET_PGROUP: Self = Self(1 << 4);

    const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    #[cfg(test)]
    pub(super) const fn union_for_test(self, other: Self) -> Self {
        self.union(other)
    }

    #[cfg(test)]
    pub(super) const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum MacOsSignalSet {
    Full,
    Empty,
    Rust193Default,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum MacOsSpawnFunction {
    Direct,
    PathSearch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum MacOsPosixSpawnOperation {
    SocketPair,
    PtyOpenMaster,
    PtyGrant,
    PtyUnlock,
    PtyName,
    PtyOpenSlave,
    PtyInitialWindowSize,
    PtyDuplicate,
    ErrorPipe,
    CloseOnExec,
    Fork,
    SignalMask,
    ChildSetup,
    Wait,
    AttributeInit,
    AttributeFlags,
    AttributeSignalDefault,
    AttributeSignalMask,
    AttributeProcessGroup,
    FileActionsInit,
    FileActionChdir,
    FileActionPreDuplicate,
    FileActionDuplicate,
    FileActionOpenNull,
    FileActionInherit,
    FileActionClose,
    Spawn,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum MacOsForkFallbackReason {
    NodeTopology,
    NodeNonOrdinaryStdio,
    NodeCredentials,
    NodeSetSidUnavailable,
    NodeAttributeEnosys(MacOsPosixSpawnOperation),
    NodeCwdActionUnavailable,
    Rust193Eligibility,
    Rust193CwdActionUnavailable,
    Rust193SetSidUnsupported,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum MacOsSpawnAttempt {
    Spawned(pid_t),
    ForkFallback(MacOsForkFallbackReason),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum MacOsPosixSpawnError {
    Route(MacOsPosixSpawnRouteError),
    ImageDialectMismatch {
        image: ProcessImageDialect,
        requested: ProcessImageDialect,
    },
    MissingNodeEnvironment,
    MissingChildDescriptor {
        index: usize,
    },
    Syscall {
        operation: MacOsPosixSpawnOperation,
        errno: c_int,
    },
    UnexpectedEnosysAfterFileMutation {
        operation: MacOsPosixSpawnOperation,
    },
    Os {
        operation: MacOsPosixSpawnOperation,
        errno: c_int,
    },
    Unix(UnixShadowSpawnError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Node20StdioFileImage {
    child_fds: [Option<RawFd>; MAX_UNIX_STDIO],
    stdio_count: usize,
}

impl Node20StdioFileImage {
    pub(super) fn from_plan<const N: usize>(
        plan: UnixStdioPlan,
        pipe_child_fds: [Option<RawFd>; N],
    ) -> Result<Self, MacOsPosixSpawnError> {
        let dispositions = plan.dispositions();
        let mut child_fds = [None; MAX_UNIX_STDIO];
        for (index, disposition) in dispositions.into_iter().enumerate() {
            child_fds[index] = match disposition {
                UnixStdioDisposition::Pipe => Some(
                    pipe_child_fds[index]
                        .ok_or(MacOsPosixSpawnError::MissingChildDescriptor { index })?,
                ),
                UnixStdioDisposition::Inherit => Some(index as RawFd),
                UnixStdioDisposition::Null => None,
            };
        }
        for extra in plan.extra() {
            let index = usize::try_from(extra.child_fd()).unwrap_or_else(|_| std::process::abort());
            child_fds[index] = Some(
                pipe_child_fds
                    .get(index)
                    .copied()
                    .flatten()
                    .ok_or(MacOsPosixSpawnError::MissingChildDescriptor { index })?,
            );
        }
        Ok(Self {
            child_fds,
            stdio_count: plan.stdio_count(),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Rust193StdioFileImage {
    child_fds: [Option<RawFd>; MAX_UNIX_STDIO],
    stdio_count: usize,
}

impl Rust193StdioFileImage {
    pub(super) fn from_plan(
        plan: UnixStdioPlan,
        configured_child_fds: [Option<RawFd>; MAX_UNIX_STDIO],
    ) -> Result<Self, MacOsPosixSpawnError> {
        let dispositions = plan.dispositions();
        for (index, disposition) in dispositions.into_iter().enumerate() {
            let needs_fd = disposition != UnixStdioDisposition::Inherit;
            if needs_fd && configured_child_fds[index].is_none() {
                return Err(MacOsPosixSpawnError::MissingChildDescriptor { index });
            }
        }
        for extra in plan.extra() {
            let index = extra.child_fd() as usize;
            if configured_child_fds[index].is_none() {
                return Err(MacOsPosixSpawnError::MissingChildDescriptor { index });
            }
        }
        Ok(Self {
            child_fds: configured_child_fds,
            stdio_count: plan.stdio_count(),
        })
    }
}

/// Injectable spelling of exactly the Darwin calls used by both recipes.
/// Implementations retain their opaque attr/file-action storage internally.
pub(super) trait MacOsPosixSpawnSyscalls {
    fn node_setsid_supported(&mut self) -> bool;
    fn node_addchdir_available(&mut self) -> bool;
    fn rust_addchdir_available(&mut self) -> bool;

    fn attr_init(&mut self) -> Result<(), c_int>;
    fn attr_destroy(&mut self);
    fn attr_set_flags(&mut self, flags: MacOsSpawnFlags) -> Result<(), c_int>;
    fn attr_set_signal_default(&mut self, set: MacOsSignalSet) -> Result<(), c_int>;
    fn attr_set_signal_mask(&mut self, set: MacOsSignalSet) -> Result<(), c_int>;
    fn attr_set_process_group(&mut self, process_group: pid_t) -> Result<(), c_int>;

    fn file_actions_init(&mut self) -> Result<(), c_int>;
    fn file_actions_destroy(&mut self);
    fn file_actions_add_chdir(&mut self, cwd: &CStr) -> Result<(), c_int>;
    fn file_actions_add_dup2(&mut self, source: RawFd, target: RawFd) -> Result<(), c_int>;
    fn file_actions_add_open(
        &mut self,
        target: RawFd,
        path: &CStr,
        flags: c_int,
        mode: libc::mode_t,
    ) -> Result<(), c_int>;
    fn file_actions_add_inherit(&mut self, fd: RawFd) -> Result<(), c_int>;
    fn file_actions_add_close(&mut self, fd: RawFd) -> Result<(), c_int>;
    fn set_blocking(&mut self, fd: RawFd);

    fn spawn(
        &mut self,
        function: MacOsSpawnFunction,
        file: &CStr,
        image: &UnixExecImage,
    ) -> Result<pid_t, c_int>;
}

pub(super) fn spawn_node20_libuv146_injected<S: MacOsPosixSpawnSyscalls>(
    syscalls: &mut S,
    image: &UnixExecImage,
    features: Node20MacOsSpawnFeatures,
    stdio: Node20StdioFileImage,
) -> Result<MacOsSpawnAttempt, MacOsPosixSpawnError> {
    require_dialect(image, ProcessImageDialect::Node20Libuv146)?;
    match select_node20_libuv146_route(features).map_err(MacOsPosixSpawnError::Route)? {
        UnixShadowSpawnRoute::Node20LibuvFork => {
            let reason = if features.ordinary_stdio {
                MacOsForkFallbackReason::NodeTopology
            } else {
                MacOsForkFallbackReason::NodeNonOrdinaryStdio
            };
            return Ok(MacOsSpawnAttempt::ForkFallback(reason));
        }
        UnixShadowSpawnRoute::Node20LibuvPosixSpawn => {}
        _ => unreachable!("Node route selector returned a foreign dialect"),
    }

    if let Err(errno) = syscalls.attr_init() {
        return node_pre_mutation_error(
            errno,
            MacOsPosixSpawnOperation::AttributeInit,
            MacOsForkFallbackReason::NodeAttributeEnosys(MacOsPosixSpawnOperation::AttributeInit),
        );
    }

    if features.set_uid.is_some() || features.set_gid.is_some() {
        syscalls.attr_destroy();
        return Ok(MacOsSpawnAttempt::ForkFallback(
            MacOsForkFallbackReason::NodeCredentials,
        ));
    }

    let mut flags = MacOsSpawnFlags::CLOEXEC_DEFAULT
        .union(MacOsSpawnFlags::SET_SIGDEFAULT)
        .union(MacOsSpawnFlags::SET_SIGMASK);
    if features.topology == ProcessTopology::UnixNewSession {
        if !syscalls.node_setsid_supported() {
            syscalls.attr_destroy();
            return Ok(MacOsSpawnAttempt::ForkFallback(
                MacOsForkFallbackReason::NodeSetSidUnavailable,
            ));
        }
        flags = flags.union(MacOsSpawnFlags::SET_SID);
    }

    if let Err(errno) = syscalls.attr_set_flags(flags) {
        syscalls.attr_destroy();
        return node_pre_mutation_error(
            errno,
            MacOsPosixSpawnOperation::AttributeFlags,
            MacOsForkFallbackReason::NodeAttributeEnosys(MacOsPosixSpawnOperation::AttributeFlags),
        );
    }
    if let Err(errno) = syscalls.attr_set_signal_default(MacOsSignalSet::Full) {
        syscalls.attr_destroy();
        return node_pre_mutation_error(
            errno,
            MacOsPosixSpawnOperation::AttributeSignalDefault,
            MacOsForkFallbackReason::NodeAttributeEnosys(
                MacOsPosixSpawnOperation::AttributeSignalDefault,
            ),
        );
    }
    if let Err(errno) = syscalls.attr_set_signal_mask(MacOsSignalSet::Empty) {
        syscalls.attr_destroy();
        return node_pre_mutation_error(
            errno,
            MacOsPosixSpawnOperation::AttributeSignalMask,
            MacOsForkFallbackReason::NodeAttributeEnosys(
                MacOsPosixSpawnOperation::AttributeSignalMask,
            ),
        );
    }
    if let Err(errno) = syscalls.file_actions_init() {
        syscalls.attr_destroy();
        return node_pre_mutation_error(
            errno,
            MacOsPosixSpawnOperation::FileActionsInit,
            MacOsForkFallbackReason::NodeAttributeEnosys(MacOsPosixSpawnOperation::FileActionsInit),
        );
    }

    if let Some(cwd) = image.cwd() {
        if !syscalls.node_addchdir_available() {
            cleanup_actions_and_attrs(syscalls);
            return Ok(MacOsSpawnAttempt::ForkFallback(
                MacOsForkFallbackReason::NodeCwdActionUnavailable,
            ));
        }
        if let Err(errno) = syscalls.file_actions_add_chdir(cwd) {
            cleanup_actions_and_attrs(syscalls);
            return node_pre_mutation_error(
                errno,
                MacOsPosixSpawnOperation::FileActionChdir,
                MacOsForkFallbackReason::NodeAttributeEnosys(
                    MacOsPosixSpawnOperation::FileActionChdir,
                ),
            );
        }
    }

    if let Err(error) = add_node20_file_actions(syscalls, stdio) {
        cleanup_actions_and_attrs(syscalls);
        return Err(error);
    }

    let spawned = node20_resolve_and_spawn(syscalls, image);
    cleanup_actions_and_attrs(syscalls);
    spawned
        .map(MacOsSpawnAttempt::Spawned)
        .map_err(|errno| MacOsPosixSpawnError::Syscall {
            operation: MacOsPosixSpawnOperation::Spawn,
            errno,
        })
}

pub(super) fn spawn_rust193_injected<S: MacOsPosixSpawnSyscalls>(
    syscalls: &mut S,
    image: &UnixExecImage,
    stdio: Rust193StdioFileImage,
) -> Result<MacOsSpawnAttempt, MacOsPosixSpawnError> {
    let eligibility = Rust193ImageEligibility::derive(image)?;
    spawn_rust193_with_image_eligibility_injected(syscalls, image, eligibility, stdio)
}

fn spawn_rust193_with_image_eligibility_injected<S: MacOsPosixSpawnSyscalls>(
    syscalls: &mut S,
    image: &UnixExecImage,
    eligibility: Rust193ImageEligibility,
    stdio: Rust193StdioFileImage,
) -> Result<MacOsSpawnAttempt, MacOsPosixSpawnError> {
    let features = eligibility.features;
    require_dialect(image, ProcessImageDialect::Rust193)?;
    match select_rust193_route(features).map_err(MacOsPosixSpawnError::Route)? {
        UnixShadowSpawnRoute::Rust193ForkFallback => {
            return Ok(MacOsSpawnAttempt::ForkFallback(
                MacOsForkFallbackReason::Rust193Eligibility,
            ));
        }
        UnixShadowSpawnRoute::Rust193PosixSpawn => {}
        _ => unreachable!("Rust route selector returned a foreign dialect"),
    }

    if features.cwd_present && !syscalls.rust_addchdir_available() {
        return Ok(MacOsSpawnAttempt::ForkFallback(
            MacOsForkFallbackReason::Rust193CwdActionUnavailable,
        ));
    }

    syscalls
        .attr_init()
        .map_err(|errno| MacOsPosixSpawnError::Syscall {
            operation: MacOsPosixSpawnOperation::AttributeInit,
            errno,
        })?;
    if let Err(errno) = syscalls.file_actions_init() {
        syscalls.attr_destroy();
        return Err(MacOsPosixSpawnError::Syscall {
            operation: MacOsPosixSpawnOperation::FileActionsInit,
            errno,
        });
    }

    if let Err(error) = add_rust193_file_actions(syscalls, stdio) {
        cleanup_actions_and_attrs(syscalls);
        return Err(error);
    }
    if let Some(cwd) = image.cwd() {
        if let Err(errno) = syscalls.file_actions_add_chdir(cwd) {
            cleanup_actions_and_attrs(syscalls);
            return Err(MacOsPosixSpawnError::Syscall {
                operation: MacOsPosixSpawnOperation::FileActionChdir,
                errno,
            });
        }
    }

    let mut flags = MacOsSpawnFlags::default();
    if let Some(process_group) = features.process_group {
        if let Err(errno) = syscalls.attr_set_process_group(process_group) {
            cleanup_actions_and_attrs(syscalls);
            return Err(MacOsPosixSpawnError::Syscall {
                operation: MacOsPosixSpawnOperation::AttributeProcessGroup,
                errno,
            });
        }
        flags = flags.union(MacOsSpawnFlags::SET_PGROUP);
    }

    if let Err(errno) = syscalls.attr_set_signal_default(MacOsSignalSet::Rust193Default) {
        cleanup_actions_and_attrs(syscalls);
        return Err(MacOsPosixSpawnError::Syscall {
            operation: MacOsPosixSpawnOperation::AttributeSignalDefault,
            errno,
        });
    }
    flags = flags.union(MacOsSpawnFlags::SET_SIGDEFAULT);

    if features.setsid {
        cleanup_actions_and_attrs(syscalls);
        return Ok(MacOsSpawnAttempt::ForkFallback(
            MacOsForkFallbackReason::Rust193SetSidUnsupported,
        ));
    }

    if let Err(errno) = syscalls.attr_set_flags(flags) {
        cleanup_actions_and_attrs(syscalls);
        return Err(MacOsPosixSpawnError::Syscall {
            operation: MacOsPosixSpawnOperation::AttributeFlags,
            errno,
        });
    }

    let spawned = syscalls.spawn(MacOsSpawnFunction::PathSearch, image.file(), image);
    cleanup_actions_and_attrs(syscalls);
    spawned
        .map(MacOsSpawnAttempt::Spawned)
        .map_err(|errno| MacOsPosixSpawnError::Syscall {
            operation: MacOsPosixSpawnOperation::Spawn,
            errno,
        })
}

#[cfg(test)]
pub(super) fn spawn_rust193_test_features_injected<S: MacOsPosixSpawnSyscalls>(
    syscalls: &mut S,
    image: &UnixExecImage,
    features: Rust193MacOsSpawnFeatures,
    stdio: Rust193StdioFileImage,
) -> Result<MacOsSpawnAttempt, MacOsPosixSpawnError> {
    spawn_rust193_with_image_eligibility_injected(
        syscalls,
        image,
        Rust193ImageEligibility { features },
        stdio,
    )
}

fn require_dialect(
    image: &UnixExecImage,
    requested: ProcessImageDialect,
) -> Result<(), MacOsPosixSpawnError> {
    if image.image_dialect() != requested {
        Err(MacOsPosixSpawnError::ImageDialectMismatch {
            image: image.image_dialect(),
            requested,
        })
    } else {
        Ok(())
    }
}

fn node_pre_mutation_error(
    errno: c_int,
    operation: MacOsPosixSpawnOperation,
    fallback: MacOsForkFallbackReason,
) -> Result<MacOsSpawnAttempt, MacOsPosixSpawnError> {
    if errno == libc::ENOSYS {
        Ok(MacOsSpawnAttempt::ForkFallback(fallback))
    } else {
        Err(MacOsPosixSpawnError::Syscall { operation, errno })
    }
}

fn cleanup_actions_and_attrs<S: MacOsPosixSpawnSyscalls>(syscalls: &mut S) {
    syscalls.file_actions_destroy();
    syscalls.attr_destroy();
}

/// Rust 1.93 stdio plus broker lanes: renumber every configured descriptor to
/// its target with the same low-fd preduplication order the Node 20 recipe
/// uses, keep untouched slots inherited, and never open replacement devices.
fn add_rust193_file_actions<S: MacOsPosixSpawnSyscalls>(
    syscalls: &mut S,
    stdio: Rust193StdioFileImage,
) -> Result<(), MacOsPosixSpawnError> {
    let stdio_count = stdio.stdio_count;
    let mut child_fds = stdio.child_fds.map(|fd| fd.unwrap_or(-1));

    for target in 0..stdio_count {
        let source = child_fds[target];
        if source < 0 || source >= target as RawFd {
            continue;
        }
        let mut temporary = stdio_count as RawFd;
        loop {
            if child_fds.iter().all(|candidate| *candidate != temporary) {
                break;
            }
            temporary += 1;
        }
        if let Err(errno) = syscalls.file_actions_add_dup2(source, temporary) {
            return Err(MacOsPosixSpawnError::Syscall {
                operation: MacOsPosixSpawnOperation::FileActionPreDuplicate,
                errno,
            });
        }
        child_fds[target] = temporary;
    }

    for (target, &source) in child_fds[..stdio_count].iter().enumerate() {
        if source < 0 {
            continue;
        }
        if source == target as RawFd {
            if let Err(errno) = syscalls.file_actions_add_inherit(source) {
                return Err(MacOsPosixSpawnError::Syscall {
                    operation: MacOsPosixSpawnOperation::FileActionInherit,
                    errno,
                });
            }
        } else if let Err(errno) = syscalls.file_actions_add_dup2(source, target as RawFd) {
            return Err(MacOsPosixSpawnError::Syscall {
                operation: MacOsPosixSpawnOperation::FileActionDuplicate,
                errno,
            });
        }
    }
    Ok(())
}

fn add_node20_file_actions<S: MacOsPosixSpawnSyscalls>(
    syscalls: &mut S,
    stdio: Node20StdioFileImage,
) -> Result<(), MacOsPosixSpawnError> {
    let stdio_count = stdio.stdio_count;
    let mut child_fds = stdio.child_fds.map(|fd| fd.unwrap_or(-1));

    // Port libuv's low-fd preduplication before any target descriptor moves.
    for target in 0..stdio_count {
        let source = child_fds[target];
        if source < 0 || source >= target as RawFd {
            continue;
        }
        let mut temporary = stdio_count as RawFd;
        loop {
            if child_fds.iter().all(|candidate| *candidate != temporary) {
                break;
            }
            temporary += 1;
        }
        node_mutating_action(
            syscalls.file_actions_add_dup2(source, temporary),
            MacOsPosixSpawnOperation::FileActionPreDuplicate,
        )?;
        child_fds[target] = temporary;
    }

    for (target, &source) in child_fds[..stdio_count].iter().enumerate() {
        if source < 0 {
            let flags = if target == 0 {
                libc::O_RDONLY
            } else {
                libc::O_RDWR
            };
            node_mutating_action(
                syscalls.file_actions_add_open(target as RawFd, DEV_NULL, flags, 0),
                MacOsPosixSpawnOperation::FileActionOpenNull,
            )?;
            continue;
        }

        if source == target as RawFd {
            node_mutating_action(
                syscalls.file_actions_add_inherit(source),
                MacOsPosixSpawnOperation::FileActionInherit,
            )?;
        } else {
            node_mutating_action(
                syscalls.file_actions_add_dup2(source, target as RawFd),
                MacOsPosixSpawnOperation::FileActionDuplicate,
            )?;
        }
        // libuv intentionally ignores this return value.
        syscalls.set_blocking(source);
    }

    for target in 0..stdio_count {
        let source = child_fds[target];
        if source < 3 || child_fds[..target].contains(&source) {
            continue;
        }
        node_mutating_action(
            syscalls.file_actions_add_close(source),
            MacOsPosixSpawnOperation::FileActionClose,
        )?;
    }
    Ok(())
}

fn node_mutating_action(
    result: Result<(), c_int>,
    operation: MacOsPosixSpawnOperation,
) -> Result<(), MacOsPosixSpawnError> {
    match result {
        Ok(()) => Ok(()),
        Err(errno) if errno == libc::ENOSYS => {
            Err(MacOsPosixSpawnError::UnexpectedEnosysAfterFileMutation { operation })
        }
        Err(errno) => Err(MacOsPosixSpawnError::Syscall { operation, errno }),
    }
}

fn node20_resolve_and_spawn<S: MacOsPosixSpawnSyscalls>(
    syscalls: &mut S,
    image: &UnixExecImage,
) -> Result<pid_t, c_int> {
    let file = image.file();
    if file.to_bytes().contains(&b'/') {
        return retry_node_posix_spawn(syscalls, file, image);
    }

    let environment = image
        .environment()
        .constructed_storage()
        .ok_or(libc::EINVAL)?;
    let path = environment
        .iter()
        .find_map(|entry| entry.to_bytes().strip_prefix(b"PATH="))
        .unwrap_or(DARWIN_DEFAULT_PATH);

    let file_bytes = file.to_bytes();
    if file_bytes.len() > DARWIN_NAME_MAX {
        return Err(libc::ENAMETOOLONG);
    }
    let path_measure = path.len().min(DARWIN_PATH_MAX - 1) + 1;
    let mut cursor = 0;
    let mut last_errno = -1;
    let mut saw_eacces = false;
    loop {
        let rest = &path[cursor..];
        let segment_len = rest
            .iter()
            .position(|byte| *byte == b':')
            .unwrap_or(rest.len());
        let at_end = segment_len == rest.len();

        if segment_len < path_measure {
            let mut candidate =
                Vec::with_capacity(segment_len + usize::from(segment_len > 0) + file_bytes.len());
            candidate.extend_from_slice(&rest[..segment_len]);
            if segment_len > 0 {
                candidate.push(b'/');
            }
            candidate.extend_from_slice(file_bytes);
            let candidate =
                CString::new(candidate).expect("Unix image and PATH entries are NUL-free");
            match retry_node_posix_spawn(syscalls, &candidate, image) {
                Ok(pid) => return Ok(pid),
                Err(libc::EACCES) => {
                    saw_eacces = true;
                    last_errno = libc::EACCES;
                }
                Err(errno @ (libc::ENOENT | libc::ENOTDIR)) => last_errno = errno,
                Err(errno) => return Err(errno),
            }
        }

        if at_end {
            break;
        }
        cursor += segment_len + 1;
    }

    if saw_eacces {
        Err(libc::EACCES)
    } else {
        Err(last_errno)
    }
}

fn retry_node_posix_spawn<S: MacOsPosixSpawnSyscalls>(
    syscalls: &mut S,
    file: &CStr,
    image: &UnixExecImage,
) -> Result<pid_t, c_int> {
    loop {
        match syscalls.spawn(MacOsSpawnFunction::Direct, file, image) {
            Err(libc::EINTR) => {}
            result => return result,
        }
    }
}

#[cfg(target_os = "macos")]
pub(super) fn spawn_node20_libuv146_darwin(
    image: &UnixExecImage,
    features: Node20MacOsSpawnFeatures,
    stdio: Node20StdioFileImage,
) -> Result<MacOsSpawnAttempt, MacOsPosixSpawnError> {
    let mut syscalls = darwin::DarwinPosixSpawnSyscalls::initialize();
    spawn_node20_libuv146_injected(&mut syscalls, image, features, stdio)
}

#[cfg(target_os = "macos")]
pub(super) fn spawn_rust193_darwin(
    image: &UnixExecImage,
    stdio: Rust193StdioFileImage,
) -> Result<MacOsSpawnAttempt, MacOsPosixSpawnError> {
    let mut syscalls = darwin::DarwinPosixSpawnSyscalls::initialize();
    spawn_rust193_injected(&mut syscalls, image, stdio)
}

#[cfg(target_os = "macos")]
pub(super) fn spawn_node20_libuv146_unregistered(
    admission: UnixSpawnAdmission<'_>,
    image: UnixExecImage,
    stdio_plan: UnixStdioPlan,
    features: Node20MacOsSpawnFeatures,
) -> Result<UnregisteredUnixSpawnedProcess, MacOsPosixSpawnError> {
    if stdio_plan.is_pty() {
        return Err(MacOsPosixSpawnError::Unix(
            UnixShadowSpawnError::PtyRequiresDedicatedRoute,
        ));
    }
    let features = features.with_ordinary_stdio(!stdio_plan.has_extra());
    let fork_guard = descriptor_fork_guard();
    let stdio = prepare_node20_socketpair_stdio(stdio_plan)?;
    let file_image = Node20StdioFileImage::from_plan(stdio_plan, stdio.all_child_raw_fds())?;
    let mut syscalls = darwin::DarwinPosixSpawnSyscalls::initialize();
    match spawn_node20_libuv146_injected(&mut syscalls, &image, features, file_image)? {
        MacOsSpawnAttempt::Spawned(pid) => {
            drop(fork_guard);
            Ok(UnregisteredUnixSpawnedProcess::from_posix_spawn(pid, stdio))
        }
        MacOsSpawnAttempt::ForkFallback(_) => {
            spawn_node20_fork_prepared(admission, image, stdio_plan, features, stdio, fork_guard)
        }
    }
}

#[cfg(target_os = "macos")]
pub(super) fn spawn_node20_pty_unregistered(
    admission: UnixSpawnAdmission<'_>,
    image: UnixExecImage,
    size: MacOsPtySize,
) -> Result<UnregisteredUnixSpawnedProcess, MacOsPosixSpawnError> {
    require_dialect(&image, ProcessImageDialect::Node20Libuv146)?;
    let fork_guard = descriptor_fork_guard();
    let stdio = prepare_node20_pty_stdio(size)?;
    spawn_node20_fork_prepared(
        admission,
        image,
        node20_pty_stdio_plan(),
        Node20MacOsSpawnFeatures::controlling_pty(),
        stdio,
        fork_guard,
    )
}

#[cfg(target_os = "macos")]
pub(super) fn spawn_rust193_unregistered(
    admission: UnixSpawnAdmission<'_>,
    image: UnixExecImage,
    stdio_plan: UnixStdioPlan,
) -> Result<UnregisteredUnixSpawnedProcess, MacOsPosixSpawnError> {
    let fork_guard = descriptor_fork_guard();
    let stdio = PreparedStdio::build_rust193(stdio_plan).map_err(MacOsPosixSpawnError::Unix)?;
    let file_image = Rust193StdioFileImage::from_plan(stdio_plan, stdio.all_child_raw_fds())?;
    let mut syscalls = darwin::DarwinPosixSpawnSyscalls::initialize();
    match spawn_rust193_injected(&mut syscalls, &image, file_image)? {
        MacOsSpawnAttempt::Spawned(pid) => {
            drop(fork_guard);
            Ok(UnregisteredUnixSpawnedProcess::from_posix_spawn(pid, stdio))
        }
        MacOsSpawnAttempt::ForkFallback(_) => {
            Rust193ForkFallbackShadowSpawner::spawn_unregistered_with_prepared_and_guard(
                admission,
                image,
                Rust193DirectPipeShadowPlan::rust193_fork_fallback(stdio_plan),
                stdio,
                fork_guard,
            )
            .map_err(MacOsPosixSpawnError::Unix)
        }
    }
}

#[cfg(target_os = "macos")]
fn descriptor_fork_guard() -> MutexGuard<'static, ()> {
    SHADOW_FORK_DESCRIPTOR_LOCK
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

#[cfg(target_os = "macos")]
fn prepare_node20_socketpair_stdio(
    plan: UnixStdioPlan,
) -> Result<PreparedStdio, MacOsPosixSpawnError> {
    let mut child: [Option<OwnedFd>; 3] = [None, None, None];
    let mut parent: [Option<OwnedFd>; 3] = [None, None, None];
    for (index, disposition) in plan.dispositions().into_iter().enumerate() {
        if disposition != UnixStdioDisposition::Pipe {
            continue;
        }
        let (parent_fd, child_fd) = node20_socketpair()?;
        parent[index] = Some(parent_fd);
        child[index] = Some(child_fd);
    }
    let mut extra = Vec::with_capacity(plan.extra().len());
    for requested in plan.extra() {
        let (parent_fd, child_fd) = node20_socketpair()?;
        restrict_extra_socket_direction(
            parent_fd.as_raw_fd(),
            child_fd.as_raw_fd(),
            requested.direction(),
        )?;
        extra.push(PreparedExtraStdio::new(
            requested.child_fd(),
            requested.direction(),
            child_fd,
            parent_fd,
        ));
    }
    Ok(PreparedStdio::from_owned_parts_with_extra(
        child, parent, extra,
    ))
}

#[cfg(target_os = "macos")]
fn restrict_extra_socket_direction(
    parent: RawFd,
    child: RawFd,
    direction: UnixExtraPipeDirection,
) -> Result<(), MacOsPosixSpawnError> {
    let (fd, how) = match direction {
        UnixExtraPipeDirection::ChildToHost => (parent, libc::SHUT_WR),
        UnixExtraPipeDirection::HostToChild => (child, libc::SHUT_WR),
        UnixExtraPipeDirection::Duplex => return Ok(()),
    };
    loop {
        if unsafe { libc::shutdown(fd, how) } == 0 {
            return Ok(());
        }
        let errno = raw_errno();
        if errno != libc::EINTR {
            return Err(MacOsPosixSpawnError::Os {
                operation: MacOsPosixSpawnOperation::SocketPair,
                errno,
            });
        }
    }
}

#[cfg(target_os = "macos")]
const fn node20_pty_stdio_plan() -> UnixStdioPlan {
    UnixStdioPlan::new(
        UnixStdioDisposition::Pipe,
        UnixStdioDisposition::Pipe,
        UnixStdioDisposition::Pipe,
    )
    .as_pty()
}

#[cfg(target_os = "macos")]
fn prepare_node20_pty_stdio(size: MacOsPtySize) -> Result<PreparedStdio, MacOsPosixSpawnError> {
    unsafe extern "C" {
        fn ptsname_r(fd: c_int, buffer: *mut libc::c_char, length: libc::size_t) -> c_int;
    }

    let master_raw = loop {
        let fd = unsafe { libc::posix_openpt(libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC) };
        if fd != -1 {
            break fd;
        }
        let errno = raw_errno();
        if errno != libc::EINTR {
            return Err(MacOsPosixSpawnError::Os {
                operation: MacOsPosixSpawnOperation::PtyOpenMaster,
                errno,
            });
        }
    };
    let master = unsafe { OwnedFd::from_raw_fd(master_raw) };

    retry_pty_minus_one(MacOsPosixSpawnOperation::PtyGrant, || unsafe {
        libc::grantpt(master.as_raw_fd())
    })?;
    retry_pty_minus_one(MacOsPosixSpawnOperation::PtyUnlock, || unsafe {
        libc::unlockpt(master.as_raw_fd())
    })?;

    let mut slave_name = [0 as libc::c_char; DARWIN_PATH_MAX];
    loop {
        let result = unsafe {
            ptsname_r(
                master.as_raw_fd(),
                slave_name.as_mut_ptr(),
                slave_name.len(),
            )
        };
        if result == 0 {
            break;
        }
        if result != libc::EINTR {
            return Err(MacOsPosixSpawnError::Os {
                operation: MacOsPosixSpawnOperation::PtyName,
                errno: result,
            });
        }
    }
    let slave_name = unsafe { CStr::from_ptr(slave_name.as_ptr()) };
    let slave_raw = loop {
        let fd = unsafe {
            libc::open(
                slave_name.as_ptr(),
                libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC,
            )
        };
        if fd != -1 {
            break fd;
        }
        let errno = raw_errno();
        if errno != libc::EINTR {
            return Err(MacOsPosixSpawnError::Os {
                operation: MacOsPosixSpawnOperation::PtyOpenSlave,
                errno,
            });
        }
    };
    let slave = unsafe { OwnedFd::from_raw_fd(slave_raw) };
    let window_size = libc::winsize {
        ws_row: size.rows,
        ws_col: size.columns,
        ws_xpixel: size.pixel_width,
        ws_ypixel: size.pixel_height,
    };
    retry_pty_minus_one(MacOsPosixSpawnOperation::PtyInitialWindowSize, || unsafe {
        libc::ioctl(
            slave.as_raw_fd(),
            libc::TIOCSWINSZ,
            std::ptr::addr_of!(window_size),
        )
    })?;

    // A process with closed parent stdio can receive 0/1/2 from the PTY calls.
    // Promote both control descriptors before cloning so the child remapper and
    // the error-pipe writer can never destroy one another.
    let master = move_pty_above_stdio(master)?;
    let slave = move_pty_above_stdio(slave)?;
    let child_stdout = duplicate_pty_descriptor(slave.as_raw_fd())?;
    let child_stderr = duplicate_pty_descriptor(slave.as_raw_fd())?;

    Ok(PreparedStdio::from_pty_parts(
        [Some(slave), Some(child_stdout), Some(child_stderr)],
        master,
    ))
}

#[cfg(target_os = "macos")]
fn retry_pty_minus_one(
    operation: MacOsPosixSpawnOperation,
    mut syscall: impl FnMut() -> c_int,
) -> Result<(), MacOsPosixSpawnError> {
    loop {
        if syscall() != -1 {
            return Ok(());
        }
        let errno = raw_errno();
        if errno != libc::EINTR {
            return Err(MacOsPosixSpawnError::Os { operation, errno });
        }
    }
}

#[cfg(target_os = "macos")]
fn move_pty_above_stdio(fd: OwnedFd) -> Result<OwnedFd, MacOsPosixSpawnError> {
    if fd.as_raw_fd() >= 3 {
        return Ok(fd);
    }
    let moved = duplicate_pty_descriptor(fd.as_raw_fd())?;
    drop(fd);
    Ok(moved)
}

#[cfg(target_os = "macos")]
fn duplicate_pty_descriptor(fd: RawFd) -> Result<OwnedFd, MacOsPosixSpawnError> {
    loop {
        let duplicated = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 3) };
        if duplicated != -1 {
            return Ok(unsafe { OwnedFd::from_raw_fd(duplicated) });
        }
        let errno = raw_errno();
        if errno != libc::EINTR {
            return Err(MacOsPosixSpawnError::Os {
                operation: MacOsPosixSpawnOperation::PtyDuplicate,
                errno,
            });
        }
    }
}

#[cfg(target_os = "macos")]
fn node20_socketpair() -> Result<(OwnedFd, OwnedFd), MacOsPosixSpawnError> {
    let mut raw = [-1; 2];
    if unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, raw.as_mut_ptr()) } == -1 {
        return Err(last_os_error(MacOsPosixSpawnOperation::SocketPair));
    }
    let parent = unsafe { OwnedFd::from_raw_fd(raw[0]) };
    let child = unsafe { OwnedFd::from_raw_fd(raw[1]) };
    set_cloexec_libuv146(parent.as_raw_fd())?;
    set_cloexec_libuv146(child.as_raw_fd())?;
    Ok((parent, child))
}

#[cfg(target_os = "macos")]
fn set_cloexec_libuv146(fd: RawFd) -> Result<(), MacOsPosixSpawnError> {
    loop {
        let result = unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) };
        if result == 0 {
            return Ok(());
        }
        let errno = raw_errno();
        if errno != libc::EINTR {
            return Err(MacOsPosixSpawnError::Os {
                operation: MacOsPosixSpawnOperation::CloseOnExec,
                errno,
            });
        }
    }
}

#[cfg(target_os = "macos")]
fn create_node_error_pipe(minimum_fd: RawFd) -> Result<(OwnedFd, OwnedFd), MacOsPosixSpawnError> {
    let mut raw = [-1; 2];
    if unsafe { libc::pipe(raw.as_mut_ptr()) } == -1 {
        return Err(last_os_error(MacOsPosixSpawnOperation::ErrorPipe));
    }
    let reader = unsafe { OwnedFd::from_raw_fd(raw[0]) };
    let writer = unsafe { OwnedFd::from_raw_fd(raw[1]) };
    set_cloexec_libuv146(reader.as_raw_fd())?;
    set_cloexec_libuv146(writer.as_raw_fd())?;
    Ok((
        move_above_runtime_stdio(reader, minimum_fd)?,
        move_above_runtime_stdio(writer, minimum_fd)?,
    ))
}

#[cfg(target_os = "macos")]
fn move_above_runtime_stdio(
    fd: OwnedFd,
    minimum_fd: RawFd,
) -> Result<OwnedFd, MacOsPosixSpawnError> {
    if fd.as_raw_fd() >= minimum_fd {
        return Ok(fd);
    }
    let moved = loop {
        let result = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_DUPFD_CLOEXEC, minimum_fd) };
        if result != -1 {
            break result;
        }
        let errno = raw_errno();
        if errno != libc::EINTR {
            return Err(MacOsPosixSpawnError::Os {
                operation: MacOsPosixSpawnOperation::CloseOnExec,
                errno,
            });
        }
    };
    Ok(unsafe { OwnedFd::from_raw_fd(moved) })
}

#[cfg(target_os = "macos")]
fn spawn_node20_fork_prepared(
    _admission: UnixSpawnAdmission<'_>,
    image: UnixExecImage,
    stdio_plan: UnixStdioPlan,
    features: Node20MacOsSpawnFeatures,
    mut stdio: PreparedStdio,
    fork_guard: MutexGuard<'_, ()>,
) -> Result<UnregisteredUnixSpawnedProcess, MacOsPosixSpawnError> {
    let (error_read, error_write) = create_node_error_pipe(stdio_plan.stdio_count() as RawFd)?;
    let child_fds = stdio.all_child_raw_fds();
    let mut blocked = std::mem::MaybeUninit::<libc::sigset_t>::uninit();
    let mut old_mask = std::mem::MaybeUninit::<libc::sigset_t>::uninit();
    unsafe {
        libc::sigfillset(blocked.as_mut_ptr());
        for signal in [
            libc::SIGKILL,
            libc::SIGSTOP,
            libc::SIGTRAP,
            libc::SIGSEGV,
            libc::SIGBUS,
            libc::SIGILL,
            libc::SIGSYS,
            libc::SIGABRT,
        ] {
            libc::sigdelset(blocked.as_mut_ptr(), signal);
        }
        if libc::pthread_sigmask(libc::SIG_BLOCK, blocked.as_ptr(), old_mask.as_mut_ptr()) != 0 {
            libc::abort();
        }
    }

    let pid = unsafe { libc::fork() };
    let fork_errno = (pid == -1).then(raw_errno);
    if pid == 0 {
        unsafe {
            node20_child_main(
                &image,
                stdio_plan,
                child_fds,
                features,
                error_read.as_raw_fd(),
                error_write.as_raw_fd(),
            )
        }
    }

    unsafe {
        if libc::pthread_sigmask(libc::SIG_SETMASK, old_mask.as_ptr(), std::ptr::null_mut()) != 0 {
            libc::abort();
        }
    }
    drop(fork_guard);
    drop(error_write);

    if pid == -1 {
        return Err(MacOsPosixSpawnError::Os {
            operation: MacOsPosixSpawnOperation::Fork,
            errno: fork_errno.unwrap_or_else(|| std::process::abort()),
        });
    }
    stdio.close_child_ends_for_parent();

    match read_node20_error_record(error_read.as_raw_fd()) {
        NodeErrorHandshake::Success => {
            drop(error_read);
            Ok(UnregisteredUnixSpawnedProcess::from_posix_spawn(pid, stdio))
        }
        NodeErrorHandshake::ChildError(errno) => {
            drop(error_read);
            drop(stdio);
            wait_exact_child_or_abort(pid);
            Err(MacOsPosixSpawnError::Os {
                operation: MacOsPosixSpawnOperation::ChildSetup,
                errno,
            })
        }
        NodeErrorHandshake::BrokenPipe => {
            drop(error_read);
            drop(stdio);
            wait_exact_child_or_abort(pid);
            Err(MacOsPosixSpawnError::Os {
                operation: MacOsPosixSpawnOperation::ChildSetup,
                errno: libc::EPIPE,
            })
        }
    }
}

#[cfg(target_os = "macos")]
enum NodeErrorHandshake {
    Success,
    ChildError(c_int),
    BrokenPipe,
}

#[cfg(target_os = "macos")]
fn read_node20_error_record(fd: RawFd) -> NodeErrorHandshake {
    let mut record: c_int = 0;
    loop {
        let read = unsafe {
            libc::read(
                fd,
                std::ptr::addr_of_mut!(record).cast(),
                std::mem::size_of::<c_int>(),
            )
        };
        if read == 0 {
            return NodeErrorHandshake::Success;
        }
        if read == std::mem::size_of::<c_int>() as isize && record < 0 {
            return NodeErrorHandshake::ChildError(-record);
        }
        if read == -1 {
            let errno = raw_errno();
            if errno == libc::EINTR {
                continue;
            }
            if errno == libc::EPIPE {
                return NodeErrorHandshake::BrokenPipe;
            }
        }
        unsafe { libc::abort() }
    }
}

#[cfg(target_os = "macos")]
unsafe fn node20_child_main(
    image: &UnixExecImage,
    stdio_plan: UnixStdioPlan,
    child_fds: [Option<RawFd>; MAX_UNIX_STDIO],
    features: Node20MacOsSpawnFeatures,
    error_read: RawFd,
    error_write: RawFd,
) -> ! {
    unsafe {
        libc::close(error_read);
    }

    for signal in 1..32 {
        if signal == libc::SIGKILL || signal == libc::SIGSTOP {
            continue;
        }
        if unsafe { libc::signal(signal, libc::SIG_DFL) } == libc::SIG_ERR {
            unsafe { node20_child_error(error_write, raw_errno()) }
        }
    }

    match features.topology {
        ProcessTopology::Inherit => {}
        ProcessTopology::UnixNewProcessGroup => {
            if unsafe { libc::setpgid(0, 0) } == -1 {
                unsafe { node20_child_error(error_write, raw_errno()) }
            }
        }
        ProcessTopology::UnixNewSession => unsafe {
            libc::setsid();
        },
        ProcessTopology::UnixControllingPty => {
            if unsafe { libc::setsid() } == -1 {
                unsafe { node20_child_error(error_write, raw_errno()) }
            }
        }
        ProcessTopology::WindowsDetached => unsafe {
            node20_child_error(error_write, libc::EINVAL)
        },
    }

    unsafe { node20_child_remap(stdio_plan, child_fds, error_write) };

    if features.topology == ProcessTopology::UnixControllingPty
        && unsafe { libc::ioctl(0, libc::TIOCSCTTY.into(), 0) } == -1
    {
        unsafe { node20_child_error(error_write, raw_errno()) }
    }

    if let Some(cwd) = image.cwd() {
        if unsafe { libc::chdir(cwd.as_ptr()) } == -1 {
            unsafe { node20_child_error(error_write, raw_errno()) }
        }
    }

    if features.set_uid.is_some() || features.set_gid.is_some() {
        let saved_errno = raw_errno();
        unsafe {
            libc::setgroups(0, std::ptr::null());
            set_raw_errno(saved_errno);
        }
    }
    if let Some(gid) = features.set_gid {
        if unsafe { libc::setgid(gid) } == -1 {
            unsafe { node20_child_error(error_write, raw_errno()) }
        }
    }
    if let Some(uid) = features.set_uid {
        if unsafe { libc::setuid(uid) } == -1 {
            unsafe { node20_child_error(error_write, raw_errno()) }
        }
    }

    let UnixEnvironmentImage::Constructed { envp_with_null, .. } = image.environment() else {
        unsafe { node20_child_error(error_write, libc::EINVAL) }
    };
    unsafe { set_node_child_environ(envp_with_null.as_ptr().cast_mut().cast()) };

    let mut empty_mask = std::mem::MaybeUninit::<libc::sigset_t>::uninit();
    unsafe {
        libc::sigemptyset(empty_mask.as_mut_ptr());
        if libc::sigprocmask(libc::SIG_SETMASK, empty_mask.as_ptr(), std::ptr::null_mut()) != 0 {
            libc::abort();
        }
        libc::execvp(image.file().as_ptr(), image.argv_with_null().as_ptr());
        node20_child_error(error_write, raw_errno())
    }
}

#[cfg(target_os = "macos")]
unsafe fn node20_child_remap(
    plan: UnixStdioPlan,
    child_fds: [Option<RawFd>; MAX_UNIX_STDIO],
    error_write: RawFd,
) {
    let mut fds = child_fds.map(|fd| fd.unwrap_or(-1));
    for (target, source) in fds[..plan.stdio_count()].iter_mut().enumerate() {
        if *source < 0 || *source >= target as RawFd {
            continue;
        }
        let moved =
            unsafe { libc::fcntl(*source, libc::F_DUPFD_CLOEXEC, plan.stdio_count() as RawFd) };
        if moved == -1 {
            unsafe { node20_child_error(error_write, raw_errno()) }
        }
        *source = moved;
    }

    for (target, &mapped_source) in fds[..plan.stdio_count()].iter().enumerate() {
        let disposition = plan
            .disposition_at(target)
            .unwrap_or_else(|| std::process::abort());
        let mut source = mapped_source;
        let mut close_fd = -1;
        if disposition == UnixStdioDisposition::Null {
            unsafe {
                libc::close(target as RawFd);
            }
            source = unsafe {
                libc::open(
                    c"/dev/null".as_ptr(),
                    if target == 0 {
                        libc::O_RDONLY
                    } else {
                        libc::O_RDWR
                    },
                )
            };
            close_fd = source;
            if source == -1 {
                unsafe { node20_child_error(error_write, raw_errno()) }
            }
        }

        if source == target as RawFd {
            if close_fd == -1 && unsafe { child_set_cloexec(source, false) } == -1 {
                unsafe { node20_child_error(error_write, raw_errno()) }
            }
        } else if unsafe { libc::dup2(source, target as RawFd) } == -1 {
            unsafe { node20_child_error(error_write, raw_errno()) }
        }

        if target <= 2 && close_fd == -1 {
            unsafe {
                child_set_blocking_ignored(source);
            }
        }
        if close_fd >= 3 {
            unsafe {
                libc::close(close_fd);
            }
        }
    }
}

#[cfg(target_os = "macos")]
unsafe fn child_set_cloexec(fd: RawFd, set: bool) -> c_int {
    loop {
        let result =
            unsafe { libc::fcntl(fd, libc::F_SETFD, if set { libc::FD_CLOEXEC } else { 0 }) };
        if result != -1 || raw_errno() != libc::EINTR {
            return result;
        }
    }
}

#[cfg(target_os = "macos")]
unsafe fn child_set_blocking_ignored(fd: RawFd) {
    let flags = loop {
        let result = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        if result != -1 || raw_errno() != libc::EINTR {
            break result;
        }
    };
    if flags == -1 || flags & libc::O_NONBLOCK == 0 {
        return;
    }
    loop {
        let result = unsafe { libc::fcntl(fd, libc::F_SETFL, flags & !libc::O_NONBLOCK) };
        if result != -1 || raw_errno() != libc::EINTR {
            return;
        }
    }
}

#[cfg(target_os = "macos")]
unsafe fn node20_child_error(error_write: RawFd, errno: c_int) -> ! {
    let error = -errno;
    loop {
        let written = unsafe {
            libc::write(
                error_write,
                std::ptr::addr_of!(error).cast(),
                std::mem::size_of::<c_int>(),
            )
        };
        if written != -1 || raw_errno() != libc::EINTR {
            unsafe { libc::_exit(127) }
        }
    }
}

#[cfg(target_os = "macos")]
unsafe fn set_node_child_environ(environment: *mut *mut libc::c_char) {
    unsafe extern "C" {
        fn _NSGetEnviron() -> *mut *mut *mut libc::c_char;
    }
    unsafe {
        *_NSGetEnviron() = environment;
    }
}

#[cfg(target_os = "macos")]
fn wait_exact_child_or_abort(pid: pid_t) {
    let mut status = 0;
    loop {
        let waited = unsafe { libc::waitpid(pid, &mut status, 0) };
        if waited == pid {
            return;
        }
        if waited == -1 && raw_errno() == libc::EINTR {
            continue;
        }
        unsafe { libc::abort() }
    }
}

#[cfg(target_os = "macos")]
fn raw_errno() -> c_int {
    io::Error::last_os_error()
        .raw_os_error()
        .unwrap_or(libc::EINVAL)
}

#[cfg(target_os = "macos")]
unsafe fn set_raw_errno(errno: c_int) {
    unsafe extern "C" {
        fn __error() -> *mut c_int;
    }
    unsafe {
        *__error() = errno;
    }
}

#[cfg(target_os = "macos")]
fn last_os_error(operation: MacOsPosixSpawnOperation) -> MacOsPosixSpawnError {
    MacOsPosixSpawnError::Os {
        operation,
        errno: raw_errno(),
    }
}

pub(super) fn ordered_environment_bytes(
    image: &UnixExecImage,
) -> Result<Vec<Vec<u8>>, MacOsPosixSpawnError> {
    image
        .environment()
        .constructed_storage()
        .map(|storage| {
            storage
                .iter()
                .map(|entry| entry.to_bytes().to_vec())
                .collect()
        })
        .ok_or(MacOsPosixSpawnError::MissingNodeEnvironment)
}

pub(super) fn environment_pointer_image(
    environment: &UnixEnvironmentImage,
) -> Option<&[*const libc::c_char]> {
    environment.constructed_envp()
}

#[cfg(target_os = "macos")]
mod darwin {
    use std::ffi::{c_void, CStr};
    use std::mem::{self, MaybeUninit};
    use std::ptr;

    use libc::{c_char, c_int, c_short, pid_t};

    use super::{
        MacOsPosixSpawnSyscalls, MacOsSignalSet, MacOsSpawnFlags, MacOsSpawnFunction,
        UnixEnvironmentImage, UnixExecImage,
    };
    use crate::runtime_process::process_environment::with_process_environment_lock;
    use crate::runtime_process::ProcessImageDialect;

    const POSIX_SPAWN_SETSID_LIBUV146: c_int = 1024;
    const ADDCHDIR_NP: &CStr = c"posix_spawn_file_actions_addchdir_np";
    const ADDCHDIR_STANDARD: &CStr = c"posix_spawn_file_actions_addchdir";

    type AddChdir =
        unsafe extern "C" fn(*mut libc::posix_spawn_file_actions_t, *const c_char) -> c_int;

    unsafe extern "C" {
        fn posix_spawn_file_actions_addinherit_np(
            actions: *mut libc::posix_spawn_file_actions_t,
            fd: c_int,
        ) -> c_int;
        fn _NSGetEnviron() -> *mut *mut *mut c_char;
    }

    pub(super) struct DarwinPosixSpawnSyscalls {
        attrs: MaybeUninit<libc::posix_spawnattr_t>,
        actions: MaybeUninit<libc::posix_spawn_file_actions_t>,
        attrs_initialized: bool,
        actions_initialized: bool,
        node_addchdir: Option<AddChdir>,
        rust_addchdir: Option<AddChdir>,
        selected_addchdir: Option<AddChdir>,
    }

    impl DarwinPosixSpawnSyscalls {
        pub(super) fn initialize() -> Self {
            let node_addchdir = unsafe { lookup_function(ADDCHDIR_NP) };
            let rust_addchdir =
                node_addchdir.or_else(|| unsafe { lookup_function(ADDCHDIR_STANDARD) });
            Self {
                attrs: MaybeUninit::uninit(),
                actions: MaybeUninit::uninit(),
                attrs_initialized: false,
                actions_initialized: false,
                node_addchdir,
                rust_addchdir,
                selected_addchdir: None,
            }
        }

        fn attrs_mut(&mut self) -> *mut libc::posix_spawnattr_t {
            self.attrs.as_mut_ptr()
        }

        fn attrs_ptr(&self) -> *const libc::posix_spawnattr_t {
            self.attrs.as_ptr()
        }

        fn actions_mut(&mut self) -> *mut libc::posix_spawn_file_actions_t {
            self.actions.as_mut_ptr()
        }

        fn actions_ptr(&self) -> *const libc::posix_spawn_file_actions_t {
            self.actions.as_ptr()
        }

        fn spawn_unlocked(
            &self,
            function: MacOsSpawnFunction,
            file: &CStr,
            image: &UnixExecImage,
        ) -> Result<pid_t, c_int> {
            if !self.attrs_initialized || !self.actions_initialized {
                return Err(libc::EINVAL);
            }
            let environment = match image.environment() {
                UnixEnvironmentImage::NativeParentInherit => unsafe {
                    *_NSGetEnviron() as *const *mut c_char
                },
                UnixEnvironmentImage::Constructed { envp_with_null, .. } => {
                    envp_with_null.as_ptr().cast::<*mut c_char>()
                }
            };
            let mut pid = 0;
            let result = unsafe {
                match function {
                    MacOsSpawnFunction::Direct => libc::posix_spawn(
                        &mut pid,
                        file.as_ptr(),
                        self.actions_ptr(),
                        self.attrs_ptr(),
                        image.argv_with_null().as_ptr().cast::<*mut c_char>(),
                        environment,
                    ),
                    MacOsSpawnFunction::PathSearch => libc::posix_spawnp(
                        &mut pid,
                        file.as_ptr(),
                        self.actions_ptr(),
                        self.attrs_ptr(),
                        image.argv_with_null().as_ptr().cast::<*mut c_char>(),
                        environment,
                    ),
                }
            };
            if result == 0 {
                Ok(pid)
            } else {
                Err(result)
            }
        }
    }

    impl MacOsPosixSpawnSyscalls for DarwinPosixSpawnSyscalls {
        fn node_setsid_supported(&mut self) -> bool {
            darwin_kernel_major().is_some_and(|major| major >= 19)
        }

        fn node_addchdir_available(&mut self) -> bool {
            self.selected_addchdir = self.node_addchdir;
            self.selected_addchdir.is_some()
        }

        fn rust_addchdir_available(&mut self) -> bool {
            self.selected_addchdir = self.rust_addchdir;
            self.selected_addchdir.is_some()
        }

        fn attr_init(&mut self) -> Result<(), c_int> {
            let result = unsafe { libc::posix_spawnattr_init(self.attrs_mut()) };
            if result == 0 {
                self.attrs_initialized = true;
                Ok(())
            } else {
                Err(result)
            }
        }

        fn attr_destroy(&mut self) {
            if self.attrs_initialized {
                unsafe {
                    libc::posix_spawnattr_destroy(self.attrs_mut());
                }
                self.attrs_initialized = false;
            }
        }

        fn attr_set_flags(&mut self, flags: MacOsSpawnFlags) -> Result<(), c_int> {
            let mut native = 0;
            if flags.contains_private(MacOsSpawnFlags::CLOEXEC_DEFAULT) {
                native |= libc::POSIX_SPAWN_CLOEXEC_DEFAULT;
            }
            if flags.contains_private(MacOsSpawnFlags::SET_SIGDEFAULT) {
                native |= libc::POSIX_SPAWN_SETSIGDEF;
            }
            if flags.contains_private(MacOsSpawnFlags::SET_SIGMASK) {
                native |= libc::POSIX_SPAWN_SETSIGMASK;
            }
            if flags.contains_private(MacOsSpawnFlags::SET_SID) {
                native |= POSIX_SPAWN_SETSID_LIBUV146;
            }
            if flags.contains_private(MacOsSpawnFlags::SET_PGROUP) {
                native |= libc::POSIX_SPAWN_SETPGROUP;
            }
            zero_result(unsafe {
                libc::posix_spawnattr_setflags(self.attrs_mut(), native as c_short)
            })
        }

        fn attr_set_signal_default(&mut self, set: MacOsSignalSet) -> Result<(), c_int> {
            let mut native = MaybeUninit::<libc::sigset_t>::uninit();
            match set {
                MacOsSignalSet::Full => unsafe {
                    // libuv 1.46 deliberately does not branch on sigfillset.
                    libc::sigfillset(native.as_mut_ptr());
                },
                MacOsSignalSet::Rust193Default => unsafe {
                    minus_one_errno(libc::sigemptyset(native.as_mut_ptr()))?;
                    minus_one_errno(libc::sigaddset(native.as_mut_ptr(), libc::SIGPIPE))?;
                },
                MacOsSignalSet::Empty => unsafe {
                    minus_one_errno(libc::sigemptyset(native.as_mut_ptr()))?;
                },
            }
            zero_result(unsafe {
                libc::posix_spawnattr_setsigdefault(self.attrs_mut(), native.as_ptr())
            })
        }

        fn attr_set_signal_mask(&mut self, set: MacOsSignalSet) -> Result<(), c_int> {
            let mut native = MaybeUninit::<libc::sigset_t>::uninit();
            match set {
                MacOsSignalSet::Empty => unsafe {
                    // libuv 1.46 deliberately does not branch on sigemptyset.
                    libc::sigemptyset(native.as_mut_ptr());
                },
                MacOsSignalSet::Full => unsafe {
                    libc::sigfillset(native.as_mut_ptr());
                },
                MacOsSignalSet::Rust193Default => unsafe {
                    minus_one_errno(libc::sigemptyset(native.as_mut_ptr()))?;
                    minus_one_errno(libc::sigaddset(native.as_mut_ptr(), libc::SIGPIPE))?;
                },
            }
            zero_result(unsafe {
                libc::posix_spawnattr_setsigmask(self.attrs_mut(), native.as_ptr())
            })
        }

        fn attr_set_process_group(&mut self, process_group: pid_t) -> Result<(), c_int> {
            zero_result(unsafe { libc::posix_spawnattr_setpgroup(self.attrs_mut(), process_group) })
        }

        fn file_actions_init(&mut self) -> Result<(), c_int> {
            let result = unsafe { libc::posix_spawn_file_actions_init(self.actions_mut()) };
            if result == 0 {
                self.actions_initialized = true;
                Ok(())
            } else {
                Err(result)
            }
        }

        fn file_actions_destroy(&mut self) {
            if self.actions_initialized {
                unsafe {
                    libc::posix_spawn_file_actions_destroy(self.actions_mut());
                }
                self.actions_initialized = false;
            }
        }

        fn file_actions_add_chdir(&mut self, cwd: &CStr) -> Result<(), c_int> {
            let function = self.selected_addchdir.ok_or(libc::ENOSYS)?;
            zero_result(unsafe { function(self.actions_mut(), cwd.as_ptr()) })
        }

        fn file_actions_add_dup2(&mut self, source: c_int, target: c_int) -> Result<(), c_int> {
            zero_result(unsafe {
                libc::posix_spawn_file_actions_adddup2(self.actions_mut(), source, target)
            })
        }

        fn file_actions_add_open(
            &mut self,
            target: c_int,
            path: &CStr,
            flags: c_int,
            mode: libc::mode_t,
        ) -> Result<(), c_int> {
            zero_result(unsafe {
                libc::posix_spawn_file_actions_addopen(
                    self.actions_mut(),
                    target,
                    path.as_ptr(),
                    flags,
                    mode,
                )
            })
        }

        fn file_actions_add_inherit(&mut self, fd: c_int) -> Result<(), c_int> {
            zero_result(unsafe { posix_spawn_file_actions_addinherit_np(self.actions_mut(), fd) })
        }

        fn file_actions_add_close(&mut self, fd: c_int) -> Result<(), c_int> {
            zero_result(unsafe { libc::posix_spawn_file_actions_addclose(self.actions_mut(), fd) })
        }

        fn set_blocking(&mut self, fd: c_int) {
            let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
            if flags != -1 && flags & libc::O_NONBLOCK != 0 {
                unsafe {
                    libc::fcntl(fd, libc::F_SETFL, flags & !libc::O_NONBLOCK);
                }
            }
        }

        fn spawn(
            &mut self,
            function: MacOsSpawnFunction,
            file: &CStr,
            image: &UnixExecImage,
        ) -> Result<pid_t, c_int> {
            if image.image_dialect() == ProcessImageDialect::Rust193 {
                with_process_environment_lock(|| self.spawn_unlocked(function, file, image))
            } else {
                self.spawn_unlocked(function, file, image)
            }
        }
    }

    impl Drop for DarwinPosixSpawnSyscalls {
        fn drop(&mut self) {
            self.file_actions_destroy();
            self.attr_destroy();
        }
    }

    unsafe fn lookup_function(name: &CStr) -> Option<AddChdir> {
        let pointer = unsafe { libc::dlsym(libc::RTLD_DEFAULT, name.as_ptr()) };
        if pointer.is_null() {
            None
        } else {
            Some(unsafe { mem::transmute_copy::<*mut c_void, AddChdir>(&pointer) })
        }
    }

    fn darwin_kernel_major() -> Option<u32> {
        let mut mib = [libc::CTL_KERN, libc::KERN_OSRELEASE];
        let mut bytes = [0_u8; 256];
        let mut length = bytes.len();
        let result = unsafe {
            libc::sysctl(
                mib.as_mut_ptr(),
                mib.len() as u32,
                bytes.as_mut_ptr().cast(),
                &mut length,
                ptr::null_mut(),
                0,
            )
        };
        if result != 0 || length == 0 {
            return None;
        }
        let terminator = bytes[..length.min(bytes.len())]
            .iter()
            .position(|byte| *byte == 0)
            .unwrap_or(length.min(bytes.len()));
        let release = std::str::from_utf8(&bytes[..terminator]).ok()?;
        let mut fields = release.split('.');
        let major = fields.next()?.parse().ok()?;
        fields.next()?.parse::<u32>().ok()?;
        fields.next()?.parse::<u32>().ok()?;
        Some(major)
    }

    fn zero_result(result: c_int) -> Result<(), c_int> {
        if result == 0 {
            Ok(())
        } else {
            Err(result)
        }
    }

    fn minus_one_errno(result: c_int) -> Result<(), c_int> {
        if result == -1 {
            Err(std::io::Error::last_os_error()
                .raw_os_error()
                .unwrap_or(libc::EINVAL))
        } else {
            Ok(())
        }
    }
}

impl MacOsSpawnFlags {
    const fn contains_private(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
}
