use std::collections::VecDeque;
use std::ffi::CStr;
use std::os::fd::RawFd;

#[cfg(target_os = "macos")]
use std::fs;
#[cfg(target_os = "macos")]
use std::io::{Read, Write};
#[cfg(target_os = "macos")]
use std::os::unix::fs::PermissionsExt;
#[cfg(target_os = "macos")]
use std::time::{Duration, Instant};

use libc::{c_int, pid_t};

use super::macos_posix_spawn::{
    ordered_environment_bytes, select_node20_libuv146_route, select_rust193_route,
    spawn_node20_libuv146_injected, spawn_rust193_injected, spawn_rust193_test_features_injected,
    MacOsForkFallbackReason, MacOsPosixSpawnError, MacOsPosixSpawnOperation,
    MacOsPosixSpawnSyscalls, MacOsSignalSet, MacOsSpawnAttempt, MacOsSpawnFlags,
    MacOsSpawnFunction, Node20MacOsSpawnFeatures, Node20StdioFileImage, Rust193MacOsSpawnFeatures,
    Rust193ProgramKind, Rust193StdioFileImage,
};
#[cfg(target_os = "macos")]
use super::macos_posix_spawn::{spawn_node20_libuv146_darwin, spawn_rust193_darwin, MacOsPtySize};
use super::plan::{Rust193EnvironmentPlan, Rust193SpawnPreimage};
#[cfg(target_os = "macos")]
use super::reaper::{ReaperPollResult, RuntimeDropAction, SharedUnixReaper};
#[cfg(target_os = "macos")]
use super::spawn::{SharedRuntimeSpawner, SharedSpawnError, SharedSpawnPolicy};
use super::unix::UnixExecImage;
use super::unix_spawn::{
    ParentStdio, RuntimeExtraPipeEndpoint, RuntimePtyMaster, UnixExtraPipeDirection,
    UnixExtraStdioPlan, UnixShadowSpawnRoute, UnixStdioDisposition, UnixStdioPlan,
    UnixStdioPlanError,
};
use super::{
    NodeSpawnPlan, ProcessAtom, ProcessEnvironmentGuard, ProcessEnvironmentPlan, ProcessTopology,
    RuntimeEnv,
};
use crate::runtime_values::RuntimeString;

#[derive(Clone, Debug, Eq, PartialEq)]
enum Call {
    NodeSetSidCapability,
    NodeChdirCapability,
    RustChdirCapability,
    AttrInit,
    AttrDestroy,
    AttrFlags(MacOsSpawnFlags),
    AttrSignalDefault(MacOsSignalSet),
    AttrSignalMask(MacOsSignalSet),
    AttrProcessGroup(pid_t),
    ActionsInit,
    ActionsDestroy,
    Chdir(Vec<u8>),
    Dup2(RawFd, RawFd),
    Open(RawFd, Vec<u8>, c_int, libc::mode_t),
    Inherit(RawFd),
    Close(RawFd),
    SetBlocking(RawFd),
    Spawn(MacOsSpawnFunction, Vec<u8>, Vec<Vec<u8>>),
}

struct FakeSyscalls {
    calls: Vec<Call>,
    responses: VecDeque<Result<pid_t, c_int>>,
    fail_call: Option<(Call, c_int)>,
    node_setsid_supported: bool,
    node_chdir_available: bool,
    rust_chdir_available: bool,
}

impl Default for FakeSyscalls {
    fn default() -> Self {
        Self {
            calls: Vec::new(),
            responses: VecDeque::new(),
            fail_call: None,
            node_setsid_supported: true,
            node_chdir_available: true,
            rust_chdir_available: true,
        }
    }
}

impl FakeSyscalls {
    fn record(&mut self, call: Call) -> Result<(), c_int> {
        self.calls.push(call.clone());
        match &self.fail_call {
            Some((expected, errno)) if *expected == call => Err(*errno),
            _ => Ok(()),
        }
    }

    fn failing(call: Call, errno: c_int) -> Self {
        Self {
            fail_call: Some((call, errno)),
            ..Self::default()
        }
    }
}

impl MacOsPosixSpawnSyscalls for FakeSyscalls {
    fn node_setsid_supported(&mut self) -> bool {
        self.calls.push(Call::NodeSetSidCapability);
        self.node_setsid_supported
    }

    fn node_addchdir_available(&mut self) -> bool {
        self.calls.push(Call::NodeChdirCapability);
        self.node_chdir_available
    }

    fn rust_addchdir_available(&mut self) -> bool {
        self.calls.push(Call::RustChdirCapability);
        self.rust_chdir_available
    }

    fn attr_init(&mut self) -> Result<(), c_int> {
        self.record(Call::AttrInit)
    }

    fn attr_destroy(&mut self) {
        self.calls.push(Call::AttrDestroy);
    }

    fn attr_set_flags(&mut self, flags: MacOsSpawnFlags) -> Result<(), c_int> {
        self.record(Call::AttrFlags(flags))
    }

    fn attr_set_signal_default(&mut self, set: MacOsSignalSet) -> Result<(), c_int> {
        self.record(Call::AttrSignalDefault(set))
    }

    fn attr_set_signal_mask(&mut self, set: MacOsSignalSet) -> Result<(), c_int> {
        self.record(Call::AttrSignalMask(set))
    }

    fn attr_set_process_group(&mut self, process_group: pid_t) -> Result<(), c_int> {
        self.record(Call::AttrProcessGroup(process_group))
    }

    fn file_actions_init(&mut self) -> Result<(), c_int> {
        self.record(Call::ActionsInit)
    }

    fn file_actions_destroy(&mut self) {
        self.calls.push(Call::ActionsDestroy);
    }

    fn file_actions_add_chdir(&mut self, cwd: &CStr) -> Result<(), c_int> {
        self.record(Call::Chdir(cwd.to_bytes().to_vec()))
    }

    fn file_actions_add_dup2(&mut self, source: RawFd, target: RawFd) -> Result<(), c_int> {
        self.record(Call::Dup2(source, target))
    }

    fn file_actions_add_open(
        &mut self,
        target: RawFd,
        path: &CStr,
        flags: c_int,
        mode: libc::mode_t,
    ) -> Result<(), c_int> {
        self.record(Call::Open(target, path.to_bytes().to_vec(), flags, mode))
    }

    fn file_actions_add_inherit(&mut self, fd: RawFd) -> Result<(), c_int> {
        self.record(Call::Inherit(fd))
    }

    fn file_actions_add_close(&mut self, fd: RawFd) -> Result<(), c_int> {
        self.record(Call::Close(fd))
    }

    fn set_blocking(&mut self, fd: RawFd) {
        self.calls.push(Call::SetBlocking(fd));
    }

    fn spawn(
        &mut self,
        function: MacOsSpawnFunction,
        file: &CStr,
        image: &UnixExecImage,
    ) -> Result<pid_t, c_int> {
        self.calls.push(Call::Spawn(
            function,
            file.to_bytes().to_vec(),
            ordered_environment_bytes(image).unwrap_or_default(),
        ));
        self.responses.pop_front().unwrap_or(Ok(7001))
    }
}

fn runtime(value: impl Into<RuntimeString>) -> ProcessAtom {
    ProcessAtom::Runtime(value.into())
}

fn node_image(file: &str, cwd: Option<&str>, entries: &[(&str, &str)]) -> UnixExecImage {
    node_image_with_args(file, &[], cwd, entries)
}

fn node_image_with_args(
    file: &str,
    args: &[&str],
    cwd: Option<&str>,
    entries: &[(&str, &str)],
) -> UnixExecImage {
    let mut environment = RuntimeEnv::new();
    for (key, value) in entries {
        environment.insert(RuntimeString::from(*key), RuntimeString::from(*value));
    }
    let plan = NodeSpawnPlan::from_normalized_parts(
        runtime(file),
        runtime(file),
        args.iter().copied().map(runtime).collect(),
        cwd.map(runtime),
        ProcessEnvironmentPlan::NodeRuntime(environment),
        false,
        false,
        false,
    )
    .unwrap();
    UnixExecImage::build_node(&plan, ProcessEnvironmentGuard::None).unwrap()
}

fn rust_image(file: &str, cwd: Option<&str>, constructed: bool) -> UnixExecImage {
    let environment = if constructed {
        Rust193EnvironmentPlan::Overlay {
            mutations: vec![super::Rust193EnvMutation::RuntimeSet {
                key: RuntimeString::from("KLAUS_ORDER"),
                value: RuntimeString::from("rust"),
            }],
        }
    } else {
        Rust193EnvironmentPlan::NativeParentInherit
    };
    let preimage = Rust193SpawnPreimage::from_deferred_parts(
        runtime(file),
        runtime(file),
        Vec::new(),
        cwd.map(runtime),
        environment,
    );
    UnixExecImage::build_rust193_with_parent_snapshot(
        &preimage,
        ProcessEnvironmentGuard::None,
        || Ok(Vec::new()),
    )
    .unwrap()
}

fn rust_image_with_mutations(
    file: &str,
    mutations: Vec<super::Rust193EnvMutation>,
) -> UnixExecImage {
    let preimage = Rust193SpawnPreimage::from_deferred_parts(
        runtime(file),
        runtime(file),
        Vec::new(),
        None,
        Rust193EnvironmentPlan::Overlay { mutations },
    );
    UnixExecImage::build_rust193_with_parent_snapshot(
        &preimage,
        ProcessEnvironmentGuard::None,
        || Ok(vec![b"PATH=/parent/bin".to_vec()]),
    )
    .unwrap()
}

fn stdio_plan(
    stdin: UnixStdioDisposition,
    stdout: UnixStdioDisposition,
    stderr: UnixStdioDisposition,
) -> UnixStdioPlan {
    UnixStdioPlan::new(stdin, stdout, stderr)
}

#[test]
fn macos_node_and_rust_route_predicates_are_separate() {
    let node = Node20MacOsSpawnFeatures::ordinary(ProcessTopology::UnixNewSession);
    assert_eq!(
        select_node20_libuv146_route(node).unwrap(),
        UnixShadowSpawnRoute::Node20LibuvPosixSpawn,
    );

    let rust =
        Rust193MacOsSpawnFeatures::source_default().with_mutated_path_and_bare_program(true, true);
    assert_eq!(
        select_rust193_route(rust).unwrap(),
        UnixShadowSpawnRoute::Rust193ForkFallback,
    );
}

#[test]
fn rust_path_eligibility_is_frozen_from_the_image_not_a_caller_boolean() {
    for mutation in [
        super::Rust193EnvMutation::RuntimeSet {
            key: RuntimeString::from("PATH"),
            value: RuntimeString::from("/child/bin"),
        },
        super::Rust193EnvMutation::RuntimeRemove {
            key: RuntimeString::from("PATH"),
        },
    ] {
        let image = rust_image_with_mutations("bare-tool", vec![mutation]);
        let caller_claims_path_was_unchanged =
            Rust193MacOsSpawnFeatures::for_image(&image).unwrap();
        assert_eq!(
            select_rust193_route(caller_claims_path_was_unchanged),
            Ok(UnixShadowSpawnRoute::Rust193ForkFallback),
            "Rust 1.93 must never let a caller hide a PATH set/remove for a bare program",
        );
        let stdio = Rust193StdioFileImage::from_plan(
            stdio_plan(
                UnixStdioDisposition::Inherit,
                UnixStdioDisposition::Inherit,
                UnixStdioDisposition::Inherit,
            ),
            [None; 7],
        )
        .unwrap();
        let mut syscalls = FakeSyscalls::default();
        assert_eq!(
            spawn_rust193_injected(&mut syscalls, &image, stdio),
            Ok(MacOsSpawnAttempt::ForkFallback(
                MacOsForkFallbackReason::Rust193Eligibility,
            )),
        );
        assert!(
            syscalls.calls.is_empty(),
            "image-derived PATH fallback must happen before any posix_spawn syscall",
        );
    }
}

#[test]
fn macos_stdio_dialects_are_distinct_types_and_postspawn_failure_uses_shared_cleanup() {
    let backend = include_str!("macos_posix_spawn.rs");
    let reaper = include_str!("reaper.rs");
    assert!(backend.contains("struct Node20StdioFileImage"));
    assert!(backend.contains("struct Rust193StdioFileImage"));
    assert!(!backend.contains("struct MacOsStdioFileImage"));
    assert!(!backend.contains("hidden_kill_and_reap"));
    assert!(reaper.contains("cleanup_unpublished_unix"));
}

#[test]
fn parent_stdio_contract_is_typed_async_and_pty_is_not_three_fake_pipes() {
    let unix_spawn = include_str!("unix_spawn.rs");
    let shared_spawn = include_str!("spawn.rs");
    assert!(unix_spawn.contains("pub(super) enum ParentStdio"));
    assert!(unix_spawn.contains("Pty(RuntimePtyMaster)"));
    assert!(unix_spawn.contains("extra: Vec<RuntimeExtraPipe>"));
    assert!(unix_spawn.contains("pub(super) enum RuntimeExtraPipeEndpoint"));
    fn require_async_read<T: tokio::io::AsyncRead>() {}
    fn require_async_write<T: tokio::io::AsyncWrite>() {}
    require_async_read::<RuntimePtyMaster>();
    require_async_write::<RuntimePtyMaster>();
    assert!(!unix_spawn.contains("pub(super) struct ShadowParentStdio"));
    assert!(!unix_spawn.contains("Handle::try_current"));
    assert!(shared_spawn.contains("runtime: Option<Handle>"));
    assert_eq!(
        shared_spawn.matches("stdio.attach(self.runtime())").count(),
        shared_spawn.matches(".authorize_unix_spawn()").count(),
        "every admitted route attaches under the pre-spawn captured Tokio handle",
    );
}

#[test]
fn production_rust_spawn_entry_cannot_accept_caller_forged_features() {
    let backend = include_str!("macos_posix_spawn.rs");
    let production = backend
        .split("pub(super) fn spawn_rust193_darwin")
        .nth(1)
        .expect("production Darwin Rust entry")
        .split('{')
        .next()
        .expect("production Darwin Rust signature");
    assert!(!production.contains("Rust193MacOsSpawnFeatures"));
    assert!(production.contains("Rust193StdioFileImage"));
}

#[test]
fn extra_pipe_plan_rejects_swapped_gapped_overflow_and_pty_combinations() {
    let standard = stdio_plan(
        UnixStdioDisposition::Null,
        UnixStdioDisposition::Null,
        UnixStdioDisposition::Null,
    );
    assert_eq!(
        standard.with_extra(&[UnixExtraStdioPlan::new(
            4,
            UnixExtraPipeDirection::ChildToHost,
        )]),
        Err(UnixStdioPlanError::NonContiguousExtraFd {
            expected: 3,
            actual: 4,
        }),
    );
    assert_eq!(
        standard.with_extra(&[
            UnixExtraStdioPlan::new(3, UnixExtraPipeDirection::Duplex),
            UnixExtraStdioPlan::new(4, UnixExtraPipeDirection::Duplex),
            UnixExtraStdioPlan::new(5, UnixExtraPipeDirection::Duplex),
            UnixExtraStdioPlan::new(6, UnixExtraPipeDirection::Duplex),
            UnixExtraStdioPlan::new(7, UnixExtraPipeDirection::Duplex),
        ]),
        Err(UnixStdioPlanError::TooManyExtraPipes { actual: 5 }),
    );
    assert_eq!(
        standard
            .as_pty()
            .with_extra(&[UnixExtraStdioPlan::new(3, UnixExtraPipeDirection::Duplex,)]),
        Err(UnixStdioPlanError::PtyCannotHaveExtraPipes),
    );
}

#[test]
fn node_recipe_ports_libuv_flags_actions_path_and_ordered_envp() {
    let image = node_image(
        "klaus-tool",
        Some("relative-cwd"),
        &[("FIRST", "1"), ("PATH", "missing::relative"), ("LAST", "3")],
    );
    let plan = stdio_plan(
        UnixStdioDisposition::Pipe,
        UnixStdioDisposition::Inherit,
        UnixStdioDisposition::Null,
    );
    let stdio = Node20StdioFileImage::from_plan(plan, [Some(8), None, None]).unwrap();
    let mut syscalls = FakeSyscalls::default();
    syscalls
        .responses
        .extend([Err(libc::ENOENT), Err(libc::EACCES), Ok(8181)]);

    let result = spawn_node20_libuv146_injected(
        &mut syscalls,
        &image,
        Node20MacOsSpawnFeatures::ordinary(ProcessTopology::Inherit),
        stdio,
    )
    .unwrap();
    assert_eq!(result, MacOsSpawnAttempt::Spawned(8181));

    let flags = syscalls
        .calls
        .iter()
        .find_map(|call| match call {
            Call::AttrFlags(flags) => Some(*flags),
            _ => None,
        })
        .unwrap();
    assert!(flags.contains(MacOsSpawnFlags::CLOEXEC_DEFAULT));
    assert!(flags.contains(MacOsSpawnFlags::SET_SIGDEFAULT));
    assert!(flags.contains(MacOsSpawnFlags::SET_SIGMASK));
    assert!(!flags.contains(MacOsSpawnFlags::SET_SID));
    assert!(syscalls
        .calls
        .contains(&Call::AttrSignalDefault(MacOsSignalSet::Full)));
    assert!(syscalls
        .calls
        .contains(&Call::AttrSignalMask(MacOsSignalSet::Empty)));
    assert!(syscalls
        .calls
        .contains(&Call::Chdir(b"relative-cwd".to_vec())));
    assert!(syscalls.calls.contains(&Call::Dup2(8, 0)));
    assert!(syscalls.calls.contains(&Call::Inherit(1)));
    assert!(syscalls
        .calls
        .contains(&Call::Open(2, b"/dev/null".to_vec(), libc::O_RDWR, 0,)));
    assert!(syscalls.calls.contains(&Call::Close(8)));

    let spawns: Vec<_> = syscalls
        .calls
        .iter()
        .filter_map(|call| match call {
            Call::Spawn(function, candidate, environment) => {
                Some((*function, candidate.clone(), environment.clone()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        spawns
            .iter()
            .map(|(_, file, _)| file.clone())
            .collect::<Vec<_>>(),
        [
            b"missing/klaus-tool".to_vec(),
            b"klaus-tool".to_vec(),
            b"relative/klaus-tool".to_vec()
        ],
    );
    assert!(spawns
        .iter()
        .all(|(function, _, _)| *function == MacOsSpawnFunction::Direct));
    assert_eq!(
        spawns[0].2,
        [
            b"FIRST=1".to_vec(),
            b"PATH=missing::relative".to_vec(),
            b"LAST=3".to_vec(),
        ],
    );
    assert_eq!(
        syscalls.calls.last(),
        Some(&Call::AttrDestroy),
        "libuv destroys actions then attributes after spawn",
    );
}

#[test]
fn node_resolver_preserves_eacces_precedence_and_enoexec_is_terminal() {
    let image = node_image("tool", None, &[("PATH", "one:two")]);
    let stdio = Node20StdioFileImage::from_plan(
        stdio_plan(
            UnixStdioDisposition::Inherit,
            UnixStdioDisposition::Inherit,
            UnixStdioDisposition::Inherit,
        ),
        [None; 7],
    )
    .unwrap();
    let mut eacces = FakeSyscalls::default();
    eacces
        .responses
        .extend([Err(libc::EACCES), Err(libc::ENOENT)]);
    assert_eq!(
        spawn_node20_libuv146_injected(
            &mut eacces,
            &image,
            Node20MacOsSpawnFeatures::ordinary(ProcessTopology::Inherit),
            stdio,
        ),
        Err(MacOsPosixSpawnError::Syscall {
            operation: MacOsPosixSpawnOperation::Spawn,
            errno: libc::EACCES,
        }),
    );

    let mut enoexec = FakeSyscalls::default();
    enoexec.responses.extend([Err(libc::ENOEXEC), Ok(9911)]);
    assert_eq!(
        spawn_node20_libuv146_injected(
            &mut enoexec,
            &image,
            Node20MacOsSpawnFeatures::ordinary(ProcessTopology::Inherit),
            stdio,
        ),
        Err(MacOsPosixSpawnError::Syscall {
            operation: MacOsPosixSpawnOperation::Spawn,
            errno: libc::ENOEXEC,
        }),
    );
    assert_eq!(
        enoexec
            .calls
            .iter()
            .filter(|call| matches!(call, Call::Spawn(..)))
            .count(),
        1,
        "posix_spawn ENOEXEC must not invent execvp's /bin/sh fallback",
    );
}

#[test]
fn node_resolver_ports_empty_overlong_name_max_eintr_and_enotdir_boundaries() {
    let inherited = stdio_plan(
        UnixStdioDisposition::Inherit,
        UnixStdioDisposition::Inherit,
        UnixStdioDisposition::Inherit,
    );
    let stdio = Node20StdioFileImage::from_plan(inherited, [None; 7]).unwrap();

    let empty_path = node_image("tool", None, &[("PATH", "")]);
    let mut empty = FakeSyscalls::default();
    empty.responses.push_back(Ok(1001));
    assert_eq!(
        spawn_node20_libuv146_injected(
            &mut empty,
            &empty_path,
            Node20MacOsSpawnFeatures::ordinary(ProcessTopology::Inherit),
            stdio,
        ),
        Ok(MacOsSpawnAttempt::Spawned(1001)),
    );
    assert!(empty.calls.iter().any(|call| matches!(
        call,
        Call::Spawn(MacOsSpawnFunction::Direct, file, _) if file == b"tool"
    )));

    let overlong_segment = format!("{}:next:last", "x".repeat(1024));
    let overlong = node_image("tool", None, &[("PATH", &overlong_segment)]);
    let mut skipped = FakeSyscalls::default();
    skipped
        .responses
        .extend([Err(libc::EINTR), Err(libc::ENOTDIR), Ok(1002)]);
    assert_eq!(
        spawn_node20_libuv146_injected(
            &mut skipped,
            &overlong,
            Node20MacOsSpawnFeatures::ordinary(ProcessTopology::Inherit),
            stdio,
        ),
        Ok(MacOsSpawnAttempt::Spawned(1002)),
    );
    let candidates: Vec<_> = skipped
        .calls
        .iter()
        .filter_map(|call| match call {
            Call::Spawn(_, file, _) => Some(file.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        candidates,
        [
            b"next/tool".to_vec(),
            b"next/tool".to_vec(),
            b"last/tool".to_vec()
        ],
        "the >= PATH_MAX segment is skipped and EINTR retries the same candidate",
    );

    let max_name = "n".repeat(255);
    let max_image = node_image(&max_name, None, &[("PATH", "bin")]);
    let mut accepted = FakeSyscalls::default();
    accepted.responses.push_back(Ok(1003));
    assert_eq!(
        spawn_node20_libuv146_injected(
            &mut accepted,
            &max_image,
            Node20MacOsSpawnFeatures::ordinary(ProcessTopology::Inherit),
            stdio,
        ),
        Ok(MacOsSpawnAttempt::Spawned(1003)),
    );

    let too_long_name = "n".repeat(256);
    let too_long = node_image(&too_long_name, None, &[("PATH", "bin")]);
    let mut rejected = FakeSyscalls::default();
    assert_eq!(
        spawn_node20_libuv146_injected(
            &mut rejected,
            &too_long,
            Node20MacOsSpawnFeatures::ordinary(ProcessTopology::Inherit),
            stdio,
        ),
        Err(MacOsPosixSpawnError::Syscall {
            operation: MacOsPosixSpawnOperation::Spawn,
            errno: libc::ENAMETOOLONG,
        }),
    );
    assert!(!rejected
        .calls
        .iter()
        .any(|call| matches!(call, Call::Spawn(..))));
}

#[test]
fn node_enosys_fallback_is_limited_to_pre_mutation_libuv_stages() {
    let image = node_image("/bin/true", Some("/tmp"), &[]);
    let stdio = Node20StdioFileImage::from_plan(
        stdio_plan(
            UnixStdioDisposition::Inherit,
            UnixStdioDisposition::Inherit,
            UnixStdioDisposition::Inherit,
        ),
        [None; 7],
    )
    .unwrap();
    let stages = [
        Call::AttrInit,
        Call::AttrFlags(
            MacOsSpawnFlags::CLOEXEC_DEFAULT
                .union_for_test(MacOsSpawnFlags::SET_SIGDEFAULT)
                .union_for_test(MacOsSpawnFlags::SET_SIGMASK),
        ),
        Call::AttrSignalDefault(MacOsSignalSet::Full),
        Call::AttrSignalMask(MacOsSignalSet::Empty),
        Call::ActionsInit,
        Call::Chdir(b"/tmp".to_vec()),
    ];
    for stage in stages {
        let mut syscalls = FakeSyscalls::failing(stage.clone(), libc::ENOSYS);
        let result = spawn_node20_libuv146_injected(
            &mut syscalls,
            &image,
            Node20MacOsSpawnFeatures::ordinary(ProcessTopology::Inherit),
            stdio,
        )
        .unwrap();
        assert!(matches!(
            result,
            MacOsSpawnAttempt::ForkFallback(MacOsForkFallbackReason::NodeAttributeEnosys(_))
        ));
    }

    let mut mutated = FakeSyscalls::failing(Call::Inherit(0), libc::ENOSYS);
    assert_eq!(
        spawn_node20_libuv146_injected(
            &mut mutated,
            &image,
            Node20MacOsSpawnFeatures::ordinary(ProcessTopology::Inherit),
            stdio,
        ),
        Err(MacOsPosixSpawnError::UnexpectedEnosysAfterFileMutation {
            operation: MacOsPosixSpawnOperation::FileActionInherit,
        }),
    );
}

#[test]
fn node_setsid_and_credential_enosys_paths_fallback_but_topologies_route_first() {
    let image = node_image("/bin/true", None, &[]);
    let stdio = Node20StdioFileImage::from_plan(
        stdio_plan(
            UnixStdioDisposition::Inherit,
            UnixStdioDisposition::Inherit,
            UnixStdioDisposition::Inherit,
        ),
        [None; 7],
    )
    .unwrap();
    let mut no_setsid = FakeSyscalls {
        node_setsid_supported: false,
        ..FakeSyscalls::default()
    };
    assert_eq!(
        spawn_node20_libuv146_injected(
            &mut no_setsid,
            &image,
            Node20MacOsSpawnFeatures::ordinary(ProcessTopology::UnixNewSession),
            stdio,
        ),
        Ok(MacOsSpawnAttempt::ForkFallback(
            MacOsForkFallbackReason::NodeSetSidUnavailable,
        )),
    );

    let mut credentials = FakeSyscalls::default();
    assert_eq!(
        spawn_node20_libuv146_injected(
            &mut credentials,
            &image,
            Node20MacOsSpawnFeatures::ordinary(ProcessTopology::Inherit)
                .with_credentials(true, false),
            stdio,
        ),
        Ok(MacOsSpawnAttempt::ForkFallback(
            MacOsForkFallbackReason::NodeCredentials,
        )),
    );

    for topology in [
        ProcessTopology::UnixNewProcessGroup,
        ProcessTopology::UnixControllingPty,
    ] {
        let mut routed = FakeSyscalls::default();
        assert_eq!(
            spawn_node20_libuv146_injected(
                &mut routed,
                &image,
                Node20MacOsSpawnFeatures::ordinary(topology),
                stdio,
            ),
            Ok(MacOsSpawnAttempt::ForkFallback(
                MacOsForkFallbackReason::NodeTopology,
            )),
        );
        assert!(routed.calls.is_empty(), "topology is selected before attrs");
    }
}

#[test]
fn rust_recipe_uses_only_configured_dup_actions_inherited_mask_and_sigpipe_default() {
    let image = rust_image("/bin/true", Some("/tmp"), true);
    let stdio = Rust193StdioFileImage::from_plan(
        stdio_plan(
            UnixStdioDisposition::Pipe,
            UnixStdioDisposition::Inherit,
            UnixStdioDisposition::Null,
        ),
        [Some(11), None, Some(12), None, None, None, None],
    )
    .unwrap();
    let features = Rust193MacOsSpawnFeatures::for_image(&image)
        .unwrap()
        .with_process_group(Some(44));
    let mut syscalls = FakeSyscalls::default();
    syscalls.responses.push_back(Ok(9191));

    assert_eq!(
        spawn_rust193_test_features_injected(&mut syscalls, &image, features, stdio),
        Ok(MacOsSpawnAttempt::Spawned(9191)),
    );
    assert!(syscalls.calls.contains(&Call::Dup2(11, 0)));
    assert!(syscalls.calls.contains(&Call::Dup2(12, 2)));
    assert!(!syscalls
        .calls
        .iter()
        .any(|call| matches!(call, Call::Inherit(_))));
    assert!(syscalls.calls.contains(&Call::Chdir(b"/tmp".to_vec())));
    assert!(syscalls.calls.contains(&Call::AttrProcessGroup(44)));
    assert!(syscalls
        .calls
        .contains(&Call::AttrSignalDefault(MacOsSignalSet::Rust193Default)));
    assert!(!syscalls
        .calls
        .iter()
        .any(|call| matches!(call, Call::AttrSignalMask(_))));
    let flags = syscalls
        .calls
        .iter()
        .find_map(|call| match call {
            Call::AttrFlags(flags) => Some(*flags),
            _ => None,
        })
        .unwrap();
    assert!(flags.contains(MacOsSpawnFlags::SET_SIGDEFAULT));
    assert!(flags.contains(MacOsSpawnFlags::SET_PGROUP));
    assert!(!flags.contains(MacOsSpawnFlags::CLOEXEC_DEFAULT));
    assert!(!flags.contains(MacOsSpawnFlags::SET_SIGMASK));
    assert_eq!(
        syscalls
            .calls
            .iter()
            .filter_map(|call| match call {
                Call::Spawn(function, file, _) => Some((*function, file.clone())),
                _ => None,
            })
            .collect::<Vec<_>>(),
        [(MacOsSpawnFunction::PathSearch, b"/bin/true".to_vec())],
    );
}

#[test]
fn rust_full_eligibility_and_late_setsid_fallback_never_enter_libuv_recipe() {
    let restrictions = [
        Rust193MacOsSpawnFeatures::source_default()
            .with_source_restrictions(true, false, false, false, false),
        Rust193MacOsSpawnFeatures::source_default()
            .with_source_restrictions(false, true, false, false, false),
        Rust193MacOsSpawnFeatures::source_default()
            .with_source_restrictions(false, false, true, false, false),
        Rust193MacOsSpawnFeatures::source_default()
            .with_source_restrictions(false, false, false, true, false),
        Rust193MacOsSpawnFeatures::source_default()
            .with_source_restrictions(false, false, false, false, true),
        Rust193MacOsSpawnFeatures::source_default().with_mutated_path_and_bare_program(true, true),
        Rust193MacOsSpawnFeatures::source_default()
            .with_cwd_and_program(true, Rust193ProgramKind::Relative),
    ];
    for features in restrictions {
        assert_eq!(
            select_rust193_route(features),
            Ok(UnixShadowSpawnRoute::Rust193ForkFallback),
        );
    }
    for eligible in [
        Rust193MacOsSpawnFeatures::source_default()
            .with_cwd_and_program(true, Rust193ProgramKind::PathLookup),
        Rust193MacOsSpawnFeatures::source_default()
            .with_cwd_and_program(true, Rust193ProgramKind::Absolute),
        Rust193MacOsSpawnFeatures::source_default().with_mutated_path_and_bare_program(true, false),
    ] {
        assert_eq!(
            select_rust193_route(eligible),
            Ok(UnixShadowSpawnRoute::Rust193PosixSpawn),
            "Apple falls back only for cwd+relative and PATH mutation+bare program",
        );
    }
    assert!(select_rust193_route(
        Rust193MacOsSpawnFeatures::source_default().with_create_pidfd(true)
    )
    .is_err());

    let image = rust_image("/bin/true", None, false);
    let stdio = Rust193StdioFileImage::from_plan(
        stdio_plan(
            UnixStdioDisposition::Inherit,
            UnixStdioDisposition::Inherit,
            UnixStdioDisposition::Inherit,
        ),
        [None; 7],
    )
    .unwrap();
    let mut syscalls = FakeSyscalls::default();
    assert_eq!(
        spawn_rust193_test_features_injected(
            &mut syscalls,
            &image,
            Rust193MacOsSpawnFeatures::for_image(&image)
                .unwrap()
                .with_setsid(true),
            stdio,
        ),
        Ok(MacOsSpawnAttempt::ForkFallback(
            MacOsForkFallbackReason::Rust193SetSidUnsupported,
        )),
    );
    assert!(syscalls
        .calls
        .contains(&Call::AttrSignalDefault(MacOsSignalSet::Rust193Default)));
    assert_eq!(
        syscalls.calls[syscalls.calls.len() - 2..],
        [Call::ActionsDestroy, Call::AttrDestroy],
        "Rust reaches its source-positioned setsid fallback after attrs/actions",
    );
}

#[test]
fn dialects_cannot_cross_into_each_others_posix_spawn_recipe() {
    let node = node_image("/bin/true", None, &[]);
    let rust = rust_image("/bin/true", None, false);
    let plan = stdio_plan(
        UnixStdioDisposition::Inherit,
        UnixStdioDisposition::Inherit,
        UnixStdioDisposition::Inherit,
    );
    let node_stdio = Node20StdioFileImage::from_plan(plan, [None; 7]).unwrap();
    let rust_stdio = Rust193StdioFileImage::from_plan(plan, [None; 7]).unwrap();
    let mut syscalls = FakeSyscalls::default();
    assert!(matches!(
        spawn_node20_libuv146_injected(
            &mut syscalls,
            &rust,
            Node20MacOsSpawnFeatures::ordinary(ProcessTopology::Inherit),
            node_stdio,
        ),
        Err(MacOsPosixSpawnError::ImageDialectMismatch { .. })
    ));
    assert!(matches!(
        spawn_rust193_injected(&mut syscalls, &node, rust_stdio),
        Err(MacOsPosixSpawnError::ImageDialectMismatch { .. })
    ));
    assert!(syscalls.calls.is_empty());
}

#[test]
fn backend_source_has_no_standard_or_tokio_command_or_shared_approximate_resolver() {
    let source = include_str!("macos_posix_spawn.rs");
    for forbidden in [
        ["std", "process", "Command"].join("::"),
        ["tokio", "process", "Command"].join("::"),
        "which::".to_owned(),
    ] {
        assert!(
            !source.contains(&forbidden),
            "forbidden delegation: {forbidden}"
        );
    }
    assert!(source.contains("DARWIN_DEFAULT_PATH"));
    assert!(source.contains("retry_node_posix_spawn"));
    assert!(!source.contains("tcgetattr"));
    assert!(!source.contains("tcsetattr"));
}

#[cfg(target_os = "macos")]
#[test]
fn darwin_real_layer_spawns_node_and_both_rust_environment_pointer_images() {
    let inherited = stdio_plan(
        UnixStdioDisposition::Inherit,
        UnixStdioDisposition::Inherit,
        UnixStdioDisposition::Inherit,
    );
    let node = node_image("/usr/bin/true", None, &[("ORDER", "node")]);
    let node_stdio = Node20StdioFileImage::from_plan(inherited, [None; 7]).unwrap();
    let node_pid = match spawn_node20_libuv146_darwin(
        &node,
        Node20MacOsSpawnFeatures::ordinary(ProcessTopology::Inherit),
        node_stdio,
    )
    .unwrap()
    {
        MacOsSpawnAttempt::Spawned(pid) => pid,
        fallback => panic!("unexpected Node fallback: {fallback:?}"),
    };
    wait_success(node_pid);

    for constructed in [false, true] {
        let rust = rust_image("/usr/bin/true", None, constructed);
        let rust_stdio = Rust193StdioFileImage::from_plan(inherited, [None; 7]).unwrap();
        let rust_pid = match spawn_rust193_darwin(&rust, rust_stdio).unwrap() {
            MacOsSpawnAttempt::Spawned(pid) => pid,
            fallback => panic!("unexpected Rust fallback: {fallback:?}"),
        };
        wait_success(rust_pid);
    }
}

#[cfg(target_os = "macos")]
fn wait_success(pid: pid_t) {
    let mut status = 0;
    let waited = unsafe { libc::waitpid(pid, &mut status, 0) };
    assert_eq!(waited, pid);
    assert!(libc::WIFEXITED(status));
    assert_eq!(libc::WEXITSTATUS(status), 0);
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn darwin_node_posix_spawn_and_owned_group_fork_share_the_project_reaper() {
    let reaper = SharedUnixReaper::ready_with_libc_for_test();
    let spawner = SharedRuntimeSpawner::with_reaper_for_test(reaper.clone());

    let posix = spawner
        .spawn_node20_macos_shadow(
            node_image("/usr/bin/true", None, &[("ORDER", "posix")]),
            stdio_plan(
                UnixStdioDisposition::Inherit,
                UnixStdioDisposition::Inherit,
                UnixStdioDisposition::Inherit,
            ),
            SharedSpawnPolicy::node20_macos(ProcessTopology::Inherit, RuntimeDropAction::KeepAlive),
        )
        .await
        .unwrap();
    let (mut posix_child, posix_stdio) = posix.into_parts();
    assert!(posix_stdio.is_null_or_inherited());
    wait_shared_child(&reaper, &mut posix_child, 0);

    let forked = spawner
        .spawn_node20_macos_shadow(
            node_image("/bin/cat", None, &[("ORDER", "fork")]),
            stdio_plan(
                UnixStdioDisposition::Pipe,
                UnixStdioDisposition::Pipe,
                UnixStdioDisposition::Null,
            ),
            SharedSpawnPolicy::node20_macos(
                ProcessTopology::UnixNewProcessGroup,
                RuntimeDropAction::KeepAlive,
            ),
        )
        .await
        .unwrap();
    let (mut forked_child, forked_stdio) = forked.into_parts();
    let pid = forked_child.identity_for_test().pid_for_test();
    assert_eq!(unsafe { libc::getpgid(pid) }, pid, "checked setpgid(0, 0)");
    let (stdin, stdout, stderr) = forked_stdio.into_standard_pipes();
    let mut stdin = stdin.unwrap();
    let mut stdout = stdout.unwrap();
    assert!(stderr.is_none());
    write_nonblocking(&mut stdin, b"node-owned-fork\n");
    drop(stdin);
    assert_eq!(read_nonblocking_to_eof(&mut stdout), b"node-owned-fork\n");
    wait_shared_child(&reaper, &mut forked_child, 0);
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn darwin_node_owned_pty_fork_has_one_master_checked_session_and_shared_reaper() {
    if !crate::runtime_process::pty_allocatable() {
        eprintln!("skipping pty test: this environment cannot allocate a pty");
        return;
    }
    let reaper = SharedUnixReaper::ready_with_libc_for_test();
    let spawner = SharedRuntimeSpawner::with_reaper_for_test(reaper.clone());
    let spawned = spawner
        .spawn_node20_macos_pty_shadow(
            node_image_with_args(
                "/bin/sh",
                &[
                    "-c",
                    "test -t 0 && test -t 1 && test -t 2 && [ \"$(ps -o tpgid= -p $$ | tr -d ' ')\" = \"$$\" ] && IFS= read -r marker && stty size",
                ],
                None,
                &[("PATH", "/usr/bin:/bin")],
            ),
            MacOsPtySize::new(31, 97),
            SharedSpawnPolicy::node20_macos(
                ProcessTopology::UnixControllingPty,
                RuntimeDropAction::KeepAlive,
            ),
        )
        .await
        .unwrap();
    let (mut child, stdio) = spawned.into_parts();
    let pid = child.identity_for_test().pid_for_test();
    assert_eq!(
        unsafe { libc::getsid(pid) },
        pid,
        "checked setsid() session"
    );
    assert_eq!(
        unsafe { libc::getpgid(pid) },
        pid,
        "session leader process group"
    );

    let ParentStdio::Pty(mut master) = stdio else {
        panic!("controlling PTY must publish exactly one duplex master")
    };
    assert_eq!(unsafe { libc::isatty(master.raw_fd_for_test()) }, 1);
    assert!(master.is_close_on_exec());
    assert!(master.is_nonblocking());

    let mut initial = unsafe { std::mem::zeroed::<libc::winsize>() };
    assert_eq!(
        unsafe {
            libc::ioctl(
                master.raw_fd_for_test(),
                libc::TIOCGWINSZ,
                std::ptr::addr_of_mut!(initial),
            )
        },
        0,
    );
    assert_eq!((initial.ws_row, initial.ws_col), (31, 97));

    master.resize(41, 111, 0, 0).unwrap();
    write_nonblocking(&mut master, b"continue\n");
    let output = read_nonblocking_to_eof(&mut master);
    assert!(
        output
            .windows(b"41 111\r\n".len())
            .any(|window| window == b"41 111\r\n"),
        "default terminal output processing and master resize: {output:?}",
    );
    wait_shared_child(&reaper, &mut child, 0);
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn darwin_node_extra_pipes_preserve_fd_index_direction_order_and_async_attachment() {
    let reaper = SharedUnixReaper::ready_with_libc_for_test();
    let spawner = SharedRuntimeSpawner::with_reaper_for_test(reaper.clone());
    let stdio_plan = stdio_plan(
        UnixStdioDisposition::Null,
        UnixStdioDisposition::Pipe,
        UnixStdioDisposition::Null,
    )
    .with_extra(&[
        UnixExtraStdioPlan::new(3, UnixExtraPipeDirection::ChildToHost),
        UnixExtraStdioPlan::new(4, UnixExtraPipeDirection::HostToChild),
        UnixExtraStdioPlan::new(5, UnixExtraPipeDirection::Duplex),
    ])
    .unwrap();
    let spawned = spawner
        .spawn_node20_macos_shadow(
            node_image_with_args(
                "/bin/sh",
                &[
                    "-c",
                    "printf 'child-three\\n' >&3; IFS= read -r four <&4; IFS= read -r five <&5; printf 'duplex:%s\\n' \"$five\" >&5; printf 'stdout:%s\\n' \"$four\"",
                ],
                None,
                &[("PATH", "/usr/bin:/bin")],
            ),
            stdio_plan,
            SharedSpawnPolicy::node20_macos(
                ProcessTopology::Inherit,
                RuntimeDropAction::KeepAlive,
            ),
        )
        .await
        .unwrap();
    let (mut child, stdio) = spawned.into_parts();
    let ParentStdio::Pipes {
        stdin,
        stdout,
        stderr,
        extra,
    } = stdio
    else {
        panic!("extra stdio must publish the typed Pipes variant")
    };
    assert!(stdin.is_none());
    assert!(stderr.is_none());
    assert_eq!(extra.len(), 3);

    let mut extra = extra.into_iter();
    let first = extra.next().unwrap();
    assert_eq!(first.child_fd(), 3);
    let RuntimeExtraPipeEndpoint::Read(mut child_three) = first.into_parent() else {
        panic!("child fd 3 must be child-to-host")
    };
    let second = extra.next().unwrap();
    assert_eq!(second.child_fd(), 4);
    let RuntimeExtraPipeEndpoint::Write(mut child_four) = second.into_parent() else {
        panic!("child fd 4 must be host-to-child")
    };
    let third = extra.next().unwrap();
    assert_eq!(third.child_fd(), 5);
    let RuntimeExtraPipeEndpoint::Duplex(mut child_five) = third.into_parent() else {
        panic!("child fd 5 must remain duplex")
    };
    assert!(extra.next().is_none());

    tokio::io::AsyncWriteExt::write_all(&mut child_four, b"host-four\n")
        .await
        .unwrap();
    tokio::io::AsyncWriteExt::write_all(&mut child_five, b"host-five\n")
        .await
        .unwrap();

    let mut fd_three = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut child_three, &mut fd_three)
        .await
        .unwrap();
    let mut fd_five = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut child_five, &mut fd_five)
        .await
        .unwrap();
    let mut standard_stdout = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut stdout.unwrap(), &mut standard_stdout)
        .await
        .unwrap();

    assert_eq!(fd_three, b"child-three\n");
    assert_eq!(fd_five, b"duplex:host-five\n");
    assert_eq!(standard_stdout, b"stdout:host-four\n");
    wait_shared_child(&reaper, &mut child, 0);
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn darwin_rust_parent_pipes_attach_as_typed_tokio_io() {
    let reaper = SharedUnixReaper::ready_with_libc_for_test();
    let spawner = SharedRuntimeSpawner::with_reaper_for_test(reaper.clone());
    let spawned = spawner
        .spawn_rust193_macos_shadow(
            rust_image("/bin/cat", None, false),
            stdio_plan(
                UnixStdioDisposition::Pipe,
                UnixStdioDisposition::Pipe,
                UnixStdioDisposition::Null,
            ),
            SharedSpawnPolicy::rust193_inherit(RuntimeDropAction::KeepAlive),
        )
        .await
        .unwrap();
    let (mut child, stdio) = spawned.into_parts();
    let ParentStdio::Pipes {
        stdin,
        stdout,
        stderr,
        extra,
    } = stdio
    else {
        panic!("Rust 1.93 direct pipes must publish typed Tokio endpoints")
    };
    assert!(stderr.is_none());
    assert!(extra.is_empty());
    let mut stdin = stdin.unwrap();
    let mut stdout = stdout.unwrap();
    assert!(stdin.is_nonblocking());
    assert!(stdout.is_nonblocking());
    tokio::io::AsyncWriteExt::write_all(&mut stdin, b"rust-async-pipe\n")
        .await
        .unwrap();
    drop(stdin);
    let mut output = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut stdout, &mut output)
        .await
        .unwrap();
    assert_eq!(output, b"rust-async-pipe\n");
    wait_shared_child(&reaper, &mut child, 0);
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn darwin_rust_posix_and_source_selected_fallback_share_the_project_reaper() {
    let reaper = SharedUnixReaper::ready_with_libc_for_test();
    let spawner = SharedRuntimeSpawner::with_reaper_for_test(reaper.clone());
    let inherited = stdio_plan(
        UnixStdioDisposition::Inherit,
        UnixStdioDisposition::Inherit,
        UnixStdioDisposition::Inherit,
    );

    for constructed in [false, true] {
        let image = rust_image("/usr/bin/true", None, constructed);
        let spawned = spawner
            .spawn_rust193_macos_shadow(
                image,
                inherited,
                SharedSpawnPolicy::rust193_inherit(RuntimeDropAction::KeepAlive),
            )
            .await
            .unwrap();
        let (mut child, stdio) = spawned.into_parts();
        assert!(stdio.is_null_or_inherited());
        wait_shared_child(&reaper, &mut child, 0);
    }

    let unique = format!("klaus-rust193-fallback-{}", std::process::id());
    let root = std::env::temp_dir().join(unique);
    fs::create_dir_all(&root).unwrap();
    let executable = root.join("rust-fallback");
    fs::write(&executable, b"#!/bin/sh\nexit 0\n").unwrap();
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();
    let image = rust_image("./rust-fallback", Some(root.to_str().unwrap()), false);
    let features = Rust193MacOsSpawnFeatures::for_image(&image).unwrap();
    assert_eq!(
        select_rust193_route(features),
        Ok(UnixShadowSpawnRoute::Rust193ForkFallback),
    );
    let spawned = spawner
        .spawn_rust193_macos_shadow(
            image,
            inherited,
            SharedSpawnPolicy::rust193_inherit(RuntimeDropAction::KeepAlive),
        )
        .await
        .unwrap();
    let (mut child, stdio) = spawned.into_parts();
    assert!(stdio.is_null_or_inherited());
    wait_shared_child(&reaper, &mut child, 0);
    fs::remove_dir_all(root).unwrap();
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn darwin_node_fork_error_record_reaps_before_returning_errno() {
    let reaper = SharedUnixReaper::ready_with_libc_for_test();
    let spawner = SharedRuntimeSpawner::with_reaper_for_test(reaper.clone());
    let result = spawner
        .spawn_node20_macos_shadow(
            node_image("/definitely/missing/klaus-node-fork", None, &[]),
            stdio_plan(
                UnixStdioDisposition::Null,
                UnixStdioDisposition::Null,
                UnixStdioDisposition::Null,
            ),
            SharedSpawnPolicy::node20_macos(
                ProcessTopology::UnixNewProcessGroup,
                RuntimeDropAction::KeepAlive,
            ),
        )
        .await;
    assert!(matches!(
        result,
        Err(SharedSpawnError::MacOs(MacOsPosixSpawnError::Os {
            operation: MacOsPosixSpawnOperation::ChildSetup,
            errno: libc::ENOENT,
        }))
    ));
    assert_eq!(reaper.active_spawn_permits_for_test(), 0);
    assert_eq!(reaper.registration_count_for_test(), 0);
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn darwin_node_real_cwd_path_env_order_and_socket_stdio_are_exact() {
    let unique = format!(
        "klaus-macos-spawn-{}-{}",
        std::process::id(),
        std::thread::current().name().unwrap_or("test")
    );
    let root = std::env::temp_dir().join(unique);
    let bin = root.join("bin");
    fs::create_dir_all(&bin).unwrap();
    let tool = bin.join("klaus-tool");
    fs::write(&tool, b"#!/bin/sh\nprintf 'cwd-path-ok'").unwrap();
    fs::set_permissions(&tool, fs::Permissions::from_mode(0o755)).unwrap();

    let reaper = SharedUnixReaper::ready_with_libc_for_test();
    let spawner = SharedRuntimeSpawner::with_reaper_for_test(reaper.clone());
    let cwd = root.to_str().unwrap();
    let spawned = spawner
        .spawn_node20_macos_shadow(
            node_image(
                "klaus-tool",
                Some(cwd),
                &[("FIRST", "1"), ("PATH", "bin"), ("LAST", "3")],
            ),
            stdio_plan(
                UnixStdioDisposition::Null,
                UnixStdioDisposition::Pipe,
                UnixStdioDisposition::Null,
            ),
            SharedSpawnPolicy::node20_macos(ProcessTopology::Inherit, RuntimeDropAction::KeepAlive),
        )
        .await
        .unwrap();
    let (mut child, stdio) = spawned.into_parts();
    let (_, stdout, _) = stdio.into_standard_pipes();
    assert_eq!(
        read_nonblocking_to_eof(&mut stdout.unwrap()),
        b"cwd-path-ok"
    );
    wait_shared_child(&reaper, &mut child, 0);

    let env_spawned = spawner
        .spawn_node20_macos_shadow(
            node_image(
                "/usr/bin/env",
                None,
                &[("FIRST", "1"), ("PATH", "/usr/bin:/bin"), ("LAST", "3")],
            ),
            stdio_plan(
                UnixStdioDisposition::Null,
                UnixStdioDisposition::Pipe,
                UnixStdioDisposition::Null,
            ),
            SharedSpawnPolicy::node20_macos(ProcessTopology::Inherit, RuntimeDropAction::KeepAlive),
        )
        .await
        .unwrap();
    let (mut env_child, env_stdio) = env_spawned.into_parts();
    let (_, env_stdout, _) = env_stdio.into_standard_pipes();
    assert_eq!(
        read_nonblocking_to_eof(&mut env_stdout.unwrap()),
        b"FIRST=1\nPATH=/usr/bin:/bin\nLAST=3\n",
    );
    wait_shared_child(&reaper, &mut env_child, 0);

    let denied = root.join("denied");
    fs::create_dir(&denied).unwrap();
    let denied_tool = denied.join("not-executable");
    fs::write(&denied_tool, b"not executable").unwrap();
    fs::set_permissions(&denied_tool, fs::Permissions::from_mode(0o644)).unwrap();
    let denied_image = node_image("not-executable", Some(cwd), &[("PATH", "denied:missing")]);
    let denied_stdio = Node20StdioFileImage::from_plan(
        stdio_plan(
            UnixStdioDisposition::Inherit,
            UnixStdioDisposition::Inherit,
            UnixStdioDisposition::Inherit,
        ),
        [None; 7],
    )
    .unwrap();
    assert_eq!(
        spawn_node20_libuv146_darwin(
            &denied_image,
            Node20MacOsSpawnFeatures::ordinary(ProcessTopology::Inherit),
            denied_stdio,
        ),
        Err(MacOsPosixSpawnError::Syscall {
            operation: MacOsPosixSpawnOperation::Spawn,
            errno: libc::EACCES,
        }),
    );
    fs::remove_dir_all(root).unwrap();
}

/// Liveness budget for waits on a spawned child's pipes or exit. A full-suite run
/// saturates the host badly enough that a child can be starved for a minute-plus,
/// so these waits are deliberately huge: the assertions behind them are pure
/// liveness ("the pipe drains" / "the child exits"), never latency. Waits also
/// SLEEP rather than `yield_now`, which would keep the waiting thread hot and
/// make the starvation it is waiting out worse.
#[cfg(target_os = "macos")]
const CHILD_LIVENESS_TIMEOUT: Duration = Duration::from_secs(180);

/// Poll interval for the waits above — short enough to stay responsive, long
/// enough to yield the core to the child being waited on.
#[cfg(target_os = "macos")]
const CHILD_POLL_INTERVAL: Duration = Duration::from_millis(5);

#[cfg(target_os = "macos")]
fn write_nonblocking(writer: &mut impl Write, mut bytes: &[u8]) {
    let deadline = Instant::now() + CHILD_LIVENESS_TIMEOUT;
    while !bytes.is_empty() {
        match writer.write(bytes) {
            Ok(0) => panic!("zero write before input completed"),
            Ok(written) => bytes = &bytes[written..],
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(Instant::now() < deadline, "nonblocking write timed out");
                std::thread::sleep(CHILD_POLL_INTERVAL);
            }
            Err(error) => panic!("write failed: {error}"),
        }
    }
}

#[cfg(target_os = "macos")]
fn read_nonblocking_to_eof(reader: &mut impl Read) -> Vec<u8> {
    let deadline = Instant::now() + CHILD_LIVENESS_TIMEOUT;
    let mut output = Vec::new();
    let mut buffer = [0_u8; 4096];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) => return output,
            Ok(read) => output.extend_from_slice(&buffer[..read]),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(Instant::now() < deadline, "nonblocking read timed out");
                std::thread::sleep(CHILD_POLL_INTERVAL);
            }
            Err(error) => panic!("read failed: {error}"),
        }
    }
}

#[cfg(target_os = "macos")]
fn wait_shared_child(
    reaper: &SharedUnixReaper,
    child: &mut super::reaper::RuntimeChild,
    expected_code: i32,
) {
    let identity = child.identity_for_test();
    let deadline = Instant::now() + CHILD_LIVENESS_TIMEOUT;
    loop {
        if let Some(outcome) = child.try_wait() {
            assert_eq!(outcome.unwrap().code(), Some(expected_code));
            break;
        }
        match reaper.poll(identity) {
            ReaperPollResult::Running if Instant::now() < deadline => {
                std::thread::sleep(CHILD_POLL_INTERVAL)
            }
            ReaperPollResult::Running => panic!("child did not exit"),
            ReaperPollResult::Published(Ok(status)) => {
                assert_eq!(status.code(), Some(expected_code));
                break;
            }
            ReaperPollResult::UnknownRegistration(_) if child.try_wait().is_some() => continue,
            other => panic!("unexpected reaper result: {other:?}"),
        }
    }
    child.disarm();
}
