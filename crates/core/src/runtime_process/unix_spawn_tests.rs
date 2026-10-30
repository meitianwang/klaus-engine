use std::ffi::CString;
use std::io::{Read, Write};

use super::plan::{Rust193EnvironmentPlan, Rust193SpawnPreimage};
use super::unix::UnixExecImage;
use super::unix_spawn::{
    Rust193DirectPipeShadowPlan, Rust193ForkFallbackShadowSpawner, UnixShadowSpawnError,
    UnixShadowSpawnRoute, UnixSpawnTestControl, UnixSpawnTestFault, UnixStdioDisposition,
    UnixStdioPlan,
};
use super::{
    NodeSpawnPlan, ProcessAtom, ProcessEnvironmentGuard, ProcessEnvironmentPlan,
    ProcessImageDialect, RuntimeEnv,
};
use crate::runtime_values::RuntimeString;

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

fn rust_overlay_image(
    file: &str,
    args: &[&str],
    mutations: Vec<super::Rust193EnvMutation>,
) -> UnixExecImage {
    let preimage = Rust193SpawnPreimage::from_deferred_parts(
        runtime(file),
        runtime(file),
        args.iter().map(|arg| runtime(*arg)).collect(),
        None,
        Rust193EnvironmentPlan::Overlay { mutations },
    );
    UnixExecImage::build_rust193_with_parent_snapshot(
        &preimage,
        ProcessEnvironmentGuard::None,
        || Ok(Vec::new()),
    )
    .expect("valid constructed Rust193 Unix image")
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
    UnixExecImage::build_node(&plan, ProcessEnvironmentGuard::None).expect("valid Node Unix image")
}

fn plan(
    stdin: UnixStdioDisposition,
    stdout: UnixStdioDisposition,
    stderr: UnixStdioDisposition,
) -> Rust193DirectPipeShadowPlan {
    Rust193DirectPipeShadowPlan::rust193_fork_fallback(UnixStdioPlan::new(stdin, stdout, stderr))
}

#[test]
fn node_dialect_with_foreign_route_fails_closed_before_any_descriptor_action() {
    let mut control = UnixSpawnTestControl::default();
    let result = Rust193ForkFallbackShadowSpawner::spawn_with_test_control(
        node_image("/bin/true"),
        Rust193DirectPipeShadowPlan::for_test(
            ProcessImageDialect::Node20Libuv146,
            UnixShadowSpawnRoute::Rust193ForkFallback,
            UnixStdioPlan::new(
                UnixStdioDisposition::Pipe,
                UnixStdioDisposition::Pipe,
                UnixStdioDisposition::Pipe,
            ),
        ),
        &mut control,
    );

    assert!(matches!(
        result,
        Err(UnixShadowSpawnError::UnsupportedSpawnRoute {
            requested: UnixShadowSpawnRoute::Rust193ForkFallback,
        })
    ));
    assert_eq!(control.descriptor_actions(), 0);
    assert_eq!(control.fork_calls(), 0);
}

#[test]
fn node_image_cannot_be_relabelled_as_rust193_before_descriptor_creation() {
    let mut control = UnixSpawnTestControl::default();
    let result = Rust193ForkFallbackShadowSpawner::spawn_with_test_control(
        node_image("/bin/true"),
        plan(
            UnixStdioDisposition::Pipe,
            UnixStdioDisposition::Pipe,
            UnixStdioDisposition::Pipe,
        ),
        &mut control,
    );

    assert!(matches!(
        result,
        Err(UnixShadowSpawnError::ImageDialectMismatch {
            image: ProcessImageDialect::Node20Libuv146,
            requested: ProcessImageDialect::Rust193,
        })
    ));
    assert_eq!(control.descriptor_actions(), 0);
    assert_eq!(control.fork_calls(), 0);
}

#[test]
fn unfinished_rust193_route_fails_closed_before_any_descriptor_action() {
    let mut control = UnixSpawnTestControl::default();
    let result = Rust193ForkFallbackShadowSpawner::spawn_with_test_control(
        rust_image("/bin/true", &[]),
        Rust193DirectPipeShadowPlan::for_test(
            ProcessImageDialect::Rust193,
            UnixShadowSpawnRoute::Rust193PosixSpawn,
            UnixStdioPlan::new(
                UnixStdioDisposition::Pipe,
                UnixStdioDisposition::Pipe,
                UnixStdioDisposition::Pipe,
            ),
        ),
        &mut control,
    );

    assert!(matches!(
        result,
        Err(UnixShadowSpawnError::UnsupportedSpawnRoute {
            requested: UnixShadowSpawnRoute::Rust193PosixSpawn,
        })
    ));
    assert_eq!(control.descriptor_actions(), 0);
    assert_eq!(control.fork_calls(), 0);
}

#[test]
fn rust193_direct_pipes_round_trip_with_typed_unique_endpoints() {
    let spawned = Rust193ForkFallbackShadowSpawner::spawn(
        rust_image("/bin/cat", &[]),
        plan(
            UnixStdioDisposition::Pipe,
            UnixStdioDisposition::Pipe,
            UnixStdioDisposition::Null,
        ),
    )
    .expect("spawn /bin/cat");
    let (mut child, stdio) = spawned.into_parts();
    let (stdin, stdout, stderr) = stdio.into_standard_pipes();
    let mut stdin = stdin.expect("typed child stdin");
    let mut stdout = stdout.expect("typed child stdout");
    assert!(stderr.is_none(), "null stderr cannot publish a pipe");

    assert!(stdin.is_fifo());
    assert!(stdout.is_fifo());
    assert!(stdin.is_close_on_exec());
    assert!(stdout.is_close_on_exec());
    assert!(!stdin.is_nonblocking());
    assert!(!stdout.is_nonblocking());

    stdin
        .write_all(b"typed direct pipe\n")
        .expect("write stdin");
    drop(stdin);

    let mut output = Vec::new();
    stdout.read_to_end(&mut output).expect("read stdout");
    assert_eq!(output, b"typed direct pipe\n");
    drop(stdout);

    let first = child.wait().expect("first wait");
    let second = child.wait().expect("cached second wait");
    assert_eq!(first, second);
    assert_eq!(first.code(), Some(0));
    assert_eq!(child.successful_wait_syscalls_for_test(), 1);
}

#[test]
fn rust193_constructed_environment_drives_execvp_path_search_without_a_map_rebuild() {
    let image = rust_overlay_image(
        "sh",
        &["-c", "printf '%s' \"$KLAUS_SHADOW_ENV\""],
        vec![
            super::Rust193EnvMutation::RuntimeSet {
                key: RuntimeString::from("KLAUS_SHADOW_ENV"),
                value: RuntimeString::from("exact-shadow-value"),
            },
            super::Rust193EnvMutation::RuntimeSet {
                key: RuntimeString::from("PATH"),
                value: RuntimeString::from("/bin:/usr/bin"),
            },
        ],
    );
    let spawned = Rust193ForkFallbackShadowSpawner::spawn(
        image,
        plan(
            UnixStdioDisposition::Null,
            UnixStdioDisposition::Pipe,
            UnixStdioDisposition::Null,
        ),
    )
    .expect("spawn sh through constructed PATH");
    let (mut child, stdio) = spawned.into_parts();
    let (_, stdout, _) = stdio.into_standard_pipes();
    let mut stdout = stdout.expect("typed child stdout");
    let mut output = Vec::new();
    stdout.read_to_end(&mut output).expect("read stdout");

    assert_eq!(output, b"exact-shadow-value");
    assert_eq!(child.wait().expect("wait").code(), Some(0));
    assert_eq!(child.successful_wait_syscalls_for_test(), 1);
}

#[test]
fn inherit_and_null_shape_publishes_no_missing_or_synthetic_pipe() {
    let spawned = Rust193ForkFallbackShadowSpawner::spawn(
        rust_image("/bin/sh", &["-c", "exit 7"]),
        plan(
            UnixStdioDisposition::Inherit,
            UnixStdioDisposition::Null,
            UnixStdioDisposition::Null,
        ),
    )
    .expect("spawn /bin/sh");
    let (mut child, stdio) = spawned.into_parts();
    assert!(stdio.is_null_or_inherited());
    assert_eq!(child.wait().expect("wait").code(), Some(7));
    assert_eq!(child.successful_wait_syscalls_for_test(), 1);
}

#[test]
fn child_exec_errno_crosses_exact_noex_channel_and_child_is_reaped() {
    let mut control = UnixSpawnTestControl::default();
    let result = Rust193ForkFallbackShadowSpawner::spawn_with_test_control(
        rust_image("/definitely/missing/klaus-runtime-exec", &[]),
        plan(
            UnixStdioDisposition::Null,
            UnixStdioDisposition::Null,
            UnixStdioDisposition::Null,
        ),
        &mut control,
    );

    assert!(matches!(
        result,
        Err(UnixShadowSpawnError::ChildExec {
            errno: libc::ENOENT
        })
    ));
    assert_eq!(control.exec_failure_wait_successes(), 1);
    assert!(control.exec_failure_parent_endpoints_closed_before_wait());
    assert!(control.closed_parent_fd_actions() >= 5);
    let pid = control.exec_failure_pid().expect("failed child pid");
    // SAFETY: the backend owns this exact child and must already have reaped it.
    let waited = unsafe { libc::waitpid(pid, std::ptr::null_mut(), libc::WNOHANG) };
    assert_eq!(waited, -1);
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ECHILD)
    );
}

#[test]
fn post_fork_shape_failure_closes_endpoints_then_kills_and_reaps_hidden_child_once() {
    let mut control =
        UnixSpawnTestControl::with_fault(UnixSpawnTestFault::DropStdoutBeforeShapeValidation);
    let result = Rust193ForkFallbackShadowSpawner::spawn_with_test_control(
        rust_image("/bin/sleep", &["30"]),
        plan(
            UnixStdioDisposition::Null,
            UnixStdioDisposition::Pipe,
            UnixStdioDisposition::Null,
        ),
        &mut control,
    );

    assert!(matches!(
        result,
        Err(UnixShadowSpawnError::ParentStdioShapeMismatch)
    ));
    assert_eq!(control.hidden_cleanup_kill_calls(), 1);
    assert_eq!(control.hidden_cleanup_wait_successes(), 1);
    assert!(control.hidden_cleanup_parent_endpoints_closed_before_kill());
    assert!(control.closed_parent_fd_actions() >= 6);
    let pid = control.hidden_cleanup_pid().expect("hidden child pid");
    // SAFETY: hidden cleanup must have consumed the only wait right.
    let waited = unsafe { libc::waitpid(pid, std::ptr::null_mut(), libc::WNOHANG) };
    assert_eq!(waited, -1);
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ECHILD)
    );
}

#[test]
fn noex_record_is_big_endian_and_closed_against_short_or_wrong_footer() {
    let encoded = super::unix_spawn::encode_rust193_exec_error(libc::EACCES);
    assert_eq!(&encoded[4..], b"NOEX");
    assert_eq!(
        super::unix_spawn::decode_rust193_exec_error(&encoded),
        Ok(libc::EACCES)
    );
    assert!(super::unix_spawn::decode_rust193_exec_error(&encoded[..7]).is_err());
    let mut wrong = encoded;
    wrong[7] = b'!';
    assert!(super::unix_spawn::decode_rust193_exec_error(&wrong).is_err());
}

#[test]
fn corrupt_noex_footer_reaps_then_enters_the_rust193_invariant_panic() {
    let mut control = UnixSpawnTestControl::with_fault(UnixSpawnTestFault::CorruptExecErrorFooter);
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = Rust193ForkFallbackShadowSpawner::spawn_with_test_control(
            rust_image("/definitely/missing/klaus-runtime-exec", &[]),
            plan(
                UnixStdioDisposition::Null,
                UnixStdioDisposition::Null,
                UnixStdioDisposition::Null,
            ),
            &mut control,
        );
    }));

    assert!(panicked.is_err());
    assert_eq!(control.invariant_failure_wait_successes(), 1);
    let pid = control
        .invariant_failure_pid()
        .expect("invariant child pid");
    // SAFETY: the invariant path must synchronously consume the wait right
    // before it panics in the parent.
    let waited = unsafe { libc::waitpid(pid, std::ptr::null_mut(), libc::WNOHANG) };
    assert_eq!(waited, -1);
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ECHILD)
    );
}

#[test]
fn rust193_stdio_source_keeps_pinned_mac_cloexec_open_and_low_fd_rules() {
    let source = include_str!("unix_spawn.rs");
    assert!(source.contains("libc::FIOCLEX"));
    assert!(source.contains("rust193_open"));
    assert!(source.contains("last_errno() != libc::EINTR"));
    // PTY descriptor duplication is the only F_DUPFD_CLOEXEC consumer; the
    // rust193 stdio path itself still relies on FIOCLEX + rust193_open.
    assert!(source.contains("F_DUPFD_CLOEXEC"));
    assert!(!source.contains("move_above_stdio"));
}

#[test]
fn backend_source_cannot_delegate_to_standard_or_tokio_command() {
    let source = include_str!("unix_spawn.rs");
    let forbidden = [
        ["std", "process", "Command"].join("::"),
        ["tokio", "process", "Command"].join("::"),
    ];
    for symbol in forbidden {
        assert!(
            !source.contains(&symbol),
            "Unix direct spawn backend delegated to {symbol}"
        );
    }
}

#[test]
fn test_fixture_paths_are_nul_free_c_strings() {
    for value in ["/bin/cat", "/bin/sh", "/bin/sleep"] {
        CString::new(value).expect("fixture path");
    }
}
