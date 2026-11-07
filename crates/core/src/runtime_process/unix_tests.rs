use std::cell::Cell;
use std::ffi::{CStr, CString, OsString};
use std::os::unix::ffi::OsStringExt;

use syn::visit::{self, Visit};

use super::plan::{Rust193EnvironmentPlan, Rust193SpawnPreimage};
use super::process_environment::with_process_environment_lock;
use super::unix::{
    UnixAllocationStage, UnixEnvironmentImage, UnixExecImage, UnixExecImageBuildError,
    UnixExecImageField,
};
use super::{
    NativeHostEnvOverlay, NativeOsString, NodeSpawnPlan, ProcessAtom, ProcessEnvironmentGuard,
    ProcessEnvironmentPlan, ProcessPlanErrorKind, ProcessPlanField, RuntimeEnv, Rust193EnvMutation,
};
use crate::runtime_values::RuntimeString;

fn runtime(value: impl Into<RuntimeString>) -> ProcessAtom {
    ProcessAtom::Runtime(value.into())
}

fn node_plan(
    file: ProcessAtom,
    argv0: ProcessAtom,
    args: Vec<ProcessAtom>,
    cwd: Option<ProcessAtom>,
    environment: RuntimeEnv,
) -> NodeSpawnPlan {
    NodeSpawnPlan::from_normalized_parts(
        file,
        argv0,
        args,
        cwd,
        ProcessEnvironmentPlan::NodeRuntime(environment),
        false,
        false,
        false,
    )
    .expect("valid normalized Node Unix image fixture")
}

fn simple_node_plan(environment: RuntimeEnv) -> NodeSpawnPlan {
    node_plan(
        runtime("node"),
        runtime("node"),
        Vec::new(),
        None,
        environment,
    )
}

fn rust_preimage(
    file: ProcessAtom,
    argv0: ProcessAtom,
    args: Vec<ProcessAtom>,
    cwd: Option<ProcessAtom>,
    environment: Rust193EnvironmentPlan,
) -> Rust193SpawnPreimage {
    Rust193SpawnPreimage::from_deferred_parts(file, argv0, args, cwd, environment)
}

fn native_parent_preimage(cwd: Option<ProcessAtom>) -> Rust193SpawnPreimage {
    rust_preimage(
        runtime("node"),
        runtime("node"),
        Vec::new(),
        cwd,
        Rust193EnvironmentPlan::NativeParentInherit,
    )
}

fn overlay_preimage(mutations: Vec<Rust193EnvMutation>) -> Rust193SpawnPreimage {
    rust_preimage(
        runtime("native-tool"),
        runtime("native-tool"),
        Vec::new(),
        None,
        Rust193EnvironmentPlan::Overlay { mutations },
    )
}

fn build_rust_with_raw(
    preimage: &Rust193SpawnPreimage,
    guard: ProcessEnvironmentGuard,
    raw: Vec<Vec<u8>>,
) -> Result<UnixExecImage, UnixExecImageBuildError> {
    UnixExecImage::build_rust193_with_parent_snapshot(preimage, guard, || Ok(raw))
}

fn cstrings(values: &[CString]) -> Vec<Vec<u8>> {
    values
        .iter()
        .map(|value| value.as_bytes().to_vec())
        .collect()
}

fn environment_bytes(image: &UnixExecImage) -> Vec<Vec<u8>> {
    let UnixEnvironmentImage::Constructed { storage, .. } = image.environment() else {
        panic!("fixture requires a constructed environment")
    };
    cstrings(storage)
}

fn assert_pointer_image(storage: &[CString], pointers: &[*const libc::c_char]) {
    assert_eq!(pointers.len(), storage.len() + 1);
    for (value, pointer) in storage.iter().zip(pointers) {
        assert_eq!(*pointer, value.as_ptr());
        // SAFETY: every pointer is owned by `storage` for the lifetime of the image.
        assert_eq!(unsafe { CStr::from_ptr(*pointer) }, value.as_c_str());
    }
    assert!(pointers
        .last()
        .copied()
        .expect("terminal pointer")
        .is_null());
}

#[test]
fn unix_image_projects_runtime_atoms_once_and_preserves_explicit_empty_cwd() {
    let lone_high = RuntimeString::from_utf16(vec![0xd800]);
    let literal_replacement = RuntimeString::from_utf16(vec![0xfffd]);
    let image = UnixExecImage::build_node(
        &node_plan(
            runtime("node"),
            runtime("custom-argv0"),
            vec![
                runtime(lone_high),
                runtime(literal_replacement),
                runtime("tail"),
            ],
            Some(runtime("")),
            RuntimeEnv::new(),
        ),
        ProcessEnvironmentGuard::None,
    )
    .expect("valid image");

    assert_eq!(image.file().to_bytes(), b"node");
    assert_eq!(image.cwd().expect("explicit cwd").to_bytes(), b"");
    assert_eq!(
        cstrings(image.argv_storage()),
        vec![
            b"custom-argv0".to_vec(),
            b"\xef\xbf\xbd".to_vec(),
            b"\xef\xbf\xbd".to_vec(),
            b"tail".to_vec(),
        ]
    );
    assert_pointer_image(image.argv_storage(), image.argv_with_null());

    let moved = Box::new(image);
    assert_pointer_image(moved.argv_storage(), moved.argv_with_null());
    let UnixEnvironmentImage::Constructed {
        storage,
        envp_with_null,
    } = moved.environment()
    else {
        panic!("empty NodeRuntime must still construct envp")
    };
    assert!(storage.is_empty());
    assert_pointer_image(storage, envp_with_null);
}

#[test]
fn omitted_and_explicit_empty_cwd_remain_distinct() {
    let omitted = build_rust_with_raw(
        &native_parent_preimage(None),
        ProcessEnvironmentGuard::None,
        Vec::new(),
    )
    .expect("omitted cwd image");
    let empty = build_rust_with_raw(
        &native_parent_preimage(Some(runtime(""))),
        ProcessEnvironmentGuard::None,
        Vec::new(),
    )
    .expect("empty cwd image");

    assert!(omitted.cwd().is_none());
    assert_eq!(empty.cwd().expect("explicit cwd").to_bytes(), b"");
}

#[test]
fn rust193_empty_executable_is_imaged_after_snapshot_for_the_os_to_reject() {
    let snapshots = Cell::new(0);
    let preimage = rust_preimage(
        runtime(""),
        runtime(""),
        Vec::new(),
        None,
        Rust193EnvironmentPlan::Overlay {
            mutations: Vec::new(),
        },
    );
    let image = UnixExecImage::build_rust193_with_parent_snapshot(
        &preimage,
        ProcessEnvironmentGuard::None,
        || {
            snapshots.set(snapshots.get() + 1);
            Ok(Vec::new())
        },
    )
    .expect("Rust std defers empty executable to execvp");

    assert_eq!(snapshots.get(), 1);
    assert_eq!(image.file().to_bytes(), b"");
    assert_eq!(image.argv_storage()[0].as_bytes(), b"");
}

#[test]
fn node_runtime_keeps_ecmascript_order_and_projected_duplicate_names() {
    let mut environment = RuntimeEnv::new();
    environment.insert(RuntimeString::from("tail"), RuntimeString::from("last"));
    environment.insert(RuntimeString::from("2"), RuntimeString::from("index"));
    environment.insert(
        RuntimeString::from_utf16(vec![0xd800]),
        RuntimeString::from("high"),
    );
    environment.insert(
        RuntimeString::from_utf16(vec![0xd801]),
        RuntimeString::from("next"),
    );
    environment.insert(
        RuntimeString::from_utf16(vec![0xfffd]),
        RuntimeString::from("replacement"),
    );
    environment.insert(RuntimeString::from("A=B"), RuntimeString::from("C"));

    let image = UnixExecImage::build_node(
        &simple_node_plan(environment),
        ProcessEnvironmentGuard::None,
    )
    .expect("valid image");
    let UnixEnvironmentImage::Constructed {
        storage,
        envp_with_null,
    } = image.environment()
    else {
        panic!("NodeRuntime must construct envp")
    };

    assert_eq!(
        cstrings(storage),
        vec![
            b"2=index".to_vec(),
            b"tail=last".to_vec(),
            b"\xef\xbf\xbd=high".to_vec(),
            b"\xef\xbf\xbd=next".to_vec(),
            b"\xef\xbf\xbd=replacement".to_vec(),
            b"A=B=C".to_vec(),
        ]
    );
    assert_pointer_image(storage, envp_with_null);
}

#[test]
fn unguarded_native_parent_is_pointer_inherit_and_never_snapshots() {
    let snapshots = Cell::new(0);
    let image = UnixExecImage::build_rust193_with_parent_snapshot(
        &native_parent_preimage(None),
        ProcessEnvironmentGuard::None,
        || {
            snapshots.set(snapshots.get() + 1);
            Ok(vec![b"SHOULD_NOT_BE_READ=1".to_vec()])
        },
    )
    .expect("native inherit image");

    assert_eq!(snapshots.get(), 0);
    assert!(matches!(
        image.environment(),
        UnixEnvironmentImage::NativeParentInherit
    ));
}

#[test]
fn sealed_native_parent_filters_only_exact_node_options_and_preserves_raw_order() {
    let raw = vec![
        b"DUP=first".to_vec(),
        b"NODE_OPTIONS=--require=evil".to_vec(),
        b"MALFORMED".to_vec(),
        b"node_options=kept".to_vec(),
        vec![b'I', b'N', b'V', b'=', 0xff],
        b"DUP=second".to_vec(),
        b"NODE_OPTIONS=".to_vec(),
        b"XNODE_OPTIONS=kept".to_vec(),
    ];
    let image = build_rust_with_raw(
        &native_parent_preimage(None),
        ProcessEnvironmentGuard::SealedNodeNoPreloadV1,
        raw,
    )
    .expect("sealed native image");
    let UnixEnvironmentImage::Constructed {
        storage,
        envp_with_null,
    } = image.environment()
    else {
        panic!("sealed inherit must construct a filtered envp")
    };

    assert_eq!(
        cstrings(storage),
        vec![
            b"DUP=first".to_vec(),
            b"MALFORMED".to_vec(),
            b"node_options=kept".to_vec(),
            vec![b'I', b'N', b'V', b'=', 0xff],
            b"DUP=second".to_vec(),
            b"XNODE_OPTIONS=kept".to_vec(),
        ]
    );
    assert_pointer_image(storage, envp_with_null);
}

fn rust_overlay_fixture_mutations() -> Vec<Rust193EnvMutation> {
    let projected_collision_one = RuntimeString::from_utf16(vec![0xd800]);
    let projected_collision_two = RuntimeString::from_utf16(vec![0xfffd]);
    let native_value = NativeOsString::from_unix_bytes_for_test(vec![0xff, b'x']);
    vec![
        Rust193EnvMutation::RuntimeRemove {
            key: RuntimeString::from("REMOVE"),
        },
        Rust193EnvMutation::RuntimeSet {
            key: projected_collision_one,
            value: RuntimeString::from("first"),
        },
        Rust193EnvMutation::RuntimeSet {
            key: projected_collision_two,
            value: RuntimeString::from("second"),
        },
        Rust193EnvMutation::NativeHostSet(NativeHostEnvOverlay::new_for_test(
            RuntimeString::from("NATIVE"),
            native_value.clone(),
        )),
        Rust193EnvMutation::RuntimeSet {
            key: RuntimeString::from("NATIVE"),
            value: RuntimeString::from("runtime-wins"),
        },
        Rust193EnvMutation::NativeHostSet(NativeHostEnvOverlay::new_for_test(
            RuntimeString::from("NATIVE_KEEP"),
            native_value,
        )),
        Rust193EnvMutation::RuntimeSet {
            key: RuntimeString::from("A"),
            value: RuntimeString::from("overlay"),
        },
    ]
}

fn rust_overlay_fixture_parent() -> Vec<Vec<u8>> {
    vec![
        b"Z=last".to_vec(),
        b"A=first".to_vec(),
        b"MALFORMED".to_vec(),
        b"A=parent-last".to_vec(),
        b"=X:=hidden".to_vec(),
        b"REMOVE=gone".to_vec(),
    ]
}

#[test]
fn rust193_overlay_ports_capture_mutation_collision_and_sorted_envkey_order() {
    let image = build_rust_with_raw(
        &overlay_preimage(rust_overlay_fixture_mutations()),
        ProcessEnvironmentGuard::None,
        rust_overlay_fixture_parent(),
    )
    .expect("Rust 1.93 overlay image");

    assert_eq!(
        environment_bytes(&image),
        vec![
            b"=X:=hidden".to_vec(),
            b"A=overlay".to_vec(),
            b"NATIVE=runtime-wins".to_vec(),
            b"NATIVE_KEEP=\xffx".to_vec(),
            b"Z=last".to_vec(),
            b"\xef\xbf\xbd=second".to_vec(),
        ]
    );
    let UnixEnvironmentImage::Constructed {
        storage,
        envp_with_null,
    } = image.environment()
    else {
        unreachable!()
    };
    assert_pointer_image(storage, envp_with_null);
}

#[test]
fn sealed_rust193_overlay_rejects_caller_node_options_before_snapshot() {
    let preimage = overlay_preimage(vec![Rust193EnvMutation::RuntimeSet {
        key: RuntimeString::from("NODE_OPTIONS"),
        value: RuntimeString::from("--require=evil"),
    }]);
    let snapshots = Cell::new(0);
    let error = UnixExecImage::build_rust193_with_parent_snapshot(
        &preimage,
        ProcessEnvironmentGuard::SealedNodeNoPreloadV1,
        || {
            snapshots.set(snapshots.get() + 1);
            Ok(Vec::new())
        },
    )
    .unwrap_err();

    assert_eq!(snapshots.get(), 0);
    assert_eq!(error, UnixExecImageBuildError::ForbiddenNodeOptionsMutation);
    assert_eq!(
        error.to_string(),
        "Custom browser environment may not set NODE_OPTIONS in the exact runtime."
    );
}

#[test]
fn sealed_rust193_overlay_filters_inherited_node_options_only() {
    let preimage = overlay_preimage(vec![Rust193EnvMutation::RuntimeSet {
        key: RuntimeString::from("node_options"),
        value: RuntimeString::from("case-kept"),
    }]);
    let image = build_rust_with_raw(
        &preimage,
        ProcessEnvironmentGuard::SealedNodeNoPreloadV1,
        vec![b"NODE_OPTIONS=parent".to_vec(), b"KEEP=parent".to_vec()],
    )
    .expect("sealed Rust overlay image");

    assert_eq!(
        environment_bytes(&image),
        vec![b"KEEP=parent".to_vec(), b"node_options=case-kept".to_vec()]
    );
}

#[test]
fn native_process_atoms_keep_exact_unix_bytes() {
    let native = NativeOsString::from_unix_bytes_for_test(vec![b'/', 0xff]);
    let preimage = rust_preimage(
        ProcessAtom::Native(native.clone()),
        ProcessAtom::Native(native.clone()),
        vec![ProcessAtom::Native(native)],
        Some(ProcessAtom::Native(
            NativeOsString::from_unix_bytes_for_test(Vec::new()),
        )),
        Rust193EnvironmentPlan::NativeParentInherit,
    );
    let image = build_rust_with_raw(&preimage, ProcessEnvironmentGuard::None, Vec::new())
        .expect("native atom image");

    assert_eq!(image.file().to_bytes(), &[b'/', 0xff]);
    assert_eq!(image.cwd().expect("empty native cwd").to_bytes(), b"");
    assert_eq!(
        cstrings(image.argv_storage()),
        vec![vec![b'/', 0xff], vec![b'/', 0xff]]
    );
}

#[test]
fn node_runtime_rejects_the_sealed_guard() {
    let error = UnixExecImage::build_node(
        &simple_node_plan(RuntimeEnv::new()),
        ProcessEnvironmentGuard::SealedNodeNoPreloadV1,
    )
    .unwrap_err();

    assert_eq!(error, UnixExecImageBuildError::GuardMismatch);
}

#[test]
fn legacy_rust_environment_cannot_enter_the_node_builder() {
    let legacy = NodeSpawnPlan::from_normalized_parts(
        runtime("node"),
        runtime("node"),
        Vec::new(),
        None,
        ProcessEnvironmentPlan::NativeParentInherit,
        false,
        false,
        false,
    )
    .unwrap();
    assert_eq!(
        UnixExecImage::build_node(&legacy, ProcessEnvironmentGuard::None).unwrap_err(),
        UnixExecImageBuildError::Rust193PreimageRequired
    );
}

#[test]
fn parent_snapshot_nul_is_a_pre_spawn_build_error() {
    let error = build_rust_with_raw(
        &native_parent_preimage(None),
        ProcessEnvironmentGuard::SealedNodeNoPreloadV1,
        vec![b"A=ok".to_vec(), b"BROKEN=left\0right".to_vec()],
    )
    .unwrap_err();

    assert_eq!(
        error,
        UnixExecImageBuildError::ContainsNul {
            field: UnixExecImageField::ParentEnvironmentEntry(1),
            byte_index: 11,
        }
    );
}

#[test]
fn rust193_snapshot_failure_precedes_deferred_nul_validation() {
    let nul = RuntimeString::from_utf16(vec![b'x'.into(), 0, b'y'.into()]);
    let preimage = rust_preimage(
        runtime("native-tool"),
        runtime("native-tool"),
        vec![runtime(nul)],
        None,
        Rust193EnvironmentPlan::Overlay {
            mutations: Vec::new(),
        },
    );

    let snapshot_error = UnixExecImage::build_rust193_with_parent_snapshot(
        &preimage,
        ProcessEnvironmentGuard::None,
        || Err(UnixExecImageBuildError::SnapshotFailure),
    )
    .unwrap_err();
    assert_eq!(snapshot_error, UnixExecImageBuildError::SnapshotFailure);

    let nul_error =
        build_rust_with_raw(&preimage, ProcessEnvironmentGuard::None, Vec::new()).unwrap_err();
    let UnixExecImageBuildError::InvalidPlan(nul_error) = nul_error else {
        panic!("deferred NUL must retain the plan error")
    };
    assert_eq!(nul_error.field(), ProcessPlanField::Argument(0));
    assert_eq!(
        nul_error.kind(),
        ProcessPlanErrorKind::ContainsNul { unit_index: 1 }
    );
}

#[test]
fn rust193_command_env_discards_overwritten_and_removed_nul_mutations() {
    let bad_value = RuntimeString::from_utf16(vec![
        b'b'.into(),
        b'a'.into(),
        b'd'.into(),
        0,
        b'v'.into(),
        b'a'.into(),
        b'l'.into(),
        b'u'.into(),
        b'e'.into(),
    ]);
    let bad_remove_key = RuntimeString::from_utf16(vec![
        b'b'.into(),
        b'a'.into(),
        b'd'.into(),
        0,
        b'k'.into(),
        b'e'.into(),
        b'y'.into(),
    ]);
    let mutations = vec![
        Rust193EnvMutation::RuntimeSet {
            key: RuntimeString::from("A"),
            value: bad_value,
        },
        Rust193EnvMutation::RuntimeSet {
            key: RuntimeString::from("A"),
            value: RuntimeString::from("good"),
        },
        Rust193EnvMutation::RuntimeRemove {
            key: bad_remove_key,
        },
    ];
    let image = build_rust_with_raw(
        &overlay_preimage(mutations),
        ProcessEnvironmentGuard::None,
        Vec::new(),
    )
    .expect("discarded NUL mutations do not reach the final envp");

    let mut command = std::process::Command::new("/usr/bin/env");
    let output = command
        .arg("-0")
        .env_clear()
        .env("A", OsString::from_vec(b"bad\0value".to_vec()))
        .env("A", "good")
        .env_remove(OsString::from_vec(b"bad\0key".to_vec()))
        .output()
        .expect("Rust 1.93 CommandEnv discards both non-surviving NUL mutations");
    assert!(output.status.success(), "env helper failed: {output:?}");
    assert_eq!(output.stdout, b"A=good\0");
    assert_eq!(environment_bytes(&image), vec![b"A=good".to_vec()]);
}

#[test]
fn rust193_command_env_rejects_a_surviving_final_nul_after_snapshot() {
    let bad_value = RuntimeString::from_utf16(vec![
        b'b'.into(),
        b'a'.into(),
        b'd'.into(),
        0,
        b'v'.into(),
        b'a'.into(),
        b'l'.into(),
        b'u'.into(),
        b'e'.into(),
    ]);
    let preimage = overlay_preimage(vec![Rust193EnvMutation::RuntimeSet {
        key: RuntimeString::from("A"),
        value: bad_value,
    }]);
    let snapshots = Cell::new(0);
    let error = UnixExecImage::build_rust193_with_parent_snapshot(
        &preimage,
        ProcessEnvironmentGuard::None,
        || {
            snapshots.set(snapshots.get() + 1);
            Ok(Vec::new())
        },
    )
    .unwrap_err();
    assert_eq!(snapshots.get(), 1);
    assert_eq!(
        error,
        UnixExecImageBuildError::ContainsNul {
            field: UnixExecImageField::EnvironmentEntry(0),
            byte_index: 5,
        }
    );

    let std_error = std::process::Command::new("/usr/bin/env")
        .arg("-0")
        .env_clear()
        .env("A", OsString::from_vec(b"bad\0value".to_vec()))
        .output()
        .expect_err("a surviving Rust 1.93 CommandEnv NUL must fail before spawn");
    assert_eq!(std_error.kind(), std::io::ErrorKind::InvalidInput);
}

#[test]
fn node_logical_nul_remains_an_early_normalization_error() {
    let nul = RuntimeString::from_utf16(vec![b'x'.into(), 0, b'y'.into()]);
    let error = NodeSpawnPlan::from_normalized_parts(
        runtime("node"),
        runtime("node"),
        vec![runtime(nul)],
        None,
        ProcessEnvironmentPlan::NodeRuntime(RuntimeEnv::new()),
        false,
        false,
        false,
    )
    .unwrap_err();
    assert_eq!(error.field(), ProcessPlanField::Argument(0));
}

#[test]
fn every_named_allocation_stage_fails_closed_before_a_process_surface_exists() {
    let node = simple_node_plan(RuntimeEnv::new());
    for stage in [
        UnixAllocationStage::RuntimeProjection,
        UnixAllocationStage::AtomStorage,
        UnixAllocationStage::ArgumentStorage,
        UnixAllocationStage::EnvironmentStorage,
        UnixAllocationStage::PointerImage,
    ] {
        assert_eq!(
            UnixExecImage::build_node_with_fault(&node, ProcessEnvironmentGuard::None, stage)
                .unwrap_err(),
            UnixExecImageBuildError::AllocationFailure { stage }
        );
    }

    let native = native_parent_preimage(None);
    let stage = UnixAllocationStage::EnvironmentFilter;
    assert_eq!(
        UnixExecImage::build_rust193_with_fault(
            &native,
            ProcessEnvironmentGuard::SealedNodeNoPreloadV1,
            Ok(vec![b"KEEP=value".to_vec()]),
            stage,
        )
        .unwrap_err(),
        UnixExecImageBuildError::AllocationFailure { stage }
    );

    let overlay = overlay_preimage(Vec::new());
    let stage = UnixAllocationStage::EnvironmentMap;
    assert_eq!(
        UnixExecImage::build_rust193_with_fault(
            &overlay,
            ProcessEnvironmentGuard::None,
            Ok(vec![b"KEEP=value".to_vec()]),
            stage,
        )
        .unwrap_err(),
        UnixExecImageBuildError::AllocationFailure { stage }
    );

    let stage = UnixAllocationStage::SnapshotCopy;
    assert_eq!(
        UnixExecImage::build_rust193_with_actual_snapshot_fault(
            &overlay,
            ProcessEnvironmentGuard::None,
            stage,
        )
        .unwrap_err(),
        UnixExecImageBuildError::AllocationFailure { stage }
    );

    let production = include_str!("unix.rs");
    for forbidden in [
        "std::process::Command",
        "tokio::process::Command",
        "RuntimeProcessContext",
        "acquire_permit",
        "libc::fork",
        "libc::exec",
    ] {
        assert!(
            !production.contains(forbidden),
            "fallible builder unexpectedly owns process/permit operation {forbidden}"
        );
    }
}

#[test]
fn fallible_projection_matches_the_authoritative_runtime_string_projection() {
    let cases = [
        RuntimeString::from("ascii"),
        RuntimeString::from("é水😀"),
        RuntimeString::from_utf16(vec![0xd800]),
        RuntimeString::from_utf16(vec![0xdc00]),
        RuntimeString::from_utf16(vec![0xd800, 0xdc00]),
        RuntimeString::from_utf16(vec![0xd800, 0xd800, 0xdc00]),
    ];
    for value in cases {
        let expected = value.to_node_utf8();
        let image = UnixExecImage::build_node(
            &node_plan(
                runtime("node"),
                runtime("node"),
                vec![runtime(value)],
                None,
                RuntimeEnv::new(),
            ),
            ProcessEnvironmentGuard::None,
        )
        .unwrap();
        assert_eq!(image.argv_storage()[1].as_bytes(), expected);
    }
}

#[test]
fn unix_image_debug_redacts_file_argv_cwd_and_environment_values() {
    let mut environment = RuntimeEnv::new();
    environment.insert(
        RuntimeString::from("SUPER_SECRET_KEY"),
        RuntimeString::from("SUPER_SECRET_VALUE"),
    );
    let image = UnixExecImage::build_node(
        &node_plan(
            runtime("SECRET_FILE"),
            runtime("SECRET_ARGV0"),
            vec![runtime("SECRET_ARG")],
            Some(runtime("SECRET_CWD")),
            environment,
        ),
        ProcessEnvironmentGuard::None,
    )
    .unwrap();
    let debug = format!("{image:?}");
    for secret in [
        "SECRET_FILE",
        "SECRET_ARGV0",
        "SECRET_ARG",
        "SECRET_CWD",
        "SUPER_SECRET_KEY",
        "SUPER_SECRET_VALUE",
    ] {
        assert!(!debug.contains(secret), "debug leaked {secret}");
    }
    assert!(debug.contains("entry_count: 1"));
}

#[test]
fn logical_process_carrier_debug_is_redacted_before_image_construction() {
    let secret = RuntimeString::from("PREIMAGE_SUPER_SECRET");
    let mutation = Rust193EnvMutation::RuntimeSet {
        key: secret.clone(),
        value: secret.clone(),
    };
    let atom = ProcessAtom::Runtime(secret.clone());
    let node = node_plan(
        atom.clone(),
        atom,
        vec![runtime(secret.clone())],
        Some(runtime(secret)),
        RuntimeEnv::new(),
    );
    let preimage = overlay_preimage(vec![mutation.clone()]);

    for debug in [
        format!("{node:?}"),
        format!("{preimage:?}"),
        format!("{mutation:?}"),
    ] {
        assert!(!debug.contains("PREIMAGE_SUPER_SECRET"));
    }
}

#[test]
fn rust193_overlay_matches_pinned_std_command_raw_environment_image() {
    let mutations = rust_overlay_fixture_mutations();
    let parent = rust_overlay_fixture_parent();
    let image = build_rust_with_raw(
        &overlay_preimage(mutations.clone()),
        ProcessEnvironmentGuard::None,
        parent.clone(),
    )
    .unwrap();

    let mut command = std::process::Command::new("/usr/bin/env");
    command.arg("-0").env_clear();
    for entry in parent {
        let Some(equals) = entry[1..]
            .iter()
            .position(|byte| *byte == b'=')
            .map(|position| position + 1)
        else {
            continue;
        };
        command.env(
            OsString::from_vec(entry[..equals].to_vec()),
            OsString::from_vec(entry[equals + 1..].to_vec()),
        );
    }
    for mutation in mutations {
        match mutation {
            Rust193EnvMutation::RuntimeSet { key, value } => {
                command.env(
                    OsString::from_vec(key.to_node_utf8()),
                    OsString::from_vec(value.to_node_utf8()),
                );
            }
            Rust193EnvMutation::RuntimeRemove { key } => {
                command.env_remove(OsString::from_vec(key.to_node_utf8()));
            }
            Rust193EnvMutation::NativeHostSet(overlay) => {
                command.env(
                    OsString::from_vec(overlay.key().to_node_utf8()),
                    OsString::from_vec(overlay.value().as_unix_bytes().to_vec()),
                );
            }
        }
    }
    let output = command.output().expect("pinned Rust std raw env helper");
    assert!(output.status.success(), "env helper failed: {output:?}");

    let mut expected = Vec::new();
    for entry in environment_bytes(&image) {
        expected.extend_from_slice(&entry);
        expected.push(0);
    }
    assert_eq!(output.stdout, expected);
}

#[test]
fn unix_image_source_has_one_fallible_projection_and_a_locked_snapshot() {
    let source = include_str!("unix.rs");
    let syntax = syn::parse_file(source).expect("Unix image source parses");
    let tokens = source
        .parse::<proc_macro2::TokenStream>()
        .expect("Unix image token stream parses");
    assert_eq!(count_identifier(&tokens, "to_node_utf8"), 0);
    assert_eq!(count_identifier(&tokens, "encode_utf8"), 1);
    assert_eq!(count_identifier(&tokens, "code_points"), 1);
    assert_eq!(
        syntax
            .items
            .iter()
            .filter(|item| matches!(item, syn::Item::Fn(item) if item.sig.ident == "project_runtime_string"))
            .count(),
        1,
        "the terminal Node UTF-8 algorithm must have one owner"
    );
    assert_locked_parent_snapshot_shape(&syntax);
    assert_typed_builder_entrypoints(&syntax);
    for forbidden in [".envs(", "HashMap<", "BTreeMap<", "to_string_lossy"] {
        assert!(
            !source.contains(forbidden),
            "Unix image contains {forbidden}"
        );
    }
}

fn count_identifier(tokens: &proc_macro2::TokenStream, expected: &str) -> usize {
    tokens
        .clone()
        .into_iter()
        .map(|token| match token {
            proc_macro2::TokenTree::Ident(identifier) => usize::from(identifier == expected),
            proc_macro2::TokenTree::Group(group) => count_identifier(&group.stream(), expected),
            _ => 0,
        })
        .sum()
}

#[derive(Default)]
struct UnixImageCallVisitor {
    environment_lock: usize,
    parent_environ: usize,
}

impl<'ast> Visit<'ast> for UnixImageCallVisitor {
    fn visit_expr_call(&mut self, call: &'ast syn::ExprCall) {
        let name = match call.func.as_ref() {
            syn::Expr::Path(path) => path.path.segments.last().map(|segment| &segment.ident),
            _ => None,
        };
        if name.is_some_and(|name| name == "with_process_environment_lock") {
            self.environment_lock += 1;
        }
        if name.is_some_and(|name| name == "parent_environ") {
            self.parent_environ += 1;
        }
        visit::visit_expr_call(self, call);
    }
}

fn assert_locked_parent_snapshot_shape(syntax: &syn::File) {
    let function = syntax
        .items
        .iter()
        .find_map(|item| match item {
            syn::Item::Fn(function) if function.sig.ident == "read_parent_environ" => {
                Some(function)
            }
            _ => None,
        })
        .expect("read_parent_environ function");
    let [syn::Stmt::Expr(syn::Expr::Call(lock_call), _)] = function.block.stmts.as_slice() else {
        panic!("read_parent_environ must consist solely of the shared lock call")
    };
    let syn::Expr::Path(lock_path) = lock_call.func.as_ref() else {
        panic!("snapshot lock must be a direct named call")
    };
    assert_eq!(
        lock_path.path.segments.last().unwrap().ident,
        "with_process_environment_lock"
    );
    let [syn::Expr::Closure(snapshot)] = lock_call.args.iter().collect::<Vec<_>>().as_slice()
    else {
        panic!("snapshot lock must receive exactly one closure")
    };

    let mut function_calls = UnixImageCallVisitor::default();
    function_calls.visit_item_fn(function);
    assert_eq!(function_calls.environment_lock, 1);
    assert_eq!(function_calls.parent_environ, 1);

    let mut closure_calls = UnixImageCallVisitor::default();
    closure_calls.visit_expr(snapshot.body.as_ref());
    assert_eq!(closure_calls.parent_environ, 1);
}

fn assert_typed_builder_entrypoints(syntax: &syn::File) {
    let methods = syntax.items.iter().find_map(|item| match item {
        syn::Item::Impl(item)
            if item.trait_.is_none()
                && matches!(item.self_ty.as_ref(), syn::Type::Path(path) if path.path.is_ident("UnixExecImage")) =>
        {
            Some(item)
        }
        _ => None,
    }).unwrap();
    for (method_name, input_name) in [
        ("build_node", "NodeSpawnPlan"),
        ("build_rust193", "Rust193SpawnPreimage"),
    ] {
        let method = methods
            .items
            .iter()
            .find_map(|item| match item {
                syn::ImplItem::Fn(item) if item.sig.ident == method_name => Some(item),
                _ => None,
            })
            .unwrap();
        let syn::FnArg::Typed(input) = &method.sig.inputs[0] else {
            panic!("builder is not a static typed entrypoint")
        };
        let syn::Type::Reference(reference) = input.ty.as_ref() else {
            panic!("builder input must be borrowed")
        };
        let syn::Type::Path(path) = reference.elem.as_ref() else {
            panic!("builder input must be a named carrier")
        };
        assert!(path.path.is_ident(input_name));
    }
}

#[test]
fn actual_parent_snapshot_waits_for_the_shared_environment_lock() {
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (completed_tx, completed_rx) = std::sync::mpsc::channel();
    let preimage = native_parent_preimage(None);

    let contender = with_process_environment_lock(|| {
        let contender = std::thread::spawn(move || {
            entered_tx.send(()).unwrap();
            let image = UnixExecImage::build_rust193(
                &preimage,
                ProcessEnvironmentGuard::SealedNodeNoPreloadV1,
            )
            .expect("actual locked parent snapshot");
            drop(image);
            completed_tx.send(()).unwrap();
        });
        entered_rx.recv().unwrap();
        assert!(
            completed_rx
                .recv_timeout(std::time::Duration::from_millis(50))
                .is_err(),
            "live environ was traversed while the shared mutation lock was held"
        );
        contender
    });

    completed_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .unwrap();
    contender.join().unwrap();
}
