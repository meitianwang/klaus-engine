//! Regression: confined execution must preserve its filesystem authority roots.
//! Root cause: platform transforms added command cwd and scratch mounts after
//! the captured policy had excluded them. Work item: 20261019.

use klaus_agent_core::runtime_process::RuntimeEnv;
use klaus_agent_core::runtime_values::{RuntimePath, RuntimeString};
use klaus_agent_core::sandbox::{create_sandbox_for_mode, policy::compute_writable_roots};
use klaus_agent_core::types::permission::SandboxMode;
use klaus_agent_core::types::sandbox::{SandboxKind, SandboxPolicy};

#[test]
fn canonical_factory_uses_platform_defaults_and_confines_enabled_executors() {
    assert_eq!(
        create_sandbox_for_mode(SandboxMode::DangerFullAccess)
            .unwrap()
            .kind(),
        SandboxKind::Process
    );
    for mode in [SandboxMode::ReadOnly, SandboxMode::WorkspaceWrite] {
        match create_sandbox_for_mode(mode) {
            Ok(backend) => {
                if cfg!(windows) {
                    assert_eq!(backend.kind(), SandboxKind::Process);
                } else {
                    assert_ne!(backend.kind(), SandboxKind::Process);
                }
            }
            Err(message) => assert!(message.contains("sandbox")),
        }
    }
}

#[test]
fn windows_writable_roots_respect_captured_cwd_and_temp_exclusions() {
    let mut environment = RuntimeEnv::new();
    environment.insert("TMPDIR".into(), "C:\\scratch".into());
    let policy = SandboxPolicy {
        writable_roots: vec!["C:\\workspace".into()],
        exclude_cwd: Some(true),
        exclude_tmpdir_env_var: Some(true),
        exclude_slash_tmp: Some(true),
        ..Default::default()
    };
    assert_eq!(
        compute_writable_roots(
            &policy,
            &RuntimePath::from("C:\\outside"),
            "windows",
            &environment
        ),
        vec![RuntimeString::from("C:\\workspace")]
    );
    let read_only = SandboxPolicy {
        writable_roots: vec![],
        ..policy
    };
    assert!(
        compute_writable_roots(&read_only, &"C:\\outside".into(), "windows", &environment)
            .is_empty()
    );
}

#[cfg(target_os = "macos")]
#[test]
fn macos_real_execution_preserves_roots_and_uncreated_metadata() {
    use klaus_agent_core::types::sandbox::ExecRequest;
    use std::process::Command;
    let root = std::env::temp_dir().join(format!("klaus-platform-{}", uuid::Uuid::new_v4()));
    let workspace = root.join("workspace");
    let outside = root.join("outside");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(root.clone());
    let runtime = |path: &std::path::Path| RuntimeString::from_utf8(path.to_str().unwrap());
    let backend = create_sandbox_for_mode(SandboxMode::WorkspaceWrite)
        .expect("macOS must provide its actual sandbox executor for this regression");
    let policy = SandboxPolicy {
        writable_roots: vec![runtime(&workspace)],
        exclude_cwd: Some(true),
        exclude_tmpdir_env_var: Some(true),
        exclude_slash_tmp: Some(true),
        network_allowed: true,
        ..Default::default()
    };
    let request = ExecRequest {
        argv: vec!["/bin/sh".into(), "-c".into(),
            "printf allowed > \"$1/allowed\" || exit 10; if printf denied > \"$2/outside\" 2>/dev/null; then exit 11; fi; if mkdir \"$1/.klaus\" 2>/dev/null; then exit 12; fi; if mv \"$1\" \"$2/moved\" 2>/dev/null; then exit 13; fi".into(),
            "sandbox-fixture".into(), runtime(&workspace), runtime(&outside)],
        cwd: RuntimePath::from(runtime(&outside)), env: None, timeout_ms: None,
    };
    let specification = backend.transform(&request, &policy);
    let status = Command::new(specification.file.try_to_utf8().unwrap())
        .args(
            specification
                .args
                .iter()
                .map(|argument| argument.try_to_utf8().unwrap()),
        )
        .current_dir(&outside)
        .status()
        .unwrap();
    assert!(status.success(), "actual sandbox command returned {status}");
    assert_eq!(
        std::fs::read(workspace.join("allowed")).unwrap(),
        b"allowed"
    );
    assert!(!outside.join("outside").exists());
    assert!(!workspace.join(".klaus").exists());
    assert!(!outside.join("moved").exists());
}

#[cfg(target_os = "macos")]
#[test]
fn macos_real_execution_applies_each_temporary_root_exclusion() {
    use klaus_agent_core::types::sandbox::ExecRequest;
    use std::process::Command;
    let fixture = uuid::Uuid::new_v4();
    let root = std::env::temp_dir().join(format!("klaus-temp-policy-{fixture}"));
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let slash_target = std::path::PathBuf::from(format!("/tmp/klaus-temp-policy-{fixture}"));
    let environment_root = std::env::var_os("TMPDIR").map(std::path::PathBuf::from);
    let environment_target = environment_root
        .as_ref()
        .unwrap_or(&root)
        .join(format!("klaus-env-temp-policy-{fixture}"));
    struct Cleanup {
        root: std::path::PathBuf,
        targets: Vec<std::path::PathBuf>,
    }
    impl Drop for Cleanup {
        fn drop(&mut self) {
            for target in &self.targets {
                let _ = std::fs::remove_file(target);
            }
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }
    let _cleanup = Cleanup {
        root,
        targets: vec![slash_target.clone(), environment_target.clone()],
    };
    let slash_root = std::fs::canonicalize("/tmp").unwrap();
    let environment_root = environment_root
        .as_ref()
        .map(|path| std::fs::canonicalize(path).unwrap());
    let environment_target_parent =
        std::fs::canonicalize(environment_target.parent().unwrap()).unwrap();
    let runtime = |path: &std::path::Path| RuntimeString::from_utf8(path.to_str().unwrap());
    let backend = create_sandbox_for_mode(SandboxMode::WorkspaceWrite).unwrap();
    for exclude_environment in [false, true] {
        for exclude_slash in [false, true] {
            for target in [&slash_target, &environment_target] {
                std::fs::write(target, "host control").unwrap();
                std::fs::remove_file(target).unwrap();
            }
            let slash_allowed = !exclude_slash
                || (!exclude_environment
                    && environment_root
                        .as_ref()
                        .is_some_and(|path| slash_root.starts_with(path)));
            let environment_allowed = (!exclude_environment && environment_root.is_some())
                || (!exclude_slash && environment_target_parent.starts_with(&slash_root));
            let policy = SandboxPolicy {
                writable_roots: vec![runtime(&workspace)],
                exclude_cwd: Some(true),
                exclude_tmpdir_env_var: Some(exclude_environment),
                exclude_slash_tmp: Some(exclude_slash),
                network_allowed: false,
                ..Default::default()
            };
            let request = ExecRequest {
                argv: vec![
                    "/bin/sh".into(), "-c".into(),
                    "printf work > \"$1/allowed\" || exit 10; if printf scratch > \"$2\" 2>/dev/null; then actual=1; else actual=0; fi; test \"$actual\" = \"$4\" || exit 11; if printf scratch > \"$3\" 2>/dev/null; then actual=1; else actual=0; fi; test \"$actual\" = \"$5\" || exit 12".into(),
                    "temp-policy-fixture".into(), runtime(&workspace), runtime(&slash_target),
                    runtime(&environment_target), if slash_allowed { "1" } else { "0" }.into(),
                    if environment_allowed { "1" } else { "0" }.into(),
                ],
                cwd: RuntimePath::from(runtime(&workspace)), env: None, timeout_ms: None,
            };
            let specification = backend.transform(&request, &policy);
            let status = Command::new(specification.file.try_to_utf8().unwrap())
                .args(
                    specification
                        .args
                        .iter()
                        .map(|arg| arg.try_to_utf8().unwrap()),
                )
                .current_dir(&workspace)
                .status()
                .unwrap();
            assert!(
                status.success(),
                "temporary-root flags {exclude_environment}/{exclude_slash}: {status}"
            );
            assert_eq!(slash_target.exists(), slash_allowed);
            assert_eq!(environment_target.exists(), environment_allowed);
        }
    }
}
