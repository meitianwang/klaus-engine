use super::*;
use crate::boundary::{AbsPath, FilesystemBoundary, NetworkBoundary};
use std::collections::BTreeMap;
use wxc_common::models::NetworkAction;

fn abs(path: &str) -> AbsPath {
    AbsPath::new(path).unwrap()
}
fn request() -> SandboxRequest {
    SandboxRequest {
        command: vec!["cmd.exe".into(), "/D".into(), "/C".into(), "exit 0".into()],
        cwd: abs(r"C:\workspace"),
        env: BTreeMap::new(),
        filesystem: FilesystemBoundary {
            readable: Some(vec![abs(r"C:\readable")]),
            ..Default::default()
        },
        network: NetworkBoundary::Denied,
        network_route_capability_sid: None,
        container_name: "klaus-native-policy-fixture".into(),
        terminal: None,
    }
}

#[test]
fn read_only_filesystem_and_direct_network_are_independent() {
    let mut input = request();
    input.network = NetworkBoundary::Allowed;
    let native = build_request(&input, MxcOptions::default(), &[], &[]).unwrap();
    assert!(native.policy.readwrite_paths.is_empty());
    assert_eq!(native.policy.readonly_paths, vec![r"C:\readable"]);
    assert_eq!(
        native.policy.network_egress.unwrap().default,
        NetworkAction::Allow
    );
    let ingress = native.policy.network_ingress.unwrap();
    assert_eq!(ingress.default, NetworkAction::Allow);
    assert_eq!(ingress.host_loopback, NetworkAction::Allow);
    assert!(native.policy.allow_local_network);
    assert!(!native.policy.fallback.allow_dacl_mutation);
    assert_eq!(native.policy.capabilities, vec!["registryRead"]);
    assert_eq!(native.policy.base_process_ui.isolation, "desktop");
    assert!(!native.policy.ui.disable);
}

#[test]
fn network_disabled_denies_egress_ingress_and_host_loopback() {
    let native = build_request(&request(), MxcOptions::default(), &[], &[]).unwrap();
    assert_eq!(
        native.policy.network_egress.unwrap().default,
        NetworkAction::Deny
    );
    let ingress = native.policy.network_ingress.unwrap();
    assert_eq!(ingress.default, NetworkAction::Deny);
    assert_eq!(ingress.host_loopback, NetworkAction::Deny);
    assert!(!native.policy.allow_local_network);
}

#[test]
fn managed_network_admits_bidirectional_loopback_without_direct_dns_or_lan() {
    let mut input = request();
    input.network = NetworkBoundary::LoopbackPorts {
        ports: vec![42311, 42312],
        allow_local_binding: true,
    };
    let native = build_request(&input, MxcOptions::default(), &[], &[]).unwrap();
    let egress = native.policy.network_egress.unwrap();
    assert_eq!(egress.default, NetworkAction::Deny);
    assert_eq!(egress.allow.len(), 1);
    assert!(egress.allow[0].ports.is_empty());
    assert_eq!(egress.allow[0].to.len(), 2);
    assert_eq!(egress.allow[0].to[0].cidr.prefix_length, 8);
    assert_eq!(egress.allow[0].to[1].cidr.prefix_length, 128);
    let ingress = native.policy.network_ingress.unwrap();
    assert_eq!(ingress.default, NetworkAction::Deny);
    assert_eq!(ingress.host_loopback, NetworkAction::Allow);
    assert!(!native.policy.allow_local_network);
    for (ports, binding) in [(vec![], true), (vec![0], true), (vec![42311], false)] {
        assert!(validate_managed_network(&ports, binding).is_err());
    }
}

#[test]
fn native_projection_retains_metadata_and_read_denials_without_creating_paths() {
    let mut input = request();
    input.filesystem.writable = vec![abs(r"C:\workspace")];
    input.filesystem.protected_metadata = vec![".git".into(), ".klaus".into()];
    input.filesystem.deny_write = vec![abs(r"C:\workspace\locked")];
    input.filesystem.deny_read = vec![abs(r"C:\readable\secret")];
    let native = build_request(&input, MxcOptions::default(), &[], &[]).unwrap();
    assert_eq!(native.policy.readwrite_paths, vec![r"C:\workspace"]);
    for path in [
        r"C:\workspace\.git",
        r"C:\workspace\.klaus",
        r"C:\workspace\locked",
    ] {
        assert!(native
            .policy
            .readonly_paths
            .iter()
            .any(|value| value == path));
    }
    assert_eq!(native.policy.denied_paths, vec![r"C:\readable\secret"]);
    assert!(!native
        .policy
        .readonly_paths
        .contains(&r"C:\readable\secret".to_owned()));
}

#[test]
fn read_only_carveouts_cannot_grant_reads_outside_a_task_ceiling() {
    let mut input = request();
    input.filesystem.deny_write.push(abs(r"D:\private"));
    let native = build_request(&input, MxcOptions::default(), &[], &[]).unwrap();
    assert!(!native
        .policy
        .readonly_paths
        .contains(&r"D:\private".to_owned()));
}

#[test]
fn windows_temp_symbols_use_filtered_temp_and_tmp_only_when_enabled() {
    let mut input = request();
    input.env.insert("TeMp".into(), r"C:\temp-a".into());
    input.env.insert("TMP".into(), r"C:\temp-b".into());
    input
        .env
        .insert("TMPDIR".into(), r"C:\not-a-windows-temp".into());
    let native = build_request(
        &input,
        MxcOptions {
            include_tmpdir_env_var: true,
            ..Default::default()
        },
        &[],
        &[],
    )
    .unwrap();
    assert_eq!(
        native.policy.readwrite_paths,
        vec![r"C:\temp-a", r"C:\temp-b"]
    );
    assert!(build_request(&input, MxcOptions::default(), &[], &[])
        .unwrap()
        .policy
        .readwrite_paths
        .is_empty());
}

#[test]
fn equal_case_folded_paths_resolve_to_one_effective_access_mode() {
    let mut input = request();
    input.filesystem.readable = Some(vec![abs(r"C:\WORKSPACE"), abs(r"C:\DENIED")]);
    input.filesystem.writable = vec![abs(r"c:\workspace"), abs(r"c:\denied")];
    input.filesystem.deny_read = vec![abs(r"C:\denied")];
    let native = build_request(&input, MxcOptions::default(), &[], &[]).unwrap();
    assert_eq!(native.policy.readwrite_paths.len(), 1);
    assert!(native.policy.readonly_paths.is_empty());
    assert_eq!(native.policy.denied_paths.len(), 1);
}

#[test]
fn bounded_transport_preserves_unicode_and_scrubs_only_launcher_state() {
    let mut input = request();
    input.command.push("🦀".repeat(20_000));
    let mut env = HashMap::from([("SECRET".to_owned(), "private-value".to_owned())]);
    let args = create_command_args(&input, MxcOptions::default(), &mut env).unwrap();
    assert_eq!(args, vec![LAUNCH_ARG]);
    assert!(!args.iter().any(|arg| arg.contains("private-value")));
    let command = transport::decode(&mut env).unwrap();
    assert_eq!(command.request.command, input.command);
    assert_eq!(env.get("SECRET").map(String::as_str), Some("private-value"));
    assert!(!env
        .keys()
        .any(|key| environment_transport::is_key(key.as_ref())));
}

#[test]
fn malformed_transport_is_bounded_and_scrubbed_even_on_failure() {
    let mut env = HashMap::from([
        ("KLAUS_MXC_LAUNCH_COUNT".to_owned(), usize::MAX.to_string()),
        ("KLAUS_MXC_LAUNCH_BYTES".to_owned(), usize::MAX.to_string()),
        ("SECRET".to_owned(), "retained".to_owned()),
    ]);
    assert!(transport::decode(&mut env).is_err());
    assert_eq!(
        env,
        HashMap::from([("SECRET".to_owned(), "retained".to_owned())])
    );
    assert!(environment_transport::encode("", &mut env).is_err());
    assert!(env.contains_key("SECRET"));
}

#[test]
fn empty_original_environment_keeps_sdk_profile_defaults() {
    let mut env = HashMap::new();
    create_command_args(&request(), MxcOptions::default(), &mut env).unwrap();
    assert!(transport::decode(&mut env).unwrap().default_child_env);
    assert!(env.is_empty());
}

#[cfg(not(windows))]
#[test]
fn native_probe_reports_unavailable_off_windows() {
    assert!(!is_available());
}

#[cfg(windows)]
#[test]
fn available_native_executor_enforces_writes_with_ordinary_reads() {
    if !is_available() {
        eprintln!("native MXC unavailable: kernel execution fixture was not exercised");
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let workspace = directory.path().join("workspace");
    let outside = directory.path().join("outside");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    let allowed = workspace.join("created.txt");
    let denied = outside.join("denied.txt");
    let mut input = request();
    input.cwd = abs(&outside.to_string_lossy());
    input.env = std::env::vars().collect();
    input.filesystem.readable = None;
    input.filesystem.writable = vec![abs(&workspace.to_string_lossy())];
    let volumes = super::windows::volume_roots(&outside, &outside).unwrap();
    input.command = vec![
        "cmd.exe".into(),
        "/D".into(),
        "/C".into(),
        format!("echo allowed>\"{}\"", allowed.display()),
    ];
    let native = build_request(&input, MxcOptions::default(), &volumes, &[]).unwrap();
    assert_eq!(super::native::launch(&native).unwrap(), 0);
    assert!(allowed.is_file());
    input.filesystem.writable.clear();
    input.command[3] = format!("echo denied>\"{}\"", denied.display());
    let native = build_request(&input, MxcOptions::default(), &volumes, &[]).unwrap();
    assert_ne!(super::native::launch(&native).unwrap(), 0);
    assert!(!denied.exists());
}
