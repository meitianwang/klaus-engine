// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified for captured permission adapter regressions.

use super::*;
use std::collections::BTreeMap;

fn path(value: &str) -> AbsPath {
    AbsPath::new(value).unwrap()
}

fn request() -> SandboxRequest {
    SandboxRequest {
        command: vec!["cmd.exe".into(), "/c".into(), "echo ready".into()],
        cwd: path(r"C:\workspace"),
        env: BTreeMap::new(),
        filesystem: FilesystemBoundary {
            readable: None,
            writable: Vec::new(),
            deny_write: Vec::new(),
            deny_read: Vec::new(),
            protected_metadata: vec![".git".into(), ".klaus".into()],
            metadata_write_allow: Vec::new(),
        },
        network: NetworkBoundary::Denied,
        network_route_capability_sid: None,
        container_name: "native-permissions-test".into(),
        terminal: None,
    }
}

#[test]
fn read_only_permissions_keep_ordinary_reads_without_creating_write_roots() {
    let captured = request();
    let permissions = ResolvedWindowsSandboxPermissions::from_request(&captured).unwrap();
    assert!(permissions.has_full_disk_read_access());
    assert!(permissions.has_symbolic_root_read_access());
    assert!(permissions.include_platform_defaults());
    assert!(permissions.readable_roots().is_empty());
    assert!(permissions.writable_roots().is_empty());
    assert!(!permissions.uses_write_capabilities());
    assert!(permissions.should_apply_network_block());
    assert_eq!(
        token_mode_for_permissions(&permissions, Path::new(r"C:\workspace")).unwrap(),
        WindowsSandboxTokenMode::ReadOnlyCapability
    );
}

#[test]
fn unsupported_read_ceiling_and_root_denial_fail_before_native_setup() {
    let mut captured = request();
    captured.filesystem.readable = Some(vec![path(r"C:\workspace")]);
    assert!(ResolvedWindowsSandboxPermissions::from_request(&captured)
        .unwrap_err()
        .to_string()
        .contains("filesystem-root read access"));
    captured.filesystem.readable = None;
    captured.filesystem.deny_read = vec![path(r"C:\")];
    assert!(ResolvedWindowsSandboxPermissions::from_request(&captured).is_err());
    captured.filesystem.deny_read.clear();
    captured.filesystem.writable = vec![path(r"C:\")];
    assert!(ResolvedWindowsSandboxPermissions::from_request(&captured)
        .unwrap_err()
        .to_string()
        .contains("Full-disk filesystem writes"));
}

#[test]
fn concrete_root_read_and_read_denials_survive_serialization() {
    let mut captured = request();
    captured.filesystem.readable = Some(vec![path(r"C:\")]);
    captured.filesystem.deny_read = vec![path(r"C:\private")];
    let permissions = ResolvedWindowsSandboxPermissions::from_request(&captured).unwrap();
    assert!(!permissions.has_full_disk_read_access());
    assert!(!permissions.has_symbolic_root_read_access());
    assert!(!permissions.include_platform_defaults());
    assert_eq!(permissions.readable_roots(), vec![PathBuf::from(r"C:\")]);
    let encoded = serde_json::to_vec(&permissions).unwrap();
    let decoded: ResolvedWindowsSandboxPermissions = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded, permissions);
    assert_eq!(decoded.filesystem.deny_read, captured.filesystem.deny_read);
}

#[test]
fn writable_capabilities_retain_explicit_denials_and_named_metadata() {
    let mut captured = request();
    captured.filesystem.writable = vec![path(r"C:\workspace"), path(r"D:\build")];
    captured.filesystem.deny_write = vec![path(r"C:\workspace\locked")];
    let permissions = ResolvedWindowsSandboxPermissions::from_request(&captured).unwrap();
    let roots = permissions.writable_roots();
    assert_eq!(roots.len(), 2);
    assert_eq!(
        roots[0].read_only_subpaths,
        vec![
            PathBuf::from(r"C:\workspace\locked"),
            PathBuf::from(r"C:\workspace\.git"),
            PathBuf::from(r"C:\workspace\.klaus")
        ]
    );
    assert_eq!(
        roots[1].read_only_subpaths,
        vec![
            PathBuf::from(r"D:\build\.git"),
            PathBuf::from(r"D:\build\.klaus")
        ]
    );
    assert_eq!(
        token_mode_for_permissions(&permissions, Path::new(r"C:\workspace")).unwrap(),
        WindowsSandboxTokenMode::WritableRootsCapability
    );
}

#[test]
fn metadata_exemptions_match_exact_protected_roots() {
    let mut captured = request();
    captured.filesystem.writable = vec![path(r"C:\workspace")];
    captured.filesystem.metadata_write_allow = vec![path(r"C:\workspace\.git\index")];
    let permissions = ResolvedWindowsSandboxPermissions::from_request(&captured).unwrap();
    assert_eq!(
        permissions.writable_roots()[0].read_only_subpaths,
        vec![
            PathBuf::from(r"C:\workspace\.git"),
            PathBuf::from(r"C:\workspace\.klaus")
        ]
    );
    captured.filesystem.metadata_write_allow = vec![path(r"C:\workspace\.git")];
    let permissions = ResolvedWindowsSandboxPermissions::from_request(&captured).unwrap();
    assert_eq!(
        permissions.writable_roots()[0].read_only_subpaths,
        vec![PathBuf::from(r"C:\workspace\.klaus")]
    );
}

#[test]
fn captured_network_scope_is_authoritative_over_proxy_environment() {
    let mut captured = request();
    captured
        .env
        .insert("HTTP_PROXY".into(), "http://127.0.0.1:9999".into());
    let denied = ResolvedWindowsSandboxPermissions::from_request(&captured).unwrap();
    assert!(denied.offline_proxy_settings().proxy_ports.is_empty());
    assert!(!denied.offline_proxy_settings().allow_local_binding);
    captured.network = NetworkBoundary::LoopbackPorts {
        ports: vec![4321, 1234],
        allow_local_binding: true,
    };
    let scoped = ResolvedWindowsSandboxPermissions::from_request(&captured).unwrap();
    assert_eq!(
        scoped.offline_proxy_settings().proxy_ports,
        vec![4321, 1234]
    );
    assert!(scoped.offline_proxy_settings().allow_local_binding);
    assert!(!scoped.network_policy().is_enabled());
    captured.network = NetworkBoundary::Allowed;
    let allowed = ResolvedWindowsSandboxPermissions::from_request(&captured).unwrap();
    assert!(allowed.network_policy().is_enabled());
    assert!(!allowed.should_apply_network_block());
}
