//! Write-restricted native Windows execution with ordinary caller read access.
//!
//! Network-disabled execution configures proxy and offline environment variables.
//! This native level does not provide a kernel network boundary.

use crate::{FilesystemBoundary, SandboxRequest};

pub mod env;
#[cfg(windows)]
mod imp;
#[cfg(windows)]
pub(crate) use imp::prepare;

/// Prepare the caller's native token and write ACLs without launching a workload.
#[cfg(windows)]
pub async fn prepare_native(
    mut request: SandboxRequest,
    config_home: std::path::PathBuf,
) -> Result<(), String> {
    tokio::task::spawn_blocking(move || {
        imp::prepare(&mut request, &config_home)
            .map(drop)
            .map_err(|error| error.to_string())
    })
    .await
    .map_err(|error| format!("Native Windows preparation worker failed: {error}"))?
}

/// Validate the native level before preparing any token or filesystem permission.
pub fn validate(request: &SandboxRequest) -> Result<(), String> {
    if request.filesystem.readable.is_some() {
        return Err(
            "Restricted read-only access requires the elevated Windows sandbox backend".into(),
        );
    }
    if !request.filesystem.deny_read.is_empty() {
        return Err("deny-read overrides require the elevated Windows sandbox backend".into());
    }
    Ok(())
}

/// Resolve write grants and existing metadata carveouts without introducing reads.
pub fn write_paths(
    filesystem: &FilesystemBoundary,
) -> Result<(Vec<crate::AbsPath>, Vec<crate::AbsPath>), String> {
    let mut denied = filesystem.deny_write.clone();
    for root in &filesystem.writable {
        for name in &filesystem.protected_metadata {
            let path = root.join(name).map_err(|error| error.to_string())?;
            if filesystem.metadata_write_allow.contains(&path) {
                continue;
            }
            if std::path::Path::new(&path.as_str()).exists() && !denied.contains(&path) {
                denied.push(path);
            }
        }
    }
    Ok((filesystem.writable.clone(), denied))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    fn request() -> SandboxRequest {
        SandboxRequest {
            command: vec!["not executed".into()],
            cwd: crate::AbsPath::new(r"C:\command").unwrap(),
            env: BTreeMap::new(),
            filesystem: FilesystemBoundary::default(),
            network: crate::NetworkBoundary::Denied,
            network_route_capability_sid: None,
            container_name: "native-policy-fixture".into(),
            terminal: None,
        }
    }
    #[test]
    fn ordinary_reads_do_not_require_any_read_roots_or_write_grants() {
        let request = request();
        assert!(validate(&request).is_ok());
        assert!(write_paths(&request.filesystem).unwrap().0.is_empty());
    }
    #[test]
    fn native_full_disk_writes_are_rejected_before_any_preparation() {
        let context = crate::protocol::NativeSandboxContext {
            level: crate::protocol::NativeSandboxLevel::Unelevated,
            config_home: r"C:\config".into(),
            unconfined_writes: true,
        };
        assert!(context.validate().unwrap_err().contains("full-disk"));
        let envelope = crate::protocol::NativeSandboxRequest {
            request: request(),
            native: Some(context),
        };
        assert!(crate::preparation::inspect_native(&envelope)
            .unwrap_err()
            .contains("full-disk"));
    }
    #[test]
    fn native_transport_preserves_level_and_full_disk_write_rejection() {
        let envelope = crate::protocol::NativeSandboxRequest {
            request: request(),
            native: Some(crate::protocol::NativeSandboxContext {
                level: crate::protocol::NativeSandboxLevel::Unelevated,
                config_home: r"C:\config".into(),
                unconfined_writes: true,
            }),
        };
        let encoded = serde_json::to_vec(&envelope).unwrap();
        let decoded: crate::protocol::NativeSandboxRequest =
            serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, envelope);
        assert!(decoded.native.unwrap().validate().is_err());
    }
    #[test]
    fn explicit_read_ceilings_require_the_elevated_native_level() {
        let mut request = request();
        request.filesystem.readable = Some(Vec::new());
        assert!(validate(&request).unwrap_err().contains("elevated"));
        request.filesystem.readable = None;
        request.filesystem.deny_read.push(request.cwd.clone());
        assert!(validate(&request).unwrap_err().contains("elevated"));
    }
}

#[cfg(all(test, windows))]
mod preparation_tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn native_preparation_creates_capabilities_without_launching_the_command() {
        let fixture = tempfile::tempdir().unwrap();
        let workspace = fixture.path().join("workspace");
        std::fs::create_dir(&workspace).unwrap();
        let config_home = fixture.path().join("config");
        std::fs::create_dir(&config_home).unwrap();
        let settings = config_home.join("config.toml");
        std::fs::write(&settings, b"existing_setting = true\n").unwrap();
        let marker = workspace.join("command-was-launched");
        let request = SandboxRequest {
            command: vec![
                "cmd.exe".into(),
                "/d".into(),
                "/c".into(),
                "mkdir".into(),
                marker.to_str().unwrap().into(),
            ],
            cwd: crate::AbsPath::new(workspace.to_str().unwrap()).unwrap(),
            env: std::env::vars().collect(),
            filesystem: FilesystemBoundary {
                writable: vec![crate::AbsPath::new(workspace.to_str().unwrap()).unwrap()],
                ..Default::default()
            },
            network: crate::NetworkBoundary::Allowed,
            network_route_capability_sid: None,
            container_name: "native-preparation-fixture".into(),
            terminal: None,
        };
        prepare_native(request, config_home.clone()).await.unwrap();
        let capabilities: crate::native::cap::CapSids = serde_json::from_slice(
            &std::fs::read(crate::native::cap::cap_sid_file(&config_home)).unwrap(),
        )
        .unwrap();
        assert_eq!(capabilities.writable_root_by_path.len(), 1);
        assert!(
            !marker.exists(),
            "preparation launched the requested command"
        );
        assert_eq!(
            std::fs::read(&settings).unwrap(),
            b"existing_setting = true\n"
        );
    }
}
