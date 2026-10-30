use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use native_windows_sys::Win32::Foundation::{CloseHandle, HANDLE};

use crate::native::{acl, cap, token};
use crate::{NetworkBoundary, SandboxRequest};

pub(crate) struct PreparedToken(HANDLE);
impl PreparedToken {
    pub(crate) fn as_raw(&self) -> HANDLE {
        self.0
    }
}
impl Drop for PreparedToken {
    fn drop(&mut self) {
        unsafe {
            CloseHandle(self.0);
        }
    }
}

struct RootCapability {
    root: PathBuf,
    sid: token::LocalSid,
}

/// Keep capability ACLs keyed by stable roots so previously granted extra roots
/// cannot expand a later token that does not contain their restricting identities.
pub(crate) fn prepare(request: &mut SandboxRequest, config_home: &Path) -> Result<PreparedToken> {
    super::validate(request).map_err(anyhow::Error::msg)?;
    std::fs::create_dir_all(config_home)?;
    let mut environment: HashMap<_, _> = request.env.clone().into_iter().collect();
    super::env::normalize_null_device_env(&mut environment);
    super::env::ensure_non_interactive_pager(&mut environment);
    if !matches!(request.network, NetworkBoundary::Allowed) {
        eprintln!("Windows unelevated sandbox network restrictions use proxy and offline environment variables; this level does not enforce a kernel network boundary.");
        super::env::apply_no_network_to_env(&mut environment)?;
    }
    if request.network_route_capability_sid.is_some() {
        eprintln!("Windows unelevated execution does not apply AppContainer route capabilities; explicit proxy routing uses the target environment.");
    }
    request.env = environment.into_iter().collect();
    let (allow, deny) = super::write_paths(&request.filesystem).map_err(anyhow::Error::msg)?;
    let cwd_text = request.cwd.as_str();
    let cwd = Path::new(&cwd_text);
    let mut capabilities = Vec::new();
    for root in &allow {
        let root = PathBuf::from(root.as_str());
        if !root.exists() {
            continue;
        }
        let sid = cap::workspace_write_cap_sid_for_root(config_home, cwd, &root)?;
        capabilities.push(RootCapability {
            root,
            sid: token::LocalSid::from_string(&sid)?,
        });
    }
    let cap_sids = cap::load_or_create_cap_sids(config_home)?;
    let readonly = if capabilities.is_empty() {
        Some(token::LocalSid::from_string(&cap_sids.readonly)?)
    } else {
        None
    };
    let base = PreparedToken(unsafe { token::get_current_token_for_restriction()? });
    let restricted = if let Some(readonly) = &readonly {
        unsafe { token::create_readonly_token_with_cap_from(base.as_raw(), readonly.as_ptr())?.0 }
    } else {
        let pointers: Vec<_> = capabilities.iter().map(|root| root.sid.as_ptr()).collect();
        unsafe { token::create_workspace_write_token_with_caps_from(base.as_raw(), &pointers)? }
    };
    let restricted = PreparedToken(restricted);
    for root in &capabilities {
        if let Err(error) =
            unsafe { acl::ensure_allow_write_aces(&root.root, &[root.sid.as_ptr()]) }
        {
            eprintln!("Windows native writable-root ACL preparation failed: {error}");
        }
    }
    for path in &deny {
        let path = PathBuf::from(path.as_str());
        // Explicit carveouts must exist before their inheritable deny is attached.
        if !path.exists() {
            std::fs::create_dir_all(&path)
                .with_context(|| format!("create deny-write path {}", path.display()))?;
        }
        let matching: Vec<_> = capabilities
            .iter()
            .filter(|root| cap::workspace_write_root_overlaps_path(&root.root, &path))
            .collect();
        let affected: Vec<_> = if matching.is_empty() {
            capabilities.iter().collect()
        } else {
            matching
        };
        for root in affected {
            if let Err(error) = unsafe { acl::add_deny_write_ace(&path, root.sid.as_ptr()) } {
                eprintln!("Windows native metadata ACL preparation failed: {error}");
            }
        }
    }
    unsafe {
        for root in &capabilities {
            acl::allow_null_device(root.sid.as_ptr());
        }
        if let Some(readonly) = &readonly {
            acl::allow_null_device(readonly.as_ptr());
        }
        if !capabilities.is_empty() {
            let mut logon = token::get_logon_sid_bytes(base.as_raw())?;
            acl::allow_null_device(logon.as_mut_ptr().cast());
        }
    }
    Ok(restricted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::os::windows::io::AsRawHandle;
    use std::time::{SystemTime, UNIX_EPOCH};
    use windows_sys::Win32::Foundation::{SetHandleInformation, HANDLE_FLAG_INHERIT};

    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let root = std::env::temp_dir()
                .join(format!("klaus-native-token-{}-{nonce}", std::process::id()));
            std::fs::create_dir_all(root.join("workspace/.git")).unwrap();
            std::fs::create_dir_all(root.join("outside")).unwrap();
            std::fs::write(root.join("outside/read"), b"ordinary caller read").unwrap();
            std::fs::write(root.join("workspace/.git/protected"), b"metadata").unwrap();
            Self(root)
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    #[ignore = "internal target invoked only by the native token execution fixture"]
    fn native_target_fixture() {
        let root =
            PathBuf::from(std::env::var_os("KLAUS_NATIVE_TOKEN_FIXTURE").expect("fixture root"));
        assert_eq!(
            std::fs::read(root.join("outside/read")).unwrap(),
            b"ordinary caller read"
        );
        assert!(std::fs::write(root.join("outside/denied"), b"denied").is_err());
        assert!(std::fs::write(root.join("workspace/.git/protected"), b"denied").is_err());
        if std::env::var("KLAUS_NATIVE_TOKEN_MODE").unwrap() == "workspace" {
            std::fs::write(root.join("workspace/allowed"), b"allowed").unwrap();
        } else {
            assert!(std::fs::write(root.join("workspace/denied"), b"denied").is_err());
        }
    }

    #[test]
    fn real_native_tokens_preserve_outside_reads_and_scope_workspace_writes() {
        for mode in ["readonly", "workspace"] {
            let fixture = Fixture::new();
            let workspace =
                crate::AbsPath::new(fixture.0.join("workspace").to_str().unwrap()).unwrap();
            let mut request = SandboxRequest {
                command: Vec::new(),
                cwd: crate::AbsPath::new(fixture.0.join("outside").to_str().unwrap()).unwrap(),
                env: std::env::vars().collect::<BTreeMap<_, _>>(),
                filesystem: crate::FilesystemBoundary {
                    writable: if mode == "workspace" {
                        vec![workspace]
                    } else {
                        Vec::new()
                    },
                    protected_metadata: vec![".git".into()],
                    ..Default::default()
                },
                network: NetworkBoundary::Allowed,
                network_route_capability_sid: None,
                container_name: "native-execution-fixture".into(),
                terminal: None,
            };
            request.env.insert(
                "KLAUS_NATIVE_TOKEN_FIXTURE".into(),
                fixture.0.to_str().unwrap().into(),
            );
            request
                .env
                .insert("KLAUS_NATIVE_TOKEN_MODE".into(), mode.into());
            let token = prepare(&mut request, &fixture.0.join("config"))
                .expect("prepare native token and source ACLs");
            let null = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open("NUL")
                .unwrap();
            let handle = null.as_raw_handle() as windows_sys::Win32::Foundation::HANDLE;
            assert_ne!(
                unsafe { SetHandleInformation(handle, HANDLE_FLAG_INHERIT, HANDLE_FLAG_INHERIT) },
                0
            );
            let target = crate::process::launch(crate::process::LaunchSpec {
                command_line: crate::process::command_line(&[
                    std::env::current_exe().unwrap().to_str().unwrap().into(),
                    "--exact".into(),
                    "unelevated::imp::tests::native_target_fixture".into(),
                    "--ignored".into(),
                    "--nocapture".into(),
                ]),
                working_directory: request.cwd.as_str(),
                environment: crate::process::environment_block(&request.env),
                identity: crate::process::LaunchIdentity::RestrictedToken(token.as_raw() as _),
                inherit: vec![handle],
                stdin: handle,
                stdout: handle,
                stderr: handle,
                pseudoconsole: None,
            })
            .expect("start real write-restricted target");
            assert_eq!(
                target.wait().expect("drain native target").exit_code,
                0,
                "{mode}"
            );
            assert!(!fixture.0.join("outside/denied").exists());
            assert_eq!(
                fixture.0.join("workspace/allowed").exists(),
                mode == "workspace"
            );
        }
    }
}
