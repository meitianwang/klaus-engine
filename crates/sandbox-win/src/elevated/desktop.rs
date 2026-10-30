// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified for native request paths, helper packaging, and execution lifecycle.

use crate::elevated::deny_read_acl::plan_deny_read_acl_paths;
use crate::elevated::logging;
use crate::elevated::path_normalization::canonicalize_path;
use crate::elevated::setup::build_payload_deny_write_paths;
use crate::elevated::setup::build_payload_roots;
use crate::elevated::setup::gather_read_roots;
use crate::elevated::setup::SandboxSetupRequest;
use crate::elevated::setup::SetupRootOverrides;
use crate::elevated::token::get_current_token_for_restriction;
use crate::elevated::token::get_user_sid_bytes;
use crate::elevated::winutil::resolve_sid;
use crate::elevated::winutil::sid_bytes_from_string;
use crate::elevated::winutil::string_from_sid_bytes;
use crate::elevated::winutil::to_wide;
use anyhow::Result;
use native_windows_sys::Win32::Foundation::CloseHandle;
use native_windows_sys::Win32::Foundation::GetLastError;
use native_windows_sys::Win32::Foundation::LocalFree;
use native_windows_sys::Win32::Foundation::HLOCAL;
use native_windows_sys::Win32::Security::Authorization::ConvertStringSecurityDescriptorToSecurityDescriptorW;
use native_windows_sys::Win32::Security::PSECURITY_DESCRIPTOR;
use native_windows_sys::Win32::Security::SECURITY_ATTRIBUTES;
use native_windows_sys::Win32::System::StationsAndDesktops::CloseDesktop;
use native_windows_sys::Win32::System::StationsAndDesktops::CreateDesktopW;
use native_windows_sys::Win32::System::StationsAndDesktops::OpenDesktopW;
use native_windows_sys::Win32::System::StationsAndDesktops::DESKTOP_CREATEMENU;
use native_windows_sys::Win32::System::StationsAndDesktops::DESKTOP_CREATEWINDOW;
use native_windows_sys::Win32::System::StationsAndDesktops::DESKTOP_DELETE;
use native_windows_sys::Win32::System::StationsAndDesktops::DESKTOP_ENUMERATE;
use native_windows_sys::Win32::System::StationsAndDesktops::DESKTOP_HOOKCONTROL;
use native_windows_sys::Win32::System::StationsAndDesktops::DESKTOP_JOURNALPLAYBACK;
use native_windows_sys::Win32::System::StationsAndDesktops::DESKTOP_JOURNALRECORD;
use native_windows_sys::Win32::System::StationsAndDesktops::DESKTOP_READOBJECTS;
use native_windows_sys::Win32::System::StationsAndDesktops::DESKTOP_READ_CONTROL;
use native_windows_sys::Win32::System::StationsAndDesktops::DESKTOP_SWITCHDESKTOP;
use native_windows_sys::Win32::System::StationsAndDesktops::DESKTOP_WRITEOBJECTS;
use native_windows_sys::Win32::System::StationsAndDesktops::DESKTOP_WRITE_DAC;
use native_windows_sys::Win32::System::StationsAndDesktops::DESKTOP_WRITE_OWNER;
use native_windows_sys::Win32::System::StationsAndDesktops::HDESK;
use rand::rngs::SmallRng;
use rand::Rng;
use rand::SeedableRng;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::ptr;
use std::sync::Mutex;
use std::sync::OnceLock;

const PRIVATE_DESKTOP_PREFIX: &str = "KlausSandboxDesktop-";

const DESKTOP_ALL_ACCESS: u32 = DESKTOP_READOBJECTS
    | DESKTOP_CREATEWINDOW
    | DESKTOP_CREATEMENU
    | DESKTOP_HOOKCONTROL
    | DESKTOP_JOURNALRECORD
    | DESKTOP_JOURNALPLAYBACK
    | DESKTOP_ENUMERATE
    | DESKTOP_WRITEOBJECTS
    | DESKTOP_SWITCHDESKTOP
    | DESKTOP_DELETE
    | DESKTOP_READ_CONTROL
    | DESKTOP_WRITE_DAC
    | DESKTOP_WRITE_OWNER;

const DESKTOP_PARTICIPANT_ACCESS: u32 =
    DESKTOP_ALL_ACCESS & !(DESKTOP_WRITE_DAC | DESKTOP_WRITE_OWNER | DESKTOP_DELETE);

static SHARED_PRIVATE_DESKTOPS: OnceLock<Mutex<HashMap<(String, DesktopPolicy), PrivateDesktop>>> =
    OnceLock::new();

#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct DesktopPolicy {
    uses_write_capabilities: bool,
    capability_sids: BTreeSet<Vec<u8>>,
    network_enabled: bool,
    network_proxy_restricting_sid: Option<Vec<u8>>,
    // None denotes the legacy backend's unrestricted reads and different desktop ACLs.
    read_roots: Option<BTreeSet<PathBuf>>,
    write_roots: BTreeSet<PathBuf>,
    deny_read_paths: BTreeSet<PathBuf>,
    deny_write_paths: BTreeSet<PathBuf>,
}

impl DesktopPolicy {
    pub(crate) fn elevated(
        request: SandboxSetupRequest<'_>,
        mut overrides: SetupRootOverrides,
        capability_sids: &[String],
        network_proxy_restricting_sid: Option<&str>,
    ) -> Result<Self> {
        // Match the complete read override passed by credential setup to the ACL helper.
        overrides.read_roots.get_or_insert_with(|| {
            gather_read_roots(
                request.command_cwd,
                request.permissions,
                request.config_home,
            )
        });
        let (read_roots, write_roots) = build_payload_roots(&request, &overrides);
        Ok(Self {
            uses_write_capabilities: request.permissions.uses_write_capabilities(),
            capability_sids: capability_sids
                .iter()
                .map(|sid| sid_bytes_from_string(sid))
                .collect::<Result<_>>()?,
            network_enabled: request.permissions.network_policy().is_enabled(),
            network_proxy_restricting_sid: network_proxy_restricting_sid
                .map(sid_bytes_from_string)
                .transpose()?,
            read_roots: Some(read_roots.into_iter().collect()),
            write_roots: write_roots.into_iter().collect(),
            deny_read_paths: plan_deny_read_acl_paths(
                overrides.deny_read_paths.as_deref().unwrap_or_default(),
            )?
            .into_iter()
            .collect(),
            deny_write_paths: build_payload_deny_write_paths(&request, overrides.deny_write_paths)
                .into_iter()
                .map(|path| canonicalize_path(&path))
                .collect(),
        })
    }
}

pub struct LaunchDesktop {
    _private_desktop: PrivateDesktop,
    startup_name: Vec<u16>,
}

impl LaunchDesktop {
    /// Opens the caller-owned private desktop without creating one or falling back to Default.
    pub fn open_private(name: &str) -> Result<Self> {
        if !name
            .strip_prefix(PRIVATE_DESKTOP_PREFIX)
            .is_some_and(|nonce| {
                !nonce.is_empty()
                    && nonce.len() <= 32
                    && nonce.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
        {
            anyhow::bail!("invalid private desktop name");
        }
        let name_wide = to_wide(name);
        let handle = unsafe {
            OpenDesktopW(
                name_wide.as_ptr(),
                /*dwflags*/ 0,
                /*finherit*/ 0,
                DESKTOP_PARTICIPANT_ACCESS,
            )
        };
        if handle.is_null() {
            anyhow::bail!("OpenDesktopW failed: {}", unsafe { GetLastError() });
        }
        Ok(Self {
            _private_desktop: PrivateDesktop {
                handle,
                name: name.to_owned(),
            },
            startup_name: to_wide(format!("Winsta0\\{name}")),
        })
    }

    pub fn startup_info_desktop(&self) -> *mut u16 {
        self.startup_name.as_ptr() as *mut u16
    }
}

/// Reuses a private desktop only for the same sandbox account and effective permissions.
pub(crate) fn shared_private_desktop_for_user(
    sandbox_username: &str,
    policy: &DesktopPolicy,
    logs_base_dir: Option<&Path>,
) -> Result<String> {
    let sandbox_sid =
        string_from_sid_bytes(&resolve_sid(sandbox_username)?).map_err(anyhow::Error::msg)?;
    let mut desktops = SHARED_PRIVATE_DESKTOPS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .map_err(|_| anyhow::anyhow!("shared private desktop cache was poisoned"))?;
    let key = (sandbox_sid.clone(), policy.clone());
    if let Some(desktop) = desktops.get(&key) {
        return Ok(desktop.name.clone());
    }

    let owner_user_sid = unsafe {
        let token = get_current_token_for_restriction()?;
        let sid = get_user_sid_bytes(token);
        CloseHandle(token);
        sid?
    };
    let owner_user_sid = string_from_sid_bytes(&owner_user_sid).map_err(anyhow::Error::msg)?;
    // CreateProcessWithLogonW shares the caller's logon SID with the sandbox account.
    // Grant ACL-management rights to the caller's user SID instead.
    // https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-createprocesswithlogonw
    let sddl = to_wide(format!(
        "D:P(A;;0x{DESKTOP_ALL_ACCESS:x};;;{owner_user_sid})(A;;0x{DESKTOP_PARTICIPANT_ACCESS:x};;;{sandbox_sid})"
    ));
    let mut security_descriptor: PSECURITY_DESCRIPTOR = ptr::null_mut();
    if unsafe {
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            sddl.as_ptr(),
            /*stringsdrevision*/ 1,
            &mut security_descriptor,
            ptr::null_mut(),
        )
    } == 0
    {
        anyhow::bail!(
            "ConvertStringSecurityDescriptorToSecurityDescriptorW failed: {}",
            unsafe { GetLastError() }
        );
    }

    let attributes = SECURITY_ATTRIBUTES {
        nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: security_descriptor,
        bInheritHandle: 0,
    };
    let mut rng = SmallRng::from_entropy();
    let name = format!("{PRIVATE_DESKTOP_PREFIX}{:032x}", rng.r#gen::<u128>());
    let name_wide = to_wide(&name);
    let handle = unsafe {
        CreateDesktopW(
            name_wide.as_ptr(),
            ptr::null(),
            ptr::null_mut(),
            /*dwflags*/ 0,
            DESKTOP_ALL_ACCESS,
            &attributes,
        )
    };
    let error = unsafe { GetLastError() };
    unsafe {
        LocalFree(security_descriptor as HLOCAL);
    }
    if handle.is_null() {
        logging::debug_log(
            &format!("CreateDesktopW failed for shared private desktop: {error}"),
            logs_base_dir,
        );
        anyhow::bail!("CreateDesktopW failed for shared private desktop: {error}");
    }

    // Retain ownership across runner exits and idle gaps; different policies stay on separate
    // desktops so GUI hooks do not automatically cross those policies.
    desktops.insert(
        key,
        PrivateDesktop {
            handle,
            name: name.clone(),
        },
    );
    Ok(name)
}

struct PrivateDesktop {
    handle: HDESK,
    name: String,
}

// SAFETY: HDESK is an opaque Windows handle, and this owner only uses it through
// Windows APIs. After setup, its only handle operation is CloseDesktop, which
// reports failure if a thread in the calling process is using the handle.
unsafe impl Send for PrivateDesktop {}

impl Drop for PrivateDesktop {
    fn drop(&mut self) {
        unsafe {
            if !self.handle.is_null() {
                let _ = CloseDesktop(self.handle);
            }
        }
    }
}
