// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified for native request paths, helper packaging, and execution lifecycle.

//! Privileged account, network, and filesystem preparation for the setup helper.
//! Full and read-ACL setup remain helper entrypoints; only they may launch a read-ACL child.

mod firewall;
mod read_acl_mutex;

use crate::elevated::acquire_sandbox_setup_lock;
use crate::elevated::add_deny_write_ace;
use crate::elevated::convert_string_sid_to_sid;
use crate::elevated::ensure_allow_mask_aces_with_inheritance;
use crate::elevated::ensure_allow_write_aces;
use crate::elevated::extract_setup_failure;
use crate::elevated::hide_newly_created_users;
use crate::elevated::install_wfp_filters;
use crate::elevated::local_user_flags;
use crate::elevated::log_note;
use crate::elevated::log_writer;
use crate::elevated::path_mask_allows;
use crate::elevated::path_write_aces_need_refresh;
use crate::elevated::resolve_sid;
use crate::elevated::sandbox_bin_dir;
use crate::elevated::sandbox_dir;
use crate::elevated::sandbox_secrets_dir;
use crate::elevated::set_local_user_flags;
use crate::elevated::setup_acl_error::record_acl_failure;
use crate::elevated::setup_acl_error::WriteAclOperation;
use crate::elevated::string_from_sid_bytes;
use crate::elevated::sync_persistent_deny_read_acls;
use crate::elevated::to_wide;
use crate::elevated::workspace_write_cap_sid_for_root;
use crate::elevated::workspace_write_root_overlaps_path;
use crate::elevated::write_setup_error_report;
use crate::elevated::SetupErrorCode;
use crate::elevated::SetupErrorReport;
use crate::elevated::SetupFailure;
use crate::elevated::SETUP_VERSION;
use anyhow::Context;
use anyhow::Result;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use native_windows_sys::Win32::Foundation::GetLastError;
use native_windows_sys::Win32::Foundation::LocalFree;
use native_windows_sys::Win32::Foundation::HLOCAL;
use native_windows_sys::Win32::NetworkManagement::NetManagement::UF_ACCOUNTDISABLE;
use native_windows_sys::Win32::Security::Authorization::ConvertStringSidToSidW;
use native_windows_sys::Win32::Security::Authorization::SetEntriesInAclW;
use native_windows_sys::Win32::Security::Authorization::SetNamedSecurityInfoW;
use native_windows_sys::Win32::Security::Authorization::EXPLICIT_ACCESS_W;
use native_windows_sys::Win32::Security::Authorization::GRANT_ACCESS;
use native_windows_sys::Win32::Security::Authorization::SE_FILE_OBJECT;
use native_windows_sys::Win32::Security::Authorization::TRUSTEE_IS_SID;
use native_windows_sys::Win32::Security::Authorization::TRUSTEE_W;
use native_windows_sys::Win32::Security::ACL;
use native_windows_sys::Win32::Security::CONTAINER_INHERIT_ACE;
use native_windows_sys::Win32::Security::DACL_SECURITY_INFORMATION;
use native_windows_sys::Win32::Security::OBJECT_INHERIT_ACE;
use native_windows_sys::Win32::Security::PROTECTED_DACL_SECURITY_INFORMATION;
use native_windows_sys::Win32::Storage::FileSystem::DELETE;
use native_windows_sys::Win32::Storage::FileSystem::FILE_GENERIC_EXECUTE;
use native_windows_sys::Win32::Storage::FileSystem::FILE_GENERIC_READ;
use native_windows_sys::Win32::Storage::FileSystem::FILE_GENERIC_WRITE;
use native_windows_sys::Win32::Storage::FileSystem::FILE_READ_ATTRIBUTES;
use native_windows_sys::Win32::Storage::FileSystem::WRITE_DAC;
use native_windows_sys::Win32::System::Threading::INFINITE;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashSet;
use std::ffi::c_void;
use std::ffi::OsStr;
use std::io::Write;
use std::os::windows::process::CommandExt;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::process::Stdio;
use std::sync::mpsc;

const DENY_ACCESS: i32 = 3;

mod sandbox_users;
use read_acl_mutex::acquire_read_acl_mutex;
use read_acl_mutex::read_acl_mutex_exists;
use sandbox_users::commit_setup_marker;
use sandbox_users::prepare_setup_marker;
use sandbox_users::provision_sandbox_users;
use sandbox_users::resolve_sandbox_users_group_sid;
use sandbox_users::sid_bytes_to_psid;

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Payload {
    version: u32,
    offline_username: String,
    online_username: String,
    config_home: PathBuf,
    command_cwd: PathBuf,
    read_roots: Vec<PathBuf>,
    write_roots: Vec<PathBuf>,
    #[serde(default)]
    deny_read_paths: Vec<PathBuf>,
    #[serde(default)]
    deny_write_paths: Vec<PathBuf>,
    proxy_ports: Vec<u16>,
    #[serde(default)]
    allow_local_binding: bool,
    real_user: String,
    #[serde(default)]
    user_profile: Option<PathBuf>,
    #[serde(default)]
    mode: SetupMode,
    #[serde(default)]
    refresh_only: bool,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
enum SetupMode {
    #[default]
    Full,
    InteractiveProvision,
    ReadAclsOnly,
}

impl SetupMode {
    fn provisions_accounts(self, refresh_only: bool) -> bool {
        match self {
            Self::InteractiveProvision => true,
            Self::Full => !refresh_only,
            Self::ReadAclsOnly => false,
        }
    }
}

#[derive(Clone, Copy)]
enum DaclInheritance {
    Inherited,
    Protected,
}

fn log_line(log: &mut dyn Write, msg: &str) -> Result<()> {
    let ts = chrono::Utc::now().to_rfc3339();
    writeln!(log, "[{ts}] {msg}").map_err(|err| {
        anyhow::Error::new(SetupFailure::new(
            SetupErrorCode::HelperLogFailed,
            format!("failed to write setup log line: {err}"),
        ))
    })?;
    Ok(())
}

fn workspace_write_cap_sids_for_path(
    config_home: &Path,
    command_cwd: &Path,
    write_roots: &[PathBuf],
    path: &Path,
) -> Result<Vec<String>> {
    let mut sid_strs = Vec::new();
    for root in write_roots {
        if workspace_write_root_overlaps_path(root, path) {
            sid_strs.push(workspace_write_cap_sid_for_root(
                config_home,
                command_cwd,
                root,
            )?);
        }
    }
    if sid_strs.is_empty() {
        if write_roots.is_empty() {
            sid_strs.push(workspace_write_cap_sid_for_root(
                config_home,
                command_cwd,
                command_cwd,
            )?);
        } else {
            for root in write_roots {
                sid_strs.push(workspace_write_cap_sid_for_root(
                    config_home,
                    command_cwd,
                    root,
                )?);
            }
        }
    }
    Ok(sid_strs)
}

fn spawn_read_acl_helper(payload: &Payload) -> Result<()> {
    let mut read_payload = payload.clone();
    read_payload.mode = SetupMode::ReadAclsOnly;
    read_payload.refresh_only = true;
    let payload_json = serde_json::to_vec(&read_payload)?;
    let payload_b64 = BASE64.encode(payload_json);
    let exe = std::env::current_exe().context("locate setup helper")?;
    let mut command = Command::new(&exe);
    crate::elevated::launch_environment::configure_command(&mut command, &payload_b64)?;
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .creation_flags(0x08000000) // CREATE_NO_WINDOW
        .spawn()
        .context("spawn read ACL helper")?;
    Ok(())
}

struct ReadAclSubjects<'a> {
    sandbox_group_psid: *mut c_void,
    rx_psids: &'a [*mut c_void],
}

fn apply_read_acls(
    read_roots: &[PathBuf],
    subjects: &ReadAclSubjects<'_>,
    log: &mut dyn Write,
    refresh_errors: &mut Vec<String>,
    access_mask: u32,
    access_label: &str,
    inheritance: u32,
) -> Result<()> {
    for root in read_roots {
        if !root.exists() {
            log_line(
                log,
                &format!("{access_label} root {} missing; skipping", root.display()),
            )?;
            continue;
        }
        let builtin_has = read_mask_allows_or_log(
            root,
            subjects.rx_psids,
            /*label*/ None,
            access_mask,
            access_label,
            refresh_errors,
            log,
        )?;
        if builtin_has {
            continue;
        }
        let sandbox_has = read_mask_allows_or_log(
            root,
            &[subjects.sandbox_group_psid],
            Some("sandbox_group"),
            access_mask,
            access_label,
            refresh_errors,
            log,
        )?;
        if sandbox_has {
            continue;
        }
        log_line(
            log,
            &format!(
                "granting {access_label} ACE to {} for sandbox users",
                root.display()
            ),
        )?;
        let result = unsafe {
            ensure_allow_mask_aces_with_inheritance(
                root,
                &[subjects.sandbox_group_psid],
                access_mask,
                inheritance,
            )
        };
        if let Err(err) = result {
            refresh_errors.push(format!(
                "grant {access_label} ACE failed on {} for sandbox_group: {err}",
                root.display()
            ));
            log_line(
                log,
                &format!(
                    "grant {access_label} ACE failed on {} for sandbox_group: {err}",
                    root.display()
                ),
            )?;
        }
    }
    Ok(())
}

fn read_mask_allows_or_log(
    root: &Path,
    psids: &[*mut c_void],
    label: Option<&str>,
    read_mask: u32,
    access_label: &str,
    refresh_errors: &mut Vec<String>,
    log: &mut dyn Write,
) -> Result<bool> {
    match path_mask_allows(root, psids, read_mask, /*require_all_bits*/ true) {
        Ok(has) => Ok(has),
        Err(e) => {
            let label_suffix = label
                .map(|value| format!(" for {value}"))
                .unwrap_or_default();
            refresh_errors.push(format!(
                "{access_label} mask check failed on {}{}: {}",
                root.display(),
                label_suffix,
                e
            ));
            log_line(
                log,
                &format!(
                    "{access_label} mask check failed on {}{}: {}; continuing",
                    root.display(),
                    label_suffix,
                    e
                ),
            )?;
            Ok(false)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn lock_sandbox_dir(
    dir: &Path,
    real_user: &str,
    sandbox_group_sid: &[u8],
    sandbox_group_access_mode: i32,
    sandbox_group_mask: u32,
    real_user_mask: u32,
    dacl_inheritance: DaclInheritance,
) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    let system_sid = resolve_sid("SYSTEM")?;
    let admins_sid = resolve_sid("Administrators")?;
    let real_sid = resolve_sid(real_user)?;
    let entries = [
        (
            sandbox_group_sid.to_vec(),
            sandbox_group_mask,
            sandbox_group_access_mode,
        ),
        (
            system_sid,
            FILE_GENERIC_READ | FILE_GENERIC_WRITE | FILE_GENERIC_EXECUTE | DELETE,
            GRANT_ACCESS,
        ),
        (
            admins_sid,
            FILE_GENERIC_READ | FILE_GENERIC_WRITE | FILE_GENERIC_EXECUTE | DELETE,
            GRANT_ACCESS,
        ),
        (real_sid, real_user_mask, GRANT_ACCESS),
    ];
    unsafe {
        let mut eas: Vec<EXPLICIT_ACCESS_W> = Vec::new();
        let mut sids: Vec<*mut c_void> = Vec::new();
        for (sid_bytes, mask, access_mode) in entries.iter().map(|(s, m, a)| (s, *m, *a)) {
            let sid_str = string_from_sid_bytes(sid_bytes).map_err(anyhow::Error::msg)?;
            let sid_w = to_wide(OsStr::new(&sid_str));
            let mut psid: *mut c_void = std::ptr::null_mut();
            if ConvertStringSidToSidW(sid_w.as_ptr(), &mut psid) == 0 {
                return Err(anyhow::anyhow!(
                    "ConvertStringSidToSidW failed: {}",
                    GetLastError()
                ));
            }
            sids.push(psid);
            eas.push(EXPLICIT_ACCESS_W {
                grfAccessPermissions: mask,
                grfAccessMode: access_mode,
                grfInheritance: OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE,
                Trustee: TRUSTEE_W {
                    pMultipleTrustee: std::ptr::null_mut(),
                    MultipleTrusteeOperation: 0,
                    TrusteeForm: TRUSTEE_IS_SID,
                    TrusteeType: TRUSTEE_IS_SID,
                    ptstrName: psid as *mut u16,
                },
            });
        }
        let mut new_dacl: *mut ACL = std::ptr::null_mut();
        let set = SetEntriesInAclW(
            eas.len() as u32,
            eas.as_ptr(),
            std::ptr::null_mut(),
            &mut new_dacl,
        );
        if set != 0 {
            return Err(anyhow::anyhow!(
                "SetEntriesInAclW sandbox dir failed: {set}",
            ));
        }
        let security_information = match dacl_inheritance {
            DaclInheritance::Inherited => DACL_SECURITY_INFORMATION,
            DaclInheritance::Protected => {
                DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION
            }
        };
        let path_w = to_wide(dir.as_os_str());
        let res = SetNamedSecurityInfoW(
            path_w.as_ptr() as *mut u16,
            SE_FILE_OBJECT,
            security_information,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            new_dacl,
            std::ptr::null_mut(),
        );
        if res != 0 {
            return Err(anyhow::anyhow!(
                "SetNamedSecurityInfoW sandbox dir failed: {res}"
            ));
        }
        if !new_dacl.is_null() {
            LocalFree(new_dacl as HLOCAL);
        }
        for sid in sids {
            if !sid.is_null() {
                LocalFree(sid as HLOCAL);
            }
        }
    }
    Ok(())
}

pub fn main() -> Result<()> {
    let mut setup_mode = None;
    let ret = real_main(&mut setup_mode);
    if let Err(e) = &ret {
        if setup_mode.is_some() {
            // Best-effort: log unexpected top-level errors.
            if let Ok(config_home) = std::env::var("KLAUS_HOME") {
                let sbx_dir = sandbox_dir(Path::new(&config_home));
                let _ = std::fs::create_dir_all(&sbx_dir);
                if let Ok(mut f) = open_setup_log(&sbx_dir) {
                    let _ = writeln!(
                        f,
                        "[{}] top-level error: {}",
                        chrono::Utc::now().to_rfc3339(),
                        e
                    );
                }
            }
        }
    }
    ret
}

fn open_setup_log(sbx_dir: &Path) -> Result<Box<dyn Write>> {
    log_writer(sbx_dir)
        .map(|log| Box::new(log) as Box<dyn Write>)
        .context("open daily sandbox log")
}

fn real_main(setup_mode: &mut Option<SetupMode>) -> Result<()> {
    let mut args = std::env::args().collect::<Vec<_>>();
    if args.len() != 2 {
        return Err(anyhow::Error::new(SetupFailure::new(
            SetupErrorCode::HelperRequestArgsFailed,
            "expected payload argument",
        )));
    }
    let payload_b64 = args.remove(1);
    let payload_b64 = if payload_b64 == crate::elevated::launch_environment::ARG {
        crate::elevated::environment_transport::decode(std::env::vars_os()).map_err(|err| {
            anyhow::Error::new(SetupFailure::new(
                SetupErrorCode::HelperRequestArgsFailed,
                format!("failed to read payload environment: {err}"),
            ))
        })?
    } else {
        payload_b64
    };
    let payload_json = BASE64.decode(payload_b64).map_err(|err| {
        anyhow::Error::new(SetupFailure::new(
            SetupErrorCode::HelperRequestArgsFailed,
            format!("failed to decode payload b64: {err}"),
        ))
    })?;
    let payload: Payload = serde_json::from_slice(&payload_json).map_err(|err| {
        anyhow::Error::new(SetupFailure::new(
            SetupErrorCode::HelperRequestArgsFailed,
            format!("failed to parse payload json: {err}"),
        ))
    })?;
    *setup_mode = Some(payload.mode);
    if payload.version != SETUP_VERSION {
        return Err(anyhow::Error::new(SetupFailure::new(
            SetupErrorCode::HelperRequestArgsFailed,
            format!(
                "setup version mismatch: expected {SETUP_VERSION}, got {}",
                payload.version
            ),
        )));
    }
    // Only this entry parses command-line JSON. Keep admission locked through
    // the entire account-provisioning operation, including initial Full setup.
    let _setup_guard = if payload.mode.provisions_accounts(payload.refresh_only) {
        let guard = acquire_sandbox_setup_lock(INFINITE)?;
        Some(guard)
    } else {
        None
    };
    run_payload(&payload)
}

fn run_payload(payload: &Payload) -> Result<()> {
    let sbx_dir = sandbox_dir(&payload.config_home);
    std::fs::create_dir_all(&sbx_dir).map_err(|err| {
        anyhow::Error::new(SetupFailure::new(
            SetupErrorCode::HelperSandboxDirCreateFailed,
            format!("failed to create sandbox dir {}: {err}", sbx_dir.display()),
        ))
    })?;
    let mut log = open_setup_log(&sbx_dir).map_err(|err| {
        anyhow::Error::new(SetupFailure::new(
            SetupErrorCode::HelperLogFailed,
            format!("open log in {} failed: {err}", sbx_dir.display()),
        ))
    })?;
    let result = run_setup(payload, &mut log, &sbx_dir);
    if let Err(err) = &result {
        let _ = log_line(&mut log, &format!("setup error: {err:?}"));
        log_note(&format!("setup error: {err:?}"), Some(sbx_dir.as_path()));
        let failure = extract_setup_failure(err)
            .map(|f| SetupFailure::new(f.code, f.message.clone()))
            .unwrap_or_else(|| {
                SetupFailure::new(SetupErrorCode::HelperUnknownError, err.to_string())
            });
        let report = SetupErrorReport {
            code: failure.code,
            message: failure.message,
        };
        let write_report = write_setup_error_report(&payload.config_home, &report);
        if let Err(write_err) = write_report {
            let _ = log_line(
                &mut log,
                &format!("setup error report write failed: {write_err}"),
            );
            log_note(
                &format!("setup error report write failed: {write_err}"),
                Some(sbx_dir.as_path()),
            );
        }
    }
    result
}

fn run_setup(payload: &Payload, log: &mut dyn Write, sbx_dir: &Path) -> Result<()> {
    let writes_setup_marker = !payload.refresh_only && payload.mode != SetupMode::ReadAclsOnly;
    if writes_setup_marker {
        prepare_setup_marker(&payload.config_home, &payload.real_user)?;
    }
    match payload.mode {
        SetupMode::ReadAclsOnly => run_read_acl_only(payload, log),
        SetupMode::InteractiveProvision => run_interactive_provision(payload, log, sbx_dir),
        SetupMode::Full => run_setup_full(payload, log, sbx_dir),
    }?;
    if writes_setup_marker {
        commit_setup_marker(
            &payload.config_home,
            &payload.offline_username,
            &payload.online_username,
            &payload.proxy_ports,
            payload.allow_local_binding,
        )?;
    }
    Ok(())
}

fn run_read_acl_only(payload: &Payload, log: &mut dyn Write) -> Result<()> {
    let _read_acl_guard = match acquire_read_acl_mutex()? {
        Some(guard) => guard,
        None => {
            log_line(log, "read ACL helper already running; skipping")?;
            return Ok(());
        }
    };
    log_line(log, "read-acl-only mode: applying read ACLs")?;
    let sandbox_group_sid = resolve_sandbox_users_group_sid()?;
    let sandbox_group_psid = sid_bytes_to_psid(&sandbox_group_sid)?;
    let mut refresh_errors: Vec<String> = Vec::new();
    if !payload.read_roots.is_empty() {
        let users_sid = resolve_sid("Users")?;
        let users_psid = sid_bytes_to_psid(&users_sid)?;
        let auth_sid = resolve_sid("Authenticated Users")?;
        let auth_psid = sid_bytes_to_psid(&auth_sid)?;
        let everyone_sid = resolve_sid("Everyone")?;
        let everyone_psid = sid_bytes_to_psid(&everyone_sid)?;
        let rx_psids = vec![users_psid, auth_psid, everyone_psid];
        let subjects = ReadAclSubjects {
            sandbox_group_psid,
            rx_psids: &rx_psids,
        };
        apply_read_acls(
            &payload.read_roots,
            &subjects,
            log,
            &mut refresh_errors,
            FILE_GENERIC_READ | FILE_GENERIC_EXECUTE,
            "read",
            OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE,
        )?;
        unsafe {
            if !users_psid.is_null() {
                LocalFree(users_psid as HLOCAL);
            }
            if !auth_psid.is_null() {
                LocalFree(auth_psid as HLOCAL);
            }
            if !everyone_psid.is_null() {
                LocalFree(everyone_psid as HLOCAL);
            }
        }
    }
    unsafe {
        if !sandbox_group_psid.is_null() {
            LocalFree(sandbox_group_psid as HLOCAL);
        }
    }
    if !refresh_errors.is_empty() {
        log_line(
            log,
            &format!("read ACL run completed with errors: {refresh_errors:?}"),
        )?;
        if payload.refresh_only {
            anyhow::bail!("read ACL run had errors");
        }
    }
    log_line(log, "read ACL run completed")?;
    Ok(())
}

fn provision_sandbox(payload: &Payload, log: &mut dyn Write, sbx_dir: &Path) -> Result<()> {
    let _setup_lock = acquire_sandbox_setup_lock(INFINITE)?;
    let mut repairing_disabled_accounts = false;
    for username in [&payload.offline_username, &payload.online_username] {
        if local_user_flags(username)?.is_some_and(|flags| flags & UF_ACCOUNTDISABLE != 0) {
            repairing_disabled_accounts = true;
        }
    }
    // Interrupted cleanup can leave one account missing and the other disabled. Keep any
    // replacement disabled too until this repair has restored the network restrictions.
    let new_user_flags = if repairing_disabled_accounts {
        UF_ACCOUNTDISABLE
    } else {
        0
    };
    let provision_result = provision_sandbox_users(
        &payload.config_home,
        &payload.offline_username,
        &payload.online_username,
        new_user_flags,
        log,
    );
    if let Err(err) = provision_result {
        if extract_setup_failure(&err).is_some() {
            return Err(err);
        }
        return Err(anyhow::Error::new(SetupFailure::new(
            SetupErrorCode::HelperUserProvisionFailed,
            format!("provision sandbox users failed: {err}"),
        )));
    }
    let users = vec![
        payload.offline_username.clone(),
        payload.online_username.clone(),
    ];
    hide_newly_created_users(&users, sbx_dir);
    let offline_sid = resolve_sid(&payload.offline_username).map_err(|err| {
        anyhow::Error::new(SetupFailure::new(
            SetupErrorCode::HelperSidResolveFailed,
            format!(
                "resolve SID for offline user {} failed: {err}",
                payload.offline_username
            ),
        ))
    })?;
    let offline_sid_str = string_from_sid_bytes(&offline_sid).map_err(anyhow::Error::msg)?;
    configure_offline_sandbox_network(payload, &offline_sid_str, log)?;
    let wfp_result = install_wfp_filters(&payload.offline_username, |message| {
        let _ = log_line(log, message);
    });
    if repairing_disabled_accounts {
        // Ordinary setup keeps its best-effort WFP behavior. Recovery must not reopen logons
        // after cleanup removed protections unless restoring those protections succeeded.
        wfp_result?;
        for username in [&payload.offline_username, &payload.online_username] {
            let flags = local_user_flags(username)?.ok_or_else(|| {
                anyhow::anyhow!("sandbox user {username} disappeared during repair")
            })?;
            set_local_user_flags(username, flags & !UF_ACCOUNTDISABLE)?;
        }
    }
    Ok(())
}

fn configure_offline_sandbox_network(
    payload: &Payload,
    offline_sid_str: &str,
    log: &mut dyn Write,
) -> Result<()> {
    let proxy_allowlist_result = firewall::ensure_offline_proxy_allowlist(
        offline_sid_str,
        &payload.proxy_ports,
        payload.allow_local_binding,
        log,
    );
    if let Err(err) = proxy_allowlist_result {
        if extract_setup_failure(&err).is_some() {
            return Err(err);
        }
        return Err(anyhow::Error::new(SetupFailure::new(
            SetupErrorCode::HelperFirewallRuleCreateOrAddFailed,
            format!("ensure offline proxy allowlist failed: {err}"),
        )));
    }
    let firewall_result = firewall::ensure_offline_network_blocks(offline_sid_str, log);
    if let Err(err) = firewall_result {
        if extract_setup_failure(&err).is_some() {
            return Err(err);
        }
        return Err(anyhow::Error::new(SetupFailure::new(
            SetupErrorCode::HelperFirewallRuleCreateOrAddFailed,
            format!("ensure offline network blocks failed: {err}"),
        )));
    }
    Ok(())
}

fn lock_persistent_sandbox_dirs(payload: &Payload, sandbox_group_sid: &[u8]) -> Result<()> {
    lock_sandbox_dir(
        &sandbox_dir(&payload.config_home),
        &payload.real_user,
        sandbox_group_sid,
        GRANT_ACCESS,
        FILE_GENERIC_READ | FILE_GENERIC_WRITE | FILE_GENERIC_EXECUTE | DELETE,
        FILE_GENERIC_READ | FILE_GENERIC_WRITE | FILE_GENERIC_EXECUTE,
        DaclInheritance::Inherited,
    )
    .map_err(|err| {
        anyhow::Error::new(SetupFailure::new(
            SetupErrorCode::HelperSandboxLockFailed,
            format!(
                "lock sandbox dir {} failed: {err}",
                sandbox_dir(&payload.config_home).display()
            ),
        ))
    })?;
    lock_sandbox_dir(
        &sandbox_secrets_dir(&payload.config_home),
        &payload.real_user,
        sandbox_group_sid,
        DENY_ACCESS,
        FILE_GENERIC_READ | FILE_GENERIC_WRITE | FILE_GENERIC_EXECUTE | DELETE,
        FILE_GENERIC_READ | FILE_GENERIC_WRITE | FILE_GENERIC_EXECUTE,
        DaclInheritance::Inherited,
    )
    .map_err(|err| {
        anyhow::Error::new(SetupFailure::new(
            SetupErrorCode::HelperSandboxLockFailed,
            format!(
                "lock sandbox secrets dir {} failed: {err}",
                sandbox_secrets_dir(&payload.config_home).display()
            ),
        ))
    })?;
    let legacy_users = sandbox_dir(&payload.config_home).join("sandbox_users.json");
    if legacy_users.exists() {
        let _ = std::fs::remove_file(&legacy_users);
    }
    Ok(())
}

fn lock_sandbox_bin_dir(payload: &Payload, sandbox_group_sid: &[u8]) -> Result<()> {
    // The owner's unelevated refresh must be able to reapply this protected DACL.
    lock_sandbox_dir(
        &sandbox_bin_dir(&payload.config_home),
        &payload.real_user,
        sandbox_group_sid,
        GRANT_ACCESS,
        FILE_GENERIC_READ | FILE_GENERIC_EXECUTE,
        FILE_GENERIC_READ | FILE_GENERIC_WRITE | FILE_GENERIC_EXECUTE | DELETE | WRITE_DAC,
        DaclInheritance::Protected,
    )
    .map_err(|err| {
        anyhow::Error::new(SetupFailure::new(
            SetupErrorCode::HelperSandboxLockFailed,
            format!(
                "lock sandbox bin dir {} failed: {err:#}",
                sandbox_bin_dir(&payload.config_home).display()
            ),
        ))
    })
}

fn run_interactive_provision(payload: &Payload, log: &mut dyn Write, sbx_dir: &Path) -> Result<()> {
    provision_sandbox(payload, log, sbx_dir)?;

    let sandbox_group_sid = resolve_sandbox_users_group_sid().map_err(|err| {
        anyhow::Error::new(SetupFailure::new(
            SetupErrorCode::HelperSidResolveFailed,
            format!("resolve sandbox users group SID failed: {err}"),
        ))
    })?;

    lock_sandbox_bin_dir(payload, &sandbox_group_sid)?;
    lock_persistent_sandbox_dirs(payload, &sandbox_group_sid)?;
    log_note("setup provisioning binary completed", Some(sbx_dir));
    Ok(())
}

fn run_setup_full(payload: &Payload, log: &mut dyn Write, sbx_dir: &Path) -> Result<()> {
    let refresh_only = payload.refresh_only;
    if !refresh_only {
        provision_sandbox(payload, log, sbx_dir)?;
    }

    let sandbox_group_sid = resolve_sandbox_users_group_sid().map_err(|err| {
        anyhow::Error::new(SetupFailure::new(
            SetupErrorCode::HelperSidResolveFailed,
            format!("resolve sandbox users group SID failed: {err}"),
        ))
    })?;
    let sandbox_group_psid = sid_bytes_to_psid(&sandbox_group_sid).map_err(|err| {
        anyhow::Error::new(SetupFailure::new(
            SetupErrorCode::HelperSidResolveFailed,
            format!("convert sandbox users group SID to PSID failed: {err}"),
        ))
    })?;
    let sandbox_group_sid_str =
        string_from_sid_bytes(&sandbox_group_sid).map_err(anyhow::Error::msg)?;

    let mut refresh_errors: Vec<String> = Vec::new();

    // Deny-read ACEs must be present before the sandboxed command starts. Apply
    // them synchronously here instead of delegating them to the background
    // helper used for read grants.
    let applied_deny_read_paths = unsafe {
        sync_persistent_deny_read_acls(
            &payload.config_home,
            &sandbox_group_sid_str,
            &payload.deny_read_paths,
            sandbox_group_psid,
        )
    }
    .context("apply deny-read ACLs")?;
    if !applied_deny_read_paths.is_empty() {
        log_line(
            log,
            &format!("applied {} deny-read ACLs", applied_deny_read_paths.len()),
        )?;
    }

    if let Some(user_profile) = payload.user_profile.as_deref() {
        if user_profile.is_absolute() {
            let mut cwd_components = payload.command_cwd.components();
            let cwd_is_under_profile = user_profile.components().all(|profile_component| {
                cwd_components.next().is_some_and(|cwd_component| {
                    cwd_component
                        .as_os_str()
                        .to_string_lossy()
                        .eq_ignore_ascii_case(&profile_component.as_os_str().to_string_lossy())
                })
            });
            if cwd_is_under_profile {
                match unsafe {
                    ensure_allow_mask_aces_with_inheritance(
                        user_profile,
                        &[sandbox_group_psid],
                        FILE_READ_ATTRIBUTES,
                        /*inheritance*/ 0,
                    )
                } {
                    Ok(true) => {
                        log_line(
                            log,
                            &format!(
                                "granted non-inheriting read-attributes ACE on user profile {}",
                                user_profile.display()
                            ),
                        )?;
                    }
                    Ok(false) => {}
                    Err(err) => {
                        log_line(
                        log,
                        &format!(
                            "failed to grant non-inheriting read-attributes ACE on user profile {}: {err:#}; continuing setup",
                            user_profile.display()
                        ),
                    )?;
                    }
                }
            }
        }
    }

    if payload.read_roots.is_empty() {
        log_line(log, "no read roots to grant; skipping read ACL helper")?;
    } else {
        match read_acl_mutex_exists() {
            Ok(true) => {
                log_line(log, "read ACL helper already running; skipping spawn")?;
            }
            Ok(false) => {
                spawn_read_acl_helper(payload).map_err(|err| {
                    anyhow::Error::new(SetupFailure::new(
                        SetupErrorCode::HelperReadAclHelperSpawnFailed,
                        format!("spawn read ACL helper failed: {err}"),
                    ))
                })?;
            }
            Err(err) => {
                log_line(
                    log,
                    &format!("read ACL mutex check failed: {err}; spawning anyway"),
                )?;
                spawn_read_acl_helper(payload).map_err(|spawn_err| {
                    anyhow::Error::new(SetupFailure::new(
                        SetupErrorCode::HelperReadAclHelperSpawnFailed,
                        format!(
                            "spawn read ACL helper failed after mutex error {err}: {spawn_err}"
                        ),
                    ))
                })?;
            }
        }
    }

    let mut grant_tasks: Vec<(PathBuf, String)> = Vec::new();

    let mut seen_deny_paths: HashSet<PathBuf> = HashSet::new();
    let mut seen_write_roots: HashSet<PathBuf> = HashSet::new();
    for root in &payload.write_roots {
        if !seen_write_roots.insert(root.clone()) {
            continue;
        }
        if !root.exists() {
            log_line(
                log,
                &format!("write root {} missing; skipping", root.display()),
            )?;
            continue;
        }
        let root_cap_sid_str =
            workspace_write_cap_sid_for_root(&payload.config_home, &payload.command_cwd, root)?;
        let root_cap_psid = unsafe {
            convert_string_sid_to_sid(&root_cap_sid_str)
                .ok_or_else(|| anyhow::anyhow!("convert write root capability SID failed"))?
        };
        let need_grant =
            match path_write_aces_need_refresh(root, &[sandbox_group_psid, root_cap_psid]) {
                Ok(needs_refresh) => needs_refresh,
                Err(e) => {
                    record_acl_failure(
                        &mut refresh_errors,
                        WriteAclOperation::Check,
                        root,
                        &e,
                        |message| log_line(log, message),
                    )?;
                    true
                }
            };
        unsafe {
            LocalFree(root_cap_psid as HLOCAL);
        }
        if need_grant {
            log_line(
                log,
                &format!(
                    "granting write ACE to {} for sandbox group and capability SID",
                    root.display()
                ),
            )?;
            grant_tasks.push((root.clone(), root_cap_sid_str));
        }
    }

    let (tx, rx) = mpsc::channel::<(PathBuf, Result<bool>)>();
    std::thread::scope(|scope| {
        for (root, root_cap_sid_str) in grant_tasks {
            let sid_strings = vec![sandbox_group_sid_str.clone(), root_cap_sid_str];
            let tx = tx.clone();
            scope.spawn(move || {
                // Convert SID strings to psids locally in this thread.
                let mut psids: Vec<*mut c_void> = Vec::new();
                for sid_str in &sid_strings {
                    if let Some(psid) = unsafe { convert_string_sid_to_sid(sid_str) } {
                        psids.push(psid);
                    } else {
                        let _ = tx.send((root.clone(), Err(anyhow::anyhow!("convert SID failed"))));
                        return;
                    }
                }

                let res = unsafe { ensure_allow_write_aces(&root, &psids) };

                for psid in psids {
                    unsafe {
                        LocalFree(psid as HLOCAL);
                    }
                }
                let _ = tx.send((root, res));
            });
        }
        drop(tx);
        for (root, res) in rx {
            match res {
                Ok(_) => {}
                Err(e) => {
                    if record_acl_failure(
                        &mut refresh_errors,
                        WriteAclOperation::Grant,
                        &root,
                        &e,
                        |message| log_line(log, message),
                    )
                    .is_err()
                    {
                        // ignore log errors inside scoped thread
                    }
                }
            }
        }
    });

    for path in &payload.deny_write_paths {
        if !seen_deny_paths.insert(path.clone()) {
            continue;
        }

        // These are deny-write carveouts, not deny-read paths. They may come from explicit
        // read-only-under-a-writable-root carveouts in the transformed sandbox policy, or from
        // legacy protected children such as `.git`, `.klaus`, and `.agents`.
        //
        // Deny ACEs attach to filesystem objects; if an explicit policy carveout does not exist
        // during setup, the sandbox could otherwise create it later under a writable parent and
        // bypass the carveout. Materialize missing carveouts as directories so the deny-write ACL
        // is present before the command starts. Legacy protected children are filtered before
        // payload creation, so this should not create sentinel directories in a workspace.
        if !path.exists() {
            std::fs::create_dir_all(path)
                .with_context(|| format!("failed to create deny-write path {}", path.display()))?;
        }

        let deny_sid_strs = workspace_write_cap_sids_for_path(
            &payload.config_home,
            &payload.command_cwd,
            &payload.write_roots,
            path,
        )?;
        for deny_sid_str in deny_sid_strs {
            let deny_psid = unsafe {
                convert_string_sid_to_sid(&deny_sid_str)
                    .ok_or_else(|| anyhow::anyhow!("convert deny capability SID failed"))?
            };

            match unsafe { add_deny_write_ace(path, deny_psid) } {
                Ok(true) => {
                    log_line(
                        log,
                        &format!("applied deny ACE to protect {}", path.display()),
                    )?;
                }
                Ok(false) => {}
                Err(err) => {
                    record_acl_failure(
                        &mut refresh_errors,
                        WriteAclOperation::Deny,
                        path,
                        &err,
                        |message| log_line(log, message),
                    )?;
                }
            }
            unsafe {
                LocalFree(deny_psid as HLOCAL);
            }
        }
    }

    lock_sandbox_bin_dir(payload, &sandbox_group_sid)?;

    if refresh_only {
        log_line(
            log,
            &format!(
                "setup refresh: processed {} write roots (read roots delegated); errors={:?}",
                payload.write_roots.len(),
                refresh_errors
            ),
        )?;
    }
    if !refresh_only {
        lock_persistent_sandbox_dirs(payload, &sandbox_group_sid)?;
    }

    unsafe {
        if !sandbox_group_psid.is_null() {
            LocalFree(sandbox_group_psid as HLOCAL);
        }
    }
    if refresh_only && !refresh_errors.is_empty() {
        log_line(
            log,
            &format!("setup refresh completed with errors: {refresh_errors:?}"),
        )?;
        anyhow::bail!("setup refresh had errors");
    }
    log_note("setup binary completed", Some(sbx_dir));
    Ok(())
}
