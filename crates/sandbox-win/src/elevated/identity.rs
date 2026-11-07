// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified for native request paths, helper packaging, and execution lifecycle.

use crate::elevated::dpapi;
use crate::elevated::logging::debug_log;
use crate::elevated::resolved_permissions::ResolvedWindowsSandboxPermissions;
use crate::elevated::setup::gather_read_roots;
use crate::elevated::setup::gather_write_roots_for_permissions;
use crate::elevated::setup::run_elevated_setup_with_proxy_settings;
use crate::elevated::setup::run_setup_refresh_with_overrides_and_proxy_settings;
use crate::elevated::setup::sandbox_users_path;
use crate::elevated::setup::setup_marker_path;
use crate::elevated::setup::OfflineProxySettings;
use crate::elevated::setup::SandboxNetworkIdentity;
use crate::elevated::setup::SandboxSetupRequest;
use crate::elevated::setup::SandboxUserRecord;
use crate::elevated::setup::SandboxUsersFile;
use crate::elevated::setup::SetupMarker;
use crate::elevated::setup::OFFLINE_USERNAME;
use crate::elevated::setup::ONLINE_USERNAME;
use crate::elevated::winutil::local_user_flags;
use anyhow::anyhow;
use anyhow::Context;
use anyhow::Result;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use native_windows_sys::Win32::NetworkManagement::NetManagement::UF_ACCOUNTDISABLE;
use native_windows_sys::Win32::NetworkManagement::NetManagement::UF_PASSWORD_EXPIRED;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

#[derive(Debug, Clone)]
struct SandboxIdentity {
    username: String,
    password: String,
}

#[derive(Debug, Clone)]
pub struct SandboxCreds {
    pub username: String,
    pub password: String,
}

/// Returns true when the on-disk setup artifacts exist and match the current
/// setup version.
///
/// This is a coarse readiness check; `require_logon_sandbox_creds` performs the
/// additional runtime validation for offline firewall settings.
pub fn sandbox_setup_is_complete(config_home: &Path) -> bool {
    let marker_ok =
        matches!(load_marker(config_home), Ok(Some(marker)) if marker.version_matches());
    if !marker_ok {
        return false;
    }

    matches!(load_users(config_home), Ok(Some(users)) if users.version_matches())
}

/// Returns true when setup artifacts and provisioned network settings match.
pub fn sandbox_setup_is_complete_with_settings(
    config_home: &Path,
    settings: &crate::elevated::WindowsSandboxProvisioningSettings,
) -> bool {
    let Ok(Some(mut marker)) = load_marker(config_home) else {
        return false;
    };

    marker.proxy_ports.sort_unstable();
    let mut proxy_ports = settings.proxy_ports.clone();
    proxy_ports.sort_unstable();
    marker.version_matches()
        && marker.proxy_ports == proxy_ports
        && marker.allow_local_binding == settings.allow_local_binding
        && matches!(load_users(config_home), Ok(Some(users)) if users.version_matches())
}

fn load_marker(config_home: &Path) -> Result<Option<SetupMarker>> {
    let path = setup_marker_path(config_home);
    let marker = match fs::read_to_string(&path) {
        Ok(contents) => match serde_json::from_str::<SetupMarker>(&contents) {
            Ok(m) => Some(m),
            Err(err) => {
                debug_log(
                    &format!("sandbox setup marker parse failed: {err}"),
                    Some(config_home),
                );
                None
            }
        },
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
        Err(err) => {
            debug_log(
                &format!("sandbox setup marker read failed: {err}"),
                Some(config_home),
            );
            None
        }
    };
    Ok(marker)
}

fn load_users(config_home: &Path) -> Result<Option<SandboxUsersFile>> {
    let path = sandbox_users_path(config_home);
    let file = match fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            debug_log(
                &format!("sandbox users read failed: {err}"),
                Some(config_home),
            );
            return Ok(None);
        }
    };
    match serde_json::from_str::<SandboxUsersFile>(&file) {
        Ok(users) => Ok(Some(users)),
        Err(err) => {
            debug_log(
                &format!("sandbox users parse failed: {err}"),
                Some(config_home),
            );
            Ok(None)
        }
    }
}

fn remove_sandbox_users_file(config_home: &Path, reason: &str) -> Result<()> {
    let path = sandbox_users_path(config_home);
    debug_log(
        &format!("{reason}; deleting {}", path.display()),
        Some(config_home),
    );
    match fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("delete {}", path.display())),
    }
}

fn decode_password(record: &SandboxUserRecord) -> Result<String> {
    let blob = BASE64_STANDARD
        .decode(record.password.as_bytes())
        .context("base64 decode password")?;
    let decrypted = dpapi::unprotect(&blob)?;
    let pwd = String::from_utf8(decrypted).context("sandbox password not utf-8")?;
    Ok(pwd)
}

fn select_identity(
    network_identity: SandboxNetworkIdentity,
    config_home: &Path,
) -> Result<Option<SandboxIdentity>> {
    let _marker = match load_marker(config_home)? {
        Some(m) if m.version_matches() => m,
        _ => return Ok(None),
    };
    let users = match load_users(config_home)? {
        Some(u) if u.version_matches() => u,
        _ => return Ok(None),
    };
    let chosen = match network_identity {
        SandboxNetworkIdentity::Offline => users.offline,
        SandboxNetworkIdentity::Online => users.online,
    };
    let password = decode_password(&chosen)?;
    Ok(Some(SandboxIdentity {
        username: chosen.username,
        password,
    }))
}

#[allow(clippy::too_many_arguments)]
pub fn require_logon_sandbox_creds(
    permissions: &ResolvedWindowsSandboxPermissions,
    command_cwd: &Path,
    env_map: &HashMap<String, String>,
    config_home: &Path,
    read_roots_override: Option<&[PathBuf]>,
    read_roots_include_platform_defaults: bool,
    write_roots_override: Option<&[PathBuf]>,
    deny_read_paths_override: &[PathBuf],
    deny_write_paths_override: &[PathBuf],
    proxy_enforced: bool,
) -> Result<SandboxCreds> {
    let needed_read = read_roots_override
        .map(<[PathBuf]>::to_vec)
        .unwrap_or_else(|| gather_read_roots(command_cwd, permissions, config_home));
    let needed_write = write_roots_override
        .map(<[PathBuf]>::to_vec)
        .unwrap_or_else(|| gather_write_roots_for_permissions(permissions));
    // Do not grant the capability token write access to KLAUS_HOME/.sandbox; the setup helper
    // grants the sandbox group access separately through lock_sandbox_dir.
    let request = SandboxSetupRequest {
        permissions,
        command_cwd,
        env_map,
        config_home,
        proxy_enforced,
    };
    let (creds, offline_proxy_settings) = require_sandbox_account(&request)?;
    run_setup_refresh_with_overrides_and_proxy_settings(
        request,
        crate::elevated::setup::SetupRootOverrides {
            read_roots: Some(needed_read),
            read_roots_include_platform_defaults,
            write_roots: Some(needed_write),
            deny_read_paths: Some(deny_read_paths_override.to_vec()),
            deny_write_paths: Some(deny_write_paths_override.to_vec()),
        },
        &offline_proxy_settings,
    )?;
    Ok(creds)
}

/// Ensures the selected account is ready; launchers must refresh filesystem ACLs separately.
pub(crate) fn require_sandbox_account(
    request: &SandboxSetupRequest<'_>,
) -> Result<(SandboxCreds, OfflineProxySettings)> {
    require_sandbox_account_with_setup(request, run_automatic_setup, local_user_flags)
}

fn require_sandbox_account_with_setup(
    request: &SandboxSetupRequest<'_>,
    run_full_setup: impl FnOnce(SandboxSetupRequest<'_>, &OfflineProxySettings) -> Result<()>,
    read_local_user_flags: impl Fn(&str) -> Result<Option<u32>>,
) -> Result<(SandboxCreds, OfflineProxySettings)> {
    let &SandboxSetupRequest {
        permissions,
        command_cwd,
        env_map,
        config_home,
        proxy_enforced,
    } = request;
    let sandbox_dir = crate::elevated::setup::sandbox_dir(config_home);
    let network_identity = SandboxNetworkIdentity::from_permissions(permissions, proxy_enforced);
    let marker = load_marker(config_home)?;
    let desired_offline_proxy_settings = desired_offline_proxy_settings(permissions);
    let mut setup_reason: Option<String> = None;

    let mut identity = match marker {
        Some(marker) if marker.version_matches() => {
            if let Some(reason) =
                marker.request_mismatch_reason(network_identity, &desired_offline_proxy_settings)
            {
                setup_reason = Some(reason);
                None
            } else {
                let selected = select_identity(network_identity, config_home)?;
                if selected.is_none() {
                    setup_reason = Some(
                        "sandbox users missing or incompatible with marker version".to_string(),
                    );
                }
                selected
            }
        }
        _ => {
            setup_reason = Some("sandbox setup marker missing or incompatible".to_string());
            None
        }
    };

    if identity.is_some() {
        // Cleanup may also have removed the group, so repair missing or disabled accounts before ACL
        // refresh can fail. Expired passwords also require full setup, since an ACL refresh
        // cannot rotate the account passwords and update the stored DPAPI credentials.
        for username in [OFFLINE_USERNAME, ONLINE_USERNAME] {
            let needs_repair = match read_local_user_flags(username) {
                Ok(Some(flags)) => flags & (UF_ACCOUNTDISABLE | UF_PASSWORD_EXPIRED) != 0,
                Ok(None) => true,
                Err(_) => false,
            };
            if needs_repair {
                let reason = "sandbox account is missing, disabled, or password expired";
                // Invalid credentials must not be retained as proof of completed setup.
                remove_sandbox_users_file(config_home, reason)?;
                setup_reason = Some(reason.to_string());
                identity = None;
                break;
            }
        }
    }

    if identity.is_none() {
        if let Some(reason) = &setup_reason {
            crate::elevated::logging::log_note(
                &format!("sandbox setup required: {reason}"),
                Some(&sandbox_dir),
            );
        } else {
            crate::elevated::logging::log_note("sandbox setup required", Some(&sandbox_dir));
        }
        run_full_setup(
            crate::elevated::setup::SandboxSetupRequest {
                permissions,
                command_cwd,
                env_map,
                config_home,
                proxy_enforced,
            },
            &desired_offline_proxy_settings,
        )?;
        for username in [OFFLINE_USERNAME, ONLINE_USERNAME] {
            if let Ok(Some(flags)) = read_local_user_flags(username) {
                anyhow::ensure!(
                    flags & UF_PASSWORD_EXPIRED == 0,
                    "Windows sandbox account password is still expired after setup"
                );
            }
        }
        identity = select_identity(network_identity, config_home)?;
    }
    let identity = identity.ok_or_else(|| {
        anyhow!(
            "Windows sandbox setup is missing or out of date; rerun the sandbox setup with elevation"
        )
    })?;
    Ok((
        SandboxCreds {
            username: identity.username,
            password: identity.password,
        },
        desired_offline_proxy_settings,
    ))
}

// Account setup runs through the native UAC helper when provisioning is required.
fn run_automatic_setup(
    request: SandboxSetupRequest<'_>,
    settings: &OfflineProxySettings,
) -> Result<()> {
    run_elevated_setup_with_proxy_settings(request, settings)
}

fn desired_offline_proxy_settings(
    permissions: &ResolvedWindowsSandboxPermissions,
) -> OfflineProxySettings {
    permissions.offline_proxy_settings()
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn refresh_logon_sandbox_creds(
    permissions: &ResolvedWindowsSandboxPermissions,
    command_cwd: &Path,
    env_map: &HashMap<String, String>,
    config_home: &Path,
    read_roots_override: Option<&[PathBuf]>,
    read_roots_include_platform_defaults: bool,
    write_roots_override: Option<&[PathBuf]>,
    deny_read_paths_override: &[PathBuf],
    deny_write_paths_override: &[PathBuf],
    proxy_enforced: bool,
) -> Result<SandboxCreds> {
    remove_sandbox_users_file(config_home, "sandbox user login failed")?;
    require_logon_sandbox_creds(
        permissions,
        command_cwd,
        env_map,
        config_home,
        read_roots_override,
        read_roots_include_platform_defaults,
        write_roots_override,
        deny_read_paths_override,
        deny_write_paths_override,
        proxy_enforced,
    )
}
