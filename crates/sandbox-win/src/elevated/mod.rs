// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified for captured native requests and packaged helpers.

//! Dedicated-account Windows execution, account preparation, and native ACL enforcement.

mod allow;
mod app_identity;
mod command_runner;
mod conpty;
mod deny_read_acl;
mod deny_read_state;
mod desktop;
mod dpapi;
mod env;
mod environment_transport;
mod framed_io;
mod helper_materialization;
mod hide_users;
mod identity;
mod ipc_framed;
mod job;
mod launch_environment;
mod logging;
mod proc_thread_attr;
mod process;
mod resolved_permissions;
mod runner_client;
mod runner_metrics;
mod runner_pipe;
mod sandbox_utils;
mod setup;
mod setup_acl_error;
mod setup_error;
mod setup_mutex;
mod setup_provisioning;
mod ssh_config_dependencies;
mod wfp;
mod wfp_setup;

pub(crate) use crate::native::{acl, cap, path_normalization, token, winutil};
pub(crate) use acl::add_deny_write_ace;
pub(crate) use acl::allow_null_device;
pub(crate) use acl::ensure_allow_mask_aces_with_inheritance;
pub(crate) use acl::ensure_allow_write_aces;
pub(crate) use acl::path_mask_allows;
pub(crate) use acl::path_write_aces_need_refresh;
pub(crate) use cap::workspace_write_cap_sid_for_root;
pub(crate) use cap::workspace_write_root_overlaps_path;
pub(crate) use conpty::spawn_conpty_process_as_user;
pub(crate) use conpty::ConptyInstance;
pub(crate) use deny_read_state::sync_persistent_deny_read_acls;
pub(crate) use desktop::LaunchDesktop;
pub(crate) use dpapi::protect as dpapi_protect;
pub(crate) use hide_users::hide_current_user_profile_dir;
pub(crate) use hide_users::hide_newly_created_users;
pub(crate) use ipc_framed::decode_bytes;
pub(crate) use ipc_framed::encode_bytes;
pub(crate) use ipc_framed::read_frame;
pub(crate) use ipc_framed::write_frame;
pub(crate) use ipc_framed::ErrorPayload;
pub(crate) use ipc_framed::ErrorStage;
pub(crate) use ipc_framed::ExitPayload;
pub(crate) use ipc_framed::FramedMessage;
pub(crate) use ipc_framed::Message;
pub(crate) use ipc_framed::OutputPayload;
pub(crate) use ipc_framed::OutputStream;
pub(crate) use ipc_framed::ResizePayload;
pub(crate) use ipc_framed::SpawnReady;
pub(crate) use ipc_framed::SpawnRequest;
pub(crate) use ipc_framed::IPC_PROTOCOL_VERSION;
pub(crate) use logging::log_note;
pub(crate) use logging::log_writer;
pub(crate) use process::read_handle_loop;
pub(crate) use process::spawn_process_with_pipes;
pub(crate) use process::PipeSpawnHandles;
pub(crate) use process::StdinMode;
pub(crate) use resolved_permissions::WindowsSandboxTokenMode;
pub(crate) use setup::sandbox_bin_dir;
pub(crate) use setup::sandbox_dir;
pub(crate) use setup::sandbox_secrets_dir;
pub(crate) use setup::SETUP_VERSION;
pub(crate) use setup_error::extract_failure as extract_setup_failure;
pub(crate) use setup_error::write_setup_error_report;
pub(crate) use setup_error::SetupErrorCode;
pub(crate) use setup_error::SetupErrorReport;
pub(crate) use setup_error::SetupFailure;
pub(crate) use setup_mutex::acquire_sandbox_setup_lock;
pub(crate) use token::convert_string_sid_to_sid;
pub(crate) use token::create_readonly_token_with_caps_and_user_from;
pub(crate) use token::create_workspace_write_token_with_caps_and_user_from;
pub(crate) use token::get_current_token_for_restriction;
pub(crate) use token::LocalSid;
pub(crate) use wfp::install_wfp_filters_for_account;
pub(crate) use wfp_setup::install_wfp_filters;
pub(crate) use winutil::ensure_sandbox_users_group;
pub(crate) use winutil::local_user_flags;
pub(crate) use winutil::resolve_sid;
pub(crate) use winutil::set_local_user_flags;
pub(crate) use winutil::string_from_sid_bytes;
pub(crate) use winutil::to_wide;
pub(crate) use winutil::SANDBOX_USERS_GROUP;

pub use command_runner::main as command_runner_main;
pub use runtime::{prepare, run, setup_is_complete, setup_matches_network, validate_request};
pub use setup_provisioning::main as setup_helper_main;
mod provisioning;
mod runtime;
pub use provisioning::provisioning_configuration;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct WindowsSandboxProvisioningSettings {
    pub proxy_ports: Vec<u16>,
    pub allow_local_binding: bool,
}

pub(crate) fn background_command(program: impl AsRef<std::ffi::OsStr>) -> std::process::Command {
    use std::os::windows::process::CommandExt;
    let mut command = std::process::Command::new(program);
    command.creation_flags(native_windows_sys::Win32::System::Threading::CREATE_NO_WINDOW);
    command
}

pub(crate) fn current_setup_user() -> Result<String> {
    use native_windows_sys::Win32::{Security as security, System::Threading as threading};
    use std::os::windows::io::{FromRawHandle, OwnedHandle};
    let mut token = std::ptr::null_mut();
    if unsafe {
        threading::OpenProcessToken(
            threading::GetCurrentProcess(),
            security::TOKEN_QUERY,
            &mut token,
        )
    } == 0
    {
        return Err(std::io::Error::last_os_error()).context("query requesting setup identity");
    }
    let _token = unsafe { OwnedHandle::from_raw_handle(token as _) };
    let sid = unsafe { token::get_user_sid_bytes(token)? };
    unsafe { winutil::account_name_from_sid(sid.as_ptr() as _) }
        .context("resolve requesting setup identity")
}

pub(crate) fn take_bytes_at_char_boundary(value: &str, max_bytes: usize) -> &str {
    let mut end = value.len().min(max_bytes);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

mod managed_deny_probe;
pub use managed_deny_probe::main as managed_deny_probe_main;
