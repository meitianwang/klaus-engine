// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified for native request paths, helper packaging, and execution lifecycle.

use anyhow::Context;
use anyhow::Result;
use serde::Deserialize;
use serde::Serialize;
use std::fs;
use std::io::ErrorKind;
use std::path::Path;
use std::path::PathBuf;

/// These represent the most common failures for the elevated sandbox setup.
///
/// Codes are used as metric tags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SetupErrorCode {
    // Orchestrator (run in CLI) failures.
    /// Failed to create `config_home/.sandbox` in the orchestrator.
    OrchestratorSandboxDirCreateFailed,
    /// Failed to determine whether the current process is elevated.
    OrchestratorElevationCheckFailed,
    /// The setup command requires an already elevated process.
    OrchestratorElevationRequired,
    /// Failed to serialize the elevation payload before launching the helper.
    OrchestratorPayloadSerializeFailed,
    /// Failed to launch the setup helper process (spawn or ShellExecuteExW).
    OrchestratorHelperLaunchFailed,
    /// User canceled the UAC prompt while launching the helper.
    OrchestratorHelperLaunchCanceled,
    /// Helper exited non-zero and no structured report was available.
    OrchestratorHelperExitNonzero,
    /// Helper exited non-zero and reading `setup_error.json` failed.
    OrchestratorHelperReportReadFailed,
    /// Helper exited successfully before setup completed.
    OrchestratorHelperIncomplete,
    // Helper (elevated process) failures.
    /// Helper failed while validating or decoding the request payload.
    HelperRequestArgsFailed,
    /// Helper failed to create `config_home/.sandbox`.
    HelperSandboxDirCreateFailed,
    /// Helper failed to open or write the setup log.
    HelperLogFailed,
    /// Helper failed in the provisioning phase (fallback bucket).
    HelperUserProvisionFailed,
    /// Helper failed to create the sandbox users local group.
    HelperUsersGroupCreateFailed,
    /// Helper failed to create or update a sandbox user account.
    HelperUserCreateOrUpdateFailed,
    /// Helper failed to protect user passwords with DPAPI.
    HelperDpapiProtectFailed,
    /// Helper failed to write the sandbox users secrets file.
    HelperUsersFileWriteFailed,
    /// Helper failed to write or protect the setup marker file.
    HelperSetupMarkerWriteFailed,
    /// Helper failed to resolve a SID or convert it to a PSID.
    HelperSidResolveFailed,
    /// Helper failed to load or convert capability SIDs.
    HelperCapabilitySidFailed,
    /// Helper failed to initialize COM for firewall configuration.
    HelperFirewallComInitFailed,
    /// Helper failed to access firewall policy or rule collections.
    HelperFirewallPolicyAccessFailed,
    /// Helper detected that local firewall policy changes will not fully take effect.
    HelperFirewallPolicyIneffective,
    /// Helper failed to create, update, or add the firewall rule.
    HelperFirewallRuleCreateOrAddFailed,
    /// Helper failed to verify the configured firewall rule scope.
    HelperFirewallRuleVerifyFailed,
    /// Helper failed to spawn the read-ACL helper process.
    HelperReadAclHelperSpawnFailed,
    /// Helper failed to lock down sandbox directories via ACLs.
    HelperSandboxLockFailed,
    /// Helper failed for an unmapped or unexpected reason.
    HelperUnknownError,
}

impl SetupErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OrchestratorSandboxDirCreateFailed => "orchestrator_sandbox_dir_create_failed",
            Self::OrchestratorElevationCheckFailed => "orchestrator_elevation_check_failed",
            Self::OrchestratorElevationRequired => "orchestrator_elevation_required",
            Self::OrchestratorPayloadSerializeFailed => "orchestrator_payload_serialize_failed",
            Self::OrchestratorHelperLaunchFailed => "orchestrator_helper_launch_failed",
            Self::OrchestratorHelperLaunchCanceled => "orchestrator_helper_launch_canceled",
            Self::OrchestratorHelperExitNonzero => "orchestrator_helper_exit_nonzero",
            Self::OrchestratorHelperReportReadFailed => "orchestrator_helper_report_read_failed",
            Self::OrchestratorHelperIncomplete => "orchestrator_helper_incomplete",
            Self::HelperRequestArgsFailed => "helper_request_args_failed",
            Self::HelperSandboxDirCreateFailed => "helper_sandbox_dir_create_failed",
            Self::HelperLogFailed => "helper_log_failed",
            Self::HelperUserProvisionFailed => "helper_user_provision_failed",
            Self::HelperUsersGroupCreateFailed => "helper_users_group_create_failed",
            Self::HelperUserCreateOrUpdateFailed => "helper_user_create_or_update_failed",
            Self::HelperDpapiProtectFailed => "helper_dpapi_protect_failed",
            Self::HelperUsersFileWriteFailed => "helper_users_file_write_failed",
            Self::HelperSetupMarkerWriteFailed => "helper_setup_marker_write_failed",
            Self::HelperSidResolveFailed => "helper_sid_resolve_failed",
            Self::HelperCapabilitySidFailed => "helper_capability_sid_failed",
            Self::HelperFirewallComInitFailed => "helper_firewall_com_init_failed",
            Self::HelperFirewallPolicyAccessFailed => "helper_firewall_policy_access_failed",
            Self::HelperFirewallPolicyIneffective => "helper_firewall_policy_ineffective",
            Self::HelperFirewallRuleCreateOrAddFailed => {
                "helper_firewall_rule_create_or_add_failed"
            }
            Self::HelperFirewallRuleVerifyFailed => "helper_firewall_rule_verify_failed",
            Self::HelperReadAclHelperSpawnFailed => "helper_read_acl_helper_spawn_failed",
            Self::HelperSandboxLockFailed => "helper_sandbox_lock_failed",
            Self::HelperUnknownError => "helper_unknown_error",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetupErrorReport {
    pub code: SetupErrorCode,
    pub message: String,
}

#[derive(Debug, PartialEq, Eq)]
pub struct SetupFailure {
    pub code: SetupErrorCode,
    pub message: String,
}

impl SetupFailure {
    pub fn new(code: SetupErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn from_report(report: SetupErrorReport) -> Self {
        Self::new(report.code, report.message)
    }
}

impl std::fmt::Display for SetupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code.as_str(), self.message)
    }
}

impl std::error::Error for SetupFailure {}

pub fn failure(code: SetupErrorCode, message: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(SetupFailure::new(code, message))
}

pub fn extract_failure(err: &anyhow::Error) -> Option<&SetupFailure> {
    err.downcast_ref::<SetupFailure>()
}

pub fn setup_error_path(config_home: &Path) -> PathBuf {
    config_home.join(".sandbox").join("setup_error.json")
}

pub fn clear_setup_error_report(config_home: &Path) -> Result<()> {
    let path = setup_error_path(config_home);
    match fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("remove {}", path.display())),
    }
}

pub fn write_setup_error_report(config_home: &Path, report: &SetupErrorReport) -> Result<()> {
    let sandbox_dir = config_home.join(".sandbox");
    fs::create_dir_all(&sandbox_dir)
        .with_context(|| format!("create sandbox dir {}", sandbox_dir.display()))?;
    let path = setup_error_path(config_home);
    let json = serde_json::to_vec_pretty(report)?;
    fs::write(&path, json).with_context(|| format!("write {}", path.display()))?;
    Ok(())
}

pub fn read_setup_error_report(config_home: &Path) -> Result<Option<SetupErrorReport>> {
    let path = setup_error_path(config_home);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err).with_context(|| format!("read {}", path.display())),
    };
    let report = serde_json::from_slice::<SetupErrorReport>(&bytes)
        .with_context(|| format!("parse {}", path.display()))?;
    Ok(Some(report))
}
