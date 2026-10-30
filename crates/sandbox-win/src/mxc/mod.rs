// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified for resolved engine permissions, launcher transport, and native dispatch.

//! Native process-security execution. Policy conversion is portable; launch
//! requires the native kernel facility and never enters ACL fallback backends.

mod environment_transport;
#[cfg(windows)]
mod native;
mod policy;
mod transport;
#[cfg(windows)]
mod windows;

use crate::boundary::NetworkBoundary;
use crate::protocol::SandboxRequest;
use anyhow::{ensure, Result};
pub use policy::PolicyError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub const LAUNCH_ARG: &str = "--klaus-internal-windows-mxc";

/// Resolved policy facts not represented by the internal container request.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct MxcOptions {
    pub unconfined_writes: bool,
    pub include_platform_defaults: bool,
    pub include_tmpdir_env_var: bool,
}

#[derive(Serialize, Deserialize)]
struct MxcCommand {
    request: SandboxRequest,
    options: MxcOptions,
    #[serde(default)]
    default_child_env: bool,
}

/// Validate the supported native managed-network shape.
pub fn validate_managed_network(ports: &[u16], allow_local_binding: bool) -> Result<()> {
    ensure!(
        !ports.is_empty(),
        "MXC managed networking requires dedicated proxy ports"
    );
    ensure!(!ports.contains(&0), "MXC proxy ports must be nonzero");
    ensure!(allow_local_binding, "MXC cannot enforce managed networking with allow_local_binding=false: native host-loopback access is bidirectional");
    Ok(())
}

/// Add a bounded environment payload; the command line contains only a marker.
pub fn create_command_args(
    request: &SandboxRequest,
    options: MxcOptions,
    env: &mut HashMap<String, String>,
) -> Result<Vec<String>> {
    if let NetworkBoundary::LoopbackPorts {
        ports,
        allow_local_binding,
    } = &request.network
    {
        validate_managed_network(ports, *allow_local_binding)?;
    }
    let mut request = request.clone();
    request.env.clear();
    transport::encode(
        &MxcCommand {
            request,
            options,
            default_child_env: env
                .keys()
                .all(|key| environment_transport::is_key(key.as_ref())),
        },
        env,
    )?;
    Ok(vec![LAUNCH_ARG.to_owned()])
}

/// Whether a native process security environment can actually be created.
pub fn is_available() -> bool {
    #[cfg(windows)]
    {
        appcontainer_common::base_container_runner::BaseContainerRunner::is_process_security_environment_usable()
    }
    #[cfg(not(windows))]
    {
        false
    }
}

/// Build the native policy from already resolved filesystem and network limits.
/// Volume roots are enumerated on the executor; this function also permits
/// deterministic policy fixtures without claiming kernel execution succeeded.
pub fn build_request(
    request: &SandboxRequest,
    options: MxcOptions,
    volume_roots: &[std::path::PathBuf],
    platform_read_roots: &[std::path::PathBuf],
) -> Result<wxc_common::models::ExecutionRequest, PolicyError> {
    policy::build_request(request, options, volume_roots, platform_read_roots)
}

/// Dispatch before ordinary engine argument parsing or configuration loading.
pub fn run_main() -> ! {
    #[cfg(windows)]
    {
        match windows::run() {
            Ok(code) => std::process::exit(code),
            Err(error) => {
                eprintln!("MXC launcher: {error:#}");
                std::process::exit(1);
            }
        }
    }
    #[cfg(not(windows))]
    {
        eprintln!("MXC sandbox execution requires Windows");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests;
