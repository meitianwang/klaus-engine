//! Native Windows process-security policy and early launcher transformation.

use crate::types::sandbox::{ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec};

#[derive(Debug, Clone, Copy, Default)]
pub struct WindowsMxcSandbox;

impl WindowsMxcSandbox {
    pub fn new() -> Self {
        Self
    }
}

impl SandboxBackend for WindowsMxcSandbox {
    fn kind(&self) -> SandboxKind {
        SandboxKind::WindowsMxc
    }

    fn is_available(&self) -> bool {
        #[cfg(windows)]
        {
            super::windows::wrapper_entry_point_installed()
                && klaus_sandbox_win::mxc::is_available()
        }
        #[cfg(not(windows))]
        {
            false
        }
    }

    fn transform(&self, request: &ExecRequest, policy: &SandboxPolicy) -> SpawnSpec {
        #[cfg(windows)]
        {
            use crate::runtime_process::RuntimeEnv;
            use crate::runtime_values::RuntimeString;
            use klaus_sandbox_win::boundary::NetworkBoundary;
            use klaus_sandbox_win::mxc::{create_command_args, MxcOptions};
            use std::collections::HashMap;
            // TEMP/TMP are resolved on the executor from the filtered environment.
            let mut resolved_policy = policy.clone();
            resolved_policy.exclude_tmpdir_env_var = Some(true);
            let (mut native, inner_env) =
                super::windows::resolved_native_request(request, &resolved_policy);
            if !policy.planning_read_only && policy.runtime_network_ceiling != Some(false) {
                if let Some(ports) = policy
                    .allow_loopback_ports
                    .as_ref()
                    .filter(|ports| !ports.is_empty())
                {
                    native.network = NetworkBoundary::LoopbackPorts {
                        ports: ports.clone(),
                        allow_local_binding: policy
                            .windows_network_allow_local_binding
                            .unwrap_or(false),
                    };
                }
            }
            let text = |value: &RuntimeString| {
                value
                    .try_to_utf8()
                    .expect("MXC requires Unicode environment variables")
            };
            let mut environment: HashMap<String, String> = inner_env
                .entries()
                .map(|(key, value)| (text(&key), text(value)))
                .collect();
            let args = create_command_args(
                &native,
                MxcOptions {
                    unconfined_writes: super::policy::has_unconfined_writes(policy),
                    include_platform_defaults: policy.include_platform_defaults.unwrap_or(true),
                    include_tmpdir_env_var: !policy.filesystem_read_only
                        && !policy.planning_read_only
                        && policy.exclude_tmpdir_env_var != Some(true),
                },
                &mut environment,
            )
            .expect("invalid native MXC launcher policy");
            let mut env = RuntimeEnv::new();
            for (key, value) in environment {
                env.insert(key.into(), value.into());
            }
            SpawnSpec {
                file: std::env::current_exe()
                    .expect("resolve Klaus engine executable")
                    .to_string_lossy()
                    .as_ref()
                    .into(),
                args: args.into_iter().map(RuntimeString::from).collect(),
                cwd: request.cwd.clone(),
                env,
            }
        }
        #[cfg(not(windows))]
        {
            let _ = (request, policy);
            panic!("native MXC execution requires Windows");
        }
    }
}
