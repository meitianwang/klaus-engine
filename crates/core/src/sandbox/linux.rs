//! Shipped Linux namespace and syscall sandbox.
//!
//! A fresh copy of the engine runs the embedded namespace launcher, then a
//! second helper installs no-new-privileges and seccomp before the command.

use crate::runtime_process::ParentEnvSnapshot;
use crate::runtime_values::RuntimeString;
use crate::types::sandbox::{ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec};

const HELPER: &str = "--klaus-linux-sandbox";
const BWRAP: &str = "--klaus-bubblewrap";
#[cfg(target_os = "linux")]
pub(super) use klaus_sandbox_linux_bwrap::helper::wslg;

fn helper_executable() -> Result<std::path::PathBuf, String> {
    #[cfg(target_os = "linux")]
    {
        klaus_sandbox_linux_bwrap::helper::resolve_executable()
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err("The Linux sandbox executor is only available on Linux.".into())
    }
}

#[derive(Debug, Default)]
pub struct LinuxSandbox {
    use_legacy_landlock: bool,
}

impl LinuxSandbox {
    pub fn new() -> Self {
        Self::default()
    }

    /// Capture the explicit deprecated in-process execution preference.
    pub fn with_legacy_landlock() -> Self {
        Self {
            use_legacy_landlock: true,
        }
    }
}

impl SandboxBackend for LinuxSandbox {
    fn kind(&self) -> SandboxKind {
        SandboxKind::Bubblewrap
    }

    fn is_available(&self) -> bool {
        cfg!(all(
            target_os = "linux",
            any(target_arch = "x86_64", target_arch = "aarch64")
        )) && helper_executable().is_ok()
    }

    fn transform(&self, request: &ExecRequest, policy: &SandboxPolicy) -> SpawnSpec {
        let environment = ParentEnvSnapshot::capture();
        let empty = Default::default();
        let environment =
            super::process::scrub_env(&environment, request.env.as_ref().unwrap_or(&empty));
        let executable = helper_executable()
            .map(|path| crate::fs::node_path::runtime_path_from_native(&path))
            .unwrap_or_else(|error| panic!("resolve sandbox executable: {error}"));
        if self.use_legacy_landlock {
            return SpawnSpec {
                file: executable.into_runtime_string(),
                args: legacy_arguments(request, policy),
                cwd: request.cwd.clone(),
                env: environment,
            };
        }
        let plan = super::bubblewrap::build_bwrap_plan(request, policy, &environment);
        let mut arguments = vec![RuntimeString::from(BWRAP)];
        match plan {
            Ok(plan) => {
                for path in plan.missing_protected_paths {
                    arguments.extend([RuntimeString::from("--protect-missing"), path]);
                }
                for path in plan.synthetic_protected_paths {
                    arguments.extend([RuntimeString::from("--synthetic-file"), path]);
                }
                for path in plan.denied_read_files {
                    arguments.extend([RuntimeString::from("--deny-file"), path]);
                }
                arguments.push(RuntimeString::from("--"));
                arguments.extend(plan.arguments);
                arguments.push(RuntimeString::from("--"));
                arguments.push(executable.as_runtime_string().clone());
                arguments.push(RuntimeString::from(HELPER));
                arguments.push(RuntimeString::from(if policy.network_allowed {
                    "--network-allowed"
                } else {
                    "--network-restricted"
                }));
                if !policy.network_allowed
                    && policy
                        .allow_loopback_ports
                        .as_ref()
                        .is_some_and(|ports| !ports.is_empty())
                {
                    arguments.push(RuntimeString::from("--managed-proxy"));
                }
                if policy.allow_all_unix_sockets == Some(true) {
                    arguments.push(RuntimeString::from("--allow-unix-sockets"));
                }
                arguments.push(RuntimeString::from("--"));
                arguments.extend(request.argv.iter().cloned());
            }
            Err(error) => {
                arguments.extend([RuntimeString::from("--fail"), RuntimeString::from(error)]);
            }
        }
        SpawnSpec {
            file: executable.into_runtime_string(),
            args: arguments,
            cwd: request.cwd.clone(),
            env: environment,
        }
    }
}

/// Dispatch internal native helper invocations before application startup.
/// Embedding executables may call this early, or ship the adjacent native helper.
pub fn dispatch() {
    #[cfg(target_os = "linux")]
    klaus_sandbox_linux_bwrap::helper::dispatch();
}

// Full filesystem access cannot carry a read or write narrowing entry. The
// in-process legacy branch has no filesystem namespace to enforce one.
fn legacy_full_filesystem_write(policy: &SandboxPolicy) -> bool {
    super::policy::has_unconfined_writes(policy)
        && !policy.filesystem_read_only
        && !policy.planning_read_only
        && policy.readable_roots.is_none()
        && policy.read_only_subpaths.as_ref().is_none_or(Vec::is_empty)
        && policy.deny_read_subpaths.as_ref().is_none_or(Vec::is_empty)
}

fn legacy_arguments(request: &ExecRequest, policy: &SandboxPolicy) -> Vec<RuntimeString> {
    let mut arguments = vec![
        RuntimeString::from(HELPER),
        RuntimeString::from(if policy.network_allowed {
            "--network-allowed"
        } else {
            "--network-restricted"
        }),
        RuntimeString::from("--use-legacy-landlock"),
    ];
    if legacy_full_filesystem_write(policy) {
        arguments.push(RuntimeString::from("--full-filesystem-write"));
    }
    if policy
        .allow_loopback_ports
        .as_ref()
        .is_some_and(|ports| !ports.is_empty())
    {
        arguments.push(RuntimeString::from("--managed-proxy"));
    }
    arguments.push(RuntimeString::from("--"));
    arguments.extend(request.argv.iter().cloned());
    arguments
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn legacy_filesystem_guard_preserves_genuine_read_and_write_ceilings() {
        let full = SandboxPolicy {
            unconfined_writes: Some(true),
            ..Default::default()
        };
        assert!(legacy_full_filesystem_write(&full));
        for restricted in [
            SandboxPolicy::default(),
            SandboxPolicy {
                filesystem_read_only: true,
                ..full.clone()
            },
            SandboxPolicy {
                readable_roots: Some(vec!["/workspace".into()]),
                ..full.clone()
            },
            SandboxPolicy {
                read_only_subpaths: Some(vec!["/locked".into()]),
                ..full.clone()
            },
            SandboxPolicy {
                deny_read_subpaths: Some(vec!["/private".into()]),
                ..full
            },
        ] {
            assert!(!legacy_full_filesystem_write(&restricted));
        }
    }
    #[test]
    fn legacy_launch_uses_the_actual_helper_without_namespace_or_path_lookup() {
        let request = ExecRequest {
            argv: vec!["/bin/sh".into(), "-c".into(), "exit 0".into()],
            cwd: "/outside".into(),
            env: None,
            timeout_ms: None,
        };
        let args = legacy_arguments(
            &request,
            &SandboxPolicy {
                unconfined_writes: Some(true),
                network_allowed: false,
                ..Default::default()
            },
        );
        assert_eq!(args[0], RuntimeString::from(HELPER));
        assert!(args.iter().any(|arg| arg == "--use-legacy-landlock"));
        assert!(args.iter().any(|arg| arg == "--full-filesystem-write"));
        assert!(!args.iter().any(|arg| arg == BWRAP));
        assert_eq!(&args[args.len() - 3..], request.argv.as_slice());
    }
}
