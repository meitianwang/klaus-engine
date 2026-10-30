// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified for the engine's resolved filesystem and network boundary types.

//! Project resolved permissions into native grants without expanding access.

use super::MxcOptions;
use crate::boundary::{AbsPath, NetworkBoundary};
use crate::protocol::SandboxRequest;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use wxc_common::cmdline::cmdline_from_argv_for_context;
use wxc_common::cmdline::CommandLineContext;
use wxc_common::cmdline::CommandLineError;
use wxc_common::filesystem_object::compare_existing_filesystem_objects;
use wxc_common::filesystem_object::normalize_object_conflicts;
use wxc_common::filesystem_object::ExistingObjectComparison;
use wxc_common::logger::Logger;
use wxc_common::logger::Mode;
use wxc_common::models::BaseProcessUiConfig;
use wxc_common::models::ContainerPolicy;
use wxc_common::models::ExecutionRequest;
use wxc_common::models::FallbackPolicy;
use wxc_common::models::NetworkAction;
use wxc_common::models::NetworkCidr;
use wxc_common::models::NetworkEgressPolicy;
use wxc_common::models::NetworkIngressPolicy;
use wxc_common::models::NetworkPeer;
use wxc_common::models::NetworkPolicy;
use wxc_common::models::NetworkRule;
use wxc_common::models::UiPolicy;

type NativePaths = HashMap<AbsPath, PathBuf>;

#[derive(Debug, thiserror::Error)]
pub enum PolicyError {
    #[error("MXC command must not be empty")]
    EmptyCommand,
    #[error("MXC requires Unicode filesystem policy paths")]
    NonUnicodePolicyPath,
    #[error("{0}")]
    PolicyResolution(String),
    #[error("enumerate MXC volume {path}: {source}")]
    EnumerateVolume {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    CommandLine(#[from] CommandLineError),
}

pub(super) fn build_request(
    command: &SandboxRequest,
    options: MxcOptions,
    volume_roots: &[PathBuf],
    platform_read_roots: &[PathBuf],
) -> Result<ExecutionRequest, PolicyError> {
    if command.command.is_empty() {
        return Err(PolicyError::EmptyCommand);
    }
    let fs = &command.filesystem;
    let mut write = collect_paths(fs.writable.iter().map(|path| PathBuf::from(path.as_str())))?;
    if options.unconfined_writes {
        write.extend(collect_paths(volume_roots.iter().cloned())?);
    }
    // Windows temporary directories come from the filtered command environment.
    // A Unix /tmp grant has no meaning on this executor.
    if options.include_tmpdir_env_var && !options.unconfined_writes {
        for (key, value) in &command.env {
            if key.eq_ignore_ascii_case("TEMP") || key.eq_ignore_ascii_case("TMP") {
                if let Ok(path) = AbsPath::new(value) {
                    write.insert(path.clone(), PathBuf::from(path.as_str()));
                }
            }
        }
    }
    let mut read = match &fs.readable {
        None => collect_paths(volume_roots.iter().cloned())?,
        Some(roots) => collect_paths(roots.iter().map(|path| PathBuf::from(path.as_str())))?,
    };
    let deny = collect_paths(fs.deny_read.iter().map(|path| PathBuf::from(path.as_str())))?;
    let mut carveouts = collect_paths(
        fs.deny_write
            .iter()
            .map(|path| PathBuf::from(path.as_str())),
    )?;
    if !options.unconfined_writes {
        for root in write.keys() {
            for name in &fs.protected_metadata {
                let path = root
                    .join(name)
                    .map_err(|error| PolicyError::PolicyResolution(error.to_string()))?;
                if !fs
                    .metadata_write_allow
                    .iter()
                    .any(|allowed| &path == allowed)
                {
                    carveouts.insert(path.clone(), PathBuf::from(path.as_str()));
                }
            }
        }
    }
    // A write carveout never introduces a new read grant outside the read ceiling.
    read.extend(
        carveouts
            .iter()
            .filter(|(path, _)| {
                !deny.keys().any(|denied| path.is_within(denied))
                    && (fs.readable.is_none()
                        || fs
                            .readable
                            .iter()
                            .flatten()
                            .any(|root| path.is_within(root))
                        || write.keys().any(|root| path.is_within(root)))
            })
            .map(|(key, path)| (key.clone(), path.clone())),
    );
    if options.include_platform_defaults {
        read.extend(collect_paths(platform_read_roots.iter().cloned())?);
    }
    write.retain(|key, _| !carveouts.contains_key(key) && !deny.contains_key(key));
    read.retain(|key, _| !write.contains_key(key) && !deny.contains_key(key));
    let volume_roots = collect_paths(volume_roots.iter().cloned())?
        .into_keys()
        .collect::<HashSet<_>>();
    prune_unavailable_volume_roots(&mut write, &volume_roots);
    prune_unavailable_volume_roots(&mut read, &volume_roots);
    // Normalize aliases before expansion so generated children inherit tightened access.
    let normalized = normalize_policy(ContainerPolicy {
        readwrite_paths: unicode_paths(write)?,
        readonly_paths: unicode_paths(read)?,
        denied_paths: unicode_paths(deny)?,
        ..Default::default()
    })?;
    let write = collect_paths(normalized.readwrite_paths.into_iter().map(PathBuf::from))?;
    let read = collect_paths(normalized.readonly_paths.into_iter().map(PathBuf::from))?;
    let deny = collect_paths(normalized.denied_paths.into_iter().map(PathBuf::from))?;
    let mut write = expand_volume_roots(write, &volume_roots)?;
    write.retain(|key, _| !carveouts.contains_key(key) && !deny.contains_key(key));
    let mut read = expand_volume_roots(read, &volume_roots)?;
    read.retain(|key, _| !write.contains_key(key) && !deny.contains_key(key));
    let network_enabled = matches!(command.network, NetworkBoundary::Allowed);
    let proxied = matches!(command.network, NetworkBoundary::LoopbackPorts { .. });
    if let NetworkBoundary::LoopbackPorts {
        ports,
        allow_local_binding,
    } = &command.network
    {
        super::validate_managed_network(ports, *allow_local_binding)
            .map_err(|error| PolicyError::PolicyResolution(error.to_string()))?;
    }
    let egress_default = if network_enabled && !proxied {
        NetworkAction::Allow
    } else {
        NetworkAction::Deny
    };
    let ingress_default = if network_enabled && !proxied {
        NetworkAction::Allow
    } else {
        NetworkAction::Deny
    };
    let mut egress = NetworkEgressPolicy {
        default: egress_default,
        ..Default::default()
    };
    if proxied {
        // PSEC host loopback is bidirectional. This shape is supported only
        // when the caller already allows local clients and servers. Keep
        // private-network ingress denied and allow no direct DNS bypass.
        egress.allow.push(NetworkRule {
            to: vec![
                NetworkPeer {
                    cidr: NetworkCidr {
                        address: std::net::Ipv4Addr::new(127, 0, 0, 0).into(),
                        prefix_length: 8,
                    },
                    except: Vec::new(),
                },
                NetworkPeer {
                    cidr: NetworkCidr {
                        address: std::net::Ipv6Addr::LOCALHOST.into(),
                        prefix_length: 128,
                    },
                    except: Vec::new(),
                },
            ],
            ports: Vec::new(),
        });
    }
    let mut request = ExecutionRequest {
        script_code: cmdline_from_argv_for_context(
            &command.command,
            CommandLineContext::WindowsCreateProcess,
        )?,
        working_directory: command.cwd.as_str(),
        env: command
            .env
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect(),
        policy: ContainerPolicy {
            capabilities: vec!["registryRead".to_owned()],
            readwrite_paths: unicode_paths(write)?,
            readonly_paths: unicode_paths(read)?,
            denied_paths: unicode_paths(deny)?,
            fallback: FallbackPolicy {
                allow_dacl_mutation: false,
            },
            default_network_policy: if egress_default == NetworkAction::Allow {
                NetworkPolicy::Allow
            } else {
                NetworkPolicy::Block
            },
            allow_local_network: ingress_default == NetworkAction::Allow,
            network_egress: Some(egress),
            network_ingress: Some(NetworkIngressPolicy {
                default: ingress_default,
                host_loopback: if network_enabled || proxied {
                    NetworkAction::Allow
                } else {
                    NetworkAction::Deny
                },
            }),
            network_specified: true,
            network_mode_specified: true,
            // PowerShell needs Win32k and desktop handles during DLL startup.
            // Keep clipboard, input injection, and system-control restrictions.
            ui: UiPolicy {
                disable: false,
                ..Default::default()
            },
            base_process_ui: BaseProcessUiConfig {
                isolation: "desktop".to_owned(),
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    };
    // Reconcile aliases introduced by expansion. MXC/PSEC separately enforces
    // restrictions on junction-resolved targets at access time.
    request.policy = normalize_policy(request.policy)?;
    Ok(request)
}
fn normalize_policy(mut policy: ContainerPolicy) -> Result<ContainerPolicy, PolicyError> {
    let mut logger = Logger::new(Mode::Buffer);
    if let Some(normalized) =
        normalize_object_conflicts(&policy, &mut logger).map_err(PolicyError::PolicyResolution)?
    {
        policy = normalized;
    }
    Ok(policy)
}

fn prune_unavailable_volume_roots(paths: &mut NativePaths, volume_roots: &HashSet<AbsPath>) {
    paths.retain(|key, path| {
        if !volume_roots.contains(key) && !key.ancestors().is_empty() {
            return true;
        }
        match std::fs::read_dir(path) {
            Ok(_) => true,
            Err(error) => !inaccessible_volume(&error),
        }
    });
}

// MXC volume-root grants are nonrecursive, so snapshot each root's current
// immediate children as recursive grants. A child created directly under the
// root after policy construction is not granted. MXC/PSEC still enforces
// restrictions on junction-resolved targets at access time.
fn expand_volume_roots(
    paths: NativePaths,
    volume_roots: &HashSet<AbsPath>,
) -> Result<NativePaths, PolicyError> {
    let mut expanded = NativePaths::new();
    for (key, path) in paths {
        if volume_roots.contains(&key) || key.ancestors().is_empty() {
            match std::fs::read_dir(&path).and_then(Iterator::collect::<std::io::Result<Vec<_>>>) {
                Ok(entries) => {
                    for entry in entries {
                        let child = entry.path();
                        if child.to_str().is_none() {
                            continue;
                        }
                        // A self-comparison is MXC's public object-identity probe.
                        // Generated grants can be skipped; explicit paths fail closed below.
                        if compare_existing_filesystem_objects(&child, &child)
                            != ExistingObjectComparison::Same
                        {
                            continue;
                        }
                        expanded.insert(path_key(&child)?, child);
                    }
                }
                // GetLogicalDrives includes disconnected and empty removable
                // drives. They must not prevent commands on available volumes.
                Err(error) if inaccessible_volume(&error) => continue,
                Err(error) => {
                    return Err(PolicyError::EnumerateVolume {
                        path,
                        source: error,
                    });
                }
            }
        }
        expanded.insert(key, path);
    }
    Ok(expanded)
}

fn collect_paths(paths: impl IntoIterator<Item = PathBuf>) -> Result<NativePaths, PolicyError> {
    paths
        .into_iter()
        .map(|path| Ok((path_key(&path)?, path)))
        .collect()
}

fn inaccessible_volume(error: &std::io::Error) -> bool {
    if matches!(
        error.kind(),
        std::io::ErrorKind::PermissionDenied
            | std::io::ErrorKind::NotFound
            | std::io::ErrorKind::NetworkUnreachable
            | std::io::ErrorKind::HostUnreachable
    ) {
        return true;
    }
    #[cfg(windows)]
    {
        use native_windows_sys::Win32::Foundation::ERROR_BAD_NETPATH;
        use native_windows_sys::Win32::Foundation::ERROR_CONNECTION_UNAVAIL;
        use native_windows_sys::Win32::Foundation::ERROR_DEVICE_NOT_CONNECTED;
        use native_windows_sys::Win32::Foundation::ERROR_NOT_READY;
        matches!(
            error.raw_os_error().map(|code| code as u32),
            Some(ERROR_NOT_READY | ERROR_DEVICE_NOT_CONNECTED | ERROR_BAD_NETPATH)
        ) || error.raw_os_error() == Some(ERROR_CONNECTION_UNAVAIL as i32)
    }
    #[cfg(not(windows))]
    {
        false
    }
}

fn unicode_paths(paths: NativePaths) -> Result<Vec<String>, PolicyError> {
    let mut paths = paths
        .into_values()
        .map(|path| {
            path.into_os_string()
                .into_string()
                .map_err(|_| PolicyError::NonUnicodePolicyPath)
        })
        .collect::<Result<Vec<_>, _>>()?;
    paths.sort();
    Ok(paths)
}

fn path_key(path: &std::path::Path) -> Result<AbsPath, PolicyError> {
    AbsPath::new(path.to_str().ok_or(PolicyError::NonUnicodePolicyPath)?)
        .map_err(|error| PolicyError::PolicyResolution(error.to_string()))
}
