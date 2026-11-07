// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified to bind the captured native filesystem and network boundary.

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::boundary::{AbsPath, FilesystemBoundary, NetworkBoundary};
use crate::protocol::SandboxRequest;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ResolvedWindowsSandboxPermissions {
    pub(crate) filesystem: FilesystemBoundary,
    pub(crate) network: NetworkBoundary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WindowsWritableRoot {
    pub(crate) root: PathBuf,
    pub(crate) read_only_subpaths: Vec<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowsSandboxTokenMode {
    ReadOnlyCapability,
    WritableRootsCapability,
}

#[derive(Clone, Copy)]
pub(crate) struct ResolvedNetworkPolicy(bool);

impl ResolvedNetworkPolicy {
    pub(crate) fn is_enabled(self) -> bool {
        self.0
    }
}

impl ResolvedWindowsSandboxPermissions {
    pub fn from_request(request: &SandboxRequest) -> Result<Self> {
        let permissions = Self {
            filesystem: request.filesystem.clone(),
            network: request.network.clone(),
        };
        permissions.validate_elevated_filesystem_policy(Path::new(&request.cwd.as_str()))?;

        Ok(permissions)
    }

    pub(crate) fn network_policy(&self) -> ResolvedNetworkPolicy {
        ResolvedNetworkPolicy(matches!(self.network, NetworkBoundary::Allowed))
    }

    #[cfg(test)]
    pub(crate) fn should_apply_network_block(&self) -> bool {
        !self.network_policy().is_enabled()
    }

    pub fn validate_elevated_filesystem_policy(&self, cwd: &Path) -> Result<()> {
        self.protected_metadata_paths()?;
        let cwd = AbsPath::new(cwd.to_string_lossy())?;
        let root = cwd
            .ancestors()
            .into_iter()
            .next()
            .unwrap_or_else(|| cwd.clone());
        let readable = self
            .filesystem
            .readable
            .as_ref()
            .is_none_or(|roots| roots.iter().any(|allowed| root.is_within(allowed)));
        if !readable
            || self
                .filesystem
                .deny_read
                .iter()
                .any(|deny| root.is_within(deny))
        {
            bail!("Elevated Windows sandbox requires effective filesystem-root read access");
        }
        if self
            .filesystem
            .writable
            .iter()
            .any(|allowed| root.is_within(allowed))
        {
            bail!("Full-disk filesystem writes cannot be enforced by the Windows sandbox");
        }
        Ok(())
    }

    fn protected_metadata_paths(&self) -> Result<Vec<AbsPath>> {
        self.filesystem
            .writable
            .iter()
            .flat_map(|root| {
                self.filesystem
                    .protected_metadata
                    .iter()
                    .map(move |name| root.join(name))
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    #[cfg(test)]
    pub(crate) fn has_full_disk_read_access(&self) -> bool {
        self.filesystem.readable.is_none() && self.filesystem.deny_read.is_empty()
    }

    pub(crate) fn has_symbolic_root_read_access(&self) -> bool {
        self.filesystem.readable.is_none()
    }

    pub(crate) fn include_platform_defaults(&self) -> bool {
        self.filesystem.readable.is_none()
    }

    pub(crate) fn readable_roots(&self) -> Vec<PathBuf> {
        self.filesystem
            .readable
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|path| PathBuf::from(path.as_str()))
            .collect()
    }

    pub(crate) fn uses_write_capabilities(&self) -> bool {
        !self.writable_roots().is_empty()
    }

    pub(crate) fn writable_roots(&self) -> Vec<WindowsWritableRoot> {
        self.filesystem
            .writable
            .iter()
            .map(|root| {
                let mut read_only_subpaths = self
                    .filesystem
                    .deny_write
                    .iter()
                    .filter(|path| path.is_within(root))
                    .cloned()
                    .collect::<Vec<_>>();
                read_only_subpaths.extend(
                    self.protected_metadata_paths()
                        .expect("validated metadata paths")
                        .into_iter()
                        .filter(|path| path.is_within(root))
                        .filter(|path| {
                            !self
                                .filesystem
                                .metadata_write_allow
                                .iter()
                                .any(|allowed| path == allowed)
                        }),
                );
                WindowsWritableRoot {
                    root: PathBuf::from(root.as_str()),
                    read_only_subpaths: read_only_subpaths
                        .into_iter()
                        .map(|path| PathBuf::from(path.as_str()))
                        .collect(),
                }
            })
            .collect()
    }

    pub(crate) fn offline_proxy_settings(&self) -> super::setup::OfflineProxySettings {
        match &self.network {
            NetworkBoundary::LoopbackPorts {
                ports,
                allow_local_binding,
            } => super::setup::OfflineProxySettings {
                proxy_ports: ports.clone(),
                allow_local_binding: *allow_local_binding,
            },
            NetworkBoundary::Denied | NetworkBoundary::Allowed => {
                super::setup::OfflineProxySettings {
                    proxy_ports: Vec::new(),
                    allow_local_binding: false,
                }
            }
        }
    }
}

pub fn token_mode_for_permissions(
    permissions: &ResolvedWindowsSandboxPermissions,
    cwd: &Path,
) -> Result<WindowsSandboxTokenMode> {
    permissions.validate_elevated_filesystem_policy(cwd)?;
    Ok(if permissions.uses_write_capabilities() {
        WindowsSandboxTokenMode::WritableRootsCapability
    } else {
        WindowsSandboxTokenMode::ReadOnlyCapability
    })
}

#[cfg(test)]
#[path = "permissions_tests.rs"]
mod tests;
