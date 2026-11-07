//! Native provenance for the exact MCP registration being classified.
//! Receipts are minted by file and plugin loaders and never decoded from client data.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::Value;

use crate::engine::approval_context::{TrustedTool, TrustedToolSource};
use crate::mcp::config_file::parse_mcp_server_entry;
use crate::mcp::types::McpServerConfig;
use crate::runtime_values::RuntimeString;
use crate::types::permission::{ApprovalActionMetadata, ApprovalActionSource};

/// A sealed receipt created while the native loader selects a server declaration.
#[derive(Clone, PartialEq)]
pub struct NativeMcpOrigin {
    source: Arc<NativeMcpSource>,
}

#[derive(Clone, PartialEq)]
enum NativeMcpSource {
    Configuration {
        home: PathBuf,
        user_file: PathBuf,
        user_entry: Option<Value>,
        effective_entry: Value,
    },
    Plugin {
        home: PathBuf,
        root: PathBuf,
        manifest: PathBuf,
        declaration: PathBuf,
        entry: Value,
    },
}

impl NativeMcpOrigin {
    pub(crate) fn configuration(
        home: PathBuf,
        user_file: PathBuf,
        user_entry: Option<Value>,
        effective_entry: Value,
    ) -> Self {
        Self {
            source: Arc::new(NativeMcpSource::Configuration {
                home,
                user_file,
                user_entry,
                effective_entry,
            }),
        }
    }

    pub(crate) fn plugin(
        home: PathBuf,
        root: PathBuf,
        manifest: PathBuf,
        declaration: PathBuf,
        entry: Value,
    ) -> Self {
        Self {
            source: Arc::new(NativeMcpSource::Plugin {
                home,
                root,
                manifest,
                declaration,
                entry,
            }),
        }
    }
}

impl std::fmt::Debug for NativeMcpOrigin {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = match self.source.as_ref() {
            NativeMcpSource::Configuration { .. } => "configuration",
            NativeMcpSource::Plugin { .. } => "plugin",
        };
        formatter
            .debug_tuple("NativeMcpOrigin")
            .field(&kind)
            .finish()
    }
}

/// The actual prepared connection and original tool identity captured by its owner.
#[derive(Clone, PartialEq)]
pub struct PreparedMcpRegistration {
    config: Arc<McpServerConfig>,
    origin: NativeMcpOrigin,
    tool_name: RuntimeString,
}

impl PreparedMcpRegistration {
    pub(crate) fn from_config(
        config: Arc<McpServerConfig>,
        tool_name: &RuntimeString,
    ) -> Option<Self> {
        let origin = config.options().native_origin.clone()?;
        Some(Self {
            config,
            origin,
            tool_name: tool_name.clone(),
        })
    }

    fn matches_entry(&self, entry: &Value) -> bool {
        let Some(expected) = parse_mcp_server_entry(self.config.name(), entry) else {
            return false;
        };
        let mut actual = self.config.as_ref().clone();
        actual.options_mut().native_origin = None;
        actual == expected
    }
}

impl std::fmt::Debug for PreparedMcpRegistration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedMcpRegistration")
            .field("origin", &self.origin)
            .finish_non_exhaustive()
    }
}

/// Bind a client declaration to a native file receipt only after full configuration equality.
/// Client-supplied metadata never participates in the receipt selection.
pub async fn bind_native_file_origins(cwd: &str, configs: &mut [McpServerConfig]) {
    let native = crate::mcp::config_file::load_mcp_server_configs(
        &crate::mcp::config_file::LoadMcpConfigsOptions {
            cwd: cwd.to_owned(),
            user_config_dir: None,
        },
    )
    .await;
    bind_matching_file_origins(configs, &native);
}

fn bind_matching_file_origins(configs: &mut [McpServerConfig], native: &[McpServerConfig]) {
    for config in configs {
        if config
            .options()
            .native_origin
            .as_ref()
            .is_some_and(|origin| matches!(origin.source.as_ref(), NativeMcpSource::Plugin { .. }))
        {
            continue;
        }
        config.options_mut().native_origin = None;
        let Some(matching) = native
            .iter()
            .find(|candidate| candidate.name() == config.name())
        else {
            continue;
        };
        let mut candidate = matching.clone();
        candidate.options_mut().native_origin = None;
        if candidate == *config {
            config.options_mut().native_origin = matching.options().native_origin.clone();
        }
    }
}

async fn home_owned(path: &Path, canonical_home: &Path) -> bool {
    tokio::fs::canonicalize(path)
        .await
        .is_ok_and(|path| path.starts_with(canonical_home))
}

/// Resolve only native ownership evidence for an asynchronous action classifier.
/// Descriptions, annotations, tool arguments, and external registrations cannot mint it.
pub(crate) async fn trusted_tool_context(metadata: &ApprovalActionMetadata) -> Option<TrustedTool> {
    let ApprovalActionSource::Mcp(tool) = &metadata.source else {
        return None;
    };
    let prepared = tool.native_registration.as_ref()?;
    if prepared.tool_name != tool.tool_name || prepared.config.name() != tool.server {
        return None;
    }
    let source = match prepared.origin.source.as_ref() {
        NativeMcpSource::Configuration {
            home,
            user_file,
            user_entry,
            effective_entry,
        } => {
            let canonical_home = tokio::fs::canonicalize(home).await.ok()?;
            if !home_owned(user_file, &canonical_home).await {
                return None;
            }
            let user_entry = user_entry.as_ref()?;
            if user_entry != effective_entry || !prepared.matches_entry(user_entry) {
                return None;
            }
            user_file.to_str()?.to_owned()
        }
        NativeMcpSource::Plugin {
            home,
            root,
            manifest,
            declaration,
            entry,
        } => {
            let canonical_home = tokio::fs::canonicalize(home).await.ok()?;
            if !home_owned(root, &canonical_home).await
                || !home_owned(manifest, &canonical_home).await
                || !home_owned(declaration, &canonical_home).await
                || !prepared.matches_entry(entry)
            {
                return None;
            }
            let manifest_contents = tokio::fs::read(manifest).await.ok()?;
            let manifest: Value = serde_json::from_slice(&manifest_contents).ok()?;
            if !manifest.is_object() {
                return None;
            }
            match manifest.get("mcpServers") {
                Some(Value::String(path)) => {
                    if !home_owned(&root.join(path), &canonical_home).await {
                        return None;
                    }
                }
                Some(Value::Object(_)) | None => {}
                Some(_) => return None,
            }
            root.to_str()?.to_owned()
        }
    };
    Some(TrustedTool {
        server: tool.server.try_to_utf8().ok()?,
        connector_id: tool
            .connector_id
            .as_ref()
            .map(RuntimeString::try_to_utf8)
            .transpose()
            .ok()?,
        source: TrustedToolSource::UserConfiguration(source),
    })
}

#[cfg(test)]
mod tests;
