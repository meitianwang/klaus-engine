//! MCP marketplace catalog — the "store" seam for MCP servers.
//!
//! This module defines only the [`McpCatalog`] trait and its data types. It is a
//! host-integration seam: the engine carries no store endpoint, credential, or
//! service-specific request/response format. A host that wants an MCP store
//! injects a backend implementing [`McpCatalog`] (see `BuiltinsOptions::mcp_catalog`).
//!
//! "Installing" an MCP server just writes its connection spec into the chosen MCP
//! config scope — no download — so the entry becomes a normal declared server,
//! connected on the next session. The config-writing helpers live in
//! [`crate::mcp::config_file`] and are available to any backend implementation.

use crate::mcp::config_file::McpScope;
use crate::mcp::types::McpServerConfig;
use crate::runtime_values::{RuntimeJson, RuntimePath, RuntimeString};
use crate::types::abort::AbortSignal;
use async_trait::async_trait;

/// A boxed, thread-safe error carrying a human-readable message.
type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// A catalog entry as surfaced to the model / CLI. `server` is the friendly config
/// written on install (a JSON object).
#[derive(Debug, Clone, PartialEq)]
pub struct McpCatalogEntry {
    pub slug: RuntimeString,
    pub name: RuntimeString,
    pub description: RuntimeString,
    /// `OFFICIAL` | `COMMUNITY`.
    pub source: Option<RuntimeString>,
    /// Friendly server spec: `{ command, args, env }` (stdio) or `{ type?, url, headers?, oauth? }`.
    pub server: RuntimeJson,
    pub keywords: Option<Vec<RuntimeString>>,
}

#[derive(Default)]
pub struct McpCatalogSearchOptions {
    pub sources: Option<Vec<RuntimeString>>,
    pub limit: Option<u32>,
    pub signal: Option<AbortSignal>,
}

pub struct McpInstallFromCatalogOptions {
    /// Project root — required so a `local`-scope install can key by the project.
    pub cwd: RuntimePath,
    /// Scope to write the server into (default `local`, private per-project).
    pub scope: Option<McpScope>,
    /// Test override for the local store dir / user config dir.
    pub user_config_dir: Option<RuntimePath>,
    pub signal: Option<AbortSignal>,
}

/// An MCP catalog backend: search the store, look up one entry, and install (= write
/// config) by slug.
#[async_trait]
pub trait McpCatalog: Send + Sync {
    async fn search(
        &self,
        query: &RuntimeString,
        opts: McpCatalogSearchOptions,
    ) -> Result<Vec<McpCatalogEntry>, BoxError>;
    /// Look up one entry by exact slug; `None` when not found.
    async fn get(
        &self,
        slug: &RuntimeString,
        signal: Option<AbortSignal>,
    ) -> Result<Option<McpCatalogEntry>, BoxError>;
    /// Install (write the server's config into the given scope). Returns the parsed
    /// config, or `None`.
    async fn install(
        &self,
        slug: &RuntimeString,
        opts: McpInstallFromCatalogOptions,
    ) -> Result<Option<McpServerConfig>, BoxError>;
}
