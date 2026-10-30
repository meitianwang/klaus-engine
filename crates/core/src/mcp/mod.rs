//! MCP (Model Context Protocol) client subsystem.
//!
//! A hand-rolled MCP client: JSON-RPC 2.0 over pluggable transports (stdio,
//! Streamable HTTP, legacy SSE, and an in-process pair), OAuth 2.0 for remote
//! servers, on-disk server declarations, a connection manager, and the tool
//! wrapper that surfaces a connected server's tools to the engine.
//!
//! [`types`] holds the config + shared wire shapes; the remaining submodules are
//! parallel siblings that build on them.

pub mod approval_provenance;
pub mod client;
pub mod config_file;
pub mod connection_manager;
pub mod credential_store;
pub mod elicitation;
pub mod loopback_authorize;
pub mod mcp_hub;
pub mod mcp_trust;
pub mod needs_auth_cache;
pub mod oauth;
pub mod oauth_provider;
pub mod persist_blob;
pub mod runtime_snapshot;
pub mod tool_naming;
pub mod tool_wrapper;
pub mod transports;
pub mod types;
