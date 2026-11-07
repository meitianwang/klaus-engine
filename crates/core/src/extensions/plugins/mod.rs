//! Plugins: a bundle of engine extensions (skills, slash commands, sub-agents,
//! output styles, lifecycle hooks, and declared MCP/LSP servers) discovered from
//! a directory containing a `plugin.json` manifest.
//!
//! [`loader`] discovers and loads plugins already present on disk; [`remote`]
//! fetches a plugin from a git/npm/tarball source, installs it under the plugin
//! home, and loads it.

pub mod archive;
pub mod catalog;
pub mod identity;
pub mod installed;
pub mod loader;
pub mod remote;
pub mod runtime;
pub mod store;
