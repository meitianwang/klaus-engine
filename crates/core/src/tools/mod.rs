//! Tool execution: builders, the tool pool, builtins, and supporting buffers.

pub mod apply_patch;
pub mod background_shells;
pub mod build_tool;
pub mod exec;
pub mod file_state;
pub mod freshness;
pub mod incremental_buffer;
pub(crate) mod input_parser;
pub(crate) mod locale_sort;
pub mod pool;
pub mod result_storage;
pub mod shell;
pub mod shell_env_snapshot;
pub mod shell_session;
pub mod shell_setup;
pub mod subagent_stream;
pub mod tool_schema;

pub mod builtins;
