//! Integration suite for the feature-gated test-support surface.
//!
//! Same single-target reason as `all.rs`; these files additionally need the
//! `testing` feature, so they are a separate target rather than a separate
//! target each.

#[path = "config_sandbox_trace.rs"]
mod config_sandbox_trace;

#[path = "exec_wait_trace.rs"]
mod exec_wait_trace;

#[path = "mcp_trace.rs"]
mod mcp_trace;

#[path = "permission_trace.rs"]
mod permission_trace;

#[path = "provider_behavior_trace.rs"]
mod provider_behavior_trace;

#[path = "query_loop_trace.rs"]
mod query_loop_trace;

#[path = "runtime_codec_roundtrip_snapshots.rs"]
mod runtime_codec_roundtrip_snapshots;

#[path = "runtime_numeric_struct_codecs.rs"]
mod runtime_numeric_struct_codecs;

#[path = "session_memory_trace.rs"]
mod session_memory_trace;

#[path = "session_persistence_trace.rs"]
mod session_persistence_trace;

#[path = "tool_result_trace.rs"]
mod tool_result_trace;

#[path = "tool_schema_trace.rs"]
mod tool_schema_trace;

#[path = "workflow_trace.rs"]
mod workflow_trace;
