//! Feature-free integration suite for this crate.
//!
//! One target instead of one per file. Each file below used to be its own
//! integration-test target, which meant linking the whole crate once per file:
//! 53 links of a 160 MB debug binary for every change to the crate. Mounting
//! them with `#[path]` keeps each file exactly where it was and leaves one
//! link. This is the same convention `regressions.rs` already documents.
//!
//! Tests that need the `testing` feature live in `all_testing.rs`.

#[path = "code_mode_host_wire.rs"]
mod code_mode_host_wire;

#[path = "regressions.rs"]
mod regressions;

#[path = "runtime_agent_memory_unit_codecs.rs"]
mod runtime_agent_memory_unit_codecs;

#[path = "runtime_browser_unit_codecs.rs"]
mod runtime_browser_unit_codecs;

#[path = "runtime_capability_unit_codecs.rs"]
mod runtime_capability_unit_codecs;

#[path = "runtime_code_mode_protocol_unit_codecs.rs"]
mod runtime_code_mode_protocol_unit_codecs;

#[path = "runtime_command_mcp_message_unit_codecs.rs"]
mod runtime_command_mcp_message_unit_codecs;

#[path = "runtime_command_process_unit_codecs.rs"]
mod runtime_command_process_unit_codecs;

#[path = "runtime_config_unit_codecs.rs"]
mod runtime_config_unit_codecs;

#[path = "runtime_context_manager_unit_codecs.rs"]
mod runtime_context_manager_unit_codecs;

#[path = "runtime_effort_mcp_pdf_unit_codecs.rs"]
mod runtime_effort_mcp_pdf_unit_codecs;

#[path = "runtime_effort_value_codecs.rs"]
mod runtime_effort_value_codecs;

#[path = "runtime_media_capability_codecs.rs"]
mod runtime_media_capability_codecs;

#[path = "runtime_error_unit_codecs.rs"]
mod runtime_error_unit_codecs;

#[path = "runtime_event_unit_codecs.rs"]
mod runtime_event_unit_codecs;

#[path = "runtime_exec_server_empty_write_struct_codecs.rs"]
mod runtime_exec_server_empty_write_struct_codecs;

#[path = "runtime_exec_server_struct_codecs.rs"]
mod runtime_exec_server_struct_codecs;

#[path = "runtime_exec_server_unit_codecs.rs"]
mod runtime_exec_server_unit_codecs;

#[path = "runtime_extension_snapshot_unit_codecs.rs"]
mod runtime_extension_snapshot_unit_codecs;

#[path = "runtime_gemini_session_struct_codecs.rs"]
mod runtime_gemini_session_struct_codecs;

#[path = "runtime_hooks_unit_codecs.rs"]
mod runtime_hooks_unit_codecs;

#[path = "runtime_macos_kqueue_notifier.rs"]
mod runtime_macos_kqueue_notifier;

#[path = "runtime_mcp_elicitation_unit_codecs.rs"]
mod runtime_mcp_elicitation_unit_codecs;

#[path = "runtime_memory_type_unit_codecs.rs"]
mod runtime_memory_type_unit_codecs;

#[path = "runtime_misc_unit_codecs.rs"]
mod runtime_misc_unit_codecs;

#[path = "runtime_path_operations.rs"]
mod runtime_path_operations;

#[path = "runtime_path_value.rs"]
mod runtime_path_value;

#[path = "runtime_permission_unit_codecs.rs"]
mod runtime_permission_unit_codecs;

#[path = "runtime_plain_struct_codecs.rs"]
mod runtime_plain_struct_codecs;

#[path = "runtime_policy_unit_codecs.rs"]
mod runtime_policy_unit_codecs;

#[path = "runtime_process_oracle.rs"]
mod runtime_process_oracle;

#[path = "runtime_provider_observability_unit_codecs.rs"]
mod runtime_provider_observability_unit_codecs;

#[path = "runtime_provider_struct_codecs.rs"]
mod runtime_provider_struct_codecs;

#[path = "runtime_provider_unit_codecs.rs"]
mod runtime_provider_unit_codecs;

#[path = "runtime_rewind_fallback_task_plugin_lsp_unit_codecs.rs"]
mod runtime_rewind_fallback_task_plugin_lsp_unit_codecs;

#[path = "runtime_scalar_json.rs"]
mod runtime_scalar_json;

#[path = "runtime_session_unit_codecs.rs"]
mod runtime_session_unit_codecs;

#[path = "runtime_subagent_lifecycle_unit_codecs.rs"]
mod runtime_subagent_lifecycle_unit_codecs;

#[path = "runtime_tool_unit_codecs.rs"]
mod runtime_tool_unit_codecs;

#[path = "runtime_typed_codec.rs"]
mod runtime_typed_codec;

#[path = "runtime_wire_schema_golden.rs"]
mod runtime_wire_schema_golden;

#[path = "tool_lifecycle_trace.rs"]
mod tool_lifecycle_trace;
