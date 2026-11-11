//! Context assembly: system prompt, memory, compaction, token accounting.

pub mod agents_md;
pub mod at_mention;
pub mod compact;
pub mod context_hints;
pub mod date_change_reminder;
pub mod edited_files_diff;
pub mod edited_files_reminder;
pub mod env_info;
pub mod images;
pub(crate) mod inspection_images;
pub mod instruction_policy;
pub mod manager;
pub mod media_budget;
pub mod memory_files;
pub(crate) mod multi_agent;
pub mod nested_memory;
pub mod nested_memory_reminder;
pub mod request_media_budget;
pub(crate) mod retained_fragments;
pub mod system_prompt;
pub mod tokens;
pub mod tool_result_budget;
pub mod total_tokens_reminder;
pub mod ultracode_reminder;
pub mod ultrathink_reminder;
pub mod usage_checkpoint;
pub mod window_usage;

pub mod skill_receipts;
