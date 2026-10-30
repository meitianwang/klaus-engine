//! Built-in tools available to the model.

pub mod advisor;
pub mod ask_user;
pub mod bash;
pub mod bash_output;
pub mod browser;
/// Retired from the product tool surface: the `exec` / `wait` pair was
/// replaced by the `REPL` tool. The implementation survives only so the frozen
/// provider-behavior fixtures can keep rendering their recorded specs.
#[cfg(any(test, feature = "testing"))]
pub mod code_mode;
pub mod cron_tools;
pub mod edit;
pub mod file_search;
pub mod glob;
pub mod grep;
pub mod host_policy;
pub mod index;
pub mod interactive_shell;
pub mod kill_shell;
pub mod ls;
pub mod lsp_tools;
pub mod multi_edit;
pub mod notebook_edit;
pub mod plugin_management;
pub mod plugin_search;
pub mod read;
pub mod repl;
pub mod request_plugin_install;
pub mod request_skill_install;
pub mod save_memory;
pub(crate) mod shell_common;
pub mod skill_search;
pub mod sleep;
pub mod swarm_tools;
pub mod task;
pub mod task_control;
pub mod task_tools;
pub mod think;
pub mod tool_search;
pub mod web_fetch;
pub mod web_fetch_redirect;
pub mod web_search;
mod web_search_bing_html;
mod web_search_browser;
pub mod workflow;
pub mod worktree_tools;
pub mod write;

pub mod mcp_prompts;
pub mod mcp_resources;
pub mod mcp_search;
pub mod request_mcp_install;
#[cfg(test)]
pub mod test_ctx;
