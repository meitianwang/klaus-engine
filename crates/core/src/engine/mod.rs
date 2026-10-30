//! The agent engine: session, turn loop, sub-agents, workflows.

// Modules below are the engine's public API surface: each one has consumers
// outside this crate (protocol/server/sdk/eval or the crate's integration
// tests). Everything else is `pub(crate)`: internal wiring that other in-crate
// layers may use but that is NOT part of the engine's external contract.
pub mod agent;
pub mod agent_definitions;
pub use builtin_agents::BUILTIN_AGENTS;
pub mod code_mode;
pub mod deps;
pub mod fork;
pub mod message_queue;
pub mod questions;
pub mod rewind;
pub mod session;
pub mod subagent_lifecycle;
pub(crate) mod subagent_rollouts;
pub mod tool_lifecycle_trace;

pub(crate) mod async_queue;
pub(crate) mod browser_tool_dependencies;
pub(crate) mod builtin_agents;
pub(crate) mod loop_detection;
pub(crate) mod observation_wait;
pub(crate) mod query;
pub(crate) mod resource_governor;
pub(crate) mod resume_repair;
pub(crate) mod session_config;
pub(crate) mod web_search_dependencies;
pub(crate) mod web_search_select;

// Still `pub` although nothing outside the crate uses them: kept on the
// engine's public surface pending the visibility sweep that follows the
// script-runner consolidation.
pub mod flow;
pub mod js_runtime;
pub mod workflow;

pub mod approval_async;
pub mod approval_capture;
pub mod approval_context;
pub(crate) mod approval_review_support;
pub(crate) mod approval_skill_evidence;

pub mod approval_requirements;
pub mod approval_requirements_cloud;

pub(crate) mod approval_action;
