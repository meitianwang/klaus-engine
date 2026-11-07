//! Permission engine: rule parsing/matching, exec policy, path rules.

pub mod bash;
pub mod durable;
pub mod engine;
pub mod execpolicy;
pub mod path_rules;
pub mod permission_update;
pub(crate) mod planning;
pub mod rule_store;
pub mod rules;
pub mod shell_parse;
pub mod shell_quote;
pub mod task_scope;

/// Internal marker the permission engine stamps onto the ALLOW input of a
/// plan-mode bash call whose read-only-ness could not be proven statically.
/// The bash tool reads it and enforces a read-only filesystem sandbox (plus
/// network off) for that one run, so plan mode's read-only promise is kept by
/// the OS rather than by a per-command prompt. It is not part of any tool
/// schema and never originates from the model: the engine only ever ADDS it on
/// its own allow path.
pub const PLAN_MODE_READONLY_ENFORCED_KEY: &str = "__klausPlanReadOnlyEnforced";
