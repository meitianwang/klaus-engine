//! Goal mode: a persisted objective a thread keeps pursuing across turns.
//!
//! The shape is deliberately asymmetric, and that asymmetry is the whole
//! mechanism:
//!
//!   - the MODEL may create a goal and may claim it `complete` or `blocked`,
//!     and nothing else. It cannot pause, resume, or budget-limit itself, so
//!     "this is hard" is not a way out;
//!   - the ENGINE keeps starting turns while the goal is active, charges every
//!     turn's usage to the goal, and stops it when the token budget runs out;
//!   - the USER (through the host) sets, edits, pauses, resumes and clears.
//!
//! Layout:
//!   - [`types`] the record and its statuses;
//!   - [`store`] the on-disk record, with the replace and budget invariants;
//!   - [`accounting`] per-turn usage deltas, charged once;
//!   - [`steering`] the three instructions injected on the goal's behalf;
//!   - [`runtime`] the serialized coordinator every transition goes through;
//!   - [`tools`] `create_goal` / `update_goal` / `get_goal`.

pub mod accounting;
pub mod objective_preview;
pub mod owned_resources;
pub mod reminder;
pub mod retained_context;
pub mod runtime;
pub mod service;
pub mod steering;
pub mod store;
pub mod tools;
pub mod types;

pub use accounting::{goal_token_delta_for_usage, BudgetLimitedGoalDisposition};
pub use runtime::{
    GoalContinuation, GoalCreateOutcome, GoalHostUpdate, GoalObserver, GoalRuntime, GoalStopReason,
    GoalUpdateOutcome,
};
pub use steering::{budget_limit_prompt, continuation_prompt, objective_updated_prompt};
pub use tools::{
    create_goal_tools, CREATE_GOAL_TOOL_NAME, GET_GOAL_TOOL_NAME, UPDATE_GOAL_TOOL_NAME,
};
pub use types::{
    GoalAccountingMode, GoalAccountingOutcome, GoalUpdate, ThreadGoal, ThreadGoalStatus,
    GOAL_OBJECTIVE_MAX_LENGTH,
};

#[cfg(any(test, feature = "testing"))]
pub mod testing_phase;
