//! Ephemeral context injection at turn entry or before each model request.

use crate::runtime_values::{RuntimePath, RuntimeString};
use crate::schemas::message::Message;
use crate::types::abort::AbortSignal;
use async_trait::async_trait;

/// What a reminder provider sees when deciding whether to inject context.
pub struct ReminderContext {
    /// The turn's working message history (read-only).
    pub messages: Vec<Message>,
    pub cwd: RuntimePath,
    /// The active structured-task list id, when one is in scope.
    pub task_list_id: Option<RuntimeString>,
    /// Current-plan binding captured by the originating run.
    pub task_plan: Option<crate::tasks::task_store::TaskPlanBinding>,
    /// The turn's abort signal. A provider that does I/O forwards it so an
    /// aborted turn cancels the work and unblocks turn entry.
    pub signal: Option<AbortSignal>,
    /// The session's plan-mode plan document, when a slug exists.
    pub plan_file_path: Option<RuntimePath>,
    /// Whether that plan document already exists on disk this turn.
    pub plan_file_exists: Option<bool>,
    /// Whether the `task` (sub-agent) tool is in this loop's tool pool — selects
    /// the parallel-exploration variant of the plan-mode workflow guidance.
    pub has_task_tool: bool,
}

/// A named request-time reminder, refreshed only while its tool is visible.
pub struct RequestReminder {
    pub name: &'static str,
    pub required_tool: &'static str,
}

/// Ephemeral context evaluated at user-turn entry by default. Request-time
/// providers opt into a named slot that is replaced or removed before each
/// model request. Output is ephemeral unless a provider explicitly opts into
/// retained conversation context.
#[async_trait]
pub trait ReminderProvider: Send + Sync {
    /// Called once when a full query turn enters, before any model request or
    /// request retry. Retained providers keep their history-backed coverage.
    fn begin_turn(&self) {}

    async fn reminder(&self, ctx: &ReminderContext) -> Option<RuntimeString>;

    /// Budget model-dependent reminders from the request's immutable binding.
    async fn reminder_with_context_window(
        &self,
        ctx: &ReminderContext,
        _window: u64,
    ) -> Option<RuntimeString> {
        self.reminder(ctx).await
    }

    /// Structured retained input from an external mailbox. None keeps the
    /// provider's existing reminder representation; Some owns this refresh.
    async fn retained_messages(&self, _ctx: &ReminderContext) -> Option<Vec<Message>> {
        None
    }

    /// Retain authoritative context as a trusted meta message. The provider
    /// must inspect `ctx.messages` to avoid duplicates and restore its fragment
    /// when compaction or a resumed history no longer contains it.
    fn retained_context(&self) -> bool {
        false
    }

    fn request_refresh(&self) -> Option<RequestReminder> {
        None
    }

    /// Completed tool rounds are independent of compacted history and start
    /// at zero for each run. Provider retries and text-only continuations do
    /// not count, so short conversations do not accumulate a work nag.
    async fn reminder_for_request(
        &self,
        ctx: &ReminderContext,
        _completed_tool_rounds: u32,
    ) -> Option<RuntimeString> {
        self.reminder(ctx).await
    }
}
