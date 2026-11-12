//! Session-scoped accounting for API-level context-editing reports.
//!
//! When a provider applies server-side context editing (clearing aged tool
//! blocks or prior-turn thinking), the response carries a report of what was
//! cleared. This state accumulates those reports for one session so later
//! requests can adapt: the running tool-use total anchors the next count-based
//! clearing trigger, which batches clears instead of repeating one on every
//! request the client sends over an unrewritten history.

use std::sync::atomic::{AtomicU64, Ordering};

/// What the server has cleared for one session so far.
#[derive(Debug, Default)]
pub struct ContextEditState {
    cleared_tool_uses: AtomicU64,
    cleared_input_tokens: AtomicU64,
    cleared_thinking_turns: AtomicU64,
    reports: AtomicU64,
}

/// A consistent-enough view of the counters for logging and tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContextEditTotals {
    pub cleared_tool_uses: u64,
    pub cleared_input_tokens: u64,
    pub cleared_thinking_turns: u64,
    pub reports: u64,
}

impl ContextEditState {
    /// Fold one applied-edits report into the running totals.
    pub fn record(&self, tool_uses: u64, input_tokens: u64, thinking_turns: u64) {
        self.cleared_tool_uses
            .fetch_add(tool_uses, Ordering::Relaxed);
        self.cleared_input_tokens
            .fetch_add(input_tokens, Ordering::Relaxed);
        self.cleared_thinking_turns
            .fetch_add(thinking_turns, Ordering::Relaxed);
        self.reports.fetch_add(1, Ordering::Relaxed);
    }

    /// Tool uses cleared so far; the anchor added to a configured count-based
    /// trigger so the next clear waits for a fresh batch of tool calls.
    pub fn cleared_tool_uses(&self) -> u64 {
        self.cleared_tool_uses.load(Ordering::Relaxed)
    }

    pub fn totals(&self) -> ContextEditTotals {
        ContextEditTotals {
            cleared_tool_uses: self.cleared_tool_uses.load(Ordering::Relaxed),
            cleared_input_tokens: self.cleared_input_tokens.load(Ordering::Relaxed),
            cleared_thinking_turns: self.cleared_thinking_turns.load(Ordering::Relaxed),
            reports: self.reports.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_accumulate_across_reports() {
        let state = ContextEditState::default();
        assert_eq!(state.cleared_tool_uses(), 0);
        state.record(8, 50_000, 0);
        state.record(4, 20_000, 2);
        assert_eq!(
            state.totals(),
            ContextEditTotals {
                cleared_tool_uses: 12,
                cleared_input_tokens: 70_000,
                cleared_thinking_turns: 2,
                reports: 2,
            }
        );
    }
}
