//! Defect: automatic continuation omitted the distinction between waiting and no progress.
//! Root cause: the continuation template lost its live-handle and restart boundaries.
//! Work item: 20261016 goal progress guidance.

use klaus_agent_core::goal::steering::continuation_prompt;
use klaus_agent_core::goal::types::{ThreadGoal, ThreadGoalStatus};

#[test]
fn automatic_continuation_requires_evidence_before_treating_a_wait_as_terminal() {
    let goal = ThreadGoal {
        thread_id: "progress-fixture".into(),
        goal_id: "progress-goal".into(),
        objective: "Finish the authorized task.".into(),
        status: ThreadGoalStatus::Active,
        token_budget: None,
        tokens_used: 0,
        time_used_seconds: 0,
        created_at_ms: 1,
        updated_at_ms: 1,
    };
    let prompt = continuation_prompt(&goal);
    for required in [
        "No-progress check:",
        "progress, a verified wait, or no progress",
        "confirmed live now",
        "An observation timeout or transient polling failure is not terminal",
        "never restart solely because observation expired",
        "Treat equivalent blockers as the same condition across turns",
        "meaningfully multi-step",
        "do not treat a checklist update as a substitute for doing the work",
    ] {
        assert!(
            prompt.contains(required),
            "missing continuation boundary: {required}"
        );
    }
}
