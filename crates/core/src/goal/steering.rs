//! The instructions the engine injects on the goal's behalf.
//!
//! Each is a fixed template with the goal's own numbers substituted in. The
//! objective is XML-escaped and wrapped in a tag that names it as data: an
//! objective is user text, and a goal that keeps being re-injected every turn
//! is exactly the place where "ignore your instructions" would otherwise get a
//! free ride into the system position.

use crate::goal::types::ThreadGoal;

const CONTINUATION_TEMPLATE: &str = include_str!("templates/continuation.md");
const BUDGET_LIMIT_TEMPLATE: &str = include_str!("templates/budget_limit.md");
const OBJECTIVE_UPDATED_TEMPLATE: &str = include_str!("templates/objective_updated.md");
const COMPLETION_REPORT_TEMPLATE: &str = include_str!("templates/completion_report.md");

pub(crate) fn escape_xml_text(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn render(template: &str, pairs: &[(&str, &str)]) -> String {
    // Scan the template once. Inserted user data is never interpreted as a
    // second template, even when it contains a placeholder-shaped string.
    let mut out = String::with_capacity(template.len());
    let mut remaining = template;
    while let Some(start) = remaining.find("{{ ") {
        out.push_str(&remaining[..start]);
        let tail = &remaining[start + 3..];
        let Some(end) = tail.find(" }}") else {
            out.push_str(&remaining[start..]);
            return out;
        };
        let key = &tail[..end];
        if let Some((_, value)) = pairs.iter().find(|(name, _)| *name == key) {
            out.push_str(value);
        } else {
            out.push_str(&remaining[start..start + 3 + end + 3]);
        }
        remaining = &tail[end + 3..];
    }
    out.push_str(remaining);
    out
}

/// `(tokens_used, token_budget, remaining_tokens)` as the templates print them.
/// A goal with no budget reports `none` / `unknown` rather than a fake number.
fn budget_fields(goal: &ThreadGoal) -> (String, String, String) {
    let tokens_used = goal.tokens_used.to_string();
    match goal.token_budget {
        Some(budget) => (
            tokens_used,
            budget.to_string(),
            goal.remaining_tokens().unwrap_or(0).to_string(),
        ),
        None => (tokens_used, "none".to_string(), "unknown".to_string()),
    }
}

fn objective_text(goal: &ThreadGoal) -> String {
    escape_xml_text(&goal.objective.try_to_utf8().unwrap_or_default())
}

/// The instruction that starts an automatic continuation turn.
pub fn continuation_prompt(goal: &ThreadGoal) -> String {
    continuation_prompt_with_progress(goal, true)
}

pub fn continuation_prompt_with_progress(goal: &ThreadGoal, progress_enabled: bool) -> String {
    let template = if progress_enabled {
        CONTINUATION_TEMPLATE.to_string()
    } else {
        let start = CONTINUATION_TEMPLATE
            .find("Progress visibility:\n")
            .expect("progress section exists");
        let end = CONTINUATION_TEMPLATE[start..]
            .find("Fidelity:\n")
            .expect("fidelity follows progress")
            + start;
        format!(
            "{}{}",
            &CONTINUATION_TEMPLATE[..start],
            &CONTINUATION_TEMPLATE[end..]
        )
    };
    let (tokens_used, token_budget, mut remaining_tokens) = budget_fields(goal);
    if goal.token_budget.is_none() {
        remaining_tokens = "unbounded".to_owned();
    }
    render(
        &template,
        &[
            ("objective", &objective_text(goal)),
            ("tokens_used", &tokens_used),
            ("token_budget", &token_budget),
            ("remaining_tokens", &remaining_tokens),
        ],
    )
}

/// The wrap-up instruction injected once, when the budget stops the goal.
pub fn budget_limit_prompt(goal: &ThreadGoal) -> String {
    let (tokens_used, token_budget, _remaining) = budget_fields(goal);
    render(
        BUDGET_LIMIT_TEMPLATE,
        &[
            ("objective", &objective_text(goal)),
            ("time_used_seconds", &goal.time_used_seconds.to_string()),
            ("tokens_used", &tokens_used),
            ("token_budget", &token_budget),
        ],
    )
}

/// The instruction injected when the user edits the objective mid-flight.
pub fn objective_updated_prompt(goal: &ThreadGoal) -> String {
    let (tokens_used, token_budget, remaining_tokens) = budget_fields(goal);
    render(
        OBJECTIVE_UPDATED_TEMPLATE,
        &[
            ("objective", &objective_text(goal)),
            ("tokens_used", &tokens_used),
            ("token_budget", &token_budget),
            ("remaining_tokens", &remaining_tokens),
        ],
    )
}

/// The instruction appended to a successful `update_goal(complete)`.
///
/// It rides the TOOL RESULT rather than a reminder because it must land exactly
/// once, at the moment the model claimed the objective was met, and reminders
/// fire per turn. The model reads its own tool result, so this is also the only
/// place the instruction cannot be missed.
pub fn completion_report_prompt() -> String {
    COMPLETION_REPORT_TEMPLATE.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::goal::types::ThreadGoalStatus;

    fn goal() -> ThreadGoal {
        ThreadGoal {
            thread_id: "t1".into(),
            goal_id: "g1".into(),
            objective: "make <b>&</b> safe".into(),
            status: ThreadGoalStatus::Active,
            token_budget: Some(1_000),
            tokens_used: 250,
            time_used_seconds: 42,
            created_at_ms: 1,
            updated_at_ms: 2,
        }
    }

    #[test]
    fn continuation_substitutes_every_placeholder() {
        let rendered = continuation_prompt(&goal());
        assert!(
            !rendered.contains("{{"),
            "unsubstituted placeholder: {rendered}"
        );
        assert!(rendered.contains("- Tokens used: 250"));
        assert!(rendered.contains("- Token budget: 1000"));
        assert!(rendered.contains("- Tokens remaining: 750"));
        // The audits the model is held to must survive rendering.
        assert!(rendered.contains("Completion audit:"));
        assert!(rendered.contains("Blocked audit:"));
        assert!(rendered.contains("at least three consecutive goal turns"));
    }

    #[test]
    fn the_objective_is_escaped_and_tagged_as_data() {
        let rendered = continuation_prompt(&goal());
        assert!(rendered.contains("<objective>\nmake &lt;b&gt;&amp;&lt;/b&gt; safe\n</objective>"));
        assert!(rendered.contains("user-provided data"));
    }

    #[test]
    fn a_budgetless_goal_reports_none_rather_than_a_number() {
        let mut budgetless = goal();
        budgetless.token_budget = None;
        let rendered = continuation_prompt(&budgetless);
        assert!(rendered.contains("- Token budget: none"));
        assert!(rendered.contains("- Tokens remaining: unbounded"));
    }

    #[test]
    fn budget_limit_reports_the_spend_and_forbids_new_work() {
        let rendered = budget_limit_prompt(&goal());
        assert!(!rendered.contains("{{"));
        assert!(rendered.contains("- Time spent pursuing goal: 42 seconds"));
        assert!(rendered.contains("do not start new substantive work"));
    }

    #[test]
    fn objective_updated_supersedes_the_previous_objective() {
        let rendered = objective_updated_prompt(&goal());
        assert!(!rendered.contains("{{"));
        assert!(rendered.contains("supersedes any previous thread goal objective"));
        assert!(rendered.contains("<untrusted_objective>"));
    }

    #[test]
    fn completion_report_matches_the_request_without_requiring_an_artifact() {
        let rendered = completion_report_prompt();
        assert!(!rendered.contains("{{"), "{rendered}");
        assert!(
            rendered.contains("the outcome and any material limitation or remaining uncertainty"),
            "{rendered}"
        );
        assert!(rendered.contains("verification evidence"), "{rendered}");
        assert!(rendered.contains("does not require a Canvas"), "{rendered}");
        assert!(!rendered.contains("canvas_check"), "{rendered}");
    }
}
