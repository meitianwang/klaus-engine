//! `create_goal` / `update_goal` / `get_goal` — the model's side of goal mode.
//!
//! Exposed when the host enables goal mode for an eligible main session.
//! Controls require that session's persistent thread runtime; temporary sessions
//! receive an explicit prerequisite error, and children cannot retire a parent goal.
//!
//! The tool descriptions carry the rules the engine cannot enforce mechanically
//! (when a goal may be created at all, and how much evidence a `blocked` claim
//! needs). What IS enforced mechanically lives in [`crate::goal::runtime`]: the
//! model can ask for `complete`, `blocked`, or an explicitly user-requested pause.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use crate::goal::runtime::{GoalCreateOutcome, GoalUpdateOutcome};
use crate::goal::types::{ThreadGoal, ThreadGoalStatus, GOAL_OBJECTIVE_MAX_LENGTH};
use crate::runtime_values::{
    runtime_codec, runtime_json, RuntimeJson, RuntimeObject, RuntimeString,
};
use crate::tools::build_tool::text_result;
use crate::types::errors::ToolError;
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};

/// Tool name for reading the current goal.
pub const GET_GOAL_TOOL_NAME: &str = "get_goal";
/// Tool name for creating a goal.
pub const CREATE_GOAL_TOOL_NAME: &str = "create_goal";
/// Tool name for claiming completion or an impasse.
pub const UPDATE_GOAL_TOOL_NAME: &str = "update_goal";

const GOAL_DISABLED: &str = "Goal mode is not enabled for this session.";
const GOAL_REQUIRES_THREAD: &str = "Goal tools require a persistent thread with goal mode enabled.";

fn runtime_object(fields: impl IntoIterator<Item = (&'static str, RuntimeJson)>) -> RuntimeJson {
    let mut object = RuntimeObject::new();
    for (key, value) in fields {
        object.insert(key, value);
    }
    RuntimeJson::Object(object)
}

/// A `{ ok: false, message }` result flagged as an error.
fn goal_error(message: impl Into<RuntimeString>) -> ToolResult {
    let message = message.into();
    let output = runtime_object([
        ("ok", RuntimeJson::Bool(false)),
        ("message", RuntimeJson::String(message.clone())),
    ]);
    text_result(output, message, true)
}

/// Check native goal integers before exposing them through model JSON numbers.
pub(crate) fn validate_goal_result(goal: &ThreadGoal) -> Result<(), String> {
    const MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;
    let exact = |field: &str, value: i64| {
        if !(-MAX_SAFE_INTEGER..=MAX_SAFE_INTEGER).contains(&value) {
            return Err(format!(
                "The goal field {field} ({value}) cannot be returned as an exact JSON integer; the supported range is -{MAX_SAFE_INTEGER} through {MAX_SAFE_INTEGER}."
            ));
        }
        Ok(())
    };
    for (field, value) in [
        ("tokensUsed", goal.tokens_used),
        ("timeUsedSeconds", goal.time_used_seconds),
        ("createdAtMs", goal.created_at_ms),
        ("updatedAtMs", goal.updated_at_ms),
    ] {
        exact(field, value)?;
    }
    if let Some(budget) = goal.token_budget {
        exact("tokenBudget", budget)?;
    }
    if let Some(remaining) = goal.remaining_tokens() {
        exact("remainingTokens", remaining)?;
    }
    Ok(())
}

/// Keep the host's structured success payload and the model's text projection identical.
fn goal_json_result(output: RuntimeJson) -> ToolResult {
    let content = runtime_json::stringify_pretty(&output);
    text_result(output, content, false)
}

fn goal_result(goal: &ThreadGoal) -> ToolResult {
    goal_result_with_report(goal, false)
}

fn goal_result_with_report(goal: &ThreadGoal, completed: bool) -> ToolResult {
    if let Err(error) = validate_goal_result(goal) {
        return goal_error(error);
    }
    let mut object = RuntimeObject::new();
    object.insert("ok", RuntimeJson::Bool(true));
    object.insert(
        "goal",
        runtime_codec::encode_value(goal).expect("ThreadGoal encodes to exact JSON"),
    );
    if let Some(remaining) = goal.remaining_tokens() {
        object.insert("remainingTokens", RuntimeJson::from(remaining));
    }
    if completed
        && goal.status == ThreadGoalStatus::Complete
        && (goal.token_budget.is_some() || goal.time_used_seconds > 0)
    {
        object.insert(
            "completionBudgetReport",
            RuntimeJson::String(budget_report(goal)),
        );
    }
    goal_json_result(RuntimeJson::Object(object))
}

/// The one-line spend report a completed goal owes the user.
fn budget_report(goal: &ThreadGoal) -> RuntimeString {
    let spend = match goal.token_budget {
        Some(budget) => format!("{} of {} budgeted tokens", goal.tokens_used, budget),
        None => format!("{} tokens", goal.tokens_used),
    };
    let objective =
        crate::goal::steering::escape_xml_text(&goal.objective.try_to_utf8().unwrap_or_default());
    // The spend, the objective, and then the report instruction. The objective is
    // repeated because the report is written FROM this text block and the model
    // may be many turns past the request that set it.
    RuntimeString::from(format!(
        "Goal complete: used {spend} over {} seconds.\n\nObjective:\n<objective>\n{objective}\n</objective>\n\n{}",
        goal.time_used_seconds,
        crate::goal::steering::completion_report_prompt()
    ))
}

fn empty_object_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {},
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .unwrap_or_default()
}

// ---- get_goal ----------------------------------------------------------------

/// Read the thread's goal.
pub struct GetGoalTool {
    input_schema: JsonSchema,
}

/// Construct the `get_goal` tool.
pub fn create_get_goal_tool() -> Arc<dyn Tool> {
    Arc::new(GetGoalTool {
        input_schema: empty_object_schema(),
    })
}

#[async_trait]
impl Tool for GetGoalTool {
    fn is_goal_state_control(&self) -> bool {
        true
    }

    fn static_name(&self) -> &str {
        GET_GOAL_TOOL_NAME
    }

    fn static_description(&self) -> &str {
        "Get the current goal for this thread, including status, budgets, token and elapsed-time usage, and remaining token budget."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn static_search_hint(&self) -> Option<&str> {
        Some("read the thread's persisted goal")
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        true
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        true
    }

    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        Some(false)
    }

    async fn call(
        &self,
        _input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let Some(runtime) = &ctx.goal else {
            return Ok(goal_error(GOAL_REQUIRES_THREAD));
        };
        if !runtime.enabled() {
            return Ok(goal_error(GOAL_DISABLED));
        }
        match runtime.goal().await {
            Ok(Some(goal)) => Ok(goal_result(&goal)),
            Ok(None) => Ok(goal_json_result(runtime_object([
                ("ok", RuntimeJson::Bool(true)),
                ("goal", RuntimeJson::Null),
            ]))),
            Err(error) => Ok(goal_error(format!(
                "The stored goal could not be read: {error}"
            ))),
        }
    }
}

// ---- create_goal -------------------------------------------------------------

fn create_goal_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "objective": {
                "type": "string",
                "description": "Required. The concrete objective to start pursuing. This starts a new active goal when no goal exists or replaces the current goal when it is complete."
            },
            "token_budget": {
                "type": "integer",
                "description": "Positive token budget for the new goal. Omit unless explicitly requested."
            }
        },
        "required": ["objective"],
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .unwrap_or_default()
}

/// Create the thread's goal.
pub struct CreateGoalTool {
    input_schema: JsonSchema,
    description: String,
}

/// Construct the `create_goal` tool.
pub fn create_create_goal_tool() -> Arc<dyn Tool> {
    Arc::new(CreateGoalTool {
        input_schema: create_goal_schema(),
        description: format!(
            "Create a goal only when explicitly requested by the user or system/developer instructions; do not infer goals from ordinary tasks.\nSet token_budget only when an explicit token budget is requested. Fails if an unfinished goal exists; use {UPDATE_GOAL_TOOL_NAME} only for status."
        ),
    })
}

#[async_trait]
impl Tool for CreateGoalTool {
    fn is_goal_state_control(&self) -> bool {
        true
    }

    fn static_name(&self) -> &str {
        CREATE_GOAL_TOOL_NAME
    }

    fn static_description(&self) -> &str {
        &self.description
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn input_schema_property_order(
        &self,
        object_path: &[String],
    ) -> Option<&'static [&'static str]> {
        object_path
            .is_empty()
            .then_some(&["objective", "token_budget"])
    }

    fn static_search_hint(&self) -> Option<&str> {
        Some("start pursuing a persisted objective")
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        false
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        false
    }

    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        Some(false)
    }

    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        let objective = input
            .get("objective")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .unwrap_or_default();
        Some(RuntimeString::from("Setting goal: ").concat(&objective))
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let Some(runtime) = &ctx.goal else {
            return Ok(goal_error(GOAL_REQUIRES_THREAD));
        };
        let Some(objective) = input
            .get("objective")
            .and_then(RuntimeJson::as_str)
            .cloned()
        else {
            return Ok(goal_error(format!(
                "objective is required and is limited to {GOAL_OBJECTIVE_MAX_LENGTH} characters"
            )));
        };
        let token_budget =
            match input.get("token_budget") {
                None => None,
                Some(value) => match value.as_number().map(|number| number.as_f64()) {
                    Some(value)
                        if value.is_finite()
                            && value > 0.0
                            && value.fract() == 0.0
                            && value <= 9_007_199_254_740_991.0 =>
                    {
                        Some(value as i64)
                    }
                    _ => return Ok(goal_error(
                        "token_budget must be a positive safe integer (at most 9007199254740991)",
                    )),
                },
            };
        let expected = ctx.goal_generation.as_ref().map(|scope| {
            scope
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        });
        match runtime.create_goal_checked_with_signal(objective, token_budget, expected.as_ref(), &ctx.signal).await {
            GoalCreateOutcome::Created(goal) => {
                if let Some(scope) = &ctx.goal_generation {
                    let mut generation = scope.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                    let captured = expected.as_ref().expect("a scoped tool captured its identity");
                    generation.generation = Some(goal.goal_id.clone());
                    generation.intent = captured.intent.saturating_add(1);
                    generation.durable_intent = captured.durable_intent.saturating_add(1);
                    generation.goal_work = captured.accounting_enabled;
                    generation.provider_admitted = captured.provider_admitted;
                }
                Ok(goal_result(&goal))
            }
            GoalCreateOutcome::Rejected(existing) => Ok(goal_error(format!(
                "A goal is already {} for this thread.\n\n<objective>\n{}\n</objective>\n\nFinish it with {UPDATE_GOAL_TOOL_NAME} or ask the user to clear it before creating another.",
                existing.status.wire_name(),
                crate::goal::steering::escape_xml_text(
                    &existing.objective.try_to_utf8().unwrap_or_default()
                )
            ))),
            GoalCreateOutcome::InvalidObjective(message) => Ok(goal_error(message)),
        }
    }
}

// ---- update_goal -------------------------------------------------------------

fn update_goal_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "status": {
                "type": "string",
                "enum": ["complete", "blocked", "paused"],
                "description": "Required. Set to `paused` only in response to an explicit user request to pause; a later resume revokes that request. Budget limits take precedence. Set to `complete` only when the objective is achieved and no required work remains. Set to `blocked` only after the same blocking condition has recurred for at least three consecutive goal turns and the agent is at an impasse. After a previously blocked goal is resumed, the resumed run starts a fresh blocked audit."
            }
        },
        "required": ["status"],
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .unwrap_or_default()
}

/// Claim completion or an impasse.
pub struct UpdateGoalTool {
    input_schema: JsonSchema,
}

/// Construct the `update_goal` tool.
pub fn create_update_goal_tool() -> Arc<dyn Tool> {
    Arc::new(UpdateGoalTool {
        input_schema: update_goal_schema(),
    })
}

#[async_trait]
impl Tool for UpdateGoalTool {
    fn is_goal_state_control(&self) -> bool {
        true
    }

    fn static_name(&self) -> &str {
        UPDATE_GOAL_TOOL_NAME
    }

    fn static_description(&self) -> &str {
        "Update the existing goal.\nUse this tool to mark the goal achieved, genuinely blocked, or paused at the user's explicit request.\nSet status to `paused` only at the user's explicit request to pause this goal, never on your own initiative. Ask if unclear; a later resume revokes that request. Report the returned status and stop goal work. Budget limits take precedence over pausing.\nSet status to `complete` only when the objective has actually been achieved and no required work remains.\nSet status to `blocked` only when the same blocking condition has repeated for at least three consecutive goal turns, counting the original/user-triggered turn and any automatic continuations, and the agent cannot make meaningful progress without user input or an external-state change.\nIf the user resumes a goal that was previously marked `blocked`, treat the resumed run as a fresh blocked audit. If the same blocking condition then repeats for at least three consecutive resumed goal turns, set status to `blocked` again.\nOnce the blocked threshold is satisfied, do not keep reporting that you are still blocked while leaving the goal active; set status to `blocked`.\nDo not use `blocked` merely because the work is hard, slow, uncertain, incomplete, or would benefit from clarification.\nDo not mark a goal complete merely because its budget is nearly exhausted or because you are stopping work.\nYou cannot use this tool to resume, budget-limit, or usage-limit a goal; those status changes are controlled by the user or system.\nWhen marking a budgeted goal achieved with status `complete`, report the successful tool result's actual goal.tokensUsed, goal.tokenBudget, and goal.timeUsedSeconds to the user. Describe positive elapsed time in a human-friendly form. Do not estimate final usage from the budget ceiling."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn static_search_hint(&self) -> Option<&str> {
        Some("mark the goal complete or blocked")
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        false
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        false
    }

    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        Some(false)
    }

    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        let status = input
            .get("status")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .unwrap_or_default();
        Some(RuntimeString::from("Marking goal ").concat(&status))
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let Some(runtime) = &ctx.goal else {
            return Ok(goal_error(GOAL_REQUIRES_THREAD));
        };
        let status =
            input
                .get("status")
                .and_then(RuntimeJson::as_str)
                .and_then(|value| match value.try_to_utf8().ok()?.as_str() {
                    "complete" => Some(ThreadGoalStatus::Complete),
                    "blocked" => Some(ThreadGoalStatus::Blocked),
                    "paused" => Some(ThreadGoalStatus::Paused),
                    _ => None,
                });
        let Some(status) = status else {
            return Ok(goal_error(
                "status must be `complete`, `blocked`, or an explicitly user-requested `paused`; other statuses are set by the user or system",
            ));
        };
        let expected = ctx.goal_generation.as_ref().map(|scope| {
            scope
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        });
        if ctx.goal_generation.is_some()
            && expected
                .as_ref()
                .is_none_or(|identity| identity.generation.is_none())
        {
            return Ok(goal_error("This turn has not adopted a goal generation; read the current goal before a new explicit turn."));
        }
        let outcome = runtime
            .update_goal_status_for_tool_with_signal(status, expected.as_ref(), &ctx.signal)
            .await;
        match outcome {
            GoalUpdateOutcome::Updated(goal) => {
                if let Some(scope) = &ctx.goal_generation {
                    let mut identity = scope
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let captured = expected
                        .as_ref()
                        .expect("a scoped tool captured its identity");
                    identity.generation = Some(goal.goal_id.clone());
                    identity.intent = captured.intent;
                    identity.durable_intent = captured.durable_intent.saturating_add(1);
                    identity.goal_work = false;
                    identity.provider_admitted = captured.provider_admitted;
                }
                Ok(goal_result_with_report(
                    &goal,
                    status == ThreadGoalStatus::Complete,
                ))
            }
            GoalUpdateOutcome::Missing => Ok(goal_error(
                "No goal is set for this thread, so there is nothing to update.",
            )),
            GoalUpdateOutcome::Forbidden(message) => Ok(goal_error(message)),
        }
    }
}

/// The three goal tools, in the order the model sees them.
pub fn create_goal_tools() -> Vec<Arc<dyn Tool>> {
    vec![
        create_get_goal_tool(),
        create_create_goal_tool(),
        create_update_goal_tool(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::goal::types::ThreadGoalStatus;

    fn goal(status: ThreadGoalStatus, budget: Option<i64>) -> ThreadGoal {
        ThreadGoal {
            thread_id: "t1".into(),
            goal_id: "g1".into(),
            objective: "ship it".into(),
            status,
            token_budget: budget,
            tokens_used: 250,
            time_used_seconds: 30,
            created_at_ms: 1,
            updated_at_ms: 2,
        }
    }

    #[test]
    fn tool_names_and_shapes_match_the_contract() {
        let tools = create_goal_tools();
        let names: Vec<RuntimeString> = tools.iter().map(|tool| tool.name()).collect();
        assert_eq!(
            names,
            vec![
                RuntimeString::from("get_goal"),
                RuntimeString::from("create_goal"),
                RuntimeString::from("update_goal"),
            ]
        );
        // get_goal takes nothing and mutates nothing.
        let get = &tools[0];
        assert!(get.is_read_only(&RuntimeJson::Object(RuntimeObject::new())));
        assert!(get.input_schema().get("required").is_none());
        // create_goal requires an objective.
        let create = &tools[1];
        assert!(!create.is_read_only(&RuntimeJson::Object(RuntimeObject::new())));
        let required = create
            .input_schema()
            .get("required")
            .and_then(RuntimeJson::as_array)
            .cloned()
            .unwrap_or_default();
        assert_eq!(required.len(), 1);
        assert_eq!(
            required[0].as_string().cloned(),
            Some(RuntimeString::from("objective"))
        );
    }

    #[test]
    fn update_goal_offers_completion_blocking_and_explicit_user_pause() {
        let tools = create_goal_tools();
        let update = &tools[2];
        let values = update
            .input_schema()
            .get("properties")
            .and_then(|properties| properties.get("status"))
            .and_then(|status| status.get("enum"))
            .and_then(RuntimeJson::as_array)
            .cloned()
            .unwrap_or_default();
        let values: Vec<RuntimeString> = values
            .iter()
            .filter_map(|value| value.as_string().cloned())
            .collect();
        assert_eq!(
            values,
            vec![
                RuntimeString::from("complete"),
                RuntimeString::from("blocked"),
                RuntimeString::from("paused")
            ],
            "resuming and system limits remain outside model authority"
        );
    }

    #[tokio::test]
    async fn every_goal_tool_reports_the_feature_being_off() {
        for tool in create_goal_tools() {
            let ctx = crate::tools::builtins::test_ctx::make_ctx(".");
            let result = tool
                .call(RuntimeJson::Object(RuntimeObject::new()), &ctx, None)
                .await
                .expect("the tool answers rather than throwing");
            assert_eq!(
                result.serializable.is_error,
                Some(true),
                "{:?} must refuse without an enabled persistent runtime",
                tool.name()
            );
            assert_eq!(
                result
                    .serializable
                    .output
                    .get("message")
                    .and_then(RuntimeJson::as_str),
                Some(&RuntimeString::from(GOAL_REQUIRES_THREAD))
            );
            let [crate::schemas::content::OutputBlock::Text(text)] =
                result.serializable.model_content.as_slice()
            else {
                panic!("the refusal must explain its prerequisite to the model")
            };
            assert_eq!(text.text, GOAL_REQUIRES_THREAD);
        }
        let mut ctx = crate::tools::builtins::test_ctx::make_ctx(".");
        let runtime = Arc::new(crate::goal::runtime::GoalRuntime::new(
            "disabled-thread".into(),
            Arc::new(|| "disabled-generation".to_owned()),
            Arc::new(|| 0),
            None,
        ));
        runtime.set_initial_enabled(false);
        ctx.goal = Some(runtime);
        let result = create_get_goal_tool()
            .call(RuntimeJson::Object(RuntimeObject::new()), &ctx, None)
            .await
            .unwrap();
        assert_eq!(result.serializable.is_error, Some(true));
        assert_eq!(
            result
                .serializable
                .output
                .get("message")
                .and_then(RuntimeJson::as_str),
            Some(&RuntimeString::from(GOAL_DISABLED))
        );
    }

    #[test]
    fn a_completed_goal_reports_its_spend_and_request_shaped_completion() {
        let with_budget = budget_report(&goal(ThreadGoalStatus::Complete, Some(1_000)))
            .try_to_utf8()
            .unwrap();
        assert!(
            with_budget
                .starts_with("Goal complete: used 250 of 1000 budgeted tokens over 30 seconds."),
            "{with_budget}"
        );
        let without = budget_report(&goal(ThreadGoalStatus::Complete, None))
            .try_to_utf8()
            .unwrap();
        assert!(
            without.starts_with("Goal complete: used 250 tokens over 30 seconds."),
            "{without}"
        );

        // Completion still carries reporting guidance without choosing an artifact.
        for text in [&with_budget, &without] {
            assert!(text.contains("verification evidence"), "{text}");
            assert!(text.contains("does not require a Canvas"), "{text}");
            // The objective travels with it — the report is written from this
            // block, possibly many turns after the request that set the goal.
            assert!(
                text.contains("Objective:\n<objective>\nship it\n</objective>"),
                "{text}"
            );
        }
    }

    #[test]
    fn a_blocked_goal_is_not_asked_for_a_report() {
        // An impasse owes the user a plain account of what is stuck. Dressing a
        // dead end up as a rendered artifact reads like progress.
        let result = goal_result(&goal(ThreadGoalStatus::Blocked, None));
        assert!(result
            .serializable
            .output
            .get("completionBudgetReport")
            .is_none());
        let visible = model_json(&result);
        assert_eq!(visible, result.serializable.output);
        assert!(!runtime_json::stringify(&visible)
            .to_node_utf8()
            .windows(6)
            .any(|part| part == b"canvas"));
    }

    fn model_json(result: &ToolResult) -> RuntimeJson {
        let [crate::schemas::content::OutputBlock::Text(text)] =
            result.serializable.model_content.as_slice()
        else {
            panic!("the model receives one complete JSON result")
        };
        runtime_json::parse(&text.text).unwrap()
    }

    #[test]
    fn the_success_payload_carries_the_goal_and_the_remaining_budget() {
        let missing = goal_json_result(runtime_object([
            ("ok", RuntimeJson::Bool(true)),
            ("goal", RuntimeJson::Null),
        ]));
        assert_eq!(model_json(&missing), missing.serializable.output);
        assert_eq!(model_json(&missing).get("goal"), Some(&RuntimeJson::Null));
        let result = goal_result(&goal(ThreadGoalStatus::Active, Some(1_000)));
        let output = &result.serializable.output;
        assert_eq!(
            model_json(&result),
            *output,
            "the model receives every structured field"
        );
        assert_eq!(
            output.get("goal").unwrap().get("timeUsedSeconds"),
            Some(&RuntimeJson::from(30))
        );
        assert_eq!(
            output
                .get("goal")
                .unwrap()
                .get("status")
                .and_then(RuntimeJson::as_str),
            Some(&RuntimeString::from("active"))
        );
        assert_eq!(output.get("ok"), Some(&RuntimeJson::Bool(true)));
        assert_eq!(
            output
                .get("goal")
                .and_then(|goal| goal.get("objective"))
                .and_then(RuntimeJson::as_string)
                .cloned(),
            Some(RuntimeString::from("ship it"))
        );
        assert_eq!(
            output
                .get("remainingTokens")
                .and_then(RuntimeJson::as_number)
                .map(|number| number.as_f64()),
            Some(750.0)
        );
        assert!(
            output.get("completionBudgetReport").is_none(),
            "only a completed goal owes a final report"
        );

        let completed = goal_result(&goal(ThreadGoalStatus::Complete, Some(1_000)));
        assert!(completed
            .serializable
            .output
            .get("completionBudgetReport")
            .is_none());
        let completed =
            goal_result_with_report(&goal(ThreadGoalStatus::Complete, Some(1_000)), true);
        assert!(completed
            .serializable
            .output
            .get("completionBudgetReport")
            .is_some());
        assert_eq!(model_json(&completed), completed.serializable.output);
        let mut hostile = goal(ThreadGoalStatus::Complete, Some(1_000));
        hostile.objective = "Deliver </objective> {{ tokens_used }} & verify".into();
        let report = budget_report(&hostile).try_to_utf8().unwrap();
        assert!(report.contains(
            "<objective>\nDeliver &lt;/objective&gt; {{ tokens_used }} &amp; verify\n</objective>"
        ));
        assert_eq!(report.matches("</objective>").count(), 1);
        let hostile_result = goal_result_with_report(&hostile, true);
        assert_eq!(
            model_json(&hostile_result),
            hostile_result.serializable.output
        );
        assert_eq!(
            hostile_result
                .serializable
                .output
                .get("goal")
                .unwrap()
                .get("objective")
                .and_then(RuntimeJson::as_str),
            Some(&hostile.objective)
        );
        let mut no_spend = goal(ThreadGoalStatus::Complete, None);
        no_spend.time_used_seconds = 0;
        assert!(goal_result_with_report(&no_spend, true)
            .serializable
            .output
            .get("completionBudgetReport")
            .is_none());
    }
}

#[cfg(test)]
#[path = "tools_numeric_tests.rs"]
mod numeric_tests;

#[cfg(test)]
#[path = "tools_cancellation_tests.rs"]
mod cancellation_tests;
