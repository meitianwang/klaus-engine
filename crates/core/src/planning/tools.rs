//! Structured root-thread questions and the execution progress checklist.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use crate::planning::runtime::PlanningRuntime;
use crate::planning::types::CollaborationMode;
use crate::runtime_values::{RuntimeMap, RuntimeObject, RuntimeString};
use crate::tools::build_tool::text_result;
use crate::types::errors::ToolError;
use crate::types::events::ToolDomainEvent;
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{
    ElicitRequest, ElicitResponse, Tool, ToolContext, ToolProgress, ToolResult,
};

pub const REQUEST_USER_INPUT_TOOL_NAME: &str = "request_user_input";
pub const UPDATE_PLAN_TOOL_NAME: &str = "update_plan";

const QUESTION_ORDER: &[&str] = &["id", "header", "question", "options", "isSecret", "isOther"];
const OPTION_ORDER: &[&str] = &["label", "description"];
const PROGRESS_ORDER: &[&str] = &["explanation", "plan"];
const STEP_ORDER: &[&str] = &["step", "status"];

fn request_user_input_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "questions": {
                "type": "array", "minItems": 1, "maxItems": 3,
                "description": "Questions to show the user. Prefer 1 and do not exceed 3.",
                "items": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string", "minLength": 1, "description": "Stable identifier for mapping answers (snake_case). Preserve this identifier when the question wording changes."},
                        "header": {"type": "string", "minLength": 1, "maxLength": 12, "description": "Short header label shown in the UI (12 or fewer characters)."},
                        "question": {"type": "string", "minLength": 1, "description": "The complete single-sentence prompt shown to the user."},
                        "options": {
                            "type": "array", "minItems": 2, "maxItems": 3,
                            "description": "Provide 2-3 meaningful, mutually exclusive choices. Put the recommended option first and suffix its label with \"(Recommended)\". Do not include an \"Other\" option; the client adds free-form Other input automatically.",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "label": {"type": "string", "minLength": 1, "description": "User-facing label (1-5 words)."},
                                    "description": {"type": "string", "minLength": 1, "description": "One short sentence explaining the impact or tradeoff if selected."}
                                },
                                "required": ["label", "description"], "additionalProperties": false
                            }
                        },
                        "isSecret": {"type": "boolean", "description": "Whether the client should conceal typed input for this question."},
                        "isOther": {"type": "boolean", "description": "Protocol metadata. The client always provides free-form Other input for this tool."}
                    },
                    "required": ["id", "header", "question", "options"], "additionalProperties": false
                }
            }
        },
        "required": ["questions"], "additionalProperties": false
    })
    .as_object()
    .cloned()
    .expect("the question schema is an object")
}

fn update_plan_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "explanation": {"type": "string", "description": "Optional explanation for this plan update."},
            "plan": {
                "type": "array", "description": "The ordered list of progress steps.",
                "items": {
                    "type": "object",
                    "properties": {
                        "step": {"type": "string", "minLength": 1, "description": "Task step text."},
                        "status": {"type": "string", "enum": ["pending", "in_progress", "completed"], "description": "Step status."}
                    },
                    "required": ["step", "status"], "additionalProperties": false
                }
            }
        },
        "required": ["plan"], "additionalProperties": false
    })
    .as_object()
    .cloned()
    .expect("the progress schema is an object")
}

fn question_property_order(path: &[String]) -> Option<&'static [&'static str]> {
    match path {
        [] => Some(&["questions"]),
        [questions, _] if questions == "questions" => Some(QUESTION_ORDER),
        [questions, _, options, _] if questions == "questions" && options == "options" => {
            Some(OPTION_ORDER)
        }
        _ => None,
    }
}

fn progress_property_order(path: &[String]) -> Option<&'static [&'static str]> {
    match path {
        [] => Some(PROGRESS_ORDER),
        [plan, _] if plan == "plan" => Some(STEP_ORDER),
        _ => None,
    }
}

fn is_blank(value: &RuntimeString) -> bool {
    value
        .as_utf16()
        .iter()
        .all(|unit| char::from_u32(u32::from(*unit)).is_some_and(char::is_whitespace))
}

fn required_text<'a>(
    object: &'a RuntimeObject,
    field: &str,
) -> Result<&'a RuntimeString, ToolError> {
    object
        .get(field)
        .and_then(Json::as_str)
        .filter(|value| !is_blank(value))
        .ok_or_else(|| ToolError::new(format!("{field} must be a nonempty string")))
}

fn parse_question_input(schema: &Json, input: &Json) -> Result<Json, ToolError> {
    let mut parsed =
        crate::tools::input_parser::parse_native_input(schema, input, &question_property_order)?;
    let questions = parsed
        .get_mut("questions")
        .and_then(Json::as_array_mut)
        .expect("the schema validates the question array");
    let mut ids = HashSet::new();
    for question in questions {
        let question = question
            .as_object_mut()
            .expect("the schema validates each question object");
        if !ids.insert(required_text(question, "id")?.clone()) {
            return Err(ToolError::new("Question identifiers must be unique"));
        }
        required_text(question, "header")?;
        required_text(question, "question")?;
        let mut labels = HashSet::new();
        for option in question
            .get("options")
            .and_then(Json::as_array)
            .expect("the schema validates the options array")
        {
            let option = option.as_object().expect("validated option object");
            if !labels.insert(required_text(option, "label")?.clone()) {
                return Err(ToolError::new(
                    "Option labels must be unique within each question",
                ));
            }
            required_text(option, "description")?;
        }
        question.insert("isOther", Json::Bool(true));
        if !question.contains_key("isSecret") {
            question.insert("isSecret", Json::Bool(false));
        }
    }
    Ok(parsed)
}

fn parse_progress_input(schema: &Json, input: &Json) -> Result<Json, ToolError> {
    let parsed =
        crate::tools::input_parser::parse_native_input(schema, input, &progress_property_order)?;
    let steps = parsed
        .get("plan")
        .and_then(Json::as_array)
        .expect("the schema validates the progress array");
    let mut in_progress = 0;
    for step in steps {
        let step = step.as_object().expect("validated progress step");
        required_text(step, "step")?;
        if step
            .get("status")
            .and_then(Json::as_str)
            .is_some_and(|status| status == "in_progress")
        {
            in_progress += 1;
        }
    }
    if in_progress > 1 {
        return Err(ToolError::new(
            "At most one step can be in_progress at a time",
        ));
    }
    Ok(parsed)
}

fn require_root(ctx: &ToolContext, tool: &str) -> Result<(), ToolError> {
    if ctx.depth.unwrap_or(0) != 0 {
        return Err(ToolError::new(format!(
            "{tool} can only be used by the root thread"
        )));
    }
    Ok(())
}

async fn emit_status(ctx: &ToolContext, on_progress: &Option<ToolProgress>, message: &str) {
    let event = ToolDomainEvent::Status {
        message: message.into(),
    };
    if let Some(progress) = on_progress {
        progress(event).await;
    } else {
        (ctx.emit)(event).await;
    }
}

fn question_result(
    questions: &[RuntimeObject],
    answers: &RuntimeMap<Vec<RuntimeString>>,
    status: RuntimeString,
) -> Result<ToolResult, ToolError> {
    if !["answered", "skipped", "cancelled", "expired"]
        .iter()
        .any(|allowed| status == *allowed)
    {
        return Err(ToolError::new(
            "The client returned an unknown question status",
        ));
    }
    if status != "answered" && !answers.is_empty() {
        return Err(ToolError::new(
            "Unanswered question requests must not contain answers",
        ));
    }
    if status == "answered" && answers.is_empty() {
        return Err(ToolError::new(
            "An answered question request must contain at least one answer",
        ));
    }
    let mut answer_object = RuntimeObject::new();
    for (id, values) in answers.entries() {
        if !questions
            .iter()
            .any(|question| question.get("id").and_then(Json::as_str) == Some(&id))
        {
            return Err(ToolError::new(
                "The client answered an unknown question identifier",
            ));
        }
        if values.is_empty() || values.iter().any(is_blank) {
            return Err(ToolError::new(
                "The client returned an empty question answer",
            ));
        }
        let mut answer = RuntimeObject::new();
        answer.insert(
            "answers",
            Json::Array(values.iter().cloned().map(Json::String).collect()),
        );
        answer_object.insert(id, Json::Object(answer));
    }
    let mut output = RuntimeObject::new();
    output.insert("answers", Json::Object(answer_object));
    output.insert("status", Json::String(status));
    let output = Json::Object(output);
    let text = crate::runtime_values::runtime_json::stringify(&output);
    Ok(text_result(output, text, false))
}

struct RequestUserInputTool {
    runtime: Arc<PlanningRuntime>,
    schema: JsonSchema,
}

impl RequestUserInputTool {
    async fn request(
        &self,
        input: Json,
        ctx: &ToolContext,
        on_progress: Option<ToolProgress>,
        call_id: Option<&RuntimeString>,
    ) -> Result<ToolResult, ToolError> {
        require_root(ctx, REQUEST_USER_INPUT_TOOL_NAME)?;
        let mode = self.runtime.mode();
        if mode != CollaborationMode::Plan && !self.runtime.default_questions_enabled() {
            return Err(ToolError::new(
                "request_user_input is unavailable in Default mode",
            ));
        }
        let input = self.parse_input(&input)?;
        let questions = input
            .get("questions")
            .and_then(Json::as_array)
            .expect("validated questions")
            .iter()
            .map(|question| question.as_object().cloned().expect("validated question"))
            .collect::<Vec<_>>();
        let empty = RuntimeMap::new();
        if ctx.signal.is_cancelled() {
            return question_result(&questions, &empty, "cancelled".into());
        }
        let elicit = ctx.elicit.as_ref().ok_or_else(|| {
            ToolError::new("No elicitation handler is available; cannot ask the user")
        })?;
        let mut metadata = RuntimeObject::new();
        metadata.insert("kind", Json::String(REQUEST_USER_INPUT_TOOL_NAME.into()));
        metadata.insert("isBlocking", Json::Bool(mode == CollaborationMode::Plan));
        if let Some(scope) = &ctx.async_question {
            metadata.insert("turnId", Json::String(scope.turn_id.clone()));
            metadata.insert("toolCallId", Json::String(scope.tool_call_id.clone()));
        }
        if let Some(call_id) = call_id {
            metadata.insert("toolCallId", Json::String(call_id.clone()));
        }
        emit_status(ctx, &on_progress, "Waiting for user input").await;
        let response = elicit
            .elicit(
                ElicitRequest {
                    prompt: required_text(&questions[0], "question")?.clone(),
                    questions: Some(questions.clone()),
                    attachments_supported: Some(false),
                    metadata: Some(metadata),
                    options: None,
                    multi_select: Some(false),
                },
                ctx.signal.clone(),
            )
            .await;
        if ctx.signal.is_cancelled() {
            return question_result(&questions, &empty, "cancelled".into());
        }
        match response {
            ElicitResponse::UserInput { answers, status } => {
                question_result(&questions, &answers, status)
            }
            ElicitResponse::Answers {
                answers,
                attachments,
            } => {
                if attachments.is_some_and(|attachments| !attachments.is_empty()) {
                    return Err(ToolError::new(
                        "request_user_input does not accept question attachments",
                    ));
                }
                let mut arrays = RuntimeMap::new();
                for (id, value) in answers.entries() {
                    arrays.insert(id, vec![value.clone()]);
                }
                let status = if arrays.is_empty() {
                    "skipped"
                } else {
                    "answered"
                };
                question_result(&questions, &arrays, status.into())
            }
            ElicitResponse::Decline { reason } => {
                let status = reason
                    .filter(|reason| reason == "expired" || reason == "skipped")
                    .unwrap_or_else(|| "cancelled".into());
                question_result(&questions, &empty, status)
            }
            ElicitResponse::Accept { .. } => Err(ToolError::new(
                "The client must return structured answers keyed by question identifier",
            )),
        }
    }
}

#[async_trait]
impl Tool for RequestUserInputTool {
    fn static_name(&self) -> &str {
        REQUEST_USER_INPUT_TOOL_NAME
    }

    fn description(&self) -> RuntimeString {
        let availability = if self.runtime.default_questions_enabled() {
            "Plan or Default mode. Questions are blocking in Plan mode and optional in Default mode. In Default mode, ask only optional questions whose answers would materially improve the work; if no answers are returned, continue with best judgment"
        } else {
            "Plan mode. Questions are blocking in Plan mode"
        };
        format!(
            "Request user input for one to three short questions and wait for the response. This tool is only available in {availability}. Only the root thread may use this tool. Never use it for runtime permission requests or permission-related escalations."
        )
        .into()
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }

    fn input_schema_property_order(&self, path: &[String]) -> Option<&'static [&'static str]> {
        question_property_order(path)
    }

    fn parse_input(&self, input: &Json) -> Result<Json, ToolError> {
        parse_question_input(&self.input_schema(), input)
    }

    fn is_enabled(&self) -> bool {
        self.runtime.mode() == CollaborationMode::Plan || self.runtime.default_questions_enabled()
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        false
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        true
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.request(input, ctx, on_progress, None).await
    }

    async fn call_with_id(
        &self,
        call_id: &RuntimeString,
        input: Json,
        ctx: &ToolContext,
        on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.request(input, ctx, on_progress, Some(call_id)).await
    }
}

struct UpdatePlanTool {
    runtime: Arc<PlanningRuntime>,
    schema: JsonSchema,
}

#[async_trait]
impl Tool for UpdatePlanTool {
    fn static_name(&self) -> &str {
        UPDATE_PLAN_TOOL_NAME
    }

    fn static_description(&self) -> &str {
        "Updates the task plan.\nProvide an optional explanation and an ordered list of plan items, each with a step and a pending, in_progress, or completed status.\nAt most one step can be in_progress at a time.\nThis is an execution checklist and progress tool, separate from a proposed plan artifact. It does not enter or exit Plan mode and is not allowed in Plan mode."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }

    fn input_schema_property_order(&self, path: &[String]) -> Option<&'static [&'static str]> {
        progress_property_order(path)
    }

    fn parse_input(&self, input: &Json) -> Result<Json, ToolError> {
        parse_progress_input(&self.input_schema(), input)
    }

    fn is_enabled(&self) -> bool {
        self.runtime.mode() == CollaborationMode::Default
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        false
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        true
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        require_root(ctx, UPDATE_PLAN_TOOL_NAME)?;
        if self.runtime.mode() != CollaborationMode::Default {
            return Err(ToolError::new(
                "update_plan is an execution checklist tool and is not allowed in Plan mode",
            ));
        }
        if ctx.signal.is_cancelled() {
            return Err(ToolError::new("The progress update was cancelled"));
        }
        let progress = self.parse_input(&input)?;
        self.runtime
            .update_progress(progress.clone(), &ctx.emit)
            .await
            .map_err(ToolError::new)?;
        emit_status(ctx, &on_progress, "Plan updated").await;
        Ok(text_result(progress, "Plan updated", false))
    }
}

/// Build the tools once; availability follows the admitted collaboration mode.
pub fn create_planning_tools(runtime: Arc<PlanningRuntime>) -> Vec<Arc<dyn Tool>> {
    vec![
        Arc::new(RequestUserInputTool {
            runtime: Arc::clone(&runtime),
            schema: request_user_input_schema(),
        }),
        Arc::new(UpdatePlanTool {
            runtime,
            schema: update_plan_schema(),
        }),
    ]
}

/// Keep request-boundary availability independent from the saved permission policy.
pub fn retain_for_mode(
    tools: &mut Vec<Arc<dyn Tool>>,
    mode: CollaborationMode,
    root: bool,
    default_questions: bool,
) {
    tools.retain(|tool| match tool.name().try_to_utf8().ok().as_deref() {
        Some("request_user_input") => {
            root && (mode == CollaborationMode::Plan || default_questions)
        }
        Some("update_plan") => root && mode == CollaborationMode::Default,
        Some("enter_plan_mode" | "create_plan" | "suggest_execution_mode") => false,
        Some(
            "write" | "edit" | "multi_edit" | "notebook_edit" | "EnterWorktree" | "ExitWorktree",
        ) => mode != CollaborationMode::Plan,
        _ => true,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::runtime_json;

    fn exact_json(source: &str) -> Json {
        runtime_json::parse(&source.into()).expect("valid test JSON")
    }

    fn question(id: &str) -> Json {
        let mut question = RuntimeObject::new();
        question.insert("id", Json::String(id.into()));
        question.insert("header", Json::String("Storage".into()));
        question.insert(
            "question",
            Json::String("Where should data be stored?".into()),
        );
        question.insert(
            "options",
            exact_json(
                r#"[{"label":"Local (Recommended)","description":"Store on this computer."},{"label":"Remote","description":"Store on a server."}]"#,
            ),
        );
        Json::Object(question)
    }

    fn question_input(questions: Vec<Json>) -> Json {
        let mut input = RuntimeObject::new();
        input.insert("questions", Json::Array(questions));
        Json::Object(input)
    }

    fn parse_questions(input: Json) -> Result<Json, ToolError> {
        parse_question_input(
            &crate::types::json::runtime_schema_from_static(&request_user_input_schema()),
            &input,
        )
    }

    fn parsed_questions(ids: &[&str]) -> Vec<RuntimeObject> {
        parse_questions(question_input(ids.iter().map(|id| question(id)).collect()))
            .unwrap()
            .get("questions")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|question| question.as_object().unwrap().clone())
            .collect()
    }

    fn parse_progress(source: &str) -> Result<Json, ToolError> {
        parse_progress_input(
            &crate::types::json::runtime_schema_from_static(&update_plan_schema()),
            &exact_json(source),
        )
    }

    #[test]
    fn questions_keep_distinct_ids_when_prompts_share_text() {
        let questions = parsed_questions(&["source_storage", "backup_storage"]);
        assert_eq!(questions.len(), 2);
        assert_eq!(questions[0].get("question"), questions[1].get("question"));
        assert_ne!(questions[0].get("id"), questions[1].get("id"));
        assert!(parse_questions(question_input(vec![question("same"), question("same")])).is_err());
    }

    #[test]
    fn questions_enforce_batch_header_and_option_bounds() {
        assert!(parse_questions(question_input(Vec::new())).is_err());
        assert!(parse_questions(question_input(
            (0..4).map(|id| question(&format!("q_{id}"))).collect()
        ))
        .is_err());
        let mut input = question("storage");
        input
            .as_object_mut()
            .unwrap()
            .insert("header", Json::String("ThirteenChars".into()));
        assert!(parse_questions(question_input(vec![input])).is_err());
        for count in [0, 1, 4] {
            let mut input = question("storage");
            input.as_object_mut().unwrap().insert(
                "options",
                Json::Array(
                    (0..count)
                        .map(|index| {
                            let mut option = RuntimeObject::new();
                            option.insert("label", Json::String(format!("Choice {index}").into()));
                            option.insert("description", Json::String("A distinct choice.".into()));
                            Json::Object(option)
                        })
                        .collect(),
                ),
            );
            assert!(parse_questions(question_input(vec![input])).is_err());
        }
    }

    #[test]
    fn questions_reject_blank_fields_and_duplicate_option_labels() {
        for field in ["id", "header", "question"] {
            let mut input = question("storage");
            input
                .as_object_mut()
                .unwrap()
                .insert(field, Json::String(" \t\n".into()));
            assert!(parse_questions(question_input(vec![input])).is_err());
        }
        let mut input = question("storage");
        input.as_object_mut().unwrap().insert(
            "options",
            exact_json(r#"[{"label":"Local","description":"First."},{"label":"Local","description":"Second."}]"#),
        );
        assert!(parse_questions(question_input(vec![input])).is_err());
    }

    #[test]
    fn question_metadata_keeps_secret_input_and_enables_client_other() {
        let mut input = question("credential");
        input
            .as_object_mut()
            .unwrap()
            .insert("isSecret", Json::Bool(true));
        input
            .as_object_mut()
            .unwrap()
            .insert("isOther", Json::Bool(false));
        let parsed = parse_questions(question_input(vec![input])).unwrap();
        let question = &parsed.get("questions").unwrap().as_array().unwrap()[0];
        assert_eq!(question.get("isSecret"), Some(&Json::Bool(true)));
        assert_eq!(question.get("isOther"), Some(&Json::Bool(true)));
        let defaults = parsed_questions(&["storage"]);
        assert_eq!(defaults[0].get("isSecret"), Some(&Json::Bool(false)));
    }

    #[test]
    fn answers_are_keyed_by_id_and_preserve_each_exact_array_entry() {
        let questions = parsed_questions(&["source_storage", "backup_storage"]);
        let mut answers = RuntimeMap::new();
        let values = vec![
            "Remote, with backups\nKeep this full answer".into(),
            RuntimeString::from_utf16(vec![0xd800, 0x61]),
        ];
        answers.insert("backup_storage".into(), values.clone());
        let result = question_result(&questions, &answers, "answered".into()).unwrap();
        assert_eq!(
            result
                .serializable
                .output
                .get("answers")
                .unwrap()
                .get("backup_storage")
                .unwrap()
                .get("answers"),
            Some(&Json::Array(values.into_iter().map(Json::String).collect()))
        );
        assert!(result
            .serializable
            .output
            .get("answers")
            .unwrap()
            .get("source_storage")
            .is_none());
        answers.insert("Where should data be stored?".into(), vec!["Local".into()]);
        assert!(question_result(&questions, &answers, "answered".into()).is_err());
    }

    #[test]
    fn question_resolution_distinguishes_skipped_cancelled_and_expired() {
        let questions = parsed_questions(&["storage"]);
        for status in ["skipped", "cancelled", "expired"] {
            let result = question_result(&questions, &RuntimeMap::new(), status.into()).unwrap();
            assert_eq!(
                result.serializable.output.get("status"),
                Some(&Json::String(status.into()))
            );
            assert_eq!(
                result.serializable.output.get("answers"),
                Some(&Json::Object(RuntimeObject::new()))
            );
        }
        assert!(question_result(&questions, &RuntimeMap::new(), "answered".into()).is_err());
        assert!(question_result(&questions, &RuntimeMap::new(), "stale".into()).is_err());
        let mut answers = RuntimeMap::new();
        answers.insert("storage".into(), vec!["Local".into()]);
        assert!(question_result(&questions, &answers, "expired".into()).is_err());
        answers.insert("storage".into(), vec!["   ".into()]);
        assert!(question_result(&questions, &answers, "answered".into()).is_err());
    }

    #[test]
    fn progress_preserves_order_explanation_and_completion() {
        let source = r#"{"explanation":"Implementation is ready for verification.","plan":[{"step":"Implement","status":"completed"},{"step":"Verify","status":"in_progress"},{"step":"Report","status":"pending"}]}"#;
        assert_eq!(parse_progress(source).unwrap(), exact_json(source));
        assert!(parse_progress(r#"{"plan":[]}"#).is_ok());
        assert!(parse_progress(r#"{"plan":[{"step":"Done","status":"completed"}]}"#).is_ok());
    }

    #[test]
    fn progress_rejects_competing_active_steps_and_invalid_statuses() {
        assert!(parse_progress(r#"{"plan":[{"step":"First","status":"in_progress"},{"step":"Second","status":"in_progress"}]}"#).is_err());
        assert!(parse_progress(r#"{"plan":[{"step":"First","status":"running"}]}"#).is_err());
        assert!(parse_progress(r#"{"plan":[{"step":" \n","status":"pending"}]}"#).is_err());
        assert!(parse_progress(r#"{"explanation":7,"plan":[]}"#).is_err());
    }
}
