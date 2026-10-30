//! The `ask_user` tool presents a validated question batch through the host's
//! elicitation handler and waits for actual user answers before continuing.

use std::sync::Arc;

use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use serde_json::json;

use crate::runtime_values::{runtime_codec, RuntimeJson, RuntimeMap, RuntimeObject, RuntimeString};
use crate::schemas::content::ContentBlock;
use crate::tools::build_tool::text_result;
use crate::types::errors::ToolError;
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{
    ElicitRequest, ElicitResponse, Tool, ToolContext, ToolProgress, ToolResult,
};

const INPUT_SCHEMA_PROPERTY_ORDER: &[&str] = &["questions", "answers", "metadata"];
const QUESTION_PROPERTY_ORDER: &[&str] = &["id", "question", "header", "options", "multiSelect"];
const OPTION_PROPERTY_ORDER: &[&str] = &["label", "description", "preview"];
const MAX_ATTACHMENT_COUNT: usize = 20;
const MAX_IMAGE_BYTES: usize = 10 * 1024 * 1024;
const MAX_TOTAL_IMAGE_BYTES: usize = 45 * 1024 * 1024;
const MAX_IMAGE_BASE64_UNITS: usize = MAX_IMAGE_BYTES.div_ceil(3) * 4;

/// Build the tool's input JSON Schema: a required `question`, plus optional
/// `options` (array of strings) and `multiSelect` (boolean).
fn legacy_input_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "question": {
                "type": "string",
                "description": "The question to ask the user"
            },
            "options": {
                "description": "Choices to present (omit for free text)",
                "type": "array",
                "items": { "type": "string" }
            },
            "multiSelect": {
                "description": "Allow selecting multiple options",
                "type": "boolean"
            }
        },
        "required": ["question"],
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .unwrap()
}

/// The model-facing contract uses one batch with two to four choices per question.
fn build_input_schema(require_ids: bool) -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "questions": {
                "type": "array", "minItems": 1, "maxItems": 4,
                "description": "Questions to ask together. Do not add an Other option; the client supplies free-text input.",
                "items": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string", "minLength": 1, "description": "Stable question identifier, unique within this batch. Keep it unchanged when rewording the same question."},
                        "question": {"type": "string", "minLength": 1, "description": "The complete question to ask."},
                        "header": {"type": "string", "minLength": 1, "maxLength": 12, "description": "Short question label, at most 12 characters."},
                        "options": {
                            "type": "array", "minItems": 2, "maxItems": 4,
                            "items": {
                                "type": "object",
                                "properties": {
                                    "label": {"type": "string", "minLength": 1},
                                    "description": {"type": "string"},
                                    "preview": {"type": "string", "description": "Optional plain-text or Markdown preview."}
                                },
                                "required": ["label", "description"], "additionalProperties": false
                            }
                        },
                        "multiSelect": {"type": "boolean", "default": false}
                    },
                    "required": if require_ids { vec!["id", "question", "header", "options", "multiSelect"] } else { vec!["question", "header", "options", "multiSelect"] }, "additionalProperties": false
                }
            },
            "answers": {
                "type": "object", "additionalProperties": {"type": "string"},
                "description": "Reserved for answers keyed by question ID and supplied by the user interface. Never fill this field yourself."
            },
            "metadata": {"type": "object", "additionalProperties": true}
        },
        "required": ["questions"], "additionalProperties": false
    }).as_object().cloned().unwrap()
}

fn structured_questions(input: &Json) -> Option<Vec<RuntimeObject>> {
    input
        .get("questions")?
        .as_array()?
        .iter()
        .map(|question| question.as_object().cloned())
        .collect()
}

fn structured_output(
    questions: &[RuntimeObject],
    answers: &RuntimeMap<RuntimeString>,
) -> RuntimeJson {
    let mut output = RuntimeObject::new();
    output.insert(
        "questions",
        RuntimeJson::Array(questions.iter().cloned().map(RuntimeJson::Object).collect()),
    );
    output.insert(
        "answers",
        runtime_codec::encode_value(answers).expect("string answer map is encodable"),
    );
    RuntimeJson::Object(output)
}

pub(crate) fn runtime_is_blank(value: &RuntimeString) -> bool {
    value
        .as_utf16()
        .iter()
        .all(|unit| char::from_u32(u32::from(*unit)).is_some_and(char::is_whitespace))
}

/// Historical batches without IDs retain their original answer keys.
pub(crate) fn question_answer_key(question: &RuntimeObject) -> Option<&RuntimeString> {
    question
        .get("id")
        .or_else(|| question.get("question"))
        .and_then(Json::as_str)
}

/// Older clients can submit an unambiguous prompt key; persist only canonical IDs.
pub(crate) fn canonical_question_map<V>(
    questions: &[RuntimeObject],
    mut values: RuntimeMap<V>,
) -> Result<RuntimeMap<V>, ToolError> {
    let keys = values.entries().map(|(key, _)| key).collect::<Vec<_>>();
    let mut canonical = RuntimeMap::new();
    for key in keys {
        let mut matches = questions.iter().filter(|question| {
            question_answer_key(question) == Some(&key)
                || question.get("question").and_then(Json::as_str) == Some(&key)
        });
        let question = matches
            .next()
            .filter(|_| matches.next().is_none())
            .ok_or_else(|| {
                ToolError::new("The response contains an unknown or ambiguous question")
            })?;
        let target = question_answer_key(question)
            .expect("validated question")
            .clone();
        if canonical.get(&target).is_some() {
            return Err(ToolError::new(
                "The response contains duplicate question answers",
            ));
        }
        canonical.insert(
            target,
            values.remove(&key).expect("existing question entry"),
        );
    }
    Ok(canonical)
}

pub(crate) fn validated_attachments(
    questions: &[RuntimeObject],
    answers: &RuntimeMap<RuntimeString>,
    attachments: Option<Box<RuntimeMap<Vec<ContentBlock>>>>,
) -> Result<Option<Vec<ContentBlock>>, ToolError> {
    let Some(mut attachments) = attachments else {
        return Ok(None);
    };
    let mut count = 0;
    let mut total_bytes = 0;
    for (question, blocks) in attachments.entries() {
        if answers.get(&question).is_none()
            || !questions
                .iter()
                .any(|item| question_answer_key(item) == Some(&question))
        {
            return Err(ToolError::new(
                "Attachments must belong to an answered, known question",
            ));
        }
        if blocks.len() > MAX_ATTACHMENT_COUNT - count {
            return Err(ToolError::new(
                "At most 20 question attachments are allowed",
            ));
        }
        count += blocks.len();
        for block in blocks {
            let ContentBlock::Image(image) = block else {
                return Err(ToolError::new("Question attachments must be images"));
            };
            if !["image/jpeg", "image/png", "image/gif", "image/webp"]
                .iter()
                .any(|mime| image.mime_type == *mime)
            {
                return Err(ToolError::new(
                    "Question images must use JPEG, PNG, GIF, or WebP MIME types",
                ));
            }
            // Bound the encoded payload before allocating its UTF-8 and decoded copies.
            if image.base64.len_utf16() > MAX_IMAGE_BASE64_UNITS {
                return Err(ToolError::new("Question images must not exceed 10 MiB"));
            }
            let encoded = image
                .base64
                .try_to_utf8()
                .map_err(|_| ToolError::new("Question images must contain valid base64"))?;
            let decoded = BASE64_STANDARD
                .decode(encoded.as_bytes())
                .map_err(|_| ToolError::new("Question images must contain valid base64"))?;
            if decoded.len() > MAX_IMAGE_BYTES {
                return Err(ToolError::new("Question images must not exceed 10 MiB"));
            }
            total_bytes += decoded.len();
            if total_bytes > MAX_TOTAL_IMAGE_BYTES {
                return Err(ToolError::new(
                    "Question images must not exceed 45 MiB in total",
                ));
            }
        }
    }
    let mut blocks = Vec::with_capacity(count);
    for question in questions {
        let key = question_answer_key(question).expect("validated question");
        if let Some(images) = attachments.remove(key) {
            blocks.extend(images);
        }
    }
    Ok((!blocks.is_empty()).then_some(blocks))
}

pub(crate) fn validate_questions(questions: &[RuntimeObject]) -> Result<(), ToolError> {
    let mut seen_keys = std::collections::HashSet::new();
    for question in questions {
        let text = question
            .get("question")
            .and_then(Json::as_str)
            .ok_or_else(|| ToolError::new("Question text is required"))?;
        if runtime_is_blank(text) {
            return Err(ToolError::new("Question texts must be nonempty"));
        }
        let key = question_answer_key(question)
            .ok_or_else(|| ToolError::new("Question IDs must be strings"))?;
        if runtime_is_blank(key) || !seen_keys.insert(key.clone()) {
            return Err(ToolError::new(
                "Question answer keys must be nonempty and unique",
            ));
        }
        if questions.iter().any(|other| {
            question_answer_key(other) != Some(key)
                && other.get("question").and_then(Json::as_str) == Some(key)
        }) {
            return Err(ToolError::new(
                "Question IDs must not match another question's text",
            ));
        }
        let header = question
            .get("header")
            .and_then(Json::as_str)
            .ok_or_else(|| ToolError::new("Question header is required"))?;
        if runtime_is_blank(header) {
            return Err(ToolError::new("Question headers must not be blank"));
        }
        let options = question
            .get("options")
            .and_then(Json::as_array)
            .ok_or_else(|| ToolError::new("Question options are required"))?;
        let mut seen_labels = std::collections::HashSet::new();
        for option in options {
            let label = option
                .get("label")
                .and_then(Json::as_str)
                .ok_or_else(|| ToolError::new("Option labels are required"))?;
            if runtime_is_blank(label) || !seen_labels.insert(label.clone()) {
                return Err(ToolError::new(
                    "Option labels must be nonempty and unique within each question",
                ));
            }
        }
    }
    Ok(())
}

/// A tool that asks the user a structured question through the host's
/// elicitation handler.
pub struct AskUserTool {
    input_schema: JsonSchema,
}

impl AskUserTool {
    fn new() -> Self {
        AskUserTool {
            input_schema: build_input_schema(true),
        }
    }
}

fn answer_output(answered: bool, values: Vec<RuntimeString>) -> RuntimeJson {
    let mut output = RuntimeObject::new();
    output.insert("answered", RuntimeJson::Bool(answered));
    output.insert(
        "values",
        RuntimeJson::Array(values.into_iter().map(RuntimeJson::String).collect()),
    );
    RuntimeJson::Object(output)
}

fn join_runtime_strings(values: &[RuntimeString], separator: &str) -> RuntimeString {
    let mut output = RuntimeString::default();
    for (index, value) in values.iter().enumerate() {
        if index != 0 {
            output = output.concat(&RuntimeString::from(separator));
        }
        output = output.concat(value);
    }
    output
}

/// Construct the `ask_user` tool as an erased registry-side [`Tool`].
pub fn create_ask_user_tool() -> Arc<dyn Tool> {
    Arc::new(AskUserTool::new())
}

#[async_trait]
impl Tool for AskUserTool {
    fn static_name(&self) -> &str {
        "ask_user"
    }

    fn static_description(&self) -> &str {
        "Ask the user one to four clarification questions or required business decisions and wait for their answers before continuing. Each question offers two to four choices; users can also write a custom answer. Use multiSelect for independent choices. Never use this tool for runtime permission requests or permission-related escalations; native permission approval remains separate and an answer is not a runtime permission grant."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn input_schema_property_order(
        &self,
        object_path: &[String],
    ) -> Option<&'static [&'static str]> {
        match object_path
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .as_slice()
        {
            [] => Some(INPUT_SCHEMA_PROPERTY_ORDER),
            ["questions", _] => Some(QUESTION_PROPERTY_ORDER),
            ["questions", _, "options", _] => Some(OPTION_PROPERTY_ORDER),
            _ => None,
        }
    }

    fn parse_input(&self, input: &Json) -> Result<Json, ToolError> {
        // Historical transcripts can still replay the former single-question input.
        // It is intentionally absent from the model-facing schema.
        if input.get("questions").is_none() && input.get("question").is_some() {
            let schema = crate::types::json::runtime_schema_from_static(&legacy_input_schema());
            return crate::tools::input_parser::parse_native_input(&schema, input, &|path| {
                path.is_empty()
                    .then_some(&["question", "options", "multiSelect"][..])
            });
        }
        let legacy_batch =
            input
                .get("questions")
                .and_then(Json::as_array)
                .is_some_and(|questions| {
                    questions
                        .iter()
                        .all(|question| question.get("id").is_none())
                });
        let schema = if legacy_batch {
            crate::types::json::runtime_schema_from_static(&build_input_schema(false))
        } else {
            self.input_schema()
        };
        let parsed = crate::tools::input_parser::parse_native_input(&schema, input, &|path| {
            self.input_schema_property_order(path)
        })?;
        let questions = structured_questions(&parsed)
            .ok_or_else(|| ToolError::new("Structured questions are required"))?;
        validate_questions(&questions)?;
        Ok(parsed)
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        false
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        true
    }

    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        Some(false)
    }

    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        let question = input
            .get("question")
            .and_then(Json::as_str)
            .or_else(|| {
                input
                    .get("questions")?
                    .as_array()?
                    .first()?
                    .get("question")?
                    .as_str()
            })
            .cloned()
            .unwrap_or_default();
        Some(RuntimeString::from("Asking: ").concat(&question.slice_utf16(..60)))
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        if input.get("questions").is_some() {
            let parsed = self.parse_input(&input)?;
            let questions = structured_questions(&parsed).expect("validated questions");
            let empty = RuntimeMap::new();
            let Some(elicit) = &ctx.elicit else {
                return Ok(text_result(
                    structured_output(&questions, &empty),
                    "No elicitation handler is available; cannot ask the user.",
                    true,
                ));
            };
            if ctx.signal.is_cancelled() {
                return Ok(text_result(
                    structured_output(&questions, &empty),
                    "User declined to answer.",
                    false,
                ));
            }
            // Only the host response is authoritative. Model-supplied answers are ignored.
            let response = elicit
                .elicit(
                    ElicitRequest {
                        prompt: questions[0]
                            .get("question")
                            .and_then(Json::as_str)
                            .cloned()
                            .expect("validated question"),
                        questions: Some(questions.clone()),
                        attachments_supported: Some(true),
                        metadata: parsed.get("metadata").and_then(Json::as_object).cloned(),
                        options: None,
                        multi_select: None,
                    },
                    ctx.signal.clone(),
                )
                .await;
            if ctx.signal.is_cancelled() {
                return Ok(text_result(
                    structured_output(&questions, &empty),
                    "User declined to answer.",
                    false,
                ));
            }
            return match response {
                ElicitResponse::Answers {
                    answers,
                    attachments,
                } => {
                    let answers = canonical_question_map(&questions, answers)?;
                    let attachments = attachments.map(|values| canonical_question_map(&questions, *values).map(Box::new)).transpose()?;
                    for (question, answer) in answers.entries() {
                        if !questions.iter().any(|item| {
                            question_answer_key(item) == Some(&question)
                        }) {
                            return Err(ToolError::new(
                                "The user response contains an unknown question",
                            ));
                        }
                        if runtime_is_blank(answer) {
                            return Err(ToolError::new(
                                "The user response contains an empty answer",
                            ));
                        }
                    }
                    let extra_user_blocks =
                        validated_attachments(&questions, &answers, attachments)?;
                    let output = structured_output(&questions, &answers);
                    let text = crate::runtime_values::runtime_json::stringify(&output);
                    let mut result = text_result(output, text, false);
                    result.serializable.extra_user_blocks = extra_user_blocks;
                    Ok(result)
                }
                ElicitResponse::Decline { reason } => {
                    let mut text = RuntimeString::from("User declined to answer");
                    if let Some(reason) = reason.filter(|reason| !reason.is_empty()) {
                        text = text.concat(&RuntimeString::from(": ")).concat(&reason);
                    }
                    Ok(text_result(
                        structured_output(&questions, &empty),
                        text.concat(&RuntimeString::from(".")),
                        false,
                    ))
                }
                ElicitResponse::Accept { .. } | ElicitResponse::UserInput { .. } => Ok(text_result(
                    structured_output(&questions, &empty),
                    "The client did not return structured answers; no user answers were recorded.",
                    true,
                )),
            };
        }

        let elicit = match &ctx.elicit {
            Some(e) => e,
            None => {
                return Ok(text_result(
                    answer_output(false, Vec::new()),
                    "No elicitation handler is available; cannot ask the user.",
                    true,
                ));
            }
        };

        let question = input
            .get("question")
            .and_then(Json::as_str)
            .cloned()
            .unwrap_or_default();
        let options = input.get("options").and_then(Json::as_array).map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().cloned())
                .collect::<Vec<RuntimeString>>()
        });
        let multi_select = input.get("multiSelect").and_then(Json::as_bool);

        let req = ElicitRequest {
            questions: None,
            attachments_supported: None,
            metadata: None,
            prompt: question,
            options,
            multi_select,
        };
        let resp = elicit.elicit(req, ctx.signal.clone()).await;

        match resp {
            ElicitResponse::Answers { .. } | ElicitResponse::UserInput { .. } => Ok(text_result(
                answer_output(false, Vec::new()),
                "The client returned structured answers for a legacy question.",
                true,
            )),
            ElicitResponse::Decline { reason } => {
                let mut text = RuntimeString::from("User declined to answer");
                if let Some(reason) = reason.filter(|reason| !reason.is_empty()) {
                    text = text.concat(&RuntimeString::from(": ")).concat(&reason);
                }
                text = text.concat(&RuntimeString::from("."));
                Ok(text_result(answer_output(false, Vec::new()), text, false))
            }
            ElicitResponse::Accept { values } => {
                let joined = join_runtime_strings(&values, "\n");
                let text = if joined.is_empty() {
                    RuntimeString::from("(no answer)")
                } else {
                    joined
                };
                Ok(text_result(answer_output(true, values), text, false))
            }
        }
    }
}

/// Optional clarification returns once presented, without holding the model turn.
pub struct AskUserAsyncTool {
    blocking: AskUserTool,
}

pub fn create_ask_user_async_tool() -> Arc<dyn Tool> {
    Arc::new(AskUserAsyncTool {
        blocking: AskUserTool::new(),
    })
}

#[async_trait]
impl Tool for AskUserAsyncTool {
    fn static_name(&self) -> &str {
        "ask_user_async"
    }
    fn static_description(&self) -> &str {
        "Ask one to four optional clarification questions without waiting. Prefer this outside plan mode when useful work can continue. The client may defer the question and answer later during this same turn. The answer arrives as a genuine user message. Continue useful work after this tool returns; do not invent an answer. Use ask_user only when progress requires a business decision from the user. Never use either question tool for runtime permission requests or permission-related escalations."
    }
    fn static_input_schema(&self) -> &JsonSchema {
        self.blocking.static_input_schema()
    }
    fn input_schema_property_order(
        &self,
        object_path: &[String],
    ) -> Option<&'static [&'static str]> {
        self.blocking.input_schema_property_order(object_path)
    }
    fn parse_input(&self, input: &Json) -> Result<Json, ToolError> {
        if input.get("questions").is_none() {
            return Err(ToolError::new(
                "Optional questions require a structured question batch",
            ));
        }
        self.blocking.parse_input(input)
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        false
    }
    fn is_read_only(&self, _input: &Json) -> bool {
        true
    }
    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        Some(false)
    }
    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        self.blocking.summarize(input)
    }
    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let parsed = self.parse_input(&input)?;
        let questions = structured_questions(&parsed).expect("validated questions");
        let Some(context) = &ctx.async_question else {
            return Err(ToolError::new(
                "Optional questions are unavailable in this context; no question was submitted",
            ));
        };
        if ctx.signal.is_cancelled() {
            return Err(ToolError::new(
                "The originating question turn was cancelled",
            ));
        }
        let record = context
            .ask(
                ElicitRequest {
                    prompt: questions[0]
                        .get("question")
                        .and_then(Json::as_str)
                        .cloned()
                        .expect("validated question"),
                    questions: Some(questions),
                    attachments_supported: Some(true),
                    metadata: parsed.get("metadata").and_then(Json::as_object).cloned(),
                    options: None,
                    multi_select: None,
                },
                uuid::Uuid::new_v4().to_string().into(),
            )
            .await
            .map_err(ToolError::new)?;
        if let Some(on_progress) = on_progress {
            on_progress(crate::types::events::ToolDomainEvent::Status {
                message: if record.state == "answered" {
                    "Optional questions have been answered."
                } else {
                    "Optional questions are available for later answers during this turn."
                }
                .into(),
            })
            .await;
        }
        let output =
            runtime_codec::encode_value(&record).expect("question record has an exact codec");
        let rendered = if record.answers.is_empty() {
            "The optional questions were accepted. Continue useful work. The user may answer later during this turn; no answer has been supplied yet."
        } else {
            "The optional questions were accepted. Submitted answers are delivered as user messages. Continue useful work; any remaining questions can be answered later during this turn."
        };
        Ok(text_result(output, rendered, false))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_process::RuntimeEnv;
    use crate::runtime_values::{runtime_json, RuntimePath};
    use crate::schemas::content::{ImageBlock, OutputBlock};
    use crate::types::abort::AbortSignal;
    use crate::types::file_state::{FileStateCache, FileStateEntry, RecordOptions};
    use crate::types::permission::PermissionContext;
    use crate::types::sandbox::{
        ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec,
    };
    use crate::types::tool::ElicitHandler;
    use std::sync::Mutex;

    struct NoopFileState;
    impl FileStateCache for NoopFileState {
        fn get(&self, _path: &RuntimePath) -> Option<FileStateEntry> {
            None
        }
        fn record(&self, _path: &RuntimePath, _mtime_ms: f64, _opts: RecordOptions) {}
        fn has(&self, _path: &RuntimePath) -> bool {
            false
        }
        fn entries(&self) -> Vec<(RuntimePath, FileStateEntry)> {
            Vec::new()
        }
        fn delete(&self, _path: &RuntimePath) {}
        fn clear(&self) {}
        fn clone_box(&self) -> Box<dyn FileStateCache> {
            Box::new(NoopFileState)
        }
    }

    struct NoopSandbox;
    impl SandboxBackend for NoopSandbox {
        fn kind(&self) -> SandboxKind {
            SandboxKind::Process
        }
        fn is_available(&self) -> bool {
            false
        }
        fn transform(&self, req: &ExecRequest, _policy: &SandboxPolicy) -> SpawnSpec {
            SpawnSpec {
                file: req.argv.first().cloned().unwrap_or_default(),
                args: req.argv.iter().skip(1).cloned().collect(),
                cwd: req.cwd.clone(),
                env: RuntimeEnv::new(),
            }
        }
    }

    fn mk_ctx() -> ToolContext {
        ToolContext {
            async_question: None,
            background_processes: Arc::new(
                crate::command_runtime::background::BackgroundCommandRegistry::new(),
            ),
            process_context: crate::runtime_process::test_runtime_process_context(),
            cwd: RuntimePath::from("."),
            signal: AbortSignal::new(),
            file_state: Arc::new(NoopFileState),
            permission: PermissionContext {
                allow_rules: Vec::new(),
                deny_rules: Vec::new(),
                ask_rules: Vec::new(),
                additional_writable_roots: Vec::new(),
                internal_writable_roots: Vec::new(),
                pinned_memory_root: None,
                should_avoid_prompts: false,
            },
            sandbox: Arc::new(NoopSandbox),
            planning_read_only: false,
            approval_policy: None,
            sandbox_mode: None,
            approvals_reviewer: None,
            web_search_access: None,
            network_allowed: None,
            sandbox_retry_on_denial: None,
            request_approval: None,
            request_network_access: None,
            enqueue_background_notice: None,
            record_rollout_event: None,
            emit: std::sync::Arc::new(|_| Box::pin(async {})),
            native_pdf: None,
            vision: None,
            shell_env: None,
            depth: None,
            forward_subagent_text: None,
            spawn_sub_agent: None,
            spawn_background_agent: None,
            spawn_workflow_agent: None,
            surface_tools: None,
            activate_skill_paths: None,
            tool_catalog: None,
            elicit: None,
            plan_file: None,
            apply_skill_context: None,
            run_bash: None,
            mcp_clients: None,
            background_tasks: None,
            task_list_id: None,
            task_plan: None,
            worktree: None,
            workflow_journal: None,
            swarm: None,
            cron: None,
            goal: None,
            goal_generation: None,
            goal_owned_resources: None,
            lsp: None,
            browser: None,
            browser_tool_mapping: None,
            run_hook: None,
            invoke_tool: None,
            advisor: None,
        }
    }

    /// An elicitation handler that returns a scripted response and records the
    /// last request it saw.
    struct ScriptedElicit {
        response: ElicitResponse,
        seen: Arc<Mutex<Option<ElicitRequest>>>,
    }

    #[async_trait]
    impl ElicitHandler for ScriptedElicit {
        async fn elicit(&self, req: ElicitRequest, _signal: AbortSignal) -> ElicitResponse {
            *self.seen.lock().unwrap() = Some(req);
            self.response.clone()
        }
    }

    fn text_of(res: &ToolResult) -> RuntimeString {
        match &res.serializable.model_content[0] {
            OutputBlock::Text(t) => t.text.clone(),
            _ => panic!("expected a text block"),
        }
    }

    fn exact_json(source: &str) -> RuntimeJson {
        runtime_json::parse(&RuntimeString::from(source)).unwrap()
    }

    #[test]
    fn exposes_expected_metadata() {
        let tool = create_ask_user_tool();
        assert_eq!(tool.name(), RuntimeString::from("ask_user"));
        assert_eq!(
            tool.description(),
            RuntimeString::from(
                "Ask the user one to four clarification questions or required business decisions and wait for their answers before continuing. Each question offers two to four choices; users can also write a custom answer. Use multiSelect for independent choices. Never use this tool for runtime permission requests or permission-related escalations; native permission approval remains separate and an answer is not a runtime permission grant."
            )
        );
        let input = exact_json(r#"{"question":"Ready?"}"#);
        assert!(tool.is_read_only(&input));
        assert!(!tool.is_concurrency_safe(&input));
        assert_eq!(tool.is_destructive(&input), Some(false));
        assert_eq!(
            tool.summarize(&input),
            Some(RuntimeString::from("Asking: Ready?"))
        );
        assert_eq!(
            tool.input_schema().get("required").unwrap(),
            &RuntimeJson::Array(vec![RuntimeJson::from("questions")])
        );
    }

    #[tokio::test]
    async fn call_errors_without_an_elicitation_handler() {
        let tool = create_ask_user_tool();
        let ctx = mk_ctx();
        let res = tool
            .call(exact_json(r#"{"question":"Ready?"}"#), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            res.serializable.output,
            exact_json(r#"{"answered":false,"values":[]}"#)
        );
        assert_eq!(
            text_of(&res),
            RuntimeString::from("No elicitation handler is available; cannot ask the user.")
        );
    }

    #[tokio::test]
    async fn call_returns_accepted_values_and_forwards_options() {
        let tool = create_ask_user_tool();
        let mut ctx = mk_ctx();
        let seen = Arc::new(Mutex::new(None));
        ctx.elicit = Some(Arc::new(ScriptedElicit {
            response: ElicitResponse::Accept {
                values: vec!["a".into(), "b".into()],
            },
            seen: seen.clone(),
        }));
        let res = tool
            .call(
                exact_json(r#"{"question":"Pick","options":["a","b"],"multiSelect":true}"#),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            res.serializable.output,
            exact_json(r#"{"answered":true,"values":["a","b"]}"#)
        );
        assert_eq!(text_of(&res), RuntimeString::from("a\nb"));

        let req = seen.lock().unwrap().clone().unwrap();
        assert_eq!(req.prompt, RuntimeString::from("Pick"));
        assert_eq!(req.attachments_supported, None);
        assert_eq!(req.options, Some(vec!["a".into(), "b".into()]));
        assert_eq!(req.multi_select, Some(true));
    }

    #[tokio::test]
    async fn call_omits_options_and_multiselect_when_absent() {
        let tool = create_ask_user_tool();
        let mut ctx = mk_ctx();
        let seen = Arc::new(Mutex::new(None));
        ctx.elicit = Some(Arc::new(ScriptedElicit {
            response: ElicitResponse::Accept { values: vec![] },
            seen: seen.clone(),
        }));
        let res = tool
            .call(exact_json(r#"{"question":"Free text?"}"#), &ctx, None)
            .await
            .unwrap();
        // Empty answer → placeholder text.
        assert_eq!(text_of(&res), RuntimeString::from("(no answer)"));
        let req = seen.lock().unwrap().clone().unwrap();
        assert_eq!(req.options, None);
        assert_eq!(req.multi_select, None);
    }

    #[tokio::test]
    async fn call_reports_a_decline_with_reason() {
        let tool = create_ask_user_tool();
        let mut ctx = mk_ctx();
        let seen = Arc::new(Mutex::new(None));
        ctx.elicit = Some(Arc::new(ScriptedElicit {
            response: ElicitResponse::Decline {
                reason: Some("busy".into()),
            },
            seen,
        }));
        let res = tool
            .call(exact_json(r#"{"question":"Ready?"}"#), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            res.serializable.output,
            exact_json(r#"{"answered":false,"values":[]}"#)
        );
        assert_eq!(
            text_of(&res),
            RuntimeString::from("User declined to answer: busy.")
        );
    }

    #[tokio::test]
    async fn call_reports_a_decline_without_reason() {
        let tool = create_ask_user_tool();
        let mut ctx = mk_ctx();
        let seen = Arc::new(Mutex::new(None));
        ctx.elicit = Some(Arc::new(ScriptedElicit {
            response: ElicitResponse::Decline { reason: None },
            seen,
        }));
        let res = tool
            .call(exact_json(r#"{"question":"Ready?"}"#), &ctx, None)
            .await
            .unwrap();
        assert_eq!(
            text_of(&res),
            RuntimeString::from("User declined to answer.")
        );
    }
    fn structured_input() -> Json {
        exact_json(
            r#"{"questions":[{"question":"Pick a framework?","header":"Framework","options":[{"label":"A","description":"First"},{"label":"B","description":"Second"}],"multiSelect":false},{"question":"Choose features?","header":"Features","options":[{"label":"A, B","description":"Both"},{"label":"C","description":"Third"}],"multiSelect":true}],"answers":{"Pick a framework?":"MODEL GUESS"},"metadata":{"source":"test"}}"#,
        )
    }

    fn image(mime: &str, base64: impl Into<RuntimeString>) -> ContentBlock {
        ContentBlock::Image(ImageBlock {
            mime_type: mime.into(),
            base64: base64.into(),
            detail: None,
            source_path: None,
        })
    }

    async fn call_with_attachments(
        key: &str,
        blocks: Vec<ContentBlock>,
    ) -> Result<ToolResult, ToolError> {
        let mut answers = RuntimeMap::new();
        answers.insert("Pick a framework?".into(), "  custom answer  ".into());
        let mut attachments = RuntimeMap::new();
        attachments.insert(key.into(), blocks);
        let mut ctx = mk_ctx();
        ctx.elicit = Some(Arc::new(ScriptedElicit {
            response: ElicitResponse::Answers {
                answers,
                attachments: Some(Box::new(attachments)),
            },
            seen: Arc::new(Mutex::new(None)),
        }));
        create_ask_user_tool()
            .call(structured_input(), &ctx, None)
            .await
    }

    #[tokio::test]
    async fn attachment_keys_must_belong_to_answered_known_questions() {
        for (key, expected_error) in [
            (
                "Unknown question?",
                "The response contains an unknown or ambiguous question",
            ),
            (
                "Choose features?",
                "Attachments must belong to an answered, known question",
            ),
        ] {
            let error = call_with_attachments(key, vec![image("image/png", "AQID")])
                .await
                .err()
                .expect("attachments for unknown or unanswered questions must fail");
            assert_eq!(error.message, RuntimeString::from(expected_error), "{key}");
        }
    }

    #[tokio::test]
    async fn question_attachments_reject_every_non_image_block_kind() {
        for source in [
            r#"{"type":"text","text":"not an image"}"#,
            r#"{"type":"document","mimeType":"application/pdf","base64":"AQID"}"#,
            r#"{"type":"thinking","text":"not an image"}"#,
            r#"{"type":"redacted_thinking","data":"opaque"}"#,
            r#"{"type":"tool_use","id":"call-1","name":"bash","input":{}}"#,
            r#"{"type":"tool_result","toolUseId":"call-1","content":[]}"#,
            r#"{"type":"web_search_call","id":"search-1","query":"not an image","results":[]}"#,
        ] {
            let block = runtime_codec::decode_value(&exact_json(source)).unwrap();
            let error = call_with_attachments("Pick a framework?", vec![block])
                .await
                .err()
                .expect("non-image blocks must fail");
            assert_eq!(
                error.message,
                RuntimeString::from("Question attachments must be images")
            );
        }
    }

    #[tokio::test]
    async fn question_images_reject_unsupported_mime_and_invalid_base64() {
        for mime in ["image/svg+xml", "application/pdf", "image/jpg", "IMAGE/PNG"] {
            let error = call_with_attachments("Pick a framework?", vec![image(mime, "AQID")])
                .await
                .err()
                .expect("unsupported MIME type must fail");
            assert_eq!(
                error.message,
                RuntimeString::from("Question images must use JPEG, PNG, GIF, or WebP MIME types")
            );
        }
        for encoded in [
            RuntimeString::from("%%%"),
            RuntimeString::from("AQ"),
            RuntimeString::from("a==="),
            RuntimeString::from("AQID\n"),
            RuntimeString::from_utf16(vec![0xd800]),
        ] {
            let error =
                call_with_attachments("Pick a framework?", vec![image("image/png", encoded)])
                    .await
                    .err()
                    .expect("invalid base64 must fail");
            assert_eq!(
                error.message,
                RuntimeString::from("Question images must contain valid base64")
            );
        }
    }

    #[tokio::test]
    async fn question_images_enforce_count_and_single_image_byte_limits() {
        let valid = image("image/png", "AQID");
        let accepted = call_with_attachments("Pick a framework?", vec![valid.clone(); 20])
            .await
            .unwrap();
        assert_eq!(accepted.serializable.extra_user_blocks.unwrap().len(), 20);
        let error = call_with_attachments("Pick a framework?", vec![valid; 21])
            .await
            .err()
            .expect("twenty-one images must fail");
        assert_eq!(
            error.message,
            RuntimeString::from("At most 20 question attachments are allowed")
        );
        let questions = structured_questions(&structured_input()).unwrap();
        let mut answers = RuntimeMap::new();
        answers.insert("Pick a framework?".into(), "A".into());
        answers.insert("Choose features?".into(), "C".into());
        let mut attachments = RuntimeMap::new();
        attachments.insert(
            "Pick a framework?".into(),
            vec![image("image/png", "AQID"); 10],
        );
        attachments.insert(
            "Choose features?".into(),
            vec![image("image/webp", "AQID"); 11],
        );
        assert_eq!(
            validated_attachments(&questions, &answers, Some(Box::new(attachments)))
                .unwrap_err()
                .message,
            RuntimeString::from("At most 20 question attachments are allowed")
        );
        for encoded in [
            BASE64_STANDARD.encode(vec![0; MAX_IMAGE_BYTES + 1]),
            "A".repeat(MAX_IMAGE_BASE64_UNITS + 4),
        ] {
            let error =
                call_with_attachments("Pick a framework?", vec![image("image/png", encoded)])
                    .await
                    .err()
                    .expect("oversized images must fail");
            assert_eq!(
                error.message,
                RuntimeString::from("Question images must not exceed 10 MiB")
            );
        }
    }

    #[tokio::test]
    async fn question_images_enforce_total_decoded_byte_limit() {
        let full = image(
            "image/jpeg",
            BASE64_STANDARD.encode(vec![0; MAX_IMAGE_BYTES]),
        );
        let remainder = MAX_TOTAL_IMAGE_BYTES - 4 * MAX_IMAGE_BYTES;
        let mut blocks = vec![full; 4];
        blocks.push(image(
            "image/gif",
            BASE64_STANDARD.encode(vec![0; remainder]),
        ));
        let accepted = call_with_attachments("Pick a framework?", blocks.clone())
            .await
            .unwrap();
        assert_eq!(accepted.serializable.extra_user_blocks.unwrap().len(), 5);
        blocks[4] = image("image/gif", BASE64_STANDARD.encode(vec![0; remainder + 1]));
        let questions = structured_questions(&structured_input()).unwrap();
        let mut answers = RuntimeMap::new();
        answers.insert("Pick a framework?".into(), "A".into());
        answers.insert("Choose features?".into(), "C".into());
        let mut attachments = RuntimeMap::new();
        let last = blocks.pop().unwrap();
        attachments.insert("Pick a framework?".into(), blocks);
        attachments.insert("Choose features?".into(), vec![last]);
        let error = validated_attachments(&questions, &answers, Some(Box::new(attachments)))
            .expect_err("more than 45 MiB across questions must fail");
        assert_eq!(
            error.message,
            RuntimeString::from("Question images must not exceed 45 MiB in total")
        );
    }

    #[tokio::test]
    async fn structured_answers_come_only_from_host_and_preserve_exact_text() {
        let tool = create_ask_user_tool();
        let mut ctx = mk_ctx();
        let seen = Arc::new(Mutex::new(None));
        let mut answers = RuntimeMap::new();
        answers.insert("Pick a framework?".into(), "  custom answer  ".into());
        answers.insert("Choose features?".into(), "A, B, C".into());
        ctx.elicit = Some(Arc::new(ScriptedElicit {
            response: ElicitResponse::Answers {
                answers: answers.clone(),
                attachments: None,
            },
            seen: seen.clone(),
        }));
        let result = tool.call(structured_input(), &ctx, None).await.unwrap();
        let request = seen.lock().unwrap().clone().unwrap();
        assert_eq!(request.questions.as_ref().unwrap().len(), 2);
        assert_eq!(request.attachments_supported, Some(true));
        assert_eq!(
            request.metadata.as_ref().unwrap().get("source"),
            Some(&RuntimeJson::from("test"))
        );
        assert_eq!(
            result.serializable.output.get("answers"),
            Some(&runtime_codec::encode_value(&answers).unwrap())
        );
        assert_eq!(
            runtime_json::parse(&text_of(&result)).unwrap(),
            result.serializable.output
        );
    }

    #[tokio::test]
    async fn structured_questions_report_missing_handler_and_decline_without_fake_answers() {
        let tool = create_ask_user_tool();
        let mut ctx = mk_ctx();
        let missing = tool.call(structured_input(), &ctx, None).await.unwrap();
        assert_eq!(missing.serializable.is_error, Some(true));
        assert!(missing
            .serializable
            .output
            .get("answers")
            .unwrap()
            .as_object()
            .unwrap()
            .is_empty());
        ctx.elicit = Some(Arc::new(ScriptedElicit {
            response: ElicitResponse::Decline {
                reason: Some("Later".into()),
            },
            seen: Arc::new(Mutex::new(None)),
        }));
        let declined = tool.call(structured_input(), &ctx, None).await.unwrap();
        assert_eq!(declined.serializable.is_error, Some(false));
        assert!(declined
            .serializable
            .output
            .get("answers")
            .unwrap()
            .as_object()
            .unwrap()
            .is_empty());
        assert_eq!(
            text_of(&declined),
            RuntimeString::from("User declined to answer: Later.")
        );
    }
}
