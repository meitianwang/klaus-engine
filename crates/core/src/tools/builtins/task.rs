//! The `task` [`Tool`]: spawn an in-process sub-agent to handle a self-contained
//! subtask.
//!
//! The child's event stream is forwarded as progress; its final message becomes
//! the result. When agent definitions are supplied, the tool advertises the
//! available roles in its description and the `agentType` enum so the model can
//! pick one — the engine then restricts that child's tools/model/system prompt to
//! the role.

use std::{num::NonZeroUsize, sync::Arc};

use async_trait::async_trait;

use crate::runtime_values::{
    runtime_json, RuntimeDecode, RuntimeJson, RuntimeObject, RuntimeString,
};
use crate::schemas::config::SubagentModelOption;
use crate::schemas::effort::EffortValue;
use crate::schemas::subagent_message::{message_preview, validate_text_message};
use crate::tools::build_tool::text_result;
use crate::tools::subagent_stream::{consume_foreground_sub_agent, ForegroundSubAgentResult};
use crate::types::errors::ToolError;
use crate::types::json::Json;
use crate::types::permission::{PermissionReason, PermissionResult};
use crate::types::tool::{
    ForkContext, Isolation, SpawnSubAgentOptions, SwarmSpawn, Tool, ToolContext, ToolProgress,
    ToolResult,
};

pub(crate) const MODEL_OVERRIDE_DESCRIPTION: &str = "Required for every new child task. Select low for simple mechanical work, medium for standard implementation or investigation, and high for complex reasoning. Choose by task complexity without asking the user to choose a tier. This explicit selection takes precedence over role and configured child defaults. Do not omit it or use \"inherit\". Concrete model IDs are disabled; use only low, medium, or high. An unavailable tier falls back to the captured parent model. Existing children keep their original model; use followup_task where available, or the message tool's explicit turn trigger.";
const REQUIRED_MODEL_ERROR: &str =
    "model is required for a new subagent: select from the advertised model policy; do not use inherit.";

fn new_child_model(
    input: &Json,
    model_catalog: bool,
    has_models: bool,
) -> Result<RuntimeString, ToolError> {
    let model = input
        .get("model")
        .and_then(Json::as_str)
        .filter(|model| {
            model
                .try_to_utf8()
                .is_ok_and(|text| !text.trim().is_empty() && text.trim() != "inherit")
        })
        .ok_or_else(|| ToolError::new(REQUIRED_MODEL_ERROR))?;
    let tier = ["low", "medium", "high"].iter().any(|tier| model == *tier);
    if !model_catalog && !tier {
        return Err(ToolError::new(
            "Concrete subagent models are disabled. Select low, medium, or high.",
        ));
    }
    if model_catalog && !has_models {
        return Err(ToolError::new("The subagent model directory has no available models. No child can be created until the directory is available."));
    }
    if model_catalog && tier {
        return Err(ToolError::new("Model-directory mode requires a concrete source:modelId from the advertised directory, not a model tier."));
    }
    Ok(model.clone())
}

pub(crate) fn model_selection_description(
    catalog: bool,
    options: &[SubagentModelOption],
) -> RuntimeString {
    if !catalog {
        return MODEL_OVERRIDE_DESCRIPTION.into();
    }
    let base = RuntimeString::from("Required for every new child task. Choose by task complexity from the exact source:modelId values in the advertised model directory. Do not pass low, medium, high, inherit, or guess model IDs. This explicit selection takes precedence over role and configured child defaults. Only an invalid or unavailable concrete choice falls back to the captured parent model. Existing children keep their original model; use followup_task where available, or the message tool's explicit turn trigger.");
    if options.is_empty() {
        return base.concat(&RuntimeString::from(" The model directory has no available models. Do not create a new child or guess an ID until the directory is available."));
    }
    let entries = options
        .iter()
        .map(|option| {
            runtime_object([
                ("value", RuntimeJson::String(option.value.clone())),
                ("label", RuntimeJson::String(option.label.clone())),
            ])
        })
        .collect();
    base.concat(&RuntimeString::from(
        " Model directory (labels are display data): ",
    ))
    .concat(&runtime_json::stringify(&RuntimeJson::Array(entries)))
}
const RESUME_CREATION_FIELDS: &[&str] = &[
    "name",
    "agentType",
    "subagent_type",
    "agent_type",
    "model",
    "permission_mode",
    "persistent",
    "run_in_background",
    "peer",
    "fork_context",
    "fork_turns",
    "effort",
    "reasoning_effort",
    "cwd",
    "output_schema",
    "memory_dir",
    "max_turns",
    "tools",
    "allowed_tools",
    "auto_allow_tools",
    "isolation",
    "swarm",
    "inherited_effort",
    "task_agent_type_raw",
    "task_resume_raw",
];
const RESUME_OVERRIDE_ERROR: &str =
    "Resume only accepts task direction; the original subagent configuration cannot be replaced.";

fn has_resume_creation_override(input: &Json) -> bool {
    RESUME_CREATION_FIELDS
        .iter()
        .any(|key| input.get(*key).is_some_and(|value| !value.is_null()))
}

/// A named sub-agent role the `task` tool can advertise + validate against.
///
/// (Faithful subset of the engine's agent definition: only the fields the tool
/// reads — `name` / `description` steer the model, while `isolation` /
/// `background` set trusted per-agent defaults.)
#[derive(Debug, Clone, Default)]
pub struct AgentDefinition {
    /// Unique role name (the value passed as `agentType` / `subagent_type`).
    pub name: RuntimeString,
    /// One-line description surfaced to the model so it knows when to delegate.
    pub description: RuntimeString,
    /// Default isolation policy for this agent definition.
    pub isolation: Option<Isolation>,
    /// Always run this agent detached as a background task when spawned.
    pub background: Option<bool>,
}

/// Options for [`create_task_tool`].
#[derive(Debug, Clone, Default)]
pub struct TaskToolOptions {
    /// Named sub-agent roles to advertise + validate against.
    pub agents: Vec<AgentDefinition>,
    pub model_catalog: bool,
    pub model_options: Vec<SubagentModelOption>,
}

/// Build the `agentType` schema fragment (shared by `subagent_type` +
/// `agentType`): an enum over the role names, else a free-string.
fn runtime_object(fields: impl IntoIterator<Item = (&'static str, RuntimeJson)>) -> RuntimeJson {
    let mut object = RuntimeObject::new();
    for (key, value) in fields {
        object.insert(key, value);
    }
    RuntimeJson::Object(object)
}

fn join_runtime_strings(values: &[RuntimeString], separator: &str) -> RuntimeString {
    let Some(mut output) = values.first().cloned() else {
        return RuntimeString::default();
    };
    let separator = RuntimeString::from(separator);
    for value in &values[1..] {
        output = output.concat(&separator).concat(value);
    }
    output
}

fn agent_type_schema(agent_names: &[RuntimeString]) -> RuntimeJson {
    if agent_names.is_empty() {
        runtime_object([
            ("type", RuntimeJson::from("string")),
            (
                "description",
                RuntimeJson::from("Optional sub-agent type/role name"),
            ),
        ])
    } else {
        let description = RuntimeString::from("Sub-agent role to delegate to. One of: ")
            .concat(&join_runtime_strings(agent_names, ", "))
            .concat(&RuntimeString::from("."));
        runtime_object([
            ("type", RuntimeJson::from("string")),
            (
                "enum",
                RuntimeJson::Array(
                    agent_names
                        .iter()
                        .cloned()
                        .map(RuntimeJson::String)
                        .collect(),
                ),
            ),
            ("description", RuntimeJson::String(description)),
        ])
    }
}

/// Build the tool's input JSON Schema.
fn build_input_schema(agent_names: &[RuntimeString], opts: &TaskToolOptions) -> RuntimeJson {
    let agent_type = agent_type_schema(agent_names);
    let mut schema = runtime_json::parse(&RuntimeString::from(
        r#"{
          "type":"object",
          "properties":{
            "description":{"type":"string","description":"A short (3-5 word) description of the task"},
            "name":{"type":"string","description":"Optional stable name for addressing this subagent in later messages."},
            "prompt":{"type":"string","description":"A self-contained task for the sub-agent to perform"},
            "subagent_type":null,
            "agentType":null,
            "model":{"type":"string","minLength":1},
            "reasoning_effort":{"type":"string","description":"Reasoning effort override for the child. Omit to inherit the parent selection."},
            "run_in_background":{"type":"boolean","description":"Sub-agents run in the background by default; you will be told when one completes, so do not poll for it — read its result with task_output (or its output file) once you are notified. Set to false to run this sub-agent synchronously when you need its result before you can do anything else."},
            "fork_turns":{"type":"string","description":"Conversation history to inherit: all (default) or none. Positive integer strings also inherit the full conversation."},
            "peer":{"type":"string","description":"Swarm peer name to give this sub-agent (it joins your team as that peer)"},
            "persistent":{"type":"boolean","description":"Spawn a PERSISTENT teammate: a long-lived worker that runs this prompt, goes idle, then keeps processing new SendMessage messages until stopped. Requires `peer` + swarm. Always runs detached; returns an id + result file."}
          },
          "required":["description","prompt","model"],
          "additionalProperties":false
        }"#,
    ))
    .expect("native task schema is valid exact JSON");
    let properties = schema
        .get_mut("properties")
        .and_then(RuntimeJson::as_object_mut)
        .expect("task schema properties are an object");
    properties.insert("subagent_type", agent_type.clone());
    properties.insert("agentType", agent_type);
    let model = properties
        .get_mut("model")
        .and_then(RuntimeJson::as_object_mut)
        .expect("task model schema is an object");
    model.insert(
        "description",
        RuntimeJson::String(model_selection_description(
            opts.model_catalog,
            &opts.model_options,
        )),
    );
    let choices = if opts.model_catalog {
        opts.model_options
            .iter()
            .map(|option| RuntimeJson::String(option.value.clone()))
            .collect::<Vec<_>>()
    } else {
        ["low", "medium", "high"].map(RuntimeJson::from).to_vec()
    };
    if !choices.is_empty() {
        model.insert("enum", RuntimeJson::Array(choices));
    }
    schema
}

fn json_input_type(value: &Json) -> &'static str {
    match value {
        Json::Null => "null",
        Json::Bool(_) => "boolean",
        Json::Number(_) => "number",
        Json::String(_) => "string",
        Json::Array(_) => "array",
        Json::Object(_) => "object",
    }
}

fn parse_task_input(
    input: &Json,
    agents: &[AgentDefinition],
    model_catalog: bool,
    has_models: bool,
) -> Result<Json, ToolError> {
    let object = input.as_object().ok_or_else(|| {
        ToolError::new(format!(
            "Invalid arguments: (root): Invalid input: expected object, received {}",
            json_input_type(input)
        ))
    })?;

    // Inspect the supplied fields before normalization can discard an override.
    if object
        .get("resume")
        .and_then(Json::as_str)
        .is_some_and(|value| !value.is_empty())
        && has_resume_creation_override(input)
    {
        return Err(ToolError::new(RESUME_OVERRIDE_ERROR));
    }

    let mut parsed = RuntimeObject::new();
    let mut issues: Vec<RuntimeString> = Vec::new();
    for key in ["description", "prompt"] {
        match object.get(key) {
            Some(value @ Json::String(_)) => {
                parsed.insert(key, value.clone());
            }
            Some(value) => issues.push(RuntimeString::from(format!(
                "{key}: Invalid input: expected string, received {}",
                json_input_type(value)
            ))),
            None => issues.push(RuntimeString::from(format!(
                "{key}: Invalid input: expected string, received undefined"
            ))),
        }
    }

    for key in ["subagent_type", "agentType"] {
        if let Some(value) = object.get(key).filter(|value| !value.is_null()) {
            let trimmed = value
                .as_str()
                .and_then(|value| value.try_to_utf8().ok())
                .map(|value| RuntimeString::from(value.trim()));
            if trimmed.as_ref().is_some_and(RuntimeString::is_empty) {
                continue;
            }
            let value = trimmed
                .as_ref()
                .map(|value| Json::String(value.clone()))
                .unwrap_or_else(|| value.clone());
            let valid = value.as_str().is_some_and(|value| {
                agents.is_empty() || agents.iter().any(|agent| &agent.name == value)
            });
            if valid {
                parsed.insert(key, value.clone());
            } else if agents.is_empty() {
                issues.push(RuntimeString::from(format!(
                    "{key}: Invalid input: expected string, received {}",
                    json_input_type(&value)
                )));
            } else {
                let expected = agents
                    .iter()
                    .map(|agent| runtime_json::stringify(&RuntimeJson::String(agent.name.clone())))
                    .collect::<Vec<_>>();
                issues.push(
                    RuntimeString::from(format!("{key}: Invalid option: expected one of "))
                        .concat(&join_runtime_strings(&expected, "|")),
                );
            }
        }
    }

    if let Some(value) = object.get("model").filter(|value| !value.is_null()) {
        if value.is_string() {
            parsed.insert("model", value.clone());
        } else {
            issues.push(RuntimeString::from(format!(
                "model: Invalid input: expected string, received {}",
                json_input_type(value)
            )));
        }
    }
    if object
        .get("resume")
        .and_then(Json::as_str)
        .is_none_or(|value| value.is_empty())
        && object
            .get("model")
            .is_none_or(|value| value.is_null() || value.is_string())
    {
        issues.extend(
            new_child_model(input, model_catalog, has_models)
                .err()
                .map(|error| RuntimeString::from(error.to_string())),
        );
    }
    if let Some(value) = object.get("reasoning_effort") {
        if value.is_null() {
            // A nullable optional selection has the same meaning as omission.
        } else if value.is_string() {
            match EffortValue::decode_runtime(value) {
                Ok(_) => {
                    parsed.insert("reasoning_effort", value.clone());
                }
                Err(error) => {
                    issues.push(RuntimeString::from(format!("reasoning_effort: {error}")))
                }
            }
        } else {
            issues.push(RuntimeString::from(format!(
                "reasoning_effort: Invalid input: expected string, received {}",
                json_input_type(value)
            )));
        }
    }
    if let Some(value) = object.get("run_in_background") {
        if value.is_boolean() {
            parsed.insert("run_in_background", value.clone());
        } else {
            issues.push(RuntimeString::from(format!(
                "run_in_background: Invalid input: expected boolean, received {}",
                json_input_type(value)
            )));
        }
    }
    if let Some(value) = object.get("resume") {
        if value.is_string() {
            parsed.insert("resume", value.clone());
        } else {
            issues.push(RuntimeString::from(format!(
                "resume: Invalid input: expected string, received {}",
                json_input_type(value)
            )));
        }
    }
    if object
        .get("fork_context")
        .is_some_and(|value| !value.is_null())
    {
        issues.push("fork_context is not supported; use fork_turns instead".into());
    }
    if let Some(value) = object.get("fork_turns").filter(|value| !value.is_null()) {
        if value.is_string() {
            parsed.insert("fork_turns", value.clone());
        } else {
            issues.push(
                format!(
                    "fork_turns: Invalid input: expected string, received {}",
                    json_input_type(value)
                )
                .into(),
            );
        }
    }
    if let Err(error) = task_fork_turns(parsed.get("fork_turns")) {
        issues.push(error);
    }
    if let Some(value) = object.get("peer") {
        if value.is_string() {
            parsed.insert("peer", value.clone());
        } else {
            issues.push(RuntimeString::from(format!(
                "peer: Invalid input: expected string, received {}",
                json_input_type(value)
            )));
        }
    }
    if let Some(value) = object.get("name") {
        if value.is_string() {
            parsed.insert("name", value.clone());
        } else {
            issues.push(RuntimeString::from(format!(
                "name: Invalid input: expected string, received {}",
                json_input_type(value)
            )));
        }
    }
    if let Some(value) = object.get("persistent") {
        if value.is_boolean() {
            parsed.insert("persistent", value.clone());
        } else {
            issues.push(RuntimeString::from(format!(
                "persistent: Invalid input: expected boolean, received {}",
                json_input_type(value)
            )));
        }
    }

    if issues.is_empty() {
        Ok(Json::Object(parsed))
    } else {
        Err(ToolError::new(
            RuntimeString::from("Invalid arguments: ").concat(&join_runtime_strings(&issues, "; ")),
        ))
    }
}

fn task_fork_turns(value: Option<&Json>) -> Result<ForkContext, RuntimeString> {
    if value.is_some_and(|value| !value.is_null() && !value.is_string()) {
        return Err("fork_turns must be `none` or `all`".into());
    }
    let text = value
        .and_then(Json::as_str)
        .map(RuntimeString::try_to_utf8)
        .transpose()
        .map_err(|_| RuntimeString::from("fork_turns must be `none` or `all`"))?;
    let text = text
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("all");
    if text.eq_ignore_ascii_case("none") {
        Ok(ForkContext::None)
    } else if text.eq_ignore_ascii_case("all") || text.parse::<NonZeroUsize>().is_ok() {
        Ok(ForkContext::All)
    } else {
        Err("fork_turns must be `none` or `all`".into())
    }
}

#[derive(Clone, Copy)]
enum JsSlot<'a> {
    Undefined,
    Value(&'a Json),
}

fn js_property<'a>(input: &'a Json, property: &'static str) -> Result<JsSlot<'a>, ToolError> {
    match input {
        Json::Null => Err(ToolError::new(format!(
            "Cannot read properties of null (reading '{property}')"
        ))),
        Json::Object(object) => Ok(object
            .get(property)
            .map(JsSlot::Value)
            .unwrap_or(JsSlot::Undefined)),
        Json::Bool(_) | Json::Number(_) | Json::String(_) | Json::Array(_) => Ok(JsSlot::Undefined),
    }
}

fn js_nullish(slot: JsSlot<'_>) -> bool {
    matches!(slot, JsSlot::Undefined | JsSlot::Value(Json::Null))
}

fn js_truthy(slot: JsSlot<'_>) -> bool {
    match slot {
        JsSlot::Undefined | JsSlot::Value(Json::Null | Json::Bool(false)) => false,
        // The JS-JSON boundary retains overflowed numeric tokens (`1e400` =>
        // `Infinity`) as arbitrary-precision markers. They have no finite
        // `as_f64` representation, but remain truthy like JS `Infinity`.
        JsSlot::Value(Json::Number(number)) => number.as_f64() != 0.0,
        JsSlot::Value(Json::String(value)) => !value.is_empty(),
        JsSlot::Value(Json::Bool(true) | Json::Array(_) | Json::Object(_)) => true,
    }
}

fn js_truthy_string<'a>(slot: JsSlot<'a>) -> Option<&'a crate::runtime_values::RuntimeString> {
    match slot {
        JsSlot::Value(Json::String(value)) if !value.is_empty() => Some(value),
        _ => None,
    }
}

fn js_truthy_non_string(slot: JsSlot<'_>) -> Option<Json> {
    match slot {
        JsSlot::Value(value) if !value.is_string() && js_truthy(slot) => Some(value.clone()),
        JsSlot::Undefined | JsSlot::Value(_) => None,
    }
}

/// Fixed JS reaches string interpolation only after a raw agent-type misses the
/// registry Map. An own non-callable `toString` makes ToPrimitive throw; arrays
/// recurse through their elements while joining them for the error text.
fn js_agent_type_to_primitive_throws(value: &Json) -> bool {
    match value {
        Json::Object(object) => object.contains_key("toString"),
        Json::Array(values) => values.iter().any(js_agent_type_to_primitive_throws),
        Json::Null | Json::Bool(_) | Json::Number(_) | Json::String(_) => false,
    }
}

/// Compose the tool description, listing available roles when any are defined.
fn build_description(agents: &[AgentDefinition]) -> RuntimeString {
    let base = "Delegate a self-contained subtask to an in-process sub-agent when permitted by the current collaboration mode. Supply the goal, relevant context and expected result. Every new child requires model: choose by task complexity from the tiers or model directory advertised in the model parameter. Concrete model choices require the model-directory setting to be enabled. Do not omit model or use inherit. The model parameter defines the active selection policy and fallback boundary. Continue an existing child with followup_task where available, or the message tool's explicit turn trigger, keeping its original model. Specify owned modules or responsibility boundaries, deliverables, and dependencies or handoffs with other tasks. Independent task calls may run in parallel; sequence calls when one needs another's result. Sub-agents run in the background by default and report completion; read a finished result with task_output, and wait only when it blocks your next step. Use SendMessage for further direction and task_stop only when the agent should stop. Verify consequential changes before relying on a report. These operations execute directly in the engine and require no workflow program.";
    if agents.is_empty() {
        return RuntimeString::from(base);
    }
    let roles: Vec<RuntimeString> = agents
        .iter()
        .map(|a| {
            let desc = if a.description.is_empty() {
                RuntimeString::from("(no description)")
            } else {
                a.description.clone()
            };
            RuntimeString::from("- ")
                .concat(&a.name)
                .concat(&RuntimeString::from(": "))
                .concat(&desc)
        })
        .collect();
    RuntimeString::from(base)
        .concat(&RuntimeString::from(
            "\n\nAvailable agent types (pass as `agentType` or `subagent_type`):\n",
        ))
        .concat(&join_runtime_strings(&roles, "\n"))
}

/// Spawn an in-process sub-agent to handle a self-contained subtask.
pub struct TaskTool {
    agents: Vec<AgentDefinition>,
    description: RuntimeString,
    input_schema: RuntimeJson,
    model_catalog: bool,
    has_models: bool,
}

impl TaskTool {
    fn new(opts: TaskToolOptions) -> Self {
        let agent_names: Vec<RuntimeString> = opts.agents.iter().map(|a| a.name.clone()).collect();
        TaskTool {
            description: build_description(&opts.agents),
            input_schema: build_input_schema(&agent_names, &opts),
            model_catalog: opts.model_catalog,
            has_models: !opts.model_options.is_empty(),
            agents: opts.agents,
        }
    }
}

/// Construct the `task` tool as an erased registry-side [`Tool`].
pub fn create_task_tool(opts: TaskToolOptions) -> Arc<dyn Tool> {
    Arc::new(TaskTool::new(opts))
}

#[async_trait]
impl Tool for TaskTool {
    fn static_name(&self) -> &str {
        "task"
    }

    fn description(&self) -> RuntimeString {
        self.description.clone()
    }

    fn input_schema(&self) -> Json {
        self.input_schema.clone()
    }

    fn is_enabled(&self) -> bool {
        true
    }

    // Multiple FOREGROUND task calls in one assistant turn fan out in parallel.
    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        true
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        // Not read-only: a sub-agent is a worker with its own tool budget, so the
        // permission engine must decide it explicitly rather than wave it through.
        false
    }

    fn planning_capability(&self, input: &Json) -> crate::types::tool::PlanningToolCapability {
        if input
            .get("resume")
            .is_some_and(|value| js_truthy(JsSlot::Value(value)))
        {
            crate::types::tool::PlanningToolCapability::None
        } else {
            let Ok(primary_type) = js_property(input, "subagent_type") else {
                return crate::types::tool::PlanningToolCapability::None;
            };
            let selected_type = if js_nullish(primary_type) {
                match js_property(input, "agentType") {
                    Ok(value) => value,
                    Err(_) => return crate::types::tool::PlanningToolCapability::None,
                }
            } else {
                primary_type
            };
            let isolation = js_truthy_string(selected_type)
                .and_then(|name| self.agents.iter().find(|agent| &agent.name == name))
                .and_then(|agent| agent.isolation);
            if isolation.is_some() {
                // Isolation setup can create a worktree before the child's
                // query tools receive their own captured effect ceiling.
                crate::types::tool::PlanningToolCapability::None
            } else {
                crate::types::tool::PlanningToolCapability::Research
            }
        }
    }

    async fn check_permissions(&self, input: &Json, ctx: &ToolContext) -> Option<PermissionResult> {
        // Plan mode admits planning sub-agents. Research is what planning IS, and
        // the guidance asks for parallel exploration agents — denying `task` here
        // made that instruction impossible to follow and serialized every
        // investigation.
        //
        // This is safe because of what a spawned child inherits, not because the
        // parent is trusted: planning imposes a read-only effect floor on the child.
        // An agent definition cannot remove its parent's planning constraints.
        if ctx.planning_read_only
            && !input
                .get("resume")
                .is_some_and(|value| js_truthy(JsSlot::Value(value)))
        {
            return Some(PermissionResult::Allow {
                updated_input: None,
                reason: PermissionReason::ToolSelfCheck,
            });
        }
        None
    }

    fn parse_input(&self, input: &Json) -> Result<Json, ToolError> {
        parse_task_input(input, &self.agents, self.model_catalog, self.has_models)
    }

    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        let d = input
            .get("description")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .unwrap_or_default();
        Some(RuntimeString::from("Delegating: ").concat(&d))
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let resume_slot = js_property(&input, "resume")?;
        if js_truthy(resume_slot) {
            let Some(target) = js_truthy_string(resume_slot) else {
                return Ok(text_result(
                    runtime_object([("ok", Json::Bool(false))]),
                    "Resume requires an existing subagent ID.",
                    true,
                ));
            };
            let Some(registry) = &ctx.background_tasks else {
                return Ok(text_result(
                    runtime_object([("ok", Json::Bool(false))]),
                    "Existing subagent control is not available in this context.",
                    true,
                ));
            };
            if has_resume_creation_override(&input) {
                return Ok(text_result(
                    runtime_object([("ok", Json::Bool(false))]),
                    RESUME_OVERRIDE_ERROR,
                    true,
                ));
            }
            let message = input
                .get("prompt")
                .and_then(Json::as_str)
                .cloned()
                .unwrap_or_default();
            let summary = message_preview(&message);
            if let Err(error) = validate_text_message(target, &message, &summary) {
                return Ok(text_result(
                    runtime_object([("ok", Json::Bool(false))]),
                    error,
                    true,
                ));
            }
            let request = crate::types::tool::SubagentMessageRequest {
                trigger_turn: true,
                target: target.clone(),
                summary,
                message,
                sender_agent_id: registry.caller_agent_id(),
                user_initiated: false,
            };
            return Ok(match registry.send_message(request).await {
                Some(Ok(result)) => text_result(
                    runtime_object([
                        ("ok", Json::Bool(true)),
                        ("agentId", Json::String(result.agent_id)),
                        ("resumed", Json::Bool(result.resumed)),
                    ]),
                    "Task direction delivered to the existing subagent.",
                    false,
                ),
                Some(Err(error)) => text_result(
                    runtime_object([
                        ("ok", Json::Bool(false)),
                        ("message", Json::String(error.clone())),
                    ]),
                    error,
                    true,
                ),
                None => text_result(
                    runtime_object([("ok", Json::Bool(false))]),
                    "No ordinary subagent matches the resume target; message teammates by name.",
                    true,
                ),
            });
        }
        let model = new_child_model(&input, self.model_catalog, self.has_models)?;
        if input
            .get("fork_context")
            .is_some_and(|value| !value.is_null())
        {
            return Err(ToolError::new(
                "fork_context is not supported; use fork_turns instead",
            ));
        }
        if ctx.spawn_sub_agent.is_none() && ctx.spawn_background_agent.is_none() {
            return Ok(text_result(
                runtime_object([
                    ("ok", RuntimeJson::Bool(false)),
                    ("text", RuntimeJson::String(RuntimeString::default())),
                ]),
                "Sub-agents are not available in this context.",
                true,
            ));
        }
        let primary_type = js_property(&input, "subagent_type")?;
        let selected_type_slot = if js_nullish(primary_type) {
            js_property(&input, "agentType")?
        } else {
            primary_type
        };
        let selected_type = js_truthy_string(selected_type_slot);
        let selected_type_raw = js_truthy_non_string(selected_type_slot);
        let persistent_slot = js_property(&input, "persistent")?;
        let persistent = js_truthy(persistent_slot);
        // A persistent teammate is addressed by its `peer` name; without one it
        // would register under a private derived name the coordinator is never
        // told. Reject up front.
        if persistent && !js_truthy(js_property(&input, "peer")?) {
            return Ok(text_result(
                runtime_object([
                    ("ok", RuntimeJson::Bool(false)),
                    ("text", RuntimeJson::String(RuntimeString::default())),
                ]),
                "A persistent teammate requires a `peer` name so teammates can message it.",
                true,
            ));
        }
        let selected_def =
            selected_type.and_then(|selected| self.agents.iter().find(|a| selected == &a.name));
        let isolation = selected_def.and_then(|definition| definition.isolation);

        let effort = input
            .get("reasoning_effort")
            .filter(|value| !value.is_null())
            .map(EffortValue::decode_runtime)
            .transpose()
            .map_err(|error| ToolError::new(format!("reasoning_effort: {error}")))?;
        let resume_slot = js_property(&input, "resume")?;
        let resume = js_truthy_string(resume_slot);
        let resume_raw = js_truthy_non_string(resume_slot);
        let prompt = match js_property(&input, "prompt")? {
            JsSlot::Value(Json::String(prompt)) => prompt.clone(),
            JsSlot::Undefined | JsSlot::Value(_) => Default::default(),
        };
        let peer = js_truthy_string(js_property(&input, "peer")?);
        let name = js_truthy_string(js_property(&input, "name")?).cloned();
        let fork_context = Some(task_fork_turns(input.get("fork_turns")).map_err(ToolError::new)?);
        let background_slot = js_property(&input, "run_in_background")?;
        // Asked for detachment in so many words. Only the literal `true` counts:
        // `parse_input` rejects a non-boolean, so anything else is not a request
        // and must not draw a refusal. Tracked apart from the default because
        // only an explicit request deserves one when the context cannot detach.
        let background_requested = matches!(background_slot, JsSlot::Value(Json::Bool(true)));
        // Likewise, opting out is the literal `false` and nothing else.
        let background_declined = matches!(background_slot, JsSlot::Value(Json::Bool(false)));

        // Detached is the DEFAULT, not an opt-in. A boolean whose default is
        // false does not get turned on: the model reads the default as the
        // recommendation, so the background path sat unused even though a
        // finished agent already reports back on its own (the engine queues a
        // task-notification the turn loop drains inline). A synchronous
        // sub-agent also holds the parent's turn open for its whole run, so
        // everything queued behind it waits. A persistent teammate and an agent
        // declared `background: true` stay detached even against an explicit
        // opt-out — their lifecycle is not the caller's to choose.
        let background_required =
            persistent || selected_def.and_then(|d| d.background).unwrap_or(false);
        let want_background = background_required || !background_declined;
        if want_background {
            if let Some(spawn_bg) = &ctx.spawn_background_agent {
                let opts = SpawnSubAgentOptions {
                    prompt: prompt.clone(),
                    name: name.clone(),
                    task_agent_type_raw: selected_type_raw.clone(),
                    agent_type: selected_type.cloned(),
                    model: Some(model.clone()),
                    effort,
                    resume: resume.cloned(),
                    task_resume_raw: resume_raw.clone(),
                    swarm: peer.map(|p| SwarmSpawn { peer: p.clone() }),
                    persistent: if persistent { Some(true) } else { None },
                    isolation,
                    fork_context,
                    ..Default::default()
                };
                let result = match spawn_bg(opts).await {
                    Ok(result) => result,
                    Err(error) => {
                        return Ok(text_result(
                            runtime_object([
                                ("ok", Json::Bool(false)),
                                ("message", Json::String(error.clone())),
                            ]),
                            error,
                            true,
                        ))
                    }
                };
                let label = if persistent {
                    "persistent teammate"
                } else {
                    "background sub-agent"
                };
                let text = RuntimeString::from("Started ")
                    .concat(&RuntimeString::from(label))
                    .concat(&RuntimeString::from(" "))
                    .concat(&result.agent_id)
                    .concat(&RuntimeString::from(". Read "))
                    .concat(result.output_file.as_runtime_string())
                    .concat(&RuntimeString::from(" once it completes for the result."));
                return Ok(text_result(
                    runtime_object([
                        ("ok", RuntimeJson::Bool(true)),
                        ("text", RuntimeJson::String(RuntimeString::default())),
                        ("agentId", RuntimeJson::String(result.agent_id)),
                        (
                            "outputFile",
                            RuntimeJson::String(result.output_file.as_runtime_string().clone()),
                        ),
                    ]),
                    text,
                    false,
                ));
            }
            // Nothing to detach with. Falling through is right when detachment
            // was only the default, but a caller who asked for it explicitly
            // would otherwise get a synchronous run and no word of it.
            if background_requested || background_required {
                return Ok(text_result(
                    runtime_object([
                        ("ok", RuntimeJson::Bool(false)),
                        ("text", RuntimeJson::String(RuntimeString::default())),
                    ]),
                    "This context cannot run a sub-agent in the background. Pass run_in_background=false to run it synchronously instead.",
                    true,
                ));
            }
        }

        if !self.agents.is_empty()
            && selected_type_raw
                .as_ref()
                .is_some_and(js_agent_type_to_primitive_throws)
        {
            return Err(ToolError::new("Cannot convert object to primitive value"));
        }

        // Foreground: forward the child's nested activity + extract its final answer.
        let Some(spawn) = ctx.spawn_sub_agent.as_ref() else {
            return Ok(text_result(
                runtime_object([
                    ("ok", RuntimeJson::Bool(false)),
                    ("text", RuntimeJson::String(RuntimeString::default())),
                ]),
                "This context cannot run a sub-agent synchronously. Omit run_in_background or set it to true to start a background sub-agent.",
                true,
            ));
        };
        let opts = SpawnSubAgentOptions {
            prompt,
            name,
            task_agent_type_raw: selected_type_raw,
            agent_type: selected_type.cloned(),
            model: Some(model),
            effort,
            resume: resume.cloned(),
            task_resume_raw: resume_raw,
            swarm: peer.map(|p| SwarmSpawn { peer: p.clone() }),
            isolation,
            fork_context,
            ..Default::default()
        };
        let run = match spawn(opts).await {
            Ok(run) => run,
            Err(error) => {
                return Ok(text_result(
                    runtime_object([
                        ("ok", Json::Bool(false)),
                        ("message", Json::String(error.clone())),
                    ]),
                    error,
                    true,
                ))
            }
        };
        let agent_id = run.agent_id.clone();
        let ForegroundSubAgentResult {
            final_text,
            failure,
        } = consume_foreground_sub_agent(ctx, run).await;
        let one_shot = ctx
            .background_tasks
            .as_ref()
            .and_then(|registry| agent_id.as_str().and_then(|id| registry.is_one_shot(id)))
            .unwrap_or(false);
        if let Some(f) = failure.filter(|failure| !failure.is_empty()) {
            let mut output = runtime_object([
                ("ok", RuntimeJson::Bool(false)),
                ("text", RuntimeJson::String(final_text)),
            ]);
            if !one_shot {
                output
                    .as_object_mut()
                    .expect("task output object")
                    .insert("agentId", agent_id);
            }
            return Ok(text_result(
                output,
                RuntimeString::from("Sub-agent failed: ").concat(&f),
                true,
            ));
        }
        let text = if final_text.is_empty() {
            RuntimeString::from("(sub-agent produced no output)")
        } else {
            final_text.clone()
        };
        let mut output = runtime_object([
            ("ok", RuntimeJson::Bool(true)),
            ("text", RuntimeJson::String(final_text)),
        ]);
        if !one_shot {
            output
                .as_object_mut()
                .expect("task output object")
                .insert("agentId", agent_id);
        }
        Ok(text_result(output, text, false))
    }
}

#[cfg(test)]
pub(super) async fn dispatch_native_message_tool(
    tool: Arc<dyn Tool>,
    visible_name: &str,
    input: Json,
    context_factory: crate::engine::query::ToolContextFactory,
    provider_info: Option<crate::types::provider::ProviderInfo>,
) -> crate::schemas::message::ToolResultMessage {
    use crate::engine::deps::{noop_compactor, EngineDeps};
    use crate::engine::query::{query, QueryParams, SystemPromptResolvable};
    use crate::permission::engine::StaticPermissionEngine;
    use crate::schemas::content::{ContentBlock, TextBlock};
    use crate::schemas::message::{Message, UserMessage};
    use crate::testing::{ev, scripted_call_model};
    use crate::types::abort::AbortSignal;
    use crate::types::provider::ModelStopReason;
    use futures::StreamExt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    let config = crate::schemas::config::default_config();
    let tools = vec![tool];
    let tool_specs = crate::tools::pool::to_tool_specs(&tools);
    let messages = Arc::new(Mutex::new(Vec::<Message>::new()));
    let recorded = messages.clone();
    let params = QueryParams {
        retained_tool_tasks: None,
        prepare_request_usage: None,
        multi_agent_instructions: true,
        thread_id: "native-input-thread".into(),
        turn_id: "native-input-turn".into(),
        model: "fake-model".into(),
        provider_info,
        messages: vec![Message::User(UserMessage {
            question_answer: None,
            uuid: "native-input-user".into(),
            content: vec![ContentBlock::Text(TextBlock {
                text: "Deliver task direction.".into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_meta: None,
        })],
        compaction_protected_message_ids: Default::default(),
        context_window: None,
        compaction_history: None,
        recover_compaction_history: None,
        compaction_output: None,
        surfaced_tools: Some(tools.iter().map(|tool| tool.name()).collect()),
        tools,
        tool_specs,
        refresh_tools: None,
        refresh_tools_async: None,
        agent_tool_policy: None,
        system_prompt: SystemPromptResolvable::Parts(Vec::new()),
        config: config.clone(),
        max_turns: None,
        cost_so_far: None,
        tool_context_base: context_factory,
        permission_engine: Arc::new(StaticPermissionEngine::new(&config)),
        hook_bus: None,
        signal: AbortSignal::new(),
        approval_handler: None,
        approval_reviewer: None,
        output_schema: None,
        token_budget: None,
        goal: None,
        goal_request_identity: None,
        goal_pending_input: None,
        goal_execution_enabled: None,
        goal_usage: None,
        web_search: None,
        on_message: Some(Box::new(move |message| {
            recorded.lock().unwrap().push(message);
            Box::pin(async { Ok(()) })
        })),
        on_permission_update: None,
        message_rollout: None,
        on_compaction: None,
        on_surface_tools: None,
        on_tool_lifecycle: None,
        provider_call_admission: None,
        on_provider_request_admitted: None,
        emit_thread_started: true,
        reminder_providers: Vec::new(),
        edited_files_reminder: None,
        effort: None,
        ultra_mode: false,
        thinking: None,
        skip_cache_write: None,
        cache_ttl: None,
        cache_domain: None,
        cache_key: None,
        plan_file_path: None,
        message_queue: None,
        input_pdf_bytes: 0,
        input_pdf_documents: 0,
        validate_native_pdf: None,
        agent_id: None,
        agent_type: None,
        context_hint_state: None,
        tool_result_budget_state: None,
        on_tool_result_budget_records: None,
        thinking_context_cold: false,
        padded_countdown_spend: Default::default(),
        task_countdown_anchor: None,
        live_model_override: None,
    };
    let mut turn = vec![ev::message_start(None)];
    turn.extend(ev::tool_use("native-input-call", visible_name, &input));
    turn.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let uuid = Arc::new(AtomicUsize::new(0));
    let deps = EngineDeps {
        provider_id: "native-input-provider".into(),
        call_model: scripted_call_model(vec![turn]),
        estimate_tokens: Arc::new(|_| 0),
        compactor: noop_compactor(),
        uuid: Arc::new(move || format!("native-input-{}", uuid.fetch_add(1, Ordering::SeqCst))),
        now: Arc::new(|| 0),
    };
    let events: Vec<_> = query(params, deps).collect().await;
    assert!(
        events.iter().any(|event| matches!(
            event,
            crate::types::events::EngineEvent::TurnCompleted { .. }
        )),
        "the scripted dispatcher turn must finish"
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, crate::types::events::EngineEvent::TurnFailed { .. })),
        "input refusal must remain a tool result rather than failing the turn"
    );
    let result = messages
        .lock()
        .unwrap()
        .iter()
        .find_map(|message| match message {
            Message::ToolResult(result) if result.tool_use_id == "native-input-call" => {
                Some(result.clone())
            }
            _ => None,
        })
        .expect("the dispatcher must produce the requested tool result");
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::behavior::{
        normalize_tool_input_for_behavior, render_tool_specs_for_behavior,
        resolve_provider_behavior_profile,
    };
    use crate::tools::builtins::test_ctx::make_ctx;
    use crate::types::errors::{EngineError, EngineErrorCode};
    use crate::types::events::EngineEvent;
    use crate::types::provider::{ProviderInfo, ToolSpec, ToolSpecKind};
    use crate::types::tool::{ForegroundSubAgentRun, ForkContext, SpawnBackgroundAgentResult};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    macro_rules! json {
        (@array [$($elements:expr,)*]) => {
            vec![$($elements,)*]
        };
        (@array [$($elements:expr),*]) => {
            vec![$($elements),*]
        };
        (@array [$($elements:expr,)*] null $($rest:tt)*) => {
            json!(@array [$($elements,)* RuntimeJson::Null] $($rest)*)
        };
        (@array [$($elements:expr,)*] true $($rest:tt)*) => {
            json!(@array [$($elements,)* RuntimeJson::Bool(true)] $($rest)*)
        };
        (@array [$($elements:expr,)*] false $($rest:tt)*) => {
            json!(@array [$($elements,)* RuntimeJson::Bool(false)] $($rest)*)
        };
        (@array [$($elements:expr,)*] [$($array:tt)*] $($rest:tt)*) => {
            json!(@array [$($elements,)* json!([$($array)*])] $($rest)*)
        };
        (@array [$($elements:expr,)*] {$($object:tt)*} $($rest:tt)*) => {
            json!(@array [$($elements,)* json!({$($object)*})] $($rest)*)
        };
        (@array [$($elements:expr,)*] $next:expr, $($rest:tt)*) => {
            json!(@array [$($elements,)* json!($next),] $($rest)*)
        };
        (@array [$($elements:expr,)*] $last:expr) => {
            json!(@array [$($elements,)* json!($last)])
        };
        (@array [$($elements:expr),*] , $($rest:tt)*) => {
            json!(@array [$($elements,)*] $($rest)*)
        };

        (@object $object:ident) => {};
        (@object $object:ident ,) => {};
        (@object $object:ident $key:literal : null, $($rest:tt)*) => {{
            $object.insert($key, RuntimeJson::Null);
            json!(@object $object $($rest)*);
        }};
        (@object $object:ident $key:literal : null) => {{
            $object.insert($key, RuntimeJson::Null);
        }};
        (@object $object:ident $key:literal : true, $($rest:tt)*) => {{
            $object.insert($key, RuntimeJson::Bool(true));
            json!(@object $object $($rest)*);
        }};
        (@object $object:ident $key:literal : true) => {{
            $object.insert($key, RuntimeJson::Bool(true));
        }};
        (@object $object:ident $key:literal : false, $($rest:tt)*) => {{
            $object.insert($key, RuntimeJson::Bool(false));
            json!(@object $object $($rest)*);
        }};
        (@object $object:ident $key:literal : false) => {{
            $object.insert($key, RuntimeJson::Bool(false));
        }};
        (@object $object:ident $key:literal : [$($array:tt)*], $($rest:tt)*) => {{
            $object.insert($key, json!([$($array)*]));
            json!(@object $object $($rest)*);
        }};
        (@object $object:ident $key:literal : [$($array:tt)*]) => {{
            $object.insert($key, json!([$($array)*]));
        }};
        (@object $object:ident $key:literal : {$($nested:tt)*}, $($rest:tt)*) => {{
            $object.insert($key, json!({$($nested)*}));
            json!(@object $object $($rest)*);
        }};
        (@object $object:ident $key:literal : {$($nested:tt)*}) => {{
            $object.insert($key, json!({$($nested)*}));
        }};
        (@object $object:ident $key:literal : $value:expr, $($rest:tt)*) => {{
            $object.insert($key, json!($value));
            json!(@object $object $($rest)*);
        }};
        (@object $object:ident $key:literal : $value:expr) => {{
            $object.insert($key, json!($value));
        }};

        (null) => { RuntimeJson::Null };
        (true) => { RuntimeJson::Bool(true) };
        (false) => { RuntimeJson::Bool(false) };
        ([]) => { RuntimeJson::Array(Vec::new()) };
        ([$($tokens:tt)+]) => {
            RuntimeJson::Array(json!(@array [] $($tokens)+))
        };
        ({}) => { RuntimeJson::Object(RuntimeObject::new()) };
        ({$($tokens:tt)+}) => {{
            let mut object = RuntimeObject::new();
            json!(@object object $($tokens)+);
            RuntimeJson::Object(object)
        }};
        ($value:expr) => {
            crate::runtime_values::runtime_codec::encode_value(&$value)
                .expect("test exact JSON expression must encode")
        };
    }

    fn text_of(res: &ToolResult) -> RuntimeString {
        match &res.serializable.model_content[0] {
            crate::schemas::content::OutputBlock::Text(t) => t.text.clone(),
            _ => panic!("expected a text block"),
        }
    }

    struct ResumeRegistry {
        reject: bool,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl crate::types::tool::BackgroundTaskRegistry for ResumeRegistry {
        fn get(&self, _: &RuntimeString) -> Option<crate::types::tool::BackgroundTaskHandle> {
            None
        }
        fn list(&self) -> Vec<crate::types::tool::BackgroundTaskHandle> {
            Vec::new()
        }
        fn stop(&self, _: &RuntimeString) -> bool {
            false
        }
        async fn wait_for_idle(&self) {}
        async fn send_message(
            &self,
            request: crate::types::tool::SubagentMessageRequest,
        ) -> Option<Result<crate::types::tool::SubagentMessageResult, RuntimeString>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(request.target, "existing");
            assert_eq!(request.message, "Continue");
            assert!(!request.user_initiated);
            if self.reject {
                Some(Err("The user cancelled this agent.".into()))
            } else {
                Some(Ok(crate::types::tool::SubagentMessageResult {
                    agent_id: "existing".into(),
                    resumed: true,
                }))
            }
        }
    }

    #[tokio::test]
    async fn legacy_resume_awaits_existing_control_and_does_not_spawn_a_replacement() {
        let mut ctx = make_ctx("/tmp");
        ctx.spawn_sub_agent = Some(Arc::new(|_| panic!("resume must not spawn")));
        let tool = create_task_tool(TaskToolOptions::default());
        for reject in [false, true] {
            let registry = Arc::new(ResumeRegistry {
                reject,
                calls: AtomicUsize::new(0),
            });
            ctx.background_tasks = Some(registry.clone());
            let result = tool
                .call(
                    tool.parse_input(
                        &json!({"description":"Follow up","prompt":"Continue","resume":"existing"}),
                    )
                    .unwrap(),
                    &ctx,
                    None,
                )
                .await
                .unwrap();
            assert_eq!(registry.calls.load(Ordering::SeqCst), 1);
            assert_eq!(result.serializable.is_error, Some(reject));
            if reject {
                assert_eq!(text_of(&result), "The user cancelled this agent.");
            } else {
                assert_eq!(
                    result.serializable.output.get("agentId"),
                    Some(&Json::String("existing".into()))
                );
            }
        }
    }

    #[tokio::test]
    async fn legacy_resume_cannot_replace_creation_authority_or_claim_a_nonstring_identity() {
        let mut ctx = make_ctx("/tmp");
        let registry = Arc::new(ResumeRegistry {
            reject: false,
            calls: AtomicUsize::new(0),
        });
        ctx.background_tasks = Some(registry.clone());
        let tool = create_task_tool(TaskToolOptions::default());
        for value in [
            json!({"description":"Follow up","prompt":"Continue","resume":"existing","model":"other"}),
            json!({"description":"Follow up","prompt":"Continue","resume":"existing","permission_mode":"bypassPermissions"}),
            json!({"description":"Follow up","prompt":"Continue","resume":"existing","fork_context":false}),
            json!({"description":"Follow up","prompt":"Continue","resume":true}),
            json!({"description":"Follow up","prompt":"Continue","resume":[]}),
        ] {
            assert!(tool.parse_input(&value).is_err());
            assert_eq!(
                tool.call(value, &ctx, None)
                    .await
                    .unwrap()
                    .serializable
                    .is_error,
                Some(true)
            );
        }
        assert_eq!(registry.calls.load(Ordering::SeqCst), 0);
    }

    #[derive(Default)]
    struct ResumeSpawnCounters {
        foreground: AtomicUsize,
        background: AtomicUsize,
    }

    fn resume_context(
        registry: Arc<ResumeRegistry>,
        spawns: Arc<ResumeSpawnCounters>,
    ) -> ToolContext {
        let mut ctx = make_ctx("/tmp");
        ctx.sandbox_mode = Some(crate::types::permission::SandboxMode::DangerFullAccess);
        ctx.approval_policy = Some(crate::types::permission::ApprovalPolicy::Never);
        ctx.background_tasks = Some(registry);
        ctx.spawn_sub_agent = Some({
            let spawns = spawns.clone();
            Arc::new(move |_| {
                spawns.foreground.fetch_add(1, Ordering::SeqCst);
                panic!("resume must not spawn a foreground replacement")
            })
        });
        ctx.spawn_background_agent = Some(Arc::new(move |_| {
            spawns.background.fetch_add(1, Ordering::SeqCst);
            panic!("resume must not spawn a background replacement")
        }));
        ctx
    }

    fn assert_no_resume_side_effects(registry: &ResumeRegistry, spawns: &ResumeSpawnCounters) {
        assert_eq!(registry.calls.load(Ordering::SeqCst), 0);
        assert_eq!(spawns.foreground.load(Ordering::SeqCst), 0);
        assert_eq!(spawns.background.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn resume_parser_and_dispatcher_reject_creation_overrides_before_stripping() {
        let registry = Arc::new(ResumeRegistry {
            reject: false,
            calls: AtomicUsize::new(0),
        });
        let spawns = Arc::new(ResumeSpawnCounters::default());
        let factory: crate::engine::query::ToolContextFactory = {
            let registry = registry.clone();
            let spawns = spawns.clone();
            Arc::new(move || resume_context(registry.clone(), spawns.clone()))
        };
        let tool = create_task_tool(TaskToolOptions::default());
        for key in RESUME_CREATION_FIELDS {
            for value in [json!(false), json!(""), json!([])] {
                let mut input = json!({
                    "description": "Follow up", "prompt": "Continue", "resume": "existing"
                });
                input.as_object_mut().unwrap().insert(*key, value);
                assert_eq!(
                    tool.parse_input(&input).unwrap_err().message,
                    RESUME_OVERRIDE_ERROR,
                    "supplied nonnull field {key} must be rejected before normalization",
                );
            }
            let mut input = json!({
                "description": "Follow up", "prompt": "Continue", "resume": "existing"
            });
            input.as_object_mut().unwrap().insert(*key, json!(false));
            let result =
                dispatch_native_message_tool(tool.clone(), "task", input, factory.clone(), None)
                    .await;
            assert!(result.is_error, "dispatcher accepted {key}");
            assert!(result.content.iter().any(|block| matches!(block,
                crate::schemas::content::OutputBlock::Text(text)
                    if text.text.contains(RESUME_OVERRIDE_ERROR)
            )));
            assert_no_resume_side_effects(&registry, &spawns);
        }
        let input = tool
            .parse_input(&json!({
                "description": "Follow up", "prompt": "Continue", "resume": "existing",
                "permission_mode": null, "cwd": null
            }))
            .unwrap();
        let result = tool.call(input, &factory(), None).await.unwrap();
        assert_eq!(result.serializable.is_error, Some(false));
        assert_eq!(registry.calls.load(Ordering::SeqCst), 1);
        assert_eq!(spawns.foreground.load(Ordering::SeqCst), 0);
        assert_eq!(spawns.background.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn resume_blank_direction_never_reaches_registry_or_spawn() {
        let registry = Arc::new(ResumeRegistry {
            reject: false,
            calls: AtomicUsize::new(0),
        });
        let spawns = Arc::new(ResumeSpawnCounters::default());
        let factory: crate::engine::query::ToolContextFactory = {
            let registry = registry.clone();
            let spawns = spawns.clone();
            Arc::new(move || resume_context(registry.clone(), spawns.clone()))
        };
        let tool = create_task_tool(TaskToolOptions::default());
        for body in ["", " \n\t", "\u{00a0}\u{3000}\u{feff}"] {
            let input = json!({
                "description": "Follow up", "prompt": body, "resume": "existing"
            });
            let parsed = tool.parse_input(&input).unwrap();
            let result = tool.call(parsed, &factory(), None).await.unwrap();
            assert_eq!(result.serializable.is_error, Some(true));
            assert_eq!(text_of(&result), "message must not be empty");
            let result =
                dispatch_native_message_tool(tool.clone(), "task", input, factory.clone(), None)
                    .await;
            assert!(result.is_error);
            assert!(result.content.iter().any(|block| matches!(block,
                crate::schemas::content::OutputBlock::Text(text)
                    if text.text == "message must not be empty"
            )));
            assert_no_resume_side_effects(&registry, &spawns);
        }
    }

    #[tokio::test]
    async fn truthy_nonstring_resume_is_rejected_without_losing_agent_type_rewrites() {
        let registry = Arc::new(ResumeRegistry {
            reject: false,
            calls: AtomicUsize::new(0),
        });
        let spawns = Arc::new(ResumeSpawnCounters::default());
        let ctx = resume_context(registry.clone(), spawns.clone());
        let tool = create_task_tool(TaskToolOptions::default());
        for value in [json!(true), json!(3), json!([]), json!({"bad": 2})] {
            let mut input = json!({"model": "low", "description":"Follow up","prompt":"Continue"});
            input.as_object_mut().unwrap().insert("resume", value);
            assert!(tool.parse_input(&input).is_err());
            let result = tool.call(input, &ctx, None).await.unwrap();
            assert_eq!(result.serializable.is_error, Some(true));
            assert_eq!(text_of(&result), "Resume requires an existing subagent ID.");
            assert_no_resume_side_effects(&registry, &spawns);
        }
    }

    #[test]
    fn metadata_and_schema_without_agents() {
        let tool = create_task_tool(TaskToolOptions::default());
        assert_eq!(tool.name(), "task");
        assert!(tool.is_concurrency_safe(&json!({})));
        assert!(!tool.is_read_only(&json!({})));
        assert!(tool
            .description()
            .starts_with("Delegate a self-contained subtask"));
        // No "Available agent types" section when there are no agents.
        assert!(!tool.description().contains("Available agent types"));
        let schema = tool.input_schema();
        assert_eq!(
            schema.get("required").unwrap(),
            &json!(["description", "prompt", "model"])
        );
        let properties = schema
            .get("properties")
            .and_then(RuntimeJson::as_object)
            .expect("task schema properties");
        assert!(!properties.contains_key("cwd"));
        assert!(!properties.contains_key("isolation"));
        // Free-string subagent_type when no roles are defined.
        let st = properties.get("subagent_type").unwrap();
        assert!(st.get("enum").is_none());
        assert!(st
            .get("type")
            .and_then(RuntimeJson::as_str)
            .is_some_and(|value| value == "string"));
    }

    #[test]
    fn schema_and_description_advertise_agent_roles() {
        let tool = create_task_tool(TaskToolOptions {
            model_catalog: false,
            model_options: Vec::new(),
            agents: vec![
                AgentDefinition {
                    name: "reviewer".into(),
                    description: "reviews code".into(),
                    ..Default::default()
                },
                AgentDefinition {
                    name: "coder".into(),
                    description: RuntimeString::default(),
                    ..Default::default()
                },
            ],
        });
        assert!(tool
            .description()
            .contains("Available agent types (pass as `agentType` or `subagent_type`):"));
        assert!(tool.description().contains("- reviewer: reviews code"));
        assert!(tool.description().contains("- coder: (no description)"));
        let schema = tool.input_schema();
        let st = schema
            .get("properties")
            .and_then(|p| p.get("subagent_type"))
            .unwrap();
        assert_eq!(st.get("enum").unwrap(), &json!(["reviewer", "coder"]));
        assert!(st
            .get("description")
            .and_then(RuntimeJson::as_str)
            .is_some_and(|value| {
                value == "Sub-agent role to delegate to. One of: reviewer, coder."
            }));
    }

    #[test]
    fn internal_and_wire_spawn_schemas_publish_raw_effort_and_full_history_selection() {
        let tool = create_task_tool(TaskToolOptions::default());
        let profile = resolve_provider_behavior_profile(
            Some(&ProviderInfo {
                name: "openai".into(),
                base_url: None,
                env_key: None,
                default_model: "gpt-5".into(),
            }),
            &RuntimeString::from("gpt-5"),
        );
        let rendered = render_tool_specs_for_behavior(
            &[ToolSpec {
                name: tool.name(),
                description: tool.description(),
                input_schema: tool.input_schema().clone(),
                kind: ToolSpecKind::Function,
                format: None,
            }],
            &profile,
        );
        for schema in [tool.input_schema(), rendered[0].input_schema.clone()] {
            assert_eq!(schema.get("additionalProperties"), Some(&json!(false)));
            let properties = schema.get("properties").unwrap();
            assert!(properties.get("fork_context").is_none());
            assert_eq!(
                properties.get("fork_turns").unwrap().get("type"),
                Some(&json!("string"))
            );
            assert_eq!(
                properties.get("reasoning_effort").unwrap().get("type"),
                Some(&json!("string"))
            );
            assert!(properties
                .get("reasoning_effort")
                .unwrap()
                .get("enum")
                .is_none());
        }
        let normalized = normalize_tool_input_for_behavior(
            &"spawn_agent".into(),
            json!({"model": "low", "task_name":"worker", "message":"inspect", "fork_turns":"3", "reasoning_effort":"custom/exact"}),
            &profile,
        );
        assert_eq!(normalized.get("fork_turns"), Some(&json!("3")));
        assert_eq!(
            normalized.get("reasoning_effort"),
            Some(&json!("custom/exact"))
        );
    }

    #[test]
    fn fork_turns_defaults_to_full_history_and_accepts_legacy_counts_without_truncation() {
        assert_eq!(task_fork_turns(None).unwrap(), ForkContext::All);
        for value in ["all", " ALL ", "3", ""] {
            assert_eq!(
                task_fork_turns(Some(&json!(value))).unwrap(),
                ForkContext::All
            );
        }
        assert_eq!(
            task_fork_turns(Some(&json!("none"))).unwrap(),
            ForkContext::None
        );
        for value in ["0", "-1", "1.5", "unknown"] {
            assert!(task_fork_turns(Some(&json!(value))).is_err());
        }
    }

    #[test]
    fn task_parser_strips_unknowns_canonicalizes_numbers_and_aggregates_fixed_ts_issues() {
        let tool = create_task_tool(TaskToolOptions {
            model_catalog: false,
            model_options: Vec::new(),
            agents: vec![
                AgentDefinition {
                    name: "reviewer".into(),
                    ..Default::default()
                },
                AgentDefinition {
                    name: "coder".into(),
                    ..Default::default()
                },
            ],
        });
        assert_eq!(
            tool.parse_input(&json!({"model": "low",
                "description": "delegate",
                "prompt": "review",
                "agentType": "reviewer",
                "fork_turns": "3",
                "extra": "strip me"
            }))
            .unwrap(),
            json!({
                "description": "delegate",
                "prompt": "review",
                "agentType": "reviewer",
                "model": "low",
                "fork_turns": "3"
            })
        );

        let error = tool
            .parse_input(&json!({
                "description": 7,
                "prompt": null,
                "subagent_type": "other",
                "agentType": 4,
                "model": false,
                "run_in_background": "x",
                "resume": 3,
                "fork_context": 0,
                "peer": true,
                "persistent": "yes",
                "isolation": "remote",
                "cwd": 9
            }))
            .unwrap_err();
        assert_eq!(
            error.message,
            concat!(
                "Invalid arguments: ",
                "description: Invalid input: expected string, received number; ",
                "prompt: Invalid input: expected string, received null; ",
                "subagent_type: Invalid option: expected one of \"reviewer\"|\"coder\"; ",
                "agentType: Invalid option: expected one of \"reviewer\"|\"coder\"; ",
                "model: Invalid input: expected string, received boolean; ",
                "run_in_background: Invalid input: expected boolean, received string; ",
                "resume: Invalid input: expected string, received number; ",
                "fork_context is not supported; use fork_turns instead; ",
                "peer: Invalid input: expected string, received boolean; ",
                "persistent: Invalid input: expected boolean, received string"
            )
        );
    }

    /// A context that can do either, with a hit counter on each side.
    fn background_probe_ctx() -> (ToolContext, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let foreground_hits = Arc::new(AtomicUsize::new(0));
        let background_hits = Arc::new(AtomicUsize::new(0));
        let mut ctx = make_ctx("/tmp");
        ctx.spawn_sub_agent = Some({
            let hits = foreground_hits.clone();
            Arc::new(move |_opts| {
                hits.fetch_add(1, Ordering::SeqCst);
                Box::pin(std::future::ready(Ok(ForegroundSubAgentRun {
                    agent_id: "foreground".into(),
                    events: Box::pin(futures::stream::empty()),
                })))
            })
        });
        ctx.spawn_background_agent = Some({
            let hits = background_hits.clone();
            Arc::new(move |_opts| {
                hits.fetch_add(1, Ordering::SeqCst);
                Box::pin(std::future::ready(Ok(SpawnBackgroundAgentResult {
                    agent_id: "background".into(),
                    output_file: "/tmp/background.out".into(),
                })))
            })
        });
        (ctx, foreground_hits, background_hits)
    }

    /// A delegation that says nothing about background DETACHES. The old default
    /// was synchronous, and a boolean defaulting to false never gets turned on —
    /// the model reads the default as the recommendation — so the background path
    /// went unused while every delegation held the parent's turn open for its
    /// whole run.
    #[tokio::test]
    async fn a_delegation_that_says_nothing_about_background_is_detached() {
        let tool = create_task_tool(TaskToolOptions::default());
        let (ctx, foreground_hits, background_hits) = background_probe_ctx();

        let result = tool
            .call(
                json!({"model": "low",  "description": "d", "prompt": "p" }),
                &ctx,
                None,
            )
            .await
            .unwrap();

        assert_eq!(background_hits.load(Ordering::SeqCst), 1);
        assert_eq!(foreground_hits.load(Ordering::SeqCst), 0);
        assert_ne!(result.serializable.is_error, Some(true));
    }

    #[tokio::test]
    async fn an_explicit_false_runs_the_sub_agent_synchronously() {
        let tool = create_task_tool(TaskToolOptions::default());
        let (ctx, foreground_hits, background_hits) = background_probe_ctx();

        tool.call(
            json!({"model": "low",  "description": "d", "prompt": "p", "run_in_background": false }),
            &ctx,
            None,
        )
        .await
        .unwrap();

        assert_eq!(foreground_hits.load(Ordering::SeqCst), 1);
        assert_eq!(background_hits.load(Ordering::SeqCst), 0);
    }

    /// Their lifecycle is not the caller's to choose: a persistent teammate and an
    /// agent declared `background: true` stay detached even against an opt-out.
    #[tokio::test]
    async fn persistent_and_declared_background_agents_ignore_an_opt_out() {
        let tool = create_task_tool(TaskToolOptions {
            model_catalog: false,
            model_options: Vec::new(),
            agents: vec![AgentDefinition {
                name: "watcher".into(),
                description: "always detached".into(),
                isolation: None,
                background: Some(true),
            }],
        });

        let (ctx, foreground_hits, background_hits) = background_probe_ctx();
        tool.call(
            json!({"model": "low",
                "description": "d",
                "prompt": "p",
                "peer": "mate",
                "persistent": true,
                "run_in_background": false
            }),
            &ctx,
            None,
        )
        .await
        .unwrap();
        assert_eq!(background_hits.load(Ordering::SeqCst), 1);
        assert_eq!(foreground_hits.load(Ordering::SeqCst), 0);

        let (ctx, foreground_hits, background_hits) = background_probe_ctx();
        tool.call(
            json!({"model": "low",
                "description": "d",
                "prompt": "p",
                "subagent_type": "watcher",
                "run_in_background": false
            }),
            &ctx,
            None,
        )
        .await
        .unwrap();
        assert_eq!(background_hits.load(Ordering::SeqCst), 1);
        assert_eq!(foreground_hits.load(Ordering::SeqCst), 0);
    }

    /// Detachment is impossible here. Defaulting to it falls back quietly, but a
    /// caller who asked for it in so many words is told, instead of getting a
    /// synchronous run dressed up as the one it requested.
    #[tokio::test]
    async fn an_explicit_request_is_refused_when_the_context_cannot_detach() {
        let tool = create_task_tool(TaskToolOptions::default());
        let foreground_hits = Arc::new(AtomicUsize::new(0));
        let mut ctx = make_ctx("/tmp");
        ctx.spawn_sub_agent = Some({
            let hits = foreground_hits.clone();
            Arc::new(move |_opts| {
                hits.fetch_add(1, Ordering::SeqCst);
                Box::pin(std::future::ready(Ok(ForegroundSubAgentRun {
                    agent_id: "foreground".into(),
                    events: Box::pin(futures::stream::empty()),
                })))
            })
        });
        // No spawn_background_agent at all.

        let refused = tool
            .call(
                json!({"model": "low",  "description": "d", "prompt": "p", "run_in_background": true }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(refused.serializable.is_error, Some(true));
        assert_eq!(foreground_hits.load(Ordering::SeqCst), 0);

        // The same context still serves a defaulted delegation synchronously.
        tool.call(
            json!({"model": "low",  "description": "d", "prompt": "p" }),
            &ctx,
            None,
        )
        .await
        .unwrap();
        assert_eq!(foreground_hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_background_only_context_does_not_require_a_foreground_spawner() {
        let tool = create_task_tool(TaskToolOptions::default());
        let (mut ctx, foreground_hits, background_hits) = background_probe_ctx();
        ctx.spawn_sub_agent = None;
        let result = tool
            .call(
                json!({"model": "low", "description":"d", "prompt":"p"}),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(result.serializable.is_error, Some(false));
        assert_eq!(background_hits.load(Ordering::SeqCst), 1);
        assert_eq!(foreground_hits.load(Ordering::SeqCst), 0);
        let result = tool
            .call(
                json!({"model": "low", "description":"d", "prompt":"p", "run_in_background":false}),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(result.serializable.is_error, Some(true));
        assert_eq!(background_hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_required_background_lifecycle_is_never_downgraded_to_foreground() {
        let tool = create_task_tool(TaskToolOptions {
            model_catalog: false,
            model_options: Vec::new(),
            agents: vec![AgentDefinition {
                name: "watcher".into(),
                background: Some(true),
                ..Default::default()
            }],
        });
        let (mut ctx, foreground_hits, _) = background_probe_ctx();
        ctx.spawn_background_agent = None;
        for input in [
            json!({"model": "low", "description":"d", "prompt":"p", "subagent_type":"watcher"}),
            json!({"model": "low", "description":"d", "prompt":"p", "peer":"worker", "persistent":true}),
        ] {
            let result = tool.call(input, &ctx, None).await.unwrap();
            assert_eq!(result.serializable.is_error, Some(true));
        }
        assert_eq!(foreground_hits.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn full_history_fork_selection_reaches_foreground_and_background_callbacks() {
        let tool = create_task_tool(TaskToolOptions::default());
        let foreground = Arc::new(Mutex::new(Vec::new()));
        let background = Arc::new(Mutex::new(Vec::new()));
        let mut ctx = make_ctx("/tmp");
        ctx.spawn_sub_agent = Some({
            let captured = foreground.clone();
            Arc::new(move |opts| {
                captured.lock().unwrap().push(opts.fork_context);
                Box::pin(std::future::ready(Ok(ForegroundSubAgentRun {
                    agent_id: "foreground".into(),
                    events: Box::pin(futures::stream::empty()),
                })))
            })
        });
        ctx.spawn_background_agent = Some({
            let captured = background.clone();
            Arc::new(move |opts| {
                captured.lock().unwrap().push(opts.fork_context);
                Box::pin(std::future::ready(Ok(SpawnBackgroundAgentResult {
                    agent_id: "background".into(),
                    output_file: "/tmp/background.out".into(),
                })))
            })
        });
        let cases = [
            ("all", ForkContext::All),
            ("none", ForkContext::None),
            ("3", ForkContext::All),
        ];
        for (value, _) in &cases {
            for detached in [false, true] {
                tool.call(json!({"model": "low", "description":"worker", "prompt":"inspect", "run_in_background":detached, "fork_turns":value}), &ctx, None).await.unwrap();
            }
        }
        let expected = cases
            .into_iter()
            .map(|(_, value)| Some(value))
            .collect::<Vec<_>>();
        assert_eq!(*foreground.lock().unwrap(), expected);
        assert_eq!(*background.lock().unwrap(), expected);
        for value in [json!(true), json!(false)] {
            assert!(tool
                .call(
                    json!({"model": "low", "description":"worker", "prompt":"inspect", "fork_context":value}),
                    &ctx,
                    None
                )
                .await
                .is_err());
        }
    }

    #[tokio::test]
    async fn fresh_foreground_and_background_forward_the_same_parsed_fork_context_and_options() {
        let tool = create_task_tool(TaskToolOptions::default());

        let foreground_capture = Arc::new(Mutex::new(None::<SpawnSubAgentOptions>));
        let mut foreground_ctx = make_ctx("/tmp");
        foreground_ctx.spawn_sub_agent = Some({
            let capture = foreground_capture.clone();
            Arc::new(move |opts| {
                *capture.lock().unwrap() = Some(opts);
                Box::pin(std::future::ready(Ok(ForegroundSubAgentRun {
                    agent_id: "foreground".into(),
                    events: Box::pin(futures::stream::empty()),
                })))
            })
        });
        let foreground_input = json!({
            "description": "foreground",
            "prompt": "foreground prompt",
            "subagent_type": "reviewer",
            "model": "high",
            "peer": "foreground-peer",
            "fork_turns": "2"
        });
        tool.call(foreground_input.clone(), &foreground_ctx, None)
            .await
            .unwrap();
        {
            let foreground = foreground_capture.lock().unwrap();
            let foreground = foreground.as_ref().expect("foreground callback");
            assert_eq!(foreground.prompt, "foreground prompt");
            assert_eq!(foreground.agent_type.as_ref(), Some(&"reviewer".into()));
            assert_eq!(foreground.model.as_ref(), Some(&"high".into()));
            assert_eq!(foreground.resume, None);
            assert_eq!(foreground.fork_context, Some(ForkContext::All));
            assert_eq!(
                foreground.swarm.as_ref().map(|swarm| swarm.peer.clone()),
                Some("foreground-peer".into())
            );
            assert_eq!(foreground.cwd, None);
            assert_eq!(foreground.isolation, None);
            assert_eq!(foreground.persistent, None);
            assert_eq!(foreground.task_agent_type_raw, None);
            assert_eq!(foreground.task_resume_raw, None);
        }

        let background_capture = Arc::new(Mutex::new(None::<SpawnSubAgentOptions>));
        let mut background_ctx = make_ctx("/tmp");
        background_ctx.spawn_sub_agent = Some(Arc::new(|_opts| {
            Box::pin(std::future::ready(Ok(ForegroundSubAgentRun {
                agent_id: "unused".into(),
                events: Box::pin(futures::stream::empty()),
            })))
        }));
        background_ctx.spawn_background_agent = Some({
            let capture = background_capture.clone();
            Arc::new(move |opts| {
                *capture.lock().unwrap() = Some(opts);
                Box::pin(std::future::ready(Ok(SpawnBackgroundAgentResult {
                    agent_id: "background".into(),
                    output_file: "/tmp/background.out".into(),
                })))
            })
        });
        let background_input = json!({
            "description": "background",
            "prompt": "background prompt",
            "subagent_type": "worker",
            "model": "medium",
            "peer": "background-peer",
            "persistent": true,
            "fork_turns": "all"
        });
        tool.call(background_input.clone(), &background_ctx, None)
            .await
            .unwrap();
        let background = background_capture.lock().unwrap();
        let background = background.as_ref().expect("background callback");
        assert_eq!(background.prompt, "background prompt");
        assert_eq!(background.agent_type.as_ref(), Some(&"worker".into()));
        assert_eq!(background.model.as_ref(), Some(&"medium".into()));
        assert_eq!(background.resume, None);
        assert_eq!(background.fork_context, Some(ForkContext::All));
        assert_eq!(
            background.swarm.as_ref().map(|swarm| swarm.peer.clone()),
            Some("background-peer".into())
        );
        assert_eq!(background.cwd, None);
        assert_eq!(background.isolation, None);
        assert_eq!(background.persistent, Some(true));
        assert_eq!(background.task_agent_type_raw, None);
        assert_eq!(background.task_resume_raw, None);
    }

    #[tokio::test]
    async fn trusted_field_matrix_preserves_js_nullish_truthiness() {
        let tool = create_task_tool(TaskToolOptions {
            model_catalog: false,
            model_options: Vec::new(),
            agents: vec![
                AgentDefinition {
                    name: "alias".into(),
                    isolation: Some(Isolation::Worktree),
                    background: Some(true),
                    ..Default::default()
                },
                AgentDefinition {
                    name: "isolated".into(),
                    isolation: Some(Isolation::Worktree),
                    ..Default::default()
                },
            ],
        });
        let captured = Arc::new(Mutex::new(Vec::<SpawnSubAgentOptions>::new()));
        let mut ctx = make_ctx("/tmp");
        ctx.spawn_sub_agent = Some({
            let captured = captured.clone();
            Arc::new(move |opts| {
                captured.lock().unwrap().push(opts);
                Box::pin(std::future::ready(Ok(ForegroundSubAgentRun {
                    agent_id: "foreground".into(),
                    events: Box::pin(futures::stream::iter(vec![EngineEvent::TurnCompleted {
                        turn_id: "foreground-turn".into(),
                        usage: None,
                        stop_reason: crate::types::errors::TerminalReason::Completed,
                    }])),
                })))
            })
        });

        let primary_empty = json!({
            "description": "trusted rewrite",
            "prompt": "empty primary blocks alias",
            "subagent_type": "",
            "agentType": "alias",
            "model": "low",
            "resume": "",
            "peer": "",
            "name": "ignored name",
            "team_name": "ignored team",
            "mode": "ignored mode"
        });
        tool.call(primary_empty.clone(), &ctx, None).await.unwrap();

        let primary_false = json!({
            "description": "trusted rewrite",
            "prompt": "false primary blocks alias",
            "subagent_type": false,
            "agentType": "alias",
            "model": "low",
            "resume": "",
            "peer": ""
        });
        tool.call(primary_false.clone(), &ctx, None).await.unwrap();

        let primary_null = json!({"model": "low",
            "description": "trusted rewrite",
            "prompt": "nullish primary uses alias",
            "subagent_type": null,
            "agentType": "alias"
        });
        let background_capture = Arc::new(Mutex::new(None::<SpawnSubAgentOptions>));
        ctx.spawn_background_agent = Some({
            let captured = background_capture.clone();
            Arc::new(move |opts| {
                *captured.lock().unwrap() = Some(opts);
                Box::pin(std::future::ready(Ok(SpawnBackgroundAgentResult {
                    agent_id: "background-alias".into(),
                    output_file: "/tmp/background-alias.out".into(),
                })))
            })
        });
        let background_result = tool.call(primary_null.clone(), &ctx, None).await.unwrap();
        assert_eq!(background_result.serializable.is_error, Some(false));
        assert_eq!(
            background_result.serializable.output.get("agentId"),
            Some(&Json::from("background-alias"))
        );
        ctx.spawn_background_agent = None;

        let legacy_isolation = json!({"model": "low",
            "description": "trusted rewrite",
            "prompt": "legacy isolation cannot suppress the agent default",
            "subagent_type": "isolated",
            "isolation": false
        });
        tool.call(legacy_isolation.clone(), &ctx, None)
            .await
            .unwrap();

        let missing_background_callback = json!({"model": "low",
            "description": "trusted rewrite",
            "prompt": "fall back to foreground",
            "run_in_background": "false"
        });
        tool.call(missing_background_callback.clone(), &ctx, None)
            .await
            .unwrap();

        let captured = captured.lock().unwrap();
        assert_eq!(captured.len(), 4);
        for actual in [&captured[0], &captured[1]] {
            assert_eq!(
                actual.agent_type, None,
                "nonnull falsy primary blocks alias"
            );
            assert_eq!(actual.model, Some("low".into()));
            assert_eq!(actual.resume, None, "empty resume is omitted");
            assert_eq!(actual.swarm, None, "empty peer is omitted");
            assert_eq!(actual.isolation, None, "blocked alias has no default");
            assert_eq!(actual.task_agent_type_raw, None);
            assert_eq!(actual.task_resume_raw, None);
        }
        let background = background_capture.lock().unwrap();
        let background = background
            .as_ref()
            .expect("required background alias callback");
        assert_eq!(background.prompt, "nullish primary uses alias");
        assert_eq!(background.agent_type.as_ref(), Some(&"alias".into()));
        assert_eq!(background.isolation, Some(Isolation::Worktree));
        assert_eq!(background.task_agent_type_raw, None);
        assert_eq!(background.task_resume_raw, None);
        assert_eq!(captured[2].agent_type.as_ref(), Some(&"isolated".into()));
        assert_eq!(
            captured[2].isolation,
            Some(Isolation::Worktree),
            "legacy task input cannot suppress the agent definition"
        );
        assert_eq!(captured[2].task_agent_type_raw, None);
        assert_eq!(captured[2].task_resume_raw, None);
        assert_eq!(captured[3].prompt, "fall back to foreground");
        assert_eq!(captured[3].task_agent_type_raw, None);
        assert_eq!(captured[3].task_resume_raw, None);
    }

    #[tokio::test]
    async fn trusted_non_string_agent_type_uses_raw_slots_in_both_spawn_paths() {
        let tool = create_task_tool(TaskToolOptions::default());
        let foreground = Arc::new(Mutex::new(None::<SpawnSubAgentOptions>));
        let mut foreground_ctx = make_ctx("/tmp");
        foreground_ctx.spawn_sub_agent = Some({
            let foreground = foreground.clone();
            Arc::new(move |opts| {
                *foreground.lock().unwrap() = Some(opts);
                Box::pin(std::future::ready(Ok(ForegroundSubAgentRun {
                    agent_id: "foreground".into(),
                    events: Box::pin(futures::stream::empty()),
                })))
            })
        });
        tool.call(
            json!({"model": "low",
                "description": "trusted rewrite",
                "prompt": "foreground",
                "subagent_type": { "bad": 1 },
                "agentType": "blocked alias"
            }),
            &foreground_ctx,
            None,
        )
        .await
        .unwrap();

        {
            let foreground = foreground.lock().unwrap();
            let foreground = foreground.as_ref().expect("foreground callback");
            assert_eq!(foreground.agent_type, None);
            assert_eq!(foreground.resume, None);
            assert_eq!(foreground.task_agent_type_raw, Some(json!({ "bad": 1 })));
            assert_eq!(foreground.task_resume_raw, None);
        }

        let background = Arc::new(Mutex::new(None::<SpawnSubAgentOptions>));
        let mut background_ctx = make_ctx("/tmp");
        background_ctx.spawn_sub_agent = Some(Arc::new(|_opts| {
            Box::pin(std::future::ready(Ok(ForegroundSubAgentRun {
                agent_id: "unused".into(),
                events: Box::pin(futures::stream::empty()),
            })))
        }));
        background_ctx.spawn_background_agent = Some({
            let background = background.clone();
            Arc::new(move |opts| {
                *background.lock().unwrap() = Some(opts);
                Box::pin(std::future::ready(Ok(SpawnBackgroundAgentResult {
                    agent_id: "background".into(),
                    output_file: "/tmp/background.out".into(),
                })))
            })
        });
        tool.call(
            json!({"model": "low",
                "description": "trusted rewrite",
                "prompt": "background",
                "subagent_type": true,
                "run_in_background": true
            }),
            &background_ctx,
            None,
        )
        .await
        .unwrap();

        let background = background.lock().unwrap();
        let background = background.as_ref().expect("background callback");
        assert_eq!(background.agent_type, None);
        assert_eq!(background.resume, None);
        assert_eq!(background.task_agent_type_raw, Some(json!(true)));
        assert_eq!(background.task_resume_raw, None);
    }

    #[tokio::test]
    async fn trusted_agent_type_with_own_to_string_throws_before_foreground_spawn() {
        let tool = create_task_tool(TaskToolOptions {
            model_catalog: false,
            model_options: Vec::new(),
            agents: vec![AgentDefinition {
                name: "reviewer".into(),
                ..Default::default()
            }],
        });
        let spawn_count = Arc::new(AtomicUsize::new(0));
        let mut ctx = make_ctx("/tmp");
        ctx.spawn_sub_agent = Some({
            let spawn_count = Arc::clone(&spawn_count);
            Arc::new(move |_opts| {
                spawn_count.fetch_add(1, Ordering::SeqCst);
                Box::pin(std::future::ready(Ok(ForegroundSubAgentRun {
                    agent_id: "unexpected".into(),
                    events: Box::pin(futures::stream::empty()),
                })))
            })
        });

        for agent_type in [json!({ "toString": null }), json!([{ "toString": 0 }])] {
            let error = match tool
                .call(
                    json!({"model": "low",
                        "description": "trusted rewrite",
                        "prompt": "must throw",
                        "agentType": agent_type
                    }),
                    &ctx,
                    None,
                )
                .await
            {
                Ok(_) => panic!("own toString unexpectedly passed JS ToPrimitive conversion"),
                Err(error) => error,
            };
            assert_eq!(error.message, "Cannot convert object to primitive value");
        }
        assert_eq!(spawn_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn empty_foreground_failure_remains_an_error_with_a_diagnostic() {
        let tool = create_task_tool(TaskToolOptions::default());
        let mut ctx = make_ctx("/tmp");
        ctx.spawn_sub_agent = Some(Arc::new(|_opts| {
            Box::pin(std::future::ready(Ok(ForegroundSubAgentRun {
                agent_id: "foreground".into(),
                events: Box::pin(futures::stream::iter(vec![EngineEvent::TurnFailed {
                    inference: None,
                    error: EngineError {
                        code: EngineErrorCode::Internal,
                        message: RuntimeString::default(),
                        cause: None,
                        request_id: None,
                        provider_kind: None,
                        retry_after_ms: None,
                    },
                }])),
            })))
        }));

        let result = tool
            .call(
                json!({"model": "low",  "description": "d", "prompt": "p" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(result.serializable.is_error, Some(true));
        assert_eq!(
            result.serializable.output.get("ok"),
            Some(&Json::Bool(false))
        );
        assert_eq!(
            text_of(&result),
            "Sub-agent failed: Sub-agent failed without an error message."
        );
    }

    #[tokio::test]
    async fn reports_when_sub_agents_are_unavailable() {
        let tool = create_task_tool(TaskToolOptions::default());
        let ctx = make_ctx("/tmp");
        let res = tool
            .call(
                json!({"model": "low",  "description": "d", "prompt": "p" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            text_of(&res),
            "Sub-agents are not available in this context."
        );
    }

    #[tokio::test]
    async fn rejects_a_persistent_teammate_without_a_peer() {
        let tool = create_task_tool(TaskToolOptions::default());
        let mut ctx = make_ctx("/tmp");
        // A spawn hook must be present to get past the availability guard.
        ctx.spawn_sub_agent = Some(Arc::new(|_opts| {
            Box::pin(std::future::ready(Ok(
                crate::types::tool::ForegroundSubAgentRun {
                    agent_id: "x".into(),
                    events: Box::pin(futures::stream::iter(Vec::new())),
                },
            )))
        }));
        let nonfinite_number = runtime_json::parse(&RuntimeString::from("1e400"))
            .expect("overflowed JSON number follows JavaScript binary64 semantics");
        for persistent in [json!(true), json!("false"), nonfinite_number] {
            let res = tool
                .call(
                    json!({"model": "low",  "description": "d", "prompt": "p", "persistent": persistent }),
                    &ctx,
                    None,
                )
                .await
                .unwrap();
            assert_eq!(res.serializable.is_error, Some(true));
            assert_eq!(
                text_of(&res),
                "A persistent teammate requires a `peer` name so teammates can message it."
            );
        }

        let res = tool
            .call(
                json!({"model": "low",  "description": "d", "prompt": "p", "persistent": true, "peer": "" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            text_of(&res),
            "A persistent teammate requires a `peer` name so teammates can message it."
        );
    }

    #[tokio::test]
    async fn legacy_cwd_and_isolation_fields_cannot_override_the_parent_environment() {
        let tool = create_task_tool(TaskToolOptions::default());
        let captured = Arc::new(Mutex::new(None::<SpawnSubAgentOptions>));
        let mut ctx = make_ctx("/tmp");
        ctx.spawn_sub_agent = Some({
            let captured = captured.clone();
            Arc::new(move |opts| {
                *captured.lock().unwrap() = Some(opts);
                Box::pin(std::future::ready(Ok(
                    crate::types::tool::ForegroundSubAgentRun {
                        agent_id: "x".into(),
                        events: Box::pin(futures::stream::iter(vec![EngineEvent::TurnCompleted {
                            turn_id: "foreground-turn".into(),
                            usage: None,
                            stop_reason: crate::types::errors::TerminalReason::Completed,
                        }])),
                    },
                )))
            })
        });
        let input = json!({"model": "low",
            "description": "d",
            "prompt": "p",
            "cwd": "",
            "isolation": "worktree"
        });

        let parsed = tool.parse_input(&input).unwrap();
        assert!(parsed.get("cwd").is_none());
        assert!(parsed.get("isolation").is_none());

        let result = tool.call(input, &ctx, None).await.unwrap();
        assert_ne!(result.serializable.is_error, Some(true));
        let captured = captured.lock().unwrap();
        let options = captured.as_ref().expect("foreground callback");
        assert_eq!(options.cwd, None);
        assert_eq!(options.isolation, None);
    }

    #[tokio::test]
    async fn trusted_null_input_preserves_the_fixed_ts_property_access_error() {
        let tool = create_task_tool(TaskToolOptions::default());
        let mut ctx = make_ctx("/tmp");
        ctx.spawn_sub_agent = Some(Arc::new(|_opts| {
            Box::pin(std::future::ready(Ok(
                crate::types::tool::ForegroundSubAgentRun {
                    agent_id: "x".into(),
                    events: Box::pin(futures::stream::iter(Vec::new())),
                },
            )))
        }));

        let error = match tool.call(RuntimeJson::Null, &ctx, None).await {
            Ok(_) => panic!("trusted null input unexpectedly succeeded"),
            Err(error) => error,
        };
        assert_eq!(
            error.message,
            "Cannot read properties of null (reading 'resume')"
        );
    }

    /// Plan mode admits a planning sub-agent: research is what planning is, and the
    /// plan-mode guidance asks for parallel exploration agents. This is safe because
    /// the spawned child inherits plan mode as a FLOOR (see `session_spawn.rs`), so
    /// it is itself read-only — the parent's trust is not what makes it safe.
    #[tokio::test]
    async fn check_permissions_allows_task_in_plan_mode_only() {
        let tool = create_task_tool(TaskToolOptions::default());
        let input = RuntimeJson::Null;

        let mut plan_ctx = make_ctx("/tmp");
        plan_ctx.planning_read_only = true;
        assert_eq!(
            tool.check_permissions(&input, &plan_ctx).await,
            Some(PermissionResult::Allow {
                updated_input: None,
                reason: PermissionReason::ToolSelfCheck,
            }),
            "plan mode must admit a planning sub-agent"
        );

        // Ordinary filesystem policies do not change the tool's own contract.
        for sandbox_mode in [
            crate::types::permission::SandboxMode::ReadOnly,
            crate::types::permission::SandboxMode::WorkspaceWrite,
            crate::types::permission::SandboxMode::DangerFullAccess,
        ] {
            let mut ctx = make_ctx("/tmp");
            ctx.sandbox_mode = Some(sandbox_mode);
            assert_eq!(
                tool.check_permissions(&input, &ctx).await,
                None,
                "{sandbox_mode:?} must not be short-circuited by the tool"
            );
        }
    }
}
