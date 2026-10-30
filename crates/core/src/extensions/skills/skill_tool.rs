//! A `skill` tool that RUNS a named skill. By default a skill runs INLINE: its
//! body is expanded into the current turn (the main loop executes its steps with
//! shared context), and its model/effort/allowed-tools are layered onto the rest
//! of the turn via `apply_skill_context`. A skill with `context: fork` instead
//! runs as an isolated sub-agent (its own context window + token budget), and its
//! final message is the tool result.
//!
//! `get_skills` is a thunk so the host can swap the available skill set between
//! turns (e.g. after re-discovery) without rebuilding the tool. Not read-only or
//! concurrency-safe: a skill may run mutating tools.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Map as SchemaMap, Value as SchemaValue};

use super::expand_skill::{expand_skill_body, ExpandSkillOptions};
use super::loader::{model_invocable, Skill, SkillContext};
use crate::extensions::arg_substitution::RunBashFuture;
use crate::extensions::skills::expand_skill::{SkillBashOpts, SkillBashRunner};
use crate::runtime_values::{RuntimeJson, RuntimeObject, RuntimeString};
use crate::tools::build_tool::text_result;
use crate::tools::subagent_stream::{consume_foreground_sub_agent, ForegroundSubAgentResult};
use crate::types::errors::ToolError;
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{
    RunBashFn, RunBashOptions, SkillContextMod, SpawnSubAgentOptions, Tool, ToolContext,
    ToolProgress, ToolResult,
};

/// The `skill` tool.
pub struct SkillTool {
    get_skills: Box<dyn Fn() -> Vec<Skill> + Send + Sync>,
    input_schema: JsonSchema,
}

/// Construct the `skill` tool. `get_skills` is a thunk so the host can swap the
/// available skill set between turns without rebuilding the tool.
pub fn create_skill_tool(
    get_skills: impl Fn() -> Vec<Skill> + Send + Sync + 'static,
) -> Arc<dyn Tool> {
    Arc::new(SkillTool {
        get_skills: Box::new(get_skills),
        input_schema: build_input_schema(),
    })
}

/// Build the tool's input JSON Schema: a required `name` and optional `args`.
fn build_input_schema() -> JsonSchema {
    let mut name_prop = SchemaMap::new();
    name_prop.insert("type".to_string(), json!("string"));
    name_prop.insert(
        "description".to_string(),
        json!("The name of the skill to run."),
    );
    let mut args_prop = SchemaMap::new();
    args_prop.insert("type".to_string(), json!("string"));
    args_prop.insert(
        "description".to_string(),
        json!("Optional arguments to pass to the skill."),
    );
    let mut properties = SchemaMap::new();
    properties.insert("name".to_string(), SchemaValue::Object(name_prop));
    properties.insert("args".to_string(), SchemaValue::Object(args_prop));
    let mut schema = SchemaMap::new();
    schema.insert("type".to_string(), json!("object"));
    schema.insert("properties".to_string(), SchemaValue::Object(properties));
    schema.insert("required".to_string(), json!(["name"]));
    schema.insert("additionalProperties".to_string(), json!(false));
    schema
}

fn skill_output(
    skill: Option<RuntimeString>,
    status: Option<&str>,
    text: RuntimeString,
) -> RuntimeJson {
    let mut output = RuntimeObject::new();
    output.insert(
        "skill",
        skill.map(RuntimeJson::String).unwrap_or(RuntimeJson::Null),
    );
    output.insert(
        "status",
        status.map(RuntimeJson::from).unwrap_or(RuntimeJson::Null),
    );
    output.insert("text", RuntimeJson::String(text));
    RuntimeJson::Object(output)
}

fn runtime_join(values: impl IntoIterator<Item = RuntimeString>, separator: &str) -> RuntimeString {
    let separator = RuntimeString::from(separator);
    let mut values = values.into_iter();
    let Some(mut output) = values.next() else {
        return RuntimeString::default();
    };
    for value in values {
        output = output.concat(&separator).concat(&value);
    }
    output
}

fn skill_runner_error_to_host(value: &RuntimeString) -> String {
    value
        .try_to_utf8()
        .unwrap_or_else(|error| format!("non-scalar skill runner error: {error}"))
}

fn skill_model_identifier_to_host(value: &RuntimeString) -> Result<String, ToolError> {
    value
        .try_to_utf8()
        .map_err(|error| ToolError::new(format!("invalid skill model identifier: {error}")))
}

fn skill_tool_identifier_to_host(value: &RuntimeString) -> Result<String, ToolError> {
    value
        .try_to_utf8()
        .map_err(|error| ToolError::new(format!("invalid skill tool identifier: {error}")))
}

/// Adapt the context's permission-gated shell runner down to the rich
/// [`SkillBashRunner`] the skill-body expander wires into its inline-bash: the
/// per-directive shell + the skill's own allowed-tools cross unchanged, and a
/// denial surfaces as the error string the expander propagates.
fn adapt_run_bash(run_bash: RunBashFn) -> SkillBashRunner {
    Arc::new(
        move |command: String, opts: SkillBashOpts| -> RunBashFuture {
            let run_bash = run_bash.clone();
            Box::pin(async move {
                let bash_opts = RunBashOptions {
                    shell: opts.shell,
                    allow_tools: opts.allow_tools,
                };
                run_bash(command, Some(bash_opts))
                    .await
                    .map_err(|error| skill_runner_error_to_host(&error.message))
            })
        },
    )
}

#[async_trait]
impl Tool for SkillTool {
    fn static_name(&self) -> &str {
        "skill"
    }

    fn static_description(&self) -> &str {
        "Run a named skill (inline by default, or as a sub-agent) and return its result. Only invoke a skill actually listed as available in this session (or one the user typed as /<name>); never guess or invent a skill name from memory."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn is_enabled(&self) -> bool {
        true
    }

    // Inline skills mutate the turn context (apply_skill_context), so they must
    // not run concurrently with other tools — same as any context-modifying tool.
    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        false
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        false
    }

    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        let name = input
            .get("name")
            .and_then(RuntimeJson::as_string)
            .cloned()
            .unwrap_or_default();
        Some(RuntimeString::from("Running skill ").concat(&name))
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let name = input
            .get("name")
            .and_then(RuntimeJson::as_string)
            .cloned()
            .ok_or_else(|| ToolError::new("skill input requires a string name"))?;
        let args = input
            .get("args")
            .and_then(RuntimeJson::as_string)
            .cloned()
            .unwrap_or_default();

        // A `disable-model-invocation` skill is invisible to the model — user
        // slash command only. Same predicate the listing uses, so a name the
        // model could not have been told about is also a name it cannot run.
        let skills: Vec<Skill> = model_invocable((self.get_skills)());
        let skill = match skills.iter().find(|skill| name == skill.name) {
            Some(s) => s.clone(),
            None => {
                let joined = runtime_join(skills.iter().map(|skill| skill.name.clone()), ", ");
                let available = if joined.is_empty() {
                    RuntimeString::from("(none)")
                } else {
                    joined
                };
                return Ok(text_result(
                    skill_output(None, None, RuntimeString::default()),
                    RuntimeString::from("Skill \"")
                        .concat(&name)
                        .concat(&RuntimeString::from("\" not found. Available skills: "))
                        .concat(&available),
                    true,
                ));
            }
        };

        // Expand the body through the SAME shared route as the user-slash path:
        // `$ARGUMENTS`, `${KLAUS_SKILL_DIR}`, and inline-bash. Inline-shell runs
        // through the permission-gated `ctx.run_bash`; the shared route folds in
        // THIS skill's allowed-tools for its own commands.
        let run_bash = ctx.run_bash.clone().map(adapt_run_bash);
        let prompt = expand_skill_body(
            &skill,
            &args,
            ExpandSkillOptions {
                run_bash,
                vars: None,
            },
        )
        .await
        .map_err(ToolError::new)?;

        // FORK: run the skill as an isolated sub-agent (its own context + budget).
        // The allowed-tools are auto-approved in the child; the model/effort
        // override + the skill's `agent` type run the child.
        if skill.context == Some(SkillContext::Fork) {
            let spawn = match &ctx.spawn_sub_agent {
                Some(spawn) => spawn,
                None => {
                    return Ok(text_result(
                        skill_output(None, None, RuntimeString::default()),
                        "Forked skills run as sub-agents, which are unavailable in this context."
                            .to_string(),
                        true,
                    ));
                }
            };
            if let Some(apply) = &ctx.apply_skill_context {
                apply(SkillContextMod {
                    invoked_skill_path: Some(skill.path.clone()),
                    invoked_skill_content: Some(prompt.clone()),
                    ..Default::default()
                });
            }
            let run = spawn(SpawnSubAgentOptions {
                prompt,
                agent_type: skill.agent.clone(),
                auto_allow_tools: skill.allowed_tools.clone(),
                model: skill.model.clone(),
                effort: skill.effort,
                ultra_mode: skill.ultra_mode,
                ..Default::default()
            })
            .await
            .map_err(ToolError::new)?;
            // Surface the forked skill's nested tool activity in real time.
            let ForegroundSubAgentResult {
                final_text,
                failure,
            } = consume_foreground_sub_agent(ctx, run).await;
            if let Some(failure) = failure {
                return Ok(text_result(
                    skill_output(Some(skill.name.clone()), Some("forked"), final_text),
                    RuntimeString::from("Skill failed: ").concat(&failure),
                    true,
                ));
            }
            let text = if final_text.is_empty() {
                RuntimeString::from("(skill produced no output)")
            } else {
                final_text.clone()
            };
            return Ok(text_result(
                skill_output(Some(skill.name.clone()), Some("forked"), final_text),
                text,
                false,
            ));
        }

        // INLINE (default): layer the skill's model/effort/allowed-tools onto the
        // rest of this turn, then return the body as the result so the main loop
        // reads and executes the skill's steps with full shared context.
        if let Some(apply) = &ctx.apply_skill_context {
            let model = skill
                .model
                .as_ref()
                .map(skill_model_identifier_to_host)
                .transpose()?;
            let allow_tools = skill
                .allowed_tools
                .as_deref()
                .map(|tools| {
                    tools
                        .iter()
                        .map(skill_tool_identifier_to_host)
                        .collect::<Result<Vec<_>, _>>()
                })
                .transpose()?;
            apply(SkillContextMod {
                invoked_skill_path: Some(skill.path.clone()),
                invoked_skill_content: Some(prompt.clone()),
                model,
                effort: skill.effort,
                ultra_mode: skill.ultra_mode,
                allow_tools,
            });
        }
        Ok(text_result(
            skill_output(
                Some(skill.name.clone()),
                Some("inline"),
                RuntimeString::default(),
            ),
            prompt,
            false,
        ))
    }
}

#[cfg(test)]
mod invocation_tests {
    use super::*;
    use std::sync::Mutex;

    fn skill(context: Option<SkillContext>, body: &str) -> Skill {
        Skill {
            name: "run".into(),
            path: "/skills/run/SKILL.md".into(),
            body: body.into(),
            context,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn inline_receipt_uses_native_skill_path_after_successful_expansion() {
        let mut ctx = crate::tools::builtins::test_ctx::make_ctx("/tmp");
        let receipts = Arc::new(Mutex::new(Vec::new()));
        let recorded = receipts.clone();
        ctx.apply_skill_context = Some(Box::new(move |change| {
            recorded.lock().unwrap().push(change)
        }));
        let tool = create_skill_tool(|| vec![skill(None, "run $ARGUMENTS")]);
        let input = crate::runtime_values::runtime_json::parse(
            &r#"{"name":"run","args":"/tmp/not-the-invoked-skill"}"#.into(),
        )
        .unwrap();
        assert_ne!(
            tool.call(input, &ctx, None)
                .await
                .unwrap()
                .serializable
                .is_error,
            Some(true)
        );
        assert_eq!(
            receipts.lock().unwrap()[0].invoked_skill_path.as_ref(),
            Some(&RuntimeString::from("/skills/run/SKILL.md"))
        );
    }

    #[tokio::test]
    async fn missing_skill_failed_expansion_and_unavailable_fork_have_no_receipt() {
        let mut ctx = crate::tools::builtins::test_ctx::make_ctx("/tmp");
        let receipts = Arc::new(Mutex::new(Vec::new()));
        let recorded = receipts.clone();
        ctx.apply_skill_context = Some(Box::new(move |change| {
            recorded.lock().unwrap().push(change)
        }));
        let input =
            || crate::runtime_values::runtime_json::parse(&r#"{"name":"run"}"#.into()).unwrap();
        let missing = create_skill_tool(Vec::new);
        assert!(
            missing
                .call(input(), &ctx, None)
                .await
                .unwrap()
                .serializable
                .is_error
                == Some(true)
        );
        let fork = create_skill_tool(|| vec![skill(Some(SkillContext::Fork), "body")]);
        assert!(
            fork.call(input(), &ctx, None)
                .await
                .unwrap()
                .serializable
                .is_error
                == Some(true)
        );
        ctx.run_bash = Some(Arc::new(|_, _| {
            Box::pin(async { Err(ToolError::new("denied")) })
        }));
        let failed = create_skill_tool(|| vec![skill(None, "!`echo blocked`")]);
        assert!(failed.call(input(), &ctx, None).await.is_err());
        assert!(receipts.lock().unwrap().is_empty());
    }
}
