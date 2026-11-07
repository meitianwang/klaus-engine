//! The `workflow` [`Tool`]: run a deterministic JavaScript orchestration script
//! that fans out and coordinates many sub-agents (fan-out,
//! parallel/pipeline stages, structured results).
//!
//! The tool is surfaced in two shapes, chosen by [`WorkflowToolOptions::allow_inline_script`]:
//!  - FULL mode (default): the model may author & run an inline JavaScript
//!    `script`, or run a host-registered workflow by `name`.
//!  - NAME-ONLY mode: inline scripts are rejected; only registered workflows run
//!    by `name`.
//!
//! The actual script executor is NOT part of this module (it runs a
//! model-authored program and is out of scope for this group). It is injected
//! as the [`WorkflowRunner`] seam: this tool validates input, resolves the
//! script (inline or by name), builds the runtime deps from its [`ToolContext`],
//! delegates the run, and formats the result.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use crate::runtime_values::{
    runtime_json, RuntimeJson, RuntimeNumber, RuntimeObject, RuntimeString,
};
use crate::tools::build_tool::text_result;
use crate::types::abort::AbortSignal;
use crate::types::errors::ToolError;
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{
    EmitFn, SpawnSubAgent, Tool, ToolContext, ToolProgress, ToolResult, WorkflowJournal,
};

/// A named workflow a host makes resolvable via `workflow(name)` and the `name`
/// input.
#[derive(Debug, Clone)]
pub struct WorkflowRef {
    pub name: RuntimeString,
    pub description: Option<RuntimeString>,
    /// The workflow program source.
    pub script: RuntimeString,
}

/// Host-supplied named workflows (a thunk, so they can change between turns).
pub type WorkflowsThunk = Arc<dyn Fn() -> Vec<WorkflowRef> + Send + Sync>;

/// Resolve a named/ref workflow to its script source (host-supplied).
pub type ResolveWorkflowFn = Arc<dyn Fn(&RuntimeString) -> Option<RuntimeString> + Send + Sync>;

/// Options for [`create_workflow_tool`].
pub struct WorkflowToolOptions {
    /// Host-supplied named workflows (a thunk, so they can change between turns).
    pub workflows: Option<WorkflowsThunk>,
    /// Allow running an INLINE workflow `script` the model authored on the
    /// spot (default `true`). When `false`, the tool is NAME-ONLY: it can run
    /// only the registered [`WorkflowToolOptions::workflows`] by `name`, and an
    /// inline `script` is rejected.
    pub allow_inline_script: Option<bool>,
    /// The injected program executor. Its real implementation — a deterministic
    /// workflow runtime that fans out sub-agents — is out of scope for this group.
    pub runner: Arc<dyn WorkflowRunner>,
}

/// Input to a single workflow run (what the tool hands the runtime).
#[derive(Debug, Clone)]
pub struct WorkflowRunInput {
    pub script: RuntimeString,
    pub args: Json,
    /// Resume: reuse the journal under this run id; new/changed calls run live.
    pub resume_from_run_id: Option<RuntimeString>,
    /// Output-token ceiling; `None` = unlimited.
    pub budget_tokens: Option<i64>,
}

/// Host capabilities the runtime needs, built by the tool from its
/// [`ToolContext`].
///
/// `emit` is borrowed from the live [`ToolContext`] for the duration of the run
/// (a [`ToolContext::emit`] is an owned `Box`, not clonable).
pub struct WorkflowRuntimeDeps<'a> {
    pub process_context: crate::runtime_process::RuntimeProcessContext,
    /// Spawn one workflow agent (depth-bounded, workflow-governed concurrency).
    pub spawn_workflow_agent: SpawnSubAgent,
    /// Persisted result journal for deterministic resume (optional).
    pub journal: Option<Arc<dyn WorkflowJournal>>,
    /// Structured progress sink (phase / log / agent lifecycle).
    pub emit: &'a EmitFn,
    /// Resolve a named/ref workflow to its script source (host-supplied).
    pub resolve_workflow: ResolveWorkflowFn,
    /// Abort signal for the whole workflow.
    pub signal: AbortSignal,
    /// Override the concurrency cap; `None` = the runtime default.
    pub max_concurrency: Option<usize>,
}

/// The static meta a workflow program declares. The `workflow` tool reads only
/// `name` (for its summary); the full meta lives with the workflow runtime.
#[derive(Debug, Clone)]
pub struct WorkflowMeta {
    pub name: RuntimeString,
}

/// The outcome of a completed workflow run.
#[derive(Debug, Clone)]
pub struct WorkflowRunResult {
    pub run_id: RuntimeString,
    pub result: Json,
    pub meta: WorkflowMeta,
    pub agents_spawned: u64,
    pub output_tokens_spent: u64,
}

/// The injected program executor. An `Err` carries the message surfaced to the
/// model.
#[async_trait]
pub trait WorkflowRunner: Send + Sync {
    async fn run<'a>(
        &self,
        input: WorkflowRunInput,
        deps: WorkflowRuntimeDeps<'a>,
    ) -> Result<WorkflowRunResult, RuntimeString>;
}

/// Run a deterministic flow-program orchestration script that spawns and
/// coordinates many sub-agents.
pub struct WorkflowTool {
    allow_inline_script: bool,
    workflows: Option<WorkflowsThunk>,
    runner: Arc<dyn WorkflowRunner>,
    input_schema: JsonSchema,
    description: RuntimeString,
    search_hint: String,
}

/// Construct the `workflow` tool as an erased registry-side [`Tool`].
pub fn create_workflow_tool(opts: WorkflowToolOptions) -> Arc<dyn Tool> {
    // `opts.allowInlineScript !== false`: default true; false only when set false.
    let allow_inline_script = opts.allow_inline_script != Some(false);

    // Advertise registered workflows so the model can run one by `name` without
    // knowing the script. The thunk is evaluated ONCE here (as in the source);
    // `resolve_workflow` re-evaluates it fresh at call time.
    let registered = opts
        .workflows
        .as_ref()
        .map(|thunk| thunk())
        .unwrap_or_default();

    let description = build_description(allow_inline_script, &registered);
    let search_hint = if allow_inline_script {
        "orchestrate many sub-agents from a deterministic script"
    } else {
        "run a registered multi-agent workflow by name"
    }
    .to_string();
    let input_schema = build_input_schema(allow_inline_script);

    Arc::new(WorkflowTool {
        allow_inline_script,
        workflows: opts.workflows,
        runner: opts.runner,
        input_schema,
        description,
        search_hint,
    })
}

/// Workflow orchestration follows the current request's delegation policy.
const OPT_IN_GUARDRAIL: &str = " Use only when the current collaboration mode permits delegation and a coordinated script would materially help the authorized task. Current proactive mode, or an applicable explicit user, project, skill or command instruction, supplies delegation authority; a prior Ultra turn does not. A workflow is an optional mechanism, not an additional approval gate or a requirement for parallel work. Use ordinary task calls for a small number of independent subtasks, keep dependent work in the current agent, and choose a scale justified by the requested outcome. Do not propose extra agents, review panels or approval questions merely because this tool exists. Existing user authorization and current scope constraints still apply.";

/// Build the model-facing description, listing any registered workflows.
fn concat_runtime_strings(parts: impl IntoIterator<Item = RuntimeString>) -> RuntimeString {
    parts
        .into_iter()
        .fold(RuntimeString::default(), |output, part| {
            output.concat(&part)
        })
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

fn build_description(allow_inline_script: bool, registered: &[WorkflowRef]) -> RuntimeString {
    let registered_block = if registered.is_empty() {
        RuntimeString::default()
    } else {
        let items: Vec<RuntimeString> = registered
            .iter()
            .map(|w| match &w.description {
                Some(description) if !description.is_empty() => concat_runtime_strings([
                    RuntimeString::from("- "),
                    w.name.clone(),
                    RuntimeString::from(": "),
                    description.clone(),
                ]),
                _ => RuntimeString::from("- ").concat(&w.name),
            })
            .collect();
        RuntimeString::from("\n\nRegistered workflows you can run by `name` (no script needed):\n")
            .concat(&join_runtime_strings(&items, "\n"))
    };

    if allow_inline_script {
        concat_runtime_strings([
            RuntimeString::from("Run a deterministic JS orchestration script that spawns and coordinates many sub-agents (fan-out, parallel/pipeline stages, structured results). Use for multi-agent decomposition, review panels, or broad sweeps."),
            RuntimeString::from(OPT_IN_GUARDRAIL),
            registered_block,
        ])
    } else {
        RuntimeString::from("Run one of the registered multi-agent workflows below by `name` (each fans out and coordinates sub-agents deterministically). Inline scripts are disabled in this configuration.")
            .concat(&RuntimeString::from(OPT_IN_GUARDRAIL))
            .concat(&registered_block)
    }
}

/// Build the tool's input JSON Schema (already normalized to provider-ready
/// form: object root, `additionalProperties: false`, no `$schema`).
fn build_input_schema(allow_inline_script: bool) -> JsonSchema {
    let script_desc = if allow_inline_script {
        "The workflow script (plain JavaScript beginning with `export const meta = {...}`). Provide this or `name`."
    } else {
        "Disabled in this configuration — inline scripts are not allowed. Run a registered workflow via `name` instead."
    };
    let name_desc = if allow_inline_script {
        "Name of a host-registered workflow to run instead of an inline `script`."
    } else {
        "Name of a registered workflow to run (see the list in this tool description)."
    };
    let schema = json!({
        "type": "object",
        "properties": {
            "script": { "description": script_desc, "type": "string" },
            "name": { "description": name_desc, "type": "string" },
            "args": { "description": "A JSON value passed to the script as the global `args`." },
            "resume_from_run_id": {
                "description": "Resume a prior run: unchanged agent() calls return cached results instead of re-running.",
                "type": "string"
            },
            "budget_tokens": {
                "description": "Optional output-token ceiling; agent() calls throw once it is reached.",
                "type": "integer",
                "exclusiveMinimum": 0,
                "maximum": 9_007_199_254_740_991_i64
            }
        },
        "additionalProperties": false
    });
    schema
        .as_object()
        .cloned()
        .expect("workflow input schema is an object")
}

/// Read an optional string field the way a post-validation `string | undefined`
/// arrives: present-and-string ⇒ `Some`, otherwise (absent / null / non-string)
/// ⇒ `None`.
fn opt_string(input: &Json, key: &str) -> Option<RuntimeString> {
    match input.get(key) {
        Some(RuntimeJson::String(s)) => Some(s.clone()),
        _ => None,
    }
}

/// A recoverable model error: `{ ok: false, message }`, text = message.
fn err(message: impl Into<RuntimeString>) -> ToolResult {
    let message = message.into();
    let mut out = RuntimeObject::new();
    out.insert("ok", RuntimeJson::Bool(false));
    out.insert("message", RuntimeJson::String(message.clone()));
    text_result(RuntimeJson::Object(out), message, true)
}

/// Exact JavaScript-compatible pretty rendering of one runtime JSON value.
fn stringify(value: &Json) -> RuntimeString {
    runtime_json::stringify_pretty(value)
}

#[async_trait]
impl Tool for WorkflowTool {
    fn static_name(&self) -> &str {
        "workflow"
    }

    fn description(&self) -> RuntimeString {
        self.description.clone()
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn input_schema_property_order(
        &self,
        object_path: &[String],
    ) -> Option<&'static [&'static str]> {
        object_path.is_empty().then_some(&[
            "script",
            "name",
            "args",
            "resume_from_run_id",
            "budget_tokens",
        ])
    }

    fn static_search_hint(&self) -> Option<&str> {
        Some(&self.search_hint)
    }

    fn max_result_size_chars(&self) -> Option<u64> {
        Some(1_000_000)
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

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let spawn = match &ctx.spawn_workflow_agent {
            Some(s) => s.clone(),
            None => return Ok(err("The workflow tool is not enabled for this session.")),
        };

        let input_script = opt_string(&input, "script");
        if input_script.is_some() && !self.allow_inline_script {
            return Ok(err(
                "Inline workflow scripts are disabled in this configuration. Run a registered workflow by `name` instead (see this tool's description for the available names).",
            ));
        }

        let inline_script = if self.allow_inline_script {
            input_script
        } else {
            None
        };
        let input_name = opt_string(&input, "name");

        // Resolve a registered workflow's script by name — evaluated fresh (the
        // host thunk can change between turns).
        let workflows = self.workflows.clone();
        let resolve: ResolveWorkflowFn = Arc::new(move |name: &RuntimeString| {
            workflows.as_ref().and_then(|thunk| {
                thunk()
                    .into_iter()
                    .find(|w| w.name == name.clone())
                    .map(|w| w.script)
            })
        });

        let script = match inline_script {
            Some(s) => Some(s),
            None => match input_name.as_ref() {
                Some(name) => resolve(name),
                None => None,
            },
        };
        let script = match script {
            Some(s) => s,
            None => {
                let message = if let Some(name) = input_name.as_ref() {
                    RuntimeString::from("No workflow named \"")
                        .concat(name)
                        .concat(&RuntimeString::from("\" is registered."))
                } else if self.allow_inline_script {
                    RuntimeString::from("A `script` (or a registered `name`) is required.")
                } else {
                    RuntimeString::from("A registered workflow `name` is required (inline scripts are disabled here).")
                };
                return Ok(err(message));
            }
        };

        let run_input = WorkflowRunInput {
            script,
            args: input.get("args").cloned().unwrap_or(Json::Null),
            resume_from_run_id: opt_string(&input, "resume_from_run_id"),
            budget_tokens: match input.get("budget_tokens") {
                Some(v) if !v.is_null() => v.as_i64(),
                _ => None,
            },
        };

        let deps = WorkflowRuntimeDeps {
            process_context: ctx.process_context.clone(),
            spawn_workflow_agent: spawn,
            journal: ctx.workflow_journal.clone(),
            emit: &ctx.emit,
            resolve_workflow: resolve.clone(),
            signal: ctx.signal.clone(),
            max_concurrency: None,
        };

        match self.runner.run(run_input, deps).await {
            Ok(run) => {
                let summary = concat_runtime_strings([
                    RuntimeString::from("Workflow \""),
                    run.meta.name,
                    RuntimeString::from(format!(
                        "\" finished: {} agent(s), {} output tokens (runId ",
                        run.agents_spawned, run.output_tokens_spent
                    )),
                    run.run_id.clone(),
                    RuntimeString::from(")."),
                ]);
                let text = summary
                    .concat(&RuntimeString::from("\n\nResult:\n"))
                    .concat(&stringify(&run.result));
                let mut out = RuntimeObject::new();
                out.insert("ok", RuntimeJson::Bool(true));
                out.insert("runId", RuntimeJson::String(run.run_id));
                out.insert("result", run.result);
                out.insert(
                    "agentsSpawned",
                    RuntimeJson::Number(RuntimeNumber::from(run.agents_spawned)),
                );
                out.insert(
                    "outputTokens",
                    RuntimeJson::Number(RuntimeNumber::from(run.output_tokens_spent)),
                );
                out.insert("message", RuntimeJson::String(summary));
                Ok(text_result(RuntimeJson::Object(out), text, false))
            }
            Err(message) => Ok(err(message)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::events::EngineEvent;
    use crate::types::tool::{EngineEventStream, SpawnSubAgentOptions};
    use std::sync::Mutex;

    fn rs(value: &str) -> RuntimeString {
        RuntimeString::from(value)
    }

    fn runtime(source: &str) -> RuntimeJson {
        runtime_json::parse(&rs(source)).expect("valid exact JSON fixture")
    }

    fn runtime_object<const N: usize>(entries: [(&'static str, RuntimeJson); N]) -> RuntimeJson {
        let mut object = RuntimeObject::new();
        for (name, value) in entries {
            object.insert(name, value);
        }
        RuntimeJson::Object(object)
    }

    fn script_input(script: &str) -> RuntimeJson {
        runtime_object([("script", RuntimeJson::String(rs(script)))])
    }

    fn name_input(name: &str) -> RuntimeJson {
        runtime_object([("name", RuntimeJson::String(rs(name)))])
    }

    /// A trivial workflow-agent spawner (an empty event stream).
    fn fake_spawn() -> SpawnSubAgent {
        Arc::new(|_opts: SpawnSubAgentOptions| {
            Box::pin(futures::stream::empty::<EngineEvent>()) as EngineEventStream
        })
    }

    /// A bare context (shared helper) with the workflow spawner optionally wired.
    fn make_ctx(with_spawn: bool) -> ToolContext {
        let mut ctx = crate::tools::builtins::test_ctx::make_ctx("/tmp");
        if with_spawn {
            ctx.spawn_workflow_agent = Some(fake_spawn());
        }
        ctx
    }

    /// A fake runner that records the launched script and returns a fixed
    /// outcome.
    struct FakeRunner {
        captured: Arc<Mutex<Option<RuntimeString>>>,
        outcome: Result<WorkflowRunResult, RuntimeString>,
    }

    #[async_trait]
    impl WorkflowRunner for FakeRunner {
        async fn run<'a>(
            &self,
            input: WorkflowRunInput,
            _deps: WorkflowRuntimeDeps<'a>,
        ) -> Result<WorkflowRunResult, RuntimeString> {
            *self.captured.lock().unwrap() = Some(input.script.clone());
            self.outcome.clone()
        }
    }

    fn ok_result() -> WorkflowRunResult {
        WorkflowRunResult {
            run_id: rs("wf_1"),
            result: runtime(r#"{"a":1}"#),
            meta: WorkflowMeta { name: rs("demo") },
            agents_spawned: 3,
            output_tokens_spent: 42,
        }
    }

    fn tool_with(
        allow_inline_script: Option<bool>,
        workflows: Option<Vec<WorkflowRef>>,
        outcome: Result<WorkflowRunResult, RuntimeString>,
    ) -> (Arc<dyn Tool>, Arc<Mutex<Option<RuntimeString>>>) {
        let captured = Arc::new(Mutex::new(None));
        let runner = Arc::new(FakeRunner {
            captured: captured.clone(),
            outcome,
        });
        let thunk: Option<WorkflowsThunk> = workflows.map(|ws| {
            let ws = ws.clone();
            Arc::new(move || ws.clone()) as WorkflowsThunk
        });
        let tool = create_workflow_tool(WorkflowToolOptions {
            workflows: thunk,
            allow_inline_script,
            runner,
        });
        (tool, captured)
    }

    fn text_of(res: &ToolResult) -> RuntimeString {
        match &res.serializable.model_content[0] {
            crate::schemas::content::OutputBlock::Text(t) => t.text.clone(),
            _ => panic!("expected a text block"),
        }
    }

    const FLOW_SCRIPT: &str = r#"{"meta":{"name":"demo","description":"d"},"body":[]}"#;

    #[test]
    fn metadata_full_mode() {
        let (tool, _) = tool_with(None, None, Ok(ok_result()));
        assert_eq!(tool.name(), "workflow");
        assert_eq!(tool.max_result_size_chars(), Some(1_000_000));
        assert_eq!(
            tool.search_hint(),
            Some(rs(
                "orchestrate many sub-agents from a deterministic script"
            ))
        );
        let input = runtime("{}");
        assert!(!tool.is_read_only(&input));
        assert!(!tool.is_concurrency_safe(&input));
        assert!(tool.is_enabled());
        assert_eq!(tool.is_destructive(&input), Some(false));
        assert_eq!(
            tool.description(),
            format!(
                "{}{}{}",
                "Run a deterministic JS orchestration script that spawns and coordinates many sub-agents ",
                concat!(
                    "(fan-out, parallel/pipeline stages, structured results). Use for multi-agent decomposition, ",
                    "review panels, or broad sweeps."
                ),
                OPT_IN_GUARDRAIL
            )
        );
    }

    #[test]
    fn metadata_name_only_mode() {
        let (tool, _) = tool_with(Some(false), None, Ok(ok_result()));
        assert_eq!(
            tool.search_hint(),
            Some(rs("run a registered multi-agent workflow by name"))
        );
        assert_eq!(
            tool.description(),
            format!("Run one of the registered multi-agent workflows below by `name` (each fans out and coordinates sub-agents deterministically). Inline scripts are disabled in this configuration.{OPT_IN_GUARDRAIL}")
        );
    }

    #[test]
    fn description_lists_registered_workflows() {
        let (tool, _) = tool_with(
            None,
            Some(vec![
                WorkflowRef {
                    name: rs("deep-research-strict"),
                    description: Some(rs("rigorous multi-agent research")),
                    script: rs(FLOW_SCRIPT),
                },
                WorkflowRef {
                    name: rs("bare"),
                    description: None,
                    script: rs(FLOW_SCRIPT),
                },
            ]),
            Ok(ok_result()),
        );
        assert!(tool.description().ends_with(
            "\n\nRegistered workflows you can run by `name` (no script needed):\n- deep-research-strict: rigorous multi-agent research\n- bare"
        ));
    }

    #[test]
    fn input_schema_shape() {
        let (tool, _) = tool_with(None, None, Ok(ok_result()));
        let schema = tool.input_schema();
        assert_eq!(
            schema.get("type").and_then(RuntimeJson::as_string),
            Some(&rs("object"))
        );
        assert_eq!(
            schema.get("additionalProperties"),
            Some(&RuntimeJson::Bool(false))
        );
        assert!(schema.get("required").is_none());
        let props = schema.get("properties").unwrap();
        // `args` carries only a description (z.unknown() has no `type`).
        assert_eq!(
            props.get("args").unwrap(),
            &runtime(
                r#"{"description":"A JSON value passed to the script as the global `args`."}"#
            )
        );
        assert_eq!(
            props.get("budget_tokens").unwrap(),
            &runtime(
                r#"{"description":"Optional output-token ceiling; agent() calls throw once it is reached.","type":"integer","exclusiveMinimum":0,"maximum":9007199254740991}"#,
            )
        );
        assert_eq!(
            props
                .get("script")
                .and_then(|p| p.get("description"))
                .and_then(RuntimeJson::as_string),
            Some(&rs("The workflow script (plain JavaScript beginning with `export const meta = {...}`). Provide this or `name`."))
        );
    }

    #[test]
    fn input_schema_name_only_descriptions() {
        let (tool, _) = tool_with(Some(false), None, Ok(ok_result()));
        let schema = tool.input_schema();
        let props = schema.get("properties").unwrap();
        assert_eq!(
            props
                .get("script")
                .and_then(|p| p.get("description"))
                .and_then(RuntimeJson::as_string),
            Some(&rs("Disabled in this configuration — inline scripts are not allowed. Run a registered workflow via `name` instead."))
        );
        assert_eq!(
            props
                .get("name")
                .and_then(|p| p.get("description"))
                .and_then(RuntimeJson::as_string),
            Some(&rs(
                "Name of a registered workflow to run (see the list in this tool description)."
            ))
        );
    }

    #[tokio::test]
    async fn errors_when_not_enabled() {
        let (tool, captured) = tool_with(None, None, Ok(ok_result()));
        let ctx = make_ctx(false);
        let res = tool
            .call(script_input(FLOW_SCRIPT), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            text_of(&res),
            "The workflow tool is not enabled for this session."
        );
        assert!(captured.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn rejects_inline_script_when_disabled() {
        let (tool, _) = tool_with(Some(false), None, Ok(ok_result()));
        let ctx = make_ctx(true);
        let res = tool
            .call(script_input(FLOW_SCRIPT), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            text_of(&res),
            "Inline workflow scripts are disabled in this configuration. Run a registered workflow by `name` instead (see this tool's description for the available names)."
        );
    }

    #[tokio::test]
    async fn requires_script_or_name_in_full_mode() {
        let (tool, _) = tool_with(None, None, Ok(ok_result()));
        let ctx = make_ctx(true);
        let res = tool.call(runtime("{}"), &ctx, None).await.unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            text_of(&res),
            "A `script` (or a registered `name`) is required."
        );
    }

    #[tokio::test]
    async fn requires_name_in_name_only_mode() {
        let (tool, _) = tool_with(Some(false), None, Ok(ok_result()));
        let ctx = make_ctx(true);
        let res = tool.call(runtime("{}"), &ctx, None).await.unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            text_of(&res),
            "A registered workflow `name` is required (inline scripts are disabled here)."
        );
    }

    #[tokio::test]
    async fn unknown_registered_name_errors() {
        let (tool, _) = tool_with(
            None,
            Some(vec![WorkflowRef {
                name: rs("known"),
                description: None,
                script: rs(FLOW_SCRIPT),
            }]),
            Ok(ok_result()),
        );
        let ctx = make_ctx(true);
        let res = tool.call(name_input("missing"), &ctx, None).await.unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            text_of(&res),
            "No workflow named \"missing\" is registered."
        );
    }

    #[tokio::test]
    async fn runs_a_registered_workflow_by_name() {
        let (tool, captured) = tool_with(
            None,
            Some(vec![WorkflowRef {
                name: rs("known"),
                description: None,
                script: rs("SCRIPT-BODY"),
            }]),
            Ok(ok_result()),
        );
        let ctx = make_ctx(true);
        let res = tool.call(name_input("known"), &ctx, None).await.unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(*captured.lock().unwrap(), Some(rs("SCRIPT-BODY")));
    }

    #[tokio::test]
    async fn runs_inline_script_and_formats_result() {
        let (tool, captured) = tool_with(None, None, Ok(ok_result()));
        let ctx = make_ctx(true);
        let res = tool
            .call(
                runtime_object([
                    ("script", RuntimeJson::String(rs("INLINE"))),
                    ("args", runtime(r#"{"q":1}"#)),
                ]),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(*captured.lock().unwrap(), Some(rs("INLINE")));
        assert_eq!(
            res.serializable.output,
            runtime(
                r#"{"ok":true,"runId":"wf_1","result":{"a":1},"agentsSpawned":3,"outputTokens":42,"message":"Workflow \"demo\" finished: 3 agent(s), 42 output tokens (runId wf_1)."}"#,
            )
        );
        assert_eq!(
            text_of(&res),
            "Workflow \"demo\" finished: 3 agent(s), 42 output tokens (runId wf_1).\n\nResult:\n{\n  \"a\": 1\n}"
        );
    }

    #[tokio::test]
    async fn result_text_preserves_lone_surrogate_as_inner_json_escape() {
        let result = runtime(r#"{"lone":"\ud800"}"#);
        let mut outcome = ok_result();
        outcome.result = result;
        let (tool, _) = tool_with(None, None, Ok(outcome));
        let ctx = make_ctx(true);

        let response = tool.call(script_input("INLINE"), &ctx, None).await.unwrap();
        let text = text_of(&response);
        assert!(
            text.ends_with("Result:\n{\n  \"lone\": \"\\ud800\"\n}"),
            "{text:?}"
        );
        assert!(!text.contains("\u{fdd0}"), "{text:?}");

        let outer = runtime_json::stringify(&runtime_object([("text", RuntimeJson::String(text))]));
        assert!(outer.contains(r#"\\ud800"#), "{outer:?}");
        assert!(!outer.contains("\u{fdd0}"), "{outer:?}");
    }

    #[tokio::test]
    async fn runtime_error_becomes_a_model_error() {
        let (tool, _) = tool_with(None, None, Err(rs("boom in the runtime")));
        let ctx = make_ctx(true);
        let res = tool.call(script_input("INLINE"), &ctx, None).await.unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(text_of(&res), "boom in the runtime");
        assert_eq!(
            res.serializable.output,
            runtime(r#"{"ok":false,"message":"boom in the runtime"}"#)
        );
    }
}
