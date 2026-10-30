//! The `sleep` tool: pause for a bounded number of seconds, resolving early
//! (without error) if the tool's abort signal fires. Useful for pacing polls of
//! a background shell.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::json;

use crate::runtime_values::{RuntimeJson, RuntimeNumber, RuntimeObject, RuntimeString};
use crate::tools::build_tool::text_result;
use crate::types::errors::ToolError;
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};

/// The upper bound (in seconds) the tool will sleep.
const MAX_SECONDS: i64 = 300;

/// Build the tool's input JSON Schema: a single required, positive, bounded
/// `seconds` number.
fn build_input_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "seconds": {
                "type": "number",
                "exclusiveMinimum": 0,
                "maximum": MAX_SECONDS,
                "description": format!("How long to sleep, in seconds (max {MAX_SECONDS})")
            }
        },
        "required": ["seconds"],
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .unwrap()
}

/// A tool that pauses for a bounded number of seconds.
pub struct SleepTool {
    input_schema: JsonSchema,
}

impl SleepTool {
    fn new() -> Self {
        SleepTool {
            input_schema: build_input_schema(),
        }
    }
}

/// Construct the `sleep` tool as an erased registry-side [`Tool`].
pub fn create_sleep_tool() -> Arc<dyn Tool> {
    Arc::new(SleepTool::new())
}

#[async_trait]
impl Tool for SleepTool {
    fn static_name(&self) -> &str {
        "sleep"
    }

    fn static_description(&self) -> &str {
        "Pause for a given number of seconds. Returns early if the call is aborted."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
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

    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        let seconds = input.get("seconds").and_then(Json::as_f64).unwrap_or(0.0);
        Some(format!("Sleeping {seconds}s").into())
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let seconds: f64 = input.get("seconds").and_then(Json::as_f64).unwrap_or(0.0);
        let ms = (seconds * 1000.0).round() as u64;
        let start = Instant::now();

        let aborted = if ctx.signal.is_cancelled() {
            true
        } else {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(ms)) => false,
                _ = ctx.signal.cancelled() => true,
            }
        };

        let slept_ms = start.elapsed().as_millis() as u64;
        let text = if aborted {
            format!("Sleep aborted after {slept_ms}ms.")
        } else {
            format!("Slept {ms}ms.")
        };
        let mut output = RuntimeObject::new();
        output.insert(
            "sleptMs",
            RuntimeJson::Number(RuntimeNumber::from(slept_ms)),
        );
        output.insert("aborted", RuntimeJson::Bool(aborted));
        Ok(text_result(RuntimeJson::Object(output), text, false))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_process::RuntimeEnv;
    use crate::runtime_values::{runtime_json, RuntimePath};
    use crate::schemas::content::OutputBlock;
    use crate::types::abort::AbortSignal;
    use crate::types::file_state::{FileStateCache, FileStateEntry, RecordOptions};
    use crate::types::permission::PermissionContext;
    use crate::types::sandbox::{
        ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec,
    };
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
        let tool = create_sleep_tool();
        assert_eq!(tool.name(), RuntimeString::from("sleep"));
        assert_eq!(
            tool.description(),
            RuntimeString::from(
                "Pause for a given number of seconds. Returns early if the call is aborted."
            )
        );
        let input = exact_json(r#"{"seconds":5}"#);
        assert!(tool.is_read_only(&input));
        assert!(tool.is_concurrency_safe(&input));
        assert!(tool.is_enabled());
        assert_eq!(tool.is_destructive(&input), Some(false));
        assert_eq!(
            tool.summarize(&input),
            Some(RuntimeString::from("Sleeping 5s"))
        );
        assert_eq!(
            tool.summarize(&exact_json(r#"{"seconds":2.5}"#)),
            Some(RuntimeString::from("Sleeping 2.5s"))
        );
    }

    #[test]
    fn input_schema_bounds_the_seconds_field() {
        let tool = create_sleep_tool();
        let schema = tool.input_schema();
        let seconds = schema
            .get("properties")
            .and_then(|p| p.get("seconds"))
            .expect("seconds property");
        assert_eq!(
            seconds.get("type").and_then(RuntimeJson::as_string),
            Some(&RuntimeString::from("number"))
        );
        assert_eq!(seconds.get("exclusiveMinimum"), Some(&RuntimeJson::from(0)));
        assert_eq!(seconds.get("maximum"), Some(&RuntimeJson::from(300)));
        assert_eq!(
            seconds.get("description").and_then(RuntimeJson::as_string),
            Some(&RuntimeString::from(
                "How long to sleep, in seconds (max 300)"
            ))
        );
        assert_eq!(
            schema.get("required").unwrap(),
            &RuntimeJson::Array(vec![RuntimeJson::from("seconds")])
        );
    }

    #[tokio::test]
    async fn call_sleeps_and_reports_the_duration() {
        let tool = create_sleep_tool();
        let ctx = mk_ctx();
        let res = tool
            .call(exact_json(r#"{"seconds":0.01}"#), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            res.serializable.output.get("aborted"),
            Some(&RuntimeJson::Bool(false))
        );
        assert_eq!(text_of(&res), RuntimeString::from("Slept 10ms."));
    }

    #[tokio::test]
    async fn call_returns_early_when_already_aborted() {
        let tool = create_sleep_tool();
        let ctx = mk_ctx();
        ctx.signal.cancel();
        let res = tool
            .call(exact_json(r#"{"seconds":300}"#), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            res.serializable.output.get("aborted"),
            Some(&RuntimeJson::Bool(true))
        );
        assert!(
            text_of(&res).starts_with("Sleep aborted after"),
            "text: {:?}",
            text_of(&res)
        );
    }
}
