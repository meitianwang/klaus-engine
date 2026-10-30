//! The `think` tool: a no-op scratchpad. Lets the model commit an explicit chunk
//! of reasoning as a tool call (useful for planning mid-turn) without any side
//! effect. The thought is acknowledged but otherwise has no effect on the
//! environment.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use crate::runtime_values::{RuntimeJson, RuntimeObject, RuntimeString};
use crate::tools::build_tool::text_result;
use crate::types::errors::ToolError;
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};

/// Build the tool's input JSON Schema: a single required `thought` string.
fn build_input_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "thought": {
                "type": "string",
                "description": "A reasoning step to record. Has no side effects."
            }
        },
        "required": ["thought"],
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .unwrap()
}

/// A no-op scratchpad tool that records a reasoning step.
pub struct ThinkTool {
    input_schema: JsonSchema,
}

impl ThinkTool {
    fn new() -> Self {
        ThinkTool {
            input_schema: build_input_schema(),
        }
    }
}

/// Construct the `think` tool as an erased registry-side [`Tool`].
pub fn create_think_tool() -> Arc<dyn Tool> {
    Arc::new(ThinkTool::new())
}

#[async_trait]
impl Tool for ThinkTool {
    fn static_name(&self) -> &str {
        "think"
    }

    fn static_description(&self) -> &str {
        "Record a thought or reasoning step. A no-op scratchpad — nothing happens beyond logging it."
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

    fn summarize(&self, _input: &Json) -> Option<RuntimeString> {
        Some("Thinking".into())
    }

    async fn call(
        &self,
        _input: Json,
        _ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let mut output = RuntimeObject::new();
        output.insert("recorded", RuntimeJson::Bool(true));
        Ok(text_result(
            RuntimeJson::Object(output),
            "Thought recorded.".to_string(),
            false,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_process::RuntimeEnv;
    use crate::runtime_values::{runtime_json, RuntimePath, RuntimeString};
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
            cwd: ".".into(),
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

    fn exact_json(source: &str) -> Json {
        runtime_json::parse(&RuntimeString::from(source)).expect("valid exact runtime JSON")
    }

    fn text_of(res: &ToolResult) -> RuntimeString {
        match &res.serializable.model_content[0] {
            OutputBlock::Text(t) => t.text.clone(),
            _ => panic!("expected a text block"),
        }
    }

    #[test]
    fn exposes_expected_metadata() {
        let tool = create_think_tool();
        assert_eq!(tool.name(), RuntimeString::from("think"));
        assert_eq!(
            tool.description(),
            RuntimeString::from("Record a thought or reasoning step. A no-op scratchpad — nothing happens beyond logging it.")
        );
        let input = exact_json(r#"{"thought":"x"}"#);
        assert!(tool.is_read_only(&input));
        assert!(tool.is_concurrency_safe(&input));
        assert!(tool.is_enabled());
        assert_eq!(tool.is_destructive(&input), Some(false));
        assert_eq!(
            tool.summarize(&input),
            Some(RuntimeString::from("Thinking"))
        );
    }

    #[test]
    fn input_schema_describes_the_thought_field() {
        let tool = create_think_tool();
        let schema = tool.input_schema();
        assert_eq!(
            schema.get("type").and_then(RuntimeJson::as_string),
            Some(&RuntimeString::from("object"))
        );
        assert_eq!(
            schema.get("additionalProperties"),
            Some(&RuntimeJson::Bool(false))
        );
        assert_eq!(
            schema.get("required").unwrap(),
            &RuntimeJson::Array(vec![RuntimeJson::from("thought")])
        );
        let thought = schema
            .get("properties")
            .and_then(|p| p.get("thought"))
            .expect("thought property");
        assert_eq!(
            thought.get("type").and_then(RuntimeJson::as_string),
            Some(&RuntimeString::from("string"))
        );
        assert_eq!(
            thought.get("description").and_then(RuntimeJson::as_string),
            Some(&RuntimeString::from(
                "A reasoning step to record. Has no side effects."
            ))
        );
    }

    #[tokio::test]
    async fn call_records_the_thought() {
        let tool = create_think_tool();
        let ctx = mk_ctx();
        let res = tool
            .call(exact_json(r#"{"thought":"step"}"#), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(res.serializable.output, exact_json(r#"{"recorded":true}"#));
        assert_eq!(text_of(&res), RuntimeString::from("Thought recorded."));
    }
}
