use crate::runtime_process::{RuntimeEnv, RuntimeProcessContext};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeJson, RuntimeObject, RuntimePath, RuntimeString,
};
use crate::schemas::content::{OutputBlock, TextBlock};
use crate::tools::build_tool::text_result;
use crate::tools::builtins::code_mode::create_code_mode_tools;
use crate::types::abort::AbortSignal;
use crate::types::errors::ToolError;
use crate::types::file_state::{FileStateCache, FileStateEntry, RecordOptions};
use crate::types::json::{Json, JsonSchema};
use crate::types::permission::PermissionContext;
use crate::types::sandbox::{ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec};
use crate::types::tool::{
    InvokeToolResult, Tool, ToolCatalogEntry, ToolContext, ToolExposure, ToolInputKind,
    ToolProgress, ToolResult,
};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};

const SCENARIO: &str = "exec-wait-code-mode";

macro_rules! exact_json {
    (null) => {
        RuntimeJson::Null
    };
    ({}) => {
        RuntimeJson::Object(RuntimeObject::new())
    };
    ([ $($value:tt),* $(,)? ]) => {
        RuntimeJson::Array(vec![$(exact_json!($value)),*])
    };
    ({ $($key:literal : $value:tt),* $(,)? }) => {{
        let mut object = RuntimeObject::new();
        $(object.insert($key, exact_json!($value));)*
        RuntimeJson::Object(object)
    }};
    ($value:expr) => {
        runtime_codec::encode_value(&$value).expect("exact trace JSON expression must encode")
    };
}

pub async fn exec_wait_trace(
    process_context: &RuntimeProcessContext,
) -> Result<Json, RuntimeCodecError> {
    let echo = Arc::new(MockTool {
        name: "echo".to_string(),
        description: "echo JSON".to_string(),
        input_kind: ToolInputKind::JsonSchema,
        input_schema: JsonSchema::new(),
    }) as Arc<dyn Tool>;

    let tool_set = create_code_mode_tools();
    tool_set.set_tool_registry(vec![
        echo.clone(),
        tool_set.exec.clone(),
        tool_set.wait.clone(),
    ]);

    let nested_calls = Arc::new(Mutex::new(Vec::<Json>::new()));
    let events = Arc::new(Mutex::new(Vec::<Json>::new()));
    let ctx = make_trace_ctx(process_context, nested_calls.clone(), events.clone());

    let immediate = call(&tool_set.exec, exact_json!("text(\"hi\")"), &ctx).await;
    let nested = call(
        &tool_set.exec,
        exact_json!(
            "const echo = await tools.echo({ value: \"x\" }); text(JSON.stringify({ tools: ALL_TOOLS, echo: { input: echo.input, name: echo.name, ok: echo.ok } }))"
        ),
        &ctx,
    )
    .await;
    let store = call(
        &tool_set.exec,
        exact_json!("store(\"answer\", { value: 42 }); text(\"stored\")"),
        &ctx,
    )
    .await;
    let load = call(
        &tool_set.exec,
        exact_json!("text(JSON.stringify(load(\"answer\")))"),
        &ctx,
    )
    .await;

    let pending_initial = call(
        &tool_set.exec,
        exact_json!("text(\"before\"); yield_control(); text(\"after\")"),
        &ctx,
    )
    .await;
    let pending_cell = cell_id(&pending_initial);
    let pending_wait = call(
        &tool_set.wait,
        exact_json!({ "cell_id": (pending_cell.clone()) }),
        &ctx,
    )
    .await;
    let pending_second_wait = call(
        &tool_set.wait,
        exact_json!({ "cell_id": (pending_cell.clone()) }),
        &ctx,
    )
    .await;

    let truncated_initial = call(
        &tool_set.exec,
        exact_json!(
            "// @exec: {\"max_output_tokens\": 3}\ntext(\"abcdef\"); yield_control(); text(\"gh\")"
        ),
        &ctx,
    )
    .await;
    let truncated_cell = cell_id(&truncated_initial);
    let truncated_wait = call(
        &tool_set.wait,
        exact_json!({ "cell_id": (truncated_cell.clone()) }),
        &ctx,
    )
    .await;

    let terminate_initial = call(
        &tool_set.exec,
        exact_json!("text(\"live\"); yield_control(); await new Promise(() => {})"),
        &ctx,
    )
    .await;
    let terminate_cell = cell_id(&terminate_initial);
    let terminated = call(
        &tool_set.wait,
        exact_json!({ "cell_id": (terminate_cell.clone()), "terminate": true }),
        &ctx,
    )
    .await;

    let bad_exec_input = call(
        &tool_set.exec,
        exact_json!({ "code": "text(\"nope\")" }),
        &ctx,
    )
    .await;
    let bad_wait_input = call(&tool_set.wait, exact_json!({}), &ctx).await;
    let thrown = call(
        &tool_set.exec,
        exact_json!("throw new Error(\"boom\")"),
        &ctx,
    )
    .await;
    let missing = call(&tool_set.wait, exact_json!({ "cell_id": "missing" }), &ctx).await;

    Ok(exact_json!({
        "scenario": SCENARIO,
        "metadata": {
            "toolNames": [(tool_set.exec.name()), (tool_set.wait.name())],
            "exec": (tool_meta(&tool_set.exec, &exact_json!("text(\"hi\")"))),
            "wait": (tool_meta(&tool_set.wait, &exact_json!({ "cell_id": "1" }))),
            "waitTerminate": (tool_meta(&tool_set.wait, &exact_json!({ "cell_id": "1", "terminate": true }))),
        },
        "calls": {
            "immediate": (immediate),
            "nested": (nested),
            "store": (store),
            "load": (load),
            "pending": {
                "initial": (pending_initial),
                "wait": (pending_wait),
                "secondWait": (pending_second_wait),
            },
            "truncated": {
                "initial": (truncated_initial),
                "wait": (truncated_wait),
            },
            "terminate": {
                "initial": (terminate_initial),
                "wait": (terminated),
            },
            "errors": {
                "badExecInput": (bad_exec_input),
                "badWaitInput": (bad_wait_input),
                "thrown": (thrown),
                "missing": (missing),
            },
        },
        "nestedCalls": (nested_calls.lock().unwrap().clone()),
        "events": (events.lock().unwrap().clone()),
    }))
}

async fn call(tool: &Arc<dyn Tool>, input: Json, ctx: &ToolContext) -> Json {
    serializable(tool.call(input, ctx, None).await.unwrap())
}

fn tool_meta(tool: &Arc<dyn Tool>, input: &Json) -> Json {
    let input_kind = match tool.input_kind() {
        ToolInputKind::JsonSchema => exact_json!({ "type": "json_schema" }),
        ToolInputKind::Freeform { format } => {
            exact_json!({ "type": "freeform", "format": (format) })
        }
    };
    let input_schema = if matches!(tool.input_kind(), ToolInputKind::Freeform { .. }) {
        Json::Null
    } else {
        tool.input_schema()
    };
    exact_json!({
        "name": (tool.name()),
        "description": (tool.description()),
        "inputKind": (input_kind),
        "inputSchema": (input_schema),
        "searchHint": (tool.search_hint()),
        "exposure": (tool.exposure()),
        "maxResultSizeChars": (tool.max_result_size_chars()),
        "mcpServer": (tool.mcp_server()),
        "isReadOnly": (tool.is_read_only(input)),
        "isConcurrencySafe": (tool.is_concurrency_safe(input)),
        "isDestructive": (tool.is_destructive(input)),
        "summarize": (tool.summarize(input)),
    })
}

fn serializable(result: ToolResult) -> Json {
    exact_json!({
        "output": (result.serializable.output),
        "modelContent": (result.serializable.model_content),
        "isError": (result.serializable.is_error.unwrap_or(false)),
        "mcpMeta": (result.serializable.mcp_meta),
    })
}

fn cell_id(result: &Json) -> &RuntimeString {
    result
        .pointer("/output/cellId")
        .and_then(Json::as_str)
        .expect("runtime response should include a cellId")
}

fn make_trace_ctx(
    process_context: &RuntimeProcessContext,
    nested_calls: Arc<Mutex<Vec<Json>>>,
    events: Arc<Mutex<Vec<Json>>>,
) -> ToolContext {
    let mut ctx = make_ctx(process_context, "/tmp/klaus-exec-wait-trace");
    ctx.tool_catalog = Some(Box::new(|| {
        vec![ToolCatalogEntry {
            name: RuntimeString::from("echo"),
            description: RuntimeString::from("echo JSON"),
            search_hint: None,
            exposure: ToolExposure::Direct,
            spec: None,
        }]
    }));
    ctx.emit = std::sync::Arc::new(move |event| {
        events
            .lock()
            .unwrap()
            .push(runtime_codec::encode_value(&event).unwrap());
        Box::pin(async {})
    });
    ctx.invoke_tool = Some(Arc::new(move |name: RuntimeString, input: Json| {
        nested_calls
            .lock()
            .unwrap()
            .push(exact_json!({ "name": (name.clone()), "input": (input.clone()) }));
        Box::pin(async move {
            let nested_output = exact_json!({
                "ok": true,
                "name": (name),
                "input": (input),
            });
            Ok(InvokeToolResult {
                is_error: false,
                output: None,
                content: vec![OutputBlock::Text(TextBlock {
                    text: crate::runtime_values::runtime_json::stringify(&nested_output),
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                })],
            })
        })
    }));
    ctx
}

struct MockTool {
    name: String,
    description: String,
    input_kind: ToolInputKind,
    input_schema: JsonSchema,
}

#[async_trait]
impl Tool for MockTool {
    fn static_name(&self) -> &str {
        &self.name
    }

    fn static_description(&self) -> &str {
        &self.description
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn input_kind(&self) -> ToolInputKind {
        self.input_kind.clone()
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

    async fn call(
        &self,
        _input: Json,
        _ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        Ok(text_result(Json::Null, String::new(), false))
    }
}

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

fn make_ctx(process_context: &RuntimeProcessContext, cwd: &str) -> ToolContext {
    ToolContext {
        async_question: None,
        background_processes: Arc::new(
            crate::command_runtime::background::BackgroundCommandRegistry::new(),
        ),
        process_context: process_context.clone(),
        cwd: RuntimePath::from(cwd),
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
        sandbox_retry_on_denial: None,
        request_approval: None,
        request_network_access: None,
        enqueue_background_notice: None,
        record_rollout_event: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::events::ToolDomainEvent;

    #[test]
    fn exact_tool_event_and_result_wires_preserve_lone_surrogates() {
        let strings = [
            RuntimeString::from_utf16([0xd800]),
            RuntimeString::from_utf16([0xdc00]),
            RuntimeString::from_utf16([0xfffd]),
        ];
        let wires = strings
            .iter()
            .map(|text| {
                let event = ToolDomainEvent::Status {
                    message: text.clone(),
                };
                let result = text_result(
                    exact_json!({ "value": (text.clone()) }),
                    text.clone(),
                    false,
                );
                let value = exact_json!({
                    "event": (runtime_codec::encode_value(&event).unwrap()),
                    "result": (serializable(result)),
                });
                assert_eq!(
                    value.pointer("/event/message").and_then(Json::as_str),
                    Some(text)
                );
                assert_eq!(
                    value.pointer("/result/output/value").and_then(Json::as_str),
                    Some(text)
                );
                assert_eq!(
                    value
                        .pointer("/result/modelContent/0/text")
                        .and_then(Json::as_str),
                    Some(text)
                );
                crate::runtime_values::runtime_json::stringify(&value)
            })
            .collect::<Vec<_>>();

        assert_ne!(wires[0], wires[1]);
        assert_ne!(wires[0], wires[2]);
        assert_ne!(wires[1], wires[2]);
    }
}
