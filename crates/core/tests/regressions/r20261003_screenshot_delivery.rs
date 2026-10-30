//! Defect: screenshot output was missing its saved path or a real model image,
//! and Code Mode discarded images returned by nested capture tools.
//! Root cause: capture persistence was optional and image blocks became JSON text.
//! Fixed by: 2026-10-03 UI screenshot delivery work item.

use async_trait::async_trait;
use klaus_agent_core::browser::{
    attest_spawn_free, BrowserEventSink, BrowserExecutor, BrowserOperation, BrowserRuntimeError,
    BrowserRuntimeRequest, BrowserRuntimeResponse,
};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{
    runtime_codec, runtime_json, RuntimeJson, RuntimePath, RuntimeString,
};
use klaus_agent_core::sandbox::process::ProcessSandbox;
use klaus_agent_core::schemas::content::OutputBlock;
use klaus_agent_core::tools::builtins::browser::create_browser_tools;
use klaus_agent_core::tools::file_state::InMemoryFileStateCache;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::permission::PermissionContext;
use klaus_agent_core::types::tool::{Tool, ToolContext, ToolResult};
use std::sync::{Arc, Mutex};

const PNG: &str =
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aEAAAAABJRU5ErkJggg==";
const PATH: &str = "/tmp/verified, UI screenshot.png";

struct Capture {
    requests: Arc<Mutex<Vec<BrowserRuntimeRequest>>>,
}
// SAFETY: this fixture only constructs in-memory protocol responses.
#[async_trait]
unsafe impl BrowserExecutor for Capture {
    async fn execute(
        &self,
        request: BrowserRuntimeRequest,
        _signal: AbortSignal,
        _emit: BrowserEventSink,
    ) -> Result<BrowserRuntimeResponse, BrowserRuntimeError> {
        let save = matches!(
            request.operation,
            BrowserOperation::Screenshot { save: true, .. }
        );
        self.requests.lock().unwrap().push(request);
        let value = runtime_json::parse(&RuntimeString::from(format!(
            "{{\"dataUrl\":\"data:image/png;base64,{PNG}\",\"mimeType\":\"image/png\",\"saved\":{save}{}}}",
            if save { format!(",\"path\":{}", serde_json::to_string(PATH).unwrap()) } else { String::new() }
        ))).unwrap();
        Ok(BrowserRuntimeResponse {
            session_id: None,
            tab_id: None,
            url: Some("https://fixture.invalid".into()),
            title: Some("Fixture".into()),
            text: None,
            snapshot: None,
            value: Some(value),
            artifacts: vec![],
            console: vec![],
            network: vec![],
            events: vec![],
            message: Some("capture complete".into()),
            ..Default::default()
        })
    }
}

fn context() -> ToolContext {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    make_ctx("/tmp", process)
}

async fn call(tool: &Arc<dyn Tool>, input: RuntimeJson, ctx: &ToolContext) -> ToolResult {
    let parsed = tool.parse_input(&input).unwrap();
    let result = tool.call(parsed, ctx, None).await.unwrap();
    assert_ne!(result.serializable.is_error, Some(true));
    result
}
fn json(source: &str) -> RuntimeJson {
    runtime_json::parse(&RuntimeString::from(source)).unwrap()
}
fn model_text(result: &ToolResult) -> String {
    result
        .serializable
        .model_content
        .iter()
        .filter_map(|block| match block {
            OutputBlock::Text(text) => Some(text.text.try_to_utf8().unwrap()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}
fn image_count(result: &ToolResult) -> usize {
    result
        .serializable
        .model_content
        .iter()
        .filter(|block| matches!(block, OutputBlock::Image(_)))
        .count()
}

#[tokio::test]
async fn screenshot_default_save_explicit_transient_and_model_image_are_one_contract() {
    let mut ctx = context();
    let requests = Arc::new(Mutex::new(Vec::new()));
    // SAFETY: Capture has no process or network side effects.
    ctx.browser = Some(unsafe {
        attest_spawn_free(Arc::new(Capture {
            requests: requests.clone(),
        }))
    });
    let tool = create_browser_tools()
        .into_iter()
        .find(|tool| tool.static_name() == "browser_screenshot")
        .unwrap();
    for (input, save) in [("{}", true), ("{\"save\":false}", false)] {
        let result = call(&tool, json(input), &ctx).await;
        assert_eq!(image_count(&result), 1);
        let text = model_text(&result);
        assert!(!text.contains(PNG));
        assert!(text.contains("capture complete"));
        assert_eq!(text.contains("Saved: "), save);
        if save {
            assert!(text.contains(&serde_json::to_string(PATH).unwrap()));
        }
        let request = requests.lock().unwrap().last().unwrap().clone();
        assert_eq!(request.permissions.file_transfer, save);
        assert!(request.permissions.capture_screenshot);
        let encoded = runtime_codec::encode_value(&request).unwrap();
        let decoded: BrowserRuntimeRequest = runtime_codec::decode_value(&encoded).unwrap();
        assert_eq!(decoded, request);
    }
    ctx.vision = Some(false);
    let result = call(&tool, json("{}"), &ctx).await;
    assert_eq!(image_count(&result), 0);
    assert!(model_text(&result).contains("cannot understand images"));
    assert!(model_text(&result).contains("Saved: "));
    assert!(!model_text(&result).contains(PNG));
}

fn make_ctx(cwd: &str, process: RuntimeProcessContext) -> ToolContext {
    ToolContext {
        async_question: None,
        background_processes: Arc::new(
            klaus_agent_core::command_runtime::background::BackgroundCommandRegistry::new(),
        ),
        process_context: process,
        cwd: RuntimePath::from(cwd),
        signal: AbortSignal::new(),
        file_state: Arc::new(InMemoryFileStateCache::new()),
        permission: PermissionContext {
            allow_rules: Vec::new(),
            deny_rules: Vec::new(),
            ask_rules: Vec::new(),
            additional_writable_roots: Vec::new(),
            internal_writable_roots: Vec::new(),
            pinned_memory_root: None,
            should_avoid_prompts: false,
        },
        sandbox: Arc::new(ProcessSandbox),
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
