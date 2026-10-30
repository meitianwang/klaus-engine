//! Shared test scaffolding for the builtin task tools: a bare [`ToolContext`]
//! whose optional capabilities are all unset, so a test wires only what it
//! exercises. Everything here is compiled only under `cfg(test)`.

use std::sync::Arc;

use crate::runtime_process::RuntimeEnv;
use crate::runtime_values::RuntimePath;
use crate::types::abort::AbortSignal;
use crate::types::file_state::{FileStateCache, FileStateEntry, RecordOptions};
use crate::types::permission::PermissionContext;
use crate::types::sandbox::{ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec};
use crate::types::tool::ToolContext;

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

/// A `ToolContext` rooted at `cwd` with every optional capability unset and a
/// discarding `emit`. Tests set the fields they need (public fields).
pub fn make_ctx(cwd: &str) -> ToolContext {
    ToolContext {
        async_question: None,
        background_processes: Arc::new(
            crate::command_runtime::background::BackgroundCommandRegistry::new(),
        ),
        process_context: crate::runtime_process::test_runtime_process_context(),
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
