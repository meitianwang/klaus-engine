//! Defect: each independent request appended its steps to the conversation's
//! completed task history, so the overview grew indefinitely.
//! Root cause: durable task storage had no explicit current-plan membership.
//! Fixed by: 2026-09-05 current-plan lifecycle work item.

use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimePath, RuntimeString};
use klaus_agent_core::sandbox::process::ProcessSandbox;
use klaus_agent_core::tasks::task_store::{query_task_list, TaskPlanBinding};
use klaus_agent_core::tools::builtins::task_tools::{
    create_task_create_tool, create_task_get_tool, create_task_list_tool, create_task_update_tool,
    create_task_write_tool,
};
use klaus_agent_core::tools::file_state::InMemoryFileStateCache;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::permission::PermissionContext;
use klaus_agent_core::types::tool::{Tool, ToolContext};
use serde_json::{json, Value};
use std::sync::Arc;

struct TaskDirectory(std::path::PathBuf);
impl Drop for TaskDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn input(value: Value) -> RuntimeJson {
    runtime_json::parse(&RuntimeString::from(value.to_string())).unwrap()
}

async fn call(tool: &Arc<dyn Tool>, value: Value, ctx: &ToolContext) -> RuntimeJson {
    let parsed = tool.parse_input(&input(value)).unwrap();
    let result = tool.call(parsed, ctx, None).await.unwrap();
    assert_ne!(result.serializable.is_error, Some(true));
    result.serializable.output
}

fn text(value: &RuntimeString) -> String {
    String::from_utf8(value.to_node_utf8()).unwrap()
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

#[tokio::test]
async fn public_tools_replace_completed_history_and_restore_only_the_current_plan() {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let list_name = format!("regression-current-plan-{}", uuid::Uuid::new_v4());
    let _directory = TaskDirectory(klaus_agent_core::fs::paths::tasks_dir(&list_name).into());
    let list_id = RuntimeString::from(list_name);
    let mut ctx = make_ctx("/tmp", process.clone());
    ctx.task_list_id = Some(list_id.clone());
    ctx.task_plan = Some(TaskPlanBinding::capture(&list_id));
    let create = create_task_create_tool();
    let write = create_task_write_tool();
    let update = create_task_update_tool();
    let list = create_task_list_tool();
    let get = create_task_get_tool();

    // Reproduce the completed fourteen-row conversation from the defect.
    for index in 1..=14 {
        call(
            &create,
            json!({"subject": format!("previous step {index}"), "description": "previous work"}),
            &ctx,
        )
        .await;
        call(
            &update,
            json!({"taskId": index.to_string(), "status": "completed"}),
            &ctx,
        )
        .await;
    }
    let previous = query_task_list(&list_id, &list_id, Default::default())
        .await
        .unwrap();
    assert_eq!(previous.tasks.len(), 14);
    let old_run = ctx.task_plan.as_ref().unwrap().fork();

    // Omitting merge is a complete replacement, not an append or hidden filter.
    call(
        &write,
        json!({"tasks": [
            {"id":"second", "subject":"new first step"},
            {"id":"first", "subject":"new second step"}
        ]}),
        &ctx,
    )
    .await;
    let current = query_task_list(&list_id, &list_id, Default::default())
        .await
        .unwrap();
    assert_eq!(current.plan_kind, "plan");
    assert_ne!(current.plan_id, previous.plan_id);
    assert_eq!(
        current
            .tasks
            .iter()
            .map(|task| text(&task.subject))
            .collect::<Vec<_>>(),
        vec!["new first step", "new second step"]
    );
    assert!(current
        .tasks
        .iter()
        .all(|task| task.id.starts_with("plan-step-")));

    // Merge IDs are caller IDs and preserve authoritative order even when the
    // supplied update order differs. Partial updates retain each title.
    call(
        &write,
        json!({"merge":true, "tasks":[
            {"id":"first", "status":"completed"},
            {"id":"second", "status":"completed"}
        ]}),
        &ctx,
    )
    .await;
    let complete = query_task_list(&list_id, &list_id, Default::default())
        .await
        .unwrap();
    assert_eq!(complete.plan_id, current.plan_id);
    assert_eq!(
        complete.tasks.len(),
        2,
        "completion retains the current plan"
    );
    assert_eq!(
        complete
            .tasks
            .iter()
            .map(|task| text(&task.subject))
            .collect::<Vec<_>>(),
        vec!["new first step", "new second step"]
    );
    assert!(write
        .call(input(json!({"tasks":[]})), &ctx, None)
        .await
        .is_err());
    assert_eq!(
        query_task_list(&list_id, &list_id, Default::default())
            .await
            .unwrap(),
        complete
    );

    // A fresh run recovers current membership from durable state. Archived
    // work remains addressable, but cannot take over the latest overview.
    let mut resumed = make_ctx("/tmp", process.clone());
    resumed.task_list_id = Some(list_id.clone());
    resumed.task_plan = Some(TaskPlanBinding::capture(&list_id));
    let restored = call(&list, json!({}), &resumed).await;
    let rows = restored.get("tasks").unwrap().as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows
        .iter()
        .all(|row| row.get("status").and_then(RuntimeJson::as_str)
            == Some(&RuntimeString::from("completed"))));
    assert_eq!(
        rows[0].get("subject").unwrap().as_str().unwrap(),
        "new first step"
    );
    call(&get, json!({"taskId":"1"}), &resumed).await;
    let mut old_run_ctx = make_ctx("/tmp", process.clone());
    old_run_ctx.task_list_id = Some(list_id.clone());
    old_run_ctx.task_plan = Some(old_run);
    assert!(write
        .call(
            input(json!({"tasks":[{"subject":"stale replacement"}]})),
            &old_run_ctx,
            None
        )
        .await
        .is_err());
    call(
        &update,
        json!({"taskId":"1", "description":"late archived detail"}),
        &old_run_ctx,
    )
    .await;
    assert_eq!(
        query_task_list(&list_id, &list_id, Default::default())
            .await
            .unwrap(),
        complete
    );

    // Ordinary plans consume no numeric IDs. Explicitly appending a structured
    // task continues after the fourteen stored tasks, with no phantom numbers.
    call(
        &create,
        json!({"subject":"deliberate follow-up", "description":"same plan"}),
        &resumed,
    )
    .await;
    let appended = query_task_list(&list_id, &list_id, Default::default())
        .await
        .unwrap();
    assert_eq!(appended.plan_id, current.plan_id);
    assert_eq!(appended.tasks.len(), 3);
    assert_eq!(appended.tasks[2].id, "15");
    process.shutdown().await.unwrap();
}
