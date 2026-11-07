//! Defect: reading a historical completion could trigger a fresh completion report.
//! Root cause: the common goal result formatter treated stored state as mutation authority.
//! Work item: 20261017 successful completion results and historical goal reads.

use klaus_agent_core::goal::runtime::{GoalRequestIdentity, GoalRuntime};
use klaus_agent_core::goal::tools::{create_get_goal_tool, create_update_goal_tool};
use klaus_agent_core::runtime_process::{
    remove_process_environment_var, set_process_environment_var, RuntimeProcessContext,
};
use klaus_agent_core::runtime_values::{
    runtime_codec, runtime_json, RuntimeJson, RuntimePath, RuntimeString,
};
use klaus_agent_core::sandbox::process::ProcessSandbox;
use klaus_agent_core::schemas::message::Usage;
use klaus_agent_core::tools::file_state::InMemoryFileStateCache;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::permission::PermissionContext;
use klaus_agent_core::types::tool::{Tool, ToolContext, ToolResult};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

struct Home {
    path: PathBuf,
    previous: Option<std::ffi::OsString>,
}
impl Home {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "klaus-goal-completion-result-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&path).unwrap();
        let previous = std::env::var_os("KLAUS_CONFIG_DIR");
        set_process_environment_var("KLAUS_CONFIG_DIR", &path);
        Self { path, previous }
    }
}
impl Drop for Home {
    fn drop(&mut self) {
        match self.previous.take() {
            Some(previous) => set_process_environment_var("KLAUS_CONFIG_DIR", previous),
            None => remove_process_environment_var("KLAUS_CONFIG_DIR"),
        }
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
fn input(value: Value) -> RuntimeJson {
    runtime_json::parse(&RuntimeString::from(value.to_string())).unwrap()
}
async fn call(tool: &Arc<dyn Tool>, value: Value, ctx: &ToolContext) -> ToolResult {
    tool.call(tool.parse_input(&input(value)).unwrap(), ctx, None)
        .await
        .unwrap()
}
fn no_report(result: &ToolResult) {
    assert!(
        result
            .serializable
            .output
            .get("completionBudgetReport")
            .is_none(),
        "{:?}",
        result.serializable
    );
}
async fn context(
    home: &Home,
    process: RuntimeProcessContext,
    thread: &str,
    budget: Option<i64>,
    elapsed_ms: i64,
) -> (Arc<GoalRuntime>, ToolContext) {
    let elapsed = Arc::new(AtomicI64::new(0));
    let runtime = Arc::new(GoalRuntime::with_clock(
        thread.into(),
        Arc::new(|| uuid::Uuid::new_v4().to_string()),
        Arc::new(|| 1_000),
        {
            let elapsed = elapsed.clone();
            Arc::new(move || elapsed.load(Ordering::SeqCst))
        },
        None,
        None,
    ));
    let created = runtime
        .create_host_goal("Finish the authorized result.".into(), None, budget, None)
        .await
        .unwrap()
        .unwrap()
        .goal;
    runtime
        .on_turn_start_for(true, Some(&created.goal_id), true)
        .await
        .unwrap();
    let identity = Arc::new(Mutex::new(runtime.request_identity()));
    runtime
        .prepare_provider_request(&[], identity.as_ref(), None, None)
        .await
        .unwrap()
        .accepted(&runtime)
        .await
        .unwrap();
    runtime.on_token_usage(&Usage {
        input_tokens: 10,
        output_tokens: 3,
        cache_read_input_tokens: 17,
        cache_creation_input_tokens: 11,
        total_tokens: Some(999),
    });
    elapsed.store(elapsed_ms, Ordering::SeqCst);
    let ctx = make_ctx(
        home.path.to_str().unwrap(),
        process,
        runtime.clone(),
        identity,
    );
    (runtime, ctx)
}

fn make_ctx(
    cwd: &str,
    process: RuntimeProcessContext,
    runtime: Arc<GoalRuntime>,
    identity: Arc<Mutex<GoalRequestIdentity>>,
) -> ToolContext {
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
        goal: Some(runtime),
        goal_generation: Some(identity),
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
async fn only_successful_completion_mutations_publish_the_actual_spend_report() {
    let home = Home::new();
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let get = create_get_goal_tool();
    let update = create_update_goal_tool();
    let mut receipts = Vec::new();
    for (index, (budget, elapsed_ms)) in
        [(Some(100), 0), (Some(100), 2_500), (None, 2_500), (None, 0)]
            .into_iter()
            .enumerate()
    {
        let (runtime, ctx) = context(
            &home,
            process.clone(),
            &format!("completion-result-{index}"),
            budget,
            elapsed_ms,
        )
        .await;
        let completed = call(&update, json!({"status":"complete"}), &ctx).await;
        assert_eq!(completed.serializable.is_error, Some(false));
        let output = &completed.serializable.output;
        assert_eq!(output.get("ok").and_then(RuntimeJson::as_bool), Some(true));
        let goal = output.get("goal").unwrap();
        assert_eq!(
            goal.get("status").and_then(RuntimeJson::as_str),
            Some(&RuntimeString::from("complete"))
        );
        assert_eq!(
            goal.get("tokensUsed").and_then(RuntimeJson::as_i64),
            Some(13)
        );
        assert_eq!(
            goal.get("timeUsedSeconds").and_then(RuntimeJson::as_i64),
            Some(elapsed_ms / 1_000)
        );
        assert_eq!(
            goal.get("tokenBudget").and_then(RuntimeJson::as_i64),
            budget
        );
        if budget.is_some() || elapsed_ms >= 1_000 {
            let report = output
                .get("completionBudgetReport")
                .unwrap()
                .as_str()
                .unwrap()
                .try_to_utf8()
                .unwrap();
            let spend = budget.map_or_else(
                || "13 tokens".to_string(),
                |budget| format!("13 of {budget} budgeted tokens"),
            );
            assert!(
                report.starts_with(&format!(
                    "Goal complete: used {spend} over {} seconds.",
                    elapsed_ms / 1_000
                )),
                "{report}"
            );
        } else {
            no_report(&completed);
        }
        let final_snapshot = runtime.snapshot().await.unwrap();
        let historical = call(&get, json!({}), &ctx).await;
        assert_eq!(historical.serializable.is_error, Some(false));
        no_report(&historical);
        assert_eq!(
            historical.serializable.output.get("goal"),
            output.get("goal")
        );
        let repeated = call(&update, json!({"status":"complete"}), &ctx).await;
        assert_eq!(repeated.serializable.is_error, Some(true));
        no_report(&repeated);
        assert_eq!(
            runtime.snapshot().await.unwrap(),
            final_snapshot,
            "a repeated completion cannot create fresh mutation authority"
        );
        receipts.push(completed.serializable.output);
        receipts.push(historical.serializable.output);
        receipts.push(repeated.serializable.output);
        runtime.on_turn_end(false).await.unwrap();
    }
    let (runtime, ctx) = context(&home, process.clone(), "paused-result", Some(100), 2_500).await;
    for _ in 0..2 {
        let paused = call(&update, json!({"status":"paused"}), &ctx).await;
        assert_eq!(paused.serializable.is_error, Some(false));
        no_report(&paused);
        assert_eq!(
            paused
                .serializable
                .output
                .get("goal")
                .unwrap()
                .get("status")
                .and_then(RuntimeJson::as_str),
            Some(&RuntimeString::from("paused"))
        );
        receipts.push(paused.serializable.output);
    }
    runtime.set_enabled(false, true).await.unwrap();
    let refused = call(&update, json!({"status":"complete"}), &ctx).await;
    assert_eq!(refused.serializable.is_error, Some(true));
    no_report(&refused);
    receipts.push(refused.serializable.output);
    runtime.on_turn_aborted().await.unwrap();
    let directory =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/goal-outcomes-logs");
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(
        directory.join("completion-tool-role-receipts.json"),
        runtime_json::stringify_pretty(&runtime_codec::encode_value(&receipts).unwrap())
            .to_node_utf8(),
    )
    .unwrap();
    process.shutdown().await.unwrap();
}
