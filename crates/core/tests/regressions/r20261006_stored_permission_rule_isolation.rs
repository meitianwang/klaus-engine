//! Defect: isolated friend sessions inherited durable owner allow/ask grants.
//! Root cause: the stock permission engine always merged the shared stored layer.
//! Fixed by: 2026-10-06 AI friends stored-permission isolation work item.

use klaus_agent_core::permission::durable::DurablePermissionLayer;
use klaus_agent_core::permission::engine::{
    PermissionDecision, PermissionEngine, StaticPermissionEngine,
};
use klaus_agent_core::permission::rule_store::DurableRules;
use klaus_agent_core::permission::rules::parse_rule_string;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimePath, RuntimeString};
use klaus_agent_core::sandbox::process::ProcessSandbox;
use klaus_agent_core::schemas::config::{default_config, EngineConfig};
use klaus_agent_core::tools::builtins::edit::create_edit_tool;
use klaus_agent_core::tools::builtins::read::create_read_tool;
use klaus_agent_core::tools::builtins::write::create_write_tool;
use klaus_agent_core::tools::file_state::InMemoryFileStateCache;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::permission::{
    PermissionBehavior, PermissionContext, PermissionDestination, PermissionReason,
    PermissionUpdate,
};
use klaus_agent_core::types::tool::{Tool, ToolContext};
use serde_json::{json, Value};
use std::sync::Arc;

pub(super) struct Directory(pub(super) std::path::PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub(super) fn fixture() -> (Directory, ToolContext) {
    let directory =
        Directory(std::env::temp_dir().join(format!("stored-permission-{}", uuid::Uuid::new_v4())));
    let cwd = directory.0.join("work");
    std::fs::create_dir_all(&cwd).unwrap();
    std::fs::create_dir_all(directory.0.join("outside")).unwrap();
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let ctx = make_ctx(cwd.to_str().unwrap(), process);
    (directory, ctx)
}

fn input(value: Value) -> RuntimeJson {
    runtime_json::parse(&RuntimeString::from(value.to_string())).unwrap()
}

fn file_calls(directory: &Directory) -> Vec<(Arc<dyn Tool>, RuntimeJson)> {
    let path = directory.0.join("outside/private.txt");
    vec![
        (create_read_tool(), input(json!({"path": path}))),
        (
            create_write_tool(),
            input(json!({"path": path, "content": "change"})),
        ),
        (
            create_edit_tool(),
            input(json!({"path": path, "oldString": "old", "newString": "new"})),
        ),
    ]
}

fn broad_grants() -> DurableRules {
    DurableRules {
        allow: vec!["Read".into(), "Write".into(), "Edit".into()],
        ..Default::default()
    }
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

#[test]
fn stored_permission_rule_config_defaults_to_existing_behavior_and_roundtrips_opt_out() {
    let defaults: EngineConfig = serde_json::from_value(json!({})).unwrap();
    assert_eq!(
        serde_json::to_value(defaults).unwrap()["useStoredPermissionRules"],
        true
    );
    let isolated: EngineConfig =
        serde_json::from_value(json!({"useStoredPermissionRules": false})).unwrap();
    assert_eq!(
        serde_json::to_value(isolated).unwrap()["useStoredPermissionRules"],
        false
    );
}

#[tokio::test]
async fn stored_permission_rule_default_keeps_shared_grants_with_empty_config_allow() {
    let (directory, ctx) = fixture();
    let config = default_config();
    assert!(config.allow.is_empty());
    let durable = Arc::new(DurablePermissionLayer::default());
    let engine = StaticPermissionEngine::with_durable_rules(&config, durable.clone());
    for _ in 0..2 {
        durable.replace_stored(&broad_grants());
        for (tool, input) in file_calls(&directory) {
            assert!(
                matches!(
                    engine.evaluate(tool.as_ref(), &input, &ctx).await,
                    PermissionDecision::Allow {
                        reason: PermissionReason::Rule { .. },
                        ..
                    }
                ),
                "default sessions retain the stored {:?} grant",
                tool.name()
            );
        }
        durable.replace_stored(&DurableRules::default());
    }
}

#[tokio::test]
async fn stored_permission_rule_opt_out_survives_reload_and_pending_grants() {
    let (directory, ctx) = fixture();
    let config: EngineConfig =
        serde_json::from_value(json!({"useStoredPermissionRules": false})).unwrap();
    let durable = Arc::new(DurablePermissionLayer::default());
    let engine = StaticPermissionEngine::with_durable_rules(&config, durable.clone());
    for _ in 0..2 {
        durable.replace_stored(&broad_grants());
        for (tool, input) in file_calls(&directory) {
            let decision = engine.evaluate(tool.as_ref(), &input, &ctx).await;
            if tool.is_read_only(&input) {
                assert!(
                    matches!(decision, PermissionDecision::Allow {
                        reason: PermissionReason::Default,
                        ..
                    }),
                    "read-only access must use the baseline instead of a stored grant: {decision:?}"
                );
            } else {
                assert!(
                    matches!(
                        decision,
                        PermissionDecision::Ask {
                            reason: PermissionReason::Default,
                            ..
                        }
                    ),
                    "isolated sessions require approval for outside {:?}: {decision:?}",
                    tool.name()
                );
            }
        }
        durable.replace_stored(&DurableRules::default());
    }
    durable.grant_pending(PermissionUpdate::AddRules {
        rules: vec![parse_rule_string("Write").unwrap()],
        behavior: PermissionBehavior::Allow,
        destination: PermissionDestination::UserSettings,
    });
    let (write, outside) = file_calls(&directory).remove(1);
    assert!(matches!(
        engine.evaluate(write.as_ref(), &outside, &ctx).await,
        PermissionDecision::Ask { .. }
    ));
    let inside = input(json!({"path": directory.0.join("work/shared.txt")}));
    assert!(matches!(
        engine
            .evaluate(create_read_tool().as_ref(), &inside, &ctx)
            .await,
        PermissionDecision::Allow {
            reason: PermissionReason::Default,
            ..
        }
    ));
}

#[tokio::test]
async fn stored_permission_rule_opt_out_keeps_denies_and_explicit_session_grants() {
    let (directory, mut ctx) = fixture();
    let config: EngineConfig =
        serde_json::from_value(json!({"useStoredPermissionRules": false})).unwrap();
    let durable = Arc::new(DurablePermissionLayer::default());
    let engine = StaticPermissionEngine::with_durable_rules(&config, durable.clone());
    let inside = input(json!({"path": directory.0.join("work/shared.txt")}));
    durable.replace_stored(&DurableRules {
        ask: vec!["Read".into()],
        ..Default::default()
    });
    assert!(matches!(
        engine
            .evaluate(create_read_tool().as_ref(), &inside, &ctx)
            .await,
        PermissionDecision::Allow {
            reason: PermissionReason::Default,
            ..
        }
    ));
    durable.replace_stored(&DurableRules {
        deny: vec!["Read".into()],
        allow: vec!["Read".into()],
        ..Default::default()
    });
    ctx.permission
        .allow_rules
        .push(parse_rule_string("Read").unwrap());
    assert!(matches!(
        engine
            .evaluate(create_read_tool().as_ref(), &inside, &ctx)
            .await,
        PermissionDecision::Deny {
            reason: PermissionReason::Rule { .. },
            ..
        }
    ));
    durable.replace_stored(&DurableRules::default());
    assert!(matches!(
        engine
            .evaluate(create_read_tool().as_ref(), &inside, &ctx)
            .await,
        PermissionDecision::Allow {
            reason: PermissionReason::Rule { .. },
            ..
        }
    ));
}
