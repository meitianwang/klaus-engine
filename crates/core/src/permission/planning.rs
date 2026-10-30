//! The admitted planning effect ceiling is independent of ordinary permissions.

use crate::fs::node_path::resolve_runtime;
use crate::permission::bash::decision::{
    evaluate_bash_command, BashDecision, BashDecisionOptions, BashRuleSets,
};
use crate::permission::bash::readonly::cwd_guard::is_cwd_bare_git_repo;
use crate::permission::bash::readonly::is_read_only_command;
use crate::permission::bash::split::extract_output_redirections;
use crate::permission::engine::{
    permission_prompt_path, permission_prompt_text, raw_path_values, resolve_raw_path,
    PermissionDecision,
};
use crate::permission::PLAN_MODE_READONLY_ENFORCED_KEY;
use crate::runtime_values::{RuntimeJson, RuntimePath, RuntimeString};
use crate::types::json::Json;
use crate::types::permission::{PermissionMode, PermissionReason};
use crate::types::sandbox::SandboxKind;
use crate::types::tool::{PermissionPathKind, PlanningToolCapability, Tool, ToolContext};

fn deny() -> PermissionDecision {
    PermissionDecision::Deny {
        message: "Plan mode is read-only; no mutating actions are permitted.".into(),
        reason: PermissionReason::Mode {
            mode: PermissionMode::Plan,
        },
    }
}

/// Recheck the exact arguments, including permission and approval rewrites.
/// This never consults mutable desired mode or converts ordinary policy to Plan.
pub(crate) async fn enforce(
    tool: &dyn Tool,
    input: &Json,
    ctx: &ToolContext,
) -> Result<Json, PermissionDecision> {
    if !ctx.planning_read_only {
        return Ok(input.clone());
    }
    if tool.is_goal_state_control() || tool.is_read_only(input) {
        return Ok(input.clone());
    }
    match tool.planning_capability(input) {
        PlanningToolCapability::Research => return Ok(input.clone()),
        PlanningToolCapability::Shell => {
            let Some(command) = tool.to_command_string(input) else {
                return Err(deny());
            };
            let command = permission_prompt_text(&command);
            let cwd = match input.get("cwd").and_then(RuntimeJson::as_str) {
                Some(path) if !path.is_empty() => {
                    resolve_runtime(&ctx.cwd, &[RuntimePath::from_runtime_string(path.clone())])
                        .map_err(|_| deny())?
                }
                _ => ctx.cwd.clone(),
            };
            let cwd_host = permission_prompt_path(&cwd);
            let redirects = extract_output_redirections(&command);
            let proven_read_only = redirects.redirections.is_empty()
                && !redirects.has_dangerous_redirection
                && matches!(
                    evaluate_bash_command(
                        &command,
                        &BashRuleSets::default(),
                        &BashDecisionOptions {
                            cwd: cwd_host.clone(),
                            writable_roots: Vec::new(),
                            home_dir: std::env::var("HOME").ok(),
                            is_read_only_subcommand: Some(Box::new(is_read_only_command)),
                            is_cwd_bare_repo: Some(Box::new(move || is_cwd_bare_git_repo(
                                &cwd_host
                            ))),
                        },
                    ),
                    BashDecision::Allow { .. }
                );
            let mut enforced = input.clone();
            let Some(object) = enforced.as_object_mut() else {
                return Err(deny());
            };
            // An input rewrite cannot retain a stale authorization marker.
            object.remove(PLAN_MODE_READONLY_ENFORCED_KEY);
            if !proven_read_only {
                if ctx.sandbox.kind() == SandboxKind::Process {
                    return Err(deny());
                }
                object.insert(
                    RuntimeString::from(PLAN_MODE_READONLY_ENFORCED_KEY),
                    RuntimeJson::Bool(true),
                );
            }
            return Ok(enforced);
        }
        PlanningToolCapability::None => {}
    }
    // Only an already bound document identity grants this exception. The path
    // class alone cannot authorize another file or a multi-target rewrite.
    if tool.permission_path_kind() == Some(PermissionPathKind::Edit) {
        if let Some(plan) = &ctx.plan_file {
            let bound =
                resolve_runtime(&ctx.cwd, std::slice::from_ref(&plan.path)).map_err(|_| deny())?;
            let paths = raw_path_values(tool, input, None);
            if !paths.is_empty()
                && paths
                    .iter()
                    .all(|path| resolve_raw_path(&ctx.cwd, path).is_ok_and(|path| path == bound))
            {
                return Ok(input.clone());
            }
        }
    }
    Err(deny())
}

/// Preserve ordinary denial/failure and ordinary prompts for allowed effects;
/// suppress any prompt whose proposed effect the planning ceiling forbids.
pub(crate) async fn intersect(
    tool: &dyn Tool,
    ctx: &ToolContext,
    mut decision: PermissionDecision,
) -> PermissionDecision {
    let input = match &mut decision {
        PermissionDecision::Allow { input, .. } | PermissionDecision::Ask { input, .. } => input,
        PermissionDecision::Deny { .. } | PermissionDecision::Fatal { .. } => return decision,
    };
    match enforce(tool, input, ctx).await {
        Ok(enforced) => *input = enforced,
        Err(denied) => return denied,
    }
    decision
}
