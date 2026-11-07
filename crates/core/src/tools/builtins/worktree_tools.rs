//! `EnterWorktree` / `ExitWorktree` — switch this session's working directory
//! into an isolated git worktree and back. The cwd switch is per-Session via
//! `ctx.worktree` (no process-wide chdir), so subsequent tool calls in the turn
//! run inside the worktree while the host process is untouched. Present only
//! when the host wires worktree management.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use crate::runtime_values::{RuntimeJson, RuntimeObject, RuntimeString};
use crate::tools::build_tool::text_result;
use crate::types::errors::ToolError;
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{
    Tool, ToolContext, ToolProgress, ToolResult, ValidationResult, WorktreeEnterResult,
    WorktreeExitAction, WorktreeExitResult,
};
use crate::worktree::worktree::{count_worktree_changes, validate_worktree_slug};

// ---- EnterWorktree ---------------------------------------------------------

/// Build the `EnterWorktree` input JSON Schema: a single optional `name` string.
fn enter_input_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "name": {
                "type": "string",
                "description": "Optional name for the worktree (and its branch). Letters, digits, dots, dashes, underscores, and \"/\" separators only; omit for an auto-generated name."
            }
        },
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .unwrap_or_default()
}

/// A `{ ok: false, message }` error result. The output payload carries only the
/// `ok` flag and the message (no path fields).
fn enter_error(message: String) -> ToolResult {
    let mut output = RuntimeObject::new();
    output.insert("ok", RuntimeJson::Bool(false));
    output.insert("message", RuntimeJson::String(message.clone().into()));
    text_result(RuntimeJson::Object(output), message, true)
}

/// Create (or resume) an isolated git worktree and switch this session into it.
pub struct EnterWorktreeTool {
    input_schema: JsonSchema,
}

/// Construct the `EnterWorktree` tool.
pub fn create_enter_worktree_tool() -> Arc<dyn Tool> {
    Arc::new(EnterWorktreeTool {
        input_schema: enter_input_schema(),
    })
}

#[async_trait]
impl Tool for EnterWorktreeTool {
    fn static_name(&self) -> &str {
        "EnterWorktree"
    }

    fn static_description(&self) -> &str {
        "Create (or resume) an isolated git worktree and switch this session into it, so changes are made on a separate branch without touching the main checkout. Exit later with ExitWorktree."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn static_search_hint(&self) -> Option<&str> {
        Some("work in an isolated git worktree on a separate branch")
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

    async fn validate_input(&self, input: &Json, _ctx: &ToolContext) -> Option<ValidationResult> {
        if let Some(name) = input.get("name").and_then(Json::as_str) {
            let name = String::from_utf8(name.to_node_utf8())
                .expect("Node UTF-8 worktree-name projection is valid UTF-8");
            let trimmed = name.trim();
            if !trimmed.is_empty() {
                if let Err(message) = validate_worktree_slug(trimmed) {
                    return Some(ValidationResult::Err {
                        message,
                        error_code: None,
                    });
                }
            }
        }
        Some(ValidationResult::Ok)
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let wt = match &ctx.worktree {
            Some(wt) => wt,
            None => {
                return Ok(enter_error(
                    "Worktree management is not enabled for this session.".to_string(),
                ))
            }
        };
        let name = input.get("name").and_then(Json::as_str).cloned();
        let WorktreeEnterResult {
            worktree_path,
            worktree_branch,
            message,
        } = wt.enter(name).await;

        let text = message
            .concat(&RuntimeString::from(" Working directory is now "))
            .concat(worktree_path.as_runtime_string())
            .concat(&RuntimeString::from("."));
        let mut output = RuntimeObject::new();
        output.insert("ok", RuntimeJson::Bool(true));
        output.insert(
            "worktreePath",
            RuntimeJson::String(worktree_path.into_runtime_string()),
        );
        if let Some(branch) = worktree_branch {
            output.insert("worktreeBranch", RuntimeJson::String(branch));
        }
        output.insert("message", RuntimeJson::String(message));
        Ok(text_result(RuntimeJson::Object(output), text, false))
    }
}

// ---- ExitWorktree ----------------------------------------------------------

/// Build the `ExitWorktree` input JSON Schema: a required `action` enum and a
/// `discard_changes` boolean (defaulting to false, still listed as required).
fn exit_input_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": ["keep", "remove"],
                "description": "Whether to keep the worktree on disk after leaving, or remove it and its branch."
            },
            "discard_changes": {
                "type": "boolean",
                "default": false,
                "description": "When removing, allow removal even though the worktree has uncommitted changes or new commits (they will be lost). Ignored for keep."
            }
        },
        "required": ["action", "discard_changes"],
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .unwrap_or_default()
}

/// A `{ ok: false, message }` error result for the exit tool.
fn exit_error(message: String) -> ToolResult {
    let mut output = RuntimeObject::new();
    output.insert("ok", RuntimeJson::Bool(false));
    output.insert("message", RuntimeJson::String(message.clone().into()));
    text_result(RuntimeJson::Object(output), message, true)
}

/// The wire spelling of an exit action.
fn action_str(action: WorktreeExitAction) -> &'static str {
    match action {
        WorktreeExitAction::Keep => "keep",
        WorktreeExitAction::Remove => "remove",
    }
}

/// Leave the current git worktree and restore the original working directory.
pub struct ExitWorktreeTool {
    input_schema: JsonSchema,
}

/// Construct the `ExitWorktree` tool.
pub fn create_exit_worktree_tool() -> Arc<dyn Tool> {
    Arc::new(ExitWorktreeTool {
        input_schema: exit_input_schema(),
    })
}

#[async_trait]
impl Tool for ExitWorktreeTool {
    fn static_name(&self) -> &str {
        "ExitWorktree"
    }

    fn static_description(&self) -> &str {
        "Leave the current git worktree and restore the original working directory. Optionally remove the worktree and its branch."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn static_search_hint(&self) -> Option<&str> {
        Some("leave the isolated git worktree and restore the original directory")
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

    fn is_destructive(&self, input: &Json) -> Option<bool> {
        Some(
            input
                .get("action")
                .and_then(Json::as_str)
                .is_some_and(|action| action == "remove"),
        )
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let ctl = match &ctx.worktree {
            Some(ctl) => ctl,
            None => {
                return Ok(exit_error(
                    "Worktree management is not enabled for this session.".to_string(),
                ))
            }
        };
        let active = match ctl.current() {
            Some(active) => active,
            None => {
                return Ok(exit_error(
                    "This session is not currently in a worktree, so there is nothing to exit."
                        .to_string(),
                ))
            }
        };

        let is_remove = input
            .get("action")
            .and_then(Json::as_str)
            .is_some_and(|action| action == "remove");
        let discard_changes = input
            .get("discard_changes")
            .and_then(Json::as_bool)
            .unwrap_or(false);

        // Removal safety gate: refuse to destroy a worktree that holds work,
        // unless the caller explicitly opts into discarding it.
        // `count_worktree_changes` returns None when the state can't be
        // determined — treat that as unsafe (fail-closed) rather than risk a
        // silent data loss.
        if is_remove && !discard_changes {
            let changes = count_worktree_changes(
                &ctx.process_context,
                &active.worktree_path,
                active.original_head_commit.as_ref(),
            )
            .await;
            match changes {
                None => {
                    return Ok(exit_error(
                        "Cannot determine whether the worktree has unsaved work, so it will not be removed. Re-run with discard_changes: true to remove it anyway, or use action: \"keep\"."
                            .to_string(),
                    ))
                }
                Some(changes) => {
                    if changes.changed_files > 0 || changes.commits > 0 {
                        let mut parts: Vec<String> = Vec::new();
                        if changes.changed_files > 0 {
                            parts.push(format!("{} uncommitted file(s)", changes.changed_files));
                        }
                        if changes.commits > 0 {
                            parts.push(format!("{} new commit(s)", changes.commits));
                        }
                        return Ok(exit_error(format!(
                            "Worktree has {}. It was NOT removed. Commit or move that work first, re-run with discard_changes: true to discard it, or use action: \"keep\".",
                            parts.join(" and ")
                        )));
                    }
                }
            }
        }

        let action = if is_remove {
            WorktreeExitAction::Remove
        } else {
            WorktreeExitAction::Keep
        };
        let WorktreeExitResult {
            action: result_action,
            original_cwd,
            worktree_path,
            worktree_branch: _worktree_branch,
            message,
        } = ctl.exit(action).await;

        let text = message
            .concat(&RuntimeString::from(" Working directory restored to "))
            .concat(original_cwd.as_runtime_string())
            .concat(&RuntimeString::from("."));
        let mut output = RuntimeObject::new();
        output.insert("ok", RuntimeJson::Bool(true));
        output.insert(
            "action",
            RuntimeJson::String(action_str(result_action).into()),
        );
        output.insert(
            "originalCwd",
            RuntimeJson::String(original_cwd.into_runtime_string()),
        );
        output.insert(
            "worktreePath",
            RuntimeJson::String(worktree_path.into_runtime_string()),
        );
        output.insert("message", RuntimeJson::String(message));
        Ok(text_result(RuntimeJson::Object(output), text, false))
    }
}

/// Both worktree tools, included when the host enables worktree management.
pub fn create_worktree_tools() -> Vec<Arc<dyn Tool>> {
    vec![create_enter_worktree_tool(), create_exit_worktree_tool()]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_process::RuntimeEnv;
    use crate::runtime_values::{runtime_json, RuntimePath, RuntimeString};
    use crate::schemas::content::OutputBlock;
    use crate::types::abort::AbortSignal;
    use crate::types::events::ToolDomainEvent;
    use crate::types::file_state::{FileStateCache, FileStateEntry, RecordOptions};
    use crate::types::permission::PermissionContext;
    use crate::types::sandbox::{
        ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec,
    };
    use crate::types::tool::{ActiveWorktree, WorktreeController};
    use crate::worktree::worktree::get_or_create_worktree;
    use std::path::PathBuf;
    use std::process::Command as StdCommand;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex;

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_dir() -> PathBuf {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "ae-worktree-tool-{}-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn runtime_path(path: &std::path::Path) -> RuntimePath {
        crate::fs::node_path::runtime_path_from_native(path)
    }

    fn host_path(path: &RuntimePath) -> PathBuf {
        crate::fs::node_path::project_runtime_path(path, "worktree tool test path").unwrap()
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

    /// A mock worktree controller: `current()` reads a settable slot; `enter`
    /// and `exit` return canned results (with `exit` echoing the requested
    /// action and clearing the active slot).
    struct MockWorktree {
        active: Mutex<Option<ActiveWorktree>>,
        enter_result: WorktreeEnterResult,
        exit_result: WorktreeExitResult,
    }

    #[async_trait]
    impl WorktreeController for MockWorktree {
        fn current(&self) -> Option<ActiveWorktree> {
            self.active.lock().unwrap().clone()
        }
        async fn enter(&self, _name: Option<RuntimeString>) -> WorktreeEnterResult {
            self.enter_result.clone()
        }
        async fn exit(&self, action: WorktreeExitAction) -> WorktreeExitResult {
            let mut r = self.exit_result.clone();
            r.action = action;
            *self.active.lock().unwrap() = None;
            r
        }
    }

    fn base_ctx() -> ToolContext {
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
            emit: std::sync::Arc::new(|_e: ToolDomainEvent| Box::pin(async {})),
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

    fn git(args: &[&str], cwd: &std::path::Path) {
        StdCommand::new("git")
            .args(args)
            .current_dir(cwd)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .expect("git runs");
    }

    // ---- metadata + schema ---------------------------------------------------

    #[test]
    fn enter_exposes_expected_metadata_and_schema() {
        let tool = create_enter_worktree_tool();
        assert_eq!(tool.name(), RuntimeString::from("EnterWorktree"));
        assert_eq!(
            tool.description(),
            RuntimeString::from("Create (or resume) an isolated git worktree and switch this session into it, so changes are made on a separate branch without touching the main checkout. Exit later with ExitWorktree.")
        );
        assert_eq!(
            tool.search_hint(),
            Some(RuntimeString::from(
                "work in an isolated git worktree on a separate branch"
            ))
        );
        assert!(tool.is_enabled());
        let input = exact_json("{}");
        assert!(!tool.is_read_only(&input));
        assert!(!tool.is_concurrency_safe(&input));
        assert_eq!(tool.is_destructive(&input), Some(false));
        assert_eq!(
            tool.input_schema(),
            exact_json(
                r#"{"type":"object","properties":{"name":{"type":"string","description":"Optional name for the worktree (and its branch). Letters, digits, dots, dashes, underscores, and \"/\" separators only; omit for an auto-generated name."}},"additionalProperties":false}"#
            )
        );
    }

    #[test]
    fn exit_exposes_expected_metadata_and_schema() {
        let tool = create_exit_worktree_tool();
        assert_eq!(tool.name(), RuntimeString::from("ExitWorktree"));
        assert_eq!(
            tool.description(),
            RuntimeString::from("Leave the current git worktree and restore the original working directory. Optionally remove the worktree and its branch.")
        );
        assert_eq!(
            tool.search_hint(),
            Some(RuntimeString::from(
                "leave the isolated git worktree and restore the original directory"
            ))
        );
        assert!(!tool.is_read_only(&exact_json(r#"{"action":"keep"}"#)));
        assert!(!tool.is_concurrency_safe(&exact_json(r#"{"action":"keep"}"#)));
        // Destructive only when removing.
        assert_eq!(
            tool.is_destructive(&exact_json(r#"{"action":"remove"}"#)),
            Some(true)
        );
        assert_eq!(
            tool.is_destructive(&exact_json(r#"{"action":"keep"}"#)),
            Some(false)
        );
        assert_eq!(
            tool.input_schema(),
            exact_json(
                r#"{"type":"object","properties":{"action":{"type":"string","enum":["keep","remove"],"description":"Whether to keep the worktree on disk after leaving, or remove it and its branch."},"discard_changes":{"type":"boolean","default":false,"description":"When removing, allow removal even though the worktree has uncommitted changes or new commits (they will be lost). Ignored for keep."}},"required":["action","discard_changes"],"additionalProperties":false}"#
            )
        );
    }

    // ---- EnterWorktree validation --------------------------------------------

    #[tokio::test]
    async fn enter_validate_input_accepts_absent_empty_and_valid_names() {
        let tool = create_enter_worktree_tool();
        let ctx = base_ctx();
        assert_eq!(
            tool.validate_input(&exact_json("{}"), &ctx).await,
            Some(ValidationResult::Ok)
        );
        assert_eq!(
            tool.validate_input(&exact_json(r#"{"name":"   "}"#), &ctx)
                .await,
            Some(ValidationResult::Ok)
        );
        assert_eq!(
            tool.validate_input(&exact_json(r#"{"name":"user/feature"}"#), &ctx)
                .await,
            Some(ValidationResult::Ok)
        );
    }

    #[tokio::test]
    async fn enter_validate_input_rejects_a_bad_slug() {
        let tool = create_enter_worktree_tool();
        let ctx = base_ctx();
        let res = tool
            .validate_input(&exact_json(r#"{"name":"a b"}"#), &ctx)
            .await
            .unwrap();
        match res {
            ValidationResult::Err {
                message,
                error_code,
            } => {
                assert!(
                    message.contains("Invalid worktree name"),
                    "message: {message}"
                );
                assert_eq!(error_code, None);
            }
            ValidationResult::Ok => panic!("expected an error"),
            ValidationResult::Fatal { error } => {
                panic!("expected a model-visible validation error, got fatal: {error}")
            }
        }
    }

    // ---- EnterWorktree call --------------------------------------------------

    #[tokio::test]
    async fn enter_reports_disabled_when_no_controller() {
        let tool = create_enter_worktree_tool();
        let ctx = base_ctx();
        let res = tool.call(exact_json("{}"), &ctx, None).await.unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            res.serializable.output,
            exact_json(
                r#"{"ok":false,"message":"Worktree management is not enabled for this session."}"#
            )
        );
        assert_eq!(
            text_of(&res),
            RuntimeString::from("Worktree management is not enabled for this session.")
        );
    }

    #[tokio::test]
    async fn enter_switches_into_the_worktree() {
        let tool = create_enter_worktree_tool();
        let mut ctx = base_ctx();
        ctx.worktree = Some(Arc::new(MockWorktree {
            active: Mutex::new(None),
            enter_result: WorktreeEnterResult {
                worktree_path: "/repo/.klaus/worktrees/feat".into(),
                worktree_branch: Some("worktree-feat".into()),
                message: "Created worktree \"feat\".".into(),
            },
            exit_result: WorktreeExitResult {
                action: WorktreeExitAction::Keep,
                original_cwd: "/repo".into(),
                worktree_path: "/repo/.klaus/worktrees/feat".into(),
                worktree_branch: None,
                message: RuntimeString::default(),
            },
        }));
        let res = tool
            .call(exact_json(r#"{"name":"feat"}"#), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            res.serializable.output,
            exact_json(
                r#"{"ok":true,"worktreePath":"/repo/.klaus/worktrees/feat","worktreeBranch":"worktree-feat","message":"Created worktree \"feat\"."}"#
            )
        );
        assert_eq!(
            text_of(&res),
            RuntimeString::from(
                "Created worktree \"feat\". Working directory is now /repo/.klaus/worktrees/feat."
            )
        );
    }

    // ---- ExitWorktree call ---------------------------------------------------

    #[tokio::test]
    async fn exit_reports_disabled_when_no_controller() {
        let tool = create_exit_worktree_tool();
        let ctx = base_ctx();
        let res = tool
            .call(exact_json(r#"{"action":"keep"}"#), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            text_of(&res),
            RuntimeString::from("Worktree management is not enabled for this session.")
        );
    }

    #[tokio::test]
    async fn exit_reports_when_not_in_a_worktree() {
        let tool = create_exit_worktree_tool();
        let mut ctx = base_ctx();
        ctx.worktree = Some(Arc::new(MockWorktree {
            active: Mutex::new(None),
            enter_result: WorktreeEnterResult {
                worktree_path: RuntimePath::default(),
                worktree_branch: None,
                message: RuntimeString::default(),
            },
            exit_result: WorktreeExitResult {
                action: WorktreeExitAction::Keep,
                original_cwd: "/repo".into(),
                worktree_path: RuntimePath::default(),
                worktree_branch: None,
                message: RuntimeString::default(),
            },
        }));
        let res = tool
            .call(exact_json(r#"{"action":"keep"}"#), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            text_of(&res),
            RuntimeString::from(
                "This session is not currently in a worktree, so there is nothing to exit."
            )
        );
    }

    #[tokio::test]
    async fn exit_keeps_and_restores_the_original_directory() {
        let tool = create_exit_worktree_tool();
        let mut ctx = base_ctx();
        ctx.worktree = Some(Arc::new(MockWorktree {
            active: Mutex::new(Some(ActiveWorktree {
                worktree_path: "/repo/.klaus/worktrees/feat".into(),
                worktree_name: "feat".into(),
                worktree_branch: Some("worktree-feat".into()),
                original_head_commit: Some("deadbeef".into()),
            })),
            enter_result: WorktreeEnterResult {
                worktree_path: RuntimePath::default(),
                worktree_branch: None,
                message: RuntimeString::default(),
            },
            exit_result: WorktreeExitResult {
                action: WorktreeExitAction::Keep,
                original_cwd: "/repo".into(),
                worktree_path: "/repo/.klaus/worktrees/feat".into(),
                worktree_branch: Some("worktree-feat".into()),
                message: "Left worktree \"feat\" (kept on disk).".into(),
            },
        }));
        let res = tool
            .call(
                exact_json(r#"{"action":"keep","discard_changes":false}"#),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            res.serializable.output,
            exact_json(
                r#"{"ok":true,"action":"keep","originalCwd":"/repo","worktreePath":"/repo/.klaus/worktrees/feat","message":"Left worktree \"feat\" (kept on disk)."}"#
            )
        );
        assert_eq!(
            text_of(&res),
            RuntimeString::from(
                "Left worktree \"feat\" (kept on disk). Working directory restored to /repo."
            )
        );
    }

    #[tokio::test]
    async fn exit_remove_with_discard_skips_the_change_gate() {
        // A non-git worktree_path would make the gate fail-closed; discard_changes
        // bypasses the gate entirely, so removal proceeds.
        let dir = temp_dir();
        let path = dir.to_string_lossy().into_owned();
        let tool = create_exit_worktree_tool();
        let mut ctx = base_ctx();
        ctx.worktree = Some(Arc::new(MockWorktree {
            active: Mutex::new(Some(ActiveWorktree {
                worktree_path: RuntimePath::from(path.clone()),
                worktree_name: "feat".into(),
                worktree_branch: Some("worktree-feat".into()),
                original_head_commit: None,
            })),
            enter_result: WorktreeEnterResult {
                worktree_path: RuntimePath::default(),
                worktree_branch: None,
                message: RuntimeString::default(),
            },
            exit_result: WorktreeExitResult {
                action: WorktreeExitAction::Keep,
                original_cwd: "/repo".into(),
                worktree_path: RuntimePath::from(path.clone()),
                worktree_branch: Some("worktree-feat".into()),
                message: "Removed worktree \"feat\".".into(),
            },
        }));
        let res = tool
            .call(
                exact_json(r#"{"action":"remove","discard_changes":true}"#),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            res.serializable
                .output
                .get("action")
                .and_then(RuntimeJson::as_string),
            Some(&RuntimeString::from("remove"))
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn exit_remove_fails_closed_when_state_is_unknown() {
        // A non-git path: count_worktree_changes returns None → fail-closed refusal.
        let dir = temp_dir();
        let path = dir.to_string_lossy().into_owned();
        let tool = create_exit_worktree_tool();
        let mut ctx = base_ctx();
        ctx.worktree = Some(Arc::new(MockWorktree {
            active: Mutex::new(Some(ActiveWorktree {
                worktree_path: RuntimePath::from(path),
                worktree_name: "feat".into(),
                worktree_branch: Some("worktree-feat".into()),
                original_head_commit: Some("deadbeef".into()),
            })),
            enter_result: WorktreeEnterResult {
                worktree_path: RuntimePath::default(),
                worktree_branch: None,
                message: RuntimeString::default(),
            },
            exit_result: WorktreeExitResult {
                action: WorktreeExitAction::Remove,
                original_cwd: "/repo".into(),
                worktree_path: RuntimePath::default(),
                worktree_branch: None,
                message: RuntimeString::default(),
            },
        }));
        let res = tool
            .call(
                exact_json(r#"{"action":"remove","discard_changes":false}"#),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            text_of(&res),
            RuntimeString::from("Cannot determine whether the worktree has unsaved work, so it will not be removed. Re-run with discard_changes: true to remove it anyway, or use action: \"keep\".")
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn exit_remove_refuses_a_dirty_worktree() {
        let _external = crate::test_support::external_process_lock().await;
        // A real git repo + worktree with an uncommitted file.
        let dir = temp_dir();
        git(&["init", "-q"], &dir);
        git(&["checkout", "-q", "-b", "work"], &dir);
        git(&["config", "user.email", "a@b.c"], &dir);
        git(&["config", "user.name", "T"], &dir);
        std::fs::write(dir.join("a.txt"), "hello\n").unwrap();
        git(&["add", "."], &dir);
        git(&["commit", "-q", "-m", "first"], &dir);
        let root = runtime_path(&dir);
        let mut ctx = base_ctx();
        let created = get_or_create_worktree(&ctx.process_context, &root, "dirty", None)
            .await
            .unwrap();
        // Make the worktree dirty.
        std::fs::write(host_path(&created.worktree_path).join("new.txt"), "x\n").unwrap();

        let tool = create_exit_worktree_tool();
        ctx.worktree = Some(Arc::new(MockWorktree {
            active: Mutex::new(Some(ActiveWorktree {
                worktree_path: created.worktree_path.clone(),
                worktree_name: "dirty".into(),
                worktree_branch: Some(created.worktree_branch.clone()),
                original_head_commit: created.head_commit.clone(),
            })),
            enter_result: WorktreeEnterResult {
                worktree_path: RuntimePath::default(),
                worktree_branch: None,
                message: RuntimeString::default(),
            },
            exit_result: WorktreeExitResult {
                action: WorktreeExitAction::Remove,
                original_cwd: root.clone(),
                worktree_path: created.worktree_path.clone(),
                worktree_branch: None,
                message: RuntimeString::default(),
            },
        }));
        let res = tool
            .call(
                exact_json(r#"{"action":"remove","discard_changes":false}"#),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        let text = text_of(&res);
        assert!(text.contains("It was NOT removed"), "text: {text:?}");
        assert!(text.contains("uncommitted file(s)"), "text: {text:?}");
        std::fs::remove_dir_all(&dir).ok();
    }
}
