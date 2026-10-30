//! Defect: a scoped task's execution permission could not follow the session's
//! live approval preset. The bound scope mode pinned the ask level for the
//! whole session, so switching the preset in a running conversation changed
//! nothing and the engine kept asking for actions the selected preset had
//! already approved inside the granted scope.
//! Root cause: the scoped permission gate compared the bound scope mode only.
//! Fixed by: 2026-10-28 friend execution permission work item: the scoped ask
//! ladder follows the live approval policy; the bound mode only seeds the
//! session's initial preset.

use super::r20261006_stored_permission_rule_isolation::{fixture, Directory};
use klaus_agent_core::permission::engine::{
    PermissionDecision, PermissionEngine, StaticPermissionEngine,
};
use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimePath, RuntimeString};
use klaus_agent_core::schemas::config::{default_config, TaskResourceMode, TaskResourceScope};
use klaus_agent_core::tools::builtins::{bash::create_bash_tool, write::create_write_tool};
use klaus_agent_core::types::permission::{ApprovalPolicy, ApprovalsReviewer, SandboxMode};
use klaus_agent_core::types::sandbox::{
    ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec,
};
use serde_json::json;
use std::sync::{Arc, Mutex};

fn input(value: serde_json::Value) -> RuntimeJson {
    runtime_json::parse(&RuntimeString::from(value.to_string())).unwrap()
}

#[derive(Default)]
struct SeatbeltSandbox(Mutex<Option<SandboxPolicy>>);
impl SandboxBackend for SeatbeltSandbox {
    fn kind(&self) -> SandboxKind {
        SandboxKind::Seatbelt
    }
    fn is_available(&self) -> bool {
        true
    }
    fn transform(&self, request: &ExecRequest, policy: &SandboxPolicy) -> SpawnSpec {
        *self.0.lock().unwrap() = Some(policy.clone());
        SpawnSpec {
            file: request.argv[0].clone(),
            args: request.argv[1..].to_vec(),
            cwd: request.cwd.clone(),
            env: request.env.clone().unwrap_or_default(),
        }
    }
}

fn definition(directory: &Directory, mode: TaskResourceMode) -> (TaskResourceScope, RuntimePath) {
    let read = directory.0.join("read");
    let write = directory.0.join("write");
    std::fs::create_dir_all(&read).unwrap();
    std::fs::create_dir_all(&write).unwrap();
    let read_root = std::fs::canonicalize(&read).unwrap();
    let write_root = std::fs::canonicalize(&write).unwrap();
    (
        TaskResourceScope {
            task_id: "friend-task-ladder".into(),
            mode,
            network_allowed: false,
            read_roots: vec![RuntimeString::from(read_root.to_str().unwrap())],
            write_roots: vec![RuntimeString::from(write_root.to_str().unwrap())],
        },
        RuntimePath::from(read_root.to_str().unwrap()),
    )
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn scoped_ask_ladder_follows_the_live_approval_policy() {
    let (directory, mut ctx) = fixture();
    ctx.sandbox = Arc::new(SeatbeltSandbox::default());
    // The bound mode says manual: under the old gate the whole session would
    // stay manual no matter what the session policy says.
    let (scope, read_root) = definition(&directory, TaskResourceMode::Manual);
    ctx.cwd = read_root;
    ctx.sandbox_mode = Some(SandboxMode::WorkspaceWrite);
    let mut config = default_config();
    config.task_resource_scope = Some(scope);
    let engine = StaticPermissionEngine::new(&config);
    let bash = create_bash_tool();
    let write = create_write_tool();
    let bash_input = input(json!({"command": "ls"}));
    let write_root = std::fs::canonicalize(directory.0.join("write")).unwrap();
    let write_input = input(json!({"path": write_root.join("note.txt"), "content": "note"}));
    // Request-approval preset: shell and edits both ask inside the scope.
    ctx.approval_policy = Some(ApprovalPolicy::OnRequest);
    ctx.approvals_reviewer = Some(ApprovalsReviewer::User);
    assert!(
        matches!(
            engine.evaluate(bash.as_ref(), &bash_input, &ctx).await,
            PermissionDecision::Ask { .. }
        ),
        "a manual ladder must ask for shell commands"
    );
    assert!(
        matches!(
            engine.evaluate(write.as_ref(), &write_input, &ctx).await,
            PermissionDecision::Ask { .. }
        ),
        "a manual ladder must ask for edits"
    );
    // Automatic-review preset: edits inside the write roots are approved while
    // shell commands still ask.
    ctx.approvals_reviewer = Some(ApprovalsReviewer::Automatic);
    assert!(
        matches!(
            engine.evaluate(write.as_ref(), &write_input, &ctx).await,
            PermissionDecision::Allow { .. }
        ),
        "an automatic-review ladder approves in-scope edits"
    );
    assert!(
        matches!(
            engine.evaluate(bash.as_ref(), &bash_input, &ctx).await,
            PermissionDecision::Ask { .. }
        ),
        "an automatic-review ladder still asks for shell commands"
    );
    // Bypass preset: everything inside the granted scope is approved.
    ctx.approval_policy = Some(ApprovalPolicy::Never);
    ctx.approvals_reviewer = Some(ApprovalsReviewer::User);
    assert!(
        matches!(
            engine.evaluate(bash.as_ref(), &bash_input, &ctx).await,
            PermissionDecision::Allow { .. }
        ),
        "a bypass ladder approves in-scope shell commands"
    );
    assert!(
        matches!(
            engine.evaluate(write.as_ref(), &write_input, &ctx).await,
            PermissionDecision::Allow { .. }
        ),
        "a bypass ladder approves in-scope edits"
    );
}
