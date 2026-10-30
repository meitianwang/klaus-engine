//! Defect: a typed startup permission identity changed during config migration.
//! Root cause: typed StartSessionOptions values were encoded as legacy config presets.
//! Work item: 20261019 live and lazy runtime policy identity consistency.

use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::RuntimePolicyUpdate;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::schemas::config::SandboxWorkspaceWrite;
use klaus_agent_core::types::permission::{
    ApprovalPolicy, ApprovalsReviewer, ExternalPermissionMode, SandboxMode,
};
use serde_json::json;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

struct TestDirectory(std::path::PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "klaus-typed-permission-identity-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.0) {
            eprintln!("Failed to remove temporary test directory: {error}");
        }
    }
}

#[tokio::test]
async fn legacy_startup_inputs_preserve_explicit_canonical_policy_and_match_live_updates() {
    let root = TestDirectory::new();
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut options = agent_options(process.clone());
    options.cwd = Some(root.path().to_str().unwrap().to_owned());
    options.config = Some(json!({
        "prewarm":false,"autoMemory":false,"autoDream":false,"agentMemory":false,
        "memoryRecall":false,"memory":{"enabled":false},
        "approval_policy":"on-request","approvals_reviewer":"user",
        "sandbox_mode":"read-only","sandbox_workspace_write":{
            "writable_roots":[],"network_access":false,
            "exclude_tmpdir_env_var":false,"exclude_slash_tmp":false
        }
    }));
    let agent = Agent::new(options);
    for mode in [
        ExternalPermissionMode::BypassPermissions,
        ExternalPermissionMode::Auto,
        ExternalPermissionMode::AcceptEdits,
        ExternalPermissionMode::DontAsk,
    ] {
        let session = agent
            .try_start_session_async(StartSessionOptions {
                cwd: Some(root.path().to_str().unwrap().into()),
                user_config_dir: Some(root.path().to_str().unwrap().into()),
                permission_mode: Some(mode),
                network_allowed: Some(true),
                ..Default::default()
            })
            .await
            .unwrap();
        let started = session.runtime_policy_state();
        assert_eq!(
            started.sandbox_mode,
            Some(SandboxMode::ReadOnly),
            "typed {mode:?} cannot synthesize a filesystem preset"
        );
        assert_eq!(started.approval_policy, Some(ApprovalPolicy::OnRequest));
        assert_eq!(started.approvals_reviewer, Some(ApprovalsReviewer::User));
        assert_eq!(
            started.sandbox_workspace_write,
            SandboxWorkspaceWrite::default()
        );
        assert!(
            started.network_allowed,
            "the independent explicit network selection survives startup"
        );
        assert_eq!(started.generation, 0);

        let commit = session
            .set_runtime_policy(RuntimePolicyUpdate {
                approval_policy: None,
                sandbox_mode: None,
                approvals_reviewer: None,
                sandbox_workspace_write: None,
                network_allowed: None,
            })
            .unwrap();
        assert_eq!(commit.previous, started);
        let mut expected = started;
        expected.generation += 1;
        assert_eq!(
            commit.effective, expected,
            "startup and materialized updates preserve the same policy tuple"
        );
        assert_eq!(session.runtime_policy_state(), expected);
        session.shutdown_and_wait().await.unwrap();
    }
    process.shutdown().await.unwrap();
}
