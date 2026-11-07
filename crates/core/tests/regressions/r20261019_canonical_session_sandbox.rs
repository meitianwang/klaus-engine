//! Defect: a compatibility permission preset could reopen direct file writes.
//! Root cause: the tool context applied the preset before its filesystem policy.
//! Work item: 20261019 canonical session sandbox execution.

use super::r20261018_independent_runtime_policy::{write_step, Directory, PublicRig};
use klaus_agent_core::engine::session::RuntimePolicyUpdate;
use klaus_agent_core::tools::builtins::write::create_write_tool;
use klaus_agent_core::types::permission::SandboxMode;

#[tokio::test]
async fn canonical_read_only_cannot_be_weakened_by_a_compatibility_permission_mode() {
    let directory = Directory::new();
    let workspace = directory.child("workspace");
    let inside = workspace.join("unapproved.txt");
    let outside = directory.child("outside").join("unapproved.txt");
    let rig = PublicRig::new(
        &workspace,
        &directory.0.join("config"),
        "read-only",
        "never",
        vec![create_write_tool()],
        vec![
            write_step(&inside, "blocked"),
            write_step(&outside, "blocked"),
        ],
        Vec::new(),
    );
    rig.session
        .set_runtime_policy(RuntimePolicyUpdate {
            approval_policy: None,
            sandbox_mode: None,
            approvals_reviewer: None,

            sandbox_workspace_write: None,
            network_allowed: Some(true),
        })
        .unwrap();
    let results = rig.run().await;
    assert_eq!(results.len(), 2);
    assert!(results.values().all(|is_error| *is_error));
    assert!(!inside.exists());
    assert!(!outside.exists());
    let policy = rig.session.runtime_policy_state();
    assert_eq!(policy.sandbox_mode, Some(SandboxMode::ReadOnly));
    assert!(
        policy.network_allowed,
        "read-only filesystem access does not revoke an explicit network selection"
    );
    rig.finish().await;
}

#[tokio::test]
async fn readonly_live_workspace_table_update_preserves_independent_network_selection() {
    let directory = Directory::new();
    let workspace = directory.child("workspace");
    let extra = directory.child("extra");
    let rig = PublicRig::new(
        &workspace,
        &directory.0.join("config"),
        "read-only",
        "never",
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let update = |table, network| RuntimePolicyUpdate {
        approval_policy: None,
        sandbox_mode: Some(SandboxMode::ReadOnly),
        approvals_reviewer: None,

        sandbox_workspace_write: table,
        network_allowed: network,
    };
    rig.session
        .set_runtime_policy(update(None, Some(true)))
        .unwrap();
    rig.session
        .set_runtime_policy(update(
            Some(klaus_agent_core::schemas::config::SandboxWorkspaceWrite {
                writable_roots: vec![extra.to_str().unwrap().into()],
                ..Default::default()
            }),
            None,
        ))
        .unwrap();
    let policy = rig.session.runtime_policy_state();
    assert!(policy.network_allowed, "workspace resource updates cannot revoke a read-only session's independent network selection");
    assert_eq!(policy.sandbox_mode, Some(SandboxMode::ReadOnly));
    assert_eq!(
        policy.sandbox_workspace_write.writable_roots,
        vec![klaus_agent_core::runtime_values::RuntimeString::from(
            extra.to_str().unwrap()
        )]
    );
    rig.finish().await;
}
