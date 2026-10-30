//! Defect: full-access memory edits failed because approval requests were disabled.
//! Root cause: sensitive-path prompts ran before unrestricted filesystem policy.
//! Work item: 20261021 full-access sensitive edits.

use super::r20261015_browser_diagnostics_and_targets::make_ctx;
use super::r20261018_independent_runtime_policy::{edit_step, write_step, Directory, PublicRig};
use super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor;
use klaus_agent_core::permission::engine::{
    PermissionDecision, PermissionEngine, StaticPermissionEngine,
};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeString};
use klaus_agent_core::schemas::config::default_config;
use klaus_agent_core::tools::builtins::{edit::create_edit_tool, write::create_write_tool};
use klaus_agent_core::types::permission::{
    ApprovalPolicy, GranularApprovalConfig, PermissionMode, PermissionReason, SandboxMode,
};

async fn public_sensitive_memory_operations(approval: &str) {
    let directory = Directory::new();
    let workspace = directory.child("workspace");
    let memory = directory.child(".klaus/projects/project/memory");
    let written = memory.join("new-note.md");
    let edited = memory.join("existing-note.md");
    std::fs::write(&edited, "roots: 19\n").unwrap();
    let rig = PublicRig::new(
        &workspace,
        &directory.0.join("config"),
        "danger-full-access",
        approval,
        vec![create_write_tool(), create_edit_tool()],
        vec![
            write_step(&written, "a new memory\n"),
            edit_step(&edited, "roots: 19", "roots: 20"),
        ],
        Vec::new(),
    );
    let results = rig.run().await;
    let actual_written = std::fs::read_to_string(&written).ok();
    let actual_edited = std::fs::read_to_string(&edited).unwrap();
    rig.finish().await;
    assert_eq!(results.len(), 2);
    assert!(
        results.values().all(|is_error| !is_error),
        "both real file tools must execute without requesting approval (approval={approval}): {results:?}"
    );
    assert_eq!(actual_written.as_deref(), Some("a new memory\n"));
    assert_eq!(actual_edited, "roots: 20\n");
}

#[tokio::test]
async fn public_full_access_writes_and_edits_sensitive_memory_without_approval() {
    public_sensitive_memory_operations("never").await;
}

#[tokio::test]
async fn public_full_access_with_on_request_writes_and_edits_sensitive_memory_without_prompt() {
    public_sensitive_memory_operations("on-request").await;
}

#[tokio::test]
async fn both_permission_entry_points_preserve_full_access_for_sensitive_edits() {
    let directory = Directory::new();
    let workspace = directory.child("workspace");
    let memory = directory.child(".klaus/projects/project/memory");
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut ctx = make_ctx(workspace.to_str().unwrap(), process.clone());
    ctx.sandbox_mode = Some(SandboxMode::DangerFullAccess);
    let evaluator = StaticPermissionEngine::new(&default_config());
    for approval in [
        ApprovalPolicy::Never,
        ApprovalPolicy::OnRequest,
        ApprovalPolicy::Granular(GranularApprovalConfig {
            sandbox_approval: false,
            rules: false,
            skill_approval: false,
            request_permissions: false,
            mcp_elicitations: false,
        }),
    ] {
        ctx.approval_policy = Some(approval);
        for tool in [create_write_tool(), create_edit_tool()] {
            let input = runtime_json::parse(&RuntimeString::from(
                serde_json::json!({
                    "path": memory.join("note.md").to_str().unwrap(),
                    "content": "new content",
                    "oldString": "old content",
                    "newString": "new content"
                })
                .to_string(),
            ))
            .unwrap();
            for exclude_prefix_grants in [false, true] {
                let decision = if exclude_prefix_grants {
                    evaluator
                        .evaluate_without_allow_prefix_rules(tool.as_ref(), &input, &ctx)
                        .await
                } else {
                    evaluator.evaluate(tool.as_ref(), &input, &ctx).await
                };
                assert!(
                matches!(decision, PermissionDecision::Allow { .. }),
                    "{:?} with approval={approval:?} exclude_prefix_grants={exclude_prefix_grants}: {decision:?}",
                tool.name()
            );
            }
        }
    }
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn untrusted_approval_policy_still_reviews_sensitive_full_access_edits() {
    let directory = Directory::new();
    let workspace = directory.child("workspace");
    let memory = directory.child(".klaus/projects/project/memory");
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut ctx = make_ctx(workspace.to_str().unwrap(), process.clone());
    ctx.sandbox_mode = Some(SandboxMode::DangerFullAccess);
    ctx.approval_policy = Some(ApprovalPolicy::UnlessTrusted);
    let evaluator = StaticPermissionEngine::new(&default_config());
    let input = runtime_json::parse(&RuntimeString::from(
        serde_json::json!({"path": memory.join("note.md").to_str().unwrap()}).to_string(),
    ))
    .unwrap();
    for tool in [create_write_tool(), create_edit_tool()] {
        for exclude_prefix_grants in [false, true] {
            let decision = if exclude_prefix_grants {
                evaluator
                    .evaluate_without_allow_prefix_rules(tool.as_ref(), &input, &ctx)
                    .await
            } else {
                evaluator.evaluate(tool.as_ref(), &input, &ctx).await
            };
            assert!(
                matches!(decision, PermissionDecision::Ask { .. }),
                "untrusted-action review remains required: {decision:?}"
            );
        }
    }
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn planning_remains_read_only_with_a_full_filesystem_policy() {
    let directory = Directory::new();
    let workspace = directory.child("workspace");
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut ctx = make_ctx(workspace.to_str().unwrap(), process.clone());
    ctx.planning_read_only = true;
    ctx.sandbox_mode = Some(SandboxMode::DangerFullAccess);
    let evaluator = StaticPermissionEngine::new(&default_config());
    let input = runtime_json::parse(&RuntimeString::from(
        serde_json::json!({"path": workspace.join("ordinary.txt").to_str().unwrap()}).to_string(),
    ))
    .unwrap();
    for approval in [ApprovalPolicy::Never, ApprovalPolicy::OnRequest] {
        ctx.approval_policy = Some(approval);
        for tool in [create_write_tool(), create_edit_tool()] {
            for exclude_prefix_grants in [false, true] {
                let decision = if exclude_prefix_grants {
                    evaluator
                        .evaluate_without_allow_prefix_rules(tool.as_ref(), &input, &ctx)
                        .await
                } else {
                    evaluator.evaluate(tool.as_ref(), &input, &ctx).await
                };
                assert!(
                    matches!(
                        decision,
                        PermissionDecision::Deny {
                            reason: PermissionReason::Mode {
                                mode: PermissionMode::Plan
                            },
                            ..
                        }
                    ),
                    "planning cannot mutate files under {approval:?}: {decision:?}"
                );
            }
        }
    }
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn full_access_preserves_explicit_deny_and_ask_rules_for_both_entry_points() {
    let directory = Directory::new();
    let workspace = directory.child("workspace");
    let memory = directory.child(".klaus/projects/project/memory");
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut ctx = make_ctx(workspace.to_str().unwrap(), process.clone());
    ctx.sandbox_mode = Some(SandboxMode::DangerFullAccess);
    ctx.approval_policy = Some(ApprovalPolicy::Never);
    for ask in [false, true] {
        let mut config = default_config();
        let rules = vec!["Write".into(), "Edit".into()];
        if ask {
            config.ask = rules;
        } else {
            config.deny = rules;
        }
        let evaluator = StaticPermissionEngine::new(&config);
        let input = runtime_json::parse(&RuntimeString::from(
            serde_json::json!({"path": memory.join("note.md").to_str().unwrap()}).to_string(),
        ))
        .unwrap();
        for tool in [create_write_tool(), create_edit_tool()] {
            for exclude_prefix_grants in [false, true] {
                let decision = if exclude_prefix_grants {
                    evaluator
                        .evaluate_without_allow_prefix_rules(tool.as_ref(), &input, &ctx)
                        .await
                } else {
                    evaluator.evaluate(tool.as_ref(), &input, &ctx).await
                };
                let expected = if ask {
                    matches!(
                        decision,
                        PermissionDecision::Ask {
                            reason: PermissionReason::Rule { .. },
                            ..
                        }
                    )
                } else {
                    matches!(
                        decision,
                        PermissionDecision::Deny {
                            reason: PermissionReason::Rule { .. },
                            ..
                        }
                    )
                };
                assert!(
                    expected,
                    "{:?} ask={ask} exclude_prefix_grants={exclude_prefix_grants}: {decision:?}",
                    tool.name()
                );
            }
        }
    }
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn canonical_read_only_and_workspace_policies_limit_real_writes() {
    for sandbox in ["read-only", "workspace-write"] {
        let directory = Directory::new();
        let workspace = directory.child("workspace");
        let inside = workspace.join("inside.txt");
        let outside = directory.child("outside").join("outside.txt");
        let rig = PublicRig::new(
            &workspace,
            &directory.0.join("config"),
            sandbox,
            "never",
            vec![create_write_tool()],
            vec![
                write_step(&inside, "inside"),
                write_step(&outside, "outside"),
            ],
            Vec::new(),
        );
        let results = rig.run().await;
        let actual_inside = std::fs::read_to_string(&inside).ok();
        let outside_exists = outside.exists();
        rig.finish().await;
        assert_eq!(results.len(), 2);
        assert_eq!(
            results.get(&RuntimeString::from("policy-effect-call-0")),
            Some(&(sandbox == "read-only")),
            "{sandbox} retains its inside-workspace write boundary"
        );
        assert_eq!(
            results.get(&RuntimeString::from("policy-effect-call-1")),
            Some(&true),
            "{sandbox} rejects writes outside its writable roots"
        );
        assert_eq!(
            actual_inside.as_deref(),
            (sandbox == "workspace-write").then_some("inside")
        );
        assert!(!outside_exists);
    }
}
