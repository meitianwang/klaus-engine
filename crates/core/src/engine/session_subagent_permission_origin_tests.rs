//! An admitted task keeps its scoped authority while later restrictions still apply.

use super::*;
use crate::engine::session::SubagentCollaborationOrigin;
use crate::engine::session::{project_subagent_permission, SubagentPermissionOrigin};
use crate::permission::rules::parse_rule_strings;
use crate::planning::CollaborationMode;
use crate::runtime_values::RuntimeDecode;
use crate::types::permission::{ApprovalPolicy, SandboxMode};
use crate::types::sandbox::SandboxKind;
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicUsize, Ordering};

fn origin_rules(names: &[&str]) -> Vec<crate::types::permission::PermissionRule> {
    parse_rule_strings(
        &names
            .iter()
            .map(|name| RuntimeString::from(*name))
            .collect::<Vec<_>>(),
    )
}

fn origin_message(
    target: &RuntimeString,
    text: &str,
    user_initiated: bool,
) -> SubagentMessageRequest {
    let mut request = message(target, text, user_initiated);
    request.sender_agent_id = None;
    request
}

fn saved_assistant_positions(request: &ModelRequest, text: &str) -> Vec<usize> {
    request
        .messages
        .iter()
        .enumerate()
        .filter_map(|(index, message)| match message {
            Message::Assistant(message)
                if message.content.iter().any(
                    |block| matches!(block, ContentBlock::Text(block) if block.text == text),
                ) =>
            {
                Some(index)
            }
            _ => None,
        })
        .collect()
}

fn install_recovery_base(session: &Arc<Session>, text: &str) {
    let parts = vec![crate::types::provider::SystemPromptPart {
        name: "recovery-base".into(),
        text: text.into(),
        cacheable: Some(true),
        cache_scope: None,
    }];
    *session.system_prompt_provider.write().unwrap() = Arc::new(move || {
        let parts = parts.clone();
        Box::pin(async move { parts })
    });
}

async fn scoped_worker(
    session: &Arc<Session>,
    permission: PermissionContext,
    origin: SubagentPermissionOrigin,
) -> RuntimeString {
    session.make_spawn_background_agent_with_permission_origin(
        0,
        AbortSignal::new(),
        Some("scoped-admission-turn".into()),
        permission,
        Arc::new(Vec::new),
        false,
        None,
        None,
        None,
        Some(origin),
        Some(crate::engine::session::SubagentCapabilityCapture::capture(
            session.runtime_capability_snapshot_for_turn(&TurnOptions::default()),
        )),
        Some(session.planning.mode()),
    )(SpawnSubAgentOptions {
        prompt: "Run with the admitted task authority.".into(),
        fork_context: Some(ForkContext::None),
        ..Default::default()
    })
    .await
    .expect("scoped creation must be accepted")
    .agent_id
}

#[tokio::test]
async fn actual_task_keeps_captured_canonical_policy_over_unchanged_parent() {
    let observed = Arc::new(Mutex::new(Vec::new()));
    let probe: AnyTool = Arc::new(ChildPolicyProbe {
        observed: observed.clone(),
        inline_shell: false,
    });
    let mut task_turn = vec![ev::message_start(None)];
    task_turn.extend(exact_tool_use_events(
        "scoped-task-call",
        "task",
        &exact_json!({"model": "low",
            "description":"Record admitted authority",
            "prompt":"Record the actual child permission context.",
            "fork_turns":"none",
            "run_in_background":false
        }),
    ));
    task_turn.push(ev::stop(
        Some(ModelStopReason::ToolUse),
        Some(make_usage(None, None)),
    ));
    let (session, requests) = fixture(
        vec![
            task_turn,
            child_policy_probe_turn(),
            text_turn("child done"),
            text_turn("root done"),
        ],
        vec![
            crate::tools::builtins::task::create_task_tool(Default::default()),
            probe,
        ],
    );
    let parent_policy = session.runtime_policy_state();
    session.mode.permission.lock().unwrap().allow_rules = origin_rules(&["task"]);
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        session.run(
            Input::Text("Delegate the scoped task.".into()),
            TurnOptions::default(),
        ),
    )
    .await
    .expect("the actual scoped Task must complete");
    assert_eq!(result.stop_reason, TerminalReason::Completed);
    assert_eq!(requests.lock().unwrap().len(), 4);
    {
        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].sandbox_mode, parent_policy.sandbox_mode);
        assert_eq!(observed[0].approval_policy, parent_policy.approval_policy);
    }
    assert_eq!(session.runtime_policy_state(), parent_policy);
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn cold_child_restores_captured_and_latest_boundary_review_without_changing_never_flow() {
    use crate::engine::session::RuntimePolicyUpdate;
    for (initial_untrusted, later_untrusted, clear_boundary, invalid_evidence) in [
        (true, false, false, false),
        (false, true, false, false),
        (false, true, true, false),
        (true, false, true, false),
        (false, true, false, true),
    ] {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let probe: AnyTool = Arc::new(ChildPolicyProbe {
            observed: observed.clone(),
            inline_shell: false,
        });
        let write = crate::tools::builtins::write::create_write_tool();
        let blocked = gate();
        let mut turns = Vec::new();
        if later_untrusted {
            turns.push(gate_turn());
        }
        turns.push(text_turn("saved"));
        if clear_boundary {
            turns.push(text_turn("later boundary"));
        }
        let (session, store, initial_requests, dir) =
            persistent_fixture(turns, vec![blocked.clone(), probe.clone(), write.clone()]).await;
        install_probe_runtime_policy(
            &session,
            SandboxMode::DangerFullAccess,
            if initial_untrusted {
                ApprovalPolicy::UnlessTrusted
            } else {
                ApprovalPolicy::Never
            },
        );
        let id = spawn_worker(
            &session,
            SpawnSubAgentOptions {
                prompt: "Retain the canonical review requirement.".into(),
                runtime_policy: Some(RuntimePolicyUpdate {
                    approval_policy: Some(ApprovalPolicy::Never),
                    sandbox_mode: Some(SandboxMode::DangerFullAccess),
                    approvals_reviewer: None,
                    sandbox_workspace_write: None,
                    network_allowed: None,
                }),
                ..Default::default()
            },
        )
        .await;
        if later_untrusted {
            entered(&blocked).await;
            install_probe_runtime_policy(
                &session,
                SandboxMode::DangerFullAccess,
                ApprovalPolicy::UnlessTrusted,
            );
            blocked.release.add_permits(1);
        }
        finished(&session, &id).await;
        settle_ordinary_owners(&session).await;
        if clear_boundary {
            install_probe_runtime_policy(
                &session,
                SandboxMode::DangerFullAccess,
                ApprovalPolicy::Never,
            );
            assert!(
                session
                    .send_subagent_message(origin_message(
                        &id,
                        "Capture the relaxed live policy at a new boundary.",
                        false
                    ))
                    .await
                    .unwrap()
                    .unwrap()
                    .resumed
            );
            finished(&session, &id).await;
            settle_ordinary_owners(&session).await;
        }
        let creation = session
            .ordinary_snapshot_for_resume(&id.try_to_utf8().unwrap())
            .await
            .unwrap();
        let expected_creation_policy = Some(if initial_untrusted {
            ApprovalPolicy::UnlessTrusted
        } else {
            ApprovalPolicy::Never
        });
        let expected_boundary_policy = (!clear_boundary).then_some(ApprovalPolicy::UnlessTrusted);
        assert_eq!(creation.config.approval_policy, expected_creation_policy);
        assert_eq!(creation.boundary_approval_policy, expected_boundary_policy);
        assert_eq!(
            creation.runtime_policy.state.approval_policy,
            Some(ApprovalPolicy::Never)
        );
        assert_eq!(
            initial_requests.lock().unwrap().len(),
            1 + usize::from(later_untrusted) + usize::from(clear_boundary)
        );
        session.shutdown_and_wait().await.unwrap();
        let saved = store
            .read_sub_agent("s1", &id.try_to_utf8().unwrap())
            .await
            .unwrap();
        let metadata = saved
            .raw_items
            .iter()
            .find_map(|item| match item {
                RolloutItem::Event(event) if event.event == "ordinary_subagent_creation" => {
                    event.payload.as_ref()
                }
                _ => None,
            })
            .unwrap();
        assert_eq!(
            metadata.get("approvalPolicy"),
            Some(&RuntimeJson::String("never".into()))
        );
        assert_eq!(
            metadata
                .get("config")
                .and_then(|config| config.get("approval_policy")),
            Some(&RuntimeJson::String(
                if initial_untrusted {
                    "untrusted"
                } else {
                    "never"
                }
                .into()
            ))
        );
        let latest_floor = saved
            .raw_items
            .iter()
            .rev()
            .find_map(|item| match item {
                RolloutItem::Event(event) if event.event == "turn_lifecycle" => event
                    .payload
                    .as_ref()
                    .and_then(|payload| payload.get("canonicalReviewFloor")),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            latest_floor.get("version").and_then(RuntimeJson::as_u64),
            Some(1)
        );
        assert_eq!(
            latest_floor.get("approvalPolicy"),
            Some(&if clear_boundary {
                RuntimeJson::Null
            } else {
                RuntimeJson::String("untrusted".into())
            })
        );
        let contexts: Vec<_> = saved
            .raw_items
            .iter()
            .filter_map(|item| match item {
                RolloutItem::TurnContext { ctx } => {
                    Some(crate::runtime_values::runtime_codec::encode_value(ctx).unwrap())
                }
                _ => None,
            })
            .collect();
        assert!(!contexts.is_empty());
        for context in contexts {
            assert_eq!(
                context.get("approvalPolicy"),
                Some(&RuntimeJson::String("never".into())),
                "turn metadata must publish the effective flow, not the inherited review floor"
            );
        }
        if invalid_evidence {
            let rollout = store
                .open_sub_agent(
                    "s1",
                    &id.try_to_utf8().unwrap(),
                    saved.meta.clone().unwrap(),
                )
                .await
                .unwrap();
            rollout
                .append_event(
                    "turn_lifecycle".into(),
                    Some(AppendEventOptions {
                        turn_id: None,
                        payload: Some(exact_json!({"canonicalReviewFloor":null})),
                    }),
                )
                .await;
            rollout.flush().await.unwrap();
            rollout.shutdown().await.unwrap();
        }
        let target = dir.join(".klaus/projects/project/memory/cold-review-floor.md");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let approvals = Arc::new(ReviewFloorApprovalProbe::default());
        let mut config = default_config();
        config.agent_memory = false;
        config.memory_recall = false;
        config.approval_policy = Some(ApprovalPolicy::Never);
        config.sandbox_mode = Some(SandboxMode::DangerFullAccess);
        let mut init = session_init_with_provider_memory_store_agents_and_snapshot_decisions(
            Arc::new(fake_provider()),
            unique_subagent_test_deps(recording_deps(
                requests.clone(),
                vec![
                    child_policy_probe_turn(),
                    review_floor_write_turn(&target),
                    text_turn("recovered"),
                ],
            )),
            config,
            vec![blocked, probe, write],
            None,
            None,
            Some(store.clone()),
            None,
            None,
            None,
        );
        init.approval_handler = Some(approvals.clone());
        let cold = Session::new(init);
        install_probe_runtime_policy(&cold, SandboxMode::DangerFullAccess, ApprovalPolicy::Never);
        if !invalid_evidence {
            let restored = cold
                .ordinary_snapshot_for_resume(&id.try_to_utf8().unwrap())
                .await
                .unwrap();
            assert_eq!(
                restored.config.approval_policy,
                Some(if initial_untrusted {
                    ApprovalPolicy::UnlessTrusted
                } else {
                    ApprovalPolicy::Never
                })
            );
            assert_eq!(restored.boundary_approval_policy, expected_boundary_policy);
            assert_eq!(
                restored.runtime_policy.state.approval_policy,
                Some(ApprovalPolicy::Never)
            );
        }
        let result = cold
            .send_subagent_message(origin_message(
                &id,
                "Recover the captured review floor.",
                false,
            ))
            .await
            .unwrap();
        if invalid_evidence {
            assert!(
                result.is_err(),
                "invalid present evidence must reject recovery"
            );
            assert!(requests.lock().unwrap().is_empty());
            assert!(observed.lock().unwrap().is_empty());
            assert!(!target.exists());
        } else {
            assert!(result.unwrap().resumed);
            finished(&cold, &id).await;
            settle_ordinary_owners(&cold).await;
            let restored = cold
                .ordinary_snapshot_for_resume(&id.try_to_utf8().unwrap())
                .await
                .unwrap();
            assert_eq!(
                restored.config.approval_policy,
                Some(if initial_untrusted {
                    ApprovalPolicy::UnlessTrusted
                } else {
                    ApprovalPolicy::Never
                })
            );
            assert_eq!(
                restored.runtime_policy.state.approval_policy,
                Some(ApprovalPolicy::Never)
            );
            assert_eq!(
                cold.runtime_policy_state().approval_policy,
                Some(ApprovalPolicy::Never)
            );
            let observations = observed.lock().unwrap();
            assert_eq!(observations.len(), 1);
            assert_eq!(observations[0].approval_policy, Some(ApprovalPolicy::Never));
            assert_eq!(
                observations[0].sandbox_mode,
                Some(SandboxMode::DangerFullAccess)
            );
            drop(observations);
            let rejected = initial_untrusted || !clear_boundary;
            assert_review_floor_write_result(&requests.lock().unwrap(), rejected);
            if rejected {
                assert!(!target.exists());
            } else {
                assert_eq!(
                    std::fs::read_to_string(&target).unwrap(),
                    "review floor probe\n"
                );
            }
        }
        assert_eq!(approvals.0.load(Ordering::SeqCst), 0);
        cold.shutdown_and_wait().await.unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[tokio::test]
async fn scoped_child_obeys_later_canonical_rules_shell_and_isolation_restrictions() {
    let blocked = gate();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let analyses = Arc::new(AtomicUsize::new(0));
    let (session, requests) = fixture(
        vec![
            gate_turn(),
            child_policy_probe_turn(),
            text_turn("restricted child done"),
        ],
        vec![
            blocked.clone(),
            Arc::new(ChildPolicyProbe {
                observed: observed.clone(),
                inline_shell: true,
            }),
            Arc::new(SourceTrackedBash {
                identity: "fixture:origin-bash:1",
                analyses: analyses.clone(),
            }),
        ],
    );
    let original = probe_runtime_policy(SandboxKind::Seatbelt, true);
    let mut original_state = original.state();
    original_state.sandbox_workspace_write.writable_roots =
        vec!["/shared".into(), "/revoked".into()];
    *session.runtime_policy.write().unwrap() =
        Arc::new(crate::engine::session::RuntimePolicySnapshot {
            state: original_state,
            sandbox_backend: original.sandbox_backend.clone(),
        });
    session.mode.permission.lock().unwrap().allow_rules = origin_rules(&["shared", "revoked"]);
    let (mut admitted, origin) =
        session.permission_context_for_policy_with_origin(&session.runtime_policy_snapshot());
    admitted
        .allow_rules
        .push(origin_rules(&["task_scoped"]).remove(0));
    admitted
        .additional_writable_roots
        .push("/task-scoped".into());
    let id = scoped_worker(&session, admitted, origin).await;
    entered(&blocked).await;
    assert_eq!(blocked.permissions.lock().unwrap().len(), 1);
    {
        let mut permission = session.mode.permission.lock().unwrap();
        permission.allow_rules = origin_rules(&["shared", "new_parent_grant"]);
        permission.deny_rules = origin_rules(&["bash", "current_deny"]);
        permission.ask_rules = origin_rules(&["current_ask"]);
    }
    let current = probe_runtime_policy(SandboxKind::Seatbelt, false);
    *session.runtime_policy.write().unwrap() =
        Arc::new(crate::engine::session::RuntimePolicySnapshot {
            state: crate::engine::session::RuntimePolicyState {
                approval_policy: Some(ApprovalPolicy::Never),
                sandbox_mode: Some(crate::types::permission::SandboxMode::WorkspaceWrite),
                approvals_reviewer: None,
                sandbox_workspace_write: crate::schemas::config::SandboxWorkspaceWrite {
                    writable_roots: vec!["/shared".into(), "/new-parent-root".into()],
                    ..Default::default()
                },
                ..current.state.clone()
            },
            sandbox_backend: current.sandbox_backend.clone(),
        });
    blocked.release.add_permits(1);
    finished(&session, &id).await;
    {
        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), 1);
        let actual = &observed[0];
        assert_eq!(actual.approval_policy, Some(ApprovalPolicy::Never));
        assert_eq!(actual.sandbox_mode, Some(SandboxMode::WorkspaceWrite));
        assert_eq!(
            actual.permission.allow_rules,
            origin_rules(&["shared", "task_scoped"])
        );
        assert_eq!(
            actual.permission.deny_rules,
            origin_rules(&["bash", "current_deny"])
        );
        assert_eq!(actual.permission.ask_rules, origin_rules(&["current_ask"]));
        assert!(actual
            .permission
            .additional_writable_roots
            .contains(&RuntimePath::from("/shared")));
        assert!(actual
            .permission
            .additional_writable_roots
            .contains(&RuntimePath::from("/task-scoped")));
        assert!(!actual
            .permission
            .additional_writable_roots
            .contains(&RuntimePath::from("/revoked")));
        assert!(!actual
            .permission
            .additional_writable_roots
            .contains(&RuntimePath::from("/new-parent-root")));
        assert_eq!(actual.sandbox, SandboxKind::Seatbelt);
        assert_eq!(actual.network, Some(false));
        assert_eq!(actual.retry, Some(false));
        assert!(actual
            .inline_error
            .as_ref()
            .unwrap()
            .contains("Inline shell command not permitted"));
    }
    assert!(
        analyses.load(Ordering::SeqCst) > 0,
        "the original bash authority must actually be checked"
    );
    assert_eq!(requests.lock().unwrap().len(), 3);
    session.shutdown_and_wait().await.unwrap();
}

#[test]
fn origin_projection_revokes_baseline_grants_and_roots_without_importing_live_grants() {
    let mut parent = policy_permission();
    parent.allow_rules = origin_rules(&["shared", "revoked"]);
    parent.additional_writable_roots = vec!["/shared".into(), "/revoked".into()];
    let origin = SubagentPermissionOrigin::capture(&parent);
    let mut admitted = parent.clone();
    admitted.allow_rules.extend(origin_rules(&["task_scoped"]));
    admitted
        .additional_writable_roots
        .push("/task-scoped".into());
    admitted.deny_rules = origin_rules(&["original_deny"]);
    admitted.ask_rules = origin_rules(&["original_ask"]);
    let mut current = parent;
    current.allow_rules = origin_rules(&["shared", "new_parent_grant"]);
    current.additional_writable_roots = vec!["/shared".into(), "/new-parent-root".into()];
    current.deny_rules = origin_rules(&["current_deny"]);
    current.ask_rules = origin_rules(&["current_ask"]);
    current.should_avoid_prompts = true;
    let actual = project_subagent_permission(&admitted, &current, Some(&origin), true);
    assert_eq!(actual.allow_rules, origin_rules(&["shared", "task_scoped"]));
    assert_eq!(
        actual.additional_writable_roots,
        vec![
            RuntimePath::from("/shared"),
            RuntimePath::from("/task-scoped")
        ]
    );
    assert_eq!(
        actual.deny_rules,
        origin_rules(&["original_deny", "current_deny"])
    );
    assert_eq!(
        actual.ask_rules,
        origin_rules(&["original_ask", "current_ask"])
    );
    assert!(actual.should_avoid_prompts);
    let legacy = project_subagent_permission(&admitted, &current, None, true);
    assert_eq!(legacy.allow_rules, origin_rules(&["shared"]));
    assert_eq!(
        legacy.additional_writable_roots,
        vec![RuntimePath::from("/shared")]
    );
}

#[derive(Clone, Copy)]
enum RecoveryOrigin {
    Scoped,
    RevokedBaseline,
    LegacyAbsent,
    CorruptPresent,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RecoveryCollaboration {
    Captured,
    LegacyV4,
    LegacyV2,
    LegacyV3StaleDigest,
    InvalidPresent,
    UpdatedMode,
}

fn historical_prompt_digest(parts: &[crate::types::provider::SystemPromptPart]) -> RuntimeString {
    let value = crate::runtime_values::runtime_codec::encode_value(&parts.to_vec()).unwrap();
    let encoded = crate::runtime_values::runtime_json::stringify(&value);
    let mut hash = Sha256::new();
    for unit in encoded.as_utf16() {
        hash.update(unit.to_le_bytes());
    }
    format!("{:x}", hash.finalize()).into()
}

async fn cold_origin_case(case: RecoveryOrigin) {
    cold_authority_case(
        case,
        CollaborationMode::Default,
        CollaborationMode::Default,
        RecoveryCollaboration::Captured,
    )
    .await;
}

async fn cold_authority_case(
    case: RecoveryOrigin,
    original_mode: CollaborationMode,
    cold_mode: CollaborationMode,
    collaboration_record: RecoveryCollaboration,
) {
    let observed = Arc::new(Mutex::new(Vec::new()));
    let probe: AnyTool = Arc::new(ChildPolicyProbe {
        observed: observed.clone(),
        inline_shell: false,
    });
    let (session, store, initial_requests, dir) =
        persistent_fixture(vec![text_turn("saved")], vec![probe.clone()]).await;
    install_recovery_base(&session, "Saved worker base instructions.");
    session
        .planning_runtime()
        .set_mode(original_mode, None)
        .unwrap();
    if matches!(case, RecoveryOrigin::RevokedBaseline) {
        session.mode.permission.lock().unwrap().allow_rules = origin_rules(&["task_scoped"]);
    }
    session
        .set_runtime_policy(crate::engine::session::RuntimePolicyUpdate {
            approval_policy: None,
            sandbox_mode: None,
            approvals_reviewer: None,
            sandbox_workspace_write: None,
            network_allowed: Some(false),
        })
        .unwrap();
    let old_generation = session.runtime_policy_state().generation;
    let (mut admitted, origin) =
        session.permission_context_for_policy_with_origin(&session.runtime_policy_snapshot());
    if !matches!(case, RecoveryOrigin::RevokedBaseline) {
        admitted.allow_rules.extend(origin_rules(&["task_scoped"]));
    }
    let captured_policy = session.runtime_policy_state();
    let id = scoped_worker(&session, admitted, origin).await;
    finished(&session, &id).await;
    settle_ordinary_owners(&session).await;
    assert_eq!(initial_requests.lock().unwrap().len(), 1);
    let (original_parts, original_model, original_depth, original_parent, original_type) = {
        let creation = session
            .ordinary_snapshot_for_resume(&id.try_to_utf8().unwrap())
            .await
            .expect("the original live worker retains its creation snapshot");
        {
            let requests = initial_requests.lock().unwrap();
            let outgoing_parts = &requests[0].system;
            let permissions: Vec<_> = outgoing_parts
                .iter()
                .filter(|part| part.name == "permissions")
                .collect();
            assert_eq!(permissions.len(), 1);
            assert!(!permissions[0].text.is_empty());
            let collaboration: Vec<_> = outgoing_parts
                .iter()
                .filter(|part| part.name == "collaboration-mode")
                .collect();
            assert_eq!(
                collaboration,
                vec![&crate::planning::instructions::instruction_part(
                    &creation.config,
                    original_mode,
                )],
                "the outgoing request separately projects its selected collaboration instructions"
            );
        }
        (
            creation.system_prompt.clone(),
            creation.config.model.clone(),
            creation.depth,
            creation.parent_agent_id.clone(),
            creation.agent_type.clone(),
        )
    };
    session.shutdown_and_wait().await.unwrap();
    let saved = store
        .read_sub_agent("s1", &id.try_to_utf8().unwrap())
        .await
        .unwrap();
    let mut metadata = saved
        .raw_items
        .iter()
        .find_map(|item| match item {
            RolloutItem::Event(event) if event.event == "ordinary_subagent_creation" => {
                event.payload.clone()
            }
            _ => None,
        })
        .expect("creation evidence must be durably recorded");
    let origin = metadata
        .get("permissionOrigin")
        .expect("new creation persists private origin");
    assert_eq!(origin.get("version").and_then(RuntimeJson::as_u64), Some(2));
    assert!(origin.get("parentMode").is_none());
    assert!(metadata.get("permissionMode").is_none());
    assert!(metadata.get("runtimePermissionMode").is_none());
    let collaboration = metadata
        .get("collaborationOrigin")
        .expect("new creation persists its selected collaboration mode");
    assert_eq!(
        collaboration.get("version").and_then(RuntimeJson::as_u64),
        Some(1)
    );
    assert_eq!(
        collaboration.get("mode").and_then(RuntimeJson::as_str),
        Some(&RuntimeString::from(original_mode.wire_name()))
    );
    assert_eq!(
        metadata.get("version").and_then(RuntimeJson::as_u64),
        Some(6)
    );
    assert!(metadata.get("systemPromptDigest").is_none());
    let saved_base: Vec<crate::types::provider::SystemPromptPart> =
        crate::runtime_values::runtime_codec::decode_value(
            metadata
                .get("baseInstructions")
                .expect("new creation stores its base instruction body"),
        )
        .unwrap();
    assert_eq!(
        saved_base,
        original_parts
            .iter()
            .filter(|part| !part.name.starts_with("agent:") && part.name != "collaboration-mode")
            .cloned()
            .collect::<Vec<_>>()
    );
    assert!(
        saved_base
            .iter()
            .any(|part| part.name == "recovery-base"
                && part.text == "Saved worker base instructions.")
    );
    let legacy = matches!(
        collaboration_record,
        RecoveryCollaboration::LegacyV2
            | RecoveryCollaboration::LegacyV3StaleDigest
            | RecoveryCollaboration::LegacyV4
    );
    let mut expected_origin_mode = Some(original_mode);
    let mut expected_collaboration_error = None;
    if collaboration_record != RecoveryCollaboration::Captured {
        let fields = metadata
            .as_object_mut()
            .expect("creation must be an object");
        if legacy {
            assert!(fields.remove("baseInstructions").is_some());
            assert!(fields.remove("collaborationOrigin").is_some());
            expected_origin_mode = None;
            let version = match collaboration_record {
                RecoveryCollaboration::LegacyV2 => 2_u32,
                RecoveryCollaboration::LegacyV3StaleDigest => 3,
                RecoveryCollaboration::LegacyV4 => 4,
                _ => unreachable!(),
            };
            fields.insert(
                "version",
                crate::runtime_values::runtime_codec::encode_value(&version).unwrap(),
            );
            fields.insert(
                "systemPromptDigest",
                RuntimeJson::String(
                    if collaboration_record == RecoveryCollaboration::LegacyV3StaleDigest {
                        "0".repeat(64).into()
                    } else {
                        historical_prompt_digest(&original_parts)
                    },
                ),
            );
            if version < 4 {
                fields.insert(
                    "permissionMode",
                    RuntimeJson::String("bypassPermissions".into()),
                );
            }
            if version == 3 {
                fields.insert(
                    "runtimePermissionMode",
                    RuntimeJson::String("default".into()),
                );
            }
            if version == 2 {
                fields.insert("sandbox", RuntimeJson::String("seatbelt".into()));
                fields.insert("requireRealSandbox", RuntimeJson::Bool(true));
                fields.insert("sandboxRetryOnDenial", RuntimeJson::Bool(false));
            }
        } else {
            match collaboration_record {
                RecoveryCollaboration::InvalidPresent => {
                    fields.insert("collaborationOrigin", RuntimeJson::Null);
                    let error = SubagentCollaborationOrigin::decode_runtime(&RuntimeJson::Null)
                        .unwrap_err();
                    expected_collaboration_error = Some(RuntimeString::from(format!(
                        "Invalid original collaboration origin: {error}"
                    )));
                }
                RecoveryCollaboration::UpdatedMode => {
                    let mode = match original_mode {
                        CollaborationMode::Default => CollaborationMode::Plan,
                        CollaborationMode::Plan => CollaborationMode::Default,
                    };
                    expected_origin_mode = Some(mode);
                    fields.insert(
                        "collaborationOrigin",
                        crate::runtime_values::runtime_codec::encode_value(
                            &SubagentCollaborationOrigin::capture(mode),
                        )
                        .unwrap(),
                    );
                }
                _ => unreachable!(),
            }
        }
    }
    let target = if matches!(
        case,
        RecoveryOrigin::LegacyAbsent | RecoveryOrigin::CorruptPresent
    ) || collaboration_record != RecoveryCollaboration::Captured
    {
        let RuntimeJson::Object(fields) = &mut metadata else {
            panic!("creation must be an object")
        };
        if matches!(case, RecoveryOrigin::LegacyAbsent) {
            assert!(fields.remove("permissionOrigin").is_some());
        } else if matches!(case, RecoveryOrigin::CorruptPresent) {
            fields.insert("permissionOrigin", RuntimeJson::Null);
        }
        let key = format!("{}-origin-copy", id.try_to_utf8().unwrap());
        let rollout = store
            .open_sub_agent("s1", &key, saved.meta.clone().unwrap())
            .await
            .unwrap();
        rollout
            .append_event(
                "ordinary_subagent_creation".into(),
                Some(AppendEventOptions {
                    turn_id: None,
                    payload: Some(metadata),
                }),
            )
            .await;
        rollout
            .append_event(
                "ordinary_subagent_creation_committed".into(),
                Some(AppendEventOptions {
                    turn_id: None,
                    payload: Some(RuntimeJson::Bool(true)),
                }),
            )
            .await;
        for item in saved.raw_items {
            if let RolloutItem::Message { message } = item {
                rollout.append_message(message).await;
            }
        }
        rollout.flush().await.unwrap();
        rollout.shutdown().await.unwrap();
        RuntimeString::from(key)
    } else {
        id
    };
    let requests = Arc::new(Mutex::new(Vec::new()));
    let mut config = default_config();
    config.agent_memory = false;
    config.memory_recall = false;
    let cold = mk_session_with_provider_memory_and_store(
        Arc::new(fake_provider()),
        unique_subagent_test_deps(recording_deps(
            requests.clone(),
            vec![child_policy_probe_turn(), text_turn("recovered")],
        )),
        config,
        vec![probe],
        None,
        None,
        Some(store),
        None,
    );
    install_recovery_base(&cold, "Current cold base template.");
    cold.planning_runtime().set_mode(cold_mode, None).unwrap();
    assert!(
        cold.runtime_policy_state().generation < old_generation,
        "recovery resets the live policy generation"
    );
    let result = cold
        .send_subagent_message(origin_message(
            &target,
            "Recover using original authority.",
            false,
        ))
        .await
        .unwrap();
    if matches!(case, RecoveryOrigin::CorruptPresent) {
        assert!(
            result.is_err(),
            "present invalid origin must reject recovery"
        );
        let expected = SubagentPermissionOrigin::decode_runtime(&RuntimeJson::Null).unwrap_err();
        assert_eq!(
            result.expect_err("corrupt permission origin must reject"),
            RuntimeString::from(format!("Invalid original permission origin: {expected}")),
            "the cold negative must reach the permission-origin decoder"
        );
        assert!(
            requests.lock().unwrap().is_empty(),
            "invalid creation evidence cannot reach a provider"
        );
        assert!(observed.lock().unwrap().is_empty());
    } else if let Some(expected) = expected_collaboration_error {
        assert!(
            result.is_err(),
            "invalid original collaboration authority must reject"
        );
        assert_eq!(
            result.expect_err("corrupt collaboration authority must reject"),
            expected
        );
        assert!(requests.lock().unwrap().is_empty());
        assert!(observed.lock().unwrap().is_empty());
    } else {
        let result = result.unwrap();
        assert!(result.resumed);
        assert_eq!(result.agent_id, target);
        finished(&cold, &target).await;
        settle_ordinary_owners(&cold).await;
        let restored = cold
            .ordinary_snapshot_for_resume(&target.try_to_utf8().unwrap())
            .await
            .unwrap();
        assert_eq!(restored.config.model, original_model);
        assert_eq!(restored.depth, original_depth);
        assert_eq!(restored.parent_agent_id, original_parent);
        assert_eq!(restored.agent_type, original_type);
        assert_ordinary_creation_bindings(
            &cold,
            &target,
            original_depth,
            Some("scoped-admission-turn"),
            original_parent.as_deref(),
        );
        let base = restored
            .system_prompt
            .iter()
            .find(|part| part.name == "recovery-base")
            .unwrap();
        assert_eq!(
            base.text,
            if legacy {
                "Current cold base template."
            } else {
                "Saved worker base instructions."
            }
        );
        let collaboration = restored
            .system_prompt
            .iter()
            .filter(|part| part.name == "collaboration-mode")
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(
            collaboration,
            expected_origin_mode
                .map(|mode| crate::planning::instructions::instruction_part(&restored.config, mode))
                .into_iter()
                .collect::<Vec<_>>()
        );
        {
            let requests = requests.lock().unwrap();
            assert_eq!(requests[0].model, original_model);
            assert_eq!(
                text_positions(&requests[0], "Run with the admitted task authority.").len(),
                1
            );
            assert_eq!(saved_assistant_positions(&requests[0], "saved").len(), 1);
            assert_eq!(
                requests[0]
                    .system
                    .iter()
                    .filter(|part| part.name == "collaboration-mode")
                    .cloned()
                    .collect::<Vec<_>>(),
                vec![crate::planning::instructions::instruction_part(
                    &restored.config,
                    cold_mode
                )]
            );
        }
        let expected = if matches!(case, RecoveryOrigin::Scoped) {
            origin_rules(&["task_scoped"])
        } else {
            Vec::new()
        };
        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].permission.allow_rules, expected);
        let policy = crate::engine::session::intersect_subagent_runtime_state(
            &captured_policy,
            &cold.runtime_policy_state(),
        );
        assert_eq!(observed[0].sandbox_mode, policy.sandbox_mode);
        assert_eq!(observed[0].approval_policy, policy.approval_policy);
        assert_eq!(requests.lock().unwrap().len(), 2);
    }
    cold.shutdown_and_wait().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn cold_recovery_keeps_scoped_origin_across_policy_generation_reset() {
    cold_origin_case(RecoveryOrigin::Scoped).await;
}

#[tokio::test]
async fn cold_recovery_applies_real_baseline_tightening_after_generation_reset() {
    cold_origin_case(RecoveryOrigin::RevokedBaseline).await;
}

#[tokio::test]
async fn cold_legacy_absent_origin_remains_conservative() {
    cold_origin_case(RecoveryOrigin::LegacyAbsent).await;
}

#[tokio::test]
async fn cold_present_corrupt_origin_rejects_before_any_provider_request() {
    cold_origin_case(RecoveryOrigin::CorruptPresent).await;
}

#[test]
fn private_origin_decoder_rejects_missing_unknown_and_unsupported_evidence() {
    use crate::runtime_values::RuntimeDecode;
    for invalid in [
        exact_json!(null),
        exact_json!({"version":1,"parentMode":"default","parentAllowRules":[]}),
        exact_json!({"version":1,"parentMode":"default","parentAllowRules":[],"parentWritableRoots":[],"extra":true}),
        exact_json!({"version":2,"parentMode":"default","parentAllowRules":[],"parentWritableRoots":[]}),
    ] {
        assert!(SubagentPermissionOrigin::decode_runtime(&invalid).is_err());
    }
}

#[tokio::test]
async fn cold_legacy_v4_current_template_with_absent_permission_origin_stays_conservative() {
    cold_authority_case(
        RecoveryOrigin::LegacyAbsent,
        CollaborationMode::Plan,
        CollaborationMode::Default,
        RecoveryCollaboration::LegacyV4,
    )
    .await;
}

#[tokio::test]
async fn cold_original_plan_collaboration_recovers_after_parent_returns_to_default() {
    cold_authority_case(
        RecoveryOrigin::Scoped,
        CollaborationMode::Plan,
        CollaborationMode::Default,
        RecoveryCollaboration::Captured,
    )
    .await;
}

#[tokio::test]
async fn cold_legacy_v4_default_uses_current_template_and_retained_authority() {
    cold_authority_case(
        RecoveryOrigin::Scoped,
        CollaborationMode::Default,
        CollaborationMode::Default,
        RecoveryCollaboration::LegacyV4,
    )
    .await;
}

#[tokio::test]
async fn cold_legacy_v4_prompt_digest_does_not_reconstruct_a_missing_mode() {
    cold_authority_case(
        RecoveryOrigin::Scoped,
        CollaborationMode::Plan,
        CollaborationMode::Default,
        RecoveryCollaboration::LegacyV4,
    )
    .await;
}

#[tokio::test]
async fn cold_legacy_v2_prompt_uses_current_template_and_migrates_authority() {
    cold_authority_case(
        RecoveryOrigin::LegacyAbsent,
        CollaborationMode::Default,
        CollaborationMode::Default,
        RecoveryCollaboration::LegacyV2,
    )
    .await;
}

#[tokio::test]
async fn cold_present_invalid_collaboration_origin_rejects_before_provider() {
    cold_authority_case(
        RecoveryOrigin::Scoped,
        CollaborationMode::Default,
        CollaborationMode::Default,
        RecoveryCollaboration::InvalidPresent,
    )
    .await;
}

#[tokio::test]
async fn cold_versioned_collaboration_origin_is_used_without_prompt_digest_matching() {
    cold_authority_case(
        RecoveryOrigin::Scoped,
        CollaborationMode::Plan,
        CollaborationMode::Default,
        RecoveryCollaboration::UpdatedMode,
    )
    .await;
}

#[tokio::test]
async fn cold_legacy_v3_stale_prompt_digest_uses_current_template_without_inventing_origin() {
    cold_authority_case(
        RecoveryOrigin::Scoped,
        CollaborationMode::Default,
        CollaborationMode::Default,
        RecoveryCollaboration::LegacyV3StaleDigest,
    )
    .await;
}

#[test]
fn legacy_permission_origin_decodes_without_rewriting_parent_mode() {
    use crate::runtime_values::runtime_codec;
    for mode in [
        "default",
        "plan",
        "acceptEdits",
        "auto",
        "bypassPermissions",
        "dontAsk",
        "bubble",
    ] {
        let legacy = crate::runtime_values::runtime_json::parse(&RuntimeString::from(format!(
            r#"{{"version":1,"parentMode":"{mode}","parentAllowRules":[],"parentWritableRoots":[]}}"#,
        ))).unwrap();
        let origin = SubagentPermissionOrigin::decode_runtime(&legacy).unwrap();
        let encoded = runtime_codec::encode_value(&origin).unwrap();
        assert_eq!(
            encoded.get("version").and_then(RuntimeJson::as_u64),
            Some(2)
        );
        assert!(encoded.get("parentMode").is_none());
    }
}

struct RetainedRoleProvider(crate::testing::FakeProvider);

impl ModelProvider for RetainedRoleProvider {
    fn info(&self) -> &ProviderInfo {
        self.0.info()
    }

    fn capabilities(&self, model: &RuntimeString) -> ProviderCapabilities {
        self.0.capabilities(model)
    }

    fn model_metadata(&self, _model: &RuntimeString) -> crate::types::provider::ModelCapabilities {
        crate::types::provider::ModelCapabilities {
            supported_reasoning_efforts: ["low", "high"].into_iter().map(Into::into).collect(),
            ..Default::default()
        }
    }

    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        self.0.stream(request, signal)
    }

    fn estimate_tokens(&self, request: &ModelRequest) -> Option<u64> {
        self.0.estimate_tokens(request)
    }
}

#[tokio::test]
async fn historical_named_role_refreshes_current_fields_with_saved_identity_and_parent_ceiling() {
    use crate::runtime_values::runtime_codec;
    use crate::schemas::effort::{EffortLevel, EffortValue};
    let historical_definition = crate::runtime_values::runtime_json::parse(&RuntimeString::from(
        r#"{"model":"test-model","name":"legacy-reader","description":"Retained reader","tools":["child_policy_probe"],"permissionMode":"bypassPermissions","effort":"low","prompt":"Inspect retained worker state."}"#,
    )).unwrap();
    let definition = AgentDefinition::decode_runtime(&historical_definition).unwrap();
    for (version, changed) in [
        (2_u32, "none"),
        (3, "description"),
        (4, "prompt"),
        (5, "prompt"),
        (3, "tools"),
        (5, "tools"),
        (4, "effort"),
        (5, "effort"),
        (3, "policy"),
        (5, "disallowed"),
        (5, "model"),
    ] {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let probe: AnyTool = Arc::new(ChildPolicyProbe {
            observed: observed.clone(),
            inline_shell: false,
        });
        let provider: Arc<dyn ModelProvider> = Arc::new(RetainedRoleProvider(fake_provider()));
        let (session, store, initial_requests, dir) =
            persistent_fixture_with_provider_and_definitions(
                vec![text_turn("saved-role-response")],
                vec![probe.clone()],
                Some(vec![definition.clone()]),
                provider.clone(),
            )
            .await;
        install_recovery_base(&session, "Saved role base instructions.");
        let id = spawn_worker(
            &session,
            SpawnSubAgentOptions {
                prompt: "Retain the original role history.".into(),
                agent_type: Some("legacy-reader".into()),
                ..Default::default()
            },
        )
        .await;
        finished(&session, &id).await;
        settle_ordinary_owners(&session).await;
        let original = session
            .ordinary_snapshot_for_resume(&id.try_to_utf8().unwrap())
            .await
            .unwrap();
        let original_model = original.config.model.clone();
        let original_depth = original.depth;
        let original_parent = original.parent_agent_id.clone();
        session.shutdown_and_wait().await.unwrap();
        let saved = store
            .read_sub_agent("s1", &id.try_to_utf8().unwrap())
            .await
            .unwrap();
        let mut metadata = saved
            .raw_items
            .iter()
            .find_map(|item| match item {
                RolloutItem::Event(event) if event.event == "ordinary_subagent_creation" => {
                    event.payload.clone()
                }
                _ => None,
            })
            .expect("creation metadata must be durable");
        let fields = metadata.as_object_mut().unwrap();
        fields.insert("version", runtime_codec::encode_value(&version).unwrap());
        if version < 5 {
            assert!(fields.remove("baseInstructions").is_some());
            fields.insert(
                "definitionDigest",
                RuntimeJson::String("outdated-role-fingerprint".into()),
            );
            fields.insert(
                "systemPromptDigest",
                RuntimeJson::String("outdated-template-fingerprint".into()),
            );
        }
        if version < 4 {
            fields.insert(
                "permissionMode",
                RuntimeJson::String("bypassPermissions".into()),
            );
        }
        if version == 3 {
            fields.insert(
                "runtimePermissionMode",
                RuntimeJson::String("default".into()),
            );
        }
        if version == 2 {
            fields.insert("sandbox", RuntimeJson::String("seatbelt".into()));
            fields.insert("requireRealSandbox", RuntimeJson::Bool(true));
            fields.insert("sandboxRetryOnDenial", RuntimeJson::Bool(false));
        }
        let target = RuntimeString::from(format!("{}-role-copy", id.try_to_utf8().unwrap()));
        let rollout = store
            .open_sub_agent("s1", &target.try_to_utf8().unwrap(), saved.meta.unwrap())
            .await
            .unwrap();
        rollout
            .append_event(
                "ordinary_subagent_creation".into(),
                Some(AppendEventOptions {
                    turn_id: None,
                    payload: Some(metadata),
                }),
            )
            .await;
        rollout
            .append_event(
                "ordinary_subagent_creation_committed".into(),
                Some(AppendEventOptions {
                    turn_id: None,
                    payload: Some(RuntimeJson::Bool(true)),
                }),
            )
            .await;
        for item in saved.raw_items {
            if let RolloutItem::Message { message } = item {
                rollout.append_message(message).await;
            }
        }
        rollout.flush().await.unwrap();
        rollout.shutdown().await.unwrap();
        let mut current_definition =
            AgentDefinition::decode_runtime(&runtime_codec::encode_value(&definition).unwrap())
                .unwrap();
        match changed {
            "description" => current_definition.description = "Current reader description".into(),
            "prompt" => current_definition.prompt = Some("Current reader instructions.".into()),
            "tools" | "disallowed" => {
                current_definition.tools = Some(vec!["child_policy_probe".into(), "write".into()]);
                if changed == "disallowed" {
                    current_definition.disallowed_tools = Some(vec!["write".into()]);
                }
            }
            "effort" => current_definition.effort = Some(EffortValue::Level(EffortLevel::High)),
            "model" => current_definition.model = Some("different-role-model".into()),
            "policy" => {
                current_definition
                    .runtime_policy
                    .as_mut()
                    .unwrap()
                    .sandbox_mode = Some(SandboxMode::WorkspaceWrite)
            }
            "none" => {}
            _ => unreachable!(),
        }
        let expected_effort = current_definition.effort.clone();
        let expected_role = current_definition.prompt.clone().unwrap();
        let expected_description = current_definition.description.clone();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let mut config = default_config();
        config.agent_memory = false;
        config.memory_recall = false;
        config.sandbox_mode = Some(SandboxMode::ReadOnly);
        config.approval_policy = Some(ApprovalPolicy::Never);
        let cold = mk_session_with_provider_memory_store_and_agents(
            provider,
            unique_subagent_test_deps(recording_deps(
                requests.clone(),
                vec![child_policy_probe_turn(), text_turn("recovered")],
            )),
            config,
            vec![probe, crate::tools::builtins::write::create_write_tool()],
            None,
            None,
            Some(store),
            None,
            Some(vec![current_definition]),
        );
        install_recovery_base(&cold, "Current role base template.");
        let result = cold
            .send_subagent_message(origin_message(&target, "Resume the retained role.", false))
            .await
            .unwrap()
            .unwrap();
        assert!(result.resumed, "version {version}, changed {changed}");
        assert_eq!(result.agent_id, target);
        finished(&cold, &target).await;
        settle_ordinary_owners(&cold).await;
        let restored = cold
            .ordinary_snapshot_for_resume(&target.try_to_utf8().unwrap())
            .await
            .unwrap();
        assert_eq!(
            restored.agent_type,
            Some(RuntimeString::from("legacy-reader"))
        );
        assert_eq!(restored.depth, original_depth);
        assert_eq!(restored.parent_agent_id, original_parent);
        assert_ordinary_creation_bindings(
            &cold,
            &target,
            original_depth,
            None,
            original_parent.as_deref(),
        );
        assert_eq!(restored.config.model, original_model);
        assert_eq!(restored.effort, expected_effort);
        assert_eq!(
            restored.definition.as_ref().unwrap().description,
            expected_description
        );
        let base = restored
            .system_prompt
            .iter()
            .find(|part| part.name == "recovery-base")
            .unwrap();
        assert_eq!(
            base.text,
            if version == 5 {
                "Saved role base instructions."
            } else {
                "Current role base template."
            }
        );
        let role = restored
            .system_prompt
            .iter()
            .filter(|part| part.name == "agent:legacy-reader")
            .collect::<Vec<_>>();
        assert_eq!(role.len(), 1);
        assert_eq!(role[0].text, expected_role);
        {
            let actual = observed.lock().unwrap();
            assert_eq!(actual.len(), 1);
            assert_eq!(actual[0].sandbox_mode, Some(SandboxMode::ReadOnly));
            assert_eq!(actual[0].approval_policy, Some(ApprovalPolicy::Never));
        }
        {
            let requests = requests.lock().unwrap();
            assert_eq!(requests.len(), 2);
            assert_eq!(requests[0].model, initial_requests.lock().unwrap()[0].model);
            assert_eq!(
                text_positions(&requests[0], "Retain the original role history.").len(),
                1
            );
            assert_eq!(
                saved_assistant_positions(&requests[0], "saved-role-response").len(),
                1
            );
            assert_eq!(
                text_positions(&requests[0], "Resume the retained role.").len(),
                1
            );
            assert!(requests[0]
                .tools
                .iter()
                .any(|tool| tool.name == "child_policy_probe"));
            assert_eq!(
                requests[0].tools.iter().any(|tool| tool.name == "write"),
                changed == "tools"
            );
        }
        cold.shutdown_and_wait().await.unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }
}
