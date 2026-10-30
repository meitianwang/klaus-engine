//! Regression coverage for retained ownership and authority-preserving child messages.

use super::*;
use crate::permission::engine::PermissionDecision;

struct HardeningConfigRoot(std::path::PathBuf);

impl HardeningConfigRoot {
    fn new() -> Self {
        Self(temp_store_dir())
    }

    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for HardeningConfigRoot {
    fn drop(&mut self) {
        use std::io::Write;

        if std::thread::panicking() {
            let _ = writeln!(
                std::io::stderr().lock(),
                "Retained hardening fixture config root after panic: {}",
                self.path().display()
            );
            return;
        }
        if let Err(error) = std::fs::remove_dir_all(self.path()) {
            let _ = writeln!(
                std::io::stderr().lock(),
                "Cannot remove hardening fixture config root {}: {error}",
                self.path().display()
            );
        }
    }
}

struct IsolatedHardeningFixture {
    session: Arc<Session>,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
    config_root: HardeningConfigRoot,
}

impl IsolatedHardeningFixture {
    fn new(turns: ScriptedTurns, tools: Vec<AnyTool>) -> Self {
        Self::with_approval_handler(turns, tools, None)
    }

    fn with_approval_handler(
        turns: ScriptedTurns,
        tools: Vec<AnyTool>,
        approval_handler: Option<Arc<dyn crate::types::permission::ApprovalHandler>>,
    ) -> Self {
        let config_root = HardeningConfigRoot::new();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let mut config = default_config();
        config.agent_memory = false;
        config.memory_recall = false;
        config.fork = false;
        let mut init = session_init_with_provider_memory_store_agents_and_snapshot_decisions(
            Arc::new(fake_provider()),
            unique_subagent_test_deps(recording_deps(requests.clone(), turns)),
            config,
            tools,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        init.user_config_dir = Some(runtime_path_from_host_path(config_root.path()));
        init.id = Some(format!("hardening-{}", uuid::Uuid::new_v4()).into());
        init.approval_handler = approval_handler;
        Self {
            session: Session::new(init),
            requests,
            config_root,
        }
    }
}

#[derive(Default)]
struct ReviewFloorApprovalProbe(std::sync::atomic::AtomicUsize);

#[async_trait::async_trait]
impl crate::types::permission::ApprovalHandler for ReviewFloorApprovalProbe {
    async fn approve(
        &self,
        _: crate::types::permission::ApprovalRequest,
        _: crate::types::abort::AbortSignal,
    ) -> crate::types::permission::ApprovalResponse {
        self.0.fetch_add(1, Ordering::SeqCst);
        crate::types::permission::ApprovalResponse::Deny { message: None }
    }
}

fn review_floor_write_turn(
    path: &std::path::Path,
) -> Vec<crate::types::provider::ModelStreamEvent> {
    let mut input = crate::runtime_values::RuntimeObject::new();
    input.insert(
        "path",
        RuntimeJson::String(runtime_path_from_host_path(path).into_runtime_string()),
    );
    input.insert(
        "content",
        RuntimeJson::String("review floor probe\n".into()),
    );
    let mut events = vec![ev::message_start(None)];
    events.extend(exact_tool_use_events(
        "review-floor-write",
        "write",
        &RuntimeJson::Object(input),
    ));
    events.push(ev::stop(
        Some(ModelStopReason::ToolUse),
        Some(make_usage(None, None)),
    ));
    events
}

fn assert_review_floor_write_result(requests: &[ModelRequest], rejected: bool) {
    for request in requests {
        let permissions: Vec<_> = request
            .system
            .iter()
            .filter(|part| part.name == "permissions")
            .collect();
        assert_eq!(permissions.len(), 1);
        assert!(
            permissions[0].text.contains("Approval policy: never."),
            "the provider must see the effective approval flow"
        );
    }
    let result = requests
        .last()
        .unwrap()
        .messages
        .iter()
        .find_map(|message| match message {
            Message::ToolResult(result) if result.tool_use_id == "review-floor-write" => {
                Some(result)
            }
            _ => None,
        })
        .expect("the next provider request must carry the real write result");
    assert_eq!(result.is_error, rejected);
    if rejected {
        assert!(
            result.content.iter().any(|block| matches!(
                block,
                crate::schemas::content::OutputBlock::Text(text)
                    if text.text.contains("current approval policy is never")
            )),
            "the inherited review demand must reach the disabled approval-flow boundary"
        );
    }
}

struct DiagnosticRequestCount {
    site: &'static str,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
}
impl Drop for DiagnosticRequestCount {
    fn drop(&mut self) {
        let count = self
            .requests
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .len();
        crate::goal::testing_phase::mark(
            "fixture.wait.counters",
            &format!(
                "site={} provider_requests={count} unwinding={}",
                self.site,
                std::thread::panicking()
            ),
        );
    }
}

#[tokio::test]
async fn ordinary_factories_and_same_id_resume_keep_creation_turn_and_nested_depth() {
    for foreground_spawn in [false, true] {
        for parent_depth in [0, 1] {
            let mut turns = Vec::new();
            if parent_depth > 0 {
                turns.push(text_turn("parent ready"));
            }
            turns.extend([text_turn("first"), text_turn("resumed")]);
            let (session, requests) = fixture(turns, vec![]);
            let original_parent = if parent_depth > 0 {
                Some(
                    completed_parent(&session, "Original direct parent")
                        .await
                        .try_to_utf8()
                        .unwrap(),
                )
            } else {
                None
            };
            let parent_output_file = original_parent.as_ref().map(|parent| {
                session
                    .background_task_registry()
                    .get(&RuntimeString::from(parent.clone()))
                    .unwrap()
                    .output_file
            });
            let child_request_offset = usize::from(parent_depth > 0);
            let parent_turn = ordinary_parent_turn_lease(&session, "unrelated-live-turn");
            let options = SpawnSubAgentOptions {
                prompt: "original child".into(),
                fork_context: Some(ForkContext::None),
                ..Default::default()
            };
            let id = if foreground_spawn {
                let run = session.make_spawn_sub_agent_with_capture(
                    parent_depth,
                    CancellationToken::new(),
                    session.permission_context(),
                    Arc::new(Vec::new),
                    false,
                    original_parent.clone(),
                    None,
                    None,
                    Some("original-spawning-turn".into()),
                )(options)
                .await
                .expect("child creation must be accepted");
                let id = run.agent_id.as_str().unwrap().clone();
                let events = tokio::time::timeout(std::time::Duration::from_secs(5), consume(run))
                    .await
                    .expect("foreground ordinary child must finish");
                assert!(events.iter().any(|event| matches!(
                    event,
                    EngineEvent::ItemCompleted {
                        item: ThreadItem::AgentMessage { text, .. }, ..
                    } if text == "first"
                )));
                id
            } else {
                session.make_spawn_background_agent(
                    parent_depth,
                    Some("original-spawning-turn".into()),
                    session.permission_context(),
                    Arc::new(Vec::new),
                    false,
                    original_parent.clone(),
                    None,
                )(options)
                .await
                .expect("child creation must be accepted")
                .agent_id
            };
            finished(&session, &id).await;
            settle_ordinary_owners(&session).await;
            assert_ordinary_creation_bindings(
                &session,
                &id,
                parent_depth + 1,
                Some("original-spawning-turn"),
                original_parent.as_deref(),
            );
            let resumed = session.make_spawn_background_agent(
                7,
                Some("replacement-spawning-turn".into()),
                session.permission_context(),
                Arc::new(Vec::new),
                false,
                None,
                None,
            )(SpawnSubAgentOptions {
                prompt: "same ID follow-up".into(),
                resume: Some(id.clone()),
                ..Default::default()
            })
            .await
            .expect("child creation must be accepted");
            assert_eq!(resumed.agent_id, id);
            settle_ordinary_owners(&session).await;
            finished(&session, &id).await;
            assert_ordinary_creation_bindings(
                &session,
                &id,
                parent_depth + 1,
                Some("original-spawning-turn"),
                original_parent.as_deref(),
            );
            {
                let requests = requests.lock().unwrap();
                assert_eq!(requests.len(), child_request_offset + 2);
                if child_request_offset != 0 {
                    assert_eq!(
                        text_positions(&requests[0], "Original direct parent").len(),
                        1
                    );
                }
                assert_eq!(
                    text_positions(&requests[child_request_offset], "original child").len(),
                    1
                );
                assert_eq!(
                    text_positions(&requests[child_request_offset + 1], "original child").len(),
                    1
                );
                assert_eq!(
                    text_positions(&requests[child_request_offset + 1], "same ID follow-up").len(),
                    1
                );
            }
            let receipts = session
                .turn_index
                .lock()
                .unwrap()
                .agent_notifications
                .clone();
            let expected_receipts = if parent_depth > 0 {
                0
            } else if foreground_spawn {
                1
            } else {
                2
            };
            assert_eq!(receipts.len(), expected_receipts);
            for receipt in receipts {
                assert_eq!(receipt.agent_id.as_ref(), Some(&id));
                assert_eq!(receipt.source, "background-task");
                assert_eq!(receipt.turn_id, Some("original-spawning-turn".into()));
            }
            drop(parent_turn);
            session.shutdown_and_wait().await.unwrap();
            std::fs::remove_file(runtime_path_to_file_io_host(&resumed.output_file)).unwrap();
            if let Some(output_file) = parent_output_file {
                std::fs::remove_file(runtime_path_to_file_io_host(&output_file)).unwrap();
            }
        }
    }
}

#[tokio::test]
async fn explicit_user_stop_supersedes_recovery_waiting_for_input_durability() {
    for user_initiated in [false, true] {
        let (session, requests) = fixture(
            vec![text_turn("initial"), text_turn("must not run")],
            vec![],
        );
        let id = spawn_worker(
            &session,
            SpawnSubAgentOptions {
                prompt: "initial".into(),
                ..Default::default()
            },
        )
        .await;
        finished(&session, &id).await;
        settle_ordinary_owners(&session).await;
        session
            .conversation
            .queue()
            .await_pending_durability()
            .await
            .unwrap();
        let visible_before = session.conversation.queue().snapshot();
        let checkpoint = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let first = Arc::new(std::sync::atomic::AtomicBool::new(true));
        session.conversation.queue().set_persistence_sink(Arc::new({
            let checkpoint = checkpoint.clone();
            let release = release.clone();
            move |_| {
                let wait = first.swap(false, std::sync::atomic::Ordering::SeqCst);
                let checkpoint = checkpoint.clone();
                let release = release.clone();
                Box::pin(async move {
                    if wait {
                        checkpoint.notify_one();
                        release.acquire().await.unwrap().forget();
                    }
                    Ok(())
                })
            }
        }));
        let send = tokio::spawn({
            let session = session.clone();
            let id = id.clone();
            async move {
                session
                    .send_subagent_message(message(&id, "superseded direction", user_initiated))
                    .await
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), checkpoint.notified())
            .await
            .unwrap();
        assert_eq!(session.conversation.queue().snapshot(), visible_before);
        assert!(session
            .conversation
            .queue()
            .admitted_snapshot()
            .iter()
            .any(|input| input.agent_id.as_ref() == Some(&id)));
        assert!(session.background_task_registry().stop_by_user(&id));
        release.add_permits(1);
        let refusal = send.await.unwrap().unwrap().unwrap_err();
        settle_ordinary_owners(&session).await;
        session
            .conversation
            .queue()
            .await_pending_durability()
            .await
            .unwrap();
        assert!(refusal.contains("newer user cancellation"), "{refusal:?}");
        assert_eq!(requests.lock().unwrap().len(), 1);
        assert!(session
            .conversation
            .queue()
            .admitted_snapshot()
            .iter()
            .all(|input| input.agent_id.as_ref() != Some(&id)));
        assert_eq!(session.conversation.queue().snapshot(), visible_before);
        session.shutdown_and_wait().await.unwrap();
    }
}

#[tokio::test]
async fn running_initial_child_applies_current_policy_and_revoked_grants_at_tool_boundary() {
    use crate::permission::rules::parse_rule_strings;
    use crate::types::permission::{ApprovalPolicy, SandboxMode};
    for (sandbox_mode, approval_policy, planning) in [
        (
            SandboxMode::DangerFullAccess,
            ApprovalPolicy::OnRequest,
            false,
        ),
        (SandboxMode::ReadOnly, ApprovalPolicy::Never, false),
        (SandboxMode::DangerFullAccess, ApprovalPolicy::Never, true),
    ] {
        let blocked = gate();
        let observed = Arc::new(Mutex::new(Vec::new()));
        let probe = Arc::new(ChildPolicyProbe {
            observed: observed.clone(),
            inline_shell: false,
        });
        let (mutator, mutations) = plan_boundary::mutation_probe();
        let mut turns = vec![gate_turn(), child_policy_probe_turn(), text_turn("done")];
        if planning {
            turns.extend([
                plan_boundary::mutation_attempt_turn(),
                text_turn("planning follow-up done"),
            ]);
        }
        let isolated = IsolatedHardeningFixture::new(turns, vec![blocked.clone(), probe, mutator]);
        let session = &isolated.session;
        let requests = &isolated.requests;
        assert_eq!(
            session.collaboration_mode(),
            crate::planning::CollaborationMode::Default,
            "fresh config root: {}",
            isolated.config_root.path().display(),
        );
        install_probe_runtime_policy(
            session,
            SandboxMode::DangerFullAccess,
            ApprovalPolicy::OnRequest,
        );
        session.mode.permission.lock().unwrap().allow_rules =
            parse_rule_strings(&["shared".into(), "revoked".into()]);
        let id = spawn_worker(
            session,
            SpawnSubAgentOptions {
                prompt: "initial".into(),
                ..Default::default()
            },
        )
        .await;
        entered(&blocked).await;
        install_probe_runtime_policy(session, sandbox_mode, approval_policy);
        if planning {
            session
                .planning_runtime()
                .set_mode(crate::planning::CollaborationMode::Plan, None)
                .unwrap();
        }
        {
            let mut current = session.mode.permission.lock().unwrap();
            current.allow_rules = parse_rule_strings(&["shared".into(), "new_grant".into()]);
            current.deny_rules = parse_rule_strings(&["current_deny".into()]);
            current.ask_rules = parse_rule_strings(&["current_ask".into()]);
        }
        assert!(
            !session
                .send_subagent_message(message(&id, "live policy direction", false))
                .await
                .unwrap()
                .unwrap()
                .resumed
        );
        blocked.release.add_permits(1);
        finished(session, &id).await;
        {
            let observed = observed.lock().unwrap();
            assert_eq!(observed.len(), 1);
            let effective = &observed[0].permission;
            assert_eq!(observed[0].sandbox_mode, Some(sandbox_mode));
            assert_eq!(observed[0].approval_policy, Some(approval_policy));
            assert!(!observed[0].planning_read_only);
            let expected = parse_rule_strings(&["shared".into()]);
            assert_eq!(effective.allow_rules, expected);
            assert_eq!(
                effective.deny_rules,
                parse_rule_strings(&["current_deny".into()])
            );
            assert_eq!(
                effective.ask_rules,
                parse_rule_strings(&["current_ask".into()])
            );
        }
        assert_eq!(
            text_positions(&requests.lock().unwrap()[1], "live policy direction").len(),
            1
        );
        if planning {
            assert_eq!(
                session.collaboration_mode(),
                crate::planning::CollaborationMode::Plan
            );
            {
                let requests = requests.lock().unwrap();
                assert_eq!(requests.len(), 3);
                for request in requests.iter() {
                    plan_boundary::assert_query_view(
                        session,
                        request,
                        crate::planning::CollaborationMode::Default,
                    );
                }
            }
            assert!(
                session
                    .send_subagent_message(message(
                        &id,
                        "Inspect the next planning boundary.",
                        false
                    ))
                    .await
                    .unwrap()
                    .unwrap()
                    .resumed
            );
            finished(session, &id).await;
            {
                let requests = requests.lock().unwrap();
                assert_eq!(requests.len(), 5);
                for request in &requests[3..] {
                    plan_boundary::assert_query_view(
                        session,
                        request,
                        crate::planning::CollaborationMode::Plan,
                    );
                }
                plan_boundary::assert_mutation_rejected(&requests[4], &mutations);
            }
            let observations = observed.lock().unwrap();
            assert_eq!(observations.len(), 2);
            plan_boundary::assert_plan_permissions(&observations[1]);
        }
        session.shutdown_and_wait().await.unwrap();
    }
}

#[tokio::test]
async fn terminal_handoff_retains_an_input_whose_checkpoint_is_still_pending() {
    let (session, requests) = fixture(
        vec![text_turn("original complete"), text_turn("follow complete")],
        vec![],
    );
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release_handoff = Arc::new(tokio::sync::Semaphore::new(0));
    let (outcome_tx, outcome_rx) = tokio::sync::oneshot::channel();
    session.gate_ordinary_handoff(entered.clone(), release_handoff.clone(), outcome_tx);
    let id = spawn_worker(
        &session,
        SpawnSubAgentOptions {
            prompt: "original".into(),
            ..Default::default()
        },
    )
    .await;
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let blocked = Arc::new(tokio::sync::Notify::new());
    let release_checkpoint = Arc::new(tokio::sync::Semaphore::new(0));
    let first = Arc::new(std::sync::atomic::AtomicBool::new(true));
    session.conversation.queue().set_persistence_sink(Arc::new({
        let blocked = blocked.clone();
        let release_checkpoint = release_checkpoint.clone();
        move |_| {
            let wait = first.swap(false, std::sync::atomic::Ordering::SeqCst);
            let blocked = blocked.clone();
            let release_checkpoint = release_checkpoint.clone();
            Box::pin(async move {
                if wait {
                    blocked.notify_one();
                    release_checkpoint.acquire().await.unwrap().forget();
                }
                Ok(())
            })
        }
    }));
    let send = tokio::spawn({
        let session = session.clone();
        let id = id.clone();
        async move {
            session
                .send_subagent_message(message(&id, "pending at completion", false))
                .await
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), blocked.notified())
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while session.conversation.queue().admitted_snapshot().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(session.conversation.queue().snapshot().is_empty());
    release_handoff.add_permits(1);
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(5), outcome_rx)
            .await
            .unwrap()
            .unwrap(),
        "the terminal owner must retain the accepted but unpublished input"
    );
    assert_eq!(
        requests.lock().unwrap().len(),
        1,
        "no inference before checkpoint durability"
    );
    release_checkpoint.add_permits(1);
    let ack = send.await.unwrap().unwrap().unwrap();
    assert_eq!(ack.agent_id, id);
    finished(&session, &id).await;
    {
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            text_positions(&requests[1], "pending at completion").len(),
            1
        );
    }
    assert!(session
        .conversation
        .queue()
        .snapshot()
        .iter()
        .all(|item| item.agent_id.as_ref() != Some(&id)));
    assert!(session
        .conversation
        .queue()
        .admitted_snapshot()
        .iter()
        .all(|item| item.agent_id.as_ref() != Some(&id)));
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn name_prefixes_are_unique_or_rejected_without_rebinding() {
    let (session, requests) = fixture(
        vec![text_turn("alpha"), text_turn("beta"), text_turn("follow")],
        vec![],
    );
    let alpha = spawn_worker(
        &session,
        SpawnSubAgentOptions {
            prompt: "alpha original".into(),
            name: Some("writer-alpha".into()),
            ..Default::default()
        },
    )
    .await;
    finished(&session, &alpha).await;
    let beta = spawn_worker(
        &session,
        SpawnSubAgentOptions {
            prompt: "beta original".into(),
            name: Some("writer-beta".into()),
            ..Default::default()
        },
    )
    .await;
    finished(&session, &beta).await;
    assert!(session
        .send_subagent_message(message(&"writer".into(), "ambiguous", false))
        .await
        .unwrap()
        .unwrap_err()
        .contains("Ambiguous"));
    assert_eq!(requests.lock().unwrap().len(), 2);
    assert_eq!(
        session
            .send_subagent_message(message(&"writer-a".into(), "alpha follow-up", false))
            .await
            .unwrap()
            .unwrap()
            .agent_id,
        alpha
    );
    finished(&session, &alpha).await;
    assert_eq!(requests.lock().unwrap().len(), 3);
    assert_eq!(
        text_positions(&requests.lock().unwrap()[2], "alpha follow-up").len(),
        1
    );
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn ordinary_resume_captures_parent_seed_without_replacing_child_history() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let mut config = default_config();
    config.fork = true;
    config.agent_memory = false;
    config.memory_recall = false;
    let session = mk_session_with_deps(
        unique_subagent_test_deps(recording_deps(
            requests.clone(),
            vec![
                text_turn("initial done"),
                text_turn("foreground done"),
                text_turn("background done"),
            ],
        )),
        config,
        vec![],
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let parent_seed: GetConversation = Arc::new({
        let calls = calls.clone();
        move || {
            calls.fetch_add(1, Ordering::SeqCst);
            vec![fork_user("unrelated-parent-seed")]
        }
    });
    let background = session.make_spawn_background_agent(
        0,
        None,
        session.permission_context(),
        parent_seed.clone(),
        false,
        None,
        None,
    );
    let original = background(SpawnSubAgentOptions {
        prompt: "retained child initial".into(),
        fork_context: Some(ForkContext::None),
        ..Default::default()
    })
    .await
    .expect("child creation must be accepted");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    finished(&session, &original.agent_id).await;

    let foreground = session.make_spawn_sub_agent(
        0,
        CancellationToken::new(),
        session.permission_context(),
        parent_seed,
        false,
        None,
        None,
    );
    let run = foreground(SpawnSubAgentOptions {
        resume: Some(original.agent_id.clone()),
        prompt: "retained foreground follow-up".into(),
        ..Default::default()
    })
    .await
    .expect("child creation must be accepted");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(run.agent_id.as_str(), Some(&original.agent_id));
    let events = consume(run).await;
    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::ItemCompleted { item: ThreadItem::AgentMessage { text, .. }, .. }
            if text == "foreground done"
    )));
    finished(&session, &original.agent_id).await;

    let resumed = background(SpawnSubAgentOptions {
        resume: Some(original.agent_id.clone()),
        prompt: "retained background follow-up".into(),
        ..Default::default()
    })
    .await
    .expect("child creation must be accepted");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(resumed.agent_id, original.agent_id);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while requests.lock().unwrap().len() < 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    finished(&session, &original.agent_id).await;
    {
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        for request in &requests[1..] {
            assert_eq!(text_positions(request, "retained child initial").len(), 1);
            assert!(text_positions(request, "unrelated-parent-seed").is_empty());
        }
        assert_eq!(
            text_positions(&requests[1], "retained foreground follow-up").len(),
            1
        );
        assert_eq!(
            text_positions(&requests[2], "retained foreground follow-up").len(),
            1
        );
        assert_eq!(
            text_positions(&requests[2], "retained background follow-up").len(),
            1
        );
    }
    session.shutdown_and_wait().await.unwrap();
    std::fs::remove_file(runtime_path_to_file_io_host(&original.output_file)).unwrap();
}

#[tokio::test]
async fn ordinary_background_output_reports_failure_and_interruption() {
    for stopped in [false, true] {
        let blocked = gate();
        let turn = if stopped {
            gate_turn()
        } else {
            provider_error_turn("compatibility failure")
        };
        let expected = if stopped {
            concat!(
                "[failed] Sub-agent execution was interrupted.\n\n",
                "Partial activity; not a completed result:\n",
                "[tool] message_gate — error: gate cancelled"
            )
        } else {
            "[failed] compatibility failure"
        };
        let (session, _) = fixture(
            vec![turn],
            if stopped {
                vec![blocked.clone()]
            } else {
                vec![]
            },
        );
        let spawned = session.make_spawn_background_agent(
            0,
            None,
            session.permission_context(),
            Arc::new(Vec::new),
            false,
            None,
            None,
        )(SpawnSubAgentOptions {
            prompt: "output format probe".into(),
            ..Default::default()
        })
        .await
        .expect("child creation must be accepted");
        if stopped {
            entered(&blocked).await;
            assert!(session
                .background_task_registry()
                .stop_by_user(&spawned.agent_id));
        }
        finished(&session, &spawned.agent_id).await;
        assert_eq!(
            std::fs::read_to_string(runtime_path_to_file_io_host(&spawned.output_file)).unwrap(),
            expected
        );
        session.shutdown_and_wait().await.unwrap();
        std::fs::remove_file(runtime_path_to_file_io_host(&spawned.output_file)).unwrap();
    }
}

#[tokio::test]
async fn foreground_existing_worker_recovery_forwards_events_and_actual_refusal() {
    let (session, requests) = fixture(vec![text_turn("first"), text_turn("recovered")], vec![]);
    let id = spawn_worker(
        &session,
        SpawnSubAgentOptions {
            prompt: "original".into(),
            ..Default::default()
        },
    )
    .await;
    finished(&session, &id).await;
    let run = foreground(
        &session,
        SpawnSubAgentOptions {
            resume: Some(id.clone()),
            prompt: "foreground follow-up".into(),
            ..Default::default()
        },
    )
    .await
    .expect("child creation must be accepted");
    assert_eq!(run.agent_id.as_str(), Some(&id));
    let events = tokio::time::timeout(std::time::Duration::from_secs(5), consume(run))
        .await
        .unwrap();
    assert!(events
        .iter()
        .any(|event| matches!(event, EngineEvent::ItemCompleted {
        item: ThreadItem::AgentMessage { text, .. }, ..
    } if text == "recovered")));
    assert!(events
        .iter()
        .any(|event| matches!(event, EngineEvent::TurnCompleted { .. })));
    assert_eq!(requests.lock().unwrap().len(), 2);
    let refused = foreground(
        &session,
        SpawnSubAgentOptions {
            resume: Some("missing-worker".into()),
            prompt: "cannot run".into(),
            ..Default::default()
        },
    )
    .await
    .err()
    .expect("missing worker must refuse creation");
    assert!(refused.contains("Unknown ordinary"));
    assert_eq!(requests.lock().unwrap().len(), 2);
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn plan_ceiling_rejects_temporary_grants_added_after_context_creation() {
    let config = default_config();
    let parent: Arc<dyn crate::permission::engine::PermissionEngine> = Arc::new(
        crate::permission::engine::StaticPermissionEngine::new(&config),
    );
    let ceiling =
        crate::engine::session::subagent_permission_ceiling(&config, parent.clone(), None, None);
    let mut ctx = crate::tools::builtins::test_ctx::make_ctx("/tmp");
    ctx.planning_read_only = true;
    ctx.permission.allow_rules = crate::permission::rules::parse_rule_strings(&["write".into()]);
    let tool = crate::tools::builtins::write::create_write_tool();
    let input =
        exact_json!({"path":"/tmp/plan-ceiling-must-not-write.txt","content":"not executed"});
    assert!(matches!(
        parent.evaluate(tool.as_ref(), &input, &ctx).await,
        PermissionDecision::Allow { .. }
    ));
    assert!(matches!(
        ceiling.evaluate(tool.as_ref(), &input, &ctx).await,
        PermissionDecision::Deny { .. }
    ));
}

#[tokio::test]
async fn native_foreground_output_uses_resolved_builtin_identity_after_creation() {
    for (role, builtin) in [("Plan", true), ("Plan", false), ("Explore", true)] {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let definition = if builtin {
            crate::engine::builtin_agents::BUILTIN_AGENTS
                .iter()
                .find(|definition| definition.name == role)
                .unwrap()
                .clone()
        } else {
            AgentDefinition {
                name: role.into(),
                tools: Some(vec![]),
                ..Default::default()
            }
        };
        let mut config = default_config();
        config.agent_memory = false;
        config.memory_recall = false;
        let session = mk_session_with_provider_memory_store_and_agents(
            Arc::new(fake_provider()),
            unique_subagent_test_deps(recording_deps(requests.clone(), vec![text_turn("done")])),
            config,
            vec![],
            None,
            None,
            None,
            None,
            Some(vec![definition]),
        );
        let mut ctx = crate::tools::builtins::test_ctx::make_ctx("/tmp");
        ctx.permission = session.permission_context();
        ctx.background_tasks = Some(session.background_task_registry());
        ctx.spawn_sub_agent = Some(session.make_spawn_sub_agent(
            0,
            CancellationToken::new(),
            ctx.permission.clone(),
            Arc::new(Vec::new),
            false,
            None,
            None,
        ));
        let tool = crate::tools::builtins::task::create_task_tool(
            crate::tools::builtins::task::TaskToolOptions::default(),
        );
        let mut value = exact_json!({"model": "low", "description":"Investigate","prompt":"original","run_in_background":false});
        let RuntimeJson::Object(fields) = &mut value else {
            unreachable!()
        };
        fields.insert("agentType", RuntimeJson::from(role));
        let input = tool.parse_input(&value).unwrap();
        let result = tool.call(input, &ctx, None).await.unwrap();
        assert_eq!(
            result
                .serializable
                .output
                .get("ok")
                .and_then(RuntimeJson::as_bool),
            Some(true),
            "{:?}",
            result.serializable.output
        );
        assert_eq!(
            result.serializable.output.get("agentId").is_none(),
            builtin && role == "Plan",
            "only one-shot builtins omit the resume identity"
        );
        assert_eq!(requests.lock().unwrap().len(), 1);
        session.shutdown_and_wait().await.unwrap();
    }
}

#[tokio::test]
async fn rejected_manual_checkpoint_does_not_clear_user_cancellation() {
    let gate = gate();
    let (session, requests) = fixture(
        vec![gate_turn(), text_turn("must not run")],
        vec![gate.clone()],
    );
    let id = spawn_worker(
        &session,
        SpawnSubAgentOptions {
            prompt: "original".into(),
            ..Default::default()
        },
    )
    .await;
    {
        let diagnostic_wait = crate::goal::testing_phase::Span::start(
            "fixture.wait",
            &format!("site=case48.toolEntered worker={id:?}"),
        );
        let diagnostic_count = DiagnosticRequestCount {
            site: "case48.toolEntered",
            requests: requests.clone(),
        };
        entered(&gate).await;
        diagnostic_wait.finish("ok");
        drop(diagnostic_count);
    }
    assert!(session.background_task_registry().stop_by_user(&id));
    finished(&session, &id).await;
    session
        .conversation
        .queue()
        .set_persistence_sink(Arc::new(|_| {
            Box::pin(async { Err("fixture checkpoint rejected".into()) })
        }));
    assert!(session
        .send_subagent_message(message(&id, "failed manual recovery", true))
        .await
        .unwrap()
        .is_err());
    let automatic = session
        .send_subagent_message(message(&id, "automatic must remain blocked", false))
        .await
        .unwrap()
        .unwrap_err();
    assert!(automatic.contains("cancelled by the user"), "{automatic:?}");
    assert_eq!(requests.lock().unwrap().len(), 1);
    assert!(session.conversation.queue().persistence_error().is_some());
    assert!(session
        .conversation
        .queue()
        .snapshot()
        .iter()
        .all(|item| item.agent_id.as_ref() != Some(&id)));
    assert!(
        session.shutdown_and_wait().await.is_err(),
        "failed queue durability must not produce a successful shutdown"
    );
}

#[tokio::test]
async fn cancellation_removes_input_even_while_its_checkpoint_is_unpublished() {
    let gate = gate();
    let (session, requests) = fixture(
        vec![gate_turn(), text_turn("fresh manual")],
        vec![gate.clone()],
    );
    let id = spawn_worker(
        &session,
        SpawnSubAgentOptions {
            prompt: "original".into(),
            ..Default::default()
        },
    )
    .await;
    entered(&gate).await;
    let blocked = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let first = Arc::new(std::sync::atomic::AtomicBool::new(true));
    session.conversation.queue().set_persistence_sink(Arc::new({
        let blocked = blocked.clone();
        let release = release.clone();
        move |_| {
            let wait = first.swap(false, std::sync::atomic::Ordering::SeqCst);
            let blocked = blocked.clone();
            let release = release.clone();
            Box::pin(async move {
                if wait {
                    blocked.notify_one();
                    release.acquire().await.unwrap().forget();
                }
                Ok(())
            })
        }
    }));
    let send = tokio::spawn({
        let session = session.clone();
        let id = id.clone();
        async move {
            session
                .send_subagent_message(message(&id, "unpublished stale", false))
                .await
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), blocked.notified())
        .await
        .unwrap();
    assert!(session.conversation.queue().snapshot().is_empty());
    assert_eq!(session.conversation.queue().admitted_snapshot().len(), 1);
    assert!(session.background_task_registry().stop_by_user(&id));
    release.add_permits(1);
    let outcome = send.await.unwrap().unwrap();
    assert!(
        outcome.is_err(),
        "a newer cancellation supersedes the in-flight input"
    );
    finished(&session, &id).await;
    assert!(
        session
            .conversation
            .queue()
            .snapshot()
            .iter()
            .all(|item| item.agent_id.as_ref() != Some(&id))
            && session
                .conversation
                .queue()
                .admitted_snapshot()
                .iter()
                .all(|item| item.agent_id.as_ref() != Some(&id)),
        "cancelled queue={:?}; admitted={:?}",
        session.conversation.queue().snapshot(),
        session.conversation.queue().admitted_snapshot()
    );
    session
        .send_subagent_message(message(&id, "fresh trusted", true))
        .await
        .unwrap()
        .unwrap();
    finished(&session, &id).await;
    {
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(text_positions(&requests[1], "unpublished stale").is_empty());
        assert_eq!(text_positions(&requests[1], "fresh trusted").len(), 1);
    }
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn persistence_barrier_does_not_close_admission_for_parent_or_child() {
    let queue = crate::engine::message_queue::MessageQueue::for_session("ordinary-barrier".into());
    queue.await_pending_durability().await.unwrap();
    let make = |id: &str| crate::engine::message_queue::QueuedMessage {
        trigger_turn: None,
        value: crate::engine::message_queue::QueuedValue::Text("input".into()),
        mode: "subagent-message".into(),
        uuid: Some(id.into()),
        session_id: Some("ordinary-barrier".into()),
        priority: Some(crate::engine::message_queue::QueuePriority::Next),
        is_meta: Some(true),
        origin: None,
        agent_id: Some("child".into()),
        correlation_id: None,
        delivery: Some(crate::engine::message_queue::SteerDelivery::User),
        display_metadata: None,
        pdf_bytes: 0,
        pdf_documents: 0,
    };
    queue.enqueue(make("message-one")).unwrap();
    queue.await_pending_durability().await.unwrap();
    let reserved = queue
        .reserve_agent_resume_durable(&"child".into())
        .await
        .unwrap();
    assert_eq!(reserved.len(), 1);
    queue.release_pending_resume(&reserved);
    queue.await_pending_durability().await.unwrap();
    queue.enqueue(make("message-two")).unwrap();
    assert_eq!(queue.snapshot().len(), 2);
}

async fn foreground(
    session: &Arc<Session>,
    mut options: SpawnSubAgentOptions,
) -> Result<ForegroundSubAgentRun, RuntimeString> {
    options.fork_context.get_or_insert(ForkContext::None);
    session.make_spawn_sub_agent(
        0,
        CancellationToken::new(),
        session.permission_context(),
        Arc::new(Vec::new),
        false,
        None,
        None,
    )(options)
    .await
}

async fn consume(run: ForegroundSubAgentRun) -> Vec<EngineEvent> {
    run.events.collect::<Vec<_>>().await
}

#[tokio::test]
async fn foreground_completed_custom_worker_keeps_history_for_background_recovery() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let session = mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        unique_subagent_test_deps(recording_deps(
            requests.clone(),
            vec![text_turn("first"), text_turn("recovered")],
        )),
        default_config(),
        vec![],
        None,
        None,
        None,
        None,
        Some(vec![AgentDefinition {
            name: "custom-worker".into(),
            tools: Some(vec![]),
            ..Default::default()
        }]),
    );
    let run = foreground(
        &session,
        SpawnSubAgentOptions {
            prompt: "FG original".into(),
            name: Some("stable-fg".into()),
            agent_type: Some("custom-worker".into()),
            ..Default::default()
        },
    )
    .await
    .expect("child creation must be accepted");
    let id = run.agent_id.as_str().unwrap().clone();
    let events = consume(run).await;
    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::TurnCompleted {
            stop_reason: TerminalReason::Completed,
            ..
        }
    )));
    finished(&session, &id).await;
    let result = session
        .send_subagent_message(message(&"stable-fg".into(), "FG follow-up", false))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.agent_id, id);
    assert!(result.resumed);
    finished(&session, &id).await;
    {
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(text_positions(&requests[1], "FG original").len(), 1);
        assert_eq!(text_positions(&requests[1], "FG follow-up").len(), 1);
    }
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn foreground_running_message_uses_real_events_and_next_tool_boundary() {
    let gate = gate();
    let (session, requests) = fixture(vec![gate_turn(), text_turn("done")], vec![gate.clone()]);
    let run = foreground(
        &session,
        SpawnSubAgentOptions {
            prompt: "FG initial".into(),
            ..Default::default()
        },
    )
    .await
    .expect("child creation must be accepted");
    let id = run.agent_id.as_str().unwrap().clone();
    let consumer = tokio::spawn(consume(run));
    entered(&gate).await;
    assert!(
        !session
            .send_subagent_message(message(&id, "FG during tool", false))
            .await
            .unwrap()
            .unwrap()
            .resumed
    );
    gate.release.add_permits(1);
    let events = consumer.await.unwrap();
    finished(&session, &id).await;
    assert!(events
        .iter()
        .any(|event| matches!(event, EngineEvent::ItemCompleted { .. })));
    assert_eq!(requests.lock().unwrap().len(), 2);
    assert_eq!(
        text_positions(&requests.lock().unwrap()[1], "FG during tool").len(),
        1
    );
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn root_interrupt_releases_foreground_observation_while_the_accepted_child_continues() {
    let gate = gate();
    let (session, requests) = fixture(
        vec![gate_turn(), text_turn("independent child complete")],
        vec![gate.clone()],
    );
    let caller = CancellationToken::new();
    let root_turn = ActiveTurnLease::try_new(
        session.clone(),
        session.model_runtime().1.model,
        caller.clone(),
    )
    .unwrap();
    let run = session.make_spawn_sub_agent(
        0,
        caller.clone(),
        session.permission_context(),
        Arc::new(Vec::new),
        false,
        None,
        None,
    )(SpawnSubAgentOptions {
        prompt: "Accepted foreground execution".into(),
        fork_context: Some(ForkContext::None),
        ..Default::default()
    })
    .await
    .expect("foreground input must be accepted");
    let id = run.agent_id.as_str().unwrap().clone();
    entered(&gate).await;
    let child_abort = session
        .subagents
        .background_agents
        .lock()
        .unwrap()
        .get(&id.try_to_utf8().unwrap())
        .unwrap()
        .abort
        .clone();
    let observer = tokio::spawn(consume(run));
    session.interrupt();
    assert!(caller.is_cancelled());
    tokio::time::timeout(std::time::Duration::from_secs(5), observer)
        .await
        .expect("root interruption must release its observer without waiting for the child")
        .unwrap();
    assert!(!child_abort.is_cancelled());
    assert_eq!(requests.lock().unwrap().len(), 1);
    drop(root_turn);
    gate.release.add_permits(1);
    finished(&session, &id).await;
    assert!(!child_abort.is_cancelled());
    assert_eq!(requests.lock().unwrap().len(), 2);
    assert!(session.background_agent_summaries().iter().any(|agent| {
        agent.id == id.try_to_utf8().unwrap() && agent.status == BackgroundTaskStatus::Completed
    }));
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn foreground_observer_drop_preserves_child_execution_and_later_recovery() {
    let gate = gate();
    let (session, requests) = fixture(
        vec![
            gate_turn(),
            text_turn("completed after observer drop"),
            text_turn("resumed after observer drop"),
        ],
        vec![gate.clone()],
    );
    let run = foreground(
        &session,
        SpawnSubAgentOptions {
            prompt: "FG before drop".into(),
            ..Default::default()
        },
    )
    .await
    .expect("child creation must be accepted");
    let id = run.agent_id.as_str().unwrap().clone();
    entered(&gate).await;
    let abort = session
        .subagents
        .background_agents
        .lock()
        .unwrap()
        .get(&id.try_to_utf8().unwrap())
        .unwrap()
        .abort
        .clone();
    drop(run);
    assert!(
        !abort.is_cancelled(),
        "dropping an observer must not cancel its accepted child"
    );
    assert_eq!(requests.lock().unwrap().len(), 1);
    gate.release.add_permits(1);
    finished(&session, &id).await;
    assert!(!abort.is_cancelled());
    assert_eq!(requests.lock().unwrap().len(), 2);
    let result = session
        .send_subagent_message(message(&id, "after caller drop", false))
        .await
        .unwrap()
        .unwrap();
    assert!(result.resumed);
    finished(&session, &id).await;
    assert_eq!(requests.lock().unwrap().len(), 3);
    assert_eq!(
        text_positions(&requests.lock().unwrap()[2], "after caller drop").len(),
        1
    );
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn foreground_model_stop_and_user_cancel_have_distinct_recovery_authority() {
    for user in [false, true] {
        let gate = gate();
        let (session, _) = fixture(
            vec![gate_turn(), text_turn("recovered")],
            vec![gate.clone()],
        );
        let run = foreground(
            &session,
            SpawnSubAgentOptions {
                prompt: "FG stop".into(),
                ..Default::default()
            },
        )
        .await
        .expect("child creation must be accepted");
        let id = run.agent_id.as_str().unwrap().clone();
        let consumer = tokio::spawn(consume(run));
        entered(&gate).await;
        let registry = session.background_task_registry();
        assert!(if user {
            registry.stop_by_user(&id)
        } else {
            registry.stop(&id)
        });
        consumer.await.unwrap();
        finished(&session, &id).await;
        let automatic = session
            .send_subagent_message(message(&id, "automatic recovery", false))
            .await
            .unwrap();
        if user {
            assert!(automatic.unwrap_err().contains("cancelled by the user"));
            assert!(
                session
                    .send_subagent_message(message(&id, "trusted recovery", true))
                    .await
                    .unwrap()
                    .unwrap()
                    .resumed
            );
        } else {
            assert!(automatic.unwrap().resumed);
        }
        finished(&session, &id).await;
        session.shutdown_and_wait().await.unwrap();
    }
}

#[tokio::test]
async fn dropped_send_waiter_cannot_abandon_the_registered_launcher() {
    let (session, requests) = fixture(
        vec![text_turn("first"), text_turn("retained message")],
        vec![],
    );
    let id = spawn_worker(
        &session,
        SpawnSubAgentOptions {
            prompt: "initial".into(),
            ..Default::default()
        },
    )
    .await;
    finished(&session, &id).await;
    let caller = session.clone();
    let target = id.clone();
    let waiter = tokio::spawn(async move {
        caller
            .send_subagent_message(message(&target, "retained delivery", false))
            .await
    });
    tokio::task::yield_now().await;
    waiter.abort();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while requests.lock().unwrap().len() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    finished(&session, &id).await;
    assert_eq!(
        text_positions(&requests.lock().unwrap()[1], "retained delivery").len(),
        1
    );
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn user_cancel_discards_unconsumed_old_input_before_trusted_new_input() {
    let gate = gate();
    let (session, requests) = fixture(
        vec![gate_turn(), text_turn("fresh manual")],
        vec![gate.clone()],
    );
    let id = spawn_worker(
        &session,
        SpawnSubAgentOptions {
            prompt: "initial".into(),
            ..Default::default()
        },
    )
    .await;
    entered(&gate).await;
    session
        .send_subagent_message(message(&id, "stale cancelled input", false))
        .await
        .unwrap()
        .unwrap();
    session.background_task_registry().stop_by_user(&id);
    finished(&session, &id).await;
    session
        .send_subagent_message(message(&id, "fresh trusted input", true))
        .await
        .unwrap()
        .unwrap();
    finished(&session, &id).await;
    {
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(text_positions(&requests[1], "stale cancelled input").is_empty());
        assert_eq!(text_positions(&requests[1], "fresh trusted input").len(), 1);
    }
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn actual_child_registry_identity_survives_runtime_history() {
    let (session, requests) = fixture(vec![text_turn("first"), text_turn("next")], vec![]);
    let id = spawn_worker(
        &session,
        SpawnSubAgentOptions {
            prompt: "initial".into(),
            ..Default::default()
        },
    )
    .await;
    finished(&session, &id).await;
    let registry = session.background_task_registry_for(Some("real-child-sender".into()));
    assert_eq!(registry.caller_agent_id(), Some("real-child-sender".into()));
    let mut request = message(&id, "sender body", true);
    request.sender_agent_id = Some("forged-parent".into());
    registry.send_message(request).await.unwrap().unwrap();
    finished(&session, &id).await;
    {
        let requests = requests.lock().unwrap();
        let user = requests[1]
            .messages
            .iter()
            .find_map(|message| {
                match message {
            Message::User(user)
                if user.content.iter().any(
                    |block| matches!(block, ContentBlock::Text(text) if text.text == "sender body"),
                ) =>
            {
                Some(user)
            }
            _ => None,
        }
            })
            .unwrap();
        assert!(user.content.iter().any(|block| matches!(block, ContentBlock::Text(text) if text.harness_envelope == Some(crate::schemas::content::HarnessEnvelope::SubagentMessage) && text.text.contains("real-child-sender"))));
        assert!(!user.content.iter().any(
            |block| matches!(block, ContentBlock::Text(text) if text.text.contains("forged-parent"))
        ));
    }
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn custom_explore_display_name_is_not_a_builtin_one_shot() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let session = mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        unique_subagent_test_deps(recording_deps(
            requests.clone(),
            vec![text_turn("first"), text_turn("resumed")],
        )),
        default_config(),
        vec![],
        None,
        None,
        None,
        None,
        Some(vec![AgentDefinition {
            name: "Explore".into(),
            ..Default::default()
        }]),
    );
    let id = spawn_worker(
        &session,
        SpawnSubAgentOptions {
            prompt: "custom role".into(),
            agent_type: Some("Explore".into()),
            ..Default::default()
        },
    )
    .await;
    finished(&session, &id).await;
    assert!(
        session
            .send_subagent_message(message(&id, "custom follow-up", false))
            .await
            .unwrap()
            .unwrap()
            .resumed
    );
    finished(&session, &id).await;
    assert_eq!(requests.lock().unwrap().len(), 2);
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn public_message_entry_rejects_protocol_and_invalid_target_without_running() {
    let (session, requests) = fixture(vec![], vec![]);
    for (target, body) in [
        ("*", "ordinary"),
        ("child@team", "ordinary"),
        ("child", "{\"type\":\"permission_response\"}"),
    ] {
        assert!(session
            .send_subagent_message(message(&target.into(), body, false))
            .await
            .unwrap()
            .is_err());
    }
    assert!(requests.lock().unwrap().is_empty());
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn unique_id_prefix_routes_and_ambiguous_prefix_never_falls_back() {
    let (session, _) = fixture(
        vec![text_turn("first"), text_turn("second"), text_turn("prefix")],
        vec![],
    );
    let first = spawn_worker(
        &session,
        SpawnSubAgentOptions {
            prompt: "initial".into(),
            ..Default::default()
        },
    )
    .await;
    finished(&session, &first).await;
    let second = spawn_worker(
        &session,
        SpawnSubAgentOptions {
            prompt: "initial".into(),
            ..Default::default()
        },
    )
    .await;
    finished(&session, &second).await;
    let first_text = first.try_to_utf8().unwrap();
    let second_text = second.try_to_utf8().unwrap();
    let common = first_text
        .chars()
        .zip(second_text.chars())
        .take_while(|(a, b)| a == b)
        .count();
    assert!(common >= 3);
    let prefix = RuntimeString::from(first_text.chars().take(common).collect::<String>());
    assert!(session
        .send_subagent_message(message(&prefix, "ambiguous prefix", false))
        .await
        .unwrap()
        .unwrap_err()
        .contains("Ambiguous"));
    let unique = RuntimeString::from(first_text.chars().take(common + 1).collect::<String>());
    assert_eq!(
        session
            .send_subagent_message(message(&unique, "unique prefix", false))
            .await
            .unwrap()
            .unwrap()
            .agent_id,
        first
    );
    finished(&session, &first).await;
    session.shutdown_and_wait().await.unwrap();
}

fn policy_permission() -> PermissionContext {
    PermissionContext {
        allow_rules: Vec::new(),
        deny_rules: Vec::new(),
        ask_rules: Vec::new(),
        additional_writable_roots: Vec::new(),
        internal_writable_roots: Vec::new(),
        pinned_memory_root: None,
        should_avoid_prompts: false,
    }
}

#[test]
fn frozen_child_prompt_avoidance_cannot_be_relaxed_by_the_parent() {
    use crate::engine::session::tighten_subagent_permission;
    for original in [false, true] {
        for current in [false, true] {
            let mut admitted = policy_permission();
            admitted.should_avoid_prompts = original;
            let mut live = policy_permission();
            live.should_avoid_prompts = current;
            assert_eq!(
                tighten_subagent_permission(&admitted, &live, true).should_avoid_prompts,
                original || current
            );
        }
    }
}

#[test]
fn frozen_child_grants_and_roots_are_intersected_without_importing_live_authority() {
    use crate::engine::session::tighten_subagent_permission;
    use crate::permission::rules::parse_rule_strings;
    let rules = |names: &[&str]| {
        parse_rule_strings(
            &names
                .iter()
                .map(|name| RuntimeString::from(*name))
                .collect::<Vec<_>>(),
        )
    };
    let mut original = policy_permission();
    original.allow_rules = rules(&["shared", "revoked"]);
    original.deny_rules = rules(&["original_deny"]);
    original.ask_rules = rules(&["original_ask"]);
    original.additional_writable_roots = vec!["/shared".into(), "/revoked".into()];
    let mut current = policy_permission();
    current.allow_rules = rules(&["shared", "new_grant"]);
    current.deny_rules = rules(&["current_deny"]);
    current.ask_rules = rules(&["current_ask"]);
    current.additional_writable_roots = vec!["/shared".into(), "/new_root".into()];
    let effective = tighten_subagent_permission(&original, &current, true);
    assert_eq!(effective.allow_rules, rules(&["shared"]));
    assert_eq!(
        effective.additional_writable_roots,
        vec![RuntimePath::from("/shared")]
    );
    assert_eq!(
        effective.deny_rules,
        rules(&["original_deny", "current_deny"])
    );
    assert_eq!(effective.ask_rules, rules(&["original_ask", "current_ask"]));
}

struct PolicyNoExecSandbox(crate::types::sandbox::SandboxKind);

impl crate::types::sandbox::SandboxBackend for PolicyNoExecSandbox {
    fn kind(&self) -> crate::types::sandbox::SandboxKind {
        self.0
    }
    fn is_available(&self) -> bool {
        true
    }
    fn transform(
        &self,
        _: &crate::types::sandbox::ExecRequest,
        _: &crate::types::sandbox::SandboxPolicy,
    ) -> crate::types::sandbox::SpawnSpec {
        panic!("A policy probe must not launch a subprocess")
    }
}

fn install_probe_runtime_policy(
    session: &Arc<Session>,
    sandbox_mode: crate::types::permission::SandboxMode,
    approval_policy: crate::types::permission::ApprovalPolicy,
) {
    let current = session.runtime_policy_snapshot();
    let kind = if sandbox_mode == crate::types::permission::SandboxMode::DangerFullAccess {
        crate::types::sandbox::SandboxKind::Process
    } else {
        crate::types::sandbox::SandboxKind::Seatbelt
    };
    let mut state = current.state();
    state.sandbox_mode = Some(sandbox_mode);
    state.approval_policy = Some(approval_policy);
    state.generation += 1;
    *session.runtime_policy.write().unwrap() =
        Arc::new(crate::engine::session::RuntimePolicySnapshot {
            state,
            sandbox_backend: Arc::new(PolicyNoExecSandbox(kind)),
        });
}

fn probe_runtime_policy(
    kind: crate::types::sandbox::SandboxKind,
    network_allowed: bool,
) -> Arc<crate::engine::session::RuntimePolicySnapshot> {
    Arc::new(crate::engine::session::RuntimePolicySnapshot {
        state: crate::engine::session::RuntimePolicyState {
            approval_policy: None,
            sandbox_mode: Some(if kind == crate::types::sandbox::SandboxKind::Process {
                crate::types::permission::SandboxMode::DangerFullAccess
            } else {
                crate::types::permission::SandboxMode::WorkspaceWrite
            }),
            approvals_reviewer: None,
            generation: 1,
            sandbox_workspace_write: crate::schemas::config::SandboxWorkspaceWrite {
                network_access: network_allowed,
                ..Default::default()
            },
            network_allowed,
        },
        sandbox_backend: Arc::new(PolicyNoExecSandbox(kind)),
    })
}

#[test]
fn child_runtime_policy_keeps_native_isolation_and_intersects_network() {
    use crate::engine::session::intersect_subagent_runtime_policy;
    use crate::types::sandbox::SandboxKind;
    for (original_kind, current_kind, expected_kind) in [
        (
            SandboxKind::Seatbelt,
            SandboxKind::Process,
            SandboxKind::Seatbelt,
        ),
        (
            SandboxKind::Process,
            SandboxKind::Seatbelt,
            SandboxKind::Seatbelt,
        ),
        (
            SandboxKind::Process,
            SandboxKind::Process,
            SandboxKind::Process,
        ),
    ] {
        for original_allowed in [false, true] {
            for current_allowed in [false, true] {
                let original = probe_runtime_policy(original_kind, original_allowed);
                let current = probe_runtime_policy(current_kind, current_allowed);
                let effective = intersect_subagent_runtime_policy(&original, &current);
                assert_eq!(effective.sandbox_backend.kind(), expected_kind);
                assert_eq!(
                    effective.state.network_allowed,
                    original_allowed && current_allowed
                );
                let expected = if expected_kind == current_kind {
                    &current
                } else {
                    &original
                };
                assert!(Arc::ptr_eq(
                    &effective.sandbox_backend,
                    &expected.sandbox_backend
                ));
            }
        }
    }
}

#[test]
fn held_child_runtime_policy_revokes_roots_without_importing_new_parent_grants() {
    use crate::engine::session::intersect_subagent_runtime_state;
    use crate::types::permission::{ApprovalPolicy, SandboxMode};
    let original = probe_runtime_policy(crate::types::sandbox::SandboxKind::Seatbelt, true);
    let current = probe_runtime_policy(crate::types::sandbox::SandboxKind::Seatbelt, false);
    let mut original = original.state();
    let mut current = current.state();
    original.sandbox_workspace_write.writable_roots = vec![
        "/workspace/shared".into(),
        "/workspace/revoked".into(),
        "/workspace/narrow/child".into(),
    ];
    current.sandbox_workspace_write.writable_roots = vec![
        "/workspace/shared/child".into(),
        "/workspace/new".into(),
        "/workspace/narrow".into(),
    ];
    original.sandbox_workspace_write.exclude_tmpdir_env_var = true;
    current.sandbox_workspace_write.exclude_slash_tmp = true;
    current.approval_policy = Some(ApprovalPolicy::Never);
    current.generation = 9;
    let effective = intersect_subagent_runtime_state(&original, &current);
    assert_eq!(
        effective.sandbox_workspace_write.writable_roots,
        vec![
            RuntimeString::from("/workspace/shared/child"),
            RuntimeString::from("/workspace/narrow/child")
        ]
    );
    assert!(effective.sandbox_workspace_write.exclude_tmpdir_env_var);
    assert!(effective.sandbox_workspace_write.exclude_slash_tmp);
    assert!(!effective.sandbox_workspace_write.network_access);
    assert!(!effective.network_allowed);
    assert_eq!(effective.approval_policy, Some(ApprovalPolicy::Never));
    assert_eq!(effective.generation, 9);
    assert_eq!(effective.sandbox_mode, Some(SandboxMode::WorkspaceWrite));
    assert_eq!(original.sandbox_workspace_write.writable_roots.len(), 3);
}

#[test]
fn canonical_extra_root_intersection_reaches_tool_context_without_revoking_scoped_roots() {
    use crate::engine::session::{constrain_subagent_canonical_roots, SubagentPermissionOrigin};
    let mut captured =
        probe_runtime_policy(crate::types::sandbox::SandboxKind::Seatbelt, false).state();
    captured.sandbox_workspace_write.writable_roots =
        vec!["/outside/wide".into(), "/outside/revoked".into()];
    let mut effective = captured.clone();
    effective.sandbox_workspace_write.writable_roots = vec!["/outside/wide/narrow".into()];
    let mut baseline = policy_permission();
    baseline.additional_writable_roots = vec![
        "/outside/wide".into(),
        "/outside/revoked".into(),
        "/tmp".into(),
    ];
    let origin = SubagentPermissionOrigin::capture(&baseline);
    let mut permission = baseline;
    permission
        .additional_writable_roots
        .push("/task-scoped".into());
    constrain_subagent_canonical_roots(&mut permission, &captured, &effective, Some(&origin));
    assert_eq!(
        permission.additional_writable_roots,
        vec![
            RuntimePath::from("/outside/wide/narrow"),
            RuntimePath::from("/tmp"),
            RuntimePath::from("/task-scoped")
        ]
    );
    assert!(!permission
        .additional_writable_roots
        .contains(&RuntimePath::from("/outside/revoked")));
}

#[cfg(unix)]
#[tokio::test]
async fn canonical_child_roots_reject_symlink_escape_and_keep_missing_descendants_usable() {
    use crate::engine::session::{
        constrain_subagent_canonical_roots, intersect_subagent_runtime_state, RuntimePolicySnapshot,
    };
    use crate::permission::engine::{PermissionEngine, StaticPermissionEngine};
    use crate::types::permission::ApprovalPolicy;
    use crate::types::sandbox::{
        ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec,
    };

    struct CaptureRoots(Arc<Mutex<Vec<RuntimeString>>>);
    impl SandboxBackend for CaptureRoots {
        fn kind(&self) -> SandboxKind {
            SandboxKind::Seatbelt
        }
        fn is_available(&self) -> bool {
            true
        }
        fn transform(&self, request: &ExecRequest, policy: &SandboxPolicy) -> SpawnSpec {
            *self.0.lock().unwrap() = policy.writable_roots.clone();
            SpawnSpec {
                file: "must-not-execute".into(),
                args: Vec::new(),
                cwd: request.cwd.clone(),
                env: Default::default(),
            }
        }
    }

    let fixture = HardeningConfigRoot::new();
    let root = std::fs::canonicalize(fixture.path()).unwrap();
    let cwd = root.join("workspace");
    let allowed = root.join("allowed");
    let outside = root.join("outside");
    for directory in [&cwd, &allowed, &outside] {
        std::fs::create_dir_all(directory).unwrap();
    }
    std::os::unix::fs::symlink(&outside, allowed.join("escape")).unwrap();
    let allowed_root = runtime_path_from_host_path(&allowed);
    let mut original = probe_runtime_policy(SandboxKind::Seatbelt, false).state();
    original.approval_policy = Some(ApprovalPolicy::Never);
    original.sandbox_workspace_write.writable_roots =
        vec![allowed_root.as_runtime_string().clone()];
    let mut config = default_config();
    config.allow = vec!["write".into()];
    let engine = StaticPermissionEngine::new(&config);
    let write = crate::tools::builtins::write::create_write_tool();

    for descendant in [allowed.join("escape"), allowed.join("escape/missing/child")] {
        let requested_root = runtime_path_from_host_path(&descendant);
        let mut current = original.clone();
        current.sandbox_workspace_write.writable_roots =
            vec![requested_root.as_runtime_string().clone()];
        let effective = intersect_subagent_runtime_state(&original, &current);
        assert!(effective.sandbox_workspace_write.writable_roots.is_empty());
        let mut permission = policy_permission();
        permission.additional_writable_roots = vec![allowed_root.clone(), requested_root];
        constrain_subagent_canonical_roots(&mut permission, &original, &effective, None);
        assert!(
            permission.additional_writable_roots.is_empty(),
            "an invalid canonical descendant cannot become an independent grant"
        );
        let captured_roots = Arc::new(Mutex::new(Vec::new()));
        let mut ctx = crate::tools::builtins::test_ctx::make_ctx(cwd.to_str().unwrap());
        ctx.permission = permission;
        ctx.sandbox = Arc::new(CaptureRoots(captured_roots.clone()));
        RuntimePolicySnapshot {
            state: effective,
            sandbox_backend: ctx.sandbox.clone(),
        }
        .apply_to_context(&mut ctx);
        ctx.sandbox.transform(
            &ExecRequest {
                argv: vec!["must-not-execute".into()],
                cwd: ctx.cwd.clone(),
                env: None,
                timeout_ms: None,
            },
            &SandboxPolicy::default(),
        );
        assert_eq!(
            *captured_roots.lock().unwrap(),
            vec![ctx.cwd.as_runtime_string().clone()]
        );
        let target = runtime_path_from_host_path(&outside.join("blocked.txt"));
        let mut input = crate::runtime_values::RuntimeObject::new();
        input.insert(
            "path",
            RuntimeJson::String(target.as_runtime_string().clone()),
        );
        input.insert("content", RuntimeJson::String("must not be written".into()));
        let input = RuntimeJson::Object(input);
        assert!(matches!(
            engine.evaluate(write.as_ref(), &input, &ctx).await,
            PermissionDecision::Deny { .. }
        ));
        assert!(matches!(
            engine
                .evaluate_without_allow_prefix_rules(write.as_ref(), &input, &ctx)
                .await,
            PermissionDecision::Deny { .. }
        ));
        assert!(crate::fs::path_safety::assert_writable(
            &target,
            &crate::fs::path_safety::PathContext {
                cwd: &ctx.cwd,
                additional_writable_roots: &ctx.permission.additional_writable_roots,
                unconfined_writes: false,
            }
        )
        .await
        .is_err());
    }
    assert!(!outside.join("blocked.txt").exists());

    let alias = root.join("allowed-alias");
    std::os::unix::fs::symlink(&allowed, &alias).unwrap();
    let cold_root = runtime_path_from_host_path(&allowed.join("cold/nested"));
    let mut aliased = original.clone();
    aliased.sandbox_workspace_write.writable_roots =
        vec![runtime_path_from_host_path(&alias).into_runtime_string()];
    let mut requested = original.clone();
    requested.sandbox_workspace_write.writable_roots = vec![cold_root.as_runtime_string().clone()];
    let effective = intersect_subagent_runtime_state(&aliased, &requested);
    assert_eq!(
        effective.sandbox_workspace_write.writable_roots,
        vec![cold_root.as_runtime_string().clone()]
    );
    let mut permission = policy_permission();
    permission.additional_writable_roots = vec![runtime_path_from_host_path(&alias)];
    constrain_subagent_canonical_roots(&mut permission, &aliased, &effective, None);
    assert_eq!(
        permission.additional_writable_roots,
        vec![cold_root.clone()]
    );
    assert!(crate::fs::path_safety::assert_writable(
        &runtime_path_from_host_path(&allowed.join("cold/nested/new.txt")),
        &crate::fs::path_safety::PathContext {
            cwd: &runtime_path_from_host_path(&cwd),
            additional_writable_roots: &permission.additional_writable_roots,
            unconfined_writes: false,
        }
    )
    .await
    .is_ok());
    assert!(
        !allowed.join("cold").exists(),
        "scope validation must not create its target"
    );
}

#[tokio::test]
async fn canonical_child_override_materializes_additional_roots_at_the_actual_tool_boundary() {
    use crate::engine::session::{RuntimePolicySnapshot, RuntimePolicyUpdate};
    use crate::types::permission::{ApprovalPolicy, SandboxMode};
    let observed = Arc::new(Mutex::new(Vec::new()));
    let probe = Arc::new(ChildPolicyProbe {
        observed: observed.clone(),
        inline_shell: false,
    });
    let (session, _) = fixture(
        vec![child_policy_probe_turn(), text_turn("done")],
        vec![probe],
    );
    install_probe_runtime_policy(
        &session,
        SandboxMode::WorkspaceWrite,
        ApprovalPolicy::OnRequest,
    );
    let mut policy = session.runtime_policy_snapshot().state();
    policy.sandbox_workspace_write.writable_roots = vec!["/outside/wide".into()];
    let backend = session.runtime_policy_snapshot().sandbox_backend.clone();
    *session.runtime_policy.write().unwrap() = Arc::new(RuntimePolicySnapshot {
        state: policy.clone(),
        sandbox_backend: backend,
    });
    let mut workspace = policy.sandbox_workspace_write;
    workspace.writable_roots = vec!["/outside/wide/narrow".into(), "/unrelated".into()];
    let id = spawn_worker(
        &session,
        SpawnSubAgentOptions {
            prompt: "Inspect the captured authority.".into(),
            runtime_policy: Some(RuntimePolicyUpdate {
                approval_policy: None,
                sandbox_mode: None,
                approvals_reviewer: None,
                sandbox_workspace_write: Some(workspace),
                network_allowed: None,
            }),
            ..Default::default()
        },
    )
    .await;
    finished(&session, &id).await;
    {
        let actual = observed.lock().unwrap();
        assert_eq!(actual.len(), 1);
        assert!(actual[0]
            .permission
            .additional_writable_roots
            .contains(&RuntimePath::from("/outside/wide/narrow")));
        assert!(!actual[0]
            .permission
            .additional_writable_roots
            .contains(&RuntimePath::from("/outside/wide")));
        assert!(!actual[0]
            .permission
            .additional_writable_roots
            .contains(&RuntimePath::from("/unrelated")));
    }
    session.shutdown_and_wait().await.unwrap();
}

#[test]
fn child_runtime_policy_override_cannot_expand_captured_filesystem_roots_or_network() {
    use crate::engine::session::{subagent_runtime_policy_state_for_update, RuntimePolicyUpdate};
    use crate::schemas::config::SandboxWorkspaceWrite;
    use crate::types::permission::{ApprovalPolicy, ApprovalsReviewer, SandboxMode};
    for parent_mode in [
        SandboxMode::ReadOnly,
        SandboxMode::WorkspaceWrite,
        SandboxMode::DangerFullAccess,
    ] {
        for requested_mode in [
            SandboxMode::ReadOnly,
            SandboxMode::WorkspaceWrite,
            SandboxMode::DangerFullAccess,
        ] {
            let mut parent =
                probe_runtime_policy(crate::types::sandbox::SandboxKind::Seatbelt, false).state();
            parent.sandbox_mode = Some(parent_mode);
            parent.approval_policy = Some(ApprovalPolicy::OnRequest);
            parent.approvals_reviewer = Some(ApprovalsReviewer::User);
            parent.sandbox_workspace_write.writable_roots = vec!["/workspace/parent".into()];
            let update = RuntimePolicyUpdate {
                approval_policy: Some(ApprovalPolicy::Never),
                sandbox_mode: Some(requested_mode),
                approvals_reviewer: Some(ApprovalsReviewer::Automatic),
                sandbox_workspace_write: Some(SandboxWorkspaceWrite {
                    writable_roots: vec!["/workspace/parent/narrow".into(), "/unrelated".into()],
                    network_access: true,
                    ..Default::default()
                }),
                network_allowed: Some(true),
            };
            let effective = subagent_runtime_policy_state_for_update(&parent, &update).unwrap();
            assert_eq!(
                effective.sandbox_mode,
                Some(parent_mode.intersect(requested_mode))
            );
            assert!(!effective.network_allowed);
            assert_eq!(effective.approval_policy, Some(ApprovalPolicy::Never));
            assert_eq!(effective.approvals_reviewer, Some(ApprovalsReviewer::User));
            let expected: Vec<RuntimeString> = if parent_mode == SandboxMode::DangerFullAccess {
                vec!["/workspace/parent/narrow".into(), "/unrelated".into()]
            } else if requested_mode == SandboxMode::DangerFullAccess {
                vec!["/workspace/parent".into()]
            } else {
                vec!["/workspace/parent/narrow".into()]
            };
            assert_eq!(effective.sandbox_workspace_write.writable_roots, expected);
        }
    }
}

#[test]
fn child_runtime_policy_keeps_readonly_and_never_independent() {
    use crate::engine::session::intersect_subagent_runtime_state;
    use crate::types::permission::{ApprovalPolicy, SandboxMode};
    let original = probe_runtime_policy(crate::types::sandbox::SandboxKind::Seatbelt, true);
    for reverse in [false, true] {
        let mut a = original.state();
        let mut b = original.state();
        a.approval_policy = Some(ApprovalPolicy::Never);
        b.sandbox_mode = Some(SandboxMode::ReadOnly);
        let effective = if reverse {
            intersect_subagent_runtime_state(&b, &a)
        } else {
            intersect_subagent_runtime_state(&a, &b)
        };
        assert_eq!(effective.approval_policy, Some(ApprovalPolicy::Never));
        assert_eq!(effective.sandbox_mode, Some(SandboxMode::ReadOnly));
    }
}

#[tokio::test]
async fn scoped_grants_keep_the_independent_readonly_approval_ceiling() {
    use crate::permission::engine::{PermissionEngine, StaticPermissionEngine};
    use crate::types::permission::{ApprovalPolicy, SandboxMode};
    use crate::types::sandbox::SandboxKind;
    let write = crate::tools::builtins::write::create_write_tool();
    let engine = StaticPermissionEngine::new(&default_config());
    let input = exact_json!({"path":"must-not-create.txt","content":"not authorized"});
    for approval in [ApprovalPolicy::OnRequest, ApprovalPolicy::Never] {
        let mut ctx = crate::tools::builtins::test_ctx::make_ctx("/tmp");
        ctx.permission.allow_rules =
            crate::permission::rules::parse_rule_strings(&["write".into()]);
        let original = probe_runtime_policy(SandboxKind::Seatbelt, false);
        let mut state = original.state();
        state.sandbox_mode = Some(SandboxMode::ReadOnly);
        state.approval_policy = Some(approval);
        let policy = crate::engine::session::RuntimePolicySnapshot {
            state,
            sandbox_backend: original.sandbox_backend.clone(),
        };
        policy.apply_to_context(&mut ctx);
        let decision = engine.evaluate(write.as_ref(), &input, &ctx).await;
        match approval {
            ApprovalPolicy::OnRequest => {
                assert!(matches!(decision, PermissionDecision::Ask { .. }))
            }
            ApprovalPolicy::Never => assert!(matches!(decision, PermissionDecision::Deny { .. })),
            _ => unreachable!(),
        }
    }
}

#[test]
fn child_runtime_intersection_preserves_the_bound_task_scope_and_backend() {
    use crate::engine::session::{intersect_subagent_runtime_policy, RuntimePolicySnapshot};
    use crate::permission::task_scope::CanonicalTaskResourceScope;
    use crate::schemas::config::{TaskResourceMode, TaskResourceScope};
    use crate::types::permission::SandboxMode;
    use crate::types::sandbox::{SandboxBackend, SandboxKind};
    let root = HardeningConfigRoot::new();
    let cwd = crate::fs::node_path::runtime_path_from_native(root.path());
    let scope = CanonicalTaskResourceScope::bind(
        &TaskResourceScope {
            task_id: "captured-runtime-task".into(),
            mode: TaskResourceMode::ScopedAuto,
            network_allowed: false,
            read_roots: Vec::new(),
            write_roots: Vec::new(),
        },
        &cwd,
    )
    .unwrap();
    let backend: Arc<dyn SandboxBackend> = Arc::new(
        crate::sandbox::task_scope::TaskScopedSandbox::new(
            Arc::new(PolicyNoExecSandbox(SandboxKind::Seatbelt)),
            scope.clone(),
        )
        .unwrap(),
    );
    let mut original = probe_runtime_policy(SandboxKind::Seatbelt, true).state();
    original.sandbox_mode = Some(SandboxMode::WorkspaceWrite);
    let mut current = original.clone();
    current.sandbox_mode = Some(SandboxMode::ReadOnly);
    current.network_allowed = false;
    let original = RuntimePolicySnapshot {
        state: original,
        sandbox_backend: backend.clone(),
    };
    let current = RuntimePolicySnapshot {
        state: current,
        sandbox_backend: backend.clone(),
    };
    let effective = intersect_subagent_runtime_policy(&original, &current);
    assert!(Arc::ptr_eq(&effective.sandbox_backend, &backend));
    assert!(Arc::ptr_eq(
        &effective.sandbox_backend.task_resource_scope().unwrap(),
        &scope
    ));
    assert_eq!(effective.state.sandbox_mode, Some(SandboxMode::ReadOnly));
    assert!(!effective.state.network_allowed);
    scope.validate_roots().unwrap();
}

#[tokio::test]
async fn frozen_permission_ceiling_preserves_explicit_rules_and_planning_denial() {
    use crate::engine::session::{bare_tool_context, subagent_permission_ceiling};
    use crate::permission::engine::StaticPermissionEngine;
    use crate::types::permission::{ApprovalPolicy, SandboxMode};
    use crate::types::sandbox::SandboxKind;
    let (session, _) = fixture(vec![], vec![]);
    let mut original = default_config();
    original.ask = vec!["write".into()];
    original.allow.clear();
    original.deny.clear();
    let mut parent = original.clone();
    parent.ask.clear();
    parent.allow = vec!["write".into()];
    let mut ctx = bare_tool_context(
        session.process_context.clone(),
        session.background_processes.clone(),
        session.cwd_now(),
        CancellationToken::new(),
        session.file_state.clone(),
        policy_permission(),
        Arc::new(PolicyNoExecSandbox(SandboxKind::Process)),
    );
    ctx.sandbox_mode = Some(SandboxMode::DangerFullAccess);
    ctx.approval_policy = Some(ApprovalPolicy::OnRequest);
    let write = crate::tools::builtins::write::create_write_tool();
    let input = exact_json!({"path":"policy-probe.txt","content":"must not be written"});
    let engine = subagent_permission_ceiling(
        &original,
        Arc::new(StaticPermissionEngine::new(&parent)),
        None,
        None,
    );
    assert!(matches!(
        engine.evaluate(write.as_ref(), &input, &ctx).await,
        PermissionDecision::Ask { .. }
    ));
    ctx.permission.should_avoid_prompts = true;
    assert!(matches!(
        engine.evaluate(write.as_ref(), &input, &ctx).await,
        PermissionDecision::Deny { .. }
    ));
    ctx.permission.should_avoid_prompts = false;
    ctx.planning_read_only = true;
    assert!(matches!(
        engine.evaluate(write.as_ref(), &input, &ctx).await,
        PermissionDecision::Deny { .. }
    ));
    ctx.planning_read_only = false;
    original.ask.clear();
    original.deny = vec!["write".into()];
    let engine = subagent_permission_ceiling(
        &original,
        Arc::new(StaticPermissionEngine::new(&parent)),
        None,
        None,
    );
    assert!(matches!(
        engine.evaluate(write.as_ref(), &input, &ctx).await,
        PermissionDecision::Deny { .. }
    ));
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn planning_floor_is_independent_of_grants_and_headless_prompt_restrictions() {
    use crate::engine::session::{
        bare_tool_context, subagent_permission_ceiling, subagent_plan_permission_floor,
    };
    use crate::permission::engine::StaticPermissionEngine;
    use crate::permission::rules::parse_rule_strings;
    use crate::types::permission::{ApprovalPolicy, SandboxMode};
    use crate::types::sandbox::SandboxKind;
    let (session, _) = fixture(vec![], vec![]);
    let mut config = default_config();
    config.allow = vec!["write".into(), "read".into()];
    config.deny.clear();
    config.ask.clear();
    let parent = Arc::new(StaticPermissionEngine::new(&config));
    let ceiling = subagent_permission_ceiling(&config, parent.clone(), None, None);
    let initial_floor = subagent_plan_permission_floor(&config, parent, None, None);
    let write = crate::tools::builtins::write::create_write_tool();
    let input = exact_json!({"path":"policy-probe.txt","content":"must not be written"});
    for avoid_prompts in [false, true] {
        let mut permission = policy_permission();
        permission.allow_rules = parse_rule_strings(&["write".into(), "read".into()]);
        permission.should_avoid_prompts = avoid_prompts;
        let mut ctx = bare_tool_context(
            session.process_context.clone(),
            session.background_processes.clone(),
            session.cwd_now(),
            CancellationToken::new(),
            session.file_state.clone(),
            permission,
            Arc::new(PolicyNoExecSandbox(SandboxKind::Process)),
        );
        ctx.sandbox_mode = Some(SandboxMode::DangerFullAccess);
        ctx.approval_policy = Some(ApprovalPolicy::Never);
        ctx.planning_read_only = true;
        assert!(matches!(
            ceiling.evaluate(write.as_ref(), &input, &ctx).await,
            PermissionDecision::Deny { .. }
        ));
        assert!(matches!(
            initial_floor.evaluate(write.as_ref(), &input, &ctx).await,
            PermissionDecision::Deny { .. }
        ));
        let read = crate::tools::builtins::read::create_read_tool();
        assert!(matches!(
            ceiling
                .evaluate(
                    read.as_ref(),
                    &exact_json!({"path":"policy-probe.txt"}),
                    &ctx
                )
                .await,
            PermissionDecision::Allow { .. }
        ));
        let bash = crate::tools::builtins::bash::create_bash_tool();
        assert!(matches!(
            ceiling
                .evaluate(
                    bash.as_ref(),
                    &exact_json!({"command":"policy-probe-command-that-must-not-run"}),
                    &ctx
                )
                .await,
            PermissionDecision::Deny { .. }
        ));
    }
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn canonical_review_floor_intersects_both_evaluators_without_replacing_approval_flow() {
    use crate::engine::session::{subagent_permission_ceiling, subagent_plan_permission_floor};
    use crate::permission::engine::StaticPermissionEngine;
    use crate::types::permission::{ApprovalPolicy, SandboxMode};
    let mut config = default_config();
    config.allow.clear();
    config.ask.clear();
    config.deny.clear();
    config.approval_policy = Some(ApprovalPolicy::Never);
    let write = crate::tools::builtins::write::create_write_tool();
    let edit = crate::tools::builtins::edit::create_edit_tool();
    let mut ctx = crate::tools::builtins::test_ctx::make_ctx("/tmp");
    ctx.permission = policy_permission();
    ctx.approval_policy = Some(ApprovalPolicy::Never);
    ctx.sandbox_mode = Some(SandboxMode::DangerFullAccess);
    let write_input =
        exact_json!({"path":".klaus/projects/project/memory/review-floor.md","content":"probe"});
    let edit_input = exact_json!({"path":".klaus/projects/project/memory/review-floor.md","old_string":"old","new_string":"new"});
    let ordinary_write_input = exact_json!({"path":"review-floor.txt","content":"probe"});
    let ordinary_edit_input =
        exact_json!({"path":"review-floor.txt","old_string":"old","new_string":"new"});
    for (captured, live, boundary, needs_review) in [
        (true, false, false, true),
        (false, true, false, true),
        (false, false, true, true),
        (false, false, false, false),
    ] {
        for plan_only in [false, true] {
            let mut captured_config = config.clone();
            if captured {
                captured_config.approval_policy = Some(ApprovalPolicy::UnlessTrusted);
            }
            let reader: crate::engine::session::SubagentApprovalPolicyReader =
                Arc::new(move || {
                    if live {
                        ApprovalPolicy::UnlessTrusted
                    } else {
                        ApprovalPolicy::Never
                    }
                });
            let parent = Arc::new(StaticPermissionEngine::new(&config));
            let engine = if plan_only {
                subagent_plan_permission_floor(
                    &captured_config,
                    parent,
                    Some(reader),
                    boundary.then_some(ApprovalPolicy::UnlessTrusted),
                )
            } else {
                subagent_permission_ceiling(
                    &captured_config,
                    parent,
                    Some(reader),
                    boundary.then_some(ApprovalPolicy::UnlessTrusted),
                )
            };
            for ignore_prefix_rules in [false, true] {
                for (tool, input, ordinary_input) in [
                    (write.as_ref(), &write_input, &ordinary_write_input),
                    (edit.as_ref(), &edit_input, &ordinary_edit_input),
                ] {
                    let decision = if ignore_prefix_rules {
                        engine
                            .evaluate_without_allow_prefix_rules(tool, input, &ctx)
                            .await
                    } else {
                        engine.evaluate(tool, input, &ctx).await
                    };
                    if needs_review {
                        assert!(
                            matches!(decision, PermissionDecision::Ask { .. }),
                            "{decision:?}"
                        );
                    } else {
                        assert!(
                            matches!(decision, PermissionDecision::Allow { .. }),
                            "{decision:?}"
                        );
                    }
                    assert_eq!(ctx.approval_policy, Some(ApprovalPolicy::Never));
                    ctx.permission.deny_rules =
                        crate::permission::rules::parse_rule_strings(&[tool.name()]);
                    assert!(matches!(
                        engine.evaluate(tool, input, &ctx).await,
                        PermissionDecision::Deny { .. }
                    ));
                    ctx.permission.deny_rules.clear();
                    ctx.sandbox_mode = Some(SandboxMode::ReadOnly);
                    assert!(matches!(
                        engine.evaluate(tool, input, &ctx).await,
                        PermissionDecision::Ask { .. }
                    ));
                    assert_eq!(ctx.approval_policy, Some(ApprovalPolicy::Never));
                    assert_eq!(ctx.sandbox_mode, Some(SandboxMode::ReadOnly));
                    let filesystem_decision = if ignore_prefix_rules {
                        engine
                            .evaluate_without_allow_prefix_rules(tool, ordinary_input, &ctx)
                            .await
                    } else {
                        engine.evaluate(tool, ordinary_input, &ctx).await
                    };
                    assert!(
                        matches!(filesystem_decision, PermissionDecision::Deny { .. }),
                        "{filesystem_decision:?}"
                    );
                    ctx.sandbox_mode = Some(SandboxMode::DangerFullAccess);
                    ctx.planning_read_only = true;
                    assert!(matches!(
                        engine.evaluate(tool, input, &ctx).await,
                        PermissionDecision::Deny { .. }
                    ));
                    ctx.planning_read_only = false;
                }
            }
        }
    }
}

struct InjectedAllowOrAsk {
    ask: bool,
}

#[async_trait::async_trait]
impl crate::permission::engine::PermissionEngine for InjectedAllowOrAsk {
    async fn evaluate(
        &self,
        _: &dyn Tool,
        input: &RuntimeJson,
        _: &ToolContext,
    ) -> PermissionDecision {
        if self.ask {
            PermissionDecision::Ask {
                message: "Injected parent request.".into(),
                input: input.clone(),
                reason: PermissionReason::Default,
                suggestions: None,
            }
        } else {
            PermissionDecision::Allow {
                input: input.clone(),
                reason: PermissionReason::Default,
            }
        }
    }
    async fn evaluate_without_allow_prefix_rules(
        &self,
        tool: &dyn Tool,
        input: &RuntimeJson,
        ctx: &ToolContext,
    ) -> PermissionDecision {
        self.evaluate(tool, input, ctx).await
    }
}

struct CapabilityCheckedEdit {
    checks: Arc<AtomicUsize>,
    expected_policy: crate::types::permission::ApprovalPolicy,
}

#[async_trait::async_trait]
impl Tool for CapabilityCheckedEdit {
    fn static_name(&self) -> &str {
        "capability_checked_edit"
    }
    fn static_description(&self) -> &str {
        "Probe the actual permission-check context."
    }
    fn input_schema(&self) -> RuntimeJson {
        exact_json!({"type":"object","properties":{"path":{"type":"string"}},"required":["path"]})
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_read_only(&self, _: &RuntimeJson) -> bool {
        false
    }
    fn is_concurrency_safe(&self, _: &RuntimeJson) -> bool {
        false
    }
    fn permission_path_kind(&self) -> Option<crate::types::tool::PermissionPathKind> {
        Some(crate::types::tool::PermissionPathKind::Edit)
    }
    fn get_path(&self, input: &RuntimeJson) -> Option<crate::types::tool::PathTargets> {
        input
            .get("path")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .map(RuntimePath::from)
            .map(crate::types::tool::PathTargets::One)
    }
    async fn check_permissions(
        &self,
        _: &RuntimeJson,
        ctx: &ToolContext,
    ) -> Option<PermissionResult> {
        assert_eq!(ctx.approval_policy, Some(self.expected_policy));
        let (Some(surface), Some(_)) = (ctx.surface_tools.as_ref(), ctx.task_plan.as_ref()) else {
            return Some(PermissionResult::Deny {
                message: "The original wired context was lost.".into(),
                reason: PermissionReason::ToolSelfCheck,
            });
        };
        surface(vec!["review-context-capability".into()])
            .await
            .expect("surface original context capability");
        self.checks.fetch_add(1, Ordering::SeqCst);
        Some(PermissionResult::Allow {
            updated_input: None,
            reason: PermissionReason::ToolSelfCheck,
        })
    }
    async fn call(
        &self,
        _: RuntimeJson,
        _: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, crate::types::errors::ToolError> {
        panic!("A context permission probe must never execute")
    }
}

#[tokio::test]
async fn static_review_floor_preserves_wired_context_and_cannot_be_erased_by_injected_verdicts() {
    use crate::engine::session::{subagent_permission_ceiling, subagent_plan_permission_floor};
    use crate::types::permission::{ApprovalPolicy, SandboxMode};
    let mut config = default_config();
    config.allow.clear();
    config.ask.clear();
    config.deny.clear();
    config.approval_policy = Some(ApprovalPolicy::UnlessTrusted);
    let checks = Arc::new(AtomicUsize::new(0));
    let surfaces = Arc::new(AtomicUsize::new(0));
    let mut ctx = crate::tools::builtins::test_ctx::make_ctx("/tmp");
    ctx.permission = policy_permission();
    ctx.approval_policy = Some(ApprovalPolicy::Never);
    ctx.sandbox_mode = Some(SandboxMode::DangerFullAccess);
    ctx.task_plan = Some(Default::default());
    let surfaced = surfaces.clone();
    ctx.surface_tools = Some(Box::new(move |names| {
        assert_eq!(
            names,
            vec![RuntimeString::from("review-context-capability")]
        );
        surfaced.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }));
    let input = exact_json!({"path":".klaus/projects/project/memory/context-probe.md"});
    for actual_policy in [ApprovalPolicy::Never, ApprovalPolicy::UnlessTrusted] {
        ctx.approval_policy = Some(actual_policy);
        config.approval_policy = Some(if actual_policy == ApprovalPolicy::Never {
            ApprovalPolicy::UnlessTrusted
        } else {
            ApprovalPolicy::Never
        });
        let tool = CapabilityCheckedEdit {
            checks: checks.clone(),
            expected_policy: actual_policy,
        };
        for parent_asks in [false, true] {
            for plan_only in [false, true] {
                let parent = Arc::new(InjectedAllowOrAsk { ask: parent_asks });
                let engine = if plan_only {
                    subagent_plan_permission_floor(&config, parent, None, None)
                } else {
                    subagent_permission_ceiling(&config, parent, None, None)
                };
                for ignore_prefix_rules in [false, true] {
                    let before = checks.load(Ordering::SeqCst);
                    let decision = if ignore_prefix_rules {
                        engine
                            .evaluate_without_allow_prefix_rules(&tool, &input, &ctx)
                            .await
                    } else {
                        engine.evaluate(&tool, &input, &ctx).await
                    };
                    assert!(
                        matches!(
                            decision,
                            PermissionDecision::Ask {
                                reason: PermissionReason::SafetyCheck,
                                ..
                            }
                        ),
                        "{decision:?}"
                    );
                    assert!(checks.load(Ordering::SeqCst) > before);
                    assert_eq!(
                        checks.load(Ordering::SeqCst),
                        surfaces.load(Ordering::SeqCst)
                    );
                    assert_eq!(ctx.approval_policy, Some(actual_policy));
                    ctx.permission.should_avoid_prompts = true;
                    assert!(matches!(
                        engine.evaluate(&tool, &input, &ctx).await,
                        PermissionDecision::Deny { .. }
                    ));
                    ctx.permission.should_avoid_prompts = false;
                    ctx.permission.deny_rules =
                        crate::permission::rules::parse_rule_strings(&["Edit".into()]);
                    assert!(matches!(
                        engine.evaluate(&tool, &input, &ctx).await,
                        PermissionDecision::Deny { .. }
                    ));
                    ctx.permission.deny_rules.clear();
                    ctx.planning_read_only = true;
                    assert!(matches!(
                        engine.evaluate(&tool, &input, &ctx).await,
                        PermissionDecision::Deny { .. }
                    ));
                    ctx.planning_read_only = false;
                }
            }
        }
    }
}

#[tokio::test]
async fn canonical_live_review_reader_fails_closed_without_a_session_owner() {
    use crate::engine::session::{subagent_approval_policy_reader, subagent_permission_ceiling};
    use crate::permission::engine::StaticPermissionEngine;
    use crate::types::permission::{ApprovalPolicy, SandboxMode};
    let (session, _) = fixture(vec![], vec![]);
    install_probe_runtime_policy(
        &session,
        SandboxMode::DangerFullAccess,
        ApprovalPolicy::Never,
    );
    let mut config = default_config();
    config.allow.clear();
    config.ask.clear();
    config.deny.clear();
    config.approval_policy = Some(ApprovalPolicy::Never);
    let mut ctx = crate::tools::builtins::test_ctx::make_ctx("/tmp");
    ctx.permission = policy_permission();
    ctx.sandbox_mode = Some(SandboxMode::DangerFullAccess);
    ctx.approval_policy = Some(ApprovalPolicy::Never);
    let write = crate::tools::builtins::write::create_write_tool();
    let input =
        exact_json!({"path":".klaus/projects/project/memory/closed-reader.md","content":"probe"});
    for (owner, has_owner) in [
        (Arc::downgrade(&session), true),
        (std::sync::Weak::new(), false),
    ] {
        assert_eq!(owner.upgrade().is_some(), has_owner);
        let engine = subagent_permission_ceiling(
            &config,
            Arc::new(StaticPermissionEngine::new(&config)),
            Some(subagent_approval_policy_reader(owner, None)),
            None,
        );
        for ignore_prefix_rules in [false, true] {
            let decision = if ignore_prefix_rules {
                engine
                    .evaluate_without_allow_prefix_rules(write.as_ref(), &input, &ctx)
                    .await
            } else {
                engine.evaluate(write.as_ref(), &input, &ctx).await
            };
            if has_owner {
                assert!(
                    matches!(decision, PermissionDecision::Allow { .. }),
                    "{decision:?}"
                );
            } else {
                assert!(
                    matches!(
                        decision,
                        PermissionDecision::Ask {
                            reason: PermissionReason::SafetyCheck,
                            ..
                        }
                    ),
                    "{decision:?}"
                );
            }
            assert_eq!(ctx.approval_policy, Some(ApprovalPolicy::Never));
        }
    }
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn actual_child_sensitive_write_keeps_captured_and_live_untrusted_review_floors() {
    use crate::engine::session::RuntimePolicyUpdate;
    use crate::types::permission::{ApprovalPolicy, SandboxMode};
    for (captured, later_untrusted) in [(true, false), (false, true), (false, false)] {
        let root = HardeningConfigRoot::new();
        let target = root
            .path()
            .join(".klaus/projects/project/memory/review-floor.md");
        let blocked = gate();
        let approvals = Arc::new(ReviewFloorApprovalProbe::default());
        let observed = Arc::new(Mutex::new(Vec::new()));
        let mut turns = Vec::new();
        if later_untrusted {
            turns.push(gate_turn());
        }
        turns.extend([
            child_policy_probe_turn(),
            review_floor_write_turn(&target),
            text_turn("done"),
        ]);
        let fixture = IsolatedHardeningFixture::with_approval_handler(
            turns,
            vec![
                blocked.clone(),
                Arc::new(ChildPolicyProbe {
                    observed: observed.clone(),
                    inline_shell: false,
                }),
                crate::tools::builtins::write::create_write_tool(),
            ],
            Some(approvals.clone()),
        );
        let session = &fixture.session;
        install_probe_runtime_policy(
            session,
            SandboxMode::DangerFullAccess,
            if captured {
                ApprovalPolicy::UnlessTrusted
            } else {
                ApprovalPolicy::Never
            },
        );
        let id = spawn_worker(
            session,
            SpawnSubAgentOptions {
                prompt: "Check the real sensitive write boundary.".into(),
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
                session,
                SandboxMode::DangerFullAccess,
                ApprovalPolicy::UnlessTrusted,
            );
            blocked.release.add_permits(1);
        }
        finished(session, &id).await;
        settle_ordinary_owners(session).await;
        let snapshot = session
            .ordinary_snapshot_for_resume(&id.try_to_utf8().unwrap())
            .await
            .unwrap();
        assert_eq!(
            snapshot.runtime_policy.state.approval_policy,
            Some(ApprovalPolicy::Never)
        );
        assert_eq!(
            snapshot.config.approval_policy,
            Some(if captured {
                ApprovalPolicy::UnlessTrusted
            } else {
                ApprovalPolicy::Never
            })
        );
        {
            let observations = observed.lock().unwrap();
            assert_eq!(observations.len(), 1);
            assert_eq!(observations[0].approval_policy, Some(ApprovalPolicy::Never));
            assert_eq!(
                observations[0].sandbox_mode,
                Some(SandboxMode::DangerFullAccess)
            );
        }
        assert_review_floor_write_result(
            &fixture.requests.lock().unwrap(),
            captured || later_untrusted,
        );
        if captured || later_untrusted {
            assert!(!target.exists());
        } else {
            assert_eq!(
                std::fs::read_to_string(&target).unwrap(),
                "review floor probe\n"
            );
        }
        assert_eq!(approvals.0.load(Ordering::SeqCst), 0);
        session.shutdown_and_wait().await.unwrap();
    }
}

#[tokio::test]
async fn nested_child_keeps_captured_review_floor_after_root_policy_relaxes() {
    use crate::engine::session::RuntimePolicyUpdate;
    use crate::types::permission::{ApprovalPolicy, SandboxMode};
    let root = HardeningConfigRoot::new();
    let target = root
        .path()
        .join(".klaus/projects/project/memory/nested-review-floor.md");
    let approvals = Arc::new(ReviewFloorApprovalProbe::default());
    let fixture = IsolatedHardeningFixture::with_approval_handler(
        vec![
            text_turn("parent saved"),
            review_floor_write_turn(&target),
            text_turn("nested done"),
        ],
        vec![crate::tools::builtins::write::create_write_tool()],
        Some(approvals.clone()),
    );
    let session = &fixture.session;
    install_probe_runtime_policy(
        session,
        SandboxMode::DangerFullAccess,
        ApprovalPolicy::UnlessTrusted,
    );
    let parent = spawn_worker(
        session,
        SpawnSubAgentOptions {
            prompt: "Keep the captured review floor.".into(),
            runtime_policy: Some(RuntimePolicyUpdate {
                approval_policy: Some(ApprovalPolicy::Never),
                sandbox_mode: None,
                approvals_reviewer: None,
                sandbox_workspace_write: None,
                network_allowed: None,
            }),
            ..Default::default()
        },
    )
    .await;
    finished(session, &parent).await;
    install_probe_runtime_policy(
        session,
        SandboxMode::DangerFullAccess,
        ApprovalPolicy::Never,
    );
    let child = session.make_spawn_background_agent(
        1,
        None,
        session.permission_context(),
        Arc::new(Vec::new),
        false,
        Some(parent.try_to_utf8().unwrap()),
        None,
    )(SpawnSubAgentOptions {
        prompt: "Use the parent's captured floor.".into(),
        fork_context: Some(ForkContext::None),
        ..Default::default()
    })
    .await
    .expect("child creation must be accepted")
    .agent_id;
    finished(session, &child).await;
    let snapshot = session
        .ordinary_snapshot_for_resume(&child.try_to_utf8().unwrap())
        .await
        .unwrap();
    assert_eq!(
        snapshot.config.approval_policy,
        Some(ApprovalPolicy::UnlessTrusted)
    );
    assert_eq!(
        snapshot.runtime_policy.state.approval_policy,
        Some(ApprovalPolicy::Never)
    );
    assert_review_floor_write_result(&fixture.requests.lock().unwrap(), true);
    assert!(!target.exists());
    assert_eq!(approvals.0.load(Ordering::SeqCst), 0);
    session.shutdown_and_wait().await.unwrap();
}

#[derive(Debug)]
struct ObservedChildPolicy {
    permission: PermissionContext,
    sandbox: crate::types::sandbox::SandboxKind,
    network: Option<bool>,
    retry: Option<bool>,
    inline_error: Option<String>,
    sandbox_mode: Option<crate::types::permission::SandboxMode>,
    approval_policy: Option<crate::types::permission::ApprovalPolicy>,
    planning_read_only: bool,
}

struct ChildPolicyProbe {
    observed: Arc<Mutex<Vec<ObservedChildPolicy>>>,
    inline_shell: bool,
}

#[async_trait::async_trait]
impl Tool for ChildPolicyProbe {
    fn static_name(&self) -> &str {
        "child_policy_probe"
    }
    fn static_description(&self) -> &str {
        "Record the actual child tool context without external side effects."
    }
    fn persistence_identity(&self) -> Option<RuntimeString> {
        Some("fixture:child-policy-probe:1".into())
    }
    fn input_schema(&self) -> RuntimeJson {
        exact_json!({"type":"object","properties":{},"additionalProperties":false})
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _: &RuntimeJson) -> bool {
        true
    }
    fn is_read_only(&self, _: &RuntimeJson) -> bool {
        true
    }
    async fn call(
        &self,
        _: RuntimeJson,
        ctx: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, crate::types::errors::ToolError> {
        let inline_error = if self.inline_shell {
            let shell = ctx
                .run_bash
                .as_ref()
                .expect("Child shell callback must exist");
            Some(
                shell(
                    "policy-probe-command-that-must-not-run".into(),
                    Some(crate::types::tool::RunBashOptions {
                        allow_tools: Some(vec!["bash".into()]),
                        ..Default::default()
                    }),
                )
                .await
                .expect_err("Current child shell restrictions must reject before execution")
                .to_string(),
            )
        } else {
            None
        };
        self.observed.lock().unwrap().push(ObservedChildPolicy {
            permission: ctx.permission.clone(),
            sandbox: ctx.sandbox.kind(),
            network: ctx.network_allowed,
            retry: ctx.sandbox_retry_on_denial,
            inline_error,
            sandbox_mode: ctx.sandbox_mode,
            approval_policy: ctx.approval_policy,
            planning_read_only: ctx.planning_read_only,
        });
        Ok(text_result(
            exact_json!({"ok":true}),
            "policy recorded",
            false,
        ))
    }
}

fn child_policy_probe_turn() -> Vec<crate::types::provider::ModelStreamEvent> {
    let mut events = vec![ev::message_start(None)];
    events.extend(exact_tool_use_events(
        "policy-probe-call",
        "child_policy_probe",
        &exact_json!({}),
    ));
    events.push(ev::stop(
        Some(ModelStopReason::ToolUse),
        Some(make_usage(None, None)),
    ));
    events
}

#[tokio::test]
async fn resumed_child_tool_context_applies_current_policy_rules_and_revoked_grants() {
    use crate::permission::rules::parse_rule_strings;
    use crate::types::permission::{ApprovalPolicy, SandboxMode};
    for (sandbox_mode, approval_policy, planning) in [
        (
            SandboxMode::DangerFullAccess,
            ApprovalPolicy::OnRequest,
            false,
        ),
        (SandboxMode::ReadOnly, ApprovalPolicy::Never, false),
        (SandboxMode::DangerFullAccess, ApprovalPolicy::Never, true),
    ] {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let probe = Arc::new(ChildPolicyProbe {
            observed: observed.clone(),
            inline_shell: false,
        });
        let (mutator, mutations) = plan_boundary::mutation_probe();
        let resumed = if planning {
            plan_boundary::mutation_attempt_turn()
        } else {
            child_policy_probe_turn()
        };
        let (session, requests) = fixture(
            vec![text_turn("initial"), resumed, text_turn("resumed")],
            vec![probe, mutator],
        );
        install_probe_runtime_policy(
            &session,
            SandboxMode::DangerFullAccess,
            ApprovalPolicy::OnRequest,
        );
        session.mode.permission.lock().unwrap().allow_rules =
            parse_rule_strings(&["shared".into(), "revoked".into()]);
        let id = spawn_worker(
            &session,
            SpawnSubAgentOptions {
                prompt: "initial".into(),
                ..Default::default()
            },
        )
        .await;
        finished(&session, &id).await;
        install_probe_runtime_policy(&session, sandbox_mode, approval_policy);
        if planning {
            session
                .planning_runtime()
                .set_mode(crate::planning::CollaborationMode::Plan, None)
                .unwrap();
        }
        {
            let mut current = session.mode.permission.lock().unwrap();
            current.allow_rules = parse_rule_strings(&["shared".into(), "new_grant".into()]);
            current.deny_rules = parse_rule_strings(&["current_deny".into()]);
            current.ask_rules = parse_rule_strings(&["current_ask".into()]);
        }
        assert!(
            session
                .send_subagent_message(message(&id, "resume", false))
                .await
                .unwrap()
                .unwrap()
                .resumed
        );
        finished(&session, &id).await;
        {
            let observed = observed.lock().unwrap();
            assert_eq!(observed.len(), 1);
            let effective = &observed[0].permission;
            assert_eq!(observed[0].sandbox_mode, Some(sandbox_mode));
            assert_eq!(observed[0].approval_policy, Some(approval_policy));
            assert_eq!(observed[0].planning_read_only, planning);
            assert_eq!(
                effective.deny_rules,
                parse_rule_strings(&["current_deny".into()])
            );
            assert_eq!(
                effective.ask_rules,
                parse_rule_strings(&["current_ask".into()])
            );
            let expected = parse_rule_strings(&["shared".into()]);
            assert_eq!(effective.allow_rules, expected);
        }
        if planning {
            assert_eq!(
                session.collaboration_mode(),
                crate::planning::CollaborationMode::Plan
            );
            let requests = requests.lock().unwrap();
            assert_eq!(requests.len(), 3);
            plan_boundary::assert_query_view(
                &session,
                &requests[0],
                crate::planning::CollaborationMode::Default,
            );
            for request in &requests[1..] {
                plan_boundary::assert_query_view(
                    &session,
                    request,
                    crate::planning::CollaborationMode::Plan,
                );
            }
            plan_boundary::assert_mutation_rejected(&requests[2], &mutations);
            plan_boundary::assert_plan_permissions(&observed.lock().unwrap()[0]);
        }
        session.shutdown_and_wait().await.unwrap();
    }
}

fn inline_authority_skill_tool() -> AnyTool {
    crate::extensions::skills::skill_tool::create_skill_tool(|| {
        vec![crate::extensions::skills::loader::Skill {
            name: "inline-authority-probe".into(),
            description: "Exercise permission-gated skill expansion.".into(),
            allowed_tools: Some(vec!["bash".into()]),
            body: "!`policy-probe-command-that-must-not-run`".into(),
            path: "/inline-authority-fixture/SKILL.md".into(),
            ..Default::default()
        }]
    })
}

fn inline_authority_skill_turn() -> Vec<crate::types::provider::ModelStreamEvent> {
    let mut events = vec![ev::message_start(None)];
    events.extend(exact_tool_use_events(
        "inline-authority-call",
        "skill",
        &exact_json!({"name":"inline-authority-probe"}),
    ));
    events.push(ev::stop(
        Some(ModelStopReason::ToolUse),
        Some(make_usage(None, None)),
    ));
    events
}

#[tokio::test]
async fn role_disallowed_bash_blocks_real_skill_inline_shell_at_creation_and_recovery() {
    use crate::types::sandbox::SandboxKind;
    let requests = Arc::new(Mutex::new(Vec::new()));
    let session = mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        unique_subagent_test_deps(recording_deps(
            requests.clone(),
            vec![
                inline_authority_skill_turn(),
                text_turn("initial"),
                inline_authority_skill_turn(),
                text_turn("resumed"),
            ],
        )),
        default_config(),
        vec![
            inline_authority_skill_tool(),
            crate::tools::builtins::bash::create_bash_tool(),
        ],
        None,
        None,
        None,
        None,
        Some(vec![AgentDefinition {
            name: "no-shell-worker".into(),
            disallowed_tools: Some(vec!["bash".into()]),
            ..Default::default()
        }]),
    );
    *session.runtime_policy.write().unwrap() = probe_runtime_policy(SandboxKind::Process, false);
    let id = spawn_worker(
        &session,
        SpawnSubAgentOptions {
            prompt: "initial".into(),
            agent_type: Some("no-shell-worker".into()),
            ..Default::default()
        },
    )
    .await;
    finished(&session, &id).await;
    assert!(
        session
            .send_subagent_message(message(&id, "resume", false))
            .await
            .unwrap()
            .unwrap()
            .resumed
    );
    finished(&session, &id).await;
    {
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 4);
        for request in [&requests[0], &requests[2]] {
            assert!(request.tools.iter().any(|tool| tool.name == "skill"));
            assert!(!request.tools.iter().any(|tool| tool.name == "bash"));
        }
        for request in [&requests[1], &requests[3]] {
            let messages = crate::runtime_values::runtime_json::stringify(
                &crate::runtime_values::runtime_codec::encode_value(&request.messages).unwrap(),
            );
            assert!(messages
                .contains("inline shell unavailable: the authorized tool set has no bash tool"));
        }
    }
    session.shutdown_and_wait().await.unwrap();
}

struct SourceTrackedBash {
    identity: &'static str,
    analyses: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait::async_trait]
impl Tool for SourceTrackedBash {
    fn static_name(&self) -> &str {
        "bash"
    }
    fn static_description(&self) -> &str {
        "Record which shell authority is analyzed."
    }
    fn persistence_identity(&self) -> Option<RuntimeString> {
        Some(self.identity.into())
    }
    fn input_schema(&self) -> RuntimeJson {
        exact_json!({"type":"object","properties":{"command":{"type":"string"}},"required":["command"],"additionalProperties":false})
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _: &RuntimeJson) -> bool {
        true
    }
    fn is_read_only(&self, _: &RuntimeJson) -> bool {
        false
    }
    fn permission_path_kind(&self) -> Option<crate::types::tool::PermissionPathKind> {
        self.analyses
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        None
    }
    async fn call(
        &self,
        _: RuntimeJson,
        _: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, crate::types::errors::ToolError> {
        panic!("The shell source fixture must never execute")
    }
}

#[tokio::test]
async fn recovered_inline_shell_analyzes_frozen_bash_object_not_current_same_name_source() {
    use crate::permission::rules::parse_rule_strings;
    use crate::types::sandbox::SandboxKind;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let original_analyses = Arc::new(AtomicUsize::new(0));
    let replacement_analyses = Arc::new(AtomicUsize::new(0));
    let original: AnyTool = Arc::new(SourceTrackedBash {
        identity: "fixture:original-bash:1",
        analyses: original_analyses.clone(),
    });
    let replacement: AnyTool = Arc::new(SourceTrackedBash {
        identity: "fixture:replacement-bash:1",
        analyses: replacement_analyses.clone(),
    });
    let skill = inline_authority_skill_tool();
    let (session, requests) = fixture(
        vec![
            text_turn("initial"),
            inline_authority_skill_turn(),
            text_turn("resumed"),
        ],
        vec![skill.clone(), replacement],
    );
    *session.runtime_policy.write().unwrap() = probe_runtime_policy(SandboxKind::Process, false);
    let id = spawn_worker(
        &session,
        SpawnSubAgentOptions {
            prompt: "initial".into(),
            tools: Some(vec![skill, original]),
            ..Default::default()
        },
    )
    .await;
    {
        let diagnostic_wait = crate::goal::testing_phase::Span::start(
            "fixture.wait",
            &format!("site=case47.initialFinished worker={id:?}"),
        );
        let diagnostic_count = DiagnosticRequestCount {
            site: "case47.initialFinished",
            requests: requests.clone(),
        };
        finished(&session, &id).await;
        diagnostic_wait.finish("ok");
        drop(diagnostic_count);
    }
    original_analyses.store(0, Ordering::SeqCst);
    replacement_analyses.store(0, Ordering::SeqCst);
    session.mode.permission.lock().unwrap().deny_rules = parse_rule_strings(&["bash".into()]);
    assert!(
        session
            .send_subagent_message(message(&id, "resume", false))
            .await
            .unwrap()
            .unwrap()
            .resumed
    );
    {
        let diagnostic_wait = crate::goal::testing_phase::Span::start(
            "fixture.wait",
            &format!("site=case47.recoveryFinished worker={id:?}"),
        );
        let diagnostic_count = DiagnosticRequestCount {
            site: "case47.recoveryFinished",
            requests: requests.clone(),
        };
        finished(&session, &id).await;
        diagnostic_wait.finish("ok");
        drop(diagnostic_count);
    }
    assert!(original_analyses.load(Ordering::SeqCst) > 0);
    assert_eq!(replacement_analyses.load(Ordering::SeqCst), 0);
    {
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        let messages = crate::runtime_values::runtime_json::stringify(
            &crate::runtime_values::runtime_codec::encode_value(&requests[2].messages).unwrap(),
        );
        assert!(messages.contains("Inline shell command not permitted"));
    }
    let mut permission = session.permission_context();
    permission.deny_rules = parse_rule_strings(&["bash".into()]);
    session
        .run_skill_bash(
            "policy-probe-command-that-must-not-run".into(),
            permission,
            session.cwd_now().as_runtime_string().try_to_utf8().unwrap(),
            CancellationToken::new(),
            None,
            session.planning.mode() == crate::planning::CollaborationMode::Plan,
        )
        .await
        .expect_err("The root skill must still obey its current bash denial");
    assert!(
        replacement_analyses.load(Ordering::SeqCst) > 0,
        "Root skills must analyze their current shell source"
    );
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn resumed_inline_shell_uses_current_denial_and_original_sandbox_network_ceiling() {
    use crate::permission::rules::parse_rule_strings;
    use crate::types::sandbox::SandboxKind;
    let observed = Arc::new(Mutex::new(Vec::new()));
    let probe = Arc::new(ChildPolicyProbe {
        observed: observed.clone(),
        inline_shell: true,
    });
    let (session, _) = fixture(
        vec![
            text_turn("initial"),
            child_policy_probe_turn(),
            text_turn("resumed"),
        ],
        vec![probe, crate::tools::builtins::bash::create_bash_tool()],
    );
    *session.runtime_policy.write().unwrap() = probe_runtime_policy(SandboxKind::Seatbelt, false);
    let id = spawn_worker(
        &session,
        SpawnSubAgentOptions {
            prompt: "initial".into(),
            ..Default::default()
        },
    )
    .await;
    finished(&session, &id).await;
    *session.runtime_policy.write().unwrap() = probe_runtime_policy(SandboxKind::Process, true);
    session.mode.permission.lock().unwrap().deny_rules = parse_rule_strings(&["bash".into()]);
    assert!(
        session
            .send_subagent_message(message(&id, "resume", false))
            .await
            .unwrap()
            .unwrap()
            .resumed
    );
    finished(&session, &id).await;
    {
        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].sandbox, SandboxKind::Seatbelt);
        assert_eq!(observed[0].network, Some(false));
        assert_eq!(observed[0].retry, Some(false));
        assert!(observed[0]
            .inline_error
            .as_ref()
            .unwrap()
            .contains("Inline shell command not permitted"));
    }
    session.shutdown_and_wait().await.unwrap();
}

#[path = "session_subagent_permission_origin_tests.rs"]
mod permission_origin_tests;

#[path = "session_subagent_plan_boundary_tests.rs"]
mod plan_boundary;
