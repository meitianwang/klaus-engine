//! Admission retains failed residents and uses each call's frozen load settings.

use super::*;
use crate::browser::{
    BrowserEventSink, BrowserExecutor, BrowserRuntime, BrowserRuntimeError, BrowserRuntimeRequest,
    BrowserRuntimeResponse,
};
use crate::capabilities::{
    RuntimeCapabilityRegistry, RuntimeCapabilityRoot, SelectedRuntimeCapabilityRoot,
};

fn load_config(session: &Arc<Session>, capacity: u32) -> crate::schemas::config::ResolvedConfig {
    let mut config = session.inference_config();
    config.max_concurrent_threads_per_session = None;
    config.max_concurrent_sub_agents = capacity;
    config
}

#[tokio::test]
async fn an_unload_failure_retains_mail_and_residency_then_tries_the_next_candidate() {
    let (session, _, _, dir) = persistent_fixture(
        vec![
            text_turn("one"),
            text_turn("two"),
            text_turn("three"),
            text_turn("four"),
        ],
        vec![],
    )
    .await;
    let one = start(&session, "one").await;
    finished(&session, &one).await;
    let two = start(&session, "two").await;
    finished(&session, &two).await;
    let three = start(&session, "three").await;
    finished(&session, &three).await;
    passive(&session, &one, "mail retained after unload failure").await;
    passive(&session, &two, "touch two").await;
    passive(&session, &three, "touch three").await;
    let retained_snapshot = session
        .ordinary_capability_retention_for_testing(&one.try_to_utf8().unwrap())
        .0
        .unwrap();
    let sidechain = |id: &RuntimeString| {
        dir.join("1-s1/subagents")
            .join(format!("agent-{}.jsonl", id.try_to_utf8().unwrap()))
    };
    let first_path = sidechain(&one);
    let first_saved = first_path.with_extension("held");
    std::fs::rename(&first_path, &first_saved).unwrap();
    std::fs::create_dir(&first_path).unwrap();
    let four = start(&session, "four").await;
    finished(&session, &four).await;
    assert!(session
        .governor
        .contains_resident(&one.try_to_utf8().unwrap()));
    assert!(!session
        .governor
        .contains_resident(&two.try_to_utf8().unwrap()));
    {
        let (snapshot, evicted) =
            session.ordinary_capability_retention_for_testing(&one.try_to_utf8().unwrap());
        assert!(
            evicted.is_none(),
            "failed unload must not publish retained proof"
        );
        assert!(std::sync::Weak::ptr_eq(
            &snapshot.unwrap(),
            &retained_snapshot
        ));
    }
    assert!(session
        .conversation
        .queue()
        .admitted_snapshot()
        .iter()
        .any(|message| { message.agent_id.as_ref() == Some(&one) && !message.triggers_turn() }));
    let three_path = sidechain(&three);
    let four_path = sidechain(&four);
    let three_saved = three_path.with_extension("held");
    let four_saved = four_path.with_extension("held");
    std::fs::rename(&three_path, &three_saved).unwrap();
    std::fs::rename(&four_path, &four_saved).unwrap();
    std::fs::create_dir(&three_path).unwrap();
    std::fs::create_dir(&four_path).unwrap();
    let residents = session.governor.resident_ids();
    let error = background(&session, 0, None)(SpawnSubAgentOptions::default())
        .await
        .err()
        .unwrap();
    assert!(error.contains("residency capacity"));
    assert!(error.contains("Resident unload failures"));
    for id in [&one, &three, &four] {
        assert!(error.contains(&id.try_to_utf8().unwrap()));
    }
    assert_eq!(session.governor.resident_ids(), residents);
    for path in [&first_path, &three_path, &four_path] {
        std::fs::remove_dir(path).unwrap();
    }
    std::fs::rename(first_saved, first_path).unwrap();
    std::fs::rename(three_saved, three_path).unwrap();
    std::fs::rename(four_saved, four_path).unwrap();
    session.shutdown_and_wait().await.unwrap();
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn deferred_workflow_and_child_creation_use_the_captured_capacity() {
    for workflow in [false, true] {
        let (session, _, _, dir) = persistent_fixture(
            vec![
                text_turn("one"),
                text_turn("two"),
                text_turn("three"),
                text_turn("captured"),
            ],
            vec![],
        )
        .await;
        let one = start(&session, "one").await;
        finished(&session, &one).await;
        let two = start(&session, "two").await;
        finished(&session, &two).await;
        let capture = Arc::new(session.begin_inference_activity().unwrap());
        let mut bundle = (*capture.bundle).clone();
        bundle.config = load_config(&session, 1);
        let capture = capture.with_bundle(Arc::new(bundle));
        let frozen_child = session.make_spawn_background_agent_with_capture(
            0,
            None,
            session.permission_context(),
            Arc::new(Vec::new),
            false,
            None,
            None,
            Some(capture.clone()),
        );
        let frozen_workflow = session.make_spawn_workflow_agent(
            0,
            CancellationToken::new(),
            session.permission_context(),
            None,
            None,
            Some(capture),
            None,
            SubagentCapabilityCapture::capture(
                session.runtime_capability_snapshot_for_turn(&TurnOptions::default()),
            ),
            session.collaboration_mode(),
        )(SpawnSubAgentOptions {
            prompt: "captured workflow".into(),
            ..Default::default()
        });
        let three = start(&session, "three").await;
        finished(&session, &three).await;
        assert_eq!(session.governor.capacity(), 3);
        assert_eq!(session.inference_config().effective_subagent_capacity(), 3);
        if workflow {
            drop(frozen_child);
            let events: Vec<_> = frozen_workflow.collect().await;
            assert!(!events
                .iter()
                .any(|event| matches!(event, EngineEvent::TurnFailed { .. })));
        } else {
            drop(frozen_workflow);
            let child = frozen_child(SpawnSubAgentOptions {
                prompt: "captured child".into(),
                ..Default::default()
            })
            .await
            .unwrap();
            finished(&session, &child.agent_id).await;
            drop(frozen_child);
        }
        assert_eq!(session.governor.resident_ids().len(), 1);
        session.shutdown_and_wait().await.unwrap();
        std::fs::remove_dir_all(dir).ok();
    }
}

#[tokio::test]
async fn cold_passive_load_ignores_execution_saturation_and_trigger_refusal_retains_the_load() {
    let gate = gate();
    let (session, store, _, dir) = persistent_fixture(
        vec![text_turn("passive target"), text_turn("trigger target")],
        vec![gate.clone()],
    )
    .await;
    let passive_id = start(&session, "passive target").await;
    finished(&session, &passive_id).await;
    let trigger_id = start(&session, "trigger target").await;
    finished(&session, &trigger_id).await;
    session.shutdown_and_wait().await.unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let config = load_config(&session, 3);
    let cold = mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        unique_subagent_test_deps(recording_deps(
            requests,
            vec![gate_turn(), gate_turn(), gate_turn()],
        )),
        config,
        vec![gate.clone()],
        None,
        None,
        Some(store),
        None,
        None,
    );
    let parent_turn = ordinary_parent_turn_lease(&cold, "cold-load-order");
    let mut active = Vec::new();
    for index in 0..3 {
        active.push(start(&cold, &format!("active {index}")).await);
        entered(&gate).await;
    }
    let passive_registry = cold.background_task_registry_for_config(None, load_config(&cold, 4));
    let mut request = message(&passive_id, "cold passive input", false);
    request.trigger_turn = false;
    let passive_result = passive_registry
        .send_message(request)
        .await
        .unwrap()
        .unwrap();
    assert!(!passive_result.resumed);
    assert!(cold
        .governor
        .contains_resident(&passive_id.try_to_utf8().unwrap()));
    let trigger_registry = cold.background_task_registry_for_config(None, load_config(&cold, 5));
    let error = trigger_registry
        .send_message(message(&trigger_id, "cold trigger input", false))
        .await
        .unwrap()
        .err()
        .unwrap();
    assert!(error.contains("execution capacity"));
    assert!(cold
        .governor
        .contains_resident(&trigger_id.try_to_utf8().unwrap()));
    assert_eq!(
        cold.ordinary_residency_config(&trigger_id.try_to_utf8().unwrap())
            .unwrap()
            .effective_subagent_capacity(),
        5
    );
    assert_eq!(cold.governor.active_count(), 3);
    gate.release.add_permits(3);
    for id in active {
        finished(&cold, &id).await;
    }
    drop(parent_turn);
    cold.shutdown_and_wait().await.unwrap();
    std::fs::remove_dir_all(dir).ok();
}

struct OwnerBrowser(std::sync::atomic::AtomicUsize);

#[async_trait::async_trait]
unsafe impl BrowserExecutor for OwnerBrowser {
    async fn execute(
        &self,
        _: BrowserRuntimeRequest,
        _: crate::types::abort::AbortSignal,
        _: BrowserEventSink,
    ) -> Result<BrowserRuntimeResponse, BrowserRuntimeError> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(BrowserRuntimeResponse::default())
    }
}

struct LocalBrowserFixture {
    session: Arc<Session>,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
    dir: PathBuf,
    target: Arc<dyn BrowserRuntime>,
    fallback: Arc<dyn BrowserRuntime>,
    target_calls: Arc<OwnerBrowser>,
    fallback_calls: Arc<OwnerBrowser>,
}

impl LocalBrowserFixture {
    async fn new(turns: ScriptedTurns, target_enabled: bool) -> Self {
        let dir = temp_store_dir();
        let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(dir.to_str().unwrap().to_owned()),
            archive_dir: None,
        }));
        let rollout: RolloutSink = Arc::from(
            store
                .create(SessionMeta {
                    id: "s1".into(),
                    created_at: 1,
                    cwd: runtime_path_from_host_path(&dir).into_runtime_string(),
                    model: "fake-model".into(),
                    title: None,
                    originator: None,
                    cli_version: None,
                    source: None,
                    role: None,
                    parent_thread_id: None,
                    forked_from_id: None,
                    git: None,
                    host_metadata: None,
                })
                .await
                .unwrap(),
        );
        let target_calls = Arc::new(OwnerBrowser(std::sync::atomic::AtomicUsize::new(0)));
        let fallback_calls = Arc::new(OwnerBrowser(std::sync::atomic::AtomicUsize::new(0)));
        // These fixture executors only increment in-memory counters.
        let target = unsafe { crate::browser::attest_spawn_free(target_calls.clone()) };
        let fallback = unsafe { crate::browser::attest_spawn_free(fallback_calls.clone()) };
        let target_root =
            RuntimeCapabilityRoot::host("target.browser").with_browser_runtime(target.clone());
        let target_root = if target_enabled {
            target_root
        } else {
            target_root.disabled()
        };
        let requests = Arc::new(Mutex::new(Vec::new()));
        let mut config = default_config();
        config.agent_memory = false;
        config.memory_recall = false;
        config.fork = false;
        let mut init = session_init_with_provider_memory_store_agents_and_snapshot_decisions(
            Arc::new(fake_provider()),
            unique_subagent_test_deps(recording_deps(requests.clone(), turns)),
            config,
            crate::tools::builtins::browser::create_browser_tools(),
            None,
            Some(rollout),
            Some(store),
            None,
            None,
            None,
        );
        init.runtime_capabilities = RuntimeCapabilityRegistry::from_roots([
            target_root,
            RuntimeCapabilityRoot::host("fallback.browser").with_browser_runtime(fallback.clone()),
        ]);
        let session = Session::new(init);
        Self {
            session,
            requests,
            dir,
            target,
            fallback,
            target_calls,
            fallback_calls,
        }
    }

    fn target_capture(&self) -> SubagentCapabilityCapture {
        SubagentCapabilityCapture::capture(self.session.runtime_capability_snapshot_for_turn(
            &TurnOptions {
                selected_capability_roots: Some(vec![SelectedRuntimeCapabilityRoot::host(
                    "target.browser",
                )]),
                ..Default::default()
            },
        ))
    }

    async fn close(self) {
        self.session.shutdown_and_wait().await.unwrap();
        std::fs::remove_dir_all(self.dir).ok();
    }
}

fn browser_owner_turn() -> Vec<crate::types::provider::ModelStreamEvent> {
    let mut events = vec![ev::message_start(None)];
    events.extend(exact_tool_use_events(
        "retained-browser-call",
        "browser_tabs",
        &exact_json!({"action":"list"}),
    ));
    events.push(ev::stop(
        Some(ModelStopReason::ToolUse),
        Some(make_usage(None, None)),
    ));
    events
}

#[tokio::test]
async fn a_locally_evicted_browser_worker_reloads_its_original_owner_instead_of_sender_defaults() {
    let fixture = LocalBrowserFixture::new(
        vec![
            text_turn("target ready"),
            text_turn("two"),
            text_turn("three"),
            text_turn("four"),
            browser_owner_turn(),
            text_turn("target resumed"),
        ],
        true,
    )
    .await;
    let session = &fixture.session;
    let selected = fixture.target_capture();
    let spawn = session.make_spawn_background_agent_with_permission_origin(
        0,
        AbortSignal::new(),
        None,
        session.permission_context(),
        Arc::new(Vec::new),
        false,
        None,
        None,
        None,
        None,
        Some(selected),
        None,
    );
    let target = spawn(SpawnSubAgentOptions {
        prompt: "target browser worker".into(),
        fork_context: Some(ForkContext::None),
        ..Default::default()
    })
    .await
    .unwrap()
    .agent_id;
    finished(session, &target).await;
    let target_id = target.try_to_utf8().unwrap();
    let original_snapshot = session
        .ordinary_capability_retention_for_testing(&target_id)
        .0
        .unwrap();
    for prompt in ["two", "three", "four"] {
        let id = start(session, prompt).await;
        finished(session, &id).await;
    }
    assert!(!session.governor.contains_resident(&target_id));
    assert!(
        original_snapshot.upgrade().is_none(),
        "local eviction must release the whole child snapshot"
    );
    {
        let (snapshot, retained) = session.ordinary_capability_retention_for_testing(&target_id);
        assert!(snapshot.is_none());
        assert!(Arc::ptr_eq(
            retained
                .as_ref()
                .unwrap()
                .capabilities()
                .browser_runtime
                .as_ref()
                .unwrap(),
            &fixture.target,
        ));
    }
    let sender = session.ordinary_resume_context(None).unwrap();
    assert!(Arc::ptr_eq(
        sender
            .runtime_capabilities
            .capabilities()
            .browser_runtime
            .as_ref()
            .unwrap(),
        &fixture.fallback
    ));
    let receipt = session
        .send_subagent_message(message(&target, "use the retained browser owner", false))
        .await
        .unwrap()
        .unwrap();
    assert!(receipt.resumed);
    finished(session, &target).await;
    assert_eq!(
        session
            .background_task_registry()
            .get(&target)
            .unwrap()
            .status,
        BackgroundTaskStatus::Completed
    );
    assert!(session.governor.contains_resident(&target_id));
    assert_eq!(
        fixture
            .target_calls
            .0
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert_eq!(
        fixture
            .fallback_calls
            .0
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert_eq!(fixture.requests.lock().unwrap().len(), 6);
    {
        let (snapshot, retained) = session.ordinary_capability_retention_for_testing(&target_id);
        assert!(
            retained.is_none(),
            "successful reload releases the retained-only copy"
        );
        assert!(Arc::ptr_eq(
            snapshot
                .unwrap()
                .upgrade()
                .unwrap()
                .runtime_capabilities
                .capabilities()
                .browser_runtime
                .as_ref()
                .unwrap(),
            &fixture.target,
        ));
    }
    drop(spawn);
    fixture.close().await;
}

#[tokio::test]
async fn retained_browser_restore_rejects_foreign_replaced_revoked_and_serialized_only_owners() {
    let original = LocalBrowserFixture::new(vec![], true).await;
    let replacement = LocalBrowserFixture::new(vec![], true).await;
    let revoked = LocalBrowserFixture::new(vec![], false).await;
    let retained = original.target_capture();
    let metadata = runtime_codec::encode_value(&retained).unwrap();
    let sender = original.session.ordinary_resume_context(None).unwrap();
    let restored = SubagentCapabilityCapture::restore(
        &original.session,
        Some(&metadata),
        &sender.runtime_capabilities,
        Some(&retained),
    )
    .unwrap();
    assert!(Arc::ptr_eq(
        restored.capabilities().browser_runtime.as_ref().unwrap(),
        &original.target
    ));
    assert!(!Arc::ptr_eq(
        restored.capabilities().browser_runtime.as_ref().unwrap(),
        &original.fallback
    ));
    // Even a sender holding the same exact Arc is not evidence of a local eviction.
    assert!(SubagentCapabilityCapture::restore(
        &original.session,
        Some(&metadata),
        &retained,
        None
    )
    .is_err());
    let foreign = replacement.target_capture();
    assert!(runtime_codec::encode_value(&foreign)
        .unwrap()
        .semantic_eq_unordered(&metadata));
    assert!(
        SubagentCapabilityCapture::restore(
            &original.session,
            Some(&metadata),
            &sender.runtime_capabilities,
            Some(&foreign),
        )
        .is_err(),
        "matching serialized evidence cannot substitute a different live owner"
    );
    assert!(
        SubagentCapabilityCapture::restore(
            &replacement.session,
            Some(&metadata),
            &retained,
            Some(&retained),
        )
        .is_err(),
        "a current registry replacement must invalidate the retained owner"
    );
    assert!(
        SubagentCapabilityCapture::restore(
            &revoked.session,
            Some(&metadata),
            &retained,
            Some(&retained),
        )
        .is_err(),
        "a disabled current root must invalidate the retained owner"
    );
    let mut changed = metadata.clone();
    changed
        .as_object_mut()
        .unwrap()
        .get_mut("evidence")
        .unwrap()
        .as_object_mut()
        .unwrap()
        .insert("unexpected", RuntimeJson::Bool(true));
    assert!(
        SubagentCapabilityCapture::restore(
            &original.session,
            Some(&changed),
            &sender.runtime_capabilities,
            Some(&retained),
        )
        .is_err(),
        "local identity must not bypass saved bound evidence"
    );
    original.close().await;
    replacement.close().await;
    revoked.close().await;
}
