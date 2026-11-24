use super::*;
use crate::sync_ext::MutexExt;
use std::time::Duration;

fn source_turn(session: &Session) -> String {
    session
        .turn_index_snapshot()
        .message_turn
        .values()
        .next()
        .unwrap()
        .try_to_utf8()
        .unwrap()
}

#[tokio::test]
async fn prediction_reuses_main_prefix_and_inference_without_transcript_changes() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let mut config = default_config();
    config.memory.enabled = false;
    config.model = "main-model".into();
    config.model_tiers.low = Some("low-model".into());
    config.effort = Some(crate::schemas::effort::EffortValue::Level(
        crate::schemas::effort::EffortLevel::High,
    ));
    let session = mk_session_with_deps(
        recording_deps(
            requests.clone(),
            vec![
                text_turn("parent reply"),
                text_turn(r#"{"suggestion":"  Add coverage  "}"#),
                text_turn(r#"{"suggestion":null}"#),
            ],
        ),
        config,
        vec![create_write_tool()],
    );
    session
        .run(Input::Text("parent context".into()), TurnOptions::default())
        .await;
    let source = source_turn(&session);
    let before = session.history_snapshot();
    let before_index = session.turn_index_snapshot();
    let result = session
        .predict_message_with_signal(
            "dynamic prompt".into(),
            source.clone(),
            None,
            AbortSignal::new(),
        )
        .await
        .unwrap();
    assert_eq!(result, Some("Add coverage".into()));
    assert_eq!(session.history_snapshot(), before);
    assert_eq!(
        session.turn_index_snapshot().message_turn,
        before_index.message_turn
    );
    let override_effort =
        crate::schemas::effort::EffortValue::Level(crate::schemas::effort::EffortLevel::Low);
    assert_eq!(
        session
            .predict_message_with_signal(
                "dynamic prompt".into(),
                source,
                Some(override_effort.clone()),
                AbortSignal::new()
            )
            .await
            .unwrap(),
        None
    );
    assert_eq!(session.history_snapshot(), before);
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    let main_req = &requests[0];
    let request = &requests[1];
    assert_eq!(request.model, "main-model");
    assert_eq!(
        request.effort,
        Some(crate::schemas::effort::EffortValue::Level(
            crate::schemas::effort::EffortLevel::High
        ))
    );
    assert_eq!(requests[2].effort, Some(override_effort));
    assert!(!request.tools.is_empty());
    assert_eq!(request.tools, main_req.tools);
    assert_eq!(request.deferred_tools, main_req.deferred_tools);
    assert_eq!(
        request.surfaced_deferred_names,
        main_req.surfaced_deferred_names
    );
    assert_eq!(request.frozen_tools, main_req.frozen_tools);
    assert_eq!(request.tool_choice, main_req.tool_choice);
    assert_eq!(request.web_search, main_req.web_search);
    assert_eq!(request.system, main_req.system);
    assert_eq!(request.cache_key, main_req.cache_key);
    assert_eq!(request.cache_key, Some(session.id.clone()));
    assert_eq!(request.skip_cache_write, Some(true));
    assert!(request.output_schema.is_none());
    assert_eq!(&request.messages[..before.len()], before.as_slice());
    assert!(
        matches!(request.messages.last(), Some(Message::User(user)) if matches!(&user.content[0], ContentBlock::Text(text) if text.text == "dynamic prompt"))
    );
}

#[tokio::test]
async fn prediction_rejects_stale_source_before_provider_io_and_preserves_history() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let session = mk_session_with_deps(
        recording_deps(
            requests.clone(),
            vec![text_turn("first"), text_turn("second")],
        ),
        default_config(),
        Vec::new(),
    );
    session
        .run(Input::Text("first".into()), TurnOptions::default())
        .await;
    let source = source_turn(&session);
    session
        .run(Input::Text("second".into()), TurnOptions::default())
        .await;
    let before = session.history_snapshot();
    assert_eq!(
        session
            .predict_message_with_signal("dynamic prompt".into(), source, None, AbortSignal::new())
            .await
            .unwrap(),
        None
    );
    assert_eq!(requests.lock().unwrap().len(), 2);
    assert_eq!(session.history_snapshot(), before);
}

#[tokio::test]
async fn prediction_cancellation_drops_provider_stream_without_mutating_parent() {
    struct DropProbe(Arc<AtomicBool>);
    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let entered = Arc::new(tokio::sync::Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let count = Arc::new(AtomicUsize::new(0));
    let mut deps = mk_deps(vec![]);
    deps.call_model = Box::new({
        let entered = entered.clone();
        let dropped = dropped.clone();
        move |_, _| {
            if count.fetch_add(1, Ordering::SeqCst) == 0 {
                return Box::pin(futures::stream::iter(text_turn("parent reply"))) as ModelStream;
            }
            entered.notify_one();
            let probe = DropProbe(dropped.clone());
            Box::pin(futures::stream::once(async move {
                let _probe = probe;
                futures::future::pending::<crate::types::provider::ModelStreamEvent>().await
            })) as ModelStream
        }
    });
    let session = mk_session_with_deps(deps, default_config(), Vec::new());
    session
        .run(Input::Text("parent context".into()), TurnOptions::default())
        .await;
    let before = session.history_snapshot();
    let source = source_turn(&session);
    let signal = AbortSignal::new();
    let task = tokio::spawn({
        let session = session.clone();
        let signal = signal.clone();
        async move {
            session
                .predict_message_with_signal("dynamic prompt".into(), source, None, signal)
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    signal.cancel();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err(),
        "prediction cancelled"
    );
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(session.history_snapshot(), before);
}

#[tokio::test]
async fn prediction_does_not_invoke_hooks_or_synchronize_memory() {
    let calls = Arc::new(AtomicUsize::new(0));
    let bus = setup_bus(Hook {
        event: HookEvent::UserPromptSubmit,
        matcher: None,
        handler: crate::hooks::types::HookHandler::Fn(Arc::new(SequencedSetupHook {
            calls: calls.clone(),
        })),
        is_async: false,
    });
    let session = mk_setup_session(
        mk_deps(vec![
            text_turn("parent"),
            text_turn(r#"{"suggestion":"next"}"#),
        ]),
        bus,
        None,
        None,
    );
    session
        .run(Input::Text("parent".into()), TurnOptions::default())
        .await;
    let before_calls = calls.load(Ordering::SeqCst);
    session
        .predict_message_with_signal(
            "dynamic prompt".into(),
            source_turn(&session),
            None,
            AbortSignal::new(),
        )
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), before_calls);

    let root = temp_store_dir();
    let host = Arc::new(SyncTestMemoryHost {
        layout: SyncTestMemoryLayout {
            base: root.to_string_lossy().into_owned(),
        },
        plans: Mutex::new(Vec::new()),
        applies: Mutex::new(Vec::new()),
        apply_results: Mutex::new(Vec::new()),
    });
    let mut config = default_config();
    config.agent_memory = true;
    let session = mk_session_with_provider_and_memory(
        Arc::new(fake_provider()),
        mk_deps(vec![
            text_turn("parent"),
            text_turn(r#"{"suggestion":"next"}"#),
        ]),
        config,
        Vec::new(),
        Some(host.clone()),
    );
    session
        .run(Input::Text("parent".into()), TurnOptions::default())
        .await;
    let before_plans = host.plans.lock().unwrap().len();
    let before_applies = host.applies.lock().unwrap().len();
    session
        .predict_message_with_signal(
            "dynamic prompt".into(),
            source_turn(&session),
            None,
            AbortSignal::new(),
        )
        .await
        .unwrap();
    assert_eq!(host.plans.lock().unwrap().len(), before_plans);
    assert_eq!(host.applies.lock().unwrap().len(), before_applies);
    session.shutdown_and_wait().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn prediction_new_foreground_admission_cancels_inflight_snapshot() {
    let entered = Arc::new(tokio::sync::Notify::new());
    let count = Arc::new(AtomicUsize::new(0));
    let mut deps = mk_deps(vec![]);
    deps.call_model = Box::new({
        let entered = entered.clone();
        move |_, _| {
            let number = count.fetch_add(1, Ordering::SeqCst);
            if number == 1 {
                entered.notify_one();
                return Box::pin(futures::stream::pending()) as ModelStream;
            }
            Box::pin(futures::stream::iter(text_turn("main reply"))) as ModelStream
        }
    });
    let session = mk_session_with_deps(deps, default_config(), Vec::new());
    session
        .run(Input::Text("first".into()), TurnOptions::default())
        .await;
    let source = source_turn(&session);
    let task = tokio::spawn({
        let session = session.clone();
        async move {
            session
                .predict_message_with_signal(
                    "dynamic prompt".into(),
                    source,
                    None,
                    AbortSignal::new(),
                )
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    session
        .run(Input::Text("second".into()), TurnOptions::default())
        .await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        None
    );
    assert_eq!(
        session
            .history_snapshot()
            .iter()
            .filter(|message| matches!(message, Message::User(_)))
            .count(),
        2
    );
}

#[tokio::test]
async fn prediction_context_reports_live_model_provider_plan_mode_and_latest_turn() {
    let mut config = default_config();
    config.model = "main-model".into();
    let session = mk_session_with_deps(mk_deps(vec![text_turn("parent")]), config, Vec::new());
    let context = session.prediction_context().await.unwrap();
    assert_eq!(context.turn_id, None);
    assert_eq!(context.model, "main-model");
    assert_eq!(context.provider, fake_provider().info().name);
    assert!(!context.running);
    assert!(!context.plan_mode);
    session
        .run(Input::Text("parent".into()), TurnOptions::default())
        .await;
    let context = session.prediction_context().await.unwrap();
    assert_eq!(
        context.turn_id,
        Some(RuntimeString::from(source_turn(&session)))
    );
    assert!(!context.running);
    session
        .request_permission_mode_change(crate::types::permission::ExternalPermissionMode::Plan)
        .unwrap();
    assert!(session.prediction_context().await.unwrap().plan_mode);
}

#[tokio::test]
async fn prediction_uses_latest_message_free_failed_turn_with_equal_lifecycle_clocks() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let mut config = default_config();
    config.memory.enabled = false;
    let session = mk_session_with_deps(
        recording_deps(
            requests.clone(),
            vec![
                text_turn("parent reply"),
                provider_error_turn("continuation failed"),
                text_turn(r#"{"suggestion":"Try the next step"}"#),
            ],
        ),
        config,
        Vec::new(),
    );
    session
        .run(Input::Text("parent context".into()), TurnOptions::default())
        .await;
    let previous_turn = session.prediction_context().await.unwrap().turn_id.unwrap();
    let before_continuation = session.history_snapshot();
    let failed = session
        .run(Input::Parts(Vec::new()), TurnOptions::default())
        .await;
    assert_eq!(failed.stop_reason, TerminalReason::Error);
    assert_eq!(session.history_snapshot(), before_continuation);
    let before_index = session.turn_index_snapshot();
    let failed_turn = before_index
        .turn_status
        .iter()
        .find_map(|(id, status)| (status == "failed").then(|| id.clone()))
        .unwrap();
    assert_ne!(failed_turn, previous_turn);
    assert!(before_index
        .message_turn
        .values()
        .all(|id| id != &failed_turn));
    assert_eq!(before_index.turn_started_at_ms[&previous_turn], 0);
    assert_eq!(before_index.turn_started_at_ms[&failed_turn], 0);
    assert_eq!(before_index.turn_finished_at_ms[&previous_turn], 0);
    assert_eq!(before_index.turn_finished_at_ms[&failed_turn], 0);
    let context = session.prediction_context().await.unwrap();
    assert_eq!(context.turn_id, Some(failed_turn.clone()));
    assert!(!context.running);
    assert_eq!(
        session
            .predict_message_with_signal(
                "dynamic prompt".into(),
                previous_turn.try_to_utf8().unwrap(),
                None,
                AbortSignal::new(),
            )
            .await
            .unwrap(),
        None
    );
    assert_eq!(requests.lock().unwrap().len(), 2);
    assert_eq!(
        session
            .predict_message_with_signal(
                "dynamic prompt".into(),
                failed_turn.try_to_utf8().unwrap(),
                None,
                AbortSignal::new(),
            )
            .await
            .unwrap(),
        Some("Try the next step".into())
    );
    assert_eq!(session.history_snapshot(), before_continuation);
    assert_eq!(session.turn_index_snapshot(), before_index);
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert_eq!(
        &requests[2].messages[..before_continuation.len()],
        before_continuation.as_slice()
    );
}

#[tokio::test]
async fn prediction_restore_preserves_message_free_terminal_source() {
    let mut config = default_config();
    config.memory.enabled = false;
    let original = mk_session_with(
        vec![
            text_turn("parent reply"),
            provider_error_turn("continuation failed"),
        ],
        config.clone(),
        Vec::new(),
    );
    original
        .run(Input::Text("parent context".into()), TurnOptions::default())
        .await;
    let previous_turn = original
        .prediction_context()
        .await
        .unwrap()
        .turn_id
        .unwrap();
    assert_eq!(
        original
            .run(Input::Parts(Vec::new()), TurnOptions::default())
            .await
            .stop_reason,
        TerminalReason::Error
    );
    let before = original.history_snapshot();
    let before_index = original.turn_index_snapshot();
    let failed_turn = before_index
        .turn_status
        .iter()
        .find_map(|(id, status)| (status == "failed").then(|| id.clone()))
        .unwrap();
    assert!(before_index
        .message_turn
        .values()
        .all(|id| id != &failed_turn));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let restored = mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        recording_deps(
            requests.clone(),
            vec![text_turn(r#"{"suggestion":"Continue here"}"#)],
        ),
        config,
        Vec::new(),
        None,
        None,
        None,
        Some(before.clone()),
        None,
    );
    restored.restore_turn_index(before_index.clone());
    restored.restore_prediction_source_turn(Some(failed_turn.clone()));
    let context = restored.prediction_context().await.unwrap();
    assert_eq!(context.turn_id, Some(failed_turn.clone()));
    assert!(!context.running);
    assert_eq!(
        restored
            .predict_message_with_signal(
                "dynamic prompt".into(),
                previous_turn.try_to_utf8().unwrap(),
                None,
                AbortSignal::new(),
            )
            .await
            .unwrap(),
        None
    );
    assert!(requests.lock().unwrap().is_empty());
    assert_eq!(
        restored
            .predict_message_with_signal(
                "dynamic prompt".into(),
                failed_turn.try_to_utf8().unwrap(),
                None,
                AbortSignal::new(),
            )
            .await
            .unwrap(),
        Some("Continue here".into())
    );
    assert_eq!(restored.history_snapshot(), before);
    assert_eq!(restored.turn_index_snapshot(), before_index);
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(&requests[0].messages[..before.len()], before.as_slice());
}

#[tokio::test]
async fn prediction_rollback_drops_inflight_provider_stream_without_a_new_turn() {
    struct DropProbe(Arc<AtomicBool>);
    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let entered = Arc::new(tokio::sync::Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let count = Arc::new(AtomicUsize::new(0));
    let mut deps = mk_deps(vec![]);
    deps.call_model = Box::new({
        let entered = entered.clone();
        let dropped = dropped.clone();
        let count = count.clone();
        move |_, _| {
            if count.fetch_add(1, Ordering::SeqCst) < 2 {
                return Box::pin(futures::stream::iter(text_turn("parent reply"))) as ModelStream;
            }
            entered.notify_one();
            let probe = DropProbe(dropped.clone());
            Box::pin(futures::stream::once(async move {
                let _probe = probe;
                futures::future::pending::<crate::types::provider::ModelStreamEvent>().await
            })) as ModelStream
        }
    });
    let session = mk_session_with_deps(deps, default_config(), Vec::new());
    session
        .run(Input::Text("first".into()), TurnOptions::default())
        .await;
    let first_turn = session.prediction_context().await.unwrap().turn_id.unwrap();
    let retained = session.history_snapshot();
    session
        .run(Input::Text("second".into()), TurnOptions::default())
        .await;
    let second_turn = session.prediction_context().await.unwrap().turn_id.unwrap();
    let task = tokio::spawn({
        let session = session.clone();
        async move {
            session
                .predict_message_with_signal(
                    "dynamic prompt".into(),
                    second_turn.try_to_utf8().unwrap(),
                    None,
                    AbortSignal::new(),
                )
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    let outcome = session.rollback(1).await.unwrap();
    assert_eq!(outcome.dropped_turns, 1);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        None
    );
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(count.load(Ordering::SeqCst), 3);
    assert_eq!(session.history_snapshot(), retained);
    assert_eq!(
        session.prediction_context().await.unwrap().turn_id,
        Some(first_turn)
    );
}

#[tokio::test]
async fn prediction_restored_clear_watermark_fences_old_sources_and_preserves_counter_bits() {
    let session = mk_session_with_deps(mk_deps(Vec::new()), default_config(), Vec::new());
    let generation = (1u64 << 53) + 1;
    session.restore_prediction_source_checkpoint((generation, None));
    session
        .record_prediction_source_checkpoint(generation - 1, Some("delayed".into()))
        .await;
    assert_eq!(
        *session.turn.latest_prediction_source.lock_recover(),
        Some((generation, None))
    );
    assert_eq!(session.turn.reserve_generation(), Some(generation + 1));
    assert_eq!(session.prediction_context().await.unwrap().turn_id, None);
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn prediction_generation_exhaustion_cannot_wrap_or_mutate_history() {
    let session = mk_session_with_deps(
        mk_deps(vec![text_turn("retained")]),
        default_config(),
        Vec::new(),
    );
    session
        .run(Input::Text("retained".into()), TurnOptions::default())
        .await;
    let before = session.history_snapshot();
    session.restore_prediction_source_checkpoint((u64::MAX - 1, None));
    assert_eq!(session.turn.reserve_generation(), None);
    assert_eq!(
        session.rollback(1).await.unwrap_err(),
        "turn admission counter is exhausted"
    );
    assert_eq!(session.history_snapshot(), before);
    let rejected = session
        .run(Input::Text("cannot wrap".into()), TurnOptions::default())
        .await;
    assert_eq!(rejected.stop_reason, TerminalReason::Error);
    assert_eq!(session.history_snapshot(), before);
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn side_task_predictions_ignore_inherited_prefix_until_side_task_has_own_human_work() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let mut config = default_config();
    config.memory.enabled = false;
    config.model = "main-model".into();
    let parent = mk_session_with_deps(
        recording_deps(requests.clone(), vec![text_turn("parent reply")]),
        config.clone(),
        Vec::new(),
    );
    parent
        .run(
            Input::Text("parent unfinished task".into()),
            TurnOptions::default(),
        )
        .await;
    let parent_turn = source_turn(&parent);
    let snapshot = parent.side_task_snapshot().unwrap();
    parent.shutdown_and_wait().await.unwrap();

    let hidden_ids = snapshot
        .history
        .iter()
        .map(super::message_uuid)
        .cloned()
        .collect();
    let mut side_deps = recording_deps(
        requests.clone(),
        vec![
            text_turn("side reply"),
            text_turn(r#"{"suggestion":"Ask a follow-up on the side answer"}"#),
        ],
    );
    let side_uuid = Arc::new(AtomicUsize::new(0));
    side_deps.uuid =
        Arc::new(move || format!("side-uuid-{}", side_uuid.fetch_add(1, Ordering::SeqCst)));
    let mut side_init = session_init_with_provider_memory_store_agents_and_snapshot_decisions(
        Arc::new(fake_provider()),
        side_deps,
        config,
        Vec::new(),
        None,
        None,
        None,
        Some(snapshot.history),
        None,
        None,
    );
    side_init.id = Some("side-1".into());
    side_init.session_role = Some(crate::schemas::rollout::SessionRole::SideTask);
    side_init.parent_thread_id = Some("s1".into());
    side_init.hidden_context_message_ids = Some(hidden_ids);
    side_init.initial_approval_context = Some(
        crate::engine::approval_capture::ReviewParentHistory::inherit_snapshot(
            &crate::engine::approval_capture::ReviewParentHistory::restore(
                &snapshot.approval_context,
            )
            .snapshot(),
            false,
        )
        .checkpoint(),
    );
    side_init.initial_approval_reviews = Some(Vec::new());
    let side = Session::new(side_init);

    // A brand-new side task has zero own human work: no source turn, no provider call.
    assert_eq!(side.prediction_context().await.unwrap().turn_id, None);
    assert_eq!(
        side.predict_message_with_signal(
            "dynamic prompt".into(),
            parent_turn,
            None,
            AbortSignal::new(),
        )
        .await
        .unwrap(),
        None
    );
    assert_eq!(requests.lock().unwrap().len(), 1);

    // After the user sends a message in the side task, prediction sees ONLY the side task's own turn.
    side.run(
        Input::Text("what does message flow mean here?".into()),
        TurnOptions::default(),
    )
    .await;
    let side_turn = side
        .prediction_context()
        .await
        .unwrap()
        .turn_id
        .expect("side task own turn becomes prediction source")
        .try_to_utf8()
        .unwrap();
    let suggestion = side
        .predict_message_with_signal("dynamic prompt".into(), side_turn, None, AbortSignal::new())
        .await
        .unwrap();
    assert_eq!(
        suggestion,
        Some("Ask a follow-up on the side answer".into())
    );
    {
        let recorded = requests.lock().unwrap();
        assert_eq!(recorded.len(), 3);
        let prediction_req = &recorded[2];
        // Inherited parent messages ("parent unfinished task" / "parent reply") must be excluded.
        let visible = side.visible_history();
        assert_eq!(visible.len(), 2);
        assert_eq!(
            &prediction_req.messages[..visible.len()],
            visible.as_slice()
        );
        assert_eq!(prediction_req.messages.len(), visible.len() + 1);
    }

    // Rolling back the side task's only turn returns prediction eligibility to empty.
    side.rollback(1).await.unwrap();
    assert_eq!(side.prediction_context().await.unwrap().turn_id, None);
    side.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn prediction_preserves_cache_prefix_across_openai_anthropic_and_gemini_modes() {
    struct NamedProvider {
        info: crate::types::provider::ProviderInfo,
    }
    impl crate::types::provider::ModelProvider for NamedProvider {
        fn info(&self) -> &crate::types::provider::ProviderInfo {
            &self.info
        }
        fn capabilities(
            &self,
            _model: &RuntimeString,
        ) -> crate::types::provider::ProviderCapabilities {
            crate::types::provider::ProviderCapabilities {
                supports_image_detail_original: false,
                streaming: true,
                tools: true,
                thinking: true,
                structured_output: true,
                web_search: true,
                prompt_cache_control: true,
                context_window: 200_000,
                max_output_tokens: 16_384,
                native_pdf: true,
                vision: true,
                image_limits: None,
                max_media_blocks: None,
            }
        }
        fn stream(&self, _: crate::types::provider::ModelRequest, _: AbortSignal) -> ModelStream {
            Box::pin(futures::stream::empty())
        }
    }

    for (provider_name, model_name, source_kind, expected_skip_cache_write) in [
        ("openai", "gpt-5.4", "codex", None),
        ("openai", "qwen-max", "qoder", None),
        ("anthropic", "claude-sonnet-4-6", "subscription", Some(true)),
        ("gemini", "gemini-2.5-pro", "antigravity", Some(true)),
    ] {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let mut config = default_config();
        config.memory.enabled = false;
        config.model = model_name.into();
        config.cache1h_ttl = true;
        config.model_source = Some(crate::types::events::InferenceSource {
            source: Some(source_kind.to_string()),
            model_id: Some(model_name.to_string()),
            tier: None,
            selection: None,
        });
        let provider = Arc::new(NamedProvider {
            info: crate::types::provider::ProviderInfo {
                name: provider_name.into(),
                default_model: model_name.into(),
                base_url: None,
                env_key: None,
            },
        });
        let session = mk_session_with_provider_and_memory(
            provider,
            recording_deps(
                requests.clone(),
                vec![
                    text_turn("completed answer"),
                    text_turn("```json\n{\"suggestion\":\"Run verification suite\"}\n```"),
                ],
            ),
            config,
            vec![create_read_tool(), create_write_tool()],
            None,
        );
        session
            .run(
                Input::Text("implement cache reuse".into()),
                TurnOptions::default(),
            )
            .await;
        let source = source_turn(&session);
        let before = session.history_snapshot();
        let predicted = session
            .predict_message_with_signal("dynamic prompt".into(), source, None, AbortSignal::new())
            .await
            .unwrap();
        assert_eq!(
            predicted,
            Some("Run verification suite".into()),
            "{provider_name}/{source_kind}"
        );
        assert_eq!(session.history_snapshot(), before);
        {
            let recorded = requests.lock().unwrap();
            assert_eq!(recorded.len(), 2, "{provider_name}/{source_kind}");
            let main_req = &recorded[0];
            let pred_req = &recorded[1];
            assert_eq!(
                pred_req.model, main_req.model,
                "{provider_name}/{source_kind}"
            );
            assert_eq!(
                pred_req.tools, main_req.tools,
                "{provider_name}/{source_kind}"
            );
            assert_eq!(
                pred_req.system, main_req.system,
                "{provider_name}/{source_kind}"
            );
            assert_eq!(
                pred_req.cache_key,
                Some(session.id.clone()),
                "{provider_name}/{source_kind}"
            );
            assert_eq!(
                pred_req.cache_ttl, main_req.cache_ttl,
                "{provider_name}/{source_kind}"
            );
            assert_eq!(
                pred_req.skip_cache_write, expected_skip_cache_write,
                "{provider_name}/{source_kind}"
            );
            assert!(
                pred_req.output_schema.is_none(),
                "{provider_name}/{source_kind}"
            );
            assert_eq!(
                &pred_req.messages[..before.len()],
                before.as_slice(),
                "{provider_name}/{source_kind}"
            );
        }
        session.shutdown_and_wait().await.unwrap();
    }
}

#[tokio::test]
async fn prediction_never_executes_tools_and_always_uses_main_session_model() {
    struct CountingReadOnlyTool {
        calls: Arc<AtomicUsize>,
        schema: JsonSchema,
    }
    #[async_trait::async_trait]
    impl Tool for CountingReadOnlyTool {
        fn static_name(&self) -> &str {
            "probe_readonly"
        }
        fn static_description(&self) -> &str {
            "Read-only probe"
        }
        fn static_input_schema(&self) -> &JsonSchema {
            &self.schema
        }
        fn is_enabled(&self) -> bool {
            true
        }
        fn is_read_only(&self, _input: &Json) -> bool {
            true
        }
        fn is_concurrency_safe(&self, _input: &Json) -> bool {
            true
        }
        async fn call(
            &self,
            _input: Json,
            _ctx: &ToolContext,
            _on_progress: Option<ToolProgress>,
        ) -> Result<ToolResult, crate::types::errors::ToolError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(text_result(Json::Null, "executed", false))
        }
    }

    struct StaleFeatureProvider {
        info: crate::types::provider::ProviderInfo,
        calls: Arc<AtomicUsize>,
    }
    impl crate::types::provider::ModelProvider for StaleFeatureProvider {
        fn info(&self) -> &crate::types::provider::ProviderInfo {
            &self.info
        }
        fn capabilities(
            &self,
            _model: &RuntimeString,
        ) -> crate::types::provider::ProviderCapabilities {
            fake_provider().capabilities(&"aux-model".into())
        }
        fn stream(&self, _: crate::types::provider::ModelRequest, _: AbortSignal) -> ModelStream {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(futures::stream::empty())
        }
    }

    let tool_calls = Arc::new(AtomicUsize::new(0));
    let aux_calls = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let mut config = default_config();
    config.memory.enabled = false;
    config.model = "main-session-model".into();
    let tool_turn = vec![
        ev::message_start(None),
        crate::types::provider::ModelStreamEvent::ToolUseStart {
            id: "call-1".into(),
            name: "probe_readonly".into(),
            thought_signature: None,
            tool_kind: None,
            input_format: None,
        },
        crate::types::provider::ModelStreamEvent::ToolUseInputDelta {
            id: "call-1".into(),
            partial_json: "{}".into(),
        },
        crate::types::provider::ModelStreamEvent::ToolUseStop {
            id: "call-1".into(),
        },
        ev::stop(Some(ModelStopReason::ToolUse), Some(make_usage(None, None))),
    ];
    let session = mk_session_with_deps(
        recording_deps(
            requests.clone(),
            vec![
                text_turn("parent reply"),
                tool_turn,
                text_turn(r#"{"suggestion":"Check main model"}"#),
            ],
        ),
        config,
        vec![
            Arc::new(CountingReadOnlyTool {
                calls: tool_calls.clone(),
                schema: serde_json::json!({"type": "object", "properties": {}})
                    .as_object()
                    .unwrap()
                    .clone(),
            }),
            create_write_tool(),
        ],
    );
    session
        .install_feature_targets(crate::engine::session::FeatureTargets {
            title: None,
            prediction: Some(crate::engine::session::InferenceTargetState::Ready(
                crate::engine::session::InferenceTarget {
                    source: "custom".into(),
                    model_id: "stale-prediction-entry".into(),
                    model: "stale-prediction-model".into(),
                    provider: Arc::new(StaleFeatureProvider {
                        info: crate::types::provider::ProviderInfo {
                            name: "stale-aux".into(),
                            default_model: "stale-prediction-model".into(),
                            base_url: None,
                            env_key: None,
                        },
                        calls: aux_calls.clone(),
                    }),
                    pricing: Vec::new(),
                    review_requirements: None,
                },
            )),
            memory: None,
        })
        .unwrap();

    session
        .run(Input::Text("parent context".into()), TurnOptions::default())
        .await;
    let source = source_turn(&session);
    let before = session.history_snapshot();

    // If the model attempts a tool call during prediction, zero tools execute and None is returned.
    assert_eq!(
        session
            .predict_message_with_signal(
                "dynamic prompt".into(),
                source.clone(),
                None,
                AbortSignal::new(),
            )
            .await
            .unwrap(),
        None
    );
    assert_eq!(tool_calls.load(Ordering::SeqCst), 0);
    assert_eq!(aux_calls.load(Ordering::SeqCst), 0);
    assert_eq!(session.history_snapshot(), before);

    // Subsequent text prediction still uses the main session model, ignoring stale feature targets.
    assert_eq!(
        session
            .predict_message_with_signal("dynamic prompt".into(), source, None, AbortSignal::new(),)
            .await
            .unwrap(),
        Some("Check main model".into())
    );
    assert_eq!(aux_calls.load(Ordering::SeqCst), 0);
    {
        let recorded = requests.lock().unwrap();
        assert_eq!(recorded.len(), 3);
        assert_eq!(recorded[1].model, "main-session-model");
        assert_eq!(recorded[2].model, "main-session-model");
    }
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn prediction_reuses_latest_turn_system_prompt_append_and_clears_when_absent() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let mut config = default_config();
    config.memory.enabled = false;
    config.model = "main-model".into();
    let session = mk_session_with_deps(
        recording_deps(
            requests.clone(),
            vec![
                text_turn("first reply"),
                text_turn(r#"{"suggestion":"Continue with tests"}"#),
                text_turn("second reply"),
                text_turn(r#"{"suggestion":"Ship it"}"#),
            ],
        ),
        config,
        vec![create_read_tool()],
    );

    let host_append = RuntimeString::from("### Host directive\nKeep subagent titles concise.");
    session
        .run(
            Input::Text("first prompt".into()),
            TurnOptions {
                system_prompt_append: Some(host_append.clone()),
                ..TurnOptions::default()
            },
        )
        .await;
    let first_source = session
        .prediction_context()
        .await
        .unwrap()
        .turn_id
        .expect("first turn id")
        .try_to_utf8()
        .unwrap();
    let first_prediction = session
        .predict_message_with_signal(
            "dynamic prompt".into(),
            first_source,
            None,
            AbortSignal::new(),
        )
        .await
        .unwrap();
    assert_eq!(first_prediction, Some("Continue with tests".into()));

    session
        .run(Input::Text("second prompt".into()), TurnOptions::default())
        .await;
    let second_source = session
        .prediction_context()
        .await
        .unwrap()
        .turn_id
        .expect("second turn id")
        .try_to_utf8()
        .unwrap();
    let second_prediction = session
        .predict_message_with_signal(
            "dynamic prompt".into(),
            second_source,
            None,
            AbortSignal::new(),
        )
        .await
        .unwrap();
    assert_eq!(second_prediction, Some("Ship it".into()));

    {
        let recorded = requests.lock().unwrap();
        assert_eq!(recorded.len(), 4);
        assert_eq!(recorded[1].system, recorded[0].system);
        assert!(
            recorded[1]
                .system
                .iter()
                .any(|part| part.name == "append" && part.text == host_append),
            "prediction must carry the preceding turn's append part"
        );
        assert_eq!(recorded[3].system, recorded[2].system);
        assert!(
            !recorded[3].system.iter().any(|part| part.name == "append"),
            "prediction must drop the append part once a newer turn omits it"
        );
    }

    session.shutdown_and_wait().await.unwrap();
}
