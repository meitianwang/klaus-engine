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
async fn prediction_inherits_context_and_main_inference_without_tools_or_transcript_changes() {
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
    let request = &requests[1];
    assert_eq!(request.model, "main-model");
    assert_eq!(
        request.effort,
        Some(crate::schemas::effort::EffortValue::Level(
            crate::schemas::effort::EffortLevel::High
        ))
    );
    assert_eq!(requests[2].effort, Some(override_effort));
    assert!(request.tools.is_empty());
    assert_eq!(request.skip_cache_write, Some(true));
    assert_eq!(&request.messages[..before.len()], before.as_slice());
    assert!(
        matches!(request.messages.last(), Some(Message::User(user)) if matches!(&user.content[0], ContentBlock::Text(text) if text.text == "dynamic prompt"))
    );
    let schema = request.output_schema.as_ref().unwrap();
    assert_eq!(schema["required"][0], RuntimeJson::from("suggestion"));
    assert_eq!(schema["additionalProperties"], RuntimeJson::Bool(false));
    assert_eq!(
        schema["properties"]["suggestion"]["anyOf"][1]["type"],
        RuntimeJson::from("null")
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
