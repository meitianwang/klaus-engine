//! Exercises creation acceptance and residency through real worker factories.

use super::*;
use crate::engine::session::SubagentCapabilityCapture;
use crate::sync_ext::MutexExt;

#[path = "session_subagent_residency_followup_tests.rs"]
mod followup;

fn background(
    session: &Arc<Session>,
    depth: u32,
    parent: Option<String>,
) -> crate::types::tool::SpawnBackgroundAgent {
    session.make_spawn_background_agent(
        depth,
        None,
        session.permission_context(),
        Arc::new(Vec::new),
        false,
        parent,
        None,
    )
}

async fn start(session: &Arc<Session>, prompt: &str) -> RuntimeString {
    background(session, 0, None)(SpawnSubAgentOptions {
        prompt: prompt.into(),
        fork_context: Some(ForkContext::None),
        ..Default::default()
    })
    .await
    .expect("creation must accept the initial input")
    .agent_id
}

async fn passive(session: &Arc<Session>, id: &RuntimeString, text: &str) {
    let mut request = message(id, text, false);
    request.trigger_turn = false;
    session
        .send_subagent_message(request)
        .await
        .expect("known identity")
        .expect("passive mail must be durable");
}

#[tokio::test]
async fn descendants_share_execution_capacity_and_independent_roots_do_not() {
    let gate = gate();
    let (session, _) = fixture(
        vec![gate_turn(), gate_turn(), gate_turn()],
        vec![gate.clone()],
    );
    let first = start(&session, "first").await;
    entered(&gate).await;
    let deep = background(&session, 100, Some(first.try_to_utf8().unwrap()));
    let second = deep(SpawnSubAgentOptions {
        prompt: "nested".into(),
        fork_context: Some(ForkContext::None),
        ..Default::default()
    })
    .await
    .unwrap()
    .agent_id;
    entered(&gate).await;
    let third = start(&session, "third").await;
    entered(&gate).await;
    assert_eq!(session.governor.active_count(), 3);
    let before = session.subagent_lifecycle_snapshot();
    let refusal =
        background(&session, 200, Some(second.try_to_utf8().unwrap()))(SpawnSubAgentOptions {
            prompt: "capacity refusal".into(),
            fork_context: Some(ForkContext::None),
            ..Default::default()
        })
        .await;
    let error = refusal.err().unwrap();
    assert!(error.contains("execution capacity"), "{error:?}");
    assert_eq!(session.subagent_lifecycle_snapshot(), before);
    let other_gate = super::gate();
    let (other, _) = fixture(vec![gate_turn()], vec![other_gate.clone()]);
    let independent = start(&other, "independent root").await;
    entered(&other_gate).await;
    assert_eq!(other.governor.active_count(), 1);
    gate.release.add_permits(3);
    other_gate.release.add_permits(1);
    for id in [&first, &second, &third] {
        finished(&session, id).await;
    }
    finished(&other, &independent).await;
    session.shutdown_and_wait().await.unwrap();
    other.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn workflow_creation_cannot_bypass_a_full_execution_pool() {
    let gate = gate();
    let (session, _) = fixture(
        vec![gate_turn(), gate_turn(), gate_turn()],
        vec![gate.clone()],
    );
    for index in 0..3 {
        start(&session, &format!("active {index}")).await;
        entered(&gate).await;
    }
    let spawn = session.make_spawn_workflow_agent(
        0,
        CancellationToken::new(),
        session.permission_context(),
        None,
        None,
        None,
        None,
        SubagentCapabilityCapture::capture(
            session.runtime_capability_snapshot_for_turn(&TurnOptions::default()),
        ),
        session.collaboration_mode(),
    );
    let before = session.subagent_lifecycle_snapshot();
    let events: Vec<_> = spawn(SpawnSubAgentOptions {
        prompt: "workflow child".into(),
        ..Default::default()
    })
    .collect()
    .await;
    assert!(events.iter().any(|event| matches!(event, EngineEvent::TurnFailed { error, .. } if error.message.contains("execution capacity"))));
    assert_eq!(session.subagent_lifecycle_snapshot(), before);
    gate.release.add_permits(3);
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn private_memory_forks_do_not_consume_child_execution_or_residency() {
    let gate = gate();
    let (session, _) = fixture(
        vec![
            gate_turn(),
            gate_turn(),
            gate_turn(),
            gate_turn(),
            text_turn("done"),
            text_turn("done"),
            text_turn("done"),
            text_turn("done"),
        ],
        vec![gate.clone()],
    );
    let mut children = Vec::new();
    for index in 0..3 {
        children.push(start(&session, &format!("child {index}")).await);
        entered(&gate).await;
    }
    let residents = session.governor.resident_ids();
    let lifecycle = session.subagent_lifecycle_snapshot();
    let memory_dir = tempfile::tempdir().unwrap();
    let memory = session.run_sub_agent(
        SpawnSubAgentOptions {
            prompt: "Run private memory maintenance.".into(),
            memory_dir: Some(RuntimePath::from(memory_dir.path().to_str().unwrap())),
            tools: Some(vec![gate.clone()]),
            ..Default::default()
        },
        1,
        CancellationToken::new(),
        session.permission_context(),
        "private-memory-fork".into(),
        RunSubAgentOpts::default(),
    );
    let memory = tokio::spawn(memory.collect::<Vec<_>>());
    entered(&gate).await;
    assert_eq!(session.governor.active_count(), 3);
    assert_eq!(session.governor.resident_ids(), residents);
    assert_eq!(session.subagent_lifecycle_snapshot(), lifecycle);
    let ordinary: Vec<_> = session
        .run_sub_agent(
            SpawnSubAgentOptions {
                prompt: "Ordinary direct child.".into(),
                fork_context: Some(ForkContext::None),
                ..Default::default()
            },
            1,
            CancellationToken::new(),
            session.permission_context(),
            "direct-child".into(),
            RunSubAgentOpts::default(),
        )
        .collect()
        .await;
    assert!(ordinary.iter().any(|event| matches!(
        event, EngineEvent::TurnFailed { error, .. }
            if error.message.contains("execution capacity")
    )));
    gate.release.add_permits(4);
    let events = memory.await.unwrap();
    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::TurnCompleted {
            stop_reason: TerminalReason::Completed,
            ..
        }
    )));
    for child in children {
        finished(&session, &child).await;
    }
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn creation_rejection_keeps_the_original_error_when_cancellation_is_ready() {
    let (session, requests) = fixture(vec![], vec![]);
    let abort = CancellationToken::new();
    let (id, admission, pending, accepted) = session
        .prepare_external_subagent(
            &SpawnSubAgentOptions::default(),
            1,
            None,
            None,
            abort.clone(),
            None,
            &session.inference_config(),
        )
        .await
        .unwrap();
    admission.reject("Original child initialization failure.".into());
    abort.cancel();
    assert_eq!(
        pending.wait(accepted).await.unwrap_err(),
        RuntimeString::from("Original child initialization failure.")
    );
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while session.has_ordinary_runtime_identity(&id) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(session.governor.resident_ids().is_empty());
    assert!(session.subagent_lifecycle_snapshot().agents.is_empty());
    assert!(requests.lock().unwrap().is_empty());
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn a_bound_factory_without_inherited_step_context_refuses_default_history() {
    let (session, requests) = fixture(vec![], vec![]);
    let error = session.make_spawn_background_agent(
        1,
        Some("spawning-step".into()),
        session.permission_context(),
        Arc::new(Vec::new),
        false,
        Some("spawning-parent".into()),
        None,
    )(SpawnSubAgentOptions {
        prompt: "Default history requires the initiating step context.".into(),
        ..Default::default()
    })
    .await
    .expect_err("missing inherited context must not silently become a fresh child");
    assert_eq!(
        error,
        RuntimeString::from("The initiating step's inherited context is unavailable.")
    );
    assert!(requests.lock().unwrap().is_empty());
    assert!(session.subagent_lifecycle_snapshot().agents.is_empty());
    assert!(session.governor.resident_ids().is_empty());
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn completed_workers_remain_resident_until_lru_unloads_real_state() {
    let (session, store, requests, dir) = persistent_fixture(
        vec![
            text_turn("one"),
            text_turn("two"),
            text_turn("three"),
            text_turn("four"),
            text_turn("resumed"),
        ],
        vec![],
    )
    .await;
    let one = start(&session, "original one").await;
    finished(&session, &one).await;
    let two = start(&session, "original two").await;
    finished(&session, &two).await;
    let three = start(&session, "original three").await;
    finished(&session, &three).await;
    assert_eq!(session.governor.resident_ids().len(), 3);
    passive(&session, &one, "touch one").await;
    let four = start(&session, "original four").await;
    finished(&session, &four).await;
    let two_id = two.try_to_utf8().unwrap();
    assert!(!session.governor.contains_resident(&two_id));
    assert!(session.background_task_registry().get(&two).is_some());
    assert!(!session
        .subagents
        .transcripts
        .lock_recover()
        .contains_key(&SubAgentMapKey::String(two.clone())));
    store.read_sub_agent("s1", &two_id).await.unwrap();
    let resident_ids = session.governor.resident_ids();
    passive(&session, &two, "queued while unloaded").await;
    assert_eq!(session.governor.resident_ids(), resident_ids);
    let resumed = session
        .send_subagent_message(message(&two, "resume same identity", false))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resumed.agent_id, two);
    finished(&session, &two).await;
    assert!(session.governor.contains_resident(&two_id));
    assert!(requests.lock().unwrap().last().unwrap().messages.iter().any(|message| matches!(message,
        Message::User(user) if user.content.iter().any(|block| matches!(block, ContentBlock::Text(text) if text.text.contains("queued while unloaded"))))));
    session.shutdown_and_wait().await.unwrap();
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn residency_pins_skip_lru_candidates_and_full_residency_refuses_without_queueing() {
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
    let pin = session
        .ensure_ordinary_loaded(
            &one.try_to_utf8().unwrap(),
            false,
            &session.inference_config(),
        )
        .await
        .unwrap();
    passive(&session, &two, "touch two").await;
    passive(&session, &three, "touch three").await;
    let four = start(&session, "four").await;
    finished(&session, &four).await;
    assert!(session
        .governor
        .contains_resident(&one.try_to_utf8().unwrap()));
    assert!(!session
        .governor
        .contains_resident(&two.try_to_utf8().unwrap()));
    let pin_three = session
        .ensure_ordinary_loaded(
            &three.try_to_utf8().unwrap(),
            false,
            &session.inference_config(),
        )
        .await
        .unwrap();
    let pin_four = session
        .ensure_ordinary_loaded(
            &four.try_to_utf8().unwrap(),
            false,
            &session.inference_config(),
        )
        .await
        .unwrap();
    let error = background(&session, 0, None)(SpawnSubAgentOptions::default())
        .await
        .err()
        .unwrap();
    assert!(error.contains("residency capacity"));
    assert_eq!(session.governor.resident_ids().len(), 3);
    drop((pin, pin_three, pin_four));
    session.shutdown_and_wait().await.unwrap();
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn pending_creation_cancellation_releases_identity_and_residency_reservation() {
    let (session, _) = fixture(vec![text_turn("accepted")], vec![]);
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Semaphore::new(0));
    session.set_ordinary_creation_gate(entered.clone(), release);
    let spawn = background(&session, 0, None);
    let task = tokio::spawn(async move {
        spawn(SpawnSubAgentOptions {
            name: Some("cancelled-name".into()),
            ..Default::default()
        })
        .await
    });
    entered.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while session.governor.try_reserve_pending_slot(1).is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(session.governor.resident_ids().is_empty());
    assert!(session.subagent_lifecycle_snapshot().agents.is_empty());
    assert!(session.background_task_registry().list().is_empty());
    let next = start(&session, "next accepted input").await;
    finished(&session, &next).await;
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn initialization_failure_returns_refusal_without_a_committed_worker() {
    let (session, _) = fixture(vec![text_turn("accepted")], vec![]);
    let error = background(&session, 0, None)(SpawnSubAgentOptions {
        isolation: Some(crate::types::tool::Isolation::Worktree),
        ..Default::default()
    })
    .await
    .expect_err("unavailable worktree must reject creation");
    assert!(error.contains("worktree") || error.contains("Worktree"));
    assert!(session.governor.resident_ids().is_empty());
    assert!(session.subagent_lifecycle_snapshot().agents.is_empty());
    assert!(session.background_task_registry().list().is_empty());
    let next = start(&session, "after refusal").await;
    finished(&session, &next).await;
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn awaiting_a_real_tool_keeps_execution_and_residency_while_other_workers_are_unloaded() {
    let gate = gate();
    let (session, _, _, dir) = persistent_fixture(
        vec![
            gate_turn(),
            text_turn("two"),
            text_turn("three"),
            text_turn("four"),
        ],
        vec![gate.clone()],
    )
    .await;
    let waiting = start(&session, "wait in the tool").await;
    entered(&gate).await;
    let two = start(&session, "two").await;
    finished(&session, &two).await;
    let three = start(&session, "three").await;
    finished(&session, &three).await;
    assert_eq!(session.governor.active_count(), 1);
    let four = start(&session, "four").await;
    finished(&session, &four).await;
    assert!(session
        .governor
        .contains_resident(&waiting.try_to_utf8().unwrap()));
    assert!(!session
        .governor
        .contains_resident(&two.try_to_utf8().unwrap()));
    assert_eq!(session.governor.active_count(), 1);
    gate.release.add_permits(1);
    finished(&session, &waiting).await;
    session.shutdown_and_wait().await.unwrap();
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn trigger_mail_protects_an_inactive_completed_worker_from_lru_eviction() {
    use crate::engine::message_queue::{QueuePriority, QueuedMessage, QueuedValue, SteerDelivery};
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
    session
        .conversation
        .queue()
        .enqueue(QueuedMessage {
            trigger_turn: Some(true),
            value: QueuedValue::Text("pending trigger".into()),
            mode: "subagent-message".into(),
            uuid: Some("resident-trigger-protection".into()),
            session_id: Some(session.id.clone()),
            priority: Some(QueuePriority::Next),
            is_meta: Some(true),
            origin: None,
            agent_id: Some(one.clone()),
            correlation_id: None,
            delivery: Some(SteerDelivery::User),
            display_metadata: None,
            pdf_bytes: 0,
            pdf_documents: 0,
        })
        .unwrap();
    session
        .conversation
        .queue()
        .await_pending_durability()
        .await
        .unwrap();
    let four = start(&session, "four").await;
    finished(&session, &four).await;
    assert!(session
        .governor
        .contains_resident(&one.try_to_utf8().unwrap()));
    assert!(!session
        .governor
        .contains_resident(&two.try_to_utf8().unwrap()));
    assert_eq!(
        session.background_task_registry().get(&one).unwrap().status,
        BackgroundTaskStatus::Completed
    );
    session.shutdown_and_wait().await.unwrap();
    std::fs::remove_dir_all(dir).ok();
}
