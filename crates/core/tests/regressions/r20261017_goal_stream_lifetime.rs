//! Defect: dropping a started event stream stranded the goal's busy ownership.
//! Root cause: goal lifecycle settlement existed only after consumer-driven stream exhaustion.
//! Work item: 20261017 persistent-goal turn lifetime recovery.

use super::goal_stream_fixture::{
    idle, session, session_slot, text_events, usage, wait_for_idle, Fixture, WAIT,
};
use futures::StreamExt;
use klaus_agent_core::engine::session::{Input, SystemPromptProvider, TurnOptions};
use klaus_agent_core::goal::types::ThreadGoalStatus;
use klaus_agent_core::types::events::EngineEvent;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Semaphore;

async fn check_drop_after(completed: bool) {
    let slot = session_slot();
    let prompt_entered = Arc::new(Semaphore::new(0));
    let system_prompt: Option<SystemPromptProvider> = if completed {
        None
    } else {
        let prompts = AtomicUsize::new(0);
        let entered = prompt_entered.clone();
        Some(Arc::new(move || {
            let entered = entered.clone();
            let held = prompts.fetch_add(1, Ordering::SeqCst) < 3;
            Box::pin(async move {
                if held {
                    entered.add_permits(1);
                    futures::future::pending::<()>().await;
                }
                Vec::new()
            })
        }))
    };
    let fixture = Fixture::with_system_prompt(&slot, system_prompt, {
        let slot = slot.clone();
        move |_, _, call| {
            let root = session(&slot);
            Box::pin(async_stream::stream! {
                if !completed || call > 0 { root.set_thread_goal(None, Some("paused".into()), None).await.unwrap(); }
                for event in text_events("Measured progress.", usage(11, 3)) { yield event; }
            })
        }
    }).await;
    fixture
        .session
        .set_thread_goal(Some("Keep making progress".into()), None, None)
        .await
        .unwrap();
    for _ in 0..if completed { 1 } else { 3 } {
        let mut stream = idle(&fixture.session).await.events;
        tokio::time::timeout(WAIT, async {
            while let Some(event) = stream.next().await {
                if matches!(event, EngineEvent::TurnFailed { .. }) {
                    panic!("{event:?}");
                }
                if if completed {
                    matches!(event, EngineEvent::TurnCompleted { .. })
                } else {
                    matches!(event, EngineEvent::TurnStarted { .. })
                } {
                    return;
                }
            }
            panic!("the requested lifecycle boundary was never emitted");
        })
        .await
        .unwrap();
        if !completed {
            tokio::time::timeout(WAIT, prompt_entered.acquire())
                .await
                .expect("the actual turn is held before provider dispatch")
                .unwrap()
                .forget();
        }
        drop(stream);
        wait_for_idle(&fixture.session).await;
        let snapshot = fixture.session.thread_goal().await.unwrap().unwrap();
        assert_eq!(
            snapshot.status,
            ThreadGoalStatus::Active,
            "subscriber loss is not evidence of an empty or failed model turn"
        );
        if completed {
            assert_eq!(
                snapshot.tokens_used, 14,
                "a completed response is settled even without another poll"
            );
        } else {
            assert!(
                fixture.requests.lock().unwrap().is_empty(),
                "a pre-provider subscriber drop contains no model progress evidence"
            );
        }
    }
    let events = tokio::time::timeout(
        WAIT,
        idle(&fixture.session).await.events.collect::<Vec<_>>(),
    )
    .await
    .expect("idle continuation must remain usable");
    assert!(
        events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnStarted { .. })),
        "{events:?}"
    );
    assert!(
        events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnCompleted { .. })),
        "{events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{events:?}"
    );
    assert_eq!(
        fixture.session.thread_goal().await.unwrap().unwrap().status,
        ThreadGoalStatus::Paused
    );
    fixture.finish().await;
}

#[tokio::test]
async fn three_pre_provider_stream_drops_release_ownership_without_synthesizing_no_progress() {
    check_drop_after(false).await;
}

#[tokio::test]
async fn dropping_events_after_turn_completed_settles_usage_and_releases_goal_ownership() {
    check_drop_after(true).await;
}

struct UnavailableGoals {
    path: PathBuf,
    backup: PathBuf,
    restored: bool,
}

impl UnavailableGoals {
    fn new(config: &std::path::Path) -> Self {
        let path = config.to_owned();
        assert!(path.is_absolute() && path.is_dir());
        assert!(
            path.join("goals.sqlite3").is_file(),
            "only the actual fixture database may be obstructed"
        );
        let backup = path.with_file_name(format!("goal-cleanup-backup-{}", uuid::Uuid::new_v4()));
        assert!(!backup.exists());
        std::fs::rename(&path, &backup).unwrap();
        std::fs::write(&path, "The fixture temporarily blocks database access.").unwrap();
        Self {
            path,
            backup,
            restored: false,
        }
    }

    fn restore(mut self) {
        std::fs::remove_file(&self.path).unwrap();
        std::fs::rename(&self.backup, &self.path).unwrap();
        self.restored = true;
    }
}

impl Drop for UnavailableGoals {
    fn drop(&mut self) {
        if !self.restored {
            let _ = std::fs::remove_file(&self.path);
            let _ = std::fs::rename(&self.backup, &self.path);
        }
    }
}

#[tokio::test]
async fn a_failed_dropped_stream_settlement_is_retried_once_before_the_next_turn() {
    let slot = session_slot();
    let usage_accepted = Arc::new(Semaphore::new(0));
    let fixture = Fixture::new(&slot, {
        let slot = slot.clone();
        let usage_accepted = usage_accepted.clone();
        move |_, _, call| {
            if call == 0 {
                let accepted = usage_accepted.clone();
                return Box::pin(async_stream::stream! {
                    for event in text_events("Progress before subscriber loss.", usage(11, 3)) { yield event; }
                    accepted.add_permits(1);
                    futures::future::pending::<()>().await;
                });
            }
            let root = session(&slot);
            if call == 1 {
                return Box::pin(async_stream::stream! {
                    assert_eq!(root.thread_goal().await.unwrap().unwrap().tokens_used, 14, "cleanup commits before replacement provider work");
                    root.set_thread_goal(None, Some("paused".into()), None).await.unwrap();
                    for event in text_events("Recovery completed.", usage(0, 0)) { yield event; }
                });
            }
            assert_eq!(call, 2, "only one later ordinary request is expected");
            Box::pin(async_stream::stream! {
                let paused = root.thread_goal().await.unwrap().unwrap();
                assert_eq!(paused.status, ThreadGoalStatus::Paused);
                assert_eq!(paused.tokens_used, 14);
                for event in text_events("The separate ordinary request completed.", usage(90, 9)) { yield event; }
            })
        }
    }).await;
    fixture
        .session
        .set_thread_goal(
            Some("Recover every accepted usage receipt".into()),
            None,
            None,
        )
        .await
        .unwrap();
    let root = fixture.session.clone();
    let driver = tokio::spawn(async move { idle(&root).await.events.collect::<Vec<_>>().await });
    tokio::time::timeout(WAIT, usage_accepted.acquire())
        .await
        .expect("the provider usage was accepted before response exhaustion")
        .unwrap()
        .forget();
    assert_eq!(
        fixture
            .session
            .thread_goal()
            .await
            .unwrap()
            .unwrap()
            .tokens_used,
        0
    );
    let unavailable = UnavailableGoals::new(fixture.config_root());
    driver.abort();
    assert!(driver.await.unwrap_err().is_cancelled());
    let runtime = fixture.session.goal_runtime().unwrap();
    let cleanup = tokio::time::timeout(WAIT, runtime.wait_for_idle_cleanup())
        .await
        .expect("failed cleanup must settle explicitly");
    assert!(
        cleanup.is_err(),
        "the pending cleanup retains a real database failure"
    );
    assert!(!runtime.turn_in_flight());
    assert!(
        fixture.session.thread_goal().await.is_err(),
        "the settlement failure is an actual storage failure"
    );
    unavailable.restore();

    let events = tokio::time::timeout(
        WAIT,
        idle(&fixture.session).await.events.collect::<Vec<_>>(),
    )
    .await
    .expect("restored storage permits idle continuation");
    assert!(
        events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnCompleted { .. })),
        "{events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{events:?}"
    );
    let recovered = fixture.session.thread_goal().await.unwrap().unwrap();
    assert_eq!(
        recovered.tokens_used, 14,
        "the interrupted response is recovered exactly once"
    );
    assert_eq!(recovered.status, ThreadGoalStatus::Paused);
    let ordinary = tokio::time::timeout(
        WAIT,
        fixture
            .session
            .run_streamed(
                Input::Text("Answer this separate request while the goal stays paused.".into()),
                TurnOptions::default(),
            )
            .events
            .collect::<Vec<_>>(),
    )
    .await
    .expect("the ordinary paused-goal request must settle");
    assert!(
        ordinary
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnCompleted { .. })),
        "{ordinary:?}"
    );
    assert!(
        !ordinary
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{ordinary:?}"
    );
    assert_eq!(
        fixture
            .session
            .thread_goal()
            .await
            .unwrap()
            .unwrap()
            .tokens_used,
        14,
        "a later ordinary request cannot charge its 99 tokens to the paused goal"
    );
    let resumed = fixture
        .session
        .set_thread_goal(None, Some("active".into()), None)
        .await
        .unwrap();
    assert_eq!(
        resumed.goal.tokens_used, 14,
        "a later status mutation cannot replay the recovered receipt"
    );
    fixture
        .session
        .set_thread_goal(None, Some("paused".into()), None)
        .await
        .unwrap();
    assert_eq!(fixture.requests.lock().unwrap().len(), 3);
    fixture.finish().await;
}

#[tokio::test]
async fn run_claim_revision_is_published_before_the_provider_and_accepts_a_second_editor_save() {
    use klaus_agent_core::goal::{runtime::GoalSnapshot, service::HostGoalMutation};
    use std::sync::Mutex;
    let slot = session_slot();
    let published = Arc::new(Mutex::new(Vec::<GoalSnapshot>::new()));
    let observed_at_provider = Arc::new(Mutex::new(None));
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(Semaphore::new(0));
    let fixture = Fixture::new(&slot, {
        let published = published.clone();
        let observed = observed_at_provider.clone();
        let entered = entered.clone();
        let release = release.clone();
        move |_, _, call| {
            assert_eq!(call, 0, "the held response is the only inference request");
            *observed.lock().unwrap() = published.lock().unwrap().last().cloned();
            entered.notify_one();
            let release = release.clone();
            Box::pin(async_stream::stream! {
                release.acquire().await.unwrap().forget();
                for event in text_events("The admitted response settled.", usage(0, 0)) {
                    yield event;
                }
            })
        }
    })
    .await;
    fixture.session.set_goal_observer(Some(Arc::new({
        let published = published.clone();
        move |snapshot| {
            let mut published = published.lock().unwrap();
            assert!(published.len() < 32, "bounded observer notifications");
            published.push(snapshot);
        }
    })));
    let created = fixture
        .session
        .set_thread_goal(
            Some("Initial editor objective".into()),
            Some("paused".into()),
            Some(Some(500)),
        )
        .await
        .unwrap();
    let first = fixture
        .session
        .set_thread_goal_checked(HostGoalMutation {
            objective: Some("First saved editor objective".into()),
            status: Some("active".into()),
            expected_goal_id: Some(created.goal.goal_id.clone()),
            expected_revision: Some(created.revision),
            ..Default::default()
        })
        .await
        .unwrap();
    let turn = idle(&fixture.session).await;
    let consumer = tokio::spawn(async move {
        let mut stream = turn.events;
        let mut count = 0;
        while stream.next().await.is_some() {
            count += 1;
            assert!(count <= 128, "bounded held-turn events");
        }
    });
    tokio::time::timeout(WAIT, entered.notified())
        .await
        .unwrap();
    let actual = fixture.session.goal_snapshot().await.unwrap();
    let observed = observed_at_provider
        .lock()
        .unwrap()
        .clone()
        .expect("the observer has the actual committed provider claim before model entry");
    assert_eq!(
        observed, actual,
        "provider admission cannot hide its new revision from the editor"
    );
    assert!(observed.revision > first.revision);
    assert_eq!(
        observed.goal.as_ref().unwrap().goal_id,
        created.goal.goal_id
    );
    let second = fixture
        .session
        .set_thread_goal_checked(HostGoalMutation {
            objective: Some("Second editor save while the response is held".into()),
            status: Some("paused".into()),
            expected_goal_id: Some(created.goal.goal_id.clone()),
            expected_revision: Some(observed.revision),
            ..Default::default()
        })
        .await
        .expect("a second editor save uses the published revision without a hidden goal:get");
    assert_eq!(second.goal.goal_id, first.goal.goal_id);
    assert_eq!(second.goal.created_at_ms, first.goal.created_at_ms);
    assert_eq!(second.goal.token_budget, first.goal.token_budget);
    assert_eq!(
        second.goal.objective,
        "Second editor save while the response is held"
    );
    assert_eq!(second.goal.status, ThreadGoalStatus::Paused);
    assert_eq!(
        fixture.session.goal_snapshot().await.unwrap().revision,
        second.revision
    );
    release.add_permits(1);
    tokio::time::timeout(WAIT, consumer).await.unwrap().unwrap();
    assert_eq!(fixture.requests.lock().unwrap().len(), 1);
    fixture.finish().await;
}

#[tokio::test]
async fn stopping_a_held_goal_publishes_run_release_before_a_direct_editor_clear() {
    use klaus_agent_core::goal::runtime::GoalSnapshot;
    use klaus_agent_core::planning::CollaborationMode;
    use std::sync::Mutex;
    let slot = session_slot();
    let entered = Arc::new(Semaphore::new(0));
    let published = Arc::new(Mutex::new(Vec::<GoalSnapshot>::new()));
    let fixture = Fixture::new(&slot, {
        let entered = entered.clone();
        move |_, signal, call| {
            assert_eq!(call, 0, "Stop cannot start a replacement inference request");
            let entered = entered.clone();
            Box::pin(async_stream::stream! {
                yield klaus_agent_core::types::provider::ModelStreamEvent::MessageStart {
                    model: "goal-stream-lifetime".into(),
                };
                entered.add_permits(1);
                signal.cancelled().await;
            })
        }
    })
    .await;
    fixture.session.set_goal_observer(Some(Arc::new({
        let published = published.clone();
        move |snapshot| {
            let mut published = published.lock().unwrap();
            assert!(published.len() < 32, "bounded observer notifications");
            published.push(snapshot);
        }
    })));
    fixture
        .session
        .goal_runtime()
        .unwrap()
        .testing_freeze_elapsed();
    let created = fixture
        .session
        .set_thread_goal(
            Some("Stop this held goal, then clear it from the editor".into()),
            None,
            Some(Some(500)),
        )
        .await
        .unwrap();
    let turn = idle(&fixture.session).await;
    let consumer = tokio::spawn(async move {
        let mut events = turn.events;
        let mut count = 0;
        while events.next().await.is_some() {
            count += 1;
            assert!(count <= 128, "bounded stopped-turn events");
        }
    });
    tokio::time::timeout(WAIT, entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    fixture.session.pause_goal_for_shutdown().await.unwrap();
    let paused = published.lock().unwrap().last().unwrap().clone();
    assert_eq!(
        paused.goal.as_ref().unwrap().status,
        ThreadGoalStatus::Paused
    );
    fixture.session.interrupt_by_user();
    tokio::time::timeout(WAIT, consumer).await.unwrap().unwrap();
    wait_for_idle(&fixture.session).await;
    let released = published.lock().unwrap().last().unwrap().clone();
    let actual = fixture.session.goal_snapshot().await.unwrap();
    assert_eq!(
        released, actual,
        "the last committed owner-release revision must already be published at idle"
    );
    assert_eq!(
        released.revision,
        paused.revision + 1,
        "removing the admitted run is one public revision"
    );
    assert_eq!(
        released.goal.as_ref().unwrap().goal_id,
        created.goal.goal_id
    );
    for mode in [CollaborationMode::Plan, CollaborationMode::Default] {
        fixture
            .session
            .planning_runtime()
            .set_mode(mode, None)
            .unwrap();
        fixture.session.synchronize_goal_mode().await.unwrap();
    }
    assert_eq!(fixture.session.goal_snapshot().await.unwrap(), released);
    assert!(fixture
        .session
        .clear_thread_goal_checked(Some(created.goal.goal_id), Some(released.revision))
        .await
        .expect("the editor clears with its published revision, without a preparatory goal:get"));
    assert!(fixture.session.thread_goal().await.unwrap().is_none());
    assert_eq!(fixture.requests.lock().unwrap().len(), 1);
    fixture.finish().await;
}
