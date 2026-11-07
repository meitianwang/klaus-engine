//! Defect: a second host-created objective reused a completed goal generation.
//! Root cause: Session::set_thread_goal created only when no goal existed.
//! Fixed for defect session 01a0745e: completed goals require a new objective
//! and creation resets the identifier, budget, elapsed time and token usage.

use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::goal::runtime::GoalUpdateOutcome;
use klaus_agent_core::goal::types::ThreadGoalStatus;
use klaus_agent_core::runtime_process::{
    remove_process_environment_var, set_process_environment_var, RuntimeProcessContext,
};
use klaus_agent_core::schemas::message::Usage;
use serde_json::json;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

struct ConfigHome {
    path: PathBuf,
    previous: Option<String>,
}

impl ConfigHome {
    fn new() -> Self {
        let previous = std::env::var("KLAUS_CONFIG_DIR").ok();
        let path = std::env::temp_dir().join(format!(
            "klaus-goal-recreation-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&path).unwrap();
        set_process_environment_var("KLAUS_CONFIG_DIR", &*path.to_string_lossy());
        Self { path, previous }
    }
}

impl Drop for ConfigHome {
    fn drop(&mut self) {
        match &self.previous {
            Some(value) => set_process_environment_var("KLAUS_CONFIG_DIR", value),
            None => remove_process_environment_var("KLAUS_CONFIG_DIR"),
        }
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

#[tokio::test]
async fn host_recreates_completed_goals_with_new_identity_and_accounting() {
    let home = ConfigHome::new();
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let now = Arc::new(AtomicI64::new(1_000));
    let mut options = agent_options(process.clone());
    options.deps.now = Some({
        let now = now.clone();
        Arc::new(move || now.load(Ordering::Relaxed))
    });
    options.deps.call_model = Some(Box::new(|_, _| {
        panic!("goal host operations must not call a model")
    }));
    let agent = Agent::new(options);
    let store: Arc<dyn klaus_agent_core::store::session_store::SessionStore> = Arc::new(
        klaus_agent_core::store::jsonl_store::JsonlSessionStore::new(
            klaus_agent_core::store::jsonl_store::JsonlSessionStoreOptions {
                dir: Some(home.path.join("sessions").to_string_lossy().into_owned()),
                archive_dir: None,
            },
        ),
    );
    let session = agent
        .open_session(
            store,
            StartSessionOptions {
                cwd: Some(home.path.to_string_lossy().to_string().into()),
                user_config_dir: Some(home.path.to_string_lossy().to_string().into()),
                config: Some(json!({ "goal": true, "prewarm": false })),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let first = session
        .set_thread_goal(Some("First objective".into()), None, Some(Some(1_000)))
        .await
        .unwrap();
    let runtime = session.goal_runtime().unwrap();
    runtime.on_turn_start().await.unwrap();
    runtime.on_token_usage(&Usage {
        input_tokens: 100,
        output_tokens: 50,
        cache_read_input_tokens: 0,
        cache_creation_input_tokens: 0,
        total_tokens: None,
    });
    now.store(4_000, Ordering::Relaxed);
    let completed = match runtime.update_goal_status(ThreadGoalStatus::Complete).await {
        GoalUpdateOutcome::Updated(goal) => goal,
        other => panic!("expected completion, got {other:?}"),
    };
    assert_eq!(completed.tokens_used, 150);
    runtime.on_turn_end(false).await.unwrap();
    assert!(session
        .set_thread_goal(None, Some("active".into()), None)
        .await
        .is_err());
    assert!(session
        .set_thread_goal(Some("   ".into()), None, Some(Some(2_000)))
        .await
        .is_err());
    assert_eq!(session.thread_goal().await.unwrap().unwrap(), completed);

    now.store(5_000, Ordering::Relaxed);
    let second = session
        .set_thread_goal(Some("Second objective".into()), None, Some(Some(2_000)))
        .await
        .unwrap();
    assert_ne!(second.goal.goal_id, first.goal.goal_id);
    assert_eq!(second.goal.status, ThreadGoalStatus::Active);
    assert_eq!(second.goal.token_budget, Some(2_000));
    assert_eq!(second.goal.tokens_used, 0);
    assert_eq!(second.goal.time_used_seconds, 0);
    assert_eq!(second.goal.created_at_ms, 5_000);
    assert!(!second.turn_in_flight);
    assert!(second.steer.unwrap().contains("Second objective"));

    session
        .set_thread_goal(None, Some("paused".into()), None)
        .await
        .unwrap();
    let edited = session
        .set_thread_goal(Some("Refined objective".into()), None, None)
        .await
        .unwrap();
    assert_eq!(edited.goal.goal_id, second.goal.goal_id);
    assert_eq!(edited.goal.status, ThreadGoalStatus::Paused);
    assert_eq!(edited.goal.token_budget, Some(2_000));
    let resumed = session
        .set_thread_goal(None, Some("active".into()), None)
        .await
        .unwrap();
    assert_eq!(resumed.goal.goal_id, second.goal.goal_id);
    assert_eq!(resumed.goal.status, ThreadGoalStatus::Active);
    runtime.update_goal_status(ThreadGoalStatus::Complete).await;
    let unbudgeted = session
        .set_thread_goal(Some("Another objective".into()), None, None)
        .await
        .unwrap();
    assert_ne!(unbudgeted.goal.goal_id, second.goal.goal_id);
    assert_eq!(unbudgeted.goal.token_budget, None);
    assert_eq!(unbudgeted.goal.tokens_used, 0);

    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
}
