//! Defect: cancelled accounting observers and failed descendant writes could lose acknowledgement.
//! Root cause: durable write ownership ended before local progress ownership was settled.
//! Work item 20261017: retain accounting acknowledgement and generation-scoped retries.

use klaus_agent_core::goal::{
    BudgetLimitedGoalDisposition, GoalAccountingMode, GoalRuntime, ThreadGoalStatus,
};
use klaus_agent_core::runtime_process::{
    remove_process_environment_var, set_process_environment_var,
};
use klaus_agent_core::schemas::message::Usage;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

const WAIT: Duration = Duration::from_secs(5);

struct Home {
    path: PathBuf,
    previous: Option<String>,
}

impl Home {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "klaus-goal-accounting-ack-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&path).unwrap();
        let previous = std::env::var("KLAUS_CONFIG_DIR").ok();
        set_process_environment_var("KLAUS_CONFIG_DIR", &*path.to_string_lossy());
        Self { path, previous }
    }
}

impl Drop for Home {
    fn drop(&mut self) {
        match &self.previous {
            Some(value) => set_process_environment_var("KLAUS_CONFIG_DIR", value),
            None => remove_process_environment_var("KLAUS_CONFIG_DIR"),
        }
        if std::thread::panicking() {
            eprintln!(
                "Retained accounting acknowledgement fixture: {}",
                self.path.display()
            );
        } else {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

/// Obstruct only this fixture's complete configuration directory. Restoring the
/// whole directory keeps SQLite's database and sidecars in the same namespace.
struct StorageObstruction {
    path: PathBuf,
    backup: PathBuf,
    restored: bool,
}

impl StorageObstruction {
    fn new(home: &Home) -> Self {
        let path = home.path.clone();
        let backup = path.with_extension("unavailable-backup");
        assert!(!backup.exists());
        std::fs::rename(&path, &backup).unwrap();
        std::fs::write(
            &path,
            b"Fixture-owned storage is temporarily unavailable.\n",
        )
        .unwrap();
        Self {
            path,
            backup,
            restored: false,
        }
    }

    fn restore(mut self) {
        self.restore_inner();
    }

    fn restore_inner(&mut self) {
        if self.restored {
            return;
        }
        std::fs::remove_file(&self.path).unwrap();
        std::fs::rename(&self.backup, &self.path).unwrap();
        self.restored = true;
    }
}

impl Drop for StorageObstruction {
    fn drop(&mut self) {
        if !self.restored {
            let _ = std::fs::remove_file(&self.path);
            let _ = std::fs::rename(&self.backup, &self.path);
        }
    }
}

fn usage(input: u64, output: u64) -> Usage {
    Usage {
        input_tokens: input,
        output_tokens: output,
        cache_read_input_tokens: 19,
        cache_creation_input_tokens: 23,
        total_tokens: Some(999),
    }
}

fn runtime(thread: &str) -> Arc<GoalRuntime> {
    Arc::new(GoalRuntime::new(
        thread.into(),
        Arc::new(|| uuid::Uuid::new_v4().to_string()),
        Arc::new(|| 0),
        None,
    ))
}

#[tokio::test]
async fn cancelling_the_accounting_observer_cannot_replay_a_committed_charge() {
    let home = Home::new();
    let entered = Arc::new(Notify::new());
    let armed = Arc::new(AtomicBool::new(false));
    let now = {
        let entered = entered.clone();
        let armed = armed.clone();
        Arc::new(move || {
            if armed.load(Ordering::SeqCst) {
                entered.notify_one();
            }
            0_i64
        }) as Arc<dyn Fn() -> i64 + Send + Sync>
    };
    let runtime = Arc::new(GoalRuntime::with_clock(
        "cancelled-accounting-ack".into(),
        Arc::new(|| uuid::Uuid::new_v4().to_string()),
        now,
        Arc::new(|| 0),
        None,
        None,
    ));
    runtime
        .create_host_goal("Keep every accepted usage receipt".into(), None, None, None)
        .await
        .unwrap()
        .unwrap();
    runtime.on_turn_start().await.unwrap();
    runtime.on_token_usage(&usage(7, 4));
    let connection = rusqlite::Connection::open(home.path.join("goals.sqlite3")).unwrap();
    connection.execute_batch("BEGIN EXCLUSIVE").unwrap();
    armed.store(true, Ordering::SeqCst);
    let observer = tokio::spawn({
        let runtime = runtime.clone();
        async move {
            runtime
                .account_progress(
                    GoalAccountingMode::ActiveOnly,
                    BudgetLimitedGoalDisposition::KeepActive,
                )
                .await
        }
    });
    tokio::time::timeout(WAIT, entered.notified())
        .await
        .expect("accounting reached the actual blocked database mutation");
    observer.abort();
    assert!(
        observer.await.unwrap_err().is_cancelled(),
        "the observer was actually cancelled"
    );
    connection.execute_batch("COMMIT").unwrap();
    for _ in 0..2 {
        tokio::time::timeout(
            WAIT,
            runtime.account_progress(
                GoalAccountingMode::ActiveOnly,
                BudgetLimitedGoalDisposition::KeepActive,
            ),
        )
        .await
        .expect("the retained write must release admission")
        .unwrap();
    }
    let goal = runtime.goal().await.unwrap().unwrap();
    assert_eq!(
        goal.tokens_used, 11,
        "the first committed charge must be acknowledged once"
    );
    assert_eq!(goal.status, ThreadGoalStatus::Active);
}

#[tokio::test]
async fn a_failed_descendant_charge_retries_once_and_cannot_charge_a_replacement() {
    let home = Home::new();
    let runtime = runtime("descendant-accounting-retry");
    let first = runtime
        .create_host_goal("Original root work".into(), None, None, None)
        .await
        .unwrap()
        .unwrap()
        .goal;
    runtime.on_turn_start().await.unwrap();
    let unavailable = StorageObstruction::new(&home);
    let failed = runtime
        .on_descendant_usage(first.goal_id.clone(), &usage(5, 3))
        .await;
    assert!(
        failed.is_err(),
        "the descendant sample hit an actual storage failure"
    );
    unavailable.restore();
    for _ in 0..2 {
        runtime
            .account_progress(
                GoalAccountingMode::ActiveOnly,
                BudgetLimitedGoalDisposition::KeepActive,
            )
            .await
            .unwrap();
    }
    assert_eq!(
        runtime.goal().await.unwrap().unwrap().tokens_used,
        8,
        "the failed descendant sample must be retained and retried exactly once"
    );

    let unavailable = StorageObstruction::new(&home);
    assert!(runtime
        .on_descendant_usage(first.goal_id.clone(), &usage(50, 30))
        .await
        .is_err());
    unavailable.restore();
    runtime.clear_goal().await.unwrap();
    let replacement = runtime
        .create_host_goal("Replacement root work".into(), None, None, None)
        .await
        .unwrap()
        .unwrap()
        .goal;
    assert_ne!(replacement.goal_id, first.goal_id);
    for _ in 0..2 {
        runtime
            .account_progress(
                GoalAccountingMode::ActiveOnly,
                BudgetLimitedGoalDisposition::KeepActive,
            )
            .await
            .unwrap();
    }
    runtime
        .on_descendant_usage(first.goal_id, &usage(500, 300))
        .await
        .unwrap();
    let current = runtime.goal().await.unwrap().unwrap();
    assert_eq!(current.goal_id, replacement.goal_id);
    assert_eq!(
        current.tokens_used, 0,
        "pending and late samples retain the old generation"
    );
}
