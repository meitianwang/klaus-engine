//! Defect: unchanged goal observation repeatedly initialized and committed the database.
//! Root cause: every snapshot reopened SQLite and entered a write transaction.
//! Work item 20261021: keep idle observation and repeated reads free of writes.

use klaus_agent_core::goal::{store, GoalRuntime};
use klaus_agent_core::runtime_process::{
    remove_process_environment_var, set_process_environment_var,
};
use rusqlite::Connection;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

struct Home {
    path: PathBuf,
    previous: Option<String>,
}
impl Home {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("klaus-goal-idle-storage-{}", uuid::Uuid::new_v4()));
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
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
fn version(connection: &Connection) -> i64 {
    connection
        .pragma_query_value(None, "data_version", |row| row.get(0))
        .unwrap()
}
#[cfg(unix)]
fn usage() -> (i64, i64) {
    let mut value = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // The kernel initializes the complete structure on a successful query.
    assert_eq!(
        unsafe { libc::getrusage(libc::RUSAGE_SELF, value.as_mut_ptr()) },
        0
    );
    let value = unsafe { value.assume_init() };
    (
        value.ru_utime.tv_sec * 1_000_000
            + i64::from(value.ru_utime.tv_usec)
            + value.ru_stime.tv_sec * 1_000_000
            + i64::from(value.ru_stime.tv_usec),
        value.ru_oublock,
    )
}
#[cfg(not(unix))]
fn usage() -> (i64, i64) {
    (0, 0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_observers_and_repeated_reads_do_not_commit_unchanged_state() {
    let home = Home::new();
    let mut runtimes = Vec::new();
    for index in 0..20 {
        let thread = format!("idle-thread-{index}");
        store::get_record(&thread).await.unwrap();
        runtimes.push(Arc::new(GoalRuntime::new(
            thread.into(),
            Arc::new(|| uuid::Uuid::new_v4().to_string()),
            Arc::new(|| 1_000),
            None,
        )));
    }
    let connection = Connection::open(home.path.join("goals.sqlite3")).unwrap();
    let before = version(&connection);
    let (cpu_before, blocks_before) = usage();
    let idle_started = Instant::now();
    for runtime in &runtimes {
        runtime.watch_external_changes();
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    let idle_elapsed = idle_started.elapsed();
    let (cpu_idle, blocks_idle) = usage();
    let idle_commits = version(&connection) - before;
    let mut latencies = Vec::new();
    for index in 0..200 {
        let started = Instant::now();
        store::get_record(&format!("idle-thread-{}", index % 20))
            .await
            .unwrap();
        latencies.push(started.elapsed().as_micros());
    }
    latencies.sort_unstable();
    let (cpu_after, blocks_after) = usage();
    let read_commits = version(&connection) - before - idle_commits;
    eprintln!("GOAL_STORAGE_METRICS threads=20 idle_ms={} idle_cpu_us={} idle_out_blocks={} idle_external_commit_epochs={} reads=200 read_cpu_us={} read_out_blocks={} read_p50_us={} read_p95_us={} read_external_commit_epochs={}", idle_elapsed.as_millis(), cpu_idle-cpu_before, blocks_idle-blocks_before, idle_commits, cpu_after-cpu_idle, blocks_after-blocks_idle, latencies[100], latencies[190], read_commits);
    drop(runtimes);
    assert_eq!(
        idle_commits, 0,
        "idle goal observation must not write unchanged state"
    );
    assert_eq!(
        read_commits, 0,
        "repeated goal reads must not write unchanged state"
    );
}

fn goal(thread: &str, objective: &str) -> klaus_agent_core::goal::ThreadGoal {
    klaus_agent_core::goal::ThreadGoal {
        thread_id: thread.into(),
        goal_id: "generation".into(),
        objective: objective.into(),
        status: klaus_agent_core::goal::ThreadGoalStatus::Active,
        token_budget: None,
        tokens_used: 0,
        time_used_seconds: 0,
        created_at_ms: 1,
        updated_at_ms: 1,
    }
}

#[tokio::test]
async fn shared_observation_delivers_an_external_sqlite_writer_without_local_reads() {
    let home = Home::new();
    store::insert_goal(goal("external", "Before"))
        .await
        .unwrap()
        .unwrap();
    let (send, mut receive) = tokio::sync::mpsc::unbounded_channel();
    let runtime = Arc::new(GoalRuntime::new(
        "external".into(),
        Arc::new(|| uuid::Uuid::new_v4().to_string()),
        Arc::new(|| 1_000),
        Some(Arc::new(move |snapshot| {
            let _ = send.send(snapshot);
        })),
    ));
    runtime.watch_external_changes();
    tokio::time::sleep(Duration::from_millis(350)).await;
    let connection = Connection::open(home.path.join("goals.sqlite3")).unwrap();
    connection.execute_batch("BEGIN IMMEDIATE; UPDATE goals SET objective = x'41006600740065007200' WHERE thread_id = 'external'; UPDATE goal_threads SET revision = revision + 1 WHERE thread_id = 'external'; COMMIT;").unwrap();
    let snapshot = tokio::time::timeout(Duration::from_secs(2), receive.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.goal.unwrap().objective, "After");
    drop(runtime);
}

#[tokio::test]
async fn cached_goal_storage_isolates_roots_and_reopens_a_replaced_database() {
    let home = Home::new();
    store::insert_goal(goal("thread", "First root"))
        .await
        .unwrap()
        .unwrap();
    let (send, mut receive) = tokio::sync::mpsc::unbounded_channel();
    let runtime = Arc::new(GoalRuntime::new(
        "thread".into(),
        Arc::new(|| uuid::Uuid::new_v4().to_string()),
        Arc::new(|| 1_000),
        Some(Arc::new(move |snapshot| {
            let _ = send.send(snapshot);
        })),
    ));
    runtime.watch_external_changes();
    tokio::time::sleep(Duration::from_millis(350)).await;
    let second = home.path.join("second");
    std::fs::create_dir_all(&second).unwrap();
    set_process_environment_var("KLAUS_CONFIG_DIR", &*second.to_string_lossy());
    store::insert_goal(goal("thread", "Second root"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        store::get_goal("thread").await.unwrap().unwrap().objective,
        "Second root"
    );
    set_process_environment_var("KLAUS_CONFIG_DIR", &*home.path.to_string_lossy());
    assert_eq!(
        store::get_goal("thread").await.unwrap().unwrap().objective,
        "First root"
    );
    let replacement = home.path.join("replacement.sqlite3");
    let connection = Connection::open(home.path.join("goals.sqlite3")).unwrap();
    connection
        .execute("VACUUM INTO ?1", [&*replacement.to_string_lossy()])
        .unwrap();
    drop(connection);
    let connection = Connection::open(&replacement).unwrap();
    connection.execute_batch("UPDATE goals SET objective = x'5200650070006c006100630065006400' WHERE thread_id = 'thread';").unwrap();
    drop(connection);
    for suffix in ["", "-wal", "-shm"] {
        let original = home.path.join(format!("goals.sqlite3{suffix}"));
        if original.exists() {
            std::fs::rename(
                &original,
                home.path.join(format!("previous.sqlite3{suffix}")),
            )
            .unwrap();
        }
    }
    std::fs::rename(replacement, home.path.join("goals.sqlite3")).unwrap();
    assert_eq!(
        store::get_goal("thread").await.unwrap().unwrap().objective,
        "Replaced"
    );
    let snapshot = tokio::time::timeout(Duration::from_secs(2), receive.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        snapshot.goal.unwrap().objective,
        "Replaced",
        "a replacement with the same revision must still reach observers"
    );
    drop(runtime);
    let repaired = home.path.join("repaired.sqlite3");
    let connection = Connection::open(home.path.join("goals.sqlite3")).unwrap();
    connection
        .execute("VACUUM INTO ?1", [&*repaired.to_string_lossy()])
        .unwrap();
    drop(connection);
    // Reusing a handle must not make a direct corrupt replacement look absent.
    std::fs::write(
        home.path.join("goals.sqlite3"),
        b"invalid database replacement",
    )
    .unwrap();
    assert_eq!(
        store::get_goal("thread").await.unwrap_err().kind(),
        std::io::ErrorKind::InvalidData
    );
    for suffix in ["", "-wal", "-shm"] {
        let original = home.path.join(format!("goals.sqlite3{suffix}"));
        if original.exists() {
            std::fs::rename(
                &original,
                home.path.join(format!("corrupt.sqlite3{suffix}")),
            )
            .unwrap();
        }
    }
    std::fs::rename(repaired, home.path.join("goals.sqlite3")).unwrap();
    assert_eq!(
        store::get_goal("thread").await.unwrap().unwrap().objective,
        "Replaced",
        "the same worker must recover after corruption is atomically repaired"
    );
}

#[tokio::test]
async fn active_root_keeps_one_handle_while_idle_root_handles_are_bounded() {
    let home = Home::new();
    store::get_record("observed").await.unwrap();
    let runtime = Arc::new(GoalRuntime::new(
        "observed".into(),
        Arc::new(|| uuid::Uuid::new_v4().to_string()),
        Arc::new(|| 1_000),
        None,
    ));
    runtime.watch_external_changes();
    tokio::time::sleep(Duration::from_millis(350)).await;
    let mut idle_roots = Vec::new();
    for index in 0..12 {
        let root = home.path.join(format!("idle-root-{index}"));
        std::fs::create_dir_all(&root).unwrap();
        set_process_environment_var("KLAUS_CONFIG_DIR", &root);
        store::get_record("thread").await.unwrap();
        idle_roots.push(root);
    }
    assert!(
        home.path.join("goals.sqlite3-wal").exists(),
        "an actively observed root must stay open while other homes are visited"
    );
    let idle_handles = idle_roots
        .iter()
        .filter(|root| root.join("goals.sqlite3-wal").exists())
        .count();
    assert_eq!(
        idle_handles, 8,
        "idle homes must share a bounded connection cache"
    );
    set_process_environment_var("KLAUS_CONFIG_DIR", &home.path);
    assert!(store::get_goal("observed").await.unwrap().is_none());
    drop(runtime);
}

#[tokio::test]
async fn failed_observation_retries_after_a_legacy_file_is_repaired() {
    let home = Home::new();
    store::get_record("initialize-storage").await.unwrap();
    let legacy = home.path.join("goals/legacy.json");
    std::fs::create_dir_all(legacy.parent().unwrap()).unwrap();
    std::fs::write(&legacy, b"invalid legacy JSON").unwrap();
    let runtime = Arc::new(GoalRuntime::new(
        "legacy".into(),
        Arc::new(|| uuid::Uuid::new_v4().to_string()),
        Arc::new(|| 1_000),
        None,
    ));
    runtime.watch_external_changes();
    tokio::time::sleep(Duration::from_millis(350)).await;
    std::fs::write(&legacy, br#"{"goal":{"threadId":"legacy","goalId":"generation","objective":"Recovered","status":"active","tokensUsed":0,"timeUsedSeconds":0,"createdAtMs":1,"updatedAtMs":1}}"#).unwrap();
    let connection = Connection::open(home.path.join("goals.sqlite3")).unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let imported: bool = connection
                .query_row(
                    "SELECT EXISTS (SELECT 1 FROM goals WHERE thread_id = 'legacy')",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            if imported {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("legacy repair must recover without an unrelated SQL write or explicit goal read");
    drop(runtime);
}

#[tokio::test]
async fn local_goal_mutations_publish_once_when_shared_observation_is_active() {
    let _home = Home::new();
    store::get_record("local-publication").await.unwrap();
    let snapshots = Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed = snapshots.clone();
    let runtime = Arc::new(GoalRuntime::new(
        "local-publication".into(),
        Arc::new(|| uuid::Uuid::new_v4().to_string()),
        Arc::new(|| 1_000),
        Some(Arc::new(move |snapshot| {
            observed.lock().unwrap().push(snapshot);
        })),
    ));
    runtime.watch_external_changes();
    tokio::time::sleep(Duration::from_millis(350)).await;
    runtime
        .create_host_goal("Publish each change once".into(), None, None, None)
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(
        snapshots.lock().unwrap().len(),
        1,
        "the shared watcher must not echo an explicitly published create"
    );
    runtime
        .update_goal_status(klaus_agent_core::goal::ThreadGoalStatus::Complete)
        .await;
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(
        snapshots.lock().unwrap().len(),
        2,
        "each durable mutation must retain one observer publication"
    );
    drop(runtime);
}
