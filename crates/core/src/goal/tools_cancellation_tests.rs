//! Defect: a cancelled model tool can create a goal after losing its caller.
//! Root cause: a queued insert outlives its reply future without a commit fence.
//! Work item: 20261017 persistent-goal request cancellation authority.

use super::*;
use crate::goal::runtime::GoalRuntime;
use crate::goal::store;
use std::future::{poll_fn, Future};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::task::Poll;
use std::time::Duration;

const WAIT: Duration = Duration::from_secs(10);

struct ConfigGuard {
    previous: Option<std::ffi::OsString>,
    root: PathBuf,
    _lock: std::sync::MutexGuard<'static, ()>,
}

impl ConfigGuard {
    fn new() -> Self {
        let lock = crate::test_support::CONFIG_ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let previous = std::env::var_os("KLAUS_CONFIG_DIR");
        let root = std::env::temp_dir().join(format!(
            "klaus-goal-tool-cancellation-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&root).unwrap();
        crate::runtime_process::set_process_environment_var("KLAUS_CONFIG_DIR", &root);
        Self {
            previous,
            root,
            _lock: lock,
        }
    }
}

impl Drop for ConfigGuard {
    fn drop(&mut self) {
        match self.previous.take() {
            Some(previous) => {
                crate::runtime_process::set_process_environment_var("KLAUS_CONFIG_DIR", previous)
            }
            None => crate::runtime_process::remove_process_environment_var("KLAUS_CONFIG_DIR"),
        }
        if std::thread::panicking() {
            eprintln!("Retained cancelled-tool fixture: {}", self.root.display());
        } else {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }
}

async fn queued_create_loses_caller(drop_handler: bool) {
    let config = ConfigGuard::new();
    assert_eq!(store::get_goal("cancelled-create").await.unwrap(), None);
    let armed = Arc::new(AtomicBool::new(false));
    let write_lease = Arc::new(Mutex::new(None::<rusqlite::Connection>));
    let runtime = Arc::new(GoalRuntime::with_clock(
        "cancelled-create".into(),
        {
            let database = config.root.join("goals.sqlite3");
            let armed = armed.clone();
            let write_lease = write_lease.clone();
            Arc::new(move || {
                if armed.swap(false, Ordering::SeqCst) {
                    // UUID allocation follows the authoritative identity read.
                    // The actual goal write must now wait behind this writer.
                    let connection = rusqlite::Connection::open(&database).unwrap();
                    connection.busy_timeout(WAIT).unwrap();
                    connection.execute_batch("BEGIN IMMEDIATE").unwrap();
                    *write_lease.lock().unwrap() = Some(connection);
                }
                uuid::Uuid::new_v4().to_string()
            })
        },
        Arc::new(|| 100),
        Arc::new(|| 0),
        None,
        None,
    ));
    runtime.on_turn_start().await.unwrap();
    let captured = runtime.request_identity();
    assert_eq!(captured.generation, None);
    let mut ctx = crate::tools::builtins::test_ctx::make_ctx(".");
    ctx.goal = Some(runtime.clone());
    ctx.goal_generation = Some(Arc::new(Mutex::new(captured)));
    let signal = ctx.signal.clone();
    let tool = create_create_goal_tool();
    let input = crate::runtime_values::runtime_json::parse(&RuntimeString::from(
        r#"{"objective":"Only pursue this while its caller is authorized"}"#,
    ))
    .unwrap();
    armed.store(true, Ordering::SeqCst);
    let mut call = Box::pin(tool.call(input, &ctx, None));
    tokio::time::timeout(
        WAIT,
        poll_fn(|cx| {
            let state = call.as_mut().poll(cx);
            assert!(state.is_pending(), "the SQLite writer must hold the tool");
            if write_lease.lock().unwrap().is_some() {
                // This poll reached the insert's reply await, so its job has
                // entered the bounded SQL queue before caller cancellation.
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }),
    )
    .await
    .expect("the real create tool must reach its blocked write");

    runtime.suppress_continuation();
    signal.cancel();
    let mut call = if drop_handler {
        drop(call);
        None
    } else {
        Some(call)
    };
    write_lease
        .lock()
        .unwrap()
        .take()
        .unwrap()
        .execute_batch("ROLLBACK")
        .unwrap();
    let tool_result = if let Some(call) = call.take() {
        Some(
            tokio::time::timeout(WAIT, call)
                .await
                .expect("cancellation must settle the admitted tool")
                .unwrap(),
        )
    } else {
        None
    };
    // This read queues after the admitted write and therefore fences its
    // completion even when the model handler and reply receiver were dropped.
    let after_cancel = tokio::time::timeout(WAIT, store::get_record("cancelled-create"))
        .await
        .expect("the worker must settle after releasing the writer")
        .unwrap();
    let reopened = GoalRuntime::with_clock(
        "cancelled-create".into(),
        Arc::new(|| "restarted-owner".into()),
        Arc::new(|| 200),
        Arc::new(|| 0),
        None,
        None,
    );
    reopened.recover_interrupted_run().await.unwrap();
    let recovered = store::get_record("cancelled-create").await.unwrap();
    assert_eq!(
        after_cancel.goal, None,
        "a model write queued before cancellation must not create an active goal afterward; restart snapshot: {recovered:?}"
    );
    assert_eq!(after_cancel.revision, 0);
    assert_eq!(after_cancel.intent_revision, 0);
    assert_eq!(recovered, after_cancel);
    if let Some(result) = tool_result {
        assert_eq!(result.serializable.is_error, Some(true));
    }
}

#[tokio::test]
async fn cancelled_create_tool_cannot_commit_a_goal_after_its_queued_write_is_released() {
    queued_create_loses_caller(false).await;
}

#[tokio::test]
async fn dropped_cancelled_create_tool_cannot_leave_an_ownerless_active_goal_on_restart() {
    queued_create_loses_caller(true).await;
}
