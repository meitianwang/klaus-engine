//! Ownership of complete tool executions through cancellation and persistence.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use futures::FutureExt;
use tokio::task::JoinHandle;
use tokio_util::task::task_tracker::TaskTrackerToken;
use tokio_util::task::TaskTracker;

use super::ToolCallResult;
use crate::runtime_values::RuntimeString;
use crate::sync_ext::MutexExt;
use crate::types::errors::ToolError;

/// Session-owned fence for accepted tool executions, including workflow calls.
/// Closing admission and registering a task share one short synchronous lock;
/// no tool future is polled while that lock is held.
#[derive(Debug, Default)]
pub struct RetainedToolTasks {
    closed: Mutex<bool>,
    tasks: TaskTracker,
    failure: Mutex<Option<RuntimeString>>,
}

impl RetainedToolTasks {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn reserve(self: &Arc<Self>) -> Result<RetainedToolTask, ToolError> {
        let closed = self.closed.lock_recover();
        if *closed {
            return Err(ToolError::fatal("Tool execution admission is closed"));
        }
        Ok(RetainedToolTask {
            completion: ToolTaskCompletion {
                owner: self.clone(),
                completed: false,
                _token: self.tasks.token(),
                expected_abort: None,
            },
        })
    }

    pub(crate) fn spawn<F>(
        self: &Arc<Self>,
        future: F,
    ) -> Result<JoinHandle<ToolCallResult>, ToolError>
    where
        F: Future<Output = ToolCallResult> + Send + 'static,
    {
        Ok(self.reserve()?.spawn(future))
    }

    pub(crate) fn spawn_query<F>(
        self: &Arc<Self>,
        future: F,
        expected_abort: Arc<AtomicBool>,
    ) -> Result<JoinHandle<Result<(), ToolError>>, ToolError>
    where
        F: Future<Output = Result<(), ToolError>> + Send + 'static,
    {
        let mut registration = self.reserve()?;
        registration.completion.expected_abort = Some(expected_abort);
        Ok(registration.spawn(future))
    }

    pub(crate) fn record_failure(&self, message: RuntimeString) {
        self.failure.lock_recover().get_or_insert(message);
    }

    /// Prevent any further tool registration before waiting for accepted work.
    pub fn close(&self) {
        let mut closed = self.closed.lock_recover();
        *closed = true;
        self.tasks.close();
    }

    /// Wait after closing admission. Accepted work either completes or records
    /// its failure; dropping a caller's JoinHandle does not abandon the work.
    pub async fn wait(&self) -> Result<(), RuntimeString> {
        self.tasks.wait().await;
        let failure = self.failure.lock_recover();
        match failure.as_ref() {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    pub async fn close_and_wait(&self) -> Result<(), RuntimeString> {
        self.close();
        self.wait().await
    }
}

/// An admitted execution, retained even while its ordered start is persisted.
pub(crate) struct RetainedToolTask {
    completion: ToolTaskCompletion,
}

impl RetainedToolTask {
    pub(crate) fn spawn<F, T>(self, future: F) -> JoinHandle<Result<T, ToolError>>
    where
        F: Future<Output = Result<T, ToolError>> + Send + 'static,
        T: Send + 'static,
    {
        // The token was acquired under the close-admission lock. Move it and
        // the completion guard before spawning so even an unpolled task is owned.
        let RetainedToolTask { mut completion } = self;
        tokio::spawn(async move {
            let result = std::panic::AssertUnwindSafe(future).catch_unwind().await;
            match result {
                Ok(result) => {
                    completion.finish();
                    result
                }
                Err(payload) => {
                    completion
                        .owner
                        .record_failure("Retained execution panicked".into());
                    completion.finish();
                    std::panic::resume_unwind(payload)
                }
            }
        })
    }
}

struct ToolTaskCompletion {
    owner: Arc<RetainedToolTasks>,
    completed: bool,
    _token: TaskTrackerToken,
    expected_abort: Option<Arc<AtomicBool>>,
}

impl ToolTaskCompletion {
    fn finish(&mut self) {
        self.completed = true;
    }
}

impl Drop for ToolTaskCompletion {
    fn drop(&mut self) {
        if !self.completed
            && !self
                .expected_abort
                .as_ref()
                .is_some_and(|expected| expected.load(Ordering::Acquire))
        {
            self.owner
                .record_failure("Retained execution was dropped before completion".into());
        }
    }
}

/// Serializes acquisition of query persistence responsibility with stream drop.
#[derive(Debug, Default)]
pub(super) struct QueryPersistence {
    state: Mutex<QueryPersistenceState>,
    pub(super) expected_abort: Arc<AtomicBool>,
}

#[derive(Debug, Default)]
struct QueryPersistenceState {
    active: usize,
    aborted: bool,
    abort_after_drain: Option<tokio::task::AbortHandle>,
}

impl QueryPersistence {
    pub(super) fn enter(self: &Arc<Self>) -> Result<QueryPersistenceGuard, ToolError> {
        let mut state = self.state.lock_recover();
        if state.aborted {
            return Err(ToolError::fatal("Query ended before persistence admission"));
        }
        state.active += 1;
        Ok(QueryPersistenceGuard {
            owner: self.clone(),
        })
    }

    pub(super) fn abort_if_idle<T>(&self, task: &JoinHandle<T>) {
        let mut state = self.state.lock_recover();
        state.abort_after_drain = Some(task.abort_handle());
        self.abort_drained(&mut state);
    }

    fn abort_drained(&self, state: &mut QueryPersistenceState) {
        if state.active == 0 {
            if let Some(task) = state.abort_after_drain.take() {
                state.aborted = true;
                self.expected_abort.store(true, Ordering::Release);
                task.abort();
            }
        }
    }
}

pub(super) struct QueryPersistenceGuard {
    owner: Arc<QueryPersistence>,
}

impl Drop for QueryPersistenceGuard {
    fn drop(&mut self) {
        let mut state = self.owner.state.lock_recover();
        state.active -= 1;
        self.owner.abort_drained(&mut state);
    }
}

/// Only accepted persistence extends a tool's cancellation grace period.
#[derive(Debug, Default)]
pub(super) struct ToolPersistence {
    state: Mutex<ToolPersistenceState>,
    changed: tokio::sync::Notify,
}

#[derive(Debug, Default)]
struct ToolPersistenceState {
    active: usize,
    draining: bool,
    closed: bool,
}

tokio::task_local! {
    static TOOL_PERSISTENCE: Arc<ToolPersistence>;
}

impl ToolPersistence {
    pub(super) async fn scope<F: Future>(self: &Arc<Self>, future: F) -> F::Output {
        TOOL_PERSISTENCE.scope(self.clone(), future).await
    }

    pub(super) fn enter(self: &Arc<Self>) -> Result<ToolPersistenceGuard, ToolError> {
        self.enter_inner(false)
    }

    fn enter_inner(
        self: &Arc<Self>,
        continuation: bool,
    ) -> Result<ToolPersistenceGuard, ToolError> {
        let mut state = self.state.lock_recover();
        if state.closed || (state.draining && !continuation) {
            return Err(ToolError::new(
                "Tool cancelled before persistence admission",
            ));
        }
        state.active += 1;
        Ok(ToolPersistenceGuard {
            owner: self.clone(),
        })
    }

    pub(super) fn enter_current(terminal: bool) -> Result<Option<ToolPersistenceGuard>, ToolError> {
        match TOOL_PERSISTENCE.try_with(|owner| owner.enter_inner(terminal)) {
            Ok(result) => result.map(Some),
            Err(_) => Ok(None),
        }
    }

    pub(super) async fn drain(&self) {
        loop {
            let notified = self.changed.notified();
            {
                let mut state = self.state.lock_recover();
                state.draining = true;
                if state.active == 0 {
                    state.closed = true;
                    return;
                }
            }
            notified.await;
        }
    }
}

pub(super) struct ToolPersistenceGuard {
    owner: Arc<ToolPersistence>,
}

impl Drop for ToolPersistenceGuard {
    fn drop(&mut self) {
        let mut state = self.owner.state.lock_recover();
        state.active -= 1;
        if state.draining && state.active == 0 {
            self.owner.changed.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn reserved_tool_start_is_included_in_close_before_spawning() {
        let tasks = Arc::new(RetainedToolTasks::new());
        let registration = tasks.reserve().unwrap();
        tasks.close();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), tasks.wait())
                .await
                .is_err()
        );
        let handle =
            registration.spawn(async { Err::<(), _>(ToolError::fatal("reserved tool stopped")) });
        assert!(handle.await.unwrap().is_err());
        tasks.wait().await.unwrap();
    }

    #[tokio::test]
    async fn closed_admission_rejects_late_tasks_and_waits_for_registered_work() {
        let tasks = Arc::new(RetainedToolTasks::new());
        let (release, pending) = tokio::sync::oneshot::channel::<()>();
        let handle = tasks
            .spawn(async move {
                pending.await.expect("test releases accepted work");
                Err(ToolError::fatal("accepted tool failure"))
            })
            .unwrap();
        drop(handle);
        tasks.close();
        assert!(tasks
            .spawn(async { Err(ToolError::fatal("late task ran")) })
            .is_err());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), tasks.wait())
                .await
                .is_err()
        );
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), tasks.wait())
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn an_explicit_persistence_failure_survives_successful_task_drain() {
        let tasks = Arc::new(RetainedToolTasks::new());
        let owner = tasks.clone();
        let task = tasks
            .spawn(async move {
                owner.record_failure("writer closed before commit".into());
                Err(ToolError::fatal("writer closed before commit"))
            })
            .unwrap();
        assert!(task.await.unwrap().is_err());
        assert_eq!(
            tasks.close_and_wait().await.unwrap_err(),
            "writer closed before commit"
        );
    }
}
