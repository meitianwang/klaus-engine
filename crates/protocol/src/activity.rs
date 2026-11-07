//! Endpoint activity admission/tracking: every dispatch, readonly helper,
//! completion, and durable-lifecycle task registers here so protocol shutdown
//! can drain, force-abort, and settle them deterministically.

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};

use tokio::sync::Notify;

use crate::server::ProtocolShutdownFailure;

#[derive(Default)]
struct ActivityState {
    admission_closed: bool,
    force_aborting: bool,
    settled: bool,
    next_id: u64,
    active: HashMap<u64, ActivityAbortState>,
}

enum ActivityAbortState {
    /// The task has been admitted but its spawning thread has not installed the
    /// Tokio abort handle yet.
    Pending,
    Attached(tokio::task::AbortHandle),
    /// Ownership was deliberately handed to another retained task. A late
    /// `attach` from the spawning thread must not revoke that handoff.
    Detached,
}

/// Retained admission/idle state for work that must settle before endpoint
/// teardown can complete.  Registration and admission close share one mutex so
/// shutdown cannot observe zero and then miss a newly admitted operation.
#[derive(Default)]
pub(crate) struct ActivityTracker {
    state: StdMutex<ActivityState>,
    idle: Notify,
}

impl ActivityTracker {
    pub(crate) fn begin(self: &Arc<Self>) -> Option<ActivityGuard> {
        let mut state = self.state.lock().unwrap();
        if state.admission_closed || state.force_aborting || state.settled {
            return None;
        }
        let id = state.next_id;
        state.next_id = state
            .next_id
            .checked_add(1)
            .expect("protocol activity id overflow");
        state.active.insert(id, ActivityAbortState::Pending);
        Some(ActivityGuard {
            tracker: self.clone(),
            id,
        })
    }

    /// Retain cleanup spawned by work that was already admitted.  The parent
    /// dispatch/chain/readonly guard is what orders this increment before
    /// shutdown's idle snapshot, so child cleanup remains registerable after
    /// new top-level work has been closed.
    pub(crate) fn retain_child(self: &Arc<Self>) -> ActivityGuard {
        let mut state = self.state.lock().unwrap();
        let id = state.next_id;
        state.next_id = state
            .next_id
            .checked_add(1)
            .expect("protocol activity id overflow");
        state.active.insert(id, ActivityAbortState::Pending);
        ActivityGuard {
            tracker: self.clone(),
            id,
        }
    }

    pub(crate) fn attach(&self, id: u64, handle: tokio::task::AbortHandle) {
        let mut state = self.state.lock().unwrap();
        let force_aborting = state.force_aborting || state.settled;
        if let Some(slot) = state.active.get_mut(&id) {
            match slot {
                // Handoff is authoritative even if force-close won before this
                // late spawning-thread attach. The writer-owned publication
                // parent must remain alive until its receipt transaction ends.
                ActivityAbortState::Detached => {}
                ActivityAbortState::Pending if force_aborting => handle.abort(),
                ActivityAbortState::Pending => {
                    *slot = ActivityAbortState::Attached(handle);
                }
                // A task has one authoritative handle; a duplicate attach may
                // neither replace nor abort it.
                ActivityAbortState::Attached(_) => {}
            }
        }
    }

    pub(crate) fn detach_abort_handle(&self, id: u64) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.force_aborting || state.settled {
            return false;
        }
        let Some(slot) = state.active.get_mut(&id) else {
            return false;
        };
        *slot = ActivityAbortState::Detached;
        true
    }

    pub(crate) fn abort_all(&self) -> usize {
        let handles = {
            let mut state = self.state.lock().unwrap();
            state.force_aborting = true;
            state
                .active
                .values()
                .filter_map(|slot| match slot {
                    ActivityAbortState::Attached(handle) => Some(handle.clone()),
                    ActivityAbortState::Pending | ActivityAbortState::Detached => None,
                })
                .collect::<Vec<_>>()
        };
        for handle in &handles {
            handle.abort();
        }
        handles.len()
    }

    pub(crate) fn close_admission(&self) {
        let mut state = self.state.lock().unwrap();
        state.admission_closed = true;
        if state.active.is_empty() {
            self.idle.notify_waiters();
        }
    }

    /// Whether shutdown already closed top-level admission on this tracker.
    #[cfg(test)]
    pub(crate) fn admission_is_closed(&self) -> bool {
        self.state.lock().unwrap().admission_closed
    }

    /// Whether any admitted task is still registered.
    #[cfg(test)]
    pub(crate) fn has_active(&self) -> bool {
        !self.state.lock().unwrap().active.is_empty()
    }

    pub(crate) async fn wait_for_idle(&self) {
        loop {
            let notified = self.idle.notified();
            if self.state.lock().unwrap().active.is_empty() {
                return;
            }
            notified.await;
        }
    }

    pub(crate) async fn shutdown_wait(
        &self,
        grace_deadline: tokio::time::Instant,
        force_deadline: tokio::time::Instant,
        label: &str,
    ) -> Result<(), ProtocolShutdownFailure> {
        if tokio::time::timeout_at(grace_deadline, self.wait_for_idle())
            .await
            .is_ok()
        {
            self.state.lock().unwrap().settled = true;
            return Ok(());
        }
        self.abort_all();
        let force_timed_out = tokio::time::timeout_at(force_deadline, self.wait_for_idle())
            .await
            .is_err();
        self.state.lock().unwrap().settled = true;
        if force_timed_out {
            Err(ProtocolShutdownFailure::timeout(format!(
                "{label} timed out during protocol shutdown and did not stop after force-abort"
            )))
        } else {
            Err(ProtocolShutdownFailure::timeout(format!(
                "{label} timed out during protocol shutdown"
            )))
        }
    }

    /// Wait for a transaction that must retain ownership after its request
    /// future is cancelled. These tasks deliberately publish no abort handle:
    /// once a durable lifecycle intent exists, force-aborting would strand a
    /// half-applied filesystem transaction. A timeout is reported to shutdown,
    /// while the retained task continues to own recovery to completion.
    pub(crate) async fn shutdown_wait_unabortable(
        &self,
        deadline: tokio::time::Instant,
        label: &str,
    ) -> Result<(), ProtocolShutdownFailure> {
        if tokio::time::timeout_at(deadline, self.wait_for_idle())
            .await
            .is_ok()
        {
            self.state.lock().unwrap().settled = true;
            Ok(())
        } else {
            Err(ProtocolShutdownFailure::timeout(format!(
                "{label} timed out during protocol shutdown; retained recovery continues"
            )))
        }
    }
}

pub(crate) struct ActivityGuard {
    tracker: Arc<ActivityTracker>,
    id: u64,
}

/// One retained task's authoritative abort handle plus the force-abort latch
/// that outlives a late publication.
#[derive(Default)]
pub(crate) struct AbortSlot {
    force_aborting: bool,
    handle: Option<tokio::task::AbortHandle>,
}

impl AbortSlot {
    pub(crate) fn publish(&mut self, handle: tokio::task::AbortHandle) {
        if self.force_aborting {
            handle.abort();
        }
        self.handle = Some(handle);
    }

    pub(crate) fn force_abort(&mut self) {
        self.force_aborting = true;
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }

    pub(crate) fn clear(&mut self) {
        self.handle = None;
    }
}

impl ActivityGuard {
    pub(crate) fn id(&self) -> u64 {
        self.id
    }
}

impl Drop for ActivityGuard {
    fn drop(&mut self) {
        let mut state = self.tracker.state.lock().unwrap();
        state.active.remove(&self.id);
        if state.active.is_empty() {
            self.tracker.idle.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn top_level_activity_rejects_settled_and_force_aborting_trackers() {
        let settled = Arc::new(ActivityTracker::default());
        let now = tokio::time::Instant::now();
        settled
            .shutdown_wait(
                now + Duration::from_secs(1),
                now + Duration::from_secs(2),
                "settled tracker",
            )
            .await
            .expect("empty tracker settles");
        assert!(
            settled.begin().is_none(),
            "settled tracker cannot admit top-level work"
        );

        let force_aborting = Arc::new(ActivityTracker::default());
        assert_eq!(force_aborting.abort_all(), 0);
        assert!(
            force_aborting.begin().is_none(),
            "force-aborting tracker cannot admit top-level work"
        );
    }

    #[tokio::test]
    async fn detached_activity_handoff_cannot_be_overwritten_by_late_attach() {
        let tracker = Arc::new(ActivityTracker::default());
        let guard = tracker.retain_child();
        let id = guard.id();
        assert!(tracker.detach_abort_handle(id));
        assert_eq!(tracker.abort_all(), 0);

        let task = tokio::spawn(async { futures::future::pending::<()>().await });
        tracker.attach(id, task.abort_handle());
        assert!(matches!(
            tracker.state.lock().unwrap().active.get(&id),
            Some(ActivityAbortState::Detached)
        ));
        tokio::task::yield_now().await;
        assert!(
            !task.is_finished(),
            "force-before-late-attach cannot abort a handed-off publication"
        );

        task.abort();
        let _ = task.await;
        drop(guard);
    }
}
