//! Test-only pause gates and capture sinks for the protocol endpoint.
//! Production code holds `Option<Arc<Gate>>` slots (always `None` outside
//! tests) and awaits `pause()` at labeled boundaries; tests install a gate,
//! observe entry, then release. Extracted from server.rs verbatim.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use klaus_agent_core::memory::maintenance::MemoryMaintenanceEventSink;
use tokio::sync::Notify;

pub(super) struct SnapshotWriteTestGate {
    entered: tokio::sync::watch::Sender<
        Option<klaus_agent_core::memory::snapshot_decision::AgentMemorySnapshotDecisionAudit>,
    >,
    release: Notify,
    armed: AtomicBool,
}

impl SnapshotWriteTestGate {
    pub(super) fn new() -> Self {
        let (entered, _) = tokio::sync::watch::channel(None);
        Self {
            entered,
            release: Notify::new(),
            armed: AtomicBool::new(true),
        }
    }

    pub(super) fn subscribe(
        &self,
    ) -> tokio::sync::watch::Receiver<
        Option<klaus_agent_core::memory::snapshot_decision::AgentMemorySnapshotDecisionAudit>,
    > {
        self.entered.subscribe()
    }

    pub(super) async fn pause_requested(
        &self,
        audit: Option<
            klaus_agent_core::memory::snapshot_decision::AgentMemorySnapshotDecisionAudit,
        >,
    ) {
        let Some(audit) = audit else {
            return;
        };
        if audit.status
            != klaus_agent_core::memory::snapshot_decision::AgentMemorySnapshotDecisionStatus::Requested
            || self
                .armed
                .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
        {
            return;
        }
        self.entered.send_replace(Some(audit));
        self.release.notified().await;
    }

    pub(super) fn release(&self) {
        self.release.notify_one();
    }
}

pub(super) struct TodoQueryTestGate {
    entered: tokio::sync::watch::Sender<bool>,
    release: Notify,
    armed: AtomicBool,
}

pub(super) struct TodoCallbackTestGate {
    pub(super) entered: Notify,
    release: (StdMutex<bool>, std::sync::Condvar),
    armed: AtomicBool,
}

pub(super) struct SessionPublicationCallbackTestGate {
    pub(super) entered: Notify,
    release: (StdMutex<bool>, std::sync::Condvar),
    armed: AtomicBool,
}

pub(super) struct SessionPublicationCallbackReleaseOnDrop(
    pub(super) Arc<SessionPublicationCallbackTestGate>,
);

impl Drop for SessionPublicationCallbackReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

pub(super) struct SessionPublicationCommitTestGate {
    entered: tokio::sync::watch::Sender<bool>,
    release: Notify,
    armed: AtomicBool,
}

pub(super) struct PendingPromptRegistrationTestGate {
    entered: tokio::sync::watch::Sender<bool>,
    release: Notify,
    armed: AtomicBool,
}

pub(super) struct StopAdmissionTestGate {
    entered: tokio::sync::watch::Sender<bool>,
    release: (StdMutex<bool>, std::sync::Condvar),
    armed: AtomicBool,
}

pub(super) struct StopAdmissionReleaseOnDrop(pub(super) Arc<StopAdmissionTestGate>);

impl Drop for StopAdmissionReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

impl PendingPromptRegistrationTestGate {
    pub(super) fn new() -> Self {
        let (entered, _) = tokio::sync::watch::channel(false);
        Self {
            entered,
            release: Notify::new(),
            armed: AtomicBool::new(true),
        }
    }

    pub(super) fn subscribe(&self) -> tokio::sync::watch::Receiver<bool> {
        self.entered.subscribe()
    }

    pub(super) async fn pause(&self) {
        if self
            .armed
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        self.entered.send_replace(true);
        self.release.notified().await;
    }

    pub(super) fn release(&self) {
        self.release.notify_one();
    }
}

impl StopAdmissionTestGate {
    pub(super) fn new() -> Self {
        let (entered, _) = tokio::sync::watch::channel(false);
        Self {
            entered,
            release: (StdMutex::new(false), std::sync::Condvar::new()),
            armed: AtomicBool::new(true),
        }
    }

    pub(super) fn subscribe(&self) -> tokio::sync::watch::Receiver<bool> {
        self.entered.subscribe()
    }

    pub(super) fn pause(&self) {
        if self
            .armed
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        self.entered.send_replace(true);
        let (released, ready) = &self.release;
        let mut released = released.lock().unwrap();
        while !*released {
            released = ready.wait(released).unwrap();
        }
    }

    pub(super) fn release(&self) {
        let (released, ready) = &self.release;
        *released.lock().unwrap() = true;
        ready.notify_all();
    }
}

impl SessionPublicationCommitTestGate {
    pub(super) fn new() -> Self {
        let (entered, _) = tokio::sync::watch::channel(false);
        Self {
            entered,
            release: Notify::new(),
            armed: AtomicBool::new(true),
        }
    }

    pub(super) fn subscribe(&self) -> tokio::sync::watch::Receiver<bool> {
        self.entered.subscribe()
    }

    pub(super) async fn pause(&self) {
        if self
            .armed
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        self.entered.send_replace(true);
        self.release.notified().await;
    }

    pub(super) fn release(&self) {
        self.release.notify_one();
    }
}

#[derive(Default)]
pub(super) struct SessionPublicationTestSinks {
    pub(super) memory: Option<MemoryMaintenanceEventSink>,
    pub(super) snapshot: Option<
        Arc<
            dyn Fn(klaus_agent_core::memory::snapshot_decision::AgentMemorySnapshotDecisionAudit)
                + Send
                + Sync,
        >,
    >,
    pub(super) queue: Option<klaus_agent_core::engine::message_queue::QueueStateSubscriber>,
    pub(super) task: Option<klaus_agent_core::tasks::task_store::TaskListChangeEnvelopeSubscriber>,
}

impl SessionPublicationCallbackTestGate {
    pub(super) fn new() -> Self {
        Self {
            entered: Notify::new(),
            release: (StdMutex::new(false), std::sync::Condvar::new()),
            armed: AtomicBool::new(true),
        }
    }

    pub(super) fn pause(&self) {
        if self
            .armed
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        self.entered.notify_one();
        let (released, ready) = &self.release;
        let mut released = released.lock().unwrap();
        while !*released {
            released = ready.wait(released).unwrap();
        }
    }

    pub(super) fn release(&self) {
        let (released, ready) = &self.release;
        *released.lock().unwrap() = true;
        ready.notify_all();
    }
}

impl TodoCallbackTestGate {
    pub(super) fn new() -> Self {
        Self {
            entered: Notify::new(),
            release: (StdMutex::new(false), std::sync::Condvar::new()),
            armed: AtomicBool::new(true),
        }
    }

    pub(super) fn pause(&self) {
        if self
            .armed
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        self.entered.notify_one();
        let (released, ready) = &self.release;
        let mut released = released.lock().unwrap();
        while !*released {
            released = ready.wait(released).unwrap();
        }
    }

    pub(super) fn release(&self) {
        let (released, ready) = &self.release;
        *released.lock().unwrap() = true;
        ready.notify_all();
    }
}

impl TodoQueryTestGate {
    pub(super) fn new() -> Self {
        let (entered, _) = tokio::sync::watch::channel(false);
        Self {
            entered,
            release: Notify::new(),
            armed: AtomicBool::new(true),
        }
    }

    pub(super) fn subscribe(&self) -> tokio::sync::watch::Receiver<bool> {
        self.entered.subscribe()
    }

    pub(super) async fn pause(&self) {
        if self
            .armed
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        self.entered.send_replace(true);
        self.release.notified().await;
    }

    pub(super) fn release(&self) {
        self.release.notify_one();
    }
}
