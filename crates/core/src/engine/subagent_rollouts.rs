//! Owns one ordered writer per child transcript while its producers are alive.
//!
//! A running agent and each running direct child hold independent leases. A
//! completed agent can therefore keep receiving its children's trace records
//! without keeping an otherwise unused writer open indefinitely.

use crate::runtime_values::RuntimeString;
use crate::store::session_store::LiveRollout;
use crate::sync_ext::MutexExt;
use futures::FutureExt;
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use tokio::runtime::Handle;
use tokio::sync::{oneshot, watch, Mutex as AsyncMutex};
use tokio::task::JoinHandle;

#[derive(Debug, Default)]
pub(crate) struct SubagentRollouts {
    inner: Arc<RolloutOwners>,
}

#[derive(Debug, Default)]
struct RolloutOwners {
    /// Opens and final closes share a gate, including their asynchronous work.
    gate: AsyncMutex<()>,
    state: Mutex<OwnerState>,
}

#[derive(Debug, Default)]
struct OwnerState {
    closing: bool,
    closed: bool,
    writers: HashMap<RuntimeString, WriterEntry>,
    tasks: Vec<JoinHandle<()>>,
    errors: Vec<RuntimeString>,
    shutdown: Option<watch::Receiver<Option<Result<(), RuntimeString>>>>,
}

#[derive(Debug)]
struct WriterEntry {
    writer: Arc<dyn LiveRollout>,
    usage_receipts: Arc<crate::store::receipt_broker::ReceiptBroker>,
    producers: usize,
    /// A failed close cannot prove that opening another writer would be safe.
    close_error: Option<RuntimeString>,
}

/// An explicit producer reservation; ordinary writer clones do not own one.
#[derive(Debug)]
pub(crate) struct SubagentRolloutLease {
    owners: Arc<RolloutOwners>,
    agent_id: RuntimeString,
    writer: Arc<dyn LiveRollout>,
    runtime: Handle,
}

impl SubagentRolloutLease {
    pub(crate) fn writer(&self) -> Arc<dyn LiveRollout> {
        self.writer.clone()
    }
}

impl Drop for SubagentRolloutLease {
    fn drop(&mut self) {
        self.owners.release(&self.agent_id, &self.runtime);
    }
}

impl SubagentRollouts {
    /// Finish an idle writer before unloading its executable runtime. Active
    /// descendant producers retain their independent writer leases.
    pub(crate) async fn flush_for_unload(
        &self,
        agent_id: &RuntimeString,
    ) -> Result<(), RuntimeString> {
        let _gate = self.inner.gate.lock().await;
        let writer = {
            let state = self.inner.state.lock_recover();
            let Some(entry) = state.writers.get(agent_id) else {
                return Ok(());
            };
            if let Some(error) = &entry.close_error {
                return Err(error.clone());
            }
            (entry.writer.clone(), entry.producers)
        };
        writer
            .0
            .flush()
            .await
            .map_err(|error| RuntimeString::from(error.to_string()))?;
        if writer.1 == 0 {
            if let Err(error) = writer.0.shutdown().await {
                let error = close_error(agent_id, &error);
                let mut state = self.inner.state.lock_recover();
                if let Some(entry) = state.writers.get_mut(agent_id) {
                    entry.close_error = Some(error.clone());
                }
                RolloutOwners::record_error(&mut state, error.clone());
                return Err(error);
            }
            self.inner.state.lock_recover().writers.remove(agent_id);
        }
        Ok(())
    }

    /// Retain the opening operation even if the requesting turn is dropped.
    /// The opener must capture owned store metadata rather than a turn borrow.
    pub(crate) async fn acquire<F, Fut>(
        self: &Arc<Self>,
        agent_id: RuntimeString,
        opener: F,
    ) -> Result<SubagentRolloutLease, RuntimeString>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<Arc<dyn LiveRollout>, RuntimeString>> + Send + 'static,
    {
        let (send, receive) = oneshot::channel();
        let runtime = Handle::current();
        {
            let mut state = self.inner.state.lock_recover();
            if state.closing {
                return Err(admission_closed());
            }
            RolloutOwners::reap_finished_tasks(&mut state);
            let owners = self.inner.clone();
            let owner_runtime = runtime.clone();
            state.tasks.push(runtime.spawn(async move {
                let result = owners.acquire_owned(agent_id, opener, owner_runtime).await;
                // A dropped receiver returns the lease here, which releases its
                // reservation through the same tracked final-close path.
                let _ = send.send(result);
            }));
        }
        receive.await.map_err(|_| {
            RuntimeString::from("Sub-agent rollout opening owner stopped unexpectedly.")
        })?
    }

    /// The caller must retain a producer lease throughout every append.
    pub(crate) fn writer(&self, agent_id: &RuntimeString) -> Option<Arc<dyn LiveRollout>> {
        let state = self.inner.state.lock_recover();
        if state.closed {
            return None;
        }
        state.writers.get(agent_id).and_then(|entry| {
            (entry.producers > 0 && entry.close_error.is_none()).then(|| entry.writer.clone())
        })
    }

    pub(crate) fn usage_receipts(
        &self,
        agent_id: &RuntimeString,
    ) -> Option<Arc<crate::store::receipt_broker::ReceiptBroker>> {
        self.inner
            .state
            .lock_recover()
            .writers
            .get(agent_id)
            .map(|entry| entry.usage_receipts.clone())
    }

    /// Close admission, finish retained operations, then close every writer.
    /// Cancellation of a caller does not cancel the shared shutdown owner.
    pub(crate) async fn shutdown(&self) -> Result<(), RuntimeString> {
        let mut completion = {
            let mut state = self.inner.state.lock_recover();
            if let Some(completion) = &state.shutdown {
                completion.clone()
            } else {
                state.closing = true;
                let (send, receive) = watch::channel(None);
                state.shutdown = Some(receive.clone());
                if state.writers.is_empty() && state.tasks.is_empty() {
                    state.closed = true;
                    send.send_replace(Some(RolloutOwners::cleanup_result(&state)));
                } else {
                    let owners = self.inner.clone();
                    tokio::spawn(async move {
                        let result = owners.shutdown_owned().await;
                        let _ = send.send(Some(result));
                    });
                }
                receive
            }
        };
        loop {
            if let Some(result) = completion.borrow().clone() {
                return result;
            }
            completion.changed().await.map_err(|_| {
                RuntimeString::from("Sub-agent rollout shutdown owner stopped unexpectedly.")
            })?;
        }
    }
}

fn admission_closed() -> RuntimeString {
    "Sub-agent rollout admission is closed.".into()
}

fn close_error(agent_id: &RuntimeString, error: &std::io::Error) -> RuntimeString {
    RuntimeString::from("Sub-agent rollout shutdown failed for ")
        .concat(agent_id)
        .concat(&RuntimeString::from(format!(": {error}")))
}

impl RolloutOwners {
    async fn acquire_owned<F, Fut>(
        self: Arc<Self>,
        agent_id: RuntimeString,
        opener: F,
        runtime: Handle,
    ) -> Result<SubagentRolloutLease, RuntimeString>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Arc<dyn LiveRollout>, RuntimeString>>,
    {
        let _gate = self.gate.lock().await;
        {
            let mut state = self.state.lock_recover();
            if state.closing {
                return Err(admission_closed());
            }
            if let Some(entry) = state.writers.get_mut(&agent_id) {
                if let Some(error) = &entry.close_error {
                    return Err(error.clone());
                }
                entry.producers += 1;
                return Ok(SubagentRolloutLease {
                    owners: self.clone(),
                    agent_id,
                    writer: entry.writer.clone(),
                    runtime,
                });
            }
        }

        let raw = opener().await?;
        let usage_receipts = crate::store::receipt_broker::ReceiptBroker::new(raw.clone());
        let ordered = crate::store::session_rollout::SessionRollout::new(raw);
        let writes = usage_receipts.clone();
        ordered.set_write_fence(Arc::new(move || {
            let writes = writes.clone();
            Box::pin(async move { writes.flush_committed().await })
        }));
        let closing = usage_receipts.clone();
        ordered.set_close_fence(Arc::new(move || {
            let closing = closing.clone();
            Box::pin(async move { closing.shutdown().await })
        }));
        let writer: Arc<dyn LiveRollout> = ordered;
        let rejected = {
            let mut state = self.state.lock_recover();
            if state.closing {
                true
            } else {
                state.writers.insert(
                    agent_id.clone(),
                    WriterEntry {
                        writer: writer.clone(),
                        usage_receipts: usage_receipts.clone(),
                        producers: 1,
                        close_error: None,
                    },
                );
                false
            }
        };
        if rejected {
            // Admission may close while the retained opener is awaiting I/O.
            // Finish closing its result before the owner reports completion.
            if let Err(error) = writer.shutdown().await {
                let error = close_error(&agent_id, &error);
                let mut state = self.state.lock_recover();
                Self::record_error(&mut state, error.clone());
                state.writers.insert(
                    agent_id,
                    WriterEntry {
                        writer,
                        usage_receipts,
                        producers: 0,
                        close_error: Some(error),
                    },
                );
            }
            return Err(admission_closed());
        }
        Ok(SubagentRolloutLease {
            owners: self.clone(),
            agent_id,
            writer,
            runtime,
        })
    }

    fn release(self: &Arc<Self>, agent_id: &RuntimeString, runtime: &Handle) {
        let mut state = self.state.lock_recover();
        if state.closed {
            return;
        }
        let Some(entry) = state.writers.get_mut(agent_id) else {
            return;
        };
        debug_assert!(entry.producers > 0, "rollout lease released twice");
        entry.producers -= 1;
        if entry.producers != 0 || state.closing {
            // The shutdown owner drains all remaining writers after its tasks.
            return;
        }
        Self::reap_finished_tasks(&mut state);
        let owners = self.clone();
        let agent_id = agent_id.clone();
        state.tasks.push(runtime.spawn(async move {
            owners.close_unused(agent_id).await;
        }));
    }

    async fn close_unused(&self, agent_id: RuntimeString) {
        let _gate = self.gate.lock().await;
        let entry = {
            let mut state = self.state.lock_recover();
            if state.closed
                || state
                    .writers
                    .get(&agent_id)
                    .is_none_or(|entry| entry.producers != 0 || entry.close_error.is_some())
            {
                return;
            }
            state.writers.remove(&agent_id).expect("unused writer")
        };
        if let Err(error) = entry.writer.shutdown().await {
            let error = close_error(&agent_id, &error);
            let mut state = self.state.lock_recover();
            Self::record_error(&mut state, error.clone());
            state.writers.insert(
                agent_id,
                WriterEntry {
                    close_error: Some(error),
                    ..entry
                },
            );
        }
    }

    async fn drain_tasks(&self) {
        loop {
            let tasks = {
                let mut state = self.state.lock_recover();
                std::mem::take(&mut state.tasks)
            };
            if tasks.is_empty() {
                return;
            }
            for task in tasks {
                if let Err(error) = task.await {
                    Self::record_error(
                        &mut self.state.lock_recover(),
                        RuntimeString::from(format!(
                            "Sub-agent rollout owner stopped unexpectedly: {error}"
                        )),
                    );
                }
            }
        }
    }

    /// Finished handles retain no useful ownership, but their failures must
    /// remain visible to shutdown even during a long sequence of agent runs.
    fn reap_finished_tasks(state: &mut OwnerState) {
        let tasks = std::mem::take(&mut state.tasks);
        for mut task in tasks {
            if !task.is_finished() {
                state.tasks.push(task);
                continue;
            }
            match (&mut task).now_or_never() {
                Some(Ok(())) => {}
                Some(Err(error)) => Self::record_error(
                    state,
                    RuntimeString::from(format!(
                        "Sub-agent rollout owner stopped unexpectedly: {error}"
                    )),
                ),
                None => state.tasks.push(task),
            }
        }
    }

    async fn shutdown_owned(&self) -> Result<(), RuntimeString> {
        self.drain_tasks().await;
        {
            let _gate = self.gate.lock().await;
            let writers = {
                let mut state = self.state.lock_recover();
                state.closed = true;
                std::mem::take(&mut state.writers)
            };
            for (agent_id, entry) in writers {
                if let Err(error) = entry.writer.shutdown().await {
                    Self::record_error(
                        &mut self.state.lock_recover(),
                        close_error(&agent_id, &error),
                    );
                }
            }
        }
        // Releases cannot register work after admission closes. This also
        // collects a cleanup registered immediately before that fence.
        self.drain_tasks().await;
        Self::cleanup_result(&self.state.lock_recover())
    }

    fn cleanup_result(state: &OwnerState) -> Result<(), RuntimeString> {
        if state.errors.is_empty() {
            Ok(())
        } else {
            let mut error = RuntimeString::from("Sub-agent rollout cleanup failed:");
            for failure in &state.errors {
                error = error.concat(&RuntimeString::from("\n")).concat(failure);
            }
            Err(error)
        }
    }

    fn record_error(state: &mut OwnerState, error: RuntimeString) {
        if !state.errors.contains(&error) {
            state.errors.push(error);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::message::Message;
    use crate::schemas::rollout::{TurnContextSnapshot, UsageLedgerItem};
    use crate::schemas::subagent_lifecycle::SubagentLifecycleRecord;
    use crate::store::session_store::{
        AppendEventOptions, CheckpointRecord, CompactionRecord, RollbackRecord,
    };
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::{Notify, Semaphore};

    #[test]
    fn an_empty_registry_closes_without_spawning_a_runtime_task() {
        let registry = SubagentRollouts::default();
        futures::executor::block_on(registry.shutdown()).unwrap();
        let state = registry.inner.state.lock_recover();
        assert!(state.closed && state.closing);
        assert!(state.tasks.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn empty_shutdown_is_ready_even_when_the_callers_deadline_has_expired() {
        let registry = SubagentRollouts::default();
        tokio::time::timeout(std::time::Duration::ZERO, registry.shutdown())
            .await
            .unwrap()
            .unwrap();
    }

    #[derive(Debug, Default)]
    struct TestRollout {
        closes: AtomicUsize,
        finished_closes: AtomicUsize,
        close_started: Notify,
        close_gate: Option<Arc<Semaphore>>,
        fail_close: bool,
        fail_flush: bool,
    }

    #[async_trait]
    impl LiveRollout for TestRollout {
        async fn prepare_batch(
            &self,
            maximum_records: usize,
        ) -> std::io::Result<crate::store::session_store::PreparedRolloutBatch<'_>> {
            crate::testing::immediate_rollout::prepare_immediate_test_rollout_batch(
                self,
                maximum_records,
            )
        }

        async fn append_message(&self, _: Message) {}
        async fn append_turn_context(&self, _: TurnContextSnapshot) {}
        async fn append_compaction(&self, _: CompactionRecord) {}
        async fn append_event(&self, _: RuntimeString, _: Option<AppendEventOptions>) {}
        async fn append_rollback(&self, _: RollbackRecord) {}
        async fn append_checkpoint(&self, _: CheckpointRecord) {}
        async fn append_usage(&self, _: UsageLedgerItem) {}
        async fn append_subagent_lifecycle(&self, _: SubagentLifecycleRecord) {}
        async fn flush(&self) -> std::io::Result<()> {
            if self.fail_flush {
                Err(std::io::Error::other("writer flush failure"))
            } else {
                Ok(())
            }
        }
        async fn shutdown(&self) -> std::io::Result<()> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            self.close_started.notify_one();
            if let Some(gate) = &self.close_gate {
                let permit = gate.acquire().await.expect("close gate");
                permit.forget();
            }
            if self.fail_close {
                Err(std::io::Error::other("writer failure"))
            } else {
                self.finished_closes.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }
    }

    async fn lease(
        registry: &Arc<SubagentRollouts>,
        writer: &Arc<TestRollout>,
    ) -> SubagentRolloutLease {
        let writer = writer.clone();
        registry
            .acquire("parent".into(), move || async move {
                Ok(writer as Arc<dyn LiveRollout>)
            })
            .await
            .expect("writer lease")
    }

    #[tokio::test]
    async fn unload_flush_failure_retains_the_registered_writer() {
        let registry = Arc::new(SubagentRollouts::default());
        let writer = Arc::new(TestRollout {
            fail_flush: true,
            ..Default::default()
        });
        let producer = lease(&registry, &writer).await;
        let error = registry
            .flush_for_unload(&"parent".into())
            .await
            .unwrap_err();
        assert!(error.contains("writer flush failure"));
        assert!(Arc::ptr_eq(
            &registry.writer(&"parent".into()).unwrap(),
            &producer.writer()
        ));
        assert_eq!(writer.closes.load(Ordering::SeqCst), 0);
        drop(producer);
        registry.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn unload_shutdown_failure_retains_its_error_and_prevents_reopening() {
        let registry = Arc::new(SubagentRollouts::default());
        let writer = Arc::new(TestRollout {
            fail_close: true,
            ..Default::default()
        });
        registry.inner.state.lock_recover().writers.insert(
            "parent".into(),
            WriterEntry {
                writer: writer.clone(),
                usage_receipts: crate::store::receipt_broker::ReceiptBroker::new(writer.clone()),
                producers: 0,
                close_error: None,
            },
        );
        let error = registry
            .flush_for_unload(&"parent".into())
            .await
            .unwrap_err();
        assert!(error.contains("writer failure"));
        assert!(registry
            .inner
            .state
            .lock_recover()
            .writers
            .get(&RuntimeString::from("parent"))
            .unwrap()
            .close_error
            .is_some());
        assert!(registry
            .acquire("parent".into(), || async {
                panic!("failed shutdown must not reopen a writer")
            })
            .await
            .is_err());
        assert!(registry
            .shutdown()
            .await
            .unwrap_err()
            .contains("writer failure"));
    }

    #[tokio::test]
    async fn a_child_producer_keeps_its_completed_parents_writer_open() {
        let registry = Arc::new(SubagentRollouts::default());
        let writer = Arc::new(TestRollout::default());
        let parent = lease(&registry, &writer).await;
        let child = registry
            .acquire("parent".into(), || async {
                panic!("existing writer must be reused")
            })
            .await
            .expect("child producer lease");
        assert!(Arc::ptr_eq(&parent.writer(), &child.writer()));
        drop(parent);
        assert!(registry.writer(&"parent".into()).is_some());
        assert_eq!(writer.closes.load(Ordering::SeqCst), 0);
        drop(child);
        writer.close_started.notified().await;
        registry.shutdown().await.expect("shutdown");
        assert_eq!(writer.closes.load(Ordering::SeqCst), 1);
        assert!(registry.writer(&"parent".into()).is_none());
    }

    #[tokio::test]
    async fn a_resumed_producer_waits_for_the_previous_writer_to_close() {
        let registry = Arc::new(SubagentRollouts::default());
        let gate = Arc::new(Semaphore::new(0));
        let previous = Arc::new(TestRollout {
            close_gate: Some(gate.clone()),
            ..Default::default()
        });
        drop(lease(&registry, &previous).await);
        previous.close_started.notified().await;
        let next = Arc::new(TestRollout::default());
        let opens = Arc::new(AtomicUsize::new(0));
        let resumed = {
            let registry = registry.clone();
            let next = next.clone();
            let opens = opens.clone();
            let previous = previous.clone();
            tokio::spawn(async move {
                registry
                    .acquire("parent".into(), move || async move {
                        assert_eq!(previous.finished_closes.load(Ordering::SeqCst), 1);
                        opens.fetch_add(1, Ordering::SeqCst);
                        Ok(next as Arc<dyn LiveRollout>)
                    })
                    .await
            })
        };
        assert_eq!(opens.load(Ordering::SeqCst), 0);
        gate.add_permits(1);
        let resumed = resumed.await.expect("resume task").expect("resume lease");
        assert_eq!(opens.load(Ordering::SeqCst), 1);
        assert_eq!(previous.closes.load(Ordering::SeqCst), 1);
        drop(resumed);
        registry.shutdown().await.expect("shutdown");
        assert_eq!(next.closes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_dropped_acquisition_still_closes_its_eventually_opened_writer() {
        let registry = Arc::new(SubagentRollouts::default());
        let writer = Arc::new(TestRollout::default());
        let opened = Arc::new(Notify::new());
        let open_gate = Arc::new(Semaphore::new(0));
        let acquiring = {
            let registry = registry.clone();
            let writer = writer.clone();
            let opened = opened.clone();
            let open_gate = open_gate.clone();
            tokio::spawn(async move {
                registry
                    .acquire("parent".into(), move || async move {
                        opened.notify_one();
                        let permit = open_gate.acquire().await.expect("open gate");
                        permit.forget();
                        Ok(writer as Arc<dyn LiveRollout>)
                    })
                    .await
            })
        };
        opened.notified().await;
        acquiring.abort();
        assert!(acquiring
            .await
            .expect_err("acquisition cancelled")
            .is_cancelled());
        open_gate.add_permits(1);
        registry.shutdown().await.expect("shutdown");
        assert_eq!(writer.closes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn shutdown_rejects_admission_and_closes_a_still_leased_writer() {
        let registry = Arc::new(SubagentRollouts::default());
        let writer = Arc::new(TestRollout::default());
        let producer = lease(&registry, &writer).await;
        registry.shutdown().await.expect("shutdown");
        assert!(registry
            .acquire("parent".into(), || async {
                panic!("closed admission must not open")
            })
            .await
            .is_err());
        assert!(registry.writer(&"parent".into()).is_none());
        drop(producer);
        registry.shutdown().await.expect("repeated shutdown");
        assert_eq!(writer.closes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn shutdown_survives_a_cancelled_waiter() {
        let registry = Arc::new(SubagentRollouts::default());
        let gate = Arc::new(Semaphore::new(0));
        let writer = Arc::new(TestRollout {
            close_gate: Some(gate.clone()),
            ..Default::default()
        });
        let producer = lease(&registry, &writer).await;
        let closing = {
            let registry = registry.clone();
            tokio::spawn(async move { registry.shutdown().await })
        };
        writer.close_started.notified().await;
        closing.abort();
        assert!(closing.await.expect_err("waiter cancelled").is_cancelled());
        gate.add_permits(1);
        registry.shutdown().await.expect("retained shutdown");
        drop(producer);
        assert_eq!(writer.closes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn failed_cleanup_is_reported_and_does_not_open_a_second_writer() {
        let registry = Arc::new(SubagentRollouts::default());
        let writer = Arc::new(TestRollout {
            fail_close: true,
            ..Default::default()
        });
        drop(lease(&registry, &writer).await);
        writer.close_started.notified().await;
        let next = registry
            .acquire("parent".into(), || async {
                panic!("failed close must block reopening")
            })
            .await;
        assert!(next.is_err());
        let error = registry.shutdown().await.expect_err("cleanup failure");
        assert!(error.contains("writer failure"));
    }
}
