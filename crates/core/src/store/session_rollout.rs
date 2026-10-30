//! Session-owned admission prefixes and their cancellation-independent receipts.

#[cfg(test)]
#[path = "session_rollout_tests.rs"]
mod tests;

use super::session_store::{
    AppendEventOptions, CheckpointRecord, CompactionRecord, LiveRollout, PreparedRolloutBatch,
    RollbackRecord,
};
use crate::runtime_values::{RuntimeJson, RuntimeString};
use crate::schemas::background_process::BackgroundProcessRecord;
use crate::schemas::message::Message;
use crate::schemas::rollout::{RolloutItem, TurnContextSnapshot, UsageLedgerItem};
use crate::schemas::subagent_lifecycle::SubagentLifecycleRecord;
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::FutureExt;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};
use tokio::sync::{watch, Notify};

type Receipt = watch::Receiver<Option<Result<(), String>>>;
type WriteFence = Arc<dyn Fn() -> BoxFuture<'static, io::Result<()>> + Send + Sync>;

#[derive(Default)]
struct Generation {
    outstanding: AtomicUsize,
    changed: Notify,
}

struct GenerationLease(Arc<Generation>);

impl Drop for GenerationLease {
    fn drop(&mut self) {
        self.0.outstanding.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_waiters();
    }
}

impl Generation {
    async fn drained(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.outstanding.load(Ordering::Acquire) == 0 {
                return;
            }
            changed.await;
        }
    }
}

struct State {
    generation: Arc<Generation>,
    flushes: Arc<Generation>,
    tail: Receipt,
    closing: bool,
    failure: Option<String>,
    shutdown: Option<Receipt>,
}

/// Ordinary writes retain a generation lease through synchronous prepared
/// commit. A newly registered prefix fences that generation before writing.
/// This prevents a write waiting for capacity from being overtaken by a turn
/// registered later, without buffering ordinary records in another queue.
pub(crate) struct SessionRollout {
    raw: Arc<dyn LiveRollout>,
    state: Arc<Mutex<State>>,
    fence: OnceLock<WriteFence>,
    close_fence: OnceLock<WriteFence>,
}

impl std::fmt::Debug for SessionRollout {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SessionRollout")
            .finish_non_exhaustive()
    }
}

impl SessionRollout {
    pub(crate) fn new(raw: Arc<dyn LiveRollout>) -> Arc<Self> {
        let (_, tail) = watch::channel(Some(Ok(())));
        Arc::new(Self {
            raw,
            state: Arc::new(Mutex::new(State {
                generation: Arc::new(Generation::default()),
                flushes: Arc::new(Generation::default()),
                tail,
                closing: false,
                failure: None,
                shutdown: None,
            })),
            fence: OnceLock::new(),
            close_fence: OnceLock::new(),
        })
    }

    pub(crate) fn set_write_fence(&self, fence: WriteFence) {
        let _ = self.fence.set(fence);
    }

    pub(crate) fn set_close_fence(&self, fence: WriteFence) {
        let _ = self.close_fence.set(fence);
    }

    fn admission(&self) -> io::Result<(Receipt, GenerationLease)> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(error) = &state.failure {
            return Err(io::Error::other(error.clone()));
        }
        if state.closing {
            return Err(io::Error::other("session rollout admission is closed"));
        }
        let generation = state.generation.clone();
        generation.outstanding.fetch_add(1, Ordering::AcqRel);
        Ok((state.tail.clone(), GenerationLease(generation)))
    }

    fn flush_admission(&self) -> io::Result<(Receipt, GenerationLease)> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.closing || state.failure.is_some() {
            return Err(io::Error::other(
                state
                    .failure
                    .clone()
                    .unwrap_or_else(|| "session rollout admission is closed".into()),
            ));
        }
        state.flushes.outstanding.fetch_add(1, Ordering::AcqRel);
        Ok((state.tail.clone(), GenerationLease(state.flushes.clone())))
    }

    async fn fence(&self) -> io::Result<()> {
        if let Some(fence) = self.fence.get() {
            fence().await?;
        }
        Ok(())
    }

    /// Register before exposing the operation, but start only after releasing
    /// synchronous state locks. Dropping an unstarted owner fails its receipt.
    pub(crate) fn register_prefix(
        self: &Arc<Self>,
        records: Vec<RolloutItem>,
    ) -> io::Result<PrefixOwner> {
        let (send, receive) = watch::channel(None);
        let (previous, generation) = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.closing || state.failure.is_some() {
                return Err(io::Error::other(
                    state
                        .failure
                        .clone()
                        .unwrap_or_else(|| "session rollout admission is closed".into()),
                ));
            }
            let previous = state.tail.clone();
            let generation =
                std::mem::replace(&mut state.generation, Arc::new(Generation::default()));
            state.tail = receive;
            (previous, generation)
        };
        let completion = PrefixCompletion {
            send,
            state: self.state.clone(),
            completed: false,
        };
        let rollout = self.clone();
        let work = async move {
            let mut completion = completion;
            let result = std::panic::AssertUnwindSafe(async {
                await_receipt(previous).await?;
                generation.drained().await;
                rollout.fence().await?;
                for record in records {
                    let prepared = rollout.raw.prepare_append().await?;
                    prepared.commit(record);
                }
                Ok(())
            })
            .catch_unwind()
            .await
            .unwrap_or_else(|_| Err(io::Error::other("session rollout prefix panicked")));
            completion.complete(result);
        };
        Ok(PrefixOwner {
            work: Some(Box::pin(work)),
        })
    }

    pub(crate) fn completed_prefix_result(&self) -> Option<io::Result<()>> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let result = state.tail.borrow().clone();
        result.map(|result| result.map_err(io::Error::other))
    }

    pub(crate) async fn wait_prefixes(&self) -> io::Result<()> {
        let tail = self
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .tail
            .clone();
        await_receipt(tail).await
    }
}

pub(crate) struct PrefixOwner {
    work: Option<BoxFuture<'static, ()>>,
}

impl PrefixOwner {
    /// Poll the same owned future once to preserve immediate backends. A
    /// pending operation is retained independently of its caller's stream.
    pub(crate) fn start(mut self) -> io::Result<()> {
        let mut work = self.work.take().expect("prefix owner starts once");
        let mut context = Context::from_waker(futures::task::noop_waker_ref());
        if let Poll::Ready(()) = work.as_mut().poll(&mut context) {
            return Ok(());
        }
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| {
            io::Error::other("pending session rollout admission requires a Tokio runtime")
        })?;
        runtime.spawn(work);
        Ok(())
    }
}

struct PrefixCompletion {
    send: watch::Sender<Option<Result<(), String>>>,
    state: Arc<Mutex<State>>,
    completed: bool,
}

impl PrefixCompletion {
    fn complete(&mut self, result: io::Result<()>) {
        let result = result.map_err(|error| error.to_string());
        if let Err(error) = &result {
            self.state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .failure
                .get_or_insert_with(|| error.clone());
        }
        self.send.send_replace(Some(result));
        self.completed = true;
    }
}

impl Drop for PrefixCompletion {
    fn drop(&mut self) {
        if !self.completed {
            self.complete(Err(io::Error::other(
                "session rollout prefix owner ended before completion",
            )));
        }
    }
}

async fn await_receipt(mut receive: Receipt) -> io::Result<()> {
    loop {
        if let Some(result) = receive.borrow().clone() {
            return result.map_err(io::Error::other);
        }
        receive
            .changed()
            .await
            .map_err(|_| io::Error::other("session rollout prefix receipt closed"))?;
    }
}

#[async_trait]
impl LiveRollout for SessionRollout {
    async fn prepare_batch(&self, maximum_records: usize) -> io::Result<PreparedRolloutBatch<'_>> {
        let (tail, lease) = self.admission()?;
        await_receipt(tail).await?;
        self.fence().await?;
        let prepared = self.raw.prepare_batch(maximum_records).await?;
        Ok(PreparedRolloutBatch::new(move |items| {
            prepared.commit(items);
            drop(lease);
        }))
    }

    async fn append_message(&self, message: Message) {
        if let Ok((tail, _lease)) = self.admission() {
            if await_receipt(tail).await.is_ok() && self.fence().await.is_ok() {
                self.raw.append_message(message).await;
            }
        }
    }
    async fn append_turn_context(&self, ctx: TurnContextSnapshot) {
        if let Ok((tail, _lease)) = self.admission() {
            if await_receipt(tail).await.is_ok() && self.fence().await.is_ok() {
                self.raw.append_turn_context(ctx).await;
            }
        }
    }
    async fn append_compaction(&self, record: CompactionRecord) {
        if let Ok((tail, _lease)) = self.admission() {
            if await_receipt(tail).await.is_ok() && self.fence().await.is_ok() {
                self.raw.append_compaction(record).await;
            }
        }
    }
    async fn append_event(&self, event: RuntimeString, options: Option<AppendEventOptions>) {
        if let Ok((tail, _lease)) = self.admission() {
            if await_receipt(tail).await.is_ok() && self.fence().await.is_ok() {
                self.raw.append_event(event, options).await;
            }
        }
    }
    async fn append_rollback(&self, record: RollbackRecord) {
        if let Ok((tail, _lease)) = self.admission() {
            if await_receipt(tail).await.is_ok() && self.fence().await.is_ok() {
                self.raw.append_rollback(record).await;
            }
        }
    }
    async fn append_checkpoint(&self, record: CheckpointRecord) {
        if let Ok((tail, _lease)) = self.admission() {
            if await_receipt(tail).await.is_ok() && self.fence().await.is_ok() {
                self.raw.append_checkpoint(record).await;
            }
        }
    }
    async fn append_usage(&self, record: UsageLedgerItem) {
        if let Ok((tail, _lease)) = self.admission() {
            if await_receipt(tail).await.is_ok() && self.fence().await.is_ok() {
                self.raw.append_usage(record).await;
            }
        }
    }
    async fn append_subagent_lifecycle(&self, record: SubagentLifecycleRecord) {
        if let Ok((tail, _lease)) = self.admission() {
            if await_receipt(tail).await.is_ok() && self.fence().await.is_ok() {
                self.raw.append_subagent_lifecycle(record).await;
            }
        }
    }
    async fn append_background_process_lifecycle(&self, record: BackgroundProcessRecord) {
        if let Ok((tail, _lease)) = self.admission() {
            if await_receipt(tail).await.is_ok() && self.fence().await.is_ok() {
                self.raw.append_background_process_lifecycle(record).await;
            }
        }
    }
    async fn update_title(&self, title: &str) -> io::Result<()> {
        let (tail, _lease) = self.admission()?;
        await_receipt(tail).await?;
        self.fence().await?;
        self.raw.update_title(title).await
    }
    async fn update_host_metadata(&self, metadata: Option<RuntimeJson>) -> io::Result<()> {
        let (tail, _lease) = self.admission()?;
        await_receipt(tail).await?;
        self.fence().await?;
        self.raw.update_host_metadata(metadata).await
    }
    fn inspection_revision(&self) -> Option<u64> {
        self.raw.inspection_revision()
    }
    async fn flush(&self) -> io::Result<()> {
        // A receipt can wait for older I/O while a newer root is admitted.
        // Keep it in the close fence, without delaying that root's prefix.
        let (tail, _lease) = self.flush_admission()?;
        await_receipt(tail).await?;
        self.fence().await?;
        self.raw.flush().await
    }
    async fn shutdown(&self) -> io::Result<()> {
        let (receive, owner) = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if let Some(receive) = &state.shutdown {
                (receive.clone(), None)
            } else {
                state.closing = true;
                let tail = state.tail.clone();
                let generation = state.generation.clone();
                let flushes = state.flushes.clone();
                let (send, receive) = watch::channel(None);
                state.shutdown = Some(receive.clone());
                let raw = self.raw.clone();
                let fence = self.fence.get().cloned();
                let close_fence = self.close_fence.get().cloned();
                let completion = PrefixCompletion {
                    send,
                    state: self.state.clone(),
                    completed: false,
                };
                let work = async move {
                    let mut completion = completion;
                    let result = std::panic::AssertUnwindSafe(async {
                        let prefix_result = await_receipt(tail).await;
                        generation.drained().await;
                        flushes.drained().await;
                        let producers_result = match close_fence {
                            Some(fence) => fence().await,
                            None => Ok(()),
                        };
                        let fence_result = match fence {
                            Some(fence) => fence().await,
                            None => Ok(()),
                        };
                        // An earlier producer error is reported, but cannot
                        // abandon the raw writer's drain and lease release.
                        let close_result = raw.shutdown().await;
                        prefix_result
                            .and(producers_result)
                            .and(fence_result)
                            .and(close_result)
                    })
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|_| Err(io::Error::other("session rollout shutdown panicked")));
                    completion.complete(result);
                };
                (
                    receive,
                    Some(PrefixOwner {
                        work: Some(Box::pin(work)),
                    }),
                )
            }
        };
        if let Some(owner) = owner {
            owner.start()?;
        }
        await_receipt(receive).await
    }
}
