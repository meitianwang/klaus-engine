//! Mid-turn steering queue: the host enqueues a message while a turn is running;
//! the query loop drains it at the tool-continuation boundary and injects it into
//! the SAME turn, so the model sees the new input without waiting for the turn to
//! end. Priority is `now` > `next` > `later`, FIFO within a priority.
//!
//! Per-session (not process-global) so steering never leaks across concurrent
//! sessions: a session's main loop and its in-process sub-agents share the one
//! instance and stay isolated via the `agent_id` filter at the drain site. Methods
//! take `&self` (internal locking) so the instance can be shared behind an `Arc`.

use crate::runtime_values::{RuntimeCodecError, RuntimeCodecErrorKind, RuntimeString};
use crate::schemas::content::{ContentBlock, ImageBlock, OutputBlock, TextBlock};
use crate::sync_ext::MutexExt;
use futures::future::BoxFuture;
use futures::FutureExt;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

mod wire;

pub(crate) use wire::QueuePersistenceState;
use wire::{
    PersistedQueueItem, PersistedQueueMutation, PersistedQueueTombstone, QueueMutationRequest,
};
pub use wire::{
    QueueEdit, QueueItemState, QueueMutationResult, QueueMutationStatus, QueuePriority,
    QueueSnapshot, QueueSnapshotItem, QueueSnapshotStatus, QueueTerminalState, QueuedMessage,
    QueuedValue, SteerDelivery,
};

type Subscriber = Arc<dyn Fn() + Send + Sync>;
pub type QueueStateSubscriber = Arc<dyn Fn(QueueSnapshot) + Send + Sync>;
type QueuePersistenceFuture = BoxFuture<'static, Result<(), RuntimeString>>;
type QueuePersistenceSink =
    Arc<dyn Fn(QueuePersistenceState) -> QueuePersistenceFuture + Send + Sync>;

pub(crate) const QUEUE_STATE_ROLLOUT_EVENT: &str = "queue_state";
const QUEUE_PERSISTENCE_SCHEMA_VERSION: u64 = 1;

struct Inner {
    /// Items carry stable identity via `Arc` so [`MessageQueue::remove`] can match
    /// by reference, mirroring the object-identity contract of the drain snapshot.
    items: Vec<Arc<QueuedMessage>>,
    revision: u64,
    reserved: HashSet<RuntimeString>,
    resume_reserved: HashSet<RuntimeString>,
    /// Public readers must never observe a mutation whose durability receipt
    /// failed. This snapshot and its identity-preserving item set advance only
    /// when the corresponding revision enters the public dispatcher.
    last_authoritative_snapshot: QueueSnapshot,
    last_authoritative_items: Vec<Arc<QueuedMessage>>,
    tombstones: HashMap<RuntimeString, QueueTerminalState>,
    mutations: HashMap<RuntimeString, (QueueMutationRequest, QueueMutationResult)>,
    session_id: Option<RuntimeString>,
    persistence_sink: Option<QueuePersistenceSink>,
    /// Final lifecycle admission gate. Shutdown closes this under the same
    /// lock that allocates persistence sequences, so every earlier producer is
    /// inside the drain barrier and every later producer fails before mutation.
    admission_closed: bool,
    /// A failed durability receipt poisons further Host mutations. Their CAS
    /// bodies must not run after the queue can no longer prove its checkpoints.
    persistence_failure: Option<RuntimeString>,
    persistence_sequence: u64,
    subscribers: Vec<(u64, Subscriber)>,
    state_subscribers: Vec<(u64, QueueStateSubscriber)>,
    next_sub_id: u64,
}

/// Per-session steering queue.
pub struct MessageQueue {
    inner: Arc<Mutex<Inner>>,
    /// Host mutations are serialized through durability so correlated results
    /// cannot overtake the checkpoint sequence that makes them authoritative.
    mutation_gate: Arc<tokio::sync::Mutex<()>>,
    dispatch: Arc<Mutex<DispatchState>>,
    persistence_dispatch: Arc<Mutex<PersistenceDispatchState>>,
    durability_dispatch: Arc<Mutex<DurabilityDispatchState>>,
    durability_notify: Arc<tokio::sync::Notify>,
    retained_persistence_tasks: Arc<Mutex<Vec<RetainedPersistenceTask>>>,
}

struct MailboxReservation<'a> {
    queue: &'a MessageQueue,
    items: Vec<Arc<QueuedMessage>>,
    committed: bool,
}

impl Drop for MailboxReservation<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.queue.release_pending_resume(&self.items);
        }
    }
}

impl MessageQueue {
    pub fn new() -> Self {
        Self::empty(None)
    }

    pub(crate) fn for_session(session_id: RuntimeString) -> Self {
        Self::empty(Some(session_id))
    }

    fn empty(session_id: Option<RuntimeString>) -> Self {
        Self::from_inner(Inner {
            items: Vec::new(),
            revision: 0,
            reserved: HashSet::new(),
            resume_reserved: HashSet::new(),
            last_authoritative_snapshot: QueueSnapshot {
                revision: 0,
                items: Vec::new(),
                status: QueueSnapshotStatus::Authoritative,
                error: None,
            },
            last_authoritative_items: Vec::new(),
            tombstones: HashMap::new(),
            mutations: HashMap::new(),
            session_id,
            persistence_sink: None,
            admission_closed: false,
            persistence_failure: None,
            persistence_sequence: 0,
            subscribers: Vec::new(),
            state_subscribers: Vec::new(),
            next_sub_id: 0,
        })
    }

    fn from_inner(inner: Inner) -> Self {
        let next_revision = inner
            .revision
            .checked_add(1)
            .expect("message queue revision overflow");
        Self {
            inner: Arc::new(Mutex::new(inner)),
            mutation_gate: Arc::new(tokio::sync::Mutex::new(())),
            dispatch: Arc::new(Mutex::new(DispatchState {
                next_revision,
                pending: BTreeMap::new(),
                dispatching: false,
            })),
            persistence_dispatch: Arc::new(Mutex::new(PersistenceDispatchState {
                next_sequence: 1,
                pending: BTreeMap::new(),
                dispatching: false,
            })),
            durability_dispatch: Arc::new(Mutex::new(DurabilityDispatchState {
                next_sequence: 1,
                pending: BTreeMap::new(),
                dispatching: false,
            })),
            durability_notify: Arc::new(tokio::sync::Notify::new()),
            retained_persistence_tasks: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Rebuild one queue from the latest complete rollout checkpoint. Any item
    /// that was reserved when the previous process stopped is atomically
    /// terminalized as `interrupted`; it is never made pending again.
    #[cfg(test)]
    pub(crate) fn from_persistence_state(
        state: QueuePersistenceState,
        expected_session_id: &RuntimeString,
    ) -> Result<(Self, bool), RuntimeCodecError> {
        Self::from_persistence_state_with_history(state, expected_session_id, &HashSet::new())
    }

    pub(crate) fn from_persistence_state_with_history(
        state: QueuePersistenceState,
        expected_session_id: &RuntimeString,
        persisted_user_uuids: &HashSet<RuntimeString>,
    ) -> Result<(Self, bool), RuntimeCodecError> {
        if state.schema_version != QUEUE_PERSISTENCE_SCHEMA_VERSION {
            return Err(queue_persistence_error(
                "unsupported queue checkpoint schema",
            ));
        }
        if &state.session_id != expected_session_id {
            return Err(queue_persistence_error(
                "queue checkpoint belongs to a different session",
            ));
        }
        // `from_inner` seeds the next public dispatch revision with `revision +
        // 1`. Reject an exhausted checkpoint as invalid persisted state instead
        // of panicking while a real endpoint is being resumed.
        if state.revision == u64::MAX {
            return Err(queue_persistence_error(
                "queue checkpoint revision is exhausted",
            ));
        }

        let mut active_uuids = HashSet::new();
        let mut items = Vec::with_capacity(state.items.len());
        for persisted in state.items {
            let mut message = persisted.message;
            let Some(uuid) = message.uuid.clone() else {
                return Err(queue_persistence_error(
                    "persisted queued item is missing its stable uuid",
                ));
            };
            if !active_uuids.insert(uuid) {
                return Err(queue_persistence_error(
                    "queue checkpoint contains duplicate active uuids",
                ));
            }
            if message.session_id.as_ref() != Some(expected_session_id) {
                return Err(queue_persistence_error(
                    "persisted queued item belongs to a different session",
                ));
            }
            // Protocol submission correlation is endpoint-local and must never
            // survive a process boundary. PDF accounting is engine state and is
            // therefore carried by the checkpoint wrapper.
            message.correlation_id = None;
            message.pdf_bytes = persisted.pdf_bytes;
            message.pdf_documents = persisted.pdf_documents;
            items.push(Arc::new(message));
        }

        let mut reserved = HashSet::new();
        for uuid in state.reserved {
            if !reserved.insert(uuid.clone()) {
                return Err(queue_persistence_error(
                    "queue checkpoint contains duplicate reserved uuids",
                ));
            }
            if !active_uuids.contains(&uuid) {
                return Err(queue_persistence_error(
                    "queue checkpoint reserves an item that is not active",
                ));
            }
        }

        let resume_count = state.resume_reserved.len();
        let resume_reserved: HashSet<_> = state.resume_reserved.into_iter().collect();
        if resume_reserved.len() != resume_count {
            return Err(queue_persistence_error("duplicate resume reservation"));
        }
        if !resume_reserved.is_subset(&reserved) {
            return Err(queue_persistence_error(
                "resume reservation is not reserved",
            ));
        }
        let mut tombstones = HashMap::new();
        for tombstone in state.tombstones {
            if active_uuids.contains(&tombstone.uuid) {
                return Err(queue_persistence_error(
                    "queue checkpoint contains an active item tombstone",
                ));
            }
            if tombstones.insert(tombstone.uuid, tombstone.state).is_some() {
                return Err(queue_persistence_error(
                    "queue checkpoint contains duplicate tombstones",
                ));
            }
        }

        let mut mutations = HashMap::new();
        for mutation in state.mutations {
            if mutation.result.mutation_id != mutation.mutation_id {
                return Err(queue_persistence_error(
                    "queue mutation ledger identity does not match its result",
                ));
            }
            if mutation.result.snapshot.revision > state.revision {
                return Err(queue_persistence_error(
                    "queue mutation ledger references a future revision",
                ));
            }
            validate_snapshot_session(&mutation.result.snapshot, expected_session_id)?;
            if mutations
                .insert(mutation.mutation_id, (mutation.request, mutation.result))
                .is_some()
            {
                return Err(queue_persistence_error(
                    "queue checkpoint contains duplicate mutation ids",
                ));
            }
        }

        let recovered_reserved = !reserved.is_empty();
        let interrupted: HashSet<_> = reserved.difference(&resume_reserved).cloned().collect();
        let handed_off: HashSet<_> = resume_reserved
            .intersection(persisted_user_uuids)
            .cloned()
            .collect();
        let mut revision = state.revision;
        if recovered_reserved {
            items.retain(|item| {
                item.uuid
                    .as_ref()
                    .is_none_or(|uuid| !interrupted.contains(uuid) && !handed_off.contains(uuid))
            });
            for uuid in &interrupted {
                tombstones.insert(uuid.clone(), QueueTerminalState::Interrupted);
            }
            for uuid in &handed_off {
                tombstones.insert(uuid.clone(), QueueTerminalState::Drained);
            }
            reserved.clear();
            revision = revision
                .checked_add(1)
                .ok_or_else(|| queue_persistence_error("queue revision overflow on recovery"))?;
        }

        let mut inner = Inner {
            items,
            revision,
            reserved,
            resume_reserved: HashSet::new(),
            last_authoritative_snapshot: QueueSnapshot {
                revision: 0,
                items: Vec::new(),
                status: QueueSnapshotStatus::Authoritative,
                error: None,
            },
            last_authoritative_items: Vec::new(),
            tombstones,
            mutations,
            session_id: Some(expected_session_id.clone()),
            persistence_sink: None,
            admission_closed: false,
            persistence_failure: None,
            persistence_sequence: 0,
            subscribers: Vec::new(),
            state_subscribers: Vec::new(),
            next_sub_id: 0,
        };
        if recovered_reserved {
            reconcile_interrupted_mutations(&mut inner, &interrupted);
            reconcile_resume_mutations(&mut inner, &resume_reserved, None);
            reconcile_resume_mutations(&mut inner, &handed_off, Some(QueueTerminalState::Drained));
        }
        publish_current_as_authoritative_locked(&mut inner);
        Ok((Self::from_inner(inner), recovered_reserved))
    }

    pub(crate) fn set_persistence_sink(&self, sink: QueuePersistenceSink) {
        let mut inner = self.inner.lock_recover();
        assert!(
            inner.session_id.is_some(),
            "queue persistence requires a session-owned queue"
        );
        assert!(
            inner.persistence_sink.replace(sink).is_none(),
            "queue persistence sink installed more than once"
        );
    }

    /// Persist the current checkpoint without changing the public queue
    /// revision. Used after recovery normalization and for mutation-ledger-only
    /// outcomes such as stale/busy/invalid.
    pub(crate) fn persist_current_state(&self) -> Result<(), RuntimeString> {
        let persistence = {
            let mut inner = self.inner.lock_recover();
            ensure_queue_writable(&inner)?;
            persistence_dispatch_locked(&mut inner)
        };
        // Appending the checkpoint is synchronous; callers that need a durable
        // boundary use the receipt returned by the Host-mutation path below.
        // Session construction cannot await, and its later shutdown flush still
        // covers this recovery-normalization checkpoint.
        self.retain_checkpoint(persistence);
        Ok(())
    }

    /// Enqueue a command (defaults priority to `next`).
    pub fn enqueue(&self, item: QueuedMessage) -> Result<(), RuntimeString> {
        self.push_with_default(item, QueuePriority::Next)
    }

    pub async fn enqueue_durable(&self, item: QueuedMessage) -> Result<(), RuntimeString> {
        let change = self.prepare_push(item, QueuePriority::Next)?;
        self.await_change_verdict(change).await
    }

    /// Enqueue a deferred notification (defaults priority to `later`).
    pub fn enqueue_pending_notification(&self, item: QueuedMessage) -> Result<(), RuntimeString> {
        self.push_with_default(item, QueuePriority::Later)
    }

    pub async fn enqueue_pending_notification_durable(
        &self,
        item: QueuedMessage,
    ) -> Result<(), RuntimeString> {
        let change = self.prepare_push(item, QueuePriority::Later)?;
        self.await_change_verdict(change).await
    }

    fn push_with_default(
        &self,
        item: QueuedMessage,
        default: QueuePriority,
    ) -> Result<(), RuntimeString> {
        let change = self.prepare_push(item, default)?;
        self.retain_change(change, None);
        Ok(())
    }

    fn prepare_push(
        &self,
        mut item: QueuedMessage,
        default: QueuePriority,
    ) -> Result<ChangeDispatch, RuntimeString> {
        let change = {
            let mut inner = self.inner.lock_recover();
            ensure_queue_writable(&inner)?;
            item.priority = Some(item.priority.unwrap_or(default));
            if let Some(session_id) = &inner.session_id {
                item.session_id = Some(session_id.clone());
            }
            if item.uuid.is_none() {
                item.uuid = Some(uuid::Uuid::new_v4().to_string().into());
            }
            if let Some(uuid) = &item.uuid {
                if inner
                    .items
                    .iter()
                    .any(|existing| existing.uuid.as_ref() == Some(uuid))
                    || inner.tombstones.contains_key(uuid)
                {
                    return Err("queue message identity already exists".into());
                }
            }
            inner.items.push(Arc::new(item));
            change_dispatch(&mut inner)
        };
        Ok(change)
    }

    /// Remove + return the highest-priority item (FIFO within priority), optionally
    /// filtered.
    pub fn dequeue(
        &self,
        filter: Option<&(dyn Fn(&QueuedMessage) -> bool + Sync)>,
    ) -> Result<Option<Arc<QueuedMessage>>, RuntimeString> {
        let (removed, change) = self.prepare_dequeue(filter)?;
        if let Some(change) = change {
            self.retain_change(change, None);
        }
        Ok(removed)
    }

    pub async fn dequeue_durable(
        &self,
        filter: Option<&(dyn Fn(&QueuedMessage) -> bool + Sync)>,
    ) -> Result<Option<Arc<QueuedMessage>>, RuntimeString> {
        let (removed, change) = self.prepare_dequeue(filter)?;
        if let Some(change) = change {
            self.await_change_verdict(change).await?;
        }
        Ok(removed)
    }

    fn prepare_dequeue(
        &self,
        filter: Option<&(dyn Fn(&QueuedMessage) -> bool + Sync)>,
    ) -> Result<(Option<Arc<QueuedMessage>>, Option<ChangeDispatch>), RuntimeString> {
        let (removed, change) = {
            let mut inner = self.inner.lock_recover();
            ensure_queue_writable(&inner)?;
            match best_index(&inner.items, filter) {
                None => (None, None),
                Some(idx) => {
                    let removed = inner.items.remove(idx);
                    if let Some(uuid) = &removed.uuid {
                        inner.reserved.remove(uuid);
                        inner.resume_reserved.remove(uuid);
                        inner
                            .tombstones
                            .insert(uuid.clone(), QueueTerminalState::Drained);
                    }
                    let change = change_dispatch(&mut inner);
                    (Some(removed), Some(change))
                }
            }
        };
        Ok((removed, change))
    }

    /// Remove + return everything (insertion order preserved).
    pub fn dequeue_all(&self) -> Result<Vec<Arc<QueuedMessage>>, RuntimeString> {
        let (all, change) = self.prepare_dequeue_all()?;
        if let Some(change) = change {
            self.retain_change(change, None);
        }
        Ok(all)
    }

    pub async fn dequeue_all_durable(&self) -> Result<Vec<Arc<QueuedMessage>>, RuntimeString> {
        let (all, change) = self.prepare_dequeue_all()?;
        if let Some(change) = change {
            self.await_change_verdict(change).await?;
        }
        Ok(all)
    }

    fn prepare_dequeue_all(
        &self,
    ) -> Result<(Vec<Arc<QueuedMessage>>, Option<ChangeDispatch>), RuntimeString> {
        let (all, change) = {
            let mut inner = self.inner.lock_recover();
            ensure_queue_writable(&inner)?;
            if inner.items.is_empty() {
                (Vec::new(), None)
            } else {
                let all = std::mem::take(&mut inner.items);
                for item in &all {
                    if let Some(uuid) = &item.uuid {
                        inner.reserved.remove(uuid);
                        inner.resume_reserved.remove(uuid);
                        inner
                            .tombstones
                            .insert(uuid.clone(), QueueTerminalState::Drained);
                    }
                }
                let change = change_dispatch(&mut inner);
                (all, Some(change))
            }
        };
        Ok((all, change))
    }

    /// Remove + return all items matching a predicate; others stay.
    pub fn dequeue_all_matching(
        &self,
        predicate: &dyn Fn(&QueuedMessage) -> bool,
    ) -> Result<Vec<Arc<QueuedMessage>>, RuntimeString> {
        self.dequeue_all_matching_with_state(predicate, QueueTerminalState::Drained)
    }

    pub fn dequeue_all_matching_with_state(
        &self,
        predicate: &dyn Fn(&QueuedMessage) -> bool,
        terminal_state: QueueTerminalState,
    ) -> Result<Vec<Arc<QueuedMessage>>, RuntimeString> {
        let (matched, change) =
            self.prepare_dequeue_all_matching_with_state(predicate, terminal_state)?;
        if let Some(change) = change {
            self.retain_change(change, None);
        }
        Ok(matched)
    }

    pub async fn dequeue_all_matching_with_state_durable(
        &self,
        predicate: &(dyn Fn(&QueuedMessage) -> bool + Sync),
        terminal_state: QueueTerminalState,
    ) -> Result<Vec<Arc<QueuedMessage>>, RuntimeString> {
        let (matched, change) =
            self.prepare_dequeue_all_matching_with_state(predicate, terminal_state)?;
        if let Some(change) = change {
            self.await_change_verdict(change).await?;
        }
        Ok(matched)
    }

    fn prepare_dequeue_all_matching_with_state(
        &self,
        predicate: &dyn Fn(&QueuedMessage) -> bool,
        terminal_state: QueueTerminalState,
    ) -> Result<(Vec<Arc<QueuedMessage>>, Option<ChangeDispatch>), RuntimeString> {
        let (matched, change) = {
            let mut inner = self.inner.lock_recover();
            ensure_queue_writable(&inner)?;
            let mut matched = Vec::new();
            let mut remaining = Vec::new();
            let reserved = inner.reserved.clone();
            for cmd in inner.items.drain(..) {
                let is_reserved = cmd
                    .uuid
                    .as_ref()
                    .is_some_and(|uuid| reserved.contains(uuid));
                if predicate(&cmd) && !is_reserved {
                    matched.push(cmd);
                } else {
                    remaining.push(cmd);
                }
            }
            inner.items = remaining;
            if matched.is_empty() {
                (Vec::new(), None)
            } else {
                for item in &matched {
                    if let Some(uuid) = &item.uuid {
                        inner.reserved.remove(uuid);
                        inner.resume_reserved.remove(uuid);
                        inner.tombstones.insert(uuid.clone(), terminal_state);
                    }
                }
                let change = change_dispatch(&mut inner);
                (matched, Some(change))
            }
        };
        Ok((matched, change))
    }

    /// Internal owner handoff must include accepted inputs whose checkpoint is pending.
    pub(crate) fn admitted_snapshot(&self) -> Vec<Arc<QueuedMessage>> {
        self.inner.lock_recover().items.clone()
    }

    pub(crate) fn persistence_error(&self) -> Option<RuntimeString> {
        self.inner.lock_recover().persistence_failure.clone()
    }

    /// Highest-priority item without removing it, optionally filtered.
    pub fn peek(
        &self,
        filter: Option<&(dyn Fn(&QueuedMessage) -> bool + Sync)>,
    ) -> Option<Arc<QueuedMessage>> {
        let inner = self.inner.lock_recover();
        best_index(&inner.last_authoritative_items, filter)
            .map(|idx| Arc::clone(&inner.last_authoritative_items[idx]))
    }

    /// Remove specific items by REFERENCE identity (pass the `Arc`s handed out by
    /// [`get_commands_by_max_priority`](Self::get_commands_by_max_priority),
    /// [`snapshot`](Self::snapshot), or [`peek`](Self::peek)).
    pub fn remove(&self, to_remove: &[Arc<QueuedMessage>]) -> Result<(), RuntimeString> {
        self.remove_with_state(to_remove, QueueTerminalState::Removed)
    }

    pub async fn remove_durable(
        &self,
        to_remove: &[Arc<QueuedMessage>],
    ) -> Result<(), RuntimeString> {
        self.remove_with_state_durable(to_remove, QueueTerminalState::Removed)
            .await
    }

    pub fn remove_with_state(
        &self,
        to_remove: &[Arc<QueuedMessage>],
        terminal_state: QueueTerminalState,
    ) -> Result<(), RuntimeString> {
        let Some(change) = self.prepare_remove_with_state(to_remove, terminal_state)? else {
            return Ok(());
        };
        self.retain_change(change, None);
        Ok(())
    }

    pub async fn remove_with_state_durable(
        &self,
        to_remove: &[Arc<QueuedMessage>],
        terminal_state: QueueTerminalState,
    ) -> Result<(), RuntimeString> {
        let Some(change) = self.prepare_remove_with_state(to_remove, terminal_state)? else {
            return Ok(());
        };
        self.await_change_verdict(change).await
    }

    fn prepare_remove_with_state(
        &self,
        to_remove: &[Arc<QueuedMessage>],
        terminal_state: QueueTerminalState,
    ) -> Result<Option<ChangeDispatch>, RuntimeString> {
        let change = {
            let mut inner = self.inner.lock_recover();
            ensure_queue_writable(&inner)?;
            if to_remove.is_empty() {
                return Ok(None);
            }
            let mut removed = Vec::new();
            inner.items.retain(|item| {
                if to_remove
                    .iter()
                    .any(|candidate| Arc::ptr_eq(item, candidate))
                {
                    removed.push(item.clone());
                    false
                } else {
                    true
                }
            });
            if removed.is_empty() {
                None
            } else {
                let resumed: HashSet<_> = removed
                    .iter()
                    .filter_map(|item| item.uuid.as_ref())
                    .filter(|uuid| inner.resume_reserved.contains(*uuid))
                    .cloned()
                    .collect();
                for item in removed {
                    if let Some(uuid) = &item.uuid {
                        inner.reserved.remove(uuid);
                        inner.resume_reserved.remove(uuid);
                        inner.tombstones.insert(uuid.clone(), terminal_state);
                    }
                }
                let mut change = change_dispatch(&mut inner);
                reconcile_resume_mutations(&mut inner, &resumed, Some(terminal_state));
                refresh_change_persistence(&inner, &mut change);
                Some(change)
            }
        };
        Ok(change)
    }

    /// Items at or above `max_priority`, WITHOUT removing them (the drain snapshot):
    /// `now` returns only now-priority; `later` returns everything.
    pub fn get_commands_by_max_priority(
        &self,
        max_priority: QueuePriority,
    ) -> Vec<Arc<QueuedMessage>> {
        let threshold = max_priority.rank();
        let inner = self.inner.lock_recover();
        inner
            .last_authoritative_items
            .iter()
            .filter(|c| eff_priority(c).rank() <= threshold)
            .map(Arc::clone)
            .collect()
    }

    /// Atomically reserve the current drain snapshot. Reserved items remain
    /// visible but reject Host edit/remove/reorder with `busy` until the query
    /// loop durably persists their continuation and removes them.
    pub fn reserve_commands_by_max_priority(
        &self,
        max_priority: QueuePriority,
        filter: &dyn Fn(&QueuedMessage) -> bool,
    ) -> Result<Vec<Arc<QueuedMessage>>, RuntimeString> {
        let (selected, change) =
            self.prepare_reserve_commands_by_max_priority(max_priority, filter)?;
        if let Some(change) = change {
            self.retain_change(change, None);
        }
        Ok(selected)
    }

    pub async fn reserve_commands_by_max_priority_durable(
        &self,
        max_priority: QueuePriority,
        filter: &(dyn Fn(&QueuedMessage) -> bool + Sync),
    ) -> Result<Vec<Arc<QueuedMessage>>, RuntimeString> {
        let (selected, change) =
            self.prepare_reserve_commands_by_max_priority(max_priority, filter)?;
        let mut reservation = MailboxReservation {
            queue: self,
            items: selected,
            committed: false,
        };
        if let Some(change) = change {
            self.await_change_verdict(change).await?;
        }
        reservation.committed = true;
        Ok(std::mem::take(&mut reservation.items))
    }

    fn prepare_reserve_commands_by_max_priority(
        &self,
        max_priority: QueuePriority,
        filter: &dyn Fn(&QueuedMessage) -> bool,
    ) -> Result<(Vec<Arc<QueuedMessage>>, Option<ChangeDispatch>), RuntimeString> {
        let threshold = max_priority.rank();
        let (selected, change) = {
            let mut inner = self.inner.lock_recover();
            ensure_queue_writable(&inner)?;
            let selected = inner
                .items
                .iter()
                .filter(|item| {
                    eff_priority(item).rank() <= threshold
                        && filter(item)
                        && item
                            .uuid
                            .as_ref()
                            .is_none_or(|uuid| !inner.reserved.contains(uuid))
                })
                .cloned()
                .collect::<Vec<_>>();
            let mut changed = false;
            for item in &selected {
                if let Some(uuid) = &item.uuid {
                    changed |= inner.reserved.insert(uuid.clone());
                    if !item.triggers_turn() {
                        inner.resume_reserved.insert(uuid.clone());
                    }
                }
            }
            let change = changed.then(|| change_dispatch(&mut inner));
            (selected, change)
        };
        Ok((selected, change))
    }

    /// Claim one FIFO batch for a new foreground turn. Unlike inline tool
    /// continuations these reservations recover to pending until a message with
    /// its original UUID has been durably handed to conversation history.
    pub(crate) fn reserve_pending_resume(&self) -> Result<Vec<Arc<QueuedMessage>>, RuntimeString> {
        let (selected, change) = self.prepare_pending_resume(None)?;
        if let Some(change) = change {
            self.retain_change(change, None);
        }
        Ok(selected)
    }

    /// Mail received between turns survives a cancelled or interrupted handoff.
    pub(crate) async fn reserve_passive_mailbox_durable(
        &self,
        agent_id: Option<&RuntimeString>,
    ) -> Result<Vec<Arc<QueuedMessage>>, RuntimeString> {
        let (selected, change) = {
            let mut inner = self.inner.lock_recover();
            ensure_queue_writable(&inner)?;
            let selected: Vec<_> = inner
                .items
                .iter()
                .filter(|item| {
                    !item.triggers_turn()
                        && item.agent_id.as_ref() == agent_id
                        && item
                            .uuid
                            .as_ref()
                            .is_some_and(|id| !inner.reserved.contains(id))
                })
                .cloned()
                .collect();
            for item in &selected {
                let id = item.uuid.as_ref().expect("mailbox identity");
                inner.reserved.insert(id.clone());
                inner.resume_reserved.insert(id.clone());
            }
            let change = (!selected.is_empty()).then(|| change_dispatch(&mut inner));
            (selected, change)
        };
        let mut reservation = MailboxReservation {
            queue: self,
            items: selected,
            committed: false,
        };
        if let Some(change) = change {
            self.await_change_verdict(change).await?;
        }
        reservation.committed = true;
        Ok(std::mem::take(&mut reservation.items))
    }

    /// Resume reservations retain their pending meaning across process recovery.
    pub(crate) async fn reserve_agent_resume_durable(
        &self,
        agent_id: &RuntimeString,
    ) -> Result<Vec<Arc<QueuedMessage>>, RuntimeString> {
        let (selected, change) = self.prepare_pending_resume(Some(agent_id))?;
        if let Some(change) = change {
            if let Err(error) = self.await_change_verdict(change).await {
                self.release_pending_resume(&selected);
                return Err(error);
            }
        }
        Ok(selected)
    }

    fn prepare_pending_resume(
        &self,
        agent_id: Option<&RuntimeString>,
    ) -> Result<(Vec<Arc<QueuedMessage>>, Option<ChangeDispatch>), RuntimeString> {
        let (selected, change) = {
            let mut inner = self.inner.lock_recover();
            ensure_queue_writable(&inner)?;
            let selected: Vec<_> = inner
                .items
                .iter()
                .filter(|item| {
                    item.agent_id.as_ref() == agent_id
                        && (agent_id.is_none() || item.mode == "subagent-message")
                        // An admitted child continuation consumes its complete
                        // mailbox in FIFO order. Passive mail cannot admit a
                        // turn: callers make that decision separately.
                        && (agent_id.is_some() || item.triggers_turn())
                        && eff_priority(item).rank() <= QueuePriority::Next.rank()
                        && item
                            .uuid
                            .as_ref()
                            .is_some_and(|uuid| !inner.reserved.contains(uuid))
                })
                .cloned()
                .collect();
            for item in &selected {
                let uuid = item.uuid.as_ref().expect("queue identity");
                inner.reserved.insert(uuid.clone());
                inner.resume_reserved.insert(uuid.clone());
            }
            let change = (!selected.is_empty()).then(|| change_dispatch(&mut inner));
            (selected, change)
        };
        Ok((selected, change))
    }

    /// Release only this still-uncommitted batch. The retained persistence owner
    /// finishes even when a caller drops its unpolled turn or is cancelled.
    pub(crate) fn release_pending_resume(&self, items: &[Arc<QueuedMessage>]) {
        let change = {
            let mut inner = self.inner.lock_recover();
            if ensure_queue_writable(&inner).is_err() {
                return;
            }
            let mut released = HashSet::new();
            for item in items {
                if let Some(uuid) = &item.uuid {
                    if inner.resume_reserved.remove(uuid) {
                        inner.reserved.remove(uuid);
                        released.insert(uuid.clone());
                    }
                }
            }
            if released.is_empty() {
                None
            } else {
                let mut change = change_dispatch(&mut inner);
                reconcile_resume_mutations(&mut inner, &released, None);
                refresh_change_persistence(&inner, &mut change);
                Some(change)
            }
        };
        if let Some(change) = change {
            self.retain_change(change, None);
        }
    }

    /// Atomically settle every continuation-owned item after active turns have
    /// drained. The items are removed, receive durable `interrupted`
    /// tombstones, and any recorded `busy` mutation replay is reconciled to the
    /// new terminal truth in the same checkpoint.
    #[cfg(test)]
    pub(crate) fn interrupt_reserved(&self) -> Result<usize, RuntimeString> {
        let Some((count, change)) = self.prepare_interrupt_reserved()? else {
            return Ok(0);
        };
        self.retain_change(change, None);
        Ok(count)
    }

    pub(crate) async fn interrupt_reserved_durable(&self) -> Result<usize, RuntimeString> {
        let Some((count, change)) = self.prepare_interrupt_reserved()? else {
            return Ok(0);
        };
        self.await_change_verdict(change).await?;
        Ok(count)
    }

    fn prepare_interrupt_reserved(&self) -> Result<Option<(usize, ChangeDispatch)>, RuntimeString> {
        let (count, change) = {
            let mut inner = self.inner.lock_recover();
            ensure_queue_writable(&inner)?;
            if inner.reserved.is_empty() {
                return Ok(None);
            }
            let resumable = std::mem::take(&mut inner.resume_reserved);
            let interrupted: HashSet<_> = std::mem::take(&mut inner.reserved)
                .difference(&resumable)
                .cloned()
                .collect();
            inner.items.retain(|item| {
                item.uuid
                    .as_ref()
                    .is_none_or(|uuid| !interrupted.contains(uuid))
            });
            for uuid in &interrupted {
                inner
                    .tombstones
                    .insert(uuid.clone(), QueueTerminalState::Interrupted);
            }
            let mut change = change_dispatch(&mut inner);
            reconcile_interrupted_mutations(&mut inner, &interrupted);
            reconcile_resume_mutations(&mut inner, &resumable, None);
            refresh_change_persistence(&inner, &mut change);
            (interrupted.len(), change)
        };
        Ok(Some((count, change)))
    }

    /// A snapshot copy (safe to iterate while the queue mutates).
    pub fn snapshot(&self) -> Vec<Arc<QueuedMessage>> {
        let inner = self.inner.lock_recover();
        inner
            .last_authoritative_items
            .iter()
            .map(Arc::clone)
            .collect()
    }

    pub fn queue_snapshot(&self) -> QueueSnapshot {
        visible_snapshot_locked(&self.inner.lock_recover())
    }

    pub async fn edit_queued(
        &self,
        mutation_id: RuntimeString,
        uuid: RuntimeString,
        expected_revision: u64,
        edit: QueueEdit,
    ) -> QueueMutationResult {
        let _mutation = self.mutation_gate.lock().await;
        let request = QueueMutationRequest::Edit {
            uuid: uuid.clone(),
            expected_revision,
            edit: edit.clone(),
        };
        let commit = (|| {
            let mut inner = self.inner.lock_recover();
            if let Some(result) = replay_mutation(&inner, &mutation_id, &request) {
                return QueueMutationCommit::immediate(result);
            }
            if let Some(result) = Self::persistence_poisoned_result_locked(
                &mut inner,
                mutation_id.clone(),
                request.clone(),
            ) {
                return QueueMutationCommit::immediate(result);
            }
            if let Some(result) = Self::admission_closed_result_locked(&inner, &mutation_id) {
                return QueueMutationCommit::immediate(result);
            }
            if expected_revision != inner.revision {
                let result = queue_mutation_result(
                    &inner,
                    mutation_id.clone(),
                    QueueMutationStatus::StaleRevision,
                    None,
                    Some("queue revision does not match expectedRevision"),
                );
                return QueueMutationCommit::ledger(&mut inner, mutation_id, request, result);
            }
            let Some(index) = inner
                .items
                .iter()
                .position(|item| item.uuid.as_ref() == Some(&uuid))
            else {
                let terminal = inner.tombstones.get(&uuid).copied();
                let result = queue_mutation_result(
                    &inner,
                    mutation_id.clone(),
                    QueueMutationStatus::Missing,
                    terminal,
                    Some("queued item no longer exists"),
                );
                return QueueMutationCommit::ledger(&mut inner, mutation_id, request, result);
            };
            if inner.reserved.contains(&uuid) {
                let result = queue_mutation_result(
                    &inner,
                    mutation_id.clone(),
                    QueueMutationStatus::Busy,
                    None,
                    Some("queued item is reserved by the active continuation"),
                );
                return QueueMutationCommit::ledger(&mut inner, mutation_id, request, result);
            }
            let current = &inner.items[index];
            if current.is_meta == Some(true) {
                let result = queue_mutation_result(
                    &inner,
                    mutation_id.clone(),
                    QueueMutationStatus::Invalid,
                    None,
                    Some("queued item is not Host-mutable"),
                );
                return QueueMutationCommit::ledger(&mut inner, mutation_id, request, result);
            }
            if edit.value.is_none() && edit.mode.is_none() && edit.priority.is_none() {
                let result = queue_mutation_result(
                    &inner,
                    mutation_id.clone(),
                    QueueMutationStatus::Invalid,
                    None,
                    Some("queue edit must change value, mode, or priority"),
                );
                return QueueMutationCommit::ledger(&mut inner, mutation_id, request, result);
            }
            if edit.value.as_ref().is_some_and(queued_value_has_document) {
                let result = queue_mutation_result(
                    &inner,
                    mutation_id.clone(),
                    QueueMutationStatus::Invalid,
                    None,
                    Some("document queue edits require re-enqueue validation"),
                );
                return QueueMutationCommit::ledger(&mut inner, mutation_id, request, result);
            }
            if edit.value.as_ref().is_some_and(|value| matches!(value,
                QueuedValue::Blocks(blocks) if blocks.iter().any(|block| matches!(block, ContentBlock::Media(_)))
            )) {
                let result = queue_mutation_result(
                    &inner, mutation_id.clone(), QueueMutationStatus::Invalid, None,
                    Some("native media queue edits require re-enqueue validation"),
                );
                return QueueMutationCommit::ledger(&mut inner, mutation_id, request, result);
            }
            let mut replacement = (**current).clone();
            if let Some(value) = edit.value {
                replacement.value = value;
                // The prior render metadata describes the old immutable input.
                replacement.display_metadata = None;
                replacement.pdf_bytes = 0;
                replacement.pdf_documents = 0;
            }
            if let Some(mode) = edit.mode {
                replacement.mode = mode;
            }
            if let Some(priority) = edit.priority {
                replacement.priority = Some(priority);
            }
            inner.items[index] = Arc::new(replacement);
            let mut change = change_dispatch(&mut inner);
            let result = QueueMutationResult {
                mutation_id: mutation_id.clone(),
                status: QueueMutationStatus::Applied,
                snapshot: change.snapshot.clone(),
                terminal_state: None,
                error: None,
            };
            inner
                .mutations
                .insert(mutation_id, (request, result.clone()));
            refresh_change_persistence(&inner, &mut change);
            QueueMutationCommit::applied(result, change)
        })();
        self.finish_mutation(commit).await
    }

    pub async fn remove_queued(
        &self,
        mutation_id: RuntimeString,
        uuid: RuntimeString,
        expected_revision: u64,
    ) -> QueueMutationResult {
        let _mutation = self.mutation_gate.lock().await;
        let request = QueueMutationRequest::Remove {
            uuid: uuid.clone(),
            expected_revision,
        };
        let commit = (|| {
            let mut inner = self.inner.lock_recover();
            if let Some(result) = replay_mutation(&inner, &mutation_id, &request) {
                return QueueMutationCommit::immediate(result);
            }
            if let Some(result) = Self::persistence_poisoned_result_locked(
                &mut inner,
                mutation_id.clone(),
                request.clone(),
            ) {
                return QueueMutationCommit::immediate(result);
            }
            if let Some(result) = Self::admission_closed_result_locked(&inner, &mutation_id) {
                return QueueMutationCommit::immediate(result);
            }
            if expected_revision != inner.revision {
                let result = queue_mutation_result(
                    &inner,
                    mutation_id.clone(),
                    QueueMutationStatus::StaleRevision,
                    None,
                    Some("queue revision does not match expectedRevision"),
                );
                return QueueMutationCommit::ledger(&mut inner, mutation_id, request, result);
            }
            let Some(index) = inner
                .items
                .iter()
                .position(|item| item.uuid.as_ref() == Some(&uuid))
            else {
                let terminal = inner.tombstones.get(&uuid).copied();
                let result = queue_mutation_result(
                    &inner,
                    mutation_id.clone(),
                    QueueMutationStatus::Missing,
                    terminal,
                    Some("queued item no longer exists"),
                );
                return QueueMutationCommit::ledger(&mut inner, mutation_id, request, result);
            };
            if inner.reserved.contains(&uuid) {
                let result = queue_mutation_result(
                    &inner,
                    mutation_id.clone(),
                    QueueMutationStatus::Busy,
                    None,
                    Some("queued item is reserved by the active continuation"),
                );
                return QueueMutationCommit::ledger(&mut inner, mutation_id, request, result);
            }
            if inner.items[index].is_meta == Some(true) {
                let result = queue_mutation_result(
                    &inner,
                    mutation_id.clone(),
                    QueueMutationStatus::Invalid,
                    None,
                    Some("queued item is not Host-mutable"),
                );
                return QueueMutationCommit::ledger(&mut inner, mutation_id, request, result);
            }
            inner.items.remove(index);
            inner
                .tombstones
                .insert(uuid.clone(), QueueTerminalState::Removed);
            let mut change = change_dispatch(&mut inner);
            let result = QueueMutationResult {
                mutation_id: mutation_id.clone(),
                status: QueueMutationStatus::Applied,
                snapshot: change.snapshot.clone(),
                terminal_state: Some(QueueTerminalState::Removed),
                error: None,
            };
            inner
                .mutations
                .insert(mutation_id, (request, result.clone()));
            refresh_change_persistence(&inner, &mut change);
            QueueMutationCommit::applied(result, change)
        })();
        self.finish_mutation(commit).await
    }

    pub async fn reorder_queued(
        &self,
        mutation_id: RuntimeString,
        uuid: RuntimeString,
        before_uuid: Option<RuntimeString>,
        expected_revision: u64,
    ) -> QueueMutationResult {
        let _mutation = self.mutation_gate.lock().await;
        let request = QueueMutationRequest::Reorder {
            uuid: uuid.clone(),
            before_uuid: before_uuid.clone(),
            expected_revision,
        };
        let commit = (|| {
            let mut inner = self.inner.lock_recover();
            if let Some(result) = replay_mutation(&inner, &mutation_id, &request) {
                return QueueMutationCommit::immediate(result);
            }
            if let Some(result) = Self::persistence_poisoned_result_locked(
                &mut inner,
                mutation_id.clone(),
                request.clone(),
            ) {
                return QueueMutationCommit::immediate(result);
            }
            if let Some(result) = Self::admission_closed_result_locked(&inner, &mutation_id) {
                return QueueMutationCommit::immediate(result);
            }
            if expected_revision != inner.revision {
                let result = queue_mutation_result(
                    &inner,
                    mutation_id.clone(),
                    QueueMutationStatus::StaleRevision,
                    None,
                    Some("queue revision does not match expectedRevision"),
                );
                return QueueMutationCommit::ledger(&mut inner, mutation_id, request, result);
            }
            let Some(from) = inner
                .items
                .iter()
                .position(|item| item.uuid.as_ref() == Some(&uuid))
            else {
                let terminal = inner.tombstones.get(&uuid).copied();
                let result = queue_mutation_result(
                    &inner,
                    mutation_id.clone(),
                    QueueMutationStatus::Missing,
                    terminal,
                    Some("queued item no longer exists"),
                );
                return QueueMutationCommit::ledger(&mut inner, mutation_id, request, result);
            };
            let target = before_uuid.as_ref().and_then(|before| {
                inner
                    .items
                    .iter()
                    .position(|item| item.uuid.as_ref() == Some(before))
            });
            if before_uuid.is_some() && target.is_none() {
                let result = queue_mutation_result(
                    &inner,
                    mutation_id.clone(),
                    QueueMutationStatus::Invalid,
                    None,
                    Some("beforeUuid does not identify a queued item"),
                );
                return QueueMutationCommit::ledger(&mut inner, mutation_id, request, result);
            }
            if target == Some(from) {
                let result = queue_mutation_result(
                    &inner,
                    mutation_id.clone(),
                    QueueMutationStatus::Invalid,
                    None,
                    Some("beforeUuid must identify a different queued item"),
                );
                return QueueMutationCommit::ledger(&mut inner, mutation_id, request, result);
            }
            let target_uuid = target.and_then(|index| inner.items[index].uuid.clone());
            if inner.reserved.contains(&uuid)
                || target_uuid
                    .as_ref()
                    .is_some_and(|target| inner.reserved.contains(target))
            {
                let result = queue_mutation_result(
                    &inner,
                    mutation_id.clone(),
                    QueueMutationStatus::Busy,
                    None,
                    Some("queued reorder touches an item reserved by the active continuation"),
                );
                return QueueMutationCommit::ledger(&mut inner, mutation_id, request, result);
            }
            if inner.items[from].is_meta == Some(true) {
                let result = queue_mutation_result(
                    &inner,
                    mutation_id.clone(),
                    QueueMutationStatus::Invalid,
                    None,
                    Some("queued item is not Host-mutable"),
                );
                return QueueMutationCommit::ledger(&mut inner, mutation_id, request, result);
            }
            if let Some(target) = target {
                if eff_priority(&inner.items[from]) != eff_priority(&inner.items[target]) {
                    let result = queue_mutation_result(
                        &inner,
                        mutation_id.clone(),
                        QueueMutationStatus::Invalid,
                        None,
                        Some("reorder requires items with the same priority"),
                    );
                    return QueueMutationCommit::ledger(&mut inner, mutation_id, request, result);
                }
            }
            let item = inner.items.remove(from);
            let insertion = match before_uuid {
                Some(before) => inner
                    .items
                    .iter()
                    .position(|candidate| candidate.uuid.as_ref() == Some(&before))
                    .expect("target remained after removing a distinct source"),
                None => {
                    let priority = eff_priority(&item);
                    inner
                        .items
                        .iter()
                        .rposition(|candidate| eff_priority(candidate) == priority)
                        .map(|index| index + 1)
                        .unwrap_or(inner.items.len())
                }
            };
            inner.items.insert(insertion, item);
            let mut change = change_dispatch(&mut inner);
            let result = QueueMutationResult {
                mutation_id: mutation_id.clone(),
                status: QueueMutationStatus::Applied,
                snapshot: change.snapshot.clone(),
                terminal_state: None,
                error: None,
            };
            inner
                .mutations
                .insert(mutation_id, (request, result.clone()));
            refresh_change_persistence(&inner, &mut change);
            QueueMutationCommit::applied(result, change)
        })();
        self.finish_mutation(commit).await
    }

    pub fn len(&self) -> usize {
        self.inner.lock_recover().last_authoritative_items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Subscribe to queue changes; returns an unsubscribe fn.
    pub fn subscribe(&self, cb: Subscriber) -> Box<dyn Fn() + Send + Sync> {
        let id = {
            let mut inner = self.inner.lock_recover();
            let id = inner.next_sub_id;
            inner.next_sub_id += 1;
            inner.subscribers.push((id, cb));
            id
        };
        let weak = Arc::downgrade(&self.inner);
        Box::new(move || {
            if let Some(inner_arc) = weak.upgrade() {
                let mut inner = inner_arc.lock_recover();
                inner.subscribers.retain(|(sid, _)| *sid != id);
            }
        })
    }

    pub fn subscribe_state(&self, callback: QueueStateSubscriber) -> Box<dyn Fn() + Send + Sync> {
        let id = {
            let mut inner = self.inner.lock_recover();
            let id = inner.next_sub_id;
            inner.next_sub_id += 1;
            inner.state_subscribers.push((id, callback));
            id
        };
        let weak = Arc::downgrade(&self.inner);
        Box::new(move || {
            if let Some(inner_arc) = weak.upgrade() {
                inner_arc
                    .lock_recover()
                    .state_subscribers
                    .retain(|(subscriber_id, _)| *subscriber_id != id);
            }
        })
    }

    /// Drop everything.
    pub fn clear(&self) -> Result<(), RuntimeString> {
        let Some(change) = self.prepare_clear()? else {
            return Ok(());
        };
        self.retain_change(change, None);
        Ok(())
    }

    pub async fn clear_durable(&self) -> Result<(), RuntimeString> {
        let Some(change) = self.prepare_clear()? else {
            return Ok(());
        };
        self.await_change_verdict(change).await
    }

    fn prepare_clear(&self) -> Result<Option<ChangeDispatch>, RuntimeString> {
        let change = {
            let mut inner = self.inner.lock_recover();
            ensure_queue_writable(&inner)?;
            if inner.items.is_empty() {
                None
            } else {
                let removed = inner.items.clone();
                inner.items.clear();
                inner.reserved.clear();
                inner.resume_reserved.clear();
                for item in removed {
                    if let Some(uuid) = &item.uuid {
                        inner
                            .tombstones
                            .insert(uuid.clone(), QueueTerminalState::Cleared);
                    }
                }
                Some(change_dispatch(&mut inner))
            }
        };
        Ok(change)
    }

    /// Publish committed revisions in exactly the order in which they became
    /// authoritative under `inner`. Callbacks run without the queue lock held,
    /// so observers may query or even mutate the queue reentrantly; reentrant
    /// changes are buffered behind the revision currently being delivered.
    fn fire_change(&self, change: ChangeDispatch) {
        let mut next = {
            let mut dispatch = self.dispatch.lock_recover();
            let revision = change.snapshot.revision;
            assert!(
                dispatch.pending.insert(revision, change).is_none(),
                "queue revision dispatched more than once"
            );
            if dispatch.dispatching {
                return;
            }
            let revision = dispatch.next_revision;
            let Some(change) = dispatch.pending.remove(&revision) else {
                return;
            };
            dispatch.dispatching = true;
            change
        };

        loop {
            self.fire_change_callbacks(next);
            let mut dispatch = self.dispatch.lock_recover();
            dispatch.next_revision = dispatch
                .next_revision
                .checked_add(1)
                .expect("message queue dispatch revision overflow");
            let revision = dispatch.next_revision;
            match dispatch.pending.remove(&revision) {
                Some(change) => {
                    next = change;
                }
                None => {
                    dispatch.dispatching = false;
                    return;
                }
            }
        }
    }

    fn fire_change_callbacks(&self, change: ChangeDispatch) {
        {
            let mut inner = self.inner.lock_recover();
            if inner.persistence_failure.is_some() {
                // Once durability is poisoned, advancing the internal
                // dispatcher is still necessary to avoid a permanent revision
                // gap, but publishing a new authoritative Host snapshot would
                // be a lie.
                return;
            }
            inner.last_authoritative_snapshot = change.snapshot.clone();
            inner.last_authoritative_items = change.authoritative_items.clone();
        }
        debug_assert!(change.persistence.is_none());
        fire(change.subscribers);
        for subscriber in change.state_subscribers {
            let snapshot = change.snapshot.clone();
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| subscriber(snapshot)));
        }
    }

    /// Checkpoint callbacks are ordered independently from public revisions
    /// because a rejected mutation records idempotency state without advancing
    /// `QueueSnapshot::revision`. Sequence buffering prevents a concurrent later
    /// revision from reaching the rollout before that ledger-only checkpoint.
    fn fire_persistence(
        &self,
        persistence: Option<PersistenceDispatch>,
    ) -> Option<StartedPersistence> {
        let mut persistence = persistence?;
        let requested_sequence = persistence.sequence;
        let started = persistence
            .started_receiver
            .take()
            .expect("queue checkpoint owns one start receiver");
        let mut next = {
            let mut dispatch = self.persistence_dispatch.lock_recover();
            let sequence = persistence.sequence;
            assert!(
                dispatch.pending.insert(sequence, persistence).is_none(),
                "queue checkpoint dispatched more than once"
            );
            if dispatch.dispatching {
                None
            } else {
                let sequence = dispatch.next_sequence;
                dispatch.pending.remove(&sequence).inspect(|_persistence| {
                    dispatch.dispatching = true;
                })
            }
        };

        while let Some(mut current) = next {
            let future = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                (current.sink)(current.state)
            }))
            .unwrap_or_else(|_| {
                Box::pin(async {
                    Err(RuntimeString::from(
                        "queue persistence sink panicked before returning a durability receipt",
                    ))
                })
            });
            if let Some(started) = current.started_sender.take() {
                let _ = started.send(future);
            }

            let mut dispatch = self.persistence_dispatch.lock_recover();
            dispatch.next_sequence = dispatch
                .next_sequence
                .checked_add(1)
                .expect("message queue checkpoint sequence overflow");
            let sequence = dispatch.next_sequence;
            next = dispatch.pending.remove(&sequence);
            if next.is_none() {
                dispatch.dispatching = false;
            }
        }

        Some(StartedPersistence {
            sequence: requested_sequence,
            receipt: Box::pin(async move {
                match started.await {
                    Ok(receipt) => receipt.await,
                    Err(_) => Err(RuntimeString::from(
                        "queue persistence dispatcher terminated before starting the receipt",
                    )),
                }
            }),
        })
    }

    fn retain_checkpoint(&self, persistence: Option<PersistenceDispatch>) {
        if let Some(started) = self.fire_persistence(persistence) {
            self.spawn_retained_receipt(started, DurabilityAction::Checkpoint);
        }
    }

    fn retain_change(
        &self,
        mut change: ChangeDispatch,
        verdict: Option<tokio::sync::oneshot::Sender<Result<(), RuntimeString>>>,
    ) {
        match self.fire_persistence(change.persistence.take()) {
            Some(started) => {
                self.spawn_retained_receipt(started, DurabilityAction::Change { change, verdict })
            }
            None => {
                self.fire_change(change);
                if let Some(verdict) = verdict {
                    let _ = verdict.send(Ok(()));
                }
            }
        }
    }

    async fn await_change_verdict(&self, change: ChangeDispatch) -> Result<(), RuntimeString> {
        let (verdict, completion) = tokio::sync::oneshot::channel();
        self.retain_change(change, Some(verdict));
        completion.await.unwrap_or_else(|_| {
            Err(self.poison_queue(
                "queue durability owner terminated before publishing its verdict".into(),
            ))
        })
    }

    async fn finish_mutation(&self, commit: QueueMutationCommit) -> QueueMutationResult {
        let QueueMutationCommit {
            result,
            change,
            persistence,
        } = commit;
        let Some(started) = self.fire_persistence(persistence) else {
            if let Some(change) = change {
                self.fire_change(change);
            }
            return result;
        };
        let (response, terminal) = tokio::sync::oneshot::channel();
        self.spawn_retained_receipt(
            started,
            DurabilityAction::Mutation {
                result: result.clone(),
                change,
                response,
            },
        );
        terminal.await.unwrap_or_else(|_| {
            self.retained_mutation_owner_failed(
                &result.mutation_id,
                "queue mutation durability owner terminated before completion".into(),
            )
        })
    }

    fn spawn_retained_receipt(&self, started: StartedPersistence, action: DurabilityAction) {
        let queue = self.clone();
        let owner = async move {
            let result = std::panic::AssertUnwindSafe(started.receipt)
                .catch_unwind()
                .await
                .unwrap_or_else(|_| {
                    Err(RuntimeString::from(
                        "queue persistence receipt panicked before reporting durability",
                    ))
                });
            queue.complete_durability(DurabilityCompletion {
                sequence: started.sequence,
                result,
                action,
            });
        };
        let task = match tokio::runtime::Handle::try_current() {
            Ok(handle) => RetainedPersistenceTask::Tokio(handle.spawn(owner)),
            Err(_) => RetainedPersistenceTask::Thread(std::thread::spawn(move || {
                futures::executor::block_on(owner)
            })),
        };
        self.retained_persistence_tasks.lock_recover().push(task);
    }

    fn complete_durability(&self, completion: DurabilityCompletion) {
        let mut next = {
            let mut dispatch = self.durability_dispatch.lock_recover();
            assert!(
                dispatch
                    .pending
                    .insert(completion.sequence, completion)
                    .is_none(),
                "queue durability receipt completed more than once"
            );
            if dispatch.dispatching {
                return;
            }
            let sequence = dispatch.next_sequence;
            let Some(completion) = dispatch.pending.remove(&sequence) else {
                return;
            };
            dispatch.dispatching = true;
            completion
        };

        loop {
            self.apply_durability_completion(next);
            let mut dispatch = self.durability_dispatch.lock_recover();
            dispatch.next_sequence = dispatch
                .next_sequence
                .checked_add(1)
                .expect("message queue durability sequence overflow");
            self.durability_notify.notify_waiters();
            let sequence = dispatch.next_sequence;
            match dispatch.pending.remove(&sequence) {
                Some(completion) => next = completion,
                None => {
                    dispatch.dispatching = false;
                    return;
                }
            }
        }
    }

    fn apply_durability_completion(&self, completion: DurabilityCompletion) {
        let prior_failure = self.inner.lock_recover().persistence_failure.clone();
        if let Some(error) = prior_failure {
            match completion.action {
                DurabilityAction::Checkpoint => {}
                DurabilityAction::Change { change, verdict } => {
                    self.fire_change(change);
                    if let Some(verdict) = verdict {
                        let _ = verdict.send(Err(error));
                    }
                }
                DurabilityAction::Mutation {
                    result,
                    change,
                    response,
                } => {
                    // This sequence was submitted before the earlier receipt
                    // poisoned the queue. Its CAS body may therefore already
                    // have executed even though its own raw receipt completed
                    // later; preserve that distinction in the terminal result.
                    let executed = change.is_some();
                    let terminal =
                        self.settle_failed_mutation(&result.mutation_id, executed, error);
                    if let Some(change) = change {
                        self.fire_change(change);
                    }
                    let _ = response.send(terminal);
                }
            }
            return;
        }
        match (completion.result, completion.action) {
            (Ok(()), DurabilityAction::Checkpoint) => {}
            (Ok(()), DurabilityAction::Change { change, verdict }) => {
                self.fire_change(change);
                if let Some(verdict) = verdict {
                    let _ = verdict.send(Ok(()));
                }
            }
            (
                Ok(()),
                DurabilityAction::Mutation {
                    result,
                    change,
                    response,
                },
            ) => {
                if let Some(change) = change {
                    self.fire_change(change);
                }
                let _ = response.send(result);
            }
            (Err(error), DurabilityAction::Checkpoint) => {
                self.poison_queue(error);
            }
            (Err(error), DurabilityAction::Change { change, verdict }) => {
                let error = self.poison_queue(error);
                self.fire_change(change);
                if let Some(verdict) = verdict {
                    let _ = verdict.send(Err(error));
                }
            }
            (
                Err(error),
                DurabilityAction::Mutation {
                    result,
                    change,
                    response,
                },
            ) => {
                let executed = change.is_some();
                let error = self.poison_queue(error);
                let terminal = self.settle_failed_mutation(&result.mutation_id, executed, error);
                if let Some(change) = change {
                    self.fire_change(change);
                }
                let _ = response.send(terminal);
            }
        }
    }

    /// Wait until every checkpoint submitted before this call has received its
    /// ordered durability verdict. Later submissions do not extend the barrier.
    pub async fn await_pending_durability(&self) -> Result<(), RuntimeString> {
        let target = self.inner.lock_recover().persistence_sequence;
        self.await_durability_through(target).await
    }

    async fn await_durability_through(&self, target: u64) -> Result<(), RuntimeString> {
        loop {
            let notified = self.durability_notify.notified();
            if self.durability_dispatch.lock_recover().next_sequence > target {
                break;
            }
            notified.await;
        }
        match self.inner.lock_recover().persistence_failure.clone() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Close mutation admission and wait for every retained receipt owner whose
    /// persistence sequence was allocated before the close. Closing and
    /// capturing the target happen under the same queue-state lock, so a
    /// producer is either included in this barrier or rejected before mutation.
    pub async fn drain_persistence_tasks(&self) -> Result<(), RuntimeString> {
        let target = {
            let mut inner = self.inner.lock_recover();
            inner.admission_closed = true;
            inner.persistence_sequence
        };
        let verdict = self.await_durability_through(target).await;
        let tasks = std::mem::take(&mut *self.retained_persistence_tasks.lock_recover());
        for task in tasks {
            match task {
                RetainedPersistenceTask::Tokio(task) => {
                    if let Err(error) = task.await {
                        self.poison_queue(
                            format!("queue persistence owner task failed: {error}").into(),
                        );
                    }
                }
                RetainedPersistenceTask::Thread(task) => {
                    if task.join().is_err() {
                        self.poison_queue("queue persistence owner thread panicked".into());
                    }
                }
            }
        }
        verdict.and_then(
            |_| match self.inner.lock_recover().persistence_failure.clone() {
                Some(error) => Err(error),
                None => Ok(()),
            },
        )
    }

    /// Abandon persistence receipts owned by a Session that was constructed for
    /// resume but was never handed to a caller.
    ///
    /// This is deliberately narrower than [`Self::drain_persistence_tasks`]. A
    /// published Session must retain every persistence owner through its real
    /// durability verdict, even after a shutdown deadline. Before publication,
    /// however, caller cancellation leaves no Host that can observe or continue
    /// the Session. Keeping a non-cooperative receipt alive would retain the
    /// rollout writer forever and prevent the orphan's bounded shutdown from
    /// closing the store lease.
    ///
    /// Admission is closed before Tokio owners are synchronously told to abort.
    /// Their handles stay in the normal drain registry, so typed shutdown still
    /// joins them before the final rollout boundary. Queue state is poisoned
    /// rather than presented as authoritative.
    pub(crate) fn abandon_unpublished_persistence_owners(&self) {
        let target = {
            let mut inner = self.inner.lock_recover();
            inner.admission_closed = true;
            inner.persistence_sequence
        };
        let tasks = self.retained_persistence_tasks.lock_recover();
        if tasks
            .iter()
            .any(|task| matches!(task, RetainedPersistenceTask::Thread(_)))
        {
            // Session construction under Tokio never takes this branch. A
            // native thread cannot be force-aborted safely, so let ordinary
            // bounded shutdown fail closed rather than claiming a fence.
            self.poison_queue(
                "unpublished queue cleanup cannot abort a native persistence owner".into(),
            );
            return;
        }
        for task in tasks.iter() {
            let RetainedPersistenceTask::Tokio(task) = task else {
                unreachable!("native persistence owners were rejected above")
            };
            task.abort();
        }
        drop(tasks);

        self.poison_queue(
            "queue persistence was cancelled because resumed Session admission was abandoned"
                .into(),
        );
        {
            let mut dispatch = self.durability_dispatch.lock_recover();
            dispatch.pending.clear();
            dispatch.next_sequence = dispatch.next_sequence.max(
                target
                    .checked_add(1)
                    .expect("message queue checkpoint sequence overflow"),
            );
            dispatch.dispatching = false;
        }
        self.durability_notify.notify_waiters();
    }

    #[cfg(test)]
    fn admission_is_closed(&self) -> bool {
        self.inner.lock_recover().admission_closed
    }

    pub(crate) fn retained_mutation_owner_failed(
        &self,
        mutation_id: &RuntimeString,
        error: RuntimeString,
    ) -> QueueMutationResult {
        let executed = self
            .inner
            .lock_recover()
            .mutations
            .get(mutation_id)
            .map(|(_, result)| result.status == QueueMutationStatus::Applied);
        if let Some(executed) = executed {
            let error = self.poison_queue(error);
            return self.settle_failed_mutation(mutation_id, executed, error);
        }
        QueueMutationResult {
            mutation_id: mutation_id.clone(),
            status: QueueMutationStatus::OutcomeUnknown,
            snapshot: self.queue_snapshot(),
            terminal_state: None,
            error: Some(error),
        }
    }

    pub(crate) fn mutation_result(
        &self,
        mutation_id: &RuntimeString,
    ) -> Option<QueueMutationResult> {
        self.inner
            .lock_recover()
            .mutations
            .get(mutation_id)
            .map(|(_, result)| result.clone())
    }

    pub(crate) fn mutation_not_admitted(
        &self,
        mutation_id: RuntimeString,
        error: RuntimeString,
    ) -> QueueMutationResult {
        QueueMutationResult {
            mutation_id,
            status: QueueMutationStatus::NotAdmitted,
            snapshot: self.queue_snapshot(),
            terminal_state: None,
            error: Some(error),
        }
    }

    fn settle_failed_mutation(
        &self,
        mutation_id: &RuntimeString,
        executed: bool,
        error: RuntimeString,
    ) -> QueueMutationResult {
        let mut inner = self.inner.lock_recover();
        let snapshot = visible_snapshot_locked(&inner);
        let (_, result) = inner
            .mutations
            .get_mut(mutation_id)
            .expect("a durability receipt only exists after the mutation ledger commit");
        result.status = if executed {
            QueueMutationStatus::OutcomeUnknown
        } else {
            QueueMutationStatus::PersistenceFailed
        };
        result.snapshot = snapshot;
        result.error = Some(error);
        result.clone()
    }

    fn poison_queue(&self, error: RuntimeString) -> RuntimeString {
        let (first_error, terminal) = {
            let mut inner = self.inner.lock_recover();
            if let Some(first_error) = &inner.persistence_failure {
                return first_error.clone();
            }
            inner.persistence_failure = Some(error.clone());
            let snapshot = visible_snapshot_locked(&inner);
            let subscribers = inner
                .state_subscribers
                .iter()
                .map(|(_, subscriber)| subscriber.clone())
                .collect::<Vec<_>>();
            (error, Some((snapshot, subscribers)))
        };
        if let Some((snapshot, subscribers)) = terminal {
            for subscriber in subscribers {
                let snapshot = snapshot.clone();
                let _ =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| subscriber(snapshot)));
            }
        }
        first_error
    }

    fn persistence_poisoned_result_locked(
        inner: &mut Inner,
        mutation_id: RuntimeString,
        request: QueueMutationRequest,
    ) -> Option<QueueMutationResult> {
        let error = inner.persistence_failure.clone()?;
        let result = QueueMutationResult {
            mutation_id: mutation_id.clone(),
            status: QueueMutationStatus::PersistenceFailed,
            snapshot: visible_snapshot_locked(inner),
            terminal_state: None,
            error: Some(error),
        };
        inner
            .mutations
            .insert(mutation_id, (request, result.clone()));
        Some(result)
    }

    fn admission_closed_result_locked(
        inner: &Inner,
        mutation_id: &RuntimeString,
    ) -> Option<QueueMutationResult> {
        inner.admission_closed.then(|| QueueMutationResult {
            mutation_id: mutation_id.clone(),
            status: QueueMutationStatus::NotAdmitted,
            snapshot: visible_snapshot_locked(inner),
            terminal_state: None,
            error: Some(
                "queue mutation was not admitted because the session is shutting down".into(),
            ),
        })
    }

    /// A slash command is routed through the host's command processor, not sent to
    /// the model as text — so it is excluded from the mid-turn drain.
    pub fn is_slash_command(value: &QueuedValue) -> bool {
        match value {
            QueuedValue::Text(s) => trim_ecmascript(s).starts_with("/"),
            QueuedValue::Blocks(_) => false,
        }
    }
}

fn trim_ecmascript(value: &RuntimeString) -> RuntimeString {
    fn is_trim_unit(unit: u16) -> bool {
        matches!(
            unit,
            0x0009..=0x000d
                | 0x0020
                | 0x00a0
                | 0x1680
                | 0x2000..=0x200a
                | 0x2028
                | 0x2029
                | 0x202f
                | 0x205f
                | 0x3000
                | 0xfeff
        )
    }

    let units = value.as_utf16();
    let start = units
        .iter()
        .position(|unit| !is_trim_unit(*unit))
        .unwrap_or(units.len());
    let end = units
        .iter()
        .rposition(|unit| !is_trim_unit(*unit))
        .map(|index| index + 1)
        .unwrap_or(start);
    value.slice_utf16(start..end)
}

impl Default for MessageQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for MessageQueue {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            mutation_gate: self.mutation_gate.clone(),
            dispatch: self.dispatch.clone(),
            persistence_dispatch: self.persistence_dispatch.clone(),
            durability_dispatch: self.durability_dispatch.clone(),
            durability_notify: self.durability_notify.clone(),
            retained_persistence_tasks: self.retained_persistence_tasks.clone(),
        }
    }
}

impl std::fmt::Debug for MessageQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let len = self
            .inner
            .lock()
            .map(|i| i.last_authoritative_items.len())
            .unwrap_or(0);
        f.debug_struct("MessageQueue").field("len", &len).finish()
    }
}

fn eff_priority(m: &QueuedMessage) -> QueuePriority {
    m.priority.unwrap_or(QueuePriority::Next)
}

/// First index of the highest-priority item (FIFO within priority), or `None`.
fn best_index(
    items: &[Arc<QueuedMessage>],
    filter: Option<&(dyn Fn(&QueuedMessage) -> bool + Sync)>,
) -> Option<usize> {
    let mut best: Option<usize> = None;
    let mut best_rank = u8::MAX;
    for (i, cmd) in items.iter().enumerate() {
        if let Some(f) = filter {
            if !f(cmd) {
                continue;
            }
        }
        let rank = eff_priority(cmd).rank();
        if rank < best_rank {
            best = Some(i);
            best_rank = rank;
        }
    }
    best
}

fn clone_subs(subs: &[(u64, Subscriber)]) -> Vec<Subscriber> {
    subs.iter().map(|(_, cb)| Arc::clone(cb)).collect()
}

fn queued_value_has_document(value: &QueuedValue) -> bool {
    matches!(value, QueuedValue::Blocks(blocks) if blocks.iter().any(|block| matches!(block, ContentBlock::Document(_))))
}

fn queue_mutation_result(
    inner: &Inner,
    mutation_id: RuntimeString,
    status: QueueMutationStatus,
    terminal_state: Option<QueueTerminalState>,
    error: Option<&str>,
) -> QueueMutationResult {
    QueueMutationResult {
        mutation_id,
        status,
        snapshot: visible_snapshot_locked(inner),
        terminal_state,
        error: error.map(Into::into),
    }
}

fn record_mutation_without_revision(
    inner: &mut Inner,
    mutation_id: RuntimeString,
    request: QueueMutationRequest,
    result: QueueMutationResult,
) -> Option<PersistenceDispatch> {
    inner.mutations.insert(mutation_id, (request, result));
    persistence_dispatch_locked(inner)
}

fn replay_mutation(
    inner: &Inner,
    mutation_id: &RuntimeString,
    request: &QueueMutationRequest,
) -> Option<QueueMutationResult> {
    inner
        .mutations
        .get(mutation_id)
        .map(|(prior_request, prior_result)| {
            if prior_request == request {
                prior_result.clone()
            } else {
                queue_mutation_result(
                    inner,
                    mutation_id.clone(),
                    QueueMutationStatus::MutationConflict,
                    None,
                    Some("mutationId was already used for a different queue mutation"),
                )
            }
        })
}

fn current_snapshot_locked(inner: &Inner) -> QueueSnapshot {
    QueueSnapshot {
        revision: inner.revision,
        items: inner
            .items
            .iter()
            .map(|message| {
                let reserved = message
                    .uuid
                    .as_ref()
                    .is_some_and(|uuid| inner.reserved.contains(uuid));
                QueueSnapshotItem {
                    state: if reserved {
                        QueueItemState::Reserved
                    } else {
                        QueueItemState::Pending
                    },
                    message: (**message).clone(),
                    mutable: message.is_meta != Some(true) && !reserved,
                }
            })
            .collect(),
        status: QueueSnapshotStatus::Authoritative,
        error: None,
    }
}

fn visible_snapshot_locked(inner: &Inner) -> QueueSnapshot {
    let mut snapshot = inner.last_authoritative_snapshot.clone();
    if let Some(error) = &inner.persistence_failure {
        snapshot.status = QueueSnapshotStatus::PersistenceFailed;
        snapshot.error = Some(error.clone());
    }
    snapshot
}

fn publish_current_as_authoritative_locked(inner: &mut Inner) {
    inner.last_authoritative_snapshot = current_snapshot_locked(inner);
    inner.last_authoritative_items = inner.items.clone();
}

fn ensure_queue_writable(inner: &Inner) -> Result<(), RuntimeString> {
    if let Some(error) = &inner.persistence_failure {
        return Err(error.clone());
    }
    if inner.admission_closed {
        return Err(RuntimeString::from(
            "queue admission is closed because the session is shutting down",
        ));
    }
    Ok(())
}

fn validate_snapshot_session(
    snapshot: &QueueSnapshot,
    expected_session_id: &RuntimeString,
) -> Result<(), RuntimeCodecError> {
    if snapshot
        .items
        .iter()
        .any(|item| item.message.session_id.as_ref() != Some(expected_session_id))
    {
        return Err(queue_persistence_error(
            "queue mutation snapshot belongs to a different session",
        ));
    }
    Ok(())
}

fn queue_persistence_error(message: &str) -> RuntimeCodecError {
    RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(message.into()))
}

fn persistence_state_locked(inner: &Inner) -> QueuePersistenceState {
    let mut reserved = inner.reserved.iter().cloned().collect::<Vec<_>>();
    reserved.sort();

    let mut tombstones = inner
        .tombstones
        .iter()
        .map(|(uuid, state)| PersistedQueueTombstone {
            uuid: uuid.clone(),
            state: *state,
        })
        .collect::<Vec<_>>();
    tombstones.sort_by(|left, right| left.uuid.cmp(&right.uuid));

    let mut mutations = inner
        .mutations
        .iter()
        .map(|(mutation_id, (request, result))| PersistedQueueMutation {
            mutation_id: mutation_id.clone(),
            request: request.clone(),
            result: result.clone(),
        })
        .collect::<Vec<_>>();
    mutations.sort_by(|left, right| left.mutation_id.cmp(&right.mutation_id));

    QueuePersistenceState {
        schema_version: QUEUE_PERSISTENCE_SCHEMA_VERSION,
        session_id: inner
            .session_id
            .clone()
            .expect("only a session-owned queue may be persisted"),
        revision: inner.revision,
        items: inner
            .items
            .iter()
            .map(|message| PersistedQueueItem {
                message: (**message).clone(),
                pdf_bytes: message.pdf_bytes,
                pdf_documents: message.pdf_documents,
            })
            .collect(),
        reserved,
        resume_reserved: {
            let mut ids: Vec<_> = inner.resume_reserved.iter().cloned().collect();
            ids.sort();
            ids
        },
        tombstones,
        mutations,
    }
}

fn persistence_dispatch_locked(inner: &mut Inner) -> Option<PersistenceDispatch> {
    if inner.persistence_failure.is_some() {
        return None;
    }
    let sink = inner.persistence_sink.clone()?;
    inner.persistence_sequence = inner
        .persistence_sequence
        .checked_add(1)
        .expect("message queue checkpoint sequence overflow");
    let (started_sender, started_receiver) = tokio::sync::oneshot::channel();
    Some(PersistenceDispatch {
        sequence: inner.persistence_sequence,
        sink,
        state: persistence_state_locked(inner),
        started_sender: Some(started_sender),
        started_receiver: Some(started_receiver),
    })
}

fn refresh_change_persistence(inner: &Inner, change: &mut ChangeDispatch) {
    if let Some(persistence) = &mut change.persistence {
        persistence.state = persistence_state_locked(inner);
    }
}

fn reconcile_interrupted_mutations(inner: &mut Inner, interrupted: &HashSet<RuntimeString>) {
    let snapshot = current_snapshot_locked(inner);
    for (request, result) in inner.mutations.values_mut() {
        if result.status == QueueMutationStatus::Busy && request.touches_any(interrupted) {
            result.status = QueueMutationStatus::Missing;
            result.snapshot = snapshot.clone();
            result.terminal_state = Some(QueueTerminalState::Interrupted);
            result.error = Some("queued item was interrupted with its active continuation".into());
        }
    }
}

fn reconcile_resume_mutations(
    inner: &mut Inner,
    touched: &HashSet<RuntimeString>,
    terminal: Option<QueueTerminalState>,
) {
    let snapshot = current_snapshot_locked(inner);
    for (request, result) in inner.mutations.values_mut() {
        if matches!(
            result.status,
            QueueMutationStatus::Busy | QueueMutationStatus::StaleRevision
        ) && request.touches_any(touched)
        {
            result.status = if terminal.is_some() {
                QueueMutationStatus::Missing
            } else {
                QueueMutationStatus::StaleRevision
            };
            result.snapshot = snapshot.clone();
            result.terminal_state = terminal;
            result.error = Some(
                "pending continuation reservation settled; refresh the queue before retrying"
                    .into(),
            );
        }
    }
}

struct ChangeDispatch {
    subscribers: Vec<Subscriber>,
    state_subscribers: Vec<QueueStateSubscriber>,
    snapshot: QueueSnapshot,
    authoritative_items: Vec<Arc<QueuedMessage>>,
    persistence: Option<PersistenceDispatch>,
}

struct DispatchState {
    next_revision: u64,
    pending: BTreeMap<u64, ChangeDispatch>,
    dispatching: bool,
}

struct PersistenceDispatch {
    sequence: u64,
    sink: QueuePersistenceSink,
    state: QueuePersistenceState,
    started_sender: Option<tokio::sync::oneshot::Sender<QueuePersistenceFuture>>,
    started_receiver: Option<tokio::sync::oneshot::Receiver<QueuePersistenceFuture>>,
}

struct PersistenceDispatchState {
    next_sequence: u64,
    pending: BTreeMap<u64, PersistenceDispatch>,
    dispatching: bool,
}

struct StartedPersistence {
    sequence: u64,
    receipt: QueuePersistenceFuture,
}

struct DurabilityCompletion {
    sequence: u64,
    result: Result<(), RuntimeString>,
    action: DurabilityAction,
}

enum DurabilityAction {
    Checkpoint,
    Change {
        change: ChangeDispatch,
        verdict: Option<tokio::sync::oneshot::Sender<Result<(), RuntimeString>>>,
    },
    Mutation {
        result: QueueMutationResult,
        change: Option<ChangeDispatch>,
        response: tokio::sync::oneshot::Sender<QueueMutationResult>,
    },
}

struct DurabilityDispatchState {
    next_sequence: u64,
    pending: BTreeMap<u64, DurabilityCompletion>,
    dispatching: bool,
}

enum RetainedPersistenceTask {
    Tokio(tokio::task::JoinHandle<()>),
    Thread(std::thread::JoinHandle<()>),
}

struct QueueMutationCommit {
    result: QueueMutationResult,
    change: Option<ChangeDispatch>,
    persistence: Option<PersistenceDispatch>,
}

impl QueueMutationCommit {
    fn immediate(result: QueueMutationResult) -> Self {
        Self {
            result,
            change: None,
            persistence: None,
        }
    }

    fn ledger(
        inner: &mut Inner,
        mutation_id: RuntimeString,
        request: QueueMutationRequest,
        result: QueueMutationResult,
    ) -> Self {
        let persistence =
            record_mutation_without_revision(inner, mutation_id, request, result.clone());
        Self {
            result,
            change: None,
            persistence,
        }
    }

    fn applied(result: QueueMutationResult, mut change: ChangeDispatch) -> Self {
        let persistence = change.persistence.take();
        Self {
            result,
            change: Some(change),
            persistence,
        }
    }
}

fn change_dispatch(inner: &mut Inner) -> ChangeDispatch {
    inner.revision = inner
        .revision
        .checked_add(1)
        .expect("message queue revision overflow");
    let snapshot = current_snapshot_locked(inner);
    let persistence = persistence_dispatch_locked(inner);
    ChangeDispatch {
        subscribers: clone_subs(&inner.subscribers),
        state_subscribers: inner
            .state_subscribers
            .iter()
            .map(|(_, subscriber)| subscriber.clone())
            .collect(),
        snapshot,
        authoritative_items: inner.items.clone(),
        persistence,
    }
}

fn fire(subs: Vec<Subscriber>) {
    for cb in subs {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| cb()));
    }
}

/// Convert a queued value into tool_result-compatible output blocks (text/image),
/// for merging into the running turn's last tool_result message. A plain string
/// becomes one text block; content blocks are filtered to the text/image subset an
/// `OutputBlock` allows (thinking/tool_use/tool_result blocks cannot appear inside a
/// tool_result and are dropped).
pub fn queued_value_to_output_blocks(value: &QueuedValue) -> Vec<OutputBlock> {
    match value {
        QueuedValue::Text(s) => {
            if !s.is_empty() {
                vec![OutputBlock::Text(TextBlock {
                    text: s.clone(),
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                })]
            } else {
                Vec::new()
            }
        }
        QueuedValue::Blocks(blocks) => {
            let mut out = Vec::new();
            for b in blocks {
                match b {
                    ContentBlock::Text(t) => out.push(OutputBlock::Text(TextBlock {
                        text: t.text.clone(),
                        citations: None,
                        harness_envelope: None,
                        message_id: None,
                        phase: None,
                    })),
                    ContentBlock::Image(i) => out.push(OutputBlock::Image(ImageBlock {
                        mime_type: i.mime_type.clone(),
                        base64: i.base64.clone(),
                        detail: i.detail,
                        source_path: i.source_path.clone(),
                    })),
                    _ => {}
                }
            }
            out
        }
    }
}

/// Provider-neutral continuation payload. Text can live inside the final tool
/// result; image/document media must remain sibling user blocks so every
/// provider can map it to its native media primitive.
#[derive(Debug, Default, PartialEq)]
pub struct QueuedValueParts {
    pub output_blocks: Vec<OutputBlock>,
    pub extra_user_blocks: Vec<ContentBlock>,
}

/// Convert a queued value into user-message content blocks.
///
/// A steer that lands as its own user message (`SteerDelivery::User`) keeps
/// every block the sender submitted; the tool-result projection above drops
/// what a `tool_result` cannot carry, which is the wrong rule for a message
/// that must read as the user's own words in the transcript.
pub fn queued_value_to_content_blocks(value: &QueuedValue) -> Vec<ContentBlock> {
    match value {
        QueuedValue::Text(text) => vec![ContentBlock::Text(TextBlock {
            text: text.clone(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })],
        QueuedValue::Blocks(blocks) => blocks.clone(),
    }
}

/// Inspect readiness without decoding images or constructing a request view.
pub fn queued_value_has_model_content(value: &QueuedValue) -> bool {
    match value {
        QueuedValue::Text(text) => !text.is_empty(),
        QueuedValue::Blocks(blocks) => blocks.iter().any(|block| {
            matches!(
                block,
                ContentBlock::Text(_)
                    | ContentBlock::Image(_)
                    | ContentBlock::Document(_)
                    | ContentBlock::Media(_)
                    | ContentBlock::ToolResult(_)
            )
        }),
    }
}

pub fn queued_value_to_tool_result_parts(value: &QueuedValue) -> QueuedValueParts {
    match value {
        QueuedValue::Text(text) => QueuedValueParts {
            output_blocks: if !text.is_empty() {
                {
                    vec![OutputBlock::Text(TextBlock {
                        text: text.clone(),
                        citations: None,
                        harness_envelope: None,
                        message_id: None,
                        phase: None,
                    })]
                }
            } else {
                Default::default()
            },
            extra_user_blocks: Vec::new(),
        },
        QueuedValue::Blocks(blocks) => {
            let mut parts = QueuedValueParts::default();
            for block in blocks {
                match block {
                    ContentBlock::Text(text) => {
                        parts.output_blocks.push(OutputBlock::Text(TextBlock {
                            text: text.text.clone(),
                            citations: None,
                            harness_envelope: None,
                            message_id: None,
                            phase: None,
                        }));
                    }
                    ContentBlock::Image(_)
                    | ContentBlock::Document(_)
                    | ContentBlock::Media(_)
                    | ContentBlock::ToolResult(_) => {
                        parts.extra_user_blocks.push(block.clone());
                    }
                    _ => {}
                }
            }
            parts
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::runtime_codec;
    use futures::executor::block_on;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{mpsc, Condvar};
    use std::time::Duration;

    fn msg(mode: &str, priority: Option<QueuePriority>) -> QueuedMessage {
        QueuedMessage {
            trigger_turn: None,
            value: QueuedValue::Text(mode.into()),
            mode: mode.into(),
            priority,
            uuid: None,
            session_id: None,
            is_meta: None,
            origin: None,
            agent_id: None,
            correlation_id: None,
            pdf_bytes: 0,
            pdf_documents: 0,
            delivery: None,
            display_metadata: None,
        }
    }

    #[test]
    fn queue_snapshot_status_codec_is_exact_and_rejects_incoherent_error_shapes() {
        let degraded = QueueSnapshot {
            revision: 7,
            items: Vec::new(),
            status: QueueSnapshotStatus::PersistenceFailed,
            error: Some("disk poison".into()),
        };
        let encoded = runtime_codec::encode_text(&degraded).unwrap();
        assert_eq!(
            encoded.try_to_utf8().unwrap(),
            r#"{"revision":7,"items":[],"status":"persistence-failed","error":"disk poison"}"#
        );
        assert_eq!(
            runtime_codec::decode_text::<QueueSnapshot>(&encoded).unwrap(),
            degraded
        );
        assert!(
            runtime_codec::decode_text::<QueueSnapshot>(&RuntimeString::from(
                r#"{"revision":7,"items":[],"status":"persistence-failed"}"#
            ))
            .is_err()
        );
        assert!(
            runtime_codec::decode_text::<QueueSnapshot>(&RuntimeString::from(
                r#"{"revision":7,"items":[],"status":"authoritative","error":"forbidden"}"#
            ))
            .is_err()
        );
        assert!(
            runtime_codec::decode_text::<QueueSnapshot>(&RuntimeString::from(
                r#"{"revision":7,"items":[],"status":"authoritative","extra":true}"#
            ))
            .is_err()
        );
        let not_admitted = runtime_codec::encode_text(&QueueMutationStatus::NotAdmitted).unwrap();
        assert_eq!(not_admitted.try_to_utf8().unwrap(), r#""not-admitted""#);
        assert_eq!(
            runtime_codec::decode_text::<QueueMutationStatus>(&not_admitted).unwrap(),
            QueueMutationStatus::NotAdmitted
        );
    }

    #[test]
    fn dequeue_respects_priority_then_fifo() {
        let q = MessageQueue::new();
        q.enqueue(msg("a", Some(QueuePriority::Later))).unwrap();
        q.enqueue(msg("b", Some(QueuePriority::Now))).unwrap();
        q.enqueue(msg("c", None)).unwrap(); // default -> next
        q.enqueue(msg("d", Some(QueuePriority::Now))).unwrap();
        // now: b, d ; next: c ; later: a
        assert_eq!(q.dequeue(None).unwrap().unwrap().mode, "b");
        assert_eq!(q.dequeue(None).unwrap().unwrap().mode, "d");
        assert_eq!(q.dequeue(None).unwrap().unwrap().mode, "c");
        assert_eq!(q.dequeue(None).unwrap().unwrap().mode, "a");
        assert!(q.dequeue(None).unwrap().is_none());
    }

    #[test]
    fn dequeue_with_filter() {
        let q = MessageQueue::new();
        q.enqueue(msg("keep", Some(QueuePriority::Now))).unwrap();
        q.enqueue(msg("skip", Some(QueuePriority::Now))).unwrap();
        let pred = |m: &QueuedMessage| m.mode == "skip";
        let got = q.dequeue(Some(&pred)).unwrap().unwrap();
        assert_eq!(got.mode, "skip");
        assert_eq!(q.len(), 1);
    }

    #[test]
    fn enqueue_pending_notification_defaults_later() {
        let q = MessageQueue::new();
        q.enqueue_pending_notification(msg("n", None)).unwrap();
        q.enqueue(msg("m", None)).unwrap();
        // m (next) outranks n (later)
        assert_eq!(q.dequeue(None).unwrap().unwrap().mode, "m");
        assert_eq!(q.dequeue(None).unwrap().unwrap().mode, "n");
    }

    #[test]
    fn dequeue_all_matching_partitions() {
        let q = MessageQueue::new();
        q.enqueue(msg("x", None)).unwrap();
        q.enqueue(msg("y", None)).unwrap();
        q.enqueue(msg("x", None)).unwrap();
        let pred = |m: &QueuedMessage| m.mode == "x";
        let matched = q.dequeue_all_matching(&pred).unwrap();
        assert_eq!(matched.len(), 2);
        assert_eq!(q.len(), 1);
        assert_eq!(q.snapshot()[0].mode, "y");
    }

    #[test]
    fn dequeue_all_matching_never_takes_items_reserved_by_active_continuation() {
        let q = MessageQueue::new();
        q.enqueue(msg("reserved", Some(QueuePriority::Next)))
            .unwrap();
        q.enqueue(msg("pending", Some(QueuePriority::Next)))
            .unwrap();
        let reserved_uuid = q.queue_snapshot().items[0].message.uuid.clone().unwrap();
        let reserved = q
            .reserve_commands_by_max_priority(QueuePriority::Next, &|message| {
                message.uuid.as_ref() == Some(&reserved_uuid)
            })
            .unwrap();
        assert_eq!(reserved.len(), 1);

        let taken = q
            .dequeue_all_matching_with_state(&|_| true, QueueTerminalState::Taken)
            .unwrap();

        assert_eq!(taken.len(), 1);
        assert_eq!(taken[0].mode, "pending");
        let snapshot = q.queue_snapshot();
        assert_eq!(snapshot.items.len(), 1);
        assert_eq!(snapshot.items[0].message.mode, "reserved");
        assert_eq!(snapshot.items[0].state, QueueItemState::Reserved);
    }

    #[test]
    fn remove_by_identity() {
        let q = MessageQueue::new();
        q.enqueue(msg("dup", None)).unwrap();
        q.enqueue(msg("dup", None)).unwrap();
        let all = q.get_commands_by_max_priority(QueuePriority::Later);
        assert_eq!(all.len(), 2);
        // remove only the first instance by identity
        q.remove(&[Arc::clone(&all[0])]).unwrap();
        assert_eq!(q.len(), 1);
        // the survivor is the second instance
        assert!(Arc::ptr_eq(&q.snapshot()[0], &all[1]));
    }

    #[test]
    fn get_commands_by_max_priority_threshold() {
        let q = MessageQueue::new();
        q.enqueue(msg("now", Some(QueuePriority::Now))).unwrap();
        q.enqueue(msg("next", Some(QueuePriority::Next))).unwrap();
        q.enqueue(msg("later", Some(QueuePriority::Later))).unwrap();
        assert_eq!(q.get_commands_by_max_priority(QueuePriority::Now).len(), 1);
        assert_eq!(q.get_commands_by_max_priority(QueuePriority::Next).len(), 2);
        assert_eq!(
            q.get_commands_by_max_priority(QueuePriority::Later).len(),
            3
        );
    }

    #[test]
    fn subscribe_fires_and_unsubscribes() {
        let q = MessageQueue::new();
        let count = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&count);
        let unsub = q.subscribe(Arc::new(move || {
            c.fetch_add(1, Ordering::SeqCst);
        }));
        q.enqueue(msg("a", None)).unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1);
        unsub();
        q.enqueue(msg("b", None)).unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn concurrent_mutations_publish_state_revisions_in_commit_order() {
        let queue = Arc::new(MessageQueue::new());
        let observed = Arc::new(Mutex::new(Vec::new()));
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let observed_by_callback = Arc::clone(&observed);
        let gate_by_callback = Arc::clone(&gate);
        let _unsubscribe = queue.subscribe_state(Arc::new(move |snapshot| {
            observed_by_callback.lock().unwrap().push(snapshot.revision);
            if snapshot.revision == 1 {
                entered_tx.send(()).unwrap();
                let (lock, condition) = &*gate_by_callback;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = condition.wait(released).unwrap();
                }
            }
        }));

        let first_queue = Arc::clone(&queue);
        let first = std::thread::spawn(move || first_queue.enqueue(msg("first", None)));
        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("revision 1 callback entered");

        let second_queue = Arc::clone(&queue);
        let second = std::thread::spawn(move || second_queue.enqueue(msg("second", None)));
        second
            .join()
            .expect("revision 2 mutation completed")
            .unwrap();
        assert_eq!(
            queue.queue_snapshot().revision,
            1,
            "a callback-blocked later revision is not public yet"
        );
        assert_eq!(*observed.lock().unwrap(), vec![1]);

        let (lock, condition) = &*gate;
        *lock.lock().unwrap() = true;
        condition.notify_all();
        first
            .join()
            .expect("revision 1 publisher completed")
            .unwrap();
        assert_eq!(queue.queue_snapshot().revision, 2);
        assert_eq!(*observed.lock().unwrap(), vec![1, 2]);
    }

    #[test]
    fn concurrent_ledger_checkpoint_reaches_sink_before_later_revision_callback() {
        let queue = Arc::new(MessageQueue::for_session("session-ordering".into()));
        let persisted = Arc::new(Mutex::new(Vec::new()));
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let (ledger_entered_tx, ledger_entered_rx) = mpsc::sync_channel(1);
        let persisted_by_sink = Arc::clone(&persisted);
        let gate_by_sink = Arc::clone(&gate);
        queue.set_persistence_sink(Arc::new(move |state| {
            let observed = (state.revision, state.mutations.len(), state.items.len());
            persisted_by_sink.lock().unwrap().push(observed);
            let entered = ledger_entered_tx.clone();
            let gate = gate_by_sink.clone();
            Box::pin(async move {
                if observed == (1, 1, 1) {
                    entered
                        .send(())
                        .expect("ledger sink entry receiver remains live");
                    let (released, condition) = &*gate;
                    let mut released = released.lock().unwrap();
                    while !*released {
                        released = condition.wait(released).unwrap();
                    }
                }
                Ok(())
            })
        }));

        queue
            .enqueue(msg("first", Some(QueuePriority::Next)))
            .unwrap();
        block_on(queue.await_pending_durability()).unwrap();
        let first_uuid = queue.queue_snapshot().items[0]
            .message
            .uuid
            .clone()
            .unwrap();
        assert_eq!(*persisted.lock().unwrap(), vec![(1, 0, 1)]);

        let callback_observations = Arc::new(Mutex::new(Vec::new()));
        let callback_observations_by_subscriber = Arc::clone(&callback_observations);
        let persisted_by_subscriber = Arc::clone(&persisted);
        let _unsubscribe = queue.subscribe_state(Arc::new(move |snapshot| {
            callback_observations_by_subscriber.lock().unwrap().push((
                snapshot.revision,
                persisted_by_subscriber.lock().unwrap().last().copied(),
            ));
        }));

        let stale_queue = Arc::clone(&queue);
        let stale = std::thread::spawn(move || {
            let result = block_on(stale_queue.remove_queued("stale".into(), first_uuid, 0));
            assert_eq!(result.status, QueueMutationStatus::StaleRevision);
        });
        ledger_entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("ledger-only checkpoint reached its slow sink");

        let enqueue_queue = Arc::clone(&queue);
        let enqueue = std::thread::spawn(move || {
            enqueue_queue
                .enqueue(msg("second", Some(QueuePriority::Next)))
                .unwrap();
        });
        enqueue.join().expect("later revision accepted");
        assert_eq!(queue.queue_snapshot().revision, 1);
        assert!(callback_observations.lock().unwrap().is_empty());

        let (released, condition) = &*gate;
        *released.lock().unwrap() = true;
        condition.notify_all();
        stale.join().expect("stale mutation completed");
        block_on(queue.await_pending_durability()).unwrap();
        assert_eq!(queue.queue_snapshot().revision, 2);

        assert_eq!(
            *persisted.lock().unwrap(),
            vec![(1, 0, 1), (1, 1, 1), (2, 1, 2)]
        );
        assert_eq!(
            *callback_observations.lock().unwrap(),
            vec![(2, Some((2, 1, 2)))]
        );
    }

    #[test]
    fn queue_mutations_are_revisioned_idempotent_and_conflict_safe() {
        let q = MessageQueue::new();
        let mut first_message = msg("first", Some(QueuePriority::Next));
        first_message.display_metadata = Some(
            crate::runtime_values::runtime_json::parse(&RuntimeString::from(
                r#"{"displayText":"old composer text"}"#,
            ))
            .unwrap(),
        );
        q.enqueue(first_message).unwrap();
        q.enqueue(msg("second", Some(QueuePriority::Next))).unwrap();
        let initial = q.queue_snapshot();
        assert_eq!(initial.revision, 2);
        let first = initial.items[0].message.uuid.clone().unwrap();
        let second = initial.items[1].message.uuid.clone().unwrap();
        assert_ne!(first, second);
        assert!(initial.items.iter().all(|item| item.mutable));

        let edited = block_on(q.edit_queued(
            "mutation-edit".into(),
            first.clone(),
            initial.revision,
            QueueEdit {
                value: Some(QueuedValue::Text("edited".into())),
                ..Default::default()
            },
        ));
        assert_eq!(edited.status, QueueMutationStatus::Applied);
        assert_eq!(edited.snapshot.revision, 3);
        assert!(edited.snapshot.items[0].message.display_metadata.is_none());
        assert_eq!(
            edited.snapshot.items[0].message.value,
            QueuedValue::Text("edited".into())
        );

        let duplicate = block_on(q.edit_queued(
            "mutation-edit".into(),
            first.clone(),
            initial.revision,
            QueueEdit {
                value: Some(QueuedValue::Text("edited".into())),
                ..Default::default()
            },
        ));
        assert_eq!(duplicate, edited);
        let conflict = block_on(q.remove_queued("mutation-edit".into(), first.clone(), 3));
        assert_eq!(conflict.status, QueueMutationStatus::MutationConflict);
        assert_eq!(q.queue_snapshot().revision, 3);

        let stale = block_on(q.remove_queued("mutation-stale".into(), second.clone(), 2));
        assert_eq!(stale.status, QueueMutationStatus::StaleRevision);
        assert_eq!(stale.snapshot.revision, 3);
    }

    #[test]
    fn reserved_items_are_busy_and_durable_drain_tombstones_are_queryable() {
        let q = MessageQueue::new();
        q.enqueue(msg("first", Some(QueuePriority::Next))).unwrap();
        let uuid = q.queue_snapshot().items[0].message.uuid.clone().unwrap();
        let reserved = q
            .reserve_commands_by_max_priority(QueuePriority::Next, &|_| true)
            .unwrap();
        assert_eq!(reserved.len(), 1);
        assert!(
            q.reserve_commands_by_max_priority(QueuePriority::Next, &|_| true)
                .unwrap()
                .is_empty(),
            "a second overlapping turn must not reserve the same item"
        );
        let snapshot = q.queue_snapshot();
        assert_eq!(snapshot.revision, 2);
        assert_eq!(snapshot.items[0].state, QueueItemState::Reserved);
        assert!(!snapshot.items[0].mutable);
        let busy =
            block_on(q.remove_queued("mutation-busy".into(), uuid.clone(), snapshot.revision));
        assert_eq!(busy.status, QueueMutationStatus::Busy);

        q.remove_with_state(&reserved, QueueTerminalState::Drained)
            .unwrap();
        let missing = block_on(q.remove_queued(
            "mutation-after-drain".into(),
            uuid,
            q.queue_snapshot().revision,
        ));
        assert_eq!(missing.status, QueueMutationStatus::Missing);
        assert_eq!(missing.terminal_state, Some(QueueTerminalState::Drained));
    }

    #[test]
    fn checkpoint_roundtrip_restores_pending_ledger_and_interrupts_reserved_items() {
        let session_id = RuntimeString::from("session-persist");
        let q = MessageQueue::for_session(session_id.clone());
        let checkpoints = Arc::new(Mutex::new(Vec::<QueuePersistenceState>::new()));
        let checkpoints_for_sink = Arc::clone(&checkpoints);
        q.set_persistence_sink(Arc::new(move |state| {
            checkpoints_for_sink.lock().unwrap().push(state);
            Box::pin(async { Ok(()) })
        }));

        let mut reserved_message = msg("reserved", Some(QueuePriority::Next));
        reserved_message.pdf_bytes = 41;
        reserved_message.pdf_documents = 1;
        reserved_message.correlation_id = Some("endpoint-one-request".into());
        q.enqueue(reserved_message).unwrap();
        let mut pending_message = msg("pending", Some(QueuePriority::Later));
        pending_message.pdf_bytes = 73;
        pending_message.pdf_documents = 2;
        pending_message.correlation_id = Some("endpoint-one-pending".into());
        let metadata = crate::runtime_values::runtime_json::parse(&RuntimeString::from(
            r#"{"displayText":"Inspect Orders","browserTabs":[{"tabId":42}]}"#,
        ))
        .unwrap();
        pending_message.display_metadata = Some(metadata.clone());
        q.enqueue(pending_message).unwrap();
        block_on(q.await_pending_durability()).unwrap();

        let initial = q.queue_snapshot();
        let reserved_uuid = initial.items[0].message.uuid.clone().unwrap();
        let pending_uuid = initial.items[1].message.uuid.clone().unwrap();
        let reserved = q
            .reserve_commands_by_max_priority(QueuePriority::Next, &|message| {
                message.uuid.as_ref() == Some(&reserved_uuid)
            })
            .unwrap();
        block_on(q.await_pending_durability()).unwrap();
        assert_eq!(reserved.len(), 1);
        let reserved_revision = q.queue_snapshot().revision;
        let busy = block_on(q.remove_queued(
            "busy-before-restart".into(),
            reserved_uuid.clone(),
            reserved_revision,
        ));
        assert_eq!(busy.status, QueueMutationStatus::Busy);

        let persisted = checkpoints.lock().unwrap().last().cloned().unwrap();
        assert_eq!(persisted.revision, reserved_revision);
        assert_eq!(persisted.mutations.len(), 1);
        let encoded = runtime_codec::encode_value(&persisted).unwrap();
        let decoded: QueuePersistenceState = runtime_codec::decode_value(&encoded).unwrap();
        assert_eq!(decoded.revision, persisted.revision);
        assert_eq!(decoded.reserved, persisted.reserved);
        assert_eq!(decoded.items[0].pdf_bytes, 41);
        assert_eq!(decoded.items[1].pdf_bytes, 73);
        assert_eq!(decoded.items[1].pdf_documents, 2);
        assert_eq!(decoded.items[0].message.correlation_id, None);
        assert_eq!(decoded.items[1].message.correlation_id, None);

        let (restored, normalized) =
            MessageQueue::from_persistence_state(decoded, &session_id).unwrap();
        assert!(normalized);
        let recovered = restored.queue_snapshot();
        assert_eq!(recovered.revision, reserved_revision + 1);
        assert_eq!(recovered.items.len(), 1);
        assert_eq!(
            recovered.items[0].message.uuid.as_ref(),
            Some(&pending_uuid)
        );
        assert_eq!(recovered.items[0].state, QueueItemState::Pending);
        assert_eq!(recovered.items[0].message.pdf_bytes, 73);
        assert_eq!(recovered.items[0].message.pdf_documents, 2);
        assert_eq!(recovered.items[0].message.correlation_id, None);
        assert_eq!(
            recovered.items[0].message.display_metadata.as_ref(),
            Some(&metadata)
        );

        let replay = block_on(restored.remove_queued(
            "busy-before-restart".into(),
            reserved_uuid.clone(),
            reserved_revision,
        ));
        assert_eq!(replay.status, QueueMutationStatus::Missing);
        assert_eq!(replay.terminal_state, Some(QueueTerminalState::Interrupted));
        assert_eq!(replay.snapshot, recovered);

        let missing = block_on(restored.remove_queued(
            "missing-after-restart".into(),
            reserved_uuid,
            recovered.revision,
        ));
        assert_eq!(missing.status, QueueMutationStatus::Missing);
        assert_eq!(
            missing.terminal_state,
            Some(QueueTerminalState::Interrupted)
        );
    }

    #[tokio::test]
    async fn agent_resume_reservation_recovers_to_pending_and_reconciles_history_uuid() {
        let session = RuntimeString::from("agent-resume-recovery");
        let queue = MessageQueue::for_session(session.clone());
        for agent in ["agent-a", "agent-b"] {
            let mut message = msg("subagent-message", Some(QueuePriority::Next));
            message.agent_id = Some(agent.into());
            queue.enqueue(message).unwrap();
        }
        let reserved = queue
            .reserve_agent_resume_durable(&"agent-a".into())
            .await
            .unwrap();
        assert_eq!(reserved.len(), 1);
        assert_eq!(reserved[0].agent_id, Some("agent-a".into()));
        let state = persistence_state_locked(&queue.inner.lock().unwrap());
        let (recovered, _) = MessageQueue::from_persistence_state(state.clone(), &session).unwrap();
        assert_eq!(recovered.queue_snapshot().items.len(), 2);
        assert!(recovered
            .queue_snapshot()
            .items
            .iter()
            .all(|item| item.state == QueueItemState::Pending));
        let applied = [reserved[0].uuid.clone().unwrap()]
            .into_iter()
            .collect::<HashSet<_>>();
        let (recovered, _) =
            MessageQueue::from_persistence_state_with_history(state, &session, &applied).unwrap();
        assert_eq!(recovered.queue_snapshot().items.len(), 1);
        assert_eq!(
            recovered.queue_snapshot().items[0].message.agent_id,
            Some("agent-b".into())
        );
        queue.release_pending_resume(&reserved);
        assert_eq!(
            queue
                .reserve_agent_resume_durable(&"agent-a".into())
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn passive_mailbox_cancellation_releases_and_recovers_its_reserved_result() {
        assert_passive_reservation_cancellation(false).await;
    }

    #[tokio::test]
    async fn passive_tool_boundary_cancellation_releases_and_recovers_its_reserved_result() {
        assert_passive_reservation_cancellation(true).await;
    }

    async fn assert_passive_reservation_cancellation(in_tool_round: bool) {
        let session_id: RuntimeString = "passive-mailbox-cancellation".into();
        let queue = Arc::new(MessageQueue::for_session(session_id.clone()));
        let mut result = msg("task-notification", Some(QueuePriority::Next));
        result.trigger_turn = Some(false);
        queue.enqueue(result).unwrap();
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let sink_entered = entered.clone();
        let sink_release = release.clone();
        queue.set_persistence_sink(Arc::new(move |state| {
            let entered = sink_entered.clone();
            let release = sink_release.clone();
            Box::pin(async move {
                if !state.resume_reserved.is_empty() {
                    entered.add_permits(1);
                    release.acquire().await.unwrap().forget();
                }
                Ok(())
            })
        }));
        let target = queue.clone();
        let task = tokio::spawn(async move {
            if in_tool_round {
                target
                    .reserve_commands_by_max_priority_durable(QueuePriority::Next, &|_| true)
                    .await
            } else {
                target.reserve_passive_mailbox_durable(None).await
            }
        });
        tokio::time::timeout(Duration::from_secs(3), entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        let checkpoint = persistence_state_locked(&queue.inner.lock_recover());
        let (recovered, _) = MessageQueue::from_persistence_state(checkpoint, &session_id).unwrap();
        assert_eq!(
            recovered.queue_snapshot().items[0].state,
            QueueItemState::Pending
        );
        assert!(!recovered.queue_snapshot().items[0].message.triggers_turn());
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(queue.inner.lock_recover().reserved.is_empty());
        release.add_permits(2);
        tokio::time::timeout(Duration::from_secs(3), queue.await_pending_durability())
            .await
            .unwrap()
            .unwrap();
        let next = queue.reserve_passive_mailbox_durable(None).await.unwrap();
        assert_eq!(next.len(), 1);
        queue.release_pending_resume(&next);
        assert!(queue.reserve_pending_resume().unwrap().is_empty());
    }

    #[test]
    fn pending_resume_checkpoint_recovers_unhanded_input_and_retires_persisted_uuids() {
        let session_id = RuntimeString::from("pending-resume-recovery");
        let queue = MessageQueue::for_session(session_id.clone());
        let mut first = msg("first", Some(QueuePriority::Next));
        first.pdf_bytes = 41;
        first.pdf_documents = 1;
        first.origin = Some("fixture".into());
        first.correlation_id = Some("live-steer".into());
        queue.enqueue(first).unwrap();
        queue
            .enqueue(msg("second", Some(QueuePriority::Now)))
            .unwrap();
        let batch = queue.reserve_pending_resume().unwrap();
        let state = persistence_state_locked(&queue.inner.lock().unwrap());
        let encoded = runtime_codec::encode_value(&state).unwrap();
        let decoded: QueuePersistenceState = runtime_codec::decode_value(&encoded).unwrap();
        let (restored, changed) =
            MessageQueue::from_persistence_state(decoded.clone(), &session_id).unwrap();
        assert!(changed);
        let snapshot = restored.queue_snapshot();
        assert_eq!(snapshot.items.len(), 2);
        assert!(snapshot
            .items
            .iter()
            .all(|item| item.state == QueueItemState::Pending && item.mutable));
        assert_eq!(snapshot.items[0].message.pdf_bytes, 41);
        assert_eq!(snapshot.items[0].message.pdf_documents, 1);
        assert_eq!(
            snapshot.items[0].message.origin.as_ref().unwrap(),
            "fixture"
        );
        assert_eq!(
            snapshot.items[0].message.correlation_id, None,
            "old endpoint correlation must not leak into a new endpoint"
        );
        let handed_off: HashSet<_> = [batch[0].uuid.clone().unwrap()].into_iter().collect();
        let (restored, _) =
            MessageQueue::from_persistence_state_with_history(decoded, &session_id, &handed_off)
                .unwrap();
        assert_eq!(restored.queue_snapshot().items.len(), 1);
        assert_eq!(
            restored.queue_snapshot().items[0].message.uuid,
            batch[1].uuid
        );
        let missing = block_on(restored.remove_queued(
            "probe".into(),
            batch[0].uuid.clone().unwrap(),
            restored.queue_snapshot().revision,
        ));
        assert_eq!(missing.terminal_state, Some(QueueTerminalState::Drained));
        queue.release_pending_resume(&batch);
        assert!(queue.queue_snapshot().items.iter().all(|item| item.mutable));
    }

    #[test]
    fn pending_resume_shutdown_keeps_unhanded_batch_pending() {
        let queue = MessageQueue::for_session("resume-shutdown".into());
        queue
            .enqueue(msg("unhanded", Some(QueuePriority::Next)))
            .unwrap();
        let batch = queue.reserve_pending_resume().unwrap();
        queue.interrupt_reserved().unwrap();
        let snapshot = queue.queue_snapshot();
        assert_eq!(snapshot.items.len(), 1);
        assert!(snapshot.items[0].mutable);
        assert_eq!(snapshot.items[0].state, QueueItemState::Pending);
        assert_eq!(snapshot.items[0].message.uuid, batch[0].uuid);
    }

    #[test]
    fn recovery_rejects_an_exhausted_revision_instead_of_panicking() {
        let session_id = RuntimeString::from("session-exhausted");
        let state = QueuePersistenceState {
            schema_version: QUEUE_PERSISTENCE_SCHEMA_VERSION,
            session_id: session_id.clone(),
            revision: u64::MAX,
            items: Vec::new(),
            reserved: Vec::new(),
            resume_reserved: Vec::new(),
            tombstones: Vec::new(),
            mutations: Vec::new(),
        };

        assert!(MessageQueue::from_persistence_state(state, &session_id).is_err());
    }

    #[test]
    fn shutdown_terminalizes_reserved_and_persists_reconciled_idempotency() {
        let session_id = RuntimeString::from("session-shutdown");
        let q = MessageQueue::for_session(session_id.clone());
        let checkpoints = Arc::new(Mutex::new(Vec::<QueuePersistenceState>::new()));
        let checkpoints_for_sink = Arc::clone(&checkpoints);
        q.set_persistence_sink(Arc::new(move |state| {
            checkpoints_for_sink.lock().unwrap().push(state);
            Box::pin(async { Ok(()) })
        }));
        q.enqueue(msg("reserved", Some(QueuePriority::Next)))
            .unwrap();
        block_on(q.await_pending_durability()).unwrap();
        let uuid = q.queue_snapshot().items[0].message.uuid.clone().unwrap();
        let reserved = q
            .reserve_commands_by_max_priority(QueuePriority::Next, &|_| true)
            .unwrap();
        block_on(q.await_pending_durability()).unwrap();
        assert_eq!(reserved.len(), 1);
        let reserved_revision = q.queue_snapshot().revision;
        assert_eq!(
            block_on(q.remove_queued("busy-at-shutdown".into(), uuid.clone(), reserved_revision,))
                .status,
            QueueMutationStatus::Busy
        );

        assert_eq!(q.interrupt_reserved().unwrap(), 1);
        block_on(q.await_pending_durability()).unwrap();
        let settled = q.queue_snapshot();
        assert!(settled.items.is_empty());
        assert_eq!(settled.revision, reserved_revision + 1);
        let replay =
            block_on(q.remove_queued("busy-at-shutdown".into(), uuid.clone(), reserved_revision));
        assert_eq!(replay.status, QueueMutationStatus::Missing);
        assert_eq!(replay.terminal_state, Some(QueueTerminalState::Interrupted));

        let persisted = checkpoints.lock().unwrap().last().cloned().unwrap();
        assert!(persisted.reserved.is_empty());
        assert!(persisted.items.is_empty());
        assert!(persisted.tombstones.iter().any(|tombstone| {
            tombstone.uuid == uuid && tombstone.state == QueueTerminalState::Interrupted
        }));
        let (restored, normalized) =
            MessageQueue::from_persistence_state(persisted, &session_id).unwrap();
        assert!(!normalized, "shutdown already wrote a terminal checkpoint");
        assert_eq!(
            block_on(restored.remove_queued("busy-at-shutdown".into(), uuid, reserved_revision,))
                .terminal_state,
            Some(QueueTerminalState::Interrupted)
        );
    }

    #[test]
    fn reorder_is_cas_ordered_and_rejects_cross_priority_moves() {
        let q = MessageQueue::new();
        q.enqueue(msg("a", Some(QueuePriority::Next))).unwrap();
        q.enqueue(msg("b", Some(QueuePriority::Next))).unwrap();
        q.enqueue(msg("c", Some(QueuePriority::Later))).unwrap();
        let snapshot = q.queue_snapshot();
        let a = snapshot.items[0].message.uuid.clone().unwrap();
        let b = snapshot.items[1].message.uuid.clone().unwrap();
        let c = snapshot.items[2].message.uuid.clone().unwrap();
        let self_target = block_on(q.reorder_queued(
            "mutation-self-target".into(),
            a.clone(),
            Some(a.clone()),
            snapshot.revision,
        ));
        assert_eq!(self_target.status, QueueMutationStatus::Invalid);
        assert_eq!(q.queue_snapshot().revision, snapshot.revision);
        let reordered = block_on(q.reorder_queued(
            "mutation-reorder".into(),
            b.clone(),
            Some(a.clone()),
            snapshot.revision,
        ));
        assert_eq!(reordered.status, QueueMutationStatus::Applied);
        assert_eq!(
            reordered
                .snapshot
                .items
                .iter()
                .map(|item| item.message.uuid.clone().unwrap())
                .collect::<Vec<_>>(),
            vec![b, a.clone(), c.clone()]
        );
        let invalid = block_on(q.reorder_queued(
            "mutation-cross-priority".into(),
            a,
            Some(c),
            reordered.snapshot.revision,
        ));
        assert_eq!(invalid.status, QueueMutationStatus::Invalid);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn host_mutation_waits_for_durability_before_public_change() {
        let queue = Arc::new(MessageQueue::for_session("durable-wait".into()));
        queue
            .enqueue(msg("pending", Some(QueuePriority::Next)))
            .unwrap();
        let initial = queue.queue_snapshot();
        let uuid = initial.items[0].message.uuid.clone().unwrap();

        let receipt_entered = Arc::new(AtomicBool::new(false));
        let release_receipt = Arc::new(tokio::sync::Notify::new());
        let entered_for_sink = receipt_entered.clone();
        let release_for_sink = release_receipt.clone();
        queue.set_persistence_sink(Arc::new(move |state| {
            if state.mutations.is_empty() {
                return Box::pin(async { Ok(()) });
            }
            let entered = entered_for_sink.clone();
            let release = release_for_sink.clone();
            Box::pin(async move {
                entered.store(true, Ordering::SeqCst);
                release.notified().await;
                Ok(())
            })
        }));

        let observed = Arc::new(Mutex::new(Vec::new()));
        let observed_for_subscriber = observed.clone();
        let _unsubscribe = queue.subscribe_state(Arc::new(move |snapshot| {
            observed_for_subscriber
                .lock()
                .unwrap()
                .push(snapshot.revision);
        }));
        let task_queue = queue.clone();
        let mutation = tokio::spawn(async move {
            task_queue
                .edit_queued(
                    "durable-edit".into(),
                    uuid,
                    initial.revision,
                    QueueEdit {
                        value: Some(QueuedValue::Text("edited".into())),
                        ..Default::default()
                    },
                )
                .await
        });

        tokio::time::timeout(Duration::from_secs(2), async {
            while !receipt_entered.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("mutation reached its durability receipt");
        assert!(!mutation.is_finished(), "Applied must wait for durability");
        assert!(
            observed.lock().unwrap().is_empty(),
            "public queue change must wait for durability"
        );

        release_receipt.notify_waiters();
        let result = mutation.await.unwrap();
        assert_eq!(result.status, QueueMutationStatus::Applied);
        assert_eq!(observed.lock().unwrap().as_slice(), &[initial.revision + 1]);
    }

    #[tokio::test]
    async fn persistence_failure_is_outcome_unknown_and_never_reexecutes() {
        let queue = MessageQueue::for_session("durable-failure".into());
        queue
            .enqueue(msg("pending", Some(QueuePriority::Next)))
            .unwrap();
        let initial = queue.queue_snapshot();
        assert_eq!(initial.status, QueueSnapshotStatus::Authoritative);
        assert_eq!(initial.error, None);
        let uuid = initial.items[0].message.uuid.clone().unwrap();
        queue.set_persistence_sink(Arc::new(move |state| {
            Box::pin(async move {
                if state.mutations.is_empty() {
                    Ok(())
                } else {
                    Err(RuntimeString::from("injected rollout writer poison"))
                }
            })
        }));
        let observed = Arc::new(Mutex::new(Vec::<QueueSnapshot>::new()));
        let observed_for_subscriber = observed.clone();
        let _unsubscribe = queue.subscribe_state(Arc::new(move |snapshot| {
            observed_for_subscriber.lock().unwrap().push(snapshot);
        }));

        let first = queue
            .edit_queued(
                "poisoned-edit".into(),
                uuid.clone(),
                initial.revision,
                QueueEdit {
                    value: Some(QueuedValue::Text("edited once".into())),
                    ..Default::default()
                },
            )
            .await;
        assert_eq!(first.status, QueueMutationStatus::OutcomeUnknown);
        assert_eq!(first.snapshot.revision, initial.revision);
        assert_eq!(
            first.snapshot.status,
            QueueSnapshotStatus::PersistenceFailed
        );
        assert_eq!(
            first.snapshot.error,
            Some(RuntimeString::from("injected rollout writer poison"))
        );
        assert_eq!(first.snapshot.items, initial.items);
        assert_eq!(
            observed.lock().unwrap().as_slice(),
            std::slice::from_ref(&first.snapshot)
        );

        let degraded = queue.queue_snapshot();
        assert_eq!(degraded, first.snapshot);
        assert_eq!(
            queue.snapshot()[0].value,
            QueuedValue::Text("pending".into())
        );
        assert_eq!(
            queue.peek(None).unwrap().value,
            QueuedValue::Text("pending".into())
        );
        assert_eq!(
            queue.get_commands_by_max_priority(QueuePriority::Later)[0].value,
            QueuedValue::Text("pending".into())
        );

        let replay = queue
            .edit_queued(
                "poisoned-edit".into(),
                uuid.clone(),
                initial.revision,
                QueueEdit {
                    value: Some(QueuedValue::Text("edited once".into())),
                    ..Default::default()
                },
            )
            .await;
        assert_eq!(replay, first);
        let later = queue
            .remove_queued("after-poison".into(), uuid.clone(), initial.revision)
            .await;
        assert_eq!(later.status, QueueMutationStatus::PersistenceFailed);
        assert_eq!(later.snapshot, degraded);
        assert_eq!(later.error, degraded.error);

        let authoritative_item = queue.snapshot()[0].clone();
        let expected_error = RuntimeString::from("injected rollout writer poison");
        assert_eq!(
            queue
                .enqueue(msg("after failure", Some(QueuePriority::Next)))
                .unwrap_err(),
            expected_error
        );
        assert_eq!(
            queue
                .enqueue_pending_notification(msg("late", None))
                .unwrap_err(),
            expected_error
        );
        assert_eq!(queue.dequeue(None).unwrap_err(), expected_error);
        assert_eq!(queue.dequeue_all().unwrap_err(), expected_error);
        assert_eq!(
            queue.dequeue_all_matching(&|_| true).unwrap_err(),
            expected_error
        );
        assert_eq!(
            queue
                .dequeue_all_matching_with_state(&|_| true, QueueTerminalState::Taken)
                .unwrap_err(),
            expected_error
        );
        assert_eq!(
            queue
                .remove(std::slice::from_ref(&authoritative_item))
                .unwrap_err(),
            expected_error
        );
        assert_eq!(
            queue
                .remove_with_state(&[authoritative_item], QueueTerminalState::Drained)
                .unwrap_err(),
            expected_error
        );
        assert_eq!(
            queue
                .reserve_commands_by_max_priority(QueuePriority::Later, &|_| true)
                .unwrap_err(),
            expected_error
        );
        assert_eq!(queue.interrupt_reserved().unwrap_err(), expected_error);
        assert_eq!(queue.clear().unwrap_err(), expected_error);
        assert_eq!(
            queue.dequeue_durable(None).await.unwrap_err(),
            expected_error
        );

        assert_eq!(queue.queue_snapshot(), degraded);
        assert_eq!(observed.lock().unwrap().len(), 1);

        let dispatch = queue.dispatch.lock().unwrap();
        assert!(
            dispatch.pending.is_empty(),
            "failed and post-poison revisions must be retired instead of accumulating"
        );
        assert!(!dispatch.dispatching);
        assert_eq!(dispatch.next_revision, initial.revision + 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retained_non_host_receipts_poison_in_sequence_and_fail_later_durable_work() {
        let queue = Arc::new(MessageQueue::for_session("retained-non-host".into()));
        let first_entered = Arc::new(AtomicBool::new(false));
        let release_first = Arc::new(tokio::sync::Notify::new());
        let first_entered_for_sink = first_entered.clone();
        let release_first_for_sink = release_first.clone();
        queue.set_persistence_sink(Arc::new(move |state| {
            if state.revision == 1 {
                let entered = first_entered_for_sink.clone();
                let release = release_first_for_sink.clone();
                Box::pin(async move {
                    entered.store(true, Ordering::SeqCst);
                    release.notified().await;
                    Err(RuntimeString::from("first checkpoint failed"))
                })
            } else {
                Box::pin(async { Ok(()) })
            }
        }));

        let observed = Arc::new(Mutex::new(Vec::<QueueSnapshot>::new()));
        let observed_for_subscriber = observed.clone();
        let _unsubscribe = queue.subscribe_state(Arc::new(move |snapshot| {
            observed_for_subscriber.lock().unwrap().push(snapshot);
        }));

        queue.enqueue(msg("first", None)).unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !first_entered.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first retained receipt started");
        assert_eq!(queue.queue_snapshot().revision, 0);

        let later_queue = queue.clone();
        let later =
            tokio::spawn(async move { later_queue.enqueue_durable(msg("second", None)).await });
        tokio::task::yield_now().await;
        assert!(
            !later.is_finished(),
            "a later raw success must wait for the earlier sequence verdict"
        );

        release_first.notify_waiters();
        assert_eq!(
            later.await.unwrap().unwrap_err(),
            RuntimeString::from("first checkpoint failed")
        );
        assert_eq!(
            queue.await_pending_durability().await.unwrap_err(),
            RuntimeString::from("first checkpoint failed")
        );
        let degraded = queue.queue_snapshot();
        assert_eq!(degraded.revision, 0);
        assert!(degraded.items.is_empty());
        assert_eq!(degraded.status, QueueSnapshotStatus::PersistenceFailed);
        assert_eq!(
            degraded.error,
            Some(RuntimeString::from("first checkpoint failed"))
        );
        assert_eq!(observed.lock().unwrap().as_slice(), &[degraded]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn persistence_drain_closes_admission_and_waits_every_prior_receipt() {
        let queue = Arc::new(MessageQueue::for_session("shutdown-admission".into()));
        let receipt_entered = Arc::new(AtomicBool::new(false));
        let release_receipt = Arc::new(tokio::sync::Notify::new());
        let entered_for_sink = receipt_entered.clone();
        let release_for_sink = release_receipt.clone();
        queue.set_persistence_sink(Arc::new(move |_| {
            let entered = entered_for_sink.clone();
            let release = release_for_sink.clone();
            Box::pin(async move {
                entered.store(true, Ordering::SeqCst);
                release.notified().await;
                Ok(())
            })
        }));

        queue
            .enqueue(msg("accepted before close", None))
            .expect("the pre-close producer is admitted");
        tokio::time::timeout(Duration::from_secs(2), async {
            while !receipt_entered.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("pre-close durability receipt started");

        let draining_queue = queue.clone();
        let drain = tokio::spawn(async move { draining_queue.drain_persistence_tasks().await });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !queue.admission_is_closed() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("drain closed queue admission");

        assert!(!drain.is_finished());
        let error = queue
            .enqueue(msg("rejected after close", None))
            .expect_err("post-close producer must fail before queue mutation");
        assert_eq!(
            error,
            RuntimeString::from("queue admission is closed because the session is shutting down")
        );

        release_receipt.notify_one();
        drain.await.unwrap().unwrap();
        let snapshot = queue.queue_snapshot();
        assert_eq!(snapshot.status, QueueSnapshotStatus::Authoritative);
        assert_eq!(snapshot.revision, 1);
        assert_eq!(snapshot.items.len(), 1);
        assert_eq!(
            snapshot.items[0].message.value,
            QueuedValue::Text("accepted before close".into())
        );
        let uuid = snapshot.items[0].message.uuid.clone().unwrap();
        let edit = queue
            .edit_queued(
                "late-edit".into(),
                uuid.clone(),
                snapshot.revision,
                QueueEdit {
                    value: Some(QueuedValue::Text("must not execute".into())),
                    ..Default::default()
                },
            )
            .await;
        let remove = queue
            .remove_queued("late-remove".into(), uuid.clone(), snapshot.revision)
            .await;
        let reorder = queue
            .reorder_queued("late-reorder".into(), uuid, None, snapshot.revision)
            .await;
        for result in [edit, remove, reorder] {
            assert_eq!(result.status, QueueMutationStatus::NotAdmitted);
            assert_eq!(result.snapshot, snapshot);
            assert_eq!(
                result.error,
                Some(RuntimeString::from(
                    "queue mutation was not admitted because the session is shutting down"
                ))
            );
        }
        assert_eq!(queue.queue_snapshot(), snapshot);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prior_receipt_failure_marks_an_already_executed_later_mutation_outcome_unknown() {
        let queue = Arc::new(MessageQueue::for_session("ordered-host-failure".into()));
        queue.enqueue(msg("durable original", None)).unwrap();
        let initial = queue.queue_snapshot();
        let uuid = initial.items[0].message.uuid.clone().unwrap();

        let sink_calls = Arc::new(AtomicUsize::new(0));
        let first_entered = Arc::new(AtomicBool::new(false));
        let release_first = Arc::new(tokio::sync::Notify::new());
        let calls_for_sink = sink_calls.clone();
        let entered_for_sink = first_entered.clone();
        let release_for_sink = release_first.clone();
        queue.set_persistence_sink(Arc::new(move |_| {
            let call = calls_for_sink.fetch_add(1, Ordering::SeqCst) + 1;
            if call == 1 {
                let entered = entered_for_sink.clone();
                let release = release_for_sink.clone();
                Box::pin(async move {
                    entered.store(true, Ordering::SeqCst);
                    release.notified().await;
                    Err(RuntimeString::from("earlier checkpoint failed"))
                })
            } else {
                Box::pin(async { Ok(()) })
            }
        }));

        // This retained checkpoint has no revision change, so the Host's
        // current revision remains valid while its receipt is still pending.
        queue.persist_current_state().unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !first_entered.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("earlier checkpoint receipt started");

        let mutation_queue = queue.clone();
        let mutation_uuid = uuid.clone();
        let mutation = tokio::spawn(async move {
            mutation_queue
                .edit_queued(
                    "already-executed-edit".into(),
                    mutation_uuid,
                    initial.revision,
                    QueueEdit {
                        value: Some(QueuedValue::Text("speculative edit".into())),
                        ..Default::default()
                    },
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while sink_calls.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("later mutation executed and submitted its own receipt");
        assert!(!mutation.is_finished());

        release_first.notify_waiters();
        let result = mutation.await.unwrap();
        assert_eq!(result.status, QueueMutationStatus::OutcomeUnknown);
        assert_eq!(
            result.error,
            Some(RuntimeString::from("earlier checkpoint failed"))
        );
        assert_eq!(result.snapshot.revision, initial.revision);
        assert_eq!(result.snapshot.items, initial.items);
        assert_eq!(
            result.snapshot.status,
            QueueSnapshotStatus::PersistenceFailed
        );

        let later = queue
            .remove_queued("not-executed-after-poison".into(), uuid, initial.revision)
            .await;
        assert_eq!(later.status, QueueMutationStatus::PersistenceFailed);
        assert_eq!(later.snapshot, result.snapshot);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_mutation_results_follow_durable_sequence() {
        let queue = Arc::new(MessageQueue::for_session("durable-order".into()));
        queue
            .enqueue(msg("first", Some(QueuePriority::Next)))
            .unwrap();
        queue
            .enqueue(msg("second", Some(QueuePriority::Next)))
            .unwrap();
        let initial = queue.queue_snapshot();
        let first_uuid = initial.items[0].message.uuid.clone().unwrap();
        let second_uuid = initial.items[1].message.uuid.clone().unwrap();
        let entered = Arc::new(AtomicUsize::new(0));
        let releases = Arc::new([tokio::sync::Notify::new(), tokio::sync::Notify::new()]);
        let entered_for_sink = entered.clone();
        let releases_for_sink = releases.clone();
        queue.set_persistence_sink(Arc::new(move |state| {
            let mutation_count = state.mutations.len();
            if mutation_count == 0 {
                return Box::pin(async { Ok(()) });
            }
            let entered = entered_for_sink.clone();
            let releases = releases_for_sink.clone();
            Box::pin(async move {
                entered.store(mutation_count, Ordering::SeqCst);
                releases[mutation_count - 1].notified().await;
                Ok(())
            })
        }));
        let observed = Arc::new(Mutex::new(Vec::new()));
        let observed_for_subscriber = observed.clone();
        let _unsubscribe = queue.subscribe_state(Arc::new(move |snapshot| {
            observed_for_subscriber
                .lock()
                .unwrap()
                .push(snapshot.revision);
        }));

        let first_queue = queue.clone();
        let first = tokio::spawn(async move {
            first_queue
                .remove_queued("durable-first".into(), first_uuid, initial.revision)
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while entered.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let second_queue = queue.clone();
        let second = tokio::spawn(async move {
            second_queue
                .remove_queued("durable-second".into(), second_uuid, initial.revision + 1)
                .await
        });
        assert!(!second.is_finished());
        releases[0].notify_waiters();
        let first_result = first.await.unwrap();
        assert_eq!(first_result.status, QueueMutationStatus::Applied);
        tokio::time::timeout(Duration::from_secs(2), async {
            while entered.load(Ordering::SeqCst) != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!second.is_finished());
        assert_eq!(observed.lock().unwrap().as_slice(), &[initial.revision + 1]);
        releases[1].notify_waiters();
        let second_result = second.await.unwrap();
        assert_eq!(second_result.status, QueueMutationStatus::Applied);
        assert_eq!(
            observed.lock().unwrap().as_slice(),
            &[initial.revision + 1, initial.revision + 2]
        );
    }

    #[test]
    fn is_slash_command_only_for_leading_slash_text() {
        assert!(MessageQueue::is_slash_command(&QueuedValue::Text(
            "  /help".into()
        )));
        assert!(!MessageQueue::is_slash_command(&QueuedValue::Text(
            "hello".into()
        )));
        assert!(!MessageQueue::is_slash_command(&QueuedValue::Blocks(
            vec![]
        )));
    }

    #[test]
    fn queued_value_to_output_blocks_filters() {
        assert_eq!(
            queued_value_to_output_blocks(&QueuedValue::Text("".into())).len(),
            0
        );
        let blocks = queued_value_to_output_blocks(&QueuedValue::Text("hi".into()));
        assert_eq!(blocks.len(), 1);
        let mixed = QueuedValue::Blocks(vec![
            ContentBlock::Text(TextBlock {
                text: "t".into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            }),
            ContentBlock::Image(ImageBlock {
                mime_type: "image/png".into(),
                base64: "AAA".into(),
                detail: None,
                source_path: None,
            }),
            ContentBlock::RedactedThinking(crate::schemas::content::RedactedThinkingBlock {
                data: "x".into(),
            }),
        ]);
        let out = queued_value_to_output_blocks(&mixed);
        assert_eq!(out.len(), 2); // the redacted-thinking block is dropped
    }

    #[test]
    fn queued_value_to_tool_result_parts_preserves_media_as_sibling_blocks() {
        let value = QueuedValue::Blocks(vec![
            ContentBlock::Text(TextBlock {
                text: "look at both".into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            }),
            ContentBlock::Image(ImageBlock {
                mime_type: "image/png".into(),
                base64: "AAAA".into(),
                detail: None,
                source_path: None,
            }),
            ContentBlock::Document(crate::schemas::content::DocumentBlock {
                mime_type: "application/pdf".into(),
                base64: "JVBERi0xLjcK".into(),
                filename: Some("report.pdf".into()),
            }),
        ]);

        let parts = queued_value_to_tool_result_parts(&value);

        assert_eq!(parts.output_blocks.len(), 1);
        assert_eq!(parts.extra_user_blocks.len(), 2);
        assert!(matches!(
            &parts.extra_user_blocks[0],
            ContentBlock::Image(_)
        ));
        assert!(matches!(
            &parts.extra_user_blocks[1],
            ContentBlock::Document(_)
        ));
    }
}
