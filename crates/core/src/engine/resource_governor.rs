//! Tracks independent execution and residency capacity for a shared child tree.
//! Execution permits follow running-task lifetimes. Completed children remain
//! resident until the owning session explicitly unloads them.

use crate::sync_ext::MutexExt;
use crate::types::abort::AbortSignal;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

#[derive(Debug, Clone, Default)]
pub struct ResourceGovernorOptions {
    /// Legacy child-only capacity, used when the total thread limit is absent.
    pub max_concurrent_sub_agents: Option<usize>,
    /// Total thread capacity, including the root thread's reserved place.
    pub max_concurrent_threads_per_session: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotAdmissionError {
    Cancelled,
    ExecutionCapacity { maximum: usize },
    ResidencyCapacity { maximum: usize },
}

impl std::fmt::Display for SlotAdmissionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cancelled => formatter.write_str("Sub-agent execution was interrupted."),
            Self::ExecutionCapacity { maximum } => write!(
                formatter,
                "Sub-agent execution capacity reached (maximum {maximum}). Wait for an active sub-agent to finish before spawning another."
            ),
            Self::ResidencyCapacity { maximum } => write!(
                formatter,
                "Sub-agent residency capacity reached (maximum {maximum})."
            ),
        }
    }
}

impl std::error::Error for SlotAdmissionError {}

#[derive(Default)]
struct ExecutionLimiter {
    active: AtomicUsize,
    maximum: OnceLock<usize>,
}

impl ExecutionLimiter {
    fn initialize(&self, maximum: usize) {
        self.maximum.get_or_init(|| maximum);
    }

    fn capacity(&self) -> usize {
        self.maximum.get().copied().unwrap_or(usize::MAX)
    }
}

#[derive(Default)]
struct ResidencyState {
    residents: VecDeque<String>,
    pending_slots: usize,
}

#[derive(Clone)]
pub struct ResourceGovernor {
    execution: Arc<ExecutionLimiter>,
    residency: Arc<Mutex<ResidencyState>>,
}

impl ResourceGovernor {
    pub fn new(opts: ResourceGovernorOptions) -> Self {
        let capacity = opts
            .max_concurrent_threads_per_session
            .map(|total| total.saturating_sub(1))
            .or(opts.max_concurrent_sub_agents)
            .unwrap_or(3);
        let execution = Arc::new(ExecutionLimiter::default());
        execution.initialize(capacity);
        Self {
            execution,
            residency: Arc::new(Mutex::new(ResidencyState::default())),
        }
    }

    /// Check current execution capacity without reserving a permit.
    /// The caller registers the permit separately when its running task starts.
    pub fn check_execution_capacity(
        &self,
        signal: Option<&AbortSignal>,
    ) -> Result<(), SlotAdmissionError> {
        if signal.is_some_and(AbortSignal::is_cancelled) {
            return Err(SlotAdmissionError::Cancelled);
        }
        let maximum = self.capacity();
        if self.execution.active.load(Ordering::Acquire) >= maximum {
            return Err(SlotAdmissionError::ExecutionCapacity { maximum });
        }
        Ok(())
    }

    /// Register a running child after admission, without checking capacity again.
    /// Its permit stays held while that child waits for descendants or messages.
    pub fn admit_turn(&self) -> SlotRelease {
        self.execution.active.fetch_add(1, Ordering::AcqRel);
        SlotRelease {
            execution: self.execution.clone(),
            released: AtomicBool::new(false),
        }
    }

    /// Compatibility entry point for callers that still check and register together.
    #[cfg(test)]
    pub fn acquire_slot(
        &self,
        signal: Option<&AbortSignal>,
    ) -> Result<SlotRelease, SlotAdmissionError> {
        self.check_execution_capacity(signal)?;
        Ok(self.admit_turn())
    }

    /// Child execution capacity fixed by the root manager's first initialization.
    pub fn capacity(&self) -> usize {
        self.execution.capacity()
    }

    /// Reserve one pending resident using this admission's effective capacity.
    /// A full pool returns control to the session so it can attempt eviction.
    pub fn try_reserve_pending_slot(&self, capacity: usize) -> Option<ResidencySlot> {
        let mut state = self.residency.lock_recover();
        if state.residents.len().saturating_add(state.pending_slots) >= capacity {
            return None;
        }
        state.pending_slots += 1;
        Some(ResidencySlot {
            residency: self.residency.clone(),
            active: true,
        })
    }

    /// Reserve with the root execution limit when no per-admission override exists.
    #[cfg(test)]
    pub fn try_reserve_default_residency_slot(&self) -> Option<ResidencySlot> {
        self.try_reserve_pending_slot(self.capacity())
    }

    /// Resident identities in least-recently-used order.
    pub fn resident_ids(&self) -> Vec<String> {
        self.residency
            .lock_recover()
            .residents
            .iter()
            .cloned()
            .collect()
    }

    #[cfg(test)]
    pub fn contains_resident(&self, id: &str) -> bool {
        self.residency
            .lock_recover()
            .residents
            .iter()
            .any(|resident| resident == id)
    }

    /// Record a loaded resident as most recently used.
    pub fn touch_resident(&self, id: &str) {
        touch_resident(&mut self.residency.lock_recover().residents, id.to_owned());
    }

    /// Forget an unloaded resident without changing pending or execution counts.
    pub fn remove_resident(&self, id: &str) {
        self.residency
            .lock_recover()
            .residents
            .retain(|resident| resident != id);
    }

    #[cfg(test)]
    pub fn active_count(&self) -> usize {
        self.execution.active.load(Ordering::Acquire)
    }
}

impl Default for ResourceGovernor {
    fn default() -> Self {
        Self::new(ResourceGovernorOptions::default())
    }
}

impl std::fmt::Debug for ResourceGovernor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.residency.lock_recover();
        formatter
            .debug_struct("ResourceGovernor")
            .field("active", &self.execution.active.load(Ordering::Acquire))
            .field("capacity", &self.capacity())
            .field("residents", &state.residents)
            .field("pending_slots", &state.pending_slots)
            .finish()
    }
}

/// An idempotent release owned by the admitted running task's lifetime guard.
pub struct SlotRelease {
    execution: Arc<ExecutionLimiter>,
    released: AtomicBool,
}

impl SlotRelease {
    pub fn release(&self) {
        if self.released.swap(true, Ordering::AcqRel) {
            return;
        }
        self.execution.active.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Drop for SlotRelease {
    fn drop(&mut self) {
        self.release();
    }
}

/// One pending residency reservation, returned automatically if admission stops.
pub struct ResidencySlot {
    residency: Arc<Mutex<ResidencyState>>,
    active: bool,
}

impl ResidencySlot {
    /// Publish one loaded resident and consume this pending reservation.
    pub fn commit(mut self, id: impl Into<String>) {
        let mut state = self.residency.lock_recover();
        state.pending_slots = state.pending_slots.saturating_sub(1);
        touch_resident(&mut state.residents, id.into());
        self.active = false;
    }

    #[cfg(test)]
    pub fn cancel(self) {
        drop(self);
    }
}

impl Drop for ResidencySlot {
    fn drop(&mut self) {
        if self.active {
            let mut state = self.residency.lock_recover();
            state.pending_slots = state.pending_slots.saturating_sub(1);
        }
    }
}

fn touch_resident(residents: &mut VecDeque<String>, id: String) {
    residents.retain(|resident| resident != &id);
    residents.push_back(id);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn governor(capacity: usize) -> ResourceGovernor {
        ResourceGovernor::new(ResourceGovernorOptions {
            max_concurrent_sub_agents: Some(capacity),
            ..Default::default()
        })
    }

    #[test]
    fn total_thread_limit_reserves_one_place_for_the_root() {
        assert_eq!(ResourceGovernor::default().capacity(), 3);
        assert_eq!(governor(7).capacity(), 7);
        let configured = ResourceGovernor::new(ResourceGovernorOptions {
            max_concurrent_sub_agents: Some(7),
            max_concurrent_threads_per_session: Some(5),
        });
        assert_eq!(configured.capacity(), 4);
    }

    #[test]
    fn execution_limit_keeps_its_first_initialization_across_clones() {
        let governor = governor(2);
        let descendant = governor.clone();
        descendant.execution.initialize(8);
        assert_eq!(governor.capacity(), 2);
        assert_eq!(descendant.capacity(), 2);
    }

    #[test]
    fn execution_checks_do_not_reserve_running_permits() {
        let governor = governor(1);
        assert_eq!(governor.check_execution_capacity(None), Ok(()));
        assert_eq!(governor.check_execution_capacity(None), Ok(()));
        assert_eq!(governor.active_count(), 0);
        let running = governor.admit_turn();
        assert_eq!(
            governor.check_execution_capacity(None),
            Err(SlotAdmissionError::ExecutionCapacity { maximum: 1 })
        );
        running.release();
        assert_eq!(governor.check_execution_capacity(None), Ok(()));
    }

    #[test]
    fn running_task_registration_does_not_repeat_the_capacity_check() {
        let governor = governor(1);
        assert_eq!(governor.check_execution_capacity(None), Ok(()));
        assert_eq!(governor.check_execution_capacity(None), Ok(()));
        let first = governor.admit_turn();
        let second = governor.admit_turn();
        assert_eq!(governor.active_count(), 2);
        drop(first);
        drop(second);
        assert_eq!(governor.active_count(), 0);
    }

    #[test]
    fn release_and_drop_balance_execution_exactly_once() {
        let governor = governor(2);
        let first = governor.acquire_slot(None).unwrap();
        let second = governor.acquire_slot(None).unwrap();
        assert_eq!(governor.active_count(), 2);
        first.release();
        first.release();
        drop(first);
        assert_eq!(governor.active_count(), 1);
        drop(second);
        assert_eq!(governor.active_count(), 0);
    }

    #[test]
    fn full_execution_pool_rejects_without_queueing_or_releasing_a_waiting_parent() {
        let governor = governor(1);
        let parent = governor.admit_turn();
        assert!(matches!(
            governor.acquire_slot(None),
            Err(SlotAdmissionError::ExecutionCapacity { maximum: 1 })
        ));
        assert_eq!(governor.active_count(), 1);
        assert!(governor.resident_ids().is_empty());
        drop(parent);
        assert!(governor.acquire_slot(None).is_ok());
        assert_eq!(governor.active_count(), 0);
    }

    #[test]
    fn cancelled_execution_check_does_not_register_a_permit() {
        let governor = governor(0);
        let signal = AbortSignal::new();
        signal.cancel();
        assert_eq!(
            governor.check_execution_capacity(Some(&signal)),
            Err(SlotAdmissionError::Cancelled)
        );
        assert!(matches!(
            governor.acquire_slot(Some(&signal)),
            Err(SlotAdmissionError::Cancelled)
        ));
        assert_eq!(governor.active_count(), 0);
    }

    #[test]
    fn pending_reservations_count_against_residency_capacity() {
        let governor = governor(2);
        let first = governor.try_reserve_pending_slot(2).unwrap();
        let second = governor.try_reserve_pending_slot(2).unwrap();
        assert!(governor.try_reserve_pending_slot(2).is_none());
        assert_eq!(governor.residency.lock_recover().pending_slots, 2);
        assert_eq!(governor.active_count(), 0);
        first.commit("first");
        assert!(governor.try_reserve_pending_slot(2).is_none());
        second.commit("second");
        assert_eq!(governor.resident_ids(), vec!["first", "second"]);
        assert_eq!(governor.residency.lock_recover().pending_slots, 0);
    }

    #[test]
    fn dropped_and_cancelled_pending_reservations_restore_capacity() {
        let governor = governor(1);
        let first = governor.try_reserve_pending_slot(1).unwrap();
        assert!(governor.try_reserve_pending_slot(1).is_none());
        drop(first);
        assert_eq!(governor.residency.lock_recover().pending_slots, 0);
        let second = governor.try_reserve_pending_slot(1).unwrap();
        second.cancel();
        assert_eq!(governor.residency.lock_recover().pending_slots, 0);
        assert!(governor.try_reserve_pending_slot(1).is_some());
        assert_eq!(governor.residency.lock_recover().pending_slots, 0);
        assert!(governor.resident_ids().is_empty());
    }

    #[test]
    fn commit_consumes_pending_and_moves_a_duplicate_resident_to_the_back() {
        let governor = governor(3);
        governor
            .try_reserve_pending_slot(3)
            .unwrap()
            .commit("first");
        governor
            .try_reserve_pending_slot(3)
            .unwrap()
            .commit("second");
        governor
            .try_reserve_pending_slot(3)
            .unwrap()
            .commit("first");
        assert_eq!(governor.resident_ids(), vec!["second", "first"]);
        assert_eq!(governor.residency.lock_recover().pending_slots, 0);
        assert!(governor.try_reserve_pending_slot(3).is_some());
    }

    #[test]
    fn touch_preserves_lru_order_and_remove_frees_only_the_named_resident() {
        let governor = governor(3);
        for id in ["first", "second", "third"] {
            governor.try_reserve_pending_slot(3).unwrap().commit(id);
        }
        governor.touch_resident("first");
        assert_eq!(governor.resident_ids(), vec!["second", "third", "first"]);
        governor.touch_resident("first");
        assert_eq!(governor.resident_ids(), vec!["second", "third", "first"]);
        assert!(governor.contains_resident("third"));
        assert!(!governor.contains_resident("missing"));
        governor.remove_resident("third");
        governor.remove_resident("missing");
        assert_eq!(governor.resident_ids(), vec!["second", "first"]);
        assert!(!governor.contains_resident("third"));
        assert!(governor.try_reserve_pending_slot(3).is_some());
    }

    #[test]
    fn execution_completion_retains_residency_until_explicit_removal() {
        let governor = governor(1);
        governor
            .try_reserve_pending_slot(1)
            .unwrap()
            .commit("child");
        let running = governor.admit_turn();
        drop(running);
        assert_eq!(governor.active_count(), 0);
        assert_eq!(governor.check_execution_capacity(None), Ok(()));
        assert_eq!(governor.resident_ids(), vec!["child"]);
        assert!(governor.try_reserve_pending_slot(1).is_none());
        governor.remove_resident("child");
        assert!(governor.try_reserve_pending_slot(1).is_some());
    }

    #[test]
    fn residency_capacity_is_selected_per_reservation_without_changing_execution_limit() {
        let governor = governor(1);
        governor
            .try_reserve_pending_slot(2)
            .unwrap()
            .commit("first");
        assert!(governor.try_reserve_pending_slot(1).is_none());
        let pending = governor.try_reserve_pending_slot(2).unwrap();
        assert!(governor.try_reserve_pending_slot(2).is_none());
        pending.commit("second");
        assert_eq!(governor.capacity(), 1);
        assert_eq!(governor.resident_ids(), vec!["first", "second"]);
        assert!(governor.try_reserve_pending_slot(1).is_none());
        assert!(governor.try_reserve_pending_slot(3).is_some());
    }

    #[test]
    fn cloned_managers_share_execution_pending_and_residents() {
        let root = governor(2);
        let child = root.clone();
        let grandchild = child.clone();
        assert!(Arc::ptr_eq(&root.execution, &grandchild.execution));
        assert!(Arc::ptr_eq(&root.residency, &grandchild.residency));
        let running = child.admit_turn();
        assert_eq!(root.active_count(), 1);
        let first = root.try_reserve_pending_slot(2).unwrap();
        let second = grandchild.try_reserve_pending_slot(2).unwrap();
        assert!(child.try_reserve_pending_slot(2).is_none());
        first.commit("child");
        second.commit("grandchild");
        grandchild.touch_resident("child");
        assert_eq!(root.resident_ids(), vec!["grandchild", "child"]);
        drop(running);
        assert_eq!(grandchild.active_count(), 0);
        child.remove_resident("grandchild");
        assert_eq!(root.resident_ids(), vec!["child"]);
    }

    #[test]
    fn guards_keep_shared_state_alive_after_the_manager_is_dropped() {
        let governor = governor(1);
        let execution = governor.execution.clone();
        let residency = governor.residency.clone();
        let running = governor.admit_turn();
        let pending = governor.try_reserve_pending_slot(1).unwrap();
        drop(governor);
        drop(running);
        drop(pending);
        assert_eq!(execution.active.load(Ordering::Acquire), 0);
        assert_eq!(residency.lock_recover().pending_slots, 0);
    }

    #[test]
    fn a_total_limit_of_one_or_zero_has_no_child_capacity() {
        for total in [0, 1] {
            let governor = ResourceGovernor::new(ResourceGovernorOptions {
                max_concurrent_threads_per_session: Some(total),
                ..Default::default()
            });
            assert_eq!(governor.capacity(), 0);
            assert_eq!(
                governor.check_execution_capacity(None),
                Err(SlotAdmissionError::ExecutionCapacity { maximum: 0 })
            );
            assert!(governor.try_reserve_default_residency_slot().is_none());
            assert!(governor.try_reserve_pending_slot(0).is_none());
            assert_eq!(governor.active_count(), 0);
            assert_eq!(governor.residency.lock_recover().pending_slots, 0);
            assert!(governor.resident_ids().is_empty());
        }
    }

    #[tokio::test]
    async fn cancelled_task_drops_execution_and_pending_guards_without_unloading_residents() {
        let governor = governor(2);
        governor
            .try_reserve_pending_slot(2)
            .unwrap()
            .commit("resident");
        let entered = Arc::new(tokio::sync::Notify::new());
        let child = governor.clone();
        let ready = entered.clone();
        let task = tokio::spawn(async move {
            let _running = child.admit_turn();
            let _pending = child.try_reserve_pending_slot(2).unwrap();
            ready.notify_one();
            std::future::pending::<()>().await;
        });
        entered.notified().await;
        assert_eq!(governor.active_count(), 1);
        assert_eq!(governor.residency.lock_recover().pending_slots, 1);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(governor.active_count(), 0);
        assert_eq!(governor.residency.lock_recover().pending_slots, 0);
        assert_eq!(governor.resident_ids(), vec!["resident"]);
        assert!(governor.try_reserve_pending_slot(2).is_some());
    }

    #[test]
    fn capacity_errors_identify_the_exhausted_resource() {
        let execution = SlotAdmissionError::ExecutionCapacity { maximum: 3 };
        let residency = SlotAdmissionError::ResidencyCapacity { maximum: 3 };
        assert_ne!(execution, residency);
        assert!(execution.to_string().contains("execution capacity"));
        assert!(residency.to_string().contains("residency capacity"));
    }
}
