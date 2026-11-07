use std::collections::{BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tokio::sync::watch;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ToolOrigin<'a> {
    Direct,
    Nested { wrapper_item_id: Option<&'a str> },
}

#[derive(Clone, Copy, Debug)]
pub struct ToolObservation<'a> {
    pub call_id: &'a str,
    pub originating_item_id: Option<&'a str>,
    pub origin: ToolOrigin<'a>,
    /// A direct custom `exec` cell; its nested action owns the approval category.
    pub is_code_wrapper: bool,
}

struct ObservedAction {
    call_id: String,
    index: usize,
    wrapper_item_id: Option<String>,
    parent_wrapper_index: Option<usize>,
    finished: bool,
}

#[derive(Default)]
struct ActionOrder {
    actions: VecDeque<ObservedAction>,
}

impl ActionOrder {
    fn record(&mut self, observation: ToolObservation<'_>, index: usize) {
        let parent_wrapper_index = match observation.origin {
            ToolOrigin::Nested { wrapper_item_id } => wrapper_item_id.and_then(|item_id| {
                self.actions
                    .iter()
                    .rev()
                    .find(|action| action.wrapper_item_id.as_deref() == Some(item_id))
                    .map(|action| action.index)
            }),
            ToolOrigin::Direct => None,
        };
        let wrapper_item_id = (observation.is_code_wrapper
            && observation.origin == ToolOrigin::Direct)
            .then(|| observation.originating_item_id.map(str::to_owned))
            .flatten();
        if self.actions.len() == 256 {
            self.actions.pop_front();
        }
        self.actions.push_back(ObservedAction {
            call_id: observation.call_id.to_owned(),
            index,
            wrapper_item_id,
            parent_wrapper_index,
            finished: false,
        });
    }

    fn action(&self, call_id: Option<&str>) -> Option<&ObservedAction> {
        let call_id = call_id?;
        self.actions
            .iter()
            .rev()
            .find(|action| action.call_id == call_id)
    }

    fn finish(&mut self, call_id: &str) {
        if let Some(action) = self
            .actions
            .iter_mut()
            .rev()
            .find(|action| action.call_id == call_id)
        {
            action.finished = true;
        }
    }

    fn discount(&self, call_id: Option<&str>, scored_index: usize) -> usize {
        usize::from(
            self.action(call_id)
                .and_then(|action| action.parent_wrapper_index)
                .is_some_and(|wrapper| wrapper > scored_index),
        )
    }

    fn pending_discount(&self, sampled_index: usize) -> usize {
        usize::from(self.actions.iter().any(|action| {
            !action.finished
                && action
                    .parent_wrapper_index
                    .is_some_and(|wrapper| wrapper > sampled_index)
        }))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ClassifierScore {
    pub action_risk: f64,
    pub sampled_at: SystemTime,
}

struct ScoreState<A> {
    score: Option<ClassifierScore>,
    score_index: usize,
    order: ActionOrder,
    latest_action: usize,
    latest_scored_action: usize,
    latest_failed_action: usize,
    js_executions: usize,
    oversized_actions: BTreeSet<String>,
    authorization: Option<A>,
}

impl<A> Default for ScoreState<A> {
    fn default() -> Self {
        Self {
            score: None,
            score_index: 0,
            order: ActionOrder::default(),
            latest_action: 0,
            latest_scored_action: 0,
            latest_failed_action: 0,
            js_executions: 0,
            oversized_actions: BTreeSet::new(),
            authorization: None,
        }
    }
}

/// Score, authorization and coverage always become visible in one publication.
pub struct ScoreProgress<A> {
    state: Mutex<ScoreState<A>>,
    updates: watch::Sender<()>,
    observations: watch::Sender<()>,
}

impl<A> Default for ScoreProgress<A> {
    fn default() -> Self {
        Self {
            state: Mutex::new(ScoreState::default()),
            updates: watch::channel(()).0,
            observations: watch::channel(()).0,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CachedScore<A> {
    pub lag: usize,
    pub score_at_or_before_action: bool,
    pub has_unscored_failure: bool,
    pub js_executions: usize,
    pub oversized: bool,
    pub action_risk: Option<f64>,
    pub authorization: Option<A>,
}

impl<A> ScoreProgress<A> {
    pub fn reset(&self) {
        *self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = ScoreState::default();
        self.updates.send_replace(());
        self.observations.send_replace(());
    }

    pub fn subscribe(&self) -> watch::Receiver<()> {
        self.updates.subscribe()
    }

    pub fn observe(&self, observation: ToolObservation<'_>) -> usize {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.latest_action = state.latest_action.saturating_add(1);
        let index = state.latest_action;
        state.order.record(observation, index);
        self.observations.send_replace(());
        index
    }

    /// Setup and reset actions must not be counted as JS executions by the host.
    pub fn observe_js_execution(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.js_executions = state.js_executions.saturating_add(1);
    }

    pub fn invalidate(&self, index: usize) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.latest_failed_action = state.latest_failed_action.max(index);
    }

    pub fn mark_oversized(&self, call_id: &str, index: usize) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.oversized_actions.insert(call_id.to_owned());
        state.latest_failed_action = state.latest_failed_action.max(index);
    }

    pub fn finish(&self, call_id: &str) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.oversized_actions.remove(call_id);
        state.order.finish(call_id);
        self.observations.send_replace(());
    }

    pub fn publish(&self, score: ClassifierScore, authorization: A, index: usize) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state
            .score
            .as_ref()
            .is_some_and(|old| old.sampled_at >= score.sampled_at)
        {
            return false;
        }
        state.score = Some(score);
        state.score_index = index;
        state.authorization = Some(authorization);
        state.latest_scored_action = state.latest_scored_action.max(index);
        self.updates.send_replace(());
        true
    }

    pub fn fail_closed(&self, sampled_at: SystemTime) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state
            .score
            .as_ref()
            .is_none_or(|old| old.sampled_at <= sampled_at)
        {
            state.score = Some(ClassifierScore {
                action_risk: 1.0,
                sampled_at,
            });
        }
    }

    pub fn clear_score(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .score = None;
    }

    pub fn inspect(&self, call_id: Option<&str>) -> CachedScore<A>
    where
        A: Clone,
    {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        CachedScore {
            lag: state
                .latest_action
                .saturating_sub(state.latest_scored_action)
                .saturating_sub(state.order.discount(call_id, state.latest_scored_action)),
            score_at_or_before_action: state
                .order
                .action(call_id)
                .is_some_and(|action| state.score_index <= action.index),
            has_unscored_failure: state.latest_failed_action > state.latest_scored_action,
            js_executions: state.js_executions,
            oversized: call_id.is_some_and(|id| state.oversized_actions.contains(id)),
            action_risk: state.score.as_ref().map(|score| score.action_risk),
            authorization: state.authorization.clone(),
        }
    }
}

pub struct PendingScore<A> {
    pub progress: Arc<ScoreProgress<A>>,
    pub index: usize,
    pub max_lag: usize,
}

impl<A> Clone for PendingScore<A> {
    fn clone(&self) -> Self {
        Self {
            progress: Arc::clone(&self.progress),
            index: self.index,
            max_lag: self.max_lag,
        }
    }
}

impl<A> PendingScore<A> {
    pub fn is_expired(&self) -> bool {
        let state = self
            .progress
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state
            .latest_action
            .saturating_sub(self.index)
            .saturating_sub(state.order.pending_discount(self.index))
            > self.max_lag
    }

    pub async fn expired(&self) {
        let mut observations = self.progress.observations.subscribe();
        while !self.is_expired() {
            if observations.changed().await.is_err() {
                return;
            }
        }
    }
}
