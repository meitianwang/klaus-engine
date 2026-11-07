use super::ClassifierError;
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TranscriptCursor {
    pub parent_history_version: u64,
    pub transcript_entry_count: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ConversationReuseKey<A, I, P> {
    pub authorization: A,
    pub instructions: I,
    pub parent_compaction: Option<P>,
}

/// Only a fully completed response may supply the next request's retained prefix.
#[derive(Clone, Debug)]
pub struct CompletedConversation<K, H> {
    pub index: usize,
    pub key: K,
    pub history: H,
    pub cursor: TranscriptCursor,
}

type SharedConversationHistory<K, H> = Arc<Mutex<Option<Arc<CompletedConversation<K, H>>>>>;

pub struct ConversationBackend<K, H> {
    history: SharedConversationHistory<K, H>,
    capacity: Mutex<Arc<Semaphore>>,
    generation: Arc<()>,
}

impl<K, H> Default for ConversationBackend<K, H> {
    fn default() -> Self {
        Self {
            history: Arc::default(),
            capacity: Mutex::new(Arc::new(Semaphore::new(16))),
            generation: Arc::new(()),
        }
    }
}

pub struct ConversationReservation<K, H> {
    pub index: usize,
    history: SharedConversationHistory<K, H>,
    _permit: OwnedSemaphorePermit,
    generation: Weak<()>,
}

impl<K, H> ConversationBackend<K, H> {
    /// Records action order and reserves preparation capacity atomically.
    /// A full retained queue fails closed without evicting another request.
    pub fn reserve(
        &self,
        observe: impl FnOnce() -> usize,
    ) -> (
        usize,
        Result<ConversationReservation<K, H>, ClassifierError>,
    ) {
        let capacity = self
            .capacity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let index = observe();
        let reservation = Arc::clone(&capacity)
            .try_acquire_owned()
            .map(|permit| ConversationReservation {
                index,
                history: Arc::clone(&self.history),
                _permit: permit,
                generation: Arc::downgrade(&self.generation),
            })
            .map_err(|_| ClassifierError::QueueFull);
        (index, reservation)
    }

    pub fn snapshot(&self) -> Option<Arc<CompletedConversation<K, H>>> {
        self.history
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl<K: PartialEq, H> ConversationReservation<K, H> {
    pub fn is_current(&self) -> bool {
        self.generation.strong_count() > 0
    }

    /// Delayed preparation cannot import evidence from an action admitted later.
    pub fn fork(&self, key: &K) -> Option<Arc<CompletedConversation<K, H>>> {
        self.history
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .filter(|old| old.index < self.index && old.key == *key)
            .cloned()
    }

    /// The caller revalidates authorization immediately before this publication.
    /// No asynchronous work occurs between the supplied validation and commit.
    pub fn commit(
        &self,
        key: K,
        history: H,
        cursor: TranscriptCursor,
        authorization_is_current: impl FnOnce() -> bool,
    ) -> bool {
        if !self.is_current() || !authorization_is_current() {
            return false;
        }
        let mut committed = self
            .history
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if committed
            .as_ref()
            .is_some_and(|old| old.index >= self.index)
        {
            return false;
        }
        *committed = Some(Arc::new(CompletedConversation {
            index: self.index,
            key,
            history,
            cursor,
        }));
        true
    }

    /// Selects a compatible committed prefix, resetting only before hard admission.
    /// `prepare` composes the evidence delta without mutating the retained history.
    pub fn prepare<R>(
        &self,
        key: &K,
        next_cursor: TranscriptCursor,
        reset_token_limit: usize,
        max_input_tokens: usize,
        mut prepare: impl FnMut(Option<&CompletedConversation<K, H>>) -> Option<(R, usize)>,
    ) -> Option<(R, usize)> {
        let previous = self.fork(key);
        let compatible = previous.as_deref().filter(|previous| {
            previous.cursor.parent_history_version == next_cursor.parent_history_version
                && previous.cursor.transcript_entry_count <= next_cursor.transcript_entry_count
        });
        let prepared = compatible
            .and_then(|history| prepare(Some(history)))
            .filter(|(_, tokens)| {
                *tokens <= reset_token_limit && *tokens <= max_input_tokens.saturating_sub(256)
            });
        prepared.or_else(|| {
            prepare(None).filter(|(_, tokens)| *tokens <= max_input_tokens.saturating_sub(256))
        })
    }
}
