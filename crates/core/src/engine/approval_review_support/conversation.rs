// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified to bind runtime-owned errors, metadata and session contracts.

//! Tracks reviewed transcript progress separately from committed conversation history.
//! Hosts serialize reviews and checkpoint commits, and supply opaque completed history.
//! An exclusive continuation can take the history without advancing reviewed progress.

/// A completed history and the transcript progress that produced it.
#[derive(Clone)]
pub struct ConversationCheckpoint<H, C> {
    history: H,
    cursor: Option<C>,
    completed_review_count: usize,
}

#[cfg(test)]
impl<H, C> ConversationCheckpoint<H, C> {
    pub fn history(&self) -> &H {
        &self.history
    }
}

/// Shared bookkeeping for a host-owned approval conversation.
pub struct ConversationState<H, C> {
    cursor: Option<C>,
    completed_review_count: usize,
    checkpoint: Option<ConversationCheckpoint<H, C>>,
}

impl<H, C> Default for ConversationState<H, C> {
    fn default() -> Self {
        Self {
            cursor: None,
            completed_review_count: 0,
            checkpoint: None,
        }
    }
}

impl<H, C: Clone> ConversationState<H, C> {
    /// Starts an independent conversation from exactly the committed history and cursor.
    pub fn fork(checkpoint: ConversationCheckpoint<H, C>) -> (Self, H) {
        (
            Self {
                cursor: checkpoint.cursor,
                completed_review_count: checkpoint.completed_review_count,
                ..Self::default()
            },
            checkpoint.history,
        )
    }

    pub fn cursor(&self) -> Option<C> {
        self.cursor.clone()
    }

    #[cfg(test)]
    pub fn completed_review_count(&self) -> usize {
        self.completed_review_count
    }

    /// Rebuilds the active transcript after compaction. Forks can still use the last
    /// committed history; it owns its own cursor and is independent of this reset.
    #[cfg(test)]
    pub fn reset_transcript(&mut self) {
        self.cursor = None;
    }

    /// Advances live progress once the host's review completes.
    pub fn complete_review(&mut self, cursor: C) {
        self.cursor = Some(cursor);
        self.completed_review_count = self.completed_review_count.saturating_add(1);
    }

    /// Pairs host-supplied history with the current progress for subsequent forks.
    pub fn commit_snapshot(&mut self, history: H) {
        self.checkpoint = Some(ConversationCheckpoint {
            history,
            cursor: self.cursor.clone(),
            completed_review_count: self.completed_review_count,
        });
    }

    pub fn snapshot(&self) -> Option<&ConversationCheckpoint<H, C>> {
        self.checkpoint.as_ref()
    }

    /// Transfers committed history to its exclusive owner, leaving progress unchanged.
    /// The host must commit a completed replacement before this state can be reused.
    #[cfg(test)]
    pub fn take_history(&mut self) -> Option<H> {
        self.checkpoint.take().map(|checkpoint| checkpoint.history)
    }
}

#[cfg(test)]
#[path = "conversation_tests.rs"]
mod tests;
