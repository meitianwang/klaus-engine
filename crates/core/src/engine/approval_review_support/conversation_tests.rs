// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified for runtime component contracts and deterministic regressions.

//! Checks that forks use committed history and progress while the next review runs.

use super::*;

#[test]
fn fork_keeps_committed_history_and_cursor_while_next_review_is_uncommitted() {
    let mut state = ConversationState::default();
    let first = (1, 2);
    state.complete_review(first);
    state.commit_snapshot(vec!["first review"]);
    let second = (1, 4);
    state.complete_review(second);

    let (fork, history) = ConversationState::fork(state.snapshot().unwrap().clone());
    assert_eq!(
        (history, fork.cursor(), fork.completed_review_count()),
        (vec!["first review"], Some(first), 1),
    );

    state.commit_snapshot(vec!["first review", "second review"]);
    let (fork, history) = ConversationState::fork(state.snapshot().unwrap().clone());
    assert_eq!(
        (history, fork.cursor(), fork.completed_review_count()),
        (vec!["first review", "second review"], Some(second), 2),
    );
    assert_eq!(
        (
            state.take_history(),
            state.cursor(),
            state.completed_review_count(),
        ),
        (Some(vec!["first review", "second review"]), Some(second), 2),
    );
    assert!(state.snapshot().is_none());
}

#[test]
fn transcript_reset_does_not_invalidate_the_last_committed_fork() {
    let mut state = ConversationState::default();
    let cursor = (4, 8);
    state.complete_review(cursor);
    state.commit_snapshot(vec!["committed"]);
    state.reset_transcript();
    assert_eq!(state.cursor(), None);
    let (fork, history) = ConversationState::fork(state.snapshot().unwrap().clone());
    assert_eq!(fork.cursor(), Some(cursor));
    assert_eq!(fork.completed_review_count(), 1);
    assert_eq!(history, vec!["committed"]);
    assert!(fork.snapshot().is_none());
}
