//! Admission regressions for raw operations racing the named submission facade.

use super::*;

fn submission(mode: TurnSubmissionMode, expected: Option<&str>) -> TurnSubmission {
    TurnSubmission {
        submission_id: "new-submission".to_owned(),
        input: Input::Text("follow up".into()),
        options: None,
        mode,
        expected_turn_id: expected.map(str::to_owned),
        queue_message_id: None,
        delivery: None,
        display_metadata: None,
    }
}

fn assert_reason(outcome: TurnSubmissionOutcome, expected: TurnSubmissionRejection) {
    match outcome {
        TurnSubmissionOutcome::NotSubmitted { code, .. } => assert_eq!(code, expected),
        other => panic!("expected refusal {expected:?}, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn atomic_submit_raw_run_and_resume_reservations_close_the_idle_window() {
    let agent = make_agent(".", Vec::new(), Some(json!({"prewarm":false})));
    let ((sr, sw), (_cr, _cw)) = duplex_pair();
    let (server, done) = start_server(make_create_session(agent, None), sr, sw);
    for op in [
        OpKind::Run {
            input: Input::Text("raw".into()),
            options: None,
        },
        OpKind::ResumeQueued,
        OpKind::ResumePending {
            after_submission_id: "completed".into(),
        },
    ] {
        let admitted = server
            .inner
            .admit_op(Op {
                id: "raw-owner".into(),
                op,
            })
            .unwrap();
        assert_reason(
            server
                .submit_turn(submission(TurnSubmissionMode::StartIfIdle, None))
                .await,
            TurnSubmissionRejection::NotIdle,
        );
        assert_reason(
            server
                .submit_turn(submission(TurnSubmissionMode::Steer, Some("old-owner")))
                .await,
            TurnSubmissionRejection::ExpectedTurnMismatch,
        );
        // A starting resume owns admission while it has no nonempty input batch.
        if !matches!(admitted.op.op, OpKind::Run { .. }) {
            assert_reason(
                server
                    .submit_turn(submission(TurnSubmissionMode::StartOrSteer, None))
                    .await,
                TurnSubmissionRejection::ActiveTurnNotSteerable,
            );
        }
        assert!(
            server.existing_session().is_none(),
            "known refusals do not initialize a lazy core"
        );
        drop(admitted);
        assert!(server
            .inner
            .run_admission
            .lock()
            .unwrap()
            .foreground
            .is_empty());
    }
    assert_reason(
        server
            .submit_turn(submission(TurnSubmissionMode::Steer, Some("raw-owner")))
            .await,
        TurnSubmissionRejection::NoActiveTurn,
    );
    server.stop();
    assert_reason(
        server
            .submit_turn(submission(TurnSubmissionMode::StartOrSteer, None))
            .await,
        TurnSubmissionRejection::SessionClosing,
    );
    done.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn atomic_submit_review_and_compact_are_explicit_non_steerable_boundaries() {
    let agent = make_agent(".", Vec::new(), Some(json!({"prewarm":false})));
    let ((sr, sw), (_cr, _cw)) = duplex_pair();
    let (server, done) = start_server(make_create_session(agent, None), sr, sw);
    for op in [
        OpKind::ReviewSession {
            target: klaus_agent_core::engine::session::ReviewTarget::UncommittedChanges,
            user_facing_hint: None,
        },
        OpKind::CompactSession {
            instructions: None,
            selection: None,
        },
    ] {
        let admitted = server
            .inner
            .admit_op(Op {
                id: "exclusive-operation".into(),
                op,
            })
            .unwrap();
        let kind = if matches!(admitted.op.op, OpKind::ReviewSession { .. }) {
            TurnSubmissionKind::Review
        } else {
            TurnSubmissionKind::Compact
        };
        for mode in [TurnSubmissionMode::Steer, TurnSubmissionMode::StartOrSteer] {
            let outcome = server
                .submit_turn(submission(mode, Some("exclusive-operation")))
                .await;
            assert!(
                matches!(outcome, TurnSubmissionOutcome::NotSubmitted {
                code: TurnSubmissionRejection::ActiveTurnNotSteerable,
                turn_kind: Some(actual), ..
            } if actual == kind),
                "{outcome:?}"
            );
        }
        assert_reason(
            server
                .submit_turn(submission(TurnSubmissionMode::StartIfIdle, None))
                .await,
            TurnSubmissionRejection::NotIdle,
        );
        assert!(server.existing_session().is_none());
        drop(admitted);
        assert!(server
            .inner
            .run_admission
            .lock()
            .unwrap()
            .non_steerable
            .is_empty());
    }
    server.stop();
    done.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn atomic_submit_ended_reservation_cannot_retain_a_late_steer() {
    let agent = make_agent(".", Vec::new(), Some(json!({"prewarm":false})));
    let ((sr, sw), (_cr, _cw)) = duplex_pair();
    let (server, done) = start_server(make_create_session(agent, None), sr, sw);
    let admitted = server
        .inner
        .admit_op(Op {
            id: "raw-owner".into(),
            op: OpKind::Run {
                input: Input::Text("raw".into()),
                options: None,
            },
        })
        .unwrap();
    // A terminal publication can finish steering before the reservation drops.
    let token = admitted
        .foreground_guard
        .as_ref()
        .unwrap()
        .reservation_token;
    server.inner.finish_steer_turn(token);
    assert_reason(
        server
            .submit_turn(submission(TurnSubmissionMode::Steer, Some("raw-owner")))
            .await,
        TurnSubmissionRejection::ActiveTurnNotSteerable,
    );
    assert!(server.existing_session().is_none());
    drop(admitted);
    assert_reason(
        server
            .submit_turn(submission(TurnSubmissionMode::Steer, Some("raw-owner")))
            .await,
        TurnSubmissionRejection::NoActiveTurn,
    );
    server.stop();
    done.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn atomic_submit_waiting_compact_preserves_regular_steer_until_worker_activation() {
    let agent = make_agent(".", Vec::new(), Some(json!({"prewarm":false})));
    let ((sr, sw), (_cr, _cw)) = duplex_pair();
    let (server, done) = start_server(make_create_session(agent, None), sr, sw);
    let session = server.prepare_session().await.unwrap();
    let regular = server
        .inner
        .admit_op(Op {
            id: "regular".into(),
            op: OpKind::Run {
                input: Input::Text("raw".into()),
                options: None,
            },
        })
        .unwrap();
    let compact = server
        .inner
        .admit_op(Op {
            id: "compact".into(),
            op: OpKind::CompactSession {
                instructions: None,
                selection: None,
            },
        })
        .unwrap();
    let outcome = server
        .submit_turn(submission(TurnSubmissionMode::Steer, Some("regular")))
        .await;
    assert!(
        matches!(outcome, TurnSubmissionOutcome::Steered { ref turn_id } if turn_id == "regular"),
        "{outcome:?}"
    );
    assert_eq!(session.queued_messages().len(), 1);
    assert_eq!(
        session.queued_messages()[0].priority,
        Some(klaus_agent_core::engine::message_queue::QueuePriority::Now)
    );
    compact.busy_guard.as_ref().unwrap().activate();
    let outcome = server
        .submit_turn(submission(TurnSubmissionMode::Steer, Some("compact")))
        .await;
    assert!(
        matches!(
            outcome,
            TurnSubmissionOutcome::NotSubmitted {
                code: TurnSubmissionRejection::ActiveTurnNotSteerable,
                turn_kind: Some(TurnSubmissionKind::Compact),
                ..
            }
        ),
        "{outcome:?}"
    );
    assert_eq!(
        session.queued_messages().len(),
        1,
        "active compact rejects without retention"
    );
    drop(compact);
    drop(regular);
    server.stop();
    done.await.unwrap();
}
