//! Defect: an ineligible continuation probe waited behind a live serial turn.
//! Root cause: completion eligibility was checked only after chain admission.
//! Work item 20261021: reject stale probes promptly without changing valid resumes.

use super::*;

#[tokio::test]
async fn ineligible_pending_resumes_settle_while_an_automatic_goal_provider_is_held() {
    let entered = Arc::new(Notify::new());
    let cancelled = Arc::new(Notify::new());
    let agent = make_seed_then_blocking_agent(".", entered.clone(), cancelled.clone());
    let mut fixture = control_fixture_with_agent(agent).await;
    fixture.send(json!({"id":"start-held-goal","op":{"type":"set_goal","objective":"Keep working until the user stops this held goal","status":"active","origin":"user"}})).await;
    let goal = fixture.event("start-held-goal").await;
    assert_eq!(goal["threadGoal"]["status"], "active", "{goal}");
    timeout(Duration::from_secs(10), entered.notified())
        .await
        .expect("the actual second goal provider remains held");
    let first = timeout(Duration::from_secs(10), async {
        loop {
            let line = fixture.lines.next_line().await.unwrap().unwrap();
            let event: Value = serde_json::from_str(&line).unwrap();
            if event["result"]["stopReason"] == "completed" {
                break event["submissionId"].as_str().unwrap().to_owned();
            }
        }
    })
    .await
    .expect("the first goal turn completed before its held successor");
    let held = fixture
        .server
        .inner
        .current_submission_id
        .lock()
        .unwrap()
        .clone();
    assert!(!held.is_empty());
    fixture.send(json!({"id":"stale-pending-probe","op":{"type":"resume_pending","afterSubmissionId":first}})).await;
    let stale = timeout(Duration::from_secs(1), fixture.event("stale-pending-probe"))
        .await
        .ok();

    // Capture cancellation at admission, before dispatch has a chance to run.
    let admitted = fixture
        .server
        .inner
        .admit_op(Op {
            id: "cancelled-pending-probe".into(),
            op: OpKind::ResumePending {
                after_submission_id: first.into(),
            },
        })
        .unwrap_or_else(|_| panic!("the cancelled probe fits foreground admission"));
    admitted.run_signal.as_ref().unwrap().cancel();
    let dispatched = fixture.server.inner.spawn_dispatch(admitted).unwrap();
    let cancelled_probe = timeout(
        Duration::from_secs(1),
        fixture.event("cancelled-pending-probe"),
    )
    .await
    .ok();
    dispatched.await.unwrap();
    let released_reservations = timeout(Duration::from_secs(1), async {
        loop {
            {
                let admission = fixture.server.inner.run_admission.lock().unwrap();
                if admission.foreground.len() == 1 && admission.foreground[0].submission_id == held
                {
                    break;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    let still_held = timeout(Duration::from_millis(10), cancelled.notified())
        .await
        .is_err();

    // Release the real provider before asserting so the pre-fix failure leaves
    // no held stream or unfinished shutdown behind the regression report.
    fixture
        .send(json!({"id":"stop-held-goal","op":{"type":"interrupt","origin":"system"}}))
        .await;
    let stop = fixture.event("stop-held-goal").await;
    assert_eq!(stop["turnInterrupt"]["accepted"], true, "{stop}");
    fixture.close().await;

    let stale = stale.expect("an ineligible probe must settle before the live goal is released");
    assert_eq!(stale["pendingResume"]["started"], false, "{stale}");
    assert_eq!(stale["pendingResume"]["reason"], "not_completed", "{stale}");
    let cancelled_probe =
        cancelled_probe.expect("a cancelled probe must settle before the live goal is released");
    assert_eq!(cancelled_probe["pendingResume"]["started"], false);
    assert_eq!(cancelled_probe["pendingResume"]["reason"], "interrupted");
    assert!(
        released_reservations,
        "rejected probes release their reservations"
    );
    assert!(
        still_held,
        "neither rejected probe interrupts the actual goal provider"
    );
}

#[tokio::test]
async fn an_eligible_pending_resume_keeps_serial_order_and_consumes_queued_input() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let mut fixture = control_fixture_with_agent(make_recording_agent(".", requests.clone())).await;
    fixture
        .send(
            json!({"id":"eligible-predecessor","op":{"type":"run","input":"first ordinary turn"}}),
        )
        .await;
    let first = fixture.event("eligible-predecessor").await;
    assert_eq!(first["result"]["stopReason"], "completed", "{first}");
    let entered = Arc::new(Notify::new());
    let (release, released) = oneshot::channel();
    fixture
        .server
        .inner
        .enqueue("held-serial-boundary".into(), {
            let entered = entered.clone();
            move || {
                Box::pin(async move {
                    entered.notify_one();
                    let _ = released.await;
                    Ok(())
                })
            }
        });
    timeout(Duration::from_secs(10), entered.notified())
        .await
        .unwrap();
    fixture
        .session
        .enqueue(
            Input::Text("the queued follow-up".into()),
            EnqueueOptions::default(),
        )
        .unwrap();
    fixture.send(json!({"id":"eligible-pending-probe","op":{"type":"resume_pending","afterSubmissionId":"eligible-predecessor"}})).await;
    let premature = timeout(
        Duration::from_millis(50),
        fixture.event("eligible-pending-probe"),
    )
    .await
    .ok();
    let kept_serial_order = premature.is_none();
    release.send(()).unwrap();
    let outcome = match premature {
        Some(outcome) => outcome,
        None => fixture.event("eligible-pending-probe").await,
    };
    let result = fixture.event("eligible-pending-probe").await;
    let no_reservations = timeout(Duration::from_secs(1), async {
        loop {
            if fixture
                .server
                .inner
                .run_admission
                .lock()
                .unwrap()
                .foreground
                .is_empty()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    let requests = requests.lock().unwrap().clone();
    fixture.close().await;
    assert!(
        kept_serial_order,
        "an eligible probe must keep serial ordering"
    );
    assert_eq!(outcome["pendingResume"]["started"], true, "{outcome}");
    assert_eq!(result["result"]["stopReason"], "completed", "{result}");
    assert_eq!(requests.len(), 2);
    assert!(format!("{:?}", requests[1].messages).contains("the queued follow-up"));
    assert!(
        no_reservations,
        "a finished eligible resume releases its reservation"
    );
}
