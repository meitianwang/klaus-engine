//! Reserved continuation decisions settle only at the actual provider boundary.

use super::*;
use futures::StreamExt;
use klaus_agent_core::schemas::message::Usage;

#[derive(Clone, Copy, Debug)]
enum Control {
    Admit,
    NoCandidate,
    Clear,
    Replace,
    Disable,
    Plan,
    Stop,
    ProviderFailure,
}

struct Gates {
    first: Arc<tokio::sync::Semaphore>,
    admission: Arc<tokio::sync::Semaphore>,
    successor: Arc<tokio::sync::Semaphore>,
}
impl Drop for Gates {
    fn drop(&mut self) {
        self.first.add_permits(1);
        self.admission.add_permits(1);
        self.successor.add_permits(1);
    }
}

fn disposition(frame: &Value) -> Option<&Value> {
    (frame["method"] == "goal.continuation").then_some(&frame["params"])
}

async fn collect_until(
    client: &mut TestClient,
    frames: &mut Vec<Value>,
    predicate: impl Fn(&Value) -> bool,
) -> Value {
    if let Some(frame) = frames.iter().find(|frame| predicate(frame)) {
        return frame.clone();
    }
    tokio::time::timeout(REPLY_TIMEOUT, async {
        loop {
            let frame = client.next_message().await;
            // These fixtures admit at most two actual requests. Bound diagnostic
            // collection independently from the duration of an awaited barrier.
            assert!(frames.len() < 256, "unexpected continuation frame flood");
            let found = predicate(&frame);
            frames.push(frame.clone());
            if found {
                return frame;
            }
        }
    })
    .await
    .expect("the exact continuation disposition or predecessor outcome must settle")
}

async fn continuation_control(control: Control) {
    continuation_control_then_start(control, false).await;
}

async fn continuation_control_then_start(control: Control, start_after_stop: bool) {
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let _config = ConfigGuard::new(root.path());
    let first_entered = Arc::new(tokio::sync::Notify::new());
    let admission_entered = Arc::new(tokio::sync::Notify::new());
    let goal_runtime = Arc::new(std::sync::OnceLock::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let gates = Gates {
        first: Arc::new(tokio::sync::Semaphore::new(0)),
        admission: Arc::new(tokio::sync::Semaphore::new(0)),
        successor: Arc::new(tokio::sync::Semaphore::new(0)),
    };
    let call_count = calls.clone();
    let first_signal = first_entered.clone();
    let first_release = gates.first.clone();
    let successor_release = gates.successor.clone();
    let agent = make_agent_with_call_model_and_config(
        Box::new(move |_, signal| {
            let index = call_count.fetch_add(1, Ordering::SeqCst);
            assert!(
                index < 2,
                "only the predecessor and one accepted successor may reach the provider"
            );
            if index == 1 && matches!(control, Control::ProviderFailure) {
                panic!("forced successor provider factory failure");
            }
            if index == 0 {
                first_signal.notify_one();
            }
            let release = if index == 0 {
                first_release.clone()
            } else {
                successor_release.clone()
            };
            Box::pin(futures::stream::iter(vec![ev::message_start(None),ev::text(if index==0 {"predecessor settled"} else {"successor settled"})])
            .chain(futures::stream::once(async move {
                tokio::select! {
                    permit=release.acquire()=>{permit.unwrap().forget();Some(ev::stop(Some(ModelStopReason::EndTurn),Some(Usage {
                        input_tokens:0,output_tokens:0,cache_read_input_tokens:0,cache_creation_input_tokens:0,total_tokens:None,
                    })))},
                    _=signal.cancelled()=>None,
                }
            }).filter_map(futures::future::ready)))
        }),
        Some(json!({"goal":true,"prewarm":false,"autoMemory":false,"autoDream":false})),
    );
    let store: Arc<dyn SessionStore> = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.path().join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let factory: CreateSession = {
        let agent = agent.clone();
        let store = store.clone();
        let admission_entered = admission_entered.clone();
        let admission_release = gates.admission.clone();
        let goal_runtime = goal_runtime.clone();
        let cwd = root.path().to_string_lossy().into_owned();
        Arc::new(move |ctx| {
            let agent = agent.clone();
            let store = store.clone();
            let entered = admission_entered.clone();
            let release = admission_release.clone();
            let goal_runtime = goal_runtime.clone();
            let cwd = cwd.clone();
            Box::pin(async move {
                let session=agent.open_session(store,StartSessionOptions {
                    id:ctx.init.thread_id,cwd:Some(cwd.into()),approval_handler:Some(ctx.approval_handler),
                    elicit_handler:Some(ctx.elicit_handler),mcp_elicitation_presenter:Some(ctx.mcp_elicitation_presenter),
                    config:Some(json!({"goal":true,"prewarm":false,"autoMemory":false,"autoDream":false})),..Default::default()
                }).await.map_err(|error|Box::new(error)as BoxError)?;
                assert!(goal_runtime.set(session.goal_runtime().unwrap()).is_ok());
                session
                    .goal_runtime()
                    .unwrap()
                    .testing_before_automatic_provider_admission(Some(Arc::new(move || {
                        let entered = entered.clone();
                        let release = release.clone();
                        Box::pin(async move {
                            entered.notify_one();
                            release.acquire().await.unwrap().forget();
                        })
                    })));
                Ok(session)
            })
        })
    };
    let lifecycle = crate::lifecycle::protocol_lifecycle(agent, Some(store));
    let mut client = TestClient::start_with(factory, Some(lifecycle));
    client
        .send(json!({"id":1,"method":"initialize","params":{
            "clientInfo":{"name":"continuation-presentation-test"},
            "capabilities":{"productEvents":true}
        }}))
        .await;
    let (initialized, _) = client.response_for(1).await;
    assert!(initialized.get("error").is_none(), "{initialized}");
    client.send(json!({"method":"initialized"})).await;
    let (started, _) = super::goal_interrupt_origin::request(
        &mut client,
        2,
        "session/start",
        json!({"init":{"threadId":"continuation-decision-thread"}}),
    )
    .await;
    let session = started["result"]["sessionId"].clone();
    let (created,mut frames)=super::goal_interrupt_origin::request(&mut client,3,"session/setGoal",json!({
        "sessionId":session,"objective":"Prove actual continuation disposition","origin":"user","submit":true,
        "clientRequestId":"continuation-disposition-create","expectedGoalId":null
    })).await;
    assert!(created.get("error").is_none(), "{created}");
    let goal_id = created["result"]["threadGoal"]["goalId"].clone();
    tokio::time::timeout(REPLY_TIMEOUT, first_entered.notified())
        .await
        .unwrap();
    if matches!(control, Control::NoCandidate) {
        let (paused, observed) = super::goal_interrupt_origin::request(
            &mut client,
            4,
            "session/setGoal",
            json!({"sessionId":session,"status":"paused","origin":"user","expectedGoalId":goal_id}),
        )
        .await;
        assert!(paused.get("error").is_none(), "{paused}");
        frames.extend(observed);
        gates.first.add_permits(1);
        collect_until(&mut client, &mut frames, |frame| {
            frame["method"] == "turn/completed"
        })
        .await;
        let (queue, observed) = super::goal_interrupt_origin::request(
            &mut client,
            5,
            "session/queueSnapshot",
            json!({"sessionId":session}),
        )
        .await;
        assert!(queue.get("error").is_none(), "{queue}");
        frames.extend(observed);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(
            !frames.iter().any(|frame| disposition(frame).is_some()),
            "a stopped goal must not reserve a candidate or hold predecessor completion"
        );
        client.close().await;
        return;
    }
    gates.first.add_permits(1);
    let pending_frame = collect_until(&mut client, &mut frames, |frame| {
        disposition(frame).is_some_and(|decision| decision["pending"] == true)
    })
    .await;
    let pending = &pending_frame["params"];
    assert_eq!(pending["sessionId"], session);
    assert_eq!(pending["goalId"], goal_id);
    assert!(pending.get("continued").is_none());
    assert!(pending
        .get("priorCompletionNotificationDisposition")
        .is_none());
    let prior = pending["priorSubmissionId"].as_str().unwrap().to_owned();
    let successor = pending["successorSubmissionId"]
        .as_str()
        .unwrap()
        .to_owned();
    let decision_id = pending["decisionId"].as_str().unwrap().to_owned();
    assert_ne!(prior, successor);
    let predecessor_terminal = collect_until(&mut client, &mut frames, |frame| {
        frame["method"] == "turn/completed" && frame["params"]["turnId"] == prior
    })
    .await;
    assert_eq!(
        predecessor_terminal["params"]["terminalNotificationDisposition"],
        "suppress"
    );
    let pending_position = frames
        .iter()
        .position(|frame| {
            disposition(frame).is_some_and(|decision| {
                decision["decisionId"] == decision_id && decision["pending"] == true
            })
        })
        .unwrap();
    let terminal_position = frames
        .iter()
        .position(|frame| frame["method"] == "turn/completed" && frame["params"]["turnId"] == prior)
        .unwrap();
    assert!(
        pending_position < terminal_position,
        "the predecessor completion must be held by an already-published decision"
    );
    let raw_predecessor_terminal = frames
        .iter()
        .position(|frame| {
            frame["params"]["event"]["submissionId"] == prior
                && matches!(
                    frame["params"]["event"]["event"]["type"].as_str(),
                    Some("turn.completed" | "turn.failed")
                )
        })
        .expect("the predecessor publishes its raw actual terminal before the result alias");
    assert!(pending_position < raw_predecessor_terminal);
    assert_eq!(
        frames[raw_predecessor_terminal]["params"]["event"]["terminalNotificationDisposition"],
        "suppress"
    );
    tokio::time::timeout(REPLY_TIMEOUT, admission_entered.notified())
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(
        !frames
            .iter()
            .any(|frame| disposition(frame)
                .is_some_and(|decision| decision["decisionId"] == decision_id
                    && decision["continued"] == true)),
        "a reservation or early requeststart event is not accepted provider work"
    );
    let replacement_goal = if matches!(control, Control::Replace) {
        let (cleared, observed) = super::goal_interrupt_origin::request(
            &mut client,
            40,
            "session/clearGoal",
            json!({"sessionId":session,"origin":"user","expectedGoalId":goal_id}),
        )
        .await;
        assert_eq!(cleared["result"]["goalCleared"], true, "{cleared}");
        frames.extend(observed);
        let (created, observed) = super::goal_interrupt_origin::request(
            &mut client, 41, "session/setGoal",
            json!({"sessionId":session,"objective":"Only the new generation may continue","status":"active","origin":"user","expectedGoalId":null}),
        ).await;
        assert!(created.get("error").is_none(), "{created}");
        frames.extend(observed);
        assert_ne!(created["result"]["threadGoal"]["goalId"], goal_id);
        Some(created["result"]["threadGoal"].clone())
    } else {
        None
    };
    let mutation = match control {
        Control::Clear => Some((
            "session/clearGoal",
            json!({"sessionId":session,"origin":"user","expectedGoalId":goal_id}),
        )),
        Control::Disable => Some((
            "session/updateConfig",
            json!({"sessionId":session,"update":{"goal":false}}),
        )),
        Control::Plan => Some((
            "session/setCollaborationMode",
            json!({"sessionId":session,"mode":"plan"}),
        )),
        Control::Stop => Some((
            "turn/interrupt",
            json!({"sessionId":session,"origin":"user-stop","submissionId":"stop-pending-successor","expectedTurnId":successor}),
        )),
        _ => None,
    };
    if let Some((method, params)) = mutation {
        let (reply, observed) =
            super::goal_interrupt_origin::request(&mut client, 4, method, params).await;
        assert!(reply.get("error").is_none(), "{control:?}: {reply}");
        frames.extend(observed);
        if matches!(control, Control::Stop) {
            // The provider admission hook is a pre-effect wait. Stop must retire
            // its cancelled foreground owner without manually opening this gate.
            // Durable write leases have separate held-writer regressions.
            let paused = collect_until(&mut client, &mut frames, |frame| {
                let goal = &frame["params"]["event"]["threadGoal"];
                goal["goalId"] == goal_id && goal["status"] == "paused"
            })
            .await;
            assert!(
                paused["params"]["event"]["goalRevision"].as_i64().unwrap()
                    > created["result"]["goalRevision"].as_i64().unwrap()
            );
            tokio::time::timeout(REPLY_TIMEOUT, async {
                while !goal_runtime.get().unwrap().continuation_suppressed() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("Stop must accept cancellation before the admission gate is released");
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(gates.admission.available_permits(), 0);
            let stop = collect_until(&mut client, &mut frames, |frame| {
                let event = &frame["params"]["event"];
                event["submissionId"] == "stop-pending-successor"
                    && event.get("turnInterrupt").is_some()
            })
            .await;
            assert_eq!(
                stop["params"]["event"]["turnInterrupt"]["accepted"], true,
                "{stop}"
            );
            assert_eq!(
                stop["params"]["event"]["turnInterrupt"]["interruptedTurnId"], successor,
                "{stop}"
            );
            assert!(
                stop["params"]["event"]["turnInterrupt"]["goalPauseError"].is_null(),
                "{stop}"
            );
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(gates.admission.available_permits(), 0);
            if start_after_stop {
                // Submit immediately after Stop, without waiting for another
                // terminal event or releasing any old admission gate.
                gates.successor.add_permits(1);
                let (started, observed) = super::goal_interrupt_origin::request(
                    &mut client,
                    5,
                    "turn/start",
                    json!({
                        "sessionId":session,"submissionId":"after-reserved-stop",
                        "input":"Run the next user task after the stopped reservation."
                    }),
                )
                .await;
                assert_eq!(
                    started["result"]["turnId"], "after-reserved-stop",
                    "{started}"
                );
                frames.extend(observed);
            }
        }
    }
    if !matches!(control, Control::Stop) {
        gates.admission.add_permits(1);
    }
    let resolved = collect_until(&mut client, &mut frames, |frame| {
        disposition(frame).is_some_and(|decision| {
            decision["decisionId"] == decision_id && decision["pending"] == false
        })
    })
    .await;
    let resolved = &resolved["params"];
    assert_eq!(resolved["priorSubmissionId"], prior);
    assert_eq!(resolved["successorSubmissionId"], successor);
    assert_eq!(resolved["goalId"], goal_id);
    assert_eq!(
        resolved["priorCompletionNotificationDisposition"],
        "suppress"
    );
    assert_eq!(
        resolved["continued"],
        matches!(control, Control::Admit),
        "{control:?}: {resolved}"
    );
    if matches!(control, Control::Admit) {
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let (paused, observed) = super::goal_interrupt_origin::request(
            &mut client,
            5,
            "session/setGoal",
            json!({
                "sessionId":session,"status":"paused","origin":"user","expectedGoalId":goal_id
            }),
        )
        .await;
        assert!(paused.get("error").is_none(), "{paused}");
        frames.extend(observed);
        gates.successor.add_permits(1);
        collect_until(&mut client, &mut frames, |frame| {
            frame["method"] == "turn/completed" && frame["params"]["turnId"] == successor
        })
        .await;
    } else if !start_after_stop {
        assert_eq!(
            calls.load(Ordering::SeqCst),
            if matches!(control, Control::ProviderFailure) {
                2
            } else {
                1
            }
        );
    }
    if start_after_stop {
        assert!(matches!(control, Control::Stop));
        let completed = collect_until(&mut client, &mut frames, |frame| {
            frame["method"] == "turn/completed"
                && frame["params"]["turnId"] == "after-reserved-stop"
        })
        .await;
        assert_eq!(
            completed["params"]["result"]["finalText"],
            "successor settled"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            frames
                .iter()
                .filter(|frame| frame["method"] == "turn/started"
                    && frame["params"]["submissionId"] == "after-reserved-stop")
                .count(),
            1,
            "the next explicit turn must execute exactly once"
        );
        assert!(
            !frames.iter().any(|frame| disposition(frame)
                .is_some_and(|decision| decision["decisionId"] == decision_id
                    && decision["continued"] == true)),
            "the stopped automatic successor must never become accepted work"
        );
        let (goal, observed) = super::goal_interrupt_origin::request(
            &mut client,
            7,
            "session/goal",
            json!({"sessionId":session}),
        )
        .await;
        assert_eq!(goal["result"]["threadGoal"]["goalId"], goal_id);
        assert_eq!(goal["result"]["threadGoal"]["status"], "paused");
        frames.extend(observed);
    }
    if let Some(replacement) = replacement_goal {
        let (current, observed) = super::goal_interrupt_origin::request(
            &mut client,
            42,
            "session/goal",
            json!({"sessionId":session}),
        )
        .await;
        frames.extend(observed);
        assert_eq!(
            current["result"]["threadGoal"]["goalId"],
            replacement["goalId"]
        );
        assert_eq!(
            current["result"]["threadGoal"]["objective"],
            replacement["objective"]
        );
        assert_eq!(current["result"]["threadGoal"]["status"], "active");
        assert_eq!(current["result"]["threadGoal"]["tokensUsed"], 0);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "the old generation cannot confirm new work"
        );
        let (paused, observed) = super::goal_interrupt_origin::request(
            &mut client, 43, "session/setGoal",
            json!({"sessionId":session,"status":"paused","origin":"user","expectedGoalId":replacement["goalId"]}),
        ).await;
        assert!(paused.get("error").is_none(), "{paused}");
        frames.extend(observed);
        // A fresh generation may have reserved its own idle task. Release its
        // barrier after the explicit pause so it cannot call the provider.
        gates.admission.add_permits(1);
    }
    let (queue, observed) = super::goal_interrupt_origin::request(
        &mut client,
        6,
        "session/queueSnapshot",
        json!({"sessionId":session}),
    )
    .await;
    assert!(queue.get("error").is_none(), "{queue}");
    frames.extend(observed);
    assert_eq!(
        frames
            .iter()
            .filter(|frame| disposition(frame)
                .is_some_and(|decision| decision["decisionId"] == decision_id
                    && decision["pending"] == false))
            .count(),
        1,
        "one decision resolves once across request callbacks and reservation drop"
    );
    if matches!(control, Control::ProviderFailure) {
        let failed = collect_until(&mut client, &mut frames, |frame| {
            frame["method"] == "turn/failed" && frame["params"]["submissionId"] == successor
        })
        .await;
        assert_eq!(
            failed["params"]["terminalNotificationDisposition"],
            "inherit"
        );
        assert!(
            failed["params"]["detail"]["error"]["message"]
                .as_str()
                .is_some_and(|message| !message.is_empty()),
            "{failed}"
        );
        let result = collect_until(&mut client, &mut frames, |frame| {
            frame["params"]["event"]["submissionId"] == successor
                && frame["params"]["event"].get("result").is_some()
        })
        .await;
        assert_eq!(
            result["params"]["event"]["terminalNotificationDisposition"],
            "inherit"
        );
        assert_eq!(result["params"]["event"]["result"]["stopReason"], "error");
        let raw_failure = frames
            .iter()
            .find(|frame| {
                frame["params"]["event"]["submissionId"] == successor
                    && frame["params"]["event"]["event"]["type"] == "turn.failed"
            })
            .expect("the classified successor diagnostic is preserved independently");
        assert_eq!(
            raw_failure["params"]["event"]["terminalNotificationDisposition"],
            "inherit"
        );
        let resolution_position = frames
            .iter()
            .position(|frame| {
                disposition(frame).is_some_and(|decision| {
                    decision["decisionId"] == decision_id && decision["pending"] == false
                })
            })
            .unwrap();
        let terminal_position = frames
            .iter()
            .position(|frame| {
                (frame["method"] == "turn/failed" && frame["params"]["submissionId"] == successor)
                    || (frame["params"]["event"]["submissionId"] == successor
                        && frame["params"]["event"]["event"]["type"] == "turn.failed")
            })
            .expect("the refused factory attempt still publishes its diagnostic failure");
        assert!(
            resolution_position < terminal_position,
            "resolve predecessor suppression before publishing the refused candidate's typed failure"
        );
    }
    client.close().await;
}

#[tokio::test]
async fn actual_admission_confirms_a_reserved_continuation_after_the_predecessor_terminal() {
    continuation_control(Control::Admit).await;
}
#[tokio::test]
async fn a_stopped_goal_has_no_candidate_and_releases_the_predecessor_completion() {
    continuation_control(Control::NoCandidate).await;
}
#[tokio::test]
async fn clearing_a_reserved_continuation_resolves_false_without_provider_work() {
    continuation_control(Control::Clear).await;
}
#[tokio::test]
async fn replacing_a_reserved_generation_resolves_the_old_decision_false() {
    continuation_control(Control::Replace).await;
}
#[tokio::test]
async fn disabling_a_reserved_continuation_resolves_false_without_provider_work() {
    continuation_control(Control::Disable).await;
}
#[tokio::test]
async fn plan_mode_invalidates_a_reserved_continuation_without_provider_work() {
    continuation_control(Control::Plan).await;
}
#[tokio::test]
async fn stop_of_a_reserved_continuation_resolves_false_without_provider_work() {
    continuation_control(Control::Stop).await;
}
#[tokio::test]
async fn stopped_reservation_releases_foreground_for_immediate_user_input() {
    continuation_control_then_start(Control::Stop, true).await;
}
#[tokio::test]
async fn a_provider_factory_failure_resolves_false_instead_of_confirming_continuation() {
    continuation_control(Control::ProviderFailure).await;
}

#[test]
fn continuation_disposition_codecs_enforce_pending_and_resolved_shapes() {
    use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeString};
    use klaus_agent_protocol::types::ProtocolGoalContinuation;
    for extra in [
        json!({"pending":true}),
        json!({"pending":false,"continued":true,"priorCompletionNotificationDisposition":"suppress"}),
        json!({"pending":false,"continued":false,"priorCompletionNotificationDisposition":"inherit"}),
    ] {
        let mut value = json!({"decisionId":"decision","priorSubmissionId":"prior","goalId":"generation","successorSubmissionId":"next"});
        value
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let exact = runtime_json::parse(&RuntimeString::from(value.to_string())).unwrap();
        let decoded: ProtocolGoalContinuation = runtime_codec::decode_value(&exact).unwrap();
        assert_eq!(runtime_codec::encode_value(&decoded).unwrap(), exact);
        value["sessionId"] = "session".into();
        let typed: wire::GoalContinuationNotification =
            serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(typed).unwrap(), value);
    }
    for extra in [
        json!({"pending":true,"continued":true}),
        json!({"pending":true,"continued":false}),
        json!({"pending":true,"continued":null}),
        json!({"pending":false}),
        json!({"pending":false,"continued":null}),
        json!({"pending":false,"continued":"false"}),
        json!({"pending":true,"unknown":true}),
    ] {
        let mut value = json!({"decisionId":"decision","priorSubmissionId":"prior"});
        value
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let exact = runtime_json::parse(&RuntimeString::from(value.to_string())).unwrap();
        assert!(
            runtime_codec::decode_value::<ProtocolGoalContinuation>(&exact).is_err(),
            "{value}"
        );
        value["sessionId"] = "session".into();
        assert!(
            serde_json::from_value::<wire::GoalContinuationNotification>(value.clone()).is_err(),
            "{value}"
        );
    }
    let schema =
        serde_json::to_value(schemars::schema_for!(wire::GoalContinuationNotification)).unwrap();
    assert_eq!(schema["oneOf"].as_array().unwrap().len(), 3);
}
