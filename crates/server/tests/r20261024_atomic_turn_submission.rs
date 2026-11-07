//! Defect 20261024: clients raced an idle observation with unconditional turn start.
//! Root cause: admission and steering used different acknowledgement boundaries.
//! Fix: turn/submit makes the decision under the protocol foreground gate.
//! This public-facade regression is mounted by server to avoid a core -> server dependency.

use super::*;
use klaus_agent_core::schemas::message::{Message, SystemSubtype};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

pub(super) async fn rpc(
    client: &mut TestClient,
    id: u64,
    method: &str,
    params: Value,
) -> (Value, Vec<Value>) {
    client
        .send(json!({"id":id,"method":method,"params":params}))
        .await;
    let (reply, seen) = client.response_for(id).await;
    assert!(reply.get("error").is_none(), "{reply}");
    (reply["result"].clone(), seen)
}

pub(super) async fn fixture() -> (
    TestClient,
    String,
    Arc<AtomicUsize>,
    Arc<Mutex<Option<Arc<Session>>>>,
) {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let agent = make_agent_with_call_model_and_config(
        Box::new(move |_, signal| {
            if count.fetch_add(1, Ordering::SeqCst) == 0 {
                Box::pin(futures::stream::once(async move {
                    signal.cancelled().await;
                    ev::stop(Some(ModelStopReason::EndTurn), None)
                }))
            } else {
                Box::pin(futures::stream::iter(text_turn("continued")))
            }
        }),
        Some(json!({"prewarm":false,"goal":false,"autoMemory":false,"autoDream":false})),
    );
    let live = Arc::new(Mutex::new(None::<Arc<Session>>));
    let capture = live.clone();
    let factory = create_session_for_agent_tests(agent);
    let create: CreateSession = Arc::new(move |ctx| {
        let factory = factory.clone();
        let capture = capture.clone();
        Box::pin(async move {
            let session = factory(ctx).await?;
            *capture.lock().unwrap() = Some(session.clone());
            Ok(session)
        })
    });
    let mut client = TestClient::start(create);
    client.initialize().await;
    let (reply, _) = rpc(&mut client, 2, "session/start", json!({"init":{}})).await;
    (
        client,
        reply["sessionId"].as_str().unwrap().to_owned(),
        calls,
        live,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn atomic_submit_concurrent_idle_starts_admit_exactly_one_and_reject_without_retention() {
    let (mut client, session_id, calls, live) = fixture().await;
    for id in 10..34 {
        client
            .send(json!({"id":id,"method":"turn/submit","params":{
                "sessionId":session_id,"submissionId":format!("candidate-{id}"),
                "mode":"start-if-idle","input":format!("candidate {id}"),
                "displayMetadata":{"displayText":format!("Display {id}")}
            }}))
            .await;
    }
    let mut results = Vec::new();
    while results.len() < 24 {
        let reply = client.next_message().await;
        if reply["id"]
            .as_u64()
            .is_some_and(|id| (10..34).contains(&id))
        {
            assert!(reply.get("error").is_none(), "{reply}");
            results.push(reply["result"].clone());
        }
    }
    let starts: Vec<_> = results
        .iter()
        .filter(|reply| reply["status"] == "started")
        .collect();
    assert_eq!(starts.len(), 1, "{results:?}");
    for reply in results.iter().filter(|reply| reply["status"] != "started") {
        assert_eq!(reply["status"], "not-submitted");
        assert_eq!(reply["reason"]["code"], "not-idle");
        assert_eq!(reply["reason"]["activeTurnId"], starts[0]["turnId"]);
    }
    let session = live.lock().unwrap().clone().unwrap();
    tokio::time::timeout(REPLY_TIMEOUT, async {
        while calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(session.queue_snapshot().items.is_empty());
    assert_eq!(
        session
            .history()
            .iter()
            .filter(
                |message| matches!(message, Message::User(user) if user.content.iter().any(|block| {
                    matches!(block, klaus_agent_core::schemas::content::ContentBlock::Text(text)
                        if text.text.try_to_utf8().unwrap().starts_with("candidate "))
                }))
            )
            .count(),
        1
    );
    let display_count = session.history().iter().filter(|message| matches!(message, Message::System(system) if system.subtype == SystemSubtype::UserDisplay)).count();
    assert_eq!(
        display_count, 1,
        "only the accepted input has display metadata"
    );
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn atomic_submit_preserves_late_applied_receipt_delivery_metadata_and_stop_queue() {
    let (mut client, session_id, calls, live) = fixture().await;
    let (started, _) = rpc(
        &mut client,
        10,
        "turn/submit",
        json!({
            "sessionId":session_id,"mode":"start-or-steer","input":"block","submissionId":"owner"
        }),
    )
    .await;
    assert_eq!(
        started,
        json!({"status":"started","submissionId":"owner","turnId":"owner"})
    );
    tokio::time::timeout(REPLY_TIMEOUT, async {
        while calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let session = live.lock().unwrap().clone().unwrap();
    let model_before = session.model_runtime().1.model;
    for (id, extra, reason) in [
        (
            11,
            json!({"input":"stale","mode":"steer","expectedTurnId":"previous"}),
            "expected-turn-mismatch",
        ),
        (
            12,
            json!({"input":"busy","mode":"start-if-idle","options":{"model":"changed"}}),
            "not-idle",
        ),
        (
            13,
            json!({"input":"  \n","mode":"start-or-steer","options":{"model":"changed"}}),
            "empty-input",
        ),
        (
            14,
            json!({"input":{"bad":true},"mode":"steer"}),
            "invalid-input",
        ),
        (
            15,
            json!({"input":"invalid queue id","mode":"steer","queueMessageId":"bad-id"}),
            "invalid-input",
        ),
    ] {
        let mut params = extra;
        params["sessionId"] = session_id.clone().into();
        let (reply, _) = rpc(&mut client, id, "turn/submit", params).await;
        assert_eq!(reply["status"], "not-submitted");
        assert_eq!(reply["reason"]["code"], reason, "{reply}");
    }
    assert!(session.queue_snapshot().items.is_empty());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(session.model_runtime().1.model, model_before);
    let queue_id = "c9a02ab1-092b-4206-8b14-bbb7d37d5f20";
    let display =
        json!({"displayText":"Inspect Orders","browserTabs":[{"tabId":42,"title":"Orders"}]});
    let (steered, _) = rpc(
        &mut client,
        16,
        "turn/submit",
        json!({
            "sessionId":session_id,"mode":"start-or-steer","expectedTurnId":"owner",
            "input":"keep after stop","submissionId":"followup","queueMessageId":queue_id,
            "delivery":"user","displayMetadata":display,"options":{"model":"ignored-on-steer"}
        }),
    )
    .await;
    assert_eq!(
        steered,
        json!({"status":"steered","submissionId":"followup","turnId":"owner"})
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "steering does not start another model request"
    );
    assert_eq!(session.model_runtime().1.model, model_before);
    let (snapshot, _) = rpc(
        &mut client,
        17,
        "session/queueSnapshot",
        json!({"sessionId":session_id}),
    )
    .await;
    assert_eq!(
        snapshot["queueSnapshot"]["items"][0]["message"]["uuid"],
        queue_id
    );
    assert_eq!(
        snapshot["queueSnapshot"]["items"][0]["message"]["displayMetadata"],
        display
    );
    assert_eq!(
        snapshot["queueSnapshot"]["items"][0]["message"]["priority"],
        "now"
    );
    let (_, seen) = rpc(
        &mut client,
        18,
        "turn/interrupt",
        json!({"sessionId":session_id}),
    )
    .await;
    let mut seen = std::collections::VecDeque::from(seen);
    loop {
        let message = match seen.pop_front() {
            Some(message) => message,
            None => client.next_message().await,
        };
        if message["method"] == "turn/completed" && message["params"]["turnId"] == "owner" {
            break;
        }
    }
    let (late, _) = rpc(
        &mut client,
        19,
        "turn/submit",
        json!({"sessionId":session_id,"mode":"steer","expectedTurnId":"owner","input":"too late"}),
    )
    .await;
    assert_eq!(late["reason"]["code"], "no-active-turn");
    let (_, seen) = rpc(
        &mut client,
        20,
        "session/resumeQueued",
        json!({"sessionId":session_id,"submissionId":"resume"}),
    )
    .await;
    let mut seen = std::collections::VecDeque::from(seen);
    let mut applied = false;
    loop {
        let message = match seen.pop_front() {
            Some(message) => message,
            None => client.next_message().await,
        };
        let event = &message["params"]["event"];
        if event["submissionId"] == "followup" && event["steered"]["applied"] == true {
            applied = true;
        }
        if message["method"] == "turn/completed" && message["params"]["turnId"] == "resume" {
            break;
        }
    }
    assert!(
        applied,
        "steered acknowledgement retains its later applied receipt"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(session.queue_snapshot().items.is_empty());
    assert_eq!(
        session
            .history()
            .iter()
            .filter(|message| matches!(message, Message::User(user) if user.uuid == queue_id))
            .count(),
        1
    );
    let history = session.history();
    assert!(history.iter().any(|message| matches!(message, Message::System(system) if system.subtype == SystemSubtype::UserDisplay && system.text.try_to_utf8().unwrap().contains("Inspect Orders"))));
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn atomic_submit_invalid_idle_input_never_starts_or_retains_a_turn() {
    let (mut client, session_id, calls, live) = fixture().await;
    for (id, input) in [
        (10, json!("")),
        (11, json!([])),
        (12, json!([{"type":"text","text":" \n"}])),
    ] {
        let (reply, _) = rpc(
            &mut client,
            id,
            "turn/submit",
            json!({"sessionId":session_id,"mode":"start-if-idle","input":input}),
        )
        .await;
        assert_eq!(reply["reason"]["code"], "empty-input", "{reply}");
    }
    let (reply, _) = rpc(
        &mut client,
        13,
        "turn/submit",
        json!({
            "sessionId":session_id,"mode":"start-if-idle","input":"retired option",
            "options":{"permissionMode":"bypassPermissions"}
        }),
    )
    .await;
    assert_eq!(reply["reason"]["code"], "invalid-input", "{reply}");
    let (reply, _) = rpc(
        &mut client,
        14,
        "turn/submit",
        json!({
            "sessionId":session_id,"mode":"start-or-steer","expectedTurnId":"old","input":"stale"
        }),
    )
    .await;
    assert_eq!(reply["reason"]["code"], "expected-turn-mismatch");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    if let Some(session) = live.lock().unwrap().as_ref() {
        assert!(session.queue_snapshot().items.is_empty());
        assert!(!session
            .history()
            .iter()
            .any(|message| matches!(message, Message::User(_))));
    }
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn atomic_submit_review_worker_keeps_its_non_steerable_lease() {
    let (mut client, session_id, calls, live) = fixture().await;
    let (_, _) = rpc(
        &mut client,
        10,
        "session/reviewStart",
        json!({
            "sessionId":session_id,"submissionId":"review-owner",
            "target":{"type":"custom","instructions":"Inspect this code"}
        }),
    )
    .await;
    tokio::time::timeout(REPLY_TIMEOUT, async {
        while calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    for (id, mode, reason) in [
        (11, "steer", "active-turn-not-steerable"),
        (12, "start-or-steer", "active-turn-not-steerable"),
        (13, "start-if-idle", "not-idle"),
    ] {
        let (reply, _) = rpc(
            &mut client,
            id,
            "turn/submit",
            json!({
                "sessionId":session_id,"mode":mode,"input":"do not retain this",
                "expectedTurnId":"review-owner"
            }),
        )
        .await;
        assert_eq!(reply["status"], "not-submitted");
        assert_eq!(reply["reason"]["code"], reason, "{reply}");
        assert_eq!(reply["reason"]["turnKind"], "review", "{reply}");
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(live
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .queue_snapshot()
        .items
        .is_empty());
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn atomic_submit_queued_compact_does_not_block_the_current_regular_turn() {
    let (mut client, session_id, calls, live) = fixture().await;
    let (started, _) = rpc(
        &mut client,
        10,
        "turn/submit",
        json!({
            "sessionId":session_id,"mode":"start-if-idle","input":"block","submissionId":"regular"
        }),
    )
    .await;
    assert_eq!(started["status"], "started");
    tokio::time::timeout(REPLY_TIMEOUT, async {
        while calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let (_, _) = rpc(
        &mut client,
        11,
        "session/compactStart",
        json!({
            "sessionId":session_id,"submissionId":"waiting-compact"
        }),
    )
    .await;
    // The terminal snapshot reply proves the preceding compact frame has
    // crossed raw admission, while the blocked regular turn holds execution.
    let (_, _) = rpc(
        &mut client,
        12,
        "session/queueSnapshot",
        json!({"sessionId":session_id}),
    )
    .await;
    let (reply, _) = rpc(
        &mut client,
        13,
        "turn/submit",
        json!({
            "sessionId":session_id,"mode":"steer","expectedTurnId":"regular",
            "submissionId":"current-followup","input":"continue the current turn","delivery":"user"
        }),
    )
    .await;
    assert_eq!(
        reply,
        json!({"status":"steered","submissionId":"current-followup","turnId":"regular"})
    );
    let session = live.lock().unwrap().clone().unwrap();
    assert_eq!(session.queued_messages().len(), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn atomic_submit_steer_requires_an_explicit_target_without_input_retention() {
    let (mut client, session_id, calls, live) = fixture().await;
    let (reply, _) = rpc(
        &mut client,
        10,
        "turn/submit",
        json!({
            "sessionId":session_id,"mode":"steer","input":"no idle target"
        }),
    )
    .await;
    assert_eq!(reply["reason"]["code"], "invalid-input");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let (reply, _) = rpc(
        &mut client,
        11,
        "turn/submit",
        json!({
            "sessionId":session_id,"mode":"start-or-steer","input":"block","submissionId":"regular"
        }),
    )
    .await;
    assert_eq!(
        reply["status"], "started",
        "automatic admission may omit its target"
    );
    tokio::time::timeout(REPLY_TIMEOUT, async {
        while calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    for (id, expected) in [(12, None), (13, Some(""))] {
        let mut params = json!({
            "sessionId":session_id,"mode":"steer","input":"do not keep",
            "options":{"model":"do-not-apply"},"displayMetadata":{"displayText":"do not keep"}
        });
        if let Some(expected) = expected {
            params["expectedTurnId"] = expected.into();
        }
        let (reply, _) = rpc(&mut client, id, "turn/submit", params).await;
        assert_eq!(reply["status"], "not-submitted");
        assert_eq!(reply["reason"]["code"], "invalid-input", "{reply}");
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let session = live.lock().unwrap().clone().unwrap();
    assert!(session.queued_messages().is_empty());
    assert!(!session
        .history()
        .iter()
        .any(|message| matches!(message, Message::System(system)
        if system.subtype == SystemSubtype::UserDisplay
            && system.text.try_to_utf8().unwrap().contains("do not keep"))));
    client.close().await;
}

#[test]
fn atomic_submit_schema_publishes_operation_kind_and_target_constraint() {
    let bundle = schema_bundle();
    let result = &bundle["requests"]["turn/submit"]["result"];
    assert_eq!(
        result["definitions"]["TurnSubmitKind"]["enum"],
        json!(["review", "compact"])
    );
    assert!(result["definitions"]["TurnSubmitReason"]["properties"]
        .get("turnKind")
        .is_some());
    let target = &bundle["requests"]["turn/submit"]["params"]["properties"]["expectedTurnId"];
    assert!(target.to_string().contains("minLength"), "{target}");
    assert!(target["description"].as_str().unwrap().contains("Required"));
    for (kind, value) in [
        (super::super::wire::TurnSubmitKind::Review, "review"),
        (super::super::wire::TurnSubmitKind::Compact, "compact"),
    ] {
        let reason = super::super::wire::TurnSubmitReason {
            code: super::super::wire::TurnSubmitReasonCode::ActiveTurnNotSteerable,
            message: "operation cannot accept input".into(),
            active_turn_id: Some("operation".into()),
            turn_kind: Some(kind),
        };
        assert_eq!(serde_json::to_value(reason).unwrap()["turnKind"], value);
    }
}
