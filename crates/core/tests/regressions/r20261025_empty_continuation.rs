//! Defect 20261025: continuing after interruption was rejected as an empty message.
//! Root cause: unified submission rejected the empty input used to resume history.
//! Fix: continue-if-idle admits an empty parts list through the atomic idle gate.
//! Mounted by server to exercise the public facade without a core -> server dependency.

use super::r20261024_atomic_turn_submission::{fixture, rpc};
use super::*;
use klaus_agent_core::schemas::message::Message;
use std::collections::VecDeque;
use std::sync::atomic::Ordering;

async fn frame_matching(
    client: &mut TestClient,
    seen: Vec<Value>,
    predicate: impl Fn(&Value) -> bool,
) -> Value {
    let mut seen = VecDeque::from(seen);
    loop {
        let frame = match seen.pop_front() {
            Some(frame) => frame,
            None => client.next_message().await,
        };
        if predicate(&frame) {
            return frame;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_continuation_after_stop_reuses_history_without_another_user_message() {
    let (mut client, session_id, calls, live) = fixture().await;
    let (started, _) = rpc(
        &mut client, 10, "turn/submit",
        json!({"sessionId":session_id,"submissionId":"original","mode":"start-if-idle","input":"original request"}),
    ).await;
    assert_eq!(started["status"], "started");
    tokio::time::timeout(REPLY_TIMEOUT, async {
        while calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let (busy, _) = rpc(
        &mut client,
        11,
        "turn/submit",
        json!({"sessionId":session_id,"submissionId":"busy","mode":"continue-if-idle","input":[]}),
    )
    .await;
    assert_eq!(busy["reason"]["code"], "not-idle", "{busy}");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let (_, seen) = rpc(
        &mut client,
        12,
        "turn/interrupt",
        json!({"sessionId":session_id,"submissionId":"stop","expectedTurnId":"original"}),
    )
    .await;
    let stopped = frame_matching(&mut client, seen, |frame| {
        let event = &frame["params"]["event"];
        event["submissionId"] == "stop" && event.get("turnInterrupt").is_some()
    })
    .await;
    assert_eq!(
        stopped["params"]["event"]["turnInterrupt"]["accepted"], true,
        "{stopped}"
    );
    let (resumed, seen) = rpc(
        &mut client, 13, "turn/submit",
        json!({"sessionId":session_id,"submissionId":"continued","mode":"continue-if-idle","input":[]}),
    ).await;
    assert_eq!(
        resumed,
        json!({"status":"started","submissionId":"continued","turnId":"continued"})
    );
    let completed = frame_matching(&mut client, seen, |frame| {
        frame["method"] == "turn/completed" && frame["params"]["turnId"] == "continued"
    })
    .await;
    assert_eq!(
        completed["params"]["result"]["stopReason"], "completed",
        "{completed}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "one continuation reaches the provider"
    );
    let session = live.lock().unwrap().clone().unwrap();
    let history = session.history();
    let users: Vec<_> = history
        .iter()
        .filter_map(|message| match message {
            Message::User(user) if user.is_meta != Some(true) => Some(user),
            _ => None,
        })
        .collect();
    assert_eq!(
        users.len(),
        1,
        "continuation preserves genuine user input: {users:?}; history: {history:?}"
    );
    assert!(matches!(users[0].content.as_slice(),
        [klaus_agent_core::schemas::content::ContentBlock::Text(text)]
            if text.text.try_to_utf8().unwrap() == "original request"
    ));
    assert!(
        session.queue_snapshot().items.is_empty(),
        "a refused continuation is never queued"
    );
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_continuation_keeps_ordinary_message_validation_and_requires_no_new_input() {
    let (mut client, session_id, calls, live) = fixture().await;
    for (index, input) in [
        json!(""),
        json!(" \n"),
        json!([]),
        json!([{"type":"text","text":" "}]),
    ]
    .into_iter()
    .enumerate()
    {
        let (reply, _) = rpc(
            &mut client,
            10 + index as u64,
            "turn/submit",
            json!({"sessionId":session_id,"mode":"start-if-idle","input":input}),
        )
        .await;
        assert_eq!(reply["reason"]["code"], "empty-input", "{reply}");
    }
    for (index, input) in [
        json!(""),
        json!("new request"),
        json!([{"type":"text","text":"new request"}]),
    ]
    .into_iter()
    .enumerate()
    {
        let (reply, _) = rpc(
            &mut client,
            20 + index as u64,
            "turn/submit",
            json!({"sessionId":session_id,"mode":"continue-if-idle","input":input}),
        )
        .await;
        assert_eq!(reply["reason"]["code"], "invalid-input", "{reply}");
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(
        live.lock().unwrap().is_none(),
        "invalid requests do not initialize a session"
    );
    client.close().await;
}
