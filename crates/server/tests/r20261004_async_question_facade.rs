//! Regression: optional questions need named durable acceptance and replayable
//! product events; a submission acknowledgement is not an answer receipt.
//! Work item: 2026-10-04 asynchronous user-question lifecycle.

use super::*;
use futures::StreamExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;

#[tokio::test]
async fn async_question_named_facade_accepts_partial_answers_and_projects_user_provenance() {
    let calls = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());
    let agent = make_agent_with_call_model(Box::new({
        let calls = calls.clone();
        let release = release.clone();
        move |_, signal| {
            let index = calls.fetch_add(1, Ordering::SeqCst);
            if index == 0 {
                Box::pin(futures::stream::iter(vec![
                    ev::message_start(None),
                    ModelStreamEvent::ToolUseStart { id: "question-call".into(), name: "ask_user_async".into(), thought_signature: None, tool_kind: None, input_format: None },
                    ModelStreamEvent::ToolUseInputDelta { id: "question-call".into(), partial_json: r#"{"questions":[{"question":"Layout?","header":"Layout","options":[{"label":"Dense","description":"rows"},{"label":"Spacious","description":"space"}],"multiSelect":false},{"question":"Language?","header":"Language","options":[{"label":"English","description":"en"},{"label":"Chinese","description":"zh"}],"multiSelect":false}]}"#.into() },
                    ModelStreamEvent::ToolUseStop { id: "question-call".into() },
                    ev::stop(Some(ModelStopReason::ToolUse), None),
                ])) as klaus_agent_core::types::provider::ModelStream
            } else if index == 1 {
                let release = release.clone();
                Box::pin(futures::stream::once(async move {
                    tokio::select! { _ = release.notified() => {}, _ = signal.cancelled() => {} }
                    text_turn("continuing")
                }).flat_map(futures::stream::iter))
            } else {
                Box::pin(futures::stream::iter(text_turn("received")))
            }
        }
    }));
    let mut client = TestClient::start(create_session_for_agent_tests(agent));
    client.send(json!({"id":1,"method":"initialize","params":{"capabilities":{"productEvents":true,"revisionedSessionEvents":true}}})).await;
    client.response_for(1).await;
    client.send(json!({"method":"initialized"})).await;
    client
        .send(
            json!({"id":2,"method":"session/start","params":{"init":{"threadId":"async-facade"}}}),
        )
        .await;
    client.response_for(2).await;
    client.send(json!({"id":3,"method":"turn/start","params":{"sessionId":"async-facade","input":"work"}})).await;
    let (_, mut events) = client.response_for(3).await;
    let question = loop {
        if let Some(event) = events
            .iter()
            .find(|event| event["method"] == "question/requested")
        {
            assert!(event["params"]["streamRevision"].is_u64());
            break event["params"]["question"].clone();
        }
        events.push(client.next_message().await);
    };
    assert_eq!(question["state"], "pending");
    assert_eq!(question["toolCallId"], "question-call");
    client.send(json!({"id":4,"method":"session/getPendingQuestions","params":{"sessionId":"async-facade"}})).await;
    let (pending, _) = client.response_for(4).await;
    assert_eq!(pending["result"]["questions"][0], question);
    let response = json!({"sessionId":"async-facade","requestId":question["requestId"],"turnId":question["turnId"],"clientResponseId":"first","answers":{"Layout?":"Dense"}});
    client
        .send(json!({"id":5,"method":"session/answerQuestion","params":response}))
        .await;
    let (first, mut answer_events) = client.response_for(5).await;
    assert_eq!(first["result"]["accepted"], true, "{first}");
    assert_eq!(first["result"]["state"], "pending");
    assert!(
        first["result"]["submissionId"].is_null(),
        "the reply is a durable direct receipt"
    );
    client
        .send(json!({"id":6,"method":"session/answerQuestion","params":response}))
        .await;
    let (duplicate, observed) = client.response_for(6).await;
    answer_events.extend(observed);
    assert_eq!(duplicate["result"], first["result"]);
    client.send(json!({"id":7,"method":"session/getPendingQuestions","params":{"sessionId":"async-facade"}})).await;
    let (pending, observed) = client.response_for(7).await;
    answer_events.extend(observed);
    assert_eq!(
        pending["result"]["questions"][0]["answers"]["Layout?"],
        "Dense"
    );
    assert_eq!(
        pending["result"]["questions"][0]["request"]["questions"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    client.send(json!({"id":8,"method":"session/answerQuestion","params":{"sessionId":"async-facade","requestId":question["requestId"],"turnId":question["turnId"],"clientResponseId":"second","answers":{"Language?":"Chinese"}}})).await;
    let (second, observed) = client.response_for(8).await;
    answer_events.extend(observed);
    assert_eq!(second["result"]["state"], "answered", "{second}");
    release.notify_one();
    loop {
        let event = client.next_message().await;
        let terminal = event["method"] == "turn/completed";
        answer_events.push(event);
        if terminal {
            break;
        }
    }
    assert!(answer_events
        .iter()
        .any(|event| event["method"] == "question/resolved"
            && event["params"]["questionResolved"]["userMessage"]["questionAnswer"]
                ["clientResponseId"]
                == "first"));
    client.send(json!({"id":9,"method":"session/getPendingQuestions","params":{"sessionId":"async-facade"}})).await;
    let (pending, _) = client.response_for(9).await;
    assert_eq!(pending["result"]["questions"], json!([]));
    client
        .send(json!({"id":10,"method":"engine/readThread","params":{"threadId":"async-facade"}}))
        .await;
    let (history, _) = client.response_for(10).await;
    assert!(history.to_string().contains("questionAnswer"), "{history}");
    assert!(history.to_string().contains("Dense"), "{history}");
    client.send(json!({"id":11,"method":"session/answerQuestion","params":{"sessionId":"async-facade","requestId":question["requestId"],"turnId":question["turnId"],"clientResponseId":"stale","answers":{"Layout?":"Spacious"}}})).await;
    let (stale, _) = client.response_for(11).await;
    assert!(stale["error"].is_object());
    client.close().await;
}
