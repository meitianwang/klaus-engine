//! Defect: live commentary and final output were indistinguishable to facade clients.
//! Root cause: text delta notifications omitted message identity and provider phase.
//! Fixed by: 2026-09-05 desktop transcript parity work item.

use super::*;

#[tokio::test]
async fn native_final_phase_reaches_facade_before_turn_completion_and_survives_history() {
    let release = Arc::new(tokio::sync::Notify::new());
    let provider_release = release.clone();
    let agent = make_agent_with_call_model_and_config(
        Box::new(move |_req, _signal| {
            let release = provider_release.clone();
            use futures::StreamExt;
            Box::pin(
                futures::stream::iter(vec![
                    ev::message_start(None),
                    ModelStreamEvent::TextMessage {
                        id: "native-commentary".into(),
                        phase: Some("commentary".into()),
                    },
                    ev::text("Checking"),
                    ModelStreamEvent::TextMessage {
                        id: "native-final".into(),
                        phase: Some("final_answer".into()),
                    },
                    ev::text("Answer"),
                ])
                .chain(futures::stream::once(async move {
                    release.notified().await;
                    ev::stop(Some(ModelStopReason::EndTurn), None)
                })),
            )
        }),
        Some(json!({"agentMemory": false, "autoMemory": false, "autoDream": false})),
    );
    let mut client = TestClient::start(create_session_for_agent_tests(agent));
    client
        .send(
            json!({"id":1,"method":"initialize","params":{"capabilities":{
                "productEvents":true,"optOutNotificationMethods":["session/event"]
            }}}),
        )
        .await;
    client.response_for(1).await;
    client
        .send(json!({"id":2,"method":"session/start","params":{"init":{}}}))
        .await;
    let (response, _) = client.response_for(2).await;
    let session_id = response["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    client
        .send(json!({"id":3,"method":"turn/start","params":{"sessionId":session_id,"input":"hi"}}))
        .await;
    let (_, pending) = client.response_for(3).await;
    let mut pending = std::collections::VecDeque::from(pending);
    let mut commentary_id = None;
    let final_id;
    let mut saw_final_start = false;
    loop {
        let message = match pending.pop_front() {
            Some(message) => message,
            None => client.next_message().await,
        };
        let method = message["method"].as_str().unwrap_or("");
        assert_ne!(method, "turn/completed", "the provider is still parked");
        if method == "item/started" && message["params"]["item"]["phase"] == "final_answer" {
            saw_final_start = true;
        }
        if method == "item/agentMessage/delta" {
            let params = &message["params"];
            let id = params["messageId"]
                .as_str()
                .expect("stable message id")
                .to_string();
            if params["phase"] == "commentary" {
                assert_eq!(params["delta"], "Checking");
                commentary_id = Some(id);
            } else if params["phase"] == "final_answer" {
                assert!(saw_final_start, "final start must precede its first delta");
                assert_eq!(params["delta"], "Answer");
                final_id = Some(id);
                break;
            }
        }
    }
    assert!(commentary_id.is_some());
    assert_ne!(commentary_id, final_id);
    release.notify_one();
    let mut completed = Vec::new();
    loop {
        let message = client.next_message().await;
        if message["method"] == "item/completed"
            && message["params"]["item"]["type"] == "agent_message"
        {
            completed.push(message["params"]["item"].clone());
        }
        if message["method"] == "turn/completed" {
            assert_eq!(
                message["params"]["result"]["finalMessageId"].as_str(),
                final_id.as_deref()
            );
            break;
        }
    }
    assert_eq!(completed.len(), 2);
    assert_eq!(completed[0]["id"].as_str(), commentary_id.as_deref());
    assert_eq!(completed[1]["id"].as_str(), final_id.as_deref());
    client
        .send(json!({"id":4,"method":"session/history","params":{"sessionId":session_id}}))
        .await;
    let (history, _) = client.response_for(4).await;
    let blocks: Vec<_> = history["result"]["history"]
        .as_array()
        .expect("history array")
        .iter()
        .filter(|message| message["type"] == "assistant")
        .flat_map(|message| message["content"].as_array().into_iter().flatten())
        .filter(|block| block["type"] == "text")
        .collect();
    assert_eq!(
        blocks.len(),
        2,
        "commentary and final remain separate in history"
    );
    assert_eq!(blocks[0]["messageId"].as_str(), commentary_id.as_deref());
    assert_eq!(blocks[0]["phase"], "commentary");
    assert_eq!(blocks[1]["messageId"].as_str(), final_id.as_deref());
    assert_eq!(blocks[1]["phase"], "final_answer");
    client.close().await;
}

#[tokio::test]
async fn unknown_provider_phase_is_not_guessed_until_the_authoritative_final_identity() {
    let mut client = TestClient::start(make_create_session(&["Answer"]));
    client
        .send(
            json!({"id":1,"method":"initialize","params":{"capabilities":{
                "productEvents":true,"optOutNotificationMethods":["session/event"]
            }}}),
        )
        .await;
    client.response_for(1).await;
    client
        .send(json!({"id":2,"method":"session/start","params":{"init":{}}}))
        .await;
    let (response, _) = client.response_for(2).await;
    let session_id = response["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    client
        .send(json!({"id":3,"method":"turn/start","params":{"sessionId":session_id,"input":"hi"}}))
        .await;
    let (_, messages) = client.response_for(3).await;
    let mut pending = std::collections::VecDeque::from(messages);
    let mut delta_id = None;
    let mut final_update = None;
    loop {
        let message = match pending.pop_front() {
            Some(message) => message,
            None => client.next_message().await,
        };
        if message["method"] == "item/agentMessage/delta" {
            assert!(message["params"].get("phase").is_none());
            delta_id = message["params"]["messageId"].as_str().map(str::to_string);
        }
        if message["method"] == "item/updated"
            && message["params"]["item"]["phase"] == "final_answer"
        {
            final_update = message["params"]["item"]["id"].as_str().map(str::to_string);
        }
        if message["method"] == "turn/completed" {
            break;
        }
    }
    assert!(delta_id.is_some());
    assert_eq!(delta_id, final_update);
    client.close().await;
}
