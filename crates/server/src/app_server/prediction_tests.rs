use super::*;

async fn seeded_client(replies: &[&str]) -> (TestClient, String, String) {
    let turns: ScriptedTurns = replies.iter().map(|reply| text_turn(reply)).collect();
    let agent = make_agent_with_call_model_and_config(
        scripted_call_model(turns),
        Some(json!({"agentMemory":false,"autoMemory":false,"autoDream":false})),
    );
    let mut client = TestClient::start(create_session_for_agent_tests(agent));
    client.initialize().await;
    client
        .send(json!({"id":2,"method":"session/start","params":{"init":{}}}))
        .await;
    let (response, _) = client.response_for(2).await;
    let session_id = response["result"]["sessionId"].as_str().unwrap().to_owned();
    client.send(json!({"id":3,"method":"turn/start","params":{"sessionId":session_id,"submissionId":"source-turn","input":"parent context"}})).await;
    let (_, seen) = client.response_for(3).await;
    if !seen
        .iter()
        .any(|message| message["method"] == "turn/completed")
    {
        loop {
            if client.next_message().await["method"] == "turn/completed" {
                break;
            }
        }
    }
    client
        .send(
            json!({"id":90,"method":"session/predictionContext","params":{"sessionId":session_id}}),
        )
        .await;
    let (context, _) = client.response_for(90).await;
    let source_turn_id = context["result"]["turnId"].as_str().unwrap().to_owned();
    (client, session_id, source_turn_id)
}

#[tokio::test]
async fn prediction_facade_returns_structured_ephemeral_result_and_live_context() {
    let (mut client, session_id, source_turn_id) =
        seeded_client(&["parent reply", r#"{"suggestion":"Next step"}"#]).await;
    client
        .send(json!({"id":4,"method":"session/history","params":{"sessionId":session_id}}))
        .await;
    let (before, _) = client.response_for(4).await;
    client
        .send(
            json!({"id":5,"method":"session/predictionContext","params":{"sessionId":session_id}}),
        )
        .await;
    let (context, _) = client.response_for(5).await;
    assert_eq!(context["result"]["turnId"], source_turn_id);
    assert_eq!(context["result"]["running"], false);
    assert_eq!(context["result"]["planMode"], false);
    assert!(context["result"]["model"].is_string());
    assert!(context["result"]["provider"].is_string());
    client.send(json!({"id":6,"method":"session/predictMessage","params":{"sessionId":session_id,"requestId":"prediction-one","sourceTurnId":source_turn_id,"prompt":"dynamic prompt","effort":"high"}})).await;
    let (response, seen) = client.response_for(6).await;
    assert_eq!(
        response["result"],
        json!({"suggestion":"Next step"}),
        "prediction response: {response}"
    );
    assert!(!seen
        .iter()
        .any(|message| message["method"] == "turn/completed"));
    client
        .send(json!({"id":7,"method":"session/history","params":{"sessionId":session_id}}))
        .await;
    let (after, _) = client.response_for(7).await;
    assert_eq!(before["result"], after["result"]);
    client.close().await;
}

#[tokio::test]
async fn prediction_facade_stale_source_returns_null_without_admitting_a_turn() {
    let (mut client, session_id, _source_turn_id) = seeded_client(&["parent reply"]).await;
    client.send(json!({"id":4,"method":"session/predictMessage","params":{"sessionId":session_id,"requestId":"prediction-stale","sourceTurnId":"old-turn","prompt":"dynamic prompt"}})).await;
    let (response, seen) = client.response_for(4).await;
    assert_eq!(response["result"], json!({"suggestion":null}));
    assert!(!seen
        .iter()
        .any(|message| message["method"] == "turn/completed"));
    client.send(json!({"id":5,"method":"session/cancelPrediction","params":{"sessionId":session_id,"requestId":"prediction-stale"}})).await;
    let (cancel, _) = client.response_for(5).await;
    assert!(cancel["result"]["submissionId"].is_string());
    client.close().await;
}

#[tokio::test]
async fn prediction_facade_cancel_before_queued_request_prevents_provider_work() {
    let (mut client, session_id, source_turn_id) =
        seeded_client(&["parent reply", r#"{"suggestion":"must not run"}"#]).await;
    client.send(json!({"id":4,"method":"session/cancelPrediction","params":{"sessionId":session_id,"requestId":"queued-prediction"}})).await;
    let (cancel, _) = client.response_for(4).await;
    assert!(cancel["result"]["submissionId"].is_string());
    client.send(json!({"id":5,"method":"session/predictMessage","params":{"sessionId":session_id,"requestId":"queued-prediction","sourceTurnId":source_turn_id,"prompt":"dynamic prompt"}})).await;
    let (response, seen) = client.response_for(5).await;
    assert_eq!(response["error"]["message"], "prediction cancelled");
    assert!(!seen
        .iter()
        .any(|message| message["method"] == "turn/completed"));
    client.send(json!({"id":6,"method":"session/predictMessage","params":{"sessionId":session_id,"requestId":"fresh-prediction","sourceTurnId":source_turn_id,"prompt":"dynamic prompt"}})).await;
    let (fresh, _) = client.response_for(6).await;
    assert_eq!(
        fresh["result"]["suggestion"], "must not run",
        "the cancelled request never consumed the scripted provider response"
    );
    client.close().await;
}
