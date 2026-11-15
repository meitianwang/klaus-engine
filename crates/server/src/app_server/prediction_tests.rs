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

#[tokio::test]
async fn persisted_side_task_has_no_prediction_source_across_resume_until_own_turn() {
    use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};

    let root = tempfile::tempdir().unwrap();
    let sessions_dir = root.path().join("sessions");
    std::fs::create_dir_all(&sessions_dir).unwrap();
    let source_rows = [
        json!({"kind":"session_meta","schemaVersion":3,"meta":{"id":"src-parent","createdAt":1700000000000_i64,"cwd":root.path(),"model":"fake","role":"main"}}),
        json!({"kind":"turn_context","ctx":{"cwd":root.path(),"model":"fake","permissionMode":"default","turnId":"parent-turn-1","startedAtMs":1700000000100_i64}}),
        json!({"kind":"message","message":{"type":"user","uuid":"pu1","content":[{"type":"text","text":"parent task"}]}}),
        json!({"kind":"message","message":{"type":"assistant","uuid":"pa1","content":[{"type":"text","text":"parent answer"}],"model":"fake"}}),
        json!({"kind":"event","event":"turn_lifecycle","turnId":"parent-turn-1","payload":{"status":"completed","finalMessageId":"pa1","startedAtMs":1700000000100_i64,"finishedAtMs":1700000000200_i64,"durationMs":100}}),
        json!({"kind":"event","event":"prediction_source","turnId":"parent-turn-1","payload":{"generation":"1"}}),
    ];
    let payload = source_rows
        .iter()
        .map(Value::to_string)
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    std::fs::write(sessions_dir.join("1700000000000-src-parent.jsonl"), payload).unwrap();

    let store: Arc<dyn klaus_agent_core::store::session_store::SessionStore> =
        Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(sessions_dir.to_string_lossy().into_owned()),
            archive_dir: None,
        }));
    let agent = make_agent_with_call_model_and_config(
        scripted_call_model(vec![
            text_turn("side answer"),
            text_turn(r#"{"suggestion":"Side follow-up"}"#),
        ]),
        Some(json!({"agentMemory":false,"autoMemory":false,"autoDream":false})),
    );
    let create =
        crate::lifecycle::engine_sessions(agent.clone(), Some(store.clone())).create_session;
    let lifecycle = crate::lifecycle::protocol_lifecycle(agent, Some(store));
    let mut client = TestClient::start_with(create, Some(lifecycle));
    client.initialize().await;

    client
        .send(json!({
            "id": 10,
            "method": "session/startSideTask",
            "params": {
                "parentThreadId": "src-parent",
                "init": { "threadId": "side-empty", "cwd": root.path() }
            }
        }))
        .await;
    let (started, _) = client.response_for(10).await;
    let side_session_id = started["result"]["sessionId"].as_str().unwrap().to_owned();

    client
        .send(json!({"id": 11, "method": "session/predictionContext", "params": {"sessionId": side_session_id}}))
        .await;
    let (ctx_before, _) = client.response_for(11).await;
    assert!(ctx_before["result"]["turnId"].is_null(), "{ctx_before}");

    // Close and resume the empty side task: still no prediction source.
    client
        .send(
            json!({"id": 12, "method": "session/close", "params": {"sessionId": side_session_id}}),
        )
        .await;
    let _ = client.response_for(12).await;

    client
        .send(json!({"id": 13, "method": "session/start", "params": {"init": {"cwd": root.path()}, "resume": "side-empty"}}))
        .await;
    let (resumed, _) = client.response_for(13).await;
    let resumed_id = resumed["result"]["sessionId"].as_str().unwrap().to_owned();

    client
        .send(json!({"id": 14, "method": "session/predictionContext", "params": {"sessionId": resumed_id}}))
        .await;
    let (ctx_resumed, _) = client.response_for(14).await;
    assert!(ctx_resumed["result"]["turnId"].is_null(), "{ctx_resumed}");

    // Complete a real human turn inside the side task: now prediction works off that turn.
    client
        .send(json!({"id": 15, "method": "turn/start", "params": {"sessionId": resumed_id, "submissionId": "side-turn-1", "input": "side question"}}))
        .await;
    let (_, seen) = client.response_for(15).await;
    if !seen.iter().any(|m| m["method"] == "turn/completed") {
        loop {
            if client.next_message().await["method"] == "turn/completed" {
                break;
            }
        }
    }

    client
        .send(json!({"id": 16, "method": "session/predictionContext", "params": {"sessionId": resumed_id}}))
        .await;
    let (ctx_after, _) = client.response_for(16).await;
    let side_turn_id = ctx_after["result"]["turnId"]
        .as_str()
        .expect("own side turn becomes prediction source")
        .to_owned();
    assert_ne!(side_turn_id, "parent-turn-1");

    client
        .send(json!({"id": 17, "method": "session/predictMessage", "params": {"sessionId": resumed_id, "requestId": "side-pred-1", "sourceTurnId": side_turn_id, "prompt": "predict next"}}))
        .await;
    let (predicted, _) = client.response_for(17).await;
    assert_eq!(predicted["result"], json!({"suggestion": "Side follow-up"}));
    client.close().await;
}
