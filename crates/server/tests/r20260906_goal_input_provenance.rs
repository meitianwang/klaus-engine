//! Goal restart instructions were recorded as user-authored turns.
//! Root cause: turn/start options discarded core's meta_input flag at the protocol boundary.
//! Regression: goal input provenance, 2026-09-06; exercise the named facade end to end.
//! This facade regression stays in server because importing the facade from core would invert dependencies.

use super::*;

fn is_retained_skill_catalog(message: &Value) -> bool {
    message["type"] == "user"
        && message["isMeta"] == true
        && message["content"].as_array().is_some_and(|content| {
            content.len() == 1
                && content[0]["type"] == "text"
                && content[0]["harnessEnvelope"] == "system-reminder"
                && content[0]["text"]
                    .as_str()
                    .is_some_and(|text| text.starts_with("<skill-catalog:"))
        })
}

#[tokio::test]
async fn turn_start_preserves_meta_input_and_first_goal_display_metadata() {
    let agent = make_agent_for_tests(
        ["first", "resume", "ordinary"]
            .into_iter()
            .map(text_turn)
            .collect(),
    );
    let create: CreateSession = Arc::new(move |ctx: SessionContext| {
        let agent = agent.clone();
        Box::pin(async move {
            Ok::<Arc<Session>, BoxError>(agent.start_session(StartSessionOptions {
                id: ctx.init.thread_id.clone(),
                config: Some(json!({ "autoMemory": false })),
                approval_handler: Some(ctx.approval_handler),
                elicit_handler: Some(ctx.elicit_handler),
                mcp_elicitation_presenter: Some(ctx.mcp_elicitation_presenter),
                permission_mode: Some(ExternalPermissionMode::BypassPermissions),
                ..Default::default()
            }))
        })
    });
    let mut client = TestClient::start(create);
    client.initialize().await;
    client
        .send(json!({ "id": 2, "method": "session/start", "params": { "init": {} } }))
        .await;
    let (response, _) = client.response_for(2).await;
    let session_id = response["result"]["sessionId"].as_str().unwrap().to_owned();
    for (index, input, options) in [
        (
            3,
            "Internal first goal instruction",
            json!({ "metaInput": false, "displayMetadata": { "origin": "goal", "displayText": "Build a CSV summary" } }),
        ),
        (
            4,
            "Internal resume instruction",
            json!({ "metaInput": true }),
        ),
        (5, "Actual user follow-up", json!({})),
    ] {
        client.send(json!({ "id": index, "method": "turn/start", "params": { "sessionId": session_id, "input": input, "options": options } })).await;
        let (_, seen) = client.response_for(index).await;
        if !seen
            .iter()
            .any(|message| message["method"] == "turn/completed")
        {
            loop {
                let message = client.next_message().await;
                if message["method"] == "turn/completed" {
                    break;
                }
            }
        }
    }
    client
        .send(
            json!({ "id": 6, "method": "session/history", "params": { "sessionId": session_id } }),
        )
        .await;
    let (response, _) = client.response_for(6).await;
    let messages = response["result"]["history"]
        .as_array()
        .expect("history messages");
    assert_eq!(
        messages
            .iter()
            .filter(|message| is_retained_skill_catalog(message))
            .count(),
        1,
        "the trusted skill catalog remains in raw history independently of goal input"
    );
    let users: Vec<&Value> = messages
        .iter()
        .filter(|message| message["type"] == "user" && !is_retained_skill_catalog(message))
        .collect();
    assert_eq!(users.len(), 3);
    assert_eq!(
        users
            .iter()
            .map(|message| message["content"][0]["text"].as_str().unwrap())
            .collect::<Vec<_>>(),
        [
            "Internal first goal instruction",
            "Internal resume instruction",
            "Actual user follow-up"
        ]
    );
    assert_ne!(users[0]["isMeta"], true);
    assert_eq!(
        users[1]["isMeta"], true,
        "an idle Goal restart is not a human turn"
    );
    assert_ne!(
        users[2]["isMeta"], true,
        "ordinary user input remains visible"
    );
    let display = messages
        .iter()
        .find(|message| message["subtype"] == "user_display")
        .expect("first goal display metadata");
    let envelope: Value = serde_json::from_str(display["text"].as_str().unwrap()).unwrap();
    assert_eq!(envelope["messageUuid"], users[0]["uuid"]);
    assert_eq!(
        envelope["localRender"]["displayText"],
        "Build a CSV summary"
    );
    assert_eq!(
        messages
            .iter()
            .filter(|message| message["subtype"] == "user_display")
            .count(),
        1
    );
    client.close().await;
}
