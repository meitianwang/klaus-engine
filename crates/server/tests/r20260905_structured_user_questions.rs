//! Defect: structured question batches could not cross the public facade.
//! Root cause: elicitation carried only one prompt and unkeyed values.
//! Fixed by: 2026-09-05 structured user question alignment work item.

use super::*;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeString};

fn contains_serialized_record(value: &Value, expected: &Value) -> bool {
    match value {
        Value::String(text) => {
            serde_json::from_str::<Value>(text).is_ok_and(|decoded| &decoded == expected)
        }
        Value::Array(items) => items
            .iter()
            .any(|item| contains_serialized_record(item, expected)),
        Value::Object(fields) => fields
            .values()
            .any(|item| contains_serialized_record(item, expected)),
        _ => false,
    }
}

#[tokio::test]
async fn structured_user_questions_roundtrip_through_facade_even_with_full_access() {
    structured_question_roundtrip(false).await;
}

#[tokio::test]
async fn stable_question_ids_roundtrip_through_facade_without_changing_answer_values() {
    structured_question_roundtrip(true).await;
}

async fn structured_question_roundtrip(stable_ids: bool) {
    let mut input = json!({"questions": [
        {"question":"Which framework?", "header":"Framework", "options":[
            {"label":"React", "description":"Existing app", "preview":"<preview>"},
            {"label":"Vue", "description":"Alternative"}
        ], "multiSelect":false},
        {"question":"Which features?", "header":"Features", "options":[
            {"label":"A, B", "description":"Combined"},
            {"label":"C", "description":"Extra"}
        ], "multiSelect":true}
    ], "answers":{"Which framework?":"model guess"}, "metadata":{"source":"facade-test"}});
    if stable_ids {
        input["questions"][0]["id"] = json!("framework");
        input["questions"][1]["id"] = json!("features");
    }
    let exact = runtime_json::parse(&RuntimeString::from(input.to_string())).unwrap();
    let mut first_turn = vec![ev::message_start(None)];
    first_turn.extend(ev::tool_use("question-1", "ask_user", &exact));
    first_turn.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let agent = make_agent_for_tests(vec![first_turn, text_turn("answers received")]);
    // The shared helper deliberately starts the session with BypassPermissions.
    let mut client = TestClient::start(create_session_for_agent_tests(agent));
    client.initialize().await;
    client
        .send(json!({"id":2,"method":"session/start","params":{"init":{}}}))
        .await;
    let (started, _) = client.response_for(2).await;
    let session_id = started["result"]["sessionId"].clone();
    client
        .send(json!({"id":3,"method":"turn/start","params":{"sessionId":session_id,"input":"ask"}}))
        .await;
    let (_, mut pending) = client.response_for(3).await;
    let request = loop {
        let message = if pending.is_empty() {
            client.next_message().await
        } else {
            pending.remove(0)
        };
        assert_ne!(
            message["method"], "turn/completed",
            "questions must wait for actual answers"
        );
        if message["method"] == "elicitation/request" {
            break message;
        }
    };
    assert_eq!(
        request["params"]["elicitation"]["request"]["questions"],
        input["questions"]
    );
    assert_eq!(
        request["params"]["elicitation"]["request"]["metadata"],
        input["metadata"]
    );
    let answers = if stable_ids {
        json!({"framework":"  user custom text  ","features":"A, B, C"})
    } else {
        json!({"Which framework?":"  user custom text  ","Which features?":"A, B, C"})
    };
    client.send(json!({"jsonrpc":"2.0","id":request["id"],"result":{"response":{"kind":"answers","answers":answers}}})).await;
    loop {
        if client.next_message().await["method"] == "turn/completed" {
            break;
        }
    }
    // Session history is the public persisted transcript, independent of event timing.
    client
        .send(json!({"id":4,"method":"session/history","params":{"sessionId":session_id}}))
        .await;
    let (history, _) = client.response_for(4).await;
    let expected = json!({"questions":input["questions"],"answers":answers});
    assert!(
        contains_serialized_record(&history, &expected),
        "the exact structured result must survive transcript persistence: {history}"
    );
    client.close().await;
}
