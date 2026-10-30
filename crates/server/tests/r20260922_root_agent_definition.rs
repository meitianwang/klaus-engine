//! Defect: named agent registration did not select the root execution role.
//! Root cause: the selected role was not resolved before inference admission.
//! Work item: 2026-09-22 team collaboration specialist execution.
//! Exercise the public named facade and the production lifecycle adapter.

use super::*;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::effort::{EffortLevel, EffortValue};
use klaus_agent_core::types::provider::ModelRequest;
use std::sync::Mutex;

#[tokio::test]
async fn root_agent_definition_named_facade_runs_the_selected_role() {
    let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let captured = requests.clone();
    let agent = make_agent_with_call_model(Box::new(move |request, _signal| {
        captured.lock().unwrap().push(request);
        Box::pin(futures::stream::iter(text_turn("specialist completed")))
    }));
    let factory = crate::lifecycle::engine_sessions(agent, None).create_session;
    let mut client = TestClient::start(factory);
    client.initialize().await;
    client
        .send(json!({
            "id": 2,
            "method": "session/start",
            "params": {"init": {
                "threadId": "specialist-facade-thread",
                "baseInstructions": "retained host base",
                "developerInstructions": "retained host developer",
                "config": {"model": "parent-model", "autoMemory": false, "modelTiers": {"high": "role-model"}},
                "agents": [{
                    "name": "reviewer",
                    "description": "Review only",
                    "prompt": "selected reviewer instructions",
                    "systemPrompt": "alias must not win",
                    "tools": ["read", "write"],
                    "disallowedTools": ["write"],
                    "model": "high",
                    "effort": "high",
                    "permissionMode": "dontAsk",
                    "maxTurns": 2
                }],
                "agentDefinitionName": "reviewer"
            }}
        }))
        .await;
    let (response, _) = client.response_for(2).await;
    assert_eq!(
        response["result"]["sessionId"], "specialist-facade-thread",
        "{response}"
    );
    assert_eq!(
        response["result"]["initialized"]["agentDefinitionName"],
        "reviewer"
    );
    assert!(
        requests.lock().unwrap().is_empty(),
        "startup must not invoke a model"
    );
    client.send(json!({"id":9,"method":"session/prepare","params":{"sessionId":"specialist-facade-thread"}})).await;
    let (prepared, _) = client.response_for(9).await;
    assert_eq!(prepared["result"]["ready"], true, "{prepared}");
    client.send(json!({"id":3,"method":"session/modelRuntime","params":{"sessionId":"specialist-facade-thread"}})).await;
    let (runtime, _) = client.response_for(3).await;
    assert_eq!(runtime["result"]["model"], "role-model", "{runtime}");
    assert_eq!(runtime["result"]["effort"], "high", "{runtime}");
    client.send(json!({"id":4,"method":"turn/start","params":{"sessionId":"specialist-facade-thread","input":"review the change"}})).await;
    let (started, mut notifications) = client.response_for(4).await;
    assert!(started["result"]["turnId"].is_string(), "{started}");
    while !notifications
        .iter()
        .any(|message| message["method"] == "turn/completed")
    {
        notifications.push(client.next_message().await);
    }
    {
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let request = &requests[0];
        assert_eq!(request.model, "role-model");
        assert_eq!(request.effort, Some(EffortValue::Level(EffortLevel::High)));
        assert_eq!(
            request
                .tools
                .iter()
                .map(|tool| tool.name.clone())
                .collect::<Vec<_>>(),
            vec![RuntimeString::from("read")]
        );
        for text in [
            "retained host base",
            "retained host developer",
            "selected reviewer instructions",
        ] {
            assert!(
                request.system.iter().any(|part| part.text == text),
                "{text}"
            );
        }
        assert!(!request
            .system
            .iter()
            .any(|part| part.text == "alias must not win"));
    }
    client.close().await;
}

#[tokio::test]
async fn root_agent_definition_unknown_fails_preparation_and_close_allows_retry() {
    let factory =
        crate::lifecycle::engine_sessions(make_agent_for_tests(vec![]), None).create_session;
    let mut client = TestClient::start(factory);
    client.initialize().await;
    client
        .send(json!({"id":2,"method":"session/start","params":{"init":{
            "threadId":"unknown-root-role","agents":[],"agentDefinitionName":"missing-specialist"
        }}}))
        .await;
    let (started, _) = client.response_for(2).await;
    assert_eq!(
        started["result"]["sessionId"], "unknown-root-role",
        "{started}"
    );
    client
        .send(json!({"id":3,"method":"session/prepare","params":{"sessionId":"unknown-root-role"}}))
        .await;
    let (response, _) = client.response_for(3).await;
    assert!(
        response["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("unknown agent definition")),
        "{response}"
    );
    assert!(response.get("result").is_none(), "{response}");
    client
        .send(json!({"id":4,"method":"session/close","params":{"sessionId":"unknown-root-role"}}))
        .await;
    assert_eq!(client.response_for(4).await.0["result"], json!({}));
    client
        .send(json!({"id":5,"method":"session/start","params":{"init":{
            "threadId":"unknown-root-role","agentDefinitionName":"reviewer",
            "agents":[{"name":"reviewer","description":"role","tools":[]}]
        }}}))
        .await;
    let (retry, _) = client.response_for(5).await;
    assert_eq!(retry["result"]["sessionId"], "unknown-root-role", "{retry}");
    client
        .send(json!({"id":6,"method":"session/prepare","params":{"sessionId":"unknown-root-role"}}))
        .await;
    let (prepared, _) = client.response_for(6).await;
    assert_eq!(prepared["result"]["ready"], true, "{prepared}");
    client.close().await;
}

#[tokio::test]
async fn root_agent_definition_invalid_selector_and_spawn_only_definition_fail_closed() {
    let factory =
        crate::lifecycle::engine_sessions(make_agent_for_tests(vec![]), None).create_session;
    let mut client = TestClient::start(factory);
    client.initialize().await;
    for (index, selector) in [json!(7), json!(""), json!(" reviewer ")]
        .into_iter()
        .enumerate()
    {
        let id = index as u64 * 3 + 2;
        client
            .send(json!({"id":id,"method":"session/start","params":{"init":{
                "agentDefinitionName":selector,
                "agents":[{"name":"reviewer","description":"role","tools":[]}]
            }}}))
            .await;
        let (response, _) = client.response_for(id).await;
        if selector.is_number() {
            assert!(response.get("error").is_some(), "{response}");
            continue;
        }
        let session_id = response["result"]["sessionId"].clone();
        assert!(session_id.is_string(), "{response}");
        client
            .send(json!({"id":id+1,"method":"session/prepare","params":{"sessionId":session_id}}))
            .await;
        let (prepared, _) = client.response_for(id + 1).await;
        assert!(
            prepared["error"]["message"]
                .as_str()
                .is_some_and(|message| message.contains("unknown agent definition")),
            "{prepared}"
        );
        assert!(prepared.get("result").is_none(), "{prepared}");
        client
            .send(json!({"id":id+2,"method":"session/close","params":{"sessionId":session_id}}))
            .await;
        assert_eq!(client.response_for(id + 2).await.0["result"], json!({}));
    }
    client
        .send(json!({"id":8,"method":"session/start","params":{"init":{
            "agentDefinitionName":"reviewer",
            "agents":[{"name":"reviewer","description":"role","isolation":"worktree"}]
        }}}))
        .await;
    let (started, _) = client.response_for(8).await;
    let session_id = started["result"]["sessionId"].clone();
    assert!(session_id.is_string(), "{started}");
    client
        .send(json!({"id":9,"method":"session/prepare","params":{"sessionId":session_id}}))
        .await;
    let (response, _) = client.response_for(9).await;
    assert!(
        response["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("cannot run as root")),
        "{response}"
    );
    client
        .send(json!({"id":10,"method":"session/close","params":{"sessionId":session_id}}))
        .await;
    assert_eq!(client.response_for(10).await.0["result"], json!({}));
    client.close().await;
}
