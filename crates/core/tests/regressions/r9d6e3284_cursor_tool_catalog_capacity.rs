//! Large caller tool catalogs failed before reaching the provider because their
//! size reused the per-step call-count guard. Fix: 9d6e3284.
//! Run through the server's public facade, where the subscription adapter lives.
use super::*;

#[tokio::test]
async fn cursor_large_catalog_preserves_all_declared_tools_and_replays_the_last_dynamic_tool() {
    let _guard = super::super::cursor::test_lock().lock().await;
    let temp = tempfile::tempdir().unwrap();
    let config = temp.path().join("config.toml");
    std::fs::write(&config, "").unwrap();
    let (api, api_task) = super::super::cursor::tests::unary_fixture().await;
    let (inference, captured, inference_task) =
        super::super::cursor::tests::image_inventory_fixture().await;
    let agent = make_agent_with_provider_and_config(Arc::new(fake_provider()), None, None);
    let mut client = TestClient::start_full(
        crate::lifecycle::engine_sessions(agent, None).create_session,
        None,
        Some(temp.path().join("rules.json")),
        Some(config),
    );
    client.initialize().await;
    client.send(json!({"id":401,"method":"cursor/adoptCredential","params":{"userInfo":{"accessToken":"fixture-token","clientIdentity":"fixture-machine","backendUrl":api,"inferenceUrl":inference}}})).await;
    assert_eq!(client.response_for(401).await.0["result"]["ok"], true);

    let schema = json!({"type":"object","properties":{"prompt":{"type":"string"}},"required":["prompt"],"additionalProperties":false});
    let mut dynamic_tools: Vec<Value> = (0..345)
        .map(|index| json!({"type":"function","name":format!("catalog_tool_{index:03}"),"description":"Caller-owned catalog entry","inputSchema":schema,"deferLoading":false,"readOnly":false}))
        .collect();
    dynamic_tools.push(json!({"type":"function","name":"generate_image","description":"Generate an image","inputSchema":schema,"deferLoading":false,"readOnly":false}));
    client.send(json!({"id":402,"method":"session/start","params":{"init":{"cwd":temp.path(),"modelSelection":"cursor:auto","permissionMode":"bypassPermissions","config":{"maxRetries":0,"maxOutputTokens":256},"dynamicTools":dynamic_tools}}})).await;
    let start = client.response_for(402).await.0;
    assert!(start.get("error").is_none(), "{start}");
    let session = start["result"]["sessionId"].as_str().unwrap().to_string();
    client.send(json!({"id":403,"method":"turn/start","params":{"sessionId":session,"input":"Call the final declared dynamic tool to generate a blue square."}})).await;
    let (started, early) = client.response_for(403).await;
    assert!(started.get("error").is_none(), "{started}");
    let mut pending: std::collections::VecDeque<_> = early.into();
    let mut events = Vec::new();
    let mut responded = false;
    while !events
        .iter()
        .any(|event: &Value| event["method"] == "turn/completed")
    {
        assert!(events.len() < 512, "bounded notifications");
        let event = match pending.pop_front() {
            Some(value) => value,
            None => client.next_message().await,
        };
        assert_ne!(event["method"], "turn/failed", "{event}");
        if let Some(call) = event["params"]["event"].get("dynamicToolCall") {
            assert!(!responded, "one local execution");
            assert_eq!(call["tool"], "generate_image");
            responded = true;
            client.send(json!({"id":404,"method":"session/resolveDynamicToolCall","params":{"sessionId":session,"callId":call["callId"],"response":{"success":true,"contentItems":[{"type":"inputText","text":"LARGE_CATALOG_RECEIPT"}]}}})).await;
        }
        events.push(event);
    }
    assert!(responded);
    assert!(serde_json::to_string(&events)
        .unwrap()
        .contains("FACADE_TOOL_OK"));
    let requests = captured.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0]["tools"], requests[1]["tools"]);
    for request in &requests {
        let tools = request["tools"].as_array().unwrap();
        assert!(tools.len() >= 346);
        let context = &request["run"]["action"]["resumeAction"]["requestContext"];
        assert_eq!(context["tools"], request["tools"]);
        let descriptors = context["mcpMetaToolOptions"]["mcpDescriptors"][0]["tools"]
            .as_array()
            .unwrap();
        for declaration in &dynamic_tools {
            let tool = tools
                .iter()
                .find(|tool| tool["toolName"] == declaration["name"])
                .expect("every declared tool reaches the provider");
            assert_eq!(tool["inputSchema"], declaration["inputSchema"]);
            let descriptor = descriptors
                .iter()
                .find(|tool| tool["toolName"] == declaration["name"])
                .expect("every declared tool reaches metadata discovery");
            assert_eq!(descriptor["inputSchema"], declaration["inputSchema"]);
        }
    }
    let roots = requests[1]["messages"].as_array().unwrap();
    assert!(roots.iter().any(|message| message["role"] == "assistant"
        && message["content"][0]["signature"] == "signed-facade"
        && message["content"][1]["toolName"] == "CallDynamicTool"));
    assert!(roots.iter().any(|message| message["role"] == "tool"
        && message["content"][0]["toolName"] == "CallDynamicTool"
        && message["content"][0]["result"]
            .as_str()
            .is_some_and(|text| text.contains("LARGE_CATALOG_RECEIPT"))));
    client.writer.shutdown().await.unwrap();
    client.close().await;
    api_task.abort();
    inference_task.abort();
}
