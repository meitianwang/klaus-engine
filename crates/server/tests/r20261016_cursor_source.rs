//! Cursor source facade contract: credentials stay private, model aliases resolve
//! to concrete account variants and source ownership survives tier resolution.
use super::*;
use crate::app_server::wire;

#[tokio::test]
async fn cursor_named_facade_dynamic_image_result_replays_visible_text_vision_and_original_signed_call(
) {
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
    client.send(json!({"id":402,"method":"session/start","params":{"init":{"cwd":temp.path(),"modelSelection":"cursor:auto","permissionMode":"bypassPermissions","config":{"maxRetries":0,"maxOutputTokens":256},"dynamicTools":[{"type":"function","name":"generate_image","description":"Generate an image","inputSchema":{"type":"object","properties":{"prompt":{"type":"string"}},"required":["prompt"],"additionalProperties":false},"deferLoading":false,"readOnly":false}]}}})).await;
    let start = client.response_for(402).await.0;
    assert!(start.get("error").is_none(), "{start}");
    let session = start["result"]["sessionId"].as_str().unwrap().to_string();
    client.send(json!({"id":403,"method":"turn/start","params":{"sessionId":session,"input":"Generate an image and describe it."}})).await;
    let (started, early) = client.response_for(403).await;
    assert!(started.get("error").is_none(), "{started}");
    let encoded = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGNgYPj/HwADAgH/5ncLrgAAAABJRU5ErkJggg==";
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
            assert!(!responded);
            assert_eq!(call["tool"], "generate_image");
            responded = true;
            client.send(json!({"id":404,"method":"session/resolveDynamicToolCall","params":{"sessionId":session,"callId":call["callId"],"response":{"success":true,"contentItems":[{"type":"inputImage","imageUrl":format!("data:image/png;base64,{encoded}")},{"type":"inputText","text":"Saved: /owned/generated.png"}]}}})).await;
        }
        events.push(event);
    }
    assert!(responded);
    assert!(serde_json::to_string(&events)
        .unwrap()
        .contains("FACADE_TOOL_OK"));
    let requests = captured.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    let roots = requests[1]["messages"].as_array().unwrap();
    assert!(roots.iter().any(|message| message["role"] == "assistant"
        && message["content"][0]["signature"] == "signed-facade"
        && message["content"][1]["toolName"] == "CallDynamicTool"));
    assert!(roots.iter().any(|message| message["role"] == "tool"
        && message["content"][0]["toolName"] == "CallDynamicTool"
        && message["content"][0]["result"]
            .as_str()
            .is_some_and(|text| text.contains("Saved: /owned/generated.png"))));
    assert!(roots.iter().any(|message| message["role"] == "user"
        && message["content"]
            .as_array()
            .is_some_and(|parts| parts.iter().any(|part| part["type"] == "image"
                && part["mimeType"] == "image/png"
                && part["image"] == encoded))));
    client.writer.shutdown().await.unwrap();
    client.close().await;
    api_task.abort();
    inference_task.abort();
}

#[tokio::test]
async fn cursor_native_capability_facade_uses_only_declared_pending_jobs_after_direct_admission_gate(
) {
    let _guard = super::super::cursor::test_lock().lock().await;
    let temp = tempfile::tempdir().unwrap();
    let config = temp.path().join("config.toml");
    std::fs::write(&config, "").unwrap();
    let unary = super::super::cursor::native_fixture().await;
    *unary.reply.lock().unwrap() = super::super::cursor::NativeReply::Status(
        403,
        json!({"code":"permission_denied","message":"Developer, Sand, training, or local agent loop access required"}),
    );
    let mut client = TestClient::start_full(
        make_create_session(&[]),
        None,
        Some(temp.path().join("rules.json")),
        Some(config),
    );
    client.initialize().await;
    for (index, image) in [false, true].into_iter().enumerate() {
        let native = super::super::cursor::native_step_fixture(false, image).await;
        let base = 310 + index as u64 * 3;
        client.send(json!({"id":base,"method":"cursor/adoptCredential","params":{"userInfo":{"accessToken":"fixture-access-token","clientIdentity":"fixture-device","backendUrl":unary.origin,"inferenceUrl":native.origin}}})).await;
        assert_eq!(client.response_for(base).await.0["result"]["ok"], true);
        let reference = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            b"\x89PNG\r\n\x1a\ncaller-reference",
        );
        let (method, params) = if image {
            (
                "cursor/generateImage",
                json!({"prompt":"Square","size":"1:1","referenceImages":[{"dataBase64":reference,"mimeType":"image/png"}],"operationId":"pending-image"}),
            )
        } else {
            (
                "cursor/search",
                json!({"query":"exact query","operationId":"pending-search"}),
            )
        };
        client
            .send(json!({"id":base+1,"method":method,"params":params}))
            .await;
        let result = client.response_for(base + 1).await.0;
        if image {
            assert_eq!(result["result"]["mimeType"], "image/png");
        } else {
            assert_eq!(
                result["result"]["results"][0]["url"],
                "https://example.com/page"
            );
        }
        tokio::time::timeout(Duration::from_secs(2), native.closed.notified())
            .await
            .unwrap();
        client
            .send(json!({"id":base+2,"method":"cursor/status"}))
            .await;
        assert_eq!(
            client.response_for(base + 2).await.0["result"]["loggedIn"],
            true
        );
    }
    client.writer.shutdown().await.unwrap();
    client.close().await;
}

#[tokio::test]
async fn cursor_native_capability_output_queue_fences_cancel_and_credential_rotation() {
    let _guard = super::super::cursor::test_lock().lock().await;
    let temp = tempfile::tempdir().expect("root");
    super::super::cursor::set_config_root(temp.path());
    assert_eq!(
        super::super::cursor::adopt_credential(
            json!({"accessToken":"old-token","clientIdentity":"fixture-device"}),
            None
        )["ok"],
        true
    );
    let old = super::super::cursor::status()["credentialId"].clone();
    let operations = Arc::new(super::super::cursor::Operations::default());
    let mut queued = operations
        .register(Some("queued-old-account"))
        .expect("slot");
    // A completed result remains owned by its original credential through queue delay.
    let generation = super::super::cursor::read_operation_generation_for_test();
    queued.bind_generation(generation);
    let cancelled = operations.register(Some("queued-cancelled")).expect("slot");
    operations
        .cancel("queued-cancelled")
        .expect("cancel queued");
    let (tx, rx) = tokio::sync::mpsc::channel(128);
    assert!(tx
        .send(super::super::service::Outgoing {
            message: wire::response(&json!(301), json!({"audioBase64":"old-private-audio"})).into(),
            cursor_operation: Some(queued),
            workbuddy_operation: None,
            provider_operation: None,
        })
        .await
        .is_ok());
    assert!(tx
        .send(super::super::service::Outgoing {
            message: wire::response(&json!(302), json!({"imageBase64":"cancelled-image"})).into(),
            cursor_operation: Some(cancelled),
            workbuddy_operation: None,
            provider_operation: None,
        })
        .await
        .is_ok());
    assert!(operations.register(Some("queued-old-account")).is_err());
    let newest = super::super::cursor::adopt_credential(
        json!({"accessToken":"new-token","clientIdentity":"fixture-device"}),
        None,
    );
    assert_ne!(newest["credentialId"], old);
    let (client, output) = tokio::io::duplex(1024);
    let writer = tokio::spawn(super::super::service::write_app_output(output, rx));
    drop(tx);
    let mut lines = BufReader::new(client).lines();
    let first: Value =
        serde_json::from_str(&lines.next_line().await.expect("read").expect("response"))
            .expect("JSON");
    let second: Value =
        serde_json::from_str(&lines.next_line().await.expect("read").expect("response"))
            .expect("JSON");
    assert_eq!(first["error"]["message"], "cursor_credential_changed");
    assert_eq!(second["error"]["message"], "cursor_operation_cancelled");
    assert!(!first.to_string().contains("old-private-audio"));
    assert!(!second.to_string().contains("cancelled-image"));
    writer.await.expect("writer");
    assert!(operations.register(Some("queued-old-account")).is_ok());
}

#[tokio::test]
async fn cursor_native_capability_facade_owns_shapes_cancellation_and_access_errors() {
    let _guard = super::super::cursor::test_lock().lock().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let config = temp.path().join("config.toml");
    std::fs::write(&config, "").expect("config");
    let fixture = super::super::cursor::native_fixture().await;
    let mut client = TestClient::start_full(
        make_create_session(&[]),
        None,
        Some(temp.path().join("rules.json")),
        Some(config),
    );
    client.initialize().await;
    client.send(json!({"id":201,"method":"cursor/adoptCredential","params":{"userInfo":{"accessToken":"fixture-access-token","clientIdentity":"fixture-device","backendUrl":fixture.origin,"inferenceUrl":fixture.origin}}})).await;
    let (adopted, _) = client.response_for(201).await;
    assert_eq!(adopted["result"]["ok"], true);
    for (id, method, params, field, mime) in [
        (
            202,
            "cursor/generateImage",
            json!({"prompt":"Square","size":"1024x1024","operationId":"image-one"}),
            "imageBase64",
            "image/png",
        ),
        (
            203,
            "cursor/generateAudio",
            json!({"text":"Hello","voice":"ara","speed":1.25,"operationId":"audio-one"}),
            "audioBase64",
            "audio/mpeg",
        ),
        (
            204,
            "cursor/generateVideo",
            json!({"prompt":"Moving square","seconds":4,"size":"1280x720","operationId":"video-one"}),
            "videoBase64",
            "video/mp4",
        ),
    ] {
        client
            .send(json!({"id":id,"method":method,"params":params}))
            .await;
        let (response, _) = client.response_for(id).await;
        assert!(
            response["result"][field]
                .as_str()
                .is_some_and(|data| !data.is_empty()),
            "{response}"
        );
        assert_eq!(response["result"]["mimeType"], mime);
        assert!(!response.to_string().contains("fixture-access-token"));
        if method == "cursor/generateVideo" {
            assert_eq!(response["result"]["seconds"], 4);
        }
    }
    client.send(json!({"id":205,"method":"cursor/search","params":{"query":"public docs","operationId":"search-one"}})).await;
    let (search, _) = client.response_for(205).await;
    assert_eq!(
        search["result"]["results"][0]["url"],
        "https://example.com/page"
    );
    client.send(json!({"id":206,"method":"cursor/generateAudio","params":{"text":"Hello","emotion":"happy"}})).await;
    let (unsupported, _) = client.response_for(206).await;
    assert!(unsupported.get("error").is_some());
    *fixture.reply.lock().unwrap() = super::super::cursor::NativeReply::Hang;
    client.send(json!({"id":207,"method":"cursor/generateAudio","params":{"text":"Wait","operationId":"cancel-now"}})).await;
    client.send(json!({"id":208,"method":"cursor/cancelOperation","params":{"operationId":"cancel-now"}})).await;
    let (cancelled, early) = client.response_for(208).await;
    assert_eq!(cancelled["result"], json!({"ok":true,"cancelled":true}));
    let failure = match early.into_iter().find(|message| message["id"] == 207) {
        Some(message) => message,
        None => client.response_for(207).await.0,
    };
    assert_eq!(failure["error"]["message"], "cursor_operation_cancelled");
    *fixture.reply.lock().unwrap() = super::super::cursor::NativeReply::Status(
        403,
        json!({"code":"permission_denied","message":"Developer, Sand, training, or local agent loop access required"}),
    );
    client
        .send(json!({"id":209,"method":"cursor/generateAudio","params":{"text":"Square"}}))
        .await;
    let (denied, _) = client.response_for(209).await;
    assert_eq!(
        denied["error"]["message"],
        "cursor_native_capability_not_enabled"
    );
    client
        .send(json!({"id":210,"method":"cursor/status"}))
        .await;
    assert_eq!(client.response_for(210).await.0["result"]["loggedIn"], true);
    client.writer.shutdown().await.expect("input EOF");
    client.close().await;
}

#[tokio::test]
async fn cursor_native_capability_connection_close_drops_pending_request() {
    let _guard = super::super::cursor::test_lock().lock().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let config = temp.path().join("config.toml");
    std::fs::write(&config, "").expect("config");
    let fixture = super::super::cursor::native_fixture().await;
    *fixture.reply.lock().unwrap() = super::super::cursor::NativeReply::Hang;
    let mut client = TestClient::start_full(
        make_create_session(&[]),
        None,
        Some(temp.path().join("rules.json")),
        Some(config),
    );
    client.initialize().await;
    client.send(json!({"id":201,"method":"cursor/adoptCredential","params":{"userInfo":{"accessToken":"fixture-access-token","clientIdentity":"fixture-device","backendUrl":fixture.origin}}})).await;
    assert_eq!(client.response_for(201).await.0["result"]["ok"], true);
    client.send(json!({"id":202,"method":"cursor/generateAudio","params":{"text":"Wait","operationId":"connection-owned"}})).await;
    tokio::time::timeout(Duration::from_secs(1), fixture.started.notified())
        .await
        .expect("native request started");
    client.writer.shutdown().await.expect("input EOF");
    tokio::time::timeout(Duration::from_secs(1), client.close())
        .await
        .expect("connection closes pending operation");
}

#[tokio::test]
async fn cursor_named_facade_lifecycle_catalog_quota_and_selection_are_consistent() {
    let _guard = super::super::cursor::test_lock().lock().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let config = temp.path().join("config.toml");
    std::fs::write(&config, "").expect("config");
    let (api, api_task) = super::super::cursor::tests::unary_fixture().await;
    let mut client = TestClient::start_full(
        make_create_session(&[]),
        None,
        Some(temp.path().join("rules.json")),
        Some(config),
    );
    client.initialize().await;
    client
        .send(json!({"id":100,"method":"cursor/status"}))
        .await;
    let (initial, _) = client.response_for(100).await;
    assert_eq!(initial["result"], json!({"loggedIn":false}));
    assert!(initial.get("error").is_none());
    client
        .send(json!({"id":21,"method":"cursor/loginStart","params":{}}))
        .await;
    let (started, _) = client.response_for(21).await;
    let flow_id = started["result"]["flowId"].as_str().expect("flow handle");
    assert_eq!(started["result"]["ok"], true);
    assert!(started["result"].get("verifier").is_none());
    client
        .send(json!({"id":22,"method":"cursor/loginCancel","params":{"flow":{"flowId":flow_id}}}))
        .await;
    let (cancelled, _) = client.response_for(22).await;
    assert_eq!(cancelled["result"]["ok"], true);
    client.send(json!({"id":2,"method":"cursor/adoptCredential","params":{"userInfo":{"accessToken":"private-fixture-token","account":"Fixture","backendUrl":api}}})).await;
    let (adoption, _) = client.response_for(2).await;
    assert_eq!(adoption["result"]["ok"], true, "{adoption}");
    let credential_id = adoption["result"]["credentialId"].clone();
    client.send(json!({"id":3,"method":"cursor/status"})).await;
    let (status, _) = client.response_for(3).await;
    assert_eq!(status["result"]["loggedIn"], true);
    assert!(!status.to_string().contains("private-fixture-token"));
    client
        .send(json!({"id":4,"method":"cursor/models","params":{"read":"warm","forceRefresh":true}}))
        .await;
    let (catalog, _) = client.response_for(4).await;
    let models = catalog["result"]["models"].as_array().expect("models");
    assert_eq!(models.len(), 1);
    let key = models[0]["key"].as_str().expect("key");
    assert_eq!(models[0]["display_name"], "Fixture High Fast");
    for (id, selection) in [(5, "auto"), (6, "high")] {
        client.send(json!({"id":id,"method":"model/selection/resolve","params":{"source":"cursor","selection":selection,"modelTierSelections":{"high":key}}})).await;
        let (resolution, _) = client.response_for(id).await;
        assert_eq!(
            resolution["result"]["selection"],
            format!("cursor:{key}"),
            "{resolution}"
        );
    }
    client.send(json!({"id":7,"method":"cursor/usage"})).await;
    let (usage, _) = client.response_for(7).await;
    assert_eq!(usage["result"]["planUsage"]["totalPercentUsed"], 42.5);
    let store = temp.path().join("cursor-subscription-oauth.json");
    let mut stored: Value = serde_json::from_slice(&std::fs::read(&store).expect("read store"))
        .expect("stored credential");
    stored["userInfo"]["expiresAt"] = json!(1);
    std::fs::write(
        &store,
        serde_json::to_vec(&stored).expect("serialize store"),
    )
    .expect("expire store");
    client
        .send(json!({"id":101,"method":"cursor/status"}))
        .await;
    let (expired, _) = client.response_for(101).await;
    assert_eq!(expired["result"]["loggedIn"], false);
    assert_eq!(expired["result"]["needsReimport"], true);
    assert_eq!(expired["result"]["credentialId"], credential_id);
    assert_eq!(
        expired["result"]["reason"],
        "cursor_adopted_credential_needs_reimport"
    );
    assert!(expired["result"].get("error").is_none());
    assert!(expired.get("error").is_none());
    client.send(json!({"id":8,"method":"cursor/logout","params":{"expectedCredentialId":"another-client"}})).await;
    let (cleanup, _) = client.response_for(8).await;
    assert_eq!(cleanup["result"]["removed"], false);
    client.send(json!({"id":9,"method":"cursor/logout","params":{"expectedCredentialId":credential_id}})).await;
    let (logout, _) = client.response_for(9).await;
    assert_eq!(logout["result"]["ok"], true);
    client
        .send(json!({"id":102,"method":"cursor/status"}))
        .await;
    let (signed_out, _) = client.response_for(102).await;
    assert_eq!(signed_out["result"], json!({"loggedIn":false}));
    assert!(signed_out.get("error").is_none());
    client
        .send(json!({"id":10,"method":"cursor/models","params":{"read":"cached"}}))
        .await;
    let (catalog, _) = client.response_for(10).await;
    assert_eq!(catalog["error"]["message"], "cursor_not_signed_in");
    client.close().await;
    api_task.abort();
}

#[tokio::test]
async fn cursor_named_facade_default_inventory_executes_search_and_read_through_the_provider() {
    run_default_inventory(false, false).await;
}

#[tokio::test]
async fn cursor_named_facade_permission_denial_prevents_write_and_replays_the_local_result() {
    run_default_inventory(true, false).await;
}

pub(super) async fn run_default_inventory(deny_write: bool, metadata: bool) {
    let _guard = super::super::cursor::test_lock().lock().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let config = temp.path().join("config.toml");
    std::fs::write(&config, "").expect("config");
    let file = temp.path().join("fixture.txt");
    std::fs::write(&file, "named facade file contents").expect("fixture file");
    let (api, api_task) = super::super::cursor::tests::unary_fixture().await;
    let denied = temp.path().join("denied.txt");
    let (inference, captured, inference_task) = if deny_write {
        super::super::cursor::tests::permission_inventory_fixture(
            denied.to_string_lossy().into_owned(),
        )
        .await
    } else if metadata {
        super::super::cursor::tests::metadata_inventory_fixture(file.to_string_lossy().into_owned())
            .await
    } else {
        super::super::cursor::tests::default_inventory_fixture(file.to_string_lossy().into_owned())
            .await
    };
    let agent = make_agent_with_provider_and_config(Arc::new(fake_provider()), None, None);
    let factory = crate::lifecycle::engine_sessions(agent, None).create_session;
    let create: CreateSession = Arc::new(move |ctx| {
        let factory = factory.clone();
        Box::pin(async move {
            let session = factory(ctx).await?;
            if deny_write {
                let policy = session.runtime_policy_state();
                assert_eq!(
                    policy.approval_policy,
                    Some(klaus_agent_core::types::permission::ApprovalPolicy::OnRequest),
                    "the fixture requires ordinary approval prompts"
                );
                assert_eq!(
                    policy.sandbox_mode,
                    Some(klaus_agent_core::types::permission::SandboxMode::ReadOnly),
                    "the fixture must not inherit unrestricted filesystem access"
                );
                assert_eq!(
                    policy.approvals_reviewer,
                    Some(klaus_agent_core::types::permission::ApprovalsReviewer::User)
                );
            }
            Ok(session)
        })
    });
    let mut client = TestClient::start_full(
        create,
        None,
        Some(temp.path().join("rules.json")),
        Some(config),
    );
    client.initialize().await;
    client.send(json!({"id":2,"method":"cursor/adoptCredential","params":{"userInfo":{"accessToken":"fixture-token","clientIdentity":"fixture-machine","backendUrl":api,"inferenceUrl":inference}}})).await;
    let (adoption, _) = client.response_for(2).await;
    assert_eq!(adoption["result"]["ok"], true, "{adoption}");
    let mut engine_config = json!({"maxRetries":0,"maxOutputTokens":256});
    if deny_write {
        engine_config["approval_policy"] = json!("on-request");
        engine_config["sandbox_mode"] = json!("read-only");
        engine_config["approvals_reviewer"] = json!("user");
    }
    client.send(json!({"id":3,"method":"session/start","params":{"init":{"cwd":temp.path(),"modelSelection":"cursor:auto","permissionMode":if deny_write {"default"}else{"bypassPermissions"},"config":engine_config,"baseInstructions":"Read the requested file."}}})).await;
    let (start, _) = client.response_for(3).await;
    assert!(start.get("error").is_none(), "{start}");
    let session = start["result"]["sessionId"]
        .as_str()
        .expect("session")
        .to_string();
    client.send(json!({"id":4,"method":"turn/start","params":{"sessionId":session,"input":"Find the read tool and read fixture.txt."}})).await;
    let (started, early) = client.response_for(4).await;
    assert!(started.get("error").is_none(), "{started}");
    let mut approvals = 0;
    let mut notifications: Vec<Value> = Vec::new();
    let mut pending: std::collections::VecDeque<_> = early.into();
    while !notifications
        .iter()
        .any(|message| message["method"] == "turn/completed")
    {
        assert!(
            notifications.len() < 512,
            "bounded facade notification fixture"
        );
        let notification = match pending.pop_front() {
            Some(event) => event,
            None => client.next_message().await,
        };
        if notification["method"] == "approval/request" {
            approvals += 1;
            assert!(deny_write, "unexpected approval");
            client.send(json!({"id":notification["id"],"result":{"response":{"decision":"deny","message":"Denied by the local fixture"}}})).await;
        }
        notifications.push(notification);
    }
    let events = serde_json::to_string(&notifications).expect("events");
    assert!(events.contains("FACADE_TOOL_OK"), "{events}");
    assert!(!events.contains("cursor_tool_type_unsupported"), "{events}");
    assert!(
        notifications
            .iter()
            .all(|message| message["method"] != "turn/failed"),
        "{events}"
    );
    let requests = captured.lock().expect("captured").clone();
    assert_eq!(requests.len(), if deny_write { 2 } else { 3 });
    assert!(
        !denied.exists(),
        "provider intent cannot bypass local permissions"
    );
    if deny_write {
        assert_eq!(approvals, 1);
        assert!(requests[1]["messages"]
            .to_string()
            .contains("Denied by the local fixture"));
    }
    let system = requests[0]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|message| message["role"] == "system")
        .unwrap()["content"]
        .as_str()
        .unwrap();
    assert!(system.contains("Tool execution binding"));
    assert!(system.contains("klaus_tools"));
    assert!(system.contains("\"callerTool\":\"read\""));
    assert!(requests[0]["run"]["systemPromptSpec"]["replace"]
        .as_str()
        .unwrap()
        .contains("Tool execution binding"));
    let tools = requests[0]["tools"].as_array().expect("tools");
    for name in ["apply_patch", "tool_search", "read"] {
        let tool = tools
            .iter()
            .find(|tool| tool["toolName"] == name)
            .unwrap_or_else(|| panic!("default inventory lacks {name}"));
        assert_eq!(tool["inputSchema"]["type"], "object");
    }
    assert!(tools
        .iter()
        .find(|tool| tool["toolName"] == "apply_patch")
        .unwrap()["inputSchema"]["properties"]
        .get("patch")
        .is_some());
    assert!(requests[1]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .any(|message| message["role"] == "tool"
            && message["content"][0]["toolName"] == "CallDynamicTool"));
    assert!(requests[1]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .any(|message| message["content"][0]["signature"] == "signed-facade"));
    if metadata {
        for (index, expected) in [(1, 1), (2, 2)] {
            assert_eq!(
                requests[index]["messages"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|message| message["role"] == "tool"
                        && message["content"][0]["toolName"] == "GetDynamicTools")
                    .count(),
                expected
            );
        }
    }
    if !deny_write {
        let read_result = requests[2]["messages"]
            .as_array()
            .unwrap()
            .iter()
            .rfind(|message| message["role"] == "tool")
            .expect("real read result");
        assert!(read_result
            .to_string()
            .contains("named facade file contents"));
        assert_eq!(
            read_result["content"][0]["toolName"],
            if metadata { "CallDynamicTool" } else { "Read" },
            "local result replays under the original native intent name"
        );
        assert!(requests[2]["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|message| message["content"]
                .as_array()
                .is_some_and(|parts| parts.iter().any(|part| part["type"] == "tool-call"
                    && part["toolName"] == if metadata { "CallDynamicTool" } else { "Read" }))));
    }
    client
        .send(json!({"id":5,"method":"session/close","params":{"sessionId":session}}))
        .await;
    let (closed, _) = client.response_for(5).await;
    assert!(closed.get("error").is_none(), "{closed}");
    client
        .send(json!({"id":6,"method":"cursor/logout","params":{}}))
        .await;
    let (logout, _) = client.response_for(6).await;
    assert_eq!(logout["result"]["ok"], true, "{logout}");
    client.close().await;
    api_task.abort();
    inference_task.abort();
}
