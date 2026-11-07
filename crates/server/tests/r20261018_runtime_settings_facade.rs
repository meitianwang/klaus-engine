//! Runtime settings apply before acknowledgement and retain their independent values.

use super::*;
use futures::StreamExt;
use klaus_agent_core::runtime_values::RuntimePath;
use klaus_agent_core::schemas::config::WebSearchAccess;
use klaus_agent_core::types::permission::{ApprovalPolicy, ApprovalsReviewer, SandboxMode};
use klaus_agent_core::types::provider::{ModelRequest, OutputVerbosity, ReasoningSummary};
use std::sync::Mutex;

async fn request(client: &mut TestClient, id: u64, method: &str, params: Value) -> Value {
    client
        .send(json!({"id":id,"method":method,"params":params}))
        .await;
    let (response, _) = client.response_for(id).await;
    assert!(response.get("error").is_none(), "{response}");
    response["result"].clone()
}

#[tokio::test]
async fn runtime_settings_config_read_has_truthful_defaults_and_persists_exact_fields() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("config.toml");
    let mut client =
        TestClient::start_full(make_create_session(&[]), None, None, Some(path.clone()));
    client.initialize().await;
    let empty = request(&mut client, 2, "config/read", json!({})).await;
    assert_eq!(empty["values"], json!({}));
    assert_eq!(empty["resolved"]["sandbox_mode"], json!("read-only"));
    assert_eq!(empty["sources"]["sandbox_mode"], json!("defaults"));
    assert!(empty["resolved"]["webSearch"].get("access").is_none());
    assert_eq!(empty["sources"]["webSearch"], json!("defaults"));
    let fields = json!({
        "approval_policy":"never", "sandbox_mode":"danger-full-access",
        "approvals_reviewer":"auto_review", "sandbox_workspace_write":{"writable_roots":[],"network_access":true,"exclude_tmpdir_env_var":false,"exclude_slash_tmp":false},
        "modelVerbosity":"high", "reasoningSummary":"detailed",
        "webSearch":{"mode":"client","source":"codex","access":"indexed"}
    });
    let edits: Vec<_> = fields
        .as_object()
        .unwrap()
        .iter()
        .map(|(key, value)| json!({"keyPath":key,"value":value}))
        .collect();
    let saved = request(
        &mut client,
        3,
        "config/batchWrite",
        json!({"edits":edits,"expectedVersion":empty["version"]}),
    )
    .await;
    assert_eq!(saved["values"], fields);
    for (key, value) in fields.as_object().unwrap() {
        assert_eq!(&saved["resolved"][key], value, "{key}");
        assert_eq!(saved["sources"][key], json!("user"), "{key}");
    }
    let reopened = request(&mut client, 4, "config/read", json!({})).await;
    assert_eq!(reopened, saved);
    let reset = request(
        &mut client,
        5,
        "config/value/write",
        json!({"keyPath":"modelVerbosity","value":null,"expectedVersion":saved["version"]}),
    )
    .await;
    assert!(reset["values"].get("modelVerbosity").is_none());
    assert!(reset["resolved"]["modelVerbosity"].is_null());
    assert_eq!(reset["sources"]["modelVerbosity"], json!("defaults"));
    assert!(std::fs::read_to_string(path)
        .unwrap()
        .contains("access = \"indexed\""));
    client.close().await;
}

#[tokio::test]
async fn runtime_settings_live_facade_ack_observes_applied_policy_and_inference() {
    let root = tempfile::tempdir().unwrap();
    let created = Arc::new(Mutex::new(None::<Arc<Session>>));
    let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let captures = requests.clone();
    let provider_started = Arc::new(tokio::sync::Notify::new());
    let provider_signal = provider_started.clone();
    let release_provider = Arc::new(tokio::sync::Semaphore::new(0));
    let provider_gate = release_provider.clone();
    let agent = make_agent_with_call_model_and_config(
        Box::new(move |request, signal| {
            {
                let mut captures = captures.lock().unwrap();
                assert!(captures.is_empty(), "bounded provider captures");
                captures.push(request);
            }
            provider_signal.notify_one();
            let gate = provider_gate.clone();
            Box::pin(
                futures::stream::once(async move {
                    tokio::select! {
                        _ = signal.cancelled() => Vec::new(),
                        permit = gate.acquire() => {
                            permit.unwrap().forget();
                            text_turn("settings observed")
                        }
                    }
                })
                .flat_map(futures::stream::iter),
            )
        }),
        Some(
            json!({"prewarm":false,"autoMemory":false,"autoDream":false,"memory":{"enabled":false}}),
        ),
    );
    let factory = crate::lifecycle::engine_sessions_in_test_root(
        agent,
        None,
        RuntimePath::from(root.path().to_string_lossy().into_owned()),
    )
    .create_session;
    let capture = created.clone();
    let create: CreateSession = Arc::new(move |ctx| {
        let factory = factory.clone();
        let capture = capture.clone();
        Box::pin(async move {
            let session = factory(ctx).await?;
            *capture.lock().unwrap() = Some(session.clone());
            Ok::<_, BoxError>(session)
        })
    });
    let mut client = TestClient::start_full(
        create,
        None,
        Some(root.path().join("rules.json")),
        Some(root.path().join("config.toml")),
    );
    client.initialize().await;
    let started = request(&mut client, 2, "session/start", json!({"init":{"threadId":"runtime-settings","cwd":root.path(),"networkAllowed":true,"config":{"prewarm":false,"sandbox_mode":"read-only","approval_policy":"on-request","approvals_reviewer":"user","sandbox_workspace_write":{"network_access":false},"webSearch":{"mode":"native","access":"cached"}}}})).await;
    let sid = started["sessionId"].as_str().unwrap();
    let ack = request(&mut client, 3, "session/updateConfig", json!({"sessionId":sid,"submissionId":"settings-live","update":{"modelVerbosity":"high","reasoningSummary":"detailed","webSearchAccess":"indexed"}})).await;
    assert_eq!(ack["submissionId"], "settings-live");
    let session = created
        .lock()
        .unwrap()
        .clone()
        .expect("configuration ACK materializes and applies the session");
    assert!(
        session.runtime_policy_state().network_allowed,
        "startup network selection survives lazy materialization"
    );
    let settings = session.runtime_inference_settings();
    assert_eq!(settings.model_verbosity, Some(OutputVerbosity::High));
    assert_eq!(settings.reasoning_summary, ReasoningSummary::Detailed);
    assert_eq!(settings.web_search_access, WebSearchAccess::Indexed);
    request(&mut client, 4, "session/setRuntimePolicy", json!({"sessionId":sid,"permissionMode":"default","approvalPolicy":"never","sandboxMode":"read-only","approvalsReviewer":"auto_review","networkAllowed":true})).await;
    let policy = session.runtime_policy_state();
    assert_eq!(policy.approval_policy, Some(ApprovalPolicy::Never));
    assert_eq!(policy.sandbox_mode, Some(SandboxMode::ReadOnly));
    assert_eq!(
        policy.approvals_reviewer,
        Some(ApprovalsReviewer::Automatic)
    );
    assert!(
        policy.network_allowed,
        "read-only permits an explicit network selection"
    );
    request(
        &mut client,
        40,
        "session/setRuntimePolicy",
        json!({
            "sessionId":sid,"permissionMode":"default","sandboxMode":"read-only",
            "sandboxWorkspaceWrite":{"network_access":false}
        }),
    )
    .await;
    assert!(
        session.runtime_policy_state().network_allowed,
        "a policy rebuild preserves the selected network value when omitted"
    );
    client.send(json!({"id":5,"method":"turn/start","params":{"sessionId":sid,"input":"Explain the current settings.","submissionId":"settings-turn"}})).await;
    let (turn, frames) = client.response_for(5).await;
    assert!(turn.get("error").is_none(), "{turn}");
    tokio::time::timeout(REPLY_TIMEOUT, provider_started.notified())
        .await
        .expect("the active provider request reaches its held stream");
    client.send(json!({"id":6,"method":"session/updateConfig","params":{"sessionId":sid,"submissionId":"invalid-active-settings","update":{"modelVerbosity":"invalid"}}})).await;
    let (invalid, _) = client.response_for(6).await;
    assert!(invalid.get("error").is_some(), "{invalid}");
    assert_eq!(
        session.runtime_inference_settings().model_verbosity,
        Some(OutputVerbosity::High),
        "an invalid update must reject without publishing settings"
    );
    request(&mut client, 7, "session/setRuntimePolicy", json!({"sessionId":sid,"permissionMode":"default","approvalPolicy":"on-request","sandboxMode":"workspace-write","approvalsReviewer":"user","networkAllowed":false})).await;
    let policy = session.runtime_policy_state();
    assert_eq!(policy.sandbox_mode, Some(SandboxMode::WorkspaceWrite));
    assert_eq!(policy.approval_policy, Some(ApprovalPolicy::OnRequest));
    assert_eq!(policy.approvals_reviewer, Some(ApprovalsReviewer::User));
    request(&mut client, 8, "session/updateConfig", json!({"sessionId":sid,"update":{"modelVerbosity":null,"reasoningSummary":"auto","webSearchAccess":"disabled"}})).await;
    let settings = session.runtime_inference_settings();
    assert_eq!(settings.model_verbosity, None);
    assert_eq!(settings.reasoning_summary, ReasoningSummary::Auto);
    assert_eq!(settings.web_search_access, WebSearchAccess::Disabled);
    release_provider.add_permits(1);
    let mut pending = std::collections::VecDeque::from(frames);
    tokio::time::timeout(REPLY_TIMEOUT, async {
        loop {
            let frame = match pending.pop_front() {
                Some(frame) => frame,
                None => client.next_message().await,
            };
            assert_ne!(frame["method"], "turn/failed", "{frame}");
            if frame["method"] == "turn/completed" {
                break;
            }
        }
    })
    .await
    .expect("the settings turn completes");
    {
        let captures = requests.lock().unwrap();
        assert_eq!(captures.len(), 1);
        assert_eq!(captures[0].model_verbosity, Some(OutputVerbosity::High));
        assert_eq!(captures[0].reasoning_summary, ReasoningSummary::Detailed);
    }
    let revision = session.model_revision();
    session
        .begin_model_update("pending-settings-model".into(), Some(revision))
        .unwrap();
    client.send(json!({"id":9,"method":"session/updateConfig","params":{"sessionId":sid,"submissionId":"settings-pending-error","update":{"webSearchAccess":"live"}}})).await;
    let (pending_error, _) = client.response_for(9).await;
    assert!(pending_error.get("error").is_some(), "{pending_error}");
    assert!(pending_error["error"]["message"]
        .as_str()
        .unwrap()
        .contains("model update is pending"));
    assert_eq!(session.model_revision(), revision);
    assert_eq!(
        session.runtime_inference_settings().web_search_access,
        WebSearchAccess::Disabled,
        "an apply error rejects the RPC without publishing a partial update"
    );
    client.close().await;
}
