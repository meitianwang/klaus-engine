//! Defect: subscription failures lost the provider's actionable error details.
//! Root cause: an SSE stream nested in a gateway error was decoded as JSON.
//! Work item: 20261019 subscription error preservation through the named facade.

use super::*;
use klaus_agent_core::runtime_values::RuntimePath;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

const NESTED_MESSAGE: &str = "The fixture request parameter is invalid";

struct FixtureEnvironment {
    root: PathBuf,
    variables: Vec<(&'static str, Option<OsString>)>,
}

impl FixtureEnvironment {
    fn new(profile: &Path, upstream: &str) -> Self {
        let root = crate::app_server::qoder::config_root();
        crate::app_server::qoder::set_config_root(profile);
        crate::app_server::qoder::catalog::forget_context();
        let mut variables = Vec::new();
        for (name, value) in [
            ("QODER_API_BASE", OsString::from(upstream)),
            (
                "QODER_HOME",
                profile.join("unavailable-client").into_os_string(),
            ),
            (
                "KLAUS_DIAGNOSTICS_DIR",
                profile.join("diagnostics").into_os_string(),
            ),
        ] {
            variables.push((name, std::env::var_os(name)));
            klaus_agent_core::runtime_process::set_process_environment_var(name, value);
        }
        Self { root, variables }
    }
}

impl Drop for FixtureEnvironment {
    fn drop(&mut self) {
        crate::app_server::qoder::set_config_root(&self.root);
        crate::app_server::qoder::catalog::forget_context();
        for (name, previous) in self.variables.drain(..).rev() {
            if let Some(previous) = previous {
                klaus_agent_core::runtime_process::set_process_environment_var(name, previous);
            } else {
                klaus_agent_core::runtime_process::remove_process_environment_var(name);
            }
        }
    }
}

struct GatewayFixture {
    url: String,
    calls: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for GatewayFixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn gateway_fixture() -> GatewayFixture {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let task = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut received = Vec::new();
            let mut buffer = [0; 8192];
            let path = loop {
                let count = socket.read(&mut buffer).await.unwrap();
                if count == 0 {
                    break None;
                }
                received.extend_from_slice(&buffer[..count]);
                let Some(end) = received.windows(4).position(|bytes| bytes == b"\r\n\r\n") else {
                    continue;
                };
                let headers = String::from_utf8_lossy(&received[..end]);
                let length = headers
                    .lines()
                    .filter_map(|line| line.split_once(':'))
                    .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                    .map(|(_, value)| value.trim().parse::<usize>().unwrap())
                    .unwrap_or(0);
                if received.len() >= end + 4 + length {
                    break headers
                        .lines()
                        .next()
                        .and_then(|line| line.split_whitespace().nth(1))
                        .map(str::to_string);
                }
            };
            let Some(path) = path else { continue };
            eprintln!("subscription fixture gateway path={path}");
            let (content_type, body) = if path.contains("agent_chat_generation") {
                let number = counted.fetch_add(1, Ordering::SeqCst) + 1;
                let nested = json!({"error": {
                    "message": NESTED_MESSAGE,
                    "code": "invalid_parameter_error",
                    "type": "invalid_request_error",
                }, "id": "chatcmpl-not-a-request-id"});
                let detail = json!({
                    "message": "Error in upstream response",
                    "code": "provider_error",
                    "type": "provider_error",
                    "request_id": format!("req_subscription_{number}"),
                    "details": format!("data: {nested}\n\n"),
                });
                let envelope = json!({"statusCode":"BAD_REQUEST", "statusCodeValue":400, "body":detail.to_string()});
                ("text/event-stream", format!("data: {envelope}\n\n"))
            } else {
                let model = json!({"key":"fixture-model", "display_name":"Fixture", "format":"openai", "source":"system", "max_input_tokens":180000, "max_output_tokens":4096, "is_reasoning":false, "is_vl":false});
                ("application/json", json!({"assistant":[model]}).to_string())
            };
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            socket.shutdown().await.unwrap();
        }
    });
    GatewayFixture { url, calls, task }
}

async fn request(
    client: &mut TestClient,
    id: u64,
    method: &str,
    params: Value,
) -> (Value, Vec<Value>) {
    eprintln!("subscription fixture request id={id} method={method}");
    client
        .send(json!({"id":id,"method":method,"params":params}))
        .await;
    let (response, notifications) = client.response_for(id).await;
    eprintln!("subscription fixture response id={id} method={method}");
    assert!(response.get("error").is_none(), "{response}");
    (response["result"].clone(), notifications)
}

fn assert_provider_details(error: &Value, request_id: &str) {
    assert!(
        error["message"]
            .as_str()
            .is_some_and(|text| text.contains(NESTED_MESSAGE)),
        "{error}"
    );
    let provider = &error["providerError"];
    assert!(
        provider["message"]
            .as_str()
            .is_some_and(|text| text.contains(NESTED_MESSAGE)),
        "{error}"
    );
    assert_eq!(provider["code"], "invalid_parameter_error", "{error}");
    assert_eq!(provider["type"], "invalid_request_error", "{error}");
    assert_eq!(provider["status"], 400, "{error}");
    assert_eq!(provider["requestId"], request_id, "{error}");
    assert_eq!(error["requestId"], request_id, "{error}");
}

#[tokio::test]
async fn subscription_nested_sse_errors_survive_named_turns_and_history_in_both_modes() {
    let _lock = crate::app_server::qoder::test_env_lock().lock().await;
    let profile = tempfile::tempdir().unwrap();
    let gateway = gateway_fixture().await;
    let _environment = FixtureEnvironment::new(profile.path(), &gateway.url);
    let config = profile.path().join("config.toml");
    std::fs::write(&config, "autoMemory = false\ncheckpointing = false\n").unwrap();
    std::fs::write(profile.path().join("qoder-subscription-oauth.json"), json!({
        "machineId":"00000000-0000-0000-0000-000000000000",
        "userInfo":{"uid":"fixture-subscription-user", "name":"Fixture", "access_token":"fixture-access-token", "security_oauth_token":"fixture-access-token", "encrypt_user_info":"fixture-encrypted-user-info", "key":"fixture-signing-key", "expire_time":4102444800_i64}
    }).to_string()).unwrap();
    let store: Arc<dyn SessionStore> = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(
            profile
                .path()
                .join("sessions")
                .to_string_lossy()
                .into_owned(),
        ),
        ..Default::default()
    }));
    let agent = make_agent_with_provider_and_config(Arc::new(fake_provider()), None, None);
    let create = crate::lifecycle::engine_sessions_in_test_root(
        agent,
        Some(store),
        RuntimePath::from(profile.path().to_string_lossy().into_owned()),
    )
    .create_session;
    let mut client = TestClient::start_full(
        create,
        None,
        Some(profile.path().join("rules.json")),
        Some(config),
    );
    request(
        &mut client,
        1,
        "initialize",
        json!({"clientInfo":{"name":"subscription-error-fixture"},"capabilities":{"productEvents":true}}),
    )
    .await;
    client.send(json!({"method":"initialized"})).await;

    for (index, mode) in ["default", "plan"].into_iter().enumerate() {
        let (started, _) = request(&mut client, 10, "session/start", json!({"init":{
            "cwd":profile.path(), "modelSelection":"qoder:fixture-model", "permissionMode":"default", "collaborationMode":mode,
            "baseInstructions":"Do not use tools.", "dynamicTools":[],
            "config":{"prewarm":false, "autoMemory":false, "checkpointing":false, "shellEnvCapture":false, "includeGitContext":false, "maxRetries":0, "maxTurns":1, "maxOutputTokens":64, "maxOutputRetryTokens":64, "unboundedNetworkRetries":false}
        }})).await;
        let session_id = started["sessionId"].as_str().unwrap();
        let (_, early) = request(&mut client, 11, "turn/start", json!({"sessionId":session_id,"input":"Return the fixture result without using tools."})).await;
        let mut pending: std::collections::VecDeque<_> = early.into();
        let mut admitted_turn_id = None;
        let failed = loop {
            let event = if let Some(event) = pending.pop_front() {
                event
            } else {
                client.next_message().await
            };
            assert!(
                event.get("method").is_none() || event.get("id").is_none(),
                "Unexpected reverse request: {event}"
            );
            if event["method"] == "turn/started" {
                admitted_turn_id = event["params"]["detail"]["turnId"]
                    .as_str()
                    .map(str::to_string);
            }
            if event["method"] == "turn/failed" {
                break event;
            }
            assert_ne!(
                event["method"], "turn/completed",
                "Failure completed successfully: {event}"
            );
        };
        // The admission reply identifies the submission; persisted diagnostics
        // use the engine turn identity published when execution starts.
        let turn_id = admitted_turn_id
            .as_deref()
            .expect("an admitted engine turn");
        let expected_request_id = format!("req_subscription_{}", index + 1);
        assert_provider_details(&failed["params"]["detail"]["error"], &expected_request_id);
        let (history, _) = request(
            &mut client,
            12,
            "session/history",
            json!({"sessionId":session_id}),
        )
        .await;
        assert_provider_details(&history["turnErrorDetails"][turn_id], &expected_request_id);
        assert!(
            history["turnErrors"][turn_id]
                .as_str()
                .is_some_and(|text| text.contains(NESTED_MESSAGE)),
            "{history}"
        );
        assert_eq!(
            gateway.calls.load(Ordering::SeqCst),
            index + 1,
            "A terminal parameter error must not retry"
        );
        request(
            &mut client,
            13,
            "session/close",
            json!({"sessionId":session_id}),
        )
        .await;
    }
    client.close().await;
}
