//! Defect: WorkBuddy rejected complete collaboration instructions before output.
//! Root cause: the source requires the system role for trusted instructions.
//! Work item: 20261019 source-scoped instruction roles through the named facade.

use super::*;
use klaus_agent_core::engine::session::resolve_config_default;
use klaus_agent_core::planning::{instructions::instruction_part, CollaborationMode};
use klaus_agent_core::runtime_values::RuntimePath;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

struct FixtureEnvironment {
    root: PathBuf,
    variables: Vec<(&'static str, Option<OsString>)>,
}

impl FixtureEnvironment {
    fn new(profile: &Path, upstream: &str) -> Self {
        let root = crate::app_server::workbuddy::config_root();
        crate::app_server::workbuddy::set_config_root(profile);
        crate::app_server::workbuddy::catalog::forget_cache();
        let mut variables = Vec::new();
        for (name, value) in [
            ("WORKBUDDY_API_BASE", OsString::from(upstream)),
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
        crate::app_server::workbuddy::set_config_root(&self.root);
        crate::app_server::workbuddy::catalog::forget_cache();
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
    requests: Arc<Mutex<Vec<Value>>>,
    unexpected_paths: Arc<Mutex<Vec<String>>>,
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
    let requests = Arc::new(Mutex::new(Vec::new()));
    let unexpected_paths = Arc::new(Mutex::new(Vec::new()));
    let captured = requests.clone();
    let unexpected = unexpected_paths.clone();
    let task = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut received = Vec::new();
            let mut buffer = [0; 8192];
            let (path, body) = loop {
                let count = socket.read(&mut buffer).await.unwrap();
                assert!(count > 0, "the local fixture receives a complete request");
                received.extend_from_slice(&buffer[..count]);
                assert!(received.len() <= 8 * 1024 * 1024, "bounded fixture input");
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
                    let path = headers
                        .lines()
                        .next()
                        .and_then(|line| line.split_whitespace().nth(1))
                        .unwrap()
                        .to_string();
                    let body = if length == 0 {
                        Value::Null
                    } else {
                        serde_json::from_slice(&received[end + 4..end + 4 + length]).unwrap()
                    };
                    break (path, body);
                }
            };
            let (status, content_type, response) = match path.as_str() {
                "/v3/config" => (
                    200,
                    "application/json",
                    json!({"models":[{
                        "id":"auto", "name":"Fixture Auto", "isDefault":true,
                        "supportsToolCall":true, "supportsReasoning":false,
                        "supportsImages":false, "contextWindow":180000,
                        "maxOutputTokens":4096
                    }]})
                    .to_string(),
                ),
                "/v2/chat/completions" => {
                    captured.lock().unwrap().push(body.clone());
                    let rejected = body["model"] == "auto"
                        && body["messages"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .any(|message| message["role"] == "developer");
                    if rejected {
                        (
                            400,
                            "application/json",
                            json!({
                                "code":11128,
                                "msg":"Illegal API invocation from an unapproved channel",
                                "requestId":"fixture-workbuddy-role-rejection"
                            })
                            .to_string(),
                        )
                    } else {
                        (
                            200,
                            "text/event-stream",
                            concat!(
                                "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"OK\"},\"finish_reason\":null}]}\n\n",
                                "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                                "data: [DONE]\n\n"
                            ).to_string(),
                        )
                    }
                }
                _ => {
                    unexpected.lock().unwrap().push(path);
                    (
                        400,
                        "application/json",
                        json!({"error":"unexpected fixture route"}).to_string(),
                    )
                }
            };
            socket.write_all(format!(
                "HTTP/1.1 {status} Fixture\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",
                response.len()
            ).as_bytes()).await.unwrap();
            socket.shutdown().await.unwrap();
        }
    });
    GatewayFixture {
        url,
        requests,
        unexpected_paths,
        task,
    }
}

async fn request(
    client: &mut TestClient,
    id: u64,
    method: &str,
    params: Value,
) -> (Value, Vec<Value>) {
    client
        .send(json!({"id":id,"method":method,"params":params}))
        .await;
    let (response, notifications) = client.response_for(id).await;
    assert!(response.get("error").is_none(), "{response}");
    (response["result"].clone(), notifications)
}

#[tokio::test]
async fn workbuddy_full_modes_use_system_without_changing_another_chat_provider() {
    let _lock = crate::app_server::workbuddy::test_env_lock().lock().await;
    let profile = tempfile::tempdir().unwrap();
    let gateway = gateway_fixture().await;
    let _environment = FixtureEnvironment::new(profile.path(), &gateway.url);
    let config = profile.path().join("config.toml");
    std::fs::write(&config, "autoMemory = false\ncheckpointing = false\n").unwrap();
    std::fs::write(
        profile.path().join("workbuddy-subscription-oauth.json"),
        json!({
            "deviceId":"fixture-device",
            "userInfo":{
                "uid":"fixture-user", "access_token":"fixture-access-token",
                "domain":gateway.url, "login_method":"adopted", "expire_time":4102444800_i64
            }
        })
        .to_string(),
    )
    .unwrap();
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
        json!({
            "clientInfo":{"name":"workbuddy-role-fixture"},
            "capabilities":{"productEvents":true}
        }),
    )
    .await;
    client.send(json!({"method":"initialized"})).await;

    for (source, expected_role) in [("workbuddy", "system"), ("native", "developer")] {
        for (mode, mode_value) in [
            ("default", CollaborationMode::Default),
            ("plan", CollaborationMode::Plan),
        ] {
            let mut init = json!({
                "cwd":profile.path(), "permissionMode":"default", "collaborationMode":mode,
                "baseInstructions":"Do not use tools.", "dynamicTools":[],
                "config":{
                    "prewarm":false, "autoMemory":false, "checkpointing":false,
                    "shellEnvCapture":false, "includeGitContext":false, "maxRetries":0,
                    "maxTurns":1, "maxOutputTokens":64, "maxOutputRetryTokens":64,
                    "unboundedNetworkRetries":false
                }
            });
            if source == "workbuddy" {
                init["modelSelection"] = json!("workbuddy:auto");
            } else {
                init["provider"] = json!({
                    "type":"openai", "api":"chat", "model":"native-fixture",
                    "baseUrl":format!("{}/v2", gateway.url)
                });
                init["auth"] = json!({"type":"api_key", "key":"fixture-native-key"});
            }
            let before = gateway.requests.lock().unwrap().len();
            let (started, _) =
                request(&mut client, 10, "session/start", json!({"init":init})).await;
            let session_id = started["sessionId"].as_str().unwrap();
            let (admission, early) = request(
                &mut client,
                11,
                "turn/start",
                json!({
                    "sessionId":session_id, "input":"Reply OK without using tools."
                }),
            )
            .await;
            let submitted = admission["turnId"]
                .as_str()
                .expect("a named turn admission");
            let mut pending: std::collections::VecDeque<_> = early.into();
            let mut text = String::new();
            let completed = loop {
                let event = if let Some(event) = pending.pop_front() {
                    event
                } else {
                    client.next_message().await
                };
                assert!(
                    event.get("method").is_none() || event.get("id").is_none(),
                    "Unexpected reverse request: {event}"
                );
                assert_ne!(
                    event["method"], "turn/failed",
                    "{source}/{mode} must complete: {event}"
                );
                if event["method"] == "item/agentMessage/delta" {
                    text.push_str(event["params"]["delta"].as_str().unwrap());
                }
                if event["method"] == "turn/completed" {
                    break event;
                }
            };
            assert_eq!(completed["params"]["turnId"], submitted);
            assert_eq!(text, "OK");
            {
                let captured = gateway.requests.lock().unwrap();
                assert_eq!(
                    captured.len(),
                    before + 1,
                    "no retry or generic 11128 replay"
                );
                let body = &captured[before];
                let expected = instruction_part(&resolve_config_default(), mode_value)
                    .text
                    .try_to_utf8()
                    .unwrap();
                let messages = body["messages"].as_array().unwrap();
                let instructions: Vec<_> = messages
                    .iter()
                    .filter(|message| {
                        message["content"]
                            .as_str()
                            .is_some_and(|text| text.contains("<collaboration_mode>"))
                    })
                    .collect();
                assert_eq!(
                    instructions.len(),
                    1,
                    "one complete trusted mode instruction: {body}"
                );
                assert_eq!(
                    instructions[0]["content"], expected,
                    "the full mode template survives"
                );
                assert_eq!(
                    instructions[0]["role"], expected_role,
                    "source-scoped wire role"
                );
                if source == "workbuddy" {
                    assert!(messages
                        .iter()
                        .all(|message| message["role"] != "developer"));
                }
            }
            request(
                &mut client,
                12,
                "session/close",
                json!({"sessionId":session_id}),
            )
            .await;
        }
    }
    assert!(
        gateway.unexpected_paths.lock().unwrap().is_empty(),
        "only local catalog and inference routes may run"
    );
    client.close().await;
}
