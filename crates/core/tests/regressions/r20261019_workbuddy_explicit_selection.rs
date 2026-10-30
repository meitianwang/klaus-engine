//! Defect: catalog narrowing silently replaced explicitly selected models.
//! Root cause: picker visibility was used as the resolution allowlist.
//! Work item: 20261019 preserve explicit selection and its model metadata.

use super::*;
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
            ("WORKBUDDY_CLIENT_VERSION", OsString::from("8.0.0")),
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

struct CatalogFixture {
    url: String,
    requests: Arc<Mutex<Vec<(String, String)>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for CatalogFixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn model(id: &str) -> Value {
    json!({
        "id":id, "name":id, "supportsToolCall":true,
        "supportsReasoning":false, "supportsImages":false,
        "maxInputTokens":180000, "maxOutputTokens":4096
    })
}

async fn catalog_fixture() -> CatalogFixture {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = requests.clone();
    let task = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut received = Vec::new();
            let mut buffer = [0; 4096];
            let (path, user_agent) = loop {
                let count = socket.read(&mut buffer).await.unwrap();
                assert!(count > 0, "a complete local catalog request");
                received.extend_from_slice(&buffer[..count]);
                assert!(received.len() <= 64 * 1024, "bounded catalog headers");
                let Some(end) = received.windows(4).position(|bytes| bytes == b"\r\n\r\n") else {
                    continue;
                };
                let headers = String::from_utf8_lossy(&received[..end]);
                let path = headers
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .unwrap()
                    .to_string();
                let user_agent = headers
                    .lines()
                    .filter_map(|line| line.split_once(':'))
                    .find(|(name, _)| name.eq_ignore_ascii_case("user-agent"))
                    .map(|(_, value)| value.trim().to_string())
                    .unwrap_or_default();
                break (path, user_agent);
            };
            captured
                .lock()
                .unwrap()
                .push((path.clone(), user_agent.clone()));
            let (status, body) = if path != "/v3/config" {
                (400, json!({"error":"unexpected fixture route"}))
            } else {
                let mut preferred = model("fast-model");
                preferred["isDefault"] = json!(true);
                let mut hidden = model("previously-selected-model");
                hidden["visibility"] = json!("hidden");
                hidden["supportsReasoning"] = json!(true);
                hidden["contextWindow"] = json!(250000);
                hidden["reasoning"] = json!({
                    "defaultEffort":"medium", "supportedEfforts":["medium","high"]
                });
                (
                    200,
                    json!({"data":{
                        "models":[preferred, hidden],
                        "agents":[{"name":"cli", "tags":["cli","default"], "models":["fast-model"]}],
                        "productFeatures":{"EnableAutoModelTiers":false}
                    }}),
                )
            };
            let body = body.to_string();
            socket.write_all(format!(
                "HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            ).as_bytes()).await.unwrap();
            socket.shutdown().await.unwrap();
        }
    });
    CatalogFixture {
        url,
        requests,
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
        .send(json!({"id":id, "method":method, "params":params}))
        .await;
    let (reply, seen) = client.response_for(id).await;
    assert!(reply.get("error").is_none(), "{reply}");
    (reply["result"].clone(), seen)
}

async fn assert_selection(selection: &str, expected: &str, prefetch_picker: bool) {
    let _lock = crate::app_server::workbuddy::test_env_lock().lock().await;
    let profile = tempfile::tempdir().unwrap();
    let gateway = catalog_fixture().await;
    let _environment = FixtureEnvironment::new(profile.path(), &gateway.url);
    let config = profile.path().join("config.toml");
    std::fs::write(&config, "autoMemory = false\ncheckpointing = false\n").unwrap();
    std::fs::write(
        profile.path().join("workbuddy-subscription-oauth.json"),
        json!({
            "deviceId":"fixture-device",
            "userInfo":{
                "uid":"fixture-selection-user", "access_token":"fixture-selection-token",
                "domain":gateway.url, "login_method":"adopted", "expire_time":4102444800_i64
            }
        })
        .to_string(),
    )
    .unwrap();
    let captured = Arc::new(Mutex::new(Vec::new()));
    let observed = captured.clone();
    let base = make_create_session(&["OK"]);
    let create: CreateSession = Arc::new(move |ctx: SessionContext| {
        let provider = ctx.provider.as_ref().expect("a source-resolved provider");
        let model = provider.info().default_model.try_to_utf8().unwrap();
        let capabilities = ctx.init.capabilities.as_ref();
        let effort = capabilities.and_then(|value| value.effort.as_ref());
        let default_effort = effort
            .and_then(|value| value.default.as_ref())
            .map(|value| value.try_to_utf8().unwrap());
        let levels: Vec<_> = effort
            .into_iter()
            .flat_map(|value| &value.levels)
            .map(|value| value.try_to_utf8().unwrap())
            .collect();
        observed.lock().unwrap().push(json!({
            "model":model,
            "contextWindow":capabilities.and_then(|value| value.context_window),
            "defaultEffort":default_effort,
            "efforts":levels
        }));
        base(ctx)
    });
    let mut client = TestClient::start_full(
        create,
        None,
        Some(profile.path().join("rules.json")),
        Some(config),
    );
    client.initialize().await;
    if prefetch_picker {
        let (result, _) = request(
            &mut client,
            10,
            "workbuddy/models",
            json!({"read":"cached"}),
        )
        .await;
        let keys: Vec<_> = result["models"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["key"].as_str().unwrap())
            .collect();
        assert_eq!(
            keys,
            ["fast-model"],
            "the explicit target is absent from the picker"
        );
    } else {
        assert!(
            !profile.path().join("workbuddy-model-catalog.json").exists(),
            "automatic selection starts without a catalog snapshot"
        );
        assert!(
            gateway.requests.lock().unwrap().is_empty(),
            "automatic selection has not warmed the catalog"
        );
    }
    let (started, _) = request(
        &mut client,
        11,
        "session/start",
        json!({"init":{
            "cwd":profile.path(), "modelSelection":format!("workbuddy:{selection}"),
            "config":{"prewarm":false, "autoMemory":false, "checkpointing":false,
                "includeGitContext":false, "shellEnvCapture":false, "maxTurns":1}
        }}),
    )
    .await;
    let session_id = started["sessionId"].as_str().unwrap();
    let (_, seen) = request(
        &mut client,
        12,
        "turn/start",
        json!({
            "sessionId":session_id, "input":"Reply OK without using tools."
        }),
    )
    .await;
    let bindings = captured.lock().unwrap().clone();
    assert_eq!(
        bindings.len(),
        1,
        "the admitted turn materializes its provider once"
    );
    assert_eq!(
        bindings[0]["model"], expected,
        "an explicit target must not become the first picker entry"
    );
    if prefetch_picker {
        assert_eq!(
            bindings[0]["defaultEffort"], "medium",
            "hidden model defaults remain available for inference"
        );
        assert_eq!(bindings[0]["efforts"], json!(["medium", "high"]));
        assert_eq!(bindings[0]["contextWindow"], 250000);
    }
    let mut pending: std::collections::VecDeque<_> = seen.into();
    loop {
        let event = if let Some(event) = pending.pop_front() {
            event
        } else {
            client.next_message().await
        };
        assert_ne!(
            event["method"], "turn/failed",
            "the fixture turn must complete: {event}"
        );
        if event["method"] == "turn/completed" {
            break;
        }
    }
    request(
        &mut client,
        13,
        "session/close",
        json!({"sessionId":session_id}),
    )
    .await;
    assert!(
        gateway
            .requests
            .lock()
            .unwrap()
            .iter()
            .all(|(path, _)| path == "/v3/config"),
        "only local catalog requests are needed; inference stays scripted"
    );
    client.close().await;
}

#[tokio::test]
async fn an_explicit_workbuddy_model_keeps_its_identity_and_hidden_capabilities() {
    assert_selection(
        "previously-selected-model",
        "previously-selected-model",
        true,
    )
    .await;
}

#[tokio::test]
async fn cold_workbuddy_automatic_selection_waits_for_the_live_default() {
    assert_selection("auto", "fast-model", false).await;
}
