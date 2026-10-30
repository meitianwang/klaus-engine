//! Defect: the catalog exposed model definitions from the wrong service audience.
//! Root cause: the catalog request omitted its command audience marker.
//! Work item: 20261019 account catalog selection through the public facade.

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
            } else if user_agent.contains("CLI/") {
                let mut disabled = model("disabled-chat");
                disabled["disabled"] = json!(true);
                let mut local = model("local-chat");
                local["local"] = json!(true);
                let media = json!({"id":"media-model", "name":"Media", "tags":["text-to-image"]});
                (
                    200,
                    json!({"data":{
                        "models":[model("public-default"), model("public-reasoner"), model("hidden-definition"), disabled, local, media],
                        "agents":[{
                            "name":"cli", "tags":["cli","default"],
                            "models":["public-default","public-reasoner","disabled-chat","local-chat","media-model"]
                        }],
                        "productFeatures":{"EnableAutoModelTiers":false}
                    }}),
                )
            } else {
                (
                    200,
                    json!({"data":{
                        "models":[model("internal-retired-model"), model("public-reasoner")],
                        "agents":[{"name":"craft", "models":["public-reasoner"]}],
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

async fn assert_catalog_facade(legacy_snapshot: bool) {
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
                "uid":"fixture-catalog-user", "access_token":"fixture-catalog-token",
                "domain":gateway.url, "login_method":"adopted", "expire_time":4102444800_i64
            }
        })
        .to_string(),
    )
    .unwrap();
    let snapshot_path = profile.path().join("workbuddy-model-catalog.json");
    let legacy_fingerprint = json!(["fixture-catalog-user", gateway.url]).to_string();
    if legacy_snapshot {
        std::fs::write(
            &snapshot_path,
            json!({
                "fingerprint":legacy_fingerprint,
                "models":[{"key":"internal-retired-model", "display_name":"Retired"}]
            })
            .to_string(),
        )
        .unwrap();
    }
    let mut client = TestClient::start_full(
        make_create_session(&[]),
        None,
        Some(profile.path().join("rules.json")),
        Some(config),
    );
    client.initialize().await;
    let reads = if legacy_snapshot {
        ["warm", "cached"]
    } else {
        ["cached", "warm"]
    };
    for (index, read) in reads.into_iter().enumerate() {
        let id = index as u64 + 10;
        client
            .send(json!({
                "id":id, "method":"workbuddy/models", "params":{"read":read}
            }))
            .await;
        let (reply, _) = client.response_for(id).await;
        assert!(reply.get("error").is_none(), "{reply}");
        let models = reply["result"]["models"].as_array().unwrap();
        let keys: Vec<_> = models
            .iter()
            .map(|row| row["key"].as_str().unwrap())
            .collect();
        assert_eq!(
            keys,
            [
                "public-default",
                "public-reasoner",
                "hidden-definition",
                "disabled-chat",
                "local-chat"
            ],
            "the {read} facade must retain the upstream chat directory: {reply}"
        );
        let snapshot: Value =
            serde_json::from_str(&std::fs::read_to_string(&snapshot_path).unwrap()).unwrap();
        if legacy_snapshot {
            assert_ne!(
                snapshot["fingerprint"], legacy_fingerprint,
                "the account catalog is stored with its current audience fingerprint"
            );
        }
    }
    {
        let requests = gateway.requests.lock().unwrap();
        assert_eq!(
            requests.len(),
            1,
            "the second facade read uses the matching cached catalog"
        );
        assert_eq!(requests[0].0, "/v3/config");
        assert!(
            requests[0].1.contains("CLI/"),
            "the catalog must declare its command audience"
        );
    }
    client.close().await;
}

#[tokio::test]
async fn catalog_facade_requests_command_audience_and_exposes_only_allowed_chat_models() {
    assert_catalog_facade(false).await;
}

#[tokio::test]
async fn catalog_facade_ignores_the_legacy_audience_snapshot_before_painting_models() {
    assert_catalog_facade(true).await;
}
