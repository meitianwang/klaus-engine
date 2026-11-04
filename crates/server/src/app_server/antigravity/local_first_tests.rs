//! Local-first catalog behavior exercised through the app-server test client.
//! The public stdio regression lives under core/tests/regressions.

use super::{TestClient, make_create_session};
use crate::app_server::antigravity;
use klaus_agent_core::runtime_process::{
    remove_process_environment_var, set_process_environment_var,
};
use serde_json::{Value, json};
use std::ffi::OsString;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Clone)]
enum Reply {
    Hold,
    Status(u16),
    Catalog(Value),
    DelayedCatalog(Value, Arc<tokio::sync::Notify>),
}

struct CloudFixture {
    origin: String,
    requests: Arc<Mutex<Vec<String>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for CloudFixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl CloudFixture {
    async fn start(reply: Reply) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&requests);
        let task = tokio::spawn(async move {
            let mut held = Vec::new();
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut received = Vec::new();
                let mut buffer = [0u8; 4096];
                let target = loop {
                    let count = socket.read(&mut buffer).await.unwrap();
                    assert!(count > 0, "a complete local request");
                    received.extend_from_slice(&buffer[..count]);
                    assert!(received.len() <= 64 * 1024, "bounded request headers");
                    let Some(end) = received.windows(4).position(|bytes| bytes == b"\r\n\r\n")
                    else {
                        continue;
                    };
                    break String::from_utf8_lossy(&received[..end])
                        .lines()
                        .next()
                        .and_then(|line| line.split_whitespace().nth(1))
                        .unwrap()
                        .to_string();
                };
                captured.lock().unwrap().push(target);
                let (status, body) = match &reply {
                    Reply::Hold => {
                        held.push(socket);
                        continue;
                    }
                    Reply::Status(status) => {
                        (*status, json!({"error":{"code":500,"message":"fixture"}}))
                    }
                    Reply::Catalog(body) => (200, body.clone()),
                    Reply::DelayedCatalog(body, release) => {
                        release.notified().await;
                        (200, body.clone())
                    }
                };
                let body = body.to_string();
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
                socket.shutdown().await.unwrap();
            }
        });
        Self {
            origin,
            requests,
            task,
        }
    }

    fn requests(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}

struct Environment(Option<OsString>);

impl Environment {
    fn set(endpoint: &str) -> Self {
        let previous = std::env::var_os("KLAUS_ANTIGRAVITY_API_URL");
        set_process_environment_var("KLAUS_ANTIGRAVITY_API_URL", endpoint);
        Self(previous)
    }
}

impl Drop for Environment {
    fn drop(&mut self) {
        if let Some(previous) = self.0.take() {
            set_process_environment_var("KLAUS_ANTIGRAVITY_API_URL", previous);
        } else {
            remove_process_environment_var("KLAUS_ANTIGRAVITY_API_URL");
        }
    }
}

fn credential() -> antigravity::auth::Credential {
    serde_json::from_value(json!({
        "accessToken":"fixture-token", "authMethod":"consumer", "project":"fixture-project"
    }))
    .unwrap()
}

fn live_catalog() -> Value {
    json!({
        "defaultAgentModelId":"live-model",
        "models":{"live-model":{"displayName":"Live Model","supportsThinking":true,"thinkingLevel":1}}
    })
}

async fn wait_until(mut ready: impl FnMut() -> bool, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while !ready() {
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    true
}

fn start_client(root: &Path) -> TestClient {
    TestClient::start_full(
        make_create_session(&[]),
        None,
        Some(root.join("rules.json")),
        Some(root.join("config.json")),
    )
}

async fn catalog_request(client: &mut TestClient, id: u64) -> Value {
    let started = Instant::now();
    client
        .send(json!({"id":id,"method":"antigravity/models","params":{"read":"warm"}}))
        .await;
    let response = client
        .response_for_with_timeout(id, Duration::from_secs(2))
        .await
        .0;
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "catalog reads must not await the network"
    );
    response
}

#[tokio::test]
async fn cold_and_inflight_reads_answer_from_the_baseline_with_one_refresh() {
    let root = tempfile::tempdir().unwrap();
    antigravity::set_config_root(root.path());
    let server = CloudFixture::start(Reply::Hold).await;
    let _environment = Environment::set(&server.origin);
    antigravity::auth::adopt(credential()).await.unwrap();
    let generation = antigravity::auth::generation().unwrap();
    std::fs::write(
        root.path().join("antigravity-models.json"),
        json!({
            "generation":"other-generation",
            "payload":{"models":{"legacy-only":{"displayName":"Other account"}}},
            "fetchedAt":1,
            "clientIdentity":antigravity::cloud_test_identity(),
        })
        .to_string(),
    )
    .unwrap();
    let mut client = start_client(root.path());
    client.initialize().await;

    let response = catalog_request(&mut client, 91).await;
    assert_eq!(response["result"]["catalogBaseline"], true);
    let rows = response["result"]["models"].as_array().unwrap();
    let baseline = rows
        .iter()
        .find(|row| row["id"] == "gemini-4-argon-medium")
        .unwrap_or_else(|| panic!("bundled catalog is missing its default: {response}"));
    assert_eq!(baseline["available"], false);
    assert_eq!(baseline["entitlementUnknown"], true);
    assert!(
        rows.iter()
            .any(|row| row["id"] == "gemini-3.8-flash-medium")
    );
    assert!(!rows.iter().any(|row| row["id"] == "legacy-only"));

    assert!(
        wait_until(|| server.requests() > 0, Duration::from_secs(2)).await,
        "the first read must start a background refresh"
    );
    for id in 92..=95 {
        let response = catalog_request(&mut client, id).await;
        assert!(response["result"]["models"].is_array());
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        server.requests(),
        1,
        "all readers share the in-flight background refresh"
    );

    client.close().await;
    antigravity::auth::logout().await.unwrap();
    assert!(antigravity::auth::generation().is_none());
    let _ = generation;
}

#[tokio::test]
async fn a_stale_credential_snapshot_paints_first_and_revalidates_behind_the_caller() {
    let root = tempfile::tempdir().unwrap();
    antigravity::set_config_root(root.path());
    let server = CloudFixture::start(Reply::Catalog(live_catalog())).await;
    let _environment = Environment::set(&server.origin);
    antigravity::auth::adopt(credential()).await.unwrap();
    let generation = antigravity::auth::generation().unwrap();
    std::fs::write(
        root.path().join("antigravity-models.json"),
        json!({
            "generation":generation,
            "payload":{"models":{"stale-only":{"displayName":"Stale Only"}}},
            "fetchedAt":1,
            "clientIdentity":antigravity::cloud_test_identity(),
        })
        .to_string(),
    )
    .unwrap();
    let mut client = start_client(root.path());
    client.initialize().await;

    let response = catalog_request(&mut client, 101).await;
    assert_eq!(response["result"]["catalogBaseline"], false);
    let rows = response["result"]["models"].as_array().unwrap();
    assert!(rows.iter().any(|row| row["id"] == "stale-only"));

    let snapshot = root.path().join("antigravity-models.json");
    assert!(
        wait_until(
            || std::fs::read_to_string(&snapshot)
                .ok()
                .and_then(|text| serde_json::from_str::<Value>(&text).ok())
                .is_some_and(|value| value["payload"]["models"]["live-model"].is_object()),
            Duration::from_secs(2)
        )
        .await,
        "a stale snapshot must revalidate in the background"
    );
    let response = catalog_request(&mut client, 102).await;
    assert_eq!(response["result"]["catalogBaseline"], false);
    let rows = response["result"]["models"].as_array().unwrap();
    assert!(rows.iter().any(|row| row["id"] == "live-model"));
    assert!(!rows.iter().any(|row| row["id"] == "stale-only"));
    assert_eq!(server.requests(), 1);

    client.close().await;
    antigravity::auth::logout().await.unwrap();
}

#[tokio::test]
async fn a_failed_refresh_observes_its_cooldown_and_a_new_account_is_not_blocked() {
    let root = tempfile::tempdir().unwrap();
    antigravity::set_config_root(root.path());
    let server = CloudFixture::start(Reply::Status(500)).await;
    let _environment = Environment::set(&server.origin);
    antigravity::auth::adopt(credential()).await.unwrap();
    let generation = antigravity::auth::generation().unwrap();
    let mut client = start_client(root.path());
    client.initialize().await;

    let response = catalog_request(&mut client, 111).await;
    assert!(response["result"]["models"].is_array());
    assert!(
        wait_until(
            || antigravity::refresh_blocked(&generation),
            Duration::from_secs(2)
        )
        .await,
        "the failed refresh must enter its cooldown"
    );
    let requests = server.requests();
    let response = catalog_request(&mut client, 112).await;
    assert!(response["result"]["models"].is_array());
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        server.requests(),
        requests,
        "a failed refresh is not restarted by every catalog read"
    );

    antigravity::auth::adopt(credential()).await.unwrap();
    let replacement = antigravity::auth::generation().unwrap();
    assert_ne!(replacement, generation);
    assert!(
        !antigravity::refresh_blocked(&replacement),
        "a replacement account starts with a clean refresh budget"
    );
    client.close().await;
    antigravity::auth::logout().await.unwrap();
}

#[tokio::test]
async fn a_late_refresh_cannot_publish_for_a_replaced_account() {
    let root = tempfile::tempdir().unwrap();
    antigravity::set_config_root(root.path());
    let release = Arc::new(tokio::sync::Notify::new());
    let server =
        CloudFixture::start(Reply::DelayedCatalog(live_catalog(), Arc::clone(&release))).await;
    let _environment = Environment::set(&server.origin);
    antigravity::auth::adopt(credential()).await.unwrap();
    let old = antigravity::auth::generation().unwrap();
    let mut client = start_client(root.path());
    client.initialize().await;

    let response = catalog_request(&mut client, 121).await;
    assert!(response["result"]["models"].is_array());
    assert!(
        wait_until(|| server.requests() == 1, Duration::from_secs(2)).await,
        "the background request must be in flight"
    );
    antigravity::auth::adopt(credential()).await.unwrap();
    let replacement = antigravity::auth::generation().unwrap();
    assert_ne!(replacement, old);
    let before = antigravity::refresh_completions();
    release.notify_one();
    assert!(
        wait_until(
            || antigravity::refresh_completions() > before,
            Duration::from_secs(2)
        )
        .await,
        "the delayed refresh must settle"
    );

    let snapshot = root.path().join("antigravity-models.json");
    if let Ok(text) = std::fs::read_to_string(&snapshot) {
        let value: Value = serde_json::from_str(&text).unwrap();
        assert_ne!(
            value["generation"], old,
            "a stale account wrote its catalog"
        );
    }
    assert_eq!(
        antigravity::auth::generation().as_deref(),
        Some(replacement.as_str())
    );

    client.close().await;
    antigravity::auth::logout().await.unwrap();
}
