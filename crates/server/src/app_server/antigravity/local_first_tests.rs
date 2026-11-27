//! Local-first catalog behavior exercised through the app-server test client.
//! The public stdio regression lives under core/tests/regressions.

use super::{make_create_session, TestClient};
use crate::app_server::antigravity;
use klaus_agent_core::runtime_process::{
    remove_process_environment_var, set_process_environment_var,
};
use serde_json::{json, Value};
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

    fn requests_to(&self, suffix: &str) -> usize {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|target| target.ends_with(suffix))
            .count()
    }
}

struct Environment(Vec<(&'static str, Option<OsString>)>);

impl Environment {
    fn set(endpoint: &str) -> Self {
        let settings = [
            ("KLAUS_ANTIGRAVITY_API_URL", endpoint),
            ("KLAUS_ANTIGRAVITY_OAUTH_URL", &format!("{endpoint}/token")),
            ("KLAUS_ANTIGRAVITY_ALLOW_DIRECT", "1"),
            ("KLAUS_ANTIGRAVITY_HUB_IDENTITY", "9.9.9:123456"),
        ];
        Self(
            settings
                .into_iter()
                .map(|(key, value)| {
                    let previous = std::env::var_os(key);
                    set_process_environment_var(key, value);
                    (key, previous)
                })
                .collect(),
        )
    }
}

impl Drop for Environment {
    fn drop(&mut self) {
        for (key, previous) in self.0.drain(..) {
            if let Some(previous) = previous {
                set_process_environment_var(key, previous);
            } else {
                remove_process_environment_var(key);
            }
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
    assert!(rows
        .iter()
        .any(|row| row["id"] == "gemini-3.8-flash-medium"));
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
        server.requests_to(":fetchAvailableModels"),
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

    let notification = client
        .next_message_with_timeout(Duration::from_secs(2))
        .await;
    assert_eq!(notification["method"], "model/catalogChanged");
    assert_eq!(notification["params"], json!({"source": "antigravity"}));
    let snapshot = root.path().join("antigravity-models.json");
    assert!(
        std::fs::read_to_string(&snapshot)
            .ok()
            .and_then(|text| serde_json::from_str::<Value>(&text).ok())
            .is_some_and(|value| value["payload"]["models"]["live-model"].is_object()),
        "a stale snapshot must revalidate in the background before notifying"
    );
    let response = catalog_request(&mut client, 102).await;
    assert_eq!(response["result"]["catalogBaseline"], false);
    let rows = response["result"]["models"].as_array().unwrap();
    assert!(rows.iter().any(|row| row["id"] == "live-model"));
    assert!(!rows.iter().any(|row| row["id"] == "stale-only"));
    assert_eq!(server.requests_to(":fetchAvailableModels"), 1);

    // An unchanged revalidation keeps the client quiet instead of pulsing UI caches.
    antigravity::set_config_root(&root.path().join("other"));
    antigravity::set_config_root(root.path());
    let before = antigravity::refresh_completions();
    let response = catalog_request(&mut client, 103).await;
    assert!(response["result"]["models"]
        .as_array()
        .unwrap()
        .iter()
        .any(|row| row["id"] == "live-model"));
    assert!(
        wait_until(
            || antigravity::refresh_completions() > before,
            Duration::from_secs(2)
        )
        .await,
        "disk snapshot revalidation must settle"
    );
    assert_eq!(server.requests_to(":fetchAvailableModels"), 2);
    client
        .send(json!({"id":104,"method":"antigravity/models","params":{"read":"cached"}}))
        .await;
    let (_, seen) = client
        .response_for_with_timeout(104, Duration::from_secs(2))
        .await;
    assert!(
        !seen
            .iter()
            .any(|msg| msg["method"] == "model/catalogChanged"),
        "an identical catalog payload must not emit a change notification: {seen:?}"
    );

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
    let requests = server.requests_to(":fetchAvailableModels");
    let response = catalog_request(&mut client, 112).await;
    assert!(response["result"]["models"].is_array());
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        server.requests_to(":fetchAvailableModels"),
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
        wait_until(
            || server.requests_to(":fetchAvailableModels") == 1,
            Duration::from_secs(2)
        )
        .await,
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

#[tokio::test]
async fn cold_resolve_awaits_inflight_catalog_refresh_without_duplicate_fetch() {
    let root = tempfile::tempdir().unwrap();
    antigravity::set_config_root(root.path());
    let release = Arc::new(tokio::sync::Notify::new());
    let server = CloudFixture::start(Reply::DelayedCatalog(
        json!({
            "defaultAgentModelId": "gemini-4-carbon-medium",
            "models": {
                "gemini-4-carbon-medium": {
                    "displayName": "Gemini 4 Carbon (Medium)",
                    "supportsThinking": true,
                    "thinkingLevel": 2
                },
                "fixture-live-only-high": {
                    "displayName": "Fixture Live Only (High)",
                    "supportsThinking": true,
                    "thinkingLevel": 3,
                    "maxTokens": 1048576,
                    "maxOutputTokens": 65536
                }
            }
        }),
        Arc::clone(&release),
    ))
    .await;
    let _environment = Environment::set(&server.origin);
    antigravity::auth::adopt(credential()).await.unwrap();
    let mut client = start_client(root.path());
    client.initialize().await;

    let baseline = catalog_request(&mut client, 130).await;
    assert_eq!(baseline["result"]["catalogBaseline"], true, "{baseline}");
    assert!(
        wait_until(
            || server.requests_to(":fetchAvailableModels") == 1,
            Duration::from_secs(2)
        )
        .await,
        "local-first read must start one background refresh"
    );

    let mut tiers = serde_json::Map::new();
    tiers.insert("medium".into(), json!("antigravity:gemini-4-carbon-medium"));
    let resolving = tokio::spawn(async move {
        antigravity::resolve("antigravity:fixture-live-only-high", &tiers).await
    });
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(
        !resolving.is_finished(),
        "cold resolve for an unbundled model must await the in-flight catalog refresh"
    );
    release.notify_one();

    let resolved = tokio::time::timeout(Duration::from_secs(3), resolving)
        .await
        .expect("resolve timed out")
        .unwrap()
        .expect("resolve must succeed once the live catalog arrives");
    assert_eq!(resolved.provider["model"], "fixture-live-only-high");
    assert_eq!(resolved.capabilities["contextWindow"], 1_048_576);
    assert_eq!(
        resolved.model_tiers.get("medium"),
        Some(&json!("gemini-4-carbon-medium"))
    );
    assert_eq!(
        server.requests_to(":fetchAvailableModels"),
        1,
        "resolve must join the in-flight catalog refresh without issuing a duplicate fetch"
    );

    client.close().await;
    antigravity::auth::logout().await.unwrap();
}

#[tokio::test]
async fn switching_accounts_does_not_drop_or_block_new_account_catalog_behind_stalled_refresh() {
    let root = tempfile::tempdir().unwrap();
    antigravity::set_config_root(root.path());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let release_stalled = Arc::new(tokio::sync::Notify::new());
    let release_clone = Arc::clone(&release_stalled);
    let server_task = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            let release = Arc::clone(&release_clone);
            tokio::spawn(async move {
                let mut received = Vec::new();
                let mut buffer = [0u8; 4096];
                let header = loop {
                    let Ok(count) = socket.read(&mut buffer).await else {
                        return;
                    };
                    if count == 0 {
                        return;
                    }
                    received.extend_from_slice(&buffer[..count]);
                    if let Some(end) = received.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                        break String::from_utf8_lossy(&received[..end]).to_string();
                    }
                };
                let stalled = header
                    .lines()
                    .any(|line| line.eq_ignore_ascii_case("authorization: bearer tok-slow"));
                if stalled {
                    release.notified().await;
                }
                let body = json!({
                    "defaultAgentModelId": "fixture-live-only-high",
                    "models": {
                        "fixture-live-only-high": {
                            "displayName": "Fixture Live Only (High)",
                            "supportsThinking": true,
                            "thinkingLevel": 3
                        }
                    }
                })
                .to_string();
                let _ = socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await;
                let _ = socket.shutdown().await;
            });
        }
    });
    let _environment = Environment::set(&origin);

    antigravity::auth::adopt(
        serde_json::from_value(json!({
            "accessToken": "tok-fast",
            "account": "fast@example.invalid",
            "authMethod": "consumer",
            "project": "proj-fast"
        }))
        .unwrap(),
    )
    .await
    .unwrap();
    antigravity::auth::adopt(
        serde_json::from_value(json!({
            "accessToken": "tok-slow",
            "account": "slow@example.invalid",
            "authMethod": "consumer",
            "project": "proj-slow"
        }))
        .unwrap(),
    )
    .await
    .unwrap();
    let status = antigravity::auth::status();
    let accounts = status["accounts"].as_array().unwrap();
    let fast_id = accounts
        .iter()
        .find(|row| row["email"] == "fast@example.invalid")
        .and_then(|row| row["id"].as_str())
        .unwrap()
        .to_string();

    let mut client = start_client(root.path());
    client.initialize().await;

    // Start slow account's background catalog refresh and leave it held in flight.
    let _ = catalog_request(&mut client, 140).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Switch head to the fast account while tok-slow's refresh is still stalled.
    antigravity::auth::switch_account(&fast_id).await.unwrap();
    let resolved = antigravity::resolve("fixture-live-only-high", &serde_json::Map::new())
        .await
        .expect("new account resolve must succeed without waiting on outgoing account refresh");
    assert_eq!(resolved.provider["model"], "fixture-live-only-high");

    release_stalled.notify_waiters();
    server_task.abort();
    client.close().await;
    antigravity::auth::logout().await.unwrap();
}

#[tokio::test]
async fn cold_resolve_surfaces_catalog_fetch_failure_instead_of_claiming_model_unavailable() {
    let root = tempfile::tempdir().unwrap();
    antigravity::set_config_root(root.path());
    let server = CloudFixture::start(Reply::Status(500)).await;
    let _environment = Environment::set(&server.origin);
    antigravity::auth::adopt(credential()).await.unwrap();

    let err = antigravity::resolve("fixture-live-only-high", &serde_json::Map::new())
        .await
        .unwrap_err();
    assert_ne!(
        err, "antigravity_model_unavailable",
        "a failed catalog fetch on a cold account must not claim the model does not exist"
    );

    antigravity::auth::logout().await.unwrap();
}

#[tokio::test]
async fn expired_token_session_start_does_not_await_oauth_refresh_and_rotates_unrenewable_head() {
    let root = tempfile::tempdir().unwrap();
    antigravity::set_config_root(root.path());
    let server = CloudFixture::start(Reply::Hold).await;
    let _environment = Environment::set(&server.origin);
    let deadline = Duration::from_millis(500);
    let mut renewable = credential();
    renewable.account = Some("renewable@example.invalid".into());
    renewable.expires_at = Some(1_000);
    renewable.refresh_token = Some("ref-valid".into());
    renewable.client_id = Some("fixture-client".into());
    renewable.client_secret = Some("fixture-secret".into());
    antigravity::auth::adopt(renewable.clone()).await.unwrap();
    for model in ["gemini-4-carbon-high", "claude-opus-5-5-disabled"] {
        tokio::time::timeout(
            deadline,
            antigravity::resolve(model, &serde_json::Map::new()),
        )
        .await
        .unwrap()
        .unwrap();
    }
    let mut client = start_client(root.path());
    client.initialize().await;
    let response = tokio::time::timeout(deadline, async {
        client
            .send(json!({"id":701,"method":"session/start","params":{"init":{
                "cwd":root.path(),"modelSelection":"antigravity:gemini-4-carbon-high",
                "modelTierSelections":{"low":"unbundled-tier-id"},"config":{"prewarm":false}
            }}}))
            .await;
        client.response_for(701).await.0
    })
    .await
    .unwrap();
    assert!(response["result"]["sessionId"].is_string(), "{response}");
    client.close().await;
    antigravity::auth::logout().await.unwrap();

    let mut head = credential();
    head.account = Some("unrenewable@example.invalid".into());
    head.expires_at = Some(1_000);
    antigravity::auth::adopt(head).await.unwrap();
    let head_generation = antigravity::auth::generation().unwrap();
    renewable.refresh_token = Some("ref-b".into());
    antigravity::auth::adopt(renewable).await.unwrap();
    assert_eq!(
        antigravity::auth::generation().as_deref(),
        Some(head_generation.as_str())
    );
    assert_eq!(
        antigravity::auth::status()["accounts"][0]["email"],
        "unrenewable@example.invalid"
    );
    tokio::time::timeout(
        deadline,
        antigravity::resolve("gemini-4-carbon-high", &serde_json::Map::new()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_ne!(
        antigravity::auth::generation().as_deref(),
        Some(head_generation.as_str())
    );
    assert_eq!(
        antigravity::auth::status()["accounts"][0]["email"],
        "renewable@example.invalid"
    );
    antigravity::auth::logout().await.unwrap();
}
