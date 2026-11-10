//! Regression: usage read a stale login seed while inference kept refreshed credentials.
//! Root cause: the usage reader bypassed the inference credential namespace and lock.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use klaus_agent_core::credential_store::{CredentialStore, NamespacedCredentialStore};
use klaus_agent_core::mcp::credential_store::InMemoryCredentialStore;
use klaus_agent_core::provider::auth::AuthSource;
use klaus_agent_core::provider::oauth::{OAuthAuth, OAuthAuthOptions};
use klaus_agent_core::types::abort::AbortSignal;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

use super::super::{claude_usage::UsageOptions, model_sources, service};
use super::{make_create_session, TestClient};

#[derive(Default)]
struct Requests {
    usage: Vec<String>,
    refresh: Vec<String>,
    unexpected: usize,
    usage_gate: Option<Arc<tokio::sync::Barrier>>,
}

struct Fixture {
    root: tempfile::TempDir,
    store: Arc<dyn CredentialStore>,
    options: UsageOptions,
    token_url: String,
    requests: Arc<Mutex<Requests>>,
    server: tokio::task::JoinHandle<()>,
}

impl Fixture {
    async fn new(accounts: &[Value], usage_status: u16, reject_seed: bool) -> Self {
        let root = tempfile::tempdir().unwrap();
        if !accounts.is_empty() {
            std::fs::write(
                model_sources::claude_subscription_path(root.path()),
                serde_json::to_vec(&json!({"version": 1, "accounts": accounts})).unwrap(),
            )
            .unwrap();
        }
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let options = UsageOptions::fixture(
            format!("{base}/usage"),
            format!("{base}/token"),
            root.path().join("locks").to_string_lossy().into_owned(),
        );
        let requests = Arc::new(Mutex::new(Requests::default()));
        let trace = requests.clone();
        let redirect = format!("{base}/must-not-follow");
        let server = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let trace = trace.clone();
                let redirect = redirect.clone();
                tokio::spawn(async move {
                    let mut received = Vec::new();
                    let (end, length) = loop {
                        let mut chunk = [0; 4096];
                        let read = socket.read(&mut chunk).await.unwrap();
                        assert_ne!(read, 0);
                        received.extend_from_slice(&chunk[..read]);
                        assert!(received.len() < 64 * 1024);
                        if let Some(end) = received
                            .windows(4)
                            .position(|window| window == b"\r\n\r\n")
                            .map(|end| end + 4)
                        {
                            let headers = std::str::from_utf8(&received[..end]).unwrap();
                            let length = headers
                                .lines()
                                .find_map(|line| {
                                    let (name, value) = line.split_once(':')?;
                                    name.eq_ignore_ascii_case("content-length")
                                        .then(|| value.trim().parse::<usize>().unwrap())
                                })
                                .unwrap_or(0);
                            if received.len() >= end + length {
                                break (end, length);
                            }
                        }
                    };
                    let headers = std::str::from_utf8(&received[..end]).unwrap();
                    let body = std::str::from_utf8(&received[end..end + length]).unwrap();
                    let bearer = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("authorization")
                                .then(|| value.trim().to_string())
                        })
                        .unwrap_or_default();
                    let (status, response) = if headers.starts_with("POST /token ") {
                        trace.lock().unwrap().refresh.push(body.to_string());
                        tokio::time::sleep(std::time::Duration::from_millis(75)).await;
                        if body.contains("fixture-refresh-bad") {
                            (400, json!({"error": "fixture-secret-refresh-failure"}))
                        } else {
                            (
                                200,
                                json!({
                                    "access_token": "fixture-refreshed-access",
                                    "refresh_token": "fixture-rotated-refresh",
                                    "expires_in": 3600,
                                }),
                            )
                        }
                    } else if headers.starts_with("GET /usage ") {
                        trace.lock().unwrap().usage.push(bearer.clone());
                        let gate = trace.lock().unwrap().usage_gate.clone();
                        if let Some(gate) = gate {
                            gate.wait().await;
                        }
                        if bearer.contains("slow") {
                            tokio::time::sleep(std::time::Duration::from_millis(800)).await;
                        }
                        let rejected = bearer.contains("-bad")
                            || (reject_seed && !bearer.contains("fixture-refreshed-access"));
                        let status = if rejected { 401 } else { usage_status };
                        if usage_status == 206 {
                            (200, json!({"large": "x".repeat(1024 * 1024)}))
                        } else if usage_status == 205 {
                            (200, json!([]))
                        } else {
                            (
                                status,
                                json!({
                                    "five_hour": {"utilization": 27.5, "resets_at": "2026-10-15T00:00:00Z"},
                                    "seven_day": {"utilization": 42, "resets_at": null},
                                    "seven_day_opus": null,
                                    "accessToken": bearer,
                                    "error": "fixture-secret-upstream-error",
                                }),
                            )
                        }
                    } else {
                        trace.lock().unwrap().unexpected += 1;
                        (200, json!({}))
                    };
                    let response = response.to_string();
                    let reply = format!(
                        "HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nLocation: {redirect}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",
                        response.len()
                    );
                    socket.write_all(reply.as_bytes()).await.unwrap();
                });
            }
        });
        Self {
            root,
            store: Arc::new(InMemoryCredentialStore::new()),
            options,
            token_url: format!("{base}/token"),
            requests,
            server,
        }
    }

    async fn client(&self) -> TestClient {
        let (client_side, server_side) = tokio::io::duplex(1 << 20);
        let (server_read, server_write) = tokio::io::split(server_side);
        let (client_read, client_write) = tokio::io::split(client_side);
        let server = tokio::spawn(service::run_app_server_with_usage(
            server_read,
            server_write,
            service::AppServerOptions {
                windows_sandbox_context: None,
                create_session: make_create_session(&[]),
                credential_store: Some(self.store.clone()),
                lifecycle: None,
                plugin_admin: None,
                permission_rules_path: Some(self.root.path().join("rules.json")),
                config_path: Some(self.root.path().join("config.json")),
                startup_init_ack_budget: None,
                session_unload_delay: None,
            },
            self.options.clone(),
        ));
        let mut client = TestClient {
            writer: client_write,
            lines: BufReader::new(client_read).lines(),
            server,
            diagnostic: None,
        };
        client.initialize().await;
        client
    }

    async fn query(&self) -> Value {
        let mut client = self.client().await;
        client
            .send(json!({"id": 2, "method": "claude/usage", "params": {}}))
            .await;
        let (response, _) = client.response_for(2).await;
        assert!(response.get("error").is_none(), "{response}");
        let result = response["result"].clone();
        client.writer.shutdown().await.unwrap();
        client.close().await;
        result
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

fn account(id: &str, expired: bool) -> Value {
    json!({
        "id": id,
        "email": format!("Fixture {id}"),
        "tokens": {
            "accessToken": format!("fixture-access-{id}"),
            "refreshToken": format!("fixture-refresh-{id}"),
            "expiresAt": if expired { 0 } else { chrono::Utc::now().timestamp_millis() + 3_600_000 },
            "subscriptionType": "max",
        },
    })
}

fn assert_safe(result: &Value) {
    let wire = result.to_string();
    for secret in [
        "fixture-access",
        "fixture-refresh",
        "fixture-refreshed",
        "fixture-rotated",
        "fixture-secret",
        "accessToken",
        "refreshToken",
        "credentialGeneration",
    ] {
        assert!(
            !wire.contains(secret),
            "credential leaked into the facade: {wire}"
        );
    }
}

#[tokio::test]
async fn claude_usage_facade_signed_out_has_no_accounts_or_network() {
    let fixture = Fixture::new(&[], 200, false).await;
    assert_eq!(
        fixture.query().await,
        json!({"signedIn": false, "accounts": []})
    );
    let trace = fixture.requests.lock().unwrap();
    assert!(trace.usage.is_empty());
    assert!(trace.refresh.is_empty());
}

#[tokio::test]
async fn claude_usage_facade_refreshes_expired_credentials_and_projects_only_usage() {
    let fixture = Fixture::new(&[account("a", true)], 200, false).await;
    let result = fixture.query().await;
    assert_eq!(
        result,
        json!({
            "signedIn": true,
            "accounts": [{
                "id": "a", "label": "Fixture a", "isCurrent": true, "plan": "max", "state": "ok",
                "windows": [
                    {"id": "five_hour", "usedPercent": 27.5, "resetsAt": 1792022400000_i64},
                    {"id": "seven_day", "usedPercent": 42.0},
                ],
            }],
        })
    );
    assert_safe(&result);
    let trace = fixture.requests.lock().unwrap();
    assert_eq!(trace.refresh.len(), 1);
    assert_eq!(trace.usage, ["Bearer fixture-refreshed-access"]);
}

#[tokio::test]
async fn claude_usage_facade_refreshes_401_once_and_reuses_the_refreshed_store() {
    let fixture = Fixture::new(&[account("a", false)], 200, true).await;
    let first = fixture.query().await;
    let second = fixture.query().await;
    assert_eq!(first["accounts"][0]["state"], "ok");
    assert_eq!(second["accounts"][0]["state"], "ok");
    assert_safe(&first);
    let trace = fixture.requests.lock().unwrap();
    assert_eq!(trace.refresh.len(), 1);
    assert_eq!(
        trace.usage,
        [
            "Bearer fixture-access-a",
            "Bearer fixture-refreshed-access",
            "Bearer fixture-refreshed-access",
        ]
    );
}

#[tokio::test]
async fn claude_usage_facade_final_401_is_an_account_error_without_secret_echo() {
    let fixture = Fixture::new(&[account("a", false)], 401, false).await;
    let result = fixture.query().await;
    assert_eq!(result["signedIn"], true);
    assert_eq!(result["accounts"][0]["state"], "error");
    assert_eq!(
        result["accounts"][0]["error"],
        "Usage request failed (HTTP 401)"
    );
    assert!(result["accounts"][0].get("windows").is_none());
    assert_safe(&result);
    let trace = fixture.requests.lock().unwrap();
    assert_eq!(trace.refresh.len(), 1);
    assert_eq!(trace.usage.len(), 2);
}

#[tokio::test]
async fn claude_usage_facade_isolates_bad_accounts_and_never_reorders_the_queue() {
    let fixture = Fixture::new(&[account("bad", false), account("good", false)], 200, false).await;
    let path = model_sources::claude_subscription_path(fixture.root.path());
    let before = std::fs::read(&path).unwrap();
    let result = fixture.query().await;
    assert_eq!(result["accounts"][0]["id"], "bad");
    assert_eq!(result["accounts"][0]["isCurrent"], true);
    assert_eq!(result["accounts"][0]["state"], "error");
    assert_eq!(result["accounts"][1]["id"], "good");
    assert_eq!(result["accounts"][1]["isCurrent"], false);
    assert_eq!(result["accounts"][1]["state"], "ok");
    assert_eq!(std::fs::read(path).unwrap(), before);
    assert_safe(&result);
}

#[tokio::test]
async fn claude_usage_facade_concurrent_reads_refresh_once_for_expiry_and_401() {
    for expired in [true, false] {
        let fixture = Fixture::new(&[account("a", expired)], 200, !expired).await;
        let mut client = fixture.client().await;
        for id in [2, 3] {
            client
                .send(json!({"id": id, "method": "claude/usage", "params": {}}))
                .await;
        }
        let mut replies = HashMap::new();
        while replies.len() < 2 {
            let response = client.next_message().await;
            if let Some(id @ (2 | 3)) = response["id"].as_u64() {
                replies.insert(id, response);
            }
        }
        for response in replies.values() {
            assert_eq!(
                response["result"]["accounts"][0]["state"], "ok",
                "{response}"
            );
            assert_safe(&response["result"]);
        }
        client.writer.shutdown().await.unwrap();
        client.close().await;
        assert_eq!(fixture.requests.lock().unwrap().refresh.len(), 1);
    }
}

#[tokio::test]
async fn claude_usage_facade_relogin_uses_a_new_namespace_and_seed() {
    let old = account("a", false);
    let fixture = Fixture::new(std::slice::from_ref(&old), 200, false).await;
    let result = fixture.query().await;
    assert_eq!(result["accounts"][0]["state"], "ok");
    let old_id =
        model_sources::account_credential_id("subscription", old["tokens"].as_object().unwrap());
    let mut replacement = account("a", false);
    replacement["tokens"]["accessToken"] = json!("fixture-access-new-login");
    replacement["tokens"]["refreshToken"] = json!("fixture-refresh-new-login");
    let new_id = model_sources::account_credential_id(
        "subscription",
        replacement["tokens"].as_object().unwrap(),
    );
    assert_ne!(old_id, new_id);
    std::fs::write(
        model_sources::claude_subscription_path(fixture.root.path()),
        json!({"accounts": [replacement]}).to_string(),
    )
    .unwrap();
    assert_eq!(fixture.query().await["accounts"][0]["state"], "ok");
    assert_eq!(
        fixture.requests.lock().unwrap().usage,
        ["Bearer fixture-access-a", "Bearer fixture-access-new-login",]
    );
    for id in [old_id, new_id] {
        let namespace: Arc<dyn CredentialStore> = Arc::new(NamespacedCredentialStore::new(
            fixture.store.clone(),
            format!("provider/anthropic/{id}"),
        ));
        assert!(
            OAuthAuth::capture_stored(&namespace, &model_sources::claude_oauth_config())
                .await
                .unwrap()
                .is_some()
        );
    }
}

#[tokio::test]
async fn claude_usage_facade_does_not_refresh_other_statuses_or_follow_redirects() {
    for status in [403, 429, 500, 302] {
        let fixture = Fixture::new(&[account("a", false)], status, false).await;
        let result = fixture.query().await;
        assert_eq!(result["accounts"][0]["state"], "error");
        assert_eq!(
            result["accounts"][0]["error"],
            format!("Usage request failed (HTTP {status})")
        );
        assert_safe(&result);
        let trace = fixture.requests.lock().unwrap();
        assert_eq!(trace.usage.len(), 1);
        assert!(trace.refresh.is_empty());
        assert_eq!(trace.unexpected, 0);
    }
}

#[tokio::test]
async fn claude_usage_facade_and_inference_share_refresh_lock_and_lineage() {
    let entry = account("a", true);
    let fixture = Fixture::new(std::slice::from_ref(&entry), 200, false).await;
    let id =
        model_sources::account_credential_id("subscription", entry["tokens"].as_object().unwrap());
    let namespace: Arc<dyn CredentialStore> = Arc::new(NamespacedCredentialStore::new(
        fixture.store.clone(),
        format!("provider/anthropic/{id}"),
    ));
    // Seed through usage, then expire the stored credential through the public OAuth API.
    assert_eq!(fixture.query().await["accounts"][0]["state"], "ok");
    let snapshot = OAuthAuth::capture_stored(&namespace, &model_sources::claude_oauth_config())
        .await
        .unwrap()
        .unwrap();
    let mut tokens = snapshot.tokens();
    tokens.expires_at = Some(0);
    let inference = OAuthAuth::new(OAuthAuthOptions {
        config: klaus_agent_core::provider::oauth::OAuthConfig {
            token_url: fixture.token_url.clone(),
            ..model_sources::claude_oauth_config()
        },
        store: Some(namespace),
        prepared_credential: Some(snapshot),
        refresh_lock_key: Some(format!("provider-anthropic-{id}")),
        refresh_lock_dir: Some(
            fixture
                .root
                .path()
                .join("locks")
                .to_string_lossy()
                .into_owned(),
        ),
        ..Default::default()
    });
    inference.set_tokens(tokens).await.unwrap();
    let signal = AbortSignal::new();
    let (headers, result) = tokio::join!(inference.get_headers(&signal), fixture.query());
    assert_eq!(
        headers.unwrap()["Authorization"],
        "Bearer fixture-refreshed-access"
    );
    assert_eq!(result["accounts"][0]["state"], "ok");
    assert_eq!(fixture.requests.lock().unwrap().refresh.len(), 2);
}

#[tokio::test]
async fn claude_usage_facade_bounded_accounts_settle_independently_and_keep_order() {
    let mut fixture = Fixture::new(
        &[
            account("slow-a", false),
            account("slow-b", false),
            account("good", false),
        ],
        200,
        false,
    )
    .await;
    fixture.options = fixture
        .options
        .clone()
        .with_budget(std::time::Duration::from_millis(200));
    let result = fixture.query().await;
    assert_eq!(result["accounts"][0]["id"], "slow-a");
    assert_eq!(result["accounts"][1]["id"], "slow-b");
    assert_eq!(
        result["accounts"][0]["error"],
        "Usage request failed (timeout)"
    );
    assert_eq!(
        result["accounts"][1]["error"],
        "Usage request failed (timeout)"
    );
    assert_eq!(result["accounts"][2]["id"], "good");
    assert_eq!(result["accounts"][2]["state"], "ok");
}

#[tokio::test]
async fn claude_usage_facade_queries_accounts_concurrently_before_any_can_settle() {
    let mut fixture = Fixture::new(
        &[
            account("a", false),
            account("b", false),
            account("c", false),
        ],
        200,
        false,
    )
    .await;
    fixture.options = fixture
        .options
        .clone()
        .with_budget(std::time::Duration::from_millis(500));
    fixture.requests.lock().unwrap().usage_gate = Some(Arc::new(tokio::sync::Barrier::new(3)));
    let result = fixture.query().await;
    for (index, id) in ["a", "b", "c"].into_iter().enumerate() {
        assert_eq!(result["accounts"][index]["id"], id);
        assert_eq!(result["accounts"][index]["state"], "ok", "{result}");
    }
}

#[tokio::test]
async fn claude_usage_facade_labels_follow_email_display_name_and_safe_fallback() {
    let email = account("a", false);
    let mut display = account("b", false);
    display.as_object_mut().unwrap().remove("email");
    display["displayName"] = json!("Display B");
    let mut fallback = account("c", false);
    fallback.as_object_mut().unwrap().remove("email");
    let result = Fixture::new(&[email, display, fallback], 200, false)
        .await
        .query()
        .await;
    assert_eq!(result["accounts"][0]["label"], "Fixture a");
    assert_eq!(result["accounts"][1]["label"], "Display B");
    assert_eq!(result["accounts"][2]["label"], "Claude");
}

#[tokio::test]
async fn claude_usage_facade_bounds_response_size_and_rejects_invalid_shapes() {
    for (status, error) in [
        (206, "Usage request failed"),
        (205, "Invalid usage response"),
    ] {
        let fixture = Fixture::new(&[account("a", false)], status, false).await;
        let result = fixture.query().await;
        assert_eq!(result["accounts"][0]["error"], error);
        assert_safe(&result);
        assert!(fixture.requests.lock().unwrap().refresh.is_empty());
    }
}

#[tokio::test]
async fn claude_usage_facade_expired_without_refresh_requires_renewal_without_network() {
    let mut entry = account("a", true);
    entry["tokens"]
        .as_object_mut()
        .unwrap()
        .remove("refreshToken");
    let fixture = Fixture::new(&[entry], 200, false).await;
    let result = fixture.query().await;
    assert_eq!(
        result["accounts"][0]["error"],
        "Claude login needs renewal; sign in again"
    );
    let trace = fixture.requests.lock().unwrap();
    assert!(trace.usage.is_empty());
    assert!(trace.refresh.is_empty());
}

#[tokio::test]
async fn claude_usage_facade_deadline_allows_expired_and_forced_refresh_to_settle() {
    for expired in [true, false] {
        let entry = account("a", expired);
        let mut fixture = Fixture::new(std::slice::from_ref(&entry), 200, !expired).await;
        fixture.options = fixture
            .options
            .clone()
            .with_budget(std::time::Duration::from_millis(40));
        let result = fixture.query().await;
        assert_eq!(
            result["accounts"][0]["error"],
            "Usage request failed (timeout)"
        );
        tokio::time::sleep(std::time::Duration::from_millis(120)).await;
        fixture.options = fixture
            .options
            .clone()
            .with_budget(std::time::Duration::from_secs(2));
        assert_eq!(fixture.query().await["accounts"][0]["state"], "ok");
        assert_eq!(fixture.requests.lock().unwrap().refresh.len(), 1);
        assert_eq!(
            std::fs::read_dir(fixture.root.path().join("locks"))
                .unwrap()
                .count(),
            0
        );
        let id = model_sources::account_credential_id(
            "subscription",
            entry["tokens"].as_object().unwrap(),
        );
        let namespace: Arc<dyn CredentialStore> = Arc::new(NamespacedCredentialStore::new(
            fixture.store.clone(),
            format!("provider/anthropic/{id}"),
        ));
        let snapshot = OAuthAuth::capture_stored(&namespace, &model_sources::claude_oauth_config())
            .await
            .unwrap()
            .unwrap();
        let inference = OAuthAuth::new(OAuthAuthOptions {
            config: model_sources::claude_oauth_config(),
            store: Some(namespace),
            prepared_credential: Some(snapshot),
            ..Default::default()
        });
        assert_eq!(
            inference.get_headers(&AbortSignal::new()).await.unwrap()["Authorization"],
            "Bearer fixture-refreshed-access"
        );
        assert_eq!(fixture.requests.lock().unwrap().refresh.len(), 1);
    }
}
