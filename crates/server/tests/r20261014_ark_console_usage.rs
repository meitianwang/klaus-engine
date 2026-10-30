//! Defect: a successful console login disappeared on its first allowance read.
//! Root cause: OAuth STS credentials were sent as an opaque browser session.
//! Work item: 20261014 Ark console allowance authentication.

use super::{make_create_session, TestClient};
use crate::app_server::ark::console_usage::{Endpoints, TEST_ENDPOINTS};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Fixture {
    root: tempfile::TempDir,
    requests: Arc<Mutex<Vec<String>>>,
    mode: Arc<Mutex<&'static str>>,
    server: tokio::task::JoinHandle<()>,
    refresh_started: Arc<tokio::sync::Notify>,
    refresh_release: Arc<tokio::sync::Notify>,
}

fn token_answer(refresh: bool) -> Value {
    let mut answer = json!({
        "access_token": json!({
            "access_key_id": if refresh { "fixture-refreshed-ak" } else { "fixture-ak" },
            "secret_access_key": "fixture-secret",
            "session_token": "fixture-security-token",
        }).to_string(),
        "token_type": "urn:ietf:params:oauth:token-type:access_token_sts",
        "expires_in": 900,
        "refresh_token": "fixture-refresh",
    });
    if refresh {
        answer.as_object_mut().unwrap().remove("refresh_token");
    }
    answer
}

impl Fixture {
    async fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join("ark-codingplan.json"),
            r#"{"apiKey":"fixture-plan-key"}"#,
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        *TEST_ENDPOINTS.write().unwrap() = Some(Endpoints {
            token: format!("{base}/token"),
            usage: format!("{base}/?Action=GetCodingPlanUsage&Version=2024-01-01"),
        });
        let requests = Arc::new(Mutex::new(Vec::new()));
        let mode = Arc::new(Mutex::new("ok"));
        let refresh_started = Arc::new(tokio::sync::Notify::new());
        let refresh_release = Arc::new(tokio::sync::Notify::new());
        let started = refresh_started.clone();
        let release = refresh_release.clone();
        let trace = requests.clone();
        let behavior = mode.clone();
        let server = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let trace = trace.clone();
                let started = started.clone();
                let release = release.clone();
                let behavior = behavior.clone();
                tokio::spawn(async move {
                    let mut raw = Vec::new();
                    loop {
                        let mut chunk = [0; 4096];
                        let read = socket.read(&mut chunk).await.unwrap();
                        assert_ne!(read, 0);
                        raw.extend_from_slice(&chunk[..read]);
                        assert!(raw.len() < 64 * 1024);
                        if let Some(end) = raw.windows(4).position(|value| value == b"\r\n\r\n") {
                            let head = String::from_utf8_lossy(&raw[..end]);
                            let length = head
                                .lines()
                                .find_map(|line| {
                                    let (key, value) = line.split_once(':')?;
                                    key.eq_ignore_ascii_case("content-length")
                                        .then(|| value.trim().parse::<usize>().unwrap())
                                })
                                .unwrap_or(0);
                            if raw.len() >= end + 4 + length {
                                break;
                            }
                        }
                    }
                    let request = String::from_utf8(raw).unwrap();
                    let mode = *behavior.lock().unwrap();
                    let refreshing = request.starts_with("POST /token ")
                        && request.contains("grant_type=refresh_token");
                    let (status, answer) = if request.starts_with("POST /token ") {
                        if refreshing && matches!(mode, "invalid_grant" | "slow_invalid_grant") {
                            (
                                400,
                                json!({"error":"invalid_grant", "secret":"must-not-leak"}),
                            )
                        } else {
                            (200, token_answer(refreshing))
                        }
                    } else if mode == "permission" {
                        (
                            403,
                            json!({"ResponseMetadata":{"Error":{"Code":"AccessDenied","Message":"must-not-leak"}}}),
                        )
                    } else if mode == "expired" && !request.contains("fixture-refreshed-ak/") {
                        (
                            403,
                            json!({"ResponseMetadata":{"Error":{"Code":"ExpiredToken"}}}),
                        )
                    } else {
                        (
                            200,
                            json!({"Result":{"QuotaUsage":[
                                {"Level":"monthly","Percent":0.03,"ResetTimestamp":0},
                                {"Level":"session","Percent":0.18,"ResetTimestamp":1792022400i64},
                                {"Level":"weekly","Percent":12.5,"ResetTimestamp":1792108800i64},
                            ]}}),
                        )
                    };
                    trace.lock().unwrap().push(request);
                    if refreshing && mode == "slow_invalid_grant" {
                        started.notify_one();
                        release.notified().await;
                    }
                    let body = answer.to_string();
                    socket.write_all(format!("HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
                });
            }
        });
        Self {
            root,
            requests,
            mode,
            server,
            refresh_started,
            refresh_release,
        }
    }

    async fn client(&self) -> TestClient {
        let mut client = TestClient::start_full(
            make_create_session(&[]),
            None,
            Some(self.root.path().join("rules.json")),
            Some(self.root.path().join("config.json")),
        );
        client.initialize().await;
        client
    }

    async fn call(&self, client: &mut TestClient, method: &str) -> Value {
        client
            .send(json!({"id":50,"method":method,"params":{}}))
            .await;
        client.response_for(50).await.0
    }

    async fn begin_login(&self, client: &mut TestClient) -> reqwest::Url {
        let answer = self.call(client, "ark/consoleLogin").await;
        let url = reqwest::Url::parse(answer["result"]["url"].as_str().unwrap()).unwrap();
        let params: std::collections::HashMap<_, _> = url.query_pairs().collect();
        let mut callback = reqwest::Url::parse(&params["redirect_uri"]).unwrap();
        callback
            .query_pairs_mut()
            .append_pair("state", &params["state"])
            .append_pair("code", "fixture-code");
        callback
    }

    async fn login(&self, client: &mut TestClient) {
        let callback = self.begin_login(client).await;
        let response = reqwest::get(callback).await.unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(
            self.call(client, "ark/consoleStatus").await["result"],
            json!({"loggedIn":true})
        );
    }

    fn expire(&self) {
        let path = self.root.path().join("ark-codingplan.json");
        let mut store: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        store["consoleCredentials"]["expires_at"] = json!(1);
        std::fs::write(path, store.to_string()).unwrap();
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
        *TEST_ENDPOINTS.write().unwrap() = None;
    }
}

#[tokio::test]
async fn ark_console_login_usage_and_reentry_keep_the_connection() {
    let _lock = crate::app_server::ark::test_env_lock().lock().await;
    let fixture = Fixture::new().await;
    let mut client = fixture.client().await;
    fixture.login(&mut client).await;
    let answer = fixture.call(&mut client, "ark/usage").await;
    assert!(answer.get("error").is_none(), "{answer}");
    assert_eq!(
        answer["result"]["windows"],
        json!([
            {"id":"five_hour","usedPercent":0.18,"resetsAt":1792022400000i64},
            {"id":"weekly","usedPercent":12.5,"resetsAt":1792108800000i64},
            {"id":"monthly","usedPercent":0.03},
        ])
    );
    assert_eq!(
        fixture.call(&mut client, "ark/consoleStatus").await["result"]["loggedIn"],
        true
    );
    assert_eq!(
        fixture.call(&mut client, "ark/status").await["result"]["loggedIn"],
        true
    );
    let requests = fixture.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].contains("scope=Console%3AAll%3AAll"));
    let usage = requests[1].to_lowercase();
    assert!(usage.starts_with("post /?action=getcodingplanusage&version=2024-01-01 "));
    assert!(usage.contains("authorization: hmac-sha256 credential=fixture-ak/"));
    assert!(usage.contains("/cn-beijing/ark/request"));
    assert!(usage.contains("x-security-token: fixture-security-token\r\n"));
    assert!(
        usage.contains("signedheaders=content-type;host;x-content-sha256;x-date;x-security-token")
    );
    assert!(!answer.to_string().contains("fixture-secret"));
    assert!(!answer.to_string().contains("fixture-security-token"));
    client.close().await;
}

#[tokio::test]
async fn ark_console_refreshes_expired_sts_once_and_preserves_the_plan_key() {
    let _lock = crate::app_server::ark::test_env_lock().lock().await;
    let fixture = Fixture::new().await;
    let mut client = fixture.client().await;
    fixture.login(&mut client).await;
    *fixture.mode.lock().unwrap() = "expired";
    let answer = fixture.call(&mut client, "ark/usage").await;
    assert_eq!(answer["result"]["quotaAvailable"], true, "{answer}");
    let requests = fixture.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 4);
    assert!(requests[2].contains("grant_type=refresh_token"));
    assert!(requests[2].contains("client_id=trn%3Asignin%3A%3A%3Adevtools%2Fsame-device"));
    assert_eq!(
        fixture.call(&mut client, "ark/consoleStatus").await["result"]["loggedIn"],
        true
    );
    client.close().await;
}

#[tokio::test]
async fn ark_console_permission_failure_preserves_login_and_invalid_grant_only_clears_console() {
    let _lock = crate::app_server::ark::test_env_lock().lock().await;
    let fixture = Fixture::new().await;
    let mut client = fixture.client().await;
    fixture.login(&mut client).await;
    *fixture.mode.lock().unwrap() = "permission";
    let error = fixture.call(&mut client, "ark/usage").await;
    assert_eq!(
        error["error"]["message"],
        "Ark console allowance request failed (HTTP 403)"
    );
    assert!(!error.to_string().contains("must-not-leak"));
    assert_eq!(
        fixture.call(&mut client, "ark/consoleStatus").await["result"]["loggedIn"],
        true
    );
    fixture.expire();
    *fixture.mode.lock().unwrap() = "invalid_grant";
    let error = fixture.call(&mut client, "ark/usage").await;
    assert_eq!(
        error["error"]["message"],
        "Ark console login needs renewal; sign in again"
    );
    assert_eq!(
        fixture.call(&mut client, "ark/consoleStatus").await["result"]["loggedIn"],
        false
    );
    assert_eq!(
        fixture.call(&mut client, "ark/status").await["result"]["loggedIn"],
        true
    );
    client.close().await;
}

#[tokio::test]
async fn ark_console_pending_callback_cannot_undo_a_plan_logout() {
    let _lock = crate::app_server::ark::test_env_lock().lock().await;
    let fixture = Fixture::new().await;
    let mut client = fixture.client().await;
    let callback = fixture.begin_login(&mut client).await;
    fixture.call(&mut client, "ark/logout").await;
    assert_eq!(reqwest::get(callback).await.unwrap().status(), 500);
    assert_eq!(
        fixture.call(&mut client, "ark/consoleStatus").await["result"]["loggedIn"],
        false
    );
    assert!(!fixture.root.path().join("ark-codingplan.json").exists());
    client.close().await;
}

#[tokio::test]
async fn ark_console_stale_refresh_cannot_clear_a_new_login() {
    let _lock = crate::app_server::ark::test_env_lock().lock().await;
    let fixture = Fixture::new().await;
    let mut first = fixture.client().await;
    fixture.login(&mut first).await;
    fixture.expire();
    *fixture.mode.lock().unwrap() = "slow_invalid_grant";
    first
        .send(json!({"id":50,"method":"ark/usage","params":{}}))
        .await;
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        fixture.refresh_started.notified(),
    )
    .await
    .unwrap();
    let mut second = fixture.client().await;
    fixture.login(&mut second).await;
    fixture.refresh_release.notify_one();
    let answer = first.response_for(50).await.0;
    assert_eq!(
        answer["error"]["message"],
        "Ark console login changed; refresh the allowance again"
    );
    assert_eq!(
        fixture.call(&mut second, "ark/consoleStatus").await["result"]["loggedIn"],
        true
    );
    *fixture.mode.lock().unwrap() = "ok";
    assert_eq!(
        fixture.call(&mut second, "ark/usage").await["result"]["quotaAvailable"],
        true
    );
    first.close().await;
    second.close().await;
}
