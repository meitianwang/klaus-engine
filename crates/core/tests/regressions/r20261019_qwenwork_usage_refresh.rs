//! Defect: expired imported credentials skipped renewal and appeared as network failures.
//! Root cause: millisecond expiry was compared as seconds and auth rejection was not retried.
//! Work item: 20261019 QwenWork allowance credential renewal.

use super::{make_create_session, TestClient};
use crate::app_server::qwenwork::{self, auth::TEST_OPENAPI_BASE};
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

fn future_expiry() -> i64 {
    chrono::Utc::now().timestamp() + 7200
}

impl Fixture {
    async fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        *TEST_OPENAPI_BASE.write().unwrap() =
            Some(format!("http://{}", listener.local_addr().unwrap()));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let mode = Arc::new(Mutex::new("ok"));
        let refresh_started = Arc::new(tokio::sync::Notify::new());
        let refresh_release = Arc::new(tokio::sync::Notify::new());
        let (trace, behavior, started, release) = (
            requests.clone(),
            mode.clone(),
            refresh_started.clone(),
            refresh_release.clone(),
        );
        let server = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let (trace, behavior, started, release) = (
                    trace.clone(),
                    behavior.clone(),
                    started.clone(),
                    release.clone(),
                );
                connections.spawn(async move {
                    let mut raw = Vec::new();
                    loop {
                        let mut chunk = [0; 4096];
                        let read = socket.read(&mut chunk).await.unwrap();
                        assert_ne!(read, 0);
                        raw.extend_from_slice(&chunk[..read]);
                        assert!(raw.len() < 64 * 1024);
                        if let Some(end) = raw.windows(4).position(|value| value == b"\r\n\r\n") {
                            let head = String::from_utf8_lossy(&raw[..end]);
                            let length = head.lines().find_map(|line| {
                                let (key, value) = line.split_once(':')?;
                                key.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().unwrap())
                            }).unwrap_or(0);
                            if raw.len() >= end + 4 + length { break; }
                        }
                    }
                    let request = String::from_utf8(raw).unwrap();
                    let mode = *behavior.lock().unwrap();
                    let refresh = request.starts_with("POST /api/v1/deviceToken/refresh?");
                    let (status, body) = if refresh {
                        match mode {
                            "refresh_rejected" => (401, json!({"errorCode":"INVALID_TOKEN","secret":"must-not-leak"}).to_string()),
                            "refresh_service" => (503, "must-not-leak".into()),
                            "refresh_malformed" => (200, "must-not-leak".into()),
                            "refresh_no_token" => (200, json!({"data":{"secret":"must-not-leak"}}).to_string()),
                            _ => (200, json!({"data":{"device_token":"fixture-new-access","refresh_token":"fixture-new-refresh","expires_at":future_expiry()*1000}}).to_string()),
                        }
                    } else if mode == "permission" {
                        (403, json!({"errorCode":"ACCESS_DENIED","secret":"must-not-leak"}).to_string())
                    } else if mode == "always_rejected" || (matches!(mode,"rejected"|"rejected_105") && !request.contains("Bearer fixture-new-access")) {
                        if mode == "rejected_105" {
                            (403, json!({"code":105,"secret":"must-not-leak"}).to_string())
                        } else {
                            (401, json!({"errorCode":"INVALID_TOKEN","secret":"must-not-leak"}).to_string())
                        }
                    } else if request.starts_with("GET /api/v2/quota/usage ") {
                        (200, json!({"data":{"user_quota":{"total":100,"used":25,"remaining":75}}}).to_string())
                    } else {
                        assert!(request.starts_with("GET /api/v1/adapter/user/account-context?"));
                        (200, json!({"data":{"quota":{"user_quota":{"total":100,"used":25,"remaining":75}}}}).to_string())
                    };
                    trace.lock().unwrap().push(request);
                    if refresh && mode == "slow" {
                        started.notify_one();
                        release.notified().await;
                    }
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

    fn seed(&self, expiry: Value, business: bool) {
        std::fs::write(self.root.path().join("qwenwork-subscription-oauth.json"), json!({
            "machineId":"fixture-device",
            "userInfo":{"uid":"fixture-user","access_token":"fixture-old-access","security_oauth_token":"fixture-old-access","refresh_token":"fixture-refresh","expire_time":expiry,"is_biz":business,"encrypt_user_info":"fixture-encrypted","key":"fixture-key"},
        }).to_string()).unwrap();
        qwenwork::catalog::forget_context();
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

    fn refreshes(&self) -> usize {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request.starts_with("POST "))
            .count()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
        *TEST_OPENAPI_BASE.write().unwrap() = None;
    }
}

#[tokio::test]
async fn qwenwork_usage_normalizes_imported_and_saved_expiry_units() {
    let _lock = qwenwork::test_env_lock().lock().await;
    let fixture = Fixture::new().await;
    let mut client = fixture.client().await;
    let seconds = future_expiry();
    for expiry in [
        json!(seconds),
        json!(seconds * 1000),
        json!((seconds * 1000).to_string()),
    ] {
        fixture.seed(expiry.clone(), false);
        assert_eq!(
            fixture.call(&mut client, "qwenwork/status").await["result"]["expiresAt"],
            seconds
        );
        client.send(json!({"id":51,"method":"qwenwork/adoptCredential","params":{"machineId":"fixture-device","userInfo":{"uid":"fixture-user","access_token":"fixture-old-access","security_oauth_token":"fixture-old-access","expire_time":expiry}}})).await;
        assert_eq!(client.response_for(51).await.0["result"]["ok"], true);
        assert_eq!(
            fixture.call(&mut client, "qwenwork/status").await["result"]["expiresAt"],
            seconds
        );
        let stored: Value = serde_json::from_slice(
            &std::fs::read(fixture.root.path().join("qwenwork-subscription-oauth.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(stored["userInfo"]["expire_time"], seconds);
    }
    assert!(fixture.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn qwenwork_usage_refreshes_legacy_milliseconds_before_personal_and_business_reads() {
    let _lock = qwenwork::test_env_lock().lock().await;
    let fixture = Fixture::new().await;
    let mut client = fixture.client().await;
    for business in [false, true] {
        fixture.requests.lock().unwrap().clear();
        fixture.seed(json!(1776345575000_i64), business);
        let answer = fixture.call(&mut client, "qwenwork/usage").await;
        assert_eq!(
            answer["result"]["quota"]["userQuota"]["used"].as_f64(),
            Some(25.0),
            "{answer}"
        );
        assert_eq!(fixture.refreshes(), 1);
        let requests = fixture.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests[1].contains("Bearer fixture-new-access"));
        drop(requests);
        let stored: Value = serde_json::from_slice(
            &std::fs::read(fixture.root.path().join("qwenwork-subscription-oauth.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(stored["userInfo"]["refresh_token"], "fixture-new-refresh");
        assert!(stored["userInfo"]["expire_time"].as_i64().unwrap() < 100_000_000_000);
    }
}

#[tokio::test]
async fn qwenwork_usage_retries_auth_rejections_once_without_retrying_permission_errors() {
    let _lock = qwenwork::test_env_lock().lock().await;
    let fixture = Fixture::new().await;
    let mut client = fixture.client().await;
    for mode in ["rejected", "rejected_105", "always_rejected", "permission"] {
        fixture.requests.lock().unwrap().clear();
        fixture.seed(json!(future_expiry()), false);
        *fixture.mode.lock().unwrap() = mode;
        let answer = fixture.call(&mut client, "qwenwork/usage").await;
        match mode {
            "always_rejected" => assert_eq!(answer["error"]["message"], "qwenwork_login_expired"),
            "permission" => assert_eq!(
                answer["error"]["message"],
                "QwenWork account-context request failed (HTTP 403)"
            ),
            _ => assert_eq!(answer["result"]["signedIn"], true, "{answer}"),
        }
        assert_eq!(fixture.refreshes(), usize::from(mode != "permission"));
        assert_eq!(
            fixture.requests.lock().unwrap().len(),
            if mode == "permission" { 1 } else { 3 }
        );
        assert!(!answer.to_string().contains("must-not-leak"));
    }
}

#[tokio::test]
async fn qwenwork_usage_failed_renewal_requests_login_without_leaking_upstream_bodies() {
    let _lock = qwenwork::test_env_lock().lock().await;
    let fixture = Fixture::new().await;
    let mut client = fixture.client().await;
    for mode in [
        "refresh_rejected",
        "refresh_service",
        "refresh_malformed",
        "refresh_no_token",
    ] {
        fixture.seed(json!(1776345575000_i64), false);
        *fixture.mode.lock().unwrap() = mode;
        let answer = fixture.call(&mut client, "qwenwork/usage").await;
        assert_eq!(
            answer["error"]["message"], "qwenwork_refresh_failed",
            "{answer}"
        );
        assert!(!answer.to_string().contains("must-not-leak"));
    }
    fixture.requests.lock().unwrap().clear();
    for strategy in ["missing_refresh", "pat"] {
        fixture.seed(json!(1776345575000_i64), false);
        let path = fixture.root.path().join("qwenwork-subscription-oauth.json");
        let mut stored: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        if strategy == "pat" {
            stored["userInfo"]["refresh_strategy"] = json!("pat");
        } else {
            stored["userInfo"]
                .as_object_mut()
                .unwrap()
                .remove("refresh_token");
        }
        std::fs::write(path, stored.to_string()).unwrap();
        let answer = fixture.call(&mut client, "qwenwork/usage").await;
        assert_eq!(
            answer["error"]["message"], "qwenwork_refresh_failed",
            "{answer}"
        );
    }
    assert!(fixture.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn qwenwork_usage_concurrent_reads_share_refresh_and_logout_cannot_be_reversed() {
    let _lock = qwenwork::test_env_lock().lock().await;
    let fixture = Fixture::new().await;
    let mut first = fixture.client().await;
    let mut second = fixture.client().await;
    fixture.seed(json!(1776345575000_i64), false);
    *fixture.mode.lock().unwrap() = "slow";
    first
        .send(json!({"id":51,"method":"qwenwork/usage","params":{}}))
        .await;
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        fixture.refresh_started.notified(),
    )
    .await
    .unwrap();
    second
        .send(json!({"id":52,"method":"qwenwork/usage","params":{}}))
        .await;
    fixture.refresh_release.notify_one();
    assert_eq!(first.response_for(51).await.0["result"]["signedIn"], true);
    assert_eq!(second.response_for(52).await.0["result"]["signedIn"], true);
    assert_eq!(fixture.refreshes(), 1);

    fixture.seed(json!(1776345575000_i64), false);
    first
        .send(json!({"id":53,"method":"qwenwork/usage","params":{}}))
        .await;
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        fixture.refresh_started.notified(),
    )
    .await
    .unwrap();
    fixture.call(&mut second, "qwenwork/logout").await;
    fixture.refresh_release.notify_one();
    assert_eq!(
        first.response_for(53).await.0["error"]["message"],
        "qwenwork_credential_changed"
    );
    assert_eq!(
        fixture.call(&mut second, "qwenwork/status").await["result"]["loggedIn"],
        false
    );
}
