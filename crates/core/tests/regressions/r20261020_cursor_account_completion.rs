//! Defect: account labels remained empty and browser grants could be discarded.
//! Root cause: missing labels were never hydrated and optional team/profile
//! metadata was treated as part of authorization completion.
//! Work item: 2026-10-20 Cursor account completion. Mounted by the public server
//! facade tests so core does not depend on the app-server implementation.

use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct AccountFixture {
    origin: String,
    block_profile: Arc<AtomicBool>,
    profiles: Arc<AtomicUsize>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for AccountFixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn fixture() -> AccountFixture {
    use bytes::Bytes;
    use http_body_util::Full;
    use hyper::Response;
    use hyper_util::rt::TokioIo;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let block_profile = Arc::new(AtomicBool::new(false));
    let profiles = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let task = tokio::spawn({
        let (block_profile, profiles, entered, release) = (
            block_profile.clone(),
            profiles.clone(),
            entered.clone(),
            release.clone(),
        );
        async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let (block_profile, profiles, entered, release) = (
                    block_profile.clone(),
                    profiles.clone(),
                    entered.clone(),
                    release.clone(),
                );
                connections.spawn(async move {
                    let service = hyper::service::service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
                        let (block_profile, profiles, entered, release) = (block_profile.clone(), profiles.clone(), entered.clone(), release.clone());
                        async move {
                            let value = if request.uri().path() == "/auth/poll" {
                                json!({"accessToken":"fixture-access","refreshToken":"fixture-refresh","selectedTeamId":null})
                            } else if request.uri().path() == "/oauth/token" {
                                let released = release.notified();
                                entered.notify_one();
                                released.await;
                                json!({"access_token":"fixture-renewed"})
                            } else {
                                assert!(request.uri().path().ends_with("GetMe"));
                                profiles.fetch_add(1, Ordering::SeqCst);
                                if block_profile.load(Ordering::SeqCst) {
                                    let released = release.notified();
                                    entered.notify_one();
                                    released.await;
                                }
                                json!({"email":"account@example.invalid"})
                            };
                            Ok::<_, std::convert::Infallible>(Response::new(Full::new(Bytes::from(value.to_string()))))
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(socket), service).await;
                });
            }
        }
    });
    AccountFixture {
        origin,
        block_profile,
        profiles,
        entered,
        release,
        task,
    }
}

async fn client(root: &std::path::Path) -> TestClient {
    let config = root.join("config.toml");
    std::fs::write(&config, "").unwrap();
    let mut client = TestClient::start_full(
        make_create_session(&[]),
        None,
        Some(root.join("rules.json")),
        Some(config),
    );
    client.initialize().await;
    super::super::cursor::set_config_root(root);
    client
}

async fn adopt(client: &mut TestClient, fixture: &AccountFixture, token: &str) {
    client.send(json!({"id":2,"method":"cursor/adoptCredential","params":{"userInfo":{"accessToken":token,"clientIdentity":"fixture-device","backendUrl":fixture.origin}}})).await;
    assert_eq!(client.response_for(2).await.0["result"]["ok"], true);
}

#[tokio::test]
async fn imported_account_without_a_cached_email_is_hydrated_by_status() {
    let _guard = super::super::cursor::test_lock().lock().await;
    let temp = tempfile::tempdir().unwrap();
    let fixture = fixture().await;
    let mut client = client(temp.path()).await;
    adopt(&mut client, &fixture, "fixture-import").await;
    client
        .send(json!({"id":3,"method":"cursor/status","params":{}}))
        .await;
    let status = client.response_for(3).await.0;
    assert_eq!(status["result"]["loggedIn"], true);
    assert_eq!(status["result"]["account"], "account@example.invalid");
    client
        .send(json!({"id":4,"method":"cursor/status","params":{}}))
        .await;
    assert_eq!(
        client.response_for(4).await.0["result"]["account"],
        "account@example.invalid"
    );
    assert_eq!(
        fixture.profiles.load(Ordering::SeqCst),
        1,
        "persist the account label without another profile request"
    );
    client.close().await;
}

#[tokio::test]
async fn browser_grant_with_no_team_completes_before_optional_profile_lookup() {
    let _guard = super::super::cursor::test_lock().lock().await;
    let temp = tempfile::tempdir().unwrap();
    let fixture = fixture().await;
    fixture.block_profile.store(true, Ordering::SeqCst);
    let mut client = client(temp.path()).await;
    let flow = super::super::cursor::tests::start_browser_login(&fixture.origin);
    client
        .send(
            json!({"id":2,"method":"cursor/loginPoll","params":{"flow":{"flowId":flow["flowId"]}}}),
        )
        .await;
    let completed = client
        .response_for_with_timeout(2, Duration::from_secs(2))
        .await
        .0;
    assert_eq!(completed["result"]["ok"], true, "{completed}");
    assert_eq!(completed["result"]["authMethod"], "browser");
    assert_eq!(
        fixture.profiles.load(Ordering::SeqCst),
        0,
        "grant completion never waits for account decoration"
    );
    let started = std::time::Instant::now();
    client
        .send(json!({"id":3,"method":"cursor/status","params":{}}))
        .await;
    let status = client
        .response_for_with_timeout(3, Duration::from_secs(3))
        .await
        .0;
    assert_eq!(status["result"]["loggedIn"], true, "{status}");
    assert!(status["result"]["account"].is_null());
    assert!(started.elapsed() < Duration::from_secs(3));
    eprintln!(
        "optional profile deadline: {:?}; login remains connected",
        started.elapsed()
    );
    client
        .send(json!({"id":4,"method":"cursor/status","params":{}}))
        .await;
    assert_eq!(
        client
            .response_for_with_timeout(4, Duration::from_secs(1))
            .await
            .0["result"]["loggedIn"],
        true
    );
    assert_eq!(
        fixture.profiles.load(Ordering::SeqCst),
        1,
        "failed optional lookups have a retry cooldown"
    );
    client.close().await;
}

#[tokio::test]
async fn status_refresh_returns_a_source_error_before_the_host_read_deadline() {
    let _guard = super::super::cursor::test_lock().lock().await;
    let temp = tempfile::tempdir().unwrap();
    let fixture = fixture().await;
    let mut client = client(temp.path()).await;
    adopt(&mut client, &fixture, "fixture-import").await;
    let path = temp.path().join("cursor-subscription-oauth.json");
    let mut stored: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    stored["authMethod"] = json!("browser");
    stored["refreshToken"] = json!("fixture-refresh");
    stored["userInfo"]["expiresAt"] = json!(chrono::Utc::now().timestamp_millis() + 30_000);
    std::fs::write(path, serde_json::to_vec(&stored).unwrap()).unwrap();
    let started = std::time::Instant::now();
    client
        .send(json!({"id":3,"method":"cursor/status","params":{}}))
        .await;
    let status = client
        .response_for_with_timeout(3, Duration::from_secs(14))
        .await
        .0;
    assert_eq!(status["result"]["loggedIn"], true, "{status}");
    assert_eq!(status["result"]["error"], "cursor_network_error");
    assert!(started.elapsed() < Duration::from_secs(15));
    assert_eq!(fixture.profiles.load(Ordering::SeqCst), 0);
    eprintln!("status refresh deadline: {:?}", started.elapsed());
    client.close().await;
}

#[tokio::test]
async fn a_late_profile_cannot_restore_an_account_after_logout() {
    let _guard = super::super::cursor::test_lock().lock().await;
    let temp = tempfile::tempdir().unwrap();
    let fixture = fixture().await;
    fixture.block_profile.store(true, Ordering::SeqCst);
    let mut client = client(temp.path()).await;
    adopt(&mut client, &fixture, "fixture-import").await;
    client
        .send(json!({"id":3,"method":"cursor/status","params":{}}))
        .await;
    tokio::time::timeout(Duration::from_secs(2), fixture.entered.notified())
        .await
        .unwrap();
    client
        .send(json!({"id":4,"method":"cursor/logout","params":{}}))
        .await;
    let (logout, early) = client.response_for(4).await;
    assert_eq!(logout["result"]["ok"], true);
    fixture.release.notify_waiters();
    let status = match early.into_iter().find(|message| message["id"] == 3) {
        Some(message) => message,
        None => client.response_for(3).await.0,
    };
    assert_ne!(status["result"]["account"], "account@example.invalid");
    assert!(!temp.path().join("cursor-subscription-oauth.json").exists());
    client
        .send(json!({"id":5,"method":"cursor/status","params":{}}))
        .await;
    assert_eq!(client.response_for(5).await.0["result"]["loggedIn"], false);
    client.close().await;
}
