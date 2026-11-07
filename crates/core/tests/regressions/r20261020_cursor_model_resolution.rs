//! Defect: selecting a Cursor source could outwait the desktop read deadline.
//! Root cause: identity reads waited for token refresh and remote catalog IO.
//! Work item: 2026-10-20 model-selection timeout. Mounted by the server facade
//! tests because core cannot depend on the app-server's public request surface.

use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Clone)]
struct CatalogReply {
    encoding: String,
    body: Vec<u8>,
}

struct CatalogFixture {
    origin: String,
    block_catalog: Arc<AtomicBool>,
    block_auth: Arc<AtomicBool>,
    requests: Arc<AtomicUsize>,
    catalog_reply: Arc<std::sync::Mutex<Option<CatalogReply>>>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for CatalogFixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn fixture() -> CatalogFixture {
    use bytes::Bytes;
    use http_body_util::Full;
    use hyper::Response;
    use hyper_util::rt::TokioIo;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let block_catalog = Arc::new(AtomicBool::new(false));
    let block_auth = Arc::new(AtomicBool::new(false));
    let requests = Arc::new(AtomicUsize::new(0));
    let catalog_reply = Arc::new(std::sync::Mutex::new(None::<CatalogReply>));
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let task = tokio::spawn({
        let (block_catalog, block_auth, requests, entered, release, catalog_reply) = (
            block_catalog.clone(),
            block_auth.clone(),
            requests.clone(),
            entered.clone(),
            release.clone(),
            catalog_reply.clone(),
        );
        async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let (block_catalog, block_auth, requests, entered, release, catalog_reply) = (
                    block_catalog.clone(),
                    block_auth.clone(),
                    requests.clone(),
                    entered.clone(),
                    release.clone(),
                    catalog_reply.clone(),
                );
                connections.spawn(async move {
                    let service = hyper::service::service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
                        let (block_catalog, block_auth, requests, entered, release, catalog_reply) = (
                            block_catalog.clone(), block_auth.clone(), requests.clone(),
                            entered.clone(), release.clone(), catalog_reply.clone(),
                        );
                        async move {
                            let auth = request.uri().path() == "/oauth/token";
                            let profile = request.uri().path().ends_with("GetMe");
                            if !auth && !profile {
                                assert!(request.uri().path().ends_with("AvailableModels"));
                                assert_eq!(request.headers()["accept-encoding"], "gzip");
                                requests.fetch_add(1, Ordering::SeqCst);
                            }
                            let blocked = if auth { &block_auth } else { &block_catalog };
                            if !profile && blocked.load(Ordering::SeqCst) {
                                let released = release.notified();
                                entered.notify_one();
                                released.await;
                            }
                            let value = if auth {
                                json!({"access_token":"renewed-fixture-token","refresh_token":"fixture-refresh"})
                            } else if profile {
                                json!({"email":"account@example.invalid"})
                            } else {
                                json!({"models":[{"name":"fixture-model","supportsAgent":true,"variants":[{"displayName":"Fixture","isDefaultNonMaxConfig":true,"parameterValues":[]}]}]})
                            };
                            let reply = if !auth && !profile {
                                catalog_reply.lock().unwrap().clone()
                            } else {
                                None
                            };
                            let response = if let Some(reply) = reply {
                                Response::builder().header("content-encoding", reply.encoding)
                                    .body(Full::new(Bytes::from(reply.body))).unwrap()
                            } else {
                                Response::new(Full::new(Bytes::from(value.to_string())))
                            };
                            Ok::<_, std::convert::Infallible>(response)
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(socket), service).await;
                });
            }
        }
    });
    CatalogFixture {
        origin,
        block_catalog,
        block_auth,
        requests,
        catalog_reply,
        entered,
        release,
        task,
    }
}

async fn client(root: &std::path::Path, fixture: &CatalogFixture) -> TestClient {
    let config = root.join("config.toml");
    std::fs::write(&config, "").unwrap();
    let mut client = TestClient::start_full(
        make_create_session(&[]),
        None,
        Some(root.join("rules.json")),
        Some(config),
    );
    client.initialize().await;
    client.send(json!({"id":2,"method":"cursor/adoptCredential","params":{"userInfo":{"accessToken":"fixture-token","clientIdentity":"fixture-device","backendUrl":fixture.origin}}})).await;
    assert_eq!(client.response_for(2).await.0["result"]["ok"], true);
    client
}

async fn warm(client: &mut TestClient) {
    client
        .send(json!({"id":3,"method":"cursor/models","params":{}}))
        .await;
    assert_eq!(
        client.response_for(3).await.0["result"]["models"][0]["key"],
        "fixture-model[]"
    );
}

#[tokio::test]
async fn cached_identity_does_not_wait_for_an_unrelated_token_refresh() {
    let _guard = super::super::cursor::test_lock().lock().await;
    let temp = tempfile::tempdir().unwrap();
    let fixture = fixture().await;
    let mut client = client(temp.path(), &fixture).await;
    warm(&mut client).await;
    let credential_path = temp.path().join("cursor-subscription-oauth.json");
    let mut credential: Value =
        serde_json::from_slice(&std::fs::read(&credential_path).unwrap()).unwrap();
    credential["authMethod"] = json!("browser");
    credential["refreshToken"] = json!("fixture-refresh");
    credential["userInfo"]["expiresAt"] = json!(chrono::Utc::now().timestamp_millis() + 30_000);
    std::fs::write(credential_path, serde_json::to_vec(&credential).unwrap()).unwrap();
    fixture.block_auth.store(true, Ordering::SeqCst);
    client
        .send(json!({"id":4,"method":"cursor/status","params":{}}))
        .await;
    tokio::time::timeout(Duration::from_secs(2), fixture.entered.notified())
        .await
        .unwrap();
    client.send(json!({"id":5,"method":"model/selection/resolve","params":{"source":"cursor","selection":"auto"}})).await;
    let resolved = client
        .response_for_with_timeout(5, Duration::from_secs(2))
        .await
        .0;
    assert_eq!(resolved["result"]["selection"], "cursor:fixture-model[]");
    fixture.release.notify_waiters();
    assert_eq!(client.response_for(4).await.0["result"]["loggedIn"], true);
    client.close().await;
}

#[tokio::test]
async fn stale_identity_returns_immediately_and_shares_background_revalidation() {
    let _guard = super::super::cursor::test_lock().lock().await;
    let temp = tempfile::tempdir().unwrap();
    let fixture = fixture().await;
    let mut client = client(temp.path(), &fixture).await;
    warm(&mut client).await;
    super::super::cursor::tests::age_catalog(Duration::from_secs(301));
    fixture.block_catalog.store(true, Ordering::SeqCst);
    for id in 4..8 {
        client.send(json!({"id":id,"method":"model/selection/resolve","params":{"source":"cursor","selection":"auto"}})).await;
        let resolved = client
            .response_for_with_timeout(id, Duration::from_secs(2))
            .await
            .0;
        assert_eq!(resolved["result"]["selection"], "cursor:fixture-model[]");
    }
    tokio::time::timeout(Duration::from_secs(2), fixture.entered.notified())
        .await
        .unwrap();
    assert_eq!(
        fixture.requests.load(Ordering::SeqCst),
        2,
        "one warm read and one shared refresh"
    );
    client
        .send(json!({"id":8,"method":"cursor/logout","params":{}}))
        .await;
    assert_eq!(client.response_for(8).await.0["result"]["ok"], true);
    client.send(json!({"id":9,"method":"model/selection/resolve","params":{"source":"cursor","selection":"auto"}})).await;
    let resolved = client.response_for(9).await.0;
    assert_eq!(resolved["error"]["message"], "cursor_not_signed_in");
    fixture.release.notify_waiters();
    client.close().await;
}

#[tokio::test]
async fn warm_picker_returns_a_stale_catalog_without_waiting() {
    let _guard = super::super::cursor::test_lock().lock().await;
    let temp = tempfile::tempdir().unwrap();
    let fixture = fixture().await;
    let mut client = client(temp.path(), &fixture).await;
    warm(&mut client).await;
    super::super::cursor::tests::age_catalog(Duration::from_secs(301));
    fixture.block_catalog.store(true, Ordering::SeqCst);
    client
        .send(json!({"id":4,"method":"cursor/models","params":{"read":"warm"}}))
        .await;
    let models = client
        .response_for_with_timeout(4, Duration::from_secs(2))
        .await
        .0;
    assert_eq!(models["result"]["models"][0]["key"], "fixture-model[]");
    tokio::time::timeout(Duration::from_secs(2), fixture.entered.notified())
        .await
        .unwrap();
    assert_eq!(fixture.requests.load(Ordering::SeqCst), 2);
    fixture.release.notify_waiters();
    client.close().await;
}

#[tokio::test]
async fn warm_picker_paints_the_owned_snapshot_after_memory_is_cleared() {
    let _guard = super::super::cursor::test_lock().lock().await;
    let temp = tempfile::tempdir().unwrap();
    let fixture = fixture().await;
    let mut client = client(temp.path(), &fixture).await;
    warm(&mut client).await;
    let stored: Value = serde_json::from_slice(
        &std::fs::read(temp.path().join("cursor-subscription-oauth.json")).unwrap(),
    )
    .unwrap();
    let snapshot: Value = serde_json::from_slice(
        &std::fs::read(temp.path().join("cursor-model-catalog.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(snapshot["projectionVersion"], 1);
    assert_eq!(snapshot["fingerprint"], stored["credentialId"]);
    super::super::cursor::tests::forget_catalog_memory();
    fixture.block_catalog.store(true, Ordering::SeqCst);
    client
        .send(json!({"id":4,"method":"cursor/models","params":{"read":"warm"}}))
        .await;
    let models = client
        .response_for_with_timeout(4, Duration::from_secs(2))
        .await
        .0;
    assert_eq!(models["result"]["models"][0]["key"], "fixture-model[]");
    tokio::time::timeout(Duration::from_secs(2), fixture.entered.notified())
        .await
        .unwrap();
    assert_eq!(fixture.requests.load(Ordering::SeqCst), 2);
    fixture.release.notify_waiters();
    client.close().await;
}

#[tokio::test]
async fn warm_picker_does_not_paint_another_accounts_snapshot() {
    let _guard = super::super::cursor::test_lock().lock().await;
    let temp = tempfile::tempdir().unwrap();
    let fixture = fixture().await;
    let mut client = client(temp.path(), &fixture).await;
    warm(&mut client).await;
    std::fs::write(
        temp.path().join("cursor-model-catalog.json"),
        serde_json::to_vec(&json!({
            "projectionVersion": 1,
            "fingerprint": "someone-else",
            "models": [{"key": "foreign-model[]", "display_name": "Foreign"}]
        }))
        .unwrap(),
    )
    .unwrap();
    super::super::cursor::tests::forget_catalog_memory();
    client
        .send(json!({"id":4,"method":"cursor/models","params":{"read":"warm"}}))
        .await;
    let models = client.response_for(4).await.0;
    assert_eq!(models["result"]["models"][0]["key"], "fixture-model[]");
    assert_eq!(fixture.requests.load(Ordering::SeqCst), 2);
    client.close().await;
}

#[tokio::test]
async fn cold_identity_reports_a_source_error_before_the_desktop_deadline() {
    let _guard = super::super::cursor::test_lock().lock().await;
    let temp = tempfile::tempdir().unwrap();
    let fixture = fixture().await;
    fixture.block_catalog.store(true, Ordering::SeqCst);
    let mut client = client(temp.path(), &fixture).await;
    let started = std::time::Instant::now();
    client.send(json!({"id":4,"method":"model/selection/resolve","params":{"source":"cursor","selection":"auto"}})).await;
    let resolved = client
        .response_for_with_timeout(4, Duration::from_secs(14))
        .await
        .0;
    assert_eq!(resolved["error"]["message"], "cursor_network_error");
    assert!(started.elapsed() < Duration::from_secs(15));
    eprintln!(
        "cold identity reply after {:?}: {}",
        started.elapsed(),
        resolved
    );
    client.close().await;
}

fn gzip(bytes: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

#[tokio::test]
async fn compressed_catalogs_decode_all_members_before_model_selection() {
    let _guard = super::super::cursor::test_lock().lock().await;
    let payload = json!({"models":[{"name":"fixture-model","supportsAgent":true}],"padding":"x".repeat(417_228)}).to_string();
    let single = gzip(payload.as_bytes());
    let midpoint = payload.len() / 2;
    let mut multiple = gzip(&payload.as_bytes()[..midpoint]);
    multiple.extend(gzip(&payload.as_bytes()[midpoint..]));
    assert!(single.len() < payload.len() / 10);
    for (encoding, body) in [("gzip", single), ("GZip", multiple)] {
        let temp = tempfile::tempdir().unwrap();
        let fixture = fixture().await;
        *fixture.catalog_reply.lock().unwrap() = Some(CatalogReply {
            encoding: encoding.into(),
            body,
        });
        let mut client = client(temp.path(), &fixture).await;
        warm(&mut client).await;
        client.send(json!({"id":4,"method":"model/selection/resolve","params":{"source":"cursor","selection":"auto"}})).await;
        let resolved = client.response_for(4).await.0;
        assert_eq!(
            resolved["result"]["selection"], "cursor:fixture-model[]",
            "{encoding}: {resolved}"
        );
        client.close().await;
    }
}

#[tokio::test]
async fn invalid_or_oversized_encoded_catalogs_never_enter_the_model_cache() {
    let _guard = super::super::cursor::test_lock().lock().await;
    let payload = br#"{"models":[{"name":"fixture-model"}]}"#;
    let valid = gzip(payload);
    let mut bad_crc = valid.clone();
    let crc_offset = bad_crc.len() - 8;
    bad_crc[crc_offset] ^= 1;
    let mut trailing = valid.clone();
    trailing.extend(b"trailing garbage");
    let oversized = vec![b'x'; 8 * 1024 * 1024 + 1];
    let inflated = gzip(&oversized);
    let cases = [
        ("crc", "gzip", bad_crc, "cursor_response_read_failed"),
        (
            "truncated",
            "gzip",
            valid[..valid.len() - 5].to_vec(),
            "cursor_response_read_failed",
        ),
        ("trailing", "gzip", trailing, "cursor_response_read_failed"),
        (
            "unsupported",
            "br",
            payload.to_vec(),
            "cursor_response_read_failed",
        ),
        (
            "stacked",
            "gzip, gzip",
            valid,
            "cursor_response_read_failed",
        ),
        (
            "decoded limit",
            "gzip",
            inflated,
            "cursor_response_too_large",
        ),
        (
            "wire limit",
            "identity",
            oversized,
            "cursor_response_too_large",
        ),
    ];
    for (name, encoding, body, error) in cases {
        let temp = tempfile::tempdir().unwrap();
        let fixture = fixture().await;
        *fixture.catalog_reply.lock().unwrap() = Some(CatalogReply {
            encoding: encoding.into(),
            body,
        });
        let mut client = client(temp.path(), &fixture).await;
        client
            .send(json!({"id":3,"method":"cursor/models","params":{}}))
            .await;
        let result = client.response_for(3).await.0;
        assert_eq!(result["error"]["message"], error, "{name}: {result}");
        *fixture.catalog_reply.lock().unwrap() = None;
        client
            .send(json!({"id":4,"method":"cursor/models","params":{}}))
            .await;
        assert_eq!(
            client.response_for(4).await.0["result"]["models"][0]["key"],
            "fixture-model[]",
            "{name}: a failed body must not poison the cache"
        );
        assert_eq!(
            fixture.requests.load(Ordering::SeqCst),
            2,
            "{name}: the recovery read must fetch a fresh catalog"
        );
        client.close().await;
    }
}
