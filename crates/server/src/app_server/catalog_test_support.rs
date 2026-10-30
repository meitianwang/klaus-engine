use serde_json::{json, Value};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(crate) fn credential_file(source: &str) -> String {
    match source {
        "bailian" => "bailian-tokenplan.json".into(),
        "minimax" => "minimax-mplan.json".into(),
        "zhipu" => "zhipu-codingplan.json".into(),
        "ark" => "ark-codingplan.json".into(),
        _ => format!("{source}-subscription-oauth.json"),
    }
}

pub(crate) fn store_credential(source: &str, root: &Path, account: &str) -> Value {
    let identity = format!("{}-{account}", root.display());
    let value = match source {
        "kimi" => json!({
            "authType": "apiKey", "apiKey": format!("secret-{identity}"),
            "account": { "userId": identity, "region": "cn" }
        }),
        "bailian" | "minimax" | "zhipu" => json!({
            "apiKey": format!("secret-{identity}"), "region": "cn"
        }),
        "ark" => json!({
            "apiKey": format!("secret-{identity}"), "region": "cn-beijing"
        }),
        _ => json!({
            "machineId": "0123456789abcdef0123456789abcdef",
            "deviceId": "fixture-device",
            "userInfo": {
                "uid": identity, "domain": "www.workbuddy.ai",
                "access_token": "fixture-access-token", "security_oauth_token": "fixture-access-token",
                "encrypt_user_info": "FIXTURE_ENCRYPTED_PROFILE",
                "key": "FIXTURE_SIGNING_KEY_0123456789abcdef",
                "expire_time": 4102444800_i64
            }
        }),
    };
    std::fs::write(root.join(credential_file(source)), value.to_string())
        .expect("credential write");
    value
}

struct EnvironmentGuard {
    name: String,
    previous: Option<std::ffi::OsString>,
}

impl EnvironmentGuard {
    fn set(name: String, value: &str) -> Self {
        let previous = std::env::var_os(&name);
        klaus_agent_core::runtime_process::set_process_environment_var(&name, value);
        Self { name, previous }
    }
}

impl Drop for EnvironmentGuard {
    fn drop(&mut self) {
        match &self.previous {
            Some(value) => {
                klaus_agent_core::runtime_process::set_process_environment_var(&self.name, value)
            }
            None => klaus_agent_core::runtime_process::remove_process_environment_var(&self.name),
        }
    }
}

pub(crate) struct Gateway {
    task: tokio::task::JoinHandle<()>,
    requests: Arc<AtomicUsize>,
    _environment: EnvironmentGuard,
}

impl Gateway {
    pub(crate) async fn start(source: &str, delay: Duration) -> Self {
        let row = json!({ "key": "live-model", "id": "live-model", "display_name": "Live", "name": "Live", "enable": true, "supportsToolCall": true });
        let body = match source {
            "qoder" => json!({ "assistant": [row] }),
            "qoderwork" | "qwenwork" => json!({ "qwork": [row] }),
            "workbuddy" => json!({ "models": [row] }),
            _ => json!({ "data": [row], "has_more": false }),
        };
        Self::start_with_body(source, delay, body).await
    }

    pub(crate) async fn start_with_body(source: &str, delay: Duration, body: Value) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("gateway bind");
        let base = format!("http://{}", listener.local_addr().expect("gateway address"));
        let name = format!(
            "{}_{}",
            source.to_uppercase(),
            if matches!(source, "bailian" | "minimax") {
                "CATALOG_BASE"
            } else {
                "API_BASE"
            }
        );
        let environment = EnvironmentGuard::set(name, &base);
        let body = body.to_string();
        let requests = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&requests);
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((mut stream, _)) = accepted else { break };
                        let count = Arc::clone(&count);
                        let body = body.clone();
                        connections.spawn(async move {
                            let mut request = [0_u8; 16384];
                            if stream.read(&mut request).await.unwrap_or(0) == 0 { return; }
                            count.fetch_add(1, Ordering::SeqCst);
                            tokio::time::sleep(delay).await;
                            let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                            let _ = stream.write_all(response.as_bytes()).await;
                        });
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {}
                }
            }
        });
        Self {
            task,
            requests,
            _environment: environment,
        }
    }

    pub(crate) async fn wait_for_request(&self) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.requests.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("background revalidation reached gateway");
    }

    pub(crate) fn request_count(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }
}

impl Drop for Gateway {
    fn drop(&mut self) {
        self.task.abort();
    }
}

// The macro mounts the same behavioral contract beside each source's existing
// catalog tests, exercising its real credential, disk and HTTP paths.
macro_rules! warm_catalog_tests {
    ($source:literal, $read:expr $(, $field:ident : $value:expr)*) => {
        fn warm_fixture() -> tempfile::TempDir {
            let fixture = tempfile::tempdir().expect("catalog fixture");
            super::super::set_config_root(fixture.path());
            forget_cache();
            fixture
        }

        #[tokio::test]
        async fn warm_matching_snapshot_paints_before_slow_live_and_revalidates() {
            use crate::app_server::catalog_test_support::{store_credential, Gateway};
            let _lock = super::super::test_env_lock().lock().await;
            let fixture = warm_fixture();
            store_credential($source, fixture.path(), "a");
            let fingerprint = super::super::auth::catalog_fingerprint().expect("fingerprint");
            let snapshot = vec![json!({ "key": "snapshot-model", "display_name": "Snapshot" })];
            write_owned_catalog(&snapshot, &fingerprint);
            let gateway = Gateway::start($source, std::time::Duration::from_millis(2000)).await;
            let start = std::time::Instant::now();
            let models = fetch_models_warm().await.unwrap_or_else(|error| panic!("{}", error.message));
            let elapsed = start.elapsed();
            assert_eq!(models, snapshot);
            assert!(elapsed < std::time::Duration::from_millis(500), "warm took {elapsed:?}");
            println!("{} matching-snapshot warm: {elapsed:?}", $source);
            gateway.wait_for_request().await;
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    if ($read)().iter().any(|model| model["key"] == "live-model") { break; }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            }).await.expect("live refresh replaced the owned snapshot");
            forget_cache();
        }

        #[tokio::test]
        async fn warm_signed_out_refuses_disk_and_logout_clears_memory() {
            use crate::app_server::catalog_test_support::store_credential;
            let _lock = super::super::test_env_lock().lock().await;
            let fixture = warm_fixture();
            store_credential($source, fixture.path(), "a");
            let fingerprint = super::super::auth::catalog_fingerprint().expect("fingerprint");
            let models = vec![json!({ "key": "snapshot-model" })];
            write_owned_catalog(&models, &fingerprint);
            *catalog_cache().lock().unwrap_or_else(|p| p.into_inner()) = Some(CatalogCache {
                fingerprint: Some(fingerprint), models, expires_at: now_ms() + CATALOG_TTL_MS,
                $( $field: $value, )*
            });
            assert!(super::super::auth::logout().is_ok());
            assert!(catalog_cache().lock().unwrap_or_else(|p| p.into_inner()).is_none());
            assert!(owned_catalog_path().exists());
            assert!(($read)().is_empty());
            let error = fetch_models_warm().await.err().expect("signed-out error");
            assert!(error.not_found, "{}", error.message);
        }

        #[tokio::test]
        async fn warm_without_snapshot_keeps_the_live_success_window() {
            use crate::app_server::catalog_test_support::{store_credential, Gateway};
            let _lock = super::super::test_env_lock().lock().await;
            let fixture = warm_fixture();
            store_credential($source, fixture.path(), "a");
            let _gateway = Gateway::start($source, std::time::Duration::from_millis(20)).await;
            let models = fetch_models_warm().await.unwrap_or_else(|error| panic!("{}", error.message));
            assert!(models.iter().any(|model| model["key"] == "live-model"), "{models:?}");
            assert!(($read)().iter().any(|model| model["key"] == "live-model"));
            forget_cache();
        }

        #[tokio::test]
        async fn warm_without_snapshot_bounds_a_slow_live_read() {
            use crate::app_server::catalog_test_support::{store_credential, Gateway};
            let _lock = super::super::test_env_lock().lock().await;
            let fixture = warm_fixture();
            store_credential($source, fixture.path(), "a");
            let gateway = Gateway::start($source, std::time::Duration::from_millis(4000)).await;
            let start = std::time::Instant::now();
            let result = fetch_models_warm().await;
            let elapsed = start.elapsed();
            assert!(elapsed >= std::time::Duration::from_millis(1200), "live had no first chance: {elapsed:?}");
            assert!(elapsed < std::time::Duration::from_millis(3000), "unbounded live read: {elapsed:?}");
            if let Ok(models) = result { assert!(!models.iter().any(|model| model["key"] == "live-model")); }
            println!("{} no-snapshot bounded warm: {elapsed:?}", $source);
            gateway.wait_for_request().await;
            forget_cache();
        }

        #[tokio::test]
        async fn warm_rejects_legacy_and_other_account_snapshots() {
            use crate::app_server::catalog_test_support::{store_credential, Gateway};
            let _lock = super::super::test_env_lock().lock().await;
            let fixture = warm_fixture();
            let credential = store_credential($source, fixture.path(), "a");
            let models = vec![json!({ "key": "old-account-model" })];
            std::fs::write(owned_catalog_path(), json!({ "models": models }).to_string()).expect("legacy snapshot");
            assert!(($read)().is_empty());
            let fingerprint = super::super::auth::catalog_fingerprint().expect("fingerprint");
            if let Some(key) = credential.get("apiKey").and_then(Value::as_str) {
                assert!(!fingerprint.contains(key), "plaintext key in fingerprint");
            }
            write_owned_catalog(&models, &fingerprint);
            assert_eq!(($read)(), models);
            store_credential($source, fixture.path(), "b");
            assert!(($read)().is_empty());
            let _gateway = Gateway::start($source, std::time::Duration::from_millis(20)).await;
            let models = fetch_models_warm().await.unwrap_or_else(|error| panic!("{}", error.message));
            assert!(models.iter().any(|model| model["key"] == "live-model"), "{models:?}");
            assert!(!models.iter().any(|model| model["key"] == "old-account-model"));
            forget_cache();
        }

        #[tokio::test]
        async fn fingerprint_tracks_the_account_site_and_contains_no_plaintext_key() {
            use crate::app_server::catalog_test_support::{credential_file, store_credential};
            let _lock = super::super::test_env_lock().lock().await;
            let fixture = warm_fixture();
            let mut credential = store_credential($source, fixture.path(), "a");
            let before = super::super::auth::catalog_fingerprint().expect("fingerprint");
            write_owned_catalog(&[json!({ "key": "old-site-model" })], &before);
            match $source {
                "workbuddy" => credential["userInfo"]["domain"] = json!("www.codebuddy.cn"),
                "kimi" => credential["account"]["region"] = json!("global"),
                "bailian" | "zhipu" => credential["region"] = json!("intl"),
                "minimax" => credential["region"] = json!("global"),
                "ark" => credential["region"] = json!("intl"),
                _ => credential["userInfo"]["uid"] = json!("another-user"),
            }
            std::fs::write(fixture.path().join(credential_file($source)), credential.to_string()).expect("credential switch");
            let after = super::super::auth::catalog_fingerprint().expect("new fingerprint");
            assert_ne!(before, after);
            assert!(($read)().is_empty());
            if matches!($source, "bailian" | "minimax" | "zhipu" | "ark") {
                let hash = after.split(':').next().expect("hash");
                assert_eq!(hash.len(), 64);
                assert!(hash.bytes().all(|byte| byte.is_ascii_hexdigit()));
                assert!(!after.contains(credential["apiKey"].as_str().expect("key")));
            }
        }

        #[tokio::test]
        async fn old_account_live_refresh_cannot_relabel_the_new_account_cache() {
            use crate::app_server::catalog_test_support::{store_credential, Gateway};
            let _lock = super::super::test_env_lock().lock().await;
            let fixture = warm_fixture();
            store_credential($source, fixture.path(), "a");
            let gateway = Gateway::start($source, std::time::Duration::from_millis(100)).await;
            let refresh = tokio::spawn(fetch_models());
            gateway.wait_for_request().await;
            store_credential($source, fixture.path(), "b");
            let _ = refresh.await.expect("refresh task");
            assert!(($read)().is_empty());
            assert!(catalog_cache().lock().unwrap_or_else(|p| p.into_inner()).is_none());
        }
    };
}

pub(crate) use warm_catalog_tests;
