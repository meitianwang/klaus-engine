use super::*;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn identity(base: &str) -> ManagedReviewCloudIdentity {
    ManagedReviewCloudIdentity {
        chatgpt_user_id: Some("user".into()),
        account_id: Some("account".into()),
        plan_type: Some("business".into()),
        backend_base_url: base.into(),
        owner_generation: 1,
    }
}

struct TemporaryHome(PathBuf);
impl TemporaryHome {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("klaus-cloud-requirements-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for TemporaryHome {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

struct TestAuth {
    identity: Mutex<Option<ManagedReviewCloudIdentity>>,
    token: Mutex<String>,
    recoveries: Mutex<VecDeque<ManagedReviewCloudAuthRecovery>>,
    recovery_owners: Mutex<Vec<ManagedReviewCloudIdentity>>,
    calls: AtomicUsize,
    replace_owner_on_current: AtomicUsize,
}
impl TestAuth {
    fn new(base: &str) -> Arc<Self> {
        Arc::new(Self {
            identity: Mutex::new(Some(identity(base))),
            token: Mutex::new("original".into()),
            recoveries: Mutex::new(VecDeque::new()),
            recovery_owners: Mutex::new(Vec::new()),
            calls: AtomicUsize::new(0),
            replace_owner_on_current: AtomicUsize::new(usize::MAX),
        })
    }
}
#[async_trait]
impl ManagedReviewCloudAuthSource for TestAuth {
    async fn current(&self) -> Result<Option<ManagedReviewCloudAuth>, String> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if call == self.replace_owner_on_current.load(Ordering::SeqCst) {
            self.identity
                .lock()
                .unwrap()
                .as_mut()
                .unwrap()
                .owner_generation += 1;
        }
        Ok(self
            .identity
            .lock()
            .unwrap()
            .clone()
            .map(|identity| ManagedReviewCloudAuth {
                headers: HashMap::from([
                    (
                        "Authorization".into(),
                        format!("Bearer {}", self.token.lock().unwrap()),
                    ),
                    (
                        "ChatGPT-Account-Id".into(),
                        identity.account_id.clone().unwrap_or_default(),
                    ),
                ]),
                identity,
                client: reqwest::Client::builder().no_proxy().build().unwrap(),
            }))
    }
    async fn recover_unauthorized(
        &self,
        _step: usize,
        expected: &ManagedReviewCloudIdentity,
    ) -> ManagedReviewCloudAuthRecovery {
        self.recovery_owners.lock().unwrap().push(expected.clone());
        let result = self
            .recoveries
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(ManagedReviewCloudAuthRecovery::Unavailable);
        if result == ManagedReviewCloudAuthRecovery::Refreshed {
            *self.token.lock().unwrap() = "refreshed".into();
        }
        result
    }
}

struct Server {
    base: String,
    requests: Arc<Mutex<Vec<String>>>,
    task: JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    async fn new(responses: Vec<(u16, String)>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let task = tokio::spawn(async move {
            for (status, body) in responses {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                loop {
                    let mut buffer = [0; 1024];
                    let count = stream.read(&mut buffer).await.unwrap();
                    if count == 0 {
                        break;
                    }
                    assert!(
                        request.len() + count <= 8192,
                        "test request headers exceeded their bound"
                    );
                    request.extend_from_slice(&buffer[..count]);
                    if request.windows(4).any(|part| part == b"\r\n\r\n") {
                        break;
                    }
                }
                captured
                    .lock()
                    .unwrap()
                    .push(String::from_utf8(request).unwrap());
                let head = format!("HTTP/1.1 {status} Response\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
                stream.write_all(head.as_bytes()).await.unwrap();
                stream.write_all(body.as_bytes()).await.unwrap();
            }
        });
        Self {
            base,
            requests,
            task,
        }
    }
}

fn new_service(auth: Arc<TestAuth>, home: &TemporaryHome) -> Service {
    Service {
        auth,
        cache: BundleCache::new(home.0.clone()),
        latest: OnceCell::new(),
        observed: Arc::new(Mutex::new(ObservedBundle::default())),
    }
}

fn network_bundle(contents: &str) -> String {
    serde_json::json!({"requirements_toml":{"enterprise_managed":[{"id":"policy","name":"Administrator","contents":contents}]}}).to_string()
}

#[test]
fn exact_plan_eligibility_does_not_promote_team_or_personal_subscriptions() {
    for plan in [
        "business",
        "ent26",
        "enterprise_cbp_automation",
        "enterprise_cbp_usage_based",
        "edu",
        "edu_plus",
        "edu_pro",
        "enterprise",
    ] {
        let mut auth = identity("https://chatgpt.com/backend-api");
        auth.plan_type = Some(plan.into());
        assert!(cloud_requirements_eligible(&auth), "{plan}");
    }
    for plan in [
        "team",
        "self_serve_business_prolite",
        "self_serve_business_usage_based",
        "plus",
        "pro",
        "free",
        "Enterprise",
    ] {
        let mut auth = identity("https://chatgpt.com/backend-api");
        auth.plan_type = Some(plan.into());
        assert!(!cloud_requirements_eligible(&auth), "{plan}");
    }
}

#[test]
fn discovery_route_uses_the_bound_backend_root() {
    assert_eq!(
        cloud_requirements_url("https://chatgpt.com/backend-api/codex/"),
        "https://chatgpt.com/backend-api/wham/config/bundle"
    );
    assert_eq!(
        cloud_requirements_url("https://chatgpt.com/"),
        "https://chatgpt.com/backend-api/wham/config/bundle"
    );
    assert_eq!(
        cloud_requirements_url("https://backend.example/"),
        "https://backend.example/api/codex/config/bundle"
    );
}

#[test]
fn network_buckets_are_optional_nullable_and_highest_priority_first() {
    for text in [
        "{}",
        "{\"config_toml\":null,\"requirements_toml\":null}",
        "{\"requirements_toml\":{\"enterprise_managed\":null}}",
    ] {
        assert_eq!(
            Bundle::from_network(text.as_bytes()).unwrap(),
            Bundle::default()
        );
    }
    let text = serde_json::json!({"requirements_toml":{"enterprise_managed":[{"id":"high","name":"High","contents":"guardian_policy_config='high'"},{"id":"low","name":"Low","contents":"guardian_policy_config='low'"}]}}).to_string();
    let bundle = Bundle::from_network(text.as_bytes()).unwrap();
    bundle.validate().unwrap();
    assert_eq!(
        bundle
            .requirements()
            .iter()
            .map(|layer| layer.id.as_str())
            .collect::<Vec<_>>(),
        ["low", "high"]
    );
}

#[tokio::test]
async fn real_backend_get_uses_native_headers_and_accepts_unsigned_network_bundle() {
    let server = Server::new(vec![(
        200,
        network_bundle("guardian_policy_config='managed'"),
    )])
    .await;
    let home = TemporaryHome::new();
    let auth = TestAuth::new(&server.base);
    let service = new_service(auth, &home);
    let snapshot = service.load_startup().await;
    let bundle = snapshot.result.unwrap();
    assert_eq!(
        bundle.requirements()[0].toml,
        "guardian_policy_config='managed'"
    );
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].starts_with("GET /api/codex/config/bundle HTTP/1.1"));
    assert!(requests[0]
        .to_ascii_lowercase()
        .contains("authorization: bearer original"));
    assert!(requests[0]
        .to_ascii_lowercase()
        .contains("chatgpt-account-id: account"));
    assert!(home.0.join("cloud-config-bundle-cache.json").is_file());
}

#[tokio::test]
async fn valid_cache_is_identity_scoped_and_reused_across_private_owner_generations() {
    let server = Server::new(vec![(
        200,
        network_bundle("guardian_extra_policy='cached'"),
    )])
    .await;
    let home = TemporaryHome::new();
    let auth = TestAuth::new(&server.base);
    let first = new_service(auth.clone(), &home);
    assert!(first.load_startup().await.result.is_ok());
    auth.identity
        .lock()
        .unwrap()
        .as_mut()
        .unwrap()
        .owner_generation = 99;
    let second = new_service(auth, &home);
    assert!(second.load_startup().await.result.is_ok());
    assert_eq!(
        server.requests.lock().unwrap().len(),
        1,
        "valid same-user/account cache must avoid another backend request"
    );
}

#[tokio::test]
async fn edited_cache_is_a_miss_and_requires_the_real_backend_again() {
    let server = Server::new(vec![
        (200, network_bundle("guardian_policy_config='original'")),
        (200, network_bundle("guardian_policy_config='fresh'")),
    ])
    .await;
    let home = TemporaryHome::new();
    let auth = TestAuth::new(&server.base);
    let first = new_service(auth.clone(), &home);
    first.load_startup().await.result.unwrap();
    let path = home.0.join("cloud-config-bundle-cache.json");
    let text = std::fs::read_to_string(&path)
        .unwrap()
        .replace("original", "tampered");
    std::fs::write(&path, text).unwrap();
    let second = new_service(auth, &home);
    let bundle = second.load_startup().await.result.unwrap();
    assert_eq!(
        bundle.requirements()[0].toml,
        "guardian_policy_config='fresh'"
    );
    assert_eq!(server.requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn unauthorized_recovery_pins_owner_and_retries_the_same_network_attempt() {
    let server = Server::new(vec![
        (401, "{}".into()),
        (200, network_bundle("guardian_policy_config='refreshed'")),
    ])
    .await;
    let home = TemporaryHome::new();
    let auth = TestAuth::new(&server.base);
    auth.recoveries
        .lock()
        .unwrap()
        .push_back(ManagedReviewCloudAuthRecovery::Refreshed);
    let service = new_service(auth.clone(), &home);
    assert!(service.load_startup().await.result.is_ok());
    assert_eq!(
        auth.recovery_owners.lock().unwrap().as_slice(),
        &[identity(&server.base)]
    );
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests[1]
        .to_ascii_lowercase()
        .contains("authorization: bearer refreshed"));
}

#[tokio::test]
async fn invalid_managed_bundle_fails_closed_without_cache_publication() {
    let server = Server::new(vec![(
        200,
        network_bundle("[auto_review]\nrequired_on_models=['provider/model']"),
    )])
    .await;
    let home = TemporaryHome::new();
    let service = new_service(TestAuth::new(&server.base), &home);
    assert_eq!(
        service.load_startup().await.result.unwrap_err().code,
        CloudRequirementsErrorCode::InvalidBundle
    );
    assert!(!home.0.join("cloud-config-bundle-cache.json").exists());
}

#[tokio::test]
async fn retired_or_superseded_policy_cannot_commit_a_prepared_cache_write() {
    let home = TemporaryHome::new();
    let service = new_service(TestAuth::new("http://127.0.0.1:1"), &home);
    let first =
        Bundle::from_network(network_bundle("guardian_policy_config='first'").as_bytes()).unwrap();
    let revision = service.observe(&first).unwrap();
    let second =
        Bundle::from_network(network_bundle("guardian_policy_config='second'").as_bytes()).unwrap();
    service.observe(&second).unwrap();
    service
        .cache
        .store(&identity("http://127.0.0.1:1"), first.clone(), revision)
        .await
        .unwrap();
    assert!(!home.0.join("cloud-config-bundle-cache.json").exists());
    let revision = service.observe(&first).unwrap();
    service.retire();
    service
        .cache
        .store(&identity("http://127.0.0.1:1"), first, revision)
        .await
        .unwrap();
    assert!(!home.0.join("cloud-config-bundle-cache.json").exists());
}

#[tokio::test]
async fn ineligible_or_missing_native_identity_skips_the_backend_entirely() {
    let home = TemporaryHome::new();
    let auth = TestAuth::new("http://127.0.0.1:1");
    auth.identity.lock().unwrap().as_mut().unwrap().plan_type = Some("team".into());
    assert_eq!(
        new_service(auth.clone(), &home)
            .load_startup()
            .await
            .result
            .unwrap(),
        Bundle::default()
    );
    *auth.identity.lock().unwrap() = None;
    assert_eq!(
        new_service(auth, &home)
            .load_startup()
            .await
            .result
            .unwrap(),
        Bundle::default()
    );
}

#[tokio::test(start_paused = true)]
async fn request_failures_and_decode_failures_retry_up_to_five_attempts() {
    let server = Server::new(vec![(503, "{}".into()); MAX_ATTEMPTS]).await;
    let home = TemporaryHome::new();
    let auth = TestAuth::new(&server.base);
    let service = new_service(auth.clone(), &home);
    let error = service
        .fetch(auth.current().await.unwrap().unwrap())
        .await
        .unwrap_err();
    assert_eq!(error.code, CloudRequirementsErrorCode::RequestFailed);
    assert_eq!(error.status_code, Some(503));
    assert_eq!(server.requests.lock().unwrap().len(), 5);
    assert!(!home.0.join("cloud-config-bundle-cache.json").exists());
    let server = Server::new(vec![
        (500, "{}".into()),
        (200, "not json".into()),
        (
            200,
            network_bundle("guardian_policy_config='retry success'"),
        ),
    ])
    .await;
    let home = TemporaryHome::new();
    let auth = TestAuth::new(&server.base);
    let service = new_service(auth.clone(), &home);
    assert!(service
        .fetch(auth.current().await.unwrap().unwrap())
        .await
        .is_ok());
    assert_eq!(server.requests.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn unrecoverable_auth_stops_without_request_retries_or_cache() {
    for recovery in [
        ManagedReviewCloudAuthRecovery::Unavailable,
        ManagedReviewCloudAuthRecovery::PermanentFailure("reauthentication required".into()),
    ] {
        let server = Server::new(vec![(401, "{}".into())]).await;
        let home = TemporaryHome::new();
        let auth = TestAuth::new(&server.base);
        auth.recoveries.lock().unwrap().push_back(recovery);
        let service = new_service(auth, &home);
        assert_eq!(
            service.load_startup().await.result.unwrap_err().code,
            CloudRequirementsErrorCode::Auth
        );
        assert_eq!(server.requests.lock().unwrap().len(), 1);
        assert!(!home.0.join("cloud-config-bundle-cache.json").exists());
    }
}

#[tokio::test]
async fn replaced_owner_cannot_publish_a_successful_inflight_response() {
    let server = Server::new(vec![(
        200,
        network_bundle("guardian_policy_config='old owner response'"),
    )])
    .await;
    let home = TemporaryHome::new();
    let auth = TestAuth::new(&server.base);
    auth.replace_owner_on_current.store(2, Ordering::SeqCst);
    let service = new_service(auth, &home);
    assert_eq!(
        service.load_startup().await.result.unwrap_err().code,
        CloudRequirementsErrorCode::StaleOwner
    );
    assert!(!home.0.join("cloud-config-bundle-cache.json").exists());
}

#[tokio::test]
async fn refresh_failure_keeps_valid_snapshot_and_success_replaces_future_loads() {
    let server = Server::new(vec![
        (200, network_bundle("guardian_policy_config='initial'")),
        (
            200,
            network_bundle("[auto_review]\nrequired_on_models=['invalid/model']"),
        ),
        (200, network_bundle("guardian_policy_config='refreshed'")),
    ])
    .await;
    let home = TemporaryHome::new();
    let auth = TestAuth::new(&server.base);
    let service = new_service(auth, &home);
    let initial = service
        .latest()
        .await
        .lock()
        .unwrap()
        .clone()
        .result
        .unwrap();
    assert!(service.refresh_once().await);
    assert_eq!(
        service
            .latest
            .get()
            .unwrap()
            .lock()
            .unwrap()
            .result
            .as_ref()
            .unwrap(),
        &initial
    );
    assert!(service.refresh_once().await);
    assert_eq!(
        service
            .latest
            .get()
            .unwrap()
            .lock()
            .unwrap()
            .result
            .as_ref()
            .unwrap()
            .requirements()[0]
            .toml,
        "guardian_policy_config='refreshed'"
    );
    assert_eq!(server.requests.lock().unwrap().len(), 3);
}

struct TimeoutThenAbsentAuth {
    calls: AtomicUsize,
}
#[async_trait]
impl ManagedReviewCloudAuthSource for TimeoutThenAbsentAuth {
    async fn current(&self) -> Result<Option<ManagedReviewCloudAuth>, String> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            std::future::pending().await
        } else {
            Ok(None)
        }
    }
    async fn recover_unauthorized(
        &self,
        _step: usize,
        _expected: &ManagedReviewCloudIdentity,
    ) -> ManagedReviewCloudAuthRecovery {
        panic!("no request was sent")
    }
}

#[tokio::test(start_paused = true)]
async fn startup_timeout_is_twenty_seconds_and_worker_recovers_after_five_seconds() {
    let home = TemporaryHome::new();
    let auth = Arc::new(TimeoutThenAbsentAuth {
        calls: AtomicUsize::new(0),
    });
    let provider = CloudManagedRequirementsProvider::new(auth.clone(), home.0.clone());
    assert_eq!(
        provider.snapshot_sync().unwrap_err().code,
        CloudRequirementsErrorCode::NotInitialized
    );
    let started = tokio::time::Instant::now();
    assert_eq!(
        provider.load().await.unwrap_err().code,
        CloudRequirementsErrorCode::Timeout
    );
    assert_eq!(started.elapsed(), Duration::from_secs(20));
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;
    assert_eq!(auth.calls.load(Ordering::SeqCst), 2);
    assert!(provider.refresh_task.is_finished());
}

#[tokio::test(start_paused = true)]
async fn provider_owns_initialization_and_cancels_background_lifetime_on_drop() {
    let home = TemporaryHome::new();
    let auth = TestAuth::new("http://127.0.0.1:1");
    auth.identity.lock().unwrap().as_mut().unwrap().plan_type = Some("team".into());
    let provider = CloudManagedRequirementsProvider::new(auth.clone(), home.0.clone());
    assert!(provider.load().await.unwrap().is_empty());
    assert!(provider.snapshot_sync().unwrap().is_empty());
    let handle = provider.refresh_task.abort_handle();
    let weak = Arc::downgrade(&provider.service);
    let calls = auth.calls.load(Ordering::SeqCst);
    drop(provider);
    tokio::task::yield_now().await;
    assert!(handle.is_finished());
    assert!(weak.upgrade().is_none());
    tokio::time::advance(Duration::from_secs(60 * 60)).await;
    assert_eq!(auth.calls.load(Ordering::SeqCst), calls);
}
