//! Defect: a subscription queue stayed on a rate-limited account despite spare
//! credentials, and exhausted queues could restart inside the same turn.
//! Root cause: auth and catalog decorators dropped credential-recovery capabilities.
//! Fixing commit: bb313dbb.
//! Follow-up: rebuilding an auth source discarded refreshed credentials, and
//! concurrent initial preparation rejected the losing seed CAS instead of adopting it.

use klaus_agent_core::credential_store::{
    CredentialStore, CredentialStoreError, NamespacedCredentialStore,
};
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, Session, TurnOptions, TurnResult};
use klaus_agent_core::mcp::credential_store::InMemoryCredentialStore;
use klaus_agent_core::provider::anthropic::{AnthropicProvider, AnthropicProviderOptions};
use klaus_agent_core::provider::auth::AuthSource;
use klaus_agent_core::provider::oauth::{
    HttpFetch, HttpRequest, HttpResponse, OAuthAuth, OAuthAuthOptions, OAuthConfig, OAuthTokens,
};
use klaus_agent_core::provider::retry::RetryOptions;
use klaus_agent_core::provider::rotation::{
    MultiAccountAuthSource, RotationAccountSpec, RotationInit,
};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{ProviderErrorKind, TerminalReason};
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

const TURN_DEADLINE: Duration = Duration::from_secs(3);
const CLEANUP_DEADLINE: Duration = Duration::from_secs(1);
const FINAL_TEXT: &str = "The account rotation fixture completed.";
const SSE_SUCCESS: &str = concat!(
    "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_rotation\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"rotation-fixture\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\n",
    "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
    "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"The account rotation fixture completed.\"}}\n\n",
    "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
    "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",\"stop_sequence\":null},\"usage\":{\"output_tokens\":7}}\n\n",
    "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
);

struct FixtureRoot(PathBuf);

impl FixtureRoot {
    fn new() -> Self {
        let root =
            std::env::temp_dir().join(format!("klaus-claude-rotation-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        Self(root)
    }
}

impl Drop for FixtureRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[derive(Clone, Copy)]
pub(super) enum Reply {
    RateLimited,
    Success,
    Unauthorized,
}

#[derive(Clone)]
struct CapturedRequest {
    account: String,
    body: Value,
    persisted_order: Option<Vec<String>>,
}

#[derive(Default)]
struct ServerState {
    refresh_requests: usize,
    replies: VecDeque<(&'static str, Reply)>,
    requests: Vec<CapturedRequest>,
}

fn persisted_order(path: &Path) -> Vec<String> {
    let document: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    document["accounts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|account| account["id"].as_str().unwrap().to_owned())
        .collect()
}

async fn read_request(socket: &mut TcpStream) -> (String, Value) {
    let mut received = Vec::new();
    let mut framing = None;
    loop {
        let mut chunk = [0_u8; 4096];
        let read = socket.read(&mut chunk).await.unwrap();
        assert_ne!(read, 0, "the fixture request must complete before EOF");
        received.extend_from_slice(&chunk[..read]);
        assert!(
            received.len() < 2 * 1024 * 1024,
            "fixture request size is bounded"
        );
        if framing.is_none() {
            if let Some(end) = received
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|position| position + 4)
            {
                let headers = std::str::from_utf8(&received[..end]).unwrap();
                assert!(
                    headers.starts_with("POST /v1/messages ")
                        || headers.starts_with("POST /never-refresh "),
                    "only inference and token renewal reach the fixture"
                );
                let length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                    .expect("the fixture accepts an explicitly framed JSON body");
                framing = Some((end, length));
            }
        }
        if let Some((end, length)) = framing {
            if received.len() >= end + length {
                let headers = std::str::from_utf8(&received[..end]).unwrap();
                if headers.starts_with("POST /never-refresh ") {
                    let form = std::str::from_utf8(&received[end..end + length]).unwrap();
                    assert!(form.contains("refresh_token=fixture-refresh-a"));
                    return ("refresh".to_owned(), Value::Null);
                }
                let mut account = "no-auth".to_owned();
                for line in headers.lines() {
                    let Some((name, value)) = line.split_once(':') else {
                        continue;
                    };
                    if name.eq_ignore_ascii_case("authorization") {
                        account = value
                            .trim()
                            .strip_prefix("Bearer fixture-token-")
                            .expect("only synthetic OAuth tokens are sent")
                            .to_owned();
                    } else if name.eq_ignore_ascii_case("x-api-key") {
                        assert_eq!(value.trim(), "fixture-api-key");
                        account = "api-key".to_owned();
                    }
                }
                return (
                    account,
                    serde_json::from_slice(&received[end..end + length]).unwrap(),
                );
            }
        }
    }
}

async fn serve(
    listener: TcpListener,
    state: Arc<Mutex<ServerState>>,
    account_file: Option<PathBuf>,
) {
    loop {
        let (mut socket, _) = listener.accept().await.unwrap();
        let (account, body) = read_request(&mut socket).await;
        if account == "refresh" {
            state.lock().unwrap().refresh_requests += 1;
            let body = r#"{"error":"invalid_grant"}"#;
            let response = format!(
                "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            continue;
        }
        let reply = {
            let mut state = state.lock().unwrap();
            state.requests.push(CapturedRequest {
                account: account.clone(),
                body,
                persisted_order: account_file.as_deref().map(persisted_order),
            });
            state.replies.pop_front()
        };
        let (status, content_type, response) = match reply {
            Some((expected, _)) if account != expected => (
                "400 Bad Request",
                "application/json",
                r#"{"type":"error","error":{"type":"invalid_request_error","message":"unexpected fixture account"}}"#,
            ),
            Some((_, Reply::Success)) => ("200 OK", "text/event-stream", SSE_SUCCESS),
            Some((_, Reply::Unauthorized)) => (
                "401 Unauthorized",
                "application/json",
                r#"{"type":"error","error":{"type":"authentication_error","message":"synthetic account is unauthorized"}}"#,
            ),
            Some((_, Reply::RateLimited)) | None => (
                "429 Too Many Requests",
                "application/json",
                r#"{"type":"error","error":{"type":"rate_limit_error","message":"synthetic account allowance is exhausted"}}"#,
            ),
        };
        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",
            response.len()
        );
        socket.write_all(response.as_bytes()).await.unwrap();
    }
}

struct RefreshFixture;

struct RacingStore {
    inner: InMemoryCredentialStore,
    arrivals: tokio::sync::Barrier,
    reads: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl CredentialStore for RacingStore {
    async fn get(&self, key: &str) -> Result<Option<String>, CredentialStoreError> {
        let value = self.inner.get(key).await?;
        if value.is_none() && self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst) < 2 {
            self.arrivals.wait().await;
        }
        Ok(value)
    }

    async fn set(&self, key: &str, value: &str) -> Result<(), CredentialStoreError> {
        self.inner.set(key, value).await
    }

    async fn delete(&self, key: &str) -> Result<(), CredentialStoreError> {
        self.inner.delete(key).await
    }

    async fn compare_exchange(
        &self,
        key: &str,
        expected: Option<&str>,
        replacement: &str,
    ) -> Result<bool, CredentialStoreError> {
        self.inner
            .compare_exchange(key, expected, replacement)
            .await
    }
}

#[tokio::test]
async fn concurrent_rotation_sources_adopt_the_initial_credential_cas_winner() {
    let root = FixtureRoot::new();
    let store: Arc<dyn CredentialStore> = Arc::new(RacingStore {
        inner: InMemoryCredentialStore::new(),
        arrivals: tokio::sync::Barrier::new(2),
        reads: std::sync::atomic::AtomicUsize::new(0),
    });
    let source = || {
        MultiAccountAuthSource::new(RotationInit {
            file_path: root
                .0
                .join("unused-queue.json")
                .to_string_lossy()
                .into_owned(),
            config: OAuthConfig {
                client_id: "initial-cas-fixture".into(),
                ..Default::default()
            },
            base_url: None,
            extra_headers: HashMap::new(),
            store: store.clone(),
            accounts: vec![RotationAccountSpec {
                id: "a".into(),
                credential_id: "subscription-initial-cas".into(),
                tokens: OAuthTokens {
                    access_token: "fixture-initial-access".into(),
                    ..Default::default()
                },
            }],
        })
        .unwrap()
    };
    let first = source();
    let second = source();
    let signal = AbortSignal::new();
    let (first, second) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(first.get_headers(&signal), second.get_headers(&signal))
    })
    .await
    .unwrap();
    assert_eq!(
        first.unwrap()["Authorization"],
        "Bearer fixture-initial-access"
    );
    assert_eq!(
        second.unwrap()["Authorization"],
        "Bearer fixture-initial-access"
    );
}

#[async_trait::async_trait]
impl HttpFetch for RefreshFixture {
    async fn fetch(
        &self,
        _url: &str,
        request: HttpRequest,
    ) -> Result<HttpResponse, klaus_agent_core::types::errors::ProviderError> {
        assert!(request.body.contains("refresh_token=fixture-login-refresh"));
        Ok(HttpResponse {
            ok: true,
            status: 200,
            body: json!({
                "access_token": "fixture-refreshed-access",
                "refresh_token": "fixture-refreshed-refresh",
                "expires_in": 3600,
            })
            .to_string(),
        })
    }
}

#[tokio::test]
async fn rebuilding_rotation_auth_preserves_refreshed_credentials_over_the_login_seed() {
    let root = FixtureRoot::new();
    let store: Arc<dyn CredentialStore> = Arc::new(InMemoryCredentialStore::new());
    let config = OAuthConfig {
        client_id: "rotation-refresh-fixture".into(),
        token_url: "https://fixture.invalid/token".into(),
        ..Default::default()
    };
    let credential_id = "subscription-fixture-login";
    let namespace: Arc<dyn CredentialStore> = Arc::new(NamespacedCredentialStore::new(
        store.clone(),
        format!("provider/anthropic/{credential_id}"),
    ));
    let login_seed = OAuthTokens {
        access_token: "fixture-login-access".into(),
        refresh_token: Some("fixture-login-refresh".into()),
        expires_at: None,
        id_token: None,
    };
    let original = OAuthAuth::new(OAuthAuthOptions {
        config: config.clone(),
        store: Some(namespace.clone()),
        tokens: Some(login_seed.clone()),
        fetch: Some(Arc::new(RefreshFixture)),
        refresh_lock_key: Some(format!("provider-anthropic-{credential_id}")),
        refresh_lock_dir: Some(root.0.to_string_lossy().into_owned()),
        ..Default::default()
    });
    original.save().await.unwrap();
    assert!(original.force_refresh().await);
    let refreshed = OAuthAuth::capture_stored(&namespace, &config)
        .await
        .unwrap()
        .unwrap()
        .tokens();
    assert_ne!(refreshed, login_seed);
    for _ in 0..2 {
        let rebuilt = MultiAccountAuthSource::new(RotationInit {
            file_path: root
                .0
                .join("unused-queue.json")
                .to_string_lossy()
                .into_owned(),
            config: config.clone(),
            base_url: None,
            extra_headers: HashMap::new(),
            store: store.clone(),
            accounts: vec![RotationAccountSpec {
                id: "a".into(),
                credential_id: credential_id.into(),
                tokens: login_seed.clone(),
            }],
        })
        .unwrap();
        let headers = AuthSource::get_headers(&rebuilt, &AbortSignal::new())
            .await
            .unwrap();
        assert_eq!(headers["Authorization"], "Bearer fixture-refreshed-access");
        assert_eq!(rebuilt.current_account_id(), "a");
        assert_eq!(
            OAuthAuth::capture_stored(&namespace, &config)
                .await
                .unwrap()
                .unwrap()
                .tokens(),
            refreshed,
        );
    }
}

pub(super) enum AuthMode {
    Multi(&'static [&'static str]),
    MultiExpired(&'static [&'static str]),
    SingleOAuth,
    StaticApiKey,
    NoAuth,
}

pub(super) struct Fixture {
    root: FixtureRoot,
    session: Arc<Session>,
    process: RuntimeProcessContext,
    server: JoinHandle<()>,
    state: Arc<Mutex<ServerState>>,
    queue: Option<Arc<MultiAccountAuthSource>>,
    account_file: Option<PathBuf>,
}

impl Fixture {
    pub(super) async fn new(mode: AuthMode) -> Self {
        let root = FixtureRoot::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let store = Arc::new(InMemoryCredentialStore::new());
        let config = OAuthConfig {
            client_id: "rotation-fixture-client".into(),
            authorize_url: format!("{base_url}/never-authorize"),
            token_url: format!("{base_url}/never-refresh"),
            redirect_uri: format!("{base_url}/never-callback"),
            ..Default::default()
        };
        let tokens = |account: &str| OAuthTokens {
            access_token: format!("fixture-token-{account}").into(),
            expires_at: (matches!(mode, AuthMode::MultiExpired(_)) && account == "a").then_some(1),
            refresh_token: (matches!(mode, AuthMode::MultiExpired(_)) && account == "a")
                .then(|| "fixture-refresh-a".into()),
            id_token: None,
        };
        let mut queue = None;
        let mut account_file = None;
        let auth_source: Option<Arc<dyn AuthSource>> = match &mode {
            AuthMode::Multi(accounts) | AuthMode::MultiExpired(accounts) => {
                let path = root.0.join("claude-accounts.json");
                let entries: Vec<Value> = accounts
                    .iter()
                    .map(|id| json!({"id":id, "fixtureMarker":format!("record-{id}")}))
                    .collect();
                std::fs::write(
                    &path,
                    serde_json::to_vec(&json!({"version":1, "accounts":entries})).unwrap(),
                )
                .unwrap();
                let source = Arc::new(
                    MultiAccountAuthSource::new(RotationInit {
                        file_path: path.to_string_lossy().into_owned(),
                        config,
                        base_url: Some(base_url.clone()),
                        extra_headers: HashMap::new(),
                        store: store.clone(),
                        accounts: accounts
                            .iter()
                            .map(|id| RotationAccountSpec {
                                id: (*id).into(),
                                credential_id: format!("fixture-subscription-{id}"),
                                tokens: tokens(id),
                            })
                            .collect(),
                    })
                    .unwrap(),
                );
                account_file = Some(path);
                queue = Some(source.clone());
                Some(source)
            }
            AuthMode::SingleOAuth => Some(Arc::new(OAuthAuth::new(OAuthAuthOptions {
                config,
                store: Some(store.clone()),
                tokens: Some(tokens("a")),
                base_url: Some(base_url.clone()),
                refresh_lock_dir: Some(root.0.join("refresh-locks").to_string_lossy().into_owned()),
                ..Default::default()
            }))),
            AuthMode::StaticApiKey | AuthMode::NoAuth => None,
        };
        let is_multi = matches!(mode, AuthMode::Multi(_) | AuthMode::MultiExpired(_));
        let provider = Arc::new(AnthropicProvider::new(AnthropicProviderOptions {
            base_url: Some(base_url),
            default_model: Some("rotation-fixture".into()),
            api_key: matches!(mode, AuthMode::StaticApiKey).then(|| "fixture-api-key".into()),
            auth_source,
            // Multi-account cases retain the real five-minute congestion hold.
            // Only controls with no rotation use a bounded ordinary retry policy.
            retry: (!is_multi).then(|| RetryOptions {
                max_attempts: Some(1),
                congestion_hold_ms: Some(0),
                unbounded_network_retries: Some(false),
                ..Default::default()
            }),
            ..Default::default()
        }));
        let process = RuntimeProcessContext::initialize(
            super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
        )
        .unwrap();
        let mut options =
            super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
        options.provider = klaus_agent_core::provider::model_catalog::with_model_catalog(
            provider,
            klaus_agent_core::provider::model_catalog::ModelCatalog::empty(),
        );
        options.credential_store = Some(store);
        options.cwd = Some(root.0.to_string_lossy().into_owned());
        options
            .config
            .as_mut()
            .unwrap()
            .as_object_mut()
            .unwrap()
            .extend(
                json!({
                    "model":"rotation-fixture", "maxOutputTokens":64,
                    "maxRetries":if is_multi {5} else {0}, "worktree":false,
                    "autoMemory":false, "agentMemory":false, "prewarm":false,
                    "idleMicrocompact":false, "earlyMicrocompact":false,
                    "speculativeCompaction":false, "loopDetection":false
                })
                .as_object()
                .unwrap()
                .clone(),
            );
        let rollout_store = Arc::new(
            klaus_agent_core::store::jsonl_store::JsonlSessionStore::new(
                klaus_agent_core::store::jsonl_store::JsonlSessionStoreOptions {
                    dir: Some(root.0.join("sessions").to_string_lossy().into_owned()),
                    archive_dir: None,
                },
            ),
        );
        let session = Agent::new(options)
            .open_session(
                rollout_store,
                StartSessionOptions {
                    cwd: Some(root.0.to_string_lossy().into_owned().into()),
                    user_config_dir: Some(root.0.to_string_lossy().into_owned().into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let state = Arc::new(Mutex::new(ServerState::default()));
        let server = tokio::spawn(serve(listener, state.clone(), account_file.clone()));
        Self {
            root,
            session,
            process,
            server,
            state,
            queue,
            account_file,
        }
    }

    pub(super) fn plan(&self, replies: &[(&'static str, Reply)]) {
        let mut state = self.state.lock().unwrap();
        assert!(
            state.replies.is_empty(),
            "the preceding turn consumed its plan"
        );
        state.replies.extend(replies.iter().copied());
    }

    pub(super) async fn run(&self) -> TurnResult {
        let signal = AbortSignal::new();
        let result = tokio::time::timeout(
            TURN_DEADLINE,
            self.session.run(
                Input::Text("Complete the synthetic rotation check.".into()),
                TurnOptions {
                    signal: Some(signal.clone()),
                    ..Default::default()
                },
            ),
        )
        .await;
        if result.is_err() {
            signal.cancel();
            let _ = tokio::time::timeout(CLEANUP_DEADLINE, self.session.shutdown_and_wait()).await;
            let _ = tokio::time::timeout(CLEANUP_DEADLINE, self.process.shutdown()).await;
        }
        result.expect("the engine turn must settle within three seconds without a congestion hold")
    }

    pub(super) fn refresh_requests(&self) -> usize {
        self.state.lock().unwrap().refresh_requests
    }

    pub(super) fn auth_failure_kinds(&self) -> Vec<String> {
        self.session
            .turn_index_snapshot()
            .turn_error_kinds
            .values()
            .map(|kind| kind.try_to_utf8().unwrap())
            .collect()
    }

    fn requests(&self) -> Vec<CapturedRequest> {
        self.state.lock().unwrap().requests.clone()
    }

    pub(super) fn assert_trace(&self, expected: &[&str]) {
        let requests = self.requests();
        let actual: Vec<&str> = requests
            .iter()
            .map(|request| request.account.as_str())
            .collect();
        assert_eq!(
            actual, expected,
            "each turn retry spends the selected account"
        );
        assert!(self.state.lock().unwrap().replies.is_empty());
    }

    pub(super) fn assert_order(&self, expected: &[&str]) {
        let path = self.account_file.as_ref().unwrap();
        assert!(path.starts_with(&self.root.0));
        assert_eq!(persisted_order(path), expected);
        assert_eq!(
            self.queue.as_ref().unwrap().current_account_id(),
            expected[0]
        );
        let document: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(document["version"], 1);
        for account in document["accounts"].as_array().unwrap() {
            let id = account["id"].as_str().unwrap();
            assert_eq!(account["fixtureMarker"], format!("record-{id}"));
        }
    }

    pub(super) async fn shutdown(&self) {
        self.session.shutdown_and_wait().await.unwrap();
        self.process.shutdown().await.unwrap();
        self.server.abort();
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

pub(super) fn assert_success(result: &TurnResult) {
    assert!(result.error.is_none(), "the public turn succeeds");
    assert_eq!(result.stop_reason, TerminalReason::Completed);
    assert_eq!(result.final_text.try_to_utf8().unwrap(), FINAL_TEXT);
}

fn assert_rate_limit_terminal(result: &TurnResult) {
    assert_eq!(result.stop_reason, TerminalReason::Error);
    let error = result
        .error
        .as_ref()
        .expect("the failed turn retains its error");
    let provider = error
        .provider_error()
        .expect("the failure retains provider metadata");
    assert_eq!(provider.kind, ProviderErrorKind::RateLimit);
    assert_eq!(provider.status, Some(429));
    assert!(
        result.final_text.is_empty(),
        "a quota failure is not success"
    );
}

#[tokio::test]
async fn auth_source_only_rotates_from_exhausted_a_to_successful_b() {
    let fixture = Fixture::new(AuthMode::Multi(&["a", "b"])).await;
    fixture.plan(&[("a", Reply::RateLimited), ("b", Reply::Success)]);
    assert_success(&fixture.run().await);
    fixture.assert_trace(&["a", "b"]);
    fixture.assert_order(&["b", "a"]);
    let requests = fixture.requests();
    assert_eq!(requests[0].body["messages"], requests[1].body["messages"]);
    fixture.shutdown().await;
}

#[tokio::test]
async fn auth_source_only_tries_a_then_b_then_successful_c_without_skipping() {
    let fixture = Fixture::new(AuthMode::Multi(&["a", "b", "c"])).await;
    fixture.plan(&[
        ("a", Reply::RateLimited),
        ("b", Reply::RateLimited),
        ("c", Reply::Success),
    ]);
    assert_success(&fixture.run().await);
    fixture.assert_trace(&["a", "b", "c"]);
    fixture.assert_order(&["c", "a", "b"]);
    let requests = fixture.requests();
    for request in &requests {
        assert_eq!(request.body["messages"], requests[0].body["messages"]);
    }
    fixture.shutdown().await;
}

#[tokio::test]
async fn exhausting_every_account_is_terminal_with_a_bounded_cycle() {
    let fixture = Fixture::new(AuthMode::Multi(&["a", "b", "c"])).await;
    assert_rate_limit_terminal(&fixture.run().await);
    fixture.assert_trace(&["a", "b", "c", "a"]);
    fixture.assert_order(&["a", "b", "c"]);
    fixture.plan(&[("a", Reply::RateLimited), ("b", Reply::Success)]);
    assert_success(&fixture.run().await);
    fixture.assert_trace(&["a", "b", "c", "a", "a", "b"]);
    fixture.assert_order(&["b", "c", "a"]);
    fixture.shutdown().await;
}

#[tokio::test]
async fn successful_response_resets_the_anchor_and_persists_each_later_rotation() {
    let fixture = Fixture::new(AuthMode::Multi(&["a", "b", "c"])).await;
    fixture.plan(&[("a", Reply::RateLimited), ("b", Reply::Success)]);
    assert_success(&fixture.run().await);
    fixture.assert_order(&["b", "c", "a"]);
    fixture.plan(&[
        ("b", Reply::RateLimited),
        ("c", Reply::RateLimited),
        ("a", Reply::RateLimited),
        ("b", Reply::Success),
    ]);
    assert_success(&fixture.run().await);
    fixture.assert_trace(&["a", "b", "b", "c", "a", "b"]);
    fixture.assert_order(&["b", "c", "a"]);
    let orders: Vec<Vec<String>> = fixture
        .requests()
        .into_iter()
        .map(|request| request.persisted_order.unwrap())
        .collect();
    assert_eq!(
        orders,
        vec![
            vec!["a", "b", "c"],
            vec!["b", "c", "a"],
            vec!["b", "c", "a"],
            vec!["c", "a", "b"],
            vec!["a", "b", "c"],
            vec!["b", "c", "a"],
        ]
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_plain_single_account_oauth_source_does_not_rotate() {
    let fixture = Fixture::new(AuthMode::SingleOAuth).await;
    fixture.plan(&[("a", Reply::RateLimited)]);
    assert_rate_limit_terminal(&fixture.run().await);
    fixture.assert_trace(&["a"]);
    assert!(fixture.queue.is_none());
    fixture.shutdown().await;
}

#[tokio::test]
async fn missing_auth_and_static_api_keys_do_not_create_a_rotation_queue() {
    for (mode, account) in [
        (AuthMode::NoAuth, "no-auth"),
        (AuthMode::StaticApiKey, "api-key"),
    ] {
        let fixture = Fixture::new(mode).await;
        fixture.plan(&[(account, Reply::RateLimited)]);
        assert_rate_limit_terminal(&fixture.run().await);
        fixture.assert_trace(&[account]);
        assert!(fixture.queue.is_none());
        fixture.shutdown().await;
    }
}
