//! Deterministic grants use an isolated credential home and local OAuth transport.
use super::*;
use std::collections::HashMap;
use tokio::sync::Notify;

#[derive(Clone)]
struct Behavior {
    token_status: u16,
    token_body: String,
    profile_status: u16,
    profile_body: String,
    blocked: bool,
}

impl Default for Behavior {
    fn default() -> Self {
        Self {
            token_status: 200,
            token_body: json!({"access_token":"fixture-access","refresh_token":"fixture-refresh","token_type":"Bearer","expires_in":60}).to_string(),
            profile_status: 200,
            profile_body: json!({"email":"login@example.invalid","verified_email":true}).to_string(),
            blocked: false,
        }
    }
}

struct MockOAuth {
    endpoints: Endpoints,
    requests: Arc<Mutex<Vec<Value>>>,
    started: Arc<Notify>,
    release: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for MockOAuth {
    fn drop(&mut self) {
        self.release.send_replace(true);
        self.task.abort();
    }
}

impl MockOAuth {
    async fn new(behavior: Behavior) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let started = Arc::new(Notify::new());
        let (release, released) = watch::channel(!behavior.blocked);
        let trace = requests.clone();
        let notified = started.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let trace = trace.clone();
                let notified = notified.clone();
                let behavior = behavior.clone();
                let mut released = released.clone();
                tokio::spawn(async move {
                    let mut raw = Vec::new();
                    let (end, length) = loop {
                        let mut buf = [0u8; 2048];
                        let count = socket.read(&mut buf).await.unwrap();
                        if count == 0 {
                            return;
                        }
                        raw.extend_from_slice(&buf[..count]);
                        assert!(raw.len() <= MAX_RESPONSE);
                        if let Some(end) = raw.windows(4).position(|window| window == b"\r\n\r\n") {
                            let head = String::from_utf8_lossy(&raw[..end]);
                            let length = head
                                .lines()
                                .find_map(|line| {
                                    let (key, value) = line.split_once(':')?;
                                    key.eq_ignore_ascii_case("content-length")
                                        .then(|| value.trim().parse::<usize>().unwrap())
                                })
                                .unwrap_or(0);
                            break (end + 4, length);
                        }
                    };
                    while raw.len() < end + length {
                        let mut buf = [0u8; 2048];
                        let count = socket.read(&mut buf).await.unwrap();
                        if count == 0 {
                            return;
                        }
                        raw.extend_from_slice(&buf[..count]);
                        assert!(raw.len() <= MAX_RESPONSE);
                    }
                    let header = String::from_utf8_lossy(&raw[..end]);
                    let path = header
                        .lines()
                        .next()
                        .unwrap()
                        .split_whitespace()
                        .nth(1)
                        .unwrap()
                        .to_string();
                    let form: HashMap<String, String> =
                        url::form_urlencoded::parse(&raw[end..end + length])
                            .into_owned()
                            .collect();
                    trace
                        .lock()
                        .unwrap()
                        .push(json!({"path":path,"form":form,"header":header}));
                    let (status, body) = if path == "/token" {
                        notified.notify_one();
                        while !*released.borrow() {
                            if released.changed().await.is_err() {
                                return;
                            }
                        }
                        if form.get("grant_type").map(String::as_str) == Some("refresh_token") {
                            (200,json!({"access_token":"fixture-renewed","token_type":"Bearer","expires_in":3600}).to_string())
                        } else {
                            (behavior.token_status, behavior.token_body)
                        }
                    } else {
                        (behavior.profile_status, behavior.profile_body)
                    };
                    let response=format!("HTTP/1.1 {status} Reply\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len());
                    let _ = socket.write_all(response.as_bytes()).await;
                });
            }
        });
        Self {
            endpoints: Endpoints {
                token: format!("{base}/token"),
                userinfo: format!("{base}/userinfo"),
            },
            requests,
            started,
            release,
            task,
        }
    }
}

struct LoginFixture {
    _root: tempfile::TempDir,
    mock: MockOAuth,
    reply: Value,
    closed: Arc<AtomicBool>,
}

impl Drop for LoginFixture {
    fn drop(&mut self) {
        disconnect(7);
    }
}

fn credential(value: &str) -> auth::Credential {
    auth::Credential {
        access_token: value.into(),
        refresh_token: None,
        expires_at: None,
        account: None,
        auth_method: "consumer".into(),
        project: None,
        client_id: None,
        client_secret: None,
    }
}

impl LoginFixture {
    async fn new(behavior: Behavior, window: Duration, previous: Option<auth::Credential>) -> Self {
        let root = tempfile::tempdir().unwrap();
        super::super::set_config_root(root.path());
        if let Some(previous) = previous {
            auth::adopt(previous).await.unwrap();
        }
        let mock = MockOAuth::new(behavior).await;
        let closed = Arc::new(AtomicBool::new(false));
        let reply = start_at(7, closed.clone(), mock.endpoints.clone(), window).await;
        assert_eq!(reply["ok"], true);
        Self {
            _root: root,
            mock,
            reply,
            closed,
        }
    }
    fn id(&self) -> &str {
        self.reply["flowId"].as_str().unwrap()
    }
    fn query(&self) -> HashMap<String, String> {
        url::Url::parse(self.reply["authUrl"].as_str().unwrap())
            .unwrap()
            .query_pairs()
            .into_owned()
            .collect()
    }
    fn callback_url(&self) -> url::Url {
        let query = self.query();
        let mut url = url::Url::parse(&query["redirect_uri"]).unwrap();
        url.query_pairs_mut()
            .append_pair("state", &query["state"])
            .append_pair("code", "fixture-code");
        url
    }
    async fn callback(&self) -> reqwest::Response {
        cloud::http_client()
            .unwrap()
            .get(self.callback_url())
            .send()
            .await
            .unwrap()
    }
    async fn terminal(&self) -> Value {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let result = poll(7, self.id()).await;
                if result["pending"] != true {
                    return result;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap()
    }
}

#[tokio::test]
async fn browser_grant_preserves_pkce_and_private_refresh_credentials() {
    let f = LoginFixture::new(Behavior::default(), LOGIN_WINDOW, None).await;
    let query = f.query();
    assert_eq!(query["code_challenge_method"], "S256");
    assert_eq!(query["access_type"], "offline");
    assert_eq!(query["prompt"], "consent");
    assert_eq!(query["scope"].split(' ').collect::<Vec<_>>(), SCOPES);
    assert!(!query.contains_key("code_verifier"));
    assert!(!query.contains_key("client_secret"));
    let response = f.callback().await;
    assert_eq!(response.status(), 200);
    let result = f.terminal().await;
    assert_eq!(result["ok"], true);
    assert_eq!(result["signedIn"], true);
    assert_eq!(result["account"], "login@example.invalid");
    for forbidden in [
        "fixture-access",
        "fixture-refresh",
        "clientSecret",
        "code_verifier",
    ] {
        assert!(!result.to_string().contains(forbidden));
    }
    let requests = f.mock.requests.lock().unwrap().clone();
    let form = &requests[0]["form"];
    assert_eq!(form["redirect_uri"], query["redirect_uri"]);
    assert_eq!(form["grant_type"], "authorization_code");
    let verifier = form["code_verifier"].as_str().unwrap();
    assert_eq!(
        URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes())),
        query["code_challenge"]
    );
    assert!(requests[1]["header"]
        .as_str()
        .unwrap()
        .to_lowercase()
        .contains("authorization: bearer fixture-access"));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(auth::path())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
    let previous = std::env::var_os("KLAUS_ANTIGRAVITY_OAUTH_URL");
    klaus_agent_core::runtime_process::set_process_environment_var(
        "KLAUS_ANTIGRAVITY_OAUTH_URL",
        &f.mock.endpoints.token,
    );
    let refreshed = auth::credential(None).await.unwrap();
    match previous {
        Some(value) => klaus_agent_core::runtime_process::set_process_environment_var(
            "KLAUS_ANTIGRAVITY_OAUTH_URL",
            value,
        ),
        None => klaus_agent_core::runtime_process::remove_process_environment_var(
            "KLAUS_ANTIGRAVITY_OAUTH_URL",
        ),
    }
    assert_eq!(refreshed.access_token, "fixture-renewed");
    assert_eq!(refreshed.refresh_token.as_deref(), Some("fixture-refresh"));
    assert_eq!(
        f.mock.requests.lock().unwrap()[2]["form"]["grant_type"],
        "refresh_token"
    );
}

#[tokio::test]
async fn invalid_callbacks_do_not_consume_the_authorization() {
    let f = LoginFixture::new(Behavior::default(), LOGIN_WINDOW, None).await;
    let client = cloud::http_client().unwrap();
    let original = f.callback_url();
    let mut invalid = original.clone();
    invalid
        .query_pairs_mut()
        .clear()
        .append_pair("state", "wrong")
        .append_pair("code", "wrong");
    assert_eq!(client.get(invalid).send().await.unwrap().status(), 400);
    assert_eq!(
        client
            .post(original.clone())
            .body("ignored")
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    let mut wrong = original.clone();
    wrong.set_path("/wrong");
    assert_eq!(client.get(wrong).send().await.unwrap().status(), 404);
    let mut duplicate = original.clone();
    duplicate
        .query_pairs_mut()
        .append_pair("state", &f.query()["state"]);
    assert_eq!(client.get(duplicate).send().await.unwrap().status(), 400);
    assert_eq!(
        client
            .get(original)
            .header("Host", "example.invalid")
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    assert_eq!(poll(7, f.id()).await["pending"], true);
    assert!(f.mock.requests.lock().unwrap().is_empty());
    assert_eq!(f.callback().await.status(), 200);
    assert_eq!(f.terminal().await["ok"], true);
}

#[tokio::test]
async fn denial_and_failed_exchanges_preserve_the_previous_account() {
    for deny in [true, false] {
        let behavior = Behavior {
            token_status: 400,
            token_body:
                json!({"error":"invalid_grant","error_description":"private service detail"})
                    .to_string(),
            ..Default::default()
        };
        let f = LoginFixture::new(behavior, LOGIN_WINDOW, Some(credential("prior-account"))).await;
        if deny {
            let mut url = f.callback_url();
            url.query_pairs_mut()
                .clear()
                .append_pair("state", &f.query()["state"])
                .append_pair("error", "access_denied");
            assert_eq!(
                cloud::http_client()
                    .unwrap()
                    .get(url)
                    .send()
                    .await
                    .unwrap()
                    .status(),
                400
            );
        } else {
            assert_eq!(f.callback().await.status(), 400);
        }
        let result = f.terminal().await;
        assert_eq!(
            result["error"],
            if deny {
                "antigravity_login_denied"
            } else {
                "antigravity_login_failed"
            }
        );
        assert!(!result.to_string().contains("private service detail"));
        assert_eq!(
            auth::credential(None).await.unwrap().access_token,
            "prior-account"
        );
    }
}

#[tokio::test]
async fn malformed_and_oversized_oauth_responses_never_publish_credentials() {
    for body in [
        "not-json".to_string(),
        "x".repeat(MAX_RESPONSE + 1),
        json!({"access_token":"value","token_type":"not-bearer","expires_in":60}).to_string(),
    ] {
        let f = LoginFixture::new(
            Behavior {
                token_body: body,
                ..Default::default()
            },
            LOGIN_WINDOW,
            None,
        )
        .await;
        assert_eq!(f.callback().await.status(), 400);
        assert_eq!(
            f.terminal().await["error"],
            "antigravity_login_invalid_response"
        );
        assert_eq!(auth::status()["signedIn"], false);
    }
}

#[tokio::test]
async fn cancellation_and_account_replacement_fence_inflight_exchange() {
    for action in ["cancel", "adopt", "logout", "disconnect"] {
        let f = LoginFixture::new(
            Behavior {
                blocked: true,
                ..Default::default()
            },
            LOGIN_WINDOW,
            None,
        )
        .await;
        let url = f.callback_url();
        let callback =
            tokio::spawn(async move { cloud::http_client().unwrap().get(url).send().await });
        f.mock.started.notified().await;
        match action {
            "cancel" => {
                assert_eq!(cancel(7, f.id()).await["cancelled"], true);
            }
            "adopt" => {
                auth::adopt(credential("replacement-account"))
                    .await
                    .unwrap();
            }
            "logout" => {
                auth::logout().await.unwrap();
            }
            _ => {
                f.closed.store(true, Ordering::Release);
                disconnect(7);
            }
        }
        f.mock.release.send_replace(true);
        let _ = callback.await;
        let result = f.terminal().await;
        assert_eq!(result["ok"], false);
        assert_eq!(auth::status()["signedIn"], action == "adopt");
        if action == "adopt" {
            assert_eq!(
                auth::credential(None).await.unwrap().access_token,
                "replacement-account"
            );
        }
    }
}

#[tokio::test]
async fn cancel_after_completion_removes_only_its_own_credential() {
    let f = LoginFixture::new(Behavior::default(), LOGIN_WINDOW, None).await;
    assert_eq!(f.callback().await.status(), 200);
    assert_eq!(f.terminal().await["signedIn"], true);
    assert_eq!(cancel(8, f.id()).await["ok"], true);
    assert_eq!(auth::status()["signedIn"], true);
    assert_eq!(cancel(7, f.id()).await["cancelled"], true);
    assert_eq!(auth::status()["signedIn"], false);
    let f = LoginFixture::new(Behavior::default(), LOGIN_WINDOW, None).await;
    assert_eq!(f.callback().await.status(), 200);
    assert_eq!(f.terminal().await["signedIn"], true);
    auth::adopt(credential("replacement-account"))
        .await
        .unwrap();
    assert_eq!(cancel(7, f.id()).await["cancelled"], true);
    assert_eq!(
        auth::credential(None).await.unwrap().access_token,
        "replacement-account"
    );
}

#[tokio::test]
async fn superseded_flow_cancellation_cannot_stop_the_new_login() {
    let mut f = LoginFixture::new(Behavior::default(), LOGIN_WINDOW, None).await;
    let old_id = f.id().to_string();
    let replacement = start_at(7, f.closed.clone(), f.mock.endpoints.clone(), LOGIN_WINDOW).await;
    assert_eq!(
        poll(7, &old_id).await["error"],
        "antigravity_login_cancelled"
    );
    assert_eq!(cancel(7, &old_id).await["ok"], true);
    f.reply = replacement;
    assert_eq!(poll(7, f.id()).await["pending"], true);
    assert_eq!(f.callback().await.status(), 200);
    assert_eq!(f.terminal().await["ok"], true);
}

#[tokio::test]
async fn expiry_and_closed_connection_release_the_callback_listener() {
    let f = LoginFixture::new(Behavior::default(), Duration::from_millis(20), None).await;
    tokio::time::sleep(Duration::from_millis(35)).await;
    assert_eq!(f.terminal().await["error"], "antigravity_login_expired");
    assert!(cloud::http_client()
        .unwrap()
        .get(f.callback_url())
        .timeout(Duration::from_secs(1))
        .send()
        .await
        .is_err());
    f.closed.store(true, Ordering::Release);
    assert_eq!(
        start_at(7, f.closed.clone(), f.mock.endpoints.clone(), LOGIN_WINDOW).await["error"],
        "antigravity_login_cancelled"
    );
}

#[test]
fn oauth_transport_and_flow_contracts_reject_untrusted_shapes() {
    for endpoint in [
        "https://evil.invalid/token",
        "http://oauth2.googleapis.com/token",
        "https://oauth2.googleapis.com/wrong",
        "https://oauth2.googleapis.com/token?next=evil",
        "https://user:password@oauth2.googleapis.com/token",
    ] {
        assert!(!trusted_endpoint(
            endpoint,
            "oauth2.googleapis.com",
            "/token"
        ));
    }
    assert!(trusted_endpoint(
        TOKEN_URL,
        "oauth2.googleapis.com",
        "/token"
    ));
    assert!(trusted_endpoint(
        USERINFO_URL,
        "www.googleapis.com",
        "/oauth2/v2/userinfo"
    ));
    use crate::app_server::wire::{AntigravityLoginFlowParams, AntigravityLoginStartParams};
    assert!(serde_json::from_value::<AntigravityLoginStartParams>(
        json!({"redirectUri":"https://evil.invalid"})
    )
    .is_err());
    assert!(serde_json::from_value::<AntigravityLoginFlowParams>(
        json!({"flow":{"flowId":"flow","verifier":"private"}})
    )
    .is_err());
    let bundle = crate::app_server::schema_bundle();
    for method in [
        "antigravity/loginStart",
        "antigravity/loginPoll",
        "antigravity/loginCancel",
    ] {
        assert!(bundle["requests"].get(method).is_some());
    }
}

#[tokio::test]
async fn transient_token_and_profile_failures_never_replace_the_previous_account() {
    for behavior in [
        Behavior {
            token_status: 503,
            token_body: "temporarily unavailable".into(),
            ..Default::default()
        },
        Behavior {
            token_status: 429,
            token_body: "rate limited".into(),
            ..Default::default()
        },
        Behavior {
            profile_status: 403,
            profile_body: "forbidden".into(),
            ..Default::default()
        },
        Behavior {
            profile_body: "not JSON".into(),
            ..Default::default()
        },
    ] {
        let expected = if behavior.token_status != 200 {
            "antigravity_login_network_error"
        } else if behavior.profile_status != 200 {
            "antigravity_login_failed"
        } else {
            "antigravity_login_invalid_response"
        };
        let f = LoginFixture::new(behavior, LOGIN_WINDOW, Some(credential("prior-account"))).await;
        assert_eq!(f.callback().await.status(), 400);
        assert_eq!(f.terminal().await["error"], expected);
        assert_eq!(
            auth::credential(None).await.unwrap().access_token,
            "prior-account"
        );
    }
}

#[test]
fn browser_sign_in_requires_a_complete_build_time_client() {
    use super::super::auth::{configured_client, CLIENT_UNCONFIGURED};
    assert_eq!(configured_client(None, None), Err(CLIENT_UNCONFIGURED));
    assert_eq!(
        configured_client(Some("id"), None),
        Err(CLIENT_UNCONFIGURED)
    );
    assert_eq!(
        configured_client(None, Some("secret")),
        Err(CLIENT_UNCONFIGURED)
    );
    assert_eq!(
        configured_client(Some(" "), Some("secret")),
        Err(CLIENT_UNCONFIGURED)
    );
    assert_eq!(
        configured_client(Some("id"), Some("secret")),
        Ok(("id", "secret"))
    );
}
