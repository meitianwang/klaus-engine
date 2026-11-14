//! Browser authorization with private PKCE state and generation-bound publication.

use super::{auth, cloud, config_root};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use rand::RngCore;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, OnceLock,
};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

const AUTHORIZE_URL: &str = "https://accounts.google.com/o/oauth2/auth";
const TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
const USERINFO_URL: &str = "https://www.googleapis.com/oauth2/v2/userinfo";
const CALLBACK_PATH: &str = "/auth/callback";
const LOGIN_WINDOW: Duration = Duration::from_secs(300);
const MAX_HEADER: usize = 16 * 1024;
const MAX_RESPONSE: usize = 64 * 1024;
const MAX_FLOWS: usize = 16;
const SCOPES: &[&str] = &[
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/cclog",
    "https://www.googleapis.com/auth/experimentsandconfigs",
    "https://www.googleapis.com/auth/aicode",
];

#[derive(Clone)]
enum Outcome {
    Pending,
    Complete {
        generation: String,
        updated_existing: bool,
        account_id: String,
    },
    Failed(&'static str),
}

struct Flow {
    id: String,
    owner: usize,
    root: PathBuf,
    deadline: Instant,
    cancelled: watch::Sender<bool>,
    outcome: Outcome,
}

fn flows() -> &'static Mutex<VecDeque<Flow>> {
    static FLOWS: OnceLock<Mutex<VecDeque<Flow>>> = OnceLock::new();
    FLOWS.get_or_init(|| Mutex::new(VecDeque::new()))
}

fn fail(flow: &mut Flow, error: &'static str) {
    flow.outcome = Outcome::Failed(error);
    flow.cancelled.send_replace(true);
}

/// Account replacement invalidates every pending exchange before publication.
pub(super) fn invalidate_pending() {
    let mut flows = flows().lock().unwrap_or_else(|p| p.into_inner());
    for flow in flows.iter_mut() {
        if matches!(flow.outcome, Outcome::Pending) {
            fail(flow, "antigravity_login_credential_changed");
        }
    }
}

/// Connection teardown cancels only its own authorization listeners.
pub fn disconnect(owner: usize) {
    let mut flows = flows().lock().unwrap_or_else(|p| p.into_inner());
    for flow in flows.iter_mut().filter(|flow| flow.owner == owner) {
        if matches!(flow.outcome, Outcome::Pending) {
            fail(flow, "antigravity_login_cancelled");
        }
    }
}

/// Dropping an endpoint future must also fence authorization callbacks.
pub(crate) struct ConnectionGuard {
    owner: usize,
    closed: Arc<AtomicBool>,
}

impl ConnectionGuard {
    pub(crate) fn new(owner: usize, closed: Arc<AtomicBool>) -> Self {
        Self { owner, closed }
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
        disconnect(self.owner);
    }
}

#[derive(Clone)]
struct Endpoints {
    token: String,
    userinfo: String,
}

fn trusted_endpoint(endpoint: &str, host: &str, path: &str) -> bool {
    let Ok(url) = url::Url::parse(endpoint) else {
        return false;
    };
    let local = matches!(
        url.host_str(),
        Some("127.0.0.1" | "localhost" | "[::1]" | "::1")
    );
    (url.scheme() == "https"
        && url.host_str() == Some(host)
        && url.port_or_known_default() == Some(443)
        && url.path() == path
        || url.scheme() == "http" && local)
        && url.username().is_empty()
        && url.password().is_none()
        && url.query().is_none()
        && url.fragment().is_none()
}

impl Endpoints {
    fn configured() -> Result<Self, &'static str> {
        let endpoints = Self {
            token: std::env::var("KLAUS_ANTIGRAVITY_OAUTH_URL")
                .unwrap_or_else(|_| TOKEN_URL.into()),
            userinfo: std::env::var("KLAUS_ANTIGRAVITY_USERINFO_URL")
                .unwrap_or_else(|_| USERINFO_URL.into()),
        };
        if !trusted_endpoint(&endpoints.token, "oauth2.googleapis.com", "/token")
            || !trusted_endpoint(
                &endpoints.userinfo,
                "www.googleapis.com",
                "/oauth2/v2/userinfo",
            )
        {
            return Err("antigravity_login_endpoint_invalid");
        }
        Ok(endpoints)
    }
}

fn failure(error: &str) -> Value {
    json!({"ok":false,"error":error})
}

pub async fn start(owner: usize, closed: Arc<AtomicBool>) -> Value {
    let endpoints = match Endpoints::configured() {
        Ok(endpoints) => endpoints,
        Err(error) => return failure(error),
    };
    start_at(owner, closed, endpoints, LOGIN_WINDOW).await
}

async fn start_at(
    owner: usize,
    closed: Arc<AtomicBool>,
    endpoints: Endpoints,
    window: Duration,
) -> Value {
    let client_id = match auth::oauth_client() {
        Ok((client_id, _)) => client_id,
        Err(error) => return failure(error),
    };
    let mut random = [0u8; 64];
    if rand::rngs::OsRng.try_fill_bytes(&mut random).is_err() {
        return failure("antigravity_login_random_failed");
    }
    let state = URL_SAFE_NO_PAD.encode(&random[..32]);
    let verifier = URL_SAFE_NO_PAD.encode(&random[32..]);
    let listener = match TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(_) => return failure("antigravity_login_callback_unavailable"),
    };
    let port = match listener.local_addr() {
        Ok(address) => address.port(),
        Err(_) => return failure("antigravity_login_callback_unavailable"),
    };
    let redirect = format!("http://localhost:{port}{CALLBACK_PATH}");
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    let mut url = url::Url::parse(AUTHORIZE_URL).expect("fixed OAuth authorize URL");
    url.query_pairs_mut().extend_pairs([
        ("client_id", client_id),
        ("response_type", "code"),
        ("redirect_uri", redirect.as_str()),
        ("scope", SCOPES.join(" ").as_str()),
        ("code_challenge", challenge.as_str()),
        ("code_challenge_method", "S256"),
        ("state", state.as_str()),
        ("access_type", "offline"),
        ("prompt", "consent"),
    ]);
    let id = uuid::Uuid::new_v4().to_string();
    let deadline = Instant::now() + window;
    let (cancelled, mut cancellation) = watch::channel(false);
    let root = config_root();
    let expected;
    {
        let _credential = auth::credential_lock().lock().await;
        if closed.load(Ordering::Acquire) {
            return failure("antigravity_login_cancelled");
        }
        expected = auth::store_fingerprint();
        let mut flows = flows().lock().unwrap_or_else(|p| p.into_inner());
        for flow in flows.iter_mut() {
            if matches!(flow.outcome, Outcome::Pending) {
                fail(flow, "antigravity_login_cancelled");
            }
        }
        while flows.len() >= MAX_FLOWS {
            let removable = flows
                .iter()
                .position(|flow| {
                    !matches!(
                        &flow.outcome,
                        Outcome::Complete { generation, .. } if auth::is_current(generation)
                    )
                })
                .unwrap_or(0);
            if let Some(flow) = flows.remove(removable) {
                flow.cancelled.send_replace(true);
            }
        }
        flows.push_back(Flow {
            id: id.clone(),
            owner,
            root: root.clone(),
            deadline,
            cancelled,
            outcome: Outcome::Pending,
        });
    }
    let response = json!({"ok":true,"flowId":id,"authUrl":url.as_str(),
        "expiresAt":chrono::Utc::now().timestamp_millis()+window.as_millis() as i64,"interval":1});
    tokio::spawn(async move {
        let authorization = Authorization {
            id: id.clone(),
            root,
            expected,
            redirect,
            state,
            verifier,
            endpoints,
            closed,
        };
        let result = tokio::select! {
            biased;
            _ = cancellation.changed() => return,
            _ = tokio::time::sleep_until(deadline.into()) => Err("antigravity_login_expired"),
            result = serve(listener, &authorization) => result,
        };
        if let Err(error) = result {
            let mut flows = flows().lock().unwrap_or_else(|p| p.into_inner());
            if let Some(flow) = flows
                .iter_mut()
                .find(|flow| flow.id == id && matches!(flow.outcome, Outcome::Pending))
            {
                fail(flow, error);
            }
        }
    });
    response
}

pub async fn poll(owner: usize, id: &str) -> Value {
    let _credential = auth::credential_lock().lock().await;
    let mut flows = flows().lock().unwrap_or_else(|p| p.into_inner());
    let Some(flow) = flows
        .iter_mut()
        .find(|flow| flow.id == id && flow.owner == owner && flow.root == config_root())
    else {
        return failure("antigravity_login_invalid_flow");
    };
    if matches!(flow.outcome, Outcome::Pending) && Instant::now() >= flow.deadline {
        fail(flow, "antigravity_login_expired");
    }
    match &flow.outcome {
        Outcome::Pending => json!({"ok":false,"pending":true}),
        Outcome::Failed(error) => failure(error),
        Outcome::Complete {
            generation,
            updated_existing,
            account_id,
        } if auth::is_current(generation) => {
            let mut reply = auth::status();
            reply["updatedExisting"] = json!(*updated_existing);
            reply["accountId"] = json!(account_id);
            reply
        }
        Outcome::Complete { .. } => failure("antigravity_login_credential_changed"),
    }
}

/// A late cancellation can remove only the credential committed by this flow.
pub async fn cancel(owner: usize, id: &str) -> Value {
    let _credential = auth::credential_lock().lock().await;
    let mut flows = flows().lock().unwrap_or_else(|p| p.into_inner());
    if let Some(flow) = flows
        .iter_mut()
        .find(|flow| flow.id == id && flow.owner == owner && flow.root == config_root())
    {
        if let Outcome::Complete { generation, .. } = &flow.outcome {
            if let Err(error) = auth::remove_generation(generation) {
                return failure(&error);
            }
        }
        fail(flow, "antigravity_login_cancelled");
    }
    json!({"ok":true,"cancelled":true})
}

struct Authorization {
    id: String,
    root: PathBuf,
    expected: Vec<(String, String)>,
    redirect: String,
    state: String,
    verifier: String,
    endpoints: Endpoints,
    closed: Arc<AtomicBool>,
}

enum Callback {
    Ignored,
    Invalid,
    Denied,
    Code(String),
}

fn callback(raw: &[u8], authorization: &Authorization) -> Callback {
    let Ok(text) = std::str::from_utf8(raw) else {
        return Callback::Invalid;
    };
    let mut lines = text.split("\r\n");
    let Some(line) = lines.next() else {
        return Callback::Invalid;
    };
    let pieces: Vec<_> = line.split(' ').collect();
    if pieces.len() != 3 || pieces[0] != "GET" || pieces[2] != "HTTP/1.1" {
        return Callback::Invalid;
    }
    let Some((path, query)) = pieces[1].split_once('?') else {
        return Callback::Ignored;
    };
    if path != CALLBACK_PATH {
        return Callback::Ignored;
    }
    let expected = match url::Url::parse(&authorization.redirect) {
        Ok(url) => url,
        Err(_) => return Callback::Invalid,
    };
    let hosts: Vec<_> = lines
        .clone()
        .filter_map(|line| line.split_once(':'))
        .filter(|(name, _)| name.eq_ignore_ascii_case("host"))
        .collect();
    if hosts.len() != 1
        || ![
            format!("localhost:{}", expected.port().unwrap_or(80)),
            format!("127.0.0.1:{}", expected.port().unwrap_or(80)),
        ]
        .contains(&hosts[0].1.trim().to_string())
    {
        return Callback::Invalid;
    }
    if lines.any(|line| {
        line.split_once(':').is_some_and(|(key, value)| {
            key.eq_ignore_ascii_case("transfer-encoding")
                || key.eq_ignore_ascii_case("content-length") && value.trim() != "0"
        })
    }) {
        return Callback::Invalid;
    }
    let mut state = None;
    let mut code = None;
    let mut error = None;
    for (name, value) in url::form_urlencoded::parse(query.as_bytes()) {
        let slot = match name.as_ref() {
            "state" => &mut state,
            "code" => &mut code,
            "error" => &mut error,
            _ => continue,
        };
        if slot.replace(value.into_owned()).is_some() {
            return Callback::Invalid;
        }
    }
    if state.as_deref() != Some(&authorization.state) {
        return Callback::Invalid;
    }
    if error.is_some() && code.is_some() {
        return Callback::Invalid;
    }
    if error.is_some() {
        return Callback::Denied;
    }
    match code {
        Some(code)
            if !code.is_empty() && code.len() <= 8192 && !code.chars().any(char::is_control) =>
        {
            Callback::Code(code)
        }
        _ => Callback::Invalid,
    }
}

async fn read_callback(socket: &mut TcpStream) -> Result<Vec<u8>, ()> {
    let mut raw = Vec::new();
    loop {
        let mut chunk = [0u8; 1024];
        let count = socket.read(&mut chunk).await.map_err(|_| ())?;
        if count == 0 || raw.len() + count > MAX_HEADER {
            return Err(());
        }
        raw.extend_from_slice(&chunk[..count]);
        if let Some(end) = raw.windows(4).position(|window| window == b"\r\n\r\n") {
            if end + 4 != raw.len() {
                return Err(());
            }
            return Ok(raw);
        }
    }
}

async fn browser_response(socket: &mut TcpStream, status: u16, message: &str) {
    let response=format!("HTTP/1.1 {status} {}\r\nContent-Type: text/plain; charset=utf-8\r\nCache-Control: no-store\r\nReferrer-Policy: no-referrer\r\nContent-Security-Policy: default-src 'none'\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{message}", if status==200 {"OK"} else {"Bad Request"},message.len());
    let _ = tokio::time::timeout(
        Duration::from_secs(2),
        socket.write_all(response.as_bytes()),
    )
    .await;
}

async fn serve(listener: TcpListener, authorization: &Authorization) -> Result<(), &'static str> {
    loop {
        let (mut socket, peer) = listener
            .accept()
            .await
            .map_err(|_| "antigravity_login_callback_unavailable")?;
        if !peer.ip().is_loopback() {
            continue;
        }
        let raw =
            match tokio::time::timeout(Duration::from_secs(5), read_callback(&mut socket)).await {
                Ok(Ok(raw)) => raw,
                _ => {
                    browser_response(&mut socket, 400, "Invalid authorization callback.").await;
                    continue;
                }
            };
        match callback(&raw, authorization) {
            Callback::Ignored => browser_response(&mut socket, 404, "Not found.").await,
            Callback::Invalid => {
                browser_response(&mut socket, 400, "Invalid authorization callback.").await
            }
            Callback::Denied => {
                browser_response(&mut socket, 400, "Authorization was not completed.").await;
                return Err("antigravity_login_denied");
            }
            Callback::Code(code) => {
                let result = exchange(&code, authorization).await;
                match result {
                    Ok(credential) => {
                        let outcome = complete(authorization, credential).await;
                        browser_response(
                            &mut socket,
                            if outcome.is_ok() { 200 } else { 400 },
                            if outcome.is_ok() {
                                "Signed in. You can close this window."
                            } else {
                                "Authorization is no longer active."
                            },
                        )
                        .await;
                        return outcome;
                    }
                    Err(error) => {
                        browser_response(&mut socket, 400, "Authorization could not be completed.")
                            .await;
                        return Err(error);
                    }
                }
            }
        }
    }
}

fn text(value: &Value, key: &str) -> Result<String, &'static str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| {
            !value.is_empty()
                && value.len() <= 32 * 1024
                && value.trim() == *value
                && !value.chars().any(char::is_control)
        })
        .map(str::to_string)
        .ok_or("antigravity_login_invalid_response")
}

async fn exchange(
    code: &str,
    authorization: &Authorization,
) -> Result<auth::Credential, &'static str> {
    let (client_id, client_secret) = auth::oauth_client()?;
    let client = cloud::http_client().map_err(|_| "antigravity_login_network_error")?;
    let started = chrono::Utc::now().timestamp_millis();
    let response = client
        .post(&authorization.endpoints.token)
        .timeout(Duration::from_secs(30))
        .form(&[
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("grant_type", "authorization_code"),
            ("code", code),
            ("redirect_uri", authorization.redirect.as_str()),
            ("code_verifier", authorization.verifier.as_str()),
        ])
        .send()
        .await
        .map_err(|_| "antigravity_login_network_error")?;
    let status = response.status();
    let bytes = cloud::read_body(response, MAX_RESPONSE)
        .await
        .map_err(|_| "antigravity_login_invalid_response")?;
    if !status.is_success() {
        return Err(if status.as_u16() == 429 || status.is_server_error() {
            "antigravity_login_network_error"
        } else {
            "antigravity_login_failed"
        });
    }
    let value: Value =
        serde_json::from_slice(&bytes).map_err(|_| "antigravity_login_invalid_response")?;
    if !text(&value, "token_type")?.eq_ignore_ascii_case("bearer") {
        return Err("antigravity_login_invalid_response");
    }
    let access_token = text(&value, "access_token")?;
    let refresh_token = if value.get("refresh_token").is_some() {
        Some(text(&value, "refresh_token")?)
    } else {
        None
    };
    let seconds = value
        .get("expires_in")
        .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
        .filter(|seconds| *seconds > 0 && *seconds <= 365 * 24 * 60 * 60)
        .ok_or("antigravity_login_invalid_response")?;
    let response = client
        .get(&authorization.endpoints.userinfo)
        .timeout(Duration::from_secs(15))
        .bearer_auth(&access_token)
        .send()
        .await
        .map_err(|_| "antigravity_login_network_error")?;
    if !response.status().is_success() {
        return Err("antigravity_login_failed");
    }
    let bytes = cloud::read_body(response, MAX_RESPONSE)
        .await
        .map_err(|_| "antigravity_login_invalid_response")?;
    let profile: Value =
        serde_json::from_slice(&bytes).map_err(|_| "antigravity_login_invalid_response")?;
    let email = text(&profile, "email")?;
    if email.len() > 320 {
        return Err("antigravity_login_invalid_response");
    }
    Ok(auth::Credential {
        access_token,
        refresh_token,
        expires_at: Some(started + seconds * 1000),
        account: Some(email),
        auth_method: "consumer".into(),
        project: None,
        client_id: None,
        client_secret: None,
    })
}

async fn complete(
    authorization: &Authorization,
    credential: auth::Credential,
) -> Result<(), &'static str> {
    let _credential = auth::credential_lock().lock().await;
    let mut flows = flows().lock().unwrap_or_else(|p| p.into_inner());
    let Some(flow) = flows
        .iter_mut()
        .find(|flow| flow.id == authorization.id && matches!(flow.outcome, Outcome::Pending))
    else {
        return Err("antigravity_login_cancelled");
    };
    if authorization.closed.load(Ordering::Acquire) {
        return Err("antigravity_login_cancelled");
    }
    if flow.deadline <= Instant::now() {
        return Err("antigravity_login_expired");
    }
    if flow.root != config_root()
        || flow.root != authorization.root
        || auth::store_fingerprint() != authorization.expected
    {
        return Err("antigravity_login_credential_changed");
    }
    let outcome = auth::store_login(credential).map_err(|_| "antigravity_login_store_failed")?;
    flow.outcome = Outcome::Complete {
        generation: outcome.generation,
        updated_existing: outcome.updated_existing,
        account_id: outcome.account_id,
    };
    Ok(())
}

#[cfg(test)]
mod tests;
