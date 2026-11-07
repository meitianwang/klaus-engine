//! Engine-owned browser authorization and refresh grants, separate from imports.
use super::{changes, config_root, credential, invalidate, state, State};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use credential::{AuthMethod, Credential, StoredCredential};
use rand::RngCore;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

const API_ORIGIN: &str = "https://api2.cursor.sh";
const FLOW_MILLIS: i64 = 300_000;
const MAX_RESPONSE: usize = 65_536;
// Public application identifier for the browser grant exchange.
const CLIENT_ID: &str = "KbZUR41cY7W6zRSdpSUJ7I7mLYBKOCmB";

#[derive(Clone)]
pub(super) struct PendingFlow {
    id: String,
    diagnostic_flow_id: String,
    uuid: String,
    verifier: String,
    identity: String,
    version: Option<String>,
    origin: String,
    credential_id: Option<String>,
    generation: u64,
    expires_at: i64,
    deadline: Instant,
    next_poll: Instant,
    polling: bool,
    failures: u8,
}

fn login_changes() -> &'static tokio::sync::watch::Sender<u64> {
    static CHANGES: OnceLock<tokio::sync::watch::Sender<u64>> = OnceLock::new();
    CHANGES.get_or_init(|| tokio::sync::watch::channel(0).0)
}

fn notify_login_change() {
    let revision = login_changes().borrow().wrapping_add(1);
    login_changes().send_replace(revision);
}

pub(super) fn cancel_pending(state: &mut State) {
    if state.login.take().is_some() {
        notify_login_change();
    }
}

fn valid_secret(value: &str) -> bool {
    !value.is_empty()
        && value.is_ascii()
        && value.len() <= 32_768
        && !value.chars().any(|c| c.is_whitespace() || c.is_control())
}

fn expires_at(token: &str) -> Option<i64> {
    let bytes = URL_SAFE_NO_PAD.decode(token.split('.').nth(1)?).ok()?;
    let claims: Value = serde_json::from_slice(&bytes).ok()?;
    claims["exp"].as_i64()?.checked_mul(1000)
}

fn auth_url(uuid: &str, verifier: &str) -> String {
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    let mut url =
        url::Url::parse("https://cursor.com/loginDeepControl").expect("fixed login origin");
    url.query_pairs_mut().extend_pairs([
        ("challenge", challenge.as_str()),
        ("uuid", uuid),
        ("mode", "login"),
        ("redirectTarget", "cli"),
    ]);
    url.to_string()
}

pub fn login_start(version: Option<String>) -> Value {
    super::diagnostics::sync_value("login_start", || login_start_inner(version))
}

fn login_start_inner(version: Option<String>) -> Value {
    start_at(version, API_ORIGIN)
}

pub(super) fn start_at(version: Option<String>, origin: &str) -> Value {
    if version.as_ref().is_some_and(|version| {
        version.is_empty()
            || version.len() > 128
            || !version.is_ascii()
            || version.chars().any(|c| c.is_control())
    }) {
        return json!({"ok":false,"error":"cursor_login_metadata_invalid"});
    }
    let mut bytes = [0_u8; 32];
    if rand::rngs::OsRng.try_fill_bytes(&mut bytes).is_err() {
        return json!({"ok":false,"error":"cursor_login_random_failed"});
    }
    let verifier = URL_SAFE_NO_PAD.encode(bytes);
    let mut state = state().lock().unwrap_or_else(|p| p.into_inner());
    let credential_id = match credential::read(&config_root(&state)) {
        Ok(stored) => Some(stored.credential_id),
        Err(error) if error == "cursor_not_signed_in" => None,
        Err(error) => return json!({"ok":false,"error":error}),
    };
    let identity = match credential::installation_identity(&config_root(&state)) {
        Ok(identity) => identity,
        Err(error) => return json!({"ok":false,"error":error}),
    };
    cancel_pending(&mut state);
    let flow = PendingFlow {
        id: uuid::Uuid::new_v4().to_string(),
        diagnostic_flow_id: uuid::Uuid::new_v4().to_string(),
        uuid: uuid::Uuid::new_v4().to_string(),
        verifier,
        identity,
        version,
        origin: origin.into(),
        credential_id,
        generation: state.generation,
        expires_at: chrono::Utc::now().timestamp_millis() + FLOW_MILLIS,
        deadline: Instant::now() + Duration::from_millis(FLOW_MILLIS as u64),
        next_poll: Instant::now(),
        polling: false,
        failures: 0,
    };
    let result = json!({"ok":true,"flowId":flow.id,"authUrl":auth_url(&flow.uuid,&flow.verifier),"expiresAt":flow.expires_at,"interval":1});
    super::diagnostics::event(
        "flow_created",
        json!({"flowTraceId":flow.diagnostic_flow_id,"generation":flow.generation}),
    );
    state.login = Some(flow);
    notify_login_change();
    result
}

pub fn login_cancel(flow_id: &str) -> Value {
    super::diagnostics::sync_value("login_cancel", || login_cancel_inner(flow_id))
}

fn login_cancel_inner(flow_id: &str) -> Value {
    let mut state = state().lock().unwrap_or_else(|p| p.into_inner());
    if state.login.as_ref().is_some_and(|flow| flow.id == flow_id) {
        cancel_pending(&mut state);
    }
    json!({"ok":true})
}

pub(super) async fn bounded(response: reqwest::Response) -> Result<(u16, Value), String> {
    use futures::StreamExt;
    super::diagnostics::headers(&response);
    let status = response.status().as_u16();
    let mut bytes = Vec::new();
    let mut source = response.bytes_stream();
    while let Some(chunk) = source.next().await {
        let chunk = chunk.map_err(|error| {
            super::diagnostics::network_error(&error);
            "cursor_login_network_error"
        })?;
        if bytes.is_empty() {
            super::diagnostics::event("body_first_chunk", json!({}));
        }
        if bytes.len() + chunk.len() > MAX_RESPONSE {
            return Err("cursor_login_response_too_large".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    super::diagnostics::event("body_complete", json!({"bytes":bytes.len()}));
    if status == 404 {
        return Ok((status, Value::Null));
    }
    let value = serde_json::from_slice(&bytes).map_err(|_| "cursor_login_invalid_response")?;
    Ok((status, value))
}

async fn poll_request(flow: &PendingFlow) -> Result<(Credential, String), String> {
    super::diagnostics::event(
        "request_send",
        json!({"endpoint":"login_poll","timeoutMs":15000,"failures":flow.failures}),
    );
    let response = credential::client()?
        .get(format!("{}/auth/poll", flow.origin))
        .query(&[
            ("uuid", flow.uuid.as_str()),
            ("verifier", flow.verifier.as_str()),
        ])
        .header("content-type", "application/json")
        .timeout(Duration::from_secs(15))
        .send()
        .await
        .map_err(|error| {
            super::diagnostics::network_error(&error);
            "cursor_login_network_error"
        })?;
    let (status, value) = bounded(response).await?;
    match status {
        404 => return Err("cursor_login_pending".into()),
        403 => return Err("cursor_login_denied".into()),
        429 => return Err("cursor_rate_limit".into()),
        500..=599 => return Err("cursor_login_network_error".into()),
        200 => {}
        _ => return Err("cursor_login_failed".into()),
    }
    super::diagnostics::event(
        "grant_shape",
        json!({"hasAccessToken":value["accessToken"].is_string(),"hasRefreshToken":value["refreshToken"].is_string(),"teamPresent":value.get("selectedTeamId").is_some_and(|team| !team.is_null())}),
    );
    let token = value["accessToken"]
        .as_str()
        .filter(|token| valid_secret(token))
        .ok_or("cursor_login_invalid_response")?;
    let refresh = value["refreshToken"]
        .as_str()
        .filter(|token| valid_secret(token))
        .ok_or("cursor_login_invalid_response")?;
    let team_id = value
        .get("selectedTeamId")
        .filter(|value| !value.is_null())
        .map(|value| {
            value
                .as_u64()
                .and_then(|team| u32::try_from(team).ok())
                .filter(|team| *team > 0 && *team <= i32::MAX as u32)
                .ok_or("cursor_login_invalid_response")
        })
        .transpose()?;
    let credential = Credential {
        access_token: token.into(),
        expires_at: expires_at(token),
        account: None,
        client_version: flow.version.clone(),
        client_identity: Some(flow.identity.clone()),
        backend_url: Some(flow.origin.clone()),
        inference_url: None,
        team_id,
    };
    credential.valid().map_err(|error| {
        if error == credential::NEEDS_REIMPORT {
            credential::NEEDS_LOGIN.into()
        } else {
            error
        }
    })?;
    Ok((credential, refresh.into()))
}

pub async fn login_poll(flow_id: &str) -> Value {
    super::diagnostics::value("login_poll", login_poll_inner(flow_id)).await
}

async fn login_poll_inner(flow_id: &str) -> Value {
    let mut changed = login_changes().subscribe();
    let flow = {
        let mut state = state().lock().unwrap_or_else(|p| p.into_inner());
        let Some(flow) = state.login.as_mut().filter(|flow| flow.id == flow_id) else {
            return json!({"ok":false,"cancelled":true,"error":"cursor_login_cancelled"});
        };
        if flow.expires_at <= chrono::Utc::now().timestamp_millis()
            || flow.deadline <= Instant::now()
        {
            cancel_pending(&mut state);
            return json!({"ok":false,"expired":true,"error":"cursor_login_expired"});
        }
        if flow.polling || flow.next_poll > Instant::now() {
            super::diagnostics::event(
                "poll_deferred",
                json!({"polling":flow.polling,"failures":flow.failures}),
            );
            return json!({"ok":false,"pending":true});
        }
        flow.polling = true;
        let flow = flow.clone();
        changed.borrow_and_update();
        flow
    };
    super::diagnostics::event(
        "poll_attempt",
        json!({"flowTraceId":flow.diagnostic_flow_id,"failures":flow.failures}),
    );
    let remaining = flow.deadline.saturating_duration_since(Instant::now());
    let result = tokio::select! {
        biased;
        _ = changed.changed() => Err("cursor_login_cancelled".into()),
        _ = tokio::time::sleep(remaining) => Err("cursor_login_expired".into()),
        result = poll_request(&flow) => result,
    };
    let mut state = state().lock().unwrap_or_else(|p| p.into_inner());
    if state
        .login
        .as_ref()
        .is_none_or(|current| current.id != flow.id)
    {
        return json!({"ok":false,"cancelled":true,"error":"cursor_login_cancelled"});
    }
    if flow.expires_at <= chrono::Utc::now().timestamp_millis() || flow.deadline <= Instant::now() {
        cancel_pending(&mut state);
        return json!({"ok":false,"expired":true,"error":"cursor_login_expired"});
    }
    let current_id = match credential::read(&config_root(&state)) {
        Ok(stored) => Some(stored.credential_id),
        Err(error) if error == "cursor_not_signed_in" => None,
        Err(error) => {
            cancel_pending(&mut state);
            return json!({"ok":false,"error":error});
        }
    };
    if state.generation != flow.generation || current_id != flow.credential_id {
        cancel_pending(&mut state);
        return json!({"ok":false,"error":"cursor_credentials_changed"});
    }
    match result {
        Ok((user_info, refresh)) => {
            let credential_id = uuid::Uuid::new_v4().to_string();
            let stored = StoredCredential {
                credential_id: credential_id.clone(),
                user_info,
                auth_method: AuthMethod::Browser,
                refresh_token: Some(refresh),
            };
            let root = config_root(&state);
            if let Err(error) = credential::write(&root, &stored) {
                cancel_pending(&mut state);
                return json!({"ok":false,"error":error});
            }
            super::diagnostics::event(
                "grant_committed",
                json!({"generation":state.generation,"hasAccount":stored.user_info.account.is_some()}),
            );
            invalidate(&mut state);
            json!({"ok":true,"credentialId":credential_id,"account":stored.user_info.account,"authMethod":"browser"})
        }
        Err(error) => {
            if error == "cursor_login_pending"
                || error == "cursor_login_network_error"
                || error == "cursor_rate_limit"
            {
                let current = state.login.as_mut().expect("matching flow");
                current.polling = false;
                if error == "cursor_login_pending" {
                    current.failures = 0;
                } else {
                    current.failures += 1;
                }
                current.next_poll =
                    Instant::now() + Duration::from_secs(1_u64 << current.failures.min(3));
                if current.failures < 3 {
                    return json!({"ok":false,"pending":true});
                }
            }
            cancel_pending(&mut state);
            json!({"ok":false,"error":error})
        }
    }
}

pub async fn ensure_valid() -> Result<(), String> {
    super::diagnostics::result("ensure_valid", ensure_valid_inner()).await
}

async fn ensure_valid_inner() -> Result<(), String> {
    super::diagnostics::event("refresh_lock_wait", json!({}));
    let admission_started = Instant::now();
    static REFRESH: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    let _refresh = REFRESH
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    super::diagnostics::event(
        "refresh_lock_acquired",
        json!({"waitMs":admission_started.elapsed().as_millis() as u64}),
    );
    let mut changed = changes().subscribe();
    let mut login_changed = login_changes().subscribe();
    let (stored, generation, login_revision) = {
        let state = state().lock().unwrap_or_else(|p| p.into_inner());
        let stored = credential::read(&config_root(&state))?;
        super::diagnostics::event(
            "credential_state",
            json!({"authMethod":stored.auth_method,"rejected":state.rejected,"generation":state.generation,"expiryKnown":stored.user_info.expires_at.is_some(),"nearExpiry":stored.user_info.expires_at.is_some_and(|expiry| expiry <= chrono::Utc::now().timestamp_millis() + 60_000),"hasAccount":stored.user_info.account.is_some(),"hasClientVersion":stored.user_info.client_version.is_some(),"hasClientIdentity":stored.user_info.client_identity.is_some(),"hasRefreshToken":stored.refresh_token.is_some()}),
        );
        if stored.auth_method != AuthMethod::Browser {
            return if state.rejected {
                Err(credential::NEEDS_REIMPORT.into())
            } else {
                stored.user_info.valid()
            };
        }
        if state.login.is_some() {
            return stored.user_info.valid().map_err(|error| {
                if error == credential::NEEDS_REIMPORT {
                    credential::NEEDS_LOGIN.into()
                } else {
                    error
                }
            });
        }
        let near_expiry = stored
            .user_info
            .expires_at
            .is_some_and(|expiry| expiry <= chrono::Utc::now().timestamp_millis() + 60_000);
        if !near_expiry && !state.rejected {
            return stored.user_info.valid();
        }
        changed.borrow_and_update();
        let login_revision = *login_changed.borrow_and_update();
        (stored, state.generation, login_revision)
    };
    let refresh = stored
        .refresh_token
        .as_deref()
        .filter(|token| valid_secret(token))
        .ok_or(credential::NEEDS_LOGIN)?;
    super::diagnostics::event("refresh_required", json!({"timeoutMs":15000}));
    let request = async {
        super::diagnostics::event("request_send", json!({"endpoint":"refresh"}));
        let response = credential::client()?.post(format!("{}/oauth/token", stored.user_info.api_origin()?))
            .header("x-cursor-client-type", "ide").json(&json!({"grant_type":"refresh_token","client_id":CLIENT_ID,"refresh_token":refresh}))
            .timeout(Duration::from_secs(15)).send().await.map_err(|error| { super::diagnostics::network_error(&error); "cursor_network_error" })?;
        bounded(response).await
    };
    let (status, response) = tokio::select! { biased;
        _ = changed.changed() => return Err("cursor_credential_changed".into()),
        _ = login_changed.changed() => return Err("cursor_credential_changed".into()),
        result = request => result?,
    };
    let mut state = state().lock().unwrap_or_else(|p| p.into_inner());
    if state.generation != generation
        || state.login.is_some()
        || *login_changes().borrow() != login_revision
        || credential::read(&config_root(&state))
            .ok()
            .is_none_or(|current| current.credential_id != stored.credential_id)
    {
        return Err("cursor_credential_changed".into());
    }
    if matches!(status, 400 | 401 | 403) || response["shouldLogout"] == true {
        invalidate(&mut state);
        state.rejected = true;
        return Err(credential::NEEDS_LOGIN.into());
    }
    if status != 200 {
        return Err(credential::http_error(status, None));
    }
    let token = response["access_token"]
        .as_str()
        .filter(|token| valid_secret(token))
        .ok_or("cursor_login_invalid_response")?;
    let refresh = match response.get("refresh_token") {
        Some(value) => value
            .as_str()
            .filter(|token| valid_secret(token))
            .ok_or("cursor_login_invalid_response")?,
        None => token,
    };
    let mut updated = stored.clone();
    updated.credential_id = uuid::Uuid::new_v4().to_string();
    updated.user_info.access_token = token.into();
    updated.user_info.expires_at = expires_at(token);
    updated.refresh_token = Some(refresh.into());
    updated.user_info.valid().map_err(|error| {
        if error == credential::NEEDS_REIMPORT {
            credential::NEEDS_LOGIN.into()
        } else {
            error
        }
    })?;
    credential::write(&config_root(&state), &updated)?;
    super::diagnostics::event("refresh_commit", json!({"generation":state.generation}));
    invalidate(&mut state);
    Ok(())
}

pub async fn status_live() -> Value {
    super::diagnostics::value("status_live", status_live_inner()).await
}

async fn status_live_inner() -> Value {
    super::diagnostics::event("status_refresh", json!({"timeoutMs":12000}));
    let result = tokio::time::timeout(Duration::from_secs(12), async {
        ensure_valid().await?;
        super::account::hydrate_missing_account().await;
        Ok::<(), String>(())
    })
    .await
    .unwrap_or_else(|_| {
        super::diagnostics::event("deadline", json!({"timeoutMs":12000}));
        Err("cursor_network_error".into())
    });
    let mut status = credential::status();
    if let Err(error) = result {
        if !matches!(
            error.as_str(),
            "cursor_not_signed_in" | credential::NEEDS_REIMPORT | credential::NEEDS_LOGIN
        ) {
            status["error"] = json!(error);
        }
    }
    super::diagnostics::event(
        "status_result",
        json!({"loggedIn":status["loggedIn"],"hasAccount":status["account"].is_string(),"needsLogin":status["needsLogin"],"needsReimport":status["needsReimport"]}),
    );
    status
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    struct Fixture {
        origin: String,
        polls: Arc<AtomicUsize>,
        refreshes: Arc<AtomicUsize>,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
        requests: Arc<std::sync::Mutex<Vec<Value>>>,
        task: tokio::task::JoinHandle<()>,
    }

    fn token(expiry: i64) -> String {
        format!(
            "header.{}.signature",
            URL_SAFE_NO_PAD.encode(json!({"exp":expiry}).to_string())
        )
    }

    async fn fixture(mode: &'static str) -> Fixture {
        use bytes::Bytes;
        use http_body_util::{BodyExt, Full};
        use hyper::Response;
        use hyper_util::rt::TokioIo;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let polls = Arc::new(AtomicUsize::new(0));
        let refreshes = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let task = {
            let (polls, refreshes, entered, release, requests) = (
                polls.clone(),
                refreshes.clone(),
                entered.clone(),
                release.clone(),
                requests.clone(),
            );
            tokio::spawn(async move {
                loop {
                    let (socket, _) = listener.accept().await.unwrap();
                    let (polls, refreshes, entered, release, requests) = (
                        polls.clone(),
                        refreshes.clone(),
                        entered.clone(),
                        release.clone(),
                        requests.clone(),
                    );
                    tokio::spawn(async move {
                        let service = hyper::service::service_fn(
                            move |request: hyper::Request<hyper::body::Incoming>| {
                                let (polls, refreshes, entered, release, requests) = (
                                    polls.clone(),
                                    refreshes.clone(),
                                    entered.clone(),
                                    release.clone(),
                                    requests.clone(),
                                );
                                async move {
                                    let path = request.uri().path().to_string();
                                    let query =
                                        request.uri().query().unwrap_or_default().to_string();
                                    let body =
                                        request.into_body().collect().await.unwrap().to_bytes();
                                    requests.lock().unwrap().push(json!({"path":path,"query":query,"body":String::from_utf8(body.to_vec()).unwrap()}));
                                    let mut status = 200;
                                    let value = if path == "/auth/poll" {
                                        let count = polls.fetch_add(1, Ordering::SeqCst);
                                        entered.notify_one();
                                        if mode == "hang" {
                                            release.notified().await;
                                        }
                                        if mode == "denied" {
                                            status = 403;
                                            json!({"error":"denied"})
                                        } else if mode == "pending" && count == 0 {
                                            status = 404;
                                            Value::Null
                                        } else {
                                            json!({"accessToken":token(chrono::Utc::now().timestamp()+3600),"refreshToken":"fixture-refresh-secret","selectedTeamId":7})
                                        }
                                    } else if path == "/oauth/token" {
                                        refreshes.fetch_add(1, Ordering::SeqCst);
                                        entered.notify_one();
                                        if mode == "refresh-hang" || mode == "refresh-hang-denied" {
                                            release.notified().await;
                                        }
                                        if mode == "refresh-denied" || mode == "refresh-hang-denied"
                                        {
                                            status = 401;
                                            json!({"shouldLogout":true})
                                        } else {
                                            json!({"access_token":token(chrono::Utc::now().timestamp()+7200)})
                                        }
                                    } else if path == "/fixture-auth" {
                                        entered.notify_one();
                                        release.notified().await;
                                        status = 401;
                                        json!({"error":{"code":"unauthenticated"}})
                                    } else {
                                        json!({"email":"fixture@example.test"})
                                    };
                                    let body = if mode == "oversized" && path == "/auth/poll" {
                                        "x".repeat(MAX_RESPONSE + 1)
                                    } else if mode == "invalid" && path == "/auth/poll" {
                                        "not json".into()
                                    } else {
                                        value.to_string()
                                    };
                                    Ok::<_, std::convert::Infallible>(
                                        Response::builder()
                                            .status(status)
                                            .body(Full::new(Bytes::from(body)))
                                            .unwrap(),
                                    )
                                }
                            },
                        );
                        let _ = hyper::server::conn::http1::Builder::new()
                            .serve_connection(TokioIo::new(socket), service)
                            .await;
                    });
                }
            })
        };
        Fixture {
            origin,
            polls,
            refreshes,
            entered,
            release,
            requests,
            task,
        }
    }

    fn expire_browser() -> StoredCredential {
        let state = state().lock().unwrap_or_else(|p| p.into_inner());
        let mut stored = credential::read(&config_root(&state)).unwrap();
        stored.user_info.expires_at = Some(1);
        credential::write(&config_root(&state), &stored).unwrap();
        stored
    }

    async fn login(fixture: &Fixture) -> Value {
        let start = start_at(None, &fixture.origin);
        let result = login_poll(start["flowId"].as_str().unwrap()).await;
        assert_eq!(result["ok"], true);
        result
    }

    #[test]
    fn pkce_uses_s256_and_never_places_the_verifier_in_the_browser_url() {
        let verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
        let url = url::Url::parse(&auth_url("flow", verifier)).unwrap();
        let query = url
            .query_pairs()
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(url.origin().ascii_serialization(), "https://cursor.com");
        assert_eq!(
            query["challenge"],
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
        assert_eq!(query["redirectTarget"], "cli");
        assert!(!url.as_str().contains(verifier));
        assert!(!query.contains_key("verifier"));
    }

    #[tokio::test]
    async fn browser_login_is_independent_bounded_private_and_distinct_from_import() {
        let _lock = super::super::test_lock().lock().await;
        let temp = tempfile::tempdir().unwrap();
        super::super::set_config_root(temp.path());
        let fixture = fixture("pending").await;
        let start = start_at(None, &fixture.origin);
        let flow = start["flowId"].as_str().unwrap();
        assert_eq!(login_poll(flow).await["pending"], true);
        assert_eq!(login_poll(flow).await["pending"], true);
        assert_eq!(
            fixture.polls.load(Ordering::SeqCst),
            1,
            "cadence is enforced in the engine"
        );
        state().lock().unwrap().login.as_mut().unwrap().next_poll = Instant::now();
        let result = login_poll(flow).await;
        assert_eq!(result["ok"], true);
        assert_eq!(result["authMethod"], "browser");
        let stored = credential::read(temp.path()).unwrap();
        assert!(stored.auth_method == AuthMethod::Browser);
        assert_eq!(stored.user_info.team_id, Some(7));
        assert_eq!(stored.user_info.client_identity.as_ref().unwrap().len(), 32);
        assert_eq!(
            stored.refresh_token.as_deref(),
            Some("fixture-refresh-secret")
        );
        let visible = json!([start, result, credential::status()]).to_string();
        assert!(!visible.contains("fixture-refresh-secret"));
        assert!(!visible.contains(stored.user_info.client_identity.as_ref().unwrap()));
        assert_eq!(credential::status()["needsReimport"], false);
        assert_eq!(credential::status()["needsLogin"], false);
        assert_eq!(status_live().await["account"], "fixture@example.test");
        assert!(fixture
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|request| request["path"] == "/auth/poll"
                && request["query"].as_str().unwrap().contains("verifier=")));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(temp.path().join("cursor-subscription-oauth.json"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        fixture.task.abort();
    }

    #[tokio::test]
    async fn malformed_oversized_denied_and_expired_flows_preserve_an_existing_login() {
        let _lock = super::super::test_lock().lock().await;
        let temp = tempfile::tempdir().unwrap();
        super::super::set_config_root(temp.path());
        let adopted = credential::adopt_credential(
            json!({"accessToken":"existing","clientIdentity":"existing-id"}),
            None,
        );
        for mode in ["invalid", "oversized", "denied"] {
            let fixture = fixture(mode).await;
            let start = start_at(None, &fixture.origin);
            let result = login_poll(start["flowId"].as_str().unwrap()).await;
            assert_eq!(result["ok"], false);
            assert_eq!(
                credential::status()["credentialId"],
                adopted["credentialId"]
            );
            fixture.task.abort();
        }
        let start = login_start(None);
        state().lock().unwrap().login.as_mut().unwrap().expires_at = 1;
        assert_eq!(
            login_poll(start["flowId"].as_str().unwrap()).await["expired"],
            true
        );
        assert_eq!(credential::status()["authMethod"], "imported");
    }

    #[tokio::test]
    async fn cancellation_logout_import_and_replacement_cannot_be_overwritten_by_a_late_poll() {
        let _lock = super::super::test_lock().lock().await;
        for operation in ["cancel", "logout", "import", "replace"] {
            let temp = tempfile::tempdir().unwrap();
            super::super::set_config_root(temp.path());
            let fixture = fixture("hang").await;
            let start = start_at(None, &fixture.origin);
            let id = start["flowId"].as_str().unwrap().to_string();
            let task = tokio::spawn({
                let id = id.clone();
                async move { login_poll(&id).await }
            });
            tokio::time::timeout(Duration::from_secs(2), fixture.entered.notified())
                .await
                .unwrap();
            assert_eq!(login_poll(&id).await["pending"], true);
            assert_eq!(fixture.polls.load(Ordering::SeqCst), 1);
            login_cancel("another-flow");
            let newer = match operation {
                "cancel" => login_cancel(&id),
                "logout" => credential::logout(None).unwrap(),
                "import" => credential::adopt_credential(
                    json!({"accessToken":"replacement","clientIdentity":"other-id"}),
                    None,
                ),
                _ => start_at(None, &fixture.origin),
            };
            let result = tokio::time::timeout(Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(result["cancelled"], true);
            fixture.release.notify_one();
            if operation == "import" {
                assert_eq!(credential::status()["credentialId"], newer["credentialId"]);
            } else {
                assert_eq!(credential::status()["loggedIn"], false);
            }
            fixture.task.abort();
        }
    }

    #[tokio::test]
    async fn only_owned_browser_grants_refresh_and_rotation_retains_the_private_install_identity() {
        let _lock = super::super::test_lock().lock().await;
        let temp = tempfile::tempdir().unwrap();
        super::super::set_config_root(temp.path());
        let fixture = fixture("success").await;
        login(&fixture).await;
        let old = expire_browser();
        assert_eq!(credential::status()["needsLogin"], true);
        assert_eq!(credential::status()["needsReimport"], false);
        let (first, second) = tokio::join!(ensure_valid(), ensure_valid());
        first.unwrap();
        second.unwrap();
        let new = credential::read(temp.path()).unwrap();
        assert_ne!(new.credential_id, old.credential_id);
        assert_eq!(new.user_info.client_identity, old.user_info.client_identity);
        assert_eq!(new.user_info.team_id, old.user_info.team_id);
        assert_eq!(
            new.refresh_token.as_ref(),
            Some(&new.user_info.access_token)
        );
        ensure_valid().await.unwrap();
        assert_eq!(fixture.refreshes.load(Ordering::SeqCst), 1);
        let request = fixture
            .requests
            .lock()
            .unwrap()
            .iter()
            .find(|request| request["path"] == "/oauth/token")
            .unwrap()
            .clone();
        let payload: Value = serde_json::from_str(request["body"].as_str().unwrap()).unwrap();
        assert_eq!(payload["grant_type"], "refresh_token");
        assert_eq!(payload["client_id"], CLIENT_ID);
        credential::adopt_credential(
            json!({"accessToken":"imported","clientIdentity":"imported-id","backendUrl":fixture.origin}),
            None,
        );
        {
            let state = state().lock().unwrap();
            let mut stored = credential::read(&config_root(&state)).unwrap();
            stored.user_info.expires_at = Some(1);
            credential::write(&config_root(&state), &stored).unwrap();
        }
        assert_eq!(
            ensure_valid().await.err().unwrap(),
            credential::NEEDS_REIMPORT
        );
        assert_eq!(fixture.refreshes.load(Ordering::SeqCst), 1);
        fixture.task.abort();
    }

    #[tokio::test]
    async fn a_refresh_cancelled_by_logout_never_resurrects_its_credential() {
        let _lock = super::super::test_lock().lock().await;
        let temp = tempfile::tempdir().unwrap();
        super::super::set_config_root(temp.path());
        let fixture = fixture("refresh-hang").await;
        login(&fixture).await;
        expire_browser();
        let task = tokio::spawn(ensure_valid());
        tokio::time::timeout(Duration::from_secs(2), fixture.entered.notified())
            .await
            .unwrap();
        // Login also signalled the fixture; wait until the actual refresh entered.
        tokio::time::timeout(Duration::from_secs(2), async {
            while fixture.refreshes.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        credential::logout(None).unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap()
                .err()
                .unwrap(),
            "cursor_credential_changed"
        );
        fixture.release.notify_one();
        assert_eq!(credential::status()["loggedIn"], false);
        fixture.task.abort();
    }
    #[tokio::test]
    async fn browser_refresh_rejection_requests_browser_login_without_reimporting() {
        let _lock = super::super::test_lock().lock().await;
        let temp = tempfile::tempdir().unwrap();
        super::super::set_config_root(temp.path());
        let fixture = fixture("refresh-denied").await;
        login(&fixture).await;
        expire_browser();
        assert_eq!(ensure_valid().await.err().unwrap(), credential::NEEDS_LOGIN);
        let status = credential::status();
        assert_eq!(status["authMethod"], "browser");
        assert_eq!(status["needsLogin"], true);
        assert_eq!(status["needsReimport"], false);
        assert_eq!(
            credential::snapshot().err().unwrap(),
            credential::NEEDS_LOGIN
        );
        assert_eq!(
            super::super::responses::public_error(credential::NEEDS_LOGIN)["error"]["type"],
            "authentication_error"
        );
        fixture.task.abort();
    }

    #[tokio::test]
    async fn browser_installation_identity_survives_cancel_and_disconnect_without_borrowing_imported_identity(
    ) {
        let _lock = super::super::test_lock().lock().await;
        let temp = tempfile::tempdir().unwrap();
        super::super::set_config_root(temp.path());
        credential::adopt_credential(
            json!({"accessToken":"existing","clientIdentity":"external-client"}),
            None,
        );
        let first = login_start(None);
        let identity = state()
            .lock()
            .unwrap()
            .login
            .as_ref()
            .unwrap()
            .identity
            .clone();
        assert_ne!(identity, "external-client");
        login_cancel(first["flowId"].as_str().unwrap());
        credential::logout(None).unwrap();
        login_start(None);
        assert_eq!(
            state().lock().unwrap().login.as_ref().unwrap().identity,
            identity
        );
        assert_eq!(
            std::fs::read_to_string(temp.path().join("cursor-install-identity")).unwrap(),
            identity
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(temp.path().join("cursor-install-identity"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
    }
    #[tokio::test]
    async fn an_explicit_browser_login_supersedes_automatic_refresh_without_losing_its_flow() {
        let _lock = super::super::test_lock().lock().await;
        for mode in ["refresh-hang", "refresh-hang-denied"] {
            let temp = tempfile::tempdir().unwrap();
            super::super::set_config_root(temp.path());
            let fixture = fixture(mode).await;
            login(&fixture).await;
            let original = expire_browser();
            let refresh = tokio::spawn(ensure_valid());
            tokio::time::timeout(Duration::from_secs(2), async {
                while fixture.refreshes.load(Ordering::SeqCst) == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            let start = start_at(None, &fixture.origin);
            let id = start["flowId"].as_str().unwrap();
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), refresh)
                    .await
                    .unwrap()
                    .unwrap()
                    .err()
                    .unwrap(),
                "cursor_credential_changed"
            );
            fixture.release.notify_one();
            let status = status_live().await;
            assert_eq!(status["authMethod"], "browser");
            assert_eq!(
                credential::read(temp.path()).unwrap().credential_id,
                original.credential_id
            );
            assert_eq!(
                fixture.refreshes.load(Ordering::SeqCst),
                1,
                "status must not start a refresh while explicit login is pending"
            );
            assert_eq!(state().lock().unwrap().login.as_ref().unwrap().id, id);
            assert_eq!(
                login_poll(id).await["ok"],
                true,
                "the explicit browser flow can still complete"
            );
            fixture.task.abort();
        }
    }
    #[tokio::test]
    async fn a_previous_unary_authentication_rejection_cannot_cancel_a_new_browser_login() {
        let _lock = super::super::test_lock().lock().await;
        let temp = tempfile::tempdir().unwrap();
        super::super::set_config_root(temp.path());
        let fixture = fixture("success").await;
        credential::adopt_credential(
            json!({"accessToken":"previous","clientIdentity":"previous-client","backendUrl":fixture.origin}),
            None,
        );
        let request = tokio::spawn(credential::unary("/fixture-auth", json!({})));
        tokio::time::timeout(Duration::from_secs(2), fixture.entered.notified())
            .await
            .unwrap();
        let start = start_at(None, &fixture.origin);
        let id = start["flowId"].as_str().unwrap();
        fixture.release.notify_one();
        assert_eq!(
            request.await.unwrap().err().unwrap(),
            credential::NEEDS_REIMPORT
        );
        assert_eq!(state().lock().unwrap().login.as_ref().unwrap().id, id);
        assert_eq!(
            credential::status()["needsReimport"],
            true,
            "the prior rejected grant cannot start new model requests"
        );
        assert_eq!(
            login_poll(id).await["ok"],
            true,
            "the new browser authorization remains usable"
        );
        fixture.task.abort();
    }
}
