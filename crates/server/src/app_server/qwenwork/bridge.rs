//! Loopback OpenAI-compatible bridge for QwenWork MaaS inference.
//!
//! QwenWork issues short-lived MaaS session tokens through `/api/desktop-sessions`
//! and serves OpenAI chat completions on the returned MaaS origin. Keeping token
//! exchange, proactive renewal, and per-model request normalization inside an
//! in-process loopback server lets long-lived Klaus sessions and multi-tier
//! subagents share one provider configuration across token rotations.

use bytes::Bytes;
use futures::StreamExt;
use http_body_util::{BodyExt, Full, Limited, StreamBody};
use hyper::{
    body::{Frame, Incoming},
    service::service_fn,
    Method, Request, Response, StatusCode,
};
use hyper_util::rt::TokioIo;
use klaus_agent_core::observability::provider_diagnostics as diagnostics;
use serde_json::{json, Value};
use std::convert::Infallible;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex, OnceLock,
};
use std::time::Duration;
use tokio::{
    net::TcpListener,
    sync::{Mutex as AsyncMutex, Semaphore},
};

type Body = http_body_util::combinators::UnsyncBoxBody<Bytes, Infallible>;

const MAX_REQUEST_BYTES: usize = 32 * 1024 * 1024;
const MAX_ERROR_BYTES: usize = 64 * 1024;
const DEFAULT_MAAS_TTL_MS: i64 = 7_200_000;
const SESSION_TIMEOUT_SECS: u64 = 30;
const ACCEPT_RETRY_MIN: Duration = Duration::from_millis(50);
const ACCEPT_RETRY_MAX: Duration = Duration::from_millis(2_000);

static SESSION_EPOCH: AtomicU64 = AtomicU64::new(1);

struct Running {
    base_url: String,
    token: String,
    thinking: Arc<Mutex<Option<String>>>,
    serving: Arc<AtomicBool>,
}

#[derive(Default)]
struct BridgeState {
    running: Option<Running>,
    pending_thinking: Option<String>,
}

#[derive(Clone, Debug)]
struct MaasSession {
    epoch: u64,
    generation: u64,
    fingerprint: Option<String>,
    api_base: String,
    base_url: String,
    access_token: String,
    session_id: String,
    expires_at_ms: i64,
    ttl_ms: i64,
}

fn state() -> &'static Mutex<BridgeState> {
    static STATE: OnceLock<Mutex<BridgeState>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(BridgeState::default()))
}

fn session_cache() -> &'static AsyncMutex<Option<MaasSession>> {
    static CACHE: OnceLock<AsyncMutex<Option<MaasSession>>> = OnceLock::new();
    CACHE.get_or_init(|| AsyncMutex::new(None))
}

pub(crate) fn forget_session() {
    SESSION_EPOCH.fetch_add(1, Ordering::AcqRel);
}

fn http_client() -> reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT
        .get_or_init(|| {
            reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .pool_max_idle_per_host(8)
                .pool_idle_timeout(Duration::from_secs(30))
                .connect_timeout(Duration::from_secs(20))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new())
        })
        .clone()
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn accept_failure_is_transient(error: &std::io::Error) -> bool {
    use std::io::ErrorKind;
    if matches!(
        error.kind(),
        ErrorKind::ConnectionAborted
            | ErrorKind::ConnectionReset
            | ErrorKind::Interrupted
            | ErrorKind::WouldBlock
            | ErrorKind::TimedOut
            | ErrorKind::OutOfMemory
    ) {
        return true;
    }
    #[cfg(unix)]
    {
        matches!(
            error.raw_os_error(),
            Some(libc::EMFILE) | Some(libc::ENFILE) | Some(libc::ENOBUFS) | Some(libc::ENOMEM)
        )
    }
    #[cfg(not(unix))]
    {
        false
    }
}

pub(crate) fn set_thinking_level(level: Option<String>) {
    let mut guard = state().lock().unwrap_or_else(|p| p.into_inner());
    if let Some(running) = guard.running.as_ref() {
        *running.thinking.lock().unwrap_or_else(|p| p.into_inner()) = level;
    } else {
        guard.pending_thinking = level;
    }
}

pub(crate) async fn ensure_started() -> Result<(String, String), String> {
    let carried_thinking = {
        let guard = state().lock().unwrap_or_else(|p| p.into_inner());
        match guard.running.as_ref() {
            Some(running) if running.serving.load(Ordering::Acquire) => {
                return Ok((running.base_url.clone(), running.token.clone()));
            }
            Some(running) => Some(running.thinking.clone()),
            None => guard
                .pending_thinking
                .clone()
                .map(|level| Arc::new(Mutex::new(Some(level)))),
        }
    };

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|error| error.to_string())?;
    let port = listener
        .local_addr()
        .map_err(|error| error.to_string())?
        .port();
    let base_url = format!("http://127.0.0.1:{port}/v1");
    let token = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let thinking = carried_thinking.unwrap_or_else(|| Arc::new(Mutex::new(None)));
    let serving = Arc::new(AtomicBool::new(true));

    {
        let mut guard = state().lock().unwrap_or_else(|p| p.into_inner());
        if let Some(running) = guard
            .running
            .as_ref()
            .filter(|running| running.serving.load(Ordering::Acquire))
        {
            let winner_base = running.base_url.clone();
            let winner_token = running.token.clone();
            let winner_thinking = running.thinking.clone();
            if let Some(level) = guard.pending_thinking.take() {
                *winner_thinking.lock().unwrap_or_else(|p| p.into_inner()) = Some(level);
            }
            return Ok((winner_base, winner_token));
        }
        if let Some(level) = guard.pending_thinking.take() {
            *thinking.lock().unwrap_or_else(|p| p.into_inner()) = Some(level);
        }
        guard.running = Some(Running {
            base_url: base_url.clone(),
            token: token.clone(),
            thinking: thinking.clone(),
            serving: serving.clone(),
        });
    }

    let private_token = token.clone();
    tokio::spawn(async move {
        let slots = Arc::new(Semaphore::new(32));
        let mut retry = ACCEPT_RETRY_MIN;
        loop {
            match listener.accept().await {
                Ok((socket, address)) => {
                    retry = ACCEPT_RETRY_MIN;
                    if !address.ip().is_loopback() {
                        continue;
                    }
                    let Ok(permit) = slots.clone().try_acquire_owned() else {
                        continue;
                    };
                    let token = private_token.clone();
                    let thinking = thinking.clone();
                    tokio::spawn(async move {
                        let _permit = permit;
                        let service = service_fn(move |request| {
                            handle(request, port, token.clone(), thinking.clone())
                        });
                        let _ = hyper::server::conn::http1::Builder::new()
                            .keep_alive(false)
                            .serve_connection(TokioIo::new(socket), service)
                            .await;
                    });
                }
                Err(error) if accept_failure_is_transient(&error) => {
                    eprintln!("[QwenWorkBridge] accept retry in {retry:?}: {error}");
                    tokio::time::sleep(retry).await;
                    retry = (retry * 2).min(ACCEPT_RETRY_MAX);
                }
                Err(error) => {
                    eprintln!("[QwenWorkBridge] listener closed, endpoint retired: {error}");
                    break;
                }
            }
        }
        serving.store(false, Ordering::Release);
    });

    eprintln!("[QwenWorkBridge] listening on {base_url}");
    Ok((base_url, token))
}

fn full_json(status: StatusCode, payload: &Value) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(
            Full::new(Bytes::from(payload.to_string()))
                .map_err(|never| match never {})
                .boxed_unsync(),
        )
        .expect("response builder")
}

fn error_json(status: StatusCode, err_type: &str, message: impl Into<String>) -> Response<Body> {
    full_json(
        status,
        &json!({
            "error": {
                "type": err_type,
                "message": message.into(),
            }
        }),
    )
}

fn is_loopback_host(host: Option<&str>, port: u16) -> bool {
    let Some(raw) = host.map(str::trim).filter(|value| !value.is_empty()) else {
        return true;
    };
    let lower = raw.to_ascii_lowercase();
    lower == format!("127.0.0.1:{port}")
        || lower == "127.0.0.1"
        || lower == format!("localhost:{port}")
        || lower == "localhost"
}

async fn handle(
    request: Request<Incoming>,
    port: u16,
    token: String,
    thinking: Arc<Mutex<Option<String>>>,
) -> Result<Response<Body>, Infallible> {
    let host = request
        .headers()
        .get("host")
        .and_then(|value| value.to_str().ok());
    if !is_loopback_host(host, port) {
        return Ok(error_json(
            StatusCode::FORBIDDEN,
            "forbidden",
            "Loopback only",
        ));
    }

    let authorized = request
        .headers()
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == format!("Bearer {token}"));
    if !authorized {
        diagnostics::record(
            "bridge_token_mismatch",
            json!({
                "source": "QwenWork",
                "sent": request.headers().contains_key("authorization"),
            }),
        );
        return Ok(error_json(
            StatusCode::UNAUTHORIZED,
            "authentication_error",
            "Invalid bridge token",
        ));
    }

    let path = request.uri().path().to_string();
    if request.method() == Method::GET && matches!(path.as_str(), "/v1/models" | "/models") {
        let models = super::fetch_models().await.unwrap_or_default();
        let data: Vec<Value> = models
            .iter()
            .map(|model| {
                json!({
                    "id": model.get("key").and_then(Value::as_str).unwrap_or(""),
                    "object": "model",
                    "owned_by": "qwenwork",
                    "display_name": model.get("display_name").cloned().unwrap_or(Value::Null),
                })
            })
            .collect();
        return Ok(full_json(
            StatusCode::OK,
            &json!({ "object": "list", "data": data }),
        ));
    }

    if request.method() != Method::POST
        || !matches!(path.as_str(), "/v1/chat/completions" | "/chat/completions")
        || request.uri().query().is_some()
    {
        return Ok(error_json(
            StatusCode::NOT_FOUND,
            "not_found",
            format!("Unknown endpoint: {path}"),
        ));
    }

    let collected = match Limited::new(request.into_body(), MAX_REQUEST_BYTES)
        .collect()
        .await
    {
        Ok(body) => body.to_bytes(),
        Err(_) => {
            return Ok(error_json(
                StatusCode::PAYLOAD_TOO_LARGE,
                "invalid_request_error",
                "QwenWork request payload exceeds maximum size",
            ));
        }
    };

    let body: Value = if collected.is_empty() {
        json!({})
    } else {
        match serde_json::from_slice(&collected) {
            Ok(parsed) => parsed,
            Err(error) => {
                return Ok(error_json(
                    StatusCode::BAD_REQUEST,
                    "invalid_request_error",
                    format!("Invalid JSON request body: {error}"),
                ));
            }
        }
    };

    let level = thinking.lock().unwrap_or_else(|p| p.into_inner()).clone();
    Ok(forward_chat_completions(body, level).await)
}

fn host_token(user: &Value) -> Result<String, String> {
    user.get("security_oauth_token")
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty() && !token.chars().any(char::is_control))
        .or_else(|| {
            user.get("access_token")
                .and_then(Value::as_str)
                .filter(|token| !token.is_empty() && !token.chars().any(char::is_control))
        })
        .map(str::to_string)
        .ok_or_else(|| "QwenWork subscription is not signed in".to_string())
}

fn bearer_header(token: &str) -> Result<reqwest::header::HeaderValue, String> {
    let mut header = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
        .map_err(|_| "QwenWork authorization token contains invalid characters".to_string())?;
    header.set_sensitive(true);
    Ok(header)
}

fn resolve_maas_base(payload: &Value) -> Result<String, String> {
    let candidate = std::env::var("QWENWORK_MAAS_BASE")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| {
            payload
                .get("base_url")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        })
        .unwrap_or_else(|| "https://maas.xiaoluozi.cn".to_string());
    super::capabilities::checked_base(&candidate)
        .map_err(|_| "QwenWork MaaS endpoint origin failed validation".to_string())
}

async fn exchange_maas_token(current: &MaasSession) -> Result<MaasSession, String> {
    let ttl_ms = if current.ttl_ms > 0 {
        current.ttl_ms
    } else {
        DEFAULT_MAAS_TTL_MS
    };
    let response = http_client()
        .post(format!("{}/access-tokens/exchange", current.base_url))
        .header(
            reqwest::header::AUTHORIZATION,
            bearer_header(&current.access_token)?,
        )
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .timeout(Duration::from_secs(SESSION_TIMEOUT_SECS))
        .json(&json!({ "ttl_ms": ttl_ms }))
        .send()
        .await
        .map_err(|error| error.to_string())?;
    if !response.status().is_success() {
        return Err(format!(
            "token exchange returned {}",
            response.status().as_u16()
        ));
    }
    let payload: Value = response.json().await.map_err(|error| error.to_string())?;
    let next_token = payload
        .get("access_token")
        .and_then(Value::as_str)
        .filter(|token| {
            !token.is_empty() && token.len() <= 16_384 && !token.chars().any(char::is_control)
        })
        .ok_or_else(|| "invalid exchanged MaaS access token".to_string())?
        .to_string();
    let now = now_ms();
    let expires_at_ms = payload
        .get("expires_at_ms")
        .or_else(|| payload.get("expires_at"))
        .and_then(Value::as_i64)
        .filter(|ts| *ts > now)
        .unwrap_or(now + ttl_ms);
    let mut updated = current.clone();
    updated.access_token = next_token;
    updated.expires_at_ms = expires_at_ms;
    Ok(updated)
}

async fn mint_maas_session(
    api: &str,
    oauth_token: &str,
    session_id: &str,
) -> Result<(u16, String), String> {
    let url = format!("{}/api/desktop-sessions", api.trim_end_matches('/'));
    let response = super::auth::with_client_headers(http_client().post(&url))
        .header(reqwest::header::AUTHORIZATION, bearer_header(oauth_token)?)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .timeout(Duration::from_secs(SESSION_TIMEOUT_SECS))
        .json(&json!({ "session_id": session_id }))
        .send()
        .await
        .map_err(|error| format!("cannot reach QwenWork session service: {error}"))?;
    let status = response.status().as_u16();
    let text = response.text().await.unwrap_or_default();
    Ok((status, text))
}

async fn acquire_maas_session(force_mint: bool) -> Result<MaasSession, (StatusCode, String)> {
    let mut guard = session_cache().lock().await;
    let _ = super::auth::ensure_fresh_token().await;

    let (mut user, mut generation) = match super::auth::native_snapshot() {
        Ok(snapshot) => snapshot,
        Err(_) => {
            let _ = super::auth::import_local();
            super::auth::native_snapshot().map_err(|_| {
                (
                    StatusCode::UNAUTHORIZED,
                    "QwenWork subscription is not signed in".to_string(),
                )
            })?
        }
    };
    let mut oauth_token = host_token(&user).map_err(|msg| (StatusCode::UNAUTHORIZED, msg))?;
    let mut fingerprint = super::auth::catalog_fingerprint();
    let api = super::catalog::api_base();
    let epoch = SESSION_EPOCH.load(Ordering::Acquire);

    let reused_session_id = guard
        .as_ref()
        .filter(|cached| {
            cached.epoch == epoch
                && cached.generation == generation
                && cached.fingerprint == fingerprint
                && cached.api_base == api
        })
        .map(|cached| cached.session_id.clone())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    if !force_mint {
        if let Some(cached) = guard.as_ref().filter(|cached| {
            cached.epoch == epoch
                && cached.generation == generation
                && cached.fingerprint == fingerprint
                && cached.api_base == api
        }) {
            let now = now_ms();
            let lead_ms = if cached.ttl_ms > 0 {
                (cached.ttl_ms / 2).clamp(1_000, 3_600_000)
            } else {
                600_000
            };
            if now.saturating_add(lead_ms) < cached.expires_at_ms {
                return Ok(cached.clone());
            }
            if now < cached.expires_at_ms {
                if let Ok(renewed) = exchange_maas_token(cached).await {
                    *guard = Some(renewed.clone());
                    return Ok(renewed);
                }
            }
        }
    }

    let (mut status, mut text) = mint_maas_session(&api, &oauth_token, &reused_session_id)
        .await
        .map_err(|msg| (StatusCode::BAD_GATEWAY, msg))?;

    if status == 401 || super::auth::login_rejected(status, &text) {
        let reread = super::auth::import_local()
            .ok()
            .is_some_and(|result| result.get("ok") == Some(&json!(true)));
        if !reread {
            let _ = super::auth::refresh_token_now().await;
        }
        if let Ok((next_user, next_gen)) = super::auth::native_snapshot() {
            if let Ok(next_token) = host_token(&next_user) {
                user = next_user;
                let _ = &user;
                generation = next_gen;
                oauth_token = next_token;
                fingerprint = super::auth::catalog_fingerprint();
                let retry = mint_maas_session(&api, &oauth_token, &reused_session_id)
                    .await
                    .map_err(|msg| (StatusCode::BAD_GATEWAY, msg))?;
                status = retry.0;
                text = retry.1;
            }
        }
    }

    if !(200..300).contains(&status) {
        let http_status = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
        let detail: String = text.chars().take(300).collect();
        return Err((
            http_status,
            format!("QwenWork session creation failed ({status}): {detail}"),
        ));
    }

    let payload: Value = serde_json::from_str(&text).map_err(|error| {
        (
            StatusCode::BAD_GATEWAY,
            format!("QwenWork session response is not JSON: {error}"),
        )
    })?;
    let access_token = payload
        .get("access_token")
        .and_then(Value::as_str)
        .filter(|token| {
            !token.is_empty() && token.len() <= 16_384 && !token.chars().any(char::is_control)
        })
        .ok_or_else(|| {
            (
                StatusCode::BAD_GATEWAY,
                "QwenWork session response omitted access_token".to_string(),
            )
        })?
        .to_string();
    let base_url = resolve_maas_base(&payload).map_err(|msg| (StatusCode::BAD_GATEWAY, msg))?;
    let session_id = payload
        .get("session_id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty() && id.len() <= 200 && !id.chars().any(char::is_control))
        .map(str::to_string)
        .unwrap_or(reused_session_id);
    let now = now_ms();
    let ttl_ms = payload
        .get("ttl_ms")
        .and_then(Value::as_i64)
        .filter(|ttl| *ttl > 0)
        .unwrap_or(DEFAULT_MAAS_TTL_MS);
    let expires_at_ms = payload
        .get("expires_at_ms")
        .or_else(|| payload.get("expires_at"))
        .and_then(Value::as_i64)
        .filter(|ts| *ts > now)
        .unwrap_or(now + ttl_ms);

    let minted = MaasSession {
        epoch: SESSION_EPOCH.load(Ordering::Acquire),
        generation,
        fingerprint,
        api_base: api,
        base_url,
        access_token,
        session_id,
        expires_at_ms,
        ttl_ms,
    };
    *guard = Some(minted.clone());
    Ok(minted)
}

pub(crate) fn prepare_chat_body(
    mut body: Value,
    model_entry: Option<&Value>,
    thinking_level: Option<&str>,
) -> Value {
    let Some(obj) = body.as_object_mut() else {
        return body;
    };

    let raw_model = obj
        .get("model")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("auto")
        .to_string();
    let resolved_model = model_entry
        .and_then(|entry| entry.get("key"))
        .and_then(Value::as_str)
        .filter(|key| !key.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| match raw_model.as_str() {
            "" | "auto" | "qwork-auto" => "flash".to_string(),
            other => other.to_string(),
        });
    obj.insert("model".into(), json!(resolved_model));

    if let Some(messages) = obj.get_mut("messages").and_then(Value::as_array_mut) {
        for message in messages {
            let Some(msg_obj) = message.as_object_mut() else {
                continue;
            };
            if msg_obj.get("role").and_then(Value::as_str) == Some("developer") {
                msg_obj.insert("role".into(), json!("system"));
            }
            if msg_obj.get("role").and_then(Value::as_str) == Some("assistant") {
                let has_tool_calls = msg_obj
                    .get("tool_calls")
                    .and_then(Value::as_array)
                    .is_some_and(|calls| !calls.is_empty());
                if has_tool_calls && !msg_obj.contains_key("reasoning_content") {
                    msg_obj.insert("reasoning_content".into(), json!(""));
                }
            }
        }
    }

    if let Some(existing) = obj
        .get("reasoning_effort")
        .and_then(Value::as_str)
        .map(str::trim)
        .map(str::to_ascii_lowercase)
    {
        if existing == "off" {
            obj.insert("reasoning_effort".into(), json!("none"));
        }
    } else if let Some(level) = thinking_level
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let lower = level.to_ascii_lowercase();
        if matches!(lower.as_str(), "off" | "none") {
            let can_disable = model_entry
                .and_then(|entry| entry.get("canDisableThinking"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if can_disable {
                obj.insert("reasoning_effort".into(), json!("none"));
            }
        } else if let Some(normalized) = super::super::qoder::catalog::engine_effort_level(&lower) {
            if let Some(supported) = model_entry
                .and_then(|entry| entry.get("efforts"))
                .and_then(Value::as_array)
            {
                let supported_strs: Vec<&str> =
                    supported.iter().filter_map(Value::as_str).collect();
                if supported_strs.contains(&normalized) {
                    obj.insert("reasoning_effort".into(), json!(normalized));
                } else if let Some(default_effort) = model_entry
                    .and_then(|entry| entry.get("defaultEffort"))
                    .and_then(Value::as_str)
                {
                    obj.insert("reasoning_effort".into(), json!(default_effort));
                }
            }
        }
    }

    if resolved_model == "lite"
        && !obj.contains_key("reasoning_effort")
        && !obj.contains_key("thinking")
    {
        obj.insert("reasoning_effort".into(), json!("none"));
    }

    let thinking_disabled = obj.get("reasoning_effort").and_then(Value::as_str) == Some("none");
    if let Some(params) = model_entry
        .and_then(|entry| entry.get("model_params"))
        .and_then(Value::as_object)
    {
        for (key, value) in params {
            if thinking_disabled && key == "thinking" {
                continue;
            }
            if !obj.contains_key(key) {
                obj.insert(key.clone(), value.clone());
            }
        }
    }

    body
}

fn format_upstream_error(status: StatusCode, raw: &str) -> Response<Body> {
    if let Ok(parsed) = serde_json::from_str::<Value>(raw) {
        let code = parsed
            .get("code")
            .and_then(|v| {
                v.as_str()
                    .map(str::to_string)
                    .or_else(|| v.as_i64().map(|n| n.to_string()))
            })
            .or_else(|| {
                parsed
                    .get("error")
                    .and_then(|e| e.get("code"))
                    .and_then(Value::as_str)
                    .map(str::to_string)
            });
        if status == StatusCode::PAYMENT_REQUIRED || code.as_deref() == Some("112") {
            let msg = parsed
                .get("message")
                .and_then(Value::as_str)
                .or_else(|| {
                    parsed
                        .get("error")
                        .and_then(|e| e.get("message"))
                        .and_then(Value::as_str)
                })
                .unwrap_or("QwenWork subscription quota exhausted");
            return full_json(
                StatusCode::PAYMENT_REQUIRED,
                &json!({
                    "error": {
                        "type": "insufficient_quota",
                        "code": "insufficient_quota",
                        "message": format!("QwenWork quota exhausted: {msg}"),
                    }
                }),
            );
        }
        if let Some(err_obj) = parsed.get("error").filter(|e| e.is_object()) {
            let mut enriched = err_obj.clone();
            if let Some(message) = enriched.get("message").and_then(Value::as_str) {
                if !message.starts_with("QwenWork") {
                    enriched["message"] = json!(format!("QwenWork upstream: {message}"));
                }
            }
            return full_json(status, &json!({ "error": enriched }));
        }
        if let Some(message) = parsed
            .get("message")
            .or_else(|| parsed.get("detail"))
            .and_then(Value::as_str)
            .filter(|text| !text.trim().is_empty())
        {
            return error_json(
                status,
                "upstream_error",
                format!("QwenWork upstream {}: {message}", status.as_u16()),
            );
        }
    }
    let snippet: String = raw.trim().chars().take(300).collect();
    let message = if snippet.is_empty() {
        format!("QwenWork upstream returned HTTP {}", status.as_u16())
    } else {
        format!("QwenWork upstream {}: {snippet}", status.as_u16())
    };
    error_json(status, "upstream_error", message)
}

async fn send_maas_chat(
    session: &MaasSession,
    payload: &Value,
    wants_stream: bool,
) -> Result<reqwest::Response, String> {
    let accept = if wants_stream {
        "text/event-stream"
    } else {
        "application/json"
    };
    http_client()
        .post(format!("{}/chat/completions", session.base_url))
        .header(
            reqwest::header::AUTHORIZATION,
            bearer_header(&session.access_token)?,
        )
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .header(reqwest::header::ACCEPT, accept)
        .header("x-session-id", &session.session_id)
        .header("x-mm-client-request-source", "qwenwork_desktop")
        .header("x-mm-device-type", std::env::consts::OS)
        .json(payload)
        .send()
        .await
        .map_err(|error| format!("QwenWork MaaS transport error: {error}"))
}

async fn forward_chat_completions(body: Value, thinking_level: Option<String>) -> Response<Body> {
    let wants_stream = body.get("stream") == Some(&json!(true));
    let requested = body
        .get("model")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("auto")
        .to_string();
    let model_entry = if requested == "lite" {
        None
    } else {
        super::lookup_model(&requested).await
    };
    let prepared = prepare_chat_body(body, model_entry.as_ref(), thinking_level.as_deref());

    let mut session = match acquire_maas_session(false).await {
        Ok(session) => session,
        Err((status, message)) => return error_json(status, "authentication_error", message),
    };

    let mut response = match send_maas_chat(&session, &prepared, wants_stream).await {
        Ok(response) => response,
        Err(message) => return error_json(StatusCode::BAD_GATEWAY, "upstream_error", message),
    };

    if response.status() == reqwest::StatusCode::UNAUTHORIZED {
        forget_session();
        if let Ok(fresh) = acquire_maas_session(true).await {
            session = fresh;
            let _ = &session;
            if let Ok(retried) = send_maas_chat(&session, &prepared, wants_stream).await {
                response = retried;
            }
        }
    }

    let status =
        StatusCode::from_u16(response.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    if !status.is_success() {
        let mut buf = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(Ok(chunk)) = stream.next().await {
            let take = chunk.len().min(MAX_ERROR_BYTES.saturating_sub(buf.len()));
            buf.extend_from_slice(&chunk[..take]);
            if buf.len() >= MAX_ERROR_BYTES {
                break;
            }
        }
        let raw = String::from_utf8_lossy(&buf);
        return format_upstream_error(status, &raw);
    }

    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or(if wants_stream {
            "text/event-stream; charset=utf-8"
        } else {
            "application/json"
        })
        .to_string();

    let stream = response.bytes_stream().filter_map(|chunk| async move {
        match chunk {
            Ok(bytes) => Some(Ok::<Frame<Bytes>, Infallible>(Frame::data(bytes))),
            Err(error) => {
                eprintln!("[QwenWorkBridge] upstream stream read error: {error}");
                None
            }
        }
    });

    Response::builder()
        .status(status)
        .header("content-type", content_type)
        .header("cache-control", "no-cache")
        .body(StreamBody::new(stream).boxed_unsync())
        .expect("stream response builder")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_roles_tool_call_reasoning_and_model_params() {
        let ling_entry = json!({
            "key": "ling-3.1-flash",
            "is_reasoning": true,
            "model_params": {
                "thinking": { "type": "enabled" }
            }
        });
        let request = json!({
            "model": "ling-3.1-flash",
            "stream": true,
            "messages": [
                { "role": "developer", "content": "Follow instructions" },
                { "role": "user", "content": "Read cargo.toml" },
                {
                    "role": "assistant",
                    "content": null,
                    "tool_calls": [{
                        "id": "call_1",
                        "type": "function",
                        "function": { "name": "read_file", "arguments": "{}" }
                    }]
                },
                { "role": "tool", "tool_call_id": "call_1", "content": "ok" }
            ]
        });
        let prepared = prepare_chat_body(request, Some(&ling_entry), Some("high"));
        assert_eq!(prepared["model"], "ling-3.1-flash");
        assert_eq!(prepared["messages"][0]["role"], "system");
        assert_eq!(prepared["messages"][2]["reasoning_content"], "");
        assert_eq!(prepared["thinking"], json!({ "type": "enabled" }));
        assert!(prepared.get("reasoning_effort").is_none());

        let disabled = prepare_chat_body(
            json!({
                "model": "ling-3.1-flash",
                "reasoning_effort": "none",
                "messages": [{ "role": "user", "content": "title" }]
            }),
            Some(&ling_entry),
            None,
        );
        assert_eq!(disabled["reasoning_effort"], "none");
        assert!(disabled.get("thinking").is_none());

        let lite = prepare_chat_body(
            json!({
                "model": "lite",
                "max_completion_tokens": 32,
                "messages": [{ "role": "user", "content": "Reply AUX_OK" }]
            }),
            None,
            Some("high"),
        );
        assert_eq!(lite["model"], "lite");
        assert_eq!(lite["reasoning_effort"], "none");
    }

    #[tokio::test]
    async fn mints_exchanges_retries_401_and_streams_over_loopback_bridge() {
        let _env_guard = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());

        let calls: Arc<Mutex<Vec<(String, String, Value)>>> = Arc::new(Mutex::new(Vec::new()));
        let mint_count = Arc::new(AtomicU64::new(0));
        let exchange_count = Arc::new(AtomicU64::new(0));
        let force_next_401 = Arc::new(AtomicBool::new(false));
        let short_ttl_ms = Arc::new(AtomicU64::new(7_200_000));

        let upstream = TcpListener::bind("127.0.0.1:0").await.expect("upstream");
        let upstream_port = upstream.local_addr().unwrap().port();
        let upstream_origin = format!("http://127.0.0.1:{upstream_port}");

        let calls_clone = calls.clone();
        let mint_clone = mint_count.clone();
        let exchange_clone = exchange_count.clone();
        let force_401_clone = force_next_401.clone();
        let ttl_clone = short_ttl_ms.clone();
        let origin_for_server = upstream_origin.clone();

        tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = upstream.accept().await else {
                    break;
                };
                let calls = calls_clone.clone();
                let mint_count = mint_clone.clone();
                let exchange_count = exchange_clone.clone();
                let force_401 = force_401_clone.clone();
                let ttl_ms = ttl_clone.clone();
                let origin = origin_for_server.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |req: Request<Incoming>| {
                        let calls = calls.clone();
                        let mint_count = mint_count.clone();
                        let exchange_count = exchange_count.clone();
                        let force_401 = force_401.clone();
                        let ttl_ms = ttl_ms.clone();
                        let origin = origin.clone();
                        async move {
                            let path = req.uri().path().to_string();
                            let auth = req
                                .headers()
                                .get("authorization")
                                .and_then(|v| v.to_str().ok())
                                .unwrap_or("")
                                .to_string();
                            let bytes = req.into_body().collect().await.unwrap().to_bytes();
                            let body: Value = if bytes.is_empty() {
                                Value::Null
                            } else {
                                serde_json::from_slice(&bytes).unwrap_or(Value::Null)
                            };
                            calls
                                .lock()
                                .unwrap()
                                .push((path.clone(), auth.clone(), body.clone()));

                            let (status, content_type, payload) = match path.as_str() {
                                "/api/desktop/chat-modes" => (
                                    StatusCode::OK,
                                    "application/json",
                                    json!({
                                        "schema_version": 2,
                                        "catalog": [{
                                            "key": "ling-3.1-flash",
                                            "i18n": { "display_name": { "zh": "Ling-3.1-Flash" } },
                                            "is_reasoning": true,
                                            "model_params": { "thinking": { "type": "enabled" } },
                                            "enable": true
                                        }]
                                    })
                                    .to_string(),
                                ),
                                "/api/desktop-sessions" => {
                                    let idx = mint_count.fetch_add(1, Ordering::SeqCst) + 1;
                                    let ttl = ttl_ms.load(Ordering::SeqCst) as i64;
                                    (
                                        StatusCode::OK,
                                        "application/json",
                                        json!({
                                            "access_token": format!("maas-token-{idx}"),
                                            "base_url": origin,
                                            "session_id": body["session_id"],
                                            "ttl_ms": ttl,
                                            "expires_at_ms": now_ms() + ttl,
                                        })
                                        .to_string(),
                                    )
                                }
                                "/v1/access-tokens/exchange" => {
                                    let idx = exchange_count.fetch_add(1, Ordering::SeqCst) + 1;
                                    (
                                        StatusCode::OK,
                                        "application/json",
                                        json!({
                                            "access_token": format!("exchanged-token-{idx}"),
                                            "expires_at": now_ms() + 7_200_000,
                                        })
                                        .to_string(),
                                    )
                                }
                                "/v1/chat/completions" => {
                                    if force_401.swap(false, Ordering::SeqCst) {
                                        (
                                            StatusCode::UNAUTHORIZED,
                                            "application/json",
                                            json!({ "error": { "message": "expired maas token" } })
                                                .to_string(),
                                        )
                                    } else {
                                        (
                                            StatusCode::OK,
                                            "text/event-stream",
                                            "data: {\"id\":\"1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"OK\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"
                                                .to_string(),
                                        )
                                    }
                                }
                                _ => (StatusCode::NOT_FOUND, "application/json", "{}".to_string()),
                            };
                            Ok::<_, Infallible>(
                                Response::builder()
                                    .status(status)
                                    .header("content-type", content_type)
                                    .body(Full::new(Bytes::from(payload)))
                                    .unwrap(),
                            )
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(socket), service)
                        .await;
                });
            }
        });

        std::env::set_var("QWENWORK_API_BASE", &upstream_origin);
        std::env::remove_var("QWENWORK_MAAS_BASE");
        super::super::adopt_credential(
            json!({
                "uid": "u-1",
                "name": "Tester",
                "security_oauth_token": "oauth-token-1",
                "access_token": "oauth-token-1",
                "refresh_token": "refresh-1",
                "expire_time": chrono::Utc::now().timestamp() + 86_400,
            }),
            Some("01234567-89ab-cdef-0123-456789abcdef".into()),
        )
        .expect("adopt");

        let (provider, auth) = super::super::resolve("ling-3.1-flash")
            .await
            .expect("resolve");
        assert_eq!(provider["model"], "ling-3.1-flash");
        assert_eq!(provider["compatibility"]["instructionRole"], "system");
        let base_url = provider["baseUrl"].as_str().unwrap().to_string();
        let bridge_token = auth["token"].as_str().unwrap().to_string();

        // 1. Streaming request mints once and injects model_params.
        let resp = http_client()
            .post(format!("{base_url}/chat/completions"))
            .bearer_auth(&bridge_token)
            .json(&json!({
                "model": "ling-3.1-flash",
                "stream": true,
                "messages": [{ "role": "developer", "content": "sys" }, { "role": "user", "content": "hi" }]
            }))
            .send()
            .await
            .expect("chat 1");
        assert_eq!(resp.status(), 200);
        let text = resp.text().await.unwrap();
        assert!(text.contains("\"content\":\"OK\""));
        assert_eq!(mint_count.load(Ordering::SeqCst), 1);

        // 2. Upstream 401 triggers transparent re-mint + retry.
        force_next_401.store(true, Ordering::SeqCst);
        let resp2 = http_client()
            .post(format!("{base_url}/chat/completions"))
            .bearer_auth(&bridge_token)
            .json(&json!({
                "model": "ling-3.1-flash",
                "stream": true,
                "messages": [{ "role": "user", "content": "retry" }]
            }))
            .send()
            .await
            .expect("chat 2");
        assert_eq!(resp2.status(), 200);
        assert_eq!(mint_count.load(Ordering::SeqCst), 2);

        // 3. Proactive half-TTL renewal calls /v1/access-tokens/exchange.
        {
            let mut guard = session_cache().lock().await;
            if let Some(session) = guard.as_mut() {
                session.ttl_ms = 4_000;
                session.expires_at_ms = now_ms() + 1_500;
            }
        }
        let resp3 = http_client()
            .post(format!("{base_url}/chat/completions"))
            .bearer_auth(&bridge_token)
            .json(&json!({
                "model": "ling-3.1-flash",
                "stream": true,
                "messages": [{ "role": "user", "content": "exchange" }]
            }))
            .send()
            .await
            .expect("chat 3");
        assert_eq!(resp3.status(), 200);
        assert_eq!(exchange_count.load(Ordering::SeqCst), 1);

        let recorded = calls.lock().unwrap().clone();
        let last_chat = recorded
            .iter()
            .rev()
            .find(|(path, _, _)| path == "/v1/chat/completions")
            .expect("chat call");
        assert_eq!(last_chat.1, "Bearer exchanged-token-1");
        assert_eq!(last_chat.2["thinking"], json!({ "type": "enabled" }));

        let _ = super::super::logout();
        std::env::remove_var("QWENWORK_API_BASE");
    }
}
