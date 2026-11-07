//! OAuth STS credentials and signed Coding Plan allowance reads.

use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicU64, Ordering};

const KEY_SESSION: &str = "consoleCredentials";
const KEY_LEGACY: &str = "consoleSessionToken";
const MAX_BODY: usize = 1024 * 1024;
static LOGIN_GENERATION: AtomicU64 = AtomicU64::new(0);
static USAGE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Clone)]
pub(crate) struct Endpoints {
    pub token: String,
    pub usage: String,
}

impl Default for Endpoints {
    fn default() -> Self {
        Self {
            token: "https://signin.volcengine.com/authorize/oauth/token".into(),
            usage: "https://open.volcengineapi.com/?Action=GetCodingPlanUsage&Version=2024-01-01"
                .into(),
        }
    }
}

#[cfg(test)]
pub(crate) static TEST_ENDPOINTS: std::sync::RwLock<Option<Endpoints>> =
    std::sync::RwLock::new(None);

pub(super) fn endpoints() -> Endpoints {
    #[cfg(test)]
    if let Some(value) = TEST_ENDPOINTS.read().unwrap().as_ref() {
        return value.clone();
    }
    Endpoints::default()
}

#[derive(Clone, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
pub(super) struct Session {
    access_key_id: String,
    secret_access_key: String,
    session_token: String,
    #[serde(default)]
    refresh_token: String,
    #[serde(default)]
    expires_at: i64,
    #[serde(default = "same_device_client")]
    client_id: String,
}

fn same_device_client() -> String {
    super::console::CLIENT_ID.into()
}

#[derive(Debug, PartialEq, Eq)]
pub enum GatewayError {
    NotLogin,
    Transport(String),
    Business(String),
}

impl GatewayError {
    pub(super) fn message(&self) -> String {
        match self {
            Self::NotLogin => "Ark console login needs renewal; sign in again".into(),
            Self::Transport(_) => {
                "Ark console allowance request failed; check your network and retry".into()
            }
            Self::Business(message) => message.clone(),
        }
    }
}

pub(super) fn http_client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(20))
        .redirect(reqwest::redirect::Policy::none())
        .user_agent(super::auth::user_agent())
        .build()
        .map_err(|_| "could not create the Ark console HTTP client".into())
}

pub(super) async fn response_json(mut response: reqwest::Response) -> Result<Value, String> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| "could not read the console response")?
    {
        if bytes.len() + chunk.len() > MAX_BODY {
            return Err("console response exceeded the size limit".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|_| "console returned an invalid response".into())
}

pub(super) fn parse_session(answer: &Value, previous: Option<&Session>) -> Result<Session, String> {
    let raw = answer
        .get("access_token")
        .ok_or("signin answered without STS credentials")?;
    let access = match raw.as_str() {
        Some(value) => {
            serde_json::from_str(value).map_err(|_| "signin returned invalid STS credentials")?
        }
        None => raw.clone(),
    };
    let mut session: Session =
        serde_json::from_value(access).map_err(|_| "signin returned incomplete STS credentials")?;
    if [
        &session.access_key_id,
        &session.secret_access_key,
        &session.session_token,
    ]
    .iter()
    .any(|value| value.trim().is_empty())
    {
        return Err("signin returned incomplete STS credentials".into());
    }
    let expires = answer
        .get("expires_in")
        .and_then(Value::as_i64)
        .filter(|value| *value > 0 && *value <= 31_536_000)
        .ok_or("signin returned an invalid credential lifetime")?;
    session.client_id = same_device_client();
    session.expires_at = chrono::Utc::now().timestamp_millis() + expires * 1000;
    session.refresh_token = answer
        .get("refresh_token")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .or_else(|| previous.map(|value| value.refresh_token.clone()))
        .unwrap_or_default();
    Ok(session)
}

fn session_value(store: &Map<String, Value>) -> Option<Value> {
    store
        .get(KEY_SESSION)
        .or_else(|| store.get(KEY_LEGACY))
        .cloned()
}

fn read_session() -> Option<(Value, Session)> {
    let store = super::auth::read_store_public()?;
    let snapshot = session_value(&store)?;
    let raw = match snapshot.as_str() {
        Some(value) => serde_json::from_str(value).ok()?,
        None => snapshot.clone(),
    };
    let session: Session = serde_json::from_value(raw).ok()?;
    if [
        &session.access_key_id,
        &session.secret_access_key,
        &session.session_token,
    ]
    .iter()
    .any(|value| value.trim().is_empty())
    {
        return None;
    }
    Some((snapshot, session))
}

pub fn console_status() -> Value {
    json!({"loggedIn": read_session().is_some()})
}

pub(super) fn invalidate_pending_login() {
    LOGIN_GENERATION.fetch_add(1, Ordering::SeqCst);
}

pub(super) fn login_generation() -> u64 {
    LOGIN_GENERATION.fetch_add(1, Ordering::SeqCst) + 1
}

pub(super) fn store_session(session: &Session, generation: u64) -> Result<(), String> {
    let value =
        serde_json::to_value(session).map_err(|_| "could not serialize the console credential")?;
    let stored = super::auth::update_store_public(|store| {
        if LOGIN_GENERATION.load(Ordering::SeqCst) != generation {
            return false;
        }
        store.remove(KEY_LEGACY);
        store.remove("consoleLoginAt");
        store.insert(KEY_SESSION.into(), value);
        true
    })?;
    if stored {
        Ok(())
    } else {
        Err("console login was cancelled or superseded".into())
    }
}

fn replace_session(expected: &Value, replacement: Option<&Session>) -> Result<bool, GatewayError> {
    let replacement = replacement
        .map(serde_json::to_value)
        .transpose()
        .map_err(|_| GatewayError::Business("Ark console credential could not be saved".into()))?;
    super::auth::update_store_public(|store| {
        if session_value(store).as_ref() != Some(expected) {
            return false;
        }
        store.remove(KEY_LEGACY);
        store.remove(KEY_SESSION);
        store.remove("consoleLoginAt");
        if let Some(value) = replacement {
            store.insert(KEY_SESSION.into(), value);
        }
        true
    })
    .map_err(|_| GatewayError::Business("Ark console credential could not be saved".into()))
}

pub fn clear_console_token() -> Result<(), String> {
    // Invalidate callbacks while holding the same store lock used by their saves.
    super::auth::update_store_public(|store| {
        invalidate_pending_login();
        let changed = session_value(store).is_some();
        store.remove(KEY_SESSION);
        store.remove(KEY_LEGACY);
        store.remove("consoleLoginAt");
        changed
    })?;
    Ok(())
}

fn reject_session(snapshot: &Value) -> Result<Session, GatewayError> {
    if replace_session(snapshot, None)? {
        Err(GatewayError::NotLogin)
    } else {
        Err(GatewayError::Business(
            "Ark console login changed; refresh the allowance again".into(),
        ))
    }
}

async fn refresh(
    http: &reqwest::Client,
    snapshot: &Value,
    session: &Session,
) -> Result<Session, GatewayError> {
    if session.refresh_token.is_empty() {
        return reject_session(snapshot);
    }
    let response = http
        .post(&endpoints().token)
        .form(&[
            ("grant_type", "refresh_token"),
            ("refresh_token", session.refresh_token.as_str()),
            ("client_id", session.client_id.as_str()),
            ("scope", super::console::SCOPE),
        ])
        .send()
        .await
        .map_err(|_| GatewayError::Transport("refresh failed".into()))?;
    let status = response.status().as_u16();
    let answer = response_json(response)
        .await
        .map_err(GatewayError::Transport)?;
    if status == 400 && answer.get("error").and_then(Value::as_str) == Some("invalid_grant") {
        return reject_session(snapshot);
    }
    if !(200..300).contains(&status) {
        return Err(GatewayError::Business(format!(
            "Ark console token refresh failed (HTTP {status})"
        )));
    }
    let updated = parse_session(&answer, Some(session))
        .map_err(|_| GatewayError::Business("Ark console returned invalid credentials".into()))?;
    if !replace_session(snapshot, Some(&updated))? {
        return Err(GatewayError::Business(
            "Ark console login changed; refresh the allowance again".into(),
        ));
    }
    Ok(updated)
}

fn hmac(key: &[u8], value: &str) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(value.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

fn signed_request(
    http: &reqwest::Client,
    endpoint: &str,
    session: &Session,
    date: &str,
) -> Result<reqwest::RequestBuilder, GatewayError> {
    let url = reqwest::Url::parse(endpoint)
        .map_err(|_| GatewayError::Business("Ark console usage endpoint is invalid".into()))?;
    let host = &url[url::Position::BeforeHost..url::Position::AfterPort];
    let payload_hash = format!("{:x}", Sha256::digest(b"{}"));
    let signed_headers = "content-type;host;x-content-sha256;x-date;x-security-token";
    let canonical_headers = format!("content-type:application/json\nhost:{host}\nx-content-sha256:{payload_hash}\nx-date:{date}\nx-security-token:{}\n", session.session_token);
    let mut query: Vec<_> = url
        .query_pairs()
        .map(|(key, value)| {
            (
                super::console::urlencoded(&key),
                super::console::urlencoded(&value),
            )
        })
        .collect();
    query.sort();
    let query = query
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&");
    let canonical = format!(
        "POST\n{}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}",
        url.path()
    );
    let day = &date[..8];
    let scope = format!("{day}/cn-beijing/ark/request");
    let to_sign = format!(
        "HMAC-SHA256\n{date}\n{scope}\n{:x}",
        Sha256::digest(canonical.as_bytes())
    );
    let mut key = hmac(session.secret_access_key.as_bytes(), day);
    for part in ["cn-beijing", "ark", "request"] {
        key = hmac(&key, part);
    }
    let signature = hmac(&key, &to_sign)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let authorization = format!(
        "HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        session.access_key_id
    );
    Ok(http
        .post(url)
        .header("content-type", "application/json")
        .header("x-content-sha256", payload_hash)
        .header("x-date", date)
        .header("x-security-token", &session.session_token)
        .header("authorization", authorization)
        .body("{}"))
}

pub async fn quota_windows() -> Result<Vec<Value>, GatewayError> {
    let _guard = USAGE_LOCK.lock().await;
    let Some((mut snapshot, mut session)) = read_session() else {
        return Ok(Vec::new());
    };
    let http = http_client().map_err(GatewayError::Transport)?;
    let mut refreshed = false;
    if session.expires_at > 0
        && session.expires_at <= chrono::Utc::now().timestamp_millis() + 60_000
    {
        session = refresh(&http, &snapshot, &session).await?;
        snapshot = serde_json::to_value(&session).expect("session serializes");
        refreshed = true;
    }
    loop {
        let date = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let response = signed_request(&http, &endpoints().usage, &session, &date)?
            .send()
            .await
            .map_err(|_| GatewayError::Transport("usage failed".into()))?;
        let status = response.status().as_u16();
        let answer = response_json(response)
            .await
            .map_err(GatewayError::Transport)?;
        let code = answer
            .pointer("/ResponseMetadata/Error/Code")
            .and_then(Value::as_str);
        if matches!(
            code,
            Some("ExpiredToken" | "InvalidSecurityToken" | "InvalidToken")
        ) && !refreshed
        {
            session = refresh(&http, &snapshot, &session).await?;
            refreshed = true;
            continue;
        }
        if !(200..300).contains(&status) || code.is_some() {
            return Err(GatewayError::Business(format!(
                "Ark console allowance request failed (HTTP {status})"
            )));
        }
        return project_windows(&answer);
    }
}

fn project_windows(answer: &Value) -> Result<Vec<Value>, GatewayError> {
    let root = answer.get("Result").unwrap_or(answer);
    let rows = root
        .get("QuotaUsage")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            GatewayError::Business("Ark console returned an invalid allowance response".into())
        })?;
    let mut windows = Vec::new();
    for (level, id) in [
        ("session", "five_hour"),
        ("weekly", "weekly"),
        ("monthly", "monthly"),
    ] {
        let Some(row) = rows
            .iter()
            .find(|row| row.get("Level").and_then(Value::as_str) == Some(level))
        else {
            continue;
        };
        let used = row
            .get("Percent")
            .and_then(Value::as_f64)
            .filter(|value| value.is_finite() && (0.0..=100.0).contains(value))
            .ok_or_else(|| {
                GatewayError::Business("Ark console returned an invalid allowance response".into())
            })?;
        let mut window = json!({"id":id,"usedPercent":used});
        if let Some(reset) = row
            .get("ResetTimestamp")
            .and_then(Value::as_i64)
            .filter(|value| *value > 0)
        {
            if let Some(ms) = reset.checked_mul(1000) {
                window["resetsAt"] = json!(ms);
            }
        }
        windows.push(window);
    }
    if !rows.is_empty() && windows.is_empty() {
        return Err(GatewayError::Business(
            "Ark console returned an invalid allowance response".into(),
        ));
    }
    Ok(windows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ark_console_sts_signature_matches_the_fixed_protocol_vector() {
        // Fixed date, fake STS credentials, and a signature generated by the
        // provider's published V4 signer pin the wire contract independently.
        let session: Session = serde_json::from_value(json!({
            "access_key_id":"fixture-ak", "secret_access_key":"fixture-secret",
            "session_token":"fixture-security-token",
        }))
        .unwrap();
        let request = signed_request(
            &http_client().unwrap(),
            &Endpoints::default().usage,
            &session,
            "20261015T060000Z",
        )
        .unwrap()
        .build()
        .unwrap();
        assert_eq!(request.headers()["authorization"],
            "HMAC-SHA256 Credential=fixture-ak/20261015/cn-beijing/ark/request, SignedHeaders=content-type;host;x-content-sha256;x-date;x-security-token, Signature=ef82a5a0c7fef28e8d139df2adf217b4c1f4a1fc1b8e5b2af5ee62c4570a88bb");
    }
}
