//! Kimi credentials, the device-flow login, and the membership allowance.
//!
//! ## Two credentials, one gateway
//!
//! A Kimi Code API key created in the console and a device-flow login both
//! authenticate to `api.kimi.com/coding`, and the store keeps whichever the user
//! last supplied ([`auth_type`]). The key is the default because it is the only
//! route Kimi documents for third-party clients; the login exists so a user does
//! not have to visit the console.
//!
//! ## One honest User-Agent, everywhere
//!
//! Every request here — including the OAuth endpoints the official CLI calls — is
//! sent with Klaus's own `User-Agent` and no client-identity headers. Kimi's
//! terms name spoofing a client identity as a violation that can suspend a
//! membership, and measurement shows the spoof buys nothing: the device
//! authorization, token, refresh, models, usage and inference endpoints all
//! answer 200 to the honest agent. The login is therefore offered as an
//! unsanctioned convenience rather than dressed up as the official client, and
//! it can break if Kimi tightens that endpoint.
//!
//! ## Two writers may refresh
//!
//! An access token lives 900 seconds, so both this module (before a catalog or
//! usage read) and the provider's own OAuth renewal (mid-session) refresh. That
//! is safe here and would not be on a rotating single-use grant: measured, Kimi
//! rotates the refresh token on every exchange **and keeps the previous one
//! valid**, so a renewed copy cannot invalidate the other writer's. The
//! [`refresh_now`] path persists the rotated set immediately — the old one is
//! already superseded, and losing it is what would strand a login.

use serde_json::{json, Map, Value};
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

/// The OAuth host the official CLI uses. Overridable for a staging login or a
/// network that has to pin the endpoint.
const DEFAULT_OAUTH_HOST: &str = "https://auth.kimi.com";
/// The device authorization route. Note the underscore: `device-authorization`
/// (hyphenated, as several third-party write-ups spell it) answers 404 from this
/// host's router, and a 404 there reads as "the flow is gone" rather than "the
/// path is wrong".
const DEVICE_AUTHORIZATION_PATH: &str = "/api/oauth/device_authorization";
const TOKEN_PATH: &str = "/api/oauth/token";
/// The page the user confirms on. Carried in the authorize URL so a renewal can
/// reproduce it, and it is never requested: the browser posts there directly.
pub const VERIFICATION_URI: &str = "https://www.kimi.com/code/authorize_device";
/// A device flow has no loopback callback, but the wire requires a redirect URI
/// on the client config. This is the standard out-of-band marker saying so; only
/// `clientId` and `tokenUrl` are ever spent on a request.
pub const OUT_OF_BAND_REDIRECT: &str = "urn:ietf:wg:oauth:2.0:oob";
/// Kimi Code's own OAuth client. It is a public client with no secret, which is
/// why a device flow can run from here at all.
pub const CLIENT_ID: &str = "17e5f671-d194-4dfb-9706-5516cb48c098";
const DEVICE_CODE_GRANT: &str = "urn:ietf:params:oauth:grant-type:device_code";

/// The domestic gateway. The overseas twin is `api.kimi.ai`; a stored credential
/// carries whichever it was issued against so a login does not move regions.
const DEFAULT_API_BASE: &str = "https://api.kimi.com/coding";
const ME_PATH: &str = "/v1/me";
const USAGE_PATH: &str = "/v1/usages";

/// Where the provider persists a renewed token set. Stable and Kimi-specific so
/// a later session finds the rotation the previous one made.
pub const CREDENTIAL_ID: &str = "kimi-membership";

/// Refresh this long before expiry. The token lives 900s, so a two-minute skew
/// leaves ample room for a slow exchange while still renewing well inside the
/// life of the token a session is holding.
const REFRESH_SKEW_SECONDS: i64 = 120;
const HTTP_TIMEOUT_MS: u64 = 20_000;
/// The authorization grant is valid 1800s upstream; bound the local wait to the
/// same order so a forgotten browser tab cannot keep a flow alive forever.
const FLOW_TIMEOUT_MS: i64 = 1_800_000;

/// The client identity sent on every request. It says what this is rather than
/// what it is not: no `X-Msh-*` family, no CLI version.
fn user_agent() -> String {
    std::env::var("KIMI_USER_AGENT")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| format!("Klaus-Desktop/{}", client_version()))
}

fn client_version() -> String {
    option_env!("CARGO_PKG_VERSION")
        .unwrap_or("0.0.0")
        .to_string()
}

pub fn oauth_host() -> String {
    for key in ["KIMI_CODE_OAUTH_HOST", "KIMI_OAUTH_HOST"] {
        if let Ok(value) = std::env::var(key) {
            let trimmed = value.trim().trim_end_matches('/').to_string();
            if !trimmed.is_empty() {
                return trimmed;
            }
        }
    }
    DEFAULT_OAUTH_HOST.to_string()
}

/// The coding gateway this credential was issued against.
/// The gateway override a deployment pins. The device flow and the key path
/// share it: a login started against an overridden OAuth host has to store the
/// matching gateway, or its token is refused on first spend.
fn env_api_base() -> Option<String> {
    std::env::var("KIMI_API_BASE")
        .ok()
        .map(|value| value.trim().trim_end_matches('/').to_string())
        .filter(|value| !value.is_empty())
}

pub(crate) fn api_base() -> String {
    env_api_base().unwrap_or_else(|| {
        read_store()
            .and_then(|store| string_field(&store, "apiBase"))
            .unwrap_or_else(|| DEFAULT_API_BASE.trim_end_matches('/').to_string())
    })
}

fn store_path() -> PathBuf {
    super::config_root().join("kimi-subscription-oauth.json")
}

fn now_seconds() -> i64 {
    chrono::Utc::now().timestamp()
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Which credential the store holds, so callers can say which one is active
/// without exposing the secret itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KimiAuthType {
    ApiKey,
    OAuth,
}

impl KimiAuthType {
    fn as_str(self) -> &'static str {
        match self {
            Self::ApiKey => "apiKey",
            Self::OAuth => "oauth",
        }
    }
}

/// The credential a `kimi:` session resolves with.
pub enum Credential {
    /// Sent as `x-api-key`, the header the official Claude Code guide uses.
    ApiKey { key: String },
    /// Sent as `Authorization: Bearer` and renewable by the provider itself.
    OAuth {
        access_token: String,
        refresh_token: String,
        expires_at: i64,
    },
}

pub fn auth_type() -> Option<KimiAuthType> {
    read_store().and_then(|store| match string_field(&store, "authType").as_deref() {
        Some("apiKey") => Some(KimiAuthType::ApiKey),
        Some("oauth") => Some(KimiAuthType::OAuth),
        _ => None,
    })
}

fn read_store() -> Option<Value> {
    let raw = std::fs::read_to_string(store_path()).ok()?;
    serde_json::from_str(&raw).ok()
}

fn write_store(store: &Value) -> Result<(), String> {
    let path = store_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let serialized = serde_json::to_string_pretty(store).map_err(|error| error.to_string())?;
    std::fs::write(&path, serialized).map_err(|error| error.to_string())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

fn string_field(store: &Value, key: &str) -> Option<String> {
    store
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

/// Read the stored credential. The caller turns it into wire auth; the token
/// itself never crosses to a host.
pub fn credential() -> Option<Credential> {
    let store = read_store()?;
    match auth_type()? {
        KimiAuthType::ApiKey => {
            let key = string_field(&store, "apiKey")?;
            Some(Credential::ApiKey { key })
        }
        KimiAuthType::OAuth => {
            let oauth = store.get("oauth")?;
            let access_token = string_field(oauth, "accessToken")?;
            let refresh_token = string_field(oauth, "refreshToken").unwrap_or_default();
            let expires_at = oauth
                .get("expiresAt")
                .and_then(Value::as_i64)
                .unwrap_or_default();
            Some(Credential::OAuth {
                access_token,
                refresh_token,
                expires_at,
            })
        }
    }
}

/// The bearer/key value to authenticate a request with, renewing first if the
/// stored token is about to expire.
pub async fn access_token() -> Result<String, super::catalog::KimiError> {
    match credential() {
        Some(Credential::ApiKey { key }) => Ok(key),
        Some(Credential::OAuth { expires_at, .. }) => {
            if expires_at - REFRESH_SKEW_SECONDS <= now_seconds() {
                ensure_fresh_token()
                    .await
                    .map_err(|error| super::catalog::KimiError {
                        message: error,
                        not_found: false,
                        login_expired: true,
                    })?;
            }
            match credential() {
                Some(Credential::ApiKey { key }) => Ok(key),
                Some(Credential::OAuth { access_token, .. }) => Ok(access_token),
                None => Err(super::catalog::KimiError {
                    message: "the Kimi credential could not be read back".into(),
                    not_found: true,
                    login_expired: false,
                }),
            }
        }
        None => Err(super::catalog::KimiError {
            message: "Kimi membership is not signed in".into(),
            not_found: true,
            login_expired: false,
        }),
    }
}

fn client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(HTTP_TIMEOUT_MS))
        .user_agent(user_agent())
        .build()
        .map_err(|error| error.to_string())
}

/// Store a console API key, verifying it against the account route first.
///
/// Verifying here rather than at the first turn is the whole point: a mistyped
/// key would otherwise sit in settings looking accepted and fail inside a
/// conversation with the service's own message. The account read also captures
/// the plan and nickname, so the settings card can show who is connected
/// without spending the key again.
pub async fn set_api_key(key: &str, api_base: Option<&str>) -> Result<Value, Value> {
    let key = key.trim();
    if key.is_empty() {
        return Ok(json!({ "ok": false, "error": "kimi_api_key_empty" }));
    }
    let base = api_base
        .map(|value| value.trim().trim_end_matches('/').to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_API_BASE.trim_end_matches('/').to_string());

    let http = client().map_err(|error| json!({ "ok": false, "error": error }))?;
    // A console key is spent as `x-api-key`, the header the official Claude Code
    // guide configures for this gateway.
    let account = match fetch_account(&http, &base, ("x-api-key", key.to_string())).await {
        Ok(account) => account,
        Err(error) => return Ok(json!({ "ok": false, "error": error })),
    };

    let previous = read_store();
    let mut store = Map::new();
    store.insert("authType".into(), json!(KimiAuthType::ApiKey.as_str()));
    store.insert("apiKey".into(), json!(key));
    store.insert("apiBase".into(), json!(base));
    store.insert("account".into(), account);
    store.insert("updatedAt".into(), json!(now_ms()));
    // Keep an OAuth login's device id so switching back does not look like a new
    // installation to the service.
    if let Some(previous) = previous.as_ref() {
        if let Some(device_id) = string_field(previous, "deviceId") {
            store.insert("deviceId".into(), json!(device_id));
        }
    }
    super::catalog::forget_cache();
    match write_store(&Value::Object(store)) {
        Ok(()) => Ok(status()),
        Err(error) => Ok(json!({ "ok": false, "error": error })),
    }
}

/// The header a credential authenticates with.
///
/// The two kinds do not share one: a console key is spent as `x-api-key`, which
/// is what the official Claude Code guide configures, while a device-flow token
/// is an OAuth bearer. Measured, this gateway answers either header alone with
/// 200 — but sending both would leave it to the service to pick one, and a
/// request whose own client cannot say which credential it presented is a
/// request that cannot be reasoned about when it fails.
fn auth_header(kind: KimiAuthType, token: &str) -> (&'static str, String) {
    match kind {
        KimiAuthType::ApiKey => ("x-api-key", token.to_string()),
        KimiAuthType::OAuth => ("authorization", format!("Bearer {token}")),
    }
}

/// The credential to authenticate with, plus the header it belongs in.
async fn spending() -> Result<(KimiAuthType, &'static str, String), super::catalog::KimiError> {
    let kind = auth_type().ok_or_else(|| super::catalog::KimiError {
        message: "Kimi membership is not signed in".into(),
        not_found: true,
        login_expired: false,
    })?;
    let token = access_token().await?;
    let (name, value) = auth_header(kind, &token);
    Ok((kind, name, value))
}

/// The account behind a credential: nickname, plan tier and region.
async fn fetch_account(
    http: &reqwest::Client,
    base: &str,
    header: (&str, String),
) -> Result<Value, String> {
    let response = http
        .get(format!("{base}{ME_PATH}"))
        .header(header.0, header.1)
        .send()
        .await
        .map_err(|error| error.to_string())?;
    let status_code = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    if status_code == 401 || status_code == 403 {
        // A rejected key is the common case, and the service's own prose is about
        // its own clients. Name the stable condition instead.
        return Err("kimi_credential_rejected".to_string());
    }
    if !(200..300).contains(&status_code) {
        let detail: String = body.chars().take(200).collect();
        return Err(format!(
            "kimi account read failed ({status_code}): {detail}"
        ));
    }
    let parsed: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    let mut account = Map::new();
    for (target, source) in [
        ("userId", "user_id"),
        ("nickname", "nickname"),
        ("userLevelName", "user_level_name"),
        ("region", "region"),
        ("status", "status"),
    ] {
        if let Some(value) = string_field(&parsed, source) {
            account.insert(target.into(), json!(value));
        }
    }
    if let Some(level) = parsed.get("user_level").and_then(Value::as_i64) {
        account.insert("userLevel".into(), json!(level));
    }
    Ok(Value::Object(account))
}

fn account_label(store: &Value) -> String {
    store
        .get("account")
        .and_then(|account| string_field(account, "nickname"))
        .or_else(|| string_field(store, "apiKey").map(|key| mask_key(&key)))
        .unwrap_or_else(|| "Kimi".into())
}

/// Show the head and tail of a key. Enough for a user to recognize which key is
/// connected, never enough to spend it.
fn mask_key(key: &str) -> String {
    if key.chars().count() <= 12 {
        return "•".repeat(8);
    }
    let head: String = key.chars().take(9).collect();
    let tail: String = key
        .chars()
        .rev()
        .take(4)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    format!("{head}…{tail}")
}

pub fn status() -> Value {
    let Some(store) = read_store() else {
        return json!({ "loggedIn": false });
    };
    let Some(kind) = auth_type() else {
        return json!({ "loggedIn": false });
    };
    let usable = match kind {
        KimiAuthType::ApiKey => string_field(&store, "apiKey").is_some(),
        KimiAuthType::OAuth => store
            .get("oauth")
            .and_then(|oauth| string_field(oauth, "accessToken"))
            .is_some(),
    };
    if !usable {
        return json!({ "loggedIn": false });
    }
    let mut out = Map::new();
    out.insert("loggedIn".into(), json!(true));
    out.insert("authType".into(), json!(kind.as_str()));
    out.insert("account".into(), json!(account_label(&store)));
    if let Some(plan) = store
        .get("account")
        .and_then(|account| string_field(account, "userLevelName"))
    {
        out.insert("plan".into(), json!(plan));
    }
    if let Some(api_key) = string_field(&store, "apiKey") {
        out.insert("apiKeyMasked".into(), json!(mask_key(&api_key)));
    }
    if let Some(expires_at) = store
        .get("oauth")
        .and_then(|oauth| oauth.get("expiresAt"))
        .and_then(Value::as_i64)
    {
        out.insert("tokenExpiresAt".into(), json!(expires_at));
    }
    Value::Object(out)
}

pub(crate) fn catalog_fingerprint() -> Option<String> {
    credential()?;
    let store = read_store()?;
    let account = store.get("account")?;
    let uid = string_field(account, "userId").or_else(|| string_field(account, "user_id"))?;
    let region = string_field(account, "region")?;
    Some(json!([uid, region]).to_string())
}

pub fn logout() -> Result<Value, Value> {
    super::catalog::forget_cache();
    match std::fs::remove_file(store_path()) {
        Ok(()) => Ok(json!({ "ok": true })),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(json!({ "ok": true })),
        Err(error) => Ok(json!({ "ok": false, "error": error.to_string() })),
    }
}

/// Kimi has no local credential store another client leaves behind in a form the
/// engine can read: the CLI keeps its tokens under its own profile and the web
/// session is a browser cookie. Saying so beats a silent "nothing found" that
/// reads as "not installed".
pub fn import_local() -> Result<Value, Value> {
    Ok(json!({
        "ok": false,
        "error": "kimi_local_credentials_unavailable",
    }))
}

struct PendingFlow {
    device_code: String,
    expires_at: i64,
}

fn pending_flows() -> &'static Mutex<std::collections::HashMap<String, PendingFlow>> {
    static FLOWS: OnceLock<Mutex<std::collections::HashMap<String, PendingFlow>>> = OnceLock::new();
    FLOWS.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

fn random_id() -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(32);
    for byte in random_bytes(16) {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn random_bytes(count: usize) -> Vec<u8> {
    // No crypto dependency here: a flow handle only has to be unguessable inside
    // this process for the lifetime of a login, and it is never a credential.
    let mut out = Vec::with_capacity(count);
    let mut seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|value| value.subsec_nanos() as u64 ^ (value.as_secs() << 7))
        .unwrap_or(0x9E37_79B9_7F4A_7C15);
    for index in 0..count {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        out.push(((seed >> (index % 8)) & 0xFF) as u8);
    }
    out
}

/// Begin a device-flow login: ask Kimi for a code, hand back the URL to open.
///
/// The engine never opens a browser; the host does, so the answer carries the
/// URL rather than acting on it.
pub async fn start() -> Result<Value, Value> {
    let http = client().map_err(|error| json!({ "ok": false, "error": error }))?;
    let response = http
        .post(format!("{}{DEVICE_AUTHORIZATION_PATH}", oauth_host()))
        .header("content-type", "application/x-www-form-urlencoded")
        .body(format!("client_id={CLIENT_ID}"))
        .send()
        .await
        .map_err(|error| json!({ "ok": false, "error": error.to_string() }))?;
    let status_code = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    if !(200..300).contains(&status_code) {
        let detail: String = body.chars().take(200).collect();
        return Ok(json!({
            "ok": false,
            "error": format!("kimi_login_start_failed:{status_code}:{detail}"),
        }));
    }
    let parsed: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    let device_code = string_field(&parsed, "device_code").unwrap_or_default();
    if device_code.is_empty() {
        return Ok(json!({
            "ok": false,
            "error": "kimi_login_start_no_code",
        }));
    }
    let user_code = string_field(&parsed, "user_code").unwrap_or_default();
    let verification_uri_complete = string_field(&parsed, "verification_uri_complete")
        .unwrap_or_else(|| format!("{VERIFICATION_URI}?user_code={user_code}"));
    let interval = parsed
        .get("interval")
        .and_then(Value::as_i64)
        .filter(|value| *value > 0)
        .unwrap_or(5);
    let expires_in = parsed
        .get("expires_in")
        .and_then(Value::as_i64)
        .filter(|value| *value > 0)
        .unwrap_or(FLOW_TIMEOUT_MS / 1000);
    let flow_id = random_id();
    let expires_at = now_ms() + std::cmp::min(expires_in * 1000, FLOW_TIMEOUT_MS);
    pending_flows()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(
            flow_id.clone(),
            PendingFlow {
                device_code,
                expires_at,
            },
        );
    Ok(json!({
        "ok": true,
        "flowId": flow_id,
        "authUrl": verification_uri_complete,
        "userCode": user_code,
        "expiresAt": expires_at,
        "interval": interval,
    }))
}

/// Trade the flow handle for the token the browser page deposited.
///
/// `authorization_pending` and `slow_down` are both "keep waiting", not failure;
/// `expired_token` and `access_denied` end the flow.
pub async fn poll(payload: &Value) -> Result<Value, Value> {
    let flow_id = payload
        .get("flowId")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let pending = {
        let mut flows = pending_flows()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match flows.get(&flow_id) {
            Some(flow) if flow.expires_at > now_ms() => flow.device_code.clone(),
            Some(_) => {
                flows.remove(&flow_id);
                return Ok(json!({ "ok": false, "expired": true, "error": "kimi_login_expired" }));
            }
            None => {
                return Ok(json!({
                    "ok": false,
                    "expired": true,
                    "error": "kimi_login_flow_unknown",
                }))
            }
        }
    };
    let http = client().map_err(|error| json!({ "ok": false, "error": error }))?;
    let response = http
        .post(format!("{}{TOKEN_PATH}", oauth_host()))
        .header("content-type", "application/x-www-form-urlencoded")
        .body(format!(
            "client_id={CLIENT_ID}&device_code={}&grant_type={DEVICE_CODE_GRANT}",
            urlencoded(&pending)
        ))
        .send()
        .await
        .map_err(|error| json!({ "ok": false, "error": error.to_string() }))?;
    let status_code = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    let parsed: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    if !(200..300).contains(&status_code) {
        let code = string_field(&parsed, "error").unwrap_or_default();
        match code.as_str() {
            "authorization_pending" | "slow_down" => {
                return Ok(json!({ "ok": false, "pending": true }))
            }
            "expired_token" | "access_denied" => {
                pending_flows()
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .remove(&flow_id);
                return Ok(json!({ "ok": false, "error": format!("kimi_login_{code}") }));
            }
            _ => {
                let detail: String = body.chars().take(200).collect();
                return Ok(json!({
                    "ok": false,
                    "error": format!("kimi_login_poll_failed:{status_code}:{detail}"),
                }));
            }
        }
    }
    pending_flows()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(&flow_id);
    match complete_login(&http, &parsed).await {
        // The host's poll loop keys on `ok`, and every other device-flow source
        // answers with it; the bare status snapshot this used to return read as
        // a failure there even though the credential had just been stored.
        Ok(value) => Ok(json!({ "ok": true, "status": value })),
        Err(error) => Ok(json!({ "ok": false, "error": error })),
    }
}

/// Percent-encode one form component, BYTE by byte.
///
/// Encoding per byte rather than per char is the whole point: these values ride a
/// credential, and a multi-byte character encoded by only its first byte would be
/// silently truncated into a different secret. This is the same rule the engine's
/// own OAuth form serializer uses (`provider::oauth::encode_component`), kept
/// identical so a Kimi token and a Claude token are encoded the one way.
fn urlencoded(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for &byte in value.as_bytes() {
        match byte {
            b'*' | b'-' | b'.' | b'_' => out.push(byte as char),
            b'0'..=b'9' | b'A'..=b'Z' | b'a'..=b'z' => out.push(byte as char),
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{:02X}", byte)),
        }
    }
    out
}

/// Persist the token set the flow just earned, with the account it belongs to.
async fn complete_login(http: &reqwest::Client, tokens: &Value) -> Result<Value, String> {
    let access_token = string_field(tokens, "access_token")
        .ok_or_else(|| "kimi_login_no_access_token".to_string())?;
    let refresh_token = string_field(tokens, "refresh_token").unwrap_or_default();
    let expires_in = tokens
        .get("expires_in")
        .and_then(Value::as_i64)
        .filter(|value| *value > 0)
        // Measured at 900s; assume a quarter hour if the service stops saying.
        .unwrap_or(900);
    // The flow's gateway is the one its OAuth host pairs with: an env override
    // wins, and a previously stored key's base must not leak in — the key and
    // the login can belong to different regions.
    let base = env_api_base().unwrap_or_else(|| DEFAULT_API_BASE.trim_end_matches('/').to_string());
    // A device-flow token is an OAuth bearer, not an API key.
    let account = fetch_account(
        http,
        &base,
        ("authorization", format!("Bearer {access_token}")),
    )
    .await
    .unwrap_or_else(|_| json!({}));

    let mut store = read_store()
        .and_then(|store| store.as_object().cloned())
        .unwrap_or_default();
    store.insert("authType".into(), json!(KimiAuthType::OAuth.as_str()));
    store.insert(
        "oauth".into(),
        json!({
            "accessToken": access_token,
            "refreshToken": refresh_token,
            "expiresAt": now_seconds() + expires_in,
            "scope": string_field(tokens, "scope"),
        }),
    );
    store.insert("apiBase".into(), json!(base));
    store.insert("account".into(), account);
    store.insert("updatedAt".into(), json!(now_ms()));
    // A key the user stored earlier is dropped: one credential is active, and
    // keeping both would leave the settings card unable to say which one a
    // session will actually spend.
    store.remove("apiKey");
    if !store.contains_key("deviceId") {
        store.insert("deviceId".into(), json!(random_id()));
    }
    super::catalog::forget_cache();
    write_store(&Value::Object(store))?;
    Ok(status())
}

/// Renew a stored device-flow login before its access token expires.
///
/// The rotated set is persisted as soon as it arrives: Kimi supersedes the old
/// refresh token here, and a crash between the exchange and the write would
/// otherwise leave the store holding a token that can no longer renew.
pub async fn ensure_fresh_token() -> Result<(), String> {
    let (expires_at, _) = match credential() {
        Some(Credential::OAuth {
            expires_at,
            refresh_token,
            ..
        }) => (expires_at, refresh_token),
        // An API key has no expiry to run against.
        Some(Credential::ApiKey { .. }) | None => return Ok(()),
    };
    if expires_at - REFRESH_SKEW_SECONDS > now_seconds() {
        return Ok(());
    }
    refresh_now().await
}

/// Spend the stored refresh token now, regardless of local expiry.
pub async fn refresh_now() -> Result<(), String> {
    let Some(store) = read_store() else {
        return Ok(());
    };
    let Some(refresh_token) = store
        .get("oauth")
        .and_then(|oauth| string_field(oauth, "refreshToken"))
    else {
        return Ok(());
    };
    let http = client().map_err(|error| error.to_string())?;
    let response = http
        .post(format!("{}{TOKEN_PATH}", oauth_host()))
        .header("content-type", "application/x-www-form-urlencoded")
        .body(format!(
            "client_id={CLIENT_ID}&grant_type=refresh_token&refresh_token={}",
            urlencoded(&refresh_token)
        ))
        .send()
        .await
        .map_err(|error| error.to_string())?;
    let status_code = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    if !(200..300).contains(&status_code) {
        // A refused refresh means the login is gone: say so with the stable code
        // the host keys a re-login on, rather than passing the service's prose
        // through as if it were a transient network fault.
        return Err("kimi_login_expired".to_string());
    }
    let parsed: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    let access_token =
        string_field(&parsed, "access_token").ok_or_else(|| "kimi_login_expired".to_string())?;
    let rotated = string_field(&parsed, "refresh_token").unwrap_or(refresh_token.clone());
    let expires_in = parsed
        .get("expires_in")
        .and_then(Value::as_i64)
        .filter(|value| *value > 0)
        .unwrap_or(900);
    let mut updated = store.as_object().cloned().unwrap_or_default();
    if let Some(oauth) = updated.get_mut("oauth").and_then(Value::as_object_mut) {
        oauth.insert("accessToken".into(), json!(access_token));
        oauth.insert("refreshToken".into(), json!(rotated));
        oauth.insert("expiresAt".into(), json!(now_seconds() + expires_in));
    }
    updated.insert("updatedAt".into(), json!(now_ms()));
    write_store(&Value::Object(updated))
}

/// The membership allowance, as rate windows the host can render.
///
/// Kimi reports ratios rather than counts — `used_ratio` against a monthly pool
/// and a five-hour window — so this answers in the same shape the Codex and
/// Claude windows use instead of inventing a credit total to divide by. Being
/// signed out is an answer, not an error, so a settings pane paints "not signed
/// in" rather than a failure.
pub async fn usage() -> Result<Value, String> {
    let Some(store) = read_store() else {
        return Ok(json!({ "signedIn": false }));
    };
    let (_, header_name, header_value) = match spending().await {
        Ok(spending) => spending,
        Err(_) => return Ok(json!({ "signedIn": false })),
    };
    let http = client().map_err(|error| error.to_string())?;
    let response = http
        .get(format!("{}{USAGE_PATH}", api_base()))
        .header(header_name, header_value)
        .send()
        .await
        .map_err(|error| error.to_string())?;
    let status_code = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    if status_code == 401 || status_code == 403 {
        return Ok(json!({ "signedIn": false }));
    }
    if !(200..300).contains(&status_code) {
        let detail: String = body.chars().take(200).collect();
        return Err(format!("kimi usage read failed ({status_code}): {detail}"));
    }
    let parsed: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    let mut out = Map::new();
    out.insert("signedIn".into(), json!(true));
    out.insert("account".into(), json!(account_label(&store)));
    if let Some(plan) = store
        .get("account")
        .and_then(|account| string_field(account, "userLevelName"))
    {
        out.insert("plan".into(), json!(plan));
    }
    let windows = usage_windows(&parsed);
    if !windows.is_empty() {
        out.insert("windows".into(), Value::Array(windows));
    }
    Ok(Value::Object(out))
}

/// Project the service's ratio buckets onto labeled windows.
///
/// Each bucket is a separate allowance with its own reset, and collapsing them
/// into one bar would hide the window that actually stops a session: the
/// five-hour one resets in minutes while the monthly one resets in weeks.
fn usage_windows(payload: &Value) -> Vec<Value> {
    let mut windows = Vec::new();
    let usages = payload.get("usages");
    for (id, key) in [
        ("five_hour", "limit_5h"),
        ("month_total", "limit_month_total"),
        ("month_code", "limit_month_code"),
    ] {
        let Some(bucket) = usages.and_then(|group| group.get(key)) else {
            continue;
        };
        let Some(used_percent) = ratio_percent(bucket.get("used_ratio")) else {
            continue;
        };
        let mut row = Map::new();
        row.insert("id".into(), json!(id));
        row.insert("usedPercent".into(), json!(used_percent));
        if let Some(resets_at) = string_field(bucket, "reset_time").and_then(epoch_ms) {
            row.insert("resetsAt".into(), json!(resets_at));
        }
        windows.push(Value::Object(row));
    }
    windows
}

fn ratio_percent(value: Option<&Value>) -> Option<f64> {
    let ratio = match value {
        Some(Value::Number(number)) => number.as_f64()?,
        Some(Value::String(text)) => text.trim().parse::<f64>().ok()?,
        _ => return None,
    };
    if !ratio.is_finite() {
        return None;
    }
    Some((ratio * 100.0).clamp(0.0, 100.0))
}

/// Parse the service's RFC3339 timestamps into epoch milliseconds.
fn epoch_ms(value: String) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(&value)
        .ok()
        .map(|parsed| parsed.timestamp_millis())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn form_encoding_is_per_byte_so_a_multibyte_credential_survives() {
        // A JWT is URL-safe ASCII today, but the encoder must not silently
        // truncate anything it is handed: encoding per CHARACTER and emitting only
        // the first byte is the bug this pins shut. "é" is two bytes (C3 A9), so a
        // byte-wise encoder yields %C3%A9 while a char-wise one would yield %C3 and
        // drop half the secret.
        assert_eq!(urlencoded("é"), "%C3%A9");
        assert_eq!(urlencoded("a b"), "a+b");
        assert_eq!(urlencoded("abc-._*XYZ123"), "abc-._*XYZ123");
        assert_eq!(urlencoded("sk/kimi+key="), "sk%2Fkimi%2Bkey%3D");
        // A realistic base64url JWT body round-trips untouched apart from its
        // reserved separators.
        assert_eq!(urlencoded("eyJhbGciOiJFUzI1NiJ9"), "eyJhbGciOiJFUzI1NiJ9");
    }

    #[test]
    fn masking_never_reveals_a_spendable_key() {
        assert_eq!(mask_key("sk-kimi-abcdefghijklmnop"), "sk-kimi-a…mnop");
        assert_eq!(mask_key("short"), "••••••••");
    }

    #[tokio::test]
    async fn a_signed_out_store_is_an_answer_not_an_error() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        assert_eq!(status(), json!({ "loggedIn": false }));
        assert!(auth_type().is_none());
        assert!(credential().is_none());
        // Signing out when nothing is stored still succeeds: the pane has to be
        // able to clear a credential that is already gone.
        assert_eq!(logout(), Ok(json!({ "ok": true })));
    }

    #[tokio::test]
    async fn a_stored_api_key_reads_back_as_the_key_credential() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        write_store(&json!({
            "authType": "apiKey",
            "apiKey": "sk-kimi-abcdefghijklmnop",
            "apiBase": "https://api.kimi.com/coding",
            "account": { "nickname": "Nick", "userLevelName": "Pro" },
        }))
        .expect("store write");

        assert_eq!(auth_type(), Some(KimiAuthType::ApiKey));
        assert!(matches!(
            credential(),
            Some(Credential::ApiKey { key }) if key == "sk-kimi-abcdefghijklmnop"
        ));
        let status = status();
        assert_eq!(status["loggedIn"], json!(true));
        assert_eq!(status["authType"], json!("apiKey"));
        assert_eq!(status["account"], json!("Nick"));
        assert_eq!(status["plan"], json!("Pro"));
        // The masked form is what a pane may show; the raw key is not.
        assert_eq!(status["apiKeyMasked"], json!("sk-kimi-a…mnop"));
        assert!(!status.to_string().contains("abcdefghijklmnop"));
    }

    #[tokio::test]
    async fn a_stored_login_reads_back_as_the_oauth_credential() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        let expires = now_seconds() + 900;
        write_store(&json!({
            "authType": "oauth",
            "oauth": {
                "accessToken": "access-token-value",
                "refreshToken": "refresh-token-value",
                "expiresAt": expires,
            },
            "account": { "nickname": "Nick" },
        }))
        .expect("store write");

        assert_eq!(auth_type(), Some(KimiAuthType::OAuth));
        assert!(matches!(
            credential(),
            Some(Credential::OAuth { access_token, refresh_token, expires_at })
                if access_token == "access-token-value"
                    && refresh_token == "refresh-token-value"
                    && expires_at == expires
        ));
        assert_eq!(status()["authType"], json!("oauth"));
        assert_eq!(status()["tokenExpiresAt"], json!(expires));
    }

    #[tokio::test]
    async fn a_store_naming_a_kind_with_no_credential_is_signed_out() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        // authType says apiKey but the key is gone — a half-written store must
        // read as signed out rather than as a credential that cannot be spent.
        write_store(&json!({ "authType": "apiKey", "apiKey": "" })).expect("store write");
        assert_eq!(status(), json!({ "loggedIn": false }));
        assert!(credential().is_none());
        write_store(&json!({ "authType": "oauth", "oauth": {} })).expect("store write");
        assert_eq!(status(), json!({ "loggedIn": false }));
        assert!(credential().is_none());
    }

    #[tokio::test]
    async fn logout_clears_the_credential_and_the_catalog_cache() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        write_store(&json!({ "authType": "apiKey", "apiKey": "sk-kimi-x" })).expect("store write");
        assert_eq!(status()["loggedIn"], json!(true));
        assert_eq!(logout(), Ok(json!({ "ok": true })));
        assert_eq!(status(), json!({ "loggedIn": false }));
    }

    #[test]
    fn usage_windows_project_each_ratio_separately() {
        let payload = json!({
            "usages": {
                "limit_5h": { "used_ratio": 0.25, "reset_time": "2026-09-29T08:35:17Z" },
                "limit_month_total": { "used_ratio": 0.001, "reset_time": "2026-10-30T00:00:00Z" },
                "limit_month_code": { "used_ratio": 0.5, "reset_time": "2026-10-30T00:00:00Z" }
            }
        });
        let windows = usage_windows(&payload);
        assert_eq!(windows.len(), 3);
        assert_eq!(windows[0]["id"], json!("five_hour"));
        assert_eq!(windows[0]["usedPercent"], json!(25.0));
        assert_eq!(windows[1]["id"], json!("month_total"));
        assert_eq!(windows[2]["id"], json!("month_code"));
        assert_eq!(windows[2]["usedPercent"], json!(50.0));
        assert!(windows[0]["resetsAt"].as_i64().unwrap() > 0);
    }

    #[test]
    fn a_ratio_outside_zero_to_one_is_clamped() {
        assert_eq!(ratio_percent(Some(&json!(2.0))), Some(100.0));
        assert_eq!(ratio_percent(Some(&json!(-1.0))), Some(0.0));
        assert_eq!(ratio_percent(Some(&json!("0.5"))), Some(50.0));
        assert_eq!(ratio_percent(Some(&json!("not-a-number"))), None);
        assert_eq!(ratio_percent(None), None);
    }

    #[test]
    fn a_missing_bucket_produces_no_window() {
        assert!(usage_windows(&json!({ "usages": {} })).is_empty());
        assert!(usage_windows(&json!({})).is_empty());
    }

    #[test]
    fn rfc3339_with_fractional_seconds_parses() {
        // The live body spells reset times with microsecond precision.
        assert!(epoch_ms("2026-09-29T08:35:17.925555Z".to_string()).is_some());
        assert!(epoch_ms("2026-09-29T08:35:17Z".to_string()).is_some());
        assert!(epoch_ms("not-a-date".to_string()).is_none());
    }

    #[tokio::test]
    async fn a_key_that_needs_no_renewal_leaves_the_store_alone() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        write_store(&json!({ "authType": "apiKey", "apiKey": "sk-kimi-x" })).expect("store write");
        // No network: an API key has no expiry, so this must not reach upstream.
        assert_eq!(ensure_fresh_token().await, Ok(()));
        assert_eq!(status()["authType"], json!("apiKey"));
    }

    #[tokio::test]
    async fn an_unexpired_login_is_not_renewed() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        write_store(&json!({
            "authType": "oauth",
            "oauth": {
                "accessToken": "still-fresh",
                "refreshToken": "refresh-token-value",
                "expiresAt": now_seconds() + 900,
            },
        }))
        .expect("store write");
        assert_eq!(ensure_fresh_token().await, Ok(()));
        assert!(matches!(
            credential(),
            Some(Credential::OAuth { access_token, .. }) if access_token == "still-fresh"
        ));
    }

    /// Point the gateway at a black hole so a resolution's background catalog
    /// refresh fails fast instead of reaching the network. The assertions read
    /// the seeded snapshot, which `fetch_models` returns before that refresh
    /// lands, so nothing here depends on connectivity.
    struct GatewayGuard;
    impl Drop for GatewayGuard {
        fn drop(&mut self) {
            klaus_agent_core::runtime_process::remove_process_environment_var("KIMI_API_BASE");
        }
    }
    fn blackhole_gateway() -> GatewayGuard {
        klaus_agent_core::runtime_process::set_process_environment_var(
            "KIMI_API_BASE",
            "http://127.0.0.1:9/coding",
        );
        GatewayGuard
    }

    #[tokio::test]
    async fn a_key_resolves_to_the_anthropic_wire_with_an_api_key_auth() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        let _gateway = blackhole_gateway();
        write_store(&json!({
            "authType": "apiKey",
            "apiKey": "sk-kimi-secret",
            "apiBase": "http://127.0.0.1:9/coding",
            "account": { "userId": "fixture-user", "region": "cn" },
        }))
        .expect("store write");
        super::super::catalog::forget_cache();
        super::super::catalog::seed_owned_catalog(&[json!({ "key": "k3", "display_name": "K3" })]);

        let (provider, auth) = super::super::resolve("k3").await.expect("resolve");
        // Kimi serves the ordinary Anthropic wire, not the OpenAI one the other
        // key-based sources use: the provider type and the join-on-/v1/messages
        // base are what make a session reach the coding gateway at all.
        assert_eq!(provider["type"], json!("anthropic"));
        assert_eq!(provider["model"], json!("k3"));
        assert_eq!(provider["baseUrl"], json!("http://127.0.0.1:9/coding"));
        assert_eq!(auth, json!({ "type": "api_key", "key": "sk-kimi-secret" }));
    }

    #[tokio::test]
    async fn a_login_resolves_to_an_oauth_auth_the_provider_can_renew() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        let _gateway = blackhole_gateway();
        write_store(&json!({
            "authType": "oauth",
            "apiBase": "http://127.0.0.1:9/coding",
            "account": { "userId": "fixture-user", "region": "cn" },
            "oauth": {
                "accessToken": "access-token-value",
                "refreshToken": "refresh-token-value",
                "expiresAt": now_seconds() + 900,
            },
        }))
        .expect("store write");
        super::super::catalog::forget_cache();
        super::super::catalog::seed_owned_catalog(&[json!({ "key": "k3", "display_name": "K3" })]);

        let (provider, auth) = super::super::resolve("k3").await.expect("resolve");
        assert_eq!(provider["type"], json!("anthropic"));
        // A 900-second access token has to be renewable mid-session, so the auth
        // is an oauth_token carrying the client config and a stable credential id
        // the provider persists a rotation under — not a static bearer that dies
        // at the quarter hour.
        assert_eq!(auth["type"], json!("oauth_token"));
        assert_eq!(auth["accessToken"], json!("access-token-value"));
        assert_eq!(auth["refreshToken"], json!("refresh-token-value"));
        assert_eq!(auth["credentialId"], json!(CREDENTIAL_ID));
        assert_eq!(auth["oauth"]["clientId"], json!(CLIENT_ID));
        assert!(auth["oauth"]["tokenUrl"]
            .as_str()
            .unwrap()
            .ends_with("/api/oauth/token"));
    }

    #[tokio::test]
    async fn a_signed_out_kimi_selection_says_so() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        let _gateway = blackhole_gateway();
        let error = super::super::resolve("k3")
            .await
            .expect_err("no Kimi credential on a temp root");
        assert!(error.contains("not signed in"), "{error}");
    }

    /// A tiny HTTP server answering the two routes a device-flow completion
    /// spends: the token exchange and the account read. Two connections, then
    /// it stops; a completed flow needs exactly those.
    fn mock_kimi_gateway() -> (std::net::SocketAddr, std::thread::JoinHandle<()>) {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind mock gateway");
        let addr = listener.local_addr().expect("mock gateway addr");
        let handle = std::thread::spawn(move || {
            for _ in 0..2 {
                let Ok((mut stream, _peer)) = listener.accept() else {
                    return;
                };
                let mut request = [0u8; 2048];
                let read = stream.read(&mut request).unwrap_or(0);
                let head = String::from_utf8_lossy(&request[..read]).to_string();
                let (status, body) = if head.starts_with("POST /api/oauth/token") {
                    (
                        "200 OK",
                        json!({
                            "access_token": "flow-token",
                            "refresh_token": "flow-refresh",
                            "expires_in": 900,
                        })
                        .to_string(),
                    )
                } else if head.starts_with("GET /v1/me") {
                    (
                        "200 OK",
                        json!({ "nickname": "Nick", "user_level_name": "Pro" }).to_string(),
                    )
                } else {
                    ("404 Not Found", "{}".to_string())
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.flush();
            }
        });
        (addr, handle)
    }

    struct OAuthEnvGuard;
    impl Drop for OAuthEnvGuard {
        fn drop(&mut self) {
            klaus_agent_core::runtime_process::remove_process_environment_var("KIMI_API_BASE");
            klaus_agent_core::runtime_process::remove_process_environment_var("KIMI_OAUTH_HOST");
        }
    }

    #[tokio::test]
    async fn a_completed_device_flow_answers_ok_with_the_stored_login() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        let (addr, server) = mock_kimi_gateway();
        let base = format!("http://{addr}");
        klaus_agent_core::runtime_process::set_process_environment_var("KIMI_API_BASE", &base);
        klaus_agent_core::runtime_process::set_process_environment_var("KIMI_OAUTH_HOST", &base);
        let _env = OAuthEnvGuard;
        pending_flows()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(
                "test-flow".to_string(),
                PendingFlow {
                    device_code: "device-code-1".to_string(),
                    expires_at: now_ms() + 60_000,
                },
            );

        let result = poll(&json!({ "flowId": "test-flow" }))
            .await
            .expect("poll succeeds");
        server.join().expect("mock gateway served both routes");

        // The host's poll loop keys on `ok`; the status snapshot rides along so
        // the settings card can show who just signed in without a second read.
        assert_eq!(result["ok"], json!(true), "{result}");
        assert_eq!(result["status"]["loggedIn"], json!(true), "{result}");
        assert_eq!(result["status"]["account"], json!("Nick"), "{result}");
        let store = read_store().expect("store written");
        assert_eq!(store["apiBase"], json!(base));
        assert!(
            pending_flows()
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_empty(),
            "a completed flow is dropped"
        );
    }
}
