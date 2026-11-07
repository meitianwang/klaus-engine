//! WorkBuddy login, credential ownership and credit balance.
//!
//! The login is the official client's own external-link flow: ask the gateway for
//! a one-time `state`, open its browser page, then poll for the token the page
//! deposited. Klaus drives that flow itself, so signing in here does not need
//! WorkBuddy installed.
//!
//! Reusing a login WorkBuddy ALREADY has is the other path, and it does need it:
//! that store encrypts its tokens under a key compiled into WorkBuddy's own
//! Electron binary, so only the host can open it, and it arrives through
//! [`adopt_credential`] — the same seam QwenWork's safeStorage profile uses.

use super::super::wire::WorkBuddyRegion;
use serde_json::{json, Map, Value};
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

/// Every login route sits under the product's `authentication.attributes.prefixPath`
/// (`/plugin`); the constants below bake it in rather than composing it, because
/// there is exactly one product and no deployment that moves it.
const AUTH_STATE_PATH: &str = "/v2/plugin/auth/state";
const AUTH_TOKEN_PATH: &str = "/v2/plugin/auth/token";
const AUTH_REFRESH_PATH: &str = "/v2/plugin/auth/token/refresh";
const ACCOUNTS_PATH: &str = "/console/accounts";
const PERSONAL_USAGE_PATH: &str = "/billing/meter/get-user-resource-summary";
const ENTERPRISE_USAGE_PATH: &str = "/v2/billing/meter/get-enterprise-user-usage";

/// `authentication.attributes.platform` from the product config: which login page
/// the browser is sent to. The overseas and domestic builds differ only here and
/// in the gateway host, both of which the stored credential carries.
const DEFAULT_PLATFORM: &str = "workbuddy-ai";
/// The overseas gateway, used before any credential exists. A signed-in store
/// carries its own `domain` and that wins, so a domestic login stays domestic.
const DEFAULT_API_BASE: &str = "https://www.workbuddy.ai";

/// What a selection names when no catalog is reachable to answer with. The live
/// catalog's own default entry always wins over this; it only bounds a picker
/// that has to paint before any fetch has landed.
pub const DEFAULT_SELECTION: &str = "default-model";

const FLOW_TIMEOUT_MS: i64 = 300_000;
const POLL_INTERVAL_SECONDS: i64 = 1;
const REFRESH_SKEW_SECONDS: i64 = 3600;
const HTTP_TIMEOUT_MS: u64 = 15_000;

/// The gateway answers a login that has not completed yet with this business code
/// while still returning HTTP 200.
const LOGIN_PENDING_CODE: i64 = 11217;
/// Seats exhausted / license expired / trial over. The gateway reports these as
/// HTTP 400 with a business code, so they are surfaced as their own message
/// rather than as a generic failure.
const LICENSE_ERROR_CODES: [i64; 3] = [12005, 11212, 11216];
/// The gateway refused this network location.
const IP_LIMIT_CODE: i64 = 10081;

fn store_path() -> PathBuf {
    super::config_root().join("workbuddy-subscription-oauth.json")
}

fn now_seconds() -> i64 {
    chrono::Utc::now().timestamp()
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn read_store() -> Option<Value> {
    let raw = std::fs::read_to_string(store_path()).ok()?;
    serde_json::from_str(&raw).ok()
}

fn write_store(device_id: &str, user_info: &Value) -> Result<(), String> {
    let path = store_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let serialized = serde_json::to_string_pretty(&json!({
        "deviceId": device_id,
        "userInfo": user_info,
    }))
    .map_err(|error| error.to_string())?;
    std::fs::write(&path, serialized).map_err(|error| error.to_string())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

pub(crate) fn credential() -> Option<(String, Value)> {
    let store = read_store()?;
    let device_id = store.get("deviceId").and_then(Value::as_str)?.to_string();
    let user_info = store.get("userInfo").cloned()?;
    if !user_info.is_object() {
        return None;
    }
    Some((device_id, user_info))
}

fn first_string(source: &Value, keys: &[&str]) -> Option<String> {
    for key in keys {
        match source.get(*key) {
            Some(Value::String(text)) if !text.is_empty() => return Some(text.clone()),
            Some(Value::Number(number)) => return Some(number.to_string()),
            _ => {}
        }
    }
    None
}

/// The gateway this credential belongs to.
///
/// The overseas build serves `www.workbuddy.ai` and the domestic one
/// `www.codebuddy.cn`, and a token issued by one is refused by the other. The
/// adopted profile carries the `domain` its own client recorded, so that wins;
/// only a login Klaus itself started falls back to the default.
pub(crate) fn api_base() -> String {
    if let Some(base) = api_base_override() {
        return base;
    }
    credential()
        .and_then(|(_, user_info)| first_string(&user_info, &["domain", "endpoint"]))
        .map(|domain| {
            if domain.starts_with("http://") || domain.starts_with("https://") {
                domain.trim_end_matches('/').to_string()
            } else {
                format!("https://{}", domain.trim_end_matches('/'))
            }
        })
        .unwrap_or_else(|| DEFAULT_API_BASE.to_string())
}

fn api_base_override() -> Option<String> {
    std::env::var("WORKBUDDY_API_BASE")
        .ok()
        .map(|value| value.trim().trim_end_matches('/').to_string())
        .filter(|value| !value.is_empty())
}

fn login_target(region: Option<WorkBuddyRegion>) -> (String, String) {
    let (base, platform) = match region {
        Some(WorkBuddyRegion::Cn) => ("https://www.codebuddy.cn", "workbuddy"),
        Some(WorkBuddyRegion::Intl) => (DEFAULT_API_BASE, DEFAULT_PLATFORM),
        None => return (api_base(), platform()),
    };
    (
        api_base_override().unwrap_or_else(|| base.to_string()),
        platform.to_string(),
    )
}

fn platform() -> String {
    std::env::var("WORKBUDDY_PLATFORM")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| {
            credential().and_then(|(_, user_info)| {
                first_string(&user_info, &["platform", "authentication_id"])
            })
        })
        .unwrap_or_else(|| DEFAULT_PLATFORM.to_string())
}

fn client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(HTTP_TIMEOUT_MS))
        .build()
        .map_err(|error| error.to_string())
}

/// The login routes are unauthenticated by design — the caller has no account
/// yet — and these four flags are how the official client tells its own HTTP
/// layer not to attach an `Authorization` it does not have.
fn unauthenticated(request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    request
        .header("X-No-Authorization", "true")
        .header("X-No-User-Id", "true")
        .header("X-No-Enterprise-Id", "true")
        .header("X-No-Department-Info", "true")
        .header("User-Agent", super::catalog::user_agent())
}

fn authenticated(
    request: reqwest::RequestBuilder,
    token: &str,
    user_info: &Value,
) -> reqwest::RequestBuilder {
    let mut request = request
        .bearer_auth(token)
        .header("Content-Type", "application/json")
        .header("User-Agent", super::catalog::user_agent());
    if let Some(domain) = first_string(user_info, &["domain"]) {
        request = request.header("X-Domain", domain);
    }
    if let Some(uid) = first_string(user_info, &["uid"]) {
        request = request.header("X-User-Id", uid);
    }
    if let Some(enterprise_id) = first_string(user_info, &["enterprise_id", "enterpriseId"]) {
        request = request
            .header("X-Enterprise-Id", enterprise_id.clone())
            .header("X-Tenant-Id", enterprise_id);
    }
    request
}

fn account_label(user_info: &Value) -> String {
    first_string(
        user_info,
        &["nickname", "name", "display_name", "email", "uid"],
    )
    .unwrap_or_else(|| "WorkBuddy".into())
}

fn access_token_of(user_info: &Value) -> String {
    first_string(user_info, &["access_token", "accessToken"]).unwrap_or_default()
}

pub struct WorkBuddyToken {
    pub access_token: String,
    pub account: String,
}

pub fn access_token() -> Result<WorkBuddyToken, super::catalog::WorkBuddyError> {
    let Some((_, user_info)) = credential() else {
        return Err(super::catalog::WorkBuddyError {
            message: "WorkBuddy subscription is not signed in".into(),
            not_found: true,
            login_expired: false,
        });
    };
    let access_token = access_token_of(&user_info);
    if access_token.is_empty() {
        return Err(super::catalog::WorkBuddyError {
            message: "WorkBuddy subscription is not signed in".into(),
            not_found: true,
            login_expired: false,
        });
    }
    Ok(WorkBuddyToken {
        access_token,
        account: account_label(&user_info),
    })
}

pub fn status() -> Value {
    let Some((_, user_info)) = credential() else {
        return json!({ "loggedIn": false });
    };
    if access_token_of(&user_info).is_empty() {
        return json!({ "loggedIn": false });
    }
    let mut out = Map::new();
    out.insert("loggedIn".into(), json!(true));
    out.insert("account".into(), json!(account_label(&user_info)));
    if let Some(region) = credential_region(&user_info) {
        out.insert("region".into(), json!(region));
    }
    if let Some(method) = first_string(&user_info, &["login_method"]) {
        out.insert("loginMethod".into(), json!(method));
    }
    if let Some(uid) = user_info.get("uid") {
        out.insert("uid".into(), uid.clone());
    }
    if let Some(expire) = user_info.get("expire_time").and_then(Value::as_i64) {
        out.insert("expiresAt".into(), json!(expire));
    }
    if let Some(edition) = first_string(&user_info, &["edition_type", "editionType"]) {
        out.insert("plan".into(), json!(edition));
    }
    Value::Object(out)
}

fn credential_region(user_info: &Value) -> Option<&'static str> {
    match first_string(user_info, &["region"]).as_deref() {
        Some("cn") => return Some("cn"),
        Some("intl") => return Some("intl"),
        _ => {}
    }
    match first_string(user_info, &["platform", "authentication_id"]).as_deref() {
        Some("workbuddy" | "workbuddy-desktop") => return Some("cn"),
        Some("workbuddy-ai" | "workbuddy-desktop-ai") => return Some("intl"),
        _ => {}
    }
    let domain = first_string(user_info, &["domain", "endpoint"])?;
    let url = if domain.contains("://") {
        domain
    } else {
        format!("https://{domain}")
    };
    match url::Url::parse(&url).ok()?.host_str()? {
        "www.codebuddy.cn" | "codebuddy.cn" | "www.workbuddy.cn" | "workbuddy.cn" => Some("cn"),
        "www.workbuddy.ai" | "workbuddy.ai" => Some("intl"),
        _ => None,
    }
}

pub(crate) fn catalog_fingerprint() -> Option<String> {
    let (_, user_info) = credential()?;
    let uid = first_string(&user_info, &["uid"])?;
    let domain = first_string(&user_info, &["domain", "endpoint"])
        .unwrap_or_else(|| DEFAULT_API_BASE.to_string());
    // Older snapshots used another catalog audience and can contain internal
    // or retired definitions. They must not render before the first CLI fetch.
    Some(json!([uid, domain, "cli-models-v1"]).to_string())
}

pub fn logout() -> Result<Value, Value> {
    super::catalog::forget_cache();
    match std::fs::remove_file(store_path()) {
        Ok(()) => Ok(json!({ "ok": true })),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(json!({ "ok": true })),
        Err(error) => Ok(json!({ "ok": false, "error": error.to_string() })),
    }
}

/// WorkBuddy has no plaintext credential cache to fall back on.
///
/// Its one store is the encrypted `.info` the official client writes, and the key
/// that opens it is compiled into that client's own Electron binary — opening it is
/// a host capability, so it arrives through [`adopt_credential`] instead. Saying so
/// here beats a silent "nothing found" that reads as "not installed".
pub fn import_local() -> Result<Value, Value> {
    Ok(json!({
        "ok": false,
        "error": "workbuddy_local_credentials_unavailable",
    }))
}

/// Store a profile the host decrypted out of WorkBuddy's own store.
///
/// `device_id` is whatever identity the official client recorded, kept so a later
/// refresh presents the same install the token was issued to.
pub fn adopt_credential(user_info: Value, device_id: Option<String>) -> Result<Value, Value> {
    let device_id = device_id
        .filter(|value| !value.trim().is_empty())
        .or_else(|| first_string(&user_info, &["device_id", "deviceId"]))
        .unwrap_or_else(random_id);
    adopt(user_info, device_id)
}

fn adopt(user_info: Value, device_id: String) -> Result<Value, Value> {
    if !user_info.is_object() {
        return Ok(json!({ "ok": false, "error": "workbuddy_client_credentials_invalid" }));
    }
    // Read every value the projection needs BEFORE borrowing the map mutably:
    // holding `as_object_mut` while reading `user_info` is a simultaneous
    // borrow of the same value.
    let access = first_string(&user_info, &["access_token", "accessToken"]).unwrap_or_default();
    if access.is_empty() {
        return Ok(json!({
            "ok": false,
            "error": "workbuddy_client_credentials_have_no_token",
        }));
    }
    let refresh = first_string(&user_info, &["refresh_token", "refreshToken"]);
    // The official store carries absolute millisecond expiries; the engine works
    // in seconds, so normalize once at the boundary.
    let expire = user_info
        .get("expiresAt")
        .or_else(|| user_info.get("expire_time"))
        .and_then(Value::as_i64)
        .map(normalize_epoch);
    let domain = first_string(&user_info, &["domain"]);

    let mut user_info = user_info;
    let target = user_info
        .as_object_mut()
        .expect("checked to be an object above");
    target.insert("access_token".into(), json!(access));
    if let Some(refresh) = refresh {
        target.insert("refresh_token".into(), json!(refresh));
    }
    if let Some(expire) = expire {
        target.insert("expire_time".into(), json!(expire));
    }
    if let Some(domain) = domain {
        target.insert("domain".into(), json!(domain));
    }
    target.insert("login_method".into(), json!("adopted"));
    target.insert("login_timestamp".into(), json!(now_seconds()));
    let user_info = Value::Object(target.clone());
    write_store(&device_id, &user_info).map_err(|error| json!({ "ok": false, "error": error }))?;
    super::catalog::forget_cache();
    Ok(json!({ "ok": true, "account": account_label(&user_info) }))
}

fn normalize_epoch(value: i64) -> i64 {
    if value > 100_000_000_000 {
        value / 1000
    } else {
        value
    }
}

fn random_id() -> String {
    super::super::qoder::uuid_v4()
}

// ------------------------------------------------------------------ login

struct PendingFlow {
    state: String,
    platform: String,
    /// The gateway this flow was opened against.
    ///
    /// A login cannot ask the stored credential which gateway it belongs to: there
    /// is no credential yet, that is the whole point. So the base is resolved once
    /// when the flow starts and carried through the poll and the account fetch,
    /// instead of each step falling back to the default and quietly sending a
    /// domestic login to the overseas gateway.
    api_base: String,
    expires_at: i64,
}

fn pending_flows() -> &'static Mutex<std::collections::HashMap<String, PendingFlow>> {
    static PENDING: OnceLock<Mutex<std::collections::HashMap<String, PendingFlow>>> =
        OnceLock::new();
    PENDING.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

/// Ask the gateway for a one-time login state and the browser page that consumes it.
pub async fn start(region: Option<WorkBuddyRegion>) -> Result<Value, Value> {
    let client = client().map_err(|error| json!({ "ok": false, "error": error }))?;
    // Resolved here rather than per request: the store is still empty at this
    // point, so every later step of THIS flow has to be told which gateway it
    // belongs to instead of re-deriving it.
    let (api_base, platform) = login_target(region);
    let url = format!("{api_base}{AUTH_STATE_PATH}");
    let response = unauthenticated(
        client
            .post(&url)
            .query(&[("platform", platform.as_str())])
            .header("Content-Type", "application/json")
            .body("{}"),
    )
    .send()
    .await
    .map_err(|error| json!({ "ok": false, "error": error.to_string() }))?;
    let status_code = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    if let Some(message) = license_or_ip_message(status_code, &body) {
        return Ok(json!({ "ok": false, "error": message }));
    }
    if !(200..300).contains(&status_code) {
        let detail: String = body.chars().take(200).collect();
        return Ok(json!({ "ok": false, "error": format!("auth state {status_code}: {detail}") }));
    }
    let parsed: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    let data = parsed.get("data").unwrap_or(&parsed);
    let auth_url = first_string(data, &["authUrl", "auth_url"]).unwrap_or_default();
    let state = first_string(data, &["state"]).unwrap_or_default();
    if auth_url.is_empty() || state.is_empty() {
        return Ok(json!({ "ok": false, "error": "auth state returned no login page" }));
    }
    let flow_id = random_id();
    let expires_at = now_ms() + FLOW_TIMEOUT_MS;
    pending_flows()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(
            flow_id.clone(),
            PendingFlow {
                state,
                platform,
                api_base,
                expires_at,
            },
        );
    Ok(json!({
        "ok": true,
        "flowId": flow_id,
        "authUrl": auth_url,
        "expiresAt": expires_at,
        "interval": POLL_INTERVAL_SECONDS,
    }))
}

/// Trade the flow handle for the token the browser page deposited.
///
/// The gateway answers HTTP 200 with business code `11217` for as long as the
/// page has not finished, which is a pending poll and not a failure.
pub async fn poll(payload: &Value) -> Result<Value, Value> {
    let flow_id = payload
        .get("flowId")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if flow_id.is_empty() {
        return Ok(json!({ "ok": false, "error": "Invalid flowId" }));
    }
    let Some((state, api_base, platform)) = pending_flows()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(&flow_id)
        .filter(|flow| flow.expires_at > now_ms())
        .map(|flow| {
            (
                flow.state.clone(),
                flow.api_base.clone(),
                flow.platform.clone(),
            )
        })
    else {
        // An expired flow is dropped rather than left to accumulate.
        pending_flows()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&flow_id);
        return Ok(json!({
            "ok": false,
            "expired": true,
            "error": "Login flow expired. Start again.",
        }));
    };
    let client = match client() {
        Ok(client) => client,
        Err(error) => return Ok(json!({ "ok": false, "error": error })),
    };
    let url = format!("{api_base}{AUTH_TOKEN_PATH}");
    let response = unauthenticated(client.get(&url).query(&[("state", state.as_str())]))
        .send()
        .await;
    let response = match response {
        Ok(response) => response,
        Err(error) => return Ok(json!({ "ok": false, "error": error.to_string() })),
    };
    let status_code = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    if let Some(message) = license_or_ip_message(status_code, &body) {
        pending_flows()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&flow_id);
        return Ok(json!({ "ok": false, "error": message }));
    }
    let parsed: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    if business_code(&parsed) == Some(LOGIN_PENDING_CODE) {
        return Ok(json!({ "ok": false, "pending": true }));
    }
    if !(200..300).contains(&status_code) {
        let detail: String = body.chars().take(200).collect();
        return Ok(json!({
            "ok": false,
            "error": format!("auth token {status_code}: {detail}"),
        }));
    }
    let data = parsed.get("data").unwrap_or(&parsed);
    let access = first_string(data, &["accessToken", "access_token"]).unwrap_or_default();
    if access.is_empty() {
        return Ok(json!({ "ok": false, "pending": true }));
    }
    pending_flows()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(&flow_id);
    match complete_login(&client, data, &api_base, &platform).await {
        Ok(account) => Ok(json!({ "ok": true, "account": account })),
        Err(error) => Ok(json!({ "ok": false, "error": error })),
    }
}

fn business_code(parsed: &Value) -> Option<i64> {
    parsed.get("code").and_then(Value::as_i64).or_else(|| {
        parsed
            .get("response")
            .and_then(|response| response.get("data"))
            .and_then(|data| data.get("code"))
            .and_then(Value::as_i64)
    })
}

/// The two gateway refusals that deserve their own sentence: a license problem is
/// not fixable by retrying, and an IP restriction is not fixable at all.
fn license_or_ip_message(status_code: u16, body: &str) -> Option<String> {
    let parsed: Value = serde_json::from_str(body).unwrap_or(Value::Null);
    match business_code(&parsed) {
        Some(code) if LICENSE_ERROR_CODES.contains(&code) => Some(format!(
            "workbuddy_license_rejected:{code}:{}",
            parsed
                .get("msg")
                .and_then(Value::as_str)
                .unwrap_or("license rejected")
        )),
        Some(IP_LIMIT_CODE) => Some("workbuddy_ip_limited".to_string()),
        _ if status_code == 401 || status_code == 403 => {
            Some("workbuddy_login_expired".to_string())
        }
        _ => None,
    }
}

async fn complete_login(
    client: &reqwest::Client,
    token_payload: &Value,
    api_base: &str,
    login_platform: &str,
) -> Result<String, String> {
    let mut fields = Map::new();
    let access = first_string(token_payload, &["accessToken", "access_token"])
        .ok_or_else(|| "login returned no access token".to_string())?;
    fields.insert("access_token".into(), json!(access));
    if let Some(refresh) = first_string(token_payload, &["refreshToken", "refresh_token"]) {
        fields.insert("refresh_token".into(), json!(refresh));
    }
    let domain = first_string(token_payload, &["domain"]).unwrap_or_else(|| api_base.to_string());
    fields.insert("domain".into(), json!(domain));
    if let Some(expire) = absolute_expiry_seconds(token_payload) {
        fields.insert("expire_time".into(), json!(expire));
    }
    fields.insert("platform".into(), json!(login_platform));
    fields.insert("login_method".into(), json!("browser"));
    fields.insert("login_timestamp".into(), json!(now_seconds()));

    let mut user_info = Value::Object(fields);
    // The token alone names no account: the display name, uid and edition all come
    // from the account list, which is also what tells a personal login from an
    // enterprise one (and enterprise is what decides the usage endpoint). The
    // account fetch uses the flow's own gateway, because the store that
    // `api_base()` reads is not written until the very end of this function.
    if let Ok(account) = fetch_account(client, &access, &user_info, api_base).await {
        merge_user_info(&mut user_info, account);
    }
    let device_id = first_string(&user_info, &["uid"]).unwrap_or_else(random_id);
    write_store(&device_id, &user_info)?;
    super::catalog::forget_cache();
    Ok(account_label(&user_info))
}

/// `expiresIn` is relative seconds and `expiresAt` absolute milliseconds; the
/// official client turns the former into the latter, and the engine works in
/// seconds throughout.
fn absolute_expiry_seconds(payload: &Value) -> Option<i64> {
    if let Some(expire) = payload
        .get("expiresAt")
        .or_else(|| payload.get("expire_time"))
        .and_then(Value::as_i64)
        .filter(|value| *value > 0)
    {
        return Some(normalize_epoch(expire));
    }
    payload
        .get("expiresIn")
        .or_else(|| payload.get("expires_in"))
        .and_then(Value::as_i64)
        .filter(|value| *value > 0)
        .map(|seconds| now_seconds() + seconds)
}

async fn fetch_account(
    client: &reqwest::Client,
    token: &str,
    user_info: &Value,
    api_base: &str,
) -> Result<Value, String> {
    let url = format!("{api_base}{ACCOUNTS_PATH}");
    let response = authenticated(client.get(&url), token, user_info)
        .send()
        .await
        .map_err(|error| error.to_string())?;
    let status_code = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    if !(200..300).contains(&status_code) {
        let detail: String = body.chars().take(200).collect();
        return Err(format!("accounts {status_code}: {detail}"));
    }
    let parsed: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    Ok(normalize_account(&parsed))
}

/// Flatten the account list to the one row that is signed in.
///
/// The list carries both spellings of most fields depending on which service
/// answered, so both are read; the fields that matter downstream are `uid`,
/// `nickname`, `type`, `enterpriseId` and `editionType`.
pub(crate) fn normalize_account(parsed: &Value) -> Value {
    let accounts = parsed
        .get("data")
        .and_then(|data| data.get("accounts"))
        .and_then(Value::as_array)
        .or_else(|| parsed.get("accounts").and_then(Value::as_array));
    let Some(accounts) = accounts else {
        return json!({});
    };
    let chosen = accounts
        .iter()
        .find(|account| {
            account
                .get("lastLogin")
                .or_else(|| account.get("last_login"))
                .and_then(Value::as_bool)
                == Some(true)
        })
        .or_else(|| accounts.first());
    let Some(account) = chosen else {
        return json!({});
    };
    let pick = |keys: &[&str]| -> Option<String> { first_string(account, keys) };
    let mut out = Map::new();
    if let Some(uid) = pick(&["uid"]) {
        out.insert("uid".into(), json!(uid));
    }
    if let Some(uin) = pick(&["uin"]) {
        out.insert("uin".into(), json!(uin));
    }
    if let Some(nickname) = pick(&["nickname", "name", "displayName"]) {
        out.insert("nickname".into(), json!(nickname));
    }
    if let Some(kind) = pick(&["type"]) {
        out.insert("type".into(), json!(kind));
    }
    if let Some(enterprise_id) = pick(&["enterpriseId", "enterprise_id"]) {
        out.insert("enterprise_id".into(), json!(enterprise_id));
    }
    if let Some(edition) = pick(&["editionType", "edition_type"]) {
        out.insert("edition_type".into(), json!(edition));
    }
    if let Some(expire) = account
        .get("expireAt")
        .or_else(|| account.get("expire_at"))
        .and_then(Value::as_i64)
        .filter(|value| *value > 0)
    {
        out.insert("subscription_expire_ms".into(), json!(expire));
    }
    Value::Object(out)
}

fn merge_user_info(target: &mut Value, extra: Value) {
    let (Some(target), Some(extra)) = (target.as_object_mut(), extra.as_object()) else {
        return;
    };
    for (key, value) in extra {
        target.insert(key.clone(), value.clone());
    }
}

// --------------------------------------------------------------- refresh

/// Whether this credential is one Klaus owns the whole lifecycle of.
///
/// A login Klaus itself started is safe to renew. A profile copied out of
/// WorkBuddy's own store is NOT: see [`refresh_token_now`].
fn owns_refresh(user_info: &Value) -> bool {
    first_string(user_info, &["login_method"]).as_deref() != Some("adopted")
}

/// Whether the stored credential is one Klaus may spend a refresh on.
///
/// The catalog asks this before trying, so a copied login is reported dead
/// straight away — with the `login_expired` flag the host keys its re-import on —
/// instead of after a refresh attempt that was always going to be refused and
/// whose refusal would then read as the reason the catalog failed.
pub(crate) fn can_refresh() -> bool {
    credential().is_some_and(|(_, user_info)| owns_refresh(&user_info))
}

pub(crate) async fn ensure_fresh_token() -> Result<(), String> {
    let Some((_, user_info)) = credential() else {
        return Ok(());
    };
    if !owns_refresh(&user_info) {
        // The copied token stays valid for about a year and WorkBuddy renews its
        // own copy on its own schedule, so "not yet expired" needs no action here.
        return Ok(());
    }
    let Some(expire_time) = user_info.get("expire_time").and_then(Value::as_i64) else {
        return Ok(());
    };
    if expire_time - REFRESH_SKEW_SECONDS > now_seconds() {
        return Ok(());
    }
    refresh_token_now().await
}

/// Exchange the stored refresh token unconditionally.
///
/// `expire_time` only bounds the session; a re-login in WorkBuddy rotates the
/// token early, and from then on every call answers 401 long before the stored
/// expiry. Callers that observe such a rejection refresh through this path
/// instead of waiting out the skew window.
///
/// A COPIED credential is refused instead. WorkBuddy's refresh returns a whole
/// new token set and overwrites the session with it, which is rotation-shaped:
/// spending the refresh token a running WorkBuddy client also holds risks
/// invalidating the copy that client has, signing the user out of their own app
/// to keep a Klaus session alive. Nothing in the client states the server's
/// policy outright, so the safe reading is that it is one-time. An adopted login
/// therefore lives on its access token (valid for about a year) and the host's
/// recovery path re-copies a fresh one rather than renewing this copy; when the
/// copy has genuinely died, the user is told to re-import or sign in.
pub async fn refresh_token_now() -> Result<(), String> {
    let Some((device_id, user_info)) = credential() else {
        return Ok(());
    };
    if !owns_refresh(&user_info) {
        return Err("workbuddy_adopted_credential_needs_reimport".to_string());
    }
    let refresh_token =
        first_string(&user_info, &["refresh_token", "refreshToken"]).ok_or_else(|| {
            "WorkBuddy subscription has expired and carries no refresh token".to_string()
        })?;
    let client = client()?;
    let url = format!("{}{AUTH_REFRESH_PATH}", api_base());
    let mut request = client
        .post(&url)
        .header("X-Refresh-Token", refresh_token)
        .header("X-Auth-Refresh-Source", "plugin")
        .header("Content-Type", "application/json")
        .header("User-Agent", super::catalog::user_agent())
        .body("{}");
    if let Some(domain) = first_string(&user_info, &["domain"]) {
        request = request.header("X-Domain", domain);
    }
    let response = request.send().await.map_err(|error| error.to_string())?;
    let status_code = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    if status_code == 401 || status_code == 403 {
        return Err("workbuddy_login_expired".to_string());
    }
    if !(200..300).contains(&status_code) {
        let detail: String = body.chars().take(200).collect();
        return Err(format!("token refresh {status_code}: {detail}"));
    }
    let parsed: Value = serde_json::from_str(&body)
        .map_err(|_| "token refresh returned invalid JSON".to_string())?;
    let data = parsed.get("data").unwrap_or(&parsed);
    let token = first_string(data, &["accessToken", "access_token"])
        .ok_or_else(|| "token refresh returned no token".to_string())?;
    let mut user_info = user_info;
    {
        let Some(target) = user_info.as_object_mut() else {
            return Err("profile is not an object".into());
        };
        target.insert("access_token".into(), json!(token));
        if let Some(refresh) = first_string(data, &["refreshToken", "refresh_token"]) {
            target.insert("refresh_token".into(), json!(refresh));
        }
        if let Some(domain) = first_string(data, &["domain"]) {
            target.insert("domain".into(), json!(domain));
        }
        if let Some(expire) = absolute_expiry_seconds(data) {
            target.insert("expire_time".into(), json!(expire));
        }
    }
    write_store(&device_id, &user_info)?;
    super::catalog::forget_cache();
    Ok(())
}

// ----------------------------------------------------------------- usage

/// The credit balance, in the same shape the host already projects for Qoder and
/// QwenWork so all three render as one "remaining of total" row.
pub async fn usage() -> Result<Value, String> {
    if access_token().is_err() {
        return Ok(json!({ "signedIn": false }));
    }
    ensure_fresh_token().await?;
    let token = access_token().map_err(|error| error.message)?;
    let (device_id, user_info) = credential().ok_or("no stored credential")?;
    let client = client()?;
    let base = api_base();

    // The account list is the only place the edition and the subscription expiry
    // live, and it is also what decides enterprise vs personal.
    let mut profile = user_info.clone();
    if let Ok(account) = fetch_account(&client, &token.access_token, &profile, &base).await {
        merge_user_info(&mut profile, account);
    }
    let enterprise_id = first_string(&profile, &["enterprise_id", "enterpriseId"]);
    let quota = match &enterprise_id {
        Some(enterprise_id) if !enterprise_id.is_empty() => {
            enterprise_quota(&client, &token.access_token, &profile, enterprise_id).await?
        }
        _ => personal_quota(&client, &token.access_token, &profile).await?,
    };
    let _ = device_id;
    Ok(json!({
        "signedIn": true,
        "account": token.account,
        "quota": quota,
    }))
}

async fn enterprise_quota(
    client: &reqwest::Client,
    token: &str,
    profile: &Value,
    enterprise_id: &str,
) -> Result<Value, String> {
    let url = format!("{}{ENTERPRISE_USAGE_PATH}", api_base());
    let response = authenticated(client.post(&url), token, profile)
        .header("X-Enterprise-Id", enterprise_id)
        .body("{}")
        .send()
        .await
        .map_err(|error| error.to_string())?;
    let status_code = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    if !(200..300).contains(&status_code) {
        let detail: String = body.chars().take(200).collect();
        return Err(format!("enterprise usage {status_code}: {detail}"));
    }
    let parsed: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    if parsed.get("code").and_then(Value::as_i64) != Some(0) {
        return Err(format!(
            "enterprise usage refused: {}",
            parsed
                .get("msg")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
        ));
    }
    Ok(quota_from_enterprise(
        parsed.get("data").unwrap_or(&parsed),
        profile,
    ))
}

/// `limit_num` of `-1` is the gateway's "unlimited": there is no remaining count
/// to show, so the quota reports the plan and no credit pool rather than a
/// nonsense bar.
pub(crate) fn quota_from_enterprise(data: &Value, profile: &Value) -> Value {
    let limit = data.get("limit_num").and_then(Value::as_f64);
    let used = data
        .get("used_num")
        .and_then(Value::as_f64)
        .unwrap_or_default();
    let mut out = Map::new();
    out.insert("userType".into(), json!("enterprise"));
    if let Some(expire) = subscription_expiry(profile) {
        out.insert("expiresAt".into(), json!(expire));
    }
    match limit {
        Some(limit) if limit < 0.0 => {
            out.insert("unlimited".into(), json!(true));
        }
        Some(limit) => {
            out.insert(
                "userQuota".into(),
                json!({
                    "total": limit,
                    "used": used,
                    "remaining": (limit - used).max(0.0),
                    "unit": "credits",
                }),
            );
        }
        None => {}
    }
    Value::Object(out)
}

async fn personal_quota(
    client: &reqwest::Client,
    token: &str,
    profile: &Value,
) -> Result<Value, String> {
    let url = format!("{}{PERSONAL_USAGE_PATH}", api_base());
    let response = authenticated(client.post(&url), token, profile)
        .body("{}")
        .send()
        .await
        .map_err(|error| error.to_string())?;
    let status_code = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    if !(200..300).contains(&status_code) {
        let detail: String = body.chars().take(200).collect();
        return Err(format!("resource summary {status_code}: {detail}"));
    }
    let parsed: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    let data = parsed.get("data").unwrap_or(&parsed);
    if data.get("Packages").is_none() && data.get("packages").is_none() {
        return Err("resource summary carried no packages".to_string());
    }
    Ok(quota_from_resource_summary(data, profile))
}

/// Sum the credit packages the same way the official client does: total, remaining
/// and used are each accumulated across packages, and a package that reports no
/// capacity contributes nothing.
///
/// The counts arrive as STRINGS (`"250"`), so every read goes through the numeric
/// coercion rather than `as_f64`, which would answer `None` for all of them.
pub(crate) fn quota_from_resource_summary(data: &Value, profile: &Value) -> Value {
    let packages = data
        .get("Packages")
        .or_else(|| data.get("packages"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut total = 0.0f64;
    let mut remaining = 0.0f64;
    let mut used = 0.0f64;
    for package in &packages {
        total += positive(
            package,
            &["CycleTotalCapacity", "cycleTotalCapacity", "cycle_total"],
        );
        remaining += positive(
            package,
            &["CycleRemainCapacity", "cycleRemainCapacity", "cycle_remain"],
        );
        used += positive(
            package,
            &["CycleUsedCapacity", "cycleUsedCapacity", "cycle_used"],
        );
    }
    let mut out = Map::new();
    out.insert(
        "userType".into(),
        json!(
            first_string(profile, &["edition_type", "editionType"]).unwrap_or_else(|| {
                if data.get("IsPaidUser").and_then(Value::as_bool) == Some(true) {
                    "pro".to_string()
                } else {
                    "personal".to_string()
                }
            })
        ),
    );
    out.insert(
        "userQuota".into(),
        json!({
            "total": total,
            "used": used,
            "remaining": remaining,
            "unit": "credits",
        }),
    );
    if let Some(expire) = subscription_expiry(profile) {
        out.insert("expiresAt".into(), json!(expire));
    }
    Value::Object(out)
}

fn positive(source: &Value, keys: &[&str]) -> f64 {
    first_string(source, keys)
        .and_then(|text| text.trim().parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value > 0.0)
        .unwrap_or_default()
}

fn subscription_expiry(profile: &Value) -> Option<i64> {
    let expire = profile
        .get("subscription_expire_ms")
        .or_else(|| profile.get("expireAt"))
        .or_else(|| profile.get("expire_at"))
        .and_then(Value::as_i64)
        .filter(|value| *value > 0)?;
    // The host's projection reads either unit; keep milliseconds as the account
    // service delivered them so no precision is lost on the way through.
    Some(expire)
}

#[cfg(test)]
#[path = "auth_region_tests.rs"]
mod region_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_summary_counts_arrive_as_strings_and_are_summed() {
        // The live personal response spells every capacity as a string, which is
        // the shape that a `as_f64`-only reader would silently turn into zeros.
        let data = json!({
            "Packages": [
                {
                    "PackageCode": "TCACA_code_006",
                    "CycleTotalCapacity": "250",
                    "CycleRemainCapacity": "250",
                    "CycleUsedCapacity": "0",
                    "CapacityUnit": "credit",
                },
                {
                    "PackageCode": "TCACA_code_035",
                    "CycleTotalCapacity": "100",
                    "CycleRemainCapacity": "40",
                    "CycleUsedCapacity": "60",
                    "CapacityUnit": "credits",
                }
            ],
            "IsPaidUser": false,
        });
        let quota = quota_from_resource_summary(&data, &json!({}));
        assert_eq!(quota["userQuota"]["total"], json!(350.0));
        assert_eq!(quota["userQuota"]["remaining"], json!(290.0));
        assert_eq!(quota["userQuota"]["used"], json!(60.0));
        assert_eq!(quota["userQuota"]["unit"], json!("credits"));
        assert_eq!(quota["userType"], json!("personal"));
    }

    #[test]
    fn the_live_personal_summary_projects_onto_one_remaining_row() {
        // The exact body the gateway returned for a real free account, captured
        // 2026-09-27. Two packages, capacities as STRINGS, and a mixed
        // `credit`/`credits` spelling that the official summation ignores — so
        // this pins the projection against the real shape rather than a guess.
        let data = json!({
            "Packages": [
                {
                    "PackageCode": "TCACA_code_006_DbXS0lrypC",
                    "CycleTotalCapacity": "250",
                    "CycleRemainCapacity": "250",
                    "CycleUsedCapacity": "0",
                    "CycleFrozenCapacity": "0",
                    "CapacityUnit": "credit",
                    "TotalCount": 1
                },
                {
                    "PackageCode": "TCACA_code_035_ArVxJcGDsm",
                    "CycleTotalCapacity": "100",
                    "CycleRemainCapacity": "100",
                    "CycleUsedCapacity": "0",
                    "CycleFrozenCapacity": "0",
                    "CapacityUnit": "credits",
                    "TotalCount": 1
                }
            ],
            "SubscriptionPackageCode": "",
            "IsPaidUser": false,
            "IsProtectedPriceUser": false,
            "ProTrialStatus": 0
        });
        let quota = quota_from_resource_summary(&data, &json!({}));
        assert_eq!(quota["userQuota"]["total"], json!(350.0));
        assert_eq!(quota["userQuota"]["remaining"], json!(350.0));
        assert_eq!(quota["userQuota"]["used"], json!(0.0));
        assert_eq!(quota["userType"], json!("personal"));
        // A frozen slice is not spent, and `TotalCount` is not a multiplier: both
        // are ignored exactly as the official client ignores them.
        assert!(quota.get("expiresAt").is_none());
    }

    #[test]
    fn a_paid_account_names_its_edition_and_a_free_one_stays_personal() {
        let paid =
            quota_from_resource_summary(&json!({ "Packages": [], "IsPaidUser": true }), &json!({}));
        assert_eq!(paid["userType"], json!("pro"));
        let edition = quota_from_resource_summary(
            &json!({ "Packages": [] }),
            &json!({ "edition_type": "flagship" }),
        );
        assert_eq!(edition["userType"], json!("flagship"));
    }

    #[test]
    fn enterprise_unlimited_reports_no_credit_pool() {
        let quota = quota_from_enterprise(&json!({ "limit_num": -1, "used_num": 12 }), &json!({}));
        assert_eq!(quota["unlimited"], json!(true));
        assert!(quota.get("userQuota").is_none());
        assert_eq!(quota["userType"], json!("enterprise"));
    }

    #[test]
    fn enterprise_limits_become_a_remaining_count() {
        let quota = quota_from_enterprise(
            &json!({ "limit_num": 500, "used_num": 120 }),
            &json!({ "subscription_expire_ms": 1822047616094_i64 }),
        );
        assert_eq!(quota["userQuota"]["total"], json!(500.0));
        assert_eq!(quota["userQuota"]["used"], json!(120.0));
        assert_eq!(quota["userQuota"]["remaining"], json!(380.0));
        assert_eq!(quota["expiresAt"], json!(1822047616094_i64));
    }

    #[test]
    fn enterprise_usage_never_reports_a_negative_remaining() {
        let quota =
            quota_from_enterprise(&json!({ "limit_num": 100, "used_num": 140 }), &json!({}));
        assert_eq!(quota["userQuota"]["remaining"], json!(0.0));
    }

    #[test]
    fn the_account_list_flattens_to_the_row_that_is_signed_in() {
        let parsed = json!({
            "code": 0,
            "data": { "accounts": [
                { "uid": "other", "nickname": "Other", "lastLogin": false },
                {
                    "uid": "fixture-uid",
                    "uin": "450700000000",
                    "nickname": "fixture-user",
                    "type": "personal",
                    "lastLogin": true,
                    "editionType": "free",
                    "expireAt": 1822047616094_i64,
                },
            ] }
        });
        let account = normalize_account(&parsed);
        assert_eq!(account["uid"], json!("fixture-uid"));
        assert_eq!(account["nickname"], json!("fixture-user"));
        assert_eq!(account["type"], json!("personal"));
        assert_eq!(account["edition_type"], json!("free"));
        assert_eq!(account["subscription_expire_ms"], json!(1822047616094_i64));
    }

    #[test]
    fn a_single_account_is_chosen_without_a_last_login_flag() {
        let parsed = json!({ "data": { "accounts": [{ "uid": "only", "nickname": "Only" }] } });
        assert_eq!(normalize_account(&parsed)["uid"], json!("only"));
        assert_eq!(normalize_account(&json!({})), json!({}));
    }

    #[test]
    fn pending_login_is_a_business_code_not_an_http_failure() {
        // Measured: HTTP 200 with `11217:login ing...` for as long as the page
        // has not finished. Treating that as an error would abort every login.
        let body = json!({ "code": 11217, "msg": "11217:login ing..." });
        assert_eq!(business_code(&body), Some(LOGIN_PENDING_CODE));
        assert_eq!(
            license_or_ip_message(200, &body.to_string()),
            None,
            "a pending poll is neither a license nor an IP refusal"
        );
    }

    #[test]
    fn license_and_ip_refusals_get_their_own_message() {
        for code in LICENSE_ERROR_CODES {
            let body = json!({ "code": code, "msg": "seats exhausted" }).to_string();
            let message = license_or_ip_message(400, &body).expect("license message");
            assert!(
                message.starts_with("workbuddy_license_rejected:"),
                "{message}"
            );
        }
        let body = json!({ "code": 10081, "msg": "ip limited" }).to_string();
        assert_eq!(
            license_or_ip_message(400, &body),
            Some("workbuddy_ip_limited".to_string())
        );
    }

    #[test]
    fn a_rejected_token_is_reported_as_a_dead_login() {
        assert_eq!(
            license_or_ip_message(401, "{}"),
            Some("workbuddy_login_expired".to_string())
        );
        assert_eq!(
            license_or_ip_message(403, "{}"),
            Some("workbuddy_login_expired".to_string())
        );
        assert_eq!(license_or_ip_message(500, "{}"), None);
    }

    #[test]
    fn relative_and_absolute_expiries_both_land_in_seconds() {
        let absolute = absolute_expiry_seconds(&json!({ "expiresAt": 1822047616094_i64 }));
        assert_eq!(absolute, Some(1822047616));
        let relative = absolute_expiry_seconds(&json!({ "expiresIn": 3600 }));
        assert!(relative.is_some_and(|at| at >= now_seconds() + 3500));
        // `expiresAt` wins when both arrive, and a zero is not an epoch.
        assert_eq!(
            absolute_expiry_seconds(&json!({ "expiresAt": 1822047616094_i64, "expiresIn": 60 })),
            Some(1822047616)
        );
        assert_eq!(absolute_expiry_seconds(&json!({ "expiresAt": 0 })), None);
    }

    #[tokio::test]
    async fn a_credential_without_a_token_is_not_a_login() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        write_store("device", &json!({ "nickname": "No Token" })).expect("store written");
        assert_eq!(status(), json!({ "loggedIn": false }));
        assert!(access_token().is_err());
        write_store(
            "device",
            &json!({ "access_token": "abc", "nickname": "Ada" }),
        )
        .expect("store written");
        assert_eq!(status()["loggedIn"], json!(true));
        assert_eq!(status()["account"], json!("Ada"));
    }

    #[tokio::test]
    async fn adopting_a_profile_keeps_the_device_identity_and_normalizes_expiry() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        let adopted = adopt_credential(
            json!({
                "accessToken": "jwt-value",
                "refreshToken": "refresh-value",
                "expiresAt": 1822047616094_i64,
                "domain": "www.workbuddy.ai",
                "uid": "fixture-uid",
                "nickname": "fixture-user",
            }),
            Some("fixture-uid".to_string()),
        )
        .expect("adopt answer");
        assert_eq!(adopted["ok"], json!(true));
        let (device_id, user_info) = credential().expect("stored");
        assert_eq!(device_id, "fixture-uid");
        assert_eq!(user_info["access_token"], json!("jwt-value"));
        assert_eq!(user_info["refresh_token"], json!("refresh-value"));
        assert_eq!(user_info["expire_time"], json!(1822047616));
        assert_eq!(user_info["login_method"], json!("adopted"));
        assert_eq!(api_base(), "https://www.workbuddy.ai");
    }

    #[tokio::test]
    async fn adopting_a_domestic_profile_switches_the_gateway_with_it() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        adopt_credential(
            json!({ "accessToken": "jwt", "domain": "www.codebuddy.cn" }),
            None,
        )
        .expect("adopt answer");
        assert_eq!(api_base(), "https://www.codebuddy.cn");
        assert_eq!(status()["loggedIn"], json!(true));
    }

    #[tokio::test]
    async fn adopting_without_a_token_is_refused_rather_than_stored() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        let refused = adopt_credential(json!({ "nickname": "Ada" }), None).expect("answer");
        assert_eq!(refused["ok"], json!(false));
        assert_eq!(
            refused["error"],
            json!("workbuddy_client_credentials_have_no_token")
        );
        assert!(credential().is_none());
    }

    #[tokio::test]
    async fn a_copied_login_is_not_one_klaus_may_refresh() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());

        // No credential at all: nothing to renew, and the catalog must not be
        // told a refresh would have worked.
        assert!(!can_refresh(), "no credential means no refresh right");

        adopt_credential(
            json!({ "accessToken": "copied-jwt", "refreshToken": "copied-refresh" }),
            Some("uid-1".to_string()),
        )
        .expect("adopt answer");
        assert!(
            !can_refresh(),
            "an adopted profile must never be refreshed by Klaus"
        );
        // The refusal is the guard that keeps a running WorkBuddy client signed in:
        // its refresh is rotation-shaped, so spending the same token could sign the
        // user out of their own app.
        assert_eq!(
            refresh_token_now().await,
            Err("workbuddy_adopted_credential_needs_reimport".to_string())
        );
        // And the pre-expiry check must not try either.
        assert_eq!(ensure_fresh_token().await, Ok(()));

        // A login Klaus itself started does own its refresh token.
        write_store(
            "uid-2",
            &json!({
                "access_token": "own-jwt",
                "refresh_token": "own-refresh",
                "login_method": "browser",
            }),
        )
        .expect("store written");
        assert!(can_refresh(), "a Klaus-started login owns its refresh");
    }

    #[test]
    fn there_is_no_plaintext_local_store_to_import() {
        // WorkBuddy's only store is encrypted under a key in its own binary, so
        // the engine's own import has nothing to read and says so instead of
        // reporting "not installed".
        let answer = import_local().expect("answer");
        assert_eq!(answer["ok"], json!(false));
        assert_eq!(
            answer["error"],
            json!("workbuddy_local_credentials_unavailable")
        );
    }

    #[tokio::test]
    async fn logout_is_idempotent() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        adopt_credential(json!({ "accessToken": "jwt" }), None).expect("adopt answer");
        assert_eq!(logout().expect("logout")["ok"], json!(true));
        assert_eq!(logout().expect("logout again")["ok"], json!(true));
        assert!(credential().is_none());
    }
}
