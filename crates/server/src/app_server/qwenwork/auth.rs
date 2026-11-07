//! QwenWork device login and credential ownership.

use super::super::qoder::sign::{generate_runtime_auth_with_profile, CosyProfile};
use serde_json::{json, Map, Value};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

// Device authorization is served by the gateway, not the public website.
const AUTH_BASE: &str = "https://gateway.qwenwork.cn";
const OPENAPI_BASE: &str = "https://gateway.qwenwork.cn";
const ACCOUNT_CONTEXT_PATH: &str =
    "/api/v1/adapter/user/account-context?include=user,plan,quota,page,data_sharing";
const ENTERPRISE_QUOTA_PATH: &str = "/api/v2/quota/usage";
const ENTERPRISE_SETTINGS_PATH: &str =
    "/api/v1/adapter/biz/settings/enterprise?include=data-sharing-mode";
const CLIENT_ID: &str = "e883ade2-e6e3-4d6d-adf7-f92ceff5fdcb";
const REDIRECT_URI: &str = "qwenwork-cn://";
const FLOW_TIMEOUT_MS: i64 = 300_000;
const POLL_INTERVAL_SECONDS: i64 = 1;
const REFRESH_SKEW_SECONDS: i64 = 3600;
const HTTP_TIMEOUT_MS: u64 = 15_000;
const CLIENT_VERSION: &str = "1.1.0";
const CLIENT_RELEASE_VERSION: &str = "1.1.0-26091701";
const CLIENT_BUILD: &str = "26091701";
const VERIFIER_ALPHABET: &[u8] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~";

#[cfg(test)]
pub(crate) static TEST_OPENAPI_BASE: std::sync::RwLock<Option<String>> =
    std::sync::RwLock::new(None);

fn openapi_base() -> String {
    #[cfg(test)]
    if let Some(base) = TEST_OPENAPI_BASE.read().unwrap().clone() {
        return base;
    }
    OPENAPI_BASE.into()
}

fn refresh_gate() -> &'static tokio::sync::Mutex<()> {
    static GATE: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    GATE.get_or_init(|| tokio::sync::Mutex::new(()))
}

fn store_path() -> PathBuf {
    super::config_root().join("qwenwork-subscription-oauth.json")
}

/// Read-only locations used by the official QwenWork/QoderWork CLI cache. The
/// desktop app's encrypted `auth-v2.dat` is adopted by the host instead; keeping
/// Electron safeStorage out of the engine preserves the single host extension
/// seam used by Qoder as well.
pub(crate) fn local_auth_dirs() -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    if let Ok(value) = std::env::var("QWENWORK_HOME") {
        let root = PathBuf::from(value.trim());
        if !value.trim().is_empty() {
            dirs.extend([root.join(".auth"), root.join("cli/.auth"), root]);
        }
    }
    let home = PathBuf::from(std::env::var("HOME").unwrap_or_default());
    if cfg!(target_os = "macos") {
        let support = home.join("Library/Application Support");
        dirs.extend([
            support.join("QwenWorkCN/cli/.auth"),
            support.join("QoderWork/cli/.auth"),
        ]);
    } else if cfg!(target_os = "windows") {
        if let Ok(app_data) = std::env::var("APPDATA") {
            let support = PathBuf::from(app_data);
            dirs.extend([
                support.join("QwenWorkCN/cli/.auth"),
                support.join("QoderWork/cli/.auth"),
            ]);
        }
    } else {
        let support = home.join(".config");
        dirs.extend([
            support.join("QwenWorkCN/cli/.auth"),
            support.join("QoderWork/cli/.auth"),
        ]);
    }
    dirs
}

/// Catalog-only fallback locations. QoderWork shares the account system but
/// ships an older-generation `qwork` catalog under its own keys, so a QwenWork
/// picker that fell back to it would paint another product's model list.
pub(crate) fn local_catalog_dirs() -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    if let Ok(value) = std::env::var("QWENWORK_HOME") {
        let root = PathBuf::from(value.trim());
        if !value.trim().is_empty() {
            dirs.extend([root.join("cli"), root.join("cli/.auth"), root]);
        }
    }
    let home = PathBuf::from(std::env::var("HOME").unwrap_or_default());
    // The official CLI keeps its catalog one level above its auth dir, so both
    // are candidates for the `models` read.
    let roots = if cfg!(target_os = "macos") {
        vec![home.join("Library/Application Support/QwenWorkCN")]
    } else if cfg!(target_os = "windows") {
        std::env::var("APPDATA")
            .ok()
            .map(|app_data| vec![PathBuf::from(app_data).join("QwenWorkCN")])
            .unwrap_or_default()
    } else {
        vec![home.join(".config/QwenWorkCN")]
    };
    for root in roots {
        dirs.extend([root.join("cli"), root.join("cli/.auth")]);
    }
    dirs
}

fn machine_os() -> &'static str {
    match std::env::consts::OS {
        "macos" => "darwin",
        other => other,
    }
}

fn client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(HTTP_TIMEOUT_MS))
        .build()
        .map_err(|error| error.to_string())
}

fn with_client_headers(request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    request
        .header("Accept", "application/json")
        .header("User-Agent", format!("qoderwork/{CLIENT_VERSION}"))
        .header("X-Request-Id", random_id())
        .header("X-QwenWork-Version", CLIENT_VERSION)
        .header("X-QwenWork-Release-Version", CLIENT_RELEASE_VERSION)
        .header("X-QwenWork-Build", CLIENT_BUILD)
        .header("X-QwenWork-Platform", machine_os())
        .header("X-QwenWork-Arch", std::env::consts::ARCH)
        .header("X-QwenWork-Channel", "stable")
}

/// QwenWork's production OpenAPI appends a base64url JSON security state to
/// login-sensitive operations. Its native UMID factors are optional even in
/// the official client, so Klaus sends the versioned context it can truthfully
/// provide instead of omitting the state parameter altogether.
fn with_login_security_state(path: &str) -> String {
    use base64::Engine as _;
    let mut state = Map::new();
    state.insert("v".into(), json!(1));
    if let Some(ip) = local_ipv4() {
        state.insert("ip".into(), json!(ip));
    }
    let encoded =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(Value::Object(state).to_string());
    let separator = if path.contains('?') { '&' } else { '?' };
    format!("{path}{separator}state={encoded}")
}

fn local_ipv4() -> Option<String> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect("8.8.8.8:80").ok()?;
    match socket.local_addr().ok()?.ip() {
        std::net::IpAddr::V4(address) if !address.is_loopback() => Some(address.to_string()),
        _ => None,
    }
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

fn store_mutation() -> &'static std::sync::Mutex<()> {
    static MUTATION: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    MUTATION.get_or_init(|| std::sync::Mutex::new(()))
}

fn write_store(machine_id: &str, user_info: &Value) -> Result<(), String> {
    let _guard = store_mutation()
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    write_store_locked(machine_id, user_info)
}

fn write_store_locked(machine_id: &str, user_info: &Value) -> Result<(), String> {
    use std::io::Write;
    let path = store_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|_| "qwenwork_credential_write_failed")?;
    }
    let temporary = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&temporary)
            .map_err(|_| "qwenwork_credential_write_failed")?;
        let serialized = serde_json::to_vec(&json!({"machineId":machine_id,"userInfo":user_info}))
            .map_err(|_| "qwenwork_credential_write_failed")?;
        file.write_all(&serialized)
            .map_err(|_| "qwenwork_credential_write_failed")?;
        file.sync_all()
            .map_err(|_| "qwenwork_credential_write_failed")?;
        std::fs::rename(&temporary, &path).map_err(|_| "qwenwork_credential_write_failed")?;
        super::capabilities::credential_changed();
        Ok(())
    })();
    let _ = std::fs::remove_file(&temporary);
    result
}

fn commit_refresh(machine_id: &str, previous: &Value, updated: &Value) -> Result<(), String> {
    let _guard = store_mutation()
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    if credential().as_ref() != Some(&(machine_id.to_string(), previous.clone())) {
        return Err("qwenwork_credential_changed".into());
    }
    write_store_locked(machine_id, updated)
}

pub(crate) fn credential() -> Option<(String, Value)> {
    let store = read_store()?;
    let machine_id = store.get("machineId").and_then(Value::as_str)?.to_string();
    let mut user_info = store.get("userInfo").cloned()?;
    if machine_id.is_empty() || !user_info.is_object() {
        return None;
    }
    normalize_expiry(&mut user_info);
    Some((machine_id, user_info))
}

/// The native service grant and its publication epoch are read under one mutation lock.
pub(crate) fn native_snapshot() -> Result<(Value, u64), String> {
    let _guard = store_mutation()
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let generation = *super::capabilities::changes().borrow();
    let (_, user) = credential().ok_or("qwenwork_not_signed_in")?;
    if generation != *super::capabilities::changes().borrow() {
        return Err("qwenwork_credential_changed".into());
    }
    Ok((user, generation))
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

fn first_string_array(source: &Value, keys: &[&str]) -> Option<Value> {
    let values = keys
        .iter()
        .find_map(|key| source.get(*key).and_then(Value::as_array))?;
    Some(Value::Array(
        values
            .iter()
            .filter_map(Value::as_str)
            .map(|value| Value::String(value.to_string()))
            .collect(),
    ))
}

fn account_label(user_info: &Value) -> String {
    first_string(user_info, &["name", "display_name", "email", "uid"])
        .unwrap_or_else(|| "QwenWork".into())
}

fn access_token_of(user_info: &Value) -> String {
    first_string(user_info, &["security_oauth_token", "access_token"]).unwrap_or_default()
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
    if let Some(uid) = user_info.get("uid") {
        out.insert("uid".into(), uid.clone());
    }
    if let Some(expire) = user_info.get("expire_time").and_then(Value::as_i64) {
        out.insert("expiresAt".into(), json!(expire));
    }
    Value::Object(out)
}

pub(crate) fn catalog_fingerprint() -> Option<String> {
    let (_, user_info) = credential()?;
    first_string(&user_info, &["uid"])
}

pub fn logout() -> Result<Value, Value> {
    let _guard = store_mutation()
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    super::capabilities::credential_changed();
    super::catalog::forget_context();
    super::catalog::forget_cache();
    match std::fs::remove_file(store_path()) {
        Ok(()) => Ok(json!({ "ok": true })),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(json!({ "ok": true })),
        Err(error) => Ok(json!({ "ok": false, "error": error.to_string() })),
    }
}

/// Adopt the official CLI login without modifying its files. The desktop app's
/// encrypted store is handled by the host and arrives through
/// [`adopt_credential`], while this fallback covers machines that only have the
/// QwenWork CLI cache.
pub fn import_local() -> Result<Value, Value> {
    match import_qwenwork_cli() {
        Ok(result) => Ok(result),
        Err(error) => Ok(json!({ "ok": false, "error": error })),
    }
}

/// Store a profile the desktop host decrypted from QwenWork's safeStorage.
/// `machine_id` is the device id the official client used when the token was
/// issued; retaining it keeps signatures and refreshes on the same install.
pub fn adopt_credential(user_info: Value, machine_id: Option<String>) -> Result<Value, Value> {
    let machine_id = machine_id
        .filter(|value| !value.trim().is_empty())
        .or_else(|| credential().map(|(id, _)| id))
        .unwrap_or_else(random_id);
    adopt(user_info, machine_id)
}

fn import_qwenwork_cli() -> Result<Value, String> {
    let Some((dir, machine_id)) = local_auth_dirs()
        .into_iter()
        .find_map(|dir| read_cli_machine_id(&dir).map(|machine_id| (dir, machine_id)))
    else {
        return Err("qwenwork_client_credentials_not_found".into());
    };
    let encrypted = std::fs::read_to_string(dir.join("user"))
        .map_err(|_| "qwenwork_client_credentials_not_found".to_string())?;
    let key: String = machine_id.chars().take(16).collect();
    let user_info = decrypt_qwenwork_cli_profile(&encrypted, &key)
        .ok_or_else(|| "qwenwork_client_credentials_invalid".to_string())?;
    adopt(user_info, machine_id).map_err(|error| error.to_string())
}

fn read_cli_machine_id(dir: &Path) -> Option<String> {
    ["id", "machine_id"]
        .into_iter()
        .find_map(|name| std::fs::read_to_string(dir.join(name)).ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// Official QwenWork CLI storage uses the same AES-128-CBC construction as
/// Qoder CLI: the first 16 characters of the adjacent device id are both key
/// and IV, and the ciphertext is standard base64.
fn decrypt_qwenwork_cli_profile(encrypted: &str, key: &str) -> Option<Value> {
    use aes::cipher::{block_padding::Pkcs7, BlockDecryptMut, KeyIvInit};
    use base64::Engine as _;

    if key.len() != 16 {
        return None;
    }
    let raw = base64::engine::general_purpose::STANDARD
        .decode(encrypted.trim())
        .ok()?;
    if raw.is_empty() || !raw.len().is_multiple_of(16) {
        return None;
    }
    let key = key.as_bytes();
    let plain = cbc::Decryptor::<aes::Aes128>::new(key.into(), key.into())
        .decrypt_padded_vec_mut::<Pkcs7>(&raw)
        .ok()?;
    serde_json::from_slice(&plain).ok()
}

fn adopt(mut user_info: Value, machine_id: String) -> Result<Value, Value> {
    normalize_expiry(&mut user_info);
    if user_info
        .get("refresh_strategy")
        .or_else(|| user_info.get("refreshStrategy"))
        .and_then(Value::as_str)
        == Some("pat")
    {
        // The official app refreshes PAT sessions by rereading QODER_PAT, which
        // is deliberately not copied out of another process's environment.
        return Ok(json!({
            "ok": false,
            "error": "qwenwork_local_pat_requires_sign_in"
        }));
    }
    if access_token_of(&user_info).is_empty() {
        return Ok(json!({
            "ok": false,
            "error": "qwenwork_client_credentials_have_no_token"
        }));
    }
    if first_string(&user_info, &["uid", "id"]).is_none() {
        return Ok(json!({
            "ok": false,
            "error": "qwenwork_client_credentials_invalid"
        }));
    }
    if let Err(error) = regenerate_runtime_fields(&mut user_info) {
        return Ok(json!({ "ok": false, "error": error }));
    }
    if let Err(error) = write_store(&machine_id, &user_info) {
        return Ok(json!({ "ok": false, "error": error }));
    }
    super::catalog::forget_context();
    Ok(json!({ "ok": true, "account": account_label(&user_info) }))
}

struct PendingFlow {
    verifier: String,
    challenge: String,
    nonce: String,
    machine_id: String,
    expires_at: i64,
}

fn pending_flows() -> &'static Mutex<std::collections::HashMap<String, PendingFlow>> {
    static PENDING: OnceLock<Mutex<std::collections::HashMap<String, PendingFlow>>> =
        OnceLock::new();
    PENDING.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

fn random_verifier() -> String {
    super::super::qoder::random_bytes(64)
        .into_iter()
        .map(|byte| VERIFIER_ALPHABET[byte as usize % VERIFIER_ALPHABET.len()] as char)
        .collect()
}

fn random_id() -> String {
    super::super::qoder::uuid_v4()
}

pub async fn start() -> Result<Value, Value> {
    let verifier = random_verifier();
    let challenge = {
        use base64::Engine as _;
        use sha2::{Digest, Sha256};
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
    };
    let nonce = random_id();
    let machine_id = credential().map(|(id, _)| id).unwrap_or_else(random_id);
    let flow_id = random_id();
    // The browser entry accepts only device-flow parameters; API security state
    // is rejected here and remains attached only to the API operations below.
    let auth_url = format!(
        "{AUTH_BASE}/device/selectAccounts?challenge={challenge}&challenge_method=S256&nonce={nonce}&machine_id={machine_id}&client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}"
    );
    let expires_at = now_ms() + FLOW_TIMEOUT_MS;
    pending_flows()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(
            flow_id.clone(),
            PendingFlow {
                verifier,
                challenge,
                nonce,
                machine_id,
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

pub async fn poll(payload: &Value) -> Result<Value, Value> {
    let flow_id = payload
        .get("flowId")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if flow_id.is_empty() {
        return Ok(json!({ "ok": false, "error": "Invalid flowId" }));
    }
    let flow = {
        let mut guard = pending_flows()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match guard.get(&flow_id) {
            None => None,
            Some(flow) if flow.expires_at <= now_ms() => {
                guard.remove(&flow_id);
                None
            }
            Some(flow) => Some((
                flow.verifier.clone(),
                flow.challenge.clone(),
                flow.nonce.clone(),
                flow.machine_id.clone(),
            )),
        }
    };
    let Some((verifier, challenge, nonce, machine_id)) = flow else {
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
    let url = format!("{OPENAPI_BASE}/api/v1/deviceToken/poll");
    let response = with_client_headers(client.get(&url))
        .query(&[
            ("nonce", nonce.as_str()),
            ("verifier", verifier.as_str()),
            ("challenge_method", "S256"),
        ])
        .send()
        .await;
    let response = match response {
        Ok(response) => response,
        Err(error) => return Ok(json!({ "ok": false, "error": error.to_string() })),
    };
    let status_code = response.status().as_u16();
    if status_code == 404 {
        return Ok(json!({ "ok": false, "pending": true }));
    }
    let body = response.text().await.unwrap_or_default();
    if !(200..300).contains(&status_code) {
        let detail: String = body.chars().take(200).collect();
        return Ok(json!({
            "ok": false,
            "error": format!("device token poll {status_code}: {detail}"),
        }));
    }
    let parsed: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    let data = parsed.get("data").unwrap_or(&parsed);
    let token = first_string(data, &["device_token", "token", "access_token"]).unwrap_or_default();
    if token.is_empty() {
        return Ok(json!({ "ok": false, "pending": true }));
    }
    let binding_matches = data.get("nonce").and_then(Value::as_str) == Some(nonce.as_str())
        && data
            .get("code_challenge")
            .or_else(|| data.get("challenge"))
            .and_then(Value::as_str)
            == Some(challenge.as_str())
        && data
            .get("code_challenge_method")
            .or_else(|| data.get("challenge_method"))
            .and_then(Value::as_str)
            == Some("S256");
    if !binding_matches {
        return Ok(
            json!({ "ok": false, "error": "Device token response did not match this login flow" }),
        );
    }
    pending_flows()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(&flow_id);
    match complete_login(&client, &machine_id, data, &token).await {
        Ok(account) => Ok(json!({ "ok": true, "account": account })),
        Err(error) => Ok(json!({ "ok": false, "error": error })),
    }
}

async fn complete_login(
    client: &reqwest::Client,
    machine_id: &str,
    token_payload: &Value,
    token: &str,
) -> Result<String, String> {
    let mut fields = Map::new();
    fields.insert("security_oauth_token".into(), json!(token));
    fields.insert("access_token".into(), json!(token));
    if let Some(refresh) = first_string(token_payload, &["refresh_token"]) {
        fields.insert("refresh_token".into(), json!(refresh));
    }
    if let Some(expire) = expiry_seconds(token_payload, "expires_at", "expires_in") {
        fields.insert("expire_time".into(), json!(expire));
    }
    fields.insert("login_method".into(), json!("browser"));
    fields.insert("login_timestamp".into(), json!(now_seconds()));
    let mut user_info = Value::Object(fields);
    let mut fetched = fetch_user_info(client, token).await?;
    enrich_runtime_metadata(client, token, &mut fetched).await;
    merge_user_info(&mut user_info, fetched);
    regenerate_runtime_fields(&mut user_info)?;
    write_store(machine_id, &user_info)?;
    super::catalog::forget_context();
    Ok(account_label(&user_info))
}

fn epoch_seconds(value: &Value) -> Option<i64> {
    if let Some(text) = value.as_str() {
        if let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(text) {
            return Some(parsed.timestamp());
        }
    }
    let number = value
        .as_i64()
        .or_else(|| value.as_str()?.parse::<i64>().ok())?;
    Some(if number > 100_000_000_000 {
        number / 1000
    } else {
        number
    })
}

fn normalize_expiry(user_info: &mut Value) {
    if let Some(seconds) = user_info.get("expire_time").and_then(epoch_seconds) {
        user_info["expire_time"] = json!(seconds);
    }
}

fn expiry_seconds(payload: &Value, absolute: &str, relative: &str) -> Option<i64> {
    if let Some(seconds) = payload.get(absolute).and_then(epoch_seconds) {
        return Some(seconds);
    }
    payload
        .get(relative)
        .and_then(Value::as_i64)
        .map(|seconds| now_seconds().saturating_add(seconds))
}

fn login_rejected(status: u16, body: &str) -> bool {
    if status == 401 {
        return true;
    }
    if status != 403 {
        return false;
    }
    let parsed: Value = serde_json::from_str(body).unwrap_or(Value::Null);
    ["code", "errorCode"].into_iter().any(|key| {
        parsed.get(key).is_some_and(|code| {
            code.as_i64() == Some(105) || matches!(code.as_str(), Some("105" | "INVALID_TOKEN"))
        })
    })
}

async fn fetch_authenticated_json(
    client: &reqwest::Client,
    token: &str,
    path: &str,
    operation: &str,
) -> Result<Value, String> {
    let url = format!("{}{path}", openapi_base());
    let response = with_client_headers(client.get(&url))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .map_err(|error| error.to_string())?;
    let status_code = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    if login_rejected(status_code, &body) {
        return Err("qwenwork_login_expired".into());
    }
    if !(200..300).contains(&status_code) {
        return Err(format!(
            "QwenWork {operation} request failed (HTTP {status_code})"
        ));
    }
    serde_json::from_str(&body).map_err(|_| format!("{operation} returned invalid JSON"))
}

async fn fetch_account_context(client: &reqwest::Client, token: &str) -> Result<Value, String> {
    fetch_authenticated_json(
        client,
        token,
        &with_login_security_state(ACCOUNT_CONTEXT_PATH),
        "account-context",
    )
    .await
}

fn normalize_account_context(response: &Value) -> Result<Value, String> {
    let context = response
        .get("data")
        .unwrap_or(response)
        .as_object()
        .ok_or_else(|| "account context is not an object".to_string())?;
    let mut normalized = context.clone();
    if let Some(user) = context.get("user").and_then(Value::as_object) {
        normalized.extend(user.clone());
        // The official client lets the endpoint-level plan, quota and page win
        // over identically named user fields after flattening `user`.
        for key in ["plan", "quota", "page"] {
            if let Some(value) = context.get(key) {
                normalized.insert(key.into(), value.clone());
            }
        }
    }
    Ok(Value::Object(normalized))
}

fn user_info_from_account_context(response: &Value) -> Result<Value, String> {
    let normalized = normalize_account_context(response)?;
    let uid = first_string(&normalized, &["id", "user_id", "userId", "uid"])
        .ok_or_else(|| "account context did not include id".to_string())?;
    let mut out = Map::new();
    out.insert("uid".into(), json!(uid));
    if let Some(name) = first_string(
        &normalized,
        &["name", "display_name", "nick_name", "username", "user_name"],
    ) {
        out.insert("name".into(), json!(name));
    }
    if let Some(email) = first_string(&normalized, &["email"]) {
        out.insert("email".into(), json!(email));
    }
    let organization = normalized.get("organization").unwrap_or(&Value::Null);
    if let Some(org) = first_string(&normalized, &["organization_id", "organizationId", "orgId"])
        .or_else(|| first_string(organization, &["org_id", "orgId", "id"]))
    {
        out.insert("organization_id".into(), json!(org));
    }
    if let Some(team) = first_string(&normalized, &["team_id", "teamId"])
        .or_else(|| first_string(organization, &["team_id", "teamId"]))
    {
        out.insert("team_id".into(), json!(team));
    }
    if let Some(org_type) = first_string(&normalized, &["org_type", "orgType"])
        .or_else(|| first_string(organization, &["org_type", "orgType", "type"]))
    {
        out.insert("org_type".into(), json!(org_type));
    }
    if let Some(tags) = first_string_array(
        &normalized,
        &[
            "organization_tags",
            "organizationTags",
            "org_tags",
            "orgTags",
        ],
    ) {
        out.insert("organization_tags".into(), tags);
    }
    let is_business = normalized
        .get("is_biz")
        .or_else(|| normalized.get("isBiz"))
        .and_then(Value::as_bool);
    if let Some(is_business) = is_business {
        out.insert("is_biz".into(), json!(is_business));
    }
    if is_business != Some(true) {
        let mode = normalized
            .get("data_sharing")
            .or_else(|| normalized.get("dataSharing"))
            .and_then(|value| value.get("mode"))
            .and_then(Value::as_str);
        match mode {
            Some("shared") => {
                out.insert("data_policy_agreed".into(), json!(true));
            }
            Some("private") => {
                out.insert("data_policy_agreed".into(), json!(false));
            }
            _ => {}
        }
    }
    if let Some(plan) = normalized.get("plan") {
        if let Some(plan_id) = first_string(plan, &["pid", "id"]) {
            out.insert("plan_id".into(), json!(plan_id));
        }
        if let Some(plan_name) = first_string(plan, &["name"]) {
            out.insert("plan_name".into(), json!(plan_name));
        }
    }
    Ok(Value::Object(out))
}

async fn fetch_user_info(client: &reqwest::Client, token: &str) -> Result<Value, String> {
    let context = fetch_account_context(client, token).await?;
    user_info_from_account_context(&context)
}

async fn fetch_organization_tags(
    client: &reqwest::Client,
    token: &str,
    organization_id: &str,
) -> Result<Value, String> {
    let path = format!(
        "/api/v1/organizations/{}/tags",
        urlencoding::encode(organization_id)
    );
    let response = fetch_authenticated_json(
        client,
        token,
        &with_login_security_state(&path),
        "organization tags",
    )
    .await?;
    let source = response.get("data").unwrap_or(&response);
    Ok(Value::Array(
        source
            .get("tags")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(|value| Value::String(value.to_string()))
            .collect(),
    ))
}

fn enterprise_data_policy(response: &Value) -> Option<bool> {
    let source = response.get("data").unwrap_or(response);
    let mode = source
        .get("data-sharing-mode")
        .and_then(|value| value.get("mode"))
        .and_then(Value::as_str)?;
    match mode {
        "shared" => Some(true),
        "private" => Some(false),
        _ => None,
    }
}

async fn enrich_runtime_metadata(client: &reqwest::Client, token: &str, user_info: &mut Value) {
    let organization_id = first_string(user_info, &["organization_id"]);
    let tags_missing = user_info
        .get("organization_tags")
        .and_then(Value::as_array)
        .is_none_or(Vec::is_empty);
    if tags_missing {
        if let Some(organization_id) = organization_id.as_deref() {
            if let Ok(tags) = fetch_organization_tags(client, token, organization_id).await {
                if let Some(target) = user_info.as_object_mut() {
                    target.insert("organization_tags".into(), tags);
                }
            }
        }
    }
    if user_info.get("is_biz").and_then(Value::as_bool) == Some(true) {
        if let Ok(response) = fetch_authenticated_json(
            client,
            token,
            ENTERPRISE_SETTINGS_PATH,
            "enterprise settings",
        )
        .await
        {
            if let Some(agreed) = enterprise_data_policy(&response) {
                if let Some(target) = user_info.as_object_mut() {
                    target.insert("data_policy_agreed".into(), json!(agreed));
                }
            }
        }
    }
}

fn finite_number(source: &Value, keys: &[&str]) -> Option<f64> {
    keys.iter()
        .find_map(|key| source.get(*key).and_then(Value::as_f64))
        .filter(|value| value.is_finite())
}

fn normalized_credit(source: &Value, organization: bool) -> Option<Value> {
    let source = source.as_object().map(|_| source)?;
    let total_keys: &[&str] = if organization {
        &["cap", "total"]
    } else {
        &["total"]
    };
    let total = finite_number(source, total_keys).unwrap_or_default();
    let used = finite_number(source, &["used"]).unwrap_or_default();
    let remaining = finite_number(source, &["remaining"]).unwrap_or((total - used).max(0.0));
    let unit = first_string(source, &["unit"]).unwrap_or_else(|| "credits".into());
    let mut normalized = Map::from_iter([
        ("used".into(), json!(used)),
        ("remaining".into(), json!(remaining)),
        ("unit".into(), json!(unit)),
    ]);
    if organization {
        normalized.insert("cap".into(), json!(total));
        let available = source
            .get("available")
            .and_then(Value::as_bool)
            .unwrap_or(total > 0.0);
        normalized.insert("available".into(), json!(available));
    } else {
        normalized.insert("total".into(), json!(total));
    }
    Some(Value::Object(normalized))
}

fn quota_from_account_context(response: &Value) -> Result<Value, String> {
    let normalized = normalize_account_context(response)?;
    let quota = normalized
        .get("quota")
        .and_then(Value::as_object)
        .ok_or_else(|| "account context did not include quota".to_string())?;
    let quota_value = Value::Object(quota.clone());
    let user_quota = quota
        .get("user_quota")
        .or_else(|| quota.get("userQuota"))
        .filter(|value| !value.is_null())
        .unwrap_or(&quota_value);
    let mut out = Map::new();
    let is_business = normalized
        .get("is_biz")
        .or_else(|| normalized.get("isBiz"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    out.insert(
        "userType".into(),
        json!(
            first_string(&quota_value, &["user_type", "userType"]).unwrap_or_else(|| {
                if is_business {
                    "enterprise".into()
                } else {
                    "personal".into()
                }
            })
        ),
    );
    out.insert(
        "userQuota".into(),
        normalized_credit(user_quota, false)
            .ok_or_else(|| "account context user quota is not an object".to_string())?,
    );
    if let Some(organization) = quota
        .get("org_resource_package")
        .or_else(|| quota.get("orgResourcePackage"))
        .or_else(|| quota.get("shared_quota"))
        .or_else(|| quota.get("sharedQuota"))
        .and_then(|value| normalized_credit(value, true))
    {
        out.insert("orgResourcePackage".into(), organization);
    }
    if let Some(expires_at) = quota.get("expires_at").or_else(|| quota.get("expiresAt")) {
        out.insert("expiresAt".into(), expires_at.clone());
    }
    Ok(Value::Object(out))
}

fn quota_from_enterprise_response(response: &Value) -> Result<Value, String> {
    let data = response
        .get("data")
        .unwrap_or(response)
        .as_object()
        .ok_or_else(|| "enterprise quota is not an object".to_string())?;
    let data_value = Value::Object(data.clone());
    let empty_quota = json!({});
    let user_quota = data
        .get("user_quota")
        .or_else(|| data.get("userQuota"))
        .filter(|value| !value.is_null())
        .unwrap_or(&empty_quota);
    let mut out = Map::new();
    out.insert("userType".into(), json!("enterprise"));
    out.insert(
        "userQuota".into(),
        normalized_credit(user_quota, false)
            .ok_or_else(|| "enterprise user quota is not an object".to_string())?,
    );
    if let Some(organization) = data
        .get("org_resource_package")
        .or_else(|| data.get("orgResourcePackage"))
        .or_else(|| data.get("shared_quota"))
        .or_else(|| data.get("sharedQuota"))
        .and_then(|value| normalized_credit(value, true))
    {
        out.insert("orgResourcePackage".into(), organization);
    }
    if let Some(expires_at) = data_value
        .get("expires_at")
        .or_else(|| data_value.get("expiresAt"))
    {
        out.insert("expiresAt".into(), expires_at.clone());
    }
    Ok(Value::Object(out))
}

fn merge_user_info(target: &mut Value, extra: Value) {
    let (Some(target), Some(extra)) = (target.as_object_mut(), extra.as_object()) else {
        return;
    };
    for (key, value) in extra {
        target.insert(key.clone(), value.clone());
    }
}

fn regenerate_runtime_fields(user_info: &mut Value) -> Result<(), String> {
    let runtime = generate_runtime_auth_with_profile(user_info, CosyProfile::QWENWORK)?;
    let Some(target) = user_info.as_object_mut() else {
        return Err("profile is not an object".into());
    };
    target.insert("encrypt_user_info".into(), json!(runtime.encrypt_user_info));
    target.insert("key".into(), json!(runtime.key));
    Ok(())
}

pub(crate) async fn ensure_fresh_token() -> Result<(), String> {
    let _refresh = refresh_gate().lock().await;
    let Some((_, user_info)) = credential() else {
        return Ok(());
    };
    let Some(expire_time) = user_info.get("expire_time").and_then(Value::as_i64) else {
        return Ok(());
    };
    if expire_time.saturating_sub(REFRESH_SKEW_SECONDS) > now_seconds() {
        return Ok(());
    }
    refresh_token_locked().await
}

/// Exchange the stored refresh token unconditionally.
///
/// `expire_time` only bounds the session; the gateway can invalidate the access
/// token early (a re-login on the official client rotates it), and then every
/// signed call answers 403 code 105 long before the stored expiry. Callers that
/// observe such a rejection refresh through this path instead of waiting out
/// the skew window.
pub async fn refresh_token_now() -> Result<(), String> {
    let _refresh = refresh_gate().lock().await;
    refresh_token_locked().await
}

async fn refresh_token_locked() -> Result<(), String> {
    refresh_token_inner().await.map_err(|error| {
        if error == "qwenwork_credential_changed" || error == "qwenwork_credential_write_failed" {
            error
        } else {
            "qwenwork_refresh_failed".into()
        }
    })
}

async fn refresh_token_inner() -> Result<(), String> {
    let Some((machine_id, mut user_info)) = credential() else {
        return Ok(());
    };
    let previous = user_info.clone();
    if first_string(&user_info, &["refresh_strategy", "refreshStrategy"]).as_deref() == Some("pat")
    {
        return Err(
            "QwenWork PAT login cannot be refreshed without the original QODER_PAT; sign in again"
                .into(),
        );
    }
    let refresh_token = first_string(&user_info, &["refresh_token"]).ok_or_else(|| {
        "QwenWork subscription has expired and carries no refresh token".to_string()
    })?;
    let client = client()?;
    let url = format!(
        "{}{}",
        openapi_base(),
        with_login_security_state("/api/v1/deviceToken/refresh")
    );
    let business_team = (user_info.get("is_biz").and_then(Value::as_bool) == Some(true))
        .then(|| first_string(&user_info, &["team_id", "teamId"]))
        .flatten();
    let target = if business_team.is_some() { "biz" } else { "c" };
    let client_id = first_string(&user_info, &["oauth_client_id", "oauthClientId"])
        .unwrap_or_else(|| CLIENT_ID.into());
    let redirect_uri = first_string(&user_info, &["oauth_redirect_uri", "oauthRedirectUri"])
        .unwrap_or_else(|| REDIRECT_URI.into());
    let mut refresh_body = Map::from_iter([
        ("refresh_token".into(), json!(refresh_token)),
        ("target".into(), json!(target)),
        ("client_id".into(), json!(client_id)),
        ("redirect_uri".into(), json!(redirect_uri)),
    ]);
    if let Some(team_id) = business_team {
        refresh_body.insert("team_id".into(), json!(team_id));
    }
    let response = with_client_headers(client.post(&url))
        .header("Content-Type", "application/json")
        .body(Value::Object(refresh_body).to_string())
        .send()
        .await
        .map_err(|error| error.to_string())?;
    let status_code = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    if !(200..300).contains(&status_code) {
        return Err(format!("token refresh failed (HTTP {status_code})"));
    }
    let parsed: Value = serde_json::from_str(&body)
        .map_err(|_| "token refresh returned invalid JSON".to_string())?;
    let data = parsed.get("data").unwrap_or(&parsed);
    let token = first_string(data, &["device_token", "token", "access_token"])
        .ok_or_else(|| "token refresh returned no token".to_string())?;
    {
        let Some(target) = user_info.as_object_mut() else {
            return Err("profile is not an object".into());
        };
        target.insert("security_oauth_token".into(), json!(token));
        target.insert("access_token".into(), json!(token));
        if let Some(refresh) = first_string(data, &["refresh_token"]) {
            target.insert("refresh_token".into(), json!(refresh));
        }
        if let Some(expire) = expiry_seconds(data, "expires_at", "expires_in") {
            target.insert("expire_time".into(), json!(expire));
        }
    }
    regenerate_runtime_fields(&mut user_info)?;
    commit_refresh(&machine_id, &previous, &user_info)?;
    super::catalog::forget_context();
    Ok(())
}

pub async fn usage() -> Result<Value, String> {
    if super::catalog::access_token().is_err() {
        return Ok(json!({ "signedIn": false }));
    }
    let before_refresh = credential().ok_or("qwenwork_credential_changed")?;
    ensure_fresh_token().await?;
    let snapshot = credential().ok_or("qwenwork_credential_changed")?;
    let client = client()?;
    match fetch_usage(&client).await {
        Err(error) if error == "qwenwork_login_expired" => {
            if access_token_of(&before_refresh.1) != access_token_of(&snapshot.1) {
                return Err(error);
            }
            let _refresh = refresh_gate().lock().await;
            let current = credential().ok_or("qwenwork_credential_changed")?;
            if current.0 != snapshot.0 || current.1.get("uid") != snapshot.1.get("uid") {
                return Err("qwenwork_credential_changed".into());
            }
            if access_token_of(&current.1) == access_token_of(&snapshot.1) {
                refresh_token_locked().await?;
            }
            drop(_refresh);
            fetch_usage(&client).await
        }
        result => result,
    }
}

async fn fetch_usage(client: &reqwest::Client) -> Result<Value, String> {
    let token = super::catalog::access_token().map_err(|error| error.message)?;
    let is_business = credential()
        .and_then(|(_, profile)| profile.get("is_biz").and_then(Value::as_bool))
        == Some(true);
    let quota = if is_business {
        let response = fetch_authenticated_json(
            client,
            &token.access_token,
            ENTERPRISE_QUOTA_PATH,
            "quota usage",
        )
        .await?;
        quota_from_enterprise_response(&response)?
    } else {
        let context = fetch_account_context(client, &token.access_token).await?;
        quota_from_account_context(&context)?
    };
    Ok(json!({ "signedIn": true, "account": token.account, "quota": quota }))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct QwenWorkHomeGuard {
        previous: Option<String>,
    }

    impl QwenWorkHomeGuard {
        fn set(path: &Path) -> Self {
            let previous = std::env::var("QWENWORK_HOME").ok();
            klaus_agent_core::runtime_process::set_process_environment_var(
                "QWENWORK_HOME",
                path.as_os_str(),
            );
            Self { previous }
        }
    }

    impl Drop for QwenWorkHomeGuard {
        fn drop(&mut self) {
            match &self.previous {
                Some(value) => klaus_agent_core::runtime_process::set_process_environment_var(
                    "QWENWORK_HOME",
                    value,
                ),
                None => klaus_agent_core::runtime_process::remove_process_environment_var(
                    "QWENWORK_HOME",
                ),
            }
        }
    }

    #[tokio::test]
    async fn stale_refresh_cannot_restore_logout_or_replace_another_account() {
        let _guard = super::super::test_env_lock().lock().await;
        let root = tempfile::tempdir().unwrap();
        super::super::set_config_root(root.path());
        let first = json!({"access_token":"first-owned-token"});
        let refreshed = json!({"access_token":"refreshed-owned-token"});
        write_store("owned-device", &first).unwrap();
        commit_refresh("owned-device", &first, &refreshed).unwrap();
        let second = json!({"access_token":"second-owned-token"});
        write_store("other-device", &second).unwrap();
        assert_eq!(
            commit_refresh("owned-device", &refreshed, &first).unwrap_err(),
            "qwenwork_credential_changed"
        );
        assert!(
            credential().is_some_and(|(device, user)| device == "other-device" && user == second)
        );
        logout().unwrap();
        assert_eq!(
            commit_refresh("other-device", &second, &refreshed).unwrap_err(),
            "qwenwork_credential_changed"
        );
        assert!(credential().is_none());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            write_store("owned-device", &first).unwrap();
            assert_eq!(
                std::fs::metadata(store_path())
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn verifier_uses_only_the_unreserved_alphabet() {
        let verifier = random_verifier();
        assert_eq!(verifier.len(), 64);
        assert!(verifier
            .bytes()
            .all(|byte| VERIFIER_ALPHABET.contains(&byte)));
    }

    #[test]
    fn account_context_flattens_the_official_nested_user_shape() {
        let context = json!({
            "data": {
                "user": {
                    "id": "user-42",
                    "name": "Ada",
                    "email": "ada@example.com",
                    "is_biz": true,
                    "organization": {
                        "org_id": "org-7",
                        "team_id": "team-3",
                        "org_type": "enterprise"
                    },
                    "organization_tags": ["tag-a", 7]
                },
                "plan": { "pid": "qwen-office-enterprise", "name": "Pro" },
                "quota": { "remaining": 9 }
            }
        });

        assert_eq!(
            user_info_from_account_context(&context).expect("profile"),
            json!({
                "uid": "user-42",
                "name": "Ada",
                "email": "ada@example.com",
                "organization_id": "org-7",
                "team_id": "team-3",
                "org_type": "enterprise",
                "organization_tags": ["tag-a"],
                "is_biz": true,
                "plan_id": "qwen-office-enterprise",
                "plan_name": "Pro"
            })
        );
    }

    #[test]
    fn personal_data_policy_comes_only_from_the_account_context_mode() {
        let shared = json!({
            "data": {
                "user": { "id": "personal-1", "is_biz": false },
                "data_sharing": { "mode": "shared" }
            }
        });
        assert_eq!(
            user_info_from_account_context(&shared).expect("shared profile")["data_policy_agreed"],
            json!(true)
        );

        let unknown = json!({
            "data": {
                "user": { "id": "personal-1", "is_biz": false },
                "data_sharing": { "mode": "unknown" }
            }
        });
        assert!(user_info_from_account_context(&unknown)
            .expect("unknown profile")
            .get("data_policy_agreed")
            .is_none());
    }

    #[test]
    fn enterprise_data_policy_uses_the_team_managed_module() {
        assert_eq!(
            enterprise_data_policy(&json!({
                "data": { "data-sharing-mode": { "mode": "shared" } }
            })),
            Some(true)
        );
        assert_eq!(
            enterprise_data_policy(&json!({
                "data-sharing-mode": { "mode": "private" }
            })),
            Some(false)
        );
        assert_eq!(
            enterprise_data_policy(&json!({
                "data-sharing-mode": { "mode": "managed" }
            })),
            None
        );
    }

    #[tokio::test]
    async fn device_login_opens_the_gateway_and_preserves_the_pending_flow() {
        use base64::Engine as _;
        use sha2::{Digest, Sha256};

        let _lock = super::super::test_env_lock().lock().await;
        let fixture = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(fixture.path());
        let result = start().await.expect("login start");
        assert_eq!(result["ok"], true);
        assert_eq!(result["interval"], 1);
        let url = url::Url::parse(result["authUrl"].as_str().expect("auth URL"))
            .expect("absolute auth URL");
        assert_eq!(
            url.origin().ascii_serialization(),
            "https://gateway.qwenwork.cn"
        );
        assert_eq!(url.path(), "/device/selectAccounts");
        assert!(url.fragment().is_none());
        let query: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(query.len(), 6);
        assert_eq!(query["challenge_method"], "S256");
        assert_eq!(query["client_id"], "e883ade2-e6e3-4d6d-adf7-f92ceff5fdcb");
        assert_eq!(query["redirect_uri"], "qwenwork-cn://");
        assert!(
            !query.contains_key("state"),
            "API state is not a browser parameter"
        );

        let flow = pending_flows()
            .lock()
            .expect("pending flows")
            .remove(result["flowId"].as_str().expect("flow ID"))
            .expect("registered flow");
        assert_eq!(query["nonce"], flow.nonce);
        assert_eq!(query["machine_id"], flow.machine_id);
        assert_eq!(query["challenge"], flow.challenge);
        assert_eq!(
            flow.challenge,
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(Sha256::digest(flow.verifier.as_bytes()))
        );
        assert_eq!(result["expiresAt"], flow.expires_at);
        assert!(flow.expires_at > now_ms());
        assert!(
            !store_path().exists(),
            "starting login must not write credentials"
        );
    }

    #[test]
    fn login_security_state_is_base64url_json_in_the_query() {
        use base64::Engine as _;
        let path = with_login_security_state("/api/v1/example?include=user");
        let encoded = path.split("&state=").nth(1).expect("state query");
        assert!(!encoded.contains('='));
        let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(encoded)
            .expect("base64url state");
        let state: Value = serde_json::from_slice(&decoded).expect("state json");
        assert_eq!(state["v"], json!(1));
        assert!(state.get("umid_token").is_none());
        assert!(state.get("x_mini_wua").is_none());
    }

    #[test]
    fn account_context_quota_is_unwrapped_and_normalized_for_the_host() {
        let context = json!({
            "data": {
                "user": { "id": "user-42", "is_biz": true },
                "quota": {
                    "user_type": "enterprise",
                    "expires_at": 1_800_000_000,
                    "user_quota": { "total": 100, "used": 25, "remaining": 75 },
                    "org_resource_package": {
                        "cap": 500,
                        "used": 125,
                        "remaining": 375,
                        "available": true,
                        "unit": "credits"
                    }
                }
            }
        });

        assert_eq!(
            quota_from_account_context(&context).expect("quota"),
            json!({
                "userType": "enterprise",
                "expiresAt": 1_800_000_000,
                "userQuota": { "total": 100.0, "used": 25.0, "remaining": 75.0, "unit": "credits" },
                "orgResourcePackage": { "cap": 500.0, "used": 125.0, "remaining": 375.0, "available": true, "unit": "credits" }
            })
        );
    }

    #[test]
    fn enterprise_quota_is_normalized_from_its_dedicated_endpoint() {
        let response = json!({
            "data": {
                "user_id": "user-42",
                "expires_at": 1_800_000_000,
                "user_quota": { "total": 20, "used": 4, "remaining": 16 },
                "org_resource_package": { "total": 80, "used": 10, "remaining": 70 }
            }
        });

        assert_eq!(
            quota_from_enterprise_response(&response).expect("enterprise quota"),
            json!({
                "userType": "enterprise",
                "expiresAt": 1_800_000_000,
                "userQuota": { "total": 20.0, "used": 4.0, "remaining": 16.0, "unit": "credits" },
                "orgResourcePackage": { "cap": 80.0, "used": 10.0, "remaining": 70.0, "available": true, "unit": "credits" }
            })
        );
    }

    #[tokio::test]
    async fn absent_credential_is_a_signed_out_answer() {
        let _lock = super::super::test_env_lock().lock().await;
        let fixture = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(fixture.path());
        super::super::catalog::forget_context();
        assert_eq!(status(), json!({ "loggedIn": false }));
        assert_eq!(
            usage().await.expect("signed out"),
            json!({ "signedIn": false })
        );
    }

    #[tokio::test]
    async fn imports_the_official_cli_store_without_mutating_it() {
        use aes::cipher::{block_padding::Pkcs7, BlockEncryptMut, KeyIvInit};
        use base64::Engine as _;

        let _lock = super::super::test_env_lock().lock().await;
        let fixture = tempfile::tempdir().expect("fixture");
        let config = fixture.path().join("config");
        let home = fixture.path().join("qwenwork-home");
        let auth = home.join(".auth");
        std::fs::create_dir_all(&config).expect("config");
        std::fs::create_dir_all(&auth).expect("auth");
        let machine_id = "01234567-89ab-cdef-0123-456789abcdef";
        let key = &machine_id.as_bytes()[..16];
        let profile = json!({
            "uid": "fixture-user",
            "name": "Fixture User",
            "security_oauth_token": "fake-qwenwork-token",
            "access_token": "fake-qwenwork-token",
            "refresh_token": "fake-refresh",
            "expire_time": 1_900_000_000_i64,
            "organization_id": "fixture-org",
            "organization_tags": ["enterprise"],
            "data_policy_agreed": true,
            "refresh_strategy": "device"
        });
        let encrypted = cbc::Encryptor::<aes::Aes128>::new(key.into(), key.into())
            .encrypt_padded_vec_mut::<Pkcs7>(profile.to_string().as_bytes());
        std::fs::write(auth.join("id"), machine_id).expect("machine id");
        std::fs::write(
            auth.join("user"),
            base64::engine::general_purpose::STANDARD.encode(encrypted),
        )
        .expect("profile");
        let before = std::fs::read(auth.join("user")).expect("before");
        super::super::set_config_root(&config);
        let _home = QwenWorkHomeGuard::set(&home);

        let imported = import_local().expect("import response");
        assert_eq!(imported["ok"], true, "{imported}");
        let (stored_machine, stored_profile) = credential().expect("stored credential");
        assert_eq!(stored_machine, machine_id);
        assert_eq!(stored_profile["uid"], "fixture-user");
        assert!(stored_profile["encrypt_user_info"].is_string());
        assert!(stored_profile["key"].is_string());
        assert_eq!(std::fs::read(auth.join("user")).expect("after"), before);
        logout().expect("logout");
    }

    #[tokio::test]
    async fn pat_safe_storage_adoption_is_explicitly_rejected() {
        let _lock = super::super::test_env_lock().lock().await;
        let fixture = tempfile::tempdir().expect("fixture");
        super::super::set_config_root(fixture.path());
        let result = adopt_credential(
            json!({
                "uid": "fixture-user",
                "security_oauth_token": "fake-token",
                "refresh_strategy": "pat"
            }),
            Some("01234567-89ab-cdef-0123-456789abcdef".into()),
        )
        .expect("adoption response");
        assert_eq!(
            result,
            json!({"ok": false, "error": "qwenwork_local_pat_requires_sign_in"})
        );
        assert!(credential().is_none());
    }
}
