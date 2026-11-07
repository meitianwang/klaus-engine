//! Qoder subscription login, owned by Klaus.
//!
//! Klaus keeps its OWN Qoder credential (`<config>/qoder-subscription-oauth.json`)
//! instead of reading and rewriting the Qoder client's store. That is what makes
//! the external subscription sources behave alike: sign in, adopt the machine's
//! existing login, sign out — and signing out here never signs the user out of
//! their Qoder IDE or qodercli.
//!
//! The login is Qoder's own device flow: a browser page authorizes a PKCE
//! challenge while this host polls for the token. Adopting an existing login
//! reads qodercli's store here, and takes the Qoder IDE's from a host that can
//! open it ([`adopt_credential`]); signing is native ([`super::sign`]), so none of
//! this needs a Qoder CLI installed.

use super::sign::generate_runtime_auth;
use serde_json::{json, Map, Value};
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

/// The Qoder device-flow constants. The client id is a public
/// identifier (there is no client secret in this flow), and the poll endpoint
/// answers 404 — not `authorization_pending` — while the browser leg is unfinished.
const PORTAL_BASE: &str = "https://qoder.com";
const OPENAPI_BASE: &str = "https://openapi.qoder.sh";
const CLIENT_ID: &str = "e883ade2-e6e3-4d6d-adf7-f92ceff5fdcb";
const FLOW_TIMEOUT_MS: i64 = 300_000;
/// Poll cadence handed to the renderer, in seconds.
const POLL_INTERVAL_SECONDS: i64 = 1;
/// Refresh once the access token is within an hour of expiry, like the client.
const REFRESH_SKEW_SECONDS: i64 = 3600;
const HTTP_TIMEOUT_MS: u64 = 15_000;

/// The verifier alphabet the client draws from (RFC 7636 unreserved set).
const VERIFIER_ALPHABET: &[u8] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~";

/// `Cosy-Version` on the OpenAPI calls. Klaus reports its own client version
/// rather than reading a qodercli `version.txt` that may not exist.
const COSY_VERSION: &str = "1.1.5";

fn store_path() -> PathBuf {
    super::config_root().join("qoder-subscription-oauth.json")
}

/// qodercli's own credential directory, still read when adopting a CLI login.
fn qodercli_auth_dir() -> PathBuf {
    let home = match std::env::var("QODER_HOME") {
        Ok(value) if !value.trim().is_empty() => PathBuf::from(value.trim()),
        _ => PathBuf::from(std::env::var("HOME").unwrap_or_default()).join(".qoder"),
    };
    home.join(".auth")
}

/// `Cosy-MachineOS`: the client reports `<arch>_<os>` (e.g. `aarch64_darwin`).
fn machine_os() -> String {
    // Only the OS name needs translating: Rust says `macos` where the client says
    // `darwin`. The architecture names already agree, so it is reported as-is.
    let arch = std::env::consts::ARCH;
    let os = match std::env::consts::OS {
        "macos" => "darwin",
        other => other,
    };
    format!("{arch}_{os}")
}

fn client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(HTTP_TIMEOUT_MS))
        .build()
        .map_err(|error| error.to_string())
}

/// Every Qoder OpenAPI call carries the same client descriptor headers.
fn with_cosy_headers(request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    request
        .header("Accept", "application/json")
        .header("User-Agent", format!("qoder/{COSY_VERSION}"))
        .header("Cosy-Version", COSY_VERSION)
        // The desktop client type: the same value the catalog and inference
        // requests are signed with, so one account never looks like two clients.
        .header("Cosy-ClientType", super::catalog::DESKTOP_CLIENT_TYPE)
        .header("Cosy-MachineOS", machine_os())
}

fn now_seconds() -> i64 {
    chrono::Utc::now().timestamp()
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

// --------------------------------------------------------------- credential store

fn read_store() -> Option<Value> {
    let raw = std::fs::read_to_string(store_path()).ok()?;
    serde_json::from_str::<Value>(&raw).ok()
}

fn write_store(machine_id: &str, user_info: &Value) -> Result<(), String> {
    let path = store_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let body = json!({ "machineId": machine_id, "userInfo": user_info });
    let serialized = serde_json::to_string_pretty(&body).map_err(|error| error.to_string())?;
    std::fs::write(&path, serialized).map_err(|error| error.to_string())?;
    // The file holds a bearer token, so it must not be world-readable.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

/// The signing inputs [`super::catalog`] needs: the machine id the credential was issued
/// under, paired with the decrypted profile. `None` means signed out.
pub(crate) fn credential() -> Option<(String, Value)> {
    let store = read_store()?;
    let machine_id = store.get("machineId").and_then(Value::as_str)?.to_string();
    let user_info = store.get("userInfo").cloned()?;
    if machine_id.is_empty() || !user_info.is_object() {
        return None;
    }
    Some((machine_id, user_info))
}

fn account_label(user_info: &Value) -> String {
    for key in ["name", "email", "uid"] {
        match user_info.get(key) {
            Some(Value::String(text)) if !text.is_empty() => return text.clone(),
            Some(Value::Number(number)) => return number.to_string(),
            _ => {}
        }
    }
    "Qoder".into()
}

fn access_token_of(user_info: &Value) -> String {
    for key in ["security_oauth_token", "access_token"] {
        if let Some(token) = user_info.get(key).and_then(Value::as_str) {
            if !token.is_empty() {
                return token.to_string();
            }
        }
    }
    String::new()
}

/// `qoder-auth:status`. A missing store is a plain signed-out answer with no
/// error, so the settings pane paints "not signed in" rather than a failure.
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
    match user_info.get("uid")? {
        Value::String(value) if !value.is_empty() => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

/// `qoder-auth:logout`. Klaus owns this file, so signing out is removing it —
/// the Qoder client's own credential is left untouched. Qoder has no revoke
/// endpoint, so the token stays valid upstream until it expires on its own.
pub fn logout() -> Result<Value, Value> {
    super::catalog::forget_context();
    super::catalog::forget_cache();
    match std::fs::remove_file(store_path()) {
        Ok(()) => Ok(json!({ "ok": true })),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(json!({ "ok": true })),
        Err(error) => Ok(json!({ "ok": false, "error": error.to_string() })),
    }
}

/// `qoder-auth:import-local`. Adopt the Qoder login this machine already holds in
/// qodercli's own store: `~/.qoder/.auth/user`, encrypted with the first 16
/// characters of the machine id sitting beside it.
///
/// The OTHER store a machine may have — the Qoder IDE's profile in VS Code secret
/// storage — is deliberately not read here. Reaching it means opening the IDE's
/// SQLite state and undoing Electron's safeStorage, which would put a SQLite
/// dependency and a specific editor's on-disk layout inside the engine. A host
/// that can read it passes what it found to [`adopt_credential`] instead, so the
/// engine stores the same credential without knowing where it came from.
pub fn import_local() -> Result<Value, Value> {
    match import_qodercli() {
        Ok(result) => Ok(result),
        Err(error) => Ok(json!({ "ok": false, "error": error })),
    }
}

/// Store a Qoder profile a host read out of some other local store.
///
/// The machine id already registered is kept when there is one: it is what the
/// credential was issued under, and minting a fresh one on every adopt would
/// register the same install over and over.
pub fn adopt_credential(user_info: Value) -> Result<Value, Value> {
    let machine_id = credential()
        .map(|(id, _)| id)
        .unwrap_or_else(random_machine_id);
    adopt(user_info, machine_id)
}

/// Adopt qodercli's store. Its profile is AES-encrypted under the first 16
/// characters of the machine id it sits beside, and that machine id is kept with
/// the profile because the credential was issued under it.
fn import_qodercli() -> Result<Value, String> {
    let machine_id = std::fs::read_to_string(qodercli_auth_dir().join("machine_id"))
        .map(|raw| raw.trim().to_string())
        .map_err(|_| "qoder_client_credentials_not_found".to_string())?;
    let encrypted = std::fs::read_to_string(qodercli_auth_dir().join("user"))
        .map_err(|_| "qoder_client_credentials_not_found".to_string())?;
    if machine_id.is_empty() {
        return Err("qoder_client_credentials_invalid".into());
    }
    let key: String = machine_id.chars().take(16).collect();
    let user_info = decrypt_qodercli_profile(&encrypted, &key)
        .ok_or("qoder_client_credentials_invalid".to_string())?;
    match adopt(user_info, machine_id) {
        Ok(result) => Ok(result),
        Err(error) => Err(error.to_string()),
    }
}

/// qodercli's store is AES-128-CBC over the 16-character key, base64'd — the same
/// construction its own `credentialStorageDecrypt` performs.
fn decrypt_qodercli_profile(encrypted: &str, key: &str) -> Option<Value> {
    use aes::cipher::{block_padding::Pkcs7, BlockDecryptMut, KeyIvInit};
    use base64::Engine as _;

    if key.len() != 16 {
        return None;
    }
    let raw = base64::engine::general_purpose::STANDARD
        .decode(encrypted.trim())
        .ok()?;
    if raw.is_empty() || raw.len() % 16 != 0 {
        return None;
    }
    let key = key.as_bytes();
    let plain = cbc::Decryptor::<aes::Aes128>::new(key.into(), key.into())
        .decrypt_padded_vec_mut::<Pkcs7>(&raw)
        .ok()?;
    serde_json::from_slice(&plain).ok()
}

/// Seal an adopted profile into Klaus's own store. The runtime pair has to be
/// regenerated here rather than copied: the source store's pair is bound to the
/// token it was issued with, and Klaus refreshes that token independently.
fn adopt(mut user_info: Value, machine_id: String) -> Result<Value, Value> {
    if access_token_of(&user_info).is_empty() {
        return Ok(json!({ "ok": false, "error": "qoder_client_credentials_have_no_token" }));
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

// --------------------------------------------------------------- device flow

struct PendingFlow {
    verifier: String,
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
    super::random_bytes(64)
        .into_iter()
        .map(|byte| VERIFIER_ALPHABET[byte as usize % VERIFIER_ALPHABET.len()] as char)
        .collect()
}

/// A random UUID-shaped device id. The Qoder client generates its own the same
/// way (it is an identifier, not a hardware fingerprint), and Klaus keeps the one
/// its credential was issued under rather than borrowing another client's.
fn random_machine_id() -> String {
    let bytes = super::random_bytes(16);
    let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

/// `qoder-auth:start`. Build the PKCE pair, hand the browser the authorization
/// page and keep the verifier host-side so the renderer only carries a flow id.
pub async fn start() -> Result<Value, Value> {
    let verifier = random_verifier();
    let challenge = {
        use base64::Engine;
        use sha2::{Digest, Sha256};
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
    };
    let nonce = random_machine_id();
    // Reuse the device id of an existing credential so re-authenticating does not
    // register the same install twice.
    let machine_id = credential()
        .map(|(id, _)| id)
        .unwrap_or_else(random_machine_id);
    let flow_id = random_machine_id();
    let auth_url = format!(
        "{PORTAL_BASE}/device/selectAccounts?challenge={challenge}&challenge_method=S256&nonce={nonce}&machine_id={machine_id}&client_id={CLIENT_ID}"
    );
    let expires_at = now_ms() + FLOW_TIMEOUT_MS;
    pending_flows()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(
            flow_id.clone(),
            PendingFlow {
                verifier,
                nonce,
                machine_id,
                expires_at,
            },
        );
    // Opening the page is best-effort: the renderer shows the URL as well, so a
    // failed handoff must not fail the flow.
    // The URL is returned to the caller, which is the host that has a browser to
    // open it with. Opening it here would put a desktop dependency inside the
    // engine, and a headless endpoint has nothing to open.
    Ok(json!({
        "ok": true, "flowId": flow_id, "authUrl": auth_url,
        "expiresAt": expires_at, "interval": POLL_INTERVAL_SECONDS,
    }))
}

/// `qoder-auth:poll`. 404 is "the browser leg is not finished"; a body carrying
/// `token` completes the login.
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
        let mut guard = pending_flows().lock().unwrap_or_else(|p| p.into_inner());
        match guard.get(&flow_id) {
            None => None,
            Some(flow) if flow.expires_at <= now_ms() => {
                guard.remove(&flow_id);
                None
            }
            Some(flow) => Some((
                flow.verifier.clone(),
                flow.nonce.clone(),
                flow.machine_id.clone(),
            )),
        }
    };
    let Some((verifier, nonce, machine_id)) = flow else {
        return Ok(
            json!({ "ok": false, "expired": true, "error": "Login flow expired. Start again." }),
        );
    };
    let client = match client() {
        Ok(client) => client,
        Err(error) => return Ok(json!({ "ok": false, "error": error })),
    };
    let url = format!("{OPENAPI_BASE}/api/v1/deviceToken/poll");
    let response = with_cosy_headers(client.get(&url))
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
    if !(200..300).contains(&status_code) {
        let detail = response.text().await.unwrap_or_default();
        let detail: String = detail.chars().take(200).collect();
        return Ok(
            json!({ "ok": false, "error": format!("device token poll {status_code}: {detail}") }),
        );
    }
    let data: Value = response.json().await.unwrap_or(Value::Null);
    let token = data
        .get("token")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if token.is_empty() {
        return Ok(json!({ "ok": false, "pending": true }));
    }
    pending_flows()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .remove(&flow_id);
    match complete_login(&client, &machine_id, &data, &token).await {
        Ok(account) => Ok(json!({ "ok": true, "account": account })),
        Err(error) => Ok(json!({ "ok": false, "error": error })),
    }
}

/// Turn a device-flow token into the stored profile: the token fields, then the
/// account's own userinfo, then the runtime fields the signer expects.
async fn complete_login(
    client: &reqwest::Client,
    machine_id: &str,
    token_payload: &Value,
    token: &str,
) -> Result<String, String> {
    let mut user_info = Map::new();
    user_info.insert("security_oauth_token".into(), json!(token));
    user_info.insert("access_token".into(), json!(token));
    if let Some(refresh) = token_payload.get("refresh_token").and_then(Value::as_str) {
        user_info.insert("refresh_token".into(), json!(refresh));
    }
    if let Some(expire) = expiry_seconds(token_payload, "expires_at", "expires_in") {
        user_info.insert("expire_time".into(), json!(expire));
    }
    if let Some(expire) = expiry_seconds(
        token_payload,
        "refresh_token_expires_at",
        "refresh_token_expires_in",
    ) {
        user_info.insert("refresh_token_expire_time".into(), json!(expire));
    }
    user_info.insert("login_method".into(), json!("browser"));
    user_info.insert("login_timestamp".into(), json!(now_seconds()));
    let mut user_info = Value::Object(user_info);
    merge_user_info(&mut user_info, fetch_user_info(client, token).await?);
    regenerate_runtime_fields(&mut user_info)?;
    write_store(machine_id, &user_info)?;
    super::catalog::forget_context();
    Ok(account_label(&user_info))
}

/// `expires_at` (absolute) wins over `expires_in` (relative), like the client.
fn expiry_seconds(payload: &Value, absolute: &str, relative: &str) -> Option<i64> {
    if let Some(text) = payload.get(absolute).and_then(Value::as_str) {
        if let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(text) {
            return Some(parsed.timestamp());
        }
    }
    if let Some(seconds) = payload.get(absolute).and_then(Value::as_i64) {
        return Some(seconds);
    }
    payload
        .get(relative)
        .and_then(Value::as_i64)
        .map(|seconds| now_seconds() + seconds)
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

/// `GET /api/v1/userinfo`: the account identity plus the organization fields the
/// runtime signer hashes. The uid is mandatory — a profile without one cannot
/// produce runtime fields.
async fn fetch_user_info(client: &reqwest::Client, token: &str) -> Result<Value, String> {
    let url = format!("{OPENAPI_BASE}/api/v1/userinfo");
    let response = with_cosy_headers(client.get(&url))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .map_err(|error| error.to_string())?;
    let status_code = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    if !(200..300).contains(&status_code) {
        let detail: String = body.chars().take(200).collect();
        return Err(format!("userinfo {status_code}: {detail}"));
    }
    let parsed: Value =
        serde_json::from_str(&body).map_err(|_| "userinfo returned invalid JSON".to_string())?;
    let organization = parsed.get("organization").cloned().unwrap_or(Value::Null);
    let uid = first_string(&parsed, &["id", "user_id", "uid"])
        .ok_or_else(|| "userinfo response did not include id".to_string())?;
    let mut out = Map::new();
    out.insert("uid".into(), json!(uid));
    if let Some(name) = first_string(&parsed, &["name", "username", "user_name"]) {
        out.insert("name".into(), json!(name));
    }
    if let Some(email) = first_string(&parsed, &["email"]) {
        out.insert("email".into(), json!(email));
    }
    if let Some(avatar) = first_string(&parsed, &["avatar_url", "avatar"]) {
        out.insert("avatar_url".into(), json!(avatar));
    }
    let organization_id = first_string(&parsed, &["orgId", "organization_id", "organizationId"])
        .or_else(|| first_string(&organization, &["org_id", "orgId", "id"]));
    if let Some(organization_id) = organization_id.clone() {
        out.insert("organization_id".into(), json!(organization_id));
    }
    if let Some(name) = first_string(
        &parsed,
        &["orgName", "organization_name", "organizationName"],
    )
    .or_else(|| first_string(&organization, &["org_name", "orgName", "name"]))
    {
        out.insert("organization_name".into(), json!(name));
    }
    let tags = parsed
        .get("organization_tags")
        .or_else(|| parsed.get("organizationTags"))
        .and_then(Value::as_array)
        .map(|tags| {
            tags.iter()
                .filter(|tag| tag.is_string())
                .cloned()
                .collect::<Vec<Value>>()
        });
    let tags = match (tags, organization_id) {
        // The organization's tag list is authoritative when there is an org.
        (_, Some(organization_id)) => {
            fetch_organization_tags(client, token, &organization_id).await
        }
        (Some(tags), None) => tags,
        (None, None) => Vec::new(),
    };
    out.insert("organization_tags".into(), json!(tags));
    // The client defaults this to true when the policy endpoint does not answer.
    out.insert("data_policy_agreed".into(), json!(true));
    Ok(Value::Object(out))
}

/// `GET /api/v1/organizations/{id}/tags`. A failure here is not fatal: the client
/// keeps whatever tags it already had rather than blocking the login.
async fn fetch_organization_tags(
    client: &reqwest::Client,
    token: &str,
    organization_id: &str,
) -> Vec<Value> {
    let encoded = organization_id.replace('/', "%2F");
    let url = format!("{OPENAPI_BASE}/api/v1/organizations/{encoded}/tags");
    let Ok(response) = with_cosy_headers(client.get(&url))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
    else {
        return Vec::new();
    };
    if !response.status().is_success() {
        return Vec::new();
    }
    let parsed: Value = response.json().await.unwrap_or(Value::Null);
    parsed
        .get("tags")
        .and_then(Value::as_array)
        .map(|tags| tags.iter().filter(|tag| tag.is_string()).cloned().collect())
        .unwrap_or_default()
}

fn merge_user_info(target: &mut Value, extra: Value) {
    let (Some(target), Some(extra)) = (target.as_object_mut(), extra.as_object()) else {
        return;
    };
    for (key, value) in extra {
        target.insert(key.clone(), value.clone());
    }
}

/// `regenerateRuntimeFields`: `encrypt_user_info` and `key` seal the account
/// profile for the server, and the signer reads them back out of the profile. A
/// profile without them cannot sign a request, so this runs on every path that
/// writes a token — login, adopt and refresh.
fn regenerate_runtime_fields(user_info: &mut Value) -> Result<(), String> {
    let runtime = generate_runtime_auth(user_info)?;
    let Some(target) = user_info.as_object_mut() else {
        return Err("profile is not an object".into());
    };
    target.insert("encrypt_user_info".into(), json!(runtime.encrypt_user_info));
    target.insert("key".into(), json!(runtime.key));
    Ok(())
}

// --------------------------------------------------------------- token refresh

/// Renew the access token when it is within an hour of expiry, the same window
/// the Qoder client uses. Callers await this immediately before spending the
/// token, so an expiring credential is never handed to the signer.
pub(crate) async fn ensure_fresh_token() -> Result<(), String> {
    let Some((machine_id, mut user_info)) = credential() else {
        return Ok(());
    };
    let Some(expire_time) = user_info.get("expire_time").and_then(Value::as_i64) else {
        return Ok(());
    };
    if expire_time - REFRESH_SKEW_SECONDS > now_seconds() {
        return Ok(());
    }
    let refresh_token = user_info
        .get("refresh_token")
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty())
        .map(str::to_string);
    let Some(refresh_token) = refresh_token else {
        return Err("Qoder subscription has expired and carries no refresh token".into());
    };
    let client = client()?;
    let url = format!("{OPENAPI_BASE}/api/v1/deviceToken/refresh");
    let response = with_cosy_headers(client.post(&url))
        .header("Content-Type", "application/json")
        .body(json!({ "refresh_token": refresh_token }).to_string())
        .send()
        .await
        .map_err(|error| error.to_string())?;
    let status_code = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    if !(200..300).contains(&status_code) {
        let detail: String = body.chars().take(200).collect();
        return Err(format!("token refresh {status_code}: {detail}"));
    }
    let parsed: Value = serde_json::from_str(&body)
        .map_err(|_| "token refresh returned invalid JSON".to_string())?;
    // The refresh response names the token `device_token`, not `token`.
    let token = parsed
        .get("device_token")
        .or_else(|| parsed.get("token"))
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty())
        .ok_or_else(|| "token refresh returned no token".to_string())?
        .to_string();
    {
        let Some(target) = user_info.as_object_mut() else {
            return Err("profile is not an object".into());
        };
        target.insert("security_oauth_token".into(), json!(token));
        target.insert("access_token".into(), json!(token));
        if let Some(refresh) = parsed.get("refresh_token").and_then(Value::as_str) {
            target.insert("refresh_token".into(), json!(refresh));
        }
        if let Some(expire) = expiry_seconds(&parsed, "expires_at", "expires_in") {
            target.insert("expire_time".into(), json!(expire));
        }
    }
    regenerate_runtime_fields(&mut user_info)?;
    write_store(&machine_id, &user_info)?;
    super::catalog::forget_context();
    Ok(())
}

// ----------------------------------------------------------------- quota usage

/// The subscription's credit quota, as Qoder reports it.
///
/// Lives here rather than in the client that draws the gauge because reading it
/// means spending the access token, and the token does not leave this crate: a
/// host asks for the quota and gets the account plus Qoder's own payload, never
/// the bearer. The upstream fields are passed through untouched — what counts as
/// a credit, a percentage or an org pool is the caller's presentation problem.
///
/// Signed out is an ANSWER (`signedIn: false`), not an error: a client offers a
/// login for that, and would have to string-match an error message to tell it
/// apart from a failed request. Only a real failure (no client, transport, a
/// non-2xx) comes back as `Err`.
///
/// The quota route takes the plain bearer with no COSY signature, so this path
/// needs the credential store and nothing from the signer.
pub async fn usage() -> Result<Value, String> {
    let Ok(token) = super::catalog::access_token() else {
        return Ok(json!({ "signedIn": false }));
    };
    let client = client()?;
    let url = format!("{OPENAPI_BASE}/api/v2/quota/usage");
    let response = with_cosy_headers(client.get(&url))
        .header("Authorization", format!("Bearer {}", token.access_token))
        .send()
        .await
        .map_err(|error| error.to_string())?;
    let status_code = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    if !(200..300).contains(&status_code) {
        let detail: String = body.chars().take(200).collect();
        return Err(format!("quota/usage {status_code}: {detail}"));
    }
    let quota: Value =
        serde_json::from_str(&body).map_err(|_| "quota/usage returned invalid JSON".to_string())?;
    Ok(json!({ "signedIn": true, "account": token.account, "quota": quota }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn machine_os_pairs_arch_with_the_platform_name() {
        let value = machine_os();
        assert!(value.contains('_'), "expected <arch>_<os>, got {value}");
        #[cfg(target_os = "macos")]
        assert!(
            value.ends_with("_darwin"),
            "macOS must report darwin, got {value}"
        );
    }

    #[test]
    fn verifier_uses_only_the_unreserved_alphabet() {
        let verifier = random_verifier();
        assert_eq!(verifier.chars().count(), 64);
        assert!(
            verifier
                .bytes()
                .all(|byte| VERIFIER_ALPHABET.contains(&byte)),
            "{verifier}"
        );
    }

    #[test]
    fn machine_id_is_uuid_shaped() {
        let id = random_machine_id();
        let groups: Vec<usize> = id.split('-').map(str::len).collect();
        assert_eq!(groups, vec![8, 4, 4, 4, 12], "{id}");
    }

    #[test]
    fn absolute_expiry_wins_over_the_relative_one() {
        let payload = json!({ "expires_at": "2030-01-01T00:00:00Z", "expires_in": 60 });
        assert_eq!(
            expiry_seconds(&payload, "expires_at", "expires_in"),
            Some(1893456000)
        );
        let relative = json!({ "expires_in": 120 });
        let resolved = expiry_seconds(&relative, "expires_at", "expires_in").unwrap();
        assert!(
            (resolved - now_seconds() - 120).abs() <= 2,
            "got {resolved}"
        );
    }

    #[test]
    fn a_profile_without_a_token_reads_back_as_signed_out() {
        let profile = json!({ "uid": "u-1", "name": "Someone" });
        assert!(access_token_of(&profile).is_empty());
        assert_eq!(account_label(&profile), "Someone");
    }

    /// Signed out is an ANSWER on both surfaces, never an error: a settings pane
    /// offers a login for `loggedIn: false`, and the quota gauge shows a "sign in"
    /// row for `signedIn: false`. Reporting either as a failure is what made a
    /// never-signed-in user see a red error instead of the button they needed.
    ///
    /// The quota answer also proves no request went out — there is no token to
    /// send one with, so an implementation that tried would have to fail instead.
    #[tokio::test]
    async fn an_absent_credential_is_a_signed_out_answer_on_both_surfaces() {
        let _lock = super::super::test_env_lock().lock().await;
        let fixture = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(fixture.path());
        super::super::catalog::forget_context();
        assert_eq!(status(), json!({ "loggedIn": false }));
        assert_eq!(
            usage().await.expect("signed out is not an error"),
            json!({ "signedIn": false })
        );
    }
}
