use super::{config_root, invalidate, state};
use base64::Engine;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::io::Write;
use std::path::Path;
use std::time::Duration;

pub(super) const NEEDS_REIMPORT: &str = "cursor_adopted_credential_needs_reimport";
pub(super) const NEEDS_LOGIN: &str = "cursor_browser_login_expired";
pub(super) const METADATA_MISSING: &str = "cursor_client_metadata_missing";
const STORE: &str = "cursor-subscription-oauth.json";
const MAX_BODY: usize = 8 * 1024 * 1024;

#[derive(Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Credential {
    pub access_token: String,
    pub expires_at: Option<i64>,
    pub account: Option<String>,
    pub client_version: Option<String>,
    pub client_identity: Option<String>,
    pub backend_url: Option<String>,
    pub inference_url: Option<String>,
    #[serde(default)]
    pub team_id: Option<u32>,
}

fn validate_origin(value: Option<&str>, default: &str) -> Result<String, String> {
    let url = url::Url::parse(value.unwrap_or(default)).map_err(|_| "cursor_endpoint_invalid")?;
    let local = matches!(url.host_str(), Some("127.0.0.1" | "localhost" | "[::1]"));
    let trusted = url.host_str().is_some_and(|host| {
        host == "cursor.sh"
            || host.ends_with(".cursor.sh")
            || host == "cursor.com"
            || host.ends_with(".cursor.com")
    });
    if !((url.scheme() == "https" && trusted) || (cfg!(test) && local && url.scheme() == "http"))
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
    {
        return Err("cursor_endpoint_invalid".into());
    }
    Ok(url.origin().ascii_serialization())
}

impl Credential {
    pub fn api_origin(&self) -> Result<String, String> {
        validate_origin(self.backend_url.as_deref(), "https://api2.cursor.sh")
    }
    pub fn inference_origin(&self) -> Result<String, String> {
        validate_origin(
            self.inference_url.as_deref(),
            "https://agentn.api5.cursor.sh",
        )
    }
    pub(super) fn valid(&self) -> Result<(), String> {
        if self.access_token.trim().is_empty()
            || self.access_token.len() > 32768
            || self
                .access_token
                .chars()
                .any(|c| c.is_whitespace() || c.is_control())
        {
            return Err("cursor_client_credentials_invalid".into());
        }
        if self
            .expires_at
            .is_some_and(|expiry| expiry <= chrono::Utc::now().timestamp_millis())
        {
            return Err(NEEDS_REIMPORT.into());
        }
        if self
            .team_id
            .is_some_and(|team| team == 0 || team > i32::MAX as u32)
        {
            return Err("cursor_client_credentials_invalid".into());
        }
        self.api_origin()?;
        self.inference_origin()?;
        for value in [&self.account, &self.client_version].into_iter().flatten() {
            if value.len() > 1024 || value.chars().any(char::is_control) {
                return Err("cursor_client_credentials_invalid".into());
            }
        }
        if self.client_identity.as_deref().is_some_and(|identity| {
            identity.is_empty()
                || identity.len() > 513
                || identity.split('/').count() > 2
                || identity.split('/').any(|part| {
                    part.is_empty()
                        || part.len() > 256
                        || !part
                            .bytes()
                            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
                })
        }) {
            return Err("cursor_client_credentials_invalid".into());
        }
        Ok(())
    }

    pub fn checksum(&self, timestamp_millis: i64) -> Result<reqwest::header::HeaderValue, String> {
        let identity = self.client_identity.as_deref().ok_or(METADATA_MISSING)?;
        let timestamp = (timestamp_millis.max(0) as u64 / 1_000_000) as u32;
        let mut bytes = [0_u8; 6];
        let mut previous = 165_u8;
        for (index, shift) in [8, 0, 24, 16, 8, 0].into_iter().enumerate() {
            bytes[index] = ((timestamp >> shift) as u8 ^ previous).wrapping_add(index as u8);
            previous = bytes[index];
        }
        let prefix = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
        let mut value = reqwest::header::HeaderValue::from_str(&format!("{prefix}{identity}"))
            .map_err(|_| "cursor_client_credentials_invalid")?;
        value.set_sensitive(true);
        Ok(value)
    }
}

#[derive(Clone, Copy, Default, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(super) enum AuthMethod {
    #[default]
    Imported,
    Browser,
}

#[derive(Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct StoredCredential {
    pub credential_id: String,
    pub user_info: Credential,
    #[serde(default)]
    pub auth_method: AuthMethod,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
}

pub(super) fn read(root: &Path) -> Result<StoredCredential, String> {
    let path = root.join(STORE);
    let len = std::fs::metadata(&path)
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                "cursor_not_signed_in"
            } else {
                "cursor_credential_read_failed"
            }
        })?
        .len();
    if len > 131072 {
        return Err("cursor_client_credentials_invalid".into());
    }
    serde_json::from_slice(&std::fs::read(path).map_err(|_| "cursor_credential_read_failed")?)
        .map_err(|_| "cursor_client_credentials_invalid".into())
}

pub(super) fn write(root: &Path, credential: &StoredCredential) -> Result<(), String> {
    std::fs::create_dir_all(root).map_err(|_| "cursor_credential_write_failed")?;
    let temp = root.join(format!(".{STORE}.{}.tmp", uuid::Uuid::new_v4()));
    let outcome = (|| {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&temp)
            .map_err(|_| "cursor_credential_write_failed")?;
        file.write_all(
            &serde_json::to_vec(credential).map_err(|_| "cursor_credential_write_failed")?,
        )
        .map_err(|_| "cursor_credential_write_failed")?;
        file.sync_all()
            .map_err(|_| "cursor_credential_write_failed")?;
        std::fs::rename(&temp, root.join(STORE)).map_err(|_| "cursor_credential_write_failed")?;
        Ok(())
    })();
    if outcome.is_err() {
        let _ = std::fs::remove_file(temp);
    }
    outcome
}

pub(super) fn installation_identity(root: &Path) -> Result<String, String> {
    std::fs::create_dir_all(root).map_err(|_| "cursor_credential_write_failed")?;
    let path = root.join("cursor-install-identity");
    let read_identity = || {
        if std::fs::metadata(&path)
            .map_err(|_| "cursor_client_metadata_missing")?
            .len()
            > 64
        {
            return Err("cursor_client_metadata_invalid".to_string());
        }
        let identity =
            std::fs::read_to_string(&path).map_err(|_| "cursor_client_metadata_missing")?;
        if identity.len() != 32 || !identity.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err("cursor_client_metadata_invalid".into());
        }
        Ok(identity)
    };
    if path.exists() {
        return read_identity();
    }
    let identity = uuid::Uuid::new_v4().simple().to_string();
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    match options.open(&path) {
        Ok(mut file) => {
            file.write_all(identity.as_bytes())
                .map_err(|_| "cursor_credential_write_failed")?;
            file.sync_all()
                .map_err(|_| "cursor_credential_write_failed")?;
            Ok(identity)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => read_identity(),
        Err(_) => Err("cursor_credential_write_failed".into()),
    }
}

pub fn adopt_credential(user_info: Value, expected_credential_id: Option<&str>) -> Value {
    super::diagnostics::sync_value("adopt_credential", || {
        adopt_credential_inner(user_info, expected_credential_id)
    })
}

fn adopt_credential_inner(user_info: Value, expected_credential_id: Option<&str>) -> Value {
    let credential = match serde_json::from_value::<Credential>(user_info) {
        Ok(value) => value,
        Err(_) => return json!({"ok":false,"error":"cursor_client_credentials_invalid"}),
    };
    super::diagnostics::event(
        "adopt_metadata",
        json!({"hasAccount":credential.account.is_some(),"hasClientVersion":credential.client_version.is_some(),"hasClientIdentity":credential.client_identity.is_some(),"hasTeam":credential.team_id.is_some(),"expiryKnown":credential.expires_at.is_some(),"expired":credential.expires_at.is_some_and(|expiry| expiry <= chrono::Utc::now().timestamp_millis()),"customApiOrigin":credential.backend_url.is_some()}),
    );
    if let Err(error) = credential.valid() {
        return json!({"ok":false,"error":error});
    }
    let mut state = state().lock().unwrap_or_else(|p| p.into_inner());
    if let Some(expected) = expected_credential_id {
        if read(&config_root(&state))
            .ok()
            .is_none_or(|stored| stored.credential_id != expected)
        {
            return json!({"ok":false,"error":"cursor_credentials_changed"});
        }
    }
    let credential_id = uuid::Uuid::new_v4().to_string();
    let stored = StoredCredential {
        credential_id: credential_id.clone(),
        user_info: credential.clone(),
        auth_method: AuthMethod::Imported,
        refresh_token: None,
    };
    let root = config_root(&state);
    if let Err(error) = write(&root, &stored) {
        return json!({"ok":false,"error":error});
    }
    super::diagnostics::event("adopt_committed", json!({"generation":state.generation}));
    invalidate(&mut state);
    json!({"ok":true,"account":credential.account,"credentialId":credential_id})
}

pub fn status() -> Value {
    super::diagnostics::sync_value("status", status_inner)
}

fn status_inner() -> Value {
    let state = state().lock().unwrap_or_else(|p| p.into_inner());
    match read(&config_root(&state)) {
        Ok(stored) => {
            let browser = stored.auth_method == AuthMethod::Browser;
            let credential = stored.user_info;
            let expired = state.rejected
                || credential
                    .expires_at
                    .is_some_and(|expiry| expiry <= chrono::Utc::now().timestamp_millis());
            let missing_metadata = credential.client_identity.is_none();
            let reason = if expired {
                Some(if browser { NEEDS_LOGIN } else { NEEDS_REIMPORT })
            } else if missing_metadata {
                Some(if browser {
                    NEEDS_LOGIN
                } else {
                    METADATA_MISSING
                })
            } else {
                None
            };
            json!({"loggedIn":!expired,"credentialId":stored.credential_id,"account":credential.account,"expiresAt":credential.expires_at,"loginExpired":expired,"needsReimport":!browser && (expired || missing_metadata),"needsLogin":browser && (expired || missing_metadata),"authMethod":stored.auth_method,"reason":reason})
        }
        Err(error) if error == "cursor_not_signed_in" => json!({"loggedIn":false}),
        Err(error) => json!({"loggedIn":false,"error":error}),
    }
}

pub fn logout(expected_credential_id: Option<&str>) -> Result<Value, String> {
    let mut state = state().lock().unwrap_or_else(|p| p.into_inner());
    if let Some(expected) = expected_credential_id {
        if read(&config_root(&state))
            .ok()
            .is_none_or(|stored| stored.credential_id != expected)
        {
            return Ok(json!({"ok":true,"removed":false}));
        }
    }
    let root = config_root(&state);
    match std::fs::remove_file(root.join(STORE)) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err("cursor_credential_delete_failed".into()),
    }
    invalidate(&mut state);
    Ok(json!({"ok":true}))
}

pub(super) fn snapshot() -> Result<(Credential, u64), String> {
    let state = state().lock().unwrap_or_else(|p| p.into_inner());
    let stored = read(&config_root(&state))?;
    let expired = if stored.auth_method == AuthMethod::Browser {
        NEEDS_LOGIN
    } else {
        NEEDS_REIMPORT
    };
    if state.rejected {
        return Err(expired.into());
    }
    stored.user_info.valid().map_err(|error| {
        if error == NEEDS_REIMPORT {
            expired.into()
        } else {
            error
        }
    })?;
    Ok((stored.user_info, state.generation))
}

pub(super) fn mark_rejected(generation: u64) -> String {
    let mut state = state().lock().unwrap_or_else(|p| p.into_inner());
    if state.generation != generation {
        return "cursor_credential_changed".into();
    }
    let error = if read(&config_root(&state))
        .is_ok_and(|stored| stored.auth_method == AuthMethod::Browser)
    {
        NEEDS_LOGIN
    } else {
        NEEDS_REIMPORT
    };
    if state.login.is_some() {
        // Rejection of the previous credential cannot cancel an explicit
        // authorization already in progress. New requests are still blocked.
        state.rejected = true;
    } else {
        invalidate(&mut state);
        state.rejected = true;
    }
    error.into()
}

pub(super) fn check_generation(generation: u64) -> Result<(), String> {
    let state = state().lock().unwrap_or_else(|p| p.into_inner());
    if state.generation != generation {
        return Err("cursor_credential_changed".into());
    }
    Ok(())
}

pub(super) fn client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(15))
        .pool_max_idle_per_host(2)
        .pool_idle_timeout(Duration::from_secs(30))
        .build()
        .map_err(|_| "cursor_http_client_failed".into())
}

pub(super) fn inference_client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .http2_prior_knowledge()
        .connect_timeout(Duration::from_secs(15))
        .pool_max_idle_per_host(2)
        .pool_idle_timeout(Duration::from_secs(30))
        .build()
        .map_err(|_| "cursor_http_client_failed".into())
}

pub(super) fn authenticated(
    request: reqwest::RequestBuilder,
    credential: &Credential,
) -> reqwest::RequestBuilder {
    let mut request = request
        .bearer_auth(&credential.access_token)
        .header("connect-protocol-version", "1")
        .header("x-cursor-client-type", "ide")
        .header("x-cursor-client-device-type", "desktop")
        .header("x-ghost-mode", "true")
        .header("x-request-id", uuid::Uuid::new_v4().to_string());
    if let Some(team) = credential.team_id {
        request = request.header("x-cursor-team-id", team.to_string());
    }
    if let Some(version) = &credential.client_version {
        request = request.header("x-cursor-client-version", version);
    }
    request
}

pub(super) fn sdk_authenticated(
    request: reqwest::RequestBuilder,
    credential: &Credential,
) -> reqwest::RequestBuilder {
    let mut request = request
        .bearer_auth(&credential.access_token)
        .header("connect-protocol-version", "1")
        .header("x-cursor-client-type", "sdk")
        .header("x-cursor-client-version", "sdk-1.0.38")
        .header("x-ghost-mode", "true");
    if let Some(team) = credential.team_id {
        request = request.header("x-cursor-team-id", team.to_string());
    }
    request
}

pub(super) async fn bounded_body(response: reqwest::Response) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| {
            super::diagnostics::network_error(&error);
            "cursor_response_read_failed"
        })?;
        if bytes.is_empty() {
            super::diagnostics::event("body_first_chunk", json!({}));
        }
        if bytes.len() + chunk.len() > MAX_BODY {
            return Err("cursor_response_too_large".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    super::diagnostics::event("body_complete", json!({"bytes":bytes.len()}));
    Ok(bytes)
}

async fn unary_body(response: reqwest::Response) -> Result<Vec<u8>, String> {
    let mut gzip = false;
    for header in response
        .headers()
        .get_all(reqwest::header::CONTENT_ENCODING)
        .iter()
    {
        for encoding in header
            .to_str()
            .map_err(|_| "cursor_response_read_failed")?
            .split(',')
        {
            let encoding = encoding.trim();
            if encoding.eq_ignore_ascii_case("identity") {
                continue;
            }
            if !gzip
                && (encoding.eq_ignore_ascii_case("gzip")
                    || encoding.eq_ignore_ascii_case("x-gzip"))
            {
                gzip = true;
            } else {
                return Err("cursor_response_read_failed".into());
            }
        }
    }
    // Bound both the wire body and its decoded form. Multi-member decoding must
    // reach EOF for valid-sized replies so CRC and truncated trailers are checked.
    let bytes = bounded_body(response).await?;
    if !gzip {
        return Ok(bytes);
    }
    let wire_bytes = bytes.len();
    super::diagnostics::event("body_decode_begin", json!({"wireBytes":wire_bytes}));
    let decoded = tokio::task::spawn_blocking(move || {
        use std::io::Read;
        let decoder = flate2::read::MultiGzDecoder::new(bytes.as_slice());
        let mut decoded = Vec::new();
        decoder
            .take((MAX_BODY + 1) as u64)
            .read_to_end(&mut decoded)
            .map_err(|_| "cursor_response_read_failed")?;
        if decoded.len() > MAX_BODY {
            return Err("cursor_response_too_large");
        }
        Ok(decoded)
    })
    .await
    .map_err(|_| "cursor_response_read_failed")?
    .map_err(str::to_owned)?;
    super::diagnostics::event(
        "body_decoded",
        json!({"wireBytes":wire_bytes,"decodedBytes":decoded.len()}),
    );
    Ok(decoded)
}

fn error_code(value: &Value) -> &str {
    match value.get("code") {
        Some(Value::String(code)) => code.as_str(),
        Some(Value::Number(code)) => match code.as_i64() {
            Some(1) => "canceled",
            Some(16) => "unauthenticated",
            Some(7) => "permission_denied",
            Some(8) => "resource_exhausted",
            Some(4 | 14) => "unavailable",
            _ => "unknown",
        },
        _ => "unknown",
    }
}

fn detail_classification(code: u32) -> Option<&'static str> {
    Some(match code {
        1 | 42 => "cursor_provider_credentials_invalid",
        2 | 3 | 6 | 11 | 12 | 18 | 38 => NEEDS_REIMPORT,
        4 | 46..=49 | 53 | 54 | 64..=66 => "cursor_permission_denied",
        5 | 61 => "cursor_model_unavailable",
        7 | 8 | 14 | 22 | 28 | 34 | 41 | 50 | 51 => "cursor_rate_limit",
        9 | 10 => "cursor_usage_limit",
        20 => "cursor_token_limit_exceeded",
        21 | 35 => "cursor_request_cancelled",
        23 => "cursor_subscription_required",
        25 | 55 | 59 | 63 => "cursor_overloaded",
        30 => "cursor_client_update_required",
        31 => "cursor_image_too_large",
        40 => "cursor_api_deprecated",
        43 => "cursor_context_length_exceeded",
        44 | 45 => "cursor_usage_pricing_required",
        13 | 33 | 36 | 37 | 39 | 57 | 62 => "cursor_overloaded",
        29 | 52 => "cursor_upstream_error",
        56 => "cursor_network_error",
        58 => "cursor_model_blocked",
        60 => "cursor_max_mode_required",
        _ => return None,
    })
}

fn detail_json_code(value: &Value) -> Option<u32> {
    match value.get("error")? {
        Value::Number(code) => code.as_u64().and_then(|code| code.try_into().ok()),
        Value::String(code) => Some(match code.as_str() {
            "ERROR_CONVERSATION_TOO_LONG" => 43,
            "ERROR_MAX_TOKENS" => 20,
            "ERROR_USER_ABORTED_REQUEST" => 21,
            "ERROR_MODEL_BLOCKED" => 58,
            "ERROR_MAX_MODE_REQUIRED" => 60,
            "ERROR_PRO_USER_ONLY" => 23,
            "ERROR_USAGE_PRICING_REQUIRED" => 44,
            "ERROR_USAGE_PRICING_REQUIRED_CHANGEABLE" => 45,
            "ERROR_FREE_USER_USAGE_LIMIT" => 9,
            "ERROR_PRO_USER_USAGE_LIMIT" => 10,
            "ERROR_FREE_USER_RATE_LIMIT_EXCEEDED" => 7,
            "ERROR_PRO_USER_RATE_LIMIT_EXCEEDED" => 8,
            "ERROR_GENERIC_RATE_LIMIT_EXCEEDED" => 22,
            "ERROR_OPENAI_RATE_LIMIT_EXCEEDED" => 14,
            "ERROR_GPT_4_VISION_PREVIEW_RATE_LIMIT" => 28,
            "ERROR_API_KEY_RATE_LIMIT" => 34,
            "ERROR_RESOURCE_EXHAUSTED" => 41,
            "ERROR_RATE_LIMITED" => 50,
            "ERROR_RATE_LIMITED_CHANGEABLE" => 51,
            "ERROR_TIMEOUT" => 25,
            "ERROR_EXTENSION_HOST_TIMEOUT" => 55,
            "ERROR_INTERNAL" => 59,
            "ERROR_SLOW_POOL" => 63,
            "ERROR_NOT_LOGGED_IN" => 2,
            "ERROR_INVALID_AUTH_ID" => 3,
            "ERROR_USER_NOT_FOUND" => 6,
            "ERROR_AUTH_TOKEN_NOT_FOUND" => 11,
            "ERROR_AUTH_TOKEN_EXPIRED" => 12,
            "ERROR_AGENT_REQUIRES_LOGIN" => 18,
            "ERROR_UNAUTHORIZED" => 38,
            "ERROR_BAD_API_KEY" => 1,
            "ERROR_BAD_USER_API_KEY" => 42,
            "ERROR_NOT_HIGH_ENOUGH_PERMISSIONS" => 4,
            "ERROR_GITHUB_NO_USER_CREDENTIALS" => 46,
            "ERROR_GITHUB_USER_NO_ACCESS" => 47,
            "ERROR_GITHUB_APP_NO_ACCESS" => 48,
            "ERROR_GITHUB_MULTIPLE_OWNERS" => 49,
            "ERROR_HOOKS_BLOCKED" => 53,
            "ERROR_SUSPICIOUS_USAGE_BLOCKED" => 54,
            "ERROR_UNSUPPORTED_REGION" => 64,
            "ERROR_ACCOUNT_CLOSED" => 65,
            "ERROR_TENANT_RESTRICTION_VIOLATION" => 66,
            "ERROR_BAD_MODEL_NAME" => 5,
            "ERROR_MODEL_NO_LONGER_SUPPORTED" => 61,
            "ERROR_DEBOUNCED" => 35,
            "ERROR_OUTDATED_CLIENT" => 30,
            "ERROR_CLAUDE_IMAGE_TOO_LARGE" => 31,
            "ERROR_DEPRECATED" => 40,
            "ERROR_NETWORK_ERROR" => 56,
            "ERROR_OPENAI" => 13,
            "ERROR_CUSTOM_MESSAGE" => 29,
            "ERROR_FILE_NOT_FOUND" => 33,
            "ERROR_BAD_REQUEST" => 36,
            "ERROR_REPOSITORY_SERVICE_REPOSITORY_IS_NOT_INITIALIZED" => 37,
            "ERROR_NOT_FOUND" => 39,
            "ERROR_CUSTOM" => 52,
            "ERROR_PROVIDER_ERROR" => 57,
            "ERROR_PRICING_WARNING" => 62,
            _ => return None,
        }),
        _ => None,
    }
}

fn detail_varint(bytes: &[u8], offset: &mut usize) -> Option<u64> {
    let mut value = 0_u64;
    for shift in (0..70).step_by(7) {
        let byte = *bytes.get(*offset)?;
        *offset += 1;
        if shift == 63 && byte > 1 {
            return None;
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some(value);
        }
    }
    None
}

fn detail_binary_code(bytes: &[u8]) -> Option<u32> {
    let mut offset = 0;
    let mut code = None;
    while offset < bytes.len() {
        let tag = detail_varint(bytes, &mut offset)?;
        let field = tag >> 3;
        let wire = tag & 7;
        if field == 0 || field > 0x1fff_ffff {
            return None;
        }
        if field == 1 {
            if wire != 0 || code.is_some() {
                return None;
            }
            code = Some(detail_varint(bytes, &mut offset)?.try_into().ok()?);
        } else {
            if (field == 2 && wire != 2) || (field == 3 && wire != 0) {
                return None;
            }
            let size = match wire {
                0 => {
                    detail_varint(bytes, &mut offset)?;
                    0
                }
                1 => 8,
                2 => detail_varint(bytes, &mut offset)?.try_into().ok()?,
                5 => 4,
                _ => return None,
            };
            offset = offset.checked_add(size).filter(|end| *end <= bytes.len())?;
        }
    }
    code
}

fn typed_error(value: &Value) -> Option<&'static str> {
    const TYPE: &str = "aiserver.v1.ErrorDetails";
    const TYPE_URL: &str = "type.googleapis.com/aiserver.v1.ErrorDetails";
    const MAX_DETAILS: usize = 16;
    const MAX_DETAIL_BYTES: usize = 16 * 1024;
    let details = value.get("details")?.as_array()?;
    if details.len() > MAX_DETAILS {
        return None;
    }
    let mut classification = None;
    for detail in details {
        let code = if detail.get("type").and_then(Value::as_str) == Some(TYPE)
            || detail.get("typeUrl").and_then(Value::as_str) == Some(TYPE_URL)
        {
            let encoded = detail.get("value")?.as_str()?;
            if encoded.len() > MAX_DETAIL_BYTES.div_ceil(3) * 4 {
                return None;
            }
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .or_else(|_| base64::engine::general_purpose::STANDARD_NO_PAD.decode(encoded))
                .ok()?;
            if bytes.len() > MAX_DETAIL_BYTES {
                return None;
            }
            detail_binary_code(&bytes)?
        } else if detail.get("@type").and_then(Value::as_str) == Some(TYPE_URL) {
            detail_json_code(detail)?
        } else {
            continue;
        };
        if let Some(next) = detail_classification(code) {
            if classification.is_some_and(|previous| previous != next) {
                return None;
            }
            classification = Some(next);
        }
    }
    classification
}

pub(super) fn upstream_error(value: &Value) -> String {
    let value = value
        .get("error")
        .filter(|value| value.is_object())
        .unwrap_or(value);
    let code = error_code(value);
    if code == "unauthenticated" {
        return NEEDS_REIMPORT.into();
    }
    if let Some(error) = typed_error(value) {
        return error.into();
    }
    let message = value
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("Cursor request failed");
    let message: String = message
        .chars()
        .take(1024)
        .filter(|c| !c.is_control())
        .collect();
    match code {
        "canceled" => "cursor_request_cancelled".into(),
        "permission_denied"
            if message.contains("RunInference") && message.contains("not enabled") =>
        {
            "cursor_inference_not_enabled".into()
        }
        // Provider messages can echo authorization or request content. Only
        // stable classifications cross the engine boundary.
        "permission_denied" => "cursor_permission_denied".into(),
        "resource_exhausted" => "cursor_rate_limit".into(),
        "unavailable" | "deadline_exceeded" => "cursor_overloaded".into(),
        _ => "cursor_upstream_error".into(),
    }
}

pub(super) fn http_error(status: u16, value: Option<&Value>) -> String {
    if status == 401 {
        return NEEDS_REIMPORT.into();
    }
    if let Some(classification) = value
        .map(upstream_error)
        .filter(|error| error != "cursor_upstream_error")
    {
        return classification;
    }
    match status {
        429 => "cursor_rate_limit".into(),
        408 | 500..=599 => "cursor_overloaded".into(),
        403 => "cursor_permission_denied".into(),
        _ => "cursor_upstream_error".into(),
    }
}

pub(super) async fn unary(path: &str, body: Value) -> Result<(Value, u64), String> {
    super::diagnostics::result("unary", unary_inner(path, body)).await
}

async fn unary_inner(path: &str, body: Value) -> Result<(Value, u64), String> {
    super::diagnostics::event("auth_wait", json!({}));
    super::auth::ensure_valid().await?;
    let (credential, generation) = snapshot()?;
    super::diagnostics::event(
        "request_send",
        json!({"endpoint":if path.ends_with("AvailableModels") { "catalog" } else { "other" },"timeoutMs":20000,"generation":generation}),
    );
    let response = authenticated(
        client()?.post(format!("{}{path}", credential.api_origin()?)),
        &credential,
    )
    .header(reqwest::header::ACCEPT_ENCODING, "gzip")
    .header("content-type", "application/json")
    .timeout(Duration::from_secs(20))
    .json(&body)
    .send()
    .await
    .map_err(|error| {
        super::diagnostics::network_error(&error);
        "cursor_network_error"
    })?;
    super::diagnostics::headers(&response);
    let status = response.status();
    let bytes = unary_body(response).await?;
    check_generation(generation)?;
    let value = serde_json::from_slice::<Value>(&bytes);
    if !status.is_success() {
        let error = http_error(status.as_u16(), value.as_ref().ok());
        if error == NEEDS_REIMPORT {
            return Err(mark_rejected(generation));
        }
        return Err(error);
    }
    let value = value.map_err(|_| "cursor_invalid_response")?;
    if value.get("error").is_some() {
        let error = upstream_error(&value);
        if error == NEEDS_REIMPORT {
            return Err(mark_rejected(generation));
        }
        return Err(error);
    }
    Ok((value, generation))
}

pub async fn usage() -> Value {
    match unary(
        "/aiserver.v1.DashboardService/GetCurrentPeriodUsage",
        json!({}),
    )
    .await
    {
        Ok((value, _)) => {
            let mut result = serde_json::Map::new();
            result.insert("ok".into(), json!(true));
            // Only transport the allowance fields the source actually supplies.
            for field in [
                "enabled",
                "billingCycleStart",
                "billingCycleEnd",
                "planUsage",
                "spendLimitUsage",
            ] {
                if let Some(value) = value.get(field) {
                    result.insert(field.into(), value.clone());
                }
            }
            Value::Object(result)
        }
        Err(error) => {
            json!({"ok":false,"loginExpired":error == NEEDS_REIMPORT || error == NEEDS_LOGIN,"error":error})
        }
    }
}
