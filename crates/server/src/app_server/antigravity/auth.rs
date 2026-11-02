//! Private, generation-bound credentials for the Antigravity cloud source.

use super::{cloud, config_root};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::OnceLock;
use tokio::sync::Mutex;

// The browser sign-in uses an installed-application OAuth client supplied at
// build time. Source builds without it keep the local-login reuse path and
// refuse browser sign-in and token renewal with an explicit error instead.
const CLIENT_ID: Option<&str> = option_env!("KLAUS_ANTIGRAVITY_OAUTH_CLIENT_ID");
const CLIENT_SECRET: Option<&str> = option_env!("KLAUS_ANTIGRAVITY_OAUTH_CLIENT_SECRET");
pub(super) const CLIENT_UNCONFIGURED: &str = "antigravity_login_unconfigured";
const REFRESH_SKEW_MS: i64 = 120_000;

#[derive(Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Credential {
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub expires_at: Option<i64>,
    pub account: Option<String>,
    pub auth_method: String,
    pub project: Option<String>,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct StoredCredential {
    generation: String,
    credential: Credential,
    #[serde(default)]
    needs_login: bool,
}

pub(super) fn credential_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn refresh_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

pub(super) fn path() -> PathBuf {
    config_root().join("antigravity-auth.json")
}

fn read() -> Option<StoredCredential> {
    use std::io::Read;
    let mut raw = Vec::new();
    std::fs::File::open(path())
        .ok()?
        .take(64 * 1024 + 1)
        .read_to_end(&mut raw)
        .ok()?;
    if raw.len() > 64 * 1024 {
        return None;
    }
    serde_json::from_slice(&raw).ok()
}

fn write(stored: &StoredCredential) -> Result<(), String> {
    use std::io::Write;
    let path = path();
    let parent = path.parent().ok_or("antigravity_store_invalid")?;
    std::fs::create_dir_all(parent).map_err(|_| "antigravity_store_unavailable")?;
    let temporary = parent.join(format!(".antigravity-auth-{}.tmp", uuid::Uuid::new_v4()));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let outcome: Result<(), String> = (|| {
        let mut file = options
            .open(&temporary)
            .map_err(|_| "antigravity_store_unavailable")?;
        let bytes = serde_json::to_vec(stored).map_err(|_| "antigravity_store_invalid")?;
        if bytes.len() > 64 * 1024 {
            return Err("antigravity_credential_too_large".into());
        }
        file.write_all(&bytes)
            .map_err(|_| "antigravity_store_unavailable")?;
        file.sync_all()
            .map_err(|_| "antigravity_store_unavailable")?;
        std::fs::rename(&temporary, &path).map_err(|_| "antigravity_store_unavailable")?;
        Ok(())
    })();
    if outcome.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    outcome
}

fn clean(value: &str) -> bool {
    !value.is_empty() && value.trim() == value && !value.chars().any(char::is_control)
}

fn validate(credential: &Credential) -> Result<(), String> {
    if credential.auth_method != "consumer" {
        return Err("antigravity_auth_method_unsupported".into());
    }
    if !clean(&credential.access_token) || credential.access_token.len() > 32 * 1024 {
        return Err("antigravity_credential_invalid".into());
    }
    for value in [
        &credential.refresh_token,
        &credential.account,
        &credential.project,
        &credential.client_id,
        &credential.client_secret,
    ]
    .into_iter()
    .flatten()
    {
        if !clean(value) || value.len() > 32 * 1024 {
            return Err("antigravity_credential_invalid".into());
        }
    }
    if credential.expires_at.is_some_and(|expiry| expiry <= 0) {
        return Err("antigravity_credential_expiry_invalid".into());
    }
    Ok(())
}

pub fn status() -> Value {
    let Some(stored) = read() else {
        return json!({"ok":true,"signedIn":false,"loggedIn":false});
    };
    let credential = stored.credential;
    let needs_login = stored.needs_login
        || credential
            .expires_at
            .is_some_and(|expiry| expiry <= chrono::Utc::now().timestamp_millis())
            && credential.refresh_token.is_none();
    json!({
        "ok":true,"signedIn":true,"loggedIn":true,
        "account":credential.account,
        "project":credential.project,
        "expiresAt":credential.expires_at,
        "authMethod":credential.auth_method,
        "canRefresh":credential.refresh_token.is_some() && !needs_login,
        "needsLogin":needs_login,
    })
}

pub async fn adopt(credential: Credential) -> Result<Value, String> {
    validate(&credential)?;
    let _guard = credential_lock().lock().await;
    super::login::invalidate_pending();
    write(&StoredCredential {
        generation: uuid::Uuid::new_v4().to_string(),
        credential,
        needs_login: false,
    })?;
    super::forget_catalog();
    Ok(status())
}

pub async fn logout() -> Result<Value, String> {
    let _guard = credential_lock().lock().await;
    super::login::invalidate_pending();
    match std::fs::remove_file(path()) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err("antigravity_store_unavailable".into()),
    }
    super::forget_catalog();
    Ok(json!({"ok":true,"signedIn":false,"loggedIn":false}))
}

pub(crate) fn generation() -> Option<String> {
    Some(read()?.generation)
}

/// A client is configured only when both halves are present and non-empty.
pub(super) fn configured_client(
    id: Option<&'static str>,
    secret: Option<&'static str>,
) -> Result<(&'static str, &'static str), &'static str> {
    match (
        id.filter(|v| !v.trim().is_empty()),
        secret.filter(|v| !v.trim().is_empty()),
    ) {
        (Some(id), Some(secret)) => Ok((id, secret)),
        _ => Err(CLIENT_UNCONFIGURED),
    }
}

#[cfg(not(test))]
pub(super) fn oauth_client() -> Result<(&'static str, &'static str), &'static str> {
    configured_client(CLIENT_ID, CLIENT_SECRET)
}

/// Unit tests exercise the OAuth exchange against a local fixture, which needs a
/// client identity whether or not this build carries a real one.
#[cfg(test)]
pub(super) fn oauth_client() -> Result<(&'static str, &'static str), &'static str> {
    configured_client(CLIENT_ID, CLIENT_SECRET).or(Ok((
        "fixture-client.apps.example.invalid",
        "fixture-client-secret",
    )))
}

/// The caller holds the credential lock and the matching login publication fence.
pub(super) fn store_login(credential: Credential) -> Result<String, String> {
    validate(&credential)?;
    let generation = uuid::Uuid::new_v4().to_string();
    write(&StoredCredential {
        generation: generation.clone(),
        credential,
        needs_login: false,
    })?;
    super::forget_catalog();
    Ok(generation)
}

/// Conditional cancellation cannot remove an account imported by a newer action.
pub(super) fn remove_generation(expected: &str) -> Result<(), String> {
    if generation().as_deref() != Some(expected) {
        return Ok(());
    }
    match std::fs::remove_file(path()) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err("antigravity_store_unavailable".into()),
    }
    super::forget_catalog();
    Ok(())
}

pub(crate) fn is_current(generation: &str) -> bool {
    read().is_some_and(|stored| stored.generation == generation)
}

pub(super) fn account() -> Option<String> {
    read()?.credential.account
}

pub(super) async fn credential(expected: Option<&str>) -> Result<Credential, String> {
    let mut stored = {
        let _guard = credential_lock().lock().await;
        read().ok_or("antigravity_not_signed_in")?
    };
    if expected.is_some_and(|generation| stored.generation != generation) {
        return Err("antigravity_credential_changed".into());
    }
    if stored.needs_login {
        return Err("antigravity_login_expired".into());
    }
    let now = chrono::Utc::now().timestamp_millis();
    if stored
        .credential
        .expires_at
        .is_none_or(|expiry| expiry > now + REFRESH_SKEW_MS)
    {
        return Ok(stored.credential);
    }
    let _refresh_guard = refresh_lock().lock().await;
    let original_generation = stored.generation.clone();
    stored = {
        let _guard = credential_lock().lock().await;
        let current = read().ok_or("antigravity_credential_changed")?;
        if current.generation != original_generation {
            return Err("antigravity_credential_changed".into());
        }
        current
    };
    if stored.needs_login {
        return Err("antigravity_login_expired".into());
    }
    if stored
        .credential
        .expires_at
        .is_none_or(|expiry| expiry > chrono::Utc::now().timestamp_millis() + REFRESH_SKEW_MS)
    {
        return Ok(stored.credential);
    }
    let refresh_token = stored
        .credential
        .refresh_token
        .as_deref()
        .ok_or("antigravity_login_expired")?;
    let (client_id, client_secret) = match (
        stored.credential.client_id.as_deref(),
        stored.credential.client_secret.as_deref(),
    ) {
        (Some(id), Some(secret)) => (id, secret),
        _ => oauth_client().map_err(|error| error.to_string())?,
    };
    let endpoint = std::env::var("KLAUS_ANTIGRAVITY_OAUTH_URL")
        .unwrap_or_else(|_| "https://oauth2.googleapis.com/token".into());
    cloud::validate_endpoint(&endpoint)?;
    let response = cloud::http_client()?
        .post(endpoint)
        .timeout(std::time::Duration::from_secs(30))
        .form(&[
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token),
            ("client_id", client_id),
            ("client_secret", client_secret),
        ])
        .send()
        .await
        .map_err(|_| "antigravity_refresh_unavailable")?;
    let status = response.status();
    if !status.is_success() {
        let oauth_error = cloud::read_body(response, 64 * 1024)
            .await
            .ok()
            .and_then(|body| serde_json::from_slice::<Value>(&body).ok());
        let invalid_grant = oauth_error
            .as_ref()
            .and_then(|value| value.get("error"))
            .and_then(Value::as_str)
            == Some("invalid_grant");
        if status == reqwest::StatusCode::UNAUTHORIZED
            || (status.is_client_error()
                && status != reqwest::StatusCode::TOO_MANY_REQUESTS
                && invalid_grant)
        {
            let _guard = credential_lock().lock().await;
            let mut current = read().ok_or("antigravity_credential_changed")?;
            if current.generation != stored.generation {
                return Err("antigravity_credential_changed".into());
            }
            current.needs_login = true;
            current.credential.refresh_token = None;
            write(&current)?;
            return Err("antigravity_login_expired".into());
        }
        return Err("antigravity_refresh_unavailable".into());
    }
    let bytes = cloud::read_body(response, 256 * 1024).await?;
    let result: Value =
        serde_json::from_slice(&bytes).map_err(|_| "antigravity_refresh_invalid")?;
    let token = result
        .get("access_token")
        .and_then(Value::as_str)
        .filter(|token| clean(token))
        .ok_or("antigravity_refresh_invalid")?;
    let expires_in = result
        .get("expires_in")
        .and_then(Value::as_i64)
        .filter(|seconds| *seconds > 0 && *seconds <= 31_536_000)
        .ok_or("antigravity_refresh_invalid")?;
    stored.credential.access_token = token.to_string();
    stored.credential.expires_at = Some(now.saturating_add(expires_in.saturating_mul(1000)));
    if let Some(rotated) = result
        .get("refresh_token")
        .and_then(Value::as_str)
        .filter(|token| clean(token))
    {
        stored.credential.refresh_token = Some(rotated.to_string());
    }
    let _guard = credential_lock().lock().await;
    let current = read().ok_or("antigravity_credential_changed")?;
    if current.generation != stored.generation {
        return Err("antigravity_credential_changed".into());
    }
    stored.credential.project = current.credential.project;
    write(&stored)?;
    Ok(stored.credential)
}

pub(super) async fn set_project(generation: &str, project: &str) -> Result<(), String> {
    if !clean(project) {
        return Err("antigravity_project_invalid".into());
    }
    let _guard = credential_lock().lock().await;
    let mut stored = read().ok_or("antigravity_not_signed_in")?;
    if stored.generation != generation {
        return Err("antigravity_credential_changed".into());
    }
    stored.credential.project = Some(project.to_string());
    write(&stored)
}

/// Drop a cached project so the next resolution asks the service again. A project
/// is the one identity kept without revalidation; a NOT_FOUND refusal is the
/// signal that it may no longer resolve.
pub(super) async fn clear_project(generation: &str) -> Result<(), String> {
    let _guard = credential_lock().lock().await;
    let mut stored = read().ok_or("antigravity_not_signed_in")?;
    if stored.generation != generation {
        return Err("antigravity_credential_changed".into());
    }
    if stored.credential.project.take().is_none() {
        return Ok(());
    }
    write(&stored)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consumer_credentials_reject_unsupported_auth_and_control_characters() {
        let mut credential: Credential = serde_json::from_value(json!({
            "accessToken":"fixture-access", "authMethod":"consumer"
        }))
        .unwrap();
        assert!(validate(&credential).is_ok());
        credential.auth_method = "enterprise".into();
        assert_eq!(
            validate(&credential),
            Err("antigravity_auth_method_unsupported".into())
        );
        credential.auth_method = "consumer".into();
        credential.access_token.push('\n');
        assert_eq!(
            validate(&credential),
            Err("antigravity_credential_invalid".into())
        );
    }
}
