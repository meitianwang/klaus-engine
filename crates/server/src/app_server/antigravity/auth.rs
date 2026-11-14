//! Private, generation-bound account queue for the Antigravity cloud source.

use super::{cloud, config_root};
use klaus_agent_core::provider::rotation::RotateOutcome;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::{Duration, SystemTime};
use tokio::sync::Mutex;

// The browser sign-in uses an installed-application OAuth client supplied at
// build time. Source builds without it keep the local-login reuse path and
// refuse browser sign-in and token renewal with an explicit error instead.
const CLIENT_ID: Option<&str> = option_env!("KLAUS_ANTIGRAVITY_OAUTH_CLIENT_ID");
const CLIENT_SECRET: Option<&str> = option_env!("KLAUS_ANTIGRAVITY_OAUTH_CLIENT_SECRET");
pub(super) const CLIENT_UNCONFIGURED: &str = "antigravity_login_unconfigured";
const REFRESH_SKEW_MS: i64 = 120_000;
const MAX_STORE_BYTES: usize = 512 * 1024;
const MAX_ACCOUNTS: usize = 32;

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
pub(super) struct StoredAccount {
    pub id: String,
    pub generation: String,
    pub credential: Credential,
    #[serde(default)]
    pub needs_login: bool,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredQueue {
    accounts: Vec<StoredAccount>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LegacyStoredCredential {
    generation: String,
    credential: Credential,
    #[serde(default)]
    needs_login: bool,
}

pub(super) struct LoginWriteOutcome {
    pub account_id: String,
    pub generation: String,
    pub updated_existing: bool,
    pub previous_count: usize,
}

pub(super) fn credential_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn refresh_lock_for(generation: &str) -> Arc<Mutex<()>> {
    static LOCKS: OnceLock<StdMutex<HashMap<String, Arc<Mutex<()>>>>> = OnceLock::new();
    let mut map = LOCKS
        .get_or_init(|| StdMutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    if map.len() > 128 {
        map.retain(|_, lock| Arc::strong_count(lock) > 1);
    }
    map.entry(generation.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

pub(super) fn path() -> PathBuf {
    config_root().join("antigravity-auth.json")
}

fn lock_dir() -> PathBuf {
    config_root()
        .join(".locks")
        .join("antigravity-accounts-rotation.lock")
}

fn with_store_file_lock<T>(run: impl FnOnce() -> Result<T, String>) -> Result<T, String> {
    let dir = lock_dir();
    if let Some(parent) = dir.parent() {
        std::fs::create_dir_all(parent).map_err(|_| "antigravity_store_unavailable")?;
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(6);
    loop {
        match std::fs::create_dir(&dir) {
            Ok(()) => break,
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                let stale = std::fs::metadata(&dir)
                    .and_then(|meta| meta.modified())
                    .ok()
                    .and_then(|modified| SystemTime::now().duration_since(modified).ok())
                    .is_some_and(|age| age > Duration::from_secs(120));
                if stale {
                    let _ = std::fs::remove_dir(&dir);
                    continue;
                }
                if std::time::Instant::now() >= deadline {
                    return Err("antigravity_store_unavailable".into());
                }
                std::thread::sleep(Duration::from_millis(25));
            }
            Err(_) => return Err("antigravity_store_unavailable".into()),
        }
    }
    let result = run();
    let _ = std::fs::remove_dir(&dir);
    result
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

pub(super) fn accounts() -> Vec<StoredAccount> {
    use std::io::Read;
    let mut raw = Vec::new();
    let Ok(file) = std::fs::File::open(path()) else {
        return Vec::new();
    };
    if file
        .take(MAX_STORE_BYTES as u64 + 1)
        .read_to_end(&mut raw)
        .is_err()
        || raw.len() > MAX_STORE_BYTES
    {
        return Vec::new();
    }
    let Ok(value) = serde_json::from_slice::<Value>(&raw) else {
        return Vec::new();
    };
    if let Some(entries) = value.get("accounts").and_then(Value::as_array) {
        return entries
            .iter()
            .filter_map(|entry| serde_json::from_value::<StoredAccount>(entry.clone()).ok())
            .filter(|entry| {
                clean(&entry.id)
                    && entry.id.len() <= 128
                    && clean(&entry.generation)
                    && entry.generation.len() <= 128
                    && validate(&entry.credential).is_ok()
            })
            .take(MAX_ACCOUNTS)
            .collect();
    }
    if let Ok(legacy) = serde_json::from_value::<LegacyStoredCredential>(value) {
        if clean(&legacy.generation)
            && legacy.generation.len() <= 128
            && validate(&legacy.credential).is_ok()
        {
            return vec![StoredAccount {
                id: "legacy".into(),
                generation: legacy.generation,
                credential: legacy.credential,
                needs_login: legacy.needs_login,
            }];
        }
    }
    Vec::new()
}

fn write_accounts_unlocked(entries: &[StoredAccount]) -> Result<(), String> {
    use std::io::Write;
    if entries.len() > MAX_ACCOUNTS {
        return Err("antigravity_account_limit_reached".into());
    }
    let path = path();
    if entries.is_empty() {
        return match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(_) => Err("antigravity_store_unavailable".into()),
        };
    }
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
    let queue = StoredQueue {
        accounts: entries.to_vec(),
    };
    let outcome: Result<(), String> = (|| {
        let mut file = options
            .open(&temporary)
            .map_err(|_| "antigravity_store_unavailable")?;
        let bytes = serde_json::to_vec(&queue).map_err(|_| "antigravity_store_invalid")?;
        if bytes.len() > MAX_STORE_BYTES {
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

fn entry_needs_login(entry: &StoredAccount, now: i64) -> bool {
    entry.needs_login
        || (entry
            .credential
            .expires_at
            .is_some_and(|expiry| expiry <= now)
            && entry.credential.refresh_token.is_none())
}

pub fn status() -> Value {
    let entries = accounts();
    let Some(head) = entries.first() else {
        return json!({"ok":true,"signedIn":false,"loggedIn":false,"accounts":[]});
    };
    let now = chrono::Utc::now().timestamp_millis();
    let head_needs_login = entry_needs_login(head, now);
    let rows: Vec<Value> = entries
        .iter()
        .enumerate()
        .map(|(index, entry)| {
            let email = entry.credential.account.clone().unwrap_or_default();
            json!({
                "id": entry.id,
                "email": email,
                "displayName": "",
                "isCurrent": index == 0,
                "needsLogin": entry_needs_login(entry, now),
            })
        })
        .collect();
    json!({
        "ok": true,
        "signedIn": true,
        "loggedIn": true,
        "account": head.credential.account,
        "project": head.credential.project,
        "expiresAt": head.credential.expires_at,
        "authMethod": head.credential.auth_method,
        "canRefresh": head.credential.refresh_token.is_some() && !head_needs_login,
        "needsLogin": head_needs_login,
        "accounts": rows,
    })
}

fn upsert_unlocked(mut credential: Credential) -> Result<LoginWriteOutcome, String> {
    validate(&credential)?;
    with_store_file_lock(|| {
        let mut entries = accounts();
        let previous_count = entries.len();
        for existing in &mut entries {
            if existing.id == "legacy" {
                existing.id = uuid::Uuid::new_v4().to_string();
            }
        }
        let incoming_email = credential
            .account
            .as_deref()
            .map(str::trim)
            .filter(|email| !email.is_empty());
        let match_index = if let Some(email) = incoming_email {
            entries
                .iter()
                .position(|entry| {
                    entry
                        .credential
                        .account
                        .as_deref()
                        .is_some_and(|stored| stored.eq_ignore_ascii_case(email))
                })
                .or_else(|| {
                    (entries.len() == 1 && entries[0].credential.account.is_none()).then_some(0)
                })
        } else {
            let refresh = credential
                .refresh_token
                .as_deref()
                .filter(|token| !token.is_empty());
            entries
                .iter()
                .position(|entry| {
                    refresh.is_some() && entry.credential.refresh_token.as_deref() == refresh
                })
                .or_else(|| {
                    entries
                        .iter()
                        .position(|entry| entry.credential.account.is_none())
                })
        };

        let generation = uuid::Uuid::new_v4().to_string();
        if let Some(index) = match_index {
            let same_named_account = incoming_email.is_some()
                && entries[index]
                    .credential
                    .account
                    .as_deref()
                    .zip(incoming_email)
                    .is_some_and(|(left, right)| left.eq_ignore_ascii_case(right));
            if credential.project.is_none() && same_named_account {
                credential.project = entries[index].credential.project.clone();
            }
            if credential.refresh_token.is_none() && same_named_account {
                credential.refresh_token = entries[index].credential.refresh_token.clone();
            }
            let account_id = entries[index].id.clone();
            let old_generation = entries[index].generation.clone();
            entries[index] = StoredAccount {
                id: account_id.clone(),
                generation: generation.clone(),
                credential,
                needs_login: false,
            };
            if index > 0 && entries[0].needs_login {
                let refreshed = entries.remove(index);
                entries.insert(0, refreshed);
            }
            write_accounts_unlocked(&entries)?;
            super::prune_generation(&old_generation);
            return Ok(LoginWriteOutcome {
                account_id,
                generation,
                updated_existing: true,
                previous_count,
            });
        }

        if entries.len() >= MAX_ACCOUNTS {
            return Err("antigravity_account_limit_reached".into());
        }
        let account_id = uuid::Uuid::new_v4().to_string();
        let added = StoredAccount {
            id: account_id.clone(),
            generation: generation.clone(),
            credential,
            needs_login: false,
        };
        if entries.first().is_some_and(|head| head.needs_login) {
            entries.insert(0, added);
        } else {
            entries.push(added);
        }
        write_accounts_unlocked(&entries)?;
        Ok(LoginWriteOutcome {
            account_id,
            generation,
            updated_existing: false,
            previous_count,
        })
    })
}

pub async fn adopt(credential: Credential) -> Result<Value, String> {
    validate(&credential)?;
    let _guard = credential_lock().lock().await;
    super::login::invalidate_pending();
    let outcome = upsert_unlocked(credential)?;
    super::sync_head_catalog();
    let mut reply = status();
    reply["accountId"] = json!(outcome.account_id);
    reply["updatedExisting"] = json!(outcome.updated_existing);
    reply["previousCount"] = json!(outcome.previous_count);
    if !outcome.updated_existing {
        reply["addedAccountId"] = json!(outcome.account_id);
    }
    Ok(reply)
}

pub async fn logout() -> Result<Value, String> {
    logout_account(None).await
}

pub async fn logout_account(account_id: Option<&str>) -> Result<Value, String> {
    let target = account_id.map(str::trim).filter(|id| !id.is_empty());
    let _guard = credential_lock().lock().await;
    if let Some(target_id) = target {
        let mut removed_generation = None;
        let found = with_store_file_lock(|| {
            let mut entries = accounts();
            let before = entries.len();
            entries.retain(|entry| {
                if entry.id == target_id {
                    removed_generation = Some(entry.generation.clone());
                    false
                } else {
                    true
                }
            });
            if entries.len() == before {
                return Ok(false);
            }
            write_accounts_unlocked(&entries)?;
            Ok(true)
        })?;
        if !found {
            return Ok(json!({"ok":false,"error":"antigravity_account_not_found"}));
        }
        if let Some(generation) = removed_generation {
            super::prune_generation(&generation);
        }
        if accounts().is_empty() {
            super::forget_catalog();
        } else {
            super::sync_head_catalog();
        }
        return Ok(status());
    }
    super::login::invalidate_pending();
    with_store_file_lock(|| write_accounts_unlocked(&[]))?;
    super::forget_catalog();
    Ok(json!({"ok":true,"signedIn":false,"loggedIn":false,"accounts":[]}))
}

pub async fn switch_account(account_id: &str) -> Result<Value, String> {
    let target_id = account_id.trim();
    if target_id.is_empty() || !clean(target_id) {
        return Ok(json!({"ok":false,"error":"antigravity_account_not_found"}));
    }
    let _guard = credential_lock().lock().await;
    let switched = with_store_file_lock(|| {
        let mut entries = accounts();
        let Some(index) = entries.iter().position(|entry| entry.id == target_id) else {
            return Ok(None);
        };
        if index > 0 {
            let selected = entries.remove(index);
            entries.insert(0, selected);
            write_accounts_unlocked(&entries)?;
            return Ok(Some(true));
        }
        Ok(Some(false))
    })?;
    let Some(changed_head) = switched else {
        return Ok(json!({"ok":false,"error":"antigravity_account_not_found"}));
    };
    if changed_head {
        super::sync_head_catalog();
    }
    let mut reply = status();
    reply["accountId"] = json!(target_id);
    Ok(reply)
}

pub(super) async fn rotate(failed_generation: &str) -> RotateOutcome {
    let _guard = credential_lock().lock().await;
    let outcome = with_store_file_lock(|| {
        let mut entries = accounts();
        if entries.len() < 2 {
            return Ok(RotateOutcome::Exhausted);
        }
        let head_generation = entries[0].generation.clone();
        if head_generation != failed_generation {
            return if entries
                .iter()
                .any(|entry| entry.generation == failed_generation)
            {
                Ok(RotateOutcome::AlreadyAdvanced(head_generation))
            } else {
                Ok(RotateOutcome::Exhausted)
            };
        }
        let failed = entries.remove(0);
        entries.push(failed);
        let next = entries[0].generation.clone();
        write_accounts_unlocked(&entries)?;
        Ok(RotateOutcome::Rotated(next))
    });
    match outcome {
        Ok(outcome) => {
            if matches!(outcome, RotateOutcome::Rotated(_)) {
                super::sync_head_catalog();
            }
            outcome
        }
        Err(_) => RotateOutcome::Exhausted,
    }
}

pub(crate) fn generation() -> Option<String> {
    accounts().into_iter().next().map(|entry| entry.generation)
}

pub(super) fn generations() -> Vec<String> {
    accounts()
        .into_iter()
        .map(|entry| entry.generation)
        .collect()
}

pub(super) fn account_count() -> usize {
    accounts().len()
}

pub(super) fn store_fingerprint() -> Vec<(String, String)> {
    accounts()
        .into_iter()
        .map(|entry| (entry.id, entry.generation))
        .collect()
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
pub(super) fn store_login(credential: Credential) -> Result<LoginWriteOutcome, String> {
    let outcome = upsert_unlocked(credential)?;
    super::sync_head_catalog();
    Ok(outcome)
}

/// Conditional cancellation removes only the account generation committed by this flow.
pub(super) fn remove_generation(expected: &str) -> Result<(), String> {
    let changed = with_store_file_lock(|| {
        let mut entries = accounts();
        let before = entries.len();
        entries.retain(|entry| entry.generation != expected);
        if entries.len() == before {
            return Ok(false);
        }
        write_accounts_unlocked(&entries)?;
        Ok(true)
    })?;
    if changed {
        super::prune_generation(expected);
        if accounts().is_empty() {
            super::forget_catalog();
        } else {
            super::sync_head_catalog();
        }
    }
    Ok(())
}

/// Whether `generation` still belongs to a stored account in the pool.
pub(crate) fn is_current(generation: &str) -> bool {
    accounts()
        .iter()
        .any(|entry| entry.generation == generation)
}

/// Whether `generation` is currently the active head of the pool.
pub(super) fn is_head_generation(generation: &str) -> bool {
    accounts()
        .first()
        .is_some_and(|head| head.generation == generation)
}

pub(crate) async fn credential(expected: Option<&str>) -> Result<Credential, String> {
    credential_inner(expected, false).await
}

pub(super) async fn force_refresh(generation: &str) -> Result<Credential, String> {
    credential_inner(Some(generation), true).await
}

/// Resolve the active head's credential, advancing past expired queued heads when
/// spare accounts remain in the pool.
pub(super) async fn active_or_rotated_credential() -> Result<Credential, String> {
    let mut attempted = Vec::new();
    loop {
        let head = generation().ok_or("antigravity_not_signed_in")?;
        match credential(Some(&head)).await {
            Ok(cred) => return Ok(cred),
            Err(error) if error == "antigravity_login_expired" && account_count() > 1 => {
                if attempted.contains(&head) {
                    return Err(error);
                }
                attempted.push(head.clone());
                match rotate(&head).await {
                    RotateOutcome::Rotated(next) | RotateOutcome::AlreadyAdvanced(next)
                        if !attempted.contains(&next) =>
                    {
                        continue;
                    }
                    _ => return Err(error),
                }
            }
            Err(error) => return Err(error),
        }
    }
}

async fn credential_inner(expected: Option<&str>, force: bool) -> Result<Credential, String> {
    let mut target = {
        let _guard = credential_lock().lock().await;
        let entries = accounts();
        if entries.is_empty() {
            return Err("antigravity_not_signed_in".into());
        }
        match expected {
            Some(generation) => entries
                .into_iter()
                .find(|entry| entry.generation == generation)
                .ok_or_else(|| "antigravity_credential_changed".to_string())?,
            None => entries.into_iter().next().unwrap(),
        }
    };
    if target.needs_login {
        return Err("antigravity_login_expired".into());
    }
    let now = chrono::Utc::now().timestamp_millis();
    if !force
        && target
            .credential
            .expires_at
            .is_none_or(|expiry| expiry > now + REFRESH_SKEW_MS)
    {
        return Ok(target.credential);
    }
    let original_generation = target.generation.clone();
    let lock = refresh_lock_for(&original_generation);
    let _refresh_guard = lock.lock().await;
    let pre_refresh_access = target.credential.access_token.clone();
    target = {
        let _guard = credential_lock().lock().await;
        accounts()
            .into_iter()
            .find(|entry| entry.generation == original_generation)
            .ok_or_else(|| "antigravity_credential_changed".to_string())?
    };
    if target.needs_login {
        return Err("antigravity_login_expired".into());
    }
    if (!force
        && target
            .credential
            .expires_at
            .is_none_or(|expiry| expiry > chrono::Utc::now().timestamp_millis() + REFRESH_SKEW_MS))
        || (force && target.credential.access_token != pre_refresh_access)
    {
        return Ok(target.credential);
    }
    let refresh_token = target
        .credential
        .refresh_token
        .as_deref()
        .ok_or("antigravity_login_expired")?;
    let (client_id, client_secret) = match (
        target.credential.client_id.as_deref(),
        target.credential.client_secret.as_deref(),
    ) {
        (Some(id), Some(secret)) => (id, secret),
        _ => oauth_client().map_err(|error| error.to_string())?,
    };
    let endpoint = std::env::var("KLAUS_ANTIGRAVITY_OAUTH_URL")
        .unwrap_or_else(|_| "https://oauth2.googleapis.com/token".into());
    cloud::validate_endpoint(&endpoint)?;
    let started = chrono::Utc::now().timestamp_millis();
    let response = cloud::http_client()?
        .post(endpoint)
        .timeout(Duration::from_secs(30))
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
            with_store_file_lock(|| {
                let mut entries = accounts();
                let current = entries
                    .iter_mut()
                    .find(|entry| entry.generation == original_generation)
                    .ok_or_else(|| "antigravity_credential_changed".to_string())?;
                current.needs_login = true;
                current.credential.refresh_token = None;
                write_accounts_unlocked(&entries)
            })?;
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
    target.credential.access_token = token.to_string();
    target.credential.expires_at = Some(started.saturating_add(expires_in.saturating_mul(1000)));
    if let Some(rotated) = result
        .get("refresh_token")
        .and_then(Value::as_str)
        .filter(|token| clean(token))
    {
        target.credential.refresh_token = Some(rotated.to_string());
    }
    let _guard = credential_lock().lock().await;
    with_store_file_lock(|| {
        let mut entries = accounts();
        let current = entries
            .iter_mut()
            .find(|entry| entry.generation == original_generation)
            .ok_or_else(|| "antigravity_credential_changed".to_string())?;
        target.credential.project = current.credential.project.clone();
        current.credential = target.credential.clone();
        current.needs_login = false;
        write_accounts_unlocked(&entries)
    })?;
    Ok(target.credential)
}

pub(super) async fn set_project(generation: &str, project: &str) -> Result<(), String> {
    if !clean(project) {
        return Err("antigravity_project_invalid".into());
    }
    let _guard = credential_lock().lock().await;
    with_store_file_lock(|| {
        let mut entries = accounts();
        if entries.is_empty() {
            return Err("antigravity_not_signed_in".into());
        }
        let entry = entries
            .iter_mut()
            .find(|entry| entry.generation == generation)
            .ok_or_else(|| "antigravity_credential_changed".to_string())?;
        entry.credential.project = Some(project.to_string());
        write_accounts_unlocked(&entries)
    })
}

/// Drop a cached project so the next resolution asks the service again. A project
/// is the one identity kept without revalidation; a NOT_FOUND refusal is the
/// signal that it may no longer resolve.
pub(super) async fn clear_project(generation: &str) -> Result<(), String> {
    let _guard = credential_lock().lock().await;
    with_store_file_lock(|| {
        let mut entries = accounts();
        if entries.is_empty() {
            return Err("antigravity_not_signed_in".into());
        }
        let entry = entries
            .iter_mut()
            .find(|entry| entry.generation == generation)
            .ok_or_else(|| "antigravity_credential_changed".to_string())?;
        if entry.credential.project.take().is_none() {
            return Ok(());
        }
        write_accounts_unlocked(&entries)
    })
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
