//! QoderWork subscription support.
//!
//! QoderWork is the international sibling of QwenWork: it signs with the same
//! COSY scheme and the same six-field account shape, but it talks to the Qoder
//! family's gateway rather than to QwenWork's own. Its model catalog is served on
//! the `qwork` scene, and its login is a PKCE device flow against Qoder's OpenAPI
//! host under QoderWork's own OAuth client and deep link — so a QoderWork login is
//! a distinct session from a Qoder IDE login on the same machine.
//!
//! The signer, the loopback bridge and the shared wire types are the Qoder
//! module's, re-exported below so this module's own files read the same way they
//! do there. What is QoderWork's own is the credential store, the account
//! endpoints, the catalog scene and the signing profile.

pub mod auth;
pub mod catalog;

use serde_json::{json, Value};
use std::path::{Path, PathBuf};

// The signer and the bridge are shared with the Qoder source: one COSY
// implementation and one loopback translator, per source instance rather than per
// source copy. Re-exported so `auth` and `catalog` address them as `super::`.
pub(crate) use super::qoder::bridge;
pub(crate) use super::qoder::sign;
pub(crate) use super::qoder::{random_bytes, uuid_v4};

static CONFIG_ROOT: std::sync::RwLock<Option<PathBuf>> = std::sync::RwLock::new(None);

pub fn set_config_root(root: &Path) {
    if let Ok(mut guard) = CONFIG_ROOT.write() {
        *guard = Some(root.to_path_buf());
    }
}

pub(crate) fn config_root() -> PathBuf {
    if let Ok(guard) = CONFIG_ROOT.read() {
        if let Some(root) = guard.as_ref() {
            return root.clone();
        }
    }
    match std::env::var("KLAUS_CONFIG_DIR") {
        Ok(value) if !value.trim().is_empty() => PathBuf::from(value.trim()),
        _ => PathBuf::from(std::env::var("HOME").unwrap_or_default()).join(".klaus"),
    }
}

/// Whether a selection names no model of its own: a bare default or a tier word
/// the host mapped to nothing. Only these follow the catalog's default entry;
/// an explicit catalog key stands as itself.
fn is_bare_default(selection: &str) -> bool {
    matches!(selection, "" | "auto" | "high" | "medium" | "low")
}

/// The model key a `qoderwork:<key>` selection puts on the wire.
///
/// An explicit catalog key stands as itself. A bare tier word or an unmapped tier
/// names the client's own default key when the catalog serves it, then the entry
/// the service marks default, then the catalog head, and falls back to the client
/// default key outright when no catalog is reachable.
pub fn wire_model(selection: &str) -> String {
    match selection {
        "" | "auto" | "high" | "medium" | "low" => {
            catalog::default_model_key().unwrap_or_else(|| catalog::DEFAULT_MODEL.to_string())
        }
        other => other.to_string(),
    }
}

/// The key the service actually served the request under, once the catalog is in
/// hand.
///
/// A bare default is handed to the catalog as a bare default rather than as the
/// key [`wire_model`] guessed from a colder cache: pre-resolving it would make the
/// catalog's exact-key match win, so a picker that painted one model while cold
/// would keep running on it after the live catalog declared a different default.
pub async fn resolved_model(selection: &str) -> String {
    let requested = wire_model(selection);
    if !is_bare_default(selection) {
        return match catalog::fetch_models().await {
            Ok(models) if !models.is_empty() => catalog::resolve_model_key(&models, &requested),
            _ => requested,
        };
    }
    // A bare default is resolved against a catalog that only this install can
    // vouch for, so the model the account runs on is never picked by another
    // product's cache directory.
    match catalog::models_for_default().await {
        models if !models.is_empty() => catalog::resolve_model_key(&models, "auto"),
        _ => requested,
    }
}

/// The provider and auth a `qoderwork:<key>` selection resolves to.
///
/// The bridge is started on demand and the engine's OpenAI chat provider is
/// pointed at it. The bearer is the bridge's own per-process token, not a
/// QoderWork credential: the signing happens inside the bridge, so no
/// subscription secret is handed to the provider layer at all.
pub async fn resolve(selection: &str) -> Result<(Value, Value), String> {
    if catalog::access_token().is_err() {
        return Err("not signed in to a QoderWork subscription".to_string());
    }
    let (base_url, token) = bridge::ensure_started_for(bridge::BridgeSource::QoderWork)
        .await
        .map_err(|error| format!("cannot start the QoderWork bridge: {error}"))?;
    let model = resolved_model(selection).await;
    let provider = json!({
        "type": "openai",
        "api": "chat",
        "model": model,
        "baseUrl": base_url,
    });
    let auth = json!({ "type": "static_bearer", "token": token });
    Ok((provider, auth))
}

/// Whether a QoderWork login is on disk, for a status report.
pub fn signed_in() -> bool {
    catalog::access_token().is_ok()
}

pub use auth::{adopt_credential, import_local, logout, poll, start, usage};
pub use catalog::{
    access_token, auth_status, fetch_models, fetch_models_warm, model_capabilities, QoderWorkError,
    QoderWorkToken,
};

/// Generate an image through the subscription's own endpoint. Answers the encoded
/// image bytes, which is what the caller writes or forwards.
pub async fn generate_image(
    prompt: &str,
    size: Option<&str>,
    session_id: &str,
) -> Result<Vec<u8>, QoderWorkError> {
    catalog::generate_image(prompt, size, session_id).await
}

/// The tier the next request should ask for.
///
/// Pushed in-process as the composer's default. Explicit per-request reasoning
/// intent takes priority, so a summary can disable thinking independently.
/// A no-op until the bridge is running.
pub fn set_thinking_level(level: Option<String>) {
    bridge::set_thinking_level_for(bridge::BridgeSource::QoderWork, level);
}

/// Refresh the stored token when it is close to expiry. Awaited immediately
/// before anything spends the token, so an expiring credential is never handed to
/// the signer or to an API call; the failure then reads as "sign in again"
/// instead of as a mid-stream rejection.
pub async fn ensure_fresh_token() -> Result<(), String> {
    auth::ensure_fresh_token().await
}

/// One catalog entry, by key.
pub async fn lookup_model(key: &str) -> Option<Value> {
    catalog::lookup_model(key).await
}

/// The context window a catalog key advertises.
pub async fn model_context_window(key: &str) -> Option<i64> {
    catalog::model_context_window(key).await
}

/// Serializes the tests that repoint the credential root or `QODERWORK_HOME`.
/// Both are per-process, so two tests fixturing them at once would read each
/// other's directory. Async-aware because those tests hold it across an await.
#[cfg(test)]
pub(crate) fn test_env_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}
