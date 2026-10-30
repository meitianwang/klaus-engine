//! QwenWork (千问办公) subscription support.
//!
//! QwenWork uses the same COSY-signed agent wire as Qoder with a distinct
//! product scene, account store and gateway. The shared loopback bridge keeps
//! Klaus as the only agent runtime; no QwenWork or Qoder CLI is nested inside a
//! session.

pub mod auth;
pub mod capabilities;
pub mod catalog;

use serde_json::{json, Value};
use std::path::{Path, PathBuf};

static CONFIG_ROOT: std::sync::RwLock<Option<PathBuf>> = std::sync::RwLock::new(None);

pub fn set_config_root(root: &Path) {
    if let Ok(mut guard) = CONFIG_ROOT.write() {
        if guard.as_deref() != Some(root) {
            *guard = Some(root.to_path_buf());
            capabilities::credential_changed();
        }
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

/// The catalog key a `qwenwork:<key>` selection puts on the wire.
///
/// An explicit catalog key stands as itself. A bare tier word or an unmapped
/// tier names the catalog's default entry: the entry whose display name is the
/// plan's flagship chat model, falling back to the list head when the lineup
/// does not carry it. With no catalog reachable the legacy `qwork-auto` alias
/// remains the fallback, and the request-time catalog normalizes it.
pub fn wire_model(selection: &str) -> String {
    match selection {
        "" | "auto" | "high" | "medium" | "low" => {
            catalog::first_model_key().unwrap_or_else(|| "qwork-auto".to_string())
        }
        other => other.to_string(),
    }
}

pub async fn resolved_model(selection: &str) -> String {
    let requested = wire_model(selection);
    match catalog::fetch_models().await {
        Ok(models) if !models.is_empty() => catalog::resolve_model_key(&models, &requested),
        _ => requested,
    }
}

pub async fn resolve(selection: &str) -> Result<(Value, Value), String> {
    if catalog::access_token().is_err() {
        return Err("not signed in to a QwenWork subscription".to_string());
    }
    let (base_url, token) =
        super::qoder::bridge::ensure_started_for(super::qoder::bridge::BridgeSource::QwenWork)
            .await
            .map_err(|error| format!("cannot start the QwenWork bridge: {error}"))?;
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

pub use auth::{adopt_credential, import_local, logout, poll, refresh_token_now, start, usage};
pub use capabilities::{generate_image, generate_music, generate_video, search, NativeResponse};
pub use catalog::{
    access_token, auth_status, fetch_models, fetch_models_warm, model_capabilities, QwenWorkError,
    QwenWorkToken,
};

pub fn set_thinking_level(level: Option<String>) {
    super::qoder::bridge::set_thinking_level_for(
        super::qoder::bridge::BridgeSource::QwenWork,
        level,
    );
}

pub async fn ensure_fresh_token() -> Result<(), String> {
    auth::ensure_fresh_token().await
}

pub async fn lookup_model(key: &str) -> Option<Value> {
    catalog::lookup_model(key).await
}

pub async fn model_context_window(key: &str) -> Option<i64> {
    catalog::model_context_window(key).await
}

#[cfg(test)]
pub(crate) fn test_env_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}
