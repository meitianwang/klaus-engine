//! Account authorization, model discovery and bounded media, voice and web services.
//! Chat uses the engine's own provider; independent account operations do not
//! create another agent runtime.

pub mod auth;
pub mod catalog;
mod media;
mod metadata;
pub(crate) mod native;
mod streaming;
mod voice;
mod web;
pub use media::{capabilities, generate_image, generate_video};
pub use native::NativeResponse;
pub(crate) use native::{Operation, Operations};
pub use streaming::stream_dictation;
pub use voice::{synthesize_speech, transcribe_audio};
pub use web::{fetch_web_page, search};

use serde_json::{json, Value};
use std::path::{Path, PathBuf};

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

/// The catalog key a `workbuddy:<key>` selection puts on the wire.
///
/// An explicit catalog key stands as itself. A bare tier word or an unmapped tier
/// names the catalog head: the picker offers no synthetic automatic route, so the
/// service default is the entry WorkBuddy itself marks `isDefault`.
pub fn wire_model(selection: &str) -> String {
    match selection {
        "" | "auto" | "high" | "medium" | "low" => {
            catalog::first_model_key().unwrap_or_else(|| auth::DEFAULT_SELECTION.to_string())
        }
        other => other.to_string(),
    }
}

pub async fn resolved_model(selection: &str) -> String {
    let requested = wire_model(selection);
    match catalog::fetch_models().await {
        Ok(models) if !models.is_empty() => {
            let requested = match selection {
                "" | "auto" | "high" | "medium" | "low" => "",
                _ => requested.as_str(),
            };
            catalog::resolve_model_key(&models, requested)
        }
        _ => requested,
    }
}

/// The provider and auth a `workbuddy:<key>` selection resolves to.
///
/// The base URL carries the gateway's `/v2` prefix because the OpenAI chat
/// provider appends `/chat/completions` itself, and that is the whole path the
/// gateway serves.
pub async fn resolve(selection: &str) -> Result<(Value, Value), String> {
    if auth::access_token().is_err() {
        return Err("not signed in to a WorkBuddy subscription".to_string());
    }
    let base_url = format!("{}/v2", auth::api_base());
    let model = resolved_model(selection).await;
    let provider = json!({
        "type": "openai",
        "api": "chat",
        "model": model,
        "baseUrl": base_url,
        // This gateway rejects developer messages with a generic channel error.
        // Keep every trusted instruction and project only its supported wire role.
        "compatibility": {
            "instructionRole": "system",
            "useModelDefaultEffort": true,
        },
    });
    let token = auth::access_token().map_err(|error| error.message)?;
    let auth = json!({ "type": "static_bearer", "token": token.access_token });
    Ok((provider, auth))
}

pub use auth::{
    access_token, adopt_credential, import_local, logout, poll, refresh_token_now, start, usage,
    WorkBuddyToken,
};
pub use catalog::{fetch_models, fetch_models_warm, model_capabilities, WorkBuddyError};

pub async fn ensure_fresh_token() -> Result<(), String> {
    auth::ensure_fresh_token().await
}

pub async fn lookup_model(key: &str) -> Option<Value> {
    catalog::lookup_model(key).await
}

pub async fn model_context_window(key: &str) -> Option<i64> {
    let entry = catalog::lookup_model(key).await?;
    Some(catalog::context_window(Some(&entry)))
}

#[cfg(test)]
pub(crate) fn test_env_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}
