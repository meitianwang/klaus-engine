//! Volcengine Ark Coding Plan support.
//!
//! Coding Plan is a subscription sold by Volcengine for its Ark model service, and
//! it is a different product from the platform's pay-as-you-go API: the plan key is
//! minted on the plan page, it is metered against the plan's own allowance rather
//! than account balance, and the plan's terms confine it to interactive use inside
//! coding and agent tools. Its gateway serves the ordinary Anthropic wire
//! (`{base}/api/anthropic/v1/messages`), so like Kimi, Bailian and the other plan
//! sources there is no signer and no local bridge — the engine's own Anthropic
//! provider is pointed straight at it.
//!
//! The credential is a plan API key. Coding Plan offers no OAuth or device flow for
//! third-party clients, so there is no login button here: the settings pane takes a
//! pasted key and the engine verifies it against the catalog route before storing
//! it.
//!
//! Allowance reads use a separate browser OAuth login. Its expiring STS
//! credentials sign the control-plane usage request; the inference key remains
//! independent of console login and renewal failures.

pub mod auth;
pub mod catalog;
pub mod console;
pub(super) mod console_usage;

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

/// What a bare selection names when nothing else can answer.
///
/// This is the plan's coding head model. The live catalog's head still wins when
/// reachable; this is only the landing spot for a bare selection and for a tier the
/// host mapped to nothing.
pub const DEFAULT_SELECTION: &str = catalog::DEFAULT_SELECTION;

/// The catalog key an `ark:<key>` selection puts on the wire.
///
/// A tier word or `auto` resolves to the host's mapping for that tier, or to the
/// plan's head when the host mapped nothing.
pub fn wire_model(selection: &str) -> String {
    match selection {
        "" | "auto" => catalog::first_model_key().unwrap_or_else(|| DEFAULT_SELECTION.to_string()),
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

/// The provider and auth an `ark:<key>` selection resolves to.
///
/// The base URL is the Anthropic-compatible root WITHOUT a trailing `/v1`: the
/// provider appends `/v1/messages` itself, and a `/v1/` suffix would make model
/// discovery compose `/v1/v1/models` and 404.
///
/// The key rides as `api_key`, which the Anthropic provider sends as `x-api-key`.
/// Measured with a live plan key, the gateway accepts BOTH `x-api-key` and
/// `Authorization: Bearer` on the same route; `api_key` is chosen because it is the
/// engine's plain-key auth shape, and because a bearer AUTH SOURCE would leave the
/// provider with no api key — the condition under which it leads every request with
/// the first-party subscription identity block, which a third-party gateway has no
/// reason to receive.
pub async fn resolve(selection: &str) -> Result<(Value, Value), String> {
    let key = auth::api_key().ok_or_else(|| {
        "not signed in to an Ark Coding Plan — add the plan API key from the plan page".to_string()
    })?;
    let base_url = auth::api_base();
    let model = resolved_model(selection).await;
    let provider = json!({
        "type": "anthropic",
        "model": model,
        "baseUrl": base_url,
    });
    let auth = json!({ "type": "api_key", "key": key });
    Ok((provider, auth))
}

pub use auth::{api_base, api_key, logout, region, set_api_key, status, usage};
pub use catalog::{capabilities_of, fetch_models, fetch_models_warm, model_capabilities, ArkError};

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
