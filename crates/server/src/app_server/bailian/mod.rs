//! Aliyun Bailian Token Plan support.
//!
//! Token Plan is a monthly Credits subscription, and it is NOT the same product as
//! the Coding Plan that the custom-provider preset already spells: different
//! gateway, different key prefix, different metering, and the two cannot be
//! migrated between. Its coding gateway serves the ordinary Anthropic wire
//! (`{base}/apps/anthropic/v1/messages`), so like Kimi there is no signer and no
//! local bridge — the engine's own Anthropic provider is pointed straight at it.
//!
//! The credential is a plan API key (`sk-sp-…`) minted in the Bailian console.
//! Token Plan offers no OAuth or device flow, so there is no login button here;
//! the settings pane takes a pasted key and the engine verifies it against the
//! inference endpoint before storing it.
//!
//! One thing worth stating because it shapes the whole module: the Anthropic
//! endpoint serves NO catalog route — `/v1/models` answers 404 there, which is
//! what Bailian's own docs say. The catalog therefore comes from the
//! OpenAI-compatible endpoint on the same plan key (`/compatible-mode/v1/models`,
//! measured 200), and the capability matrix beside it is a declared table rather
//! than something read at runtime, because that route returns ids only.

pub mod auth;
pub mod catalog;
pub mod console;

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

/// What a selection names when no catalog is reachable to answer with.
///
/// This is NOT `auto`, even though `auto` is the platform's routing model and the
/// one Bailian's own Claude Code preset names — because of a mismatch between what
/// the engine must send and what `auto` accepts.
///
/// The engine's Anthropic provider only uses a gateway's NATIVE structured output
/// when `is_first_party`, so on this third-party wire every structured request goes
/// through the forced-tool emulation, which puts `tool_choice` on the wire and turns
/// thinking off. Measured, `auto` refuses `tool_choice` outright
/// (`400 auto does not support parameter(s): tool_choice`), so any structured call
/// on it fails — and session title generation IS a structured call, on the low tier.
/// Defaulting there would break the title of every conversation.
///
/// `qwen3.8-max` is the plan's flagship chat model, measured taking the forced-tool
/// path cleanly. This is the cold-cache fallback only; with a catalog in hand
/// [`catalog::first_model_key`] resolves a bare selection to the same model. The
/// low tier that session titles run on is pinned separately in the catalog module,
/// so raising the default main model does not raise the cost of a title call for a
/// main model that cannot serve a structured request.
pub const DEFAULT_SELECTION: &str = "qwen3.8-max";

/// The catalog key a `bailian:<key>` selection puts on the wire.
///
/// A tier word resolves to the host's mapping for that tier, or to
/// [`DEFAULT_SELECTION`] when the host mapped nothing. That fallback is the same
/// safety reason the default is not `auto`: an unmapped low tier is what session
/// titles run on.
pub fn wire_model(selection: &str) -> String {
    match selection {
        "" => catalog::first_model_key().unwrap_or_else(|| DEFAULT_SELECTION.to_string()),
        // `auto` is excluded from the catalog (see catalog.rs), and an exclusion
        // that a stale persisted selection could still reach is not an exclusion.
        // Route it to the head the same way an unlisted id is routed.
        "auto" => catalog::first_model_key().unwrap_or_else(|| DEFAULT_SELECTION.to_string()),
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

/// The provider and auth a `bailian:<key>` selection resolves to.
///
/// The base URL is the Anthropic-compatible root WITHOUT a trailing `/v1`: the
/// provider appends `/v1/messages` itself, and Bailian's own FAQ warns that a
/// `/v1/` suffix makes model discovery compose `/v1/v1/models` and 404.
///
/// The key rides as `api_key`, which the Anthropic provider sends as `x-api-key`.
/// Measured, this gateway accepts either `x-api-key` or `Authorization: Bearer`
/// with the same key, so the choice is about staying on the path the provider
/// already implements for a third-party Anthropic wire.
pub async fn resolve(selection: &str) -> Result<(Value, Value), String> {
    let key = auth::api_key().ok_or_else(|| {
        "not signed in to a Bailian Token Plan — add the plan API key from the Bailian console"
            .to_string()
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

pub use auth::{
    api_base, api_key, logout, media_credential, region, set_api_key, status, usage, Region,
};
pub use catalog::{
    capabilities_of, fetch_models, fetch_models_warm, model_capabilities, structured_output_safe,
    structured_safe_fallback, BailianError,
};
pub use console::{clear_console_token, console_login_start, console_status, GatewayError};

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
