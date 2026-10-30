//! MiniMax M Plan support.
//!
//! M Plan is a subscription that spends ONE shared allowance across text, image,
//! audio and video, in three tiers (Go, Explore, Build). It is not the same product
//! as the platform's pay-as-you-go API: different key prefix (`sk-cp-` against
//! `sk-api-`), different balance, and the two cannot be mixed. A subscription key
//! spends plan allowance and credit packs only, never account balance.
//!
//! The text gateway serves the ordinary Anthropic wire
//! (`{base}/anthropic/v1/messages`), so like Kimi and Bailian there is no signer
//! and no local bridge — the engine's own Anthropic provider is pointed straight at
//! it. The credential is a subscription key minted in the MiniMax console; the plan
//! offers no OAuth or device flow for third-party clients, so there is no login
//! button here and the settings pane takes a pasted key.
//!
//! ## Two sites that do not share keys
//!
//! A China site and a global site both serve the plan and both mint `sk-cp-` keys,
//! so the region is not recoverable from the key. Measured, a China-subscription
//! key answers `invalid api key` on the global site. Region is therefore DETECTED
//! by trying each site rather than defaulted — see [`auth::set_api_key`].
//!
//! ## The catalog hides the plan's own flagship
//!
//! Both catalog routes list eight M2.x/M3 models and never list
//! `MiniMax-M3.1-Flash-Preview`, which is the model the plan is sold on and the one
//! that answers 200 on the same key. That model is injected at the head of every
//! projection; see [`catalog`].
//!
//! ## Video
//!
//! Explore and Build tiers add the H3 video model, and video draw is counted
//! against the WEEKLY allowance window only. The video API is the service's own
//! async task endpoint (submit, poll, download) rather than anything the engine's
//! provider layer speaks, so the host runs it directly using the stored
//! subscription key from [`auth::api_key`]. That is the same arrangement the Codex
//! subscription's image route uses, and it is why no video facade method lives
//! here.

pub mod auth;
pub mod catalog;

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

/// What a bare selection names.
///
/// This is the plan's flagship text model and the only one whose effort knob was
/// measured to actually move the thinking budget. It is safe as the MAIN model
/// because ordinary turns are not structured requests — but it is NOT structured
/// safe, so it cannot serve the low tier, which is what session titles run on.
/// `minimax_tiers` pins an unmapped low tier to a measured M2.x model instead; see
/// [`catalog::structured_safe_fallback`].
pub const DEFAULT_SELECTION: &str = "MiniMax-M3.1-Flash-Preview";

/// The catalog key a `minimax:<key>` selection puts on the wire.
///
/// A tier word or `auto` resolves to the host's mapping for that tier, or to the
/// plan's flagship when the host mapped nothing. Unlike Bailian, MiniMax publishes
/// NO `auto` routing model, so a bare `auto` must resolve to a concrete model
/// rather than reaching the wire — an unlisted model is refused with
/// `400 model not exist`. The live catalog head wins when one is reachable.
pub fn wire_model(selection: &str) -> String {
    match selection.trim() {
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

/// The provider and auth a `minimax:<key>` selection resolves to.
///
/// The base URL is the Anthropic-compatible root WITHOUT a trailing `/v1`: the
/// provider appends `/v1/messages` itself, so a `/v1` here would compose
/// `/anthropic/v1/v1/messages` and 404.
///
/// The key rides as `api_key`, which the Anthropic provider sends as `x-api-key`.
/// Measured, this gateway accepts either `x-api-key` or `Authorization: Bearer`
/// with the same key, so the choice is about staying on the path the provider
/// already implements for a third-party Anthropic wire.
pub async fn resolve(selection: &str) -> Result<(Value, Value), String> {
    let key = auth::api_key().ok_or_else(|| {
        "not signed in to a MiniMax M Plan — add the subscription key from the MiniMax console"
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
    structured_safe_fallback, MinimaxError,
};

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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_bare_selection_names_the_flagship() {
        let _lock = test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        set_config_root(temp.path());
        catalog::forget_cache();
        assert_eq!(wire_model(""), DEFAULT_SELECTION);
        assert_eq!(wire_model("   "), DEFAULT_SELECTION);
        assert_eq!(wire_model("MiniMax-M2.7"), "MiniMax-M2.7");
    }

    #[tokio::test]
    async fn auto_resolves_to_a_concrete_model_because_this_plan_publishes_no_routing_model() {
        let _lock = test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        set_config_root(temp.path());
        catalog::forget_cache();
        // Unlike Bailian, MiniMax has no model literally named `auto`, so letting a
        // bare `auto` reach the wire would be refused with `400 model not exist` on
        // the first turn. The selection resolves here instead, with no catalog at
        // hand, and the live head wins once one is reachable.
        assert_eq!(wire_model("auto"), DEFAULT_SELECTION);
        assert_eq!(wire_model(" auto "), DEFAULT_SELECTION);
    }

    #[tokio::test]
    async fn resolve_refuses_a_signed_out_source_rather_than_inventing_a_key() {
        let _lock = test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        set_config_root(temp.path());
        let error = resolve("").await.unwrap_err();
        assert!(error.contains("not signed in"), "{error}");
    }

    #[tokio::test]
    async fn resolve_points_the_anthropic_provider_at_the_site_with_no_v1_suffix() {
        let _lock = test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        set_config_root(temp.path());
        // A stored key with no live network: the catalog fetch fails, so the
        // requested model goes to the wire unchanged and the provider shape is
        // still asserted.
        auth::write_store_for_test(&json!({
            "apiKey": "sk-cp-abcdefghijklmnop",
            "region": "cn",
        }));
        catalog::forget_cache();
        let (provider, auth) = resolve("MiniMax-M2.5").await.expect("resolve");
        assert_eq!(provider["type"], json!("anthropic"));
        assert_eq!(
            provider["baseUrl"],
            json!("https://api.minimax.cn/anthropic")
        );
        assert_eq!(provider["model"], json!("MiniMax-M2.5"));
        assert_eq!(auth["type"], json!("api_key"));
        assert_eq!(auth["key"], json!("sk-cp-abcdefghijklmnop"));
    }
}
