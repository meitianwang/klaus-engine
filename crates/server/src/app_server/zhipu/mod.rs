//! Zhipu GLM Coding Plan support.
//!
//! Coding Plan is a monthly subscription sold by Zhipu, and it is a different
//! product from the platform's pay-as-you-go API: a plan key is minted on the
//! plan page, it is metered in credits against a rolling five-hour and weekly
//! allowance rather than per token, and the plan's own terms confine it to
//! interactive use inside coding and agent tools. Its coding gateway serves the
//! ordinary Anthropic wire (`{base}/v1/messages`), so like Kimi and the Bailian
//! Token Plan there is no signer and no local bridge — the engine's own Anthropic
//! provider is pointed straight at it.
//!
//! The credential is a plan API key. Coding Plan offers no OAuth or device flow,
//! so there is no login button here: the settings pane takes a pasted key and the
//! engine verifies it against the catalog route before storing it.
//!
//! Two things shape this module, and both were measured rather than assumed:
//!
//! * Unlike the Bailian gateway, this one serves its catalog on the SAME endpoint
//!   as inference (`{base}/v1/models`), and it wants `x-api-key` rather than a
//!   bearer. One root and one header serve both reads.
//! * The plan is served by TWO sites whose keys do not cross: Zhipu's domestic
//!   platform and its international one. A key is bound to the site that issued
//!   it, so the pane picks a site instead of taking a free-form host — same shape
//!   as the Bailian source, and for the same reason.

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

/// What a selection names when no catalog is reachable to answer with.
///
/// This is the plan's top model. There is no routing model to avoid here — the
/// catalog lists concrete models only — and the forced-tool path session titles use
/// was measured on every model the plan serves, so nothing about this default is
/// load-bearing for correctness. With a catalog in hand [`catalog::first_model_key`]
/// resolves a bare selection to the same key.
pub const DEFAULT_SELECTION: &str = "glm-5.3";

/// The catalog key a `zhipu:<key>` selection puts on the wire.
///
/// A tier word resolves to the host's mapping for that tier, or to
/// [`DEFAULT_SELECTION`] when the host mapped nothing.
///
/// `auto` is routed to the head rather than passed through, and that is a measured
/// fact about this gateway rather than a preference: the plan has no routing model
/// by that name, and asking it for one answers `400 [1211][模型不存在]`. It still
/// has to be handled here because `auto` is what the host stores as its default
/// model pick, so a conversation whose source was switched to this plan without
/// naming a model arrives with exactly that value. Sending it through would fail
/// the first turn of every such conversation.
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

/// The provider and auth a `zhipu:<key>` selection resolves to.
///
/// The base URL is the Anthropic-compatible root WITHOUT a trailing `/v1`: the
/// provider appends `/v1/messages` itself, and a `/v1/` suffix would make model
/// discovery compose `/v1/v1/models` and 404.
///
/// The key rides as `api_key`, which the Anthropic provider sends as `x-api-key`.
/// That is the header the plan's own documentation spells for this endpoint.
pub async fn resolve(selection: &str) -> Result<(Value, Value), String> {
    let key = auth::api_key().ok_or_else(|| {
        "not signed in to a Zhipu Coding Plan — add the plan API key from the plan page".to_string()
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

pub use auth::{api_base, api_key, logout, region, set_api_key, status, usage, Region};
pub use catalog::{
    capabilities_of, fetch_models, fetch_models_warm, model_capabilities, ZhipuError,
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
    use serde_json::json;

    /// Seed a known catalog so `wire_model`'s head routing is testable against a
    /// fixed list rather than against whatever the network answers.
    fn seed(models: &[&str]) {
        catalog::forget_cache();
        crate::app_server::catalog_test_support::store_credential("zhipu", &config_root(), "head");
        let entries: Vec<Value> = models
            .iter()
            .map(|id| json!({ "key": id, "display_name": id.to_uppercase() }))
            .collect();
        catalog::seed_owned_catalog(&entries);
    }

    #[tokio::test]
    async fn auto_and_empty_route_to_the_catalog_head_not_to_the_wire() {
        let _lock = test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        set_config_root(temp.path());
        seed(&["glm-4.6", "glm-5.3-flash", "glm-5.3"]);
        assert_eq!(
            catalog::first_model_key().as_deref(),
            Some(DEFAULT_SELECTION)
        );

        // `auto` has no model on this gateway (measured: it answers
        // `400 [1211][模型不存在]`), but it is the host's default pick, so it must
        // resolve to the head rather than reaching the wire. The head prefers the
        // documented fallback over list order.
        assert_eq!(wire_model("auto"), DEFAULT_SELECTION);
        assert_eq!(wire_model(""), DEFAULT_SELECTION);
        // A concrete model and a tier the host already mapped pass through as-is.
        assert_eq!(wire_model("glm-4.6"), "glm-4.6");
    }

    #[tokio::test]
    async fn with_no_catalog_reachable_auto_falls_back_to_the_default_selection() {
        let _lock = test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        set_config_root(temp.path());
        catalog::forget_cache();
        // No seeded snapshot and no network: the head read answers None, so the
        // default is what carries `auto` and an empty selection to something that
        // can serve a turn.
        assert_eq!(wire_model("auto"), DEFAULT_SELECTION);
        assert_eq!(wire_model(""), DEFAULT_SELECTION);
    }
}
