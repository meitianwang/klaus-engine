//! Kimi (Moonshot AI) membership support.
//!
//! Kimi Code serves the Kimi membership's coding allowance over the ordinary
//! Anthropic wire (`{base}/v1/messages`), so unlike Qoder, QwenWork and
//! QoderWork there is no signer and no local bridge to translate a proprietary
//! protocol: the engine's own Anthropic provider is pointed straight at the
//! gateway. The Kimi-specific knowledge left is which credential the caller
//! holds, which models the plan serves, and what the allowance looks like.
//!
//! Two credentials reach that one gateway, and this module keeps both:
//!
//! * An **API key** created in the Kimi Code console. This is the only route
//!   Kimi documents for third-party clients, so it is the default.
//! * A **device-flow login** (RFC 8628) against `auth.kimi.com`, the flow the
//!   official CLI runs. It is offered as the alternative because it spares the
//!   user a console visit, but Kimi has never sanctioned it for third parties:
//!   the endpoints below are the ones the official CLI calls, and Klaus sends its
//!   own honest `User-Agent` with no client-identity headers. Kimi's terms call
//!   out spoofing a client identity as a violation, so this path deliberately
//!   does not do it — which also means it can be withdrawn upstream at any time.
//!
//! Both land in one store, and [`auth::auth_type`] says which one is current.

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
/// The plan's 1M-context coding model. Kimi's own catalog order puts an older
/// preview at the head, so the head is NOT what a bare selection should land on:
/// [`catalog::first_model_key`] prefers this key whenever the catalog serves it,
/// and only an unreachable catalog or a lineup that dropped the model falls back
/// to the head.
pub const DEFAULT_SELECTION: &str = "k3";

/// The catalog key a `kimi:<key>` selection puts on the wire.
///
/// An explicit catalog key stands as itself. A bare tier word names the plan's
/// default model: Kimi publishes no synthetic automatic route, so the service
/// default is [`DEFAULT_SELECTION`] when the catalog serves it and the catalog's
/// first entry otherwise.
pub fn wire_model(selection: &str) -> String {
    match selection {
        "" | "auto" | "high" | "medium" | "low" => {
            catalog::first_model_key().unwrap_or_else(|| DEFAULT_SELECTION.to_string())
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

/// The provider and auth a `kimi:<key>` selection resolves to.
///
/// The provider is the ordinary Anthropic wire with Kimi's base URL, which the
/// provider joins with `/v1/messages` itself. The auth follows whichever
/// credential the store holds:
///
/// * an API key becomes `api_key`, which the provider sends as `x-api-key`;
/// * a device-flow login becomes `oauth_token` with the client config, so the
///   provider renews the 15-minute access token itself and persists the rotated
///   refresh token. Without that, a session outliving one token would die at the
///   quarter hour; measured, Kimi rotates the refresh token on every use but
///   keeps the previous one valid, so a renewed copy does not invalidate the
///   store's.
pub async fn resolve(selection: &str) -> Result<(Value, Value), String> {
    let credential = auth::credential().ok_or_else(|| {
        "not signed in to a Kimi membership — add a Kimi Code API key or sign in".to_string()
    })?;
    let base_url = auth::api_base();
    let model = resolved_model(selection).await;
    let provider = json!({
        "type": "anthropic",
        "model": model,
        "baseUrl": base_url,
    });
    let auth = match credential {
        auth::Credential::ApiKey { key } => json!({ "type": "api_key", "key": key }),
        auth::Credential::OAuth {
            access_token,
            refresh_token,
            expires_at,
        } => json!({
            "type": "oauth_token",
            "accessToken": access_token,
            "refreshToken": refresh_token,
            "expiresAt": expires_at,
            // The provider persists rotated tokens under this id, so it must be
            // stable across sessions and unique to Kimi.
            "credentialId": auth::CREDENTIAL_ID,
            // Nested, not flat: the wire reads the client config from `oauth`, so
            // the same keys spread across the auth object decode as absent and the
            // provider would hold a token it cannot renew.
            //
            // The wire requires all four client fields, but a device flow has no
            // loopback redirect: the browser posts to Kimi's own page and the token
            // arrives over this module's poll instead. `authorizeUrl` is therefore
            // the real verification page the user is sent to, and `redirectUri` is
            // the standard out-of-band marker saying "no callback". Only `tokenUrl`
            // and `clientId` are ever spent — a refresh posts exactly those two
            // plus the grant type, so neither placeholder rides a request.
            "oauth": {
                "clientId": auth::CLIENT_ID,
                "authorizeUrl": auth::VERIFICATION_URI,
                "tokenUrl": format!("{}/api/oauth/token", auth::oauth_host()),
                "redirectUri": auth::OUT_OF_BAND_REDIRECT,
            },
            "baseUrl": base_url,
        }),
    };
    Ok((provider, auth))
}

pub use auth::{
    access_token, auth_type, credential, import_local, logout, poll, refresh_now, set_api_key,
    start, status, usage, Credential, KimiAuthType,
};
pub use catalog::{fetch_models, fetch_models_warm, model_capabilities, KimiError};

/// Refresh a stored device-flow login before it expires.
///
/// A no-op for an API key, which has no expiry to run against.
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
