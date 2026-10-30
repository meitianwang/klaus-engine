//! The Qoder subscription, end to end.
//!
//! A Qoder session does not speak a standard wire: requests are re-shaped into
//! Qoder's `agent_chat_generation` payload, signed per request with the COSY
//! scheme, and its SSE envelope is unwrapped back into an OpenAI stream. The four
//! pieces below are that whole path — the credential store ([`auth`]), the signer
//! ([`sign`]), the model catalog ([`catalog`]) and the loopback translator
//! ([`bridge`]).
//!
//! ## Why this lives in the engine
//!
//! It used to live in the desktop client, which made "use my Qoder subscription"
//! a thing only that client could do: the CLI had no signer, so it had no way to
//! reach the service. None of this needs a GUI. The signer is native Rust, the
//! credential is a file on disk, and the translator is a loopback HTTP server, so
//! the capability belongs where every client can reach it. The desktop client now
//! calls this copy instead of carrying its own.
//!
//! What stays with the host: opening a browser for the login page, and reading a
//! login out of the Qoder IDE's own store (SQLite plus Electron safeStorage).
//! [`auth::start`] returns the URL and the caller opens it; a host that can read
//! the IDE store hands the profile to [`auth::adopt_credential`]. Neither belongs
//! in an engine that may be running headless.

pub mod auth;
pub mod bridge;
pub mod catalog;
pub mod sign;
pub(crate) mod speech;

use std::path::{Path, PathBuf};

use serde_json::{json, Value};

/// A request id / session id in the shape Qoder's API expects.
///
/// Its own helper rather than a dependency on the desktop client's, which is
/// where these calls used to read it from.
pub(crate) fn uuid_v4() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// `n` cryptographically random bytes, for the signer's AES key and the login
/// flow's PKCE verifier.
pub(crate) fn random_bytes(n: usize) -> Vec<u8> {
    use rand::RngCore;
    let mut bytes = vec![0u8; n];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes
}

/// Where the Qoder credential is kept.
///
/// Held here rather than re-derived in each module below, so one endpoint has one
/// answer to "am I signed in". Re-pointable because it follows the endpoint's
/// config root, and an endpoint may be built against a different profile than the
/// last one in this process.
static CONFIG_ROOT: std::sync::RwLock<Option<PathBuf>> = std::sync::RwLock::new(None);

/// Point the Qoder credential store at a config root.
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

/// The model key a `qoder:<key>` selection puts on the wire.
///
/// An explicit catalog key stands as itself. A bare tier word or an unmapped
/// tier defers to the catalog's first entry: the picker offers no synthetic
/// automatic route, so the service default is the catalog head. With no
/// catalog reachable the service's own `auto` route remains the fallback.
/// One definition, because the capability set and the provider payload have
/// to describe the SAME model.
pub fn wire_model(selection: &str) -> String {
    match selection {
        "" | "auto" | "high" | "medium" | "low" => {
            catalog::first_model_key().unwrap_or_else(|| "auto".to_string())
        }
        other => other.to_string(),
    }
}

/// The provider and auth a `qoder:<key>` selection resolves to.
///
/// The bridge is started on demand and the engine's OpenAI chat provider is
/// pointed at it. The bearer is the bridge's own per-process token, not a Qoder
/// credential: the Qoder signing happens inside the bridge, so no subscription
/// secret is handed to the provider layer at all.
pub async fn resolve(selection: &str) -> Result<(Value, Value), String> {
    if catalog::access_token().is_err() {
        return Err("not signed in to a Qoder subscription — run `klaus login qoder`".to_string());
    }
    let (base_url, token) = bridge::ensure_started()
        .await
        .map_err(|error| format!("cannot start the Qoder bridge: {error}"))?;
    let provider = json!({
        "type": "openai",
        "api": "chat",
        "model": wire_model(selection),
        "baseUrl": base_url,
    });
    let auth = json!({ "type": "static_bearer", "token": token });
    Ok((provider, auth))
}

/// Whether a Qoder login is on disk, for a status report.
pub fn signed_in() -> bool {
    catalog::access_token().is_ok()
}

// ------------------------------------------------------------- host surface
//
// What a client outside this crate may call. Curated on purpose: the modules
// above are `pub` so this file can reach them, but their internals (the signer's
// context, the bridge's routing, the catalog's cache) are `pub(crate)` and
// stay that way. A host drives the login, reads the status, and asks about a
// model; it never signs a request itself.

pub use auth::{adopt_credential, import_local, logout, poll, start, usage};
pub use catalog::{
    access_token, auth_status, fetch_models, fetch_models_warm, model_capabilities, QoderError,
    QoderToken,
};

/// The tier the next request should ask for.
///
/// Pushed in-process as the composer's default. Explicit per-request reasoning
/// intent takes priority, so a summary can disable thinking independently.
/// A no-op until the bridge is running.
pub fn set_thinking_level(level: Option<String>) {
    bridge::set_thinking_level(level);
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

/// Generate an image through the subscription's own endpoint. Answers the encoded
/// image bytes, which is what the caller writes or forwards.
pub async fn generate_image(
    prompt: &str,
    size: Option<&str>,
    session_id: &str,
) -> Result<Vec<u8>, QoderError> {
    catalog::generate_image(prompt, size, session_id).await
}

/// Serializes the tests that repoint the credential root or `QODER_HOME`. Both
/// are per-process, so two tests fixturing them at once would read each other's
/// directory. Async-aware because those tests hold it across an await.
#[cfg(test)]
pub(crate) fn test_env_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}
