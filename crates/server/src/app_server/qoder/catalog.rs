//! Qoder subscription signing + model catalog. The signed-in profile comes from
//! Klaus's own credential store ([`super::auth`]); this module turns it into a
//! signing context ([`super::sign`]) and projects the model scenes for the model
//! picker.
//!
//! Signing is native and needs nothing installed beside Klaus. It used to run on
//! Qoder's own wasm lifted out of an installed qodercli, which meant Qoder mode
//! was dead on any machine without that CLI — including every machine that has
//! only the Qoder IDE. Being signed out is still never papered over: callers get
//! a plain "not signed in" instead of an unsigned request.

use super::sign::{SignedRequest, SigningContext, INFER_PATH, MODEL_LIST_PATH};
use serde_json::{json, Map, Value};
use std::path::PathBuf;
use std::sync::Mutex;

fn api_base() -> String {
    std::env::var("QODER_API_BASE")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "https://api3.qoder.sh".into())
}

const CATALOG_TTL_MS: i64 = 6 * 60 * 60 * 1000;
const CATALOG_FETCH_TIMEOUT_MS: u64 = 10_000;
/// The bounded live window when the picker has no matching owned snapshot.
const WARM_CATALOG_TIMEOUT_MS: u64 = 1500;

/// `Cosy-ClientType`. `5` (the CLI default) routes Ultimate to a broken pipeline
/// node; `6` — the desktop IDE type Klaus actually is — resolves correctly. The
/// login and refresh calls in [`super::auth`] send the same value, so one account
/// never looks like two different clients.
pub(crate) const DESKTOP_CLIENT_TYPE: &str = "6";

/// `<QODER_HOME>` mirror: QODER_HOME wins, else `~/.qoder`. Only the on-disk
/// model catalog fallback still reads from here — the credential and the signer
/// no longer depend on a Qoder install existing at all.
fn qoder_home() -> PathBuf {
    match std::env::var("QODER_HOME") {
        Ok(value) if !value.trim().is_empty() => PathBuf::from(value.trim()),
        _ => PathBuf::from(std::env::var("HOME").unwrap_or_default()).join(".qoder"),
    }
}

fn auth_dir() -> PathBuf {
    qoder_home().join(".auth")
}

/// Errors carry the "the store simply isn't there" bit separately, because
/// callers must report a missing store as *signed out* rather than as an *error*.
pub struct QoderError {
    pub message: String,
    pub not_found: bool,
}

impl QoderError {
    fn msg(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            not_found: false,
        }
    }
}

/// The signing context, cached against a fingerprint of the credential it was
/// built from.
struct CachedContext {
    fingerprint: String,
    context: SigningContext,
    user_info: Value,
}

fn context_slot() -> &'static Mutex<Option<CachedContext>> {
    static SLOT: std::sync::OnceLock<Mutex<Option<CachedContext>>> = std::sync::OnceLock::new();
    SLOT.get_or_init(|| Mutex::new(None))
}

/// Drop the cached signing context. Called whenever the stored profile changes
/// (login, adopt, refresh, sign-out) so the next signature is built from the
/// credential that is on disk now, not the one that was there at first use.
pub(crate) fn forget_context() {
    let mut guard = context_slot().lock().unwrap_or_else(|p| p.into_inner());
    *guard = None;
    drop(guard);
    forget_cache();
}

/// Build or reuse the signing context from Klaus's stored profile, rebuilding it
/// whenever that profile changes so a refreshed token is picked up instead of
/// signing with a stale one.
fn with_context<T>(
    action: impl FnOnce(&SigningContext, &Value) -> Result<T, QoderError>,
) -> Result<T, QoderError> {
    // No stored credential is "signed out", not a failure: the settings pane and
    // the auth pill both render it as a state the user can act on.
    let Some((machine_id, user_info)) = super::auth::credential() else {
        return Err(QoderError {
            message: "Qoder subscription is not signed in".into(),
            not_found: true,
        });
    };
    // Fingerprint the credential the context was built from: the token and its
    // expiry are exactly what a refresh rewrites.
    let token = user_info
        .get("security_oauth_token")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let expire_time = user_info
        .get("expire_time")
        .and_then(Value::as_i64)
        .unwrap_or_default();
    let fingerprint = format!("{machine_id}:{}:{expire_time}", token.len());

    let mut guard = context_slot().lock().unwrap_or_else(|p| p.into_inner());
    let stale = guard
        .as_ref()
        .is_none_or(|cached| cached.fingerprint != fingerprint);
    if stale {
        let context = SigningContext::new(&machine_id, DESKTOP_CLIENT_TYPE, &user_info)
            .map_err(QoderError::msg)?;
        *guard = Some(CachedContext {
            fingerprint,
            context,
            user_info,
        });
    }
    let cached = guard.as_ref().expect("context populated above");
    action(&cached.context, &cached.user_info)
}

fn account_label(user_info: &Value) -> String {
    for key in ["name", "email", "uid"] {
        if let Some(text) = user_info.get(key).and_then(Value::as_str) {
            if !text.is_empty() {
                return text.to_string();
            }
        }
        // uid is sometimes numeric in the store; String(...) accepts either.
        if let Some(number) = user_info.get(key).and_then(Value::as_i64) {
            return number.to_string();
        }
    }
    "Qoder".into()
}

pub struct QoderToken {
    pub access_token: String,
    pub account: String,
}

/// getQoderAccessToken. Errors when signed out, while still letting the caller
/// tell "no credential at all" (the `not_found` bit) from "a profile with no
/// token in it" (the `not signed in` message).
pub fn access_token() -> Result<QoderToken, QoderError> {
    with_context(|_, user_info| {
        let access_token = user_info
            .get("access_token")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if access_token.is_empty() {
            return Err(QoderError::msg("Qoder subscription is not signed in"));
        }
        Ok(QoderToken {
            access_token,
            account: account_label(user_info),
        })
    })
}

/// getQoderAuthStatus mirror — note a missing credential file is plain
/// `{ loggedIn: false }` with NO error field, so the settings pane shows "not
/// signed in" rather than a failure.
pub fn auth_status() -> Value {
    super::auth::status()
}

// ------------------------------------------------------------- model catalog

/// parseQoderModelScenes. Field names are the wire names the renderer already
/// consumes, so they stay snake_case wherever Qoder's own payload is snake_case.
pub fn parse_model_scenes(parsed: &Value) -> Vec<Value> {
    // The gateway ships a synthetic `auto` service route in the lineup. The
    // picker offers no automatic row — a bare default and unmapped tiers name
    // the catalog head instead — so the route must not surface as a choice,
    // and every "catalog head" fallback has to name a real model, not it.
    parse_model_scene_names(parsed, &["assistant", "chat"])
        .into_iter()
        .filter(|model| model.get("key").and_then(Value::as_str) != Some("auto"))
        .collect()
}

/// Project one of the COSY model scenes into the engine's shared catalog shape.
/// Qoder serves `assistant`/`chat`; QwenWork serves `qwork` on the same signed
/// catalog route.
pub(crate) fn parse_model_scene_names(parsed: &Value, names: &[&str]) -> Vec<Value> {
    let scene = names
        .iter()
        .find_map(|name| parsed.get(*name).filter(|value| value.is_array()))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    scene
        .iter()
        .filter(|model| {
            model
                .get("key")
                .and_then(Value::as_str)
                .is_some_and(|k| !k.is_empty())
        })
        .map(|model| {
            let thinking = model
                .get("thinking_config")
                .or_else(|| model.get("thinkingConfig"));
            let nested_efforts = thinking
                .and_then(|t| t.get("enabled"))
                .and_then(|e| e.get("efforts"));
            let efforts = model.get("efforts").or(nested_efforts);
            let effort_keys: Vec<String> = match efforts {
                Some(Value::Object(map)) => map.keys().cloned().collect(),
                Some(Value::Array(values)) => values
                    .iter()
                    .filter_map(|value| {
                        value
                            .as_str()
                            .or_else(|| value.get("key").and_then(Value::as_str))
                            .map(str::to_string)
                    })
                    .collect(),
                _ => Vec::new(),
            };
            let default_effort = model
                .get("default_effort")
                .or_else(|| model.get("defaultEffort"))
                .and_then(Value::as_str)
                .map(str::to_string)
                .or_else(|| {
                    nested_efforts.and_then(Value::as_object).and_then(|map| {
                        map.iter()
                            .find(|(_, value)| {
                                value.get("is_default") == Some(&json!(true))
                                    || value.get("isDefault") == Some(&json!(true))
                            })
                            .map(|(key, _)| key.clone())
                    })
                });
            let context_config = model
                .get("context_config")
                .or_else(|| model.get("contextConfig"));
            let mut context_tokens: Vec<i64> = model
                .get("available_context_windows")
                .or_else(|| model.get("availableContextWindows"))
                .and_then(Value::as_array)
                .map(|values| {
                    values
                        .iter()
                        .filter_map(number_of)
                        .filter(|tokens| *tokens > 0)
                        .collect()
                })
                .unwrap_or_default();
            context_tokens.extend(
                context_config
                    .and_then(Value::as_object)
                    .map(|map| {
                        map.values()
                            .filter_map(|entry| {
                                entry
                                    .get("token_count")
                                    .or_else(|| entry.get("tokenCount"))
                                    .and_then(number_of)
                            })
                            .filter(|tokens| *tokens > 0)
                            .collect::<Vec<i64>>()
                    })
                    .unwrap_or_default(),
            );
            context_tokens.sort_unstable();
            context_tokens.dedup();

            let mut out = Map::new();
            out.insert(
                "key".into(),
                json!(model.get("key").and_then(Value::as_str).unwrap_or_default()),
            );
            out.insert(
                "display_name".into(),
                model.get("display_name").cloned().unwrap_or(Value::Null),
            );
            out.insert(
                "is_reasoning".into(),
                json!(truthy(model.get("is_reasoning"))),
            );
            out.insert("is_vl".into(), json!(truthy(model.get("is_vl"))));
            // `max_input_tokens` is emitted only when numeric — JSON.stringify
            // drops an `undefined` field, so the key must be absent, not null.
            if let Some(tokens) = model.get("max_input_tokens").and_then(number_of) {
                out.insert("max_input_tokens".into(), json!(tokens));
            }
            if let Some(tokens) = model.get("max_output_tokens").and_then(number_of) {
                out.insert("max_output_tokens".into(), json!(tokens));
            }
            if let Some(tokens) = model
                .get("default_context_window")
                .or_else(|| model.get("defaultContextWindow"))
                .and_then(number_of)
            {
                out.insert("default_context_window".into(), json!(tokens));
            }
            if !effort_keys.is_empty() {
                out.insert("efforts".into(), json!(&effort_keys));
            }
            if let Some(default_effort) = default_effort {
                out.insert("defaultEffort".into(), json!(default_effort));
            }
            if truthy(
                model
                    .get("supports_disabled")
                    .or_else(|| model.get("supportsDisabled")),
            ) || thinking
                .and_then(|t| t.get("disabled"))
                .is_some_and(|v| !v.is_null())
            {
                out.insert("canDisableThinking".into(), json!(true));
            }
            if !effort_keys.is_empty()
                || thinking
                    .and_then(|t| t.get("enabled"))
                    .is_some_and(|v| !v.is_null())
            {
                out.insert("canEnableThinking".into(), json!(true));
            }
            if !context_tokens.is_empty() {
                out.insert("contextTokens".into(), json!(context_tokens));
            }
            // The credit multiplier ships camelCase on the SDK layer and
            // snake_case on the server layer; normalize to one spelling so a
            // client never has to know which layer it was handed.
            if let Some(factor) = model
                .get("priceFactor")
                .or_else(|| model.get("price_factor"))
                .and_then(number_of_f64)
            {
                out.insert("priceFactor".into(), json!(factor));
            }
            // Preserve the non-secret model metadata that the official SDK
            // exposes so settings, the composer and session resolution all read
            // the same catalog instead of maintaining parallel hard-coded maps.
            for key in [
                "description",
                "format",
                "source",
                "provider",
                "tags",
                "feature_switches",
                "functionSwitches",
                "function_switches",
                "thinking_config",
                "context_config",
                "is_default",
                "is_new",
                "is_free",
                // Brand glyph URL the gateway ships per model; the official
                // selector renders it beside the name. The credit multiplier
                // is normalized above because its two layers spell it
                // differently.
                "icon",
            ] {
                if let Some(value) = model.get(key) {
                    out.insert(key.into(), value.clone());
                }
            }
            super::super::conversation_catalog::mark_purpose("", model, &mut out);
            Value::Object(out)
        })
        .collect()
}

fn number_of(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number.as_f64().filter(|n| n.is_finite()).map(|n| n as i64),
        Value::String(text) => text.trim().parse::<f64>().ok().map(|n| n as i64),
        _ => None,
    }
}

/// `number_of` truncates to whole units, which is right for token counts and
/// wrong for a fractional credit multiplier.
fn number_of_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64().filter(|n| n.is_finite()),
        Value::String(text) => text.trim().parse::<f64>().ok().filter(|n| n.is_finite()),
        _ => None,
    }
}

fn truthy(value: Option<&Value>) -> bool {
    match value {
        Some(Value::Bool(flag)) => *flag,
        Some(Value::Number(number)) => number.as_f64().is_some_and(|n| n != 0.0),
        Some(Value::String(text)) => !text.is_empty(),
        _ => false,
    }
}

struct CatalogCache {
    fingerprint: Option<String>,
    models: Vec<Value>,
    expires_at: i64,
}

pub(crate) fn forget_cache() {
    *catalog_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
}

fn catalog_cache() -> &'static Mutex<Option<CatalogCache>> {
    static CACHE: std::sync::OnceLock<Mutex<Option<CatalogCache>>> = std::sync::OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(None))
}

/// The catalog head's key from the fresh in-process cache. The picker offers
/// no synthetic automatic route, so an unmapped tier or a bare default names
/// this entry. Reading stays inside Klaus's own state — never a third-party
/// cache directory, never a fetch — so a selection resolution answers from
/// what this process already knows. `None` leaves the caller on the service's
/// own route.
pub(crate) fn first_model_key() -> Option<String> {
    {
        let guard = catalog_cache().lock().unwrap_or_else(|p| p.into_inner());
        if let Some(cache) = guard
            .as_ref()
            .filter(|cache| cache.fingerprint == super::auth::catalog_fingerprint())
        {
            if now_ms() < cache.expires_at {
                return first_key_in(&cache.models);
            }
        }
    }
    // The on-disk snapshot follows the config root, so a cold process still
    // names the catalog head instead of the service route.
    first_key_in(&read_local_models())
}

/// The catalog's first entry key. The picker offers no synthetic automatic
/// route, so an unmapped tier or a bare default names this entry.
fn first_key_in(models: &[Value]) -> Option<String> {
    models
        .first()
        .and_then(|model| model.get("key"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// readLocalQoderModels mirror: Qoder's own on-disk catalog, plain JSON, no
/// signing needed. It may lag the live schema, so it is a fallback only.
fn read_local_models() -> Vec<Value> {
    std::fs::read_to_string(auth_dir().join("models"))
        .ok()
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
        .map(|parsed| parse_model_scenes(&parsed))
        .unwrap_or_default()
}

fn owned_catalog_path() -> PathBuf {
    super::config_root().join("qoder-model-catalog.json")
}

fn read_owned_catalog() -> Option<Vec<Value>> {
    let fingerprint = super::auth::catalog_fingerprint()?;
    let raw = std::fs::read_to_string(owned_catalog_path()).ok()?;
    let value: Value = serde_json::from_str(&raw).ok()?;
    if value.get("projectionVersion").and_then(Value::as_u64) != Some(1)
        || value.get("fingerprint").and_then(Value::as_str) != Some(fingerprint.as_str())
    {
        return None;
    }
    value
        .get("models")
        .and_then(Value::as_array)
        .cloned()
        .filter(|models| !models.is_empty())
}

fn write_owned_catalog(models: &[Value], fingerprint: &str) {
    let path = owned_catalog_path();
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Ok(serialized) = serde_json::to_string(
        &json!({ "projectionVersion": 1, "models": models, "fingerprint": fingerprint }),
    ) {
        let _ = std::fs::write(&path, serialized);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
        }
    }
}

/// Sign and issue the catalog GET, then parse. The route is requested without
/// `Encode=1`, so the answer is plain JSON and there is nothing to decrypt.
async fn fetch_live_models() -> Result<Vec<Value>, QoderError> {
    let signed: SignedRequest =
        with_context(|context, _| Ok(context.sign_get(&api_base(), MODEL_LIST_PATH)))?;
    let client = reqwest::Client::builder()
        // Without a deadline a slow catalog endpoint pins every caller (the
        // model pill would sit on "no models" for the whole hang).
        .timeout(std::time::Duration::from_millis(CATALOG_FETCH_TIMEOUT_MS))
        .build()
        .map_err(|e| QoderError::msg(e.to_string()))?;
    let mut request = client.get(&signed.url);
    for (name, value) in &signed.headers {
        request = request.header(name, value);
    }
    // The signer decides whether the call carries a body; dropping one it emitted
    // would send a request the signed headers do not describe.
    if let Some(body) = &signed.body {
        request = request.body(body.clone());
    }
    let response = request
        .send()
        .await
        .map_err(|e| QoderError::msg(e.to_string()))?;
    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    if !status.is_success() {
        let detail: String = text.chars().take(200).collect();
        return Err(QoderError::msg(format!(
            "model/list {}: {detail}",
            status.as_u16()
        )));
    }
    let parsed = serde_json::from_str::<Value>(&text)
        .map_err(|e| QoderError::msg(format!("model/list body is not JSON: {e}")))?;
    Ok(parse_model_scenes(&parsed))
}

/// fetchQoderModels mirror: stale-while-revalidate over a 6h live TTL. Cold
/// starts paint instantly from whatever catalog is at hand (expired live cache,
/// else Qoder's on-disk file) while a refresh runs in the background; the local
/// file never becomes terminal when a live fetch can land, because it may lack
/// `thinking_config` and newer models.
///
/// There is deliberately no "skip the cache" flag: a caller that wants the live
/// catalog wants it because the cache may be stale, which is what the background
/// revalidate below already fixes. Callers that must not paint the on-disk file at
/// all use [`fetch_models_warm`] instead.
pub async fn fetch_models() -> Result<Vec<Value>, QoderError> {
    {
        let guard = catalog_cache().lock().unwrap_or_else(|p| p.into_inner());
        if let Some(cache) = guard
            .as_ref()
            .filter(|cache| cache.fingerprint == super::auth::catalog_fingerprint())
        {
            if now_ms() < cache.expires_at {
                return Ok(cache.models.clone());
            }
        }
    }
    let stale = {
        let guard = catalog_cache().lock().unwrap_or_else(|p| p.into_inner());
        match guard.as_ref() {
            Some(cache)
                if !cache.models.is_empty()
                    && cache.fingerprint == super::auth::catalog_fingerprint() =>
            {
                cache.models.clone()
            }
            _ => read_local_models(),
        }
    };
    if !stale.is_empty() {
        tokio::spawn(async move {
            if let Err(error) = refresh_live_catalog().await {
                eprintln!("[QoderBridge] background catalog refresh failed, keeping the stale catalog: {}", error.message);
            }
        });
        return Ok(stale);
    }
    match refresh_live_catalog().await {
        Ok(models) if !models.is_empty() => Ok(models),
        Ok(_) => Ok(read_local_models()),
        Err(error) => {
            eprintln!(
                "[QoderBridge] live model catalog failed, falling back to the local cache: {}",
                error.message
            );
            let local = read_local_models();
            // A cold start with neither a live catalog nor a local file has
            // nothing to show; surface why instead of an empty success.
            if local.is_empty() {
                Err(error)
            } else {
                Ok(local)
            }
        }
    }
}

async fn refresh_live_catalog() -> Result<Vec<Value>, QoderError> {
    let fingerprint = super::auth::catalog_fingerprint();
    let models = fetch_live_models().await?;
    if fingerprint != super::auth::catalog_fingerprint() {
        return Ok(models);
    }
    if !models.is_empty() {
        let mut guard = catalog_cache().lock().unwrap_or_else(|p| p.into_inner());
        *guard = Some(CatalogCache {
            fingerprint: fingerprint.clone(),
            models: models.clone(),
            expires_at: now_ms() + CATALOG_TTL_MS,
        });
    }
    if !models.is_empty() {
        if let Some(fingerprint) = fingerprint.as_deref() {
            write_owned_catalog(&models, fingerprint);
        }
    }
    Ok(models)
}

/// The picker's catalog read. Two deliberate differences from `fetch_models`:
/// signed out it errors instead of painting the on-disk fallback (the settings
/// pane reports "not signed in", and the picker must agree with an empty list
/// rather than fabricating options from an unrelated file). A matching owned
/// snapshot paints immediately and revalidates in the background. Without one,
/// the live catalog gets a bounded first chance; a hot cache serves instantly.
pub async fn fetch_models_warm() -> Result<Vec<Value>, QoderError> {
    if super::auth::credential().is_none() {
        return Err(QoderError {
            message: "Qoder subscription is not signed in".into(),
            not_found: true,
        });
    }
    {
        let guard = catalog_cache().lock().unwrap_or_else(|p| p.into_inner());
        if let Some(cache) = guard
            .as_ref()
            .filter(|cache| cache.fingerprint == super::auth::catalog_fingerprint())
        {
            if now_ms() < cache.expires_at {
                return Ok(cache.models.clone());
            }
        }
    }
    if let Some(local) = read_owned_catalog() {
        tokio::spawn(async {
            if let Err(error) = refresh_live_catalog().await {
                eprintln!("[QoderBridge] catalog refresh failed: {}", error.message);
            }
        });
        return Ok(local);
    }
    let warmed = tokio::time::timeout(
        std::time::Duration::from_millis(WARM_CATALOG_TIMEOUT_MS),
        refresh_live_catalog(),
    )
    .await;
    if let Ok(Ok(models)) = warmed {
        if !models.is_empty() {
            return Ok(models);
        }
    }
    // The live catalog did not land in time: paint from whatever is at hand and
    // keep revalidating in the background for the next open.
    let stale = {
        let guard = catalog_cache().lock().unwrap_or_else(|p| p.into_inner());
        match guard.as_ref() {
            Some(cache)
                if !cache.models.is_empty()
                    && cache.fingerprint == super::auth::catalog_fingerprint() =>
            {
                cache.models.clone()
            }
            _ => read_local_models(),
        }
    };
    tokio::spawn(async move {
        if let Err(error) = refresh_live_catalog().await {
            eprintln!(
                "[QoderBridge] background catalog refresh failed, keeping the stale catalog: {}",
                error.message
            );
        }
    });
    Ok(stale)
}

// ------------------------------------------------------------- inference bridge

/// Sign a Qoder inference (`agent_chat_generation`) request. The signer puts the
/// body on the wire as the `Encode=1` envelope (see [`super::sign::INFER_PATH`] for
/// why a plaintext body cannot survive the gateway's WAF), so callers hand it
/// plain JSON and never encode themselves. Used by the loopback bridge
/// ([`super::bridge`]).
pub(crate) fn sign_infer_request(
    body_json: &str,
    model_key: &str,
) -> Result<SignedRequest, QoderError> {
    with_context(|context, _| Ok(context.sign_infer(&api_base(), body_json, model_key, "system")))
}

/// The image-generation route: the Qoder client's own ImageGen tool posts
/// {payload: JSON.stringify({model, prompt, size}), encodeVersion, sessionId,
/// requestId} here and reads back {data:[{url}]} (the url is a https link or a
/// data: URL). Two wire facts were settled by driving Qoder's auth wasm
/// directly with a probe script that lived in the product repo's retired Tauri
/// shell (`apps/tauri/scripts/probe-qoder-image-route.mjs`, readable only through
/// that repo's git history — it is not in this submodule's): the route lives
/// behind the /algo gateway prefix like the inference family, and `Encode=1`
/// is mandatory — the body must carry the obfuscated envelope from
/// [`super::sign::encode_body`], which is also what the signature covers.
/// The response is plain JSON.
const IMAGE_GEN_PATH: &str = "/algo/api/v2/service/pro/generateImage?Encode=1";

/// The image route's base+path, overridable so probes can settle which prefix
/// the deployment actually serves.
fn image_gen_url() -> String {
    std::env::var("KLAUS_QODER_IMAGE_GEN_URL")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| format!("{}{}", api_base().trim_end_matches('/'), IMAGE_GEN_PATH))
}

/// The Qoder-side default image model (the client's ImageGen default when no
/// model policy overrides it). KLAUS_QODER_IMAGE_MODEL overrides it for probes.
const IMAGE_GEN_MODEL: &str = "ge3i";

/// Generate one image through the Qoder subscription. Returns the raw image
/// bytes (downloading the https URL or decoding the data: URL the answer
/// carries). Text-to-image only: the route accepts no source images.
pub(crate) async fn generate_image(
    prompt: &str,
    size: Option<&str>,
    session_id: &str,
) -> Result<Vec<u8>, QoderError> {
    let model = std::env::var("KLAUS_QODER_IMAGE_MODEL")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| IMAGE_GEN_MODEL.to_string());
    let payload = json!({
        "model": model,
        "prompt": prompt,
        "size": size.unwrap_or("1024x1024"),
    });
    let body = json!({
        "payload": payload.to_string(),
        "encodeVersion": "1",
        "sessionId": session_id,
        "requestId": super::uuid_v4(),
    })
    .to_string();
    // `Encode=1` means the wire body is the obfuscated envelope, and the
    // signature proves the encoded bytes — so encode first, then sign.
    let wire_body = super::sign::encode_body(&body);
    let signed = sign_image_gen_request(&wire_body)?;
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(300))
        .build()
        .map_err(|e| QoderError::msg(e.to_string()))?;
    let mut request = client.post(&signed.url);
    for (name, value) in &signed.headers {
        request = request.header(name, value);
    }
    let response = request
        .body(signed.body.clone().unwrap_or_default())
        .send()
        .await
        .map_err(|e| QoderError::msg(e.to_string()))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|e| QoderError::msg(e.to_string()))?;
    if std::env::var_os("KLAUS_QODER_IMAGE_GEN_DEBUG").is_some() {
        eprintln!(
            "[qoder-image] status={} body head: {:.300}",
            status.as_u16(),
            text
        );
    }
    if !status.is_success() {
        let detail: String = text.chars().take(200).collect();
        return Err(QoderError::msg(format!(
            "generateImage {}: {detail}",
            status.as_u16()
        )));
    }
    let parsed: Value = serde_json::from_str(&text)
        .map_err(|e| QoderError::msg(format!("generateImage body is not JSON: {e}")))?;
    let url = parsed
        .get("data")
        .and_then(Value::as_array)
        .and_then(|data| data.first())
        .and_then(|item| item.get("url"))
        .and_then(Value::as_str)
        .filter(|url| !url.is_empty())
        .ok_or_else(|| QoderError::msg("generateImage answered without data[0].url"))?;
    fetch_image_bytes(&client, url).await
}

/// The answer's url is either a data: URL (decode in place) or an https link
/// (download it).
async fn fetch_image_bytes(client: &reqwest::Client, url: &str) -> Result<Vec<u8>, QoderError> {
    if let Some((_, encoded)) = url.split_once("base64,") {
        use base64::Engine as _;
        return base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|e| QoderError::msg(format!("undecodable data URL image: {e}")));
    }
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|e| QoderError::msg(e.to_string()))?;
    let status = response.status();
    if !status.is_success() {
        return Err(QoderError::msg(format!(
            "image download HTTP {}",
            status.as_u16()
        )));
    }
    let bytes = response
        .bytes()
        .await
        .map_err(|e| QoderError::msg(e.to_string()))?;
    Ok(bytes.to_vec())
}

/// The image-generation route: signed plain-JSON POST, same signature tuple as
/// the inference route. Returns the signed request for the caller to send.
pub(crate) fn sign_image_gen_request(body_json: &str) -> Result<SignedRequest, QoderError> {
    with_context(|context, _| Ok(context.sign_post_url(&image_gen_url(), body_json)))
}

/// The gateway host serving the signed one-shot search route. The search
/// service lives on its own host, apart from the inference shards.
fn search_base() -> String {
    std::env::var("KLAUS_QODER_SEARCH_BASE")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "https://center.qoder.sh".to_string())
}

/// The signed one-shot search route: the wire body is
/// `{payload: <stringified search request>, encodeVersion}` and the answer is
/// plain JSON holding `pageItems`. Returns the signed request for the caller
/// to send.
pub(crate) fn sign_search_post(body_json: &str) -> Result<SignedRequest, QoderError> {
    // `Encode=1` means the wire body is the obfuscated envelope, exactly like
    // the image route: encode first, then sign the encoded bytes. The signing
    // path strips the query, so the flag never enters the signature.
    let url = format!(
        "{}{}",
        search_base().trim_end_matches('/'),
        "/algo/api/v1/webSearch/oneSearch?Encode=1"
    );
    let encoded = super::sign::encode_body(body_json);
    with_context(|context, _| Ok(context.sign_post_url(&url, &encoded)))
}

/// The inference route Klaus signs, exposed for the bridge's diagnostics.
pub(crate) fn infer_path() -> &'static str {
    INFER_PATH
}

/// The projected catalog row for `key` (stale-while-revalidate like the model
/// picker), used by the bridge to resolve reasoning effort and the pinned
/// context window. Auto uses the conservative intersection of enabled models;
/// other unknown keys return `None`.
pub(crate) async fn lookup_model(key: &str) -> Option<Value> {
    let models = fetch_models().await.ok()?;
    if key == "auto" {
        return Some(auto_model_info(&models));
    }
    models
        .into_iter()
        .find(|model| model.get("key").and_then(Value::as_str) == Some(key))
}

/// The pinned context window for `key` (resolveQoderContextLength): the largest
/// advertised window, else `max_input_tokens`, else the default. Rides
/// `config.contextWindow` so the engine's compaction threshold matches what the
/// bridge actually sends upstream.
pub(crate) async fn model_context_window(key: &str) -> Option<i64> {
    Some(super::bridge::resolve_qoder_context_length(
        lookup_model(key).await.as_ref(),
    ))
}

/// What ONE Qoder model can do, in the shape the session init's `capabilities`
/// field takes.
///
/// Read off the catalog instead of hardcoded: the service itself says which keys
/// are vision-language (`is_vl`) and how wide their context is. Declaring it at
/// all is the point — a provider reads exactly what the init declared, so a Qoder
/// session that declared nothing refused every image with "this model cannot
/// understand images" while the bridge was announcing `is_vl` upstream.
///
/// The advertised effort ladder is carried into the core provider, so every
/// request can override the composer's in-process default (including child runs
/// and per-turn Ultra). No unsupported local preset is declared as a wire tier.
pub async fn model_capabilities(key: &str) -> Value {
    if key == "auto" {
        return auto_capabilities().await;
    }
    let entry = lookup_model(key).await;
    capabilities_for_entry(entry.as_ref())
}

/// `auto` names no catalog entry: the service picks the model, so the declared set
/// has to hold for ANY key it may pick. Vision therefore needs the WHOLE enabled
/// lineup to be vision-language — claiming it because one listed model can see
/// would send an image to a model that cannot — and the window is the bridge's own
/// default, which is what the bridge sends upstream for `auto`.
async fn auto_capabilities() -> Value {
    let models = fetch_models().await.unwrap_or_default();
    let mut caps = json!({
        "vision": !models.is_empty() && models.iter().all(entry_is_vision),
        "contextWindow": super::bridge::QODER_DEFAULT_CONTEXT_TOKENS,
        "reasoningDialect": "qoder",
    });
    if let Some(effort) = native_effort_capability(&auto_model_info(&models)) {
        caps["effort"] = effort;
    }
    caps
}

/// The provider declaration and bridge resolve the same conservative auto ladder.
/// No default is invented, and a model without thinking removes the common ladder.
fn auto_model_info(models: &[Value]) -> Value {
    let mut common: Option<Vec<Value>> = None;
    for model in models {
        let levels: Vec<Value> = model
            .get("efforts")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .filter(|level| engine_effort_level(level).is_some())
            .map(|level| json!(level.trim().to_ascii_lowercase()))
            .collect();
        common = Some(match common {
            None => levels,
            Some(previous) => previous
                .into_iter()
                .filter(|level| levels.contains(level))
                .collect(),
        });
    }
    json!({"key":"auto", "efforts":common.unwrap_or_default()})
}

/// A boolean speed override is available only when both native choices exist.
pub(crate) fn fast_mode_switch(entry: &Value) -> Option<&Value> {
    let switches = entry
        .get("feature_switches")
        .or_else(|| entry.get("functionSwitches"))
        .or_else(|| entry.get("function_switches"))?
        .as_array()?;
    switches.iter().find(|switch| {
        switch.get("key").and_then(Value::as_str) == Some("highspeed")
            && switch.get("type").and_then(Value::as_str) == Some("bool")
            && switch
                .get("options")
                .and_then(Value::as_object)
                .is_some_and(|options| {
                    options.contains_key("true") && options.contains_key("false")
                })
    })
}

fn capabilities_for_entry(entry: Option<&Value>) -> Value {
    let mut caps = json!({
        "vision": entry.is_some_and(entry_is_vision),
        "contextWindow": super::bridge::resolve_qoder_context_length(entry),
        "reasoningDialect":"qoder",
        "fastMode": entry.and_then(super::catalog::fast_mode_switch).is_some(),
    });
    if let Some(effort) = entry.and_then(native_effort_capability) {
        caps["effort"] = effort;
    }
    caps
}

/// Concrete native effort only. Vendor 'ultra' is not the local Ultra mode;
/// without a concrete supported lower tier, preserve the endpoint's default.
pub(crate) fn engine_effort_level(level: &str) -> Option<&'static str> {
    match level.trim().to_ascii_lowercase().as_str() {
        "minimal" | "low" => Some("low"),
        "medium" => Some("medium"),
        "high" => Some("high"),
        "xhigh" => Some("xhigh"),
        "max" => Some("max"),
        _ => None,
    }
}

pub(crate) fn native_effort_capability(model: &Value) -> Option<Value> {
    let levels: Vec<&str> = model
        .get("efforts")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .filter_map(engine_effort_level)
        .fold(Vec::new(), |mut levels, level| {
            if !levels.contains(&level) {
                levels.push(level);
            }
            levels
        });
    if levels.is_empty() {
        return None;
    }
    let mut effort = json!({"levels":levels});
    if let Some(default) = model
        .get("defaultEffort")
        .and_then(Value::as_str)
        .and_then(engine_effort_level)
        .filter(|default| levels.contains(default))
    {
        effort["default"] = json!(default);
    }
    Some(effort)
}

fn entry_is_vision(entry: &Value) -> bool {
    entry.get("is_vl").and_then(Value::as_bool).unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    crate::app_server::catalog_test_support::warm_catalog_tests!("qoder", || read_owned_catalog()
        .unwrap_or_default());

    /// `QODER_HOME` for the length of one test, then back as it was.
    ///
    /// Environment mutation goes through core's process-environment API, never
    /// `std::env::set_var`: the process environment is shared with exact parent
    /// capture, and a repository-wide guard test fails on any raw mutation outside
    /// that lock. Restoring on drop rather than at the end of the body so a failed
    /// assertion cannot leak a temp path into the rest of the binary.
    struct QoderHomeGuard {
        previous: Option<String>,
    }

    impl QoderHomeGuard {
        fn set(path: &std::path::Path) -> Self {
            let previous = std::env::var("QODER_HOME").ok();
            klaus_agent_core::runtime_process::set_process_environment_var(
                "QODER_HOME",
                path.as_os_str(),
            );
            Self { previous }
        }
    }

    impl Drop for QoderHomeGuard {
        fn drop(&mut self) {
            match &self.previous {
                Some(value) => klaus_agent_core::runtime_process::set_process_environment_var(
                    "QODER_HOME",
                    value,
                ),
                None => {
                    klaus_agent_core::runtime_process::remove_process_environment_var("QODER_HOME")
                }
            }
        }
    }

    /// The gateway ships a synthetic `auto` service route in the lineup. It
    /// names no model of its own, so it must never surface as a picker row,
    /// and the catalog head every default falls back to must be a real model.
    #[test]
    fn the_catalog_drops_the_synthetic_auto_route() {
        let parsed = serde_json::json!({
            "assistant": [
                { "key": "auto", "display_name": "Auto", "enable": true },
                { "key": "ultimate", "display_name": "Ultimate", "enable": true }
            ]
        });
        let models = parse_model_scenes(&parsed);
        let keys: Vec<&str> = models
            .iter()
            .filter_map(|model| model.get("key").and_then(Value::as_str))
            .collect();
        assert_eq!(
            keys,
            vec!["ultimate"],
            "auto must not be a picker row: {models:?}"
        );
        assert_eq!(first_key_in(&models).as_deref(), Some("ultimate"));
    }

    /// A signed-out profile must not paint Qoder's on-disk catalog file. The file
    /// is whatever an installed Qoder client last cached, so painting it fabricates
    /// a full model picker while the settings pane truthfully says "not signed in"
    /// — and every option in it would fail on the first turn. The settings read
    /// ([`fetch_models`]) may fall back to that file; the picker's warm read
    /// may not.
    #[tokio::test]
    async fn the_warm_read_refuses_the_local_catalog_while_signed_out() {
        let _lock = super::super::test_env_lock().lock().await;
        let fixture = tempfile::tempdir().expect("tempdir");
        let config = fixture.path().join("config");
        let home = fixture.path().join("qoder-home");
        std::fs::create_dir_all(&config).expect("config dir");
        std::fs::create_dir_all(home.join(".auth")).expect("auth dir");
        std::fs::write(
            home.join(".auth").join("models"),
            r#"{"assistant":[{"key":"ultimate","display_name":"Ultimate","enable":true}]}"#,
        )
        .expect("catalog file");
        // An empty config root is a signed-out Klaus; QODER_HOME points at the
        // stale catalog a Qoder install would have left behind.
        super::super::set_config_root(&config);
        let _home = QoderHomeGuard::set(&home);
        forget_context();

        // The file really is readable — otherwise this test would pass for the
        // wrong reason.
        let local = read_local_models();
        let warm = fetch_models_warm().await;

        assert_eq!(local.len(), 1, "the fixture catalog must parse: {local:?}");
        match warm {
            Ok(models) => panic!("signed out answered a catalog: {models:?}"),
            Err(error) => {
                assert!(
                    error.not_found,
                    "a missing credential is not-found, not a failure"
                );
                assert!(error.message.contains("not signed in"), "{}", error.message);
            }
        }
    }

    /// Drop the cached catalog. The cache is process-global, so a test that
    /// fixtures a lineup must not read the previous test's models.
    fn forget_catalog() {
        let mut guard = catalog_cache().lock().unwrap_or_else(|p| p.into_inner());
        *guard = None;
    }

    /// The declared capability set is the catalog's own `is_vl` and window, per
    /// key. `auto` is the interesting case: the service picks the model, so it may
    /// only claim vision when the WHOLE lineup can see — otherwise an image would
    /// be sent to whichever text-only model the service happened to choose.
    #[tokio::test]
    async fn model_capabilities_project_the_catalog_vision_flag() {
        let _lock = super::super::test_env_lock().lock().await;
        let fixture = tempfile::tempdir().expect("tempdir");
        let config = fixture.path().join("config");
        let home = fixture.path().join("qoder-home");
        std::fs::create_dir_all(&config).expect("config dir");
        std::fs::create_dir_all(home.join(".auth")).expect("auth dir");
        let catalog_file = home.join(".auth").join("models");
        std::fs::write(
            &catalog_file,
            r#"{"assistant":[
                {"key":"ultimate","display_name":"Ultimate","enable":true,"is_vl":true,"max_input_tokens":320000},
                {"key":"text-only","display_name":"Text","enable":true,"is_vl":false}
            ]}"#,
        )
        .expect("catalog file");
        super::super::set_config_root(&config);
        let _home = QoderHomeGuard::set(&home);
        forget_context();
        forget_catalog();

        let ultimate = model_capabilities("ultimate").await;
        assert_eq!(ultimate["vision"], json!(true));
        assert_eq!(ultimate["contextWindow"], json!(320_000));
        assert_eq!(ultimate["reasoningDialect"], json!("qoder"));

        let text_only = model_capabilities("text-only").await;
        assert_eq!(text_only["vision"], json!(false));

        // A key the catalog does not list declares nothing it cannot back.
        assert_eq!(
            model_capabilities("retired-key").await["vision"],
            json!(false)
        );

        let mixed_auto = model_capabilities("auto").await;
        assert_eq!(mixed_auto["reasoningDialect"], json!("qoder"));
        assert_eq!(
            mixed_auto["vision"],
            json!(false),
            "a mixed lineup cannot promise an eye: {mixed_auto}"
        );

        // The same rule the other way: an all-vision lineup makes `auto` safe.
        std::fs::write(
            &catalog_file,
            r#"{"assistant":[
                {"key":"ultimate","display_name":"Ultimate","enable":true,"is_vl":true},
                {"key":"performance","display_name":"Performance","enable":true,"is_vl":true}
            ]}"#,
        )
        .expect("catalog file");
        forget_catalog();
        let vision_auto = model_capabilities("auto").await;
        assert_eq!(vision_auto["vision"], json!(true), "{vision_auto}");
        assert_eq!(
            vision_auto["contextWindow"],
            json!(super::super::bridge::QODER_DEFAULT_CONTEXT_TOKENS)
        );
    }

    #[test]
    fn qoder_auto_request_effort_uses_the_same_conservative_native_ladder() {
        let models = vec![
            json!({"efforts":["minimal","high","xhigh","ultra"]}),
            json!({"efforts":["minimal","high","max"]}),
        ];
        let info = auto_model_info(&models);
        assert_eq!(info["efforts"], json!(["minimal", "high"]));
        assert_eq!(
            native_effort_capability(&info).unwrap()["levels"],
            json!(["low", "high"])
        );
        assert_eq!(
            super::super::bridge::resolve_qoder_effort(Some(&info), Some("low")),
            "minimal"
        );
        assert_eq!(
            super::super::bridge::resolve_qoder_effort(Some(&info), Some("ultra")),
            "high"
        );
        assert!(
            native_effort_capability(&auto_model_info(&[models[0].clone(), json!({})])).is_none()
        );
    }

    #[test]
    fn qoder_catalog_effort_capability_carries_real_request_overrides() {
        let entry = json!({"efforts":["minimal","low","high","xhigh","max","ultra"], "defaultEffort":"ultra"});
        let caps = capabilities_for_entry(Some(&entry));
        assert_eq!(
            caps["effort"]["levels"],
            json!(["low", "high", "xhigh", "max"])
        );
        assert!(caps["effort"].get("default").is_none());
        assert!(capabilities_for_entry(Some(&json!({"efforts":["ultra"]})))
            .get("effort")
            .is_none());
        assert!(capabilities_for_entry(None).get("effort").is_none());
    }

    /// Live probe against the real subscription image route. Run explicitly:
    /// `cargo test -p klaus-agent-server qoder_image_live -- --ignored --nocapture`.
    /// It reads the real credential and spends one generation, so it needs the
    /// machine's own config root — `--ignored` (not `--include-ignored`) keeps the
    /// fixtured tests above from repointing it.
    #[tokio::test]
    #[ignore = "live network call against the real subscription"]
    async fn qoder_image_live_probe() {
        let bytes = generate_image("a small gray kitten sticker", None, "image-gen-live-probe")
            .await
            // `QoderError` carries no Debug (it is reported, not formatted), so the
            // message is what a failure prints here.
            .map_err(|error| error.message)
            .expect("the image route answered");
        assert!(
            bytes.len() > 1024,
            "an image is at least a KB, got {}",
            bytes.len()
        );
        assert!(
            bytes.starts_with(b"\x89PNG\r\n\x1a\n"),
            "the route answers PNG"
        );
        eprintln!("qoder image route ok: {} bytes", bytes.len());
    }
    #[test]
    fn speed_capability_requires_both_boolean_choices_and_preserves_native_aliases() {
        let switch = json!({"key":"highspeed","type":"bool","options":{"true":{},"false":{}}});
        for alias in ["feature_switches", "functionSwitches", "function_switches"] {
            let mut entry = json!({});
            entry[alias] = json!([switch.clone()]);
            assert_eq!(capabilities_for_entry(Some(&entry))["fastMode"], true);
        }
        for entry in [
            json!({}),
            json!({"feature_switches":[{"key":"highspeed","type":"bool","options":{"true":{}}}]}),
            json!({"feature_switches":[{"key":"highspeed","type":"string","options":{"true":{},"false":{}}}]}),
        ] {
            assert_eq!(capabilities_for_entry(Some(&entry))["fastMode"], false);
        }
    }
}
