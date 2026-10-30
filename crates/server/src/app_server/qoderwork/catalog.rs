//! QoderWork subscription signing + model catalog. The signed-in profile comes
//! from Klaus's own credential store ([`super::auth`]); this module turns it into
//! a signing context ([`super::sign`]) and projects the model catalog for the
//! picker.
//!
//! Signing is native and needs nothing installed beside Klaus: no QoderWork CLI,
//! no wasm lifted out of one. Being signed out is still never papered over —
//! callers get a plain "not signed in" instead of an unsigned request.
//!
//! QoderWork is served by the same gateway as the rest of the Qoder family and
//! answers the same signed routes; what is its own is the `qwork` catalog scene
//! and the signing profile the account was issued under.

use super::sign::{SignedRequest, SigningContext};

/// QoderWork's catalog route asks for the encoded response envelope, unlike the
/// plain-JSON read the rest of the Qoder family uses.
const MODEL_LIST_PATH: &str = "/algo/api/v2/model/list?Encode=1";
use serde_json::{json, Map, Value};
use std::path::PathBuf;
use std::sync::Mutex;

fn api_base() -> String {
    std::env::var("QODERWORK_API_BASE")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "https://api3.qoder.sh".into())
}

const CATALOG_TTL_MS: i64 = 6 * 60 * 60 * 1000;
const CATALOG_FETCH_TIMEOUT_MS: u64 = 10_000;
/// How long the picker's cold start waits for the live catalog before painting
/// the on-disk fallback, which can lag the live catalog by weeks.
const WARM_CATALOG_TIMEOUT_MS: u64 = 1500;

/// `Cosy-ClientType`. `5` (the CLI default) routes Ultimate to a broken pipeline
/// node; `6` — the desktop IDE type Klaus actually is — resolves correctly. The
/// login and refresh calls in [`super::auth`] send the same value, so one account
/// never looks like two different clients.
pub(crate) const DESKTOP_CLIENT_TYPE: &str = "6";

/// Candidate directories holding the official QoderWork CLI's plain-JSON model
/// cache. Only the on-disk catalog fallback reads them — the credential and the
/// signer never depend on a QoderWork install existing at all, and a signed live
/// refresh stays the authority. `QODERWORK_HOME` wins when a test sets it.
fn local_catalog_dirs() -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    if let Ok(value) = std::env::var("QODERWORK_HOME") {
        let root = PathBuf::from(value.trim());
        if !value.trim().is_empty() {
            dirs.extend([root.join("cli/.auth"), root.join(".auth"), root]);
        }
    }
    let home = PathBuf::from(std::env::var("HOME").unwrap_or_default());
    // The desktop client's own config root, which holds the same cache layout.
    let own = home.join(".qoderwork");
    dirs.extend([own.join(".auth"), own.join("cli/.auth")]);
    let support = if cfg!(target_os = "macos") {
        home.join("Library/Application Support")
    } else if cfg!(target_os = "windows") {
        std::env::var("APPDATA")
            .map(PathBuf::from)
            .unwrap_or_else(|_| home.join("AppData/Roaming"))
    } else {
        home.join(".config")
    };
    let root = support.join("QoderWork");
    dirs.extend([root.join("cli/.auth"), root.join("cli")]);
    dirs
}

/// The model QoderWork's client asks for when the user has not picked one.
///
/// This is the authoritative default, not merely a fallback: [`default_key_in`]
/// prefers it over whatever entry the service marks `is_default`, because that
/// flag has been observed pointing at different entries across catalog generations
/// on one machine, and a default that moves with it is not a default the user can
/// reason about. It only yields when the catalog does not serve this key at all.
pub(crate) const DEFAULT_MODEL: &str = "qmodel_38max";

/// Errors carry the "the store simply isn't there" bit separately, because
/// callers must report a missing store as *signed out* rather than as an *error*.
pub struct QoderWorkError {
    pub message: String,
    pub not_found: bool,
    /// The gateway rejected the access token as a dead login (403 code 105).
    /// The official client's store may hold a fresher token, so the catalog path
    /// reports this as a stable code and lets the host reread that store — there
    /// is no refresh-token retry behind it, because spending one would invalidate
    /// the copy the official client is using.
    pub login_expired: bool,
}

impl QoderWorkError {
    fn msg(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            not_found: false,
            login_expired: false,
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
    action: impl FnOnce(&SigningContext, &Value) -> Result<T, QoderWorkError>,
) -> Result<T, QoderWorkError> {
    // No stored credential is "signed out", not a failure: the settings pane and
    // the auth pill both render it as a state the user can act on.
    let Some((machine_id, user_info)) = super::auth::credential() else {
        return Err(QoderWorkError {
            message: "QoderWork subscription is not signed in".into(),
            not_found: true,
            login_expired: false,
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
        let context = SigningContext::new_with_profile(
            &machine_id,
            DESKTOP_CLIENT_TYPE,
            &user_info,
            super::sign::CosyProfile::QODERWORK,
        )
        .map_err(QoderWorkError::msg)?;
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

pub struct QoderWorkToken {
    pub access_token: String,
    pub account: String,
}

/// getQoderAccessToken. Errors when signed out, while still letting the caller
/// tell "no credential at all" (the `not_found` bit) from "a profile with no
/// token in it" (the `not signed in` message).
pub fn access_token() -> Result<QoderWorkToken, QoderWorkError> {
    with_context(|_, user_info| {
        let access_token = user_info
            .get("access_token")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if access_token.is_empty() {
            return Err(QoderWorkError::msg(
                "QoderWork subscription is not signed in",
            ));
        }
        Ok(QoderWorkToken {
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
    let scene_root = scene_root(parsed);
    // The gateway ships a synthetic `auto` service route in the lineup. The
    // picker offers no automatic row — a bare default and unmapped tiers name the
    // service's own default instead — so the route must not surface as a choice,
    // and every default fallback has to name a real model, not it.
    let projected: Vec<Value> = parse_model_scene_names(scene_root, &["qwork"])
        .into_iter()
        .filter(|model| model.get("key").and_then(Value::as_str) != Some("auto"))
        .collect();
    attach_service_default(scene_root, projected)
}

/// Some gateway builds wrap the scene object in a `data` envelope and some do
/// not. Unwrapped once here so the projection and the default flag are read from
/// the SAME object: reading one from the wrapper and the other from the root is
/// how a well-formed answer turns into an empty catalog.
fn scene_root(parsed: &Value) -> &Value {
    match parsed.get("qwork") {
        Some(_) => parsed,
        None => parsed.get("data").unwrap_or(parsed),
    }
}

/// The shared projection carries only what a picker row needs, so it drops the
/// `is_default` flag the scene rows arrive with. Which entry the service calls
/// its own default is this service's answer, not Klaus's to guess, and the two
/// do not always agree: a live catalog read declared the premium tier default
/// while the older catalog generation on the same machine declared the standard
/// one. Re-attached by key, from the same scene the rows were projected from, so
/// a bare default follows whatever generation is actually in hand.
fn attach_service_default(scene_root: &Value, projected: Vec<Value>) -> Vec<Value> {
    let Some(rows) = scene_root.get("qwork").and_then(Value::as_array) else {
        return projected;
    };
    let default_keys: std::collections::HashSet<&str> = rows
        .iter()
        .filter(|row| {
            row.get("is_default") == Some(&json!(true))
                || row.get("isDefault") == Some(&json!(true))
        })
        .filter_map(|row| row.get("key").and_then(Value::as_str))
        .collect();
    if default_keys.is_empty() {
        return projected;
    }
    projected
        .into_iter()
        .map(|mut model| {
            let is_default = model
                .get("key")
                .and_then(Value::as_str)
                .is_some_and(|key| default_keys.contains(key));
            if let Some(target) = model.as_object_mut() {
                target.insert("is_default".into(), json!(is_default));
            }
            model
        })
        .collect()
}

/// The gateway reports a dead login as HTTP 403 with `{"code":"105"}`; the body
/// may arrive through the same `Encode=1` envelope as a success, so decode before
/// reading the code.
fn is_login_expired_body(body: &str) -> bool {
    let decoded = super::sign::decode_server_response(body).unwrap_or(body.to_string());
    let parsed: Value = serde_json::from_str(&decoded).unwrap_or(Value::Null);
    matches!(
        parsed.get("code"),
        Some(Value::String(text)) if text.trim() == "105"
    ) || parsed.get("code") == Some(&json!(105))
}

/// Project one of the COSY model scenes into the engine's shared catalog shape.
/// Qoder serves `assistant`/`chat`; QoderWork serves `qwork` on the same signed
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
            super::super::conversation_catalog::mark_purpose("qoderwork", model, &mut out);
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

/// The catalog a DEFAULT resolution may consult: the fresh in-process cache, a
/// bounded live refresh, then Klaus's own on-disk snapshot.
///
/// Deliberately narrower than [`fetch_models`], which also falls back to the
/// official client's cache directory. That fallback is fine for painting a picker
/// — the list is the same family of models either way — but a default decides
/// which model the account actually runs on, so it must not be chosen by whatever
/// another install on this machine happened to leave behind.
pub(crate) async fn models_for_default() -> Vec<Value> {
    {
        let guard = catalog_cache().lock().unwrap_or_else(|p| p.into_inner());
        if let Some(cache) = guard
            .as_ref()
            .filter(|cache| cache.fingerprint == super::auth::catalog_fingerprint())
        {
            if now_ms() < cache.expires_at {
                return cache.models.clone();
            }
        }
    }
    if let Ok(models) = refresh_live_catalog().await {
        if !models.is_empty() {
            return models;
        }
    }
    read_owned_catalog().unwrap_or_default()
}

/// The key a bare default names, from what this process already holds: the fresh
/// in-process cache, else Klaus's own on-disk snapshot. Neither is another
/// product's cache directory and neither fetches, so a selection resolution never
/// waits on the network. The precedence is [`default_key_in`]'s — the client's own
/// default key when this catalog serves it, then the entry the service marks
/// default, then the list head. `None` leaves the caller on that client default.
pub(crate) fn default_model_key() -> Option<String> {
    {
        let guard = catalog_cache().lock().unwrap_or_else(|p| p.into_inner());
        if let Some(cache) = guard
            .as_ref()
            .filter(|cache| cache.fingerprint == super::auth::catalog_fingerprint())
        {
            if now_ms() < cache.expires_at {
                return default_key_in(&cache.models);
            }
        }
    }
    // Klaus's own snapshot, never another product's cache directory: a model
    // selection must not depend on which clients this machine happens to have
    // installed.
    default_key_in(&read_owned_catalog().unwrap_or_default())
}

/// Resolve a requested key against the real catalog. An exact key wins; a bare
/// default or the service's legacy `auto` alias names the client's own default,
/// falling back to the catalog head when that entry is gone.
pub(crate) fn resolve_model_key(models: &[Value], requested: &str) -> String {
    let requested = if requested.trim().is_empty() {
        "auto"
    } else {
        requested.trim()
    };
    let has = |key: &str| {
        models
            .iter()
            .any(|model| model.get("key").and_then(Value::as_str) == Some(key))
    };
    if has(requested) {
        return requested.to_string();
    }
    if matches!(requested, "auto" | DEFAULT_MODEL) {
        if let Some(key) = default_key_in(models) {
            return key;
        }
    }
    models
        .iter()
        .find_map(|model| model.get("key").and_then(Value::as_str))
        .unwrap_or(requested)
        .to_string()
}

/// The entry the service marks as its own default, or `None` when the catalog
/// declares none. Strict: it never substitutes the list head, because the head is
/// only coincidentally the default for one catalog generation.
fn declared_default_key(models: &[Value]) -> Option<String> {
    models
        .iter()
        .find(|model| {
            model.get("is_default") == Some(&json!(true))
                || model.get("isDefault") == Some(&json!(true))
        })
        .and_then(|model| model.get("key"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// The key a bare default names, in the one precedence both readers follow: the
/// client's own default key when this catalog serves it, then the entry the service
/// marks default, then the list head. The client key comes first because the
/// default a user gets is Klaus's product decision, and the service's flag has been
/// observed pointing at different entries across catalog generations on one
/// machine. Two readers that disagreed would make the picker show one model and the
/// request run on another.
pub(crate) fn default_key_in(models: &[Value]) -> Option<String> {
    let served = |key: &str| {
        models
            .iter()
            .any(|model| model.get("key").and_then(Value::as_str) == Some(key))
    };
    if served(DEFAULT_MODEL) {
        return Some(DEFAULT_MODEL.to_string());
    }
    if let Some(key) = declared_default_key(models) {
        return Some(key);
    }
    first_key_in(models)
}

/// The catalog's first entry key.
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

/// How young another product's on-disk catalog may be before the settings list
/// will paint it. That file is a cache of whatever lineup the official client
/// last fetched, and a machine that signed in months ago keeps it forever: a
/// half-year-old file once painted a retired four-model lineup over the live
/// nine-model one. Young enough to be a useful cold-start paint, old enough to
/// reject a cache from a previous catalog generation.
const FOREIGN_CACHE_MAX_AGE_MS: i64 = 7 * 24 * 60 * 60 * 1000;

/// Klaus's own non-secret catalog snapshot first, then the official CLI's
/// plain-JSON cache while it is still young. The snapshot is written on every
/// successful live refresh, so a cold process paints what this install actually
/// saw rather than what another product's install happened to leave behind.
fn read_local_models() -> Vec<Value> {
    if let Some(models) = read_owned_catalog() {
        return models;
    }
    local_catalog_dirs()
        .into_iter()
        .find_map(|dir| {
            let path = dir.join("models");
            let modified = std::fs::metadata(&path).ok()?.modified().ok()?;
            let age_ms = std::time::SystemTime::now()
                .duration_since(modified)
                .ok()?
                .as_millis();
            if age_ms > FOREIGN_CACHE_MAX_AGE_MS as u128 {
                return None;
            }
            std::fs::read_to_string(path)
                .ok()
                .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
                .map(|parsed| parse_model_scenes(&parsed))
                .filter(|models| !models.is_empty())
        })
        .unwrap_or_default()
}

/// The catalog Klaus wrote itself, under the config root. Non-secret by
/// construction: model keys, display names and capability flags, no credential.
fn owned_catalog_path() -> PathBuf {
    super::config_root().join("qoderwork-model-catalog.json")
}

fn read_owned_catalog() -> Option<Vec<Value>> {
    let fingerprint = super::auth::catalog_fingerprint()?;
    std::fs::read_to_string(owned_catalog_path())
        .ok()
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
        .filter(|value| {
            value.get("projectionVersion").and_then(Value::as_u64) == Some(1)
                && value.get("fingerprint").and_then(Value::as_str) == Some(fingerprint.as_str())
        })
        .and_then(|value| value.get("models").and_then(Value::as_array).cloned())
        .filter(|models| !models.is_empty())
}

fn write_owned_catalog(models: &[Value], fingerprint: &str) {
    let path = owned_catalog_path();
    if let Some(parent) = path.parent() {
        if std::fs::create_dir_all(parent).is_err() {
            return;
        }
    }
    let Ok(serialized) = serde_json::to_string_pretty(&json!({
        "projectionVersion": 1,
        "updatedAt": now_ms(),
        "fingerprint": fingerprint,
        "models": models,
    })) else {
        return;
    };
    if std::fs::write(&path, serialized).is_ok() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
        }
    }
}

/// Sign and issue the catalog GET, then parse. QoderWork's route asks for the
/// `Encode=1` response envelope, so the body is decoded before the scene is read.
async fn fetch_live_models() -> Result<Vec<Value>, QoderWorkError> {
    let signed: SignedRequest =
        with_context(|context, _| Ok(context.sign_get(&api_base(), MODEL_LIST_PATH)))?;
    let client = reqwest::Client::builder()
        // Without a deadline a slow catalog endpoint pins every caller (the
        // model pill would sit on "no models" for the whole hang).
        .timeout(std::time::Duration::from_millis(CATALOG_FETCH_TIMEOUT_MS))
        .build()
        .map_err(|e| QoderWorkError::msg(e.to_string()))?;
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
        .map_err(|e| QoderWorkError::msg(e.to_string()))?;
    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    if !status.is_success() {
        let detail: String = text.chars().take(200).collect();
        return Err(QoderWorkError {
            message: format!("model/list {}: {detail}", status.as_u16()),
            not_found: false,
            login_expired: status.as_u16() == 403 && is_login_expired_body(&text),
        });
    }
    // Decoding falls back to the body it was given, which keeps a test gateway or
    // a gradual rollout that answers plain JSON working.
    let decoded = super::sign::decode_server_response(&text).unwrap_or(text);
    let parsed = serde_json::from_str::<Value>(&decoded)
        .map_err(|e| QoderWorkError::msg(format!("model/list body is not JSON: {e}")))?;
    Ok(parse_model_scenes(&parsed))
}

/// fetchQoderModels mirror: stale-while-revalidate over a 6h live TTL. Cold
/// starts paint instantly from whatever catalog is at hand (expired live cache,
/// else Klaus's own snapshot, else a young official-client cache) while a
/// refresh runs in the background; a local file never becomes terminal when a
/// live fetch can land, because it may lack `thinking_config` and newer models.
///
/// Signed out this read errors like the warm one: a disk file from another
/// product's install is not this account's lineup.
///
/// There is deliberately no "skip the cache" flag: a caller that wants the live
/// catalog wants it because the cache may be stale, which is what the background
/// revalidate below already fixes.
pub async fn fetch_models() -> Result<Vec<Value>, QoderWorkError> {
    // Signed out there is no lineup of this account's to paint: the on-disk
    // files belong to another product's install (or to a login this Klaus has
    // never seen), so falling back to them would show a full picker whose every
    // option fails on the first turn while the settings pane truthfully says
    // "not signed in". Same rule as the warm read.
    if super::auth::credential().is_none() {
        return Err(QoderWorkError {
            message: "QoderWork subscription is not signed in".into(),
            not_found: true,
            login_expired: false,
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
                eprintln!("[QoderWorkBridge] background catalog refresh failed, keeping the stale catalog: {}", error.message);
            }
        });
        return Ok(stale);
    }
    match refresh_live_catalog().await {
        Ok(models) if !models.is_empty() => Ok(models),
        Ok(_) => Ok(read_local_models()),
        Err(error) => {
            eprintln!(
                "[QoderWorkBridge] live model catalog failed, falling back to the local cache: {}",
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

async fn refresh_live_catalog() -> Result<Vec<Value>, QoderWorkError> {
    let fingerprint = super::auth::catalog_fingerprint();
    let models = fetch_live_models().await?;
    if fingerprint != super::auth::catalog_fingerprint() {
        return Ok(models);
    }
    if !models.is_empty() {
        if let Some(fingerprint) = fingerprint.as_deref() {
            write_owned_catalog(&models, fingerprint);
        }
        let mut guard = catalog_cache().lock().unwrap_or_else(|p| p.into_inner());
        *guard = Some(CatalogCache {
            fingerprint: fingerprint.clone(),
            models: models.clone(),
            expires_at: now_ms() + CATALOG_TTL_MS,
        });
    }
    Ok(models)
}

/// The picker's catalog read. Two deliberate differences from `fetch_models`:
/// signed out it errors instead of painting the on-disk fallback (the settings
/// pane reports "not signed in", and the picker must agree with an empty list
/// rather than fabricating options from an unrelated file). A matching owned
/// snapshot paints immediately and revalidates in the background. Without one,
/// the live catalog gets a bounded first chance; a hot cache serves instantly.
pub async fn fetch_models_warm() -> Result<Vec<Value>, QoderWorkError> {
    if super::auth::credential().is_none() {
        return Err(QoderWorkError {
            message: "QoderWork subscription is not signed in".into(),
            not_found: true,
            login_expired: false,
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
                eprintln!(
                    "[QoderWorkBridge] catalog refresh failed: {}",
                    error.message
                );
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
                "[QoderWorkBridge] background catalog refresh failed, keeping the stale catalog: {}",
                error.message
            );
        }
    });
    Ok(stale)
}

// ------------------------------------------------------------- inference bridge

/// Sign a QoderWork inference (`agent_chat_generation`) request. The signer puts the
/// body on the wire as the `Encode=1` envelope (see [`super::sign::INFER_PATH`] for
/// why a plaintext body cannot survive the gateway's WAF), so callers hand it
/// plain JSON and never encode themselves. Used by the loopback bridge
/// ([`super::bridge`]).
pub(crate) fn sign_infer_request(
    body_json: &str,
    model_key: &str,
) -> Result<SignedRequest, QoderWorkError> {
    with_context(|context, _| Ok(context.sign_infer(&api_base(), body_json, model_key, "system")))
}

/// The image-generation route. The request is
/// {payload: JSON.stringify({model, prompt, size}), encodeVersion, sessionId,
/// requestId} and the answer is {data:[{url}]}, where the url is either an https
/// link or a data: URL. Two wire facts a probe settled against this gateway: the
/// route lives behind the /algo prefix like the inference family, and `Encode=1`
/// is mandatory — the body must carry the obfuscated envelope from
/// [`super::sign::encode_body`], which is also what the signature covers. The
/// response itself is plain JSON.
const IMAGE_GEN_PATH: &str = "/algo/api/v2/service/pro/generateImage?Encode=1";

/// The image route's base+path, overridable so probes can settle which prefix the
/// deployment actually serves. Named for THIS source: sharing the sibling's
/// override would let one source's probe silently retarget the other's requests.
fn image_gen_url() -> String {
    std::env::var("KLAUS_QODERWORK_IMAGE_GEN_URL")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| format!("{}{}", api_base().trim_end_matches('/'), IMAGE_GEN_PATH))
}

/// The service's default image model, used when no model policy overrides it.
/// KLAUS_QODERWORK_IMAGE_MODEL overrides it for probes.
const IMAGE_GEN_MODEL: &str = "ge3i";

/// Generate one image through the QoderWork subscription. Returns the raw image
/// bytes (downloading the https URL or decoding the data: URL the answer
/// carries). Text-to-image only: the route accepts no source images.
pub(crate) async fn generate_image(
    prompt: &str,
    size: Option<&str>,
    session_id: &str,
) -> Result<Vec<u8>, QoderWorkError> {
    let model = std::env::var("KLAUS_QODERWORK_IMAGE_MODEL")
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
        .map_err(|e| QoderWorkError::msg(e.to_string()))?;
    let mut request = client.post(&signed.url);
    for (name, value) in &signed.headers {
        request = request.header(name, value);
    }
    let response = request
        .body(signed.body.clone().unwrap_or_default())
        .send()
        .await
        .map_err(|e| QoderWorkError::msg(e.to_string()))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|e| QoderWorkError::msg(e.to_string()))?;
    if std::env::var_os("KLAUS_QODERWORK_IMAGE_GEN_DEBUG").is_some() {
        eprintln!(
            "[qoderwork-image] status={} body head: {:.300}",
            status.as_u16(),
            text
        );
    }
    if !status.is_success() {
        let detail: String = text.chars().take(200).collect();
        return Err(QoderWorkError::msg(format!(
            "generateImage {}: {detail}",
            status.as_u16()
        )));
    }
    let parsed: Value = serde_json::from_str(&text)
        .map_err(|e| QoderWorkError::msg(format!("generateImage body is not JSON: {e}")))?;
    let url = parsed
        .get("data")
        .and_then(Value::as_array)
        .and_then(|data| data.first())
        .and_then(|item| item.get("url"))
        .and_then(Value::as_str)
        .filter(|url| !url.is_empty())
        .ok_or_else(|| QoderWorkError::msg("generateImage answered without data[0].url"))?;
    fetch_image_bytes(&client, url).await
}

/// The answer's url is either a data: URL (decode in place) or an https link
/// (download it).
async fn fetch_image_bytes(client: &reqwest::Client, url: &str) -> Result<Vec<u8>, QoderWorkError> {
    if let Some((_, encoded)) = url.split_once("base64,") {
        use base64::Engine as _;
        return base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|e| QoderWorkError::msg(format!("undecodable data URL image: {e}")));
    }
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|e| QoderWorkError::msg(e.to_string()))?;
    let status = response.status();
    if !status.is_success() {
        return Err(QoderWorkError::msg(format!(
            "image download HTTP {}",
            status.as_u16()
        )));
    }
    let bytes = response
        .bytes()
        .await
        .map_err(|e| QoderWorkError::msg(e.to_string()))?;
    Ok(bytes.to_vec())
}

/// The image-generation route: signed plain-JSON POST, same signature tuple as
/// the inference route. Returns the signed request for the caller to send.
pub(crate) fn sign_image_gen_request(body_json: &str) -> Result<SignedRequest, QoderWorkError> {
    with_context(|context, _| Ok(context.sign_post_url(&image_gen_url(), body_json)))
}

/// The gateway host serving the signed one-shot search route. The search
/// service lives on its own host, apart from the inference shards.
fn search_base() -> String {
    std::env::var("KLAUS_QODERWORK_SEARCH_BASE")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "https://center.qoder.sh".to_string())
}

/// The signed one-shot search route: the wire body is
/// `{payload: <stringified search request>, encodeVersion}` and the answer is
/// plain JSON holding `pageItems`. Returns the signed request for the caller
/// to send.
pub(crate) fn sign_search_post(body_json: &str) -> Result<SignedRequest, QoderWorkError> {
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

fn capabilities_for_entry(entry: Option<&Value>) -> Value {
    let mut caps = json!({
        "vision": entry.is_some_and(entry_is_vision),
        "contextWindow": super::bridge::resolve_qoder_context_length(entry),
        "reasoningDialect":"qoder",
        "fastMode": entry.and_then(super::super::qoder::catalog::fast_mode_switch).is_some(),
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
    crate::app_server::catalog_test_support::warm_catalog_tests!("qoderwork", || {
        read_owned_catalog().unwrap_or_default()
    });

    /// `QODERWORK_HOME` for the length of one test, then back as it was.
    ///
    /// Environment mutation goes through core's process-environment API, never
    /// `std::env::set_var`: the process environment is shared with exact parent
    /// capture, and a repository-wide guard test fails on any raw mutation outside
    /// that lock. Restoring on drop rather than at the end of the body so a failed
    /// assertion cannot leak a temp path into the rest of the binary.
    struct QoderWorkHomeGuard {
        previous: Option<String>,
    }

    impl QoderWorkHomeGuard {
        fn set(path: &std::path::Path) -> Self {
            let previous = std::env::var("QODERWORK_HOME").ok();
            klaus_agent_core::runtime_process::set_process_environment_var(
                "QODERWORK_HOME",
                path.as_os_str(),
            );
            Self { previous }
        }
    }

    impl Drop for QoderWorkHomeGuard {
        fn drop(&mut self) {
            match &self.previous {
                Some(value) => klaus_agent_core::runtime_process::set_process_environment_var(
                    "QODERWORK_HOME",
                    value,
                ),
                None => klaus_agent_core::runtime_process::remove_process_environment_var(
                    "QODERWORK_HOME",
                ),
            }
        }
    }

    /// The gateway ships a synthetic `auto` service route in the lineup. It
    /// names no model of its own, so it must never surface as a picker row,
    /// and the catalog head every default falls back to must be a real model.
    #[test]
    fn the_catalog_drops_the_synthetic_auto_route() {
        let parsed = serde_json::json!({
            "qwork": [
                { "key": "auto", "display_name": "Auto", "enable": true },
                { "key": "qwork-ultimate", "display_name": "Premium", "enable": true }
            ]
        });
        let models = parse_model_scenes(&parsed);
        let keys: Vec<&str> = models
            .iter()
            .filter_map(|model| model.get("key").and_then(Value::as_str))
            .collect();
        assert_eq!(
            keys,
            vec!["qwork-ultimate"],
            "auto must not be a picker row: {models:?}"
        );
        assert_eq!(first_key_in(&models).as_deref(), Some("qwork-ultimate"));
    }

    /// A signed-out profile must not paint the on-disk catalog file. The file is
    /// whatever an installed QoderWork client last cached, so painting it fabricates
    /// a full model picker while the settings pane truthfully says "not signed in"
    /// — and every option in it would fail on the first turn. Neither the settings
    /// read ([`fetch_models`]) nor the picker's warm read may fall back to it.
    #[tokio::test]
    async fn the_warm_read_refuses_the_local_catalog_while_signed_out() {
        let _lock = super::super::test_env_lock().lock().await;
        let fixture = tempfile::tempdir().expect("tempdir");
        let config = fixture.path().join("config");
        let home = fixture.path().join("qoderwork-home");
        std::fs::create_dir_all(&config).expect("config dir");
        std::fs::create_dir_all(home.join(".auth")).expect("auth dir");
        std::fs::write(
            home.join(".auth").join("models"),
            r#"{"qwork":[{"key":"qwork-ultimate","display_name":"Premium","enable":true}]}"#,
        )
        .expect("catalog file");
        // An empty config root is a signed-out Klaus; QODERWORK_HOME points at the
        // stale catalog a QoderWork install would have left behind.
        super::super::set_config_root(&config);
        let _home = QoderWorkHomeGuard::set(&home);
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
        // The settings read follows the same rule: signed out, another product's
        // cache is not this account's lineup.
        match fetch_models().await {
            Ok(models) => panic!("signed out answered a catalog: {models:?}"),
            Err(error) => assert!(error.not_found, "{}", error.message),
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
    /// The lineup rides the process cache behind a fixture credential: a catalog
    /// read is a signed-in read, and it must not depend on another product's
    /// on-disk cache nor reach the network.
    #[tokio::test]
    async fn model_capabilities_project_the_catalog_vision_flag() {
        let _lock = super::super::test_env_lock().lock().await;
        let fixture = tempfile::tempdir().expect("tempdir");
        let config = fixture.path().join("config");
        let home = fixture.path().join("qoderwork-home");
        std::fs::create_dir_all(&config).expect("config dir");
        std::fs::create_dir_all(home.join(".auth")).expect("auth dir");
        std::fs::write(
            config.join("qoderwork-subscription-oauth.json"),
            r#"{"machineId":"test-machine","userInfo":{"access_token":"test-token","security_oauth_token":"test-token","expire_time":4102444800000}}"#,
        )
        .expect("credential store");
        super::super::set_config_root(&config);
        let _home = QoderWorkHomeGuard::set(&home);
        forget_context();
        let seed = |models: Vec<Value>| {
            forget_catalog();
            let mut guard = catalog_cache().lock().unwrap_or_else(|p| p.into_inner());
            *guard = Some(CatalogCache {
                fingerprint: super::super::auth::catalog_fingerprint(),
                models,
                expires_at: now_ms() + 60_000,
            });
        };
        seed(vec![
            json!({"key":"qwork-ultimate","display_name":"Premium","enable":true,"is_vl":true,"max_input_tokens":320000}),
            json!({"key":"text-only","display_name":"Text","enable":true,"is_vl":false}),
        ]);

        let ultimate = model_capabilities("qwork-ultimate").await;
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
        seed(vec![
            json!({"key":"ultimate","display_name":"Ultimate","enable":true,"is_vl":true}),
            json!({"key":"performance","display_name":"Performance","enable":true,"is_vl":true}),
        ]);
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

    /// The default a bare selection names comes from Klaus's own snapshot, never
    /// from the official client's cache directory. A selection that resolved
    /// through a third-party cache would answer differently depending on which
    /// products the machine happens to have installed — and would name a model
    /// this account was never offered.
    #[tokio::test]
    async fn the_catalog_head_comes_from_klaus_snapshot_not_a_third_party_cache() {
        let _lock = super::super::test_env_lock().lock().await;
        let fixture = tempfile::tempdir().expect("tempdir");
        let config = fixture.path().join("config");
        let home = fixture.path().join("qoderwork-home");
        std::fs::create_dir_all(&config).expect("config dir");
        std::fs::create_dir_all(home.join(".auth")).expect("auth dir");
        // The official CLI cache another product left behind. Its head is a model
        // this account never sees.
        std::fs::write(
            home.join(".auth").join("models"),
            r#"{"qwork":[{"key":"someone-elses-head","display_name":"Foreign","enable":true}]}"#,
        )
        .expect("third-party catalog file");
        super::super::set_config_root(&config);
        let _home = QoderWorkHomeGuard::set(&home);
        forget_context();
        forget_catalog();

        // With no snapshot of its own, the default names the client's own model
        // rather than the foreign head.
        assert_eq!(default_model_key(), None, "no snapshot, no catalog head");
        assert_eq!(
            super::super::wire_model("auto"),
            DEFAULT_MODEL,
            "with no snapshot the client default stands"
        );

        // A live refresh writes the snapshot, and from then on the head is this
        // install's own answer.
        crate::app_server::catalog_test_support::store_credential("qoderwork", &config, "head");
        let fingerprint = super::super::auth::catalog_fingerprint().expect("fingerprint");
        write_owned_catalog(
            &[json!({"key":"qwork-ultimate","display_name":"Premium"})],
            &fingerprint,
        );
        forget_catalog();
        assert_eq!(default_model_key().as_deref(), Some("qwork-ultimate"));

        // The foreign cache stays readable as the last resort for the settings
        // list once this install has no snapshot of its own; it just never decides
        // the default.
        let _ = std::fs::remove_file(owned_catalog_path());
        forget_catalog();
        assert_eq!(
            read_local_models()
                .iter()
                .filter_map(|model| model.get("key").and_then(Value::as_str))
                .collect::<Vec<_>>(),
            vec!["someone-elses-head"],
            "the official cache is still the last-resort list"
        );
        assert_eq!(default_model_key(), None, "but never the catalog head");
    }

    /// Another product's cache is a cold-start paint, not an archive: a file the
    /// official client wrote months ago describes a retired lineup, and painting
    /// it over the live catalog is how a four-model picker once survived a
    /// nine-model gateway. The mtime gate keeps the young file and drops the old
    /// one; Klaus's own snapshot has no age limit because every refresh rewrites
    /// it.
    #[tokio::test]
    async fn a_stale_foreign_cache_never_paints() {
        let _lock = super::super::test_env_lock().lock().await;
        let fixture = tempfile::tempdir().expect("tempdir");
        let config = fixture.path().join("config");
        let home = fixture.path().join("qoderwork-home");
        std::fs::create_dir_all(&config).expect("config dir");
        std::fs::create_dir_all(home.join(".auth")).expect("auth dir");
        let catalog_file = home.join(".auth").join("models");
        std::fs::write(
            &catalog_file,
            r#"{"qwork":[{"key":"retired-lineup","display_name":"Old","enable":true}]}"#,
        )
        .expect("third-party catalog file");
        super::super::set_config_root(&config);
        let _home = QoderWorkHomeGuard::set(&home);
        forget_context();

        // Freshly written: a legitimate cold-start paint.
        assert_eq!(
            read_local_models()
                .iter()
                .filter_map(|model| model.get("key").and_then(Value::as_str))
                .collect::<Vec<_>>(),
            vec!["retired-lineup"],
            "a young foreign cache still paints"
        );

        // Backdate past the gate: the same bytes are now a retired generation.
        let old = std::time::SystemTime::now()
            - std::time::Duration::from_millis((FOREIGN_CACHE_MAX_AGE_MS + 60 * 60 * 1000) as u64);
        std::fs::File::options()
            .write(true)
            .open(&catalog_file)
            .expect("reopen")
            .set_modified(old)
            .expect("backdate mtime");
        assert!(
            read_local_models().is_empty(),
            "a foreign cache older than the gate must not paint"
        );
    }

    /// An unmapped tier and a bare default name the client's own default model,
    /// and an explicit catalog key stands as itself.
    #[test]
    fn a_requested_key_resolves_against_the_real_catalog() {
        let models = vec![
            json!({"key":"qwork-ultimate","display_name":"Premium","enable":true}),
            json!({"key":"qwork-auto","display_name":"Standard","enable":true}),
            json!({"key":DEFAULT_MODEL,"display_name":"Qwen3.8-Max","enable":true}),
        ];
        assert_eq!(
            resolve_model_key(&models, "qwork-ultimate"),
            "qwork-ultimate"
        );
        assert_eq!(resolve_model_key(&models, "auto"), DEFAULT_MODEL);
        assert_eq!(resolve_model_key(&models, ""), DEFAULT_MODEL);
        // A retired key falls back to the catalog head rather than going to the
        // wire as something the service cannot serve.
        assert_eq!(resolve_model_key(&models, "retired"), "qwork-ultimate");
    }

    /// A bare default follows the client's own default model when the service
    /// serves it, then the entry the service marks `is_default`, then the head.
    ///
    /// The client key comes first because which model an unpicked conversation runs
    /// on is Klaus's product decision, and the service's own flag has been observed
    /// pointing at different entries across catalog generations on one machine — a
    /// default that moves with it is not one a user can reason about. The shared
    /// projection drops that flag, so this source re-attaches it for the second
    /// rung. Both readers are checked at every rung: the picker's pre-catalog
    /// default and the request-time resolver have to name the same model, or the
    /// picker shows one model and the request runs on another.
    #[test]
    fn a_bare_default_prefers_the_client_default_then_the_service_flag_then_the_head() {
        let parsed = serde_json::json!({
            "qwork": [
                { "key": "qwork-ultimate", "display_name": "Premium", "enable": true, "is_default": false },
                { "key": "qwork-auto", "display_name": "Standard", "enable": true, "is_default": true },
                { "key": DEFAULT_MODEL, "display_name": "Qwen3.8-Max", "enable": true, "is_default": false }
            ]
        });
        let models = parse_model_scenes(&parsed);
        // The service's flag survives the shared projection, and still names its
        // own entry — it is simply outranked by the client's default.
        assert_eq!(
            models
                .iter()
                .find(|model| model["key"] == "qwork-auto")
                .and_then(|model| model.get("is_default")),
            Some(&json!(true))
        );
        assert_eq!(declared_default_key(&models).as_deref(), Some("qwork-auto"));
        assert_eq!(default_key_in(&models).as_deref(), Some(DEFAULT_MODEL));
        assert_eq!(resolve_model_key(&models, "auto"), DEFAULT_MODEL);
        assert_eq!(resolve_model_key(&models, ""), DEFAULT_MODEL);
        // An explicit premium pick still stands.
        assert_eq!(
            resolve_model_key(&models, "qwork-ultimate"),
            "qwork-ultimate"
        );

        // A lineup that does not serve the client's default falls through to the
        // entry the service marks default.
        let without_client_default = vec![
            json!({"key":"qwork-ultimate","enable":true,"is_default":false}),
            json!({"key":"qwork-auto","enable":true,"is_default":true}),
        ];
        assert_eq!(
            declared_default_key(&without_client_default).as_deref(),
            Some("qwork-auto")
        );
        assert_eq!(
            resolve_model_key(&without_client_default, "auto"),
            "qwork-auto"
        );
        assert_eq!(
            default_key_in(&without_client_default).as_deref(),
            Some("qwork-auto"),
            "the service flag is the second rung, not the head"
        );

        // With neither the client default nor a declared default in the lineup,
        // both readers agree on the head.
        let head_only = vec![
            json!({"key":"qwork-ultimate","enable":true}),
            json!({"key":"safety","enable":true}),
        ];
        assert_eq!(declared_default_key(&head_only), None);
        assert_eq!(
            default_key_in(&head_only).as_deref(),
            Some("qwork-ultimate")
        );
        assert_eq!(resolve_model_key(&head_only, "auto"), "qwork-ultimate");
    }

    /// Some gateway builds wrap the scene object in a `data` envelope. The
    /// projection and the default flag must be read from the SAME object: reading
    /// one from the wrapper and the other from the root turns a well-formed answer
    /// into an empty catalog, and the caller silently keeps a stale list.
    #[test]
    fn a_wrapped_catalog_projects_the_same_models_and_the_same_default() {
        let rows = serde_json::json!([
            { "key": "qwork-ultimate", "display_name": "Premium", "enable": true, "is_default": false },
            { "key": "qwork-auto", "display_name": "Standard", "enable": true, "is_default": true }
        ]);
        let top = parse_model_scenes(&serde_json::json!({ "qwork": rows }));
        let wrapped = parse_model_scenes(&serde_json::json!({ "data": { "qwork": rows } }));
        assert_eq!(
            wrapped.len(),
            top.len(),
            "the wrapped answer projects the same rows: {wrapped:?}"
        );
        assert_eq!(wrapped, top, "and the same shape, default flag included");
        assert_eq!(
            wrapped
                .iter()
                .find(|model| model["key"] == "qwork-auto")
                .and_then(|model| model.get("is_default")),
            Some(&json!(true)),
            "the wrapped answer keeps the default flag: {wrapped:?}"
        );
        assert_eq!(resolve_model_key(&wrapped, "auto"), "qwork-auto");

        // A wrapper with no scene inside is an empty catalog, not a crash and not
        // some other scene's models.
        assert!(parse_model_scenes(&serde_json::json!({ "data": {} })).is_empty());
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
            // `QoderWorkError` carries no Debug (it is reported, not formatted), so the
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
    fn speed_capability_is_catalog_gated() {
        let entry = json!({"functionSwitches":[{"key":"highspeed","type":"bool","options":{"true":{},"false":{}}}]});
        assert_eq!(capabilities_for_entry(Some(&entry))["fastMode"], true);
        assert_eq!(capabilities_for_entry(None)["fastMode"], false);
    }
}
