//! QwenWork COSY signing and model catalog.

use super::super::qoder::sign::{CosyProfile, SignedRequest, SigningContext};
use serde_json::{json, Value};
use std::sync::Mutex;

const MODEL_LIST_PATH: &str = "/algo/api/v2/model/list?Encode=1";
const CATALOG_TTL_MS: i64 = 6 * 60 * 60 * 1000;
const FETCH_TIMEOUT_MS: u64 = 10_000;
const WARM_TIMEOUT_MS: u64 = 1500;
const DESKTOP_CLIENT_TYPE: &str = "6";

/// The gateway host the signed routes use; the plain OpenAPI media routes live
/// on the same host (the official client resolves one OpenAPI domain).
pub(crate) fn api_base() -> String {
    std::env::var("QWENWORK_API_BASE")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        // QwenWork serves its own gateway: the official client's logs show
        // every signed call — model list and inference alike — on
        // gateway.qwenwork.cn. Qoder's gateway answers the same paths but
        // rejects QwenWork credentials with 403 code 105.
        .unwrap_or_else(|| "https://gateway.qwenwork.cn".into())
}

pub struct QwenWorkError {
    pub message: String,
    pub not_found: bool,
    /// The gateway rejected the access token as a dead login (403 code 105).
    /// The stored session may still be refreshable, so the catalog path trades
    /// the refresh token in and retries once before giving up.
    pub login_expired: bool,
}

impl QwenWorkError {
    pub(crate) fn msg(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            not_found: false,
            login_expired: false,
        }
    }
}

struct CachedContext {
    fingerprint: String,
    context: SigningContext,
    user_info: Value,
}

fn context_slot() -> &'static Mutex<Option<CachedContext>> {
    static SLOT: std::sync::OnceLock<Mutex<Option<CachedContext>>> = std::sync::OnceLock::new();
    SLOT.get_or_init(|| Mutex::new(None))
}

pub(crate) fn forget_context() {
    *context_slot()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    forget_cache();
}

fn with_context<T>(
    action: impl FnOnce(&SigningContext, &Value) -> Result<T, QwenWorkError>,
) -> Result<T, QwenWorkError> {
    let Some((machine_id, user_info)) = super::auth::credential() else {
        return Err(QwenWorkError {
            message: "QwenWork subscription is not signed in".into(),
            not_found: true,
            login_expired: false,
        });
    };
    let token = user_info
        .get("security_oauth_token")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let expire_time = user_info
        .get("expire_time")
        .and_then(Value::as_i64)
        .unwrap_or_default();
    let fingerprint = format!("{machine_id}:{}:{expire_time}", token.len());
    let mut guard = context_slot()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if guard
        .as_ref()
        .is_none_or(|cached| cached.fingerprint != fingerprint)
    {
        let context = SigningContext::new_with_profile(
            &machine_id,
            DESKTOP_CLIENT_TYPE,
            &user_info,
            CosyProfile::QWENWORK,
        )
        .map_err(QwenWorkError::msg)?;
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
    for key in ["name", "display_name", "email", "uid"] {
        match user_info.get(key) {
            Some(Value::String(value)) if !value.is_empty() => return value.clone(),
            Some(Value::Number(value)) => return value.to_string(),
            _ => {}
        }
    }
    "QwenWork".into()
}

pub struct QwenWorkToken {
    pub access_token: String,
    pub account: String,
}

pub fn access_token() -> Result<QwenWorkToken, QwenWorkError> {
    with_context(|_, user_info| {
        let access_token = user_info
            .get("access_token")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if access_token.is_empty() {
            return Err(QwenWorkError::msg("QwenWork subscription is not signed in"));
        }
        Ok(QwenWorkToken {
            access_token,
            account: account_label(user_info),
        })
    })
}

pub fn auth_status() -> Value {
    super::auth::status()
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

/// The catalog head's key from the fresh in-process cache, then Klaus's own
/// on-disk snapshot. The picker offers no synthetic automatic route, so an
/// unmapped tier or a bare default names this entry. Both readers follow the
/// config root and neither fetches: the official CLI cache `read_local_models`
/// also mirrors is a third-party directory a selection must not depend on.
/// `None` leaves the caller on the legacy wire alias.
pub(crate) fn first_model_key() -> Option<String> {
    {
        let guard = catalog_cache()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(cache) = guard
            .as_ref()
            .filter(|cache| cache.fingerprint == super::auth::catalog_fingerprint())
        {
            if now_ms() < cache.expires_at {
                return default_key_in(&cache.models);
            }
        }
    }
    read_owned_catalog().as_deref().and_then(default_key_in)
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

/// The model a bare default names.
///
/// Prefers the entry whose display name is the plan's flagship chat model, so the
/// default is a product decision rather than an accident of the order the service
/// happens to list. Matching is on the name and not the key because this catalog
/// keys its entries with opaque `mode-<uuid>` identifiers that carry no model
/// identity and are not stable across catalog generations — a hardcoded key would
/// silently stop matching and fall through to the head.
///
/// The name comparison is normalized (case, separators and internal punctuation
/// folded away) but exact, because the lineup also carries a longer-named variant
/// of the same model that must not be picked up by a prefix match. Falls back to
/// the list head when the catalog serves no entry with that name.
fn default_key_in(models: &[Value]) -> Option<String> {
    models
        .iter()
        .find(|model| {
            model
                .get("display_name")
                .and_then(Value::as_str)
                .is_some_and(|name| normalize_model_name(name) == DEFAULT_MODEL_NAME)
        })
        .and_then(|model| model.get("key"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| first_key_in(models))
}

/// A display name with case, whitespace and separators folded away, so one
/// spelling of a model name matches however the service punctuates it.
fn normalize_model_name(name: &str) -> String {
    name.chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(|character| character.to_lowercase())
        .collect()
}

/// The normalized display name of the model a bare default lands on. Produced by
/// [`normalize_model_name`] from the name as the picker paints it.
const DEFAULT_MODEL_NAME: &str = "qwen38max";

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

async fn fetch_live_models() -> Result<Vec<Value>, QwenWorkError> {
    let signed = with_context(|context, _| Ok(context.sign_get(&api_base(), MODEL_LIST_PATH)))?;
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(FETCH_TIMEOUT_MS))
        .build()
        .map_err(|error| QwenWorkError::msg(error.to_string()))?;
    let mut request = client.get(&signed.url);
    for (name, value) in &signed.headers {
        request = request.header(name, value);
    }
    let response = request
        .send()
        .await
        .map_err(|error| QwenWorkError::msg(error.to_string()))?;
    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    if !status.is_success() {
        let detail: String = text.chars().take(200).collect();
        return Err(QwenWorkError {
            message: format!("model/list {}: {detail}", status.as_u16()),
            not_found: false,
            login_expired: status.as_u16() == 403 && is_login_expired_body(&text),
        });
    }
    // QwenWork's catalog route uses `Encode=1` for the response as well. The
    // official wrapper falls back to the raw body when decoding throws, which
    // also keeps test gateways and gradual server rollouts that return plain
    // JSON compatible.
    let decoded = super::super::qoder::sign::decode_server_response(&text).unwrap_or(text);
    let parsed: Value = serde_json::from_str(&decoded)
        .map_err(|error| QwenWorkError::msg(format!("model/list body is not JSON: {error}")))?;
    // The official client reads the scene directly from the decrypted object.
    // Some gateways wrap that same object in `data`, so accept the wrapper only
    // as a fallback. Preferring `data` unconditionally loses a valid top-level
    // catalog when the response also carries unrelated response metadata there.
    Ok(parse_live_model_response(&parsed))
}

/// The gateway reports a dead login as HTTP 403 with `{"code":"105"}`; the body
/// may arrive through the same `Encode=1` envelope as a success, so decode
/// before reading the code.
fn is_login_expired_body(body: &str) -> bool {
    let decoded =
        super::super::qoder::sign::decode_server_response(body).unwrap_or(body.to_string());
    let parsed: Value = serde_json::from_str(&decoded).unwrap_or(Value::Null);
    matches!(
        parsed.get("code"),
        Some(Value::String(text)) if text.trim() == "105"
    ) || parsed.get("code") == Some(&json!(105))
}

fn parse_live_model_response(parsed: &Value) -> Vec<Value> {
    let direct = super::super::qoder::catalog::parse_model_scene_names(parsed, &["qwork"]);
    if !direct.is_empty() {
        return direct;
    }
    parsed
        .get("data")
        .map(|data| super::super::qoder::catalog::parse_model_scene_names(data, &["qwork"]))
        .unwrap_or_default()
}

/// Read Klaus's own non-secret catalog snapshot first, then the official CLI's
/// plain-JSON cache. QwenWork desktop encrypts its private cache, so Klaus never
/// reaches into that format; a signed live refresh remains the authority.
fn read_local_models() -> Vec<Value> {
    if let Some(models) = read_owned_catalog() {
        return models;
    }
    super::auth::local_catalog_dirs()
        .into_iter()
        .find_map(|dir| {
            std::fs::read_to_string(dir.join("models"))
                .ok()
                .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
                .map(|parsed| {
                    super::super::qoder::catalog::parse_model_scene_names(&parsed, &["qwork"])
                })
                .filter(|models| !models.is_empty())
        })
        .unwrap_or_default()
}

fn owned_catalog_path() -> std::path::PathBuf {
    super::config_root().join("qwenwork-model-catalog.json")
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

async fn refresh_live_catalog() -> Result<Vec<Value>, QwenWorkError> {
    let fingerprint = super::auth::catalog_fingerprint();
    let models = match fetch_live_models().await {
        Ok(models) => models,
        // A 403/105 means the copied access token died: the official client
        // refreshes are one-time, so whoever refreshed last holds the only
        // live token. Reread before spending anything — re-reading the
        // official CLI store costs no refresh token, while refreshing here
        // would invalidate the official client's copy in turn. Only when no
        // store can be reread does the error travel up to the host, which
        // owns the desktop store's decryption and the last-resort refresh.
        Err(error) if error.login_expired => {
            if fingerprint != super::auth::catalog_fingerprint() {
                return Err(error);
            }
            let reread = super::auth::import_local()
                .ok()
                .is_some_and(|result| result.get("ok") == Some(&json!(true)));
            if !reread {
                return Err(error);
            }
            fetch_live_models().await?
        }
        Err(error) => return Err(error),
    };
    if fingerprint != super::auth::catalog_fingerprint() {
        return Ok(models);
    }
    if !models.is_empty() {
        if let Some(fingerprint) = fingerprint.as_deref() {
            write_owned_catalog(&models, fingerprint);
        }
        *catalog_cache()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(CatalogCache {
            fingerprint: fingerprint.clone(),
            models: models.clone(),
            expires_at: now_ms() + CATALOG_TTL_MS,
        });
    }
    Ok(models)
}

pub async fn fetch_models() -> Result<Vec<Value>, QwenWorkError> {
    {
        let guard = catalog_cache()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
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
        let guard = catalog_cache()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
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
        tokio::spawn(async {
            if let Err(error) = refresh_live_catalog().await {
                eprintln!("[QwenWorkBridge] catalog refresh failed: {}", error.message);
            }
        });
        return Ok(stale);
    }
    match refresh_live_catalog().await {
        Ok(models) if !models.is_empty() => Ok(models),
        Ok(_) => Ok(read_local_models()),
        Err(error) => {
            let local = read_local_models();
            if local.is_empty() {
                Err(error)
            } else {
                Ok(local)
            }
        }
    }
}

pub async fn fetch_models_warm() -> Result<Vec<Value>, QwenWorkError> {
    if super::auth::credential().is_none() {
        return Err(QwenWorkError {
            message: "QwenWork subscription is not signed in".into(),
            not_found: true,
            login_expired: false,
        });
    }
    {
        let guard = catalog_cache()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
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
                eprintln!("[QwenWorkBridge] catalog refresh failed: {}", error.message);
            }
        });
        return Ok(local);
    }
    match tokio::time::timeout(
        std::time::Duration::from_millis(WARM_TIMEOUT_MS),
        refresh_live_catalog(),
    )
    .await
    {
        Ok(Ok(models)) if !models.is_empty() => Ok(models),
        Ok(Ok(_)) => Ok(read_local_models()),
        Ok(Err(error)) => {
            let local = read_local_models();
            if local.is_empty() {
                Err(error)
            } else {
                Ok(local)
            }
        }
        Err(_) => {
            let stale = catalog_cache()
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_ref()
                .filter(|cache| cache.fingerprint == super::auth::catalog_fingerprint())
                .map(|cache| cache.models.clone())
                .filter(|models| !models.is_empty())
                .unwrap_or_else(read_local_models);
            tokio::spawn(async {
                if let Err(error) = refresh_live_catalog().await {
                    eprintln!("[QwenWorkBridge] catalog refresh failed: {}", error.message);
                }
            });
            Ok(stale)
        }
    }
}

/// Resolve QwenWork's legacy `auto` aliases against the actual qwork catalog.
/// Exact keys win. Then the official compatibility order is tried, followed by
/// the service default and finally the first enabled entry.
pub(crate) fn resolve_model_key(models: &[Value], requested: &str) -> String {
    let requested = if requested.is_empty() {
        "auto"
    } else {
        requested
    };
    let has = |key: &str| {
        models
            .iter()
            .any(|model| model.get("key").and_then(Value::as_str) == Some(key))
    };
    if has(requested) {
        return requested.to_string();
    }
    if matches!(requested, "auto" | "qwork-auto") {
        for candidate in [requested, "qwork-auto", "auto", "flash"] {
            if has(candidate) {
                return candidate.to_string();
            }
        }
    }
    if let Some(default) = default_key_in(models) {
        return default;
    }
    requested.to_string()
}

pub(crate) fn sign_infer_request(
    body_json: &str,
    model_key: &str,
) -> Result<SignedRequest, QwenWorkError> {
    with_context(|context, _| Ok(context.sign_infer(&api_base(), body_json, model_key, "system")))
}

pub(crate) async fn lookup_model(key: &str) -> Option<Value> {
    let models = fetch_models().await.ok()?;
    let wire = resolve_model_key(&models, &super::wire_model(key));
    models
        .into_iter()
        .find(|model| model.get("key").and_then(Value::as_str) == Some(wire.as_str()))
}

pub(crate) async fn model_context_window(key: &str) -> Option<i64> {
    Some(super::super::qoder::bridge::resolve_qoder_context_length(
        lookup_model(key).await.as_ref(),
    ))
}

pub async fn model_capabilities(key: &str) -> Value {
    let entry = lookup_model(key).await;
    let mut capabilities = json!({
        "vision": entry
            .as_ref()
            .and_then(|model| model.get("is_vl"))
            .and_then(Value::as_bool)
            .unwrap_or(false),
        "contextWindow": super::super::qoder::bridge::resolve_qoder_context_length(entry.as_ref()),
        "reasoningDialect": "qoder",
    });
    if let Some(output) = entry
        .as_ref()
        .and_then(|model| model.get("max_output_tokens"))
        .and_then(Value::as_i64)
        .filter(|tokens| *tokens > 0)
    {
        capabilities["maxOutputTokens"] = json!(output);
    }
    if let Some(effort) = entry
        .as_ref()
        .and_then(super::super::qoder::catalog::native_effort_capability)
    {
        capabilities["effort"] = effort;
    }
    capabilities
}

#[cfg(test)]
mod tests {
    use super::super::super::qoder::catalog::engine_effort_level;
    use super::*;
    crate::app_server::catalog_test_support::warm_catalog_tests!("qwenwork", || {
        read_owned_catalog().unwrap_or_default()
    });

    #[test]
    fn live_catalog_prefers_the_official_top_level_scene_then_accepts_a_data_wrapper() {
        let direct = json!({
            "qwork": [{ "key": "qwork-auto", "display_name": "Standard" }],
            "data": { "trace": "not-a-catalog" }
        });
        assert_eq!(parse_live_model_response(&direct)[0]["key"], "qwork-auto");

        let wrapped = json!({
            "data": {
                "qwork": [{ "key": "qwork-ultimate", "display_name": "Premium" }]
            }
        });
        assert_eq!(
            parse_live_model_response(&wrapped)[0]["key"],
            "qwork-ultimate"
        );
    }

    #[test]
    fn engine_effort_aliases_are_normalized_and_unknown_values_are_rejected() {
        assert_eq!(engine_effort_level("minimal"), Some("low"));
        assert_eq!(engine_effort_level("ULTRA"), None);
        assert_eq!(engine_effort_level("xhigh"), Some("xhigh"));
        assert_eq!(engine_effort_level("none"), None);
        assert_eq!(engine_effort_level("vendor-special"), None);
    }

    #[test]
    fn qwork_scene_keeps_per_model_thinking_context_and_capability_metadata() {
        let parsed = json!({
            "qwork": [{
                "key": "qwork-advanced",
                "display_name": "Advanced",
                "enable": true,
                "is_vl": true,
                "is_reasoning": true,
                "max_input_tokens": 180000,
                "max_output_tokens": "32768",
                "format": "openai",
                "source": "system",
                "thinking_config": {
                    "disabled": {},
                    "enabled": { "efforts": {
                        "low": {},
                        "high": { "is_default": true }
                    }}
                },
                "context_config": {
                    "standard": { "token_count": 64000 },
                    "large": { "token_count": "180000" }
                },
                "description": "【高可信模型】软件工程与深度推理，可处理L1-L4数据",
                "priceFactor": 0.5,
                "icon": "https://img.alicdn.com/imgextra/qwork-advanced.svg"
            }, {
                "key": "qwork-lite",
                "display_name": "Economy",
                // The server layer spells the multiplier snake_case.
                "price_factor": 0.1
            }]
        });
        let models =
            super::super::super::qoder::catalog::parse_model_scene_names(&parsed, &["qwork"]);
        assert_eq!(models[0]["key"], "qwork-advanced");
        assert_eq!(models[0]["is_vl"], true);
        assert_eq!(models[0]["is_reasoning"], true);
        assert_eq!(models[0]["efforts"], json!(["low", "high"]));
        assert_eq!(models[0]["defaultEffort"], "high");
        assert_eq!(models[0]["canDisableThinking"], true);
        assert_eq!(models[0]["contextTokens"], json!([64000, 180000]));
        assert_eq!(models[0]["max_output_tokens"], 32768);
        assert_eq!(models[0]["format"], "openai");
        assert_eq!(models[0]["source"], "system");
        // The official selector shows the gateway's credit multiplier and brand
        // glyph beside each name; dropping them here would starve the client.
        assert_eq!(
            models[0]["description"],
            json!("【高可信模型】软件工程与深度推理，可处理L1-L4数据")
        );
        assert_eq!(models[0]["priceFactor"], json!(0.5));
        assert_eq!(
            models[0]["icon"],
            json!("https://img.alicdn.com/imgextra/qwork-advanced.svg")
        );
        assert_eq!(models[1]["priceFactor"], json!(0.1));
    }

    #[test]
    fn normalized_sdk_metadata_and_official_auto_fallback_order_are_supported() {
        let parsed = json!({
            "qwork": [{
                "key": "qwork-advanced",
                "display_name": "Advanced",
                "efforts": ["medium", "high"],
                "supports_disabled": true,
                "default_effort": "medium",
                "available_context_windows": [32000, "128000"],
                "default_context_window": 128000,
                "max_output_tokens": 16384
            }, {
                "key": "first",
                "display_name": "First"
            }]
        });
        let models =
            super::super::super::qoder::catalog::parse_model_scene_names(&parsed, &["qwork"]);
        assert_eq!(models[0]["efforts"], json!(["medium", "high"]));
        assert_eq!(models[0]["defaultEffort"], "medium");
        assert_eq!(models[0]["canDisableThinking"], true);
        assert_eq!(models[0]["contextTokens"], json!([32000, 128000]));
        assert_eq!(models[0]["default_context_window"], 128000);
        assert_eq!(resolve_model_key(&models, "auto"), "qwork-advanced");
        assert_eq!(resolve_model_key(&models, "missing"), "qwork-advanced");

        let aliases = vec![
            json!({"key":"flash"}),
            json!({"key":"auto"}),
            json!({"key":"qwork-auto"}),
        ];
        assert_eq!(resolve_model_key(&aliases, "auto"), "auto");
        assert_eq!(resolve_model_key(&aliases, "qwork-auto"), "qwork-auto");
        assert_eq!(resolve_model_key(&aliases, "missing"), "flash");
    }
}
