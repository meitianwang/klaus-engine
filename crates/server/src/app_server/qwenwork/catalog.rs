//! QwenWork model catalog discovery, projection, and caching.

use super::super::qoder::sign::{CosyProfile, SigningContext};
use serde_json::{json, Map, Value};
use std::sync::Mutex;

const CHAT_MODES_PATH: &str = "/api/desktop/chat-modes";
const CATALOG_TTL_MS: i64 = 6 * 60 * 60 * 1000;
const FETCH_TIMEOUT_MS: u64 = 10_000;
const WARM_TIMEOUT_MS: u64 = 1500;
const DESKTOP_CLIENT_TYPE: &str = "6";
const PROJECTION_VERSION: u64 = 1;
const CANONICAL_EFFORT_ORDER: &[&str] = &["low", "medium", "high", "xhigh", "max"];

/// The gateway host serving QwenWork account, catalog, and session endpoints.
pub(crate) fn api_base() -> String {
    std::env::var("QWENWORK_API_BASE")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "https://gateway.qwenwork.cn".into())
}

pub struct QwenWorkError {
    pub message: String,
    pub not_found: bool,
    /// The gateway rejected the access token as a dead login (401 or 403 code 105).
    /// The stored session may still be rereadable from a local cache, so the
    /// catalog path tries rereading once before surfacing expiry.
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
    _context: SigningContext,
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

fn with_user_info<T>(
    action: impl FnOnce(&Value) -> Result<T, QwenWorkError>,
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
            _context: context,
            user_info,
        });
    }
    let cached = guard.as_ref().expect("context populated above");
    action(&cached.user_info)
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
    with_user_info(|user_info| {
        let access_token = user_info
            .get("security_oauth_token")
            .and_then(Value::as_str)
            .filter(|token| !token.is_empty())
            .or_else(|| {
                user_info
                    .get("access_token")
                    .and_then(Value::as_str)
                    .filter(|token| !token.is_empty())
            })
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
/// on-disk snapshot.
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

fn first_key_in(models: &[Value]) -> Option<String> {
    models
        .first()
        .and_then(|model| model.get("key"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

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

fn normalize_model_name(name: &str) -> String {
    name.chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(|character| character.to_lowercase())
        .collect()
}

const DEFAULT_MODEL_NAME: &str = "qwen4max";

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

async fn fetch_live_models() -> Result<Vec<Value>, QwenWorkError> {
    let token = access_token()?.access_token;
    if token.chars().any(char::is_control) {
        return Err(QwenWorkError::msg("QwenWork credential token is invalid"));
    }
    let mut auth_header = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
        .map_err(|_| QwenWorkError::msg("QwenWork credential token is invalid"))?;
    auth_header.set_sensitive(true);
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(FETCH_TIMEOUT_MS))
        .build()
        .map_err(|error| QwenWorkError::msg(error.to_string()))?;
    let url = format!("{}{CHAT_MODES_PATH}", api_base().trim_end_matches('/'));
    let response = super::auth::with_client_headers(client.get(&url))
        .header(reqwest::header::AUTHORIZATION, auth_header)
        .send()
        .await
        .map_err(|error| QwenWorkError::msg(error.to_string()))?;
    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    if !status.is_success() {
        let detail: String = text.chars().take(200).collect();
        return Err(QwenWorkError {
            message: format!("chat-modes {}: {detail}", status.as_u16()),
            not_found: false,
            login_expired: super::auth::login_rejected(status.as_u16(), &text)
                || (status.as_u16() == 403 && is_login_expired_body(&text)),
        });
    }
    let decoded = super::super::qoder::sign::decode_server_response(&text).unwrap_or(text);
    let parsed: Value = serde_json::from_str(&decoded)
        .map_err(|error| QwenWorkError::msg(format!("chat-modes body is not JSON: {error}")))?;
    Ok(parse_live_model_response(&parsed))
}

fn is_login_expired_body(body: &str) -> bool {
    let decoded =
        super::super::qoder::sign::decode_server_response(body).unwrap_or(body.to_string());
    let parsed: Value = serde_json::from_str(&decoded).unwrap_or(Value::Null);
    matches!(
        parsed.get("code"),
        Some(Value::String(text)) if text.trim() == "105"
    ) || parsed.get("code") == Some(&json!(105))
}

fn truthy(value: Option<&Value>) -> bool {
    match value {
        Some(Value::Bool(flag)) => *flag,
        Some(Value::Number(number)) => number.as_i64().unwrap_or(0) != 0,
        Some(Value::String(text)) => matches!(
            text.trim().to_ascii_lowercase().as_str(),
            "true" | "1" | "yes"
        ),
        _ => false,
    }
}

fn positive_i64(value: Option<&Value>) -> Option<i64> {
    match value {
        Some(Value::Number(number)) => number.as_i64().filter(|n| *n > 0),
        Some(Value::String(text)) => text.trim().parse::<i64>().ok().filter(|n| *n > 0),
        _ => None,
    }
}

fn localized_text(model: &Value, top_key: &str, i18n_key: &str) -> Option<String> {
    if let Some(direct) = model
        .get(top_key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
    {
        return Some(direct.to_string());
    }
    let group = model.get("i18n").and_then(|i18n| i18n.get(i18n_key))?;
    for locale in ["zh", "zh-CN", "en"] {
        if let Some(value) = group
            .get(locale)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|text| !text.is_empty())
        {
            return Some(value.to_string());
        }
    }
    None
}

fn parse_thinking_policy(model: &Value) -> Option<(Vec<String>, Option<String>, bool, bool)> {
    let policy = model.get("thinking_policy")?.as_object()?;
    let levels = policy.get("levels").and_then(Value::as_object);
    let mut supported: Vec<String> = Vec::new();
    let mut supports_disabled = truthy(policy.get("supports_disabled"));
    if let Some(levels) = levels {
        for (level_key, entry) in levels {
            if matches!(
                level_key.trim().to_ascii_lowercase().as_str(),
                "off" | "none" | "disabled"
            ) {
                supports_disabled = true;
            }
            if let Some(raw_effort) = entry.get("effort").and_then(Value::as_str) {
                if matches!(
                    raw_effort.trim().to_ascii_lowercase().as_str(),
                    "off" | "none" | "disabled"
                ) {
                    supports_disabled = true;
                } else if let Some(normalized) =
                    super::super::qoder::catalog::engine_effort_level(raw_effort)
                {
                    if !supported.iter().any(|existing| existing == normalized) {
                        supported.push(normalized.to_string());
                    }
                }
            }
        }
    }
    supported.sort_by_key(|effort| {
        CANONICAL_EFFORT_ORDER
            .iter()
            .position(|known| known == effort)
            .unwrap_or(usize::MAX)
    });

    let default_effort = policy
        .get("default_level")
        .and_then(Value::as_str)
        .and_then(|default_level| {
            levels
                .and_then(|map| map.get(default_level))
                .and_then(|entry| entry.get("effort"))
                .and_then(Value::as_str)
                .and_then(super::super::qoder::catalog::engine_effort_level)
                .or_else(|| super::super::qoder::catalog::engine_effort_level(default_level))
        })
        .filter(|effort| supported.iter().any(|existing| existing == effort))
        .map(str::to_string)
        .or_else(|| supported.first().cloned());

    let can_enable = !supported.is_empty() || truthy(model.get("is_reasoning"));
    Some((supported, default_effort, supports_disabled, can_enable))
}

fn parse_chat_modes_catalog(parsed: &Value) -> Vec<Value> {
    let Some(items) = parsed.get("catalog").and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut models = Vec::new();
    for model in items {
        let Some(key) = model
            .get("key")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|text| !text.is_empty())
        else {
            continue;
        };

        let mut context_tokens: Vec<i64> = model
            .get("available_context_windows")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|item| positive_i64(Some(item)))
            .collect();
        let mut default_from_context_config: Option<i64> = None;
        if let Some(config) = model.get("context_config").and_then(Value::as_object) {
            for entry in config.values() {
                if let Some(tokens) = positive_i64(entry.get("token_count")) {
                    context_tokens.push(tokens);
                    if truthy(entry.get("is_default")) && default_from_context_config.is_none() {
                        default_from_context_config = Some(tokens);
                    }
                }
            }
        }
        context_tokens.sort_unstable();
        context_tokens.dedup();

        let (efforts, default_effort, can_disable_thinking, can_enable_from_policy) =
            if let Some(parsed_policy) = parse_thinking_policy(model) {
                parsed_policy
            } else {
                let legacy = super::super::qoder::catalog::parse_model_scene_names(
                    &json!({ "qwork": [model] }),
                    &["qwork"],
                )
                .into_iter()
                .next();
                let efforts = legacy
                    .as_ref()
                    .and_then(|row| row.get("efforts"))
                    .and_then(Value::as_array)
                    .map(|arr| {
                        arr.iter()
                            .filter_map(Value::as_str)
                            .map(str::to_string)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                let default_effort = legacy
                    .as_ref()
                    .and_then(|row| row.get("defaultEffort"))
                    .and_then(Value::as_str)
                    .map(str::to_string);
                let can_disable = legacy
                    .as_ref()
                    .and_then(|row| row.get("canDisableThinking"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let can_enable = legacy
                    .as_ref()
                    .and_then(|row| row.get("canEnableThinking"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                (efforts, default_effort, can_disable, can_enable)
            };

        let model_params_enables_thinking = model
            .get("model_params")
            .and_then(|params| params.get("thinking"))
            .and_then(|thinking| thinking.get("type"))
            .and_then(Value::as_str)
            == Some("enabled");
        let is_reasoning = truthy(model.get("is_reasoning"));
        let can_enable_thinking =
            can_enable_from_policy || is_reasoning || model_params_enables_thinking;

        let mut out = Map::new();
        out.insert("key".into(), json!(key));
        out.insert(
            "display_name".into(),
            localized_text(model, "display_name", "display_name")
                .map(Value::String)
                .unwrap_or(Value::Null),
        );
        if let Some(description) = localized_text(model, "description", "description") {
            out.insert("description".into(), json!(description));
        }
        if let Some(i18n) = model.get("i18n") {
            out.insert("i18n".into(), i18n.clone());
        }
        out.insert("is_reasoning".into(), json!(is_reasoning));
        out.insert("is_vl".into(), json!(truthy(model.get("is_vl"))));
        if let Some(tokens) = positive_i64(model.get("max_input_tokens")) {
            out.insert("max_input_tokens".into(), json!(tokens));
        }
        if let Some(tokens) = positive_i64(model.get("max_output_tokens")) {
            out.insert("max_output_tokens".into(), json!(tokens));
        }
        if let Some(tokens) = positive_i64(
            model
                .get("default_context_window")
                .or_else(|| model.get("defaultContextWindow")),
        )
        .or(default_from_context_config)
        {
            out.insert("default_context_window".into(), json!(tokens));
        }
        if !context_tokens.is_empty() {
            out.insert("contextTokens".into(), json!(context_tokens));
        }
        if !efforts.is_empty() {
            out.insert("efforts".into(), json!(efforts));
        }
        if let Some(default_effort) = default_effort {
            out.insert("defaultEffort".into(), json!(default_effort));
        }
        if can_disable_thinking {
            out.insert("canDisableThinking".into(), json!(true));
        }
        if can_enable_thinking {
            out.insert("canEnableThinking".into(), json!(true));
        }
        if let Some(factor) = model
            .get("priceFactor")
            .or_else(|| model.get("price_factor"))
            .and_then(Value::as_f64)
            .filter(|value| value.is_finite())
        {
            out.insert("priceFactor".into(), json!(factor));
        }
        for passthrough in [
            "format",
            "source",
            "enable",
            "disabled",
            "status",
            "tags",
            "badges",
            "promotion",
            "frontier",
            "show_zero_price_factor",
            "feature_switches",
            "thinking_policy",
            "thinking_config",
            "model_params",
            "context_config",
            "is_default",
            "is_new",
            "is_free",
            "icon",
        ] {
            if let Some(value) = model.get(passthrough).filter(|value| !value.is_null()) {
                out.insert(passthrough.into(), value.clone());
            }
        }
        super::super::conversation_catalog::mark_purpose("qwenwork", model, &mut out);
        models.push(Value::Object(out));
    }
    models
}

fn parse_live_model_response(parsed: &Value) -> Vec<Value> {
    let direct_catalog = parse_chat_modes_catalog(parsed);
    if !direct_catalog.is_empty() {
        return direct_catalog;
    }
    if let Some(wrapped) = parsed.get("data") {
        let wrapped_catalog = parse_chat_modes_catalog(wrapped);
        if !wrapped_catalog.is_empty() {
            return wrapped_catalog;
        }
    }
    let legacy_direct = super::super::qoder::catalog::parse_model_scene_names(parsed, &["qwork"]);
    if !legacy_direct.is_empty() {
        return legacy_direct;
    }
    parsed
        .get("data")
        .map(|data| super::super::qoder::catalog::parse_model_scene_names(data, &["qwork"]))
        .unwrap_or_default()
}

/// Read Klaus's own non-secret catalog snapshot first, then the official CLI's
/// plain-JSON cache when present.
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
                .map(|parsed| parse_live_model_response(&parsed))
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
    if value.get("projectionVersion").and_then(Value::as_u64) != Some(PROJECTION_VERSION)
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
        "projectionVersion": PROJECTION_VERSION,
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

/// Resolve QwenWork's legacy `auto` aliases against the actual catalog while
/// keeping explicit auxiliary keys such as `lite` intact.
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
    if has(requested) || requested == "lite" {
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
    fn live_catalog_prefers_chat_modes_and_falls_back_to_legacy_scene_or_data_wrapper() {
        let chat_modes = json!({
            "schema_version": 2,
            "catalog": [{
                "key": "ling-3.1-flash",
                "i18n": {
                    "display_name": { "zh": "Ling-3.1-Flash", "en": "Ling-3.1-Flash" },
                    "description": { "zh": "高效编程模型", "en": "Fast coding model" }
                },
                "is_reasoning": true,
                "is_vl": false,
                "max_input_tokens": 200000,
                "max_output_tokens": 32000,
                "context_config": {
                    "standard": { "token_count": 200000, "is_default": true }
                },
                "model_params": {
                    "thinking": { "type": "enabled" }
                },
                "price_factor": 0.25,
                "enable": true,
                "format": "openai/chat-completions",
                "source": "system"
            }, {
                "key": "qwen4-max",
                "i18n": {
                    "display_name": { "zh": "Qwen4-Max", "en": "Qwen4-Max" }
                },
                "is_reasoning": true,
                "is_vl": true,
                "max_input_tokens": 900000,
                "max_output_tokens": 64000,
                "context_config": {
                    "standard": { "token_count": 200000, "is_default": true },
                    "medium": { "token_count": 400000 },
                    "large": { "token_count": 1000000 }
                },
                "thinking_policy": {
                    "default_level": "auto",
                    "levels": {
                        "auto": { "effort": "high" },
                        "high": { "effort": "max" },
                        "low": { "effort": "low" }
                    }
                },
                "price_factor": 2.0,
                "enable": true,
                "format": "anthropic/messages"
            }]
        });
        let parsed = parse_live_model_response(&chat_modes);
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0]["key"], "ling-3.1-flash");
        assert_eq!(parsed[0]["display_name"], "Ling-3.1-Flash");
        assert_eq!(parsed[0]["description"], "高效编程模型");
        assert_eq!(parsed[0]["priceFactor"], json!(0.25));
        assert_eq!(parsed[0]["model_params"]["thinking"]["type"], "enabled");
        assert!(parsed[0].get("efforts").is_none());

        assert_eq!(parsed[1]["key"], "qwen4-max");
        assert_eq!(parsed[1]["efforts"], json!(["low", "high", "max"]));
        assert_eq!(parsed[1]["defaultEffort"], "high");
        assert_eq!(parsed[1]["default_context_window"], 200000);
        assert_eq!(parsed[1]["contextTokens"], json!([200000, 400000, 1000000]));
        assert_eq!(default_key_in(&parsed).as_deref(), Some("qwen4-max"));
        assert_eq!(resolve_model_key(&parsed, "lite"), "lite");

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
