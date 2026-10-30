//! WorkBuddy model catalog.
//!
//! The catalog is the cloud product config, exactly the document the official
//! client fetches and caches: nothing about the lineup is baked in here, so a
//! model the service adds or retires shows up without a Klaus release.
//!
//! Unlike Qoder and QwenWork this gateway signs nothing — a plain bearer token
//! is the whole credential — so the catalog is an ordinary authenticated GET and
//! inference goes straight to the gateway instead of through a local bridge.

use serde_json::{json, Map, Value};
use std::sync::Mutex;

const CONFIG_PATH: &str = "/v3/config";
const CATALOG_TTL_MS: i64 = 6 * 60 * 60 * 1000;
const FETCH_TIMEOUT_MS: u64 = 10_000;
const WARM_TIMEOUT_MS: u64 = 1500;

/// The product identifier required by authenticated service routes.
/// The catalog additionally declares its CLI profile when fetching definitions.
const CLIENT_VERSION: &str = "5.6.2";

/// The entry the service marks as the default. A selection that names no model
/// resolves here rather than to a hard-coded id, so a service-side change of
/// default is followed instead of fought.
const DEFAULT_MODEL: &str = "default-model";

/// Auto routes can be exposed as three explicit service tiers when enabled.
const AUTO_TIERS: [&str; 3] = ["fast-model", "balanced-model", "deep-model"];

/// Fallback context window when a catalog entry declares none.
const DEFAULT_CONTEXT_TOKENS: i64 = 180_000;

pub struct WorkBuddyError {
    pub message: String,
    pub not_found: bool,
    /// The gateway rejected the access token as a dead login (401/403). The
    /// stored session may still be refreshable, so the catalog path trades the
    /// refresh token in and retries once before giving up.
    pub login_expired: bool,
}

impl WorkBuddyError {
    fn msg(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            not_found: false,
            login_expired: false,
        }
    }
}

struct CatalogCache {
    fingerprint: Option<String>,
    models: Vec<Value>,
    model_metadata: Vec<Value>,
    expires_at: i64,
}

fn catalog_cache() -> &'static Mutex<Option<CatalogCache>> {
    static CACHE: std::sync::OnceLock<Mutex<Option<CatalogCache>>> = std::sync::OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(None))
}

pub(crate) fn forget_cache() {
    *catalog_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn client_version() -> String {
    std::env::var("WORKBUDDY_CLIENT_VERSION")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| CLIENT_VERSION.to_string())
}

pub(crate) fn user_agent() -> String {
    format!("WorkBuddy/{}", client_version())
}

fn http_client() -> Result<reqwest::Client, WorkBuddyError> {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(FETCH_TIMEOUT_MS))
        .build()
        .map_err(|error| WorkBuddyError::msg(error.to_string()))
}

async fn fetch_live_models() -> Result<(Vec<Value>, Vec<Value>), WorkBuddyError> {
    let token = super::auth::access_token()?;
    let url = format!("{}{CONFIG_PATH}", super::auth::api_base());
    let response = http_client()?
        .get(&url)
        .bearer_auth(&token.access_token)
        // The CLI token selects the interactive account directory. Without it,
        // the service returns legacy definitions for internal agents as well.
        .header(
            "User-Agent",
            format!("{} CLI/{}", user_agent(), env!("CARGO_PKG_VERSION")),
        )
        .send()
        .await
        .map_err(|error| WorkBuddyError::msg(error.to_string()))?;
    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    if !status.is_success() {
        let detail: String = text.chars().take(200).collect();
        return Err(WorkBuddyError {
            message: format!("config {}: {detail}", status.as_u16()),
            not_found: false,
            login_expired: matches!(status.as_u16(), 401 | 403),
        });
    }
    let parsed: Value = serde_json::from_str(&text)
        .map_err(|error| WorkBuddyError::msg(format!("config body is not JSON: {error}")))?;
    let config = super::metadata::effective(&parsed);
    let models = parse_config_models(&config);
    if models.is_empty() && !super::metadata::unresolved(&config).is_empty() {
        return Err(WorkBuddyError {
            message: "workbuddy_model_metadata_unavailable".into(),
            not_found: false,
            login_expired: false,
        });
    }
    // Keep declared capabilities for explicit selections from saved sessions,
    // even when the current agent no longer offers them in its picker.
    let mut definitions = config;
    if let Some(fields) = definitions.as_object_mut() {
        fields.remove("agents");
    }
    Ok((models, parse_config_models(&definitions)))
}

/// Project the product config's `models[]` into the catalog entry shape the
/// composer, the settings pane and session resolution all read.
///
/// Account flags and agent allowlists do not narrow the model directory.
/// Explicit media generators remain in their own catalogs.
pub(crate) fn parse_config_models(parsed: &Value) -> Vec<Value> {
    let data = parsed.get("data").unwrap_or(parsed);
    let Some(models) = data.get("models").and_then(Value::as_array) else {
        return Vec::new();
    };
    let expand_auto = data["productFeatures"]["EnableAutoModelTiers"] == true
        && models.iter().any(|model| model["id"] == "auto");
    let mut entries: Vec<_> = models
        .iter()
        .filter(|model| !expand_auto || model["id"] != "auto")
        .collect();
    if expand_auto {
        entries.sort_by_key(|model| {
            AUTO_TIERS
                .iter()
                .position(|id| model["id"] == *id)
                .unwrap_or(AUTO_TIERS.len())
        });
    }
    entries
        .into_iter()
        .filter(|model| is_chat_model(model))
        .filter_map(catalog_entry)
        .collect()
}

fn is_chat_model(model: &Value) -> bool {
    let legacy_media = [
        "gemini-3.0-pro-image",
        "gemini-3.1-flash-image",
        "gemini-2.5-flash-image",
        "hunyuan-image-v3.0",
        "hunyuan-image-v2.0-general-edit",
        "hunyuan-video-art",
    ];
    let id = model.get("id").and_then(Value::as_str).unwrap_or_default();
    let media_tags = [
        "text-to-image",
        "image-to-image",
        "text-to-video",
        "image-to-video",
    ];
    let is_generator = model.get("videoGeneration").is_some_and(Value::is_object)
        || model
            .get("tags")
            .and_then(Value::as_array)
            .is_some_and(|tags| {
                tags.iter()
                    .filter_map(Value::as_str)
                    .any(|tag| media_tags.contains(&tag))
            });
    !id.is_empty() && !is_generator && !legacy_media.contains(&id)
}

fn catalog_entry(model: &Value) -> Option<Value> {
    let key = model.get("id").and_then(Value::as_str)?.to_string();
    let mut out = Map::new();
    out.insert("key".into(), json!(key));
    out.insert(
        "display_name".into(),
        json!(model
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or(key.as_str())),
    );
    out.insert(
        "is_reasoning".into(),
        json!(truthy(model.get("supportsReasoning"))),
    );
    out.insert("is_vl".into(), json!(truthy(model.get("supportsImages"))));

    let context = number_of(model.get("contextWindow"))
        .or_else(|| number_of(model.get("maxInputTokens")))
        .filter(|tokens| *tokens > 0);
    if let Some(tokens) = context {
        out.insert("max_input_tokens".into(), json!(tokens));
        out.insert("contextTokens".into(), json!([tokens]));
    }
    if let Some(tokens) = number_of(model.get("maxOutputTokens")).filter(|n| *n > 0) {
        out.insert("max_output_tokens".into(), json!(tokens));
    }

    // Two catalog spellings of "this model reasons": a fixed effort the service
    // picked, or the set the user may choose from. Both become an engine effort
    // ladder so the composer's thinking control has something to offer.
    let reasoning = model.get("reasoning");
    let mut levels: Vec<&str> = reasoning
        .and_then(|value| value.get("supportedEfforts"))
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .filter_map(engine_effort_level)
                .fold(Vec::new(), |mut acc, level| {
                    if !acc.contains(&level) {
                        acc.push(level);
                    }
                    acc
                })
        })
        .unwrap_or_default();
    if levels.is_empty() {
        if let Some(level) = reasoning
            .and_then(|value| value.get("effort"))
            .and_then(Value::as_str)
            .and_then(engine_effort_level)
        {
            levels.push(level);
        }
    }
    if !levels.is_empty() {
        out.insert("efforts".into(), json!(levels));
        let default = reasoning
            .and_then(|value| value.get("defaultEffort"))
            .or_else(|| reasoning.and_then(|value| value.get("effort")))
            .and_then(Value::as_str)
            .and_then(|level| declared_default_effort(level, &levels));
        if let Some(default) = default {
            out.insert("defaultEffort".into(), json!(default));
        }
    }
    // `canDisableThinking` is deliberately NOT carried over. The catalog declares
    // it, but the gateway answers `reasoning_effort: "none"` with 200 and then
    // reasons anyway — measured on kimi-k2.8-preview, where `none` produced more
    // reasoning output than `low`. Klaus's only lever is `reasoning_effort`, so
    // offering an "off" rung here would be a control that does nothing.

    if let Some(factor) = credit_factor(model.get("credits")) {
        out.insert("priceFactor".into(), json!(factor));
    }
    for (source, target) in [
        ("descriptionZh", "descriptionZh"),
        ("descriptionEn", "descriptionEn"),
        ("iconUrl", "icon"),
    ] {
        if let Some(value) = model.get(source) {
            out.insert(target.into(), value.clone());
        }
    }
    // One `description` for consumers that do not localize; the host picks
    // between the two spellings above when it knows the UI language.
    let description = model
        .get("descriptionZh")
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
        .or_else(|| {
            model
                .get("descriptionEn")
                .and_then(Value::as_str)
                .filter(|text| !text.is_empty())
        });
    if let Some(description) = description {
        out.insert("description".into(), json!(description));
    }
    if truthy(model.get("isDefault")) {
        out.insert("is_default".into(), json!(true));
    }
    super::super::conversation_catalog::mark_purpose("workbuddy", model, &mut out);
    Some(Value::Object(out))
}

/// The credit multiplier out of the catalog's own copy, `"x0.34 credits"` — the
/// same number the official picker shows beside the model name.
fn credit_factor(credits: Option<&Value>) -> Option<f64> {
    let text = credits.and_then(Value::as_str)?.trim();
    let digits: String = text
        .trim_start_matches('x')
        .trim_start_matches('X')
        .chars()
        .take_while(|character| character.is_ascii_digit() || *character == '.')
        .collect();
    let factor: f64 = digits.parse().ok()?;
    if factor.is_finite() && factor > 0.0 {
        Some(factor)
    } else {
        None
    }
}

fn number_of(value: Option<&Value>) -> Option<i64> {
    match value {
        Some(Value::Number(number)) => number.as_f64().filter(|n| n.is_finite()).map(|n| n as i64),
        Some(Value::String(text)) => text
            .trim()
            .parse::<f64>()
            .ok()
            .filter(|n| n.is_finite())
            .map(|n| n as i64),
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

fn declared_default_effort<'a>(level: &str, levels: &[&'a str]) -> Option<&'a str> {
    if level.trim().eq_ignore_ascii_case("ultra") {
        // A source alias may select only a rung the same source declared.
        return ["max", "xhigh", "high", "medium", "low"]
            .into_iter()
            .find_map(|candidate| levels.iter().copied().find(|level| *level == candidate));
    }
    engine_effort_level(level).filter(|level| levels.contains(level))
}

/// The catalog head: the entry the service marks as default, else the first one.
///
/// Both readers follow the config root and neither fetches, so a picker that has
/// to name a model before any catalog is in hand still names a real one.
pub(crate) fn first_model_key() -> Option<String> {
    let from = |models: &[Value]| -> Option<String> {
        models
            .iter()
            .find(|model| model.get("is_default") == Some(&json!(true)))
            .or_else(|| models.first())
            .and_then(|model| model.get("key"))
            .and_then(Value::as_str)
            .map(str::to_string)
    };
    {
        let guard = catalog_cache()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(cache) = guard
            .as_ref()
            .filter(|cache| cache.fingerprint == super::auth::catalog_fingerprint())
        {
            if now_ms() < cache.expires_at {
                return from(&cache.models);
            }
        }
    }
    from(&read_local_models())
}

/// Klaus's own non-secret catalog snapshot: what a successful live fetch left
/// behind, so a picker can paint before the network answers.
fn owned_catalog_path() -> std::path::PathBuf {
    super::config_root().join("workbuddy-model-catalog.json")
}

fn read_owned_catalog() -> Option<Value> {
    let fingerprint = super::auth::catalog_fingerprint()?;
    std::fs::read_to_string(owned_catalog_path())
        .ok()
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
        .filter(|value| {
            value.get("projectionVersion").and_then(Value::as_u64) == Some(1)
                && value.get("fingerprint").and_then(Value::as_str) == Some(fingerprint.as_str())
        })
}

fn read_local_models() -> Vec<Value> {
    read_owned_catalog()
        .and_then(|value| value.get("models").and_then(Value::as_array).cloned())
        .unwrap_or_default()
}

fn write_catalog_snapshot(models: &[Value], model_metadata: &[Value], fingerprint: &str) {
    let path = owned_catalog_path();
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Ok(serialized) = serde_json::to_string(&json!({
        "projectionVersion": 1, "models": models, "modelMetadata": model_metadata, "fingerprint": fingerprint
    })) {
        let _ = std::fs::write(&path, serialized);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
        }
    }
}

#[cfg(test)]
fn write_owned_catalog(models: &[Value], fingerprint: &str) {
    write_catalog_snapshot(models, models, fingerprint);
}

async fn refresh_live_catalog() -> Result<Vec<Value>, WorkBuddyError> {
    let fingerprint = super::auth::catalog_fingerprint();
    match fetch_live_models().await {
        Ok((models, model_metadata)) if !models.is_empty() => {
            if fingerprint != super::auth::catalog_fingerprint() {
                return Ok(models);
            }
            {
                let mut guard = catalog_cache()
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                *guard = Some(CatalogCache {
                    fingerprint: fingerprint.clone(),
                    models: models.clone(),
                    model_metadata: model_metadata.clone(),
                    expires_at: now_ms() + CATALOG_TTL_MS,
                });
            }
            if let Some(fingerprint) = fingerprint.as_deref() {
                write_catalog_snapshot(&models, &model_metadata, fingerprint);
            }
            Ok(models)
        }
        Ok(_) => Err(WorkBuddyError {
            message: "the WorkBuddy catalog answered with no usable model".into(),
            not_found: true,
            login_expired: false,
        }),
        Err(error) if error.login_expired && super::auth::can_refresh() => {
            if fingerprint != super::auth::catalog_fingerprint() {
                return Err(error);
            }
            // A login Klaus itself started, and WorkBuddy rotated the token out
            // from under it: worth one refresh before the picker is told the
            // subscription is dead. An ADOPTED login is not retried here — Klaus
            // may not spend that refresh token — so it falls through to the arm
            // below and keeps `login_expired`, which is the flag the host keys its
            // re-import on.
            super::auth::refresh_token_now()
                .await
                .map_err(|refresh| WorkBuddyError {
                    message: format!("{} (after refresh: {refresh})", error.message),
                    ..error
                })?;
            let (models, model_metadata) = fetch_live_models().await?;
            if fingerprint != super::auth::catalog_fingerprint() {
                return Ok(models);
            }
            if models.is_empty() {
                return Err(WorkBuddyError {
                    message: "the WorkBuddy catalog answered with no usable model".into(),
                    not_found: true,
                    login_expired: false,
                });
            }
            {
                let mut guard = catalog_cache()
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                *guard = Some(CatalogCache {
                    fingerprint: fingerprint.clone(),
                    models: models.clone(),
                    model_metadata: model_metadata.clone(),
                    expires_at: now_ms() + CATALOG_TTL_MS,
                });
            }
            if let Some(fingerprint) = fingerprint.as_deref() {
                write_catalog_snapshot(&models, &model_metadata, fingerprint);
            }
            Ok(models)
        }
        Err(error) => Err(error),
    }
}

/// A settings page's read: stale-while-revalidate, so the pane paints at once
/// and refreshes behind itself.
pub async fn fetch_models() -> Result<Vec<Value>, WorkBuddyError> {
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
                eprintln!("[WorkBuddy] catalog refresh failed: {}", error.message);
            }
        });
        return Ok(stale);
    }
    match refresh_live_catalog().await {
        Ok(models) => Ok(models),
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

/// A picker's read: signed out is an ERROR, so the picker paints EMPTY and agrees
/// with the settings pane's "not signed in" instead of fabricating options from a
/// stale file. A matching owned snapshot paints immediately and refreshes behind
/// the picker; without one, the live catalog gets a bounded first chance.
pub async fn fetch_models_warm() -> Result<Vec<Value>, WorkBuddyError> {
    if super::auth::credential().is_none() {
        return Err(WorkBuddyError {
            message: "WorkBuddy subscription is not signed in".into(),
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
    let local = read_local_models();
    if !local.is_empty() {
        tokio::spawn(async {
            if let Err(error) = refresh_live_catalog().await {
                eprintln!("[WorkBuddy] catalog refresh failed: {}", error.message);
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
        Ok(Ok(models)) => Ok(models),
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
                    eprintln!("[WorkBuddy] catalog refresh failed: {}", error.message);
                }
            });
            Ok(stale)
        }
    }
}

/// Explicit model IDs survive catalog visibility changes. Only an automatic
/// selection follows the service default or the first available entry.
pub(crate) fn resolve_model_key(models: &[Value], requested: &str) -> String {
    if !requested.is_empty() && requested != "auto" {
        return requested.to_string();
    }
    models
        .iter()
        .find(|model| model["is_default"] == true)
        .or_else(|| models.iter().find(|model| model["key"] == DEFAULT_MODEL))
        .or_else(|| models.first())
        .and_then(|model| model["key"].as_str())
        .unwrap_or(DEFAULT_MODEL)
        .to_string()
}

pub(crate) async fn lookup_model(key: &str) -> Option<Value> {
    let models = fetch_models().await.ok()?;
    let matching = |model: &&Value| model.get("key").and_then(Value::as_str) == Some(key);
    if let Some(model) = models.iter().find(matching) {
        return Some(model.clone());
    }
    {
        let guard = catalog_cache()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(model) = guard
            .as_ref()
            .filter(|cache| cache.fingerprint == super::auth::catalog_fingerprint())
            .and_then(|cache| cache.model_metadata.iter().find(matching))
        {
            return Some(model.clone());
        }
    }
    read_owned_catalog()?
        .get("modelMetadata")?
        .as_array()?
        .iter()
        .find(matching)
        .cloned()
}

pub(crate) fn context_window(entry: Option<&Value>) -> i64 {
    entry
        .and_then(|model| {
            model
                .get("contextTokens")
                .and_then(Value::as_array)
                .and_then(|tokens| tokens.iter().filter_map(Value::as_i64).next_back())
                .or_else(|| model.get("max_input_tokens").and_then(Value::as_i64))
        })
        .filter(|tokens| *tokens > 0)
        .unwrap_or(DEFAULT_CONTEXT_TOKENS)
}

/// What the main model can do, declared from the catalog rather than from the
/// model id: which levels a model answers to is the service's statement, and a
/// Klaus-side guess would send an effort the gateway never named.
pub async fn model_capabilities(key: &str) -> Value {
    let entry = lookup_model(key).await;
    let mut capabilities = json!({
        "vision": entry
            .as_ref()
            .and_then(|model| model.get("is_vl"))
            .and_then(Value::as_bool)
            .unwrap_or(false),
        "contextWindow": context_window(entry.as_ref()),
        // The standard OpenAI effort vocabulary, because that is what the gateway
        // accepts: it takes `reasoning_effort` verbatim in
        // {low, medium, high, xhigh, max}. Two dialects are deliberately NOT used
        // here:
        //   * Qoder's, whose whole difference is the literal `none` it sends to
        //     detach thinking — this gateway answers `none` with HTTP 200 and
        //     then reasons ANYWAY (measured on kimi-k2.8-preview: `none` produced
        //     more reasoning output than `low`).
        //   * plain OpenAI, which sends the output cap as `max_completion_tokens`.
        //     This gateway ignores that field entirely — measured: a cap of 20
        //     yielded 404 completion tokens — and honours only `max_tokens`, the
        //     same field its own client sends. Since WorkBuddy bills per credit
        //     consumed, a silently uncapped turn is a billing bug, not a cosmetic
        //     one. `legacyTokens` spells the cap the way this endpoint reads it.
        "reasoningDialect": "legacyTokens",
    });
    if let Some(output) = entry
        .as_ref()
        .and_then(|model| model.get("max_output_tokens"))
        .and_then(Value::as_i64)
        .filter(|tokens| *tokens > 0)
    {
        capabilities["maxOutputTokens"] = json!(output);
    }
    if let Some(model) = entry.as_ref() {
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
        if !levels.is_empty() {
            let mut effort = json!({ "levels": levels });
            if let Some(default) = model
                .get("defaultEffort")
                .and_then(Value::as_str)
                .and_then(|level| declared_default_effort(level, &levels))
            {
                effort["default"] = json!(default);
            }
            capabilities["effort"] = effort;
        }
    }
    capabilities
}

#[cfg(test)]
mod tests {
    use super::*;
    crate::app_server::catalog_test_support::warm_catalog_tests!(
        "workbuddy", read_local_models, model_metadata: Vec::new()
    );

    fn model(id: &str, extra: Value) -> Value {
        let mut value = json!({
            "id": id,
            "name": id.to_uppercase(),
            "supportsToolCall": true,
        });
        if let (Some(map), Some(extra)) = (value.as_object_mut(), extra.as_object()) {
            for (key, item) in extra {
                map.insert(key.clone(), item.clone());
            }
        }
        value
    }

    #[test]
    fn catalog_keeps_chat_models_without_account_or_location_filtering() {
        let parsed = json!({
            "code": 0,
            "data": { "models": [
                model("fast-model", json!({ "maxInputTokens": 200000, "isDefault": true })),
                // Image and video generators: no tool support, not chat models.
                model("gpt-image-2.5", json!({ "supportsToolCall": null, "tags": ["text-to-image"] })),
                model("seedance-2.5", json!({ "supportsToolCall": null, "videoGeneration": {} })),
                model("hunyuan-image-v3.0", json!({ "supportsToolCall": null, "tags": [] })),
                // The user's own local model: a llama-server on 127.0.0.1.
                model("custom-local:local_model", json!({ "local": true })),
                model("retired", json!({ "disabled": true })),
            ] }
        });
        let models = parse_config_models(&parsed);
        let keys: Vec<&str> = models
            .iter()
            .filter_map(|entry| entry.get("key").and_then(Value::as_str))
            .collect();
        assert_eq!(
            keys,
            vec!["fast-model", "custom-local:local_model", "retired"]
        );
    }

    #[test]
    fn catalog_reads_a_bare_models_array_too() {
        let parsed = json!({ "models": [model("deep-model", json!({}))] });
        assert_eq!(parse_config_models(&parsed).len(), 1);
    }

    #[test]
    fn agent_allowlists_do_not_restrict_the_model_directory() {
        let parsed = json!({
            "data": {
                "models": [
                    model("fast-model", json!({})),
                    model("hy4-preview", json!({})),
                    model("hidden-by-admin", json!({})),
                ],
                "agents": [
                    { "name": "other", "models": ["hidden-by-admin"] },
                    { "name": "cli", "tags": ["cli", "default"], "models": ["fast-model", "hy4-preview"] },
                ],
            }
        });
        let models = parse_config_models(&parsed);
        let keys: Vec<&str> = models
            .iter()
            .filter_map(|entry| entry.get("key").and_then(Value::as_str))
            .collect();
        assert_eq!(keys, vec!["fast-model", "hy4-preview", "hidden-by-admin"]);
    }

    #[test]
    fn enabled_auto_tiers_preserve_the_full_catalog() {
        let config = json!({
            "productFeatures": {"EnableAutoModelTiers": true},
            "models": [
                model("auto", json!({})),
                model("chat", json!({})),
                model("deep-model", json!({})),
                model("hidden", json!({})),
                model("balanced-model", json!({})),
                model("fast-model", json!({"isDefault": true})),
            ],
            "agents": [{"name": "cli", "models": ["auto", "chat"]}],
        });
        let keys = |config: &Value| {
            parse_config_models(config)
                .iter()
                .map(|entry| entry["key"].as_str().unwrap().to_string())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            keys(&config),
            AUTO_TIERS
                .into_iter()
                .chain(["chat", "hidden"])
                .collect::<Vec<_>>()
        );
        for flag in [json!(false), Value::Null] {
            let mut disabled = config.clone();
            disabled["productFeatures"]["EnableAutoModelTiers"] = flag;
            assert_eq!(
                keys(&disabled),
                vec![
                    "auto",
                    "chat",
                    "deep-model",
                    "hidden",
                    "balanced-model",
                    "fast-model"
                ]
            );
        }
        let mut hidden_auto = config.clone();
        hidden_auto["agents"][0]["models"] = json!(["chat"]);
        assert_eq!(keys(&hidden_auto), keys(&config));
        let mut incomplete = config;
        incomplete["models"]
            .as_array_mut()
            .unwrap()
            .retain(|model| model["id"] != "deep-model");
        assert_eq!(
            keys(&incomplete),
            vec!["fast-model", "balanced-model", "chat", "hidden"]
        );
    }

    #[test]
    fn a_config_that_publishes_no_whitelist_shows_every_chat_model() {
        // Absent agents, an empty whitelist and an agent with no `models` array all
        // mean "nothing withheld" — blanking the picker here would be a regression
        // against a config that simply carries no visibility rules.
        let models = json!([
            model("fast-model", json!({})),
            model("deep-model", json!({}))
        ]);
        for data in [
            json!({ "models": models }),
            json!({ "models": models, "agents": [] }),
            json!({ "models": models, "agents": [{ "name": "cli", "models": [] }] }),
            json!({ "models": models, "agents": [{ "name": "cli" }] }),
        ] {
            assert_eq!(
                parse_config_models(&json!({ "data": data })).len(),
                2,
                "{data}"
            );
        }
    }

    #[test]
    fn agent_selection_does_not_narrow_the_catalog() {
        let parsed = json!({
            "data": {
                "models": [model("fast-model", json!({})), model("deep-model", json!({}))],
                "agents": [
                    { "name": "cli", "models": ["deep-model"] },
                    { "name": "workbuddy", "tags": ["default"], "models": ["fast-model"] },
                ],
            }
        });
        let models = parse_config_models(&parsed);
        assert_eq!(models.len(), 2);
        assert_eq!(models[0]["key"], json!("fast-model"));
        assert_eq!(models[1]["key"], json!("deep-model"));
    }

    #[test]
    fn supported_efforts_become_the_engine_ladder_with_its_default() {
        let entry = catalog_entry(&model(
            "kimi-k2.8-preview",
            json!({
                "reasoning": {
                    "supportedEfforts": ["low", "high", "max"],
                    "defaultEffort": "high",
                    "canDisableThinking": true,
                },
                "maxInputTokens": 1000000,
                "maxOutputTokens": 32000,
                "supportsImages": true,
            }),
        ))
        .expect("chat model");
        assert_eq!(entry["efforts"], json!(["low", "high", "max"]));
        assert_eq!(entry["defaultEffort"], json!("high"));
        // The catalog says thinking can be disabled; the gateway disagrees, so
        // the entry must not advertise a rung Klaus cannot deliver.
        assert!(entry.get("canDisableThinking").is_none());
        assert_eq!(entry["max_input_tokens"], json!(1000000));
        assert_eq!(entry["contextTokens"], json!([1000000]));
        assert_eq!(entry["max_output_tokens"], json!(32000));
        assert_eq!(entry["is_vl"], json!(true));
    }

    #[test]
    fn a_fixed_effort_still_declares_one_rung() {
        let entry = catalog_entry(&model(
            "fast-model",
            json!({ "reasoning": { "effort": "medium", "summary": "auto" } }),
        ))
        .expect("chat model");
        assert_eq!(entry["efforts"], json!(["medium"]));
        assert_eq!(entry["defaultEffort"], json!("medium"));
    }

    #[test]
    fn credit_copy_becomes_a_numeric_multiplier() {
        assert_eq!(credit_factor(Some(&json!("x0.34 credits"))), Some(0.34_f64));
        assert_eq!(credit_factor(Some(&json!("x6.67"))), Some(6.67_f64));
        // An empty string is the catalog's "no charge", not a zero multiplier.
        assert_eq!(credit_factor(Some(&json!(""))), None);
        assert_eq!(credit_factor(Some(&json!("x0.00"))), None);
        assert_eq!(credit_factor(None), None);
    }

    #[test]
    fn zh_copy_wins_and_both_spellings_survive() {
        let entry = catalog_entry(&model(
            "balanced-model",
            json!({
                "descriptionZh": "速度与质量兼顾",
                "descriptionEn": "Balanced speed and quality",
                "iconUrl": "https://example.test/icon.png",
            }),
        ))
        .expect("chat model");
        assert_eq!(entry["description"], json!("速度与质量兼顾"));
        assert_eq!(entry["descriptionEn"], json!("Balanced speed and quality"));
        assert_eq!(entry["icon"], json!("https://example.test/icon.png"));
    }

    #[test]
    fn resolution_preserves_explicit_ids_and_defaults_only_automatic_selections() {
        let models = vec![
            json!({ "key": "default-model", "is_default": true }),
            json!({ "key": "gpt-5.5" }),
        ];
        assert_eq!(resolve_model_key(&models, "gpt-5.5"), "gpt-5.5");
        assert_eq!(resolve_model_key(&models, ""), "default-model");
        assert_eq!(resolve_model_key(&models, "auto"), "default-model");
        assert_eq!(
            resolve_model_key(
                &[
                    json!({"key":"first"}),
                    json!({"key":"preferred","is_default":true})
                ],
                ""
            ),
            "preferred"
        );
        assert_eq!(
            resolve_model_key(&models, "not-in-catalog"),
            "not-in-catalog"
        );
    }

    #[tokio::test]
    async fn the_head_is_the_service_default_not_the_first_row() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        forget_cache();
        crate::app_server::catalog_test_support::store_credential("workbuddy", temp.path(), "head");
        let fingerprint = super::super::auth::catalog_fingerprint().expect("fingerprint");
        // Clear the process-global cache so the snapshot below is what answers;
        // another test's spawned refresh must not win this read.
        forget_cache();
        std::fs::write(
            owned_catalog_path(),
            json!({ "projectionVersion": 1, "fingerprint": fingerprint, "models": [
                { "key": "gpt-5.5" },
                { "key": "default-model", "is_default": true },
            ] })
            .to_string(),
        )
        .expect("snapshot written");
        assert_eq!(
            first_model_key().as_deref(),
            Some("default-model"),
            "the head follows the service's own default flag, not array order"
        );
    }

    #[tokio::test]
    async fn the_head_falls_back_to_the_first_row_when_nothing_is_flagged() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        forget_cache();
        crate::app_server::catalog_test_support::store_credential("workbuddy", temp.path(), "head");
        let fingerprint = super::super::auth::catalog_fingerprint().expect("fingerprint");
        std::fs::write(
            owned_catalog_path(),
            json!({ "projectionVersion": 1, "fingerprint": fingerprint, "models": [{ "key": "gpt-5.5" }, { "key": "kimi-k3" }] }).to_string(),
        )
        .expect("snapshot written");
        assert_eq!(first_model_key().as_deref(), Some("gpt-5.5"));
    }

    #[tokio::test]
    async fn the_head_is_none_when_no_catalog_is_reachable() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        forget_cache();
        // No snapshot and nothing cached: a picker must be told there is no head
        // rather than be handed an invented one.
        assert_eq!(first_model_key(), None);
    }

    #[test]
    fn a_chat_entry_projects_a_full_catalog_row() {
        let entry = catalog_entry(&model(
            "gpt-5.6-terra",
            json!({
                "name": "GPT-5.6 Terra",
                "maxInputTokens": 272000,
                "maxOutputTokens": 72000,
                "supportsImages": true,
                "supportsReasoning": true,
                "credits": "x1.39 credits",
                "reasoning": {
                    "supportedEfforts": ["low", "medium", "high", "xhigh", "max"],
                    "defaultEffort": "high",
                    "canDisableThinking": true,
                },
                "isDefault": true,
            }),
        ))
        .expect("chat model");
        assert_eq!(entry["key"], json!("gpt-5.6-terra"));
        assert_eq!(entry["display_name"], json!("GPT-5.6 Terra"));
        assert_eq!(entry["priceFactor"], json!(1.39));
        assert_eq!(entry["is_default"], json!(true));
        assert_eq!(
            entry["efforts"],
            json!(["low", "medium", "high", "xhigh", "max"])
        );
        assert_eq!(entry["defaultEffort"], json!("high"));
        assert_eq!(entry["is_vl"], json!(true));
        assert_eq!(entry["is_reasoning"], json!(true));
    }

    #[test]
    fn an_entry_without_reasoning_declares_no_ladder() {
        // `deep-model` ships no `reasoning` block and no `supportsReasoning`:
        // inventing a ladder would put a thinking control on a model that has
        // nothing to do with it.
        let entry = catalog_entry(&model(
            "deep-model",
            json!({ "maxInputTokens": 176000, "credits": "x3.33 credits" }),
        ))
        .expect("chat model");
        assert!(entry.get("efforts").is_none());
        assert!(entry.get("defaultEffort").is_none());
        assert_eq!(entry["is_reasoning"], json!(false));
        assert_eq!(entry["priceFactor"], json!(3.33));
    }

    #[tokio::test]
    async fn capabilities_declare_the_legacy_output_cap_and_never_qoders_none() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        forget_cache();
        crate::app_server::catalog_test_support::store_credential("workbuddy", temp.path(), "caps");
        let _gateway = crate::app_server::catalog_test_support::Gateway::start(
            "workbuddy",
            std::time::Duration::from_secs(5),
        )
        .await;
        let fingerprint = super::super::auth::catalog_fingerprint().expect("fingerprint");
        // A catalog snapshot the warm read would otherwise fetch for.
        std::fs::write(
            super::super::config_root().join("workbuddy-model-catalog.json"),
            json!({ "projectionVersion": 1, "fingerprint": fingerprint, "models": [
                catalog_entry(&model("gpt-5.6-terra", json!({
                    "maxInputTokens": 272000,
                    "maxOutputTokens": 72000,
                    "supportsImages": true,
                    "reasoning": { "supportedEfforts": ["low", "high"] },
                }))).expect("chat model"),
            ] })
            .to_string(),
        )
        .expect("snapshot written");

        let capabilities = model_capabilities("gpt-5.6-terra").await;
        // `none` is the whole reason Qoder has its own dialect, and this gateway
        // accepts it without detaching thought; and plain OpenAI would spell the
        // output cap in a field this gateway ignores. Both are wrong here.
        assert_eq!(capabilities["reasoningDialect"], json!("legacyTokens"));
        assert_ne!(capabilities["reasoningDialect"], json!("qoder"));
        assert_eq!(capabilities["contextWindow"], json!(272000));
        assert_eq!(capabilities["maxOutputTokens"], json!(72000));
        assert_eq!(capabilities["vision"], json!(true));
        assert_eq!(capabilities["effort"]["levels"], json!(["low", "high"]));
        assert!(
            capabilities.get("thinking").is_none(),
            "no off rung: the gateway cannot detach thinking"
        );
    }

    #[test]
    fn context_window_falls_back_when_the_entry_declares_nothing() {
        assert_eq!(context_window(None), DEFAULT_CONTEXT_TOKENS);
        assert_eq!(context_window(Some(&json!({}))), DEFAULT_CONTEXT_TOKENS);
        assert_eq!(
            context_window(Some(&json!({ "max_input_tokens": 256000 }))),
            256000
        );
        assert_eq!(
            context_window(Some(&json!({ "contextTokens": [1000, 200000] }))),
            200000
        );
    }
    #[test]
    fn a_vendor_top_alias_selects_only_a_declared_native_rung() {
        for (levels, expected) in [
            (json!(["low", "high"]), "high"),
            (json!(["low", "max"]), "max"),
            (json!(["low", "high", "ultra"]), "high"),
        ] {
            let entry = catalog_entry(&json!({"id": "fixture", "reasoning": {
                "supportedEfforts": levels, "defaultEffort": "ultra"
            }}))
            .expect("entry");
            assert_eq!(entry["defaultEffort"], json!(expected));
            assert!(!entry["efforts"]
                .as_array()
                .unwrap()
                .contains(&json!("ultra")));
        }
        let unknown = catalog_entry(&json!({"id": "fixture", "reasoning": {
            "supportedEfforts": ["ultra"], "defaultEffort": "ultra"
        }}))
        .expect("entry");
        assert!(unknown.get("efforts").is_none());
        assert!(unknown.get("defaultEffort").is_none());
    }
}
