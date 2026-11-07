//! The Bailian Token Plan catalog and its declared capability matrix.
//!
//! The model LIST is live: Token Plan serves different models per edition (the
//! team edition adds `qwen3.6-plus`, the `kimi-k2.x` family, `glm-5.1`/`glm-5`
//! and `MiniMax-M2.5`), so a hardcoded list would either hide models a user's plan
//! serves or offer ones it refuses with `403 AccessDenied`. The list is read from
//! the OpenAI-compatible endpoint, because the Anthropic endpoint serves no
//! catalog route at all (`/v1/models` there answers 404, which is exactly what
//! Bailian's own docs say).
//!
//! The CAPABILITIES beside that list are a declared table, and that is not
//! laziness: `/compatible-mode/v1/models` returns `id`, `created`, `owned_by` and
//! nothing else — no context length, no effort ladder, no modality. Every value in
//! the table below was measured against the live endpoint on a real personal-edition
//! key on 2026-09-29, and each row says what was measured. A model the table does
//! not know gets conservative defaults rather than a guess.

use serde_json::{json, Map, Value};
use std::sync::{Mutex, OnceLock};

const MODELS_PATH: &str = "/models";
const CATALOG_TTL_MS: i64 = 6 * 60 * 60 * 1000;
const FETCH_TIMEOUT_MS: u64 = 15_000;
/// A picker without a matching snapshot gives the live list this bounded window.
const WARM_TIMEOUT_MS: u64 = 1500;
/// OpenAI-style listing is paged; 100 is the largest page the shape implies and
/// the current answer is one page of 16 with `has_more: false`.
const PAGE_SIZE: i64 = 100;
const MAX_PAGES: u8 = 10;

/// What a bare selection names when nothing else can answer.
///
/// Deliberately NOT `auto`: it is excluded from the catalog because it refuses the
/// `tool_choice` parameter the engine must send for structured output, which would
/// break session titles. See [`STRUCTURALLY_UNUSABLE_MODELS`].
///
/// `qwen3.8-max` is the plan's flagship chat model, measured taking the forced-tool
/// structured path cleanly (`can_disable_thinking` true), so a bare selection lands
/// on a model that serves every path a session uses. The live catalog's head still
/// wins when reachable — and this key is what the head resolves to, because
/// [`first_model_key`] prefers it over the service's own listing order.
const FALLBACK_MODEL: &str = "qwen3.8-max";

/// The model an unmapped low tier is pinned to.
///
/// A separate declaration from [`FALLBACK_MODEL`] on purpose. The low tier is what
/// session titles run on, and title generation is a structured request, so the pin
/// has to be a model measured accepting that shape — but it should also be the
/// cheapest one that is, because a title costs nothing in quality by running on a
/// light model. Coupling the two would price every conversation's title at the
/// flagship the moment the flagship became the default, which is a cost change
/// nobody asked for.
const STRUCTURED_SAFE_PIN: &str = "qwen3.8-flash";

/// Media models belong to their dedicated image and audio catalogs.
const NON_CHAT_MODELS: &[&str] = &[
    "wan2.7-image",
    "wan2.7-image-pro",
    "qwen-audio-3.0-tts-plus",
    "qwen-audio-3.0-realtime-plus",
];

/// A model that talks fine but cannot serve everything the engine asks of it.
///
/// `auto` is the platform's routing model and handles ordinary turns well — but it
/// refuses the `tool_choice` parameter outright (`400 auto does not support
/// parameter(s): tool_choice`). On a third-party Anthropic wire the engine cannot
/// use native structured output (the provider gates that on `is_first_party`, which
/// is an anti-abuse boundary, not a knob), so every structured request goes through
/// forced-tool emulation and puts `tool_choice` on the wire. Session title
/// generation is exactly such a request, so selecting `auto` would break the title
/// of every conversation while everything else appeared to work.
///
/// That is worse than not offering it: the ten concrete models in this catalog
/// cover the same ground, and the routing convenience is not worth a failure that
/// reads as an unrelated bug.
const STRUCTURALLY_UNUSABLE_MODELS: &[&str] = &["auto"];

/// The full five-rung ladder. Measured accepted on every chat model below EXCEPT
/// `glm-5.3`, which refuses `medium` and `xhigh` with HTTP 400.
const FULL_LADDER: [&str; 5] = ["low", "medium", "high", "xhigh", "max"];
/// What `glm-5.3` actually accepts. Measured: low/high/max return 200, and
/// medium/xhigh return 400 `InvalidParameter`.
const GLM_53_LADDER: [&str; 3] = ["low", "high", "max"];

/// One measured model: context window, whether thinking can be turned off,
/// whether it reads images, which effort rungs it accepts, and whether it takes
/// the native structured-output shape.
struct ModelFacts {
    reasoning_known: bool,
    context_tokens: i64,
    can_disable_thinking: bool,
    vision: bool,
    /// `None` means the full ladder.
    ladder: Option<&'static [&'static str]>,
    native_structured_output: bool,
}

/// The measured matrix.
///
/// Context windows are the service's OWN stated input ceiling, read out of its
/// refusal message (`Range of input length should be [1, N]`) rather than from a
/// marketing page — that is the number the engine must not pack past, because
/// overshooting it kills a turn with a provider 400 instead of compacting. The
/// qwen family's 983616 independently matches the value Bailian's own Claude Code
/// preset sets for `CLAUDE_CODE_MAX_CONTEXT_TOKENS`.
fn facts(model: &str) -> ModelFacts {
    match model {
        // `auto` is the platform's routing model: it accepted every rung, took an
        // image and answered its colour, turned thinking off cleanly, and honoured
        // the native JSON schema.
        "auto" | "qwen3.8-max" | "qwen3.8-flash" | "qwen3.7-plus" | "qwen3.6-flash" => ModelFacts {
            reasoning_known: true,
            context_tokens: 983_616,
            can_disable_thinking: true,
            vision: true,
            ladder: None,
            native_structured_output: true,
        },
        // Measured: rejects an image block outright ("Unexpected item type in
        // content"), while its siblings read the same image and named its colour.
        "qwen3.7-max" => ModelFacts {
            reasoning_known: true,
            context_tokens: 983_616,
            can_disable_thinking: true,
            vision: false,
            ladder: None,
            native_structured_output: true,
        },
        // Measured: refuses `thinking:{"type":"disabled"}` with
        // `enable_thinking parameter is restricted to True`, and refuses medium
        // and xhigh. Native structured output works, which is the only reason it
        // can be used for structured work at all — the forced-tool emulation the
        // engine falls back to is the exact shape this model rejects.
        "glm-5.3" => ModelFacts {
            reasoning_known: true,
            context_tokens: 1_048_576,
            can_disable_thinking: false,
            vision: false,
            ladder: Some(&GLM_53_LADDER),
            native_structured_output: true,
        },
        "glm-5.2" => ModelFacts {
            reasoning_known: true,
            context_tokens: 1_048_576,
            can_disable_thinking: true,
            vision: false,
            ladder: None,
            native_structured_output: true,
        },
        "deepseek-v4-pro" => ModelFacts {
            reasoning_known: true,
            context_tokens: 1_000_000,
            can_disable_thinking: true,
            vision: false,
            ladder: None,
            native_structured_output: true,
        },
        "deepseek-v4-flash-0731" => ModelFacts {
            reasoning_known: true,
            context_tokens: 1_000_000,
            can_disable_thinking: true,
            vision: false,
            ladder: None,
            native_structured_output: true,
        },
        // The mirror image of glm-5.3: it refused the NATIVE shape with
        // `400 This response_format type is unavailable` while accepting the
        // forced-tool emulation. It also read an image and named its colour.
        "deepseek-v4.1-flash" => ModelFacts {
            reasoning_known: true,
            context_tokens: 1_000_000,
            can_disable_thinking: true,
            vision: true,
            ladder: None,
            native_structured_output: false,
        },
        // An unknown model — a team-edition key lists kimi/glm-5.1/MiniMax that a
        // personal key cannot see, and the platform adds models over time. Keep it
        // usable with the safest reading: no effort knob declared (an undeclared
        // rung is clamped away, a wrongly declared one is refused with 400), no
        // vision, a window small enough that compaction happens before the service
        // refuses, and the emulated structured-output path — which is what the
        // engine already does for every non-first-party wire and what every
        // measured model except one accepted.
        _ => ModelFacts {
            reasoning_known: false,
            context_tokens: 200_000,
            can_disable_thinking: true,
            vision: false,
            ladder: None,
            native_structured_output: false,
        },
    }
}

/// Why a catalog read failed, and which recovery applies.
///
/// `not_found` means there is nothing to list (no key stored), `login_expired`
/// means the key was refused so the host should send the user back to re-enter it.
#[derive(Debug, Clone)]
pub struct BailianError {
    pub message: String,
    pub not_found: bool,
    pub login_expired: bool,
}

impl std::fmt::Display for BailianError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

struct CatalogCache {
    fingerprint: Option<String>,
    models: Vec<Value>,
    expires_at: i64,
}

fn catalog_cache() -> &'static Mutex<Option<CatalogCache>> {
    static CACHE: OnceLock<Mutex<Option<CatalogCache>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(None))
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn http_client() -> Result<reqwest::Client, BailianError> {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(FETCH_TIMEOUT_MS))
        .user_agent(super::auth::user_agent())
        .build()
        .map_err(|error| BailianError {
            message: error.to_string(),
            not_found: false,
            login_expired: false,
        })
}

/// Read every page the catalog serves.
///
/// Paging is honoured rather than assumed away: the answer carries `has_more` and
/// a cursor, and silently taking one page would drop models the moment the plan
/// grows past it. A dropped model is invisible to the user — the picker just stops
/// offering it — so the loop is worth the few lines.
async fn fetch_live_models() -> Result<Vec<Value>, BailianError> {
    let key = super::auth::api_key().ok_or_else(|| BailianError {
        // Prose, not a stable code: this message is what the settings tier card
        // renders when a catalog read fails, and a code would print as a code.
        message: "not signed in to a Bailian Token Plan".into(),
        not_found: true,
        login_expired: false,
    })?;
    let http = http_client()?;
    let base = super::auth::catalog_base();
    let mut models = Vec::new();
    let mut after: Option<String> = None;
    for _ in 0..MAX_PAGES {
        let mut request = http
            .get(format!("{base}{MODELS_PATH}"))
            .header("authorization", format!("Bearer {key}"))
            .query(&[("limit", PAGE_SIZE.to_string())]);
        if let Some(cursor) = after.as_deref() {
            request = request.query(&[("after", cursor.to_string())]);
        }
        let response = request.send().await.map_err(|error| BailianError {
            message: error.to_string(),
            not_found: false,
            login_expired: false,
        })?;
        let status_code = response.status().as_u16();
        let body = response.text().await.unwrap_or_default();
        if status_code == 401 || status_code == 403 {
            return Err(BailianError {
                message: "bailian_key_rejected".into(),
                not_found: false,
                login_expired: true,
            });
        }
        if !(200..300).contains(&status_code) {
            let detail: String = body.chars().take(200).collect();
            return Err(BailianError {
                message: format!("the Bailian catalog answered {status_code}: {detail}"),
                not_found: false,
                login_expired: false,
            });
        }
        let parsed: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
        let rows = parsed
            .get("data")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let last_id = parsed
            .get("last_id")
            .and_then(Value::as_str)
            .map(str::to_string);
        models.extend(rows.iter().filter_map(catalog_entry));
        match parsed.get("has_more").and_then(Value::as_bool) {
            Some(true) if last_id.is_some() => after = last_id,
            _ => break,
        }
    }
    if models.is_empty() {
        return Err(BailianError {
            message: "the Bailian catalog listed no usable model".into(),
            not_found: true,
            login_expired: false,
        });
    }
    Ok(models)
}

/// Project one catalog row onto the entry the picker and capability reader use.
///
/// Rows for models that cannot serve a conversation are dropped here, so no caller
/// has to remember to filter them.
fn catalog_entry(model: &Value) -> Option<Value> {
    let key = model
        .get("id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    if NON_CHAT_MODELS.contains(&key) || STRUCTURALLY_UNUSABLE_MODELS.contains(&key) {
        return None;
    }
    let facts = facts(key);
    let mut entry = Map::new();
    entry.insert("key".into(), json!(key));
    // The catalog carries no display name, so the id is the label.
    entry.insert("display_name".into(), json!(key));
    entry.insert("contextTokens".into(), json!(facts.context_tokens));
    entry.insert("is_vl".into(), json!(facts.vision));
    if facts.reasoning_known {
        entry.insert(
            "canDisableThinking".into(),
            json!(facts.can_disable_thinking),
        );
    }
    if facts.reasoning_known {
        let ladder: &[&str] = facts.ladder.unwrap_or(&FULL_LADDER);
        entry.insert("efforts".into(), json!(ladder));
        // Every model in the measured set thinks by default and honours the top rung,
        // so the ladder's head is a sensible default rather than an invented one.
        entry.insert("defaultEffort".into(), json!(ladder[ladder.len() - 1]));
    }
    super::super::conversation_catalog::mark_purpose("bailian", model, &mut entry);
    Some(Value::Object(entry))
}

fn owned_catalog_path() -> std::path::PathBuf {
    super::config_root().join("bailian-model-catalog.json")
}

fn read_local_models() -> Vec<Value> {
    let Some(fingerprint) = super::auth::catalog_fingerprint() else {
        return Vec::new();
    };
    std::fs::read_to_string(owned_catalog_path())
        .ok()
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
        .filter(|value| {
            value.get("projectionVersion").and_then(Value::as_u64) == Some(1)
                && value.get("fingerprint").and_then(Value::as_str) == Some(fingerprint.as_str())
        })
        .and_then(|value| value.get("models").and_then(Value::as_array).cloned())
        .unwrap_or_default()
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

/// Seed the on-disk snapshot so a test resolves against a known catalog.
///
/// With a snapshot present and the TTL cache empty, [`fetch_models`] hands it back
/// at once and refreshes behind itself, so a test asserts against a fixed list
/// instead of whatever the network answered. Same seam the Kimi catalog exposes.
#[cfg(test)]
pub fn seed_owned_catalog(models: &[Value]) {
    if let Some(fingerprint) = super::auth::catalog_fingerprint() {
        write_owned_catalog(models, &fingerprint);
    }
}

/// Invalidate the catalog on credential changes. Memory is cleared; a disk
/// snapshot remains unreadable unless its fingerprint matches the current key
/// and region.
pub fn forget_cache() {
    *catalog_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
}

fn remember(models: &[Value], fingerprint: Option<String>) {
    if fingerprint != super::auth::catalog_fingerprint() {
        return;
    }
    {
        let mut guard = catalog_cache()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *guard = Some(CatalogCache {
            fingerprint: fingerprint.clone(),
            models: models.to_vec(),
            expires_at: now_ms() + CATALOG_TTL_MS,
        });
    }
    if let Some(fingerprint) = fingerprint.as_deref() {
        write_owned_catalog(models, fingerprint);
    }
}

/// A settings page's read: stale-while-revalidate, so the pane paints at once and
/// refreshes behind itself.
pub async fn fetch_models() -> Result<Vec<Value>, BailianError> {
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
    let fingerprint = super::auth::catalog_fingerprint();
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
        tokio::spawn(async move {
            match fetch_live_models().await {
                Ok(models) => remember(&models, fingerprint),
                Err(error) => eprintln!("[Bailian] catalog refresh failed: {}", error.message),
            }
        });
        return Ok(stale);
    }
    match fetch_live_models().await {
        Ok(models) => {
            remember(&models, fingerprint);
            Ok(models)
        }
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

/// A picker's read: signed out is an ERROR, so the picker paints empty and agrees
/// with the settings pane rather than offering models no key can serve.
pub async fn fetch_models_warm() -> Result<Vec<Value>, BailianError> {
    if super::auth::api_key().is_none() {
        return Err(BailianError {
            message: "not signed in to a Bailian Token Plan".into(),
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
            if now_ms() < cache.expires_at && !cache.models.is_empty() {
                return Ok(cache.models.clone());
            }
        }
    }
    let local = read_local_models();
    let fingerprint = super::auth::catalog_fingerprint();
    if !local.is_empty() {
        tokio::spawn(async move {
            match fetch_live_models().await {
                Ok(models) => remember(&models, fingerprint),
                Err(error) => eprintln!("[Bailian] catalog refresh failed: {}", error.message),
            }
        });
        return Ok(local);
    }
    match tokio::time::timeout(
        std::time::Duration::from_millis(WARM_TIMEOUT_MS),
        fetch_live_models(),
    )
    .await
    {
        Ok(Ok(models)) => {
            remember(&models, fingerprint);
            Ok(models)
        }
        Ok(Err(error)) => {
            let local = read_local_models();
            if local.is_empty() {
                Err(error)
            } else {
                Ok(local)
            }
        }
        Err(_) => {
            let local = read_local_models();
            if local.is_empty() {
                Err(BailianError {
                    message: "the Bailian catalog did not answer in time".into(),
                    not_found: false,
                    login_expired: false,
                })
            } else {
                Ok(local)
            }
        }
    }
}

/// The catalog head: what a bare tier word resolves to.
pub fn first_model_key() -> Option<String> {
    let models = {
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
            _ => {
                drop(guard);
                read_local_models()
            }
        }
    };
    models
        .iter()
        .find(|model| model.get("key").and_then(Value::as_str) == Some(FALLBACK_MODEL))
        .or_else(|| models.first())
        .and_then(|model| model.get("key"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// The catalog key a requested key actually maps to.
///
/// An unknown key falls back to the routing model rather than going to the wire
/// as-is: the service answers an unlisted model with `400 Model not exist`, and a
/// stale selection should land on something that works.
pub fn resolve_model_key(models: &[Value], requested: &str) -> String {
    if models
        .iter()
        .any(|model| model.get("key").and_then(Value::as_str) == Some(requested))
    {
        return requested.to_string();
    }
    first_key_of(models).unwrap_or_else(|| FALLBACK_MODEL.to_string())
}

fn first_key_of(models: &[Value]) -> Option<String> {
    models
        .iter()
        .find(|model| model.get("key").and_then(Value::as_str) == Some(FALLBACK_MODEL))
        .or_else(|| models.first())
        .and_then(|model| model.get("key"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

pub async fn lookup_model(key: &str) -> Option<Value> {
    let models = fetch_models().await.ok()?;
    models
        .into_iter()
        .find(|model| model.get("key").and_then(Value::as_str) == Some(key))
}

pub fn context_window(entry: Option<&Value>) -> i64 {
    entry
        .and_then(|model| model.get("contextTokens"))
        .and_then(Value::as_i64)
        .filter(|tokens| *tokens > 0)
        .unwrap_or(200_000)
}

/// Whether a model can serve the engine's structured-output path.
///
/// On a third-party Anthropic wire the provider cannot use NATIVE structured
/// output — `use_native_structured_output` gates on `is_first_party`, which is an
/// anti-abuse boundary rather than a knob — so every structured request goes
/// through forced-tool emulation instead, and that path puts `tool_choice` on the
/// wire together with `thinking:{"type":"disabled"}`.
///
/// Measured against the live gateway with the exact body the engine builds, every
/// model this catalog serves accepts that combination except `glm-5.3`, which
/// refuses to turn thinking off (`400 The value of the enable_thinking parameter is
/// restricted to True`). It is also the only model here with
/// `can_disable_thinking: false`, so that measured field is the discriminator
/// rather than a name match.
///
/// This matters beyond structured output as a feature: session titles run on the
/// LOW tier as a structured request, so a main model that fails here silently loses
/// the generated title of every conversation that uses it.
pub fn structured_output_safe(key: &str) -> bool {
    facts(key).can_disable_thinking
}

/// A structured-safe model to pin an unmapped low tier to.
///
/// Prefers [`STRUCTURED_SAFE_PIN`] — the cheapest model measured taking the
/// forced-tool path — when the catalog serves it, so a title call stays on a light
/// model even after the plan's flagship became the default main model. Otherwise the
/// first safe model the plan serves. `None` only when the catalog offers no safe
/// model at all, in which case the caller leaves the tier unmapped and the engine
/// falls back to the main model as it would have anyway.
pub async fn structured_safe_fallback() -> Option<String> {
    let models = fetch_models().await.ok()?;
    let keys: Vec<String> = models
        .iter()
        .filter_map(|model| model.get("key").and_then(Value::as_str).map(str::to_string))
        .collect();
    keys.iter()
        .find(|key| key.as_str() == STRUCTURED_SAFE_PIN && structured_output_safe(key))
        .or_else(|| keys.iter().find(|key| structured_output_safe(key)))
        .cloned()
}

/// What a Bailian model can do, from the measured matrix.
///
/// Every field is something measured against the live endpoint, and the two that
/// matter most are the ones that break a session if declared wrongly:
///
/// * `structuredOutput` follows whether the model accepts `output_config.format`.
///   It is NOT uniformly true: measured, `deepseek-v4.1-flash` refuses it with
///   `400 This response_format type is unavailable` while accepting the engine's
///   forced-tool emulation, and `glm-5.3` is the mirror image — native works,
///   emulation 400s because the model will not turn thinking off. Declaring it for
///   both breaks one; declaring it for neither breaks the other.
/// * `thinking.adaptive` is true and `budget_tokens` is never used, because the
///   gateway answers `thinking:{"type":"enabled","budget_tokens":N}` with
///   `400 does not support parameter(s): thinking_budget`.
///
/// Absent on purpose: `nativePdf` and `webSearch`, neither of which was measured,
/// and `oneMillionContext`, because the 1M beta was only probed through the
/// platform's own `auto` routing and the windows here come from the service's
/// stated input ceilings instead.
pub async fn model_capabilities(key: &str) -> Value {
    capabilities_of(lookup_model(key).await.as_ref(), key)
}

/// The projection itself, over an entry already in hand.
///
/// Split out from the async lookup so the declared set is testable against the
/// measured matrix rather than against a copy of this code.
pub fn capabilities_of(entry: Option<&Value>, key: &str) -> Value {
    let facts = facts(key);
    let window = context_window(entry);
    let mut capabilities = json!({
        "vision": facts.vision,
        "contextWindow": window,
        // Prompt-cache breakpoints were measured accepted: a system block with
        // `cache_control` answered 200 rather than rejecting the field.
        "promptCacheControl": true,
        "structuredOutput": facts.native_structured_output,
        // `adaptive` only. The budget shape is refused with HTTP 400, so declaring
        // it would put a parameter on the wire the gateway rejects.
        "thinking": {
            "adaptive": true,
            "interleaved": false,
            "canDisableThinking": facts.can_disable_thinking,
        },
    });
    // `tools` is deliberately NOT declared, even though every chat model here was
    // measured driving tools (each emitted a `tool_use` block for a declared tool,
    // answered from a fed-back `tool_result`, and emitted TWO tool_use blocks when
    // asked about two cities — so this source can carry an agent session). The
    // Anthropic provider treats tool support as a provider-level truth and answers
    // `tools: true` unconditionally, never reading a per-model flag; only the OpenAI
    // provider reads one. Declaring it here would be a field that cannot change
    // behaviour, and declaring it FALSE for some model would be worse — silently
    // ignored, so the table would read as a control that does nothing.
    if let Some(model) = entry {
        let levels: Vec<&str> = model
            .get("efforts")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
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
                .filter(|default| levels.contains(default))
            {
                effort["default"] = json!(default);
            }
            capabilities["effort"] = effort;
        }
    }
    // `canDisableThinking` is deliberately NOT carried into the capability set: the
    // engine's capability decoder is strict and rejects an unknown field, which
    // fails session startup outright (`invalid protocol op: runtime codec
    // UnknownField at /op/init/capabilities/...`). It stays on the catalog ENTRY,
    // where the host reads it to decide whether the picker offers an off rung —
    // glm-5.3 refuses `thinking:{"type":"disabled"}` with HTTP 400, so offering one
    // there would be a control that breaks the turn.
    capabilities
}

#[cfg(test)]
mod tests {
    use super::*;
    crate::app_server::catalog_test_support::warm_catalog_tests!("bailian", read_local_models);

    /// One catalog row as the live endpoint returns it: id and bookkeeping only.
    fn row(id: &str) -> Value {
        json!({ "id": id, "object": "model", "owned_by": "system", "created": 1789973043 })
    }

    #[test]
    fn models_that_cannot_serve_a_conversation_are_dropped() {
        // Measured: the image generators reject a text message's content shape, the
        // audio models answer 500, and the decision model answers "url error".
        // Offering any of them would be a picker row that fails on selection.
        for non_chat in NON_CHAT_MODELS {
            assert!(
                catalog_entry(&row(non_chat)).is_none(),
                "{non_chat} must not reach the picker"
            );
        }
        assert!(catalog_entry(&row("qwen3.8-max")).is_some());
    }

    #[test]
    fn the_routing_model_is_excluded_because_it_refuses_tool_choice() {
        // Measured: `auto` answers 400 `auto does not support parameter(s):
        // tool_choice`. The engine must send tool_choice for every structured
        // request on a third-party Anthropic wire, and session titles are one, so
        // offering `auto` would break the title of every conversation that selected
        // it while the rest of the session appeared to work.
        assert!(catalog_entry(&row("auto")).is_none());
        assert_eq!(STRUCTURALLY_UNUSABLE_MODELS, &["auto"]);
    }

    #[test]
    fn a_row_without_an_id_is_not_a_model() {
        assert!(catalog_entry(&json!({ "object": "model" })).is_none());
        assert!(catalog_entry(&json!({ "id": "  " })).is_none());
    }

    #[test]
    fn the_default_is_the_flagship_and_a_structured_call_can_use_it() {
        // A bare selection lands on the plan's flagship chat model rather than on
        // whatever the service lists first. The flagship was measured taking the
        // forced-tool structured path (`can_disable_thinking` true), so the same
        // model can serve a conversation and the low-tier title call. A picker row
        // is labelled with its id because the catalog carries no display name of
        // its own.
        let entry = entry_of("qwen3.8-max");
        assert_eq!(entry["key"], json!("qwen3.8-max"));
        assert_eq!(entry["display_name"], json!("qwen3.8-max"));
        assert_eq!(FALLBACK_MODEL, "qwen3.8-max");
        // The low-tier pin stays on the cheaper measured-safe model, so raising the
        // default main model does not raise the cost of a title call.
        assert_eq!(STRUCTURED_SAFE_PIN, "qwen3.8-flash");
        assert!(structured_output_safe(STRUCTURED_SAFE_PIN));
    }

    #[test]
    fn context_windows_are_the_service_stated_ceilings() {
        // Read out of the gateway's own refusal (`Range of input length should be
        // [1, N]`), which is the number the engine must not pack past.
        assert_eq!(entry_of("qwen3.8-max")["contextTokens"], json!(983_616));
        assert_eq!(entry_of("glm-5.3")["contextTokens"], json!(1_048_576));
        assert_eq!(
            entry_of("deepseek-v4-pro")["contextTokens"],
            json!(1_000_000)
        );
    }

    #[test]
    fn glm_53_declares_only_the_three_rungs_it_accepts() {
        // Measured: medium and xhigh answer HTTP 400 on this model.
        let entry = entry_of("glm-5.3");
        assert_eq!(entry["efforts"], json!(["low", "high", "max"]));
        assert_eq!(entry["defaultEffort"], json!("max"));
        // And it will not turn thinking off, so no off rung may be offered.
        assert_eq!(entry["canDisableThinking"], json!(false));
        let capabilities = capabilities_of(Some(&entry), "glm-5.3");
        assert_eq!(
            capabilities["effort"]["levels"],
            json!(["low", "high", "max"]),
            "the ladder must not be padded back to five rungs"
        );
        assert!(capabilities.get("canDisableThinking").is_none());
    }

    #[test]
    fn every_other_chat_model_declares_the_full_ladder() {
        for model in [
            "qwen3.8-max",
            "qwen3.8-flash",
            "qwen3.7-plus",
            "qwen3.7-max",
            "qwen3.6-flash",
            "glm-5.2",
            "deepseek-v4-pro",
            "deepseek-v4.1-flash",
            "deepseek-v4-flash-0731",
        ] {
            assert_eq!(
                entry_of(model)["efforts"],
                json!(["low", "medium", "high", "xhigh", "max"]),
                "{model}"
            );
            assert_eq!(
                entry_of(model)["canDisableThinking"],
                json!(true),
                "{model}"
            );
        }
    }

    #[test]
    fn vision_follows_what_each_model_actually_read() {
        // Each of these was shown one solid-colour image and named the colour;
        // qwen3.7-max refused the image block outright and the glm/deepseek rows
        // answered "White"/"unanswerable", i.e. they did not see it.
        for sees in [
            "qwen3.8-max",
            "qwen3.8-flash",
            "qwen3.7-plus",
            "qwen3.6-flash",
            "deepseek-v4.1-flash",
        ] {
            assert_eq!(entry_of(sees)["is_vl"], json!(true), "{sees} reads images");
        }
        for blind in ["qwen3.7-max", "glm-5.3", "glm-5.2", "deepseek-v4-pro"] {
            assert_eq!(entry_of(blind)["is_vl"], json!(false), "{blind} does not");
        }
    }

    #[test]
    fn structured_output_is_declared_per_model_not_uniformly() {
        // The two mirror-image cases: glm-5.3 refuses the forced-tool emulation
        // (it will not disable thinking), deepseek-v4.1-flash refuses the native
        // shape. Declaring one value for both breaks one of them.
        assert!(
            capabilities_of(Some(&entry_of("glm-5.3")), "glm-5.3")["structuredOutput"]
                .as_bool()
                .unwrap()
        );
        assert!(!capabilities_of(
            Some(&entry_of("deepseek-v4.1-flash")),
            "deepseek-v4.1-flash"
        )["structuredOutput"]
            .as_bool()
            .unwrap());
        assert!(
            capabilities_of(Some(&entry_of("qwen3.8-flash")), "qwen3.8-flash")["structuredOutput"]
                .as_bool()
                .unwrap()
        );
    }

    #[test]
    fn thinking_declares_adaptive_only_because_the_budget_shape_is_refused() {
        // Measured 400: "auto does not support parameter(s): thinking_budget",
        // and the qwen family answers the same way.
        let capabilities = capabilities_of(Some(&entry_of("qwen3.8-max")), "qwen3.8-max");
        assert_eq!(
            capabilities["thinking"],
            json!({
                "adaptive": true,
                "interleaved": false,
                "canDisableThinking": facts("qwen3.8-max").can_disable_thinking,
            })
        );
    }

    #[test]
    fn the_capability_set_only_names_fields_the_engine_codec_accepts() {
        // Regression: `canDisableThinking` was emitted here and the engine's
        // capability decoder is `deny_unknown: true`, so EVERY session on this
        // source failed at startup with
        //   invalid protocol op: runtime codec UnknownField at
        //   /op/init/capabilities/canDisableThinking
        // — a failure the unit tests could not see, because they assert the
        // projection's shape rather than that the shape is decodable. The field
        // belongs on the catalog entry (where the host reads it to decide whether
        // the picker offers an off rung), not in the capability set.
        //
        // This list is the whole legal vocabulary, copied from
        // `impl_runtime_struct!(ModelCapabilitiesInit, deny_unknown: true, …)` in
        // protocol/src/types/exact_runtime_codec.rs. A capability added here must be
        // added there too, or it is not a capability — it is a startup crash.
        const LEGAL: [&str; 14] = [
            "tools",
            "effort",
            "thinking",
            "structuredOutput",
            "webSearch",
            "promptCacheControl",
            "nativePdf",
            "vision",
            "oneMillionContext",
            "taskBudgets",
            "contextWindow",
            "maxOutputTokens",
            "reasoningDialect",
            "geminiThinking",
        ];
        for model in [
            "qwen3.8-max",
            "glm-5.3",
            "glm-5.2",
            "deepseek-v4.1-flash",
            "deepseek-v4-pro",
            "kimi-k2.6",
        ] {
            let capabilities = capabilities_of(Some(&entry_of(model)), model);
            for field in capabilities.as_object().expect("object").keys() {
                assert!(
                    LEGAL.contains(&field.as_str()),
                    "{model}: `{field}` is not a ModelCapabilitiesInit field and would \
                     fail session startup with runtime codec UnknownField"
                );
            }
        }
        // And the specific field that caused it must be absent, on the model where
        // it is most tempting to declare.
        assert!(
            capabilities_of(Some(&entry_of("glm-5.3")), "glm-5.3")
                .get("canDisableThinking")
                .is_none(),
            "canDisableThinking must stay off the capability set"
        );
    }

    #[test]
    fn tools_are_not_declared_because_the_provider_ignores_a_per_model_flag() {
        // Every chat model here was measured driving tools, so an agent session
        // works — but the Anthropic provider answers `tools: true` unconditionally
        // and never reads a declared flag (only the OpenAI provider does). Emitting
        // one here would be dead weight, and emitting FALSE would be a control that
        // silently does nothing. Absence is the honest declaration.
        for model in ["qwen3.8-max", "glm-5.3", "deepseek-v4.1-flash"] {
            assert!(
                capabilities_of(Some(&entry_of(model)), model)
                    .get("tools")
                    .is_none(),
                "{model}: tools must not be declared where the provider ignores it"
            );
        }
    }

    #[test]
    fn structured_output_safety_follows_whether_thinking_can_be_turned_off() {
        // On a third-party Anthropic wire the engine cannot use native structured
        // output (`use_native_structured_output` gates on `is_first_party`, an
        // anti-abuse boundary), so every structured request goes through forced-tool
        // emulation, which puts `tool_choice` AND `thinking:{"type":"disabled"}` on
        // the wire. Measured against the live gateway with that exact body: every
        // model in this catalog accepts it EXCEPT glm-5.3, which answers
        // 400 `The value of the enable_thinking parameter is restricted to True`.
        //
        // That matters beyond structured output as a feature: session titles run on
        // the LOW tier as a structured request, so a glm-5.3 main model with an
        // unmapped low tier would silently lose the title of every conversation.
        assert!(
            !structured_output_safe("glm-5.3"),
            "glm-5.3 refuses the shape a structured request must send"
        );
        for model in [
            "qwen3.8-flash",
            "qwen3.8-max",
            "qwen3.7-plus",
            "qwen3.7-max",
            "qwen3.6-flash",
            "glm-5.2",
            "deepseek-v4-pro",
            "deepseek-v4.1-flash",
            "deepseek-v4-flash-0731",
        ] {
            assert!(
                structured_output_safe(model),
                "{model} was measured accepting it"
            );
        }
        // An unmeasured model is treated as safe: its facts default to
        // can_disable_thinking true, and refusing it here would pin the low tier
        // away from a model nobody measured failing.
        assert!(structured_output_safe("kimi-k2.6"));
    }

    #[test]
    fn unmeasured_capabilities_stay_absent() {
        let capabilities = capabilities_of(Some(&entry_of("qwen3.8-max")), "qwen3.8-max");
        for absent in ["nativePdf", "webSearch", "oneMillionContext"] {
            assert!(
                capabilities.get(absent).is_none(),
                "{absent} was never measured and must not be declared"
            );
        }
        assert_eq!(capabilities["promptCacheControl"], json!(true));
    }

    #[test]
    fn an_unknown_model_gets_conservative_defaults_rather_than_a_guess() {
        // A team-edition key lists models a personal key cannot see; one of them
        // arriving here must stay usable instead of being declared capable.
        let capabilities = capabilities_of(None, "kimi-k2.6");
        assert_eq!(capabilities["contextWindow"], json!(200_000));
        assert_eq!(capabilities["vision"], json!(false));
        assert!(
            capabilities.get("effort").is_none(),
            "no ladder without a measurement"
        );
    }

    #[test]
    fn an_unknown_key_falls_back_to_the_routing_model() {
        let models = vec![entry_of("qwen3.8-flash"), entry_of("qwen3.8-max")];
        assert_eq!(resolve_model_key(&models, "qwen3.8-max"), "qwen3.8-max");
        assert_eq!(resolve_model_key(&models, "retired-model"), "qwen3.8-max");
        assert_eq!(resolve_model_key(&[], "anything"), FALLBACK_MODEL);
    }

    #[test]
    fn the_head_prefers_the_fallback_model_over_catalog_order() {
        // The gateway lists whatever order it likes; a bare selection still lands
        // on the plan's flagship rather than on the first row that happened to
        // arrive.
        let models = vec![
            entry_of("qwen3.7-max"),
            entry_of("qwen3.8-flash"),
            entry_of("qwen3.8-max"),
            entry_of("glm-5.3"),
        ];
        assert_eq!(first_key_of(&models).as_deref(), Some("qwen3.8-max"));
        assert_eq!(resolve_model_key(&models, "no-such-model"), "qwen3.8-max");
        // With the fallback absent, the first listed entry heads it.
        let without_fallback = vec![entry_of("qwen3.7-max"), entry_of("glm-5.3")];
        assert_eq!(
            first_key_of(&without_fallback).as_deref(),
            Some("qwen3.7-max")
        );
    }

    fn entry_of(model: &str) -> Value {
        catalog_entry(&row(model))
            .unwrap_or_else(|| panic!("{model} should project to a catalog entry"))
    }
    #[test]
    fn an_unmeasured_catalog_row_does_not_advertise_native_reasoning_controls() {
        let entry = catalog_entry(&json!({"id": "unmeasured-future-model"})).expect("entry");
        assert!(entry.get("efforts").is_none());
        assert!(entry.get("defaultEffort").is_none());
        assert!(entry.get("canDisableThinking").is_none());
    }
}
