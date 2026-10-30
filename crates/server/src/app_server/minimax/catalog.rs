//! The MiniMax M Plan catalog and its declared capability matrix.
//!
//! M Plan is a subscription that spends one shared allowance across text, image,
//! audio and video. Its text models are served on the ordinary Anthropic wire
//! (`{base}/anthropic/v1/messages`) with the subscription key as `x-api-key`, so
//! the engine's own Anthropic provider is pointed straight at it — no signer, no
//! local bridge.
//!
//! The model LIST is read live from the OpenAI-compatible route (`/v1/models`),
//! because that is where the plan publishes it. But the live list has one gap this
//! module has to close: it omits `MiniMax-M3.1-Flash-Preview`, the headline model
//! every M Plan tier serves and the one MiniMax's own M Plan docs name as the
//! default. Measured, that model answers 200 on a real subscription key while
//! never appearing in either catalog route. A catalog read alone would hide the
//! plan's own flagship, so it is injected at the head of every projection.
//!
//! The CAPABILITIES beside the list are a declared table, and that is not
//! laziness: `/v1/models` returns `id`, `created`, `owned_by` and nothing else —
//! no context length, no effort ladder, no modality. Every value below was
//! measured against the live cn endpoint on a real subscription key on
//! 2026-10-08, and each row says what was measured. A model the table does not
//! know gets conservative defaults rather than a guess.

use serde_json::{json, Map, Value};
use std::sync::{Mutex, OnceLock};

const MODELS_PATH: &str = "/models";
const CATALOG_TTL_MS: i64 = 6 * 60 * 60 * 1000;
const FETCH_TIMEOUT_MS: u64 = 15_000;
/// A picker without a matching snapshot gives the live list this bounded window.
const WARM_TIMEOUT_MS: u64 = 1500;
/// The listing is paged; 100 is the largest page the OpenAI-style shape implies
/// and the current answer is a single page with `has_more: false`.
const PAGE_SIZE: i64 = 100;
const MAX_PAGES: u8 = 10;

/// What a bare selection names when nothing else can answer.
///
/// This is the plan's flagship text model, the one MiniMax's own M Plan preset
/// names and the only one whose effort knob was measured to actually move the
/// thinking budget (low→47, medium→30, high→28, max→52 thinking tokens on the
/// same prompt). It is injected into the catalog rather than read from it — see
/// the module note — so a bare selection always lands on a model the key serves.
const DEFAULT_SELECTION: &str = "MiniMax-M3.1-Flash-Preview";

/// The documented effort ladder. Measured accepted on `MiniMax-M3.1-Flash-Preview`
/// at every rung (`low`, `medium`, `high`, `xhigh`, `max`); `none` is refused with
/// 400 and `minimal` is undocumented, so neither is offered.
const FULL_LADDER: [&str; 5] = ["low", "medium", "high", "xhigh", "max"];

/// One measured model: context window, whether thinking can be turned off, whether
/// the forced-tool structured path returns a tool call, whether it reads images,
/// and which effort rungs it accepts.
struct ModelFacts {
    reasoning_known: bool,
    context_tokens: i64,
    /// Whether the picker may offer a "thinking off" rung. False when the model
    /// refuses `thinking:{"type":"disabled"}` outright with HTTP 400.
    can_disable_thinking: bool,
    /// Whether the forced-tool structured-output path actually returns a
    /// `tool_use` block.
    ///
    /// This is a SEPARATE question from [`Self::can_disable_thinking`], and the
    /// difference is the whole reason this table is not one flag. On a third-party
    /// Anthropic wire the engine cannot use native structured output
    /// (`use_native_structured_output` gates on `is_first_party`), so every
    /// structured request sends `tool_choice` together with
    /// `thinking:{"type":"disabled"}`. Measured against that exact body:
    ///
    /// * `MiniMax-M3.1-Flash-Preview` refuses `disabled` with 400 — it "requires
    ///   adaptive thinking" — so it cannot serve the path at all.
    /// * `MiniMax-M3` ACCEPTS `disabled` with 200 but then answers with a plain
    ///   text block and `stop_reason: end_turn`, emitting NO `tool_use`. That is
    ///   worse than a 400: it is an empty structured result, not an error.
    /// * every `MiniMax-M2.x` model accepts `disabled` AND emits the `tool_use`.
    ///
    /// Session titles run on the LOW tier as a structured request, so a main model
    /// that fails here silently loses the generated title of every conversation
    /// that uses it. Only the M2.x family is safe, which is why the low tier is
    /// pinned to one of them when the main model is M3 or M3.1.
    structured_safe: bool,
    vision: bool,
    /// `None` means no effort knob is declared.
    ladder: Option<&'static [&'static str]>,
}

/// The measured matrix.
///
/// Context windows are the service's own stated input ceiling. `MiniMax-M3.1-
/// Flash-Preview` and `MiniMax-M3` are 1M-token multimodal models; the M2.x
/// family is 204,800 tokens and text-only. Vision was measured by asking each
/// model to name the colour of an 8×8 solid-red PNG: M3.1 answered "Red" and M3
/// engaged with the image, while every M2.x model answered that it could not see
/// an image.
fn facts(model: &str) -> ModelFacts {
    match model {
        // Headline M Plan model. 1M context, native multimodal, adjustable
        // thinking depth. Measured: refuses `thinking:{"type":"disabled"}` with
        // `400 requires adaptive thinking`, so it cannot serve the forced-tool
        // structured path and cannot be offered a thinking-off rung. Its effort
        // ladder is the one measured to change the thinking budget, so it is the
        // only model here that declares one.
        "MiniMax-M3.1-Flash-Preview" => ModelFacts {
            reasoning_known: true,
            context_tokens: 1_000_000,
            can_disable_thinking: false,
            structured_safe: false,
            vision: true,
            ladder: Some(&FULL_LADDER),
        },
        // 1M-context multimodal frontier model. Measured: ACCEPTS
        // `thinking:{"type":"disabled"}` (so a thinking-off rung is safe) but then
        // returns text with no `tool_use` on the forced-tool path, so it is NOT
        // structured-safe. Effort is accepted with 200 but was not measured to
        // change behaviour, so no ladder is declared — offering a knob that does
        // nothing reads as a control that is broken.
        "MiniMax-M3" => ModelFacts {
            reasoning_known: true,
            context_tokens: 1_000_000,
            can_disable_thinking: true,
            structured_safe: false,
            vision: true,
            ladder: None,
        },
        // The M2.x family: 204,800-token text models. Every one measured accepting
        // `thinking:{"type":"disabled"}` AND emitting a `tool_use` on the
        // forced-tool path, so they are the structured-safe set the low tier pins
        // to. Vision was measured absent (each answered it could not see the
        // image). Effort was accepted but not measured to change behaviour, so no
        // ladder is declared.
        "MiniMax-M2.7"
        | "MiniMax-M2.7-highspeed"
        | "MiniMax-M2.5"
        | "MiniMax-M2.5-highspeed"
        | "MiniMax-M2.1"
        | "MiniMax-M2.1-highspeed"
        | "MiniMax-M2" => ModelFacts {
            reasoning_known: true,
            context_tokens: 204_800,
            can_disable_thinking: true,
            structured_safe: true,
            vision: false,
            ladder: None,
        },
        // An unknown model — the platform adds models over time and a higher tier
        // may serve more. Keep it usable with the safest reading: no effort knob
        // (an undeclared rung is clamped away, a wrongly declared one is refused
        // with 400), no vision, and NOT structured-safe so the low tier is pinned
        // to a measured M2.x rather than risking a silent title failure on a model
        // this table has not seen. Thinking controls remain undeclared until measured.
        _ => ModelFacts {
            reasoning_known: false,
            context_tokens: 204_800,
            can_disable_thinking: false,
            structured_safe: false,
            vision: false,
            ladder: None,
        },
    }
}

/// Why a catalog read failed, and which recovery applies.
///
/// `not_found` means there is nothing to list (no key stored), `login_expired`
/// means the key was refused so the host should send the user back to re-enter it.
#[derive(Debug, Clone)]
pub struct MinimaxError {
    pub message: String,
    pub not_found: bool,
    pub login_expired: bool,
}

impl std::fmt::Display for MinimaxError {
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

fn http_client() -> Result<reqwest::Client, MinimaxError> {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(FETCH_TIMEOUT_MS))
        .user_agent(super::auth::user_agent())
        .build()
        .map_err(|error| MinimaxError {
            message: error.to_string(),
            not_found: false,
            login_expired: false,
        })
}

/// Ensure the flagship model is present at the head of a projected list.
///
/// The live catalog omits `MiniMax-M3.1-Flash-Preview` even though every M Plan
/// tier serves it and it is the documented default. Without this, a bare
/// selection could not resolve to the plan's own headline model and the effort
/// knob would never appear, because the only model that declares a ladder would be
/// absent from the picker.
fn ensure_default_head(models: &mut Vec<Value>) {
    if models
        .iter()
        .any(|model| model.get("key").and_then(Value::as_str) == Some(DEFAULT_SELECTION))
    {
        // Present but maybe not first; move it to the head so a bare selection and
        // the picker's default row agree.
        if let Some(index) = models
            .iter()
            .position(|model| model.get("key").and_then(Value::as_str) == Some(DEFAULT_SELECTION))
        {
            let head = models.remove(index);
            models.insert(0, head);
        }
        return;
    }
    if let Some(entry) = catalog_entry(&json!({ "id": DEFAULT_SELECTION })) {
        models.insert(0, entry);
    }
}

/// Read every page the catalog serves.
///
/// Paging is honoured rather than assumed away: the answer carries `has_more` and
/// a cursor, and silently taking one page would drop models the moment the plan
/// grows past it. A dropped model is invisible to the user — the picker just stops
/// offering it — so the loop is worth the few lines.
async fn fetch_live_models() -> Result<Vec<Value>, MinimaxError> {
    let key = super::auth::api_key().ok_or_else(|| MinimaxError {
        // Prose, not a stable code: this message is what the settings tier card
        // renders when a catalog read fails, and a code would print as a code.
        message: "not signed in to a MiniMax M Plan".into(),
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
        let response = request.send().await.map_err(|error| MinimaxError {
            message: error.to_string(),
            not_found: false,
            login_expired: false,
        })?;
        let status_code = response.status().as_u16();
        let body = response.text().await.unwrap_or_default();
        if status_code == 401 || status_code == 403 {
            return Err(MinimaxError {
                message: "minimax_key_rejected".into(),
                not_found: false,
                login_expired: true,
            });
        }
        if !(200..300).contains(&status_code) {
            let detail: String = body.chars().take(200).collect();
            return Err(MinimaxError {
                message: format!("the MiniMax catalog answered {status_code}: {detail}"),
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
        // The OpenAI-compatible listing answers `data` with no `last_id`/`has_more`
        // in the measured response, but the paged shape is honoured in case a
        // future answer carries them.
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
    ensure_default_head(&mut models);
    if models.is_empty() {
        return Err(MinimaxError {
            message: "the MiniMax catalog listed no usable model".into(),
            not_found: true,
            login_expired: false,
        });
    }
    Ok(models)
}

/// Project one catalog row onto the entry the picker and capability reader use.
fn catalog_entry(model: &Value) -> Option<Value> {
    let key = model
        .get("id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
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
    // `structuredSafe` rides on the ENTRY, not the capability set: the engine's
    // capability decoder is strict and rejects an unknown field, which would fail
    // session startup. The host reads it only to decide the low-tier pin; the
    // picker does not surface it.
    entry.insert("structuredSafe".into(), json!(facts.structured_safe));
    if let Some(ladder) = facts.ladder {
        entry.insert("efforts".into(), json!(ladder));
        // Every rung was measured accepted and the top one is the documented
        // default, so the ladder's tail is a sensible default rather than an
        // invented one.
        entry.insert("defaultEffort".into(), json!(ladder[ladder.len() - 1]));
    }
    super::super::conversation_catalog::mark_purpose("minimax", model, &mut entry);
    Some(Value::Object(entry))
}

fn owned_catalog_path() -> std::path::PathBuf {
    super::config_root().join("minimax-mplan-catalog.json")
}

fn read_local_models() -> Vec<Value> {
    let Some(fingerprint) = super::auth::catalog_fingerprint() else {
        return Vec::new();
    };
    std::fs::read_to_string(owned_catalog_path())
        .ok()
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
        .filter(|value| {
            value.get("fingerprint").and_then(Value::as_str) == Some(fingerprint.as_str())
        })
        .and_then(|value| value.get("models").and_then(Value::as_array).cloned())
        .unwrap_or_default()
}

fn write_owned_catalog(models: &[Value], fingerprint: &str) {
    let path = owned_catalog_path();
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Ok(serialized) =
        serde_json::to_string(&json!({ "models": models, "fingerprint": fingerprint }))
    {
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
/// instead of whatever the network answered. Same seam the Bailian catalog exposes.
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
pub async fn fetch_models() -> Result<Vec<Value>, MinimaxError> {
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
                Err(error) => eprintln!("[MiniMax] catalog refresh failed: {}", error.message),
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
pub async fn fetch_models_warm() -> Result<Vec<Value>, MinimaxError> {
    if super::auth::api_key().is_none() {
        return Err(MinimaxError {
            message: "not signed in to a MiniMax M Plan".into(),
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
                Err(error) => eprintln!("[MiniMax] catalog refresh failed: {}", error.message),
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
                Err(MinimaxError {
                    message: "the MiniMax catalog did not answer in time".into(),
                    not_found: false,
                    login_expired: false,
                })
            } else {
                Ok(local)
            }
        }
    }
}

/// The catalog head: what a bare selection resolves to.
///
/// Prefers the flagship model, which [`ensure_default_head`] keeps at the head, so
/// a bare selection lands on the model the plan is sold on.
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
        .find(|model| model.get("key").and_then(Value::as_str) == Some(DEFAULT_SELECTION))
        .or_else(|| models.first())
        .and_then(|model| model.get("key"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// The catalog key a requested key actually maps to.
///
/// An unknown key falls back to the head rather than going to the wire as-is: the
/// service answers an unlisted model with `400 model not exist`, and a stale
/// selection should land on something that works.
pub fn resolve_model_key(models: &[Value], requested: &str) -> String {
    if models
        .iter()
        .any(|model| model.get("key").and_then(Value::as_str) == Some(requested))
    {
        return requested.to_string();
    }
    first_key_of(models).unwrap_or_else(|| DEFAULT_SELECTION.to_string())
}

fn first_key_of(models: &[Value]) -> Option<String> {
    models
        .iter()
        .find(|model| model.get("key").and_then(Value::as_str) == Some(DEFAULT_SELECTION))
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
        .unwrap_or(204_800)
}

/// Whether a model can serve the engine's structured-output path.
///
/// On a third-party Anthropic wire the provider cannot use NATIVE structured
/// output — `use_native_structured_output` gates on `is_first_party`, which is an
/// anti-abuse boundary rather than a knob — so every structured request goes
/// through forced-tool emulation instead, and that path puts `tool_choice` on the
/// wire together with `thinking:{"type":"disabled"}`.
///
/// Measured against the live gateway with the exact body the engine builds, only
/// the M2.x family accepts that combination AND returns a `tool_use`. The two
/// frontier models fail differently: M3.1 refuses `disabled` with 400, and M3
/// accepts it with 200 but emits plain text and no `tool_use`. Both are captured by
/// [`ModelFacts::structured_safe`] rather than by `can_disable_thinking`, which is
/// true for M3 and would wrongly clear it.
///
/// This matters beyond structured output as a feature: session titles run on the
/// LOW tier as a structured request, so a main model that fails here silently loses
/// the generated title of every conversation that uses it.
pub fn structured_output_safe(key: &str) -> bool {
    facts(key).structured_safe
}

/// A structured-safe model to pin an unmapped low tier to.
///
/// Prefers the catalog head when that head is itself safe, so the pin is the same
/// model a bare selection already lands on; otherwise the first safe model the plan
/// serves. Because the flagship head is NOT safe here, this resolves to the first
/// M2.x model in the catalog. `None` only when the catalog offers no safe model at
/// all, in which case the caller leaves the tier unmapped and the engine falls back
/// to the main model as it would have anyway.
pub async fn structured_safe_fallback() -> Option<String> {
    let models = fetch_models().await.ok()?;
    let keys: Vec<String> = models
        .iter()
        .filter_map(|model| model.get("key").and_then(Value::as_str).map(str::to_string))
        .collect();
    keys.iter()
        .find(|key| key.as_str() == DEFAULT_SELECTION && structured_output_safe(key))
        .or_else(|| keys.iter().find(|key| structured_output_safe(key)))
        .cloned()
}

/// What a MiniMax model can do, from the measured matrix.
///
/// Every field is something measured against the live endpoint. Two notes on what
/// is deliberately absent:
///
/// * `structuredOutput` is false for every model. Native structured output is
///   gated on `is_first_party`, which this third-party wire is not, so the engine
///   never uses it here; declaring it true would imply a path that cannot run.
/// * `thinking.adaptive` is true and `budget_tokens` is never used. Measured,
///   `thinking:{"type":"adaptive"}` is accepted on every model, and the budget
///   shape (`{"type":"enabled","budget_tokens":N}`) is accepted too but the engine
///   prefers adaptive on a wire that declares it.
///
/// Absent on purpose: `nativePdf` and `webSearch`, neither of which was measured,
/// and `oneMillionContext`, because the 1M window here comes from the service's own
/// stated ceiling and is carried through `contextWindow` rather than the beta flag
/// (which would attach a beta header the endpoint was not measured to need).
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
        // Prompt-cache breakpoints were measured live: repeat calls reported
        // `cache_read_input_tokens` rather than rejecting a `cache_control` block.
        "promptCacheControl": true,
        // Native structured output is never used on a third-party wire (see the
        // function note), so this is false for every model. The forced-tool
        // emulation the engine falls back to is what `structured_output_safe`
        // measures separately.
        "structuredOutput": false,
        // `adaptive` only. Every model accepts `thinking:{"type":"adaptive"}`.
        "thinking": {
            "adaptive": true,
            "interleaved": false,
            "canDisableThinking": facts.can_disable_thinking,
        },
    });
    // `tools` is deliberately NOT declared, even though every chat model here was
    // measured driving tools (the forced-tool probe emitted a `tool_use` and a
    // plain read_file probe emitted one too). The Anthropic provider treats tool
    // support as a provider-level truth and answers `tools: true` unconditionally,
    // never reading a per-model flag; only the OpenAI provider reads one. Declaring
    // it here would be a field that cannot change behaviour.
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
    // `structuredSafe` is deliberately NOT carried into
    // the capability set: the engine's capability decoder is strict and rejects an
    // unknown field, which fails session startup outright. They stay on the catalog
    // ENTRY, where the host reads `canDisableThinking` to decide whether the picker
    // offers an off rung and the engine reads `structuredSafe` to decide the
    // low-tier pin.
    capabilities
}

#[cfg(test)]
mod tests {
    use super::*;
    crate::app_server::catalog_test_support::warm_catalog_tests!("minimax", read_local_models);

    /// One catalog row as the live endpoint returns it: id and bookkeeping only.
    fn row(id: &str) -> Value {
        json!({ "id": id, "object": "model", "owned_by": "minimax", "created": 1780272000 })
    }

    #[test]
    fn the_flagship_is_injected_even_though_the_live_catalog_omits_it() {
        // The measured `/v1/models` answer lists eight M2.x/M3 models and never
        // MiniMax-M3.1-Flash-Preview, yet that model answers 200 on the same key.
        // A catalog read alone would hide the plan's headline model and its effort
        // knob, so the projection always carries it at the head.
        let mut models: Vec<Value> = ["MiniMax-M3", "MiniMax-M2.5"]
            .iter()
            .filter_map(|id| catalog_entry(&row(id)))
            .collect();
        ensure_default_head(&mut models);
        assert_eq!(
            models[0].get("key").and_then(Value::as_str),
            Some(DEFAULT_SELECTION)
        );
        // Injecting twice does not duplicate it.
        ensure_default_head(&mut models);
        let count = models
            .iter()
            .filter(|model| model.get("key").and_then(Value::as_str) == Some(DEFAULT_SELECTION))
            .count();
        assert_eq!(count, 1);
    }

    #[test]
    fn structured_safety_is_not_the_same_flag_as_can_disable_thinking() {
        // M3 is the case that separates the two: it accepts `thinking:disabled`
        // (so a thinking-off rung is safe) but returns no `tool_use` on the
        // forced-tool path (so it cannot serve a structured request). Collapsing
        // these into one flag, as a single `can_disable_thinking` would, clears M3
        // for the low tier and silently loses every conversation's title.
        assert!(facts("MiniMax-M3").can_disable_thinking);
        assert!(!facts("MiniMax-M3").structured_safe);
        // M3.1 refuses disabled outright, so it is neither.
        assert!(!facts("MiniMax-M3.1-Flash-Preview").can_disable_thinking);
        assert!(!facts("MiniMax-M3.1-Flash-Preview").structured_safe);
        // The M2.x family is the structured-safe set.
        for model in [
            "MiniMax-M2.7",
            "MiniMax-M2.7-highspeed",
            "MiniMax-M2.5",
            "MiniMax-M2.5-highspeed",
            "MiniMax-M2.1",
            "MiniMax-M2.1-highspeed",
            "MiniMax-M2",
        ] {
            assert!(
                structured_output_safe(model),
                "{model} must be structured-safe"
            );
            assert!(facts(model).can_disable_thinking);
        }
    }

    #[test]
    fn an_unknown_model_is_not_assumed_structured_safe() {
        // A model this table has not seen must not be trusted to serve the title
        // path; pinning the low tier away from it is the safe reading.
        assert!(!structured_output_safe("MiniMax-M9-hypothetical"));
        assert!(!facts("MiniMax-M9-hypothetical").can_disable_thinking);
    }

    #[test]
    fn only_the_flagship_declares_an_effort_ladder() {
        // Measured: the effort knob visibly changes M3.1's thinking budget, but was
        // not measured to change M3 or the M2.x models, so offering it there would
        // be a control that does nothing.
        assert_eq!(
            facts("MiniMax-M3.1-Flash-Preview").ladder,
            Some(&FULL_LADDER[..])
        );
        assert!(facts("MiniMax-M3").ladder.is_none());
        assert!(facts("MiniMax-M2.5").ladder.is_none());
    }

    #[test]
    fn the_entry_carries_the_host_flags_but_the_capabilities_do_not() {
        let entry = catalog_entry(&row("MiniMax-M3.1-Flash-Preview")).expect("entry");
        assert_eq!(entry["canDisableThinking"], json!(false));
        assert_eq!(entry["structuredSafe"], json!(false));
        assert_eq!(entry["contextTokens"], json!(1_000_000));
        assert_eq!(entry["is_vl"], json!(true));
        assert_eq!(entry["efforts"], json!(FULL_LADDER));
        assert_eq!(entry["defaultEffort"], json!("max"));

        let caps = capabilities_of(Some(&entry), "MiniMax-M3.1-Flash-Preview");
        // A strict capability decoder rejects unknown fields, so these must NOT
        // leak into the capability set even though they ride on the entry.
        assert!(caps.get("canDisableThinking").is_none());
        assert!(caps.get("structuredSafe").is_none());
        assert_eq!(caps["vision"], json!(true));
        assert_eq!(caps["contextWindow"], json!(1_000_000));
        assert_eq!(caps["structuredOutput"], json!(false));
        assert_eq!(caps["thinking"]["adaptive"], json!(true));
        assert_eq!(caps["effort"]["levels"], json!(FULL_LADDER));
        assert_eq!(caps["effort"]["default"], json!("max"));
    }

    #[test]
    fn a_row_without_an_id_is_not_a_model() {
        assert!(catalog_entry(&json!({ "object": "model" })).is_none());
        assert!(catalog_entry(&json!({ "id": "  " })).is_none());
    }

    #[tokio::test]
    async fn the_low_tier_pins_to_a_structured_safe_model_not_the_flagship() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        forget_cache();
        crate::app_server::catalog_test_support::store_credential("minimax", temp.path(), "tier");
        let _gateway = crate::app_server::catalog_test_support::Gateway::start(
            "minimax",
            std::time::Duration::from_secs(5),
        )
        .await;
        // A catalog shaped like the live answer: flagship injected at the head,
        // M3 next, then the M2.x family. The head is NOT structured-safe, so the
        // fallback must reach past it into the M2.x set.
        let mut models: Vec<Value> = ["MiniMax-M3", "MiniMax-M2.7", "MiniMax-M2.5"]
            .iter()
            .filter_map(|id| catalog_entry(&row(id)))
            .collect();
        ensure_default_head(&mut models);
        seed_owned_catalog(&models);
        // The matching disk snapshot answers before the delayed live response,
        // exercising the real fallback function against a fixed catalog.
        let fallback = structured_safe_fallback().await;
        assert_eq!(fallback.as_deref(), Some("MiniMax-M2.7"));
        assert!(!structured_output_safe(DEFAULT_SELECTION));
        // And the head it must reach past really is the flagship.
        assert_eq!(
            models[0].get("key").and_then(Value::as_str),
            Some(DEFAULT_SELECTION)
        );
    }
    #[test]
    fn an_unmeasured_catalog_row_does_not_advertise_native_reasoning_controls() {
        let entry = catalog_entry(&json!({"id": "unmeasured-future-model"})).expect("entry");
        assert!(entry.get("efforts").is_none());
        assert!(entry.get("defaultEffort").is_none());
        assert!(entry.get("canDisableThinking").is_none());
    }
}
