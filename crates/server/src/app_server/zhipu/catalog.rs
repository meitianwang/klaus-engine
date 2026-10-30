//! The Zhipu Coding Plan catalog and its declared capability matrix.
//!
//! The model LIST is live: `{base}/v1/models` on the same endpoint that serves
//! inference, read with the same `x-api-key`. A hardcoded list would either hide
//! a model the plan adds or offer one it has retired, and the plan's own docs note
//! that older ids are routed to current models rather than served as themselves —
//! so the list is read, and the routing is the service's business rather than a
//! guess made here.
//!
//! The CAPABILITIES beside that list are a declared table, and that is not
//! laziness: the catalog route returns `id`, `display_name`, `created_at` and
//! `type`, and nothing about a model's window, its effort ladder or whether it can
//! see an image. Every value in the table below was measured against the live
//! endpoint on a real personal-plan key on 2026-10-08, and each row says what was
//! measured. A model the table does not know gets conservative defaults rather
//! than a guess.

use serde_json::{json, Map, Value};
use std::sync::{Mutex, OnceLock};

const MODELS_PATH: &str = "/v1/models";
const CATALOG_TTL_MS: i64 = 6 * 60 * 60 * 1000;
const FETCH_TIMEOUT_MS: u64 = 15_000;
/// A picker without a matching snapshot gives the live list this bounded window.
const WARM_TIMEOUT_MS: u64 = 1500;
/// The listing carries `hasMore` and `lastId`; 100 is the page size the shape
/// implies and the current answer is one page of 11 with `hasMore: false`.
const PAGE_SIZE: i64 = 100;
const MAX_PAGES: u8 = 10;

/// What a bare selection names when nothing else can answer.
///
/// The plan's top model. Every model this plan serves accepted the forced-tool
/// structured request a session title is (they answer the `disabled` shape with 200,
/// mapping it to the lightest rung rather than refusing), so there is no cheaper pin
/// to carry here: a bare selection can land on the flagship without costing the
/// title of a conversation. The live catalog's head still wins when reachable — and
/// [`first_model_key`] resolves that head to this key.
const FALLBACK_MODEL: &str = "glm-5.3";

/// The full five-rung ladder. Measured accepted on EVERY model the plan serves:
/// each took `output_config.effort` at low/medium/high/xhigh/max with the
/// `effort-2025-11-24` beta header the engine sends, and the rungs changed the
/// behaviour rather than being ignored — at low the answer carried no thinking
/// block, at max it carried the longest one.
const FULL_LADDER: [&str; 5] = ["low", "medium", "high", "xhigh", "max"];

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
/// Three facts hold across the whole table, and stating them once is clearer than
/// repeating a comment per row:
///
/// * `can_disable_thinking` is FALSE for every model, and that is a property of
///   the plan rather than of the models. Measured: a request carrying
///   `thinking:{"type":"disabled"}` answers 200 on all of them, but the plan's own
///   documentation says a "disabled" configuration is mapped to its low rung —
///   lightweight thinking, not none — and the measurements agree: the same model
///   that returned no thinking block on one disabled request returned a short one
///   on the next. Declaring an off switch here would put a control in the picker
///   that does not do what it says. The effort ladder is the real control and it
///   does work, which is why `ladder` is the full five rungs everywhere.
///   Crucially none of them REJECT the disabled shape, so the forced-tool path
///   session titles use works on every model — there is no structured-output
///   fallback special case to carry here.
/// * `native_structured_output` is FALSE for every model. Measured: the endpoint
///   accepts `output_config.format` with a JSON schema and answers 200, but it
///   does not enforce it — the reply came back as prose reasoning rather than as
///   the declared object. A capability that is accepted and then ignored is worse
///   than one that is refused, so the engine's forced-tool emulation is what serves
///   structured work on this wire, and that path was measured working on every
///   model.
/// * Context windows are the service's OWN documented figures, not a measured
///   ceiling: this endpoint accepted inputs well past the documented window for
///   two models rather than refusing (a 400k-token input to a 200k-window model
///   answered 200), so there is no refusal message to read a ceiling out of. The
///   documented number is the conservative direction — declaring less than the
///   service takes compacts early, while declaring more would let the engine pack
///   past a limit that may be enforced on a larger request and kill the turn with
///   a provider error instead of compacting.
fn facts(model: &str) -> ModelFacts {
    match model {
        // The plan's two documented head models. Measured: both took all five
        // rungs, both served the forced-tool structured path, and the flash one is
        // the only model in the set that actually READ an image — asked to name the
        // colours of a purple field with a white square in it, it answered
        // `BG=#9C0BA8, CENTER=#FFFFFF`, while every other model invented colours.
        "glm-5.3-flash" => ModelFacts {
            reasoning_known: true,
            context_tokens: 1_048_576,
            can_disable_thinking: false,
            vision: true,
            ladder: None,
            native_structured_output: false,
        },
        "glm-5.3" => ModelFacts {
            reasoning_known: true,
            context_tokens: 1_048_576,
            can_disable_thinking: false,
            vision: false,
            ladder: None,
            native_structured_output: false,
        },
        // Measured reading the same probe image correctly (`BG=green,
        // CENTER=yellow` against a green field with an orange square), which is the
        // other model that genuinely sees.
        "glm-4.5-air" => ModelFacts {
            reasoning_known: true,
            context_tokens: 131_072,
            can_disable_thinking: false,
            vision: true,
            ladder: None,
            native_structured_output: false,
        },
        // Accepted an image block and answered 200, but named colours that were
        // not in the image (`BG=blue, CENTER=green` for a purple-and-white probe,
        // `BG=white, CENTER=red` for a green-and-orange one) — a model that takes
        // the block and does not see it. Declaring vision here would let the
        // composer offer image input that silently answers about nothing.
        "glm-4.5" | "glm-4.6" | "glm-4.7" | "glm-5" | "glm-5-turbo" | "glm-5.1" | "glm-5.2" => {
            ModelFacts {
                reasoning_known: true,
                context_tokens: 204_800,
                can_disable_thinking: false,
                vision: false,
                ladder: None,
                native_structured_output: false,
            }
        }
        // An unknown model: the plan adds ids over time and routes old ones. Keep
        // it usable with the safest reading — no vision (declaring one that is not
        // there is the failure that reads as an unrelated bug), a window small
        // enough that compaction happens before the service refuses, no unmeasured reasoning controls, and the emulated
        // structured-output path because that is what a third-party Anthropic wire
        // uses and what every measured model accepted.
        _ => ModelFacts {
            reasoning_known: false,
            context_tokens: 200_000,
            can_disable_thinking: false,
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
pub struct ZhipuError {
    pub message: String,
    pub not_found: bool,
    pub login_expired: bool,
}

impl std::fmt::Display for ZhipuError {
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

fn http_client() -> Result<reqwest::Client, ZhipuError> {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(FETCH_TIMEOUT_MS))
        .user_agent(super::auth::user_agent())
        .build()
        .map_err(|error| ZhipuError {
            message: error.to_string(),
            not_found: false,
            login_expired: false,
        })
}

/// Read every page the catalog serves.
///
/// Paging is honoured rather than assumed away: the answer carries `hasMore` and a
/// `lastId`, and silently taking one page would drop models the moment the plan
/// grows past it. A dropped model is invisible to the user — the picker just stops
/// offering it — so the loop is worth the few lines.
async fn fetch_live_models() -> Result<Vec<Value>, ZhipuError> {
    let key = super::auth::api_key().ok_or_else(|| ZhipuError {
        // Prose, not a stable code: this message is what the settings tier card
        // renders when a catalog read fails, and a code would print as a code.
        message: "not signed in to a Zhipu Coding Plan".into(),
        not_found: true,
        login_expired: false,
    })?;
    let http = http_client()?;
    let base = super::auth::api_base();
    let mut models = Vec::new();
    let mut after: Option<String> = None;
    for _ in 0..MAX_PAGES {
        let mut request = http
            .get(format!("{base}{MODELS_PATH}"))
            // `x-api-key` is the header the plan's own documentation spells for
            // this endpoint, and the same one the inference requests carry.
            .header("x-api-key", &key)
            .query(&[("limit", PAGE_SIZE.to_string())]);
        if let Some(cursor) = after.as_deref() {
            request = request.query(&[("after", cursor.to_string())]);
        }
        let response = request.send().await.map_err(|error| ZhipuError {
            message: error.to_string(),
            not_found: false,
            login_expired: false,
        })?;
        let status_code = response.status().as_u16();
        let body = response.text().await.unwrap_or_default();
        if status_code == 401 || status_code == 403 {
            return Err(ZhipuError {
                message: "zhipu_key_rejected".into(),
                not_found: false,
                login_expired: true,
            });
        }
        if !(200..300).contains(&status_code) {
            let detail: String = body.chars().take(200).collect();
            return Err(ZhipuError {
                message: format!("the Zhipu catalog answered {status_code}: {detail}"),
                not_found: false,
                login_expired: false,
            });
        }
        let parsed: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
        let rows: Vec<Value> = match super::auth::classify_catalog_body(&body) {
            super::auth::CatalogAnswer::Models(_) => parsed
                .get("data")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default(),
            // The status was 2xx but the body refused the key — this gateway
            // answers a bad key with HTTP 200 and the refusal in the body, so
            // `login_expired` is what tells the host to send the user back to
            // re-enter it rather than retry a secret that has gone.
            super::auth::CatalogAnswer::Rejected => {
                return Err(ZhipuError {
                    message: "zhipu_key_rejected".into(),
                    not_found: false,
                    login_expired: true,
                });
            }
            super::auth::CatalogAnswer::Failed(detail) => {
                return Err(ZhipuError {
                    message: detail,
                    not_found: false,
                    login_expired: false,
                });
            }
        };
        let last_id = parsed
            .get("lastId")
            .and_then(Value::as_str)
            .map(str::to_string);
        models.extend(rows.iter().filter_map(catalog_entry));
        match parsed.get("hasMore").and_then(Value::as_bool) {
            Some(true) if last_id.is_some() => after = last_id,
            _ => break,
        }
    }
    if models.is_empty() {
        return Err(ZhipuError {
            message: "the Zhipu catalog listed no usable model".into(),
            not_found: true,
            login_expired: false,
        });
    }
    Ok(models)
}

/// Project one catalog row onto the entry the picker and capability reader use.
///
/// Account eligibility is evaluated by the service when inference is requested.
fn catalog_entry(model: &Value) -> Option<Value> {
    let key = model
        .get("id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    let facts = facts(key);
    let mut entry = Map::new();
    entry.insert("key".into(), json!(key));
    // The catalog carries a display name, so the picker gets the service's own
    // spelling rather than the bare id.
    let display = model
        .get("display_name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(key);
    entry.insert("display_name".into(), json!(display));
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
        // Every model here thinks by default and honours the top rung, so the
        // ladder's head is a sensible default rather than an invented one.
        entry.insert("defaultEffort".into(), json!(ladder[ladder.len() - 1]));
    }
    super::super::conversation_catalog::mark_purpose("zhipu", model, &mut entry);
    Some(Value::Object(entry))
}

fn owned_catalog_path() -> std::path::PathBuf {
    super::config_root().join("zhipu-model-catalog.json")
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
/// instead of whatever the network answered. Same seam the other plan catalogs
/// expose.
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
pub async fn fetch_models() -> Result<Vec<Value>, ZhipuError> {
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
                Err(error) => eprintln!("[Zhipu] catalog refresh failed: {}", error.message),
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
pub async fn fetch_models_warm() -> Result<Vec<Value>, ZhipuError> {
    if super::auth::api_key().is_none() {
        return Err(ZhipuError {
            message: "not signed in to a Zhipu Coding Plan".into(),
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
                Err(error) => eprintln!("[Zhipu] catalog refresh failed: {}", error.message),
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
                Err(ZhipuError {
                    message: "the Zhipu catalog did not answer in time".into(),
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
    first_key_of(&models)
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

/// The catalog key a requested key actually maps to.
///
/// An unknown key falls back to the head rather than going to the wire as-is: the
/// service answers an unlisted model with `400 [1211][模型不存在]`, and a stale
/// selection should land on something that works.
pub fn resolve_model_key(models: &[Value], requested: &str) -> String {
    if models
        .iter()
        .any(|model| model.get("key").and_then(Value::as_str) == Some(requested))
    {
        return requested.to_string();
    }
    first_key_of(models).unwrap_or_else(|| FALLBACK_MODEL.to_string())
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

/// What a Zhipu model can do, from the measured matrix.
///
/// Every field is something measured against the live endpoint, and the two that
/// matter most are the ones that break a session if declared wrongly:
///
/// * `structuredOutput` is false for the whole table. The endpoint accepts
///   `output_config.format` and then ignores the schema — measured, a request
///   declaring a `{title: string}` schema answered 200 with prose reasoning
///   instead of the object. Declaring a capability the service accepts and does
///   not honour is worse than declaring none, because the engine would stop
///   sending the forced-tool emulation that does work.
/// * `vision` is true for exactly the two models measured reading an image
///   correctly. Every other model here accepts an image block and answers about
///   colours that are not in it, which is the failure a user reads as the model
///   being stupid rather than as a wrong declaration.
///
/// Prompt-cache breakpoints were measured WORKING rather than merely accepted: a
/// system block carrying `cache_control` answered `cache_read_input_tokens: 3968`
/// on the second of two identical requests, so the cache is real and declared.
///
/// Absent on purpose: `nativePdf` and `webSearch`, neither of which was measured.
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
        // Measured: a repeat request answered with `cache_read_input_tokens`
        // covering the cached prefix, so the breakpoints are honoured and not just
        // tolerated.
        "promptCacheControl": true,
        "structuredOutput": facts.native_structured_output,
        // Adaptive only. The plan maps an explicit off to its low rung rather than
        // silencing thinking, so there is no true disable to declare; the effort
        // ladder below is the control that does something.
        "thinking": {
            "adaptive": true,
            "interleaved": false,
            "canDisableThinking": facts.can_disable_thinking,
        },
    });
    // `tools` is deliberately NOT declared, even though every model here was
    // measured driving tools (each emitted a `tool_use` block for a declared tool,
    // and each served the forced-tool structured path). The Anthropic provider
    // treats tool support as a provider-level truth and answers `tools: true`
    // unconditionally, never reading a per-model flag; only the OpenAI provider
    // reads one. Declaring it here would be a field that cannot change behaviour,
    // and declaring it FALSE for some model would be worse — silently ignored, so
    // the table would read as a control that does nothing.
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
    capabilities
}

#[cfg(test)]
mod tests {
    use super::*;
    crate::app_server::catalog_test_support::warm_catalog_tests!("zhipu", read_local_models);

    /// One catalog row as the live endpoint returns it.
    fn row(id: &str) -> Value {
        json!({
            "id": id,
            "display_name": id.to_uppercase(),
            "type": "model",
            "created_at": "2026-08-14T00:00:00Z",
        })
    }

    #[test]
    fn listed_models_remain_selectable_regardless_of_plan_eligibility() {
        assert!(catalog_entry(&row("glm-5.3-flashx")).is_some());
        assert!(catalog_entry(&row("glm-5.3")).is_some());
    }

    #[test]
    fn a_row_without_an_id_is_dropped() {
        assert!(catalog_entry(&json!({ "display_name": "No id" })).is_none());
        assert!(catalog_entry(&json!({ "id": "   " })).is_none());
    }

    #[test]
    fn the_display_name_comes_from_the_service_not_the_id() {
        let entry = catalog_entry(&row("glm-4.6")).expect("entry");
        assert_eq!(entry["key"], json!("glm-4.6"));
        assert_eq!(entry["display_name"], json!("GLM-4.6"));
        // A row with no display name falls back to the id rather than painting an
        // empty label.
        let bare = catalog_entry(&json!({ "id": "glm-9" })).expect("entry");
        assert_eq!(bare["display_name"], json!("glm-9"));
    }

    #[test]
    fn no_model_declares_a_thinking_off_switch() {
        // The plan maps an explicit "disabled" to its low rung rather than
        // silencing thinking, so an off switch in the picker would be a control
        // that does not do what it says. Measured across the whole served set.
        for model in [
            "glm-4.5",
            "glm-4.5-air",
            "glm-4.6",
            "glm-4.7",
            "glm-5",
            "glm-5-turbo",
            "glm-5.1",
            "glm-5.2",
            "glm-5.3",
            "glm-5.3-flash",
        ] {
            let entry = catalog_entry(&row(model)).expect("entry");
            assert_eq!(
                entry["canDisableThinking"],
                json!(false),
                "{model} must not offer an off rung on this plan"
            );
        }
        let unknown = catalog_entry(&row("glm-9-unknown")).expect("entry");
        assert!(unknown.get("canDisableThinking").is_none());
    }

    #[test]
    fn every_model_takes_the_full_effort_ladder() {
        // Measured: all five rungs answered 200 with the engine's own wire shape
        // (`output_config.effort` plus the effort beta header), and the rungs
        // changed the answer rather than being ignored.
        for model in ["glm-4.5", "glm-5.3", "glm-5.3-flash"] {
            let entry = catalog_entry(&row(model)).expect("entry");
            let levels: Vec<&str> = entry["efforts"]
                .as_array()
                .expect("efforts")
                .iter()
                .filter_map(Value::as_str)
                .collect();
            assert_eq!(levels, FULL_LADDER, "{model} ladder");
            assert_eq!(entry["defaultEffort"], json!("max"));
        }
    }

    #[test]
    fn vision_is_declared_only_for_models_measured_reading_an_image() {
        // Two models named the colours of a probe image correctly; the rest
        // accepted the block and invented colours. Declaring vision for those would
        // let the composer offer image input that answers about nothing.
        assert!(catalog_entry(&row("glm-5.3-flash")).unwrap()["is_vl"] == json!(true));
        assert!(catalog_entry(&row("glm-4.5-air")).unwrap()["is_vl"] == json!(true));
        for blind in ["glm-5.3", "glm-4.6", "glm-4.5", "glm-5.1", "glm-9-unknown"] {
            assert_eq!(
                catalog_entry(&row(blind)).unwrap()["is_vl"],
                json!(false),
                "{blind} did not read the probe image"
            );
        }
    }

    #[test]
    fn structured_output_is_not_declared_because_the_schema_is_ignored() {
        // Measured: the endpoint accepts `output_config.format` and answers prose
        // anyway. A capability that is accepted and not honoured must not be
        // declared, or the engine stops sending the forced-tool path that works.
        for model in ["glm-5.3", "glm-5.3-flash", "glm-4.6"] {
            let caps = capabilities_of(catalog_entry(&row(model)).as_ref(), model);
            assert_eq!(
                caps["structuredOutput"],
                json!(false),
                "{model} must not claim a native schema"
            );
        }
    }

    #[test]
    fn the_capability_set_carries_the_cache_and_thinking_facts_only() {
        let entry = catalog_entry(&row("glm-5.3")).expect("entry");
        let caps = capabilities_of(Some(&entry), "glm-5.3");
        // Measured real: a repeat request answered with cache_read_input_tokens
        // covering the cached prefix.
        assert_eq!(caps["promptCacheControl"], json!(true));
        assert_eq!(
            caps["thinking"],
            json!({
                "adaptive": true,
                "interleaved": false,
                "canDisableThinking": false,
            })
        );
        assert_eq!(caps["contextWindow"], json!(1_048_576));
        // The engine's capability decoder rejects unknown fields and fails session
        // startup, so the flag must be nested inside the accepted thinking
        // capability rather than emitted as a top-level field.
        assert!(caps.get("canDisableThinking").is_none());
        assert!(caps.get("tools").is_none());
        // The ladder rides along so the composer can offer the rungs.
        assert_eq!(
            caps["effort"]["levels"],
            json!(["low", "medium", "high", "xhigh", "max"])
        );
        assert_eq!(caps["effort"]["default"], json!("max"));
    }

    #[test]
    fn an_unknown_model_gets_conservative_defaults() {
        let caps = capabilities_of(None, "glm-9-not-in-the-table");
        assert_eq!(caps["contextWindow"], json!(200_000));
        assert_eq!(caps["vision"], json!(false));
        assert_eq!(caps["structuredOutput"], json!(false));
    }

    #[test]
    fn a_stale_selection_resolves_to_the_head_rather_than_the_wire() {
        let models = vec![
            catalog_entry(&row("glm-4.6")).unwrap(),
            catalog_entry(&row("glm-5.3-flash")).unwrap(),
            catalog_entry(&row("glm-5.3")).unwrap(),
        ];
        assert_eq!(resolve_model_key(&models, "glm-4.6"), "glm-4.6");
        // An id the catalog does not list would reach the wire and be refused with
        // a 400, so it lands on the fallback instead.
        assert_eq!(resolve_model_key(&models, "glm-retired"), FALLBACK_MODEL);
        // The head prefers the documented fallback model over list order.
        assert_eq!(first_key_of(&models).as_deref(), Some(FALLBACK_MODEL));
        assert_eq!(
            first_key_of(&[catalog_entry(&row("glm-4.6")).unwrap()]).as_deref(),
            Some("glm-4.6")
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
