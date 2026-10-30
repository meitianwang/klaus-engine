//! The Volcengine Ark Coding Plan catalog and its declared capability matrix.
//!
//! The model LIST is live: `{base}/v1/models` on the same root that serves
//! inference, read with the same Bearer key. Measured with a live plan key on
//! 2026-10-09: the route answers 135 rows carrying `id`, `name` and a lifecycle
//! `status`. A hardcoded list would either hide a model the plan adds or offer one
//! it has retired.
//!
//! The directory preserves platform rows regardless of lifecycle status. The
//! service decides whether an account may run a selected model. The declared
//! structured-safety set below separately guides forced-tool auxiliary requests.
//!
//! The CAPABILITIES beside that list are a declared table, and that is not
//! laziness: the catalog route returns `id`, `name`, `status` and nothing about a
//! model's window, its effort ladder or whether it can see an image. Every value
//! in the table below was measured against the live endpoint on a real plan key,
//! and each row says what was measured. A model the table does not know gets
//! conservative defaults rather than a guess.

use serde_json::{json, Map, Value};
use std::sync::{Mutex, OnceLock};

const MODELS_PATH: &str = "/v1/models";
const CATALOG_TTL_MS: i64 = 6 * 60 * 60 * 1000;
const FETCH_TIMEOUT_MS: u64 = 15_000;
/// A cold picker waits this long for the live list before painting the snapshot.
const WARM_TIMEOUT_MS: u64 = 1500;
/// The listing carries no paging fields (measured: `data` plus `object`, one page
/// of 135); the loop is kept so a future paged answer cannot silently drop models.
const PAGE_SIZE: i64 = 200;
const MAX_PAGES: u8 = 10;

/// What a bare selection names when nothing else can answer.
///
/// The plan's own routing alias: the console manages which model it points at, and
/// the gateway answers it on a plan key (measured). It is the safe landing spot for
/// a bare selection and for the low tier that session titles run on, because it
/// accepted the forced-tool structured path.
pub const DEFAULT_SELECTION: &str = "ark-code-latest";

/// The stable model names the plan publishes, injected into every projection.
///
/// Measured with a live plan key on 2026-10-09: the catalog route lists VERSIONED
/// snapshots (`doubao-seed-2-1-pro-260915`) and never lists these aliases, while
/// the aliases are what the plan's own integration guide publishes and what the
/// gateway answers on the wire. A picker built from the catalog alone would offer
/// date-stamped snapshots that rotate out from under a stored selection, and would
/// not offer the names the plan is sold on. Injecting them is the same move the
/// MiniMax source makes for a flagship its catalog omits, in the opposite
/// direction: there the missing row was one model, here it is the alias layer.
///
/// Every alias here was probed: each answered a turn on the plan key. The four that
/// refuse the disabled-thinking shape are still offered for ordinary turns; the
/// tier mapping pins them away from the unmapped low tier instead.
const PLAN_ALIASES: &[&str] = &[
    "ark-code-latest",
    "doubao-seed-evolving",
    "doubao-seed-2.1-pro",
    "doubao-seed-2.1-lite",
    "doubao-seed-2.0-mini",
    "minimax-m3",
    "glm-5.3",
    "glm-latest",
    "glm-5.3-flash",
    "deepseek-v4.1-flash",
    "deepseek-v4-flash",
    "deepseek-v4-pro",
    "kimi-k2.7-code",
    "kimi-k2.8-preview",
    "kimi-k3",
];

/// Reduce an id to its family so a versioned snapshot and its stable alias share
/// one row of the measured matrix.
///
/// `doubao-seed-2-1-pro-260915` and `doubao-seed-2.1-pro` are the same model at two
/// granularities; stripping a trailing date stamp and every non-alphanumeric
/// character maps both onto `doubaoseed21pro`. A name that matches no alias keeps
/// its own normalization and falls through to the conservative default.
fn family_key(id: &str) -> String {
    let stripped = id
        .rsplit_once('-')
        .map(|(head, tail)| {
            if tail.len() == 6 && tail.chars().all(|c| c.is_ascii_digit()) {
                head
            } else {
                id
            }
        })
        .unwrap_or(id);
    stripped
        .chars()
        .filter(|c| c.is_alphanumeric())
        .flat_map(|c| c.to_lowercase())
        .collect()
}

fn facts_for_id(id: &str) -> String {
    let normalized = family_key(id);
    PLAN_ALIASES
        .iter()
        .find(|alias| family_key(alias) == normalized)
        .map(|alias| alias.to_string())
        .unwrap_or_else(|| id.to_string())
}

/// The full five-rung ladder. Measured accepted on every model probed: each took
/// `output_config.effort` at low/medium/high/xhigh/max with the `effort-2025-11-24`
/// beta header the engine sends, and answered 200. The thinking length did not track
/// the rung monotonically on every model, so the ladder is declared as ACCEPTED
/// rather than as a promise that each rung buys more thought.
const FULL_LADDER: [&str; 5] = ["low", "medium", "high", "xhigh", "max"];

/// One measured model: context window, whether thinking can be turned off, whether
/// it reads images, which effort rungs it accepts, and whether it takes the native
/// structured-output shape.
struct ModelFacts {
    reasoning_known: bool,
    context_tokens: i64,
    can_disable_thinking: bool,
    vision: bool,
    /// Whether the model served the forced-tool structured path WITH
    /// `thinking:{"type":"disabled"}` attached — the exact body session titles send.
    structured_safe: bool,
}

/// The measured matrix, probed on 2026-10-09 with a live plan key.
///
/// Three facts hold across the table and are stated once here rather than per row:
///
/// * `can_disable_thinking` is FALSE for the whole table, and for two different
///   reasons that must not be confused. The doubao, deepseek and kimi rows ACCEPT
///   the disabled shape and answer without a thinking block. The glm rows REFUSE it
///   with `400 InvalidParameter: thinking.type 'disabled' is not supported by this
///   model`. Declaring an off switch per model would put a control in the picker
///   that either does nothing or errors, so the capability is not declared at all;
///   the effort ladder is the real control and it is accepted everywhere.
/// * `structured_safe` is the fact that decides whether a model can serve session
///   titles, which run on the low tier as a forced-tool request carrying
///   `thinking:{"type":"disabled"}`. Measured: the doubao and deepseek rows emitted
///   the tool_use block with that body attached; the glm rows were refused by it
///   (they reject the disabled shape) though they serve the same forced-tool
///   request WITHOUT it. A model that is not structured-safe is still offered for
///   ordinary turns — it is only pinned away from the unmapped low tier.
/// * Context windows are the plan's own published configuration values (the
///   integration guide's per-model `limit.context`), not a measured ceiling: this
///   gateway accepted inputs without refusing at the documented window, so there is
///   no refusal message to read a ceiling out of. The documented number is the
///   conservative direction.
fn facts(model: &str) -> ModelFacts {
    let normalized = facts_for_id(model);
    match normalized.as_str() {
        // The plan's routing alias. Measured: answered a turn and the forced-tool
        // path with the disabled shape attached, so it can serve titles.
        "ark-code-latest" => ModelFacts {
            reasoning_known: true,
            context_tokens: 256_000,
            can_disable_thinking: false,
            vision: true,
            structured_safe: true,
        },
        // Measured reading the probe image correctly (purple field, white square),
        // and serving the forced-tool path with the disabled shape attached.
        "doubao-seed-evolving" | "doubao-seed-2.1-pro" | "doubao-seed-2.1-lite" => ModelFacts {
            reasoning_known: true,
            context_tokens: 1_024_000,
            can_disable_thinking: false,
            vision: true,
            structured_safe: true,
        },
        "doubao-seed-2.0-mini" => ModelFacts {
            reasoning_known: true,
            context_tokens: 256_000,
            can_disable_thinking: false,
            vision: true,
            structured_safe: true,
        },
        // Measured reading the probe image correctly and serving the forced-tool
        // path with the disabled shape attached.
        "deepseek-v4.1-flash" => ModelFacts {
            reasoning_known: true,
            context_tokens: 1_024_000,
            can_disable_thinking: false,
            vision: true,
            structured_safe: true,
        },
        "deepseek-v4-flash" => ModelFacts {
            reasoning_known: true,
            context_tokens: 1_024_000,
            can_disable_thinking: false,
            vision: true,
            structured_safe: true,
        },
        // Accepted the image block and answered 200, but named colours that were not
        // in the probe (black field, blue square against purple-and-white). A model
        // that takes the block and does not see it must not be declared visual, or
        // the composer offers image input that silently answers about nothing.
        // Served the forced-tool path with the disabled shape attached.
        "deepseek-v4-pro" => ModelFacts {
            reasoning_known: true,
            context_tokens: 1_024_000,
            can_disable_thinking: false,
            vision: false,
            structured_safe: true,
        },
        // Measured reading the probe image correctly. kimi-k3 served the forced-tool
        // path with the disabled shape; kimi-k2.8-preview answered the image but its
        // forced-tool answer with the disabled shape was prose rather than the tool
        // block, so it is not pinned to titles.
        "kimi-k3" => ModelFacts {
            reasoning_known: true,
            context_tokens: 1_024_000,
            can_disable_thinking: false,
            vision: true,
            structured_safe: true,
        },
        // Measured: accepted the image block and answered 200, but its answer was an
        // EMPTY text block after a long thinking block on both probes — no evidence
        // it saw anything. Not declared visual. It did serve an ordinary turn, so it
        // stays offered for text.
        "kimi-k2.8-preview" => ModelFacts {
            reasoning_known: true,
            context_tokens: 1_024_000,
            can_disable_thinking: false,
            vision: false,
            structured_safe: false,
        },
        // Accepted the image block, answered 200, and returned an empty text block
        // after a long thinking block — no evidence it saw the probe. Not declared
        // visual. kimi-k2.7-code additionally REFUSED the forced-tool path even
        // without the disabled shape (`400 InvalidParameter`), so it is not
        // structured-safe either.
        "kimi-k2.7-code" => ModelFacts {
            reasoning_known: true,
            context_tokens: 256_000,
            can_disable_thinking: false,
            vision: false,
            structured_safe: false,
        },
        // The resold glm rows. Measured: they REFUSE `thinking:{"type":"disabled"}`
        // with 400, and they REFUSE an image block with `400 Model only support text
        // input`. They DO serve the forced-tool path when the disabled shape is left
        // off, so they are usable for ordinary turns and for explicitly mapped
        // tiers — but never for the unmapped low tier that titles run on.
        "glm-5.3" | "glm-latest" => ModelFacts {
            reasoning_known: true,
            context_tokens: 1_024_000,
            can_disable_thinking: false,
            vision: false,
            structured_safe: false,
        },
        // Measured reading the probe image correctly (purple field, white square),
        // but like its siblings it refuses the disabled thinking shape.
        "glm-5.3-flash" => ModelFacts {
            reasoning_known: true,
            context_tokens: 1_024_000,
            can_disable_thinking: false,
            vision: true,
            structured_safe: false,
        },
        // Measured reading the probe image correctly and serving the forced-tool
        // path with the disabled shape attached.
        "minimax-m3" => ModelFacts {
            reasoning_known: true,
            context_tokens: 1_024_000,
            can_disable_thinking: false,
            vision: true,
            structured_safe: true,
        },
        // An unknown model: the plan adds ids over time and routes an alias to
        // whatever the console points at. Keep it usable with the safest reading —
        // no vision (declaring one that is not there is the failure that reads as an
        // unrelated bug), a window small enough that compaction happens before the
        // service refuses, no unmeasured reasoning controls, and
        // NOT structured-safe, so an unknown model is never silently pinned to the
        // title path that sends the disabled shape a refusing model would 400.
        _ => ModelFacts {
            reasoning_known: false,
            context_tokens: 256_000,
            can_disable_thinking: false,
            vision: false,
            structured_safe: false,
        },
    }
}

/// Why a catalog read failed, and which recovery applies.
///
/// `not_found` means there is nothing to list (no key stored), `login_expired`
/// means the key was refused so the host should send the user back to re-enter it.
#[derive(Debug, Clone)]
pub struct ArkError {
    pub message: String,
    pub not_found: bool,
    pub login_expired: bool,
}

impl std::fmt::Display for ArkError {
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

fn http_client() -> Result<reqwest::Client, ArkError> {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(FETCH_TIMEOUT_MS))
        .user_agent(super::auth::user_agent())
        .build()
        .map_err(|error| ArkError {
            message: error.to_string(),
            not_found: false,
            login_expired: false,
        })
}

/// Ensure the plan's published model names are present in a projected list, with
/// the routing alias at the head.
///
/// Measured with a live plan key: the catalog route lists 135 platform rows as
/// versioned snapshots (`doubao-seed-2-1-pro-260915`) and never lists the stable
/// aliases the plan is sold on — `ark-code-latest`, `kimi-k3`, `glm-5.3` and the
/// rest — even though the gateway answers every one of them on the same key. A
/// picker built from the catalog alone would offer date-stamped snapshots that
/// rotate out from under a stored selection and omit the names the plan publishes.
/// Injecting the missing aliases is the same move the MiniMax source makes for a
/// flagship its catalog omits.
///
/// The alias goes in ahead of the snapshots it names so a bare selection and the
/// picker's default row agree on the routing alias.
fn ensure_plan_aliases(models: &mut Vec<Value>) {
    for alias in PLAN_ALIASES.iter().rev() {
        if models
            .iter()
            .any(|model| model.get("key").and_then(Value::as_str) == Some(*alias))
        {
            continue;
        }
        if let Some(entry) = catalog_entry(&json!({ "id": alias })) {
            models.insert(0, entry);
        }
    }
    // Head the list with the routing alias specifically, so `first_model_key`
    // answers it rather than whichever alias sorted first.
    if let Some(index) = models
        .iter()
        .position(|model| model.get("key").and_then(Value::as_str) == Some(DEFAULT_SELECTION))
    {
        let head = models.remove(index);
        models.insert(0, head);
    }
}

/// Read every page the catalog serves, authenticating with an explicit key rather
/// than the stored one. Split out so the key-verification read and the session read
/// share one implementation.
pub(crate) async fn fetch_live_with_key(key: &str) -> Result<Vec<Value>, ArkError> {
    let http = http_client()?;
    let base = super::auth::api_base();
    let mut models = Vec::new();
    let mut after: Option<String> = None;
    for _ in 0..MAX_PAGES {
        let mut request = http
            .get(format!("{base}{MODELS_PATH}"))
            // Bearer is the header the plan's own guide configures, and the one the
            // gateway answers an unauthenticated request about with a real 401.
            .header("Authorization", format!("Bearer {key}"))
            .query(&[("limit", PAGE_SIZE.to_string())]);
        if let Some(cursor) = after.as_deref() {
            request = request.query(&[("after", cursor.to_string())]);
        }
        let response = request.send().await.map_err(|error| ArkError {
            message: error.to_string(),
            not_found: false,
            login_expired: false,
        })?;
        let status_code = response.status().as_u16();
        let body = response.text().await.unwrap_or_default();
        if status_code == 401 || status_code == 403 {
            return Err(ArkError {
                message: "ark_key_rejected".into(),
                not_found: false,
                login_expired: true,
            });
        }
        if !(200..300).contains(&status_code) {
            let detail: String = body.chars().take(200).collect();
            return Err(ArkError {
                message: format!("the Ark catalog answered {status_code}: {detail}"),
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
            .get("lastId")
            .or_else(|| parsed.get("last_id"))
            .and_then(Value::as_str)
            .map(str::to_string);
        models.extend(rows.iter().filter_map(catalog_entry));
        match parsed.get("hasMore").and_then(Value::as_bool) {
            Some(true) if last_id.is_some() => after = last_id,
            _ => break,
        }
    }
    if models.is_empty() {
        return Err(ArkError {
            message: "the Ark catalog listed no usable model".into(),
            not_found: true,
            login_expired: false,
        });
    }
    // The plan's published aliases never appear in the platform list, so the ones
    // the list does not carry are injected; see [`ensure_plan_aliases`].
    ensure_plan_aliases(&mut models);
    Ok(models)
}

/// Seed the cache and the on-disk snapshot from entries already read.
///
/// The key-verification read has the whole catalog in hand; throwing it away would
/// make the picker's first warm read pay for a second network round inside its
/// 1500ms budget, and lose to it on a cold connection.
pub fn remember_entries(entries: &[Value]) {
    remember(entries, super::auth::catalog_fingerprint());
}

/// Read every page the catalog serves.
async fn fetch_live_models() -> Result<Vec<Value>, ArkError> {
    let key = super::auth::api_key().ok_or_else(|| ArkError {
        // Prose, not a stable code: this message is what the settings tier card
        // renders when a catalog read fails, and a code would print as a code.
        message: "not signed in to an Ark Coding Plan".into(),
        not_found: true,
        login_expired: false,
    })?;
    fetch_live_with_key(&key).await
}

/// Project one catalog row onto the entry the picker and capability reader use.
///
/// Service status does not determine catalog membership or selection.
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
        .get("name")
        .or_else(|| model.get("display_name"))
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
    entry.insert("structuredSafe".into(), json!(facts.structured_safe));
    if facts.reasoning_known {
        let ladder: &[&str] = &FULL_LADDER;
        entry.insert("efforts".into(), json!(ladder));
        // Every model here thinks by default and honours the top rung, so the ladder's
        // head is a sensible default rather than an invented one.
        entry.insert("defaultEffort".into(), json!(ladder[ladder.len() - 1]));
    }
    super::super::conversation_catalog::mark_purpose("ark", model, &mut entry);
    Some(Value::Object(entry))
}

fn owned_catalog_path() -> std::path::PathBuf {
    super::config_root().join("ark-model-catalog.json")
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
pub async fn fetch_models() -> Result<Vec<Value>, ArkError> {
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
                Err(error) => eprintln!("[Ark] catalog refresh failed: {}", error.message),
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
pub async fn fetch_models_warm() -> Result<Vec<Value>, ArkError> {
    if super::auth::api_key().is_none() {
        return Err(ArkError {
            message: "not signed in to an Ark Coding Plan".into(),
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
                Err(error) => eprintln!("[Ark] catalog refresh failed: {}", error.message),
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
                Err(ArkError {
                    message: "the Ark catalog did not answer in time".into(),
                    not_found: false,
                    login_expired: false,
                })
            } else {
                Ok(local)
            }
        }
    }
}

/// The catalog head: what a bare selection resolves to. Prefers the plan's routing
/// alias when the list carries it, else the first live row.
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
        .find(|model| model.get("key").and_then(Value::as_str) == Some(DEFAULT_SELECTION))
        .or_else(|| models.first())
        .and_then(|model| model.get("key"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// The catalog key a requested key actually maps to.
///
/// An unknown key falls back to the head rather than going to the wire as-is: the
/// gateway answers an unlisted model with an error, and a stale selection should
/// land on something that works.
pub fn resolve_model_key(models: &[Value], requested: &str) -> String {
    if models
        .iter()
        .any(|model| model.get("key").and_then(Value::as_str) == Some(requested))
    {
        return requested.to_string();
    }
    first_key_of(models).unwrap_or_else(|| DEFAULT_SELECTION.to_string())
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
        .unwrap_or(256_000)
}

/// Whether a model can serve the forced-tool structured path WITH the disabled
/// thinking shape attached — the exact body session titles send on the low tier.
///
/// This is the fact the tier mapping consults: a model that refuses that body (the
/// resold glm rows do, with a 400) must not be pinned to an unmapped low tier, or
/// every conversation title on this source would die with a parameter error.
pub fn structured_output_safe(key: &str) -> bool {
    facts(key).structured_safe
}

/// The structured-safe model to pin an unmapped low tier to, when the main model is
/// not one. Prefers the plan's routing alias, then any measured-safe row in the
/// live catalog.
pub async fn structured_safe_fallback() -> Option<String> {
    if let Ok(models) = fetch_models().await {
        if models
            .iter()
            .any(|model| model.get("key").and_then(Value::as_str) == Some(DEFAULT_SELECTION))
        {
            return Some(DEFAULT_SELECTION.to_string());
        }
        if let Some(safe) = models.iter().find(|model| {
            model
                .get("key")
                .and_then(Value::as_str)
                .is_some_and(structured_output_safe)
        }) {
            return safe.get("key").and_then(Value::as_str).map(str::to_string);
        }
    }
    Some(DEFAULT_SELECTION.to_string())
}

/// What an Ark model can do, from the measured matrix.
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
        // Prompt-cache breakpoints were not measured on this gateway, so the
        // capability is not declared: claiming a cache the service may ignore would
        // make the engine send breakpoints for nothing, and claiming none when it
        // does honour them costs only a missed optimisation.
        "promptCacheControl": false,
        // Measured: the gateway accepts `output_config.format`-shaped requests but
        // the plan's own guide routes structured work through tools, and the
        // forced-tool path was measured working on the safe rows. Declaring a
        // native schema the service does not enforce would stop the engine sending
        // the emulation that does work.
        "structuredOutput": false,
        // Adaptive only. The glm rows refuse an explicit off and the doubao rows
        // merely accept it; neither is a published off switch, so the effort ladder
        // below is the control that does something.
        "thinking": {
            "adaptive": true,
            "interleaved": false,
            "canDisableThinking": facts.can_disable_thinking,
        },
    });
    // `tools` is deliberately NOT declared: the Anthropic provider treats tool
    // support as a provider-level truth and never reads a per-model flag, so a
    // field here could not change behaviour and would read as a control that does
    // nothing.
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
    crate::app_server::catalog_test_support::warm_catalog_tests!("ark", read_local_models);

    /// One catalog row as the live endpoint returns it.
    fn row(id: &str, status: Option<&str>) -> Value {
        let mut entry = Map::new();
        entry.insert("id".into(), json!(id));
        entry.insert("name".into(), json!(id));
        entry.insert("object".into(), json!("model"));
        if let Some(status) = status {
            entry.insert("status".into(), json!(status));
        }
        Value::Object(entry)
    }

    #[test]
    fn listed_models_remain_selectable_regardless_of_lifecycle_status() {
        for status in ["Shutdown", "Retiring", "Offline", "Closed", "Available"] {
            assert!(catalog_entry(&row("listed-model", Some(status))).is_some());
        }
        assert!(catalog_entry(&row("listed-model", None)).is_some());
    }

    #[test]
    fn a_row_without_an_id_is_dropped() {
        assert!(catalog_entry(&json!({ "name": "No id" })).is_none());
        assert!(catalog_entry(&json!({ "id": "   " })).is_none());
    }

    #[test]
    fn the_models_that_refuse_a_disabled_thinking_shape_are_not_structured_safe() {
        // The resold glm rows answer `thinking:{"type":"disabled"}` with a 400, and
        // session titles send exactly that body on the low tier. Pinning one of them
        // to an unmapped low tier would kill every title on this source.
        for refusing in ["glm-5.3", "glm-latest", "glm-5.3-flash", "kimi-k2.7-code"] {
            assert!(
                !structured_output_safe(refusing),
                "{refusing} must not serve the title path"
            );
        }
        for safe in [
            "ark-code-latest",
            "doubao-seed-2.1-pro",
            "doubao-seed-2.1-lite",
            "doubao-seed-2.0-mini",
            "deepseek-v4-pro",
            "deepseek-v4-flash",
            "deepseek-v4.1-flash",
            "kimi-k3",
            "minimax-m3",
        ] {
            assert!(structured_output_safe(safe), "{safe} served the title path");
        }
    }

    #[test]
    fn vision_is_declared_only_for_models_measured_reading_the_probe() {
        // Two answers were wrong in ways that matter: deepseek-v4-pro invented
        // colours for a purple-and-white probe, and the kimi code previews returned
        // an empty text block. Declaring vision for those would let the composer
        // offer image input that answers about nothing.
        for seeing in [
            "ark-code-latest",
            "doubao-seed-evolving",
            "doubao-seed-2.1-pro",
            "doubao-seed-2.1-lite",
            "doubao-seed-2.0-mini",
            "deepseek-v4.1-flash",
            "deepseek-v4-flash",
            "kimi-k3",
            "minimax-m3",
            "glm-5.3-flash",
        ] {
            assert!(facts(seeing).vision, "{seeing} read the probe");
        }
        for blind in [
            "deepseek-v4-pro",
            "kimi-k2.7-code",
            "kimi-k2.8-preview",
            "glm-5.3",
        ] {
            assert!(!facts(blind).vision, "{blind} did not read the probe");
        }
    }

    #[test]
    fn no_model_declares_a_thinking_off_switch() {
        // The glm rows refuse it outright and the doubao rows merely accept it;
        // neither is a published off switch, so an off rung in the picker would be a
        // control that either errors or does nothing.
        for model in [
            "ark-code-latest",
            "doubao-seed-2.1-pro",
            "glm-5.3",
            "kimi-k3",
        ] {
            assert!(!facts(model).can_disable_thinking, "{model}");
        }
    }

    #[test]
    fn every_model_takes_the_full_effort_ladder() {
        // Measured: all five rungs answered 200 with the engine's own wire shape
        // (`output_config.effort` plus the effort beta header).
        let entry = catalog_entry(&row("doubao-seed-2-1-pro-260915", None)).expect("entry");
        let levels: Vec<&str> = entry["efforts"]
            .as_array()
            .expect("efforts")
            .iter()
            .filter_map(Value::as_str)
            .collect();
        assert_eq!(levels, FULL_LADDER);
        assert_eq!(entry["defaultEffort"], json!("max"));
    }

    #[test]
    fn the_capability_set_carries_only_decodable_fields() {
        let entry = catalog_entry(&row("doubao-seed-2-1-pro-260915", None)).expect("entry");
        let caps = capabilities_of(Some(&entry), "doubao-seed-2.1-pro");
        assert_eq!(caps["contextWindow"], json!(1_024_000));
        assert_eq!(caps["vision"], json!(true));
        assert_eq!(
            caps["thinking"],
            json!({
                "adaptive": true,
                "interleaved": false,
                "canDisableThinking": false,
            })
        );
        // The engine's capability decoder rejects unknown fields and fails session
        // startup, so the per-model structured facts must stay off the capability
        // set. The thinking flag is part of the accepted thinking capability.
        assert!(caps.get("canDisableThinking").is_none());
        assert!(caps.get("structuredSafe").is_none());
        assert!(caps.get("tools").is_none());
        assert_eq!(
            caps["effort"]["levels"],
            json!(["low", "medium", "high", "xhigh", "max"])
        );
    }

    #[test]
    fn an_unknown_model_gets_conservative_defaults() {
        let caps = capabilities_of(None, "some-model-not-in-the-table");
        assert_eq!(caps["contextWindow"], json!(256_000));
        assert_eq!(caps["vision"], json!(false));
        assert_eq!(caps["structuredOutput"], json!(false));
        // Not structured-safe: an unknown model is never silently pinned to the
        // title path that sends a body a refusing model would 400.
        assert!(!structured_output_safe("some-model-not-in-the-table"));
    }

    #[test]
    fn a_stale_selection_resolves_to_the_head_rather_than_the_wire() {
        // The catalog carries versioned snapshots and the injected aliases; a pick
        // that is neither would reach the wire and be refused, so it lands on the
        // plan's routing alias instead.
        let models = vec![
            catalog_entry(&row("doubao-seed-2-1-pro-260915", None)).unwrap(),
            catalog_entry(&row("ark-code-latest", None)).unwrap(),
        ];
        assert_eq!(
            resolve_model_key(&models, "doubao-seed-2-1-pro-260915"),
            "doubao-seed-2-1-pro-260915"
        );
        assert_eq!(
            resolve_model_key(&models, "doubao-retired"),
            DEFAULT_SELECTION
        );
        // The head prefers the plan's routing alias over list order.
        assert_eq!(first_key_of(&models).as_deref(), Some(DEFAULT_SELECTION));
    }

    #[test]
    fn a_versioned_snapshot_and_its_stable_alias_share_one_capability_row() {
        // The catalog lists `doubao-seed-2-1-pro-260915`; the plan publishes
        // `doubao-seed-2.1-pro`. Both are the same model, so both must read the same
        // measured window and vision facts rather than one falling through to the
        // conservative default.
        let snapshot = catalog_entry(&row("doubao-seed-2-1-pro-260915", None)).expect("entry");
        assert_eq!(snapshot["contextTokens"], json!(1_024_000));
        assert_eq!(snapshot["is_vl"], json!(true));
        let alias = catalog_entry(&row("doubao-seed-2.1-pro", None)).expect("entry");
        assert_eq!(alias["contextTokens"], json!(1_024_000));
        assert_eq!(alias["is_vl"], json!(true));
    }
    #[test]
    fn an_unmeasured_catalog_row_does_not_advertise_native_reasoning_controls() {
        let entry = catalog_entry(&json!({"id": "unmeasured-future-model"})).expect("entry");
        assert!(entry.get("efforts").is_none());
        assert!(entry.get("defaultEffort").is_none());
        assert!(entry.get("canDisableThinking").is_none());
    }
}
