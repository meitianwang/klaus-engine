//! The Codex subscription's model catalog.
//!
//! Which models a ChatGPT/Codex subscription may run, and how wide each one's
//! context is, is only knowable from the service: the ids are opaque and the
//! windows move with the plan. The engine needs both while resolving a session —
//! the window becomes the model's declared `contextWindow`, and the id list is
//! what makes a tier pick trustworthy — so the fetch belongs here, beside the
//! resolver, rather than in a client that would have to hand the answer over.
//!
//! Cached for six hours against the (account, client version) pair it was fetched
//! for, because a session start must not pay for a round trip that a picker
//! already made.
//!
//! A read never waits on the network: memory cache, then the disk copy of the
//! last good catalog, then the catalog bundled with the build, and a background
//! revalidation replaces whichever of those answered once the service replies.
//! A session start that woke onto a dead network must resolve in milliseconds,
//! not in one fetch timeout per catalog read.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use serde_json::{json, Map, Value};

use super::model_sources;

const CATALOG_TTL_MS: i64 = 6 * 60 * 60 * 1000;
const NORMALIZED_CATALOG_VERSION: u64 = 3;
const FETCH_TIMEOUT_MS: u64 = 15_000;
/// A dead route or a blackholed DNS must fail fast instead of spending the whole
/// fetch timeout: the background revalidation retries after its cooldown anyway.
const CONNECT_TIMEOUT_MS: u64 = 4_000;
/// Silence between two failed background revalidations, so a machine that woke
/// onto a dead network pays for at most one fetch per minute of catalog reads.
const REVERIFY_COOLDOWN_MS: i64 = 60_000;

/// The backend whitelists first-party originators and answers 403 to anything
/// else, so every call identifies as the official CLI: this originator value plus
/// the matching User-Agent shape.
const ORIGINATOR: &str = "codex_cli_rs";

/// Overridable so the catalog can be driven against a mock, and because the real
/// host is DNS-poisoned on some networks.
fn models_endpoint() -> String {
    std::env::var("KLAUS_CODEX_MODELS_URL")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "https://chatgpt.com/backend-api/codex/models".into())
}

/// A live bearer plus the account the service buckets it under.
#[derive(Clone)]
pub(super) struct Token {
    pub(super) access_token: String,
    pub(super) account_id: String,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum CatalogRead {
    /// The session-resolution read. It answers from whatever catalog is at hand
    /// and revalidates behind the caller, like `Cached`: a provider must never
    /// wait on the network. The variant survives because the wire still names it.
    Warm,
    /// Answer from whatever catalog is at hand (memory, then disk) and refresh
    /// behind the caller, so a settings page never waits on the network.
    Cached,
}

struct Cached {
    account_id: String,
    client_version: String,
    expires_at: i64,
    models: Vec<Value>,
}

fn cache() -> &'static Mutex<Option<Cached>> {
    static CACHE: OnceLock<Mutex<Option<Cached>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(None))
}

/// The last good catalog on disk, keyed like the memory cache.
///
/// The memory cache dies with the process, so an app relaunch inside the six
/// hour window used to pay a full network round trip again. The disk copy is
/// what lets a read answer at once after a relaunch. Only a successful live
/// revalidation replaces this copy.
fn disk_cache_path(config_root: &Path) -> PathBuf {
    config_root.join("codex-model-catalog.json")
}

fn read_disk_cache(
    config_root: &Path,
    account_id: &str,
    client_version: &str,
) -> Option<Vec<Value>> {
    let raw = std::fs::read_to_string(disk_cache_path(config_root)).ok()?;
    let value: Value = serde_json::from_str(&raw).ok()?;
    if value.get("accountId").and_then(Value::as_str) != Some(account_id) {
        return None;
    }
    if value.get("clientVersion").and_then(Value::as_str) != Some(client_version) {
        return None;
    }
    let mut models = value.get("models").and_then(Value::as_array)?.clone();
    if value
        .get("schemaVersion")
        .and_then(Value::as_u64)
        .unwrap_or_default()
        < NORMALIZED_CATALOG_VERSION
    {
        restore_legacy_output_verbosity(&mut models);
        restore_legacy_review_metadata(&mut models);
    }
    (!models.is_empty()).then_some(models)
}

fn write_disk_cache(config_root: &Path, account_id: &str, client_version: &str, models: &[Value]) {
    let value = json!({
        "schemaVersion": NORMALIZED_CATALOG_VERSION,
        "accountId": account_id,
        "clientVersion": client_version,
        "models": models,
    });
    let Ok(raw) = serde_json::to_string(&value) else {
        return;
    };
    let _ = std::fs::write(disk_cache_path(config_root), raw);
}

/// The catalog shipped inside the build: a known-good service answer reduced to
/// the fields a session resolution reads. A signed-in read with neither a memory
/// nor a disk cache answers from here instead of waiting on the network; the
/// background revalidation replaces it once the service answers. Empty only if
/// the build itself carries no catalog, in which case a read with nothing at
/// hand is honest about having nothing.
fn bundled_models() -> Vec<Value> {
    static BUNDLED: OnceLock<Vec<Value>> = OnceLock::new();
    BUNDLED
        .get_or_init(|| {
            serde_json::from_str(include_str!("bundled_codex_models.json")).unwrap_or_default()
        })
        .clone()
}

fn bundled_review_models() -> Vec<Value> {
    let mut models = bundled_models();
    let internal: Vec<Value> =
        serde_json::from_str(include_str!("bundled_internal_models.json")).unwrap_or_default();
    for model in internal {
        if !models
            .iter()
            .any(|entry| entry.get("id") == model.get("id"))
        {
            models.push(model);
        }
    }
    models
}

/// Canonical models explicitly available on the standard API. Subscription-only
/// rows remain available to their own authenticated source.
pub(crate) fn bundled_api_models() -> Vec<Value> {
    bundled_review_models()
        .into_iter()
        .filter(|entry| entry.get("supportedInApi").and_then(Value::as_bool) == Some(true))
        .collect()
}

/// Older normalized caches discarded output-detail metadata. Recover only the
/// explicit declaration for an exact bundled model, without replacing cached
/// identities, limits, or explicit declarations. A current-version live answer
/// remains authoritative even when it omits support for this capability.
fn restore_legacy_output_verbosity(models: &mut [Value]) {
    let bundled = bundled_models();
    for model in models {
        if [
            "outputVerbosity",
            "support_verbosity",
            "supportVerbosity",
            "default_verbosity",
            "defaultVerbosity",
        ]
        .iter()
        .any(|key| model.get(*key).is_some())
        {
            continue;
        }
        let Some(id) = model.get("id").and_then(Value::as_str) else {
            continue;
        };
        let Some(verbosity) = bundled
            .iter()
            .find(|entry| entry.get("id").and_then(Value::as_str) == Some(id))
            .and_then(declared_output_verbosity)
        else {
            continue;
        };
        if let Some(fields) = model.as_object_mut() {
            fields.insert("outputVerbosity".into(), json!(verbosity.as_str()));
        }
    }
}

/// Older normalized cache documents removed internal-only rows and review
/// metadata. Recover only those omitted fields from the bundled offline source.
fn restore_legacy_review_metadata(models: &mut Vec<Value>) {
    let picker_seed = bundled_models();
    let bundled = bundled_review_models();
    for entry in &bundled {
        let Some(id) = entry.get("id").and_then(Value::as_str) else {
            continue;
        };
        if let Some(model) = models
            .iter_mut()
            .find(|model| model.get("id").and_then(Value::as_str) == Some(id))
        {
            if let Some(fields) = model.as_object_mut() {
                for key in [
                    "visibility",
                    "autoReviewModelOverride",
                    "requiresSandboxedReview",
                    "nodeReplAutoReviewRequired",
                    "guardian",
                    "guardianV2",
                    "modelMessages",
                    "compHash",
                    "supportsReasoningSummaryParameter",
                    "defaultReasoningSummary",
                    "supportsImageDetailOriginal",
                    "modelSpecialty",
                    "usedFallbackModelMetadata",
                    "effectiveContextWindowPercent",
                    "supportedReasoningEfforts",
                    "defaultReasoningEffort",
                ] {
                    if !fields.contains_key(key) {
                        if let Some(value) = entry.get(key) {
                            fields.insert(key.into(), value.clone());
                        }
                    }
                }
            }
        } else if !picker_visible(entry)
            && picker_seed
                .iter()
                .any(|seed| seed.get("id") == entry.get("id"))
        {
            models.push(entry.clone());
        }
    }
}

/// One background revalidation in flight at a time, plus a cooldown after a
/// failed one: catalog reads must not queue a fetch each while the network
/// stays dead.
struct Reverify {
    inflight: bool,
    retry_at: i64,
}

fn reverify_state() -> &'static Mutex<Reverify> {
    static STATE: OnceLock<Mutex<Reverify>> = OnceLock::new();
    STATE.get_or_init(|| {
        Mutex::new(Reverify {
            inflight: false,
            retry_at: 0,
        })
    })
}

/// Refresh both caches behind the caller. Never awaited by a read: the catalog
/// that answered stays authoritative until this lands.
fn schedule_reverify(config_root: &Path, account_id: &str, client_version: &str, endpoint: &str) {
    if !fetch_allowed(endpoint) {
        return;
    }
    let mut guard = reverify_state()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    if guard.inflight || guard.retry_at > now_ms() {
        return;
    }
    guard.inflight = true;
    let root = config_root.to_path_buf();
    let account = account_id.to_string();
    let version = client_version.to_string();
    let endpoint = endpoint.to_string();
    tokio::spawn(async move {
        let outcome = async {
            let token = token(&root)
                .await
                .filter(|token| token.account_id == account)
                .ok_or_else(|| "not signed in to a ChatGPT/Codex subscription".to_string())?;
            fetch_live(&root, &token, &version, &endpoint).await
        }
        .await;
        let mut guard = reverify_state()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        guard.inflight = false;
        if outcome.is_err() {
            guard.retry_at = now_ms() + REVERIFY_COOLDOWN_MS;
        }
    });
}

/// Whether the catalog may be fetched at all.
///
/// FALSE under `cfg(test)` unless the endpoint was repointed, deliberately: the
/// default endpoint is the REAL service, so a unit test that resolved a fixtured
/// Codex login would spend a network round trip on it and read a live catalog into
/// a process-global cache. A test that wants a catalog points
/// `KLAUS_CODEX_MODELS_URL` at its own server, or passes a loopback stub address
/// straight in — both are provably not the real service.
fn fetch_allowed(endpoint: &str) -> bool {
    if !cfg!(test) {
        return true;
    }
    std::env::var_os("KLAUS_CODEX_MODELS_URL").is_some()
        || endpoint.starts_with("http://127.0.0.1:")
        || endpoint.starts_with("http://localhost:")
}

/// The catalog for the signed-in account, from the cache when it is still warm.
///
/// An empty answer is an ERROR, not a valid catalog: caching it would leave every
/// model window and tier check blind for the next six hours.
pub(crate) async fn models(config_root: &Path) -> Result<Vec<Value>, String> {
    models_with_read(config_root, CatalogRead::Warm).await
}

pub(crate) async fn all_models(config_root: &Path) -> Result<Vec<Value>, String> {
    models_with_read_at(config_root, CatalogRead::Warm, &models_endpoint()).await
}

fn picker_visible(model: &Value) -> bool {
    super::conversation_catalog::is_conversation_model("codex", model)
}

/// Read the current account's complete local catalog without revalidation or I/O
/// to a remote service. An account change invalidates the admitted source.
pub(crate) fn all_models_offline(config_root: &Path, account_id: &str) -> Vec<Value> {
    let current = model_sources::codex_account(config_root).and_then(|account| {
        account
            .get("accountId")
            .and_then(Value::as_str)
            .map(str::to_string)
    });
    if current.as_deref() != Some(account_id) {
        return Vec::new();
    }
    let version = model_sources::codex_client_version();
    let cached = cache()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .as_ref()
        .filter(|entry| entry.account_id == account_id && entry.client_version == version)
        .map(|entry| entry.models.clone());
    cached
        .or_else(|| read_disk_cache(config_root, account_id, &version))
        .unwrap_or_else(bundled_review_models)
}

/// Conversation choices honor source visibility. Internal runtime callers use
/// the complete catalog through `all_models` or `all_models_offline`.
pub(crate) async fn models_with_read(
    config_root: &Path,
    read: CatalogRead,
) -> Result<Vec<Value>, String> {
    models_with_read_at(config_root, read, &models_endpoint())
        .await
        .map(|models| models.into_iter().filter(picker_visible).collect())
}

/// The endpoint is a parameter so tests can drive the catalog against a local
/// stub without touching the process environment, which other suites read
/// while running in parallel.
///
/// The order is memory cache, disk copy, bundled catalog, and only a signed-in
/// read with none of the three — and a build that permits fetching — waits for
/// the live service. A stale window is preferable to a session start parked on
/// a fetch timeout: the revalidation corrects the window as soon as the service
/// answers, while a blocked start corrects nothing.
pub(crate) async fn models_with_read_at(
    config_root: &Path,
    _read: CatalogRead,
    endpoint: &str,
) -> Result<Vec<Value>, String> {
    let client_version = model_sources::codex_client_version();
    let now = now_ms();
    let account_id = model_sources::codex_account(config_root).and_then(|account| {
        account
            .get("accountId")
            .and_then(Value::as_str)
            .map(str::to_string)
    });
    let cached = {
        let guard = cache().lock().unwrap_or_else(|poison| poison.into_inner());
        guard
            .as_ref()
            .filter(|entry| {
                Some(entry.account_id.as_str()) == account_id.as_deref()
                    && entry.client_version == client_version
                    && entry.expires_at > now
            })
            .map(|entry| entry.models.clone())
    };
    if let Some(models) = cached {
        return Ok(models);
    }
    let Some(account_id) = account_id else {
        return Err("not signed in to a ChatGPT/Codex subscription".to_string());
    };
    if let Some(stale) = read_disk_cache(config_root, &account_id, &client_version) {
        schedule_reverify(config_root, &account_id, &client_version, endpoint);
        return Ok(stale);
    }
    let bundled = bundled_models();
    if !bundled.is_empty() {
        schedule_reverify(config_root, &account_id, &client_version, endpoint);
        return Ok(bundled);
    }
    if !fetch_allowed(endpoint) {
        return Err("the Codex model catalog is not reachable from this build".into());
    }
    let token = token(config_root)
        .await
        .ok_or_else(|| "not signed in to a ChatGPT/Codex subscription".to_string())?;
    fetch_live(config_root, &token, &client_version, endpoint).await
}

async fn fetch_live(
    config_root: &Path,
    token: &Token,
    client_version: &str,
    endpoint: &str,
) -> Result<Vec<Value>, String> {
    let models = fetch(token, client_version, endpoint).await?;
    let mut guard = cache().lock().unwrap_or_else(|poison| poison.into_inner());
    *guard = Some(Cached {
        account_id: token.account_id.clone(),
        client_version: client_version.to_string(),
        expires_at: now_ms() + CATALOG_TTL_MS,
        models: models.clone(),
    });
    write_disk_cache(config_root, &token.account_id, client_version, &models);
    Ok(models)
}

/// The ids the catalog advertises. `None` = no catalog (signed out, or the fetch
/// failed), in which case a tier pick cannot be validated against anything.
pub(crate) async fn ids(config_root: &Path) -> Option<Vec<String>> {
    let models = models(config_root).await.ok()?;
    Some(
        models
            .iter()
            .filter_map(|model| model.get("id").and_then(Value::as_str).map(str::to_string))
            .collect(),
    )
}

/// What one model's limits are, as the catalog reports them. `None` on a field
/// means the service said nothing, and the provider's own default stands rather
/// than a number invented here.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ModelLimits {
    pub native_deferred_tools: Option<bool>,
    pub context_window: Option<i64>,
    pub max_output_tokens: Option<i64>,
    pub output_verbosity: Option<klaus_agent_core::types::provider::OutputVerbosity>,
    /// The effort rungs THIS model advertises, restricted to the ones the engine
    /// can spell. `None` means the catalog said nothing about the ladder (or was
    /// unreadable); `Some(vec![])` means it advertised rungs and not one of them
    /// is spellable. The two must stay distinguishable: the first keeps the
    /// provider's own default, the second declares no effort knob at all.
    pub effort_levels: Option<Vec<String>>,
    pub effort_default: Option<klaus_agent_core::schemas::effort::EffortLevel>,
    pub auto_review_model_override: Option<String>,
    pub requires_sandboxed_review: bool,
    pub node_repl_auto_review_required: bool,
    pub guardian: Option<Value>,
    pub guardian_v2: Option<Value>,
    pub model_messages: Option<Value>,
    pub comp_hash: Option<String>,
    pub supports_reasoning_summary_parameter: Option<bool>,
    pub default_reasoning_summary: Option<Value>,
    pub supported_reasoning_efforts: Vec<String>,
    pub default_reasoning_effort: Option<String>,
    pub multi_agent_reasoning_effort: Option<String>,
    pub effective_context_window_percent: Option<i64>,
    pub auto_compact_token_limit: Option<u64>,
    pub used_fallback_model_metadata: bool,
    pub model_specialty: Option<String>,
    pub supports_image_detail_original: bool,
}

/// The rungs of a catalog ladder the engine can actually put on the wire.
///
/// `EffortLevel` is low/medium/high/xhigh/max, so a catalog rung outside that set
/// is dropped rather than folded onto a neighbour: folding `ultra` into `max`
/// would declare support for a rung the model does not advertise while losing the
/// one it does (the two are distinct presets on the current lineup — `ultra` adds
/// automatic task delegation). The host's picker folds them for display, which is
/// its own choice; the declared capability must stay honest.
///
/// Matching is case-insensitive and the result keeps ladder order rather than the
/// catalog's, so a backend that spells `High` or lists its rungs out of order
/// declares the same capability as one that does neither.
pub(crate) fn spellable_effort_levels(levels: &[String]) -> Vec<String> {
    const LADDER: [&str; 5] = ["low", "medium", "high", "xhigh", "max"];
    LADDER
        .iter()
        .filter(|rung| levels.iter().any(|level| level.eq_ignore_ascii_case(rung)))
        .map(|rung| (*rung).to_string())
        .collect()
}

/// The context window and output ceiling ONE model reports, matched
/// case-insensitively. The window prefers `contextWindow` over
/// `maxContextWindow`; the ceiling is whatever spelling the backend used for its
/// output cap.
pub(crate) async fn limits(config_root: &Path, model: &str) -> ModelLimits {
    let Ok(models) = all_models(config_root).await else {
        return ModelLimits::default();
    };
    let wanted = model.trim().to_ascii_lowercase();
    let Some(entry) = models.iter().find(|entry| {
        entry
            .get("id")
            .and_then(Value::as_str)
            .is_some_and(|id| id.to_ascii_lowercase() == wanted)
    }) else {
        return ModelLimits::default();
    };
    limits_from_entry(entry)
}

pub(crate) fn limits_from_entry(entry: &Value) -> ModelLimits {
    let advertised: Option<Vec<String>> = entry
        .get("supportedReasoningLevels")
        .and_then(Value::as_array)
        .map(|levels| {
            levels
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<Vec<String>>()
        })
        .map(|levels| spellable_effort_levels(&levels));
    ModelLimits {
        native_deferred_tools: entry.get("nativeDeferredTools").and_then(Value::as_bool),
        context_window: entry
            .get("contextWindow")
            .or_else(|| entry.get("maxContextWindow"))
            .and_then(Value::as_i64)
            .filter(|window| *window > 0),
        max_output_tokens: entry
            .get("maxOutputTokens")
            .and_then(Value::as_i64)
            .filter(|output| *output > 0),
        effort_levels: advertised,
        supports_image_detail_original: entry
            .get("supportsImageDetailOriginal")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        supported_reasoning_efforts: entry
            .get("supportedReasoningEfforts")
            .or_else(|| entry.get("supportedReasoningLevels"))
            .and_then(Value::as_array)
            .map(|levels| {
                levels
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default(),
        default_reasoning_effort: entry
            .get("defaultReasoningEffort")
            .or_else(|| entry.get("defaultReasoning"))
            .and_then(Value::as_str)
            .map(str::to_string),
        multi_agent_reasoning_effort: entry
            .get("multiAgentReasoningEffort")
            .or_else(|| entry.get("multi_agent_reasoning_effort"))
            .and_then(Value::as_str)
            .map(str::to_string),
        auto_compact_token_limit: entry.get("autoCompactTokenLimit").and_then(Value::as_u64),
        effective_context_window_percent: entry
            .get("effectiveContextWindowPercent")
            .and_then(Value::as_i64),
        used_fallback_model_metadata: entry
            .get("usedFallbackModelMetadata")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        model_specialty: entry
            .get("modelSpecialty")
            .and_then(Value::as_str)
            .map(str::to_string),
        output_verbosity: declared_output_verbosity(entry),
        effort_default: entry
            .get("defaultReasoning")
            .cloned()
            .and_then(|value| serde_json::from_value(value).ok()),
        auto_review_model_override: entry
            .get("autoReviewModelOverride")
            .and_then(Value::as_str)
            .map(str::to_string),
        requires_sandboxed_review: entry
            .get("requiresSandboxedReview")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        node_repl_auto_review_required: entry
            .get("nodeReplAutoReviewRequired")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        guardian: entry
            .get("guardian")
            .filter(|value| !value.is_null())
            .cloned(),
        guardian_v2: entry
            .get("guardianV2")
            .filter(|value| !value.is_null())
            .cloned(),
        model_messages: entry
            .get("modelMessages")
            .filter(|value| !value.is_null())
            .cloned(),
        comp_hash: entry
            .get("compHash")
            .and_then(Value::as_str)
            .map(str::to_string),
        supports_reasoning_summary_parameter: entry
            .get("supportsReasoningSummaryParameter")
            .and_then(Value::as_bool),
        default_reasoning_summary: entry
            .get("defaultReasoningSummary")
            .filter(|value| !value.is_null())
            .cloned(),
    }
}

fn declared_output_verbosity(
    entry: &Value,
) -> Option<klaus_agent_core::types::provider::OutputVerbosity> {
    use klaus_agent_core::types::provider::OutputVerbosity;

    let supported = entry
        .get("support_verbosity")
        .or_else(|| entry.get("supportVerbosity"))
        .and_then(Value::as_bool);
    if supported == Some(false) {
        return None;
    }
    if let Some(value) = entry
        .get("outputVerbosity")
        .and_then(Value::as_str)
        .and_then(OutputVerbosity::parse)
    {
        return Some(value);
    }
    if supported != Some(true) {
        return None;
    }
    Some(
        first_string(entry, &["default_verbosity", "defaultVerbosity"])
            .as_deref()
            .and_then(OutputVerbosity::parse)
            .unwrap_or(OutputVerbosity::Low),
    )
}

/// The catalog in the shape a model picker reads: visible choices, plus which
/// account it belongs to. Signed-out and failed-fetch answer with the reason
/// instead of an empty list, so a picker can tell "no subscription" from "the
/// service did not answer".
pub async fn list(config_root: &Path, read: CatalogRead) -> Value {
    match models_with_read(config_root, read).await {
        Ok(models) => {
            let account = model_sources::codex_account(config_root)
                .and_then(|account| {
                    account
                        .get("email")
                        .and_then(Value::as_str)
                        .filter(|email| !email.is_empty())
                        .map(str::to_string)
                })
                .unwrap_or_default();
            let account_id = model_sources::codex_account(config_root)
                .and_then(|account| {
                    account
                        .get("accountId")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
                .unwrap_or_default();
            json!({
                "ok": true,
                "account": if account.is_empty() { json!(account_id.clone()) } else { json!(account) },
                "accountId": account_id,
                "models": models,
            })
        }
        Err(error) => json!({ "ok": false, "error": error, "models": [] }),
    }
}

/// Forget the cached catalog. The credential changed, so the next read must not
/// answer with the previous account's models.
pub fn invalidate() {
    let mut guard = cache().lock().unwrap_or_else(|poison| poison.into_inner());
    *guard = None;
    // A new credential may talk to a service the cooled-down revalidation has
    // not been allowed to ask about yet.
    let mut reverify = reverify_state()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    reverify.retry_at = 0;
}

async fn fetch(token: &Token, client_version: &str, endpoint: &str) -> Result<Vec<Value>, String> {
    let separator = if endpoint.contains('?') { '&' } else { '?' };
    let url = format!("{endpoint}{separator}client_version={client_version}");
    let response = reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(FETCH_TIMEOUT_MS))
        .connect_timeout(std::time::Duration::from_millis(CONNECT_TIMEOUT_MS))
        .build()
        .map_err(|error| error.to_string())?
        .get(url)
        .header("Authorization", format!("Bearer {}", token.access_token))
        .header("ChatGPT-Account-ID", &token.account_id)
        .header("Accept", "application/json")
        .header("originator", ORIGINATOR)
        .header("User-Agent", user_agent(client_version))
        .header("version", client_version)
        .send()
        .await
        .map_err(|error| error.to_string())?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        let reason = status.canonical_reason().unwrap_or("");
        return Err(if body.is_empty() {
            format!("{} {reason}", status.as_u16())
        } else {
            format!("{} {reason} - {body}", status.as_u16())
        });
    }
    let body: Value = response.json().await.map_err(|error| error.to_string())?;
    let models = parse(&body);
    if models.is_empty() {
        return Err("the Codex model catalog returned no available models".into());
    }
    Ok(models)
}

/// Project the backend's list into the fields a caller here needs, keeping the
/// source metadata spellings. Hidden rows remain available to internal model
/// selection; the picker-facing read filters them separately. Rows retain the
/// backend's own priority order.
///
/// Optional fields are OMITTED when absent rather than sent as null: a reader
/// branches on the key being there at all.
pub(crate) fn parse(body: &Value) -> Vec<Value> {
    let source = match body {
        Value::Array(entries) => entries.clone(),
        _ => body
            .get("models")
            .or_else(|| body.get("data"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default(),
    };
    let mut seen: Vec<String> = Vec::new();
    let mut models: Vec<Value> = Vec::new();
    for item in &source {
        let Some(id) = first_string(item, &["slug", "id", "model"]) else {
            continue;
        };
        let visibility = item.get("visibility").and_then(|value| match value {
            Value::String(text) if !text.is_empty() => Some(text.clone()),
            Value::Bool(true) => Some("true".into()),
            Value::Number(number) if number.as_f64() != Some(0.0) => Some(number.to_string()),
            _ => None,
        });
        let mut model = Map::new();
        if let Some(visibility) = visibility {
            model.insert("visibility".into(), json!(visibility));
        }
        for (target, keys) in [
            (
                "nativeDeferredTools",
                &[
                    "native_deferred_tools",
                    "nativeDeferredTools",
                    "supports_tool_search",
                ][..],
            ),
            (
                "autoReviewModelOverride",
                &["auto_review_model_override", "autoReviewModelOverride"][..],
            ),
            (
                "requiresSandboxedReview",
                &["requires_sandboxed_review", "requiresSandboxedReview"][..],
            ),
            (
                "nodeReplAutoReviewRequired",
                &[
                    "node_repl_auto_review_required",
                    "nodeReplAutoReviewRequired",
                ][..],
            ),
            ("guardian", &["guardian"][..]),
            ("guardianV2", &["guardian_v2", "guardianV2"][..]),
            ("modelMessages", &["model_messages", "modelMessages"][..]),
            ("compHash", &["comp_hash", "compHash"][..]),
            ("modelSpecialty", &["model_specialty", "modelSpecialty"][..]),
            (
                "supportsImageDetailOriginal",
                &[
                    "supports_image_detail_original",
                    "supportsImageDetailOriginal",
                ][..],
            ),
            (
                "usedFallbackModelMetadata",
                &["used_fallback_model_metadata", "usedFallbackModelMetadata"][..],
            ),
            (
                "autoCompactTokenLimit",
                &["auto_compact_token_limit", "autoCompactTokenLimit"][..],
            ),
            (
                "effectiveContextWindowPercent",
                &[
                    "effective_context_window_percent",
                    "effectiveContextWindowPercent",
                ][..],
            ),
            (
                "supportedReasoningEfforts",
                &["supported_reasoning_efforts", "supportedReasoningEfforts"][..],
            ),
            (
                "defaultReasoningEffort",
                &["default_reasoning_effort", "defaultReasoningEffort"][..],
            ),
            (
                "multiAgentReasoningEffort",
                &["multi_agent_reasoning_effort", "multiAgentReasoningEffort"][..],
            ),
            (
                "supportedInApi",
                &["supported_in_api", "supportedInApi"][..],
            ),
            (
                "supportsReasoningSummaryParameter",
                &[
                    "supports_reasoning_summary_parameter",
                    "supportsReasoningSummaryParameter",
                ][..],
            ),
            (
                "defaultReasoningSummary",
                &["default_reasoning_summary", "defaultReasoningSummary"][..],
            ),
        ] {
            if let Some(value) = keys
                .iter()
                .find_map(|key| item.get(*key))
                .filter(|value| !value.is_null())
            {
                model.insert(target.into(), value.clone());
            }
        }
        model.insert("id".into(), json!(id));
        if let Some(name) = first_string(item, &["display_name", "displayName", "name"]) {
            model.insert("name".into(), json!(name));
        }
        if let Some(description) = first_string(item, &["description"]) {
            model.insert("description".into(), json!(description));
        }
        if let Some(default) = [
            "default_reasoning_level",
            "defaultReasoningLevel",
            "defaultReasoning",
        ]
        .iter()
        .find_map(|key| item.get(*key).and_then(Value::as_str))
        {
            model
                .entry("defaultReasoningEffort")
                .or_insert(json!(default));
        }
        if let Some(levels) = item
            .get("supported_reasoning_levels")
            .or_else(|| item.get("supportedReasoningLevels"))
            .and_then(Value::as_array)
        {
            let raw: Vec<String> = levels
                .iter()
                .filter_map(|level| {
                    level
                        .as_str()
                        .map(str::to_string)
                        .or_else(|| first_string(level, &["effort", "level"]))
                })
                .collect();
            model
                .entry("supportedReasoningEfforts")
                .or_insert(json!(raw));
        }
        // The reasoning ladder a picker offers for this model, and where it starts.
        if let Some(reasoning) =
            first_string(item, &["default_reasoning_level", "defaultReasoningLevel"])
        {
            model.insert("defaultReasoning".into(), json!(reasoning));
        }
        if let Some(levels) = reasoning_levels(
            item.get("supported_reasoning_levels")
                .filter(|value| !value.is_null())
                .or_else(|| item.get("supportedReasoningLevels")),
        ) {
            model.insert("supportedReasoningLevels".into(), json!(levels));
        }
        if let Some(verbosity) = declared_output_verbosity(item) {
            model.insert("outputVerbosity".into(), json!(verbosity.as_str()));
        }
        if let Some(window) = first_positive(
            item,
            &[
                "context_window",
                "contextWindow",
                "max_input_tokens",
                "maxInputTokens",
            ],
        ) {
            model.insert("contextWindow".into(), json!(window));
        }
        if let Some(window) = first_positive(item, &["max_context_window", "maxContextWindow"]) {
            model.insert("maxContextWindow".into(), json!(window));
        }
        // The output ceiling, under whichever spelling the backend used. It becomes
        // the model's declared `maxOutputTokens`, which is the most a request may
        // ask for — without it a long answer stops at the provider default.
        if let Some(output) = first_positive(
            item,
            &[
                "max_output_tokens",
                "maxOutputTokens",
                "max_completion_tokens",
                "maxCompletionTokens",
                "max_tokens",
                "maxTokens",
            ],
        ) {
            model.insert("maxOutputTokens".into(), json!(output));
        }
        if let Some(priority) = item.get("priority").and_then(Value::as_f64) {
            model.insert("priority".into(), json!(priority));
        }
        // A later duplicate replaces the earlier entry in place, so the order the
        // service listed them in survives.
        match seen.iter().position(|listed| listed == &id) {
            Some(index) => models[index] = Value::Object(model),
            None => {
                seen.push(id);
                models.push(Value::Object(model));
            }
        }
    }
    // Priority first, id as the tiebreaker; a model with no priority sorts last.
    models.sort_by(|a, b| {
        let priority = |model: &Value| {
            model
                .get("priority")
                .and_then(Value::as_f64)
                .unwrap_or(f64::MAX)
        };
        let id = |model: &Value| {
            model
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string()
        };
        priority(a)
            .partial_cmp(&priority(b))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| id(a).cmp(&id(b)))
    });
    models
}

/// The advertised effort ladder, from either plain strings or objects carrying an
/// `effort`/`level` field, lowercased and de-duplicated in order.
fn reasoning_levels(raw: Option<&Value>) -> Option<Vec<String>> {
    let mut levels: Vec<String> = Vec::new();
    for entry in raw?.as_array()? {
        let level = match entry {
            Value::String(text) => Some(text.trim().to_lowercase()),
            _ => first_string(entry, &["effort", "level"]).map(|text| text.to_lowercase()),
        };
        if let Some(level) = level.filter(|level| !level.is_empty()) {
            if !levels.contains(&level) {
                levels.push(level);
            }
        }
    }
    (!levels.is_empty()).then_some(levels)
}

fn first_string(item: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .filter_map(|key| item.get(*key).and_then(Value::as_str))
        .map(str::trim)
        .find(|text| !text.is_empty())
        .map(str::to_string)
}

/// The first positive number under any of `keys`, floored. Numbers, numeric
/// strings and booleans all coerce, because the backend is not consistent about
/// which it sends and a window dropped for its spelling reads as "no window".
fn first_positive(item: &Value, keys: &[&str]) -> Option<i64> {
    keys.iter()
        .filter_map(|key| item.get(*key))
        .find_map(|value| {
            let number = match value {
                Value::Number(number) => number.as_f64(),
                Value::String(text) => text.trim().parse::<f64>().ok(),
                Value::Bool(flag) => Some(if *flag { 1.0 } else { 0.0 }),
                _ => None,
            }?;
            (number.is_finite() && number > 0.0).then_some(number.floor() as i64)
        })
}

/// `codex_cli_rs/{version} ({os} {release}; {arch})`. The os/arch spellings are
/// the official client's, not Rust's, because the string has to be
/// indistinguishable from it.
pub(super) fn user_agent(version: &str) -> String {
    static RELEASE: OnceLock<String> = OnceLock::new();
    let release = RELEASE.get_or_init(|| {
        std::process::Command::new("uname")
            .arg("-r")
            .output()
            .ok()
            .filter(|out| out.status.success())
            .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
            .unwrap_or_default()
    });
    let os_name = match std::env::consts::OS {
        "macos" => "Mac OS",
        "windows" => "Windows",
        "linux" => "Linux",
        other => other,
    };
    let arch = match std::env::consts::ARCH {
        "aarch64" => "arm64",
        "x86_64" => "x64",
        other => other,
    };
    format!("{ORIGINATOR}/{version} ({os_name} {release}; {arch})")
}

/// A bearer good enough to spend now: the stored one while it has life left,
/// otherwise a renewed one, persisted so the next process does not repeat the
/// round trip.
///
/// The renewal keeps the account id it was stored under. The service can hand
/// back a token for a migrated id, and following that migration is the login
/// command's job — a catalog read must not quietly re-key the credential store.
pub(super) async fn token(config_root: &Path) -> Option<Token> {
    token_for_credential(config_root, None).await
}

pub(super) async fn token_for_credential(
    config_root: &Path,
    expected: Option<&str>,
) -> Option<Token> {
    static REFRESH_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let _guard = REFRESH_LOCK.lock().await;
    let account = model_sources::codex_account(config_root)?;
    let identity = format!(
        "{}:{}",
        account.get("accountId")?.as_str()?,
        model_sources::account_credential_id("codex", &account)
    );
    if expected.is_some_and(|expected| expected != identity) {
        return None;
    }
    let account_id = account
        .get("accountId")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())?
        .to_string();
    let stored = account
        .get("accessToken")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let expires_at = account.get("accessTokenExpiresAt").and_then(Value::as_i64);
    let now = now_ms();
    if !stored.is_empty() && expires_at.is_none_or(|at| at - model_sources::REFRESH_BUFFER_MS > now)
    {
        return Some(Token {
            access_token: stored.to_string(),
            account_id,
        });
    }
    let refresh_token = account
        .get("refreshToken")
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty())?;
    let mut response = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_millis(FETCH_TIMEOUT_MS))
        .build()
        .ok()?
        .post(format!("{}/oauth/token", model_sources::codex_auth_base()))
        .header("content-type", "application/x-www-form-urlencoded")
        .body(format!(
            "grant_type=refresh_token&refresh_token={}&client_id={}&scope=openid+profile+email",
            form_encode(refresh_token),
            model_sources::codex_client_id()
        ))
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    const MAX_TOKEN_RESPONSE: usize = 64 * 1024;
    if response
        .content_length()
        .is_some_and(|len| len > MAX_TOKEN_RESPONSE as u64)
    {
        return None;
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.ok()? {
        if body.len().saturating_add(chunk.len()) > MAX_TOKEN_RESPONSE {
            return None;
        }
        body.extend_from_slice(&chunk);
    }
    let tokens: Value = serde_json::from_slice(&body).ok()?;
    let access_token = tokens
        .get("access_token")
        .and_then(Value::as_str)?
        .to_string();
    let next_refresh = tokens
        .get("refresh_token")
        .and_then(Value::as_str)
        .unwrap_or(refresh_token);
    let mut renewed = account.clone();
    renewed.insert("accessToken".into(), json!(access_token));
    renewed.insert("refreshToken".into(), json!(next_refresh));
    if let Some(id_token) = tokens
        .get("id_token")
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty())
    {
        renewed.insert("idToken".into(), json!(id_token));
    }
    if let Some(expires_in) = tokens.get("expires_in").and_then(Value::as_i64) {
        renewed.insert(
            "accessTokenExpiresAt".into(),
            json!(now + expires_in * 1000),
        );
    }
    renewed.insert("updatedAt".into(), json!(now));
    model_sources::write_refreshed_codex_login(config_root, &account, Value::Object(renewed))
        .ok()?;
    invalidate();
    Some(Token {
        access_token,
        account_id,
    })
}

fn form_encode(raw: &str) -> String {
    raw.bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (byte as char).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_millis() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    #[test]
    fn complete_internal_catalog_keeps_picker_seed_separate() {
        let public = super::bundled_models();
        let all = super::bundled_review_models();
        assert_eq!(all.len(), 11);
        assert_eq!(public.len(), 8);
        let cyber = all
            .iter()
            .find(|entry| entry["id"] == "gpt-daybreak-blue-latest")
            .unwrap();
        assert_eq!(cyber["modelSpecialty"], "cyber");
        assert_eq!(cyber["visibility"], "hide");
        assert_eq!(cyber["supportsImageDetailOriginal"], true);
        assert!(super::bundled_api_models()
            .iter()
            .any(|entry| entry["id"] == "gpt-5.5"));
    }

    #[test]
    fn internal_metadata_preserves_source_efforts_budget_and_specialty() {
        let entries = super::parse(
            &serde_json::json!({"models":[{"slug":"internal","visibility":"hide","model_specialty":"cyber","used_fallback_model_metadata":true,"effective_context_window_percent":-2,"supported_reasoning_levels":[{"effort":"none"},{"effort":"custom/Exact"}],"default_reasoning_level":"custom/Exact"}]}),
        );
        let limits = super::limits_from_entry(&entries[0]);
        assert_eq!(limits.model_specialty.as_deref(), Some("cyber"));
        assert_eq!(limits.effective_context_window_percent, Some(-2));
        assert!(limits.used_fallback_model_metadata);
        assert_eq!(limits.supported_reasoning_efforts, ["none", "custom/Exact"]);
        assert_eq!(
            limits.default_reasoning_effort.as_deref(),
            Some("custom/Exact")
        );
        assert!(limits.effort_levels.as_ref().unwrap().is_empty());
        let mut legacy =
            vec![serde_json::json!({"id":"gpt-daybreak-blue-latest","contextWindow":120000})];
        super::restore_legacy_review_metadata(&mut legacy);
        assert_eq!(legacy[0]["modelSpecialty"], "cyber");
        assert_eq!(legacy[0]["supportsImageDetailOriginal"], true);
        assert_eq!(legacy[0]["contextWindow"], 120000);
    }

    #[test]
    fn multi_agent_effort_metadata_accepts_both_catalog_spellings() {
        for key in ["multi_agent_reasoning_effort", "multiAgentReasoningEffort"] {
            let mut entry = serde_json::json!({"slug":"multi-agent-model"});
            entry[key] = serde_json::json!("custom/Exact");
            assert_eq!(
                super::limits_from_entry(&entry)
                    .multi_agent_reasoning_effort
                    .as_deref(),
                Some("custom/Exact")
            );
            let entries = super::parse(&serde_json::json!({"models":[entry]}));
            assert_eq!(entries[0]["multiAgentReasoningEffort"], "custom/Exact");
            assert_eq!(
                super::limits_from_entry(&entries[0])
                    .multi_agent_reasoning_effort
                    .as_deref(),
                Some("custom/Exact")
            );
        }
        for entry in [
            serde_json::json!({"slug":"multi-agent-model"}),
            serde_json::json!({"slug":"multi-agent-model","multi_agent_reasoning_effort":null}),
            serde_json::json!({"slug":"multi-agent-model","multiAgentReasoningEffort":42}),
        ] {
            assert!(super::limits_from_entry(&entry)
                .multi_agent_reasoning_effort
                .is_none());
            let entries = super::parse(&serde_json::json!({"models":[entry]}));
            assert!(super::limits_from_entry(&entries[0])
                .multi_agent_reasoning_effort
                .is_none());
        }
    }

    use super::*;

    fn store_codex_login(root: &Path, account_id: &str) {
        std::fs::write(
            root.join("openai-subscription-oauth.json"),
            json!({
                "defaultAccountId": "acc-1",
                "accounts": {
                    "acc-1": {
                        "accountId": account_id,
                        "accessToken": "at-fresh",
                        "refreshToken": "rt-fresh",
                        "accessTokenExpiresAt": now_ms() + 3_600_000,
                    }
                }
            })
            .to_string(),
        )
        .expect("credential write");
    }

    /// A session-resolution read with a disk copy must answer from it without
    /// touching the network: the endpoint is a refused loopback port, so any
    /// fetch would surface as an error rather than a slow answer.
    #[tokio::test]
    async fn a_warm_read_answers_from_the_disk_cache_without_the_network() {
        let root = tempfile::tempdir().expect("temp dir");
        store_codex_login(root.path(), "acc-disk");
        write_disk_cache(
            root.path(),
            "acc-disk",
            &model_sources::codex_client_version(),
            &[json!({ "id": "disk-model", "contextWindow": 123456 })],
        );
        invalidate();
        let models =
            models_with_read_at(root.path(), CatalogRead::Warm, "http://127.0.0.1:9/models")
                .await
                .expect("the disk copy answers");
        assert_eq!(models[0]["id"], json!("disk-model"));
        assert_eq!(models[0]["contextWindow"], json!(123456));
    }

    #[tokio::test]
    async fn legacy_warm_disk_catalog_restores_known_output_detail_offline() {
        let root = tempfile::tempdir().expect("temp dir");
        store_codex_login(root.path(), "acc-legacy-output-detail");
        let mut legacy = bundled_models();
        let known_count = legacy.len();
        assert!(known_count > 0);
        for (index, model) in legacy.iter_mut().enumerate() {
            let fields = model.as_object_mut().expect("model object");
            fields.remove("outputVerbosity");
            fields.insert("contextWindow".into(), json!(123000 + index));
        }
        legacy.push(json!({
            "id": "gpt-6.1-sol-unlisted",
            "contextWindow": 765432,
        }));
        let raw = json!({
            "accountId": "acc-legacy-output-detail",
            "clientVersion": model_sources::codex_client_version(),
            "models": legacy,
        })
        .to_string();
        std::fs::write(disk_cache_path(root.path()), &raw).expect("legacy cache write");
        invalidate();
        let models =
            models_with_read_at(root.path(), CatalogRead::Warm, "http://127.0.0.1:9/models")
                .await
                .expect("the legacy disk copy answers before refresh");
        assert_eq!(models.len(), legacy.len());
        for (index, model) in models.iter().take(known_count).enumerate() {
            assert_eq!(model["outputVerbosity"], json!("low"));
            let mut original_fields = model.clone();
            original_fields
                .as_object_mut()
                .unwrap()
                .remove("outputVerbosity");
            assert_eq!(original_fields, legacy[index]);
        }
        assert_eq!(models[known_count], legacy[known_count]);
        assert_eq!(
            std::fs::read_to_string(disk_cache_path(root.path())).unwrap(),
            raw,
            "only a successful live answer rewrites the cache"
        );
    }

    #[test]
    fn disk_cache_schema_keeps_authoritative_omission_and_explicit_output_detail() {
        let root = tempfile::tempdir().expect("temp dir");
        let version = model_sources::codex_client_version();
        let rows = vec![
            json!({"id": "gpt-6.1-sol", "contextWindow": 123456}),
            json!({"id": "gpt-6-astra", "outputVerbosity": "high"}),
            json!({"id": "gpt-6-sol", "support_verbosity": false}),
            json!({"id": "gpt-6-luna", "default_verbosity": "medium"}),
            json!({"id": "unknown-model", "contextWindow": 765432}),
        ];
        write_disk_cache(root.path(), "acc-output-schema", &version, &rows);
        let mut envelope: Value =
            serde_json::from_str(&std::fs::read_to_string(disk_cache_path(root.path())).unwrap())
                .unwrap();
        assert_eq!(envelope["schemaVersion"], json!(NORMALIZED_CATALOG_VERSION));
        assert_eq!(
            read_disk_cache(root.path(), "acc-output-schema", &version).unwrap(),
            rows,
            "current source omissions and overrides remain authoritative"
        );
        envelope.as_object_mut().unwrap().remove("schemaVersion");
        std::fs::write(disk_cache_path(root.path()), envelope.to_string()).unwrap();
        let restored = read_disk_cache(root.path(), "acc-output-schema", &version).unwrap();
        assert_eq!(restored[0]["outputVerbosity"], json!("low"));
        for original in &rows[1..] {
            let id = original["id"].as_str().unwrap();
            let current = restored.iter().find(|entry| entry["id"] == id).unwrap();
            for (key, value) in original.as_object().unwrap() {
                assert_eq!(current.get(key), Some(value));
            }
        }
        assert!(read_disk_cache(root.path(), "different-account", &version).is_none());
        assert!(read_disk_cache(root.path(), "acc-output-schema", "different-version").is_none());
    }

    /// Signed in but with nothing cached at all, the bundled catalog answers:
    /// a first start on a dead network still resolves a provider, and the
    /// revalidation replaces the bundled window once the service answers.
    #[tokio::test]
    async fn a_signed_in_read_with_no_cache_falls_back_to_the_bundled_catalog() {
        let root = tempfile::tempdir().expect("temp dir");
        store_codex_login(root.path(), "acc-bundled");
        invalidate();
        let models =
            models_with_read_at(root.path(), CatalogRead::Warm, "http://127.0.0.1:9/models")
                .await
                .expect("the bundled catalog answers");
        assert!(!models.is_empty(), "the build ships a catalog");
        assert!(
            models
                .iter()
                .all(|model| model.get("id").and_then(Value::as_str).is_some()),
            "every bundled row carries an id"
        );
    }

    /// Signed out is still an error, not a bundled answer: offering models no
    /// account can run would sign the session into nothing.
    #[tokio::test]
    async fn a_signed_out_read_names_the_missing_credential() {
        let root = tempfile::tempdir().expect("temp dir");
        invalidate();
        let error =
            models_with_read_at(root.path(), CatalogRead::Warm, "http://127.0.0.1:9/models")
                .await
                .expect_err("signed out must not resolve");
        assert!(error.contains("not signed in"), "{error}");
    }

    #[test]
    fn parse_declares_output_detail_only_when_supported() {
        let models = parse(&json!({"models": [
            {"id":"supported","support_verbosity":true},
            {"id":"source-default","support_verbosity":true,"default_verbosity":"high"},
            {"id":"explicit","outputVerbosity":"medium"},
            {"id":"disabled","support_verbosity":false,"outputVerbosity":"high"},
            {"id":"gpt-6.1-sol","default_verbosity":"low"},
            {"id":"invalid-default","support_verbosity":true,"default_verbosity":"verbose"}
        ]}));
        let find = |id: &str| models.iter().find(|model| model["id"] == id).unwrap();
        assert_eq!(find("supported")["outputVerbosity"], json!("low"));
        assert_eq!(find("source-default")["outputVerbosity"], json!("high"));
        assert_eq!(find("explicit")["outputVerbosity"], json!("medium"));
        assert!(find("disabled").get("outputVerbosity").is_none());
        assert!(find("gpt-6.1-sol").get("outputVerbosity").is_none());
        assert_eq!(find("invalid-default")["outputVerbosity"], json!("low"));
    }

    #[test]
    fn parse_retains_hidden_models_and_picker_filtering_preserves_service_order() {
        let body = json!({
            "models": [
                { "slug": "gpt-5.6-sol", "display_name": "Sol", "context_window": 400000, "priority": 1 },
                { "id": "hidden", "visibility": "none", "context_window": 1000, "priority": 2 },
                { "id": "gpt-5.6-luna", "max_input_tokens": 272000, "priority": 3 },
            ]
        });
        let models = parse(&body);
        let ids: Vec<&str> = models
            .iter()
            .filter_map(|model| model.get("id").and_then(Value::as_str))
            .collect();
        assert_eq!(ids, vec!["gpt-5.6-sol", "hidden", "gpt-5.6-luna"]);
        let visible: Vec<_> = models
            .iter()
            .filter(|model| picker_visible(model))
            .collect();
        assert_eq!(visible.len(), 2);
        assert_eq!(visible[0]["name"], json!("Sol"));
        assert_eq!(visible[0]["contextWindow"], json!(400000));
        assert_eq!(visible[1]["contextWindow"], json!(272000));
    }

    #[tokio::test]
    async fn conversation_choices_exclude_internal_rows_from_the_complete_catalog() {
        let root = tempfile::tempdir().unwrap();
        let account = format!("catalog-{}", root.path().display());
        store_codex_login(root.path(), &account);
        let rows = parse(&json!({"models":[
            {"id":"visible","visibility":"list"},
            {"id":"internal","visibility":"hide"}
        ]}));
        write_disk_cache(
            root.path(),
            &account,
            &model_sources::codex_client_version(),
            &rows,
        );
        let models = models_with_read(root.path(), CatalogRead::Warm)
            .await
            .unwrap();
        assert_eq!(models.len(), 1);
        assert_eq!(models[0]["id"], "visible");
        let complete = all_models(root.path()).await.unwrap();
        assert_eq!(complete, rows);
    }

    /// A picker paints rows in the backend's priority order, and it branches on
    /// the reasoning keys being present at all — so an absent ladder must be an
    /// absent key, and both wire spellings have to land on the same field.
    #[test]
    fn parse_sorts_by_priority_and_carries_the_reasoning_ladder() {
        let body = json!([
            { "id": "late", "priority": 9 },
            { "id": "unranked" },
            {
                "id": "first",
                "priority": 1,
                "default_reasoning_level": "medium",
                "supported_reasoning_levels": ["LOW", { "effort": "High" }, "low"],
            },
        ]);
        let models = parse(&body);
        let ids: Vec<&str> = models
            .iter()
            .filter_map(|model| model.get("id").and_then(Value::as_str))
            .collect();
        assert_eq!(ids, vec!["first", "late", "unranked"]);
        assert_eq!(models[0]["defaultReasoning"], json!("medium"));
        assert_eq!(
            models[0]["supportedReasoningLevels"],
            json!(["low", "high"]),
            "lowercased, de-duplicated, in order"
        );
        assert!(
            models[1].get("supportedReasoningLevels").is_none(),
            "no ladder means no key: {}",
            models[1]
        );
    }

    #[test]
    fn parse_replaces_a_duplicate_in_place() {
        let body = json!({
            "data": [
                { "id": "a", "context_window": 1, "priority": 1 },
                { "id": "b", "context_window": 2, "priority": 2 },
                { "id": "a", "context_window": 3, "priority": 1 },
            ]
        });
        let models = parse(&body);
        let ids: Vec<&str> = models
            .iter()
            .filter_map(|model| model.get("id").and_then(Value::as_str))
            .collect();
        assert_eq!(ids, vec!["a", "b"]);
        assert_eq!(models[0]["contextWindow"], json!(3));
    }

    #[test]
    fn a_zero_or_negative_window_is_not_a_window() {
        let body =
            json!({ "models": [{ "id": "a", "context_window": 0, "max_context_window": -5 }] });
        let models = parse(&body);
        assert!(models[0].get("contextWindow").is_none());
        assert!(models[0].get("maxContextWindow").is_none());
    }

    /// The backend has sent windows as strings; dropping one for its spelling
    /// would silently size the session to the provider default instead.
    #[test]
    fn a_numeric_string_window_still_counts() {
        let body = json!({ "models": [{ "id": "a", "context_window": " 272000 " }] });
        assert_eq!(parse(&body)[0]["contextWindow"], json!(272000));
    }

    /// The user agent has to read like the official client's, including the
    /// Node-style os/arch spellings.
    #[test]
    fn user_agent_uses_the_official_client_shape() {
        let agent = user_agent("1.2.3");
        assert!(agent.starts_with("codex_cli_rs/1.2.3 ("), "{agent}");
        assert!(agent.ends_with(')'), "{agent}");
        assert!(!agent.contains("aarch64"), "{agent}");
        assert!(!agent.contains("x86_64"), "{agent}");
    }

    /// A one-shot HTTP server whose response is explicitly released by the test.
    async fn one_shot_server(
        body: String,
    ) -> (
        String,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("stub bind");
        let addr = listener.local_addr().expect("local addr");
        let length = body.len();
        let (release, response_ready) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut buf = [0u8; 4096];
            let mut head = Vec::new();
            loop {
                use tokio::io::AsyncReadExt;
                let read = socket.read(&mut buf).await.unwrap_or(0);
                assert_ne!(read, 0, "the request head must be complete");
                head.extend_from_slice(&buf[..read]);
                if head.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            response_ready.await.expect("response released");
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n{body}"
            );
            use tokio::io::AsyncWriteExt;
            socket.write_all(response.as_bytes()).await.expect("write");
            let _ = socket.shutdown().await;
        });
        (format!("http://{addr}/models"), release, handle)
    }

    fn signed_in_store() -> Value {
        json!({
            "defaultAccountId": "acct-1",
            "accounts": {
                "acct-1": {
                    "accountId": "acct-1",
                    "accessToken": "live-token",
                    "accessTokenExpiresAt": now_ms() + 3_600_000,
                }
            }
        })
    }

    async fn wait_for_persisted_refresh(root: &Path, expected: &str) {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let persisted =
                    read_disk_cache(root, "acct-1", &model_sources::codex_client_version());
                let refreshing = reverify_state()
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .inflight;
                if !refreshing
                    && persisted
                        .as_ref()
                        .and_then(|models| models.first())
                        .and_then(|model| model["id"].as_str())
                        == Some(expected)
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("background refresh must finish and persist its catalog");
    }

    #[test]
    fn default_service_endpoint_cannot_mark_a_background_refresh_inflight_in_tests() {
        assert!(
            std::env::var_os("KLAUS_CODEX_MODELS_URL").is_none(),
            "default-endpoint fixture requires no explicit endpoint override"
        );
        let endpoint = "https://chatgpt.com/backend-api/codex/models";
        assert!(!fetch_allowed(endpoint));
        assert!(fetch_allowed("http://127.0.0.1:9/models"));
        let before = {
            let state = reverify_state()
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            (state.inflight, state.retry_at)
        };
        assert!(!before.0, "the fixture starts without a refresh in flight");
        schedule_reverify(
            Path::new("/unused-catalog-fixture"),
            "fixture-account",
            "fixture-version",
            endpoint,
        );
        let state = reverify_state()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        assert_eq!((state.inflight, state.retry_at), before);
    }

    /// Cold reads use the bundle before a live response is available. A completed
    /// background refresh persists it for the next process, whose disk-backed
    /// read also returns before the next refresh response is released.
    #[tokio::test]
    async fn a_cached_read_answers_from_disk_and_refreshes_behind_the_caller() {
        let root = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            model_sources::codex_store_path(root.path()),
            serde_json::to_string(&signed_in_store()).expect("store json"),
        )
        .expect("store write");
        invalidate();

        let live = json!({ "models": [{ "id": "gpt-live", "priority": 1 }] });
        let (url, release, server) = one_shot_server(live.to_string()).await;
        let models = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            models_with_read_at(root.path(), CatalogRead::Cached, &url),
        )
        .await
        .expect("a cold cached read must not await the live response")
        .expect("the bundle answers the first cached read");
        assert_eq!(models, bundled_models());
        release.send(()).expect("release first response");
        tokio::time::timeout(std::time::Duration::from_secs(10), server)
            .await
            .expect("first server completes")
            .expect("server join");
        wait_for_persisted_refresh(root.path(), "gpt-live").await;
        let disk = std::fs::read_to_string(disk_cache_path(root.path())).expect("disk cache");
        assert!(disk.contains("gpt-live"), "{disk}");

        // Relaunch: the memory cache is gone while the disk copy stands in for
        // a catalog fetched in an earlier process.
        invalidate();
        let fresh = json!({ "models": [{ "id": "gpt-fresh", "priority": 1 }] });
        let (url2, release2, server2) = one_shot_server(fresh.to_string()).await;
        let stale = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            models_with_read_at(root.path(), CatalogRead::Cached, &url2),
        )
        .await
        .expect("a disk-backed read must not await the live response")
        .expect("stale read");
        assert_eq!(stale[0]["id"], json!("gpt-live"), "answers from disk");

        release2.send(()).expect("release second response");
        tokio::time::timeout(std::time::Duration::from_secs(10), server2)
            .await
            .expect("refresh server completes")
            .expect("refresh server join");
        wait_for_persisted_refresh(root.path(), "gpt-fresh").await;
        let fresh = models_with_read_at(root.path(), CatalogRead::Cached, &url2)
            .await
            .expect("warm memory answers");
        assert_eq!(fresh[0]["id"], json!("gpt-fresh"));
        let disk = std::fs::read_to_string(disk_cache_path(root.path())).expect("disk cache");
        assert!(disk.contains("gpt-fresh"), "{disk}");
        invalidate();
    }

    #[test]
    fn offline_review_catalog_retains_required_metadata_without_exposing_hidden_rows() {
        let root = tempfile::tempdir().unwrap();
        let account = format!("review-catalog-{}", uuid::Uuid::new_v4());
        store_codex_login(root.path(), &account);
        let rows = parse(&json!({"models":[
            {"slug":"visible","visibility":"list","auto_review_model_override":"review-private","requires_sandboxed_review":true,"node_repl_auto_review_required":true,"guardian":{"shell":"adaptive"},"model_messages":{"guardian_v2":{"risk_score_template":"fixture"}},"comp_hash":"revision-3"},
            {"slug":"review-private","visibility":"hide","default_reasoning_level":"medium","supported_reasoning_levels":[{"effort":"low"},{"effort":"medium"}],"supports_reasoning_summary_parameter":false,"default_reasoning_summary":"none","context_window":272000}
        ]}));
        write_disk_cache(
            root.path(),
            &account,
            &model_sources::codex_client_version(),
            &rows,
        );
        let offline = all_models_offline(root.path(), &account);
        assert_eq!(offline.len(), 2);
        assert_eq!(
            offline.iter().filter(|entry| picker_visible(entry)).count(),
            1
        );
        let visible = limits_from_entry(
            offline
                .iter()
                .find(|entry| entry["id"] == "visible")
                .unwrap(),
        );
        assert_eq!(
            visible.auto_review_model_override.as_deref(),
            Some("review-private")
        );
        assert!(visible.requires_sandboxed_review);
        assert!(visible.node_repl_auto_review_required);
        assert_eq!(visible.guardian, Some(json!({"shell":"adaptive"})));
        assert_eq!(visible.comp_hash.as_deref(), Some("revision-3"));
        let hidden = limits_from_entry(
            offline
                .iter()
                .find(|entry| entry["id"] == "review-private")
                .unwrap(),
        );
        assert_eq!(
            hidden.effort_default,
            Some(klaus_agent_core::schemas::effort::EffortLevel::Medium)
        );
        assert_eq!(hidden.supports_reasoning_summary_parameter, Some(false));
        assert_eq!(hidden.default_reasoning_summary, Some(json!("none")));
        assert!(all_models_offline(root.path(), "another-account").is_empty());
        store_codex_login(root.path(), "replaced-account");
        assert!(all_models_offline(root.path(), &account).is_empty());
    }
}
