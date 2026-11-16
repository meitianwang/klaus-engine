//! The public Gemini chat lineup served through the shared direct-cloud account.
//!
//! Account pool, credential refresh, transport identity and the local egress
//! relay are shared with the Antigravity source: one Google login on one
//! backend, one quota pool. This module owns what is source-specific — the
//! catalog projection to public Gemini chat identities, its own local-first
//! snapshot and change notices, the `gemini_*` error vocabulary, and the
//! `gemini/...` facade behaviour.

mod catalog;
mod cloud;
#[cfg(test)]
mod tests;

use super::antigravity;
use klaus_agent_core::provider::gemini::{
    GeminiAdapterPolicy, GeminiProvider, GeminiProviderOptions, GeminiThinkingDialect,
};
use klaus_agent_core::provider::retry::RetryOptions;
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeString};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::ModelProvider;
use klaus_agent_protocol::types::{GeminiThinkingInit, ProtocolSessionInit};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

static CONFIG_ROOT: RwLock<Option<PathBuf>> = RwLock::new(None);
static CATALOG_PUBLISH_FENCE: Mutex<()> = Mutex::new(());
const CATALOG_TTL_MS: i64 = 5 * 60 * 1000;
const CATALOG_RETRY_COOLDOWN_MS: i64 = 60 * 1000;
const SNAPSHOT_FILE: &str = "gemini-models.json";

pub fn set_config_root(root: &Path) {
    let _publish = CATALOG_PUBLISH_FENCE
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    let mut current = CONFIG_ROOT.write().unwrap_or_else(|p| p.into_inner());
    if current.as_deref() != Some(root) {
        *current = Some(root.to_path_buf());
        drop(current);
        cache().lock().unwrap_or_else(|p| p.into_inner()).clear();
        refresh_state()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clear();
    }
}

fn config_root() -> PathBuf {
    CONFIG_ROOT
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .clone()
        .unwrap_or_else(|| {
            std::env::var("KLAUS_CONFIG_DIR")
                .ok()
                .filter(|root| !root.trim().is_empty())
                .map(PathBuf::from)
                .unwrap_or_else(|| {
                    PathBuf::from(std::env::var("HOME").unwrap_or_default()).join(".klaus")
                })
        })
}

#[derive(Clone)]
struct CachedCatalog {
    payload: Value,
    fetched_at: i64,
    from_disk: bool,
}

fn cache() -> &'static Mutex<HashMap<String, CachedCatalog>> {
    static CACHE: OnceLock<Mutex<HashMap<String, CachedCatalog>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn refresh_lock() -> Arc<tokio::sync::Mutex<()>> {
    static LOCK: OnceLock<Arc<tokio::sync::Mutex<()>>> = OnceLock::new();
    LOCK.get_or_init(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone()
}

fn refresh_state() -> &'static Mutex<HashMap<String, i64>> {
    static STATE: OnceLock<Mutex<HashMap<String, i64>>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Re-pin the local snapshot to the current head of the shared account pool.
/// Called after every credential-pool mutation this source surfaces.
pub(crate) fn sync_head_catalog() {
    let root = config_root();
    let Some(head) = antigravity::auth::generation() else {
        forget_catalog();
        return;
    };
    let should_notify = {
        let _publish = CATALOG_PUBLISH_FENCE
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let valid = antigravity::auth::generations();
        cache()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|generation, _| valid.contains(generation));
        refresh_state()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|generation, _| valid.contains(generation));
        let path = root.join(SNAPSHOT_FILE);
        let existing_gen = std::fs::read(&path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
            .and_then(|value| {
                value
                    .get("generation")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            });
        if let Some(cached) = cache()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&head)
            .cloned()
        {
            let snapshot = json!({
                "generation": head,
                "payload": cached.payload,
                "fetchedAt": cached.fetched_at,
                "clientIdentity": antigravity::cloud::client_identity(),
            });
            if let Ok(bytes) = serde_json::to_vec(&snapshot) {
                let _ = std::fs::write(path, bytes);
            }
            existing_gen.as_deref() != Some(head.as_str())
        } else {
            if existing_gen.as_deref().is_some_and(|gen| gen != head) {
                let _ = std::fs::remove_file(path);
            }
            existing_gen.as_deref().is_some_and(|gen| gen != head)
        }
    };
    if should_notify {
        super::model_sources::notify_catalog_changed(root, "gemini");
    }
}

fn forget_catalog() {
    let _publish = CATALOG_PUBLISH_FENCE
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    cache().lock().unwrap_or_else(|p| p.into_inner()).clear();
    refresh_state()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clear();
    let _ = std::fs::remove_file(config_root().join(SNAPSHOT_FILE));
}

fn cached_payload(generation: &str) -> Option<CachedCatalog> {
    if let Some(cached) = cache()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(generation)
        .cloned()
    {
        return Some(cached);
    }
    let path = config_root().join(SNAPSHOT_FILE);
    if std::fs::metadata(&path).ok()?.len() > 8 * 1024 * 1024 {
        return None;
    }
    let snapshot: Value = serde_json::from_slice(&std::fs::read(path).ok()?).ok()?;
    if snapshot["generation"].as_str()? != generation
        || snapshot["clientIdentity"].as_str()? != antigravity::cloud::client_identity()
    {
        return None;
    }
    Some(CachedCatalog {
        payload: snapshot.get("payload")?.clone(),
        fetched_at: snapshot.get("fetchedAt")?.as_i64()?,
        from_disk: true,
    })
}

async fn fetch_available_models(
    generation: &str,
    project: &str,
    signal: &AbortSignal,
) -> Result<Value, String> {
    let body = json!({ "project": project });
    let result =
        match antigravity::cloud::rpc("fetchAvailableModels", body, Some(generation), signal).await
        {
            // A dead cached project would pin the catalog to its last good
            // snapshot forever; resolve a fresh project and ask once more.
            Err(message) if antigravity::is_not_found_message(&message) => {
                let healed = antigravity::heal_project(generation, signal)
                    .await
                    .map_err(|error| cloud::retag(&error))?;
                antigravity::cloud::rpc(
                    "fetchAvailableModels",
                    json!({ "project": healed }),
                    Some(generation),
                    signal,
                )
                .await
            }
            result => result,
        };
    result.map_err(|error| cloud::retag(&error))
}

async fn refresh_catalog(generation: &str) -> Result<Value, String> {
    let root = config_root();
    let signal = AbortSignal::new();
    let project = antigravity::project(Some(generation), &signal)
        .await
        .map_err(|error| cloud::retag(&error))?;
    let payload = fetch_available_models(generation, &project, &signal).await?;
    if !payload.get("models").is_some_and(Value::is_object) {
        return Err("gemini_catalog_invalid".into());
    }
    let payload = catalog::project_payload(&payload);
    // Publishing is fenced by the same lock credential adoption and logout
    // use: a refresh that finishes after either event must not recreate the
    // snapshot they just invalidated or write it under a different
    // configuration root.
    let _credential = antigravity::auth::credential_lock().lock().await;
    let changed = {
        let _publish = CATALOG_PUBLISH_FENCE
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if !antigravity::auth::is_current(generation) {
            return Err("gemini_credential_changed".into());
        }
        if config_root() != root {
            return Err("gemini_credential_changed".into());
        }
        let changed = cached_payload(generation)
            .map(|cached| cached.payload)
            .as_ref()
            != Some(&payload);
        let fetched_at = chrono::Utc::now().timestamp_millis();
        cache().lock().unwrap_or_else(|p| p.into_inner()).insert(
            generation.to_string(),
            CachedCatalog {
                payload: payload.clone(),
                fetched_at,
                from_disk: false,
            },
        );
        let is_head = antigravity::auth::is_head_generation(generation);
        if is_head {
            let snapshot = json!({
                "generation": generation,
                "payload": payload,
                "fetchedAt": fetched_at,
                "clientIdentity": antigravity::cloud::client_identity(),
            });
            let path = root.join(SNAPSHOT_FILE);
            if let Ok(bytes) = serde_json::to_vec(&snapshot) {
                let _ = std::fs::write(path, bytes);
            }
        }
        changed && is_head
    };
    drop(_credential);
    if changed {
        super::model_sources::notify_catalog_changed(root, "gemini");
    }
    Ok(payload)
}

fn refresh_allowed(generation: &str, now: i64) -> bool {
    let state = refresh_state().lock().unwrap_or_else(|p| p.into_inner());
    state.get(generation).copied().unwrap_or(0) <= now
}

fn record_refresh_failure(generation: &str, now: i64) {
    if !antigravity::auth::is_current(generation) {
        return;
    }
    refresh_state()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(
            generation.to_string(),
            now.saturating_add(CATALOG_RETRY_COOLDOWN_MS),
        );
}

fn record_refresh_success(generation: &str) {
    refresh_state()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .remove(generation);
}

/// Revalidate behind the caller. A catalog read never waits for this task,
/// and the refresh lock makes concurrent picker reads share one request. A
/// failed fetch is not retried until the cooldown passes.
fn schedule_refresh(generation: String) {
    if !refresh_allowed(&generation, chrono::Utc::now().timestamp_millis()) {
        return;
    }
    let Ok(permit) = refresh_lock().try_lock_owned() else {
        return;
    };
    tokio::spawn(async move {
        let _permit = permit;
        match tokio::time::timeout(
            std::time::Duration::from_secs(15),
            refresh_catalog(&generation),
        )
        .await
        {
            Ok(Ok(_)) => record_refresh_success(&generation),
            Err(_) => record_refresh_failure(&generation, chrono::Utc::now().timestamp_millis()),
            Ok(Err(_)) => {
                record_refresh_failure(&generation, chrono::Utc::now().timestamp_millis())
            }
        }
    });
}

/// A local-first catalog read: memory for the current generation, then the
/// identity-bound disk snapshot (age decides revalidation, not usability),
/// then the sanitized bundled baseline while entitlement stays unknown.
async fn catalog_payload(_warm: bool) -> Result<Value, String> {
    let generation = antigravity::auth::generation().ok_or("gemini_not_signed_in")?;
    if let Some(cached) = cached_payload(&generation) {
        if cached.from_disk
            || chrono::Utc::now().timestamp_millis() - cached.fetched_at > CATALOG_TTL_MS
        {
            schedule_refresh(generation);
        }
        return Ok(cached.payload);
    }
    let bundled = catalog::bundled_payload();
    if bundled
        .get("models")
        .and_then(Value::as_object)
        .is_none_or(|models| models.is_empty())
    {
        schedule_refresh(generation);
        return Err("gemini_catalog_not_ready".into());
    }
    schedule_refresh(generation);
    Ok(bundled)
}

pub async fn models(warm: bool) -> Result<Value, String> {
    // Snapshot staleness compares the recorded client identity synchronously;
    // resolve it off the async worker pool before any catalog read.
    antigravity::cloud::ensure_identity().await;
    let generation = antigravity::auth::generation().ok_or("gemini_not_signed_in")?;
    let payload = catalog_payload(warm).await?;
    if !antigravity::auth::is_current(&generation) {
        return Err("gemini_credential_changed".into());
    }
    let mut reply = super::conversation_catalog::response("gemini", catalog::models_rows(&payload));
    reply["catalogBaseline"] =
        json!(payload.get("catalogBaseline").and_then(Value::as_bool) == Some(true));
    Ok(reply)
}

pub fn status() -> Value {
    antigravity::auth::status()
}

pub async fn adopt(credential: antigravity::auth::Credential) -> Result<Value, String> {
    let outcome = antigravity::auth::adopt(credential)
        .await
        .map_err(|error| cloud::retag(&error))?;
    sync_head_catalog();
    Ok(outcome)
}

pub async fn logout_account(account_id: Option<&str>) -> Result<Value, String> {
    let outcome = antigravity::auth::logout_account(account_id)
        .await
        .map_err(|error| cloud::retag(&error))?;
    sync_head_catalog();
    Ok(outcome)
}

pub async fn switch_account(account_id: &str) -> Result<Value, String> {
    let outcome = antigravity::auth::switch_account(account_id)
        .await
        .map_err(|error| cloud::retag(&error))?;
    sync_head_catalog();
    Ok(outcome)
}

/// Project the shared account's usage down to this source's shared Gemini
/// pool: keep only windows for public Gemini identities (plus gemini-labelled
/// pool windows when the service groups them) and merge the duplicated
/// per-model buckets into one worst-remaining pool row.
pub async fn usage() -> Result<Value, String> {
    let payload = antigravity::usage()
        .await
        .map_err(|error| cloud::retag(&error))?;
    Ok(filter_usage(payload))
}

fn filter_usage(mut payload: Value) -> Value {
    if let Some(rows) = payload.get_mut("accounts").and_then(Value::as_array_mut) {
        for row in rows {
            filter_usage_row(row);
        }
    }
    filter_usage_row(&mut payload);
    payload
}

fn filter_usage_row(row: &mut Value) {
    let Some(windows) = row.get("windows").and_then(Value::as_array).cloned() else {
        return;
    };
    let owned: Vec<Value> = windows
        .iter()
        .filter(|window| {
            window
                .get("modelId")
                .and_then(Value::as_str)
                .is_some_and(catalog::is_public_chat_id)
        })
        .cloned()
        .collect();
    let kept = if owned.is_empty() {
        windows
            .iter()
            .filter(|window| {
                window.get("modelId").is_none()
                    && [window.get("groupLabel"), window.get("label")]
                        .into_iter()
                        .flatten()
                        .filter_map(Value::as_str)
                        .any(|label| label.to_ascii_lowercase().contains("gemini"))
            })
            .cloned()
            .collect::<Vec<_>>()
    } else {
        owned
    };
    match merge_pool_window(kept) {
        Some(window) => row["windows"] = json!([window]),
        None => {
            if let Some(object) = row.as_object_mut() {
                object.remove("windows");
            }
        }
    }
}

fn merge_pool_window(windows: Vec<Value>) -> Option<Value> {
    let mut base = windows.first()?.clone();
    let mut worst_fraction: Option<f64> = None;
    let mut earliest_reset: Option<i64> = None;
    for window in &windows {
        if let Some(fraction) = window.get("remainingFraction").and_then(Value::as_f64) {
            worst_fraction = Some(worst_fraction.map_or(fraction, |current| current.min(fraction)));
        }
        if let Some(reset) = window.get("resetsAt").and_then(Value::as_i64) {
            earliest_reset = Some(earliest_reset.map_or(reset, |current| current.min(reset)));
        }
    }
    let object = base.as_object_mut()?;
    object.insert("id".into(), json!("gemini:shared-pool"));
    object.insert("label".into(), json!("Gemini"));
    object.remove("modelId");
    object.remove("bucketId");
    if let Some(fraction) = worst_fraction {
        let fraction = fraction.clamp(0.0, 1.0);
        object.insert("remainingFraction".into(), json!(fraction));
        object.insert("remainingPercent".into(), json!(fraction * 100.0));
        object.insert("usedPercent".into(), json!((1.0 - fraction) * 100.0));
    }
    if let Some(reset) = earliest_reset {
        object.insert("resetsAt".into(), json!(reset));
    }
    Some(base)
}

pub async fn main_model(
    selection: &str,
    tiers: &serde_json::Map<String, Value>,
) -> Result<String, String> {
    let requested = match selection {
        "" | "auto" | "high" | "medium" | "low" => tiers
            .get(selection)
            .and_then(Value::as_str)
            .filter(|model| !model.is_empty()),
        model => Some(model),
    };
    if let Some(model) = requested {
        return Ok(model.strip_prefix("gemini:").unwrap_or(model).to_string());
    }
    let models = catalog::models_rows(&catalog_payload(true).await?);
    models
        .iter()
        .filter(|model| catalog::is_chat_model(model))
        .find(|model| model.get("disabled").and_then(Value::as_bool) != Some(true))
        .and_then(|model| model["key"].as_str())
        .map(str::to_string)
        .ok_or_else(|| "gemini_no_models".into())
}

pub async fn resolve(
    selection: &str,
    tiers: &serde_json::Map<String, Value>,
) -> Result<super::model_sources::Resolved, String> {
    antigravity::auth::active_or_rotated_credential()
        .await
        .map_err(|error| cloud::retag(&error))?;
    let model = main_model(selection, tiers).await?;
    let rows = catalog::models_rows(&catalog_payload(true).await?);
    let selected = rows
        .iter()
        .find(|entry| entry["key"].as_str() == Some(&model))
        .ok_or("gemini_model_unavailable")?;
    if !catalog::is_chat_model(selected) {
        return Err("gemini_model_purpose_unsupported".into());
    }
    let by_model: Vec<Value> = rows
        .iter()
        .filter(|entry| entry["key"].as_str() != Some(&model) && catalog::is_chat_model(entry))
        .map(|entry| json!({"model": entry["key"], "capabilities": entry["capabilities"]}))
        .collect();
    let mut model_tiers = serde_json::Map::new();
    for tier in ["high", "medium", "low"] {
        if let Some(pick) = tiers.get(tier).and_then(Value::as_str) {
            let pick = pick.strip_prefix("gemini:").unwrap_or(pick);
            if rows
                .iter()
                .any(|entry| entry["key"].as_str() == Some(pick) && catalog::is_chat_model(entry))
            {
                model_tiers.insert(tier.into(), json!(pick));
            }
        }
    }
    Ok(super::model_sources::Resolved {
        provider: json!({"type": "gemini", "model": model}),
        auth: Value::Null,
        capabilities: selected["capabilities"].clone(),
        capabilities_by_model: by_model,
        model_tiers,
        pricing: None,
    })
}

fn dialect(init: Option<&GeminiThinkingInit>) -> GeminiThinkingDialect {
    match init {
        Some(init) if init.mode == "budget" => GeminiThinkingDialect::Budget {
            min: init.min.unwrap_or(0),
            max: init.max.unwrap_or(32_768),
            can_disable: init.can_disable,
        },
        _ => GeminiThinkingDialect::Level,
    }
}

fn model_details(generation: &str, model: &str) -> Option<Value> {
    let payload = cached_payload(generation)
        .map(|cached| cached.payload)
        .unwrap_or_else(catalog::bundled_payload);
    catalog::model_details(&payload, model)
}

fn selected_dialect(model: &RuntimeString, init: &ProtocolSessionInit) -> GeminiThinkingDialect {
    let discovered = antigravity::auth::generation().and_then(|generation| {
        let model = model.try_to_utf8().ok()?;
        let metadata = model_details(&generation, &model)?;
        let capabilities = antigravity::catalog::model_capabilities(&metadata);
        let thinking = capabilities.get("geminiThinking")?;
        runtime_codec::decode_text::<GeminiThinkingInit>(&RuntimeString::from(thinking.to_string()))
            .ok()
    });
    if let Some(discovered) = discovered.as_ref() {
        return dialect(Some(discovered));
    }
    let declared = init
        .capabilities_by_model
        .iter()
        .flatten()
        .find(|entry| entry.model == *model)
        .and_then(|entry| entry.capabilities.gemini_thinking.as_ref())
        .or_else(|| {
            init.capabilities
                .as_ref()
                .and_then(|caps| caps.gemini_thinking.as_ref())
        });
    dialect(declared)
}

pub(crate) fn decorate(
    provider: Arc<dyn ModelProvider>,
    init: &ProtocolSessionInit,
) -> Arc<dyn ModelProvider> {
    let model = provider.info().default_model.clone();
    let capabilities = provider.model_metadata(&model);
    let mut by_model: HashMap<_, _> = provider
        .offline_model_catalog()
        .into_iter()
        .filter_map(|(id, caps)| Some((id.try_to_utf8().ok()?, caps)))
        .collect();
    for entry in init.capabilities_by_model.iter().flatten() {
        if let Ok(id) = entry.model.try_to_utf8() {
            by_model.insert(id, provider.model_metadata(&entry.model));
        }
    }
    let thinking_dialects_by_model = by_model
        .keys()
        .map(|id| {
            (
                id.clone(),
                selected_dialect(&RuntimeString::from(id.as_str()), init),
            )
        })
        .collect();
    let conversation_id = init
        .thread_id
        .as_ref()
        .and_then(|id| id.try_to_utf8().ok())
        .filter(|id| !id.trim().is_empty())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let admission_generations = antigravity::auth::generations();
    let multi_account = admission_generations.len() > 1;
    Arc::new(GeminiProvider::new(GeminiProviderOptions {
        default_model: model.try_to_utf8().ok(),
        client: Some(Arc::new(cloud::RetaggingClient::pooled(conversation_id))),
        retry: Some(RetryOptions {
            yield_to_credential_rotation: multi_account,
            ..Default::default()
        }),
        stream_admission: Some(Arc::new(move || {
            admission_generations
                .iter()
                .any(|generation| antigravity::auth::is_current(generation))
        })),
        capabilities: Some(capabilities),
        capabilities_by_model: Some(by_model),
        thinking_dialect: Some(selected_dialect(&model, init)),
        thinking_dialects_by_model: Some(thinking_dialects_by_model),
        minimal_reasoning_models: None,
        adapter_policy: GeminiAdapterPolicy {
            replay_signed_parts: true,
            scope_signatures_to_model: true,
            honor_tools_capability: true,
            model_tool_budget: true,
        },
        ..Default::default()
    }))
}
