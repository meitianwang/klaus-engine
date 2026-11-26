//! Antigravity account access through its direct cloud model service.

mod audio;
pub mod auth;
mod cache_diagnostics;
mod catalog;
mod cloud;
mod egress;
pub mod login;
mod sanitize;
mod usage;

use klaus_agent_core::provider::gemini::{
    GeminiAdapterPolicy, GeminiProvider, GeminiProviderOptions, GeminiThinkingDialect,
};
use klaus_agent_core::provider::retry::RetryOptions;
use klaus_agent_core::provider::rotation::RotateOutcome;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::ModelProvider;
use klaus_agent_protocol::types::{GeminiThinkingInit, ProtocolSessionInit};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

static CONFIG_ROOT: RwLock<Option<PathBuf>> = RwLock::new(None);
static CATALOG_PUBLISH_FENCE: Mutex<()> = Mutex::new(());
#[cfg(test)]
static REFRESH_COMPLETIONS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
const CATALOG_TTL_MS: i64 = 5 * 60 * 1000;
const CATALOG_RETRY_COOLDOWN_MS: i64 = 60 * 1000;

pub fn set_config_root(root: &Path) {
    let _publish = CATALOG_PUBLISH_FENCE
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    let mut current = CONFIG_ROOT.write().unwrap_or_else(|p| p.into_inner());
    if current.as_deref() != Some(root) {
        *current = Some(root.to_path_buf());
        drop(current);
        login::invalidate_pending();
        cache().lock().unwrap_or_else(|p| p.into_inner()).clear();
        credit_cache()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clear();
        refresh_state()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clear();
        usage::clear_all();
        refresh_locks()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clear();
        cloud::clear_session_leases();
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

#[derive(Clone)]
struct RefreshCooldown {
    retry_at: i64,
    error: String,
}

fn refresh_locks() -> &'static Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>> {
    static LOCKS: OnceLock<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>> = OnceLock::new();
    LOCKS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn generation_refresh_lock(generation: &str) -> Arc<tokio::sync::Mutex<()>> {
    refresh_locks()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .entry(generation.to_string())
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone()
}

fn refresh_state() -> &'static Mutex<HashMap<String, RefreshCooldown>> {
    static STATE: OnceLock<Mutex<HashMap<String, RefreshCooldown>>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn credit_cache() -> &'static Mutex<HashMap<String, Value>> {
    static CACHE: OnceLock<Mutex<HashMap<String, Value>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(super) fn record_credits(generation: &str, payload: &Value) {
    if !auth::is_current(generation) {
        return;
    }
    let mut credits = serde_json::Map::new();
    for key in ["remainingCredits", "consumedCredits"] {
        if let Some(value) = payload.get(key).filter(|value| value.is_array()) {
            credits.insert(key.into(), value.clone());
        }
    }
    if !credits.is_empty() {
        credit_cache()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(generation.into(), Value::Object(credits));
    }
}

pub(super) fn prune_generation(generation: &str) {
    let _publish = CATALOG_PUBLISH_FENCE
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    cache()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .remove(generation);
    refresh_state()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .remove(generation);
    refresh_locks()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .remove(generation);
    credit_cache()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .remove(generation);
    usage::prune_generation(generation);
    unmark(&HEARTBEAT_SPAWNED, generation);
    unmark(&SURFACE_BOOTSTRAPPED, generation);
    cloud::prune_session_leases(generation);
}

pub(super) fn sync_head_catalog() {
    let root = config_root();
    let Some(head) = auth::generation() else {
        forget_catalog();
        return;
    };
    let should_notify = {
        let _publish = CATALOG_PUBLISH_FENCE
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let valid = auth::generations();
        cache()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|gen, _| valid.contains(gen));
        credit_cache()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|gen, _| valid.contains(gen));
        refresh_state()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|gen, _| valid.contains(gen));
        usage::retain_generations(&valid);
        refresh_locks()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|gen, _| valid.contains(gen));
        let path = root.join("antigravity-models.json");
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
                "clientIdentity": cloud::client_identity(),
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
        super::model_sources::notify_catalog_changed(root, "antigravity");
    }
    if tokio::runtime::Handle::try_current().is_ok()
        && cache()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains_key(&head)
    {
        spawn_heartbeat(&head);
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
    refresh_locks()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clear();
    credit_cache()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clear();
    usage::clear_all();
    cloud::clear_session_leases();
    let _ = std::fs::remove_file(config_root().join("antigravity-models.json"));
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
    let path = config_root().join("antigravity-models.json");
    if std::fs::metadata(&path).ok()?.len() > 8 * 1024 * 1024 {
        return None;
    }
    let snapshot: Value = serde_json::from_slice(&std::fs::read(path).ok()?).ok()?;
    if snapshot["generation"].as_str()? != generation
        || snapshot["clientIdentity"].as_str()? != cloud::client_identity()
    {
        return None;
    }
    Some(CachedCatalog {
        payload: snapshot.get("payload")?.clone(),
        fetched_at: snapshot.get("fetchedAt")?.as_i64()?,
        from_disk: true,
    })
}

#[cfg(test)]
pub(super) fn cloud_test_identity() -> String {
    cloud::client_identity()
}

pub(super) async fn project(
    expected: Option<&str>,
    signal: &AbortSignal,
) -> Result<String, String> {
    let generation = match expected {
        Some(expected) => {
            if !auth::is_current(expected) {
                return Err("antigravity_credential_changed".into());
            }
            expected.to_string()
        }
        None => auth::generation().ok_or("antigravity_not_signed_in")?,
    };
    let credential = auth::credential(Some(&generation)).await?;
    if let Some(project) = credential.project {
        return Ok(project);
    }
    // The account is resolved from the bearer token alone; the hub's own
    // probe carries nothing but the client class, so richer metadata would
    // only deviate from the expected request shape.
    let result = cloud::rpc(
        "loadCodeAssist",
        json!({ "metadata": { "ideType": "ANTIGRAVITY" } }),
        Some(&generation),
        signal,
    )
    .await?;
    usage::cache_load_code_assist(&generation, &result);
    let project = result
        .get("cloudaicompanionProject")
        .and_then(|value| {
            value
                .as_str()
                .or_else(|| value.get("id").and_then(Value::as_str))
        })
        .filter(|project| !project.is_empty())
        .ok_or("antigravity_project_unavailable")?;
    auth::set_project(&generation, project).await?;
    Ok(project.to_string())
}

async fn refresh_catalog(generation: &str) -> Result<Value, String> {
    let root = config_root();
    let signal = AbortSignal::new();
    let project = project(Some(generation), &signal).await?;
    let payload = fetch_available_models(generation, &project, &signal).await?;
    if !payload.get("models").is_some_and(Value::is_object) {
        return Err("antigravity_catalog_invalid".into());
    }
    let payload = catalog::public_payload(&payload);
    // Publishing is fenced by the same lock credential adoption and logout use:
    // a refresh that finishes after either event must not recreate the snapshot
    // they just invalidated or write it under a different configuration root.
    let _credential = auth::credential_lock().lock().await;
    let changed = {
        let _publish = CATALOG_PUBLISH_FENCE
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if !auth::is_current(generation) {
            return Err("antigravity_credential_changed".into());
        }
        if config_root() != root {
            return Err("antigravity_credential_changed".into());
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
        let is_head = auth::is_head_generation(generation);
        if is_head {
            let snapshot = json!({
                "generation":generation,"payload":payload,"fetchedAt":fetched_at,
                "clientIdentity":cloud::client_identity(),
            });
            let path = root.join("antigravity-models.json");
            if let Ok(bytes) = serde_json::to_vec(&snapshot) {
                let _ = std::fs::write(path, bytes);
            }
        }
        changed && is_head
    };
    drop(_credential);
    if changed {
        super::model_sources::notify_catalog_changed(root, "antigravity");
    }
    spawn_bootstrap_surfaces(generation, &project);
    spawn_heartbeat(generation);
    Ok(payload)
}

/// One-shot side calls the hub client makes alongside its catalog fetch.
/// Surface fidelity only: responses are discarded, failures never reach model
/// availability, and the calls run detached so the catalog refresh completes
/// (and releases its shared permit) exactly as fast as the bare fetch would.
fn spawn_bootstrap_surfaces(generation: &str, project: &str) {
    if !mark_once(&SURFACE_BOOTSTRAPPED, generation) {
        return;
    }
    let generation = generation.to_string();
    let project = project.to_string();
    tokio::spawn(async move {
        let signal = AbortSignal::new();
        let (scoped, experiments) = tokio::join!(
            cloud::rpc(
                "loadCodeAssist",
                json!({
                    "cloudaicompanionProject": project,
                    "metadata": { "ideType": "ANTIGRAVITY" }
                }),
                Some(&generation),
                &signal
            ),
            cloud::rpc(
                "listExperiments",
                json!({
                    "project": project,
                    "metadata": { "updateChannel": "stable" }
                }),
                Some(&generation),
                &signal
            ),
        );
        let _ = (scoped, experiments);
        let _ = cloud::rpc_get(
            "buildWithGooglePlugins?page_size=1000",
            Some(&generation),
            &signal,
        )
        .await;
    });
}

fn mark_once(
    set: &std::sync::OnceLock<std::sync::Mutex<std::collections::HashSet<String>>>,
    generation: &str,
) -> bool {
    set.get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(generation.to_string())
}

fn unmark(
    set: &std::sync::OnceLock<std::sync::Mutex<std::collections::HashSet<String>>>,
    generation: &str,
) {
    if let Some(guard) = set.get() {
        guard
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(generation);
    }
}

static SURFACE_BOOTSTRAPPED: std::sync::OnceLock<
    std::sync::Mutex<std::collections::HashSet<String>>,
> = std::sync::OnceLock::new();
static HEARTBEAT_SPAWNED: std::sync::OnceLock<std::sync::Mutex<std::collections::HashSet<String>>> =
    std::sync::OnceLock::new();

/// Periodic control-plane keepalive for the single active head account only.
/// Standby accounts in the pool stay silent until promoted to the head slot.
fn spawn_heartbeat(generation: &str) {
    if !auth::is_head_generation(generation)
        || auth::is_generation_disabled(generation)
        || !mark_once(&HEARTBEAT_SPAWNED, generation)
    {
        return;
    }
    let generation = generation.to_string();
    tokio::spawn(async move {
        let base = std::env::var("KLAUS_ANTIGRAVITY_HEARTBEAT_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(360);
        loop {
            let spread = (base / 4).max(1);
            let jitter = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| u64::from(elapsed.subsec_nanos()) % spread)
                .unwrap_or(0);
            tokio::time::sleep(std::time::Duration::from_secs(base + jitter)).await;
            if !auth::is_current(&generation)
                || !auth::is_head_generation(&generation)
                || auth::is_generation_disabled(&generation)
            {
                unmark(&HEARTBEAT_SPAWNED, &generation);
                break;
            }
            let signal = AbortSignal::new();
            if let Ok(payload) = cloud::rpc(
                "loadCodeAssist",
                json!({ "metadata": { "ideType": "ANTIGRAVITY" } }),
                Some(&generation),
                &signal,
            )
            .await
            {
                usage::cache_load_code_assist(&generation, &payload);
            }
            if !auth::is_current(&generation)
                || !auth::is_head_generation(&generation)
                || auth::is_generation_disabled(&generation)
            {
                unmark(&HEARTBEAT_SPAWNED, &generation);
                break;
            }
            let _ = refresh_catalog(&generation).await;
        }
    });
}

/// A NOT_FOUND refusal names an entity the service no longer resolves. The cached
/// project is the only identity kept without revalidation, so it is the first
/// suspect whenever an otherwise authenticated request hits that verdict.
pub(super) fn is_not_found_message(message: &str) -> bool {
    message.contains("[NOT_FOUND]")
}

/// Drop the cached project and resolve a fresh one from the service.
pub(super) async fn heal_project(generation: &str, signal: &AbortSignal) -> Result<String, String> {
    auth::clear_project(generation).await?;
    project(Some(generation), signal).await
}

async fn fetch_available_models(
    generation: &str,
    project: &str,
    signal: &AbortSignal,
) -> Result<Value, String> {
    let body = json!({ "project": project });
    match cloud::rpc("fetchAvailableModels", body, Some(generation), signal).await {
        // A dead cached project would pin the catalog to its last good snapshot
        // forever; resolve a fresh project and ask once more.
        Err(message) if is_not_found_message(&message) => {
            let healed = heal_project(generation, signal).await?;
            cloud::rpc(
                "fetchAvailableModels",
                json!({ "project": healed }),
                Some(generation),
                signal,
            )
            .await
        }
        result => result,
    }
}

fn refresh_allowed(generation: &str, now: i64) -> bool {
    if auth::is_generation_disabled(generation) {
        return false;
    }
    let state = refresh_state().lock().unwrap_or_else(|p| p.into_inner());
    state
        .get(generation)
        .map(|entry| entry.retry_at)
        .unwrap_or(0)
        <= now
}

fn record_refresh_failure(generation: &str, now: i64, error: impl Into<String>) {
    if !auth::is_current(generation) {
        return;
    }
    refresh_state()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(
            generation.to_string(),
            RefreshCooldown {
                retry_at: now.saturating_add(CATALOG_RETRY_COOLDOWN_MS),
                error: error.into(),
            },
        );
}

fn last_refresh_error(generation: &str) -> Option<String> {
    refresh_state()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(generation)
        .map(|entry| entry.error.clone())
}

fn record_refresh_success(generation: &str) {
    refresh_state()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .remove(generation);
}

fn has_fresh_memory_catalog(generation: &str) -> bool {
    cache()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(generation)
        .is_some_and(|cached| {
            !cached.from_disk
                && chrono::Utc::now().timestamp_millis() - cached.fetched_at <= CATALOG_TTL_MS
        })
}

/// Revalidate behind the caller. A catalog read never waits for this task, and
/// the per-generation refresh lock makes concurrent picker reads share one
/// request without an outgoing account's in-flight fetch dropping the new
/// account's refresh. A failed fetch is not retried by passive reads until the
/// cooldown passes, so an offline machine does not start one request per read.
fn schedule_refresh(generation: String) {
    if !refresh_allowed(&generation, chrono::Utc::now().timestamp_millis()) {
        return;
    }
    let Ok(permit) = generation_refresh_lock(&generation).try_lock_owned() else {
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
            Err(_) => record_refresh_failure(
                &generation,
                chrono::Utc::now().timestamp_millis(),
                "antigravity_catalog_timeout",
            ),
            Ok(Err(err)) => {
                record_refresh_failure(&generation, chrono::Utc::now().timestamp_millis(), err)
            }
        }
        #[cfg(test)]
        REFRESH_COMPLETIONS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    });
}

/// Await a live catalog for the given credential generation when model
/// resolution cannot answer from local state alone. Joins an already in-flight
/// refresh for the same generation instead of issuing a second request.
async fn ensure_live_catalog(generation: &str) -> Result<Value, String> {
    let lock = generation_refresh_lock(generation);
    let (joined_inflight, _permit) = match lock.try_lock() {
        Ok(permit) => (false, permit),
        Err(_) => {
            let permit = tokio::time::timeout(std::time::Duration::from_secs(15), lock.lock())
                .await
                .map_err(|_| "antigravity_catalog_timeout".to_string())?;
            (true, permit)
        }
    };
    if !auth::is_current(generation) {
        return Err("antigravity_credential_changed".into());
    }
    if has_fresh_memory_catalog(generation) {
        if let Some(cached) = cached_payload(generation) {
            return Ok(cached.payload);
        }
    }
    if joined_inflight {
        if let Some(error) = last_refresh_error(generation) {
            return Err(error);
        }
    }
    match tokio::time::timeout(
        std::time::Duration::from_secs(15),
        refresh_catalog(generation),
    )
    .await
    {
        Ok(Ok(payload)) => {
            record_refresh_success(generation);
            Ok(payload)
        }
        Ok(Err(err)) => {
            if err != "antigravity_credential_changed" {
                record_refresh_failure(generation, chrono::Utc::now().timestamp_millis(), &err);
            }
            Err(err)
        }
        Err(_) => {
            record_refresh_failure(
                generation,
                chrono::Utc::now().timestamp_millis(),
                "antigravity_catalog_timeout",
            );
            Err("antigravity_catalog_timeout".into())
        }
    }
}

#[cfg(test)]
pub(super) fn refresh_blocked(generation: &str) -> bool {
    let state = refresh_state().lock().unwrap_or_else(|p| p.into_inner());
    state
        .get(generation)
        .is_some_and(|entry| entry.retry_at > chrono::Utc::now().timestamp_millis())
}

#[cfg(test)]
pub(super) fn refresh_completions() -> usize {
    REFRESH_COMPLETIONS.load(std::sync::atomic::Ordering::SeqCst)
}

/// A local-first catalog read.
///
/// Memory is used only for the current credential generation. A disk snapshot
/// is accepted when it belongs to the same credential generation and client
/// identity; its age decides whether a background revalidation is needed, not
/// whether the picker may use it. With neither local source, the sanitized
/// bundled catalog supplies model identities and their capabilities while the
/// account's entitlement remains unknown until the service answers.
async fn catalog_payload(_warm: bool) -> Result<Value, String> {
    let generation = auth::generation().ok_or("antigravity_not_signed_in")?;
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
        return Err("antigravity_catalog_not_ready".into());
    }
    schedule_refresh(generation);
    Ok(bundled)
}

pub async fn models(warm: bool) -> Result<Value, String> {
    // Snapshot staleness compares the recorded client identity synchronously;
    // resolve it off the async worker pool before any catalog read.
    cloud::ensure_identity().await;
    let generation = auth::generation().ok_or("antigravity_not_signed_in")?;
    let payload = catalog_payload(warm).await?;
    if !auth::is_current(&generation) {
        return Err("antigravity_credential_changed".into());
    }
    let mut reply =
        super::conversation_catalog::response("antigravity", catalog::selectable_models(&payload));
    reply["catalogBaseline"] =
        json!(payload.get("catalogBaseline").and_then(Value::as_bool) == Some(true));
    Ok(reply)
}

pub async fn capabilities() -> Result<Value, String> {
    if auth::generation().is_none() {
        return Ok(json!({"signedIn":false,"reason":"antigravity_not_signed_in"}));
    }
    let mut capabilities = catalog::native_capabilities(&catalog_payload(true).await?);
    if capabilities
        .pointer("/transcription/models")
        .and_then(Value::as_array)
        .is_some_and(|models| !models.is_empty())
    {
        capabilities["transcription"]["available"] = json!(true);
        capabilities["transcription"]["protocol"] = json!("cloud-bidi");
        capabilities["transcription"]["streaming"] = json!(true);
        if let Some(transcription) = capabilities["transcription"].as_object_mut() {
            transcription.remove("reason");
        }
    }
    Ok(capabilities)
}

/// Stream live microphone frames without retaining or pacing a complete file.
pub(crate) async fn stream_dictation(
    input: tokio::sync::mpsc::Receiver<Vec<u8>>,
    ready: tokio::sync::oneshot::Sender<String>,
    publish: crate::app_server::qoder::speech::TextCallback,
    signal: AbortSignal,
) -> Result<String, String> {
    let generation = auth::generation().ok_or("antigravity_not_signed_in")?;
    let model = resolve_native_model(&generation, "audioTranscriptionModelIds", None).await?;
    let credential = auth::credential(Some(&generation)).await?;
    let partial_generation = generation.clone();
    let ready_generation = generation.clone();
    let valid_generation = generation.clone();
    let stream = audio::transcribe_stream(
        input,
        audio::Credentials {
            access_token: &credential.access_token,
            model: &model,
            pool_key: Some(&generation),
        },
        audio::StreamCallbacks {
            ready: Some(Box::new(move || {
                if auth::is_current(&ready_generation) {
                    let _ = ready.send(ready_generation);
                }
            })),
            partial: Some(std::sync::Arc::new(move |text| {
                let generation = partial_generation.clone();
                let publish = publish.clone();
                Box::pin(async move {
                    if auth::is_current(&generation) {
                        publish(text).await;
                    }
                })
            })),
            valid: Some(std::sync::Arc::new(move || {
                auth::is_current(&valid_generation)
            })),
        },
        signal,
    );
    let changed = async {
        let mut timer = tokio::time::interval(std::time::Duration::from_millis(100));
        loop {
            timer.tick().await;
            if !auth::is_current(&generation) {
                break;
            }
        }
    };
    let response = tokio::select! {
        biased;
        _ = changed => return Err("antigravity_credential_changed".into()),
        result = stream => result?,
    };
    if !auth::is_current(&generation) {
        return Err("antigravity_credential_changed".into());
    }
    Ok(response)
}

pub async fn transcribe_audio(
    params: super::wire::AntigravityTranscribeParams,
    signal: AbortSignal,
) -> Result<Value, String> {
    let generation = auth::generation().ok_or("antigravity_not_signed_in")?;
    if params
        .expected_credential_generation
        .as_ref()
        .is_some_and(|expected| expected != &generation)
    {
        return Err("antigravity_credential_changed".into());
    }
    let model = resolve_native_model(&generation, "audioTranscriptionModelIds", None).await?;
    let credential = auth::credential(Some(&generation)).await?;
    let response = match audio::transcribe(
        audio::AudioInput {
            audio_base64: &params.audio_base64,
            mime_type: Some(&params.mime_type),
            language: params.language.as_deref(),
        },
        audio::Credentials {
            access_token: &credential.access_token,
            model: &model,
            pool_key: Some(&generation),
        },
        signal.clone(),
    )
    .await
    {
        Ok(response) => response,
        Err(error)
            if error == "antigravity_transcription_authentication_required"
                && !signal.is_cancelled() =>
        {
            match auth::force_refresh(&generation, Some(&credential.access_token)).await {
                Ok(renewed) => {
                    audio::transcribe(
                        audio::AudioInput {
                            audio_base64: &params.audio_base64,
                            mime_type: Some(&params.mime_type),
                            language: params.language.as_deref(),
                        },
                        audio::Credentials {
                            access_token: &renewed.access_token,
                            model: &model,
                            pool_key: Some(&generation),
                        },
                        signal,
                    )
                    .await?
                }
                Err(refresh_err) if refresh_err == "antigravity_credential_changed" => {
                    return Err(refresh_err);
                }
                Err(_) => return Err(error),
            }
        }
        Err(error) => return Err(error),
    };
    if !auth::is_current(&generation) {
        return Err("antigravity_credential_changed".into());
    }
    Ok(response)
}

pub async fn usage(force: bool) -> Result<Value, String> {
    usage::usage(force).await
}

fn explicit_selection_model(
    selection: &str,
    tiers: &serde_json::Map<String, Value>,
) -> Option<String> {
    let requested = match selection {
        "" | "auto" | "high" | "medium" | "low" => tiers
            .get(selection)
            .and_then(Value::as_str)
            .filter(|model| !model.is_empty()),
        model => Some(model),
    };
    requested.map(|model| {
        let bare = model.strip_prefix("antigravity:").unwrap_or(model);
        super::conversation_catalog::canonical_antigravity_model_id(bare).to_string()
    })
}

fn callable_chat_model(model: &Value) -> bool {
    catalog::is_chat_model(model) && model.get("disabled").and_then(Value::as_bool) != Some(true)
}

fn default_chat_model(payload: &Value) -> Result<String, String> {
    resolve_chat_placeholder(payload, None)
}

fn resolve_chat_placeholder(payload: &Value, family: Option<&str>) -> Result<String, String> {
    let models = catalog::normalize_models(payload);
    let mut callable = models
        .iter()
        .filter(|model| callable_chat_model(model))
        .filter_map(|model| model["key"].as_str());
    let same_family = family.and_then(|family| {
        // Prefer the normal high variant, then the base/legacy thinking variant,
        // then any other declared effort in the service's catalog order.
        callable
            .clone()
            .filter_map(|key| {
                let suffix = key.strip_prefix(family)?;
                let rank = match suffix {
                    "-high" => 0,
                    "" | "-thinking" => 1,
                    "-off" | "-minimal" | "-extra-low" | "-low" | "-medium" | "-xhigh"
                    | "-extra-high" | "-max" => 2,
                    _ => return None,
                };
                Some((rank, key))
            })
            .min_by_key(|(rank, _)| *rank)
            .map(|(_, key)| key)
    });
    same_family
        .or_else(|| callable.next())
        .map(str::to_string)
        .ok_or_else(|| "antigravity_no_models".into())
}

pub async fn main_model(
    selection: &str,
    tiers: &serde_json::Map<String, Value>,
) -> Result<String, String> {
    let tiers = super::conversation_catalog::canonical_antigravity_tiers(tiers);
    if let Some(model) = explicit_selection_model(selection, &tiers) {
        if super::conversation_catalog::antigravity_placeholder_family(&model).is_some() {
            let resolved = resolve(selection, &tiers).await?;
            return resolved.provider["model"]
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| "antigravity_no_models".into());
        }
        return Ok(model);
    }
    cloud::ensure_identity().await;
    default_chat_model(&catalog_payload(true).await?)
}

pub async fn resolve(
    selection: &str,
    tiers: &serde_json::Map<String, Value>,
) -> Result<super::model_sources::Resolved, String> {
    use super::conversation_catalog::antigravity_placeholder_family;

    cloud::ensure_identity().await;
    let tiers = super::conversation_catalog::canonical_antigravity_tiers(tiers);
    let explicit = explicit_selection_model(selection, &tiers);
    let placeholder = explicit.as_deref().and_then(antigravity_placeholder_family);
    for attempt in 0..3 {
        auth::active_or_rotated_credential().await?;
        let generation = auth::generation().ok_or("antigravity_not_signed_in")?;
        let (mut payload, mut schedule_background_refresh) =
            if let Some(cached) = cached_payload(&generation) {
                let stale = cached.from_disk
                    || chrono::Utc::now().timestamp_millis() - cached.fetched_at > CATALOG_TTL_MS;
                (cached.payload, stale)
            } else {
                (catalog::bundled_payload(), true)
            };
        let is_baseline = payload.get("catalogBaseline").and_then(Value::as_bool) == Some(true);
        let fresh_in_memory = has_fresh_memory_catalog(&generation);

        if (explicit.is_none() && is_baseline) || (placeholder.is_some() && !fresh_in_memory) {
            match ensure_live_catalog(&generation).await {
                Ok(live) => {
                    payload = live;
                    schedule_background_refresh = false;
                }
                Err(err) if err == "antigravity_credential_changed" && attempt < 2 => continue,
                Err(err) if err == "antigravity_credential_changed" => return Err(err),
                Err(err) if placeholder.is_some() => return Err(err),
                Err(_) => {}
            }
        }

        let model = match (&explicit, placeholder) {
            (_, Some(family)) => resolve_chat_placeholder(&payload, Some(family))?,
            (Some(model), None) => model.clone(),
            (None, None) => default_chat_model(&payload)?,
        };
        let mut rows = catalog::models_with_unavailable_paid(&payload);
        let selected_present = rows
            .iter()
            .any(|entry| entry["key"].as_str() == Some(model.as_str()));
        let tier_missing = ["high", "medium", "low"].into_iter().any(|tier| {
            tiers
                .get(tier)
                .and_then(Value::as_str)
                .filter(|pick| !pick.is_empty())
                .map(|pick| pick.strip_prefix("antigravity:").unwrap_or(pick))
                .is_some_and(|pick| {
                    if antigravity_placeholder_family(pick).is_some() {
                        is_baseline
                    } else {
                        !rows.iter().any(|entry| entry["key"].as_str() == Some(pick))
                    }
                })
        });

        if !selected_present && !fresh_in_memory {
            match ensure_live_catalog(&generation).await {
                Ok(live) => {
                    payload = live;
                    rows = catalog::models_with_unavailable_paid(&payload);
                    schedule_background_refresh = false;
                }
                Err(err) if err == "antigravity_credential_changed" && attempt < 2 => continue,
                Err(err) => return Err(err),
            }
        } else if tier_missing && !fresh_in_memory {
            match ensure_live_catalog(&generation).await {
                Ok(live) => {
                    payload = live;
                    rows = catalog::models_with_unavailable_paid(&payload);
                    schedule_background_refresh = false;
                }
                Err(err) if err == "antigravity_credential_changed" && attempt < 2 => continue,
                Err(err) if err == "antigravity_credential_changed" => return Err(err),
                Err(_) => {}
            }
        }

        if !auth::is_current(&generation) {
            if attempt < 2 {
                continue;
            }
            return Err("antigravity_credential_changed".into());
        }
        if schedule_background_refresh {
            schedule_refresh(generation);
        }

        let selected = rows
            .iter()
            .find(|entry| entry["key"].as_str() == Some(model.as_str()))
            .ok_or("antigravity_model_unavailable")?;
        if !catalog::is_chat_model(selected) {
            return Err("antigravity_model_purpose_unsupported".into());
        }
        let by_model: Vec<Value> = rows
            .iter()
            .filter(|entry| {
                entry["key"].as_str() != Some(model.as_str()) && catalog::is_chat_model(entry)
            })
            .map(|entry| json!({"model":entry["key"],"capabilities":entry["capabilities"]}))
            .collect();
        let mut model_tiers = serde_json::Map::new();
        for tier in ["high", "medium", "low"] {
            if let Some(pick) = tiers.get(tier).and_then(Value::as_str) {
                let pick = pick.strip_prefix("antigravity:").unwrap_or(pick);
                if let Some(family) = antigravity_placeholder_family(pick) {
                    if payload["catalogBaseline"] != true {
                        if let Ok(model) = resolve_chat_placeholder(&payload, Some(family)) {
                            model_tiers.insert(tier.into(), json!(model));
                        }
                    }
                    continue;
                }
                if rows.iter().any(|entry| {
                    entry["key"].as_str() == Some(pick) && catalog::is_chat_model(entry)
                }) {
                    model_tiers.insert(tier.into(), json!(pick));
                }
            }
        }
        return Ok(super::model_sources::Resolved {
            provider: json!({"type":"gemini","model":model}),
            auth: Value::Null,
            capabilities: selected["capabilities"].clone(),
            capabilities_by_model: by_model,
            model_tiers,
            pricing: None,
        });
    }
    Err("antigravity_credential_changed".into())
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

pub(super) fn model_details(generation: &str, model: &str) -> Option<Value> {
    let payload = cached_payload(generation)
        .map(|cached| cached.payload)
        .unwrap_or_else(catalog::bundled_payload);
    catalog::model_details(&payload, model)
}

fn selected_dialect(model: &RuntimeString, init: &ProtocolSessionInit) -> GeminiThinkingDialect {
    use klaus_agent_core::runtime_values::runtime_codec;
    let discovered = auth::generation().and_then(|generation| {
        let model = model.try_to_utf8().ok()?;
        let metadata = model_details(&generation, &model)?;
        let capabilities = catalog::model_capabilities(&metadata);
        let thinking = capabilities.get("geminiThinking")?;
        runtime_codec::decode_text::<GeminiThinkingInit>(&RuntimeString::from(thinking.to_string()))
            .ok()
    });
    if discovered.is_some() {
        return dialect(discovered.as_ref());
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

fn apply_source_compaction_cap(
    model_id: Option<&str>,
    capabilities: &mut klaus_agent_core::types::provider::ModelCapabilities,
    generation: Option<&str>,
) {
    let details = generation
        .zip(model_id)
        .and_then(|(gen, id)| model_details(gen, id));
    let context_window = capabilities.context_window.or_else(|| {
        details
            .as_ref()
            .and_then(|d| d.get("maxTokens"))
            .and_then(Value::as_u64)
            .filter(|&n| n > 0)
    });
    let max_output = capabilities.max_output_tokens.or_else(|| {
        details
            .as_ref()
            .and_then(|d| d.get("maxOutputTokens"))
            .and_then(Value::as_u64)
            .filter(|&n| n > 0)
    });
    if let Some(source_limit) =
        catalog::model_auto_compact_token_limit(details.as_ref(), context_window, max_output)
    {
        capabilities.auto_compact_token_limit = Some(
            capabilities
                .auto_compact_token_limit
                .map_or(source_limit, |declared| declared.min(source_limit)),
        );
    }
}

pub(super) fn decorate(
    provider: Arc<dyn ModelProvider>,
    init: &ProtocolSessionInit,
) -> Arc<dyn ModelProvider> {
    let head_generation = auth::generation();
    let model = provider.info().default_model.clone();
    let default_model_id = model.try_to_utf8().ok();
    let mut capabilities = provider.model_metadata(&model);
    apply_source_compaction_cap(
        default_model_id.as_deref(),
        &mut capabilities,
        head_generation.as_deref(),
    );
    let mut by_model: HashMap<_, _> = provider
        .offline_model_catalog()
        .into_iter()
        .filter_map(|(id, mut caps)| {
            let id = id.try_to_utf8().ok()?;
            apply_source_compaction_cap(Some(&id), &mut caps, head_generation.as_deref());
            Some((id, caps))
        })
        .collect();
    for entry in init.capabilities_by_model.iter().flatten() {
        if let Ok(id) = entry.model.try_to_utf8() {
            let mut caps = provider.model_metadata(&entry.model);
            apply_source_compaction_cap(Some(&id), &mut caps, head_generation.as_deref());
            by_model.insert(id, caps);
        }
    }
    let minimal_reasoning_models: HashMap<String, RuntimeString> = head_generation
        .as_deref()
        .map(|generation| {
            let payload = cached_payload(generation)
                .map(|cached| cached.payload)
                .unwrap_or_else(catalog::bundled_payload);
            let mut mapped = HashMap::new();
            for (from, to) in catalog::minimal_reasoning_models(&payload) {
                if !by_model.contains_key(&from) {
                    continue;
                }
                if !by_model.contains_key(&to) {
                    if let Some(details) = catalog::model_details(&payload, &to) {
                        let raw_caps = catalog::model_capabilities(&details);
                        if let Ok(caps) =
                            klaus_agent_core::runtime_values::runtime_codec::decode_text::<
                                klaus_agent_core::types::provider::ModelCapabilities,
                            >(&RuntimeString::from(
                                raw_caps.to_string(),
                            ))
                        {
                            by_model.insert(to.clone(), caps);
                        }
                    }
                }
                if by_model.contains_key(&to) {
                    mapped.insert(from, RuntimeString::from(to));
                }
            }
            mapped
        })
        .unwrap_or_default();
    let output_token_defaults = by_model
        .keys()
        .map(String::as_str)
        .chain(default_model_id.as_deref())
        .map(|id| {
            let details = head_generation
                .as_deref()
                .and_then(|generation| model_details(generation, id));
            (
                id.to_owned(),
                catalog::output_token_defaults(details.as_ref()),
            )
        })
        .collect();
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
    let admission_generations = auth::generations();
    let multi_account = admission_generations.len() > 1;
    Arc::new(GeminiProvider::new(GeminiProviderOptions {
        default_model: default_model_id,
        client: Some(Arc::new(cloud::CloudClient::pooled(conversation_id))),
        retry: Some(RetryOptions {
            yield_to_credential_rotation: multi_account,
            max_timeout_attempts: Some(cloud::STREAM_HEADER_TIMEOUT_MAX_ATTEMPTS),
            ..Default::default()
        }),
        stream_admission: Some(Arc::new(move || {
            admission_generations
                .iter()
                .any(|generation| auth::is_current(generation))
        })),
        capabilities: Some(capabilities),
        capabilities_by_model: Some(by_model),
        output_token_defaults,
        thinking_dialect: Some(selected_dialect(&model, init)),
        thinking_dialects_by_model: Some(thinking_dialects_by_model),
        minimal_reasoning_models: Some(minimal_reasoning_models),
        adapter_policy: GeminiAdapterPolicy {
            replay_signed_parts: true,
            scope_signatures_to_model: true,
            honor_tools_capability: true,
            model_tool_budget: true,
        },
        ..Default::default()
    }))
}

fn native_model(payload: &Value, field: &str, requested: Option<&str>) -> Result<String, String> {
    let models = payload
        .get(field)
        .and_then(Value::as_array)
        .ok_or("antigravity_capability_unavailable")?;
    match requested {
        Some(model) if models.iter().any(|value| value.as_str() == Some(model)) => {
            Ok(model.to_string())
        }
        Some(_) => Err("antigravity_native_model_unavailable".into()),
        None => models
            .iter()
            .find_map(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| "antigravity_capability_unavailable".into()),
    }
}

async fn resolve_native_model(
    generation: &str,
    field: &str,
    requested: Option<&str>,
) -> Result<String, String> {
    cloud::ensure_identity().await;
    let (payload, schedule_background_refresh) = if let Some(cached) = cached_payload(generation) {
        let stale = cached.from_disk
            || chrono::Utc::now().timestamp_millis() - cached.fetched_at > CATALOG_TTL_MS;
        (cached.payload, stale)
    } else {
        (catalog::bundled_payload(), true)
    };
    match native_model(&payload, field, requested) {
        Ok(model) => {
            if schedule_background_refresh {
                schedule_refresh(generation.to_string());
            }
            Ok(model)
        }
        Err(_) if !has_fresh_memory_catalog(generation) => {
            let live = ensure_live_catalog(generation).await?;
            native_model(&live, field, requested)
        }
        Err(err) => Err(err),
    }
}

async fn generate_content_with_rotation(
    model_field: &str,
    requested_model: Option<&str>,
    request_type: &str,
    session_id: Option<&str>,
    build_request: impl Fn(&str) -> Value,
    signal: &AbortSignal,
) -> Result<(String, String, Value), String> {
    let conversation_id = session_id
        .filter(|id| !id.trim().is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let mut attempted: Vec<String> = Vec::new();
    loop {
        let generation = auth::generation().ok_or("antigravity_not_signed_in")?;
        let model = resolve_native_model(&generation, model_field, requested_model).await?;
        let outcome: Result<Value, String> = async {
            let project = project(Some(&generation), signal).await?;
            let (wire_conv_id, wire_step, wire_session_id) =
                cloud::wire_conversation_context(&conversation_id, &generation, 0);
            let mut inner_request = build_request(&model);
            if let Some(obj) = inner_request.as_object_mut() {
                obj.insert("sessionId".into(), Value::String(wire_session_id));
            }
            let body = cloud::envelope_with_context(
                &project,
                &model,
                inner_request,
                request_type,
                Some(&wire_conv_id),
                Some(wire_step),
            );
            cloud::rpc("generateContent", body, Some(&generation), signal).await
        }
        .await;
        match outcome {
            Ok(response) => return Ok((generation, model, response)),
            Err(err)
                if !signal.is_cancelled()
                    && cloud::is_rotatable_message(&err)
                    && auth::account_count() > 1
                    && !attempted.contains(&generation) =>
            {
                attempted.push(generation.clone());
                match auth::rotate(&generation).await {
                    RotateOutcome::Rotated(next) if !attempted.contains(&next) => {
                        cloud::wait_rotation_cooldown(signal)
                            .await
                            .map_err(str::to_string)?;
                        continue;
                    }
                    RotateOutcome::AlreadyAdvanced(next) if !attempted.contains(&next) => {
                        continue;
                    }
                    _ => return Err(err),
                }
            }
            Err(err) => return Err(err),
        }
    }
}

pub async fn generate_image(
    params: super::wire::AntigravityImageParams,
    signal: AbortSignal,
) -> Result<Value, String> {
    use base64::Engine;
    if params.prompt.trim().is_empty() || params.prompt.len() > 128 * 1024 {
        return Err("antigravity_image_prompt_invalid".into());
    }
    let mut parts = vec![json!({"text":params.prompt})];
    let mut total_bytes = 0usize;
    for image in &params.reference_images {
        if !matches!(
            image.mime_type.as_str(),
            "image/png" | "image/jpeg" | "image/webp" | "image/gif"
        ) || image.data_base64.len() > 16 * 1024 * 1024
        {
            return Err("antigravity_reference_image_invalid".into());
        }
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(&image.data_base64)
            .map_err(|_| "antigravity_reference_image_invalid")?;
        total_bytes = total_bytes.saturating_add(decoded.len());
        if total_bytes > 20 * 1024 * 1024 {
            return Err("antigravity_reference_images_too_large".into());
        }
        parts.push(json!({"inlineData":{"mimeType":image.mime_type,"data":image.data_base64}}));
    }
    let aspect_ratio = match params.size.as_deref() {
        None | Some("1024x1024" | "1:1") => "1:1",
        Some("1536x1024" | "3:2") => "3:2",
        Some("1024x1536" | "2:3") => "2:3",
        Some("16:9") => "16:9",
        Some("9:16") => "9:16",
        Some(_) => return Err("antigravity_image_size_unsupported".into()),
    };
    let (generation, model, response) = generate_content_with_rotation(
        "imageGenerationModelIds",
        params.model.as_deref(),
        "image_gen",
        params.session_id.as_deref(),
        |_| {
            json!({
                "contents":[{"role":"user","parts":parts}],
                "generationConfig":{"candidateCount":1,"imageConfig":{"aspectRatio":aspect_ratio}},
            })
        },
        &signal,
    )
    .await?;
    let content = response
        .pointer("/response/candidates/0/content/parts")
        .or_else(|| response.pointer("/candidates/0/content/parts"))
        .and_then(Value::as_array)
        .ok_or("antigravity_image_response_invalid")?;
    let images:Vec<Value> = content.iter().filter_map(|part| {
        let inline = part.get("inlineData")?;
        Some(json!({"imageBase64":inline.get("data")?.as_str()?,"mimeType":inline.get("mimeType")?.as_str()?}))
    }).collect();
    let mut result = images
        .first()
        .cloned()
        .ok_or("antigravity_image_not_returned")?;
    result["images"] = json!(images);
    result["model"] = json!(model);
    if !auth::is_current(&generation) {
        return Err("antigravity_credential_changed".into());
    }
    Ok(result)
}

pub async fn search(
    params: super::wire::AntigravitySearchParams,
    signal: AbortSignal,
) -> Result<Value, String> {
    if params.query.trim().is_empty() || params.query.len() > 32 * 1024 {
        return Err("antigravity_search_query_invalid".into());
    }
    if !params.allowed_domains.is_empty()
        || !params.blocked_domains.is_empty()
        || params.freshness.is_some()
    {
        return Err("antigravity_search_filter_unsupported".into());
    }
    let query_text = params.query.clone();
    let (generation, model, response) = generate_content_with_rotation(
        "webSearchModelIds",
        params.model.as_deref(),
        "web_search",
        params.session_id.as_deref(),
        |_| {
            json!({
                "contents":[{"role":"user","parts":[{"text":query_text}]}],
                "tools":[{"googleSearch":{}}],
            })
        },
        &signal,
    )
    .await?;
    let candidate = response
        .pointer("/response/candidates/0")
        .or_else(|| response.pointer("/candidates/0"))
        .ok_or("antigravity_search_response_invalid")?;
    let text = candidate
        .pointer("/content/parts")
        .and_then(Value::as_array)
        .map(|parts| {
            parts
                .iter()
                .filter_map(|part| part["text"].as_str())
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default();
    let results: Vec<Value> = candidate
        .pointer("/groundingMetadata/groundingChunks")
        .and_then(Value::as_array)
        .map(|chunks| {
            chunks
                .iter()
                .filter_map(|chunk| {
                    let web = chunk.get("web")?;
                    let url = web.get("uri")?.as_str()?;
                    if !url.starts_with("https://") && !url.starts_with("http://") {
                        return None;
                    }
                    Some(json!({"url":url,"title":web["title"].as_str().unwrap_or(url)}))
                })
                .collect()
        })
        .unwrap_or_default();
    if !auth::is_current(&generation) {
        return Err("antigravity_credential_changed".into());
    }
    Ok(
        json!({"results":results,"text":text,"model":model,"groundingMetadata":candidate.get("groundingMetadata")}),
    )
}

pub(super) fn search_backend() -> klaus_agent_core::tools::builtins::web_search::WebSearchBackend {
    Arc::new(|query, signal| {
        Box::pin(async move {
            let result = search(
                super::wire::AntigravitySearchParams {
                    query: query
                        .try_to_utf8()
                        .map_err(|_| RuntimeString::from("antigravity_search_query_invalid"))?,
                    model: None,
                    allowed_domains: Vec::new(),
                    blocked_domains: Vec::new(),
                    freshness: None,
                    operation_id: None,
                    session_id: None,
                },
                signal,
            )
            .await
            .map_err(RuntimeString::from)?;
            Ok(result["results"]
                .as_array()
                .ok_or_else(|| RuntimeString::from("antigravity_search_response_invalid"))?
                .iter()
                .map(
                    |row| klaus_agent_core::tools::builtins::web_search::WebSearchResult {
                        title: RuntimeString::from(row["title"].as_str().unwrap_or("")),
                        url: RuntimeString::from(row["url"].as_str().unwrap_or("")),
                        snippet: None,
                    },
                )
                .collect())
        })
    })
}

#[cfg(test)]
mod tests;
