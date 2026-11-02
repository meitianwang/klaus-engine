//! Antigravity account access through its direct cloud model service.

mod audio;
pub mod auth;
mod catalog;
mod cloud;
pub mod login;

use klaus_agent_core::provider::gemini::{
    GeminiAdapterPolicy, GeminiProvider, GeminiProviderOptions, GeminiThinkingDialect,
};
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::ModelProvider;
use klaus_agent_protocol::types::{GeminiThinkingInit, ProtocolSessionInit};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

static CONFIG_ROOT: RwLock<Option<PathBuf>> = RwLock::new(None);
const CATALOG_TTL_MS: i64 = 5 * 60 * 1000;

pub fn set_config_root(root: &Path) {
    let mut current = CONFIG_ROOT.write().unwrap_or_else(|p| p.into_inner());
    if current.as_deref() != Some(root) {
        *current = Some(root.to_path_buf());
        drop(current);
        login::invalidate_pending();
        *cache().lock().unwrap_or_else(|p| p.into_inner()) = None;
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
    generation: String,
    payload: Value,
    fetched_at: i64,
}

fn cache() -> &'static Mutex<Option<CachedCatalog>> {
    static CACHE: OnceLock<Mutex<Option<CachedCatalog>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(None))
}

fn refresh_lock() -> Arc<tokio::sync::Mutex<()>> {
    static LOCK: OnceLock<Arc<tokio::sync::Mutex<()>>> = OnceLock::new();
    LOCK.get_or_init(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone()
}

fn credit_cache() -> &'static Mutex<Option<(String, Value)>> {
    static CACHE: OnceLock<Mutex<Option<(String, Value)>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(None))
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
        *credit_cache().lock().unwrap_or_else(|p| p.into_inner()) =
            Some((generation.into(), Value::Object(credits)));
    }
}

fn forget_catalog() {
    *cache().lock().unwrap_or_else(|p| p.into_inner()) = None;
    *credit_cache().lock().unwrap_or_else(|p| p.into_inner()) = None;
    let _ = std::fs::remove_file(config_root().join("antigravity-models.json"));
}

fn cached_payload(generation: &str) -> Option<CachedCatalog> {
    if let Some(cached) = cache()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .as_ref()
        .filter(|cached| cached.generation == generation)
        .cloned()
    {
        return Some(cached);
    }
    let path = config_root().join("antigravity-models.json");
    if std::fs::metadata(&path).ok()?.len() > 8 * 1024 * 1024 {
        return None;
    }
    let snapshot: Value = serde_json::from_slice(&std::fs::read(path).ok()?).ok()?;
    if snapshot["generation"].as_str()? != generation {
        return None;
    }
    Some(CachedCatalog {
        generation: generation.to_string(),
        payload: snapshot.get("payload")?.clone(),
        fetched_at: snapshot.get("fetchedAt")?.as_i64()?,
    })
}

pub(super) async fn project(
    expected: Option<&str>,
    signal: &AbortSignal,
) -> Result<String, String> {
    let generation = auth::generation().ok_or("antigravity_not_signed_in")?;
    if expected.is_some_and(|expected| expected != generation) {
        return Err("antigravity_credential_changed".into());
    }
    let credential = auth::credential(Some(&generation)).await?;
    if let Some(project) = credential.project {
        return Ok(project);
    }
    let result = cloud::rpc(
        "loadCodeAssist",
        json!({
            "metadata":{
                "ideType":"ANTIGRAVITY", "ideName":"antigravity", "pluginType":"GEMINI",
                "ideVersion":"2.24.0", "pluginVersion":"2.24.0", "platform":platform_id(),
            },
            "mode":"HEALTH_CHECK",
        }),
        Some(&generation),
        signal,
    )
    .await?;
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

fn platform_id() -> &'static str {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "aarch64") => "DARWIN_ARM64",
        ("macos", _) => "DARWIN_AMD64",
        ("windows", "aarch64") => "WINDOWS_ARM64",
        ("windows", _) => "WINDOWS_AMD64",
        ("linux", "aarch64") => "LINUX_ARM64",
        _ => "LINUX_AMD64",
    }
}

async fn refresh_catalog(generation: &str) -> Result<Value, String> {
    let signal = AbortSignal::new();
    let project = project(Some(generation), &signal).await?;
    let payload = fetch_available_models(generation, &project, &signal).await?;
    if !payload.get("models").is_some_and(Value::is_object) {
        return Err("antigravity_catalog_invalid".into());
    }
    let payload = catalog::public_payload(&payload);
    if !auth::is_current(generation) {
        return Err("antigravity_credential_changed".into());
    }
    let fetched_at = chrono::Utc::now().timestamp_millis();
    *cache().lock().unwrap_or_else(|p| p.into_inner()) = Some(CachedCatalog {
        generation: generation.to_string(),
        payload: payload.clone(),
        fetched_at,
    });
    let snapshot = json!({"generation":generation,"payload":payload,"fetchedAt":fetched_at});
    let path = config_root().join("antigravity-models.json");
    if let Ok(bytes) = serde_json::to_vec(&snapshot) {
        let _ = std::fs::write(path, bytes);
    }
    Ok(payload)
}

/// A NOT_FOUND refusal names an entity the service no longer resolves. The cached
/// project is the only identity kept without revalidation, so it is the first
/// suspect whenever an otherwise authenticated request hits that verdict.
pub(super) fn is_not_found_message(message: &str) -> bool {
    message.contains("[NOT_FOUND]")
}

/// Drop the cached project and resolve a fresh one from the service.
pub(super) async fn heal_project(
    generation: &str,
    signal: &AbortSignal,
) -> Result<String, String> {
    auth::clear_project(generation).await?;
    project(Some(generation), signal).await
}

async fn fetch_available_models(
    generation: &str,
    project: &str,
    signal: &AbortSignal,
) -> Result<Value, String> {
    let body = json!({
        "project":project,"requestId":uuid::Uuid::new_v4().to_string(),
    });
    match cloud::rpc("fetchAvailableModels", body, Some(generation), signal).await {
        // A dead cached project would pin the catalog to its last good snapshot
        // forever; resolve a fresh project and ask once more.
        Err(message) if is_not_found_message(&message) => {
            let healed = heal_project(generation, signal).await?;
            cloud::rpc(
                "fetchAvailableModels",
                json!({
                    "project":healed,"requestId":uuid::Uuid::new_v4().to_string(),
                }),
                Some(generation),
                signal,
            )
            .await
        }
        result => result,
    }
}

async fn catalog_payload(warm: bool) -> Result<Value, String> {
    let generation = auth::generation().ok_or("antigravity_not_signed_in")?;
    if let Some(cached) = cached_payload(&generation) {
        if chrono::Utc::now().timestamp_millis() - cached.fetched_at > CATALOG_TTL_MS {
            if let Ok(permit) = refresh_lock().try_lock_owned() {
                tokio::spawn(async move {
                    let _permit = permit;
                    let _ = refresh_catalog(&generation).await;
                });
            }
        }
        return Ok(cached.payload);
    }
    let lock = refresh_lock();
    let _permit = lock.lock().await;
    if let Some(cached) = cached_payload(&generation) {
        return Ok(cached.payload);
    }
    if warm {
        match tokio::time::timeout(
            std::time::Duration::from_secs(15),
            refresh_catalog(&generation),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err("antigravity_catalog_timeout".into()),
        }
    } else {
        refresh_catalog(&generation).await
    }
}

pub async fn models(warm: bool) -> Result<Value, String> {
    let generation = auth::generation().ok_or("antigravity_not_signed_in")?;
    let payload = catalog_payload(warm).await?;
    if !auth::is_current(&generation) {
        return Err("antigravity_credential_changed".into());
    }
    Ok(super::conversation_catalog::response(
        "antigravity",
        catalog::models_with_unavailable_paid(&payload),
    ))
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
    let payload = catalog_payload(true).await?;
    let model = native_model(&payload, "audioTranscriptionModelIds", None)?;
    let credential = auth::credential(Some(&generation)).await?;
    let partial_generation = generation.clone();
    let ready_generation = generation.clone();
    let valid_generation = generation.clone();
    let stream = audio::transcribe_stream(
        input,
        audio::Credentials {
            access_token: &credential.access_token,
            model: &model,
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
    let payload = catalog_payload(true).await?;
    let model = native_model(&payload, "audioTranscriptionModelIds", None)?;
    let credential = auth::credential(Some(&generation)).await?;
    let response = audio::transcribe(
        audio::AudioInput {
            audio_base64: &params.audio_base64,
            mime_type: Some(&params.mime_type),
            language: params.language.as_deref(),
        },
        audio::Credentials {
            access_token: &credential.access_token,
            model: &model,
        },
        signal,
    )
    .await?;
    if !auth::is_current(&generation) {
        return Err("antigravity_credential_changed".into());
    }
    Ok(response)
}

pub async fn usage() -> Result<Value, String> {
    let Some(generation) = auth::generation() else {
        return Ok(json!({"signedIn":false}));
    };
    let signal = AbortSignal::new();
    let project = project(Some(&generation), &signal).await?;
    let quota = cloud::rpc(
        "retrieveUserQuota",
        json!({"project":project}),
        Some(&generation),
        &signal,
    )
    .await;
    let summary = cloud::rpc(
        "retrieveUserQuotaSummary",
        json!({"project":project}),
        Some(&generation),
        &signal,
    )
    .await;
    if quota.is_err() && summary.is_err() {
        return Err(quota
            .err()
            .unwrap_or_else(|| "antigravity_usage_unavailable".into()));
    }
    if !auth::is_current(&generation) {
        return Err("antigravity_credential_changed".into());
    }
    let metadata=cloud::rpc("loadCodeAssist",json!({"cloudaicompanionProject":project,"metadata":{"ideType":"ANTIGRAVITY","pluginType":"GEMINI","ideName":"antigravity","platform":platform_id(),"ideVersion":"2.24.0","pluginVersion":"2.24.0"}}),Some(&generation),&signal).await.unwrap_or(Value::Null);
    let plan = metadata
        .get("paidTier")
        .or_else(|| metadata.get("currentTier"))
        .and_then(|tier| tier.get("name").or_else(|| tier.get("id")))
        .and_then(Value::as_str);
    let mut summary = summary.unwrap_or(Value::Null);
    if summary.is_null() {
        summary = json!({});
    }
    let live_credits = credit_cache()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .as_ref()
        .filter(|(owner, _)| owner == &generation)
        .map(|(_, value)| value.clone());
    let credits = live_credits
        .as_ref()
        .and_then(|value| value.get("remainingCredits"))
        .or_else(|| metadata.pointer("/paidTier/availableCredits"))
        .or_else(|| metadata.pointer("/currentTier/availableCredits"));
    if let Some(credits) = credits {
        summary["availableCredits"] = credits.clone();
    }
    if !auth::is_current(&generation) {
        return Err("antigravity_credential_changed".into());
    }
    let mut usage = catalog::normalize_usage(
        &quota.unwrap_or(Value::Null),
        &summary,
        auth::account().as_deref(),
        plan,
    );
    if let Some(consumed) = live_credits
        .as_ref()
        .and_then(|value| value.get("consumedCredits"))
    {
        usage["lastRequestConsumedCredits"] = consumed.clone();
    }
    Ok(usage)
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
        return Ok(model
            .strip_prefix("antigravity:")
            .unwrap_or(model)
            .to_string());
    }
    let models = catalog::normalize_models(&catalog_payload(true).await?);
    models
        .iter()
        .filter(|model| catalog::is_chat_model(model))
        .find(|model| model.get("disabled").and_then(Value::as_bool) != Some(true))
        .and_then(|model| model["key"].as_str())
        .map(str::to_string)
        .ok_or_else(|| "antigravity_no_models".into())
}

pub async fn resolve(
    selection: &str,
    tiers: &serde_json::Map<String, Value>,
) -> Result<super::model_sources::Resolved, String> {
    auth::credential(None).await?;
    let model = main_model(selection, tiers).await?;
    let rows = catalog::models_with_unavailable_paid(&catalog_payload(true).await?);
    let selected = rows
        .iter()
        .find(|entry| entry["key"].as_str() == Some(&model))
        .ok_or("antigravity_model_unavailable")?;
    if !catalog::is_chat_model(selected) {
        return Err("antigravity_model_purpose_unsupported".into());
    }
    let by_model: Vec<Value> = rows
        .iter()
        .filter(|entry| entry["key"].as_str() != Some(&model) && catalog::is_chat_model(entry))
        .map(|entry| json!({"model":entry["key"],"capabilities":entry["capabilities"]}))
        .collect();
    let mut model_tiers = serde_json::Map::new();
    for tier in ["high", "medium", "low"] {
        if let Some(pick) = tiers.get(tier).and_then(Value::as_str) {
            let pick = pick.strip_prefix("antigravity:").unwrap_or(pick);
            if rows
                .iter()
                .any(|entry| entry["key"].as_str() == Some(pick) && catalog::is_chat_model(entry))
            {
                model_tiers.insert(tier.into(), json!(pick));
            }
        }
    }
    Ok(super::model_sources::Resolved {
        provider: json!({"type":"gemini","model":model}),
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

pub(super) fn model_details(generation: &str, model: &str) -> Option<Value> {
    let payload = cached_payload(generation)?.payload;
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

pub(super) fn decorate(
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
    let generation = auth::generation().unwrap_or_default();
    let admission_generation = generation.clone();
    Arc::new(GeminiProvider::new(GeminiProviderOptions {
        default_model: model.try_to_utf8().ok(),
        client: Some(Arc::new(cloud::CloudClient { generation })),
        stream_admission: Some(Arc::new(move || auth::is_current(&admission_generation))),
        capabilities: Some(capabilities),
        capabilities_by_model: Some(by_model),
        thinking_dialect: Some(selected_dialect(&model, init)),
        thinking_dialects_by_model: Some(thinking_dialects_by_model),
        adapter_policy: GeminiAdapterPolicy {
            replay_signed_parts: true,
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

pub async fn generate_image(
    params: super::wire::AntigravityImageParams,
    signal: AbortSignal,
) -> Result<Value, String> {
    use base64::Engine;
    if params.prompt.trim().is_empty() || params.prompt.len() > 128 * 1024 {
        return Err("antigravity_image_prompt_invalid".into());
    }
    let generation = auth::generation().ok_or("antigravity_not_signed_in")?;
    let payload = catalog_payload(true).await?;
    let model = native_model(&payload, "imageGenerationModelIds", params.model.as_deref())?;
    let project = project(Some(&generation), &signal).await?;
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
    let response = cloud::rpc(
        "generateContent",
        cloud::envelope(
            &project,
            &model,
            json!({
                "contents":[{"role":"user","parts":parts}],
                "generationConfig":{"candidateCount":1,"imageConfig":{"aspectRatio":aspect_ratio}},
            }),
            "image_gen",
        ),
        Some(&generation),
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
    let generation = auth::generation().ok_or("antigravity_not_signed_in")?;
    let payload = catalog_payload(true).await?;
    let model = native_model(&payload, "webSearchModelIds", params.model.as_deref())?;
    let project = project(Some(&generation), &signal).await?;
    let response = cloud::rpc(
        "generateContent",
        cloud::envelope(
            &project,
            &model,
            json!({
                "contents":[{"role":"user","parts":[{"text":params.query}]}],
                "tools":[{"googleSearch":{}}],
            }),
            "web_search",
        ),
        Some(&generation),
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
